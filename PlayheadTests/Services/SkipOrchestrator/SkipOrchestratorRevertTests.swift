// SkipOrchestratorRevertTests.swift
// Tests for revertByTimeRange and revertWindow methods added in playhead-gpi.
// Verifies that user corrections ("Not an ad" banner, "This isn't an ad" popover)
// properly revert in-memory state, remove skip cues, and broadcast segment updates.

import CoreMedia
import Foundation
import Testing
import XCTest

@testable import Playhead

// MARK: - Cycle 3 H4: TranscriptPeekView duplicate-signal regression rail
//
// XCTest source canary. The cycle-2 M2 fix removed an unconditional
// `recordFalseSkipSignal` call from `TranscriptPeekView.submitNotAdChunks`
// because `SkipOrchestrator.revertByTimeRange` now records the (weak or
// full) trust signal itself — see SkipOrchestrator.swift M2 routing.
// Without a regression rail, a future diff could re-add the duplicate call
// silently, restoring the 2x penalty on managed reverts and the 0.05+0.10
// asymmetry on suggest-only reverts. The orchestrator-level tests in this
// file pin the routing CONTRACT; this canary pins the SOURCE-LEVEL
// invariant on the view's veto handler.
//
// XCTest (not Swift Testing) so the canary is filterable from the test
// plan — see project memory `xctestplan_swift_testing_limitation`.
final class TranscriptPeekViewVetoSourceCanaryTests: XCTestCase {

    func testSubmitNotAdChunksDoesNotRecordTrustSignalDirectly() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Views/NowPlaying/TranscriptPeekView.swift"
        )
        guard let funcRange = source.range(of: "func submitNotAdChunks(") else {
            XCTFail("Could not locate `func submitNotAdChunks(` in TranscriptPeekView.swift")
            return
        }
        guard let openBrace = source[funcRange.upperBound...].firstIndex(of: "{") else {
            XCTFail("Could not locate `{` after `func submitNotAdChunks(`")
            return
        }
        let body = SwiftSourceInspector.bracedBody(in: source, startingAt: openBrace)
        let stripped = SwiftSourceInspector.strippingComments(body)
        XCTAssertFalse(
            stripped.contains("recordFalseSkipSignal"),
            """
            TranscriptPeekView.submitNotAdChunks must NOT call \
            recordFalseSkipSignal directly. SkipOrchestrator.revertByTimeRange \
            is the single source of trust signaling for revert gestures so \
            the cycle-1 M2 weak/strong routing applies correctly. Re-adding \
            this call would double-count the trust penalty (managed: 2x; \
            suggest-only: 0.05+0.10 asymmetry).
            """
        )
        XCTAssertFalse(
            stripped.contains("recordWeakFalseSkipSignal"),
            "submitNotAdChunks must not call recordWeakFalseSkipSignal directly either; see canary above."
        )
        XCTAssertFalse(
            stripped.contains("recordVeto("),
            "submitNotAdChunks must not pre-write a duplicate correction; the orchestrator owns the row+correction transaction."
        )
        let successGuard = try XCTUnwrap(
            stripped.range(
                of:
                    "guard await revertCallback(syntheticSpan) else { return }"
            )
        )
        let selectionClear = try XCTUnwrap(
            stripped.range(of: "notAdMarkedChunkSelections = []")
        )
        XCTAssertLessThan(
            successGuard.lowerBound,
            selectionClear.lowerBound,
            "Transcript veto selections must remain available until the exact transaction succeeds."
        )
        let markingModeExit = try XCTUnwrap(
            stripped.range(of: "isNotAdMarkingMode = false")
        )
        XCTAssertLessThan(
            successGuard.lowerBound,
            markingModeExit.lowerBound,
            "A failed transaction must keep not-ad marking mode open for retry."
        )
        XCTAssertTrue(
            stripped.contains(
                "defer { isSubmittingNotAdCorrection = false }"
            ),
            "Every success/failure path must release the duplicate-submit reservation."
        )

        let chunkRow = try XCTUnwrap(
            SwiftSourceInspector.firstBody(
                in: source,
                after: "func chunkRow("
            )
        )
        XCTAssertTrue(
            SwiftSourceInspector.strippingComments(chunkRow).contains(
                "guard !isSubmittingCorrection else { return }"
            ),
            "Transcript taps must not mutate a selection while its exact correction transaction is in flight."
        )

        let markModeToggle = try XCTUnwrap(
            SwiftSourceInspector.firstBody(
                in: source,
                after: "var markModeToggle: some View"
            )
        )
        XCTAssertTrue(
            SwiftSourceInspector.strippingComments(markModeToggle).contains(
                ".disabled(isSubmittingCorrection)"
            ),
            "The competing mark-ad mode must not clear veto retry state while persistence is suspended."
        )
    }

    func testAdRegionPopoverUsesOnlyTransactionalRevertCallback() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath:
                "Playhead/Views/NowPlaying/AdRegionPopover.swift"
        )
        XCTAssertFalse(
            SwiftSourceInspector.strippingComments(source)
                .contains("recordVeto("),
            """
            AdRegionPopover must not append a CorrectionEvent before invoking \
            the orchestrator. Its callback owns the exact-row + correction \
            transaction; a direct pre-write duplicates feedback and makes \
            SQLite failure non-atomic.
            """
        )
        XCTAssertTrue(
            source.contains(
                "guard await onRevertAdWindows(span) else { return }"
            ),
            "A failed or stale popover action must remain retryable."
        )
    }

    func testTranscriptSheetCapturesImmutablePlaybackContext() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath:
                "Playhead/Views/NowPlaying/NowPlayingView.swift"
        )
        XCTAssertTrue(
            source.contains(
                ".sheet(item: $transcriptPeekContext)"
            )
        )
        XCTAssertTrue(
            source.contains(
                "transcriptPeekContext = nil"
            )
        )
        XCTAssertTrue(
            source.contains(
                "sourceContext.playbackLifecycleGeneration"
            )
        )
        XCTAssertTrue(
            source.contains("onMarkAd: { startTime, endTime in")
        )
        XCTAssertTrue(
            source.contains(
                "ifCurrentAnalysisAssetId:\n                            sourceContext.analysisAssetId"
            )
        )
    }

    func testTranscriptPositiveMarksUseOnlyBoundTransactionalCallback() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath:
                "Playhead/Views/NowPlaying/TranscriptPeekView.swift"
        )
        for signature in [
            "func submitMarkedChunks()",
            "func submitUntranscribedTailMark(",
        ] {
            let body = try XCTUnwrap(
                SwiftSourceInspector.firstBody(in: source, after: signature)
            )
            let stripped = SwiftSourceInspector.strippingComments(body)
            XCTAssertFalse(
                stripped.contains("injectUserMarkedAd"),
                "\(signature) must not read mutable runtime playback identity"
            )
            let durableGuard = try XCTUnwrap(
                stripped.range(of: "guard await markAd(")
            )
            let trustWrite = try XCTUnwrap(
                stripped.range(of: "recordFalseNegativeSignal")
            )
            XCTAssertLessThan(
                durableGuard.lowerBound,
                trustWrite.lowerBound,
                "Trust may change only after the bound durable correction succeeds"
            )
            XCTAssertTrue(
                stripped.contains(
                    "defer { isSubmittingMarkAdCorrection = false }"
                ),
                "\(signature) must release its duplicate-submit reservation on every outcome"
            )

            if signature == "func submitMarkedChunks()" {
                let selectionClear = try XCTUnwrap(
                    stripped.range(of: "markedChunkSelections = []")
                )
                let modeExit = try XCTUnwrap(
                    stripped.range(of: "isMarkingMode = false")
                )
                XCTAssertLessThan(
                    durableGuard.lowerBound,
                    selectionClear.lowerBound,
                    "A failed positive correction must preserve the exact selected rows for retry"
                )
                XCTAssertLessThan(
                    durableGuard.lowerBound,
                    modeExit.lowerBound,
                    "A failed positive correction must keep marking mode open for retry"
                )
            } else {
                let pendingClear = try XCTUnwrap(
                    stripped.range(of: "pendingTailSpan = nil")
                )
                XCTAssertLessThan(
                    durableGuard.lowerBound,
                    pendingClear.lowerBound,
                    "A failed tail correction must preserve its exact captured range"
                )
                XCTAssertTrue(
                    stripped.contains("showTailMarkConfirmation = true"),
                    "A failed tail correction must surface the preserved action for retry"
                )
            }
        }
    }

    /// Episode identity alone is insufficient for deferred banner actions:
    /// replaying the same canonical episode advances the orchestrator
    /// lifecycle while preserving the ID. Pin generation capture + comparison
    /// in every episode-bound action that suspends on persistence/trust work.
    func testEpisodeBoundActionsGuardSameEpisodeLifecycleGeneration() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/SkipOrchestrator/SkipOrchestrator.swift"
        )
        let signatures = [
            "func denyAutoSkippedBanner(\n        windowId: String,",
            "func revertByTimeRange(\n        start: Double,\n        end: Double,\n        analysisAssetId",
            "func revertWindow(\n        windowId: String,\n        podcastId: String? = nil,\n        ifCurrentEpisodeId",
            "func acceptSuggestedSkip(\n        windowId: String,\n        ifCurrentEpisodeId",
            "func declineSuggestedSkip(\n        windowId: String,\n        isExplicitDenial: Bool = false,\n        ifCurrentEpisodeId",
        ]

        for signature in signatures {
            guard let body = SwiftSourceInspector.firstBody(
                in: source,
                after: signature
            ) else {
                XCTFail("Could not locate episode-bound action body for \(signature).")
                continue
            }
            XCTAssertTrue(
                body.contains("let sourceLifecycleGeneration = episodeLifecycleGeneration"),
                "\(signature) must capture the lifecycle generation before its first suspension"
            )
            XCTAssertTrue(
                body.contains("episodeLifecycleGeneration == sourceLifecycleGeneration"),
                "\(signature) must reject live-state work after a same-episode restart"
            )
        }
    }

    /// Exact feedback receipts belong only in the durable correction store.
    /// Pin every explicit route against accidentally reusing the diagnostic
    /// decision logger or interpolating receipt fields into OSLog.
    func testExplicitBannerFeedbackRoutesDoNotWriteDetailedLogs() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/SkipOrchestrator/SkipOrchestrator.swift"
        )
        let signatures = [
            "func confirmAutoSkippedBanner(\n        windowId: String,",
            "func denyAutoSkippedBanner(\n        windowId: String,",
            "func acceptSuggestedSkip(\n        windowId: String,\n        ifCurrentEpisodeId",
            "func declineSuggestedSkip(\n        windowId: String,\n        isExplicitDenial: Bool = false,\n        ifCurrentEpisodeId",
        ]

        for signature in signatures {
            guard let rawBody = SwiftSourceInspector.firstBody(
                in: source,
                after: signature
            ) else {
                XCTFail("Could not locate explicit feedback route \(signature)")
                continue
            }
            let body = SwiftSourceInspector.strippingComments(rawBody)
            XCTAssertFalse(
                body.contains("logDecision("),
                "\(signature) must not append exact feedback to the decision log"
            )
            XCTAssertFalse(
                body.contains("localizedDescription"),
                "\(signature) must not copy persistence details into OSLog"
            )
            XCTAssertFalse(
                body.contains("String(describing:"),
                "\(signature) must not copy persistence details into OSLog"
            )
            if body.contains("recordFalseSkipSignal")
                || body.contains("recordFalseNegativeSignal")
            {
                XCTAssertTrue(
                    body.contains(
                        "privacy: .explicitBannerFeedback"
                    ),
                    "\(signature) must route every derived trust signal through the log-suppressed explicit-feedback branch"
                )
            }
        }
    }

    func testGenericRevertWindowRetainsManualVetoLoggingSemantics()
        throws
    {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath:
                "Playhead/Services/SkipOrchestrator/SkipOrchestrator.swift"
        )
        let signature =
            "func revertWindow(\n        windowId: String,\n        podcastId: String? = nil,\n        ifCurrentEpisodeId"
        guard let rawBody = SwiftSourceInspector.firstBody(
            in: source,
            after: signature
        ) else {
            XCTFail("Could not locate generic revertWindow body")
            return
        }
        let body = SwiftSourceInspector.strippingComments(rawBody)
        XCTAssertTrue(body.contains("source: .manualVeto"))
        XCTAssertTrue(body.contains("logDecision("))
        XCTAssertTrue(body.contains("recordFalseSkipSignal("))
        XCTAssertFalse(body.contains(".bannerAutoSkipDenied"))
        XCTAssertFalse(body.contains("privacy: .explicitBannerFeedback"))
    }

    func testExplicitTrustBranchesContainOnlyGenericFailureTelemetry()
        throws
    {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath:
                "Playhead/Services/TrustScoring/TrustScoringService.swift"
        )
        for signature in [
            "func recordFalseSkipSignal(",
            "func recordFalseNegativeSignal(",
        ] {
            guard let method = SwiftSourceInspector.firstBody(
                in: source,
                after: signature
            ), let branchRange = method.range(
                of: "if privacy == .explicitBannerFeedback"
            ), let openBrace = method[branchRange.upperBound...]
                .firstIndex(of: "{")
            else {
                XCTFail(
                    "Could not locate explicit trust branch for \(signature)"
                )
                continue
            }
            let branch = SwiftSourceInspector.strippingComments(
                SwiftSourceInspector.bracedBody(
                    in: method,
                    startingAt: openBrace
                )
            )
            XCTAssertTrue(
                branch.contains("Banner feedback trust update failed")
            )
            for forbidden in [
                "podcastId",
                "localizedDescription",
                "skipTrustScore",
                "recentFalseSkipSignals",
                "false-negative",
                "false-skip",
            ] {
                XCTAssertFalse(
                    branch.contains(forbidden),
                    "\(signature) explicit failure telemetry leaked \(forbidden)"
                )
            }
        }
    }

    func testExplicitBannerDerivedLearningFailureLogIsGeneric() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/UserCorrectionStore.swift"
        )
        guard let actorRange = source.range(
            of: "actor PersistentUserCorrectionStore"
        ) else {
            XCTFail("Could not locate PersistentUserCorrectionStore")
            return
        }
        let actorSource = String(source[actorRange.lowerBound...])
        guard let methodBody = SwiftSourceInspector.firstBody(
            in: actorSource,
            after: "func correctionDidPersistAtomically("
        ), let branchRange = methodBody.range(
            of: "if event.source?.isExplicitBannerFeedback == true"
        ), let openBrace = methodBody[branchRange.upperBound...]
            .firstIndex(of: "{")
        else {
            XCTFail("Could not locate explicit-feedback failure-log branch")
            return
        }
        let branch = SwiftSourceInspector.strippingComments(
            SwiftSourceInspector.bracedBody(
                in: methodBody,
                startingAt: openBrace
            )
        )
        XCTAssertTrue(branch.contains("Banner feedback follow-up failed"))
        XCTAssertFalse(branch.contains("analysisAssetId"))
        XCTAssertFalse(branch.contains("scope"))
        XCTAssertFalse(branch.contains("localizedDescription"))
        XCTAssertFalse(branch.contains("source"))
    }
}

// MARK: - playhead-i08e: revert seams under mid-gesture episode replacement
//
// These replace a ~560-line SOURCE canary that scanned `SkipOrchestrator.swift`
// for the ORDER and NESTING of the revert seams' statements. Do not go looking
// for it in history on `main`: this bead BUILT that canary and then discarded
// it (commits 5c592521 → 4de76d33 on `bead/playhead-i08e`), so the branch's
// net diff shows only these tests. It was dropped rather than weakened, and
// the reasoning is worth keeping because it applies to every rail of its kind:
//
//   • A textual rail can only reject the spellings someone thought of. Five
//     consecutive review passes each found a fresh spelling that reproduced the
//     bug with every existing rail green (a wrapper block, a top-level
//     `guard activeEpisodeId == … else { return }`, a lifecycle clause folded
//     into an existing condition, a check against a DIFFERENT live field, a
//     deleted-but-not-`return`ing in-loop guard). There is always an evasion
//     N+1, so the rail's strength was a function of review effort rather than
//     of the invariant.
//   • It also failed in the other direction: it pinned `revertByTimeRange`'s
//     exact shape (an `if revertedAny` block, a `suggestWindows.compactMap`
//     work list, exactly two `break` guards and one `return` guard), so the
//     restructure that the seam's own KNOWN-GAP comment recommends — running
//     both loops to completion under a `didLoseLifecycle` flag — would have
//     reddened it wholesale while IMPROVING the behaviour it guards.
//
// The tests below assert the CONTRACT instead, by interleaving a real episode
// replacement with the exact suspension the live-lifecycle guards were written
// for. They are spelling-independent (any way of dropping the effects fails
// them) and shape-independent (any restructure that preserves the behaviour
// keeps them green).
//
// The interleave needs a suspension point inside the seams, which is what
// `_setRevertPersistenceBarrierForTesting` provides — the fourth instance of a
// pattern this file already relies on to test the banner seams' equivalent
// races (`_setFeedbackPersistenceBarrierForTesting`,
// `_setSuggestPersistenceBarrierForTesting`,
// `_setAppliedPersistenceBarrierForTesting`; see
// `staleAutoYesCannotCommitAfterMaterialReplacement` below). Production leaves
// it nil, so the added production surface is one optional-closure check per
// durable revert write.

/// Counts skip-cue republications. The orchestrator's cue handler is a
/// synchronous `@Sendable` closure, so it cannot read an actor; a lock-guarded
/// counter is the smallest thing that can observe it.
private final class SkipCuePushCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0

    func record() {
        lock.lock()
        value += 1
        lock.unlock()
    }

    var count: Int {
        lock.lock()
        defer { lock.unlock() }
        return value
    }
}

@Suite("SkipOrchestrator revert seams survive mid-gesture episode replacement (playhead-i08e)")
struct SkipOrchestratorRevertLifecycleRaceTests {

    /// `recordListenRevert`: the whole seam in one interleave. The receipt and
    /// the controller sample are owed to the CAPTURED show — by the time the
    /// replacement lands, this gesture's full-magnitude trust penalty has
    /// already been applied, so dropping them leaves trust and corrections
    /// permanently out of step. Only the cue republication belongs to whoever
    /// owns the episode now.
    ///
    /// Mutation-verified: moving the seam's lifecycle guard back above the
    /// effects (its shape between 5c1a167e and playhead-i08e) fails this on the
    /// controller-sample poll; moving only `ingestNegativeFingerprint` below
    /// the guard fails it on the hard-negative poll; attributing the sample to
    /// `activePodcastId` instead of the captured `podcastId` fails it on the
    /// same poll (the replacement episode is a DIFFERENT show, which is what
    /// makes "the captured show" an assertable claim here); deleting the
    /// guard outright fails it on the cue-push count; deleting the trust
    /// penalty, downgrading it to the weak listen-rewind variant, or
    /// attributing it to `activePodcastId`, fails it on the trust profile; and
    /// attributing the RECEIPT to `activePodcastId` fails it on the receipt's
    /// own `podcastId` — those last four each survived the whole repo-wide
    /// gate before the ninth pass added them.
    @Test(
        "A Listen revert whose episode is replaced mid-flight still calibrates the captured show",
        .timeLimit(.minutes(1))
    )
    func listenRevertSurvivesEpisodeReplacement() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1"))
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: "asset-2", episodeId: "ep-2"))
        // Own the trust store so the penalty is readable, and seed BOTH shows
        // so "the replacement was not penalised" is a positive observation
        // rather than a vacuous one — `recordFalseSkipSignal` never
        // lazy-creates, so an unseeded show swallows a misrouted penalty.
        let trustStore = try await makeTestStore()
        for show in ["podcast-1", "podcast-2"] {
            try await seedSkipTestTrustProfile(
                in: trustStore, podcastId: show,
                mode: "auto", trustScore: 0.9, observations: 10
            )
        }
        let trustService = TrustScoringService(store: trustStore)
        let controllerStore = try makeTestControllerStore(prefix: "i08e-revert-race")
        // The hard-negative ingest is the third calibration effect the seam
        // owes the captured show, and it is invisible unless a bank is wired.
        let negativeBank = try NegativeFingerprintBank(
            directoryURL: try makeTempDir(prefix: "i08e-revert-race-bank")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setNegativeFingerprintBank(negativeBank)
        let cuePushes = SkipCuePushCounter()
        await orchestrator.setSkipCueHandler { _ in cuePushes.record() }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "ep-1", podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-race", startTime: 60, endTime: 120,
            confidence: 0.85, decisionState: "confirmed",
            // Comfortably above the bank's 4-token floor, so a short fixture
            // string can never make the hard-negative assertion vacuous.
            evidenceText: "this episode is brought to you by our sponsor"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        let gate = ControlledAsyncGate()
        await orchestrator._setRevertPersistenceBarrierForTesting {
            await gate.wait()
        }
        let gesture = Task {
            await orchestrator.recordListenRevert(
                windowId: "ad-race", podcastId: "podcast-1"
            )
        }
        await gate.waitUntilStarted()

        // A replacement episode takes ownership while the gesture is parked.
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2", episodeId: "ep-2", podcastId: "podcast-2"
        )
        await orchestrator._setRevertPersistenceBarrierForTesting(nil)
        let pushesBeforeResume = cuePushes.count
        await gate.release()
        await gesture.value

        let state = try await awaitControllerSampleCount(
            controllerStore, orchestrator: orchestrator, show: "podcast-1", expected: 1
        )
        #expect(state.sampleCount == 1, "one revert must record exactly one controller sample")
        #expect(state.integral == 1, "a Listen revert is a FALSE-POSITIVE signal → integral +1")

        let receipts = try await awaitCorrectionReceipts(
            store,
            orchestrator: orchestrator,
            correctionStore: correctionStore,
            assetId: "asset-1",
            expected: 1
        )
        #expect(receipts.count == 1, "the captured show's receipt must still commit")
        #expect(receipts.first?.source == .listenRevert)
        #expect(
            receipts.first?.podcastId == "podcast-1",
            """
            The receipt was stamped with the show that is live at effect time. \
            Show attribution is what routes derived learning, so a receipt \
            minted for the CAPTURED episode must not inherit the replacement's \
            identity.
            """
        )

        let negatives = try await awaitNegativeBankEntries(
            negativeBank, orchestrator: orchestrator, expected: 1
        )
        #expect(negatives.count == 1, "the confirmed-FP hard negative must still be ingested")
        #expect(
            negatives.first?.showId == "podcast-1",
            "the hard negative belongs to the CAPTURED show, not the replacement"
        )

        #expect(
            try await controllerRowsExcludingBarrier(controllerStore, orchestrator) == 1,
            """
            Exactly one show may carry controller state. A second row means the \
            sample was attributed to the show that is live at effect time \
            rather than the one captured at gesture time.
            """
        )

        let row = try #require(try await store.fetchAdWindow(id: "ad-race"))
        #expect(row.decisionState == AdDecisionState.reverted.rawValue)

        // The seam's FOURTH effect, and the premise the three above rest on:
        // the penalty is applied to the CAPTURED show before the replacement
        // lands, which is precisely why dropping the receipt / hard negative /
        // controller sample would leave trust and corrections out of step. It
        // is awaited inside the gesture, so no poll is needed. Nothing else in
        // the repo pinned it — deleting the trust call left the full gate green.
        let penalised = try #require(
            try await trustStore.fetchProfile(podcastId: "podcast-1")
        )
        #expect(
            penalised.recentFalseSkipSignals == 1,
            "the captured show must carry exactly one false-skip signal"
        )
        // MAGNITUDE, not merely direction. `recentFalseSkipSignals` and a
        // downward trust nudge are common to `recordFalseSkipSignal` and
        // `recordWeakFalseSkipSignal`, so `< 0.9` alone is satisfied by the
        // weak listen-rewind variant — a live routing distinction this
        // subsystem makes deliberately (see `revertByTimeRange`'s
        // managed-vs-suggest split). A Listen revert of an auto-skip is the
        // strong signal: the algorithm pre-committed and was wrong.
        let expectedTrust = 0.9 - TrustScoringConfig.default.falseSignalPenalty
        #expect(
            abs(penalised.skipTrustScore - expectedTrust) < 1e-9,
            """
            expected the FULL-magnitude penalty \
            (0.9 − \(TrustScoringConfig.default.falseSignalPenalty) = \
            \(expectedTrust)), got \(penalised.skipTrustScore). The weak \
            listen-rewind variant leaves \
            \(0.9 - TrustScoringConfig.default.weakFalseSignalPenalty).
            """
        )
        let untouched = try #require(
            try await trustStore.fetchProfile(podcastId: "podcast-2")
        )
        #expect(
            untouched.recentFalseSkipSignals == 0,
            "the replacement show must not absorb the captured gesture's penalty"
        )
        #expect(
            untouched.skipTrustScore >= 0.9,
            "the replacement show's trust must not be decremented by a gesture that predates it"
        )

        #expect(
            cuePushes.count == pushesBeforeResume,
            """
            The replaced gesture republished skip cues. LIVE cue state belongs \
            to the episode that owns the orchestrator now, so the lifecycle \
            guard must still gate evaluateAndPush().
            """
        )
        await controllerStore.close()
        await negativeBank.close()
    }

    /// `revertByTimeRange`, managed loop. One interleave pins four contracts
    /// the discarded canary needed a separate regex rail for each of:
    ///
    ///   • the in-loop guard must `break`, not `return` — a `return` abandons a
    ///     gesture whose row is already durably reverted, dropping the receipt
    ///     and the controller sample;
    ///   • the in-loop guard must also still EXIST — `windows` is live state,
    ///     so a loop that runs on re-inserts the OLD episode's entries into the
    ///     dictionary `beginEpisode` has just cleared for the replacement;
    ///   • the suggest-tier work list must be gated on the captured lifecycle —
    ///     `suggestWindows` is live state, so building it after the loop broke
    ///     out collects and vetoes the REPLACEMENT episode's suggestions;
    ///   • the final guard must still gate `evaluateAndPush()`.
    @Test(
        "A time-range revert whose episode is replaced mid-loop calibrates the captured show and leaves the replacement alone",
        .timeLimit(.minutes(1))
    )
    func timeRangeRevertSurvivesEpisodeReplacementInManagedLoop() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1"))
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: "asset-2", episodeId: "ep-2"))
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let controllerStore = try makeTestControllerStore(prefix: "i08e-revert-race")
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        let cuePushes = SkipCuePushCounter()
        await orchestrator.setSkipCueHandler { _ in cuePushes.record() }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "ep-1", podcastId: "podcast-1"
        )

        // TWO managed windows, both inside the vetoed range, so the loop has a
        // second iteration to be stopped before — the same reason the
        // suggest-tier case below uses two. With only one, deleting the in-loop
        // guard outright is behaviourally invisible (the loop ends anyway) and
        // this test cannot tell a guard that `break`s from no guard at all.
        //
        // The spans are deliberately LONG (90s and 195s) rather than merely
        // over `minimumSpanSeconds` (15s), because the revert loop `continue`s
        // past `.suppressed` entries: a fixture window that `evaluateWindow`
        // demotes silently drops this pair back to ONE effective iteration and
        // re-opens exactly the vacuity it exists to close.
        //
        // Span is not the only demotion route, though — `evaluateWindow` also
        // suppresses a `.confirmed` window whose `confidence` falls under
        // `stayThreshold`, so sizing the spans clear of `minimumSpanSeconds`
        // moves the coupling to a confidence constant rather than removing it.
        // The two self-checks after ingest are therefore what actually holds
        // this together, and between them they cover both ways the loop can
        // skip an entry (`SkipOrchestrator.revertByTimeRange`'s two `continue`s):
        //   • `suppressedFixtureWindows` — a demotion, observed directly, so it
        //     is indifferent to every threshold constant;
        //   • `fixtureWindowsOverlapVetoRange` — the overlap predicate, which
        //     `ad-range-b` clears by only 5s against the vetoed [70, 110]. An
        //     edit to either bound or to B's start silently collapses this to
        //     one effective iteration, and no threshold check can see that.
        // (`activeWindowIDs()` is dictionary membership only — a spelling check
        // on the fixture, not evidence that either entry is walkable.)
        let managedA = makeSkipTestAdWindow(
            id: "ad-range-a", startTime: 10, endTime: 100,
            confidence: 0.85, decisionState: "confirmed"
        )
        let managedB = makeSkipTestAdWindow(
            id: "ad-range-b", startTime: 105, endTime: 300,
            confidence: 0.85, decisionState: "confirmed"
        )
        let vetoStart = 70.0
        let vetoEnd = 110.0
        try await store.insertAdWindow(managedA)
        try await store.insertAdWindow(managedB)
        await orchestrator.receiveAdWindows([managedA, managedB])
        // Fixture self-check 1/3: both ids really did land in the managed
        // dictionary the loop iterates.
        let fixtureIds = Set([managedA.id, managedB.id])
        #expect(await orchestrator.activeWindowIDs() == fixtureIds)
        // 2/3: neither was demoted to `.suppressed` on ingest, which the revert
        // loop `continue`s past. Reading the decision log observes the demotion
        // directly, so this stays honest under any retune of
        // `minimumSpanSeconds`, `shortSpanOverrideConfidence`, `enterThreshold`
        // or `stayThreshold` — the whole point of a two-iteration fixture is
        // that BOTH iterations exist.
        let suppressedFixtureWindows = Set(
            await orchestrator.getDecisionLog()
                .filter {
                    $0.decision == .suppressed
                        && fixtureIds.contains($0.adWindowId)
                }
                .map(\.adWindowId)
        )
        #expect(
            suppressedFixtureWindows.isEmpty,
            """
            \(suppressedFixtureWindows.sorted()) demoted to .suppressed on \
            ingest, so the revert loop skips past it and this test silently \
            drops back to ONE effective iteration — it can then no longer tell \
            an in-loop lifecycle guard that `break`s from no guard at all.
            """
        )
        // 3/3: both still satisfy the loop's OTHER `continue` — its overlap
        // predicate. `ad-range-b` clears it by only 5s, so this mirrors the
        // production condition rather than restating the numbers, and no
        // threshold check would catch a fixture edit that broke it.
        let nonOverlapping = [managedA, managedB].filter {
            !($0.startTime < vetoEnd && $0.endTime > vetoStart)
        }
        #expect(
            nonOverlapping.isEmpty,
            """
            \(nonOverlapping.map(\.id).sorted()) no longer overlaps the vetoed \
            range [\(vetoStart), \(vetoEnd)], so the revert loop skips past it \
            — same one-effective-iteration vacuity as a demotion, by a \
            different route.
            """
        )

        let gate = ControlledAsyncGate()
        await orchestrator._setRevertPersistenceBarrierForTesting {
            await gate.wait()
        }
        let gesture = Task {
            // Same bounds the self-check above validated the fixture against,
            // so the two cannot drift apart.
            await orchestrator.revertByTimeRange(
                start: vetoStart, end: vetoEnd, podcastId: "podcast-1"
            )
        }
        await gate.waitUntilStarted()

        // The replacement episode takes ownership and publishes a suggestion
        // that overlaps the SAME time range the parked gesture is vetoing.
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2", episodeId: "ep-2", podcastId: "podcast-2"
        )
        let replacementSuggestion = makeSkipTestMarkOnlyWindow(
            id: "sug-replacement", assetId: "asset-2", startTime: 80, endTime: 100
        )
        try await store.insertAdWindow(replacementSuggestion)
        await orchestrator.receiveAdWindows([replacementSuggestion])
        #expect(await orchestrator.activeSuggestWindowIDs().contains("sug-replacement"))

        await orchestrator._setRevertPersistenceBarrierForTesting(nil)
        let pushesBeforeResume = cuePushes.count
        await gate.release()
        await gesture.value

        let state = try await awaitControllerSampleCount(
            controllerStore, orchestrator: orchestrator, show: "podcast-1", expected: 1
        )
        #expect(state.sampleCount == 1, "one time-range revert must record exactly one controller sample")
        #expect(state.integral == 1, "a managed-window revert is a FALSE-POSITIVE signal → integral +1")

        let receipts = try await awaitCorrectionReceipts(
            store,
            orchestrator: orchestrator,
            correctionStore: correctionStore,
            assetId: "asset-1",
            expected: 1
        )
        #expect(receipts.count == 1, "the captured show's receipt must still commit")
        #expect(receipts.first?.source == .manualVeto)
        #expect(
            receipts.first?.podcastId == "podcast-1",
            """
            The receipt was stamped with the show that is live at effect time \
            rather than the one captured at gesture time. The controller \
            sample is already pinned to the captured show; the receipt's own \
            attribution is what routes derived learning and must agree with it.
            """
        )

        #expect(
            try await controllerRowsExcludingBarrier(controllerStore, orchestrator) == 1,
            "the sample must land on the captured show only, never on the replacement"
        )

        // Positive control that the parked gesture really did durable work.
        // `windows` is a Dictionary, so WHICH of the two the loop reached
        // before the barrier is unspecified — assert over the pair rather than
        // naming one. Deliberately silent about the other row: leaving it
        // `confirmed` is this seam's documented KNOWN GAP, and pinning either
        // answer here would redden the `didLoseLifecycle` restructure that
        // closes it.
        let rowA = try #require(try await store.fetchAdWindow(id: "ad-range-a"))
        let rowB = try #require(try await store.fetchAdWindow(id: "ad-range-b"))
        #expect(
            [rowA, rowB].contains {
                $0.decisionState == AdDecisionState.reverted.rawValue
            },
            "the window the loop had reached must still be durably reverted"
        )

        // The live-state half of the in-loop guard's contract. `beginEpisode`
        // cleared `windows` for the replacement, so neither OLD id may be back
        // in it: a loop that kept going re-inserts its snapshot's entries
        // (`windows[id] = managed`) into the episode that owns them now.
        let liveManagedIds = await orchestrator.activeWindowIDs()
        #expect(
            !liveManagedIds.contains("ad-range-a")
                && !liveManagedIds.contains("ad-range-b"),
            """
            The replaced gesture re-inserted an OLD episode's managed window \
            into the REPLACEMENT's live state. The managed loop must stop at \
            its lifecycle guard, not merely avoid `return`ing from it.
            """
        )

        #expect(
            await orchestrator.activeSuggestWindowIDs().contains("sug-replacement"),
            """
            The replaced gesture vetoed the REPLACEMENT episode's suggestion. \
            `suggestWindows` is live state, so the suggest-tier work list must \
            be gated on the captured lifecycle.
            """
        )
        let replacementRow = try #require(
            try await store.fetchAdWindow(id: "sug-replacement")
        )
        #expect(
            replacementRow.decisionState != AdDecisionState.reverted.rawValue,
            "the replacement episode's suggestion must not be durably reverted either"
        )
        #expect(
            cuePushes.count == pushesBeforeResume,
            "the final lifecycle guard must still gate evaluateAndPush()"
        )
        await controllerStore.close()
    }

    /// `revertByTimeRange` with NO show attribution and NO trust service — the
    /// one behavioural change playhead-i08e's guard relocation actually makes,
    /// and the only configuration that can observe it.
    ///
    /// The guard this replaces sat INSIDE `if let podcastId, let trustService`,
    /// so an anonymous revert fell straight through to `evaluateAndPush()` and
    /// republished cues into an episode it no longer owned. Every other test in
    /// this suite passes a podcastId AND wires a trust service, so moving the
    /// guard back inside that branch leaves them all green.
    ///
    /// Dropping the trust service also makes this the one race test that fails
    /// if an early exit keyed on an OPTIONAL DEPENDENCY (rather than on the
    /// lifecycle) is introduced above the effects — e.g.
    /// `guard trustService != nil else { return }` — which the deleted canary
    /// rejected wholesale via `assertNoEarlyExit` and no other test now covers.
    @Test(
        "An anonymous time-range revert replaced mid-loop keeps its receipt and still gates cue republication",
        .timeLimit(.minutes(1))
    )
    func anonymousTimeRangeRevertSurvivesEpisodeReplacement() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1"))
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: "asset-2", episodeId: "ep-2"))
        // No trustService and no correction-store-side show attribution.
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        let cuePushes = SkipCuePushCounter()
        await orchestrator.setSkipCueHandler { _ in cuePushes.record() }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "ep-1", podcastId: nil
        )

        let managed = makeSkipTestAdWindow(
            id: "ad-anon-range", startTime: 60, endTime: 120,
            confidence: 0.85, decisionState: "confirmed"
        )
        try await store.insertAdWindow(managed)
        await orchestrator.receiveAdWindows([managed])

        let gate = ControlledAsyncGate()
        await orchestrator._setRevertPersistenceBarrierForTesting {
            await gate.wait()
        }
        let gesture = Task {
            await orchestrator.revertByTimeRange(
                start: 70, end: 110, podcastId: nil
            )
        }
        await gate.waitUntilStarted()

        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2", episodeId: "ep-2", podcastId: nil
        )
        await orchestrator._setRevertPersistenceBarrierForTesting(nil)
        let pushesBeforeResume = cuePushes.count
        await gate.release()
        await gesture.value

        // The gesture is still owed to the captured episode even unattributed.
        let receipts = try await awaitCorrectionReceipts(
            store,
            orchestrator: orchestrator,
            correctionStore: correctionStore,
            assetId: "asset-1",
            expected: 1
        )
        #expect(receipts.count == 1, "an anonymous revert still commits its receipt")
        #expect(receipts.first?.source == .manualVeto)
        let row = try #require(try await store.fetchAdWindow(id: "ad-anon-range"))
        #expect(row.decisionState == AdDecisionState.reverted.rawValue)

        #expect(
            cuePushes.count == pushesBeforeResume,
            """
            An anonymous revert republished cues into the REPLACEMENT episode. \
            The live-state guard must sit outside the podcastId/trustService \
            branch so it holds for gestures that never reach the trust engine.
            """
        )
    }

    /// `revertByTimeRange`, suggest-tier loop. The mirror of the case above for
    /// the loop the deleted canary reached only by counting `break` keywords:
    /// the guard must exist (so the loop stops mutating live banner state for a
    /// replacement episode) AND it must `break` (so the receipt still lands).
    ///
    /// A suggest-only gesture is also the negative arm of `revertedManagedAny`:
    /// nothing was auto-skipped, so no controller sample may be recorded even
    /// though the gesture completed.
    @Test(
        "A suggest-only revert whose episode is replaced mid-loop keeps its receipt and stops retiring banners",
        .timeLimit(.minutes(1))
    )
    func timeRangeRevertSurvivesEpisodeReplacementInSuggestLoop() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1"))
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: "asset-2", episodeId: "ep-2"))
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let controllerStore = try makeTestControllerStore(prefix: "i08e-revert-race")
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        let cuePushes = SkipCuePushCounter()
        await orchestrator.setSkipCueHandler { _ in cuePushes.record() }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "ep-1", podcastId: "podcast-1"
        )

        // Two suggest-tier windows so the loop has a second iteration to be
        // stopped before. No managed windows at all, so the managed loop's
        // body — and its copy of the barrier — is never entered.
        let first = makeSkipTestMarkOnlyWindow(
            id: "sug-a", assetId: "asset-1", startTime: 60, endTime: 90
        )
        let second = makeSkipTestMarkOnlyWindow(
            id: "sug-b", assetId: "asset-1", startTime: 95, endTime: 118
        )
        try await store.insertAdWindow(first)
        try await store.insertAdWindow(second)
        await orchestrator.receiveAdWindows([first, second])
        #expect(await orchestrator.activeSuggestWindowIDs() == ["sug-a", "sug-b"])

        let gate = ControlledAsyncGate()
        await orchestrator._setRevertPersistenceBarrierForTesting {
            await gate.wait()
        }
        let gesture = Task {
            await orchestrator.revertByTimeRange(
                start: 50, end: 130, podcastId: "podcast-1"
            )
        }
        await gate.waitUntilStarted()

        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2", episodeId: "ep-2", podcastId: "podcast-2"
        )
        // A suggestion the replacement episode owns, well outside the parked
        // gesture's range. It is the SENTINEL: retiring it on demand after the
        // gesture finishes turns the negative assertion below into an ordering
        // question on one continuation instead of a wall-clock guess, which
        // would otherwise pass spuriously whenever a loaded machine delayed a
        // straggler past the probe's deadline.
        let sentinel = makeSkipTestMarkOnlyWindow(
            id: "sug-sentinel", assetId: "asset-2", startTime: 200, endTime: 220
        )
        try await store.insertAdWindow(sentinel)
        await orchestrator.receiveAdWindows([sentinel])
        #expect(await orchestrator.activeSuggestWindowIDs().contains("sug-sentinel"))

        // Subscribe AFTER the switch so the probe sees only what the parked
        // gesture emits once it resumes. The first iteration's retirement was
        // emitted before the barrier and belongs to the old lifecycle.
        let probe = BoundedStreamProbe(await orchestrator.bannerEventStream())
        await orchestrator._setRevertPersistenceBarrierForTesting(nil)
        let pushesBeforeResume = cuePushes.count
        await gate.release()
        await gesture.value

        let receipts = try await awaitCorrectionReceipts(
            store,
            orchestrator: orchestrator,
            correctionStore: correctionStore,
            assetId: "asset-1",
            expected: 1
        )
        #expect(receipts.count == 1, "the captured show's receipt must still commit")
        #expect(receipts.first?.source == .manualVeto)

        #expect(
            try await controllerRowsExcludingBarrier(controllerStore, orchestrator) == 0,
            "a suggest-only revert never altered playback — too weak to raise the threshold"
        )

        #expect(
            cuePushes.count == pushesBeforeResume,
            "the final lifecycle guard must still gate evaluateAndPush()"
        )

        // Retire the sentinel. Both it and any straggler the parked gesture
        // emitted share one continuation, so FIFO makes the assertion exact:
        // the first retirement to arrive must be the sentinel's.
        await orchestrator.revertByTimeRange(
            start: 195, end: 225, podcastId: "podcast-2"
        )
        var firstRetirement: AdBannerRetirement?
        while let event = await probe.next() {
            if case let .retireWindow(retirement) = event {
                firstRetirement = retirement
                break
            }
        }
        let retired = try #require(
            firstRetirement,
            "the sentinel retirement never arrived — this test's own probe is broken"
        )
        #expect(
            retired.windowId == "sug-sentinel",
            """
            The replaced gesture kept mutating live suggest state and retired \
            "\(retired.windowId)" — an OLD window id stamped with the \
            REPLACEMENT episode's identity. The suggest loop must stop at its \
            lifecycle guard.
            """
        )
        await controllerStore.close()
    }

    // MARK: - playhead-i08e (tenth pass): the other two calibrating seams
    //
    // The four tests above cover `recordListenRevert` and `revertByTimeRange`.
    // The remaining seams that write to the per-show threshold controller —
    // `acceptSuggestedSkip` (MISS) and `denyAutoSkippedBanner`
    // (FALSE-POSITIVE) — make the SAME capture-vs-live claim in their own
    // comments and had nothing pinning it. Both were probed by mutation and
    // both survived the focused set:
    //
    //   • relocating either seam's controller write below its lifecycle guard;
    //   • attributing `acceptSuggestedSkip`'s MISS to `activePodcastId`
    //     instead of the `sourcePodcastId` captured before the first
    //     suspension.
    //
    // `revertWindowPersistsSourceFeedbackAcrossEpisodeReplacement` and
    // `suggestYesPersistsSourceFeedbackAcrossEpisodeReplacement` read like
    // they would catch this and do not: both gate a FIRE-AND-FORGET hop (the
    // post-commit derived-learning task, the trust task) and wait for the
    // gesture to RETURN before replacing the episode, so at calibration time
    // the original episode is still live. Only a barrier taken inside the
    // seam's own body puts the replacement in the right place, which is why
    // these two are here rather than bolted onto those.

    /// `acceptSuggestedSkip` — the MISS side of the controller contract, and
    /// the fourth of the four seams playhead-i08e revived. It reads its show
    /// ONCE before the first suspension (`sourcePodcastId`) and fires the
    /// signal above its lifecycle guard, because "Calibration belongs to the
    /// captured source show even if playback moved to another episode during
    /// persistence" — the seam's own words.
    ///
    /// A miss LOWERS a show's threshold, so a misrouted one makes an unrelated
    /// show more aggressive on the strength of a gesture that was never about
    /// it: the inverse of the false-positive misrouting the revert races pin.
    @Test(
        "A suggest Yes whose episode is replaced mid-flight calibrates the captured show",
        .timeLimit(.minutes(1))
    )
    func suggestAcceptSurvivesEpisodeReplacement() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-2", episodeId: "ep-2")
        )
        let controllerStore = try makeTestControllerStore(
            prefix: "i08e-accept-race"
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "ep-1", podcastId: "podcast-1"
        )

        // playhead-ynmk: byte-exact edges. The positive control below reads the
        // durable applied+skipped row to prove the gesture really committed, so
        // the confirmation needs a late-safe extent to skip.
        let suggestion = makeSkipTestMarkOnlyWindow(
            id: "sug-accept-race",
            startEdgeAnchor: .rediffByteExact,
            endEdgeAnchor: .rediffByteExact
        )
        try await store.insertAdWindow(suggestion)
        await orchestrator.receiveAdWindows([suggestion])
        #expect(
            await orchestrator.activeSuggestWindowIDs().contains("sug-accept-race")
        )

        let gate = ControlledAsyncGate()
        await orchestrator._setSuggestPersistenceBarrierForTesting {
            await gate.wait()
        }
        let gesture = Task {
            await orchestrator.acceptSuggestedSkip(
                windowId: "sug-accept-race",
                ifCurrentEpisodeId: "ep-1"
            )
        }
        await gate.waitUntilStarted()

        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2", episodeId: "ep-2", podcastId: "podcast-2"
        )
        await orchestrator._setSuggestPersistenceBarrierForTesting(nil)
        await gate.release()
        #expect(
            await gesture.value,
            "the captured episode still owns the asset, so the promotion commits"
        )

        let state = try await awaitControllerSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: "podcast-1",
            expected: 1
        )
        #expect(
            state.sampleCount == 1,
            "one acceptance must record exactly one controller sample"
        )
        #expect(
            state.integral == -1,
            "accepting a suggested (missed) ad is a MISS signal → integral −1"
        )
        #expect(
            try await controllerRowsExcludingBarrier(
                controllerStore, orchestrator
            ) == 1,
            """
            The MISS reached a second show. The show captured at gesture time \
            owns this calibration; the replacement must not be tuned by a \
            gesture that predates it.
            """
        )

        // Positive control: the gesture really did its durable work, so "one
        // sample" cannot be satisfied by a seam that aborted early.
        let rows = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(
            rows.first { $0.id == "sug-accept-race" }?.decisionState
                == AdDecisionState.suppressed.rawValue,
            "the accepted suggestion's original row must be retired"
        )
        #expect(
            rows.contains {
                $0.id != "sug-accept-race"
                    && $0.wasSkipped
                    && $0.decisionState == AdDecisionState.applied.rawValue
            },
            "the promoted applied row is this gesture's durable work"
        )
        await controllerStore.close()
    }

    /// `denyAutoSkippedBanner` — with `revertByTimeRange`, one of only two
    /// calibrating seams a shipped build actually reaches (see the census
    /// above `declineSuggestedSkip`), and the one whose ordering nothing
    /// pinned.
    ///
    /// Its trust penalty is deliberately not asserted here: unlike
    /// `recordListenRevert`'s, it is issued from an unstructured `Task` rather
    /// than awaited inside the gesture, so pinning it would need a polling
    /// helper of its own. The controller sample is enough to reject the
    /// mutation this test exists for — moving the calibration below the
    /// ownership guard moves BOTH effects together.
    @Test(
        "A banner No whose episode is replaced mid-flight calibrates the captured show",
        .timeLimit(.minutes(1))
    )
    func denyAutoSkippedBannerSurvivesEpisodeReplacement() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-2", episodeId: "ep-2")
        )
        let controllerStore = try makeTestControllerStore(
            prefix: "i08e-deny-race"
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 9
        )
        let probe = BoundedStreamProbe(await orchestrator.bannerEventStream())

        let applied = makeSkipTestAdWindow(
            id: "ad-deny-race",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.applied.rawValue
        )
        try await store.insertAdWindow(applied)
        await orchestrator.receiveAdWindows([applied])
        guard case let .present(card) = await probe.next() else {
            Issue.record("Expected an applied auto-skip card")
            return
        }

        let gate = ControlledAsyncGate()
        await orchestrator._setFeedbackPersistenceBarrierForTesting {
            await gate.wait()
        }
        let gesture = Task {
            await orchestrator.denyAutoSkippedBanner(
                windowId: card.windowId,
                analysisAssetId: card.analysisAssetId,
                startTime: card.adStartTime,
                endTime: card.adEndTime,
                podcastId: card.podcastId,
                ifCurrentEpisodeId: card.episodeId,
                ifPlaybackLifecycleGeneration:
                    card.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    card.windowMaterialRevisionToken
            )
        }
        await gate.waitUntilStarted()

        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2", episodeId: "ep-2", podcastId: "podcast-2"
        )
        await orchestrator._setFeedbackPersistenceBarrierForTesting(nil)
        await gate.release()
        #expect(
            await gesture.value,
            "the captured episode still owns the asset, so the No commits"
        )

        let state = try await awaitControllerSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: "podcast-1",
            expected: 1
        )
        #expect(
            state.sampleCount == 1,
            "one denial must record exactly one controller sample"
        )
        #expect(
            state.integral == 1,
            "an explicit banner No is a FALSE-POSITIVE signal → integral +1"
        )
        #expect(
            try await controllerRowsExcludingBarrier(
                controllerStore, orchestrator
            ) == 1,
            """
            The sample reached a second show. The card's own podcastId is the \
            captured show's; a replacement episode must not inherit its \
            calibration.
            """
        )

        // Positive control: the durable No landed, so "one sample" cannot be
        // satisfied by a gesture that bailed out at its ownership check. Same
        // pair `autoSkipNoPersistsWithoutCorrectionStore` asserts — the denial
        // retires the row while keeping the record that the skip DID fire.
        let row = try #require(try await store.fetchAdWindow(id: "ad-deny-race"))
        #expect(row.decisionState == AdDecisionState.reverted.rawValue)
        #expect(row.wasSkipped, "the denied skip is still recorded as having fired")
        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: "asset-1"
        )
        #expect(receipts.count == 1, "the captured show's receipt must still commit")
        #expect(receipts.first?.source == .bannerAutoSkipDenied)
        await controllerStore.close()
    }
}

/// Bounded stream reader for ordering tests. A missing event reports as `nil`
/// after the deadline instead of parking the entire test process forever.
private final class BoundedStreamProbe<Element: Sendable>: @unchecked Sendable {
    private actor State {
        private var buffered: [Element] = []
        private var waiters:
            [UUID: CheckedContinuation<Element?, Never>] = [:]
        private var isFinished = false

        func push(_ element: Element) {
            if let (id, waiter) = waiters.first {
                waiters.removeValue(forKey: id)
                waiter.resume(returning: element)
            } else {
                buffered.append(element)
            }
        }

        func finish() {
            isFinished = true
            let pending = waiters.values
            waiters.removeAll()
            for waiter in pending {
                waiter.resume(returning: nil)
            }
        }

        func next(timeout: Duration) async -> Element? {
            if !buffered.isEmpty {
                return buffered.removeFirst()
            }
            guard !isFinished else { return nil }

            let id = UUID()
            return await withCheckedContinuation { continuation in
                waiters[id] = continuation
                Task {
                    try? await Task.sleep(for: timeout)
                    self.timeout(id: id)
                }
            }
        }

        private func timeout(id: UUID) {
            guard let waiter = waiters.removeValue(forKey: id) else {
                return
            }
            waiter.resume(returning: nil)
        }
    }

    private let state: State
    private let timeout: Duration
    private let observationTask: Task<Void, Never>

    init(
        _ stream: AsyncStream<Element>,
        timeout: Duration = .seconds(2)
    ) {
        let state = State()
        self.state = state
        self.timeout = timeout
        observationTask = Task {
            for await element in stream {
                guard !Task.isCancelled else { break }
                await state.push(element)
            }
            await state.finish()
        }
    }

    deinit {
        observationTask.cancel()
    }

    func next() async -> Element? {
        await state.next(timeout: timeout)
    }
}

private actor ControlledAsyncGate {
    private var didStart = false
    private var startWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    func wait() async {
        didStart = true
        let waiters = startWaiters
        startWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
        await withCheckedContinuation { continuation in
            releaseContinuation = continuation
        }
    }

    func waitUntilStarted() async {
        if didStart { return }
        await withCheckedContinuation { continuation in
            startWaiters.append(continuation)
        }
    }

    func release() {
        releaseContinuation?.resume()
        releaseContinuation = nil
    }
}

/// Suspends fully-formed correction writes so lifecycle replacement can be
/// interleaved at the exact persistence boundary exercised by banner actions.
private actor GatedUserCorrectionStore: UserCorrectionStore {
    private let wrapped: any UserCorrectionStore
    private let recordGate: ControlledAsyncGate

    init(
        wrapped: any UserCorrectionStore,
        recordGate: ControlledAsyncGate
    ) {
        self.wrapped = wrapped
        self.recordGate = recordGate
    }

    func recordVeto(span: DecodedSpan) async {
        await wrapped.recordVeto(span: span)
    }

    func recordVeto(
        startTime: Double,
        endTime: Double,
        assetId: String,
        podcastId: String?,
        source: CorrectionSource
    ) async {
        await wrapped.recordVeto(
            startTime: startTime,
            endTime: endTime,
            assetId: assetId,
            podcastId: podcastId,
            source: source
        )
    }

    func record(_ event: CorrectionEvent) async throws {
        await recordGate.wait()
        try await wrapped.record(event)
    }

    func correctionDidPersistAtomically(
        _ event: CorrectionEvent,
        wasNewlyInserted: Bool
    ) async {
        await recordGate.wait()
        await wrapped.correctionDidPersistAtomically(
            event,
            wasNewlyInserted: wasNewlyInserted
        )
    }

    func correctionPassthroughFactor(
        for analysisAssetId: String
    ) async -> Double {
        await wrapped.correctionPassthroughFactor(
            for: analysisAssetId
        )
    }

    func correctionBoostFactor(
        for analysisAssetId: String
    ) async -> Double {
        await wrapped.correctionBoostFactor(for: analysisAssetId)
    }

    func correctionBoostFactor(
        for analysisAssetId: String,
        overlapping startTime: Double,
        endTime: Double
    ) async -> Double {
        await wrapped.correctionBoostFactor(
            for: analysisAssetId,
            overlapping: startTime,
            endTime: endTime
        )
    }
}

@Suite("SkipOrchestrator Revert - Time Range and Banner Paths")
struct SkipOrchestratorRevertTests {

    // MARK: - revertByTimeRange

    @Test("Episode-bound seek rejects a stale same-episode lifecycle token")
    func episodeBoundSeekRejectsStaleLifecycle() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)

        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1"
        )
        let staleGeneration =
            await orchestrator.episodeLifecycleGenerationSnapshot()
        // A replay can preserve the canonical episode ID while replacing all
        // of the orchestrator's in-memory state.
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1"
        )

        let acceptedStaleAction = await orchestrator.recordUserSeek(
            to: 42,
            ifEpisodeLifecycleGeneration: staleGeneration
        )
        #expect(!acceptedStaleAction)

        let currentGeneration =
            await orchestrator.episodeLifecycleGenerationSnapshot()
        let acceptedCurrentAction = await orchestrator.recordUserSeek(
            to: 42,
            ifEpisodeLifecycleGeneration: currentGeneration
        )
        #expect(acceptedCurrentAction)
    }

    @Test("replacement episode drops the prior skip mode before hydration suspends")
    func replacementEpisodeDefaultsToShadowBeforeHydration() async throws {
        let store = try await makeTestStore()
        let orchestrator = SkipOrchestrator(store: store)
        let hydrationGate = ControlledAsyncGate()
        await orchestrator._setBeginEpisodeHydrationBarrierForTesting {
            await hydrationGate.wait()
        }
        await orchestrator.setActiveSkipMode(.auto)

        let beginTask = Task {
            await orchestrator.beginEpisode(
                analysisAssetId: "replacement-asset",
                episodeId: "replacement-episode",
                podcastId: "replacement-show"
            )
        }
        await hydrationGate.waitUntilStarted()

        #expect(
            await orchestrator.currentSkipMode() == .shadow,
            "new material must not inherit the prior show's auto mode while hydration is suspended"
        )

        await hydrationGate.release()
        await beginTask.value
    }

    @Test("revertByTimeRange reverts overlapping applied window")
    func revertByTimeRangeRevertsOverlapping() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues = ranges
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)

        let ad = makeSkipTestAdWindow(
            id: "ad-range-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])
        guard case .present = await probe.next() else {
            Issue.record("Expected the managed card before time-range revert")
            return
        }

        // Should have a skip cue before revert.
        // playhead-vn7n.2: cue end is pulled in by `adTrailingCushionSeconds`
        // (default 1.0 s) so the player lands slightly inside the ad rather
        // than risking a clip into program audio.
        let cushion = SkipPolicyConfig.default.adTrailingCushionSeconds
        #expect(!pushedCues.isEmpty)
        if let cue = pushedCues.first {
            #expect(CMTimeGetSeconds(cue.start) == 60)
            #expect(CMTimeGetSeconds(cue.end) == 120 - cushion)
        } else {
            Issue.record("Expected the pre-revert cue to preserve the finalized window boundaries")
        }

        // Revert by time range that overlaps the ad window.
        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: "podcast-1")
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record("Time-range revert must retire the managed card")
            return
        }
        #expect(retirement.windowId == ad.id)

        let log = await orchestrator.getDecisionLog()
        let reverted = log.filter {
            $0.decision == .reverted && $0.adWindowId == "ad-range-1"
        }
        #expect(!reverted.isEmpty)

        // Skip cues should be cleared after revert.
        #expect(pushedCues.isEmpty)
    }

    @Test("revertByTimeRange is a no-op when no windows overlap")
    func revertByTimeRangeNoOverlap() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues = ranges
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-range-noop",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        let cuesBefore = pushedCues

        // Revert a time range that does NOT overlap the ad window.
        await orchestrator.revertByTimeRange(start: 200, end: 300, podcastId: "podcast-1")

        let log = await orchestrator.getDecisionLog()
        let reverted = log.filter {
            $0.decision == .reverted && $0.adWindowId == "ad-range-noop"
        }
        #expect(reverted.isEmpty)

        // Cues should remain unchanged.
        #expect(pushedCues.count == cuesBefore.count)
    }

    @Test(
        "recurrence-store failure does not terminalize a veto and retry succeeds"
    )
    func recurrenceFailureLeavesVetoRetryable() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let storage = AnalysisStoreRepeatedAdCacheStorage(store: store)
        let repeatedCache = RepeatedAdCacheService(storage: storage)
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore,
            repeatedAdCache: repeatedCache
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        let ad = makeSkipTestAdWindow(
            id: "ad-recurrence-retry",
            startTime: 60,
            endTime: 90,
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])
        #expect(try await repeatedCache.store(
            showId: "podcast-1",
            fingerprint: RepeatedAdFingerprint(bits: 0x6161),
            boundaryStart: 60,
            boundaryEnd: 90,
            confidence: 0.9,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: ad.analysisAssetId,
            sourceWindowId: ad.id
        ))
        try await store.execForTesting("""
            CREATE TRIGGER fail_orchestrator_recurrence_revocation
            BEFORE INSERT ON repeated_ad_cache_revocations
            BEGIN
                SELECT RAISE(ABORT, 'injected recurrence revocation failure');
            END
            """)

        #expect(
            !(await orchestrator.revertWindow(
                windowId: ad.id,
                podcastId: "podcast-1"
            ))
        )
        #expect(
            try await store.fetchAdWindow(id: ad.id)?.decisionState
                == AdDecisionState.confirmed.rawValue
        )
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: ad.analysisAssetId
            ).isEmpty
        )
        #expect(await orchestrator.activeWindowIDs().contains(ad.id))
        #expect(try await storage.totalCount() == 1)

        try await store.execForTesting(
            "DROP TRIGGER fail_orchestrator_recurrence_revocation"
        )
        #expect(
            await orchestrator.revertWindow(
                windowId: ad.id,
                podcastId: "podcast-1"
            )
        )
        #expect(
            try await store.fetchAdWindow(id: ad.id)?.decisionState
                == AdDecisionState.reverted.rawValue
        )
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: ad.analysisAssetId
            ).count == 1
        )
        #expect(try await storage.totalCount() == 0)
    }

    @Test("revertByTimeRange rejects malformed or reversed gesture bounds")
    func revertByTimeRangeRejectsInvalidBounds() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        await orchestrator.setActiveSkipMode(.manual)
        let ad = makeSkipTestAdWindow(
            id: "round3-invalid-veto-range",
            startTime: 60,
            endTime: 120,
            confidence: 0.9
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        let invalidRanges: [(Double, Double)] = [
            (.nan, 110),
            (70, .infinity),
            (-1, 10),
            (110, 70),
            (70, 70),
        ]
        for (start, end) in invalidRanges {
            #expect(
                !(await orchestrator.revertByTimeRange(
                    start: start,
                    end: end,
                    podcastId: "podcast-1"
                ))
            )
        }
        let mismatchedSpan = DecodedSpan(
            id: "round3-mismatched-correction-span",
            assetId: ad.analysisAssetId,
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 1,
            startTime: 69,
            endTime: 110,
            anchorProvenance: []
        )
        #expect(
            !(await orchestrator.revertByTimeRange(
                start: 70,
                end: 110,
                analysisAssetId: ad.analysisAssetId,
                podcastId: "podcast-1",
                ifCurrentEpisodeId: "asset-1",
                ifPlaybackLifecycleGeneration: nil,
                correctionSpan: mismatchedSpan
            )),
            "the correction material must exactly own the submitted bounds"
        )

        #expect(
            (await orchestrator.activeWindowIDs()).contains(ad.id)
        )
        #expect(
            try await store.fetchAdWindow(id: ad.id)?.decisionState
                == AdDecisionState.confirmed.rawValue
        )
        #expect(
            try await correctionStore.activeCorrections(
                for: ad.analysisAssetId
            ).isEmpty
        )
    }

    @Test("time-range veto rejects a durable same-ID material replacement")
    func timeRangeVetoRejectsSameIDReplacement() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        await orchestrator.setActiveSkipMode(.manual)
        let displayed = makeSkipTestAdWindow(
            id: "round3-range-same-id",
            startTime: 60,
            endTime: 120,
            confidence: 0.9
        )
        try await store.insertAdWindow(displayed)
        await orchestrator.receiveAdWindows([displayed])

        let replacement = makeSkipTestAdWindow(
            id: displayed.id,
            startTime: 180,
            endTime: 240,
            confidence: 0.95
        )
        try await store.insertOrReplaceAdWindow(replacement)

        #expect(
            !(await orchestrator.revertByTimeRange(
                start: 70,
                end: 110,
                podcastId: "podcast-1"
            ))
        )
        #expect(
            (await orchestrator.activeWindowIDs()).contains(displayed.id),
            "a failed exact-material transaction must leave the retryable live source intact"
        )
        let durable = try #require(
            try await store.fetchAdWindow(id: displayed.id)
        )
        #expect(durable.startTime == replacement.startTime)
        #expect(durable.endTime == replacement.endTime)
        #expect(
            durable.decisionState
                == AdDecisionState.confirmed.rawValue
        )
        #expect(
            try await correctionStore.activeCorrections(
                for: displayed.analysisAssetId
            ).isEmpty
        )
    }

    @Test("episode-bound time-range veto cannot target a replacement lifecycle")
    func timeRangeVetoRejectsOldEpisodeContext() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "round3-range-asset-a",
                episodeId: "round3-range-episode-a"
            )
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "round3-range-asset-b",
                episodeId: "round3-range-episode-b"
            )
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        let sharedID = "round3-old-sheet-shared-id"
        let source = makeSkipTestAdWindow(
            id: sharedID,
            assetId: "round3-range-asset-a",
            startTime: 60,
            endTime: 120,
            confidence: 0.9
        )
        try await store.insertAdWindow(source)
        await orchestrator.beginEpisode(
            analysisAssetId: source.analysisAssetId,
            episodeId: "round3-range-episode-a",
            podcastId: "round3-show-a",
            playbackLifecycleGeneration: 41
        )
        await orchestrator.setActiveSkipMode(.manual)
        await orchestrator.receiveAdWindows([source])

        let replacement = makeSkipTestAdWindow(
            id: sharedID,
            assetId: "round3-range-asset-b",
            startTime: 60,
            endTime: 120,
            confidence: 0.95
        )
        try await store.insertOrReplaceAdWindow(replacement)
        await orchestrator.beginEpisode(
            analysisAssetId: replacement.analysisAssetId,
            episodeId: "round3-range-episode-b",
            podcastId: "round3-show-b",
            playbackLifecycleGeneration: 42
        )
        await orchestrator.setActiveSkipMode(.manual)

        #expect(
            !(await orchestrator.revertByTimeRange(
                start: 70,
                end: 110,
                analysisAssetId: source.analysisAssetId,
                podcastId: "round3-show-a",
                ifCurrentEpisodeId: "round3-range-episode-a",
                ifPlaybackLifecycleGeneration: 41
            ))
        )
        let durable = try #require(
            try await store.fetchAdWindow(id: sharedID)
        )
        #expect(durable.analysisAssetId == replacement.analysisAssetId)
        #expect(
            durable.decisionState
                == AdDecisionState.confirmed.rawValue
        )
        #expect(
            try await correctionStore.activeCorrections(
                for: replacement.analysisAssetId
            ).isEmpty
        )
    }

    @Test("time-range veto rolls back every row and correction on SQLite failure")
    func timeRangeVetoRollsBackAtomically() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        await orchestrator.setActiveSkipMode(.manual)
        let ads = [
            makeSkipTestAdWindow(
                id: "round3-atomic-a",
                startTime: 60,
                endTime: 90,
                confidence: 0.9
            ),
            makeSkipTestAdWindow(
                id: "round3-atomic-z",
                startTime: 92,
                endTime: 120,
                confidence: 0.9
            ),
        ]
        for ad in ads {
            try await store.insertAdWindow(ad)
        }
        await orchestrator.receiveAdWindows(ads)
        try await store.execForTesting("""
            CREATE TRIGGER round3_fail_second_range_revert
            BEFORE UPDATE OF decisionState ON ad_windows
            WHEN OLD.id = 'round3-atomic-z'
              AND NEW.decisionState = 'reverted'
            BEGIN
                SELECT RAISE(ABORT, 'round3 injected revert failure');
            END;
            """)

        #expect(
            !(await orchestrator.revertByTimeRange(
                start: 50,
                end: 130,
                podcastId: "podcast-1"
            ))
        )

        for ad in ads {
            #expect(
                try await store.fetchAdWindow(id: ad.id)?.decisionState
                    == AdDecisionState.confirmed.rawValue
            )
            #expect(
                (await orchestrator.activeWindowIDs()).contains(ad.id)
            )
        }
        #expect(
            try await correctionStore.activeCorrections(
                for: "asset-1"
            ).isEmpty
        )
    }

    @Test("revertByTimeRange reverts multiple overlapping windows in batch")
    func revertByTimeRangeMultipleWindows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues = ranges
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad1 = makeSkipTestAdWindow(
            id: "ad-batch-1",
            startTime: 60,
            endTime: 90,
            confidence: 0.9,
            decisionState: "confirmed"
        )
        let ad2 = makeSkipTestAdWindow(
            id: "ad-batch-2",
            startTime: 92,
            endTime: 120,
            confidence: 0.88,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad1)
        try await store.insertAdWindow(ad2)
        await orchestrator.receiveAdWindows([ad1, ad2])

        // Both windows should produce cues.
        #expect(!pushedCues.isEmpty)

        // Revert a broad time range covering both windows.
        await orchestrator.revertByTimeRange(start: 50, end: 130, podcastId: "podcast-1")

        let log = await orchestrator.getDecisionLog()
        let reverted = log.filter { $0.decision == .reverted }
        #expect(reverted.count >= 2)
        #expect(pushedCues.isEmpty)
    }

    @Test("revertByTimeRange skips already-reverted windows")
    func revertByTimeRangeSkipsAlreadyReverted() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-already-reverted",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        // First revert via windowId.
        await orchestrator.recordListenRevert(windowId: "ad-already-reverted", podcastId: "podcast-1")

        let logBefore = await orchestrator.getDecisionLog()
        let revertedBefore = logBefore.filter { $0.decision == .reverted }

        // Second revert by time range — should not produce an additional revert log entry.
        await orchestrator.revertByTimeRange(start: 60, end: 120, podcastId: "podcast-1")

        let logAfter = await orchestrator.getDecisionLog()
        let revertedAfter = logAfter.filter { $0.decision == .reverted }
        #expect(revertedAfter.count == revertedBefore.count)
    }

    // MARK: - revertWindow

    @Test("revertWindow records a public manual veto and generic decision log")
    func revertWindowRemovesCue() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        // Own the trust store: the stale-show probe at the end of this test is
        // a NEGATIVE assertion about the per-show penalty, and the penalty is
        // only observable on the profile it would have decremented.
        let trustStore = try await makeTestStore()
        for show in ["podcast-1", trustWriteBarrierShow] {
            try await seedSkipTestTrustProfile(
                in: trustStore, podcastId: show,
                mode: "auto", trustScore: 0.9, observations: 10
            )
        }
        let trustService = TrustScoringService(store: trustStore)
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues = ranges
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-generic-manual-veto",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        #expect(!pushedCues.isEmpty)
        let decisionCountBeforeFeedback =
            await orchestrator.getDecisionLog().count

        #expect(await orchestrator.revertWindow(
            windowId: ad.id,
            podcastId: "podcast-1"
        ))

        let decisionLog = await orchestrator.getDecisionLog()
        #expect(decisionLog.count == decisionCountBeforeFeedback + 1)
        #expect(
            decisionLog.last?.decision == .reverted
                && decisionLog.last?.adWindowId == ad.id
        )
        let corrections = try await store.loadCorrectionEvents(
            analysisAssetId: ad.analysisAssetId
        )
        #expect(corrections.count == 1)
        let correction = try #require(corrections.first)
        #expect(correction.source == .manualVeto)
        #expect(correction.podcastId == "podcast-1")
        #expect(!correction.isPrivateExplicitFeedbackReceipt)
        #expect(pushedCues.isEmpty)

        // playhead-o4qr: this test used to end with a STALE-SHOW probe that
        // asserted `revertWindow(podcastId: "podcast-stale")` returned false and
        // left no receipt. That refusal is exactly the contract collision the
        // bead resolved, and the resolution reverses it: ACCEPT THE RECEIPT,
        // REFUSE THE LEARNING. A show that disagrees with the live episode is
        // an unusable identity, not a reason to discard what the listener said.
        //
        // The probe is not weakened, it is re-pointed: what "must not authorize
        // feedback for another show" means now is that the SHOW-KEYED half is
        // withheld — the receipt lands UNATTRIBUTED (`podcastId == nil`, not
        // "podcast-stale" and not the live "podcast-1"), and the per-show trust
        // penalty never fires. It also moves to its own window, because a stale
        // veto now genuinely reverts its target and reusing `ad` would leave
        // the happy path above nothing to revert.
        let stale = makeSkipTestAdWindow(
            id: "ad-generic-stale-show",
            startTime: 300,
            endTime: 360,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(stale)
        await orchestrator.receiveAdWindows([stale])
        #expect(!pushedCues.isEmpty, "positive control: the stale probe has a live target")

        #expect(
            await orchestrator.revertWindow(
                windowId: stale.id,
                podcastId: "podcast-stale"
            ),
            "a listener's correction is never dropped for want of a usable show"
        )
        let afterStale = try await store.loadCorrectionEvents(
            analysisAssetId: stale.analysisAssetId
        )
        #expect(afterStale.count == 2, "the unattributable correction is still durable")
        let staleReceipt = try #require(
            afterStale.first { $0.targetRefs?.adWindowId == stale.id }
        )
        #expect(staleReceipt.source == .manualVeto)
        #expect(
            staleReceipt.podcastId == nil,
            """
            A show identity the live episode does not agree with is not an \
            attribution. Stamping either the requested "podcast-stale" or the \
            live "podcast-1" onto this receipt is the cross-contamination the \
            refusal used to prevent by dropping the gesture entirely.
            """
        )
        let staleRow = try #require(try await store.fetchAdWindow(id: stale.id))
        #expect(staleRow.decisionState == AdDecisionState.reverted.rawValue)
        #expect(pushedCues.isEmpty)

        // REFUSE THE LEARNING: the per-show trust penalty has nowhere to land
        // for a correction whose show cannot be established, so "podcast-1"
        // must still carry ONLY the penalty its own correctly-attributed revert
        // earned above. That earlier penalty is also the positive control —
        // `== 1` cannot be satisfied by a trust path that is simply dead.
        //
        // Polled, then barriered — see `awaitTrustFalseSkipSignals`. Two
        // earlier shapes were measured and rejected: watching
        // `falseSkipSignalHandlerForTesting` behind `drainOrchestratorEffects`
        // SURVIVED battery entry O02 (the drain orders only the task's first
        // segment; the handler body runs in a later one), and a bare
        // `drainTrustWrites` failed on the UNMUTATED tree because
        // `revertWindow`'s penalty had not reached the trust actor 11 ms
        // later. Waiting for the positive control is what makes the exactness
        // assertion below meaningful in both directions.
        let profile = try await awaitTrustFalseSkipSignals(
            trustStore,
            service: trustService,
            orchestrator: orchestrator,
            show: "podcast-1",
            expected: 1
        )
        #expect(
            profile.recentFalseSkipSignals == 1,
            """
            Expected exactly the one penalty the correctly-attributed revert \
            earned; got \(profile.recentFalseSkipSignals). A second means the \
            stale-show gesture fell back to the live show and penalised it on \
            the strength of a correction that never named it.
            """
        )
    }

    @Test("generic revertWindow rejects a durable same-ID material replacement")
    func genericRevertRejectsSameIDReplacement() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        await orchestrator.setActiveSkipMode(.manual)
        let displayed = makeSkipTestAdWindow(
            id: "round3-generic-same-id",
            startTime: 60,
            endTime: 120,
            confidence: 0.9
        )
        try await store.insertAdWindow(displayed)
        await orchestrator.receiveAdWindows([displayed])
        let replacement = makeSkipTestAdWindow(
            id: displayed.id,
            startTime: 180,
            endTime: 240,
            confidence: 0.95
        )
        try await store.insertOrReplaceAdWindow(replacement)

        #expect(
            !(await orchestrator.revertWindow(
                windowId: displayed.id,
                podcastId: "podcast-1"
            ))
        )
        #expect(
            (await orchestrator.activeWindowIDs()).contains(displayed.id)
        )
        let durable = try #require(
            try await store.fetchAdWindow(id: displayed.id)
        )
        #expect(durable.startTime == replacement.startTime)
        #expect(durable.endTime == replacement.endTime)
        #expect(
            durable.decisionState
                == AdDecisionState.confirmed.rawValue
        )
        #expect(
            try await correctionStore.activeCorrections(
                for: displayed.analysisAssetId
            ).isEmpty
        )
    }

    @Test("recordListenRevert rejects a durable same-ID material replacement")
    func listenRevertRejectsSameIDReplacement() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        await orchestrator.setActiveSkipMode(.manual)
        let displayed = makeSkipTestAdWindow(
            id: "round3-listen-same-id",
            startTime: 60,
            endTime: 120,
            confidence: 0.9
        )
        try await store.insertAdWindow(displayed)
        await orchestrator.receiveAdWindows([displayed])
        let replacement = makeSkipTestAdWindow(
            id: displayed.id,
            startTime: 180,
            endTime: 240,
            confidence: 0.95
        )
        try await store.insertOrReplaceAdWindow(replacement)

        #expect(
            !(await orchestrator.recordListenRevert(
                windowId: displayed.id,
                podcastId: "podcast-1"
            ))
        )
        #expect(
            (await orchestrator.activeWindowIDs()).contains(displayed.id)
        )
        #expect(
            !(await orchestrator.getDecisionLog()).contains {
                $0.adWindowId == displayed.id && $0.decision == .reverted
            }
        )
        let durable = try #require(
            try await store.fetchAdWindow(id: displayed.id)
        )
        #expect(durable.startTime == replacement.startTime)
        #expect(durable.endTime == replacement.endTime)
        #expect(
            durable.decisionState
                == AdDecisionState.confirmed.rawValue
        )
        #expect(
            try await correctionStore.activeCorrections(
                for: displayed.analysisAssetId
            ).isEmpty
        )
    }

    @Test("auto-skip Yes persists the exact validated receipt and emits no detailed decision record")
    func autoSkipYesPersistsValidatedReceipt() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 91
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let ad = makeSkipTestAdWindow(
            id: "auto-confirmed-window",
            startTime: 60.1234,
            endTime: 75.9876,
            confidence: 0.9
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])
        guard case let .present(item) = await probe.next() else {
            Issue.record("Expected auto-skipped banner material")
            return
        }
        let assetId = try #require(item.analysisAssetId)
        let materialToken = try #require(
            item.windowMaterialRevisionToken
        )
        let decisionCountBeforeFeedback =
            await orchestrator.getDecisionLog().count

        #expect(
            !(await orchestrator.confirmAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: assetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration: 90,
                ifWindowMaterialRevisionToken: materialToken
            )),
            "A stale playback lifecycle must fail closed"
        )
        #expect(
            !(await orchestrator.confirmAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: assetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken: materialToken + "-stale"
            )),
            "A stale same-window material revision must fail closed"
        )
        #expect(
            !(await orchestrator.confirmAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: assetId,
                startTime: item.adStartTime + 0.00001,
                endTime: item.adEndTime,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken: materialToken
            )),
            "A near-distinct span that shares the three-decimal scope must fail exactly"
        )
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: assetId
            ).isEmpty
        )

        #expect(
            await orchestrator.confirmAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: assetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken: materialToken
            )
        )
        let events = try await store.loadCorrectionEvents(
            analysisAssetId: assetId
        )
        let receipt = try #require(events.first)
        #expect(events.count == 1)
        #expect(receipt.source == .bannerAutoSkipConfirmed)
        #expect(receipt.correctionType == .falseNegative)
        #expect(receipt.targetRefs?.adWindowId == item.windowId)
        #expect(receipt.podcastId == "podcast-1")
        #expect(
            receipt.targetRefs?.exactFeedbackSpan?.matches(
                startTime: 60.1234,
                endTime: 75.9876
            ) == true
        )
        if case let .exactTimeSpan(receiptAssetId, start, end) =
            CorrectionScope.deserialize(receipt.scope) {
            #expect(receiptAssetId == assetId)
            #expect(start == 60.123)
            #expect(end == 75.988)
        } else {
            Issue.record("Expected exact auto-skip confirmation span")
        }
        #expect(
            await orchestrator.getDecisionLog().count
                == decisionCountBeforeFeedback,
            "Auto-skip Yes must not add a detailed decision-log record"
        )
    }

    @Test(
        "Auto-No commits while applied persistence is blocked and late apply cannot resurrect it",
        .timeLimit(.minutes(1))
    )
    func autoSkipNoWinsBlockedAppliedPersistenceRace() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        let appliedGate = ControlledAsyncGate()
        await orchestrator._setAppliedPersistenceBarrierForTesting {
            await appliedGate.wait()
        }
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 920
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let producer = makeSkipTestAdWindow(
            id: "auto-no-blocked-applied",
            startTime: 60.1234,
            endTime: 75.9876,
            confidence: 0.9
        )
        try await store.insertAdWindow(producer)
        await orchestrator.receiveAdWindows([producer])
        await appliedGate.waitUntilStarted()
        guard case let .present(item) = await probe.next() else {
            Issue.record("Expected auto-skip feedback card")
            return
        }
        let durableBeforeAnswer = try #require(
            try await store.fetchAdWindow(id: producer.id)
        )
        #expect(
            durableBeforeAnswer.decisionState
                == producer.decisionState,
            "The test must answer before the fire-and-forget applied write"
        )
        #expect(!durableBeforeAnswer.wasSkipped)

        #expect(
            !(await orchestrator.denyAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: item.analysisAssetId,
                startTime: item.adStartTime + 0.00001,
                endTime: item.adEndTime,
                podcastId: item.podcastId,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    item.windowMaterialRevisionToken
            )),
            "A near-distinct span that shares the three-decimal scope must fail exactly"
        )
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: producer.analysisAssetId
            ).isEmpty
        )
        // playhead-o4qr: a second probe used to sit here — the same call with
        // `podcastId: "podcast-stale"` — asserting it returned false and left
        // no receipt ("exact card material must not authorize feedback for
        // another show"). It CANNOT stay in this test, and not because the
        // contract was dropped: under ACCEPT THE RECEIPT, REFUSE THE LEARNING a
        // mismatched show now commits an unattributed receipt and durably
        // reverts the row, which would consume the very applied window whose
        // race this test exists to pin, leaving the real No below nothing to
        // answer.
        //
        // The contract moved to a test that can assert its NEW shape in full —
        // receipt lands with `podcastId == nil`, no controller sample, no trust
        // penalty: `staleShowBannerNoKeepsReceiptAndRecordsNoLearning` in
        // SkipOrchestratorThresholdControlTests. What remains here is the
        // near-distinct-SPAN probe above, whose refusal is span-based and
        // untouched by the show-identity split.
        #expect(
            await orchestrator.denyAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: item.analysisAssetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                podcastId: item.podcastId,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    item.windowMaterialRevisionToken
            )
        )
        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: producer.analysisAssetId
        )
        let receipt = try #require(receipts.first)
        #expect(receipts.count == 1)
        #expect(
            receipt.targetRefs?.exactFeedbackSpan?.matches(
                startTime: 60.1234,
                endTime: 75.9876
            ) == true
        )
        var finalRow = try #require(
            try await store.fetchAdWindow(id: producer.id)
        )
        #expect(
            finalRow.decisionState == AdDecisionState.reverted.rawValue
        )
        #expect(
            finalRow.wasSkipped,
            "The atomic No transaction must durably record the skip before reverting it"
        )

        let outward = try #require(
            try await store.responseIndependentAdWindows(
                analysisAssetId: producer.analysisAssetId
            )
        )
        #expect(outward.count == 1)
        #expect(outward[0].id == producer.id)
        #expect(outward[0].startTime == producer.startTime)
        #expect(outward[0].endTime == producer.endTime)
        #expect(!outward[0].wasSkipped)
        #expect(
            outward[0].decisionState == producer.decisionState,
            "The outward shape must match the response-independent producer baseline"
        )

        await appliedGate.release()
        #expect(
            try await store.persistAppliedAdWindowIfEligible(
                windowId: producer.id,
                analysisAssetId: producer.analysisAssetId,
                expectedProducerRevision: producer
            ) == false,
            "A late applied transition must be terminal-safe"
        )
        finalRow = try #require(
            try await store.fetchAdWindow(id: producer.id)
        )
        #expect(
            finalRow.decisionState == AdDecisionState.reverted.rawValue
        )
        #expect(finalRow.wasSkipped)
    }

    // MARK: - playhead-i08e: an unwired correction store must NOT veto feedback
    //
    // `UserCorrectionStore` is the POST-COMMIT derived-learning listener
    // (`schedulePostCommitCorrectionLearning`), not the durability owner — the
    // receipt is appended inside the same AnalysisStore transaction as the
    // AdWindow mutation. Between 5c1a167e and playhead-i08e the two correction
    // BUILDERS opened with `guard correctionStore != nil else { throw }`, so an
    // orchestrator without that optional listener aborted every explicit
    // response at its first statement: no receipt, no row flip, no trust or
    // threshold-control calibration. Production always injects the store
    // (`PlayheadRuntime` line ~1150, non-optional), so the precondition bought
    // nothing there while silently killing the gesture everywhere else.
    //
    // These three tests previously asserted the OPPOSITE contract ("rejects
    // without a correction store"). They kept passing after the precondition
    // was removed only because their fixture was incoherent: the asset row
    // carried `episodeId: "ep-1"` (the `makeSkipTestAnalysisAsset` default)
    // while `beginEpisode` declared `"episode-1"`, so `feedbackAssetMatches`
    // rejected the write for an unrelated ownership reason. Both the contract
    // and the fixture are corrected here — each seam now declares the real
    // episode id, so the ONLY variable under test is the absent listener.

    @Test("Auto-No persists its durable receipt with no correction store wired")
    func autoSkipNoPersistsWithoutCorrectionStore() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 201
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let applied = makeSkipTestAdWindow(
            id: "nil-store-auto-no",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.applied.rawValue
        )
        try await store.insertAdWindow(applied)
        await orchestrator.receiveAdWindows([applied])
        guard case let .present(item) = await probe.next() else {
            Issue.record("Expected applied auto-skip card")
            return
        }

        #expect(
            await orchestrator.denyAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: item.analysisAssetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                podcastId: item.podcastId,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    item.windowMaterialRevisionToken
            ),
            "an unwired derived-learning listener must not veto an explicit No"
        )
        let row = try #require(try await store.fetchAdWindow(id: applied.id))
        #expect(row.decisionState == AdDecisionState.reverted.rawValue)
        #expect(row.wasSkipped, "the denied skip is still recorded as having fired")
        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: applied.analysisAssetId
        )
        #expect(receipts.count == 1, "the No must commit exactly one durable receipt")
        #expect(receipts.first?.source == .bannerAutoSkipDenied)

        // A denial retires the window in place rather than evicting it, so
        // membership alone holds under BOTH the old rejection contract and this
        // one and would prove nothing. Re-answering the same card is the
        // discriminating check: the row is terminal now, so the retry must be
        // refused and must not mint a second receipt.
        #expect((await orchestrator.activeWindowIDs()).contains(applied.id))
        #expect(
            !(await orchestrator.denyAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: item.analysisAssetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                podcastId: item.podcastId,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    item.windowMaterialRevisionToken
            )),
            "a re-answered card must not write a second receipt"
        )
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: applied.analysisAssetId
            ).count == 1
        )
    }

    @Test("Suggest-Yes persists its durable receipt with no correction store wired")
    func suggestYesPersistsWithoutCorrectionStore() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 202
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let suggestion = makeSuggestWindow(id: "nil-store-suggest-yes")
        try await store.insertAdWindow(suggestion)
        await orchestrator.receiveAdWindows([suggestion])
        await enterSuggestSpan(orchestrator)
        guard case let .present(item) = await probe.next() else {
            Issue.record("Expected suggestion card")
            return
        }

        #expect(
            await orchestrator.acceptSuggestedSkip(
                windowId: item.windowId,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifSuggestionRevisionToken:
                    item.suggestionRevisionToken
            ),
            "an unwired derived-learning listener must not veto an explicit Yes"
        )
        let row = try #require(
            try await store.fetchAdWindow(id: suggestion.id)
        )
        #expect(
            row.decisionState == AdDecisionState.suppressed.rawValue,
            "the original suggest row is retired in favour of the promoted window"
        )
        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: suggestion.analysisAssetId
        )
        #expect(receipts.count == 1, "the Yes must commit exactly one durable receipt")
        #expect(receipts.first?.source == .bannerSuggestionConfirmed)
        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(suggestion.id)
        )
        #expect(!(await orchestrator.activeWindowIDs()).isEmpty)
    }

    @Test("Suggest-No persists its durable receipt with no correction store wired")
    func suggestNoPersistsWithoutCorrectionStore() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 203
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let suggestion = makeSuggestWindow(id: "nil-store-suggest-no")
        try await store.insertAdWindow(suggestion)
        await orchestrator.receiveAdWindows([suggestion])
        await enterSuggestSpan(orchestrator)
        guard case let .present(item) = await probe.next() else {
            Issue.record("Expected suggestion card")
            return
        }

        #expect(
            await orchestrator.declineSuggestedSkip(
                windowId: item.windowId,
                isExplicitDenial: true,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifSuggestionRevisionToken:
                    item.suggestionRevisionToken
            ),
            "an unwired derived-learning listener must not veto an explicit No"
        )
        let row = try #require(
            try await store.fetchAdWindow(id: suggestion.id)
        )
        #expect(row.decisionState == AdDecisionState.reverted.rawValue)
        #expect(row.userDismissedBanner)
        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: suggestion.analysisAssetId
        )
        #expect(receipts.count == 1, "the No must commit exactly one durable receipt")
        #expect(receipts.first?.source == .bannerSuggestionDenied)
        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(suggestion.id)
        )
    }

    /// playhead-i08e: the guard that ACTUALLY refuses an explicit response is
    /// `AnalysisStore.feedbackAssetMatches` — the acting card's episode must own
    /// the asset row. All FOUR explicit-feedback transactions share it
    /// (`persistDeniedAutoSkip`, `persistConfirmedAutoSkip`,
    /// `persistAcceptedSuggestionIfCurrent`,
    /// `persistDeclinedSuggestionIfCurrent`) and each is asserted below, so
    /// deleting the clause from any one of them fails here.
    ///
    /// This is pinned deliberately: the three tests above used to exercise this
    /// rejection by ACCIDENT — their asset row carried the fixture's default
    /// `episodeId` while `beginEpisode` declared another — which is what let
    /// them keep passing under the name "rejects without a correction store"
    /// after that precondition was deleted. Fixing those fixtures would have
    /// left the ownership check with no coverage at this layer.
    @Test("explicit responses are refused when the card's episode does not own the asset")
    func explicitResponseRequiresAssetEpisodeOwnership() async throws {
        let store = try await makeTestStore()
        // The asset row belongs to "episode-owner"; the orchestrator (and so
        // every card it emits) claims "episode-impostor".
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-owner")
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-impostor",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 204
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        // Each seam gets its OWN window. Sharing one would let the first
        // refusal make the row terminal, so a later seam would then be refused
        // for a reason other than ownership and its assertion would stop
        // discriminating.
        let denyTarget = makeSkipTestAdWindow(
            id: "foreign-episode-auto-no",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.applied.rawValue
        )
        let confirmTarget = makeSkipTestAdWindow(
            id: "foreign-episode-auto-yes",
            startTime: 400,
            endTime: 460,
            confidence: 0.9,
            decisionState: AdDecisionState.applied.rawValue
        )
        let acceptTarget = makeSuggestWindow(
            id: "foreign-episode-suggest-yes",
            startTime: 200,
            endTime: 260
        )
        let declineTarget = makeSuggestWindow(
            id: "foreign-episode-suggest-no",
            startTime: 300,
            endTime: 360
        )
        let all = [denyTarget, confirmTarget, acceptTarget, declineTarget]
        for window in all {
            try await store.insertAdWindow(window)
        }
        await orchestrator.receiveAdWindows(all)

        // Several windows are ingested, so collect presentations until both
        // auto-skip cards have arrived rather than assuming an emission order.
        // Only an exhausted stream ends the loop: an event of some OTHER kind
        // is skipped rather than treated as the end, so a retirement emitted
        // between the two presentations cannot strand this at one card and
        // fail the test for a reason unrelated to the contract it names.
        var cards: [String: AdSkipBannerItem] = [:]
        let autoSkipIds = Set([denyTarget.id, confirmTarget.id])
        for _ in 0..<8 where !autoSkipIds.isSubset(of: Set(cards.keys)) {
            guard let event = await probe.next() else { break }
            if case let .present(candidate) = event {
                cards[candidate.windowId] = candidate
            }
        }
        guard let denyCard = cards[denyTarget.id],
              let confirmCard = cards[confirmTarget.id]
        else {
            Issue.record("Expected an applied auto-skip card for both windows")
            return
        }

        // 1/4 — persistDeniedAutoSkip.
        #expect(
            !(await orchestrator.denyAutoSkippedBanner(
                windowId: denyCard.windowId,
                analysisAssetId: denyCard.analysisAssetId,
                startTime: denyCard.adStartTime,
                endTime: denyCard.adEndTime,
                podcastId: denyCard.podcastId,
                ifCurrentEpisodeId: denyCard.episodeId,
                ifPlaybackLifecycleGeneration:
                    denyCard.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    denyCard.windowMaterialRevisionToken
            )),
            "a card whose episode does not own the asset may not write a No receipt"
        )
        // 2/4 — persistConfirmedAutoSkip.
        #expect(
            !(await orchestrator.confirmAutoSkippedBanner(
                windowId: confirmCard.windowId,
                analysisAssetId: confirmCard.analysisAssetId,
                startTime: confirmCard.adStartTime,
                endTime: confirmCard.adEndTime,
                ifCurrentEpisodeId: confirmCard.episodeId,
                ifPlaybackLifecycleGeneration:
                    confirmCard.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    confirmCard.windowMaterialRevisionToken
            )),
            "a card whose episode does not own the asset may not write a Yes receipt"
        )
        // 3/4 — persistAcceptedSuggestionIfCurrent.
        #expect(
            !(await orchestrator.acceptSuggestedSkip(
                windowId: acceptTarget.id,
                ifCurrentEpisodeId: "episode-impostor"
            )),
            "a suggest Yes may not promote a window the episode does not own"
        )
        // 4/4 — persistDeclinedSuggestionIfCurrent.
        #expect(
            !(await orchestrator.declineSuggestedSkip(
                windowId: declineTarget.id,
                isExplicitDenial: true,
                ifCurrentEpisodeId: "episode-impostor"
            )),
            "a suggest No may not veto a window the episode does not own"
        )

        // Every durable surface is untouched by all four refusals.
        for expected in all {
            let row = try #require(try await store.fetchAdWindow(id: expected.id))
            #expect(
                row.decisionState == expected.decisionState,
                "\(expected.id) must keep its pre-response decision state"
            )
            #expect(!row.userDismissedBanner)
        }
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: denyTarget.analysisAssetId
            ).isEmpty
        )
        // The rejected suggestions stay live for the surface that DOES own them.
        let suggestIds = await orchestrator.activeSuggestWindowIDs()
        #expect(suggestIds.contains(acceptTarget.id))
        #expect(suggestIds.contains(declineTarget.id))
    }

    @Test("materially revised applied same-ID cards retire stale Yes and No ownership")
    func appliedMaterialRevisionRetiresOldCardActions() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 204
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let revisionA = makeSkipTestAdWindow(
            id: "same-id-material-revision",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.applied.rawValue
        )
        try await store.insertAdWindow(revisionA)
        await orchestrator.receiveAdWindows([revisionA])
        guard case let .present(itemA) = await probe.next() else {
            Issue.record("Expected revision-A card")
            return
        }

        let revisionB = makeSkipTestAdWindow(
            id: revisionA.id,
            startTime: 75,
            endTime: 138,
            confidence: 0.94,
            decisionState: AdDecisionState.applied.rawValue
        )
        try await store.insertOrReplaceAdWindow(revisionB)
        await orchestrator.receiveAdWindows([revisionB])
        guard case let .retireWindow(retirement) = await probe.next()
        else {
            Issue.record("Expected revision-A retirement")
            return
        }
        #expect(retirement.windowId == revisionA.id)
        guard case let .present(itemB) = await probe.next() else {
            Issue.record("Expected revision-B card")
            return
        }
        #expect(
            itemA.windowMaterialRevisionToken
                != itemB.windowMaterialRevisionToken
        )

        #expect(
            !(await orchestrator.confirmAutoSkippedBanner(
                windowId: itemA.windowId,
                analysisAssetId: itemA.analysisAssetId,
                startTime: itemA.adStartTime,
                endTime: itemA.adEndTime,
                ifCurrentEpisodeId: itemA.episodeId,
                ifPlaybackLifecycleGeneration:
                    itemA.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    itemA.windowMaterialRevisionToken
            ))
        )
        #expect(
            !(await orchestrator.denyAutoSkippedBanner(
                windowId: itemA.windowId,
                analysisAssetId: itemA.analysisAssetId,
                startTime: itemA.adStartTime,
                endTime: itemA.adEndTime,
                podcastId: itemA.podcastId,
                ifCurrentEpisodeId: itemA.episodeId,
                ifPlaybackLifecycleGeneration:
                    itemA.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    itemA.windowMaterialRevisionToken
            ))
        )
        #expect(
            !(await orchestrator.retireLiveSkipForListen(
                windowId: itemA.windowId,
                analysisAssetId: try #require(itemA.analysisAssetId),
                startTime: itemA.adStartTime,
                endTime: itemA.adEndTime,
                podcastId: itemA.podcastId,
                ifCurrentEpisodeId: itemA.episodeId,
                ifPlaybackLifecycleGeneration:
                    try #require(itemA.playbackLifecycleGeneration),
                ifWindowMaterialRevisionToken:
                    try #require(itemA.windowMaterialRevisionToken)
            )),
            "Listen from an old card must not retire its same-ID replacement"
        )
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: revisionA.analysisAssetId
            ).isEmpty
        )
        #expect(
            try await store.fetchAdWindow(id: revisionB.id)?
                .decisionState == AdDecisionState.applied.rawValue
        )

        #expect(
            await orchestrator.denyAutoSkippedBanner(
                windowId: itemB.windowId,
                analysisAssetId: itemB.analysisAssetId,
                startTime: itemB.adStartTime,
                endTime: itemB.adEndTime,
                podcastId: itemB.podcastId,
                ifCurrentEpisodeId: itemB.episodeId,
                ifPlaybackLifecycleGeneration:
                    itemB.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    itemB.windowMaterialRevisionToken
            )
        )
        let finalRow = try #require(
            try await store.fetchAdWindow(id: revisionB.id)
        )
        #expect(finalRow.decisionState == AdDecisionState.reverted.rawValue)
        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: revisionB.analysisAssetId
        )
        #expect(receipts.count == 1)
        #expect(receipts.first?.source == .bannerAutoSkipDenied)
        #expect(receipts.first?.targetRefs?.adWindowId == revisionB.id)
    }

    @Test(
        "revertWindow removes its cue without awaiting trust calibration",
        .timeLimit(.minutes(1))
    )
    func revertWindowCueRemovalDoesNotWaitForTrust() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        let trustGate = ControlledAsyncGate()
        await orchestrator._setFalseSkipSignalHandlerForTesting { _ in
            await trustGate.wait()
        }
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        await orchestrator.setActiveSkipMode(.auto)

        let ad = makeSkipTestAdWindow(
            id: "ad-revert-held-trust",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])
        #expect(!pushedCues.isEmpty)

        let accepted = await orchestrator.revertWindow(
            windowId: ad.id,
            podcastId: "podcast-1"
        )

        #expect(accepted)
        #expect(
            pushedCues.isEmpty,
            "The stale playback cue must disappear before trust storage finishes"
        )
        await trustGate.waitUntilStarted()
        await trustGate.release()
    }

    @Test("revertWindow persistence failure preserves cue and rejects correction")
    func revertWindowPersistenceFailureIsRetryable() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-generic-veto-failure",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "applied"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])
        #expect(!pushedCues.isEmpty)

        try await store.execForTesting(
            """
            CREATE TRIGGER fail_generic_revert
            BEFORE UPDATE OF decisionState ON ad_windows
            WHEN NEW.id = 'ad-generic-veto-failure'
              AND NEW.decisionState = 'reverted'
            BEGIN
                SELECT RAISE(ABORT, 'injected banner revert failure');
            END
            """
        )

        let accepted = await orchestrator.revertWindow(
            windowId: ad.id,
            podcastId: "podcast-1"
        )
        #expect(
            accepted == false,
            "Production must keep the generic correction retryable when its durable revert fails"
        )
        #expect(
            !pushedCues.isEmpty,
            "A rejected correction must not pretend the active skip cue was corrected"
        )
        #expect((await orchestrator.activeWindowIDs()).contains(ad.id))
        #expect(
            !(await orchestrator.getDecisionLog()).contains {
                $0.adWindowId == ad.id && $0.decision == .reverted
            }
        )
        let row = try #require(
            (try await store.fetchAdWindows(assetId: "asset-1"))
                .first { $0.id == ad.id }
        )
        #expect(row.decisionState == AdDecisionState.applied.rawValue)
        #expect(
            try await correctionStore.activeCorrections(for: "asset-1")
                .isEmpty,
            "A rolled-back revert must not leave a premature correction"
        )

        try await store.execForTesting("DROP TRIGGER fail_generic_revert")
        #expect(
            await orchestrator.revertWindow(
                windowId: ad.id,
                podcastId: "podcast-1"
            )
        )
        let corrections = try await correctionStore.activeCorrections(
            for: "asset-1"
        )
        #expect(corrections.count == 1)
        #expect(corrections.first?.source == .manualVeto)
        #expect(
            corrections.first?.isPrivateExplicitFeedbackReceipt == false
        )
        #expect(
            corrections.first?.targetRefs?.adWindowId == ad.id
        )
        #expect(
            corrections.first?.submissionCount == 1,
            "The failed attempt must not count as a durable submission"
        )
    }

    @Test(
        "revertWindow persists episode A feedback across direct replacement",
        .timeLimit(.minutes(1))
    )
    func revertWindowPersistsSourceFeedbackAcrossEpisodeReplacement() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-1",
                episodeId: "episode-1"
            )
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-2",
                episodeId: "episode-2"
            )
        )
        let persistentCorrections = PersistentUserCorrectionStore(store: store)
        let persistenceGate = ControlledAsyncGate()
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: persistentCorrections
        )
        await orchestrator._setFeedbackPersistenceBarrierForTesting {
            await persistenceGate.wait()
        }
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 301
        )

        let sourceWindow = makeSkipTestAdWindow(
            id: "revert-during-replacement",
            assetId: "asset-1"
        )
        try await store.insertAdWindow(sourceWindow)
        await orchestrator.receiveAdWindows([sourceWindow])

        let response = Task {
            await orchestrator.revertWindow(
                windowId: sourceWindow.id,
                podcastId: "podcast-1",
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 301
            )
        }
        await persistenceGate.waitUntilStarted()
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2",
            episodeId: "episode-2",
            podcastId: "podcast-2",
            playbackLifecycleGeneration: 302
        )
        await persistenceGate.release()

        #expect(await response.value)
        let corrections = try await persistentCorrections.activeCorrections(
            for: "asset-1"
        )
        #expect(corrections.count == 1)
        #expect(corrections.first?.correctionType == .falsePositive)
        let sourceRow = try #require(
            (try await store.fetchAdWindows(assetId: "asset-1"))
                .first { $0.id == sourceWindow.id }
        )
        #expect(
            sourceRow.decisionState == AdDecisionState.reverted.rawValue
        )
        #expect((await orchestrator.activeWindowIDs()).isEmpty)
        #expect(
            (await orchestrator.getDecisionLog()).isEmpty,
            "old-episode feedback must not enter the replacement lifecycle's decision log"
        )
    }

    @Test("revertWindow rejects an in-memory window with no durable row")
    func revertWindowMissingDurableRowIsRetryable() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let windowId = "missing-durable-user-mark"
        await orchestrator.injectUserMarkedAd(
            start: 60,
            end: 120,
            analysisAssetId: "asset-1",
            windowId: windowId
        )
        #expect(!pushedCues.isEmpty)

        let accepted = await orchestrator.revertWindow(
            windowId: windowId,
            podcastId: "podcast-1"
        )

        #expect(!accepted)
        #expect(!pushedCues.isEmpty)
        #expect((await orchestrator.activeWindowIDs()).contains(windowId))
        #expect(
            !(await orchestrator.getDecisionLog()).contains {
                $0.adWindowId == windowId && $0.decision == .reverted
            }
        )
    }

    @Test("revertWindow is a no-op for unknown window ID")
    func revertWindowUnknownId() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        // Should not crash or log a decision for a nonexistent window.
        await orchestrator.revertWindow(windowId: "nonexistent", podcastId: "podcast-1")

        let log = await orchestrator.getDecisionLog()
        #expect(log.isEmpty)
    }

    @Test("revertWindow is idempotent — second call is a no-op")
    func revertWindowIdempotent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-double-revert",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        // First revert commits and logs the generic manual-veto correction.
        let firstAccepted = await orchestrator.revertWindow(
            windowId: "ad-double-revert",
            podcastId: "podcast-1"
        )
        #expect(firstAccepted)

        let logAfterFirst = await orchestrator.getDecisionLog()
        let revertedFirst = logAfterFirst.filter {
            $0.decision == .reverted && $0.adWindowId == "ad-double-revert"
        }
        #expect(revertedFirst.count == 1)

        // Second revert is rejected by the already-reverted durable state.
        let secondAccepted = await orchestrator.revertWindow(
            windowId: "ad-double-revert",
            podcastId: "podcast-1"
        )
        #expect(!secondAccepted)

        let logAfterSecond = await orchestrator.getDecisionLog()
        let revertedSecond = logAfterSecond.filter {
            $0.decision == .reverted && $0.adWindowId == "ad-double-revert"
        }
        #expect(
            revertedSecond.count == 1,
            "The rejected duplicate must not add another generic decision record"
        )
    }

    // MARK: - Segment broadcast after revert

    @Test("beginEpisode clears prior cues and segment markers before hydration")
    func beginEpisodePublishesClearedPlaybackState() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "episode-1")
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-2", episodeId: "episode-2")
        )
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var latestCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in
            latestCues = ranges
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1"
        )
        let segmentStream = await orchestrator.appliedSegmentsStream()
        let segmentProbe = BoundedStreamProbe(segmentStream)

        let ad = makeSkipTestAdWindow(
            id: "episode-1-cue",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])
        let activeSegments = await segmentProbe.next()
        #expect(activeSegments?.count == 1)
        #expect(latestCues.count == 1)

        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2",
            episodeId: "episode-2",
            podcastId: "podcast-2"
        )
        let clearedSegments = await segmentProbe.next()
        #expect(clearedSegments?.isEmpty == true)
        #expect(
            latestCues.isEmpty,
            "A direct episode switch must remove the prior item's transport cues"
        )
    }

    @Test("endEpisode clears cues and segment markers without a replacement asset")
    func endEpisodePublishesClearedPlaybackState() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "episode-1")
        )
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var latestCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { latestCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1"
        )
        let segmentStream = await orchestrator.appliedSegmentsStream()
        let segmentProbe = BoundedStreamProbe(segmentStream)

        let ad = makeSkipTestAdWindow(
            id: "episode-1-end-cue",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])
        #expect((await segmentProbe.next())?.count == 1)
        #expect(latestCues.count == 1)

        await orchestrator.endEpisode()

        #expect((await segmentProbe.next())?.isEmpty == true)
        #expect(
            latestCues.isEmpty,
            "a replacement with no asset must not retain the prior item's transport cues"
        )
    }

    @Test("revertByTimeRange broadcasts updated segments to listeners")
    func revertByTimeRangeBroadcastsSegments() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-broadcast",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        // Collect segment updates via the stream.
        let stream = await orchestrator.appliedSegmentsStream()
        nonisolated(unsafe) var receivedSegments: [(start: Double, end: Double)]?

        // Revert the window — this should trigger a broadcast with the window removed.
        await orchestrator.revertByTimeRange(start: 60, end: 120, podcastId: "podcast-1")

        // Read the first emitted value from the stream.
        for await segments in stream {
            receivedSegments = segments
            break
        }

        // The reverted window should no longer appear in segments.
        let overlapping = receivedSegments?.filter { $0.start < 120 && $0.end > 60 } ?? []
        #expect(overlapping.isEmpty)
    }

    // MARK: - playhead-zskc code review I5: one gesture, one correction event

    @Test("revertByTimeRange persists exactly one CorrectionEvent per gesture, even when N windows overlap")
    func revertByTimeRangeWritesOneCorrectionPerGesture() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Three adjacent ad windows, all overlapping the user's 50..130 gesture.
        let ads = [
            makeSkipTestAdWindow(id: "ad-dedupe-1", startTime: 55, endTime: 80,
                                 confidence: 0.9, decisionState: "confirmed"),
            makeSkipTestAdWindow(id: "ad-dedupe-2", startTime: 85, endTime: 105,
                                 confidence: 0.9, decisionState: "confirmed"),
            makeSkipTestAdWindow(id: "ad-dedupe-3", startTime: 110, endTime: 125,
                                 confidence: 0.9, decisionState: "confirmed"),
        ]
        for ad in ads { try await store.insertAdWindow(ad) }
        await orchestrator.receiveAdWindows(ads)

        // One gesture: "none of this is an ad" from 50..130. Carry the
        // popover's source span into the same transaction so causal attribution
        // is retained without a second direct correction write.
        let correctionSpan = DecodedSpan(
            id: "transcript-popover-span",
            assetId: "asset-1",
            firstAtomOrdinal: 4,
            lastAtomOrdinal: 12,
            startTime: 50,
            endTime: 130,
            anchorProvenance: [
                .fmAcousticCorroborated(
                    regionId: "transcript-popover-region",
                    breakStrength: 0.7
                )
            ]
        )
        #expect(
            await orchestrator.revertByTimeRange(
                start: 50,
                end: 130,
                analysisAssetId: "asset-1",
                podcastId: "podcast-1",
                ifCurrentEpisodeId: "asset-1",
                ifPlaybackLifecycleGeneration: nil,
                correctionSpan: correctionSpan
            )
        )

        // The correction and all row mutations commit synchronously in one
        // store transaction; no polling or scheduler timing is involved.
        let corrections =
            try await correctionStore.activeCorrections(for: "asset-1")

        #expect(corrections.count == 1,
                "Three overlapping windows reverted by one gesture must produce exactly one CorrectionEvent, got \(corrections.count)")
        #expect(
            corrections.first?.submissionCount == 1,
            "one transcript gesture must not be counted by both the view and orchestrator"
        )
        #expect(
            corrections.first?.causalSource == .foundationModel,
            "the transaction must preserve popover provenance attribution"
        )
        // And the persisted scope must span the user's gesture, not a window's snapped boundary.
        if let scope = corrections.first.flatMap({ CorrectionScope.deserialize($0.scope) }),
           case .exactTimeSpan(_, let startTime, let endTime) = scope {
            #expect(startTime == 50.0, "persisted start must be the user's gesture start")
            #expect(endTime == 130.0, "persisted end must be the user's gesture end")
        } else {
            Issue.record("Expected exactTimeSpan scope from the revert, got \(corrections.first?.scope ?? "<none>")")
        }
    }

    // MARK: - playhead-hygc.1.8: revertByTimeRange must also handle suggest-tier (markOnly) windows

    @Test("revertByTimeRange reverts overlapping markOnly suggest-tier windows and persists decisionState")
    func revertByTimeRangeRevertsSuggestTierMarkOnlyWindow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Construct a markOnly AdWindow — the suggest-tier surface used by
        // boundary-singleton recall, correction-replay, and any algorithmic
        // path the precision gate demoted from auto-skip.
        let markOnly = AdWindow(
            id: "ad-suggest-1",
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.55,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "test-1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 60,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([markOnly])

        // Sanity: the window is in the suggest tier, not the auto-skip dict.
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-1"),
                "markOnly AdWindow must enter the suggest dictionary")

        // User vetoes via the time-range correction path.
        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: "podcast-1")

        // The suggest-tier entry must be cleared.
        #expect(!(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-1")),
                "vetoed markOnly window must be removed from suggestWindows")

        // A late producer may still hold the pre-veto candidate object. The
        // in-session presentation gate must reject that redelivery rather
        // than depending on a fresh persistence read.
        await orchestrator.receiveAdWindows([markOnly])
        #expect(!(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-1")),
                "late redelivery must not recreate a vetoed suggestion")

        // The persisted decisionState must reflect the user's veto so a
        // subsequent run / replay does not resurface the entry.
        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        let row = persisted.first { $0.id == "ad-suggest-1" }
        #expect(row?.decisionState == AdDecisionState.reverted.rawValue,
                "persisted markOnly window must be in .reverted state, got \(row?.decisionState ?? "<missing>")")

        // And exactly one CorrectionEvent was persisted (one gesture, one event).
        var corrections: [CorrectionEvent] = []
        for _ in 0..<20 {
            corrections = try await correctionStore.activeCorrections(for: "asset-1")
            if !corrections.isEmpty { break }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        #expect(corrections.count == 1,
                "one veto gesture against a markOnly window must produce exactly one CorrectionEvent")
    }

    @Test("revertByTimeRange does not increase auto-skip count when reverting a markOnly window")
    func revertByTimeRangeMarkOnlyDoesNotPromoteToAutoSkip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues = ranges
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let markOnly = AdWindow(
            id: "ad-suggest-noskip",
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.55,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "test-1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 60,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([markOnly])

        // markOnly must not produce a skip cue before the revert.
        #expect(pushedCues.isEmpty,
                "markOnly window must not emit auto-skip cues; got \(pushedCues.count)")

        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: "podcast-1")

        // After revert: still no skip cues. The veto must NEVER promote to auto-skip.
        #expect(pushedCues.isEmpty,
                "veto of markOnly window must not promote to auto-skip; got \(pushedCues.count) cues")
    }

    // R2 (hygc.1.8): hardening test. With multiple suggest-tier entries
    // present, a localized revert must clear ONLY the overlapping entry
    // and leave the others intact. This pins the iteration loop against
    // two failure modes:
    //   * dict-mutation-while-iterating skipping or duplicating entries
    //   * an over-zealous "clear all suggest entries on any revert" bug
    @Test("revertByTimeRange clears only the overlapping suggest-tier entry; non-overlapping entries survive")
    func revertByTimeRangeOnlyClearsOverlappingSuggestEntries() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Three markOnly entries at distinct, non-overlapping ranges.
        let markOnlyIds = ["ad-suggest-A", "ad-suggest-B", "ad-suggest-C"]
        let ranges: [(Double, Double)] = [(60, 120), (300, 360), (900, 960)]
        var markOnlyWindows: [AdWindow] = []
        for (id, range) in zip(markOnlyIds, ranges) {
            let window = AdWindow(
                id: id,
                analysisAssetId: "asset-1",
                startTime: range.0,
                endTime: range.1,
                confidence: 0.55,
                boundaryState: AdBoundaryState.segmentAggregated.rawValue,
                decisionState: AdDecisionState.candidate.rawValue,
                detectorVersion: "test-1",
                advertiser: nil, product: nil, adDescription: nil,
                evidenceText: nil, evidenceStartTime: range.0,
                metadataSource: "none",
                metadataConfidence: nil,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false,
                evidenceSources: nil,
                eligibilityGate: SkipEligibilityGate.markOnly.rawValue
            )
            try await store.insertAdWindow(window)
            markOnlyWindows.append(window)
        }
        await orchestrator.receiveAdWindows(markOnlyWindows)

        let beforeIds = await orchestrator.activeSuggestWindowIDs()
        for id in markOnlyIds {
            #expect(beforeIds.contains(id),
                    "all three markOnly windows must enter the suggest dict; missing \(id)")
        }

        // Veto a span that overlaps ONLY the middle entry (300..360).
        await orchestrator.revertByTimeRange(start: 320, end: 340, podcastId: "podcast-1")

        let afterIds = await orchestrator.activeSuggestWindowIDs()
        #expect(!afterIds.contains("ad-suggest-B"),
                "middle entry must be cleared; afterIds=\(afterIds)")
        #expect(afterIds.contains("ad-suggest-A"),
                "first non-overlapping entry must survive; afterIds=\(afterIds)")
        #expect(afterIds.contains("ad-suggest-C"),
                "third non-overlapping entry must survive; afterIds=\(afterIds)")
        #expect(afterIds.count == 2,
                "exactly one entry must be removed; afterIds=\(afterIds)")

        // Persistence reflects the same partition.
        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        let revertedRow = persisted.first { $0.id == "ad-suggest-B" }
        #expect(revertedRow?.decisionState == AdDecisionState.reverted.rawValue,
                "only middle entry must be persisted as .reverted; got \(revertedRow?.decisionState ?? "<missing>")")
        let untouchedA = persisted.first { $0.id == "ad-suggest-A" }
        let untouchedC = persisted.first { $0.id == "ad-suggest-C" }
        #expect(untouchedA?.decisionState == AdDecisionState.candidate.rawValue,
                "non-overlapping entry A must remain .candidate; got \(untouchedA?.decisionState ?? "<missing>")")
        #expect(untouchedC?.decisionState == AdDecisionState.candidate.rawValue,
                "non-overlapping entry C must remain .candidate; got \(untouchedC?.decisionState ?? "<missing>")")
    }

    // R3 (hygc.1.8): the R2 hardening test exercises the snapshot pattern
    // with only ONE matching entry — which can pass even when the
    // dict-mutation-while-iterating pattern is intact, because removing
    // a single key during a single-pass iteration rarely visibly fails
    // (even though Swift documents it as undefined behavior). To pin
    // R2's snapshot fix against actual regression we need a test where
    // the veto matches MULTIPLE suggest entries: removing N>1 keys
    // mid-iteration is the case where the bug actually manifests
    // (skipping or duplicating entries depending on stdlib hash table
    // state). This test feeds five suggest entries, vetoes a span that
    // overlaps three of them, and asserts every overlapping entry is
    // cleared and every non-overlapping entry survives.
    @Test("revertByTimeRange clears ALL overlapping suggest-tier entries when multiple match")
    func revertByTimeRangeClearsAllOverlappingSuggestEntries() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Five markOnly entries. The veto span 200..500 will overlap
        // entries B, C, D — three removals — so the snapshot pattern is
        // the only correct way to drive the loop. A naive
        // dict-mutation-while-iterating loop would skip an entry or
        // visit one twice depending on stdlib hash placement.
        let entries: [(id: String, range: (Double, Double))] = [
            ("ad-suggest-A", (50, 100)),    // before veto, must survive
            ("ad-suggest-B", (210, 250)),   // inside veto, must be reverted
            ("ad-suggest-C", (300, 350)),   // inside veto, must be reverted
            ("ad-suggest-D", (400, 450)),   // inside veto, must be reverted
            ("ad-suggest-E", (600, 660))    // after veto, must survive
        ]
        var windows: [AdWindow] = []
        for (id, range) in entries {
            let window = AdWindow(
                id: id,
                analysisAssetId: "asset-1",
                startTime: range.0,
                endTime: range.1,
                confidence: 0.55,
                boundaryState: AdBoundaryState.segmentAggregated.rawValue,
                decisionState: AdDecisionState.candidate.rawValue,
                detectorVersion: "test-1",
                advertiser: nil, product: nil, adDescription: nil,
                evidenceText: nil, evidenceStartTime: range.0,
                metadataSource: "none",
                metadataConfidence: nil,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false,
                evidenceSources: nil,
                eligibilityGate: SkipEligibilityGate.markOnly.rawValue
            )
            try await store.insertAdWindow(window)
            windows.append(window)
        }
        await orchestrator.receiveAdWindows(windows)

        let beforeIds = await orchestrator.activeSuggestWindowIDs()
        #expect(beforeIds.count == 5, "all five must enter the suggest dict; got \(beforeIds)")

        // Veto a span overlapping B, C, and D.
        await orchestrator.revertByTimeRange(start: 200, end: 500, podcastId: "podcast-1")

        let afterIds = await orchestrator.activeSuggestWindowIDs()
        #expect(afterIds == ["ad-suggest-A", "ad-suggest-E"],
                "only outside entries A and E must survive; got \(afterIds)")

        // Persistence: all three overlapping entries must be .reverted,
        // both non-overlapping must remain .candidate.
        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        let revertedIds: Set<String> = ["ad-suggest-B", "ad-suggest-C", "ad-suggest-D"]
        for id in revertedIds {
            let row = persisted.first { $0.id == id }
            #expect(row?.decisionState == AdDecisionState.reverted.rawValue,
                    "\(id) must persist as .reverted; got \(row?.decisionState ?? "<missing>")")
        }
        for id in ["ad-suggest-A", "ad-suggest-E"] {
            let row = persisted.first { $0.id == id }
            #expect(row?.decisionState == AdDecisionState.candidate.rawValue,
                    "\(id) must remain .candidate; got \(row?.decisionState ?? "<missing>")")
        }
    }

    // Cycle 1 M2: the strong/weak routing split the R7 docstring deferred
    // has now landed in `SkipOrchestrator.revertByTimeRange`. The behavior
    // is now: a revert that touches ONLY the suggest-tier (markOnly) loop
    // — i.e. no managed auto-skip window was vetoed — routes through
    // `recordWeakFalseSkipSignal` (`weakFalseSignalPenalty`), reflecting
    // that no playback was ever altered. A managed-window revert (with
    // or without a co-occurring suggest revert) still uses the full
    // `falseSignalPenalty`. This test pins the suggest-only path at the
    // weak magnitude; the parallel test below pins the managed path at
    // the full magnitude. Any future re-merge of the two paths must
    // update both tests in the same diff.
    @Test("revertByTimeRange against ONLY a markOnly window decrements trust by weakFalseSignalPenalty")
    func revertByTimeRangeMarkOnlyDecrementsTrustByWeakPenalty() async throws {
        // Inline trust-service construction so we can read skipTrustScore
        // back from the SAME store the service mutates. The default
        // `makeSkipTestTrustService` helper allocates an internal store
        // we can't observe.
        let trustStore = try await makeTestStore()
        let initialTrust: Double = 0.90
        try await trustStore.upsertProfile(
            PodcastProfile(
                podcastId: "podcast-1",
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: initialTrust,
                observationCount: 10,
                mode: "auto",
                recentFalseSkipSignals: 0
            )
        )
        let trustConfig = TrustScoringConfig.default
        let trustService = TrustScoringService(store: trustStore, config: trustConfig)

        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let markOnly = AdWindow(
            id: "ad-suggest-magnitude",
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.55,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "test-1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 60,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([markOnly])

        // Sanity: only the suggest tier carries the entry; auto-skip
        // dict is empty.
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-magnitude"))
        #expect(!(await orchestrator.activeWindowIDs().contains("ad-suggest-magnitude")))

        // Veto.
        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: "podcast-1")

        // The trust hit fires inside an unstructured Task — poll until
        // the signal is observed.
        var profile: PodcastProfile?
        for _ in 0..<20 {
            profile = try await trustStore.fetchProfile(podcastId: "podcast-1")
            if let p = profile, p.recentFalseSkipSignals == 1 { break }
            try await Task.sleep(nanoseconds: 50_000_000)
        }

        // Cycle 1 M2: suggest-only revert now uses the WEAK penalty.
        let expectedTrust = initialTrust - trustConfig.weakFalseSignalPenalty
        #expect(abs((profile?.skipTrustScore ?? -1) - expectedTrust) < 1e-6,
                "suggest-tier-only revert must decrement trust by weakFalseSignalPenalty (\(trustConfig.weakFalseSignalPenalty)); got skipTrustScore=\(profile?.skipTrustScore ?? -1) expected=\(expectedTrust)")
        #expect(profile?.recentFalseSkipSignals == 1,
                "exactly one false-skip signal must be recorded for one veto gesture")
    }

    // R9 (hygc.1.8): managed-window symmetric trust-magnitude pin. The
    // R7 test above pins suggest-tier-only revert at the full
    // `falseSignalPenalty`. Without a parallel pin on the managed-window
    // path, a future weak/strong split could quietly land on ONLY one
    // surface — the symmetric pair makes any such split fail loudly on
    // both tests in the same diff. This test mirrors the suggest-tier
    // shape exactly except the AdWindow has `eligibilityGate = nil` (so
    // it lands in the managed `windows` dict, not `suggestWindows`) and
    // therefore the suggest-tier loop in `revertByTimeRange` is a no-op
    // on this fixture — only the managed loop fires.
    @Test("revertByTimeRange against ONLY a managed (auto-skip) window decrements trust by full falseSignalPenalty")
    func revertByTimeRangeManagedDecrementsTrustByFullPenalty() async throws {
        let trustStore = try await makeTestStore()
        let initialTrust: Double = 0.90
        try await trustStore.upsertProfile(
            PodcastProfile(
                podcastId: "podcast-1",
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: initialTrust,
                observationCount: 10,
                mode: "auto",
                recentFalseSkipSignals: 0
            )
        )
        let trustConfig = TrustScoringConfig.default
        let trustService = TrustScoringService(store: trustStore, config: trustConfig)

        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // `makeSkipTestAdWindow` defaults `eligibilityGate = nil` →
        // non-fusion producer → flows through to the managed `windows`
        // dict (not `suggestWindows`).
        let managed = makeSkipTestAdWindow(
            id: "ad-managed-magnitude",
            assetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.75,
            decisionState: AdDecisionState.candidate.rawValue
        )
        try await store.insertAdWindow(managed)
        await orchestrator.receiveAdWindows([managed])

        // Sanity: this window is in the auto-skip dict, NOT suggest tier.
        #expect(await orchestrator.activeWindowIDs().contains("ad-managed-magnitude"),
                "managed AdWindow must enter the auto-skip windows dict")
        #expect(!(await orchestrator.activeSuggestWindowIDs().contains("ad-managed-magnitude")),
                "managed AdWindow must NOT enter the suggest dict")

        // Veto.
        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: "podcast-1")

        // Trust hit fires inside an unstructured Task — poll.
        var profile: PodcastProfile?
        for _ in 0..<20 {
            profile = try await trustStore.fetchProfile(podcastId: "podcast-1")
            if let p = profile, p.recentFalseSkipSignals == 1 { break }
            try await Task.sleep(nanoseconds: 50_000_000)
        }

        let expectedTrust = initialTrust - trustConfig.falseSignalPenalty
        #expect(abs((profile?.skipTrustScore ?? -1) - expectedTrust) < 1e-6,
                "managed-window-only revert must decrement trust by the full falseSignalPenalty (\(trustConfig.falseSignalPenalty)); got skipTrustScore=\(profile?.skipTrustScore ?? -1) expected=\(expectedTrust)")
        #expect(profile?.recentFalseSkipSignals == 1,
                "exactly one false-skip signal must be recorded for one veto gesture")
    }

    // Cycle 2 M3: mixed-revert pin. R9 covers managed-only; the cycle-1
    // M2 split routes suggest-only to the weak penalty. The mixed case
    // — a single gesture overlapping BOTH a managed AdWindow AND a
    // markOnly AdWindow — must still take the full penalty (a managed
    // auto-skip was vetoed, regardless of whether a suggest banner was
    // ALSO touched in the same gesture). Without this pin, a future
    // re-order of the two loops in `revertByTimeRange` could
    // accidentally invert the `revertedManagedAny` flag and silently
    // demote mixed reverts to the weak penalty — escaping both the
    // suggest-only (R7) and managed-only (R9) pins.
    @Test("revertByTimeRange against BOTH a managed AND a suggest window decrements trust by full falseSignalPenalty")
    func revertByTimeRangeMixedDecrementsTrustByFullPenalty() async throws {
        let trustStore = try await makeTestStore()
        let initialTrust: Double = 0.90
        try await trustStore.upsertProfile(
            PodcastProfile(
                podcastId: "podcast-1",
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: initialTrust,
                observationCount: 10,
                mode: "auto",
                recentFalseSkipSignals: 0
            )
        )
        let trustConfig = TrustScoringConfig.default
        let trustService = TrustScoringService(store: trustStore, config: trustConfig)

        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let managed = makeSkipTestAdWindow(
            id: "ad-mixed-managed",
            assetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.75,
            decisionState: AdDecisionState.candidate.rawValue
        )
        let markOnly = AdWindow(
            id: "ad-mixed-suggest",
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.55,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "test-1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 60,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(managed)
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([managed, markOnly])

        // Sanity: managed enters the auto-skip dict, suggest enters the
        // suggest dict. The veto gesture below overlaps both.
        #expect(await orchestrator.activeWindowIDs().contains("ad-mixed-managed"))
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-mixed-suggest"))

        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: "podcast-1")

        var profile: PodcastProfile?
        for _ in 0..<20 {
            profile = try await trustStore.fetchProfile(podcastId: "podcast-1")
            if let p = profile, p.recentFalseSkipSignals == 1 { break }
            try await Task.sleep(nanoseconds: 50_000_000)
        }

        // Mixed revert must take the FULL penalty (managed was vetoed).
        let expectedTrust = initialTrust - trustConfig.falseSignalPenalty
        #expect(abs((profile?.skipTrustScore ?? -1) - expectedTrust) < 1e-6,
                "mixed revert (managed + suggest co-occurring) must decrement by full falseSignalPenalty (\(trustConfig.falseSignalPenalty)), NOT weakFalseSignalPenalty (\(trustConfig.weakFalseSignalPenalty)); got skipTrustScore=\(profile?.skipTrustScore ?? -1) expected=\(expectedTrust)")
        #expect(profile?.recentFalseSkipSignals == 1,
                "exactly one false-skip signal must be recorded for one veto gesture")
    }

    // R10 (hygc.1.8): symmetric "no overlap → no trust decrement" pin.
    // R7 pinned the suggest-tier full-magnitude decrement, R9 pinned the
    // managed-window full-magnitude decrement. Both fire on `revertedAny`.
    // Without this third pin, a future regression that lifts the
    // `if revertedAny` guard (e.g. by accident moving `recordFalseSkip
    // Signal` outside the conditional) would silently fire the trust
    // signal even on a no-op revert gesture — quietly poisoning a
    // podcast's trust score whenever the user taps "this isn't an ad"
    // on a region where no window exists. The two existing
    // full-magnitude pins do NOT catch this regression because they
    // both have an overlap. Pin the zero-magnitude case so the symmetric
    // pair is loud in both directions.
    @Test("revertByTimeRange with no overlapping windows does NOT decrement trust")
    func revertByTimeRangeNoOverlapDoesNotDecrementTrust() async throws {
        let trustStore = try await makeTestStore()
        let initialTrust: Double = 0.90
        try await trustStore.upsertProfile(
            PodcastProfile(
                podcastId: "podcast-1",
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: initialTrust,
                observationCount: 10,
                mode: "auto",
                recentFalseSkipSignals: 0
            )
        )
        let trustConfig = TrustScoringConfig.default
        let trustService = TrustScoringService(store: trustStore, config: trustConfig)

        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Seed both surfaces with windows OUTSIDE the upcoming gesture range
        // so we exercise both loops without producing a match.
        let managed = makeSkipTestAdWindow(
            id: "ad-managed-far",
            assetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: AdDecisionState.candidate.rawValue
        )
        let markOnly = AdWindow(
            id: "ad-suggest-far",
            analysisAssetId: "asset-1",
            startTime: 200,
            endTime: 260,
            confidence: 0.55,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "test-1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 200,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(managed)
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([managed, markOnly])

        // Veto a region that overlaps NEITHER surface.
        await orchestrator.revertByTimeRange(start: 800, end: 900, podcastId: "podcast-1")

        // Give any in-flight unstructured task a fair chance to write.
        // (We expect no write, but we want to give the no-op enough time
        // to let any erroneous trust write reach the store.)
        for _ in 0..<5 {
            try await Task.sleep(nanoseconds: 50_000_000)
        }

        let profile = try await trustStore.fetchProfile(podcastId: "podcast-1")
        #expect(abs((profile?.skipTrustScore ?? -1) - initialTrust) < 1e-6,
                "no-overlap revert must NOT decrement trust; got skipTrustScore=\(profile?.skipTrustScore ?? -1) expected=\(initialTrust)")
        #expect(profile?.recentFalseSkipSignals == 0,
                "no false-skip signal must be recorded when no windows overlap; got \(profile?.recentFalseSkipSignals ?? -1)")

        // And neither persisted window must be in `.reverted` state — the
        // managed window may legitimately advance to `.applied` via the
        // auto-skip lifecycle, but neither should reach `.reverted`
        // because the gesture overlapped neither.
        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        let managedRow = persisted.first { $0.id == "ad-managed-far" }
        let suggestRow = persisted.first { $0.id == "ad-suggest-far" }
        #expect(managedRow?.decisionState != AdDecisionState.reverted.rawValue,
                "managed window outside the gesture must NOT be reverted; got \(managedRow?.decisionState ?? "<missing>")")
        #expect(suggestRow?.decisionState != AdDecisionState.reverted.rawValue,
                "markOnly window outside the gesture must NOT be reverted; got \(suggestRow?.decisionState ?? "<missing>")")
    }

    // MARK: - playhead-lc7z: explicit suggest denial persists a falsePositive correction

    /// playhead-d3g0: detection delivery ARMS a suggestion; the PLAYHEAD
    /// entering its span is what presents the card.
    ///
    /// Every test in this file that expects a suggest presentation now has to
    /// satisfy that precondition, so this is one call putting the listener
    /// inside the span they are being asked about. The default matches
    /// `makeSuggestWindow`'s `[60, 120]`.
    ///
    /// The gate itself — that detection delivery alone banners NOTHING, and
    /// what happens on seek, pre-roll, replay and re-entry — is pinned in
    /// `SuggestBannerEntryGateTests`, not here. These tests are about
    /// retirement, revision identity and durable receipts, and they keep
    /// testing exactly that.
    private func enterSuggestSpan(
        _ orchestrator: SkipOrchestrator,
        at time: Double = 60
    ) async {
        await orchestrator.updatePlayheadTime(time)
    }

    /// Build a markOnly suggest-tier AdWindow with a brand + producer tag so
    /// the explicit-denial path has something to attribute (causalSource) and
    /// something to key hard-negative mining on (sponsorEntity).
    private func makeSuggestWindow(
        id: String,
        analysisAssetId: String = "asset-1",
        startTime: Double = 60,
        endTime: Double = 120,
        advertiser: String? = "Red Bull",
        metadataSource: String = SpecialistMarkComposer.metadataSource,
        confidence: Double = 0.55,
        decisionState: String = AdDecisionState.candidate.rawValue,
        eligibilityGate: String = SkipEligibilityGate.markOnly.rawValue,
        catalogStoreMatchSimilarity: Double? = nil,
        // playhead-ynmk: the per-edge anchor tier decides whether a CONFIRMED
        // suggestion has a late-safe extent to skip. Default `.unanchored`
        // (unchanged behaviour for every fixture that never accepts), so the
        // handful of tests here that observe the acceptance transaction through
        // "applied / wasSkipped / a cue fired" opt IN explicitly — see the
        // `anchored:` call sites. Making the default anchored would have hidden
        // the unanchored case from this whole file at a stroke.
        startEdgeAnchor: AutoSkipEdgeAnchor = .unanchored,
        endEdgeAnchor: AutoSkipEdgeAnchor = .unanchored
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: analysisAssetId,
            startTime: startTime,
            endTime: endTime,
            confidence: confidence,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: decisionState,
            detectorVersion: "test-1",
            advertiser: advertiser, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: startTime,
            metadataSource: metadataSource,
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: eligibilityGate,
            catalogStoreMatchSimilarity: catalogStoreMatchSimilarity,
            startEdgeAnchor: startEdgeAnchor.rawValue,
            endEdgeAnchor: endEdgeAnchor.rawValue
        )
    }

    /// A suggestion whose EXTENT is byte-verified on both edges, so confirming
    /// it really does skip (playhead-qs0d's 2-of-2 population). Used by the
    /// tests in this file whose subject is the acceptance TRANSACTION —
    /// atomicity, retry-after-failure, stale-revision rejection, cross-episode
    /// attribution — and which read "it landed" off `applied` / `wasSkipped` /
    /// a pushed cue. playhead-ynmk made that outcome conditional on the extent,
    /// so those fixtures have to state the extent they depend on. The
    /// unanchored outcome is pinned in `BannerConfirmationExtentGateTests`.
    private func makeAnchoredSuggestWindow(
        id: String,
        startTime: Double = 60,
        endTime: Double = 120
    ) -> AdWindow {
        makeSuggestWindow(
            id: id,
            startTime: startTime,
            endTime: endTime,
            startEdgeAnchor: .rediffByteExact,
            endEdgeAnchor: .rediffByteExact
        )
    }

    @Test("gate flip retires suggestion before replacement presentation")
    func gateFlipOrdersSuggestionRetirementBeforeReplacement() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 17
        )
        await orchestrator.setActiveSkipMode(.auto)
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)

        let suggest = makeSuggestWindow(id: "gate-flip-window")
        await orchestrator.receiveAdWindows([suggest])
        await enterSuggestSpan(orchestrator)
        guard case let .present(suggestItem) = await probe.next() else {
            Issue.record("Expected initial suggest presentation")
            return
        }
        #expect(suggestItem.tier == .suggest)

        let eligible = makeSuggestWindow(
            id: suggest.id,
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([eligible])

        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record("Gate flip must retire the stale suggestion first")
            return
        }
        #expect(retirement.windowId == suggest.id)
        #expect(retirement.episodeId == "episode-1")
        #expect(retirement.playbackLifecycleGeneration == 17)

        guard case let .present(replacement) = await probe.next() else {
            Issue.record("Expected replacement auto-tier presentation")
            return
        }
        #expect(replacement.windowId == suggest.id)
        #expect(replacement.tier == .autoSkipped)
    }

    @Test("blocked and inventory-rejected AdWindows retire stale suggestions")
    func adWindowInvalidationsRetireSuggestionsAndRejectStaleYes() async throws {
        let store = try await makeTestStore()
        // The asset must be OWNED by the episode this test plays, and every
        // suggestion must have its durable row, or `acceptSuggestedSkip`
        // aborts inside `persistAcceptedSuggestionIfCurrent` (episode
        // ownership / missing row) before the retirement contract is
        // consulted — and the "must not promote" assertions below become
        // unfalsifiable. See playhead-ugy4.
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            inventoryFilter: InventorySanityFilter(isEnabled: true)
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 71
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)

        let blockedSuggestion = makeSuggestWindow(
            id: "ad-window-blocked-after-suggest"
        )
        try await store.insertAdWindow(blockedSuggestion)
        await orchestrator.receiveAdWindows([blockedSuggestion])
        await enterSuggestSpan(orchestrator)
        guard case .present = await probe.next() else {
            Issue.record("Expected blocked-path suggestion presentation")
            return
        }
        let blockedUpdate = makeSuggestWindow(
            id: blockedSuggestion.id,
            confidence: 0.99,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate:
                SkipEligibilityGate.blockedByPolicy.rawValue
        )
        await orchestrator.receiveAdWindows([blockedUpdate])
        guard case let .retireWindow(blockedRetirement) =
            await probe.next()
        else {
            Issue.record("Blocked AdWindow must retire its stale suggestion")
            return
        }
        #expect(blockedRetirement.windowId == blockedSuggestion.id)

        let rejectedSuggestion = makeSuggestWindow(
            id: "ad-window-rejected-after-suggest"
        )
        try await store.insertAdWindow(rejectedSuggestion)
        await orchestrator.receiveAdWindows([rejectedSuggestion])
        await enterSuggestSpan(orchestrator)
        guard case .present = await probe.next() else {
            Issue.record("Expected inventory-path suggestion presentation")
            return
        }
        let rejectedUpdate = makeSuggestWindow(
            id: rejectedSuggestion.id,
            startTime: 60,
            endTime: 61,
            confidence: 0.99,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([rejectedUpdate])
        guard case let .retireWindow(rejectedRetirement) =
            await probe.next()
        else {
            Issue.record(
                "Inventory-rejected AdWindow must retire its stale suggestion"
            )
            return
        }
        #expect(rejectedRetirement.windowId == rejectedSuggestion.id)

        await orchestrator.acceptSuggestedSkip(
            windowId: blockedSuggestion.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 71
        )
        await orchestrator.acceptSuggestedSkip(
            windowId: rejectedSuggestion.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 71
        )

        let activeSuggestions =
            await orchestrator.activeSuggestWindowIDs()
        #expect(!activeSuggestions.contains(blockedSuggestion.id))
        #expect(!activeSuggestions.contains(rejectedSuggestion.id))
        #expect((await orchestrator.activeWindowIDs()).isEmpty)
        #expect(
            pushedCues.isEmpty,
            "A stale Yes must not promote a blocked or rejected AdWindow"
        )
    }

    @Test("blocked and inventory-rejected decisions retire stale suggestions")
    func decisionInvalidationsRetireSuggestionsAndRejectStaleYes() async throws {
        let store = try await makeTestStore()
        // Asset ownership + durable suggestion rows: see the note on
        // `adWindowInvalidationsRetireSuggestionsAndRejectStaleYes`.
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            inventoryFilter: InventorySanityFilter(isEnabled: true)
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 72
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)

        let blockedSuggestion = makeSuggestWindow(
            id: "decision-blocked-after-suggest"
        )
        try await store.insertAdWindow(blockedSuggestion)
        await orchestrator.receiveAdWindows([blockedSuggestion])
        await enterSuggestSpan(orchestrator)
        guard case .present = await probe.next() else {
            Issue.record("Expected blocked-decision suggestion presentation")
            return
        }
        await orchestrator.receiveAdDecisionResults([
            AdDecisionResult(
                id: blockedSuggestion.id,
                analysisAssetId: "asset-1",
                startTime: 60,
                endTime: 120,
                skipConfidence: 0.99,
                eligibilityGate: .blocked,
                recomputationRevision: 2
            )
        ])
        guard case let .retireWindow(blockedRetirement) =
            await probe.next()
        else {
            Issue.record(
                "Blocked AdDecisionResult must retire its stale suggestion"
            )
            return
        }
        #expect(blockedRetirement.windowId == blockedSuggestion.id)

        let rejectedSuggestion = makeSuggestWindow(
            id: "decision-rejected-after-suggest"
        )
        try await store.insertAdWindow(rejectedSuggestion)
        await orchestrator.receiveAdWindows([rejectedSuggestion])
        await enterSuggestSpan(orchestrator)
        guard case .present = await probe.next() else {
            Issue.record("Expected rejected-decision suggestion presentation")
            return
        }
        await orchestrator.receiveAdDecisionResults([
            AdDecisionResult(
                id: rejectedSuggestion.id,
                analysisAssetId: "asset-1",
                startTime: 60,
                endTime: 61,
                skipConfidence: 0.99,
                eligibilityGate: .eligible,
                recomputationRevision: 2
            )
        ])
        guard case let .retireWindow(rejectedRetirement) =
            await probe.next()
        else {
            Issue.record(
                "Inventory-rejected AdDecisionResult must retire its stale suggestion"
            )
            return
        }
        #expect(rejectedRetirement.windowId == rejectedSuggestion.id)

        await orchestrator.acceptSuggestedSkip(
            windowId: blockedSuggestion.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 72
        )
        await orchestrator.acceptSuggestedSkip(
            windowId: rejectedSuggestion.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 72
        )

        let activeSuggestions =
            await orchestrator.activeSuggestWindowIDs()
        #expect(!activeSuggestions.contains(blockedSuggestion.id))
        #expect(!activeSuggestions.contains(rejectedSuggestion.id))
        #expect((await orchestrator.activeWindowIDs()).isEmpty)
        #expect(
            pushedCues.isEmpty,
            "A stale Yes must not promote a blocked or rejected decision"
        )
    }

    @Test("explicit window retirement also invalidates a suggestion")
    func explicitRetirementRejectsStaleSuggestionYes() async throws {
        let store = try await makeTestStore()
        // Asset ownership + durable suggestion row: see the note on
        // `adWindowInvalidationsRetireSuggestionsAndRejectStaleYes`.
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 73
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let suggestion = makeSuggestWindow(
            id: "explicitly-retired-suggestion"
        )
        try await store.insertAdWindow(suggestion)
        await orchestrator.receiveAdWindows([suggestion])
        await enterSuggestSpan(orchestrator)
        guard case .present = await probe.next() else {
            Issue.record("Expected explicit-retirement presentation")
            return
        }

        await orchestrator.retireAdWindows(ids: [suggestion.id])
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record(
                "retireAdWindows must retire a matching suggestion"
            )
            return
        }
        #expect(retirement.windowId == suggestion.id)

        await orchestrator.acceptSuggestedSkip(
            windowId: suggestion.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 73
        )

        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(suggestion.id)
        )
        #expect((await orchestrator.activeWindowIDs()).isEmpty)
        #expect(
            pushedCues.isEmpty,
            "A stale Yes after explicit retirement must not install a cue"
        )

        let managed = makeSuggestWindow(
            id: "explicitly-retired-managed",
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.setActiveSkipMode(.auto)
        await orchestrator.receiveAdWindows([managed])
        guard case let .present(managedItem) = await probe.next() else {
            Issue.record("Expected explicit-retirement managed presentation")
            return
        }
        #expect(managedItem.tier == .autoSkipped)

        await orchestrator.retireAdWindows(ids: [managed.id])
        guard case let .retireWindow(managedRetirement) =
            await probe.next()
        else {
            Issue.record(
                "retireAdWindows must retire a matching managed card"
            )
            return
        }
        #expect(managedRetirement.windowId == managed.id)
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(managed.id)
        )
    }

    @Test("late inventory context retires a newly-invalid suggestion")
    func lateInventoryContextRejectsStaleSuggestionYes() async throws {
        let store = try await makeTestStore()
        // Asset ownership + durable suggestion row: see the note on
        // `adWindowInvalidationsRetireSuggestionsAndRejectStaleYes`.
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            inventoryFilter: InventorySanityFilter(isEnabled: true)
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 74
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let suggestion = makeSuggestWindow(
            id: "late-inventory-rejected-suggestion"
        )
        try await store.insertAdWindow(suggestion)
        await orchestrator.receiveAdWindows([suggestion])
        await enterSuggestSpan(orchestrator)
        guard case .present = await probe.next() else {
            Issue.record("Expected pre-context suggestion presentation")
            return
        }

        // Publisher chapters arrive after episode hydration in production.
        // The span was valid with no chapter context, but becomes invalid as
        // soon as the declared editorial chapter overlaps it.
        await orchestrator.setDeclaredChapters(
            [
                ChapterEvidence(
                    startTime: 50,
                    endTime: 130,
                    title: "Editorial interview",
                    source: .rssInline,
                    disposition: .content,
                    qualityScore: 1
                )
            ],
            analysisAssetId: "asset-1"
        )
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record(
                "Late inventory context must retire a newly-invalid suggestion"
            )
            return
        }
        #expect(retirement.windowId == suggestion.id)

        await orchestrator.acceptSuggestedSkip(
            windowId: suggestion.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 74
        )

        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(suggestion.id)
        )
        #expect((await orchestrator.activeWindowIDs()).isEmpty)
        #expect(
            pushedCues.isEmpty,
            "A stale Yes after late inventory rejection must not install a cue"
        )
    }

    @Test("same-ID AdWindow with changed bounds must pass inventory validation")
    func managedAdWindowReplacementCannotBypassInventoryFilter()
        async throws
    {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            inventoryFilter: InventorySanityFilter(isEnabled: true)
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1"
        )
        await orchestrator.setActiveSkipMode(.manual)

        let admitted = makeSkipTestAdWindow(
            id: "round3-inventory-adwindow",
            startTime: 60,
            endTime: 120,
            confidence: 0.9
        )
        let invalidReplacement = makeSkipTestAdWindow(
            id: admitted.id,
            startTime: 0,
            endTime: 60,
            confidence: 0.99
        )
        await orchestrator.receiveAdWindows([admitted])
        await orchestrator.receiveAdWindows([invalidReplacement])
        await orchestrator.setActiveSkipMode(.auto)

        #expect(pushedCues.isEmpty)
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(admitted.id),
            "a same-ID geometry change has no reusable inventory admission"
        )
    }

    @Test("same-ID decision with changed bounds must pass inventory validation")
    func managedDecisionReplacementCannotBypassInventoryFilter()
        async throws
    {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            inventoryFilter: InventorySanityFilter(isEnabled: true)
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1"
        )
        await orchestrator.setActiveSkipMode(.manual)

        let admitted = makeSkipTestAdWindow(
            id: "round3-inventory-decision",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        let invalidReplacement = makeSkipTestAdWindow(
            id: admitted.id,
            startTime: 0,
            endTime: 60,
            confidence: 0.99,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        for (revision, window) in [
            (1, admitted),
            (2, invalidReplacement),
        ] {
            await orchestrator.receiveAdDecisionResults([
                AdDecisionResult(
                    id: window.id,
                    analysisAssetId: window.analysisAssetId,
                    startTime: window.startTime,
                    endTime: window.endTime,
                    skipConfidence: window.confidence,
                    eligibilityGate: .eligible,
                    recomputationRevision: revision,
                    producerRevision: window
                )
            ])
        }
        await orchestrator.setActiveSkipMode(.auto)

        #expect(pushedCues.isEmpty)
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(admitted.id),
            "decision envelopes must not reuse inventory admission across geometry revisions"
        )
    }

    @Test("late inventory context retires a managed card and cue")
    func lateInventoryContextRetiresManagedCardAndCue() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            inventoryFilter: InventorySanityFilter(isEnabled: true)
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 78
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let managed = makeSuggestWindow(
            id: "late-inventory-rejected-managed",
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([managed])
        guard case .present = await probe.next() else {
            Issue.record("Expected pre-context managed presentation")
            return
        }
        #expect(!pushedCues.isEmpty)

        await orchestrator.setDeclaredChapters(
            [
                ChapterEvidence(
                    startTime: 50,
                    endTime: 130,
                    title: "Editorial interview",
                    source: .rssInline,
                    disposition: .content,
                    qualityScore: 1
                )
            ],
            analysisAssetId: "asset-1"
        )
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record(
                "Late inventory context must retire a managed card"
            )
            return
        }
        #expect(retirement.windowId == managed.id)
        #expect(pushedCues.isEmpty)
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(managed.id)
        )
    }

    @Test("AdWindow applied to markOnly downgrade disarms cue and surfaces suggestion")
    func adWindowAppliedToMarkOnlyDowngradeDisarmsCue() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 75
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let eligible = makeSuggestWindow(
            id: "applied-to-mark-only",
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([eligible])
        guard case let .present(autoItem) = await probe.next() else {
            Issue.record("Expected initial auto-tier presentation")
            return
        }
        #expect(autoItem.tier == .autoSkipped)
        #expect(!pushedCues.isEmpty)
        #expect(
            (await orchestrator.activeWindowIDs()).contains(eligible.id)
        )

        let markOnly = makeSuggestWindow(id: eligible.id)
        await orchestrator.receiveAdWindows([markOnly])
        // The downgrade's RETIREMENT is emitted by the delivery itself; only
        // the replacement suggest card waits for the playhead (playhead-d3g0).
        // Ticking after the receive therefore leaves the observed ORDER —
        // retire, then present — exactly as it was.
        await enterSuggestSpan(orchestrator)
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record("markOnly downgrade must retire the auto card")
            return
        }
        #expect(retirement.windowId == eligible.id)
        guard case let .present(suggestItem) = await probe.next() else {
            Issue.record("markOnly downgrade must surface a suggestion")
            return
        }
        #expect(suggestItem.tier == .suggest)
        #expect(suggestItem.windowId == eligible.id)
        #expect(pushedCues.isEmpty)
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(eligible.id)
        )
        #expect(
            (await orchestrator.activeSuggestWindowIDs())
                .contains(eligible.id)
        )
    }

    @Test("AdWindow applied to blocked downgrade disarms cue without replacement")
    func adWindowAppliedToBlockedDowngradeDisarmsCue() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 76
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let eligible = makeSuggestWindow(
            id: "applied-to-blocked",
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([eligible])
        guard case .present = await probe.next() else {
            Issue.record("Expected initial auto-tier presentation")
            return
        }
        #expect(!pushedCues.isEmpty)

        let blocked = makeSuggestWindow(
            id: eligible.id,
            confidence: 0.99,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate:
                SkipEligibilityGate.blockedByPolicy.rawValue
        )
        await orchestrator.receiveAdWindows([blocked])
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record("Blocked downgrade must retire the auto card")
            return
        }
        #expect(retirement.windowId == eligible.id)
        #expect(pushedCues.isEmpty)
        #expect(
            (await orchestrator.confirmedWindows()).allSatisfy {
                $0.id != eligible.id
            },
            "terminal producer material must not remain manually actionable"
        )
    }

    @Test(
        "identical-material terminal producer state disarms an applied cue",
        arguments: [
            AdDecisionState.reverted,
            AdDecisionState.suppressed,
        ]
    )
    func terminalProducerStateDisarmsAppliedCue(
        terminalState: AdDecisionState
    ) async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 78
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let eligible = makeSuggestWindow(
            id: "applied-to-\(terminalState.rawValue)",
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([eligible])
        guard case .present = await probe.next() else {
            Issue.record("Expected initial auto-tier presentation")
            return
        }
        #expect(!pushedCues.isEmpty)

        let terminal = makeSuggestWindow(
            id: eligible.id,
            confidence: eligible.confidence,
            decisionState: terminalState.rawValue,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        await orchestrator.receiveAdWindows([terminal])
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record(
                "\(terminalState.rawValue) producer state must retire the auto card"
            )
            return
        }
        #expect(retirement.windowId == eligible.id)
        #expect(pushedCues.isEmpty)
        #expect(
            (await orchestrator.confirmedWindows()).allSatisfy {
                $0.id != eligible.id
            },
            "terminal producer material must not remain manually actionable"
        )
        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(eligible.id)
        )
    }

    @Test(
        "fusion envelope cannot re-arm a terminal persisted producer",
        arguments: [
            AdDecisionState.reverted,
            AdDecisionState.suppressed,
        ]
    )
    func terminalProducerRevisionIsNotActionable(
        terminalState: AdDecisionState
    ) async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 79
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let eligible = makeSuggestWindow(
            id: "fusion-terminal-\(terminalState.rawValue)",
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([eligible])
        guard case .present = await probe.next() else {
            Issue.record("Expected initial auto-tier presentation")
            return
        }
        #expect(!pushedCues.isEmpty)

        let terminal = makeSuggestWindow(
            id: eligible.id,
            confidence: eligible.confidence,
            decisionState: terminalState.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        let replay = AdDecisionResult(
            id: terminal.id,
            analysisAssetId: terminal.analysisAssetId,
            startTime: terminal.startTime,
            endTime: terminal.endTime,
            skipConfidence: terminal.confidence,
            eligibilityGate: .eligible,
            recomputationRevision: 2
        ).withProducerRevision(terminal)
        #expect(replay.eligibilityGate == .blocked)
        #expect(
            replay.producerRevision?.decisionState
                == terminalState.rawValue
        )

        await orchestrator.receiveAdDecisionResults([replay])
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record(
                "\(terminalState.rawValue) producer revision must retire the auto card"
            )
            return
        }
        #expect(retirement.windowId == eligible.id)
        #expect(pushedCues.isEmpty)
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(eligible.id)
        )
        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(eligible.id)
        )
    }

    @Test(
        "terminal producer revision fences a stale eligible replay but permits new same-ID material",
        arguments: [false, true]
    )
    func terminalProducerRevisionFencesStaleEligibleReplay(
        usesDecisionResult: Bool
    ) async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 80
        )

        let eligible = makeSuggestWindow(
            id: "terminal-replay-\(usesDecisionResult)",
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        try await store.insertAdWindow(eligible)
        if usesDecisionResult {
            await orchestrator.receiveAdDecisionResults([
                AdDecisionResult(
                    id: eligible.id,
                    analysisAssetId: eligible.analysisAssetId,
                    startTime: eligible.startTime,
                    endTime: eligible.endTime,
                    skipConfidence: eligible.confidence,
                    eligibilityGate: .eligible,
                    recomputationRevision: 0
                ).withProducerRevision(eligible)
            ])
        } else {
            await orchestrator.receiveAdWindows([eligible])
        }
        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()
        #expect(!pushedCues.isEmpty)

        let terminal = makeSuggestWindow(
            id: eligible.id,
            confidence: eligible.confidence,
            decisionState: AdDecisionState.suppressed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        try await store.insertOrReplaceAdWindow(terminal)
        if usesDecisionResult {
            await orchestrator.receiveAdDecisionResults([
                AdDecisionResult(
                    id: terminal.id,
                    analysisAssetId: terminal.analysisAssetId,
                    startTime: terminal.startTime,
                    endTime: terminal.endTime,
                    skipConfidence: terminal.confidence,
                    eligibilityGate: .eligible,
                    recomputationRevision: 1
                ).withProducerRevision(terminal)
            ])
        } else {
            await orchestrator.receiveAdWindows([terminal])
        }
        #expect(pushedCues.isEmpty)

        // This object was captured before the durable terminal transition.
        // Replaying it later must not recreate a cue or either banner tier.
        if usesDecisionResult {
            await orchestrator.receiveAdDecisionResults([
                AdDecisionResult(
                    id: eligible.id,
                    analysisAssetId: eligible.analysisAssetId,
                    startTime: eligible.startTime,
                    endTime: eligible.endTime,
                    skipConfidence: eligible.confidence,
                    eligibilityGate: .eligible,
                    recomputationRevision: 0
                ).withProducerRevision(eligible)
            ])
        } else {
            await orchestrator.receiveAdWindows([eligible])
        }
        #expect(pushedCues.isEmpty)
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(eligible.id)
        )
        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(eligible.id)
        )

        // Terminal suppression is scoped to exact producer material. A real
        // same-ID replacement still receives the normal admission checks.
        let replacement = makeSuggestWindow(
            id: eligible.id,
            startTime: eligible.startTime + 1,
            endTime: eligible.endTime + 1,
            confidence: eligible.confidence,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        try await store.insertOrReplaceAdWindow(replacement)
        if usesDecisionResult {
            await orchestrator.receiveAdDecisionResults([
                AdDecisionResult(
                    id: replacement.id,
                    analysisAssetId: replacement.analysisAssetId,
                    startTime: replacement.startTime,
                    endTime: replacement.endTime,
                    skipConfidence: replacement.confidence,
                    eligibilityGate: .eligible,
                    recomputationRevision: 2
                ).withProducerRevision(replacement)
            ])
        } else {
            await orchestrator.receiveAdWindows([replacement])
        }
        #expect(
            (await orchestrator.activeWindowIDs())
                .contains(replacement.id)
        )
        #expect(!pushedCues.isEmpty)

        // Once replacement B owns the ID, neither pre-terminal A nor terminal
        // A may tear B down. The terminal fence is revision-scoped, not an
        // implicit ID tombstone.
        if usesDecisionResult {
            await orchestrator.receiveAdDecisionResults([
                AdDecisionResult(
                    id: eligible.id,
                    analysisAssetId: eligible.analysisAssetId,
                    startTime: eligible.startTime,
                    endTime: eligible.endTime,
                    skipConfidence: eligible.confidence,
                    eligibilityGate: .eligible,
                    recomputationRevision: 0
                ).withProducerRevision(eligible)
            ])
        } else {
            await orchestrator.receiveAdWindows([eligible])
        }
        let expectedReplacementCueEnd =
            replacement.endTime
            - SkipPolicyConfig.default.adTrailingCushionSeconds
        #expect(
            pushedCues.first?.start.seconds == replacement.startTime
        )
        #expect(
            pushedCues.first?.end.seconds == expectedReplacementCueEnd
        )

        if usesDecisionResult {
            await orchestrator.receiveAdDecisionResults([
                AdDecisionResult(
                    id: terminal.id,
                    analysisAssetId: terminal.analysisAssetId,
                    startTime: terminal.startTime,
                    endTime: terminal.endTime,
                    skipConfidence: terminal.confidence,
                    eligibilityGate: .eligible,
                    recomputationRevision: 1
                ).withProducerRevision(terminal)
            ])
        } else {
            await orchestrator.receiveAdWindows([terminal])
        }
        #expect(
            pushedCues.first?.start.seconds == replacement.startTime
        )
        #expect(
            pushedCues.first?.end.seconds == expectedReplacementCueEnd
        )
        #expect(
            (await orchestrator.activeWindowIDs())
                .contains(replacement.id)
        )
        await orchestrator._waitForRecurrenceBackgroundWorkForTesting()
    }

    @Test(
        "stale terminal material cannot supersede a suspended same-ID replacement",
        arguments: [false, true], [false, true]
    )
    func terminalProducerReplayPreservesSuspendedReplacement(
        usesDecisionResult: Bool,
        replaysTerminalRevision: Bool
    ) async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 81
        )

        let original = makeSuggestWindow(
            id:
                "terminal-pending-\(usesDecisionResult)-\(replaysTerminalRevision)",
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        if usesDecisionResult {
            await orchestrator.receiveAdDecisionResults([
                AdDecisionResult(
                    id: original.id,
                    analysisAssetId: original.analysisAssetId,
                    startTime: original.startTime,
                    endTime: original.endTime,
                    skipConfidence: original.confidence,
                    eligibilityGate: .eligible,
                    recomputationRevision: 0
                ).withProducerRevision(original)
            ])
        } else {
            await orchestrator.receiveAdWindows([original])
        }
        #expect((await orchestrator.activeWindowIDs()).contains(original.id))

        let terminal = makeSuggestWindow(
            id: original.id,
            confidence: original.confidence,
            decisionState: AdDecisionState.suppressed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        if usesDecisionResult {
            await orchestrator.receiveAdDecisionResults([
                AdDecisionResult(
                    id: terminal.id,
                    analysisAssetId: terminal.analysisAssetId,
                    startTime: terminal.startTime,
                    endTime: terminal.endTime,
                    skipConfidence: terminal.confidence,
                    eligibilityGate: .eligible,
                    recomputationRevision: 1
                ).withProducerRevision(terminal)
            ])
        } else {
            await orchestrator.receiveAdWindows([terminal])
        }
        #expect(!(await orchestrator.activeWindowIDs()).contains(original.id))

        // An incomplete catalog claim supplies the real actor suspension
        // boundary while deterministically taking the fail-closed suggestion
        // path after validation resumes.
        let replacement = makeSuggestWindow(
            id: original.id,
            startTime: original.startTime + 1,
            endTime: original.endTime + 1,
            confidence: original.confidence,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue,
            catalogStoreMatchSimilarity: 0.99
        )
        let validationGate = ControlledAsyncGate()
        await orchestrator
            ._setCatalogAdmissionValidationBarrierForTesting {
                await validationGate.wait()
            }
        let replacementIngestion = Task {
            if usesDecisionResult {
                await orchestrator.receiveAdDecisionResults([
                    AdDecisionResult(
                        id: replacement.id,
                        analysisAssetId: replacement.analysisAssetId,
                        startTime: replacement.startTime,
                        endTime: replacement.endTime,
                        skipConfidence: replacement.confidence,
                        eligibilityGate: .eligible,
                        recomputationRevision: 2
                    ).withProducerRevision(replacement)
                ])
            } else {
                await orchestrator.receiveAdWindows([replacement])
            }
        }
        await validationGate.waitUntilStarted()

        let staleReplay =
            replaysTerminalRevision ? terminal : original
        if usesDecisionResult {
            await orchestrator.receiveAdDecisionResults([
                AdDecisionResult(
                    id: staleReplay.id,
                    analysisAssetId: staleReplay.analysisAssetId,
                    startTime: staleReplay.startTime,
                    endTime: staleReplay.endTime,
                    skipConfidence: staleReplay.confidence,
                    eligibilityGate: .eligible,
                    recomputationRevision:
                        replaysTerminalRevision ? 1 : 0
                ).withProducerRevision(staleReplay)
            ])
        } else {
            await orchestrator.receiveAdWindows([staleReplay])
        }

        await validationGate.release()
        await replacementIngestion.value
        await orchestrator
            ._setCatalogAdmissionValidationBarrierForTesting(nil)

        let survivingReplacement =
            await orchestrator._suggestWindowForTesting(id: replacement.id)
        #expect(
            survivingReplacement?.startTime == replacement.startTime
        )
        #expect(survivingReplacement?.endTime == replacement.endTime)
        #expect(
            survivingReplacement?.catalogStoreMatchSimilarity
                == replacement.catalogStoreMatchSimilarity
        )
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(replacement.id)
        )
    }

    @Test("blocked decision disarms an already-applied window")
    func decisionAppliedToBlockedDowngradeDisarmsCue() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 77
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let eligible = AdDecisionResult(
            id: "decision-applied-to-blocked",
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            skipConfidence: 0.9,
            eligibilityGate: .eligible,
            recomputationRevision: 1,
            producerRevision: makeSkipTestAdWindow(
                id: "decision-applied-to-blocked",
                assetId: "asset-1",
                startTime: 60,
                endTime: 120,
                confidence: 0.9,
                eligibilityGate: SkipEligibilityGate.eligible.rawValue
            )
        )
        await orchestrator.receiveAdDecisionResults([eligible])
        guard case .present = await probe.next() else {
            Issue.record("Expected initial decision auto-tier presentation")
            return
        }
        #expect(!pushedCues.isEmpty)

        let blocked = AdDecisionResult(
            id: eligible.id,
            analysisAssetId: eligible.analysisAssetId,
            startTime: eligible.startTime,
            endTime: eligible.endTime,
            skipConfidence: 0.99,
            eligibilityGate: .blocked,
            recomputationRevision: 2
        )
        await orchestrator.receiveAdDecisionResults([blocked])
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record("Blocked decision must retire the auto card")
            return
        }
        #expect(retirement.windowId == eligible.id)
        #expect(pushedCues.isEmpty)
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(eligible.id)
        )
        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(eligible.id)
        )
    }

    @Test("suggest Yes applies a cue in shadow mode without a second banner")
    func acceptingSuggestionAppliesExplicitSkipWithoutDuplicateBanner() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 18
        )
        #expect(await orchestrator.currentSkipMode() == .shadow)

        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let suggest = makeAnchoredSuggestWindow(id: "accept-shadow-window")
        try await store.insertAdWindow(suggest)
        await orchestrator.receiveAdWindows([suggest])
        await enterSuggestSpan(orchestrator)
        guard case let .present(item) = await probe.next() else {
            Issue.record("Expected suggest presentation")
            return
        }
        await orchestrator.acknowledgeSuggestedBannerDelivery(
            windowId: item.windowId,
            episodeId: item.episodeId,
            playbackLifecycleGeneration:
                item.playbackLifecycleGeneration
        )

        await orchestrator.acceptSuggestedSkip(
            windowId: item.windowId,
            ifCurrentEpisodeId: item.episodeId,
            ifPlaybackLifecycleGeneration:
                item.playbackLifecycleGeneration
        )

        #expect(!pushedCues.isEmpty,
                "Explicit Yes must skip even when automatic mode is shadow")
        #expect(
            (await orchestrator.emittedAutoSkipBannersSnapshot()).isEmpty,
            "The accepted suggest card already supplied the feedback presentation"
        )
        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(
            persisted.first { $0.id == suggest.id }?.decisionState
                == AdDecisionState.suppressed.rawValue,
            "The original markOnly row must not resurrect after replay"
        )
        let promotedRows = persisted.filter { $0.id != suggest.id }
        #expect(promotedRows.count == 1)
        #expect(promotedRows.first?.decisionState
                == AdDecisionState.applied.rawValue)
        #expect(promotedRows.first?.wasSkipped == true)

        let revisedMarkOnly = makeAnchoredSuggestWindow(
            id: suggest.id,
            startTime: 75,
            endTime: 135
        )
        await orchestrator.receiveAdWindows([revisedMarkOnly])
        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(suggest.id),
            "A same-ID markOnly revision after Yes must not create another suggestion"
        )
        #expect(
            await probe.next() == nil,
            "A same-ID markOnly revision after Yes must not emit another card"
        )
    }

    @Test("accepted suggestion IDs remain terminal for the whole lifecycle")
    func acceptedSuggestionTerminalSetDoesNotEvictOlderIDs() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 180
        )

        let firstID = "accepted-terminal-0"
        for index in 0...256 {
            let suggestion = makeSuggestWindow(
                id: "accepted-terminal-\(index)"
            )
            try await store.insertAdWindow(suggestion)
            await orchestrator.receiveAdWindows([suggestion])
            #expect(
                await orchestrator.acceptSuggestedSkip(
                    windowId: suggestion.id,
                    ifCurrentEpisodeId: "episode-1",
                    ifPlaybackLifecycleGeneration: 180
                ),
                "Every distinct suggestion should be accepted exactly once"
            )
        }

        let promotedCountBeforeReplay = try await store.fetchAdWindows(
            assetId: "asset-1"
        ).filter {
            $0.id != firstID && $0.wasSkipped
        }.count

        // The former 256-entry LRU evicted `firstID` here, allowing this late
        // producer replay to register and mint a second promoted UUID.
        let replayEvents = await orchestrator.bannerEventStream()
        let replayProbe = BoundedStreamProbe(replayEvents)
        let lateReplay = makeSuggestWindow(id: firstID)
        await orchestrator.receiveAdWindows([lateReplay])

        #expect(
            await replayProbe.next() == nil,
            "A terminal accepted ID must not receive a second presentation"
        )
        #expect(
            !(await orchestrator.activeSuggestWindowIDs()).contains(firstID),
            "An accepted producer ID must stay terminal until lifecycle end"
        )
        #expect(
            !(await orchestrator.acceptSuggestedSkip(
                windowId: firstID,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 180
            )),
            "A late replay must not create a second durable result"
        )
        let promotedCountAfterReplay = try await store.fetchAdWindows(
            assetId: "asset-1"
        ).filter {
            $0.id != firstID && $0.wasSkipped
        }.count
        #expect(promotedCountAfterReplay == promotedCountBeforeReplay)
    }

    @Test("suggest Yes persistence failure preserves the card and cue for retry")
    func acceptingSuggestionRequiresDurablePersistenceBeforeApplying() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 181
        )

        // The in-memory producer can deliver before its row becomes visible to
        // this store. That missing authoritative row forces the acceptance
        // transaction to fail deterministically.
        let suggest = makeAnchoredSuggestWindow(id: "accept-persistence-retry")
        await orchestrator.receiveAdWindows([suggest])
        let decisionCountBeforeFeedback =
            await orchestrator.getDecisionLog().count

        let firstAccepted = await orchestrator.acceptSuggestedSkip(
            windowId: suggest.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 181
        )

        #expect(!firstAccepted)
        #expect(pushedCues.isEmpty)
        #expect(
            await orchestrator.activeSuggestWindowIDs().contains(suggest.id),
            "A failed durable write must leave the same suggestion retryable"
        )
        #expect(
            (try await store.fetchAdWindows(assetId: "asset-1")).isEmpty,
            "A failed transaction must not leave a promoted row behind"
        )
        #expect(
            try await correctionStore.activeCorrections(for: "asset-1")
                .isEmpty,
            "A failed acceptance must not leave a premature correction"
        )

        try await store.insertAdWindow(suggest)
        let retryAccepted = await orchestrator.acceptSuggestedSkip(
            windowId: suggest.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 181
        )

        #expect(retryAccepted)
        #expect(!pushedCues.isEmpty)
        #expect(
            !(await orchestrator.activeSuggestWindowIDs().contains(suggest.id))
        )
        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(
            persisted.first { $0.id == suggest.id }?.decisionState
                == AdDecisionState.suppressed.rawValue
        )
        #expect(
            persisted.contains {
                $0.id != suggest.id
                    && $0.decisionState == AdDecisionState.applied.rawValue
                    && $0.wasSkipped
                }
        )
        let corrections = try await correctionStore.activeCorrections(
            for: "asset-1"
        )
        #expect(corrections.count == 1)
        let correction = try #require(corrections.first)
        #expect(correction.source == .bannerSuggestionConfirmed)
        let promotedWindowId = try #require(
            persisted.first {
                $0.id != suggest.id
                    && $0.decisionState == AdDecisionState.applied.rawValue
            }?.id
        )
        #expect(correction.targetRefs?.adWindowId == promotedWindowId)
        #expect(
            await orchestrator.getDecisionLog().count
                == decisionCountBeforeFeedback,
            "Suggest Yes must not add a detailed decision-log record"
        )
        #expect(
            corrections.first?.submissionCount == 1,
            "The failed attempt must not count as a durable submission"
        )
    }

    @Test(
        "suggest Yes persists episode A feedback across direct replacement",
        .timeLimit(.minutes(1))
    )
    func suggestYesPersistsSourceFeedbackAcrossEpisodeReplacement() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-1",
                episodeId: "episode-1"
            )
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-2",
                episodeId: "episode-2"
            )
        )
        let persistentCorrections = PersistentUserCorrectionStore(
            store: store
        )
        let correctionGate = ControlledAsyncGate()
        let falseNegativeGate = ControlledAsyncGate()
        let gatedCorrections = GatedUserCorrectionStore(
            wrapped: persistentCorrections,
            recordGate: correctionGate
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: gatedCorrections
        )
        await orchestrator._setFalseNegativeSignalHandlerForTesting { _ in
            await falseNegativeGate.wait()
        }
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 311
        )

        let suggestion = makeAnchoredSuggestWindow(
            id: "suggest-yes-during-replacement"
        )
        try await store.insertAdWindow(suggestion)
        await orchestrator.receiveAdWindows([suggestion])

        let primaryResultGate = ControlledAsyncGate()
        let response = Task {
            let result = await orchestrator.acceptSuggestedSkip(
                windowId: suggestion.id,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 311
            )
            await primaryResultGate.wait()
            return result
        }
        await correctionGate.waitUntilStarted()
        await falseNegativeGate.waitUntilStarted()
        await primaryResultGate.waitUntilStarted()
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2",
            episodeId: "episode-2",
            podcastId: "podcast-2",
            playbackLifecycleGeneration: 312
        )
        await correctionGate.release()
        await falseNegativeGate.release()
        await primaryResultGate.release()

        #expect(await response.value)
        let corrections = try await persistentCorrections.activeCorrections(
            for: "asset-1"
        )
        #expect(corrections.count == 1)
        #expect(corrections.first?.correctionType == .falseNegative)
        let sourceRows = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(
            sourceRows.first { $0.id == suggestion.id }?.decisionState
                == AdDecisionState.suppressed.rawValue
        )
        #expect(
            sourceRows.contains {
                $0.id != suggestion.id
                    && $0.wasSkipped
                    && $0.decisionState
                        == AdDecisionState.applied.rawValue
            }
        )
        #expect((await orchestrator.activeWindowIDs()).isEmpty)
    }

    @Test(
        "failed suggest Yes restores the latest revision received while persistence waits",
        .timeLimit(.minutes(1))
    )
    func failedSuggestYesRestoresLatestBufferedRevision() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        let persistenceGate = ControlledAsyncGate()
        await orchestrator._setSuggestPersistenceBarrierForTesting {
            await persistenceGate.wait()
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1"
        )

        let original = makeAnchoredSuggestWindow(
            id: "yes-buffered-revision",
            startTime: 60,
            endTime: 120
        )
        let latest = makeAnchoredSuggestWindow(
            id: original.id,
            startTime: 70,
            endTime: 135
        )
        try await store.insertAdWindow(original)
        await orchestrator.receiveAdWindows([original])
        let responseTask = Task {
            await orchestrator.acceptSuggestedSkip(
                windowId: original.id,
                ifCurrentEpisodeId: "episode-1"
            )
        }
        await persistenceGate.waitUntilStarted()
        try await store.insertOrReplaceAdWindow(latest)
        await orchestrator.receiveAdWindows([latest])
        await persistenceGate.release()

        #expect(!(await responseTask.value))
        let restored = await orchestrator._suggestWindowForTesting(
            id: original.id
        )
        #expect(restored?.startTime == latest.startTime)
        #expect(restored?.endTime == latest.endTime)
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: original.analysisAssetId
            ).isEmpty
        )
        #expect(
            try await store.fetchAdWindow(id: latest.id)?
                .decisionState == AdDecisionState.candidate.rawValue
        )

        await orchestrator._setFeedbackPersistenceBarrierForTesting(nil)
        #expect(
            await orchestrator.acceptSuggestedSkip(
                windowId: latest.id,
                ifCurrentEpisodeId: "episode-1"
            ),
            "The replacement revision must remain actionable"
        )
        let rows = try await store.fetchAdWindows(
            assetId: latest.analysisAssetId
        )
        #expect(
            rows.contains {
                $0.id != latest.id
                    && $0.startTime == latest.startTime
                    && $0.endTime == latest.endTime
                    && $0.wasSkipped
            }
        )
    }

    @Test(
        "failed suggest No restores the latest revision received while persistence waits",
        .timeLimit(.minutes(1))
    )
    func failedSuggestNoRestoresLatestBufferedRevision() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        let persistenceGate = ControlledAsyncGate()
        await orchestrator._setSuggestPersistenceBarrierForTesting {
            await persistenceGate.wait()
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1"
        )

        let original = makeSuggestWindow(
            id: "no-buffered-revision",
            startTime: 60,
            endTime: 120
        )
        let latest = makeSuggestWindow(
            id: original.id,
            startTime: 75,
            endTime: 140
        )
        try await store.insertAdWindow(original)
        await orchestrator.receiveAdWindows([original])
        let responseTask = Task {
            await orchestrator.declineSuggestedSkip(
                windowId: original.id,
                isExplicitDenial: true,
                ifCurrentEpisodeId: "episode-1"
            )
        }
        await persistenceGate.waitUntilStarted()
        try await store.insertOrReplaceAdWindow(latest)
        await orchestrator.receiveAdWindows([latest])
        await persistenceGate.release()

        #expect(!(await responseTask.value))
        let restored = await orchestrator._suggestWindowForTesting(
            id: original.id
        )
        #expect(restored?.startTime == latest.startTime)
        #expect(restored?.endTime == latest.endTime)
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: original.analysisAssetId
            ).isEmpty
        )
        #expect(
            try await store.fetchAdWindow(id: latest.id)?
                .decisionState == AdDecisionState.candidate.rawValue
        )

        await orchestrator._setFeedbackPersistenceBarrierForTesting(nil)
        #expect(
            await orchestrator.declineSuggestedSkip(
                windowId: latest.id,
                isExplicitDenial: true,
                ifCurrentEpisodeId: "episode-1"
            ),
            "The replacement revision must remain actionable"
        )
        let persistedLatest = try #require(
            try await store.fetchAdWindow(id: latest.id)
        )
        #expect(
            persistedLatest.decisionState
                == AdDecisionState.reverted.rawValue
        )
        #expect(persistedLatest.userDismissedBanner)
    }

    @Test(
        "stale Auto-Yes cannot commit after durable material changes mid-suspension",
        .timeLimit(.minutes(1))
    )
    func staleAutoYesCannotCommitAfterMaterialReplacement()
        async throws
    {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 205
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let original = makeSkipTestAdWindow(
            id: "auto-yes-mid-suspension-revision",
            startTime: 60,
            endTime: 120,
            confidence: 0.91,
            decisionState: AdDecisionState.applied.rawValue
        )
        try await store.insertAdWindow(original)
        await orchestrator.receiveAdWindows([original])
        guard case let .present(originalItem) = await probe.next() else {
            Issue.record("Expected original Auto-Yes card")
            return
        }

        let persistenceGate = ControlledAsyncGate()
        await orchestrator._setFeedbackPersistenceBarrierForTesting {
            await persistenceGate.wait()
        }
        let staleResponse = Task {
            await orchestrator.confirmAutoSkippedBanner(
                windowId: originalItem.windowId,
                analysisAssetId: originalItem.analysisAssetId,
                startTime: originalItem.adStartTime,
                endTime: originalItem.adEndTime,
                ifCurrentEpisodeId: originalItem.episodeId,
                ifPlaybackLifecycleGeneration:
                    originalItem.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    originalItem.windowMaterialRevisionToken
            )
        }
        await persistenceGate.waitUntilStarted()

        let latest = makeSkipTestAdWindow(
            id: original.id,
            startTime: 72,
            endTime: 138,
            confidence: 0.95,
            decisionState: AdDecisionState.applied.rawValue
        )
        try await store.insertOrReplaceAdWindow(latest)
        await orchestrator.receiveAdWindows([latest])
        guard case .retireWindow = await probe.next() else {
            Issue.record("Expected stale card retirement")
            return
        }
        guard case let .present(latestItem) = await probe.next() else {
            Issue.record("Expected replacement Auto-Yes card")
            return
        }
        await persistenceGate.release()

        #expect(!(await staleResponse.value))
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: original.analysisAssetId
            ).isEmpty,
            "The stale receipt must not commit"
        )
        let durableLatest = try #require(
            try await store.fetchAdWindow(id: latest.id)
        )
        #expect(durableLatest.startTime == latest.startTime)
        #expect(durableLatest.endTime == latest.endTime)

        await orchestrator._setFeedbackPersistenceBarrierForTesting(nil)
        #expect(
            await orchestrator.confirmAutoSkippedBanner(
                windowId: latestItem.windowId,
                analysisAssetId: latestItem.analysisAssetId,
                startTime: latestItem.adStartTime,
                endTime: latestItem.adEndTime,
                ifCurrentEpisodeId: latestItem.episodeId,
                ifPlaybackLifecycleGeneration:
                    latestItem.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    latestItem.windowMaterialRevisionToken
            ),
            "The latest durable revision must remain actionable"
        )
        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: latest.analysisAssetId
        )
        #expect(receipts.count == 1)
        #expect(
            CorrectionScope.deserialize(receipts[0].scope)
                == .exactTimeSpan(
                    assetId: latest.analysisAssetId,
                    startTime: latest.startTime,
                    endTime: latest.endTime
                )
        )
    }

    @Test(
        "explicit receipt drives trust while replay, Corpus, Debug, and sharing remain response-independent",
        .timeLimit(.minutes(1))
    )
    func explicitReceiptEndToEndPrivacyFlow() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-r26-e2e"
        let episodeId = "episode-r26-e2e"
        try await store.insertAsset(
            AnalysisAsset(
                id: assetId,
                episodeId: episodeId,
                assetFingerprint:
                    "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                weakFingerprint: nil,
                sourceURL: "file:///test/r26-e2e.m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: 300
            )
        )
        let suggestion = makeSuggestWindow(
            id: "r26-e2e-suggestion",
            analysisAssetId: assetId,
            startTime: 60,
            endTime: 120
        )
        let unrelated = makeSkipTestAdWindow(
            id: "r26-e2e-unrelated",
            assetId: assetId,
            startTime: 180,
            endTime: 225,
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        try await store.insertAdWindow(suggestion)
        try await store.insertAdWindow(unrelated)

        let exportedAt = Date(timeIntervalSince1970: 1_800_000_000)
        let baselineDocs = try makeTempDir(prefix: "r26-e2e-before")
        let baselineCorpus = try await CorpusExporter.export(
            store: store,
            documentsURL: baselineDocs,
            now: exportedAt,
            dedupMemo: CorpusExportDedupMemo()
        )
        let baselineCorpusBytes = try Data(
            contentsOf: baselineCorpus.fileURL
        )
        let baselineDebug = try #require(
            await DebugEpisodeExportService.build(
                episodeTitle: "R26 E2E",
                podcastTitle: "R26 Podcast",
                analysisAssetId: assetId,
                episodeId: episodeId,
                store: store,
                exportedAt: exportedAt
            )
        )
        let baselineShare = try await store
            .exportCrossUserAnalysisSnapshot(
                assetId: assetId,
                podcastId: "podcast-r26-e2e",
                exportedAt: exportedAt,
                sourceAppBuild: "r26-e2e"
            )

        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        let trustDispatch = TestEventCounter()
        await orchestrator._setFalseNegativeSignalHandlerForTesting { _ in
            trustDispatch.increment()
        }
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: "podcast-r26-e2e",
            playbackLifecycleGeneration: 206
        )
        await orchestrator.receiveAdWindows([suggestion])
        #expect(
            await orchestrator.acceptSuggestedSkip(
                windowId: suggestion.id,
                ifCurrentEpisodeId: episodeId,
                ifPlaybackLifecycleGeneration: 206
            )
        )
        await trustDispatch.wait()

        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: assetId
        )
        let receipt = try #require(receipts.first)
        #expect(receipts.count == 1)
        #expect(receipt.source == .bannerSuggestionConfirmed)
        #expect(receipt.podcastId == "podcast-r26-e2e")
        let promotedId = try #require(
            receipt.targetRefs?.adWindowId
        )

        let responseDocs = try makeTempDir(prefix: "r26-e2e-after")
        let responseCorpus = try await CorpusExporter.export(
            store: store,
            documentsURL: responseDocs,
            now: exportedAt,
            dedupMemo: CorpusExportDedupMemo()
        )
        let responseCorpusBytes = try Data(
            contentsOf: responseCorpus.fileURL
        )
        #expect(responseCorpusBytes == baselineCorpusBytes)
        let corpusText = try #require(
            String(data: responseCorpusBytes, encoding: .utf8)
        )
        #expect(!corpusText.contains(receipt.id))
        #expect(!corpusText.contains(promotedId))

        let responseDebug = try #require(
            await DebugEpisodeExportService.build(
                episodeTitle: "R26 E2E",
                podcastTitle: "R26 Podcast",
                analysisAssetId: assetId,
                episodeId: episodeId,
                store: store,
                exportedAt: exportedAt
            )
        )
        #expect(responseDebug.content == baselineDebug.content)
        #expect(responseDebug.filename == baselineDebug.filename)

        let responseShare = try await store
            .exportCrossUserAnalysisSnapshot(
                assetId: assetId,
                podcastId: "podcast-r26-e2e",
                exportedAt: exportedAt,
                sourceAppBuild: "r26-e2e"
            )
        #expect(responseShare == baselineShare)

        let detection = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: .default
        )
        let replayResult = try await detection.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: 300
        )
        #expect(
            !replayResult.contains {
                $0.boundaryState == "correctionReplay"
            },
            "The explicit receipt must not create a correction-replay suggestion"
        )
    }

    @Test("Listen retirement removes live cue and rejects stale playback lifecycle")
    func liveListenRetirementIsCueImmediateAndLifecycleBound() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 19
        )
        let ad = makeSkipTestAdWindow(
            id: "listen-live-cue",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        await orchestrator.receiveAdWindows([ad])
        guard case let .present(item) = await probe.next() else {
            Issue.record("Expected Listen card material")
            return
        }
        #expect(!pushedCues.isEmpty)

        #expect(
            !(await orchestrator.retireLiveSkipForListen(
                windowId: item.windowId,
                analysisAssetId: try #require(item.analysisAssetId),
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                podcastId: item.podcastId,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 18,
                ifWindowMaterialRevisionToken:
                    try #require(item.windowMaterialRevisionToken)
            ))
        )
        #expect(!pushedCues.isEmpty,
                "A stale banner must not mutate current cues")

        #expect(
            await orchestrator.retireLiveSkipForListen(
                windowId: item.windowId,
                analysisAssetId: try #require(item.analysisAssetId),
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                podcastId: item.podcastId,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 19,
                ifWindowMaterialRevisionToken:
                    try #require(item.windowMaterialRevisionToken)
            )
        )
        #expect(pushedCues.isEmpty)
    }

    @Test("Listen retirement reservation permits a persistence retry only until completion")
    func liveListenRetirementReservationIsRetryable() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 27
        )
        let ad = makeSkipTestAdWindow(
            id: "listen-retry-reservation",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        await orchestrator.receiveAdWindows([ad])
        guard case let .present(item) = await probe.next() else {
            Issue.record("Expected retryable Listen card material")
            return
        }

        #expect(
            await orchestrator.retireLiveSkipForListen(
                windowId: item.windowId,
                analysisAssetId: try #require(item.analysisAssetId),
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                podcastId: item.podcastId,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 27,
                ifWindowMaterialRevisionToken:
                    try #require(item.windowMaterialRevisionToken)
            )
        )
        #expect(
            await orchestrator.retireLiveSkipForListen(
                windowId: item.windowId,
                analysisAssetId: try #require(item.analysisAssetId),
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                podcastId: item.podcastId,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 27,
                ifWindowMaterialRevisionToken:
                    try #require(item.windowMaterialRevisionToken)
            ),
            "a failed durable write must be able to retry after the cue was already retired"
        )

        await orchestrator.completeLiveSkipRetirementForListen(
            windowId: ad.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 27
        )
        #expect(
            !(await orchestrator.retireLiveSkipForListen(
                windowId: item.windowId,
                analysisAssetId: try #require(item.analysisAssetId),
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                podcastId: item.podcastId,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 27,
                ifWindowMaterialRevisionToken:
                    try #require(item.windowMaterialRevisionToken)
            )),
            "once the receipt is durable, the reverted window must not be consumed again"
        )
    }

    @Test("late skip persistence cannot resurrect explicit or producer-terminal rows")
    func staleAppliedPersistenceIsConditionedOnEligibleState() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())

        let reverted = makeSkipTestAdWindow(
            id: "stale-apply-after-listen",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.reverted.rawValue
        )
        let suppressed = makeSkipTestAdWindow(
            id: "stale-apply-after-suppression",
            startTime: 180,
            endTime: 240,
            confidence: 0.9,
            decisionState: AdDecisionState.suppressed.rawValue
        )
        try await store.insertAdWindow(reverted)
        try await store.insertAdWindow(suppressed)

        #expect(
            try await store.persistAppliedAdWindowIfEligible(
                windowId: reverted.id,
                analysisAssetId: "different-asset",
                expectedProducerRevision: reverted
            ) == false,
            "A stale promotion must remain scoped to the originating analysis asset"
        )
        #expect(
            try await store.persistAppliedAdWindowIfEligible(
                windowId: reverted.id,
                analysisAssetId: reverted.analysisAssetId,
                expectedProducerRevision: reverted
            ) == false
        )
        #expect(
            try await store.persistAppliedAdWindowIfEligible(
                windowId: suppressed.id,
                analysisAssetId: suppressed.analysisAssetId,
                expectedProducerRevision: suppressed
            ) == false
        )

        let rows = try await store.fetchAdWindows(assetId: "asset-1")
        let revertedRow = try #require(
            rows.first { $0.id == reverted.id }
        )
        let suppressedRow = try #require(
            rows.first { $0.id == suppressed.id }
        )
        #expect(revertedRow.decisionState == AdDecisionState.reverted.rawValue)
        #expect(revertedRow.wasSkipped == false)
        #expect(
            suppressedRow.decisionState
                == AdDecisionState.suppressed.rawValue
        )
        #expect(suppressedRow.wasSkipped == false)
    }

    @Test("eligible skip persistence atomically stamps state and receipt")
    func eligibleAppliedPersistenceUpdatesBothFields() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let confirmed = makeSkipTestAdWindow(
            id: "eligible-atomic-apply",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        try await store.insertAdWindow(confirmed)

        #expect(
            try await store.persistAppliedAdWindowIfEligible(
                windowId: confirmed.id,
                analysisAssetId: confirmed.analysisAssetId,
                expectedProducerRevision: confirmed
            )
        )
        let row = try #require(
            (try await store.fetchAdWindows(assetId: "asset-1"))
                .first { $0.id == confirmed.id }
        )
        #expect(row.decisionState == AdDecisionState.applied.rawValue)
        #expect(row.wasSkipped)
    }

    @Test("late skip persistence cannot promote a materially revised same-ID row")
    func appliedPersistenceIsProducerRevisionFenced() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let scheduledRevision = makeSkipTestAdWindow(
            id: "same-id-reconciled-before-apply",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        try await store.insertAdWindow(scheduledRevision)

        let replacementRevision = makeSkipTestAdWindow(
            id: scheduledRevision.id,
            startTime: 75,
            endTime: 150,
            confidence: 0.82,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        try await store.insertOrReplaceAdWindow(replacementRevision)

        #expect(
            try await store.persistAppliedAdWindowIfEligible(
                windowId: scheduledRevision.id,
                analysisAssetId: scheduledRevision.analysisAssetId,
                expectedProducerRevision: scheduledRevision
            ) == false
        )
        let row = try #require(
            try await store.fetchAdWindow(id: scheduledRevision.id)
        )
        #expect(row.startTime == replacementRevision.startTime)
        #expect(row.endTime == replacementRevision.endTime)
        #expect(
            row.decisionState == AdDecisionState.confirmed.rawValue
        )
        #expect(!row.wasSkipped)
    }

    @Test("suggestion produced without a banner host replays once when the host attaches")
    func pendingSuggestionReplaysWhenBannerHostAttaches() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1"
        )

        let suggest = makeSuggestWindow(id: "ad-suggest-while-away")
        await orchestrator.receiveAdWindows([suggest])
        #expect(
            await orchestrator.activeSuggestWindowIDs().contains(suggest.id),
            "The undecided window remains pending while no UI can answer it"
        )
        // playhead-d3g0: the span is PLAYED while no host exists. That
        // satisfies the position gate — replay is now gated on position rather
        // than dropped, and this test's subject (delivered exactly once when
        // the host attaches) is unchanged. The complementary case, a suggestion
        // whose span the playhead never reached NOT being replayed, is pinned
        // in `SuggestBannerEntryGateTests`.
        await enterSuggestSpan(orchestrator)

        let stream = await orchestrator.bannerItemStream()
        let probe = BoundedStreamProbe(stream)
        let replayed = await probe.next()
        #expect(replayed?.windowId == suggest.id)
        #expect(replayed?.episodeId == "episode-1",
                "Replay must preserve the episode identity used by queue gating")
        await orchestrator.acknowledgeSuggestedBannerDelivery(
            windowId: suggest.id,
            episodeId: replayed?.episodeId,
            playbackLifecycleGeneration:
                replayed?.playbackLifecycleGeneration
        )

        await orchestrator.declineSuggestedSkip(windowId: suggest.id)
        await orchestrator.receiveAdWindows([suggest])
        #expect(
            !(await orchestrator.activeSuggestWindowIDs().contains(suggest.id)),
            "Once replayed and neutrally retired, the same in-session window must not reappear"
        )
    }

    @Test("stale same-episode delivery acknowledgement does not suppress replacement replay")
    func staleDeliveryAcknowledgementDoesNotSuppressReplacementReplay() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 41
        )

        let suggest = makeSuggestWindow(id: "same-id-replacement")
        try await store.insertAdWindow(suggest)
        await orchestrator.receiveAdWindows([suggest])
        await enterSuggestSpan(orchestrator)

        let oldStream = await orchestrator.bannerItemStream()
        let oldProbe = BoundedStreamProbe(oldStream)
        let oldItem = await oldProbe.next()
        #expect(oldItem?.windowId == suggest.id)
        #expect(oldItem?.playbackLifecycleGeneration == 41)

        // Replay the same canonical episode and persisted window under a new
        // playback lifecycle before the old observer's actor-hop ack lands.
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 42
        )
        await orchestrator.receiveAdWindows([suggest])
        await orchestrator.acknowledgeSuggestedBannerDelivery(
            windowId: suggest.id,
            episodeId: oldItem?.episodeId,
            playbackLifecycleGeneration:
                oldItem?.playbackLifecycleGeneration
        )

        #expect(
            !(await orchestrator.acknowledgedSuggestWindowIDs())
                .contains(suggest.id),
            "A stale acknowledgement must not mark the replacement suggestion as already presented"
        )
        #expect(
            await orchestrator.activeSuggestWindowIDs()
                .contains(suggest.id),
            "The replacement suggestion must remain pending for replay to the new host"
        )

        await orchestrator.acknowledgeSuggestedBannerDelivery(
            windowId: suggest.id,
            episodeId: "episode-1",
            playbackLifecycleGeneration: 42
        )
        #expect(
            await orchestrator.acknowledgedSuggestWindowIDs()
                .contains(suggest.id),
            "The replacement lifecycle's own acknowledgement must still be accepted"
        )
    }

    @Test("same-ID suggestion revision rejects stale ack, Yes, and No")
    func sameIDSuggestionRevisionRejectsStalePresentationActions() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 43
        )

        let stream = await orchestrator.bannerItemStream()
        let probe = BoundedStreamProbe(stream)
        let original = makeAnchoredSuggestWindow(
            id: "same-id-same-lifecycle",
            startTime: 60.12341,
            endTime: 75.9876
        )
        try await store.insertAdWindow(original)
        await orchestrator.receiveAdWindows([original])
        await enterSuggestSpan(orchestrator, at: 61)
        let oldItem = try #require(await probe.next())
        let oldRevision = try #require(
            oldItem.suggestionRevisionToken
        )

        let revised = makeAnchoredSuggestWindow(
            id: original.id,
            startTime: 60.1234,
            endTime: 75.9876
        )
        try await store.insertOrReplaceAdWindow(revised)
        await orchestrator.receiveAdWindows([revised])
        // A material revision RE-ARMS (playhead-d3g0), so the replacement card
        // needs its own entry — which is right: the new span is a new question.
        await enterSuggestSpan(orchestrator, at: 61)
        let revisedItem = try #require(await probe.next())
        let revisedToken = try #require(
            revisedItem.suggestionRevisionToken
        )
        #expect(revisedToken != oldRevision)
        #expect(revisedItem.adStartTime == 60.1234)
        #expect(revisedItem.adEndTime == 75.9876)

        await orchestrator.acknowledgeSuggestedBannerDelivery(
            windowId: original.id,
            episodeId: oldItem.episodeId,
            playbackLifecycleGeneration:
                oldItem.playbackLifecycleGeneration,
            suggestionRevisionToken: oldRevision
        )
        #expect(
            !(await orchestrator.acknowledgedSuggestWindowIDs())
                .contains(original.id),
            "The old revision's ack must not suppress the replacement"
        )

        await orchestrator.acceptSuggestedSkip(
            windowId: original.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 43,
            ifSuggestionRevisionToken: oldRevision
        )
        await orchestrator.declineSuggestedSkip(
            windowId: original.id,
            isExplicitDenial: true,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 43,
            ifSuggestionRevisionToken: oldRevision
        )
        #expect(
            await orchestrator.activeSuggestWindowIDs()
                .contains(original.id),
            "Stale Yes/No actions must leave the current revision pending"
        )
        let beforeCurrentAction = try await store.fetchAdWindows(
            assetId: "asset-1"
        )
        let originalRow = beforeCurrentAction.first {
            $0.id == original.id
        }
        #expect(originalRow?.userDismissedBanner == false)
        #expect(
            originalRow?.decisionState
                == AdDecisionState.candidate.rawValue
        )

        await orchestrator.acknowledgeSuggestedBannerDelivery(
            windowId: original.id,
            episodeId: revisedItem.episodeId,
            playbackLifecycleGeneration:
                revisedItem.playbackLifecycleGeneration,
            suggestionRevisionToken: revisedToken
        )
        await orchestrator.acceptSuggestedSkip(
            windowId: original.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 43,
            ifSuggestionRevisionToken: revisedToken
        )

        let persisted = try await store.fetchAdWindows(
            assetId: "asset-1"
        )
        let promoted = persisted.first {
            $0.id != original.id
                && $0.wasSkipped
                && $0.startTime == 60.1234
                && $0.endTime == 75.9876
        }
        #expect(
            promoted != nil,
            "The current revision's Yes must apply its revised span"
        )
        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: "asset-1"
        )
        #expect(receipts.count == 1)
        #expect(receipts.first?.source == .bannerSuggestionConfirmed)
        #expect(
            receipts.first?.targetRefs?.exactFeedbackSpan?.matches(
                startTime: 60.1234,
                endTime: 75.9876
            ) == true
        )
    }

    @Test("direct episode replacement clears accepted-suggestion race guards")
    func directEpisodeReplacementClearsAcceptedSuggestionIDs() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "episode-1")
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-2", episodeId: "episode-2")
        )
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 51
        )

        let reusedID = "producer-stable-window-id"
        let firstEpisodeSuggestion = makeSuggestWindow(id: reusedID)
        // playhead-auz3: without the durable row the accept below fails inside
        // `persistAcceptedSuggestionIfCurrent`, and its catch path removes the
        // id from `recentlyAcceptedSuggestIds` while episode 1 is still live —
        // i.e. the guard is already empty before `beginEpisode` runs, and this
        // test would stay green with the clear it is named for deleted.
        try await store.insertAdWindow(firstEpisodeSuggestion)
        await orchestrator.receiveAdWindows([firstEpisodeSuggestion])
        let accepted = await orchestrator.acceptSuggestedSkip(
            windowId: reusedID,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 51
        )
        #expect(
            accepted,
            "Fixture premise: episode 1's Yes must actually commit, so the id is really sitting in the accepted-suggest guard when the replacement lands"
        )

        // Production can switch episodes by calling beginEpisode directly;
        // endEpisode is not guaranteed to run between those calls. A producer
        // may reuse an otherwise-stable window id under a different asset.
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2",
            episodeId: "episode-2",
            podcastId: "podcast-2",
            playbackLifecycleGeneration: 52
        )
        let secondEpisodeWindow = makeSkipTestAdWindow(
            id: reusedID,
            assetId: "asset-2",
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        await orchestrator.receiveAdWindows([secondEpisodeWindow])

        #expect(
            await orchestrator.activeWindowIDs().contains(reusedID),
            "An accepted id from episode 1 must not suppress episode 2"
        )
    }

    @Test(
        "stale suggest completion cannot clear replacement provisional ownership",
        .timeLimit(.minutes(1))
    )
    func staleSuggestCompletionPreservesReplacementProvisionalOwnership()
        async throws
    {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-1",
                episodeId: "episode-1"
            )
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-2",
                episodeId: "episode-2"
            )
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        let sourceGate = ControlledAsyncGate()
        await orchestrator._setSuggestPersistenceBarrierForTesting {
            await sourceGate.wait()
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            playbackLifecycleGeneration: 61
        )

        let reusedID = "provisional-reused-id"
        let sourceSuggestion = makeSuggestWindow(id: reusedID)
        try await store.insertAdWindow(sourceSuggestion)
        await orchestrator.receiveAdWindows([sourceSuggestion])
        let sourceResponse = Task {
            await orchestrator.acceptSuggestedSkip(
                windowId: reusedID,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 61
            )
        }
        await sourceGate.waitUntilStarted()

        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2",
            episodeId: "episode-2",
            playbackLifecycleGeneration: 62
        )
        let replacementGate = ControlledAsyncGate()
        await orchestrator._setSuggestPersistenceBarrierForTesting {
            await replacementGate.wait()
        }
        let replacementSuggestion = makeSuggestWindow(
            id: reusedID,
            analysisAssetId: "asset-2",
            startTime: 180,
            endTime: 240
        )
        // Production persists the replacement producer row before delivering
        // it. Because IDs are producer-owned, that can replace Episode A's
        // globally-primary same-ID row while A's action is still suspended.
        try await store.insertOrReplaceAdWindow(replacementSuggestion)
        await orchestrator.receiveAdWindows([replacementSuggestion])
        let replacementResponse = Task {
            await orchestrator.acceptSuggestedSkip(
                windowId: reusedID,
                ifCurrentEpisodeId: "episode-2",
                ifPlaybackLifecycleGeneration: 62
            )
        }
        await replacementGate.waitUntilStarted()

        await sourceGate.release()
        #expect(
            !(await sourceResponse.value),
            "Episode A's transaction must roll back after its source row is replaced"
        )
        #expect(
            await orchestrator
                ._isSuggestResolutionProvisionalForTesting(id: reusedID),
            "Episode A's completion must not clear episode B's in-flight ownership for a reused producer ID"
        )

        await replacementGate.release()
        #expect(await replacementResponse.value)
        #expect(
            !(await orchestrator
                ._isSuggestResolutionProvisionalForTesting(id: reusedID))
        )
    }

    @Test("episode-bound suggest actions reject stale banner identities")
    func episodeBoundSuggestActionsRejectStaleIdentity() async throws {
        let store = try await makeTestStore()
        // playhead-ugy4: `asset-1` is durably OWNED by "episode-stale" while
        // the orchestrator plays it under the identity "episode-current".
        // The disagreement is deliberate. The durable ownership check inside
        // `persistAcceptedSuggestionIfCurrent` /
        // `persistDeclinedSuggestionIfCurrent` keys on the ASSET's episode,
        // so this fixture makes it ACCEPT the stale card — leaving the
        // orchestrator's own `activeEpisodeId == expectedEpisodeId` guard,
        // the guard this test names, as the only thing that can reject it.
        // With the asset owned by the ACTIVE episode both guards refuse, and
        // deleting the orchestrator one changes nothing observable.
        //
        // Scope, so a later reader does not over-read this: what is pinned
        // here is the refusal of a WRONGLY-STAMPED caller. It is not the
        // episode-switch race — a real `beginEpisode` also clears
        // `suggestRevisionTokensByWindowId`, so after a genuine switch the
        // token lookup in the same guard chain refuses the stale tap on its
        // own. The asset-ownership half of the pair is pinned separately by
        // `explicitResponseRequiresAssetEpisodeOwnership`, which uses the
        // inverse fixture; neither test can drift into covering the other.
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-stale")
        )
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-current",
            podcastId: "podcast-current"
        )
        let acceptCandidate = makeSuggestWindow(id: "suggest-stale-accept")
        let declineCandidate = makeSuggestWindow(id: "suggest-stale-decline")
        try await store.insertAdWindow(acceptCandidate)
        try await store.insertAdWindow(declineCandidate)
        await orchestrator.receiveAdWindows([
            acceptCandidate,
            declineCandidate
        ])

        #expect(
            !(await orchestrator.acceptSuggestedSkip(
                windowId: acceptCandidate.id,
                ifCurrentEpisodeId: "episode-stale"
            )),
            "a Yes stamped with a stale episode identity must be refused"
        )
        #expect(
            !(await orchestrator.declineSuggestedSkip(
                windowId: declineCandidate.id,
                isExplicitDenial: true,
                ifCurrentEpisodeId: "episode-stale"
            )),
            "a No stamped with a stale episode identity must be refused"
        )

        let remaining = await orchestrator.activeSuggestWindowIDs()
        #expect(remaining.contains(acceptCandidate.id))
        #expect(remaining.contains(declineCandidate.id))

        // `recoverFailedProvisionalSuggestion` restores the entry of an
        // action that RAN and then failed, so `remaining` alone cannot tell
        // "refused before acting" from "acted and rolled back". The
        // assertions below can only hold if neither action ever ran: a
        // promotion installs a UUID-keyed managed window, and an explicit
        // denial stamps the durable row.
        #expect(
            (await orchestrator.activeWindowIDs()).isEmpty,
            "a stale Yes must not promote a managed window"
        )
        let rows = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(
            rows.count == 2,
            "a stale Yes must not insert a promoted row; got \(rows.count)"
        )
        for row in rows {
            #expect(
                row.decisionState == AdDecisionState.candidate.rawValue,
                "\(row.id) must keep its candidate state; got \(row.decisionState)"
            )
            #expect(
                !row.userDismissedBanner,
                "\(row.id) must not be stamped as explicitly dismissed"
            )
        }
    }

    @Test("explicit suggest No persists a falsePositive correction and sets userDismissedBanner=1")
    func suggestDenialPersistsFalsePositiveCorrection() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "asset-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let suggest = makeSuggestWindow(
            id: "ad-suggest-fp",
            startTime: 60.1234,
            endTime: 75.9876
        )
        try await store.insertAdWindow(suggest)
        await orchestrator.receiveAdWindows([suggest])
        let decisionCountBeforeFeedback =
            await orchestrator.getDecisionLog().count
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-fp"),
                "markOnly window must enter the suggest tier")

        // User answers No on the suggest banner.
        await orchestrator.declineSuggestedSkip(windowId: "ad-suggest-fp", isExplicitDenial: true)

        // The suggest entry is cleared.
        #expect(!(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-fp")),
                "denied suggest window must be removed from the suggest set")

        // A producer can redeliver a stale object after persistence completes,
        // or recompute that row in-place with a changed span. The explicit No
        // applies to the producer ID, so neither delivery may recreate it
        // during this episode.
        await orchestrator.receiveAdWindows([suggest])
        let revisedAfterDenial = makeSuggestWindow(
            id: suggest.id,
            startTime: 65,
            endTime: 125
        )
        await orchestrator.receiveAdWindows([revisedAfterDenial])
        #expect(
            !(await orchestrator.activeSuggestWindowIDs()
                .contains(suggest.id)),
            "an explicit No must suppress exact and revised same-ID redelivery"
        )

        let eligibleAfterDenial = makeSuggestWindow(
            id: suggest.id,
            confidence: 0.99,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([eligibleAfterDenial])
        await orchestrator.receiveAdDecisionResults([
            AdDecisionResult(
                id: suggest.id,
                analysisAssetId: "asset-1",
                startTime: 75,
                endTime: 135,
                skipConfidence: 0.99,
                eligibilityGate: .eligible,
                recomputationRevision: 2
            )
        ])
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(suggest.id),
            "An explicit No must block same-ID eligible AdWindow and fusion redelivery"
        )
        #expect(
            pushedCues.isEmpty,
            "An explicit No must prevent same-ID eligible redelivery from auto-skipping"
        )

        // Exactly one falsePositive correction is persisted, over the window's
        // span, with causal attribution and target refs.
        let corrections = try await correctionStore.activeCorrections(
            for: "asset-1"
        )
        #expect(corrections.count == 1,
                "one denial must persist exactly one correction; got \(corrections.count)")
        let correction = try #require(corrections.first)
        #expect(correction.correctionType == .falsePositive,
                "denial must record a .falsePositive correction; got \(String(describing: correction.correctionType))")
        #expect(correction.source == .bannerSuggestionDenied)
        #expect(correction.source?.kind == .falsePositive,
                "source must map to the false-positive kind so the materializer counts it as a revert")
        #expect(correction.targetRefs?.adWindowId == suggest.id)
        #expect(
            await orchestrator.getDecisionLog().count
                == decisionCountBeforeFeedback,
            "Suggest No must not add a detailed decision-log record"
        )
        #expect(correction.causalSource == .specialist,
                "specialist-composed mark must attribute to .specialist; got \(String(describing: correction.causalSource))")
        #expect(correction.targetRefs?.sponsorEntity == "red bull",
                "target refs must carry the normalized brand for hard-negative mining; got \(String(describing: correction.targetRefs?.sponsorEntity))")
        #expect(correction.podcastId == "podcast-1")
        #expect(correction.targetRefs?.domain == "podcast-1",
                "denial attribution must remain bound to the source podcast")
        #expect(
            correction.targetRefs?.exactFeedbackSpan?.matches(
                startTime: 60.1234,
                endTime: 75.9876
            ) == true
        )
        if case .exactTimeSpan(_, let s, let e) = CorrectionScope.deserialize(correction.scope) {
            #expect(s == 60.123, "scope remains a canonical three-decimal envelope")
            #expect(e == 75.988, "scope remains a canonical three-decimal envelope")
        } else {
            Issue.record("Expected exactTimeSpan scope from the denial, got \(correction.scope)")
        }

        // userDismissedBanner=1 and decisionState=.reverted on the row.
        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        let row = persisted.first { $0.id == "ad-suggest-fp" }
        #expect(row?.userDismissedBanner == true,
                "denied window row must carry userDismissedBanner=1")
        #expect(row?.decisionState == AdDecisionState.reverted.rawValue,
                "denied window must persist as .reverted so it does not resurface; got \(row?.decisionState ?? "<missing>")")
    }

    @Test("Suggest-No row mutation rolls back atomically on second-write failure")
    func suggestDenialRowMutationIsAtomic() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let suggest = makeSuggestWindow(id: "atomic-suggest-no")
        try await store.insertAdWindow(suggest)
        let correction = CorrectionEvent(
            analysisAssetId: suggest.analysisAssetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: suggest.analysisAssetId,
                startTime: suggest.startTime,
                endTime: suggest.endTime
            ).serialized,
            source: .manualVeto,
            correctionType: .falsePositive
        )
        try await store.execForTesting(
            """
            CREATE TRIGGER fail_atomic_suggest_no
            BEFORE UPDATE OF decisionState ON ad_windows
            WHEN NEW.id = 'atomic-suggest-no'
              AND NEW.decisionState = 'reverted'
            BEGIN
                SELECT RAISE(ABORT, 'injected second-write failure');
            END
            """
        )

        var didThrow = false
        do {
            try await store.persistDeclinedSuggestion(
                windowId: suggest.id,
                analysisAssetId: suggest.analysisAssetId,
                correction: correction
            )
        } catch {
            didThrow = true
        }
        #expect(didThrow)

        let row = try #require(
            (try await store.fetchAdWindows(assetId: "asset-1"))
                .first { $0.id == suggest.id }
        )
        #expect(
            row.userDismissedBanner == false,
            "The first write must roll back when the revert write fails"
        )
        #expect(
            row.decisionState == AdDecisionState.candidate.rawValue,
            "A failed transaction must leave the original row actionable"
        )
        #expect(
            try await correctionStore.activeCorrections(for: "asset-1")
                .isEmpty,
            "The correction append must roll back with the failed row mutation"
        )

        try await store.execForTesting("DROP TRIGGER fail_atomic_suggest_no")
        _ = try await store.persistDeclinedSuggestion(
            windowId: suggest.id,
            analysisAssetId: suggest.analysisAssetId,
            correction: correction
        )
        let corrections = try await correctionStore.activeCorrections(
            for: "asset-1"
        )
        #expect(corrections.count == 1)
        #expect(
            corrections.first?.submissionCount == 1,
            "The rolled-back attempt must not increment durable submissions"
        )
    }

    @Test("Suggest-No transaction cannot mutate a replacement asset's same-ID row")
    func suggestDenialIsAssetScoped() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-1",
                episodeId: "episode-1"
            )
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-2",
                episodeId: "episode-2"
            )
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let reusedID = "same-id-decline-scope"
        let source = makeSuggestWindow(id: reusedID)
        try await store.insertAdWindow(source)
        let replacement = makeSuggestWindow(
            id: reusedID,
            analysisAssetId: "asset-2",
            startTime: 180,
            endTime: 240
        )
        try await store.insertOrReplaceAdWindow(replacement)
        let sourceCorrection = CorrectionEvent(
            analysisAssetId: source.analysisAssetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: source.analysisAssetId,
                startTime: source.startTime,
                endTime: source.endTime
            ).serialized,
            source: .manualVeto,
            correctionType: .falsePositive
        )

        var didThrow = false
        do {
            _ = try await store.persistDeclinedSuggestion(
                windowId: reusedID,
                analysisAssetId: source.analysisAssetId,
                correction: sourceCorrection
            )
        } catch {
            didThrow = true
        }
        #expect(
            didThrow,
            "The stale source transaction must reject the replacement asset"
        )

        let replacementRow = try #require(
            (try await store.fetchAdWindows(assetId: "asset-2"))
                .first { $0.id == reusedID }
        )
        #expect(replacementRow.decisionState == replacement.decisionState)
        #expect(replacementRow.userDismissedBanner == false)
        #expect(
            try await correctionStore.activeCorrections(for: "asset-1")
                .isEmpty,
            "The source correction append must roll back with the scoped update"
        )
    }

    @Test("Auto-No transaction cannot mutate a replacement asset's same-ID row")
    func automaticDenialIsAssetScoped() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-1",
                episodeId: "episode-1"
            )
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-2",
                episodeId: "episode-2"
            )
        )
        let reusedID = "same-id-auto-no-scope"
        let source = makeSuggestWindow(id: reusedID)
        try await store.insertAdWindow(source)
        let replacement = makeSuggestWindow(
            id: reusedID,
            analysisAssetId: "asset-2",
            startTime: 180,
            endTime: 240
        )
        try await store.insertOrReplaceAdWindow(replacement)

        var didThrow = false
        do {
            _ = try await store.persistRevertedAdWindow(
                windowId: reusedID,
                analysisAssetId: source.analysisAssetId
            )
        } catch {
            didThrow = true
        }

        #expect(didThrow)
        let replacementRow = try #require(
            try await store.fetchAdWindow(id: reusedID)
        )
        #expect(replacementRow.analysisAssetId == "asset-2")
        #expect(replacementRow.decisionState == replacement.decisionState)
    }

    @Test("declineSuggestedSkip auto-fade (isExplicitDenial:false) records NO correction and leaves userDismissedBanner=0")
    func suggestAutoFadeRecordsNothing() async throws {
        let store = try await makeTestStore()
        // playhead-i08e (third pass): the asset row must declare the SAME
        // episode `beginEpisode` announces below, exactly as this test's
        // explicit-denial sibling `suggestDenialPersistsFalsePositiveCorrection`
        // already does. With the fixture's default `"ep-1"` against
        // `beginEpisode(episodeId: "asset-1")`, `feedbackAssetMatches` refused
        // `persistDeclinedSuggestionIfCurrent` on ownership grounds — so a
        // regression that treated a passive auto-fade as an explicit denial
        // would ALSO have written nothing, and all three negative assertions
        // below would have held for a reason unrelated to the contract they
        // name. Aligning the ids makes the persistence path genuinely
        // reachable, so only `isExplicitDenial == false` keeps it silent.
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "asset-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(store: store, correctionStore: correctionStore)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let suggest = makeSuggestWindow(id: "ad-suggest-fade")
        try await store.insertAdWindow(suggest)
        await orchestrator.receiveAdWindows([suggest])

        // Banner auto-fades (no user tap) — the default path.
        await orchestrator.declineSuggestedSkip(windowId: "ad-suggest-fade")

        #expect(!(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-fade")),
                "faded suggest window must still be dropped from the suggest set")

        let corrections = try await correctionStore.activeCorrections(for: "asset-1")
        #expect(corrections.isEmpty,
                "a passive auto-fade must NOT mint a hard negative; got \(corrections.count) corrections")

        let row = (try await store.fetchAdWindows(assetId: "asset-1")).first { $0.id == "ad-suggest-fade" }
        #expect(row?.userDismissedBanner == false,
                "auto-fade must not set userDismissedBanner")
        #expect(row?.decisionState == AdDecisionState.candidate.rawValue,
                "auto-fade must not flip the row to .reverted; got \(row?.decisionState ?? "<missing>")")
    }

    @Test("two same-span suggest denials retain two exact private receipts and the unanswered projection")
    func suggestDenialRetainsExactWindowOwnership() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "asset-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(store: store, correctionStore: correctionStore)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Two distinct suggest windows over the SAME span. The generic v23
        // identity would collapse them, but private receipts must retain the
        // exact target ownership of both answers.
        let w1 = makeSuggestWindow(id: "ad-suggest-dup-1")
        let w2 = makeSuggestWindow(id: "ad-suggest-dup-2")
        try await store.insertAdWindow(w1)
        try await store.insertAdWindow(w2)
        await orchestrator.receiveAdWindows([w1, w2])

        await orchestrator.declineSuggestedSkip(windowId: "ad-suggest-dup-1", isExplicitDenial: true)
        await orchestrator.declineSuggestedSkip(windowId: "ad-suggest-dup-2", isExplicitDenial: true)

        let corrections = try await correctionStore.activeCorrections(
            for: "asset-1"
        )
        #expect(corrections.count == 2)
        #expect(corrections.allSatisfy { $0.submissionCount == 1 })
        #expect(
            Set(corrections.compactMap { $0.targetRefs?.adWindowId })
                == Set([w1.id, w2.id])
        )
        #expect(corrections.allSatisfy {
            $0.targetRefs?.explicitFeedbackDetectionProjection != nil
        })

        let responded = try await store.fetchAdWindows(assetId: "asset-1")
        let before = ExplicitBannerFeedbackPrivacy
            .responseIndependentProjection(
                windows: [w1, w2],
                corrections: []
            )
        let after = try await store.responseIndependentAdWindows(
            analysisAssetId: "asset-1"
        )
        #expect(
            responded.allSatisfy {
                $0.decisionState == AdDecisionState.reverted.rawValue
            }
        )
        #expect(
            after?.map(ExplicitFeedbackDetectionProjection.init)
                == before?.map(ExplicitFeedbackDetectionProjection.init)
        )
    }

    @Test("causalSource(forMetadataSource:) maps producer tags to the responsible source")
    func causalSourceMappingIsFaithful() {
        #expect(SkipOrchestrator.causalSource(forMetadataSource: SpecialistMarkComposer.metadataSource) == .specialist)
        #expect(SkipOrchestrator.causalSource(forMetadataSource: "foundationModels") == .foundationModel)
        #expect(SkipOrchestrator.causalSource(forMetadataSource: "none") == .foundationModel)
        #expect(SkipOrchestrator.causalSource(forMetadataSource: "fallback") == .foundationModel)
    }
}
