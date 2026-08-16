// PartialActionDismissTests.swift
// playhead-zxqj — "i had a number of times where i couldnt dismiss an ad
// usually when i had already dismissed part of the window (or confirmed part)"
// (Dan, 2026-08-15).
//
// The gesture is: transcript sheet -> "Not ad" -> select rows -> Done ->
// "Dismiss ad". It ends in `SkipOrchestrator.revertByTimeRange`, which answers
// a `Bool` that the sheet swallowed. Nothing happened, and nothing said so.
//
// WHAT THIS SUITE PINS, and why the last test is the one that failed:
//
//   `persistRevertedAdWindowsIfCurrent` is ALL-OR-NOTHING. One expected row it
//   will not take — already `reverted` or `suppressed`, missing, or moved —
//   rolls the WHOLE transaction back. Until this bead the live dictionaries
//   fed that transaction directly, and `suggestWindows` is filtered by neither
//   decision state nor durable existence. So ONE stale live entry made every
//   later dismiss over any range that touched it fail, including for every
//   OTHER window the gesture could have reverted.
//
//   The entry gets stale by the listener's own earlier action: the revert's
//   live cleanup skipped any suggest entry whose revision had changed while
//   the durable transaction was in flight, so the id survived in the
//   actionable set with its durable row already `reverted`.
//
//   That is a partial user action making the remaining part unreachable —
//   which is the property this bead exists to forbid.
//
// The first four tests are the shapes that turned out to be FINE. They are
// kept deliberately: each one was a plausible reading of the report, each was
// checked against the device pull's own window stacks, and each passed before
// the fix as well as after. A suite that only contains the shape that broke
// cannot tell a later reader which readings were already excluded.

import Foundation
import Testing

@testable import Playhead

/// Minimal rendezvous gate: the barrier parks, the test drives, then releases.
///
/// A local copy rather than a shared helper because the other suite's
/// `ControlledAsyncGate` is `private` to its file; duplicating twenty lines is
/// cheaper than widening another suite's surface for one caller.
actor ZxqjGate {
    private var started: [CheckedContinuation<Void, Never>] = []
    private var waiters: [CheckedContinuation<Void, Never>] = []
    private var didStart = false
    private var isReleased = false

    func wait() async {
        didStart = true
        for continuation in started { continuation.resume() }
        started.removeAll()
        guard !isReleased else { return }
        await withCheckedContinuation { waiters.append($0) }
    }

    func waitUntilStarted() async {
        guard !didStart else { return }
        await withCheckedContinuation { started.append($0) }
    }

    func release() {
        isReleased = true
        for continuation in waiters { continuation.resume() }
        waiters.removeAll()
    }
}

@Suite("A partial action never makes the rest undismissable (playhead-zxqj)")
struct PartialActionDismissTests {

    private func vetoSpan(
        assetId: String,
        start: Double,
        end: Double
    ) -> DecodedSpan {
        DecodedSpan(
            id: String(format: "%@-veto-%.3f-%.3f", assetId, start, end),
            assetId: assetId,
            firstAtomOrdinal: 0,
            lastAtomOrdinal: Int.max,
            startTime: start,
            endTime: end,
            anchorProvenance: []
        )
    }

    private func markOnlyWindow(
        id: String,
        start: Double,
        end: Double
    ) -> AdWindow {
        makeSkipTestAdWindow(
            id: id,
            startTime: start,
            endTime: end,
            confidence: 1.0,
            decisionState: AdDecisionState.candidate.rawValue,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
    }

    /// Byte-for-byte the row `AdDetectionService.recordUserMarkedAd` persists.
    private func userMarkWindow(
        id: String,
        start: Double,
        end: Double
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: "asset-1",
            startTime: start,
            endTime: end,
            confidence: 1.0,
            boundaryState: "userMarked",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "userCorrection",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: start,
            metadataSource: "userCorrection",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
    }

    private func makeOrchestrator(
        store: AnalysisStore
    ) async -> SkipOrchestrator {
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.setSkipCueHandler { _ in }
        return orchestrator
    }

    // 1: confirm a suggest banner, then dismiss the same span in the SAME
    // session. This is Dan's "(or confirmed part)".
    @Test("confirming a suggestion does not block dismissing the same span", .timeLimit(.minutes(1)))
    func confirmThenDismissSameSpan() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        try await store.insertAdWindow(
            markOnlyWindow(id: "suggest-1", start: 100, end: 160)
        )
        let orchestrator = await makeOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 3
        )
        let accepted = await orchestrator.acceptSuggestedSkip(
            windowId: "suggest-1"
        )
        #expect(accepted, "precondition: the confirm must commit")

        let vetoed = await orchestrator.revertByTimeRange(
            start: 100,
            end: 160,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 3,
            correctionSpan: vetoSpan(assetId: "asset-1", start: 100, end: 160)
        )
        #expect(vetoed, "dismissing after confirming the same span must commit")
    }

    // 2: mark an ad, then dismiss part of it in the SAME session.
    @Test("marking an ad does not block dismissing part of it", .timeLimit(.minutes(1)))
    func markThenDismissPart() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        let orchestrator = await makeOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 3
        )
        try await store.insertAdWindow(
            userMarkWindow(id: "user-mark-1", start: 100, end: 160)
        )
        await orchestrator.injectUserMarkedAd(
            start: 100,
            end: 160,
            analysisAssetId: "asset-1",
            windowId: "user-mark-1"
        )

        let vetoed = await orchestrator.revertByTimeRange(
            start: 120,
            end: 140,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 3,
            correctionSpan: vetoSpan(assetId: "asset-1", start: 120, end: 140)
        )
        #expect(vetoed, "dismissing part of a just-marked region must commit")
    }

    // 3: dismiss part of a window, then dismiss another part.
    @Test("dismissing part does not block dismissing the rest", .timeLimit(.minutes(1)))
    func dismissPartThenDismissRest() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        try await store.insertAdWindow(
            markOnlyWindow(id: "w-a", start: 100, end: 160)
        )
        try await store.insertAdWindow(
            markOnlyWindow(id: "w-b", start: 150, end: 200)
        )
        let orchestrator = await makeOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 3
        )
        let first = await orchestrator.revertByTimeRange(
            start: 100,
            end: 130,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 3,
            correctionSpan: vetoSpan(assetId: "asset-1", start: 100, end: 130)
        )
        #expect(first, "precondition: the first dismiss must commit")

        let second = await orchestrator.revertByTimeRange(
            start: 155,
            end: 190,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 3,
            correctionSpan: vetoSpan(assetId: "asset-1", start: 155, end: 190)
        )
        #expect(second, "dismissing the rest must commit")
    }

    // 4: the field stack on C0610BF9 at 884.8-914.8 —
    // suppressed(original) + confirmed(promoted) + applied(userMark)
    // + confirmed(userMark), then a dismiss over a sub-range.
    @Test("the 08-15 device window stack stays dismissable", .timeLimit(.minutes(1)))
    func fieldStackStaysDismissable() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        try await store.insertAdWindow(
            markOnlyWindow(id: "suggest-1", start: 884.7, end: 914.8)
        )
        let orchestrator = await makeOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 3
        )
        #expect(await orchestrator.acceptSuggestedSkip(windowId: "suggest-1"))

        for markId in ["user-mark-1", "user-mark-2"] {
            try await store.insertAdWindow(
                userMarkWindow(id: markId, start: 884.7, end: 914.8)
            )
            await orchestrator.injectUserMarkedAd(
                start: 884.7,
                end: 914.8,
                analysisAssetId: "asset-1",
                windowId: markId
            )
        }

        let vetoed = await orchestrator.revertByTimeRange(
            start: 890,
            end: 900,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 3,
            correctionSpan: vetoSpan(assetId: "asset-1", start: 890, end: 900)
        )
        #expect(vetoed, "a sub-range of the field stack must be dismissable")
    }

    // 5 — THE ONE THAT FAILED. A producer revision lands while the FIRST dismiss is committing, so
    // the live cleanup skips the suggest entry it just reverted in the store.
    // Every later dismiss over any range that overlaps that entry is refused
    // ENTIRELY — including the windows it could have reverted.
    @Test("a stale live suggest entry cannot refuse a later dismiss", .timeLimit(.minutes(1)))
    func staleSuggestEntryCannotRefuseALaterDismiss() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        try await store.insertAdWindow(
            markOnlyWindow(id: "w-a", start: 100, end: 160)
        )
        try await store.insertAdWindow(
            markOnlyWindow(id: "w-b", start: 150, end: 200)
        )
        let orchestrator = await makeOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 3
        )

        let gate = ZxqjGate()
        await orchestrator._setRevertPersistenceBarrierForTesting {
            await gate.wait()
        }
        let gesture = Task {
            await orchestrator.revertByTimeRange(
                start: 100,
                end: 130,
                analysisAssetId: "asset-1",
                podcastId: "podcast-1",
                ifCurrentEpisodeId: "ep-1",
                ifPlaybackLifecycleGeneration: 3,
                correctionSpan: vetoSpan(
                    assetId: "asset-1", start: 100, end: 130
                )
            )
        }
        await gate.waitUntilStarted()
        // A materially-changed same-ID revision arrives while the durable
        // transaction is parked. Production shape: a final-pass refinement.
        let refreshed = makeSkipTestAdWindow(
            id: "w-a",
            startTime: 100,
            endTime: 160,
            confidence: 0.9,
            decisionState: AdDecisionState.candidate.rawValue,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        await orchestrator.receiveAdWindows([refreshed])
        await orchestrator._setRevertPersistenceBarrierForTesting(nil)
        await gate.release()
        #expect(await gesture.value, "precondition: the first dismiss commits")

        let leftover = await orchestrator._suggestWindowForTesting(id: "w-a")
        #expect(
            leftover == nil,
            "a committed revert is terminal for the producer ID: the live suggest entry must go"
        )

        let second = await orchestrator.revertByTimeRange(
            start: 155,
            end: 190,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 3,
            correctionSpan: vetoSpan(assetId: "asset-1", start: 155, end: 190)
        )
        #expect(second, "the next dismiss must not be refused by the entry the first one left behind")

        let after = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(
            after.first { $0.id == "w-b" }?.decisionState
                == AdDecisionState.reverted.rawValue,
            "w-b — a different window, which the gesture could always have reverted — must be reverted"
        )
    }

    // 6: the instrument. A rule that never fires is indistinguishable from a
    // codebase with no violations, so prove it fires by making it fire — in
    // BOTH directions, because a recorder wired only to the failure path
    // cannot tell "refused" from "not instrumented yet".
    @Test("every dismiss records its outcome, committed or refused", .timeLimit(.minutes(1)))
    func everyDismissRecordsItsOutcome() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        try await store.insertAdWindow(
            markOnlyWindow(id: "w-a", start: 100, end: 160)
        )
        let orchestrator = await makeOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 3
        )

        #expect(
            await orchestrator.manualVetoOutcomeCount(.committed) == 0,
            "precondition: no gesture has been recorded yet"
        )

        #expect(await orchestrator.revertByTimeRange(
            start: 100,
            end: 130,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 3,
            correctionSpan: vetoSpan(assetId: "asset-1", start: 100, end: 130)
        ))
        #expect(
            await orchestrator.manualVetoOutcomeCount(.committed) == 1,
            "a committed dismiss must be recorded, not only a refused one"
        )

        // A sheet whose captured playback lifecycle has moved on. Benign, and
        // the one refusal here that is not a defect — but it must still be
        // told apart from a listener who never tapped.
        #expect(await orchestrator.revertByTimeRange(
            start: 100,
            end: 130,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 99,
            correctionSpan: vetoSpan(assetId: "asset-1", start: 100, end: 130)
        ) == false)
        #expect(
            await orchestrator.manualVetoOutcomeCount(.refusedStaleContext) == 1,
            "a stale-context refusal must name itself"
        )

        // A range the app has never made any claim about: no ad_window, no
        // decoded_span. Refused, and distinctly so.
        #expect(await orchestrator.revertByTimeRange(
            start: 900,
            end: 950,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 3,
            correctionSpan: vetoSpan(assetId: "asset-1", start: 900, end: 950)
        ) == false)
        #expect(
            await orchestrator
                .manualVetoOutcomeCount(.refusedNothingToCorrect) == 1,
            "a refusal over unclaimed audio must name itself"
        )
        #expect(
            await orchestrator.manualVetoOutcomeCount(.committed) == 1,
            "no refusal may be booked as a commit"
        )
    }
}
