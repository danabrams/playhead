// PartialActionDismissTests.swift
// playhead-zxqj — "i had a number of times where i couldnt dismiss an ad
// usually when i had already dismissed part of the window (or confirmed part)"
// (Dan, 2026-08-15).
//
// The gesture is: transcript sheet -> "Not ad" -> select rows -> Done ->
// "Dismiss ad". It ends in `SkipOrchestrator.revertByTimeRange`, which answers
// a `Bool` that the sheet swallowed. Nothing happened, and nothing said so.
//
// THE PROPERTY THIS SUITE EXISTS FOR: a target the durable transaction cannot
// accept must never refuse the gesture for the targets it can.
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
//   live cleanup skipped any entry whose revision had changed while the
//   durable transaction was in flight, so the id survived in the actionable
//   set with its durable row already `reverted`. That is a partial user action
//   making the remaining part unreachable — the thing this bead forbids.
//
// HOW THE NINE READ, because the order is not the order they were written:
//
//   1-4  The shapes that turned out to be FINE. Each was a plausible reading
//        of the report, each was checked against the 08-15 pull's own window
//        stacks, and each passed BEFORE the fix as well as after. Kept so a
//        later reader can see which readings were already excluded rather
//        than re-deriving them.
//   5    The one that FAILED, all three assertions: the suggest-tier stale
//        entry, reached by a producer update landing during the store hop.
//   6-7  The instrument, one test per direction. A recorder wired only to
//        failures cannot tell "refused" from "not instrumented yet".
//   8    The invariant ISOLATED, and the reason it exists is a mutation
//        result rather than an argument: the rail for the target-set fix
//        SURVIVED against test 5, because the cleanup fix already removes
//        that entry. Test 8 uses the state no cleanup can prevent — a live
//        window the STORE HAS NO ROW FOR.
//   9    The MANAGED twin of 5, and a worse symptom: that tier owns a live
//        CUE, so a stale entry there goes on skipping audio the listener has
//        just said is not an ad.
//
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

    // 6 and 7: the instrument. A rule that never fires is indistinguishable
    // from a codebase with no violations, so prove it fires by making it fire.
    // Split in two, one test per DIRECTION, because a recorder wired only to
    // the failure path cannot tell "refused" from "not instrumented yet" and a
    // single test covering both cannot say which half regressed.

    /// Shared fixture: an episode with one mark-only window at [100, 160].
    private func makeInstrumentedSession() async throws
        -> (AnalysisStore, SkipOrchestrator) {
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
        return (store, orchestrator)
    }

    @Test("a COMMITTED dismiss is recorded", .timeLimit(.minutes(1)))
    func aCommittedDismissIsRecorded() async throws {
        let (_, orchestrator) = try await makeInstrumentedSession()
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
    }

    @Test("a REFUSED dismiss is recorded, and names which refusal", .timeLimit(.minutes(1)))
    func aRefusedDismissIsRecorded() async throws {
        let (_, orchestrator) = try await makeInstrumentedSession()

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
            await orchestrator.manualVetoOutcomeCount(.committed) == 0,
            "no refusal may be booked as a commit"
        )
    }

    // 8 — THE INVARIANT, isolated. Tests 5 and 9 are races the cleanup can
    // prevent; this is the state no cleanup can. The live session and the
    // store are written separately and re-analysis REPLACES an asset's window
    // rows (new ids), so a live entry the store has no row for is an ordinary
    // consequence of the two lifetimes rather than an exotic one — and
    // `persistRevertedAdWindowsIfCurrent` requires `fetchAdWindow` to return a
    // row for EVERY expected target. Before this bead that one live-only
    // window rolled the whole transaction back, so the persisted window beside
    // it — which the gesture could always have reverted — was left alone.
    @Test(
        "a live window with no durable row cannot refuse the dismiss beside it",
        .timeLimit(.minutes(1))
    )
    func liveOnlyWindowCannotRefuseTheDismissBesideIt() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        // Persisted, revertible, and squarely inside the range.
        try await store.insertAdWindow(
            markOnlyWindow(id: "w-persisted", start: 150, end: 200)
        )
        let orchestrator = await makeOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 3
        )
        // Delivered to the live session and NEVER written to the store.
        // `.applied` so the tier routing keeps it in the MANAGED dictionary
        // rather than carding it as a suggestion — the tier is incidental
        // here; what this test is about is the missing row.
        await orchestrator.receiveAdWindows([
            makeSkipTestAdWindow(
                id: "w-live-only",
                startTime: 100,
                endTime: 160,
                confidence: 0.95,
                decisionState: AdDecisionState.applied.rawValue
            )
        ])
        #expect(
            await orchestrator.activeWindowIDs().contains("w-live-only"),
            """
            precondition: the session must be holding the row-less window, \
            otherwise this test proves nothing about the target set
            """
        )

        let vetoed = await orchestrator.revertByTimeRange(
            start: 155,
            end: 190,
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            ifCurrentEpisodeId: "ep-1",
            ifPlaybackLifecycleGeneration: 3,
            correctionSpan: vetoSpan(assetId: "asset-1", start: 155, end: 190)
        )
        #expect(
            vetoed,
            "a live window the store has no row for must not refuse the gesture"
        )
        let after = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(
            after.first { $0.id == "w-persisted" }?.decisionState
                == AdDecisionState.reverted.rawValue,
            "the persisted window the gesture covers must be reverted"
        )
    }

    // 9: the MANAGED twin of test 5. The auto-skip tier keeps its own
    // dictionary and its own cue; a stale entry there does not merely refuse a
    // later dismiss, it goes on SKIPPING audio the listener said was not an ad.
    @Test(
        "a committed dismiss leaves no live window still claiming the ad",
        .timeLimit(.minutes(1))
    )
    func committedDismissLeavesNoLiveClaim() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "ep-1")
        )
        let ad = makeSkipTestAdWindow(
            id: "w-managed",
            startTime: 100,
            endTime: 160,
            confidence: 0.95,
            decisionState: AdDecisionState.applied.rawValue
        )
        try await store.insertAdWindow(ad)
        let orchestrator = await makeOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "ep-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 3
        )
        await orchestrator.receiveAdWindows([ad])

        let gate = ZxqjGate()
        await orchestrator._setRevertPersistenceBarrierForTesting {
            await gate.wait()
        }
        let gesture = Task {
            await orchestrator.revertByTimeRange(
                start: 110,
                end: 130,
                analysisAssetId: "asset-1",
                podcastId: "podcast-1",
                ifCurrentEpisodeId: "ep-1",
                ifPlaybackLifecycleGeneration: 3,
                correctionSpan: vetoSpan(
                    assetId: "asset-1", start: 110, end: 130
                )
            )
        }
        await gate.waitUntilStarted()
        // A refinement of the same id lands while the durable revert is parked.
        await orchestrator.receiveAdWindows([
            makeSkipTestAdWindow(
                id: "w-managed",
                startTime: 100,
                endTime: 160,
                confidence: 0.97,
                decisionState: AdDecisionState.applied.rawValue
            )
        ])
        await orchestrator._setRevertPersistenceBarrierForTesting(nil)
        await gate.release()
        #expect(await gesture.value, "precondition: the dismiss commits")

        #expect(
            await orchestrator
                ._managedDecisionStateForTesting(id: "w-managed") == .reverted,
            """
            the live window must be reverted too: a live cue over a span whose \
            durable row this same gesture just reverted goes on skipping audio \
            the listener said was not an ad
            """
        )
    }
}
