// SkipOrchestratorThresholdControlTests.swift
// playhead-xsdz.11: WRITE-PATH wiring for the per-show auto-skip threshold
// controller. These prove the orchestrator feeds the controller store the
// CORRECT signal at the CORRECT seam:
//   • a Listen revert of a managed auto-skip window  → FALSE-POSITIVE (raise)
//   • a manual "not an ad" revert (revertWindow)      → FALSE-POSITIVE (raise)
//   • denying an auto-skipped banner ("No")           → FALSE-POSITIVE (raise)
//   • accepting a suggested skip we did not auto-skip → MISS (lower)
// and that with NO store wired (the flag-OFF production default) those seams
// still run to completion. The complementary "records NOTHING" contracts —
// which need a wired store to be observable at all — are pinned in the
// negative-coverage section further down.
//
// playhead-i08e: three of those seams (revertWindow, denyAutoSkippedBanner,
// acceptSuggestedSkip) went dead when the explicit-feedback refactor made
// minting the correction receipt throw unless a UserCorrectionStore happened
// to be wired — the throw aborted the seam before its controller write. The
// coverage below now pins each seam in BOTH the minimally-wired configuration
// (no correction store) and the production configuration (correction store
// wired), and asserts an exact sample count so a double write that cancels to
// integral 0 cannot pass either.

import CoreMedia
import Foundation
import Testing
@testable import Playhead

/// playhead-i08e: raised by `awaitSampleCount` when no controller sample lands
/// inside the polling budget.
///
/// The helper previously returned `PerShowThresholdControllerState.zero` on
/// timeout, which made a DEAD write path (no sample recorded at all) look
/// exactly like a live write path that computed `integral == 0`. That
/// ambiguity is what let this regression read as "the controller math is
/// wrong". Failing loudly keeps the two diagnoses apart.
private struct ControllerSampleTimeout: Error, CustomStringConvertible {
    let show: String
    let expected: Int
    let observed: Int

    var description: String {
        """
        No controller sample was recorded for show "\(show)" within the ~10s \
        budget: expected sampleCount >= \(expected), observed \(observed). \
        The seam under test never reached recordThresholdControlSignal — this \
        is a DEAD WRITE PATH, not a miscomputed controller value.
        """
    }
}

/// Bounded read of the first `.present` banner item. `denyAutoSkippedBanner`
/// validates the action against the exact material the card displayed, so the
/// test has to answer with the orchestrator's own emission rather than a
/// locally reconstructed token. Returns nil instead of parking the suite
/// forever when no presentation arrives.
private func firstPresentedBannerItem(
    _ stream: AsyncStream<AdBannerStreamEvent>
) async -> AdSkipBannerItem? {
    await withTaskGroup(of: AdSkipBannerItem?.self) { group in
        group.addTask {
            for await event in stream {
                if case let .present(item) = event { return item }
            }
            return nil
        }
        group.addTask {
            try? await Task.sleep(for: .seconds(5))
            return nil
        }
        let first = await group.next() ?? nil
        group.cancelAll()
        return first
    }
}

@Suite("SkipOrchestrator per-show threshold control write path (playhead-xsdz.11)")
struct SkipOrchestratorThresholdControlTests {

    private let podcastId = "podcast-1"
    /// The episode the `makeSkipTestAnalysisAsset()` fixture belongs to.
    /// `beginEpisode` must be told the SAME episode id the asset row carries:
    /// the durable explicit-feedback transactions
    /// (`persistDeniedAutoSkip`, `persistAcceptedSuggestionIfCurrent`) refuse
    /// to write when the acting card's episode does not own the asset, which
    /// is exactly what production guarantees by passing the real episode id.
    private let episodeId = "ep-1"

    private func makeControllerStore() throws -> PerShowThresholdControllerStore {
        let dir = try makeTempDir(prefix: "xsdz11-orch-store")
        return try PerShowThresholdControllerStore(directoryURL: dir)
    }

    /// Show id used ONLY as a write barrier. Never asserted on directly; its
    /// row is subtracted out by `controllerRowsExcludingBarrier`.
    private static let barrierShow = "i08e-controller-write-barrier"

    /// Poll until the show's sampleCount reaches `expected`. The controller
    /// write is fire-and-forget via an unstructured Task, so a gesture can
    /// return before its sample lands.
    ///
    /// playhead-i08e: throws rather than returning `.zero` when the budget
    /// expires, so "nothing was ever recorded" can never masquerade as a
    /// computed value. The budget is generous on purpose — it is only ever
    /// spent on the failing path (a satisfied poll exits on its first read), so
    /// a saturated machine cannot turn a live write path into a red test.
    private func pollSampleCount(
        _ store: PerShowThresholdControllerStore,
        show: String,
        expected: Int
    ) async throws -> PerShowThresholdControllerState {
        var state = PerShowThresholdControllerState.zero
        for _ in 0..<200 { // up to ~10s, only consumed when the write is dead
            state = await store.state(forShow: show)
            if state.sampleCount >= expected { return state }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        throw ControllerSampleTimeout(
            show: show,
            expected: expected,
            observed: state.sampleCount
        )
    }

    /// playhead-i08e (fourth pass): flush any controller write the gesture
    /// under test may have issued, WITHOUT a wall-clock guess.
    ///
    /// Issues one write of our own through the same production seam and waits
    /// for it to become visible. A write the gesture issued is an unstructured
    /// `Task` created on the orchestrator's executor (`Task {}` in
    /// `recordThresholdControlSignal` inherits actor context, and nothing in
    /// the path is `Task.detached`), strictly before this call's; each then
    /// hops to the same `PerShowThresholdControllerStore` actor. Same-priority
    /// actor jobs run in enqueue order, so observing the barrier's row means
    /// the earlier write has landed.
    ///
    /// Two honest limits. That ordering is an implementation property of the
    /// default actor executor, not a language guarantee. And it orders only
    /// writes whose `Task` was created synchronously inside the gesture — a
    /// regression that wrote from a task spawned by a LATER hop (say derived
    /// learning calling back into the orchestrator) would still be enqueued
    /// after the barrier.
    ///
    /// It replaces a fixed 500 ms settle, which had both limits plus a worse
    /// one: a fixed window is the wrong shape for a negative assertion. It
    /// never makes a correct implementation fail, but on a loaded machine a
    /// REGRESSION's write can land just after it expires and the assertion
    /// silently passes. The barrier scales with load instead of racing it.
    private func drainControllerWrites(
        _ store: PerShowThresholdControllerStore,
        _ orchestrator: SkipOrchestrator
    ) async throws {
        let before = await store.state(forShow: Self.barrierShow).sampleCount
        await orchestrator.recordThresholdControlMiss(
            podcastId: Self.barrierShow
        )
        _ = try await pollSampleCount(
            store,
            show: Self.barrierShow,
            expected: before + 1
        )
    }

    /// Wait for the seam's sample, then re-read behind a barrier so callers'
    /// `sampleCount == expected` assertions bound the count from ABOVE as well
    /// as below — a second, opposing write issued by the same gesture would
    /// otherwise land just after the poll returned and cancel the integral
    /// unobserved.
    private func awaitSampleCount(
        _ store: PerShowThresholdControllerStore,
        orchestrator: SkipOrchestrator,
        show: String,
        expected: Int
    ) async throws -> PerShowThresholdControllerState {
        _ = try await pollSampleCount(store, show: show, expected: expected)
        try await drainControllerWrites(store, orchestrator)
        return await store.state(forShow: show)
    }

    /// Row count for the "this seam must record NOTHING" assertions, read
    /// behind the barrier above and with the barrier's own row subtracted.
    ///
    /// Counting ROWS (not one show's samples) also catches a write misrouted to
    /// some other show id.
    private func controllerRowsExcludingBarrier(
        _ store: PerShowThresholdControllerStore,
        _ orchestrator: SkipOrchestrator
    ) async throws -> Int {
        try await drainControllerWrites(store, orchestrator)
        return try await store.count() - 1
    }

    @Test("Listen revert of a managed auto-skip window records a FALSE-POSITIVE signal (integral +1)")
    func listenRevertRecordsFalsePositive() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeControllerStore()
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let ad = makeSkipTestAdWindow(id: "ad-fp", startTime: 60, endTime: 120, confidence: 0.85, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        await orchestrator.recordListenRevert(windowId: "ad-fp", podcastId: podcastId)

        let state = try await awaitSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: podcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1, "one revert must record exactly one controller sample")
        #expect(state.integral == 1, "a Listen revert is a FALSE-POSITIVE signal → integral +1")
        await controllerStore.close()
    }

    @Test("Manual 'not an ad' revertWindow records a FALSE-POSITIVE signal")
    func revertWindowRecordsFalsePositive() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeControllerStore()
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let ad = makeSkipTestAdWindow(id: "ad-veto", startTime: 60, endTime: 120, confidence: 0.85, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        #expect(await orchestrator.revertWindow(windowId: "ad-veto", podcastId: podcastId))

        let state = try await awaitSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: podcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1, "one veto must record exactly one controller sample")
        #expect(state.integral == 1, "a manual veto of a managed window is a FALSE-POSITIVE signal → integral +1")

        // playhead-i08e root-cause rail: the seam went dead because minting the
        // receipt threw when no UserCorrectionStore was wired. Assert the
        // RECEIPT itself, not just its downstream controller sample — otherwise
        // simply hoisting the controller write above the persistence block
        // would satisfy this suite while the user's correction stayed dropped.
        // The receipt's durability is owned by the AnalysisStore transaction,
        // so it must land regardless of the optional learning listener.
        let receipts = try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
        #expect(receipts.count == 1, "the veto must commit exactly one durable receipt")
        #expect(receipts.first?.source == .manualVeto)
        await controllerStore.close()
    }

    /// playhead-i08e: the same seam in the PRODUCTION wiring. A correction
    /// store is always injected by `PlayheadRuntime`, so this is the shape the
    /// app actually runs.
    ///
    /// What this adds over its unwired sibling is ONLY the wired listener:
    /// whether `schedulePostCommitCorrectionLearning` — which no-ops without a
    /// store — induces a second controller write once it is live. (The sibling
    /// is the regression rail: restoring the `correctionStore != nil`
    /// precondition reddens it and leaves this one green.) Deleting the seam's
    /// controller write kills both, so this is not independent coverage of the
    /// write itself.
    @Test("Manual 'not an ad' revertWindow records exactly one FALSE-POSITIVE with the correction store wired")
    func revertWindowWithCorrectionStoreRecordsOneFalsePositive() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeControllerStore()
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let ad = makeSkipTestAdWindow(id: "ad-veto-wired", startTime: 60, endTime: 120, confidence: 0.85, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        #expect(await orchestrator.revertWindow(windowId: "ad-veto-wired", podcastId: podcastId))

        let state = try await awaitSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: podcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1, "one veto must record exactly one controller sample")
        #expect(state.integral == 1, "a manual veto of a managed window is a FALSE-POSITIVE signal → integral +1")
        await controllerStore.close()
    }

    @Test("revertByTimeRange of a managed auto-skip window records a FALSE-POSITIVE signal")
    func revertByTimeRangeManagedRecordsFalsePositive() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeControllerStore()
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let ad = makeSkipTestAdWindow(id: "ad-tr", startTime: 60, endTime: 120, confidence: 0.85, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: podcastId)

        let state = try await awaitSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: podcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1, "one time-range revert must record exactly one controller sample")
        #expect(state.integral == 1, "a managed-window time-range revert is a FALSE-POSITIVE signal → integral +1")
        await controllerStore.close()
    }

    /// playhead-i08e: the explicit banner "No" on an applied auto-skip is the
    /// third seam that routes through the atomic correction commit, so it
    /// carries the same dead-write-path risk as `revertWindow`.
    @Test("Denying an auto-skipped banner records a FALSE-POSITIVE signal")
    func denyAutoSkippedBannerRecordsFalsePositive() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeControllerStore()
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: episodeId,
            podcastId: podcastId,
            playbackLifecycleGeneration: 7
        )
        let events = await orchestrator.bannerEventStream()

        let ad = makeSkipTestAdWindow(id: "ad-deny", startTime: 60, endTime: 120, confidence: 0.9, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        let item = try #require(await firstPresentedBannerItem(events))
        #expect(item.windowId == "ad-deny")
        #expect(
            await orchestrator.denyAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: item.analysisAssetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                podcastId: item.podcastId,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration: item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken: item.windowMaterialRevisionToken
            ),
            "the exact displayed material must be accepted"
        )

        let state = try await awaitSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: podcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1, "one denial must record exactly one controller sample")
        #expect(state.integral == 1, "an explicit banner No is a FALSE-POSITIVE signal → integral +1")
        await controllerStore.close()
    }

    @Test("Accepting a suggested (not-auto-skipped) ad records a MISS signal (integral −1)")
    func acceptSuggestedSkipRecordsMiss() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeControllerStore()
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let markOnly = makeMarkOnlySuggestWindow(id: "ad-suggest-miss")
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([markOnly])
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-miss"))

        #expect(await orchestrator.acceptSuggestedSkip(windowId: "ad-suggest-miss"))

        let state = try await awaitSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: podcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1, "one acceptance must record exactly one controller sample")
        #expect(state.integral == -1, "accepting a suggested (missed) ad is a MISS signal → integral −1")
        await controllerStore.close()
    }

    /// playhead-i08e: the same seam in the PRODUCTION wiring — see
    /// `revertWindowWithCorrectionStoreRecordsOneFalsePositive` for exactly
    /// what the wired variant does and does not add.
    @Test("Accepting a suggested ad records exactly one MISS with the correction store wired")
    func acceptSuggestedSkipWithCorrectionStoreRecordsOneMiss() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeControllerStore()
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let markOnly = makeMarkOnlySuggestWindow(id: "ad-suggest-miss-wired")
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([markOnly])
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-miss-wired"))

        #expect(await orchestrator.acceptSuggestedSkip(windowId: "ad-suggest-miss-wired"))

        let state = try await awaitSampleCount(
            controllerStore,
            orchestrator: orchestrator,
            show: podcastId,
            expected: 1
        )
        #expect(state.sampleCount == 1, "one acceptance must record exactly one controller sample")
        #expect(state.integral == -1, "accepting a suggested (missed) ad is a MISS signal → integral −1")
        await controllerStore.close()
    }

    /// playhead-i08e (fourth pass): renamed from `noStoreNoWrite` / "performs
    /// no controller write". With no store wired there is no surface on which a
    /// write could be observed, so that title named a contract this test cannot
    /// falsify — the same defect class the bead is cleaning up. What it really
    /// pins, and what the pre-fix code failed, is that both seams RUN TO
    /// COMPLETION in the flag-OFF default instead of trapping on the absent
    /// dependency. The observable half lives in
    /// `anonymousRevertRecordsNoControllerSample`.
    @Test("No controller store wired ⇒ the revert seams still run to completion (flag-OFF default)")
    func noControllerStoreLeavesRevertSeamsIntact() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        // Deliberately do NOT wire a controller store — this is the production
        // flag-OFF default. The revert must still work and must not crash.
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let listened = makeSkipTestAdWindow(id: "ad-none", startTime: 60, endTime: 120, confidence: 0.85, decisionState: "confirmed")
        let vetoed = makeSkipTestAdWindow(id: "ad-none-veto", startTime: 300, endTime: 360, confidence: 0.85, decisionState: "confirmed")
        try await store.insertAdWindow(listened)
        try await store.insertAdWindow(vetoed)
        await orchestrator.receiveAdWindows([listened, vetoed])

        await orchestrator.recordListenRevert(windowId: "ad-none", podcastId: podcastId)
        // playhead-i08e: cover a seam this bead made live again, not only the
        // one that never broke — `revertWindow` is the path whose first
        // statement used to throw when no correction store was wired.
        #expect(await orchestrator.revertWindow(windowId: "ad-none-veto", podcastId: podcastId))

        // Durable proof of completion, not merely "did not crash": an aborted
        // gesture is also silent. `revertWindow`'s receipt is committed by the
        // AnalysisStore transaction, so it lands even with no correction store.
        let receipts = try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
        #expect(
            receipts.count == 1,
            "the veto must still commit its durable receipt with the flag off"
        )
        #expect(receipts.first?.source == .manualVeto)
        let row = try #require(try await store.fetchAdWindow(id: "ad-none-veto"))
        #expect(row.decisionState == AdDecisionState.reverted.rawValue)
        // `recordListenRevert` needs its own durable evidence. Its receipt goes
        // through `persistManualCorrectionVeto`, which no-ops without a
        // correction store, so the row flip is the only observable it leaves —
        // and without asserting it, making `return` the seam's first statement
        // would keep this test green even though the title names both seams.
        let listenedRow = try #require(try await store.fetchAdWindow(id: "ad-none"))
        #expect(
            listenedRow.decisionState == AdDecisionState.reverted.rawValue,
            "the Listen revert must also have run to completion"
        )

        // playhead-i08e (third pass): a closing probe that built a BRAND-NEW
        // controller store in a fresh temp directory and asserted `count() == 0`
        // was deleted here — empty by construction, unfalsifiable.
    }

    // MARK: - playhead-i08e (third pass): seams that must record NOTHING
    //
    // The suite above proves each calibration seam records exactly ONE sample
    // with the right sign. The other half of that contract — the seams and
    // conditions that must record NO sample — had no coverage at all, and it
    // is the half most exposed by this bead: reviving four dead explicit-
    // feedback paths makes it newly possible for one of them to start writing
    // where it must not. Each test below pairs its negative assertion with a
    // POSITIVE CONTROL (the gesture returned true / its durable receipt or row
    // flip landed) so "recorded nothing" can never be satisfied by a seam that
    // simply aborted — which is exactly how the original regression hid.

    /// The controller is per-show, so a correction carrying no show id has
    /// nowhere to land (`recordThresholdControlSignal`'s second guard). This
    /// replaces the deleted fresh-store probe in
    /// `noControllerStoreLeavesRevertSeamsIntact`: the store IS
    /// wired here, so a regression that fell back to `activePodcastId` — which
    /// `beginEpisode` set to a real value below — turns this red.
    @Test("An anonymous revert (no podcastId) records no controller sample")
    func anonymousRevertRecordsNoControllerSample() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let controllerStore = try makeControllerStore()
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let ad = makeSkipTestAdWindow(id: "ad-anon", startTime: 60, endTime: 120, confidence: 0.85, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        #expect(await orchestrator.revertWindow(windowId: "ad-anon", podcastId: nil))
        let receipts = try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
        #expect(receipts.count == 1, "the veto itself still commits its durable receipt")
        #expect(receipts.first?.source == .manualVeto)

        #expect(
            try await controllerRowsExcludingBarrier(controllerStore, orchestrator) == 0,
            "an unattributed correction must not be folded into any show's controller state"
        )
        await controllerStore.close()
    }

    /// `confirmAutoSkippedBanner` is capture-only: the user agreeing that a
    /// skip was right is a TRUE positive, and the controller models only
    /// false-positive (raise) and miss (lower). Writing here would push the
    /// threshold on agreement.
    @Test("Confirming an auto-skipped banner records no controller sample")
    func confirmAutoSkippedBannerRecordsNoControllerSample() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeControllerStore()
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: episodeId,
            podcastId: podcastId,
            playbackLifecycleGeneration: 7
        )
        let events = await orchestrator.bannerEventStream()

        let ad = makeSkipTestAdWindow(id: "ad-confirm", startTime: 60, endTime: 120, confidence: 0.9, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        let item = try #require(await firstPresentedBannerItem(events))
        #expect(item.windowId == "ad-confirm")
        #expect(
            await orchestrator.confirmAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: item.analysisAssetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration: item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken: item.windowMaterialRevisionToken
            ),
            "the exact displayed material must be accepted"
        )
        let receipts = try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
        #expect(receipts.count == 1, "the Yes ran to completion and committed its receipt")
        #expect(receipts.first?.source == .bannerAutoSkipConfirmed)

        #expect(
            try await controllerRowsExcludingBarrier(controllerStore, orchestrator) == 0,
            "agreeing with a skip is a true positive — it must not move the threshold"
        )
        await controllerStore.close()
    }

    /// `declineSuggestedSkip` is capture-only for the mirror reason: the
    /// algorithm only OFFERED a banner, it never altered playback, so a No is
    /// too weak to raise the auto-skip threshold. This seam was one of the four
    /// revived by playhead-i08e — before the fix it aborted at its first
    /// statement, so this assertion would have been vacuous.
    @Test("An explicit suggest-tier No records no controller sample")
    func declineSuggestedSkipRecordsNoControllerSample() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let controllerStore = try makeControllerStore()
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        let markOnly = makeMarkOnlySuggestWindow(id: "ad-suggest-no")
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([markOnly])
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-no"))

        #expect(
            await orchestrator.declineSuggestedSkip(
                windowId: "ad-suggest-no",
                isExplicitDenial: true
            ),
            "the explicit No must run to completion"
        )
        let receipts = try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
        #expect(receipts.count == 1, "the No ran to completion and committed its receipt")
        #expect(receipts.first?.source == .bannerSuggestionDenied)

        #expect(
            try await controllerRowsExcludingBarrier(controllerStore, orchestrator) == 0,
            "a suggest-tier veto never altered playback — too weak to raise the threshold"
        )
        await controllerStore.close()
    }

    /// The `revertedManagedAny` routing that playhead-i08e relocated above the
    /// trust hop. `revertByTimeRangeManagedRecordsFalsePositive` covers the
    /// true arm; dropping the condition entirely would leave that test green,
    /// so the false arm needs its own rail.
    @Test("A suggest-tier-only revertByTimeRange records no controller sample")
    func revertByTimeRangeSuggestOnlyRecordsNoControllerSample() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let controllerStore = try makeControllerStore()
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setPerShowThresholdControllerStore(controllerStore)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: episodeId, podcastId: podcastId)

        // markOnly ONLY — the gesture must find no managed auto-skip window,
        // so `revertedManagedAny` stays false.
        let markOnly = makeMarkOnlySuggestWindow(id: "ad-suggest-range")
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([markOnly])
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-range"))

        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: podcastId)

        #expect(
            !(await orchestrator.activeSuggestWindowIDs()).contains("ad-suggest-range"),
            "the suggest tier really was vetoed — the gesture is not a no-op"
        )
        let row = try #require(try await store.fetchAdWindow(id: "ad-suggest-range"))
        #expect(row.decisionState == AdDecisionState.reverted.rawValue)

        #expect(
            try await controllerRowsExcludingBarrier(controllerStore, orchestrator) == 0,
            "a suggest-only revert never altered playback — too weak to raise the threshold"
        )
        await controllerStore.close()
    }

    /// A markOnly (suggest-tier) window — surfaced as a suggest banner, never
    /// auto-skipped. Accepting it is the "we missed an ad" gesture.
    private func makeMarkOnlySuggestWindow(id: String) -> AdWindow {
        AdWindow(
            id: id,
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
    }
}
