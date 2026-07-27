// SkipOrchestratorThresholdControlTests.swift
// playhead-xsdz.11: WRITE-PATH wiring for the per-show auto-skip threshold
// controller. These prove the orchestrator feeds the controller store the
// CORRECT signal at the CORRECT seam:
//   • a Listen revert of a managed auto-skip window  → FALSE-POSITIVE (raise)
//   • a manual "not an ad" revert (revertWindow)      → FALSE-POSITIVE (raise)
//   • denying an auto-skipped banner ("No")           → FALSE-POSITIVE (raise)
//   • accepting a suggested skip we did not auto-skip → MISS (lower)
// and that with NO store wired (the flag-OFF production default) the
// orchestrator performs no controller write.
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
        No controller sample was recorded for show "\(show)" within the ~2s \
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

    /// Poll until the show's sampleCount reaches `expected` (the controller
    /// write is fire-and-forget via an unstructured Task), then re-read after a
    /// short settle so callers' `sampleCount == expected` assertions bound the
    /// count from ABOVE as well as below — a second, opposing write issued by
    /// the same gesture would otherwise land just after the poll returned and
    /// cancel the integral unobserved. The settle is a wall-clock window, so it
    /// catches a duplicate issued by the seam itself (the realistic
    /// regression), not one deferred behind an arbitrarily long hop.
    ///
    /// playhead-i08e: throws rather than returning `.zero` when the budget
    /// expires, so "nothing was ever recorded" can never masquerade as a
    /// computed value.
    private func awaitSampleCount(
        _ store: PerShowThresholdControllerStore,
        show: String,
        expected: Int
    ) async throws -> PerShowThresholdControllerState {
        var state = PerShowThresholdControllerState.zero
        for _ in 0..<40 { // up to ~2s
            state = await store.state(forShow: show)
            if state.sampleCount >= expected { break }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        guard state.sampleCount >= expected else {
            throw ControllerSampleTimeout(
                show: show,
                expected: expected,
                observed: state.sampleCount
            )
        }
        try await Task.sleep(nanoseconds: 200_000_000)
        return await store.state(forShow: show)
    }

    /// playhead-i08e (third pass): row count for the "this seam must record
    /// NOTHING" assertions, read only after a settle window.
    ///
    /// `recordThresholdControlSignal` writes through an unstructured `Task`, so
    /// reading the store the instant a gesture returns reports 0 for a seam
    /// that is about to write. Without the settle, every negative assertion
    /// below would pass against an implementation that DOES write — i.e. it
    /// would be vacuous, which is the specific defect this bead is cleaning up.
    /// Counting ROWS (not one show's samples) also catches a write misrouted to
    /// some other show id.
    private func settledControllerRowCount(
        _ store: PerShowThresholdControllerStore
    ) async throws -> Int {
        try await Task.sleep(nanoseconds: 500_000_000)
        return try await store.count()
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

        let state = try await awaitSampleCount(controllerStore, show: podcastId, expected: 1)
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

        let state = try await awaitSampleCount(controllerStore, show: podcastId, expected: 1)
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
    /// app actually runs; the exact `sampleCount` also rejects a second
    /// controller write issued anywhere in the same gesture.
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

        let state = try await awaitSampleCount(controllerStore, show: podcastId, expected: 1)
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

        let state = try await awaitSampleCount(controllerStore, show: podcastId, expected: 1)
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

        let state = try await awaitSampleCount(controllerStore, show: podcastId, expected: 1)
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

        let state = try await awaitSampleCount(controllerStore, show: podcastId, expected: 1)
        #expect(state.sampleCount == 1, "one acceptance must record exactly one controller sample")
        #expect(state.integral == -1, "accepting a suggested (missed) ad is a MISS signal → integral −1")
        await controllerStore.close()
    }

    /// playhead-i08e: the same seam in the PRODUCTION wiring (see
    /// `revertWindowWithCorrectionStoreRecordsOneFalsePositive`).
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

        let state = try await awaitSampleCount(controllerStore, show: podcastId, expected: 1)
        #expect(state.sampleCount == 1, "one acceptance must record exactly one controller sample")
        #expect(state.integral == -1, "accepting a suggested (missed) ad is a MISS signal → integral −1")
        await controllerStore.close()
    }

    @Test("No controller store wired ⇒ a revert performs no controller write (flag-OFF default)")
    func noStoreNoWrite() async throws {
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

        // Must complete cleanly with no store side effect to observe.
        await orchestrator.recordListenRevert(windowId: "ad-none", podcastId: podcastId)
        // playhead-i08e: cover a seam this bead made live again, not only the
        // one that never broke — `revertWindow` must stay a no-op on the
        // controller when the flag is off.
        #expect(await orchestrator.revertWindow(windowId: "ad-none-veto", podcastId: podcastId))

        // The seams must have run to completion, not merely "not crashed" — an
        // aborted gesture also writes no controller sample, so without this the
        // assertion below would hold just as well for the dead write path this
        // bead fixed. `revertWindow`'s receipt is committed by the AnalysisStore
        // transaction and so lands even with no correction store wired.
        let receipts = try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
        #expect(
            receipts.count == 1,
            "the veto must still commit its durable receipt with the flag off"
        )
        #expect(receipts.first?.source == .manualVeto)
        let row = try #require(try await store.fetchAdWindow(id: "ad-none-veto"))
        #expect(row.decisionState == AdDecisionState.reverted.rawValue)

        // playhead-i08e (third pass): this test used to close by building a
        // BRAND-NEW `PerShowThresholdControllerStore` in a fresh unique temp
        // directory and asserting `count() == 0`, commented as "proving the
        // orchestrator wrote nowhere". A store that was created after the
        // gesture, in a directory nothing has ever written to, is empty by
        // construction — the assertion could not fail for any implementation
        // and proved nothing. It is deleted rather than rewritten: with no
        // controller store wired there is, by definition, no surface on which
        // a write could be observed, so the honest content of this test is the
        // durable evidence above that both seams ran to completion instead of
        // trapping or aborting. The observable half of the no-write contract —
        // a store IS wired but the correction has no show to attribute to — is
        // covered by `anonymousRevertRecordsNoControllerSample`.
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
    /// replaces the deleted fresh-store probe in `noStoreNoWrite`: the store IS
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
            try await settledControllerRowCount(controllerStore) == 0,
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
            try await settledControllerRowCount(controllerStore) == 0,
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
            try await settledControllerRowCount(controllerStore) == 0,
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
            try await settledControllerRowCount(controllerStore) == 0,
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
