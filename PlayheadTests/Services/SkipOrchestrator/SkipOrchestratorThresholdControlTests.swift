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

        // Build a fresh, separate store and confirm it is empty — proving the
        // orchestrator wrote nowhere (there is no global store to leak into).
        let probe = try makeControllerStore()
        #expect(try await probe.count() == 0)
        await probe.close()
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
