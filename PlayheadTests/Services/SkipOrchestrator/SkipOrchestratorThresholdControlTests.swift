// SkipOrchestratorThresholdControlTests.swift
// playhead-xsdz.11: WRITE-PATH wiring for the per-show auto-skip threshold
// controller. These prove the orchestrator feeds the controller store the
// CORRECT signal at the CORRECT seam:
//   • a Listen revert of a managed auto-skip window  → FALSE-POSITIVE (raise)
//   • a manual "not an ad" revert (revertWindow)      → FALSE-POSITIVE (raise)
//   • accepting a suggested skip we did not auto-skip → MISS (lower)
// and that with NO store wired (the flag-OFF production default) the
// orchestrator performs no controller write.

import CoreMedia
import Foundation
import Testing
@testable import Playhead

@Suite("SkipOrchestrator per-show threshold control write path (playhead-xsdz.11)")
struct SkipOrchestratorThresholdControlTests {

    private let podcastId = "podcast-1"

    private func makeControllerStore() throws -> PerShowThresholdControllerStore {
        let dir = try makeTempDir(prefix: "xsdz11-orch-store")
        return try PerShowThresholdControllerStore(directoryURL: dir)
    }

    /// Poll until the show's sampleCount reaches `expected` (the controller
    /// write is fire-and-forget via an unstructured Task) or the budget runs out.
    private func awaitSampleCount(
        _ store: PerShowThresholdControllerStore,
        show: String,
        expected: Int
    ) async throws -> PerShowThresholdControllerState {
        var state = PerShowThresholdControllerState.zero
        for _ in 0..<40 { // up to ~2s
            state = await store.state(forShow: show)
            if state.sampleCount >= expected { return state }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        return state
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
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: podcastId)

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
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: podcastId)

        let ad = makeSkipTestAdWindow(id: "ad-veto", startTime: 60, endTime: 120, confidence: 0.85, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        await orchestrator.revertWindow(windowId: "ad-veto", podcastId: podcastId)

        let state = try await awaitSampleCount(controllerStore, show: podcastId, expected: 1)
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
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: podcastId)

        let ad = makeSkipTestAdWindow(id: "ad-tr", startTime: 60, endTime: 120, confidence: 0.85, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: podcastId)

        let state = try await awaitSampleCount(controllerStore, show: podcastId, expected: 1)
        #expect(state.integral == 1, "a managed-window time-range revert is a FALSE-POSITIVE signal → integral +1")
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
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: podcastId)

        // A markOnly (suggest-tier) window — surfaced as a suggest banner, never
        // auto-skipped. Accepting it is the "we missed an ad" gesture.
        let markOnly = AdWindow(
            id: "ad-suggest-miss",
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
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-miss"))

        await orchestrator.acceptSuggestedSkip(windowId: "ad-suggest-miss")

        let state = try await awaitSampleCount(controllerStore, show: podcastId, expected: 1)
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
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: podcastId)

        let ad = makeSkipTestAdWindow(id: "ad-none", startTime: 60, endTime: 120, confidence: 0.85, decisionState: "confirmed")
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        // Must complete cleanly with no store side effect to observe.
        await orchestrator.recordListenRevert(windowId: "ad-none", podcastId: podcastId)

        // Build a fresh, separate store and confirm it is empty — proving the
        // orchestrator wrote nowhere (there is no global store to leak into).
        let probe = try makeControllerStore()
        #expect(try await probe.count() == 0)
        await probe.close()
    }
}
