import CoreMedia
import Foundation
import Testing
@testable import Playhead

// MARK: - Test Helpers

private func makeTestAdWindow(
    id: String = UUID().uuidString,
    assetId: String = "asset-1",
    startTime: Double = 60,
    endTime: Double = 120,
    confidence: Double = 0.8
) -> AdWindow {
    AdWindow(
        id: id,
        analysisAssetId: assetId,
        startTime: startTime,
        endTime: endTime,
        confidence: confidence,
        boundaryState: "acousticRefined",
        decisionState: AdDecisionState.confirmed.rawValue,
        detectorVersion: "test",
        advertiser: nil, product: nil, adDescription: nil,
        evidenceText: nil, evidenceStartTime: nil,
        metadataSource: "test",
        metadataConfidence: nil, metadataPromptVersion: nil,
        wasSkipped: false, userDismissedBanner: false
    )
}

/// Thread-safe accumulator for pushed cue arrays. Avoids "mutation of captured
/// var in concurrently-executing code" by synchronizing via an actor.
private actor CueAccumulator {
    var cues: [[CMTimeRange]] = []
    func append(_ batch: [CMTimeRange]) { cues.append(batch) }
}

// MARK: - SkipOrchestrator.injectUserMarkedAd Tests

@Suite("SkipOrchestrator - User Marked Ad Injection")
struct UserMarkedAdInjectionTests {

    @Test("injectUserMarkedAd creates a window and pushes skip cues in auto mode")
    func injectCreatesWindowAndPushesCues() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)

        let accumulator = CueAccumulator()
        await orchestrator.setSkipCueHandler { cues in
            Task { await accumulator.append(cues) }
        }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1")

        await orchestrator.injectUserMarkedAd(
            start: 60.0,
            end: 120.0,
            analysisAssetId: "asset-1"
        )

        // Allow the fire-and-forget Task in the handler to complete.
        try await Task.sleep(for: .milliseconds(50))

        let pushedCues = await accumulator.cues
        // Should have pushed at least one set of cues.
        #expect(!pushedCues.isEmpty, "Expected skip cues to be pushed")

        // The last push should contain a cue covering approximately 60-120s.
        let lastCues = pushedCues.last!
        #expect(!lastCues.isEmpty, "Expected at least one skip cue")

        let cue = lastCues.first!
        let cueStart = CMTimeGetSeconds(cue.start)
        let cueEnd = CMTimeGetSeconds(cue.end)
        // playhead-vn7n.2: trailing cushion subtracts `adTrailingCushionSec`
        // from the pod end so the player stops just shy of program audio.
        let cushion = SkipPolicyConfig.default.adTrailingCushionSec
        #expect(cueStart == 60.0)
        #expect(cueEnd == 120.0 - cushion)
    }

    @Test("injectUserMarkedAd emits banner item")
    func injectEmitsBannerItem() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1")

        // Subscribe to banner stream before injection.
        let bannerStream = await orchestrator.bannerItemStream()
        let bannerTask = Task<AdSkipBannerItem?, Never> {
            for await item in bannerStream {
                return item
            }
            return nil
        }

        await orchestrator.injectUserMarkedAd(
            start: 60.0,
            end: 120.0,
            analysisAssetId: "asset-1"
        )

        // Give the stream a moment to deliver.
        try await Task.sleep(for: .milliseconds(100))
        bannerTask.cancel()
        let bannerItem = await bannerTask.value

        #expect(bannerItem != nil, "Expected a banner item to be emitted")
        if let item = bannerItem {
            #expect(item.adStartTime == 60.0)
            #expect(item.adEndTime == 120.0)
        }
    }

    @Test("injectUserMarkedAd broadcasts applied segments")
    func injectBroadcastsAppliedSegments() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1")

        // Subscribe to segment stream.
        let segmentStream = await orchestrator.appliedSegmentsStream()
        let segmentTask = Task<[(start: Double, end: Double)]?, Never> {
            for await segments in segmentStream {
                if !segments.isEmpty { return segments }
            }
            return nil
        }

        await orchestrator.injectUserMarkedAd(
            start: 60.0,
            end: 120.0,
            analysisAssetId: "asset-1"
        )

        try await Task.sleep(for: .milliseconds(100))
        segmentTask.cancel()
        let segments = await segmentTask.value

        #expect(segments != nil, "Expected applied segments to be broadcast")
        if let segs = segments {
            #expect(!segs.isEmpty)
            if let segment = segs.first {
                #expect(segment.start == 60.0)
                #expect(segment.end == 120.0)
            } else {
                Issue.record("Expected the injected user-marked span to be broadcast unchanged")
            }
        }
    }
}

// MARK: - AdDetectionService.recordUserMarkedAd Tests

@Suite("AdDetectionService - User Marked Ad Persistence")
struct UserMarkedAdPersistenceTests {

    @Test("recordUserMarkedAd persists AdWindow to store")
    func recordPersistsAdWindow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())

        let correctionStore = PersistentUserCorrectionStore(store: store)

        let service = AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor()
        )
        await service.setUserCorrectionStore(correctionStore)

        await service.recordUserMarkedAd(
            analysisAssetId: "asset-1",
            startTime: 30.0,
            endTime: 90.0,
            podcastId: "podcast-1"
        )

        // Verify the AdWindow was persisted.
        let windows = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(!windows.isEmpty, "Expected at least one ad window to be persisted")

        let userWindow = windows.first { $0.metadataSource == "userCorrection" }
        #expect(userWindow != nil, "Expected a userCorrection-sourced ad window")
        if let w = userWindow {
            #expect(w.startTime == 30.0)
            #expect(w.endTime == 90.0)
            #expect(w.confidence == 1.0)
            #expect(w.decisionState == AdDecisionState.confirmed.rawValue)
        }
    }

    @Test("recordUserMarkedAd records CorrectionEvent")
    func recordPersistsCorrectionEvent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())

        let correctionStore = PersistentUserCorrectionStore(store: store)

        let service = AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor()
        )
        await service.setUserCorrectionStore(correctionStore)

        await service.recordUserMarkedAd(
            analysisAssetId: "asset-1",
            startTime: 30.0,
            endTime: 90.0,
            podcastId: "podcast-1"
        )

        // Verify CorrectionEvent was recorded via the AnalysisStore directly.
        let corrections = try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
        #expect(!corrections.isEmpty, "Expected at least one correction event")

        let fnCorrection = corrections.first { $0.source == .falseNegative }
        #expect(fnCorrection != nil, "Expected a falseNegative correction event")

        // playhead-zskc: verify the scope is window-precise (exactTimeSpan
        // with the user-supplied start/end), NOT the old whole-episode
        // `exactSpan:0:Int.max` fallback.
        let fn = try #require(fnCorrection)
        let parsed = CorrectionScope.deserialize(fn.scope)
        guard case .exactTimeSpan(let assetId, let startTime, let endTime) = parsed else {
            Issue.record("Expected exactTimeSpan scope, got \(fn.scope)")
            return
        }
        #expect(assetId == "asset-1")
        #expect(startTime == 30.0, "startTime must match caller's boundary")
        #expect(endTime == 90.0, "endTime must match caller's boundary")
        // playhead-zskc code review M4: confirm correctionType mapping from
        // source.kind persists through the AdDetectionService path.
        #expect(fn.correctionType == .falseNegative,
                "falseNegative source must persist as correctionType.falseNegative")
    }
}

// MARK: - Integration: Inject + Persist Roundtrip

@Suite("User Marked Ad - Integration")
struct UserMarkedAdIntegrationTests {

    @Test("inject + persist roundtrip creates both skip cue and persisted window")
    func injectAndPersistRoundtrip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)

        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)

        let accumulator = CueAccumulator()
        await orchestrator.setSkipCueHandler { cues in
            Task { await accumulator.append(cues) }
        }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1")

        let detectionService = AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor()
        )
        await detectionService.setUserCorrectionStore(correctionStore)

        // Simulate what PlayheadRuntime.injectUserMarkedAd does.
        await orchestrator.injectUserMarkedAd(
            start: 60.0,
            end: 120.0,
            analysisAssetId: "asset-1"
        )
        await detectionService.recordUserMarkedAd(
            analysisAssetId: "asset-1",
            startTime: 60.0,
            endTime: 120.0,
            podcastId: "podcast-1"
        )

        // Allow the fire-and-forget Task in the handler to complete.
        try await Task.sleep(for: .milliseconds(50))

        // Verify skip cues were pushed.
        let pushedCues = await accumulator.cues
        #expect(!pushedCues.isEmpty, "Expected skip cues from orchestrator")

        // Verify AdWindow persisted.
        let windows = try await store.fetchAdWindows(assetId: "asset-1")
        let userWindow = windows.first { $0.metadataSource == "userCorrection" }
        #expect(userWindow != nil, "Expected persisted userCorrection AdWindow")

        // Verify correction event.
        let corrections = try await store.loadCorrectionEvents(analysisAssetId: "asset-1")
        let fnCorrection = corrections.first { $0.source == .falseNegative }
        #expect(fnCorrection != nil, "Expected persisted falseNegative correction")
        if let fn = fnCorrection {
            // playhead-zskc: persisted correction must use window-precise
            // exactTimeSpan scope rather than the old 0...Int.max fallback.
            #expect(fn.scope.hasPrefix("exactTimeSpan:"),
                    "Expected exactTimeSpan scope, got \(fn.scope)")
        }
    }
}
