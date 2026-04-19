// SkipOrchestratorPreloadTests.swift

import CoreMedia
import XCTest
@testable import Playhead

final class SkipOrchestratorPreloadTests: XCTestCase {

    private var store: AnalysisStore!
    private var orchestrator: SkipOrchestrator!

    override func setUp() async throws {
        try await super.setUp()
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("SkipOrchestratorPreloadTests-\(UUID().uuidString)")
        store = try await AnalysisStore.open(directory: dir)

        // Insert a dummy analysis asset so store lookups work.
        try await store.insertAsset(AnalysisAsset(
            id: "asset-1",
            episodeId: "ep-1",
            assetFingerprint: "fp",
            weakFingerprint: nil,
            sourceURL: "file:///test.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "complete",
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        orchestrator = SkipOrchestrator(store: store)
    }

    // MARK: - Tests

    func testBeginEpisodeLoadsCues() async throws {
        // Insert pre-materialized skip cues into the store.
        let cues = [
            SkipCue(
                id: "cue-1",
                analysisAssetId: "asset-1",
                cueHash: "hash-1",
                startTime: 10.0,
                endTime: 40.0,
                confidence: 0.85,
                source: "preAnalysis",
                materializedAt: Date().timeIntervalSince1970,
                wasSkipped: false,
                userDismissed: false
            ),
            SkipCue(
                id: "cue-2",
                analysisAssetId: "asset-1",
                cueHash: "hash-2",
                startTime: 60.0,
                endTime: 90.0,
                confidence: 0.9,
                source: "preAnalysis",
                materializedAt: Date().timeIntervalSince1970,
                wasSkipped: false,
                userDismissed: false
            ),
        ]
        try await store.insertSkipCues(cues)

        // Track skip cues pushed via the handler.
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues = ranges
        }

        // beginEpisode should load the cues and push them through the pipeline.
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        // The orchestrator should have processed the pre-loaded cues.
        // In default shadow mode, windows are confirmed (not applied), so the
        // decision log should have entries for the preloaded cues.
        let log = await orchestrator.getDecisionLog()
        XCTAssertFalse(log.isEmpty, "Decision log should contain entries from preloaded cues")

        // Confirmed windows should be available.
        let confirmed = await orchestrator.confirmedWindows()
        XCTAssertEqual(confirmed.count, 2, "Both preloaded cues should appear as confirmed windows")
    }

    func testBeginEpisodeWithNoCues() async throws {
        // No cues in store -- beginEpisode should succeed without error.
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        let log = await orchestrator.getDecisionLog()
        XCTAssertTrue(log.isEmpty, "No decisions should be logged when store has no cues")

        let confirmed = await orchestrator.confirmedWindows()
        XCTAssertTrue(confirmed.isEmpty, "No confirmed windows when store has no cues")
    }

    func testLiveDedupWithPreloaded() async throws {
        // Pre-materialize a cue in the store.
        let cue = SkipCue(
            id: "cue-pre",
            analysisAssetId: "asset-1",
            cueHash: "hash-pre",
            startTime: 20.0,
            endTime: 50.0,
            confidence: 0.8,
            source: "preAnalysis",
            materializedAt: Date().timeIntervalSince1970,
            wasSkipped: false,
            userDismissed: false
        )
        try await store.insertSkipCues([cue])

        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        // Now send a live AdWindow covering the same region.
        let liveWindow = AdWindow(
            id: "cue-pre",  // Same ID as the preloaded cue.
            analysisAssetId: "asset-1",
            startTime: 20.0,
            endTime: 50.0,
            confidence: 0.8,
            boundaryState: "confirmed",
            decisionState: "confirmed",
            detectorVersion: "live-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "live",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )

        await orchestrator.receiveAdWindows([liveWindow])

        // The orchestrator should NOT create a duplicate -- the same window ID
        // means the existing managed window is updated (not duplicated).
        let confirmed = await orchestrator.confirmedWindows()
        XCTAssertEqual(confirmed.count, 1, "Duplicate window should not create a second confirmed entry")
    }
}
