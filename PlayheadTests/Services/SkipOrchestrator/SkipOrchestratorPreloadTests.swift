// SkipOrchestratorPreloadTests.swift
//
// Bug 5 (skip-cues-deletion): the orchestrator now preloads
// confirmed-confidence rows directly from `ad_windows` rather than
// from the (deleted) `skip_cues` table. These tests pin the new
// preload path: high-confidence ad_windows are synthesized into the
// orchestrator's `confirmed` set, low-confidence and zero-length rows
// are filtered out, and live ingestion still dedups by window ID.

import CoreMedia
import XCTest
@testable import Playhead

final class SkipOrchestratorPreloadTests: XCTestCase {

    private var store: AnalysisStore!
    private var orchestrator: SkipOrchestrator!

    override func setUp() async throws {
        try await super.setUp()
        let dir = try makeTempDir(prefix: "SkipOrchestratorPreloadTests")
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

    // MARK: - Helpers

    /// Build a representative AdWindow row for the preload tests.
    /// `decisionState` defaults to `confirmed` so the seeded row is
    /// indistinguishable from one promoted by the live detection path.
    private func makeAdWindow(
        id: String,
        start: Double,
        end: Double,
        confidence: Double,
        decisionState: String = "confirmed"
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: "asset-1",
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: "confirmed",
            decisionState: decisionState,
            detectorVersion: "test-v1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: nil,
            metadataSource: "none",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false
        )
    }

    // MARK: - Tests

    func testBeginEpisodeLoadsHighConfidenceAdWindows() async throws {
        // Seed two high-confidence ad_windows (≥ 0.7) directly into the store.
        try await store.insertAdWindow(
            makeAdWindow(id: "win-1", start: 10.0, end: 40.0, confidence: 0.85)
        )
        try await store.insertAdWindow(
            makeAdWindow(id: "win-2", start: 60.0, end: 90.0, confidence: 0.9)
        )

        // Track skip cues pushed via the handler.
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues = ranges
        }

        // beginEpisode should load the windows and push them through the pipeline.
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        // The orchestrator should have processed the pre-loaded windows.
        // In default shadow mode, windows are confirmed (not applied), so the
        // decision log should have entries for the preloaded windows.
        let log = await orchestrator.getDecisionLog()
        XCTAssertFalse(log.isEmpty, "Decision log should contain entries from preloaded windows")

        // Confirmed windows should be available.
        let confirmed = await orchestrator.confirmedWindows()
        XCTAssertEqual(confirmed.count, 2, "Both preloaded windows should appear as confirmed windows")

        // The handler observation isn't asserted here — shadow-mode runs do
        // not push cues through to the playback service, but the local
        // variable is referenced so the compiler keeps the closure active.
        _ = pushedCues
    }

    func testBeginEpisodeWithNoAdWindows() async throws {
        // No ad_windows in store -- beginEpisode should succeed without error.
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        let log = await orchestrator.getDecisionLog()
        XCTAssertTrue(log.isEmpty, "No decisions should be logged when store has no ad_windows")

        let confirmed = await orchestrator.confirmedWindows()
        XCTAssertTrue(confirmed.isEmpty, "No confirmed windows when store has no ad_windows")
    }

    func testLowConfidenceAdWindowsAreFilteredFromPreload() async throws {
        // Mix of high- and low-confidence rows. Only the high-confidence
        // row (≥ 0.7) should be picked up by the preload.
        try await store.insertAdWindow(
            makeAdWindow(id: "win-high", start: 10.0, end: 40.0, confidence: 0.85)
        )
        try await store.insertAdWindow(
            makeAdWindow(id: "win-low", start: 60.0, end: 90.0, confidence: 0.5)
        )

        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        let confirmed = await orchestrator.confirmedWindows()
        XCTAssertEqual(confirmed.count, 1, "Only the ≥0.7 window should preload")
        XCTAssertEqual(confirmed.first?.id, "win-high")
    }

    func testZeroLengthAdWindowFilteredFromPreload() async throws {
        // Zero-length window: endTime == startTime → must be filtered even
        // if confidence clears the threshold. This mirrors the
        // (deleted) materializer's `endTime > startTime` guard.
        try await store.insertAdWindow(
            makeAdWindow(id: "win-zero", start: 60.0, end: 60.0, confidence: 0.95)
        )

        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        let confirmed = await orchestrator.confirmedWindows()
        XCTAssertTrue(confirmed.isEmpty, "Zero-length window must not preload")
    }

    func testLiveDedupWithPreloaded() async throws {
        // Pre-seed an ad_window in the store.
        try await store.insertAdWindow(
            makeAdWindow(id: "win-pre", start: 20.0, end: 50.0, confidence: 0.8)
        )

        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        // Now send a live AdWindow with the SAME ID covering the same region.
        let liveWindow = AdWindow(
            id: "win-pre",  // Same ID as the preloaded row.
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

        // The orchestrator must NOT create a duplicate -- the same window ID
        // means the existing managed window is updated (not duplicated).
        let confirmed = await orchestrator.confirmedWindows()
        XCTAssertEqual(confirmed.count, 1, "Duplicate window must not create a second confirmed entry")
    }
}
