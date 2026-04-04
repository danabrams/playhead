// SkipCueMaterializerTests.swift

import XCTest
@testable import Playhead

final class SkipCueMaterializerTests: XCTestCase {

    private var store: AnalysisStore!

    override func setUp() async throws {
        try await super.setUp()
        // In-memory store (unique temp dir per test).
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("SkipCueMaterializerTests-\(UUID().uuidString)")
        store = try await AnalysisStore.open(directory: dir)

        // Insert a dummy analysis asset so foreign-key-like lookups work.
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
    }

    // MARK: - Tests

    func testMaterializeAboveThreshold() async throws {
        let windows = [
            makeAdWindow(id: "w1", start: 10, end: 40, confidence: 0.8),
            makeAdWindow(id: "w2", start: 50, end: 80, confidence: 0.9),
            makeAdWindow(id: "w3", start: 100, end: 130, confidence: 0.5),
        ]

        let materializer = SkipCueMaterializer(store: store, confidenceThreshold: 0.7)
        let cues = try await materializer.materialize(windows: windows, analysisAssetId: "asset-1")

        XCTAssertEqual(cues.count, 2, "Only windows at 0.8 and 0.9 should pass the 0.7 threshold")

        let fetched = try await store.fetchSkipCues(for: "asset-1")
        XCTAssertEqual(fetched.count, 2)
    }

    func testDedup() async throws {
        let windows = [
            makeAdWindow(id: "w1", start: 10, end: 40, confidence: 0.8),
        ]

        let materializer = SkipCueMaterializer(store: store, confidenceThreshold: 0.7)

        let first = try await materializer.materialize(windows: windows, analysisAssetId: "asset-1")
        XCTAssertEqual(first.count, 1)

        // Second call with the same windows should still return cues (they are created
        // in-memory), but the store should only contain 1 row due to cueHash UNIQUE.
        let second = try await materializer.materialize(windows: windows, analysisAssetId: "asset-1")
        XCTAssertEqual(second.count, 1, "materialize always returns the mapped cues")

        let fetched = try await store.fetchSkipCues(for: "asset-1")
        XCTAssertEqual(fetched.count, 1, "INSERT OR IGNORE deduplicates on cueHash")
    }

    func testCueHashRounding() {
        // Windows at 12.3-45.7 and 12.8-45.2 both truncate to Int(12):Int(45).
        let hash1 = SkipCueMaterializer.computeCueHash(
            analysisAssetId: "asset-1", startTime: 12.3, endTime: 45.7
        )
        let hash2 = SkipCueMaterializer.computeCueHash(
            analysisAssetId: "asset-1", startTime: 12.8, endTime: 45.2
        )
        XCTAssertEqual(hash1, hash2, "Fractional seconds should round to same integer hash")
    }

    func testBelowThresholdFiltered() async throws {
        let windows = [
            makeAdWindow(id: "w1", start: 10, end: 40, confidence: 0.3),
            makeAdWindow(id: "w2", start: 50, end: 80, confidence: 0.5),
            makeAdWindow(id: "w3", start: 100, end: 130, confidence: 0.69),
        ]

        let materializer = SkipCueMaterializer(store: store, confidenceThreshold: 0.7)
        let cues = try await materializer.materialize(windows: windows, analysisAssetId: "asset-1")

        XCTAssertEqual(cues.count, 0, "All windows are below the 0.7 threshold")

        let fetched = try await store.fetchSkipCues(for: "asset-1")
        XCTAssertEqual(fetched.count, 0)
    }

    func testSourceTagging() async throws {
        let windows = [
            makeAdWindow(id: "w1", start: 10, end: 40, confidence: 0.8),
            makeAdWindow(id: "w2", start: 50, end: 80, confidence: 0.9),
        ]

        let materializer = SkipCueMaterializer(store: store, confidenceThreshold: 0.7)
        let cues = try await materializer.materialize(
            windows: windows,
            analysisAssetId: "asset-1",
            source: "preAnalysis"
        )

        XCTAssertEqual(cues.count, 2)
        for cue in cues {
            XCTAssertEqual(cue.source, "preAnalysis", "Every materialized cue should carry the source tag")
        }

        // Verify persisted cues also have the correct source.
        let fetched = try await store.fetchSkipCues(for: "asset-1")
        for cue in fetched {
            XCTAssertEqual(cue.source, "preAnalysis")
        }
    }

    // MARK: - Helpers

    private func makeAdWindow(id: String, start: Double, end: Double, confidence: Double) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: "asset-1",
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: "confirmed",
            decisionState: "confirmed",
            detectorVersion: "test-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }
}
