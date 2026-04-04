// TestFixtureValidationTests.swift
// Validates that shared stubs and factories compile and behave correctly.

import Foundation
import Testing
@testable import Playhead

@Suite("Test Fixture Validation")
struct TestFixtureValidationTests {

    // MARK: - StubAnalysisAudioProvider

    @Test("StubAnalysisAudioProvider returns configured shards")
    func stubAnalysisAudioProviderReturnsShards() async throws {
        let stub = StubAnalysisAudioProvider()
        let shard = makeShard(id: 0, episodeID: "ep-1")
        stub.shardsToReturn = [shard]

        let fileURL = LocalAudioURL(URL(fileURLWithPath: "/tmp/test.mp3"))!
        let result = try await stub.decode(fileURL: fileURL, episodeID: "ep-1", shardDuration: 30)
        #expect(result.count == 1)
        #expect(result[0].episodeID == "ep-1")
    }

    @Test("StubAnalysisAudioProvider throws configured error")
    func stubAnalysisAudioProviderThrows() async {
        let stub = StubAnalysisAudioProvider()
        stub.errorToThrow = AnalysisAudioError.cancelled

        let fileURL = LocalAudioURL(URL(fileURLWithPath: "/tmp/test.mp3"))!
        await #expect(throws: AnalysisAudioError.self) {
            try await stub.decode(fileURL: fileURL, episodeID: "ep-1", shardDuration: 30)
        }
    }

    // MARK: - StubAdDetectionProvider

    @Test("StubAdDetectionProvider returns configured AdWindows from hotPath")
    func stubAdDetectionHotPath() async throws {
        let stub = StubAdDetectionProvider()
        let window = makeAdWindow(startTime: 10, endTime: 40, confidence: 0.9)
        stub.hotPathResult = [window]

        let result = try await stub.runHotPath(chunks: [], analysisAssetId: "asset-1", episodeDuration: 600)
        #expect(result.count == 1)
        #expect(result[0].startTime == 10)
        #expect(stub.hotPathCallCount == 1)
    }

    @Test("StubAdDetectionProvider backfill tracks calls and can throw")
    func stubAdDetectionBackfill() async throws {
        let stub = StubAdDetectionProvider()
        try await stub.runBackfill(chunks: [], analysisAssetId: "asset-1", podcastId: "pod-1", episodeDuration: 600)
        #expect(stub.backfillCallCount == 1)

        stub.backfillError = AnalysisAudioError.cancelled
        await #expect(throws: AnalysisAudioError.self) {
            try await stub.runBackfill(chunks: [], analysisAssetId: "asset-1", podcastId: "pod-1", episodeDuration: 600)
        }
    }

    // MARK: - StubCapabilitiesProvider

    @Test("StubCapabilitiesProvider returns configured snapshot")
    func stubCapabilitiesProvider() async {
        let snapshot = makeCapabilitySnapshot(thermalState: .serious, isLowPowerMode: true)
        let stub = StubCapabilitiesProvider(snapshot: snapshot)

        let current = await stub.currentSnapshot
        #expect(current.thermalState == .serious)
        #expect(current.isLowPowerMode == true)
        #expect(current.shouldThrottleAnalysis == true)
    }

    @Test("StubCapabilitiesProvider streams current snapshot")
    func stubCapabilitiesProviderStream() async {
        let stub = StubCapabilitiesProvider()
        let stream = await stub.capabilityUpdates()
        var snapshots: [CapabilitySnapshot] = []
        for await s in stream {
            snapshots.append(s)
        }
        #expect(snapshots.count == 1)
        #expect(snapshots[0].thermalState == .nominal)
    }

    // MARK: - StubDownloadProvider

    @Test("StubDownloadProvider returns configured URLs and fingerprints")
    func stubDownloadProvider() async {
        let stub = StubDownloadProvider()
        let url = URL(fileURLWithPath: "/tmp/cached.mp3")
        stub.cachedURLs["ep-1"] = url
        stub.fingerprints["ep-1"] = AudioFingerprint(weak: "weak-fp", strong: "strong-fp")

        let cachedURL = await stub.cachedFileURL(for: "ep-1")
        #expect(cachedURL == url)

        let fp = await stub.fingerprint(for: "ep-1")
        #expect(fp?.strong == "strong-fp")

        let missing = await stub.cachedFileURL(for: "nonexistent")
        #expect(missing == nil)
    }

    // MARK: - TestHelpers

    @Test("makeTempDir creates a directory and TestTempDirTracker tracks it")
    func tempDirAndTracker() throws {
        let tracker = TestTempDirTracker()
        let dir = try makeTempDir(prefix: "ValidationTest")
        tracker.track(dir)
        #expect(FileManager.default.fileExists(atPath: dir.path))
    }

    @Test("makeTestStore creates a migrated AnalysisStore")
    func testStoreCreation() async throws {
        let store = try await makeTestStore()
        // Verify basic operation by inserting and fetching.
        let asset = AnalysisAsset(
            id: "asset-1",
            episodeId: "ep-1",
            assetFingerprint: "fp-1",
            weakFingerprint: nil,
            sourceURL: "https://example.com/ep.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "pending",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)
        let fetched = try await store.fetchAsset(id: "asset-1")
        #expect(fetched != nil)
        #expect(fetched?.episodeId == "ep-1")
    }

    @Test("makeShard produces a valid shard with expected sample count")
    func shardFactory() {
        let shard = makeShard(id: 3, episodeID: "ep-test", startTime: 60, duration: 10)
        #expect(shard.id == 3)
        #expect(shard.episodeID == "ep-test")
        #expect(shard.startTime == 60)
        #expect(shard.duration == 10)
        #expect(shard.sampleCount == 160_000) // 16 kHz * 10 s
    }

    // MARK: - TestFactories

    @Test("makeAdWindow produces valid AdWindow with defaults")
    func adWindowFactory() {
        let window = makeAdWindow()
        #expect(window.startTime == 60.0)
        #expect(window.endTime == 90.0)
        #expect(window.confidence == 0.85)
        #expect(window.decisionState == "candidate")
    }

    @Test("makeCapabilitySnapshot produces valid snapshot")
    func capabilitySnapshotFactory() {
        let snapshot = makeCapabilitySnapshot(thermalState: .critical, isLowPowerMode: true)
        #expect(snapshot.thermalState == .critical)
        #expect(snapshot.isLowPowerMode == true)
        #expect(snapshot.shouldThrottleAnalysis == true)
    }
}
