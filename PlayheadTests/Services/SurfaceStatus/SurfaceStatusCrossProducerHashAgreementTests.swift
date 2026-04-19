// SurfaceStatusCrossProducerHashAgreementTests.swift
// End-to-end guard on the `false_ready_rate` metric contract: the
// `episode_id_hash` stamped by `SkipOrchestrator` on `auto_skip_fired`
// MUST byte-match the hash `EpisodeSurfaceStatusObserver` stamps on
// `ready_entered` for the same episode. Otherwise the script that pairs
// numerator (auto_skip_fired) with denominator (ready_entered) — by
// episode_id_hash — would never find a match and the metric would
// collapse.
//
// Regression context: a prior build hashed the analysis-asset UUID on
// `auto_skip_fired` while hashing the canonical episode key on
// `ready_entered`. The two strings are never equal, so the pairing
// failed silently and `false_ready_rate` reported ~100% false. This
// test drives both producers with the same episodeId and verifies the
// resulting JSONL entries carry identical episodeIdHash values.
//
// Scope: playhead-o45p — M1 code-review fix.

import Foundation
import Testing

@testable import Playhead

@Suite("SurfaceStatus cross-producer hash agreement (playhead-o45p M1)", .serialized)
struct SurfaceStatusCrossProducerHashAgreementTests {

    private static func makeTempDirectory() -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("o45p-cross-\(UUID().uuidString)", isDirectory: true)
        try? FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }

    private static func readAllEntries(_ url: URL) throws -> [SurfaceStateTransitionEntry] {
        let data = try Data(contentsOf: url)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try String(decoding: data, as: UTF8.self)
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { try decoder.decode(SurfaceStateTransitionEntry.self, from: Data($0.utf8)) }
    }

    private static func makeCapabilitySnapshot() -> CapabilitySnapshot {
        CapabilitySnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: true,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 10_000_000_000,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
    }

    @Test("ready_entered and auto_skip_fired share the same episodeIdHash for one episode")
    func readyAndAutoSkipAgreeOnEpisodeHash() async throws {
        let dir = Self.makeTempDirectory()
        SurfaceStatusInvariantLogger._resetForTesting(directory: dir)
        defer { SurfaceStatusInvariantLogger._resetForTesting() }

        // Shared episode identity. Intentionally distinct from the asset
        // ID so a regression that hashes the asset instead of the
        // episode would make the two producers disagree.
        let episodeId = "episode-o45p-cross"
        let assetId = "asset-o45p-cross"

        // --- Producer A: EpisodeSurfaceStatusObserver (ready_entered) ---
        //
        // Seed a `.complete` asset and drive the observer's cold-start
        // path. With a usable capability snapshot and full coverage, the
        // reducer maps to a ready-for-playback disposition and the
        // emitter fires `ready_entered` through the default logger sink
        // — which writes to the same JSONL session file the orchestrator
        // writes to.
        let store = try await makeTestStore()
        let asset = AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(assetId).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.complete.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)

        let snapshot = Self.makeCapabilitySnapshot()
        let observer = EpisodeSurfaceStatusObserver(
            store: store,
            capabilitySnapshotProvider: { snapshot }
            // Use the default episodeIdHasher and default emitter so the
            // test exercises the real production hashing salt + the real
            // JSONL sink — exactly what runs in the app.
        )
        await observer.observeEpisodePlayStarted(episodeId: episodeId)

        // --- Producer B: SkipOrchestrator (auto_skip_fired) ---
        //
        // Drive a high-confidence confirmed window through the auto-skip
        // path so `recordAutoSkipFired` fires to the same JSONL file.
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: "podcast-1"
        )
        let window = makeSkipTestAdWindow(
            id: "ad-o45p-cross",
            assetId: assetId,
            startTime: 30.0,
            endTime: 60.0,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([window])

        // --- Assert agreement on the episode_id_hash ---
        SurfaceStatusInvariantLogger._flushForTesting()
        let sessionURL = try #require(SurfaceStatusInvariantLogger._currentSessionFileURL())

        // Retry briefly while the serial write queue drains — both
        // producers enqueue asynchronously.
        var readyEntries: [SurfaceStateTransitionEntry] = []
        var autoSkipEntries: [SurfaceStateTransitionEntry] = []
        for _ in 0..<10 {
            let all = try Self.readAllEntries(sessionURL)
            readyEntries = all.filter { $0.eventType == .readyEntered }
            autoSkipEntries = all.filter { $0.eventType == .autoSkipFired }
            if !readyEntries.isEmpty, !autoSkipEntries.isEmpty { break }
            SurfaceStatusInvariantLogger._flushForTesting()
        }

        let readyHash = try #require(readyEntries.first?.episodeIdHash)
        let autoSkipHash = try #require(autoSkipEntries.first?.episodeIdHash)

        // The core contract: byte-identical hashes. Without this,
        // `scripts/false_ready_rate.swift` cannot pair the two events
        // and the metric is silently broken.
        #expect(readyHash == autoSkipHash)

        // And both must match the episode-ID hash (not the asset-ID
        // hash) — defends against a regression that hashes the analysis
        // asset in either site.
        let expectedHash = SurfaceStatusInvariantLogger.hashEpisodeId(episodeId)
        #expect(readyHash == expectedHash)
        #expect(autoSkipHash == expectedHash)
    }
}
