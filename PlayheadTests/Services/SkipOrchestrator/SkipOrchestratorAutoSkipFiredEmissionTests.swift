// SkipOrchestratorAutoSkipFiredEmissionTests.swift
// Verifies that SkipOrchestrator emits an `auto_skip_fired` event to the
// ol05 state-transition log whenever its auto-skip policy applies a
// window at playhead-time.
//
// Scope: playhead-o45p (false_ready_rate instrumentation — Wave 4 pass
// criterion 3).
//
// Concurrency: each test constructs its own `SurfaceStatusInvariantLogger`
// instance pointed at a unique temp directory. No shared global state,
// so tests run in parallel without a cross-suite mutex.

import Foundation
import Testing

@testable import Playhead

@Suite("SkipOrchestrator — auto_skip_fired emission (playhead-o45p)")
struct SkipOrchestratorAutoSkipFiredEmissionTests {

    private static func makeTempDirectory() -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("o45p-orchestrator-\(UUID().uuidString)", isDirectory: true)
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

    @Test("auto_skip_fired is emitted when auto mode applies a high-confidence window")
    func autoSkipFiredEmitted() async throws {
        let dir = Self.makeTempDirectory()
        let logger = SurfaceStatusInvariantLogger(directory: dir)
        let hasher: @Sendable (String) -> String = { [logger] in
            logger.hashEpisodeId($0)
        }

        let runTag = UUID().uuidString
        let assetId = "asset-o45p-\(runTag)"
        let episodeId = "episode-o45p-\(runTag)"

        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId))
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            invariantLogger: logger,
            episodeIdHasher: hasher
        )
        // Use distinct asset and episode IDs so the assertion at the
        // bottom actually proves we hash the episode ID (not the asset
        // ID). `false_ready_rate` pairs `auto_skip_fired` with
        // `ready_entered` by the episode-ID hash; if the orchestrator
        // hashed the asset ID, every pairing would silently fail.
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: "podcast-1"
        )

        // High-confidence confirmed window — the auto-skip policy will
        // promote it to `.applied` and fire the skip cue. Span must be
        // >= 15s (the default minimum) AND confidence above the 0.65
        // enter threshold to reach the apply path cleanly.
        let window = makeSkipTestAdWindow(
            id: "ad-o45p-\(runTag)",
            assetId: assetId,
            startTime: 30.0,
            endTime: 60.0,
            confidence: 0.8,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([window])

        logger.flushForTesting()

        let sessionURL = try #require(logger.currentSessionFileURL)
        // Retry briefly while the serial write queue drains.
        var autoSkipEntries: [SurfaceStateTransitionEntry] = []
        for _ in 0..<10 {
            let all = try Self.readAllEntries(sessionURL)
            autoSkipEntries = all.filter { $0.eventType == .autoSkipFired }
            if !autoSkipEntries.isEmpty { break }
            logger.flushForTesting()
        }

        #expect(autoSkipEntries.count == 1)
        #expect(autoSkipEntries.first?.windowStartMs == 30_000)
        #expect(autoSkipEntries.first?.windowEndMs == 60_000)

        // The episode hash must match what the hasher produces for the
        // EPISODE ID (NOT the analysis asset ID) — cross-event
        // correlation with `ready_entered` depends on this.
        #expect(autoSkipEntries.first?.episodeIdHash == hasher(episodeId))
        // And the asset-ID hash must NOT match — guards against a
        // regression where the orchestrator reverts to hashing the asset.
        #expect(autoSkipEntries.first?.episodeIdHash != hasher(assetId))
    }

    @Test("auto_skip_fired is NOT emitted when mode is shadow (no auto-skip)")
    func autoSkipFiredNotEmittedInShadowMode() async throws {
        let dir = Self.makeTempDirectory()
        let logger = SurfaceStatusInvariantLogger(directory: dir)
        let hasher: @Sendable (String) -> String = { [logger] in
            logger.hashEpisodeId($0)
        }

        let runTag = UUID().uuidString
        let assetId = "asset-o45p-shadow-\(runTag)"
        let episodeId = "episode-o45p-shadow-\(runTag)"

        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId))
        let trustService = try await makeSkipTestTrustService(
            mode: "shadow",
            trustScore: 0.5,
            observations: 0
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            invariantLogger: logger,
            episodeIdHasher: hasher
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: "podcast-1"
        )

        let window = makeSkipTestAdWindow(
            id: "ad-o45p-shadow-\(runTag)",
            assetId: assetId,
            startTime: 30.0,
            endTime: 60.0,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([window])

        logger.flushForTesting()

        // If no session file was ever opened, there are no entries. If
        // one was opened, it must not contain any autoSkipFired entries.
        if let sessionURL = logger.currentSessionFileURL,
           FileManager.default.fileExists(atPath: sessionURL.path) {
            let entries = try Self.readAllEntries(sessionURL)
            let autoSkipEntries = entries.filter { $0.eventType == .autoSkipFired }
            #expect(autoSkipEntries.isEmpty)
        }
    }
}
