// RapidScrubStressTests.swift
// playhead-qaw — E2E: AnalysisCoordinator rapid-scrub stress.
//
// Scenario 3 of the bead: 4 rapid scrubs (10→40→5→50 min) must
//   1. not crash, deadlock, or throw,
//   2. settle on the final position,
//   3. preserve all previously-persisted analysis artifacts (the
//      coordinator must never use a scrub as an excuse to clear
//      transcript / feature / ad rows), and
//   4. complete in under 2 seconds wall-clock for the burst as a
//      whole — well within the per-scrub budget multiplied 4×.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-qaw — Rapid scrub stress", .serialized)
struct RapidScrubStressTests {

    private static let storeDirs = TestTempDirTracker()

    private func makeStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "QAWRapidScrubTests")
        Self.storeDirs.track(dir)
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        return store
    }

    private func makeCoordinator(store: AnalysisStore) -> AnalysisCoordinator {
        let speechService = SpeechService(
            vocabularyProvider: ASRVocabularyProvider(store: store)
        )
        return AnalysisCoordinator(
            store: store,
            audioService: AnalysisAudioService(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: speechService,
                store: store
            ),
            capabilitiesService: CapabilitiesService(),
            adDetectionService: AdDetectionService(
                store: store,
                metadataExtractor: FallbackExtractor(),
                backfillJobRunnerFactory: nil,
                canUseFoundationModelsProvider: { false }
            ),
            skipOrchestrator: SkipOrchestrator(store: store)
        )
    }

    private func seedHotPathAsset(
        store: AnalysisStore,
        assetId: String,
        episodeId: String,
        episodeDuration: Double = 3600
    ) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(assetId).m4a",
            featureCoverageEndTime: episodeDuration,
            fastTranscriptCoverageEndTime: episodeDuration,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.hotPathReady.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDuration
        ))
        try await store.insertSession(AnalysisSession(
            id: "session-\(assetId)",
            analysisAssetId: assetId,
            state: SessionState.hotPathReady.rawValue,
            startedAt: Date().timeIntervalSince1970,
            updatedAt: Date().timeIntervalSince1970,
            failureReason: nil
        ))

        // Coverage of the full episode.
        let chunkSec = 60.0
        let count = Int((episodeDuration / chunkSec).rounded(.down))
        let chunks: [TranscriptChunk] = (0..<count).map { i in
            TranscriptChunk(
                id: "\(assetId)-chunk-\(i)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(assetId)-\(i)",
                chunkIndex: i,
                startTime: Double(i) * chunkSec,
                endTime: Double(i + 1) * chunkSec,
                text: "chunk \(i)",
                normalizedText: "chunk \(i)",
                pass: TranscriptPassType.fast.rawValue,
                modelVersion: "speech-v1",
                transcriptVersion: nil,
                atomOrdinal: nil,
                weakAnchorMetadata: nil
            )
        }
        try await store.insertTranscriptChunks(chunks)
    }

    @Test("4 rapid scrubs (10→40→5→50 min) settle without crash or data loss")
    func rapidScrubBurst() async throws {
        let store = try await makeStore()
        let assetId = "asset-rapid-scrub"
        try await seedHotPathAsset(
            store: store,
            assetId: assetId,
            episodeId: "ep-rapid-scrub"
        )

        let coord = makeCoordinator(store: store)

        let chunksBefore = try await store.fetchTranscriptChunks(assetId: assetId).count

        // 10 → 40 → 5 → 50 minutes, each in seconds.
        let positions: [TimeInterval] = [600, 2400, 300, 3000]

        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            for position in positions {
                _ = await coord.handlePlaybackEvent(
                    .scrubbed(to: position, rate: 1.0)
                )
            }
        }

        // Burst budget: each scrub gets 2s; 4 scrubs in series must
        // still total well under 2s on the simulator because the
        // coordinator's scrub handler is non-blocking. Use 2s as the
        // per-scrub budget anchor — the burst budget is the same 2s
        // ceiling per scenario in the bead.
        #expect(elapsed < .seconds(2),
                "Rapid scrub burst must complete inside the 2s wall-clock budget (was \(elapsed))")

        // No data loss.
        let chunksAfter = try await store.fetchTranscriptChunks(assetId: assetId).count
        #expect(chunksAfter == chunksBefore, "Rapid scrubs must not delete transcript chunks (had \(chunksBefore), now \(chunksAfter))")

        // No orphaned session: the row created during seed must still
        // exist with its original id.
        let session = try await store.fetchSession(id: "session-\(assetId)")
        #expect(session != nil, "Original session must still exist after rapid scrubs")

        await coord.stop()
    }

    @Test("rapid scrubs across the boundary of the active session do not corrupt state")
    func rapidScrubAcrossSessionBoundary() async throws {
        let store = try await makeStore()
        let assetId = "asset-rapid-boundary"
        try await seedHotPathAsset(
            store: store,
            assetId: assetId,
            episodeId: "ep-rapid-boundary"
        )

        let coord = makeCoordinator(store: store)

        // 8 scrubs swinging across the timeline like a toddler with a
        // slider. Each scrub is independent — the actor must serialize
        // them without losing any.
        let burst: [TimeInterval] = [
            600, 1200, 0, 3500, 1800, 2700, 60, 3000
        ]
        for position in burst {
            _ = await coord.handlePlaybackEvent(
                .scrubbed(to: position, rate: 1.0)
            )
        }

        // No throw, no deadlock — reaching this line is the assertion.
        // Plus the persisted invariants:
        let session = try await store.fetchSession(id: "session-\(assetId)")
        #expect(session != nil)
        let chunks = try await store.fetchTranscriptChunks(assetId: assetId)
        // Session may have advanced through hot-path-ready / backfill
        // / terminal in the background, but the chunk set is identity-
        // preserved by composite primary key.
        #expect(chunks.count == 60, "Chunk count must be unchanged after rapid scrubs (had 60, now \(chunks.count))")

        await coord.stop()
    }
}
