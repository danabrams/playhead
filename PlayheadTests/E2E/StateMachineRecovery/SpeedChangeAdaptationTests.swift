// SpeedChangeAdaptationTests.swift
// playhead-qaw — E2E: AnalysisCoordinator speed-change handling.
//
// Scenario 4 of the bead: switching playback speed during analysis must
//   1. update the coordinator's internal snapshot so downstream stages
//      observe the new rate (which feeds into the wall-clock safety
//      margin used by the hot-path planner),
//   2. complete within the 2-second wall-clock budget per change, and
//   3. not delete or duplicate any persisted analysis artifact.
//
// Note on scope: the actual hot-path safety-margin recalculation is
// owned by `TranscriptEngineService.handleSpeedChange` and is unit-
// tested elsewhere. The invariant under test here is the coordinator-
// level non-destruction guarantee plus the budget — the coordinator
// must not block the caller when forwarding a speed change, regardless
// of which downstream service is doing the recalc.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-qaw — Speed change adaptation", .serialized)
struct SpeedChangeAdaptationTests {

    private static let storeDirs = TestTempDirTracker()

    private func makeStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "QAWSpeedTests")
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

    private func seedActiveAsset(
        store: AnalysisStore,
        assetId: String,
        episodeId: String
    ) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(assetId).m4a",
            featureCoverageEndTime: 600,
            fastTranscriptCoverageEndTime: 600,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.hotPathReady.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 600
        ))
        try await store.insertSession(AnalysisSession(
            id: "session-\(assetId)",
            analysisAssetId: assetId,
            state: SessionState.hotPathReady.rawValue,
            startedAt: Date().timeIntervalSince1970,
            updatedAt: Date().timeIntervalSince1970,
            failureReason: nil
        ))
        let chunks: [TranscriptChunk] = (0..<60).map { i in
            TranscriptChunk(
                id: "\(assetId)-chunk-\(i)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(assetId)-\(i)",
                chunkIndex: i,
                startTime: Double(i) * 10,
                endTime: Double(i + 1) * 10,
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

    @Test("speedChanged(rate:3.0) returns within the 2-second budget")
    func speedChangeReturnsWithinBudget() async throws {
        let store = try await makeStore()
        let coord = makeCoordinator(store: store)

        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            _ = await coord.handlePlaybackEvent(
                .speedChanged(rate: 3.0, time: 600.0)
            )
        }
        #expect(elapsed < .seconds(2),
                "Speed-change event must return inside 2 seconds (was \(elapsed))")
    }

    @Test("1x → 3x → 1x cycle preserves persisted analysis artifacts")
    func speedCyclePreservesPriorWork() async throws {
        let store = try await makeStore()
        let assetId = "asset-speed-cycle"
        try await seedActiveAsset(
            store: store,
            assetId: assetId,
            episodeId: "ep-speed-cycle"
        )

        let coord = makeCoordinator(store: store)

        let chunksBefore = try await store.fetchTranscriptChunks(assetId: assetId).count

        // Cycle through 1x → 1.5x → 3x → back to 1x.
        for rate in [Float(1.0), 1.5, 3.0, 1.5, 1.0] {
            _ = await coord.handlePlaybackEvent(
                .speedChanged(rate: rate, time: 30.0)
            )
        }

        // Allow any spawned reprioritization tasks to settle.
        try? await Task.sleep(for: .milliseconds(100))

        let chunksAfter = try await store.fetchTranscriptChunks(assetId: assetId).count
        #expect(chunksAfter == chunksBefore, "Speed cycles must not delete transcript chunks (had \(chunksBefore), now \(chunksAfter))")

        await coord.stop()
    }

    @Test("speed change while no active session is a safe no-op")
    func speedChangeWithoutActiveSessionDoesNotCrash() async throws {
        let store = try await makeStore()
        let coord = makeCoordinator(store: store)

        // No session has been resolved on this coordinator —
        // `activeShards`, `activeAssetId` are nil. The handler must
        // tolerate a speed change in this state without throwing or
        // deadlocking.
        _ = await coord.handlePlaybackEvent(
            .speedChanged(rate: 2.0, time: 0)
        )
        // Reaching this line is the assertion.
        await coord.stop()
    }
}
