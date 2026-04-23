// EpisodeDurationPersistenceTests.swift
// playhead-gtt9.1.1: tests for Option B — persist episode duration on
// the `analysis_assets` row at spool time so the coverage guard has a
// durable denominator even when `activeShards` is never rehydrated
// (resume-from-persisted-`.backfill` paths).
//
// These tests complement the fail-safe shortcut tests in
// `PipelineSnapshotTests.swift` (Option C). Together they close the
// gap identified in playhead-gtt9.1: four real episodes were stamped
// `analysisState='complete'` with only 90s of fast-pass transcript
// covering 1800-7000s of audio because `currentEpisodeDuration()`
// returned 0 on resume and the guards bypassed permissively.

import Foundation
import Testing
@testable import Playhead

// MARK: - Pure resolver

@Suite("AnalysisCoordinator – resolveEpisodeDuration (gtt9.1.1)")
struct ResolveEpisodeDurationTests {

    private func makeShard(startTime: TimeInterval, duration: TimeInterval) -> AnalysisShard {
        AnalysisShard(
            id: Int(startTime),
            episodeID: "ep-test",
            startTime: startTime,
            duration: duration,
            samples: []
        )
    }

    @Test("activeShards is the first-choice source")
    func activeShardsWinsOverPersisted() {
        let shards = [
            makeShard(startTime: 0, duration: 30),
            makeShard(startTime: 30, duration: 30),
            makeShard(startTime: 60, duration: 30)
        ]
        let duration = AnalysisCoordinator.resolveEpisodeDuration(
            activeShards: shards,
            persistedDuration: 1800
        )
        #expect(duration == 90, "activeShards sum must win when both are present")
    }

    @Test("falls back to persistedDuration when activeShards is nil")
    func persistedUsedWhenShardsMissing() {
        let duration = AnalysisCoordinator.resolveEpisodeDuration(
            activeShards: nil,
            persistedDuration: 1800
        )
        #expect(duration == 1800)
    }

    @Test("returns 0 when both sources missing")
    func bothMissingReturnsZero() {
        let duration = AnalysisCoordinator.resolveEpisodeDuration(
            activeShards: nil,
            persistedDuration: nil
        )
        #expect(duration == 0)
    }

    @Test("empty shards fall through to persistedDuration")
    func emptyShardsFallsThroughToPersisted() {
        // The new helper uses `activeShards` when non-nil AND non-empty.
        // A non-nil-but-empty array shouldn't falsely claim duration = 0
        // and hide a real persisted value.
        let duration = AnalysisCoordinator.resolveEpisodeDuration(
            activeShards: [],
            persistedDuration: 1800
        )
        #expect(duration == 1800)
    }

    @Test("nonpositive persistedDuration is ignored")
    func nonpositivePersistedIsIgnored() {
        // A row persisted at `episodeDurationSec = 0` should be treated
        // the same as "missing" so the fail-safe path in the guards
        // fires rather than a rounded-to-zero ratio being compared.
        let duration = AnalysisCoordinator.resolveEpisodeDuration(
            activeShards: nil,
            persistedDuration: 0
        )
        #expect(duration == 0)
    }
}

// MARK: - Spool-time persistence (integration)

@Suite("AnalysisCoordinator – spool persists episodeDurationSec (gtt9.1.1)", .serialized)
struct SpoolPersistsEpisodeDurationTests {

    private func makeStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "EpisodeDurationPersistenceTests")
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        return store
    }

    @Test("updateEpisodeDuration persists and roundtrips")
    func updatePersistsValue() async throws {
        let store = try await makeStore()

        let assetId = "asset-dur-1"
        let asset = AnalysisAsset(
            id: assetId,
            episodeId: "ep-dur-1",
            assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(assetId).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)

        // Freshly-inserted row carries no duration.
        let beforeUpdate = try await store.fetchAsset(id: assetId)
        #expect(beforeUpdate?.episodeDurationSec == nil)

        // Persist the total audio duration (sum of shard durations).
        let totalAudio: Double = 1800.5  // e.g. 3 × 600.17s shards from decode
        try await store.updateEpisodeDuration(id: assetId, episodeDurationSec: totalAudio)

        let afterUpdate = try await store.fetchAsset(id: assetId)
        #expect(afterUpdate?.episodeDurationSec == totalAudio)
    }

    @Test("updateEpisodeDuration overwrites previous values (idempotent)")
    func updateOverwrites() async throws {
        let store = try await makeStore()

        let assetId = "asset-dur-2"
        let asset = AnalysisAsset(
            id: assetId,
            episodeId: "ep-dur-2",
            assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(assetId).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)

        try await store.updateEpisodeDuration(id: assetId, episodeDurationSec: 900)
        try await store.updateEpisodeDuration(id: assetId, episodeDurationSec: 1800)
        let finalRow = try await store.fetchAsset(id: assetId)
        #expect(finalRow?.episodeDurationSec == 1800)
    }
}

// MARK: - Resume-from-backfill end-to-end

/// Integration test mirroring the shape of CoverageGuardRecoveryTests:
/// seed a store in `.backfill` state with only 90s of fast-pass chunks
/// against a persisted 1800s episode duration, then invoke the resume
/// path with `activeShards == nil`. The coverage guard must fail the
/// session rather than stamp `.complete`.
///
/// Pre-fix (playhead-gtt9.1.1): `currentEpisodeDuration()` returned 0
/// on resume because `activeShards` was never rehydrated, both guards
/// bypassed permissively, and the session transitioned to `.complete`.
@Suite("AnalysisCoordinator – resume-from-backfill blocks .complete (gtt9.1.1)", .serialized)
struct ResumeFromBackfillBlocksCompleteTests {

    private func makeStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "ResumeFromBackfillBlocksCompleteTests")
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

    private func makeAsset(id: String, episodeDurationSec: Double?) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: 90,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.backfill.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDurationSec
        )
    }

    private func makeSession(id: String, assetId: String) -> AnalysisSession {
        AnalysisSession(
            id: id,
            analysisAssetId: assetId,
            state: SessionState.backfill.rawValue,
            startedAt: Date().timeIntervalSince1970,
            updatedAt: Date().timeIntervalSince1970,
            failureReason: nil
        )
    }

    private func makeChunk(
        assetId: String,
        chunkIndex: Int,
        startTime: Double,
        endTime: Double
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-chunk-\(chunkIndex)",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-\(assetId)-\(chunkIndex)",
            chunkIndex: chunkIndex,
            startTime: startTime,
            endTime: endTime,
            text: "x",
            normalizedText: "x",
            pass: TranscriptPassType.fast.rawValue,
            modelVersion: "speech-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            weakAnchorMetadata: nil
        )
    }

    @Test("resume with 90s fast-pass + persisted 1800s duration transitions .failed not .complete")
    func resumeWith90sChunksFailsInsteadOfComplete() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        // Seed: asset in `.backfill` with persisted duration, session
        // in `.backfill` with no failure, 3 fast-pass chunks covering
        // 0-90s (the production 90s ceiling from T0). activeShards is
        // unset on the coordinator (its fresh state).
        let assetId = "asset-gtt911-1"
        let sessionId = "session-gtt911-1"
        try await store.insertAsset(makeAsset(id: assetId, episodeDurationSec: 1800))
        try await store.insertSession(makeSession(id: sessionId, assetId: assetId))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, chunkIndex: 0, startTime: 0, endTime: 30),
            makeChunk(assetId: assetId, chunkIndex: 1, startTime: 30, endTime: 60),
            makeChunk(assetId: assetId, chunkIndex: 2, startTime: 60, endTime: 90)
        ])

        // Drive the resume path directly via the test seam that mirrors
        // production `runFromBackfill` on a fresh coordinator (transcript
        // task unset, activeShards nil).
        await coordinator.resumeBackfillForTesting(
            sessionId: sessionId,
            assetId: assetId,
            episodeId: "ep-\(assetId)"
        )

        // Expected post-fix: session must be `.failed` (coverage-guard
        // or restart-driven), never `.complete`. Pre-fix it was
        // `.complete` with analysisState 'complete'.
        let sessionAfter = try await store.fetchSession(id: sessionId)
        #expect(sessionAfter?.state != SessionState.complete.rawValue,
                "Resume-from-backfill with 90s/1800s coverage must NOT stamp .complete (gtt9.1.1)")
        #expect(sessionAfter?.state == SessionState.failed.rawValue,
                "Session should transition to .failed when restart is signalled")

        let assetAfter = try await store.fetchAsset(id: assetId)
        #expect(assetAfter?.analysisState == SessionState.failed.rawValue,
                "Asset state must mirror the session transition")
    }
}
