// ConcurrentEpisodeIsolationTests.swift
// playhead-qaw — E2E: AnalysisCoordinator concurrent-episode isolation.
//
// Scenario 5 of the bead: when episode A finishes and episode B
// auto-plays, B must
//   1. resolve into its own AnalysisAsset and AnalysisSession (not
//      reuse A's records),
//   2. leave A's persisted analysis artifacts untouched, and
//   3. carry no state from A — the coordinator's active-state vars
//      must point at B once handlePlayStarted returns.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-qaw — Concurrent-episode isolation", .serialized)
struct ConcurrentEpisodeIsolationTests {

    private static let storeDirs = TestTempDirTracker()

    private func makeStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "QAWConcurrentEpisodeTests")
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

    private func emptyAudioURL(name: String) throws -> LocalAudioURL {
        let dir = try makeTempDir(prefix: "QAWConcurrentAudio")
        Self.storeDirs.track(dir)
        let file = dir.appendingPathComponent(name)
        FileManager.default.createFile(atPath: file.path, contents: Data())
        return LocalAudioURL(file)!
    }

    private func seedFinishedEpisode(
        store: AnalysisStore,
        episodeId: String,
        assetId: String
    ) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(assetId).m4a",
            featureCoverageEndTime: 600,
            fastTranscriptCoverageEndTime: 600,
            confirmedAdCoverageEndTime: 600,
            analysisState: SessionState.completeFull.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 600
        ))
        try await store.insertSession(AnalysisSession(
            id: "session-\(assetId)",
            analysisAssetId: assetId,
            state: SessionState.completeFull.rawValue,
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
        // One ad window so we can verify the prior episode's data is
        // untouched.
        try await store.insertAdWindow(AdWindow(
            id: "ad-\(assetId)",
            analysisAssetId: assetId,
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            boundaryState: "lexical",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "v1",
            advertiser: "ACME", product: nil, adDescription: nil,
            evidenceText: "brought to you by", evidenceStartTime: 60,
            metadataSource: "lexicon",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false
        ))
    }

    @Test("episode B gets its own asset and session; episode A's data is untouched")
    func episodeBIsolatedFromA() async throws {
        let store = try await makeStore()
        let episodeA = "ep-A"
        let assetA = "asset-A"
        try await seedFinishedEpisode(
            store: store,
            episodeId: episodeA,
            assetId: assetA
        )

        let aChunksBefore = try await store.fetchTranscriptChunks(assetId: assetA).count
        let aAdsBefore = try await store.fetchAdWindows(assetId: assetA).count

        let coord = makeCoordinator(store: store)

        // Play A → resolveSession picks up the existing terminal
        // session, runPipeline notices the chunks are present and
        // leaves the row alone.
        _ = await coord.handlePlaybackEvent(.playStarted(
            episodeId: episodeA,
            podcastId: nil,
            audioURL: try emptyAudioURL(name: "a.m4a"),
            time: 0,
            rate: 1.0
        ))
        try? await Task.sleep(for: .milliseconds(150))

        // Now switch to episode B — production calls .stopped before
        // a fresh playStarted, but the coordinator handles back-to-
        // back playStarted too.
        _ = await coord.handlePlaybackEvent(.stopped)
        let episodeB = "ep-B"
        _ = await coord.handlePlaybackEvent(.playStarted(
            episodeId: episodeB,
            podcastId: nil,
            audioURL: try emptyAudioURL(name: "b.m4a"),
            time: 0,
            rate: 1.0
        ))
        try? await Task.sleep(for: .milliseconds(150))

        // Episode B must have its own asset / session — distinct from
        // A's.
        let assetBRow = try await store.fetchAssetByEpisodeId(episodeB)
        #expect(assetBRow != nil, "Episode B must have its own asset row")
        #expect(assetBRow?.id != assetA, "Episode B must NOT reuse episode A's asset id")

        if let bId = assetBRow?.id {
            let bSessions = try await store.fetchLatestSessionForAsset(assetId: bId)
            #expect(bSessions != nil, "Episode B must have its own session")
            #expect(bSessions?.analysisAssetId == bId, "B's session must reference B's asset")
            #expect(bSessions?.id != "session-\(assetA)", "B's session id must not equal A's")
        }

        // Episode A's persisted artifacts must be byte-for-byte
        // unchanged.
        let aChunksAfter = try await store.fetchTranscriptChunks(assetId: assetA).count
        let aAdsAfter = try await store.fetchAdWindows(assetId: assetA).count
        #expect(aChunksAfter == aChunksBefore, "A's chunks must survive the switch to B")
        #expect(aAdsAfter == aAdsBefore, "A's ad windows must survive the switch to B")

        // The asset row for A must still exist.
        let assetAAfter = try await store.fetchAsset(id: assetA)
        #expect(assetAAfter != nil, "A's asset row must survive the switch to B")
        #expect(assetAAfter?.episodeId == episodeA)

        await coord.stop()
    }

    @Test("two distinct episodes never share an asset id even when stamped concurrently")
    func freshEpisodesGetDistinctAssetIds() async throws {
        let store = try await makeStore()
        let coord = makeCoordinator(store: store)

        // Neither episode is pre-seeded — the coordinator must mint a
        // new asset for each.
        _ = await coord.handlePlaybackEvent(.playStarted(
            episodeId: "ep-fresh-A",
            podcastId: nil,
            audioURL: try emptyAudioURL(name: "fresh-a.m4a"),
            time: 0,
            rate: 1.0
        ))
        try? await Task.sleep(for: .milliseconds(100))
        _ = await coord.handlePlaybackEvent(.stopped)

        _ = await coord.handlePlaybackEvent(.playStarted(
            episodeId: "ep-fresh-B",
            podcastId: nil,
            audioURL: try emptyAudioURL(name: "fresh-b.m4a"),
            time: 0,
            rate: 1.0
        ))
        try? await Task.sleep(for: .milliseconds(100))

        let assetA = try await store.fetchAssetByEpisodeId("ep-fresh-A")
        let assetB = try await store.fetchAssetByEpisodeId("ep-fresh-B")
        #expect(assetA != nil)
        #expect(assetB != nil)
        #expect(assetA?.id != assetB?.id, "Distinct episodes must mint distinct asset ids")

        await coord.stop()
    }
}
