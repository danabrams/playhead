// FinalizeBackfillDenominatorFixTests.swift
// `bead/finalize-denominator-fix`: regression tests for the stale-
// denominator bug at the `finalizeBackfill` callsite.
//
// Background: on libsyn/flightcast feeds with truncated
// `<itunes:duration>` metadata, the initial-decode shard sum is a tiny
// fraction (~13–23%) of the true audio length. That sum is written to
// `analysis_assets.episodeDurationSec` at spool time and cached in
// `cachedPersistedEpisodeDuration`, but it is never refreshed when the
// streaming-decode shards extend `activeShards`. Meanwhile, the launch-
// time duration-backfill sweep probes the cached audio file and
// rewrites `episodeDurationSec` to the probed truth. When
// `finalizeBackfill` then reads the live `currentEpisodeDuration()`
// (driven by `activeShards` and the stale cache), the resulting ratio
// is computed against ~13% of the real episode length and the session
// gets stamped `completeFull` after only a small fraction of audio was
// actually transcribed.
//
// Real-world example: asset 8A9DFC82, 5645s episode, closed
// `completeFull` with `terminalReason="full coverage: transcript 1.163,
// feature 1.724"` after only 1290s of feature coverage and 870s of
// transcript (denominator was 748s, the stale shard sum).
//
// Fix: at the finalize callsite the denominator is now
// `max(currentEpisodeDuration(), asset.episodeDurationSec ?? 0)`, so a
// probe-rewritten or larger persisted value beats a stale shard sum.

import Foundation
import Testing

@testable import Playhead

@Suite("AnalysisCoordinator – finalizeBackfill denominator fix", .serialized)
struct FinalizeBackfillDenominatorFixTests {

    // MARK: - Fixtures

    private func makeStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "FinalizeDenominatorFixTests")
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

    private func makeAsset(
        id: String,
        episodeDurationSec: Double?,
        featureCoverageEndTime: Double?,
        fastTranscriptCoverageEndTime: Double?
    ) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(id).m4a",
            featureCoverageEndTime: featureCoverageEndTime,
            fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime,
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

    private func makeShard(
        episodeId: String,
        index: Int,
        startTime: Double,
        duration: Double
    ) -> AnalysisShard {
        AnalysisShard(
            id: index,
            episodeID: episodeId,
            startTime: startTime,
            duration: duration,
            samples: []
        )
    }

    // Build a shard set whose end-time sum totals approximately
    // `totalSeconds`. Two shards are sufficient to make the resolver
    // pick rule 1 (non-empty `activeShards`).
    private func makeShards(
        episodeId: String,
        totalSeconds: Double
    ) -> [AnalysisShard] {
        let half = totalSeconds / 2.0
        return [
            makeShard(episodeId: episodeId, index: 0, startTime: 0, duration: half),
            makeShard(episodeId: episodeId, index: 1, startTime: half, duration: half)
        ]
    }

    // MARK: - Reproduction of asset 8A9DFC82

    /// The literal numbers from the bug report. The asset row carries
    /// `episodeDurationSec=5645` (already healed by the duration-
    /// backfill probe sweep). The live `activeShards` sum is ~748s
    /// (the truncated-metadata initial decode that was never refreshed
    /// after streaming-decode appended). Feature/transcript coverage
    /// satisfies the 95% floor against 748 but is well below 95% of
    /// 5645. Pre-fix the verdict was `.completeFull` with
    /// `transcript 1.163, feature 1.724`. Post-fix the verdict must
    /// not be `.completeFull` — coverage truly is short.
    @Test("asset 8A9DFC82 reproduction: stale shard sum must not stamp .completeFull")
    func staleShardSumDoesNotStampCompleteFull() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        let assetId = "asset-8A9DFC82"
        let sessionId = "session-8A9DFC82"
        let trueDuration: Double = 5645
        // Match the bug report numbers: 870s transcript, 1290s feature.
        // Both are above 95% of 748 (the stale shard sum) but below 95%
        // of 5645 (the truth).
        let transcriptEnd: Double = 870
        let featureEnd: Double = 1290

        try await store.insertAsset(makeAsset(
            id: assetId,
            episodeDurationSec: trueDuration,
            featureCoverageEndTime: featureEnd,
            fastTranscriptCoverageEndTime: transcriptEnd
        ))
        try await store.insertSession(makeSession(id: sessionId, assetId: assetId))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, chunkIndex: 0, startTime: 0, endTime: 435),
            makeChunk(assetId: assetId, chunkIndex: 1, startTime: 435, endTime: transcriptEnd)
        ])

        // Stale shard sum: ~748s, the value the bug hit production with.
        let staleShards = makeShards(episodeId: "ep-\(assetId)", totalSeconds: 748)

        try await coordinator.finalizeBackfillForTesting(
            sessionId: sessionId,
            assetId: assetId,
            episodeId: "ep-\(assetId)",
            activeShards: staleShards
        )

        let sessionAfter = try await store.fetchSession(id: sessionId)
        #expect(
            sessionAfter?.state != SessionState.completeFull.rawValue,
            "Stale-denominator regression: session must NOT be stamped .completeFull (true coverage is ~15%, denominator should reflect 5645s)"
        )

        // The stale-denominator scenario maps to a feature-coverage
        // shortfall (1290/5645 ≈ 0.229 < 0.95), so the classifier
        // routes to .failedFeature. We assert this exact terminal so
        // future drift is caught.
        #expect(
            sessionAfter?.state == SessionState.failedFeature.rawValue,
            "Expected .failedFeature; got \(sessionAfter?.state ?? "nil")"
        )
    }

    /// Verify that the persisted `terminalReason` ratios are computed
    /// against the corrected 5645s denominator, not the stale 748s.
    /// Pre-fix the row carried "full coverage: transcript 1.163, feature
    /// 1.724" — both ratios > 1.0, a tell-tale sign of the bug. Post-fix
    /// the ratios must reflect the 5645s denominator.
    @Test("terminalReason ratios use the corrected denominator (5645, not 748)")
    func terminalReasonUsesCorrectedDenominator() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        let assetId = "asset-denom-reason"
        let sessionId = "session-denom-reason"
        let trueDuration: Double = 5645
        let transcriptEnd: Double = 870
        let featureEnd: Double = 1290

        try await store.insertAsset(makeAsset(
            id: assetId,
            episodeDurationSec: trueDuration,
            featureCoverageEndTime: featureEnd,
            fastTranscriptCoverageEndTime: transcriptEnd
        ))
        try await store.insertSession(makeSession(id: sessionId, assetId: assetId))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, chunkIndex: 0, startTime: 0, endTime: transcriptEnd)
        ])

        let staleShards = makeShards(episodeId: "ep-\(assetId)", totalSeconds: 748)

        try await coordinator.finalizeBackfillForTesting(
            sessionId: sessionId,
            assetId: assetId,
            episodeId: "ep-\(assetId)",
            activeShards: staleShards
        )

        let assetAfter = try await store.fetchAsset(id: assetId)
        let reason = assetAfter?.terminalReason ?? ""

        // Sanity check: the reason must mention 5645 (or 5645.0) — the
        // corrected denominator — and must not mention 748, the stale
        // shard sum.
        #expect(
            reason.contains("5645"),
            "terminalReason should report the 5645s denominator. Got: \(reason)"
        )
        #expect(
            !reason.contains("748"),
            "terminalReason must NOT contain the stale 748s denominator. Got: \(reason)"
        )

        // Also confirm the ratio is computed against the corrected
        // denominator: 1290/5645 ≈ 0.228, well below 1.0 — the pre-fix
        // bug printed 1.724 for feature ratio. We just assert it's not
        // an above-1 ratio to keep the check independent of format
        // string changes.
        #expect(
            !reason.contains("1.724"),
            "terminalReason must not carry the pre-fix above-1 feature ratio. Got: \(reason)"
        )
    }

    // MARK: - Healthy regression: shard sum matches persisted

    /// When the shard sum and the persisted duration agree (the common
    /// case — short episodes, healthy feed metadata), the fix must be a
    /// no-op: a fully-covered episode still resolves to `.completeFull`.
    /// `max(x, x) == x`.
    @Test("healthy case (shard sum == episodeDurationSec) resolves to .completeFull")
    func healthyCaseStillCompletesFull() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        let assetId = "asset-healthy"
        let sessionId = "session-healthy"
        let duration: Double = 1800

        try await store.insertAsset(makeAsset(
            id: assetId,
            episodeDurationSec: duration,
            featureCoverageEndTime: duration,
            fastTranscriptCoverageEndTime: duration
        ))
        try await store.insertSession(makeSession(id: sessionId, assetId: assetId))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, chunkIndex: 0, startTime: 0, endTime: duration)
        ])

        let healthyShards = makeShards(episodeId: "ep-\(assetId)", totalSeconds: duration)

        try await coordinator.finalizeBackfillForTesting(
            sessionId: sessionId,
            assetId: assetId,
            episodeId: "ep-\(assetId)",
            activeShards: healthyShards
        )

        let sessionAfter = try await store.fetchSession(id: sessionId)
        #expect(
            sessionAfter?.state == SessionState.completeFull.rawValue,
            "Healthy regression: full coverage on a non-stale denominator must still stamp .completeFull. Got: \(sessionAfter?.state ?? "nil")"
        )

        let assetAfter = try await store.fetchAsset(id: assetId)
        let reason = assetAfter?.terminalReason ?? ""
        #expect(
            reason.contains("transcript 1.000") || reason.contains("transcript 1."),
            "terminalReason should reflect ratio ≈1.0. Got: \(reason)"
        )
    }

    // MARK: - Live shard sum > persisted (rule-1 still wins)

    /// When `activeShards` extends past the persisted duration (e.g. a
    /// streaming decode produced more audio than was recorded on the
    /// row), the live shard sum is still authoritative — `max(x, y)`
    /// just happens to pick `x`. This guards against a regression where
    /// someone "fixes" the bug by always preferring the persisted
    /// value: that would re-introduce a different staleness, where a
    /// row with 0 or a too-small persisted value would shadow the live
    /// truth.
    @Test("live shard sum greater than persisted is still respected")
    func liveShardSumAboveAssetWins() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        let assetId = "asset-live-bigger"
        let sessionId = "session-live-bigger"
        // Persisted is small (e.g. an old feed metadata that hasn't been
        // re-probed yet); live shards have already grown past it.
        let persisted: Double = 1000
        let liveDuration: Double = 3600

        try await store.insertAsset(makeAsset(
            id: assetId,
            episodeDurationSec: persisted,
            featureCoverageEndTime: liveDuration,
            fastTranscriptCoverageEndTime: liveDuration
        ))
        try await store.insertSession(makeSession(id: sessionId, assetId: assetId))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: assetId, chunkIndex: 0, startTime: 0, endTime: liveDuration)
        ])

        let liveShards = makeShards(episodeId: "ep-\(assetId)", totalSeconds: liveDuration)

        try await coordinator.finalizeBackfillForTesting(
            sessionId: sessionId,
            assetId: assetId,
            episodeId: "ep-\(assetId)",
            activeShards: liveShards
        )

        let sessionAfter = try await store.fetchSession(id: sessionId)
        // Coverage matches the live shard sum (3600s) which is the
        // chosen denominator (max(3600, 1000)) — so .completeFull.
        #expect(
            sessionAfter?.state == SessionState.completeFull.rawValue,
            "When live shard sum exceeds persisted, max() must still resolve to live. Got: \(sessionAfter?.state ?? "nil")"
        )
    }
}
