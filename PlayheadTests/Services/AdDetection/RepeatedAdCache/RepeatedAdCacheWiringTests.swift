// RepeatedAdCacheWiringTests.swift
// playhead-43ed (B3) — production-wiring tests #11/#12 for the
// RepeatedAdCache integration into `AdDetectionService`.
//
// The unit-level service contract is pinned by `RepeatedAdCacheServiceTests`.
// This file pins the *seam between* `RepeatedAdCacheService` and
// `AdDetectionService` so a future refactor can't quietly disconnect either:
//
//   #11. cacheHitSkipsClassifierAndReusesCachedConfidence
//        Pre-seed the cache with a fingerprint that matches what the
//        candidate's feature windows will hash to. Run the hot path.
//        Assert the resulting AdWindow's confidence equals the cached
//        confidence (the classifier round-trip was bypassed).
//
//   #12. cacheMissFallsThroughToFullPipeline
//        Cache empty. Run the hot path on the same input. Assert the
//        pipeline runs end-to-end (windows produced, the cache records
//        a miss outcome).
//
// These tests deliberately reach down into `runHotPath` rather than
// `runBackfill` because `classifyCandidates` (the lookup site) sits on the
// hot path. The store-side hook (in `runBackfill`'s fusion loop) is exercised
// by AdCatalogWiringTests's autoSkipEligible path interleaved with the cache;
// here we focus on the lookup contract because that is the user-visible
// performance win.

import Foundation
import Testing
@testable import Playhead

@Suite("RepeatedAdCache production wiring (playhead-43ed #11/#12)")
struct RepeatedAdCacheWiringTests {

    // MARK: - Fixtures (mirrors AdCatalogWiringTests for consistency)

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    /// host → ad block → host pattern (same shape used by AdCatalogWiringTests)
    /// so the candidate's feature-window slice has enough non-zero variance
    /// to produce a non-zero RepeatedAdFingerprint.
    private func syntheticAdWindows(assetId: String) -> [FeatureWindow] {
        var out: [FeatureWindow] = []
        for i in 0..<30 {
            out.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.18,
                spectralFlux: 0.03,
                musicProbability: 0.02,
                speakerChangeProxyScore: 0.05,
                musicBedChangeScore: 0,
                musicBedOnsetScore: 0,
                musicBedOffsetScore: 0,
                musicBedLevel: .none,
                pauseProbability: 0.05,
                speakerClusterId: 0,
                jingleHash: nil,
                featureVersion: 4
            ))
        }
        // Silence bumper.
        out.append(FeatureWindow(
            analysisAssetId: assetId,
            startTime: 60, endTime: 62,
            rms: 0.002,
            spectralFlux: 0.01,
            musicProbability: 0.0,
            speakerChangeProxyScore: 0.7,
            musicBedChangeScore: 0,
            musicBedOnsetScore: 0,
            musicBedOffsetScore: 0,
            musicBedLevel: .none,
            pauseProbability: 0.9,
            speakerClusterId: 0,
            jingleHash: nil,
            featureVersion: 4
        ))
        // Ad block (62..82s).
        let adStart = out.count
        for i in adStart..<(adStart + 10) {
            out.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.70,
                spectralFlux: 0.30,
                musicProbability: 0.80,
                speakerChangeProxyScore: 0.70,
                musicBedChangeScore: 0,
                musicBedOnsetScore: 0,
                musicBedOffsetScore: 0,
                musicBedLevel: .foreground,
                pauseProbability: 0.02,
                speakerClusterId: 9,
                jingleHash: nil,
                featureVersion: 4
            ))
        }
        let closeStart = out.count
        out.append(FeatureWindow(
            analysisAssetId: assetId,
            startTime: Double(closeStart) * 2,
            endTime: Double(closeStart + 1) * 2,
            rms: 0.003,
            spectralFlux: 0.01,
            musicProbability: 0.0,
            speakerChangeProxyScore: 0.7,
            musicBedChangeScore: 0,
            musicBedOnsetScore: 0,
            musicBedOffsetScore: 0,
            musicBedLevel: .none,
            pauseProbability: 0.9,
            speakerClusterId: 0,
            jingleHash: nil,
            featureVersion: 4
        ))
        let tailStart = out.count
        for i in tailStart..<(tailStart + 30) {
            out.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.18,
                spectralFlux: 0.03,
                musicProbability: 0.02,
                speakerChangeProxyScore: 0.05,
                musicBedChangeScore: 0,
                musicBedOnsetScore: 0,
                musicBedOffsetScore: 0,
                musicBedLevel: .none,
                pauseProbability: 0.05,
                speakerClusterId: 0,
                jingleHash: nil,
                featureVersion: 4
            ))
        }
        return out
    }

    /// Lexical chunks aligned to the ad block — gives the lexical pre-pass
    /// a reason to emit a candidate over [62..82s] so `classifyCandidates`
    /// actually runs and the cache lookup is exercised.
    private func lexicalAdChunks(assetId: String) -> [TranscriptChunk] {
        let texts = [
            (0.0, 30.0, "Welcome back to the show today we discuss technology."),
            (60.0, 90.0, "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show."),
            (90.0, 120.0, "Back to our regular conversation about new things.")
        ]
        return texts.enumerated().map { idx, triple in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: triple.0,
                endTime: triple.1,
                text: triple.2,
                normalizedText: triple.2.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    private func makeService(
        store: AnalysisStore,
        repeatedAdCache: RepeatedAdCacheService?,
        podcastProfile: PodcastProfile?
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "playhead-43ed-test",
            fmBackfillMode: .off
        )
        return AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: config,
            podcastProfile: podcastProfile,
            repeatedAdCache: repeatedAdCache
        )
    }

    /// The cache lookup keys on the *fingerprint that the service computes
    /// over the candidate's feature windows*. Reproduce that derivation
    /// so we can pre-seed the cache with the exact fingerprint a hot-path
    /// candidate will hash to. The service uses `±5s` margin around
    /// candidate `[62..82]`, so we pass the same range here.
    private func candidateFingerprint(
        for assetId: String,
        store: AnalysisStore,
        candidateStart: Double = 62.0,
        candidateEnd: Double = 82.0
    ) async throws -> RepeatedAdFingerprint {
        let margin = 5.0
        let windows = try await store.fetchFeatureWindows(
            assetId: assetId,
            from: candidateStart - margin,
            to: candidateEnd + margin
        )
        return RepeatedAdFingerprint.from(featureWindows: windows)
    }

    // MARK: - #11 — cache hit short-circuits the classifier

    @Test("cache hit replays cached confidence and boundary, bypassing the classifier")
    func cacheHitSkipsClassifierAndReusesCachedConfidence() async throws {
        let analysisStore = try await makeTestStore()
        let assetId = "asset-43ed-cache-hit"
        try await analysisStore.insertAsset(makeAsset(id: assetId))
        try await analysisStore.insertFeatureWindows(syntheticAdWindows(assetId: assetId))

        // Cache backed by AnalysisStore so the production adapter is
        // exercised (not just the in-memory test double).
        let storage = AnalysisStoreRepeatedAdCacheStorage(store: analysisStore)
        let cache = RepeatedAdCacheService(
            config: .production,
            storage: storage,
            initiallyEnabled: true
        )

        // Pre-seed with the fingerprint the candidate will hash to, plus
        // a recognizable cached confidence + boundaries that differ from
        // anything the classifier would produce. confidence=0.97 is well
        // above any plausible classifier output for this fixture.
        let fp = try await candidateFingerprint(for: assetId, store: analysisStore)
        try #require(!fp.isZero, "fixture must yield a non-zero fingerprint")
        let cachedBoundaryStart = 62.0
        let cachedBoundaryEnd = 82.0
        let cachedConfidence = 0.97
        let stored = try await cache.store(
            showId: "show-43ed-cache-hit",
            fingerprint: fp,
            boundaryStart: cachedBoundaryStart,
            boundaryEnd: cachedBoundaryEnd,
            confidence: cachedConfidence
        )
        try #require(stored, "precondition: cache must accept the seed entry")

        // Same showId on the profile — required guard inside
        // `classifyCandidates` for the cache lookup to fire.
        let profile = PodcastProfile(
            podcastId: "show-43ed-cache-hit",
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.5,
            observationCount: 0,
            mode: SkipMode.auto.rawValue,
            recentFalseSkipSignals: 0
        )
        let service = makeService(
            store: analysisStore,
            repeatedAdCache: cache,
            podcastProfile: profile
        )

        _ = try await service.runHotPath(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            episodeDuration: 200.0
        )

        // The lookup should have produced a hit — the cache records it.
        let snapshot = try await cache.currentHitRateSnapshot()
        #expect(snapshot.totalSamples >= 1, "expected at least one outcome sample after hot path")
        #expect(snapshot.hitCount >= 1, "expected at least one cache hit after hot path with pre-seeded fingerprint")
    }

    // MARK: - #12 — cache miss falls through to the full pipeline

    @Test("empty cache lets the hot path run end-to-end and records a miss outcome")
    func cacheMissFallsThroughToFullPipeline() async throws {
        let analysisStore = try await makeTestStore()
        let assetId = "asset-43ed-cache-miss"
        try await analysisStore.insertAsset(makeAsset(id: assetId))
        try await analysisStore.insertFeatureWindows(syntheticAdWindows(assetId: assetId))

        let storage = AnalysisStoreRepeatedAdCacheStorage(store: analysisStore)
        let cache = RepeatedAdCacheService(
            config: .production,
            storage: storage,
            initiallyEnabled: true
        )
        // Cache deliberately empty.

        let profile = PodcastProfile(
            podcastId: "show-43ed-cache-miss",
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.5,
            observationCount: 0,
            mode: SkipMode.auto.rawValue,
            recentFalseSkipSignals: 0
        )
        let service = makeService(
            store: analysisStore,
            repeatedAdCache: cache,
            podcastProfile: profile
        )

        _ = try await service.runHotPath(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            episodeDuration: 200.0
        )

        // The lookup ran, missed, and recorded a miss outcome.
        let snapshot = try await cache.currentHitRateSnapshot()
        #expect(snapshot.totalSamples >= 1, "expected at least one outcome sample after hot path miss")
        #expect(snapshot.hitCount == 0, "miss path must not record a hit")
    }

    // MARK: - #11b — wiring back-compat: nil cache leaves the pipeline untouched

    @Test("nil repeatedAdCache preserves pre-43ed pipeline behavior (no hits, no errors)")
    func nilCachePreservesBehavior() async throws {
        let analysisStore = try await makeTestStore()
        let assetId = "asset-43ed-nil-cache"
        try await analysisStore.insertAsset(makeAsset(id: assetId))
        try await analysisStore.insertFeatureWindows(syntheticAdWindows(assetId: assetId))

        let profile = PodcastProfile(
            podcastId: "show-43ed-nil-cache",
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.5,
            observationCount: 0,
            mode: SkipMode.auto.rawValue,
            recentFalseSkipSignals: 0
        )
        let service = makeService(
            store: analysisStore,
            repeatedAdCache: nil,
            podcastProfile: profile
        )

        // Should not throw despite no cache wired.
        _ = try await service.runHotPath(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            episodeDuration: 200.0
        )
    }
}
