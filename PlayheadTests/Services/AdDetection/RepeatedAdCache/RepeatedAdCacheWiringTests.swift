// RepeatedAdCacheWiringTests.swift
// playhead-43ed (B3) — production-wiring tests #11/#12 for the
// RepeatedAdCache integration into `AdDetectionService`.
//
// The unit-level service contract is pinned by `RepeatedAdCacheServiceTests`.
// This file pins the *seam between* `RepeatedAdCacheService` and
// `AdDetectionService` so a future refactor can't quietly disconnect either:
//
//   #11. cacheHitDoesNotReplaceCurrentEpisodeClassifier
//        Pre-seed the cache with a fingerprint that matches what the
//        candidate's feature windows will hash to. Run the hot path.
//        Assert stale source-episode bounds/confidence are never replayed.
//
//   #12. cacheMissFallsThroughToFullPipeline
//        Cache empty. Run the hot path on the same input. Assert the
//        pipeline runs end-to-end (windows produced, the cache records
//        a miss outcome).
//
// These tests deliberately reach down into `runHotPath` because
// `classifyCandidates` (the lookup site) sits there. Authoritative cache
// ingress is exercised with the catalog lifecycle in AdCatalogWiringTests.

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
                featureVersion: 5
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
            featureVersion: 5
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
                featureVersion: 5
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
            featureVersion: 5
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
                featureVersion: 5
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

    // MARK: - #11 — cache hit remains non-authoritative

    @Test("cache hit cannot replay stale source bounds or replace current classifier")
    func cacheHitDoesNotReplaceCurrentEpisodeClassifier() async throws {
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

        // Pre-seed with the fingerprint the candidate will hash to, but use
        // absolute coordinates from a different episode. Replaying them into
        // this 200-second episode would be unmistakably unsafe.
        let fp = try await candidateFingerprint(for: assetId, store: analysisStore)
        try #require(!fp.isZero, "fixture must yield a non-zero fingerprint")
        let cachedBoundaryStart = 3_493.02
        let cachedBoundaryEnd = 3_537.95
        let cachedConfidence = 0.97
        let stored = try await cache.store(
            showId: "show-43ed-cache-hit",
            fingerprint: fp,
            boundaryStart: cachedBoundaryStart,
            boundaryEnd: cachedBoundaryEnd,
            confidence: cachedConfidence,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "asset-43ed-confirmed",
            sourceWindowId: "window-43ed-confirmed"
        )
        try #require(stored, "precondition: cache must accept the seed entry")

        // The request below carries this same show ID. The profile remains
        // present for classifier priors, but is never a cache-key fallback.
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
        struct CurrentEpisodeClassifier: ClassifierService {
            func classify(
                inputs: [ClassifierInput],
                priors: ShowPriors
            ) -> [ClassifierResult] {
                inputs.map { classify(input: $0, priors: priors) }
            }

            func classify(
                input: ClassifierInput,
                priors: ShowPriors
            ) -> ClassifierResult {
                ClassifierResult(
                    candidateId: input.candidate.id,
                    analysisAssetId: input.candidate.analysisAssetId,
                    startTime: input.candidate.startTime,
                    endTime: input.candidate.endTime,
                    adProbability: 0.91,
                    startAdjustment: 0,
                    endAdjustment: 0,
                    signalBreakdown: SignalBreakdown(
                        lexicalScore: 0.91,
                        rmsDropScore: 0,
                        spectralChangeScore: 0,
                        musicScore: 0,
                        speakerChangeScore: 0,
                        priorScore: 0
                    )
                )
            }
        }
        let service = AdDetectionService(
            store: analysisStore,
            classifier: CurrentEpisodeClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90,
                detectorVersion: "playhead-43ed-current-episode-test",
                fmBackfillMode: .off
            ),
            podcastProfile: profile,
            repeatedAdCache: cache
        )

        let windows = try await service.runHotPath(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            episodeDuration: 200.0,
            podcastId: profile.podcastId
        )

        #expect(!windows.isEmpty)
        #expect(windows.allSatisfy { $0.endTime <= 200 })
        #expect(windows.allSatisfy {
            $0.startTime != cachedBoundaryStart
                && $0.endTime != cachedBoundaryEnd
                && $0.confidence != cachedConfidence
        })
        // The lookup still contributes recurrence telemetry after the current
        // classifier independently confirms the candidate.
        let snapshot = try await cache.currentHitRateSnapshot()
        #expect(snapshot.totalSamples >= 1, "expected at least one outcome sample after hot path")
        #expect(snapshot.hitCount >= 1, "expected at least one cache hit after hot path with pre-seeded fingerprint")
    }

    @Test("missing and noncanonical request show IDs never query a stale podcast profile cache")
    func missingRequestShowFailsClosedDespiteProfile() async throws {
        let analysisStore = try await makeTestStore()
        let assetId = "asset-43ed-missing-request-show"
        try await analysisStore.insertAsset(makeAsset(id: assetId))
        try await analysisStore.insertFeatureWindows(
            syntheticAdWindows(assetId: assetId)
        )

        let storage = AnalysisStoreRepeatedAdCacheStorage(
            store: analysisStore
        )
        let cache = RepeatedAdCacheService(
            config: .production,
            storage: storage,
            initiallyEnabled: true
        )
        let fingerprint = try await candidateFingerprint(
            for: assetId,
            store: analysisStore
        )
        try #require(!fingerprint.isZero)
        #expect(try await cache.store(
            showId: "stale-profile-show",
            fingerprint: fingerprint,
            boundaryStart: 62,
            boundaryEnd: 82,
            confidence: 0.97,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "seed-asset",
            sourceWindowId: "seed-window"
        ))

        let staleProfile = PodcastProfile(
            podcastId: "stale-profile-show",
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
            podcastProfile: staleProfile
        )

        for requestShowId: String? in [
            nil,
            " \n ",
            " stale-profile-show ",
        ] {
            _ = try await service.runHotPath(
                chunks: lexicalAdChunks(assetId: assetId),
                analysisAssetId: assetId,
                episodeDuration: 200,
                podcastId: requestShowId
            )
        }

        let snapshot = try await cache.currentHitRateSnapshot()
        #expect(
            snapshot.totalSamples == 0,
            "missing request identity must perform no recurrence lookup, even when the actor still holds a matching profile"
        )
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
            episodeDuration: 200.0,
            podcastId: profile.podcastId
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
            episodeDuration: 200.0,
            podcastId: profile.podcastId
        )
    }

    // MARK: - C2/C4 — auto-disable feedback loop must not fire on miss-only
    // traffic against a fresh, empty cache. Pre-fix, every lexical candidate
    // that didn't hit the cache called `recordOutcome(hit: false)` regardless
    // of whether the classifier ultimately said "this is an ad", so a fresh
    // show with N candidates would saturate the rolling window with N misses
    // and trip the 5% floor permanently. Post-fix, misses are only recorded
    // when the classifier verdict for that candidate clears the same gate
    // that controls `store(...)` (`adProbability >= storeConfidenceThreshold`).
    // A non-ad candidate (a miss against an empty cache + low classifier
    // probability) is noise, not signal.

    @Test("miss-only traffic on a fresh empty cache must not trip auto-disable when classifier rejects below threshold")
    func missOnlyTrafficDoesNotAutoDisableFreshCache() async throws {
        let analysisStore = try await makeTestStore()
        let assetId = "asset-43ed-c2-miss-only"
        try await analysisStore.insertAsset(makeAsset(id: assetId))
        try await analysisStore.insertFeatureWindows(syntheticAdWindows(assetId: assetId))

        // Tiny min-sample threshold so a single deferred miss outcome
        // would be enough to trip auto-disable IF the C2 fix were not in
        // place. Pre-fix: every lexical candidate (most of which the
        // classifier would reject below the storeConfidenceThreshold
        // floor) calls `recordOutcome(false)` regardless of verdict, so
        // the auto-disable guard fires on the very first run. Post-fix:
        // only candidates whose classifier verdict clears the same gate
        // that controls `store(...)` count as miss outcomes.
        let cfg = RepeatedAdCacheConfig(
            storeConfidenceThreshold: 0.85,
            hammingDistanceThreshold: 3,
            perShowCap: 3,
            globalCap: 5,
            entryMaxAge: 90 * 24 * 60 * 60,
            autoDisableWindow: 14 * 24 * 60 * 60,
            autoDisableHitRateFloor: 0.05,
            autoDisableMinSamples: 1
        )
        let storage = AnalysisStoreRepeatedAdCacheStorage(store: analysisStore)
        let cache = RepeatedAdCacheService(
            config: cfg,
            storage: storage,
            initiallyEnabled: true
        )

        let profile = PodcastProfile(
            podcastId: "show-43ed-c2-miss-only",
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

        // Stub classifier that returns probability strictly below the
        // store-confidence floor (0.85). Models the production reality
        // that most lexical candidates are non-ads — the classifier
        // does its job and rejects them. Under the pre-C2 wiring, every
        // such miss recorded `recordOutcome(false)`, saturating the
        // rolling-window metric on the first session. Under C2 the
        // miss outcomes are gated by classifier verdict against the
        // same threshold that controls `store(...)`, so a fresh cache
        // sees zero noise from non-ad traffic.
        struct BelowThresholdStubClassifier: ClassifierService {
            func classify(inputs: [ClassifierInput], priors: ShowPriors) -> [ClassifierResult] {
                inputs.map { classify(input: $0, priors: priors) }
            }
            func classify(input: ClassifierInput, priors: ShowPriors) -> ClassifierResult {
                ClassifierResult(
                    candidateId: input.candidate.id,
                    analysisAssetId: input.candidate.analysisAssetId,
                    startTime: input.candidate.startTime,
                    endTime: input.candidate.endTime,
                    adProbability: 0.50, // below storeConfidenceThreshold (0.85)
                    startAdjustment: 0,
                    endAdjustment: 0,
                    signalBreakdown: SignalBreakdown(
                        lexicalScore: 0,
                        rmsDropScore: 0,
                        spectralChangeScore: 0,
                        musicScore: 0,
                        speakerChangeScore: 0,
                        priorScore: 0
                    )
                )
            }
        }

        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "playhead-43ed-c2-test",
            fmBackfillMode: .off
        )
        let service = AdDetectionService(
            store: analysisStore,
            classifier: BelowThresholdStubClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            podcastProfile: profile,
            repeatedAdCache: cache
        )

        _ = try await service.runHotPath(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            episodeDuration: 200.0,
            podcastId: profile.podcastId
        )

        // The cache must remain enabled — a fresh cache with miss-only
        // traffic against candidates the classifier rejected below the
        // store-confidence floor MUST NOT auto-disable. The deferred
        // miss outcomes are noise, not signal.
        #expect(await cache.isEnabled() == true,
                "fresh cache must not auto-disable from miss-only traffic where the classifier verdict didn't clear the storeConfidenceThreshold gate")
        let reason = await cache.currentDisableReason()
        #expect(reason == nil, "expected no disable reason, got \(String(describing: reason))")

        // No outcome samples should have been recorded at all — the
        // miss path is fully gated by classifier verdict and the
        // classifier rejected every candidate below threshold.
        let snapshot = try await cache.currentHitRateSnapshot()
        #expect(snapshot.totalSamples == 0,
                "expected no recorded outcomes for miss-only traffic below the storeConfidenceThreshold gate; got \(snapshot.totalSamples)")
    }

    // MARK: - C2 — confirmed-ad miss DOES record an outcome
    //
    // Companion to `missOnlyTrafficDoesNotAutoDisableFreshCache`: when
    // the classifier verdict for a cache-miss candidate clears the
    // `storeConfidenceThreshold` gate, the miss outcome IS recorded so
    // the auto-disable guard can act on real signal.

    @Test("classifier-confirmed miss DOES record an outcome (C2 contract symmetry)")
    func classifierConfirmedMissRecordsOutcome() async throws {
        let analysisStore = try await makeTestStore()
        let assetId = "asset-43ed-c2-confirmed-miss"
        try await analysisStore.insertAsset(makeAsset(id: assetId))
        try await analysisStore.insertFeatureWindows(syntheticAdWindows(assetId: assetId))

        let storage = AnalysisStoreRepeatedAdCacheStorage(store: analysisStore)
        let cache = RepeatedAdCacheService(
            config: .production,
            storage: storage,
            initiallyEnabled: true
        )

        let profile = PodcastProfile(
            podcastId: "show-43ed-c2-confirmed-miss",
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

        // Stub that returns ABOVE the storeConfidenceThreshold so the
        // miss path's deferred outcome IS recorded.
        struct AboveThresholdStubClassifier: ClassifierService {
            func classify(inputs: [ClassifierInput], priors: ShowPriors) -> [ClassifierResult] {
                inputs.map { classify(input: $0, priors: priors) }
            }
            func classify(input: ClassifierInput, priors: ShowPriors) -> ClassifierResult {
                ClassifierResult(
                    candidateId: input.candidate.id,
                    analysisAssetId: input.candidate.analysisAssetId,
                    startTime: input.candidate.startTime,
                    endTime: input.candidate.endTime,
                    adProbability: 0.95, // above storeConfidenceThreshold
                    startAdjustment: 0,
                    endAdjustment: 0,
                    signalBreakdown: SignalBreakdown(
                        lexicalScore: 0,
                        rmsDropScore: 0,
                        spectralChangeScore: 0,
                        musicScore: 0,
                        speakerChangeScore: 0,
                        priorScore: 0
                    )
                )
            }
        }

        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "playhead-43ed-c2-confirmed-test",
            fmBackfillMode: .off
        )
        let service = AdDetectionService(
            store: analysisStore,
            classifier: AboveThresholdStubClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            podcastProfile: profile,
            repeatedAdCache: cache
        )

        _ = try await service.runHotPath(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            episodeDuration: 200.0,
            podcastId: profile.podcastId
        )

        // At least one miss outcome should have been recorded — the
        // candidate's classifier verdict cleared the gate.
        let snapshot = try await cache.currentHitRateSnapshot()
        #expect(snapshot.totalSamples >= 1,
                "expected at least one outcome for a classifier-confirmed miss; got \(snapshot.totalSamples)")
        #expect(snapshot.hitCount == 0,
                "miss path must not record a hit; got \(snapshot.hitCount) hits")
    }
}
