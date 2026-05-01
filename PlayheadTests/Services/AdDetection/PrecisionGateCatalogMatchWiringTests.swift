// PrecisionGateCatalogMatchWiringTests.swift
// playhead-2m2i: regression for the catalog-match wiring at the
// `precisionGateLabel` call sites.
//
// Bug
// ---
// `AdDetectionService.precisionGateLabel` builds an
// `AutoSkipPrecisionGateInput` without ever setting
// `catalogMatchSimilarity`. The field defaults to 0, which means
// `SafetySignal.catalogMatch` (wired in playhead-gtt9.13) can never
// fire from the hot path — catalog evidence is silently dropped on the
// floor before the gate sees it. Empty `AdCatalogStore` masks the bug
// today, but a populated catalog with a real fingerprint match should
// admit a borderline (no-other-signal) window to auto-skip.
//
// These tests drive `AdDetectionService.runHotPath` end-to-end with
// an `AdCatalogStore` that already contains a matching fingerprint,
// then assert the persisted `AdWindow.eligibilityGate` reflects
// catalog-corroborated auto-skip vs. mark-only as expected.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-2m2i — precisionGateLabel honors AdCatalogStore matches")
struct PrecisionGateCatalogMatchWiringTests {

    // MARK: - Fixtures

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

    /// Build a feature grid covering [0, duration) where the band
    /// `[adStart, adEnd)` carries a structurally distinct fingerprint
    /// (RMS / spectral-flux / speaker-cluster pattern) so its
    /// `AcousticFingerprint.fromFeatureWindows` slice differs from the
    /// surrounding speech and is recoverable across episodes.
    ///
    /// `musicBedLevel` is held at `.none` everywhere so the
    /// `sustainedAcousticAdSignature` safety signal cannot fire — the
    /// only safety signal we want firing in these tests is
    /// `.catalogMatch`, isolated from acoustic / lexical / slot / user.
    private func insertFingerprintableFeatureGrid(
        store: AnalysisStore,
        assetId: String,
        duration: Double,
        adStart: Double,
        adEnd: Double
    ) async throws {
        var windows: [FeatureWindow] = []
        var t = 0.0
        let step = 2.0
        while t < duration {
            let end = min(t + step, duration)
            let inAd = (t >= adStart && end <= adEnd)
            // Two distinct fingerprint cohorts: speech-like outside the ad
            // band, structurally different inside. AcousticFingerprint
            // hashes feature-vector buckets so a stable per-band pattern
            // is what makes the cross-episode match recoverable.
            let rms: Double      = inAd ? 0.55 : 0.18
            let flux: Double     = inAd ? 0.45 : 0.05
            let cluster: Int     = inAd ? 1    : 0
            windows.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: t,
                endTime: end,
                rms: rms,
                spectralFlux: flux,
                musicProbability: 0.05, // below the music-bed threshold
                musicBedLevel: .none,   // disables sustainedAcousticAdSignature
                pauseProbability: 0.05,
                speakerClusterId: cluster,
                jingleHash: nil,
                // Default `FeatureExtractionConfig.default.featureVersion` is
                // 4; lower versions cause `fetchFeatureWindows` to silently
                // return [] (its `minimumFeatureVersion` filter), which
                // makes downstream fingerprints zero — see B7 history in
                // AcousticFeaturePipeline tests.
                featureVersion: 4
            ))
            t = end
        }
        try await store.insertFeatureWindows(windows)
    }

    private func makeService(
        store: AnalysisStore,
        classifier: ClassifierService,
        adCatalogStore: AdCatalogStore?
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "2m2i-test",
            fmBackfillMode: .off,
            autoSkipConfidenceThreshold: 0.80
        )
        return AdDetectionService(
            store: store,
            classifier: classifier,
            metadataExtractor: FallbackExtractor(),
            config: config,
            adCatalogStore: adCatalogStore
        )
    }

    /// Pre-seed the catalog with a fingerprint computed from this asset's
    /// own ad-band feature windows. Because `AdCatalogStore.matches`
    /// considers `show_id IS NULL` entries globally, we deliberately
    /// store with `showId: nil` — the hot path doesn't have a podcastId
    /// in scope and queries with a nil show, which still hits global
    /// rows. Returns the seeded fingerprint for later assertion.
    @discardableResult
    private func seedCatalogFromFeatureBand(
        catalog: AdCatalogStore,
        store: AnalysisStore,
        assetId: String,
        adStart: Double,
        adEnd: Double
    ) async throws -> AcousticFingerprint {
        let bandFeatures = try await store.fetchFeatureWindows(
            assetId: assetId,
            from: adStart,
            to: adEnd
        )
        let fingerprint = AcousticFingerprint.fromFeatureWindows(bandFeatures)
        #expect(!fingerprint.isZero,
                "precondition: ad-band features must produce a non-zero fingerprint or this test cannot prove anything")
        _ = try await catalog.insert(
            showId: nil,
            episodePosition: .unknown,
            durationSec: adEnd - adStart,
            acousticFingerprint: fingerprint,
            transcriptSnippet: nil,
            sponsorTokens: nil,
            originalConfidence: 0.95
        )
        return fingerprint
    }

    // MARK: - 1. RED → GREEN: catalog match admits borderline window to autoSkip

    /// The single-window hot path drives a 0.85-confidence chunk through
    /// classifier + precision gate. The chunk text uses ONLY weak
    /// transitionMarker lexical hits (Class C in Bug 8 forensics) so:
    ///   - strongLexicalAdPhrase   does NOT fire
    ///   - sustainedAcousticAdSignature  does NOT fire (musicBedLevel=.none)
    ///   - metadataSlotPrior       does NOT fire (mid-episode)
    ///   - userConfirmedLocalPattern  does NOT fire (no correctionStore)
    ///
    /// Without the catalog signal this window demotes to `markOnly`
    /// (which is precisely what `Bug8MarkOnlyForensicTests.class C`
    /// pins). The fix wires a non-zero `catalogMatchSimilarity` into
    /// the gate input so `SafetySignal.catalogMatch` fires and admits
    /// auto-skip. WITHOUT the fix, this test FAILS because
    /// `precisionGateLabel` drops the catalog field on the floor.
    @Test("hot path single-window: pre-seeded catalog fingerprint admits a no-other-signal 0.85 window to eligibilityGate=autoSkip")
    func catalogMatchAdmitsNoOtherSignalWindowToAutoSkip() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-2m2i-catalog-admits"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 3600
        let adStart: Double = 1500
        let adEnd: Double = 1560

        try await insertFingerprintableFeatureGrid(
            store: store,
            assetId: assetId,
            duration: duration,
            adStart: adStart,
            adEnd: adEnd
        )

        let catalogDir = try makeTempDir(prefix: "2m2i-catalog-admits")
        let catalog = try AdCatalogStore(directoryURL: catalogDir)
        try await seedCatalogFromFeatureBand(
            catalog: catalog,
            store: store,
            assetId: assetId,
            adStart: adStart,
            adEnd: adEnd
        )

        let classifier = SlotScoringClassifier2m2i(
            scoresByStartTime: [:],
            defaultScore: 0.10,
            chunkScore: 0.85
        )
        let service = makeService(
            store: store,
            classifier: classifier,
            adCatalogStore: catalog
        )

        // Mid-episode chunk with only transitionMarker hits. Mirrors the
        // `singleHighConfidenceWindowWithoutSafetySignalsStaysMarkOnly`
        // test's lexical setup, which on an EMPTY catalog produces
        // markOnly. Adding a populated catalog match must flip the
        // outcome to autoSkip — that is the wiring the bead enables.
        let normalized = "anyway back to the show without further ado"
        let chunk = TranscriptChunk(
            id: "chunk-2m2i-admit",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-2m2i-admit",
            chunkIndex: 0,
            startTime: adStart,
            endTime: adEnd,
            text: normalized,
            normalizedText: normalized,
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )

        _ = try await service.runHotPath(
            chunks: [chunk],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        try #require(persisted.count == 1,
                     "exactly one AdWindow expected from the single-window path; got \(persisted.count)")
        #expect(persisted.first?.eligibilityGate == "autoSkip",
                "with catalog match and otherwise-no signals, the precision gate must admit autoSkip; got \(String(describing: persisted.first?.eligibilityGate))")
    }

    // MARK: - 2. EDGE: catalog match BELOW the floor → behavior unchanged

    /// A populated catalog whose top match for this span sits below
    /// `AutoSkipPrecisionGateConfig.catalogMatchSignalFloor` (0.80) must
    /// NOT fire `SafetySignal.catalogMatch` — the gate's signal floor is
    /// the precision rail, and a sub-floor match is no admission ticket.
    /// The window stays at markOnly (the same outcome as no catalog at
    /// all). We construct this by giving the asset's ad-band a
    /// fingerprint that is structurally similar to the catalog entry but
    /// not identical, by inserting a fingerprint computed from a
    /// DIFFERENT asset's nearby-but-not-equal feature pattern.
    ///
    /// In practice the cleanest deterministic way to get a sub-floor
    /// match is to seed the catalog with a fingerprint that does NOT
    /// match this asset at all (similarity ≈ 0). That covers the same
    /// invariant the bead names: a populated catalog whose best match
    /// is below the floor must not flip the gate.
    @Test("hot path single-window: catalog match below the signal floor leaves the window at markOnly")
    func catalogMatchBelowSignalFloorStaysMarkOnly() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-2m2i-below-floor"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 3600
        let adStart: Double = 1500
        let adEnd: Double = 1560

        try await insertFingerprintableFeatureGrid(
            store: store,
            assetId: assetId,
            duration: duration,
            adStart: adStart,
            adEnd: adEnd
        )

        // Seed the catalog with a fingerprint from a STRUCTURALLY
        // DIFFERENT asset — a uniform low-RMS speech grid produces a
        // distinct fingerprint vs. the ad-band cluster shape used by
        // `assetId`. Match similarity will land far below the 0.80 floor.
        let fingerprintAsset = "asset-2m2i-below-floor-noise"
        try await store.insertAsset(makeAsset(id: fingerprintAsset))
        var noiseWindows: [FeatureWindow] = []
        for i in 0..<30 {
            noiseWindows.append(FeatureWindow(
                analysisAssetId: fingerprintAsset,
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.10,                 // distinct from the 0.55 ad band
                spectralFlux: 0.01,        // distinct from the 0.45 ad band
                musicProbability: 0.01,
                musicBedLevel: .none,
                pauseProbability: 0.5,
                speakerClusterId: 0,       // distinct from cluster=1
                jingleHash: nil,
                featureVersion: 4
            ))
        }
        try await store.insertFeatureWindows(noiseWindows)
        let noiseFeatures = try await store.fetchFeatureWindows(
            assetId: fingerprintAsset,
            from: 0,
            to: 60
        )
        let noiseFingerprint = AcousticFingerprint.fromFeatureWindows(noiseFeatures)
        try #require(!noiseFingerprint.isZero,
                     "precondition: a non-zero noise fingerprint is required to populate the catalog")

        let catalogDir = try makeTempDir(prefix: "2m2i-catalog-below-floor")
        let catalog = try AdCatalogStore(directoryURL: catalogDir)
        _ = try await catalog.insert(
            showId: nil,
            episodePosition: .unknown,
            durationSec: 60,
            acousticFingerprint: noiseFingerprint,
            transcriptSnippet: nil,
            sponsorTokens: nil,
            originalConfidence: 0.95
        )

        let classifier = SlotScoringClassifier2m2i(
            scoresByStartTime: [:],
            defaultScore: 0.10,
            chunkScore: 0.85
        )
        let service = makeService(
            store: store,
            classifier: classifier,
            adCatalogStore: catalog
        )

        let normalized = "anyway back to the show without further ado"
        let chunk = TranscriptChunk(
            id: "chunk-2m2i-below-floor",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-2m2i-below-floor",
            chunkIndex: 0,
            startTime: adStart,
            endTime: adEnd,
            text: normalized,
            normalizedText: normalized,
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )

        _ = try await service.runHotPath(
            chunks: [chunk],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        try #require(persisted.count == 1,
                     "exactly one AdWindow expected from the single-window path; got \(persisted.count)")
        #expect(persisted.first?.eligibilityGate == "markOnly",
                "catalog match below the 0.80 signal floor must NOT promote to autoSkip; got \(String(describing: persisted.first?.eligibilityGate))")
    }

    // MARK: - 3. EDGE: nil AdCatalogStore → behavior unchanged (today's path)

    /// Without an `AdCatalogStore`, the precision gate must behave
    /// identically to the pre-bead world: borderline single-window with
    /// only transitionMarker lexical hits → markOnly. This locks in the
    /// "empty catalog masks the bug" property the bead writeup names —
    /// after the fix lands, that property must continue to hold so the
    /// fix never accidentally skips on speculation.
    @Test("hot path single-window: nil AdCatalogStore preserves pre-bead markOnly outcome (no regression)")
    func nilCatalogStorePreservesMarkOnly() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-2m2i-nil-catalog"
        try await store.insertAsset(makeAsset(id: assetId))
        let duration: Double = 3600
        let adStart: Double = 1500
        let adEnd: Double = 1560
        try await insertFingerprintableFeatureGrid(
            store: store,
            assetId: assetId,
            duration: duration,
            adStart: adStart,
            adEnd: adEnd
        )

        let classifier = SlotScoringClassifier2m2i(
            scoresByStartTime: [:],
            defaultScore: 0.10,
            chunkScore: 0.85
        )
        let service = makeService(
            store: store,
            classifier: classifier,
            adCatalogStore: nil   // ← the only difference vs. test 1
        )

        let normalized = "anyway back to the show without further ado"
        let chunk = TranscriptChunk(
            id: "chunk-2m2i-nil-catalog",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-2m2i-nil-catalog",
            chunkIndex: 0,
            startTime: adStart,
            endTime: adEnd,
            text: normalized,
            normalizedText: normalized,
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )

        _ = try await service.runHotPath(
            chunks: [chunk],
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        try #require(persisted.count == 1,
                     "exactly one AdWindow expected from the single-window path; got \(persisted.count)")
        #expect(persisted.first?.eligibilityGate == "markOnly",
                "nil catalog must reproduce the pre-bead outcome (markOnly); got \(String(describing: persisted.first?.eligibilityGate))")
    }
}

// MARK: - Test doubles
//
// Local copy of the AutoSkipPrecisionGateIntegrationTests classifier
// double — kept private to this test file so the two test suites can
// evolve independently. The shape mirrors the original: a per-start-
// time score map for Tier 1 slots plus an optional `chunkScore`
// override that the single-window classification path uses for chunks
// (input.candidate.id NOT prefixed with "tier1-").
private final class SlotScoringClassifier2m2i: @unchecked Sendable, ClassifierService {
    private let scoresByStartTime: [Double: Double]
    private let defaultScore: Double
    private let chunkScore: Double?

    init(
        scoresByStartTime: [Double: Double],
        defaultScore: Double,
        chunkScore: Double? = nil
    ) {
        self.scoresByStartTime = scoresByStartTime
        self.defaultScore = defaultScore
        self.chunkScore = chunkScore
    }

    func classify(inputs: [ClassifierInput], priors: ShowPriors) -> [ClassifierResult] {
        inputs.map { classify(input: $0, priors: priors) }
    }

    func classify(input: ClassifierInput, priors: ShowPriors) -> ClassifierResult {
        let probability: Double
        if let chunkScore, !input.candidate.id.hasPrefix("tier1-") {
            probability = chunkScore
        } else {
            probability = scoresByStartTime[input.candidate.startTime] ?? defaultScore
        }
        return ClassifierResult(
            candidateId: input.candidate.id,
            analysisAssetId: input.candidate.analysisAssetId,
            startTime: input.candidate.startTime,
            endTime: input.candidate.endTime,
            adProbability: probability,
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
