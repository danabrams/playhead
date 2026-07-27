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
// fire from the hot path — catalog provenance is silently dropped before
// the gate sees it. A populated catalog must preserve that provenance while
// remaining suggest-only unless current-episode strong evidence independently
// corroborates the candidate.
//
// These tests drive `AdDetectionService.runHotPath` end-to-end with
// an `AdCatalogStore` that already contains a matching fingerprint,
// then assert the persisted `AdWindow.eligibilityGate` remains conservative
// while the complete match provenance survives.

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
                // 5; lower versions cause `fetchFeatureWindows` to silently
                // return [] (its `minimumFeatureVersion` filter), which
                // makes downstream fingerprints zero — see B7 history in
                // AcousticFeaturePipeline tests.
                featureVersion: 5
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

    private func storedCandidate(
        id: String,
        assetId: String,
        start: Double,
        end: Double,
        evidenceText: String,
        eligibilityGate: String? = SkipEligibilityGate.eligible.rawValue,
        catalogMatch: Bool = false,
        descriptiveMetadata: Bool = false,
        evidenceStartTime: Double = 1500,
        startEdgeAnchor: AutoSkipEdgeAnchor = .unanchored,
        endEdgeAnchor: AutoSkipEdgeAnchor = .unanchored
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: 0.95,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "2m2i-test",
            advertiser: descriptiveMetadata ? "Stale advertiser" : nil,
            product: descriptiveMetadata ? "Stale product" : nil,
            adDescription: descriptiveMetadata
                ? "Stale description"
                : nil,
            evidenceText: evidenceText,
            evidenceStartTime: evidenceStartTime,
            metadataSource: descriptiveMetadata ? "old-extractor" : "fusion-v1",
            metadataConfidence: descriptiveMetadata ? 0.88 : 0.95,
            metadataPromptVersion: descriptiveMetadata ? "7" : nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: catalogMatch ? "[\"catalog\"]" : nil,
            eligibilityGate: eligibilityGate,
            catalogStoreMatchSimilarity: catalogMatch ? 0.99 : nil,
            catalogFingerprintVersion: catalogMatch
                ? CatalogFingerprintVersion.currentCatalog.rawValue
                : nil,
            catalogMatchedEntryId: catalogMatch
                ? "11111111-1111-1111-1111-111111111111"
                : nil,
            catalogMatchedShowId: catalogMatch ? "show-2m2i" : nil,
            catalogMatchedLearningSource: catalogMatch
                ? CatalogLearningSource.userMarkedAd.rawValue
                : nil,
            catalogMatchedLearningLifecycle: catalogMatch
                ? CatalogLearningLifecycle.explicitConfirmation.rawValue
                : nil,
            startEdgeAnchor: startEdgeAnchor.rawValue,
            endEdgeAnchor: endEdgeAnchor.rawValue
        )
    }

    /// Pre-seed the catalog with a fingerprint computed from this asset's
    /// own ad-band feature windows for one exact show. Hot-path matching must
    /// carry that show identity; nil/blank identifiers fail closed.
    @discardableResult
    private func seedCatalogFromFeatureBand(
        catalog: AdCatalogStore,
        store: AnalysisStore,
        assetId: String,
        adStart: Double,
        adEnd: Double,
        showId: String = "show-2m2i"
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
            showId: showId,
            episodePosition: .unknown,
            durationSec: adEnd - adStart,
            acousticFingerprint: fingerprint,
            transcriptSnippet: nil,
            sponsorTokens: nil,
            originalConfidence: 0.95,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: assetId,
            sourceWindowId: "seed-\(assetId)",
            sourceStartTime: adStart,
            sourceEndTime: adEnd
        )
        return fingerprint
    }

    // MARK: - 1. Catalog match is provenance, not sole auto-skip authority

    /// The single-window hot path drives a 0.85-confidence chunk through
    /// classifier + precision gate. The chunk text uses ONLY weak
    /// transitionMarker lexical hits (Class C in Bug 8 forensics) so:
    ///   - strongLexicalAdPhrase   does NOT fire
    ///   - sustainedAcousticAdSignature  does NOT fire (musicBedLevel=.none)
    ///   - metadataSlotPrior       does NOT fire (mid-episode)
    ///   - userConfirmedLocalPattern  does NOT fire (no correctionStore)
    ///
    /// The pure precision gate still reports `catalogMatch` for diagnostics,
    /// but the runtime admission policy requires an independent strong signal.
    /// This prevents an erroneous learned row from promoting an otherwise
    /// mixed/unanchored span by itself while retaining full match provenance.
    @Test("hot path single-window: catalog-only evidence remains markOnly while preserving provenance")
    func catalogMatchAloneDoesNotAdmitAutoSkip() async throws {
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
        // markOnly. Adding a populated catalog match must preserve that
        // conservative disposition because catalog evidence is learned data,
        // not an independent confirmation of this episode's boundaries.
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
            episodeDuration: duration,
            podcastId: "show-2m2i"
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        try #require(persisted.count == 1,
                     "exactly one AdWindow expected from the single-window path; got \(persisted.count)")
        #expect(persisted.first?.eligibilityGate == "markOnly",
                "catalog evidence alone must not admit autoSkip; got \(String(describing: persisted.first?.eligibilityGate))")
        #expect(
            persisted.first?.catalogFingerprintVersion
                == CatalogFingerprintVersion.relativeFeatureSummaryV2.rawValue
        )
        #expect(persisted.first?.catalogMatchedEntryId != nil)
        #expect(persisted.first?.catalogMatchedShowId == "show-2m2i")
        #expect(
            persisted.first?.catalogMatchedLearningSource
                == CatalogLearningSource.userMarkedAd.rawValue
        )
        #expect(
            persisted.first?.catalogMatchedLearningLifecycle
                == CatalogLearningLifecycle.explicitConfirmation.rawValue
        )
        #expect(
            (persisted.first?.catalogStoreMatchSimilarity ?? 0)
                >= Double(AdCatalogStore.defaultSimilarityFloor)
        )
    }

    @Test("hot-path same-ID geometry replacement uses fresh gate, catalog provenance, and anchors")
    func changedGeometryDoesNotInheritExistingAuthority() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-2m2i-reconcile-authority"
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

        let normalized = "anyway back to the show without further ado"
        let existing = storedCandidate(
            id: "stable-hot-path-id",
            assetId: assetId,
            start: 1400,
            end: 1600,
            evidenceText: normalized,
            catalogMatch: true,
            descriptiveMetadata: true,
            evidenceStartTime: adStart,
            startEdgeAnchor: .rediffByteExact,
            endEdgeAnchor: .stingerSnapped
        )
        try await store.insertAdWindow(existing)

        let service = makeService(
            store: store,
            classifier: SlotScoringClassifier2m2i(
                scoresByStartTime: [:],
                defaultScore: 0.10,
                chunkScore: 0.85
            ),
            adCatalogStore: nil
        )
        let chunk = TranscriptChunk(
            id: "chunk-2m2i-reconcile-authority",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-2m2i-reconcile-authority",
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

        let emitted = try await service.runHotPath(
            chunks: [chunk],
            analysisAssetId: assetId,
            episodeDuration: duration,
            podcastId: "show-2m2i"
        )
        let replacement = try #require(emitted.first)
        let persisted = try #require(
            try await store.fetchAdWindow(id: existing.id)
        )

        #expect(replacement.id == existing.id)
        #expect(
            replacement.startTime != existing.startTime
                || replacement.endTime != existing.endTime,
            "fixture must exercise a material geometry replacement"
        )
        for window in [replacement, persisted] {
            #expect(window.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)
            #expect(window.evidenceSources == nil)
            #expect(window.catalogStoreMatchSimilarity == nil)
            #expect(window.catalogFingerprintVersion == nil)
            #expect(window.catalogMatchedEntryId == nil)
            #expect(window.catalogMatchedShowId == nil)
            #expect(window.catalogMatchedLearningSource == nil)
            #expect(window.catalogMatchedLearningLifecycle == nil)
            #expect(window.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
            #expect(window.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
            #expect(window.advertiser == nil)
            #expect(window.product == nil)
            #expect(window.adDescription == nil)
            #expect(window.evidenceStartTime == adStart)
            #expect(window.metadataSource == "none")
            #expect(window.metadataConfidence == nil)
            #expect(window.metadataPromptVersion == nil)
        }
    }

    @Test("same-geometry replay cannot retain a stale automatic catalog gate")
    func sameGeometryRefreshesCatalogAuthority() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-2m2i-reconcile-same-geometry"
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
        let service = makeService(
            store: store,
            classifier: SlotScoringClassifier2m2i(
                scoresByStartTime: [:],
                defaultScore: 0.10,
                chunkScore: 0.85
            ),
            adCatalogStore: nil
        )
        let normalized = "anyway back to the show without further ado"
        let chunk = TranscriptChunk(
            id: "chunk-2m2i-reconcile-same-geometry",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-2m2i-reconcile-same-geometry",
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

        let firstRun = try await service.runHotPath(
            chunks: [chunk],
            analysisAssetId: assetId,
            episodeDuration: duration,
            podcastId: "show-2m2i"
        )
        let freshMarkOnly = try #require(firstRun.first)
        #expect(
            freshMarkOnly.eligibilityGate
                == SkipEligibilityGate.markOnly.rawValue
        )

        // Simulate the persisted output of an earlier run whose device-local
        // catalog row supplied the automatic gate, then make that catalog
        // unavailable without changing the detected geometry.
        let staleAutomatic = storedCandidate(
            id: freshMarkOnly.id,
            assetId: assetId,
            start: freshMarkOnly.startTime,
            end: freshMarkOnly.endTime,
            evidenceText: normalized,
            eligibilityGate: "autoSkip",
            catalogMatch: true,
            startEdgeAnchor: .rediffByteExact,
            endEdgeAnchor: .stingerSnapped
        )
        try await store.insertOrReplaceAdWindow(staleAutomatic)

        let secondRun = try await service.runHotPath(
            chunks: [chunk],
            analysisAssetId: assetId,
            episodeDuration: duration,
            podcastId: "show-2m2i"
        )
        let emitted = try #require(secondRun.first)
        let persisted = try #require(
            try await store.fetchAdWindow(id: staleAutomatic.id)
        )

        #expect(emitted.id == staleAutomatic.id)
        #expect(emitted.startTime == staleAutomatic.startTime)
        #expect(emitted.endTime == staleAutomatic.endTime)
        for window in [emitted, persisted] {
            #expect(
                window.eligibilityGate
                    == SkipEligibilityGate.markOnly.rawValue
            )
            #expect(window.evidenceSources == nil)
            #expect(window.catalogStoreMatchSimilarity == nil)
            #expect(window.catalogFingerprintVersion == nil)
            #expect(window.catalogMatchedEntryId == nil)
            #expect(window.catalogMatchedShowId == nil)
            #expect(window.catalogMatchedLearningSource == nil)
            #expect(window.catalogMatchedLearningLifecycle == nil)
            // Geometry stayed exact, so independent physical-edge provenance
            // may survive even though learned decision authority may not.
            #expect(
                window.startEdgeAnchor
                    == AutoSkipEdgeAnchor.rediffByteExact.rawValue
            )
            #expect(
                window.endEdgeAnchor
                    == AutoSkipEdgeAnchor.stingerSnapped.rawValue
            )
        }
    }

    @Test("hot-path geometry treats signed zero as the persisted span")
    func signedZeroGeometryPreservesConservativeAuthorityIdentity() {
        let persisted = storedCandidate(
            id: "signed-zero-persisted",
            assetId: "asset-signed-zero-reconciliation",
            start: 0.0,
            end: 60,
            evidenceText: "sponsor message",
            eligibilityGate:
                SkipEligibilityGate.blockedByUserCorrection.rawValue
        )
        let replayed = storedCandidate(
            id: "signed-zero-replayed",
            assetId: "asset-signed-zero-reconciliation",
            start: -0.0,
            end: 60,
            evidenceText: "sponsor message",
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        let changed = storedCandidate(
            id: "signed-zero-changed",
            assetId: "asset-signed-zero-reconciliation",
            start: -0.0,
            end: 61,
            evidenceText: "sponsor message",
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )

        #expect(
            AdDetectionService.hasSameHotPathGeometry(persisted, replayed),
            "a signed-zero replay must retain the persisted conservative gate"
        )
        #expect(
            !AdDetectionService.hasSameHotPathGeometry(persisted, changed),
            "materially changed bounds must still use fresh authority"
        )
    }

    @Test("hot-path reconcile transaction rejects a stale update or retirement atomically")
    func staleExpectedRevisionRollsBackWholeReconcile() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-2m2i-reconcile-cas"
        try await store.insertAsset(makeAsset(id: assetId))

        let updateOriginal = storedCandidate(
            id: "candidate-to-update",
            assetId: assetId,
            start: 10,
            end: 40,
            evidenceText: "update original"
        )
        let retireOriginal = storedCandidate(
            id: "candidate-to-retire",
            assetId: assetId,
            start: 100,
            end: 130,
            evidenceText: "retire original"
        )
        try await store.insertAdWindows([updateOriginal, retireOriginal])

        let updateReplacement = storedCandidate(
            id: updateOriginal.id,
            assetId: assetId,
            start: 12,
            end: 42,
            evidenceText: "fresh update"
        )
        let retireReplacement = storedCandidate(
            id: retireOriginal.id,
            assetId: assetId,
            start: 200,
            end: 240,
            evidenceText: "same-ID replacement must survive"
        )
        try await store.insertOrReplaceAdWindow(retireReplacement)

        await #expect(
            throws: AnalysisStoreError.staleAdWindowRevision(
                id: retireOriginal.id
            )
        ) {
            try await store.upsertHotPathAdWindows(
                [updateReplacement],
                existingIDs: [updateOriginal.id],
                retiredIDs: [retireOriginal.id],
                expectedProducerRevisions: [
                    updateOriginal.id: updateOriginal,
                    retireOriginal.id: retireOriginal,
                ]
            )
        }

        let updateAfter = try #require(
            try await store.fetchAdWindow(id: updateOriginal.id)
        )
        let retireAfter = try #require(
            try await store.fetchAdWindow(id: retireOriginal.id)
        )
        #expect(updateAfter.startTime == updateOriginal.startTime)
        #expect(updateAfter.endTime == updateOriginal.endTime)
        #expect(updateAfter.evidenceText == updateOriginal.evidenceText)
        #expect(retireAfter.startTime == retireReplacement.startTime)
        #expect(retireAfter.endTime == retireReplacement.endTime)
        #expect(retireAfter.evidenceText == retireReplacement.evidenceText)
    }

    // MARK: - 2. EDGE: catalog match BELOW the floor → behavior unchanged

    /// A populated catalog whose top match for this span sits below
    /// `AutoSkipPrecisionGateConfig.catalogMatchSignalFloor` (0.90) must
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
        // `assetId`. Match similarity will land far below the 0.90 floor.
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
                featureVersion: 5
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
            showId: "show-2m2i",
            episodePosition: .unknown,
            durationSec: 60,
            acousticFingerprint: noiseFingerprint,
            transcriptSnippet: nil,
            sponsorTokens: nil,
            originalConfidence: 0.95,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: fingerprintAsset,
            sourceWindowId: "seed-\(fingerprintAsset)",
            sourceStartTime: 0,
            sourceEndTime: 60
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
            episodeDuration: duration,
            podcastId: "show-2m2i"
        )

        let persisted = try await store.fetchAdWindows(assetId: assetId)
        try #require(persisted.count == 1,
                     "exactly one AdWindow expected from the single-window path; got \(persisted.count)")
        #expect(persisted.first?.eligibilityGate == "markOnly",
                "catalog match below the 0.90 signal floor must NOT promote to autoSkip; got \(String(describing: persisted.first?.eligibilityGate))")
        #expect(
            persisted.first?.catalogStoreMatchSimilarity == 0,
            "a completed exact-show catalog miss must persist 0 rather than masquerading as an unavailable query"
        )
        #expect(persisted.first?.catalogMatchedEntryId == nil)
        #expect(persisted.first?.catalogMatchedShowId == nil)
    }

    @Test("hot path fails closed for missing and noncanonical show identifiers")
    func missingShowIdentityDoesNotMatchCatalog() async throws {
        for (suffix, showId) in [
            ("nil", nil),
            ("blank", " \n\t"),
            ("noncanonical", " show-2m2i "),
        ] as [(String, String?)] {
            let store = try await makeTestStore()
            let assetId = "asset-2m2i-missing-show-\(suffix)"
            try await store.insertAsset(makeAsset(id: assetId))
            let duration: Double = 600
            let adStart: Double = 250
            let adEnd: Double = 310
            try await insertFingerprintableFeatureGrid(
                store: store,
                assetId: assetId,
                duration: duration,
                adStart: adStart,
                adEnd: adEnd
            )
            let catalog = try AdCatalogStore(
                directoryURL: makeTempDir(prefix: "2m2i-\(suffix)")
            )
            try await seedCatalogFromFeatureBand(
                catalog: catalog,
                store: store,
                assetId: assetId,
                adStart: adStart,
                adEnd: adEnd
            )
            let service = makeService(
                store: store,
                classifier: SlotScoringClassifier2m2i(
                    scoresByStartTime: [:],
                    defaultScore: 0.10,
                    chunkScore: 0.85
                ),
                adCatalogStore: catalog
            )
            let normalized = "anyway back to the show without further ado"
            let chunk = TranscriptChunk(
                id: "chunk-\(suffix)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(suffix)",
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
                episodeDuration: duration,
                podcastId: showId
            )
            let persisted = try #require(
                try await store.fetchAdWindows(assetId: assetId).first
            )
            #expect(persisted.eligibilityGate == "markOnly")
            #expect(persisted.catalogStoreMatchSimilarity == nil)
            #expect(persisted.catalogFingerprintVersion == nil)
            #expect(persisted.catalogMatchedEntryId == nil)
            #expect(persisted.catalogMatchedShowId == nil)
            #expect(persisted.catalogMatchedLearningSource == nil)
            #expect(persisted.catalogMatchedLearningLifecycle == nil)
        }
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
            episodeDuration: duration,
            podcastId: "show-2m2i"
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
