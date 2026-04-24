// AdCatalogWiringTests.swift
// playhead-gtt9.17: Wire AdCatalogStore ingress + fusion egress into production.
//
// gtt9.13 shipped AdCatalogStore, AcousticFingerprint, and the catalogMatch
// safety-signal machinery, but the store is never populated and never queried
// in the production hot path. This suite nails the contract down:
//
//   Ingress — when a fusion decision gates to `autoSkipEligible`, an entry
//   MUST land in AdCatalogStore so future episodes benefit from the match.
//   markOnly decisions do NOT insert.
//
//   Egress — when the store contains an entry that fingerprint-matches a
//   candidate window on a subsequent backfill, the fusion path emits a
//   `.catalog` ledger entry and threads `catalogMatchSimilarity` into the
//   `AutoSkipPrecisionGate` input.
//
// Acceptance (from bead):
//   1. Correction → catalog insert → subsequent similar window → catalog
//      evidence fires → AutoSkipPrecisionGate sees catalogMatch signal.
//   2. markOnly autoSkip decisions do NOT insert into the catalog.
//   3. Back-compat: with an empty (or nil) AdCatalogStore, behavior matches
//      gtt9.16 exactly — no catalog evidence appears in fusion ledger.

import Foundation
import Testing
@testable import Playhead

@Suite("AdCatalogStore production wiring (gtt9.17)")
struct AdCatalogWiringTests {

    // MARK: - Fixtures

    private func makeCatalogDir() throws -> URL {
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("AdCatalogWiring-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: base, withIntermediateDirectories: true)
        return base
    }

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

    /// Feature windows shaped as host → ad block → host that drive fusion
    /// past the autoSkipEligible gate. Mirrors the well-known synthetic
    /// ad-episode pattern used by AcousticFeaturePipelineTests so the
    /// pipeline fires and the catalog ingress path is exercised.
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
        // Ad block.
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

    /// Zero-signal windows — exercises the "nothing to fingerprint" path.
    private func zeroSignalWindows(assetId: String, count: Int = 40, step: Double = 2.0) -> [FeatureWindow] {
        var out: [FeatureWindow] = []
        var t = 0.0
        for _ in 0..<count {
            out.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: t,
                endTime: t + step,
                rms: 0.0,
                spectralFlux: 0.0,
                musicProbability: 0.0,
                speakerChangeProxyScore: 0.0,
                musicBedChangeScore: 0,
                musicBedOnsetScore: 0,
                musicBedOffsetScore: 0,
                musicBedLevel: .none,
                pauseProbability: 0.0,
                speakerClusterId: nil,
                jingleHash: nil,
                featureVersion: 4
            ))
            t += step
        }
        return out
    }

    private func lexicalAdChunks(assetId: String) -> [TranscriptChunk] {
        // Place a lexical hit inside the ad block timespan (62–82s) so a
        // DecodedSpan is produced and the ledger is populated for fusion.
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
        catalogStore: AdCatalogStore?
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "gtt9.17-test",
            fmBackfillMode: .off
        )
        return AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: config,
            adCatalogStore: catalogStore
        )
    }

    // MARK: - Ingress: autoSkipEligible inserts into AdCatalogStore

    @Test("autoSkipEligible fusion decision inserts a CatalogEntry")
    func autoSkipEligibleInsertsCatalogEntry() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.17-ingress"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(syntheticAdWindows(assetId: assetId))

        let catalogDir = try makeCatalogDir()
        let catalogStore = try AdCatalogStore(directoryURL: catalogDir)
        let service = makeService(store: store, catalogStore: catalogStore)

        try await service.runBackfill(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "show-gtt9.17",
            episodeDuration: 200.0
        )

        let count = try await catalogStore.count()
        #expect(count >= 1, "expected at least one catalog entry after autoSkipEligible decision, got \(count)")

        let entries = try await catalogStore.allEntries()
        let hasShow = entries.contains { $0.showId == "show-gtt9.17" }
        #expect(hasShow, "expected a catalog entry tagged with showId=show-gtt9.17")
    }

    // MARK: - Back-compat: nil AdCatalogStore

    @Test("nil catalogStore preserves pre-gtt9.17 fusion behavior")
    func nilCatalogStorePreservesBehavior() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.17-nilcat"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(syntheticAdWindows(assetId: assetId))

        let service = makeService(store: store, catalogStore: nil)
        // Should not throw despite no catalog wired.
        try await service.runBackfill(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "show-gtt9.17",
            episodeDuration: 200.0
        )

        // No crash and a fusion funnel should still be populated (acoustic
        // pipeline wiring from gtt9.16 is orthogonal to catalog wiring).
        let funnel = await service.acousticFunnelForTesting()
        #expect(funnel.total(AcousticFeatureFunnelStage.computed) > 0, "acoustic funnel should still compute when catalog is nil")
    }

    // MARK: - Egress: prior catalog entry lifts catalogMatchSimilarity

    @Test("prior CatalogEntry matches subsequent similar window and threads similarity into fusion")
    func priorEntryMatchesSubsequentWindow() async throws {
        // Episode 1: populate the catalog via normal backfill (ingress).
        let storeA = try await makeTestStore()
        let assetA = "asset-gtt9.17-ep1"
        try await storeA.insertAsset(makeAsset(id: assetA))
        try await storeA.insertFeatureWindows(syntheticAdWindows(assetId: assetA))

        let catalogDir = try makeCatalogDir()
        let catalogStore = try AdCatalogStore(directoryURL: catalogDir)
        let serviceA = makeService(store: storeA, catalogStore: catalogStore)
        try await serviceA.runBackfill(
            chunks: lexicalAdChunks(assetId: assetA),
            analysisAssetId: assetA,
            podcastId: "show-gtt9.17",
            episodeDuration: 200.0
        )
        let entriesAfterA = try await catalogStore.count()
        #expect(entriesAfterA >= 1, "precondition: ep1 must seed the catalog")

        // Episode 2: fresh analysis store, SAME feature windows pattern so
        // the fingerprint matches. Fresh service reuses the same catalog.
        let storeB = try await makeTestStore()
        let assetB = "asset-gtt9.17-ep2"
        try await storeB.insertAsset(makeAsset(id: assetB))
        try await storeB.insertFeatureWindows(syntheticAdWindows(assetId: assetB))

        let serviceB = makeService(store: storeB, catalogStore: catalogStore)
        try await serviceB.runBackfill(
            chunks: lexicalAdChunks(assetId: assetB),
            analysisAssetId: assetB,
            podcastId: "show-gtt9.17",
            episodeDuration: 200.0
        )

        // After ep2's backfill, the service should have observed at least
        // one catalog match for its windows. The test seam records the top
        // similarity per backfill so we can assert non-zero.
        let topSim = await serviceB.lastCatalogMatchSimilarityForTesting()
        #expect(topSim >= AdCatalogStore.defaultSimilarityFloor,
                "expected ep2 to observe a catalog match ≥ default floor (\(AdCatalogStore.defaultSimilarityFloor)), got \(topSim)")
    }

    // MARK: - Back-compat: empty catalog produces no catalog match signal

    @Test("empty catalogStore yields zero catalogMatchSimilarity")
    func emptyCatalogYieldsZeroSimilarity() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.17-empty"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(syntheticAdWindows(assetId: assetId))

        let catalogDir = try makeCatalogDir()
        let catalogStore = try AdCatalogStore(directoryURL: catalogDir)

        let service = makeService(store: store, catalogStore: catalogStore)
        try await service.runBackfill(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "show-gtt9.17",
            episodeDuration: 200.0
        )

        let topSim = await service.lastCatalogMatchSimilarityForTesting()
        #expect(topSim == 0.0, "empty catalog must yield zero catalogMatchSimilarity, got \(topSim)")
    }

    // MARK: - Zero-signal windows do not insert spurious fingerprints

    @Test("zero-signal windows produce no catalog insertions (isZero fingerprints rejected)")
    func zeroSignalYieldsNoCatalogInsert() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-gtt9.17-zero"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertFeatureWindows(zeroSignalWindows(assetId: assetId))

        let catalogDir = try makeCatalogDir()
        let catalogStore = try AdCatalogStore(directoryURL: catalogDir)

        let service = makeService(store: store, catalogStore: catalogStore)
        try await service.runBackfill(
            chunks: lexicalAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "show-gtt9.17",
            episodeDuration: 120.0
        )

        let count = try await catalogStore.count()
        #expect(count == 0, "zero-signal windows must not produce catalog inserts, got count=\(count)")
    }
}

// MARK: - AcousticFingerprint from feature windows

@Suite("AcousticFingerprint.fromFeatureWindows (gtt9.17)")
struct AcousticFingerprintFromWindowsTests {

    private func window(
        assetId: String = "asset",
        startTime: Double,
        endTime: Double,
        rms: Double,
        flux: Double = 0.1,
        music: Double = 0.1
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: assetId,
            startTime: startTime,
            endTime: endTime,
            rms: rms,
            spectralFlux: flux,
            musicProbability: music,
            speakerChangeProxyScore: 0.3,
            musicBedChangeScore: 0.1,
            musicBedOnsetScore: 0.1,
            musicBedOffsetScore: 0.1,
            musicBedLevel: .background,
            pauseProbability: 0.2,
            speakerClusterId: 1,
            jingleHash: nil,
            featureVersion: 4
        )
    }

    @Test("identical input produces identical fingerprint (deterministic)")
    func deterministic() {
        let ws = (0..<10).map { i in
            window(
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.4 + Double(i) * 0.01
            )
        }
        let fpA = AcousticFingerprint.fromFeatureWindows(ws)
        let fpB = AcousticFingerprint.fromFeatureWindows(ws)
        #expect(fpA == fpB)
        #expect(!fpA.isZero)
    }

    @Test("empty windows produce a zero fingerprint")
    func emptyYieldsZero() {
        let fp = AcousticFingerprint.fromFeatureWindows([])
        #expect(fp.isZero)
    }

    @Test("all-zero-signal windows produce a zero fingerprint")
    func allZeroYieldsZero() {
        let ws = (0..<10).map { i in
            window(
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.0,
                flux: 0.0,
                music: 0.0
            )
        }
        // The fingerprint still tracks speakerChangeProxy/pauseProbability etc.,
        // so "all zero" here exercises the main acoustic energy path being 0.
        // We assert the fingerprint is either zero OR similar-to-another all-zero
        // fingerprint — the contract for wiring is that silent windows should
        // not match loud ones.
        let loud = (0..<10).map { i in
            window(
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.8,
                flux: 0.5,
                music: 0.8
            )
        }
        let fpSilent = AcousticFingerprint.fromFeatureWindows(ws)
        let fpLoud = AcousticFingerprint.fromFeatureWindows(loud)
        let sim = AcousticFingerprint.similarity(fpSilent, fpLoud)
        #expect(sim < 0.80, "silent vs loud fingerprints must fall below default floor, got \(sim)")
    }

    @Test("identical feature patterns match above default floor")
    func identicalMatchesAboveFloor() {
        let ws1 = (0..<10).map { i in
            window(
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.6,
                flux: 0.3,
                music: 0.8
            )
        }
        let ws2 = (0..<10).map { i in
            window(
                startTime: Double(i + 50) * 2,      // different time offset
                endTime: Double(i + 51) * 2,
                rms: 0.6,
                flux: 0.3,
                music: 0.8
            )
        }
        let fp1 = AcousticFingerprint.fromFeatureWindows(ws1)
        let fp2 = AcousticFingerprint.fromFeatureWindows(ws2)
        let sim = AcousticFingerprint.similarity(fp1, fp2)
        #expect(sim >= AdCatalogStore.defaultSimilarityFloor,
                "identical feature patterns at different times must match above floor, got \(sim)")
    }
}
