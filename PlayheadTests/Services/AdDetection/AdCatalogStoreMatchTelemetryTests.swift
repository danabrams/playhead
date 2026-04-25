// AdCatalogStoreMatchTelemetryTests.swift
// playhead-epfk: surface AdCatalogStore match telemetry in corpus export
// so NARL can measure the correction loop.
//
// The bead exists because the schema collapsed two distinct producers
// under the single `.catalog` evidence-source label:
//   - `EvidenceCatalogBuilder` extracts sponsor tokens / promo codes /
//     URLs from the *current* episode's transcript (in-pipeline).
//   - `AdCatalogStore` matches an acoustic fingerprint against the
//     cross-episode SQLite store accumulated from prior auto-skips
//     and user corrections (correction-loop signal).
//
// Both emit `EvidenceLedgerEntry(source: .catalog, ...)` and there is
// no way to disentangle them downstream — until we stamp a
// `subSource` field at the call site and persist the per-window
// `catalogStoreMatchSimilarity` on `AdWindow` so the corpus export
// can carry the signal NARL needs to close the loop.
//
// These tests pin down the four invariants of the bead:
//   1. Schema fix: `EvidenceCatalogBuilder`'s entry stamps
//      `subSource = .transcriptCatalog`; the `AdCatalogStore` match
//      stamps `subSource = .fingerprintStore`.
//   2. Backwards compat: a v1 `FrozenEvidenceEntry` JSON without the
//      `subSource` key decodes as `nil`, and a v1 `DecisionLogEntry.LedgerEntry`
//      log line without the key likewise decodes as `nil`.
//   3. Corpus export emits the field: `corpus-export.jsonl` carries an
//      `ad_window` record per persisted `AdWindow` with
//      `catalogStoreMatchSimilarity` populated (or explicit JSON null).
//   4. Replay regression: the value `lastCatalogMatchSimilarityForTesting()`
//      reports for a backfill matches the value persisted onto the
//      `AdWindow.catalogStoreMatchSimilarity` column for the matching
//      span (i.e. the test seam and the persistence path agree).

#if DEBUG

import Foundation
import Testing
@testable import Playhead

@Suite("AdCatalogStore match telemetry (playhead-epfk)")
struct AdCatalogStoreMatchTelemetryTests {

    // MARK: - 1. Schema fix unit test

    /// `EvidenceCatalogBuilder`'s in-pipeline transcript catalog entries
    /// must stamp `subSource = .transcriptCatalog`. This is the half of
    /// the schema fix that runs every backfill, regardless of whether
    /// `AdCatalogStore` is wired.
    @Test("buildCatalogLedgerEntries stamps subSource=.transcriptCatalog on the in-pipeline entry")
    func transcriptCatalogStampsSubSource() async throws {
        let store = try await makeTestStore()
        let svc = AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "epfk-test",
                fmBackfillMode: .off
            )
        )

        // A span overlapping at least one EvidenceEntry → at least one
        // ledger entry → the subSource stamp must be present.
        let span = DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-epfk-1", firstAtomOrdinal: 0, lastAtomOrdinal: 10),
            assetId: "asset-epfk-1",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            startTime: 60.0,
            endTime: 90.0,
            anchorProvenance: []
        )
        let entry = EvidenceEntry(
            evidenceRef: 1,
            category: .promoCode,
            matchedText: "SHOW10",
            normalizedText: "show10",
            atomOrdinal: 5,
            startTime: 65.0,
            endTime: 67.0,
            count: 1,
            firstTime: 65.0,
            lastTime: 67.0
        )

        // `buildCatalogLedgerEntries` is actor-isolated (AdDetectionService is
        // an actor) — hop onto the actor before calling.
        let ledger = await svc.buildCatalogLedgerEntries(
            span: span,
            entries: [entry],
            fusionConfig: FusionWeightConfig()
        )
        try #require(ledger.count == 1, "expected exactly one transcript catalog ledger entry, got \(ledger.count)")
        let only = ledger[0]
        #expect(only.source == .catalog)
        #expect(only.subSource == .transcriptCatalog,
                "transcript-derived catalog entries must carry subSource=.transcriptCatalog so NARL can disentangle them from AdCatalogStore matches; got \(String(describing: only.subSource))")
    }

    /// The cross-episode `AdCatalogStore` match path must stamp
    /// `subSource = .fingerprintStore`. The fastest way to assert this
    /// without standing up a full backfill is to verify that
    /// `BackfillEvidenceFusion.buildLedger()` preserves the subSource
    /// stamp through the cap re-stamp — without that preservation, even
    /// a correctly-stamped call site would lose the label.
    @Test("BackfillEvidenceFusion.buildLedger preserves subSource through cap re-stamp")
    func fusionPreservesSubSourceThroughCap() {
        // Synthetic span at 10..40s.
        let span = DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-cap", firstAtomOrdinal: 0, lastAtomOrdinal: 10),
            assetId: "asset-cap",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            startTime: 10.0,
            endTime: 40.0,
            anchorProvenance: []
        )
        let config = FusionWeightConfig()
        // Weight intentionally above catalogCap so the cap re-stamp path
        // executes and we exercise the preservation branch.
        let fingerprintEntry = EvidenceLedgerEntry(
            source: .catalog,
            weight: config.catalogCap * 10.0,
            detail: .catalog(entryCount: 1),
            subSource: .fingerprintStore
        )
        let transcriptEntry = EvidenceLedgerEntry(
            source: .catalog,
            weight: 0.05,
            detail: .catalog(entryCount: 2),
            subSource: .transcriptCatalog
        )
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [fingerprintEntry, transcriptEntry],
            metadataEntries: [],
            mode: .off,
            config: config
        )
        let ledger = fusion.buildLedger()
        let catalogEntries = ledger.filter { $0.source == .catalog }
        try? #require(catalogEntries.count == 2)
        let subSources = Set(catalogEntries.map { $0.subSource })
        #expect(subSources.contains(.fingerprintStore),
                "fingerprintStore subSource must survive cap re-stamp; if absent, the schema collision is back")
        #expect(subSources.contains(.transcriptCatalog),
                "transcriptCatalog subSource must survive cap re-stamp; if absent, the schema collision is back")

        // The fingerprint-store entry's weight must in fact be capped
        // (not the original 10x cap), proving the preservation runs
        // *through* the cap branch rather than around it.
        let fpRow = catalogEntries.first { $0.subSource == .fingerprintStore }
        #expect(fpRow?.weight == config.catalogCap,
                "cap re-stamp must clip weight to catalogCap (\(config.catalogCap)), got \(String(describing: fpRow?.weight))")
    }

    // MARK: - 2. Backwards compat: v1 fixture → subSource = nil

    /// A `FrozenEvidenceEntry` JSON line that pre-dates playhead-epfk
    /// has no `subSource` key. It must decode cleanly with
    /// `subSource = nil` so all the v1 fixtures in
    /// `PlayheadTests/Fixtures/NarlEval/2026-04-22/...` keep loading.
    @Test("FrozenEvidenceEntry decodes pre-epfk fixture (no subSource key) with subSource=nil")
    func frozenEvidenceEntryV1BackCompat() throws {
        // Shape lifted verbatim from the 2026-04-22 Conan fixture's
        // evidenceCatalog[0] entry (see the Python audit at the top of
        // this file's header). No `subSource` key → must decode as nil.
        let v1JSON = """
        {
          "source": "metadata",
          "weight": 0.7,
          "windowStart": 120.0,
          "windowEnd": 180.0,
          "classificationTrust": 0.2
        }
        """.data(using: .utf8)!

        let entry = try JSONDecoder().decode(FrozenTrace.FrozenEvidenceEntry.self, from: v1JSON)
        #expect(entry.source == "metadata")
        #expect(entry.weight == 0.7)
        #expect(entry.windowStart == 120.0)
        #expect(entry.windowEnd == 180.0)
        #expect(entry.classificationTrust == 0.2)
        #expect(entry.subSource == nil,
                "absent subSource key must decode as nil so v1 fixtures keep loading; got \(String(describing: entry.subSource))")

        // And on round-trip, the absent-subSource entry must NOT emit a
        // `subSource` key (encodeIfPresent contract) — otherwise byte-equal
        // round-trip on v1 fixtures would diverge.
        let reEncoded = try JSONEncoder().encode(entry)
        let reEncodedString = String(data: reEncoded, encoding: .utf8) ?? ""
        #expect(!reEncodedString.contains("subSource"),
                "FrozenEvidenceEntry must omit the subSource key when nil so v1 fixtures stay byte-stable on round-trip; got \(reEncodedString)")
    }

    /// A `DecisionLogEntry.LedgerEntry` row that pre-dates playhead-epfk
    /// has no `subSource` key. Replay tooling reading old
    /// `decision-log.jsonl` files must continue to decode them.
    @Test("DecisionLogEntry.LedgerEntry decodes pre-epfk log line (no subSource key) with subSource=nil")
    func decisionLogLedgerEntryV1BackCompat() throws {
        // Pre-epfk schema: source/weight/classificationTrust/detail.
        let v1JSON = """
        {
          "source": "catalog",
          "weight": 0.20,
          "classificationTrust": 1.0,
          "detail": {
            "kind": "catalog",
            "entryCount": 3
          }
        }
        """.data(using: .utf8)!

        let entry = try JSONDecoder().decode(DecisionLogEntry.LedgerEntry.self, from: v1JSON)
        #expect(entry.source == "catalog")
        #expect(entry.weight == 0.20)
        #expect(entry.classificationTrust == 1.0)
        #expect(entry.detail.kind == "catalog")
        #expect(entry.detail.entryCount == 3)
        #expect(entry.subSource == nil,
                "absent subSource key must decode as nil for pre-epfk decision-log lines; got \(String(describing: entry.subSource))")
    }

    // MARK: - 3. Corpus export emits the field

    /// End-to-end: persist an `AdWindow` carrying a known
    /// `catalogStoreMatchSimilarity`, run `CorpusExporter.export`, and
    /// confirm the emitted JSONL has one `ad_window` record with the
    /// matching value. The export contract is the only public surface
    /// NARL eval consumes — it must carry the field.
    @Test("CorpusExporter emits ad_window record with catalogStoreMatchSimilarity populated")
    func corpusExportEmitsCatalogStoreMatchSimilarity() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "epfk-corpus-export")

        let asset = makeTestAsset(id: "asset-epfk-export")
        try await store.insertAsset(asset)

        // Window with a populated catalog-store similarity.
        let matched = AdWindow(
            id: "win-matched",
            analysisAssetId: asset.id,
            startTime: 60.0,
            endTime: 90.0,
            confidence: 0.92,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "epfk-test",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: 60.0,
            metadataSource: "fusion-v1",
            metadataConfidence: 0.85,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            catalogStoreMatchSimilarity: 0.91
        )
        // Window with NIL similarity (catalog store wasn't wired).
        let unmatched = AdWindow(
            id: "win-nil",
            analysisAssetId: asset.id,
            startTime: 200.0,
            endTime: 240.0,
            confidence: 0.55,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "epfk-test",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: 200.0,
            metadataSource: "fusion-v1",
            metadataConfidence: 0.42,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            catalogStoreMatchSimilarity: nil
        )
        try await store.insertAdWindows([matched, unmatched])

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)
        #expect(result.adWindowCount == 2,
                "both ad windows must serialize; got \(result.adWindowCount)")

        let lines = try String(contentsOf: result.fileURL, encoding: .utf8)
            .split(whereSeparator: { $0.isNewline })
            .map(String.init)
            .filter { !$0.isEmpty }
        let adWindowJSONs: [[String: Any]] = try lines
            .compactMap { line -> [String: Any]? in
                guard let data = line.data(using: .utf8),
                      let obj = try JSONSerialization.jsonObject(with: data) as? [String: Any],
                      (obj["type"] as? String) == "ad_window" else { return nil }
                return obj
            }
        try #require(adWindowJSONs.count == 2,
                     "expected 2 ad_window JSONL records, got \(adWindowJSONs.count)")

        // Locate the matched row and assert similarity.
        guard let matchedJSON = adWindowJSONs.first(where: { ($0["id"] as? String) == "win-matched" }) else {
            Issue.record("matched ad_window row missing from export")
            return
        }
        #expect(matchedJSON["analysisAssetId"] as? String == asset.id)
        #expect(matchedJSON["catalogStoreMatchSimilarity"] as? Double == 0.91,
                "matched window must carry catalogStoreMatchSimilarity=0.91; got \(String(describing: matchedJSON["catalogStoreMatchSimilarity"]))")

        // Locate the unmatched row and assert NULL.
        guard let unmatchedJSON = adWindowJSONs.first(where: { ($0["id"] as? String) == "win-nil" }) else {
            Issue.record("unmatched ad_window row missing from export")
            return
        }
        #expect(unmatchedJSON.keys.contains("catalogStoreMatchSimilarity"),
                "the catalogStoreMatchSimilarity key must be present even when nil so eval can distinguish 'not wired' from 'omitted by old schema'")
        #expect(unmatchedJSON["catalogStoreMatchSimilarity"] is NSNull,
                "nil catalogStoreMatchSimilarity must serialize as JSON null; got \(String(describing: unmatchedJSON["catalogStoreMatchSimilarity"]))")
    }

    // MARK: - 4. Replay regression

    /// The test-seam value `lastCatalogMatchSimilarityForTesting()`
    /// must equal the value persisted onto the matching `AdWindow`'s
    /// `catalogStoreMatchSimilarity` column. If the two diverge,
    /// the corpus export and the in-pipeline gate would be measuring
    /// different things and NARL eval would mis-attribute the loop.
    @Test("lastCatalogMatchSimilarityForTesting equals AdWindow.catalogStoreMatchSimilarity for the matching span")
    func replayRegressionSeamEqualsPersistence() async throws {
        // Episode 1: seed the catalog (ingress).
        let storeA = try await makeTestStore()
        let assetA = "asset-epfk-replay-ep1"
        try await storeA.insertAsset(makeAsset(id: assetA))
        try await storeA.insertFeatureWindows(syntheticAdWindows(assetId: assetA))

        let catalogDir = try makeTempDir(prefix: "epfk-catalog")
        let catalogStore = try AdCatalogStore(directoryURL: catalogDir)
        let serviceA = makeService(store: storeA, catalogStore: catalogStore)
        try await serviceA.runBackfill(
            chunks: lexicalAdChunks(assetId: assetA),
            analysisAssetId: assetA,
            podcastId: "show-epfk",
            episodeDuration: 200.0
        )
        try #require(try await catalogStore.count() >= 1,
                     "precondition: ep1 must seed the catalog so ep2 can match")

        // Episode 2: same fingerprint pattern → cross-episode match.
        let storeB = try await makeTestStore()
        let assetB = "asset-epfk-replay-ep2"
        try await storeB.insertAsset(makeAsset(id: assetB))
        try await storeB.insertFeatureWindows(syntheticAdWindows(assetId: assetB))

        let serviceB = makeService(store: storeB, catalogStore: catalogStore)
        try await serviceB.runBackfill(
            chunks: lexicalAdChunks(assetId: assetB),
            analysisAssetId: assetB,
            podcastId: "show-epfk",
            episodeDuration: 200.0
        )

        let seamSimilarity = await serviceB.lastCatalogMatchSimilarityForTesting()
        #expect(seamSimilarity >= AdCatalogStore.defaultSimilarityFloor,
                "precondition: seam must observe a match ≥ floor (\(AdCatalogStore.defaultSimilarityFloor)); got \(seamSimilarity). Without a real match this regression test cannot fire.")

        let persistedWindows = try await storeB.fetchAdWindows(assetId: assetB)
        try #require(!persistedWindows.isEmpty,
                     "ep2 backfill must have persisted at least one AdWindow for the regression check")

        // The matching window's persisted similarity must equal the seam.
        // Use `max` because the seam reports the per-backfill top match,
        // and that's exactly what a downstream NARL eval would compare.
        let persistedTop: Double = persistedWindows
            .compactMap { $0.catalogStoreMatchSimilarity }
            .max() ?? -1
        #expect(persistedTop >= 0.0,
                "at least one persisted AdWindow must carry catalogStoreMatchSimilarity (not nil) for the regression check to be meaningful")
        // Allow tiny float-precision drift between the Float seam and
        // the Double persistence column; both must round to the same
        // value at 5 decimal places, which is far tighter than NARL's
        // 0.80 floor / 0.20 cap headroom.
        let seamAsDouble = Double(seamSimilarity)
        let drift = abs(seamAsDouble - persistedTop)
        #expect(drift < 1e-5,
                "the test seam (\(seamAsDouble)) and the persisted AdWindow.catalogStoreMatchSimilarity (\(persistedTop)) must agree within 1e-5; drift=\(drift). If they diverge, the corpus export is measuring something different from what the in-pipeline gate sees.")
    }

    // MARK: - Test scaffolding (parallels AdCatalogWiringTests)

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

    /// Mirror of `AdCatalogWiringTests.syntheticAdWindows` so the
    /// fingerprint pattern that drives the cross-episode match is
    /// identical. If the pattern diverges, the regression test loses
    /// its precondition.
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
        let adStart = out.count
        for i in adStart..<(adStart + 10) {
            out.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: 62 + Double(i - adStart) * 2,
                endTime: 62 + Double(i - adStart + 1) * 2,
                rms: 0.55,
                spectralFlux: 0.45,
                musicProbability: 0.85,
                speakerChangeProxyScore: 0.10,
                musicBedChangeScore: 0.4,
                musicBedOnsetScore: 0.3,
                musicBedOffsetScore: 0.3,
                musicBedLevel: .foreground,
                pauseProbability: 0.05,
                speakerClusterId: 1,
                jingleHash: nil,
                featureVersion: 4
            ))
        }
        out.append(FeatureWindow(
            analysisAssetId: assetId,
            startTime: 82, endTime: 84,
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
        for i in 0..<30 {
            let t = 84 + Double(i) * 2
            out.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: t, endTime: t + 2,
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
        catalogStore: AdCatalogStore?
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "epfk-test",
            fmBackfillMode: .off
        )
        return AdDetectionService(
            store: store,
            metadataExtractor: FallbackExtractor(),
            config: config,
            adCatalogStore: catalogStore
        )
    }
}

#endif
