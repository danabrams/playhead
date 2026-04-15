// CorrectionAttributionTests.swift
// Phase EF2 (playhead-ef2.3.1): Tests for CorrectionAttribution types,
// causal inference logic, schema extension, and integration with
// PersistentUserCorrectionStore.

import XCTest
@testable import Playhead

final class CorrectionAttributionTests: XCTestCase {

    // MARK: - CorrectionType round-trip

    func testCorrectionTypeRawValueRoundTrip() {
        for type in CorrectionType.allCases {
            let raw = type.rawValue
            let decoded = CorrectionType(rawValue: raw)
            XCTAssertEqual(decoded, type, "CorrectionType.\(type) must round-trip through rawValue")
        }
    }

    // MARK: - CausalSource round-trip

    func testCausalSourceRawValueRoundTrip() {
        for source in CausalSource.allCases {
            let raw = source.rawValue
            let decoded = CausalSource(rawValue: raw)
            XCTAssertEqual(decoded, source, "CausalSource.\(source) must round-trip through rawValue")
        }
    }

    // MARK: - CorrectionTargetRefs Codable round-trip

    func testTargetRefsCodableRoundTrip() throws {
        let refs = CorrectionTargetRefs(
            atomIds: [3, 7, 12],
            evidenceRefs: ["[E0]", "[E3]"],
            fingerprintId: "fp-abc",
            domain: "example.com",
            sponsorEntity: "squarespace"
        )
        let data = try JSONEncoder().encode(refs)
        let decoded = try JSONDecoder().decode(CorrectionTargetRefs.self, from: data)
        XCTAssertEqual(decoded, refs)
    }

    func testTargetRefsAllNilFieldsCodableRoundTrip() throws {
        let refs = CorrectionTargetRefs()
        let data = try JSONEncoder().encode(refs)
        let decoded = try JSONDecoder().decode(CorrectionTargetRefs.self, from: data)
        XCTAssertEqual(decoded, refs)
    }

    // MARK: - inferCausalSource: lexical top source

    func testInferCausalSourceLexicalTopWeight() {
        let entries: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(source: .lexical, weight: 0.5, detail: .lexical(matchedCategories: ["url"])),
            EvidenceLedgerEntry(source: .fm, weight: 0.2, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
        ]
        let result = CausalInference.inferCausalSource(provenance: [], ledgerEntries: entries)
        XCTAssertEqual(result, .lexical)
    }

    // MARK: - inferCausalSource: FM > 0.3 of total

    func testInferCausalSourceFMAboveThreshold() {
        let entries: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(source: .fm, weight: 0.4, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
            EvidenceLedgerEntry(source: .acoustic, weight: 0.5, detail: .acoustic(breakStrength: 0.8)),
        ]
        // FM weight = 0.4, total = 0.9, FM fraction = 0.4/0.9 = 0.444 > 0.3
        let result = CausalInference.inferCausalSource(provenance: [], ledgerEntries: entries)
        XCTAssertEqual(result, .foundationModel)
    }

    // MARK: - inferCausalSource: FM exactly at 0.3 threshold

    func testInferCausalSourceFMAtExactThreshold() {
        // FM at exactly 0.3 of total should NOT trigger the FM rule (> 0.3, not >=).
        let entries: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(source: .fm, weight: 0.3, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
            EvidenceLedgerEntry(source: .acoustic, weight: 0.7, detail: .acoustic(breakStrength: 0.8)),
        ]
        // FM weight = 0.3, total = 1.0, FM fraction = 0.3 — not > 0.3
        let result = CausalInference.inferCausalSource(provenance: [], ledgerEntries: entries)
        XCTAssertEqual(result, .acoustic, "FM at exactly 0.3 fraction should not trigger FM rule")
    }

    // MARK: - inferCausalSource: fingerprint top source

    func testInferCausalSourceFingerprintTopWeight() {
        let entries: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(source: .fingerprint, weight: 0.6, detail: .fingerprint(matchCount: 3, averageSimilarity: 0.95)),
            EvidenceLedgerEntry(source: .fm, weight: 0.1, detail: .fm(disposition: .containsAd, band: .weak, cohortPromptLabel: "v1")),
        ]
        let result = CausalInference.inferCausalSource(provenance: [], ledgerEntries: entries)
        XCTAssertEqual(result, .fingerprint)
    }

    // MARK: - inferCausalSource: acoustic highest weight, FM below threshold

    func testInferCausalSourceAcousticHighestWeight() {
        let entries: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(source: .acoustic, weight: 0.7, detail: .acoustic(breakStrength: 0.9)),
            EvidenceLedgerEntry(source: .fm, weight: 0.2, detail: .fm(disposition: .containsAd, band: .weak, cohortPromptLabel: "v1")),
        ]
        // FM fraction = 0.2/0.9 = 0.222 < 0.3, acoustic is top
        let result = CausalInference.inferCausalSource(provenance: [], ledgerEntries: entries)
        XCTAssertEqual(result, .acoustic)
    }

    // MARK: - inferCausalSource: tied weights (deterministic tie-break)

    func testInferCausalSourceTiedWeightsDeterministic() {
        // When two non-lexical, non-FM sources tie, the result must be
        // deterministic (sorted by rawValue as tie-breaker).
        let entries: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(source: .acoustic, weight: 0.5, detail: .acoustic(breakStrength: 0.9)),
            EvidenceLedgerEntry(source: .fingerprint, weight: 0.5, detail: .fingerprint(matchCount: 2, averageSimilarity: 0.9)),
        ]
        let result = CausalInference.inferCausalSource(provenance: [], ledgerEntries: entries)
        // "acoustic" < "fingerprint" lexicographically, so acoustic wins the tie.
        XCTAssertEqual(result, .acoustic, "Tied weights should resolve deterministically via rawValue ordering")
    }

    // MARK: - inferCausalSource: all-zero weights fall back to provenance

    func testInferCausalSourceAllZeroWeightsFallsBackToProvenance() {
        let entries: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(source: .lexical, weight: 0.0, detail: .lexical(matchedCategories: ["url"])),
            EvidenceLedgerEntry(source: .fm, weight: 0.0, detail: .fm(disposition: .containsAd, band: .weak, cohortPromptLabel: "v1")),
        ]
        let provenance: [AnchorRef] = [
            .fmConsensus(regionId: "r1", consensusStrength: 0.8)
        ]
        let result = CausalInference.inferCausalSource(provenance: provenance, ledgerEntries: entries)
        XCTAssertEqual(result, .foundationModel, "All-zero weights should fall back to provenance inference")
    }

    // MARK: - inferCausalSource: empty ledger, provenance-only

    func testInferCausalSourceFromProvenanceFMConsensus() {
        let provenance: [AnchorRef] = [
            .fmConsensus(regionId: "r1", consensusStrength: 0.8)
        ]
        let result = CausalInference.inferCausalSource(provenance: provenance, ledgerEntries: [])
        XCTAssertEqual(result, .foundationModel)
    }

    func testInferCausalSourceFromProvenanceEvidenceCatalog() {
        let entry = EvidenceEntry(
            evidenceRef: 0,
            category: .url,
            matchedText: "example.com/promo",
            normalizedText: "example.com/promo",
            atomOrdinal: 5,
            startTime: 10.0,
            endTime: 12.0
        )
        let provenance: [AnchorRef] = [.evidenceCatalog(entry: entry)]
        let result = CausalInference.inferCausalSource(provenance: provenance, ledgerEntries: [])
        XCTAssertEqual(result, .lexical)
    }

    func testInferCausalSourceFromProvenanceAcousticCorroborated() {
        let provenance: [AnchorRef] = [
            .fmAcousticCorroborated(regionId: "r1", breakStrength: 0.5)
        ]
        let result = CausalInference.inferCausalSource(provenance: provenance, ledgerEntries: [])
        // fmAcousticCorroborated counts as FM, so should be .foundationModel
        XCTAssertEqual(result, .foundationModel)
    }

    func testInferCausalSourceEmptyProvenanceDefaultsToFM() {
        let result = CausalInference.inferCausalSource(provenance: [], ledgerEntries: [])
        XCTAssertEqual(result, .foundationModel)
    }

    // MARK: - buildTargetRefs

    func testBuildTargetRefsFromEvidenceCatalog() {
        let entry = EvidenceEntry(
            evidenceRef: 3,
            category: .brandSpan,
            matchedText: "Squarespace",
            normalizedText: "squarespace",
            atomOrdinal: 7,
            startTime: 15.0,
            endTime: 17.0
        )
        let provenance: [AnchorRef] = [.evidenceCatalog(entry: entry)]
        let refs = CausalInference.buildTargetRefs(provenance: provenance, ledgerEntries: [])
        XCTAssertNotNil(refs)
        XCTAssertEqual(refs?.atomIds, [7])
        XCTAssertEqual(refs?.evidenceRefs, ["[E3]"])
        XCTAssertEqual(refs?.sponsorEntity, "squarespace")
    }

    func testBuildTargetRefsEmptyProvenanceReturnsNil() {
        let refs = CausalInference.buildTargetRefs(provenance: [], ledgerEntries: [])
        XCTAssertNil(refs)
    }

    func testBuildTargetRefsExplicitSponsorOverridesInferred() {
        let entry = EvidenceEntry(
            evidenceRef: 0,
            category: .brandSpan,
            matchedText: "BrandA",
            normalizedText: "branda",
            atomOrdinal: 1,
            startTime: 1.0,
            endTime: 2.0
        )
        let provenance: [AnchorRef] = [.evidenceCatalog(entry: entry)]
        let refs = CausalInference.buildTargetRefs(
            provenance: provenance,
            ledgerEntries: [],
            sponsorEntity: "explicit-sponsor"
        )
        XCTAssertEqual(refs?.sponsorEntity, "explicit-sponsor")
    }

    // MARK: - CorrectionEvent with attribution fields

    func testCorrectionEventAttributionDefaultsToNil() {
        let event = CorrectionEvent(
            analysisAssetId: "asset-1",
            scope: "exactSpan:asset-1:0:5"
        )
        XCTAssertNil(event.correctionType)
        XCTAssertNil(event.causalSource)
        XCTAssertNil(event.targetRefs)
    }

    func testCorrectionEventWithAttribution() {
        let refs = CorrectionTargetRefs(atomIds: [1, 2, 3])
        let event = CorrectionEvent(
            analysisAssetId: "asset-1",
            scope: "exactSpan:asset-1:0:5",
            correctionType: .falsePositive,
            causalSource: .lexical,
            targetRefs: refs
        )
        XCTAssertEqual(event.correctionType, .falsePositive)
        XCTAssertEqual(event.causalSource, .lexical)
        XCTAssertEqual(event.targetRefs?.atomIds, [1, 2, 3])
    }

    // MARK: - Schema: new columns exist after migration

    func testSchemaHasAttributionColumns() async throws {
        let dir = try makeTempDir(prefix: "CorrectionAttributionTests")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        XCTAssertTrue(try probeColumnExists(in: dir, table: "correction_events", column: "correctionType"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "correction_events", column: "causalSource"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "correction_events", column: "targetRefsJSON"))
    }

    // MARK: - Persistence round-trip with attribution

    func testAttributionPersistenceRoundTrip() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-attr"))

        let refs = CorrectionTargetRefs(
            atomIds: [5, 10],
            evidenceRefs: ["[E0]"],
            fingerprintId: "fp-123",
            domain: "podcast.example.com",
            sponsorEntity: "squarespace"
        )
        let event = CorrectionEvent(
            analysisAssetId: "asset-attr",
            scope: CorrectionScope.exactSpan(assetId: "asset-attr", ordinalRange: 5...15).serialized,
            source: .manualVeto,
            correctionType: .falsePositive,
            causalSource: .lexical,
            targetRefs: refs
        )
        try await correctionStore.record(event)

        let loaded = try await correctionStore.activeCorrections(for: "asset-attr")
        XCTAssertEqual(loaded.count, 1)
        let loaded0 = loaded[0]
        XCTAssertEqual(loaded0.correctionType, .falsePositive)
        XCTAssertEqual(loaded0.causalSource, .lexical)
        XCTAssertNotNil(loaded0.targetRefs)
        XCTAssertEqual(loaded0.targetRefs?.atomIds, [5, 10])
        XCTAssertEqual(loaded0.targetRefs?.evidenceRefs, ["[E0]"])
        XCTAssertEqual(loaded0.targetRefs?.fingerprintId, "fp-123")
        XCTAssertEqual(loaded0.targetRefs?.domain, "podcast.example.com")
        XCTAssertEqual(loaded0.targetRefs?.sponsorEntity, "squarespace")
    }

    func testAttributionNilFieldsPersistCorrectly() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-nil-attr"))

        // Legacy event without attribution.
        let event = CorrectionEvent(
            analysisAssetId: "asset-nil-attr",
            scope: CorrectionScope.exactSpan(assetId: "asset-nil-attr", ordinalRange: 0...3).serialized,
            source: .manualVeto
        )
        try await correctionStore.record(event)

        let loaded = try await correctionStore.activeCorrections(for: "asset-nil-attr")
        XCTAssertEqual(loaded.count, 1)
        XCTAssertNil(loaded[0].correctionType)
        XCTAssertNil(loaded[0].causalSource)
        XCTAssertNil(loaded[0].targetRefs)
    }

    // MARK: - recordVeto integration: attribution is populated

    func testRecordVetoPopulatesAttribution() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-veto-attr"))

        let entry = EvidenceEntry(
            evidenceRef: 0,
            category: .url,
            matchedText: "example.com/promo",
            normalizedText: "example.com/promo",
            atomOrdinal: 5,
            startTime: 10.0,
            endTime: 12.0
        )
        let span = DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-veto-attr", firstAtomOrdinal: 2, lastAtomOrdinal: 8),
            assetId: "asset-veto-attr",
            firstAtomOrdinal: 2,
            lastAtomOrdinal: 8,
            startTime: 10.0,
            endTime: 40.0,
            anchorProvenance: [.evidenceCatalog(entry: entry)]
        )
        await correctionStore.recordVeto(span: span)

        let events = try await correctionStore.activeCorrections(for: "asset-veto-attr")
        XCTAssertEqual(events.count, 1)
        let event = events[0]
        XCTAssertEqual(event.correctionType, .falsePositive)
        XCTAssertEqual(event.causalSource, .lexical, "URL evidence catalog should infer lexical causal source")
        XCTAssertNotNil(event.targetRefs)
        XCTAssertEqual(event.targetRefs?.atomIds, [5])
        XCTAssertEqual(event.targetRefs?.evidenceRefs, ["[E0]"])
    }

    func testRecordVetoWithLedgerEntriesUsesLedger() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-ledger"))

        let span = DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-ledger", firstAtomOrdinal: 0, lastAtomOrdinal: 5),
            assetId: "asset-ledger",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 5,
            startTime: 0,
            endTime: 20.0,
            anchorProvenance: []
        )
        let ledger: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(source: .fm, weight: 0.6, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
            EvidenceLedgerEntry(source: .lexical, weight: 0.1, detail: .lexical(matchedCategories: ["promoCode"])),
        ]
        await correctionStore.recordVeto(span: span, ledgerEntries: ledger)

        let events = try await correctionStore.activeCorrections(for: "asset-ledger")
        XCTAssertEqual(events.count, 1)
        // FM weight = 0.6, total = 0.7, FM fraction = 0.857 > 0.3
        XCTAssertEqual(events[0].causalSource, .foundationModel)
    }

    // MARK: - recordVeto with brandSpan: sponsorOnShow also gets attribution

    func testRecordVetoWithBrandSpanCarriesAttributionOnBothEvents() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-brand-attr"))

        let brandEntry = EvidenceEntry(
            evidenceRef: 0,
            category: .brandSpan,
            matchedText: "BetterHelp",
            normalizedText: "betterhelp",
            atomOrdinal: 4,
            startTime: 8.0,
            endTime: 10.0
        )
        let span = DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-brand-attr", firstAtomOrdinal: 2, lastAtomOrdinal: 8),
            assetId: "asset-brand-attr",
            firstAtomOrdinal: 2,
            lastAtomOrdinal: 8,
            startTime: 5.0,
            endTime: 35.0,
            anchorProvenance: [.evidenceCatalog(entry: brandEntry)]
        )
        await correctionStore.recordVeto(span: span)

        let events = try await correctionStore.activeCorrections(for: "asset-brand-attr")
        XCTAssertEqual(events.count, 2, "Should write exactSpan + sponsorOnShow")

        // Both events should have attribution.
        for event in events {
            XCTAssertEqual(event.correctionType, .falsePositive)
            XCTAssertEqual(event.causalSource, .lexical)
        }

        // The sponsorOnShow event should reference the sponsor entity.
        let sponsorEvent = events.first { $0.scope.hasPrefix("sponsorOnShow:") }
        XCTAssertNotNil(sponsorEvent)
        XCTAssertEqual(sponsorEvent?.targetRefs?.sponsorEntity, "betterhelp")
    }

    // MARK: - Backward compatibility: legacy events without attribution

    func testLegacyEventsLoadWithNilAttribution() async throws {
        let analysisStore = try await makeTestStore()
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-legacy"))

        // Manually insert a legacy event without attribution columns via raw SQL
        // to simulate pre-ef2.3.1 data.
        let legacyEvent = CorrectionEvent(
            analysisAssetId: "asset-legacy",
            scope: CorrectionScope.exactSpan(assetId: "asset-legacy", ordinalRange: 0...5).serialized,
            source: .manualVeto
        )
        try await analysisStore.appendCorrectionEvent(legacyEvent)

        let loaded = try await analysisStore.loadCorrectionEvents(analysisAssetId: "asset-legacy")
        XCTAssertEqual(loaded.count, 1)
        XCTAssertNil(loaded[0].correctionType)
        XCTAssertNil(loaded[0].causalSource)
        XCTAssertNil(loaded[0].targetRefs)
    }

    // MARK: - All CorrectionType values persist correctly

    func testAllCorrectionTypesPersist() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-types"))

        for (i, type) in CorrectionType.allCases.enumerated() {
            let event = CorrectionEvent(
                analysisAssetId: "asset-types",
                scope: CorrectionScope.exactSpan(assetId: "asset-types", ordinalRange: i...(i+1)).serialized,
                correctionType: type,
                causalSource: .lexical
            )
            try await correctionStore.record(event)
        }

        let loaded = try await correctionStore.activeCorrections(for: "asset-types")
        XCTAssertEqual(loaded.count, CorrectionType.allCases.count)
        let loadedTypes = Set(loaded.compactMap(\.correctionType))
        XCTAssertEqual(loadedTypes, Set(CorrectionType.allCases))
    }

    // MARK: - All CausalSource values persist correctly

    func testAllCausalSourcesPersist() async throws {
        let analysisStore = try await makeTestStore()
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        try await analysisStore.insertAsset(makeTestAsset(id: "asset-sources"))

        for (i, source) in CausalSource.allCases.enumerated() {
            let event = CorrectionEvent(
                analysisAssetId: "asset-sources",
                scope: CorrectionScope.exactSpan(assetId: "asset-sources", ordinalRange: i...(i+1)).serialized,
                correctionType: .falsePositive,
                causalSource: source
            )
            try await correctionStore.record(event)
        }

        let loaded = try await correctionStore.activeCorrections(for: "asset-sources")
        XCTAssertEqual(loaded.count, CausalSource.allCases.count)
        let loadedSources = Set(loaded.compactMap(\.causalSource))
        XCTAssertEqual(loadedSources, Set(CausalSource.allCases))
    }
}
