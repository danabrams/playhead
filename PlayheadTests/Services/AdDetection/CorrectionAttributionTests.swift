// CorrectionAttributionTests.swift
// Phase EF2 (playhead-ef2.3.1): Tests for CorrectionAttribution types,
// causal inference logic, schema extension, and integration with
// PersistentUserCorrectionStore.

import XCTest
import SQLite3
@testable import Playhead

final class CorrectionAttributionTests: XCTestCase {

    // MARK: - CorrectionType round-trip

    func testMaterialIdentityCanonicalizesSignedZero() {
        let negativeZero = makePrivacyWindow(
            id: "signed-zero-window",
            startTime: -0.0
        )
        let positiveZero = makePrivacyWindow(
            id: "signed-zero-window",
            startTime: 0.0
        )

        XCTAssertTrue(
            AdWindowMaterialIdentity.sameProducerRevision(
                negativeZero,
                positiveZero
            )
        )
        XCTAssertTrue(
            ExactFeedbackSpan(startTime: -0.0, endTime: 45)
                .matches(startTime: 0.0, endTime: 45)
        )
    }

    /// playhead-ar60 R1 review: the named residual gap. `producerRevisionToken`
    /// is a CAS fence — `persistRevertedAdWindowsIfCurrent` and the hot-path
    /// reconcile refuse to mutate a row whose revision moved. V47 added a
    /// second number the skip gate reads, so a fence that cannot see it would
    /// let a write land against a revision that differs in exactly the field
    /// that decides whether the span is skipped.
    func testProducerRevisionDiscriminatesActuationConfidence() {
        let suppressed = makePrivacyWindow(
            id: "actuation-fence-window",
            skipConfidence: 0.0039
        )
        let notSuppressed = makePrivacyWindow(
            id: "actuation-fence-window",
            skipConfidence: nil
        )
        let other = makePrivacyWindow(
            id: "actuation-fence-window",
            skipConfidence: 0.71
        )

        XCTAssertEqual(suppressed.confidence, notSuppressed.confidence)
        XCTAssertFalse(
            AdWindowMaterialIdentity.sameProducerRevision(
                suppressed,
                notSuppressed
            ),
            "nil and a persisted actuation number are different revisions"
        )
        XCTAssertFalse(
            AdWindowMaterialIdentity.sameProducerRevision(suppressed, other)
        )
        XCTAssertTrue(
            AdWindowMaterialIdentity.sameProducerRevision(
                suppressed,
                makePrivacyWindow(
                    id: "actuation-fence-window",
                    skipConfidence: 0.0039
                )
            )
        )
    }

    func testExplicitReceiptRejectsEmbeddedNULWindowIdentity() {
        XCTAssertNil(
            CorrectionTargetRefs(
                adWindowId: "window\u{0}other"
            ).canonicalExplicitAdWindowIDs
        )
    }

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
            adWindowId: "promoted-window",
            adWindowIds: ["original-window", "promoted-window"],
            explicitFeedbackDetectionProjection:
                ExplicitFeedbackDetectionProjection(
                    makePrivacyWindow(id: "original-window")
                ),
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

    func testInferCausalSourceIgnoresObservabilityRows() {
        let entries: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(source: .audit, weight: 1.0, detail: .classifier(score: 1.0)),
            EvidenceLedgerEntry(source: .operational, weight: 1.0, detail: .classifier(score: 1.0)),
            EvidenceLedgerEntry(source: .lexical, weight: 0.1, detail: .lexical(matchedCategories: ["url"])),
        ]

        let result = CausalInference.inferCausalSource(provenance: [], ledgerEntries: entries)

        XCTAssertEqual(result, .lexical)
    }

    func testInferCausalSourceIgnoresLearnedCatalogDiagnostic() {
        let entries: [EvidenceLedgerEntry] = [
            EvidenceLedgerEntry(
                source: .catalog,
                weight: 1,
                detail: .catalog(entryCount: 1),
                subSource: .fingerprintStore
            ),
            EvidenceLedgerEntry(
                source: .acoustic,
                weight: 0.1,
                detail: .acoustic(breakStrength: 0.5)
            ),
        ]

        let result = CausalInference.inferCausalSource(
            provenance: [],
            ledgerEntries: entries
        )

        XCTAssertEqual(result, .acoustic)
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
        XCTAssertTrue(try probeColumnExists(in: dir, table: "correction_events", column: "correctionIdentityKey"))
    }

    func testV31ReceiptIdentityMigrationBackfillsWithoutLosingRows()
        async throws
    {
        let dir = try makeTempDir(prefix: "CorrectionReceiptIdentityV32")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        let asset = makeTestAsset(id: "privacy-asset")
        try await bootstrap.insertAsset(asset)

        let original = makePrivacyWindow(id: "legacy-owned-window")
        let scope = CorrectionScope.exactTimeSpan(
            assetId: asset.id,
            startTime: original.startTime,
            endTime: original.endTime
        )
        func explicitReceipt(id: String, window: AdWindow) -> CorrectionEvent {
            CorrectionEvent(
                id: id,
                analysisAssetId: asset.id,
                scope: scope.serialized,
                createdAt: 100,
                source: .bannerSuggestionDenied,
                correctionType: .falsePositive,
                targetRefs: CorrectionTargetRefs(
                    adWindowId: window.id,
                    adWindowIds: [window.id],
                    explicitFeedbackDetectionProjection:
                        ExplicitFeedbackDetectionProjection(window)
                )
            )
        }
        let legacyExplicit = explicitReceipt(
            id: "legacy-valid-explicit",
            window: original
        )
        let insertedLegacyExplicit =
            try await bootstrap.appendCorrectionEvent(legacyExplicit)
        XCTAssertTrue(insertedLegacyExplicit)
        let legacyGeneric = CorrectionEvent(
            id: "legacy-generic",
            analysisAssetId: asset.id,
            scope: CorrectionScope.exactTimeSpan(
                assetId: asset.id,
                startTime: 200,
                endTime: 245
            ).serialized,
            createdAt: 101,
            source: .manualVeto,
            correctionType: .falsePositive
        )
        let insertedLegacyGeneric =
            try await bootstrap.appendCorrectionEvent(legacyGeneric)
        XCTAssertTrue(insertedLegacyGeneric)

        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        XCTAssertEqual(
            sqlite3_open_v2(
                dbURL.path,
                &db,
                SQLITE_OPEN_READWRITE,
                nil
            ),
            SQLITE_OK
        )
        defer {
            if let db {
                sqlite3_close_v2(db)
            }
        }
        let malformedScope = CorrectionScope.exactTimeSpan(
            assetId: asset.id,
            startTime: 300,
            endTime: 345
        )
        let rewind = """
            DROP INDEX IF EXISTS idx_correction_events_identity;
            ALTER TABLE correction_events DROP COLUMN correctionIdentityKey;
            CREATE UNIQUE INDEX idx_correction_events_identity
            ON correction_events(
                analysisAssetId,
                effectiveCorrectionType,
                normalizedScopeKey
            );
            INSERT INTO correction_events(
                id, analysisAssetId, scope, createdAt, source,
                correctionType, firstSeenAt, lastSeenAt, submissionCount,
                normalizedScopeKey, effectiveCorrectionType
            ) VALUES (
                'legacy-malformed-explicit',
                '\(asset.id)',
                '\(malformedScope.serialized)',
                102,
                '\(CorrectionSource.bannerSuggestionDenied.rawValue)',
                '\(CorrectionType.falsePositive.rawValue)',
                102,
                102,
                1,
                '\(malformedScope.normalizedIdentityKey)',
                '\(CorrectionType.falsePositive.rawValue)'
            );
            UPDATE _meta SET value = '31' WHERE key = 'schema_version';
            """
        var errorMessage: UnsafeMutablePointer<CChar>?
        let rewindResult = sqlite3_exec(
            db,
            rewind,
            nil,
            nil,
            &errorMessage
        )
        if rewindResult != SQLITE_OK {
            let message = errorMessage.map { String(cString: $0) }
                ?? "unknown SQLite error"
            sqlite3_free(errorMessage)
            XCTFail("Failed to build v31 fixture: \(message)")
            return
        }
        sqlite3_close_v2(db)
        db = nil

        AnalysisStore.resetMigratedPathsForTesting()
        let migrated = try AnalysisStore(directory: dir)
        try await migrated.migrate()
        let migratedVersion = try await migrated.schemaVersion()
        XCTAssertEqual(
            migratedVersion,
            AnalysisStore.currentSchemaVersion
        )
        XCTAssertTrue(
            try probeColumnExists(
                in: dir,
                table: "correction_events",
                column: "correctionIdentityKey"
            )
        )

        let migratedRows = try await migrated.loadCorrectionEvents(
            analysisAssetId: asset.id
        )
        XCTAssertEqual(migratedRows.count, 3)
        XCTAssertNotNil(
            migratedRows.first {
                $0.id == "legacy-malformed-explicit"
                    && $0.targetRefs == nil
            }
        )

        let retryInserted = try await migrated.appendCorrectionEvent(
            explicitReceipt(
                id: "legacy-valid-explicit-retry",
                window: original
            )
        )
        XCTAssertFalse(retryInserted)
        let sameSpanGeneric = CorrectionEvent(
            id: "generic-after-v32",
            analysisAssetId: asset.id,
            scope: scope.serialized,
            createdAt: 200,
            source: .manualVeto,
            correctionType: .falsePositive
        )
        let insertedSameSpanGeneric =
            try await migrated.appendCorrectionEvent(sameSpanGeneric)
        XCTAssertTrue(insertedSameSpanGeneric)
        let otherTarget = makePrivacyWindow(id: "other-same-span-window")
        let insertedOtherTarget =
            try await migrated.appendCorrectionEvent(
                explicitReceipt(
                    id: "other-target-after-v32",
                    window: otherTarget
                )
            )
        XCTAssertTrue(insertedOtherTarget)

        let finalRows = try await migrated.loadCorrectionEvents(
            analysisAssetId: asset.id
        )
        XCTAssertEqual(finalRows.count, 5)
        XCTAssertEqual(
            finalRows.first { $0.id == legacyExplicit.id }?
                .submissionCount,
            2
        )
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

    // MARK: - Explicit feedback response-independent projection

    func testExplicitReceiptIdentitySeparatesGenericRouteAndTargetWhileRetriesDedupe()
        async throws
    {
        for explicitFirst in [false, true] {
            let store = try await makeTestStore()
            try await store.insertAsset(makeTestAsset(id: "privacy-asset"))
            let original = makePrivacyWindow(id: "identity-window")
            let scope = CorrectionScope.exactTimeSpan(
                assetId: "privacy-asset",
                startTime: original.startTime,
                endTime: original.endTime
            ).serialized
            let generic = CorrectionEvent(
                id: "generic-\(explicitFirst)",
                analysisAssetId: "privacy-asset",
                scope: scope,
                createdAt: 1,
                source: .manualVeto,
                correctionType: .falsePositive
            )
            let explicit = CorrectionEvent(
                id: "explicit-\(explicitFirst)",
                analysisAssetId: "privacy-asset",
                scope: scope,
                createdAt: 2,
                source: .bannerSuggestionDenied,
                correctionType: .falsePositive,
                targetRefs: CorrectionTargetRefs(
                    adWindowId: original.id,
                    adWindowIds: [original.id],
                    explicitFeedbackDetectionProjection:
                        ExplicitFeedbackDetectionProjection(original)
                )
            )
            for event in explicitFirst
                ? [explicit, generic]
                : [generic, explicit]
            {
                _ = try await store.appendCorrectionEvent(event)
            }
            let rows = try await store.loadCorrectionEvents(
                analysisAssetId: "privacy-asset"
            )
            XCTAssertEqual(rows.count, 2)
            XCTAssertEqual(Set(rows.compactMap(\.source)), [
                .manualVeto,
                .bannerSuggestionDenied,
            ])
        }

        let store = try await makeTestStore()
        try await store.insertAsset(makeTestAsset(id: "privacy-asset"))
        let first = makePrivacyWindow(id: "same-span-first")
        let second = makePrivacyWindow(id: "same-span-second")
        let scope = CorrectionScope.exactTimeSpan(
            assetId: "privacy-asset",
            startTime: first.startTime,
            endTime: first.endTime
        ).serialized
        func receipt(
            id: String,
            source: CorrectionSource,
            window: AdWindow
        ) -> CorrectionEvent {
            CorrectionEvent(
                id: id,
                analysisAssetId: "privacy-asset",
                scope: scope,
                source: source,
                correctionType: source.kind.correctionType,
                targetRefs: CorrectionTargetRefs(
                    adWindowId: window.id,
                    adWindowIds: [window.id],
                    explicitFeedbackDetectionProjection:
                        ExplicitFeedbackDetectionProjection(window)
                )
            )
        }
        let firstNo = receipt(
            id: "first-no",
            source: .bannerSuggestionDenied,
            window: first
        )
        _ = try await store.appendCorrectionEvent(firstNo)
        _ = try await store.appendCorrectionEvent(receipt(
            id: "first-no-retry",
            source: .bannerSuggestionDenied,
            window: first
        ))
        _ = try await store.appendCorrectionEvent(receipt(
            id: "second-no",
            source: .bannerSuggestionDenied,
            window: second
        ))
        _ = try await store.appendCorrectionEvent(receipt(
            id: "same-target-other-route",
            source: .bannerAutoSkipDenied,
            window: first
        ))
        let explicitRows = try await store.loadCorrectionEvents(
            analysisAssetId: "privacy-asset"
        )
        XCTAssertEqual(explicitRows.count, 3)
        XCTAssertEqual(
            explicitRows.first(where: { $0.id == firstNo.id })?
                .submissionCount,
            2
        )
        XCTAssertEqual(
            Set(explicitRows.compactMap {
                $0.targetRefs?.adWindowId
            }),
            [first.id, second.id]
        )

        let genericOne = CorrectionEvent(
            id: "generic-control-one",
            analysisAssetId: "privacy-asset",
            scope: scope,
            source: .manualVeto,
            correctionType: .falsePositive
        )
        let genericTwo = CorrectionEvent(
            id: "generic-control-two",
            analysisAssetId: "privacy-asset",
            scope: scope,
            source: .manualVeto,
            correctionType: .falsePositive
        )
        _ = try await store.appendCorrectionEvent(genericOne)
        _ = try await store.appendCorrectionEvent(genericTwo)
        let allRows = try await store.loadCorrectionEvents(
            analysisAssetId: "privacy-asset"
        )
        XCTAssertEqual(allRows.count, 4)
        XCTAssertEqual(
            allRows.first(where: { $0.id == genericOne.id })?
                .submissionCount,
            2
        )
    }

    func testExplicitFeedbackTransactionRollsBackPersistedBodyMismatch()
        async throws
    {
        let store = try await makeTestStore()
        let asset = makeTestAsset(id: "privacy-asset")
        let original = makePrivacyWindow(id: "mismatch-window")
        try await store.insertAsset(asset)
        try await store.insertAdWindow(original)

        let scope = CorrectionScope.exactTimeSpan(
            assetId: asset.id,
            startTime: original.startTime,
            endTime: original.endTime
        ).serialized
        let staleProjection = makePrivacyWindow(
            id: original.id,
            boundaryState: "stale-private-projection"
        )
        let persisted = CorrectionEvent(
            id: "persisted-mismatched-receipt",
            analysisAssetId: asset.id,
            scope: scope,
            source: .bannerSuggestionDenied,
            correctionType: .falsePositive,
            targetRefs: CorrectionTargetRefs(
                adWindowId: original.id,
                adWindowIds: [original.id],
                explicitFeedbackDetectionProjection:
                    ExplicitFeedbackDetectionProjection(staleProjection)
            )
        )
        _ = try await store.appendCorrectionEvent(persisted)

        let expected = CorrectionEvent(
            id: "incoming-correct-receipt",
            analysisAssetId: asset.id,
            scope: scope,
            source: .bannerSuggestionDenied,
            correctionType: .falsePositive,
            targetRefs: CorrectionTargetRefs(
                adWindowId: original.id,
                adWindowIds: [original.id],
                explicitFeedbackDetectionProjection:
                    ExplicitFeedbackDetectionProjection(original)
            )
        )
        let result = try await store.persistDeclinedSuggestionIfCurrent(
            windowId: original.id,
            analysisAssetId: asset.id,
            expectedEpisodeId: asset.episodeId,
            expectedStartTime: original.startTime,
            expectedEndTime: original.endTime,
            expectedProducerRevision: original,
            expectedMaterialToken:
                AdWindowMaterialIdentity.suggestionToken(original),
            correction: expected
        )

        XCTAssertNil(result)
        let storedWindows = try await store.fetchAdWindows(assetId: asset.id)
        let current = try XCTUnwrap(storedWindows.first)
        XCTAssertEqual(current.decisionState, original.decisionState)
        XCTAssertEqual(
            current.userDismissedBanner,
            original.userDismissedBanner
        )
        XCTAssertEqual(current.wasSkipped, original.wasSkipped)

        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: asset.id
        )
        XCTAssertEqual(receipts.count, 1)
        XCTAssertEqual(receipts.first?.id, persisted.id)
        XCTAssertEqual(receipts.first?.submissionCount, 1)
        XCTAssertEqual(receipts.first?.targetRefs, persisted.targetRefs)
    }

    func testAllExplicitRoutesMatchTheirPreResponseDetectionProjection() {
        // playhead-nq8z: `missedAutoSkipListDenied` is in this population
        // because the privacy projection is keyed on
        // `isExplicitBannerFeedback`, which the new case joins. The window
        // rows it produces are the CARD denial's exactly — same seam, same
        // transaction, same terminal state — so it shares that arm below
        // rather than getting one of its own.
        let sources: [CorrectionSource] = [
            .bannerAutoSkipConfirmed,
            .bannerAutoSkipDenied,
            .missedAutoSkipListDenied,
            .bannerSuggestionConfirmed,
            .bannerSuggestionDenied,
        ]

        for (index, source) in sources.enumerated() {
            let original = makePrivacyWindow(
                id: "privacy-original-\(index)",
                startTime: 100 + Double(index * 100)
            )
            let expected = ExplicitBannerFeedbackPrivacy
                .responseIndependentProjection(
                    windows: [original],
                    corrections: []
                )
            let promoted = makePrivacyWindow(
                id: "privacy-promoted-\(index)",
                startTime: original.startTime,
                boundaryState: "userConfirmedSuggested",
                decisionState: AdDecisionState.applied.rawValue,
                wasSkipped: true
            )
            let responseRows: [AdWindow]
            let targetIds: [String]
            let singularTarget: String
            switch source {
            case .bannerAutoSkipConfirmed:
                responseRows = [
                    makePrivacyWindow(
                        id: original.id,
                        startTime: original.startTime,
                        decisionState: AdDecisionState.applied.rawValue,
                        wasSkipped: true
                    ),
                ]
                targetIds = [original.id]
                singularTarget = original.id
            case .bannerAutoSkipDenied, .missedAutoSkipListDenied:
                responseRows = [
                    makePrivacyWindow(
                        id: original.id,
                        startTime: original.startTime,
                        decisionState: AdDecisionState.reverted.rawValue,
                        wasSkipped: true
                    ),
                ]
                targetIds = [original.id]
                singularTarget = original.id
            case .bannerSuggestionConfirmed:
                responseRows = [
                    makePrivacyWindow(
                        id: original.id,
                        startTime: original.startTime,
                        decisionState:
                            AdDecisionState.suppressed.rawValue
                    ),
                    promoted,
                ]
                targetIds = [original.id, promoted.id]
                singularTarget = promoted.id
            case .bannerSuggestionDenied:
                responseRows = [
                    makePrivacyWindow(
                        id: original.id,
                        startTime: original.startTime,
                        decisionState: AdDecisionState.reverted.rawValue,
                        dismissed: true
                    ),
                ]
                targetIds = [original.id]
                singularTarget = original.id
            case .listenRevert, .manualVeto, .falseNegative:
                XCTFail("Unexpected non-explicit source")
                continue
            }
            let event = CorrectionEvent(
                analysisAssetId: original.analysisAssetId,
                scope: CorrectionScope.exactTimeSpan(
                    assetId: original.analysisAssetId,
                    startTime: original.startTime,
                    endTime: original.endTime
                ).serialized,
                source: source,
                correctionType: source.kind.correctionType,
                targetRefs: CorrectionTargetRefs(
                    adWindowId: singularTarget,
                    adWindowIds: targetIds,
                    explicitFeedbackDetectionProjection:
                        ExplicitFeedbackDetectionProjection(original)
                )
            )
            let actual = ExplicitBannerFeedbackPrivacy
                .responseIndependentProjection(
                    windows: responseRows,
                    corrections: [event],
                    frozenBaseline: expected
                )

            XCTAssertEqual(
                actual?.map(ExplicitFeedbackDetectionProjection.init),
                expected?.map(ExplicitFeedbackDetectionProjection.init),
                "\(source) must preserve row identity/count/span and every detection diagnostic"
            )
        }
    }

    func testAmbiguousExplicitProjectionFailsClosed() {
        let original = makePrivacyWindow(id: "ambiguous-original")
        let event = CorrectionEvent(
            analysisAssetId: original.analysisAssetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: original.analysisAssetId,
                startTime: original.startTime,
                endTime: original.endTime
            ).serialized,
            source: .bannerSuggestionConfirmed,
            correctionType: .falseNegative,
            targetRefs: CorrectionTargetRefs(
                adWindowId: original.id
            )
        )

        XCTAssertNil(
            ExplicitBannerFeedbackPrivacy.responseIndependentProjection(
                windows: [original],
                corrections: [event]
            )
        )
    }

    func testPreexistingPlaybackStatesProjectButMalformedRowsFailClosed() {
        let original = makePrivacyWindow(id: "fail-closed-original")
        let reverted = makePrivacyWindow(
            id: "legacy-reverted",
            decisionState: AdDecisionState.reverted.rawValue
        )
        let dismissed = makePrivacyWindow(
            id: "legacy-dismissed",
            dismissed: true
        )
        let confirmed = makePrivacyWindow(
            id: "legacy-confirmed",
            boundaryState: "userConfirmedSuggested"
        )
        for legacy in [reverted, dismissed, confirmed] {
            XCTAssertNotNil(
                ExplicitBannerFeedbackPrivacy
                    .responseIndependentProjection(
                        windows: [legacy],
                        corrections: []
                    ),
                "Without a durable private marker, preexisting UX state is part of the unanswered asset and must not tombstone unrelated diagnostics"
            )
        }

        func denial(
            id: String,
            targetIDs: [String],
            projection: AdWindow
        ) -> CorrectionEvent {
            CorrectionEvent(
                id: id,
                analysisAssetId: original.analysisAssetId,
                scope: CorrectionScope.exactTimeSpan(
                    assetId: original.analysisAssetId,
                    startTime: original.startTime,
                    endTime: original.endTime
                ).serialized,
                source: .bannerSuggestionDenied,
                correctionType: .falsePositive,
                targetRefs: CorrectionTargetRefs(
                    adWindowId: targetIDs.first,
                    adWindowIds: targetIDs,
                    explicitFeedbackDetectionProjection:
                        ExplicitFeedbackDetectionProjection(projection)
                )
            )
        }
        let response = makePrivacyWindow(
            id: original.id,
            decisionState: AdDecisionState.reverted.rawValue,
            dismissed: true
        )
        XCTAssertNil(
            ExplicitBannerFeedbackPrivacy.responseIndependentProjection(
                windows: [response],
                corrections: [
                    denial(
                        id: "missing-target",
                        targetIDs: ["not-captured"],
                        projection: original
                    ),
                ]
            )
        )
        XCTAssertNil(
            ExplicitBannerFeedbackPrivacy.responseIndependentProjection(
                windows: [response],
                corrections: [
                    denial(
                        id: "duplicate-target",
                        targetIDs: [original.id, original.id],
                        projection: original
                    ),
                ]
            )
        )
        XCTAssertNil(
            ExplicitBannerFeedbackPrivacy.responseIndependentProjection(
                windows: [response],
                corrections: [
                    denial(
                        id: "overlap-one",
                        targetIDs: [original.id],
                        projection: original
                    ),
                    denial(
                        id: "overlap-two",
                        targetIDs: [original.id],
                        projection: original
                    ),
                ]
            )
        )
        let inconsistent = makePrivacyWindow(
            id: original.id,
            startTime: original.startTime + 1
        )
        XCTAssertNil(
            ExplicitBannerFeedbackPrivacy.responseIndependentProjection(
                windows: [response],
                corrections: [
                    denial(
                        id: "inconsistent-projection",
                        targetIDs: [original.id],
                        projection: inconsistent
                    ),
                ]
            )
        )
    }

    func testPriorListenStateDoesNotBlockLaterAtomicFeedbackBaseline()
        async throws
    {
        let store = try await makeTestStore()
        let asset = makeTestAsset(id: "privacy-asset")
        let priorListen = makePrivacyWindow(
            id: "prior-listen",
            startTime: 30,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        let laterSuggestion = makePrivacyWindow(
            id: "later-suggestion",
            startTime: 120,
            decisionState: AdDecisionState.candidate.rawValue
        )
        try await store.insertAsset(asset)
        try await store.insertAdWindow(priorListen)
        try await store.insertAdWindow(laterSuggestion)
        try await store.persistListenRewind(
            windowId: priorListen.id,
            analysisAssetId: asset.id,
            podcastId: "privacy-podcast",
            createdAt: Date(timeIntervalSince1970: 1_700_000_000)
        )

        let fetchedPriorListen = try await store.fetchAdWindow(
            id: priorListen.id
        )
        let currentPriorListen = try XCTUnwrap(fetchedPriorListen)
        XCTAssertEqual(
            currentPriorListen.decisionState,
            AdDecisionState.reverted.rawValue
        )
        let source = CorrectionSource.bannerSuggestionDenied
        let correction = CorrectionEvent(
            id: "later-explicit-receipt",
            analysisAssetId: asset.id,
            scope: CorrectionScope.exactTimeSpan(
                assetId: asset.id,
                startTime: laterSuggestion.startTime,
                endTime: laterSuggestion.endTime
            ).serialized,
            source: source,
            correctionType: source.kind.correctionType,
            targetRefs: CorrectionTargetRefs(
                adWindowId: laterSuggestion.id,
                adWindowIds: [laterSuggestion.id],
                explicitFeedbackDetectionProjection:
                    ExplicitFeedbackDetectionProjection(
                        laterSuggestion
                    ),
                exactFeedbackSpan: ExactFeedbackSpan(
                    startTime: laterSuggestion.startTime,
                    endTime: laterSuggestion.endTime
                )
            )
        )
        let didPersist = try await store
            .persistDeclinedSuggestionIfCurrent(
                windowId: laterSuggestion.id,
                analysisAssetId: asset.id,
                expectedEpisodeId: asset.episodeId,
                expectedStartTime: laterSuggestion.startTime,
                expectedEndTime: laterSuggestion.endTime,
                expectedProducerRevision: laterSuggestion,
                expectedMaterialToken:
                    AdWindowMaterialIdentity.suggestionToken(
                        laterSuggestion
                    ),
                correction: correction
            )
        XCTAssertEqual(didPersist, true)

        let fetchedProjection =
            try await store.responseIndependentAdWindows(
                analysisAssetId: asset.id
            )
        let projection = try XCTUnwrap(
            fetchedProjection
        )
        XCTAssertEqual(
            Set(projection.map(\.id)),
            Set([priorListen.id, laterSuggestion.id])
        )
        XCTAssertEqual(
            projection.first(where: { $0.id == priorListen.id })?
                .decisionState,
            AdDecisionState.confirmed.rawValue,
            "The unrelated Listen action is normalized to detection-only state"
        )
        let fetchedFrozenAssetProjection =
            try await store.responseIndependentAssetEgressProjection(
                analysisAssetId: asset.id
            )
        let frozenAssetProjection = try XCTUnwrap(
            fetchedFrozenAssetProjection
        )
        XCTAssertEqual(
            frozenAssetProjection.listenRewinds,
            [
                AdListenRewindRow(
                    analysisAssetId: asset.id,
                    windowId: priorListen.id,
                    podcastId: "privacy-podcast",
                    time: priorListen.startTime,
                    createdAt: Date(
                        timeIntervalSince1970: 1_700_000_000
                    )
                ),
            ],
            "A rewind recorded before the first explicit marker is frozen exactly"
        )
        let persistedSuggestion = try await store.fetchAdWindow(
            id: laterSuggestion.id
        )
        XCTAssertEqual(
            persistedSuggestion?.decisionState,
            AdDecisionState.reverted.rawValue,
            "The later local response still commits"
        )

        try await store.execForTesting(
            "DELETE FROM correction_events WHERE analysisAssetId = '\(asset.id)'"
        )
        let markerOnlyProjection =
            try await store.responseIndependentAdWindows(
                analysisAssetId: asset.id
            )
        XCTAssertEqual(
            markerOnlyProjection?.map(\.id),
            projection.map(\.id),
            "The captured marker remains authoritative after receipt loss"
        )
        let fetchedMarkerOnlyAssetProjection =
            try await store.responseIndependentAssetEgressProjection(
                analysisAssetId: asset.id
            )
        let markerOnlyAssetProjection = try XCTUnwrap(
            fetchedMarkerOnlyAssetProjection
        )
        XCTAssertEqual(
            markerOnlyAssetProjection.listenRewinds,
            frozenAssetProjection.listenRewinds,
            "Receipt loss cannot trigger a live rewind fallback"
        )
    }

    func testFrozenRewindsSurviveTargetDeletionAndSameIDForeignReuse()
        async throws
    {
        let store = try await makeTestStore()
        let asset = makeTestAsset(id: "privacy-asset")
        let target = makePrivacyWindow(
            id: "reused-window-id",
            decisionState: AdDecisionState.candidate.rawValue
        )
        try await store.insertAsset(asset)
        try await store.insertAdWindow(target)
        let originalRewind = AdListenRewindRow(
            analysisAssetId: asset.id,
            windowId: target.id,
            podcastId: "original-podcast",
            time: target.startTime,
            createdAt: Date(timeIntervalSince1970: 1_700_000_100)
        )
        try await store.insertListenRewind(
            windowId: originalRewind.windowId,
            analysisAssetId: originalRewind.analysisAssetId,
            podcastId: originalRewind.podcastId,
            time: originalRewind.time,
            createdAt: originalRewind.createdAt
        )

        let source = CorrectionSource.bannerSuggestionDenied
        let correction = CorrectionEvent(
            id: "same-id-reuse-receipt",
            analysisAssetId: asset.id,
            scope: CorrectionScope.exactTimeSpan(
                assetId: asset.id,
                startTime: target.startTime,
                endTime: target.endTime
            ).serialized,
            source: source,
            correctionType: source.kind.correctionType,
            targetRefs: CorrectionTargetRefs(
                adWindowId: target.id,
                adWindowIds: [target.id],
                explicitFeedbackDetectionProjection:
                    ExplicitFeedbackDetectionProjection(target),
                exactFeedbackSpan: ExactFeedbackSpan(
                    startTime: target.startTime,
                    endTime: target.endTime
                )
            )
        )
        let didPersist =
            try await store.persistDeclinedSuggestionIfCurrent(
                windowId: target.id,
                analysisAssetId: asset.id,
                expectedEpisodeId: asset.episodeId,
                expectedStartTime: target.startTime,
                expectedEndTime: target.endTime,
                expectedProducerRevision: target,
                expectedMaterialToken:
                    AdWindowMaterialIdentity.suggestionToken(target),
                correction: correction
            )
        XCTAssertEqual(didPersist, true)

        // Delete the original owner, then reuse the globally unique window ID
        // for another asset. A live ownership JOIN would now misattribute the
        // original row to the replacement asset before it records any rewind.
        try await store.execForTesting(
            "DELETE FROM ad_windows WHERE id = '\(target.id)'"
        )
        let foreignAsset = makeTestAsset(id: "foreign-asset")
        let foreignWindow = makePrivacyWindow(
            id: target.id,
            analysisAssetId: foreignAsset.id,
            startTime: 240,
            boundaryState: "foreign-boundary"
        )
        try await store.insertAsset(foreignAsset)
        try await store.insertAdWindow(foreignWindow)

        let fetchedForeignBeforeOwnRewind =
            try await store.responseIndependentAssetEgressProjection(
                analysisAssetId: foreignAsset.id
            )
        let foreignBeforeOwnRewind = try XCTUnwrap(
            fetchedForeignBeforeOwnRewind
        )
        XCTAssertTrue(
            foreignBeforeOwnRewind.listenRewinds.isEmpty,
            "A reused window ID must not make A's orphan rewind B-owned"
        )

        let corpusDirectory = try makeTempDir(
            prefix: "CorpusExport-reused-rewind-owner"
        )
        let corpus = try await CorpusExporter.export(
            store: store,
            documentsURL: corpusDirectory,
            dedupMemo: CorpusExportDedupMemo()
        )
        let corpusData = try Data(contentsOf: corpus.fileURL)
        let foreignCorpusRewinds = try corpusData.split(
            separator: 0x0A
        ).compactMap { line -> [String: Any]? in
            let object = try JSONSerialization.jsonObject(
                with: Data(line)
            )
            guard let json = object as? [String: Any],
                  json["type"] as? String == "listen_rewind",
                  json["analysisAssetId"] as? String
                    == foreignAsset.id
            else {
                return nil
            }
            return json
        }
        XCTAssertTrue(
            foreignCorpusRewinds.isEmpty,
            "B Corpus must exclude A's orphan podcast and rewind"
        )

        let foreignRewind = AdListenRewindRow(
            analysisAssetId: foreignAsset.id,
            windowId: foreignWindow.id,
            podcastId: "foreign-podcast",
            time: foreignWindow.startTime,
            createdAt: Date(timeIntervalSince1970: 1_700_000_200)
        )
        try await store.insertListenRewind(
            windowId: foreignRewind.windowId,
            analysisAssetId: foreignRewind.analysisAssetId,
            podcastId: foreignRewind.podcastId,
            time: foreignRewind.time,
            createdAt: foreignRewind.createdAt
        )
        let fetchedForeignWithOwnRewind =
            try await store.responseIndependentAssetEgressProjection(
                analysisAssetId: foreignAsset.id
            )
        let foreignWithOwnRewind = try XCTUnwrap(
            fetchedForeignWithOwnRewind
        )
        XCTAssertEqual(
            foreignWithOwnRewind.listenRewinds,
            [foreignRewind]
        )

        let fetchedFrozen =
            try await store.responseIndependentAssetEgressProjection(
                analysisAssetId: asset.id
            )
        let frozen = try XCTUnwrap(
            fetchedFrozen
        )
        XCTAssertEqual(frozen.listenRewinds, [originalRewind])

        let foreignSource = CorrectionSource.bannerSuggestionDenied
        let foreignCorrection = CorrectionEvent(
            id: "foreign-same-id-reuse-receipt",
            analysisAssetId: foreignAsset.id,
            scope: CorrectionScope.exactTimeSpan(
                assetId: foreignAsset.id,
                startTime: foreignWindow.startTime,
                endTime: foreignWindow.endTime
            ).serialized,
            source: foreignSource,
            correctionType:
                foreignSource.kind.correctionType,
            targetRefs: CorrectionTargetRefs(
                adWindowId: foreignWindow.id,
                adWindowIds: [foreignWindow.id],
                explicitFeedbackDetectionProjection:
                    ExplicitFeedbackDetectionProjection(
                        foreignWindow
                    ),
                exactFeedbackSpan: ExactFeedbackSpan(
                    startTime: foreignWindow.startTime,
                    endTime: foreignWindow.endTime
                )
            )
        )
        let foreignDidPersist =
            try await store.persistDeclinedSuggestionIfCurrent(
                windowId: foreignWindow.id,
                analysisAssetId: foreignAsset.id,
                expectedEpisodeId: foreignAsset.episodeId,
                expectedStartTime: foreignWindow.startTime,
                expectedEndTime: foreignWindow.endTime,
                expectedProducerRevision: foreignWindow,
                expectedMaterialToken:
                    AdWindowMaterialIdentity.suggestionToken(
                        foreignWindow
                    ),
                correction: foreignCorrection
            )
        XCTAssertEqual(foreignDidPersist, true)

        try await store.execForTesting("""
            DELETE FROM correction_events
            WHERE analysisAssetId = '\(foreignAsset.id)'
            """)
        let fetchedMarkerOnlyForeign =
            try await store.responseIndependentAssetEgressProjection(
                analysisAssetId: foreignAsset.id
            )
        let markerOnlyForeign = try XCTUnwrap(
            fetchedMarkerOnlyForeign
        )
        XCTAssertEqual(
            markerOnlyForeign.listenRewinds,
            [foreignRewind],
            "B's frozen baseline stays B-clean after receipt deletion"
        )
        let frozenCorpusDirectory = try makeTempDir(
            prefix: "CorpusExport-frozen-reused-rewind-owner"
        )
        let frozenCorpus = try await CorpusExporter.export(
            store: store,
            documentsURL: frozenCorpusDirectory,
            dedupMemo: CorpusExportDedupMemo()
        )
        let frozenCorpusData = try Data(
            contentsOf: frozenCorpus.fileURL
        )
        let frozenForeignCorpusRewinds = try frozenCorpusData.split(
            separator: 0x0A
        ).compactMap { line -> [String: Any]? in
            let object = try JSONSerialization.jsonObject(
                with: Data(line)
            )
            guard let json = object as? [String: Any],
                  json["type"] as? String == "listen_rewind",
                  json["analysisAssetId"] as? String
                    == foreignAsset.id
            else {
                return nil
            }
            return json
        }
        XCTAssertEqual(frozenForeignCorpusRewinds.count, 1)
        XCTAssertEqual(
            frozenForeignCorpusRewinds.first?["podcastId"]
                as? String,
            foreignRewind.podcastId
        )
    }

    func testCorruptOrMissingBaselineNeverFallsBackToLiveRows()
        async throws
    {
        let store = try await makeTestStore()
        let asset = makeTestAsset(id: "privacy-asset")
        let window = makePrivacyWindow(
            id: "baseline-integrity-window",
            decisionState: AdDecisionState.confirmed.rawValue
        )
        try await store.insertAsset(asset)
        try await store.insertAdWindow(window)
        let crossAssetPayload =
            ExplicitFeedbackEgressBaselinePayload(
                analysisAssetId: asset.id,
                confirmedAdCoverageEndTime: nil,
                windows: [
                    ExplicitFeedbackDetectionProjection(window),
                ],
                decodedSpans: [],
                listenRewinds: [
                    AdListenRewindRow(
                        analysisAssetId: "foreign-asset",
                        windowId: window.id,
                        podcastId: "foreign-podcast",
                        time: window.startTime,
                        createdAt: Date(
                            timeIntervalSince1970: 1_700_000_299
                        )
                    ),
                ]
            )
        XCTAssertNil(
            ExplicitBannerFeedbackPrivacy
                .validatedBaselineListenRewinds(
                    crossAssetPayload,
                    expectedAssetId: asset.id,
                    expectedWindowIDs: [window.id]
                ),
            "An authenticated payload cannot carry a foreign-owned rewind"
        )
        try await store.insertListenRewind(
            windowId: window.id,
            analysisAssetId: asset.id,
            podcastId: "integrity-podcast",
            time: window.startTime,
            createdAt: Date(timeIntervalSince1970: 1_700_000_300)
        )
        let source = CorrectionSource.bannerAutoSkipConfirmed
        let receipt = CorrectionEvent(
            id: "baseline-integrity-receipt",
            analysisAssetId: asset.id,
            scope: CorrectionScope.exactTimeSpan(
                assetId: asset.id,
                startTime: window.startTime,
                endTime: window.endTime
            ).serialized,
            source: source,
            correctionType: source.kind.correctionType,
            targetRefs: CorrectionTargetRefs(
                adWindowId: window.id,
                adWindowIds: [window.id],
                explicitFeedbackDetectionProjection:
                    ExplicitFeedbackDetectionProjection(window),
                exactFeedbackSpan: ExactFeedbackSpan(
                    startTime: window.startTime,
                    endTime: window.endTime
                )
            )
        )
        let didPersist = try await store.persistConfirmedAutoSkip(
                windowId: window.id,
                analysisAssetId: asset.id,
                expectedEpisodeId: asset.episodeId,
                expectedStartTime: window.startTime,
                expectedEndTime: window.endTime,
                expectedProducerRevision: window,
                expectedMaterialToken:
                    AdWindowMaterialIdentity.autoSkipToken(
                        window: window,
                        displayedStart: window.startTime,
                        displayedEnd: window.endTime
                    ),
                correction: receipt
        )
        XCTAssertEqual(didPersist, true)

        try await store.execForTesting("""
            UPDATE explicit_feedback_egress_baselines
            SET listenRewindCount = listenRewindCount + 1
            WHERE analysisAssetId = '\(asset.id)'
            """)
        do {
            _ = try await store.responseIndependentAssetEgressProjection(
                analysisAssetId: asset.id
            )
            XCTFail("A corrupt authenticated rewind count must throw")
        } catch {
            // Expected: no live fallback.
        }

        try await store.execForTesting("""
            UPDATE explicit_feedback_egress_baselines
            SET listenRewindCount = listenRewindCount - 1,
                payloadJSON = '{}'
            WHERE analysisAssetId = '\(asset.id)'
            """)
        do {
            _ = try await store.responseIndependentAssetEgressProjection(
                analysisAssetId: asset.id
            )
            XCTFail("A corrupt authenticated rewind payload must throw")
        } catch {
            // Expected: no live fallback.
        }

        try await store.execForTesting("""
            UPDATE explicit_feedback_egress_baselines
            SET payloadSHA256 = 'invalid'
            WHERE analysisAssetId = '\(asset.id)'
            """)
        do {
            _ = try await store.responseIndependentAdWindows(
                analysisAssetId: asset.id
            )
            XCTFail("A corrupt authenticated payload must throw")
        } catch {
            // Expected: no live fallback.
        }

        try await store.execForTesting("""
            DELETE FROM explicit_feedback_egress_baselines
            WHERE analysisAssetId = '\(asset.id)'
            """)
        let projectionWithoutBaseline =
            try await store.responseIndependentAdWindows(
                analysisAssetId: asset.id
            )
        XCTAssertNil(
            projectionWithoutBaseline,
            "A surviving durable private receipt with no baseline must fail closed"
        )
    }

    private func makePrivacyWindow(
        id: String,
        analysisAssetId: String = "privacy-asset",
        startTime: Double = 30,
        skipConfidence: Double? = nil,
        boundaryState: String = "privacy-boundary",
        decisionState: String = AdDecisionState.candidate.rawValue,
        wasSkipped: Bool = false,
        dismissed: Bool = false
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: analysisAssetId,
            startTime: startTime,
            endTime: startTime + 45,
            confidence: 0.876,
            skipConfidence: skipConfidence,
            boundaryState: boundaryState,
            decisionState: decisionState,
            detectorVersion: "privacy-detector",
            advertiser: "Detection Sponsor",
            product: "Detection Product",
            adDescription: "Detection Description",
            evidenceText: "Detection Evidence",
            evidenceStartTime: startTime + 2,
            metadataSource: "privacy-source",
            metadataConfidence: 0.765,
            metadataPromptVersion: "privacy-prompt",
            wasSkipped: wasSkipped,
            userDismissedBanner: dismissed,
            evidenceSources: "lexical,acoustic",
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            catalogStoreMatchSimilarity: 0.654,
            startEdgeAnchor: "privacy-start",
            endEdgeAnchor: "privacy-end"
        )
    }
}
