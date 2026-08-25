// SemanticScanVerdictProvenanceV61MigrationTests.swift
// playhead-iw7q: pin the V61 change that stops a PERMISSIVE coarse row being
// byte-identical at rest to one the model actually produced.
//
// WHAT WENT WRONG, so the rails below read as answers to a question.
//
// `PermissiveAdGrammar.parse` writes a hardcoded `certainty: .strong` into the
// `passA` payload of every `containsAd` it returns, whatever the model said —
// its own header says so: "the FM never inferred these classification
// dimensions, the runner is hardcoding them." The REFINED half of that
// fabrication has recorded itself since playhead-92im
// (`ownershipInferenceWasSuppressed: true` on every `makeAnchorlessSpan`).
//
// The COARSE half recorded nothing. `SemanticScanResult.usedPermissiveFallback`
// EXISTED and had NO COLUMN in `semantic_scan_results`, so it was computed by
// the runner, carried on the struct, and dropped at the write. Three consumers
// inherited the fabricated grade — the sweep composer's certainty factor
// (ceiling), `AdDetectionService.buildFMLedgerEntries`' fusion weight
// (playhead-yx0f), and playhead-6ruv's attribution, which could name a brand
// and could never certify that a `.unrefined` mark's PRESENCE verdict was the
// model's. Three separate places in the tree said the gap was "filed
// separately" and no such bead existed.
//
// The directions this file has to cover, because closing one leaves the defect
// alive in the others:
//
//   1. THE COLUMN EXISTS AND THE FLAG REACHES DISK. A `.permissive` row is
//      distinguishable at rest from a `.model` one, in the BYTES rather than
//      through the reader whose correctness is also under test.
//   2. UNKNOWN IS NOT ZERO — the backfill decision, pinned in both directions.
//      A pre-V61 row arrives at V61 with a NULL, decodes to `.unknown`, and
//      `.unknown` is NOT `.model`. A migration that seeded 0 would certify
//      1,406 coarse rows on the 2026-08-21 t6 pull as the model's own: this
//      bead's own defect, inverted and shipped as a migration.
//   3. THE THREE STATES SURVIVE INTO THE READER, and neither of the two
//      predicates a consumer can ask is the other's negation.
//   4. THE REPLACE TAKES THE INCOMING VALUE. `disposition` and `spansJSON` are
//      overwritten by the same statement, so carrying the old provenance
//      forward would attribute one attempt's path to another attempt's band —
//      and that is the OPPOSITE rule to `createdAt` / `firstAttemptAt`, which
//      are properties of the row's history rather than of its verdict.
//   5. THE LADDER REACHES HEAD from a fixture that has no table at all.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("a PERMISSIVE coarse row stops being byte-identical to a genuine one (playhead-iw7q)")
struct SemanticScanVerdictProvenanceV61MigrationTests {

    private static let cohort = """
        {"promptLabel":"l","promptHash":"p","schemaHash":"s","scanPlanHash":"sp",\
        "normalizationHash":"n","osBuild":"26A","locale":"en_US","appBuild":"1"}
        """

    /// The exact payload `BackfillJobRunner.encodeSupport` writes for a
    /// permissive coarse screening, and — byte for byte — for a genuine one.
    /// That IS the defect: nothing here distinguishes them.
    private static let coarseStrongPayload = #"{"supportLineRefs":[46],"certainty":"strong"}"#

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "SemanticScanVerdictProvenanceV61")
    }

    // MARK: - Raw-column probe
    //
    // Deliberately NOT routed through `AnalysisStore` for the disk claims, for
    // the sibling V55/V56 suites' reason: the claim is about what is ON DISK,
    // and asking the store would ask the same read whose correctness is under
    // test. A matched pair of bugs in the bind and the read would agree
    // perfectly, which is exactly how a column can look present and be inert.

    /// `.some(nil)` = the row exists and the column is NULL.
    /// `nil`        = there is no such row.
    private func rawFlag(in directory: URL, rowId: String) throws -> Int?? {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "rawFlag", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        let sql = "SELECT usedPermissiveFallback FROM semantic_scan_results WHERE id = ?"
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(domain: "rawFlag", code: 2)
        }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, (rowId as NSString).utf8String, -1, nil)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        if sqlite3_column_type(stmt, 0) == SQLITE_NULL { return .some(nil) }
        return .some(Int(sqlite3_column_int(stmt, 0)))
    }

    private func columnExists(in directory: URL, table: String, column: String) throws -> Bool {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "columnExists", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, "PRAGMA table_info(\(table))", -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(domain: "columnExists", code: 2)
        }
        defer { sqlite3_finalize(stmt) }
        while sqlite3_step(stmt) == SQLITE_ROW {
            if let name = sqlite3_column_text(stmt, 1), String(cString: name) == column {
                return true
            }
        }
        return false
    }

    /// Rewind a migrated store to the V60 SHAPE, not merely the V60 version
    /// stamp — the sibling V52/V55/V56 rule, and for its reason: a rewind that
    /// only touches `_meta` proves nothing, because the rung would then run
    /// against a column that already exists and the work under test would be
    /// indistinguishable from a no-op.
    ///
    /// Pinned to the LITERAL 60 — "pre-iw7q" is a fixed historical fact, and
    /// `currentSchemaVersion - 1` would stop meaning it the moment head moves.
    private func rewindToV60(_ store: AnalysisStore) async throws {
        try await store.execForTesting(
            "ALTER TABLE semantic_scan_results DROP COLUMN usedPermissiveFallback"
        )
        try await store.setMetaValue(forKey: "schema_version", value: "60")
    }

    // MARK: - Fixtures

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func coarseRow(
        id: String,
        assetId: String,
        provenance: ScanVerdictProvenance,
        status: SemanticScanStatus = .success,
        spansJSON: String = SemanticScanVerdictProvenanceV61MigrationTests.coarseStrongPayload,
        window: Int = 0
    ) -> SemanticScanResult {
        // The reuse key is built from the ATOM ORDINALS, not the times, so two
        // fixture rows meant to coexist must differ here — otherwise
        // `UNIQUE(reuseKeyHash)` makes the second an upsert of the first and
        // the assertion under test is about a row that no longer exists.
        SemanticScanResult(
            id: id,
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: window * 100,
            windowLastAtomOrdinal: window * 100 + 10,
            windowStartTime: Double(window) * 100,
            windowEndTime: Double(window) * 100 + 42.9,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: spansJSON,
            status: status,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            scanCohortJSON: Self.cohort,
            transcriptVersion: "tv-1",
            verdictProvenance: provenance,
            createdAt: 1_755_147_470.0
        )
    }

    // MARK: - 1. The column exists and the flag reaches disk

    @Test("the two rows that used to be identical now differ IN THE BYTES")
    func permissiveAndModelRowsDifferOnDisk() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-b"))

        // Byte-identical `spansJSON`, byte-identical everything else. Before
        // V61 these two were the same row on disk.
        try await store.insertSemanticScanResult(
            coarseRow(id: "scan-model", assetId: "asset-b", provenance: .model, window: 0)
        )
        try await store.insertSemanticScanResult(
            coarseRow(id: "scan-perm", assetId: "asset-b", provenance: .permissive, window: 1)
        )

        #expect(try rawFlag(in: dir, rowId: "scan-model") == .some(0))
        #expect(try rawFlag(in: dir, rowId: "scan-perm") == .some(1))
    }

    @Test("an UNKNOWN provenance writes SQL NULL, not 0")
    func unknownWritesNull() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-u"))
        try await store.insertSemanticScanResult(
            coarseRow(id: "scan-u", assetId: "asset-u", provenance: .unknown)
        )

        // `?? 0` in the bind would write 0 here, and 0 means `.model`. The
        // whole distinction lives on this one line of the writer.
        #expect(try rawFlag(in: dir, rowId: "scan-u") == .some(Int?.none))
    }

    @Test("the flag round-trips through the store for all three states")
    func roundTrip() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-rt"))

        for (index, provenance) in ScanVerdictProvenance.allCases.enumerated() {
            try await store.insertSemanticScanResult(
                coarseRow(
                    id: "scan-rt-\(provenance.rawValue)",
                    assetId: "asset-rt",
                    provenance: provenance,
                    window: index
                )
            )
        }
        for provenance in ScanVerdictProvenance.allCases {
            let row = try #require(
                try await store.fetchSemanticScanResult(id: "scan-rt-\(provenance.rawValue)")
            )
            #expect(row.verdictProvenance == provenance)
        }
    }

    // MARK: - 2. UNKNOWN IS NOT ZERO — the backfill decision

    @Test("a pre-V61 row arrives at V61 as UNKNOWN, and the column is NULL")
    func migrationDoesNotBackfill() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-pre"))
        try await store.insertSemanticScanResult(
            coarseRow(id: "scan-pre", assetId: "asset-pre", provenance: .model)
        )
        // Drop the column and the stamp: this row is now indistinguishable from
        // one a pre-iw7q binary wrote, which is the population the migration
        // meets.
        try await rewindToV60(store)
        #expect(try columnExists(in: dir, table: "semantic_scan_results",
                                 column: "usedPermissiveFallback") == false)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(try columnExists(in: dir, table: "semantic_scan_results",
                                 column: "usedPermissiveFallback"))
        // THE BACKFILL DECISION. A `DEFAULT 0` or a seeding UPDATE would read
        // `.some(0)` here and would say "the model graded this" about a row
        // nobody can speak for.
        #expect(try rawFlag(in: dir, rowId: "scan-pre") == .some(Int?.none))

        let row = try #require(try await reopened.fetchSemanticScanResult(id: "scan-pre"))
        #expect(row.verdictProvenance == .unknown)
        #expect(row.verdictProvenance != .model, "unknown is not zero")
        #expect(row.verdictProvenance != .permissive, "and it is not the other one either")
    }

    @Test("UNKNOWN licenses nothing: the coarse band a pre-V61 row carries is NOT attributable")
    func unknownDoesNotLicenseTheBand() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-band"))
        try await store.insertSemanticScanResult(
            coarseRow(id: "scan-band", assetId: "asset-band", provenance: .model)
        )
        try await rewindToV60(store)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        let row = try #require(try await reopened.fetchSemanticScanResult(id: "scan-band"))

        // The payload still SAYS `strong`. What changed is that nothing on the
        // row licenses reading it as the model's.
        #expect(row.spansJSON.contains("strong"))
        #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == nil)
        #expect(SemanticSweepMarkComposer.certaintyFactor(of: row)
                == SemanticSweepMarkComposer.certaintyFactor(nil))
    }

    @Test("the migration is idempotent and never overwrites a value already recorded")
    func migrationIsIdempotent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-idem"))
        try await store.insertSemanticScanResult(
            coarseRow(id: "scan-idem", assetId: "asset-idem", provenance: .permissive)
        )

        // Stamp back WITHOUT dropping the column: the shape a store has if the
        // rung is re-entered.
        try await store.setMetaValue(forKey: "schema_version", value: "60")
        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(try rawFlag(in: dir, rowId: "scan-idem") == .some(1),
                "a re-run must not reset a recorded provenance")
        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)
    }

    // MARK: - 3. Three states, and two predicates that are not each other

    @Test("decoded(persistedFlag:) maps the three column states onto the three cases")
    func decodeIsTotal() {
        #expect(ScanVerdictProvenance.decoded(persistedFlag: nil) == .unknown)
        #expect(ScanVerdictProvenance.decoded(persistedFlag: false) == .model)
        #expect(ScanVerdictProvenance.decoded(persistedFlag: true) == .permissive)
    }

    @Test("persistedFlag round-trips every case, and only .unknown is NULL")
    func persistedFlagRoundTrips() {
        for provenance in ScanVerdictProvenance.allCases {
            #expect(ScanVerdictProvenance.decoded(persistedFlag: provenance.persistedFlag) == provenance)
        }
        #expect(ScanVerdictProvenance.unknown.persistedFlag == nil)
        #expect(ScanVerdictProvenance.model.persistedFlag == false)
        #expect(ScanVerdictProvenance.permissive.persistedFlag == true)
    }

    @Test("the two predicates are not each other's negation — unknown is false for BOTH")
    func predicatesAreNotComplementary() {
        #expect(ScanVerdictProvenance.model.licensesCoarseCertaintyBand)
        #expect(ScanVerdictProvenance.permissive.licensesCoarseCertaintyBand == false)
        #expect(ScanVerdictProvenance.unknown.licensesCoarseCertaintyBand == false)

        #expect(ScanVerdictProvenance.permissive.isKnownPermissive)
        #expect(ScanVerdictProvenance.model.isKnownPermissive == false)
        // THE POINT. A telemetry reader that counted permissive rows as
        // `!licensesCoarseCertaintyBand` would book every unattributed row as a
        // bypass — the same substitution, one column along.
        #expect(ScanVerdictProvenance.unknown.isKnownPermissive == false)

        let exactlyOne = ScanVerdictProvenance.allCases.filter(\.licensesCoarseCertaintyBand)
        #expect(exactlyOne == [.model])
        let exactlyOnePermissive = ScanVerdictProvenance.allCases.filter(\.isKnownPermissive)
        #expect(exactlyOnePermissive == [.permissive])
    }

    @Test("init(observedPermissiveFallback:) never produces .unknown")
    func observationNeverProducesUnknown() {
        #expect(ScanVerdictProvenance(observedPermissiveFallback: true) == .permissive)
        #expect(ScanVerdictProvenance(observedPermissiveFallback: false) == .model)
    }

    @Test("the struct's own default is .unknown, not .model")
    func structDefaultIsUnknown() {
        let row = SemanticScanResult(
            id: "scan-default",
            analysisAssetId: "asset-default",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1,
            windowStartTime: 0,
            windowEndTime: 10,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: Self.coarseStrongPayload,
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            scanCohortJSON: Self.cohort,
            transcriptVersion: "tv-1"
        )
        #expect(row.verdictProvenance == .unknown)
        // A writer that says nothing withholds the licence rather than granting
        // it — the under-claiming direction.
        #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == nil)
    }

    // MARK: - 4. The replace takes the INCOMING value

    @Test("a replace overwrites the provenance, because it overwrites the verdict")
    func replaceTakesTheIncomingProvenance() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-rep"))

        // Same geometry ⇒ same `reuseKeyHash` ⇒ the second write replaces the
        // first. The `@Generable` path failed and the bypass answered.
        try await store.insertSemanticScanResult(
            coarseRow(id: "scan-rep", assetId: "asset-rep", provenance: .model,
                      status: .decodingFailure, spansJSON: "[]")
        )
        try await store.insertSemanticScanResult(
            coarseRow(id: "scan-rep", assetId: "asset-rep", provenance: .permissive)
        )

        #expect(try rawFlag(in: dir, rowId: "scan-rep") == .some(1))
        let row = try #require(try await store.fetchSemanticScanResult(id: "scan-rep"))
        // The payload is the SECOND attempt's, so the provenance must be too.
        // Preserving `.model` beside a bypass-written `strong` would rebuild
        // this bead's defect out of two correct-looking writes.
        #expect(row.verdictProvenance == .permissive)
        #expect(row.spansJSON == Self.coarseStrongPayload)
        // …and the HISTORY columns follow the opposite rule, unchanged.
        #expect(row.createdAt == 1_755_147_470.0)
        #expect(row.attemptCount == 2)
    }

    @Test("a replace can also LOSE a provenance, and NULL is the honest answer")
    func replaceCanReturnToUnknown() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-lose"))

        try await store.insertSemanticScanResult(
            coarseRow(id: "scan-lose", assetId: "asset-lose", provenance: .permissive,
                      status: .decodingFailure, spansJSON: "[]")
        )
        // A writer with nothing to say about the NEW payload. Carrying the old
        // `.permissive` forward would label THIS payload with the PREVIOUS
        // attempt's path — a value that names one thing read as though it named
        // another, which is the class of defect this column closes.
        try await store.insertSemanticScanResult(
            coarseRow(id: "scan-lose", assetId: "asset-lose", provenance: .unknown)
        )

        #expect(try rawFlag(in: dir, rowId: "scan-lose") == .some(Int?.none))
        let row = try #require(try await store.fetchSemanticScanResult(id: "scan-lose"))
        #expect(row.verdictProvenance == .unknown)
    }

    // MARK: - 5. The ladder

    @Test("a fixture with no semantic_scan_results still reaches head")
    func migrationSkipsMissingTable() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.execForTesting("DROP TABLE semantic_scan_results")
        try await store.setMetaValue(forKey: "schema_version", value: "60")

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)
    }

    @Test("a fresh install and an upgrade converge on the same shape")
    func freshInstallHasTheColumn() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try columnExists(in: dir, table: "semantic_scan_results",
                                 column: "usedPermissiveFallback"))
        // playhead-7dgx: 62, not 61. This line reads the version off the
        // DATABASE, so the sweep that bumps the `currentSchemaVersion`
        // guards cannot see it — the trap the V44 suite's own comment
        // records, hit again one rung later. V62 creates two NEW tables and
        // touches no existing column, so nothing else here moves.
        // playhead-4xmz: 63, not 62. Same line, same trap, same reason — V63
        // creates two NEW tables (`download_work_journal` and its arming row)
        // and touches no existing column.
        #expect(try await store.schemaVersion() == 63)
    }
}
