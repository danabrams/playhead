// PrewarmHitUnmeasuredV52MigrationTests.swift
// playhead-exxc: pin the V52 change that turns
// `semantic_scan_results.prewarmHit` from a NOT NULL boolean nobody measured
// into a NULLABLE one that says so.
//
// WHAT WENT WRONG, so the rails below read as answers to a question.
// The 2026-08-11 virgin-DB overnight pull reported `prewarmHit: {0: 95}` and it
// was quoted as "every Foundation Models call overnight was a cold start". The
// column could not have read anything else. `prewarmHit` is a field of the
// PASS-level `FMCoarseScanOutput` / `FMRefinementScanOutput`; every row in this
// table is built from a WINDOW-level `FMCoarseWindowOutput` /
// `FMRefinementWindowOutput`, which has no such field. So all four builders in
// `BackfillJobRunner` passed the literal `false` — the only value typeable at a
// site with nothing to read — and `git log -S "prewarmHit: true" -- Playhead/`
// is empty across the repository's whole history. `{0: 95}` measured the
// instrument.
//
// The two directions this file has to cover, because closing one leaves the
// defect fully alive in the other:
//
//   1. THE COLUMN CAN NOW HOLD "UNMEASURED". A nullable column plus a
//      `map`-based bind and an `optionalInt`-based read. If any of the three
//      regresses, a nil comes back as `false` and the row is claiming a cold
//      start again.
//   2. THE PRODUCTION PATH ACTUALLY WRITES IT. This is the direction a unit
//      test never reaches on its own: every rail in (1) passes with all four
//      builders still stamping `prewarmHit: false`, because they would simply
//      be exercising a capability nobody uses — the exact shape (a mechanism
//      with no production consumer) that this bead's queue has now hit five
//      times. `productionBuildersNameNoPrewarmHit` is the rail for it, and it
//      is a SOURCE canary rather than a behavioural one because `Bool?` still
//      lets an author type `false`: making the wrong value un-typeable is not
//      possible here, so the rail names the sites instead.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("semantic_scan_results.prewarmHit is unmeasured, and V52 says so (playhead-exxc)")
struct PrewarmHitUnmeasuredV52MigrationTests {

    private static let cohort = """
        {"promptLabel":"l","promptHash":"p","schemaHash":"s","scanPlanHash":"sp",\
        "normalizationHash":"n","osBuild":"26A","locale":"en_US","appBuild":"1"}
        """

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "PrewarmHitUnmeasuredV52")
    }

    // MARK: - Raw-column probes
    //
    // Deliberately NOT routed through `AnalysisStore`. The whole claim is about
    // what is ON DISK; asking the store would ask the same `optionalInt` read
    // whose correctness is under test, and a matched pair of bugs in the bind
    // and the read would agree with each other perfectly.

    /// `true` when the named column is declared NULLABLE.
    private func columnIsNullable(in directory: URL, table: String, column: String) throws -> Bool {
        try withReadOnlyHandle(in: directory) { db in
            var stmt: OpaquePointer?
            guard sqlite3_prepare_v2(db, "PRAGMA table_info(\(table))", -1, &stmt, nil) == SQLITE_OK else {
                throw NSError(domain: "columnIsNullable", code: 1)
            }
            defer { sqlite3_finalize(stmt) }
            while sqlite3_step(stmt) == SQLITE_ROW {
                guard let name = sqlite3_column_text(stmt, 1),
                      String(cString: name) == column else { continue }
                // PRAGMA table_info column 3 is `notnull`.
                return sqlite3_column_int(stmt, 3) == 0
            }
            throw NSError(domain: "columnIsNullable", code: 2)
        }
    }

    /// The raw stored value of `prewarmHit` for one row: `.some(nil)` for SQL
    /// NULL, `.some(.some(n))` for an integer, `nil` when the row is missing.
    private func rawPrewarmHit(in directory: URL, rowId: String) throws -> Int?? {
        try withReadOnlyHandle(in: directory) { db in
            var stmt: OpaquePointer?
            let sql = "SELECT prewarmHit FROM semantic_scan_results WHERE id = ?"
            guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
                throw NSError(domain: "rawPrewarmHit", code: 1)
            }
            defer { sqlite3_finalize(stmt) }
            sqlite3_bind_text(stmt, 1, rowId, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self))
            guard sqlite3_step(stmt) == SQLITE_ROW else { return Int??.none }
            if sqlite3_column_type(stmt, 0) == SQLITE_NULL {
                return Int??.some(nil)
            }
            return Int??.some(Int(sqlite3_column_int(stmt, 0)))
        }
    }

    private func withReadOnlyHandle<T>(in directory: URL, _ body: (OpaquePointer?) throws -> T) throws -> T {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "withReadOnlyHandle", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        return try body(db)
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

    /// A row shaped exactly as the production builders now shape it: every
    /// optional the caller cannot observe is simply not passed.
    private func unmeasuredRow(id: String, assetId: String) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 10,
            windowStartTime: 0,
            windowEndTime: 60,
            scanPass: "passA",
            transcriptQuality: .good,
            disposition: .noAds,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: 1_234,
            scanCohortJSON: Self.cohort,
            transcriptVersion: "tv-1"
        )
    }

    // MARK: - 1. The column can hold "unmeasured"

    @Test("an omitted prewarmHit defaults to nil, not to false")
    func omittedArgumentIsNilNotFalse() throws {
        let row = unmeasuredRow(id: "row-default", assetId: "asset-default")
        // The parameter carries `= nil`. If a future edit restores `= false`,
        // every production row silently claims a measured cold start again and
        // nothing else in this file would notice: the disk rails below would
        // keep passing, writing a perfectly well-formed 0.
        #expect(row.prewarmHit == nil)
    }

    @Test("fresh DB: prewarmHit is nullable at head, and head is 52")
    func freshDbColumnIsNullable() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        // Pinned to the LITERAL head so the next schema bump has to read this
        // rung, matching the convention of every sibling migration suite.
        #expect(AnalysisStore.currentSchemaVersion == 52)
        #expect(try columnIsNullable(in: dir, table: "semantic_scan_results", column: "prewarmHit"))
    }

    @Test("a row written with no prewarmHit stores SQL NULL and reads back nil")
    func unmeasuredRowRoundTripsAsNull() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-null"))
        try await store.insertSemanticScanResult(unmeasuredRow(id: "row-null", assetId: "asset-null"))

        // On disk, not via the store: a `?? 0` in the bind would put a 0 here
        // and the struct read would agree with it.
        let raw = try rawPrewarmHit(in: dir, rowId: "row-null")
        #expect(raw != nil, "the row should exist")
        #expect(raw ?? 99 == nil, "prewarmHit must be SQL NULL, not 0")

        let readBack = try await store.fetchSemanticScanResults(analysisAssetId: "asset-null")
        #expect(readBack.count == 1)
        // `sqlite3_column_int` returns 0 for NULL. If the read regresses to the
        // bare column call this reads `false` — "measured, and it was cold" —
        // which is precisely the sentence this bead was filed to retract.
        #expect(readBack.first?.prewarmHit == nil)
    }

    @Test("a writer that DID observe warmth can still record either verdict")
    func measuredValuesRoundTripBothWays() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-measured"))

        for (id, value, expectedRaw) in [("row-warm", true, 1), ("row-cold", false, 0)] {
            let base = unmeasuredRow(id: id, assetId: "asset-measured")
            let measured = SemanticScanResult(
                id: base.id,
                analysisAssetId: base.analysisAssetId,
                windowFirstAtomOrdinal: base.windowFirstAtomOrdinal,
                windowLastAtomOrdinal: base.windowLastAtomOrdinal,
                windowStartTime: base.windowStartTime,
                windowEndTime: base.windowEndTime,
                scanPass: base.scanPass,
                transcriptQuality: base.transcriptQuality,
                disposition: base.disposition,
                spansJSON: base.spansJSON,
                status: base.status,
                attemptCount: base.attemptCount,
                errorContext: base.errorContext,
                inputTokenCount: base.inputTokenCount,
                outputTokenCount: base.outputTokenCount,
                latencyMs: base.latencyMs,
                prewarmHit: value,
                scanCohortJSON: base.scanCohortJSON,
                transcriptVersion: base.transcriptVersion,
                reuseScope: id
            )
            try await store.insertSemanticScanResult(measured)
            #expect(try rawPrewarmHit(in: dir, rowId: id) ?? nil == expectedRaw)
        }

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-measured")
        #expect(rows.first(where: { $0.id == "row-warm" })?.prewarmHit == true)
        #expect(rows.first(where: { $0.id == "row-cold" })?.prewarmHit == false)
    }

    // MARK: - The migration itself

    @Test("a V51-shaped store's stored zeroes become NULL, and the rest of the row survives")
    func seededV51ZeroesBecomeNull() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-legacy"))
        try await store.insertSemanticScanResult(unmeasuredRow(id: "row-legacy", assetId: "asset-legacy"))

        // Rewind to the v51 SHAPE, not merely the v51 version stamp. A rewind
        // that only touches `_meta` proves nothing: the rung would then run
        // against a column that is already nullable, and the DROP it is being
        // tested for would be indistinguishable from a no-op.
        try await store.execForTesting("ALTER TABLE semantic_scan_results DROP COLUMN prewarmHit")
        try await store.execForTesting(
            "ALTER TABLE semantic_scan_results ADD COLUMN prewarmHit INTEGER NOT NULL DEFAULT 0")
        // Pinned to the LITERAL 51 — "pre-exxc" is a fixed historical fact, and
        // `currentSchemaVersion - 1` would stop meaning it the moment head moves.
        try await store.setMetaValue(forKey: "schema_version", value: "51")
        #expect(try rawPrewarmHit(in: dir, rowId: "row-legacy") ?? nil == 0,
                "the rewind must reproduce the constant this bead is about")
        #expect(!(try columnIsNullable(in: dir, table: "semantic_scan_results", column: "prewarmHit")))

        AnalysisStore.resetMigratedPathsForTesting()
        let upgraded = try AnalysisStore(directory: dir)
        try await upgraded.migrate()

        #expect(try await upgraded.schemaVersion() == 52)
        #expect(try columnIsNullable(in: dir, table: "semantic_scan_results", column: "prewarmHit"))
        #expect(try rawPrewarmHit(in: dir, rowId: "row-legacy") ?? 99 == nil,
                "the 95 stored zeroes were a compile-time constant; V52 must not preserve them as data")

        // The DROP + ADD moves the column to the tail. Every reader indexes off
        // `semanticScanResultColumns`, which names its columns, so nothing after
        // index 16 may shift — this asserts it rather than trusting it.
        let rows = try await upgraded.fetchSemanticScanResults(analysisAssetId: "asset-legacy")
        #expect(rows.count == 1)
        #expect(rows.first?.prewarmHit == nil)
        #expect(rows.first?.scanCohortJSON == Self.cohort)
        #expect(rows.first?.transcriptVersion == "tv-1")
        #expect(rows.first?.latencyMs == 1_234)
        #expect(rows.first?.jobPhase == "shadow")
        #expect(rows.first?.status == .success)
    }

    @Test("the V52 rung is idempotent")
    func migrationIsIdempotent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-idem"))
        try await store.insertSemanticScanResult(unmeasuredRow(id: "row-idem", assetId: "asset-idem"))

        AnalysisStore.resetMigratedPathsForTesting()
        let again = try AnalysisStore(directory: dir)
        try await again.migrate()

        #expect(try await again.schemaVersion() == 52)
        #expect(try columnIsNullable(in: dir, table: "semantic_scan_results", column: "prewarmHit"))
        // The second climb must not have dropped and re-added the column out
        // from under an existing row.
        #expect(try await again.fetchSemanticScanResults(analysisAssetId: "asset-idem").count == 1)
    }

    // MARK: - 2. The production path actually writes it
    //
    // See the file header: everything above passes with all four builders still
    // stamping `prewarmHit: false`. This is the rail that does not.

    @Test("no SemanticScanResult built by BackfillJobRunner names prewarmHit")
    func productionBuildersNameNoPrewarmHit() throws {
        let source = try Self.appSourceRoot()
            .appendingPathComponent("Services/AdDetection/BackfillJobRunner.swift")
        let text = try String(contentsOf: source, encoding: .utf8)

        // Argument sites only. `coarse.prewarmHit` / `refinement.prewarmHit`
        // are the PASS-level field feeding `OperationalMetrics`, a different
        // quantity on a different type, and are deliberately untouched here.
        let offenders = text
            .split(separator: "\n", omittingEmptySubsequences: false)
            .enumerated()
            .filter { $0.element.contains("prewarmHit:") }
            .map { "BackfillJobRunner.swift:\($0.offset + 1): \($0.element.trimmingCharacters(in: .whitespaces))" }

        #expect(
            offenders.isEmpty,
            """
            A SemanticScanResult builder is naming `prewarmHit` again. Every one \
            of these sites holds a WINDOW-level output, which carries no warmth \
            signal, so whatever it passes is invented — and the column goes back \
            to reading like a measurement. Omit the argument; nil is the truth. \
            Offenders:
            \(offenders.joined(separator: "\n"))
            """
        )
    }

    /// Resolves the `Playhead/` source root by walking up from this test file
    /// (`#filePath` is stamped into the binary at compile time).
    private static func appSourceRoot(file: StaticString = #filePath) throws -> URL {
        let thisFile = URL(fileURLWithPath: String(describing: file))
        let repoRoot = thisFile
            .deletingLastPathComponent() // AnalysisStore/
            .deletingLastPathComponent() // Persistence/
            .deletingLastPathComponent() // PlayheadTests/
            .deletingLastPathComponent() // repo root
        let app = repoRoot.appendingPathComponent("Playhead", isDirectory: true)
        var isDir: ObjCBool = false
        guard FileManager.default.fileExists(atPath: app.path, isDirectory: &isDir), isDir.boolValue else {
            throw NSError(
                domain: "PrewarmHitUnmeasuredV52MigrationTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "App source root not found at \(app.path)"]
            )
        }
        return app
    }
}
