// DownloadWorkJournalV63MigrationTests.swift
// playhead-4xmz — the V63 rung, and the three-state reading a device pull makes.
//
// THE READING THIS RUNG EXISTS TO MAKE POSSIBLE, and every rail below pins one
// cell of it:
//
//   * `download_work_journal` ABSENT     -> the build predates the instrument.
//                                           Zero events says NOTHING.
//   * present, `armedLaunches = 0`       -> the instrument shipped and no
//                                           launch armed it. Still nothing.
//   * `armedLaunches = N > 0`, zero rows,
//     `writeFailures = 0`                -> a POSITIVE CLAIM.
//
// The first cell is why `aV62StoreGenuinelyLacksTheTables` exists. Without it,
// "the table is always there" is an assumption, and an assumption is exactly
// what a reader cannot check against a pulled file.
//
// AND THE DISCRIMINATOR IS THE TABLE, NOT THE STAMP. `createTables()` runs
// unconditionally on every open, BEFORE the ladder, so a store parked below the
// V39 rollback floor holds both tables and a live arming row while reading 38.
// A reader following a `schema_version < 63` recipe would discard real rows
// from exactly the devices most likely to be producing them.
// `theTablesExistBelowTheV39RollbackFloor` pins that pairing, on V62's
// precedent.

import Foundation
import Testing
import SQLite3
@testable import Playhead

@Suite("AnalysisStore V63 — the download-path work journal (playhead-4xmz)")
struct DownloadWorkJournalV63MigrationTests {

    private static let journalTable = "download_work_journal"
    private static let armingTable = "download_work_journal_arming"

    private func openRawReadWrite(_ directory: URL) throws -> OpaquePointer? {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK else {
            throw NSError(domain: "OpenRawReadWrite", code: 1)
        }
        sqlite3_busy_timeout(db, 3000)
        return db
    }

    private func probeExists(in directory: URL, type: String, name: String) throws -> Bool {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "ProbeExists", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        let sql = "SELECT 1 FROM sqlite_master WHERE type=? AND name=?"
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(domain: "ProbeExists", code: 2)
        }
        defer { sqlite3_finalize(stmt) }
        let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
        sqlite3_bind_text(stmt, 1, type, -1, transient)
        sqlite3_bind_text(stmt, 2, name, -1, transient)
        return sqlite3_step(stmt) == SQLITE_ROW
    }

    private func probeTableExists(in directory: URL, table: String) throws -> Bool {
        try probeExists(in: directory, type: "table", name: table)
    }

    /// Rewinds a head-shaped store to the state a V62 device is in: the two V63
    /// tables gone and the stamp back at 62.
    ///
    /// `62` is written literally rather than as `currentSchemaVersion - 1`, on
    /// the V61 and V62 suites' precedent: the arithmetic form stops meaning
    /// "the rung before this one" the moment head moves, and then this rail
    /// silently tests a different migration than the one it is named after.
    private func rewindToV62(_ directory: URL) throws {
        let db = try openRawReadWrite(directory)
        defer { sqlite3_close_v2(db) }
        for sql in [
            "DROP TABLE IF EXISTS \(Self.journalTable)",
            "DROP TABLE IF EXISTS \(Self.armingTable)",
            "UPDATE _meta SET value = '62' WHERE key = 'schema_version'",
        ] {
            guard sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK else {
                throw NSError(domain: "RewindToV62", code: 2)
            }
        }
    }

    private func seedStamp(_ version: Int, in directory: URL) throws {
        let db = try openRawReadWrite(directory)
        defer { sqlite3_close_v2(db) }
        let sql = "UPDATE _meta SET value = '\(version)' WHERE key = 'schema_version'"
        guard sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK else {
            throw NSError(domain: "SeedStamp", code: 1)
        }
    }

    private func makeHeadStore(prefix: String) async throws -> (AnalysisStore, URL) {
        let dir = try makeTempDir(prefix: prefix)
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)
        return (store, dir)
    }

    // MARK: - 1. The discriminator: a V62 store really has no tables

    /// If this ever passes vacuously — because something else creates the
    /// tables — then "the table is absent" stops being evidence that a build
    /// predates the instrument, and the three-state reading collapses to two.
    @Test("a V62-shaped store genuinely lacks both tables, which is what makes ABSENT readable")
    func aV62StoreGenuinelyLacksTheTables() async throws {
        let (_, dir) = try await makeHeadStore(prefix: "V63Absent")
        #expect(try probeTableExists(in: dir, table: Self.journalTable))

        try rewindToV62(dir)
        #expect(try probeTableExists(in: dir, table: Self.journalTable) == false)
        #expect(try probeTableExists(in: dir, table: Self.armingTable) == false)
    }

    // MARK: - 2. The ladder-only seam climbs it

    @Test("a V62 store climbs to head through the ladder-only seam and gains both tables")
    func theLadderOnlySeamReachesV63() async throws {
        let (store, dir) = try await makeHeadStore(prefix: "V63Ladder")
        try rewindToV62(dir)

        // `migrateOnlyForTesting` deliberately bypasses `createTables()`, so
        // this proves the RUNG creates the tables — not the belt-and-braces DDL
        // that runs on every open. A rung that only ever ran behind
        // `createTables()` would be untestable and, on a device, unreachable.
        try await store.migrateOnlyForTesting()

        // Against the CONSTANT, never a literal — a head assertion written as a
        // database-read literal is the shape a `currentSchemaVersion` grep
        // cannot find, which cost V62 a commit.
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: Self.journalTable))
        #expect(try probeTableExists(in: dir, table: Self.armingTable))
        #expect(try probeExists(
            in: dir, type: "index", name: "idx_download_work_journal_occurred"
        ))
        #expect(try probeExists(
            in: dir, type: "index", name: "idx_download_work_journal_episode"
        ))

        let arming = try #require(
            try await store.fetchDownloadWorkJournalArming(),
            "the rung must seed the arming row, not leave it absent"
        )
        #expect(arming.armedLaunches == 0)
        #expect(arming.firstArmedAt == nil)
    }

    @Test("a store seeded two rungs back still reaches head, so V63 does not depend on running alone")
    func aV61StoreClimbsThroughV62ToHead() async throws {
        let (store, dir) = try await makeHeadStore(prefix: "V63FromV61")
        try rewindToV62(dir)
        try seedStamp(61, in: dir)

        try await store.migrateOnlyForTesting()
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: Self.journalTable))
    }

    // MARK: - 3. Idempotence — and specifically that a re-run cannot ERASE

    /// The rung runs on every launch of every build at or below head. If a
    /// second pass reset `armedLaunches`, the arming count would report
    /// "launches since the last migration" while being read as "launches ever"
    /// — the standing defect class, arriving through an idempotence bug.
    @Test("re-running the rung preserves armedLaunches, the stamps, and the rows")
    func theRungIsIdempotentAndNeverErases() async throws {
        let (store, dir) = try await makeHeadStore(prefix: "V63Idempotent")

        try await store.noteDownloadWorkJournalInstrumentArmed(at: 1_000.0)
        try await store.noteDownloadWorkJournalInstrumentArmed(at: 2_000.0)
        try await store.insertDownloadWorkJournalEntry(
            DownloadWorkJournalRecord(
                id: "row-keepme",
                episodeId: "ep-keepme",
                eventType: .failed,
                cause: .noNetwork,
                occurredAt: 1_500.0,
                metadataJSON: "{}"
            )
        )
        let before = try #require(try await store.fetchDownloadWorkJournalArming())
        #expect(before.armedLaunches == 2)
        #expect(before.firstArmedAt == 1_000.0)
        #expect(before.lastArmedAt == 2_000.0)

        // Rewind only the STAMP, leaving the tables and their rows in place —
        // the shape a partially-rolled-back device is in, and the one where an
        // over-eager `CREATE`/`INSERT` would destroy history.
        try seedStamp(62, in: dir)
        try await store.migrateOnlyForTesting()

        let after = try #require(try await store.fetchDownloadWorkJournalArming())
        #expect(after.armedLaunches == 2)
        #expect(after.firstArmedAt == before.firstArmedAt)
        #expect(after.lastArmedAt == before.lastArmedAt)
        #expect(after.installedAt == before.installedAt)
        #expect(try await store.fetchDownloadWorkJournal().rows.count == 1)
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
    }

    // MARK: - 4. A fresh install lands on the same shape

    @Test("a fresh store is at head with both DOWNLOAD-JOURNAL tables and a seeded arming row")
    func aFreshStoreIsAtHead() async throws {
        let (store, dir) = try await makeHeadStore(prefix: "V63Fresh")
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: Self.journalTable))
        #expect(try probeTableExists(in: dir, table: Self.armingTable))
        #expect(try #require(try await store.fetchDownloadWorkJournalArming()).armedLaunches == 0)
    }

    /// WHY `createTables()` CARRIES THE DDL AS WELL AS THE RUNG.
    ///
    /// A store whose stamp is at head has every rung gated out — `guard
    /// observed < 63` is false — so if the tables are missing, the ladder can
    /// never put them back and the instrument is dead on that install forever.
    /// `createTables()` runs unconditionally on every open, which is what
    /// repairs it. Without this rail, removing the `createTables()` call is
    /// invisible to every runtime test in the tree.
    @Test("a store STAMPED at head but missing the DOWNLOAD-JOURNAL tables gets them back on the next open")
    func aStampedHeadStoreRepairsMissingTables() async throws {
        let (_, dir) = try await makeHeadStore(prefix: "V63StampedHead")

        let db = try openRawReadWrite(dir)
        for sql in [
            "DROP TABLE IF EXISTS \(Self.journalTable)",
            "DROP TABLE IF EXISTS \(Self.armingTable)",
        ] {
            #expect(sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK, "\(sql)")
        }
        sqlite3_close_v2(db)
        #expect(try probeTableExists(in: dir, table: Self.journalTable) == false)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: Self.journalTable))
        #expect(try probeTableExists(in: dir, table: Self.armingTable))
        #expect(
            try #require(try await reopened.fetchDownloadWorkJournalArming()).armedLaunches == 0
        )
    }

    /// THE STAMP IS NOT THE DISCRIMINATOR, and this is the state that proves
    /// it.
    ///
    /// V39 is allowed to fail without throwing: it rolls back to its savepoint
    /// and leaves `schema_version` at 38 so the next launch retries, and every
    /// rung from V40 up declines to step over that. But `createTables()` runs
    /// BEFORE the ladder and unconditionally — so such a store carries both V63
    /// tables and a working arming row while reading 38. The fixture is the V40
    /// and V62 suites', verbatim: two assets colliding on one fingerprint plus
    /// a trigger that aborts the delete V39 needs.
    @Test("the DOWNLOAD-JOURNAL tables exist BELOW the V39 rollback floor, so presence and not the stamp is the discriminator")
    func theTablesExistBelowTheV39RollbackFloor() async throws {
        let (_, dir) = try await makeHeadStore(prefix: "V63BelowFloor")

        let db = try openRawReadWrite(dir)
        let insertOld = "INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt) VALUES ('v63-dupe-old', 'v63-ep-collide', 'ffed', 'file:///tmp/x.mp3', 'pending', 1.0)"
        let insertNew = "INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt) VALUES ('v63-dupe-new', 'v63-ep-collide', 'ffed', 'file:///tmp/x.mp3', 'pending', 2.0)"
        let guardTrigger = "CREATE TRIGGER v63_v39_guard BEFORE DELETE ON analysis_assets BEGIN SELECT RAISE(ABORT, 'v63 below-floor fixture'); END"
        for sql in [
            "DROP TABLE IF EXISTS \(Self.journalTable)",
            "DROP TABLE IF EXISTS \(Self.armingTable)",
            "DROP INDEX IF EXISTS idx_assets_episode_fingerprint",
            "DROP INDEX IF EXISTS idx_chunks_asset_pass_fingerprint",
            insertOld,
            insertNew,
            guardTrigger,
            "UPDATE _meta SET value = '38' WHERE key = 'schema_version'",
        ] {
            #expect(sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK, "\(sql)")
        }
        sqlite3_close_v2(db)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        // The stamp is stuck below the floor…
        #expect(try await reopened.schemaVersion() == 38)
        // …and the tables are nonetheless present and usable.
        #expect(try probeTableExists(in: dir, table: Self.journalTable))
        #expect(try probeTableExists(in: dir, table: Self.armingTable))
        try await reopened.noteDownloadWorkJournalInstrumentArmed(at: 5.0)
        #expect(
            try #require(try await reopened.fetchDownloadWorkJournalArming()).armedLaunches == 1
        )
    }

    // MARK: - 5. The rung touches nothing else

    /// V63 CREATES TWO NEW TABLES and touches no existing one — no column, no
    /// UPDATE, no DELETE, and no backfill. Nothing could be backfilled: every
    /// download event before this build went to a no-op recorder and left no
    /// trace, so the absence of pre-V63 rows is honest.
    ///
    /// The rail is the `work_journal` table specifically, because that is the
    /// one a careless implementation of this bead would have written into —
    /// and a row there is not merely misfiled, it is an input to
    /// `AnalysisCoordinator.recoverOrphans`.
    @Test("the download journal writes leave work_journal untouched")
    func theAnalysisJournalIsNotTouched() async throws {
        let (store, dir) = try await makeHeadStore(prefix: "V63Untouched")
        let recorder = AnalysisStoreDownloadWorkJournalRecorder(store: store)

        await recorder.recordFinalized(episodeId: "ep-x")
        await recorder.recordFailed(episodeId: "ep-x", cause: .noNetwork, metadataJSON: "{}")
        await recorder.recordPreempted(
            episodeId: "ep-x", cause: .appForceQuitRequiresRelaunch, metadataJSON: "{}"
        )

        let db = try openRawReadWrite(dir)
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        let prepared: Int32 = sqlite3_prepare_v2(
            db, "SELECT count(*) FROM work_journal", -1, &stmt, nil
        )
        #expect(prepared == SQLITE_OK)
        defer { sqlite3_finalize(stmt) }
        let stepped: Int32 = sqlite3_step(stmt)
        #expect(stepped == SQLITE_ROW)
        // Hoisted into a typed local rather than inlined into the `#expect`:
        // the macro expands its argument twice and the C-interop overloads
        // made the combined expression time the type-checker out.
        let analysisJournalRows: Int64 = sqlite3_column_int64(stmt, 0)
        #expect(
            analysisJournalRows == 0,
            """
            a download event must never land in `work_journal`: its `event_type` is what
            `AnalysisCoordinator.recoverOrphans` routes on, and `.failed`/`.finalized` there
            mean \"clear the lease and do not requeue\" — so a transfer failure written into
            it would terminate an ANALYSIS generation for a reason that has nothing to do
            with analysis
            """
        )
        #expect(try await store.fetchDownloadWorkJournal().rows.count == 3)
    }
}
