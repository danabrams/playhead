// BackgroundDownloadDropsV62MigrationTests.swift
// playhead-7dgx — the V62 rung, and the three-state ladder a device pull reads.
//
// THE READING THIS RUNG EXISTS TO MAKE POSSIBLE, and every rail below pins one
// rung of it:
//
//   * `background_download_drops` ABSENT  -> the build predates the instrument.
//                                           Zero drops says NOTHING.
//   * present, `armedLaunches = 0`        -> the instrument shipped and no
//                                           launch armed it. Still nothing.
//   * `armedLaunches = N > 0`, zero rows,
//     `dropWriteFailures = 0`             -> a POSITIVE CLAIM.
//
// The first rung is why `aV61StoreGenuinelyLacksTheTable` exists. Without it
// "the table is always there" is an assumption, and an assumption is exactly
// what a reader cannot check against a pulled file.
//
// AND THE DISCRIMINATOR IS THE TABLE, NOT THE STAMP — which a first cut of
// this bead got wrong, in the header of the file it was documenting. It said
// "`_meta.schema_version` reads < 62", and that is not the same set.
// `createTables()` runs unconditionally on every open, BEFORE the ladder, so a
// store parked below the V39 rollback floor — a real, documented, guarded
// population, which is why every rung from V40 up carries a `guard observed >=
// N` — holds both tables and a live arming row at a stamp of 38. Rows written
// there are genuine, and a reader following the stamp recipe would throw them
// away. `theTablesExistBelowTheV39RollbackFloor` pins the pairing.

import Foundation
import Testing
import SQLite3
@testable import Playhead

@Suite("AnalysisStore V62 — the dropped-background-download ledger (playhead-7dgx)")
struct BackgroundDownloadDropsV62MigrationTests {

    private static let dropsTable = "background_download_drops"
    private static let armingTable = "background_download_drop_arming"

    /// A raw read-write handle carrying the same busy timeout the store's own
    /// connection uses. Without it, a write racing the live WAL connection in
    /// this process returns `SQLITE_BUSY` immediately and the test fails for a
    /// reason that is not its subject.
    private func openRawReadWrite(_ directory: URL) throws -> OpaquePointer? {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK else {
            throw NSError(domain: "OpenRawReadWrite", code: 1)
        }
        sqlite3_busy_timeout(db, 3000)
        return db
    }

    private func probeTableExists(in directory: URL, table: String) throws -> Bool {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "ProbeTableExists", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        let sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?"
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(domain: "ProbeTableExists", code: 2)
        }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(
            stmt, 1, table, -1,
            unsafeBitCast(-1, to: sqlite3_destructor_type.self)
        )
        return sqlite3_step(stmt) == SQLITE_ROW
    }

    /// Rewinds a head-shaped store to the state a V61 device is in: the two
    /// V62 tables gone and the stamp back at 61.
    ///
    /// `61` is written literally rather than as `currentSchemaVersion - 1`,
    /// on the V61 suite's own precedent: the arithmetic form stops meaning
    /// "the rung before this one" the moment head moves, and then this rail
    /// silently tests a different migration than the one it is named after.
    private func rewindToV61(_ directory: URL) throws {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK else {
            throw NSError(domain: "RewindToV61", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        for sql in [
            "DROP TABLE IF EXISTS \(Self.dropsTable)",
            "DROP TABLE IF EXISTS \(Self.armingTable)",
            "UPDATE _meta SET value = '61' WHERE key = 'schema_version'",
        ] {
            guard sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK else {
                throw NSError(domain: "RewindToV61", code: 2)
            }
        }
    }

    // MARK: - 1. The discriminator: a V61 store really has no table

    /// If this ever passes vacuously — because something else creates the
    /// table — then "the table is absent" stops being evidence that a build
    /// predates the instrument, and the whole three-state reading collapses to
    /// two.
    @Test("a V61-shaped store genuinely lacks both tables, which is what makes ABSENT readable")
    func aV61StoreGenuinelyLacksTheTable() async throws {
        let dir = try makeTempDir(prefix: "V62Absent")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)
        #expect(try probeTableExists(in: dir, table: Self.dropsTable))

        try rewindToV61(dir)
        #expect(try probeTableExists(in: dir, table: Self.dropsTable) == false)
        #expect(try probeTableExists(in: dir, table: Self.armingTable) == false)
    }

    // MARK: - 2. The ladder-only seam climbs it

    @Test("a V61 store climbs to head through the ladder-only seam and gains both tables")
    func theLadderOnlySeamReachesV62() async throws {
        let dir = try makeTempDir(prefix: "V62Ladder")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)
        try rewindToV61(dir)

        // `migrateOnlyForTesting` deliberately bypasses `createTables()`, so
        // this proves the RUNG creates the tables — not the belt-and-braces
        // DDL that runs on every open. A rung that only ever ran behind
        // `createTables()` would be untestable and, on a device, unreachable.
        try await store.migrateOnlyForTesting()

        // Against the CONSTANT, never a literal. A head assertion written as a
        // database-read literal is the exact shape this branch had to repair in
        // two other suites, and a `currentSchemaVersion` grep cannot find it.
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: Self.dropsTable))
        #expect(try probeTableExists(in: dir, table: Self.armingTable))
        #expect(try probeIndexExists(in: dir, indexName: "idx_background_download_drops_occurred"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_background_download_drops_episode"))

        let fetched = try await store.fetchBackgroundDownloadDropArming()
        let arming = try #require(fetched, "the rung must seed the arming row, not leave it absent")
        #expect(arming.armedLaunches == 0)
        #expect(arming.firstArmedAt == nil)
    }

    @Test("a store seeded two rungs back still reaches head, so V62 does not depend on running alone")
    func aV60StoreClimbsThroughV61ToHead() async throws {
        let dir = try makeTempDir(prefix: "V62FromV60")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)
        try rewindToV61(dir)
        try seedSchemaVersion(60, in: dir)

        try await store.migrateOnlyForTesting()
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: Self.dropsTable))
    }

    // MARK: - 3. Idempotence — and specifically that a re-run cannot ERASE

    /// The rung runs on every launch of every build at or below head. If a
    /// second pass reset `armedLaunches`, the arming count would report
    /// "launches since the last migration" while being read as "launches ever"
    /// — the standing defect class, arriving through an idempotence bug.
    @Test("re-running the rung preserves armedLaunches and installedAt")
    func theRungIsIdempotentAndNeverErasesTheArmingRow() async throws {
        let dir = try makeTempDir(prefix: "V62Idempotent")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)

        try await store.noteBackgroundDownloadDropInstrumentArmed(at: 1000.0)
        try await store.noteBackgroundDownloadDropInstrumentArmed(at: 2000.0)
        let beforeFetch = try await store.fetchBackgroundDownloadDropArming()
        let before = try #require(beforeFetch)
        #expect(before.armedLaunches == 2)

        // Rewind only the STAMP, leaving the tables and their rows in place —
        // the shape a partially-rolled-back device is in, and the one where an
        // over-eager `CREATE`/`INSERT` would destroy history.
        try seedSchemaVersion(61, in: dir)
        try await store.migrateOnlyForTesting()

        let afterFetch = try await store.fetchBackgroundDownloadDropArming()
        let after = try #require(afterFetch)
        #expect(after.armedLaunches == 2)
        #expect(after.firstArmedAt == before.firstArmedAt)
        #expect(after.lastArmedAt == before.lastArmedAt)
        #expect(after.installedAt == before.installedAt)
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
    }

    // MARK: - 4. A fresh install lands on the same shape

    @Test("a fresh store is at head with both tables and a seeded arming row")
    func aFreshStoreIsAtHead() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: Self.dropsTable))
        #expect(try probeTableExists(in: dir, table: Self.armingTable))
        let fetched = try await store.fetchBackgroundDownloadDropArming()
        #expect(try #require(fetched).armedLaunches == 0)
    }

    /// WHY `createTables()` CARRIES THE DDL AS WELL AS THE RUNG, and the only
    /// state in which the difference is observable.
    ///
    /// A store whose stamp is at head has every rung gated out — `guard
    /// observed < 62` is false — so if the tables are missing, the ladder can
    /// never put them back and the instrument is dead on that install forever.
    /// `createTables()` runs unconditionally on every open, which is what
    /// repairs it. This is the same shape `MigrationLadderTests` pins for the
    /// older tables ("stamped head repairs missing additive columns and
    /// tombstone tables"); without this rail, removing the `createTables()`
    /// call is invisible to every runtime test in the tree.
    @Test("a store STAMPED at head but missing the tables gets them back on the next open")
    func aStampedHeadStoreRepairsMissingTables() async throws {
        let dir = try makeTempDir(prefix: "V62StampedHead")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)

        // Drop the tables but LEAVE the stamp at head — every rung is now
        // gated out and only `createTables()` can repair this.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        for sql in [
            "DROP TABLE IF EXISTS \(Self.dropsTable)",
            "DROP TABLE IF EXISTS \(Self.armingTable)",
        ] {
            #expect(sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK)
        }
        sqlite3_close_v2(db)
        #expect(try probeTableExists(in: dir, table: Self.dropsTable) == false)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(try await reopened.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: Self.dropsTable))
        #expect(try probeTableExists(in: dir, table: Self.armingTable))
        let fetched = try await reopened.fetchBackgroundDownloadDropArming()
        #expect(try #require(fetched).armedLaunches == 0)
    }

    /// A STORE CARRYING AN OLDER SHAPE OF THESE TABLES MUST STILL OPEN — and
    /// this rail exists because it did not, on this very branch.
    ///
    /// `createBackgroundDownloadDropTables()` runs from `createTables()`, which
    /// is unconditional and runs before the ladder, so its `CREATE TABLE IF NOT
    /// EXISTS` is a NO-OP against a table that already exists in an older
    /// shape. When `dropWriteFailures` was added in a later commit, the seed
    /// `INSERT` named a column that was not there, SQLite refused the
    /// statement, `createTables()` threw, the whole `runSchemaMigration()`
    /// transaction rolled back — and THE STORE STOPPED OPENING AT ALL.
    ///
    /// What it cost, and why the rail is worth its length: the symptom was a
    /// completely unrelated trust-profile test failing, because a store that
    /// will not open fails everything downstream of it. Nothing pointed at this
    /// table. The unmutated baseline of the mutation battery is what caught it.
    ///
    /// The version guard cannot help here — the rung never runs, because
    /// `createTables()` throws first — so the repair has to live in the DDL
    /// helper, and every future column added to either table owes an
    /// `addColumnIfNeeded` beside it. This test is what makes forgetting that
    /// a red rail rather than a bricked device.
    @Test("a store carrying the PRE-dropWriteFailures shape still opens, and is repaired")
    func anOlderArmingShapeIsRepairedRatherThanBrickingTheStore() async throws {
        let dir = try makeTempDir(prefix: "V62OldShape")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)

        // Rebuild the arming table in its pre-`dropWriteFailures` shape, with a
        // row in it, exactly as a store created by the earlier commit holds it.
        let db = try openRawReadWrite(dir)
        let oldShape = "CREATE TABLE background_download_drop_arming (id INTEGER PRIMARY KEY CHECK (id = 1), armedLaunches INTEGER NOT NULL DEFAULT 0, firstArmedAt REAL, lastArmedAt REAL, installedAt REAL NOT NULL)"
        let oldRow = "INSERT INTO background_download_drop_arming (id, armedLaunches, firstArmedAt, lastArmedAt, installedAt) VALUES (1, 4, 100.0, 200.0, 50.0)"
        for sql in ["DROP TABLE \(Self.armingTable)", oldShape, oldRow] {
            #expect(sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK, "\(sql)")
        }
        sqlite3_close_v2(db)
        #expect(try probeColumnExists(in: dir, table: Self.armingTable, column: "dropWriteFailures") == false)

        // The open must SUCCEED. Before the repair this threw, and it took the
        // entire analysis pipeline down with it.
        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        #expect(try probeColumnExists(in: dir, table: Self.armingTable, column: "dropWriteFailures"))
        let fetched = try await reopened.fetchBackgroundDownloadDropArming()
        let arming = try #require(fetched)
        // The history SURVIVES the repair — a column add must not reset the
        // counter it is being added beside.
        #expect(arming.armedLaunches == 4)
        #expect(arming.firstArmedAt == 100.0)
        #expect(arming.installedAt == 50.0)
        #expect(arming.dropWriteFailures == 0, "a column with no history reads zero, which is the honest value here")

        // And the store is genuinely usable afterwards, not merely open.
        try await reopened.noteBackgroundDownloadDropWriteFailure(at: 300.0)
        let after = try #require(try await reopened.fetchBackgroundDownloadDropArming())
        #expect(after.dropWriteFailures == 1)
        #expect(after.armedLaunches == 4)
    }

    /// THE STAMP IS NOT THE DISCRIMINATOR, and this is the state that proves
    /// it.
    ///
    /// V39 is allowed to fail without throwing: it rolls back to its savepoint
    /// and leaves `schema_version` at 38 so the next launch retries, and every
    /// rung from V40 up declines to step over that. But `createTables()` runs
    /// BEFORE the ladder and unconditionally — so such a store carries both V62
    /// tables and a working arming row while reading 38.
    ///
    /// A reader who took `schema_version < 62` to mean "this build had no
    /// instrument" would discard real rows from exactly the devices most likely
    /// to be producing them. The fixture is the V40 suite's, verbatim: two
    /// assets colliding on one fingerprint plus a trigger that aborts the
    /// delete V39 needs.
    @Test("the tables exist BELOW the V39 rollback floor, so table presence and not the stamp is the discriminator")
    func theTablesExistBelowTheV39RollbackFloor() async throws {
        let dir = try makeTempDir(prefix: "V62BelowFloor")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)

        let db = try openRawReadWrite(dir)
        let insertOld = "INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt) VALUES ('v62-dupe-old', 'v62-ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 1.0)"
        let insertNew = "INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt) VALUES ('v62-dupe-new', 'v62-ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 2.0)"
        let guardTrigger = "CREATE TRIGGER v62_v39_guard BEFORE DELETE ON analysis_assets BEGIN SELECT RAISE(ABORT, 'v62 below-floor fixture'); END"
        for sql in [
            "DROP TABLE IF EXISTS \(Self.dropsTable)",
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
        #expect(try probeTableExists(in: dir, table: Self.dropsTable))
        #expect(try probeTableExists(in: dir, table: Self.armingTable))
        try await reopened.noteBackgroundDownloadDropInstrumentArmed(at: 5.0)
        let fetched = try await reopened.fetchBackgroundDownloadDropArming()
        #expect(try #require(fetched).armedLaunches == 1)
    }

    // MARK: - 6. The drift guard

    /// 61 -> 62 read for every rung that asserts `currentSchemaVersion`: V62
    /// CREATES TWO NEW TABLES and touches no existing one — no column, no
    /// UPDATE, no DELETE, and no backfill (every drop before this build
    /// deleted its own evidence, so there is nothing to recover). Nothing any
    /// other migration suite asserts can move because of it.
    @Test("head is 62")
    func headIsSixtyTwo() {
        #expect(AnalysisStore.currentSchemaVersion == 62)
    }
}
