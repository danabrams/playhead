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
//   * `armedLaunches = N > 0`, zero rows  -> a POSITIVE CLAIM.
//
// The first rung is why `aV61StoreGenuinelyLacksTheTable` exists. Without it
// "the table is always there" is an assumption, and an assumption is exactly
// what a reader cannot check against a pulled file.

import Foundation
import Testing
import SQLite3
@testable import Playhead

@Suite("AnalysisStore V62 — the dropped-background-download ledger (playhead-7dgx)")
struct BackgroundDownloadDropsV62MigrationTests {

    private static let dropsTable = "background_download_drops"
    private static let armingTable = "background_download_drop_arming"

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
        try rewindToV61(dir)

        // `migrateOnlyForTesting` deliberately bypasses `createTables()`, so
        // this proves the RUNG creates the tables — not the belt-and-braces
        // DDL that runs on every open. A rung that only ever ran behind
        // `createTables()` would be untestable and, on a device, unreachable.
        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == 62)
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

    // MARK: - 5. The drift guard

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
