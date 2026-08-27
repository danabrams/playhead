// BackgroundDownloadDropLaunchIdentityV64MigrationTests.swift
// playhead-sdis — the V64 rung, and the one reading it exists to make
// possible: `count(*)` is EPISODES, `count(DISTINCT sessionCrossingId)` is
// DAEMON REFUSALS, and `count(DISTINCT launchId)` is LAUNCHES.
//
// V62 shipped a table whose only count was a count of episodes, and
// playhead-7dgx's retry recommendation was written against it claiming a
// `session_not_vended` refusal is a per-LAUNCH outage. Nothing in the table
// could falsify that. These rails pin the four columns that can.
//
// WHY EVERY COLUMN IS NULLABLE, since that is the first thing a reader will
// question. SQLite cannot `ALTER TABLE … ADD COLUMN … NOT NULL` without a
// DEFAULT, and there is no honest default for an identity:
//
//   * one shared sentinel (`''`, `'pre-v64'`) makes every pre-V64 row ONE
//     launch under `count(DISTINCT launchId)`;
//   * a per-row sentinel makes them as many launches as there are rows.
//
// Both are a value that names an ABSENCE being read as a presence, which is
// the defect class this whole bead exists to remove. `NULL` is the one
// spelling SQL's own `count(DISTINCT)` declines to count, so it fails safe —
// the identical call V61's `usedPermissiveFallback` made. What enforces the
// bead's `NOT NULL` intent instead is the WRITE PATH: the initializer
// `DownloadManager` reaches takes a non-optional `launchId` and
// `launchArmingState`, so no row this build writes can be null.
// `aPreV64RowSurvivesTheMigrationCarryingNoIdentity` is what makes that a
// measured claim rather than a stated one.

import Foundation
import Testing
import SQLite3
@testable import Playhead

@Suite("AnalysisStore V64 — launch, crossing and arming identity on the drop ledger (playhead-sdis)")
struct BackgroundDownloadDropLaunchIdentityV64MigrationTests {

    private static let dropsTable = "background_download_drops"
    private static let armingTable = "background_download_drop_arming"

    /// The four columns this rung adds, and the table each belongs to. Held in
    /// one place so a rail cannot check three of them and read green.
    private static let addedColumns: [(table: String, column: String)] = [
        (dropsTable, "launchId"),
        (dropsTable, "sessionCrossingId"),
        (dropsTable, "launchArmingState"),
        (armingTable, "lastArmedLaunchId"),
    ]

    /// A raw read-write handle carrying the same busy timeout the store's own
    /// connection uses — see the V62 suite for why the timeout is not optional.
    private func openRawReadWrite(_ directory: URL) throws -> OpaquePointer? {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK else {
            throw NSError(domain: "OpenRawReadWrite", code: 1)
        }
        sqlite3_busy_timeout(db, 3000)
        return db
    }

    /// Reads one TEXT cell, distinguishing "the column holds NULL" from "the
    /// column holds the empty string" — which is the whole subject of this
    /// suite and is exactly what `sqlite3` the CLI renders identically.
    private func rawOptionalText(
        in directory: URL, sql: String
    ) throws -> String?? {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "RawOptionalText", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(domain: "RawOptionalText", code: 2)
        }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        if sqlite3_column_type(stmt, 0) == SQLITE_NULL { return .some(nil) }
        guard let raw = sqlite3_column_text(stmt, 0) else { return .some(nil) }
        return .some(String(cString: raw))
    }

    /// Rewinds a head-shaped store to the state a V63 device is in: the four
    /// V64 columns gone and the stamp back at 63.
    ///
    /// The columns are removed by REBUILDING the two tables in their V63 shape
    /// rather than by `ALTER TABLE … DROP COLUMN`, because rebuilding is what
    /// reproduces the state that BRICKED the store at V62: a table that exists
    /// in an older shape, against which `CREATE TABLE IF NOT EXISTS` is a
    /// no-op. `61` and `63` are written literally rather than as
    /// `currentSchemaVersion - 1`, on the V61 and V62 suites' precedent — the
    /// arithmetic form stops meaning "the rung before this one" the moment head
    /// moves, and then the rail silently tests a different migration.
    private func rewindToV63(_ directory: URL, keepingRows: Bool) throws {
        let db = try openRawReadWrite(directory)
        defer { sqlite3_close_v2(db) }
        let dropsV63 = """
            CREATE TABLE background_download_drops (
                id TEXT PRIMARY KEY, episodeId TEXT NOT NULL, reason TEXT NOT NULL,
                occurredAt REAL NOT NULL, podcastId TEXT, unattributedReason TEXT,
                isExplicitDownload INTEGER NOT NULL, boundSeconds REAL NOT NULL)
            """
        let armingV63 = """
            CREATE TABLE background_download_drop_arming (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                armedLaunches INTEGER NOT NULL DEFAULT 0,
                dropWriteFailures INTEGER NOT NULL DEFAULT 0,
                firstArmedAt REAL, lastArmedAt REAL, installedAt REAL NOT NULL)
            """
        var statements = [
            "DROP TABLE IF EXISTS \(Self.dropsTable)",
            "DROP TABLE IF EXISTS \(Self.armingTable)",
            dropsV63,
            armingV63,
        ]
        if keepingRows {
            statements.append("""
                INSERT INTO background_download_drops
                (id, episodeId, reason, occurredAt, podcastId, unattributedReason,
                 isExplicitDownload, boundSeconds)
                VALUES ('v63-row', 'ep-v63', 'session_not_vended', 900.0,
                        'show-v63', NULL, 0, 10.0)
                """)
            statements.append("""
                INSERT INTO background_download_drop_arming
                (id, armedLaunches, dropWriteFailures, firstArmedAt, lastArmedAt,
                 installedAt)
                VALUES (1, 7, 2, 100.0, 200.0, 50.0)
                """)
        }
        statements.append("UPDATE _meta SET value = '63' WHERE key = 'schema_version'")
        for sql in statements {
            guard sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK else {
                throw NSError(
                    domain: "RewindToV63", code: 2,
                    userInfo: [NSLocalizedDescriptionKey: sql]
                )
            }
        }
    }

    // MARK: - 1. The discriminator: a V63 store really lacks the columns

    /// If this ever passes vacuously — because something else adds the columns
    /// — then "`launchId IS NULL` means the row predates V64" stops being
    /// evidence, and every reading built on it collapses.
    @Test("a V63-shaped store genuinely lacks all four columns, which is what makes NULL readable")
    func aV63StoreGenuinelyLacksTheColumns() async throws {
        let dir = try makeTempDir(prefix: "V64Absent")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)
        for (table, column) in Self.addedColumns {
            #expect(try probeColumnExists(in: dir, table: table, column: column), "\(table).\(column)")
        }

        try rewindToV63(dir, keepingRows: false)
        for (table, column) in Self.addedColumns {
            #expect(
                try probeColumnExists(in: dir, table: table, column: column) == false,
                "\(table).\(column) must be absent in the V63 shape, or this suite proves nothing"
            )
        }
    }

    // MARK: - 2. The ladder-only seam climbs it

    /// `migrateOnlyForTesting` deliberately bypasses `createTables()`, so this
    /// proves the RUNG adds the columns — not the belt-and-braces DDL that runs
    /// on every open. A rung registered in one ladder and not the other is
    /// invisible to every fixture-driven test, and it cost V60 a commit.
    @Test("a V63 store climbs to head through the ladder-only seam and gains all four columns")
    func theLadderOnlySeamReachesV64() async throws {
        let dir = try makeTempDir(prefix: "V64Ladder")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)
        try rewindToV63(dir, keepingRows: false)

        try await store.migrateOnlyForTesting()

        // Against the CONSTANT, never a literal — a head assertion written as a
        // database-read literal is invisible to a `currentSchemaVersion` grep.
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        for (table, column) in Self.addedColumns {
            #expect(try probeColumnExists(in: dir, table: table, column: column), "\(table).\(column)")
        }
    }

    @Test("a store seeded two rungs back still reaches head, so V64 does not depend on running alone")
    func aV62StoreClimbsThroughV63ToHead() async throws {
        let dir = try makeTempDir(prefix: "V64FromV62")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)
        try rewindToV63(dir, keepingRows: false)
        try seedSchemaVersion(62, in: dir)

        try await store.migrateOnlyForTesting()
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        for (table, column) in Self.addedColumns {
            #expect(try probeColumnExists(in: dir, table: table, column: column), "\(table).\(column)")
        }
    }

    // MARK: - 3. NOTHING IS BACKFILLED, and the pre-V64 row proves it

    /// THE CENTRAL RAIL OF THIS SUITE.
    ///
    /// A pre-V64 row must survive the migration carrying NULL in all three new
    /// columns — not `''`, not `'unknown'`, not `'pre-v64'`. A sentinel would
    /// be counted by `count(DISTINCT launchId)`: one shared sentinel reports
    /// every pre-V64 row as ONE launch, a per-row sentinel reports them as as
    /// many launches as there are rows, and both are a value that names an
    /// absence read as a presence.
    ///
    /// The raw read distinguishes NULL from `''` deliberately: the `sqlite3`
    /// CLI renders both as an empty cell, so a reader following a device-pull
    /// recipe cannot tell them apart and this is the only place the difference
    /// is checkable.
    @Test("a pre-V64 row survives the migration carrying NO identity — NULL, never a sentinel")
    func aPreV64RowSurvivesTheMigrationCarryingNoIdentity() async throws {
        let dir = try makeTempDir(prefix: "V64NoBackfill")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)
        try rewindToV63(dir, keepingRows: true)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        for column in ["launchId", "sessionCrossingId", "launchArmingState"] {
            let cell = try rawOptionalText(
                in: dir,
                sql: "SELECT \(column) FROM background_download_drops WHERE id = 'v63-row'"
            )
            let value = try #require(cell, "the pre-V64 row must SURVIVE the migration")
            #expect(
                value == nil,
                """
                \(column) must be NULL on a pre-V64 row, and it reads \
                \(String(describing: value)). A sentinel here is counted by \
                count(DISTINCT launchId) and turns an absence into launches.
                """
            )
        }
        // And the row's ORIGINAL content is untouched — a migration that
        // rebuilt the table would pass the NULL checks above while having
        // silently discarded the history the ledger exists to hold.
        let arming = try #require(try await reopened.fetchBackgroundDownloadDropArming())
        #expect(arming.armedLaunches == 7)
        #expect(arming.dropWriteFailures == 2)
        #expect(arming.firstArmedAt == 100.0)
        #expect(arming.installedAt == 50.0)
        #expect(
            arming.lastArmedLaunchId == nil,
            "a launch id invented for an arming that happened before the column existed would be a fabricated identity"
        )

        // The Swift reader materializes it rather than refusing it: a NULL is a
        // legitimate absence, and only an UNDECODABLE value is a refusal.
        let page = try await reopened.fetchBackgroundDownloadDrops()
        #expect(page.rows.count == 1)
        #expect(page.unrecognizedLaunchArmingStateRows == 0)
        let row = try #require(page.rows.first)
        #expect(row.launchId == nil)
        #expect(row.sessionCrossingId == nil)
        #expect(row.launchArmingState == nil)
        #expect(row.episodeId == "ep-v63", "the rest of the row must be intact")
    }

    // MARK: - 4. The older shape must not BRICK the store

    /// THE BRICKING PRECEDENT, and the reason this rail is worth its length.
    ///
    /// `createBackgroundDownloadDropTables()` runs from `createTables()`, which
    /// is unconditional and runs BEFORE the ladder, so its `CREATE TABLE IF NOT
    /// EXISTS` is a NO-OP against a table that already exists in an older
    /// shape. When `dropWriteFailures` was added at V62 without an
    /// `addColumnIfNeeded`, the seed `INSERT` named a column that was not
    /// there, SQLite refused it, `createTables()` threw, the whole
    /// `runSchemaMigration()` transaction rolled back — AND THE STORE STOPPED
    /// OPENING AT ALL, surfacing as an unrelated trust-profile test failing.
    ///
    /// V64 adds four more columns and therefore owes four more
    /// `addColumnIfNeeded` calls. This is what makes forgetting one a red rail
    /// rather than a bricked device — and it is the direction the V62 rail
    /// cannot cover, because that one rebuilds only the ARMING table.
    @Test("a store carrying the PRE-V64 shape of BOTH tables still opens, and is repaired")
    func anOlderShapeIsRepairedRatherThanBrickingTheStore() async throws {
        let dir = try makeTempDir(prefix: "V64OldShape")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)
        try rewindToV63(dir, keepingRows: true)
        // Leave the STAMP at head, so every rung is gated out and only the
        // unconditional `createTables()` can repair this. That is the state a
        // device is in after a build that shipped the columns without the
        // repair, and the rung's own guard cannot help there.
        try seedSchemaVersion(AnalysisStore.currentSchemaVersion, in: dir)

        // The open must SUCCEED.
        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        for (table, column) in Self.addedColumns {
            #expect(try probeColumnExists(in: dir, table: table, column: column), "\(table).\(column)")
        }
        // And the store is genuinely USABLE afterwards, not merely open — the
        // distinction the V62 bricking made expensive.
        try await reopened.insertBackgroundDownloadDrop(
            BackgroundDownloadDropRecord(
                episodeId: "ep-after-repair",
                reason: .transferTaskNotVended,
                context: DownloadContext(
                    podcastId: "show", isExplicitDownload: true,
                    podcastTitle: "Show", episodeTitle: "Episode"
                ),
                boundSeconds: 10,
                launchId: "launch-after-repair",
                launchArmingState: .armed
            )
        )
        try await reopened.noteBackgroundDownloadDropInstrumentArmed(
            launchId: "launch-after-repair", at: 400.0
        )
        let arming = try #require(try await reopened.fetchBackgroundDownloadDropArming())
        #expect(arming.armedLaunches == 8, "the history must survive a column add")
        #expect(arming.lastArmedLaunchId == "launch-after-repair")
    }

    // MARK: - 5. Idempotence

    @Test("re-running the rung preserves the arming row and the identities on it")
    func theRungIsIdempotentAndNeverErasesIdentity() async throws {
        let dir = try makeTempDir(prefix: "V64Idempotent")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)

        try await store.noteBackgroundDownloadDropInstrumentArmed(
            launchId: "launch-one", at: 1000.0
        )
        try await store.noteBackgroundDownloadDropInstrumentArmed(
            launchId: "launch-two", at: 2000.0
        )
        let before = try #require(try await store.fetchBackgroundDownloadDropArming())
        #expect(before.armedLaunches == 2)
        #expect(before.lastArmedLaunchId == "launch-two")

        // Rewind only the STAMP, leaving the tables and their rows in place —
        // the shape a partially-rolled-back device is in, and the one where an
        // over-eager `CREATE`/`INSERT` would destroy history.
        try seedSchemaVersion(63, in: dir)
        try await store.migrateOnlyForTesting()

        let after = try #require(try await store.fetchBackgroundDownloadDropArming())
        #expect(after.armedLaunches == 2)
        #expect(after.firstArmedAt == before.firstArmedAt)
        #expect(after.lastArmedAt == before.lastArmedAt)
        #expect(after.lastArmedLaunchId == "launch-two")
        #expect(after.installedAt == before.installedAt)
        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
    }

    // MARK: - 6. `lastArmedLaunchId` moves with `lastArmedAt` and nothing else

    /// The two are ONE FACT SPELLED TWO WAYS. A `lastArmedLaunchId` that
    /// lagged its own timestamp would name a launch that is not the one
    /// `lastArmedAt` dates — worse than no launch id at all, because a reader
    /// comparing it against a drop row's `launchId` would get a confident wrong
    /// answer.
    @Test("lastArmedLaunchId follows every arming, and firstArmedAt still means THE FIRST")
    func lastArmedLaunchIdFollowsTheLatestArming() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        let seeded = try #require(try await store.fetchBackgroundDownloadDropArming())
        #expect(seeded.lastArmedLaunchId == nil, "a seeded row has never armed")

        try await store.noteBackgroundDownloadDropInstrumentArmed(launchId: "L1", at: 10.0)
        let one = try #require(try await store.fetchBackgroundDownloadDropArming())
        #expect(one.lastArmedLaunchId == "L1")
        #expect(one.firstArmedAt == 10.0)

        try await store.noteBackgroundDownloadDropInstrumentArmed(launchId: "L2", at: 20.0)
        let two = try #require(try await store.fetchBackgroundDownloadDropArming())
        #expect(two.lastArmedLaunchId == "L2")
        #expect(two.lastArmedAt == 20.0)
        #expect(
            two.firstArmedAt == 10.0,
            "firstArmedAt means THE FIRST TIME; there is deliberately no firstArmedLaunchId, so this stamp is all that anchors the start of the window"
        )
    }

    /// A WRITE FAILURE IS NOT AN ARMING, and NEITHER of the two paths through
    /// `noteBackgroundDownloadDropWriteFailure` may claim one.
    ///
    /// It has two, and a rail covering one of them is a rail about one of them:
    /// it UPDATES the arming row the V62 seed installed, and it RE-CREATES a
    /// row that has been deleted (a hand-edited or partially rolled-back
    /// store). Naming a launch on either would manufacture exactly the claim
    /// `armedLaunches = 0` exists to withhold — in the one column a drop row's
    /// `launchId` is compared against.
    @Test("a write failure names NO launch, on the update path and on the re-create path")
    func aWriteFailureNeverNamesALaunch() async throws {
        let dir = try makeTempDir(prefix: "V64FailureNoLaunch")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)

        // PATH ONE: the seeded row is there, so this is an UPDATE.
        let seeded = try #require(try await store.fetchBackgroundDownloadDropArming())
        #expect(seeded.armedLaunches == 0, "the V62 seed has never armed")
        try await store.noteBackgroundDownloadDropWriteFailure(at: 600.0)
        let updated = try #require(try await store.fetchBackgroundDownloadDropArming())
        #expect(updated.dropWriteFailures == 1)
        #expect(updated.armedLaunches == 0)
        #expect(updated.lastArmedAt == nil, "a failed write is not an arming and does not date one")
        #expect(
            updated.lastArmedLaunchId == nil,
            "…and it does not NAME one either, for the identical reason"
        )

        // PATH TWO: the row is gone, so this is a RE-CREATE.
        let db = try openRawReadWrite(dir)
        #expect(
            sqlite3_exec(db, "DELETE FROM \(Self.armingTable)", nil, nil, nil) == SQLITE_OK
        )
        sqlite3_close_v2(db)
        #expect(try await store.fetchBackgroundDownloadDropArming() == nil)

        try await store.noteBackgroundDownloadDropWriteFailure(at: 700.0)
        let rebuilt = try #require(try await store.fetchBackgroundDownloadDropArming())
        #expect(rebuilt.dropWriteFailures == 1)
        #expect(rebuilt.armedLaunches == 0)
        #expect(
            rebuilt.lastArmedLaunchId == nil,
            "a failure is not an arming; a launch id here would be a fabricated denominator entry"
        )
    }

    // MARK: - 7. Round trip, and the refusal that is NOT a NULL

    @Test("every value written round-trips exactly, for all four arming states")
    func identityRoundTrips() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        let context = DownloadContext(
            podcastId: "show-sdis", isExplicitDownload: false,
            podcastTitle: "Show", episodeTitle: "Episode"
        )
        for (index, state) in BackgroundDownloadDropLaunchArming.allCases.enumerated() {
            try await store.insertBackgroundDownloadDrop(
                BackgroundDownloadDropRecord(
                    episodeId: "ep-\(state.rawValue)",
                    reason: .sessionNotVended,
                    context: context,
                    boundSeconds: 10,
                    launchId: "launch-\(state.rawValue)",
                    launchArmingState: state,
                    sessionCrossingId: "crossing-\(state.rawValue)",
                    occurredAt: Double(1000 + index)
                )
            )
        }
        let page = try await store.fetchBackgroundDownloadDrops()
        #expect(page.rows.count == BackgroundDownloadDropLaunchArming.allCases.count)
        #expect(page.unrecognizedLaunchArmingStateRows == 0)
        for state in BackgroundDownloadDropLaunchArming.allCases {
            let row = try #require(page.rows.first { $0.episodeId == "ep-\(state.rawValue)" })
            #expect(row.launchId == "launch-\(state.rawValue)")
            #expect(row.sessionCrossingId == "crossing-\(state.rawValue)")
            #expect(row.launchArmingState == state)
        }
        #expect(
            BackgroundDownloadDropLaunchArming.allCases.count == 4,
            "four states, four round-trips. A fifth added without a raw value here is a state nothing proves survives the disk."
        )
    }

    /// UNKNOWN IS NOT `notAttempted`, AND IT IS NOT NULL EITHER.
    ///
    /// A row written by a build with a wider vocabulary is REFUSED and COUNTED,
    /// on the precedent `reason` and `unattributedReason` already set. Coercing
    /// it to `.notAttempted` would inflate the population that claims "this
    /// launch is not in the denominator"; mapping it to nil would say the row
    /// PREDATES V64, which a row carrying a value demonstrably does not.
    @Test("a launchArmingState this build cannot decode is counted, not coerced and not read as pre-V64")
    func anUndecodableArmingStateIsCountedRatherThanCoerced() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        let db = try openRawReadWrite(dir)
        let sql = """
            INSERT INTO background_download_drops
            (id, episodeId, reason, occurredAt, podcastId, unattributedReason,
             isExplicitDownload, boundSeconds, launchId, sessionCrossingId,
             launchArmingState)
            VALUES ('future', 'ep-future', 'session_not_vended', 1.0, 'show', NULL,
                    0, 10.0, 'launch-future', 'crossing-future', 'armed_by_a_later_build')
            """
        #expect(sqlite3_exec(db, sql, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        let page = try await store.fetchBackgroundDownloadDrops()
        #expect(page.rows.isEmpty, "the row must NOT reach `rows` wearing a state it does not carry")
        #expect(page.unrecognizedLaunchArmingStateRows == 1)
        #expect(page.unrecognizedReasonRows == 0, "the reason decoded fine; the losses are counted separately")
        #expect(page.unrecognizedUnattributedReasonRows == 0)
        #expect(
            page.totalRowsSeen == 1,
            "totalRowsSeen is the honest denominator and must include a row it could not read"
        )
    }

    // MARK: - 8. The page is DETERMINISTIC under equal timestamps

    /// N joiners of ONE crossing write rows whose `occurredAt` can be equal to
    /// the double's precision — which is the population this bead exists to
    /// make readable. `ORDER BY occurredAt DESC` alone leaves their order, and
    /// therefore WHICH of them a `limit` cuts off, unspecified: a page whose
    /// contents depend on SQLite's scan order reports `truncated` over an
    /// arbitrary set. The `, id DESC` tiebreaker is what makes the window a
    /// window.
    @Test("rows sharing one timestamp page deterministically, so a limit cuts a stated set")
    func equalTimestampsPageDeterministically() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        let context = DownloadContext(
            podcastId: "show-sdis", isExplicitDownload: false,
            podcastTitle: "Show", episodeTitle: "Episode"
        )
        // Inserted in an order that is neither the required output order nor
        // its reverse, so a query that fell back on rowid — in either
        // direction — returns something else.
        for suffix in ["a", "c", "b", "d"] {
            try await store.insertBackgroundDownloadDrop(
                BackgroundDownloadDropRecord(
                    episodeId: "ep-\(suffix)",
                    reason: .sessionNotVended,
                    context: context,
                    boundSeconds: 10,
                    launchId: "launch-tie",
                    launchArmingState: .armed,
                    sessionCrossingId: "crossing-tie",
                    occurredAt: 5000.0,
                    id: "row-\(suffix)"
                )
            )
        }
        // THE RAIL IS VACUOUS UNLESS THE STAMPS ARE GENUINELY EQUAL — if they
        // are not, `occurredAt DESC` is doing all the work and the tiebreaker
        // is never exercised. Read raw, because the Swift row carries a Double
        // that a reader would have to compare by hand.
        let db = try openRawReadWrite(dir)
        var stmt: OpaquePointer?
        #expect(
            sqlite3_prepare_v2(
                db, "SELECT count(DISTINCT occurredAt) FROM \(Self.dropsTable)", -1, &stmt, nil
            ) == SQLITE_OK
        )
        #expect(sqlite3_step(stmt) == SQLITE_ROW)
        #expect(
            sqlite3_column_int64(stmt, 0) == 1,
            "four rows must share ONE stamp, or this rail measures the timestamp and not the tiebreaker"
        )
        sqlite3_finalize(stmt)
        sqlite3_close_v2(db)

        let full = try await store.fetchBackgroundDownloadDrops()
        #expect(full.rows.map(\.id) == ["row-d", "row-c", "row-b", "row-a"])
        let capped = try await store.fetchBackgroundDownloadDrops(limit: 2)
        #expect(capped.truncated)
        #expect(
            capped.rows.map(\.id) == ["row-d", "row-c"],
            "the window must be the same two rows on every read, or `truncated` names a set nobody can state"
        )
    }

    // MARK: - 9. The three readings, end to end on the disk

    /// THE QUERIES THE BEAD EXISTS FOR, run against real rows.
    ///
    /// One outage that cost forty episodes, forty separate outages, and a
    /// launch where nobody was counting are three DIFFERENT answers now, and
    /// this is the rail that states them as SQL rather than as prose. It uses
    /// raw SQL deliberately: that is what a human runs against a device pull,
    /// and the Swift reader has no production caller.
    @Test("one outage, forty outages, and nobody counting are three different queries")
    func theThreeReadingsAreDistinguishable() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        let context = DownloadContext(
            podcastId: "show-sdis", isExplicitDownload: false,
            podcastTitle: "Show", episodeTitle: "Episode"
        )
        func insert(
            _ episode: String, launch: String, crossing: String?,
            arming: BackgroundDownloadDropLaunchArming, at when: Double
        ) async throws {
            try await store.insertBackgroundDownloadDrop(
                BackgroundDownloadDropRecord(
                    episodeId: episode, reason: .sessionNotVended, context: context,
                    boundSeconds: 10, launchId: launch, launchArmingState: arming,
                    sessionCrossingId: crossing, occurredAt: when, id: "id-\(episode)"
                )
            )
        }
        // ONE outage on launch A: four episodes, ONE crossing.
        for index in 1...4 {
            try await insert("A\(index)", launch: "launch-A", crossing: "crossing-A", arming: .armed, at: 100.0)
        }
        // THREE separate outages on launch B: three episodes, THREE crossings —
        // the shape a launch that retried repeatedly produces, and the one
        // `launchId` alone reports identically to the four rows above.
        for index in 1...3 {
            try await insert("B\(index)", launch: "launch-B", crossing: "crossing-B\(index)", arming: .armed, at: 200.0)
        }
        // And one drop on a DEGRADED launch — the store opened lazily, the
        // launch Task never armed, so this row's launch is in NO denominator.
        try await insert("C1", launch: "launch-C", crossing: "crossing-C", arming: .notAttempted, at: 300.0)
        try await store.noteBackgroundDownloadDropInstrumentArmed(launchId: "launch-A", at: 90.0)
        try await store.noteBackgroundDownloadDropInstrumentArmed(launchId: "launch-B", at: 190.0)

        func scalar(_ sql: String) throws -> Int {
            let dbURL = dir.appendingPathComponent("analysis.sqlite")
            var db: OpaquePointer?
            #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK)
            defer { sqlite3_close_v2(db) }
            var stmt: OpaquePointer?
            #expect(sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK)
            defer { sqlite3_finalize(stmt) }
            #expect(sqlite3_step(stmt) == SQLITE_ROW)
            return Int(sqlite3_column_int64(stmt, 0))
        }

        let where7dgx = "FROM background_download_drops WHERE reason='session_not_vended'"
        #expect(try scalar("SELECT count(*) \(where7dgx)") == 8, "EPISODES — the only number V62 could produce")
        #expect(
            try scalar("SELECT count(DISTINCT sessionCrossingId) \(where7dgx)") == 5,
            "DAEMON REFUSALS: one on launch A, three on launch B, one on launch C"
        )
        #expect(
            try scalar("SELECT count(DISTINCT launchId) \(where7dgx)") == 3,
            "LAUNCHES — the numerator that is finally in the same unit as armedLaunches"
        )
        // AND THE TWO POPULATIONS ARE NOT THE SAME SHAPE, which is the point:
        // launch A is 4 episodes from 1 refusal, launch B is 3 from 3.
        #expect(
            try scalar("SELECT count(DISTINCT sessionCrossingId) \(where7dgx) AND launchId='launch-A'") == 1
        )
        #expect(
            try scalar("SELECT count(DISTINCT sessionCrossingId) \(where7dgx) AND launchId='launch-B'") == 3,
            "three refusals inside ONE launch: this is why launchId alone cannot separate the populations"
        )
        // NOBODY COUNTING — identifiable per ROW, which is the reading
        // playhead-7dgx documented and could not measure.
        #expect(
            try scalar("SELECT count(*) \(where7dgx) AND launchArmingState <> 'armed'") == 1
        )
        #expect(
            try scalar("SELECT count(DISTINCT launchId) \(where7dgx) AND launchArmingState = 'armed'") == 2,
            "two launches are genuinely in the denominator; the third is not, and the row says so"
        )
        let arming = try #require(try await store.fetchBackgroundDownloadDropArming())
        #expect(arming.armedLaunches == 2, "the denominator, in LAUNCHES, matching the numerator's unit")
        #expect(arming.lastArmedLaunchId == "launch-B")
    }

    // MARK: - 10. The drift guard

    @Test("head is 64")
    func headIsSixtyFour() {
        // 63 -> 64 read for this rung (playhead-sdis): V64 ADDS FOUR NULLABLE
        // COLUMNS and only to the two playhead-7dgx tables. No other table, no
        // UPDATE, no DELETE, no DEFAULT and no backfill.
        // 64 -> 65 read for this rung (playhead-1gu0): V65 RENAMES ONE COLUMN —
        // `semantic_scan_results.runCorrelationId` becomes `backfillJobId`, and its
        // index moves with it. A pure `ALTER TABLE … RENAME COLUMN`: no row moves, no
        // value is written, nothing is backfilled and no other table is named. It names
        // nothing this rung asserts, so no assertion here moves.
        #expect(AnalysisStore.currentSchemaVersion == 66)
    }
}
