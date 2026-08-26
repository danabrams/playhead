// BackgroundDownloadDropIdentityTests.swift
// playhead-sdis — THE QUERIES A DEVICE PULL RUNS, against the FILE.
//
// `count(*)` on `background_download_drops` counts EPISODES AFFECTED and was
// being read as a number of daemon OUTAGES. Three suites make the two
// separable and each owns one layer, so a rail belongs to exactly one of them:
//
//   * `BackgroundDownloadDropLaunchIdentityV64MigrationTests` — the SCHEMA.
//     What the rung adds, what it must not backfill, what must not brick.
//   * `BackgroundDownloadDropOutageIdentityTests` — the WRITE PATH. What
//     `DownloadManager` stamps on a row, on each of the three drop sites.
//   * THIS FILE — the READING. Multi-row populations mixed in ONE table and
//     told apart by SQL run through a SECOND CONNECTION, exactly as
//     `sqlite3 analysis.sqlite` would on a pull.
//
// WHY THE QUERY AND NOT THE SWIFT PAGE. `fetchBackgroundDownloadDrops` has NO
// production caller — playhead-7dgx says so in its own bd comment, and the
// reason is that a human on a device pull runs raw SQL. The Swift reader is
// also deliberately LOSSY (it skips rows whose enums it cannot decode and
// counts them separately), so a rail written only against `page.rows` measures
// a different population from the one the answer will come from. The one
// exception is section 4's `no_recorder` rail, which is about the reader's
// VOCABULARY and says so.
//
// TEN RAILS WERE DELETED FROM THIS FILE RATHER THAN MERGED, because two rails
// asserting one property is noise. Everything about a SINGLE row's stamps —
// which launch, which crossing, which arming state, on which of the three drop
// sites — lives in the write-path suite; everything about what the rung does
// to a store lives in the schema suite. What is left here is the set of
// readings that need MORE THAN ONE ROW to mean anything, plus the two
// strengthenings folded into their counterparts rather than duplicated (the
// `count(DISTINCT)`-declines-NULL check, and the episodes-are-not-launches
// discriminator).
//
// ─────────────────────────────────────────────────────────────────────────
// THE THREE POPULATIONS, AND WHY TWO COLUMNS ARE NEEDED TO SEPARATE THEM
// ─────────────────────────────────────────────────────────────────────────
// Two independent mechanisms multiply rows, and neither column covers both:
//
//   * A REFUSAL IS NEVER CACHED. One launch can hold many refusals, so
//     `launchId` alone tells "one launch" from "forty launches" and says
//     nothing about how many refusals happened inside one of them.
//   * ONE REFUSAL MINTS N ROWS. `backgroundSession(for:requestedBy:)` lets
//     concurrent callers JOIN an in-flight crossing; every joiner gives up on
//     the FIRST caller's deadline and writes its own row. `sessionCrossingId`
//     is what those rows share.
//
// A column that separated only ONE of the two axes would pass a rail built on
// the other, which is why `mixedPopulationsAreSeparated` builds both shapes
// into one table and reads them with one `GROUP BY` rather than building each
// in a table of its own.
//
// ─────────────────────────────────────────────────────────────────────────
// HOW THE JOINED CROSSING IS REACHED AT ALL
// ─────────────────────────────────────────────────────────────────────────
// It needs a refusal that SUSPENDS, and until this bead no seam had one —
// `neverAnswers`, `refusesCallsLabelled` and `intermittentlyRefusesCallsLabelled`
// all answer synchronously, so the in-flight entry is written and cleared
// inside one uninterrupted crossing and a second caller can never find it.
// `BackgroundSessionIO.Behavior.suspendsThenRefusesCallsLabelled` is that
// seam; its own docs carry the argument for why it suspends rather than
// blocking and why it is not a held queue plus an expiring bound.
//
// EVERY WAIT HERE IS ON AN EVENT THAT BOTH IMPLEMENTATIONS REACH, never on a
// clock — `playhead-gpdb` R1's rule, and the reason the 2026-08-13 merge gate
// rejected the version of that rail which waited a fixed number of ticks.

import Foundation
import Testing
import SQLite3
@testable import Playhead

@Suite("background_download_drops – one OUTAGE is not forty EPISODES (playhead-sdis)")
struct BackgroundDownloadDropIdentityTests {

    // MARK: - Seams and helpers

    /// Deliberately NOT `BackgroundSessionIO.defaultTimeout`: asserting against
    /// the shipped constant compares it with itself. Never waited out — every
    /// seam below refuses rather than expiring.
    private static let bound: TimeInterval = 7.5

    /// Refuses the session crossing SYNCHRONOUSLY. Each caller therefore starts
    /// and finishes its own crossing, which is the SEPARATE-OUTAGE population.
    private static func creationRefusingIO() -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: .refusesCallsLabelled(
                DownloadManager.sessionCreationLabelPrefix
            ),
            timeout: bound,
            queueLabel: "sdis.test.creation-refused.\(UUID().uuidString)"
        )
    }

    /// Refuses the session crossing only once `gate` opens — the JOINED-CROSSING
    /// population. See the file header.
    private static func heldThenRefusingIO(
        gate: SuspendingSeamGate
    ) -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: .suspendsThenRefusesCallsLabelled(
                DownloadManager.sessionCreationLabelPrefix,
                until: { await gate.wait() }
            ),
            timeout: bound,
            queueLabel: "sdis.test.joined-crossing.\(UUID().uuidString)"
        )
    }

    /// Waits until `condition` holds, and reports whether it ever did.
    ///
    /// Every caller waits on an event that is GUARANTEED to happen — a caller
    /// arriving at the crossing decision — so load can make the wait longer and
    /// can never change the answer. The deadline is a safety net so a genuinely
    /// broken build fails instead of hanging the plan, and is deliberately
    /// enormous next to what the wait costs; it is `BackgroundSessionCreationTests`'
    /// number for the same reason.
    private static func waited(until condition: () async -> Bool) async -> Bool {
        let deadline = ContinuousClock.now + .seconds(120)
        while ContinuousClock.now < deadline {
            if await condition() { return true }
            try? await Task.sleep(for: .milliseconds(2))
        }
        return await condition()
    }

    private static func manager(
        store: AnalysisStore?,
        sessionIO: BackgroundSessionIO,
        launchId: String? = nil
    ) throws -> DownloadManager {
        let recorder: any BackgroundDownloadDropRecording
        if let store {
            recorder = AnalysisStoreBackgroundDownloadDropRecorder(store: store)
        } else {
            recorder = NoopBackgroundDownloadDropRecorder()
        }
        // `launchId` is passed only where a rail needs to NAME a launch in a
        // SQL literal. Everywhere else the production default (a fresh UUID)
        // runs, so the rails exercise the shipped spelling.
        if let launchId {
            return DownloadManager(
                cacheDirectory: try makeTempDir(prefix: "sdisCache"),
                sessionIO: sessionIO,
                dropRecorder: recorder,
                launchId: launchId
            )
        }
        return DownloadManager(
            cacheDirectory: try makeTempDir(prefix: "sdisCache"),
            sessionIO: sessionIO,
            dropRecorder: recorder
        )
    }

    private static func context() -> DownloadContext {
        DownloadContext(
            podcastId: "show-sdis",
            isExplicitDownload: false,
            podcastTitle: "Show",
            episodeTitle: "Episode"
        )
    }

    private static func drive(
        _ manager: DownloadManager, episodeId: String
    ) async {
        await manager.backgroundDownload(
            episodeId: episodeId,
            from: URL(string: "https://cdn.example.com/\(episodeId).mp3")!,
            context: context()
        )
    }

    // MARK: - Raw SQL, because that is where the answer comes from

    /// Runs `sql` against the store FILE through a SECOND connection, and
    /// returns every row as text (NULL as `nil`).
    ///
    /// The SQL is what a device pull runs; the connection flags are not.
    /// `AnalysisStore` sets `journal_mode = WAL`, and a WAL database admits a
    /// `SQLITE_OPEN_READONLY` connection only while some other connection holds
    /// the `-shm` — so a read-only probe here would pass or fail depending on
    /// whether the store's own handle happened to still be up, which is a
    /// measurement of SQLite's file locking rather than of this bead.
    /// `SQLITE_OPEN_READWRITE` is what every other probe in this tree uses, and
    /// nothing below writes through it except `writeBehindTheStore`, which says
    /// so in its name.
    ///
    /// `busy_timeout` because the store's own connection is live on the same
    /// file and a concurrent reader would otherwise get `SQLITE_BUSY` at once,
    /// failing the rail for a reason that is not its subject.
    private static func query(
        _ directory: URL, _ sql: String
    ) throws -> [[String?]] {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK else {
            throw StoreProbeFailure.couldNotOpen(dbURL.path)
        }
        defer { sqlite3_close_v2(db) }
        sqlite3_busy_timeout(db, 5000)
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw StoreProbeFailure.couldNotPrepare(
                sql, String(cString: sqlite3_errmsg(db))
            )
        }
        defer { sqlite3_finalize(stmt) }
        var out: [[String?]] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            var row: [String?] = []
            for column in 0..<sqlite3_column_count(stmt) {
                if let raw = sqlite3_column_text(stmt, column) {
                    row.append(String(cString: raw))
                } else {
                    row.append(nil)
                }
            }
            out.append(row)
        }
        return out
    }

    /// One row, one column, as an `Int`. Throws rather than defaulting: a
    /// zero invented from a missing row is exactly the reading this whole
    /// instrument exists to refuse.
    private static func scalar(_ directory: URL, _ sql: String) throws -> Int {
        let rows = try query(directory, sql)
        guard rows.count == 1, let first = rows.first?.first, let text = first else {
            throw StoreProbeFailure.notASingleValue(sql, rows.count)
        }
        guard let value = Int(text) else {
            throw StoreProbeFailure.notAnInteger(sql, text)
        }
        return value
    }

    private enum StoreProbeFailure: Error, CustomStringConvertible {
        case couldNotOpen(String)
        case couldNotPrepare(String, String)
        case notASingleValue(String, Int)
        case notAnInteger(String, String)

        var description: String {
            switch self {
            case .couldNotOpen(let path):
                return "could not open \(path) read-only"
            case .couldNotPrepare(let sql, let message):
                return "could not prepare `\(sql)`: \(message)"
            case .notASingleValue(let sql, let count):
                return "`\(sql)` returned \(count) rows; a scalar query must return exactly one"
            case .notAnInteger(let sql, let text):
                return "`\(sql)` returned `\(text)`, which is not an integer"
            }
        }
    }

    /// Opens and migrates a store, and forces the tables into existence so a
    /// second connection can read them.
    private static func openedStore() async throws -> (AnalysisStore, URL) {
        let (store, dir) = try await makeTestStoreWithDirectory()
        // `AnalysisStore` opens LAZILY. Without this the file on disk may not
        // exist yet and the read-only probe below would fail for a reason
        // unrelated to any rail.
        _ = try await store.fetchBackgroundDownloadDrops()
        return (store, dir)
    }

    /// THE QUERY. Verbatim from `BackgroundDownloadDropLedger.swift`'s header,
    /// so a rail failing here is a rail saying the documented recipe stopped
    /// working.
    private static let threeReadings = """
        SELECT count(*)                           AS episodes_affected,
               count(DISTINCT sessionCrossingId)  AS daemon_refusals,
               count(DISTINCT launchId)           AS launches_affected
        FROM background_download_drops WHERE reason='session_not_vended'
        """

    private static func readings(_ directory: URL) throws -> (
        episodes: Int, refusals: Int, launches: Int
    ) {
        let rows = try query(directory, threeReadings)
        guard rows.count == 1, rows[0].count == 3 else {
            throw StoreProbeFailure.notASingleValue(threeReadings, rows.count)
        }
        func number(_ index: Int) throws -> Int {
            guard let text = rows[0][index], let value = Int(text) else {
                throw StoreProbeFailure.notAnInteger(
                    threeReadings, rows[0][index] ?? "NULL"
                )
            }
            return value
        }
        return (try number(0), try number(1), try number(2))
    }

    // MARK: - 1. The three readings, each from the SAME query




    /// ALL THREE POPULATIONS IN ONE TABLE, told apart by ONE `GROUP BY`.
    ///
    /// The three rails above each see a table containing only their own
    /// population, so each of them would pass against a column that happened to
    /// be constant within a test. This one mixes a joined outage, two lone
    /// refusals and a second launch into one file and asks the question a pull
    /// actually asks: for each launch, how many episodes and how many refusals?
    @Test("mixed populations in one table are separated by launch AND by crossing")
    func mixedPopulationsAreSeparated() async throws {
        let (store, dir) = try await Self.openedStore()

        // Launch one: a joined crossing that cost three episodes.
        let gate = SuspendingSeamGate()
        let joined = try Self.manager(
            store: store,
            sessionIO: Self.heldThenRefusingIO(gate: gate),
            launchId: "launch-joined"
        )
        try await joined.bootstrap()
        await joined.armDropLedger()
        let allArrived: Bool = await withTaskGroup(of: Void.self) { group in
            for index in 0..<3 {
                group.addTask { await Self.drive(joined, episodeId: "ep-mixed-join-\(index)") }
            }
            let arrived = await Self.waited {
                await joined.sessionCrossingArrivalsForTesting == 3
            }
            await gate.open()
            return arrived
        }
        #expect(allArrived, "the joined half of this fixture is vacuous unless all three overlapped")

        // Launch two: two refusals that share nothing but the process.
        let lonely = try Self.manager(
            store: store,
            sessionIO: Self.creationRefusingIO(),
            launchId: "launch-lonely"
        )
        try await lonely.bootstrap()
        await lonely.armDropLedger()
        await Self.drive(lonely, episodeId: "ep-mixed-lone-0")
        await Self.drive(lonely, episodeId: "ep-mixed-lone-1")

        // The table as a whole: 5 episodes, 3 refusals, 2 launches. Every one
        // of the three numbers is different, so no pair of them can be swapped
        // without a rail moving.
        let reading = try Self.readings(dir)
        #expect(reading.episodes == 5)
        #expect(reading.refusals == 3, "one joined crossing plus two lone ones")
        #expect(reading.launches == 2)

        // And PER LAUNCH, which is the query a reader runs next.
        let perLaunch = try Self.query(dir, """
            SELECT launchId, count(*), count(DISTINCT sessionCrossingId)
            FROM background_download_drops
            WHERE reason='session_not_vended'
            GROUP BY launchId ORDER BY launchId
            """)
        #expect(
            perLaunch == [
                ["launch-joined", "3", "1"],
                ["launch-lonely", "2", "2"],
            ],
            """
            three episodes lost to ONE refusal in one launch, and two episodes \
            lost to TWO refusals in another. `count(*)` alone reads 3 and 2 and \
            says nothing about which is the outage. Got: \(perLaunch)
            """
        )
    }



    // MARK: - 2. Rows written when NOBODY WAS COUNTING

    /// A PRE-V64 ROW IS `launchId IS NULL`, AND `count(DISTINCT)` DECLINES TO
    /// COUNT IT.
    ///
    /// This is the sentinel trap, railed in both directions. A shared sentinel
    /// (`''`, `'pre-v64'`) collapses every pre-V64 row into ONE launch; a
    /// per-row sentinel expands them into as many launches as there are rows.
    /// Both are values that name an ABSENCE read as a presence. NULL is the one
    /// spelling that adds nothing, and the rail below is what proves the
    /// shipped schema chose it: two legacy rows and one real launch must read
    /// `launches = 1`, not 2 and not 3.
    @Test("rows written before V64 are launchId IS NULL and add nothing to the launch count")
    func preV64RowsAreNullAndUncounted() async throws {
        let (store, dir) = try await Self.openedStore()
        let manager = try Self.manager(
            store: store,
            sessionIO: Self.creationRefusingIO(),
            launchId: "launch-modern"
        )
        try await manager.bootstrap()
        await manager.armDropLedger()
        await Self.drive(manager, episodeId: "ep-modern")

        // Two rows as a pre-V64 build wrote them: every new column absent.
        try Self.writeBehindTheStore(dir, """
            INSERT INTO background_download_drops
            (id, episodeId, reason, occurredAt, podcastId, unattributedReason,
             isExplicitDownload, boundSeconds)
            VALUES ('legacy-1', 'ep-legacy-1', 'session_not_vended', 1.0, NULL, NULL, 0, 10.0),
                   ('legacy-2', 'ep-legacy-2', 'session_not_vended', 2.0, NULL, NULL, 0, 10.0)
            """)

        let reading = try Self.readings(dir)
        #expect(reading.episodes == 3, "three rows, and every one of them is a real lost episode")
        #expect(
            reading.launches == 1,
            """
            ONE launch. A shared sentinel would read 2 (the legacy rows folded \
            into one phantom launch) and a per-row sentinel 3 — both of them a \
            value that names an absence being counted as a presence
            """
        )
        #expect(
            reading.refusals == 1,
            "the same argument one column along: two legacy rows are not two daemon refusals"
        )

        // And the population is ADDRESSABLE rather than merely uncounted: a
        // reader must be able to ask how much of the table predates the rung.
        #expect(
            try Self.scalar(dir, """
                SELECT count(*) FROM background_download_drops WHERE launchId IS NULL
                """) == 2,
            "`launchId IS NULL` is how a pull sizes what this build cannot say anything about"
        )
    }

    /// `launchArmingState` PARTITIONS THE TABLE, and its NULL means the third
    /// thing.
    ///
    /// Three states in one file, from three different causes, told apart by one
    /// `GROUP BY`:
    ///   * `armed`         — this row's launch IS in `armedLaunches`;
    ///   * `not_attempted` — it is NOT, and never will be counted for this row;
    ///   * NULL            — the row predates V64 and nothing can be said.
    /// Folding any pair of those together is the collapse the column exists to
    /// prevent, and each of the three has a different remedy for a reader.
    @Test("launchArmingState separates counted, uncounted and unknowable rows")
    func armingStatePartitionsTheTable() async throws {
        let (store, dir) = try await Self.openedStore()

        let armed = try Self.manager(
            store: store, sessionIO: Self.creationRefusingIO(), launchId: "launch-armed"
        )
        try await armed.bootstrap()
        await armed.armDropLedger()
        await Self.drive(armed, episodeId: "ep-armed")

        // A launch that never armed. In production this is the DEGRADED launch:
        // `openAtLaunch` failed, so `PlayheadRuntime`'s launch Task returned
        // before the arming — and the store opens LAZILY, so this row lands
        // anyway. It is also the shape of a drop that simply raced the Task.
        let unarmed = try Self.manager(
            store: store, sessionIO: Self.creationRefusingIO(), launchId: "launch-degraded"
        )
        try await unarmed.bootstrap()
        await Self.drive(unarmed, episodeId: "ep-degraded")

        try Self.writeBehindTheStore(dir, """
            INSERT INTO background_download_drops
            (id, episodeId, reason, occurredAt, podcastId, unattributedReason,
             isExplicitDownload, boundSeconds)
            VALUES ('legacy-3', 'ep-legacy-3', 'session_not_vended', 1.0, NULL, NULL, 0, 10.0)
            """)

        let grouped = try Self.query(dir, """
            SELECT ifnull(launchArmingState, 'PRE-V64'), count(*), count(DISTINCT launchId)
            FROM background_download_drops GROUP BY 1 ORDER BY 1
            """)
        #expect(
            grouped == [
                ["PRE-V64", "1", "0"],
                ["armed", "1", "1"],
                ["not_attempted", "1", "1"],
            ],
            """
            three rows, three states, and the pre-V64 row contributes ZERO to \
            the launch count in its own group as well as in the total. Got: \(grouped)
            """
        )
    }

    // MARK: - 3. The arming join — two quantities, finally one unit


    /// AND THE CASE THAT MAKES THE RATIO HONEST: a launch that dropped without
    /// arming pushes the NUMERATOR ABOVE THE DENOMINATOR.
    ///
    /// playhead-7dgx documented this as reachable and could not measure it: the
    /// store opens LAZILY, so a DEGRADED launch — one whose `openAtLaunch`
    /// failed and whose launch Task returned before the arming — can still land
    /// a drop row. `armedLaunches` is a LOWER BOUND on counting launches and NOT
    /// an upper bound on recording ones, and a rail that only ever built the
    /// equal case would let a reader treat it as one.
    @Test("a DEGRADED launch drops without arming, and the numerator exceeds the denominator")
    func aDegradedLaunchExceedsTheDenominator() async throws {
        let (store, dir) = try await Self.openedStore()

        let counted = try Self.manager(
            store: store, sessionIO: Self.creationRefusingIO(), launchId: "launch-counted"
        )
        try await counted.bootstrap()
        await counted.armDropLedger()
        await Self.drive(counted, episodeId: "ep-counted")

        // The degraded launch. `armDropLedger()` is NEVER called, which is
        // exactly what `PlayheadRuntime` does when `openAtLaunch` reports the
        // store closed — and the drop still lands, through the lazy open.
        let degraded = try Self.manager(
            store: store, sessionIO: Self.creationRefusingIO(), launchId: "launch-degraded"
        )
        try await degraded.bootstrap()
        await Self.drive(degraded, episodeId: "ep-degraded")

        let launches = try Self.scalar(
            dir, "SELECT count(DISTINCT launchId) FROM background_download_drops"
        )
        let armed = try Self.scalar(
            dir, "SELECT armedLaunches FROM background_download_drop_arming"
        )
        #expect(launches == 2)
        #expect(armed == 1)
        #expect(
            launches > armed,
            """
            REACHABLE AND MEANINGFUL, not a contradiction: `armedLaunches` is a \
            lower bound on launches that were COUNTING and says nothing about \
            launches that RECORDED
            """
        )

        // …and the table says WHICH row is the uncounted one, which is the part
        // that was unmeasurable before this bead.
        let uncounted = try Self.query(dir, """
            SELECT launchId, episodeId FROM background_download_drops
            WHERE launchArmingState IS NOT 'armed' ORDER BY launchId
            """)
        #expect(
            uncounted == [["launch-degraded", "ep-degraded"]],
            """
            one row, named. Without launchArmingState a reader could see \
            2 > 1 and could not tell which of the two rows was outside the \
            denominator. Got: \(uncounted)
            """
        )
    }

    /// A DEGRADED LAUNCH'S DROP IS IDENTIFIABLE ON DISK, through a second
    /// connection, with no Swift in the path.
    ///
    /// The rail above reads the same file through the same helper as everything
    /// else; this one is the device-pull rehearsal. It asserts the three things
    /// a pull would see and nothing else: the row exists, it NAMES its launch,
    /// and it says that launch is not in the denominator — while the arming row
    /// says nobody armed at all.
    @Test("a drop on a launch that never armed is legible from the FILE alone")
    func aDegradedLaunchIsLegibleFromTheFile() async throws {
        let (store, dir) = try await Self.openedStore()
        let degraded = try Self.manager(
            store: store, sessionIO: Self.creationRefusingIO(), launchId: "launch-only-degraded"
        )
        try await degraded.bootstrap()
        // No `armDropLedger()`, on purpose. This is the DEGRADED launch.
        await Self.drive(degraded, episodeId: "ep-lazy-open")

        let row = try Self.query(dir, """
            SELECT episodeId, launchId, launchArmingState
            FROM background_download_drops
            """)
        #expect(
            row == [["ep-lazy-open", "launch-only-degraded", "not_attempted"]],
            "the whole finding, on one row, from the file: \(row)"
        )

        let arming = try Self.query(dir, """
            SELECT armedLaunches, firstArmedAt, lastArmedAt, lastArmedLaunchId
            FROM background_download_drop_arming
            """)
        #expect(
            arming == [["0", nil, nil, nil]],
            """
            NOBODY WAS COUNTING, and the arming row says so in four places \
            rather than by being absent. A zero date or a sentinel launch id \
            here would date an arming that never happened. Got: \(arming)
            """
        )
    }

    /// `lastArmedLaunchId` IS THE JOIN, AND IT IS NOT A SET.
    ///
    /// It names exactly one launch — the most recent to arm — and it is here so
    /// the two tables share a vocabulary at all; before it they shared no
    /// column. The rail asserts both halves: the join works for the launch it
    /// names, and it goes STALE for the earlier one, which is precisely why the
    /// per-row question is answered by `launchArmingState` instead.
    @Test("lastArmedLaunchId joins the two tables, and names ONE launch rather than a set")
    func lastArmedLaunchIdJoinsButIsNotASet() async throws {
        let (store, dir) = try await Self.openedStore()
        for index in 0..<2 {
            let manager = try Self.manager(
                store: store,
                sessionIO: Self.creationRefusingIO(),
                launchId: "launch-join-\(index)"
            )
            try await manager.bootstrap()
            await manager.armDropLedger()
            await Self.drive(manager, episodeId: "ep-join-row-\(index)")
        }

        // The documented join, verbatim from `BackgroundDownloadDropArming`'s
        // own doc comment.
        let mostRecent = try Self.scalar(dir, """
            SELECT count(*) FROM background_download_drops WHERE launchId =
            (SELECT lastArmedLaunchId FROM background_download_drop_arming)
            """)
        #expect(mostRecent == 1, "the launch that armed most recently also dropped one download")

        let named = try Self.query(dir, """
            SELECT lastArmedLaunchId, armedLaunches FROM background_download_drop_arming
            """)
        #expect(
            named == [["launch-join-1", "2"]],
            """
            TWO launches armed and the row names ONE of them. Reading this \
            column as the set of armed launches would lose `launch-join-0` \
            entirely — which is why it is documented as a join key and not as \
            membership. Got: \(named)
            """
        )
    }


    // MARK: - 4. The arming state is sampled AT THE WRITE, not at construction

    /// ONE LAUNCH, TWO ROWS, TWO DIFFERENT ARMING STATES.
    ///
    /// A drop that RACES the launch Task genuinely was written before the
    /// arming, and a row saying so is the measurement — which is why
    /// `dropLedgerArming` is read on the actor at the moment of the write
    /// rather than captured when the record is constructed. Capturing it at
    /// construction would make both rows below read `not_attempted`; capturing
    /// it once per launch would make both read whatever the launch ended up as.
    /// Both survive every other rail in this file.
    @Test("a drop before the arming and a drop after it disagree, in ONE launch")
    func theArmingStateIsSampledAtTheWrite() async throws {
        let (store, dir) = try await Self.openedStore()
        let manager = try Self.manager(
            store: store, sessionIO: Self.creationRefusingIO(), launchId: "launch-racing"
        )
        try await manager.bootstrap()

        await Self.drive(manager, episodeId: "ep-before-arming")
        await manager.armDropLedger()
        await Self.drive(manager, episodeId: "ep-after-arming")

        let rows = try Self.query(dir, """
            SELECT episodeId, launchId, launchArmingState
            FROM background_download_drops ORDER BY episodeId
            """)
        #expect(
            rows == [
                ["ep-after-arming", "launch-racing", "armed"],
                ["ep-before-arming", "launch-racing", "not_attempted"],
            ],
            """
            one process, one launch id, two different answers to "is this row's \
            launch in the denominator" — and both answers are true at the \
            moment they were written. Got: \(rows)
            """
        )
    }


    /// `no_recorder` is EXPRESSIBLE, and unreachable on a persisted row.
    ///
    /// Arming and recording go to the SAME injected `dropRecorder`, so a
    /// conformer that returns `.notRecording` for one returns it for the other
    /// and no row lands — which is why this is asserted on the enum and on the
    /// empty table rather than on a row. It exists anyway because folding it
    /// into `arming_failed` would report a write that FAILED where nothing was
    /// attempted: the collapse `BackgroundDownloadDropWriteOutcome` exists to
    /// prevent, one table along, and the one that sent a reader to diagnose
    /// SQLite on a device whose only fault was its wiring.
    @Test("no_recorder is a state the READ path can express, and no wired build can write one")
    func noRecorderIsExpressibleAndUnpersistable() async throws {
        // 1. THE VOCABULARY IS COMPLETE ON THE READ PATH. If it were not, a row
        //    carrying `no_recorder` — written by some later build, or by a hand
        //    edit — would be counted as UNREADABLE, which is a different claim
        //    from "this launch had no recorder". Round-tripped through the
        //    store rather than through the enum alone, because the enum is not
        //    what a pull reads.
        let (store, dir) = try await Self.openedStore()
        try Self.writeBehindTheStore(dir, """
            INSERT INTO background_download_drops
            (id, episodeId, reason, occurredAt, podcastId, unattributedReason,
             isExplicitDownload, boundSeconds, launchId, sessionCrossingId,
             launchArmingState)
            VALUES ('row-norec', 'ep-norec', 'session_not_vended', 1.0, NULL, NULL, 0, 10.0,
                    'launch-norec', 'crossing-norec', 'no_recorder')
            """)
        let page = try await store.fetchBackgroundDownloadDrops()
        #expect(page.rows.count == 1)
        #expect(page.rows.first?.launchArmingState == .noRecorder)
        #expect(
            page.unrecognizedLaunchArmingStateRows == 0,
            "a state the vocabulary HAS must not be counted as one it does not"
        )
        #expect(
            BackgroundDownloadDropLaunchArming.allCases.count == 4,
            "four states. A fifth without a rail here is a value a pull cannot interpret."
        )

        // 2. AND A WIRED BUILD CANNOT PRODUCE ONE. Arming and recording go to
        //    the SAME injected `dropRecorder`, so a conformer that answers
        //    `.notRecording` to the arming answers it to the drop as well and
        //    NO ROW LANDS. This is the unwired build — `DownloadManager`'s
        //    default, and the state `workJournalRecorder` shipped in for four
        //    months (playhead-4xmz).
        let (unwiredStore, unwiredDir) = try await Self.openedStore()
        let manager = try Self.manager(
            store: nil, sessionIO: Self.creationRefusingIO(), launchId: "launch-unwired"
        )
        try await manager.bootstrap()
        await manager.armDropLedger()
        await Self.drive(manager, episodeId: "ep-unwired")

        // The manager wrote nowhere, so a SECOND store — freshly migrated, and
        // demonstrably healthy on the next line — still reads zero. Without the
        // health check the zero would be indistinguishable from a broken store,
        // which is the exact confusion the three-state outcome enum exists for.
        #expect(
            try Self.scalar(
                unwiredDir, "SELECT count(*) FROM background_download_drops"
            ) == 0
        )
        let arming = try await unwiredStore.fetchBackgroundDownloadDropArming()
        #expect(
            try #require(arming).armedLaunches == 0,
            """
            an unwired build's counters are LEGITIMATELY zero — the fault is \
            the wiring, not the store, and the store is here to prove it could \
            have held a row
            """
        )
    }



    // MARK: - Writing behind the store's back

    /// Executes `sql` on the store file through a second READWRITE connection.
    ///
    /// This is how a row from another BUILD is modelled — there is no other
    /// way, because this build's write path cannot produce one. `busy_timeout`
    /// for the same reason the read helper has one.
    private static func writeBehindTheStore(_ directory: URL, _ sql: String) throws {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK else {
            throw StoreProbeFailure.couldNotOpen(dbURL.path)
        }
        defer { sqlite3_close_v2(db) }
        sqlite3_busy_timeout(db, 5000)
        var message: UnsafeMutablePointer<CChar>?
        guard sqlite3_exec(db, sql, nil, nil, &message) == SQLITE_OK else {
            let text = message.map { String(cString: $0) } ?? "unknown"
            sqlite3_free(message)
            throw StoreProbeFailure.couldNotPrepare(sql, text)
        }
    }
}
