// BackgroundDownloadDropIdentityTests.swift
// playhead-sdis — `count(*)` on `background_download_drops` counts EPISODES
// AFFECTED, and it was being read as a number of daemon OUTAGES. These rails
// are what make the two separable, and every one of them asserts on the SQL a
// device pull would actually run rather than on the Swift reader.
//
// ─────────────────────────────────────────────────────────────────────────
// WHY THE QUERY AND NOT THE SWIFT PAGE
// ─────────────────────────────────────────────────────────────────────────
// `fetchBackgroundDownloadDrops` has NO production caller — playhead-7dgx says
// so in its own bd comment, and the reason is that a human on a device pull
// runs raw SQL. The Swift reader is also deliberately LOSSY (it skips rows
// whose enums it cannot decode and counts them separately), so a rail written
// only against `page.rows` measures a different population from the one the
// answer will come from. Section 1 therefore runs
// `count(DISTINCT sessionCrossingId)` against the file, through a second
// connection, exactly as `sqlite3 analysis.sqlite` would.
//
// The Swift reader is not abandoned: sections 4 and 5 are about IT, because
// the decode refusal and the page's third unreadable-row counter are
// properties only it has.
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
// So the rails come in a matched set: forty rows / one crossing / one launch;
// forty rows / forty crossings / one launch; forty rows / forty crossings /
// forty launches. A column that separated only one of the two axes would pass
// one of the three and fail the others, which is why they are written as three
// readings of ONE query rather than three different queries.
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

    /// The number the bead is written in. Large enough that "forty rows, one
    /// crossing" and "forty rows, forty crossings" cannot be confused with each
    /// other or with an off-by-one, and cheap because every one of the forty is
    /// refused before it reaches `nsurlsessiond`.
    private static let episodes = 40

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
        gate: CrossingGate
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

    /// A one-shot gate the SEAM awaits and the RAIL opens.
    ///
    /// An actor rather than a `DispatchSemaphore` because `perform`'s prologue
    /// runs on the cooperative pool: a semaphore would occupy a pool thread the
    /// runtime does not replace, for the whole barrier. Waiters resume in the
    /// order they arrived, and a `wait()` after `open()` returns at once, so no
    /// caller can be stranded by ordering.
    private actor CrossingGate {
        private var opened = false
        private var waiting: [CheckedContinuation<Void, Never>] = []

        func open() {
            guard !opened else { return }
            opened = true
            let pending = waiting
            waiting = []
            for continuation in pending { continuation.resume() }
        }

        func wait() async {
            if opened { return }
            await withCheckedContinuation { continuation in
                waiting.append(continuation)
            }
        }
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
        invariantRecorder: RecordedInvariantViolations? = nil,
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
                invariantRecorder: invariantRecorder?.recorder,
                dropRecorder: recorder,
                launchId: launchId
            )
        }
        return DownloadManager(
            cacheDirectory: try makeTempDir(prefix: "sdisCache"),
            sessionIO: sessionIO,
            invariantRecorder: invariantRecorder?.recorder,
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

    /// ONE OUTAGE THAT COST FORTY EPISODES.
    ///
    /// Forty concurrent callers, one in-flight crossing, one refusal. Before
    /// this bead the forty rows shared no marker at all and `count(*)` read as
    /// forty daemon refusals — the number playhead-7dgx's retry recommendation
    /// rests on, and the number it could not falsify.
    ///
    /// THE RAIL IS VACUOUS UNLESS THE FORTY GENUINELY OVERLAP, which is why the
    /// arrival barrier is asserted before anything else: with a synchronous
    /// refusal every caller starts its own crossing and this rail would read
    /// forty crossings while nothing was wrong.
    @Test("forty episodes lost to ONE daemon refusal: 40 rows, 1 crossing, 1 launch")
    func oneOutageThatCostFortyEpisodes() async throws {
        let (store, dir) = try await Self.openedStore()
        let gate = CrossingGate()
        let manager = try Self.manager(
            store: store, sessionIO: Self.heldThenRefusingIO(gate: gate)
        )
        try await manager.bootstrap()
        await manager.armDropLedger()

        let barrier: (arrived: Bool, count: Int) = await withTaskGroup(
            of: Void.self
        ) { group in
            for index in 0..<Self.episodes {
                group.addTask {
                    await Self.drive(manager, episodeId: "ep-join-\(index)")
                }
            }
            // THE BARRIER. Every caller reaching the crossing decision is an
            // event BOTH implementations reach — the one with the join joins
            // there, one without starts a second crossing there — so waiting
            // for it is a wait rather than a race. Read while the crossing is
            // still held, so the number describes the barrier.
            let arrived = await Self.waited {
                await manager.sessionCrossingArrivalsForTesting == Self.episodes
            }
            let count = await manager.sessionCrossingArrivalsForTesting
            await gate.open()
            return (arrived, count)
        }

        #expect(
            barrier.arrived,
            """
            the rail is vacuous unless all \(Self.episodes) callers reached the \
            crossing decision while it was held: \(barrier.count) arrived
            """
        )

        let reading = try Self.readings(dir)
        #expect(
            reading.episodes == Self.episodes,
            "count(*) is EPISODES AFFECTED and there were \(Self.episodes) of them"
        )
        #expect(
            reading.refusals == 1,
            """
            ONE crossing, so ONE daemon refusal. \(reading.refusals) means the \
            joiners minted ids of their own, which is the reading this column \
            exists to remove
            """
        )
        #expect(reading.launches == 1, "one process wrote all of them")

        // And the ratio the bead is about, stated as the two quantities rather
        // than as one number: 40 episodes per 1 refusal.
        #expect(reading.episodes > reading.refusals)
    }

    /// FORTY SEPARATE OUTAGES, ONE LAUNCH.
    ///
    /// The mirror, and it is not a formality: `launchId` alone reads this case
    /// and the one above IDENTICALLY (one launch, forty rows), which is the
    /// whole reason the bead's one-column fix was not enough. A refusal is
    /// never cached — `sessionCreationRetriesAfterARefusal` is the property —
    /// so forty sequential requests really are forty crossings.
    @Test("forty separate refusals in ONE launch: 40 rows, 40 crossings, 1 launch")
    func fortySeparateOutagesInOneLaunch() async throws {
        let (store, dir) = try await Self.openedStore()
        let manager = try Self.manager(
            store: store, sessionIO: Self.creationRefusingIO()
        )
        try await manager.bootstrap()
        await manager.armDropLedger()

        for index in 0..<Self.episodes {
            await Self.drive(manager, episodeId: "ep-seq-\(index)")
        }

        let reading = try Self.readings(dir)
        #expect(reading.episodes == Self.episodes)
        #expect(
            reading.refusals == Self.episodes,
            """
            each sequential request crossed on its own — a refusal is never \
            cached — so these really are \(Self.episodes) daemon refusals
            """
        )
        #expect(
            reading.launches == 1,
            "…in ONE launch, which is exactly what launchId alone cannot distinguish from the joined case"
        )
    }

    /// FORTY LAUNCHES. The third axis, and the one `sessionCrossingId` alone
    /// cannot see: it reads this case and the one above identically (forty
    /// crossings, forty rows).
    ///
    /// Fewer managers than `episodes` on purpose — each one is a whole
    /// `DownloadManager` with its own cache directory, and the property is
    /// "distinct launches are distinct", not a number.
    @Test("N launches are N launches: distinct managers mint distinct launch ids")
    func distinctLaunchesAreDistinguishable() async throws {
        let (store, dir) = try await Self.openedStore()
        let launches = 4
        for index in 0..<launches {
            let manager = try Self.manager(
                store: store, sessionIO: Self.creationRefusingIO()
            )
            try await manager.bootstrap()
            await manager.armDropLedger()
            await Self.drive(manager, episodeId: "ep-launch-\(index)")
        }

        let reading = try Self.readings(dir)
        #expect(reading.episodes == launches)
        #expect(reading.refusals == launches)
        #expect(
            reading.launches == launches,
            """
            \(launches) processes wrote these rows. A process-wide `static` \
            launchId — the spelling the column's own docs refuse — would read \
            1 here while every other assertion in this file still passed
            """
        )
    }

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
        let gate = CrossingGate()
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

    /// The crossing id on the row is the id of the crossing that was REFUSED,
    /// and not merely some id.
    ///
    /// Every rail above compares the column with ITSELF — equal here, distinct
    /// there — so all of them would pass against a per-row UUID that had
    /// nothing to do with any crossing, as long as the joiners happened to
    /// share it. The refusal record on the surface-status stream carries
    /// `crossing=<id>` and is written by the caller that STARTED the crossing,
    /// inside `backgroundSessionRidingCrossing`; the row is written by
    /// `backgroundDownload` afterwards. Asserting the two are EQUAL is the only
    /// thing in this file that pins the column to the event it names.
    @Test("the row's crossing id is the one the REFUSAL record names")
    func theRowNamesTheCrossingThatWasRefused() async throws {
        let (store, dir) = try await Self.openedStore()
        let recording = RecordedInvariantViolations()
        let manager = try Self.manager(
            store: store,
            sessionIO: Self.creationRefusingIO(),
            invariantRecorder: recording
        )
        try await manager.bootstrap()
        await manager.armDropLedger()
        await Self.drive(manager, episodeId: "ep-witness")

        let refusals = recording.sessionRefusals(from: "background_download")
        #expect(refusals.count == 1, "one crossing, one refusal record")
        let description = try #require(refusals.first)
        // `crossing=<uuid> ` — the field is followed by a space in the record.
        let marker = "crossing="
        let start = try #require(description.range(of: marker)?.upperBound)
        let recorded = String(
            description[start...].prefix(while: { !$0.isWhitespace })
        )
        #expect(recorded.isEmpty == false, "the refusal record must NAME its crossing")

        let rows = try Self.query(dir, """
            SELECT sessionCrossingId FROM background_download_drops
            WHERE episodeId='ep-witness'
            """)
        #expect(rows.count == 1)
        #expect(
            rows.first?.first == recorded,
            """
            the drop row must carry the crossing the refusal record names. \
            row=\(rows.first?.first ?? "NULL") record=\(recorded). Two different \
            values mean the column is a fresh UUID wearing a crossing's name, \
            which every equality rail in this file would still pass
            """
        )
    }

    /// NULL FOR THE OTHER TWO REASONS IS A STATEMENT, NOT A GAP.
    ///
    /// `transferTaskNotVended` and `transferNotResumed` each follow one
    /// `sessionIO.perform` the caller submitted alone, so for them a row IS a
    /// call. Giving them a per-row UUID would be a column in bijection with the
    /// primary key — no information, and an invitation to read
    /// `count(DISTINCT sessionCrossingId)` across all reasons as an outage
    /// count. This rail is what stops a later diff "filling in the gap".
    @Test("a reason that rides no crossing records NULL, and count(DISTINCT) declines to count it")
    func aReasonWithNoCrossingRecordsNull() async throws {
        let (store, dir) = try await Self.openedStore()
        let manager = try Self.manager(
            store: store,
            sessionIO: BackgroundSessionIO(
                behavior: .refusesCallsLabelled("downloadTask(with:) for"),
                timeout: Self.bound,
                queueLabel: "sdis.test.task-refused.\(UUID().uuidString)"
            )
        )
        try await manager.bootstrap()
        // BOTH a `defer` and a trailing `await`, on `StreamingDownloadTests`'
        // precedent: the `defer` covers the throw path, where a trailing call
        // is skipped by any `try` above it, and the trailing call covers the
        // pass path, where the `defer`'s unstructured `Task` is awaited by
        // nothing and can leave a live session on a process-wide background
        // identifier past the end of the test.
        defer { Task { await manager.invalidateBackgroundSessionsForTesting() } }
        await manager.armDropLedger()
        await Self.drive(manager, episodeId: "ep-task")

        let rows = try Self.query(dir, """
            SELECT reason, sessionCrossingId, launchId IS NULL
            FROM background_download_drops
            """)
        #expect(rows.count == 1)
        #expect(rows.first?[0] == "transfer_task_not_vended")
        #expect(
            rows.first?[1] == nil,
            "nothing JOINS a downloadTask submission, so for this reason a row IS a call"
        )
        #expect(
            rows.first?[2] == "0",
            """
            …and the row still carries a LAUNCH. `sessionCrossingId IS NULL` \
            alone cannot tell this row from one that predates V64; the pair of \
            columns can, which is why the header says to read them together
            """
        )

        // Across ALL reasons the crossing count is 1, not 2 — SQL's own
        // count(DISTINCT) skips the NULL. A sentinel would have made it 2 and
        // invented a daemon refusal that never happened.
        #expect(
            try Self.scalar(dir, """
                SELECT count(DISTINCT sessionCrossingId) FROM background_download_drops
                """) == 0,
            "no session refusal happened here at all, and the table says zero rather than one"
        )
        await manager.invalidateBackgroundSessionsForTesting()
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

    /// `count(DISTINCT launchId)` AGAINST `armedLaunches`, WHICH IS A COUNT OF
    /// LAUNCHES.
    ///
    /// This is the bead's headline claim and it needs the two numbers side by
    /// side. Before V64 the numerator was EPISODES and the denominator was
    /// LAUNCHES, so their ratio was not a quantity at all. Two launches that
    /// both armed and both dropped: the numerator reads 2 and the denominator
    /// reads 2, and the ratio is 1.0 — a number that means something.
    @Test("count(DISTINCT launchId) and armedLaunches are in the SAME unit")
    func theNumeratorAndDenominatorShareAUnit() async throws {
        let (store, dir) = try await Self.openedStore()
        for index in 0..<2 {
            let manager = try Self.manager(
                store: store,
                sessionIO: Self.creationRefusingIO(),
                launchId: "launch-unit-\(index)"
            )
            try await manager.bootstrap()
            await manager.armDropLedger()
            // Two episodes each, so `count(*)` reads FOUR while both of the
            // quantities under test read two. Without this the rail could not
            // tell a launch count from an episode count.
            await Self.drive(manager, episodeId: "ep-unit-\(index)-a")
            await Self.drive(manager, episodeId: "ep-unit-\(index)-b")
        }

        let episodes = try Self.scalar(dir, "SELECT count(*) FROM background_download_drops")
        let launches = try Self.scalar(
            dir, "SELECT count(DISTINCT launchId) FROM background_download_drops"
        )
        let armed = try Self.scalar(
            dir, "SELECT armedLaunches FROM background_download_drop_arming"
        )
        #expect(episodes == 4, "four episodes were lost")
        #expect(launches == 2, "…on two launches")
        #expect(armed == 2, "…and both of them armed the recorder")
        #expect(
            launches == armed,
            """
            the ratio the bead exists to make computable. `count(*) / \
            armedLaunches` would read 2.0 here and would be episodes-per-launch \
            wearing a rate's name
            """
        )
    }

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

    /// The write-failure re-create path must NOT name a launch.
    ///
    /// `noteBackgroundDownloadDropWriteFailure` re-creates a missing arming row
    /// so a hand-edited or partially rolled-back store still counts. A write
    /// FAILURE is not an arming, so that path leaves `armedLaunches` at 0 —
    /// and, for the identical reason, must leave `lastArmedLaunchId` NULL.
    /// Naming a launch there would manufacture the very claim the row exists to
    /// withhold, in the one column a drop row's `launchId` is compared against.
    @Test("a write failure that re-creates the arming row names NO launch")
    func aWriteFailureNamesNoLaunch() async throws {
        let (store, dir) = try await Self.openedStore()
        try await store.noteBackgroundDownloadDropWriteFailure(at: 99.0)
        let row = try Self.query(dir, """
            SELECT armedLaunches, dropWriteFailures, lastArmedLaunchId, lastArmedAt
            FROM background_download_drop_arming
            """)
        #expect(
            row == [["0", "1", nil, nil]],
            """
            armedLaunches stays 0 (the V62 seed's value — nobody armed), \
            dropWriteFailures goes to 1, and BOTH the launch id and the arming \
            date stay NULL. A failed write is not an arming, so naming a launch \
            here would manufacture the very claim this row exists to withhold — \
            in the one column a drop row's `launchId` is compared against. \
            Got: \(row)
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

    /// `arming_failed` IS ITS OWN STATE and is not folded into `not_attempted`.
    ///
    /// The two are different claims with different futures: `not_attempted` is
    /// "not yet, and possibly never", while `arming_failed` is "it ran, the
    /// counter write failed, and this launch will never be in `armedLaunches`"
    /// — the write is not retried. A reader who could not tell them apart would
    /// go looking for a race that never happened.
    @Test("an arming whose counter write FAILED is its own state on every later row")
    func aFailedArmingIsItsOwnState() async throws {
        let dir = try makeTempDir(prefix: "sdisArmFailure")
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        TestScratchReaper.shared.adopt(dir, owner: store)
        _ = try await store.fetchBackgroundDownloadDrops()

        // Break exactly the ARMING table, leaving the drops table writable —
        // the shape a partial failure has.
        try Self.writeBehindTheStore(dir, "DROP TABLE background_download_drop_arming")

        let manager = try Self.manager(
            store: store, sessionIO: Self.creationRefusingIO(), launchId: "launch-arm-failed"
        )
        try await manager.bootstrap()
        await manager.armDropLedger()
        await Self.drive(manager, episodeId: "ep-arm-failed")

        let rows = try Self.query(dir, """
            SELECT launchId, launchArmingState FROM background_download_drops
            """)
        #expect(
            rows == [["launch-arm-failed", "arming_failed"]],
            """
            the arming RAN and its write failed, so this launch is not in \
            `armedLaunches` and never will be. Reading it as `not_attempted` \
            would say the drop merely raced the launch Task. Got: \(rows)
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

    // MARK: - 5. What the Swift reader does with a value it cannot decode

    /// THE SAME RULE A THIRD TIME. `reason` and `unattributedReason` are each
    /// skipped-and-counted when this build cannot decode them; so is
    /// `launchArmingState`, and it needs its OWN counter.
    ///
    /// Coercing an unknown raw value to `.notAttempted` would inflate the
    /// population that claims "this launch is not in the denominator", which is
    /// the very claim the column exists to make honestly. Mapping it to nil
    /// would say the row PREDATES V64, which a row carrying a value
    /// demonstrably does not.
    @Test("an unrecognized launchArmingState is counted separately, never coerced")
    func anUnknownArmingStateIsReportedRatherThanCoerced() async throws {
        let (store, dir) = try await Self.openedStore()
        try Self.writeBehindTheStore(dir, """
            INSERT INTO background_download_drops
            (id, episodeId, reason, occurredAt, podcastId, unattributedReason,
             isExplicitDownload, boundSeconds, launchId, sessionCrossingId,
             launchArmingState)
            VALUES ('row-2027', 'ep-2027', 'session_not_vended', 1.0, NULL, NULL, 0, 10.0,
                    'launch-2027', 'crossing-2027', 'an_arming_state_from_2027')
            """)

        let page = try await store.fetchBackgroundDownloadDrops()
        #expect(page.rows.isEmpty, "a row whose arming this build cannot read must not be materialized")
        #expect(page.unrecognizedLaunchArmingStateRows == 1)
        #expect(page.unrecognizedReasonRows == 0, "the three losses are counted separately")
        #expect(page.unrecognizedUnattributedReasonRows == 0)
        #expect(page.totalRowsSeen == 1, "'one drop' and 'one drop I can read' are different claims")

        // And a NULL is NOT counted here: that is a pre-V64 row, a legitimate
        // absence, returned in `rows` with a nil arming state.
        try Self.writeBehindTheStore(dir, """
            INSERT INTO background_download_drops
            (id, episodeId, reason, occurredAt, podcastId, unattributedReason,
             isExplicitDownload, boundSeconds)
            VALUES ('row-legacy', 'ep-legacy', 'session_not_vended', 2.0, NULL, NULL, 0, 10.0)
            """)
        let second = try await store.fetchBackgroundDownloadDrops()
        #expect(second.rows.count == 1)
        #expect(second.rows.first?.launchArmingState == nil)
        #expect(second.rows.first?.launchId == nil)
        #expect(
            second.unrecognizedLaunchArmingStateRows == 1,
            "still one — a NULL is an absence this build understands, not a value it cannot read"
        )
        #expect(second.totalRowsSeen == 2)
    }

    /// `ORDER BY occurredAt DESC, id DESC` — the TIEBREAKER, not decoration.
    ///
    /// N rows minted by N joiners of one crossing carry `occurredAt` values
    /// that can be equal to the double's precision. With `occurredAt` alone,
    /// which of them a `limit` cuts off is whatever SQLite's scan order happens
    /// to be — so the page's `truncated` flag names an arbitrary set, and two
    /// reads of the same file can disagree.
    @Test("rows sharing an occurredAt are ordered by id, so a limit cuts deterministically")
    func equalStampsAreBrokenByIdDescending() async throws {
        let (store, dir) = try await Self.openedStore()
        // Byte-identical stamps, inserted in an order that does NOT match the
        // required output order — so a query that fell back on rowid would
        // return them the other way round.
        try Self.writeBehindTheStore(dir, """
            INSERT INTO background_download_drops
            (id, episodeId, reason, occurredAt, podcastId, unattributedReason,
             isExplicitDownload, boundSeconds, launchId, sessionCrossingId,
             launchArmingState)
            VALUES ('id-a', 'ep-a', 'session_not_vended', 500.0, NULL, NULL, 0, 10.0,
                    'launch-tie', 'crossing-tie', 'armed'),
                   ('id-c', 'ep-c', 'session_not_vended', 500.0, NULL, NULL, 0, 10.0,
                    'launch-tie', 'crossing-tie', 'armed'),
                   ('id-b', 'ep-b', 'session_not_vended', 500.0, NULL, NULL, 0, 10.0,
                    'launch-tie', 'crossing-tie', 'armed')
            """)

        let page = try await store.fetchBackgroundDownloadDrops()
        #expect(
            page.rows.map(\.id) == ["id-c", "id-b", "id-a"],
            "descending id breaks the tie: \(page.rows.map(\.id))"
        )
        let capped = try await store.fetchBackgroundDownloadDrops(limit: 2)
        #expect(
            capped.rows.map(\.id) == ["id-c", "id-b"],
            """
            and the LIMIT therefore cuts a stated set rather than an arbitrary \
            one — `truncated` means nothing if two reads of one file disagree \
            about which rows fitted. Got: \(capped.rows.map(\.id))
            """
        )
        #expect(capped.truncated)
        // The three rows really do share a stamp; without this the ordering
        // above could be `occurredAt DESC` doing all the work.
        #expect(
            try Self.scalar(dir, """
                SELECT count(DISTINCT occurredAt) FROM background_download_drops
                """) == 1,
            "the rail is vacuous unless the stamps are genuinely equal"
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
