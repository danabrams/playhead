// BackgroundDownloadDropLedger.swift
// playhead-7dgx — a background download the transfer daemon never started
// leaves a DURABLE, COUNTABLE row instead of one `os_log` line.
//
// ─────────────────────────────────────────────────────────────────────────
// WHAT WAS WRONG
// ─────────────────────────────────────────────────────────────────────────
// `DownloadManager.backgroundDownload` abandons the transfer on three
// distinct paths, and every one of them was CORRECT and SILENT: the
// in-flight reservation is released, the attribution sidecar is deleted,
// an error is logged, and the function returns. Nothing retries, nothing
// requeues, no state reaches the user, and — the part this file fixes —
// NOTHING COUNTS IT. From the app's point of view the download was
// requested and simply never happened, and the only witness was an
// `os_log` line that no device pull captures.
//
// The question "how often does this fire in the field" therefore had no
// answer at all. It has one now: `SELECT count(*) FROM
// background_download_drops`.
//
// ─────────────────────────────────────────────────────────────────────────
// THIS FILE DOES NOT RETRY, AND THAT IS DELIBERATE
// ─────────────────────────────────────────────────────────────────────────
// The retry/backoff POLICY is Dan's call, and widening the bound is not
// obviously safe — `playhead-ola7` records that background `URLSession`
// CREATION can still block the cooperative pool, which is the deadlock the
// bound exists to escape. So this bead makes the loss MEASURABLE and stops
// there.
//
// One exception to "elapsed IS the bound" below, because it is not universally
// true: `backgroundSession(for:requestedBy:)` lets a second caller JOIN an
// in-flight crossing rather than submitting its own, and such a caller gives up
// when the FIRST caller's deadline fires — after an arbitrary fraction of the
// bound. Those rows carry a `boundSeconds` the call never waited out.
//
// `boundSeconds` RECORDS WHICH CEILING WAS CONFIGURED AND NOTHING MORE, and
// that limit is stated here because the obvious reading of it is wrong.
// `BackgroundSessionIO.timeout` is a `let` on one shared instance and
// `onItsOwnQueue` copies it verbatim, so every production row will read 10.0
// until somebody changes the constant: the column has ZERO VARIANCE within a
// build. And because `perform` returns nil AT the deadline, the elapsed time
// on exactly these rows IS the bound, by construction. So it cannot answer
// "would 15 s have caught this one" — the data for that is the latency
// distribution of the calls that SUCCEEDED, and nothing records it. What the
// column does buy is that a later change to the bound cannot silently
// re-scale this table's whole history, and that a device running a
// non-default bound is not read as if it ran the shipped one.
//
// ─────────────────────────────────────────────────────────────────────────
// UNKNOWN IS NOT ZERO — AND THE READER NEEDS TWO QUERIES, NOT ONE
// ─────────────────────────────────────────────────────────────────────────
// A counter that cannot tell "zero drops" from "nobody was counting" is
// the defect, not the fix. There are two ways to be uncounted and they are
// distinguished by two different facts on disk:
//
//   1. THE BUILD PREDATES THE INSTRUMENT. `background_download_drops` DOES
//      NOT EXIST. That is a schema fact, not a convention: a pull can check
//      it without trusting anybody.
//
//      READ THE TABLE, NOT THE STAMP. `_meta.schema_version < 62` is NOT the
//      discriminator and using it throws away real evidence. `createTables()`
//      runs unconditionally on every open, BEFORE the migration ladder, so a
//      store parked below the V39 rollback floor — a real, documented,
//      guarded population; see the `guard observed >= N` rule every rung from
//      V40 up carries — holds both tables and a live arming row while its
//      stamp still reads 38. Rows written there are genuine.
//      `BackgroundDownloadDropsV62MigrationTests` pins that pairing so nobody
//      re-derives the wrong recipe from the stamp.
//
//   2. THE BUILD HAS THE TABLE BUT NOBODY INSTALLED THE RECORDER. The
//      recorder below is an injected dependency whose default is a NO-OP
//      (see `NoopBackgroundDownloadDropRecorder`), and this repo has
//      already shipped exactly that hole once: `DownloadManager`'s
//      `workJournalRecorder` defaulted to `NoopWorkJournalRecorder` and
//      production never replaced it, so every `recordFailed` it made went
//      nowhere — for four months. A default no-op plus an intention is not a
//      mechanism. (Fixed in playhead-4xmz, which this bead's own reasoning
//      found; see `DownloadWorkJournalLedger.swift`. Kept in the past tense
//      rather than deleted, because it is the shipped instance this argument
//      rests on.)
//      `background_download_drop_arming` is the mechanism: one row, whose
//      `armedLaunches` is incremented once per launch from
//      ``DownloadManager/armDropLedger()`` — see below for exactly what that
//      counts, because the obvious reading is wrong. `armedLaunches = 0`
//      alongside zero drops means NOBODY WAS COUNTING; `armedLaunches = 37`
//      alongside zero drops AND zero `dropWriteFailures` is a positive claim.
//
//   3. THE RECORDER WAS LIVE AND ITS WRITES FAILED. Without a third state,
//      `armedLaunches > 0` with zero rows would be indistinguishable from a
//      genuine "no drops" — and a store can stop being writable for reasons
//      that have nothing to do with downloads: `SQLITE_FULL`, or a
//      `handleEventsForBackgroundURLSession` relaunch before first unlock,
//      since `analysis.sqlite` is `completeUntilFirstUserAuthentication`.
//      `dropWriteFailures` on the arming row counts them, so state (c) stops
//      being reachable by silence.
//
//      THE RESIDUAL, NAMED RATHER THAN HIDDEN: the failure counter is itself
//      a store write. If BOTH fail, this database says nothing — so a drop
//      that could not be recorded ALSO raises
//      `backgroundDownloadDropNotRecorded` on the surface-status invariant
//      stream, a JSON Lines file under `Caches/` that a device pull already
//      reads.
//
//      **THAT FALLBACK IS A DIFFERENT FILE, NOT A DIFFERENT FAILURE DOMAIN,
//      and an earlier version of this comment claimed "two independent media
//      have to fail together" — which is the kind of claim this file exists to
//      stop people making.** The app sets no data-protection entitlement, so
//      the JSONL takes the container default: the same
//      `completeUntilFirstUserAuthentication` class `AnalysisStore` sets
//      explicitly. A pre-first-unlock background relaunch — one of the two
//      causes named above — silences BOTH, and so does a full volume. The
//      JSONL write is fire-and-forget onto its own queue, and it rotates per
//      launch at 200 files on a volume iOS purges under pressure, while the
//      ledger it backstops is unbounded.
//
//      What it genuinely covers is narrower and still worth having: a failure
//      LOCAL TO THIS DATABASE — a corrupt or dropped table, a lock on one
//      table — where the row cannot be written and the file system is fine.
//      Read it as that, not as a second chance at a dead device.
//
// THESE ARE THREE CELLS OF A TRUTH TABLE, NOT A LADDER, and reading them in
// order is how a reader reaches a wrong conclusion. `armedLaunches = 0` beside
// REAL DROP ROWS is reachable — a drop before the launch Task arms, or an
// arming write that itself failed — and it means "these rows are real and the
// denominator is missing", not "nothing happened". Read all three numbers
// every time.
//
// TWO MECHANICAL TRAPS FOR WHOEVER RUNS THE QUERIES.
//
//   * `sqlite3` prints an EMPTY LINE for a missing row, which is one glance
//     from `0|0||`. Ask `SELECT count(*) FROM background_download_drop_arming`
//     FIRST. The Swift reader returns nil rather than a synthesized zero for
//     exactly this reason; nothing on the raw-SQL path gives you that.
//   * A `GROUP BY reason` returns EVERY raw value, including ones this build
//     cannot decode (measured). It is the SWIFT reader that is lossy here:
//     `fetchBackgroundDownloadDrops` skips a row whose `reason` it cannot
//     decode and counts it into `unrecognizedReasonRows`, so `rows` under-
//     reports a later build's fourth case while raw SQL does not. Quote
//     per-reason shares from SQL, and read the two `unrecognized…` counters
//     before quoting anything from the Swift page.
//
// AND A "START FRESH" MOVES THE WHOLE HISTORY ASIDE.
// `AnalysisStoreRecoveryCoordinator.quarantineAndRebuild` renames the store to
// a sibling `AnalysisStore-quarantined-<stamp>-*` and builds a fresh one whose
// `armedLaunches` is 0 and whose `installedAt` dates the REBUILD. Before
// concluding "never armed", list those siblings — and take them in the pull.
//
// NAME THE NUMERATOR AND THE DENOMINATOR. The numerator is
// `background_download_drops` rows. The denominator `armedLaunches` gives
// is LAUNCHES THAT ARMED THE RECORDER — it is NOT the number of background
// downloads attempted, and a drop RATE cannot be computed from it. The
// attempt denominator would cost a store write on the download hot path
// and a new suspension point inside the reservation region
// `playhead-nsjn`/`-gpdb`/`-7l6n` built; it is deliberately not taken here.
// Read `armedLaunches` as "was anyone counting, and for how long", never
// as "out of how many downloads".
//
// WHAT `armedLaunches` COUNTS, exactly, because the obvious reading is wrong
// in two directions. It is incremented from ONE production call site:
// `PlayheadRuntime`'s launch Task, immediately after
// `AnalysisStoreRecoveryCoordinator.openAtLaunch` reports the store OPEN. So
// it counts LAUNCHES ON WHICH THE STORE OPENED AND THE RECORDER WAS LIVE —
// the population that could actually have written a row, and the only honest
// denominator for "and saw none".
//
//   * A DEGRADED LAUNCH IS DELIBERATELY NOT COUNTED. `openAtLaunch` failing is
//     the documented "playback works, analysis does not" path, and counting
//     those launches would let a run of unopenable ones read as a positive
//     claim. **That is NOT the same as "such a launch could not have written a
//     row", and an earlier version of this comment said it was.**
//     `AnalysisStore` opens LAZILY, so a drop later in that same launch
//     reaches `ensureOpen()` through `insertBackgroundDownloadDrop` and may
//     well succeed. Rows can exist whose launch is not in the denominator.
//   * A LAUNCH THAT DROPS BEFORE THE LAUNCH TASK REACHES THE ARMING IS NOT
//     COUNTED EITHER, and that window is wider than it looks: `openAtLaunch`
//     runs the whole migration ladder first, and `PlayheadRuntime` already
//     records (playhead-8u3i) that a cold BGProcessingTask wake fires its
//     handler ahead of this Task's body.
//   * A LAUNCH THAT DIES BEFORE THE TASK RUNS AT ALL is likewise uncounted,
//     even though the recorder was live from the moment `DownloadManager` was
//     constructed.
//
// Every direction is conservative — it can under-claim that somebody was
// counting, never over-claim — so read `armedLaunches` as a LOWER BOUND on
// COUNTING launches and NOT as an upper bound on RECORDING ones. The
// consequence to hold on to is above: `armedLaunches = 0` beside real rows is
// a reachable, meaningful state, not a contradiction.
//
// It is armed from `PlayheadRuntime` rather than from
// `DownloadManager.bootstrap()`, where it started, for two reasons found in
// review. Arming there made `bootstrap()` `async` and put its cache-directory
// creation behind a full `AnalysisStore` open — on an upgrade launch, the
// whole migration ladder — widening a window in which every other method on
// the actor sees the cache directories absent. And it made `DownloadManager`
// an UNMANAGED opener of `analysis.sqlite`, racing
// `AnalysisStoreRecoveryCoordinator` and able to bring the store up outside
// the coordinator that counts consecutive failures and decides whether to
// offer a rebuild. Arming from the one place that already knows the store is
// open avoids both, and makes the quantity mean something better besides.

import Foundation
import OSLog

// MARK: - BackgroundDownloadDropReason

/// WHICH bound `DownloadManager.backgroundDownload` missed.
///
/// The three are separate failures with separate remedies and they must not
/// be collapsed into one "the daemon was slow" bucket:
///
///   * `sessionNotVended` — no background `URLSession` exists, so the whole
///     download subsystem is refusing for this process; session
///     construction is memoized per role, so this is a per-launch outage
///     rather than a per-episode one.
///   * `transferTaskNotVended` — the session is alive and the daemon would
///     not mint a task for THIS url. One episode is affected.
///   * `transferNotResumed` — a task was created and could not be started.
///     Strictly worse than the other two while it lasts, because a
///     suspended task fires no delegate callback at all; the transfer is
///     abandoned by `abandonUnstartedTransfer`.
///
/// A raw value is written to disk, so these strings are a schema and
/// renaming one is a migration.
enum BackgroundDownloadDropReason: String, Sendable, Codable, Equatable, CaseIterable {
    /// `backgroundSession(for:requestedBy:)` returned nil: `nsurlsessiond`
    /// did not hand back a background `URLSession` inside the bound.
    case sessionNotVended = "session_not_vended"

    /// `session.downloadTask(with:)` did not answer inside the bound, so
    /// no transfer object was ever created.
    case transferTaskNotVended = "transfer_task_not_vended"

    /// `task.resume()` did not answer inside the bound. The task exists and
    /// is suspended; `abandonUnstartedTransfer` retires it.
    case transferNotResumed = "transfer_not_resumed"
}

// MARK: - BackgroundDownloadDropRecord

/// One abandoned background download, as it lands on disk.
///
/// Every field answers a question the bead asked and nothing else:
/// `episodeId` + `podcastId` are "to whom", `reason` + `boundSeconds` are
/// "under what conditions", `occurredAt` is "when", and the row's existence
/// is "how often".
struct BackgroundDownloadDropRecord: Sendable, Equatable {

    /// Row identity. A UUID rather than `(episodeId, reason)` because a
    /// repeat drop for the same episode is the single most interesting
    /// thing this table can show, and a key that collapsed it would hide
    /// exactly the population the bead exists to size.
    let id: String

    /// The episode whose download was abandoned.
    let episodeId: String

    /// Which bound was missed.
    let reason: BackgroundDownloadDropReason

    /// Unix epoch seconds at the moment the drop was decided.
    let occurredAt: Double

    /// The show, when the caller could name one. `nil` is a MEASURED
    /// absence, never a forgotten one — `DownloadContext` makes a caller
    /// spell out why it cannot resolve the show, and that reason travels in
    /// `unattributedReason`.
    let podcastId: String?

    /// Why `podcastId` is nil. Non-nil exactly when `podcastId` is nil,
    /// mirroring `DownloadContext`'s own invariant.
    let unattributedReason: DownloadContext.UnattributedReason?

    /// `true` when a human pressed a download control, `false` for
    /// subscription auto-download and every other background route. A
    /// dropped explicit download is a user-visible broken promise; a
    /// dropped auto-download is a missed opportunity. Counting them
    /// together would average two different products.
    let isExplicitDownload: Bool

    /// WHICH CEILING WAS CONFIGURED for the crossing this row's download died
    /// on. Read the file header before using it, because the two obvious
    /// readings are both wrong.
    ///
    /// It is NOT evidence for "should the bound be wider": the value has zero
    /// variance within a build, and the data that question needs is the latency
    /// of the calls that SUCCEEDED, which nothing records. And it is not
    /// reliably the elapsed time either — a caller that JOINED an in-flight
    /// session crossing gives up on the FIRST caller's deadline, after some
    /// fraction of it.
    ///
    /// What it does buy: a later change to the bound cannot silently re-scale
    /// this table's whole history, and a device running a non-default bound is
    /// not read as if it ran the shipped one.
    let boundSeconds: Double

    init(
        id: String = UUID().uuidString,
        episodeId: String,
        reason: BackgroundDownloadDropReason,
        occurredAt: Double,
        podcastId: String?,
        unattributedReason: DownloadContext.UnattributedReason?,
        isExplicitDownload: Bool,
        boundSeconds: Double
    ) {
        self.id = id
        self.episodeId = episodeId
        self.reason = reason
        self.occurredAt = occurredAt
        self.podcastId = podcastId
        self.unattributedReason = unattributedReason
        self.isExplicitDownload = isExplicitDownload
        self.boundSeconds = boundSeconds
    }

    /// Builds a record from the `DownloadContext` the caller already
    /// supplied, so the show identity on the drop row and the show identity
    /// on the deleted attribution sidecar cannot disagree.
    init(
        episodeId: String,
        reason: BackgroundDownloadDropReason,
        context: DownloadContext,
        boundSeconds: Double,
        occurredAt: Double = Date().timeIntervalSince1970,
        id: String = UUID().uuidString
    ) {
        self.init(
            id: id,
            episodeId: episodeId,
            reason: reason,
            occurredAt: occurredAt,
            podcastId: context.podcastId,
            unattributedReason: context.unattributedReason,
            isExplicitDownload: context.isExplicitDownload,
            boundSeconds: boundSeconds
        )
    }
}

// MARK: - BackgroundDownloadDropPage

/// One read of `background_download_drops`, with everything a caller needs in
/// order not to over-read the array.
///
/// Three companions to `rows`, and each exists because the array alone would
/// let a reader state something stronger than the data supports.
struct BackgroundDownloadDropPage: Sendable, Equatable {

    /// The decoded rows, most recent first.
    let rows: [BackgroundDownloadDropRecord]

    /// Rows whose `reason` this build cannot decode — written by a build with
    /// a wider vocabulary. They are NOT in `rows` and they are NOT lost:
    /// "three drops" and "three drops I can read" are different claims.
    let unrecognizedReasonRows: Int

    /// Rows whose `unattributedReason` this build cannot decode. Counted
    /// separately from the above because it is a different loss — the drop's
    /// classification survives, the show attribution does not.
    let unrecognizedUnattributedReasonRows: Int

    /// `true` when more rows exist than the `limit` allowed back.
    ///
    /// Without it, a window that stops at its own ceiling reports "this is
    /// what happened" while meaning "this is what fitted" — and the two
    /// unreadable-row counters above would then be computed over the window
    /// rather than over the table, silently.
    let truncated: Bool

    /// Every drop this page could read AND could not read. The honest
    /// denominator for any per-reason share taken off `rows`.
    var totalRowsSeen: Int {
        rows.count + unrecognizedReasonRows + unrecognizedUnattributedReasonRows
    }
}

// MARK: - BackgroundDownloadDropReadRefusal

/// Why one persisted row could not be materialized. Returned rather than
/// swallowed so the loss is counted rather than inferred from an absence.
enum BackgroundDownloadDropReadRefusal: Error, Sendable, Equatable {
    /// The `reason` column holds a raw value this build does not know.
    case unrecognizedReason
    /// The `unattributedReason` column holds a raw value this build does not
    /// know. A row in this state cannot be built honestly: a nil there would
    /// say the caller NAMED a show, which is the opposite of what the row
    /// records.
    case unrecognizedUnattributedReason
}

// MARK: - BackgroundDownloadDropArming

/// The single `background_download_drop_arming` row: the positive claim
/// that somebody was counting.
///
/// Read `armedLaunches == 0` as NOBODY WAS COUNTING, not as "no launches" —
/// the row is seeded by the V62 migration precisely so that the state
/// "instrument installed, never armed" is expressible instead of being
/// indistinguishable from an empty table.
struct BackgroundDownloadDropArming: Sendable, Equatable {
    /// Launches on which the analysis store opened and the recorder was live.
    /// A LOWER BOUND, and NOT a count of download attempts — see this file's
    /// header for exactly what it does and does not measure.
    let armedLaunches: Int

    /// Drops whose durable row could NOT be written.
    ///
    /// This is what stops "armed, and zero rows" from being reachable by
    /// silence. Read `armedLaunches > 0 && drops == 0 && dropWriteFailures == 0`
    /// as the positive claim; the same pair with `dropWriteFailures > 0` says
    /// the opposite — drops happened and this database could not hold them,
    /// and the surface-status invariant stream has the details.
    let dropWriteFailures: Int
    /// When the first such launch happened. `nil` while `armedLaunches` is
    /// 0, and `nil` is the whole point: a zero here would date an arming
    /// that never occurred.
    let firstArmedAt: Double?
    /// When the most recent one happened. `nil` while `armedLaunches` is 0.
    let lastArmedAt: Double?
    /// When this row was created.
    ///
    /// Usually the V62 migration, i.e. when this install first opened a build
    /// carrying the instrument. NOT ALWAYS: `noteBackgroundDownloadDropInstrumentArmed`
    /// re-creates the row if it is missing — that is deliberate, so a hand-
    /// edited or partially-rolled-back store still counts — and on that path
    /// this stamps the ARM time instead. There is a THIRD writer for the same
    /// reason — `noteBackgroundDownloadDropWriteFailure` also re-creates a
    /// missing row, and stamps the time of the FAILURE. So read it as "the
    /// earliest moment this install is known to have carried the instrument",
    /// which is true on all three paths, rather than as the migration's own
    /// timestamp.
    let installedAt: Double
}

// MARK: - BackgroundDownloadDropWriteOutcome

/// What happened to one attempted ledger write.
///
/// THREE STATES BECAUSE A `Bool` COLLAPSES TWO OF THEM, and this file exists to
/// stop exactly that. With a Bool, the no-op recorder's `false` and a genuine
/// SQLite failure are the same value — so an UNWIRED build raised
/// `backgroundDownloadDropNotRecorded` saying the row "could not be written"
/// and pointing at `dropWriteFailures`, on a device where no write was ever
/// attempted and that counter reads 0. A reader would go and diagnose SQLite.
/// The wiring canary makes that unreachable in production; it is expressible
/// here, which is enough reason for the third case.
enum BackgroundDownloadDropWriteOutcome: Sendable, Equatable {
    /// The row reached disk.
    case landed
    /// A recorder that writes tried and failed. The durable counter carries it
    /// unless that write failed too.
    case writeFailed
    /// This recorder does not write at all. Nothing was attempted, nothing
    /// failed, and no counter anywhere moved.
    case notRecording
}

// MARK: - BackgroundDownloadDropRecording

/// Where an abandoned background download is recorded.
///
/// Deliberately NOT a closure pair (the shape `invariantRecorder` uses):
/// the store write is `async` and this protocol keeps it `async`, so a
/// caller AWAITS the row reaching disk. The alternative — fire-and-forget
/// into a detached `Task` — would leave "the row survives process death"
/// depending on a task that a dying process may never schedule, which is
/// the defect one layer along from the one this bead fixes. It also makes
/// every test here deterministic instead of a poll loop.
protocol BackgroundDownloadDropRecording: Sendable {

    /// Durably record one abandoned background download.
    ///
    /// - Returns: what happened to the write. A conformer MUST NOT throw — the
    ///   caller is on the recovery path of a failure it has already handled —
    ///   but it must not swallow the outcome either. The caller uses anything
    ///   other than `.landed` to raise the loss on a second medium, which is
    ///   the only reason a drop the database could not hold is not silent.
    ///
    /// A conformer that records nothing BY DESIGN returns `.notRecording`,
    /// never `.landed`: claiming success would make the no-op indistinguishable
    /// from a working ledger at the one call site that checks.
    ///
    /// NOT `@discardableResult`. An earlier cut marked it so, which removed the
    /// only compile-time enforcement of the sentence above: a later caller
    /// could drop the outcome with no diagnostic and the loss would go quiet
    /// again. Nothing needs the attribute — both production callers bind it.
    func recordDrop(
        _ record: BackgroundDownloadDropRecord
    ) async -> BackgroundDownloadDropWriteOutcome

    /// Record that this process installed a live recorder, so that zero
    /// drops can be read as a positive claim rather than as silence.
    ///
    /// - Returns: what happened to the count. The DENOMINATOR has the same hole
    ///   the numerator does — a launch whose arming write failed is
    ///   byte-identical to a launch that never ran — so the outcome is
    ///   reported for the same reason, and the caller raises the loss on the
    ///   surface-status stream. The direction is conservative (an uncounted
    ///   arming under-claims that somebody was counting), which is why this is
    ///   a report rather than a second durable counter: a failure counter for
    ///   the failure counter is where that regress has to stop.
    ///
    /// Idempotence is the CALLER's: `DownloadManager.armDropLedger()` has one
    /// production call site and no guard of its own, and a conformer counts
    /// every call it receives. `BackgroundDownloadDropWiringSourceCanaryTests`
    /// is what pins the single-call-site property, because nothing at runtime
    /// can see it.
    ///
    /// NOT `@discardableResult`, for the reason given on `recordDrop`.
    func recordInstrumentArmed(
        at now: Double
    ) async -> BackgroundDownloadDropWriteOutcome
}

// MARK: - NoopBackgroundDownloadDropRecorder

/// Records nothing.
///
/// The default for `DownloadManager`, and therefore what every test that
/// does not care about this ledger gets. The no-op is spelled out at the
/// conformer rather than hidden behind a protocol default for the reason
/// `WorkJournalRecording` gives in its own docs: a silent drop must be a
/// visible decision.
///
/// **A production `DownloadManager` holding one of these is a defect**, and
/// it is not a hypothetical — `workJournalRecorder` was in exactly that
/// state for four months (playhead-4xmz, since fixed).
/// `BackgroundDownloadDropWiringSourceCanaryTests` is what stops it happening
/// here, and `armedLaunches` is what would show it on a device pull if the
/// canary were ever out-spelled.
struct NoopBackgroundDownloadDropRecorder: BackgroundDownloadDropRecording {
    /// `.notRecording`, and specifically NOT `.writeFailed`: nothing was
    /// attempted, so a caller must not go on to report a database that could
    /// not hold a row. That distinction is the reason the outcome is an enum.
    func recordDrop(
        _ record: BackgroundDownloadDropRecord
    ) async -> BackgroundDownloadDropWriteOutcome { .notRecording }

    /// Same, for the denominator.
    func recordInstrumentArmed(
        at now: Double
    ) async -> BackgroundDownloadDropWriteOutcome { .notRecording }
}

// MARK: - AnalysisStoreBackgroundDownloadDropRecorder

/// Production binding: writes into `analysis.sqlite`, which is the file a
/// device pull copies.
///
/// A `struct` over an immutable `AnalysisStore` reference — the shape
/// `AnalysisStoreBackgroundTaskRunLedger` and `AnalysisStoreDAIStitchRecorder`
/// already use — so `Sendable` needs no argument.
///
/// Best-effort by design, on the `AnalysisStoreBackgroundTaskRunLedger`
/// precedent: a store error is logged and swallowed. The caller has already
/// released the reservation and dropped the sidecar, so a failed ledger
/// write costs diagnostics, never correctness — and throwing here would
/// turn a recorded loss into an unrecorded one.
struct AnalysisStoreBackgroundDownloadDropRecorder: BackgroundDownloadDropRecording {

    private let store: AnalysisStore
    private let logger = Logger(
        subsystem: "com.playhead",
        category: "BackgroundDownloadDropLedger"
    )

    init(store: AnalysisStore) {
        self.store = store
    }

    func recordDrop(
        _ record: BackgroundDownloadDropRecord
    ) async -> BackgroundDownloadDropWriteOutcome {
        do {
            try await store.insertBackgroundDownloadDrop(record)
            return .landed
        } catch {
            logger.error(
                "background download drop NOT recorded for episode=\(record.episodeId, privacy: .public) reason=\(record.reason.rawValue, privacy: .public): \(String(describing: error), privacy: .public)"
            )
            // A second, independent durable write, so that a pull holding only
            // this database can still tell "no drops" from "drops this store
            // could not hold". It can fail too — the caller raises the loss on
            // the surface-status stream for exactly that case.
            do {
                try await store.noteBackgroundDownloadDropWriteFailure(
                    at: Date().timeIntervalSince1970
                )
            } catch {
                logger.error(
                    "background download drop write-failure counter ALSO failed: \(String(describing: error), privacy: .public)"
                )
            }
            return .writeFailed
        }
    }

    func recordInstrumentArmed(
        at now: Double
    ) async -> BackgroundDownloadDropWriteOutcome {
        do {
            try await store.noteBackgroundDownloadDropInstrumentArmed(at: now)
            return .landed
        } catch {
            logger.error(
                "background download drop instrument NOT armed: \(String(describing: error), privacy: .public)"
            )
            return .writeFailed
        }
    }
}
