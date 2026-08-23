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
//      `workJournalRecorder` defaults to `NoopWorkJournalRecorder` and
//      production never replaces it, so every `recordFailed` it makes goes
//      nowhere. A default no-op plus an intention is not a mechanism.
//      `background_download_drop_arming` is the mechanism: one row,
//      `armedLaunches` incremented once per process from
//      `DownloadManager.bootstrap()`. `armedLaunches = 0` alongside zero
//      drops means NOBODY WAS COUNTING; `armedLaunches = 37` alongside
//      zero drops is a positive claim that 37 launches carried a live
//      recorder and saw no drop.
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
//      a store write. If BOTH fail, this database is left saying nothing — so
//      a drop that could not be recorded ALSO raises
//      `backgroundDownloadDropNotRecorded` on the surface-status invariant
//      stream, which is a DIFFERENT medium (JSON Lines under `Caches/`) and
//      one a device pull already reads. Two independent media have to fail
//      together before the loss goes quiet.
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
//   * A DEGRADED LAUNCH IS DELIBERATELY NOT COUNTED. `openAtLaunch` failing
//     is the documented "playback works, analysis does not" path; a launch
//     that could not open the store observed nothing, and counting it would
//     let a run of unopenable launches read as a positive claim.
//   * A LAUNCH THAT DIES BEFORE THAT TASK RUNS IS NOT COUNTED EITHER, even
//     though the recorder was live from the moment `DownloadManager` was
//     constructed. So this is a LOWER BOUND.
//
// Both directions are conservative: it can under-claim that somebody was
// counting, never over-claim.
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

    /// The `BackgroundSessionIO` bound, in seconds, that expired to produce
    /// this row. Recorded rather than assumed so that a later change to the
    /// bound does not silently re-scale the whole history of this table —
    /// and so the "should the bound be wider" question has evidence.
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
    /// this stamps the ARM time instead. So read it as "the earliest moment
    /// this install is known to have carried the instrument", which is true on
    /// both paths, rather than as the migration's own timestamp.
    let installedAt: Double
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
    /// - Returns: `true` when the row reached disk. A conformer MUST NOT
    ///   throw — the caller is on the recovery path of a failure it has
    ///   already handled — but it must not swallow the outcome either. The
    ///   caller uses `false` to raise the loss on a second, independent
    ///   medium, which is the only reason a drop the database could not hold
    ///   is not silent.
    ///
    /// A conformer that records nothing BY DESIGN returns `false`: there is
    /// no row, and claiming otherwise would make the no-op indistinguishable
    /// from a working ledger at the one call site that checks.
    @discardableResult
    func recordDrop(_ record: BackgroundDownloadDropRecord) async -> Bool

    /// Record that this process installed a live recorder, so that zero
    /// drops can be read as a positive claim rather than as silence.
    ///
    /// Idempotence is the CALLER's: `DownloadManager.bootstrap()` runs once
    /// per manager, and a conformer counts every call it receives.
    func recordInstrumentArmed(at now: Double) async
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
/// it is not a hypothetical — `workJournalRecorder` is in exactly that
/// state today. `BackgroundDownloadDropWiringSourceCanaryTests` is what
/// stops it happening here, and `armedLaunches` is what would show it on a
/// device pull if the canary were ever out-spelled.
struct NoopBackgroundDownloadDropRecorder: BackgroundDownloadDropRecording {
    /// `false`, not `true`: nothing was written, and a no-op that reported
    /// success would be the one shape this whole file exists to make
    /// impossible.
    func recordDrop(_ record: BackgroundDownloadDropRecord) async -> Bool { false }
    func recordInstrumentArmed(at now: Double) async {}
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

    func recordDrop(_ record: BackgroundDownloadDropRecord) async -> Bool {
        do {
            try await store.insertBackgroundDownloadDrop(record)
            return true
        } catch {
            logger.error(
                "background download drop NOT recorded for episode=\(record.episodeId, privacy: .public) reason=\(record.reason.rawValue, privacy: .public): \(String(describing: error), privacy: .public)"
            )
            // A second, independent durable write, so that a pull holding only
            // this database can still tell "no drops" from "drops this store
            // could not hold". It can fail too — the caller raises the loss on
            // the surface-status stream for exactly that case.
            do {
                try await store.noteBackgroundDownloadDropWriteFailure()
            } catch {
                logger.error(
                    "background download drop write-failure counter ALSO failed: \(String(describing: error), privacy: .public)"
                )
            }
            return false
        }
    }

    func recordInstrumentArmed(at now: Double) async {
        do {
            try await store.noteBackgroundDownloadDropInstrumentArmed(at: now)
        } catch {
            logger.error(
                "background download drop instrument NOT armed: \(String(describing: error), privacy: .public)"
            )
        }
    }
}
