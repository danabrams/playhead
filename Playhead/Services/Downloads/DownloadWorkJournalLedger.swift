// DownloadWorkJournalLedger.swift
// playhead-4xmz — the DOWNLOAD half of the work journal stops being a no-op.
//
// ─────────────────────────────────────────────────────────────────────────
// WHAT WAS WRONG
// ─────────────────────────────────────────────────────────────────────────
// `DownloadManager.init` takes `workJournalRecorder: WorkJournalRecording =
// NoopWorkJournalRecorder()`, and NOTHING IN PRODUCTION EVER REPLACED IT.
// `PlayheadRuntime` builds exactly one `DownloadManager` and passed
// `invariantRecorder:` and `dropRecorder:` and nothing else, so every event
// the download path emitted went to a method whose body is `{}`:
//
//   * `DownloadManager.recordBackgroundFailure`   — a terminal background
//     transfer failure, carrying the `SliceMetadata` blob
//     `SliceCompletionInstrumentation` built for it;
//   * `DownloadManager.handleBackgroundDownloadComplete` — a transfer that
//     landed;
//   * `ForceQuitResumeScan.scanForSuspendedTransfers` — the cold-launch
//     preempted/corrupted rows;
//   * `ForceQuitResumeScan.resumeSuspendedTransfer` — a corrupt resume blob.
//
// A default no-op plus an intention is not a mechanism. It is the shape
// `playhead-1nl6` removed the protocol DEFAULTS to prevent — a conformer must
// DECIDE to swallow — and the default argument made that decision silently at
// the one call site that matters.
//
// ─────────────────────────────────────────────────────────────────────────
// WHY THE ONE-LINE FIX WOULD HAVE BEEN WORSE THAN THE BUG
// ─────────────────────────────────────────────────────────────────────────
// A real `WorkJournalRecording` conformer already exists and is wired:
// `AnalysisStoreWorkJournalRecorder`, on `AnalysisWorkScheduler`. Passing it
// to `DownloadManager` is one line and it is the WRONG one, for two reasons.
//
// The first is the one the bead predicted. `AnalysisStoreWorkJournalRecorder`
// resolves `{generationID, schedulerEpoch}` from
// `store.fetchLatestJobForEpisode(episodeId)` and RETURNS EARLY when there is
// no `analysis_jobs` row. A background download that fails before any analysis
// job exists — the common case for an auto-download — logs a warning and
// writes nothing. Wiring alone reproduces the defect with extra steps.
//
// The second was found while establishing the first, and it is the reason this
// file exists rather than a widened `work_journal` insert.
// **`work_journal.event_type` IS A COLD-LAUNCH RECOVERY INPUT, NOT AN
// OBSERVABILITY FIELD** (`WorkJournalEntry.EventType`, playhead-rqgr).
// `AnalysisCoordinator.recoverOrphans` reads the LAST `work_journal` row for
// the `{episode_id, generation_id}` of every job whose lease has expired and
// routes it through `EventType.orphanRecoveryRouting`, where `.failed` and
// `.finalized` both mean `terminalNoRequeue` — clear the lease slot, do NOT
// requeue. So a row saying THE DOWNLOAD FAILED, written under the ANALYSIS
// job's generation, tells cold-launch recovery that the ANALYSIS work is over.
// That is playhead-rqgr's shipped defect re-created from a new writer, and it
// is this repo's standing class exactly: a value that names one thing (a
// transfer died) read as though it named another (this generation is
// terminal).
//
// The download half therefore MUST NOT write into `work_journal`. Not "should
// not" — a wiring that did would trade a silent instrument for a live bug.
//
// ─────────────────────────────────────────────────────────────────────────
// WHAT THE ROW KEYS ON, AND THE PRECEDENT IT FOLLOWS
// ─────────────────────────────────────────────────────────────────────────
// `download_work_journal` is keyed on the EPISODE and is deliberately NOT
// foreign-keyed, which is `rediff_day_zero_kickoffs`' answer to the identical
// problem (see the V41 block comment in `AnalysisStore.swift`). There, the
// give-up worth recording was precisely the one where no `analysis_assets` row
// had ever been registered, so an FK would have rejected every insert that
// mattered. Here the event worth recording is precisely the one where no
// `analysis_jobs` row exists, and the only identity the download path holds is
// an episode id.
//
// THERE IS NO `generationID` COLUMN, and its absence is a decision rather than
// an omission. Capturing one would cost a store READ on the download
// completion path for a join nobody has asked for, and — worse — a nullable
// generation column beside an `eventType` spelled `failed` is the exact shape
// that invites a later reader to join these rows back into orphan recovery,
// which is the hazard above.
//
// `DownloadWorkJournalEventType` is its own enum rather than a reuse of
// `WorkJournalEntry.EventType` for the same reason. That type's central
// documented property is `orphanRecoveryRouting`, and it is FALSE of every row
// in this table: nothing routes on these. A type whose defining property does
// not apply to the values it is carrying is the same defect one layer up.
//
// ─────────────────────────────────────────────────────────────────────────
// AN EMPTY JOURNAL AND AN UNRECORDED ONE ARE DIFFERENT FACTS
// ─────────────────────────────────────────────────────────────────────────
// Ask of any instrument: WHAT WOULD THIS READ IF THE THING IT CLAIMS TO RECORD
// HAD NEVER HAPPENED? For four months the answer for `workJournalRecorder` was
// "exactly what it reads today", which is why nobody noticed. So the arming
// row from playhead-7dgx is repeated here rather than admired:
//
//   * no `download_work_journal` TABLE
//         — this build predates the instrument. Zero says NOTHING.
//         READ THE TABLE, NOT `_meta.schema_version`: `createTables()` runs
//         unconditionally and BEFORE the migration ladder, so a store parked
//         below the V39 rollback floor carries both tables and real rows at a
//         stamp of 38.
//   * table present, `armedLaunches = 0`
//         — no launch armed it. NOBODY WAS COUNTING.
//   * `armedLaunches = N > 0`, zero rows, `writeFailures = 0`
//         — a POSITIVE CLAIM: N launches carried a live recorder.
//   * `writeFailures > 0`
//         — events happened and this database could not hold them.
//
// TWO MORE REACHABLE STATES THE FOUR BULLETS DO NOT NAME, both found at review
// 3, and both are ways a row can be MISSING with every counter healthy:
//
//   * A FINALIZED EVENT DISCARDED BY CANCELLATION — **TEST-ONLY TODAY**, because
//     nothing in production reaches `retireBackgroundTransfers` (measured at
//     review 5; see the `catch is CancellationError` arm). It leaves no row and moves no
//     counter — that is exactly what the `catch is CancellationError` arm is
//     for. So `armedLaunches = N > 0, rows = 0, writeFailures = 0` — the
//     POSITIVE CLAIM — is reachable on an install where finalized events
//     happened and were deliberately dropped. Read the claim as "no event this
//     build chose to keep", not as "no event occurred". It also biases the
//     rate below against `finalized`, by the population this bead's own
//     cancellation handling creates. **AND "DELIBERATELY" IS NOT THE SAME AS
//     "ITS BYTES WERE DELETED"** — an earlier version of this bullet said it
//     was: the unlink that follows the cancel can THROW, so a discarded row
//     can belong to an artifact that is still there. That is L-7.
//   * TABLE PRESENT, ARMING ROW ABSENT. `fetchDownloadWorkJournalArming`
//     returns nil for it rather than a synthesized zero, deliberately — but
//     raw SQL returns NO ROWS, which `sqlite3` prints as a blank line and a
//     reader glances past as 0. `SELECT count(*) FROM
//     download_work_journal_arming` first, exactly as at V62.
//
// READ THE LIST, NOT A NUMBER. Every count this paragraph has carried has been
// wrong, and the last attempt to summarise HOW was wrong too — it said "four
// while listing six", but `b057e36e` said FOUR and listed four; the two extra
// states arrived a commit later. Walking the commits is the only way to get
// that history right, which is the argument for not carrying a number at all
// rather than for carrying a better one. It is deleted, not incremented.
// One more reading, and it is the one the bullets above do not cover:
// `armedLaunches = 0` beside real rows is reachable — an event before the launch Task arms, or an arming write
// that itself failed — and means "these rows are real and the denominator is
// missing". Read all of the numbers, every time.
//
// `armedLaunches` IS NOT AN ATTEMPT COUNT. It counts LAUNCHES ON WHICH THE
// STORE OPENED AND THE RECORDER WAS LIVE, incremented from one production site
// in `PlayheadRuntime`'s launch Task immediately after
// `AnalysisStoreRecoveryCoordinator.openAtLaunch` reports the store OPEN. It is
// a LOWER BOUND in every direction — a degraded launch is deliberately not
// counted, and neither is a launch that dies before the Task runs, even though
// the recorder was live from the moment `DownloadManager` was constructed.
//
// ─────────────────────────────────────────────────────────────────────────
// THE DENOMINATOR THIS TABLE HAS AND `background_download_drops` DOES NOT
// ─────────────────────────────────────────────────────────────────────────
// `finalized` IS RECORDED, on purpose. playhead-7dgx had to say "no drop RATE
// can be computed from `armedLaunches`" because counting download ATTEMPTS
// would have put a store write on the hot path and a suspension point inside
// the reservation region playhead-nsjn / -gpdb / -7l6n built. This site is not
// that site: `recordFinalized` runs after canonical placement, the strong pin
// and the analysis enqueue, and it already awaits a journal `Task`. So
//
//     SELECT eventType, count(*) FROM download_work_journal GROUP BY eventType
//
// is a real completion-versus-failure split for the download half, which is
// the question 7dgx's ledger could only ever answer half of.
//
// IT IS NOT FREE, AND AN EARLIER VERSION OF THIS PARAGRAPH SAID IT WAS. It
// claimed `recordFinalized` "is not that site" because it runs after canonical
// placement and already awaits a journal `Task` — which quotes the EXISTENCE of
// an `await`, not its cost, and that is the standing defect class. What is
// actually true is L-6.
//
// NAME THE POPULATION BEFORE QUOTING A RATE FROM IT — and the `failed` bucket
// has THREE WRITERS, not one. An earlier version of this paragraph said the
// split was "over transfers that reached a terminal delegate callback", which
// is true of `finalized` and of only ONE of the three `failed` writers:
//
//   * `DownloadManager.recordBackgroundFailure` — a terminal delegate
//     callback, and the only one that is a transfer OUTCOME;
//   * `ForceQuitResumeScan.scanForSuspendedTransfers`, corrupted-blob branch —
//     cold-launch blob HYGIENE, no callback, `stage=forceQuitResumeScan.corrupted`;
//   * `ForceQuitResumeScan.resumeSuspendedTransfer`, empty-blob branch — a
//     user-initiated RESUME attempt, no callback,
//     `stage=forceQuitResumeScan.resume.corrupted`.
//
// So a bare `finalized / (finalized + failed)` is depressed by two event
// classes that are not transfer outcomes, and one of them can fire for an
// episode that goes on to finalize anyway — counting the same transfer twice.
// Split on the stage the blob carries:
//
//     SELECT eventType, count(*) FROM download_work_journal
//     WHERE COALESCE(json_extract(metadata, '$.stage'), '')
//           NOT LIKE 'forceQuitResumeScan%'
//     GROUP BY eventType
//
// THE `COALESCE` IS LOAD-BEARING AND ITS ABSENCE COST THIS PARAGRAPH ITS WHOLE
// POINT. `recordFinalized` writes `metadata = '{}'`, so `json_extract` returns
// NULL, and `NULL NOT LIKE 'x%'` is NULL, not true — the row is excluded.
// Measured on sqlite 3.54.0 against a 5-row fixture (1 finalized, 3 failed, 1
// preempted): the bare `GROUP BY` gives `failed 3 / finalized 1 / preempted 1`;
// the un-coalesced filter gives `failed 1` and NOTHING ELSE. A recipe offered
// to correct a mis-stated completion rate produced a 0 % one instead — the
// standing defect class inside the paragraph written to remove it, and nothing
// tests the SQL in a comment.
//
// It also excludes, in both spellings: the three pre-start abandonments in
// `backgroundDownload` (those are `background_download_drops` rows and reach no
// callback at all), and any transfer that vanishes with neither a callback nor
// a resume blob. Reading the narrow population as the wide one is exactly the
// thing this file's own "WHAT WOULD THIS READ IF…" paragraph is about.
//
// ─────────────────────────────────────────────────────────────────────────
// LIMITS, NAMED
// ─────────────────────────────────────────────────────────────────────────
// L-1 UNBOUNDED. `finalized` fires on every successful background download, so
//     unlike `background_download_drops` this table has a non-rare writer.
//
//     MEASURED rather than reasoned, because the arithmetic here was wrong by
//     3x on the first cut and the two things it omitted are the reason. Recipe,
//     so it can be re-run: 7,300 `finalized` rows with DISTINCT `episodeId`s,
//     `metadata` literally `{}`, `cause` NULL, the shipped DDL verbatim,
//     `VACUUM`ed, file size ÷ rows, sqlite3 3.54.0. Every figure is a FLOOR —
//     a live WAL database is larger.
//
//     THE SPREAD IS THE FIXTURE, NOT THE SQLITE BUILD, and this line said the
//     opposite for two rounds. Reviews 3 and 7 measured 127/140/170 at epLen
//     16 against 123/134/162 here, ON THE SAME 3.54.0 — the difference is the
//     `episodeId` STRINGS, which change index page fill. So the recipe has to
//     name them: `https://feeds.example.com/show{i%40}/rss.xml::guid-{i}`,
//     truncated or padded to `epLen`, 7,300 distinct. A different string
//     shape moves every column by a few bytes and moves the conclusion by
//     nothing. The artifact is
//     `/Users/dabrams/playhead-gate-artifacts/4xmz/rowsize-measurement.txt`;
//     re-run the recipe, do not quote the digits.
//
//     `epLen` is the length of `episodeId`, which on this path is
//     `Episode.canonicalEpisodeKey` = `feedURL.absoluteString + "::" +
//     feedItemGUID` — a long compound key, NOT a short id. MEASURED on the
//     2026-08-21 device pull (review 3): n = 29, min 75, median 77, mean 79.8,
//     max 91. 88 is kept as the highlighted column because it is conservative
//     against that distribution; 16 is not a case that occurs and is here only
//     to show the shape.
//
//         epLen   table B/row   +occurred idx   +episode idx   MB/yr @ 20/day
//            16           123             134            162             1.18
//            36           144             154            203             1.48
//        ** 88 **         198             209        ** 313 **       ** 2.28 **
//           160           274             284            463             3.38
//
//     The two omissions: `id TEXT PRIMARY KEY` builds an implicit
//     `sqlite_autoindex`, so the 36-char UUID is stored TWICE; and the two
//     explicit indexes store the long `episodeId` again. At epLen 88 the
//     indexes are 115 of the 313 bytes — 37 %, and `idx_..._episode` alone is
//     104 of them, on the one table whose only named limit is growth. It is
//     kept anyway, deliberately: the reader this table is for runs raw
//     `sqlite3` against a pulled file, `WHERE episodeId = ?` is the query they
//     will write, and `idx_background_download_drops_episode` is equally
//     unqueried from Swift for the same reason. Named here rather than left to
//     be discovered.
//
//     ~2.3 MB/year is small enough that it is NOT pruned, on `work_journal`'s
//     and `background_download_drops`' own precedent. A newest-N ring was
//     considered and REJECTED: it evicts the rare `failed`/`preempted` rows
//     preferentially, i.e. it destroys exactly the population the table exists
//     for while leaving the common one intact.
// L-2 The residual line goes to the surface-status JSON Lines stream, which
//     playhead-dyvh2 MEASURED is not an independent medium — the app sets no
//     data-protection entitlement, so it takes the same
//     `completeUntilFirstUserAuthentication` class `analysis.sqlite` sets
//     explicitly, and a pre-first-unlock background relaunch silences both. It
//     covers a failure LOCAL TO THIS DATABASE and nothing wider. Read it as
//     that.
// L-3 `WorkJournalRecording` is void-returning and is shared with
//     `AnalysisWorkScheduler`, whose contract this bead may not change, so this
//     recorder reports its own write failure rather than returning an outcome
//     the caller inspects (the shape `BackgroundDownloadDropRecording` uses).
//     The consequence is that the CALLER cannot tell a live recorder from the
//     no-op; `DownloadWorkJournalWiringSourceCanaryTests` and `armedLaunches`
//     are what cover that, in source and on disk respectively.
// L-4 A `quarantineAndRebuild` "start fresh" moves the whole history aside into
//     a sibling `AnalysisStore-quarantined-<stamp>-*` whose `armedLaunches` is
//     0 and whose `installedAt` dates the REBUILD. List those siblings before
//     concluding "never armed", and take them in the pull.

// L-5 THE FORCE-QUIT SCAN NOW AWAITS A STORE WRITE, INSIDE A 2 s SLA.
//     `PlayheadAppDelegate.didFinishLaunchingWithOptions` fires
//     `scanForSuspendedTransfers()` and logs an error if it exceeds 2 s. Its
//     two recorder calls were no-ops before this bead; they are
//     `AnalysisStore` writes now, and `insertDownloadWorkJournalEntry` reaches
//     `ensureOpen()`, which runs the whole migration ladder. So on the first
//     launch of a build carrying a new rung the scan can serialize behind it —
//     and it makes the download path an UNMANAGED opener of `analysis.sqlite`,
//     which is the hazard `PlayheadRuntime` names as the reason `armDropLedger`
//     was moved OUT of `DownloadManager.bootstrap()`.
//
//     GATING ON `store.isOpen` DOES NOT FIX IT and was rejected for that
//     reason, not for taste: reading `isOpen` is itself an actor hop, so it
//     serializes behind the very migration it is trying to avoid waiting for.
//     What it would buy is the unmanaged-open half at the cost of losing the
//     cold-launch rows entirely — on the launch they are about.
//
//     What bounds the harm, stated so nobody reads this as free: the SLA is an
//     `os_log` error line and nothing acts on it; the scan ALREADY carries a
//     10 s bounded `enumerationIO` crossing (playhead-rouw) that can blow the
//     same budget under a wedged daemon; and the failure COUNTING survives —
//     a thrown migrate rolls `didOpen` back, so
//     `AnalysisStoreRecoveryCoordinator.openAtLaunch` still runs, still fails,
//     and still counts. Nobody has measured the ladder's cost on a device —
//     filed as playhead-16xkq together with L-6, because they are one
//     measurement and one decision.
//
// L-6 THE FINALIZED WRITE IS A STORE WRITE INSIDE THE IN-FLIGHT RESERVATION,
//     which is the cost playhead-7dgx declined to pay for an attempt counter
//     and this bead pays for a denominator. Measured against the code rather
//     than asserted: `bgInFlightEpisodes` still holds the episode across
//     `await journalTask.value` — `finishBackgroundTransfer` runs AFTER it — so
//     the write happens inside the per-episode reservation playhead-nsjn /
//     -gpdb / -7l6n built, and `insertDownloadWorkJournalEntry` reaches
//     `ensureOpen()`, which runs the whole migration ladder on first touch. It
//     also postpones `touchAccess`, `deleteResumeData`, `evictIfNeeded` and the
//     day-0 rediff seam `notifyBackgroundDownloadCompleted` by one store
//     round-trip. Before this bead that await was a no-op. Filed as
//     playhead-16xkq: the decision is Dan's, and the measurement comes first.
//
//     WHY IT IS PAID ANYWAY: the reservation is per-EPISODE, not global, so it
//     delays a re-download of the same episode and nothing else; and moving the
//     write into a detached task would re-open the hazard L-5's neighbour
//     describes — the ownership re-check runs AFTER the await, so an unawaited
//     write can outlive the deletion that revoked it. Nobody has measured the
//     round-trip on a device; filed as playhead-16xkq, and the decision it
//     carries is Dan's.
//
//
// L-7 A CANCELLED FINALIZATION IS NOT ALWAYS A DELETED ARTIFACT, in two ways,
//     both found at review 4 and both leaving a row DESTROYED while the bytes
//     survive — with `writeFailures = 0` and `armedLaunches` healthy, so the
//     list above reads it as the POSITIVE CLAIM.
//
//       * THE DELETE CAN THROW AFTER THE CANCEL — on a DORMANT path.
//         `removeCache` cancels through `cancelDownload` and then calls
//         `removeAllAudioArtifacts`, which genuinely throws, from `removeItem`
//         and from its own post-condition guard; `clearCache()` has the same
//         shape and is deliberately fail-closed. Both are reachable only from
//         tests today (measured at review 5: neither has a caller outside
//         `PlayheadTests/`), so this is a property of the mechanism rather than
//         of any shipping behaviour. Moving the retire after a SUCCESSFUL
//         unlink would re-open the race the retire exists to close, so it is
//         documented rather than fixed.
//       * AND THE DELETION PATHS THAT ACTUALLY RUN DO NOT CANCEL AT ALL.
//         There are TWO, not one — an earlier version of this bullet said
//         "the only", which is the standing defect class in the sentence
//         written to name a population.
//
//         LRU EVICTION IS THE OTHER, and it runs constantly:
//         `DownloadManager.evictIfNeeded` unlinks a completed episode's audio
//         AND its pin, from three sites INSIDE THE ACTOR (foreground
//         completion, streaming completion, the background deposit) and from
//         nowhere outside it. It enters no retire. Say "inside the actor":
//         a tree-wide count of the bare name returns FIVE, because two
//         unrelated stores have an `evictIfNeeded()` of their own — which is
//         how the rail for this sentence failed on its first run.
//         The RACE is nonetheless closed there, and by something else
//         entirely: `evictIfNeeded` skips `bgInFlightEpisodes`, and
//         `finishBackgroundTransfer` runs AFTER `await journalTask.value`, so
//         an episode whose finalization is in flight is protected. That is a
//         guard in another function, not a property of this one — which is why
//         it is written down here rather than assumed. What it does NOT
//         protect is a row already written for an episode evicted later, and
//         nothing could: the row is a record that the transfer finished, not a
//         claim that the bytes are still there.
//
//         THE USER-ACTION ONE is Settings' "Clear Cached Audio"
//         (`SettingsViewModel.clearAudioCache`) unlinks the cache directory
//         from a detached Task WITHOUT entering the `DownloadManager` actor,
//         so nothing is retired and a `finalized` row can outlive its bytes.
//         This was "the converse" for a round, which read as though it were
//         the smaller half; it is the half a USER ACTION reaches, and the
//         only one that loses a row — but NOT the only one that ships, which
//         is what the bullet above says and what an earlier version of this
//         sentence contradicted three lines later. Routing it through
//         `downloadManager` is an architecture change and is FILED as
//         playhead-86sfq rather than taken here — read that bead before
//         assuming the row is the only thing that path loses; it also skips the
//         ownership bump, the transfer cancellation and three in-memory
//         indexes.
//

import Foundation
import OSLog

// MARK: - DownloadWorkJournalEventType

/// What the download path is reporting about one episode's transfer.
///
/// Three cases because the download path emits three, and no more: a
/// vocabulary wider than its emitters is a vocabulary whose extra cases nobody
/// can ever produce.
///
/// A raw value is written to disk, so these strings are a schema and renaming
/// one is a migration.
///
/// **These are NOT `WorkJournalEntry.EventType`, and the difference is not
/// cosmetic.** That enum's defining property is `orphanRecoveryRouting` —
/// which arm of `AnalysisCoordinator.recoverOrphans` a job takes when one of
/// its values is the last row for a `{episode, generation}`. NOTHING routes on
/// the values here: this table is read by a human on a device pull and by
/// nothing else in the app. Sharing the type would have said otherwise in the
/// one place a reader looks.
// `Codable` is deliberately absent: nothing encodes or decodes this type —
// persistence goes through `rawValue` explicitly, on both sides — and a
// conformance nobody exercises is a claim about the type that no test can
// check. `Sendable`/`Equatable`/`CaseIterable` are all used.
enum DownloadWorkJournalEventType: String, Sendable, Equatable, CaseIterable {
    /// The background transfer completed and its artifact is in place.
    /// The DENOMINATOR — see this file's header for why it is recorded.
    case finalized

    /// The background transfer ended terminally, or a persisted resume blob
    /// was unusable. `cause` carries which.
    case failed

    /// The transfer was cut short and may be resumed later — in practice the
    /// force-quit / cold-launch scan finding a live resume blob.
    case preempted
}

// MARK: - DownloadWorkJournalRecord

/// One download-path work-journal event, as it lands on disk.
struct DownloadWorkJournalRecord: Sendable, Equatable {

    /// Row identity. A UUID rather than `(episodeId, eventType)` because a
    /// repeated failure for one episode is among the most interesting things
    /// this table can show, and a key that collapsed it would hide the
    /// population worth sizing.
    let id: String

    /// The episode whose transfer this event is about. The ONLY identity the
    /// download path holds, and deliberately not foreign-keyed — see the
    /// header.
    let episodeId: String

    /// What happened.
    let eventType: DownloadWorkJournalEventType

    /// Why, when there is a why. `nil` exactly for ``DownloadWorkJournalEventType/finalized``:
    /// a successful transfer has no miss cause, and inventing one would put a
    /// value in a column whose whole job is to carry a reason.
    ///
    /// **THAT BICONDITIONAL IS A CONVENTION, NOT A CONSTRAINT, and the
    /// direction that is enforced is the one that has a writer.** No `CHECK`
    /// backs it and `insertDownloadWorkJournalEntry` accepts any combination.
    /// A `CHECK ((eventType = 'finalized') = (cause IS NULL))` was considered
    /// and rejected: it would reject a future wider `eventType` at INSERT time
    /// — turning a vocabulary extension into a store that cannot be written
    /// to — which is a worse failure than a mislabelled column. What is
    /// covered is `finalized` → nil (a rail and a mutant); `failed`/`preempted`
    /// → non-nil is the writer's discipline and nothing checks it.
    let cause: InternalMissCause?

    /// Unix epoch seconds at the moment the event was recorded.
    let occurredAt: Double

    /// The `SliceMetadata` JSON blob the emission site built — stage, error
    /// text, bytes written, device class. Opaque here: this type does not
    /// parse it, and neither does the store.
    ///
    /// It is the payload playhead-1nl6 removed the protocol default for,
    /// having found a default that forwarded to the metadata-less overload and
    /// silently dropped it. Persisting it is this conformer's explicit
    /// decision, made visible at the conformer as that bead requires.
    let metadataJSON: String

    init(
        id: String = UUID().uuidString,
        episodeId: String,
        eventType: DownloadWorkJournalEventType,
        cause: InternalMissCause?,
        occurredAt: Double,
        metadataJSON: String
    ) {
        self.id = id
        self.episodeId = episodeId
        self.eventType = eventType
        self.cause = cause
        self.occurredAt = occurredAt
        self.metadataJSON = metadataJSON
    }
}

// MARK: - DownloadWorkJournalPage

/// One read of `download_work_journal`, with everything a caller needs in
/// order not to over-read the array it is handed.
struct DownloadWorkJournalPage: Sendable, Equatable {

    /// The decoded rows, most recent first.
    let rows: [DownloadWorkJournalRecord]

    /// Rows whose `eventType` this build cannot decode — written by a build
    /// with a wider vocabulary. They are NOT in `rows` and they are NOT lost.
    ///
    /// They are DROPPED rather than folded into a default case for the reason
    /// playhead-7dgx gives one table over: a wider build's row collapsed into
    /// `failed` would silently inflate that population, and an inflated
    /// population is worse than a counted absence.
    ///
    /// The `cause` column needs no such counter — `InternalMissCause` carries
    /// a forward-compat `.unknown(String)` case for exactly this, so an
    /// unrecognized cause round-trips verbatim instead of being lost.
    let unrecognizedEventTypeRows: Int

    /// `true` when more rows exist than the `limit` allowed back. Without it a
    /// window that stops at its own ceiling reports "this is what happened"
    /// while meaning "this is what fitted".
    let truncated: Bool

    /// Every row this page could read AND could not read **OVER THIS WINDOW**,
    /// which is the honest denominator for a per-event share taken off `rows`
    /// and is NOT a table statistic.
    ///
    /// When `truncated` is true this equals `limit` exactly — the loop stops at
    /// `seen > ceiling` — so at the default 500 and L-1's 20 events a day it
    /// stops describing the table after about 25 days. The table's own totals
    /// come from SQL. The word "window" is on the property rather than only in
    /// a note beside the query because this property is what a caller reads.
    var totalRowsSeen: Int {
        rows.count + unrecognizedEventTypeRows
    }
}

// MARK: - DownloadWorkJournalArming

/// The single `download_work_journal_arming` row: the positive claim that
/// somebody was counting.
///
/// Read `armedLaunches == 0` as NOBODY WAS COUNTING, not as "no launches" —
/// the row is seeded by the V63 migration precisely so that "instrument
/// installed, never armed" is expressible rather than indistinguishable from
/// an empty table.
struct DownloadWorkJournalArming: Sendable, Equatable {

    /// Launches on which the analysis store opened and the recorder was live.
    /// A LOWER BOUND, and NOT a count of download attempts — see the header.
    let armedLaunches: Int

    /// Events whose durable row could NOT be written.
    ///
    /// This is what stops "armed, and zero rows" from being reachable by
    /// silence. Read `armedLaunches > 0 && rows == 0 && writeFailures == 0` as
    /// the positive claim; the same pair with `writeFailures > 0` says the
    /// opposite.
    let writeFailures: Int

    /// When the first arming happened. `nil` while `armedLaunches` is 0, and
    /// `nil` is the whole point: a zero here would date an arming that never
    /// occurred.
    let firstArmedAt: Double?

    /// When the most recent one happened. `nil` while `armedLaunches` is 0.
    let lastArmedAt: Double?

    /// The earliest moment this install is known to have carried the
    /// instrument.
    ///
    /// Usually the V63 migration. NOT ALWAYS: both
    /// `noteDownloadWorkJournalInstrumentArmed` and
    /// `noteDownloadWorkJournalWriteFailure` re-create the row if it is
    /// missing — deliberately, so a hand-edited or partially-rolled-back store
    /// still counts — and on those paths this stamps the ARM or the FAILURE
    /// instead. Read it as the sentence above, which is true on all three
    /// paths, rather than as the migration's own timestamp.
    let installedAt: Double
}

// MARK: - NoopWorkJournalRecorder, and why it is not redefined here

// `NoopWorkJournalRecorder` lives in `BackgroundSessionIdentifier.swift`
// alongside the protocol and is unchanged. It remains the right default for
// tests and previews. What changes with this bead is that PRODUCTION no longer
// holds one — see `AnalysisStoreDownloadWorkJournalRecorder` below and
// `DownloadWorkJournalWiringSourceCanaryTests`, which is what makes the wiring
// impossible to revert silently.

// MARK: - AnalysisStoreDownloadWorkJournalRecorder

/// Production binding of `WorkJournalRecording` for the DOWNLOAD path: writes
/// into `analysis.sqlite`, which is the file a device pull copies whole.
///
/// A `struct` over an immutable `AnalysisStore` reference — the shape
/// `AnalysisStoreBackgroundDownloadDropRecorder` and
/// `AnalysisStoreBackgroundTaskRunLedger` already use — so `Sendable` needs no
/// argument.
///
/// **Deliberately NOT `AnalysisStoreWorkJournalRecorder`.** That type writes
/// `work_journal`, whose `event_type` column is an input to cold-launch orphan
/// recovery; see this file's header for why routing download events there is a
/// live defect rather than an inert one.
///
/// Best-effort by design, on the `AnalysisStoreBackgroundDownloadDropRecorder`
/// precedent: a store error is logged, counted, and swallowed. Every caller is
/// on the recovery path of a failure it has already handled, and throwing here
/// would turn a recorded loss into an unrecorded one.
struct AnalysisStoreDownloadWorkJournalRecorder: WorkJournalRecording {

    private let store: AnalysisStore

    /// The surface-status stream, for the residual: an event whose row AND
    /// whose failure counter both failed to write. Optional because tests
    /// construct this without one; production always passes it.
    ///
    /// Read L-2 in this file's header before treating it as a second failure
    /// domain — it is a different FILE, not a different domain.
    private let invariantRecorder: (
        @Sendable (InvariantViolation.Code, String) -> Void
    )?

    private let logger = Logger(
        subsystem: "com.playhead",
        category: "DownloadWorkJournal"
    )

    init(
        store: AnalysisStore,
        invariantRecorder: (
            @Sendable (InvariantViolation.Code, String) -> Void
        )? = nil
    ) {
        self.store = store
        self.invariantRecorder = invariantRecorder
    }

    // MARK: WorkJournalRecording

    /// HONOURS CANCELLATION, and it is the only one of the four that does.
    ///
    /// `DownloadManager.retireBackgroundTransfers` CANCELS the finalization
    /// `Task` this runs inside, and its contract (see
    /// ``DownloadManager/backgroundJournalFinalizations``) is that "a recorder
    /// suspended before its durable append cannot publish a stale `.finalized`
    /// row" — cache deletion retires these before unlinking the bytes.
    /// Before this bead that held for free, because the recorder returned
    /// immediately; now the window is however long the `AnalysisStore` actor is
    /// busy, so it has to be honoured explicitly.
    ///
    /// THE CHECK THAT MATTERS IS THE ONE INSIDE THE ACTOR. An actor hop is not
    /// a cancellation point, so a `Task.isCancelled` read before it cannot see
    /// a cancellation landing during it —
    /// `insertDownloadWorkJournalEntryUnlessCancelled` is where the decision is
    /// made. `AnalysisStoreWorkJournalRecorder` keeps BOTH a pre-hop read and
    /// an in-actor one; this recorder deliberately keeps only the in-actor one,
    /// because a cheap pre-hop guard satisfies every test that can be written
    /// deterministically and would leave the in-actor check with no rail at
    /// all. The pre-hop read still exists at the production call site
    /// (`DownloadManager.handleBackgroundDownloadComplete`, at the `guard
    /// !Task.isCancelled` inside the journal `Task`), where it costs nothing.
    func recordFinalized(episodeId: String) async {
        await append(
            episodeId: episodeId,
            eventType: .finalized,
            cause: nil,
            metadataJSON: "{}",
            honoringCancellation: true
        )
    }

    /// The metadata-less overload. No download-path site calls it today; it is
    /// implemented rather than trapped because a protocol requirement whose
    /// body is `fatalError` is a crash waiting for the first new caller.
    func recordFailed(episodeId: String, cause: InternalMissCause) async {
        await append(
            episodeId: episodeId,
            eventType: .failed,
            cause: cause,
            metadataJSON: "{}",
            honoringCancellation: false
        )
    }

    /// Persists the blob. Stated here at the conformer, as playhead-1nl6
    /// requires: this recorder's decision is to KEEP the `SliceMetadata` JSON,
    /// not to drop it.
    func recordFailed(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {
        await append(
            episodeId: episodeId,
            eventType: .failed,
            cause: cause,
            metadataJSON: metadataJSON,
            honoringCancellation: false
        )
    }

    /// Persists the blob, same decision and same reason as above.
    func recordPreempted(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {
        await append(
            episodeId: episodeId,
            eventType: .preempted,
            cause: cause,
            metadataJSON: metadataJSON,
            honoringCancellation: false
        )
    }

    // MARK: Arming

    /// Record that this launch carried a LIVE download work-journal recorder,
    /// so that zero rows can be read as a positive claim instead of as
    /// silence.
    ///
    /// NOT a `WorkJournalRecording` requirement, and that is deliberate:
    /// adding one would oblige every conformer of a protocol shared with
    /// `AnalysisWorkScheduler` — including seven test doubles — to answer a
    /// question only the download half asks. `PlayheadRuntime` calls it on the
    /// SAME INSTANCE it injected into `DownloadManager`, and
    /// `DownloadWorkJournalWiringSourceCanaryTests` is what pins that identity,
    /// because nothing at runtime can see it.
    ///
    /// Idempotence is the CALLER's: this counts every call it receives.
    func recordInstrumentArmed(at now: Double) async {
        do {
            try await store.noteDownloadWorkJournalInstrumentArmed(at: now)
        } catch {
            logger.error(
                "download work journal NOT armed for this launch: \(String(describing: error), privacy: .public)"
            )
            // The DENOMINATOR's own silent failure. A launch whose arming write
            // failed is byte-identical on disk to a launch that never ran, and
            // it is one of the two things that make `armedLaunches = 0` beside
            // real rows reachable — so it is said out loud rather than left to
            // be inferred from a number that did not move.
            invariantRecorder?(
                .downloadWorkJournalNotRecorded,
                "arming=failed — this launch had a live download work-journal "
                + "recorder and download_work_journal_arming.armedLaunches did "
                + "not move, so any row it goes on to write has no launch in "
                + "the denominator: \(String(describing: error))"
            )
        }
    }

    // MARK: Private

    /// `honoringCancellation` is a DECISION STATED AT THE CONFORMER, which is
    /// what playhead-1nl6 requires of anything a recorder chooses to swallow.
    ///
    /// `true` only for `finalized`, and the asymmetry is the point. A finalized
    /// row is published from a `Task` the manager CANCELS when it deletes the
    /// bytes, so honouring cancellation is what stops a row claiming an
    /// artifact that is gone. The other three are awaited INLINE on the
    /// recovery path of a failure that has already happened — dropping one of
    /// those because an enclosing task was cancelled would lose exactly the
    /// record this bead exists to create, which is the opposite trade.
    private func append(
        episodeId: String,
        eventType: DownloadWorkJournalEventType,
        cause: InternalMissCause?,
        metadataJSON: String,
        honoringCancellation: Bool
    ) async {
        // NO CHEAP `Task.isCancelled` PRE-CHECK HERE, deliberately, and this is
        // the second time this bead has had to choose between a cheap guard and
        // a testable one. A pre-hop read would satisfy every test that cancels
        // BEFORE calling — which is every test one can write deterministically
        // — and the in-actor check, the only one that can see a cancellation
        // landing DURING the hop, would then have no rail at all. Its mutant
        // would SURVIVE, or worse, would be killed or not depending on task
        // scheduling. The hop costs a suspension on a cancelled finalization
        // and nothing else.
        let now = Date().timeIntervalSince1970
        let record = DownloadWorkJournalRecord(
            episodeId: episodeId,
            eventType: eventType,
            cause: cause,
            occurredAt: now,
            metadataJSON: metadataJSON
        )
        do {
            if honoringCancellation {
                try await store.insertDownloadWorkJournalEntryUnlessCancelled(
                    record
                )
            } else {
                try await store.insertDownloadWorkJournalEntry(record)
            }
            return
        } catch is CancellationError {
            // A CANCELLED WRITE IS NOT A FAILED WRITE. Counting it into
            // `writeFailures` would say this database could not hold a row,
            // which is a claim about the STORE — and it is the one reading that
            // counter exists to make.
            //
            // NOTHING IS LOST THAT ANYBODY ASKED TO KEEP, AND THE HONEST
            // REASON IS THAT NOTHING IN PRODUCTION CANCELS AT ALL TODAY.
            // MEASURED over `Playhead/**`: `retireBackgroundTransfers` has two
            // callers, `cancelDownload(episodeId:)` and `clearCache()`;
            // `cancelDownload`'s only caller is `removeCache(for:)`; and
            // `removeCache(for:)` and `clearCache()` BOTH have zero callers
            // outside `PlayheadTests/`. The retire/cancel mechanism is DORMANT
            // in shipping builds and is exercised only by tests.
            //
            // THREE REVIEW ROUNDS SHORTENED THAT SENTENCE WRONGLY, EACH ONE
            // FRAME FURTHER OUT, and the pattern is worth more than any of the
            // fixes. Review 2 read `cancelDownload` in isolation, concluded it
            // deleted nothing, and made the cancel conditional — which disarmed
            // the per-episode DELETE path and reddened
            // `cacheDeletionRacingFinalizationDoesNotJournalSuccess`. Review 3
            // caught that and wrote "two deleting callers". Review 4 found one
            // of the two has no production caller and wrote "ONE production
            // path retires". Review 5 found the other one has none either. Each
            // round tightened the PROSE while the instrument that would have
            // refuted it was pointed one call frame away — so the canary now
            // pins the tree-wide call-site COUNTS of all three, and every one
            // of them is a number rather than a claim.
            //
            // WHAT THAT MEANS FOR THE CANCELLATION HANDLING BELOW: it defends a
            // path nothing currently takes, which is why it is worth having
            // rather than worth removing — the moment a delete affordance is
            // wired to `removeCache` (there is no per-episode delete in the UI
            // today), the race is live and the row must not survive the bytes.
            // It is also why the fifth state in this file's header is marked
            // TEST-ONLY.
            //
            // AND THE BULK DELETE A USER ACTUALLY REACHES DOES NOT COME THROUGH
            // HERE AT ALL. Settings' "Clear Cached Audio" is
            // `SettingsViewModel.clearAudioCache`, which enumerates
            // `DownloadManager.defaultCacheDirectory()` from a DETACHED TASK
            // and unlinks its contents without entering this actor — so a
            // `finalized` row CAN outlive its bytes there. It is ONE OF TWO
            // deletion paths that run — LRU `evictIfNeeded` is the other, and
            // it is safe from this race only because it skips
            // `bgInFlightEpisodes`. This said "the only deletion path that
            // runs" for a round AFTER L-7 was corrected to say there are two,
            // and survived the sweep because the phrase is WRAPPED ACROSS TWO
            // LINES: no grep for it could match. Limit L-7; playhead-86sfq.
            return
        } catch {
            logger.error(
                "download work journal row NOT written for episode=\(episodeId, privacy: .public) event=\(eventType.rawValue, privacy: .public): \(String(describing: error), privacy: .public)"
            )
            await noteWriteFailure(
                episodeId: episodeId,
                eventType: eventType,
                cause: cause,
                at: now,
                rowError: error
            )
        }
    }

    /// A second, independent durable write, so that a pull holding only this
    /// database can still tell "no events" from "events this store could not
    /// hold". It can fail too — that residual is what reaches the
    /// surface-status stream.
    private func noteWriteFailure(
        episodeId: String,
        eventType: DownloadWorkJournalEventType,
        cause: InternalMissCause?,
        at now: Double,
        rowError: Error
    ) async {
        do {
            try await store.noteDownloadWorkJournalWriteFailure(at: now)
            return
        } catch {
            logger.error(
                "download work journal write-failure counter ALSO failed: \(String(describing: error), privacy: .public)"
            )
        }
        // Both durable writes failed, so nothing in this database records the
        // event at all. The description carries everything the lost row would
        // have, so the loss is recoverable from this line alone.
        invariantRecorder?(
            .downloadWorkJournalNotRecorded,
            "row=failed counter=failed episode=\(episodeId) "
            + "event=\(eventType.rawValue) "
            + "cause=\(cause?.rawValue ?? "none") "
            + "at=\(now) error=\(String(describing: rowError))"
        )
    }
}
