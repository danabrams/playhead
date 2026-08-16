// DurableThrowRecord.swift
// playhead-3lc3: four production sites wrote a Swift value's DESCRIPTION into
// three durable columns. It is the defect `playhead-59c8` removed from
// `backfill_jobs.deferReason`'s generic arm, `playhead-sckv` removed from the
// same column's typed arm, and `playhead-v7q6` forbids by name — surviving in
// the three columns nobody had swept.
//
// ===== THE FOUR LINES =====
//
//   1. `AnalysisWorkScheduler` outer catch, TERMINAL arm -> analysis_jobs.lastErrorCode
//          lastErrorCode: "\(Self.maxAttemptsReachedPrefix)\(error.localizedDescription)"
//   2. `AnalysisWorkScheduler` outer catch, RETRY arm     -> analysis_jobs.lastErrorCode
//          lastErrorCode: error.localizedDescription
//   3. `AnalysisCoordinator.runPipeline` outer catch      -> analysis_sessions.failureReason
//          failureReason: String(describing: error)
//      (`resumeBackfillForTesting` is the same line on a DEBUG seam.)
//   4. `BackgroundProcessingService` pre-analysis recovery -> background_task_runs.lastErrorCode
//          lastErrorCode: String(describing: error)
//
// ===== `localizedDescription` IS THE WORSE OF THE TWO AND READS LIKE THE SAFER =====
//
// It is LOCALIZED. The same failure produces different bytes on a device whose
// language differs, so the column cannot be grouped ACROSS devices at all — and
// it is worse than that within one device, because neither `AnalysisStoreError`
// nor `AnalysisCoordinatorError` conforms to `LocalizedError`. Foundation then
// synthesises the description from the bridged `NSError`, and what lands in the
// column is a SENTENCE plus an ENUM ORDINAL. Measured by running it, because the
// prediction was wrong — see below:
//
//     AnalysisStoreError.notFound.localizedDescription
//         -> "The operation couldn’t be completed. (Playhead.AnalysisStoreError error 12.)"
//     AnalysisCoordinatorError.noAudioAvailable(episodeId:).localizedDescription
//         -> "The operation couldn’t be completed. (Playhead.AnalysisCoordinatorError error 1.)"
//
// Read what that column held: a localized apology and a number. Note the
// typographic apostrophe — U+2019, not `'` — so even a same-locale `GROUP BY` is
// one Unicode normalisation away from splitting the population.
//
// **AND THE NUMBER IS NOT THE DECLARATION INDEX, WHICH IS THE STRONGER FORM OF
// THE SAME COMPLAINT.** The first draft of `DurableThrowRecordTests` predicted
// `4`, because `notFound` is the fifth case declared. It is **12**. The bridge
// uses the enum's TAG, and a multi-payload enum lays its PAYLOAD cases out first
// in declaration order and its EMPTY cases after them — the same layout
// `UnclassifiedModelFailure`'s header derives for `ModelManagerError` ("29
// payload cases (tags 0…28) then 20 empty cases"). Twelve of these thirteen
// cases carry a payload, so the single empty one lands at 12. So the persisted
// number moves not only when a case is added or reordered, but when an
// ASSOCIATED VALUE IS ADDED TO ANY EXISTING CASE — no case added, none renamed,
// none reordered, and every historical row now names a different failure. That
// was learned by writing the prediction down and watching the rail go red, which
// is the only way this file's claims are allowed to be made.
//
// `String(describing:)` at least produces one spelling per build. But not the
// spelling anyone predicts: BOTH error types here conform to
// `CustomStringConvertible`, which `String(describing:)` prefers over
// reflection, so what the column got was the enum's PROSE and not its case —
// `.notFound` persisted as `Row not found`, which names no case at all. That is
// the trap `playhead-sckv` measured on `AnalysisStoreError` and it repeats here
// on `AnalysisCoordinatorError`. Verify by running, never by reading the type.
//
// ===== WHY THIS IS 59c8's SHAPE AND NOT sckv's, WHICH IS A REAL DIFFERENCE ====
//
// `playhead-sckv` records `case=<caseName>` because its site catches a TYPED
// `AnalysisStoreError` — a closed enum of thirteen cases with a compile-time
// name for each. **All four sites here catch `any Error`**: the scheduler's
// outer catch stands under a runner, a store and an asset resolver; the
// coordinator's stands under the whole pipeline; the recovery task's stands
// under `reconcile()` and a drain. There is no case name to carry, because there
// is no enum.
//
// That is exactly the situation `playhead-59c8` faced on the generic arm, and
// its answer is the one reused here verbatim: an error's stable machine identity
// is its bridged `(domain, code)` plus the DEEPEST `(domain, code)` its
// `NSUnderlyingErrorKey` chain reaches. `error as NSError` is total in Swift, so
// it never fails and never guesses. This file therefore delegates BOTH halves of
// the grammar to ``UnclassifiedModelFailure`` — `identity(of:)` and
// `sanitize(_:)` — rather than restating them, for the reason
// ``StoreFailureRecord/sanitize(_:)`` gives one column over: one implementation
// cannot drift from itself.
//
// ===== WHAT THE THREE COLUMNS HOLD TODAY, COUNTED RATHER THAN ASSUMED =====
//
// Five preserved device pulls, db-pull8 → db-pull12 (2026-08-13 09:46 →
// 2026-08-15 20:09).
//
// **COUNT ROW IDENTITIES ACROSS THE UNION, NOT PER-PULL TOTALS SUMMED.** The
// first draft of this paragraph added the five snapshots together and reported
// 26 non-null job causes, 44 background ones and 9 sessions. These are five
// photographs of three growing tables, so that arithmetic counts the same row up
// to five times — this repo's standing defect class committed inside the
// paragraph that measures it. The figures below are `SELECT DISTINCT` over the
// five files:
//
//   analysis_jobs.lastErrorCode       38 rows in the largest snapshot. NINE
//                                     distinct jobIds have EVER carried a
//                                     non-null cause, over FOUR distinct
//                                     spellings.
//   analysis_sessions.failureReason   FOUR distinct sessions have ever existed.
//                                     `failureReason` is NULL on every one, in
//                                     every pull.
//   background_task_runs.lastError…   268 rows in the largest snapshot.
//                                     SEVENTEEN distinct runIds have ever
//                                     carried a non-null cause, and all
//                                     seventeen are `orphan_at_launch`.
//
// **NO MIGRATION IS NEEDED, AND FOR THREE DIFFERENT REASONS — say which.**
//
//   * `analysis_sessions.failureReason`: the population is EMPTY. Four sessions,
//     NULL on all four, every pull. Nothing to repair.
//   * `background_task_runs.lastErrorCode`: seventeen rows have a cause and
//     every one is `orphan_at_launch`, a named token written by
//     `reapOrphanBackgroundTaskRuns` from a different arm. The arm this bead
//     changes has produced zero rows.
//   * `analysis_jobs.lastErrorCode`: the four spellings ever observed are
//     `coverageInsufficient:noProgress`, `backgroundWindowExpired`,
//     `transcription:cancelled` and
//     `assetResolution: Insert failed: UNIQUE constraint failed: …`. The first
//     three are named tokens from other arms. **The fourth IS prose, and FIVE of
//     the nine rows that ever carried a cause carried it** — and it is written
//     by NEITHER of the two sites this bead fixes. It comes from the
//     asset-resolution arm's `"assetResolution: \(error)"`, two lines the bead's
//     own enumeration missed; filed as `playhead-3c4k` rather than fixed here.
//
// **AND A PULL IS A LOWER BOUND, NOT A CENSUS.** Traced per row across the five
// files, every one of those five prose rows was later CLEARED OR OVERWRITTEN by
// a named token from a different arm:
//
//   070B2272  p8=assetResolution  p9=assetResolution  p10=NULL   p11=NULL   p12=NULL
//   133D75FD  p8=assetResolution  p9=NULL             p10=bgWinExpired  p11=bgWinExpired  p12=NULL
//   26D9075B  p8=assetResolution  p9=assetResolution  p10=bgWinExpired  p11=NULL   p12=NULL
//   2BCAB736  p8=assetResolution  p9=assetResolution  p10=NULL   p11=NULL   p12=NULL
//   6966117E  p8=assetResolution  p9=assetResolution  p10=transcription:cancelled  p11=NULL  p12=NULL
//
// `lastErrorCode` is a LAST-WRITER column, not an append-only ledger: the next
// arm to touch the row overwrites it and `requeueOrphanedLease` NULLs it
// outright. So the number of times prose has been written here is strictly
// greater than the number of prose rows any pull can show, and the true count is
// unrecoverable from these files. Five is a floor.
//
// So the honest reading is the opposite of the reassuring one. This column is
// not a theoretical risk that has never fired: **it is the one of the three that
// has demonstrably taken a Swift error's description in the field, through a
// fifth site, on a majority of the rows that ever carried a cause.** The two
// arms this bead fixes have simply not been the ones to fire yet. Cheap now,
// expensive on the first field failure.
//
// ===== WHO READS THESE COLUMNS, ENUMERATED PER COLUMN =====
//
// `analysis_jobs.lastErrorCode` — ONE production consumer switches on content,
// and this bead writes into it: ``AnalysisWorkScheduler/isAttemptCapTerminal(_:)``
// tests `state == "superseded" && lastErrorCode.hasPrefix("maxAttemptsReached:")`,
// reached from `AnalysisJobReconciler`'s cap-out-retry rescue and from
// ``AnalysisWorkScheduler/isRescuableTerminal(_:)``. **Site 1 writes exactly the
// row that predicate matches**, so the `maxAttemptsReached:` prefix is
// load-bearing and is preserved verbatim; only the SUFFIX changes, from a
// localized apology to a token. `isNoProgressTerminal(_:)` compares the whole
// string to `coverageInsufficient:noProgress` and cannot match either spelling.
// `PersistedStateInvariantEvaluator` tests PRESENCE (`!= nil`) and then
// sanitizes for a witness line; `ActivitySnapshotProvider` and
// `DogfoodDiagnosticsAnalysisHealth` only sanitize and print. No SQL predicate.
//
// `analysis_sessions.failureReason` — ONE production consumer switches on
// content, and it is the only SQL predicate over any of the three columns:
// `AnalysisStore.fetchFailedSessions(withFailureReasonPrefix:)`,
// `WHERE state = ? AND failureReason LIKE ?`, called from
// ``AnalysisCoordinator/recoverCoverageGuardFailures()`` with
// `coverageGuardFailureReasonPrefix = "transcript coverage "`, which then PARSES
// A DOUBLE out of the matched string and divides by it. **Nothing has written
// that prefix since `e9388f5f`** (`feat(gtt9.8): classifyBackfillTerminal +
// finalizeBackfill rewire`), which deleted the `String(format: "transcript
// coverage %.1f/%.1fs (ratio %.3f < %.3f)", …)` the sweep was built to read. So
// it is a stranded reader looking for a prefix no writer produces — filed
// separately, not repaired here. It is unaffected either way: neither the old
// spelling nor the new one can begin with `transcript coverage `, and the token
// this file emits contains no space at all.
//
// `analysis_sessions.failureReason` has two further reads, both harmless and
// both worth naming so the next sweep does not have to rediscover them:
// `DogfoodDiagnosticsAnalysisHealth` tests it for EMPTINESS to raise a
// `.missingFailureReason` staleness flag (a token is never empty, and neither
// was the prose), and interpolates it into a redacted, truncated `.retry`
// recommendation note. Neither branches on its content.
//
// `background_task_runs.lastErrorCode` — NOTHING switches on it, and **nothing
// exports it either**. `finishRun` writes it, `reapOrphanBackgroundTaskRuns`
// writes the literal `orphan_at_launch`, and the only SQL that mentions it
// besides those is `lastErrorCode = COALESCE(?, lastErrorCode)` preserving the
// old value on a NULL bind. An earlier draft of this file claimed
// `DiagnosticsExportService` exports it as `last_error_code`; that is FALSE and
// an independent enumeration caught it. That JSON key belongs to
// `DogfoodDiagnosticsAnalysisJobSnapshot`, i.e. to `analysis_jobs`; the only
// export path that touches background runs is `RediffBackgroundRunSummary`,
// which projects eight fields and drops this one.
//
// That correction makes the case for a token STRONGER rather than weaker. A
// column that no code reads and no bundle exports has exactly one consumer left
// — a human running `GROUP BY` on a device pull — which is precisely the reader
// a localized sentence defeats.
//
// ===== TWO PROPERTIES, STATED SO THEY CAN BE CHECKED =====
//
//   * **What each token reads when the thing never happened.** Each is written
//     from ONE arm, so an absent prefix is a positive claim that the arm never
//     fired. Within a token every field is present: `under=none` says the error
//     carried no underlying chain, and is never an empty value or a dropped
//     field.
//   * **Nothing here decides anything.** No retry is charged, no permanence is
//     concluded, no state is chosen. Each function turns a throw the site could
//     not classify into something a device pull can `GROUP BY`. Unlike
//     `StoreFailureRecord`, there is no second field carrying a CONCLUSION —
//     because these arms conclude nothing about the error itself. The scheduler's
//     terminal-vs-retry decision is already readable from the row (`state`,
//     `attemptCount`, and the `maxAttemptsReached:` prefix), and duplicating it
//     inside the token would be a second ruler for a quantity the row measures.
//
// LIMIT, named rather than hidden, and the same one both sibling types name:
// `PersistedStateInvariantEvaluator.sanitize` truncates to 80 characters for its
// witness line. The COLUMN holds the whole token and a device pull reads the
// column. Do not read a truncated witness as the record.

import Foundation

/// The durable, countable record of a throw that reached one of three
/// catch-alls standing over an open set of error types.
///
/// **Deliberately decides nothing** — see this file's header. It is the sibling
/// of ``UnclassifiedModelFailure`` in both role and grammar: that one records an
/// unclassifiable throw in `backfill_jobs.deferReason`, this one records the
/// same class of event in `analysis_jobs.lastErrorCode`,
/// `analysis_sessions.failureReason` and `background_task_runs.lastErrorCode`.
/// It is NOT a sibling of ``StoreFailureRecord``, whose `case=` field only
/// exists because its one site catches a closed thirteen-case enum.
enum DurableThrowRecord {

    /// The greppable family for `analysis_jobs.lastErrorCode`.
    ///
    /// Its own prefix, sharing none with the other causes that column holds:
    /// `maxAttemptsReached:`, `staleFingerprint:`, `assetResolution:`,
    /// `backgroundWindowExpired`, `coverageInsufficient:`, `cancelMidRun`,
    /// `transcription:` or `reconciler_unavailable`.
    ///
    /// The prefix is the CONDITION, per ``FMDaemonRefusal``'s R2-Fix1 rule, and
    /// the condition is "the job's run threw something this scheduler did not
    /// classify". It is deliberately NOT a phase or an arm: the terminal arm
    /// still carries ``AnalysisWorkScheduler/maxAttemptsReachedPrefix`` IN FRONT
    /// of this token, which is what `isAttemptCapTerminal(_:)` matches on, so a
    /// capped row reads `maxAttemptsReached:jobThrew(…)` and a retried one reads
    /// `jobThrew(…)`. One prefix per condition; the arm is spelled by the
    /// existing prefix and by `state`.
    static let jobThrewPrefix = "jobThrew"

    /// The greppable family for `analysis_sessions.failureReason`.
    ///
    /// Sharing none with that column's other writers — `transcript coverage `
    /// (the stranded coverage-guard prefix), `Unknown state in DB: `,
    /// `corrupt session state on resume: `, or the prose
    /// `classifyBackfillTerminal` verdict reasons.
    ///
    /// The condition is "the analysis pipeline threw and the session was failed
    /// for it".
    static let sessionPipelineThrewPrefix = "sessionPipelineThrew"

    /// The greppable family for `background_task_runs.lastErrorCode`.
    ///
    /// Sharing none with that column's only other value, `orphan_at_launch`, or
    /// with the `reconciler_unavailable` written by the sibling arm eight lines
    /// up in the same handler.
    ///
    /// The condition is "the pre-analysis recovery body threw". The row already
    /// carries `entryPoint`, so the prefix does not restate which task it was.
    static let recoveryThrewPrefix = "recoveryThrew"

    /// The identity fields shared by all three tokens:
    /// `domain=…,code=…,under=…`.
    ///
    /// Delegates wholly to ``UnclassifiedModelFailure/identity(of:)`` and
    /// ``UnclassifiedModelFailure/sanitize(_:)``. Not re-derived here, and not
    /// re-bounded here: that type's 64-character domain budget, its
    /// head-plus-tail truncation with a `~` marker, and its eight-level cap on
    /// the underlying walk are the measured values `playhead-59c8` established,
    /// and a second copy of them is a second ruler.
    ///
    /// `under=none` when the error carried no `NSUnderlyingErrorKey` /
    /// `NSMultipleUnderlyingErrorsKey` chain — a WORD, so a reader can tell "no
    /// underlying error" from "the field was dropped".
    static func identityFields(of error: Error) -> String {
        let identity = UnclassifiedModelFailure.identity(of: error)
        let under: String
        if let underlyingDomain = identity.underlyingDomain,
           let underlyingCode = identity.underlyingCode {
            under = "\(UnclassifiedModelFailure.sanitize(underlyingDomain))/\(underlyingCode)"
        } else {
            under = UnclassifiedModelFailure.noUnderlyingToken
        }
        return "domain=\(UnclassifiedModelFailure.sanitize(identity.domain))"
            + ",code=\(identity.code)"
            + ",under=\(under)"
    }

    /// The durable cause for `analysis_jobs.lastErrorCode`, written by
    /// ``AnalysisWorkScheduler``'s outer catch.
    ///
    /// `jobThrew(domain=…,code=…,under=…)`. The TERMINAL arm prepends
    /// ``AnalysisWorkScheduler/maxAttemptsReachedPrefix`` to this value — that
    /// prefix is what `isAttemptCapTerminal(_:)` matches, and it must stay in
    /// front. The retry arm writes this value alone.
    static func jobLastErrorCode(for error: Error) -> String {
        "\(jobThrewPrefix)(\(identityFields(of: error)))"
    }

    /// The durable cause for `analysis_sessions.failureReason`, written by
    /// ``AnalysisCoordinator``'s pipeline catch-all.
    ///
    /// `sessionPipelineThrew-<resumeState>(domain=…,code=…,under=…)`.
    ///
    /// - Parameter resumeState: the state the pipeline was DRIVING when it
    ///   threw — the one field the row itself can never answer, because the
    ///   write that carries this reason overwrites `analysis_sessions.state`
    ///   with `failed`. A `queued` throw and a `backfill` throw are different
    ///   failures with the same identity, and without this they are one string.
    ///   This is the analogue of ``UnclassifiedModelFailure``'s `<phase>`, and
    ///   it is the only place any of the three tokens carries a second
    ///   discriminator, because it is the only place one is destroyed by the
    ///   write.
    static func sessionFailureReason(for error: Error, resumeState: SessionState) -> String {
        "\(sessionPipelineThrewPrefix)-\(resumeState.rawValue)(\(identityFields(of: error)))"
    }

    /// The durable cause for `background_task_runs.lastErrorCode`, written by
    /// the pre-analysis recovery task's failure arm.
    ///
    /// `recoveryThrew(domain=…,code=…,under=…)`.
    static func backgroundTaskLastErrorCode(for error: Error) -> String {
        "\(recoveryThrewPrefix)(\(identityFields(of: error)))"
    }
}
