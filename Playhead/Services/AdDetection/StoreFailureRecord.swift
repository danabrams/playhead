// StoreFailureRecord.swift
// playhead-sckv: the typed `AnalysisStoreError` arm wrote `String(describing:)`
// into the durable `backfill_jobs.deferReason` column — the exact defect
// `playhead-59c8` removed from the arm DIRECTLY BELOW IT, three days earlier,
// and the one `playhead-v7q6` forbids by name.
//
// ===== WHAT WAS WRONG, IN ONE LINE =====
//
//     try await store.markBackfillJobFailed(
//         jobId: job.jobId,
//         reason: String(describing: storeError),   // <- the defect
//         retryCount: persistedRetryCount
//     )
//
// **WHAT THAT ACTUALLY WROTE IS WORSE THAN THE REFLECTION FORM, AND THE BEAD'S
// OWN EXAMPLES HAD IT WRONG.** The bead predicted
// `insertFailed("UNIQUE constraint failed: …")` — `String(describing:)`'s
// reflected spelling, which at least leads with the case name. But
// `AnalysisStoreError` conforms to `CustomStringConvertible`, and
// `String(describing:)` prefers that conformance over reflection, so what the
// column really got was the enum's own prose (verified by running it):
//
//     .insertFailed("UNIQUE constraint failed: semantic_scan_results.id")
//         -> "Insert failed: UNIQUE constraint failed: semantic_scan_results.id"
//     .openFailed(code: 14, message: "unable to open database file")
//         -> "SQLite open failed (14): unable to open database file"
//     .notFound
//         -> "Row not found"
//
// So the payload is still there, unbounded and per-row — and the CASE NAME is
// gone. `notFound` persists as `Row not found`, which names no case at all; a
// reader cannot recover `notFound` from it except by matching the sentence
// against the enum's source at that commit. No `GROUP BY` counts that population
// and no prefix grep finds it. It is also not an API contract in either
// direction: a `description` is written for humans and is edited freely, so
// yesterday's rows and today's rows disagree about what the same failure is
// called and nothing in the type system notices. This repo's standing defect
// class, one column over: a value that names one thing (a DESCRIPTION, for
// humans, unstable) stored where another is meant (a CODE, for machines, stable).
// It is also why reading the DEFECT off the bead rather than off the running
// program would have produced a test asserting on a string the app never wrote.
//
// ===== WHAT MAKES IT WORSE HERE THAN ON THE GENERIC ARM =====
//
// The runner ALREADY COMPUTED both halves of the honest answer and threw them
// away. `Self.caseName(of: storeError)` — a stable case-name token that exists
// for exactly this reason, added by bd-1tl — was computed TWO LINES BELOW the
// write and spent on an ephemeral log line. `Self.isPermanent(storeError)` was
// computed ABOVE the write, used to decide the retry charge, and then computed a
// second time for the same log line. A row's PERMANENCE is the single most useful
// thing about it: it is what separates a charge of 1 from a short-circuit
// straight to `AdmissionController.maxRetries`, i.e. whether the job is retired.
// That was unreadable from a device pull while both values sat in scope.
//
// ===== WHAT THE COLUMN HOLDS TODAY, MEASURED RATHER THAN ASSUMED =====
//
// Every preserved device pull, `backfill_jobs.deferReason`, counted with
// `GROUP BY` (db-pull8 through db-pull12, 2026-08-11 → 2026-08-15):
//
//   pull  rows  distinct spellings, all of them
//   ----  ----  --------------------------------------------------------------
//     8      4  underCoverageBudgetSpent-fullEpisodeScan ×3
//                 · expiredWithoutProgress-fullEpisodeScan ×1
//     9      4  the same four
//    10      9  underCoverageBudgetSpent-fullEpisodeScan ×8
//                 · ONE `Error Domain=FoundationModels.LanguageModelError
//                   Code=-1 "…(ModelManagerServices.ModelManagerError error
//                   1001.)"` — 300 characters, the row playhead-59c8 was filed
//                   from, and written by the GENERIC arm
//    11     11  four named tokens, no prose
//    12     13  five named tokens, no prose
//
// **The typed-store arm has produced ZERO rows across all five pulls.** Not one
// `AnalysisStoreError` has reached this write in the field. So there is nothing
// to migrate — see the note on repair below, which states that as a finding
// rather than as an assumption, because "the population is empty" is exactly the
// kind of claim that is worth writing down with its counts.
//
// ===== THE SHAPE, WHICH IS 59c8's AND DELIBERATELY NOT A SECOND CONVENTION ====
//
//     storeFailure-<phase>(case=<caseName>,permanent=<true|false>)
//
// A greppable prefix that names the CONDITION (`FMDaemonRefusal`'s R2-Fix1 rule),
// the phase, and a parenthetical carrying what a reader needs NEXT without
// changing what the grep counts — exactly `unclassifiedModelError-<phase>(
// domain=…,code=…,under=…)` and `metadataStall-refused(peers=0)` before it. Two
// arms one line apart writing two different shapes is how this bead came to
// exist; it is not repeated here.
//
// **Why `permanent=` is a second FIELD and not a second RULER.** It is not
// derivable from `case=`: `insertFailed` classifies BOTH ways, permanent when its
// message carries `payloadTooLarge:` or
// `AnalysisStore.impossibleWindowGeometryPrefix` and recoverable otherwise. So a
// reader who had only the case name could not tell a row charged 1 from a row
// short-circuited to `maxRetries`, which is the whole decision the arm made. The
// two fields answer different questions — WHICH error, and WHAT THE RUNNER
// CONCLUDED about it — and the token carries the conclusion because that is the
// part no later reader can re-derive from the column.
//
// **What it reads when the thing never happened.** The token is written from one
// arm only, so no `storeFailure-` rows means no typed store error reached it —
// which is what all five pulls say today. Within a row every field is present and
// positive: `permanent=false` is a claim, never an omission.
//
// **It is not a second `retryCount` and not a classification.** Nothing here
// counts anything and nothing here decides anything. `isPermanent` is computed by
// `BackfillJobRunner` and drives the charge; this type only makes the decision it
// already made READABLE. Passing the two values in rather than deriving them here
// is deliberate: the runner binds each ONCE, above the write, and the arithmetic,
// the token and the log line all consume the same two locals, so no two of them
// can disagree about the row in front of them.
//
// ===== NO MIGRATION, AND THE ARGUMENT IS COUNTS RATHER THAN CAUTION =====
//
// `playhead-e6d3`'s rule — a repair is principled exactly when the rows were
// failed under a rule that no longer holds — does not even get to apply here:
// there are no rows. The population this bead changes the spelling of is EMPTY on
// every pull that exists (0 of 4, 0 of 4, 0 of 9, 0 of 11, 0 of 13). A migration
// predicate over it would have to string-match `String(describing:)` output,
// which is the quantity this bead exists to remove, and it would match nothing.
// `playhead-vmq5` (the repair question this bead blocks) is now writable against
// a token instead: `deferReason LIKE 'storeFailure-%'`.
//
// ===== WHO READS THE COLUMN, ENUMERATED =====
//
// Exactly one production consumer matches on `backfill_jobs.deferReason` content:
// `AnalysisStore`'s V50 repair, `deferReason LIKE 'underCoverageBudgetSpent-%'`
// (playhead-e6d3). It cannot match a `storeFailure-` row and is unaffected.
// `SemanticScanClaim` owns a second prefix (`scan_claim:`) that only SQL reads.
// Everything else — `PersistedStateInvariants`' `defer_reason=` witness,
// `DiagnosticsBundleBuilder` — only SANITIZES and PRINTS. Nothing switches on the
// value, so this change cannot alter any behaviour; it changes what a device pull
// can count.
//
// LIMIT, named rather than hidden, and the same one `UnclassifiedModelFailure`
// names: `PersistedStateInvariantEvaluator.sanitize` truncates a `deferReason` to
// 80 characters for its witness line. The longest token this type can emit is 83
// (`scanRandomAuditWindows` / `specialistHostReadScan` with
// `evidenceEventBodyMismatch`); on `fullEpisodeScan`, the only phase any field row
// has ever carried, the worst case is 76 and fits. The COLUMN holds the whole
// thing and a device pull reads the column. Do not read a truncated witness as
// the record.

import Foundation

/// The durable, countable record of an ``AnalysisStoreError`` that failed a
/// backfill job — the value `BackfillJobRunner` writes to
/// `backfill_jobs.deferReason` on the typed-store arm.
///
/// **This type deliberately decides nothing.** It is the sibling of
/// ``UnclassifiedModelFailure`` in role, not in family: that one records a throw
/// the app COULD NOT classify, this one records one it DID classify and what it
/// concluded. Neither is a ``FMDaemonRefusal`` — no row written here is excused,
/// deferred, or given its retry budget back.
enum StoreFailureRecord {

    /// The greppable family. Its own prefix, sharing none with `rateLimited-`,
    /// `metadataStall-`, `inferenceTimeout-`, `expiredWithoutProgress-`,
    /// `cancelled-during-`, `underCoverage-`, `underCoverageBudgetSpent-`,
    /// `transcriptCeilingBelowFloor-`, `unclassifiedModelError-` or
    /// `scan_claim:`.
    ///
    /// The prefix is the CONDITION, per ``FMDaemonRefusal``'s R2-Fix1 rule, and
    /// the condition here is "the analysis store refused this write". Joining an
    /// existing family would put a persistence failure into a count that means
    /// something else entirely.
    static let causePrefix = "storeFailure"

    /// The durable cause. `storeFailure-<phase>(case=…,permanent=…)`.
    ///
    /// Both values are passed IN rather than derived here, and that is the point:
    /// the runner binds `caseName` and `isPermanent` once, above the write, and
    /// the retry arithmetic, this token and the log line all read those same two
    /// locals. Deriving them a second time here would be a second ruler for a
    /// quantity the arm has already measured.
    ///
    /// - Parameters:
    ///   - caseName: the stable case-name token from
    ///     ``BackfillJobRunner/caseName(of:)`` — the case alone, never its
    ///     payload.
    ///   - isPermanent: what the runner concluded about REPRODUCIBILITY, i.e.
    ///     whether replaying these inputs against this schema fails identically.
    ///     It is the input to the short-circuit that sends the row straight to
    ///     `AdmissionController.maxRetries`.
    ///   - phase: the job's phase, so a reader can tell which pass died without
    ///     cross-referencing logs.
    static func deferReason(
        caseName: String,
        isPermanent: Bool,
        phase: BackfillJobPhase
    ) -> String {
        "\(causePrefix)-\(phase.rawValue)"
            + "(case=\(sanitize(caseName)),permanent=\(isPermanent))"
    }

    /// Make a case name safe to live in the token's `key=value` grammar.
    ///
    /// Delegates to ``UnclassifiedModelFailure/sanitize(_:)`` because the GRAMMAR
    /// is shared — no whitespace, and none of the four characters that would end
    /// a field or a parenthetical — and one implementation cannot drift from
    /// itself. What is NOT shared is the budget: that function bounds a
    /// fully-qualified type name at 64 characters, and the longest case name this
    /// enum can be handed is `evidenceEventBodyMismatch` at 25, so the bound is
    /// not binding and never has been.
    ///
    /// That coupling is deliberately made OBSERVABLE rather than trusted:
    /// `StoreFailureRecordTests` asserts `sanitize(caseName(of: e)) == caseName(of: e)`
    /// for every one of the thirteen `AnalysisStoreError` cases, so a future bead
    /// that tightens the domain budget below 25 turns that rail red and names the
    /// coupling instead of silently truncating a case name in a durable column.
    ///
    /// A name that sanitizes away to nothing reads
    /// ``UnclassifiedModelFailure/unknownDomainToken`` — the sentinel is shared
    /// on purpose and the FIELD NAME is what disambiguates it, so `case=unknown`
    /// and `domain=unknown` are never confusable in a pull. It is unreachable
    /// from ``BackfillJobRunner/caseName(of:)``, whose thirteen returns are
    /// compile-time identifiers; seeing it means that mapping grew a case whose
    /// name is not a bare identifier, which is a defect the field states rather
    /// than an empty `case=` a reader would have to notice.
    static func sanitize(_ caseName: String) -> String {
        UnclassifiedModelFailure.sanitize(caseName)
    }
}
