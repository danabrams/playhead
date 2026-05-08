// ChapterPlanAssembler.swift
// playhead-au2v.1.8: Plan-level failure handling on top of per-call FM
// labeling (au2v.1.7's `ChapterLabelingService`).
//
// The assembler is the LAYER ABOVE per-chapter labeling: it sees every
// `LabelingResult` for an episode and decides (a) whether the FM was
// healthy enough that we can trust ANY plan it produced, and (b) which
// chapters survive into the assembled `ChapterPlan`.
//
// Two policies, derived from the bead spec:
//
// 1. Operational rate threshold (system-distrust signal).
//    If `operationalUnclearCount / totalChapters > 0.30`, the FM was too
//    unhealthy this run for the per-chapter answers to be trusted. We
//    do NOT assemble a plan; the caller (phase orchestrator, bead
//    au2v.1.10/.12) treats this as an abort signal and emits the
//    `chapter_phase_operational_unclear_rate_exceeded` diagnostic. The
//    phase will retry on the next backfill window when the FM is
//    healthier. Threshold strictness is INTENTIONALLY `>` (strict), not
//    `>=`: exactly 30% does NOT abort. The reasoning is that a single
//    extra operational failure on a 10-chapter episode (3 → 4 of 10)
//    SHOULD push us across; matching strictly at 3/10 is a coin-flip
//    boundary and the spec author's intent is "more than a third".
//
// 2. Plan assembly with semantic/operational distinction.
//    - Operational unclears are REMOVED from the plan. We do not know
//      what the chapter actually was, and we will not fabricate a
//      label; the surrounding chapters effectively absorb the gap.
//      `ChapterEvidence` does not carry an explicit ordinal index, so
//      "re-indexing" amounts to dropping the rows from the array;
//      remaining chapters keep their original `startTime` / `endTime`
//      / `title`.
//    - Semantic unclears (FM said "I cannot tell") are KEPT with
//      `disposition = .ambiguous` and `qualityScore` from the FM's
//      reported confidence (typically low). Downstream consumers
//      naturally weight via `qualityScore`.
//
// 3. Plan-level high-unclear warning.
//    After assembly, if `(operational + semantic) / totalChapters
//    > 0.50` we set a flag in the assembly result. The plan IS still
//    written; per-chapter `qualityScore` weighting handles trust at
//    the consumer side. The caller decides whether and how to surface
//    the warning (typically by emitting the new
//    `chapter_phase_high_unclear_rate` diagnostic).
//    The same strict-`>` semantics apply: exactly 50% does NOT trip
//    the warning.
//
// Confidence math:
//   `planConfidence` is computed by reusing `ChapterPlan.computePlanConfidence`,
//   which already implements the duration-weighted formula:
//     `sum(c.qualityScore × c.duration) / total_duration`
//   Reusing the existing method avoids math drift and keeps a single
//   source of truth for malformed-bound / non-finite handling.
//
// Why this lives in its own file:
//   The bead spec offers three locations: extend `ChapterLabelingService`,
//   live inside `ChapterGenerationPhase`, or a new file. We pick the new
//   file because:
//     * `ChapterLabelingService` is a leaf that handles ONE region —
//       coupling plan-level policy into it would muddle that
//       responsibility.
//     * `ChapterGenerationPhase` is bead 10's territory and may be in
//       flight in another worktree; touching it from this bead would
//       create a merge hazard.
//   A standalone module also keeps the unit tests simple — no FM, no
//   phase orchestration, just `[LabelingResult]` → assembly result.

import Foundation
import OSLog

// MARK: - ChapterPlanAssembler

/// Plan-level policy for converting a list of per-chapter `LabelingResult`
/// rows into a `ChapterPlan` (or an abort signal).
struct ChapterPlanAssembler: Sendable {

    // MARK: Constants

    /// Strict threshold: if `operational / total > this`, ABORT and
    /// return no plan. `0.30` per bead spec. Intentionally strict (`>`,
    /// not `>=`) — exactly 30% does NOT abort.
    static let operationalUnclearRateThreshold: Double = 0.30

    /// Strict threshold: if `(operational + semantic) / total > this`,
    /// flag a high-unclear-rate warning on the assembled plan. `0.50`
    /// per bead spec. Strict (`>`, not `>=`) — exactly 50% does NOT
    /// trip the warning.
    static let highUnclearRateWarningThreshold: Double = 0.50

    // MARK: Dependencies

    private let logger: Logger

    init(
        logger: Logger = Logger(subsystem: "com.playhead", category: "ChapterPlanAssembler")
    ) {
        self.logger = logger
    }

    // MARK: - Result types

    /// Information the caller needs to emit the
    /// `chapter_phase_operational_unclear_rate_exceeded` diagnostic.
    /// Carries the same numeric fields as
    /// `ChapterPhasePayload.OperationalUnclearRateExceeded` so the
    /// caller can plug them straight into the event factory.
    struct AbortInfo: Sendable, Equatable {
        let labeledCount: Int
        let operationalUnclearCount: Int
        /// Fraction in `[0, 1]`.
        let operationalUnclearRate: Double
        /// Threshold value that was used (always
        /// `operationalUnclearRateThreshold` at the time of the call).
        let threshold: Double
    }

    /// Information the caller may use to emit the high-unclear
    /// warning diagnostic. `highUnclearRateExceeded == true` means the
    /// caller should emit `chapter_phase_high_unclear_rate`. The plan
    /// is still assembled either way.
    struct AssemblyWarnings: Sendable, Equatable {
        let labeledCount: Int
        let operationalUnclearCount: Int
        let semanticUnclearCount: Int
        let totalUnclearCount: Int
        /// `(operational + semantic) / total`. `0` when total is zero.
        let totalUnclearRate: Double
        let highUnclearRateExceeded: Bool
        let threshold: Double
    }

    /// Outcome of `assemble(...)`. Either a usable plan + warnings, or
    /// an abort signal that the caller surfaces as the
    /// `chapter_phase_operational_unclear_rate_exceeded` diagnostic.
    enum AssemblyResult: Sendable {
        case assembled(plan: ChapterPlan, warnings: AssemblyWarnings)
        case aborted(AbortInfo)
    }

    // MARK: - Entry point

    /// Project a list of per-chapter labeling results into a
    /// `ChapterPlan` or an abort signal.
    ///
    /// - Parameters:
    ///   - results: Per-chapter labeling outcomes from
    ///     `ChapterLabelingService.label(...)`. Order is preserved into
    ///     the assembled plan (operational rows are filtered out, but
    ///     the relative order of the surviving rows matches the input).
    ///   - episodeContentHash: Stable identity for the episode, used
    ///     as the plan's cache key. Empty input does not assemble an
    ///     abort, but the caller may still choose not to write an
    ///     empty plan to cache.
    ///   - candidatesDetected: Total candidate boundaries the boundary
    ///     detector emitted before any cap was applied. Recorded in
    ///     the plan's `generationDiagnostics`.
    ///   - candidatesKept: Candidates that survived the cap and went
    ///     into FM labeling. Recorded in the plan's
    ///     `generationDiagnostics`.
    ///   - generatedAt: Timestamp recorded in the assembled plan.
    /// - Returns: `.assembled` with a populated `ChapterPlan` plus
    ///   warnings, or `.aborted` carrying the numbers needed to emit
    ///   the operational-unclear-rate diagnostic. Empty input returns
    ///   `.assembled` with an empty chapter list and zero confidence
    ///   (we never abort on zero-input — there is nothing to be
    ///   distrustful of).
    func assemble(
        results: [LabelingResult],
        episodeContentHash: String,
        candidatesDetected: Int,
        candidatesKept: Int,
        generatedAt: Date
    ) -> AssemblyResult {
        let total = results.count
        let operationalCount = results.reduce(0) { acc, r in
            acc + (r.failureMode == .operational ? 1 : 0)
        }
        let semanticCount = results.reduce(0) { acc, r in
            acc + (r.failureMode == .semantic ? 1 : 0)
        }

        // Operational-rate gate. Strict `>` — exactly 30% does NOT
        // abort. Empty input never aborts: `total == 0` makes the
        // rate computation moot and we return an empty assembled
        // plan instead.
        if total > 0 {
            let operationalRate = Double(operationalCount) / Double(total)
            if operationalRate > Self.operationalUnclearRateThreshold {
                logger.notice(
                    "chapterplan.assemble.aborted operational=\(operationalCount, privacy: .public) total=\(total, privacy: .public) rate=\(operationalRate, privacy: .public) threshold=\(Self.operationalUnclearRateThreshold, privacy: .public)"
                )
                return .aborted(
                    AbortInfo(
                        labeledCount: total,
                        operationalUnclearCount: operationalCount,
                        operationalUnclearRate: operationalRate,
                        threshold: Self.operationalUnclearRateThreshold
                    )
                )
            }
        }

        // Drop operational rows; keep semantic + confident in their
        // original relative order. `ChapterEvidence` is positional —
        // there is no explicit index field — so removing rows is the
        // entire "re-index" operation.
        let keptChapters = results
            .filter { $0.failureMode != .operational }
            .map { $0.chapter }

        let planConfidence = ChapterPlan.computePlanConfidence(keptChapters)

        let diagnostics = ChapterPlanDiagnostics(
            candidatesDetected: candidatesDetected,
            candidatesKept: candidatesKept,
            operationalUnclearCount: operationalCount,
            semanticUnclearCount: semanticCount
        )

        let plan = ChapterPlan(
            episodeContentHash: episodeContentHash,
            chapters: keptChapters,
            planConfidence: planConfidence,
            generatedAt: generatedAt,
            generationDiagnostics: diagnostics
        )

        // High-unclear warning. Same strict-`>` rule. Empty input
        // produces a zero rate (no warning).
        let totalUnclear = operationalCount + semanticCount
        let totalUnclearRate: Double
        if total > 0 {
            totalUnclearRate = Double(totalUnclear) / Double(total)
        } else {
            totalUnclearRate = 0.0
        }
        let highWarn = totalUnclearRate > Self.highUnclearRateWarningThreshold

        if highWarn {
            logger.notice(
                "chapterplan.assemble.high_unclear_warning operational=\(operationalCount, privacy: .public) semantic=\(semanticCount, privacy: .public) total=\(total, privacy: .public) rate=\(totalUnclearRate, privacy: .public) threshold=\(Self.highUnclearRateWarningThreshold, privacy: .public)"
            )
        }

        let warnings = AssemblyWarnings(
            labeledCount: total,
            operationalUnclearCount: operationalCount,
            semanticUnclearCount: semanticCount,
            totalUnclearCount: totalUnclear,
            totalUnclearRate: totalUnclearRate,
            highUnclearRateExceeded: highWarn,
            threshold: Self.highUnclearRateWarningThreshold
        )

        return .assembled(plan: plan, warnings: warnings)
    }
}
