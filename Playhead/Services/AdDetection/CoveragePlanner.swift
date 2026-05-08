// CoveragePlanner.swift
// Selects the phase-3 FM backfill scan policy for a show before scanning.
//
// playhead-au2v.1.14: extended with chapter-evidence-informed audit
// window selection. When chapter evidence is available and the chapter
// signal mode is `.enabled`, the planner replaces a configurable fraction
// (default 0.5) of the random audit slots with chapter-informed
// selections (ad-disposition chapters force-included, high-quality
// content-disposition chapters excluded). Evidence is supplied per-call
// via `CoveragePlannerContext.chapterEvidence`, so the same code path
// serves creator-supplied chapters (id3/pc20/rssInline) and inferred
// chapters (the on-device chapter generation phase) — the
// `qualityScore` carries the trust signal.

import Foundation

enum CoveragePolicy: String, Codable, Sendable, Hashable, CaseIterable {
    case fullCoverage
    case targetedWithAudit
    case periodicFullRescan
}

/// Closed time interval (seconds, episode-relative) recommended by the
/// planner as a target for an audit-window scan slot. Carries the
/// originating chapter's `qualityScore` so downstream consumers can
/// weight the recommendation if they wish.
///
/// `kind` describes why the interval was selected: `.adChapter` means an
/// ad-disposition chapter that should ALWAYS be audited; `.contentExcluded`
/// is recorded for telemetry symmetry only — excluded intervals are NOT
/// returned in `ChapterInformedAuditSelection.includes` (their omission
/// is the signal). Reserved for a future bead that wants the full picture.
struct ChapterAuditInterval: Sendable, Equatable {
    enum Kind: String, Sendable, Hashable, Equatable {
        case adChapter
        case contentExcluded
    }

    let startTime: TimeInterval
    let endTime: TimeInterval
    let kind: Kind
    let qualityScore: Float
}

/// Chapter-informed audit-window guidance produced by `CoveragePlanner`
/// when the chapter signal mode is `.enabled` and usable chapter
/// evidence is supplied via `CoveragePlannerContext.chapterEvidence`.
///
/// `includes` are intervals the planner recommends auditing. `excludes`
/// are intervals (high-quality content-disposition chapters) the planner
/// recommends NOT auditing unless the boundary borders an unscanned
/// region — the border condition is the consumer's responsibility (the
/// planner does not know the ambient scan plan), so the consumer
/// inspects `excludes` against its own coverage map.
///
/// `replacementFraction` is the configured fraction of random audit
/// slots that chapter-informed selections MAY replace. The planner
/// EMITS the guidance + the budget; the audit-window narrower is the
/// component that allocates actual slots. If `includes.count` exceeds
/// `replacementFraction * total_audit_slots`, the consumer is expected
/// to truncate; if fewer chapter-informed candidates exist than the
/// budget would allow, the excess slots remain random. Wiring into the
/// narrower (`TargetedWindowNarrower.auditSegments`) is a later bead.
///
/// Semantics of the budget (clarification): `replacementFraction`
/// budgets the ADDITIONS — chapter `includes` that may be promoted
/// into the audit-slot set. `excludes` are SUBTRACTIONS (high-quality
/// content regions the consumer should not audit unless a coverage-
/// border condition forces it) and are NOT counted against the
/// budget; they are a separate signal to the consumer.
///
/// `evidenceCount` and `planConfidence` are the inputs the planner used
/// to make the decision (recorded so audits can be reproduced from the
/// emitted plan alone).
///
/// Order: `includes` and `excludes` preserve the iteration order of the
/// input `chapterEvidence` array. Consumers MUST NOT rely on any other
/// ordering; if a stable sort is needed downstream the consumer should
/// sort defensively (e.g. by `startTime`).
struct ChapterInformedAuditSelection: Sendable, Equatable {
    let includes: [ChapterAuditInterval]
    let excludes: [ChapterAuditInterval]
    let replacementFraction: Double
    let evidenceCount: Int
    let planConfidence: Double
}

/// Closed-vocabulary reason the planner did NOT consume chapter
/// evidence. Surfaced as the snake_case `reason` field on the
/// `coverage_plan_chapter_skipped` diagnostic event.
enum ChapterAuditSkipReason: String, Sendable, Hashable, Equatable {
    /// `chapterSignalMode` was `.off` or `.shadow` — consumers do not
    /// read the plan in those modes (`consumersReadChapterPlan == false`).
    case modeDisabled = "mode_disabled"
    /// `chapterEvidence` was `nil` or empty.
    case noChapterEvidence = "no_chapter_evidence"
    /// Every supplied chapter was ambiguous, dropped (corrupt bounds /
    /// NaN quality), or failed its quality gate.
    case noUsableChapters = "no_usable_chapters"
    /// Usable chapters survived the filters but the duration-weighted
    /// plan confidence was below `minPlanConfidence`.
    case lowPlanConfidence = "low_plan_confidence"
}

/// Discriminated diagnostic-summary record describing what the planner
/// did with chapter evidence on a `targetedWithAudit` plan. Returned as
/// `CoveragePlan.chapterAuditDiagnostic` so callers (which DO know the
/// installID + episodeId) can convert the summary to a
/// `ChapterPhaseEvent` via `CoveragePlanner.event(for:)`.
///
/// Only emitted on `targetedWithAudit` plans. Other policies
/// (`fullCoverage`, `periodicFullRescan`) do not run the audit-window
/// selection step at all, so emitting an `…_chapter_skipped` event for
/// a `fullCoverage` plan would be noise rather than signal.
enum ChapterAuditDiagnostic: Sendable, Equatable {
    case informed(ChapterInformedAuditSelection)
    case skipped(reason: ChapterAuditSkipReason, evidenceCount: Int)
}

struct CoveragePlannerContext: Sendable, Equatable {
    let observedEpisodeCount: Int
    /// Cycle 2 C4: this flag is **stable recall**, not precision — the
    /// metric was historically misnamed. The store column / row JSON
    /// keys still use the legacy `stablePrecisionFlag` name to avoid a
    /// migration; the rename is code-only. See the documentation on
    /// `TargetedWindowNarrower.recallSample` for the formula direction.
    let stableRecall: Bool
    let isFirstEpisodeAfterCohortInvalidation: Bool
    let recallDegrading: Bool
    let sponsorDriftDetected: Bool
    let auditMissDetected: Bool
    let episodesSinceLastFullRescan: Int
    let periodicFullRescanIntervalEpisodes: Int

    /// playhead-au2v.1.14: tri-state gate that controls whether the
    /// planner reads chapter evidence. Defaults to `.off` for safety
    /// (no behaviour change on call sites that don't yet plumb the
    /// flag through). The planner's chapter-informed code path only
    /// fires when this is `.enabled` AND `chapterEvidence` is non-nil
    /// AND has usable chapters AND the computed plan confidence meets
    /// the configured threshold. In `.off` and `.shadow` the planner
    /// behaves bit-identically to pre-au2v.1.14 (no chapter
    /// consultation at all).
    let chapterSignalMode: ChapterSignalMode

    /// playhead-au2v.1.14: chapter-marker evidence to consult during
    /// audit-window selection. Nil means "no chapter evidence
    /// available" and is the safe default for callers that haven't
    /// been wired to the chapter generation phase yet (beads 12/13).
    /// When non-nil, the planner may consume it gated on
    /// `chapterSignalMode == .enabled`.
    ///
    /// Source-agnostic: the planner does not branch on
    /// `ChapterSource`. Creator chapters (`.id3`, `.pc20`,
    /// `.rssInline`) and inferred chapters (`.inferred`) both flow
    /// through the same code path; the per-chapter `qualityScore` is
    /// the trust signal.
    let chapterEvidence: [ChapterEvidence]?

    init(
        observedEpisodeCount: Int,
        stableRecall: Bool,
        isFirstEpisodeAfterCohortInvalidation: Bool,
        recallDegrading: Bool,
        sponsorDriftDetected: Bool,
        auditMissDetected: Bool,
        episodesSinceLastFullRescan: Int,
        periodicFullRescanIntervalEpisodes: Int,
        chapterSignalMode: ChapterSignalMode = .off,
        chapterEvidence: [ChapterEvidence]? = nil
    ) {
        self.observedEpisodeCount = observedEpisodeCount
        self.stableRecall = stableRecall
        self.isFirstEpisodeAfterCohortInvalidation = isFirstEpisodeAfterCohortInvalidation
        self.recallDegrading = recallDegrading
        self.sponsorDriftDetected = sponsorDriftDetected
        self.auditMissDetected = auditMissDetected
        self.episodesSinceLastFullRescan = episodesSinceLastFullRescan
        self.periodicFullRescanIntervalEpisodes = periodicFullRescanIntervalEpisodes
        self.chapterSignalMode = chapterSignalMode
        self.chapterEvidence = chapterEvidence
    }
}

struct CoveragePlan: Sendable, Equatable {
    let policy: CoveragePolicy
    let phases: [BackfillJobPhase]
    let auditWindowSampleRate: Double?

    /// playhead-au2v.1.14: chapter-informed audit-window guidance.
    /// Non-nil only on `targetedWithAudit` plans where the planner
    /// successfully consulted chapter evidence. Nil means the
    /// downstream consumer should fall back to today's
    /// random-audit-window selection (no behaviour change vs.
    /// pre-au2v.1.14).
    let chapterInformedAudit: ChapterInformedAuditSelection?

    /// playhead-au2v.1.14: discriminated diagnostic summary describing
    /// what the planner did with chapter evidence on a
    /// `targetedWithAudit` plan. `nil` on `fullCoverage` /
    /// `periodicFullRescan` plans (audit selection does not run there)
    /// and on `targetedWithAudit` plans pre-dating the chapter signal
    /// (no `chapterSignalMode` / `chapterEvidence` plumbing).
    ///
    /// Convert to a `ChapterPhaseEvent` via
    /// `CoveragePlanner.event(for:installID:episodeId:timestamp:)`
    /// when the caller has the install/episode ids in hand. The
    /// projection helper is the only sanctioned emit path: the
    /// planner is a `Sendable` value type and intentionally has no
    /// access to install/episode ids.
    let chapterAuditDiagnostic: ChapterAuditDiagnostic?

    init(
        policy: CoveragePolicy,
        phases: [BackfillJobPhase],
        auditWindowSampleRate: Double?,
        chapterInformedAudit: ChapterInformedAuditSelection? = nil,
        chapterAuditDiagnostic: ChapterAuditDiagnostic? = nil
    ) {
        self.policy = policy
        self.phases = phases
        self.auditWindowSampleRate = auditWindowSampleRate
        self.chapterInformedAudit = chapterInformedAudit
        self.chapterAuditDiagnostic = chapterAuditDiagnostic
    }
}

struct CoveragePlanner: Sendable {
    static let defaultColdStartEpisodeThreshold = 5
    static let defaultPeriodicFullRescanIntervalEpisodes = 10
    static let defaultAuditWindowSampleRate = 0.12

    // playhead-au2v.1.14 tunables. The defaults are the values the bead
    // spec calls out; overrides exist only so the shadow-mode A/B can
    // sweep them. Any change to these defaults is a behaviour change
    // and must come with a refreshed eval run.

    /// Default fraction of random audit slots that may be replaced by
    /// chapter-informed selections when chapter evidence is available
    /// and the gate is `.enabled`. The actual replacement honours the
    /// supply of usable ad-disposition chapters; if there are fewer
    /// chapter-informed candidates than the fraction allows, the
    /// excess slots remain random.
    static let defaultReplacementFraction = 0.5

    /// Default minimum `qualityScore` for an ad-disposition chapter
    /// to ALWAYS be included in the audit set.
    static let defaultAdChapterMinQualityForAuditInclusion: Float = 0.4

    /// Default minimum `qualityScore` for a content-disposition chapter
    /// to be excluded from the audit set (subject to the consumer's
    /// border-condition check).
    static let defaultContentChapterMinQualityForExclusion: Float = 0.7

    /// Default minimum `planConfidence` (duration-weighted aggregate
    /// across the supplied chapters) for the chapter-informed code
    /// path to fire. Below this floor the planner falls back to
    /// today's random selection — the chapter signal is too weak to
    /// trust as audit guidance.
    static let defaultMinPlanConfidence = 0.3

    let coldStartEpisodeThreshold: Int
    let periodicFullRescanIntervalEpisodes: Int
    let auditWindowSampleRate: Double

    let replacementFraction: Double
    let adChapterMinQualityForAuditInclusion: Float
    let contentChapterMinQualityForExclusion: Float
    let minPlanConfidence: Double

    /// Constructs a planner.
    ///
    /// - Parameters:
    ///   - auditWindowSampleRate: Fraction of episode duration that
    ///     `targetedWithAudit` plans dedicate to random audit windows.
    ///     The value is **hard-clamped** to the inclusive range
    ///     `[0.10, 0.15]`. The lower bound preserves enough audit
    ///     coverage to detect harvester drift; the upper bound caps FM
    ///     token spend per episode (~15% audit, ~85% targeted). Values
    ///     outside this range are silently clamped — callers should
    ///     treat anything outside `[0.10, 0.15]` as a configuration
    ///     bug.
    ///   - replacementFraction: Fraction of the configured audit slots
    ///     that the chapter-informed code path may replace with
    ///     chapter-derived intervals. Clamped to `[0, 1]`.
    ///   - adChapterMinQualityForAuditInclusion: Minimum `qualityScore`
    ///     for an ad-disposition chapter to force inclusion. Clamped
    ///     to `[0, 1]`.
    ///   - contentChapterMinQualityForExclusion: Minimum
    ///     `qualityScore` for a content-disposition chapter to be
    ///     excluded. Clamped to `[0, 1]`.
    ///   - minPlanConfidence: Floor on the duration-weighted plan
    ///     confidence (computed via `ChapterPlan.computePlanConfidence`)
    ///     below which the chapter-informed path falls back to random
    ///     selection. Clamped to `[0, 1]`.
    init(
        coldStartEpisodeThreshold: Int = CoveragePlanner.defaultColdStartEpisodeThreshold,
        periodicFullRescanIntervalEpisodes: Int = CoveragePlanner.defaultPeriodicFullRescanIntervalEpisodes,
        auditWindowSampleRate: Double = CoveragePlanner.defaultAuditWindowSampleRate,
        replacementFraction: Double = CoveragePlanner.defaultReplacementFraction,
        adChapterMinQualityForAuditInclusion: Float
            = CoveragePlanner.defaultAdChapterMinQualityForAuditInclusion,
        contentChapterMinQualityForExclusion: Float
            = CoveragePlanner.defaultContentChapterMinQualityForExclusion,
        minPlanConfidence: Double = CoveragePlanner.defaultMinPlanConfidence
    ) {
        self.coldStartEpisodeThreshold = coldStartEpisodeThreshold
        self.periodicFullRescanIntervalEpisodes = periodicFullRescanIntervalEpisodes
        self.auditWindowSampleRate = min(max(auditWindowSampleRate, 0.10), 0.15)
        self.replacementFraction = min(max(replacementFraction, 0.0), 1.0)
        self.adChapterMinQualityForAuditInclusion
            = min(max(adChapterMinQualityForAuditInclusion, 0.0), 1.0)
        self.contentChapterMinQualityForExclusion
            = min(max(contentChapterMinQualityForExclusion, 0.0), 1.0)
        self.minPlanConfidence = min(max(minPlanConfidence, 0.0), 1.0)
    }

    /// Acknowledges that a `periodicFullRescan` (or any full coverage scan)
    /// has been executed by returning a context with
    /// `episodesSinceLastFullRescan` zeroed.
    ///
    /// **Caller contract:** after executing a full rescan plan returned by
    /// ``plan(for:)``, the caller MUST record the rescan via this method
    /// before planning the next episode. Without it the planner will keep
    /// emitting `periodicFullRescan` on every subsequent call once the
    /// threshold has been crossed, deadlocking the policy loop.
    func reset(context: CoveragePlannerContext) -> CoveragePlannerContext {
        CoveragePlannerContext(
            observedEpisodeCount: context.observedEpisodeCount,
            stableRecall: context.stableRecall,
            isFirstEpisodeAfterCohortInvalidation: context.isFirstEpisodeAfterCohortInvalidation,
            recallDegrading: context.recallDegrading,
            sponsorDriftDetected: context.sponsorDriftDetected,
            auditMissDetected: context.auditMissDetected,
            episodesSinceLastFullRescan: 0,
            periodicFullRescanIntervalEpisodes: context.periodicFullRescanIntervalEpisodes,
            chapterSignalMode: context.chapterSignalMode,
            chapterEvidence: context.chapterEvidence
        )
    }

    func plan(for context: CoveragePlannerContext) -> CoveragePlan {
        if shouldUseFullCoverage(context) {
            return CoveragePlan(
                policy: .fullCoverage,
                phases: [.fullEpisodeScan],
                auditWindowSampleRate: nil
            )
        }

        if shouldUsePeriodicFullRescan(context) {
            return CoveragePlan(
                policy: .periodicFullRescan,
                phases: [.fullEpisodeScan],
                auditWindowSampleRate: nil
            )
        }

        if context.observedEpisodeCount >= coldStartEpisodeThreshold && context.stableRecall {
            let diagnostic = chapterAuditDiagnostic(for: context)
            let informed: ChapterInformedAuditSelection?
            switch diagnostic {
            case .informed(let selection): informed = selection
            case .skipped:                 informed = nil
            }
            return CoveragePlan(
                policy: .targetedWithAudit,
                phases: [
                    .scanHarvesterProposals,
                    .scanLikelyAdSlots,
                    .scanRandomAuditWindows,
                ],
                auditWindowSampleRate: auditWindowSampleRate,
                chapterInformedAudit: informed,
                chapterAuditDiagnostic: diagnostic
            )
        }

        return CoveragePlan(
            policy: .fullCoverage,
            phases: [.fullEpisodeScan],
            auditWindowSampleRate: nil
        )
    }

    private func shouldUseFullCoverage(_ context: CoveragePlannerContext) -> Bool {
        context.observedEpisodeCount < coldStartEpisodeThreshold ||
        context.isFirstEpisodeAfterCohortInvalidation ||
        context.recallDegrading ||
        context.auditMissDetected
    }

    private func shouldUsePeriodicFullRescan(_ context: CoveragePlannerContext) -> Bool {
        context.sponsorDriftDetected ||
        context.episodesSinceLastFullRescan >= max(
            1,
            context.periodicFullRescanIntervalEpisodes > 0
                ? context.periodicFullRescanIntervalEpisodes
                : periodicFullRescanIntervalEpisodes
        )
    }

    // MARK: - Chapter-informed audit-window selection (au2v.1.14)

    /// Compute the discriminated chapter-audit diagnostic for a
    /// `targetedWithAudit` plan. Returns `.informed(...)` when the
    /// planner has consulted chapter evidence successfully, or
    /// `.skipped(...)` (with the snake_case-friendly reason) when one
    /// of the four documented edge cases fires (mode disabled, no
    /// evidence, no usable chapters, low plan confidence). All four
    /// `.skipped` paths produce IDENTICAL plan output to pre-au2v.1.14
    /// and are covered by `CoveragePlannerTests`.
    private func chapterAuditDiagnostic(
        for context: CoveragePlannerContext
    ) -> ChapterAuditDiagnostic {
        // Edge case 1: mode disabled (.off or .shadow) — never consult.
        // .shadow runs the chapter generation phase but consumers do
        // not read; this matches `ChapterSignalMode.consumersReadChapterPlan`.
        guard context.chapterSignalMode.consumersReadChapterPlan else {
            return .skipped(
                reason: .modeDisabled,
                evidenceCount: context.chapterEvidence?.count ?? 0
            )
        }

        // Edge case 2: no chapter evidence available.
        guard let chapters = context.chapterEvidence, !chapters.isEmpty else {
            return .skipped(reason: .noChapterEvidence, evidenceCount: 0)
        }

        // Filter the candidate chapters into the two pools we'll act on.
        // Ambiguous chapters intentionally fall through to "normal random
        // probability" — i.e. we neither force-include nor force-exclude
        // them. That is implemented by simply omitting them from BOTH
        // `includes` and `excludes`: the consumer's ambient random
        // selection will treat their region as un-classified.
        var includes: [ChapterAuditInterval] = []
        var excludes: [ChapterAuditInterval] = []
        for chapter in chapters {
            // Skip chapters with corrupt time bounds — same defensive
            // posture as `ChapterPlan.computePlanConfidence`.
            guard chapter.startTime.isFinite else { continue }
            let resolvedEnd = effectiveEndTime(for: chapter)
            guard resolvedEnd > chapter.startTime, resolvedEnd.isFinite else { continue }
            // Skip chapters with corrupt quality so a single NaN cannot
            // poison threshold comparisons.
            guard chapter.qualityScore.isFinite else { continue }

            switch chapter.disposition {
            case .adBreak:
                if chapter.qualityScore > adChapterMinQualityForAuditInclusion {
                    includes.append(ChapterAuditInterval(
                        startTime: chapter.startTime,
                        endTime: resolvedEnd,
                        kind: .adChapter,
                        qualityScore: chapter.qualityScore
                    ))
                }
            case .content:
                if chapter.qualityScore > contentChapterMinQualityForExclusion {
                    excludes.append(ChapterAuditInterval(
                        startTime: chapter.startTime,
                        endTime: resolvedEnd,
                        kind: .contentExcluded,
                        qualityScore: chapter.qualityScore
                    ))
                }
            case .ambiguous:
                continue
            }
        }

        // Edge case 3: only ambiguous chapters survived the filters
        // (or every disposition'd chapter failed its quality gate).
        // Nothing to act on — fall back to random.
        guard !includes.isEmpty || !excludes.isEmpty else {
            return .skipped(reason: .noUsableChapters, evidenceCount: chapters.count)
        }

        // Edge case 4: weak plan confidence. Compute it from the
        // supplied evidence (using the same duration-weighted formula
        // as `ChapterPlan.computePlanConfidence` so the two stay in
        // lock-step). Below the floor we fall back to random.
        let planConfidence = ChapterPlan.computePlanConfidence(chapters)
        guard planConfidence >= minPlanConfidence else {
            return .skipped(reason: .lowPlanConfidence, evidenceCount: chapters.count)
        }

        return .informed(
            ChapterInformedAuditSelection(
                includes: includes,
                excludes: excludes,
                replacementFraction: replacementFraction,
                evidenceCount: chapters.count,
                planConfidence: planConfidence
            )
        )
    }

    /// Resolve a chapter's effective end time for interval purposes.
    /// Mirrors `ChapterPlan.effectiveDuration` semantics so callers can
    /// reason about a single rule: missing `endTime` → 60 s nominal,
    /// non-finite → drop, malformed (`<= startTime`) → drop.
    private func effectiveEndTime(for chapter: ChapterEvidence) -> TimeInterval {
        guard let end = chapter.endTime else {
            return chapter.startTime + 60.0
        }
        return end
    }

    // MARK: - Diagnostic event projection

    /// Convert the chapter-audit diagnostic on a plan into a
    /// `ChapterPhaseEvent` ready to drop into the diagnostics bundle.
    /// Returns `nil` when the plan carries no diagnostic (e.g.
    /// non-targeted plans) so call sites can write
    /// `if let event = CoveragePlanner.event(for: plan, …) { events.append(event) }`.
    static func event(
        for plan: CoveragePlan,
        installID: UUID,
        episodeId: String,
        timestamp: Double
    ) -> ChapterPhaseEvent? {
        guard let diagnostic = plan.chapterAuditDiagnostic else { return nil }
        switch diagnostic {
        case .informed(let selection):
            return .coveragePlanChapterInformed(
                installID: installID,
                episodeId: episodeId,
                timestamp: timestamp,
                fractionReplaced: selection.replacementFraction,
                adChapterIncludedCount: selection.includes.count,
                contentChapterExcludedCount: selection.excludes.count,
                planConfidence: selection.planConfidence
            )
        case .skipped(let reason, let evidenceCount):
            return .coveragePlanChapterSkipped(
                installID: installID,
                episodeId: episodeId,
                timestamp: timestamp,
                reason: reason.rawValue,
                evidenceCount: evidenceCount
            )
        }
    }
}
