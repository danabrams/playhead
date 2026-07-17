// BroadCorrectionEvaluator.swift
// ef2.6.1: Broader learned correction scopes (Layer B).
//
// Evaluates whether repeated user corrections at a given scope should be
// promoted into a persistent rule. Each scope has thresholds for:
//   - Required weighted correction count (explicit=1.0, implicit=0.3)
//   - Required distinct episodes
//   - Required distinct calendar dates (for some scopes)
//   - Decay window (corrections older than N days are excluded)
//
// Layer A (exactSpan) vetoes are permanent and never decay — they are
// handled by the existing UserCorrectionStore and are not part of this
// evaluator.

import Foundation

// MARK: - BroadCorrectionScope

/// The four Layer B scope levels with their promotion thresholds and decay.
enum BroadCorrectionScope: Sendable, Equatable, CaseIterable {
    case phraseOnShow
    case sponsorOnShow
    case domainOwnershipOnShow
    case jingleOnShow

    /// Minimum weighted correction count to promote.
    /// Explicit corrections contribute 1.0; implicit contribute 0.3.
    var requiredCorrectionCount: Int {
        switch self {
        case .phraseOnShow:            return 3
        case .sponsorOnShow:           return 2
        case .domainOwnershipOnShow:   return 2
        case .jingleOnShow:            return 3
        }
    }

    /// Minimum number of distinct episodes the corrections must span.
    var requiredDistinctEpisodes: Int {
        switch self {
        case .phraseOnShow:            return 2
        case .sponsorOnShow:           return 2
        case .domainOwnershipOnShow:   return 2
        case .jingleOnShow:            return 2
        }
    }

    /// Minimum number of distinct calendar dates (UTC) the corrections must
    /// span, or nil if no date diversity is required.
    var requiredDistinctDates: Int? {
        switch self {
        case .phraseOnShow:            return 2
        case .sponsorOnShow:           return nil
        case .domainOwnershipOnShow:   return nil
        case .jingleOnShow:            return 2
        }
    }

    /// Corrections older than this many days are excluded from evaluation.
    var decayDays: Int {
        switch self {
        case .phraseOnShow:            return 120
        case .sponsorOnShow:           return 180
        case .domainOwnershipOnShow:   return 360
        case .jingleOnShow:            return 90
        }
    }
}

// MARK: - CorrectionFeedbackKind

/// Whether a correction was an explicit user gesture or implicit behavioral signal.
enum CorrectionFeedbackKind: Sendable, Equatable {
    /// User explicitly corrected (tapped "not an ad", listen revert, etc.)
    case explicit
    /// Inferred from user behavior (e.g. listening through a skipped segment)
    case implicit

    /// Weight applied to this feedback when computing the weighted correction count.
    var weight: Double {
        switch self {
        case .explicit: return 1.0
        case .implicit: return 0.3
        }
    }
}

// MARK: - CorrectionDirection

/// Whether a correction argues to SUPPRESS detection ("not an ad") or BOOST it
/// ("missed an ad"). Derived from `CorrectionKind` — `.falsePositive` →
/// `.suppress`, `.falseNegative` → `.boost`.
///
/// playhead-xsdz.34 (guardrail 1): Layer-B ACT-ALONE authority is granted ONLY
/// in the `.suppress` direction. A `.boost` entry NEVER acts alone — its worst
/// case is a false skip eating content, the outcome the never-act-alone design
/// exists to prevent.
enum CorrectionDirection: Sendable, Equatable {
    case suppress
    case boost
}

// MARK: - CorrectionLedgerEntry

/// A single correction record with resolved metadata for diversity checks.
///
/// This is a pre-resolved view of a `CorrectionEvent` enriched with the
/// episode ID (resolved from the analysis asset) and the feedback kind.
struct CorrectionLedgerEntry: Sendable {
    /// The episode this correction came from.
    let episodeId: String
    /// When the correction was made.
    let correctionDate: Date
    /// Whether this is an explicit correction or implicit feedback.
    let feedbackKind: CorrectionFeedbackKind
    /// playhead-xsdz.34: suppress vs boost. Defaults to `.suppress` — the
    /// existing Layer-B scopes (phrase/sponsor/domain/jingle) are all
    /// suppression-direction, so pre-xsdz.34 call sites are semantically
    /// unchanged.
    let direction: CorrectionDirection

    init(
        episodeId: String,
        correctionDate: Date,
        feedbackKind: CorrectionFeedbackKind,
        direction: CorrectionDirection = .suppress
    ) {
        self.episodeId = episodeId
        self.correctionDate = correctionDate
        self.feedbackKind = feedbackKind
        self.direction = direction
    }
}

// MARK: - BroadCorrectionEvaluator

/// Pure-value evaluator for Layer B correction promotion.
///
/// Given a set of correction ledger entries and a scope, determines whether
/// the promotion threshold is met. All state is passed in — this type has
/// no side effects and no persistence dependency.
enum BroadCorrectionEvaluator {

    /// Evaluate whether the given entries satisfy the promotion criteria for the scope.
    ///
    /// Steps:
    /// 1. Filter out entries older than the scope's decay window.
    /// 2. Check weighted correction count meets threshold.
    /// 3. Check episode diversity meets threshold.
    /// 4. Check date diversity meets threshold (if required).
    ///
    /// - Parameters:
    ///   - scope: The Layer B scope to evaluate.
    ///   - entries: Pre-resolved correction ledger entries for this scope.
    ///   - referenceDate: The "now" date for decay calculations (defaults to current time).
    /// - Returns: `true` if the scope should be promoted into a persistent rule.
    static func shouldPromote(
        scope: BroadCorrectionScope,
        entries: [CorrectionLedgerEntry],
        referenceDate: Date = Date()
    ) -> Bool {
        // 1. Decay filter: exclude entries older than the scope's decay window.
        let decayCutoff = referenceDate.addingTimeInterval(-Double(scope.decayDays) * 86400)
        let active = entries.filter { $0.correctionDate >= decayCutoff }

        guard !active.isEmpty else { return false }

        // 2. Weighted correction count.
        // Use a small epsilon to guard against floating-point accumulation
        // (e.g. 10 × 0.3 = 2.9999…97 must still satisfy a threshold of 3).
        let weightedCount = active.reduce(0.0) { $0 + $1.feedbackKind.weight }
        guard weightedCount >= Double(scope.requiredCorrectionCount) - 1e-9 else { return false }

        // 3. Episode diversity.
        let distinctEpisodes = Set(active.map(\.episodeId))
        guard distinctEpisodes.count >= scope.requiredDistinctEpisodes else { return false }

        // 4. Date diversity (if required).
        if let requiredDates = scope.requiredDistinctDates {
            let calendar = Calendar(identifier: .gregorian)
            let utc = TimeZone(identifier: "UTC") ?? .gmt
            let distinctDates = Set(active.map { entry -> DateComponents in
                calendar.dateComponents(in: utc, from: entry.correctionDate)
            }.map { components -> String in
                // Use year-month-day as the calendar day key.
                "\(components.year ?? 0)-\(components.month ?? 0)-\(components.day ?? 0)"
            })
            guard distinctDates.count >= requiredDates else { return false }
        }

        return true
    }

    /// playhead-xsdz.34 (Part 3 / design §4, Dan-INCLUDED): whether a SINGLE
    /// explicit `.falsePositive` (suppress-direction) veto, ON ITS OWN, promotes
    /// a Layer-B SUPPRESSION rule for `scope` — the deliberate overturn of the
    /// "0.3× weak-labels-never-act-alone" corroboration requirement.
    ///
    /// This is a SEPARATE, explicitly-tested behavior — NOT an implicit side
    /// effect of `shouldPromote` (which keeps its full ≥count / diversity
    /// corroboration path untouched, so every prior test holds). Callers choose
    /// this path deliberately for a one-tap show-wide suppression.
    ///
    /// GUARDRAILS (design §8 — a violation of any is a shipped regression):
    ///   • Act-alone authority is granted ONLY to `.explicit` feedback in the
    ///     `.suppress` direction. A `.boost` (`.falseNegative`) entry NEVER acts
    ///     alone here; an `.implicit` (0.3×) entry NEVER acts alone here. Both
    ///     fall through to the corroboration-based `shouldPromote`.
    ///   • Suppress-only ⇒ the worst case is a MISSED skip (safe), never a false
    ///     skip eating content. No auto-skip is ever CREATED by this path.
    ///
    /// Decayed entries are excluded first, using the same per-scope decay window
    /// as `shouldPromote`.
    static func shouldActAloneSuppress(
        scope: BroadCorrectionScope,
        entries: [CorrectionLedgerEntry],
        referenceDate: Date = Date()
    ) -> Bool {
        let decayCutoff = referenceDate.addingTimeInterval(-Double(scope.decayDays) * 86400)
        return entries.contains { entry in
            entry.correctionDate >= decayCutoff
                && entry.feedbackKind == .explicit
                && entry.direction == .suppress
        }
    }
}
