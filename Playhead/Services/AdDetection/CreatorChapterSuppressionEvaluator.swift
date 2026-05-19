// CreatorChapterSuppressionEvaluator.swift
// playhead-rxuv: Content-chapter suppression for the AdDetectionService
// fusion path.
//
// The primary value of the creator-chapter bead is false-positive
// reduction: when a candidate ad span lies inside a chapter the
// publisher labeled as editorial content (Interview, Q&A, Discussion,
// News, etc. — see `ChapterDispositionClassifier.contentPatterns`),
// auto-skipping that span would skip part of the actual conversation
// the user wants to hear. This evaluator scans the asset's chapter
// evidence and decides whether a given decoded span should be demoted
// to `.blockedByPolicy` (mark-only at best — not actionable).
//
// Design choices:
//
// 1. Pure value type, no I/O. Like `ChapterMetadataEvidenceBuilder`,
//    `CandidateWindowSelector`, and `DecisionMapper`, the evaluator is
//    deterministic over its inputs; the caller owns when to apply it
//    in the per-span loop.
//
// 2. Only creator-source chapters participate. `.inferred` chapters
//    are out of scope per the rxuv bead spec — the follow-on
//    `playhead-w7oi` bead will wire LLM-inferred chapters. Filtering
//    here (not at the call site) keeps the responsibility colocated.
//
// 3. Soft-overlap requirement. The span must lie *substantially* inside
//    the content chapter (default ≥ 50% of the span's duration) — a
//    short content chapter that grazes the edge of a span shouldn't
//    suppress a long, well-supported ad proposal. The overlap fraction
//    is computed against the span (the thing we're deciding on), not
//    the chapter, so a tiny ad span inside a large content chapter is
//    correctly suppressed even if the chapter's overlap fraction is
//    small.
//
// 4. Quality floor (`0.30`) matches the positive-evidence builder
//    (`ChapterMetadataEvidenceBuilder.qualityFloor`). Untitled or
//    weakly-classified content chapters that wouldn't be trusted to
//    add weight on the recall side are also not trusted to suppress
//    on the precision side.
//
// 5. Scoring is honest. The evaluator never modifies
//    `proposalConfidence` or `skipConfidence` — it only signals the
//    eligibility-gate demotion. The caller threads this through the
//    same post-`DecisionMapper.map()` shape used by
//    `applyFMSuppression`, so the gate downgrade is structurally
//    indistinguishable from other suppression paths.

import Foundation

/// Decides whether a given decoded span should be suppressed because
/// it lies inside a creator-labeled `.content` chapter.
///
/// Stateless: every entry-point is `static`. Lives as an enum
/// namespace to match `CandidateWindowSelector`'s pattern.
enum CreatorChapterSuppressionEvaluator {

    /// Minimum chapter quality score for a `.content` chapter to be
    /// trusted as a suppression signal. Matches
    /// `ChapterMetadataEvidenceBuilder.qualityFloor` (`0.30`) so the
    /// recall and precision sides agree on which chapters are
    /// trustworthy enough to influence fusion.
    static let qualityFloor: Float = 0.30

    /// Minimum fraction of the span's duration that must lie inside
    /// the content chapter for suppression to fire. `0.50` (≥ half)
    /// per the bead's "substantially inside" framing. Strict `>=`
    /// inclusive at the boundary so a span exactly half-covered by a
    /// content chapter does trigger suppression — the alternative
    /// (strict `>`) would create a "coin-flip" edge that depends on
    /// floating-point rounding of acoustic-snapped boundaries.
    static let minSpanOverlapFraction: Double = 0.50

    /// Fallback duration (seconds) used when a content chapter has no
    /// `endTime`. Mirrors the positive-evidence builder's open-ended
    /// chapter rule (`ChapterMetadataEvidenceBuilder.chapterOverlapsSpan`
    /// uses the same 60s fallback). The open-ended-content-chapter case
    /// is genuinely rare — the parser only leaves `endTime == nil` on
    /// the final chapter, and a final chapter spans the rest of the
    /// episode by definition — but keeping the fallback consistent
    /// across the two builders avoids a "ad-chapters use 60s, content
    /// chapters use ∞" surprise.
    static let openEndedFallbackDuration: TimeInterval = 60.0

    // MARK: - Public API

    /// Whether `span` should have its eligibility gate demoted because
    /// it sits inside a creator-supplied `.content` chapter.
    ///
    /// Returns `false` (no suppression) when:
    ///   * `chapters` is empty (graceful no-op for episodes without
    ///     publisher-supplied chapters).
    ///   * No content chapter clears the quality floor.
    ///   * No qualifying content chapter overlaps the span by at least
    ///     `minSpanOverlapFraction` of the span's duration.
    ///   * The span has zero (or negative) duration — overlap fraction
    ///     is undefined, so we conservatively decline to suppress.
    ///
    /// Returns `true` only when at least one creator-source
    /// (`ChapterSource.isCreatorSource == true`) `.content` chapter
    /// above the quality floor covers `>= minSpanOverlapFraction` of
    /// the span.
    static func shouldSuppress(
        span: DecodedSpan,
        chapters: [ChapterEvidence]
    ) -> Bool {
        guard !chapters.isEmpty else { return false }

        let spanDuration = span.endTime - span.startTime
        guard spanDuration > 0 else { return false }

        // Filter to creator-supplied content chapters above the quality
        // floor. Inferred chapters (`source == .inferred`) are excluded —
        // the rxuv bead is scoped to publisher chapters.
        let qualifying = chapters.filter { chapter in
            guard chapter.disposition == .content else { return false }
            guard chapter.source.isCreatorSource else { return false }
            guard chapter.qualityScore >= Self.qualityFloor else { return false }
            return chapterIsWellFormed(chapter)
        }
        guard !qualifying.isEmpty else { return false }

        for chapter in qualifying {
            let overlap = spanOverlap(span: span, chapter: chapter)
            let fraction = overlap / spanDuration
            if fraction >= Self.minSpanOverlapFraction {
                return true
            }
        }
        return false
    }

    // MARK: - Private

    /// Reject chapters whose bounds are non-finite or zero-length.
    /// `endTime == nil` is allowed (open-ended chapter, uses fallback).
    private static func chapterIsWellFormed(_ chapter: ChapterEvidence) -> Bool {
        guard chapter.startTime.isFinite, chapter.startTime >= 0 else { return false }
        if let end = chapter.endTime {
            guard end.isFinite, end > chapter.startTime else { return false }
        }
        return true
    }

    /// Length of the intersection between `span` and `chapter` (using
    /// the open-ended fallback duration when the chapter has no
    /// `endTime`).
    private static func spanOverlap(
        span: DecodedSpan,
        chapter: ChapterEvidence
    ) -> TimeInterval {
        let chStart = chapter.startTime
        let chEnd = chapter.endTime ?? (chStart + Self.openEndedFallbackDuration)
        let lower = max(span.startTime, chStart)
        let upper = min(span.endTime, chEnd)
        return max(0, upper - lower)
    }
}
