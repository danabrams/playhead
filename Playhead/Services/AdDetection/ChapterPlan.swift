// ChapterPlan.swift
// playhead-au2v.1.1: Foundation data-model artifact for the chapter-signal
// sub-epic.
//
// A `ChapterPlan` is the orchestration envelope produced by the chapter
// generation phase (boundary detection + FM labeling). Consumers — the
// CoveragePlanner audit-window selector (au2v.1.14), FM prompt builders
// (au2v.1.16), and the existing BackfillEvidenceFusion path through
// `ChapterMetadataEvidenceBuilder` — read the embedded
// `[ChapterEvidence]` directly. Reusing `ChapterEvidence` (rather than
// inventing a parallel `Chapter` type) means inferred chapters look
// identical to creator-supplied ones to consumers, except via the
// `ChapterSource.inferred` enum case.
//
// Cache identity: this struct is the value persisted by
// `ChapterPlanCache` keyed on the `episodeContentHash` (the same
// content hash key used by other on-device FM artifact caches).
//
// Schema versioning: `schemaVersion` is checked on read; a mismatch is
// treated as a cache miss, which lets the phase regenerate without a
// migration step. Bump the version any time the serialized shape
// changes (added/removed/renamed fields, semantic redefinition of an
// existing field).

import Foundation

// MARK: - ChapterPlanDiagnostics

/// Per-plan generation diagnostics. Recorded inside the plan so the
/// cache file is a self-describing record of what the chapter
/// generation phase actually did when it produced this plan.
///
/// Privacy: counts only — no titles, content, or user data. Safe to
/// log and to ship in diagnostics exports.
struct ChapterPlanDiagnostics: Sendable, Codable, Equatable {
    /// Total candidate chapter boundaries the boundary detector
    /// emitted before any cap or filter was applied.
    let candidatesDetected: Int
    /// Candidates that survived the configured per-episode cap and
    /// proceeded into FM labeling.
    let candidatesKept: Int
    /// Labeled chapters whose FM disposition was "operationally
    /// unclear" (the labeling service could not confidently classify
    /// the chapter as ad-break vs. content based on operational cues).
    let operationalUnclearCount: Int
    /// Labeled chapters whose FM disposition was "semantically
    /// unclear" (the labeling service could not confidently classify
    /// the chapter as ad-break vs. content based on transcript
    /// content).
    let semanticUnclearCount: Int

    init(
        candidatesDetected: Int = 0,
        candidatesKept: Int = 0,
        operationalUnclearCount: Int = 0,
        semanticUnclearCount: Int = 0
    ) {
        self.candidatesDetected = candidatesDetected
        self.candidatesKept = candidatesKept
        self.operationalUnclearCount = operationalUnclearCount
        self.semanticUnclearCount = semanticUnclearCount
    }
}

// MARK: - ChapterPlan

/// Orchestration envelope produced by the chapter generation phase.
///
/// A `ChapterPlan` is keyed in the cache by `episodeContentHash` and
/// captures everything the downstream consumers need: the inferred
/// chapter list (already in `[ChapterEvidence]` form), an aggregate
/// confidence number for the whole plan, the time the plan was
/// generated, the schema version it was serialized under, and a small
/// counts-only diagnostics record.
struct ChapterPlan: Sendable, Codable, Equatable {

    /// The current serialized-shape version. Bump on any breaking
    /// change to this struct's persisted JSON layout. `ChapterPlanCache`
    /// treats a mismatch on read as a cache miss.
    static let currentSchemaVersion: Int = 1

    /// Stable identity of the analyzed asset (content-hash of the
    /// underlying audio + relevant metadata). Matches the cache key.
    let episodeContentHash: String
    /// Inferred chapters for the whole episode. Every chapter has
    /// `source == .inferred` at construction time (the chapter
    /// generation phase is the only producer of this struct).
    let chapters: [ChapterEvidence]
    /// Duration-weighted confidence across the plan's chapters.
    /// Consumers can use it as a coarse "trust the whole plan?" gate
    /// without inspecting individual chapter scores.
    let planConfidence: Double
    /// Wall-clock instant the plan was assembled.
    let generatedAt: Date
    /// Serialized schema version. Defaults to `currentSchemaVersion`
    /// when not supplied; older plans on disk will keep their original
    /// value and trigger a cache miss on read after a bump.
    let schemaVersion: Int
    /// Counts-only diagnostics about how the plan was generated.
    let generationDiagnostics: ChapterPlanDiagnostics

    init(
        episodeContentHash: String,
        chapters: [ChapterEvidence],
        planConfidence: Double,
        generatedAt: Date,
        schemaVersion: Int = ChapterPlan.currentSchemaVersion,
        generationDiagnostics: ChapterPlanDiagnostics = ChapterPlanDiagnostics()
    ) {
        self.episodeContentHash = episodeContentHash
        self.chapters = chapters
        self.planConfidence = planConfidence
        self.generatedAt = generatedAt
        self.schemaVersion = schemaVersion
        self.generationDiagnostics = generationDiagnostics
    }

    // MARK: - Confidence math

    /// Compute duration-weighted plan confidence:
    ///   `sum(chapter.qualityScore × duration) / total_duration`
    ///
    /// Duration semantics:
    /// - `endTime` present and `> startTime` → use the real interval.
    /// - `endTime` is `nil` → 60s nominal (matches
    ///   `ChapterMetadataEvidenceBuilder` open-ended fallback).
    /// - `endTime` set but `<= startTime` (malformed) → chapter is
    ///   skipped (treated as zero contribution to numerator and
    ///   denominator).
    ///
    /// `qualityScore` values outside the documented `[0, 1]` range are
    /// folded back into `[0, 1]` after the weighted average so the
    /// returned confidence is always a valid probability.
    ///
    /// Returns `0.0` for an empty chapter list or when no chapter has
    /// usable duration.
    static func computePlanConfidence(_ chapters: [ChapterEvidence]) -> Double {
        guard !chapters.isEmpty else { return 0.0 }

        var weightedSum: Double = 0.0
        var totalDuration: Double = 0.0

        for chapter in chapters {
            let duration = effectiveDuration(of: chapter)
            guard duration > 0, duration.isFinite else { continue }
            let quality = Double(chapter.qualityScore)
            weightedSum += quality * duration
            totalDuration += duration
        }

        guard totalDuration > 0 else { return 0.0 }
        // Clamp to [0, 1] defensively in case a producer emits a
        // qualityScore outside the documented range.
        let raw = weightedSum / totalDuration
        return max(0.0, min(1.0, raw))
    }

    /// Effective duration of a chapter for confidence weighting.
    ///
    /// Three cases:
    /// 1. `endTime` is set and `> startTime` → use the real interval.
    /// 2. `endTime` is nil → fall back to 60s nominal (matches
    ///    `ChapterMetadataEvidenceBuilder`'s open-ended-chapter rule).
    /// 3. `endTime` is set but `<= startTime` (malformed; producers
    ///    upstream should already filter these) → return 0 so the
    ///    chapter is skipped by the `duration > 0` guard in
    ///    `computePlanConfidence`. This avoids silently fabricating
    ///    a 60s nominal duration for a chapter the producer told us
    ///    was zero-or-negative length.
    private static func effectiveDuration(of chapter: ChapterEvidence) -> Double {
        guard let end = chapter.endTime else { return 60.0 }
        guard end.isFinite, end > chapter.startTime else { return 0.0 }
        return end - chapter.startTime
    }
}
