// InventorySanityFilter.swift
// playhead-xr3t — Lightweight inventory sanity check.
//
// Post-hoc filter on ad-skip span candidates that rejects obviously-bad
// spans BEFORE they become user-visible skip decisions. Runs at the
// `AdDetection fusion → SkipOrchestrator` boundary so this bead does NOT
// modify fusion behaviour — fusion output is fed into the filter, and
// only spans that pass survive to the orchestrator's managed-window set.
//
// Rejection rules (rejects on ANY match):
//   (a) duration < 2 s
//   (b) span starts within 3 s of episode start OR ends within 3 s of
//       episode end (only applied when episode duration is known and > 0)
//   (c) span overlaps any **declared** content chapter, where "declared"
//       means a creator-source ChapterEvidence (id3, pc20, rssInline).
//       Inferred chapters (playhead-w7oi / playhead-au2v.1 outputs) are
//       explicitly NOT consulted — see `ChapterSource.isCreatorSource`.
//       Ad-break chapters never cause rejection (the span overlapping an
//       ad-break chapter is the intended outcome).
//
// Stateless by design. The filter is constructed once with its
// configuration; every evaluation is a pure function over the candidate
// span plus the contextual `episodeDuration` and `declaredChapters` it
// is passed.
//
// Flag-gated by `LightweightInventoryChecksSettings.enabled`. When the
// flag is OFF the filter is a no-op pass-through (every span resolves
// as `.passed`) so pre-Phase-3 behaviour is exactly restored.

import Foundation

/// Outcome of evaluating a single span through the inventory sanity
/// filter. `.passed` means the span survives; `.rejected` carries the
/// first matching rule for diagnostics and tests.
enum InventorySanityResult: Sendable, Equatable {
    case passed
    case rejected(reason: InventorySanityRejectionReason)

    /// True iff the span survives the filter.
    var isPassed: Bool {
        if case .passed = self { return true }
        return false
    }
}

/// Reason a span was rejected. Order matches the rule order in the
/// filter (`tooShort` before `tooEarly` before `tooLate` before
/// `overlapsDeclaredChapter`) — `evaluate(...)` returns the FIRST
/// matching rule so the reason is deterministic.
enum InventorySanityRejectionReason: String, Sendable, Equatable, CaseIterable {
    /// Rule (a): `endTime - startTime < durationFloorSeconds`.
    case tooShort
    /// Rule (b): `startTime < edgeMarginSeconds` (head edge).
    case tooEarly
    /// Rule (b): `endTime > episodeDuration - edgeMarginSeconds` (tail edge).
    case tooLate
    /// Rule (c): span overlaps a creator-source content chapter.
    case overlapsDeclaredChapter
}

/// Lightweight inventory sanity check (playhead-xr3t).
///
/// Apply at the fusion → SkipOrchestrator boundary. The filter is
/// stateless: construct once, call `evaluate(...)` per candidate span.
struct InventorySanityFilter: Sendable, Equatable {

    /// Minimum acceptable span duration in seconds. Spans with
    /// `endTime - startTime < this` are rejected as `.tooShort`. Uses a
    /// strict less-than so exactly 2.0 s passes.
    let durationFloorSeconds: TimeInterval

    /// Edge guard in seconds — spans whose head is in the first
    /// `edgeMarginSeconds` of the episode or whose tail is in the last
    /// `edgeMarginSeconds` are rejected. Uses strict comparisons so a
    /// span starting at exactly 3.0 s (or ending at exactly
    /// `duration - 3.0`) passes.
    let edgeMarginSeconds: TimeInterval

    /// When `false`, `evaluate(...)` always returns `.passed`. This is
    /// the "feature flag OFF, restore pre-Phase-3 behaviour" mode and is
    /// asserted by the rollback tests.
    let isEnabled: Bool

    init(
        isEnabled: Bool,
        durationFloorSeconds: TimeInterval = 2.0,
        edgeMarginSeconds: TimeInterval = 3.0
    ) {
        self.isEnabled = isEnabled
        // Clamp to non-negative so a misconfigured negative threshold can
        // never invert the comparison and pass everything as "too short".
        self.durationFloorSeconds = max(0, durationFloorSeconds)
        self.edgeMarginSeconds = max(0, edgeMarginSeconds)
    }

    /// Default production filter — wires `LightweightInventoryChecksSettings`.
    /// Defaults to ON per bead spec ("Default ON for new builds").
    static func production(
        settings: LightweightInventoryChecksSettings = .load()
    ) -> InventorySanityFilter {
        InventorySanityFilter(isEnabled: settings.enabled)
    }

    // MARK: - Evaluation

    /// Evaluate a single candidate span.
    ///
    /// - Parameters:
    ///   - startTime: Span start in seconds, episode-relative.
    ///   - endTime: Span end in seconds, episode-relative. Must be >=
    ///     `startTime`; degenerate spans (`endTime <= startTime`) are
    ///     always rejected as `.tooShort` regardless of the threshold.
    ///   - episodeDuration: Episode duration in seconds. Pass `nil` or
    ///     a non-positive value when unknown — the head-edge guard
    ///     remains active (it doesn't need the duration), but the tail-
    ///     edge guard becomes a no-op. Under-filtering on unknown
    ///     duration is the safer failure mode.
    ///   - declaredChapters: Chapters from publisher RSS / ID3 / PC20.
    ///     Pass `[]` when no chapter context is available. Inferred
    ///     chapters MUST be excluded by the caller — see
    ///     `ChapterSource.isCreatorSource`.
    func evaluate(
        startTime: Double,
        endTime: Double,
        episodeDuration: Double?,
        declaredChapters: [ChapterEvidence]
    ) -> InventorySanityResult {
        guard isEnabled else { return .passed }

        // Rule (a): duration floor. Strict less-than so a span of exactly
        // `durationFloorSeconds` passes. Degenerate spans (end <= start)
        // produce a non-positive duration and are caught here.
        let duration = endTime - startTime
        if !(duration >= durationFloorSeconds) {
            // The `!(... >= ...)` form catches NaN explicitly too — a
            // NaN endpoint compares false in every direction, so it
            // falls into the `.tooShort` bucket rather than being
            // silently passed.
            return .rejected(reason: .tooShort)
        }

        // Rule (b) head: span starts in the first `edgeMarginSeconds`.
        // Strict less-than so a span starting at exactly the margin
        // boundary (e.g. 3.0 s) passes.
        if startTime < edgeMarginSeconds {
            return .rejected(reason: .tooEarly)
        }

        // Rule (b) tail: span ends in the last `edgeMarginSeconds`.
        // Only applied when duration is known and finite — otherwise
        // we'd reject every span whose `endTime > -3.0`.
        if let episodeDuration, episodeDuration > 0, episodeDuration.isFinite {
            let tailBoundary = episodeDuration - edgeMarginSeconds
            if endTime > tailBoundary {
                return .rejected(reason: .tooLate)
            }
        }

        // Rule (c): overlap with a declared, NON-ad-break content
        // chapter. Ad-break chapters never trigger rejection — a span
        // overlapping an ad-break chapter is exactly what fusion is
        // supposed to produce.
        //
        // Open-interval overlap: a span [s, e] overlaps a chapter
        // [cs, ce] iff `s < ce && e > cs`. We deliberately use strict
        // comparisons so a span that merely *touches* a chapter
        // boundary (e.g. span ends at chapter start, or starts at
        // chapter end) does NOT count as overlap. Spec: "span touching
        // but not overlapping chapter boundary" is a passing case.
        for chapter in declaredChapters {
            // Only consider creator-source chapters. The caller is
            // expected to pre-filter, but enforce here too as a
            // defense-in-depth check — `.inferred` chapters slipping
            // into the input list would otherwise produce false
            // rejections.
            guard chapter.source.isCreatorSource else { continue }
            // Ad-break chapters do not cause rejection — overlapping
            // them is the *intended* fusion outcome.
            if chapter.disposition == .adBreak { continue }

            // Open chapter end: when an RSS chapter lacks an explicit
            // end time it implicitly runs to the next chapter's start
            // (or episode end). Without that signal the safest thing
            // is to treat the chapter as covering [startTime, +∞);
            // a span starting after the chapter's start can still
            // overlap. This is consistent with how downstream
            // chapter-evidence consumers handle missing `endTime`.
            let chapterStart = chapter.startTime
            let chapterEnd = chapter.endTime ?? .infinity

            // Strict-strict overlap: spans that merely touch the
            // boundary (start == chapterEnd, or end == chapterStart)
            // do NOT count.
            if startTime < chapterEnd && endTime > chapterStart {
                return .rejected(reason: .overlapsDeclaredChapter)
            }
        }

        return .passed
    }
}
