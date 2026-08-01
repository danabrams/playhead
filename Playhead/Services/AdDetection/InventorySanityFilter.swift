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
//   (b) span LIES WITHIN the first 3 s of the episode, or within the
//       last 3 s (the tail half only when episode duration is known
//       and > 0). See "The edge rules read the inner edge" below —
//       this rule used to test the span's START and END, and that
//       rejected every pre-roll and every post-roll.
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
//
// THE EDGE RULES READ THE INNER EDGE (playhead-b6r2)
// --------------------------------------------------
// Rule (b) was specified as "reject spans IN the first / last 3 s" and
// implemented as `startTime < 3` / `endTime > duration - 3`. A pre-roll
// starts at 0.0 and a post-roll ends at the episode end, so the
// implementation rejected 100 % of the population the rule was aimed
// at. Measured on the 2026-08-01 field session (episode D9B513CD): day-0
// rediff minted four byte-exact windows at confidence 1.00 and rule (b)
// dropped TWO of them, `d0-1` as `tooEarly` and `d0-4` as `tooLate`,
// before tier routing — so no skip, no banner, no mark. Dan heard the ad.
//
// The fix follows Dan's 2026-07-29 ruling, "outer edges are free to
// widen, inner edges eat the show". A pre-roll's OUTER edge is its
// start, pinned to 0 by the episode boundary; there is nothing there to
// guard. Its INNER edge is its end. A post-roll's outer edge is its
// end, its inner edge its start. So the rule measures the inner edge
// and ignores the free one, which — given rule (a)'s floor and the
// orchestrator's `startTime >= 0` material check — is exactly the
// spec's own word, WITHIN.
//
// WHAT THIS GIVES UP, STATED. The old tail rule incidentally rejected a
// span that swallowed the whole episode, and the new one does not. That
// protection was never coherent: it keyed on the outer edge, so it
// rejected [3928, 3930] and [0, 3930] on identical grounds while
// admitting [500, 3920] — 57 minutes of show — because that span stops
// ten seconds early. Separating a 30 s post-roll from a 57-minute one
// needs a WIDTH test. Rule (a) is a floor by design; this filter has no
// ceiling, and adding one is a policy question (what is the widest
// legitimate ad break?) rather than a silent side effect of this fix.
// Pinned by `theEdgeRulesNoLongerRejectASpanThatSwallowsTheWholeEpisode`
// so the loss is a recorded decision, not a future surprise.

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
    /// Rule (b): `endTime <= edgeMarginSeconds` — the span lies within the
    /// first `edgeMarginSeconds` of the episode. Note this is the span's
    /// INNER edge; a pre-roll that merely BEGINS at 0 is not too early
    /// (playhead-b6r2).
    case tooEarly
    /// Rule (b): `startTime >= episodeDuration - edgeMarginSeconds` — the
    /// span lies within the last `edgeMarginSeconds`. The inner edge at
    /// this end is the START, so a post-roll that merely ENDS at the
    /// episode end is not too late (playhead-b6r2).
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

    /// Edge guard in seconds — spans lying WITHIN the first
    /// `edgeMarginSeconds` of the episode, or within the last
    /// `edgeMarginSeconds`, are rejected. The boundary case (a span that
    /// exactly fills the band: `[0, 3.0]`, or `[duration - 3.0, duration]`)
    /// is inside the band and rejects; a span that extends past the band by
    /// any amount passes.
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

    /// The configuration production runs on a fresh install — i.e. what
    /// `.production()` resolves to when no value has been persisted, which is
    /// the state a wipe-and-reinstall leaves the device in.
    ///
    /// playhead-b6r2: this exists so `SkipOrchestrator.init`'s default and the
    /// field's configuration are THE SAME EXPRESSION rather than two literals
    /// that agreed until they didn't. The old init default was
    /// `InventorySanityFilter(isEnabled: false)` against a production default
    /// of ON, and that divergence is why playhead-djl0's reproduction of the
    /// 2026-08-01 field case asserted the banner IS emitted and passed: the
    /// guard under investigation was switched off in every observation surface
    /// and live in the field. A future change to `defaultEnabled` now moves
    /// both sides at once.
    static let productionDefaultConfiguration = InventorySanityFilter(
        isEnabled: LightweightInventoryChecksSettings.defaultEnabled
    )

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
    ///     edge guard becomes a no-op, because the band it measures is
    ///     defined relative to the episode end. Under-filtering on
    ///     unknown duration is the safer failure mode.
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

        // Rule (b) head: the span LIES WITHIN the first `edgeMarginSeconds`.
        // The test is on `endTime` — the span's INNER edge — because the
        // outer edge is pinned to the episode boundary and cannot cost the
        // listener a second of show. `<=` so a span that exactly fills the
        // margin band ([0, 3.0]) is still "in the first 3 s" and rejects,
        // while one that pokes out of it by any amount does not.
        if endTime <= edgeMarginSeconds {
            return .rejected(reason: .tooEarly)
        }

        // Rule (b) tail: the mirror. The span lies within the last
        // `edgeMarginSeconds`, tested on `startTime` — the inner edge at
        // this end. Only applied when duration is known and finite;
        // otherwise there is no denominator to measure the band against.
        if let episodeDuration, episodeDuration > 0, episodeDuration.isFinite {
            let tailBoundary = episodeDuration - edgeMarginSeconds
            if startTime >= tailBoundary {
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

            // Open chapter end: when a chapter lacks an explicit end
            // time, its true span is unknown. The caller (e.g.
            // `SkipOrchestrator.setDeclaredChapters`) is responsible
            // for synthesizing an end from the next chapter's start
            // where available — see review-round-2 fix. For the
            // trailing open-ended chapter (last in the episode, no
            // successor) we have no bound, so we must NOT reject:
            // over-rejection means the user expected an ad-skip and
            // didn't get one (a regression in xr3t terms). Skip the
            // chapter for this rule. The other rules ((a), (b)-tail)
            // still apply.
            guard let chapterEnd = chapter.endTime else { continue }
            let chapterStart = chapter.startTime

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
