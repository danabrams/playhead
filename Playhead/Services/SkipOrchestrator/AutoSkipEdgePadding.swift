// AutoSkipEdgePadding.swift
// playhead-98co: asymmetric start-edge padding for auto-skip (Gate-2 blocker).
//
// Why this exists
// ---------------
// Auto-skip error direction is asymmetric. A skip that starts LATE plays a
// sliver of ad (annoying, recoverable); a skip that starts EARLY clips real
// content (trust-destroying). Symmetrically at the end: ending EARLY replays
// ad tail (annoying); ending LATE clips content (bad). Measured against the
// 2026-07-15 gold ear-audit, end edges are stinger-accurate but start edges
// are weak (tol-adjusted p50 24.4 s) — so ship-grade auto-skip pads each edge
// by a margin DERIVED FROM DATA, per anchor tier, sized so the
// content-clipping direction had zero observed residual events.
//
// Every margin constant below is derived in
// `docs/autoskip-edge-padding-derivation-2026-07-20.md` (reproduce with
// `scripts/l2f-derive-autoskip-padding.py`). Do not retune by hand: if the
// boundary stack changes, re-run the derivation and update BOTH the doc and
// these constants together.
//
// Contract
// --------
//   • Padding only ever SHRINKS the skipped region — a skip window is never
//     widened beyond the marked span. The marked span itself (banners,
//     transcript overlay, applied-segment broadcasts) is untouched; padding
//     applies exclusively to the playback SKIP WINDOW.
//   • An edge may only be auto-skipped when its anchor tier PROVES
//     late-safety. Unanchored starts have no cheap late-safe margin
//     (derivation §5) — spans without a hard start anchor return nil
//     (caller keeps them markOnly).
//   • Degenerate spans whose width is consumed by the combined margins
//     return nil (skip suppressed, span stays markOnly).
//   • markOnly / suggest / banner spans never reach this policy — the
//     orchestrator consults it only on the auto-skip path. Skips whose EXTENT
//     the user chose (a span marked in the transcript, a manual tap on an
//     ordinary detection row) are exempted by the caller: those edges are the
//     user's own.
//
//     playhead-ynmk narrowed that exemption. It used to include an ACCEPTED
//     SUGGESTION, on the premise that a banner Yes chose the edges. It does
//     not: the banner asks "is this an ad?", and the DETECTOR drew the span.
//     Measured false in the field on 2026-07-31 — three both-edges-unanchored
//     spans confirmed by one tap each, 0 of 3 correct, 210 s of SHOW skipped.
//     A confirmation is now governed by this policy exactly like an auto-skip,
//     and independently of the Gate-2 master switch (the same targeted
//     activation shape qs0d used for byte-exact pairs, and safe for the same
//     reason: this policy only ever shrinks or suppresses).
//     `UserSpanAssertion.assertsExtent` is where the two cases are told apart.
//   • CUSHION STACKING: the orchestrator's pre-existing pod-level trailing
//     cushion (`SkipPolicyConfig.adTrailingCushionSeconds`, 1.0 s,
//     playhead-vn7n.2) applies AFTER these margins, to each merged cue's
//     end in `pushMergedCues`. The effective flag-ON end pull-in is
//     therefore endMargin + 1.0 s (e.g. 11.25 s total for an unanchored
//     end). Deliberate, same-direction stacking: the margin guarantees the
//     cue end never passes the true ad end; the cushion cedes one extra
//     second of ad tail (the recoverable direction). Note the 1.0 s
//     remainder floor below is checked BEFORE the cushion — a
//     minimum-remainder span therefore collapses to a zero-length cue
//     (a harmless no-op skip, the pre-existing clamp in `pushMergedCues`).
//     See derivation doc §7.
//
// The policy is pure and stateless: anchors are INPUTS. Per-edge anchor
// provenance is derived at fusion/decision-build time (rediff `.rediffSlot`
// width ownership and the `StingerRefiner` snap trace both live in
// AdDetectionService), persisted on the `AdWindow` row, and stamped into
// `SkipOrchestrator.edgeAnchorsByWindowId` at ingest (playhead-hdgk). A row
// with no derived anchor still classifies `.unanchored` on both edges — so a
// pipeline span with neither a rediff-slot nor a stinger snap remains
// unskippable under flag-ON. That default is the intended conservative
// posture, not a shortcut; enabling auto-skip is a separate Gate-2 decision.

import Foundation

// MARK: - UserSpanAssertion

/// What a USER asserted about a span, as distinct from what the DETECTOR
/// measured about it (playhead-ynmk).
///
/// `AdWindow.confidence` is the measured quantity and a user gesture must never
/// write it. Apply the standing check to the value a tap used to synthesise: a
/// confirmation wrote `confidence = 1.0`, and a span of pure show content
/// confirmed by mistake would read `1.00` just the same — so the number
/// recorded that a tap happened, not that an ad exists. That is how 210 s of
/// show came to be skipped on 2026-07-31 with the aggregator 0 of 3 correct,
/// and it is the same defect class as pz32's checkmark.
///
/// The gesture is recorded instead in the `AdWindow.boundaryState` column,
/// which has always carried it and which nothing reads as certainty. This enum
/// is that column's user-owned half, typed.
///
/// The two cases are NOT interchangeable, and the difference is the bead.
enum UserSpanAssertion: String, Sendable, Equatable, CaseIterable {
    /// The user drew this span themselves in the transcript (playhead-527u).
    /// Asserts PRESENCE **and** EXTENT — the edges are the user's own.
    case userMarked
    /// The user answered Yes to a suggest-tier banner. Asserts PRESENCE ONLY:
    /// the question was "is this an ad?", the DETECTOR drew the edges, and the
    /// user was never asked whether it is safe to cut from here to there.
    case userConfirmedSuggested

    /// Whether this assertion covers where the span BEGINS and ENDS.
    ///
    /// Only a span the user drew does. A confirmation's extent stays the
    /// detector's, so it is governed by `AutoSkipEdgePadding` like any other
    /// detector extent.
    ///
    /// This is the fidelity ladder (feedback_manual_marks_override_2026-07-29:
    /// transcript marking > banner response > inferred listenRevert) expressed
    /// as a capability rather than a rank. A confirmation still outranks an
    /// inferred revert on PRESENCE — nothing here touches that. It simply never
    /// outranked anything on EXTENT, because it never carried extent
    /// information in the first place.
    var assertsExtent: Bool {
        switch self {
        case .userMarked:
            return true
        case .userConfirmedSuggested:
            return false
        }
    }
}

extension AdWindow {
    /// The user gesture recorded on this row, if any — `nil` for every purely
    /// detector-produced boundary state. See `UserSpanAssertion`.
    var userAssertion: UserSpanAssertion? {
        UserSpanAssertion(rawValue: boundaryState)
    }
}

// MARK: - AutoSkipEdgeAnchor

/// The provenance tier of one edge of a decision span, as known at skip
/// time. Determines which derived margin applies to that edge.
enum AutoSkipEdgeAnchor: String, Sendable, Equatable, CaseIterable {
    /// The edge was set by the byte-exact rediff differ (A/B fetch
    /// alignment, playhead-xsdz.44/xsdz.57): byte-verified splice mapped to
    /// the A timeline.
    case rediffByteExact
    /// The edge was snapped by the per-show stinger bank
    /// (`StingerRefiner`, trace `startSnapped` / `endSnapped` true).
    case stingerSnapped
    /// No hard anchor: FM/lexical/aggregator boundary, unsnapped stinger
    /// consult, or unknown provenance. The conservative default.
    case unanchored
}

extension AutoSkipEdgeAnchor {
    /// Derive one edge's anchor tier from its authoritative decision-build
    /// signals (playhead-hdgk). Start and end are derived by calling this
    /// once per edge, INDEPENDENTLY — nothing here couples the two.
    ///
    /// Precedence, highest first:
    ///   1. `.rediffByteExact` — this edge was set by the byte-exact rediff
    ///      differ (the span carries `.rediffSlot` width ownership). The byte
    ///      differ outranks a stinger snap even in the (production-impossible,
    ///      since rediff-owned spans bypass the refiner) case where both fire:
    ///      the differ "did not misfire" — same rationale as the
    ///      `stingerStartDemotedShowKeys` scoping.
    ///   2. `.stingerSnapped` — the `StingerRefiner` snapped this edge (its
    ///      trace `startSnapped` / `endSnapped` is true for this edge).
    ///   3. `.unanchored` — neither fired. The conservative default.
    ///
    /// Pure and total; no actor state, no side effects.
    static func derive(
        rediffByteExact: Bool,
        stingerSnapped: Bool
    ) -> AutoSkipEdgeAnchor {
        if rediffByteExact { return .rediffByteExact }
        if stingerSnapped { return .stingerSnapped }
        return .unanchored
    }
}

// MARK: - AutoSkipEdgePadding

/// Pure policy: maps a marked span + per-edge anchors to the late-safe
/// playback skip window, or nil when no late-safe window exists (the span
/// stays markOnly). See file header for the contract and the derivation doc
/// for every number.
enum AutoSkipEdgePadding {

    /// Master enable. Default OFF: auto-skip itself is held behind Gate 2,
    /// and flag-OFF preserves byte-identical orchestrator behavior. Flip
    /// deliberately, with the Gate-2 blocker set
    /// (wraj surfacing + veto masks + 3-run reproducibility) green.
    static let isEnabledByDefault = false

    /// playhead-qs0d: the TARGETED activation predicate — is this policy live
    /// for a span carrying THESE anchors?
    ///
    /// Read the master switch's polarity carefully before touching this. With
    /// `isEnabledByDefault == false` the orchestrator does not "skip nothing" —
    /// it skips every eligible span at its RAW, unpadded bounds, because
    /// `paddedCueSpan` short-circuits to the snapped span. The master switch ON
    /// is therefore strictly MORE conservative than OFF: padding only ever
    /// shrinks a window or suppresses it (`startMargin` returns nil for an
    /// unanchored start), never widens one and never admits a span that was
    /// previously blocked.
    ///
    /// That polarity is what makes a targeted activation safe. A span whose
    /// BOTH edges are `.rediffByteExact` is one the byte differ proved
    /// sample-accurately, and turning the policy on for exactly those spans
    /// gives them the derived late-safe margins (0.50 s in / 0.75 s off)
    /// WITHOUT changing a single other span's behavior — every other anchor
    /// combination still takes the master switch's answer. Dan asked for
    /// *rediff* to skip; this is that, and nothing else.
    ///
    /// Both edges are required, not just the start: an `.unanchored` end on a
    /// byte-exact start would pull in `endMarginUnanchoredSeconds` (10.25 s),
    /// a margin derived for a population this lane is not.
    ///
    /// `masterEnabled` is the caller's RUNTIME flag (the orchestrator's
    /// `edgePaddingEnabled`, seeded from `isEnabledByDefault` but overridable),
    /// taken as an input rather than read from the static so this stays pure
    /// and a runtime disable really disables the master lane.
    static func isActive(
        masterEnabled: Bool,
        startAnchor: AutoSkipEdgeAnchor,
        endAnchor: AutoSkipEdgeAnchor
    ) -> Bool {
        if masterEnabled { return true }
        return startAnchor == .rediffByteExact && endAnchor == .rediffByteExact
    }

    // MARK: Derived margins (seconds) — derivation doc §5

    /// Start margin, rediff byte-exact tier: 0 early events across the
    /// xsdz.44 spike's 11 gold breaks (consistent late bias, median
    /// +0.29 s) + 0.3 s gold attestation tolerance, 0.25 s grid.
    static let startMarginRediffByteExactSeconds = 0.50
    /// Start margin, stinger-snapped tier: worst non-demoted early snap
    /// −0.18 s (morbid) / −0.02 s (nikki good snaps, tol ±0.5) → 0.52,
    /// 0.25 s grid.
    static let startMarginStingerSnappedSeconds = 0.75
    /// End margin, rediff byte-exact tier: worst late +0.22 s + 0.3 s tol.
    static let endMarginRediffByteExactSeconds = 0.75
    /// End margin, stinger-snapped tier: worst replicated late +0.44 s
    /// (smartless) + 0.3 s tol. (The 44-gold ted +30.92 was adjudicated as
    /// a gold under-label via the 2026-07-16 requalify — derivation §4.)
    static let endMarginStingerSnappedSeconds = 0.75
    /// End margin, unanchored tier — **unreachable for fusion spans since
    /// playhead-2350**, kept because the tier itself still exists and the
    /// derivation is real. 98co's measured verdict was that an unanchored END is
    /// survivable (ending early replays ad tail) so long as the START is
    /// anchored. 2350 is stricter on a different axis: a span with ANY
    /// unanchored edge is demoted to mark-only in fusion and therefore never
    /// reaches this policy at all. Strictly safer, and deliberate — but if the
    /// anchored-start/unanchored-end combination is ever readmitted, re-derive
    /// this number rather than assuming it still holds.
    ///
    /// Worst late unsnapped end across BOTH
    /// measured builds — +7.55 s (conan, gold v6, 07-16 xsdz39bank build)
    /// and +9.92 s (smartless 05-21, 07-17 danshows build) — + 0.3 s tol,
    /// 0.25 s grid → 10.25. This tier's tail is build-sensitive
    /// (derivation §6a), hence the cross-build sizing. Ending early
    /// replays ad tail — annoying but recoverable, so this tier stays
    /// skippable when (and only when) the START edge is anchored.
    static let endMarginUnanchoredSeconds = 10.25

    /// After padding, the remaining skip window must retain at least this
    /// many seconds or the skip is suppressed (degenerate span → markOnly).
    static let minimumSkippableRemainderSeconds = 1.0

    /// Per-show demotion of STINGER-SNAPPED starts (derivation §5): shows
    /// whose pre-anchor has an observed misfire mode that no cheap padding
    /// covers. nikki-glaser: 2/7 snapped starts ~29 s early in both golds
    /// (pre-anchor confidence 0.67, the weakest in the bank). Keys follow
    /// the `StingerBank.showKeys` alias convention — corpus `showSlug` AND
    /// production `podcastId` (RSS feed URL) — resolved by exact match.
    /// Scoped to `.stingerSnapped` starts only: a rediff byte-exact start
    /// on the same show is NOT demoted (the byte differ did not misfire).
    static let stingerStartDemotedShowKeys: Set<String> = [
        "the-nikki-glaser-podcast",
        "https://www.omnycontent.com/d/playlist/e73c998e-6e60-432f-8610-ae210140c5b1/0d8967bb-212c-4f2e-85bb-ae2700380ca7/2558cddf-28c7-463d-b70b-ae2700380cc3/podcast.rss",
    ]

    // MARK: Per-edge margins

    /// The start-edge margin for an anchor tier, or nil when the tier is
    /// unskippable (no late-safe margin exists — derivation §5 verdict:
    /// unanchored starts stay markOnly).
    static func startMargin(
        for anchor: AutoSkipEdgeAnchor,
        showKey: String? = nil
    ) -> Double? {
        switch anchor {
        case .rediffByteExact:
            return startMarginRediffByteExactSeconds
        case .stingerSnapped:
            if let showKey, stingerStartDemotedShowKeys.contains(showKey) {
                return nil
            }
            return startMarginStingerSnappedSeconds
        case .unanchored:
            return nil
        }
    }

    /// The end-edge margin for an anchor tier. Every end tier has a defined
    /// margin: ending early is the recoverable direction, so even an
    /// unanchored end is skippable at the (large) derived margin.
    static func endMargin(for anchor: AutoSkipEdgeAnchor) -> Double {
        switch anchor {
        case .rediffByteExact:
            return endMarginRediffByteExactSeconds
        case .stingerSnapped:
            return endMarginStingerSnappedSeconds
        case .unanchored:
            return endMarginUnanchoredSeconds
        }
    }

    // MARK: Skip window

    /// Compute the late-safe playback skip window for a marked span.
    ///
    /// Returns nil when the span must NOT be auto-skipped (caller keeps it
    /// markOnly): non-positive-width span, unskippable start tier
    /// (unanchored, or stinger-snapped on a demoted show), or a degenerate
    /// span whose width the combined margins consume.
    ///
    /// Invariants (pinned by tests):
    ///   • shrink-only: `result.start >= spanStart && result.end <= spanEnd`
    ///   • `result.end - result.start >= minimumSkippableRemainderSeconds`
    static func skipWindow(
        spanStart: Double,
        spanEnd: Double,
        startAnchor: AutoSkipEdgeAnchor,
        endAnchor: AutoSkipEdgeAnchor,
        showKey: String? = nil
    ) -> (start: Double, end: Double)? {
        guard spanStart.isFinite, spanEnd.isFinite, spanEnd > spanStart else {
            return nil
        }
        guard let rawStartMargin = startMargin(for: startAnchor, showKey: showKey) else {
            return nil
        }
        // Defensive clamps: margins are non-negative constants by
        // construction; the max(0, _) makes the shrink-only invariant
        // structural rather than conventional.
        let paddedStart = spanStart + max(0.0, rawStartMargin)
        let paddedEnd = spanEnd - max(0.0, endMargin(for: endAnchor))
        guard paddedEnd - paddedStart >= minimumSkippableRemainderSeconds else {
            return nil
        }
        return (start: paddedStart, end: paddedEnd)
    }
}
