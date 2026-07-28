// SpanExtentSupport.swift
// playhead-2350: EXTENT (boundary) support, represented separately from PRESENCE.
//
// Why this exists
// ---------------
// Fusion produced ONE number — `DecisionResult.skipConfidence` — and used it for
// two different jobs: "is there an ad here?" (PRESENCE) and "does this span cover
// the ad?" (EXTENT). The 2026-07-25 THEMOVE Catalyst replay is what that costs: FM
// coarse scans correctly reported `containsAd` over huge windows (17.04–1183.62,
// 49.02–1376.94, 3010.44–3575.88) while fusion attached that presence mass to
// narrow lexical/music SEEDS and shipped the seed geometry as the ad. Every
// persisted edge was `.unanchored`, yet every window carried
// `eligibilityGate == .eligible` and the correct-show arm made all four
// `autoSkipEligible`. One of those seeds began 12.72 s inside the host's sign-off.
//
// A span with strong presence evidence and invented edges is a BANNER, never a
// skip. This type is the extent half of the verdict: it says how well each edge
// is INDEPENDENTLY supported, and it is the sole input to the unanchored-edge
// auto-skip block applied in
// `DecisionResult.withExtentSupport(_:blockingUnanchoredAutoSkip:)`.
//
// Deliberately NOT in scope (playhead-4xqf owns it): inferring wider boundaries.
// This type never moves an edge. It only reports how much the existing edges are
// worth, so a span whose edges the pipeline invented cannot auto-skip.

import Foundation

// MARK: - ExtentAnchorTier

/// How well ONE edge of a span is independently supported, as an ORDINAL tier.
///
/// Deliberately a tier, not a probability: the pipeline has no calibrated
/// per-edge boundary likelihood, and manufacturing one would repeat the exact
/// mistake this bead fixes. The measured per-tier edge error that motivates the
/// ordering lives in `docs/autoskip-edge-padding-derivation-2026-07-20.md`
/// (`AutoSkipEdgePadding`'s margins are derived from the same evidence).
///
/// Conformances are deliberately minimal — no `CaseIterable`, no `Codable`. The
/// tier is derived from `AutoSkipEdgeAnchor` on demand and is not persisted (the
/// per-edge anchor raw values already are, on the `ad_windows` row), so a
/// speculative conformance would only be a shape to maintain. Add one when a
/// caller actually needs it. Mirrors the playhead-fqc8 `PromotionTrack` call.
enum ExtentAnchorTier: Int, Sendable, Hashable, Comparable {
    /// No independent support: the edge is where an FM/lexical/aggregator seed
    /// happened to land. The pipeline invented it.
    case none = 0
    /// Corroborated by an independent acoustic observation of the same edge —
    /// today, a `StingerRefiner` snap onto a per-show stinger bank match.
    case corroborated = 1
    /// Deterministic: the byte-exact rediff differ proved the origin served
    /// different bytes at this edge.
    case deterministic = 2

    static func < (lhs: Self, rhs: Self) -> Bool { lhs.rawValue < rhs.rawValue }
}

extension AutoSkipEdgeAnchor {
    /// The extent tier this anchor provenance is worth. One definition, shared
    /// by the gate and by diagnostics, so "anchored" cannot drift between them.
    var extentTier: ExtentAnchorTier {
        switch self {
        case .rediffByteExact: return .deterministic
        case .stingerSnapped: return .corroborated
        case .unanchored: return .none
        }
    }
}

// MARK: - SpanExtentSupport

/// The EXTENT half of a fused verdict: per-edge anchor provenance and the tier
/// it is worth. Pure value type; carries no scores and no geometry.
///
/// Start and end are tracked INDEPENDENTLY — a span with a byte-exact start and
/// an invented end is not "half safe", it is unsafe, because the invented edge
/// is the one that clips content.
struct SpanExtentSupport: Sendable, Equatable, Hashable {
    /// Provenance tier of the leading edge.
    let startAnchor: AutoSkipEdgeAnchor
    /// Provenance tier of the trailing edge.
    let endAnchor: AutoSkipEdgeAnchor

    /// The conservative default: neither edge is independently supported. This
    /// is what a span gets when nothing proved its geometry — including any
    /// call site that has not yet derived extent support.
    static let unanchored = SpanExtentSupport(
        startAnchor: .unanchored,
        endAnchor: .unanchored
    )

    var startTier: ExtentAnchorTier { startAnchor.extentTier }
    var endTier: ExtentAnchorTier { endAnchor.extentTier }

    /// A span is only as well-bounded as its WEAKER edge.
    var tier: ExtentAnchorTier { min(startTier, endTier) }

    /// `true` iff BOTH edges are independently supported. This — and only this —
    /// admits a span to auto-skip under playhead-2350.
    var isFullyAnchored: Bool { tier != .none }

    /// Which edges are unanchored, for diagnostics/logging. Stable order.
    var unanchoredEdges: [String] {
        var edges: [String] = []
        if startTier == .none { edges.append("start") }
        if endTier == .none { edges.append("end") }
        return edges
    }

    /// Ordinal encoding of `tier` in `[0, 1]` for LOGGING and MEASUREMENT only
    /// (decision logs and the replay harness want a number per verdict).
    ///
    /// NOT a probability and NOT calibrated — no decision path may compare it
    /// against a threshold. `isFullyAnchored` / `tier` are the decision inputs.
    var ordinalConfidence: Double {
        Double(tier.rawValue) / Double(ExtentAnchorTier.deterministic.rawValue)
    }

    /// Derive extent support for a fusion span from the two authoritative
    /// decision-build sources, start and end resolved INDEPENDENTLY:
    ///   • `.rediffByteExact` — the span carries `.rediffSlot` width ownership
    ///     (the byte-exact rediff differ set BOTH edges; refiners are bypassed
    ///     for width-owned spans, so this is whole-span by construction).
    ///   • `.stingerSnapped` — the `StingerRefiner` snapped this specific edge
    ///     (`trace.startSnapped` / `trace.endSnapped`).
    ///   • `.unanchored` — neither. The conservative default.
    ///
    /// A post-fusion geometry rewrite (finalizer trim/merge/split) invalidates
    /// both earlier edge claims and forces the conservative pair regardless of
    /// their original source: the rewritten edges are nobody's observation.
    ///
    /// Deliberately splice-agnostic: `.spliceSlot` is ACOUSTIC width, not
    /// byte-exact, so a splice-owned edge stays `.unanchored`.
    static func derive(
        anchorProvenance: [AnchorRef],
        stingerTrace: StingerRefinementTrace?,
        geometryWasRewritten: Bool = false
    ) -> SpanExtentSupport {
        guard !geometryWasRewritten else { return .unanchored }
        let rediffOwnsWidth = anchorProvenance.contains(.rediffSlot)
        return SpanExtentSupport(
            startAnchor: AutoSkipEdgeAnchor.derive(
                rediffByteExact: rediffOwnsWidth,
                stingerSnapped: stingerTrace?.startSnapped ?? false
            ),
            endAnchor: AutoSkipEdgeAnchor.derive(
                rediffByteExact: rediffOwnsWidth,
                stingerSnapped: stingerTrace?.endSnapped ?? false
            )
        )
    }
}
