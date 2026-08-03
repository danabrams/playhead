// HotPathExtentGate.swift
// playhead-bllt: the HOT PATH's half of playhead-2350's presence-vs-extent gate.
//
// Why this exists
// ---------------
// playhead-2350 established the rule — *a span with strong presence evidence
// and invented edges is a BANNER, never a skip* — and enforced it in
// `AdDetectionService.runBackfill`'s emission loop via
// `DecisionResult.withExtentSupport(_:blockingUnanchoredAutoSkip:)`. That
// closed the FUSION producer. It did not close the other one.
//
// `ad_windows` has a second producer that reaches auto-skip: the hot path.
// Two sites write it —
//   • `AdDetectionService.runHotPath`'s single-window post-classify site, and
//   • `runSegmentAggregation`'s promotion site —
// and both stamp `eligibilityGate` from `precisionGateLabel`, which answers
// PRESENCE only ("is the score high enough, with a strong corroborator?").
// Neither site sets `AdWindow.startEdgeAnchor` / `endEdgeAnchor`, so every row
// they write is `.unanchored` on BOTH edges by construction — the exact
// condition 2350 demotes fusion rows for. A hot-path row could therefore carry
// `"autoSkip"` into `SkipOrchestrator`'s managed tier with edges nobody proved,
// and did.
//
// This mattered more after playhead-nqey shipped `certaintyTieredSkipEnabled`
// ON: with tiered skip live, a second producer reaching auto-skip unchecked is
// a live defect rather than a latent one.
//
// The only last-mile per-edge veto, `AutoSkipEdgePadding`, ships
// `isEnabledByDefault == false` (playhead-98co's deliberate posture, priced at
// n=44 and waiting on the corpus growth of playhead-xdh7), so it does not run
// for anyone today and cannot be the answer.
//
// WHY NOT INSIDE `precisionGateLabel`
// -----------------------------------
// Two reasons, and the second is the real one.
//   1. `AdDetectionServicePrecisionGateLabelCanaryTests` source-inspects that
//      function's body and requires `label: "autoSkip"` in a value-producing
//      position. Gating there would force the canary to be weakened.
//   2. 2350's own shape. It demotes at the EMISSION site, after the decision,
//      so the decision stays an honest statement about presence and the
//      demotion stays an honest statement about extent. Mixing the two back
//      into one verdict is the mistake 2350 exists to have undone.
//
// THE GOVERNING CONSTRAINT
// ------------------------
// Any change here must only ever make FEWER things auto-skippable, never more.
// That is not a style note; it is what makes the change safe to ship without
// the corpus that playhead-98co could not get. `gatedLabel` is a total function
// over a finite domain and `HotPathExtentGateMonotonicityTests` enumerates it
// EXHAUSTIVELY — every label × every start anchor × every end anchor × both
// flag values — asserting that admission never rises. Not sampled, not
// representative: all of it.

import Foundation

// MARK: - HotPathAdmission

/// What a listener can experience from a persisted hot-path row, as an ORDINAL.
///
/// This is the quantity the monotonicity proof is stated over, so it is
/// deliberately about the LISTENER rather than about the string: two different
/// labels that both reach nobody are the same rung.
enum HotPathAdmission: Int, Sendable, Hashable, Comparable, CaseIterable {
    /// Nothing reaches the listener. Either no row is written at all (a `nil`
    /// label is `AutoSkipPrecisionGate.detectionOnly`, which both hot-path
    /// sites `guard let` into a `continue`), or a row is written with a stamp
    /// `SkipOrchestrator.receiveAdWindows` refuses as malformed
    /// (`droppedMalformedEligibilityGate`).
    case none = 0
    /// The suggest tier. The listener is ASKED, by a banner that fires when the
    /// playhead ENTERS the span (playhead-d3g0). No audio is removed.
    case banner = 1
    /// The managed tier. Audio is removed without asking.
    case autoSkip = 2

    static func < (lhs: Self, rhs: Self) -> Bool { lhs.rawValue < rhs.rawValue }
}

// MARK: - HotPathExtentGate

/// The hot path's presence-vs-extent gate: pure, total, and the single place
/// the rule is spelled for this producer.
enum HotPathExtentGate {

    /// The persisted `ad_windows.eligibilityGate` literal that admits a row to
    /// the auto-skip (managed) tier.
    ///
    /// NOT `SkipEligibilityGate.eligible.rawValue`. This literal is not a
    /// `SkipEligibilityGate` raw value at all — it rides the orchestrator's
    /// legacy nil-gate contract, is whitelisted by name in three consumers, and
    /// is pinned by `AdDetectionServicePrecisionGateLabelCanaryTests` against
    /// exactly the well-meaning "align it with the enum" change that would
    /// silently mismatch every row already on a user's device. playhead-y87g
    /// re-diagnosed this and found it to be a documented producer contract, not
    /// a bug; do not chase it.
    static let autoSkipLabel = "autoSkip"

    /// The persisted literal that routes a row to the suggest (banner) tier.
    /// Round-trips as `SkipEligibilityGate.markOnly`.
    static let markOnlyLabel = "markOnly"

    /// Where a persisted hot-path row carrying `label` can land.
    ///
    /// Total by construction: anything the orchestrator does not recognise is
    /// `.none`, because that is what the orchestrator does with it.
    static func admission(of label: String?) -> HotPathAdmission {
        switch label {
        case .none: return .none
        case .some(autoSkipLabel): return .autoSkip
        case .some(markOnlyLabel): return .banner
        case .some: return .none
        }
    }

    /// Apply the extent half of the verdict to a hot-path precision-gate label.
    ///
    /// Contract — deliberately the same additive, post-gate shape as
    /// `DecisionResult.withExtentSupport`:
    ///   • **NEVER promotes.** The ONLY transition is
    ///     `"autoSkip"` → `"markOnly"`, and only when the extent has an
    ///     unanchored edge and the block is enabled. Every other input is
    ///     returned byte-identically, including unrecognised strings.
    ///   • **NEVER drops a row.** `nil` in, `nil` out; a non-`nil` label never
    ///     becomes `nil`. A demoted span keeps its banner — the ad IS there, we
    ///     just cannot prove where it starts or stops. Turning the demotion
    ///     into a drop would trade a wrong skip for a missed ad, which is a
    ///     different decision than the one this bead was given.
    ///   • **NEVER touches the score.** `AdWindow.confidence` is presence and
    ///     stays honest; only actionability changes.
    ///
    /// A span is only as well-bounded as its weaker edge, so this reads
    /// `SpanExtentSupport.isFullyAnchored` — BOTH edges — rather than either
    /// one. An invented end clips the show just as an invented start does.
    ///
    /// - Parameter label: the `precisionGateLabel` verdict for this row.
    /// - Parameter extent: the per-edge provenance the row will be PERSISTED
    ///   with. Pass the same value that is written to
    ///   `AdWindow.startEdgeAnchor` / `endEdgeAnchor` — one variable used
    ///   twice, never two expressions that happen to agree (the playhead-6qvf
    ///   lesson).
    /// - Parameter blockingUnanchoredAutoSkip: production passes
    ///   `AdDetectionConfig.unanchoredExtentBlocksAutoSkip`, which ships `true`.
    ///   The SAME flag 2350 uses, deliberately: one switch, both producers, so
    ///   the kill switch cannot half-fire.
    static func gatedLabel(
        _ label: String?,
        extent: SpanExtentSupport,
        blockingUnanchoredAutoSkip: Bool
    ) -> String? {
        guard blockingUnanchoredAutoSkip,
              !extent.isFullyAnchored,
              label == autoSkipLabel
        else { return label }
        return markOnlyLabel
    }

    /// Whether `gatedLabel` would change this row's verdict — for the caller
    /// that wants to log the demotion without re-deriving the condition.
    static func demotes(
        _ label: String?,
        extent: SpanExtentSupport,
        blockingUnanchoredAutoSkip: Bool
    ) -> Bool {
        gatedLabel(
            label,
            extent: extent,
            blockingUnanchoredAutoSkip: blockingUnanchoredAutoSkip
        ) != label
    }

    /// The isp5 census sub-cause a suggest-tier row carries when its persisted
    /// extent is unanchored — i.e. when, whatever else was true of it, it could
    /// not have been auto-skipped under this gate.
    ///
    /// `nil` for a fully-anchored row, deliberately. A detail that fires on
    /// every delivery is exactly as useless as one that never fires; see the
    /// `retired=` asymmetry in `AdWindowIngestOutcome`'s header for the same
    /// rule stated about the census row itself.
    ///
    /// The edge list is part of the reason. "start+end" is the hot path's
    /// signature (unanchored by construction on both edges); a lone "end" is a
    /// fusion row whose trailing edge nobody snapped, and those are different
    /// news.
    static func censusDetail(for extent: SpanExtentSupport) -> String? {
        guard !extent.isFullyAnchored else { return nil }
        return "unanchored_extent_" + extent.unanchoredEdges.joined(separator: "+")
    }
}
