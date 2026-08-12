// SkipDetectorClass.swift
// playhead-gard: WHICH DETECTOR produced a window, as a first-class value.
//
// Why this exists
// ---------------
// `podcast_profiles` carried ONE trust triple per show — `skipTrustScore`,
// `mode`, `recentFalseSkipSignals` — and `SkipOrchestrator` consulted it for
// every window regardless of provenance. Measured on the owner's device
// 2026-08-01: a show sat at trust 0.20 / `mode: manual` because of three vetoes
// of `segmentAggregated` windows (confidence 0.40–0.42, BOTH edges unanchored)
// that playhead-ynmk has since stopped from ever skipping. The aggregator went
// 0-for-3 — and in doing so it suppressed byte-exact rediff on the same show,
// a different instrument with a different error profile that went 2-for-2 on
// the same corpus.
//
// A 0.40-confidence detector's mistakes were gating a deterministic one. That
// is `feedback_certainty_tiered_skip_2026-07-17` — auto-skip high-certainty
// DAI, mark when uncertain, PER CLASS and not on one global threshold —
// missing from the trust layer. Dan's call, 2026-08-01: "per detector is good".
//
// This enum is the class. It is deliberately COARSE and deliberately derived
// from columns that are ALREADY PERSISTED on the `ad_windows` row
// (`boundaryState`, `startEdgeAnchor`, `endEdgeAnchor`), so classifying a
// window needs no new plumbing, no producer cooperation, and works on rows
// written before this bead existed.

import Foundation

// MARK: - SkipDetectorClass

/// The detector whose error history governs a window's skip eligibility.
///
/// Four classes, chosen because they are the four distinct ERROR PROFILES the
/// pipeline actually has — not because they are the four producers. A finer
/// partition would be untestable (most classes would never accumulate an
/// observation); a coarser one is the scalar this bead removes.
enum SkipDetectorClass: String, Sendable, Hashable, CaseIterable, Codable {

    /// The byte-exact rediff differ owns BOTH edges of this span
    /// (playhead-xsdz.44/57, narrowed to the byte arm by playhead-6qvf). The
    /// origin served different bytes here; that is a measurement, not a model
    /// output.
    case rediffByteExact

    /// `SegmentAggregator` fused sub-threshold per-window scores into a
    /// coherent segment (`AdBoundaryState.segmentAggregated`, playhead-0usd).
    /// The class that went 0-for-3 on the owner's device.
    case segmentAggregated

    /// The span carries a USER gesture in `boundaryState`
    /// (`UserSpanAssertion.userMarked` / `.userConfirmedSuggested`). Its
    /// eligibility is the user's own, and it is tracked separately so a
    /// detector never inherits credit or blame for a span a person drew.
    case userAsserted

    /// Everything else: lexical seeds, acoustic refinement, FM fusion, and any
    /// producer this enum does not know about. The conservative bucket — an
    /// unrecognised `boundaryState` lands here rather than in a class with a
    /// weaker gate.
    case fusion

    // MARK: Policy

    /// Whether this class's eligibility is governed by the SHOW's trust
    /// history at all.
    ///
    /// `false` for `.rediffByteExact`, and this is the decision the bead
    /// exists to take. Three reasons, in order of weight:
    ///
    ///  1. **The ladder measures the wrong thing for it.** `shadow → manual →
    ///     auto` exists to earn confidence that a STATISTICAL model behaves on
    ///     an unseen show. The byte differ is not a model; it reports what the
    ///     origin server did. There is nothing about the show to learn before
    ///     trusting the observation — which is exactly why the day-0 mint
    ///     (playhead-qs0d) fires on the FIRST listen, before any transcript or
    ///     analysis exists. A show-history gate would keep the one signal that
    ///     works on a first listen permanently mark-only.
    ///  2. **The show scalar is not evidence about it.** Dan's three vetoes
    ///     were of aggregator spans. Nothing in them is evidence about a byte
    ///     differ, and treating them as such is the defect.
    ///  3. **Trust was never what made a bad rediff edge safe.** A rediff span
    ///     still passes the eligibility gate, `AutoSkipEdgePadding` (which
    ///     refuses without an anchored start), the certainty tier — 6qvf's
    ///     chroma arm resolves `.unanchored` and stays markOnly — the enter
    ///     threshold, and seek suppression. Removing the show gate removes the
    ///     one input that was measuring a different detector.
    ///
    /// It is EXEMPT FROM THE SHOW'S HISTORY, NOT UNGOVERNED. `.rediffByteExact`
    /// keeps its own per-(show, detector) ledger entry and demotes on its own
    /// vetoes like every other class — see `DetectorTrustLedger`. Byte-exactness
    /// proves DIVERGENCE, not AD-NESS, and a show that serves personalised
    /// non-ad material (regional variants, dynamic host reads that the listener
    /// considers content) is a real failure mode the user must be able to shut
    /// off. A class nobody can demote would be the same shape of defect as one
    /// scalar for everybody, pointed the other way.
    var consultsShowTrust: Bool {
        switch self {
        case .rediffByteExact:
            return false
        case .segmentAggregated, .userAsserted, .fusion:
            return true
        }
    }

    /// The mode a class holds on a show whose ledger has nothing to say about
    /// it, when `consultsShowTrust` is `false`.
    ///
    /// Only `.rediffByteExact` reads this. Every other class seeds from the
    /// legacy per-show scalar (see `DetectorTrustLedger.seed(for:from:)`), so
    /// no show silently changes posture on upgrade except for the one class
    /// this bead deliberately releases.
    ///
    /// **playhead-u0vv: this is a FLOOR the class returns to, not only a seed
    /// it starts from.** As a pure seed it stopped applying the instant a
    /// listener touched the show at all: any attributed gesture materializes
    /// every class's entry (`TrustScoringService.applyFalseSkipSignal`), after
    /// which `DetectorTrustLedger.seed` is never consulted again for that show
    /// — so two vetoes demoted byte-exact rediff to `manual` with no way back,
    /// permanently disabling the one signal that works on a FIRST listen. See
    /// `DetectorModeAuthority` for the return trip and for why it is not the
    /// self-certification playhead-lqcp closed.
    static let showIndependentSeedMode: SkipMode = .auto

    // MARK: Classification

    /// Classify a persisted window from the three columns that already carry
    /// its provenance.
    ///
    /// Precedence, and why:
    ///  1. **User assertion first.** A gesture recorded in `boundaryState`
    ///     replaces the producer's label there, so nothing else is legible
    ///     anyway — and a span a person drew belongs to the person.
    ///  2. **Byte-exact rediff next**, recognised through the SHARED
    ///     definition: `ExtentAnchorTier.deterministic` on BOTH edges, which
    ///     post-6qvf is the byte arm and nothing else. Deriving it here from
    ///     the anchors rather than re-spelling `.rediffSlot` is the 6qvf lesson
    ///     — a second expression that happens to agree is how the certainty
    ///     tier and its consumers came apart.
    ///  3. **The aggregator's own `boundaryState`.**
    ///  4. **`.fusion`** for everything left, including unrecognised values.
    ///
    /// Pure and total. No actor state, no I/O, no defaults that depend on when
    /// it is called.
    static func classify(
        boundaryState: String,
        startAnchor: AutoSkipEdgeAnchor,
        endAnchor: AutoSkipEdgeAnchor
    ) -> SkipDetectorClass {
        if UserSpanAssertion(rawValue: boundaryState) != nil {
            return .userAsserted
        }
        let support = SpanExtentSupport(
            startAnchor: startAnchor,
            endAnchor: endAnchor
        )
        if support.tier == .deterministic {
            return .rediffByteExact
        }
        if AdBoundaryState(rawValue: boundaryState) == .segmentAggregated {
            return .segmentAggregated
        }
        return .fusion
    }
}

// MARK: - DetectorModeAuthority (playhead-u0vv)

/// WHO decided a detector class's mode — and therefore who may put it back
/// after the class's own evidence has stopped arguing otherwise.
///
/// This is the counterpart to `AutoPromotionConfidenceEvidence`, and it is
/// deliberately a SEPARATE type, because the two answer different questions and
/// letting them blur is how playhead-lqcp gets reopened by accident:
///
///   * `AutoPromotionConfidenceEvidence` asks **"has this show's own history
///     EARNED an auto?"** That is self-certification — a class accumulating its
///     own observations and declaring itself trustworthy — and lqcp closed it,
///     because Dan's ruling makes auto conditional on high confidence and no
///     quantity on this tree can evaluate that condition.
///   * This type asks **"who set this class's mode before the show had any
///     history at all, and does that decision still stand?"** The answer is a
///     CONSTANT written by a different authority — `showIndependentSeedMode`,
///     which encodes a property of the INSTRUMENT ("the byte differ reports
///     what the origin served; that is a measurement, not a model output") —
///     and nothing a class accumulates can change it.
///
/// **The structural reason a restoration cannot become a promotion.** The mode
/// a restoration produces is READ OFF THIS VALUE. `observationCount` and
/// `trustScore` are not inputs to it and cannot be: the only thing a class's
/// own history is allowed to do is WITHHOLD the restoration while the class
/// still owes veto evidence. So no amount of self-observation reaches a mode
/// nobody granted — which is precisely the property lqcp was protecting — and
/// re-opening the promotion rung would still be a source edit inside
/// `AutoPromotionConfidenceEvidence`, next to the reasons.
enum DetectorModeAuthority: Sendable, Equatable {
    /// `SkipDetectorClass.showIndependentSeedMode`, carried BY VALUE so that a
    /// consumer returns the authority's answer rather than one of its own. A
    /// retune of that constant therefore retunes the restoration with it; a
    /// consumer that hard-codes `.auto` is a defect this payload makes visible.
    case showIndependentSeed(SkipMode)

    /// The mode this authority declares. The only way to obtain a mode from an
    /// authority — there is no default and no fallback.
    var declaredMode: SkipMode {
        switch self {
        case .showIndependentSeed(let mode):
            return mode
        }
    }
}

extension SkipDetectorClass {
    /// The authority governing this class's mode, or `nil` when the mode is the
    /// SHOW's own trust history to decide.
    ///
    /// `nil` for every `consultsShowTrust == true` class, and that is the guard
    /// that keeps restoration to the one class the exemption was written for. A
    /// show-governed class has no authority above its own record: the ladder
    /// measures exactly the right thing for it, so there is nothing to restore
    /// it TO except a value it would have had to earn.
    var modeAuthority: DetectorModeAuthority? {
        guard !consultsShowTrust else { return nil }
        return .showIndependentSeed(Self.showIndependentSeedMode)
    }
}

extension AdWindow {
    /// The detector class of this row, read from its own persisted columns.
    ///
    /// `SkipOrchestrator` prefers the runtime-stamped anchors when it has them
    /// (`resolvedEdgeAnchors`), because ingest can know a provenance the row
    /// has not been rewritten with yet; this property is the row-only fallback
    /// and the one every non-orchestrator reader uses.
    var detectorClass: SkipDetectorClass {
        // playhead-bllt: through the SHARED row decode (`AdWindow
        // .extentSupport`), not a fourth local re-spelling of it. Behaviour is
        // unchanged — that property is the same expression this used to inline.
        let support = extentSupport
        return SkipDetectorClass.classify(
            boundaryState: boundaryState,
            startAnchor: support.startAnchor,
            endAnchor: support.endAnchor
        )
    }
}
