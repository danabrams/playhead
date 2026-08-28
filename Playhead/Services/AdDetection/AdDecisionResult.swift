// AdDecisionResult.swift
// Phase 6 (playhead-4my.6.3): Models for persisted decisions and append-only events.
// Phase 7 (playhead-4my.7.1): CorrectionSource enum + expanded CorrectionEvent.
//
// DecisionResultArtifact — per-asset persistence container (ad_decision_results table).
//   The runtime per-window view (AdDecisionResult in SkipOrchestrator.swift) is derived
//   by deserializing decisionJSON from this artifact.
// DecisionEvent, CorrectionEvent — append-only audit trails.

import Foundation

// MARK: - DecisionResultArtifact

/// Per-asset persistence container for fusion decisions (ad_decision_results table).
/// decisionJSON encodes the full array of per-window AdDecisionResult values.
struct DecisionResultArtifact: Sendable, Identifiable, Equatable {
    let id: String
    let analysisAssetId: String
    let decisionCohortJSON: String  // serialized DecisionCohort
    let inputArtifactRefs: String   // JSON array of input artifact refs
    let decisionJSON: String        // JSON: array of per-window decisions + gates
    let createdAt: Double
}

// MARK: - DecisionEvent

/// Append-only record of a single window-level decision.
struct DecisionEvent: Sendable, Identifiable, Equatable {
    let id: String
    let analysisAssetId: String
    let eventType: String
    let windowId: String
    let proposalConfidence: Double
    let skipConfidence: Double
    let eligibilityGate: String    // SkipEligibilityGate.rawValue
    let policyAction: String       // SkipPolicyAction.rawValue
    let decisionCohortJSON: String
    let createdAt: Double
    /// Structured explanation trace (playhead-ef2.1.4). Compact JSON encoding of
    /// `DecisionExplanation`. Nil for events created before this field was added.
    let explanationJSON: String?

    init(
        id: String,
        analysisAssetId: String,
        eventType: String,
        windowId: String,
        proposalConfidence: Double,
        skipConfidence: Double,
        eligibilityGate: String,
        policyAction: String,
        decisionCohortJSON: String,
        createdAt: Double,
        explanationJSON: String? = nil
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.eventType = eventType
        self.windowId = windowId
        self.proposalConfidence = proposalConfidence
        self.skipConfidence = skipConfidence
        self.eligibilityGate = eligibilityGate
        self.policyAction = policyAction
        self.decisionCohortJSON = decisionCohortJSON
        self.createdAt = createdAt
        self.explanationJSON = explanationJSON
    }
}

// MARK: - ProposalAuthority

/// Indicates whether a source's contribution was strong or weak relative to its cap.
enum ProposalAuthority: String, Sendable, Codable, Equatable {
    /// Weight exceeds half the source's cap — meaningful contributor.
    case strong
    /// Weight is below half the source's cap — marginal contributor.
    case weak
}

// MARK: - SourceEvidence

/// Per-source evidence breakdown for a single decision. Aggregated across all
/// ledger entries of the same source type.
struct SourceEvidence: Sendable, Codable, Equatable {
    /// Source type name (matches EvidenceSourceType.rawValue).
    let source: String
    /// Total aggregated weight from all entries of this source type.
    let weight: Double
    /// The cap that was applied to this source type.
    let capApplied: Double
    /// Whether this source's contribution was strong or weak relative to its cap.
    let authority: ProposalAuthority
}

// MARK: - ActionRationale

/// Links the threshold, gate, and policy to the final skip eligibility determination.
struct ActionRationale: Sendable, Codable, Equatable {
    /// The skip confidence threshold used for auto-skip promotion. Compared
    /// against the PRESENCE score only — extent never enters a threshold.
    let threshold: Double
    /// The eligibility gate value at decision time.
    let gate: String
    /// The policy action applied.
    let policyAction: String
    /// Whether the decision was ultimately skip-eligible.
    let skipEligible: Bool
    /// playhead-2350: EXTENT provenance of the leading edge
    /// (`AutoSkipEdgeAnchor.rawValue`). `nil` on rows written before the split
    /// existed. Recorded — never thresholded — so replay can tell a demoted
    /// "we know it's an ad but not where it ends" verdict apart from a
    /// low-presence one.
    let startEdgeAnchor: String?
    /// playhead-2350: EXTENT provenance of the trailing edge.
    let endEdgeAnchor: String?
    /// playhead-2350: whether BOTH edges were independently supported — the
    /// precondition for auto-skip. `nil` on pre-2350 rows.
    let extentFullyAnchored: Bool?

    init(
        threshold: Double,
        gate: String,
        policyAction: String,
        skipEligible: Bool,
        startEdgeAnchor: String? = nil,
        endEdgeAnchor: String? = nil,
        extentFullyAnchored: Bool? = nil
    ) {
        self.threshold = threshold
        self.gate = gate
        self.policyAction = policyAction
        self.skipEligible = skipEligible
        self.startEdgeAnchor = startEdgeAnchor
        self.endEdgeAnchor = endEdgeAnchor
        self.extentFullyAnchored = extentFullyAnchored
    }
}

// MARK: - DecisionExplanation

/// Structured explanation trace for a single fusion decision. Stored as compact JSON
/// in DecisionEvent.explanationJSON for QA, debugging, replay, and counterfactual
/// evaluation. Not user-facing.
struct DecisionExplanation: Sendable, Codable, Equatable {
    /// Per-source evidence breakdown with calibrated weights and authority.
    let evidenceBreakdown: [SourceEvidence]
    /// Which evidence families contributed to the final score (source type names).
    let contributingFamilies: [String]
    /// Links threshold/policy/gate to the skip eligibility outcome.
    let actionRationale: ActionRationale

    /// Build an explanation from the decision ledger and result.
    ///
    /// Aggregates ledger entries by source type, computes per-source authority
    /// relative to the configured cap, and produces the action rationale from
    /// the decision result and policy action.
    static func build(
        ledger: [EvidenceLedgerEntry],
        decision: DecisionResult,
        policyAction: SkipPolicyAction,
        config: FusionWeightConfig,
        skipThreshold: Double
    ) -> DecisionExplanation {
        // Aggregate weights per source type
        let scoringLedger = ledger.filter(\.contributesToAutomaticDecision)
        var weightBySource: [EvidenceSourceType: Double] = [:]
        for entry in scoringLedger {
            weightBySource[entry.source, default: 0.0] += entry.weight
        }

        // Stable sort order: follow EvidenceSourceType.allCases ordering
        let sortedSources = EvidenceSourceType.allCases.filter { weightBySource[$0] != nil }

        let breakdown: [SourceEvidence] = sortedSources.map { sourceType in
            let totalWeight = weightBySource[sourceType]!
            let cap = capForSource(sourceType, config: config)
            let authority: ProposalAuthority = totalWeight > cap * 0.5 ? .strong : .weak
            return SourceEvidence(
                source: sourceType.rawValue,
                weight: totalWeight,
                capApplied: cap,
                authority: authority
            )
        }

        let families = sortedSources.map { $0.rawValue }

        // playhead-2350: `skipEligible` deliberately carries no extra extent
        // term. When the gate is armed (the shipped default) an unanchored span
        // was already demoted off `.eligible` by
        // `DecisionResult.withExtentSupport`, so the check below is extent-aware
        // without restating the rule. With the gate disabled this can report
        // `skipEligible: true` beside `extentFullyAnchored: false` — which is
        // the honest trace of that configuration, not a contradiction: the
        // explanation records what the pipeline decided, and the anchors say why
        // it could have decided otherwise.
        let isSkipEligible = policyAction == .autoSkipEligible
            && decision.eligibilityGate == .eligible

        let rationale = ActionRationale(
            threshold: skipThreshold,
            gate: decision.eligibilityGate.rawValue,
            policyAction: policyAction.rawValue,
            skipEligible: isSkipEligible,
            startEdgeAnchor: decision.extentSupport.startAnchor.rawValue,
            endEdgeAnchor: decision.extentSupport.endAnchor.rawValue,
            extentFullyAnchored: decision.extentSupport.isFullyAnchored
        )

        return DecisionExplanation(
            evidenceBreakdown: breakdown,
            contributingFamilies: families,
            actionRationale: rationale
        )
    }

    /// Returns the configured cap for a given source type.
    private static func capForSource(_ source: EvidenceSourceType, config: FusionWeightConfig) -> Double {
        switch source {
        case .fm: return config.fmCap
        case .lexical: return config.lexicalCap
        case .acoustic: return config.acousticCap
        case .catalog: return config.catalogCap
        case .classifier: return config.classifierCap
        case .fingerprint: return config.fingerprintCap
        case .metadata: return config.metadataCap  // playhead-z3ch
        // playhead-2hpn: `.musicBed` now has its own cap (default 0.25)
        // so the scoped-music-bed-generalization boost (0.10 → 0.25)
        // is not silently truncated to `acousticCap = 0.20`. The
        // flag-OFF legacy path emits ≤ 0.20, so the higher cap is
        // byte-identical for that case; only the boost path benefits.
        case .musicBed: return config.musicBedCap
        case .breakAlignment: return config.breakAlignmentCap  // playhead-fqc8: independent budget from the RMS-drop family.
        case .lexicalAutoAd: return config.lexicalAutoAdCap  // playhead-xsdz.1: high-precision lexical auto-ad rule; own larger budget so a vetted combo can clear the qualified auto-skip threshold.
        case .audioForensics: return config.audioForensicsCap  // playhead-xsdz.8: composite boundary-discontinuity channel; one modest corroborative cap.
        case .crossEpisodeMemory: return config.crossEpisodeMemoryCap  // playhead-xsdz.9: cross-episode positive copy-alignment boost; modest corroborative cap, no qualified track.
        case .rhetoricalGrammar: return config.rhetoricalGrammarCap  // playhead-xsdz.12: rhetorical act-sequence grammar; modest text-derived corroborative cap, no qualified track.
        case .crossShowSyndication: return config.crossShowSyndicationCap  // playhead-xsdz.13: cross-show syndication footprint; modest cross-library corroborative cap, no qualified track.
        case .rediffConfirmed: return 0.0  // playhead-xsdz.62: deterministic byte-exact presence KIND marker; emitted weight-0 (no score budget) — it only increments the corroboration quorum's distinctKinds.count, never adds skip mass.
        case .fusedScore: return 1.0  // Fused score is post-aggregation; no per-source cap applies.
        case .audit, .operational: return 0.0  // Phase 11 observability rows are not fusion inputs.
        }
    }
}

// MARK: - CorrectionSource

/// The UI gesture or mechanism that produced a user correction.
enum CorrectionSource: String, Sendable, Codable, CaseIterable {
    /// User tapped "Listen" on a span that was auto-skipped, reverting the skip.
    case listenRevert
    /// User explicitly vetoed a span via "This isn't an ad".
    case manualVeto
    /// User reported a missed ad (false negative) — "Hearing an ad" button,
    /// transcript tap-to-mark, or (playhead-q6y3) "Always skip <sponsor> on
    /// this show", which asserts the same direction over a `.sponsorOnShow`
    /// scope instead of a span.
    case falseNegative
    /// Durable, private receipt for Yes on an already auto-skipped banner.
    case bannerAutoSkipConfirmed
    /// Durable, private receipt for No on an already auto-skipped banner
    /// CARD — a tap on the card itself, which is presented while the playhead
    /// is inside the window.
    case bannerAutoSkipDenied
    /// Durable, private receipt for No on a row of the PASSIVE MISSED-SKIP
    /// LIST (playhead-2d6i's `TranscriptPeekView` footer), for an auto-skip
    /// that fired with no banner host attached.
    ///
    /// WHY THIS IS A SEPARATE CASE AND NOT `bannerAutoSkipDenied`
    /// (playhead-nq8z). `playhead-bwxi` added
    /// `correction_events.playheadTimeAtCorrection` (V59) so that
    /// `position ∈ [span.start, span.end)` is a question a row can be ASKED —
    /// three `bannerAutoSkipConfirmed` rows for windows at 23, 56 and 71
    /// minutes were recorded on 2026-08-21 by a listener 87 seconds in, and
    /// nothing in the row said so. The list's whole premise is that the
    /// listener has moved on: a veto from it is made from wherever they are
    /// NOW, so its position is DELIBERATELY outside the span, and stamping the
    /// span instead would fabricate the containment the column exists to
    /// expose. But that left `bannerAutoSkipDenied` with `position ∉ span`
    /// carrying two readings — a list veto minutes later, which is exactly
    /// what the surface is for, and a card's No tapped after the playhead left
    /// the span (cards persist past the span end), which is the older, weaker
    /// one. A corpus reader could not separate them, and the entire value of
    /// V59 is that a corpus can be FILTERED rather than trusted wholesale.
    ///
    /// THE ASYMMETRY IS DELIBERATE AND THERE IS NO CONFIRM TWIN. The list
    /// offers no Yes: a listener who never saw a card never HEARD the ad — it
    /// was skipped — so a confirmation from there would be the strongest
    /// positive signal the trust system takes, recorded for audio they never
    /// reached. That is the 2026-08-21 incident verbatim.
    /// `MissedAutoSkipListWiringSourceCanaryTests` pins the absence. If a Yes
    /// from a list is ever wanted, it is a product decision and a new bead —
    /// not a case added here for symmetry.
    case missedAutoSkipListDenied
    /// Durable, private receipt for Yes on a suggest-tier banner.
    case bannerSuggestionConfirmed
    /// Durable, private receipt for No on a suggest-tier banner.
    case bannerSuggestionDenied

    /// How much this gesture is worth when two corrections claim the same
    /// material — HIGHER WINS.
    ///
    /// playhead-u45d. Two corrections over the same asset + span + kind share
    /// one `correction_events` identity (see
    /// `AnalysisStore.appendCorrectionEvent`), so exactly one `source` can
    /// describe that row. Before this rank the survivor was whichever gesture
    /// happened to arrive FIRST, which meant a listener who had once rewound
    /// through a span could never afterwards mark it "not an ad": the veto
    /// landed on the existing `listenRevert` row, bumped its audit counters,
    /// and left it a `listenRevert` — so `userVetoedTimeRanges` never saw it
    /// and the span stayed highlighted. That is this bead's reported symptom.
    ///
    /// The ladder, and the reasoning behind each rung:
    ///
    ///   3  `manualVeto` / `falseNegative` — the listener selected specific
    ///      transcript sentences and said what they are. The bounds are
    ///      THEIRS and the claim is unambiguous.
    ///   2  the four banner answers and the missed-skip list's veto — a tap
    ///      about a whole detected window, whose range came from the DETECTOR
    ///      rather than the listener.
    ///   1  `listenRevert` — INFERRED from a rewind-through. It may mean "not
    ///      an ad", or it may mean the listener simply wanted to hear it.
    ///
    /// Rank is deliberately not unique per case: the four banner answers are
    /// one rung because they are the same gesture pointed at different tiers.
    /// Equal rank means "no precedence", which the persistence layer resolves
    /// exactly as it did before this rank existed.
    ///
    /// `missedAutoSkipListDenied` joins that rung for the same reason
    /// (playhead-nq8z): it is the SAME claim about the SAME detector-provided
    /// range, made through a different surface. Rank answers "whose bounds are
    /// these", not "how long ago was the tap" — the tap's lateness is what
    /// `playheadTimeAtCorrection` records, and demoting the list veto for it
    /// would confuse the two. Nothing turns on the tie in practice: the two
    /// denial sources cannot collide, because `explicitReceiptIdentityKey`
    /// includes `source.rawValue` (so they are different identities) and a
    /// window that has been denied is `.reverted`, which
    /// `denyAutoSkippedBanner` refuses a second time.
    ///
    /// `falseNegative` sits at 3 as `manualVeto`'s opposite-signed twin — the
    /// same deliberate transcript assertion, pointed the other way. Its rank
    /// is currently inert, because `effectiveCorrectionType` is part of the
    /// dedupe identity and so an FN correction never collides with an FP one;
    /// it is stated anyway so the ladder is total and a future reader does not
    /// have to infer a missing rung.
    var fidelityRank: Int {
        switch self {
        case .manualVeto, .falseNegative:
            return 3
        case .bannerAutoSkipConfirmed,
             .bannerAutoSkipDenied,
             .missedAutoSkipListDenied,
             .bannerSuggestionConfirmed,
             .bannerSuggestionDenied:
            return 2
        case .listenRevert:
            return 1
        }
    }

    /// Explicit per-window answers are retained on-device for correctness and
    /// learning, but are never diagnostic-export material.
    ///
    /// READ THE NAME AS "EXPLICIT PRIVATE RECEIPT", NOT AS "CAME FROM A
    /// BANNER" (playhead-nq8z). `missedAutoSkipListDenied` comes from a LIST
    /// and is `true` here, and that is not a compromise — this flag is what
    /// three separate mechanisms key off, and the list veto belongs in all
    /// three:
    ///
    ///   • `AnalysisStore.appendCorrectionEvent` requires a non-empty
    ///     `explicitReceiptIdentityKey` for such a row, i.e. the v32 durable
    ///     per-window identity. `false` would give it the generic v23
    ///     three-column identity instead, and a list veto would dedupe against
    ///     an unrelated manual veto of the same span.
    ///   • `genericCrossSourceConflictSQL` refuses to rewrite a STORED source
    ///     in this set, which is what stops a later rank-3 gesture promoting a
    ///     private receipt into export material.
    ///   • `CorpusExporter.correctionLine` drops it. A veto is a private
    ///     answer about this listener's own episode whichever surface asked;
    ///     the diagnostic corpus never carried the other four and must not
    ///     start with this one.
    ///
    /// The name is left alone deliberately: renaming it touches six production
    /// readers and a source canary, for no behavioural gain, and bead scope is
    /// the ceiling.
    var isExplicitBannerFeedback: Bool {
        switch self {
        case .bannerAutoSkipConfirmed,
             .bannerAutoSkipDenied,
             .missedAutoSkipListDenied,
             .bannerSuggestionConfirmed,
             .bannerSuggestionDenied:
            return true
        case .listenRevert, .manualVeto, .falseNegative:
            return false
        }
    }
}

// MARK: - AutoSkipDenialSurface

/// WHICH SURFACE a "not an ad" answer about an already-applied automatic skip
/// was made from (playhead-nq8z).
///
/// One value, two legal spellings, and it exists so the choice is made by a
/// TYPE rather than by a literal at a call site. `denyAutoSkippedBanner` and
/// `AnalysisStore.persistDeniedAutoSkip` both take it and neither has a
/// DEFAULT: a defaulted surface is precisely how the next surface gets
/// recorded as a card, which is the ambiguity this bead exists to remove one
/// layer along. The parameter is required so the compiler walks every writer
/// to a decision.
///
/// It is deliberately NOT `CorrectionSource`. Passing the source directly
/// would let a caller hand that seam `.falseNegative`, which is nonsense the
/// store would then have to reject at runtime; here the two denial sources are
/// the only representable values.
enum AutoSkipDenialSurface: Sendable, Hashable, CaseIterable {
    /// `AdBannerView`'s No, on the card presented for this window.
    case card
    /// A row of the passive missed-auto-skip list (playhead-2d6i), answered
    /// from wherever the listener now is.
    case missedAutoSkipList

    /// The durable `correction_events.source` this surface writes.
    var correctionSource: CorrectionSource {
        switch self {
        case .card: return .bannerAutoSkipDenied
        case .missedAutoSkipList: return .missedAutoSkipListDenied
        }
    }
}

// MARK: - CorrectionKind

/// Distinguishes whether a correction is a false positive ("not an ad") or
/// false negative ("is an ad") report. Derived from the CorrectionSource.
enum CorrectionKind: Sendable {
    /// User says the system incorrectly flagged content as an ad.
    case falsePositive
    /// User says the system missed an ad that is currently playing.
    case falseNegative
}

extension CorrectionSource {
    /// The semantic kind of correction this source represents.
    var kind: CorrectionKind {
        switch self {
        case .listenRevert, .manualVeto,
             .bannerAutoSkipDenied, .missedAutoSkipListDenied,
             .bannerSuggestionDenied:
            return .falsePositive
        case .falseNegative,
             .bannerAutoSkipConfirmed, .bannerSuggestionConfirmed:
            return .falseNegative
        }
    }
}

extension CorrectionKind {
    /// Map to the persisted `CorrectionType` value. FP/FN `CorrectionType`
    /// cases share names with `CorrectionKind` — this extension centralises
    /// the mapping so call sites don't reimplement the switch.
    var correctionType: CorrectionType {
        switch self {
        case .falseNegative: return .falseNegative
        case .falsePositive: return .falsePositive
        }
    }
}

// MARK: - CorrectionEvent

/// Append-only record of a user correction. Schema owned here; Phase 7 writes to it.
///
/// Corrections are scoped — they may apply to an exact span, a sponsor across
/// all episodes of a podcast, or a phrase/campaign on a show. The scope is
/// serialized as a `CorrectionScope` string and stored in the `scope` column.
struct CorrectionEvent: Sendable, Equatable {
    /// UUID string for the event row.
    let id: String
    /// The analysisAssetId of the episode where the correction was made.
    let analysisAssetId: String
    /// Serialized `CorrectionScope` string (e.g. "exactSpan:asset123:10:20").
    let scope: String
    /// When the correction was recorded (seconds since epoch).
    let createdAt: Double
    /// The UI mechanism that generated this correction.
    let source: CorrectionSource?
    /// The podcast feed ID, if known at correction time.
    let podcastId: String?
    /// ef2.3.1: Semantic nature of the correction (FP/FN/boundary).
    let correctionType: CorrectionType?
    /// ef2.3.1: Pipeline component most responsible for the error.
    let causalSource: CausalSource?
    /// ef2.3.1: JSON-encoded CorrectionTargetRefs for downstream analysis.
    let targetRefs: CorrectionTargetRefs?
    /// playhead-hygc.1.6: Cumulative submission count for this semantic
    /// identity. `1` for a freshly-recorded correction; increments by 1
    /// every time the same logical correction is re-submitted (idempotent
    /// upsert at the persistence layer). `nil` for legacy rows that
    /// pre-date schema v23 and were never re-submitted post-migration —
    /// readers should treat `nil` as `1`.
    let submissionCount: Int?
    /// playhead-hygc.1.6: Wall-clock of the most recent submission of
    /// this semantic identity. Equal to `createdAt` for a fresh
    /// correction; updated on each idempotent upsert. `nil` for legacy
    /// rows; readers should treat `nil` as `createdAt`.
    let lastSeenAt: Double?
    /// Durable privacy discriminator for explicit banner receipts.
    ///
    /// Fresh in-memory events derive this from `source`. Persistence readers
    /// additionally set it when the v32 `correctionIdentityKey` is non-empty,
    /// so a damaged/unknown source string cannot turn a private receipt into
    /// diagnostic export material.
    let isPrivateExplicitFeedbackReceipt: Bool
    /// playhead-bwxi: WHERE THE LISTENER WAS when the tap happened, in episode
    /// seconds. `nil` for rows written before schema V59, and for corrections
    /// that do not come from a playback gesture.
    ///
    /// THE QUESTION THIS COLUMN EXISTS TO ANSWER. On 2026-08-21 four
    /// `bannerAutoSkipConfirmed` rows landed on asset 0FF7EFF3 within six
    /// seconds of each other, for windows at 0, 1370, 3367 and 4279 s. Three of
    /// them were confirmations of audio the listener had not reached — the
    /// banner was presenting the whole episode at once (fixed in the same
    /// bead). Nothing in the row said so: `createdAt` is wall clock, `scope`
    /// and `targetRefs` carry the WINDOW's span, and the listener's own
    /// position was recorded nowhere. So a confirmation made after hearing and
    /// one made seventy minutes early were the same row, and every historical
    /// `bannerAutoSkipConfirmed` is unfalsifiable in both directions.
    ///
    /// With this, `playheadTimeAtCorrection ∈ [span.start, span.end)` is
    /// checkable per row, and a corpus can be filtered on it rather than
    /// trusted wholesale. Deliberately NOT part of the correction identity —
    /// it is evidence about the gesture, not a discriminator between gestures,
    /// and folding it into the key would turn one re-submitted correction into
    /// several rows.
    let playheadTimeAtCorrection: Double?
    /// Exact v32 persistence discriminator when this event was loaded from
    /// SQLite. In-memory events leave it nil and compute their identity from
    /// source/targets. Keeping the stored key lets read-side dedupe honor the
    /// durable identity even when those descriptive columns are damaged.
    let persistedCorrectionIdentityKey: String?

    init(
        id: String = UUID().uuidString,
        analysisAssetId: String,
        scope: String,
        createdAt: Double = Date().timeIntervalSince1970,
        source: CorrectionSource? = nil,
        podcastId: String? = nil,
        correctionType: CorrectionType? = nil,
        // playhead-bwxi: declared HERE, between `correctionType` and
        // `causalSource`, so every existing labelled call site can add it
        // without reordering anything else.
        playheadTimeAtCorrection: Double? = nil,
        causalSource: CausalSource? = nil,
        targetRefs: CorrectionTargetRefs? = nil,
        submissionCount: Int? = nil,
        lastSeenAt: Double? = nil,
        isPrivateExplicitFeedbackReceipt: Bool? = nil,
        persistedCorrectionIdentityKey: String? = nil
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.scope = scope
        self.createdAt = createdAt
        self.source = source
        self.podcastId = podcastId
        self.correctionType = correctionType
        self.causalSource = causalSource
        self.targetRefs = targetRefs
        self.submissionCount = submissionCount
        self.lastSeenAt = lastSeenAt
        // playhead-bwxi: a non-finite position is no position. A NaN would
        // persist as a REAL that compares false against every span in both
        // directions, which is exactly the "unfalsifiable" state this column
        // exists to remove.
        self.playheadTimeAtCorrection =
            playheadTimeAtCorrection.flatMap { $0.isFinite ? $0 : nil }
        self.persistedCorrectionIdentityKey =
            persistedCorrectionIdentityKey
        self.isPrivateExplicitFeedbackReceipt =
            isPrivateExplicitFeedbackReceipt == true
                || persistedCorrectionIdentityKey?.isEmpty == false
                || source?.isExplicitBannerFeedback == true
    }

    static func == (lhs: Self, rhs: Self) -> Bool {
        lhs.id == rhs.id
            && lhs.analysisAssetId == rhs.analysisAssetId
            && lhs.scope == rhs.scope
            && lhs.createdAt == rhs.createdAt
            && lhs.source == rhs.source
            && lhs.podcastId == rhs.podcastId
            && lhs.correctionType == rhs.correctionType
            && lhs.causalSource == rhs.causalSource
            && lhs.targetRefs == rhs.targetRefs
            && lhs.submissionCount == rhs.submissionCount
            && lhs.lastSeenAt == rhs.lastSeenAt
            && lhs.isPrivateExplicitFeedbackReceipt
                == rhs.isPrivateExplicitFeedbackReceipt
            && lhs.playheadTimeAtCorrection == rhs.playheadTimeAtCorrection
    }
}
