// FusionLiftHarnessSupport.swift
// playhead-au2v.1.27 — Phase C: hermetic support helpers for the
// env-gated Mac Catalyst chapter-fusion A/B harness
// (`ChapterFusionLiftABTests`).
//
// This is test-target-only code (it lives alongside `FusionLiftScoring.swift`
// in PlayheadTests, so it never bloats the shipped app binary). The harness
// itself runs the REAL `AdDetectionService.runBackfill` against real audio +
// Foundation Models on Mac Catalyst — that part is NOT hermetic and is gated
// behind `PLAYHEAD_CHAPTER_FUSION_LIFT_AB=1`. Everything in THIS file is
// pure value-shuffling extracted from the harness so it can be unit-tested
// on the simulator with no audio / FM / live-pipeline dependency:
//
//   1. `FusionLiftTranscriptVersion` — the transcript-version derivation
//      wrapper. It must reproduce EXACTLY the `transcriptVersion` that
//      `AdDetectionService.runBackfill` computes internally, because the
//      treatment run's chapter plan is cached under that key and the
//      Phase-B wire-in reads it back under the same key. A mismatch silently
//      degenerates the treatment arm into the baseline (a false-zero lift) —
//      so this derivation is load-bearing and gets its own tests.
//   2. `FusionLiftModeAccumulator` — per-mode (off / enabled) accumulation
//      of ground-truth + detected spans across the 12 episodes, folded into
//      a single `MetricsBatch` per mode via Phase A's greedy IoU pairing.
//   3. `FusionLiftReport` — the readable lift-table formatting (off vs
//      enabled precision / recall / F1 + deltas) plus the git-ignored JSON
//      summary payload.
//
// Scoring is NOT reimplemented here: the bridges (`MetricGroundTruthAd` /
// `MetricDetectedAd`), `SpanF1`, and `FusionLiftResult` all come from
// Phase A's `FusionLiftScoring.swift`.

import Foundation
@testable import Playhead

// MARK: - Transcript-version derivation wrapper

/// Reproduces the `transcriptVersion` that `AdDetectionService.runBackfill`
/// derives internally from a chunk set.
///
/// CHURN RISK #1 (transcript-hash mismatch → silent plan eviction → false
/// zero): the chapter-generation phase writes its `ChapterPlan` into the
/// `ChapterPlanCache` under whatever hash its injected
/// `TranscriptHashProviding` returns, and the Phase-B wire-in
/// (`resolveChapterEvidenceForShadowPhase`) reads the plan back under
/// `version.transcriptVersion`. For the treatment arm to actually steer the
/// CoveragePlanner, BOTH keys must be identical. The only way to guarantee
/// that is to derive the sticky hash from the SAME chunks, with the SAME
/// normalization/source hashes, AND the SAME `pass == "final"` pre-filter
/// that `runBackfill` applies before atomizing.
///
/// `runBackfill` does (verbatim, AdDetectionService.swift ~:1900):
///   ```
///   let finalChunks = { let f = chunks.filter { $0.pass == "final" }
///                       return f.isEmpty ? chunks : f }()
///   let (_, transcriptVersion) = TranscriptAtomizer.atomize(
///       chunks: finalChunks, analysisAssetId: …,
///       normalizationHash: "norm-v1", sourceHash: "asr-v1")
///   ```
/// This wrapper mirrors that exactly so the harness's sticky provider and
/// the wire-in agree on the cache key.
enum FusionLiftTranscriptVersion {

    /// The `pass` value `runBackfill` treats as the canonical transcript.
    static let finalPass = "final"
    /// Normalization hash `runBackfill` stamps into the atomizer.
    static let normalizationHash = "norm-v1"
    /// Source (ASR) hash `runBackfill` stamps into the atomizer.
    static let sourceHash = "asr-v1"

    /// Apply the SAME `pass == "final"` pre-filter `runBackfill` uses:
    /// keep only final-pass chunks, but if that leaves nothing, fall back
    /// to the full set (so an all-non-final transcript still hashes to a
    /// stable, non-empty version rather than the empty-input version).
    static func finalChunks(from chunks: [TranscriptChunk]) -> [TranscriptChunk] {
        let filtered = chunks.filter { $0.pass == finalPass }
        return filtered.isEmpty ? chunks : filtered
    }

    /// Derive the `transcriptVersion` string for a chunk set, matching
    /// `AdDetectionService.runBackfill`'s internal derivation byte-for-byte.
    static func derive(
        chunks: [TranscriptChunk],
        analysisAssetId: String
    ) -> String {
        let (_, version) = TranscriptAtomizer.atomize(
            chunks: finalChunks(from: chunks),
            analysisAssetId: analysisAssetId,
            normalizationHash: normalizationHash,
            sourceHash: sourceHash
        )
        return version.transcriptVersion
    }
}

// MARK: - Per-mode accumulation

/// The two arms of the A/B: chapter signal OFF (baseline) vs ENABLED
/// (treatment). Both arms run with `fmBackfillMode: .full` — only
/// `chapterSignalMode` varies. `.off` is the production default;
/// `.enabled` threads the inferred `ChapterPlan` into the CoveragePlanner.
enum FusionLiftArm: String, Sendable, CaseIterable {
    case off
    case enabled
}

// MARK: - Lexical-scorer A/B arm configuration (playhead-xsdz.liveab)

/// The arms of the lexical-scorer live A/B (`LexicalScorerLiveABTests`).
/// Unlike the chapter A/B above, every arm here keeps `chapterSignalMode:
/// .off` and `fmBackfillMode: .full`; the program under test is the shipped
/// lexical-scorer trio:
///   * xsdz.1 — the lexical-auto-ad rule, gated post-xsdz.6 by the
///     `AdDetectionConfig.lexicalAutoAdEnabled` BOOLEAN (`true` = on builds
///     the `.lexicalAutoAd` ledger entry; `false` = off, the production
///     default, skips it entirely — removing both the auto-skip track and the
///     entry's fusion mass). NOTE: raising `lexicalAutoAdQualifiedThreshold`
///     no longer disables the rule (the entry would still be built), so the
///     threshold is held at the production default across every arm.
///   * xsdz.2 — inward lexical-cluster region tightening in
///     `TargetedWindowNarrower`, gated by
///     `NarrowingConfig.lexicalClusterSnapEnabled`.
///   * xsdz.3 — lexically-nominated audit windows in the same narrower,
///     gated by the SAME `lexicalClusterSnapEnabled` flag.
///
/// Each arm is defined by exactly TWO orthogonal toggles:
///   * `xsdz1On` → the `lexicalAutoAdEnabled` flag (on = true, off = false).
///   * `xsdz23On` → the `lexicalClusterSnapEnabled` flag (on = true, off =
///     false). xsdz.2 and xsdz.3 share this one flag and CANNOT be separated
///     without new production plumbing, so they always move together as a
///     single arm leg.
///
/// The two endpoints (`baseline` = both off, `alon` = both on) are the
/// cumulative A/B; the two singletons (`xsdz1only`, `xsdz23only`) isolate
/// which feature regresses live ad detection in a single Catalyst run.
/// Post-xsdz.6 the production default is `xsdz23only` (xsdz.1 off, snap on).
enum LexicalScorerArm: String, Sendable, CaseIterable {
    /// Program OFF: xsdz.1 disabled (`lexicalAutoAdEnabled: false`) AND
    /// xsdz.2/.3 disabled (`lexicalClusterSnapEnabled: false`). The
    /// acoustic-only, no-auto-ad A/B baseline — the reference every other
    /// arm's delta is measured from.
    case baseline
    /// xsdz.1 ONLY: lexical-auto-ad rule on (`lexicalAutoAdEnabled: true`),
    /// region-tighten + audit-nominate off (`lexicalClusterSnapEnabled:
    /// false`). Isolates the auto-ad rule's contribution.
    case xsdz1only
    /// xsdz.2+xsdz.3 ONLY: region-tighten + audit-nominate on
    /// (`lexicalClusterSnapEnabled: true`), auto-ad rule off
    /// (`lexicalAutoAdEnabled: false`). Isolates the narrower's contribution.
    /// The two share one flag so they move together. Post-xsdz.6 this arm IS
    /// the production default.
    case xsdz23only
    /// All on: production defaults PLUS the xsdz.1 rule re-enabled
    /// (`lexicalAutoAdEnabled: true`, `lexicalClusterSnapEnabled: true`) — the
    /// cumulative-treatment endpoint.
    case alon

    /// Whether the xsdz.1 lexical-auto-ad rule is enabled in this arm (drives
    /// the `lexicalAutoAdEnabled` flag).
    var xsdz1On: Bool {
        switch self {
        case .baseline, .xsdz23only: return false
        case .xsdz1only, .alon: return true
        }
    }

    /// Whether the xsdz.2/.3 lexical-cluster snap (region-tighten +
    /// audit-nominate) is enabled in this arm (drives
    /// `lexicalClusterSnapEnabled`).
    var xsdz23On: Bool {
        switch self {
        case .baseline, .xsdz1only: return false
        case .xsdz23only, .alon: return true
        }
    }

    /// Back-compat: the single-boolean "program on" used by the cumulative
    /// (2-endpoint) view. Only the two endpoints map to a single program
    /// state; the singletons are mixed, so `programOn` is intentionally
    /// undefined for them (they are not "the program", they are one leg).
    var programOn: Bool { xsdz1On && xsdz23On }
}

/// Pure, hermetic builder for the arms' configs. Extracted from the harness
/// so the arm construction is unit-testable on the simulator with no audio /
/// FM / pipeline (acceptance criterion: a hermetic test for the non-trivial
/// config helper, plus the per-arm isolation guard). The harness then
/// injects:
///   * `adDetectionConfig(...)` into `AdDetectionService` (the
///     `lexicalAutoAdEnabled` flag toggles xsdz.1), and
///   * `narrowingConfig(...)` into the `BackfillJobRunner` it constructs in
///     its live runner factory (the `lexicalClusterSnapEnabled` flag toggles
///     xsdz.2/.3).
///
/// Each arm is defined by exactly TWO orthogonal toggles — `xsdz1On` (the
/// auto-ad threshold) and `xsdz23On` (the cluster-snap flag). The primitive
/// builders below take those two booleans directly; the `programOn:` overloads
/// are thin shims for the cumulative-endpoint (both-on / both-off) view used by
/// the back-compat helpers and the chapter A/B.
///
/// CRITICAL invariant (asserted by the unit tests): each arm deviates from
/// `.default` ONLY in its two program gates — the xsdz.1 gate
/// (`AdDetectionConfig.lexicalAutoAdEnabled`) and the xsdz.2/.3 gate
/// (`NarrowingConfig.lexicalClusterSnapEnabled`). Each toggle moves exactly
/// one field; every non-gate field stays equal to `.default` across all arms.
/// A mislabeled arm (a toggle wired to the wrong field) would attribute a
/// regression to the wrong feature — so the isolation is the load-bearing
/// correctness property the hermetic tests pin.
///
/// Post-playhead-xsdz.6 the production default is xsdz.1 OFF
/// (`lexicalAutoAdEnabled == false`) + xsdz.2/.3 ON
/// (`lexicalClusterSnapEnabled == true`), so the arm that matches production
/// is `xsdz23only`, NOT `alon`. The xsdz.1 gate is the `lexicalAutoAdEnabled`
/// BOOLEAN — not a threshold: after xsdz.6 the rule's ledger entry is only
/// built when the flag is `true`, so raising `lexicalAutoAdQualifiedThreshold`
/// no longer disables the rule (the entry, and its fusion mass, would still be
/// built). The harness therefore holds `lexicalAutoAdQualifiedThreshold` at the
/// production default (0.50) across every arm and toggles the boolean alone.
enum LexicalScorerArmConfig {

    /// The `lexicalAutoAdEnabled` flag for the xsdz.1 toggle: `true` builds the
    /// high-precision `.lexicalAutoAd` ledger entry (rule active), `false` (the
    /// production default since xsdz.6) skips it entirely — removing both the
    /// `lexicalAutoAdQualified` auto-skip track and the entry's fusion mass.
    static func lexicalAutoAdEnabled(xsdz1On: Bool) -> Bool { xsdz1On }

    /// The `NarrowingConfig` for the xsdz.2/.3 toggle. ON is
    /// `NarrowingConfig.default` verbatim. OFF is the same shape with ONLY
    /// `lexicalClusterSnapEnabled` flipped to `false` — mirrors the baseline
    /// config `RegionTighteningCorpusEvalTests` uses, and the per-anchor /
    /// cap / acoustic-snap fields are kept equal to `.default` so the ONLY
    /// difference this toggle makes is the lexical-cluster gate.
    static func narrowingConfig(xsdz23On: Bool) -> NarrowingConfig {
        if xsdz23On {
            return .default
        }
        let base = NarrowingConfig.default
        return NarrowingConfig(
            perAnchorPaddingSegments: base.perAnchorPaddingSegments,
            maxNarrowedSegmentsPerPhase: base.maxNarrowedSegmentsPerPhase,
            acousticBreakSnapMaxDistanceSeconds: base.acousticBreakSnapMaxDistanceSeconds,
            lexicalClusterSnapEnabled: false,
            lexicalClusterGapSeconds: base.lexicalClusterGapSeconds,
            lexicalClusterMarginSegments: base.lexicalClusterMarginSegments,
            lexicalClusterMinHits: base.lexicalClusterMinHits
        )
    }

    /// Build the full `AdDetectionConfig` for an arm. Every field other than
    /// `lexicalAutoAdEnabled` is held identical across arms (and to the values
    /// the chapter A/B uses) — including `lexicalAutoAdQualifiedThreshold` at
    /// its production default — with `fmBackfillMode: .full` and
    /// `chapterSignalMode: .off` so the FM scan runs and feeds the fusion
    /// ledger but the chapter signal stays out of the way.
    static func adDetectionConfig(xsdz1On: Bool) -> AdDetectionConfig {
        AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "xsdz.liveab",
            fmBackfillMode: .full,
            lexicalAutoAdEnabled: lexicalAutoAdEnabled(xsdz1On: xsdz1On),
            chapterSignalMode: .off
        )
    }

    // MARK: Per-arm builders

    /// The `AdDetectionConfig` for a sweep arm — reads the arm's xsdz.1 toggle.
    static func adDetectionConfig(for arm: LexicalScorerArm) -> AdDetectionConfig {
        adDetectionConfig(xsdz1On: arm.xsdz1On)
    }

    /// The `NarrowingConfig` for a sweep arm — reads the arm's xsdz.2/.3 toggle.
    static func narrowingConfig(for arm: LexicalScorerArm) -> NarrowingConfig {
        narrowingConfig(xsdz23On: arm.xsdz23On)
    }

    // MARK: Cumulative-endpoint shims (back-compat)

    /// Cumulative-endpoint xsdz.1 gate: both gates move together off `programOn`.
    static func lexicalAutoAdEnabled(programOn: Bool) -> Bool {
        lexicalAutoAdEnabled(xsdz1On: programOn)
    }

    /// Cumulative-endpoint `NarrowingConfig`: both gates move together off
    /// `programOn`.
    static func narrowingConfig(programOn: Bool) -> NarrowingConfig {
        narrowingConfig(xsdz23On: programOn)
    }

    /// Cumulative-endpoint `AdDetectionConfig`: both gates move together off
    /// `programOn`.
    static func adDetectionConfig(programOn: Bool) -> AdDetectionConfig {
        adDetectionConfig(xsdz1On: programOn)
    }
}

// MARK: - Evidence-Fragility gate A/B arm configuration (playhead-xsdz.7 live A/B)

/// The two arms of the Evidence-Fragility live A/B (`FragilityGateLiveABTests`).
/// Both arms run the REAL `AdDetectionService.runBackfill` with
/// `fmBackfillMode: .full` (FM ad scan → fusion ledger → `insertAdWindows`),
/// `chapterSignalMode: .off`, ALL off-by-default evidence channels FALSE, and
/// `NarrowingConfig.default` (xsdz.2/.3 cluster-snap ON — production state).
/// The ONLY field that differs between the arms is the xsdz.7
/// `AdDetectionConfig.evidenceFragilityPenaltyEnabled` boolean:
///   * `baseline`  — current main PRODUCTION state. `evidenceFragilityPenaltyEnabled:
///     false`, so `applyFragilityPenalty` returns `skipConfidence` UNCHANGED.
///   * `treatment` — identical to baseline EXCEPT `evidenceFragilityPenaltyEnabled:
///     true`, with the production-default `fragilityThreshold` (2.0) and
///     `fragilityPenalty` (0.85). The soft, post-fusion precision gate is live.
///
/// This is the load-bearing correctness property of the A/B: the two arms must
/// differ in EXACTLY one field. A second field drifting between arms would
/// attribute a precision/recall change to the fragility gate that some other
/// flag actually caused, making the measurement meaningless. The hermetic
/// `FragilityGateArmConfigTests` pin the one-field isolation on the simulator
/// before the (expensive, Catalyst-only) live A/B ever runs.
enum FragilityGateArm: String, Sendable, CaseIterable {
    /// Current main production state: fragility gate OFF.
    case baseline
    /// Production state PLUS the xsdz.7 fragility gate enabled.
    case treatment

    /// Whether `evidenceFragilityPenaltyEnabled` is set in this arm — the ONLY
    /// `AdDetectionConfig` field that distinguishes the two arms.
    var fragilityEnabled: Bool {
        switch self {
        case .baseline: return false
        case .treatment: return true
        }
    }
}

/// Pure, hermetic builder for the fragility A/B's per-arm configs. Extracted
/// from the harness so the arm construction is unit-testable on the simulator
/// with no audio / FM / pipeline (acceptance criterion: a hermetic test that
/// pins the one-field isolation).
///
/// CRITICAL invariant (asserted by `FragilityGateArmConfigTests`): the two arms
/// deviate from each other ONLY in `evidenceFragilityPenaltyEnabled`; every
/// other `AdDetectionConfig` field is byte-identical and equal to the PRODUCTION
/// default (`AdDetectionConfig.default`) — including `fmBackfillMode: .full`,
/// `chapterSignalMode: .off`, every off-by-default evidence-channel flag FALSE,
/// and `lexicalAutoAdQualifiedThreshold` at its production default. Both arms
/// also use `NarrowingConfig.default` (snap ON — xsdz.2/.3 are KEPT ON in
/// production, so the baseline MUST have snap on). The fragility tuning knobs
/// (`fragilityThreshold` / `fragilityPenalty`) stay at their production defaults
/// in BOTH arms; the treatment arm differs only by flipping the master flag on.
enum FragilityGateArmConfig {

    /// Build the full `AdDetectionConfig` for the fragility A/B given the single
    /// `evidenceFragilityPenaltyEnabled` toggle. Every other field is copied
    /// VERBATIM from `AdDetectionConfig.default` (the production state), so the
    /// only thing this builder can vary is the fragility master flag. Sourcing
    /// every non-toggle field from `.default` (rather than re-typing literals)
    /// means the baseline arm tracks production automatically if a default ever
    /// changes — the harness can never silently drift from production.
    static func adDetectionConfig(fragilityEnabled: Bool) -> AdDetectionConfig {
        let p = AdDetectionConfig.default
        return AdDetectionConfig(
            candidateThreshold: p.candidateThreshold,
            confirmationThreshold: p.confirmationThreshold,
            suppressionThreshold: p.suppressionThreshold,
            hotPathLookahead: p.hotPathLookahead,
            detectorVersion: p.detectorVersion,
            fmBackfillMode: p.fmBackfillMode,
            fmScanBudgetSeconds: p.fmScanBudgetSeconds,
            fmConsensusThreshold: p.fmConsensusThreshold,
            markOnlyThreshold: p.markOnlyThreshold,
            autoSkipConfidenceThreshold: p.autoSkipConfidenceThreshold,
            classifierSeedQualifiedThreshold: p.classifierSeedQualifiedThreshold,
            lexicalAutoAdQualifiedThreshold: p.lexicalAutoAdQualifiedThreshold,
            lexicalAutoAdEnabled: p.lexicalAutoAdEnabled,
            segmentUICandidateThreshold: p.segmentUICandidateThreshold,
            segmentAutoSkipThreshold: p.segmentAutoSkipThreshold,
            bracketRefinementEnabled: p.bracketRefinementEnabled,
            bracketRefinementMinTrust: p.bracketRefinementMinTrust,
            bracketRefinementMinCoarseScore: p.bracketRefinementMinCoarseScore,
            bracketRefinementMinFineConfidence: p.bracketRefinementMinFineConfidence,
            transcriptBoundaryCueEnabled: p.transcriptBoundaryCueEnabled,
            // THE ONLY between-arm difference.
            evidenceFragilityPenaltyEnabled: fragilityEnabled,
            // Tuning knobs held at the production default in BOTH arms.
            fragilityThreshold: p.fragilityThreshold,
            fragilityPenalty: p.fragilityPenalty,
            chapterSignalMode: p.chapterSignalMode,
            audioForensicsEnabled: p.audioForensicsEnabled,
            crossEpisodeMemoryEnabled: p.crossEpisodeMemoryEnabled,
            rhetoricalGrammarEnabled: p.rhetoricalGrammarEnabled,
            crossShowSyndicationEnabled: p.crossShowSyndicationEnabled,
            temporalRegularizationEnabled: p.temporalRegularizationEnabled,
            temporalNeighborWindowSeconds: p.temporalNeighborWindowSeconds,
            temporalHighConfidenceNeighborThreshold: p.temporalHighConfidenceNeighborThreshold,
            temporalIsolationPenaltyFactor: p.temporalIsolationPenaltyFactor,
            temporalMinDwellSeconds: p.temporalMinDwellSeconds,
            temporalMinDwellPenaltyFactor: p.temporalMinDwellPenaltyFactor,
            perShowThresholdControlEnabled: p.perShowThresholdControlEnabled,
            perShowThresholdProportionalGain: p.perShowThresholdProportionalGain,
            perShowThresholdIntegralGain: p.perShowThresholdIntegralGain,
            perShowThresholdMaxOffset: p.perShowThresholdMaxOffset,
            perShowThresholdMinSamples: p.perShowThresholdMinSamples
        )
    }

    /// The `AdDetectionConfig` for a fragility A/B arm — reads the arm's single
    /// `fragilityEnabled` toggle.
    static func adDetectionConfig(for arm: FragilityGateArm) -> AdDetectionConfig {
        adDetectionConfig(fragilityEnabled: arm.fragilityEnabled)
    }

    /// The `NarrowingConfig` for BOTH arms: `NarrowingConfig.default` (snap ON).
    /// xsdz.2/.3 are KEPT ON in production, so the baseline keeps snap on too —
    /// the fragility flag is the only difference, NOT the narrowing config.
    /// Identical across arms; exposed so the harness wires the same value into
    /// both arms' live runner factories and the isolation test can assert it.
    static func narrowingConfig(for arm: FragilityGateArm) -> NarrowingConfig {
        _ = arm // both arms use the production default — narrowing does not vary
        return .default
    }

    /// The exhaustive list of the `AdDetectionConfig` fields the isolation test
    /// compares pairwise across the two arms. `AdDetectionConfig` is not
    /// `Equatable`, so the isolation test enumerates fields explicitly; this
    /// closure-keyed approach keeps that enumeration in ONE place. Each entry is
    /// `(field name, extractor)`; the test asserts every NON-fragility field is
    /// equal across arms. Returning `String(describing:)` lets a single helper
    /// compare heterogeneous field types without per-type boilerplate. The
    /// extractors are `@Sendable` so the static array is concurrency-safe.
    static let comparableFields: [(name: String, value: @Sendable (AdDetectionConfig) -> String)] = [
        ("candidateThreshold", { String(describing: $0.candidateThreshold) }),
        ("confirmationThreshold", { String(describing: $0.confirmationThreshold) }),
        ("suppressionThreshold", { String(describing: $0.suppressionThreshold) }),
        ("hotPathLookahead", { String(describing: $0.hotPathLookahead) }),
        ("detectorVersion", { $0.detectorVersion }),
        ("fmBackfillMode", { String(describing: $0.fmBackfillMode) }),
        ("fmScanBudgetSeconds", { String(describing: $0.fmScanBudgetSeconds) }),
        ("fmConsensusThreshold", { String(describing: $0.fmConsensusThreshold) }),
        ("markOnlyThreshold", { String(describing: $0.markOnlyThreshold) }),
        ("autoSkipConfidenceThreshold", { String(describing: $0.autoSkipConfidenceThreshold) }),
        ("classifierSeedQualifiedThreshold", { String(describing: $0.classifierSeedQualifiedThreshold) }),
        ("lexicalAutoAdQualifiedThreshold", { String(describing: $0.lexicalAutoAdQualifiedThreshold) }),
        ("lexicalAutoAdEnabled", { String(describing: $0.lexicalAutoAdEnabled) }),
        ("segmentUICandidateThreshold", { String(describing: $0.segmentUICandidateThreshold) }),
        ("segmentAutoSkipThreshold", { String(describing: $0.segmentAutoSkipThreshold) }),
        ("bracketRefinementEnabled", { String(describing: $0.bracketRefinementEnabled) }),
        ("bracketRefinementMinTrust", { String(describing: $0.bracketRefinementMinTrust) }),
        ("bracketRefinementMinCoarseScore", { String(describing: $0.bracketRefinementMinCoarseScore) }),
        ("bracketRefinementMinFineConfidence", { String(describing: $0.bracketRefinementMinFineConfidence) }),
        ("transcriptBoundaryCueEnabled", { String(describing: $0.transcriptBoundaryCueEnabled) }),
        // evidenceFragilityPenaltyEnabled is INTENTIONALLY excluded — it is the
        // one field allowed to differ across arms.
        ("fragilityThreshold", { String(describing: $0.fragilityThreshold) }),
        ("fragilityPenalty", { String(describing: $0.fragilityPenalty) }),
        ("chapterSignalMode", { String(describing: $0.chapterSignalMode) }),
        ("audioForensicsEnabled", { String(describing: $0.audioForensicsEnabled) }),
        ("crossEpisodeMemoryEnabled", { String(describing: $0.crossEpisodeMemoryEnabled) }),
        ("rhetoricalGrammarEnabled", { String(describing: $0.rhetoricalGrammarEnabled) }),
        ("crossShowSyndicationEnabled", { String(describing: $0.crossShowSyndicationEnabled) }),
        ("temporalRegularizationEnabled", { String(describing: $0.temporalRegularizationEnabled) }),
        ("temporalNeighborWindowSeconds", { String(describing: $0.temporalNeighborWindowSeconds) }),
        ("temporalHighConfidenceNeighborThreshold", { String(describing: $0.temporalHighConfidenceNeighborThreshold) }),
        ("temporalIsolationPenaltyFactor", { String(describing: $0.temporalIsolationPenaltyFactor) }),
        ("temporalMinDwellSeconds", { String(describing: $0.temporalMinDwellSeconds) }),
        ("temporalMinDwellPenaltyFactor", { String(describing: $0.temporalMinDwellPenaltyFactor) }),
        ("perShowThresholdControlEnabled", { String(describing: $0.perShowThresholdControlEnabled) }),
        ("perShowThresholdProportionalGain", { String(describing: $0.perShowThresholdProportionalGain) }),
        ("perShowThresholdIntegralGain", { String(describing: $0.perShowThresholdIntegralGain) }),
        ("perShowThresholdMaxOffset", { String(describing: $0.perShowThresholdMaxOffset) }),
        ("perShowThresholdMinSamples", { String(describing: $0.perShowThresholdMinSamples) }),
    ]
}

/// Accumulates ground-truth and detected ad spans across episodes for ONE
/// arm, then folds them into a single `MetricsBatch` using Phase A's greedy
/// IoU pairing (which buckets by `(podcastId, episodeId)`, so cross-episode
/// leakage is impossible). Pure value type — no I/O, no pipeline.
///
/// Each episode contributes:
///   - its ground-truth spans (bridged from `CorpusAnnotation.adWindows`), and
///   - its detected spans (bridged from the persisted `[AdWindow]` rows,
///     with audit/observability rows filtered out by the Phase-A bridge).
/// attributed to the SAME `(podcastId, episodeId)` pair so they can pair.
struct FusionLiftModeAccumulator: Sendable {
    private(set) var groundTruth: [MetricGroundTruthAd] = []
    private(set) var detections: [MetricDetectedAd] = []

    init() {}

    /// Add one episode's worth of ground truth + detections.
    ///
    /// - Parameters:
    ///   - annotationWindows: the corpus ground-truth ad windows.
    ///   - adWindows: the persisted store rows produced by `runBackfill`.
    ///   - podcastId: episode-stable show id (must match the value passed to
    ///     `runBackfill`).
    ///   - episodeId: episode-stable id used to bucket pairs. The detection
    ///     rows only know their `analysisAssetId`, so the caller supplies the
    ///     same `episodeId` to both bridges — that pairing key is what lets a
    ///     GT span match a detection from the same episode.
    mutating func addEpisode(
        annotationWindows: [CorpusAnnotation.AdWindow],
        adWindows: [AdWindow],
        podcastId: String,
        episodeId: String
    ) {
        for (index, window) in annotationWindows.enumerated() {
            groundTruth.append(MetricGroundTruthAd(
                annotationWindow: window,
                id: "\(episodeId)-gt-\(index)",
                podcastId: podcastId,
                episodeId: episodeId
            ))
        }
        detections.append(contentsOf: MetricsBatch.skipEligibleDetections(
            from: adWindows,
            podcastId: podcastId,
            episodeId: episodeId
        ))
    }

    /// Fold the accumulated spans into a paired batch via greedy IoU.
    func batch() -> MetricsBatch {
        MetricsBatch.pair(groundTruth: groundTruth, detections: detections)
    }

    /// Convenience: the count-based span F1 for this arm.
    func spanF1() -> SpanF1 {
        SpanF1(batch: batch())
    }

    /// Convenience: the full 9-metric summary for this arm (the lift diff
    /// uses the seconds-based coverage P/R off this).
    func summary() -> MetricsSummary {
        MetricsSummary(batch: batch())
    }
}

// MARK: - Lift report

/// A readable, serializable summary of the A/B lift. Pure value type. Holds
/// both lenses Phase A exposes:
///   - the SECONDS-based coverage lift (`coverageLift`, from `MetricsSummary`),
///   - the COUNT-based span lift (`spanLift`, from `SpanF1`),
/// plus the raw per-arm counts so the JSON dump is self-describing.
///
/// "Delta" is always `enabled − off`; positive means the chapter signal
/// HELPED that metric. Undefined metrics propagate to `nil` (never a
/// misleading 0.0), matching the Phase-A contract.
struct FusionLiftReport: Sendable, Codable, Equatable {

    struct ArmCounts: Sendable, Codable, Equatable {
        let groundTruthSpans: Int
        let detectedSpans: Int
        let truePositives: Int
        let falsePositives: Int
        let misses: Int
        let spanPrecision: Double?
        let spanRecall: Double?
        let spanF1: Double?
        let coveragePrecision: Double?
        let coverageRecall: Double?
    }

    let episodeCount: Int
    let offArm: ArmCounts
    let enabledArm: ArmCounts
    /// Count-based span lift (how many ads paired vs invented/missed).
    let spanPrecisionDelta: Double?
    let spanRecallDelta: Double?
    let spanF1Delta: Double?
    /// Seconds-based coverage lift (how many ad seconds covered).
    let coveragePrecisionDelta: Double?
    let coverageRecallDelta: Double?
    let coverageF1Delta: Double?

    /// Build a report from the two accumulators.
    init(
        episodeCount: Int,
        off: FusionLiftModeAccumulator,
        enabled: FusionLiftModeAccumulator
    ) {
        self.episodeCount = episodeCount

        let offSpan = off.spanF1()
        let enabledSpan = enabled.spanF1()
        let offSummary = off.summary()
        let enabledSummary = enabled.summary()

        self.offArm = Self.armCounts(
            accumulator: off, spanF1: offSpan, summary: offSummary
        )
        self.enabledArm = Self.armCounts(
            accumulator: enabled, spanF1: enabledSpan, summary: enabledSummary
        )

        let spanLift = FusionLiftResult(off: offSpan, enabled: enabledSpan)
        self.spanPrecisionDelta = spanLift.precisionDelta
        self.spanRecallDelta = spanLift.recallDelta
        self.spanF1Delta = spanLift.f1Delta

        let coverageLift = FusionLiftResult(off: offSummary, enabled: enabledSummary)
        self.coveragePrecisionDelta = coverageLift.precisionDelta
        self.coverageRecallDelta = coverageLift.recallDelta
        self.coverageF1Delta = coverageLift.f1Delta
    }

    private static func armCounts(
        accumulator: FusionLiftModeAccumulator,
        spanF1: SpanF1,
        summary: MetricsSummary
    ) -> ArmCounts {
        ArmCounts(
            groundTruthSpans: accumulator.groundTruth.count,
            detectedSpans: accumulator.detections.count,
            truePositives: spanF1.truePositives,
            falsePositives: spanF1.falsePositives,
            misses: spanF1.misses,
            spanPrecision: spanF1.precision,
            spanRecall: spanF1.recall,
            spanF1: spanF1.f1,
            coveragePrecision: summary.coveragePrecision,
            coverageRecall: summary.coverageRecall
        )
    }

    /// Render a fixed-width, human-readable lift table for the test log.
    /// Undefined metrics render as `n/a`; defined values to 4 decimals.
    func table() -> String {
        func fmt(_ value: Double?) -> String {
            guard let value else { return "n/a" }
            return String(format: "%.4f", value)
        }
        func signed(_ value: Double?) -> String {
            guard let value else { return "n/a" }
            return String(format: "%+.4f", value)
        }
        return """
        === Chapter-Fusion Lift A/B (au2v.1.27 Phase C) ===
        episodes scored: \(episodeCount)
        arm          GT  det   TP  FP  miss   spanP    spanR   spanF1   covP     covR
        off       \(pad(offArm.groundTruthSpans, 4))\(pad(offArm.detectedSpans, 5))\(pad(offArm.truePositives, 5))\(pad(offArm.falsePositives, 4))\(pad(offArm.misses, 6))  \(col(fmt(offArm.spanPrecision)))\(col(fmt(offArm.spanRecall)))\(col(fmt(offArm.spanF1)))\(col(fmt(offArm.coveragePrecision)))\(col(fmt(offArm.coverageRecall)))
        enabled   \(pad(enabledArm.groundTruthSpans, 4))\(pad(enabledArm.detectedSpans, 5))\(pad(enabledArm.truePositives, 5))\(pad(enabledArm.falsePositives, 4))\(pad(enabledArm.misses, 6))  \(col(fmt(enabledArm.spanPrecision)))\(col(fmt(enabledArm.spanRecall)))\(col(fmt(enabledArm.spanF1)))\(col(fmt(enabledArm.coveragePrecision)))\(col(fmt(enabledArm.coverageRecall)))
        --- lift (enabled − off) ---
        span:     precisionΔ=\(signed(spanPrecisionDelta))  recallΔ=\(signed(spanRecallDelta))  f1Δ=\(signed(spanF1Delta))
        coverage: precisionΔ=\(signed(coveragePrecisionDelta))  recallΔ=\(signed(coverageRecallDelta))  f1Δ=\(signed(coverageF1Delta))
        """
    }

    private func pad(_ value: Int, _ width: Int) -> String {
        let s = String(value)
        return String(repeating: " ", count: max(0, width - s.count)) + s
    }

    private func col(_ s: String) -> String {
        // 9-char column (8 content + 1 separator space).
        (s + String(repeating: " ", count: 9)).prefix(9).description
    }

    /// Encode the report to pretty-printed, sorted-key JSON for the
    /// git-ignored repo-root dump.
    func jsonData() throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        return try encoder.encode(self)
    }
}

// MARK: - Lexical-scorer per-feature SWEEP report (playhead-xsdz.liveab)

/// A readable, serializable summary of the 4-arm per-feature sweep. Pure
/// value type. Generalizes the earlier 2-arm cumulative lift report (baseline
/// / treatment) to N arms (baseline, xsdz1only, xsdz23only, alon) so a single
/// Catalyst run isolates which lexical-scorer feature regresses live ad
/// detection. Structurally a per-arm sibling of the chapter A/B's
/// `FusionLiftReport`. Each arm reports its raw counts + metrics; the deltas
/// are always measured vs the `baseline` arm (both gates off). Reuses Phase
/// A's `SpanF1` / `FusionLiftResult` scorers verbatim — no reimplementation.
///
/// "Delta" is `arm − baseline`; positive means that feature HELPED the
/// metric. Undefined metrics propagate to `nil` (never a misleading 0.0),
/// matching the Phase-A contract. The baseline arm's own deltas are zero (it
/// is measured against itself).
struct LexicalScorerSweepReport: Sendable, Codable, Equatable {

    /// One arm's raw counts + metrics + its delta-vs-baseline. The arm label
    /// is the `LexicalScorerArm.rawValue` so the JSON dump is self-describing.
    struct ArmRow: Sendable, Codable, Equatable {
        let arm: String
        let xsdz1On: Bool
        let xsdz23On: Bool
        let groundTruthSpans: Int
        let detectedSpans: Int
        let truePositives: Int
        let falsePositives: Int
        let misses: Int
        let spanPrecision: Double?
        let spanRecall: Double?
        let spanF1: Double?
        let coveragePrecision: Double?
        let coverageRecall: Double?
        // Deltas vs baseline (count-based span lens).
        let spanPrecisionDelta: Double?
        let spanRecallDelta: Double?
        let spanF1Delta: Double?
        // Deltas vs baseline (seconds-based coverage lens).
        let coveragePrecisionDelta: Double?
        let coverageRecallDelta: Double?
        let coverageF1Delta: Double?
    }

    let episodeCount: Int
    /// Rows in `LexicalScorerArm.allCases` order, baseline first.
    let rows: [ArmRow]

    /// Build the sweep report from one accumulator per arm. The dictionary
    /// MUST contain every `LexicalScorerArm` case (the harness scores all 4);
    /// arms are emitted in `allCases` order so the table and JSON are stable.
    /// The baseline arm anchors every delta.
    init(
        episodeCount: Int,
        accumulators: [LexicalScorerArm: FusionLiftModeAccumulator]
    ) {
        self.episodeCount = episodeCount

        let baselineAcc = accumulators[.baseline] ?? FusionLiftModeAccumulator()
        let baselineSpan = baselineAcc.spanF1()
        let baselineSummary = baselineAcc.summary()

        self.rows = LexicalScorerArm.allCases.map { arm in
            let acc = accumulators[arm] ?? FusionLiftModeAccumulator()
            let span = acc.spanF1()
            let summary = acc.summary()

            // FusionLiftResult names its sides `off`/`enabled`; here `off` is
            // the baseline arm and `enabled` is this arm, so the delta reads
            // `arm − baseline`.
            let spanLift = FusionLiftResult(off: baselineSpan, enabled: span)
            let coverageLift = FusionLiftResult(off: baselineSummary, enabled: summary)

            return ArmRow(
                arm: arm.rawValue,
                xsdz1On: arm.xsdz1On,
                xsdz23On: arm.xsdz23On,
                groundTruthSpans: acc.groundTruth.count,
                detectedSpans: acc.detections.count,
                truePositives: span.truePositives,
                falsePositives: span.falsePositives,
                misses: span.misses,
                spanPrecision: span.precision,
                spanRecall: span.recall,
                spanF1: span.f1,
                coveragePrecision: summary.coveragePrecision,
                coverageRecall: summary.coverageRecall,
                spanPrecisionDelta: spanLift.precisionDelta,
                spanRecallDelta: spanLift.recallDelta,
                spanF1Delta: spanLift.f1Delta,
                coveragePrecisionDelta: coverageLift.precisionDelta,
                coverageRecallDelta: coverageLift.recallDelta,
                coverageF1Delta: coverageLift.f1Delta
            )
        }
    }

    /// Render a fixed-width, human-readable sweep table for the test log.
    /// One row per arm + a per-arm delta-vs-baseline block. Undefined metrics
    /// render as `n/a`; defined values to 4 decimals.
    func table() -> String {
        func fmt(_ value: Double?) -> String {
            guard let value else { return "n/a" }
            return String(format: "%.4f", value)
        }
        func signed(_ value: Double?) -> String {
            guard let value else { return "n/a" }
            return String(format: "%+.4f", value)
        }

        var lines: [String] = [
            "=== Lexical-Scorer Per-Feature Sweep A/B (xsdz.1 / xsdz.2+.3) ===",
            "episodes scored: \(episodeCount)",
            "arm          GT  det   TP  FP  miss   spanP    spanR   spanF1   covP     covR",
        ]
        for row in rows {
            lines.append(
                "\(armLabel(row.arm))\(pad(row.groundTruthSpans, 4))\(pad(row.detectedSpans, 5))\(pad(row.truePositives, 5))\(pad(row.falsePositives, 4))\(pad(row.misses, 6))  \(col(fmt(row.spanPrecision)))\(col(fmt(row.spanRecall)))\(col(fmt(row.spanF1)))\(col(fmt(row.coveragePrecision)))\(col(fmt(row.coverageRecall)))"
            )
        }
        lines.append("--- per-arm lift (arm − baseline) ---")
        for row in rows where row.arm != LexicalScorerArm.baseline.rawValue {
            lines.append(
                "\(armLabel(row.arm)) span pΔ=\(signed(row.spanPrecisionDelta)) rΔ=\(signed(row.spanRecallDelta)) f1Δ=\(signed(row.spanF1Delta)) | cov pΔ=\(signed(row.coveragePrecisionDelta)) rΔ=\(signed(row.coverageRecallDelta))"
            )
        }
        return lines.joined(separator: "\n")
    }

    private func armLabel(_ arm: String) -> String {
        (arm + String(repeating: " ", count: 10)).prefix(10).description
    }

    private func pad(_ value: Int, _ width: Int) -> String {
        let s = String(value)
        return String(repeating: " ", count: max(0, width - s.count)) + s
    }

    private func col(_ s: String) -> String {
        // 9-char column (8 content + 1 separator space).
        (s + String(repeating: " ", count: 9)).prefix(9).description
    }

    /// Encode the report to pretty-printed, sorted-key JSON for the
    /// git-ignored repo-root dump.
    func jsonData() throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        return try encoder.encode(self)
    }
}
