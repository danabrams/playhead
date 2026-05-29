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
        // The 2-arm A/B holds the tuning knobs at the production default; the
        // parameterized variant below is what the SWEEP uses to probe operating
        // points. Both arms of the original A/B keep the default tuning.
        adDetectionConfig(
            fragilityEnabled: fragilityEnabled,
            fragilityThreshold: AdDetectionConfig.default.fragilityThreshold,
            fragilityPenalty: AdDetectionConfig.default.fragilityPenalty
        )
    }

    /// playhead-xsdz.7 SWEEP: build the full `AdDetectionConfig` varying ONLY
    /// the three fragility-tuning fields (`evidenceFragilityPenaltyEnabled`,
    /// `fragilityThreshold`, `fragilityPenalty`). EVERY other field is copied
    /// VERBATIM from `AdDetectionConfig.default`, so a sweep arm can never drift
    /// from production on any non-tuning axis — the hermetic isolation test pins
    /// exactly this property. This is the single source of the sweep's per-arm
    /// `AdDetectionConfig`; the baseline arm passes `fragilityEnabled: false`
    /// (tuning values are then inert because the gate is off, but they are still
    /// held at the default so the off arm == the production `.default`).
    static func adDetectionConfig(
        fragilityEnabled: Bool,
        fragilityThreshold: Double,
        fragilityPenalty: Double
    ) -> AdDetectionConfig {
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
            // THE between-arm differences: the master flag plus (for the sweep)
            // the two tuning knobs. The 2-arm A/B passes the production-default
            // tuning so only the flag varies; the sweep varies all three.
            evidenceFragilityPenaltyEnabled: fragilityEnabled,
            fragilityThreshold: fragilityThreshold,
            fragilityPenalty: fragilityPenalty,
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

    // MARK: - Sweep operating points (playhead-xsdz.7 sweep)

    /// playhead-xsdz.7 SWEEP: the fields the sweep's isolation test compares
    /// across arms. The sweep deliberately VARIES three fields —
    /// `evidenceFragilityPenaltyEnabled`, `fragilityThreshold`,
    /// `fragilityPenalty` — so they must be EXCLUDED from the pairwise-equal
    /// check (unlike the 2-arm A/B, which holds the two tuning knobs fixed and
    /// therefore keeps them in `comparableFields`). Derived from
    /// `comparableFields` by dropping the two tuning fields, so the sweep's
    /// "every other field is equal" guarantee automatically tracks any field
    /// added to the canonical list.
    static let sweepVaryingFieldNames: Set<String> = [
        "evidenceFragilityPenaltyEnabled", // already absent from comparableFields
        "fragilityThreshold",
        "fragilityPenalty",
    ]

    /// The fields the SWEEP isolation test asserts are byte-identical across all
    /// sweep arms — `comparableFields` minus the two tuning knobs the sweep is
    /// allowed to vary. (`evidenceFragilityPenaltyEnabled` is already excluded
    /// from `comparableFields`.)
    static let sweepComparableFields: [(name: String, value: @Sendable (AdDetectionConfig) -> String)] =
        comparableFields.filter { !sweepVaryingFieldNames.contains($0.name) }

    /// The `AdDetectionConfig` for a sweep arm — varies ONLY the three fragility
    /// tuning fields (flag/threshold/penalty); every other field is `.default`.
    static func adDetectionConfig(for arm: FragilitySweepArm) -> AdDetectionConfig {
        adDetectionConfig(
            fragilityEnabled: arm.fragilityEnabled,
            fragilityThreshold: arm.fragilityThreshold,
            fragilityPenalty: arm.fragilityPenalty
        )
    }

    /// The `NarrowingConfig` for a sweep arm: `.default` (snap ON) for EVERY
    /// arm — the narrowing config never varies in the sweep, only the fragility
    /// tuning does. Exposed so the harness wires the same value into every arm's
    /// live runner factory and the isolation test can assert it.
    static func narrowingConfig(for arm: FragilitySweepArm) -> NarrowingConfig {
        _ = arm
        return .default
    }
}

// MARK: - Evidence-Fragility threshold/penalty SWEEP arms (playhead-xsdz.7)

/// The arms of the Evidence-Fragility threshold/penalty SWEEP
/// (`FragilityGateLiveABTests`'s Part B). The `baseline` arm is the production
/// state with the gate OFF — the SAME arm the per-span diagnostic runs on, so
/// the single Catalyst pass never runs the baseline twice. The four treatment
/// arms each turn the gate ON at a distinct (threshold, penalty) operating
/// point, chosen to probe whether a LOWER threshold and/or STRONGER penalty
/// drops false positives without hurting recall (the prior live A/B found the
/// DEFAULT 2.0/0.85 point fires only on already-correct spans).
///
/// LOAD-BEARING isolation property (pinned hermetically by
/// `FragilityGateArmConfigTests`): every arm deviates from
/// `AdDetectionConfig.default` ONLY in the three fragility tuning fields
/// (`evidenceFragilityPenaltyEnabled` / `fragilityThreshold` /
/// `fragilityPenalty`). A drift on any OTHER field would attribute a sweep
/// delta to the tuning that some other flag actually caused.
enum FragilitySweepArm: String, Sendable, CaseIterable {
    /// Gate OFF — production state. Doubles as the sweep's baseline AND the
    /// per-span diagnostic arm (run once).
    case baseline
    /// Gate ON at threshold 1.5 / penalty 0.85 (lower the firing bar; same
    /// penalty as default).
    case t15p85
    /// Gate ON at threshold 1.0 / penalty 0.85 (lower the firing bar further).
    case t10p85
    /// Gate ON at threshold 0.7 / penalty 0.70 (broader firing + stronger cut).
    case t07p70
    /// Gate ON at threshold 0.5 / penalty 0.50 (broadest firing + harshest cut).
    case t05p50

    /// Whether the fragility gate is enabled in this arm.
    var fragilityEnabled: Bool {
        switch self {
        case .baseline: return false
        case .t15p85, .t10p85, .t07p70, .t05p50: return true
        }
    }

    /// The `fragilityThreshold` operating point for this arm. The baseline keeps
    /// the production default (inert — the gate is off), so the off arm is
    /// byte-identical to production `.default`.
    var fragilityThreshold: Double {
        switch self {
        case .baseline: return AdDetectionConfig.default.fragilityThreshold
        case .t15p85: return 1.5
        case .t10p85: return 1.0
        case .t07p70: return 0.7
        case .t05p50: return 0.5
        }
    }

    /// The `fragilityPenalty` operating point for this arm. Baseline keeps the
    /// production default (inert — the gate is off).
    var fragilityPenalty: Double {
        switch self {
        case .baseline: return AdDetectionConfig.default.fragilityPenalty
        case .t15p85, .t10p85: return 0.85
        case .t07p70: return 0.70
        case .t05p50: return 0.50
        }
    }
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

// MARK: - Evidence-Fragility threshold/penalty SWEEP report (playhead-xsdz.7)

/// A readable, serializable summary of the N-arm fragility tuning SWEEP. Pure
/// value type, structurally a sibling of `LexicalScorerSweepReport`: one row
/// per `FragilitySweepArm`, baseline first, with every treatment arm's deltas
/// measured vs the baseline (gate OFF) arm. Reuses Phase A's `SpanF1` /
/// `FusionLiftResult` scorers verbatim — no reimplementation.
///
/// "Delta" is `arm − baseline`; a NEGATIVE `falsePositives`-style movement is
/// the goal (fewer FPs), and the deltas surface whether dropping FPs cost any
/// recall. The baseline row's deltas are zero (measured against itself).
struct FragilitySweepReport: Sendable, Codable, Equatable {

    /// One sweep arm's raw counts + metrics + tuning point + delta-vs-baseline.
    struct ArmRow: Sendable, Codable, Equatable {
        let arm: String
        let fragilityEnabled: Bool
        let fragilityThreshold: Double
        let fragilityPenalty: Double
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
        let truePositivesDelta: Int
        let falsePositivesDelta: Int
        let missesDelta: Int
        let spanPrecisionDelta: Double?
        let spanRecallDelta: Double?
        let spanF1Delta: Double?
        // Deltas vs baseline (seconds-based coverage lens).
        let coveragePrecisionDelta: Double?
        let coverageRecallDelta: Double?
        let coverageF1Delta: Double?
    }

    let episodeCount: Int
    /// Rows in `FragilitySweepArm.allCases` order, baseline first.
    let rows: [ArmRow]

    /// Build the sweep report from one accumulator per arm. The dictionary MUST
    /// contain every `FragilitySweepArm` case; arms emit in `allCases` order so
    /// the table and JSON are stable. The baseline arm anchors every delta.
    init(
        episodeCount: Int,
        accumulators: [FragilitySweepArm: FusionLiftModeAccumulator]
    ) {
        self.episodeCount = episodeCount

        let baselineAcc = accumulators[.baseline] ?? FusionLiftModeAccumulator()
        let baselineSpan = baselineAcc.spanF1()
        let baselineSummary = baselineAcc.summary()

        self.rows = FragilitySweepArm.allCases.map { arm in
            let acc = accumulators[arm] ?? FusionLiftModeAccumulator()
            let span = acc.spanF1()
            let summary = acc.summary()

            // `FusionLiftResult` names its sides off/enabled; off = baseline,
            // enabled = this arm, so the delta reads `arm − baseline`.
            let spanLift = FusionLiftResult(off: baselineSpan, enabled: span)
            let coverageLift = FusionLiftResult(off: baselineSummary, enabled: summary)

            return ArmRow(
                arm: arm.rawValue,
                fragilityEnabled: arm.fragilityEnabled,
                fragilityThreshold: arm.fragilityThreshold,
                fragilityPenalty: arm.fragilityPenalty,
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
                truePositivesDelta: span.truePositives - baselineSpan.truePositives,
                falsePositivesDelta: span.falsePositives - baselineSpan.falsePositives,
                missesDelta: span.misses - baselineSpan.misses,
                spanPrecisionDelta: spanLift.precisionDelta,
                spanRecallDelta: spanLift.recallDelta,
                spanF1Delta: spanLift.f1Delta,
                coveragePrecisionDelta: coverageLift.precisionDelta,
                coverageRecallDelta: coverageLift.recallDelta,
                coverageF1Delta: coverageLift.f1Delta
            )
        }
    }

    /// Render a fixed-width sweep table for the test log.
    func table() -> String {
        func fmt(_ value: Double?) -> String {
            guard let value else { return "n/a" }
            return String(format: "%.4f", value)
        }
        func signed(_ value: Double?) -> String {
            guard let value else { return "n/a" }
            return String(format: "%+.4f", value)
        }
        func signedInt(_ value: Int) -> String { String(format: "%+d", value) }

        var lines: [String] = [
            "=== Evidence-Fragility Threshold/Penalty Sweep (xsdz.7 Part B) ===",
            "episodes scored: \(episodeCount)",
            "arm          thr   pen    GT  det   TP  FP  miss   spanP    spanR   spanF1   covP     covR",
        ]
        for row in rows {
            let thr = row.fragilityEnabled ? String(format: "%.2f", row.fragilityThreshold) : "  -"
            let pen = row.fragilityEnabled ? String(format: "%.2f", row.fragilityPenalty) : "  -"
            lines.append(
                "\(armLabel(row.arm))\(col6(thr))\(col6(pen)) \(pad(row.groundTruthSpans, 4))\(pad(row.detectedSpans, 5))\(pad(row.truePositives, 5))\(pad(row.falsePositives, 4))\(pad(row.misses, 6))  \(col(fmt(row.spanPrecision)))\(col(fmt(row.spanRecall)))\(col(fmt(row.spanF1)))\(col(fmt(row.coveragePrecision)))\(col(fmt(row.coverageRecall)))"
            )
        }
        lines.append("--- per-arm delta (arm − baseline) ---")
        for row in rows where row.arm != FragilitySweepArm.baseline.rawValue {
            lines.append(
                "\(armLabel(row.arm)) TPΔ=\(signedInt(row.truePositivesDelta)) FPΔ=\(signedInt(row.falsePositivesDelta)) missΔ=\(signedInt(row.missesDelta)) | span pΔ=\(signed(row.spanPrecisionDelta)) rΔ=\(signed(row.spanRecallDelta)) f1Δ=\(signed(row.spanF1Delta)) | cov pΔ=\(signed(row.coveragePrecisionDelta)) rΔ=\(signed(row.coverageRecallDelta))"
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
        (s + String(repeating: " ", count: 9)).prefix(9).description
    }

    private func col6(_ s: String) -> String {
        let padded = String(repeating: " ", count: max(0, 6 - s.count)) + s
        return padded
    }

    /// Encode the report to pretty-printed, sorted-key JSON for the git-ignored
    /// repo-root dump.
    func jsonData() throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        return try encoder.encode(self)
    }
}

// MARK: - Per-span fragility DIAGNOSTIC (playhead-xsdz.7 Part A)

/// The label a per-span diagnostic row gets after joining it to the scorer's
/// greedy-IoU pairing. EXACTLY the four mutually-exclusive outcomes the scorer
/// produces, attributed to the span the diagnostic recorded.
enum FragilitySpanLabel: String, Sendable, Codable, Equatable {
    /// The span became a skip-eligible detection that paired with a GT ad.
    case truePositive
    /// The span became a skip-eligible detection that paired with NO GT ad.
    case falsePositive
    /// The span did NOT become a skip-eligible detection (suppressed / not
    /// promoted) — i.e. the pipeline correctly declined to skip it. There is no
    /// detection row for it in the scorer batch.
    case correctlyRejected
}

/// One labeled per-span diagnostic row: the recorded fragility geometry plus
/// the episode it came from and the TP/FP/correctly-rejected label assigned by
/// the SAME greedy-IoU pairing the metrics scorer uses. Pure value type → safe
/// to serialize directly into the per-span JSON dump.
struct LabeledFragilitySpanRow: Sendable, Codable, Equatable {
    let episodeId: String
    let podcastId: String
    let spanId: String
    let spanStart: Double
    let spanEnd: Double
    let proposalConfidence: Double
    let skipConfidence: Double
    let maxSingleEntryWeight: Double
    let distinctEvidenceFamilyDepth: Int
    let margin: Double
    let fragilityScore: Double
    let label: String
}

/// Pure, hermetic joiner: labels each recorded `FragilitySpanDiagnostic` for an
/// episode by running the SAME `MetricsBatch.pair` greedy-IoU pairing the scorer
/// uses, then attributing TP/FP via the SAME detection-row identity the scorer
/// scores.
///
/// CORRECTNESS (the load-bearing part): the diagnostic fires for EVERY decoded
/// span, but the scorer only scores SKIP-ELIGIBLE detections (the persisted
/// `AdWindow`s whose `decisionState` is candidate/confirmed/applied). A persisted
/// fusion `AdWindow` is built (`buildFusionAdWindow`) with `startTime`/`endTime`
/// copied VERBATIM from the decoded span's refined boundaries, so each
/// skip-eligible detection's `(startTime, endTime)` equals exactly one
/// diagnostic row's `(spanStart, spanEnd)`. We therefore:
///   1. Bridge GT + skip-eligible detections and run `MetricsBatch.pair`
///      (IDENTICAL inputs to the accumulator the metrics come from).
///   2. For each TP pair, mark its detection's `(start, end)` as a TP key; for
///      each FP pair, mark it as an FP key.
///   3. Walk the diagnostic rows: a row whose `(spanStart, spanEnd)` matches a
///      TP key → `.truePositive`; an FP key → `.falsePositive`; neither (the
///      span never became a skip-eligible detection) → `.correctlyRejected`.
/// Keying on the rounded `(start, end)` pair (not on `AdWindow.id`, which is a
/// fresh UUID unrelated to the span id) is exact because the fusion window
/// inherits the span boundaries unmodified.
enum FragilityPerSpanLabeler {

    /// Rounding granularity for the `(start, end)` join key. The fusion window
    /// copies the span boundaries verbatim (same `Double` bits), so rounding is
    /// belt-and-suspenders against any future float reformat; 1e-3 s (1 ms) is
    /// far finer than the ±0.5 s corpus precision and far coarser than any
    /// float noise.
    static let keyQuantum: Double = 1e-3

    private struct SpanKey: Hashable {
        let start: Int
        let end: Int
        init(start: Double, end: Double, quantum: Double) {
            self.start = Int((start / quantum).rounded())
            self.end = Int((end / quantum).rounded())
        }
    }

    /// Label one episode's diagnostic rows against its golden + detected windows.
    ///
    /// - Parameters:
    ///   - rows: the per-span diagnostic rows recorded by the observer for this
    ///     episode (one per decoded span).
    ///   - annotationWindows: the corpus golden ad windows.
    ///   - adWindows: the persisted store rows produced by the same scored run.
    ///   - podcastId / episodeId: the SAME ids the metrics accumulator buckets
    ///     on (so the pairing is identical).
    static func label(
        rows: [FragilitySpanDiagnostic],
        annotationWindows: [CorpusAnnotation.AdWindow],
        adWindows: [AdWindow],
        podcastId: String,
        episodeId: String
    ) -> [LabeledFragilitySpanRow] {
        // Bridge GT + skip-eligible detections EXACTLY as the accumulator does.
        let groundTruth = annotationWindows.enumerated().map { index, window in
            MetricGroundTruthAd(
                annotationWindow: window,
                id: "\(episodeId)-gt-\(index)",
                podcastId: podcastId,
                episodeId: episodeId
            )
        }
        let detections = MetricsBatch.skipEligibleDetections(
            from: adWindows,
            podcastId: podcastId,
            episodeId: episodeId
        )

        // Same greedy-IoU pairing the scorer uses.
        let batch = MetricsBatch.pair(groundTruth: groundTruth, detections: detections)

        // Build TP/FP keys off the DETECTION side of each pair (the side whose
        // (start, end) equals a diagnostic row's span boundaries).
        var tpKeys = Set<SpanKey>()
        var fpKeys = Set<SpanKey>()
        for pair in batch.pairs {
            guard let detected = pair.detected else { continue } // misses have no detection row
            let key = SpanKey(start: detected.startTime, end: detected.endTime, quantum: keyQuantum)
            if pair.isTruePositive {
                tpKeys.insert(key)
            } else if pair.isFalsePositive {
                fpKeys.insert(key)
            }
        }

        return rows.map { row in
            let key = SpanKey(start: row.spanStart, end: row.spanEnd, quantum: keyQuantum)
            let label: FragilitySpanLabel
            if tpKeys.contains(key) {
                label = .truePositive
            } else if fpKeys.contains(key) {
                label = .falsePositive
            } else {
                label = .correctlyRejected
            }
            return LabeledFragilitySpanRow(
                episodeId: episodeId,
                podcastId: podcastId,
                spanId: row.spanId,
                spanStart: row.spanStart,
                spanEnd: row.spanEnd,
                proposalConfidence: row.proposalConfidence,
                skipConfidence: row.skipConfidence,
                maxSingleEntryWeight: row.maxSingleEntryWeight,
                distinctEvidenceFamilyDepth: row.distinctEvidenceFamilyDepth,
                margin: row.margin,
                fragilityScore: row.fragilityScore,
                label: label.rawValue
            )
        }
    }
}

/// The full per-span fragility diagnostic report: every labeled row across all
/// scored episodes plus a group summary contrasting the FP rows against the TP
/// rows (mean/median fragility, margin, concentration, depth) and an explicit
/// verdict on whether FP fragilities are systematically higher than TP. Pure
/// value type → serialized directly into the per-span JSON dump.
struct FragilityPerSpanDiagnosticReport: Sendable, Codable, Equatable {

    /// Mean/median of one numeric field over one label group. `nil` when the
    /// group is empty (never a misleading 0.0).
    struct GroupStat: Sendable, Codable, Equatable {
        let count: Int
        let meanFragility: Double?
        let medianFragility: Double?
        let meanMargin: Double?
        let medianMargin: Double?
        let meanConcentration: Double?
        let medianConcentration: Double?
        let meanDepth: Double?
        let medianDepth: Double?
    }

    let episodeCount: Int
    let rows: [LabeledFragilitySpanRow]
    let falsePositiveStats: GroupStat
    let truePositiveStats: GroupStat
    let correctlyRejectedStats: GroupStat
    /// `true` iff BOTH groups are non-empty AND mean FP fragility > mean TP
    /// fragility (the geometry is, on average, discriminative in the expected
    /// direction). `nil` when either group is empty (the comparison is undefined).
    let fpFragilitySystematicallyHigherThanTP: Bool?
    /// Same verdict on the MEDIAN (robust to outliers). `nil` when undefined.
    let fpFragilityMedianHigherThanTP: Bool?

    init(episodeCount: Int, rows: [LabeledFragilitySpanRow]) {
        self.episodeCount = episodeCount
        self.rows = rows

        let fp = rows.filter { $0.label == FragilitySpanLabel.falsePositive.rawValue }
        let tp = rows.filter { $0.label == FragilitySpanLabel.truePositive.rawValue }
        let cr = rows.filter { $0.label == FragilitySpanLabel.correctlyRejected.rawValue }

        let fpStat = Self.groupStat(fp)
        let tpStat = Self.groupStat(tp)
        self.falsePositiveStats = fpStat
        self.truePositiveStats = tpStat
        self.correctlyRejectedStats = Self.groupStat(cr)

        if let fpMean = fpStat.meanFragility, let tpMean = tpStat.meanFragility {
            self.fpFragilitySystematicallyHigherThanTP = fpMean > tpMean
        } else {
            self.fpFragilitySystematicallyHigherThanTP = nil
        }
        if let fpMed = fpStat.medianFragility, let tpMed = tpStat.medianFragility {
            self.fpFragilityMedianHigherThanTP = fpMed > tpMed
        } else {
            self.fpFragilityMedianHigherThanTP = nil
        }
    }

    /// `concentration = maxSingleEntryWeight / max(proposalConfidence, ε)` — the
    /// SAME concentration term `AdDetectionConfig.fragilityScore` computes (ε =
    /// `AdDetectionConfig.fragilityEpsilon`). Derived here only for the group
    /// summary; the per-row score itself comes from the production helper.
    private static func concentration(_ row: LabeledFragilitySpanRow) -> Double {
        let eps = AdDetectionConfig.fragilityEpsilon
        return row.maxSingleEntryWeight / max(row.proposalConfidence, eps)
    }

    private static func groupStat(_ group: [LabeledFragilitySpanRow]) -> GroupStat {
        GroupStat(
            count: group.count,
            meanFragility: mean(group.map(\.fragilityScore)),
            medianFragility: median(group.map(\.fragilityScore)),
            meanMargin: mean(group.map(\.margin)),
            medianMargin: median(group.map(\.margin)),
            meanConcentration: mean(group.map(concentration)),
            medianConcentration: median(group.map(concentration)),
            meanDepth: mean(group.map { Double($0.distinctEvidenceFamilyDepth) }),
            medianDepth: median(group.map { Double($0.distinctEvidenceFamilyDepth) })
        )
    }

    private static func mean(_ values: [Double]) -> Double? {
        guard !values.isEmpty else { return nil }
        return values.reduce(0, +) / Double(values.count)
    }

    private static func median(_ values: [Double]) -> Double? {
        guard !values.isEmpty else { return nil }
        let sorted = values.sorted()
        let n = sorted.count
        if n % 2 == 1 { return sorted[n / 2] }
        return (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }

    /// Human-readable summary for the test log.
    func table() -> String {
        func fmt(_ v: Double?) -> String {
            guard let v else { return "n/a" }
            return String(format: "%.4f", v)
        }
        func verdict(_ b: Bool?) -> String {
            guard let b else { return "n/a (a group is empty)" }
            return b ? "YES (FP > TP)" : "NO (FP <= TP)"
        }
        func line(_ name: String, _ s: GroupStat) -> String {
            "\(name): n=\(s.count) fragility(mean=\(fmt(s.meanFragility)) median=\(fmt(s.medianFragility))) margin(mean=\(fmt(s.meanMargin))) concentration(mean=\(fmt(s.meanConcentration))) depth(mean=\(fmt(s.meanDepth)))"
        }
        return """
        === Per-Span Fragility Diagnostic (xsdz.7 Part A) ===
        episodes scored: \(episodeCount)   labeled spans: \(rows.count)
        \(line("FP ", falsePositiveStats))
        \(line("TP ", truePositiveStats))
        \(line("CR ", correctlyRejectedStats))
        verdict — FP fragility systematically higher than TP? mean: \(verdict(fpFragilitySystematicallyHigherThanTP))  median: \(verdict(fpFragilityMedianHigherThanTP))
        """
    }

    /// Encode to pretty-printed, sorted-key JSON for the git-ignored repo-root
    /// per-span dump.
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

// MARK: - Brand-appearance A/B arm configuration (playhead-brandab)

/// The FOUR arms of the brand-appearance precision-signal A/B
/// (`BrandAppearanceLiveABTests`). Both signals are OFF by default in
/// production; the A/B isolates each ON its own AND their interaction:
///   * xsdz.12 — rhetorical act-sequence grammar, gated by the
///     `AdDetectionConfig.rhetoricalGrammarEnabled` BOOLEAN. A pure per-span
///     TEXT signal: toggling the flag is sufficient (no store).
///   * xsdz.13 — cross-show syndication, gated by the
///     `AdDetectionConfig.crossShowSyndicationEnabled` BOOLEAN. This signal also
///     needs a WIRED `CrossShowSyndicationStore` shared across episodes — the
///     flag's production consequence (`PlayheadRuntime` constructs the store iff
///     the flag is on). The harness therefore wires the shared store ONLY into
///     the two arms where this flag is on; the store presence is part of the
///     flag's production meaning, not an extra independent axis.
///
/// Each arm is defined by exactly TWO orthogonal toggles:
///   * `xsdz12On` → the `rhetoricalGrammarEnabled` flag.
///   * `xsdz13On` → the `crossShowSyndicationEnabled` flag (and, in the harness,
///     the shared-store wiring).
///
/// LOAD-BEARING isolation property (pinned hermetically by
/// `BrandAppearanceArmConfigTests`): every arm deviates from
/// `AdDetectionConfig.default` ONLY in these two flags; every other field is
/// byte-identical and equal to the production default. A drift on any other
/// field would attribute a precision change to a brand-appearance signal that
/// some OTHER flag actually caused, making the measurement meaningless.
enum BrandAppearanceArm: String, Sendable, CaseIterable {
    /// Production state: both signals OFF. The reference every delta is
    /// measured from. NO syndication store (gating parity with production).
    case baseline
    /// xsdz.12 ONLY: rhetorical grammar on, cross-show syndication off. Isolates
    /// the rhetorical-grammar signal. NO syndication store.
    case xsdz12Only
    /// xsdz.13 ONLY: cross-show syndication on (with the shared store wired),
    /// rhetorical grammar off. Isolates the syndication signal.
    case xsdz13Only
    /// BOTH ON: both signals enabled (with the shared store wired). The
    /// interaction endpoint.
    case bothOn

    /// Whether the xsdz.12 rhetorical-grammar signal is enabled in this arm
    /// (drives `rhetoricalGrammarEnabled`).
    var xsdz12On: Bool {
        switch self {
        case .baseline, .xsdz13Only: return false
        case .xsdz12Only, .bothOn: return true
        }
    }

    /// Whether the xsdz.13 cross-show-syndication signal is enabled in this arm
    /// (drives `crossShowSyndicationEnabled` AND the shared-store wiring).
    var xsdz13On: Bool {
        switch self {
        case .baseline, .xsdz12Only: return false
        case .xsdz13Only, .bothOn: return true
        }
    }

    /// Whether this arm requires a wired, shared `CrossShowSyndicationStore`.
    /// IDENTICAL to `xsdz13On` — the store is the flag's production consequence
    /// (`PlayheadRuntime` builds it iff `crossShowSyndicationEnabled`), so the
    /// baseline + xsdz12-only arms construct NO store (gating parity).
    var requiresSyndicationStore: Bool { xsdz13On }
}

/// Pure, hermetic builder for the brand-appearance arms' configs. Extracted from
/// the harness so the arm construction is unit-testable on the simulator with no
/// audio / FM / pipeline. Sources EVERY non-toggle field VERBATIM from
/// `AdDetectionConfig.default`, so the arms can never silently drift from
/// production on any axis other than the two flags under test.
enum BrandAppearanceArmConfig {

    /// Build the full `AdDetectionConfig` for the brand-appearance A/B given the
    /// two flag toggles. Every field other than `rhetoricalGrammarEnabled` and
    /// `crossShowSyndicationEnabled` is copied VERBATIM from
    /// `AdDetectionConfig.default` (the production state), so the only things this
    /// builder can vary are the two signals under test.
    static func adDetectionConfig(
        rhetoricalGrammarEnabled: Bool,
        crossShowSyndicationEnabled: Bool
    ) -> AdDetectionConfig {
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
            evidenceFragilityPenaltyEnabled: p.evidenceFragilityPenaltyEnabled,
            fragilityThreshold: p.fragilityThreshold,
            fragilityPenalty: p.fragilityPenalty,
            chapterSignalMode: p.chapterSignalMode,
            audioForensicsEnabled: p.audioForensicsEnabled,
            crossEpisodeMemoryEnabled: p.crossEpisodeMemoryEnabled,
            // THE between-arm differences: the two brand-appearance flags.
            rhetoricalGrammarEnabled: rhetoricalGrammarEnabled,
            crossShowSyndicationEnabled: crossShowSyndicationEnabled,
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

    /// The `AdDetectionConfig` for a brand-appearance arm — reads the arm's two
    /// flag toggles.
    static func adDetectionConfig(for arm: BrandAppearanceArm) -> AdDetectionConfig {
        adDetectionConfig(
            rhetoricalGrammarEnabled: arm.xsdz12On,
            crossShowSyndicationEnabled: arm.xsdz13On
        )
    }

    /// The `NarrowingConfig` for EVERY arm: `NarrowingConfig.default` (snap ON —
    /// xsdz.2/.3 are KEPT ON in production, so the baseline keeps snap on too).
    /// The narrowing config never varies in this A/B — only the two
    /// brand-appearance flags do. Exposed so the harness wires the same value
    /// into every arm's live runner factory and the isolation test can assert it.
    static func narrowingConfig(for arm: BrandAppearanceArm) -> NarrowingConfig {
        _ = arm
        return .default
    }

    /// The names of the two fields the A/B is allowed to vary. Used to derive
    /// `comparableFields` (every OTHER field, asserted byte-identical across arms
    /// and equal to `.default`).
    static let varyingFieldNames: Set<String> = [
        "rhetoricalGrammarEnabled",
        "crossShowSyndicationEnabled",
    ]

    /// The fields the isolation test asserts are byte-identical across all four
    /// arms (and equal to `AdDetectionConfig.default`). Derived from the canonical
    /// `FragilityGateArmConfig.comparableFields` list MINUS the two brand-appearance
    /// flags, so it automatically tracks any field added to the canonical list.
    /// (`FragilityGateArmConfig.comparableFields` already EXCLUDES
    /// `evidenceFragilityPenaltyEnabled`, which this A/B holds at `.default` — that
    /// is handled by the parallel named-invariant assertions in the tests.)
    static let comparableFields: [(name: String, value: @Sendable (AdDetectionConfig) -> String)] =
        FragilityGateArmConfig.comparableFields.filter { !varyingFieldNames.contains($0.name) }
}

// MARK: - Brand-appearance publish date

/// Parses the publish date encoded in a corpus episode id of the shape
/// `<show>-YYYY-MM-DD-<slug>` (e.g. `doac-2026-05-07-ww3-expert-...`). The
/// dogfood corpus encodes each episode's real publish date in its id; the
/// brand-appearance harness uses it to (a) PROCESS episodes in publish-date order
/// and (b) STAMP each episode's cross-show syndication observations at its real
/// publish date, so the xsdz.13 ≥14-day temporal-persistence gate can be
/// satisfied within one Catalyst pass if the corpus spans ≥14 days. Pure / static
/// so it is unit-testable on the simulator.
enum BrandAppearancePublishDate {

    /// Extract the publish date (UTC midday) from an episode id by finding the
    /// first `YYYY-MM-DD` token. Returns `nil` when no date token is present.
    static func parse(fromEpisodeId episodeId: String) -> Date? {
        // Scan hyphen-delimited tokens for the first `YYYY-MM-DD` triple.
        let tokens = episodeId.split(separator: "-", omittingEmptySubsequences: false).map(String.init)
        guard tokens.count >= 3 else { return nil }
        for i in 0...(tokens.count - 3) {
            guard let year = fourDigitYear(tokens[i]),
                  let month = twoDigit(tokens[i + 1], range: 1...12),
                  let day = twoDigit(tokens[i + 2], range: 1...31) else { continue }
            var components = DateComponents()
            components.year = year
            components.month = month
            components.day = day
            components.hour = 12 // midday UTC — avoids DST/midnight edge ambiguity
            var calendar = Calendar(identifier: .gregorian)
            calendar.timeZone = TimeZone(identifier: "UTC") ?? .current
            return calendar.date(from: components)
        }
        return nil
    }

    private static func fourDigitYear(_ s: String) -> Int? {
        guard s.count == 4, let v = Int(s), (1900...2999).contains(v) else { return nil }
        return v
    }

    private static func twoDigit(_ s: String, range: ClosedRange<Int>) -> Int? {
        guard s.count == 2, let v = Int(s), range.contains(v) else { return nil }
        return v
    }
}

// MARK: - Brand-appearance per-arm fire counts

/// Per-arm accumulated brand-appearance channel fire counts across all scored
/// episodes plus the xsdz.13 syndication-gate-reached entity count. Pure value
/// type so the harness can accumulate it and serialize it into the JSON dump.
/// Without these, a null A/B result is ambiguous (did the channel never fire, or
/// fire-but-no-effect?); the counts disambiguate.
struct BrandAppearanceFireTally: Sendable, Codable, Equatable {
    /// Spans whose ledger carried a positive `.rhetoricalGrammar` entry (xsdz.12).
    var rhetoricalGrammarFiredSpans: Int = 0
    /// Spans whose ledger carried a positive `.crossShowSyndication` entry (xsdz.13).
    var crossShowSyndicationFiredSpans: Int = 0
    /// Spans whose ledger carried a positive `.audioForensics` entry (xsdz.8).
    var audioForensicsFiredSpans: Int = 0
    /// Total decoded spans the tap observer saw across this arm's episodes.
    var observedSpans: Int = 0
    /// Distinct sponsor ENTITIES that reached the xsdz.13 spread+persistence gate
    /// in this arm's SHARED store (measured directly via the production evaluator
    /// over the shared store after all episodes processed). `0` for arms with no
    /// store. This is the decisive "did xsdz.13 reach its gate at all?" number.
    var syndicationGatedEntities: Int = 0

    /// Fold one episode's tap counts into the arm total.
    mutating func add(_ counts: BrandAppearanceChannelFireCounts) {
        rhetoricalGrammarFiredSpans += counts.rhetoricalGrammarFiredSpans
        crossShowSyndicationFiredSpans += counts.crossShowSyndicationFiredSpans
        audioForensicsFiredSpans += counts.audioForensicsFiredSpans
        observedSpans += counts.observedSpans
    }
}

// MARK: - Brand-appearance per-feature SWEEP report (playhead-brandab)

/// A readable, serializable summary of the 4-arm brand-appearance A/B. Pure
/// value type, structurally a sibling of `LexicalScorerSweepReport`: one row per
/// `BrandAppearanceArm`, baseline first, with every treatment arm's deltas
/// measured vs the baseline. Reuses Phase A's `SpanF1` / `FusionLiftResult`
/// scorers verbatim — no reimplementation. Each row also carries the per-channel
/// FIRE counts so a null lift is interpretable.
///
/// "Delta" is `arm − baseline`; positive means that signal HELPED the metric.
/// Undefined metrics propagate to `nil` (never a misleading 0.0). The baseline
/// arm's own deltas are zero (measured against itself).
struct BrandAppearanceSweepReport: Sendable, Codable, Equatable {

    /// One arm's raw counts + metrics + fire tally + delta-vs-baseline.
    struct ArmRow: Sendable, Codable, Equatable {
        let arm: String
        let xsdz12On: Bool
        let xsdz13On: Bool
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
        // Per-channel fire instrumentation (so a null lift is interpretable).
        let rhetoricalGrammarFiredSpans: Int
        let crossShowSyndicationFiredSpans: Int
        let observedSpans: Int
        let syndicationGatedEntities: Int
        // Deltas vs baseline (count-based span lens).
        let truePositivesDelta: Int
        let falsePositivesDelta: Int
        let missesDelta: Int
        let spanPrecisionDelta: Double?
        let spanRecallDelta: Double?
        let spanF1Delta: Double?
        // Deltas vs baseline (seconds-based coverage lens).
        let coveragePrecisionDelta: Double?
        let coverageRecallDelta: Double?
        let coverageF1Delta: Double?
    }

    let episodeCount: Int
    /// Rows in the order the arms were RUN, baseline first.
    let rows: [ArmRow]

    /// Build the report from one accumulator + fire tally per arm. The
    /// dictionaries MUST contain every `BrandAppearanceArm` case; arms emit in
    /// `allCases` order so the table and JSON are stable. The baseline arm anchors
    /// every delta. Convenience for the full 4-arm sweep.
    init(
        episodeCount: Int,
        accumulators: [BrandAppearanceArm: FusionLiftModeAccumulator],
        fireTallies: [BrandAppearanceArm: BrandAppearanceFireTally]
    ) {
        self.init(
            episodeCount: episodeCount,
            arms: BrandAppearanceArm.allCases,
            accumulators: accumulators,
            fireTallies: fireTallies
        )
    }

    /// Build the report over EXACTLY the arms in `arms`, in the order given
    /// (baseline first). The split single-signal A/B passes run only
    /// `[.baseline, <one treatment>]`, so the report must emit only those rows —
    /// not phantom zero rows for arms the pass never ran. `arms` MUST start with
    /// `.baseline` (it anchors every delta) and the dictionaries MUST contain an
    /// entry for each arm in `arms`. Arms emit in the given order so the table and
    /// JSON are stable.
    init(
        episodeCount: Int,
        arms: [BrandAppearanceArm],
        accumulators: [BrandAppearanceArm: FusionLiftModeAccumulator],
        fireTallies: [BrandAppearanceArm: BrandAppearanceFireTally]
    ) {
        self.episodeCount = episodeCount

        let baselineAcc = accumulators[.baseline] ?? FusionLiftModeAccumulator()
        let baselineSpan = baselineAcc.spanF1()
        let baselineSummary = baselineAcc.summary()

        self.rows = arms.map { arm in
            let acc = accumulators[arm] ?? FusionLiftModeAccumulator()
            let span = acc.spanF1()
            let summary = acc.summary()
            let fire = fireTallies[arm] ?? BrandAppearanceFireTally()

            // FusionLiftResult names its sides off/enabled; off = baseline,
            // enabled = this arm, so the delta reads `arm − baseline`.
            let spanLift = FusionLiftResult(off: baselineSpan, enabled: span)
            let coverageLift = FusionLiftResult(off: baselineSummary, enabled: summary)

            return ArmRow(
                arm: arm.rawValue,
                xsdz12On: arm.xsdz12On,
                xsdz13On: arm.xsdz13On,
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
                rhetoricalGrammarFiredSpans: fire.rhetoricalGrammarFiredSpans,
                crossShowSyndicationFiredSpans: fire.crossShowSyndicationFiredSpans,
                observedSpans: fire.observedSpans,
                syndicationGatedEntities: fire.syndicationGatedEntities,
                truePositivesDelta: span.truePositives - baselineSpan.truePositives,
                falsePositivesDelta: span.falsePositives - baselineSpan.falsePositives,
                missesDelta: span.misses - baselineSpan.misses,
                spanPrecisionDelta: spanLift.precisionDelta,
                spanRecallDelta: spanLift.recallDelta,
                spanF1Delta: spanLift.f1Delta,
                coveragePrecisionDelta: coverageLift.precisionDelta,
                coverageRecallDelta: coverageLift.recallDelta,
                coverageF1Delta: coverageLift.f1Delta
            )
        }
    }

    /// Render a fixed-width, human-readable table for the test log, including a
    /// per-channel fire-count block so a null lift is interpretable.
    func table() -> String {
        func fmt(_ value: Double?) -> String {
            guard let value else { return "n/a" }
            return String(format: "%.4f", value)
        }
        func signed(_ value: Double?) -> String {
            guard let value else { return "n/a" }
            return String(format: "%+.4f", value)
        }
        func signedInt(_ value: Int) -> String { String(format: "%+d", value) }

        var lines: [String] = [
            "=== Brand-Appearance Precision-Signal A/B (xsdz.12 / xsdz.13) ===",
            "episodes scored: \(episodeCount)",
            "arm          GT  det   TP  FP  miss   spanP    spanR   spanF1   covP     covR",
        ]
        for row in rows {
            lines.append(
                "\(armLabel(row.arm))\(pad(row.groundTruthSpans, 4))\(pad(row.detectedSpans, 5))\(pad(row.truePositives, 5))\(pad(row.falsePositives, 4))\(pad(row.misses, 6))  \(col(fmt(row.spanPrecision)))\(col(fmt(row.spanRecall)))\(col(fmt(row.spanF1)))\(col(fmt(row.coveragePrecision)))\(col(fmt(row.coverageRecall)))"
            )
        }
        lines.append("--- per-arm lift (arm − baseline) ---")
        for row in rows where row.arm != BrandAppearanceArm.baseline.rawValue {
            lines.append(
                "\(armLabel(row.arm)) TPΔ=\(signedInt(row.truePositivesDelta)) FPΔ=\(signedInt(row.falsePositivesDelta)) missΔ=\(signedInt(row.missesDelta)) | span pΔ=\(signed(row.spanPrecisionDelta)) rΔ=\(signed(row.spanRecallDelta)) f1Δ=\(signed(row.spanF1Delta)) | cov pΔ=\(signed(row.coveragePrecisionDelta)) rΔ=\(signed(row.coverageRecallDelta))"
            )
        }
        lines.append("--- fire instrumentation (did each channel fire?) ---")
        for row in rows {
            lines.append(
                "\(armLabel(row.arm)) spans=\(row.observedSpans) | xsdz.12 grammar fired=\(row.rhetoricalGrammarFiredSpans) | xsdz.13 syndication fired=\(row.crossShowSyndicationFiredSpans) gatedEntities=\(row.syndicationGatedEntities)"
            )
        }
        return lines.joined(separator: "\n")
    }

    private func armLabel(_ arm: String) -> String {
        (arm + String(repeating: " ", count: 11)).prefix(11).description
    }

    private func pad(_ value: Int, _ width: Int) -> String {
        let s = String(value)
        return String(repeating: " ", count: max(0, width - s.count)) + s
    }

    private func col(_ s: String) -> String {
        (s + String(repeating: " ", count: 9)).prefix(9).description
    }

    /// Encode the report to pretty-printed, sorted-key JSON for the git-ignored
    /// repo-root dump.
    func jsonData() throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        return try encoder.encode(self)
    }
}

// MARK: - Audio-forensics / temporal-reg A/B (playhead-actempo)

/// The two precision signals the actempo live A/B measures, each in its OWN
/// single-signal 2-arm method (one signal per Catalyst pass). Both are OFF by
/// default in production; the A/B isolates each ON its own against its OWN
/// baseline:
///   * xsdz.8  — composite audio-forensics boundary evidence, gated by the
///     `AdDetectionConfig.audioForensicsEnabled` BOOLEAN. A LEDGER-ENTRY signal:
///     the treatment arm emits a `.audioForensics` entry per qualifying span, so
///     its FIRE count is "spans that received a `.audioForensics` ledger entry"
///     (tallied via the nil-default `BrandAppearanceChannelTapObserver`, which
///     reads the SAME pre-suppression ledger the decision is built from).
///   * xsdz.10 — lightweight temporal regularization, gated by the
///     `AdDetectionConfig.temporalRegularizationEnabled` BOOLEAN. NOT a ledger
///     entry: a post-fusion multiplicative penalty on `skipConfidence`, so its
///     FIRE count is "spans whose confidence the penalty pass actually changed"
///     (recorded via the nil-default `TemporalRegularizationObserver`, which
///     captures the EXACT penalty-applied span count the production pass computes).
enum ActempoSignal: String, Sendable, CaseIterable {
    case audioForensics   // xsdz.8
    case temporalRegularization // xsdz.10

    /// Human-facing label for tables / JSON.
    var label: String {
        switch self {
        case .audioForensics: return "xsdz.8 audio-forensics"
        case .temporalRegularization: return "xsdz.10 temporal-reg"
        }
    }

    /// The `AdDetectionConfig` field name this signal's treatment arm flips. Used
    /// to DERIVE the per-signal `comparableFields` (every OTHER field, asserted
    /// byte-identical across the two arms and equal to `.default`).
    var varyingFieldName: String {
        switch self {
        case .audioForensics: return "audioForensicsEnabled"
        case .temporalRegularization: return "temporalRegularizationEnabled"
        }
    }
}

/// The two arms of a single-signal actempo A/B. Generic across both signals: the
/// `ActempoArmConfig` builder flips the ONE field the signal owns. Every other
/// `AdDetectionConfig` field is byte-identical and equal to `AdDetectionConfig.default`.
enum ActempoArm: String, Sendable, CaseIterable {
    /// Production state for the signal under test: the signal OFF. The reference
    /// every delta is measured from.
    case baseline
    /// Production state PLUS the single signal under test enabled.
    case treatment

    /// Whether the single signal under test is enabled in this arm.
    var signalEnabled: Bool {
        switch self {
        case .baseline: return false
        case .treatment: return true
        }
    }
}

/// Pure, hermetic builder for the actempo arms' configs. Extracted from the
/// harness so the arm construction is unit-testable on the simulator with no
/// audio / FM / pipeline (acceptance criterion: a hermetic test that pins the
/// one-field isolation). Sources EVERY non-toggle field VERBATIM from
/// `AdDetectionConfig.default`, so the arms can never silently drift from
/// production on any axis other than the ONE flag under test.
///
/// CRITICAL invariant (asserted by `ActempoArmConfigTests`): for a given signal
/// the two arms deviate ONLY in that signal's single boolean
/// (`audioForensicsEnabled` for xsdz.8, `temporalRegularizationEnabled` for
/// xsdz.10); every other field is byte-identical and equal to the production
/// default. A drift on any OTHER field would attribute a precision change to a
/// signal that some OTHER flag actually caused — so the isolation is the
/// load-bearing correctness property the hermetic tests pin. The temporal-reg
/// tuning knobs (`temporalNeighborWindowSeconds` / `…NeighborThreshold` /
/// `…IsolationPenaltyFactor` / `…MinDwellSeconds` / `…MinDwellPenaltyFactor`)
/// stay at their production defaults in BOTH arms; only the master flag flips.
enum ActempoArmConfig {

    /// Build the full `AdDetectionConfig` for the actempo A/B given the single
    /// signal under test and whether its flag is enabled. Every field other than
    /// the one this signal owns is copied VERBATIM from `AdDetectionConfig.default`
    /// (the production state). The OTHER signal's flag is held at `.default`
    /// (false), so the arms never cross-contaminate.
    static func adDetectionConfig(signal: ActempoSignal, enabled: Bool) -> AdDetectionConfig {
        let p = AdDetectionConfig.default
        let audioForensicsEnabled = (signal == .audioForensics) ? enabled : p.audioForensicsEnabled
        let temporalRegularizationEnabled = (signal == .temporalRegularization) ? enabled : p.temporalRegularizationEnabled
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
            evidenceFragilityPenaltyEnabled: p.evidenceFragilityPenaltyEnabled,
            fragilityThreshold: p.fragilityThreshold,
            fragilityPenalty: p.fragilityPenalty,
            chapterSignalMode: p.chapterSignalMode,
            // THE between-arm difference: exactly ONE of these flips for the arm.
            audioForensicsEnabled: audioForensicsEnabled,
            crossEpisodeMemoryEnabled: p.crossEpisodeMemoryEnabled,
            rhetoricalGrammarEnabled: p.rhetoricalGrammarEnabled,
            crossShowSyndicationEnabled: p.crossShowSyndicationEnabled,
            temporalRegularizationEnabled: temporalRegularizationEnabled,
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

    /// The `AdDetectionConfig` for an actempo arm of a given signal.
    static func adDetectionConfig(signal: ActempoSignal, for arm: ActempoArm) -> AdDetectionConfig {
        adDetectionConfig(signal: signal, enabled: arm.signalEnabled)
    }

    /// The `NarrowingConfig` for EVERY arm: `NarrowingConfig.default` (snap ON —
    /// xsdz.2/.3 are KEPT ON in production, so the baseline keeps snap on too).
    /// The narrowing config never varies in this A/B — only the one signal flag
    /// does. Exposed so the harness wires the same value into every arm's live
    /// runner factory and the isolation test can assert it.
    static func narrowingConfig(for arm: ActempoArm) -> NarrowingConfig {
        _ = arm
        return .default
    }

    /// The names of the two fields ANY actempo signal can vary across its arms
    /// (the two master flags). Used to derive each signal's `comparableFields`.
    static let allSignalFlagNames: Set<String> = [
        "audioForensicsEnabled",
        "temporalRegularizationEnabled",
    ]

    /// The fields the isolation test asserts are byte-identical across a given
    /// signal's two arms (and equal to `AdDetectionConfig.default`): the canonical
    /// `FragilityGateArmConfig.comparableFields` list MINUS the ONE flag this
    /// signal varies. The OTHER signal's flag stays IN the list (it must not move),
    /// so cross-contamination between the two A/Bs is caught. Derived from the
    /// canonical list so it automatically tracks any field added there.
    static func comparableFields(
        for signal: ActempoSignal
    ) -> [(name: String, value: @Sendable (AdDetectionConfig) -> String)] {
        FragilityGateArmConfig.comparableFields.filter { $0.name != signal.varyingFieldName }
    }
}

// MARK: - Actempo per-arm fire tally (playhead-actempo)

/// Per-arm accumulated FIRE instrumentation for a single-signal actempo A/B,
/// across all scored episodes. Carries BOTH signals' fire mechanisms so one type
/// serves both methods; for any given A/B only the measured signal's counters are
/// populated (the other stays 0). Pure value type so the harness accumulates it
/// and serializes it into the JSON dump. Without these, a null A/B result is
/// ambiguous (a metric delta ≤±2 FP is FM intra-run noise on this corpus), so the
/// fire count is what makes the result interpretable.
struct ActempoFireTally: Sendable, Codable, Equatable {
    /// xsdz.8: spans whose ledger carried a positive `.audioForensics` entry.
    var audioForensicsFiredSpans: Int = 0
    /// xsdz.10: candidate detections whose `skipConfidence` the temporal-reg
    /// penalty pass actually changed (the signal's "fire").
    var temporalRegPenaltyAppliedSpans: Int = 0
    /// xsdz.10: candidate detections the temporal-reg pass evaluated for this arm
    /// (the denominator — only counted when the pass actually ran).
    var temporalRegCandidateSpans: Int = 0
    /// Total decoded spans the tap observer saw across this arm's episodes (the
    /// xsdz.8 denominator; populated for both A/Bs since the tap always runs).
    var observedSpans: Int = 0

    /// Fold one episode's audio-forensics tap counts into the arm total.
    mutating func addAudioForensics(_ counts: BrandAppearanceChannelFireCounts) {
        audioForensicsFiredSpans += counts.audioForensicsFiredSpans
        observedSpans += counts.observedSpans
    }

    /// Fold one episode's temporal-reg observer counts into the arm total.
    mutating func addTemporalReg(_ counts: TemporalRegularizationFireCounts) {
        temporalRegPenaltyAppliedSpans += counts.penaltyAppliedSpans
        temporalRegCandidateSpans += counts.candidateSpans
    }
}

// MARK: - Actempo single-signal A/B report (playhead-actempo)

/// A readable, serializable summary of a single-signal actempo A/B (2 arms:
/// baseline + treatment). Pure value type, structurally a sibling of
/// `BrandAppearanceSweepReport` / `FragilitySweepReport`: one row per
/// `ActempoArm`, baseline first, the treatment arm's deltas measured vs the
/// baseline. Reuses Phase A's `SpanF1` / `FusionLiftResult` scorers verbatim — no
/// reimplementation. Each row carries the per-signal FIRE counts so a null lift
/// is interpretable.
///
/// "Delta" is `treatment − baseline`; a NEGATIVE `falsePositives` movement is the
/// goal (fewer FPs) for these precision signals. Undefined metrics propagate to
/// `nil` (never a misleading 0.0). The baseline arm's own deltas are zero.
struct ActempoSweepReport: Sendable, Codable, Equatable {

    /// One arm's raw counts + metrics + fire tally + delta-vs-baseline.
    struct ArmRow: Sendable, Codable, Equatable {
        let arm: String
        let signalEnabled: Bool
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
        // Per-signal fire instrumentation (so a null lift is interpretable).
        let audioForensicsFiredSpans: Int
        let temporalRegPenaltyAppliedSpans: Int
        let temporalRegCandidateSpans: Int
        let observedSpans: Int
        // Deltas vs baseline (count-based span lens).
        let truePositivesDelta: Int
        let falsePositivesDelta: Int
        let missesDelta: Int
        let spanPrecisionDelta: Double?
        let spanRecallDelta: Double?
        let spanF1Delta: Double?
        // Deltas vs baseline (seconds-based coverage lens).
        let coveragePrecisionDelta: Double?
        let coverageRecallDelta: Double?
        let coverageF1Delta: Double?
    }

    /// Which precision signal this A/B measured (so the JSON is self-describing).
    let signal: String
    let episodeCount: Int
    /// Rows in the order the arms were RUN, baseline first.
    let rows: [ArmRow]

    /// Build the report over EXACTLY the arms in `arms`, in the order given
    /// (baseline first). The single-signal A/B passes run `[.baseline, .treatment]`.
    /// `arms` MUST start with `.baseline` (it anchors every delta) and the
    /// dictionaries MUST contain an entry for each arm in `arms`.
    init(
        signal: ActempoSignal,
        episodeCount: Int,
        arms: [ActempoArm],
        accumulators: [ActempoArm: FusionLiftModeAccumulator],
        fireTallies: [ActempoArm: ActempoFireTally]
    ) {
        self.signal = signal.rawValue
        self.episodeCount = episodeCount

        let baselineAcc = accumulators[.baseline] ?? FusionLiftModeAccumulator()
        let baselineSpan = baselineAcc.spanF1()
        let baselineSummary = baselineAcc.summary()

        self.rows = arms.map { arm in
            let acc = accumulators[arm] ?? FusionLiftModeAccumulator()
            let span = acc.spanF1()
            let summary = acc.summary()
            let fire = fireTallies[arm] ?? ActempoFireTally()

            // FusionLiftResult names its sides off/enabled; off = baseline,
            // enabled = this arm, so the delta reads `arm − baseline`.
            let spanLift = FusionLiftResult(off: baselineSpan, enabled: span)
            let coverageLift = FusionLiftResult(off: baselineSummary, enabled: summary)

            return ArmRow(
                arm: arm.rawValue,
                signalEnabled: arm.signalEnabled,
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
                audioForensicsFiredSpans: fire.audioForensicsFiredSpans,
                temporalRegPenaltyAppliedSpans: fire.temporalRegPenaltyAppliedSpans,
                temporalRegCandidateSpans: fire.temporalRegCandidateSpans,
                observedSpans: fire.observedSpans,
                truePositivesDelta: span.truePositives - baselineSpan.truePositives,
                falsePositivesDelta: span.falsePositives - baselineSpan.falsePositives,
                missesDelta: span.misses - baselineSpan.misses,
                spanPrecisionDelta: spanLift.precisionDelta,
                spanRecallDelta: spanLift.recallDelta,
                spanF1Delta: spanLift.f1Delta,
                coveragePrecisionDelta: coverageLift.precisionDelta,
                coverageRecallDelta: coverageLift.recallDelta,
                coverageF1Delta: coverageLift.f1Delta
            )
        }
    }

    /// Render a fixed-width, human-readable table for the test log, including a
    /// fire-count block so a null lift is interpretable.
    func table() -> String {
        func fmt(_ value: Double?) -> String {
            guard let value else { return "n/a" }
            return String(format: "%.4f", value)
        }
        func signed(_ value: Double?) -> String {
            guard let value else { return "n/a" }
            return String(format: "%+.4f", value)
        }
        func signedInt(_ value: Int) -> String { String(format: "%+d", value) }

        var lines: [String] = [
            "=== Actempo Single-Signal A/B (\(signal)) ===",
            "episodes scored: \(episodeCount)",
            "arm          GT  det   TP  FP  miss   spanP    spanR   spanF1   covP     covR",
        ]
        for row in rows {
            lines.append(
                "\(armLabel(row.arm))\(pad(row.groundTruthSpans, 4))\(pad(row.detectedSpans, 5))\(pad(row.truePositives, 5))\(pad(row.falsePositives, 4))\(pad(row.misses, 6))  \(col(fmt(row.spanPrecision)))\(col(fmt(row.spanRecall)))\(col(fmt(row.spanF1)))\(col(fmt(row.coveragePrecision)))\(col(fmt(row.coverageRecall)))"
            )
        }
        lines.append("--- treatment delta (treatment − baseline) ---")
        for row in rows where row.arm != ActempoArm.baseline.rawValue {
            lines.append(
                "\(armLabel(row.arm)) TPΔ=\(signedInt(row.truePositivesDelta)) FPΔ=\(signedInt(row.falsePositivesDelta)) missΔ=\(signedInt(row.missesDelta)) | span pΔ=\(signed(row.spanPrecisionDelta)) rΔ=\(signed(row.spanRecallDelta)) f1Δ=\(signed(row.spanF1Delta)) | cov pΔ=\(signed(row.coveragePrecisionDelta)) rΔ=\(signed(row.coverageRecallDelta))"
            )
        }
        lines.append("--- fire instrumentation (did the signal fire?) ---")
        for row in rows {
            lines.append(
                "\(armLabel(row.arm)) spans=\(row.observedSpans) | xsdz.8 audioForensics fired=\(row.audioForensicsFiredSpans) | xsdz.10 penaltyApplied=\(row.temporalRegPenaltyAppliedSpans)/\(row.temporalRegCandidateSpans)"
            )
        }
        return lines.joined(separator: "\n")
    }

    private func armLabel(_ arm: String) -> String {
        (arm + String(repeating: " ", count: 11)).prefix(11).description
    }

    private func pad(_ value: Int, _ width: Int) -> String {
        let s = String(value)
        return String(repeating: " ", count: max(0, width - s.count)) + s
    }

    private func col(_ s: String) -> String {
        (s + String(repeating: " ", count: 9)).prefix(9).description
    }

    /// Encode the report to pretty-printed, sorted-key JSON for the git-ignored
    /// repo-root dump.
    func jsonData() throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        return try encoder.encode(self)
    }
}
enum FbsignalsSignal: String, Sendable, CaseIterable {
    case crossEpisodeMemory   // xsdz.9
    case perShowThreshold     // xsdz.11

    var label: String {
        switch self {
        case .crossEpisodeMemory: return "xsdz.9 cross-episode-memory"
        case .perShowThreshold: return "xsdz.11 per-show-threshold"
        }
    }

    var varyingFieldName: String {
        switch self {
        case .crossEpisodeMemory: return "crossEpisodeMemoryEnabled"
        case .perShowThreshold: return "perShowThresholdControlEnabled"
        }
    }

    var requiresNegativeFingerprintBank: Bool { self == .crossEpisodeMemory }

    var requiresPerShowThresholdControllerStore: Bool { self == .perShowThreshold }
}

enum FbsignalsArm: String, Sendable, CaseIterable {
    case baseline
    case treatment

    var signalEnabled: Bool {
        switch self {
        case .baseline: return false
        case .treatment: return true
        }
    }
}

enum FbsignalsArmConfig {

    static func adDetectionConfig(signal: FbsignalsSignal, enabled: Bool) -> AdDetectionConfig {
        let p = AdDetectionConfig.default
        let crossEpisodeMemoryEnabled = (signal == .crossEpisodeMemory) ? enabled : p.crossEpisodeMemoryEnabled
        let perShowThresholdControlEnabled = (signal == .perShowThreshold) ? enabled : p.perShowThresholdControlEnabled
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
            evidenceFragilityPenaltyEnabled: p.evidenceFragilityPenaltyEnabled,
            fragilityThreshold: p.fragilityThreshold,
            fragilityPenalty: p.fragilityPenalty,
            chapterSignalMode: p.chapterSignalMode,
            audioForensicsEnabled: p.audioForensicsEnabled,
            crossEpisodeMemoryEnabled: crossEpisodeMemoryEnabled,
            rhetoricalGrammarEnabled: p.rhetoricalGrammarEnabled,
            crossShowSyndicationEnabled: p.crossShowSyndicationEnabled,
            temporalRegularizationEnabled: p.temporalRegularizationEnabled,
            temporalNeighborWindowSeconds: p.temporalNeighborWindowSeconds,
            temporalHighConfidenceNeighborThreshold: p.temporalHighConfidenceNeighborThreshold,
            temporalIsolationPenaltyFactor: p.temporalIsolationPenaltyFactor,
            temporalMinDwellSeconds: p.temporalMinDwellSeconds,
            temporalMinDwellPenaltyFactor: p.temporalMinDwellPenaltyFactor,
            perShowThresholdControlEnabled: perShowThresholdControlEnabled,
            perShowThresholdProportionalGain: p.perShowThresholdProportionalGain,
            perShowThresholdIntegralGain: p.perShowThresholdIntegralGain,
            perShowThresholdMaxOffset: p.perShowThresholdMaxOffset,
            perShowThresholdMinSamples: p.perShowThresholdMinSamples
        )
    }

    static func adDetectionConfig(signal: FbsignalsSignal, for arm: FbsignalsArm) -> AdDetectionConfig {
        adDetectionConfig(signal: signal, enabled: arm.signalEnabled)
    }

    static func narrowingConfig(for arm: FbsignalsArm) -> NarrowingConfig {
        _ = arm
        return .default
    }

    static let allSignalFlagNames: Set<String> = [
        "crossEpisodeMemoryEnabled",
        "perShowThresholdControlEnabled",
    ]

    static func comparableFields(
        for signal: FbsignalsSignal
    ) -> [(name: String, value: @Sendable (AdDetectionConfig) -> String)] {
        FragilityGateArmConfig.comparableFields.filter { $0.name != signal.varyingFieldName }
    }
}

struct FbsignalsFireTally: Sendable, Codable, Equatable {
    var crossEpisodeMemoryPositiveFiredSpans: Int = 0
    var crossEpisodeMemorySuppressedSpans: Int = 0
    var crossEpisodeMemorySuppressionCandidateSpans: Int = 0
    var observedSpans: Int = 0

    var perShowThresholdShiftedSpans: Int = 0
    var perShowThresholdCandidateSpans: Int = 0
    var perShowThresholdOffsetSum: Double = 0

    mutating func addChannelTap(_ counts: BrandAppearanceChannelFireCounts) {
        crossEpisodeMemoryPositiveFiredSpans += counts.crossEpisodeMemoryFiredSpans
        observedSpans += counts.observedSpans
    }

    mutating func addNegativeBankSuppression(_ counts: NegativeBankSuppressionFireCounts) {
        crossEpisodeMemorySuppressedSpans += counts.suppressedSpans
        crossEpisodeMemorySuppressionCandidateSpans += counts.candidateSpans
    }

    mutating func addPerShowThreshold(_ counts: PerShowThresholdOffsetFireCounts) {
        perShowThresholdShiftedSpans += counts.thresholdShiftedSpans
        perShowThresholdCandidateSpans += counts.candidateSpans
        perShowThresholdOffsetSum += counts.resolvedOffset
    }
}

struct FbsignalsSweepReport: Sendable, Codable, Equatable {

    struct ArmRow: Sendable, Codable, Equatable {
        let arm: String
        let signalEnabled: Bool
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
        let crossEpisodeMemoryPositiveFiredSpans: Int
        let crossEpisodeMemorySuppressedSpans: Int
        let crossEpisodeMemorySuppressionCandidateSpans: Int
        let perShowThresholdShiftedSpans: Int
        let perShowThresholdCandidateSpans: Int
        let perShowThresholdOffsetSum: Double
        let observedSpans: Int
        let truePositivesDelta: Int
        let falsePositivesDelta: Int
        let missesDelta: Int
        let spanPrecisionDelta: Double?
        let spanRecallDelta: Double?
        let spanF1Delta: Double?
        let coveragePrecisionDelta: Double?
        let coverageRecallDelta: Double?
        let coverageF1Delta: Double?
    }

    let signal: String
    let episodeCount: Int
    let rows: [ArmRow]

    init(
        signal: FbsignalsSignal,
        episodeCount: Int,
        arms: [FbsignalsArm],
        accumulators: [FbsignalsArm: FusionLiftModeAccumulator],
        fireTallies: [FbsignalsArm: FbsignalsFireTally]
    ) {
        self.signal = signal.rawValue
        self.episodeCount = episodeCount

        let baselineAcc = accumulators[.baseline] ?? FusionLiftModeAccumulator()
        let baselineSpan = baselineAcc.spanF1()
        let baselineSummary = baselineAcc.summary()

        self.rows = arms.map { arm in
            let acc = accumulators[arm] ?? FusionLiftModeAccumulator()
            let span = acc.spanF1()
            let summary = acc.summary()
            let fire = fireTallies[arm] ?? FbsignalsFireTally()

            let spanLift = FusionLiftResult(off: baselineSpan, enabled: span)
            let coverageLift = FusionLiftResult(off: baselineSummary, enabled: summary)

            return ArmRow(
                arm: arm.rawValue,
                signalEnabled: arm.signalEnabled,
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
                crossEpisodeMemoryPositiveFiredSpans: fire.crossEpisodeMemoryPositiveFiredSpans,
                crossEpisodeMemorySuppressedSpans: fire.crossEpisodeMemorySuppressedSpans,
                crossEpisodeMemorySuppressionCandidateSpans: fire.crossEpisodeMemorySuppressionCandidateSpans,
                perShowThresholdShiftedSpans: fire.perShowThresholdShiftedSpans,
                perShowThresholdCandidateSpans: fire.perShowThresholdCandidateSpans,
                perShowThresholdOffsetSum: fire.perShowThresholdOffsetSum,
                observedSpans: fire.observedSpans,
                truePositivesDelta: span.truePositives - baselineSpan.truePositives,
                falsePositivesDelta: span.falsePositives - baselineSpan.falsePositives,
                missesDelta: span.misses - baselineSpan.misses,
                spanPrecisionDelta: spanLift.precisionDelta,
                spanRecallDelta: spanLift.recallDelta,
                spanF1Delta: spanLift.f1Delta,
                coveragePrecisionDelta: coverageLift.precisionDelta,
                coverageRecallDelta: coverageLift.recallDelta,
                coverageF1Delta: coverageLift.f1Delta
            )
        }
    }

    func table() -> String {
        func fmt(_ value: Double?) -> String {
            guard let value else { return "n/a" }
            return String(format: "%.4f", value)
        }
        func signed(_ value: Double?) -> String {
            guard let value else { return "n/a" }
            return String(format: "%+.4f", value)
        }
        func signedInt(_ value: Int) -> String { String(format: "%+d", value) }

        var lines: [String] = [
            "=== Fbsignals Single-Signal A/B (\(signal)) ===",
            "episodes scored: \(episodeCount)",
            "arm          GT  det   TP  FP  miss   spanP    spanR   spanF1   covP     covR",
        ]
        for row in rows {
            lines.append(
                "\(armLabel(row.arm))\(pad(row.groundTruthSpans, 4))\(pad(row.detectedSpans, 5))\(pad(row.truePositives, 5))\(pad(row.falsePositives, 4))\(pad(row.misses, 6))  \(col(fmt(row.spanPrecision)))\(col(fmt(row.spanRecall)))\(col(fmt(row.spanF1)))\(col(fmt(row.coveragePrecision)))\(col(fmt(row.coverageRecall)))"
            )
        }
        lines.append("--- treatment delta (treatment − baseline) ---")
        for row in rows where row.arm != FbsignalsArm.baseline.rawValue {
            lines.append(
                "\(armLabel(row.arm)) TPΔ=\(signedInt(row.truePositivesDelta)) FPΔ=\(signedInt(row.falsePositivesDelta)) missΔ=\(signedInt(row.missesDelta)) | span pΔ=\(signed(row.spanPrecisionDelta)) rΔ=\(signed(row.spanRecallDelta)) f1Δ=\(signed(row.spanF1Delta)) | cov pΔ=\(signed(row.coveragePrecisionDelta)) rΔ=\(signed(row.coverageRecallDelta))"
            )
        }
        lines.append("--- fire instrumentation (did the signal fire? EXPECTED 0 — cold start) ---")
        for row in rows {
            lines.append(
                "\(armLabel(row.arm)) spans=\(row.observedSpans) | xsdz.9 positiveBoost=\(row.crossEpisodeMemoryPositiveFiredSpans) negSuppress=\(row.crossEpisodeMemorySuppressedSpans)/\(row.crossEpisodeMemorySuppressionCandidateSpans) | xsdz.11 thresholdShifted=\(row.perShowThresholdShiftedSpans)/\(row.perShowThresholdCandidateSpans) offsetSum=\(String(format: "%.4f", row.perShowThresholdOffsetSum))"
            )
        }
        return lines.joined(separator: "\n")
    }

    private func armLabel(_ arm: String) -> String {
        (arm + String(repeating: " ", count: 11)).prefix(11).description
    }

    private func pad(_ value: Int, _ width: Int) -> String {
        let s = String(value)
        return String(repeating: " ", count: max(0, width - s.count)) + s
    }

    private func col(_ s: String) -> String {
        (s + String(repeating: " ", count: 9)).prefix(9).description
    }

    func jsonData() throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        return try encoder.encode(self)
    }
}
