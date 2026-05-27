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
///   * xsdz.1 — the lexical-auto-ad qualified track, gated by
///     `AdDetectionConfig.lexicalAutoAdQualifiedThreshold` (0.50 = on; a
///     value `>= autoSkipConfidenceThreshold`, here 2.0, makes the track a
///     no-op — the rule still contributes fusion mass but never promotes
///     alone).
///   * xsdz.2 — inward lexical-cluster region tightening in
///     `TargetedWindowNarrower`, gated by
///     `NarrowingConfig.lexicalClusterSnapEnabled`.
///   * xsdz.3 — lexically-nominated audit windows in the same narrower,
///     gated by the SAME `lexicalClusterSnapEnabled` flag.
///
/// Each arm is defined by exactly TWO orthogonal toggles:
///   * `xsdz1On` → the `lexicalAutoAdQualifiedThreshold` (on = 0.50, off = 2.0).
///   * `xsdz23On` → the `lexicalClusterSnapEnabled` flag (on = true, off =
///     false). xsdz.2 and xsdz.3 share this one flag and CANNOT be separated
///     without new production plumbing, so they always move together as a
///     single arm leg.
///
/// The two endpoints (`baseline` = both off, `alon` = both on) are the
/// cumulative A/B; the two singletons (`xsdz1only`, `xsdz23only`) isolate
/// which feature regresses live ad detection in a single Catalyst run.
enum LexicalScorerArm: String, Sendable, CaseIterable {
    /// Program OFF: xsdz.1 disabled (threshold 2.0) AND xsdz.2/.3 disabled
    /// (`lexicalClusterSnapEnabled: false`). The acoustic-only, no-auto-ad
    /// A/B baseline — the reference every other arm's delta is measured from.
    case baseline
    /// xsdz.1 ONLY: lexical-auto-ad qualified track on (threshold 0.50),
    /// region-tighten + audit-nominate off (`lexicalClusterSnapEnabled:
    /// false`). Isolates the auto-ad track's contribution.
    case xsdz1only
    /// xsdz.2+xsdz.3 ONLY: region-tighten + audit-nominate on
    /// (`lexicalClusterSnapEnabled: true`), auto-ad track off (threshold 2.0).
    /// Isolates the narrower's contribution. The two share one flag so they
    /// move together.
    case xsdz23only
    /// All on: production defaults (threshold 0.50,
    /// `lexicalClusterSnapEnabled: true`) — the current main default and the
    /// cumulative-treatment endpoint.
    case alon

    /// Whether the xsdz.1 lexical-auto-ad qualified track is enabled in this
    /// arm (drives the `lexicalAutoAdQualifiedThreshold`).
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
///     `lexicalAutoAdQualifiedThreshold` toggles xsdz.1), and
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
/// CRITICAL invariant (asserted by the unit tests): the all-on arm
/// (`xsdz1On && xsdz23On`) is byte-identical to production defaults — its
/// threshold == `AdDetectionConfig.default.lexicalAutoAdQualifiedThreshold`
/// and its `NarrowingConfig` == `NarrowingConfig.default`. The other arms
/// deviate ONLY in those two program gates: each toggle moves exactly one
/// field, and every non-gate field stays equal to `.default` across all arms.
/// A mislabeled arm (a toggle wired to the wrong field) would attribute a
/// regression to the wrong feature — so the isolation is the load-bearing
/// correctness property the hermetic tests pin.
enum LexicalScorerArmConfig {

    /// The threshold value that DISABLES the xsdz.1 qualified track. Any
    /// value `>= AdDetectionConfig.autoSkipConfidenceThreshold` (default
    /// 0.80) works; 2.0 is unmistakably "off" and matches the bead text.
    static let disabledQualifiedThreshold: Double = 2.0

    /// The `lexicalAutoAdQualifiedThreshold` for the xsdz.1 toggle: production
    /// default (0.50) when on, `disabledQualifiedThreshold` (2.0) when off.
    static func lexicalAutoAdQualifiedThreshold(xsdz1On: Bool) -> Double {
        xsdz1On
            ? AdDetectionConfig.default.lexicalAutoAdQualifiedThreshold
            : disabledQualifiedThreshold
    }

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
    /// `lexicalAutoAdQualifiedThreshold` is held identical across arms (and to
    /// the values the chapter A/B uses), with `fmBackfillMode: .full` and
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
            lexicalAutoAdQualifiedThreshold: lexicalAutoAdQualifiedThreshold(xsdz1On: xsdz1On),
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

    /// Cumulative-endpoint threshold: both gates move together off `programOn`.
    static func lexicalAutoAdQualifiedThreshold(programOn: Bool) -> Double {
        lexicalAutoAdQualifiedThreshold(xsdz1On: programOn)
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
