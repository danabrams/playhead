// NarlEvalReport.swift
// playhead-narl.1: Report schema and rendering for the counterfactual eval harness.
//
// Writes:
//   .eval-out/narl/<timestamp>/report.json   — versioned schema, machine-readable
//   .eval-out/narl/<timestamp>/report.md     — tables per (show × config × metric)
//   .eval-out/narl/trend.jsonl               — append one row per (show, config, metric)

import Foundation

// MARK: - Report schema

/// Versioned schema. Bump on breaking shape changes.
enum NarlEvalReportSchema {
    static let version: Int = 1
}

/// A (show, config) rollup with all metric families.
struct NarlReportRollup: Sendable, Codable {
    let show: String
    let config: String  // "default" | "allEnabled"
    let episodeCount: Int
    let excludedEpisodeCount: Int
    let windowMetrics: [NarlWindowMetricsAtThreshold]
    let secondLevel: NarlSecondLevelMetrics
    /// Diagnostic totals across episodes.
    let totalLexicalInjectionAdds: Int
    let totalPriorShiftAdds: Int
    let totalEpisodesWithShadowCoverage: Int
    /// gtt9.6: mean-per-episode coverage metrics (ratios are averaged,
    /// FN-second counts are summed across episodes).
    let coverageMetrics: NarlCoverageMetrics
    /// gtt9.6: episodes in this rollup whose `pipelineCoverageFailureAsset`
    /// flag fired.
    let pipelineCoverageFailureAssetCount: Int
}

/// gtt9.7 C1: per-episode counts emitted by `CorrectionNormalizer`, persisted
/// alongside the metric fields so report consumers can see how many raw rows
/// were bucketed where. Previously these counts only surfaced as stdout log
/// lines under `narl.normalizer:`; placing them in the typed report makes
/// before/after deltas (raw → span/whole-asset/boundary/unknown) queryable
/// from `.eval-out/narl/<ts>/report.json` rather than test output.
///
/// Field semantics (see `CorrectionNormalizer.swift`):
///   - `rawCount`            — count of FrozenCorrection rows fed into normalize.
///   - `spanFNCount`         — span-level FN corrections after merge.
///   - `spanFPCount`         — span-level FP corrections after merge.
///   - `wholeAssetVetoCount` — whole-asset vetoes after (assetId, kind) dedup.
///   - `wholeAssetEndorseCount` — whole-asset endorsements after dedup.
///   - `unknownCount`        — rows the normalizer couldn't place (ordinal
///                             exactSpans, unrecognized source+type combos,
///                             malformed scope prefixes). Excluded from
///                             span metrics. Does NOT include Layer B rows.
///   - `boundaryRefinementCount` — rows with correctionType in
///                             {startTooEarly/Late, endTooEarly/Late}.
///   - `layerBCount`         — production-valid show-level scopes
///                             (`sponsorOnShow`/`phraseOnShow`/
///                             `campaignOnShow`/`domainOwnershipOnShow`/
///                             `jingleOnShow`) that the harness does not
///                             yet evaluate against. Tracked separately
///                             from `unknownCount` so an operator can
///                             distinguish "5 valid corrections we don't
///                             yet score" from "5 corrections we failed
///                             to parse". See review S1 (2026-04-23).
struct NarlNormalizerCounts: Sendable, Codable, Equatable {
    let rawCount: Int
    let spanFNCount: Int
    let spanFPCount: Int
    let wholeAssetVetoCount: Int
    let wholeAssetEndorseCount: Int
    let unknownCount: Int
    let boundaryRefinementCount: Int
    let layerBCount: Int

    static let zero = NarlNormalizerCounts(
        rawCount: 0,
        spanFNCount: 0,
        spanFPCount: 0,
        wholeAssetVetoCount: 0,
        wholeAssetEndorseCount: 0,
        unknownCount: 0,
        boundaryRefinementCount: 0,
        layerBCount: 0
    )

    /// Build from a `NormalizedCorrections` result + the raw-row count used
    /// as input. Centralizes the veto/endorse split so callers don't have to
    /// hand-filter `wholeAssetCorrections` at every site.
    init(from normalized: NormalizedCorrections, rawCount: Int) {
        self.rawCount = rawCount
        self.spanFNCount = normalized.spanFN.count
        self.spanFPCount = normalized.spanFP.count
        self.wholeAssetVetoCount = normalized.wholeAssetCorrections
            .filter { $0.kind == .veto }.count
        self.wholeAssetEndorseCount = normalized.wholeAssetCorrections
            .filter { $0.kind == .endorse }.count
        self.unknownCount = normalized.unknownCount
        self.boundaryRefinementCount = normalized.boundaryRefinementCount
        self.layerBCount = normalized.layerBCount
    }

    init(
        rawCount: Int,
        spanFNCount: Int,
        spanFPCount: Int,
        wholeAssetVetoCount: Int,
        wholeAssetEndorseCount: Int,
        unknownCount: Int,
        boundaryRefinementCount: Int,
        layerBCount: Int
    ) {
        self.rawCount = rawCount
        self.spanFNCount = spanFNCount
        self.spanFPCount = spanFPCount
        self.wholeAssetVetoCount = wholeAssetVetoCount
        self.wholeAssetEndorseCount = wholeAssetEndorseCount
        self.unknownCount = unknownCount
        self.boundaryRefinementCount = boundaryRefinementCount
        self.layerBCount = layerBCount
    }

    /// Codable with a default-fallback on `layerBCount` so pre-S1 report
    /// artifacts (gtt9.7 initial land) still decode.
    enum CodingKeys: String, CodingKey {
        case rawCount, spanFNCount, spanFPCount
        case wholeAssetVetoCount, wholeAssetEndorseCount
        case unknownCount, boundaryRefinementCount, layerBCount
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.rawCount = try c.decode(Int.self, forKey: .rawCount)
        self.spanFNCount = try c.decode(Int.self, forKey: .spanFNCount)
        self.spanFPCount = try c.decode(Int.self, forKey: .spanFPCount)
        self.wholeAssetVetoCount = try c.decode(Int.self, forKey: .wholeAssetVetoCount)
        self.wholeAssetEndorseCount = try c.decode(Int.self, forKey: .wholeAssetEndorseCount)
        self.unknownCount = try c.decode(Int.self, forKey: .unknownCount)
        self.boundaryRefinementCount = try c.decode(Int.self, forKey: .boundaryRefinementCount)
        self.layerBCount = (try? c.decode(Int.self, forKey: .layerBCount)) ?? 0
    }
}

/// Per-episode entry in the report (one row per trace × config).
struct NarlReportEpisodeEntry: Sendable, Codable {
    let episodeId: String
    let podcastId: String
    let show: String
    let config: String
    let isExcluded: Bool
    let exclusionReason: String?
    let groundTruthWindowCount: Int
    let predictedWindowCount: Int
    let windowMetrics: [NarlWindowMetricsAtThreshold]
    let secondLevel: NarlSecondLevelMetrics
    let lexicalInjectionAdds: Int
    let priorShiftAdds: Int
    let hasShadowCoverage: Bool
    /// gtt9.6: per-episode coverage + FN-rate metrics.
    let coverageMetrics: NarlCoverageMetrics
    /// gtt9.6: per-GT-span FN decomposition. Excluded episodes carry `[]`.
    let fnDecomposition: [NarlFNDecomp]
    /// gtt9.7 C1: per-episode counts emitted by `CorrectionNormalizer`. This
    /// is observability-only — the metric fields above are still derived from
    /// the existing NarlGroundTruth pipeline; the normalizer's output is
    /// carried alongside so the delta is visible in the persisted report.
    let normalizerCounts: NarlNormalizerCounts
    /// gtt9.21: detector-version stamp from the FrozenTrace this entry
    /// summarizes. Empty string for pre-gtt9.21 captures. Carried per-
    /// episode (not just per-rollup) because a real-data eval can pull
    /// from a mid-rollout fleet where one show's episodes were captured
    /// across multiple binaries.
    let detectorVersion: String
    /// gtt9.21: build-commit-SHA stamp from the FrozenTrace.
    let buildCommitSHA: String
    /// playhead-jif5: gtt9.15 pipeline-coverage classification of the
    /// underlying trace, persisted onto the entry so the rollup
    /// aggregator (and the renderer's "Excluded episodes" section) can
    /// filter out `pipeline-coverage-limited` episodes without re-
    /// classifying. Defaults to `.unknown` for pre-jif5 report artifacts.
    let pipelineCoverageBucket: NarlPipelineCoverageBucket

    init(
        episodeId: String,
        podcastId: String,
        show: String,
        config: String,
        isExcluded: Bool,
        exclusionReason: String?,
        groundTruthWindowCount: Int,
        predictedWindowCount: Int,
        windowMetrics: [NarlWindowMetricsAtThreshold],
        secondLevel: NarlSecondLevelMetrics,
        lexicalInjectionAdds: Int,
        priorShiftAdds: Int,
        hasShadowCoverage: Bool,
        coverageMetrics: NarlCoverageMetrics = .zero,
        fnDecomposition: [NarlFNDecomp] = [],
        normalizerCounts: NarlNormalizerCounts = .zero,
        detectorVersion: String = "",
        buildCommitSHA: String = "",
        pipelineCoverageBucket: NarlPipelineCoverageBucket = .unknown
    ) {
        self.episodeId = episodeId
        self.podcastId = podcastId
        self.show = show
        self.config = config
        self.isExcluded = isExcluded
        self.exclusionReason = exclusionReason
        self.groundTruthWindowCount = groundTruthWindowCount
        self.predictedWindowCount = predictedWindowCount
        self.windowMetrics = windowMetrics
        self.secondLevel = secondLevel
        self.lexicalInjectionAdds = lexicalInjectionAdds
        self.priorShiftAdds = priorShiftAdds
        self.hasShadowCoverage = hasShadowCoverage
        self.coverageMetrics = coverageMetrics
        self.fnDecomposition = fnDecomposition
        self.normalizerCounts = normalizerCounts
        self.detectorVersion = detectorVersion
        self.buildCommitSHA = buildCommitSHA
        self.pipelineCoverageBucket = pipelineCoverageBucket
    }

    /// Codable default handling: older `report.json` artifacts written before
    /// gtt9.7 do not carry a `normalizerCounts` block. Decode gracefully by
    /// defaulting the field to `.zero`. Encode always emits the new field.
    enum CodingKeys: String, CodingKey {
        case episodeId, podcastId, show, config, isExcluded, exclusionReason
        case groundTruthWindowCount, predictedWindowCount
        case windowMetrics, secondLevel
        case lexicalInjectionAdds, priorShiftAdds, hasShadowCoverage
        case coverageMetrics, fnDecomposition
        case normalizerCounts
        case detectorVersion, buildCommitSHA
        case pipelineCoverageBucket
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.episodeId = try c.decode(String.self, forKey: .episodeId)
        self.podcastId = try c.decode(String.self, forKey: .podcastId)
        self.show = try c.decode(String.self, forKey: .show)
        self.config = try c.decode(String.self, forKey: .config)
        self.isExcluded = try c.decode(Bool.self, forKey: .isExcluded)
        self.exclusionReason = try c.decodeIfPresent(String.self, forKey: .exclusionReason)
        self.groundTruthWindowCount = try c.decode(Int.self, forKey: .groundTruthWindowCount)
        self.predictedWindowCount = try c.decode(Int.self, forKey: .predictedWindowCount)
        self.windowMetrics = try c.decode([NarlWindowMetricsAtThreshold].self, forKey: .windowMetrics)
        self.secondLevel = try c.decode(NarlSecondLevelMetrics.self, forKey: .secondLevel)
        self.lexicalInjectionAdds = try c.decode(Int.self, forKey: .lexicalInjectionAdds)
        self.priorShiftAdds = try c.decode(Int.self, forKey: .priorShiftAdds)
        self.hasShadowCoverage = try c.decode(Bool.self, forKey: .hasShadowCoverage)
        self.coverageMetrics = (try? c.decode(NarlCoverageMetrics.self, forKey: .coverageMetrics)) ?? .zero
        self.fnDecomposition = (try? c.decode([NarlFNDecomp].self, forKey: .fnDecomposition)) ?? []
        self.normalizerCounts = (try? c.decode(NarlNormalizerCounts.self, forKey: .normalizerCounts)) ?? .zero
        self.detectorVersion = (try? c.decode(String.self, forKey: .detectorVersion)) ?? ""
        self.buildCommitSHA = (try? c.decode(String.self, forKey: .buildCommitSHA)) ?? ""
        self.pipelineCoverageBucket = (try? c.decode(NarlPipelineCoverageBucket.self,
                                                     forKey: .pipelineCoverageBucket)) ?? .unknown
    }
}

struct NarlEvalReport: Sendable, Codable {
    let schemaVersion: Int
    let generatedAt: Date
    let runId: String
    let iouThresholds: [Double]
    let rollups: [NarlReportRollup]
    let episodes: [NarlReportEpisodeEntry]
    let notes: [String]
    /// gtt9.8 follow-up: per-`analysisState` stratification. The bundled
    /// ALL/Conan/DoaC rollups hide a real signal — episodes that
    /// completed transcription fully (`completeFull`) should have
    /// systematically different detection quality than episodes that
    /// stopped early. Bucketing by `trace.analysisState` lets the
    /// downstream reader compare "pre-9.1.1 partial captures" vs
    /// "post-9.1.1 full captures" without them diluting each other in
    /// the aggregate.
    ///
    /// Optional for Codable back-compat: older `report.json` artifacts
    /// written before this field existed decode with `nil`.
    let terminalReasonBuckets: [NarlReportTerminalReasonRollup]?

    /// gtt9.15: coverage-aware 3-bucket classifier rollup. One row per
    /// `NarlPipelineCoverageBucket` (always 3 rows in canonical order:
    /// scoring → pipeline → unknown) with a per-bucket episode count.
    /// Coarser than `terminalReasonBuckets` — answers the headline
    /// 2026-04-24 Finding 1 question (full-coverage miss vs
    /// partial-coverage miss) at a glance.
    ///
    /// Optional for Codable back-compat: older `report.json` artifacts
    /// written before this field existed decode with `nil`.
    let pipelineCoverageBuckets: [NarlPipelineCoverageBucketRollup]?

    init(
        schemaVersion: Int,
        generatedAt: Date,
        runId: String,
        iouThresholds: [Double],
        rollups: [NarlReportRollup],
        episodes: [NarlReportEpisodeEntry],
        notes: [String],
        terminalReasonBuckets: [NarlReportTerminalReasonRollup]? = nil,
        pipelineCoverageBuckets: [NarlPipelineCoverageBucketRollup]? = nil
    ) {
        self.schemaVersion = schemaVersion
        self.generatedAt = generatedAt
        self.runId = runId
        self.iouThresholds = iouThresholds
        self.rollups = rollups
        self.episodes = episodes
        self.notes = notes
        self.terminalReasonBuckets = terminalReasonBuckets
        self.pipelineCoverageBuckets = pipelineCoverageBuckets
    }

    /// Codable default handling: older `report.json` artifacts don't
    /// carry `terminalReasonBuckets` / `pipelineCoverageBuckets`.
    /// Decode to `nil`. Encode uses `encodeIfPresent` so a nil value
    /// round-trips as an absent key rather than `"...": null` — keeps
    /// the wire format identical to the pre-field shape when the
    /// aggregator is opted out.
    enum CodingKeys: String, CodingKey {
        case schemaVersion, generatedAt, runId, iouThresholds
        case rollups, episodes, notes
        case terminalReasonBuckets
        case pipelineCoverageBuckets
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            schemaVersion: try c.decode(Int.self, forKey: .schemaVersion),
            generatedAt: try c.decode(Date.self, forKey: .generatedAt),
            runId: try c.decode(String.self, forKey: .runId),
            iouThresholds: try c.decode([Double].self, forKey: .iouThresholds),
            rollups: try c.decode([NarlReportRollup].self, forKey: .rollups),
            episodes: try c.decode([NarlReportEpisodeEntry].self, forKey: .episodes),
            notes: try c.decode([String].self, forKey: .notes),
            terminalReasonBuckets: try c.decodeIfPresent(
                [NarlReportTerminalReasonRollup].self,
                forKey: .terminalReasonBuckets
            ),
            pipelineCoverageBuckets: try c.decodeIfPresent(
                [NarlPipelineCoverageBucketRollup].self,
                forKey: .pipelineCoverageBuckets
            )
        )
    }

    func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(schemaVersion, forKey: .schemaVersion)
        try c.encode(generatedAt, forKey: .generatedAt)
        try c.encode(runId, forKey: .runId)
        try c.encode(iouThresholds, forKey: .iouThresholds)
        try c.encode(rollups, forKey: .rollups)
        try c.encode(episodes, forKey: .episodes)
        try c.encode(notes, forKey: .notes)
        try c.encodeIfPresent(terminalReasonBuckets, forKey: .terminalReasonBuckets)
        try c.encodeIfPresent(pipelineCoverageBuckets, forKey: .pipelineCoverageBuckets)
    }
}

// MARK: - Terminal-reason stratification (gtt9.8 follow-up)

/// Bucket key for the new terminalReason stratification. Values mirror
/// the six canonical `SessionState` terminal rawValues plus an
/// `.unknown` catch-all covering pre-9.8 captures (where
/// `trace.analysisState` is nil) and any non-terminal state that leaked
/// into a FrozenTrace (e.g. a capture whose asset was still in
/// `backfill` / `spooling` at snapshot time).
///
/// Rationale: `terminalReason` in the lifecycle log is prose
/// ("full coverage: transcript 1.000, feature 1.000") — useful for
/// humans but unstable as a bucket key. `analysisState` is the
/// canonical, code-grade SessionState.rawValue emitted on the same row
/// and is what the harness's `adjustPipelineFailureFlag` already keys
/// on. We name the field/bucket set "terminalReason" to match how the
/// user describes the cohort ("stratify by why the pipeline ended"),
/// while the classifier's underlying key is `analysisState`.
enum NarlTerminalReasonBucket: String, Sendable, Codable, CaseIterable {
    case completeFull
    case completeFeatureOnly
    case completeTranscriptPartial
    case cancelledBudget
    case failedTranscript
    case failedFeature
    /// Catch-all for (a) pre-9.8 captures whose FrozenTrace carries
    /// `analysisState == nil`, and (b) any non-canonical or non-terminal
    /// state (backfill, spooling, the deprecated monolithic `.complete`,
    /// etc.). Crucially this bucket is NOT silently dropped — most
    /// 2026-04-24 fixtures currently fall into it.
    case unknown

    /// Resolve a trace to its bucket. The only inputs are
    /// `trace.analysisState`; `trace.terminalReason` (the human-readable
    /// string) is deliberately NOT consulted — it's descriptive, not
    /// categorical, and a partial-coverage terminal can carry a nil
    /// `terminalReason` even when `analysisState` is set (the 34C7E7CF
    /// stall in the 2026-04-23 lifecycle log is the canonical example).
    static func classify(_ trace: FrozenTrace) -> NarlTerminalReasonBucket {
        guard let state = trace.analysisState else { return .unknown }
        return NarlTerminalReasonBucket(rawValue: state) ?? .unknown
    }

    /// Lifecycle-ordered sort index for report rendering. Buckets read
    /// naturally in pipeline-exit-severity order: fully-successful
    /// outcomes first, partial outcomes next, failures last, unknown
    /// as the catch-all tail. Alphabetical rawValue ordering would
    /// interleave "cancelledBudget" ahead of "completeFull" — visually
    /// wrong for a stratified dashboard.
    var sortOrder: Int {
        switch self {
        case .completeFull: return 0
        case .completeFeatureOnly: return 1
        case .completeTranscriptPartial: return 2
        case .cancelledBudget: return 3
        case .failedTranscript: return 4
        case .failedFeature: return 5
        case .unknown: return 6
        }
    }
}

/// A (bucket × config) rollup. Shape mirrors `NarlReportRollup` but
/// keyed on `bucket` rather than `show`, and carries only the metric
/// families that are meaningful at this cohort resolution. Coverage-
/// derived AutoSkip Prec/Recall are hoisted into first-class fields
/// (rather than nested under `coverageMetrics`) so a downstream `jq`
/// reader can pull them without needing to know the aggregation rule.
struct NarlReportTerminalReasonRollup: Sendable, Codable, Equatable {
    /// The canonical bucket key.
    let bucket: NarlTerminalReasonBucket
    /// `"default"` | `"allEnabled"` — one rollup row per bucket × config.
    let config: String
    /// Number of non-excluded episode entries in this bucket.
    let episodeCount: Int
    /// Number of excluded entries (whole-asset veto) in this bucket.
    let excludedEpisodeCount: Int
    /// Window-level metrics at each τ in {0.3, 0.5, 0.7}. Same semantics
    /// as `NarlReportRollup.windowMetrics`: corpus-level pool of TP/FP
    /// across all episodes in the bucket.
    let windowMetrics: [NarlWindowMetricsAtThreshold]
    /// Second-level (Sec-F1) metrics, same corpus-level pool.
    let secondLevel: NarlSecondLevelMetrics
    /// Corpus-pooled AutoSkip precision across non-excluded episodes
    /// in the bucket: sum(TP seconds) / sum(TP + FP seconds). 0.0 when
    /// the bucket has no predicted auto-skip seconds. The headline
    /// user-facing number per the 2026-04-24 findings §5. (Previously
    /// a per-episode mean; pooled to match the show-rollup convention
    /// where small-bucket means diverge materially from the true
    /// corpus number.)
    let autoSkipPrecision: Double
    /// Corpus-pooled AutoSkip recall: sum(TP seconds) / sum(TP + FN
    /// seconds). 0.0 when the bucket has no GT seconds.
    let autoSkipRecall: Double

    /// Stratify a set of episode entries by bucket × config, producing
    /// one rollup row per non-empty bucket. Empty buckets are elided —
    /// a zero-episode row would just be noise in the rendered report.
    ///
    /// - Parameters:
    ///   - traces: every FrozenTrace loaded by the harness. Used to
    ///     resolve `episodeId → analysisState` so the bucket key is
    ///     derived from lifecycle-log truth, not inferred from the
    ///     entry.
    ///   - entries: every `NarlReportEpisodeEntry` emitted by the
    ///     harness (one per trace × config). Excluded entries are
    ///     counted under `excludedEpisodeCount` but never folded into
    ///     the metric aggregation — same rule as `NarlReportRollup`.
    ///   - pipelinesByEpisodeId: map from `"<episodeId>|<config>"` to
    ///     the list of (predicted, groundTruth) window pairs for that
    ///     key. The list handles the case where the same episodeId
    ///     appears in multiple fixture date-dirs (e.g. the Conan
    ///     117-min asset captured on both 2026-04-23 and 2026-04-24) —
    ///     each copy contributes its own (pred, gt) to the bucket
    ///     pool. The aggregator pools all lists across every episode
    ///     in the bucket and computes corpus-level metrics — identical
    ///     rule to `NarlEvalHarnessTests.runHarnessCollectingReport`'s
    ///     show-rollup loop. Missing keys contribute empty lists.
    static func stratify(
        traces: [FrozenTrace],
        entries: [NarlReportEpisodeEntry],
        pipelinesByEpisodeId: [String: [(pred: [NarlTimeRange], gt: [NarlTimeRange])]]
    ) -> [NarlReportTerminalReasonRollup] {
        // Build episodeId → bucket lookup. The same episodeId can
        // appear in multiple date-dirs of the fixture tree (e.g. the
        // Conan 117-min asset `flightcast:01KM20W...` captured on both
        // 2026-04-23 and 2026-04-24). When the bucket differs across
        // duplicates, prefer the NON-`.unknown` classification so a
        // later 9.8 capture doesn't get masked by an earlier pre-9.8
        // copy of the same episode. Equal buckets collapse into
        // themselves.
        var bucketByEpisodeId: [String: NarlTerminalReasonBucket] = [:]
        for trace in traces {
            let bucket = NarlTerminalReasonBucket.classify(trace)
            if let existing = bucketByEpisodeId[trace.episodeId] {
                if existing == .unknown && bucket != .unknown {
                    bucketByEpisodeId[trace.episodeId] = bucket
                }
            } else {
                bucketByEpisodeId[trace.episodeId] = bucket
            }
        }

        struct Aggregate {
            var episodeCount: Int = 0
            var excludedEpisodeCount: Int = 0
            var allPred: [NarlTimeRange] = []
            var allGt: [NarlTimeRange] = []
            // Corpus-pooled AutoSkip counters (seconds-granularity).
            // TP = predicted auto-skip seconds that overlap GT.
            // FP = predicted auto-skip seconds outside GT.
            // FN = GT seconds not covered by predicted auto-skip.
            // Accumulated per-episode (no cross-episode merge) so the
            // rollup pools sums the same way the show-rollup pools
            // allPred/allGt in NarlEvalHarnessTests — precision/recall
            // divide the sums rather than averaging per-episode rates.
            var autoSkipTpSec: Double = 0
            var autoSkipFpSec: Double = 0
            var autoSkipFnSec: Double = 0
        }

        // Track which (episodeId, config) pairs we've already consumed
        // from `pipelinesByEpisodeId`. Each entry pops one (pred, gt)
        // from the per-key queue in order, so duplicate entries (same
        // episodeId captured on multiple days) each get their own
        // pair — no double-counting, no dropped episode.
        var pipelineCursors: [String: Int] = [:]

        var buckets: [String: Aggregate] = [:]
        for entry in entries {
            let bucket = bucketByEpisodeId[entry.episodeId] ?? .unknown
            let key = "\(bucket.rawValue)|\(entry.config)"
            var agg = buckets[key] ?? Aggregate()
            if entry.isExcluded {
                agg.excludedEpisodeCount += 1
            } else {
                agg.episodeCount += 1
                let pipeKey = "\(entry.episodeId)|\(entry.config)"
                let cursor = pipelineCursors[pipeKey] ?? 0
                if let pipes = pipelinesByEpisodeId[pipeKey], cursor < pipes.count {
                    let pipe = pipes[cursor]
                    agg.allPred.append(contentsOf: pipe.pred)
                    agg.allGt.append(contentsOf: pipe.gt)
                    // Per-episode seconds-granularity TP / FP / FN for
                    // AutoSkip. Summing these across episodes and
                    // dividing at emit time corpus-pools precision /
                    // recall instead of mean-of-ratios — matches the
                    // show-rollup's allPred/allGt pooling convention.
                    let predTotal = NarlCoverageMetricsCompute.unionLength(pipe.pred)
                    let gtTotal = NarlCoverageMetricsCompute.unionLength(pipe.gt)
                    let tpSec = NarlCoverageMetricsCompute.overlapLength(pipe.pred, pipe.gt)
                    agg.autoSkipTpSec += tpSec
                    agg.autoSkipFpSec += max(0, predTotal - tpSec)
                    agg.autoSkipFnSec += max(0, gtTotal - tpSec)
                    pipelineCursors[pipeKey] = cursor + 1
                }
            }
            buckets[key] = agg
        }

        let rollups: [NarlReportTerminalReasonRollup] = buckets
            .compactMap { key, agg -> NarlReportTerminalReasonRollup? in
                // Elide buckets with zero episodes AND zero excluded — they
                // carry no information. A bucket with only excluded entries
                // still surfaces the excluded count (parity with the show-
                // rollup "excluded-only" emission in the main harness).
                if agg.episodeCount == 0 && agg.excludedEpisodeCount == 0 { return nil }
                let parts = key.split(separator: "|", maxSplits: 1).map(String.init)
                guard parts.count == 2,
                      let bucket = NarlTerminalReasonBucket(rawValue: parts[0]) else {
                    return nil
                }
                let winMetrics = [0.3, 0.5, 0.7].map { τ in
                    NarlWindowMetrics.compute(
                        predicted: agg.allPred, groundTruth: agg.allGt, threshold: τ
                    )
                }
                let secMetrics = NarlSecondLevel.compute(
                    predicted: agg.allPred, groundTruth: agg.allGt
                )
                // Corpus-pool AutoSkip precision/recall: divide the
                // summed TP/FP/FN seconds rather than averaging per-
                // episode rates. Zero-guard falls back to 0.0 — matches
                // NarlCoverageMetricsCompute.compute's convention at the
                // per-episode level (empty-denominator → 0, not NaN).
                let precDenom = agg.autoSkipTpSec + agg.autoSkipFpSec
                let recDenom = agg.autoSkipTpSec + agg.autoSkipFnSec
                let autoSkipPrec = precDenom > 0 ? agg.autoSkipTpSec / precDenom : 0.0
                let autoSkipRec = recDenom > 0 ? agg.autoSkipTpSec / recDenom : 0.0
                return NarlReportTerminalReasonRollup(
                    bucket: bucket,
                    config: parts[1],
                    episodeCount: agg.episodeCount,
                    excludedEpisodeCount: agg.excludedEpisodeCount,
                    windowMetrics: winMetrics,
                    secondLevel: secMetrics,
                    autoSkipPrecision: autoSkipPrec,
                    autoSkipRecall: autoSkipRec
                )
            }
            // Lifecycle-ordered rendering: primary key is the bucket's
            // sortOrder (completeFull → … → unknown), secondary key is
            // the config name for stability across runs. Previously the
            // sort was alphabetical on "<rawValue>|<config>", which put
            // cancelledBudget ahead of completeFull — visually wrong
            // for a stratified dashboard read.
            .sorted { lhs, rhs in
                if lhs.bucket.sortOrder != rhs.bucket.sortOrder {
                    return lhs.bucket.sortOrder < rhs.bucket.sortOrder
                }
                return lhs.config < rhs.config
            }

        return rollups
    }
}

// MARK: - Pipeline-coverage bucketing (gtt9.15)

/// Tunable knobs for `NarlPipelineCoverageBucket.classify(_:thresholds:)`.
///
/// Introduced by playhead-ahez to give harness thresholds a named home
/// rather than a bare static constant. A second coverage-related
/// threshold is expected shortly (e.g. a different bucket boundary or
/// a custom-eval run); routing both through one struct means the
/// plumbing already exists when that second field arrives — no churn
/// in the classifier's public signature at that point.
///
/// Intentionally minimal: only `scoringLimitedCoverageFloor` today
/// (promoted from the previous `scoringLimitedRatioThreshold` static
/// constant). Do not preemptively add fields — the whole point of the
/// struct is to have a canonical place for them when real callers
/// need them.
///
/// Not Codable: this is an in-memory config type, not a field on any
/// persisted report schema. The report's existing wire format is
/// unaffected.
struct NarlPipelineCoverageThresholds: Sendable, Equatable {
    /// Coverage-end ratio at which we treat the pipeline as having run
    /// to completion. The 0.95 floor mirrors the gtt9.15 spec — leaves
    /// a margin for the few seconds of trailing silence that production
    /// commonly truncates from the analysis window.
    let scoringLimitedCoverageFloor: Double

    /// Production defaults — current behavior at gtt9.15 land.
    static let `default` = NarlPipelineCoverageThresholds(
        scoringLimitedCoverageFloor: 0.95
    )
}

/// Coarser 3-bucket classifier on top of the gtt9.8 lifecycle fields.
/// Whereas `NarlTerminalReasonBucket` keys on the canonical 6 SessionState
/// rawValues (useful but operator-noisy), this bucket answers a single
/// product-relevant question:
///
///   "When the harness scores a miss, is it a scoring deficit or a
///    coverage deficit?"
///
/// Concretely:
///   - `.scoringLimited` — the pipeline ran transcript coverage to
///     completion (terminalReason mentions "full coverage" OR
///     fastTranscriptCoverageEndTime ≥ durationSec *
///     `thresholds.scoringLimitedCoverageFloor`; production default
///     0.95). Any FN on this asset is a classifier / promotion / fusion
///     problem, NOT a coverage problem.
///   - `.pipelineCoverageLimited` — the pipeline stopped before the
///     episode end (transcript-coverage-end < threshold) OR
///     terminalReason indicates a coverage failure. Misses on this
///     asset are unattributable to scoring quality.
///   - `.unknown` — lifecycle fields are absent (pre-gtt9.8 fixtures or
///     a degenerate 0/nil duration). Reported but never silently
///     dropped — most 2026-04-22/-23 fixtures land here.
///
/// Why a separate enum from `NarlTerminalReasonBucket`: the per-state
/// stratification answers "what state did the asset terminate in?";
/// this 3-bucket classifier answers the headline cohort split for
/// the 2026-04-24 Finding 1 question (71F0C2AE full-coverage missed
/// vs 34C7E7CF partial-coverage missed). Both shapes coexist in the
/// report — they're complementary, not redundant.
enum NarlPipelineCoverageBucket: String, Sendable, Codable, CaseIterable {
    case scoringLimited = "scoring-limited"
    case pipelineCoverageLimited = "pipeline-coverage-limited"
    case unknown

    /// Substrings in `terminalReason` that signal a pipeline coverage
    /// failure. Matched case-sensitively to mirror the production
    /// formatter (see `Playhead/Services/AnalysisCoordinator/AnalysisCoordinator.swift`
    /// for the producer of these strings).
    static let coverageFailureReasonNeedles: [String] = [
        "transcript coverage ",
        "feature coverage ",
        "coverage failure",
    ]

    /// Substring in `terminalReason` that signals full coverage. The
    /// production formatter writes
    /// `"full coverage: transcript X, feature Y"` on the
    /// completeFull terminal row.
    static let fullCoverageReasonNeedle: String = "full coverage"

    /// Classify a single trace. Order of evaluation:
    ///   1. terminalReason contains "full coverage"           → scoring
    ///   2. terminalReason contains a coverage-failure needle → pipeline
    ///   3. ratio = fastTranscriptCoverageEndTime / durationSec
    ///        a. ratio ≥ thresholds.scoringLimitedCoverageFloor → scoring
    ///        b. 0 < ratio                                    → pipeline
    ///   4. otherwise (no usable signal)                      → unknown
    ///
    /// `thresholds` defaults to `.default` (production: 0.95 floor) so
    /// existing call-sites are source-compatible. Pass a custom
    /// `NarlPipelineCoverageThresholds` to rebucket for a what-if
    /// eval (playhead-ahez).
    static func classify(
        _ trace: FrozenTrace,
        thresholds: NarlPipelineCoverageThresholds = .default
    ) -> NarlPipelineCoverageBucket {
        if let reason = trace.terminalReason {
            if reason.contains(fullCoverageReasonNeedle) {
                return .scoringLimited
            }
            if coverageFailureReasonNeedles.contains(where: { reason.contains($0) }) {
                return .pipelineCoverageLimited
            }
        }
        // Need both duration and coverage-end to compute a ratio. A 0
        // or negative duration is degenerate — bucket as unknown rather
        // than divide-by-zero into a NaN ratio. (A nil coverage-end with
        // a positive duration falls through to the same bucket: we
        // can't honestly call it scoring or pipeline-limited without
        // either a reason hit or a coverage measurement.)
        guard let duration = trace.durationSec, duration > 0,
              let coverageEnd = trace.fastTranscriptCoverageEndTime
        else {
            return .unknown
        }
        let ratio = coverageEnd / duration
        if ratio >= thresholds.scoringLimitedCoverageFloor {
            return .scoringLimited
        }
        return .pipelineCoverageLimited
    }

    /// Tally traces into the canonical 3-bucket ordering. Always
    /// returns 3 entries (zero-count buckets included) so report
    /// consumers can rely on a stable shape.
    ///
    /// `thresholds` defaults to `.default`; pass a custom value to
    /// re-bucket a fixture corpus under alternate harness settings
    /// without touching the classifier at each call-site.
    static func countsPerBucket(
        traces: [FrozenTrace],
        thresholds: NarlPipelineCoverageThresholds = .default
    ) -> [NarlPipelineCoverageBucketRollup] {
        var counts: [NarlPipelineCoverageBucket: Int] = [
            .scoringLimited: 0,
            .pipelineCoverageLimited: 0,
            .unknown: 0,
        ]
        for trace in traces {
            counts[classify(trace, thresholds: thresholds), default: 0] += 1
        }
        // Canonical order: scoring → pipeline → unknown. The case order
        // in `allCases` matches this ordering, but spell it out
        // explicitly so a future re-order of the enum can't silently
        // scramble the output.
        return [
            NarlPipelineCoverageBucketRollup(bucket: .scoringLimited,
                                             count: counts[.scoringLimited] ?? 0),
            NarlPipelineCoverageBucketRollup(bucket: .pipelineCoverageLimited,
                                             count: counts[.pipelineCoverageLimited] ?? 0),
            NarlPipelineCoverageBucketRollup(bucket: .unknown,
                                             count: counts[.unknown] ?? 0),
        ]
    }
}

/// Per-bucket count rollup row. Tiny by design — the bucket carries the
/// classification semantics; this struct just pairs it with a count.
struct NarlPipelineCoverageBucketRollup: Sendable, Codable, Equatable {
    let bucket: NarlPipelineCoverageBucket
    let count: Int
}

// MARK: - Trend log row

/// One row in `.eval-out/narl/trend.jsonl`. We emit many per run (one per
/// show × config × metric). Reading via `jq` or the bead's approval engine
/// is straightforward.
struct NarlTrendRow: Sendable, Codable {
    let schemaVersion: Int
    let runId: String
    let generatedAt: Date
    let show: String
    let config: String
    let metric: String
    let thresholdTau: Double?
    let value: Double
}

// MARK: - Renderer

enum NarlEvalRenderer {

    /// Render the markdown report: one table per (show × config × metric family).
    static func renderMarkdown(_ report: NarlEvalReport) -> String {
        var out = ""
        out += "# narl counterfactual eval — run \(report.runId)\n\n"
        out += "Generated: \(isoFormatter().string(from: report.generatedAt))\n"
        out += "Schema: v\(report.schemaVersion)\n\n"

        // gtt9.21: capture provenance — list every distinct
        // (detectorVersion, buildCommitSHA) pair across the corpus
        // along with how many episodes each pair contributed. Mixed-
        // binary corpora are common (a real-data eval can pull from
        // a fleet that's mid-rollout); without this section a per-show
        // F1 swing has no way to be attributed to a specific binary.
        // Per-episode columns elsewhere can stratify further if a
        // single show's metrics are split across multiple SHAs.
        let provenanceCounts = report.episodes.reduce(
            into: [String: Int]()
        ) { acc, episode in
            // Empty-string sentinels (pre-gtt9.21 fixtures) collapse
            // to the explicit "(absent)" label so they're not
            // confused with "(empty)" — both render readably in the
            // markdown table.
            let dv = episode.detectorVersion.isEmpty ? "(absent)" : episode.detectorVersion
            let sha = episode.buildCommitSHA.isEmpty ? "(absent)" : episode.buildCommitSHA
            acc["\(dv)|\(sha)", default: 0] += 1
        }
        out += "## Capture provenance\n\n"
        if provenanceCounts.isEmpty {
            // No episodes ⇒ no provenance to render. Print the section
            // anyway so the header word "detectorVersion" appears at
            // least once in the report (acceptance contract).
            out += "_No episodes; detectorVersion / buildCommitSHA unavailable._\n\n"
        } else {
            out += "| detectorVersion | buildCommitSHA | Episodes |\n"
            out += "|---|---|---|\n"
            for (key, count) in provenanceCounts.sorted(by: { $0.key < $1.key }) {
                let parts = key.split(separator: "|", maxSplits: 1).map(String.init)
                let dv = parts.count > 0 ? parts[0] : ""
                let sha = parts.count > 1 ? parts[1] : ""
                out += "| \(dv) | \(sha) | \(count) |\n"
            }
            out += "\n"
        }

        if !report.notes.isEmpty {
            out += "## Notes\n\n"
            for note in report.notes {
                out += "- \(note)\n"
            }
            out += "\n"
        }

        out += "## Summary (rollups)\n\n"
        out += "| Show | Config | Episodes | Excluded | Win F1 @ τ=0.3 | @ 0.5 | @ 0.7 | Second-level F1 | LexInj adds | PriorShift adds | Shadow-covered |\n"
        out += "|---|---|---|---|---|---|---|---|---|---|---|\n"
        for r in report.rollups {
            let f13 = r.windowMetrics.first(where: { abs($0.threshold - 0.3) < 1e-6 })?.f1 ?? 0
            let f15 = r.windowMetrics.first(where: { abs($0.threshold - 0.5) < 1e-6 })?.f1 ?? 0
            let f17 = r.windowMetrics.first(where: { abs($0.threshold - 0.7) < 1e-6 })?.f1 ?? 0
            out += "| \(r.show) | \(r.config) | \(r.episodeCount) | \(r.excludedEpisodeCount) "
            out += "| \(fmt(f13)) | \(fmt(f15)) | \(fmt(f17)) | \(fmt(r.secondLevel.f1)) "
            out += "| \(r.totalLexicalInjectionAdds) | \(r.totalPriorShiftAdds) | \(r.totalEpisodesWithShadowCoverage) |\n"
        }
        out += "\n"

        // Split into metric table (non-excluded) and a deduped excluded list.
        // Each episode appears once per config in `report.episodes`, so without
        // deduping, an excluded episode would show up twice in the summary
        // with identical zeros (LOW-4).
        //
        // playhead-jif5: the per-episode metric table still renders every
        // entry where `isExcluded == false` — pipeline-coverage-limited
        // episodes stay visible there for transparency. The excluded list
        // is union'd with pipeline-coverage-limited entries so the report
        // explains why headline rollups dropped them.
        let included = report.episodes.filter { !$0.isExcluded }
        let excluded = report.episodes.filter {
            $0.isExcluded || $0.pipelineCoverageBucket == .pipelineCoverageLimited
        }

        out += "## Per-episode\n\n"
        out += "| Episode | Podcast | Config | GT | Pred | F1@0.3 | F1@0.5 | F1@0.7 | Sec-F1 | PipelineFail |\n"
        out += "|---|---|---|---|---|---|---|---|---|---|\n"
        for e in included {
            let f13 = e.windowMetrics.first(where: { abs($0.threshold - 0.3) < 1e-6 })?.f1 ?? 0
            let f15 = e.windowMetrics.first(where: { abs($0.threshold - 0.5) < 1e-6 })?.f1 ?? 0
            let f17 = e.windowMetrics.first(where: { abs($0.threshold - 0.7) < 1e-6 })?.f1 ?? 0
            let failMarker = e.coverageMetrics.pipelineCoverageFailureAsset ? "WARN pipelineCoverageFailureAsset" : ""
            out += "| \(e.episodeId) | \(e.podcastId) | \(e.config) | \(e.groundTruthWindowCount) | \(e.predictedWindowCount) "
            out += "| \(fmt(f13)) | \(fmt(f15)) | \(fmt(f17)) | \(fmt(e.secondLevel.f1)) | \(failMarker) |\n"
        }
        out += "\n"

        // gtt9.6: coverage + FN decomposition per-rollup.
        out += "## Coverage + FN decomposition\n\n"
        out += "Transcript coverage is a lower bound — counted only from "
        out += "`lexical` / `fm` / `catalog` evidence sources — until gtt9.8 "
        out += "ships the coverage contract.\n\n"
        out += "| Show | Config | ScoredCov | TranscriptCov | UnscoredFN | "
        out += "PipelineFN (s) | ClassifierFN (s) | PromotionFN (s) | PipelineFailAssets |\n"
        out += "|---|---|---|---|---|---|---|---|---|\n"
        for r in report.rollups {
            let cm = r.coverageMetrics
            out += "| \(r.show) | \(r.config) "
            out += "| \(fmt(cm.scoredCoverageRatio)) "
            out += "| \(fmt(cm.transcriptCoverageRatio)) "
            out += "| \(fmt(cm.unscoredFNRate)) "
            out += "| \(fmt(cm.pipelineCoverageFNSeconds)) "
            out += "| \(fmt(cm.classifierRecallFNSeconds)) "
            out += "| \(fmt(cm.promotionRecallFNSeconds)) "
            out += "| \(r.pipelineCoverageFailureAssetCount) |\n"
        }
        out += "\n"

        // gtt9.8 follow-up: per-analysisState stratification. Emits a
        // table that matches the show-rollup shape but keyed on
        // `bucket` rather than `show`. Only present when the report
        // carries the optional field; older `report.json` artifacts
        // decode with `terminalReasonBuckets == nil` and this block
        // renders as the explanatory paragraph without the table.
        out += "## Terminal-reason stratification\n\n"
        out += "Bucketed by `trace.analysisState` (lifecycle-log truth). "
        out += "Episodes that completed transcription fully (`completeFull`) "
        out += "should be compared to each other, not to partial-coverage "
        out += "or failed captures — the bundled ALL / Conan / DoaC rollups "
        out += "above dilute these cohorts together. `unknown` covers pre-9.8 "
        out += "captures where `analysisState` is nil and any non-terminal "
        out += "state (backfill, spooling, etc.).\n\n"
        if let buckets = report.terminalReasonBuckets, !buckets.isEmpty {
            out += "| Bucket | Config | Episodes | Excluded | Win F1 @ τ=0.3 | @ 0.5 | @ 0.7 | Sec-F1 | AutoSkip Prec | AutoSkip Recall |\n"
            out += "|---|---|---|---|---|---|---|---|---|---|\n"
            for b in buckets {
                let f13 = b.windowMetrics.first(where: { abs($0.threshold - 0.3) < 1e-6 })?.f1 ?? 0
                let f15 = b.windowMetrics.first(where: { abs($0.threshold - 0.5) < 1e-6 })?.f1 ?? 0
                let f17 = b.windowMetrics.first(where: { abs($0.threshold - 0.7) < 1e-6 })?.f1 ?? 0
                out += "| \(b.bucket.rawValue) | \(b.config) | \(b.episodeCount) | \(b.excludedEpisodeCount) "
                out += "| \(fmt(f13)) | \(fmt(f15)) | \(fmt(f17)) | \(fmt(b.secondLevel.f1)) "
                out += "| \(fmt(b.autoSkipPrecision)) | \(fmt(b.autoSkipRecall)) |\n"
            }
            out += "\n"
        } else {
            out += "_No `terminalReasonBuckets` in this run (pre-stratification report or empty corpus)._\n\n"
        }

        // gtt9.15: coverage-aware 3-bucket classification. Coarser
        // than the per-state stratification above — answers the
        // headline 2026-04-24 Finding 1 question (full-coverage miss
        // vs partial-coverage miss) at a glance. Always renders the
        // section header so readers know the field exists; falls back
        // to an explanatory note when the report predates gtt9.15.
        out += "## Pipeline-coverage classification\n\n"
        out += "Bucketed by `terminalReason` + `fastTranscriptCoverageEndTime` "
        out += "/ `durationSec` ratio (≥ 0.95 → scoring-limited). "
        out += "`scoring-limited` means the pipeline ran transcript coverage "
        out += "to completion — any miss is a classifier/promotion/fusion "
        out += "issue, not a coverage issue. `pipeline-coverage-limited` means "
        out += "the asset stopped before episode end. `unknown` covers pre-9.8 "
        out += "captures with no lifecycle telemetry.\n\n"
        if let coverageBuckets = report.pipelineCoverageBuckets, !coverageBuckets.isEmpty {
            out += "| Bucket | Episodes |\n"
            out += "|---|---|\n"
            for b in coverageBuckets {
                out += "| \(b.bucket.rawValue) | \(b.count) |\n"
            }
            out += "\n"
        } else {
            out += "_No `pipelineCoverageBuckets` in this run (pre-gtt9.15 report or empty corpus)._\n\n"
        }

        if !excluded.isEmpty {
            // Dedupe by episodeId — the exclusion reason is the same across
            // configs (ground-truth construction is config-agnostic).
            var seen = Set<String>()
            let dedupedExcluded = excluded.filter { seen.insert($0.episodeId).inserted }
            out += "## Excluded episodes\n\n"
            out += "| Episode | Podcast | Reason |\n"
            out += "|---|---|---|\n"
            for e in dedupedExcluded {
                // playhead-jif5: when an entry is excluded from rollups
                // *only* by the pipeline-coverage classifier (not by a
                // whole-asset veto), surface that as the reason. A whole-
                // asset veto wins precedence — `isExcluded` is the harder
                // signal and may carry a more specific exclusionReason.
                let reason: String
                if e.isExcluded {
                    reason = e.exclusionReason ?? "yes"
                } else {
                    reason = "pipelineCoverageLimited:\(e.episodeId)"
                }
                out += "| \(e.episodeId) | \(e.podcastId) | \(reason) |\n"
            }
            out += "\n"
        }

        return out
    }

    private static func fmt(_ v: Double) -> String {
        if v.isNaN || v.isInfinite { return "-" }
        return String(format: "%.3f", v)
    }

    /// Fresh ISO8601 formatter per call — ISO8601DateFormatter is not Sendable,
    /// so we avoid a module-global static. Rendering is off the hot path.
    private static func isoFormatter() -> ISO8601DateFormatter {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return f
    }
}

// MARK: - Trend log builder

enum NarlTrendLog {
    /// Expand a set of rollups into flat trend rows, one per (show, config,
    /// metric, threshold).
    static func rows(from report: NarlEvalReport) -> [NarlTrendRow] {
        var out: [NarlTrendRow] = []
        for r in report.rollups {
            for w in r.windowMetrics {
                out.append(NarlTrendRow(
                    schemaVersion: report.schemaVersion,
                    runId: report.runId,
                    generatedAt: report.generatedAt,
                    show: r.show,
                    config: r.config,
                    metric: "window_f1",
                    thresholdTau: w.threshold,
                    value: w.f1
                ))
                out.append(NarlTrendRow(
                    schemaVersion: report.schemaVersion,
                    runId: report.runId,
                    generatedAt: report.generatedAt,
                    show: r.show,
                    config: r.config,
                    metric: "window_precision",
                    thresholdTau: w.threshold,
                    value: w.precision
                ))
                out.append(NarlTrendRow(
                    schemaVersion: report.schemaVersion,
                    runId: report.runId,
                    generatedAt: report.generatedAt,
                    show: r.show,
                    config: r.config,
                    metric: "window_recall",
                    thresholdTau: w.threshold,
                    value: w.recall
                ))
                out.append(NarlTrendRow(
                    schemaVersion: report.schemaVersion,
                    runId: report.runId,
                    generatedAt: report.generatedAt,
                    show: r.show,
                    config: r.config,
                    metric: "window_mean_matched_iou",
                    thresholdTau: w.threshold,
                    value: w.meanMatchedIoU
                ))
            }
            out.append(NarlTrendRow(
                schemaVersion: report.schemaVersion,
                runId: report.runId,
                generatedAt: report.generatedAt,
                show: r.show,
                config: r.config,
                metric: "second_f1",
                thresholdTau: nil,
                value: r.secondLevel.f1
            ))
            out.append(NarlTrendRow(
                schemaVersion: report.schemaVersion,
                runId: report.runId,
                generatedAt: report.generatedAt,
                show: r.show,
                config: r.config,
                metric: "second_precision",
                thresholdTau: nil,
                value: r.secondLevel.precision
            ))
            out.append(NarlTrendRow(
                schemaVersion: report.schemaVersion,
                runId: report.runId,
                generatedAt: report.generatedAt,
                show: r.show,
                config: r.config,
                metric: "second_recall",
                thresholdTau: nil,
                value: r.secondLevel.recall
            ))
            // gtt9.6 coverage + FN-decomposition metrics.
            let coverageRows: [(String, Double)] = [
                ("scored_coverage_ratio", r.coverageMetrics.scoredCoverageRatio),
                ("transcript_coverage_ratio", r.coverageMetrics.transcriptCoverageRatio),
                ("candidate_recall", r.coverageMetrics.candidateRecall),
                ("auto_skip_precision", r.coverageMetrics.autoSkipPrecision),
                ("auto_skip_recall", r.coverageMetrics.autoSkipRecall),
                ("segment_iou", r.coverageMetrics.segmentIoU),
                ("unscored_fn_rate", r.coverageMetrics.unscoredFNRate),
                ("pipeline_coverage_failure_count",
                 Double(r.pipelineCoverageFailureAssetCount)),
            ]
            for (name, value) in coverageRows {
                out.append(NarlTrendRow(
                    schemaVersion: report.schemaVersion,
                    runId: report.runId,
                    generatedAt: report.generatedAt,
                    show: r.show,
                    config: r.config,
                    metric: name,
                    thresholdTau: nil,
                    value: value
                ))
            }
        }
        return out
    }

    /// Serialize rows as JSONL-ready lines (no trailing newline).
    static func jsonlLines(for rows: [NarlTrendRow]) throws -> [Data] {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        return try rows.map { try encoder.encode($0) }
    }
}
