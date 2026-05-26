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
