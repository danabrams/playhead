// NarlThresholdSweepTests.swift
// playhead-35tu: Pre-dogfood calibration of `AdDetectionConfig.segmentAutoSkipThreshold`.
//
// Sweeps the auto-skip threshold over a fixed set of values against the
// existing labeled NARL frozen-trace corpus. For each (trace, threshold),
// derives a span-level positive set from `windowScore.fusedSkipConfidence
// >= threshold` (the actual scalar score the production gate sees), merges
// adjacent windows, and computes IoU=0.5 precision/recall/F1 against the
// ground-truth ad spans built by `NarlGroundTruth`.
//
// What we are NOT doing:
//   - Re-running detection. Threshold gating is applied at the END of the
//     replay pipeline, so re-scoring frozen predictions is sufficient.
//   - Modifying production code or the existing harness. This file is
//     additive and runs as its own Swift Testing suite.
//   - Sweeping `segmentUICandidateThreshold`. Held fixed at 0.40 per spec.
//   - Replaying lexical-injection / prior-shift gates. The sweep operates
//     on raw post-fusion scores so the curve reflects the threshold knob
//     in isolation. (Equivalent to `MetadataActivationConfig.allEnabled`
//     when the activation effects are already baked into
//     `fusedSkipConfidence` — for v2 captures, the raw score is the
//     post-fusion value the precision gate consumes.)
//
// Output:
//   `.eval-out/narl/<ts>-sweep/sweep.csv`  — machine-readable curve
//   `.eval-out/narl/<ts>-sweep/sweep.md`   — human-readable per-show table
//
// V1 fixtures (those without a `windowScores` array) are skipped because
// they carry only a boolean per-span ad flag; we cannot honestly sweep a
// threshold against a pre-thresholded boolean.

import Foundation
import Testing
@testable import Playhead

@Suite("NarlThresholdSweep")
struct NarlThresholdSweepTests {

    /// The thresholds to sweep. Production default is 0.55; the sweep
    /// brackets it on both sides at 0.05 spacing.
    static let sweepThresholds: [Double] = [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]

    /// IoU threshold for window-level matching (one curve, IoU=0.5 — the
    /// midpoint of the existing harness's [0.3, 0.5, 0.7] sweep).
    static let iouThreshold: Double = 0.5

    /// Output subdir under `.eval-out/narl/`.
    static let evalOutputSubpath = ".eval-out/narl"

    // MARK: - Sweep entry

    /// One row of the sweep CSV.
    struct SweepRow: Sendable {
        let show: String
        let threshold: Double
        let episodeCount: Int
        let truePositives: Int
        let falsePositives: Int
        let falseNegatives: Int
        let precision: Double
        let recall: Double
        let f1: Double
    }

    // MARK: - Test entry

    @Test("Threshold sweep: produces precision/recall/F1 curve for AdDetectionConfig.segmentAutoSkipThreshold")
    func runThresholdSweep() throws {
        let traces = try NarlEvalHarnessTests.loadAllFixtureTraces()

        // Partition v2 (windowScores present) from v1 (only baseline span
        // booleans). v1 traces are skipped from the sweep — they cannot be
        // honestly re-thresholded — but we report the count in the markdown.
        let v2Traces = traces.filter { !$0.windowScores.isEmpty }
        let v1ExcludedCount = traces.count - v2Traces.count

        // For each (show, threshold) bucket, accumulate the predicted
        // windows + GT windows so we can re-pool TP/FP/FN at corpus level
        // (matches the existing harness's rollup methodology).
        var perBucket: [String: (predicted: [NarlTimeRange], gt: [NarlTimeRange], episodeCount: Int)] = [:]

        // Also track per-show episode count (across all thresholds it's
        // the same number) so we can render the table cleanly.
        var perShowEpisodes: [String: Int] = [:]

        for trace in v2Traces {
            let gtResult = NarlGroundTruth.build(for: trace)
            // Match harness behavior: skip whole-asset-vetoed episodes from
            // metric aggregation. (They contribute to v1ExcludedCount-style
            // accounting via the harness's separate excludedCounts table —
            // but for the sweep we just leave them out of the curve.)
            guard !gtResult.isExcluded else { continue }

            let show = NarlEvalHarnessTests.showName(for: trace)
            perShowEpisodes[show, default: 0] += 1

            for threshold in Self.sweepThresholds {
                let predicted = Self.predictAtThreshold(trace: trace, threshold: threshold)

                // Per-show bucket.
                let key = "\(show)|\(String(format: "%.2f", threshold))"
                perBucket[key, default: ([], [], 0)].predicted.append(contentsOf: predicted)
                perBucket[key, default: ([], [], 0)].gt.append(contentsOf: gtResult.adWindows)
                perBucket[key, default: ([], [], 0)].episodeCount += 1

                // Aggregate "ALL" bucket.
                let allKey = "ALL|\(String(format: "%.2f", threshold))"
                perBucket[allKey, default: ([], [], 0)].predicted.append(contentsOf: predicted)
                perBucket[allKey, default: ([], [], 0)].gt.append(contentsOf: gtResult.adWindows)
                perBucket[allKey, default: ([], [], 0)].episodeCount += 1
            }
        }

        // Render rows.
        var rows: [SweepRow] = []
        for (key, bundle) in perBucket {
            let parts = key.split(separator: "|", maxSplits: 1).map(String.init)
            let show = parts[0]
            let threshold = Double(parts[1]) ?? 0
            let metrics = NarlWindowMetrics.compute(
                predicted: NarlGroundTruth.mergeOverlaps(bundle.predicted),
                groundTruth: NarlGroundTruth.mergeOverlaps(bundle.gt),
                threshold: Self.iouThreshold
            )
            rows.append(SweepRow(
                show: show,
                threshold: threshold,
                episodeCount: bundle.episodeCount,
                truePositives: metrics.truePositives,
                falsePositives: metrics.falsePositives,
                falseNegatives: metrics.falseNegatives,
                precision: metrics.precision,
                recall: metrics.recall,
                f1: metrics.f1
            ))
        }

        // Sort: ALL first, then alphabetical by show, then ascending threshold.
        rows.sort { lhs, rhs in
            if lhs.show != rhs.show {
                if lhs.show == "ALL" { return true }
                if rhs.show == "ALL" { return false }
                return lhs.show < rhs.show
            }
            return lhs.threshold < rhs.threshold
        }

        // Write artifacts.
        let outputDir = try Self.writeArtifacts(
            rows: rows,
            v1ExcludedCount: v1ExcludedCount,
            v2TraceCount: v2Traces.count,
            totalTraceCount: traces.count
        )

        // Acceptance: artifacts written + curve has rows when corpus is non-empty.
        let csvURL = outputDir.appendingPathComponent("sweep.csv")
        let mdURL = outputDir.appendingPathComponent("sweep.md")
        #expect(FileManager.default.fileExists(atPath: csvURL.path),
                "sweep.csv should be written to \(csvURL.path)")
        #expect(FileManager.default.fileExists(atPath: mdURL.path),
                "sweep.md should be written to \(mdURL.path)")
        if !v2Traces.isEmpty {
            #expect(!rows.isEmpty,
                    "expected non-empty sweep rows when v2 corpus has fixtures")
        }
    }

    // MARK: - Predictor (threshold-only)

    /// Build a span-level prediction set for a single trace at a single
    /// threshold by:
    ///   1. Filtering windowScores where `fusedSkipConfidence >= threshold`.
    ///   2. Mapping each surviving window to a `NarlTimeRange`.
    ///   3. Merging overlapping/adjacent ranges (matches production's
    ///      aggregator behavior at the span level).
    ///
    /// We deliberately do NOT layer prior-shift / lexical-injection on top
    /// — the sweep isolates the threshold knob. The activation gates are
    /// already implicitly captured in `fusedSkipConfidence` for v2 traces
    /// (the score is the post-fusion value the precision gate consumes).
    static func predictAtThreshold(
        trace: FrozenTrace,
        threshold: Double
    ) -> [NarlTimeRange] {
        let positives: [NarlTimeRange] = trace.windowScores
            .filter { $0.fusedSkipConfidence >= threshold }
            .map { NarlTimeRange(start: $0.windowStart, end: $0.windowEnd) }
        return NarlGroundTruth.mergeOverlaps(positives)
    }

    // MARK: - Output

    @discardableResult
    static func writeArtifacts(
        rows: [SweepRow],
        v1ExcludedCount: Int,
        v2TraceCount: Int,
        totalTraceCount: Int
    ) throws -> URL {
        let runId = "\(NarlEvalHarnessTests.makeRunId())-sweep"
        let root = try NarlEvalHarnessTests.evalOutputRootURL()
        let outputDir = root.appendingPathComponent(runId)
        try FileManager.default.createDirectory(
            at: outputDir, withIntermediateDirectories: true
        )

        // CSV
        var csv = "show,threshold,episode_count,true_positives,false_positives,false_negatives,precision,recall,f1\n"
        for row in rows {
            csv += "\(row.show),"
            csv += String(format: "%.2f", row.threshold) + ","
            csv += "\(row.episodeCount),"
            csv += "\(row.truePositives),"
            csv += "\(row.falsePositives),"
            csv += "\(row.falseNegatives),"
            csv += String(format: "%.4f", row.precision) + ","
            csv += String(format: "%.4f", row.recall) + ","
            csv += String(format: "%.4f", row.f1) + "\n"
        }
        try csv.data(using: .utf8)!.write(to: outputDir.appendingPathComponent("sweep.csv"))

        // Markdown
        var md = "# NARL threshold sweep — `segmentAutoSkipThreshold`\n\n"
        md += "Generated: \(ISO8601DateFormatter().string(from: Date()))\n\n"
        md += "Run id: `\(runId)`\n\n"
        md += "## Corpus summary\n\n"
        md += "- Total fixtures loaded: **\(totalTraceCount)**\n"
        md += "- v2 (windowScores present, included in sweep): **\(v2TraceCount)**\n"
        md += "- v1 (no windowScores, excluded from sweep): **\(v1ExcludedCount)**\n"
        md += "- IoU threshold for window match: **\(String(format: "%.2f", iouThreshold))**\n"
        md += "- `segmentUICandidateThreshold` held fixed at: **0.40**\n"
        md += "- Production default `segmentAutoSkipThreshold`: **0.55**\n\n"
        md += "## Per-show curves\n\n"

        // Group rows by show, in the same sorted order they were appended.
        var byShow: [String: [SweepRow]] = [:]
        var showOrder: [String] = []
        for row in rows {
            if byShow[row.show] == nil { showOrder.append(row.show) }
            byShow[row.show, default: []].append(row)
        }

        for show in showOrder {
            let showRows = byShow[show] ?? []
            let episodeCount = showRows.first?.episodeCount ?? 0
            md += "### \(show) (\(episodeCount) episode-runs / threshold)\n\n"
            md += "| threshold | TP | FP | FN | precision | recall | F1 |\n"
            md += "|-----------|----|----|----|-----------|--------|-----|\n"
            for r in showRows {
                md += String(
                    format: "| %.2f | %d | %d | %d | %.3f | %.3f | %.3f |\n",
                    r.threshold,
                    r.truePositives,
                    r.falsePositives,
                    r.falseNegatives,
                    r.precision,
                    r.recall,
                    r.f1
                )
            }
            // F1 knee
            if let knee = showRows.max(by: { $0.f1 < $1.f1 }) {
                md += "\nF1 knee: threshold **\(String(format: "%.2f", knee.threshold))** "
                md += "→ P=\(String(format: "%.3f", knee.precision)) "
                md += "R=\(String(format: "%.3f", knee.recall)) "
                md += "F1=\(String(format: "%.3f", knee.f1))\n\n"
            } else {
                md += "\nNo data.\n\n"
            }
        }

        md += "## Caveats\n\n"
        md += "- Sweep operates on per-window `fusedSkipConfidence` "
        md += "thresholds, then merges adjacent windows into spans. The "
        md += "production pipeline aggregates raw classifier output via "
        md += "`SegmentAggregator` before the precision gate sees a "
        md += "score — we are not replaying that layer. Curves are an "
        md += "honest upper bound on the threshold's discriminative "
        md += "power as visible in frozen traces.\n"
        md += "- v1 fixtures (\(v1ExcludedCount) excluded) carry only a "
        md += "boolean per-span ad flag and cannot be re-thresholded.\n"
        md += "- IoU=0.5 matching may underweight recall on very short "
        md += "ad windows where a single-second drift drops the IoU "
        md += "under the threshold.\n"
        md += "- Per-show fixture counts are small (single-digit "
        md += "episodes typical); treat per-show knees as directional, "
        md += "not authoritative.\n"

        try md.data(using: .utf8)!.write(to: outputDir.appendingPathComponent("sweep.md"))

        return outputDir
    }
}
