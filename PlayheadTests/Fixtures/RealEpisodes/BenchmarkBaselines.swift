// BenchmarkBaselines.swift
// Versioned snapshots of ad detection metrics on real podcast episodes.
// Each phase milestone should add a new baseline here after running the
// RealEpisodeBenchmarkTests suite. Compare current runs to the latest
// baseline to catch regressions or celebrate improvements.

import Foundation

struct DetectionBenchmark: Sendable, Equatable {
    /// Human-readable label for this measurement (phase + date).
    let label: String
    /// ISO date string when this was measured.
    let measuredOn: String
    /// Optional git SHA at measurement time.
    let commitHash: String?

    // Ground truth (constants for this episode)
    let totalAds: Int
    let totalAdSeconds: Double

    // AdWindow layer (final output of AdDetectionService)
    let adWindowCount: Int
    let adWindowRecall: Double        // 0..1
    let adSecondCoverage: Double      // 0..1

    // Evidence catalog layer (Phase 2 output)
    let evidenceCatalogEntries: Int
    let evidenceCatalogRecall: Double       // 0..1
    let evidenceCatalogPrecision: Double    // 0..1

    // LexicalScanner layer
    let lexicalCandidateCount: Int
    let lexicalCandidateRecall: Double      // 0..1

    // Weighted by skip confidence gradient
    let weightedRecall: Double

    // Per-ad span coverage (keyed by ad ID from the fixture)
    let perAdSpanCoverage: [String: Double]

    /// Compare this benchmark to another and return per-metric deltas.
    func compareTo(_ other: DetectionBenchmark) -> BenchmarkDelta {
        BenchmarkDelta(
            fromLabel: other.label,
            toLabel: self.label,
            adWindowRecallDelta: adWindowRecall - other.adWindowRecall,
            adSecondCoverageDelta: adSecondCoverage - other.adSecondCoverage,
            evidenceCatalogRecallDelta: evidenceCatalogRecall - other.evidenceCatalogRecall,
            evidenceCatalogPrecisionDelta: evidenceCatalogPrecision - other.evidenceCatalogPrecision,
            lexicalCandidateRecallDelta: lexicalCandidateRecall - other.lexicalCandidateRecall,
            weightedRecallDelta: weightedRecall - other.weightedRecall
        )
    }
}

struct BenchmarkDelta: Sendable {
    let fromLabel: String
    let toLabel: String
    let adWindowRecallDelta: Double
    let adSecondCoverageDelta: Double
    let evidenceCatalogRecallDelta: Double
    let evidenceCatalogPrecisionDelta: Double
    let lexicalCandidateRecallDelta: Double
    let weightedRecallDelta: Double

    /// Render a human-readable diff summary.
    var summary: String {
        func line(_ label: String, _ delta: Double) -> String {
            let pct = delta * 100
            let arrow = delta > 0.005 ? "↑" : (delta < -0.005 ? "↓" : "=")
            let sign = delta >= 0 ? "+" : ""
            return "  \(arrow) \(label.padding(toLength: 32, withPad: " ", startingAt: 0)) \(sign)\(String(format: "%.1f", pct))pp"
        }
        return """
        Delta: \(fromLabel) → \(toLabel)
        \(line("AdWindow recall", adWindowRecallDelta))
        \(line("Ad-second coverage", adSecondCoverageDelta))
        \(line("Evidence recall", evidenceCatalogRecallDelta))
        \(line("Evidence precision", evidenceCatalogPrecisionDelta))
        \(line("Lexical candidate recall", lexicalCandidateRecallDelta))
        \(line("Weighted recall", weightedRecallDelta))
        """
    }
}

enum DetectionBenchmarkHistory {

    /// Phase 2 baseline — measured 2026-04-06 on Conan "Fanhausen Revisited".
    /// At this point: Phase 2 (cue harvesters + evidence catalog) is complete,
    /// but no changes to pre-Phase 2 code (LexicalScanner/ClassifierService) yet,
    /// and Phase 3 (FM scanner) hasn't started.
    static let phase2Baseline = DetectionBenchmark(
        label: "Phase 2 baseline",
        measuredOn: "2026-04-06",
        commitHash: nil,
        totalAds: 4,
        totalAdSeconds: 77,
        adWindowCount: 0,
        adWindowRecall: 0.0,
        adSecondCoverage: 0.026,
        evidenceCatalogEntries: 7,
        evidenceCatalogRecall: 0.50,
        evidenceCatalogPrecision: 0.571,
        lexicalCandidateCount: 0,
        lexicalCandidateRecall: 0.0,
        weightedRecall: 0.48,
        perAdSpanCoverage: [
            "cvs-preroll": 0.02,
            "kelly-ripa-1": 0.0,
            "siriusxm-credits": 0.11,
            "kelly-ripa-2": 0.0,
        ]
    )

    /// All baselines in chronological order. Most recent last.
    static let all: [DetectionBenchmark] = [phase2Baseline]

    /// The most recent baseline to compare current runs against.
    static var latest: DetectionBenchmark { all.last! }
}
