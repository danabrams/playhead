// NarlApprovalPolicyTests.swift
// playhead-narl.3: Unit tests for the per-episode recommendation engine.
//
// Covers (bead acceptance):
//   - All three decision states (recommendFlip / holdOff / insufficientData).
//   - ε boundary behavior: precision delta exactly at −ε, just above, just below.
//   - Partial-coverage episodes → insufficientData.
//   - Missing fmSchedulingEnabled (= no shadow coverage) → insufficientData,
//     gracefully, not a throw.
//   - Excluded episodes → insufficientData.
//   - Configurable policy parameters actually change behavior.

import Foundation
import Testing
@testable import Playhead

@Suite("NarlApprovalPolicy – three decision states")
struct NarlApprovalDecisionStateTests {

    @Test("recommendFlip when both checks pass and shadow coverage present")
    func recommendFlipHappyPath() {
        let report = makeReport(episodes: [
            makeEntry(episodeId: "e1", config: "default",
                      precision: 0.80, recall: 0.70, hasShadow: true),
            makeEntry(episodeId: "e1", config: "allEnabled",
                      precision: 0.79, recall: 0.72, hasShadow: true),
        ])
        let recs = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: .default)
        #expect(recs.count == 1)
        #expect(recs[0].decision == .recommendFlip)
        #expect(recs[0].recallCheckPassed == true)
        #expect(recs[0].precisionCheckPassed == true)
        #expect(recs[0].reasoning.contains("flip ok"))
    }

    @Test("holdOff when recall regresses")
    func holdOffOnRecallRegression() {
        let report = makeReport(episodes: [
            makeEntry(episodeId: "e1", config: "default",
                      precision: 0.80, recall: 0.70, hasShadow: true),
            makeEntry(episodeId: "e1", config: "allEnabled",
                      precision: 0.80, recall: 0.60, hasShadow: true),
        ])
        let recs = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: .default)
        #expect(recs[0].decision == .holdOff)
        #expect(recs[0].recallCheckPassed == false)
        #expect(recs[0].precisionCheckPassed == true)
        #expect(recs[0].reasoning.contains("recall regressed"))
    }

    @Test("holdOff when precision regresses beyond ε")
    func holdOffOnPrecisionRegression() {
        let report = makeReport(episodes: [
            makeEntry(episodeId: "e1", config: "default",
                      precision: 0.80, recall: 0.70, hasShadow: true),
            makeEntry(episodeId: "e1", config: "allEnabled",
                      precision: 0.70, recall: 0.70, hasShadow: true),
        ])
        let recs = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: .default)
        #expect(recs[0].decision == .holdOff)
        #expect(recs[0].recallCheckPassed == true)
        #expect(recs[0].precisionCheckPassed == false)
        #expect(recs[0].reasoning.contains("precision regressed"))
    }

    @Test("insufficientData when shadow coverage missing and policy requires it")
    func insufficientDataWhenNoShadow() {
        let report = makeReport(episodes: [
            makeEntry(episodeId: "e1", config: "default",
                      precision: 0.80, recall: 0.70, hasShadow: false),
            makeEntry(episodeId: "e1", config: "allEnabled",
                      precision: 0.80, recall: 0.70, hasShadow: false),
        ])
        let recs = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: .default)
        #expect(recs[0].decision == .insufficientData)
        #expect(recs[0].reasoning.contains("pending narl.2"))
        // Graceful, not a throw — the test would have faulted before here if so.
    }
}

@Suite("NarlApprovalPolicy – ε boundary behavior")
struct NarlApprovalEpsilonBoundaryTests {

    /// default recall is matched, precision varies around the ε boundary.
    /// ε = 0.02, default precision = 0.80 → boundary is 0.78.
    @Test("Precision exactly at default − ε → recommendFlip (inclusive)")
    func exactlyAtEpsilonBoundary() {
        let report = makeReport(episodes: [
            makeEntry(episodeId: "e1", config: "default",
                      precision: 0.80, recall: 0.70, hasShadow: true),
            makeEntry(episodeId: "e1", config: "allEnabled",
                      precision: 0.78, recall: 0.70, hasShadow: true),
        ])
        let recs = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: .default)
        #expect(recs[0].decision == .recommendFlip,
                "ε is inclusive: precision ≥ default.precision − ε should pass")
    }

    @Test("Precision just above (default − ε) → recommendFlip")
    func justAboveEpsilon() {
        let report = makeReport(episodes: [
            makeEntry(episodeId: "e1", config: "default",
                      precision: 0.80, recall: 0.70, hasShadow: true),
            makeEntry(episodeId: "e1", config: "allEnabled",
                      precision: 0.785, recall: 0.70, hasShadow: true),
        ])
        let recs = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: .default)
        #expect(recs[0].decision == .recommendFlip)
    }

    @Test("Precision just below (default − ε) → holdOff")
    func justBelowEpsilon() {
        let report = makeReport(episodes: [
            makeEntry(episodeId: "e1", config: "default",
                      precision: 0.80, recall: 0.70, hasShadow: true),
            makeEntry(episodeId: "e1", config: "allEnabled",
                      precision: 0.77, recall: 0.70, hasShadow: true),
        ])
        let recs = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: .default)
        #expect(recs[0].decision == .holdOff)
        #expect(recs[0].precisionCheckPassed == false)
    }

    @Test("Configurable ε: larger tolerance flips a borderline case to recommendFlip")
    func largerEpsilonFlipsBorderline() {
        let entries: [NarlReportEpisodeEntry] = [
            makeEntry(episodeId: "e1", config: "default",
                      precision: 0.80, recall: 0.70, hasShadow: true),
            makeEntry(episodeId: "e1", config: "allEnabled",
                      precision: 0.75, recall: 0.70, hasShadow: true),
        ]
        let report = makeReport(episodes: entries)

        let strict = NarlApprovalPolicy(
            precisionEpsilon: 0.02, requireShadowCoverage: true, iouThreshold: 0.5
        )
        #expect(NarlApprovalPolicyEvaluator.evaluate(report: report, policy: strict)[0].decision == .holdOff)

        let tolerant = NarlApprovalPolicy(
            precisionEpsilon: 0.10, requireShadowCoverage: true, iouThreshold: 0.5
        )
        #expect(NarlApprovalPolicyEvaluator.evaluate(report: report, policy: tolerant)[0].decision == .recommendFlip)
    }
}

@Suite("NarlApprovalPolicy – partial coverage and missing data")
struct NarlApprovalPartialCoverageTests {

    @Test("Excluded episode (whole-asset veto) → insufficientData")
    func excludedEpisode() {
        let report = makeReport(episodes: [
            makeExcludedEntry(episodeId: "v1", config: "default",
                              reason: "wholeAssetVeto:v1"),
            makeExcludedEntry(episodeId: "v1", config: "allEnabled",
                              reason: "wholeAssetVeto:v1"),
        ])
        let recs = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: .default)
        #expect(recs[0].decision == .insufficientData)
        #expect(recs[0].reasoning.contains("excluded"))
        #expect(recs[0].reasoning.contains("wholeAssetVeto"))
    }

    @Test("Missing .allEnabled entry → insufficientData with clear reason")
    func missingAllEnabled() {
        let report = makeReport(episodes: [
            makeEntry(episodeId: "e1", config: "default",
                      precision: 0.80, recall: 0.70, hasShadow: true),
        ])
        let recs = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: .default)
        #expect(recs[0].decision == .insufficientData)
        #expect(recs[0].reasoning.contains(".allEnabled"))
    }

    @Test("Missing .default entry → insufficientData")
    func missingDefault() {
        let report = makeReport(episodes: [
            makeEntry(episodeId: "e1", config: "allEnabled",
                      precision: 0.80, recall: 0.70, hasShadow: true),
        ])
        let recs = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: .default)
        #expect(recs[0].decision == .insufficientData)
        #expect(recs[0].reasoning.contains(".default"))
    }

    @Test("One side has shadow, other doesn't → insufficientData (require both)")
    func partialShadowCoverage() {
        let report = makeReport(episodes: [
            makeEntry(episodeId: "e1", config: "default",
                      precision: 0.80, recall: 0.70, hasShadow: true),
            makeEntry(episodeId: "e1", config: "allEnabled",
                      precision: 0.80, recall: 0.70, hasShadow: false),
        ])
        let recs = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: .default)
        #expect(recs[0].decision == .insufficientData)
    }

    @Test("requireShadowCoverage=false allows flip without shadow (caller opt-in)")
    func shadowRequirementDisablable() {
        let report = makeReport(episodes: [
            makeEntry(episodeId: "e1", config: "default",
                      precision: 0.80, recall: 0.70, hasShadow: false),
            makeEntry(episodeId: "e1", config: "allEnabled",
                      precision: 0.80, recall: 0.70, hasShadow: false),
        ])
        let policy = NarlApprovalPolicy(
            precisionEpsilon: 0.02, requireShadowCoverage: false, iouThreshold: 0.5
        )
        let recs = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: policy)
        #expect(recs[0].decision == .recommendFlip)
    }

    @Test("Missing τ in harness metrics → insufficientData, not a throw")
    func missingThresholdGracefully() {
        let entries: [NarlReportEpisodeEntry] = [
            makeEntry(episodeId: "e1", config: "default",
                      precision: 0.80, recall: 0.70, hasShadow: true,
                      thresholds: [0.3]),
            makeEntry(episodeId: "e1", config: "allEnabled",
                      precision: 0.80, recall: 0.70, hasShadow: true,
                      thresholds: [0.3]),
        ]
        let report = makeReport(episodes: entries)
        let policy = NarlApprovalPolicy(
            precisionEpsilon: 0.02, requireShadowCoverage: true, iouThreshold: 0.5
        )
        let recs = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: policy)
        #expect(recs[0].decision == .insufficientData)
        #expect(recs[0].reasoning.contains("τ=0.5"))
    }
}

@Suite("NarlApprovalPolicy – multi-τ AND semantics")
struct NarlApprovalMultiThresholdTests {

    /// Construct an entry whose precision varies per τ: the episode passes
    /// the precision gate at τ=0.3/0.5 but regresses at τ=0.7 (boundary-
    /// sensitive degradation). Single-τ @ 0.5 would miss this; multi-τ
    /// AND must catch it.
    private static func makeEntryPerThreshold(
        episodeId: String,
        config: String,
        precisionByTau: [(tau: Double, precision: Double, recall: Double)]
    ) -> NarlReportEpisodeEntry {
        let metrics = precisionByTau.map { row in
            NarlWindowMetricsAtThreshold(
                threshold: row.tau,
                truePositives: 1, falsePositives: 0, falseNegatives: 0,
                precision: row.precision, recall: row.recall, f1: 0,
                meanMatchedIoU: 1.0
            )
        }
        return NarlReportEpisodeEntry(
            episodeId: episodeId,
            podcastId: "podcast-\(episodeId)",
            show: "TestShow",
            config: config,
            isExcluded: false,
            exclusionReason: nil,
            groundTruthWindowCount: 1,
            predictedWindowCount: 1,
            windowMetrics: metrics,
            secondLevel: NarlSecondLevelMetrics(
                truePositiveSeconds: 0, falsePositiveSeconds: 0, falseNegativeSeconds: 0,
                precision: 0, recall: 0, f1: 0
            ),
            lexicalInjectionAdds: 0,
            priorShiftAdds: 0,
            hasShadowCoverage: true
        )
    }

    @Test("holdOff when precision regresses only at τ=0.7 under multi-τ policy")
    func boundaryPrecisionRegressionCaughtAtHighTau() {
        // default: precision 0.80 across all τ. allEnabled: precision
        // 0.80 at τ=0.3/0.5, drops to 0.60 at τ=0.7 (boundaries got
        // sloppy). ε = 0.02 → 0.60 fails at τ=0.7.
        let entries: [NarlReportEpisodeEntry] = [
            Self.makeEntryPerThreshold(
                episodeId: "e-boundary", config: "default",
                precisionByTau: [(0.3, 0.80, 0.70), (0.5, 0.80, 0.70), (0.7, 0.80, 0.70)]
            ),
            Self.makeEntryPerThreshold(
                episodeId: "e-boundary", config: "allEnabled",
                precisionByTau: [(0.3, 0.80, 0.70), (0.5, 0.80, 0.70), (0.7, 0.60, 0.70)]
            ),
        ]
        let report = NarlEvalReport(
            schemaVersion: NarlEvalReportSchema.version,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            runId: "multi-tau-run",
            iouThresholds: [0.3, 0.5, 0.7],
            rollups: [],
            episodes: entries,
            notes: []
        )

        // Single-τ @ 0.5 misses the regression → would recommend flip.
        let singleTau = NarlApprovalPolicy(
            precisionEpsilon: 0.02,
            requireShadowCoverage: true,
            iouThreshold: 0.5
        )
        let singleRec = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: singleTau)[0]
        #expect(singleRec.decision == .recommendFlip,
                "single-τ @ 0.5 should not see the τ=0.7 boundary regression")

        // Multi-τ AND catches it and names τ=0.7 as the failure axis.
        let multiTau = NarlApprovalPolicy(
            precisionEpsilon: 0.02,
            requireShadowCoverage: true,
            iouThresholds: [0.3, 0.5, 0.7]
        )
        let multiRec = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: multiTau)[0]
        #expect(multiRec.decision == .holdOff)
        #expect(multiRec.reasoning.contains("τ=0.70"),
                "reasoning should name the failing threshold; got: \(multiRec.reasoning)")
        #expect(multiRec.reasoning.contains("precision regressed"))
        #expect(multiRec.precisionCheckPassed == false)
        // Recall held at every τ, so the aggregate recall flag remains true.
        #expect(multiRec.recallCheckPassed == true)
    }

    @Test("recommendFlip when every τ passes under multi-τ policy")
    func multiTauAllPass() {
        let entries: [NarlReportEpisodeEntry] = [
            Self.makeEntryPerThreshold(
                episodeId: "e-pass", config: "default",
                precisionByTau: [(0.3, 0.80, 0.70), (0.5, 0.80, 0.70), (0.7, 0.80, 0.70)]
            ),
            Self.makeEntryPerThreshold(
                episodeId: "e-pass", config: "allEnabled",
                precisionByTau: [(0.3, 0.82, 0.72), (0.5, 0.79, 0.72), (0.7, 0.78, 0.72)]
            ),
        ]
        let report = NarlEvalReport(
            schemaVersion: NarlEvalReportSchema.version,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            runId: "multi-tau-run",
            iouThresholds: [0.3, 0.5, 0.7],
            rollups: [],
            episodes: entries,
            notes: []
        )
        // .default policy uses multi-τ [0.3, 0.5, 0.7].
        let rec = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: .default)[0]
        #expect(rec.decision == .recommendFlip)
        #expect(rec.reasoning.contains("flip ok"))
        #expect(rec.recallCheckPassed == true)
        #expect(rec.precisionCheckPassed == true)
    }

    @Test("Mixed failure: recall fails at τ=0.3, precision fails at τ=0.7")
    func mixedFailureNamesBothAxes() {
        // default: precision 0.80, recall 0.70 across τ.
        // allEnabled: recall 0.60 at τ=0.3 (recall regression),
        //              precision 0.60 at τ=0.7 (precision regression),
        //              normal at τ=0.5.
        let entries: [NarlReportEpisodeEntry] = [
            Self.makeEntryPerThreshold(
                episodeId: "e-mixed", config: "default",
                precisionByTau: [(0.3, 0.80, 0.70), (0.5, 0.80, 0.70), (0.7, 0.80, 0.70)]
            ),
            Self.makeEntryPerThreshold(
                episodeId: "e-mixed", config: "allEnabled",
                precisionByTau: [(0.3, 0.80, 0.60), (0.5, 0.80, 0.70), (0.7, 0.60, 0.70)]
            ),
        ]
        let report = NarlEvalReport(
            schemaVersion: NarlEvalReportSchema.version,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            runId: "multi-tau-run",
            iouThresholds: [0.3, 0.5, 0.7],
            rollups: [],
            episodes: entries,
            notes: []
        )
        let rec = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: .default)[0]
        #expect(rec.decision == .holdOff)
        #expect(rec.reasoning.contains("τ=0.30"), "got: \(rec.reasoning)")
        #expect(rec.reasoning.contains("τ=0.70"), "got: \(rec.reasoning)")
        #expect(rec.reasoning.contains("recall regressed"))
        #expect(rec.reasoning.contains("precision regressed"))
        // Aggregate flags: at least one τ failed each axis → both false.
        #expect(rec.recallCheckPassed == false)
        #expect(rec.precisionCheckPassed == false)
    }

    @Test("Single-τ convenience initializer round-trips through primaryIouThreshold accessor")
    func singleTauInitRoundTrip() {
        let policy = NarlApprovalPolicy(
            precisionEpsilon: 0.02,
            requireShadowCoverage: true,
            iouThreshold: 0.6
        )
        #expect(policy.iouThresholds == [0.6])
        #expect(policy.primaryIouThreshold == 0.6)
    }

    @Test("Legacy single-τ Codable payload decodes into iouThresholds")
    func legacySingleTauDecodes() throws {
        let legacyJSON = #"""
        {
          "precisionEpsilon": 0.02,
          "requireShadowCoverage": true,
          "iouThreshold": 0.5
        }
        """#
        let data = try #require(legacyJSON.data(using: .utf8))
        let decoded = try JSONDecoder().decode(NarlApprovalPolicy.self, from: data)
        #expect(decoded.iouThresholds == [0.5])
        #expect(decoded.primaryIouThreshold == 0.5)
    }
}

@Suite("NarlApprovalPolicy – aggregation & ordering")
struct NarlApprovalAggregationTests {

    @Test("Multiple episodes yield one recommendation each, sorted by episodeId")
    func multiEpisodeOrdering() {
        let report = makeReport(episodes: [
            makeEntry(episodeId: "b", config: "default",
                      precision: 0.80, recall: 0.70, hasShadow: true),
            makeEntry(episodeId: "b", config: "allEnabled",
                      precision: 0.80, recall: 0.72, hasShadow: true),
            makeEntry(episodeId: "a", config: "default",
                      precision: 0.80, recall: 0.70, hasShadow: true),
            makeEntry(episodeId: "a", config: "allEnabled",
                      precision: 0.50, recall: 0.70, hasShadow: true),
        ])
        let recs = NarlApprovalPolicyEvaluator.evaluate(report: report, policy: .default)
        #expect(recs.count == 2)
        #expect(recs[0].episodeId == "a")
        #expect(recs[0].decision == .holdOff)
        #expect(recs[1].episodeId == "b")
        #expect(recs[1].decision == .recommendFlip)
    }
}

// MARK: - Test fixture helpers

private func makeReport(episodes: [NarlReportEpisodeEntry]) -> NarlEvalReport {
    NarlEvalReport(
        schemaVersion: NarlEvalReportSchema.version,
        generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
        runId: "test-run",
        iouThresholds: [0.3, 0.5, 0.7],
        rollups: [],
        episodes: episodes,
        notes: []
    )
}

private func makeEntry(
    episodeId: String,
    config: String,
    precision: Double,
    recall: Double,
    hasShadow: Bool,
    thresholds: [Double] = [0.3, 0.5, 0.7]
) -> NarlReportEpisodeEntry {
    let windowMetrics = thresholds.map { tau in
        NarlWindowMetricsAtThreshold(
            threshold: tau,
            truePositives: 1, falsePositives: 0, falseNegatives: 0,
            precision: precision, recall: recall, f1: 0,
            meanMatchedIoU: 1.0
        )
    }
    return NarlReportEpisodeEntry(
        episodeId: episodeId,
        podcastId: "podcast-\(episodeId)",
        show: "TestShow",
        config: config,
        isExcluded: false,
        exclusionReason: nil,
        groundTruthWindowCount: 1,
        predictedWindowCount: 1,
        windowMetrics: windowMetrics,
        secondLevel: NarlSecondLevelMetrics(
            truePositiveSeconds: 0, falsePositiveSeconds: 0, falseNegativeSeconds: 0,
            precision: 0, recall: 0, f1: 0
        ),
        lexicalInjectionAdds: 0,
        priorShiftAdds: 0,
        hasShadowCoverage: hasShadow
    )
}

private func makeExcludedEntry(
    episodeId: String,
    config: String,
    reason: String
) -> NarlReportEpisodeEntry {
    NarlReportEpisodeEntry(
        episodeId: episodeId,
        podcastId: "podcast-\(episodeId)",
        show: "TestShow",
        config: config,
        isExcluded: true,
        exclusionReason: reason,
        groundTruthWindowCount: 0,
        predictedWindowCount: 0,
        windowMetrics: [],
        secondLevel: NarlSecondLevelMetrics(
            truePositiveSeconds: 0, falsePositiveSeconds: 0, falseNegativeSeconds: 0,
            precision: 0, recall: 0, f1: 0
        ),
        lexicalInjectionAdds: 0,
        priorShiftAdds: 0,
        hasShadowCoverage: false
    )
}
