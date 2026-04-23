// ClassifyBackfillTerminalTests.swift
// playhead-gtt9.8: pure-helper classifier that picks one of the six
// expanded terminal `SessionState`s based on (transcript coverage,
// feature coverage, thermal/budget cancellation, and per-pipeline
// failures). The helper also produces a `terminalReason: String` that
// `finalizeBackfill` persists into `analysis_assets.terminalReason`.
//
// Tests exercise the classifier in isolation — no coordinator graph,
// no store, no actor — so coverage of the decision matrix is cheap.
// Priority order (highest wins):
//   1. budgetCancelled      -> .cancelledBudget
//   2. featureFailed        -> .failedFeature
//   3. transcriptFailed     -> .failedTranscript
//   4. coverage analysis:
//        4a. feature ≥ 95% AND transcript ≥ 95%   -> .completeFull
//        4b. feature ≥ 95% AND transcript == 0    -> .completeFeatureOnly
//        4c. 0 < transcript < 95% AND feature ≥ 95% -> .completeTranscriptPartial
//        4d. feature < 95%                         -> .failedFeature (coverage-short)
//        4e. unknown duration (fail-safe)          -> .failedTranscript

import Foundation
import Testing

@testable import Playhead

@Suite("AnalysisCoordinator.classifyBackfillTerminal — gtt9.8")
struct ClassifyBackfillTerminalTests {

    // MARK: - Helpers

    private func chunk(
        startTime: Double,
        endTime: Double,
        id: String = UUID().uuidString
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: id,
            analysisAssetId: "asset-classify",
            segmentFingerprint: "fp-\(id)",
            chunkIndex: 0,
            startTime: startTime,
            endTime: endTime,
            text: "x",
            normalizedText: "x",
            pass: TranscriptPassType.fast.rawValue,
            modelVersion: "speech-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            weakAnchorMetadata: nil
        )
    }

    // MARK: - Priority 1: budget cancellation wins over everything

    @Test("budget cancellation wins even with full coverage")
    func budgetCancelWinsEvenIfFullCoverage() {
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 3600)],
            episodeDuration: 3600,
            featureCoverage: 3600,
            budgetCancelled: true,
            transcriptFailed: false,
            featureFailed: false
        )
        #expect(verdict.state == .cancelledBudget)
        #expect(verdict.reason.contains("budget") || verdict.reason.contains("cancel"))
    }

    // MARK: - Priority 2: feature failure wins over transcript failure

    @Test("feature failure wins over transcript failure")
    func featureFailureWinsOverTranscriptFailure() {
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [],
            episodeDuration: 3600,
            featureCoverage: 0,
            budgetCancelled: false,
            transcriptFailed: true,
            featureFailed: true
        )
        #expect(verdict.state == .failedFeature)
    }

    // MARK: - Priority 3: transcript failure

    @Test("transcript failure without feature failure resolves to .failedTranscript")
    func transcriptFailureResolves() {
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 100)],
            episodeDuration: 3600,
            featureCoverage: 3600,
            budgetCancelled: false,
            transcriptFailed: true,
            featureFailed: false
        )
        #expect(verdict.state == .failedTranscript)
    }

    // MARK: - Priority 4a: completeFull

    @Test("full feature + full transcript coverage => .completeFull")
    func fullCoverageIsCompleteFull() {
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 3600)],
            episodeDuration: 3600,
            featureCoverage: 3600,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false
        )
        #expect(verdict.state == .completeFull)
    }

    @Test("95% transcript + 95% feature coverage is still .completeFull (threshold)")
    func atThresholdIsCompleteFull() {
        // 3420/3600 == 0.95, matches the existing finalizeBackfillVerdict
        // threshold exactly.
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 3420)],
            episodeDuration: 3600,
            featureCoverage: 3420,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false
        )
        #expect(verdict.state == .completeFull)
    }

    // MARK: - Priority 4b: completeFeatureOnly

    @Test("feature ≥ 95% + zero transcript coverage => .completeFeatureOnly")
    func featureFullZeroTranscriptIsCompleteFeatureOnly() {
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [],
            episodeDuration: 3600,
            featureCoverage: 3600,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false
        )
        #expect(verdict.state == .completeFeatureOnly)
    }

    // MARK: - Priority 4c: completeTranscriptPartial

    @Test("partial transcript with full feature coverage => .completeTranscriptPartial")
    func partialTranscriptIsCompleteTranscriptPartial() {
        // 689/3600 ≈ 0.19 — the prod example from finalizeBackfillVerdict
        // tests. Not zero, not ≥ 95%.
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 689.82)],
            episodeDuration: 3600,
            featureCoverage: 3600,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false
        )
        #expect(verdict.state == .completeTranscriptPartial)
    }

    // MARK: - Priority 4d: feature short-coverage maps to .failedFeature

    @Test("feature coverage below threshold maps to .failedFeature")
    func shortFeatureCoverageIsFailedFeature() {
        // Transcript full but features only cover 30 minutes of a 60-
        // minute episode — this is a feature-side shortfall and the
        // classifier routes it to .failedFeature so the harness doesn't
        // double-report it as a transcript problem.
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 3600)],
            episodeDuration: 3600,
            featureCoverage: 1800,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false
        )
        #expect(verdict.state == .failedFeature)
    }

    // MARK: - Priority 4e: unknown duration fails safe to .failedTranscript

    @Test("unknown episode duration fails safe to .failedTranscript")
    func unknownDurationFailsSafe() {
        // playhead-gtt9.1.1 fail-safe semantics: when the denominator is
        // unknown (<= 0) we cannot prove the transcript is complete, so
        // the classifier routes to .failedTranscript. The caller is
        // expected to re-queue.
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 90)],
            episodeDuration: 0,
            featureCoverage: nil,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false
        )
        #expect(verdict.state == .failedTranscript)
    }

    // MARK: - terminalReason is always a non-empty descriptive string

    @Test("every verdict carries a non-empty human-readable reason")
    func everyVerdictHasReason() {
        let cases: [AnalysisCoordinator.BackfillTerminalVerdict] = [
            AnalysisCoordinator.classifyBackfillTerminal(
                chunks: [chunk(startTime: 0, endTime: 3600)],
                episodeDuration: 3600,
                featureCoverage: 3600,
                budgetCancelled: false,
                transcriptFailed: false,
                featureFailed: false
            ),
            AnalysisCoordinator.classifyBackfillTerminal(
                chunks: [],
                episodeDuration: 3600,
                featureCoverage: 3600,
                budgetCancelled: true,
                transcriptFailed: false,
                featureFailed: false
            ),
            AnalysisCoordinator.classifyBackfillTerminal(
                chunks: [chunk(startTime: 0, endTime: 600)],
                episodeDuration: 3600,
                featureCoverage: 3600,
                budgetCancelled: false,
                transcriptFailed: false,
                featureFailed: false
            ),
        ]
        for verdict in cases {
            #expect(!verdict.reason.isEmpty)
        }
    }
}
