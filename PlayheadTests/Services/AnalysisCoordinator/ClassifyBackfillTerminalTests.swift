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

    /// playhead-gqx4: the ad-scan term for tests that are exercising one of
    /// the OTHER priorities. Full measured coverage, so the classifier's new
    /// third term never masks the branch under test.
    private let fullyScanned = AnalysisCoordinator.AdScanCoverage(
        fraction: 1.0,
        limit: .stoppedShort
    )

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
            featureFailed: false,
            adScan: fullyScanned
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
            featureFailed: true,
            adScan: fullyScanned
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
            featureFailed: false,
            adScan: fullyScanned
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
            featureFailed: false,
            adScan: fullyScanned
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
            featureFailed: false,
            adScan: fullyScanned
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
            featureFailed: false,
            adScan: fullyScanned
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
            featureFailed: false,
            adScan: fullyScanned
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
            featureFailed: false,
            adScan: fullyScanned
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
            featureFailed: false,
            adScan: fullyScanned
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
                featureFailed: false,
                adScan: fullyScanned
            ),
            AnalysisCoordinator.classifyBackfillTerminal(
                chunks: [],
                episodeDuration: 3600,
                featureCoverage: 3600,
                budgetCancelled: true,
                transcriptFailed: false,
                featureFailed: false,
                adScan: fullyScanned
            ),
            AnalysisCoordinator.classifyBackfillTerminal(
                chunks: [chunk(startTime: 0, endTime: 600)],
                episodeDuration: 3600,
                featureCoverage: 3600,
                budgetCancelled: false,
                transcriptFailed: false,
                featureFailed: false,
                adScan: fullyScanned
            ),
        ]
        for verdict in cases {
            #expect(!verdict.reason.isEmpty)
        }
    }

    // MARK: - playhead-gqx4: the clean terminal requires measured ad-scan coverage

    /// The bead's headline row, verbatim. Asset B10C7BC8 on the product
    /// owner's device: 3,468 s of audio, transcript 3,466 s (0.999), feature
    /// 3,468 s (1.000), and a semantic ad scan that examined 90 s — 2.6%.
    /// The pre-fix classifier stamped `.completeFull` and wrote its own
    /// confession into `terminalReason`.
    @Test("3% ad scan with full transcript + feature is NOT the clean terminal")
    func realDeviceThreePercentScanIsNotCompleteFull() {
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 3466)],
            episodeDuration: 3468,
            featureCoverage: 3468,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false,
            adScan: .init(fraction: 90.0 / 3468.0, limit: .stoppedShort)
        )
        #expect(verdict.state != .completeFull)
        #expect(verdict.state == .completeAdScanPartial)
        // Still a terminal, and a DEGRADED one — that pairing is what makes
        // the surface draw ◐ instead of ✓ without any new UI code.
        #expect(verdict.state.isTerminalCompletion)
        #expect(verdict.state.isDegradedTerminalCompletion)
        // Machine-readable: the limiting cause is a closed-vocabulary token.
        #expect(verdict.reason.contains(AnalysisCoordinator.AdScanLimit.stoppedShort.rawValue))
        #expect(verdict.reason.contains("ad scan 0.026"))
        // And the numbers that used to be the WHOLE story are still there,
        // so the row remains diagnosable.
        #expect(verdict.reason.contains("transcript 0.999"))
        #expect(verdict.reason.contains("feature 1.000"))
    }

    /// The other two rows from the bead, plus the audited i7qe episode which
    /// the 2026-07-29 pull found had since been stamped `completeFull` at
    /// 38.8% scan — the two beads' shapes converging on one terminal.
    @Test(
        "every under-scanned device row degrades",
        arguments: [
            (3170.0, 3170.0, 3170.0, 0.275),   // 144C8A80
            (3578.0, 3576.0, 3578.0, 0.187),   // 8FECFDDE
            (2113.0, 2113.0, 2112.0, 0.388),   // 820134BF (the i7qe fixture)
            (3213.0, 3210.0, 3210.0, 0.0),     // 7A481794 — scan produced nothing
        ]
    )
    func underScannedDeviceRowsDegrade(
        duration: Double,
        transcriptEnd: Double,
        featureEnd: Double,
        adScanFraction: Double
    ) {
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: transcriptEnd)],
            episodeDuration: duration,
            featureCoverage: featureEnd,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false,
            adScan: .init(fraction: adScanFraction, limit: .stoppedShort)
        )
        #expect(verdict.state == .completeAdScanPartial)
    }

    /// Unmeasurable is not clean. `nil` arrives for real: no coverage-lane
    /// row at all, an unknown duration, or a duration the transcript's own
    /// reach contradicts (see `AnalysisCoverageSummary.adScanFraction`).
    @Test("unmeasured ad-scan coverage cannot reach the clean terminal")
    func unmeasuredAdScanIsNotCompleteFull() {
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 3600)],
            episodeDuration: 3600,
            featureCoverage: 3600,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false,
            adScan: .unmeasured
        )
        #expect(verdict.state == .completeAdScanPartial)
        #expect(verdict.reason.contains("unmeasured"))
    }

    /// A non-finite fraction must not sneak past the comparison.
    @Test("non-finite ad-scan fractions do not satisfy the clean terminal")
    func nonFiniteAdScanIsNotCompleteFull() {
        for poisoned in [Double.nan, .infinity, -.infinity] {
            let verdict = AnalysisCoordinator.classifyBackfillTerminal(
                chunks: [chunk(startTime: 0, endTime: 3600)],
                episodeDuration: 3600,
                featureCoverage: 3600,
                budgetCancelled: false,
                transcriptFailed: false,
                featureFailed: false,
                adScan: .init(fraction: poisoned, limit: .stoppedShort)
            )
            #expect(verdict.state == .completeAdScanPartial, "\(poisoned) must not clean-complete")
        }
    }

    /// The clean terminal is still REACHABLE — a fix that made `completeFull`
    /// unreachable would be a different bug wearing this one's clothes.
    @Test("a genuinely fully-scanned episode still reaches .completeFull")
    func fullyScannedEpisodeStillReachesCompleteFull() {
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 3600)],
            episodeDuration: 3600,
            featureCoverage: 3600,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false,
            adScan: .init(fraction: 1.0, limit: .stoppedShort)
        )
        #expect(verdict.state == .completeFull)
        #expect(!verdict.state.isDegradedTerminalCompletion)
        // The reason now NAMES the ad scan, so the row proves the third term
        // was evaluated rather than merely not contradicted.
        #expect(verdict.reason.contains("ad scan 1.000"))
    }

    /// Exactly at the floor. `finalizeBackfillMinCoverageRatio` is the same
    /// number the transcript and feature terms use — the classifier gains a
    /// third term, not a third threshold.
    @Test("ad-scan coverage exactly at the finalize floor is clean")
    func adScanAtFloorIsCompleteFull() {
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 3600)],
            episodeDuration: 3600,
            featureCoverage: 3600,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false,
            adScan: .init(
                fraction: AnalysisCoordinator.finalizeBackfillMinCoverageRatio,
                limit: .stoppedShort
            )
        )
        #expect(verdict.state == .completeFull)
    }

    /// A hair under the floor is not.
    @Test("ad-scan coverage just under the finalize floor degrades")
    func adScanJustUnderFloorDegrades() {
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 3600)],
            episodeDuration: 3600,
            featureCoverage: 3600,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false,
            adScan: .init(
                fraction: AnalysisCoordinator.finalizeBackfillMinCoverageRatio - 0.01,
                limit: .refusal
            )
        )
        #expect(verdict.state == .completeAdScanPartial)
        #expect(verdict.reason.contains(AnalysisCoordinator.AdScanLimit.refusal.rawValue))
    }

    /// The higher-priority branches must not be reachable via the new term:
    /// a short ad scan must never turn a budget cancellation, a pipeline
    /// failure, or a feature shortfall into a completion.
    @Test("a short ad scan never overrides a higher-priority terminal")
    func shortAdScanDoesNotOverrideHigherPriorities() {
        let starved = AnalysisCoordinator.AdScanCoverage(fraction: 0.01, limit: .guardrail)
        let cancelled = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 3600)],
            episodeDuration: 3600,
            featureCoverage: 3600,
            budgetCancelled: true,
            transcriptFailed: false,
            featureFailed: false,
            adScan: starved
        )
        #expect(cancelled.state == .cancelledBudget)

        let featureShort = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 3600)],
            episodeDuration: 3600,
            featureCoverage: 1800,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false,
            adScan: starved
        )
        #expect(featureShort.state == .failedFeature)

        let transcriptPartial = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 600)],
            episodeDuration: 3600,
            featureCoverage: 3600,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false,
            adScan: starved
        )
        #expect(transcriptPartial.state == .completeTranscriptPartial)

        let featureOnly = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [],
            episodeDuration: 3600,
            featureCoverage: 3600,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false,
            adScan: starved
        )
        #expect(featureOnly.state == .completeFeatureOnly)
    }

    // MARK: - playhead-gqx4: it TERMINATES — there is no retry loop

    /// The whole point of degrading rather than declining to terminate. An
    /// asset whose scan cannot advance must stop, or the "fix" is an infinite
    /// retry loop, which is a worse bug than the one being fixed.
    ///
    /// This bounds the attempts explicitly: drive the classifier the way a
    /// stuck asset would be driven, with the scan making no progress at all
    /// between attempts, and assert that EVERY attempt yields a terminal
    /// completion. Because the state is terminal, `runPipeline`'s resume arm
    /// takes the "already terminal" branch, so the pipeline runs the ad scan
    /// at most once per session — the attempt count cannot grow without a
    /// deliberate re-queue.
    @Test("a permanently unscannable asset terminates on every attempt")
    func unscannableAssetAlwaysTerminates() {
        let stuck = AnalysisCoordinator.AdScanCoverage(fraction: 0.026, limit: .refusal)
        var states: [SessionState] = []
        for _ in 0..<25 {
            let verdict = AnalysisCoordinator.classifyBackfillTerminal(
                chunks: [chunk(startTime: 0, endTime: 3466)],
                episodeDuration: 3468,
                featureCoverage: 3468,
                budgetCancelled: false,
                transcriptFailed: false,
                featureFailed: false,
                adScan: stuck
            )
            states.append(verdict.state)
        }
        #expect(states.count == 25)
        #expect(states.allSatisfy { $0.isTerminalCompletion })
        #expect(states.allSatisfy { $0 == .completeAdScanPartial })
        // Deterministic: the same inputs never oscillate between terminals,
        // so no caller can be goaded into re-driving by a changing verdict.
        #expect(Set(states).count == 1)
        // And the terminal offers exactly one successor — a deliberate
        // re-queue — so nothing can loop back into `.backfill` on its own.
        #expect(SessionState.completeAdScanPartial.validTransitions == [.queued])
        #expect(!SessionState.completeAdScanPartial.canTransition(to: .backfill))
    }

    // MARK: - playhead-gqx4: the limiting cause is derived, not guessed

    @Test("the limiting cause names the most explanatory failure present")
    func limitingCausePrecedence() {
        #expect(AnalysisCoordinator.adScanLimit(coverageLaneStatuses: []) == .neverRan)
        #expect(
            AnalysisCoordinator.adScanLimit(coverageLaneStatuses: [.success, .noAds])
                == .stoppedShort
        )
        // A single refusal outranks a pile of successes — it is the thing a
        // reader needs to know.
        #expect(
            AnalysisCoordinator.adScanLimit(
                coverageLaneStatuses: [.success, .success, .refusal]
            ) == .refusal
        )
        // …and outranks a guardrail, which outranks a decode failure, and so
        // on down the declared order.
        #expect(
            AnalysisCoordinator.adScanLimit(
                coverageLaneStatuses: [.guardrailViolation, .refusal]
            ) == .refusal
        )
        #expect(
            AnalysisCoordinator.adScanLimit(
                coverageLaneStatuses: [.decodingFailure, .guardrailViolation]
            ) == .guardrail
        )
        #expect(
            AnalysisCoordinator.adScanLimit(
                coverageLaneStatuses: [.cancelled, .permissiveDecodingFailure]
            ) == .decodeFailure
        )
        #expect(
            AnalysisCoordinator.adScanLimit(coverageLaneStatuses: [.rateLimited]) == .interrupted
        )
        #expect(
            AnalysisCoordinator.adScanLimit(coverageLaneStatuses: [.thermalDeferred])
                == .interrupted
        )
        #expect(
            AnalysisCoordinator.adScanLimit(coverageLaneStatuses: [.assetsUnavailable])
                == .unavailable
        )
        // An unrecognised persisted string is a transient failure, never a
        // silent success.
        #expect(AnalysisCoordinator.adScanLimit(coverageLaneStatuses: [nil]) == .transient)
    }

    /// Every status maps somewhere: a new `SemanticScanStatus` case must not
    /// be able to disappear into `.stoppedShort` by accident, which would
    /// re-create playhead-8ysk's "a label applied to an absence".
    @Test("every scan status is classified, and only verdicts read as stoppedShort")
    func everyStatusIsClassified() {
        for status in SemanticScanStatus.allCases {
            let limit = AnalysisCoordinator.adScanLimit(coverageLaneStatuses: [status])
            if status.didExamineWindow {
                #expect(limit == .stoppedShort, "\(status.rawValue) is a verdict")
            } else {
                #expect(
                    limit != .stoppedShort && limit != .neverRan,
                    "\(status.rawValue) must name a limiting cause"
                )
            }
        }
    }
}
