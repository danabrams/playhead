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

    /// Exactly at the floor.
    @Test("ad-scan coverage exactly at the sufficiency floor is clean")
    func adScanAtFloorIsCompleteFull() {
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 3600)],
            episodeDuration: 3600,
            featureCoverage: 3600,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false,
            adScan: .init(
                fraction: AnalysisCoordinator.AdScanCoverage.sufficientFraction,
                limit: .stoppedShort
            )
        )
        #expect(verdict.state == .completeFull)
    }

    /// A hair under the floor is not.
    @Test("ad-scan coverage just under the sufficiency floor degrades")
    func adScanJustUnderFloorDegrades() {
        let verdict = AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: 3600)],
            episodeDuration: 3600,
            featureCoverage: 3600,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false,
            adScan: .init(
                fraction: AnalysisCoordinator.AdScanCoverage.sufficientFraction - 0.01,
                limit: .refusal
            )
        )
        #expect(verdict.state == .completeAdScanPartial)
        #expect(verdict.reason.contains(AnalysisCoordinator.AdScanLimit.refusal.rawValue))
    }

    /// The three surfaces that consume measured ad-scan coverage must agree on
    /// ONE number. If they drift, an episode lands in a band where the pipeline
    /// calls itself done, the runner keeps trying to extend it, and the library
    /// still shows ◐ — with nothing able to close the gap.
    ///
    /// Deliberately NOT the transcript's floor: 0.95 is calibrated for a decoder
    /// chopping seconds off the end of an episode, which says nothing about how
    /// much audio a semantic scan read.
    @Test("the terminal, the checkmark and the scan-stop share one floor")
    func sufficiencyFloorIsSharedAcrossSurfaces() {
        #expect(
            AnalysisCoordinator.AdScanCoverage.sufficientFraction
                == episodePreparationCompleteThreshold
        )
        #expect(
            AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
                == AnalysisCoordinator.AdScanCoverage.sufficientFraction
        )
        #expect(
            AnalysisCoordinator.AdScanCoverage.sufficientFraction
                != AnalysisCoordinator.finalizeBackfillMinCoverageRatio,
            "the ad-scan floor must not silently inherit the transcript's floor"
        )
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

    /// playhead-gqx4 (adversarial review): the residual causes are chosen by
    /// whether the FRACTION was measurable, and a genuine scan failure always
    /// outranks the denominator complaint — a refusal is the more explanatory
    /// answer even when the duration is also broken.
    @Test("an unmeasurable denominator is named, and never outranks a real failure")
    func unmeasurableDurationIsNamed() {
        // Scan ran, nothing failed, but the fraction is unavailable: the
        // denominator is the limit, not the scan. Folding this into
        // `stoppedShort` would send a reader hunting for a scan problem that
        // does not exist — and it is the commonest nil arm in practice, because
        // any asset whose declared duration disagrees with its audio by >5%
        // lands here until the duration-backfill sweep repairs the row.
        #expect(
            AnalysisCoordinator.adScanLimit(
                coverageLaneStatuses: [.success, .noAds],
                fractionIsMeasurable: false
            ) == .unmeasurableDuration
        )
        // Same rows, measurable denominator: the pass simply stopped short.
        #expect(
            AnalysisCoordinator.adScanLimit(
                coverageLaneStatuses: [.success, .noAds],
                fractionIsMeasurable: true
            ) == .stoppedShort
        )
        // A real failure still wins over the denominator complaint.
        #expect(
            AnalysisCoordinator.adScanLimit(
                coverageLaneStatuses: [.success, .refusal],
                fractionIsMeasurable: false
            ) == .refusal
        )
        // No rows at all outranks both: the scan never ran.
        #expect(
            AnalysisCoordinator.adScanLimit(
                coverageLaneStatuses: [],
                fractionIsMeasurable: false
            ) == .neverRan
        )
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
                    limit != .stoppedShort && limit != .neverRan
                        && limit != .unmeasurableDuration,
                    "\(status.rawValue) must name a limiting cause"
                )
            }
        }
    }
}

// MARK: - playhead-csbq: ratios consumed as decision inputs must be bounded

/// playhead-csbq. `transcriptRatio` and `featureRatio` are both
/// `covered / episodeDuration` and neither had ever had an UPPER bound — the
/// classifier only tested them against a 0.95 floor. The device rows wrote the
/// consequence in their own words:
///
///   `full coverage: transcript 5.277, feature 5.277`
///   `full coverage: transcript 2.158, feature 5.110`
///
/// A ratio of 5.277 says the numerator measured 527.7% of the episode, which
/// means the numerator and the denominator describe different audio and NO
/// ratio between them is meaningful. The second row's two ratios DISAGREE
/// (2.158 vs 5.110), which rules out one shared scaling error and proves at
/// least one numerator is independently wrong.
///
/// gqx4's `adScanFraction` already withholds on the shape where the FAST
/// transcript overshoots. It cannot see either hole closed here: `coverageEnd`
/// is `MAX(endTime)` over ALL canonical chunks, so a FINAL-pass overshoot
/// clears gqx4's fast-only reach guard, and the FEATURE watermark is not an
/// input to `adScanFraction` at all.
@Suite("AnalysisCoordinator coverage-ratio bounds — playhead-csbq")
struct ClassifyBackfillTerminalRatioBoundsTests {

    private let fullyScanned = AnalysisCoordinator.AdScanCoverage(
        fraction: 1.0,
        limit: .stoppedShort
    )

    private func chunk(startTime: Double, endTime: Double) -> TranscriptChunk {
        let id = UUID().uuidString
        return TranscriptChunk(
            id: id,
            analysisAssetId: "asset-csbq",
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

    private func classify(
        coverageEnd: Double,
        episodeDuration: Double,
        featureCoverage: Double,
        adScan: AnalysisCoordinator.AdScanCoverage
    ) -> AnalysisCoordinator.BackfillTerminalVerdict {
        AnalysisCoordinator.classifyBackfillTerminal(
            chunks: [chunk(startTime: 0, endTime: coverageEnd)],
            episodeDuration: episodeDuration,
            featureCoverage: featureCoverage,
            budgetCancelled: false,
            transcriptFailed: false,
            featureFailed: false,
            adScan: adScan
        )
    }

    /// THE BEAD'S ORIGINAL OBSERVATION, reproduced end to end: a 608 s episode
    /// whose transcript reaches 3210 s — ratio 5.277 — with an ad scan that
    /// clears its floor. Before this guard the classifier stamped `.completeFull`
    /// and wrote its own confession into `terminalReason`.
    @Test("transcript ratio 5.277 can no longer be reported as full coverage")
    func transcriptRatio5277IsNotFullCoverage() {
        let verdict = classify(
            coverageEnd: 3210,
            episodeDuration: 608,
            featureCoverage: 3210,
            adScan: fullyScanned
        )
        #expect(verdict.state != .completeFull)
        #expect(verdict.state == .completeTranscriptPartial)
        // NAMED with a stable token, the way `AdScanLimit` is, so a diagnostics
        // capture can separate it without a device attached.
        #expect(
            verdict.reason.contains(
                AnalysisCoordinator.CoverageDenominatorFault.contradictedDuration.rawValue
            )
        )
        // And it says which term is wrong, with both numbers.
        #expect(verdict.reason.contains("transcript reaches 3210.0s of a declared 608.0s"))
    }

    /// The second device row: transcript 2.158 and feature 5.110, disagreeing.
    /// The FEATURE watermark alone is enough to disqualify the clean terminal —
    /// `adScanFraction` never looks at it, so nothing else would have caught it.
    @Test("a contradicted FEATURE watermark alone blocks the clean terminal")
    func featureRatioOvershootAloneBlocksCompleteFull() {
        // Transcript healthy (ratio 1.0), feature reaching 5.11x the duration.
        let verdict = classify(
            coverageEnd: 528,
            episodeDuration: 528,
            featureCoverage: 2698,
            adScan: fullyScanned
        )
        #expect(verdict.state != .completeFull)
        #expect(
            verdict.reason.contains(
                AnalysisCoordinator.CoverageDenominatorFault.contradictedDuration.rawValue
            )
        )
        #expect(verdict.reason.contains("feature reaches 2698.0s of a declared 528.0s"))
        #expect(!verdict.reason.contains("transcript reaches"))
    }

    /// THE FALSE-POSITIVE RAIL. Feed-vs-measured duration drift is normal and
    /// must not cost an episode its ✓. The tolerance is
    /// `AnalysisCoverageSummary.adScanDurationToleranceSec` — deliberately the
    /// SAME rule `adScanFraction` applies, so the terms cannot disagree about
    /// how much drift is normal.
    @Test("healthy and within-tolerance overshoots still reach completeFull")
    func healthyRatiosStillCompleteFull() {
        // Exactly complete.
        #expect(
            classify(
                coverageEnd: 3600, episodeDuration: 3600,
                featureCoverage: 3600, adScan: fullyScanned
            ).state == .completeFull
        )
        // A 1000 s episode tolerates min(30, 5%) == 30 s of overshoot.
        #expect(
            classify(
                coverageEnd: 1029, episodeDuration: 1000,
                featureCoverage: 1029, adScan: fullyScanned
            ).state == .completeFull
        )
        // A short episode's tolerance is the 5% arm: 200 s tolerates 10 s.
        #expect(
            classify(
                coverageEnd: 209, episodeDuration: 200,
                featureCoverage: 209, adScan: fullyScanned
            ).state == .completeFull
        )
        // One second past the 5% arm is refused.
        #expect(
            classify(
                coverageEnd: 211, episodeDuration: 200,
                featureCoverage: 200, adScan: fullyScanned
            ).state != .completeFull
        )
    }

    /// The guard is purely RESTRICTIVE: it can only ever remove `.completeFull`.
    /// When the ad scan is also unmeasurable, gqx4's richer naming must still
    /// win, because "the audio was never read for ads" is the more explanatory
    /// answer than "the denominator is wrong".
    @Test("gqx4's ad-scan naming still wins when both terms are unusable")
    func adScanNamingTakesPrecedence() {
        let verdict = classify(
            coverageEnd: 3210,
            episodeDuration: 608,
            featureCoverage: 3210,
            adScan: AnalysisCoordinator.AdScanCoverage(
                fraction: nil,
                limit: .unmeasurableDuration
            )
        )
        #expect(verdict.state == .completeAdScanPartial)
        #expect(verdict.reason.contains("unmeasurableDuration"))
    }

    /// The pure helper, at its boundary. A ratio at or below 1 (plus tolerance)
    /// is usable; anything above it is not, in either term.
    @Test("contradictedCoverageDenominator names each offending term")
    func contradictedDenominatorHelperBoundary() {
        // Usable.
        #expect(
            AnalysisCoordinator.contradictedCoverageDenominator(
                transcriptCoveredSec: 1000, featureCoveredSec: 1000, episodeDuration: 1000
            ) == nil
        )
        // Exactly at the tolerance edge (30 s for a 1000 s episode).
        #expect(
            AnalysisCoordinator.contradictedCoverageDenominator(
                transcriptCoveredSec: 1030, featureCoveredSec: 1030, episodeDuration: 1000
            ) == nil
        )
        // Both terms wrong ⇒ both named.
        let both = AnalysisCoordinator.contradictedCoverageDenominator(
            transcriptCoveredSec: 5277, featureCoveredSec: 5277, episodeDuration: 1000
        )
        #expect(both?.contains("transcript reaches") == true)
        #expect(both?.contains("feature reaches") == true)
        // A non-finite numerator is not usable either.
        #expect(
            AnalysisCoordinator.contradictedCoverageDenominator(
                transcriptCoveredSec: .nan, featureCoveredSec: 100, episodeDuration: 1000
            ) != nil
        )
        // A non-positive denominator is NOT this helper's business — the
        // classifier's own priority-6 guard already fails safe on it, and
        // reporting here would double-name the same fault.
        #expect(
            AnalysisCoordinator.contradictedCoverageDenominator(
                transcriptCoveredSec: 100, featureCoveredSec: 100, episodeDuration: 0
            ) == nil
        )
    }
}
