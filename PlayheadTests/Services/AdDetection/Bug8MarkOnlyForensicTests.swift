// Bug8MarkOnlyForensicTests.swift
// Bug 8: forensic-locking tests for the markOnly precision-gate decision.
//
// Context
// -------
// A captured xcappdata bundle (analysis.sqlite, 2026-04-30) showed:
//   - autoSkip:  5 windows
//   - markOnly: 63 windows  (decisionState=candidate, all of them)
//   - NULL gate: 76 windows  (mix of userMarked banner-tap rows and
//                             pre-instrumentation legacy rows)
//
// The 63:5 ratio LOOKED like a mis-calibrated mark-only policy demoting
// auto-skip-eligible windows. Cross-checking the markOnly population
// against the gate's three rejection reasons (`belowAutoSkipThreshold`,
// `durationImplausible`, `noSafetySignals`) showed the policy was acting
// CORRECTLY for a fresh device with sparse evidence:
//
//   - 50 of 63 (79%) had segmentScore < autoSkipThreshold (0.55) → correct
//     `belowAutoSkipThreshold` demotion.
//   - 12 of 63 (19%) had segmentScore ≥ 0.55 but duration outside the
//     [30, 90] s ad-duration band → correct `durationImplausible`
//     demotion. (Sub-1s and >100s candidates are not real ads.)
//   -  1 of 63  (2%) had segmentScore ≥ 0.55 AND plausible duration but
//     ZERO safety signals fired → correct `noSafetySignals` demotion on
//     a fresh device whose catalog/correction store is empty.
//
// These tests do NOT change behaviour. They lock the per-class
// invariants the captured device confirmed so a future re-tuning cannot
// silently flip a correctly-cautious markOnly into an unjustified
// autoSkip without explicitly updating the policy contract here.
//
// Bug 8 verdict: NO POLICY FIX. The markOnly distribution is the
// precision gate working as designed; the device simply hasn't
// accumulated enough evidence to fire safety signals on borderline
// windows. Re-tuning thresholds on a single capture would weaken the
// precision contract this code exists to enforce.

import Foundation
import Testing
@testable import Playhead

@Suite("Bug 8 — markOnly precision-gate forensics (capture: 2026-04-30)")
struct Bug8MarkOnlyForensicTests {

    /// Default `catalogMatchSimilarity: 0` matches the production default
    /// in `AutoSkipPrecisionGateInput.init` and is load-bearing for the
    /// Class C ("no safety signals") tests: similarity below the 0.80
    /// floor disables `SafetySignal.catalogMatch`. Pass an explicit
    /// value > 0.80 to fire the catalog signal in tests that exercise it.
    private func makeInput(
        startTime: Double = 100,
        endTime: Double = 160,
        segmentScore: Double,
        episodeDuration: Double = 3600,
        overlappingFeatureWindows: [FeatureWindow] = [],
        lexicalCategories: Set<LexicalPatternCategory> = [],
        userCorrectionBoostFactor: Double = 1.0,
        catalogMatchSimilarity: Float = 0
    ) -> AutoSkipPrecisionGateInput {
        AutoSkipPrecisionGateInput(
            segmentStartTime: startTime,
            segmentEndTime: endTime,
            segmentScore: segmentScore,
            episodeDuration: episodeDuration,
            overlappingFeatureWindows: overlappingFeatureWindows,
            lexicalCategories: lexicalCategories,
            userCorrectionBoostFactor: userCorrectionBoostFactor,
            catalogMatchSimilarity: catalogMatchSimilarity
        )
    }

    // MARK: - Class A: belowAutoSkipThreshold (50 of 63 captured windows)

    /// The dominant markOnly bucket on the capture (50/63 ≈ 79%): score
    /// crossed the 0.40 ui-candidate floor but never reached the 0.55
    /// auto-skip threshold. Even when EVERY non-positional safety signal
    /// fires, the score gate alone must keep the window out of auto-skip
    /// — the safety signals are an AND with the threshold, never an
    /// override.
    ///
    /// Slot prior is deliberately NOT exercised here (mid-roll position):
    /// it is positional, not evidence-driven, so corroborating it with
    /// the four evidence-driven signals doesn't change the contract.
    @Test("class A: 0.40 ≤ score < 0.55 stays markOnly even when all four evidence-driven safety signals fire")
    func belowThresholdMarkOnlyEvenWithAllSafetySignals() {
        // Mid-roll @ 1500..1560 in a 3000-s episode → slot prior does NOT fire.
        let features: [FeatureWindow] = stride(from: 1500.0, to: 1560.0, by: 2.0).map { t in
            FeatureWindow(
                analysisAssetId: "asset-bug8",
                startTime: t,
                endTime: t + 2,
                rms: 0.3,
                spectralFlux: 0.2,
                musicProbability: 0.8,
                musicBedLevel: .background,
                pauseProbability: 0.1,
                speakerClusterId: 1,
                jingleHash: nil,
                featureVersion: 1
            )
        }
        // Score chosen at the upper edge of the (0.40, 0.55) band but
        // strictly below 0.55: the strongest possible adversarial input
        // for "below threshold" while remaining unambiguously inside the
        // band the capture populated.
        let scoreJustBelowAutoSkipThreshold = 0.5499
        let input = makeInput(
            startTime: 1500,
            endTime: 1560,
            segmentScore: scoreJustBelowAutoSkipThreshold,
            episodeDuration: 3000,
            overlappingFeatureWindows: features,
            lexicalCategories: [.sponsor, .promoCode, .urlCTA, .purchaseLanguage],
            userCorrectionBoostFactor: 5.0,
            catalogMatchSimilarity: 0.95   // > 0.80 floor → catalogMatch fires
        )

        // Sanity: confirm the four evidence-driven signals genuinely
        // fire on this input. If a future change to the gate's signal
        // detectors silently flips one off, this assertion fails before
        // the classification check, making the regression diagnosable.
        let signals = AutoSkipPrecisionGate.collectSafetySignals(for: input)
        #expect(signals.contains(.strongLexicalAdPhrase))
        #expect(signals.contains(.sustainedAcousticAdSignature))
        #expect(signals.contains(.userConfirmedLocalPattern))
        #expect(signals.contains(.catalogMatch))
        #expect(signals.contains(.metadataSlotPrior) == false,
                "mid-roll position must not fire slot prior")

        let result = AutoSkipPrecisionGate.classify(input: input)
        #expect(result == .uiCandidate(reason: .belowAutoSkipThreshold),
                "captured class A: sub-0.55 must always stay markOnly regardless of safety-signal density; got \(result)")
    }

    /// Sweep every confidence in (0.40, 0.55) and confirm every value
    /// gates to markOnly. This is the populated band the capture showed:
    /// most markOnly windows live here.
    @Test("class A sweep: every score in (0.40, 0.55) gates to markOnly with belowAutoSkipThreshold reason")
    func belowThresholdSweepInBand() {
        for score in stride(from: 0.41, through: 0.54, by: 0.01) {
            let input = makeInput(
                startTime: 100,
                endTime: 160,
                segmentScore: score,
                episodeDuration: 3600,
                lexicalCategories: [.sponsor]
            )
            let result = AutoSkipPrecisionGate.classify(input: input)
            #expect(result == .uiCandidate(reason: .belowAutoSkipThreshold),
                    "score=\(score) must demote to belowAutoSkipThreshold; got \(result)")
        }
    }

    // MARK: - Class B: durationImplausible (12 of 63 captured windows)

    /// The capture's high-confidence-but-too-short bucket: scores up to
    /// 0.90 but durations under 1 s. These would be terrifying to
    /// auto-skip — a 0.9 s "ad" is almost certainly classifier noise on
    /// a transient. Lock the lower-bound guard.
    @Test("class B: subsecond duration with high score (0.90) demotes to durationImplausible")
    func subsecondHighConfidenceDemotesToDurationImplausible() {
        let input = makeInput(
            startTime: 1000,
            endTime: 1000.9,           // 0.9 s, mirroring observed capture
            segmentScore: 0.90,
            episodeDuration: 3600,
            lexicalCategories: [.sponsor]
        )
        let result = AutoSkipPrecisionGate.classify(input: input)
        #expect(result == .uiCandidate(reason: .durationImplausible),
                "subsecond high-confidence span must demote: got \(result)")
    }

    /// The capture's other duration-implausible end: scores ~0.88 over
    /// 117 s, which exceeds the 90 s upper bound. Multi-ad-break
    /// coalescence — also unsafe to auto-skip without further refinement.
    @Test("class B: very-long duration (117 s) with high score demotes to durationImplausible")
    func veryLongHighConfidenceDemotesToDurationImplausible() {
        let input = makeInput(
            startTime: 1000,
            endTime: 1000 + 117,
            segmentScore: 0.88,
            episodeDuration: 3600,
            lexicalCategories: [.sponsor]
        )
        let result = AutoSkipPrecisionGate.classify(input: input)
        #expect(result == .uiCandidate(reason: .durationImplausible),
                "117-s span exceeds typicalAdDuration upper bound; got \(result)")
    }

    /// 17.35 s @ 0.90 confidence: the SINGLE highest-confidence markOnly
    /// row in the captured DB. Below the 30 s lower bound, so demotion
    /// is correct. Lock this as a regression guard — anyone who pushes
    /// the lower bound below 17 s without a calibration record (gtt9.3)
    /// will break this test, forcing a deliberate decision.
    @Test("class B forensic: 17.35 s @ 0.90 (capture's top markOnly row) demotes to durationImplausible")
    func capturedTopMarkOnlyRowIsDurationImplausible() {
        let input = makeInput(
            startTime: 2799.18,
            endTime: 2799.18 + 17.3512500000002,
            segmentScore: 0.900187966510443,
            episodeDuration: 3600,
            lexicalCategories: [.sponsor]
        )
        let result = AutoSkipPrecisionGate.classify(input: input)
        #expect(result == .uiCandidate(reason: .durationImplausible),
                "17.35-s span < 30-s lower bound; capture row F3CB8FBF demoted correctly: got \(result)")
    }

    // MARK: - Class C: noSafetySignals (1 of 63 captured windows)

    /// The capture's edge case: ~31 s @ 0.61 confidence in the plausible
    /// duration band, but no safety signal fired. On a fresh device with
    /// no catalog ingestion, no user corrections, no music-bed evidence,
    /// and a mid-roll position, this IS the correct outcome — saying
    /// "auto-skip" on a single classifier score with zero corroboration
    /// is exactly what the precision gate was built to refuse.
    @Test("class C: mid-roll plausible-duration plausible-score window with no signals demotes to noSafetySignals")
    func freshDeviceMidRollNoSignalsDemotesCorrectly() {
        // 31.6 s mirrors the captured 0.61-confidence row's duration.
        let input = makeInput(
            startTime: 1500,
            endTime: 1500 + 31.6305263157894,
            segmentScore: 0.612468857647611,
            episodeDuration: 3600,
            overlappingFeatureWindows: [],
            lexicalCategories: [],
            userCorrectionBoostFactor: 1.0,
            catalogMatchSimilarity: 0
        )

        // Belt-and-suspenders: the genuinely-empty signal set is what
        // makes this Class C, not just the gate's classification of it.
        // If a future change made one of these signals fire by accident
        // (e.g., a default that silently moves), this assertion fails
        // first and pinpoints the broken signal.
        let signals = AutoSkipPrecisionGate.collectSafetySignals(for: input)
        #expect(signals.isEmpty,
                "fresh-device mid-roll input must produce an empty signal set; got \(signals)")

        let result = AutoSkipPrecisionGate.classify(input: input)
        #expect(result == .uiCandidate(reason: .noSafetySignals),
                "fresh-device mid-roll with no signals must be noSafetySignals; got \(result)")
    }

    /// And the symmetric positive: the SAME window, given a single
    /// plausible safety signal (a strong lexical category) DOES gate to
    /// auto-skip. This pins the precision contract: the gate is not a
    /// blanket downvote; it correctly opens to auto-skip the moment ONE
    /// independent signal corroborates the classifier.
    @Test("class C symmetric: SAME score+duration with one strong safety signal gates to autoSkipEligible")
    func sameRowAdmittedWhenOneSignalFires() {
        let input = makeInput(
            startTime: 1500,
            endTime: 1500 + 31.6305263157894,
            segmentScore: 0.612468857647611,
            episodeDuration: 3600,
            lexicalCategories: [.sponsor]   // single corroborating signal
        )
        let result = AutoSkipPrecisionGate.classify(input: input)
        guard case .autoSkipEligible(let signals) = result else {
            Issue.record("same row + one safety signal must admit autoSkip; got \(result)")
            return
        }
        #expect(signals == [.strongLexicalAdPhrase],
                "exactly one signal expected (strongLexicalAdPhrase); got \(signals)")
    }

    // MARK: - Population invariant: the captured 63:5 ratio is explainable

    /// One representative input per rejection class, classified in
    /// sequence. The capture's 63 markOnly rows partition into these
    /// three reasons; this test does not exhaustively replay the
    /// capture, it asserts that each class still resolves to the same
    /// reason it did at investigation time. If a future change adds a
    /// fourth rejection reason or merges two, this test fails loudly
    /// and the policy genuinely needs review.
    @Test("each documented markOnly rejection class still classifies to its expected reason")
    func eachRejectionClassClassifiesToExpectedReason() {
        let belowThreshold = makeInput(
            startTime: 100, endTime: 160,
            segmentScore: 0.49, episodeDuration: 3600,
            lexicalCategories: [.sponsor]
        )
        #expect(AutoSkipPrecisionGate.classify(input: belowThreshold)
                == .uiCandidate(reason: .belowAutoSkipThreshold))

        let durationImplausible = makeInput(
            startTime: 100, endTime: 100.9,
            segmentScore: 0.90, episodeDuration: 3600,
            lexicalCategories: [.sponsor]
        )
        #expect(AutoSkipPrecisionGate.classify(input: durationImplausible)
                == .uiCandidate(reason: .durationImplausible))

        let noSignals = makeInput(
            startTime: 1500, endTime: 1560,
            segmentScore: 0.70, episodeDuration: 3600,
            overlappingFeatureWindows: [], lexicalCategories: [],
            userCorrectionBoostFactor: 1.0, catalogMatchSimilarity: 0
        )
        #expect(AutoSkipPrecisionGate.classify(input: noSignals)
                == .uiCandidate(reason: .noSafetySignals))
    }
}
