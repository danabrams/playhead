// AutoSkipPrecisionGateTests.swift
// playhead-gtt9.11: unit tests for the pure-value precision gate.
//
// These tests exercise `AutoSkipPrecisionGate` in isolation — no
// AdDetectionService, no store, no classifier. The gate is a pure
// function over (segment, config, surrounding context), so its tests
// are cheap and exhaustive on the decision matrix.

import Foundation
import Testing
@testable import Playhead

@Suite("AutoSkipPrecisionGate — three-way classification + safety signals")
struct AutoSkipPrecisionGateTests {

    // MARK: - Helpers

    private func makeInput(
        segmentStartTime: Double = 100,
        segmentEndTime: Double = 160,
        segmentScore: Double,
        episodeDuration: Double = 3600,
        overlappingFeatureWindows: [FeatureWindow] = [],
        lexicalCategories: Set<LexicalPatternCategory> = [],
        userCorrectionBoostFactor: Double = 1.0
    ) -> AutoSkipPrecisionGateInput {
        AutoSkipPrecisionGateInput(
            segmentStartTime: segmentStartTime,
            segmentEndTime: segmentEndTime,
            segmentScore: segmentScore,
            episodeDuration: episodeDuration,
            overlappingFeatureWindows: overlappingFeatureWindows,
            lexicalCategories: lexicalCategories,
            userCorrectionBoostFactor: userCorrectionBoostFactor
        )
    }

    private func featureWindow(
        startTime: Double,
        endTime: Double,
        musicBedLevel: MusicBedLevel
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "asset-gate-test",
            startTime: startTime,
            endTime: endTime,
            rms: 0.3,
            spectralFlux: 0.2,
            musicProbability: musicBedLevel == .none ? 0.0 : 0.8,
            musicBedLevel: musicBedLevel,
            pauseProbability: 0.1,
            speakerClusterId: 1,
            jingleHash: nil,
            featureVersion: 1
        )
    }

    // MARK: - 1. Detection-only (score < uiCandidateThreshold)

    @Test("segmentScore below uiCandidateThreshold classifies as detectionOnly")
    func detectionOnlyBelowUICandidateThreshold() {
        let input = makeInput(segmentScore: 0.30)
        let result = AutoSkipPrecisionGate.classify(input: input)
        #expect(result == .detectionOnly)
    }

    @Test("segmentScore at exactly uiCandidateThreshold does NOT classify as detectionOnly")
    func detectionOnlyBoundaryInclusive() {
        let input = makeInput(segmentScore: 0.40)
        let result = AutoSkipPrecisionGate.classify(input: input)
        if case .detectionOnly = result {
            Issue.record("0.40 should NOT be detectionOnly; got \(result)")
        }
    }

    // MARK: - 2. UI candidate: below autoSkipThreshold

    @Test("segmentScore in [uiCandidate, autoSkip) classifies as uiCandidate(.belowAutoSkipThreshold)")
    func uiCandidateBelowAutoSkipThreshold() {
        // 0.45 is between 0.40 and 0.55.
        let input = makeInput(segmentScore: 0.45)
        let result = AutoSkipPrecisionGate.classify(input: input)
        #expect(result == .uiCandidate(reason: .belowAutoSkipThreshold))
    }

    // MARK: - 3. UI candidate: duration implausible

    @Test("segmentScore above autoSkipThreshold but duration below typicalAdDuration → uiCandidate(.durationImplausible)")
    func uiCandidateWhenDurationTooShort() {
        // 10 s segment, score 0.70, and we give a strong lexical signal to
        // isolate duration as the rejection reason.
        let input = makeInput(
            segmentStartTime: 100,
            segmentEndTime: 110,
            segmentScore: 0.70,
            lexicalCategories: [.sponsor]
        )
        let result = AutoSkipPrecisionGate.classify(input: input)
        #expect(result == .uiCandidate(reason: .durationImplausible))
    }

    @Test("segmentScore above autoSkipThreshold but duration above typicalAdDuration → uiCandidate(.durationImplausible)")
    func uiCandidateWhenDurationTooLong() {
        let input = makeInput(
            segmentStartTime: 100,
            segmentEndTime: 100 + 180, // 180 s, above 90 s upper bound
            segmentScore: 0.70,
            lexicalCategories: [.sponsor]
        )
        let result = AutoSkipPrecisionGate.classify(input: input)
        #expect(result == .uiCandidate(reason: .durationImplausible))
    }

    // MARK: - 4. UI candidate: no safety signals

    @Test("segmentScore above autoSkipThreshold with plausible duration but ZERO safety signals → uiCandidate(.noSafetySignals)")
    func uiCandidateWhenNoSafetySignalsFire() {
        // Position chosen to avoid pre/post-roll slot prior: mid-episode.
        // Empty lexical. No music feature coverage. No user correction.
        let input = makeInput(
            segmentStartTime: 1500,
            segmentEndTime: 1560,
            segmentScore: 0.70,
            episodeDuration: 3000,
            overlappingFeatureWindows: [],
            lexicalCategories: [],
            userCorrectionBoostFactor: 1.0
        )
        let result = AutoSkipPrecisionGate.classify(input: input)
        #expect(result == .uiCandidate(reason: .noSafetySignals))
    }

    // MARK: - 5. Auto-skip eligible (multiple signal flavors)

    @Test("strong lexical category alone fires strongLexicalAdPhrase signal and admits auto-skip")
    func autoSkipAdmittedByLexicalSignal() {
        let input = makeInput(
            segmentStartTime: 1500,
            segmentEndTime: 1560,
            segmentScore: 0.70,
            episodeDuration: 3000,
            lexicalCategories: [.sponsor]
        )
        let result = AutoSkipPrecisionGate.classify(input: input)
        guard case .autoSkipEligible(let signals) = result else {
            Issue.record("expected autoSkipEligible; got \(result)")
            return
        }
        #expect(signals.contains(.strongLexicalAdPhrase))
    }

    @Test("metadata slot prior alone (pre-roll position) admits auto-skip")
    func autoSkipAdmittedBySlotPriorPreRoll() {
        // Segment centered at 30 s in a 3600 s episode → < 10% (360 s) from start.
        let input = makeInput(
            segmentStartTime: 0,
            segmentEndTime: 60,
            segmentScore: 0.70,
            episodeDuration: 3600,
            lexicalCategories: []
        )
        let result = AutoSkipPrecisionGate.classify(input: input)
        guard case .autoSkipEligible(let signals) = result else {
            Issue.record("expected autoSkipEligible; got \(result)")
            return
        }
        #expect(signals.contains(.metadataSlotPrior))
    }

    @Test("sustained music-bed feature coverage admits auto-skip via sustainedAcousticAdSignature")
    func autoSkipAdmittedByAcousticSignal() {
        // 60 s segment, 30 s of background music → 50% coverage >= 20% threshold.
        let features: [FeatureWindow] = stride(from: 100.0, to: 130.0, by: 2.0).map { t in
            featureWindow(startTime: t, endTime: t + 2, musicBedLevel: .background)
        }
        let input = makeInput(
            segmentStartTime: 100,
            segmentEndTime: 160,
            segmentScore: 0.70,
            episodeDuration: 3600,
            overlappingFeatureWindows: features,
            lexicalCategories: []
        )
        let result = AutoSkipPrecisionGate.classify(input: input)
        guard case .autoSkipEligible(let signals) = result else {
            Issue.record("expected autoSkipEligible; got \(result)")
            return
        }
        #expect(signals.contains(.sustainedAcousticAdSignature))
    }

    @Test("user-correction boost factor > 1.0 admits auto-skip via userConfirmedLocalPattern")
    func autoSkipAdmittedByUserCorrectionSignal() {
        let input = makeInput(
            segmentStartTime: 1500,
            segmentEndTime: 1560,
            segmentScore: 0.70,
            episodeDuration: 3000,
            lexicalCategories: [],
            userCorrectionBoostFactor: 1.25
        )
        let result = AutoSkipPrecisionGate.classify(input: input)
        guard case .autoSkipEligible(let signals) = result else {
            Issue.record("expected autoSkipEligible; got \(result)")
            return
        }
        #expect(signals.contains(.userConfirmedLocalPattern))
    }

    // MARK: - Signal unit tests

    @Test("strongLexicalAdPhrase fires on sponsor, promoCode, urlCTA, purchaseLanguage but not transitionMarker alone")
    func strongLexicalAdPhraseEnumeration() {
        let strong: [LexicalPatternCategory] = [.sponsor, .promoCode, .urlCTA, .purchaseLanguage]
        for c in strong {
            #expect(AutoSkipPrecisionGate.isStrongLexicalAdPhrase(categories: [c]),
                    "expected strong signal for single category \(c)")
        }
        #expect(AutoSkipPrecisionGate.isStrongLexicalAdPhrase(categories: [.transitionMarker]) == false,
                "transitionMarker alone must not be a strong signal")
        #expect(AutoSkipPrecisionGate.isStrongLexicalAdPhrase(categories: []) == false,
                "empty category set must not be a strong signal")
        // Mixed transition + strong → strong fires.
        #expect(AutoSkipPrecisionGate.isStrongLexicalAdPhrase(categories: [.transitionMarker, .sponsor]),
                "transition + strong should fire the strong signal")
    }

    @Test("metadata slot prior: pre-roll, mid-roll, post-roll classification")
    func metadataSlotPriorBuckets() {
        let d: Double = 3600
        // Pre-roll: center at 30 s → 30/3600 = 0.008 ≤ 0.10 → fires.
        #expect(AutoSkipPrecisionGate.isMetadataSlotPrior(
            segmentCenter: 30, episodeDuration: d, slotFraction: 0.10))
        // Exactly on boundary: center at 360 s (10%) → fires (inclusive).
        #expect(AutoSkipPrecisionGate.isMetadataSlotPrior(
            segmentCenter: 360, episodeDuration: d, slotFraction: 0.10))
        // Mid-roll: center at 1800 s → no.
        #expect(AutoSkipPrecisionGate.isMetadataSlotPrior(
            segmentCenter: 1800, episodeDuration: d, slotFraction: 0.10) == false)
        // Post-roll: center at 3570 s → fires.
        #expect(AutoSkipPrecisionGate.isMetadataSlotPrior(
            segmentCenter: 3570, episodeDuration: d, slotFraction: 0.10))
    }

    @Test("sustainedAcousticAdSignature respects coverage fraction and partial overlap clipping")
    func sustainedAcousticAdSignatureCoverage() {
        // Segment [100, 160). 20% floor = 12 s of music required.
        // 14 s of foreground music scattered → fires.
        let enough: [FeatureWindow] = stride(from: 100.0, to: 114.0, by: 2.0).map { t in
            featureWindow(startTime: t, endTime: t + 2, musicBedLevel: .foreground)
        }
        #expect(AutoSkipPrecisionGate.isSustainedAcousticAdSignature(
            featureWindows: enough,
            segmentStart: 100, segmentEnd: 160,
            minCoverage: 0.20))

        // Only 4 s (well below 12 s) → does not fire.
        let tooLittle: [FeatureWindow] = [
            featureWindow(startTime: 100, endTime: 102, musicBedLevel: .background),
            featureWindow(startTime: 102, endTime: 104, musicBedLevel: .background)
        ]
        #expect(AutoSkipPrecisionGate.isSustainedAcousticAdSignature(
            featureWindows: tooLittle,
            segmentStart: 100, segmentEnd: 160,
            minCoverage: 0.20) == false)

        // Partial-overlap: feature window straddles the segment start,
        // only the intersected 1 s counts, not the full 2 s.
        let straddle: [FeatureWindow] = [
            featureWindow(startTime: 99, endTime: 101, musicBedLevel: .foreground)
        ]
        // 1 s / 60 s = 1.67% — below 20%.
        #expect(AutoSkipPrecisionGate.isSustainedAcousticAdSignature(
            featureWindows: straddle,
            segmentStart: 100, segmentEnd: 160,
            minCoverage: 0.20) == false)
    }

    @Test("collectSafetySignals returns all firing signals simultaneously")
    func collectSafetySignalsComposite() {
        let features: [FeatureWindow] = stride(from: 0.0, to: 60.0, by: 2.0).map { t in
            featureWindow(startTime: t, endTime: t + 2, musicBedLevel: .background)
        }
        let input = makeInput(
            segmentStartTime: 0,
            segmentEndTime: 60,
            segmentScore: 0.80,
            episodeDuration: 3600,
            overlappingFeatureWindows: features,
            lexicalCategories: [.sponsor, .transitionMarker],
            userCorrectionBoostFactor: 1.5
        )
        let signals = AutoSkipPrecisionGate.collectSafetySignals(for: input)
        #expect(signals.contains(.strongLexicalAdPhrase))
        #expect(signals.contains(.sustainedAcousticAdSignature))
        #expect(signals.contains(.metadataSlotPrior))
        #expect(signals.contains(.userConfirmedLocalPattern))
        #expect(signals.contains(.catalogMatch) == false,
                "catalogMatch is reserved for gtt9.13; must not fire here")
    }
}
