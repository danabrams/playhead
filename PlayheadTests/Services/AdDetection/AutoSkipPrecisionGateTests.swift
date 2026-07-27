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
        analysisAssetId: String = "asset-gate-test",
        segmentStartTime: Double = 100,
        segmentEndTime: Double = 160,
        segmentScore: Double,
        episodeDuration: Double = 3600,
        overlappingFeatureWindows: [FeatureWindow] = [],
        lexicalCategories: Set<LexicalPatternCategory> = [],
        userCorrectionBoostFactor: Double = 1.0,
        catalogMatchSimilarity: Float = 0
    ) -> AutoSkipPrecisionGateInput {
        AutoSkipPrecisionGateInput(
            analysisAssetId: analysisAssetId,
            segmentStartTime: segmentStartTime,
            segmentEndTime: segmentEndTime,
            segmentScore: segmentScore,
            episodeDuration: episodeDuration,
            overlappingFeatureWindows: overlappingFeatureWindows,
            lexicalCategories: lexicalCategories,
            userCorrectionBoostFactor: userCorrectionBoostFactor,
            catalogMatchSimilarity: catalogMatchSimilarity
        )
    }

    private func featureWindow(
        assetId: String = "asset-gate-test",
        startTime: Double,
        endTime: Double,
        musicBedLevel: MusicBedLevel,
        rms: Double = 0.3,
        spectralFlux: Double = 0.2,
        musicProbability: Double? = nil,
        speakerChangeProxyScore: Double = 0,
        musicBedChangeScore: Double = 0,
        musicBedOnsetScore: Double = 0,
        musicBedOffsetScore: Double = 0,
        pauseProbability: Double = 0.1,
        featureVersion: Int =
            FeatureExtractionConfig.default.featureVersion
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: assetId,
            startTime: startTime,
            endTime: endTime,
            rms: rms,
            spectralFlux: spectralFlux,
            musicProbability: musicProbability
                ?? (musicBedLevel == .none ? 0.0 : 0.8),
            speakerChangeProxyScore: speakerChangeProxyScore,
            musicBedChangeScore: musicBedChangeScore,
            musicBedOnsetScore: musicBedOnsetScore,
            musicBedOffsetScore: musicBedOffsetScore,
            musicBedLevel: musicBedLevel,
            pauseProbability: pauseProbability,
            speakerClusterId: 1,
            jingleHash: nil,
            featureVersion: featureVersion
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

    @Test("catalog evidence is observable but cannot independently admit auto-skip")
    func catalogOnlyEvidenceRemainsDiagnostic() {
        let input = makeInput(
            segmentStartTime: 1500,
            segmentEndTime: 1560,
            segmentScore: 0.90,
            episodeDuration: 3000,
            catalogMatchSimilarity: AdCatalogStore.defaultSimilarityFloor
        )

        #expect(
            AutoSkipPrecisionGate.collectSafetySignals(for: input)
                == [.catalogMatch]
        )
        #expect(
            AutoSkipPrecisionGate.classify(input: input)
                == .uiCandidate(reason: .noSafetySignals)
        )
    }

    @Test("catalog evidence remains diagnostic beside an independent corroborator")
    func catalogEvidenceIsReportedAlongsideDecisionSignal() {
        let input = makeInput(
            segmentStartTime: 1500,
            segmentEndTime: 1560,
            segmentScore: 0.90,
            episodeDuration: 3000,
            lexicalCategories: [.sponsor],
            catalogMatchSimilarity: AdCatalogStore.defaultSimilarityFloor
        )

        #expect(
            AutoSkipPrecisionGate.classify(input: input)
                == .autoSkipEligible(
                    firedSignals: [.strongLexicalAdPhrase, .catalogMatch]
                )
        )
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
            analysisAssetId: "asset-gate-test",
            featureWindows: enough,
            segmentStart: 100, segmentEnd: 160,
            minCoverage: 0.20))

        // Only 4 s (well below 12 s) → does not fire.
        let tooLittle: [FeatureWindow] = [
            featureWindow(startTime: 100, endTime: 102, musicBedLevel: .background),
            featureWindow(startTime: 102, endTime: 104, musicBedLevel: .background)
        ]
        #expect(AutoSkipPrecisionGate.isSustainedAcousticAdSignature(
            analysisAssetId: "asset-gate-test",
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
            analysisAssetId: "asset-gate-test",
            featureWindows: straddle,
            segmentStart: 100, segmentEnd: 160,
            minCoverage: 0.20) == false)
    }

    @Test("overlapping or malformed feature windows cannot create acoustic authority")
    func acousticCoverageRejectsOverlappingOrMalformedIntervals() {
        // Seven heavily overlapping 2 s windows cover only [100, 102.6].
        // Summing each row independently would claim 14 s and clear the
        // 12 s floor even though the union covers just 2.6 s.
        let overlapping = (0..<7).map { index in
            let start = 100 + Double(index) * 0.1
            return featureWindow(
                startTime: start,
                endTime: start + 2,
                musicBedLevel: .background
            )
        }
        #expect(
            AutoSkipPrecisionGate.isSustainedAcousticAdSignature(
                analysisAssetId: "asset-gate-test",
                featureWindows: overlapping,
                segmentStart: 100,
                segmentEnd: 160,
                minCoverage: 0.20
            ) == false
        )

        // Even when the overlapping cohort's union exceeds the 12 s floor,
        // it is malformed persisted material and must fail closed. A mere
        // interval-union implementation would incorrectly fire here.
        let wideOverlapping = (0..<14).map { index in
            let start = 100 + Double(index)
            return featureWindow(
                startTime: start,
                endTime: start + 2,
                musicBedLevel: .background
            )
        }
        #expect(
            !AutoSkipPrecisionGate.isSustainedAcousticAdSignature(
                analysisAssetId: "asset-gate-test",
                featureWindows: wideOverlapping,
                segmentStart: 100,
                segmentEnd: 160,
                minCoverage: 0.20
            )
        )

        // A corrupt unbounded row must be ignored rather than clipped to the
        // whole segment and treated as independent acoustic authority.
        let malformed = [
            featureWindow(
                startTime: -.infinity,
                endTime: .infinity,
                musicBedLevel: .foreground
            ),
            featureWindow(
                startTime: -10,
                endTime: 2,
                musicBedLevel: .foreground
            ),
            featureWindow(
                startTime: 100,
                endTime: 160,
                musicBedLevel: .foreground
            )
        ]
        #expect(
            AutoSkipPrecisionGate.isSustainedAcousticAdSignature(
                analysisAssetId: "asset-gate-test",
                featureWindows: malformed,
                segmentStart: 100,
                segmentEnd: 160,
                minCoverage: 0.20
            ) == false
        )

        let incompatibleVersion = stride(
            from: 100.0,
            to: 114.0,
            by: 2.0
        ).map {
            featureWindow(
                startTime: $0,
                endTime: $0 + 2,
                musicBedLevel: .foreground,
                featureVersion:
                    FeatureExtractionConfig.default.featureVersion + 1
            )
        }
        #expect(
            !AutoSkipPrecisionGate.isSustainedAcousticAdSignature(
                analysisAssetId: "asset-gate-test",
                featureWindows: incompatibleVersion,
                segmentStart: 100,
                segmentEnd: 160,
                minCoverage: 0.20
            )
        )

        let mixedAssets = stride(
            from: 100.0,
            to: 114.0,
            by: 2.0
        ).enumerated().map { index, start in
            featureWindow(
                assetId: index.isMultiple(of: 2)
                    ? "asset-a" : "asset-b",
                startTime: start,
                endTime: start + 2,
                musicBedLevel: .foreground
            )
        }
        #expect(
            !AutoSkipPrecisionGate.isSustainedAcousticAdSignature(
                analysisAssetId: "asset-gate-test",
                featureWindows: mixedAssets,
                segmentStart: 100,
                segmentEnd: 160,
                minCoverage: 0.20
            )
        )
    }

    @Test("any malformed feature row invalidates acoustic automatic authority")
    func acousticCoverageRejectsMalformedNonMusicRows() {
        let validMusic = stride(
            from: 100.0,
            to: 114.0,
            by: 2.0
        ).map {
            featureWindow(
                startTime: $0,
                endTime: $0 + 2,
                musicBedLevel: .background
            )
        }
        let maximumDuration =
            MusicDetectionConfig.supportedWindowDurations.max() ?? 0
        let malformedRows: [(String, FeatureWindow)] = [
            (
                "noncanonical asset",
                featureWindow(
                    assetId: " asset-gate-test ",
                    startTime: 200,
                    endTime: 202,
                    musicBedLevel: .none
                )
            ),
            (
                "embedded-NUL asset",
                featureWindow(
                    assetId: "asset-gate-test\u{0}other",
                    startTime: 200,
                    endTime: 202,
                    musicBedLevel: .none
                )
            ),
            (
                "non-finite geometry",
                featureWindow(
                    startTime: .nan,
                    endTime: 202,
                    musicBedLevel: .none
                )
            ),
            (
                "unsupported duration",
                featureWindow(
                    startTime: 200,
                    endTime: 200 + maximumDuration + 1,
                    musicBedLevel: .none
                )
            ),
            (
                "undeclared in-range duration",
                featureWindow(
                    startTime: 200,
                    endTime: 203,
                    musicBedLevel: .none
                )
            ),
            (
                "non-finite energy",
                featureWindow(
                    startTime: 200,
                    endTime: 202,
                    musicBedLevel: .none,
                    rms: .nan
                )
            ),
            (
                "negative flux",
                featureWindow(
                    startTime: 200,
                    endTime: 202,
                    musicBedLevel: .none,
                    spectralFlux: -0.1
                )
            ),
            (
                "out-of-domain music probability",
                featureWindow(
                    startTime: 200,
                    endTime: 202,
                    musicBedLevel: .none,
                    musicProbability: 1.1
                )
            ),
            (
                "non-finite change score",
                featureWindow(
                    startTime: 200,
                    endTime: 202,
                    musicBedLevel: .none,
                    speakerChangeProxyScore: .infinity
                )
            ),
            (
                "negative onset score",
                featureWindow(
                    startTime: 200,
                    endTime: 202,
                    musicBedLevel: .none,
                    musicBedOnsetScore: -0.1
                )
            ),
            (
                "out-of-domain pause probability",
                featureWindow(
                    startTime: 200,
                    endTime: 202,
                    musicBedLevel: .none,
                    pauseProbability: 1.1
                )
            ),
        ]

        for (label, malformedRow) in malformedRows {
            #expect(
                !AutoSkipPrecisionGate.isSustainedAcousticAdSignature(
                    analysisAssetId: "asset-gate-test",
                    featureWindows: validMusic + [malformedRow],
                    segmentStart: 100,
                    segmentEnd: 160,
                    minCoverage: 0.20
                ),
                "\(label) must invalidate the entire acoustic evidence cohort"
            )
        }
    }

    @Test("homogeneous feature material from a different asset cannot authorize")
    func acousticCoverageRejectsForeignAssetMaterial() {
        let foreignMusic = stride(
            from: 100.0,
            to: 114.0,
            by: 2.0
        ).map {
            featureWindow(
                assetId: "asset-foreign",
                startTime: $0,
                endTime: $0 + 2,
                musicBedLevel: .background
            )
        }
        let input = makeInput(
            analysisAssetId: "asset-gate-test",
            segmentStartTime: 100,
            segmentEndTime: 160,
            segmentScore: 0.9,
            episodeDuration: 600,
            overlappingFeatureWindows: foreignMusic
        )

        #expect(
            AutoSkipPrecisionGate.classify(input: input)
                == .uiCandidate(reason: .noSafetySignals)
        )
        #expect(
            !AutoSkipPrecisionGate.collectSafetySignals(for: input)
                .contains(.sustainedAcousticAdSignature)
        )
    }

    // MARK: - Boundary / edge-case tests (I2, I4)

    /// I2: zero-duration input is malformed decision material, not merely an
    /// implausible-but-displayable proposal. It must fail closed before score
    /// or marker admission so no zero-width cue can enter persistence/replay.
    @Test("zero-duration window does not crash and fails closed")
    func zeroDurationWindowClassification() {
        let input = makeInput(
            segmentStartTime: 100,
            segmentEndTime: 100,
            segmentScore: 0.70,
            lexicalCategories: [.sponsor]
        )
        let result = AutoSkipPrecisionGate.classify(input: input)
        #expect(
            result == .detectionOnly,
            "zero-duration window must be rejected as malformed; got \(result)"
        )
    }

    /// I4: exact boundary — `segmentScore == autoSkipThreshold` (0.55) must
    /// be ≥, not >, so the window IS eligible for auto-skip when other
    /// gates pass. Regression guard against threshold comparison drift.
    @Test("segmentScore == autoSkipThreshold (0.55) is ≥-inclusive and eligible when other gates pass")
    func autoSkipThresholdBoundaryInclusive() {
        // Pre-roll slot to guarantee a safety signal fires; duration in [30, 90].
        let input = makeInput(
            segmentStartTime: 0,
            segmentEndTime: 60,
            segmentScore: 0.55,
            episodeDuration: 3600,
            lexicalCategories: []
        )
        let result = AutoSkipPrecisionGate.classify(input: input)
        guard case .autoSkipEligible = result else {
            Issue.record("exactly 0.55 must admit auto-skip (≥, not >); got \(result)")
            return
        }
    }

    @Test("non-finite decision numerics fail closed")
    func nonFiniteDecisionNumericsFailClosed() {
        for input in [
            makeInput(
                segmentScore: .nan,
                lexicalCategories: [.sponsor]
            ),
            makeInput(
                segmentStartTime: .infinity,
                segmentScore: 0.9,
                lexicalCategories: [.sponsor]
            ),
            makeInput(
                segmentScore: 0.9,
                episodeDuration: .nan,
                lexicalCategories: [.sponsor]
            ),
        ] {
            #expect(
                AutoSkipPrecisionGate.classify(input: input)
                    == .detectionOnly
            )
        }
    }

    @Test("out-of-domain score and episode geometry fail closed")
    func outOfDomainDecisionInputsFailClosed() {
        for input in [
            makeInput(
                segmentScore: 1.01,
                lexicalCategories: [.sponsor]
            ),
            makeInput(
                segmentStartTime: -30,
                segmentEndTime: 30,
                segmentScore: 0.9,
                lexicalCategories: [.sponsor]
            ),
            makeInput(
                segmentStartTime: 3_570,
                segmentEndTime: 3_630,
                segmentScore: 0.9,
                episodeDuration: 3_600,
                lexicalCategories: [.sponsor]
            ),
            makeInput(
                segmentStartTime: 160,
                segmentEndTime: 100,
                segmentScore: 0.9,
                lexicalCategories: [.sponsor]
            ),
            makeInput(
                segmentStartTime: 100,
                segmentEndTime: 100,
                segmentScore: 0.9,
                lexicalCategories: [.sponsor]
            ),
        ] {
            #expect(
                AutoSkipPrecisionGate.classify(input: input)
                    == .detectionOnly
            )
        }
    }

    @Test("zero-valued acoustic or duration floors cannot create automatic authority")
    func zeroValuedAuthorityFloorsFailClosed() {
        let input = makeInput(
            segmentStartTime: 1_500,
            segmentEndTime: 1_560,
            segmentScore: 0.9,
            episodeDuration: 3_000
        )
        let zeroAcousticFloor = AutoSkipPrecisionGateConfig(
            uiCandidateThreshold: 0.4,
            autoSkipThreshold: 0.55,
            typicalAdDuration: 30...90,
            minMusicBedCoverage: 0,
            slotFraction: 0.1
        )
        #expect(
            AutoSkipPrecisionGate.classify(
                input: input,
                config: zeroAcousticFloor
            ) == .detectionOnly
        )

        let zeroDurationFloor = AutoSkipPrecisionGateConfig(
            uiCandidateThreshold: 0.4,
            autoSkipThreshold: 0.55,
            typicalAdDuration: 0...90,
            minMusicBedCoverage: 0.2,
            slotFraction: 0.1
        )
        let zeroDurationInput = makeInput(
            segmentStartTime: 100,
            segmentEndTime: 100,
            segmentScore: 0.9,
            lexicalCategories: [.sponsor]
        )
        #expect(
            AutoSkipPrecisionGate.classify(
                input: zeroDurationInput,
                config: zeroDurationFloor
            ) == .detectionOnly
        )
    }

    @Test("non-finite optional signals do not fire")
    func nonFiniteOptionalSignalsDoNotFire() {
        let input = AutoSkipPrecisionGateInput(
            analysisAssetId: "asset-gate-test",
            segmentStartTime: 100,
            segmentEndTime: 160,
            segmentScore: 0.9,
            episodeDuration: 3_600,
            overlappingFeatureWindows: [],
            lexicalCategories: [],
            userCorrectionBoostFactor: .infinity,
            catalogMatchSimilarity: .infinity
        )
        let signals = AutoSkipPrecisionGate.collectSafetySignals(for: input)
        #expect(!signals.contains(.userConfirmedLocalPattern))
        #expect(!signals.contains(.catalogMatch))
    }

    @Test("out-of-domain user-correction boosts fail closed")
    func outOfDomainUserCorrectionBoostDoesNotAuthorize() {
        let input = makeInput(
            segmentStartTime: 1500,
            segmentEndTime: 1560,
            segmentScore: 0.90,
            episodeDuration: 3000,
            userCorrectionBoostFactor: 2.000_001
        )
        let signals = AutoSkipPrecisionGate.collectSafetySignals(for: input)
        #expect(!signals.contains(.userConfirmedLocalPattern))
        #expect(
            AutoSkipPrecisionGate.classify(input: input)
                == .uiCandidate(reason: .noSafetySignals)
        )
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
                "the default zero score must not claim a catalog match")
    }
}
