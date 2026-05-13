import Foundation
import Testing
@testable import Playhead

@Suite("CoreMLSequenceClassifier")
struct CoreMLSequenceClassifierTests {

    @Test("Absent model falls back to RuleBasedClassifier behavior")
    func absentModelFallsBackToRuleBasedClassifier() {
        let input = makeInput()
        let priors = ShowPriors.empty
        let classifier = CoreMLSequenceClassifier(predictor: nil)
        let fallback = RuleBasedClassifier()

        let actual = classifier.classify(input: input, priors: priors)
        let expected = fallback.classify(input: input, priors: priors)

        assertSameClassification(actual, expected)
    }

    // Cycle 1 H1: production-shape regression rail. The default initializer
    // passes `CoreMLLayer2SequencePredictor.bundled()` for `predictor` —
    // i.e. the exact closure production builds run. When no
    // `Layer2SequenceClassifier.mlmodelc` is bundled (the current state of
    // the repo), `bundled()` returns nil and the classifier silently
    // delegates to RuleBased. The other fallback test stubs `predictor: nil`
    // directly, which does NOT exercise this bundle-lookup path; a typo or
    // resource-build-phase regression that broke bundle loading would
    // produce identical observable behavior but be invisible to that test.
    // This test pins the production path: default-init must match
    // RuleBased when no model artifact is shipped. If a future bundle
    // includes a real `.mlmodelc`, this test will fail — the appropriate
    // response is to remove this test and replace it with one that pins
    // the model-backed contract.
    @Test("Default-init in a no-model bundle matches RuleBasedClassifier exactly")
    func defaultInitMatchesRuleBasedFallback() {
        let input = makeInput()
        let priors = ShowPriors.empty
        let classifier = CoreMLSequenceClassifier()
        let fallback = RuleBasedClassifier()

        let actual = classifier.classify(input: input, priors: priors)
        let expected = fallback.classify(input: input, priors: priors)

        assertSameClassification(actual, expected)
    }

    @Test("Prediction failure falls back to RuleBasedClassifier behavior")
    func predictionFailureFallsBackToRuleBasedClassifier() {
        let input = makeInput()
        let priors = ShowPriors.empty
        let classifier = CoreMLSequenceClassifier(
            predictor: ThrowingLayer2Predictor()
        )
        let fallback = RuleBasedClassifier()

        let actual = classifier.classify(input: input, priors: priors)
        let expected = fallback.classify(input: input, priors: priors)

        assertSameClassification(actual, expected)
    }

    @Test("Injected model receives stable finite feature matrix and produces calibrated bounded result")
    func injectedModelReceivesFeatureMatrixAndClampsOutputs() throws {
        let predictor = CapturingLayer2Predictor(
            prediction: Layer2SequencePrediction(
                adProbability: 1.3,
                startAdjustment: -9.0,
                endAdjustment: 9.0
            )
        )
        let classifier = CoreMLSequenceClassifier(predictor: predictor)
        let result = classifier.classify(input: makeInput(includeNonFiniteWindow: true), priors: .empty)

        #expect(result.adProbability == 0.95)
        #expect(result.startAdjustment == -3.0)
        #expect(result.endAdjustment == 3.0)
        #expect(result.startTime == 9.0)
        #expect(result.endTime == 24.0)

        let requests = predictor.requests
        #expect(requests.count == 1)
        let matrix = try #require(requests.first?.featureMatrix)
        #expect(!matrix.isEmpty)
        #expect(matrix.allSatisfy { $0.count == 24 })
        #expect(matrix.flatMap { $0 }.allSatisfy { $0.isFinite })
    }

    @Test("PodcastProfile-derived slot and sponsor priors lift repeat-listen score on model path")
    func podcastProfilePriorsLiftModelProbability() {
        let predictor = CapturingLayer2Predictor(
            prediction: Layer2SequencePrediction(
                adProbability: 0.50,
                startAdjustment: 0.0,
                endAdjustment: 0.0
            )
        )
        let classifier = CoreMLSequenceClassifier(
            predictor: predictor,
            probabilityCalibrator: .identity
        )
        let input = makeInput(
            startTime: 58,
            endTime: 62,
            episodeDuration: 200,
            evidenceText: "This segment is sponsored by Acme."
        )
        let profile = PodcastProfile(
            podcastId: "repeat-listen-show",
            sponsorLexicon: "acme",
            normalizedAdSlotPriors: "[0.30]",
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0,
            observationCount: 20,
            mode: "test",
            recentFalseSkipSignals: 0
        )
        let repeatListenPriors = ShowPriors.from(profile: profile)

        let empty = classifier.classify(input: input, priors: .empty)
        let withPriors = classifier.classify(input: input, priors: repeatListenPriors)

        #expect(withPriors.adProbability > empty.adProbability + 0.07)
        #expect(withPriors.signalBreakdown.priorScore == 1.0)
    }

    @Test("Boundary adjustments cannot invert short spans")
    func boundaryAdjustmentsCannotInvertShortSpans() {
        let classifier = CoreMLSequenceClassifier(
            predictor: CapturingLayer2Predictor(
                prediction: Layer2SequencePrediction(
                    adProbability: 0.90,
                    startAdjustment: 3.0,
                    endAdjustment: -3.0
                )
            ),
            probabilityCalibrator: .identity
        )
        let input = makeInput(startTime: 10, endTime: 11)

        let result = classifier.classify(input: input, priors: .empty)

        #expect(result.startTime == 10)
        #expect(result.endTime == 11)
        #expect(result.startAdjustment == 0)
        #expect(result.endAdjustment == 0)
    }

    @Test("Boundary adjustments cannot produce out-of-episode spans")
    func boundaryAdjustmentsCannotLeaveEpisodeBounds() {
        let classifier = CoreMLSequenceClassifier(
            predictor: CapturingLayer2Predictor(
                prediction: Layer2SequencePrediction(
                    adProbability: 0.90,
                    startAdjustment: -3.0,
                    endAdjustment: 3.0
                )
            ),
            probabilityCalibrator: .identity
        )
        let input = makeInput(startTime: 1.0, endTime: 119.0, episodeDuration: 120.0)

        let result = classifier.classify(input: input, priors: .empty)

        #expect(result.startTime == 1.0)
        #expect(result.endTime == 119.0)
        #expect(result.startAdjustment == 0.0)
        #expect(result.endAdjustment == 0.0)
    }

    @Test("Model boundary output improves over lexical-only candidate bounds")
    func modelBoundaryOutputImprovesOverLexicalOnlyBounds() {
        let classifier = CoreMLSequenceClassifier(
            predictor: CapturingLayer2Predictor(
                prediction: Layer2SequencePrediction(
                    adProbability: 0.80,
                    startAdjustment: -2.0,
                    endAdjustment: 3.0
                )
            ),
            probabilityCalibrator: .identity
        )
        let input = makeInput(startTime: 10, endTime: 20)
        let result = classifier.classify(input: input, priors: .empty)

        let lexicalError = abs(10.0 - 8.0) + abs(20.0 - 23.0)
        let refinedError = abs(result.startTime - 8.0) + abs(result.endTime - 23.0)

        #expect(refinedError < lexicalError)
        #expect(result.startTime == 8.0)
        #expect(result.endTime == 23.0)
    }

    @Test("Batch classification preserves input order and caps model sequence length")
    func batchClassificationPreservesInputOrderAndCapsSequenceLength() throws {
        let predictor = CapturingLayer2Predictor(
            prediction: Layer2SequencePrediction(
                adProbability: 0.70,
                startAdjustment: 0.0,
                endAdjustment: 0.0
            )
        )
        let classifier = CoreMLSequenceClassifier(
            predictor: predictor,
            probabilityCalibrator: .identity
        )
        let inputs = [
            makeInput(id: "late", startTime: 90, endTime: 100, windowCount: 140),
            makeInput(id: "early", startTime: 10, endTime: 20, windowCount: 140),
            makeInput(id: "middle", startTime: 45, endTime: 55, windowCount: 140),
        ]

        let results = classifier.classify(inputs: inputs, priors: .empty)

        #expect(results.map(\.candidateId) == ["late", "early", "middle"])
        #expect(predictor.requests.map(\.candidateId) == ["late", "early", "middle"])
        #expect(predictor.requests.map { $0.featureMatrix.count } == [96, 96, 96])
    }

    @Test("Feature matrix encodes lexical categories, normalized position, and priors")
    func featureMatrixEncodesCandidateSignals() throws {
        let input = makeInput(
            startTime: 40,
            endTime: 50,
            episodeDuration: 200,
            categories: [.sponsor, .promoCode],
            evidenceText: "Use code PLAYHEAD at Acme."
        )
        let priors = ShowPriors(
            slotPositions: [0.225],
            knownSponsors: ["acme"],
            jingleFingerprints: [],
            trustWeight: 0.8
        )

        let matrix = CoreMLSequenceClassifier.makeFeatureMatrix(input: input, priors: priors)
        let first = try #require(matrix.first)

        #expect(first[10] == 0.72) // lexical confidence
        #expect(first[12] == 1.0)  // sponsor category
        #expect(first[13] == 1.0)  // promo-code category
        #expect(abs(first[17] - 0.225) < 0.000_001)
        #expect(first[19] > 0.0)   // slot prior
        #expect(first[20] == 0.8)  // prior trust
        #expect(first[21] == 1.0)  // known sponsor hit
    }

    @Test("Empty sponsor prior terms do not create sponsor hits")
    func emptySponsorPriorTermsDoNotCreateSponsorHits() throws {
        let input = makeInput(evidenceText: "No actual sponsor evidence.")
        let matrix = CoreMLSequenceClassifier.makeFeatureMatrix(
            input: input,
            priors: ShowPriors(
                slotPositions: [],
                knownSponsors: ["", "   "],
                jingleFingerprints: [],
                trustWeight: 1.0
            )
        )
        let first = try #require(matrix.first)

        #expect(first[21] == 0.0)
    }

    @Test("Sponsor prior terms require term boundaries")
    func sponsorPriorTermsRequireTermBoundaries() throws {
        let partialWord = makeInput(evidenceText: "This is helpful host commentary.")
        let exactPhrase = makeInput(evidenceText: "This break is sponsored by Help.")
        let priors = ShowPriors(
            slotPositions: [],
            knownSponsors: ["Help"],
            jingleFingerprints: [],
            trustWeight: 1.0
        )

        let partialMatrix = CoreMLSequenceClassifier.makeFeatureMatrix(input: partialWord, priors: priors)
        let exactMatrix = CoreMLSequenceClassifier.makeFeatureMatrix(input: exactPhrase, priors: priors)

        #expect(try #require(partialMatrix.first)[21] == 0.0)
        #expect(try #require(exactMatrix.first)[21] == 1.0)
    }

    @Test("CoreML predictor pads or truncates matrices to fixed model sequence shape")
    func predictorNormalizesMatricesToFixedModelShape() throws {
        let matrix = [
            Array(repeating: 0.1, count: 24),
            Array(repeating: 0.2, count: 24),
            Array(repeating: 0.3, count: 24),
        ]

        let padded = try CoreMLLayer2SequencePredictor.normalizeMatrix(
            matrix,
            expectedShape: [NSNumber(value: 4), NSNumber(value: 24)]
        )
        #expect(padded.shape == [NSNumber(value: 4), NSNumber(value: 24)])
        #expect(padded.matrix.count == 4)
        #expect(padded.matrix[0][0] == 0.1)
        #expect(padded.matrix[2][0] == 0.3)
        #expect(padded.matrix[3].allSatisfy { $0 == 0.0 })

        let truncated = try CoreMLLayer2SequencePredictor.normalizeMatrix(
            matrix,
            expectedShape: [NSNumber(value: 1), NSNumber(value: 2), NSNumber(value: 24)]
        )
        #expect(truncated.shape == [NSNumber(value: 1), NSNumber(value: 2), NSNumber(value: 24)])
        #expect(truncated.matrix.count == 2)
        #expect(truncated.matrix.map { $0[0] } == [0.1, 0.2])
    }

    private func makeInput(
        id: String = "candidate-coreml",
        startTime: Double = 12,
        endTime: Double = 21,
        episodeDuration: Double = 120,
        confidence: Double = 0.72,
        hitCount: Int = 3,
        categories: Set<LexicalPatternCategory> = [.sponsor, .urlCTA],
        evidenceText: String = "Sponsored by Acme at acme dot com.",
        windowCount: Int = 6,
        includeNonFiniteWindow: Bool = false
    ) -> ClassifierInput {
        ClassifierInput(
            candidate: LexicalCandidate(
                id: id,
                analysisAssetId: "asset-\(id)",
                startTime: startTime,
                endTime: endTime,
                confidence: confidence,
                hitCount: hitCount,
                categories: categories,
                evidenceText: evidenceText,
                evidenceStartTime: startTime,
                detectorVersion: "test"
            ),
            featureWindows: makeWindows(
                assetId: "asset-\(id)",
                startTime: startTime - 2,
                count: windowCount,
                includeNonFiniteWindow: includeNonFiniteWindow
            ),
            episodeDuration: episodeDuration
        )
    }

    private func makeWindows(
        assetId: String,
        startTime: Double,
        count: Int,
        includeNonFiniteWindow: Bool
    ) -> [FeatureWindow] {
        (0..<count).map { index in
            let start = startTime + Double(index) * 2.0
            return FeatureWindow(
                analysisAssetId: assetId,
                startTime: start,
                endTime: start + 2.0,
                rms: includeNonFiniteWindow && index == 0 ? .nan : 0.25 + Double(index % 3) * 0.05,
                spectralFlux: includeNonFiniteWindow && index == 1 ? .infinity : 0.20 + Double(index % 2) * 0.08,
                musicProbability: 0.30,
                speakerChangeProxyScore: 0.40,
                musicBedChangeScore: 0.35,
                musicBedOnsetScore: 0.25,
                musicBedOffsetScore: 0.10,
                musicBedLevel: index.isMultiple(of: 2) ? .background : .none,
                pauseProbability: 0.15,
                speakerClusterId: index % 3,
                jingleHash: nil,
                featureVersion: 1
            )
        }
    }

    private func assertSameClassification(
        _ actual: ClassifierResult,
        _ expected: ClassifierResult,
        sourceLocation: SourceLocation = #_sourceLocation
    ) {
        #expect(actual.candidateId == expected.candidateId, sourceLocation: sourceLocation)
        #expect(actual.analysisAssetId == expected.analysisAssetId, sourceLocation: sourceLocation)
        #expect(actual.startTime == expected.startTime, sourceLocation: sourceLocation)
        #expect(actual.endTime == expected.endTime, sourceLocation: sourceLocation)
        #expect(actual.adProbability == expected.adProbability, sourceLocation: sourceLocation)
        #expect(actual.startAdjustment == expected.startAdjustment, sourceLocation: sourceLocation)
        #expect(actual.endAdjustment == expected.endAdjustment, sourceLocation: sourceLocation)
        #expect(actual.signalBreakdown.lexicalScore == expected.signalBreakdown.lexicalScore, sourceLocation: sourceLocation)
        #expect(actual.signalBreakdown.rmsDropScore == expected.signalBreakdown.rmsDropScore, sourceLocation: sourceLocation)
        #expect(actual.signalBreakdown.spectralChangeScore == expected.signalBreakdown.spectralChangeScore, sourceLocation: sourceLocation)
        #expect(actual.signalBreakdown.musicScore == expected.signalBreakdown.musicScore, sourceLocation: sourceLocation)
        #expect(actual.signalBreakdown.speakerChangeScore == expected.signalBreakdown.speakerChangeScore, sourceLocation: sourceLocation)
        #expect(actual.signalBreakdown.priorScore == expected.signalBreakdown.priorScore, sourceLocation: sourceLocation)
    }
}

private final class CapturingLayer2Predictor: @unchecked Sendable, Layer2SequencePredicting {
    private let prediction: Layer2SequencePrediction
    private(set) var requests: [Layer2SequenceModelInput] = []

    init(prediction: Layer2SequencePrediction) {
        self.prediction = prediction
    }

    func predict(input: Layer2SequenceModelInput) throws -> Layer2SequencePrediction {
        requests.append(input)
        return prediction
    }
}

private final class ThrowingLayer2Predictor: @unchecked Sendable, Layer2SequencePredicting {
    enum Failure: Error {
        case predictionFailed
    }

    func predict(input: Layer2SequenceModelInput) throws -> Layer2SequencePrediction {
        throw Failure.predictionFailed
    }
}
