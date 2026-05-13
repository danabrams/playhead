import CoreML
import Foundation
import OSLog

struct Layer2SequencePrediction: Sendable, Equatable {
    let adProbability: Double
    let startAdjustment: Double
    let endAdjustment: Double
}

struct Layer2SequenceModelInput: Sendable, Equatable {
    let candidateId: String
    let featureMatrix: [[Double]]
}

protocol Layer2SequencePredicting: Sendable {
    func predict(input: Layer2SequenceModelInput) throws -> Layer2SequencePrediction
}

enum Layer2SequenceModelError: Error {
    case missingOutput(String)
    case invalidFeatureShape
    case unsupportedFeatureShape([NSNumber])
}

/// CoreML-backed Layer 2 classifier for refining lexical candidates with
/// acoustic feature sequences. When no model is bundled, or prediction fails,
/// this preserves the existing `RuleBasedClassifier` behavior exactly.
struct CoreMLSequenceClassifier: ClassifierService {
    static let bundledModelName = "Layer2SequenceClassifier"

    private enum Constant {
        static let maxSequenceLength = 96
        static let featureCount = 24
        static let maxBoundaryAdjust = BoundaryRefiner.maxBoundaryAdjust
        static let minSpanDuration = 0.10
        static let maxHitCountForNormalization = 8.0
        static let maxDurationForNormalization = 180.0
        static let priorProbabilityLift = 0.10
        static let sponsorProbabilityLift = 0.06
    }

    private let predictor: (any Layer2SequencePredicting)?
    private let fallback: RuleBasedClassifier
    private let probabilityCalibrator: MonotonicCalibrator
    private let logger = Logger(subsystem: "com.playhead", category: "CoreMLSequenceClassifier")

    init(
        predictor: (any Layer2SequencePredicting)? = CoreMLLayer2SequencePredictor.bundled(),
        fallback: RuleBasedClassifier = RuleBasedClassifier(),
        probabilityCalibrator: MonotonicCalibrator = ScoreCalibrationProfile.v1.calibrator(for: .classifier)
    ) {
        self.predictor = predictor
        self.fallback = fallback
        self.probabilityCalibrator = probabilityCalibrator
        // Cycle 1 H1: leave a one-line forensic trail when no model is bundled
        // so dogfood / support can grep a single log to confirm the runtime
        // path. Without this, a misnamed `.mlmodelc` resource or accidental
        // bundle exclusion is invisible until a score-distribution shift is
        // noticed downstream.
        if predictor == nil {
            logger.info("Layer 2 sequence model not bundled — using RuleBasedClassifier fallback for all classifications.")
        }
    }

    func classify(inputs: [ClassifierInput], priors: ShowPriors) -> [ClassifierResult] {
        inputs.map { classify(input: $0, priors: priors) }
    }

    func classify(input: ClassifierInput, priors: ShowPriors) -> ClassifierResult {
        guard let predictor else {
            return fallback.classify(input: input, priors: priors)
        }

        let modelInput = Layer2SequenceModelInput(
            candidateId: input.candidate.id,
            featureMatrix: Self.makeFeatureMatrix(input: input, priors: priors)
        )

        do {
            let prediction = try predictor.predict(input: modelInput)
            return makeResult(
                input: input,
                priors: priors,
                prediction: prediction
            )
        } catch {
            logger.error("Layer 2 CoreML prediction failed for candidate=\(input.candidate.id, privacy: .public): \(String(describing: error), privacy: .public)")
            return fallback.classify(input: input, priors: priors)
        }
    }

    static func makeFeatureMatrix(
        input: ClassifierInput,
        priors: ShowPriors
    ) -> [[Double]] {
        let candidate = input.candidate
        let sortedWindows = input.featureWindows.sorted { $0.startTime < $1.startTime }
        let windows = downsample(sortedWindows, maxCount: Constant.maxSequenceLength)
        let priorScore = RegionScoring.computePriorScore(
            startTime: candidate.startTime,
            endTime: candidate.endTime,
            episodeDuration: input.episodeDuration,
            priors: priors
        )
        let sponsorHit = knownSponsorHit(candidate: candidate, priors: priors)
        let normalizedPosition = normalizedPosition(
            startTime: candidate.startTime,
            endTime: candidate.endTime,
            episodeDuration: input.episodeDuration
        )
        let normalizedDuration = clamp01((candidate.endTime - candidate.startTime) / Constant.maxDurationForNormalization)
        let lexicalConfidence = clamp01(candidate.confidence)
        let hitCount = clamp01(Double(candidate.hitCount) / Constant.maxHitCountForNormalization)
        let categoryFlags = LexicalPatternCategory.allCases.map { category in
            candidate.categories.contains(category) ? 1.0 : 0.0
        }

        if windows.isEmpty {
            return [
                featureVector(
                    window: nil,
                    candidate: candidate,
                    sequenceIndex: 0,
                    sequenceCount: 1,
                    lexicalConfidence: lexicalConfidence,
                    hitCount: hitCount,
                    categoryFlags: categoryFlags,
                    normalizedPosition: normalizedPosition,
                    normalizedDuration: normalizedDuration,
                    priorScore: priorScore,
                    priorTrust: priors.trustWeight,
                    sponsorHit: sponsorHit
                )
            ]
        }

        return windows.enumerated().map { index, window in
            featureVector(
                window: window,
                candidate: candidate,
                sequenceIndex: index,
                sequenceCount: windows.count,
                lexicalConfidence: lexicalConfidence,
                hitCount: hitCount,
                categoryFlags: categoryFlags,
                normalizedPosition: normalizedPosition,
                normalizedDuration: normalizedDuration,
                priorScore: priorScore,
                priorTrust: priors.trustWeight,
                sponsorHit: sponsorHit
            )
        }
    }

    private func makeResult(
        input: ClassifierInput,
        priors: ShowPriors,
        prediction: Layer2SequencePrediction
    ) -> ClassifierResult {
        let candidate = input.candidate
        let windows = input.featureWindows
        let lexicalScore = Self.clamp01(candidate.confidence)
        let rmsDropScore = RegionScoring.computeRmsDropScore(windows: windows)
        let spectralChangeScore = RegionScoring.computeSpectralChangeScore(windows: windows)
        let musicScore = RegionScoring.computeMusicScore(windows: windows)
        let speakerChangeScore = RegionScoring.computeSpeakerChangeScore(windows: windows)
        let slotPriorScore = RegionScoring.computePriorScore(
            startTime: candidate.startTime,
            endTime: candidate.endTime,
            episodeDuration: input.episodeDuration,
            priors: priors
        )
        let sponsorPriorScore = Self.knownSponsorHit(candidate: candidate, priors: priors) * Self.clamp01(priors.trustWeight)
        let priorScore = max(slotPriorScore, sponsorPriorScore)

        let calibrated = probabilityCalibrator.calibrate(prediction.adProbability)
        let lifted = calibrated
            + (1.0 - calibrated) * Constant.priorProbabilityLift * slotPriorScore
            + (1.0 - calibrated) * Constant.sponsorProbabilityLift * sponsorPriorScore
        let adProbability = Self.clamp01(lifted)

        let adjustments = Self.safeBoundaryAdjustments(
            candidateStart: candidate.startTime,
            candidateEnd: candidate.endTime,
            episodeDuration: input.episodeDuration,
            startAdjustment: prediction.startAdjustment,
            endAdjustment: prediction.endAdjustment
        )

        return ClassifierResult(
            candidateId: candidate.id,
            analysisAssetId: candidate.analysisAssetId,
            startTime: candidate.startTime + adjustments.start,
            endTime: candidate.endTime + adjustments.end,
            adProbability: adProbability,
            startAdjustment: adjustments.start,
            endAdjustment: adjustments.end,
            signalBreakdown: SignalBreakdown(
                lexicalScore: lexicalScore,
                rmsDropScore: rmsDropScore,
                spectralChangeScore: spectralChangeScore,
                musicScore: musicScore,
                speakerChangeScore: speakerChangeScore,
                priorScore: priorScore
            )
        )
    }

    private static func featureVector(
        window: FeatureWindow?,
        candidate: LexicalCandidate,
        sequenceIndex: Int,
        sequenceCount: Int,
        lexicalConfidence: Double,
        hitCount: Double,
        categoryFlags: [Double],
        normalizedPosition: Double,
        normalizedDuration: Double,
        priorScore: Double,
        priorTrust: Double,
        sponsorHit: Double
    ) -> [Double] {
        let sequencePosition = sequenceCount <= 1
            ? 0.0
            : Double(sequenceIndex) / Double(sequenceCount - 1)
        let relativeWindowStart: Double
        if let window {
            let duration = max(candidate.endTime - candidate.startTime, 0.001)
            relativeWindowStart = (window.startTime - candidate.startTime) / duration
        } else {
            relativeWindowStart = 0.0
        }

        let rawFeatures: [Double] = [
            window?.rms ?? 0.0,
            window?.spectralFlux ?? 0.0,
            window?.musicProbability ?? 0.0,
            window?.pauseProbability ?? 0.0,
            speakerClusterFeature(window?.speakerClusterId),
            window?.speakerChangeProxyScore ?? 0.0,
            window?.musicBedChangeScore ?? 0.0,
            window?.musicBedOnsetScore ?? 0.0,
            window?.musicBedOffsetScore ?? 0.0,
            musicBedLevelFeature(window?.musicBedLevel ?? .none),
            lexicalConfidence,
            hitCount,
            categoryFlags[safe: 0] ?? 0.0,
            categoryFlags[safe: 1] ?? 0.0,
            categoryFlags[safe: 2] ?? 0.0,
            categoryFlags[safe: 3] ?? 0.0,
            categoryFlags[safe: 4] ?? 0.0,
            normalizedPosition,
            normalizedDuration,
            priorScore,
            clamp01(priorTrust),
            sponsorHit,
            relativeWindowStart,
            sequencePosition,
        ]

        precondition(rawFeatures.count == Constant.featureCount)
        return rawFeatures.map(sanitizeFeature)
    }

    private static func downsample(_ windows: [FeatureWindow], maxCount: Int) -> [FeatureWindow] {
        guard windows.count > maxCount else { return windows }
        guard maxCount > 1 else { return Array(windows.prefix(maxCount)) }
        return (0..<maxCount).map { outputIndex in
            let sourceIndex = Int(
                (Double(outputIndex) * Double(windows.count - 1) / Double(maxCount - 1)).rounded()
            )
            return windows[sourceIndex]
        }
    }

    private static func safeBoundaryAdjustments(
        candidateStart: Double,
        candidateEnd: Double,
        episodeDuration: Double,
        startAdjustment: Double,
        endAdjustment: Double
    ) -> (start: Double, end: Double) {
        let start = clampAdjustment(startAdjustment)
        let end = clampAdjustment(endAdjustment)
        let proposedStart = candidateStart + start
        let proposedEnd = candidateEnd + end
        let hasFiniteEpisodeBounds = episodeDuration.isFinite && episodeDuration > 0
        guard proposedStart.isFinite,
              proposedEnd.isFinite,
              proposedStart >= 0.0,
              !hasFiniteEpisodeBounds || proposedEnd <= episodeDuration,
              proposedEnd - proposedStart >= Constant.minSpanDuration
        else {
            return (0.0, 0.0)
        }
        return (start, end)
    }

    private static func clampAdjustment(_ adjustment: Double) -> Double {
        guard adjustment.isFinite else { return 0.0 }
        return max(-Constant.maxBoundaryAdjust, min(Constant.maxBoundaryAdjust, adjustment))
    }

    private static func knownSponsorHit(candidate: LexicalCandidate, priors: ShowPriors) -> Double {
        guard !priors.knownSponsors.isEmpty else { return 0.0 }
        let evidence = candidate.evidenceText.lowercased()
        guard !evidence.isEmpty else { return 0.0 }
        return priors.knownSponsors.contains { sponsor in
            let normalizedSponsor = sponsor.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            return !normalizedSponsor.isEmpty && sponsorTermMatches(normalizedSponsor, in: evidence)
        } ? 1.0 : 0.0
    }

    private static func sponsorTermMatches(_ sponsor: String, in evidence: String) -> Bool {
        var searchStart = evidence.startIndex
        while let range = evidence.range(of: sponsor, range: searchStart..<evidence.endIndex) {
            if isTermBoundary(before: range.lowerBound, in: evidence),
               isTermBoundary(after: range.upperBound, in: evidence) {
                return true
            }
            searchStart = range.upperBound
        }
        return false
    }

    private static func isTermBoundary(before index: String.Index, in text: String) -> Bool {
        guard index > text.startIndex else { return true }
        return !isAlphaNumeric(text[text.index(before: index)])
    }

    private static func isTermBoundary(after index: String.Index, in text: String) -> Bool {
        guard index < text.endIndex else { return true }
        return !isAlphaNumeric(text[index])
    }

    private static func isAlphaNumeric(_ character: Character) -> Bool {
        character.unicodeScalars.allSatisfy { CharacterSet.alphanumerics.contains($0) }
    }

    private static func normalizedPosition(
        startTime: Double,
        endTime: Double,
        episodeDuration: Double
    ) -> Double {
        guard episodeDuration > 0 else { return 0.0 }
        return clamp01(((startTime + endTime) / 2.0) / episodeDuration)
    }

    private static func speakerClusterFeature(_ speakerClusterId: Int?) -> Double {
        guard let speakerClusterId else { return 0.0 }
        return clamp01(Double(abs(speakerClusterId % 16)) / 15.0)
    }

    private static func musicBedLevelFeature(_ level: MusicBedLevel) -> Double {
        switch level {
        case .none:
            return 0.0
        case .background:
            return 0.5
        case .foreground:
            return 1.0
        }
    }

    private static func sanitizeFeature(_ value: Double) -> Double {
        guard value.isFinite else { return 0.0 }
        return max(-1.0, min(1.0, value))
    }

    private static func clamp01(_ value: Double) -> Double {
        guard value.isFinite else { return 0.0 }
        return max(0.0, min(1.0, value))
    }
}

final class CoreMLLayer2SequencePredictor: @unchecked Sendable, Layer2SequencePredicting {
    private let model: MLModel
    private let inputName: String
    private let probabilityOutputName: String
    private let startAdjustmentOutputName: String
    private let endAdjustmentOutputName: String
    private let expectedInputShape: [NSNumber]?

    init(
        model: MLModel,
        inputName: String = "sequence",
        probabilityOutputName: String = "adProbability",
        startAdjustmentOutputName: String = "boundaryStartAdjust",
        endAdjustmentOutputName: String = "boundaryEndAdjust",
        expectedInputShape: [NSNumber]? = nil
    ) {
        self.model = model
        self.inputName = inputName
        self.probabilityOutputName = probabilityOutputName
        self.startAdjustmentOutputName = startAdjustmentOutputName
        self.endAdjustmentOutputName = endAdjustmentOutputName
        self.expectedInputShape = expectedInputShape
            ?? model.modelDescription.inputDescriptionsByName[inputName]?.multiArrayConstraint?.shape
    }

    static func bundled(
        bundle: Bundle = .main,
        modelName: String = CoreMLSequenceClassifier.bundledModelName
    ) -> CoreMLLayer2SequencePredictor? {
        guard let url = bundle.url(forResource: modelName, withExtension: "mlmodelc"),
              let model = try? MLModel(contentsOf: url) else {
            return nil
        }
        return CoreMLLayer2SequencePredictor(model: model)
    }

    func predict(input: Layer2SequenceModelInput) throws -> Layer2SequencePrediction {
        let array = try makeMultiArray(from: input.featureMatrix)
        let provider = try MLDictionaryFeatureProvider(dictionary: [inputName: array])
        let output = try model.prediction(from: provider)
        return Layer2SequencePrediction(
            adProbability: try scalarOutput(named: probabilityOutputName, from: output),
            startAdjustment: try scalarOutput(named: startAdjustmentOutputName, from: output),
            endAdjustment: try scalarOutput(named: endAdjustmentOutputName, from: output)
        )
    }

    static func normalizeMatrix(
        _ matrix: [[Double]],
        expectedShape: [NSNumber]?
    ) throws -> (matrix: [[Double]], shape: [NSNumber]) {
        guard let first = matrix.first, !first.isEmpty else {
            throw Layer2SequenceModelError.invalidFeatureShape
        }
        let featureCount = first.count
        guard matrix.allSatisfy({ $0.count == featureCount }) else {
            throw Layer2SequenceModelError.invalidFeatureShape
        }

        guard let expectedShape, !expectedShape.isEmpty else {
            return (
                matrix,
                [NSNumber(value: matrix.count), NSNumber(value: featureCount)]
            )
        }

        let dimensions = expectedShape.map(\.intValue)
        let sequenceLength: Int
        let expectedFeatureCount: Int
        switch dimensions {
        case let shape where shape.count == 2:
            sequenceLength = shape[0]
            expectedFeatureCount = shape[1]
        case let shape where shape.count == 3 && shape[0] == 1:
            sequenceLength = shape[1]
            expectedFeatureCount = shape[2]
        default:
            throw Layer2SequenceModelError.unsupportedFeatureShape(expectedShape)
        }

        guard sequenceLength > 0,
              expectedFeatureCount == featureCount else {
            throw Layer2SequenceModelError.unsupportedFeatureShape(expectedShape)
        }

        var normalized = Array(matrix.prefix(sequenceLength))
        if normalized.count < sequenceLength {
            normalized.append(contentsOf: Array(
                repeating: Array(repeating: 0.0, count: featureCount),
                count: sequenceLength - normalized.count
            ))
        }
        return (normalized, expectedShape)
    }

    private func makeMultiArray(from matrix: [[Double]]) throws -> MLMultiArray {
        let normalized = try Self.normalizeMatrix(matrix, expectedShape: expectedInputShape)
        let matrix = normalized.matrix
        let featureCount = matrix[0].count
        let array = try MLMultiArray(
            shape: normalized.shape,
            dataType: .float32
        )
        for row in 0..<matrix.count {
            for column in 0..<featureCount {
                let index = row * featureCount + column
                array[index] = NSNumber(value: Float(matrix[row][column]))
            }
        }
        return array
    }

    private func scalarOutput(
        named name: String,
        from output: MLFeatureProvider
    ) throws -> Double {
        guard let value = output.featureValue(for: name) else {
            throw Layer2SequenceModelError.missingOutput(name)
        }
        switch value.type {
        case .double:
            return value.doubleValue
        case .int64:
            return Double(value.int64Value)
        case .multiArray:
            // Cycle 1 M4: guard against zero-length arrays before subscripting.
            // `MLMultiArray.subscript(_:)` raises an NSException on out-of-bounds
            // access — that exception cannot be caught by Swift's `try` / `do`,
            // so the `do { try predictor.predict(...) }` wrapper above would
            // not save us. Treat empty multiArrays the same as a missing output.
            guard let array = value.multiArrayValue, array.count > 0 else {
                throw Layer2SequenceModelError.missingOutput(name)
            }
            return array[0].doubleValue
        case .dictionary:
            let dict = value.dictionaryValue
            for key in ["ad", "true", "1"] {
                if let probability = dict[AnyHashable(key)] {
                    return probability.doubleValue
                }
            }
            if let probability = dict[AnyHashable(1)] {
                return probability.doubleValue
            }
            throw Layer2SequenceModelError.missingOutput(name)
        default:
            throw Layer2SequenceModelError.missingOutput(name)
        }
    }
}

private extension Array {
    subscript(safe index: Int) -> Element? {
        indices.contains(index) ? self[index] : nil
    }
}
