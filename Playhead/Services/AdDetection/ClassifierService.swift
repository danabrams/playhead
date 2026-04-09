// ClassifierService.swift
// Layer 2 of the ad detection pipeline: sequence classifier.
//
// Refines rough LexicalCandidate regions using combined acoustic features
// (RMS, spectral flux, music probability, speaker clustering) and lexical
// confidence into stable ad spans with calibrated probability scores.
//
// Two implementations:
//   1. RuleBasedClassifier — heuristic combination of feature + lexical signals.
//      Ships as the default until a trained CoreML model is available.
//   2. (Future) CoreMLClassifier — sequence model consuming the same inputs.
//
// CoreML model interface (when trained):
//   Input:  [CandidateFeatureVector] — one per feature window in the region
//           Each vector: rms, spectralFlux, musicProbability, pauseProbability,
//                        speakerClusterId (one-hot), lexicalConfidence,
//                        categoryFlags (5 bools), positionInEpisode (0..1)
//   Output: adProbability (Float), boundaryStartAdjust (Float),
//           boundaryEndAdjust (Float)
//   The model should be a small GRU or 1-D conv (~200 KB) exportable via
//   coremltools from a PyTorch training pipeline.

import Foundation
import OSLog

// MARK: - Classifier Input / Output

/// A candidate region with its associated acoustic features and lexical score,
/// ready for classification.
struct ClassifierInput: Sendable {
    /// The lexical candidate that seeded this region.
    let candidate: LexicalCandidate
    /// Feature windows overlapping the candidate time range, sorted by startTime.
    let featureWindows: [FeatureWindow]
    /// Episode total duration, used to compute relative position.
    let episodeDuration: Double
}

/// Classification result for a single candidate region.
struct ClassifierResult: Sendable {
    /// Original candidate ID.
    let candidateId: String
    /// Analysis asset ID.
    let analysisAssetId: String
    /// Refined start time after boundary adjustment.
    let startTime: Double
    /// Refined end time after boundary adjustment.
    let endTime: Double
    /// Calibrated ad probability (0.0...1.0).
    let adProbability: Double
    /// How much the start boundary was adjusted (negative = earlier).
    let startAdjustment: Double
    /// How much the end boundary was adjusted (positive = later).
    let endAdjustment: Double
    /// Breakdown of contributing signals for debugging.
    let signalBreakdown: SignalBreakdown
}

/// Debugging breakdown of individual signal contributions.
struct SignalBreakdown: Sendable {
    let lexicalScore: Double
    let rmsDropScore: Double
    let spectralChangeScore: Double
    let musicScore: Double
    let speakerChangeScore: Double
    let priorScore: Double
}

// MARK: - Per-show priors

/// Parsed per-show priors from PodcastProfile for weighting classifications.
struct ShowPriors: Sendable {
    /// Normalized ad slot positions as fractions of episode duration (0..1).
    /// Parsed from PodcastProfile.normalizedAdSlotPriors JSON.
    let slotPositions: [Double]
    /// Known sponsor names for boosted confidence.
    let knownSponsors: [String]
    /// Jingle fingerprints for boundary detection.
    let jingleFingerprints: [String]
    /// How much to trust priors (based on observation count).
    let trustWeight: Double

    static let empty = ShowPriors(
        slotPositions: [],
        knownSponsors: [],
        jingleFingerprints: [],
        trustWeight: 0.0
    )

    /// Parse priors from a PodcastProfile.
    static func from(profile: PodcastProfile?) -> ShowPriors {
        guard let profile else { return .empty }

        let slots: [Double]
        if let json = profile.normalizedAdSlotPriors,
           let data = json.data(using: .utf8),
           let parsed = try? JSONDecoder().decode([Double].self, from: data) {
            slots = parsed
        } else {
            slots = []
        }

        let sponsors: [String]
        if let lexicon = profile.sponsorLexicon, !lexicon.isEmpty {
            sponsors = lexicon
                .components(separatedBy: ",")
                .map { $0.trimmingCharacters(in: .whitespaces).lowercased() }
                .filter { !$0.isEmpty }
        } else {
            sponsors = []
        }

        let jingles: [String]
        if let json = profile.jingleFingerprints,
           let data = json.data(using: .utf8),
           let parsed = try? JSONDecoder().decode([String].self, from: data) {
            jingles = parsed
        } else {
            jingles = []
        }

        // Trust weight scales with observation count, saturating around 20.
        let trust = min(Double(profile.observationCount) / 20.0, 1.0)

        return ShowPriors(
            slotPositions: slots,
            knownSponsors: sponsors,
            jingleFingerprints: jingles,
            trustWeight: trust
        )
    }
}

// MARK: - Shared Region Scoring

/// Shared signal scoring helpers used both by the hot-path classifier and
/// by the Phase 4 region feature backfill.
enum RegionScoring {
    /// RMS drop threshold: a window whose RMS is this fraction below the
    /// region mean is considered a significant energy change.
    private static let rmsDropFraction: Double = 0.35

    /// Spectral flux threshold: windows above this percentile of the region's
    /// flux distribution indicate a timbral transition.
    private static let spectralFluxPercentile: Double = 0.80

    /// How close (as a fraction of episode duration) a candidate must be
    /// to a known ad slot position to receive the prior boost.
    private static let slotProximityThreshold: Double = 0.05

    /// Score based on RMS energy drops at region boundaries.
    /// Ads typically start/end with a noticeable volume change.
    static func computeRmsDropScore(windows: [FeatureWindow]) -> Double {
        guard windows.count >= 3 else { return 0.0 }

        let rmsValues = windows.map(\.rms)
        let mean = rmsValues.reduce(0, +) / Double(rmsValues.count)
        guard mean > 0 else { return 0.0 }

        // Check first and last windows for energy transition.
        let firstRms = rmsValues[0]
        let lastRms = rmsValues[rmsValues.count - 1]
        let interiorMean = rmsValues.dropFirst().dropLast().reduce(0, +)
            / Double(max(rmsValues.count - 2, 1))

        var score = 0.0

        // Entry transition: RMS change at start.
        let entryDelta = abs(firstRms - interiorMean) / mean
        if entryDelta > Self.rmsDropFraction {
            score += 0.5
        }

        // Exit transition: RMS change at end.
        let exitDelta = abs(lastRms - interiorMean) / mean
        if exitDelta > Self.rmsDropFraction {
            score += 0.5
        }

        return min(score, 1.0)
    }

    /// Score based on spectral flux spikes at boundaries.
    /// High spectral flux indicates timbral change (different speaker/production).
    static func computeSpectralChangeScore(windows: [FeatureWindow]) -> Double {
        guard windows.count >= 3 else { return 0.0 }

        let fluxValues = windows.map(\.spectralFlux)
        let sorted = fluxValues.sorted()
        let threshold = sorted[Int(Double(sorted.count) * Self.spectralFluxPercentile)]

        // Check boundary windows for spectral transitions.
        var score = 0.0

        if fluxValues[0] > threshold { score += 0.4 }
        if fluxValues[fluxValues.count - 1] > threshold { score += 0.4 }

        // Interior high-flux windows suggest production changes (jingles, etc).
        let interiorHighFlux = fluxValues.dropFirst().dropLast()
            .filter { $0 > threshold }.count
        let interiorFraction = Double(interiorHighFlux)
            / Double(max(fluxValues.count - 2, 1))
        score += interiorFraction * 0.2

        return min(score, 1.0)
    }

    /// Score based on music probability across the region.
    /// Ad segments often have background music or jingles.
    static func computeMusicScore(windows: [FeatureWindow]) -> Double {
        guard !windows.isEmpty else { return 0.0 }

        let musicProbs = windows.map(\.musicProbability)
        let avgMusic = musicProbs.reduce(0, +) / Double(musicProbs.count)

        // Ads often have some music. A moderate music probability is a
        // positive signal. Very high music (>0.8) might be actual music
        // content, so we cap the contribution.
        if avgMusic > 0.8 {
            return 0.5 // Could be music content, not just ad jingle
        }
        return min(avgMusic * 1.5, 1.0)
    }

    /// Score based on speaker cluster changes at region boundaries.
    /// Ad reads often involve a different speaker or a return to the
    /// main host after the ad.
    static func computeSpeakerChangeScore(windows: [FeatureWindow]) -> Double {
        guard windows.count >= 2 else { return 0.0 }

        let speakerIds = windows.compactMap(\.speakerClusterId)
        guard speakerIds.count >= 2 else { return 0.0 }

        let uniqueSpeakers = Set(speakerIds)

        // Multiple speakers in the region = higher ad probability.
        if uniqueSpeakers.count >= 3 { return 1.0 }
        if uniqueSpeakers.count == 2 { return 0.7 }

        // Check if boundary speakers differ from interior.
        if let first = windows.first?.speakerClusterId,
           let last = windows.last?.speakerClusterId,
           first == last {
            // Same speaker at boundaries, might be host returning.
            let interiorIds = windows.dropFirst().dropLast().compactMap(\.speakerClusterId)
            let interiorSet = Set(interiorIds)
            if !interiorSet.isEmpty && !interiorSet.contains(first) {
                return 0.9 // Classic "host -> ad reader -> host" pattern
            }
        }

        return 0.0
    }

    /// Score based on per-show ad slot priors.
    /// Shows that consistently place ads at certain positions get a boost.
    static func computePriorScore(
        startTime: Double,
        endTime: Double,
        episodeDuration: Double,
        priors: ShowPriors
    ) -> Double {
        guard !priors.slotPositions.isEmpty,
              episodeDuration > 0,
              priors.trustWeight > 0
        else { return 0.0 }

        let candidateCenter = (startTime + endTime) / 2.0
        let normalizedPosition = candidateCenter / episodeDuration

        // Find the closest known ad slot position.
        let minDistance = priors.slotPositions
            .map { abs($0 - normalizedPosition) }
            .min() ?? 1.0

        guard minDistance <= Self.slotProximityThreshold else { return 0.0 }

        // Score inversely proportional to distance, scaled by trust.
        let proximity = 1.0 - (minDistance / Self.slotProximityThreshold)
        return proximity * priors.trustWeight
    }
}

// MARK: - ClassifierService Protocol

/// Protocol for ad region classifiers. Both the rule-based heuristic
/// and the future CoreML model implement this interface.
protocol ClassifierService: Sendable {
    /// Classify a batch of candidate regions. Returns results sorted by
    /// start time. Implementations must complete within the hot-path
    /// budget (< 100 ms per candidate).
    func classify(inputs: [ClassifierInput], priors: ShowPriors) -> [ClassifierResult]

    /// Classify a single candidate. Convenience for streaming hot-path use.
    func classify(input: ClassifierInput, priors: ShowPriors) -> ClassifierResult
}

// MARK: - RuleBasedClassifier

/// Heuristic classifier that combines acoustic features and lexical signals
/// to produce calibrated ad probabilities. Serves as the production default
/// until a trained CoreML model replaces it.
///
/// Scoring pipeline:
///   1. Compute per-signal scores across the candidate's feature windows.
///   2. Apply per-show prior adjustments.
///   3. Combine via weighted sum, then calibrate through a sigmoid.
///   4. Snap boundaries to acoustic transitions (RMS drops / spectral flux peaks).
struct RuleBasedClassifier: ClassifierService {

    private let logger = Logger(subsystem: "com.playhead", category: "RuleBasedClassifier")

    // MARK: - Tuning Constants

    /// Signal weights for the final combination.
    private enum Weight {
        static let lexical:        Double = 0.40
        static let rmsDrop:        Double = 0.20
        static let spectralChange: Double = 0.15
        static let music:          Double = 0.10
        static let speakerChange:  Double = 0.05
        static let prior:          Double = 0.10
    }

    /// Sigmoid steepness for calibration.
    private static let sigmoidK: Double = 8.0
    /// Sigmoid midpoint (raw score at which output = 0.5).
    /// Set to 0.25 so strong lexical signals alone (weight 0.40, max
    /// contribution ~0.38) produce calibrated scores above the
    /// orchestrator's enter threshold (0.65). This aligns with
    /// LexicalScanner's design: "catches 60-70% of ads via lexical
    /// signals alone."
    private static let sigmoidMid: Double = 0.25

    /// Maximum boundary adjustment (seconds).
    private static let maxBoundaryAdjust: Double = 3.0

    // MARK: - Batch API

    func classify(inputs: [ClassifierInput], priors: ShowPriors) -> [ClassifierResult] {
        inputs.map { classify(input: $0, priors: priors) }
    }

    // MARK: - Single Candidate API

    func classify(input: ClassifierInput, priors: ShowPriors) -> ClassifierResult {
        let windows = input.featureWindows
        let candidate = input.candidate

        // --- Per-signal scores ---

        let lexicalScore = candidate.confidence

        let rmsDropScore = RegionScoring.computeRmsDropScore(windows: windows)
        let spectralChangeScore = RegionScoring.computeSpectralChangeScore(windows: windows)
        let musicScore = RegionScoring.computeMusicScore(windows: windows)
        let speakerChangeScore = RegionScoring.computeSpeakerChangeScore(windows: windows)
        let priorScore = RegionScoring.computePriorScore(
            startTime: candidate.startTime,
            endTime: candidate.endTime,
            episodeDuration: input.episodeDuration,
            priors: priors
        )

        // --- Weighted combination ---

        let rawScore =
            Weight.lexical        * lexicalScore +
            Weight.rmsDrop        * rmsDropScore +
            Weight.spectralChange * spectralChangeScore +
            Weight.music          * musicScore +
            Weight.speakerChange  * speakerChangeScore +
            Weight.prior          * priorScore

        // --- Calibrate via sigmoid ---

        let calibrated = sigmoid(rawScore, k: Self.sigmoidK, mid: Self.sigmoidMid)

        // --- Boundary adjustment ---

        let (startAdj, endAdj) = computeBoundaryAdjustments(
            windows: windows,
            candidateStart: candidate.startTime,
            candidateEnd: candidate.endTime
        )

        let breakdown = SignalBreakdown(
            lexicalScore: lexicalScore,
            rmsDropScore: rmsDropScore,
            spectralChangeScore: spectralChangeScore,
            musicScore: musicScore,
            speakerChangeScore: speakerChangeScore,
            priorScore: priorScore
        )

        return ClassifierResult(
            candidateId: candidate.id,
            analysisAssetId: candidate.analysisAssetId,
            startTime: candidate.startTime + startAdj,
            endTime: candidate.endTime + endAdj,
            adProbability: calibrated,
            startAdjustment: startAdj,
            endAdjustment: endAdj,
            signalBreakdown: breakdown
        )
    }

    // MARK: - Boundary Adjustment

    /// Snap candidate boundaries to the nearest acoustic transition.
    /// Looks for RMS drops or spectral flux peaks near the edges.
    private func computeBoundaryAdjustments(
        windows: [FeatureWindow],
        candidateStart: Double,
        candidateEnd: Double
    ) -> (startAdjust: Double, endAdjust: Double) {
        guard windows.count >= 3 else { return (0.0, 0.0) }

        let startAdj = findNearestTransition(
            windows: windows,
            anchor: candidateStart,
            searchDirection: .backward
        )
        let endAdj = findNearestTransition(
            windows: windows,
            anchor: candidateEnd,
            searchDirection: .forward
        )

        return (
            max(-Self.maxBoundaryAdjust, min(startAdj, Self.maxBoundaryAdjust)),
            max(-Self.maxBoundaryAdjust, min(endAdj, Self.maxBoundaryAdjust))
        )
    }

    private enum SearchDirection {
        case forward, backward
    }

    /// Find the nearest acoustic transition (RMS drop or spectral spike)
    /// near an anchor time. Returns the time offset to adjust the boundary.
    private func findNearestTransition(
        windows: [FeatureWindow],
        anchor: Double,
        searchDirection: SearchDirection
    ) -> Double {
        // Find windows near the anchor.
        let nearbyWindows = windows.filter {
            abs(($0.startTime + $0.endTime) / 2.0 - anchor) <= Self.maxBoundaryAdjust
        }
        guard nearbyWindows.count >= 2 else { return 0.0 }

        // Look for the biggest RMS jump between consecutive windows.
        var bestDelta = 0.0
        var bestTime = anchor

        for i in 0 ..< nearbyWindows.count - 1 {
            let w1 = nearbyWindows[i]
            let w2 = nearbyWindows[i + 1]
            let rmsDelta = abs(w2.rms - w1.rms)
            let fluxBoost = max(w1.spectralFlux, w2.spectralFlux) * 0.5
            let combined = rmsDelta + fluxBoost

            if combined > bestDelta {
                bestDelta = combined
                bestTime = (w1.endTime + w2.startTime) / 2.0
            }
        }

        // Only adjust if we found a meaningful transition.
        guard bestDelta > 0.05 else { return 0.0 }

        return bestTime - anchor
    }

    // MARK: - Calibration

    /// Sigmoid function for calibrating raw scores to probabilities.
    private func sigmoid(_ x: Double, k: Double, mid: Double) -> Double {
        1.0 / (1.0 + exp(-k * (x - mid)))
    }
}
