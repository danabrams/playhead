// FeatureExtraction.swift
// On-device audio feature extraction for the ad detection hot path.
//
// Extracts per-window acoustic features from decoded analysis shards:
//   - RMS energy (volume level)
//   - Spectral flux (timbral change — music beds, stingers)
//   - Pause probability (silence/low-energy detection)
//   - Music probability (SoundAnalysis built-in classifier + acoustic fallback)
//   - Speaker change proxy score (acoustic fallback in Phase A)
//   - Music bed change score (music probability derivative)
//   - Speaker cluster ID (deferred — requires validated labels)
//   - Jingle hash (deferred — requires fingerprint database)
//
// Uses Accelerate framework (vDSP, vForce) for all DSP. Runs faster than
// real-time on target devices. Feature windows are persisted to SQLite via
// AnalysisStore and never recomputed.

@preconcurrency import AVFoundation
import Accelerate
import Foundation
#if canImport(SoundAnalysis)
import SoundAnalysis
#endif

// MARK: - Configuration

/// Parameters controlling feature extraction behaviour.
struct FeatureExtractionConfig: Sendable {
    /// Duration of each feature window in seconds.
    /// Defaults to 2.0 s — short enough to capture ad boundaries, long enough
    /// for stable spectral estimates.
    let windowDuration: TimeInterval

    /// Overlap fraction between consecutive windows (0 = no overlap, 0.5 = 50%).
    let overlapFraction: Double

    /// Sample rate of input audio (must match AnalysisAudioService output).
    let sampleRate: Double

    /// FFT size for spectral analysis. Must be a power of 2.
    let fftSize: Int

    /// RMS threshold below which a window is considered a pause candidate.
    let pauseRmsThreshold: Float

    /// Current feature version tag written to each FeatureWindow.
    let featureVersion: Int

    static let `default` = FeatureExtractionConfig(
        windowDuration: 2.0,
        overlapFraction: 0.0,
        sampleRate: 16_000,
        fftSize: 1024,
        pauseRmsThreshold: 0.03,
        // featureVersion history:
        //   v1: initial release. pauseRmsThreshold 0.005, linear RMS curve —
        //       produced a ~0.002 mean pauseProbability on real podcast audio
        //       (effectively always "not a pause").
        //   v2: recalibrated threshold to 0.03 and switched to a smooth
        //       monotonic log-RMS curve. Observed ~0.020 mean on the Conan
        //       verification episode — still modest, but non-degenerate.
        //   v3: add speakerChangeProxyScore / musicBedChangeScore and replace
        //       the hardcoded musicProbability stub with SoundAnalysis-backed
        //       extraction plus acoustic fallback.
        //   v4: persist seam-state checkpoints and retro-correct shard-boundary
        //       smoothing so resumed extraction does not keep the old v3
        //       boundary bias.
        //
        // Consumers only serve rows at the current feature version, and
        // extraction rewinds coverage to the earliest stale window before
        // reprocessing so older rows are replaced incrementally.
        featureVersion: 4
    )
}

// MARK: - FeatureExtractionError

enum FeatureExtractionError: Error, CustomStringConvertible {
    case emptyInput
    case fftSetupFailed
    case alreadyExtracted

    var description: String {
        switch self {
        case .emptyInput: "No audio samples provided for feature extraction"
        case .fftSetupFailed: "Failed to create vDSP FFT setup"
        case .alreadyExtracted: "Features already extracted for this range"
        }
    }
}

enum FeatureSignalExtraction {
    private static let epsilon: Double = 1e-9

    struct MusicProbabilityTimeline: Sendable {
        struct Observation: Sendable {
            let startTime: Double
            let endTime: Double
            let probability: Double

            var centerTime: Double { (startTime + endTime) / 2.0 }
        }

        private let observations: [Observation]
        // playhead-rfu-aac M5: precomputed at init so probability(...)
        // can use it as a backwards-scan termination bound and avoid the
        // O(n) full-sweep `.filter` from the prior implementation. Equal
        // to the longest observation duration in the corpus, so any
        // observation with `startTime <= windowStart - maxObservationDuration`
        // cannot overlap a query window starting at windowStart.
        private let maxObservationDuration: Double

        init(observations: [Observation]) {
            let sorted = observations.sorted {
                if $0.startTime != $1.startTime { return $0.startTime < $1.startTime }
                if $0.endTime != $1.endTime { return $0.endTime < $1.endTime }
                return $0.probability > $1.probability
            }
            self.observations = sorted
            self.maxObservationDuration = sorted.reduce(0.0) { acc, obs in
                max(acc, obs.endTime - obs.startTime)
            }
        }

        func offset(by timeOffset: Double) -> MusicProbabilityTimeline {
            MusicProbabilityTimeline(
                observations: observations.map { observation in
                    Observation(
                        startTime: observation.startTime + timeOffset,
                        endTime: observation.endTime + timeOffset,
                        probability: observation.probability
                    )
                }
            )
        }

        func probability(forWindowStart windowStartTime: Double, end windowEndTime: Double) -> Double? {
            guard !observations.isEmpty else { return nil }

            // playhead-rfu-aac M5: prior shape `.filter { ... }` swept all
            // observations every call. With per-shard observation counts in
            // the thousands and one query per analysis window, that became
            // visible in CPU profiles. Replace with a binary-search-bounded
            // backwards scan: observations are sorted by startTime, so the
            // upper bound at `startTime >= windowEndTime` caps the search
            // forward, and `maxObservationDuration` caps it backward (no
            // observation starting earlier than `windowStart - maxDuration`
            // can possibly overlap because its endTime <= startTime + maxDuration).
            let upperBound = upperBoundIndex(forStartTime: windowEndTime)
            let lowerCutoff = windowStartTime - maxObservationDuration
            var bestOverlapProbability: Double? = nil
            var idx = upperBound - 1
            while idx >= 0 {
                let obs = observations[idx]
                if obs.startTime <= lowerCutoff { break }
                if obs.endTime > windowStartTime && obs.startTime < windowEndTime {
                    if bestOverlapProbability == nil || obs.probability > bestOverlapProbability! {
                        bestOverlapProbability = obs.probability
                    }
                }
                idx -= 1
            }
            if let best = bestOverlapProbability {
                return best
            }

            // No overlap: fall back to the nearest-by-center neighbor.
            // This branch is rare (only when the query falls in a gap)
            // so the linear scan here is acceptable.
            let windowCenter = (windowStartTime + windowEndTime) / 2.0
            guard let nearest = observations.min(by: {
                abs($0.centerTime - windowCenter) < abs($1.centerTime - windowCenter)
            }) else {
                return nil
            }
            return nearest.probability
        }

        /// First index `i` such that `observations[i].startTime >= startTime`,
        /// or `observations.count` when no such index exists. Standard
        /// half-open upper-bound shape.
        private func upperBoundIndex(forStartTime startTime: Double) -> Int {
            var lo = 0
            var hi = observations.count
            while lo < hi {
                let mid = (lo + hi) / 2
                if observations[mid].startTime < startTime {
                    lo = mid + 1
                } else {
                    hi = mid
                }
            }
            return lo
        }
    }

    static func buildMusicProbabilityTimeline(
        samples: [Float],
        sampleRate: Double
    ) -> MusicProbabilityTimeline? {
        #if canImport(SoundAnalysis)
        return soundAnalysisMusicProbabilityTimeline(samples: samples, sampleRate: sampleRate)
        #else
        return nil
        #endif
    }

    static func acousticMusicProbability(
        magnitudes: [Float],
        rms: Float,
        spectralFlux: Float
    ) -> Double {
        acousticMusicProbability(
            magnitudes: magnitudes,
            rms: Double(rms),
            spectralFlux: Double(spectralFlux)
        )
    }

    static func musicProbability(
        acousticProbability: Double,
        timeline: MusicProbabilityTimeline?,
        windowStartTime: Double,
        windowEndTime: Double
    ) -> Double {
        let timelineProbability = timeline?.probability(
            forWindowStart: windowStartTime,
            end: windowEndTime
        ) ?? 0
        return max(acousticProbability, timelineProbability)
    }

    static func musicBedChangeScore(
        currentMusicProbability: Double,
        previousMusicProbability: Double?
    ) -> Double {
        guard let previousMusicProbability else { return 0 }
        return clamp(abs(currentMusicProbability - previousMusicProbability) * 1.5)
    }

    static func speakerChangeProxyScore(
        currentRms: Double,
        previousRms: Double?,
        currentMagnitudes: [Float],
        previousMagnitudes: [Float]?,
        pauseProbability: Double,
        spectralFlux: Double
    ) -> Double {
        let pauseComponent = clamp(pauseProbability)
        let fluxComponent = clamp(spectralFlux * 2.5)
        let timbreComponent = previousMagnitudes.map {
            spectralDistance(current: currentMagnitudes, previous: $0)
        } ?? 0
        let rmsComponent = previousRms.map { clamp(abs(currentRms - $0) * 4.0) } ?? 0

        return clamp(
            pauseComponent * 0.4 +
            fluxComponent * 0.25 +
            timbreComponent * 0.25 +
            rmsComponent * 0.10
        )
    }

    static func smoothSpeakerChangeProxyScores(
        _ rawScores: [Double],
        leadingPreviousRawScore: Double? = nil
    ) -> [Double] {
        guard !rawScores.isEmpty else { return [] }

        return rawScores.enumerated().map { index, rawScore in
            let previous = index > 0 ? rawScores[index - 1] : leadingPreviousRawScore
            let next = index + 1 < rawScores.count ? rawScores[index + 1] : nil
            return smoothedSpeakerChangeProxyScore(
                current: rawScore,
                previous: previous,
                next: next
            )
        }
    }

    static func smoothedSpeakerChangeProxyScore(
        current: Double,
        previous: Double?,
        next: Double?
    ) -> Double {
        var weightedSum = current * 0.5
        var totalWeight = 0.5

        if let previous {
            weightedSum += previous * 0.25
            totalWeight += 0.25
        }

        if let next {
            weightedSum += next * 0.25
            totalWeight += 0.25
        }

        return clamp(weightedSum / totalWeight)
    }

    private static func acousticMusicProbability(
        magnitudes: [Float],
        rms: Double,
        spectralFlux: Double
    ) -> Double {
        guard !magnitudes.isEmpty else { return 0 }

        let total = Double(magnitudes.reduce(0, +))
        guard total > 0 else { return 0 }

        let normalized = magnitudes.map { max(Double($0) / total, epsilon) }
        let mean = 1.0 / Double(normalized.count)
        let logMean = normalized.reduce(0.0) { $0 + log($1) } / Double(normalized.count)
        let flatness = exp(logMean) / mean
        let tonalness = clamp(1.0 - flatness)
        let stability = clamp(1.0 - min(spectralFlux * 2.0, 1.0))
        let energy = clamp((rms - 0.005) / 0.08)

        return clamp(tonalness * 0.55 + stability * 0.30 + energy * 0.15)
    }

    private static func spectralDistance(current: [Float], previous: [Float]) -> Double {
        let count = min(current.count, previous.count)
        guard count > 0 else { return 0 }

        let currentSum = Double(current.prefix(count).reduce(0, +))
        let previousSum = Double(previous.prefix(count).reduce(0, +))
        guard currentSum > 0, previousSum > 0 else { return 0 }

        var l1Distance = 0.0
        for index in 0..<count {
            let currentValue = Double(current[index]) / currentSum
            let previousValue = Double(previous[index]) / previousSum
            l1Distance += abs(currentValue - previousValue)
        }

        return clamp(l1Distance / 2.0)
    }

    private static func clamp(_ value: Double, lower: Double = 0, upper: Double = 1) -> Double {
        min(max(value, lower), upper)
    }

    #if canImport(SoundAnalysis)
    private static func soundAnalysisMusicProbabilityTimeline(
        samples: [Float],
        sampleRate: Double
    ) -> MusicProbabilityTimeline? {
        guard !samples.isEmpty else { return nil }

        guard let format = AVAudioFormat(
            standardFormatWithSampleRate: sampleRate,
            channels: 1
        ), let buffer = AVAudioPCMBuffer(
            pcmFormat: format,
            frameCapacity: AVAudioFrameCount(samples.count)
        ) else {
            return nil
        }

        buffer.frameLength = buffer.frameCapacity
        samples.withUnsafeBufferPointer { source in
            guard
                let channelData = buffer.floatChannelData,
                let baseAddress = source.baseAddress
            else { return }

            channelData[0].assign(from: baseAddress, count: samples.count)
        }

        do {
            let analyzer = SNAudioStreamAnalyzer(format: format)
            let request = try SNClassifySoundRequest(classifierIdentifier: .version1)
            let observer = MusicClassificationObserver(
                musicIdentifier: request.knownClassifications.first {
                    $0.caseInsensitiveCompare("music") == .orderedSame
                } ?? "music"
            )

            try analyzer.add(request, withObserver: observer)
            analyzer.analyze(buffer, atAudioFramePosition: 0)
            analyzer.completeAnalysis()
            return observer.timeline
        } catch {
            #if DEBUG
            print("[FeatureExtraction] SoundAnalysis music classifier failed: \(error)")
            #endif
            return nil
        }
    }

    private final class MusicClassificationObserver: NSObject, SNResultsObserving {
        private let lock = NSLock()
        private let musicIdentifier: String
        private var observations: [MusicProbabilityTimeline.Observation] = []
        private var didFail = false

        init(musicIdentifier: String) {
            self.musicIdentifier = musicIdentifier
        }

        var timeline: MusicProbabilityTimeline? {
            lock.lock()
            defer { lock.unlock() }
            guard !didFail, !observations.isEmpty else { return nil }
            return MusicProbabilityTimeline(observations: observations)
        }

        func request(_ request: SNRequest, didProduce result: SNResult) {
            guard
                let classificationResult = result as? SNClassificationResult,
                let classification = classificationResult.classification(forIdentifier: musicIdentifier)
            else {
                return
            }

            let timeRange = classificationResult.timeRange
            let observation = MusicProbabilityTimeline.Observation(
                startTime: timeRange.start.seconds,
                endTime: timeRange.end.seconds,
                probability: Double(classification.confidence)
            )

            lock.lock()
            observations.append(observation)
            lock.unlock()
        }

        func request(_ request: SNRequest, didFailWithError error: Error) {
            lock.lock()
            didFail = true
            observations.removeAll(keepingCapacity: true)
            lock.unlock()
        }

        func requestDidComplete(_ request: SNRequest) {}
    }
    #endif
}

// MARK: - FeatureExtractionService

/// Extracts acoustic features from analysis shards and persists them to the
/// AnalysisStore. Designed for both hot-path (near-real-time during playback)
/// and backfill (batch processing of downloaded episodes).
///
/// Thread-safe via actor isolation. All DSP runs on the actor's executor,
/// which is a cooperative thread pool — no dedicated queue needed.
actor FeatureExtractionService {
    private struct ExtractionState {
        var previousMagnitudes: [Float]?
        var previousMusicProbability: Double?
        var previousRms: Double?
        var lastRawSpeakerChangeProxyScore: Double?
        var penultimateRawSpeakerChangeProxyScore: Double?
        var lastWindowStartTime: Double?
        var lastWindowEndTime: Double?
        private var rmsSum: Double = 0
        private var rmsCount: Int = 0

        /// Running mean RMS across all windows processed so far.
        /// Returns 0 if no windows have been processed yet.
        var runningMeanRms: Double {
            guard rmsCount > 0 else { return 0 }
            return rmsSum / Double(rmsCount)
        }

        mutating func updateRunningMeanRms(_ rms: Double) {
            rmsSum += rms
            rmsCount += 1
        }

        init() {}

        init(checkpoint: FeatureExtractionCheckpoint) {
            previousMagnitudes = checkpoint.lastMagnitudes
            previousMusicProbability = checkpoint.lastMusicProbability
            previousRms = checkpoint.lastRms
            lastRawSpeakerChangeProxyScore = checkpoint.lastRawSpeakerChangeProxyScore
            penultimateRawSpeakerChangeProxyScore = checkpoint.penultimateRawSpeakerChangeProxyScore
            lastWindowStartTime = checkpoint.lastWindowStartTime
            lastWindowEndTime = checkpoint.lastWindowEndTime
        }
    }

    private struct PriorWindowSmoothingUpdate {
        let startTime: Double
        let endTime: Double
        let speakerChangeProxyScore: Double
    }

    private struct ExtractionWindowInput {
        let samples: [Float]
        let startTime: Double
    }

    private let config: FeatureExtractionConfig
    private let store: AnalysisStore
    private let musicProbabilityTimelineBuilder: @Sendable ([Float], Double) -> FeatureSignalExtraction.MusicProbabilityTimeline?

    // Reusable FFT setup — allocated once, used for every window.
    private let fftSetup: vDSP.FFT<DSPSplitComplex>?
    private let fftLog2n: vDSP_Length

    #if DEBUG
    /// Test-only watermark of the largest in-flight per-call accumulator
    /// observed across the lifetime of this service. Used by
    /// `FeatureExtractionSignalTests.peakAccumulatorBoundedAcrossShards`
    /// to pin the no-double-source-of-truth invariant from playhead-wmjr.
    /// Reset to zero at the start of each `extractAndPersist(...)` call.
    private(set) var _peakInFlightWindowCountForTesting: Int = 0

    func peakInFlightWindowCountForTesting() -> Int {
        _peakInFlightWindowCountForTesting
    }
    #endif

    init(
        store: AnalysisStore,
        config: FeatureExtractionConfig = .default,
        musicProbabilityTimelineBuilder: @escaping @Sendable ([Float], Double) -> FeatureSignalExtraction.MusicProbabilityTimeline? = FeatureSignalExtraction.buildMusicProbabilityTimeline
    ) {
        self.config = config
        self.store = store
        self.musicProbabilityTimelineBuilder = musicProbabilityTimelineBuilder

        // Pre-allocate FFT setup for the configured size.
        let log2n = vDSP_Length(log2(Double(config.fftSize)))
        self.fftLog2n = log2n
        self.fftSetup = vDSP.FFT(log2n: log2n, radix: .radix2, ofType: DSPSplitComplex.self)
    }

    // MARK: - Public API

    /// Extract features from a sequence of analysis shards and persist to SQLite.
    ///
    /// Skips windows that have already been extracted (based on
    /// `featureCoverageEndTime` in the analysis asset). Updates coverage
    /// after each shard so extraction is resumable.
    ///
    /// SQLite (via `AnalysisStore.fetchFeatureWindows(assetId:from:to:)`)
    /// is the sole source of truth for extracted windows — this method
    /// returns no in-memory mirror of what it persisted (playhead-wmjr).
    /// Callers that need the persisted rows should fetch from the store.
    ///
    /// - Parameters:
    ///   - shards: Decoded audio shards from AnalysisAudioService.
    ///   - analysisAssetId: The analysis asset these shards belong to.
    ///   - existingCoverage: End time already covered (skip windows before this).
    ///   - preemption: Optional context from
    ///     `LanePreemptionCoordinator` (playhead-01t8). When provided,
    ///     the service polls `signal.isPreemptionRequested()` after
    ///     each shard's feature batch + checkpoint persists. On a
    ///     true result it `acknowledge()`s the coordinator and returns
    ///     early. This is the (a) "post-shard in FeatureExtraction"
    ///     AND (b) "FeatureExtractionCheckpoint transitions" safe
    ///     points from the bead spec — they are the same boundary
    ///     from the caller's perspective because
    ///     `persistFeatureExtractionBatch` atomically writes
    ///     windows + checkpoint + coverage.
    func extractAndPersist(
        shards: [AnalysisShard],
        analysisAssetId: String,
        existingCoverage: Double = 0,
        preemption: PreemptionContext? = nil
    ) async throws {
        guard !shards.isEmpty else {
            throw FeatureExtractionError.emptyInput
        }

        #if DEBUG
        _peakInFlightWindowCountForTesting = 0
        #endif

        var effectiveCoverage = try await repairCoverageForCurrentFeatureVersion(
            analysisAssetId: analysisAssetId,
            existingCoverage: existingCoverage
        )
        var extractionState = try await loadExtractionState(
            analysisAssetId: analysisAssetId,
            coverageEndTime: effectiveCoverage
        )

        for shard in shards {
            try Task.checkCancellation()

            // Skip shards fully covered by prior extraction.
            let shardEnd = shard.startTime + shard.duration
            if shardEnd <= effectiveCoverage { continue }

            let input = makeExtractionWindowInput(
                for: shard,
                effectiveCoverage: effectiveCoverage
            )
            guard !input.samples.isEmpty else { continue }

            let extractionResult = extractWindows(
                from: input.samples,
                shardStartTime: input.startTime,
                analysisAssetId: analysisAssetId,
                initialState: extractionState
            )
            let windows = extractionResult.windows
            extractionState = extractionResult.state

            guard !windows.isEmpty else { continue }

            #if DEBUG
            if windows.count > _peakInFlightWindowCountForTesting {
                _peakInFlightWindowCountForTesting = windows.count
            }
            #endif

            let checkpoint = makeCheckpoint(
                analysisAssetId: analysisAssetId,
                state: extractionState
            )
            let priorWindowStoreUpdate = extractionResult.priorWindowUpdate.map { priorWindowUpdate in
                FeatureWindowSpeakerChangeProxyUpdate(
                    assetId: analysisAssetId,
                    startTime: priorWindowUpdate.startTime,
                    endTime: priorWindowUpdate.endTime,
                    featureVersion: config.featureVersion,
                    speakerChangeProxyScore: priorWindowUpdate.speakerChangeProxyScore
                )
            }

            // Persist the batch atomically so coverage never advances beyond
            // the checkpoint required to resume seam-aware smoothing.
            try await store.persistFeatureExtractionBatch(
                assetId: analysisAssetId,
                windows: windows,
                priorWindowUpdate: priorWindowStoreUpdate,
                checkpoint: checkpoint,
                coverageEndTime: windows.last?.endTime
            )

            if let lastWindow = windows.last {
                effectiveCoverage = lastWindow.endTime
            }

            // playhead-01t8 safe points (a) + (b): the shard's windows,
            // checkpoint, and coverage watermark are durable. If a
            // higher-lane job has flipped the preemption signal we
            // acknowledge and exit here — the DB is crash-safe and
            // the next run will resume from `effectiveCoverage`.
            if let preemption, await preemption.isPreemptionRequested() {
                await preemption.acknowledge()
                return
            }
        }
    }

    /// Extract features from raw samples without persisting.
    /// Useful for preview / testing.
    func extract(
        from samples: [Float],
        startTime: Double,
        analysisAssetId: String
    ) -> [FeatureWindow] {
        extractWindows(
            from: samples,
            shardStartTime: startTime,
            analysisAssetId: analysisAssetId,
            initialState: ExtractionState()
        ).windows
    }

    // MARK: - Window Extraction

    /// Slice samples into windows and compute features for each.
    private func extractWindows(
        from samples: [Float],
        shardStartTime: TimeInterval,
        analysisAssetId: String,
        initialState: ExtractionState
    ) -> (
        windows: [FeatureWindow],
        state: ExtractionState,
        priorWindowUpdate: PriorWindowSmoothingUpdate?
    ) {
        let samplesPerWindow = Int(config.windowDuration * config.sampleRate)
        let hopSize = Int(Double(samplesPerWindow) * (1.0 - config.overlapFraction))
        guard hopSize > 0, samplesPerWindow > 0 else {
            return ([], initialState, nil)
        }

        // playhead-rfu-aac L3: bail early when the input shard is shorter
        // than a single window. The body's `while` already short-circuits
        // in this case, but the music-probability timeline builder above
        // it runs SoundAnalysis end-to-end on the input and is the
        // expensive step. With sub-window inputs we'd produce an empty
        // result anyway; skip the work outright.
        guard samples.count >= samplesPerWindow else {
            return ([], initialState, nil)
        }

        var windows: [FeatureWindow] = []
        var rawSpeakerChangeProxyScores: [Double] = []
        var offset = 0
        var state = initialState
        let musicTimeline = musicProbabilityTimelineBuilder(samples, config.sampleRate)?
            .offset(by: shardStartTime)

        while offset + samplesPerWindow <= samples.count {
            let windowSamples = Array(samples[offset..<(offset + samplesPerWindow)])
            let windowStartTime = shardStartTime + Double(offset) / config.sampleRate
            let windowEndTime = windowStartTime + config.windowDuration

            // RMS energy
            let rms = computeRMS(windowSamples)

            // Spectral magnitudes for flux calculation
            let magnitudes = computeMagnitudeSpectrum(windowSamples)
            let priorMagnitudes = state.previousMagnitudes

            // Spectral flux (difference from previous window)
            let flux: Float
            if let prev = priorMagnitudes {
                flux = computeSpectralFlux(current: magnitudes, previous: prev)
            } else {
                flux = 0
            }

            // Pause probability — high when RMS is below threshold
            let pauseProb = computePauseProbability(rms: rms)
            let acousticMusicProb = FeatureSignalExtraction.acousticMusicProbability(
                magnitudes: magnitudes,
                rms: rms,
                spectralFlux: flux
            )
            let musicProb = FeatureSignalExtraction.musicProbability(
                acousticProbability: acousticMusicProb,
                timeline: musicTimeline,
                windowStartTime: windowStartTime,
                windowEndTime: windowEndTime
            )
            let speakerProxy = FeatureSignalExtraction.speakerChangeProxyScore(
                currentRms: Double(rms),
                previousRms: state.previousRms,
                currentMagnitudes: magnitudes,
                previousMagnitudes: priorMagnitudes,
                pauseProbability: Double(pauseProb),
                spectralFlux: Double(flux)
            )
            let musicClassification = MusicBedClassifier.classify(
                musicProbability: musicProb,
                previousMusicProbability: state.previousMusicProbability,
                rms: Double(rms),
                localMeanRms: state.runningMeanRms,
                spectralFlux: Double(flux)
            )
            state.previousMagnitudes = magnitudes
            state.previousMusicProbability = musicProb
            state.previousRms = Double(rms)
            state.updateRunningMeanRms(Double(rms))

            let window = FeatureWindow(
                analysisAssetId: analysisAssetId,
                startTime: windowStartTime,
                endTime: windowEndTime,
                rms: Double(rms),
                spectralFlux: Double(flux),
                musicProbability: musicProb,
                speakerChangeProxyScore: speakerProxy,
                musicBedChangeScore: musicClassification.changeScore,
                musicBedOnsetScore: musicClassification.onsetScore,
                musicBedOffsetScore: musicClassification.offsetScore,
                musicBedLevel: musicClassification.level,
                pauseProbability: Double(pauseProb),
                speakerClusterId: nil, // Deferred
                jingleHash: nil, // Deferred
                featureVersion: config.featureVersion
            )
            windows.append(window)
            rawSpeakerChangeProxyScores.append(speakerProxy)

            offset += hopSize
        }

        let priorWindowUpdate: PriorWindowSmoothingUpdate?
        if
            let firstRawScore = rawSpeakerChangeProxyScores.first,
            let priorRawScore = initialState.lastRawSpeakerChangeProxyScore,
            let priorWindowStart = initialState.lastWindowStartTime,
            let priorWindowEnd = initialState.lastWindowEndTime
        {
            priorWindowUpdate = PriorWindowSmoothingUpdate(
                startTime: priorWindowStart,
                endTime: priorWindowEnd,
                speakerChangeProxyScore: FeatureSignalExtraction.smoothedSpeakerChangeProxyScore(
                    current: priorRawScore,
                    previous: initialState.penultimateRawSpeakerChangeProxyScore,
                    next: firstRawScore
                )
            )
        } else {
            priorWindowUpdate = nil
        }

        guard !windows.isEmpty else { return (windows, state, priorWindowUpdate) }

        let smoothedSpeakerChangeProxyScores = FeatureSignalExtraction.smoothSpeakerChangeProxyScores(
            rawSpeakerChangeProxyScores,
            leadingPreviousRawScore: initialState.lastRawSpeakerChangeProxyScore
        )
        guard smoothedSpeakerChangeProxyScores.count == windows.count else {
            return (windows, state, priorWindowUpdate)
        }

        let smoothedWindows = windows.enumerated().map { index, window in
            rebuildWindow(window, speakerChangeProxyScore: smoothedSpeakerChangeProxyScores[index])
        }

        let trailingRawScores =
            [initialState.penultimateRawSpeakerChangeProxyScore, initialState.lastRawSpeakerChangeProxyScore]
            .compactMap { $0 } + rawSpeakerChangeProxyScores
        if let lastRaw = trailingRawScores.last {
            state.lastRawSpeakerChangeProxyScore = lastRaw
        }
        if trailingRawScores.count >= 2 {
            state.penultimateRawSpeakerChangeProxyScore = trailingRawScores[trailingRawScores.count - 2]
        } else {
            state.penultimateRawSpeakerChangeProxyScore = nil
        }
        if let lastWindow = smoothedWindows.last {
            state.lastWindowStartTime = lastWindow.startTime
            state.lastWindowEndTime = lastWindow.endTime
        }

        return (smoothedWindows, state, priorWindowUpdate)
    }

    private func rebuildWindow(
        _ window: FeatureWindow,
        speakerChangeProxyScore: Double
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: window.analysisAssetId,
            startTime: window.startTime,
            endTime: window.endTime,
            rms: window.rms,
            spectralFlux: window.spectralFlux,
            musicProbability: window.musicProbability,
            speakerChangeProxyScore: speakerChangeProxyScore,
            musicBedChangeScore: window.musicBedChangeScore,
            musicBedOnsetScore: window.musicBedOnsetScore,
            musicBedOffsetScore: window.musicBedOffsetScore,
            musicBedLevel: window.musicBedLevel,
            pauseProbability: window.pauseProbability,
            speakerClusterId: window.speakerClusterId,
            jingleHash: window.jingleHash,
            featureVersion: window.featureVersion
        )
    }

    private func loadExtractionState(
        analysisAssetId: String,
        coverageEndTime: Double
    ) async throws -> ExtractionState {
        guard coverageEndTime > 0 else { return ExtractionState() }
        guard let checkpoint = try await store.fetchFeatureExtractionCheckpoint(
            assetId: analysisAssetId,
            featureVersion: config.featureVersion,
            endingAt: coverageEndTime
        ) else {
            return ExtractionState()
        }
        var state = ExtractionState(checkpoint: checkpoint)

        // Pre-seed runningMeanRms from persisted feature windows so that
        // amplitude-relative classification (amplitudeIsHigh) works correctly
        // immediately after checkpoint resume, rather than starting from zero.
        let recentWindows = try await store.fetchFeatureWindows(
            assetId: analysisAssetId,
            from: 0,
            to: coverageEndTime,
            minimumFeatureVersion: config.featureVersion
        )
        for window in recentWindows {
            state.updateRunningMeanRms(window.rms)
        }

        return state
    }

    private func makeExtractionWindowInput(
        for shard: AnalysisShard,
        effectiveCoverage: Double
    ) -> ExtractionWindowInput {
        guard effectiveCoverage > shard.startTime else {
            return ExtractionWindowInput(samples: shard.samples, startTime: shard.startTime)
        }

        let secondsToSkip = min(shard.duration, max(0, effectiveCoverage - shard.startTime))
        let samplesToSkip = min(
            shard.samples.count,
            max(0, Int((secondsToSkip * config.sampleRate).rounded(.down)))
        )
        return ExtractionWindowInput(
            samples: Array(shard.samples.dropFirst(samplesToSkip)),
            startTime: shard.startTime + Double(samplesToSkip) / config.sampleRate
        )
    }

    private func makeCheckpoint(
        analysisAssetId: String,
        state: ExtractionState
    ) -> FeatureExtractionCheckpoint? {
        guard
            let lastWindowStartTime = state.lastWindowStartTime,
            let lastWindowEndTime = state.lastWindowEndTime,
            let lastRms = state.previousRms,
            let lastMusicProbability = state.previousMusicProbability,
            let lastRawSpeakerChangeProxyScore = state.lastRawSpeakerChangeProxyScore,
            let lastMagnitudes = state.previousMagnitudes
        else {
            return nil
        }

        return FeatureExtractionCheckpoint(
            analysisAssetId: analysisAssetId,
            lastWindowStartTime: lastWindowStartTime,
            lastWindowEndTime: lastWindowEndTime,
            lastRms: lastRms,
            lastMusicProbability: lastMusicProbability,
            lastRawSpeakerChangeProxyScore: lastRawSpeakerChangeProxyScore,
            penultimateRawSpeakerChangeProxyScore: state.penultimateRawSpeakerChangeProxyScore,
            lastMagnitudes: lastMagnitudes,
            featureVersion: config.featureVersion
        )
    }

    private func repairCoverageForCurrentFeatureVersion(
        analysisAssetId: String,
        existingCoverage: Double
    ) async throws -> Double {
        guard existingCoverage > 0 else { return existingCoverage }

        guard let earliestStaleWindowStart = try await store.earliestFeatureWindowStart(
            assetId: analysisAssetId,
            before: existingCoverage,
            earlierThanFeatureVersion: config.featureVersion
        ) else {
            return existingCoverage
        }

        try await store.updateFeatureCoverage(
            id: analysisAssetId,
            endTime: earliestStaleWindowStart
        )
        return earliestStaleWindowStart
    }

    // MARK: - DSP: RMS Energy

    /// Compute root mean square energy using vDSP.
    private func computeRMS(_ samples: [Float]) -> Float {
        var rms: Float = 0
        vDSP_rmsqv(samples, 1, &rms, vDSP_Length(samples.count))
        return rms
    }

    // MARK: - DSP: Magnitude Spectrum

    /// Compute magnitude spectrum using vDSP FFT.
    /// Returns the magnitude of each frequency bin (half-spectrum).
    private func computeMagnitudeSpectrum(_ samples: [Float]) -> [Float] {
        guard let fftSetup else { return [] }

        let n = config.fftSize
        let halfN = n / 2

        // Prepare input — take first `n` samples or zero-pad.
        var input = [Float](repeating: 0, count: n)
        let copyCount = min(samples.count, n)
        input.replaceSubrange(0..<copyCount, with: samples[0..<copyCount])

        // Apply Hann window to reduce spectral leakage.
        var window = [Float](repeating: 0, count: n)
        vDSP_hann_window(&window, vDSP_Length(n), Int32(vDSP_HANN_NORM))
        vDSP_vmul(input, 1, window, 1, &input, 1, vDSP_Length(n))

        // Convert to split complex form.
        var realPart = [Float](repeating: 0, count: halfN)
        var imagPart = [Float](repeating: 0, count: halfN)
        var magnitudes = [Float](repeating: 0, count: halfN)

        realPart.withUnsafeMutableBufferPointer { realBuf in
            imagPart.withUnsafeMutableBufferPointer { imagBuf in
                var splitComplex = DSPSplitComplex(
                    realp: realBuf.baseAddress!,
                    imagp: imagBuf.baseAddress!
                )
                input.withUnsafeBufferPointer { inputBuf in
                    inputBuf.baseAddress!.withMemoryRebound(
                        to: DSPComplex.self, capacity: halfN
                    ) { complexPtr in
                        vDSP_ctoz(complexPtr, 2, &splitComplex, 1, vDSP_Length(halfN))
                    }
                }

                // Forward FFT.
                fftSetup.forward(input: splitComplex, output: &splitComplex)

                // Compute magnitudes into a local array — don't write back through the split complex buffers.
                vDSP_zvabs(&splitComplex, 1, &magnitudes, 1, vDSP_Length(halfN))
            }
        }

        return magnitudes
    }

    // MARK: - DSP: Spectral Flux

    /// Spectral flux: sum of positive differences in magnitude spectrum between
    /// consecutive windows. Captures onset events (music stingers, transitions).
    private func computeSpectralFlux(current: [Float], previous: [Float]) -> Float {
        let count = min(current.count, previous.count)
        guard count > 0 else { return 0 }

        // diff = current - previous
        var diff = [Float](repeating: 0, count: count)
        vDSP_vsub(previous, 1, current, 1, &diff, 1, vDSP_Length(count))

        // Half-wave rectify: keep only positive differences (onsets, not offsets).
        var zero: Float = 0
        vDSP_vthres(diff, 1, &zero, &diff, 1, vDSP_Length(count))

        // Sum the rectified differences.
        var sum: Float = 0
        vDSP_sve(diff, 1, &sum, vDSP_Length(count))

        // Normalize by bin count for comparability across FFT sizes.
        return sum / Float(count)
    }

    // MARK: - DSP: Pause Probability

    /// Simple pause detector based on RMS energy.
    /// Returns a probability in [0, 1] — 1.0 = definite pause, 0.0 = loud audio.
    ///
    /// Uses a smooth, monotonic curve anchored on `pauseRmsThreshold`:
    ///   rms ≤ threshold/4  → 1.0 (definite silence / room tone)
    ///   rms == threshold    → 0.5 (boundary between pause and speech)
    ///   rms ≥ threshold*4   → 0.0 (definite speech/music)
    /// Linear interpolation in log-RMS space keeps the curve well-behaved
    /// across the wide dynamic range of real podcast audio.
    ///
    /// Observed behaviour on the Conan verification episode: **~0.020 mean**
    /// pauseProbability across the full transcript. An earlier design target
    /// of 0.05-0.15 was aspirational and is not achieved on real podcast
    /// audio with this curve — most windows contain speech, and the log-RMS
    /// curve correctly reports low pause probabilities for them. Future
    /// tuning (Phase 3+) may widen the window or add hysteresis if the
    /// downstream break detector needs higher recall on inter-ad gaps.
    private func computePauseProbability(rms: Float) -> Float {
        guard rms > 0 else { return 1.0 }
        let threshold = config.pauseRmsThreshold
        guard threshold > 0 else { return 0 }

        // Map log2(rms/threshold) from [-2, +2] onto [1, 0].
        let ratio = log2(rms / threshold)
        let t = (ratio + 2.0) / 4.0  // 0 at ratio=-2, 1 at ratio=+2
        return max(0, min(1, 1.0 - t))
    }
}
