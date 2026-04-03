// FeatureExtraction.swift
// On-device audio feature extraction for the ad detection hot path.
//
// Extracts per-window acoustic features from decoded analysis shards:
//   - RMS energy (volume level)
//   - Spectral flux (timbral change — music beds, stingers)
//   - Pause probability (silence/low-energy detection)
//   - Music probability (placeholder — requires ML model, stubbed to 0)
//   - Speaker cluster ID (deferred — requires embeddings)
//   - Jingle hash (deferred — requires fingerprint database)
//
// Uses Accelerate framework (vDSP, vForce) for all DSP. Runs faster than
// real-time on target devices. Feature windows are persisted to SQLite via
// AnalysisStore and never recomputed.

import Accelerate
import Foundation

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
        pauseRmsThreshold: 0.005,
        featureVersion: 1
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

// MARK: - FeatureExtractionService

/// Extracts acoustic features from analysis shards and persists them to the
/// AnalysisStore. Designed for both hot-path (near-real-time during playback)
/// and backfill (batch processing of downloaded episodes).
///
/// Thread-safe via actor isolation. All DSP runs on the actor's executor,
/// which is a cooperative thread pool — no dedicated queue needed.
actor FeatureExtractionService {

    private let config: FeatureExtractionConfig
    private let store: AnalysisStore

    // Reusable FFT setup — allocated once, used for every window.
    private let fftSetup: vDSP.FFT<DSPSplitComplex>?
    private let fftLog2n: vDSP_Length

    init(store: AnalysisStore, config: FeatureExtractionConfig = .default) {
        self.config = config
        self.store = store

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
    /// - Parameters:
    ///   - shards: Decoded audio shards from AnalysisAudioService.
    ///   - analysisAssetId: The analysis asset these shards belong to.
    ///   - existingCoverage: End time already covered (skip windows before this).
    /// - Returns: Array of extracted FeatureWindow records.
    @discardableResult
    func extractAndPersist(
        shards: [AnalysisShard],
        analysisAssetId: String,
        existingCoverage: Double = 0
    ) async throws -> [FeatureWindow] {
        guard !shards.isEmpty else {
            throw FeatureExtractionError.emptyInput
        }

        var allWindows: [FeatureWindow] = []

        for shard in shards {
            try Task.checkCancellation()

            // Skip shards fully covered by prior extraction.
            let shardEnd = shard.startTime + shard.duration
            if shardEnd <= existingCoverage { continue }

            let windows = extractWindows(
                from: shard.samples,
                shardStartTime: shard.startTime,
                analysisAssetId: analysisAssetId
            )

            // Filter out windows that fall within already-covered range.
            let newWindows = windows.filter { $0.startTime >= existingCoverage }
            guard !newWindows.isEmpty else { continue }

            // Persist batch to SQLite.
            try await store.insertFeatureWindows(newWindows)

            // Update coverage watermark.
            if let lastWindow = newWindows.last {
                try await store.updateFeatureCoverage(
                    id: analysisAssetId,
                    endTime: lastWindow.endTime
                )
            }

            allWindows.append(contentsOf: newWindows)
        }

        return allWindows
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
            analysisAssetId: analysisAssetId
        )
    }

    // MARK: - Window Extraction

    /// Slice samples into windows and compute features for each.
    private func extractWindows(
        from samples: [Float],
        shardStartTime: TimeInterval,
        analysisAssetId: String
    ) -> [FeatureWindow] {
        let samplesPerWindow = Int(config.windowDuration * config.sampleRate)
        let hopSize = Int(Double(samplesPerWindow) * (1.0 - config.overlapFraction))
        guard hopSize > 0, samplesPerWindow > 0 else { return [] }

        var windows: [FeatureWindow] = []
        var offset = 0
        var previousMagnitudes: [Float]?

        while offset + samplesPerWindow <= samples.count {
            let windowSamples = Array(samples[offset..<(offset + samplesPerWindow)])
            let windowStartTime = shardStartTime + Double(offset) / config.sampleRate
            let windowEndTime = windowStartTime + config.windowDuration

            // RMS energy
            let rms = computeRMS(windowSamples)

            // Spectral magnitudes for flux calculation
            let magnitudes = computeMagnitudeSpectrum(windowSamples)

            // Spectral flux (difference from previous window)
            let flux: Float
            if let prev = previousMagnitudes {
                flux = computeSpectralFlux(current: magnitudes, previous: prev)
            } else {
                flux = 0
            }
            previousMagnitudes = magnitudes

            // Pause probability — high when RMS is below threshold
            let pauseProb = computePauseProbability(rms: rms)

            let window = FeatureWindow(
                analysisAssetId: analysisAssetId,
                startTime: windowStartTime,
                endTime: windowEndTime,
                rms: Double(rms),
                spectralFlux: Double(flux),
                musicProbability: 0, // Requires ML model — deferred
                pauseProbability: Double(pauseProb),
                speakerClusterId: nil, // Deferred
                jingleHash: nil, // Deferred
                featureVersion: config.featureVersion
            )
            windows.append(window)

            offset += hopSize
        }

        return windows
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
    private func computePauseProbability(rms: Float) -> Float {
        guard rms >= 0 else { return 1.0 }

        let threshold = config.pauseRmsThreshold
        if rms < threshold * 0.5 {
            // Well below threshold — very likely a pause.
            return 1.0
        } else if rms < threshold {
            // Near threshold — interpolate linearly.
            let t = (rms - threshold * 0.5) / (threshold * 0.5)
            return 1.0 - t
        } else if rms < threshold * 2.0 {
            // Above threshold but still quiet — low probability.
            let t = (rms - threshold) / threshold
            return max(0, 0.3 * (1.0 - t))
        } else {
            return 0
        }
    }
}
