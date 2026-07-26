// AcousticFingerprint.swift
// playhead-gtt9.13: Compact on-device acoustic fingerprint used by
// `AdCatalogStore` to cross-match ad spans across episodes.
//
// Design goals
// ------------
// * Pure value type — easy to ship across actor boundaries and to serialize.
// * Fixed vector length (`vectorLength = 64`) with explicit, versioned layout
//   and comparison semantics.
// * Deterministic generation from `[Float]` PCM so two spans of identical
//   audio produce identical fingerprints (testable, cacheable).
// * On-device compute only — no network, no cloud (legal mandate).
//
// Why a mel-style summary vector (not MFCC, not a neural embedding)
// ----------------------------------------------------------------
// The MVP similarity job is "was this ad run already fingerprinted from an
// earlier episode?". Ads that recur tend to be byte-identical reads of the
// same creative — a mel-energy-ish summary plus a short "zero-cross rate"
// tail is enough to discriminate. A neural embedding would be stronger but
// would add a model dependency for no precision the corpus actually needs
// today. gtt9.12 owns richer acoustic features; if it produces a better
// vector, the store schema stores a `Data` blob and can migrate.

import Accelerate
import Foundation

/// Compatibility boundary for persisted acoustic vectors.
///
/// The original catalog stored only a 256-byte blob and implicitly compared
/// every vector with cosine, even when the vector's semantics were unknown.
/// Versions are therefore about both the vector layout and the admission
/// metric; callers must never compare different versions.
enum CatalogFingerprintVersion: Int, Sendable, Hashable, Codable {
    /// Pre-playhead-o4qr rows. Their 64 floats are retained for audit and
    /// migration, but are never admitted by the current matcher.
    case legacyCosineV1 = 1
    /// Eight FeatureWindow streams × eight summary statistics, compared with
    /// the calibrated relative-distance score.
    case relativeFeatureSummaryV2 = 2
    /// PCM spectral-summary layout. It is intentionally distinct from the
    /// FeatureWindow layout even though both contain 64 floats.
    case relativePCMSummaryV1 = 3

    static let currentCatalog: CatalogFingerprintVersion = .relativeFeatureSummaryV2
}

// MARK: - AcousticFingerprint

/// Compact fixed-length acoustic fingerprint of an ad span.
///
/// The vector is normalized to unit length for stable serialization. Its
/// version selects the compatible comparison metric; different versions
/// always fail closed.
///
/// The public API is intentionally thin:
///   * `init(values:)` validates length and normalizes.
///   * `similarity(_:_:)` computes version-compatible similarity.
///   * `fromPCM(_:sampleRate:)` derives a fingerprint from raw mono PCM.
struct AcousticFingerprint: Sendable, Hashable, Codable {

    /// Fixed vector length. 64 floats is ~256 bytes — compact enough to
    /// store thousands of entries per user with no meaningful disk cost,
    /// large enough to carry mel-band envelope + a few scalar summary
    /// descriptors without saturating.
    static let vectorLength: Int = 64

    /// Normalized, non-negative feature values. Length always equals
    /// `vectorLength`; constructor guards this.
    let values: [Float]
    /// Layout + comparison semantics for `values`.
    let version: CatalogFingerprintVersion

    /// Construct from a non-negative feature vector. The input is padded
    /// or truncated to `vectorLength`, then L2-normalized for stable scale.
    ///
    /// Returns `nil` if any input element is negative — the
    /// `similarity(_:_:)` contract assumes finite, non-negative fingerprints,
    /// and accepting other values would silently invalidate that. Zero-length
    /// or all-zero inputs are
    /// valid and produce a canonical zero fingerprint that compares
    /// similarity 0 against everything (including itself).
    init?(
        values: [Float],
        version: CatalogFingerprintVersion = .currentCatalog
    ) {
        for v in values where !v.isFinite || v < 0 { return nil }

        let clipped: [Float]
        if values.count >= Self.vectorLength {
            clipped = Array(values.prefix(Self.vectorLength))
        } else {
            clipped = values + [Float](repeating: 0, count: Self.vectorLength - values.count)
        }

        // L2 norm.
        var sumSq: Float = 0
        for v in clipped { sumSq += v * v }
        let norm = sqrtf(sumSq)
        guard norm.isFinite else { return nil }
        if norm <= .ulpOfOne {
            self.values = [Float](repeating: 0, count: Self.vectorLength)
        } else {
            var out = clipped
            let inv = 1.0 / norm
            for i in 0..<out.count { out[i] = out[i] * inv }
            self.values = out
        }
        self.version = version
    }

    /// True iff the fingerprint is the canonical zero fingerprint
    /// (constructed from an empty or all-zero input). Zero fingerprints
    /// should never match — callers can filter them out.
    var isZero: Bool {
        for v in values where v != 0 { return false }
        return true
    }

    /// Canonical zero fingerprint, equivalent to `AcousticFingerprint(values: [])!`.
    /// Used by internal builders that want to bail out without forcing
    /// the failable init's unwrap at every site.
    static var zero: AcousticFingerprint {
        zero(version: .currentCatalog)
    }

    private static func zero(
        version: CatalogFingerprintVersion
    ) -> AcousticFingerprint {
        AcousticFingerprint(
            rawNormalizedValues: [Float](repeating: 0, count: vectorLength),
            version: version
        )
    }

    // MARK: - Serialization

    /// Encode to a little-endian `Data` blob for SQLite storage.
    var data: Data {
        var out = Data()
        out.reserveCapacity(values.count * MemoryLayout<Float>.size)
        for v in values {
            var bits = v.bitPattern.littleEndian
            withUnsafeBytes(of: &bits) { out.append(contentsOf: $0) }
        }
        return out
    }

    /// Decode from the `data` representation. Returns nil on wrong length.
    init?(
        data: Data,
        version: CatalogFingerprintVersion = .currentCatalog
    ) {
        let stride = MemoryLayout<Float>.size
        guard data.count == Self.vectorLength * stride else { return nil }
        // playhead-rfu-aac (cycle-3 L1): the encoder emits explicit
        // `bits.littleEndian` UInt32 bit-patterns — round-trip symmetrically
        // by reading UInt32 LE and reconstructing each Float via
        // `Float(bitPattern:)`. A naive `bindMemory(to: Float.self)` would
        // be host-endianness-dependent and silently break if this code
        // were ever ported off little-endian Apple silicon.
        let vs: [Float] = data.withUnsafeBytes { rawBuf -> [Float] in
            var out: [Float] = []
            out.reserveCapacity(Self.vectorLength)
            for i in 0..<Self.vectorLength {
                // `Data` does not promise UInt32 alignment (a slice may start
                // at any byte offset), so decode without rebinding its storage.
                let bits = UInt32(
                    littleEndian: rawBuf.loadUnaligned(
                        fromByteOffset: i * stride,
                        as: UInt32.self
                    )
                )
                out.append(Float(bitPattern: bits))
            }
            return out
        }
        guard vs.count == Self.vectorLength,
              vs.allSatisfy({ $0.isFinite && $0 >= 0 }) else {
            return nil
        }
        // Persisted blobs bypass `init(values:)`, so validate the invariant
        // that constructor establishes. Otherwise a corrupted but finite
        // vector can overflow the relative-distance calculation or silently
        // participate with an arbitrary scale.
        let sumSquared = vs.reduce(into: 0.0) { partial, value in
            partial += Double(value) * Double(value)
        }
        guard sumSquared == 0
                || (sumSquared.isFinite && abs(sumSquared - 1) <= 0.001)
        else {
            return nil
        }
        // Already normalized when we wrote; reconstruct without renormalizing
        // by injecting straight into a bypass initializer.
        self = AcousticFingerprint(
            rawNormalizedValues: vs,
            version: version
        )
    }

    /// Internal: construct from values that are already L2-normalized.
    /// Used by `init?(data:)` to avoid touching the norm a second time.
    private init(
        rawNormalizedValues vs: [Float],
        version: CatalogFingerprintVersion
    ) {
        precondition(vs.count == Self.vectorLength)
        self.values = vs
        self.version = version
    }

    private enum CodingKeys: String, CodingKey {
        case values
        case version
    }

    /// JSON/plist payloads written before the catalog fingerprint boundary was
    /// explicit contain only `values`. Preserve their bytes for audit, but
    /// classify them as legacy cosine vectors so they can never be compared
    /// with the current FeatureWindow cohort.
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let decodedValues = try container.decode([Float].self, forKey: .values)
        let decodedVersion = try container.decodeIfPresent(
            CatalogFingerprintVersion.self,
            forKey: .version
        ) ?? .legacyCosineV1
        let sumSquared = decodedValues.reduce(into: 0.0) {
            partial,
            value in
            partial += Double(value) * Double(value)
        }
        guard decodedValues.count == Self.vectorLength,
              decodedValues.allSatisfy({ $0.isFinite && $0 >= 0 }),
              sumSquared == 0
                || (
                    sumSquared.isFinite
                        && abs(sumSquared - 1) <= 0.001
                ) else {
            throw DecodingError.dataCorruptedError(
                forKey: .values,
                in: container,
                debugDescription:
                    "Acoustic fingerprint values must be a finite, non-negative, normalized 64-value vector"
            )
        }
        self = AcousticFingerprint(
            rawNormalizedValues: decodedValues,
            version: decodedVersion
        )
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(values, forKey: .values)
        try container.encode(version, forKey: .version)
    }

    // MARK: - Similarity

    /// Version-compatible similarity in `[0, 1]`.
    ///
    /// Current FeatureWindow fingerprints use an interpretable mean relative
    /// distance: each dimension contributes `|a-b| / (a+b+epsilon)`, then the
    /// mean is mapped to similarity with `exp(-4d)`. The multiplier is part of
    /// the versioned semantics, calibrated against the committed 50-row
    /// Catalyst fixture: all 1,225 real negative cross-pairs fall below 0.90
    /// while exact recurrences and small boundary shifts remain above it.
    ///
    /// Mismatched versions fail closed with zero similarity.
    static func similarity(
        _ a: AcousticFingerprint,
        _ b: AcousticFingerprint
    ) -> Float {
        if a.isZero || b.isZero { return 0 }
        guard a.version == b.version else { return 0 }
        switch a.version {
        case .legacyCosineV1:
            return legacyCosineSimilarity(a, b)
        case .relativeFeatureSummaryV2, .relativePCMSummaryV1:
            var relativeDistance: Float = 0
            for i in 0..<vectorLength {
                let lhs = a.values[i]
                let rhs = b.values[i]
                relativeDistance += abs(lhs - rhs) / (lhs + rhs + 0.000_001)
            }
            relativeDistance /= Float(vectorLength)
            let similarity = expf(-4 * relativeDistance)
            guard similarity.isFinite else { return 0 }
            return min(1, max(0, similarity))
        }
    }

    /// Legacy cosine retained only for deterministic migration/calibration
    /// tests and diagnostics. Production admission goes through
    /// `similarity(_:_:)`, which enforces the version boundary.
    static func legacyCosineSimilarity(
        _ a: AcousticFingerprint,
        _ b: AcousticFingerprint
    ) -> Float {
        if a.isZero || b.isZero { return 0 }
        var dot: Float = 0
        // Manual loop outperforms vDSP_dotpr for n=64 by avoiding the
        // symbol lookup for negligible inputs; kept simple and in-line.
        for i in 0..<vectorLength {
            dot += a.values[i] * b.values[i]
        }
        assert(dot >= -.ulpOfOne, "AcousticFingerprint.similarity dot=\(dot) negative — non-negative invariant broken")
        // Clamp to guard against tiny FP drift above 1.0 / below 0.0.
        if dot > 1.0 { return 1.0 }
        if dot < 0.0 { return 0.0 }
        return dot
    }

    // MARK: - PCM → fingerprint

    /// Derive a fingerprint from mono PCM at an arbitrary sample rate.
    ///
    /// The vector is built as:
    ///   * 60 mel-ish band energies (log-compressed, non-negative)
    ///   * 1  zero-crossing rate
    ///   * 1  RMS energy
    ///   * 1  spectral centroid (normalized 0..1)
    ///   * 1  spectral flatness (0..1)
    ///
    /// For span durations below ~0.25s this returns a zero fingerprint
    /// (too short to be a reliable match). Callers should check `isZero`.
    static func fromPCM(_ pcm: [Float], sampleRate: Double) -> AcousticFingerprint {
        // Compare in `Double` instead of converting an untrusted rate to
        // `Int`: `Int(.infinity)` and out-of-range finite values trap.
        guard sampleRate.isFinite,
              sampleRate > 0,
              Double(pcm.count) >= sampleRate * 0.25 else {
            return AcousticFingerprint.zero(version: .relativePCMSummaryV1)
        }

        let bandCount = 60
        var bands = [Float](repeating: 0, count: bandCount)

        // Chunked STFT with 512-sample window, 256 hop. Not a perfect mel
        // filterbank — we approximate by log-spacing the FFT bin groups
        // into `bandCount` bands. Cheap, deterministic, sufficient.
        let windowSize = 512
        let hopSize = 256
        guard pcm.count >= windowSize else {
            return AcousticFingerprint.zero(version: .relativePCMSummaryV1)
        }

        let binCount = windowSize / 2
        // Precompute log-spaced band edges across the positive FFT bins.
        var bandEdges = [Int](repeating: 0, count: bandCount + 1)
        for i in 0...bandCount {
            let frac = Double(i) / Double(bandCount)
            // Log mapping: bin = floor(binCount * (2^(frac*log2(binCount+1)) - 1) / binCount)
            // Simpler monotone log: binCount * (exp(frac * ln(binCount)) - 1) / (binCount - 1)
            let logIdx = exp(frac * log(Double(binCount))) - 1.0
            let clamped = max(0, min(binCount - 1, Int(logIdx)))
            bandEdges[i] = clamped
        }

        var frameCount = 0
        var zcrSum: Float = 0
        var rmsSum: Float = 0
        var centroidSum: Float = 0
        var flatnessSum: Float = 0

        // Hann window table.
        var hann = [Float](repeating: 0, count: windowSize)
        for i in 0..<windowSize {
            hann[i] = 0.5 - 0.5 * cosf(2 * .pi * Float(i) / Float(windowSize - 1))
        }

        // Set up a single vDSP DFT plan for the whole episode. The plan is
        // reusable across frames and converts the per-frame work from the
        // pre-fix O(windowSize²) hand-rolled DFT (≈ 246M trig calls for a
        // 30s ad at 16 kHz) to the highly-optimized vDSP path.
        guard let dftSetup = vDSP_DFT_zop_CreateSetup(
            nil,
            vDSP_Length(windowSize),
            .FORWARD
        ) else {
            return AcousticFingerprint.zero(version: .relativePCMSummaryV1)
        }
        defer { vDSP_DFT_DestroySetup(dftSetup) }

        // Per-frame scratch buffers. Imag input is zero-filled (real input).
        var realIn = [Float](repeating: 0, count: windowSize)
        let imagIn = [Float](repeating: 0, count: windowSize)
        var realOut = [Float](repeating: 0, count: windowSize)
        var imagOut = [Float](repeating: 0, count: windowSize)

        var offset = 0
        while offset + windowSize <= pcm.count {
            var zc: Int = 0
            var rms: Float = 0
            for i in 0..<windowSize {
                let s = pcm[offset + i]
                realIn[i] = s * hann[i]
                rms += s * s
                if i > 0 {
                    let prev = pcm[offset + i - 1]
                    if (prev >= 0) != (s >= 0) { zc += 1 }
                }
            }
            rms = sqrtf(rms / Float(windowSize))

            // Compute magnitude spectrum via vDSP forward DFT. Replaces the
            // naive O(windowSize²) hand-rolled DFT — ~50× faster on device.
            vDSP_DFT_Execute(dftSetup, realIn, imagIn, &realOut, &imagOut)

            var mags = [Float](repeating: 0, count: binCount)
            var magSum: Float = 0
            var logMagSum: Float = 0
            var weightedBinSum: Float = 0
            for k in 0..<binCount {
                let re = realOut[k]
                let im = imagOut[k]
                let m = sqrtf(re * re + im * im)
                mags[k] = m
                magSum += m
                logMagSum += logf(max(m, 1e-9))
                weightedBinSum += m * Float(k)
            }

            // Accumulate per-band log energy.
            for b in 0..<bandCount {
                let lo = bandEdges[b]
                let hi = max(lo + 1, bandEdges[b + 1])
                var e: Float = 0
                for k in lo..<hi { e += mags[k] }
                bands[b] += logf(1 + e)
            }

            // Scalars.
            zcrSum += Float(zc) / Float(windowSize)
            rmsSum += rms
            if magSum > 1e-9 {
                centroidSum += (weightedBinSum / magSum) / Float(binCount)
                // Flatness = geomean / arithmean on linear magnitudes.
                let geom = expf(logMagSum / Float(binCount))
                let arith = magSum / Float(binCount)
                flatnessSum += (arith > 1e-9) ? (geom / arith) : 0
            }
            frameCount += 1
            offset += hopSize
        }

        guard frameCount > 0 else {
            return AcousticFingerprint.zero(version: .relativePCMSummaryV1)
        }

        let fc = Float(frameCount)
        for i in 0..<bandCount { bands[i] /= fc }

        var vector = bands
        vector.append(zcrSum / fc)
        vector.append(rmsSum / fc)
        vector.append(centroidSum / fc)
        vector.append(flatnessSum / fc)

        // Vector is non-negative by construction (log energies, |zcr|,
        // rms, centroid as fraction, flatness as positive ratio). The
        // `?? .zero` is belt-and-suspenders against FP edge cases.
        return AcousticFingerprint(
            values: vector,
            version: .relativePCMSummaryV1
        ) ?? AcousticFingerprint.zero(version: .relativePCMSummaryV1)
    }

    // MARK: - FeatureWindow → fingerprint (gtt9.17)

    /// Derive a fingerprint from a sequence of `FeatureWindow`s.
    ///
    /// Where `fromPCM` works at the audio layer, this constructor maps the
    /// episode-level acoustic feature stream (the same signal the
    /// `AcousticFeaturePipeline` consumes) to the catalog's fingerprint
    /// space. The main MVP call sites (`AdDetectionService.runBackfill`)
    /// already have `[FeatureWindow]` on hand and do NOT have raw PCM, so
    /// this is the cheap path into `AdCatalogStore` without re-decoding
    /// audio.
    ///
    /// Mapping
    /// -------
    /// 8 feature streams × 8 summary statistics = 64-dim vector, matching
    /// `AcousticFingerprint.vectorLength`. Deliberately TIME-INVARIANT —
    /// no window timestamps enter the summary so the same creative recurring
    /// at a different timestamp in another episode produces the same
    /// fingerprint (which is the whole point of a catalog).
    ///
    /// Feature streams:
    ///   1. rms
    ///   2. spectralFlux
    ///   3. musicProbability
    ///   4. speakerChangeProxyScore
    ///   5. musicBedChangeScore
    ///   6. musicBedOnsetScore
    ///   7. musicBedOffsetScore
    ///   8. pauseProbability
    ///
    /// Stats per stream (all bounded non-negative):
    ///   1. mean
    ///   2. max
    ///   3. min
    ///   4. population standard deviation
    ///   5. sum / N (a compatibility slot intentionally equal to mean in the
    ///      calibrated v2 layout)
    ///   6. mean of top-3 values (p90-ish)
    ///   7. mean of bottom-3 values (p10-ish)
    ///   8. active-fraction (share of windows with value > 0.5)
    ///
    /// The resulting 64-float vector is passed to `init(values:)`, which
    /// L2-normalizes and zero-guards in the usual way. Zero/empty input
    /// returns a zero fingerprint (filtered out by the catalog insert path).
    static func fromFeatureWindows(_ windows: [FeatureWindow]) -> AcousticFingerprint {
        guard !windows.isEmpty else {
            return AcousticFingerprint.zero
        }

        // Persistence is an untrusted boundary. Do not turn corrupt rows into
        // a different, apparently valid fingerprint by clamping bad values:
        // that could let malformed exact material match learned evidence.
        // Timestamps remain excluded from the vector, but they still have to
        // prove these are real windows from one analysis asset.
        let sourceAssetId = windows[0].analysisAssetId
        let currentFeatureVersion =
            FeatureExtractionConfig.default.featureVersion
        guard RecurrenceMaterialIdentity.canonicalIdentifier(sourceAssetId)
                != nil,
              windows.allSatisfy({ window in
                  window.analysisAssetId == sourceAssetId
                      && window.startTime.isFinite
                      && window.endTime.isFinite
                      && window.startTime >= 0
                      && window.endTime > window.startTime
                      && MusicDetectionConfig.supportedWindowDurations
                          .contains { duration in
                              abs(
                                  (window.endTime - window.startTime)
                                      - duration
                              ) <= 0.001
                          }
                      // Feature semantics are part of the fingerprint
                      // population. A future or legacy extraction version
                      // cannot be mixed into the calibrated v2 layout.
                      && window.featureVersion == currentFeatureVersion
                      && [window.rms, window.spectralFlux]
                          .allSatisfy { $0.isFinite && $0 >= 0 }
                      && [
                          window.musicProbability,
                          window.speakerChangeProxyScore,
                          window.musicBedChangeScore,
                          window.musicBedOnsetScore,
                          window.musicBedOffsetScore,
                          window.pauseProbability,
                      ].allSatisfy {
                          $0.isFinite && (0...1).contains($0)
                      }
              }) else {
            return AcousticFingerprint.zero
        }
        let intervals = windows
            .map { (start: $0.startTime, end: $0.endTime) }
            .sorted {
                if $0.start != $1.start {
                    return $0.start < $1.start
                }
                return $0.end < $1.end
            }
        guard zip(intervals, intervals.dropFirst()).allSatisfy({ pair in
            pair.0.end <= pair.1.start
        }) else {
            // The calibrated feature cohort is non-overlapping. Duplicate or
            // overlapping persisted rows can otherwise over-weight a subset
            // of the material's distributions and mint a different match.
            return AcousticFingerprint.zero
        }

        let streams: [[Double]] = [
            windows.map { $0.rms },
            windows.map { $0.spectralFlux },
            windows.map { $0.musicProbability },
            windows.map { $0.speakerChangeProxyScore },
            windows.map { $0.musicBedChangeScore },
            windows.map { $0.musicBedOnsetScore },
            windows.map { $0.musicBedOffsetScore },
            windows.map { $0.pauseProbability }
        ]

        var vector: [Float] = []
        vector.reserveCapacity(64)

        for stream in streams {
            guard !stream.isEmpty else {
                for _ in 0..<8 { vector.append(0) }
                continue
            }

            let n = Double(stream.count)
            let sum = stream.reduce(0, +)
            let mean = sum / n
            let maxV = stream.max() ?? 0
            let minV = stream.min() ?? 0

            var variance: Double = 0
            for v in stream {
                let d = v - mean
                variance += d * d
            }
            variance /= n
            let stddev = variance > 0 ? variance.squareRoot() : 0

            let energyScaled = sum / n

            let sorted = stream.sorted(by: >)   // descending
            let topK = min(3, sorted.count)
            var topMean: Double = 0
            for i in 0..<topK { topMean += sorted[i] }
            topMean /= Double(topK)

            let ascCount = min(3, sorted.count)
            var bottomMean: Double = 0
            for i in (sorted.count - ascCount)..<sorted.count { bottomMean += sorted[i] }
            bottomMean /= Double(ascCount)

            let activeFraction = Double(stream.filter { $0 > 0.5 }.count) / n

            vector.append(Float(mean))
            vector.append(Float(maxV))
            vector.append(Float(minV))
            vector.append(Float(stddev))
            vector.append(Float(energyScaled))
            vector.append(Float(topMean))
            vector.append(Float(bottomMean))
            vector.append(Float(activeFraction))
        }

        // Inputs and derived statistics were validated above; the
        // `?? .zero` remains a final overflow/representation guard.
        return AcousticFingerprint(
            values: vector,
            version: .relativeFeatureSummaryV2
        ) ?? .zero
    }
}

// MARK: - Debug description

extension AcousticFingerprint: CustomStringConvertible {
    var description: String {
        if isZero {
            return "AcousticFingerprint(v\(version.rawValue), zero)"
        }
        let head = values.prefix(4).map { String(format: "%.3f", $0) }.joined(separator: ",")
        return "AcousticFingerprint(v\(version.rawValue), [\(head)...], len=\(values.count))"
    }
}
