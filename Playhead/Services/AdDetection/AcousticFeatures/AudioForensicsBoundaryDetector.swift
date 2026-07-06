// AudioForensicsBoundaryDetector.swift
// playhead-xsdz.8: Composite audio-forensics boundary evidence channel.
//
// Goal
// ----
// Detect the PHYSICAL signature of audio insertion at a candidate ad boundary
// — discontinuities that ad TEXT cannot fake. An inserted ad is mastered and
// recorded separately from the host audio, so its start and end edges carry a
// step change in loudness, spectral character, noise floor, and production
// environment that a continuous host conversation does NOT exhibit. This is a
// transcript-free corroborator.
//
// Per the cross-model idea duel's explicit recommendation, the four
// sub-signals are MERGED into ONE capped evidence channel (`.audioForensics`,
// cap `FusionWeightConfig.audioForensicsCap`) — NOT three separate channels /
// caps — and the whole feature is gated by ONE OFF-by-default flag
// (`AdDetectionConfig.audioForensicsEnabled`). When that flag is off the
// producer is never called, so behaviour is byte-identical to pre-xsdz.8.
//
// HONESTY NOTE — what is computed from real signal vs. deferred
// -------------------------------------------------------------
// The backfill ledger path receives `[FeatureWindow]` only; raw audio buffers
// are NOT reachable at the ledger seam (the decoder upstream has already
// reduced audio to per-window features). So this detector computes the four
// sub-signals from the per-window features that ALREADY exist on
// `FeatureWindow`, exactly mirroring how `LufsShift` / `SpectralShift` /
// `DynamicRange` derive their signals:
//
//   • loudness / RMS jump      — `20·log10(rms)` dBFS step across the edge.
//                                REAL signal (same proxy as `LufsShift.dbfs`).
//   • spectral-character shift — `spectralFlux` step across the edge. REAL
//                                signal. (The duel framed this as a
//                                "spectral-centroid shift"; the pipeline does
//                                NOT compute a per-window spectral centroid —
//                                only per-bin spectral *flux*. Flux is the
//                                available spectral-discontinuity feature and
//                                tracks the same timbral-change phenomenon, so
//                                we use it and name it honestly. True spectral
//                                centroid / MFCC-tilt would need extra FFT work
//                                in the feature extractor — deferred, same as
//                                `SpectralShift`'s MFCC-delta TODO.)
//   • noise-floor change       — step in the LOW-energy floor (5th-percentile
//                                dBFS) of the windows on each side. REAL signal
//                                derived from `rms`.
//   • recording-environment    — step in `musicProbability` across the edge,
//                                a proxy for a production/environment change
//                                (a produced ad bed vs. dry host speech). REAL
//                                signal. True room-tone / reverb-tail analysis
//                                would need raw audio — deferred for lack of
//                                buffer access at this seam.
//
// Everything below operates on real, already-available signal. No constant
// stubs, no fabricated channel.
//
// Pure value type. Deterministic. No I/O, no async, no per-show state.

import Foundation
import OSLog

// MARK: - AudioForensicsBoundaryDetector

struct AudioForensicsBoundaryDetector: Sendable {

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "AudioForensicsBoundaryDetector"
    )

    // MARK: - Config (precision-first, conservative)

    struct Config: Sendable, Equatable {
        /// How many windows on each side of an edge form the "outside"
        /// (context) and "inside" (span interior) aggregates. A boundary
        /// step is the difference between the median of `edgeRadius` windows
        /// just outside the edge and `edgeRadius` windows just inside it.
        let edgeRadius: Int

        /// Sigma multiplier at which a per-sub-signal z-score maps to 1.0.
        /// A step of `saturationSigma` standard deviations (vs. the episode's
        /// own distribution of that feature) is treated as a maximal
        /// discontinuity. 3.0 ≈ "a clearly out-of-distribution jump".
        let saturationSigma: Double

        /// Minimum per-sub-signal z-score that counts as "this sub-signal
        /// fired". Sub-signals below this floor contribute nothing and are
        /// not counted in `contributingSignalCount`. 1.0σ keeps content-like
        /// jitter out of the merge.
        let signalFloorSigma: Double

        /// Minimum merged boundary score below which NO ledger entry is
        /// emitted. Keeps the channel conservative: a smooth, content-like
        /// boundary (every sub-signal near zero) produces no entry at all.
        let minBoundaryScore: Double

        /// How many of the strongest per-edge sub-signals are averaged into
        /// the per-edge merged score. Using the top-K (rather than all four)
        /// rewards a boundary where a FEW sub-signals are strong without
        /// diluting them by the sub-signals that did not fire — while still
        /// requiring corroboration across modalities for a high score.
        let mergeTopK: Int

        static let `default` = Config(
            edgeRadius: 2,
            saturationSigma: 3.0,
            signalFloorSigma: 1.0,
            minBoundaryScore: 0.15,
            mergeTopK: 2
        )

        init(
            edgeRadius: Int = 2,
            saturationSigma: Double = 3.0,
            signalFloorSigma: Double = 1.0,
            minBoundaryScore: Double = 0.15,
            mergeTopK: Int = 2
        ) {
            self.edgeRadius = edgeRadius
            self.saturationSigma = saturationSigma
            self.signalFloorSigma = signalFloorSigma
            self.minBoundaryScore = minBoundaryScore
            self.mergeTopK = mergeTopK
        }
    }

    /// Floor applied to RMS before the dBFS conversion — mirrors
    /// `LufsShift.dbfs` so the loudness sub-signal is consistent with the
    /// existing pipeline.
    private static let rmsFloor: Double = 1e-6

    /// Percentile used for the per-side noise-floor estimate. The low-energy
    /// floor of a side is its 5th-percentile dBFS (the quiet inter-word gaps),
    /// which captures room tone / background bed level independent of speech
    /// loudness.
    private static let noiseFloorPercentile: Double = 0.05

    private let config: Config

    init(config: Config = .default) {
        self.config = config
    }

    // MARK: - Public API

    /// Build the composite audio-forensics ledger entry for a span, or `[]`
    /// when no significant boundary discontinuity is measured.
    ///
    /// - Parameters:
    ///   - span: the candidate span; its start/end edges are the boundaries.
    ///   - episodeWindows: ALL feature windows for the episode (used to
    ///     compute the per-feature episode sigma for normalization). The span
    ///     interior and the surrounding context are sliced from this.
    ///   - fusionConfig: supplies `audioForensicsCap` for the emitted weight.
    /// - Returns: a single `.audioForensics` entry when the merged boundary
    ///   score clears `config.minBoundaryScore`; otherwise `[]`.
    func buildEntries(
        span: DecodedSpan,
        episodeWindows: [FeatureWindow],
        fusionConfig: FusionWeightConfig
    ) -> [EvidenceLedgerEntry] {
        // Need a non-trivial, non-flat episode to estimate sigma AND windows on
        // both sides of an edge. Too few windows / zero usable variance ⇒ no
        // honest normalization ⇒ no entry (conservative). This also covers the
        // empty-features and perfectly-flat-episode edge cases.
        guard let prep = prepared(episodeWindows: episodeWindows) else { return [] }
        let windows = prep.windows
        let stats = prep.stats

        // Score each edge (start, end) and take the stronger one — a real
        // insertion need only show its seam at one edge to be evidence, and
        // the BoundaryRefiner may have snapped the other edge into content.
        let startScore = boundaryStepScore(
            at: span.startTime,
            windows: windows,
            stats: stats
        )
        let endScore = boundaryStepScore(
            at: span.endTime,
            windows: windows,
            stats: stats
        )

        let best: BoundaryStepScore
        if endScore.merged > startScore.merged {
            best = endScore
        } else {
            best = startScore
        }

        guard best.merged >= config.minBoundaryScore else { return [] }

        let weight = min(
            best.merged * fusionConfig.audioForensicsCap,
            fusionConfig.audioForensicsCap
        )

        Self.logger.debug(
            "[xsdz.8] span=\(span.id, privacy: .public) FIRED boundaryScore=\(best.merged, privacy: .public) dominant=\(best.dominantSignal, privacy: .public) contributing=\(best.contributingCount, privacy: .public) weight=\(weight, privacy: .public)"
        )

        return [EvidenceLedgerEntry(
            source: .audioForensics,
            weight: weight,
            detail: .audioForensics(
                boundaryScore: best.merged,
                dominantSignal: best.dominantSignal,
                contributingSignalCount: best.contributingCount
            )
        )]
    }

    // MARK: - Candidate-edge scoring (playhead-xsdz.19)

    /// One candidate boundary time scored WITHOUT emitting a ledger entry.
    /// The `stepScore` is the same merged, σ-normalized boundary-discontinuity
    /// value `buildEntries` computes per edge; `SpliceSlotResolver` uses it to
    /// score acoustic-splice candidate edges. Mirrors the diagnostics carried on
    /// the `.audioForensics` ledger detail.
    struct CandidateEdgeScore: Sendable, Equatable {
        /// Merged boundary-discontinuity score in [0, 1].
        let stepScore: Double
        /// The sub-signal that contributed the most mass (`"loudnessJump"`,
        /// `"spectralShift"`, `"noiseFloor"`, `"environment"`, or `"none"`).
        let dominantSignal: String
        /// How many sub-signals cleared their per-signal σ-floor.
        let contributingSignalCount: Int
    }

    /// Score a SINGLE candidate boundary time with the exact σ-normalized
    /// edge-step math `buildEntries` uses internally, without emitting a ledger
    /// entry. Returns `nil` when the episode cannot be honestly normalized
    /// (fewer than 3 windows, or zero usable feature variance); the caller
    /// treats an unscorable edge as carrying no boundary evidence. Pure and
    /// deterministic.
    func scoreCandidateEdge(
        at time: Double,
        episodeWindows: [FeatureWindow]
    ) -> CandidateEdgeScore? {
        guard let prep = prepared(episodeWindows: episodeWindows) else { return nil }
        let score = boundaryStepScore(at: time, windows: prep.windows, stats: prep.stats)
        return CandidateEdgeScore(
            stepScore: score.merged,
            dominantSignal: score.dominantSignal,
            contributingSignalCount: score.contributingCount
        )
    }

    /// Shared normalization prep for both `buildEntries` and
    /// `scoreCandidateEdge`: the ONE place the "≥3 windows AND usable variance"
    /// contract and the time-sort live, so the exposed candidate scoring and the
    /// ledger-emitting path can never fork. Returns `nil` when the episode is too
    /// small or perfectly flat to normalize against.
    private func prepared(
        episodeWindows: [FeatureWindow]
    ) -> (windows: [FeatureWindow], stats: EpisodeFeatureStats)? {
        guard episodeWindows.count >= 3 else { return nil }
        // Sort by time so "windows just outside / inside an edge" is
        // well-defined regardless of caller ordering.
        let windows = episodeWindows.sorted { $0.startTime < $1.startTime }
        // Episode-wide sigma for each feature, used to z-score the edge steps.
        let stats = EpisodeFeatureStats(windows: windows, rmsFloor: Self.rmsFloor)
        guard stats.hasUsableVariance else { return nil }
        return (windows, stats)
    }

    // MARK: - Per-edge scoring

    /// Result of scoring a single edge: the merged [0,1] score plus
    /// diagnostics (which sub-signal dominated, how many fired).
    private struct BoundaryStepScore {
        let merged: Double
        let dominantSignal: String
        let contributingCount: Int

        static let zero = BoundaryStepScore(
            merged: 0,
            dominantSignal: "none",
            contributingCount: 0
        )
    }

    /// One labelled sub-signal contribution.
    private struct SubSignal {
        let label: String
        let score: Double
    }

    /// Compute the merged discontinuity score at a single time boundary.
    ///
    /// "Outside" = the `edgeRadius` windows immediately BEFORE `time`.
    /// "Inside"  = the `edgeRadius` windows immediately AT/AFTER `time`.
    /// Each sub-signal is the |outside − inside| step expressed in that
    /// feature's episode-sigma units, mapped to [0,1] via the σ-floor /
    /// σ-saturation ramp, then the top-K are averaged.
    private func boundaryStepScore(
        at time: Double,
        windows: [FeatureWindow],
        stats: EpisodeFeatureStats
    ) -> BoundaryStepScore {
        // Index of the first window whose start is >= the boundary time — i.e.
        // the first "inside" window. `outside` is the run ending just before.
        let firstInside = windows.firstIndex { $0.startTime >= time - 1e-9 } ?? windows.count

        let outsideLo = max(0, firstInside - config.edgeRadius)
        let outsideHi = firstInside // exclusive
        let insideLo = firstInside
        let insideHi = min(windows.count, firstInside + config.edgeRadius)

        // A boundary at the very first or very last window has no context on
        // one side. With no outside (or no inside) windows we cannot measure a
        // STEP — return zero rather than inventing one. This is the
        // single-window-span / boundary-at-episode-edge edge case.
        guard outsideHi > outsideLo, insideHi > insideLo else {
            return .zero
        }

        let outside = Array(windows[outsideLo..<outsideHi])
        let inside = Array(windows[insideLo..<insideHi])

        // --- loudness / RMS jump (dBFS step, σ-normalized) ---
        let outLoud = Self.medianDbfs(outside, floor: Self.rmsFloor)
        let inLoud = Self.medianDbfs(inside, floor: Self.rmsFloor)
        let loudness = normalizedStep(
            abs(outLoud - inLoud),
            sigma: stats.loudnessSigma
        )

        // --- spectral-character shift (flux step, σ-normalized) ---
        let outFlux = Self.median(outside.map(\.spectralFlux))
        let inFlux = Self.median(inside.map(\.spectralFlux))
        let spectral = normalizedStep(
            abs(outFlux - inFlux),
            sigma: stats.fluxSigma
        )

        // --- noise-floor change (low-energy-floor dBFS step, σ-normalized) ---
        // Decorrelated from the loudness sub-signal: a UNIFORM level change
        // (pure gain jump at the seam) shifts the 5th-percentile floor and the
        // median by the SAME amount, so a raw `|outFloor − inFloor|` step would
        // fire identically to `loudness` and double-count one physical
        // phenomenon — inflating both `contributingSignalCount` and the top-2
        // merge for what is really a single loudness event. We therefore
        // measure the floor step RELATIVE to the median (loudness) step:
        // subtracting the common-mode median shift leaves only the floor's
        // EXCESS movement — equivalently the change in the tail-to-median gap,
        // which is invariant to uniform gain. A clean inserted ad with its own
        // room tone changes the floor beyond the speech-level shift and still
        // fires; a pure level jump now contributes nothing here (it is already
        // captured by `loudness`). The residual is still a difference of dBFS
        // steps, so `loudnessSigma` (the episode dBFS spread) remains its
        // correct natural normalization scale.
        let outFloor = Self.percentileDbfs(
            outside, percentile: Self.noiseFloorPercentile, floor: Self.rmsFloor
        )
        let inFloor = Self.percentileDbfs(
            inside, percentile: Self.noiseFloorPercentile, floor: Self.rmsFloor
        )
        let floorStep = outFloor - inFloor
        let loudnessStep = outLoud - inLoud
        let noiseFloor = normalizedStep(
            abs(floorStep - loudnessStep),
            sigma: stats.loudnessSigma
        )

        // --- recording-environment / production change (music-prob step) ---
        let outEnv = Self.median(outside.map(\.musicProbability))
        let inEnv = Self.median(inside.map(\.musicProbability))
        let environment = normalizedStep(
            abs(outEnv - inEnv),
            sigma: stats.musicProbSigma
        )

        let subSignals = [
            SubSignal(label: "loudnessJump", score: loudness),
            SubSignal(label: "spectralShift", score: spectral),
            SubSignal(label: "noiseFloor", score: noiseFloor),
            SubSignal(label: "environment", score: environment),
        ]

        return merge(subSignals)
    }

    /// Merge per-sub-signal scores into one boundary score by averaging the
    /// strongest `mergeTopK`. Records the dominant sub-signal and how many
    /// cleared their σ-floor (i.e. have a non-zero mapped score).
    private func merge(_ subSignals: [SubSignal]) -> BoundaryStepScore {
        let sorted = subSignals.sorted { $0.score > $1.score }
        let contributing = subSignals.filter { $0.score > 0 }.count

        let k = max(1, min(config.mergeTopK, sorted.count))
        let topK = sorted.prefix(k)
        let merged = topK.reduce(0.0) { $0 + $1.score } / Double(k)

        let dominant = sorted.first.map { $0.score > 0 ? $0.label : "none" } ?? "none"

        return BoundaryStepScore(
            merged: clampUnit(merged),
            dominantSignal: dominant,
            contributingCount: contributing
        )
    }

    /// Map an absolute feature step (in raw feature units) to [0,1] using its
    /// episode sigma: below `signalFloorSigma` ⇒ 0, at/above `saturationSigma`
    /// ⇒ 1, linear in between. A non-positive sigma (degenerate / zero-variance
    /// feature) yields 0 — we cannot honestly normalize a flat distribution.
    private func normalizedStep(_ step: Double, sigma: Double) -> Double {
        guard sigma > 0, step.isFinite else { return 0 }
        let z = step / sigma
        guard z >= config.signalFloorSigma else { return 0 }
        let span = config.saturationSigma - config.signalFloorSigma
        guard span > 0 else { return 1 }
        return clampUnit((z - config.signalFloorSigma) / span)
    }

    // MARK: - Static numeric helpers

    static func dbfs(rms: Double, floor: Double) -> Double {
        20 * log10(max(rms, floor))
    }

    static func medianDbfs(_ windows: [FeatureWindow], floor: Double) -> Double {
        median(windows.map { dbfs(rms: $0.rms, floor: floor) })
    }

    /// The `percentile`-th (0…1) dBFS value of a side — the low-energy floor.
    static func percentileDbfs(
        _ windows: [FeatureWindow],
        percentile: Double,
        floor: Double
    ) -> Double {
        let values = windows.map { dbfs(rms: $0.rms, floor: floor) }.sorted()
        guard !values.isEmpty else { return 0 }
        let clamped = max(0, min(1, percentile))
        let idx = Int((Double(values.count - 1) * clamped).rounded())
        return values[idx]
    }

    static func median(_ values: [Double]) -> Double {
        let sorted = values.sorted()
        guard !sorted.isEmpty else { return 0 }
        let mid = sorted.count / 2
        if sorted.count.isMultiple(of: 2) {
            return (sorted[mid - 1] + sorted[mid]) / 2
        }
        return sorted[mid]
    }
}

// MARK: - EpisodeFeatureStats

/// Episode-wide spread (population standard deviation) of each feature used by
/// the boundary detector, for σ-normalizing the per-edge steps. Computed once
/// per span-batch call. `hasUsableVariance` is false for a perfectly flat
/// episode (the zero-variance edge case) so the detector emits nothing rather
/// than dividing by zero.
struct EpisodeFeatureStats: Sendable, Equatable {
    let loudnessSigma: Double
    let fluxSigma: Double
    let musicProbSigma: Double

    init(windows: [FeatureWindow], rmsFloor: Double) {
        let dbfsValues = windows.map { 20 * log10(max($0.rms, rmsFloor)) }
        self.loudnessSigma = Self.populationStdDev(dbfsValues)
        self.fluxSigma = Self.populationStdDev(windows.map(\.spectralFlux))
        self.musicProbSigma = Self.populationStdDev(windows.map(\.musicProbability))
    }

    /// At least one feature must have non-zero spread, or there is no
    /// discontinuity to normalize against.
    var hasUsableVariance: Bool {
        loudnessSigma > 0 || fluxSigma > 0 || musicProbSigma > 0
    }

    /// Variance below this floor is treated as zero. A perfectly flat episode
    /// (every window identical) produces a variance of ~1e-30 rather than
    /// exactly 0 due to floating-point summation residue in the two-pass mean/
    /// deviation computation; without this floor `hasUsableVariance` would
    /// wrongly report `true` for a flat episode and the detector would try to
    /// sigma-normalize against numerical noise. The floor is far below any
    /// real feature spread (dBFS deltas are O(1), flux/musicProb O(0.01+)).
    static let varianceEpsilon: Double = 1e-12

    static func populationStdDev(_ values: [Double]) -> Double {
        guard values.count > 1 else { return 0 }
        let n = Double(values.count)
        let mean = values.reduce(0, +) / n
        let variance = values.reduce(0.0) { acc, v in
            let d = v - mean
            return acc + d * d
        } / n
        guard variance > varianceEpsilon, variance.isFinite else { return 0 }
        return variance.squareRoot()
    }
}
