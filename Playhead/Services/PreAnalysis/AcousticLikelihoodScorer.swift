// AcousticLikelihoodScorer.swift
// playhead-gtt9.24: scheduler-time acoustic ad-likelihood scoring.
//
// Acoustic features (music-bed onset/offset, speaker-change proxies,
// music-bed level, spectral flux) currently contribute to fusion *after*
// a window has been transcribed. They do NOT influence *which* windows
// get transcribed — the scheduler today walks the audio in tier-ladder
// order (T0 → T1 → T2) regardless of whether those regions contain
// anything ad-relevant. For long episodes that overflow the BG-task
// budget, that means the trailing 60–90 min are never scored.
//
// This scorer promotes acoustic features from "fusion input" to
// "scheduler input": given the per-window features that are already
// persisted in `feature_windows` (Stage 2 output), compute a per-window
// `[0, 1]` ad-likelihood. The scheduler uses the score to pick the next
// window to transcribe — high-likelihood windows get transcribed first.
//
// Design decisions resolved against the bead body:
//
// 1. **Score function** combines onset + offset + speaker-change +
//    music-bed level + spectral flux. Weights are deliberate priors
//    chosen to favour the most ad-correlated cues (music-bed onset / offset)
//    without locking in a pre-calibration value. gtt9.3's grid search
//    will later replace these the same way it does
//    `AcousticFeatureFusion.Weights`.
//
// 2. **Combiner is bounded-additive** (sum of weighted contributions
//    clamped to [0, 1]) so multiple independent acoustic signals
//    reinforce each other — the same shape used by
//    `AcousticFeatureFusion.combine`. Max-take-all would have biased
//    toward single-feature outliers and missed the multi-signal stack
//    that's the bead's whole premise.
//
// 3. **Pure function over `FeatureWindow`** so it can be exhaustively
//    unit-tested without standing up the scheduler. Stateless; lives
//    as an enum namespace.
//
// 4. **No transcript dependency.** The whole point of this bead is to
//    score windows BEFORE transcription, so the scorer must not consult
//    transcript chunks. (Lexical features remain a separate channel,
//    consumed by classifier-time fusion not scheduler-time selection.)

import Foundation

/// Per-window acoustic ad-likelihood score the scheduler consults to
/// pick which window to transcribe next.
struct AcousticLikelihoodScore: Sendable, Equatable {
    /// Window start time (seconds from episode start).
    let windowStart: Double
    /// Window end time (seconds from episode start).
    let windowEnd: Double
    /// Combined acoustic ad-likelihood in `[0, 1]`. Higher = more
    /// likely to contain an ad transition or ad body.
    let score: Double
}

enum AcousticLikelihoodScorer {

    /// Weighting for the per-feature contributions to the per-window
    /// likelihood score. Sums to 1.0 by construction (so the combined
    /// score's natural ceiling stays at 1.0 before clamping).
    ///
    /// Priors emphasise music-bed onset/offset because in the gtt9.16
    /// hot-path wiring those features were the ones whose
    /// `firingRate` was most predictive of ad regions. Speaker-change
    /// proxy and music-bed level are secondary contributors.
    /// Spectral-flux is tertiary — it fires on many non-ad events
    /// (laughter, applause) so we keep its weight low.
    struct Weights: Sendable, Equatable {
        let musicBedOnset: Double
        let musicBedOffset: Double
        let speakerChangeProxy: Double
        let musicBedLevel: Double
        let spectralFlux: Double

        static let defaultPriors = Weights(
            musicBedOnset: 0.30,
            musicBedOffset: 0.30,
            speakerChangeProxy: 0.20,
            musicBedLevel: 0.10,
            spectralFlux: 0.10
        )
    }

    /// Compute the per-window likelihood for every persisted feature
    /// window. The output array is index-aligned with `windows`.
    ///
    /// - Parameters:
    ///   - windows: Already-persisted `feature_windows` for an asset,
    ///     in episode-time order. Caller is responsible for ordering;
    ///     this function does not sort.
    ///   - weights: Override the default priors (tests + future
    ///     calibration).
    /// - Returns: Per-window scores, same count as input.
    static func score(
        windows: [FeatureWindow],
        weights: Weights = .defaultPriors
    ) -> [AcousticLikelihoodScore] {
        windows.map { window in
            AcousticLikelihoodScore(
                windowStart: window.startTime,
                windowEnd: window.endTime,
                score: scoreOne(window, weights: weights)
            )
        }
    }

    /// Compute the per-window likelihood for a single window. Exposed
    /// so callers that want the score without paying for an array
    /// allocation (e.g. inline gating in the scheduler) can reach
    /// directly into the math.
    static func scoreOne(
        _ window: FeatureWindow,
        weights: Weights = .defaultPriors
    ) -> Double {
        let levelComponent = musicBedLevelContribution(window.musicBedLevel)
        let raw =
            weights.musicBedOnset       * window.musicBedOnsetScore
            + weights.musicBedOffset    * window.musicBedOffsetScore
            + weights.speakerChangeProxy * window.speakerChangeProxyScore
            + weights.musicBedLevel     * levelComponent
            + weights.spectralFlux      * normalizedSpectralFlux(window.spectralFlux)
        return clampUnit(raw)
    }

    /// Compute the maximum per-window likelihood among the feature
    /// windows that overlap the half-open span `[startTime, endTime)`.
    /// Returns `nil` when no feature window overlaps the span — the
    /// caller treats `nil` as "score unknown" and never gates on it.
    ///
    /// Used by the gtt9.1 transcript-gate (`AnalysisJobRunner`) to
    /// reduce a shard's worth of feature windows to a single per-shard
    /// likelihood: a shard inherits its strongest acoustic-ad signal,
    /// which keeps a high-likelihood onset window from being washed out
    /// by surrounding low-likelihood quiet windows that share the same
    /// shard. Max-take-all is the right shape here (unlike `combine` in
    /// `AcousticFeatureFusion`) because a shard that *contains* an ad
    /// transition should be scheduled even if most of the shard is host
    /// conversation — we transcribe the whole shard anyway, so the max
    /// is what governs scheduler value.
    ///
    /// - Parameters:
    ///   - windows: Per-asset persisted feature windows in any order.
    ///   - startTime: Span start in episode-relative seconds (inclusive).
    ///   - endTime: Span end in episode-relative seconds (exclusive).
    ///     Half-open intervals match the shard convention used by
    ///     `AnalysisShard` (`startTime`, `startTime + duration`).
    ///   - weights: Score weights (default priors).
    /// - Returns: The maximum scored likelihood over overlapping
    ///   windows, or `nil` if no window overlaps the span.
    static func maxLikelihoodInSpan(
        windows: [FeatureWindow],
        startTime: Double,
        endTime: Double,
        weights: Weights = .defaultPriors
    ) -> Double? {
        var best: Double?
        for window in windows {
            // Half-open overlap: [w.start, w.end) ∩ [startTime, endTime) ≠ ∅
            // iff w.start < endTime && w.end > startTime.
            guard window.startTime < endTime, window.endTime > startTime else {
                continue
            }
            let s = scoreOne(window, weights: weights)
            if best == nil || s > (best ?? 0) {
                best = s
            }
        }
        return best
    }

    /// Find the highest-likelihood region in the windows that lies
    /// strictly *beyond* `currentCoverageSec`. Returns the region's
    /// `(start, end, score)` triple, or `nil` when no window past the
    /// cutoff scores above `threshold`.
    ///
    /// This is the scheduler's primary entry point: it asks "is there
    /// a likely-ad region in the unscored portion of the episode that
    /// I should escalate coverage to?" The threshold is the gate that
    /// decides whether escalation is worth the BG-task wake.
    ///
    /// "Region" today is just the single highest-scoring window. A
    /// future iteration could merge contiguous high-score windows
    /// into a span; for the bead's "promote one window past T2"
    /// shape, single-window granularity is sufficient.
    ///
    /// - Parameters:
    ///   - windows: Per-asset persisted feature windows in episode-time
    ///     order. Windows whose `endTime <= currentCoverageSec` are
    ///     ignored — they're already transcribed (or about to be by
    ///     the existing tier ladder).
    ///   - currentCoverageSec: The job's current `desiredCoverageSec`
    ///     or the asset's `fastTranscriptCoverageEndTime`, whichever
    ///     the caller wants to treat as the cutoff. For acoustic
    ///     promotion the natural choice is the larger of the two.
    ///   - threshold: Minimum score required to flag a window as
    ///     promotion-worthy. 0.5 default — half the theoretical max.
    ///   - weights: Score weights (default priors).
    static func highestLikelihoodBeyond(
        windows: [FeatureWindow],
        currentCoverageSec: Double,
        threshold: Double = 0.5,
        weights: Weights = .defaultPriors
    ) -> AcousticLikelihoodScore? {
        var best: AcousticLikelihoodScore?
        for window in windows where window.endTime > currentCoverageSec {
            let s = scoreOne(window, weights: weights)
            guard s >= threshold else { continue }
            if best == nil || s > (best?.score ?? 0) {
                best = AcousticLikelihoodScore(
                    windowStart: window.startTime,
                    windowEnd: window.endTime,
                    score: s
                )
            }
        }
        return best
    }

    // MARK: - Private helpers

    /// Convert `MusicBedLevel` into a `[0, 1]` contribution.
    /// `.foreground` is the strongest signal (jingles / loud ad music
    /// stingers overwhelmingly correlate with ad transitions);
    /// `.background` is moderate (quiet production beds run under both
    /// hosts and ads, so it's evidence but not dispositive);
    /// `.none` zeroes out.
    private static func musicBedLevelContribution(_ level: MusicBedLevel) -> Double {
        switch level {
        case .none:       return 0.0
        case .background: return 0.5
        case .foreground: return 1.0
        }
    }

    /// Spectral flux readings vary widely with episode-level loudness
    /// normalization. We compress to a `[0, 1]` proxy via a soft cap
    /// at 0.5 — flux above that ceiling adds no further mass. Keeps
    /// the contribution stable across episodes whose loudness profile
    /// puts raw flux in different absolute ranges.
    private static func normalizedSpectralFlux(_ flux: Double) -> Double {
        guard flux > 0 else { return 0 }
        return min(1.0, flux / 0.5)
    }
}
