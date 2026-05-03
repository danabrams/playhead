// ShowLocalPriorsBuilder.swift
// playhead-084j: Real producer for `ShowLocalPriors`.
//
// Until this bead landed, `ShowLocalPriors` had no production producer —
// it was only constructed from tests. The 4-level prior hierarchy ran with
// global defaults only, so `DurationPrior` never reflected per-show ad
// length distributions.
//
// This builder reads the per-show `AdDurationStats` aggregate persisted on
// `PodcastProfile.adDurationStatsJSON` and turns it into a `ShowLocalPriors`
// value the resolver can consume. The aggregate is updated by
// `AdDetectionService.updatePriors` after each backfill from the durations
// of confirmed `AdWindow`s for the episode.
//
// Threshold and shape calibration:
//   • `minSampleCount` (5): below this we return `nil` so the resolver falls
//     through to global defaults. Five samples is the same magic number the
//     resolver uses for `showLocalThreshold` — see `PriorHierarchyResolver`.
//     Keeping the two in lock-step means a profile that *can* feed
//     show-local override always *does* feed it (no off-by-one window).
//   • Range half-width (`durationRangeHalfWidth`): a fixed 12-second band
//     around the observed mean. Wide enough to absorb sample-mean noise at
//     the threshold, narrow enough to be visibly different from the global
//     30...90s when the show's ads run short (5s) or long (180s).
//
// GUARDRAIL: The builder is pure. Persistence (column + accumulator) lives
// in `AnalysisStore` and `AdDetectionService.updatePriors` respectively.
//
// CURRENT CONSUMPTION (cycle-1 H1, 2026-05-03): the only `ShowLocalPriors`
// field this builder populates is `typicalAdDuration` (driven by the
// `meanDuration` column on `AdDurationStats`). The other ShowLocalPriors
// fields (`musicBracketTrust`, `metadataTrust`, `fmBudgetBias`,
// `fingerprintTransferConfidence`, `sponsorRecurrenceExpectation`) are
// emitted as `nil` because no production aggregator exists for them yet
// and the resolver short-circuits on `nil` per-field. Pairs with the
// downstream consumption note in `PriorHierarchy.swift`.
//
// Network priors and trait writers are filed as separate beads — see the
// PR description for IDs.

import Foundation

// MARK: - AdDurationStats

/// Per-show observed ad duration aggregate.
///
/// The schema is intentionally minimal: a running mean and a sample count.
/// We use a streaming-mean update rather than persisting the full duration
/// vector so the JSON column never grows beyond a fixed footprint.
struct AdDurationStats: Codable, Sendable, Equatable {
    /// EMA-style running mean of observed ad durations (seconds).
    /// Clamped to [0, +∞) on read.
    let meanDuration: TimeInterval
    /// Number of confirmed ad-window durations that have contributed to
    /// `meanDuration`. The same value gates the show-local override in
    /// `ShowLocalPriorsBuilder.build` — when below `minSampleCount`, the
    /// builder returns nil and the hierarchy falls back to global defaults.
    /// Clamped to [0, `maxSampleCount`] on read.
    let sampleCount: Int

    /// cycle-1 L2: hard ceiling on `sampleCount` so the Welford-style
    /// streaming mean update (`mean += (d - mean) / Double(count)`) can't
    /// silently round to a no-op once `count` grows past the resolution
    /// of `Double` integer-step. At 100_000 samples each new observation
    /// still moves the mean by at least 1e-5 seconds (10 microseconds),
    /// well above the precision floor. A daily-listener show won't reach
    /// this in years; the ceiling exists for pathological / corrupt
    /// payloads (Int.max written by a hand-edited JSON file, or a
    /// runaway loop that persisted millions of synthetic durations).
    /// Beyond the ceiling we stop counting — the running mean is already
    /// well-converged and further updates would be sub-precision.
    static let maxSampleCount: Int = 100_000

    init(meanDuration: TimeInterval, sampleCount: Int) {
        self.meanDuration = max(0, meanDuration)
        self.sampleCount = min(Self.maxSampleCount, max(0, sampleCount))
    }

    /// Custom `Decodable.init` that funnels decoded values back through the
    /// clamping memberwise initializer. Without this, a hand-edited or
    /// version-skewed JSON payload like `{"meanDuration":-5,"sampleCount":10}`
    /// would round-trip as a negative mean — `JSONDecoder`'s synthesized
    /// init writes directly to the stored `let` properties, bypassing the
    /// `max(0, ...)` clamp. Funneling through `init(meanDuration:sampleCount:)`
    /// makes the clamp authoritative across every construction path.
    private enum CodingKeys: String, CodingKey {
        case meanDuration
        case sampleCount
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let rawMean = try container.decode(TimeInterval.self, forKey: .meanDuration)
        let rawCount = try container.decode(Int.self, forKey: .sampleCount)
        self.init(meanDuration: rawMean, sampleCount: rawCount)
    }

    /// Sentinel for a brand-new show with no observations yet.
    static let empty = AdDurationStats(meanDuration: 0, sampleCount: 0)
}

// MARK: - ShowLocalPriorsBuilder

/// Pure helper that converts persisted `AdDurationStats` into the
/// `ShowLocalPriors` value the `PriorHierarchyResolver` expects.
enum ShowLocalPriorsBuilder {

    /// Minimum sample count before show-local priors override the resolver.
    /// Matches `PriorHierarchyResolver.showLocalThreshold` (5) so the gate
    /// is consistent at both levels.
    static let minSampleCount: Int = 5

    /// Half-width (seconds) of the typicalAdDuration band centered on the
    /// observed mean. A 12-second half-width yields a 24-second-wide band,
    /// noticeably narrower than the 60-second-wide global default but with
    /// enough slack to absorb 1σ sample-mean noise once `sampleCount ≥ 5`.
    static let durationRangeHalfWidth: TimeInterval = 12

    /// cycle-1 L3: minimum realistic ad duration (seconds). Anything
    /// shorter is almost certainly a boundary-snap artifact — real
    /// pre-roll / mid-roll ads run at minimum several seconds. Folding
    /// sub-second "ads" into the mean would silently drag the show-
    /// local typical down. Used by `mergeDurations` as a hard filter
    /// in addition to the caller-side filter in
    /// `AdDetectionService.updatePriors`.
    static let minRealisticDuration: TimeInterval = 1.0

    /// Build show-local priors from the current `PodcastProfile`, or `nil`
    /// when the profile has no usable aggregate yet.
    ///
    /// Returns nil when:
    ///   • `profile == nil`
    ///   • `adDurationStatsJSON == nil` or empty
    ///   • the JSON cannot be decoded
    ///   • `sampleCount < minSampleCount`
    /// All four nil paths funnel the resolver to global defaults — graceful
    /// degradation for shows without enough history.
    static func build(from profile: PodcastProfile?) -> ShowLocalPriors? {
        guard let profile,
              let json = profile.adDurationStatsJSON,
              !json.isEmpty,
              let data = json.data(using: .utf8),
              let stats = try? JSONDecoder().decode(AdDurationStats.self, from: data),
              stats.sampleCount >= minSampleCount,
              stats.meanDuration > 0
        else {
            return nil
        }

        let lower = max(0, stats.meanDuration - durationRangeHalfWidth)
        let upper = stats.meanDuration + durationRangeHalfWidth
        let range: ClosedRange<TimeInterval> = lower...upper

        // Episode count drives the resolver's blend weight via
        // `PriorHierarchyResolver.showLocalBlendWeight`. We use the
        // profile's `observationCount` (number of episodes processed)
        // as the canonical "episodes observed" signal. The threshold
        // gate here is the sample count (number of observed ads); the
        // blend weight scales with observed episodes.
        //
        // cycle-1 L1: pass `observationCount` through verbatim. The
        // previous implementation floored episodeCount at
        // `PriorHierarchyResolver.showLocalThreshold` "so the resolver
        // gate is guaranteed to clear", which papered over a real
        // inconsistency: a profile with sampleCount >= 5 but
        // observationCount < 5 (e.g. one episode that yielded 5
        // confirmed ads) wouldn't actually have enough cross-episode
        // generality to justify activating show-local priors, and the
        // resolver's `episodeCount >= showLocalThreshold` check is the
        // gate that catches this. By passing the raw value, we let the
        // resolver enforce its own contract instead of having the
        // builder lie about it. In the common case the two grow
        // together, so this only changes behavior on the pathological
        // single-episode-many-ads pattern that should fall back to
        // global defaults anyway.
        return ShowLocalPriors(
            musicBracketTrust: nil,
            metadataTrust: nil,
            fmBudgetBias: nil,
            fingerprintTransferConfidence: nil,
            sponsorRecurrenceExpectation: nil,
            typicalAdDuration: range,
            episodeCount: profile.observationCount
        )
    }

    /// Streaming-mean update for `AdDurationStats`. Accumulates new ad
    /// durations into the existing aggregate without growing the payload.
    ///
    /// The classic Welford-style mean update is `μ' = μ + (xₙ - μ)/n`. We
    /// extend that to a batch by folding each new sample sequentially, so
    /// callers can pass either a single duration or a full episode's worth.
    ///
    /// Filter: finite-and-realistic (`d.isFinite && d >= minRealisticDuration`).
    /// The inner gate here is intentionally stricter than the caller's
    /// pre-filter in `AdDetectionService.updatePriors` (which still permits
    /// `d > 0`); belt-and-suspenders in case a future caller forgets to
    /// pre-filter or relaxes its own gate. A zero-or-near-zero-second "ad"
    /// is meaningless (it would still be counted toward `sampleCount` while
    /// pulling the running mean toward zero), which is why we tighten here
    /// rather than mirror the caller's looser gate.
    ///
    /// cycle-1 L3: tightened from `d > 0` to
    /// `d >= minRealisticDuration` (1.0s). A sub-second AdWindow is
    /// almost certainly a boundary-snap artifact rather than a real
    /// ad; counting it would understate the running mean. Real-world
    /// pre-roll/mid-roll ads are at minimum a few seconds, so 1.0s is
    /// a generous floor that still rejects degenerate detections.
    ///
    /// cycle-1 L2: stop accumulating once `count` reaches
    /// `AdDurationStats.maxSampleCount`. Past the ceiling the mean is
    /// already well-converged and further updates would be sub-precision
    /// (`Double` integer-step rounding makes them no-ops). Short-
    /// circuiting in the loop keeps `mean` and `count` coherent — if we
    /// kept folding samples in but the init-time clamp caps `count`, the
    /// mean would drift while the count silently froze, producing an
    /// inconsistent aggregate.
    static func mergeDurations(
        existing: AdDurationStats,
        newDurations: [TimeInterval]
    ) -> AdDurationStats {
        guard !newDurations.isEmpty else { return existing }

        var mean = existing.meanDuration
        var count = existing.sampleCount

        for d in newDurations where d.isFinite && d >= minRealisticDuration {
            if count >= AdDurationStats.maxSampleCount { break }
            count += 1
            mean += (d - mean) / Double(count)
        }

        return AdDurationStats(meanDuration: mean, sampleCount: count)
    }
}
