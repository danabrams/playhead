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
    let sampleCount: Int

    init(meanDuration: TimeInterval, sampleCount: Int) {
        self.meanDuration = max(0, meanDuration)
        self.sampleCount = max(0, sampleCount)
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
        // profile's `observationCount` (number of episodes processed) as
        // the canonical "episodes observed" signal. The threshold gate
        // here is the sample count (number of observed ads); the blend
        // weight scales with observed episodes. Both grow together in
        // practice, so this keeps the resolver's contract intact.
        //
        // Coupling note (see `PriorHierarchy.swift` `showLocalThreshold`):
        // we floor `episodeCount` at the resolver's `showLocalThreshold` so
        // any value the builder produces is guaranteed to clear the
        // resolver's `episodeCount >= showLocalThreshold` gate. The
        // independent gate here is `sampleCount >= minSampleCount` above;
        // the resolver-side check becomes a no-op when the builder emits
        // a non-nil value at all.
        return ShowLocalPriors(
            musicBracketTrust: nil,
            metadataTrust: nil,
            fmBudgetBias: nil,
            fingerprintTransferConfidence: nil,
            sponsorRecurrenceExpectation: nil,
            typicalAdDuration: range,
            episodeCount: max(profile.observationCount, PriorHierarchyResolver.showLocalThreshold)
        )
    }

    /// Streaming-mean update for `AdDurationStats`. Accumulates new ad
    /// durations into the existing aggregate without growing the payload.
    ///
    /// The classic Welford-style mean update is `μ' = μ + (xₙ - μ)/n`. We
    /// extend that to a batch by folding each new sample sequentially, so
    /// callers can pass either a single duration or a full episode's worth.
    ///
    /// Filter: positive-and-finite (`d.isFinite && d > 0`). Matches the
    /// caller-side filter in `AdDetectionService.updatePriors` so the two
    /// gates can't drift independently. A zero-second "ad" is meaningless
    /// (it would still be counted toward `sampleCount` while pulling the
    /// running mean toward zero) — guarding here is belt-and-suspenders
    /// in case a future caller forgets to pre-filter.
    static func mergeDurations(
        existing: AdDurationStats,
        newDurations: [TimeInterval]
    ) -> AdDurationStats {
        guard !newDurations.isEmpty else { return existing }

        var mean = existing.meanDuration
        var count = existing.sampleCount

        for d in newDurations where d.isFinite && d > 0 {
            count += 1
            mean += (d - mean) / Double(count)
        }

        return AdDurationStats(meanDuration: mean, sampleCount: count)
    }
}
