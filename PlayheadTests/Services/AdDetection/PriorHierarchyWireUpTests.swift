// PriorHierarchyWireUpTests.swift
// playhead-084j: Verify that PriorHierarchyResolver is invoked from the
// production AdDetectionService backfill path and that the resolved priors
// flow through DurationPrior — not the global default.
//
// Test strategy:
//   • Unit-level: lock the ShowLocalPriorsBuilder contract for deriving
//     ShowLocalPriors from a PodcastProfile.adDurationStatsJSON aggregate.
//   • Wire-up level: lock the AdDetectionService.resolveEpisodePriors entry
//     point so it composes global + trait + show-local once per episode and
//     hands the result to DurationPrior. The full backfill is too heavy to
//     run inline here — the wire-up tests target the resolver entry directly,
//     and BackfillEvidenceFusionTests already covers the DecisionMapper math.
//
// Acceptance per the bead:
//   • Resolver is invoked from production code (not just tests).
//   • ResolvedPriors is available to the backfill fusion path.
//   • DurationPrior uses the resolved typicalAdDuration, not the global default.
//   • Show-local priors override global when enough episodes are observed.
//   • No regression on shows without accumulated priors (graceful fallback).

import Foundation
import SQLite3
import Testing
@testable import Playhead

@Suite("PriorHierarchy production wire-up (playhead-084j)")
struct PriorHierarchyWireUpTests {

    // MARK: - ShowLocalPriorsBuilder unit tests

    @Test("nil profile yields nil show-local priors")
    func builderNilProfile() {
        let local = ShowLocalPriorsBuilder.build(from: nil)
        #expect(local == nil)
    }

    @Test("profile with no adDurationStatsJSON yields nil")
    func builderEmptyStatsField() {
        let profile = makeProfile(adDurationStatsJSON: nil, observationCount: 10)
        #expect(ShowLocalPriorsBuilder.build(from: profile) == nil)
    }

    @Test("profile with corrupt adDurationStatsJSON yields nil")
    func builderCorruptStatsField() {
        let profile = makeProfile(adDurationStatsJSON: "{not json", observationCount: 10)
        #expect(ShowLocalPriorsBuilder.build(from: profile) == nil)
    }

    @Test("builder threshold: below minSampleCount yields nil")
    func builderBelowSampleThreshold() {
        // Threshold uses sampleCount (number of observed ad windows fed in).
        // A profile that has only 2 observed ads should not build show-local
        // priors — too few samples to override global.
        let stats = AdDurationStats(meanDuration: 25, sampleCount: 2)
        let profile = makeProfile(
            adDurationStatsJSON: stats.encodeForTesting(),
            observationCount: 2
        )
        #expect(ShowLocalPriorsBuilder.build(from: profile) == nil)
    }

    @Test("builder at sample threshold yields show-local priors")
    func builderAtSampleThreshold() {
        let stats = AdDurationStats(meanDuration: 30, sampleCount: ShowLocalPriorsBuilder.minSampleCount)
        let profile = makeProfile(
            adDurationStatsJSON: stats.encodeForTesting(),
            observationCount: 5
        )
        let local = ShowLocalPriorsBuilder.build(from: profile)
        #expect(local != nil)
        #expect(local?.episodeCount == 5)
    }

    @Test("builder narrows typicalAdDuration around the show's mean")
    func builderShapesNarrowDuration() {
        // A show whose ads are tightly distributed around 5s should produce
        // a typicalAdDuration centered on 5s (much narrower than the global
        // 30...90s default).
        let stats = AdDurationStats(meanDuration: 5, sampleCount: 20)
        let profile = makeProfile(
            adDurationStatsJSON: stats.encodeForTesting(),
            observationCount: 12
        )
        let local = ShowLocalPriorsBuilder.build(from: profile)
        let range = try! #require(local?.typicalAdDuration)
        // The center of the range should be near the observed mean.
        let center = (range.lowerBound + range.upperBound) / 2.0
        #expect(abs(center - 5) < 5)
        // The range should be measurably narrower than the standard 30...90 (60s wide).
        let width = range.upperBound - range.lowerBound
        #expect(width < 60)
    }

    @Test("AdDurationStats clamps negative meanDuration on decode")
    func adDurationStatsDecodeClampsNegativeMean() throws {
        // A hand-edited or version-skewed JSON payload with a negative
        // mean must not survive decode — the custom `init(from:)` funnels
        // raw values through `init(meanDuration:sampleCount:)` so the
        // `max(0, ...)` clamp is authoritative across every construction
        // path. Without this, `JSONDecoder`'s synthesized init would
        // write the negative value directly to the stored property.
        let corrupt = #"{"meanDuration":-5,"sampleCount":10}"#
        let data = Data(corrupt.utf8)
        let stats = try JSONDecoder().decode(AdDurationStats.self, from: data)
        #expect(stats.meanDuration == 0)
        #expect(stats.sampleCount == 10)
    }

    @Test("AdDurationStats clamps negative sampleCount on decode")
    func adDurationStatsDecodeClampsNegativeCount() throws {
        let corrupt = #"{"meanDuration":12.5,"sampleCount":-7}"#
        let data = Data(corrupt.utf8)
        let stats = try JSONDecoder().decode(AdDurationStats.self, from: data)
        #expect(stats.meanDuration == 12.5)
        #expect(stats.sampleCount == 0)
    }

    @Test("AdDurationStats clamps huge sampleCount on decode (cycle-1 L2)")
    func adDurationStatsClampsHugeSampleCount() throws {
        // A corrupt or runaway payload could land Int.max on disk;
        // without a ceiling, the Welford-style streaming mean update
        // (`mean += (d - mean) / Double(count)`) eventually rounds new
        // samples to no-ops once `count` exceeds Double's integer-step
        // resolution, but `sampleCount` keeps climbing — leaving an
        // inconsistent aggregate. Clamp to `maxSampleCount`.
        let huge = #"{"meanDuration":42.0,"sampleCount":999999999}"#
        let data = Data(huge.utf8)
        let stats = try JSONDecoder().decode(AdDurationStats.self, from: data)
        #expect(stats.sampleCount == AdDurationStats.maxSampleCount)
        #expect(stats.meanDuration == 42.0)
    }

    @Test("mergeDurations rejects sub-1s durations (cycle-1 L3)")
    func mergeDurationsFiltersUnrealisticDurations() {
        // Sub-second "ads" are almost always boundary-snap artifacts
        // rather than real pre-roll/mid-roll. Folding them into the
        // mean would drag the show-local typical toward zero.
        let seed = AdDurationStats(meanDuration: 30, sampleCount: 5)
        let merged = ShowLocalPriorsBuilder.mergeDurations(
            existing: seed,
            newDurations: [0.5, 0.99, -1.0, 0.0]
        )
        // None of these durations should count; aggregate unchanged.
        #expect(merged.sampleCount == seed.sampleCount)
        #expect(merged.meanDuration == seed.meanDuration)

        // A duration AT the floor (1.0s) is still suspicious but
        // accepted — the boundary is "anything under 1s rejected".
        let acceptedFloor = ShowLocalPriorsBuilder.mergeDurations(
            existing: seed,
            newDurations: [1.0]
        )
        #expect(acceptedFloor.sampleCount == seed.sampleCount + 1)
    }

    @Test("mergeDurations short-circuits at maxSampleCount (cycle-1 L2)")
    func mergeDurationsRespectsCeiling() {
        // Seed the aggregate just below the ceiling, then merge enough
        // durations to (in absence of the ceiling) push count well past
        // it. We expect mergeDurations to break out of the fold once
        // count == maxSampleCount so the mean and count stay coherent.
        let seed = AdDurationStats(
            meanDuration: 30,
            sampleCount: AdDurationStats.maxSampleCount - 2
        )
        let newDurations = Array(repeating: 60.0, count: 100)
        let merged = ShowLocalPriorsBuilder.mergeDurations(
            existing: seed,
            newDurations: newDurations
        )
        #expect(merged.sampleCount == AdDurationStats.maxSampleCount)
        // Mean should have moved toward 60 by exactly 2 samples'
        // worth, not 100. With seed mean 30, two 60s observations
        // bring mean to ~30 + (30/(N-1)) + (30/N), all sub-precision
        // for N≈100k. The mean must remain within sane bounds (didn't
        // run away).
        #expect(merged.meanDuration >= 30)
        #expect(merged.meanDuration < 31)
    }

    @Test("builder passes observationCount through verbatim (cycle-1 L1)")
    func builderDoesNotFloorEpisodeCount() {
        // cycle-1 L1: previously the builder floored `episodeCount` at
        // `PriorHierarchyResolver.showLocalThreshold` (5) so the resolver
        // gate was guaranteed to clear. That papered over a real
        // inconsistency: a profile with sampleCount >= 5 but
        // observationCount < 5 (one episode yielding many ads) wouldn't
        // have enough cross-episode generality to justify activating
        // show-local priors. The builder now passes `observationCount`
        // through verbatim and lets the resolver enforce its own gate.
        //
        // Construct a profile with sampleCount >= minSampleCount but a
        // small `observationCount=2`, and assert the builder emits
        // episodeCount=2 (not 5).
        let stats = AdDurationStats(
            meanDuration: 30,
            sampleCount: ShowLocalPriorsBuilder.minSampleCount
        )
        let profile = makeProfile(
            adDurationStatsJSON: stats.encodeForTesting(),
            observationCount: 2
        )
        // Builder still requires sampleCount >= minSampleCount, which
        // we satisfy. The observationCount value should flow through
        // unchanged so the resolver can gate the activation.
        let local = ShowLocalPriorsBuilder.build(from: profile)
        #expect(local?.episodeCount == 2)
    }

    @Test("builder with mean 60 (typical ad) keeps a normal range")
    func builderShapesTypicalDuration() {
        let stats = AdDurationStats(meanDuration: 60, sampleCount: 30)
        let profile = makeProfile(
            adDurationStatsJSON: stats.encodeForTesting(),
            observationCount: 20
        )
        let local = ShowLocalPriorsBuilder.build(from: profile)
        let range = try! #require(local?.typicalAdDuration)
        // Range should bracket 60 (typical ad length) within a sensible band.
        #expect(range.lowerBound < 60)
        #expect(range.upperBound > 60)
    }

    // MARK: - AdDetectionService.resolveEpisodePriors wire-up

    @Test("resolveEpisodePriors returns global defaults with no profile")
    func wireUpNoProfile() async {
        let store = try! await makeTestStore()
        let service = makeService(store: store, profile: nil)
        let resolved = await service.resolveEpisodePriorsForTesting()
        #expect(resolved.activeLevel == .global)
        #expect(resolved.typicalAdDuration == GlobalPriorDefaults.standard.typicalAdDuration)
    }

    @Test("resolveEpisodePriors with a profile lacking show-local stats stays at global")
    func wireUpProfileWithoutShowLocal() async {
        let store = try! await makeTestStore()
        let profile = makeProfile(
            adDurationStatsJSON: nil,
            observationCount: 1
        )
        let service = makeService(store: store, profile: profile)
        let resolved = await service.resolveEpisodePriorsForTesting()
        #expect(resolved.activeLevel == .global)
        #expect(resolved.typicalAdDuration == GlobalPriorDefaults.standard.typicalAdDuration)
    }

    @Test("resolveEpisodePriors with show-local stats activates showLocal")
    func wireUpShowLocalActivates() async {
        let store = try! await makeTestStore()
        let stats = AdDurationStats(meanDuration: 5, sampleCount: 20)
        let profile = makeProfile(
            adDurationStatsJSON: stats.encodeForTesting(),
            observationCount: 12
        )
        let service = makeService(store: store, profile: profile)
        let resolved = await service.resolveEpisodePriorsForTesting()
        #expect(resolved.activeLevel == .showLocal)
        // Range should be shifted toward the 5s mean — center much smaller than 60.
        let center = (resolved.typicalAdDuration.lowerBound + resolved.typicalAdDuration.upperBound) / 2.0
        #expect(center < 30)
    }

    @Test("DurationPrior built from resolved priors uses the resolved range")
    func wireUpDurationPriorUsesResolved() async {
        let store = try! await makeTestStore()
        let stats = AdDurationStats(meanDuration: 5, sampleCount: 20)
        let profile = makeProfile(
            adDurationStatsJSON: stats.encodeForTesting(),
            observationCount: 12
        )
        let service = makeService(store: store, profile: profile)
        let resolved = await service.resolveEpisodePriorsForTesting()
        let prior = DurationPrior(resolvedPriors: resolved)
        // The resolver blends the builder's show-local range (0...17) with the
        // global default 30...90 at the show-local weight (0.8 at episode count
        // 12, see PriorHierarchyResolver.showLocalBlendWeight). Resulting
        // resolved range is approximately 6...31.6, putting 10s squarely in
        // the peak region of the resolved prior. The standard prior at 10s
        // is still in the [5, 30) bumper region — well below peak. The
        // observable difference proves DurationPrior(resolvedPriors:) is
        // actually consuming the resolver's output, not silently using
        // GlobalPriorDefaults.standard.
        let standard = DurationPrior.standard
        let mShowLocal = prior.multiplier(forDuration: 10)
        let mStandard = standard.multiplier(forDuration: 10)
        #expect(mShowLocal > mStandard)
    }

    @Test("resolveEpisodePriors does not throw on a corrupt stats payload")
    func wireUpResolveGracefulOnCorruption() async {
        let store = try! await makeTestStore()
        let profile = makeProfile(
            adDurationStatsJSON: "{not json",
            observationCount: 10
        )
        let service = makeService(store: store, profile: profile)
        let resolved = await service.resolveEpisodePriorsForTesting()
        // Falls back to global defaults rather than crashing.
        #expect(resolved.activeLevel == .global)
        #expect(resolved.typicalAdDuration == GlobalPriorDefaults.standard.typicalAdDuration)
        // cycle-1 M2: a malformed payload should ALSO fire a `.error` log
        // through `AdDetectionService.staticLogger` so the corruption is
        // visible in DiagnosticReports / `log show` queries. We can't
        // assert that from a unit test (Logger writes to OSLog, not a
        // capturable sink), and adding a Logger test seam for this one
        // call site would over-engineer the diagnostic — the contract is
        // verified by reading `decodeAdDurationStats`'s body.
    }

    // MARK: - Network priors wire-up (playhead-spxs)

    /// playhead-spxs: when the current show has a `networkId` but is itself
    /// brand-new (no `adDurationStatsJSON`, no traitProfileJSON), and there
    /// are sibling profiles in the same network with usable
    /// `adDurationStatsJSON`, the resolver must surface
    /// `activeLevel == .network`. That is the load-bearing wire-up signal —
    /// without it, the network tier would still be the no-op it was before
    /// this bead.
    @Test("resolveEpisodePriors activates network tier when siblings carry stats")
    func wireUpNetworkTierActivates() async throws {
        let store = try await makeTestStore()
        let networkId = "pod-net-spxs-active"

        // Seed two sibling shows with usable `adDurationStatsJSON`.
        // Each well above the per-show sample threshold so the builder
        // accepts them, and clustered around 10s so the network typical
        // ad-duration center lands far from the global (60s) midpoint.
        let siblingStats = AdDurationStats(meanDuration: 10, sampleCount: 20)
        let sibling1 = makeProfile(
            podcastId: "pod-sibling-1",
            adDurationStatsJSON: siblingStats.encodeForTesting(),
            observationCount: 12,
            networkId: networkId
        )
        let sibling2 = makeProfile(
            podcastId: "pod-sibling-2",
            adDurationStatsJSON: siblingStats.encodeForTesting(),
            observationCount: 8,
            networkId: networkId
        )
        try await store.upsertProfile(sibling1)
        try await store.upsertProfile(sibling2)

        // Current show: same network, but brand-new — no own stats yet.
        // observationCount 0 keeps the network decay weight at its peak
        // (0.5), so the network tier blends in at full strength.
        let current = makeProfile(
            podcastId: "pod-current-spxs",
            adDurationStatsJSON: nil,
            observationCount: 0,
            networkId: networkId
        )
        try await store.upsertProfile(current)

        let service = makeService(store: store, profile: current)
        let resolved = await service.resolveEpisodePriorsForTesting()
        #expect(resolved.activeLevel == .network)
    }

    /// playhead-spxs: the wire-up's load-bearing claim. With the network
    /// tier active, the resolved typicalAdDuration must measurably move
    /// toward the network's converging mean — not stay parked on the
    /// global default (30...90, midpoint 60s). Sibling shows here all
    /// average ~10s, so the resolver's blend should drop the typical
    /// duration center well below the global midpoint.
    @Test("network priors measurably narrow resolved typicalAdDuration vs global")
    func wireUpNetworkNarrowsDurationRange() async throws {
        let store = try await makeTestStore()
        let networkId = "pod-net-spxs-narrow"

        let siblingStats = AdDurationStats(meanDuration: 10, sampleCount: 20)
        for i in 0..<3 {
            let sibling = makeProfile(
                podcastId: "pod-sibling-narrow-\(i)",
                adDurationStatsJSON: siblingStats.encodeForTesting(),
                observationCount: 8,
                networkId: networkId
            )
            try await store.upsertProfile(sibling)
        }

        // Current show has the network identity but no own observations.
        let current = makeProfile(
            podcastId: "pod-current-narrow",
            adDurationStatsJSON: nil,
            observationCount: 0,
            networkId: networkId
        )
        try await store.upsertProfile(current)

        let service = makeService(store: store, profile: current)
        let resolved = await service.resolveEpisodePriorsForTesting()

        // Global midpoint is 60s. After blending in the network's ~10s
        // mean at the peak decay weight (0.5), the resolved center must
        // sit below the global default's midpoint — a measurable shift.
        let globalCenter = (GlobalPriorDefaults.standard.typicalAdDuration.lowerBound +
                            GlobalPriorDefaults.standard.typicalAdDuration.upperBound) / 2.0
        let resolvedCenter = (resolved.typicalAdDuration.lowerBound +
                              resolved.typicalAdDuration.upperBound) / 2.0
        #expect(resolvedCenter < globalCenter)
    }

    /// playhead-spxs: graceful fallback. When the current show has no
    /// `networkId`, the network-priors lookup is skipped entirely and the
    /// resolver behaves identically to the pre-spxs wire-up — global
    /// defaults when nothing else is available, show-local when it is.
    @Test("resolveEpisodePriors with networkId == nil falls back to global (no network tier)")
    func wireUpNoNetworkIdFallsBackToGlobal() async throws {
        let store = try await makeTestStore()

        // Sibling shows exist, but the current show has no networkId, so
        // the lookup is never made. activeLevel must remain `.global`.
        let siblingStats = AdDurationStats(meanDuration: 10, sampleCount: 20)
        let sibling = makeProfile(
            podcastId: "pod-sibling-no-net",
            adDurationStatsJSON: siblingStats.encodeForTesting(),
            observationCount: 8,
            networkId: "some-other-network"
        )
        try await store.upsertProfile(sibling)

        let current = makeProfile(
            podcastId: "pod-current-no-net",
            adDurationStatsJSON: nil,
            observationCount: 0,
            networkId: nil
        )
        try await store.upsertProfile(current)

        let service = makeService(store: store, profile: current)
        let resolved = await service.resolveEpisodePriorsForTesting()
        #expect(resolved.activeLevel == .global)
        #expect(resolved.typicalAdDuration == GlobalPriorDefaults.standard.typicalAdDuration)
    }

    /// playhead-spxs: precedence preserved. When both network-tier and
    /// show-local data exist, show-local must dominate (its weight is
    /// 0.6+ vs network's 0.5 peak, and it's a higher level in the
    /// hierarchy enum). Without this, the network tier could undermine
    /// the more specific per-show signal.
    @Test("show-local priors still dominate when both tiers are available")
    func wireUpShowLocalDominatesNetwork() async throws {
        let store = try await makeTestStore()
        let networkId = "pod-net-spxs-precedence"

        // Network siblings cluster around 10s.
        let siblingStats = AdDurationStats(meanDuration: 10, sampleCount: 20)
        for i in 0..<2 {
            let sibling = makeProfile(
                podcastId: "pod-sibling-prec-\(i)",
                adDurationStatsJSON: siblingStats.encodeForTesting(),
                observationCount: 8,
                networkId: networkId
            )
            try await store.upsertProfile(sibling)
        }

        // Current show has its own observations clustered around 60s
        // and meets the show-local threshold (sampleCount >= 5,
        // observationCount >= 5).
        let ownStats = AdDurationStats(meanDuration: 60, sampleCount: 20)
        let current = makeProfile(
            podcastId: "pod-current-prec",
            adDurationStatsJSON: ownStats.encodeForTesting(),
            observationCount: 12,
            networkId: networkId
        )
        try await store.upsertProfile(current)

        let service = makeService(store: store, profile: current)
        let resolved = await service.resolveEpisodePriorsForTesting()
        #expect(resolved.activeLevel == .showLocal)
    }

    /// playhead-spxs cycle-1 L-3: pin the empty-string guard in
    /// `resolveEpisodePriors`. Production guards `!networkId.isEmpty` so
    /// a row written with `networkId == ""` (e.g. a corrupt import or a
    /// future bug that bypasses the COALESCE upsert) doesn't trigger a
    /// pointless `fetchProfiles(forNetworkId: "")` SQL fetch — and, more
    /// importantly, can't accidentally activate the network tier when
    /// no real network identity exists. Without this guard, every
    /// row with empty `networkId` would group into the same "" bucket
    /// and pollute the cross-show signal across unrelated shows.
    @Test("resolveEpisodePriors with networkId == \"\" falls back to global (no network tier)")
    func wireUpEmptyStringNetworkIdFallsBackToGlobal() async throws {
        let store = try await makeTestStore()

        // Sibling exists with empty-string networkId — the same value
        // the current show has. If the production guard regressed, the
        // resolver would group these into a "" bucket and activate the
        // network tier from this fake aggregate.
        let siblingStats = AdDurationStats(meanDuration: 10, sampleCount: 20)
        let sibling = makeProfile(
            podcastId: "pod-sibling-empty-net",
            adDurationStatsJSON: siblingStats.encodeForTesting(),
            observationCount: 8,
            networkId: ""
        )
        try await store.upsertProfile(sibling)

        let current = makeProfile(
            podcastId: "pod-current-empty-net",
            adDurationStatsJSON: nil,
            observationCount: 0,
            networkId: ""
        )
        try await store.upsertProfile(current)

        let service = makeService(store: store, profile: current)
        let resolved = await service.resolveEpisodePriorsForTesting()
        #expect(resolved.activeLevel == .global)
        #expect(resolved.typicalAdDuration == GlobalPriorDefaults.standard.typicalAdDuration)
    }

    /// playhead-spxs cycle-1 missing-test: the decay-weight contract.
    /// Once a show has accumulated >= 10 episodes of its own observations,
    /// `NetworkPriors.decayedWeight(episodesObserved:)` returns 0 and the
    /// resolver's `if let net = networkPriors, networkDecay > 0` guard
    /// skips the entire network blend. Activation must collapse back to
    /// global even though `networkPriors` is non-nil and siblings exist
    /// — the cross-show signal is no longer needed once enough self-data
    /// has accrued.
    ///
    /// Pins the load-bearing arithmetic in `decayedWeight`: a future
    /// change that swaps the formula (e.g. removes the `max(0, ...)`
    /// floor or extends decay past 10) would activate the network tier
    /// here and flip `activeLevel` away from `.global`.
    @Test("high observationCount decays network weight to 0 and falls back to global")
    func wireUpHighObservationCountDecaysNetworkTierToZero() async throws {
        let store = try await makeTestStore()
        let networkId = "pod-net-spxs-decay"

        // Network siblings cluster around 10s — the same fixture as
        // `wireUpNetworkNarrowsDurationRange`.
        let siblingStats = AdDurationStats(meanDuration: 10, sampleCount: 20)
        for i in 0..<3 {
            let sibling = makeProfile(
                podcastId: "pod-sibling-decay-\(i)",
                adDurationStatsJSON: siblingStats.encodeForTesting(),
                observationCount: 8,
                networkId: networkId
            )
            try await store.upsertProfile(sibling)
        }

        // Current show has the network identity but observationCount
        // is at the decay floor (10). The resolver computes
        // networkDecay = 0.5 * max(0, 1 - 10/10) = 0, so the network
        // blend's `networkDecay > 0` guard short-circuits.
        // adDurationStatsJSON is nil so show-local is also inactive,
        // which leaves only the global tier — the test of record.
        let current = makeProfile(
            podcastId: "pod-current-decay",
            adDurationStatsJSON: nil,
            observationCount: 10,
            networkId: networkId
        )
        try await store.upsertProfile(current)

        let service = makeService(store: store, profile: current)
        let resolved = await service.resolveEpisodePriorsForTesting()
        #expect(resolved.activeLevel == .global)
        // Resolved typicalAdDuration must equal the global default
        // exactly — no blend happened.
        #expect(resolved.typicalAdDuration == GlobalPriorDefaults.standard.typicalAdDuration)
    }

    /// playhead-spxs cycle-2 L-2: `decayedWeight(episodesObserved:)`
    /// must clamp negative inputs to 0 episodes — i.e. return the
    /// max-weight 0.5 floor, not a value > 0.5 from a misimplemented
    /// negative-arithmetic path. The production formula is
    /// `0.5 * max(0, 1 - max(0, episodesObserved) / 10.0)`; without
    /// the inner `max(episodesObserved, 0)`, a negative input would
    /// flip the linear ramp's sign and weight network priors HIGHER
    /// than the at-zero floor.
    ///
    /// This pins the inner clamp specifically — separate from the
    /// existing `wireUpHighObservationCountDecaysNetworkTierToZero`
    /// test which pins the outer `max(0, ...)` floor at 10+
    /// observations.
    @Test("decayedWeight clamps negative observationCount to 0 (= 0.5)")
    func decayedWeightClampsNegativeToZero() {
        #expect(NetworkPriors.decayedWeight(episodesObserved: -1) == 0.5)
        #expect(NetworkPriors.decayedWeight(episodesObserved: -100) == 0.5)
        // Anchor point: the same value as observationCount = 0.
        #expect(
            NetworkPriors.decayedWeight(episodesObserved: -1) ==
            NetworkPriors.decayedWeight(episodesObserved: 0)
        )
    }

    /// playhead-spxs cycle-2 L-3: NaN guard test for the
    /// `weight: max(1.0, Float(stats.sampleCount))` clamp inside
    /// `NetworkPriorsBuilder.build`. The `NetworkPriorAggregator`'s
    /// weighted-average code path divides by total weight; if every
    /// surviving snapshot reported weight 0, the divisor would be 0
    /// and the result would be NaN. The clamp ensures every snapshot
    /// contributes at least weight 1, so the aggregate stays finite.
    ///
    /// This test calls `NetworkPriorAggregator.aggregate` directly with
    /// a hand-built snapshot whose `weight: 0` so the divide-by-zero
    /// path is exercised in isolation. The aggregator's behavior under
    /// `weight: 0` confirms the producer needs the clamp — without the
    /// clamp, a future builder that propagates `weight: 0` through
    /// would silently produce NaN priors. Pair with the `min` clamp
    /// in `NetworkPriorsBuilder.build` to make the chain end-to-end
    /// safe.
    @Test("NetworkPriorAggregator.aggregate with weight=0 produces 0 (not NaN) for weighted axes")
    func aggregateWithZeroWeightSnapshotIsFinite() {
        // Single snapshot with weight: 0 — the divide-by-zero scenario
        // the builder's max(1.0, ...) clamp guards against. The
        // aggregator's `weightedAverage` returns 0 on totalWeight == 0
        // (its own short-circuit), so the resulting priors should be
        // finite and 0-valued for the weighted scalar axes — not NaN.
        let snap = ShowPriorSnapshot(
            sponsors: [:],
            slotPositions: [],
            averageAdDuration: 30,
            musicBracketRate: 0.5,
            metadataTrust: 0.5,
            weight: 0
        )
        let priors = try! #require(NetworkPriorAggregator.aggregate([snap]))
        #expect(!priors.musicBracketPrevalence.isNaN,
                "aggregator must short-circuit divide-by-zero, not produce NaN")
        #expect(!priors.metadataTrustAverage.isNaN,
                "aggregator must short-circuit divide-by-zero, not produce NaN")
    }

    /// playhead-spxs cycle-2 L-4: pin the `GlobalPriorDefaults.standard`
    /// values that interact with the network tier's blend math. Three
    /// values matter:
    ///
    ///   • `musicBracketTrust = 0.5` — matches the
    ///     `NetworkPriorsBuilder` snapshot default of 0.5, so the
    ///     blend on this axis is currently a numeric no-op for any
    ///     network of arbitrary size. Pinning this value here surfaces
    ///     a test failure if either the global default OR the snapshot
    ///     default changes — a future producer that lights up real
    ///     `musicBracketRate` data will then materially flow through.
    ///   • `metadataTrust = 0.5` — same shape as above.
    ///   • `sponsorRecurrenceExpectation = 0.3` — does NOT match the
    ///     network's derived value (which is 0 when `commonSponsors`
    ///     is empty, as it always is today). The blend therefore
    ///     materially pulls this axis toward 0 with weight
    ///     `networkDecay`. Pinning the value documents the asymmetry.
    ///
    /// The blend behavior is described in
    /// `NetworkPriorsBuilder.swift`'s file header. This test makes the
    /// load-bearing constants concrete so a refactor can't drift any
    /// of them without flipping this assertion.
    @Test("GlobalPriorDefaults.standard pins values the network tier blends against")
    func globalPriorDefaultsStandardValuesArePinned() {
        let g = GlobalPriorDefaults.standard
        #expect(g.musicBracketTrust == 0.5,
                "matches NetworkPriorsBuilder snapshot default — keeps the network blend a numeric no-op on this axis until a real producer lights up musicBracketRate")
        #expect(g.metadataTrust == 0.5,
                "matches NetworkPriorsBuilder snapshot default — same shape as musicBracketTrust")
        #expect(g.sponsorRecurrenceExpectation == 0.3,
                "differs from the network-derived 0 — the blend pulls this axis materially toward 0 with weight networkDecay")
    }

    // playhead-spxs cycle-2 M-1 (DEFERRED with architectural rationale):
    // the fall-through path when `store.fetchProfiles(forNetworkId:)`
    // THROWS (rather than returns empty) is documented in
    // `resolveEpisodePriors`'s do-catch block but is not exercised by
    // a behavioral test here.
    //
    // Why deferred: the production `AnalysisStore` is a concrete actor
    // (~50 methods, no protocol seam). Injecting a throwing mock would
    // require either:
    //   (a) introducing an `AnalysisStoreProtocol` abstraction for
    //       AdDetectionService's dependency, or
    //   (b) adding a test-only `forceCloseForTesting()` (or similar
    //       fault-injection seam) on the real store under #if DEBUG.
    // Both are architectural changes — option (a) is a wide protocol
    // surface across an actor; option (b) widens the public test
    // surface of a load-bearing persistence type. CLAUDE.md's
    // "Decision Authority" section says: "Never swap frameworks, APIs,
    // or architectural approaches without explicit approval. Present
    // the options and tradeoffs, then wait for a decision."
    //
    // The cycle-2 reviewer characterized this as "a one-stub job" but
    // that framing assumed an existing protocol. There is no such
    // protocol; `StubAnalysisStore` in PlayheadTests/Helpers/Stubs.swift
    // is a thin wrapper around the real store, not a substitutable
    // mock. The do-catch remains verified by code reading only.
    //
    // Mitigation: the catch path's only behavior is to log and continue
    // with `networkPriors = nil` / `networkDecay = 0` — both already
    // exercised by the empty-fetch path
    // (`wireUpEmptyStringNetworkIdFallsBackToGlobal` and
    // `wireUpNoNetworkIdFallsBackToGlobalForNewShows`). The branch the
    // missing test would cover is the `logger.warning` call only.
    //
    // To activate this test in a future cycle: either pick option (a)
    // or (b) above, get explicit user approval, then add a test that
    // verifies `activeLevel != .network` and no error escapes when
    // `fetchProfiles` throws.

    /// playhead-spxs: round-trip persistence for the new `networkId`
    /// column. Locks the SQL bind/read path so a future migration can't
    /// silently drop the column.
    @Test("networkId survives upsertProfile round-trip")
    func networkIdPersists() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-spxs-roundtrip"
        let seed = makeProfile(
            podcastId: podcastId,
            adDurationStatsJSON: nil,
            observationCount: 0,
            networkId: "wondery"
        )
        try await store.upsertProfile(seed)

        let fetched = try await store.fetchProfile(podcastId: podcastId)
        #expect(fetched?.networkId == "wondery")
    }

    /// playhead-spxs: COALESCE protection for `networkId`. Once a
    /// network identity is recorded, subsequent upserts that don't carry
    /// the column forward must NOT clobber the persisted value to NULL.
    /// Mirrors the cycle-1 #192 contract for `adDurationStatsJSON`.
    @Test("upsertProfile with nil networkId preserves previously-recorded value (COALESCE)")
    func upsertNilNetworkIdPreservesExisting() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-spxs-coalesce"
        let seed = makeProfile(
            podcastId: podcastId,
            adDurationStatsJSON: nil,
            observationCount: 0,
            networkId: "iheart"
        )
        try await store.upsertProfile(seed)

        // Re-upsert with networkId == nil, simulating a writer that
        // doesn't have the column in scope.
        let rebuilt = makeProfile(
            podcastId: podcastId,
            adDurationStatsJSON: nil,
            observationCount: 1,
            networkId: nil
        )
        try await store.upsertProfile(rebuilt)

        let reloaded = try await store.fetchProfile(podcastId: podcastId)
        #expect(reloaded?.networkId == "iheart")
        // Confirm the upsert applied (observationCount changed) — so
        // the COALESCE clause is what saved the column, not a no-op.
        #expect(reloaded?.observationCount == 1)
    }

    /// playhead-spxs: `fetchProfiles(forNetworkId:)` returns only profiles
    /// whose `networkId` matches the supplied id, and an empty array when
    /// no profile matches. The SQL is the only entry point the resolver
    /// uses to build a network aggregate, so a regression here would
    /// silently disable the network tier.
    @Test("fetchProfiles(forNetworkId:) filters by networkId")
    func fetchProfilesForNetworkIdFiltersCorrectly() async throws {
        let store = try await makeTestStore()
        let networkA = "spotify-studios"
        let networkB = "iheart"

        try await store.upsertProfile(makeProfile(
            podcastId: "p-A1", adDurationStatsJSON: nil,
            observationCount: 1, networkId: networkA
        ))
        try await store.upsertProfile(makeProfile(
            podcastId: "p-A2", adDurationStatsJSON: nil,
            observationCount: 2, networkId: networkA
        ))
        try await store.upsertProfile(makeProfile(
            podcastId: "p-B1", adDurationStatsJSON: nil,
            observationCount: 3, networkId: networkB
        ))
        try await store.upsertProfile(makeProfile(
            podcastId: "p-noNetwork", adDurationStatsJSON: nil,
            observationCount: 4, networkId: nil
        ))

        let aProfiles = try await store.fetchProfiles(forNetworkId: networkA)
        #expect(aProfiles.count == 2)
        let aIds = Set(aProfiles.map(\.podcastId))
        #expect(aIds == ["p-A1", "p-A2"])

        let bProfiles = try await store.fetchProfiles(forNetworkId: networkB)
        #expect(bProfiles.count == 1)
        #expect(bProfiles.first?.podcastId == "p-B1")

        // Unknown network id returns empty; nil-network profiles never
        // show up under any concrete networkId lookup.
        let cProfiles = try await store.fetchProfiles(forNetworkId: "no-such-network")
        #expect(cProfiles.isEmpty)
    }

    /// playhead-spxs cycle-2 H-2: defense-in-depth for empty-string
    /// networkId at the AnalysisStore layer. The production caller in
    /// `AdDetectionService.resolveEpisodePriors` already guards on
    /// `!networkId.isEmpty`, but a future caller, a corrupted profile
    /// row, or a refactor that bypasses the AdDetection guard would
    /// otherwise issue a `WHERE networkId = ''` SQL fetch that matches
    /// every row whose column was written as the empty string — a
    /// phantom network of mis-grouped shows.
    ///
    /// This test pins the AnalysisStore-layer early-return: even if the
    /// store contains rows with empty-string networkIds, the fetch
    /// returns `[]` and the resolver short-circuits before
    /// `NetworkPriorsBuilder` ever sees them. Pair with the wire-up
    /// test `wireUpEmptyStringNetworkIdFallsBackToGlobal`, which pins
    /// the same guard at the AdDetectionService layer — the two tests
    /// together pin both layers of the defense-in-depth.
    @Test("fetchProfiles(forNetworkId: \"\") returns empty even when \"\"-network rows exist")
    func fetchProfilesForEmptyNetworkIdReturnsEmpty() async throws {
        let store = try await makeTestStore()

        // Seed the store with two profiles whose networkId is the
        // empty string. Without the guard, fetchProfiles(forNetworkId:
        // "") would return both — the phantom-network scenario.
        try await store.upsertProfile(makeProfile(
            podcastId: "p-empty-1", adDurationStatsJSON: nil,
            observationCount: 1, networkId: ""
        ))
        try await store.upsertProfile(makeProfile(
            podcastId: "p-empty-2", adDurationStatsJSON: nil,
            observationCount: 2, networkId: ""
        ))

        // Sanity-check the seed: the empty-string value must actually
        // round-trip into the column (not get coerced to NULL by the
        // COALESCE-preserve path), or the test below would pass for
        // the wrong reason.
        let seeded = try await store.fetchProfile(podcastId: "p-empty-1")
        #expect(seeded?.networkId == "",
                "test depends on empty-string networkId surviving upsert")

        let result = try await store.fetchProfiles(forNetworkId: "")
        #expect(result.isEmpty,
                "empty-string networkId must short-circuit before the SQL bind")
    }

    /// playhead-spxs cycle-5 missing-test: pin that the current show's
    /// own profile IS included in the network aggregate.
    ///
    /// The resolver flow is: read `currentPodcastProfile.networkId`, then
    /// `fetchProfiles(forNetworkId:)`. The fetch's `WHERE networkId = ?`
    /// has no SQL-side exclusion of the current `podcastId`, so the
    /// builder ALWAYS sees the current show's own row alongside its
    /// siblings. This is the intended behavior — the network priors
    /// represent "what this network looks like at observation time,
    /// including this show," not "siblings only" — but it has no test
    /// pinning it. A future refactor that excludes the current show
    /// (e.g. `WHERE networkId = ? AND podcastId != ?`) would
    /// silently shrink the aggregate by one show; for a 2-show network
    /// with one observed show, that drops the network tier from
    /// `showCount = 2` to `showCount = 1` (the cycle-2 minShows-1
    /// fallback would still produce something, but with weaker
    /// signal). This test pins the inclusion contract.
    @Test("fetchProfiles(forNetworkId:) includes the queried show's own profile")
    func fetchProfilesIncludesSelfRow() async throws {
        let store = try await makeTestStore()
        let networkId = "self-incl-network"

        // Seed the "current show" plus one sibling — the network has
        // exactly two profiles. If the fetch ever excludes the row
        // matching some implicit "current podcastId", the count would
        // drop below 2.
        try await store.upsertProfile(makeProfile(
            podcastId: "p-current",
            adDurationStatsJSON: nil,
            observationCount: 5,
            networkId: networkId
        ))
        try await store.upsertProfile(makeProfile(
            podcastId: "p-sibling",
            adDurationStatsJSON: nil,
            observationCount: 7,
            networkId: networkId
        ))

        let profiles = try await store.fetchProfiles(forNetworkId: networkId)
        #expect(profiles.count == 2,
                "network fetch must include the current show; got \(profiles.count) rows")
        let podcastIds = Set(profiles.map(\.podcastId))
        #expect(podcastIds == ["p-current", "p-sibling"],
                "expected both rows; got \(podcastIds)")
    }

    /// playhead-spxs cycle-2 L-1: source canary that pins the
    /// documented drift between `AnalysisStore.migrate()` and
    /// `AnalysisStore.migrateOnlyForTesting()` for `podcast_profiles`
    /// columns added via direct `addColumnIfNeeded(...)` calls
    /// (i.e. NOT through the versioned `migrate*V<N>IfNeeded()` ladder).
    ///
    /// The drift is documented in-source on `migrate()` (search for
    /// "Drift note (spxs cycle-2 L-1)"). The ladder-only test seam
    /// `migrateOnlyForTesting()` intentionally does NOT replay the
    /// three direct `addColumnIfNeeded` calls (`traitProfileJSON`,
    /// `adDurationStatsJSON`, `networkId`) because the migration-ladder
    /// tests don't seed real podcast-profile rows and so don't need
    /// the new columns. This test pins that the diff is exactly
    /// `{traitProfileJSON, adDurationStatsJSON, networkId}` — and ONLY
    /// those — so a future change that adds a fourth direct
    /// `addColumnIfNeeded` for podcast_profiles to `migrate()` without
    /// mirroring it in `migrateOnlyForTesting()` will trip this canary
    /// and force the engineer to choose: either (a) replay it in the
    /// ladder seam too, or (b) extend this allow-list with a
    /// justification.
    ///
    /// Source canary (regex over `AnalysisStore.swift`) rather than
    /// behavioral test: a behavioral comparison would need to open
    /// two stores in parallel against different seed shapes — costly
    /// in test setup, and the static check is sufficient because the
    /// drift is purely a static-source asymmetry.
    @Test("migrate() vs migrateOnlyForTesting() drift on podcast_profiles direct addColumnIfNeeded calls")
    func migrateLadderDriftMatchesDocumentation() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Persistence/AnalysisStore/AnalysisStore.swift"
        )
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)

        // Inspect `runSchemaMigration()` — the private helper that
        // `migrate()` delegates to via `ensureOpen()`. The public
        // `migrate()` is a thin wrapper that just calls `ensureOpen()`,
        // so the addColumnIfNeeded calls live in `runSchemaMigration()`.
        let migrateColumns = try Self.podcastProfileAddColumnSet(
            inFunctionNamed: "runSchemaMigration",
            sourceText: source,
            strippedText: stripped
        )
        let ladderColumns = try Self.podcastProfileAddColumnSet(
            inFunctionNamed: "migrateOnlyForTesting",
            sourceText: source,
            strippedText: stripped
        )

        let drift = migrateColumns.subtracting(ladderColumns)
        // Three direct podcast_profiles `addColumnIfNeeded` calls live in
        // `migrate()` but not in `migrateOnlyForTesting()` (which only
        // mirrors the versioned ladder steps): `traitProfileJSON`
        // (ef2.5.1), `adDurationStatsJSON` (playhead-084j), and
        // `networkId` (playhead-spxs). The drift note in `migrate()`
        // documents the latter two; the canary catches the older
        // `traitProfileJSON` gap as well so the engineer renaming/adding
        // a fourth column has to update *this* allow-list deliberately.
        let documentedDrift: Set<String> = [
            "traitProfileJSON",
            "adDurationStatsJSON",
            "networkId"
        ]

        #expect(
            drift == documentedDrift,
            """
            playhead-spxs cycle-2 L-1: drift between `migrate()` and \
            `migrateOnlyForTesting()` for `podcast_profiles` direct \
            addColumnIfNeeded(...) columns has changed. Expected \
            drift: \(documentedDrift.sorted()). Actual drift: \
            \(drift.sorted()). \
            \
            If a new column was added to `migrate()` without mirroring \
            in `migrateOnlyForTesting()`, either: \
            (a) mirror the addColumnIfNeeded call in the ladder seam \
            (preferred — closes the ghost-test risk), OR \
            (b) extend this canary's `documentedDrift` set with a \
            comment justifying why the new column doesn't belong in \
            the ladder seam (e.g. "ladder tests don't seed rows that \
            bind this column"). \
            \
            Reverse drift (columns in ladder but not migrate) is also \
            wrong — every ladder-seam addColumnIfNeeded should have a \
            counterpart in production migrate() so the two paths \
            converge on the same column set. \
            \
            migrate() set: \(migrateColumns.sorted()) \
            migrateOnlyForTesting() set: \(ladderColumns.sorted())
            """
        )

        // Reverse-drift sanity check: nothing in the ladder seam that
        // isn't also in migrate(). If the ladder ever gains a column
        // that migrate() lacks, the production migrate() is missing a
        // shipped column — a worse failure mode than the forward drift.
        let reverseDrift = ladderColumns.subtracting(migrateColumns)
        #expect(
            reverseDrift.isEmpty,
            """
            playhead-spxs cycle-2 L-1: REVERSE drift detected. \
            `migrateOnlyForTesting()` adds podcast_profiles columns \
            that `migrate()` does NOT add: \(reverseDrift.sorted()). \
            Production migrate() is missing a shipped column — fix \
            migrate() to add this column too, then re-run.
            """
        )
    }

    /// Cycle-2 L-1 helper: extract the set of `podcast_profiles`
    /// columns added via direct `addColumnIfNeeded(table: "podcast_profiles", column: "<NAME>", ...)`
    /// calls inside a named function's body.
    ///
    /// Implementation: locate the function signature by literal substring
    /// `"func <name>(`", slice from there to the next top-level `func ` or
    /// to end-of-file (whichever comes first), strip comments via
    /// `SwiftSourceInspector.strippingComments` (which preserves string
    /// literals — required so the regex can match `"podcast_profiles"`),
    /// then regex-extract column names. The next-`func ` boundary is
    /// imperfect (a closure `func` reference would terminate early), but
    /// `AnalysisStore` has no such occurrence in the body of either
    /// `migrate()` or `migrateOnlyForTesting()`. Disambiguates `migrate`
    /// vs `migrateOnlyForTesting` by anchoring to `func <name>(` (the `(`
    /// rules out the longer name).
    private static func podcastProfileAddColumnSet(
        inFunctionNamed funcName: String,
        sourceText: String,
        strippedText: String
    ) throws -> Set<String> {
        let signature = "func \(funcName)("
        // Cycle-4 L-1: anchor the signature lookup on `strippedText`
        // (comments + strings blanked out) so a doc-comment mention like
        // `/// see func runSchemaMigration(` cannot beat the real
        // declaration to the first match. `strippingCommentsAndStrings`
        // preserves length character-for-character (cycle-26 M-3), so we
        // can round-trip through NSRange to obtain an index range valid in
        // `sourceText`. (Swift's `String.Index` instances are not directly
        // interchangeable across String values even when the underlying
        // bytes match, hence the explicit conversion.)
        guard let strippedSignatureRange = strippedText.range(of: signature) else {
            throw NSError(
                domain: "MigrateDriftCanary",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey:
                    "Could not locate `\(signature)` in AnalysisStore.swift"]
            )
        }
        let strippedSignatureNSRange = NSRange(strippedSignatureRange, in: strippedText)
        guard let signatureRange = Range(strippedSignatureNSRange, in: sourceText) else {
            throw NSError(
                domain: "MigrateDriftCanary",
                code: 2,
                userInfo: [NSLocalizedDescriptionKey:
                    "Could not project signature range from strippedText into sourceText for `\(signature)`"]
            )
        }

        // Slice runs from *after* the signature to the next column-4
        // method declaration (`func ` or `private func `, etc.) or
        // end-of-file. The leading `\n    ` ensures we don't accidentally
        // split on a nested `func` keyword inside a string literal or
        // doc comment; `AnalysisStore` methods are uniformly indented at
        // 4 spaces. The access-modifier alternation is required because
        // `migrateOnlyForTesting()` is followed by
        // `private func migrateSelfDescribingTitlesV15IfNeeded()` —
        // without matching `private func`, the slice would otherwise
        // wrap that helper's body and pollute the column set with
        // columns it adds (e.g. `title`).
        //
        // Cycle-3 L-1 / cycle-4 L-4: enumerate every Swift modifier that
        // can precede `func` in a class body, not just the access-modifier
        // set. A future helper declared `static func`, `nonisolated func`,
        // `final override func`, `required convenience init` (the
        // initializer modifiers travel together with `func` siblings),
        // etc. between the two ladder methods would otherwise be missed
        // by the boundary, and the slice would bleed into that helper's
        // body — silently scanning more text than the canary claims to.
        // The `{0,4}` repeat covers combinations like
        // `private static final override func` (rarely written, but
        // legal); five+ modifiers is implausible and stops the regex
        // from being unbounded.
        let bodyStart = signatureRange.upperBound
        let modifierAlternation = "(?:private|internal|public|fileprivate|open"
            + "|static|nonisolated|final|class|override|dynamic"
            + "|mutating|nonmutating|required|convenience)"
        let boundaryPattern = #"\n    (?:"# + modifierAlternation + #" ){0,4}func "#
        let boundaryRegex = try NSRegularExpression(pattern: boundaryPattern)
        let scanRange = NSRange(bodyStart..<sourceText.endIndex, in: sourceText)
        let firstBoundary = boundaryRegex.firstMatch(in: sourceText, range: scanRange)
        let bodyEnd: String.Index
        if let firstBoundary,
           let boundaryRange = Range(firstBoundary.range, in: sourceText) {
            bodyEnd = boundaryRange.lowerBound
        } else {
            bodyEnd = sourceText.endIndex
        }
        let body = String(sourceText[bodyStart..<bodyEnd])

        // Strip comments only — strings must remain intact so the regex
        // can match `"podcast_profiles"` and `"<NAME>"`.
        let bodyNoComments = SwiftSourceInspector.strippingComments(body)

        let pattern = #"addColumnIfNeeded\(\s*table:\s*"podcast_profiles"\s*,\s*column:\s*"([^"]+)""#
        let regex = try NSRegularExpression(pattern: pattern)
        let bodyNS = NSRange(bodyNoComments.startIndex..., in: bodyNoComments)
        var columns: Set<String> = []
        regex.enumerateMatches(in: bodyNoComments, range: bodyNS) { match, _, _ in
            guard let match,
                  let nameRange = Range(match.range(at: 1), in: bodyNoComments) else {
                return
            }
            columns.insert(String(bodyNoComments[nameRange]))
        }
        return columns
    }

    /// playhead-spxs cycle-3 L-5: pin that `actor NetworkPriorStore`
    /// remains a future-caching stub with no production callers.
    ///
    /// Cycle-2 marked the actor as UNUSED STUB in its docstring (see
    /// the docstring above `actor NetworkPriorStore` in
    /// `NetworkPriors.swift`) and explicitly told future callers
    /// "do not wire new call sites through this type" — but a
    /// docstring alone is advisory. This canary makes the constraint
    /// load-bearing: any non-test, non-declaration reference to
    /// `NetworkPriorStore` from production code (under `Playhead/`,
    /// excluding the file that *declares* the actor) trips the test and
    /// forces the engineer to either delete the unused stub or
    /// document the reactivation deliberately.
    ///
    /// The production path goes through `NetworkPriorsBuilder.build(...)`
    /// (called from `AdDetectionService.resolveEpisodePriors`); a parallel
    /// path through this in-memory cache would diverge from the SQL-
    /// backed `fetchProfiles(forNetworkId:)` source of truth and create
    /// a memoization staleness problem before any profile shows it's
    /// needed.
    @Test("NetworkPriorStore actor has no production callers (cycle-3 L-5)")
    func networkPriorStoreHasNoProductionCallers() throws {
        let thisFile = URL(fileURLWithPath: #filePath)
        // .../PlayheadTests/Services/AdDetection/PriorHierarchyWireUpTests.swift
        //   -> AdDetection -> Services -> PlayheadTests -> repo root
        let repoRoot = thisFile
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let appRoot = repoRoot.appendingPathComponent("Playhead", isDirectory: true)

        // The single source-of-truth declaration. Other production files
        // that match this exact suffix are the only ones allowed to
        // reference `NetworkPriorStore` — and that file does so
        // exclusively in its own type declaration.
        let declarationFileSuffix = "/Services/AdDetection/NetworkPriors.swift"

        let fm = FileManager.default
        guard let enumerator = fm.enumerator(
            at: appRoot,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            Issue.record("Could not enumerate \(appRoot.path)")
            return
        }

        var offendingReferences: [String] = []
        for case let url as URL in enumerator {
            guard url.pathExtension == "swift" else { continue }
            // Skip the declaring file — it legitimately mentions the
            // type in its `actor NetworkPriorStore { ... }` declaration.
            if url.path.hasSuffix(declarationFileSuffix) { continue }

            let source = try String(contentsOf: url, encoding: .utf8)
            // Strip comments AND strings: we want to catch real call
            // sites, not mentions in headers / strings / doc comments.
            let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
            guard stripped.contains("NetworkPriorStore") else { continue }

            offendingReferences.append(
                url.path.replacingOccurrences(of: repoRoot.path + "/", with: "")
            )
        }

        #expect(
            offendingReferences.isEmpty,
            """
            playhead-spxs cycle-3 L-5: production code now references \
            `NetworkPriorStore` outside its declaring file. The actor is \
            an UNUSED STUB intentionally kept for future caching; the \
            production path runs through `NetworkPriorsBuilder.build(...)`. \
            If you genuinely need the in-memory cache, update this canary \
            with the new call sites AND the docstring on \
            `actor NetworkPriorStore` (currently warns "do not wire new \
            call sites through this type"); otherwise remove the call \
            site.

            Offending files: \(offendingReferences.sorted())
            """
        )
    }

    /// playhead-spxs cycle-4 M-2 positive control for the C3 L-5
    /// canary: prove the
    /// `strippingCommentsAndStrings(...).contains("NetworkPriorStore")`
    /// predicate ACTUALLY flags a real call site. Without this control,
    /// a future refactor of `strippingCommentsAndStrings` (e.g. one
    /// that over-strips identifiers, accidentally drops `NetworkPriorStore`
    /// tokens, or returns an empty string on certain inputs) would let
    /// the canary go silently green even against a regressed source.
    ///
    /// The control runs the exact predicate the production canary uses
    /// — strip, then `.contains` — against a synthetic Swift source
    /// where `NetworkPriorStore` appears as a real reference (not in a
    /// comment, not in a string), and asserts the predicate returns
    /// `true`. We also pin a negative shape: the same identifier
    /// embedded *only* in a `//` comment and a `"…"` string literal
    /// must be filtered away by the stripper.
    @Test("NetworkPriorStore canary predicate fires on synthetic call site (cycle-4 M-2)")
    func networkPriorStoreCanaryPredicateFiresOnSyntheticCallSite() {
        // Real reference: instantiating the actor in a function body.
        // After comment + string stripping, the identifier MUST remain.
        let regressionFixture = """
        import Foundation

        @MainActor
        final class SomeProductionType {
            func wireUp() async {
                let store = NetworkPriorStore()
                await store.fetch()
            }
        }
        """
        let strippedReal = SwiftSourceInspector.strippingCommentsAndStrings(regressionFixture)
        #expect(
            strippedReal.contains("NetworkPriorStore"),
            """
            playhead-spxs cycle-4 M-2 positive control failure: the C3 \
            L-5 canary's predicate (`strippingCommentsAndStrings(source) \
            .contains("NetworkPriorStore")`) did NOT flag a synthetic \
            source whose function body instantiates the actor. The \
            stripper is now over-aggressive — it is dropping bare \
            identifiers — which means the production canary has gone \
            blind. Audit `strippingCommentsAndStrings` and either \
            tighten its scope to comments + strings only or replace \
            this predicate with a regex that does not depend on the \
            stripper.
            """
        )

        // Negative shape: identifier only inside a comment + string.
        // The stripper must remove BOTH so the predicate returns false.
        // If this side fails, the canary will *over*-trip on benign
        // mentions in headers / log messages.
        let benignMentionsFixture = """
        // This comment mentions NetworkPriorStore but it is not a call site.
        let docs = "See NetworkPriorStore in NetworkPriors.swift for details."
        """
        let strippedBenign = SwiftSourceInspector.strippingCommentsAndStrings(benignMentionsFixture)
        #expect(
            !strippedBenign.contains("NetworkPriorStore"),
            """
            playhead-spxs cycle-4 M-2 negative control failure: the C3 \
            L-5 canary's predicate FLAGGED a fixture where \
            `NetworkPriorStore` appears ONLY inside a `//` comment and a \
            `"…"` string literal. The stripper is now under-aggressive \
            — it is leaving comment / string identifiers in place — \
            which means the production canary will trip on doc-comment \
            mentions and log-line strings even when production code is \
            clean. Audit `strippingCommentsAndStrings`.
            """
        )
    }

    /// playhead-spxs cycle-3 M-1: pin that `podcast_profiles.networkId`
    /// has a SQL index. `fetchProfiles(forNetworkId:)` runs
    /// `WHERE networkId = ?` on every `resolveEpisodePriors()` call;
    /// without the index this is a full table scan that grows linearly
    /// with profile-row count. The index is created by
    /// `runSchemaMigration()` next to the column add.
    ///
    /// Source canary (regex over `AnalysisStore.swift`) rather than
    /// behavioral test: `sqlite_master` would also work but the index
    /// existence is a static-source contract — and a behavioral test
    /// that opens a store + queries `sqlite_master` is heavier than a
    /// regex match on the migration SQL.
    @Test("podcast_profiles.networkId has SQL index in runSchemaMigration (cycle-3 M-1)")
    func networkIdColumnHasSqlIndex() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Persistence/AnalysisStore/AnalysisStore.swift"
        )
        // cycle-5 L-1: require the partial WHERE clause. Most rows in
        // `podcast_profiles` have NULL `networkId` (the column was added
        // late and most shows have no recorded network); a partial index
        // keyed on `WHERE networkId IS NOT NULL` skips those NULL rows
        // entirely, halving index size in the typical library. A
        // regression that drops the partial clause would leave the index
        // bloated with NULL entries the planner cannot use anyway, so
        // this canary anchors on the full DDL shape.
        let pattern = #"CREATE INDEX IF NOT EXISTS idx_podcast_profiles_networkId\s+ON podcast_profiles\(networkId\)\s+WHERE networkId IS NOT NULL"#
        let regex = try NSRegularExpression(pattern: pattern)
        let nsRange = NSRange(source.startIndex..., in: source)
        let matches = regex.numberOfMatches(in: source, range: nsRange)
        #expect(
            matches == 1,
            """
            playhead-spxs cycle-3 M-1 / cycle-5 L-1: expected exactly one \
            `CREATE INDEX IF NOT EXISTS idx_podcast_profiles_networkId \
            ON podcast_profiles(networkId) WHERE networkId IS NOT NULL` \
            in AnalysisStore.swift; found \(matches). Without the index, \
            every `resolveEpisodePriors()` does a full scan of \
            `podcast_profiles`, becoming O(n) in library size. The \
            partial WHERE is required so the index isn't bloated with \
            NULL entries the planner can't use anyway. If the index \
            name has been renamed, update this canary AND verify the \
            new index actually covers the (networkId) lookup with the \
            same partial-WHERE shape.
            """
        )
    }

    /// playhead-spxs cycle-4 L-2: behavioral complement to the cycle-3
    /// M-1 source canary. Source canary pins that the `CREATE INDEX`
    /// statement EXISTS in production; this test pins that SQLite's
    /// query planner ACTUALLY USES the index for the production query
    /// shape (`SELECT … FROM podcast_profiles WHERE networkId = ?`).
    ///
    /// Why both: a partial index (`WHERE networkId IS NOT NULL`) is
    /// only consulted when the planner can prove the query predicate
    /// implies the partial. A future refactor that changes the production
    /// SQL to e.g. `WHERE networkId IS ?` (NULL-tolerant) or
    /// `WHERE networkId = ? OR networkId IS NULL` would silently bypass
    /// the index — the source canary would still pass (the index still
    /// exists) but the production read would scan the table.
    ///
    /// Test strategy: open a fresh in-memory SQLite database (avoids
    /// any AnalysisStore instantiation cost), apply just the schema
    /// shape that matters for the index — `podcast_profiles(networkId)`
    /// — recreate the partial index with the exact production DDL,
    /// then run `EXPLAIN QUERY PLAN` against the production query
    /// shape. Assert the plan output mentions `idx_podcast_profiles_networkId`.
    @Test("EXPLAIN QUERY PLAN for fetchProfiles(forNetworkId:) uses idx_podcast_profiles_networkId (cycle-4 L-2)")
    func explainQueryPlanUsesNetworkIdIndex() throws {
        var db: OpaquePointer?
        let openRC = sqlite3_open_v2(
            ":memory:",
            &db,
            SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE,
            nil
        )
        #expect(openRC == SQLITE_OK)
        defer { if let db { sqlite3_close(db) } }

        // Minimal schema shape: only what the index needs. Production
        // `podcast_profiles` has many more columns, but the planner's
        // partial-index decision turns on (a) the indexed column type
        // and (b) the partial WHERE clause matching the query predicate.
        let ddl = """
            CREATE TABLE podcast_profiles (
                podcastId TEXT PRIMARY KEY,
                networkId TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_podcast_profiles_networkId
                ON podcast_profiles(networkId)
                WHERE networkId IS NOT NULL;
            """
        var errMsg: UnsafeMutablePointer<CChar>?
        let execRC = sqlite3_exec(db, ddl, nil, nil, &errMsg)
        #expect(execRC == SQLITE_OK, "DDL exec failed: \(errMsg.map { String(cString: $0) } ?? "")")

        // Production query shape — pull from
        // `AnalysisStore.fetchProfiles(forNetworkId:)`. We mirror the
        // `WHERE networkId = ?` predicate; column list is irrelevant
        // to the planner's index decision so we use `*`.
        let plan = try collectExplainQueryPlan(
            db: db,
            sql: "EXPLAIN QUERY PLAN SELECT * FROM podcast_profiles WHERE networkId = ?"
        )

        // The planner's text contains either "USING INDEX" or
        // "USING COVERING INDEX" followed by the index name. We just
        // need the index name to appear somewhere in the plan rows.
        let combined = plan.joined(separator: "\n")
        #expect(
            combined.contains("idx_podcast_profiles_networkId"),
            """
            playhead-spxs cycle-4 L-2: SQLite's query planner did NOT \
            choose `idx_podcast_profiles_networkId` for the production \
            `WHERE networkId = ?` shape. The partial index \
            (`WHERE networkId IS NOT NULL`) is only used when the query \
            predicate implies the partial — which `=` does (NULL is \
            never equal to anything in SQL). If the production query \
            has been refactored to a NULL-tolerant predicate (e.g. \
            `IS ?`), restore the equality predicate or drop the \
            partial WHERE clause from the index. The cycle-3 M-1 \
            source canary alone is no longer sufficient — it pins the \
            index DDL, not its use.

            EXPLAIN QUERY PLAN output:
            \(plan.joined(separator: "\\n"))
            """
        )
    }

    /// playhead-spxs cycle-6 missing-test (residual L-2 gap): source
    /// canary that pins the snapshot-consistency invariant in
    /// `resolveEpisodePriors`. The cycle-5 L-2 fix snapshots
    /// `observationCount` from `currentPodcastProfile` BEFORE the
    /// `store.fetchProfiles(forNetworkId:)` await so that an interleaving
    /// actor turn that mutates `currentPodcastProfile` cannot corrupt
    /// the decay weight. A behavioral test for actor reentry timing
    /// would be inherently flaky; instead, this canary asserts the
    /// pre-await capture pattern at the source level — a regression
    /// that moves the `observationCount` read to AFTER the await would
    /// flip the canary.
    ///
    /// Test strategy: load `AdDetectionService.swift`, locate the
    /// `resolveEpisodePriors` body, and verify that the literal
    /// `let observedAtSnapshot = snapshotProfile.observationCount`
    /// appears textually BEFORE the `try await store.fetchProfiles`
    /// line within the same function body.
    @Test("resolveEpisodePriors snapshots observationCount before fetchProfiles await (cycle-6)")
    func resolveEpisodePriorsSnapshotsObservationCountPreAwait() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AdDetectionService.swift"
        )

        // Slice from the resolver method signature to the next top-level
        // `func ` (or end of file). Mirrors the boundary heuristic used
        // by `podcastProfileAddColumnSet`.
        let signature = "func resolveEpisodePriors("
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
        guard let strippedSigRange = stripped.range(of: signature) else {
            Issue.record("Could not locate `\(signature)` in AdDetectionService.swift")
            return
        }
        let strippedSigNS = NSRange(strippedSigRange, in: stripped)
        guard let signatureRange = Range(strippedSigNS, in: source) else {
            Issue.record("Could not project signature range from strippedText into sourceText")
            return
        }
        let bodyStart = signatureRange.upperBound
        let modifierAlternation = "(?:private|internal|public|fileprivate|open"
            + "|static|nonisolated|final|class|override|dynamic"
            + "|mutating|nonmutating|required|convenience)"
        let boundaryPattern = #"\n    (?:"# + modifierAlternation + #" ){0,4}func "#
        let boundaryRegex = try NSRegularExpression(pattern: boundaryPattern)
        let scanRange = NSRange(bodyStart..<source.endIndex, in: source)
        let firstBoundary = boundaryRegex.firstMatch(in: source, range: scanRange)
        let bodyEnd: String.Index
        if let firstBoundary,
           let boundaryRange = Range(firstBoundary.range, in: source) {
            bodyEnd = boundaryRange.lowerBound
        } else {
            bodyEnd = source.endIndex
        }
        let rawBody = String(source[bodyStart..<bodyEnd])
        // cycle-7 L-1: search the comment/string-stripped form so a
        // future refactor that quotes the snapshot literal in a doc
        // comment above an awaited fetch can't satisfy the ordering
        // check. Stripping only blanks comment/string contents — code
        // anchors are preserved verbatim, so anchor ordering inside
        // `body` reflects the true source ordering.
        let body = SwiftSourceInspector.strippingCommentsAndStrings(rawBody)

        // The pre-await snapshot anchor and the await line. The snapshot
        // line must come strictly before the await.
        let snapshotAnchor = "let observedAtSnapshot = snapshotProfile.observationCount"
        let awaitAnchor = "try await store.fetchProfiles(forNetworkId:"

        guard let snapshotIdx = body.range(of: snapshotAnchor)?.lowerBound else {
            Issue.record(
                """
                playhead-spxs cycle-6 (cycle-5 L-2 residual): expected \
                `\(snapshotAnchor)` in `resolveEpisodePriors`. The L-2 fix \
                requires snapshotting `observationCount` into a local \
                BEFORE the `fetchProfiles` await so an interleaving \
                actor turn can't corrupt the decay weight. If the \
                snapshot variable was renamed, update this canary to \
                match.
                """
            )
            return
        }
        guard let awaitIdx = body.range(of: awaitAnchor)?.lowerBound else {
            Issue.record(
                """
                playhead-spxs cycle-6 (cycle-5 L-2 residual): expected \
                `\(awaitAnchor)` in `resolveEpisodePriors`. If the \
                fetch call shape was refactored, update this canary.
                """
            )
            return
        }

        #expect(
            snapshotIdx < awaitIdx,
            """
            playhead-spxs cycle-6 (cycle-5 L-2 residual): \
            `observationCount` snapshot must appear BEFORE the \
            `fetchProfiles` await. Found snapshot at offset \
            \(body.distance(from: body.startIndex, to: snapshotIdx)) \
            and await at offset \
            \(body.distance(from: body.startIndex, to: awaitIdx)). \
            Moving the snapshot AFTER the await re-introduces the \
            reentrancy hazard the L-2 fix closed.
            """
        )
    }

    /// playhead-spxs cycle-7 L-1 positive control for the cycle-6
    /// canary: prove that
    /// `strippingCommentsAndStrings(...)` actually removes the
    /// snapshot literal when it appears ONLY inside a `//` comment
    /// or a `"…"` string. Without this control, a future regression
    /// in the stripper would let a refactor that quotes
    /// `let observedAtSnapshot = snapshotProfile.observationCount`
    /// in a doc comment ABOVE an awaited fetch falsely satisfy the
    /// cycle-6 canary's ordering predicate.
    @Test("Pre-await snapshot canary stripper survives comment-spoof (cycle-7 L-1)")
    func preAwaitSnapshotCanaryStripperSurvivesCommentSpoof() {
        // Positive control: real source pattern (snapshot precedes await
        // in code, no comment spoofing). Stripping must preserve both
        // anchors so the cycle-6 canary still finds them.
        let realFixture = """
        do {
            let observedAtSnapshot = snapshotProfile.observationCount
            let siblings = try await store.fetchProfiles(forNetworkId: networkId)
            _ = (observedAtSnapshot, siblings)
        }
        """
        let strippedReal = SwiftSourceInspector.strippingCommentsAndStrings(realFixture)
        #expect(
            strippedReal.contains("let observedAtSnapshot = snapshotProfile.observationCount"),
            """
            playhead-spxs cycle-7 L-1 positive control failure: \
            `strippingCommentsAndStrings` dropped the snapshot literal \
            from a real code body. The stripper has gone over-aggressive \
            on identifiers, which would make the cycle-6 canary blind to \
            real regressions.
            """
        )
        #expect(
            strippedReal.contains("try await store.fetchProfiles(forNetworkId:"),
            """
            playhead-spxs cycle-7 L-1 positive control failure: \
            `strippingCommentsAndStrings` dropped the await literal from \
            a real code body. The stripper has gone over-aggressive on \
            identifiers, which would make the cycle-6 canary blind to \
            real regressions.
            """
        )

        // Negative control: spoof fixture where the snapshot literal
        // appears ONLY in a doc comment ABOVE the await, with no real
        // pre-await read. Stripping must remove the commented-out
        // literal so the cycle-6 canary's `<` ordering check fails on
        // this shape (rather than passing on a comment).
        let spoofFixture = """
        do {
            // let observedAtSnapshot = snapshotProfile.observationCount
            let siblings = try await store.fetchProfiles(forNetworkId: networkId)
            let observedAtSnapshot = snapshotProfile.observationCount
            _ = (observedAtSnapshot, siblings)
        }
        """
        let strippedSpoof = SwiftSourceInspector.strippingCommentsAndStrings(spoofFixture)
        let snapshotIdx = strippedSpoof.range(of: "let observedAtSnapshot = snapshotProfile.observationCount")?.lowerBound
        let awaitIdx = strippedSpoof.range(of: "try await store.fetchProfiles(forNetworkId:")?.lowerBound
        #expect(
            snapshotIdx != nil && awaitIdx != nil,
            "spoof fixture should still contain both anchors after stripping"
        )
        if let snapshotIdx, let awaitIdx {
            #expect(
                snapshotIdx > awaitIdx,
                """
                playhead-spxs cycle-7 L-1 negative control failure: in the \
                spoof fixture the only real `observedAtSnapshot` read is \
                AFTER the await; the stripper must have removed the \
                commented-out literal so that the cycle-6 canary's \
                pre-await ordering check correctly fails. Instead, the \
                stripper preserved the comment, which means a future \
                refactor could pass the cycle-6 canary while still \
                violating snapshot consistency.
                """
            )
        }
    }

    /// playhead-spxs cycle-8 missing-test: source canary on the
    /// graceful-degradation contract for the network tier's
    /// `fetchProfiles(forNetworkId:)` call. When the SQL hop throws,
    /// `resolveEpisodePriors` MUST log and fall through with the
    /// network-tier locals at their defaults (`networkPriors == nil`,
    /// `networkDecay == 0`) — never propagating the error to callers.
    /// Otherwise a transient store failure cascades into a global
    /// ad-detection outage, since this resolver is on the hot path
    /// for every episode start.
    ///
    /// A behavioral test would require injecting a throwing
    /// `AnalysisStore` stub through the actor's persistence
    /// interface, which the current architecture doesn't expose.
    /// This canary asserts the structural invariants at the source
    /// level instead. Three checks:
    ///   1. `do {` precedes the `fetchProfiles` await (the call is
    ///      wrapped, not a bare `try await` that escapes the actor).
    ///   2. `} catch` follows the await (errors are caught locally).
    ///   3. The catch body calls `logger.warning(` AND does NOT
    ///      `throw`, assign to `networkPriors`, or assign to
    ///      `networkDecay` — the locals must retain their pre-`do`
    ///      defaults so the resolver falls through cleanly.
    @Test("resolveEpisodePriors fetchProfiles error falls through to log-and-continue (cycle-8 missing-test)")
    func resolveEpisodePriorsCatchLogsAndFallsThrough() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AdDetectionService.swift"
        )
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
        let signature = "func resolveEpisodePriors("
        guard let sigRange = stripped.range(of: signature) else {
            Issue.record("could not locate `\(signature)` in AdDetectionService.swift")
            return
        }
        let bodyStart = sigRange.upperBound
        let modifierAlternation = "(?:private|internal|public|fileprivate|open"
            + "|static|nonisolated|final|class|override|dynamic"
            + "|mutating|nonmutating|required|convenience)"
        let boundaryPattern = #"\n    (?:"# + modifierAlternation + #" ){0,4}func "#
        let boundaryRegex = try NSRegularExpression(pattern: boundaryPattern)
        let scanRange = NSRange(bodyStart..<stripped.endIndex, in: stripped)
        let firstBoundary = boundaryRegex.firstMatch(in: stripped, range: scanRange)
        let bodyEnd: String.Index
        if let firstBoundary, let r = Range(firstBoundary.range, in: stripped) {
            bodyEnd = r.lowerBound
        } else {
            bodyEnd = stripped.endIndex
        }
        let body = String(stripped[bodyStart..<bodyEnd])

        let awaitAnchor = "try await store.fetchProfiles(forNetworkId:"
        guard let awaitRange = body.range(of: awaitAnchor) else {
            Issue.record(
                """
                playhead-spxs cycle-8 missing-test: expected \
                `\(awaitAnchor)` in `resolveEpisodePriors`. If the \
                fetch call shape was refactored, update this canary.
                """
            )
            return
        }

        // Step 1: `do {` must precede the await — the call must be
        // wrapped, not a bare `try await` that propagates errors.
        let preAwait = body[..<awaitRange.lowerBound]
        #expect(
            preAwait.range(of: "do {", options: .backwards) != nil,
            """
            playhead-spxs cycle-8 missing-test: expected `do {` \
            before the `fetchProfiles(forNetworkId:)` await. The \
            graceful-degradation contract requires the SQL hop to \
            throw into a local catch so a transient store failure \
            falls through to network-tier-disabled, not all the \
            way up the resolver's stack.
            """
        )

        // Step 2: `} catch` must follow the await.
        let postAwait = body[awaitRange.upperBound...]
        guard let catchOpenRange = postAwait.range(of: "} catch") else {
            Issue.record(
                """
                playhead-spxs cycle-8 missing-test: expected `} catch` \
                after the `fetchProfiles(forNetworkId:)` await — \
                without it, a thrown SQL error escapes the resolver \
                entirely and the network tier becomes a hot-path \
                liability instead of best-effort augmentation.
                """
            )
            return
        }

        // Step 3: extract the catch body (from the brace after
        // `catch` to the matching `}`). The current catch is
        // single-statement with no nested braces, so the next `}`
        // closes it.
        let afterCatch = postAwait[catchOpenRange.upperBound...]
        guard let catchOpenBrace = afterCatch.range(of: "{") else {
            Issue.record("could not find `{` opening the catch body")
            return
        }
        let catchInteriorStart = catchOpenBrace.upperBound
        guard let catchCloseBrace = afterCatch[catchInteriorStart...].range(of: "}") else {
            Issue.record("could not find `}` closing the catch body")
            return
        }
        let catchInterior = String(afterCatch[catchInteriorStart..<catchCloseBrace.lowerBound])

        // Step 3a: catch logs (graceful degradation, not silent swallow).
        #expect(
            catchInterior.contains("logger.warning("),
            """
            playhead-spxs cycle-8 missing-test: expected \
            `logger.warning(` inside the `fetchProfiles` catch. \
            Removing the log turns network-tier failures invisible \
            — at minimum an `os_log`/`.warning` must remain so \
            on-device debugging can observe the fall-through path.
            """
        )

        // Step 3b: catch must not rethrow or assign the network-tier
        // locals — those defaults (`nil` / `0`) are what produce the
        // graceful resolver fall-through to trait+showLocal+global.
        let forbiddenInCatch: [(needle: String, why: String)] = [
            ("throw ", "rethrowing escalates a single-show transient SQL miss into a global resolver failure"),
            ("networkPriors =", "assigning here breaks the contract that fetch failure leaves the network tier disabled (nil)"),
            ("networkDecay =", "assigning here breaks the contract that fetch failure leaves the network tier disabled (0)"),
        ]
        for (needle, why) in forbiddenInCatch {
            #expect(
                !catchInterior.contains(needle),
                """
                playhead-spxs cycle-8 missing-test: catch body for \
                `fetchProfiles(forNetworkId:)` must not contain \
                `\(needle)`. \(why). Catch interior was: \
                \(catchInterior)
                """
            )
        }
    }

    /// Helper for cycle-4 L-2: collect the plain-text rows of an
    /// `EXPLAIN QUERY PLAN` statement.
    private func collectExplainQueryPlan(
        db: OpaquePointer?,
        sql: String
    ) throws -> [String] {
        var stmt: OpaquePointer?
        let prepareRC = sqlite3_prepare_v2(db, sql, -1, &stmt, nil)
        guard prepareRC == SQLITE_OK else {
            throw NSError(
                domain: "ExplainQueryPlanCanary",
                code: Int(prepareRC),
                userInfo: [NSLocalizedDescriptionKey:
                    "sqlite3_prepare_v2 failed (\(prepareRC)) for `\(sql)`"]
            )
        }
        defer { sqlite3_finalize(stmt) }

        // EXPLAIN QUERY PLAN columns: id, parent, notused, detail
        var rows: [String] = []
        while sqlite3_step(stmt) == SQLITE_ROW {
            let detailCol = 3
            if let cstr = sqlite3_column_text(stmt, Int32(detailCol)) {
                rows.append(String(cString: cstr))
            }
        }
        return rows
    }

    // MARK: - PodcastProfile.adDurationStatsJSON column round-trip

    @Test("adDurationStatsJSON survives upsertProfile round-trip")
    func adDurationStatsJSONPersists() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-stats-persist-1"
        let stats = AdDurationStats(meanDuration: 42, sampleCount: 17)
        let json = stats.encodeForTesting()
        let seed = makeProfile(
            podcastId: podcastId,
            adDurationStatsJSON: json,
            observationCount: 8
        )
        try await store.upsertProfile(seed)

        let fetched = try await store.fetchProfile(podcastId: podcastId)
        #expect(fetched?.adDurationStatsJSON == json)
    }

    /// cycle-1 #192: COALESCE no-op pin for `adDurationStatsJSON`.
    ///
    /// `upsertProfile` wraps the column in
    /// `COALESCE(excluded.adDurationStatsJSON, podcast_profiles.adDurationStatsJSON)`
    /// so that a profile rebuild that doesn't carry the stats forward
    /// (e.g. trust-scoring, which builds a profile without this column
    /// in scope) preserves the previously-recorded aggregate rather
    /// than clobbering it with NULL. Without the COALESCE clause the
    /// per-show ad-duration mean would be wiped on every trust-score
    /// recompute, defeating the point of persisting it across launches.
    /// This test pins the contract end-to-end: seed → upsert with
    /// nil → reload still has the seed's stats.
    @Test("upsertProfile with nil adDurationStatsJSON preserves previously-recorded aggregate (COALESCE)")
    func upsertNilAdDurationStatsPreservesExisting() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-coalesce-stats"
        let stats = AdDurationStats(meanDuration: 27.5, sampleCount: 11)
        let json = stats.encodeForTesting()
        let seed = makeProfile(
            podcastId: podcastId,
            adDurationStatsJSON: json,
            observationCount: 4
        )
        try await store.upsertProfile(seed)

        // Re-upsert with adDurationStatsJSON: nil (e.g. trust-scoring
        // rebuilding the profile without this column in scope).
        let rebuilt = makeProfile(
            podcastId: podcastId,
            adDurationStatsJSON: nil,
            observationCount: 5
        )
        try await store.upsertProfile(rebuilt)

        let reloaded = try await store.fetchProfile(podcastId: podcastId)
        #expect(reloaded?.adDurationStatsJSON == json)
        // Confirms the upsert applied (observationCount changed) — so
        // the COALESCE clause is what saved the aggregate, not a
        // silent no-op upsert.
        #expect(reloaded?.observationCount == 5)
    }

    /// playhead-v7v8: After `updatePriorsForTesting` runs three times for
    /// the same show, the persisted `traitProfileJSON` must decode to a
    /// `ShowTraitProfile` whose `episodesObserved` >= 3, which in turn
    /// flips `traitProfile.isReliable` to true. The resolver wired into
    /// `resolveEpisodePriorsForTesting()` reads that profile and reports
    /// `ResolvedPriors.activeLevel == .traitDerived`. This test fails if
    /// the producer or persistence step is missing — the canonical
    /// failure mode that the bead is designed to close.
    @Test("updatePriors traits feed traitDerived activeLevel after 3 episodes")
    func updatePriorsActivatesTraitDerivedTier() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-v7v8-trait-derived-1"
        let assetId = "asset-v7v8-trait-derived-1"
        try await store.insertAsset(makeAsset(id: assetId, episodeId: "ep-v7v8-1"))

        // Seed a starter profile so the create-branch isn't exercised
        // (the resolver works either way; this just keeps the loop
        // exclusively on the update path so the EMA increment is
        // observable from episode 1).
        let seed = makeProfile(
            podcastId: podcastId,
            adDurationStatsJSON: nil,
            observationCount: 0
        )
        try await store.upsertProfile(seed)

        // Drive three "episodes" through updatePriors. Each call provides
        // distinct confirmed ad windows so the snapshot derivations have
        // signal (rather than landing on the no-signal neutral defaults).
        for episodeIndex in 0..<3 {
            let window = makeAdWindow(
                id: "win-v7v8-\(episodeIndex)",
                assetId: assetId,
                startTime: Double(episodeIndex) * 100,
                endTime: Double(episodeIndex) * 100 + 30
            )
            // Reload the profile each iteration so the service's snapshot
            // mirrors what runBackfill would carry forward.
            let current = try await store.fetchProfile(podcastId: podcastId)
            let service = makeService(store: store, profile: current)
            try await service.updatePriorsForTesting(
                podcastId: podcastId,
                nonSuppressedWindows: [window],
                episodeDuration: 600,
                // cycle-1 L4: this test only verifies that
                // `episodesObserved` increments enough to flip
                // `isReliable`, not that real signals shape the trait
                // values. The companion test
                // `updatePriorsActivatesTraitDerivedTierWithRealSignal`
                // exercises the producer math with non-empty inputs.
                featureWindows: [],
                chunks: []
            )
        }

        // Direct read of the persisted profile: episodesObserved should
        // now be >= 3, which is the gate `ShowTraitProfile.isReliable`
        // checks. Without the v7v8 producer + persistence wire-up,
        // `traitProfileJSON` would be nil and `traitProfile` would
        // decode as `.unknown` (episodesObserved == 0).
        let after = try #require(await store.fetchProfile(podcastId: podcastId))
        let traitJSON = try #require(
            after.traitProfileJSON,
            "Expected traitProfileJSON to be persisted after 3 updatePriors calls"
        )
        let trait = try JSONDecoder().decode(
            ShowTraitProfile.self,
            from: Data(traitJSON.utf8)
        )
        #expect(trait.episodesObserved >= 3)
        #expect(trait.isReliable)

        // End-to-end resolver assertion: the service's resolver entry
        // point must report `.traitDerived` as the dominant level when
        // it sees this profile.
        let service = makeService(store: store, profile: after)
        let resolved = await service.resolveEpisodePriorsForTesting()
        #expect(resolved.activeLevel == .traitDerived)
    }

    /// cycle-1 L4 companion: drive `updatePriorsForTesting` with realistic
    /// non-empty `featureWindows` (high `musicProbability`) so the trait
    /// snapshot derivations actually run on real signal — not the
    /// no-signal neutral defaults that the ergonomic-default version of
    /// `updatePriorsForTesting` silently fed in. After 3 episodes the
    /// resolved profile must have `musicDensity > 0.5`, locking in that
    /// the producer math reaches `traitProfileJSON` end-to-end. Without
    /// this gate, a future refactor could disconnect the producer from
    /// the persisted profile and the existing
    /// `updatePriorsActivatesTraitDerivedTier` test would still pass
    /// because the EMA-on-empty path increments `episodesObserved` from
    /// the neutral 0.5 defaults.
    @Test("updatePriors with real feature signal flows trait values into traitProfileJSON")
    func updatePriorsActivatesTraitDerivedTierWithRealSignal() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-v7v8-trait-real-signal-1"
        let assetId = "asset-v7v8-trait-real-signal-1"
        try await store.insertAsset(makeAsset(id: assetId, episodeId: "ep-v7v8-real-1"))

        // Realistic feature signal: 5 windows in a row, each with a
        // high `musicProbability` of 0.8. The producer's
        // `deriveMusicDensity` averages these → trait snapshot's
        // `musicDensity` ≈ 0.8. EMA-blended over 3 episodes against
        // identical inputs converges to 0.8 (the first-episode branch
        // replaces the sentinel directly; subsequent EMA passes with
        // alpha=0.3 against the same target leave the value stationary).
        let highMusicWindows: [FeatureWindow] = (0..<5).map { i in
            makeFeatureWindow(
                assetId: assetId,
                startTime: Double(i),
                endTime: Double(i + 1),
                musicProbability: 0.8
            )
        }
        let chunks: [TranscriptChunk] = [
            makeChunk(assetId: assetId, chunkIndex: 0, start: 0, end: 5, speakerId: 1),
            makeChunk(assetId: assetId, chunkIndex: 1, start: 5, end: 10, speakerId: 2)
        ]

        for episodeIndex in 0..<3 {
            let window = makeAdWindow(
                id: "win-real-\(episodeIndex)",
                assetId: assetId,
                startTime: Double(episodeIndex) * 100,
                endTime: Double(episodeIndex) * 100 + 30
            )
            let current = try await store.fetchProfile(podcastId: podcastId)
            let service = makeService(store: store, profile: current)
            try await service.updatePriorsForTesting(
                podcastId: podcastId,
                nonSuppressedWindows: [window],
                episodeDuration: 600,
                featureWindows: highMusicWindows,
                chunks: chunks
            )
        }

        let after = try #require(await store.fetchProfile(podcastId: podcastId))
        let traitJSON = try #require(after.traitProfileJSON)
        let trait = try JSONDecoder().decode(
            ShowTraitProfile.self,
            from: Data(traitJSON.utf8)
        )
        #expect(trait.episodesObserved >= 3)
        #expect(trait.isReliable)
        // High-music windows must surface as a high `musicDensity` —
        // the no-signal default would yield 0 here. > 0.5 proves the
        // real signal threaded all the way to `traitProfileJSON`.
        #expect(trait.musicDensity > 0.5)

        let service = makeService(store: store, profile: after)
        let resolved = await service.resolveEpisodePriorsForTesting()
        #expect(resolved.activeLevel == .traitDerived)
    }

    /// cycle-1 missing test (#1): the *first* call to `updatePriors` for
    /// a podcast with no persisted profile must take the create-branch
    /// inside `mutateProfile` and write a non-nil `traitProfileJSON`
    /// derived from `ShowTraitProfile.unknown.updated(from: snapshot)`.
    /// Without this, a fresh show's first backfill would silently miss
    /// the trait-profile bootstrap and the EMA path would never start.
    @Test("updatePriors bootstraps traitProfileJSON for a show with no prior profile")
    func updatePriorsBootstrapsTraitProfileForFreshShow() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-v7v8-fresh-bootstrap-1"
        let assetId = "asset-v7v8-fresh-bootstrap-1"
        try await store.insertAsset(makeAsset(id: assetId, episodeId: "ep-v7v8-fresh-1"))

        // Crucially: NO seed profile. The first updatePriors call must
        // exercise `mutateProfile`'s `create` branch, which is the only
        // place the bootstrap path
        // `ShowTraitProfile.unknown.updated(from: snapshot)` lives.
        let preExisting = try await store.fetchProfile(podcastId: podcastId)
        #expect(preExisting == nil)

        let window = makeAdWindow(
            id: "win-fresh-bootstrap-1",
            assetId: assetId,
            startTime: 100,
            endTime: 130
        )
        let service = makeService(store: store, profile: nil)
        try await service.updatePriorsForTesting(
            podcastId: podcastId,
            nonSuppressedWindows: [window],
            episodeDuration: 600,
            featureWindows: [],
            chunks: []
        )

        let after = try #require(await store.fetchProfile(podcastId: podcastId))
        let traitJSON = try #require(
            after.traitProfileJSON,
            "First updatePriors call on a fresh show must write a non-nil traitProfileJSON via the create branch"
        )
        let trait = try JSONDecoder().decode(
            ShowTraitProfile.self,
            from: Data(traitJSON.utf8)
        )
        // The bootstrap path replaces the sentinel directly with the
        // first snapshot, so episodesObserved should be exactly 1
        // — not still 0 (sentinel) and not >= 3 (already reliable).
        #expect(trait.episodesObserved == 1)
        #expect(trait.isReliable == false)
    }

    @Test("updatePriors merges new ad-window durations into adDurationStatsJSON")
    func updatePriorsAccumulatesDurations() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-updatepriors-stats-1"

        // Seed a profile so the existing show-local stats are visible.
        let initial = AdDurationStats(meanDuration: 30, sampleCount: 5)
        let seed = makeProfile(
            podcastId: podcastId,
            adDurationStatsJSON: initial.encodeForTesting(),
            observationCount: 3
        )
        try await store.upsertProfile(seed)

        // Insert an asset + a confirmed ad window with a 10-second duration.
        // `updatePriors` doesn't read from the store for windows (callers
        // pass them in), but the asset row is still required because the
        // test-store schema will reject orphan AdWindow rows under FK
        // constraints. Inserting it keeps the harness honest.
        let assetId = "asset-updatepriors-stats-1"
        try await store.insertAsset(makeAsset(id: assetId, episodeId: "ep-1"))
        let window = makeAdWindow(
            id: "win-updatepriors-stats-1",
            assetId: assetId,
            startTime: 100,
            endTime: 110
        )
        try await store.insertAdWindow(window)

        // Drive the production `updatePriors` path end-to-end: the create/
        // update closures inside `mutateProfile` are the only place where
        // `decodeAdDurationStats` and `encodeAdDurationStats` are wired up,
        // and unit-testing the streaming-mean helper alone wouldn't catch a
        // regression where the closure stops calling them.
        let service = makeService(store: store, profile: seed)
        try await service.updatePriorsForTesting(
            podcastId: podcastId,
            nonSuppressedWindows: [window],
            episodeDuration: 600,
            // cycle-1 L4: this test exercises only the
            // `adDurationStatsJSON` accumulation path, which is fed by
            // the confirmed-ad windows above; trait-snapshot inputs are
            // intentionally empty so the duration aggregate isn't
            // entangled with the trait merge.
            featureWindows: [],
            chunks: []
        )

        // The persisted profile should now reflect a 6-sample aggregate with
        // a mean shifted toward the new 10s observation (Welford-style).
        let updated = try #require(await store.fetchProfile(podcastId: podcastId))
        let updatedJSON = try #require(updated.adDurationStatsJSON)
        let updatedStats = try JSONDecoder().decode(
            AdDurationStats.self,
            from: Data(updatedJSON.utf8)
        )
        #expect(updatedStats.sampleCount == initial.sampleCount + 1)
        // The mean must move toward the new observation, but not all the way.
        #expect(updatedStats.meanDuration < initial.meanDuration)
        #expect(updatedStats.meanDuration > 10)
    }

    // MARK: - traitProfileJSON round-trip through AnalysisStore

    @Test("traitProfileJSON survives AnalysisStore upsert/fetch round-trip with all fields intact")
    func traitProfileJSONRoundTripsThroughAnalysisStore() async throws {
        // Cycle-2 L2: the persisted column is `String?` (raw JSON). Encode a
        // ShowTraitProfile carrying non-default values for every field,
        // upsert through the production `AnalysisStore.upsertProfile` SQL
        // path, fetch the row back, decode, and assert the snapshot is
        // bit-equivalent. This locks in the schema/SQL bind/SQL read path
        // so a future column rename or accidental COALESCE wrap can't
        // silently drop or corrupt trait fields.
        let store = try await makeTestStore()
        let podcastId = "podcast-trait-roundtrip-1"
        let original = ShowTraitProfile(
            musicDensity: 0.31,
            speakerTurnRate: 0.62,
            singleSpeakerDominance: 0.43,
            structureRegularity: 0.74,
            sponsorRecurrence: 0.25,
            insertionVolatility: 0.86,
            transcriptReliability: 0.57,
            episodesObserved: 7
        )
        let encoded = try JSONEncoder().encode(original)
        let json = try #require(String(data: encoded, encoding: .utf8))

        let seed = PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.5,
            observationCount: 7,
            mode: SkipMode.shadow.rawValue,
            recentFalseSkipSignals: 0,
            traitProfileJSON: json,
            title: nil,
            adDurationStatsJSON: nil
        )
        try await store.upsertProfile(seed)

        let fetched = try #require(await store.fetchProfile(podcastId: podcastId))
        let fetchedJSON = try #require(
            fetched.traitProfileJSON,
            "traitProfileJSON must survive the upsert/fetch round-trip"
        )
        let decoded = try JSONDecoder().decode(
            ShowTraitProfile.self,
            from: Data(fetchedJSON.utf8)
        )
        #expect(decoded == original)
    }

    @Test("traitProfileJSON UPDATE overwrites the prior value (no COALESCE on the column)")
    func traitProfileJSONUpsertOverwritesPriorValue() async throws {
        // Cycle-3 M-2: pin the SQL contract that
        // `AnalysisStore.upsertProfile`'s UPDATE branch writes
        // `traitProfileJSON = excluded.traitProfileJSON` (overwrite, not
        // COALESCE). The cycle-15 M-2 / cycle-17 M-1 atomicity canaries
        // assume this contract — they pin the carry-forward in the
        // closure as the load-bearing safety net because a default-`nil`
        // would silently nil the persisted column. If the SQL is ever
        // changed to `COALESCE(excluded.traitProfileJSON, traitProfileJSON)`,
        // those canaries lose their meaning (a default-nil constructor
        // would no longer clobber the persisted JSON). This test catches
        // a quiet COALESCE drift directly.
        let store = try await makeTestStore()
        let podcastId = "podcast-trait-overwrite-1"

        // First upsert: write `traitProfileJSON = A` (musicDensity=0.3).
        let profileA = ShowTraitProfile(
            musicDensity: 0.3,
            speakerTurnRate: 4.0,
            singleSpeakerDominance: 0.5,
            structureRegularity: 0.6,
            sponsorRecurrence: 0.2,
            insertionVolatility: 0.4,
            transcriptReliability: 0.7,
            episodesObserved: 1
        )
        let jsonA = try #require(
            String(data: try JSONEncoder().encode(profileA), encoding: .utf8)
        )
        try await store.upsertProfile(
            PodcastProfile(
                podcastId: podcastId,
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: 0.5,
                observationCount: 1,
                mode: SkipMode.shadow.rawValue,
                recentFalseSkipSignals: 0,
                traitProfileJSON: jsonA,
                title: nil,
                adDurationStatsJSON: nil
            )
        )

        // Second upsert with the same podcastId: write a DIFFERENT
        // `traitProfileJSON = B` (musicDensity=0.7). If the column is
        // overwriting (the contract this test pins), the persisted value
        // must decode to B. If the column ever becomes COALESCE-wrapped,
        // the second write would lose any nil-only fields and a future
        // `traitProfileJSON: nil` would silently retain A — exactly the
        // shape the atomicity canaries assume can't happen.
        let profileB = ShowTraitProfile(
            musicDensity: 0.7,
            speakerTurnRate: 4.0,
            singleSpeakerDominance: 0.5,
            structureRegularity: 0.6,
            sponsorRecurrence: 0.2,
            insertionVolatility: 0.4,
            transcriptReliability: 0.7,
            episodesObserved: 2
        )
        let jsonB = try #require(
            String(data: try JSONEncoder().encode(profileB), encoding: .utf8)
        )
        try await store.upsertProfile(
            PodcastProfile(
                podcastId: podcastId,
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: 0.5,
                observationCount: 2,
                mode: SkipMode.shadow.rawValue,
                recentFalseSkipSignals: 0,
                traitProfileJSON: jsonB,
                title: nil,
                adDurationStatsJSON: nil
            )
        )

        let fetched = try #require(await store.fetchProfile(podcastId: podcastId))
        let fetchedJSON = try #require(
            fetched.traitProfileJSON,
            "second upsert must leave traitProfileJSON populated"
        )
        let decoded = try JSONDecoder().decode(
            ShowTraitProfile.self,
            from: Data(fetchedJSON.utf8)
        )
        #expect(decoded == profileB, "UPDATE must overwrite, not COALESCE")
        #expect(abs(decoded.musicDensity - 0.7) < 0.001)
    }

    // MARK: - Helpers

    private func makeProfile(
        podcastId: String = "podcast-test-1",
        adDurationStatsJSON: String?,
        observationCount: Int,
        networkId: String? = nil
    ) -> PodcastProfile {
        PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.5,
            observationCount: observationCount,
            mode: SkipMode.shadow.rawValue,
            recentFalseSkipSignals: 0,
            traitProfileJSON: nil,
            title: nil,
            adDurationStatsJSON: adDurationStatsJSON,
            networkId: networkId
        )
    }

    private func makeAsset(id: String, episodeId: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: episodeId,
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func makeAdWindow(
        id: String,
        assetId: String,
        startTime: Double,
        endTime: Double
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: startTime,
            endTime: endTime,
            confidence: 0.95,
            boundaryState: "confirmed",
            decisionState: AdDecisionState.applied.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "test",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: true,
            userDismissedBanner: false
        )
    }

    private func makeFeatureWindow(
        assetId: String,
        startTime: Double,
        endTime: Double,
        musicProbability: Double
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: assetId,
            startTime: startTime,
            endTime: endTime,
            rms: 0,
            spectralFlux: 0,
            musicProbability: musicProbability,
            speakerChangeProxyScore: 0,
            musicBedChangeScore: 0,
            musicBedOnsetScore: 0,
            musicBedOffsetScore: 0,
            musicBedLevel: .none,
            pauseProbability: 0,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 1
        )
    }

    private func makeChunk(
        assetId: String,
        chunkIndex: Int,
        start: Double,
        end: Double,
        speakerId: Int?
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "chunk-\(assetId)-\(chunkIndex)",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-\(chunkIndex)",
            chunkIndex: chunkIndex,
            startTime: start,
            endTime: end,
            text: "stub",
            normalizedText: "stub",
            pass: "final",
            modelVersion: "test-model",
            transcriptVersion: "v1",
            atomOrdinal: chunkIndex,
            weakAnchorMetadata: nil,
            speakerId: speakerId
        )
    }

    private func makeService(store: AnalysisStore, profile: PodcastProfile?) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .off
            ),
            podcastProfile: profile
        )
    }
}

// MARK: - Test-only encoding helpers

extension AdDurationStats {
    /// Convenience for tests to round-trip the stats payload through JSON
    /// without exposing the encoder/decoder publicly.
    func encodeForTesting() -> String {
        let data = try! JSONEncoder().encode(self)
        return String(data: data, encoding: .utf8)!
    }
}
