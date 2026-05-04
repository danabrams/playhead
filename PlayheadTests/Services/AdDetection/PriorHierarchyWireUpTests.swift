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

    // playhead-spxs cycle-1 L-2 deferred: the fall-through path when
    // `store.fetchProfiles(forNetworkId:)` THROWS (rather than returns
    // empty) is documented in `resolveEpisodePriors`'s do-catch block
    // but is not exercised by a behavioral test here. Reason: the
    // production `AnalysisStore` is a concrete actor without a protocol
    // seam, so injecting a throwing mock would require either a new
    // protocol abstraction (out of spxs scope) or a test-only failure
    // injection point on the real store. Both are larger than the
    // bead's mandate. Cycle-1 reviewer flagged this as L-2; deferring
    // until either (a) a protocol seam is introduced for unrelated
    // reasons, or (b) a future bead specifically addresses fault
    // injection. The do-catch itself is verified only by code reading.

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
