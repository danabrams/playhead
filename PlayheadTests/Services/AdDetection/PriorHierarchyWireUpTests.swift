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
            episodeDuration: 600
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

    // MARK: - Helpers

    private func makeProfile(
        podcastId: String = "podcast-test-1",
        adDurationStatsJSON: String?,
        observationCount: Int
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
            adDurationStatsJSON: adDurationStatsJSON
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
