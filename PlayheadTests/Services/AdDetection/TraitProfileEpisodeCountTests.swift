// TraitProfileEpisodeCountTests.swift
// playhead-g7ln: `ShowTraitProfile.episodesObserved` counts EPISODES.
//
// The defect, restated so a reader of this file does not have to go and find
// the bead. `ShowTraitProfile.updated(from:)` adds one on every call, and its
// only production caller — `AdDetectionService.updatePriors` — ran once per
// completed `runBackfill`. One asset is backfilled many times (hot path, final
// pass, re-drives, transcript-version bumps, playhead-15d0's resume), so the
// column counted BACKFILLS while both of its readers are named for episodes.
// Measured on the 2026-08-18 t3 device pull, against the
// `trust_episode_observations` ledger and the `analysis_assets` rows:
// **104 and 56 against 8 and 7 distinct episodes** — 13.0x and 8.0x.
//
// What this suite pins, and what it deliberately cannot:
//
//   * THE WITNESS — one claimed episode followed by eight unclaimed
//     re-backfills leaves `episodesObserved == 1`, not 9. That is the device's
//     own shape, driven through the production `mutateProfile` closures.
//   * THE MIRROR — every OTHER prior on the same profile still accumulates on
//     an unclaimed backfill. The gate is scoped to the trait profile; a fix
//     that froze the whole priors merge would pass the witness above and be
//     wrong, and `updatePriorsAccumulatesDurations` in
//     `PriorHierarchyWireUpTests` only ever drives the claimed direction.
//   * THE CREATE BRANCH in both directions.
//   * THE CONSEQUENCE — the same episode count at the resolver, so a mutant
//     that fixes the number but leaves the tier saturated is still killed.
//
// It cannot see the CALL SITE — that `runBackfill` forwards the claim result
// rather than a literal `true`. `recordConfirmedWindowObservation` is private
// and `runBackfill` is not drivable from here, so that property belongs to
// `TraitEpisodeCountSourceCanaryTests`, which reads the source.

import Foundation
import Testing
@testable import Playhead

@Suite("Trait profile episode count (playhead-g7ln)")
struct TraitProfileEpisodeCountTests {

    // MARK: - 1. The witness: nine backfills of one episode are one episode

    /// The device's own shape. Under the pre-g7ln code this reads 9.
    @Test("one claimed episode plus eight re-backfills counts ONE episode")
    func reBackfillsOfOneEpisodeCountOnce() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-g7ln-witness"
        let assetId = "asset-g7ln-witness"
        try await store.insertAsset(makeAsset(id: assetId, episodeId: "ep-g7ln-witness"))
        try await store.upsertProfile(makeSeedProfile(podcastId: podcastId))

        let window = makeAdWindow(id: "win-g7ln-witness", assetId: assetId, startTime: 100, endTime: 130)

        // Backfill 1 takes the claim. Backfills 2-9 find it taken — the exact
        // sequence `runBackfill` produces for one asset.
        for backfillIndex in 0..<9 {
            let current = try await store.fetchProfile(podcastId: podcastId)
            let service = makeService(store: store, profile: current)
            try await service.updatePriorsForTesting(
                podcastId: podcastId,
                nonSuppressedWindows: [window],
                episodeDuration: 600,
                featureWindows: [],
                chunks: [],
                countsAsEpisodeObservation: backfillIndex == 0
            )
        }

        let trait = try await requireTrait(store, podcastId)
        #expect(trait.episodesObserved == 1, "nine backfills of one asset are ONE episode")
        #expect(trait.isReliable == false, "one episode is below the >= 3 reliability gate")
    }

    /// The direction the fix must NOT break: distinct episodes still count.
    @Test("three claimed episodes count three, and the tier turns on")
    func claimedEpisodesStillCount() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-g7ln-three"
        let assetId = "asset-g7ln-three"
        try await store.insertAsset(makeAsset(id: assetId, episodeId: "ep-g7ln-three"))
        try await store.upsertProfile(makeSeedProfile(podcastId: podcastId))

        for episodeIndex in 0..<3 {
            let window = makeAdWindow(
                id: "win-g7ln-three-\(episodeIndex)",
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
                featureWindows: [],
                chunks: [],
                countsAsEpisodeObservation: true
            )
        }

        let trait = try await requireTrait(store, podcastId)
        #expect(trait.episodesObserved == 3)
        #expect(trait.isReliable)
    }

    /// An unclaimed backfill leaves the persisted JSON BYTE-IDENTICAL. Stronger
    /// than "the count did not move": the EMA must not run at all, because ~9
    /// near-identical snapshots of one episode are not an average over nine.
    @Test("an unclaimed backfill leaves traitProfileJSON byte-identical")
    func unclaimedBackfillDoesNotTouchTheColumn() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-g7ln-bytes"
        let assetId = "asset-g7ln-bytes"
        try await store.insertAsset(makeAsset(id: assetId, episodeId: "ep-g7ln-bytes"))
        try await store.upsertProfile(makeSeedProfile(podcastId: podcastId))

        let window = makeAdWindow(id: "win-g7ln-bytes", assetId: assetId, startTime: 10, endTime: 40)
        let musicWindows = (0..<10).map {
            makeFeatureWindow(
                assetId: assetId,
                startTime: Double($0) * 60,
                endTime: Double($0) * 60 + 60,
                musicProbability: 0.9
            )
        }

        let seedService = makeService(store: store, profile: try await store.fetchProfile(podcastId: podcastId))
        try await seedService.updatePriorsForTesting(
            podcastId: podcastId,
            nonSuppressedWindows: [window],
            episodeDuration: 600,
            featureWindows: musicWindows,
            chunks: [],
            countsAsEpisodeObservation: true
        )
        let afterClaim = try #require(try await store.fetchProfile(podcastId: podcastId)?.traitProfileJSON)

        // A second pass over the SAME asset, with different signal — low music
        // this time, so an EMA that ran would visibly move `musicDensity`.
        let quietWindows = (0..<10).map {
            makeFeatureWindow(
                assetId: assetId,
                startTime: Double($0) * 60,
                endTime: Double($0) * 60 + 60,
                musicProbability: 0.0
            )
        }
        let second = makeService(store: store, profile: try await store.fetchProfile(podcastId: podcastId))
        try await second.updatePriorsForTesting(
            podcastId: podcastId,
            nonSuppressedWindows: [window],
            episodeDuration: 600,
            featureWindows: quietWindows,
            chunks: [],
            countsAsEpisodeObservation: false
        )

        let afterReBackfill = try #require(try await store.fetchProfile(podcastId: podcastId)?.traitProfileJSON)
        #expect(afterReBackfill == afterClaim, "an unclaimed backfill must not rewrite the trait column at all")
    }

    // MARK: - 2. The mirror: the gate is scoped to the TRAIT profile

    /// A fix that froze the whole priors merge would pass every assertion
    /// above. This is the test that says it did not.
    @Test("an unclaimed backfill still accumulates durations, slots and sponsors")
    func unclaimedBackfillStillAccumulatesEveryOtherPrior() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-g7ln-mirror"
        let assetId = "asset-g7ln-mirror"
        try await store.insertAsset(makeAsset(id: assetId, episodeId: "ep-g7ln-mirror"))

        let initial = AdDurationStats(meanDuration: 30, sampleCount: 5)
        try await store.upsertProfile(
            PodcastProfile(
                podcastId: podcastId,
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: 0.5,
                observationCount: 3,
                mode: SkipMode.shadow.rawValue,
                recentFalseSkipSignals: 0,
                traitProfileJSON: nil,
                title: nil,
                adDurationStatsJSON: initial.encodeForTesting(),
                networkId: nil
            )
        )

        let window = makeAdWindow(
            id: "win-g7ln-mirror",
            assetId: assetId,
            startTime: 100,
            endTime: 110,
            advertiser: "Acme Mattress"
        )

        let service = makeService(store: store, profile: try await store.fetchProfile(podcastId: podcastId))
        try await service.updatePriorsForTesting(
            podcastId: podcastId,
            nonSuppressedWindows: [window],
            episodeDuration: 600,
            featureWindows: [],
            chunks: [],
            countsAsEpisodeObservation: false
        )

        let after = try #require(await store.fetchProfile(podcastId: podcastId))
        // The trait column is untouched…
        #expect(after.traitProfileJSON == nil)
        #expect(after.traitProfile.episodesObserved == 0)
        // …and every other prior on the same row moved.
        let stats = try JSONDecoder().decode(
            AdDurationStats.self,
            from: Data(try #require(after.adDurationStatsJSON).utf8)
        )
        #expect(stats.sampleCount == initial.sampleCount + 1, "the duration aggregate is per-AD, not per-episode")
        #expect(after.normalizedAdSlotPriors != nil, "slot priors still merge on an unclaimed backfill")
        #expect(after.sponsorLexicon == "acme mattress", "the sponsor lexicon still merges too")
        // And the columns the priors merge only ever carries forward are intact.
        #expect(after.observationCount == 3, "playhead-2qz6: updatePriors never writes this")
        #expect(after.mode == SkipMode.shadow.rawValue)
        #expect(after.skipTrustScore == 0.5)
    }

    // MARK: - 3. The create branch, in both directions

    /// `updatePriors`' create branch is reached only when the trust path did
    /// NOT create the profile — i.e. when nothing was claimed. Seeding a
    /// one-episode profile there credits an episode nothing witnessed, exactly
    /// as `observationCount: 1` would (playhead-2qz6 seeds 0 for that reason).
    @Test("an unclaimed create seeds NO trait profile")
    func unclaimedCreateSeedsNothing() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-g7ln-create-false"
        let assetId = "asset-g7ln-create-false"
        try await store.insertAsset(makeAsset(id: assetId, episodeId: "ep-g7ln-create-false"))
        #expect(try await store.fetchProfile(podcastId: podcastId) == nil)

        let window = makeAdWindow(id: "win-g7ln-create-false", assetId: assetId, startTime: 100, endTime: 130)
        let service = makeService(store: store, profile: nil)
        try await service.updatePriorsForTesting(
            podcastId: podcastId,
            nonSuppressedWindows: [window],
            episodeDuration: 600,
            featureWindows: [],
            chunks: [],
            countsAsEpisodeObservation: false
        )

        let after = try #require(await store.fetchProfile(podcastId: podcastId))
        #expect(after.traitProfileJSON == nil, "an unwitnessed episode must not seed a one-episode profile")
        #expect(after.traitProfile.episodesObserved == 0)
        #expect(after.observationCount == 0, "playhead-2qz6's sibling claim on the same branch")
        // The next backfill that DOES claim gets the first-episode semantics,
        // one pass later: a NULL column decodes as `.unknown`, whose
        // `updated(from:)` REPLACES rather than blends.
        let second = makeService(store: store, profile: after)
        try await second.updatePriorsForTesting(
            podcastId: podcastId,
            nonSuppressedWindows: [window],
            episodeDuration: 600,
            featureWindows: [],
            chunks: [],
            countsAsEpisodeObservation: true
        )
        let trait = try await requireTrait(store, podcastId)
        #expect(trait.episodesObserved == 1)
    }

    // MARK: - 4. The consequence at the resolver

    /// The number is not the point; what the number buys is. A mutant that
    /// counts correctly and leaves the tier saturated must still die.
    @Test("the trait tier is inactive at one episode and active at three")
    func resolverFollowsTheEpisodeCount() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-g7ln-resolver"
        let assetId = "asset-g7ln-resolver"
        try await store.insertAsset(makeAsset(id: assetId, episodeId: "ep-g7ln-resolver"))
        try await store.upsertProfile(makeSeedProfile(podcastId: podcastId))

        let window = makeAdWindow(id: "win-g7ln-resolver", assetId: assetId, startTime: 100, endTime: 130)

        // One claimed episode, then eight re-backfills — the device's shape.
        for backfillIndex in 0..<9 {
            let current = try await store.fetchProfile(podcastId: podcastId)
            let service = makeService(store: store, profile: current)
            try await service.updatePriorsForTesting(
                podcastId: podcastId,
                nonSuppressedWindows: [window],
                episodeDuration: 600,
                featureWindows: [],
                chunks: [],
                countsAsEpisodeObservation: backfillIndex == 0
            )
        }
        let afterOne = try #require(await store.fetchProfile(podcastId: podcastId))
        let resolvedAtOne = await makeService(store: store, profile: afterOne)
            .resolveEpisodePriorsForTesting(podcastId: podcastId)
        #expect(resolvedAtOne.activeLevel == .global, "one episode must not activate the trait tier")
        #expect(resolvedAtOne.levelContributions[.traitDerived] == nil)

        // Two more real episodes reach the >= 3 gate.
        for episodeIndex in 1...2 {
            let current = try await store.fetchProfile(podcastId: podcastId)
            let service = makeService(store: store, profile: current)
            try await service.updatePriorsForTesting(
                podcastId: podcastId,
                nonSuppressedWindows: [
                    makeAdWindow(
                        id: "win-g7ln-resolver-\(episodeIndex)",
                        assetId: assetId,
                        startTime: Double(episodeIndex) * 200,
                        endTime: Double(episodeIndex) * 200 + 30
                    )
                ],
                episodeDuration: 600,
                featureWindows: [],
                chunks: [],
                countsAsEpisodeObservation: true
            )
        }
        let afterThree = try #require(await store.fetchProfile(podcastId: podcastId))
        #expect(afterThree.traitProfile.episodesObserved == 3)
        let resolvedAtThree = await makeService(store: store, profile: afterThree)
            .resolveEpisodePriorsForTesting(podcastId: podcastId)
        #expect(resolvedAtThree.activeLevel == .traitDerived)
        // 0.4 at three episodes, NOT the 0.6 clamp the old unit reached inside
        // the first one. The ramp is what the count buys, so it is asserted.
        #expect(abs((resolvedAtThree.levelContributions[.traitDerived] ?? 0) - 0.4) < 0.001)
    }

    // MARK: - 5. The blend weight the device was sitting at

    /// Pure-function rails on the two readers, at the numbers the pull
    /// actually held. Not a restatement of `PriorHierarchyTests`: these are the
    /// specific before/after pairs this bead's measurement quotes, so a change
    /// to either constant lands on a test that names the device.
    @Test("the device's counts: 104 and 56 were indistinguishable from 7")
    func deviceCountsSaturatedTheRamp() {
        // Pre-fix: the persisted values, in backfills.
        #expect(PriorHierarchyResolver.traitBlendWeight(episodesObserved: 104) == 0.6)
        #expect(PriorHierarchyResolver.traitBlendWeight(episodesObserved: 56) == 0.6)
        // Post-migration: the trait tier is off entirely at 0, so no weight is
        // reached at all — `isReliable` is what gates the block that calls it.
        #expect(ShowTraitProfile.unknown.isReliable == false)
        // And the honest ramp a show climbs back through.
        #expect(PriorHierarchyResolver.traitBlendWeight(episodesObserved: 3) == 0.4)
        #expect(abs(PriorHierarchyResolver.traitBlendWeight(episodesObserved: 5) - 0.5) < 0.001)
        #expect(abs(PriorHierarchyResolver.traitBlendWeight(episodesObserved: 7) - 0.6) < 0.001)
    }

    // MARK: - Helpers

    private func requireTrait(
        _ store: AnalysisStore,
        _ podcastId: String
    ) async throws -> ShowTraitProfile {
        let profile = try #require(await store.fetchProfile(podcastId: podcastId))
        let json = try #require(profile.traitProfileJSON, "expected a persisted trait profile")
        return try JSONDecoder().decode(ShowTraitProfile.self, from: Data(json.utf8))
    }

    private func makeSeedProfile(podcastId: String) -> PodcastProfile {
        PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.5,
            observationCount: 0,
            mode: SkipMode.shadow.rawValue,
            recentFalseSkipSignals: 0,
            traitProfileJSON: nil,
            title: nil,
            adDurationStatsJSON: nil,
            networkId: nil
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
        endTime: Double,
        advertiser: String? = nil
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
            advertiser: advertiser,
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
