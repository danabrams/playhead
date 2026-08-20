// AdDetectionServiceProfileKeyingTests.swift
// playhead-2kxd: `AdDetectionService.currentPodcastProfile` was a SINGLETON
// STANDING FOR A SET — one optional slot naming "the current podcast" on an
// actor that more than one episode's analysis enters.
//
// WHY A BEHAVIOURAL SUITE AND A SOURCE CANARY, RATHER THAN EITHER ALONE.
// Three review rounds had already looked at this slot and each hardened ONE
// read site: `classifyCandidates` ("never fall back … that actor state may
// belong to the previous episode"), `resolveEpisodePriors` (snapshot two
// fields before the await), and skeptical-review-cycle-16 M-1 (delete the
// public setter). Reading the code was demonstrably not sufficient evidence —
// so the rails here are (a) tests that DRIVE two shows through the actor and
// (b) a canary that fails the moment a fourth read site is written, because a
// prose survey of read sites decays the instant somebody adds one.
//
// THE TESTS BELOW FAIL AGAINST THE PRE-FIX CODE. Evidence is in the PR body:
// the same scenario run against the parent commit's `AdDetectionService`
// (which spells the resolver `resolveEpisodePriorsForTesting()`, with no show
// argument) reports `.global` for show A the moment show B finishes a
// backfill. The mutants in `scripts/mutation-battery.sh` (PK series) restore
// the slot semantics one property at a time on today's code.

import Foundation
import Testing
@testable import Playhead

@Suite("AdDetectionService per-show profile keying (playhead-2kxd)")
struct AdDetectionServiceProfileKeyingTests {

    // MARK: - Fixtures

    /// Show A: enough confirmed-ad samples for the show-local tier to
    /// ACTIVATE, so "did this resolve A's profile?" is a single observable
    /// (`activeLevel == .showLocal`) rather than a numeric near-miss.
    private static let showA = "show-A-2kxd"
    /// Show B: a different podcast entirely. Never carries show-local stats,
    /// so a resolution that lands on B reports `.global`.
    private static let showB = "show-B-2kxd"

    private func makeProfile(
        podcastId: String,
        adDurationStatsJSON: String? = nil,
        observationCount: Int = 0,
        title: String? = nil,
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
            title: title,
            adDurationStatsJSON: adDurationStatsJSON,
            networkId: networkId
        )
    }

    /// A profile whose show-local stats clear `ShowLocalPriorsBuilder`'s
    /// sample floor AND `PriorHierarchyResolver`'s episode gate — the same
    /// shape `PriorHierarchyWireUpTests.wireUpShowLocalActivates` uses.
    private func makeShowLocalActivatingProfile(
        podcastId: String,
        title: String? = nil
    ) -> PodcastProfile {
        let stats = AdDurationStats(meanDuration: 5, sampleCount: 20)
        return makeProfile(
            podcastId: podcastId,
            adDurationStatsJSON: stats.encodeForTesting(),
            observationCount: 12,
            title: title
        )
    }

    private func makeService(
        store: AnalysisStore,
        profile: PodcastProfile?
    ) -> AdDetectionService {
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

    private func makeAdWindow(
        id: String,
        startTime: Double,
        endTime: Double
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: "asset-2kxd",
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

    /// Drive one show's post-fusion priors merge through the production path.
    /// This is the ONLY writer of the per-show profile cache in the shipping
    /// app — production never passes `podcastProfile:` at init — which is what
    /// made a single slot answer every episode with the last show that
    /// finished a backfill.
    private func finishBackfill(
        _ service: AdDetectionService,
        forShow podcastId: String,
        windowId: String = "win-2kxd"
    ) async throws {
        try await service.updatePriorsForTesting(
            podcastId: podcastId,
            nonSuppressedWindows: [makeAdWindow(id: windowId, startTime: 10, endTime: 40)],
            episodeDuration: 600,
            featureWindows: [],
            chunks: [],
            countsAsEpisodeObservation: false
        )
    }

    // MARK: - The defect, stated as a test

    /// THE RAIL. Show A's episode asks for its priors; show B finishes a
    /// backfill in between; A must still be told about A.
    ///
    /// Pre-fix this fails, and it fails in the direction that matters: A's
    /// show-local tier — 20 confirmed ad samples over 12 episodes — silently
    /// deactivates and A's fusion falls back to `GlobalPriorDefaults`, because
    /// the slot now holds B's brand-new profile. It is not a hypothetical
    /// concurrency window either: `nowCap` is 2, a playback job bypasses the
    /// cap entirely, and even strictly SEQUENTIAL episodes hit it, since
    /// nothing ever loaded the slot for the episode being analysed.
    @Test("resolveEpisodePriors answers for the show it was ASKED about, not the last show written")
    func resolvePriorsIsKeyedToTheRequestedShow() async throws {
        let store = try await makeTestStore()
        let service = makeService(
            store: store,
            profile: makeShowLocalActivatingProfile(podcastId: Self.showA)
        )

        // Baseline: A's own priors are active before anything else happens.
        let before = await service.resolveEpisodePriorsForTesting(podcastId: Self.showA)
        #expect(
            before.activeLevel == .showLocal,
            "fixture check: show A must start with its show-local tier active"
        )

        // A DIFFERENT SHOW finishes a backfill on the same actor.
        try await finishBackfill(service, forShow: Self.showB)

        let after = await service.resolveEpisodePriorsForTesting(podcastId: Self.showA)
        #expect(
            after.activeLevel == .showLocal,
            """
            playhead-2kxd: show A's priors were resolved from show B's \
            profile. A single `currentPodcastProfile` slot answers every \
            read with whichever show last finished a backfill.
            """
        )
        #expect(after.typicalAdDuration == before.typicalAdDuration)
    }

    /// The mirror: asking about B gets B, not the profile A was seeded with.
    /// Without this the suite could be satisfied by an accessor that always
    /// returns the init-time profile.
    @Test("resolveEpisodePriors for the OTHER show reports that show, not the seeded one")
    func resolvePriorsForTheOtherShowIsNotTheSeededShow() async throws {
        let store = try await makeTestStore()
        let service = makeService(
            store: store,
            profile: makeShowLocalActivatingProfile(podcastId: Self.showA)
        )

        try await finishBackfill(service, forShow: Self.showB)

        let resolvedB = await service.resolveEpisodePriorsForTesting(podcastId: Self.showB)
        #expect(
            resolvedB.activeLevel == .global,
            """
            show B has one confirmed ad window and no accumulated history, so \
            its show-local tier must stay inactive. Reporting `.showLocal` \
            here means B was answered with A's profile.
            """
        )
    }

    /// GENUINE INTERLEAVING, not a sequential clobber. Each round races show
    /// B's `updatePriors` — which suspends on `store.mutateProfile`, a real
    /// reentrancy point on this actor — against show A's resolver, which
    /// suspends on `store.fetchProfiles`. Whichever lands first, A must be
    /// told about A.
    ///
    /// Note the asymmetry that makes this a sound rail: on the FIXED code it
    /// cannot fail under any interleaving, because the answer is a dictionary
    /// lookup on the id the caller supplied. Only the broken code is
    /// order-sensitive, so a green result here is deterministic and a red one
    /// is a real finding.
    @Test("two shows interleaved across the actor's await points never read each other's profile")
    func interleavedShowsNeverCrossRead() async throws {
        let store = try await makeTestStore()
        let service = makeService(
            store: store,
            profile: makeShowLocalActivatingProfile(podcastId: Self.showA)
        )

        for round in 0..<12 {
            async let write: Void = finishBackfill(
                service,
                forShow: "\(Self.showB)-\(round)",
                windowId: "win-2kxd-\(round)"
            )
            async let resolvedA = service.resolveEpisodePriorsForTesting(podcastId: Self.showA)

            try await write
            let a = await resolvedA
            #expect(
                a.activeLevel == .showLocal,
                """
                playhead-2kxd round \(round): show A resolved to \
                \(a.activeLevel) while show B's priors merge was in flight. \
                The profile must be keyed by show, not held in a slot the \
                interleaving turn rewrites.
                """
            )
        }
    }

    // MARK: - The keying itself

    @Test("a completed backfill for one show leaves the other show's entry untouched")
    func writingOneShowDoesNotDisturbAnother() async throws {
        let store = try await makeTestStore()
        let service = makeService(
            store: store,
            profile: makeProfile(podcastId: Self.showA, title: "Show A")
        )

        try await finishBackfill(service, forShow: Self.showB)

        let a = await service.cachedPodcastProfileForTesting(showId: Self.showA)
        let b = await service.cachedPodcastProfileForTesting(showId: Self.showB)
        #expect(a?.podcastId == Self.showA)
        #expect(a?.title == "Show A", "show A's cached profile must be the one it was seeded with")
        #expect(b?.podcastId == Self.showB)
    }

    @Test("a show nobody has analysed resolves to nothing, not to somebody else's profile")
    func unknownShowFailsClosed() async throws {
        let store = try await makeTestStore()
        let service = makeService(
            store: store,
            profile: makeProfile(podcastId: Self.showA, title: "Show A")
        )

        #expect(await service.cachedPodcastProfileForTesting(showId: "show-never-seen") == nil)
    }

    /// `classifyCandidates` has always insisted that "a nil/blank request must
    /// fail closed instead of querying a stale show's cache". playhead-2kxd
    /// makes that the rule for the profile too, so the two cannot drift.
    @Test("a nil or empty show id fails closed even when profiles are cached")
    func nilOrEmptyShowIdFailsClosed() async throws {
        let store = try await makeTestStore()
        let service = makeService(
            store: store,
            profile: makeProfile(podcastId: Self.showA)
        )
        try await finishBackfill(service, forShow: Self.showB)

        #expect(await service.cachedPodcastProfileForTesting(showId: nil) == nil)
        #expect(await service.cachedPodcastProfileForTesting(showId: "") == nil)

        let resolved = await service.resolveEpisodePriorsForTesting(podcastId: nil)
        #expect(
            resolved.activeLevel == .global,
            "a caller with no show identity gets global defaults, never the last show's priors"
        )
    }

    /// An empty `podcastId` is this codebase's spelling of "no show identity"
    /// (see `RegionShadowPhase.run` and `SemanticScanClaim.claimRow`), so it
    /// must never become a dictionary key — otherwise every identity-less
    /// episode shares one entry and the slot is back under another name.
    ///
    /// ASSERTED ON THE KEY SET, NOT ON THE READER. `cachedPodcastProfileForTesting(showId: "")`
    /// answers `nil` whether the empty id was never stored or the reader
    /// merely refuses to look it up, so a rail written on the reader alone
    /// would hold with `cachePodcastProfile`'s guard deleted — a test that
    /// passes if the thing it names never happened. Both writers are exercised:
    /// the init seed (`seededProfileMap`) and the post-backfill cache
    /// (`cachePodcastProfile`) have separate guards and separate mutants.
    @Test("a profile carrying an empty podcastId is not cached at all, by either writer")
    func emptyPodcastIdIsNeverAKey() async throws {
        let store = try await makeTestStore()
        let service = makeService(
            store: store,
            profile: makeProfile(podcastId: "", title: "identity-less")
        )

        // Writer 1: the init seed.
        #expect(
            await service.cachedProfileShowIdsForTesting().isEmpty,
            "seededProfileMap admitted an empty podcastId as a key"
        )

        // Writer 2: a completed backfill that carries no show identity.
        try await finishBackfill(service, forShow: "", windowId: "win-2kxd-empty")
        #expect(
            !(await service.cachedProfileShowIdsForTesting().contains("")),
            "cachePodcastProfile admitted an empty podcastId as a key"
        )

        #expect(await service.cachedPodcastProfileForTesting(showId: "") == nil)
        #expect(await service.cachedPodcastProfileForTesting(showId: nil) == nil)
    }

    // MARK: - What the substitution is WORTH, on the owner's real device

    /// playhead-2kxd acceptance criterion 6. "Two shows can be in the pipeline
    /// at once" only matters if reading the wrong one CHANGES something, and
    /// that is a question about real profiles rather than about fixtures.
    ///
    /// These two payloads are copied verbatim out of `podcast_profiles` in the
    /// 2026-08-18 t3 device pull — the only two rows in it, and the whole
    /// population of shows the owner's device has ever analysed. Both clear
    /// every gate, so both shows have an ACTIVE show-local tier: substituting
    /// one for the other is not a fallback to defaults, it is one show's
    /// measured ad length asserted about another's episode.
    ///
    /// The test asserts the DIFFERENCE, not a particular number, so a future
    /// change to `durationRangeHalfWidth` or the blend weights does not make
    /// it a maintenance chore. What it pins is the claim the bead rests on:
    /// on the real corpus these are not interchangeable.
    @Test("device corpus: the two shows' resolved priors are materially different")
    func deviceCorpusShowsAreNotInterchangeable() {
        // Conan O'Brien Needs A Friend — 488 confirmed ad samples, mean 41.1 s.
        let conan = makeProfile(
            podcastId: "https://feeds.simplecast.com/dHoohVNH",
            adDurationStatsJSON: #"{"sampleCount":488,"meanDuration":41.08485035056562}"#,
            observationCount: 8
        )
        // The Diary Of A CEO — 113 samples, mean 20.0 s.
        let doac = makeProfile(
            podcastId: "https://rss2.flightcast.com/xmsftuzjjykcmqwolaqn6mdn",
            adDurationStatsJSON: #"{"sampleCount":113,"meanDuration":19.969017852110145}"#,
            observationCount: 7
        )

        func resolved(_ profile: PodcastProfile) -> ResolvedPriors {
            PriorHierarchyResolver.resolve(
                globalDefaults: .standard,
                networkPriors: nil,
                networkDecay: 0,
                traitProfile: profile.traitProfile,
                showLocalPriors: ShowLocalPriorsBuilder.build(from: profile)
            )
        }

        let conanPriors = resolved(conan)
        let doacPriors = resolved(doac)

        #expect(conanPriors.activeLevel == .showLocal)
        #expect(doacPriors.activeLevel == .showLocal)
        #expect(
            conanPriors.typicalAdDuration != doacPriors.typicalAdDuration,
            """
            playhead-2kxd: the two shows on the device pull resolve to the same \
            typicalAdDuration, which would mean a cross-show read costs nothing \
            on this corpus. Measured 2026-08-18 they do not: 41.1 s mean against \
            20.0 s. Conan \(conanPriors.typicalAdDuration), DOAC \
            \(doacPriors.typicalAdDuration).
            """
        )
        // Direction, so the reading is not just "they differ": the show with
        // the longer measured ads must resolve to the longer band.
        #expect(conanPriors.typicalAdDuration.lowerBound > doacPriors.typicalAdDuration.lowerBound)
        #expect(conanPriors.typicalAdDuration.upperBound > doacPriors.typicalAdDuration.upperBound)
    }

    /// The key set is the real subject of the bead: one entry per show, named
    /// by that show. Pinned directly so a future change that quietly collapses
    /// it back to a single entry is a red test rather than a code review.
    @Test("the cache holds one entry per show, named by that show")
    func theCacheIsKeyedByShow() async throws {
        let store = try await makeTestStore()
        let service = makeService(
            store: store,
            profile: makeShowLocalActivatingProfile(podcastId: Self.showA)
        )
        #expect(await service.cachedProfileShowIdsForTesting() == [Self.showA])

        try await finishBackfill(service, forShow: Self.showB)
        #expect(
            await service.cachedProfileShowIdsForTesting() == [Self.showA, Self.showB],
            "show B's backfill must ADD an entry, not replace show A's"
        )
    }
}
