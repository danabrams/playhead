// ShowSkipModeControlTests.swift
// playhead-usn1: the per-show auto-skip control must WORK for a show the app
// already knows.
//
// THE FIELD REPORT (Dan, 2026-08-01, on the post-batch build): "i now dont
// appear to have anyway to turn autoskip on for a new show? it just says show
// unknown" — then the correction that reshaped the bead: "its not a brand new
// show it was a show i had downloaded several eipsodes if".
//
// THE BEAD'S LEADING HYPOTHESIS WAS `episode.podcast == nil`. It is wrong, and
// these tests exist partly so it cannot be re-adopted. Three independent facts
// rule it out:
//
//   * There is exactly ONE production path that constructs an `Episode`
//     (`PodcastDiscoveryService.persist`) and it always passes `podcast:`.
//     Nothing anywhere assigns `episode.podcast = nil`.
//   * `EpisodeListView`'s `@Query` filters on
//     `episode.podcast?.persistentModelID == podcastID`, so an episode with no
//     show cannot be rendered in the library and cannot be tapped.
//   * A NULL `analysis_jobs.podcastId` (the shape that prompted the hypothesis)
//     has a DIFFERENT producer — the download path, playhead-kkzu — and is NULL
//     for 13 of 13 rows on the device even for shows whose trust profiles are
//     keyed by their real feed URLs in the same database.
//
// THE ACTUAL DEFECT IS A ONE-SHOT READ OF A VALUE THAT IS NOT READY YET.
// `EpisodeListView.playEpisode` sets `navigateToNowPlaying = true` in the SAME
// turn it spawns `runtime.playEpisode`, so Now Playing appears immediately. Its
// `onAppear` called `loadSkipMode` exactly once. But `performPlayEpisode` calls
// `skipOrchestrator.endEpisode()` early — installing `.shadow` /
// `.noActiveEpisode` — and only reaches `beginEpisode`, the one thing that
// resolves the show, after transport load, `play()`, and analysis-asset
// resolution. The one read therefore observed the CLEARED pair, and
// `.noActiveEpisode` reports no resolved show identity, so playhead-djl0's pill
// rendered "Show Unknown" and withheld the menu — for a show whose identity
// resolves perfectly.
//
// The podcast TITLE never showed this, because `syncMetadata` re-reads it on
// every playback state event. Two facts about the same show, on two different
// cadences: one correct, one frozen before the answer existed.
//
// WHAT IS PINNED HERE
//   A. The show identity is recoverable from the episode ROW, not only from the
//      relationship — and the recovered value is byte-identical to the one the
//      relationship gives.
//   B. The orchestrator PUSHES the mode and its cause, so a subscriber that
//      attached before `beginEpisode` still ends up with the resolved answer.
//   C. The view model tracks that push for as long as the screen is mounted.
//   D. `setShowSkipMode` never silently skips: a write it cannot perform is a
//      named, counted, logged refusal, and the surface withdraws its optimistic
//      claim.
//   E. An identity only `beginEpisode` could recover reaches the runtime too, so
//      the trust lookup, the pill and the menu's write target agree.

import Foundation
import Testing

@testable import Playhead

// MARK: - Fixtures

private enum ShowControlFixture {

    static let feedURL = URL(string: "https://feeds.simplecast.com/dHoohVNH")!
    static let guid = "d9b513cd-0001"

    /// An episode built the way `PodcastDiscoveryService.persist` builds one:
    /// the SAME `feedURL` value reaches `Episode.init`'s key derivation and the
    /// `Podcast` the relationship points at.
    static func makeEpisode(
        feedURL: URL = feedURL,
        guid: String = guid,
        attachPodcast: Bool
    ) -> Episode {
        let podcast = attachPodcast
            ? Podcast(feedURL: feedURL, title: "A Show", author: "An Author")
            : nil
        return Episode(
            feedItemGUID: guid,
            feedURL: feedURL,
            podcast: podcast,
            title: "An Episode",
            audioURL: URL(string: "https://cdn.example.com/ep.mp3")!
        )
    }

    /// A podcast id no other test can collide with. The runtime's `AnalysisStore`
    /// is the test host's real one, shared for the process, so every profile
    /// this file writes is namespaced.
    static func uniquePodcastId() -> String {
        "https://usn1.test/\(UUID().uuidString)"
    }
}

/// Collects everything a `skipModeStream()` has emitted so far, in order.
///
/// `AsyncStream`'s default buffering is unbounded, so nothing is dropped between
/// attach and drain — which is what lets these tests assert on the SEQUENCE of
/// transitions rather than only on the final value.
private actor SkipModeRecorder {
    private var snapshots: [SkipModeSnapshot] = []
    private var task: Task<Void, Never>?

    func start(_ stream: AsyncStream<SkipModeSnapshot>) {
        task = Task {
            for await snapshot in stream {
                await self.append(snapshot)
            }
        }
    }

    private func append(_ snapshot: SkipModeSnapshot) {
        snapshots.append(snapshot)
    }

    /// Wait until `count` snapshots have arrived, or give up. Polling rather
    /// than a timeout on the stream itself so "nothing further arrived" is a
    /// bounded observation instead of a hang.
    func waitForCount(_ count: Int) async -> [SkipModeSnapshot] {
        for _ in 0..<200 where snapshots.count < count {
            try? await Task.sleep(for: .milliseconds(5))
        }
        return snapshots
    }

    func stop() {
        task?.cancel()
        task = nil
    }
}

// MARK: - A. The show identity lives on the episode row

@Suite("An episode's show identity survives a missing relationship (playhead-usn1)")
struct EpisodeShowIdentityTests {

    private typealias Fx = ShowControlFixture

    @Test("the relationship is the primary source")
    func theRelationshipIsThePrimarySource() {
        let episode = Fx.makeEpisode(attachPodcast: true)
        #expect(episode.resolvedShowIdentity == Fx.feedURL.absoluteString)
    }

    /// The field-facing half: an episode with no materialised relationship still
    /// knows which show it belongs to.
    @Test("a missing relationship still resolves the show from the episode key")
    func aMissingRelationshipStillResolvesTheShow() {
        let episode = Fx.makeEpisode(attachPodcast: false)
        #expect(episode.podcast == nil, "the fixture must actually be showless")
        #expect(episode.resolvedShowIdentity == Fx.feedURL.absoluteString)
    }

    /// THE load-bearing equality. A recovered identifier that differs from the
    /// relationship's — even by a character — would key trust and recurrence
    /// evidence into a neighbouring namespace, which is strictly worse than
    /// resolving nothing.
    @Test("the recovered identity is byte-identical to the relationship's")
    func theRecoveredIdentityMatchesTheRelationship() {
        let withShow = Fx.makeEpisode(attachPodcast: true)
        let withoutShow = Fx.makeEpisode(attachPodcast: false)
        #expect(withShow.resolvedShowIdentity == withoutShow.resolvedShowIdentity)
    }

    /// The reason the derivation strips the KNOWN guid suffix rather than
    /// splitting on the first `::`.
    @Test("a guid containing the separator still recovers the feed URL")
    func aGuidContainingTheSeparatorStillWorks() {
        let episode = Fx.makeEpisode(guid: "tag::2026::0001", attachPodcast: false)
        #expect(episode.resolvedShowIdentity == Fx.feedURL.absoluteString)
    }

    /// The other reason: an IPv6 literal host puts a `::` inside the feed URL
    /// itself, before the separator.
    @Test("an IPv6 feed host still recovers the feed URL")
    func anIPv6FeedHostStillWorks() throws {
        let ipv6 = try #require(URL(string: "http://[::1]/feed.xml"))
        let episode = Fx.makeEpisode(feedURL: ipv6, attachPodcast: false)
        #expect(episode.resolvedShowIdentity == ipv6.absoluteString)
    }

    @Test("a key that is not this episode's key recovers nothing")
    func aMismatchedKeyRecoversNothing() {
        #expect(Episode.showIdentity(
            fromCanonicalEpisodeKey: "https://feeds.example.com/x::guid-a",
            feedItemGUID: "guid-b"
        ) == nil)
    }

    @Test("an empty feed-URL prefix recovers nothing, never an empty identity")
    func anEmptyPrefixRecoversNothing() {
        #expect(Episode.showIdentity(
            fromCanonicalEpisodeKey: "::guid-a", feedItemGUID: "guid-a"
        ) == nil)
    }

    /// The same canonicalisation `SkipOrchestrator.beginEpisode` applies to the
    /// value it is handed. A non-canonical spelling is not an identity.
    @Test("a non-canonical spelling is refused rather than trimmed")
    func aNonCanonicalSpellingIsRefused() {
        #expect(Episode.showIdentity(
            fromCanonicalEpisodeKey: " https://feeds.example.com/x ::guid-a",
            feedItemGUID: "guid-a"
        ) == nil)
    }
}

// MARK: - B. The orchestrator pushes the mode and its cause

@Suite("The skip mode reaches the surface by push, not by sampling (playhead-usn1)",
       .timeLimit(.minutes(1)))
struct SkipModeStreamTests {

    private static let assetId = "asset-usn1"
    private static let episodeId = "ep-usn1"
    private static let seededPodcastId = "podcast-1"

    private static func makeOrchestrator(
        store: AnalysisStore
    ) async throws -> SkipOrchestrator {
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )
        return SkipOrchestrator(
            store: store,
            trustService: try await makeSkipTestTrustService(
                mode: "auto", trustScore: 0.9, observations: 10
            )
        )
    }

    @Test("the current pair is replayed the moment a subscriber attaches")
    func theCurrentPairIsReplayedOnAttach() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Self.makeOrchestrator(store: store)
        let recorder = SkipModeRecorder()
        await recorder.start(await orchestrator.skipModeStream())

        let snapshots = await recorder.waitForCount(1)
        await recorder.stop()
        #expect(snapshots.first == SkipModeSnapshot.noActiveEpisode)
    }

    /// THE FIELD DEFECT, at the layer that causes it. A subscriber that attached
    /// BEFORE the episode began — which is what Now Playing does, because the
    /// screen is presented in the same turn as the tap — must still end up with
    /// the show's real mode.
    @Test("a subscriber attached before beginEpisode receives the resolved mode")
    func aSubscriberAttachedBeforeBeginEpisodeIsUpdated() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Self.makeOrchestrator(store: store)
        let recorder = SkipModeRecorder()
        await recorder.start(await orchestrator.skipModeStream())

        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.seededPodcastId
        )

        let snapshots = await recorder.waitForCount(2)
        await recorder.stop()
        #expect(snapshots.last == SkipModeSnapshot(
            mode: .auto, resolution: .showTrustProfile
        ))
    }

    /// The negative control for the test above: without the push the last thing
    /// a pre-attached subscriber ever saw is the cleared pair, which is exactly
    /// what the field build rendered.
    @Test("the cleared pair is not the LAST thing a pre-attached subscriber sees")
    func theClearedPairIsNotTheFinalWord() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Self.makeOrchestrator(store: store)
        let recorder = SkipModeRecorder()
        await recorder.start(await orchestrator.skipModeStream())

        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.seededPodcastId
        )

        let snapshots = await recorder.waitForCount(2)
        await recorder.stop()
        #expect(snapshots.last?.resolution != .noActiveEpisode)
    }

    @Test("endEpisode publishes the cleared pair")
    func endEpisodePublishesTheClearedPair() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Self.makeOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.seededPodcastId
        )
        let recorder = SkipModeRecorder()
        await recorder.start(await orchestrator.skipModeStream())

        await orchestrator.endEpisode()

        let snapshots = await recorder.waitForCount(2)
        await recorder.stop()
        #expect(snapshots.last == SkipModeSnapshot.noActiveEpisode)
    }

    @Test("an explicit choice publishes the session override")
    func anExplicitChoicePublishes() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Self.makeOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.seededPodcastId
        )
        let recorder = SkipModeRecorder()
        await recorder.start(await orchestrator.skipModeStream())

        await orchestrator.setActiveSkipMode(.manual)

        let snapshots = await recorder.waitForCount(2)
        await recorder.stop()
        #expect(snapshots.last == SkipModeSnapshot(
            mode: .manual, resolution: .sessionOverride
        ))
    }

    /// A subscriber that attached during the PREVIOUS episode must not keep
    /// rendering that show's mode across the suspensions the next lookup takes.
    @Test("beginEpisode publishes the cleared pair before it resolves the show")
    func beginEpisodeClearsBeforeResolving() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Self.makeOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.seededPodcastId
        )
        let recorder = SkipModeRecorder()
        await recorder.start(await orchestrator.skipModeStream())

        // A second episode with NO show. Without the pre-lookup clear, the
        // cleared pair never appears and the prior show's `.auto` stands until
        // the verdict lands.
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: nil
        )

        let snapshots = await recorder.waitForCount(3)
        await recorder.stop()
        #expect(snapshots.contains(SkipModeSnapshot.noActiveEpisode))
    }
}

// MARK: - C. The view model tracks the push

@Suite("Now Playing tracks the skip mode for as long as it is mounted (playhead-usn1)",
       .timeLimit(.minutes(1)))
@MainActor
struct NowPlayingSkipModeSubscriptionTests {

    private static let assetId = "asset-usn1-vm"
    private static let episodeId = "ep-usn1-vm"

    /// Attach the view model's subscription and PROVE it is live before
    /// returning.
    ///
    /// `observeSkipMode` registers its continuation inside an unstructured Task,
    /// so a test that calls it and immediately begins an episode has no
    /// guarantee about which happened first. If the episode wins, the
    /// replay-on-attach hands over the already-resolved snapshot and the test
    /// passes even with the verdict publish deleted — which is exactly what
    /// mutation U01 demonstrated on the first run of this file. The sentinel is
    /// a resolution neither the replay (`.noActiveEpisode`) nor the verdict
    /// (`.showTrustProfile`) can produce, so watching it be overwritten is
    /// positive proof the subscription is attached.
    private static func attachAndAwaitSubscription(
        _ viewModel: NowPlayingViewModel,
        to orchestrator: SkipOrchestrator
    ) async {
        viewModel.skipModeResolution = .trustProfileUnreadable
        viewModel.observeSkipMode(from: orchestrator)
        for _ in 0..<400
        where viewModel.skipModeResolution == .trustProfileUnreadable {
            try? await Task.sleep(for: .milliseconds(5))
        }
    }

    /// THE FIELD DEFECT, at the surface. The screen appears, subscribes, and the
    /// episode begins AFTERWARDS — the real ordering. Before this bead the view
    /// model took one reading here and kept it, so the pill said "Show Unknown"
    /// and withheld the menu for a show sitting in `auto`.
    @Test("the view model learns the mode resolved after it appeared")
    func theViewModelLearnsTheLateResolvedMode() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: Self.assetId, episodeId: Self.episodeId)
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: try await makeSkipTestTrustService(
                mode: "auto", trustScore: 0.9, observations: 10
            )
        )
        let viewModel = NowPlayingViewModel(
            runtime: PlayheadRuntime(isPreviewRuntime: true)
        )

        await Self.attachAndAwaitSubscription(viewModel, to: orchestrator)
        #expect(viewModel.skipModeResolution == .noActiveEpisode,
                "precondition: the screen must be subscribed BEFORE the episode begins")

        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: "podcast-1"
        )

        for _ in 0..<200 where viewModel.skipModeResolution != .showTrustProfile {
            try? await Task.sleep(for: .milliseconds(5))
        }
        viewModel.stopObservingSkipMode()
        #expect(viewModel.activeSkipMode == .auto)
        #expect(viewModel.skipModeResolution == .showTrustProfile)
    }

    /// And the pill it drives is therefore selectable — which is the whole point
    /// of the bead. Dan: the per-show toggle is the manual lever precisely
    /// BECAUSE detection is unreliable, so "correctly hidden" is not an
    /// acceptable end state.
    @Test("the pill the tracked resolution drives offers the per-show menu")
    func theTrackedResolutionOffersTheMenu() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: Self.assetId, episodeId: Self.episodeId)
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: try await makeSkipTestTrustService(
                mode: "auto", trustScore: 0.9, observations: 10
            )
        )
        let viewModel = NowPlayingViewModel(
            runtime: PlayheadRuntime(isPreviewRuntime: true)
        )

        await Self.attachAndAwaitSubscription(viewModel, to: orchestrator)
        #expect(viewModel.skipModeResolution == .noActiveEpisode,
                "precondition: the screen must be subscribed BEFORE the episode begins")

        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: "podcast-1"
        )
        for _ in 0..<200 where viewModel.skipModeResolution != .showTrustProfile {
            try? await Task.sleep(for: .milliseconds(5))
        }
        viewModel.stopObservingSkipMode()

        let presentation = SkipModePillPresentation(
            mode: viewModel.activeSkipMode,
            resolution: viewModel.skipModeResolution
        )
        #expect(presentation.isModeSelectable)
        #expect(presentation.label == "Auto")
    }
}

// MARK: - D. The write refuses loudly rather than skipping silently

@Suite("A per-show skip-mode write is never silently skipped (playhead-usn1)",
       .timeLimit(.minutes(1)))
@MainActor
struct ShowSkipModeWriteTests {

    private typealias Fx = ShowControlFixture

    @Test("a session with no show REFUSES the write instead of skipping it")
    func aShowlessSessionRefusesTheWrite() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "a", episodeId: "ep-usn1-refuse", podcastId: nil
        )
        let outcome = await runtime.setShowSkipMode(
            .auto, orchestrator: runtime.skipOrchestrator
        )
        #expect(outcome == .refusedNoShowIdentity)
    }

    @Test("a refused write is counted")
    func aRefusedWriteIsCounted() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "a", episodeId: "ep-usn1-count", podcastId: nil
        )
        let before = runtime.refusedShowSkipModeWriteCount
        _ = await runtime.setShowSkipMode(
            .auto, orchestrator: runtime.skipOrchestrator
        )
        #expect(runtime.refusedShowSkipModeWriteCount == before + 1)
    }

    /// A refusal that still moved the session mode would be the same lie in a
    /// shorter-lived form: the listener sees their choice take effect and it
    /// vanishes at the next episode with nothing stored anywhere.
    @Test("a refused write does not change the session mode either")
    func aRefusedWriteLeavesTheSessionModeAlone() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "a", episodeId: "ep-usn1-session", podcastId: nil
        )
        let before = await runtime.skipOrchestrator.currentSkipMode()
        _ = await runtime.setShowSkipMode(
            .auto, orchestrator: runtime.skipOrchestrator
        )
        #expect(await runtime.skipOrchestrator.currentSkipMode() == before)
    }

    @Test("a resolved show persists the choice and says so")
    func aResolvedShowPersistsTheChoice() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let podcastId = Fx.uniquePodcastId()
        runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "a", episodeId: "ep-usn1-ok", podcastId: podcastId
        )
        let outcome = await runtime.setShowSkipMode(
            .auto, orchestrator: runtime.skipOrchestrator
        )
        #expect(outcome == .persisted(podcastId: podcastId))
    }

    @Test("the persisted choice is what a later trust lookup reads back")
    func thePersistedChoiceIsReadBack() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let podcastId = Fx.uniquePodcastId()
        runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "a", episodeId: "ep-usn1-read", podcastId: podcastId
        )
        _ = await runtime.setShowSkipMode(
            .auto, orchestrator: runtime.skipOrchestrator
        )
        let resolved = await runtime.trustService.resolveMode(podcastId: podcastId)
        #expect(resolved.mode == .auto)
        #expect(resolved.resolution == .showTrustProfile)
    }

    /// The negative control. A refusal counter that also ticked on success would
    /// make every "the refusal happened" assertion vacuous.
    @Test("a successful write is not counted as a refusal")
    func aSuccessfulWriteIsNotCounted() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "a",
            episodeId: "ep-usn1-neg",
            podcastId: Fx.uniquePodcastId()
        )
        let before = runtime.refusedShowSkipModeWriteCount
        _ = await runtime.setShowSkipMode(
            .manual, orchestrator: runtime.skipOrchestrator
        )
        #expect(runtime.refusedShowSkipModeWriteCount == before)
    }

    /// "Persists across relaunch", proved where the claim actually lives: a
    /// SECOND `AnalysisStore` opened on the same directory, which is what a
    /// relaunch does.
    @Test("a stored per-show choice survives a fresh store over the same directory")
    func aStoredChoiceSurvivesAReopen() async throws {
        let dir = try makeTempDir(prefix: "usn1-relaunch")
        let podcastId = "https://feeds.simplecast.com/dHoohVNH"

        let firstStore = try AnalysisStore(directory: dir)
        try await firstStore.migrate()
        await TrustScoringService(store: firstStore)
            .setUserOverride(podcastId: podcastId, mode: .auto)

        let secondStore = try AnalysisStore(directory: dir)
        try await secondStore.migrate()
        let resolved = await TrustScoringService(store: secondStore)
            .resolveMode(podcastId: podcastId)
        #expect(resolved.mode == .auto)
        #expect(resolved.resolution == .showTrustProfile)
    }
}

@Suite("A refused write withdraws the surface's optimistic claim (playhead-usn1)",
       .timeLimit(.minutes(1)))
@MainActor
struct RefusedSkipModeSelectionTests {

    /// `noteSkipModeSelection` moves the pill to `.sessionOverride` before the
    /// write is attempted, which is right when the write succeeds. When it is
    /// refused the claim is false and must be taken back — otherwise the pill
    /// reports the listener's choice back to them while nothing has stored it.
    @Test("a refused write restores the pill to what it said before the tap")
    func aRefusedWriteRestoresThePill() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "a", episodeId: "ep-usn1-vm-refuse", podcastId: nil
        )
        let viewModel = NowPlayingViewModel(runtime: runtime)
        viewModel.activeSkipMode = .shadow
        viewModel.skipModeResolution = .unresolvedShowIdentity

        viewModel.setSkipMode(.auto, orchestrator: runtime.skipOrchestrator)

        for _ in 0..<200 where viewModel.skipModeResolution == .sessionOverride {
            try? await Task.sleep(for: .milliseconds(5))
        }
        #expect(viewModel.skipModeResolution == .unresolvedShowIdentity)
        #expect(viewModel.activeSkipMode == .shadow)
    }
}

// MARK: - E. A recovered identity reaches BOTH halves of the session

@Suite("An identity only the orchestrator recovered reaches the runtime (playhead-usn1)",
       .timeLimit(.minutes(1)))
@MainActor
struct RecoveredShowIdentityAdoptionTests {

    private typealias Fx = ShowControlFixture

    /// playhead-djl0 recovered the show inside `beginEpisode` and stopped there.
    /// A session in that state reports a RESOLVED identity — so the pill offers
    /// the menu — while `currentPodcastId` is nil and the write goes nowhere.
    /// That is the "accepts a choice and forgets it" defect djl0 withheld the
    /// control to avoid; carrying the value back closes it instead.
    @Test("the runtime adopts an identity only the orchestrator could recover")
    func theRuntimeAdoptsTheRecoveredIdentity() async throws {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let episodeId = "ep-usn1-adopt-\(UUID().uuidString)"
        let assetId = "asset-\(UUID().uuidString)"
        let podcastId = Fx.uniquePodcastId()
        try await runtime.analysisStore.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )
        await runtime.skipOrchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: podcastId
        )
        let generation = runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: assetId, episodeId: episodeId, podcastId: nil
        )
        #expect(runtime.currentPodcastId == nil, "the runtime must start showless")

        await runtime.adoptRecoveredShowIdentity(
            generation: generation, episodeId: episodeId
        )
        #expect(runtime.currentPodcastId == podcastId)
    }

    /// The caller's value always wins. Adoption only ever widens.
    @Test("adoption never overwrites an identity the runtime already has")
    func adoptionNeverOverwrites() async throws {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let episodeId = "ep-usn1-keep-\(UUID().uuidString)"
        let assetId = "asset-\(UUID().uuidString)"
        let runtimeShow = Fx.uniquePodcastId()
        try await runtime.analysisStore.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )
        await runtime.skipOrchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: Fx.uniquePodcastId()
        )
        let generation = runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: assetId, episodeId: episodeId, podcastId: runtimeShow
        )

        await runtime.adoptRecoveredShowIdentity(
            generation: generation, episodeId: episodeId
        )
        #expect(runtime.currentPodcastId == runtimeShow)
    }

    /// A superseded playback request must not write its show onto the session
    /// that replaced it.
    @Test("a superseded play request does not adopt")
    func aSupersededRequestDoesNotAdopt() async throws {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        let episodeId = "ep-usn1-stale-\(UUID().uuidString)"
        let assetId = "asset-\(UUID().uuidString)"
        try await runtime.analysisStore.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )
        await runtime.skipOrchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: Fx.uniquePodcastId()
        )
        let staleGeneration = runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: assetId, episodeId: episodeId, podcastId: nil
        )
        // A newer play request supersedes it.
        runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: assetId, episodeId: episodeId, podcastId: nil
        )

        await runtime.adoptRecoveredShowIdentity(
            generation: staleGeneration, episodeId: episodeId
        )
        #expect(runtime.currentPodcastId == nil)
    }
}

// MARK: - The refusal's durable trace

/// Drain the runtime logger's JSON Lines session file. Mirrors the retry-drain
/// playhead-djl0's suites use: the sentinel is enqueued AFTER the write under
/// test on the same serial queue, so its arrival proves that write has landed.
///
/// Asserts PRESENCE only. The runtime's logger writes to the shared default
/// session directory, so an absence assertion here would be an assertion about
/// every other test in the process too.
private func drainRuntimeInvariantCodes(
    _ logger: SurfaceStatusInvariantLogger,
    untilSentinel sentinel: String
) -> [InvariantViolation.Code] {
    logger.invariantViolated(code: .unknown, description: sentinel)
    var entries: [SurfaceStateTransitionEntry] = []
    for _ in 0..<10 {
        logger.flushForTesting()
        guard let sessionURL = logger.currentSessionFileURL,
              let data = try? Data(contentsOf: sessionURL) else { continue }
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        entries = String(decoding: data, as: UTF8.self)
            .split(separator: "\n", omittingEmptySubsequences: true)
            .compactMap {
                try? decoder.decode(
                    SurfaceStateTransitionEntry.self, from: Data($0.utf8)
                )
            }
        if entries.contains(where: {
            $0.invariantViolation?.description == sentinel
        }) { break }
    }
    return entries
        .filter { $0.invariantViolation?.description != sentinel }
        .compactMap(\.invariantViolation?.code)
}

@Suite("A refused per-show write leaves a durable trace (playhead-usn1)",
       .timeLimit(.minutes(1)))
@MainActor
struct RefusedShowSkipModeWriteDiagnosticsTests {

    /// The third of "named, counted, SURFACED". The field investigation this bead
    /// came from went looking for a line explaining a control that did nothing,
    /// and the branch wrote none.
    ///
    /// The code is its OWN, not playhead-djl0's `.skipModeShowIdentityUnresolved`:
    /// that one records a READ at episode start that found no show, this one
    /// records a WRITE the listener actually asked for and did not get. Merging
    /// them would leave an operator unable to tell "we never knew the show" from
    /// "the listener tried to set a preference and it went nowhere" — the same
    /// collapse djl0 spent a bead undoing, one layer over.
    @Test("a refused write is recorded under its own code")
    func aRefusedWriteIsRecorded() async {
        let runtime = PlayheadRuntime(isPreviewRuntime: true)
        runtime._setUserMarkPlaybackContextForTesting(
            analysisAssetId: "a", episodeId: "ep-usn1-log", podcastId: nil
        )
        _ = await runtime.setShowSkipMode(
            .auto, orchestrator: runtime.skipOrchestrator
        )
        let codes = drainRuntimeInvariantCodes(
            runtime.surfaceStatusLogger,
            untilSentinel: "usn1-sentinel-\(UUID().uuidString)"
        )
        #expect(codes.contains(.skipModeWriteRefusedNoShowIdentity))
    }
}
