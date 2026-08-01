// DayZeroMarkDeliveryTests.swift
// playhead-96ot: a day-0 mint must reach the LIVE session that produced it.
//
// THE DEFECT. `AdDetectionService.mintByteExactDayZeroMarks` persists its
// `AdWindow`s through `store.upsertHotPathAdWindows` — and then nothing carried
// them to `SkipOrchestrator` during the session that minted them:
//
//   * both `DayZeroRediffTrigger` call sites in `PlayheadRuntime` (the
//     play-time trigger and the Download & Analyze tap) are fire-and-forget
//     `Task.detached` and DISCARD the outcome;
//   * `RediffRefetchProduction`'s `.dayZeroMarked` arm writes rediff state and
//     the day-0 attempt ledger row, and nothing else;
//   * so `receiveAdWindows` was only ever reached later, via the cross-launch
//     `beginEpisode` preload.
//
// Day-0 fires ~19 s into a FIRST listen — exactly when the ads it found are
// still AHEAD of the playhead. playhead-qs0d made those windows genuinely
// auto-skippable (`eligibilityGate = .eligible`, `rediffByteExact` on both
// edges, late-safe padding active), so the gap between "rediff is on" and
// "rediff visibly skipped an ad" is this delivery and nothing else. The user
// had to re-open the episode.
//
// WHAT IS PINNED HERE, in the order the fix has to hold together:
//
//   1. `SkipOrchestrator.ingestPersistedAdWindows` — the mid-session re-read.
//      It admits rows by the SAME rule the cross-launch preload uses, and it
//      no-ops unless the asset is the one currently playing (the check is
//      inside the actor, so there is no window to race).
//   2. `SweepSummary.dayZeroMarkCount` — the honest signal the delivery keys
//      off. Not `rotatedCount`, which means something else.
//   3. `DayZeroRediffTrigger.mintedMarkDelivery` — invoked once, and ONLY for
//      a run that actually persisted rows.
//   4. End to end over REAL divergent MP3 bytes: begin an episode with an
//      empty `ad_windows` table (a true first listen), run day-0, and watch a
//      skip cue appear WITHOUT a second `beginEpisode`. That is Dan's
//      acceptance test in unit form.
//
// THREE NEIGHBOURING CONTRACTS THIS MUST NOT BREAK, each asserted rather than
// assumed:
//
//   * playhead-d3g0 — a suggest window delivered mid-session ARMS and waits for
//     `updatePlayheadTime`; ingest never emits at registration (the reason d3g0
//     gave: `currentPlayheadTime` is a stale 0 between `beginEpisode` and the
//     first observation), and a window still asks at most once per episode.
//   * playhead-ynmk / qs0d — a 9s6q segment-recovered day-0 slot stays
//     `unanchored` + `markOnly`, so mid-session delivery MARKS it and skips
//     nothing.
//   * The preload's own admission rule — a `.reverted` row is not resurrected
//     by the new door.
//
// OBSERVATION METHOD for the banner assertions is d3g0's, verbatim: a
// single-consumer iterator plus a sentinel window driven after the operation
// under test. No sleeps, no polling, no second task — emission is synchronous
// inside the actor and `AsyncStream` buffers on `yield`, so "nothing arrived"
// is a positive observation rather than a timeout.

import AVFoundation
import Foundation
import Testing

@testable import Playhead

// MARK: - Shared fixtures

/// Single-consumer reader over a banner stream (playhead-d3g0's pattern).
private struct DeliveryBannerReader {
    private var iterator: AsyncStream<AdSkipBannerItem>.AsyncIterator

    init(_ stream: AsyncStream<AdSkipBannerItem>) {
        iterator = stream.makeAsyncIterator()
    }

    mutating func drain(until sentinel: String) async -> [AdSkipBannerItem] {
        var collected: [AdSkipBannerItem] = []
        while let item = await iterator.next() {
            if item.windowId == sentinel { return collected }
            collected.append(item)
        }
        return collected
    }
}

private enum DayZeroDeliveryFixture {

    static let assetId = "asset-1"
    static let episodeId = "asset-1"
    /// MUST be the show `makeSkipTestTrustService` seeds. A show with no
    /// profile resolves to `.shadow`, under which NOTHING is auto-skipped and
    /// every cue assertion below would pass for the wrong reason.
    static let podcastId = "podcast-1"

    static func makeAutoOrchestrator(
        store: AnalysisStore,
        assetId: String = assetId,
        episodeId: String = episodeId
    ) async throws -> SkipOrchestrator {
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        return SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
    }

    /// A row shaped exactly like what `mintByteExactDayZeroMarks` persists for
    /// a STRICT (monotonic-clean) byte-exact slot after playhead-qs0d.
    static func makeStrictDayZeroWindow(
        id: String = "day0-strict",
        assetId: String = assetId,
        start: Double = 60,
        end: Double = 120
    ) -> AdWindow {
        makeDayZeroWindow(
            id: id, assetId: assetId, start: start, end: end,
            anchor: .rediffByteExact, gate: .eligible
        )
    }

    /// The 9s6q SEGMENT-RECOVERED lane: minted, but unanchored and mark-only.
    static func makeRecoveredDayZeroWindow(
        id: String = "day0-recovered",
        assetId: String = assetId,
        start: Double = 60,
        end: Double = 120
    ) -> AdWindow {
        makeDayZeroWindow(
            id: id, assetId: assetId, start: start, end: end,
            anchor: .unanchored, gate: .markOnly
        )
    }

    static func makeDayZeroWindow(
        id: String,
        assetId: String,
        start: Double,
        end: Double,
        anchor: AutoSkipEdgeAnchor,
        gate: SkipEligibilityGate,
        decisionState: String = AdDecisionState.candidate.rawValue,
        confidence: Double = 1.0
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: AdDetectionService.dayZeroRediffByteExactBoundaryState,
            decisionState: decisionState,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: start,
            metadataSource: AdDetectionService.dayZeroRediffByteExactMetadataSource,
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: gate.rawValue,
            catalogStoreMatchSimilarity: nil,
            startEdgeAnchor: anchor.rawValue,
            endEdgeAnchor: anchor.rawValue
        )
    }

    static func persist(_ windows: [AdWindow], in store: AnalysisStore) async throws {
        try await store.upsertHotPathAdWindows(windows, existingIDs: [], retiredIDs: [])
    }

    static func cueStart(_ cue: CMTimeRange) -> Double { CMTimeGetSeconds(cue.start) }
    static func cueEnd(_ cue: CMTimeRange) -> Double {
        CMTimeGetSeconds(cue.start + cue.duration)
    }
}

// MARK: - 1. The mid-session re-read

@Suite("Mid-session AdWindow ingest (playhead-96ot)", .timeLimit(.minutes(1)))
struct SkipOrchestratorMidSessionIngestTests {

    private typealias Fx = DayZeroDeliveryFixture

    /// THE bead's headline assertion, in the shape of the acceptance test:
    /// the episode is ALREADY playing when the rows land, and a skip cue
    /// appears without a second `beginEpisode`.
    ///
    /// Before this bead `ingestPersistedAdWindows` did not exist and the only
    /// door into `receiveAdWindows` was the cross-launch preload, so the same
    /// rows produced nothing at all until the user re-opened the episode.
    @Test("a day-0 row persisted MID-SESSION reaches the orchestrator and fires a skip cue")
    func midSessionIngestFiresASkipCue() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeAutoOrchestrator(store: store)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.podcastId
        )
        // Fixture control: a TRUE first listen. If the preload had anything to
        // find, the cue below would prove the preload works, not the ingest.
        #expect(pushedCues.isEmpty, "control: nothing is preloaded on a first listen")
        #expect(await orchestrator.activeWindowIDs().isEmpty)

        // ~19 s into the listen, day-0 mints.
        try await Fx.persist([Fx.makeStrictDayZeroWindow()], in: store)
        let delivered = await orchestrator.ingestPersistedAdWindows(
            analysisAssetId: Fx.assetId
        )

        #expect(delivered == 1, "the minted row was forwarded to receiveAdWindows")
        #expect(await orchestrator.activeWindowIDs() == ["day0-strict"])
        let cue = try #require(pushedCues.first, "a skip cue must fire IN THIS SESSION")
        // playhead-qs0d's late-safe bounds for a both-edges byte-exact span:
        // 60 + 0.50 start margin, 120 - 0.75 end margin - 1.0 pod cushion.
        #expect(Fx.cueStart(cue) == 60.50)
        #expect(Fx.cueEnd(cue) == 118.25)
    }

    /// The asset-identity guard. A day-0 run for an episode the user is NOT
    /// listening to must change nothing live — the rows stay on disk and the
    /// next `beginEpisode` preload picks them up, which is exactly the
    /// pre-96ot behaviour and is correct for that case.
    @Test("ingest for an asset that is NOT the one playing delivers nothing")
    func ingestForAnInactiveAssetDeliversNothing() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeAutoOrchestrator(store: store)
        // A second asset, minted while asset-1 plays.
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-2", episodeId: "ep-2")
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.podcastId
        )
        try await Fx.persist(
            [Fx.makeStrictDayZeroWindow(id: "day0-other", assetId: "asset-2")],
            in: store
        )

        let delivered = await orchestrator.ingestPersistedAdWindows(
            analysisAssetId: "asset-2"
        )

        #expect(delivered == 0, "the minted asset is not the one playing")
        #expect(await orchestrator.activeWindowIDs().isEmpty)
        #expect(pushedCues.isEmpty)
        // The marks are NOT lost — they are on disk for the next launch.
        #expect(try await store.fetchAdWindows(assetId: "asset-2").count == 1)
    }

    /// Nothing is playing at all — the Download & Analyze tap's normal case.
    @Test("ingest with no active episode delivers nothing")
    func ingestWithNoActiveEpisodeDeliversNothing() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeAutoOrchestrator(store: store)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        try await Fx.persist([Fx.makeStrictDayZeroWindow()], in: store)

        let delivered = await orchestrator.ingestPersistedAdWindows(
            analysisAssetId: Fx.assetId
        )

        #expect(delivered == 0, "no beginEpisode ⇒ no live session to deliver to")
        #expect(pushedCues.isEmpty)
    }

    /// The admission rule is the PRELOAD's, not a second one written by hand.
    /// A user-vetoed row must not be resurrected through the new door — the
    /// preload has excluded `.reverted` since cycle-21 H-1 for exactly this
    /// reason, and a divergent filter here would be a silent veto bypass.
    @Test("a user-vetoed row is not resurrected by the mid-session door")
    func revertedRowsAreNotIngested() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeAutoOrchestrator(store: store)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.podcastId
        )
        try await Fx.persist([Fx.makeDayZeroWindow(
            id: "day0-vetoed", assetId: Fx.assetId, start: 60, end: 120,
            anchor: .rediffByteExact, gate: .eligible,
            decisionState: AdDecisionState.reverted.rawValue
        )], in: store)

        let delivered = await orchestrator.ingestPersistedAdWindows(
            analysisAssetId: Fx.assetId
        )

        #expect(delivered == 0, "a reverted row is outside the preload's admission set")
        #expect(await orchestrator.activeWindowIDs().isEmpty)
        #expect(pushedCues.isEmpty)
    }

    /// playhead-ynmk / qs0d, through the new door: a 9s6q segment-recovered
    /// day-0 slot has NOT earned an anchor, so mid-session delivery must MARK
    /// it and skip nothing. Delivering marks in-session is the point of the
    /// bead; delivering a SKIP for an unanchored span would be the 210 s
    /// regression qs0d exists to prevent.
    @Test("a segment-recovered day-0 slot delivered mid-session marks and skips NOTHING")
    func recoveredSlotMarksButNeverSkips() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeAutoOrchestrator(store: store)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.podcastId
        )
        try await Fx.persist([Fx.makeRecoveredDayZeroWindow()], in: store)

        let delivered = await orchestrator.ingestPersistedAdWindows(
            analysisAssetId: Fx.assetId
        )

        #expect(delivered == 1, "the row IS delivered — it just cannot skip")
        #expect(await orchestrator.activeSuggestWindowIDs() == ["day0-recovered"],
                "markOnly routes to the suggest tier")
        #expect(await orchestrator.activeWindowIDs().isEmpty,
                "an unanchored mark-only span never enters the auto-skip map")
        #expect(pushedCues.isEmpty, "presence, not extent — nothing is skipped")
    }
}

// MARK: - 2. Mid-session delivery vs playhead-d3g0's arming

@Suite("Mid-session ingest and the suggest entry gate (playhead-96ot × d3g0)",
       .timeLimit(.minutes(1)))
struct MidSessionIngestSuggestArmingTests {

    private typealias Fx = DayZeroDeliveryFixture

    private static let sentinelId = "96ot-sentinel"

    private static func fireSentinel(_ orchestrator: SkipOrchestrator, at time: Double) async {
        let sentinel = Fx.makeRecoveredDayZeroWindow(
            id: sentinelId, start: time, end: time + 4
        )
        await orchestrator.receiveAdWindows([sentinel])
        await orchestrator.updatePlayheadTime(time)
    }

    /// d3g0's hazard, from the new side. `currentPlayheadTime` is a stale 0
    /// between `beginEpisode` and the first position observation, which is why
    /// d3g0 deliberately does NOT emit at registration. The mid-session door
    /// must not reintroduce that: it ARMS, and the next observation presents.
    ///
    /// The span here CONTAINS the live playhead, so an emit-at-registration
    /// implementation would look correct on the banner count while being wrong
    /// about where the emission came from — the assertion is therefore ordered
    /// against a sentinel rather than counted.
    @Test("mid-session ingest ARMS a suggestion; the next playhead observation presents it")
    func ingestArmsAndTheNextObservationEmits() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeAutoOrchestrator(store: store)
        var reader = DeliveryBannerReader(await orchestrator.bannerItemStream())
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.podcastId
        )
        // The listener is already INSIDE the span when the mint lands.
        await orchestrator.updatePlayheadTime(70)
        try await Fx.persist([Fx.makeRecoveredDayZeroWindow()], in: store)

        await orchestrator.ingestPersistedAdWindows(analysisAssetId: Fx.assetId)
        // Nothing yet: ingest arms, it does not present.
        await Self.fireSentinel(orchestrator, at: 400)
        let beforeObservation = await reader.drain(until: Self.sentinelId)
        #expect(beforeObservation.isEmpty,
                "ingest must not emit at registration — d3g0 owns the trigger")

        // The next observation inside the span presents it.
        await orchestrator.updatePlayheadTime(71)
        await Self.fireSentinel(orchestrator, at: 500)
        let afterObservation = await reader.drain(until: Self.sentinelId)
        #expect(afterObservation.map(\.windowId) == ["day0-recovered"])
    }

    /// d3g0's once-per-window guarantee, under a producer that now has a second
    /// door. Day-0 can re-deliver the same rows (a Download & Analyze tap after
    /// a play-time trigger, or the preload plus an ingest), and a repeat
    /// delivery of an UNCHANGED revision must not re-ask.
    @Test("repeated mid-session ingest of the same row asks at most once")
    func repeatedIngestAsksOnce() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeAutoOrchestrator(store: store)
        var reader = DeliveryBannerReader(await orchestrator.bannerItemStream())
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.podcastId
        )
        try await Fx.persist([Fx.makeRecoveredDayZeroWindow()], in: store)

        await orchestrator.ingestPersistedAdWindows(analysisAssetId: Fx.assetId)
        await orchestrator.updatePlayheadTime(70)
        await orchestrator.ingestPersistedAdWindows(analysisAssetId: Fx.assetId)
        await orchestrator.updatePlayheadTime(71)
        await orchestrator.ingestPersistedAdWindows(analysisAssetId: Fx.assetId)
        await orchestrator.updatePlayheadTime(72)

        await Self.fireSentinel(orchestrator, at: 400)
        let emitted = await reader.drain(until: Self.sentinelId)
        #expect(emitted.map(\.windowId) == ["day0-recovered"],
                "three ingests and three ticks inside the span still ask ONCE")
    }

    /// A span the playhead has already passed offers nothing to skip, so a
    /// late mint over it must stay silent — d3g0's `[start, end)` rule, applied
    /// to material that arrives after the fact. This is the common case for a
    /// day-0 run that finishes after a pre-roll has played.
    @Test("a mid-session mint BEHIND the playhead never banners")
    func mintBehindThePlayheadStaysSilent() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeAutoOrchestrator(store: store)
        var reader = DeliveryBannerReader(await orchestrator.bannerItemStream())
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.podcastId
        )
        await orchestrator.updatePlayheadTime(300)
        try await Fx.persist([Fx.makeRecoveredDayZeroWindow()], in: store)

        await orchestrator.ingestPersistedAdWindows(analysisAssetId: Fx.assetId)
        await orchestrator.updatePlayheadTime(301)

        await Self.fireSentinel(orchestrator, at: 400)
        let emitted = await reader.drain(until: Self.sentinelId)
        #expect(emitted.isEmpty, "the audio is already gone — asking about it is the d3g0 defect")
    }
}

// MARK: - 3. The trigger's delivery decision

@Suite("Day-0 trigger delivers its minted marks (playhead-96ot)")
struct DayZeroTriggerMarkDeliveryTests {

    private static let day = RediffRefetchPolicy.Configuration.secondsPerDay
    private static let enclosure = URL(string: "https://cdn.example.com/current.mp3")!
    private static let played = URL(fileURLWithPath: "/tmp/played-a.mp3")

    private final class DeliverySpy: @unchecked Sendable {
        private(set) var assetIds: [String] = []
        func record(_ assetId: String) { assetIds.append(assetId) }
    }

    private static func makeTrigger(
        minter: SpyDayZeroMinter,
        delivery: DeliverySpy,
        reachability: TransportSnapshot.Reachability = .wifi,
        isCharging: Bool = true
    ) -> DayZeroRediffTrigger {
        let service = RediffRefetchService(
            enabled: true,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: KWaySpyFullFetcher(),
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: SpyRefetchRecorder(),
            fileRemover: SpyTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            dayZeroMinter: minter,
            now: { 100 * day }
        )
        return DayZeroRediffTrigger(
            service: service,
            enabled: true,
            kWayFetchCount: 2,
            reachabilityProvider: { reachability },
            chargeStateProvider: { isCharging },
            deepScanOptInProvider: { false },
            attemptRecordProvider: { _ in nil },
            suppressionRecorder: { _, _, _ in },
            mintedMarkDelivery: { delivery.record($0) }
        )
    }

    @discardableResult
    private static func fire(_ trigger: DayZeroRediffTrigger)
        async -> RediffRefetchService.SweepSummary {
        await trigger.triggerIfEligible(
            analysisAssetId: "asset-day0",
            enclosureURL: enclosure,
            playedFileURL: played
        )
    }

    /// The counter the delivery keys off. `rotatedCount` is ALSO 1 here — for
    /// the unrelated reason that a mark resolves the shared attempt state — so
    /// the two are asserted separately: they agree on this run and mean
    /// different things.
    @Test("a marked day-0 run reports its MARK count, distinct from rotatedCount")
    func summaryCarriesTheMarkCount() async {
        let minter = SpyDayZeroMinter()
        minter.markCountToReturn = 3
        let summary = await Self.fire(
            Self.makeTrigger(minter: minter, delivery: DeliverySpy())
        )
        #expect(summary.dayZeroMarkCount == 3, "three AdWindows were persisted")
        #expect(summary.rotatedCount == 1, "one candidate resolved — a different fact")
    }

    @Test("a marked day-0 run delivers its asset id EXACTLY once")
    func markedRunDelivers() async {
        let delivery = Self.DeliverySpy()
        let minter = SpyDayZeroMinter()   // 1 mark by default
        await Self.fire(Self.makeTrigger(minter: minter, delivery: delivery))
        #expect(delivery.assetIds == ["asset-day0"])
    }

    /// The narrowing that keeps the delivery honest. An unmarked run persisted
    /// NOTHING, so re-reading `ad_windows` would forward only rows the session
    /// already has — churn dressed up as progress.
    @Test("an UNMARKED day-0 run delivers nothing")
    func unmarkedRunDeliversNothing() async {
        let delivery = Self.DeliverySpy()
        let minter = SpyDayZeroMinter()
        minter.markCountToReturn = 0
        let summary = await Self.fire(Self.makeTrigger(minter: minter, delivery: delivery))
        #expect(summary.dayZeroMarkCount == 0)
        #expect(delivery.assetIds.isEmpty)
    }

    /// An `allSlotsAlreadyCovered` exit also mints zero rows, and is the exit a
    /// REPLAY takes. Pinned separately from `noDivergentSlot` because it is the
    /// one that fires when the session demonstrably already has the windows.
    @Test("a run whose slots were already covered delivers nothing")
    func alreadyCoveredRunDeliversNothing() async {
        let delivery = Self.DeliverySpy()
        let minter = SpyDayZeroMinter()
        minter.markCountToReturn = 0
        minter.unmarkedExitToReturn = .allSlotsAlreadyCovered
        await Self.fire(Self.makeTrigger(minter: minter, delivery: delivery))
        #expect(delivery.assetIds.isEmpty)
    }

    /// The live gate still runs first. A cellular play never fetches, never
    /// mints, and therefore must never deliver.
    @Test("a gate-blocked run delivers nothing")
    func gateBlockedRunDeliversNothing() async {
        let delivery = Self.DeliverySpy()
        let minter = SpyDayZeroMinter()
        await Self.fire(Self.makeTrigger(
            minter: minter, delivery: delivery, reachability: .cellular
        ))
        #expect(minter.calls.isEmpty, "control: the gate blocked before the mint")
        #expect(delivery.assetIds.isEmpty)
    }
}

// MARK: - 4. End to end: a first listen skips in the session that minted it

@Suite("Day-0 first listen skips IN SESSION (playhead-96ot)", .timeLimit(.minutes(2)))
struct DayZeroFirstListenInSessionSkipTests {

    private typealias Fx = DayZeroDeliveryFixture
    private static let day = RediffRefetchPolicy.Configuration.secondsPerDay
    private static let enclosure = URL(string: "https://cdn.example.com/current.mp3")!

    /// A/B pair with ONE ID3-separated ad block over ≈[95, 165] s — the same
    /// construction the qs0d and activation-wiring suites use, and a chain the
    /// byte gate accepts on its STRICT (monotonic-clean) arm.
    private struct StrictDivergentTriple {
        let aURL: URL
        let b0: URL
        let b1: URL
        let aData: Data
        let bData: Data

        static func stage(in dir: URL) throws -> StrictDivergentTriple {
            let adStartFrame = 3637      // ≈ 95.008 s
            let adFrames = 2680          // ≈ 70.008 s
            let contentFrames = 10_719   // ≈ 280.0 s of played (A) audio
            let c1 = SyntheticMP3.frames(count: adStartFrame, seed: 0xC0FFEE)
            let c2 = SyntheticMP3.frames(count: contentFrames - adStartFrame - adFrames, seed: 0xFACADE)
            let ad = SyntheticMP3.frames(count: adFrames, seed: 0xAD_B10C)
            let aData = SyntheticMP3.file(c1 + [SyntheticMP3.id3v2(payloadBytes: 32)] + ad + c2)
            let bData = SyntheticMP3.file(c1 + c2)
            let aURL = dir.appendingPathComponent("a.mp3", isDirectory: false)
            let b0 = dir.appendingPathComponent("b0.fresh.mp3", isDirectory: false)
            let b1 = dir.appendingPathComponent("b1.fresh.mp3", isDirectory: false)
            try aData.write(to: aURL)
            try bData.write(to: b0)
            try bData.write(to: b1)
            return StrictDivergentTriple(aURL: aURL, b0: b0, b1: b1, aData: aData, bData: bData)
        }
    }

    private final class StubDecoder: AudioFileDecoding, @unchecked Sendable {
        func decodeMono16kHz(fileURL: URL) async throws -> [Float] { [] }
    }

    private static func makeAdService(store: AnalysisStore) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40, confirmationThreshold: 0.70,
                suppressionThreshold: 0.25, hotPathLookahead: 90.0,
                detectorVersion: "96ot-test", fmBackfillMode: .off,
                rediffSlotOwnershipEnabled: true
            ),
            rediffBSideProvider: RediffBSideStagingProvider(
                decoder: StubDecoder(), durationProbe: { _ in nil }
            )
        )
    }

    private static func makeTrigger(
        store: AnalysisStore,
        bFiles: [URL],
        delivery: @escaping @Sendable (String) async -> Void
    ) -> DayZeroRediffTrigger {
        let service = RediffRefetchService(
            enabled: true,
            config: .production,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: RealFilesKWayFetcher(files: bFiles),
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: SpyRefetchRecorder(),
            fileRemover: FileManagerTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            dayZeroMinter: AdDetectionDayZeroByteExactMinter(
                adDetection: makeAdService(store: store)
            ),
            now: { 100 * day }
        )
        return DayZeroRediffTrigger(
            service: service,
            enabled: true,
            kWayFetchCount: 2,
            reachabilityProvider: { .wifi },
            chargeStateProvider: { true },
            deepScanOptInProvider: { false },
            attemptRecordProvider: { _ in nil },
            suppressionRecorder: { _, _, _ in },
            mintedMarkDelivery: delivery
        )
    }

    private static func insertAsset(
        store: AnalysisStore,
        assetId: String,
        episodeId: String,
        sourceURL: String
    ) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: assetId, episodeId: episodeId, assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil, sourceURL: sourceURL,
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil, episodeDurationSec: 280
        ))
    }

    /// DAN'S ACCEPTANCE TEST, in unit form.
    ///
    /// The episode is playing, `ad_windows` is empty (a true first listen — no
    /// transcript, no analysis), day-0 byte-aligns two real B copies against
    /// the pinned A-side, mints, and a SKIP CUE appears — with no second
    /// `beginEpisode` anywhere in this test. On main the same sequence ends
    /// with the rows on disk and the orchestrator holding nothing.
    @Test("a first-listen day-0 mint produces a skip cue WITHOUT relaunching the episode")
    func firstListenMintSkipsInSession() async throws {
        let dir = try makeTempDir(prefix: "Ot96FirstListen")
        defer { try? FileManager.default.removeItem(at: dir) }
        let triple = try StrictDivergentTriple.stage(in: dir)
        // Fixture control: this pair really does take the STRICT arm, which is
        // the only arm qs0d promotes to auto-skip. Without it the test would
        // silently assert the mark-only lane.
        try #require(
            RediffByteAligner.align(aData: triple.aData, bData: triple.bData).monotonicClean,
            "fixture control: a single removed-in-B block must chain monotonically"
        )
        try #require(RediffActivation.dayZeroByteExactAutoSkipEnabled,
                     "fixture control: qs0d's promotion must be ON for a cue to fire")

        let store = try await makeTestStore()
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        try await Self.insertAsset(
            store: store, assetId: "asset-fl", episodeId: "ep-fl",
            sourceURL: triple.aURL.absoluteString
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }

        // THE SESSION. One beginEpisode, at the top, over an empty table.
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-fl",
            episodeId: "ep-fl",
            podcastId: Fx.podcastId
        )
        #expect(pushedCues.isEmpty, "control: a true first listen preloads nothing")

        let trigger = Self.makeTrigger(
            store: store,
            bFiles: [triple.b0, triple.b1],
            delivery: { assetId in
                await orchestrator.ingestPersistedAdWindows(analysisAssetId: assetId)
            }
        )
        let summary = await trigger.triggerIfEligible(
            analysisAssetId: "asset-fl",
            enclosureURL: Self.enclosure,
            playedFileURL: triple.aURL
        )

        #expect(summary.dayZeroMarkCount == 1, "control: the byte differ minted one slot")
        let cue = try #require(
            pushedCues.first,
            "the ad day-0 just found must be skippable IN THIS SESSION"
        )
        // ≈95.008 + 0.50 start margin; ≈165.016 - 0.75 end margin - 1.0 cushion.
        #expect(Fx.cueStart(cue) >= 95.4 && Fx.cueStart(cue) <= 95.7,
                "late-safe start ≈95.5, got \(Fx.cueStart(cue))")
        #expect(Fx.cueEnd(cue) >= 163.1 && Fx.cueEnd(cue) <= 163.4,
                "late-safe end ≈163.27, got \(Fx.cueEnd(cue))")
        #expect(await orchestrator.activeWindowIDs().count == 1)
    }

    /// The same run, for an episode the listener is NOT on. The marks land on
    /// disk (so the next launch preloads them) and the live session is
    /// untouched — no cue, no window, no banner for audio in another episode.
    @Test("a day-0 mint for a DIFFERENT asset persists its marks and leaves the session alone")
    func mintForAnotherAssetLeavesTheSessionAlone() async throws {
        let dir = try makeTempDir(prefix: "Ot96OtherAsset")
        defer { try? FileManager.default.removeItem(at: dir) }
        let triple = try StrictDivergentTriple.stage(in: dir)

        let store = try await makeTestStore()
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        try await Self.insertAsset(
            store: store, assetId: "asset-playing", episodeId: "ep-playing",
            sourceURL: "file:///tmp/playing.mp3"
        )
        try await Self.insertAsset(
            store: store, assetId: "asset-minted", episodeId: "ep-minted",
            sourceURL: triple.aURL.absoluteString
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-playing",
            episodeId: "ep-playing",
            podcastId: Fx.podcastId
        )

        let trigger = Self.makeTrigger(
            store: store,
            bFiles: [triple.b0, triple.b1],
            delivery: { assetId in
                await orchestrator.ingestPersistedAdWindows(analysisAssetId: assetId)
            }
        )
        let summary = await trigger.triggerIfEligible(
            analysisAssetId: "asset-minted",
            enclosureURL: Self.enclosure,
            playedFileURL: triple.aURL
        )

        #expect(summary.dayZeroMarkCount == 1, "control: the mint still happened")
        #expect(try await store.fetchAdWindows(assetId: "asset-minted").count == 1,
                "the marks are durable — the next launch preloads them")
        #expect(pushedCues.isEmpty, "the listening session is untouched")
        #expect(await orchestrator.activeWindowIDs().isEmpty)
    }
}
