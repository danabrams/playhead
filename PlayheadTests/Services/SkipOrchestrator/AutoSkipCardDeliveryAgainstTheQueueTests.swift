// AutoSkipCardDeliveryAgainstTheQueueTests.swift
// playhead-8cjo: THE PARTITION, ASSERTED AGAINST THE QUEUE.
//
// `BannerPlayheadBiconditionalTests` pins `cards ∪ list == entered` and
// `cards ∩ list == ∅` at the ORCHESTRATOR BOUNDARY, and its own header says it
// cannot see the queue. That blind spot is this bead: `emitBannerItem` branched
// on `!bannerContinuations.isEmpty || !bannerEventContinuations.isEmpty` —
// "somebody is SUBSCRIBED" — and `AdBannerQueue.enqueue(_:hostGeneration:)`
// returns `false` and DROPS the item afterwards whenever the host generation
// has moved on: the reattach window in
// `NowPlayingView.onChange(of: bannerPlaybackContext)`, a
// `discardAllOnHostDisappear` whose stream has not torn down yet, or an item
// whose episode / playback lifecycle does not match the host's. In that window
// the skip produced NEITHER a card the listener saw NOR a row they could
// correct, and the orchestrator had already booked it as delivered.
//
// WHAT THIS SUITE CAN SEE THAT THE OLD RAIL CANNOT. It drives a REAL
// `SkipOrchestrator` into a REAL `AdBannerQueue` through the REAL forwarding
// rule — `BannerHostDelivery.forward`, the same function
// `NowPlayingViewModel.observeBanners` runs, not a re-implementation of it. So
// `queue.currentBanner` and `deliveredAutoSkipCardWindowIDs()` are two
// witnesses and the tests below assert BOTH — independent for a single card
// into an empty lane, which is what every case here drives, and NOT
// independent for a card queued behind another (see the stated-limit test at
// the bottom of this file, which drives exactly that and records what today's
// code does). The claim they support is: a
// rejected enqueue must leave the queue empty AND the list holding a row, and
// an accepted one must show the card AND clear the row. A test that re-spelled
// the forwarding would only ever prove that two call sites agree today, which
// is exactly how the auto tier came to have no acknowledgement while the
// suggest tier had one.
//
// THE THIRD CASE, which no queue can report: the observation `Task` is
// cancelled between the yield and the enqueue, so nothing reaches the queue at
// all and nothing comes back. `anEventNobodyForwardsLeavesAListRow` is that
// one, and it is why the acknowledgement is POSITIVE-ONLY: a design that waited
// for a rejection to be reported would book that skip as a card forever.
//
// OBSERVATION METHOD — the sentinel frame boundary of
// `BannerPlayheadBiconditionalTests`, over the EVENT stream. Emission is
// synchronous inside the actor and `AsyncStream` buffers on `yield`, so by the
// time an awaited orchestrator call returns everything it emitted is already in
// the buffer; a sentinel driven after the step under test makes "nothing
// arrived" a positive observation rather than a timeout.

import Foundation
import Testing

@testable import Playhead

// MARK: - Reader

/// Single-consumer reader over the production banner EVENT stream.
private struct BannerEventFrameReader {
    private var iterator: AsyncStream<AdBannerStreamEvent>.AsyncIterator

    init(_ stream: AsyncStream<AdBannerStreamEvent>) {
        iterator = stream.makeAsyncIterator()
    }

    /// Everything that arrived before the sentinel's own `.present`.
    ///
    /// BOUNDED, so a sentinel that never arrives fails with its own name rather
    /// than on the suite's `.timeLimit`. An unbounded `while let` here would
    /// turn "the frame boundary stopped being emitted" into a 60 s timeout,
    /// which in this repo's gate baseline is indistinguishable from one more
    /// starvation flake — the loud failure would be filed as noise.
    mutating func drain(until sentinel: String) async -> [AdBannerStreamEvent] {
        var collected: [AdBannerStreamEvent] = []
        for _ in 0..<Self.maxEventsPerFrame {
            guard let event = await iterator.next() else { return collected }
            if case let .present(item) = event, item.windowId == sentinel {
                return collected
            }
            collected.append(event)
        }
        Issue.record(
            """
            the sentinel '\(sentinel)' never arrived within \
            \(Self.maxEventsPerFrame) events. The frame boundary is itself a \
            suggest banner, so this means the suggest tier stopped emitting — \
            not that the auto tier under test is wrong.
            """
        )
        return collected
    }

    /// Generous: a frame carries at most a handful of events. Large enough that
    /// it can never bound a healthy run, small enough to fail in milliseconds.
    private static let maxEventsPerFrame = 64
}

@Suite(
    "playhead-8cjo — a card the QUEUE refused is a list row, not a delivery",
    .timeLimit(.minutes(1))
)
struct AutoSkipCardDeliveryAgainstTheQueueTests {

    // MARK: Fixture — the 2026-08-21 episode again, so the two suites agree

    private static let assetId = "asset-1"
    private static let episodeId = "asset-1"
    /// MUST be the show `makeSkipTestTrustService` seeds, or nothing reaches
    /// `.applied` and every test here passes vacuously.
    private static let podcastId = "podcast-1"
    private static let episodeDuration: Double = 4309.42
    private static let playbackLifecycleGeneration: UInt64 = 1

    private static let preRoll = (id: "preroll", start: 0.0, end: 86.831)
    private static let midRollOne =
        (id: "midroll-1", start: 1369.809, end: 1548.487)
    private static let midRollTwo =
        (id: "midroll-2", start: 3367.262, end: 3534.576)
    private static let postRoll =
        (id: "postroll", start: 4279.302, end: 4309.420)

    /// Parked in program audio, inside NO window under test and inside the
    /// EPISODE — a sentinel past the asset's duration is retired by
    /// `InventorySanityFilter`'s tail-edge rule before it reaches a tier, and a
    /// frame boundary that can be filtered out is not a frame boundary.
    private static let sentinelStart: Double = 1000

    private static func autoWindow(
        id: String,
        start: Double,
        end: Double
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: 1.0,
            boundaryState: "lexical",
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: start,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue,
            startEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue
        )
    }

    /// A `markOnly` window used only as a frame boundary.
    private static func sentinelWindow(id: String) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: sentinelStart,
            endTime: sentinelStart + 4,
            confidence: 0.41,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: sentinelStart,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
    }

    private static func makeOrchestrator() async throws
        -> (orchestrator: SkipOrchestrator, store: AnalysisStore) {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: assetId,
                episodeId: episodeId,
                episodeDurationSec: episodeDuration
            )
        )
        let trust = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trust,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: podcastId,
            playbackLifecycleGeneration: playbackLifecycleGeneration
        )
        return (orchestrator, store)
    }

    /// A queue whose dwell never elapses.
    ///
    /// The default is an 8 s `Task.sleep`, and `currentBanner` is one of the two
    /// witnesses these tests turn on — under the full plan a test can be starved
    /// for far longer than eight seconds, so the default would make every
    /// `currentBanner` assertion here a race against a wall clock. That is the
    /// load-sensitive shape `scripts/perf-tests.sh` exists to keep out of the
    /// gate, and injecting the dwell removes it rather than widening a margin.
    @MainActor
    private static func makeQueue() -> AdBannerQueue {
        AdBannerQueue(
            autoDismissSleep: { _ in
                try? await Task.sleep(for: .seconds(86_400))
            }
        )
    }

    /// Drive one observation and hand back everything the orchestrator emitted
    /// for it, the sentinel excluded.
    private static func step(
        _ orchestrator: SkipOrchestrator,
        _ reader: inout BannerEventFrameReader,
        to time: Double,
        index: Int
    ) async -> [AdBannerStreamEvent] {
        await orchestrator.updatePlayheadTime(time)
        let sentinelId = "8cjo-sentinel-\(index)"
        await orchestrator.receiveAdWindows([sentinelWindow(id: sentinelId)])
        await orchestrator.updatePlayheadTime(sentinelStart)
        return await reader.drain(until: sentinelId)
    }

    /// The production forwarding rule, verbatim — this is the function
    /// `NowPlayingViewModel.observeBanners` runs for every event.
    private static func forward(
        _ events: [AdBannerStreamEvent],
        _ orchestrator: SkipOrchestrator,
        _ queue: AdBannerQueue,
        hostGeneration: UInt64
    ) async {
        for event in events {
            await BannerHostDelivery.forward(
                event,
                from: orchestrator,
                into: queue,
                hostGeneration: hostGeneration
            )
        }
        // playhead-pzojm: the forwarding rule no longer acknowledges the auto
        // tier. The DISPLAY BOUNDARY does — `recordBannerShown(for:)`, which
        // `NowPlayingView` calls from the card's `onAppear`, and whose
        // `onAutoSkipCardPresented` the view wires to the orchestrator. This
        // helper plays the view for that one step, and does the hop
        // synchronously rather than through the view's `Task`, so a test reads
        // a settled orchestrator, never a racing one.
        //
        // A card the queue REFUSED is never current, so nothing here fires
        // for it — which is exactly the refusal tests' claim, unchanged. A
        // card queued BEHIND another is not current either, and the
        // presentation guard makes a second call for the same current card
        // a no-op: so an ad pod's second card is acknowledged only once it is
        // actually shown.
        let presented = await MainActor.run { () -> AdSkipBannerItem? in
            guard let current = queue.currentBanner,
                  current.tier == .autoSkipped,
                  queue.recordBannerShown(for: current)
            else { return nil }
            return current
        }
        if let presented {
            await orchestrator.acknowledgeAutoSkippedBannerDelivery(
                windowId: presented.windowId,
                episodeId: presented.episodeId,
                playbackLifecycleGeneration: presented.playbackLifecycleGeneration,
                windowMaterialRevisionToken: presented.windowMaterialRevisionToken
            )
        }
    }

    // MARK: - 1. The queue took it: a card, and no row

    /// The control, and the thing that makes every rejection test below
    /// non-vacuous: when the host and the item agree, the queue really does
    /// present the card and the receipt really does go away.
    @MainActor
    @Test("The queue accepts, so the skip is a card and leaves no row")
    func anAcceptedEnqueueIsACardAndClearsTheReceipt() async throws {
        let (orchestrator, _) = try await Self.makeOrchestrator()
        var reader = BannerEventFrameReader(
            await orchestrator.bannerEventStream()
        )
        let queue = Self.makeQueue()
        let hostGeneration = queue.activateHost(
            for: Self.episodeId,
            playbackLifecycleGeneration: Self.playbackLifecycleGeneration
        )

        await orchestrator.receiveAdWindows([
            Self.autoWindow(
                id: Self.preRoll.id,
                start: Self.preRoll.start,
                end: Self.preRoll.end
            )
        ])
        let events = await Self.step(orchestrator, &reader, to: 40, index: 0)
        await Self.forward(
            events, orchestrator, queue, hostGeneration: hostGeneration
        )

        #expect(
            queue.currentBanner?.windowId == Self.preRoll.id,
            """
            the queue is showing \(String(describing: queue.currentBanner?.windowId)) \
            after an enqueue it should have accepted. Every rejection test in \
            this suite is measured against this one, so a fixture that cannot \
            get a card presented makes all of them vacuous.
            """
        )
        let delivered = await orchestrator.deliveredAutoSkipCardWindowIDs()
        #expect(
            delivered == [Self.preRoll.id],
            """
            the queue accepted the card and the orchestrator recorded \
            \(delivered.sorted()) as delivered. This is the control every \
            refusal test in this suite is measured against.
            """
        )
        // The RAW dictionary as well as the filtered read. MS06 survived twice
        // on an empty filtered read taken as evidence of a removal that had
        // not happened; `missedAutoSkipReceipts()` filters on `windows[…]`,
        // `.applied` and a token match, so it can go quiet for reasons that
        // have nothing to do with the acknowledgement.
        #expect(
            await orchestrator._missedAutoSkipReceiptCountForTesting() == 0,
            "the receipt is still in the dictionary; the filtered read merely hides it"
        )
        let list = await orchestrator.missedAutoSkipReceipts()
        #expect(
            list.isEmpty,
            """
            the listener was shown a card and the missed-skip list still holds \
            \(list.map(\.windowId)). The list is the ELSE branch of the card, \
            not a parallel ledger.
            """
        )
    }

    // MARK: - 2. THE BEAD: the queue refused, three ways

    /// A REJECTED ENQUEUE IS A LIST ROW. Parameterised over the three ways
    /// `AdBannerQueue` refuses an item.
    ///
    /// TWO OF THE THREE REACH THE SAME CLAUSE, and saying so is more useful
    /// than the tidier claim this comment used to make. `activateHost` and
    /// `discardAllOnHostDisappear` BOTH do `hostGeneration &+= 1`, so the
    /// stale-generation and host-disappeared arms are both refused by the
    /// generation clause; only the episode-mismatch arm reaches
    /// `acceptsHostScopedItem`. The `isHostActive` clause is unreachable from
    /// outside because the bump is atomic with it — a caller cannot obtain the
    /// post-disappear generation to test it with. The three arms are still
    /// worth walking: they are three different PRODUCTION MOMENTS, and a
    /// future change that stopped bumping the generation on disappear would
    /// leave the second arm as the only thing looking at `isHostActive`.
    ///
    ///   * a stale HOST GENERATION — the reattach window in
    ///     `NowPlayingView.onChange(of: bannerPlaybackContext)`, where the old
    ///     observation task is still delivering into a queue that has already
    ///     been re-activated;
    ///   * a host that DISAPPEARED (`discardAllOnHostDisappear`) whose stream
    ///     has not torn down yet;
    ///   * an item whose EPISODE does not match the host's, which is the
    ///     orchestrator still serving the previous episode while
    ///     `PlayheadRuntime` finishes its asynchronous setup.
    @MainActor
    @Test(
        "A rejected enqueue leaves a list row, not a card",
        arguments: [
            QueueRefusal.staleHostGeneration,
            QueueRefusal.hostDisappeared,
            QueueRefusal.episodeMismatch,
        ]
    )
    func aRejectedEnqueueBecomesAListRow(refusal: QueueRefusal) async throws {
        let (orchestrator, _) = try await Self.makeOrchestrator()
        var reader = BannerEventFrameReader(
            await orchestrator.bannerEventStream()
        )
        let queue = Self.makeQueue()
        // The generation the observation task was started with — exactly what
        // `observeBanners` captures at `onAppear`.
        var hostGeneration = queue.activateHost(
            for: Self.episodeId,
            playbackLifecycleGeneration: Self.playbackLifecycleGeneration
        )
        switch refusal {
        case .staleHostGeneration:
            // The host re-activates; the in-flight observation task keeps the
            // OLD generation, which is the whole point of the generation.
            _ = queue.activateHost(
                for: Self.episodeId,
                playbackLifecycleGeneration: Self.playbackLifecycleGeneration
            )
        case .hostDisappeared:
            queue.discardAllOnHostDisappear()
        case .episodeMismatch:
            hostGeneration = queue.activateHost(
                for: "a-different-episode",
                playbackLifecycleGeneration: Self.playbackLifecycleGeneration
            )
        }

        await orchestrator.receiveAdWindows([
            Self.autoWindow(
                id: Self.preRoll.id,
                start: Self.preRoll.start,
                end: Self.preRoll.end
            )
        ])
        let events = await Self.step(orchestrator, &reader, to: 40, index: 0)

        // NON-VACUITY, and the half that separates this bead from
        // playhead-2d6i: the item really was yielded to a live subscriber. If
        // it were not, this would be 2d6i's unattached case under a new name.
        let yielded = await orchestrator.emittedAutoSkipBannersSnapshot()
        #expect(
            yielded.contains(Self.preRoll.id),
            """
            \(refusal): nothing reached the yield-to-subscriber path, so the \
            queue was never offered anything to refuse and this test says \
            nothing about playhead-8cjo.
            """
        )

        await Self.forward(
            events, orchestrator, queue, hostGeneration: hostGeneration
        )

        #expect(
            queue.currentBanner == nil,
            """
            \(refusal): the queue presented \
            \(String(describing: queue.currentBanner?.windowId)) after an \
            enqueue it should have refused. The fixture is not reproducing the \
            refusal it names.
            """
        )
        let delivered = await orchestrator.deliveredAutoSkipCardWindowIDs()
        #expect(
            delivered.isEmpty,
            """
            \(refusal): \(delivered.sorted()) is booked as a card the listener \
            saw, and the queue threw it away. That is playhead-8cjo verbatim — \
            "a host is attached" is not "a card was shown".
            """
        )
        let list = await orchestrator.missedAutoSkipReceipts()
        #expect(
            list.map(\.windowId) == [Self.preRoll.id],
            """
            \(refusal): the skip fired, the queue binned the card, and the \
            missed-skip list holds \(list.map(\.windowId)). A skip with \
            neither a card nor a row is a false skip the listener cannot see \
            and therefore cannot correct.
            """
        )
    }

    /// THE CASE NO QUEUE CAN REPORT, and the reason the acknowledgement is
    /// positive-only.
    ///
    /// `observeBanners` guards its loop body on `!Task.isCancelled`, and
    /// `NowPlayingView` cancels that task on `onDisappear` and on every
    /// `bannerPlaybackContext` change. An item yielded a moment earlier is then
    /// never enqueued at all — no rejection, no acceptance, nothing. A design
    /// that booked the card and waited for a rejection to arrive would leave
    /// this skip permanently uncorrectable, which is why "no acknowledgement"
    /// means NOT DELIVERED rather than "assume it landed".
    @MainActor
    @Test("An event nobody forwards leaves a list row — silence is not a delivery")
    func anEventNobodyForwardsLeavesAListRow() async throws {
        let (orchestrator, _) = try await Self.makeOrchestrator()
        var reader = BannerEventFrameReader(
            await orchestrator.bannerEventStream()
        )
        let queue = Self.makeQueue()
        _ = queue.activateHost(
            for: Self.episodeId,
            playbackLifecycleGeneration: Self.playbackLifecycleGeneration
        )

        await orchestrator.receiveAdWindows([
            Self.autoWindow(
                id: Self.preRoll.id,
                start: Self.preRoll.start,
                end: Self.preRoll.end
            )
        ])
        // Drained and DROPPED: the observation task died between the yield and
        // the enqueue.
        let events = await Self.step(orchestrator, &reader, to: 40, index: 0)

        // NON-VACUITY. Without this the test passes when NOTHING was emitted at
        // all, which is a different situation entirely — and the one where the
        // fixture, not the code, is broken.
        #expect(
            events.contains {
                if case let .present(item) = $0 {
                    return item.windowId == Self.preRoll.id
                }
                return false
            },
            """
            the skip was never announced, so nothing was there to go unforwarded \
            and this test says nothing about silence being treated as a delivery.
            """
        )
        #expect(queue.currentBanner == nil)
        let delivered = await orchestrator.deliveredAutoSkipCardWindowIDs()
        #expect(delivered.isEmpty)
        let list = await orchestrator.missedAutoSkipReceipts()
        #expect(
            list.map(\.windowId) == [Self.preRoll.id],
            """
            nothing ever reached the queue and the skip left \
            \(list.map(\.windowId)). The listener heard the show jump and has \
            nothing to say No to.
            """
        )
    }

    /// A ROW FROM A REJECTED ENQUEUE IS A REAL CORRECTION, not a placeholder.
    ///
    /// Every rail above could be satisfied by a row that exists and cannot be
    /// acted on, which is the decorative outcome playhead-2d6i made the whole
    /// feature conditional on avoiding. So the row goes through
    /// `denyAutoSkippedBanner` — the seam a card's No reaches — and the durable
    /// receipt is read back.
    @MainActor
    @Test("A row created by a refused enqueue still reaches the card's own veto")
    func aRowFromARefusedEnqueueStillReachesTheVeto() async throws {
        let (orchestrator, store) = try await Self.makeOrchestrator()
        var reader = BannerEventFrameReader(
            await orchestrator.bannerEventStream()
        )
        let queue = Self.makeQueue()
        let staleGeneration = queue.activateHost(
            for: Self.episodeId,
            playbackLifecycleGeneration: Self.playbackLifecycleGeneration
        )
        _ = queue.activateHost(
            for: Self.episodeId,
            playbackLifecycleGeneration: Self.playbackLifecycleGeneration
        )

        let window = Self.autoWindow(
            id: Self.preRoll.id,
            start: Self.preRoll.start,
            end: Self.preRoll.end
        )
        // `persistDeniedAutoSkip` re-reads the row it is vetoing, so the window
        // has to exist durably as well as in the actor.
        try await store.insertAdWindow(window)
        await orchestrator.receiveAdWindows([window])
        let events = await Self.step(orchestrator, &reader, to: 40, index: 0)
        await Self.forward(
            events, orchestrator, queue, hostGeneration: staleGeneration
        )

        let row = try #require(
            await orchestrator.missedAutoSkipReceipts().first,
            "the refused enqueue left no row to correct"
        )
        await orchestrator.updatePlayheadTime(2000)
        let accepted = await orchestrator.denyAutoSkippedBanner(
            windowId: row.item.windowId,
            analysisAssetId: row.item.analysisAssetId,
            startTime: row.item.adStartTime,
            endTime: row.item.adEndTime,
            podcastId: row.item.podcastId,
            ifCurrentEpisodeId: row.item.episodeId,
            ifPlaybackLifecycleGeneration:
                row.item.playbackLifecycleGeneration,
            ifWindowMaterialRevisionToken:
                row.item.windowMaterialRevisionToken,
            surface: .missedAutoSkipList
        )
        #expect(
            accepted,
            """
            the veto was refused, so the row this bead adds is a button that \
            does nothing — which is worse than the silence it replaced.
            """
        )
        let events2 = try await store.loadCorrectionEvents(
            analysisAssetId: Self.assetId
        )
        // playhead-nq8z: the row a REFUSED ENQUEUE produces is a list row, so
        // its durable source is `missedAutoSkipListDenied`. Same seam, same
        // transaction; the surface argument is the whole difference, and it is
        // what tells a corpus reader that this veto's position being outside
        // the window is the design rather than a card answered late.
        #expect(
            events2.contains { $0.source == .missedAutoSkipListDenied },
            """
            no missedAutoSkipListDenied row. \
            Got \(events2.map { String(describing: $0.source) }).
            """
        )
        #expect(
            !events2.contains { $0.source == .bannerAutoSkipDenied },
            """
            a row the queue never delivered as a card wrote the CARD's source. \
            The listener answered from the passive list, minutes later and \
            from elsewhere in the episode, and the row has to say so.
            """
        )
        #expect(
            !events2.contains { $0.source == .bannerAutoSkipConfirmed },
            """
            a confirmation exists for a skip whose card the queue threw away. \
            A listener who never saw a card never heard the ad, and \
            `bannerAutoSkipConfirmed` is the strongest positive signal the \
            trust system takes.
            """
        )
    }

    // MARK: - 3. The partition, over a walk, with a reattach in the middle

    /// THE PARTITION AGAINST THE QUEUE, at every observation of a walk in which
    /// the host re-attaches half way through.
    ///
    /// The focused tests above each hold the queue's verdict fixed for one
    /// window. Production does not: the listener plays, a skip is carded, the
    /// episode context changes, `NowPlayingView` re-activates the queue, and the
    /// in-flight observation task keeps delivering under the old generation. So
    /// the same walk must produce cards on one side of that moment and rows on
    /// the other, and the partition must hold at every step of it.
    @MainActor
    @Test("THE PARTITION AGAINST THE QUEUE: cards before the reattach, rows after")
    func thePartitionHoldsAcrossAReattach() async throws {
        let (orchestrator, _) = try await Self.makeOrchestrator()
        var reader = BannerEventFrameReader(
            await orchestrator.bannerEventStream()
        )
        let queue = Self.makeQueue()
        let hostGeneration = queue.activateHost(
            for: Self.episodeId,
            playbackLifecycleGeneration: Self.playbackLifecycleGeneration
        )

        let spans = [
            Self.preRoll, Self.midRollOne, Self.midRollTwo, Self.postRoll,
        ]
        await orchestrator.receiveAdWindows(
            spans.map {
                Self.autoWindow(id: $0.id, start: $0.start, end: $0.end)
            }
        )

        // Inside each span in episode order. The reattach happens after the
        // second, so the first two are cards and the last two are rows.
        let schedule: [(time: Double, id: String)] = [
            (40, Self.preRoll.id),
            (1450, Self.midRollOne.id),
            (3400, Self.midRollTwo.id),
            (4300, Self.postRoll.id),
        ]
        var enteredSoFar: Set<String> = []
        var expectedCards: Set<String> = []
        var expectedRows: Set<String> = []

        for (index, stop) in schedule.enumerated() {
            // playhead-pzojm: the fixture's dwell is 86,400 s, so without this
            // the pre-roll's card is STILL on screen at 1450 s and mid-roll
            // one queues behind it, unseen. Under the old enqueue-boundary
            // that was invisible — acceptance booked it as shown regardless.
            // Under the display boundary it is the ad-pod defect itself, and
            // this walk is not about that (the pod test is). In production
            // the 8 s dwell ends long before the next mid-roll, so the honest
            // fixture lets the dwell end too: a neutral dismissal, the same
            // thing the auto-fade timer performs, before each later stop.
            if index >= 1, queue.currentBanner != nil {
                queue.dismiss()
            }
            if index == 2 {
                // The reattach. `observeBanners` is restarted with a new
                // generation; the task still running carries the old one.
                _ = queue.activateHost(
                    for: Self.episodeId,
                    playbackLifecycleGeneration:
                        Self.playbackLifecycleGeneration
                )
            }
            let events = await Self.step(
                orchestrator, &reader, to: stop.time, index: index
            )
            await Self.forward(
                events, orchestrator, queue, hostGeneration: hostGeneration
            )
            enteredSoFar.insert(stop.id)
            if index < 2 {
                expectedCards.insert(stop.id)
            } else {
                expectedRows.insert(stop.id)
            }

            let cards = await orchestrator.deliveredAutoSkipCardWindowIDs()
            let rows = await orchestrator.missedAutoSkipReceipts()
            let list = Set(rows.map(\.windowId))

            #expect(
                rows.count == list.count,
                """
                step \(index): \(rows.count) rows for \(list.count) distinct \
                windows — \(rows.map(\.windowId).sorted()). One skip is one \
                row, and it is keyed by window so an acknowledgement seam \
                cannot make it two.
                """
            )
            #expect(
                cards.union(list) == enteredSoFar,
                """
                step \(index) (playhead \(stop.time) s):
                  entered but neither carded nor listed: \
                \(enteredSoFar.subtracting(cards.union(list)).sorted()) — the \
                queue threw the card away and nothing recorded the skip.
                  carded or listed but NOT entered: \
                \(cards.union(list).subtracting(enteredSoFar).sorted()).
                """
            )
            #expect(
                cards.intersection(list).isEmpty,
                """
                step \(index): \(cards.intersection(list).sorted()) produced \
                BOTH a card and a list row.
                """
            )
            #expect(
                cards == expectedCards,
                """
                step \(index): cards are \(cards.sorted()) and the queue \
                accepted \(expectedCards.sorted()). The reattach at step 2 \
                invalidates the observation task's generation, so nothing \
                after it may be booked as shown.
                """
            )
            #expect(
                list == expectedRows,
                "step \(index): rows are \(list.sorted()), expected \(expectedRows.sorted())"
            )
        }
    }

    // MARK: - 3b. The retire arm, and the limit the accept boundary carries

    /// `BannerHostDelivery.forward` has TWO arms and only the present arm had
    /// a test. Mutation AK11 deletes retire handling as a side effect and was
    /// expected to redden only the canaries — nothing behavioural noticed that
    /// orchestrator retirements had stopped reaching the queue.
    ///
    /// THE RETIREMENT IS CONSTRUCTED HERE RATHER THAN DRIVEN OUT OF THE
    /// ORCHESTRATOR, and the first version of this test got that wrong. It
    /// vetoed the window and expected a retirement, on the assumption that a
    /// veto emits one; `denyAutoSkippedBanner` does not — the emitters are the
    /// producer-side invalidation paths (`retireAllNonRevertedWindowStateIfPresent`
    /// and `retireManagedWindowIfPresent`). The assumption cost a run and is
    /// recorded because it is the same shape as the bead itself: a value
    /// believed to name one event naming another.
    ///
    /// The arm's job is to hand a retirement to the queue under the host
    /// generation it was started with, and that is what is asserted. WHICH
    /// orchestrator path emits one is a different question with its own rails.
    @MainActor
    @Test("A retirement forwarded to the queue pulls the card the orchestrator invalidated")
    func aForwardedRetirementPullsTheCard() async throws {
        let (orchestrator, _) = try await Self.makeOrchestrator()
        var reader = BannerEventFrameReader(
            await orchestrator.bannerEventStream()
        )
        let queue = Self.makeQueue()
        let hostGeneration = queue.activateHost(
            for: Self.episodeId,
            playbackLifecycleGeneration: Self.playbackLifecycleGeneration
        )
        await orchestrator.receiveAdWindows([
            Self.autoWindow(
                id: Self.preRoll.id,
                start: Self.preRoll.start,
                end: Self.preRoll.end
            )
        ])
        await Self.forward(
            await Self.step(orchestrator, &reader, to: 40, index: 0),
            orchestrator, queue, hostGeneration: hostGeneration
        )
        // NON-VACUITY: there is a card to retire.
        #expect(
            queue.currentBanner?.windowId == Self.preRoll.id,
            "no card was presented, so a retirement below would retire nothing"
        )

        await BannerHostDelivery.forward(
            .retireWindow(
                AdBannerRetirement(
                    windowId: Self.preRoll.id,
                    episodeId: Self.episodeId,
                    playbackLifecycleGeneration:
                        Self.playbackLifecycleGeneration
                )
            ),
            from: orchestrator,
            into: queue,
            hostGeneration: hostGeneration
        )

        #expect(
            queue.currentBanner == nil,
            """
            a retirement for \(Self.preRoll.id) reached the forwarding rule and \
            the queue is still showing \
            \(String(describing: queue.currentBanner?.windowId)). A card whose \
            window the producer has invalidated can still collect an answer.
            """
        )
    }

    /// STATED LIMIT: "the queue ACCEPTED it" is not "the listener saw it".
    ///
    /// An ad POD is two adjacent auto-skip windows. The transport skips the
    /// first, so the playhead lands at its end — which is the second's start —
    /// and entry to the second follows within one observer tick. `canCoalesce`
    /// refuses to merge them (`if a.windowId != b.windowId { return false }`),
    /// so card A presents and card B is APPENDED behind it. If the listener
    /// leaves Now Playing inside A's 8 s dwell, `discardAllOnHostDisappear` ->
    /// `discardAllNeutrally` does `queue.removeAll()` and B is destroyed unseen
    /// — already acknowledged, so it leaves no row.
    ///
    /// This bead's acceptance criterion is acceptance BY THE QUEUE and that is
    /// what shipped. The stronger boundary the repo already owns is
    /// `AdBannerQueue.recordBannerShown(for:)`, whose own comment says
    /// "Queue-current is not the same as user-visible".
    ///
    /// **This test asserts what the code does TODAY, and the follow-up is
    /// playhead-pzojm.**
    /// When the acknowledgement moves to the display edge it will FAIL, and it
    /// is meant to — a limit that drifts silently is how a known gap becomes an
    /// unknown one.
    @MainActor
    @Test("An ad pod's second card, discarded unseen, keeps its row — the receipt is retired at the display boundary")
    func aQueuedCardDiscardedUnseenIsStillBookedDelivered() async throws {
        let (orchestrator, _) = try await Self.makeOrchestrator()
        var reader = BannerEventFrameReader(
            await orchestrator.bannerEventStream()
        )
        let queue = Self.makeQueue()
        let hostGeneration = queue.activateHost(
            for: Self.episodeId,
            playbackLifecycleGeneration: Self.playbackLifecycleGeneration
        )
        // Adjacent, as a pod is: the first ends where the second begins.
        let first = Self.autoWindow(id: "pod-a", start: 0.0, end: 86.831)
        let second = Self.autoWindow(id: "pod-b", start: 86.831, end: 140.0)
        await orchestrator.receiveAdWindows([first, second])

        await Self.forward(
            await Self.step(orchestrator, &reader, to: 40, index: 0),
            orchestrator, queue, hostGeneration: hostGeneration
        )
        await Self.forward(
            await Self.step(orchestrator, &reader, to: 100, index: 1),
            orchestrator, queue, hostGeneration: hostGeneration
        )
        // The first is presented; the second is behind it, unseen.
        #expect(
            queue.currentBanner?.windowId == "pod-a",
            "the fixture did not present the first card, so nothing is queued behind it"
        )

        queue.discardAllOnHostDisappear()

        let cards = await orchestrator.deliveredAutoSkipCardWindowIDs()
        let list = Set(await orchestrator.missedAutoSkipReceipts().map(\.windowId))
        #expect(queue.currentBanner == nil)
        #expect(
            cards == ["pod-a"],
            """
            only the card the listener SAW is booked delivered; got \
            \(cards.sorted()). If "pod-b" is here, the acknowledgement has \
            moved back to `enqueue` — playhead-8cjo one layer down, and the \
            defect playhead-pzojm was filed for.
            """
        )
        #expect(
            list == ["pod-b"],
            """
            the second card of the pod was queued behind the first, never \
            shown, and thrown away when the host left — so its receipt must \
            still be a row the listener can correct. Got \(list.sorted()).
            """
        )
        #expect(
            await orchestrator._missedAutoSkipReceiptCountForTesting() == 1,
            "the raw dictionary must hold exactly the unseen card's receipt"
        )
    }

    // MARK: - 4. The suggest tier keeps its own contract

    /// THE POSITIVE DIRECTION, and it existed nowhere until mutation AK15
    /// surfaced its absence.
    ///
    /// `aRefusedSuggestItemIsNotAcknowledged` below asserts only that a REFUSED
    /// suggestion is not acknowledged — and DELETING the suggest acknowledgement
    /// altogether satisfies that perfectly. AK15 does exactly that and the test
    /// stayed green, so nothing behavioural asserted that an ACCEPTED suggestion
    /// IS acknowledged. That is an absence claim satisfied by a total absence,
    /// which is this bead's own defect class wearing the other tier's clothes.
    ///
    /// The consequence in production, had it regressed: an acknowledged
    /// suggestion is what stops `replayPendingSuggestBanners` handing the same
    /// span to the next host, so without the acknowledgement the listener is
    /// asked about the same suggestion again on every attach.
    @MainActor
    @Test("An ACCEPTED suggest item IS acknowledged, so the next host is not asked again")
    func anAcceptedSuggestItemIsAcknowledged() async throws {
        let (orchestrator, _) = try await Self.makeOrchestrator()
        var reader = BannerEventFrameReader(
            await orchestrator.bannerEventStream()
        )
        let queue = Self.makeQueue()
        let hostGeneration = queue.activateHost(
            for: Self.episodeId,
            playbackLifecycleGeneration: Self.playbackLifecycleGeneration
        )

        let suggestion = Self.sentinelWindow(id: "suggest-accepted")
        await orchestrator.receiveAdWindows([suggestion])
        await orchestrator.updatePlayheadTime(Self.sentinelStart + 1)
        let sentinelId = "8cjo-sentinel-suggest-accept"
        await orchestrator.receiveAdWindows([
            Self.sentinelWindow(id: sentinelId)
        ])
        await orchestrator.updatePlayheadTime(Self.sentinelStart + 2)
        let events = await reader.drain(until: sentinelId)
        // NON-VACUITY: the subject really was presented.
        #expect(
            events.contains {
                if case let .present(item) = $0 {
                    return item.windowId == "suggest-accepted"
                }
                return false
            },
            "the suggest subject never presented, so nothing was there to accept"
        )

        await Self.forward(
            events, orchestrator, queue, hostGeneration: hostGeneration
        )
        let acknowledged = await orchestrator.acknowledgedSuggestWindowIDs()
        #expect(
            acknowledged.contains("suggest-accepted"),
            """
            the queue ACCEPTED the suggestion and the orchestrator was never \
            told. `replayPendingSuggestBanners` will hand the same span to the \
            next host, so the listener is asked about it again on every attach \
            — and nothing else in the tree notices, because the sibling test \
            only pins the refusal direction.
            """
        )
    }

    /// `BannerHostDelivery` now owns BOTH acknowledgements, so the tier this
    /// bead did not change is pinned here: a suggest item the queue refuses
    /// must NOT be acknowledged, because an acknowledged suggestion stops being
    /// replayed to the next host (`replayPendingSuggestBanners`) and the
    /// listener is asked nothing about a span they are about to hear.
    @MainActor
    @Test("A refused SUGGEST item is not acknowledged, so it survives for the next host")
    func aRefusedSuggestItemIsNotAcknowledged() async throws {
        let (orchestrator, _) = try await Self.makeOrchestrator()
        var reader = BannerEventFrameReader(
            await orchestrator.bannerEventStream()
        )
        let queue = Self.makeQueue()
        let staleGeneration = queue.activateHost(
            for: Self.episodeId,
            playbackLifecycleGeneration: Self.playbackLifecycleGeneration
        )
        _ = queue.activateHost(
            for: Self.episodeId,
            playbackLifecycleGeneration: Self.playbackLifecycleGeneration
        )

        // The sentinel window IS a suggest window, so the frame boundary and
        // the subject are the same shape; use a second markOnly span as the
        // subject and the sentinel to close the frame around it.
        let suggestion = Self.sentinelWindow(id: "suggest-subject")
        await orchestrator.receiveAdWindows([suggestion])
        await orchestrator.updatePlayheadTime(Self.sentinelStart + 1)
        let sentinelId = "8cjo-sentinel-suggest"
        await orchestrator.receiveAdWindows([
            Self.sentinelWindow(id: sentinelId)
        ])
        await orchestrator.updatePlayheadTime(Self.sentinelStart + 2)
        let events = await reader.drain(until: sentinelId)
        #expect(
            events.contains {
                if case let .present(item) = $0 {
                    return item.windowId == "suggest-subject"
                }
                return false
            },
            "the suggest subject never presented, so nothing was refused"
        )

        await Self.forward(
            events, orchestrator, queue, hostGeneration: staleGeneration
        )
        let acknowledged = await orchestrator.acknowledgedSuggestWindowIDs()
        #expect(
            !acknowledged.contains("suggest-subject"),
            """
            a suggestion the queue REFUSED was marked as delivered. It will \
            never be replayed to the host that attaches next, so the listener \
            is asked nothing about a span they are about to hear — \
            playhead-8cjo's defect on the tier that already had the seam.
            """
        )
    }
}

// MARK: - The three refusals

/// The three guards `AdBannerQueue` refuses an item on. Named rather than
/// booleaned because each is a different production moment, and a fix that
/// handled one of them is not a fix.
enum QueueRefusal: Sendable, CustomStringConvertible {
    case staleHostGeneration
    case hostDisappeared
    case episodeMismatch

    var description: String {
        switch self {
        case .staleHostGeneration:
            return "stale host generation (the reattach window)"
        case .hostDisappeared:
            return "the host disappeared"
        case .episodeMismatch:
            return "the item's episode is not the host's"
        }
    }
}
