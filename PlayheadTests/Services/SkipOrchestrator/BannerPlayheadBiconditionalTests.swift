// BannerPlayheadBiconditionalTests.swift
// playhead-bwxi: THE BANNER TRACKS THE PLAYHEAD, IN BOTH DIRECTIONS.
//
//     A banner is presented for window W at time T  IF AND ONLY IF  W contains T.
//
// Dan, 2026-08-21: "This is a second time we've had this regression, we
// definitely need a regression test for it." It is the second time the property
// broke and the THIRD time it was found in the field, and the three failures do
// not all point the same way:
//
//   playhead-d3g0 (2026-07-31, SUGGEST tier).  Three spans at 3.5, 44.5 and 80
//     minutes delivered as one detection batch, all three confirmed inside
//     20.1 s, two of them false positives, 210 s of show skipped. FLOOD.
//   playhead-isp5 (2026-08-01, SUGGEST tier).  The playhead was INSIDE a
//     markOnly day-0 pre-roll (0.0-45.1 s, confidence 1.00) and no banner
//     appeared at all. He heard the ad. SILENCE.
//   playhead-bwxi (2026-08-21, AUTO-SKIP tier).  Four cards at ~87 s for
//     windows at 0, 1370, 3367 and 4279 s. Three `bannerAutoSkipConfirmed`
//     rows — the strongest positive signal the trust system takes — recorded
//     for audio he had not heard. He stopped using the banner inside one
//     episode. FLOOD, on the tier d3g0 did not touch.
//
// WHY THIS IS ONE PROPERTY AND NOT TWO TESTS. Each arrow on its own passes
// through one of the two field failures, so a suite that states them separately
// invites a later reader to keep the arrow that is failing and quietly drop the
// other:
//
//   * "playhead inside W  =>  banner for W" passes on 2026-08-21. bwxi's FIRST
//     card, [0.0-86.8], genuinely did contain the playhead; the defect is the
//     other three. Catches isp5, misses bwxi.
//   * "banner for W  =>  playhead inside W" passes on 2026-08-01. isp5's
//     failure was silence — there was no banner to be wrong about. Catches
//     bwxi, misses isp5.
//
// So the assertion below is a SET EQUALITY, evaluated at every observation of a
// walk: the set of windows that have bannered is the set of windows the
// playhead has entered. `⊋` is a flood, `⊊` is a silence, and one `#expect`
// cannot be half-satisfied.
//
// WHAT IT DRIVES, because both regressions are about behaviour OVER TIME. A
// single position assertion can see neither: isp5 is "an entry happened and
// nothing fired", bwxi is "one entry fired four things". The walk uses the
// 2026-08-21 episode's own shape — four windows on a 4309.4 s asset — and
// asserts at EVERY step, including the steps between windows where the correct
// answer is "nothing new".
//
// AND IT ASSERTS THE COUNT, not merely the membership. bwxi is not "a wrong
// banner"; it is FOUR banners where ONE entry occurred. Membership alone is
// satisfied by a card that fires four times.
//
// BOTH TIERS, in the same property. The two paths are genuinely different code
// — `registerSuggestedWindow` / `emitSuggestBannersOnPlayheadEntry` for the
// markOnly suggest card, `evaluateAndPush` / `emitAutoSkipBannersOnPlayheadEntry`
// for the auto-skip receipt — and pinning one while the other regressed is
// exactly what happened between d3g0 and this bead.
//
// OBSERVATION METHOD — borrowed from `SuggestBannerEntryGateTests`, and for its
// reason. Emission is SYNCHRONOUS inside the actor and `AsyncStream` buffers on
// `yield`, so by the time an awaited orchestrator call returns, everything it
// emitted is already in the stream buffer. A reader that owns the iterator
// pulls it without suspending, and a SENTINEL driven after the step under test
// is an exact frame boundary — so "nothing arrived" is a positive observation
// rather than a timeout. No observer task, no polling, no deadline.

import CoreMedia
import Foundation
import Testing

@testable import Playhead

// MARK: - Tier under test

/// The two presentation paths. Named rather than booleaned because the whole
/// point is that they are different code reached through different doors.
enum BwxiBannerTier: Sendable, CustomStringConvertible {
    /// `eligibilityGate = eligible`, byte-exact edges, auto mode: the managed
    /// tier. `evaluateAndPush` promotes to `.applied` and the card is the
    /// "Skipped …" receipt. This is bwxi's path.
    case autoSkip
    /// `eligibilityGate = markOnly`: the suggest tier. The card asks a
    /// question. This is d3g0's and isp5's path.
    case suggest

    var description: String {
        switch self {
        case .autoSkip: return "auto-skip (playhead-bwxi)"
        case .suggest: return "suggest (playhead-d3g0/isp5)"
        }
    }

    var expectedItemTier: AdBannerTier {
        switch self {
        case .autoSkip: return .autoSkipped
        case .suggest: return .suggest
        }
    }
}

// MARK: - Reader

/// Single-consumer reader over the banner stream. Owns the iterator, so a pull
/// returns already-buffered items without depending on any other task being
/// scheduled.
private struct BiconditionalBannerReader {
    private var iterator: AsyncStream<AdSkipBannerItem>.AsyncIterator

    init(_ stream: AsyncStream<AdSkipBannerItem>) {
        iterator = stream.makeAsyncIterator()
    }

    mutating func drain(until sentinel: String) async -> [AdSkipBannerItem] {
        var collected: [AdSkipBannerItem] = []
        while let item = await iterator.next() {
            if item.windowId == sentinel {
                return collected
            }
            collected.append(item)
        }
        return collected
    }
}

// MARK: - Suite

@Suite(
    "playhead-bwxi — a banner is presented for W at T iff W contains T",
    .timeLimit(.minutes(1))
)
struct BannerPlayheadBiconditionalTests {

    // MARK: Fixture — the 2026-08-21 episode, verbatim

    private static let assetId = "asset-1"
    private static let episodeId = "asset-1"
    /// MUST be the show `makeSkipTestTrustService` seeds. A show with no
    /// profile resolves to `.shadow` for show-governed classes; the auto-tier
    /// half of this suite would then never reach `.applied` and would pass
    /// vacuously.
    private static let podcastId = "podcast-1"

    /// Asset 0FF7EFF3, "Gillian Anderson", 4309.4 s. Every span is a
    /// `dayZeroRediffByteExact` window that the device recorded as
    /// `wasSkipped=1, gate=eligible, decisionState=applied` — the skipping was
    /// perfect and is not what this bead is about.
    private static let fieldWindows:
        [(id: String, start: Double, end: Double, label: String)] = [
            (id: "bwxi-preroll", start: 0.0, end: 86.831, label: "pre-roll"),
            (id: "bwxi-midroll-1", start: 1369.809, end: 1548.487, label: "mid-roll 1 (23 min)"),
            (id: "bwxi-midroll-2", start: 3367.262, end: 3534.576, label: "mid-roll 2 (56 min)"),
            (id: "bwxi-postroll", start: 4279.302, end: 4309.420, label: "post-roll (71 min)"),
        ]

    private static let episodeDuration: Double = 4309.42

    /// Parked in program audio between the pre-roll and the first mid-roll:
    /// inside NO window under test, so the frame boundary can never itself be
    /// an entry — and inside the EPISODE, which the first version of this file
    /// got wrong. A sentinel at 5000 s on a 4309.42 s asset is retired by
    /// `InventorySanityFilter`'s tail-edge rule (playhead-b6r2) before it ever
    /// reaches a tier, so it never emitted, `drain` never returned, and all six
    /// tests died on their time limit. A frame boundary that can be filtered
    /// out is not a frame boundary.
    private static let sentinelStart: Double = 1000

    // MARK: Fixture construction

    private static func fieldWindow(
        id: String,
        start: Double,
        end: Double,
        tier: BwxiBannerTier
    ) -> AdWindow {
        // The auto tier needs a class that reaches `.auto`. Both edges
        // `.rediffByteExact` is what the field rows carry and what
        // `SkipDetectorClass.showIndependentSeedMode` grants auto mode to
        // without depending on the show profile. The suggest tier needs the
        // opposite — a class the managed tier will not take — which is what
        // `eligibilityGate = markOnly` decides at the door.
        let anchor: AutoSkipEdgeAnchor = tier == .autoSkip
            ? .rediffByteExact
            : .unanchored
        return AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            // The field rows are confidence 1.0.
            confidence: 1.0,
            boundaryState: tier == .autoSkip
                ? "lexical"
                : AdBoundaryState.segmentAggregated.rawValue,
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
            eligibilityGate: tier == .autoSkip
                ? SkipEligibilityGate.eligible.rawValue
                : SkipEligibilityGate.markOnly.rawValue,
            startEdgeAnchor: anchor.rawValue,
            endEdgeAnchor: anchor.rawValue
        )
    }

    /// A `markOnly` window used only as a frame boundary. `markOnly` because
    /// the suggest path never consults the skip mode, so the same sentinel is
    /// valid in both halves of the suite.
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

    fileprivate static func makeHarness() async throws -> SkipOrchestrator {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: assetId,
                episodeId: episodeId,
                // Reproduce production: `AnalysisCoordinator` pushes a duration
                // before every `receiveAdWindows`, which is the only thing that
                // arms the inventory filter's tail-edge rule. The post-roll of
                // this episode has to survive it, and playhead-b6r2 is the
                // record of what happens when a real span does not.
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
            podcastId: podcastId
        )
        return orchestrator
    }

    // MARK: The walk

    /// One observation of the walk: where the playhead was put, and every
    /// banner THAT observation produced.
    private struct Step {
        let time: Double
        let emitted: [AdSkipBannerItem]
    }

    /// Advance the playhead to `time` and collect exactly the banners that
    /// observation produced.
    ///
    /// The sentinel is delivered while the playhead is still at `time` (so the
    /// `evaluateAndPush` its delivery triggers sees the walk's position, never
    /// the sentinel's), and only then driven, so everything ahead of it in the
    /// buffer belongs to the step under test. The next step re-sets the
    /// position from scratch, so parking the playhead at the sentinel costs
    /// nothing.
    private static func step(
        _ orchestrator: SkipOrchestrator,
        _ reader: inout BiconditionalBannerReader,
        to time: Double,
        index: Int
    ) async -> Step {
        await orchestrator.updatePlayheadTime(time)
        let sentinelId = "bwxi-sentinel-\(index)"
        await orchestrator.receiveAdWindows([sentinelWindow(id: sentinelId)])
        await orchestrator.updatePlayheadTime(sentinelStart)
        let emitted = await reader.drain(until: sentinelId)
        return Step(time: time, emitted: emitted)
    }

    /// The observation schedule. Entry points are the exact span starts; the
    /// steps in between are positions inside NO window, where the correct
    /// answer is "nothing new" and a flood shows up as a surplus.
    ///
    /// Deliberately includes an observation one tick BEFORE the first mid-roll
    /// (`1369.559`) and one AT the post-roll's end (`4309.420`, excluded by the
    /// half-open interval), so the two boundary directions are both walked.
    private static var schedule: [Double] {
        let tick = PlaybackService.periodicTimeObserverIntervalSeconds
        return [
            0.0,                    // inside the pre-roll, from the first frame
            40.0,                   // still inside it
            86.831,                 // its END — half-open, so outside
            600.0,                  // program audio
            1369.809 - tick,        // one observer tick short of mid-roll 1
            1369.809,               // entry
            1450.0,                 // inside
            1548.487,               // end — outside
            2400.0,                 // program audio
            3367.262,               // mid-roll 2 entry
            3400.0,                 // inside
            3534.576,               // end — outside
            4000.0,                 // program audio
            4279.302,               // post-roll entry
            4300.0,                 // inside
            4309.420,               // end — outside
        ]
    }

    private static func windowsContaining(_ time: Double) -> Set<String> {
        Set(
            fieldWindows
                .filter { time >= $0.start && time < $0.end }
                .map(\.id)
        )
    }

    // MARK: - 1. THE PROPERTY

    /// THE BICONDITIONAL, asserted at every observation of a real walk, on both
    /// presentation paths.
    ///
    /// `banneredSoFar == enteredSoFar` is one claim with two arrows. Reading it
    /// as two facts is the mistake this suite exists to prevent:
    ///   * `banneredSoFar ⊋ enteredSoFar` — a card for audio the listener has
    ///     not reached. bwxi (four cards at 87 s), d3g0 (three at 20 s).
    ///   * `banneredSoFar ⊊ enteredSoFar` — the playhead is in an ad and
    ///     nothing was offered. isp5.
    @Test(
        "THE PROPERTY: at every observation, the bannered set IS the entered set",
        arguments: [BwxiBannerTier.autoSkip, BwxiBannerTier.suggest]
    )
    func banneredSetEqualsEnteredSetAtEveryObservation(tier: BwxiBannerTier) async throws {
        let orchestrator = try await Self.makeHarness()
        var reader = BiconditionalBannerReader(await orchestrator.bannerItemStream())
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }

        // The delivery that caused the field incident: the whole episode's
        // windows in ONE batch, before a single position observation.
        await orchestrator.receiveAdWindows(
            Self.fieldWindows.map {
                Self.fieldWindow(id: $0.id, start: $0.start, end: $0.end, tier: tier)
            }
        )

        // (a) THE DELIVERY ITSELF PRESENTS NOTHING. Without this, every
        // assertion below could be satisfied by cards that were already sitting
        // in the buffer from detection time and merely got counted late.
        let atDelivery = await Self.step(
            orchestrator, &reader, to: 0.0 - 1, index: -1
        )
        #expect(
            atDelivery.emitted.isEmpty,
            """
            \(tier): detection delivery presented \(atDelivery.emitted.count) \
            banner(s) — \(atDelivery.emitted.map(\.windowId)) — for a listener \
            who has not reached any of them. This is the 2026-08-21 field \
            incident: four cards inside six seconds for windows at 0, 23, 56 \
            and 71 minutes, three of them confirmed as ads he had never heard.
            """
        )

        // (b) THE DECISION STILL HAPPENED. A fix that simply stopped deciding
        // would satisfy every assertion in this file, so prove the other half:
        // the auto tier armed all four and pushed their cues; the suggest tier
        // registered all four.
        let allWindowIds = Set(Self.fieldWindows.map(\.id))
        switch tier {
        case .autoSkip:
            let armed = await orchestrator.armedAutoSkipBannerWindowIDs()
            #expect(
                armed == allWindowIds,
                """
                the auto tier must DECIDE at delivery and merely defer the \
                card; got armed = \(armed.sorted())
                """
            )
            #expect(
                pushedCues.count == Self.fieldWindows.count,
                """
                Dan's verdict on the SKIPPING was "perfect" and this bead must \
                not touch it: all \(Self.fieldWindows.count) cues must be \
                pushed at delivery; got \(pushedCues.count)
                """
            )
        case .suggest:
            // Sentinels are suggest windows too, and a suggestion is NOT
            // removed from `suggestWindows` when it fires — only on veto,
            // accept or retirement — so the frame boundaries are still in
            // there and have to be discounted.
            let registered = await orchestrator.activeSuggestWindowIDs()
                .filter { !$0.hasPrefix("bwxi-sentinel") }
            #expect(
                registered == allWindowIds,
                """
                all four must be REGISTERED — the orchestrator still needs to \
                know about them; got \(registered.sorted())
                """
            )
        }

        // (c) THE WALK.
        var banneredSoFar: Set<String> = []
        var enteredSoFar: Set<String> = []
        var emissionCounts: [String: Int] = [:]

        for (index, time) in Self.schedule.enumerated() {
            let observed = await Self.step(
                orchestrator, &reader, to: time, index: index
            )
            // Computed BEFORE `enteredSoFar` absorbs this observation: the
            // windows this one observation is the FIRST to be inside.
            let newlyEntered = Self.windowsContaining(time)
                .subtracting(enteredSoFar)
            for item in observed.emitted {
                emissionCounts[item.windowId, default: 0] += 1
                banneredSoFar.insert(item.windowId)
            }
            enteredSoFar.formUnion(Self.windowsContaining(time))

            // THE BICONDITIONAL.
            #expect(
                banneredSoFar == enteredSoFar,
                """
                \(tier) — playhead \(time) s (step \(index)):
                  bannered but NOT entered: \(banneredSoFar.subtracting(enteredSoFar).sorted()) \
                (a card for audio the listener has not reached — playhead-bwxi / playhead-d3g0)
                  entered but NOT bannered: \(enteredSoFar.subtracting(banneredSoFar).sorted()) \
                (the playhead is in an ad and nothing was offered — playhead-isp5)
                """
            )

            // THE COUNT AT THIS MOMENT. bwxi is not "a wrong banner", it is
            // FOUR banners where ONE entry occurred, so the per-step delta is
            // asserted EXACTLY: this observation presents the windows it is the
            // first to be inside, and nothing else.
            #expect(
                Set(observed.emitted.map(\.windowId)) == newlyEntered
                    && observed.emitted.count == newlyEntered.count,
                """
                \(tier) — playhead \(time) s (step \(index)) presented \
                \(observed.emitted.map(\.windowId)) but the windows entered for \
                the FIRST time by this observation are \(newlyEntered.sorted())
                """
            )
            #expect(
                observed.emitted.allSatisfy { $0.tier == tier.expectedItemTier },
                """
                \(tier) — a card arrived on the wrong tier: \
                \(observed.emitted.map { "\($0.windowId)=\($0.tier)" })
                """
            )
        }

        // (d) EXACTLY ONE CARD PER WINDOW over the whole walk. Set equality
        // above is satisfied by a window that banners on every tick it is
        // inside; the listener would see the same flood.
        #expect(
            emissionCounts == Dictionary(
                uniqueKeysWithValues: Self.fieldWindows.map { ($0.id, 1) }
            ),
            """
            \(tier): every window must present EXACTLY ONE card across the whole \
            episode; got \(emissionCounts.sorted { $0.key < $1.key })
            """
        )
    }

    // MARK: - 2. The field moment, stated as itself

    /// The single observation the bead is about, isolated so a failure names it
    /// rather than a step index: the listener is 87 seconds into a 72-minute
    /// episode, and exactly one window contains him.
    @Test(
        "The field moment: at 87 s only the pre-roll may have bannered — not four",
        arguments: [BwxiBannerTier.autoSkip, BwxiBannerTier.suggest]
    )
    func atEightySevenSecondsOnlyThePreRollHasBannered(tier: BwxiBannerTier) async throws {
        let orchestrator = try await Self.makeHarness()
        var reader = BiconditionalBannerReader(await orchestrator.bannerItemStream())

        await orchestrator.receiveAdWindows(
            Self.fieldWindows.map {
                Self.fieldWindow(id: $0.id, start: $0.start, end: $0.end, tier: tier)
            }
        )

        // Playback from the top through the pre-roll, ending where Dan's four
        // taps were recorded — 08:31:36 to :42, ~87 s in.
        var received: [AdSkipBannerItem] = []
        for (index, time) in [0.0, 40.0, 80.0, 86.5].enumerated() {
            let observed = await Self.step(
                orchestrator, &reader, to: time, index: index
            )
            received.append(contentsOf: observed.emitted)
        }

        #expect(
            received.map(\.windowId) == ["bwxi-preroll"],
            """
            \(tier): at 87 s into a 4309 s episode the listener has reached ONE \
            window. He got \(received.count): \(received.map(\.windowId)).

            The field record, verbatim:
                08:31:36  bannerAutoSkipConfirmed  [   0.000 -   86.831]
                08:31:39  bannerAutoSkipConfirmed  [1369.809 - 1548.487]
                08:31:40  bannerAutoSkipConfirmed  [3367.262 - 3534.576]
                08:31:42  bannerAutoSkipConfirmed  [4279.302 - 4309.420]

            The last three are windows at 23, 56 and 71 minutes, confirmed as \
            ads by a listener who had heard none of them. `bannerAutoSkipConfirmed` \
            is the strongest positive signal the trust system takes.
            """
        )
    }

    // MARK: - 3. isp5's direction, stated as itself

    /// The 2026-08-01 case: the playhead is INSIDE a day-0 pre-roll and the
    /// listener is offered nothing. Isolated for the same reason as the test
    /// above — so the silence direction fails with its own name.
    ///
    /// The span is isp5's verbatim: 0.0-45.1 s, confidence 1.00, markOnly. It
    /// runs on BOTH tiers because the gate that swallowed it
    /// (`InventorySanityFilter`'s head-edge rule, playhead-b6r2) sat upstream
    /// of the tier split and would have taken either one.
    @Test(
        "isp5's direction: a playhead inside a day-0 pre-roll is never left silent",
        arguments: [BwxiBannerTier.autoSkip, BwxiBannerTier.suggest]
    )
    func aPlayheadInsideADayZeroPreRollIsNeverSilent(tier: BwxiBannerTier) async throws {
        let orchestrator = try await Self.makeHarness()
        var reader = BiconditionalBannerReader(await orchestrator.bannerItemStream())

        await orchestrator.receiveAdWindows([
            Self.fieldWindow(id: "isp5-preroll", start: 0.0, end: 45.1, tier: tier)
        ])

        let observed = await Self.step(orchestrator, &reader, to: 0.0, index: 0)
        #expect(
            observed.emitted.map(\.windowId) == ["isp5-preroll"],
            """
            \(tier): the playhead is at 0.0 s, inside a 0.0-45.1 s day-0 window \
            at confidence 1.00, and the listener was offered \
            \(observed.emitted.count) card(s). On 2026-08-01 that silence meant \
            he heard the ad — the window had been retired upstream by the \
            inventory filter's head-edge rule (playhead-isp5 / playhead-b6r2), \
            which is a DIFFERENT cause from the presentation gate this bead \
            moved and is why both arrows have to be pinned here.
            """
        )
    }

    // MARK: - 4. The two tiers agree, and the transport agrees with both

    /// The auto tier's receipt fires on the SAME predicate the transport uses
    /// to fire the skip — `>= start, < end`, half-open, on the same
    /// observation. That is what makes "the receipt announces a skip that
    /// happened" true by construction rather than by timing luck: if the
    /// orchestrator never observes containment, the transport never fired a
    /// skip either.
    @Test("Entry is half-open at both ends, and the same on both tiers")
    func entryIsHalfOpenAndIdenticalAcrossTiers() async throws {
        for tier in [BwxiBannerTier.autoSkip, BwxiBannerTier.suggest] {
            let orchestrator = try await Self.makeHarness()
            var reader = BiconditionalBannerReader(await orchestrator.bannerItemStream())
            await orchestrator.receiveAdWindows([
                Self.fieldWindow(id: "edge", start: 100, end: 160, tier: tier)
            ])

            // A hair before the start: outside.
            let before = await Self.step(
                orchestrator, &reader, to: 100 - 0.001, index: 0
            )
            #expect(
                before.emitted.isEmpty,
                "\(tier): 99.999 s is not inside [100, 160); got \(before.emitted.map(\.windowId))"
            )

            // Exactly at the start: inside. Inclusive, because that is the
            // instant the whole ad is still ahead of the listener.
            let atStart = await Self.step(orchestrator, &reader, to: 100, index: 1)
            #expect(
                atStart.emitted.map(\.windowId) == ["edge"],
                "\(tier): entry at the span start must present immediately; got \(atStart.emitted.map(\.windowId))"
            )
        }
    }

    /// The end is EXCLUSIVE, and the way to prove it is a window the playhead
    /// only ever observes at its end — never inside. A closed interval would
    /// present a card for a span that is already behind the listener.
    @Test(
        "A window the playhead only ever reaches the END of never banners",
        arguments: [BwxiBannerTier.autoSkip, BwxiBannerTier.suggest]
    )
    func aWindowObservedOnlyAtItsEndNeverBanners(tier: BwxiBannerTier) async throws {
        let orchestrator = try await Self.makeHarness()
        var reader = BiconditionalBannerReader(await orchestrator.bannerItemStream())
        await orchestrator.receiveAdWindows([
            Self.fieldWindow(id: "end-only", start: 100, end: 160, tier: tier)
        ])

        let atEnd = await Self.step(orchestrator, &reader, to: 160, index: 0)
        #expect(
            atEnd.emitted.isEmpty,
            """
            \(tier): 160 s is the span's END and the interval is half-open — a \
            span the listener has left offers nothing to skip and nothing to \
            confirm; got \(atEnd.emitted.map(\.windowId))
            """
        )

        let past = await Self.step(orchestrator, &reader, to: 400, index: 1)
        #expect(
            past.emitted.isEmpty,
            "\(tier): a span entirely behind the playhead never banners; got \(past.emitted.map(\.windowId))"
        )
    }

    // MARK: - 5. Presentation latency is still correctness

    /// The fix moves presentation from the delivery path to the position path,
    /// which costs at most one position-observer tick. Pin that the cost is one
    /// tick and not a dwell: requiring a SECOND observation inside the span
    /// would put a skip affordance behind up to two ticks of the ad, which is
    /// the thing `suggestEntryLatencyBudgetSeconds` exists to forbid.
    @Test(
        "The FIRST observation inside the span presents — no dwell, no second tick",
        arguments: [BwxiBannerTier.autoSkip, BwxiBannerTier.suggest]
    )
    func theFirstObservationInsideTheSpanPresents(tier: BwxiBannerTier) async throws {
        let orchestrator = try await Self.makeHarness()
        var reader = BiconditionalBannerReader(await orchestrator.bannerItemStream())
        let tick = PlaybackService.periodicTimeObserverIntervalSeconds
        let spanStart = 600.0
        let lastObservationBefore = spanStart - 0.001
        let firstObservationInside = lastObservationBefore + tick

        await orchestrator.receiveAdWindows([
            Self.fieldWindow(id: "budget", start: spanStart, end: 700, tier: tier)
        ])
        let before = await Self.step(
            orchestrator, &reader, to: lastObservationBefore, index: 0
        )
        #expect(before.emitted.isEmpty, "\(tier): 1 ms before the span is not inside it")

        let inside = await Self.step(
            orchestrator, &reader, to: firstObservationInside, index: 1
        )
        #expect(
            inside.emitted.map(\.windowId) == ["budget"],
            """
            \(tier): the FIRST observation inside the span must present. \
            Requiring a second puts the card up to \(2 * tick) s into the ad. \
            Got \(inside.emitted.map(\.windowId)).
            """
        )
        #expect(
            firstObservationInside - spanStart
                <= SkipOrchestrator.suggestEntryLatencyBudgetSeconds,
            """
            worst-case presentation lateness is \(firstObservationInside - spanStart) s, \
            over the \(SkipOrchestrator.suggestEntryLatencyBudgetSeconds) s budget
            """
        )
    }
}
