// SuggestBannerEntryGateTests.swift
// playhead-d3g0: the uncertain/suggest banner fires when the PLAYHEAD ENTERS
// the span, not when detection delivers it.
//
// THE FIELD PROOF (Dan's device, 2026-07-31, episode DE0784D8, 92 min).
// `correction_events`, verbatim:
//
//     14:53:05   bannerSuggestionConfirmed   210- 240 s   (3.5 min in)
//     14:53:23   bannerSuggestionConfirmed  2670-2700 s   (44.5 min in)  +17.9 s
//     14:53:25   bannerSuggestionConfirmed  4800-4950 s   (80 min in)    +20.1 s
//     15:11:35   manualVeto  falsePositive   210- 240 s   +18.5 min
//     15:13:01   manualVeto  falsePositive  4800-4950 s   +20.0 min
//
// Three spans at 3.5, 44.5 and 80 minutes were confirmed inside 20.1 SECONDS.
// It is physically impossible to have heard them. They arrived as one
// DETECTION batch — `registerSuggestedWindow` is called from
// `receiveAdWindows`, and `updatePlayheadTime` emitted nothing at all — so the
// banner had no playhead gate whatsoever. Every answer authorised a skip of
// audio the listener had never reached; ~18 minutes later, when playback
// actually got there, two were show content and were vetoed. 210 s of show.
//
// DAN'S DECISION, 2026-07-31: "when it enters so I can skip."
//
// So the trigger is ENTRY, not "after the listener has heard it". That makes
// the banner a PROSPECTIVE SKIP AFFORDANCE, and three things follow, each
// pinned below:
//
//   • LATENCY IS CORRECTNESS. A banner that appears 3 s into the ad has missed
//     the point — the listener is already hearing what they wanted to skip.
//     The budget is `SkipOrchestrator.suggestEntryLatencyBudgetSeconds` and it
//     is tied to the real transport tick, not to a wish.
//   • PRE-ROLL is not a special case. An ad at 0:00 fires the instant playback
//     starts, because that is when the playhead enters it.
//   • RE-ENTRY ON SEEK does not re-ask. A span fires at most once per revision;
//     scrubbing back into it is not a new question.
//
// Contrast `emitBannerItem(for: ManagedWindow)`, the AUTO-SKIP banner, which
// was always playhead-driven ("after an automatic skip is actually applied").
// The suggest path simply never got the same treatment.
//
// OBSERVATION METHOD — no sleeps, no timeouts, no observer task.
//
// Emission happens SYNCHRONOUSLY inside the orchestrator actor, and
// `AsyncStream` buffers on `yield`. So by the time
// `await orchestrator.updatePlayheadTime(...)` returns, everything it emitted is
// already in the stream's buffer, and a reader that owns the iterator pulls it
// without ever suspending on an empty buffer. A SENTINEL window driven after
// the operation under test is therefore an exact frame boundary: whatever comes
// back before it is the complete set, including the empty set.
//
// The first version of this file used a separate observer actor polling a
// 5 s deadline. It passed standalone and FAILED all fifteen of its own tests in
// the full ~8,300-test gate, where the observer task was starved past the
// deadline and every drain reported "nothing arrived". That is a test harness
// asserting scheduler luck, and the fix was to stop needing a second task at
// all rather than to raise the number. `.timeLimit` is the only bound left, and
// it exists solely so a genuine regression fails instead of hanging.

import Foundation
import Testing

@testable import Playhead

// MARK: - Reader

/// Single-consumer reader over a banner stream. Owns the iterator, so a pull
/// returns already-buffered items without depending on any other task getting
/// scheduled.
private struct BannerReader {
    private var iterator: AsyncStream<AdSkipBannerItem>.AsyncIterator

    init(_ stream: AsyncStream<AdSkipBannerItem>) {
        iterator = stream.makeAsyncIterator()
    }

    /// Every item up to `sentinel`, consuming the sentinel. Empty means the
    /// operation under test emitted nothing — a positive observation, not a
    /// timeout.
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

// A stream read that never resolves would otherwise hang the whole gate. This
// is the ONLY bound in the suite — every assertion below is a synchronous
// consequence of an awaited actor call, so under load these tests get SLOWER
// and never wrong.
@Suite("Suggest banner entry gate (playhead-d3g0)", .timeLimit(.minutes(1)))
struct SuggestBannerEntryGateTests {

    // MARK: - Fixture

    fileprivate static let assetId = "asset-1"
    fileprivate static let episodeId = "asset-1"
    /// MUST be the show `makeSkipTestTrustService` seeds — a show with no
    /// profile resolves to `.shadow`. Nothing here depends on auto mode (a
    /// suggest window never enters the skip-evaluation path), but keeping the
    /// harness identical to `BannerConfirmationExtentGateTests` means a future
    /// cue assertion added here cannot become silently vacuous.
    fileprivate static let podcastId = "podcast-1"

    /// The three field spans, verbatim.
    private static let fieldSpans: [(id: String, start: Double, end: Double)] = [
        (id: "d3g0-field-preroll-ish", start: 210, end: 240),
        (id: "d3g0-field-midroll-1", start: 2670, end: 2700),
        (id: "d3g0-field-midroll-2", start: 4800, end: 4950),
    ]

    /// A `markOnly` window — the only kind that reaches the suggest tier.
    /// Anchors default to the field case's both-edges-`unanchored`.
    fileprivate static func makeSuggestion(
        id: String,
        start: Double,
        end: Double,
        confidence: Double = 0.41,
        startAnchor: AutoSkipEdgeAnchor = .unanchored,
        endAnchor: AutoSkipEdgeAnchor = .unanchored
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
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
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            startEdgeAnchor: startAnchor.rawValue,
            endEdgeAnchor: endAnchor.rawValue
        )
    }

    fileprivate static func makeHarness() async throws
        -> (orchestrator: SkipOrchestrator, store: AnalysisStore) {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: podcastId
        )
        return (orchestrator, store)
    }

    /// Register a sentinel suggestion and drive the playhead into it, so the
    /// stream's ordering proves what did and did not precede it.
    ///
    /// The sentinel is delivered in its OWN `receiveAdWindows` call, always
    /// after the windows under test, so that on the pre-fix (detection-time)
    /// emitter it is still ordered last — otherwise these tests would pass
    /// against the very defect they exist to catch.
    fileprivate static func fireSentinel(
        _ orchestrator: SkipOrchestrator,
        id: String,
        at time: Double
    ) async {
        let sentinel = makeSuggestion(id: id, start: time, end: time + 4)
        await orchestrator.receiveAdWindows([sentinel])
        await orchestrator.updatePlayheadTime(time)
    }

    /// Assert that nothing has bannered yet, using a sentinel driven at a
    /// position that is inside NO window under test.
    ///
    /// Every test that only ever observes the stream AFTER walking into its
    /// span passes against the pre-fix emitter for the wrong reason — the
    /// banner was already sitting in the buffer from detection time. Three of
    /// the tests here did exactly that on the first run. This is the guard.
    fileprivate static func expectNothingBanneredYet(
        _ orchestrator: SkipOrchestrator,
        _ reader: inout BannerReader,
        sentinelId: String,
        drivingPlayheadTo time: Double,
        _ because: String,
        sourceLocation: SourceLocation = #_sourceLocation
    ) async {
        await fireSentinel(orchestrator, id: sentinelId, at: time)
        let received = await reader.drain(until: sentinelId)
        #expect(
            received.isEmpty,
            "\(because); got \(received.map(\.windowId))",
            sourceLocation: sourceLocation
        )
    }

    // MARK: - 1. The field regression

    @Test("Field case: one batch of three spans emits NOTHING until the playhead reaches each")
    func fieldBatchEmitsNothingUntilPlayheadEntersEachSpan() async throws {
        let (orchestrator, _) = try await Self.makeHarness()
        var reader = BannerReader(await orchestrator.bannerItemStream())

        // The delivery that caused the field incident: three spans at 3.5,
        // 44.5 and 80 minutes, in ONE detection batch.
        await orchestrator.receiveAdWindows(
            Self.fieldSpans.map {
                Self.makeSuggestion(id: $0.id, start: $0.start, end: $0.end)
            }
        )
        #expect(
            await orchestrator.activeSuggestWindowIDs().count == 3,
            "all three must be REGISTERED — the orchestrator still needs to know about them"
        )

        // The playhead is at 1 s. Nothing has been reached.
        await Self.fireSentinel(orchestrator, id: "d3g0-sentinel-a", at: 1)
        let beforeEntry = await reader.drain(until: "d3g0-sentinel-a")
        #expect(
            beforeEntry.isEmpty,
            """
            Detection delivery emitted \(beforeEntry.count) banner(s) for audio the \
            listener has not reached: \(beforeEntry.map(\.windowId)). This is the \
            2026-07-31 field incident — three spans confirmed inside 20.1 s, two of \
            them false positives, 210 s of show skipped.
            """
        )

        // Now walk the playhead into each span, in order.
        await orchestrator.updatePlayheadTime(215)
        await orchestrator.updatePlayheadTime(2680)
        await orchestrator.updatePlayheadTime(4850)
        await Self.fireSentinel(orchestrator, id: "d3g0-sentinel-b", at: 4990)

        let afterEntry = await reader.drain(until: "d3g0-sentinel-b")
        #expect(
            afterEntry.map(\.windowId) == Self.fieldSpans.map(\.id),
            "each span must banner exactly once, in playhead order; got \(afterEntry.map(\.windowId))"
        )
        #expect(afterEntry.allSatisfy { $0.tier == .suggest })
    }

    // MARK: - 2. The trigger point

    @Test("Fires on the first observation INSIDE the span, and not one tick earlier")
    func firesOnFirstObservationInsideSpanNotBefore() async throws {
        let (orchestrator, _) = try await Self.makeHarness()
        var reader = BannerReader(await orchestrator.bannerItemStream())

        await orchestrator.receiveAdWindows([
            Self.makeSuggestion(id: "d3g0-entry", start: 60, end: 120)
        ])

        // One transport tick short of the edge: still outside.
        await orchestrator.updatePlayheadTime(
            60 - PlaybackService.periodicTimeObserverIntervalSeconds
        )
        await Self.fireSentinel(orchestrator, id: "d3g0-sentinel-outside", at: 5)
        let outside = await reader.drain(until: "d3g0-sentinel-outside")
        #expect(
            outside.isEmpty,
            "a playhead one tick BEFORE the span has not entered it; got \(outside.map(\.windowId))"
        )

        // Exactly on the edge: entry. Inclusive at the start — the whole point
        // is that there is still an entire ad left to skip.
        await orchestrator.updatePlayheadTime(60)
        await Self.fireSentinel(orchestrator, id: "d3g0-sentinel-inside", at: 130)
        let inside = await reader.drain(until: "d3g0-sentinel-inside")
        #expect(
            inside.map(\.windowId) == ["d3g0-entry"],
            "entry at the span start must fire immediately; got \(inside.map(\.windowId))"
        )
    }

    @Test("Entry latency budget is derived from the real transport tick, not chosen")
    func entryLatencyBudgetIsTiedToTheTransportTick() {
        // The dominant term is position-observer quantisation: the orchestrator
        // learns the playhead moved only when `AVPlayer`'s periodic observer
        // fires. A budget smaller than one tick would be unachievable by
        // construction; one that ignores the tick would be a wish.
        #expect(
            SkipOrchestrator.suggestEntryLatencyBudgetSeconds
                >= PlaybackService.periodicTimeObserverIntervalSeconds,
            "the budget must cover at least one position-observer tick"
        )
        #expect(
            SkipOrchestrator.suggestEntryLatencyBudgetSeconds <= 0.5,
            """
            Dan's decision makes latency correctness, not polish: a banner that \
            appears late is a banner for an ad the listener is already hearing.
            """
        )
    }

    /// The WORST case, constructed rather than sampled: a position observation
    /// lands a hair BEFORE the span start, so the next one — a full tick later —
    /// is the first that can see the entry. Nothing the orchestrator does can
    /// beat that; the only way to lose here is to demand more than one
    /// observation (a dwell, a hysteresis count, an `evaluateAndPush` round
    /// trip), which is exactly the regression this pins.
    @Test("Worst-case tick alignment still banners inside the budget")
    func worstCaseTickAlignmentBannersInsideTheBudget() async throws {
        let (orchestrator, _) = try await Self.makeHarness()
        var reader = BannerReader(await orchestrator.bannerItemStream())

        let spanStart = 60.0
        let tick = PlaybackService.periodicTimeObserverIntervalSeconds
        let lastObservationBefore = spanStart - 0.001
        let firstObservationInside = lastObservationBefore + tick

        await orchestrator.receiveAdWindows([
            Self.makeSuggestion(id: "d3g0-budget", start: spanStart, end: 120)
        ])

        await orchestrator.updatePlayheadTime(lastObservationBefore)
        try await Self.expectNothingBanneredYet(
            orchestrator, &reader,
            sentinelId: "d3g0-sentinel-budget-pre",
            drivingPlayheadTo: 5,
            "an observation 1 ms before the span start has not entered it"
        )

        await orchestrator.updatePlayheadTime(firstObservationInside)
        await Self.fireSentinel(orchestrator, id: "d3g0-sentinel-budget", at: 300)
        let received = await reader.drain(until: "d3g0-sentinel-budget")
        #expect(
            received.map(\.windowId) == ["d3g0-budget"],
            """
            The FIRST observation inside the span must banner. Requiring a second \
            one — any dwell or hysteresis — puts the affordance behind up to \
            \(2 * tick) s of the ad. Got \(received.map(\.windowId)).
            """
        )
        #expect(
            firstObservationInside - spanStart
                <= SkipOrchestrator.suggestEntryLatencyBudgetSeconds,
            """
            Worst-case entry lateness is \(firstObservationInside - spanStart) s, \
            over the \(SkipOrchestrator.suggestEntryLatencyBudgetSeconds) s budget. \
            The listener is already hearing what they wanted to skip.
            """
        )
    }

    // MARK: - 3. Pre-roll (deliberate, not accidental)

    /// DELIBERATE CHOICE, written down rather than inherited: 0:00 gets NO
    /// special treatment. Entry is entry, the pre-roll is the ad a listener
    /// most wants gone, and holding the banner back for a beat would put the
    /// affordance behind the very audio it exists to remove.
    ///
    /// The choice has a second half that only shows up when the episode is
    /// RESUMED: a pre-roll is not "the episode has a pre-roll, so banner it",
    /// it is "the playhead is in the pre-roll". Someone resuming at 10:00 is
    /// not in it and must not be asked about it. Both halves are here, and the
    /// resumed half is what makes this test fail against the detection-time
    /// emitter.
    @Test("Pre-roll banners when the playhead is AT 0:00 — and not when the episode is resumed past it")
    func preRollBannersOnlyWhenThePlayheadIsInIt() async throws {
        let (orchestrator, _) = try await Self.makeHarness()
        var reader = BannerReader(await orchestrator.bannerItemStream())

        // Resumed at 10:00. Detection then delivers the pre-roll.
        await orchestrator.recordUserSeek(to: 600)
        await orchestrator.updatePlayheadTime(600)
        await orchestrator.receiveAdWindows([
            Self.makeSuggestion(id: "d3g0-preroll", start: 0, end: 30)
        ])
        try await Self.expectNothingBanneredYet(
            orchestrator, &reader,
            sentinelId: "d3g0-sentinel-preroll-resumed",
            drivingPlayheadTo: 601,
            "a listener resumed at 10:00 is not in the pre-roll and must not be asked about it"
        )

        // Now they scrub back to the top and the pre-roll is genuinely playing.
        await orchestrator.recordUserSeek(to: 0)
        await orchestrator.updatePlayheadTime(0)
        await Self.fireSentinel(orchestrator, id: "d3g0-sentinel-preroll", at: 400)

        let received = await reader.drain(until: "d3g0-sentinel-preroll")
        #expect(
            received.map(\.windowId) == ["d3g0-preroll"],
            "a span starting at 0 must fire at playhead 0; got \(received.map(\.windowId))"
        )
    }

    // MARK: - 4. Spans the playhead never enters

    @Test("A span already behind the playhead never banners")
    func spanBehindThePlayheadNeverBanners() async throws {
        let (orchestrator, _) = try await Self.makeHarness()
        var reader = BannerReader(await orchestrator.bannerItemStream())

        await orchestrator.updatePlayheadTime(400)
        await orchestrator.receiveAdWindows([
            Self.makeSuggestion(id: "d3g0-past", start: 60, end: 120)
        ])
        await orchestrator.updatePlayheadTime(401)
        await orchestrator.updatePlayheadTime(402)
        await Self.fireSentinel(orchestrator, id: "d3g0-sentinel-past", at: 500)

        let received = await reader.drain(until: "d3g0-sentinel-past")
        #expect(
            received.isEmpty,
            """
            A span the playhead has already passed offers no skip. Asking about \
            it is the "banner for audio already gone" half of the field incident; \
            got \(received.map(\.windowId)).
            """
        )
    }

    // MARK: - 5. Once per window, and seek re-entry

    @Test("Entry fires at most once per window, however many ticks land inside")
    func entryFiresAtMostOncePerWindow() async throws {
        let (orchestrator, _) = try await Self.makeHarness()
        var reader = BannerReader(await orchestrator.bannerItemStream())

        await orchestrator.receiveAdWindows([
            Self.makeSuggestion(id: "d3g0-once", start: 60, end: 120)
        ])
        try await Self.expectNothingBanneredYet(
            orchestrator, &reader,
            sentinelId: "d3g0-sentinel-once-pre",
            drivingPlayheadTo: 40,
            "detection delivery must not banner before the playhead reaches the span"
        )
        for step in 0..<40 {
            await orchestrator.updatePlayheadTime(60 + Double(step) * 0.25)
        }
        await Self.fireSentinel(orchestrator, id: "d3g0-sentinel-once", at: 300)

        let received = await reader.drain(until: "d3g0-sentinel-once")
        #expect(
            received.map(\.windowId) == ["d3g0-once"],
            "40 ticks inside one span must produce exactly one banner; got \(received.count)"
        )
    }

    @Test("Seeking backwards into an already-fired span does not re-ask")
    func seekingBackIntoAFiredSpanDoesNotRefire() async throws {
        let (orchestrator, _) = try await Self.makeHarness()
        var reader = BannerReader(await orchestrator.bannerItemStream())

        await orchestrator.receiveAdWindows([
            Self.makeSuggestion(id: "d3g0-seek", start: 60, end: 120)
        ])
        try await Self.expectNothingBanneredYet(
            orchestrator, &reader,
            sentinelId: "d3g0-sentinel-seek-pre",
            drivingPlayheadTo: 40,
            "detection delivery must not banner before the playhead reaches the span"
        )
        await orchestrator.updatePlayheadTime(60)
        await orchestrator.updatePlayheadTime(200)
        _ = try #require(await reader.drain(until: "d3g0-seek"))

        // Scrub back into the span. The question was already asked; re-asking
        // it is how a banner becomes noise, and the once-per-window guarantee
        // is what stops it.
        await orchestrator.recordUserSeek(to: 65)
        await orchestrator.updatePlayheadTime(65)
        await orchestrator.updatePlayheadTime(70)
        await Self.fireSentinel(orchestrator, id: "d3g0-sentinel-seek", at: 300)

        let received = await reader.drain(until: "d3g0-sentinel-seek")
        #expect(
            received.isEmpty,
            "a backwards seek must not re-fire an answered span; got \(received.map(\.windowId))"
        )
    }

    // MARK: - 6. Late Now Playing host

    @Test("A host that attaches late replays only spans the playhead has entered — exactly once")
    func lateHostReplaysOnlyEnteredSpansExactlyOnce() async throws {
        let (orchestrator, _) = try await Self.makeHarness()

        // No Now Playing surface yet: nothing is observing the banner stream.
        await orchestrator.receiveAdWindows([
            Self.makeSuggestion(id: "d3g0-replay-entered", start: 60, end: 120),
            Self.makeSuggestion(id: "d3g0-replay-unentered", start: 3000, end: 3060),
        ])
        await orchestrator.updatePlayheadTime(65)

        // Now Playing appears.
        var reader = BannerReader(await orchestrator.bannerItemStream())
        await Self.fireSentinel(orchestrator, id: "d3g0-sentinel-replay", at: 200)

        let replayed = await reader.drain(until: "d3g0-sentinel-replay")
        #expect(
            replayed.map(\.windowId) == ["d3g0-replay-entered"],
            """
            A suggestion produced while Now Playing was absent must still be \
            delivered exactly once — but gated on POSITION, not dropped and not \
            replayed for audio the listener has not reached. \
            Got \(replayed.map(\.windowId)).
            """
        )
    }

    // MARK: - 7. Evidence snapshot

    @Test("Moving the emit later means the banner carries the RICHER catalog")
    func emitAtEntryCarriesTheCatalogThatArrivedAfterDetection() async throws {
        let (orchestrator, _) = try await Self.makeHarness()
        var reader = BannerReader(await orchestrator.bannerItemStream())

        await orchestrator.receiveAdWindows([
            Self.makeSuggestion(id: "d3g0-catalog", start: 60, end: 120)
        ])

        // The catalog lands AFTER detection delivery — the normal ordering, as
        // transcript material keeps arriving. Snapshot-at-emit is unchanged as
        // a rule; the emit simply happens later, so the snapshot is richer.
        await orchestrator.setEvidenceCatalog(
            EvidenceCatalog(
                analysisAssetId: Self.assetId,
                transcriptVersion: "v-test",
                entries: [
                    EvidenceEntry(
                        evidenceRef: 0,
                        category: .disclosurePhrase,
                        matchedText: "sponsored by",
                        normalizedText: "sponsored by",
                        atomOrdinal: 0,
                        startTime: 75,
                        endTime: 76,
                        count: 1,
                        firstTime: 75,
                        lastTime: 76
                    )
                ]
            )
        )
        await orchestrator.updatePlayheadTime(60)
        await Self.fireSentinel(orchestrator, id: "d3g0-sentinel-catalog", at: 300)

        let received = await reader.drain(until: "d3g0-sentinel-catalog")
        let banner = try #require(
            received.first { $0.windowId == "d3g0-catalog" },
            "entry did not banner; got \(received.map(\.windowId))"
        )
        #expect(
            banner.evidenceCatalogEntries.map(\.evidenceRef) == [0],
            """
            Emitting at entry rather than at detection means a catalog that \
            arrived in between is IN the snapshot. Got \
            \(banner.evidenceCatalogEntries.count) entries.
            """
        )
    }
}

// MARK: - Skip affordance

/// playhead-d3g0 folds in the consequence of playhead-ynmk (#313), and without
/// it entry-firing ships a broken affordance.
///
/// ynmk made a banner confirmation assert PRESENCE, never EXTENT: the user
/// answers "is this an ad?", the DETECTOR drew the edges, so a confirmation is
/// governed by `AutoSkipEdgePadding` exactly like an auto-skip. On a span with
/// both edges `unanchored` — the entire field population, 3 of 3 — that policy
/// returns nil and the confirmation produces a MARK. Playback does not move.
///
/// Combine that with entry-firing and the user taps at the exact moment the ad
/// starts and NOTHING HAPPENS. That is the opposite of "when it enters so I can
/// skip". A banner must not offer an action it cannot perform, so the card
/// carries `confirmationSkipsPlayback`, resolved from the SAME per-edge policy
/// the acceptance transaction consults — not a second derivation that could
/// drift from it. `affordanceMatchesWhatTheTapActuallyDoes` is the test that
/// keeps the two honest with each other.
@Suite("Suggest banner skip affordance (playhead-d3g0)", .timeLimit(.minutes(1)))
struct SuggestBannerSkipAffordanceTests {

    /// Drive one markOnly window to its entry banner and hand back the card
    /// plus the live harness, so a test can compare the card's claim against
    /// what the acceptance transaction then durably does.
    private static func banner(
        for window: AdWindow,
        entering time: Double
    ) async throws -> (
        item: AdSkipBannerItem,
        orchestrator: SkipOrchestrator,
        store: AnalysisStore
    ) {
        let harness = try await SuggestBannerEntryGateTests.makeHarness()
        var reader = BannerReader(
            await harness.orchestrator.bannerItemStream()
        )
        try await harness.store.insertAdWindow(window)
        await harness.orchestrator.receiveAdWindows([window])
        await harness.orchestrator.updatePlayheadTime(time)
        await SuggestBannerEntryGateTests.fireSentinel(
            harness.orchestrator, id: "d3g0-affordance-sentinel", at: 8000
        )
        let received = await reader.drain(until: "d3g0-affordance-sentinel")
        let item = try #require(
            received.first { $0.windowId == window.id },
            "entry did not banner; got \(received.map(\.windowId))"
        )
        return (item, harness.orchestrator, harness.store)
    }

    /// The field population: confidence 0.40-0.42, both edges unanchored. ynmk
    /// makes confirming these a MARK, so the card must not present a skip.
    @Test("A both-edges-unanchored span does not offer a skip it cannot perform")
    func unanchoredSpanDoesNotOfferSkip() async throws {
        let window = SuggestBannerEntryGateTests.makeSuggestion(
            id: "d3g0-afford-unanchored", start: 4800, end: 4950
        )
        let (item, _, _) = try await Self.banner(for: window, entering: 4800)
        #expect(
            item.confirmationSkipsPlayback == false,
            """
            Both edges unanchored: `AutoSkipEdgePadding` has no late-safe window, \
            so confirming MARKS and playback does not move. Offering "skip" here \
            is a button that does nothing at the exact moment the user wants it \
            to do something.
            """
        )
    }

    /// playhead-qs0d's 2-of-2 byte-exact population. These must keep offering a
    /// real skip — the fix must not overshoot into "never offer a skip".
    @Test("A byte-exact span still offers a real skip")
    func byteExactSpanStillOffersSkip() async throws {
        let window = SuggestBannerEntryGateTests.makeSuggestion(
            id: "d3g0-afford-byteexact",
            start: 4800,
            end: 4950,
            startAnchor: .rediffByteExact,
            endAnchor: .rediffByteExact
        )
        let (item, _, _) = try await Self.banner(for: window, entering: 4800)
        #expect(
            item.confirmationSkipsPlayback,
            "a both-edges-byte-exact span is exactly the population qs0d activated skipping for"
        )
    }

    /// The anti-lie test. Whatever the card claims, the acceptance transaction
    /// must do — for both anchor populations, through the DURABLE row rather
    /// than through the same in-memory predicate the card read.
    @Test(
        "The card's claim matches what the tap actually does",
        arguments: [
            (
                label: "both-unanchored",
                start: AutoSkipEdgeAnchor.unanchored,
                end: AutoSkipEdgeAnchor.unanchored
            ),
            (
                label: "both-byte-exact",
                start: AutoSkipEdgeAnchor.rediffByteExact,
                end: AutoSkipEdgeAnchor.rediffByteExact
            ),
            (
                label: "anchored-start-unanchored-end",
                start: AutoSkipEdgeAnchor.rediffByteExact,
                end: AutoSkipEdgeAnchor.unanchored
            ),
        ]
    )
    func affordanceMatchesWhatTheTapActuallyDoes(
        label: String,
        start: AutoSkipEdgeAnchor,
        end: AutoSkipEdgeAnchor
    ) async throws {
        let windowId = "d3g0-afford-match-\(label)"
        let window = SuggestBannerEntryGateTests.makeSuggestion(
            id: windowId,
            start: 4800,
            end: 4950,
            startAnchor: start,
            endAnchor: end
        )
        let (item, orchestrator, store) = try await Self.banner(
            for: window, entering: 4800
        )

        #expect(await orchestrator.acceptSuggestedSkip(windowId: windowId))
        let rows = try await store.fetchAdWindows(
            assetId: SuggestBannerEntryGateTests.assetId
        )
        let promoted = try #require(
            rows.first { $0.id != windowId && $0.startTime == 4800 },
            "[\(label)] the confirmation did not persist a promoted row"
        )
        #expect(
            promoted.wasSkipped == item.confirmationSkipsPlayback,
            """
            [\(label)] the card said confirmation \
            \(item.confirmationSkipsPlayback ? "skips" : "only marks"), the durable \
            row says wasSkipped=\(promoted.wasSkipped). A banner that promises a \
            skip it will not perform is the affordance bug; a banner that hides a \
            skip it WILL perform is the same bug mirrored.
            """
        )
        #expect(
            (promoted.decisionState == AdDecisionState.applied.rawValue)
                == item.confirmationSkipsPlayback,
            "[\(label)] decisionState and the card's claim must agree too"
        )
    }

    /// The copy seam. Exactly one place produces the mark-only wording, so the
    /// copy pass (playhead-1mq1.1) has one file to edit.
    @Test("Mark-only copy drops the skip promise; skippable copy is untouched")
    func markOnlyCopyDropsTheSkipPromise() {
        let skippable = AdBannerView.feedbackChoiceContent(
            for: .suggest, confirmationSkipsPlayback: true
        )
        #expect(skippable.confirmLabel == "Yes")
        #expect(skippable.confirmAccessibilityLabel == "Yes, skip this sponsor break")
        #expect(skippable.confirmAccessibilityHint == "Confirms this is an ad and skips it")

        let markOnly = AdBannerView.feedbackChoiceContent(
            for: .suggest, confirmationSkipsPlayback: false
        )
        #expect(
            markOnly.confirmLabel != skippable.confirmLabel,
            "the mark-only card must not present the same action as the skipping one"
        )
        for text in [
            markOnly.confirmLabel,
            markOnly.confirmAccessibilityLabel,
            markOnly.confirmAccessibilityHint,
        ] {
            #expect(
                !text.lowercased().contains("skip"),
                """
                "\(text)" promises a skip this confirmation cannot perform. \
                Playback does not move on a both-edges-unanchored span (ynmk).
                """
            )
        }
        // The DENY side is unchanged in both: "No" already promises nothing.
        #expect(markOnly.denyLabel == skippable.denyLabel)
        #expect(
            markOnly.denyAccessibilityHint == skippable.denyAccessibilityHint
        )
    }

    /// An auto-skipped banner reports a skip that ALREADY happened, so the
    /// field is meaningless there and its default must not perturb that copy.
    @Test("Auto-skipped copy is unaffected by the affordance flag")
    func autoSkippedCopyUnaffected() {
        let base = AdBannerView.feedbackChoiceContent(for: .autoSkipped)
        for flag in [true, false] {
            #expect(
                AdBannerView.feedbackChoiceContent(
                    for: .autoSkipped, confirmationSkipsPlayback: flag
                ) == base
            )
        }
    }
}
