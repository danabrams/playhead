// EligibilityLadderMonotonicityTests.swift
// playhead-wq34: a window that earned MORE provenance must never produce LESS
// user-visible output.
//
// THE INVERSION, as it shipped. `eligibilityGate` is a ladder — `.eligible`
// carries strictly more provenance than `.markOnly` — and at the listener it
// ran backwards:
//
//   * `.markOnly` → `SkipOrchestrator.receiveAdWindows` routes to the SUGGEST
//     tier. `emitSuggestBannersOnPlayheadEntry` fires on playhead entry and
//     consults the playhead, never the skip mode. The listener gets a card with
//     Yes/No, in `.shadow`, `.manual` and `.auto` alike.
//   * `.eligible` (and `nil`, and the `"autoSkip"` literal) → the MANAGED tier.
//     `evaluateWindow` returns `.confirmed` for both `.shadow` and `.manual`
//     (the two arms differ only in a log string) and `evaluateAndPush` banners
//     only on `.applied`. For any class that is not `.auto`: NO SKIP AND NO
//     CARD.
//
// Post-playhead-lqcp, `.segmentAggregated`, `.fusion` and `.userAsserted`
// cannot reach `.auto` without an explicit user override, so on the shipped
// default the pipeline's highest-confidence output was the one the listener
// never saw. playhead-6akp is the same defect seen from the other end: a
// demoted byte-exact class emits `.eligible` windows that produce no banner, so
// the listener can never say Yes, so playhead-u0vv's restoration can never
// fire.
//
// WHY A PROBE AND NOT UNIT TESTS. The claim is about what the LISTENER
// RECEIVES, and that is a property of the whole orchestrator — two admission
// doors, two tiers, an arming set, a playhead-entry emit trigger and a cue
// push. A unit test of any one of those can be green while the listener gets
// nothing, which is exactly the state that shipped. So these drive the real
// `SkipOrchestrator` and assert on pushed skip cues and emitted banners.
//
// THE MATRIX IS THE CLAIM. {`.markOnly`, `.eligible`} × {`.shadow`, `.manual`,
// `.auto`} is stated as a table, twice: once as the exact expected outcome per
// cell (so "turn everything into a card" fails), and once as the ORDERING
// `eligible >= markOnly` per mode (so the monotonicity property is asserted as
// a property rather than inferred by a reader comparing six literals).
//
// OBSERVATION METHOD — borrowed verbatim from `SuggestBannerEntryGateTests`,
// and for its reason. Emission is SYNCHRONOUS inside the actor and
// `AsyncStream` buffers on `yield`, so by the time an awaited orchestrator call
// returns, everything it emitted is already in the buffer. A reader that owns
// the iterator pulls it without ever suspending, and a SENTINEL window driven
// after the operation under test is an exact frame boundary. No observer task,
// no polling, no deadline — under load these get slower and never wrong.

import CoreMedia
import Foundation
import Testing

@testable import Playhead

// MARK: - What the listener got

/// The listener-visible result of one delivery, as an ORDINAL.
///
/// This is the quantity `HotPathAdmission` claims to be "about the LISTENER
/// rather than about the string", and the one the shipped ladder inverted. The
/// order is not a preference: `.card` strictly dominates `.nothing` because it
/// can be answered, and `.skip` strictly dominates `.card` because the audio is
/// gone without the listener doing anything.
private enum ListenerOutcome: Int, Comparable, CustomStringConvertible {
    /// No banner of any tier, no skip cue. A passive timeline block at most.
    case nothing = 0
    /// A suggest-tier card, with Yes/No, fired on playhead entry.
    case card = 1
    /// A skip cue was pushed and the auto-tier banner announced it.
    case skip = 2

    static func < (lhs: Self, rhs: Self) -> Bool {
        lhs.rawValue < rhs.rawValue
    }

    var description: String {
        switch self {
        case .nothing: return "nothing"
        case .card: return "a suggest card"
        case .skip: return "a skip"
        }
    }
}

// MARK: - Reader

/// Single-consumer reader over the banner stream. Owns the iterator, so a pull
/// returns already-buffered items without depending on any other task getting
/// scheduled.
private struct LadderBannerReader {
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

// MARK: - Collector

/// The detector classes credited through
/// `SkipOrchestrator`'s correct-observation seam. An actor because the seam is
/// invoked from a `Task` the orchestrator spawns, so a captured `var` would be
/// a race rather than an observation.
private actor CreditedDetectors {
    private var recorded: [SkipDetectorClass] = []

    func record(_ detector: SkipDetectorClass) { recorded.append(detector) }
    func count() -> Int { recorded.count }
    func snapshot() -> [SkipDetectorClass] { recorded }
}

// MARK: - Suite

@Suite(
    "playhead-wq34 — the eligibility ladder never inverts at the listener",
    .timeLimit(.minutes(1))
)
struct EligibilityLadderMonotonicityTests {

    private static let assetId = "asset-1"
    private static let episodeId = "asset-1"
    /// MUST be the show `makeSkipTestTrustService` seeds; a show with no
    /// profile resolves to `.shadow` for every show-governed class and the
    /// `.auto` row of the matrix would become vacuous.
    private static let podcastId = "podcast-1"

    private static let spanStart: Double = 60
    private static let spanEnd: Double = 120
    private static let sentinelStart: Double = 300

    // MARK: Fixture

    /// A window whose detector class is `.segmentAggregated` — one of the three
    /// SHOW-GOVERNED classes, so its mode is the profile's and the matrix's
    /// mode axis is real. `.rediffByteExact` would seed `.auto` regardless of
    /// the profile (`SkipDetectorClass.showIndependentSeedMode`) and collapse
    /// the axis to one value.
    private static func aggregatedWindow(
        id: String,
        gate: SkipEligibilityGate?,
        start: Double = spanStart,
        end: Double = spanEnd,
        decisionState: AdDecisionState = .confirmed,
        boundaryState: String = AdBoundaryState.segmentAggregated.rawValue
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            // Comfortably above `enterThreshold`, so the managed tier's
            // pre-mode early returns are not what decides these cells.
            confidence: 0.90,
            boundaryState: boundaryState,
            decisionState: decisionState.rawValue,
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
            eligibilityGate: gate?.rawValue,
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
    }

    /// A byte-exact span: both edges `.rediffByteExact`, which is the SHARED
    /// definition `SkipDetectorClass.classify` reads.
    private static func byteExactWindow(
        id: String,
        gate: SkipEligibilityGate?,
        start: Double = spanStart,
        end: Double = spanEnd
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: 0.90,
            boundaryState: "lexical",
            decisionState: AdDecisionState.confirmed.rawValue,
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
            eligibilityGate: gate?.rawValue,
            startEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue
        )
    }

    private static func makeHarness(
        mode: SkipMode
    ) async throws -> (
        orchestrator: SkipOrchestrator,
        trust: TrustScoringService,
        store: AnalysisStore
    ) {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )
        let trust = try await makeSkipTestTrustService(
            mode: mode.rawValue,
            trustScore: mode == .auto ? 0.9 : 0.5,
            observations: mode == .auto ? 10 : 0
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trust,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        return (orchestrator, trust, store)
    }

    /// Deliver one window, walk the playhead into its span, and report what the
    /// listener received.
    ///
    /// The sentinel is a `markOnly` window delivered AFTER the window under
    /// test and driven at a position outside it. `markOnly` because the suggest
    /// path is the one tier that never consults the mode, so the same sentinel
    /// is a valid frame boundary in all three modes.
    private static func listenerOutcome(
        _ orchestrator: SkipOrchestrator,
        delivering window: AdWindow,
        reader: inout LadderBannerReader
    ) async -> (outcome: ListenerOutcome, cues: Int, banners: [AdSkipBannerItem]) {
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }

        await orchestrator.receiveAdWindows([window])
        let cuesAtDelivery = pushedCues.count
        await orchestrator.updatePlayheadTime(window.startTime + 5)

        let sentinelId = "wq34-sentinel-\(window.id)"
        await orchestrator.receiveAdWindows([
            aggregatedWindow(
                id: sentinelId,
                gate: .markOnly,
                start: sentinelStart,
                end: sentinelStart + 4
            )
        ])
        await orchestrator.updatePlayheadTime(sentinelStart)

        let received = await reader.drain(until: sentinelId)
        let mine = received.filter { $0.windowId == window.id }
        let outcome: ListenerOutcome
        if cuesAtDelivery > 0 {
            outcome = .skip
        } else if mine.contains(where: { $0.tier == .suggest }) {
            outcome = .card
        } else {
            outcome = .nothing
        }
        return (outcome, cuesAtDelivery, mine)
    }

    // MARK: - 1. The matrix

    /// The single claim, as a table. One cell per {gate} × {mode}.
    ///
    /// BEFORE this bead the `.eligible` row read `nothing / nothing / skip` —
    /// strictly below the `.markOnly` row in two of three modes, which is the
    /// inversion. AFTER, it reads `card / card / skip`.
    @Test(
        "THE MATRIX: {markOnly, eligible} × {shadow, manual, auto} — eligible is never worse",
        arguments: [SkipMode.shadow, .manual, .auto]
    )
    func eligibleNeverDeliversLessThanMarkOnly(mode: SkipMode) async throws {
        // markOnly and eligible are run on SEPARATE orchestrators. One
        // orchestrator delivering both would let the first delivery's
        // `banneredWindowIds` / `inAdState` bookkeeping colour the second, and
        // the claim is about two independent rows, not a sequence.
        var outcomes: [SkipEligibilityGate: ListenerOutcome] = [:]
        var cueCounts: [SkipEligibilityGate: Int] = [:]

        for gate in [SkipEligibilityGate.markOnly, .eligible] {
            let (orchestrator, trust, _) = try await Self.makeHarness(mode: mode)
            var reader = LadderBannerReader(await orchestrator.bannerItemStream())
            await orchestrator.beginEpisode(
                analysisAssetId: Self.assetId,
                episodeId: Self.episodeId,
                podcastId: Self.podcastId
            )

            // THE STATE THIS TEST IS ABOUT, ASSERTED RATHER THAN ASSUMED. The
            // matrix's mode axis is worthless if the ledger disagrees with the
            // profile — and a veto that forks the per-class ledger is exactly
            // how that happens. Ask the same service the orchestrator asked.
            let resolved = await trust.resolveDetectorModes(
                podcastId: Self.podcastId
            )
            try #require(
                resolved.mode(for: .segmentAggregated) == mode,
                """
                the ledger resolved \(resolved.mode(for: .segmentAggregated)) for \
                .segmentAggregated, not \(mode) — this cell would prove nothing
                """
            )

            let observed = await Self.listenerOutcome(
                orchestrator,
                delivering: Self.aggregatedWindow(
                    id: "wq34-\(gate.rawValue)-\(mode.rawValue)",
                    gate: gate
                ),
                reader: &reader
            )
            outcomes[gate] = observed.outcome
            cueCounts[gate] = observed.cues
        }

        let markOnly = try #require(outcomes[.markOnly])
        let eligible = try #require(outcomes[.eligible])

        // (a) THE ORDERING — the monotonicity property itself.
        #expect(
            eligible >= markOnly,
            """
            THE LADDER IS INVERTED in \(mode) mode: a row stamped .eligible got \
            \(eligible) while the identical row stamped .markOnly got \(markOnly). \
            A window that earned MORE provenance must never produce LESS \
            user-visible output.
            """
        )

        // (b) THE EXACT CELLS — so "make everything a card" cannot pass (a).
        #expect(
            markOnly == .card,
            "a markOnly row always cards on playhead entry, in every mode; got \(markOnly)"
        )
        switch mode {
        case .shadow, .manual:
            #expect(
                eligible == .card,
                """
                an .eligible row whose detector class is \(mode) must reach the \
                listener as a suggest card, not a silent .confirmed; got \(eligible)
                """
            )
            #expect(
                cueCounts[.eligible] == 0,
                "and it must NOT skip — \(mode) is not an authorisation to cut audio"
            )
        case .auto:
            #expect(
                eligible == .skip,
                """
                UNCHANGED FOR .auto: an .eligible row for an auto class still \
                auto-skips. This bead must not touch the auto path; got \(eligible)
                """
            )
        }
    }

    // MARK: - 2. The tiers stay disjoint

    /// A window must not produce a suggest card AND a managed banner.
    ///
    /// The construction makes this structural rather than lucky — the fallback
    /// routes through the SAME branch `.markOnly` takes, which `continue`s
    /// before `windows[id]` is ever written, so `evaluateAndPush` cannot see
    /// the row. This asserts the consequence at both ends: exactly one banner,
    /// and the row present in exactly one of the two tier maps.
    @Test("A diverted row is in the suggest tier and NOT the managed one — never both")
    func divertedRowOccupiesExactlyOneTier() async throws {
        let (orchestrator, _, _) = try await Self.makeHarness(mode: .manual)
        var reader = LadderBannerReader(await orchestrator.bannerItemStream())
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )

        let observed = await Self.listenerOutcome(
            orchestrator,
            delivering: Self.aggregatedWindow(id: "wq34-disjoint", gate: .eligible),
            reader: &reader
        )

        #expect(observed.banners.count == 1, "exactly one card, not two")
        #expect(observed.banners.first?.tier == .suggest)
        #expect(
            await orchestrator.activeSuggestWindowIDs().contains("wq34-disjoint")
        )
        #expect(
            !(await orchestrator.activeWindowIDs().contains("wq34-disjoint")),
            """
            the row is in BOTH tiers — playhead-rfu-sad: a still-visible suggest \
            banner over a managed window can re-fire acceptSuggestedSkip and \
            synthesize a duplicate managed window under a fresh UUID
            """
        )
        #expect(
            !(await orchestrator.emittedAutoSkipBannersSnapshot())
                .contains("wq34-disjoint"),
            "no auto-tier banner: nothing was skipped, so nothing may say 'Skipped …'"
        )
    }

    // MARK: - 3. The two `.applied` exclusions

    /// A durably-`.applied` row is a RECEIPT for a skip that happened. Turning
    /// it into a question is a regression in the other direction — and worse
    /// than cosmetic: `forwardPersistedAdWindows` pre-populates
    /// `banneredWindowIds` for applied rows, so a diverted one would land on
    /// `.droppedAlreadyBannered` with no card AND no cue, silently regressing
    /// cross-launch auto-skip.
    @Test("A durably-applied row still re-cues on reload in a non-auto mode")
    func appliedReceiptIsNotDivertedToSuggest() async throws {
        let (orchestrator, _, _) = try await Self.makeHarness(mode: .manual)
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }

        await orchestrator.receiveAdWindows([
            Self.aggregatedWindow(
                id: "wq34-applied",
                gate: .eligible,
                decisionState: .applied
            )
        ])

        #expect(
            pushedCues.count == 1,
            """
            a previously-skipped ad must push its cue again on the next launch. \
            The mode is \(SkipMode.manual) and the class is show-governed, so the \
            monotonicity fallback sees this row — and must decline it.
            """
        )
        #expect(
            await orchestrator.activeWindowIDs().contains("wq34-applied"),
            "the receipt belongs in the managed tier"
        )
        #expect(
            (await orchestrator.activeSuggestWindowIDs()).isEmpty,
            "a receipt is not a question"
        )
    }

    /// The in-session twin, and the ONE case where the mode genuinely changes
    /// under a live episode: the listener moves the Settings / Now Playing skip
    /// control down while an ad has already been skipped.
    ///
    /// `setActiveSkipMode` is the only writer of `activeDetectorSkipModes`
    /// outside `beginEpisode` (which clears `windows`), so it is the only way
    /// `existingState == .applied` can meet a non-`.auto` mode inside one
    /// session. Without the exclusion, the next producer replay would retire
    /// the applied window and offer it as a card — retracting a skip that
    /// already happened and asking about audio that is already gone.
    @Test("A window applied in-session survives a mid-episode demotion and its replay")
    func inSessionAppliedWindowIsNotDivertedToSuggest() async throws {
        let (orchestrator, _, _) = try await Self.makeHarness(mode: .auto)
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }

        let row = Self.aggregatedWindow(id: "wq34-insession", gate: .eligible)
        await orchestrator.receiveAdWindows([row])
        try #require(
            pushedCues.count == 1,
            "the auto-mode skip must fire, or the replay below proves nothing"
        )

        // The listener turns the control down mid-episode. Every class is now
        // `.manual`.
        await orchestrator.setActiveSkipMode(.manual)

        // The producer re-delivers the same id.
        await orchestrator.receiveAdWindows([row])
        #expect(
            pushedCues.count == 1,
            "the replay must not retract a skip that has already happened"
        )
        #expect(
            await orchestrator.activeWindowIDs().contains("wq34-insession"),
            "the applied receipt stays in the managed tier"
        )
        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains("wq34-insession"),
            "and must not be re-offered as a question"
        )
    }

    // MARK: - 4. The second door

    /// `receiveAdDecisionResults` — the final-pass backfill's fusion handoff —
    /// admits ONLY `.eligible` rows, so before this bead every row it delivered
    /// for a non-`.auto` class was silent BY CONSTRUCTION. Fixing one door and
    /// not the other would put the inversion between the doors: the identical
    /// row carding when the preload delivered it and vanishing when backfill
    /// did.
    @Test("The decision-result door obeys the same rule as the AdWindow door")
    func decisionResultDoorAlsoDivertsToSuggest() async throws {
        let (orchestrator, _, _) = try await Self.makeHarness(mode: .shadow)
        var reader = LadderBannerReader(await orchestrator.bannerItemStream())
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }

        let row = Self.aggregatedWindow(id: "wq34-fusion", gate: .eligible)
        await orchestrator.receiveAdDecisionResults([
            AdDecisionResult(
                id: row.id,
                analysisAssetId: row.analysisAssetId,
                startTime: row.startTime,
                endTime: row.endTime,
                skipConfidence: row.actuationConfidence,
                eligibilityGate: .eligible,
                recomputationRevision: 1
            ).withProducerRevision(row)
        ])
        await orchestrator.updatePlayheadTime(Self.spanStart + 5)

        let sentinelId = "wq34-fusion-sentinel"
        await orchestrator.receiveAdWindows([
            Self.aggregatedWindow(
                id: sentinelId,
                gate: .markOnly,
                start: Self.sentinelStart,
                end: Self.sentinelStart + 4
            )
        ])
        await orchestrator.updatePlayheadTime(Self.sentinelStart)
        let received = await reader.drain(until: sentinelId)

        #expect(pushedCues.isEmpty, "shadow mode skips nothing")
        #expect(
            received.contains { $0.windowId == row.id && $0.tier == .suggest },
            """
            the backfill's fusion handoff delivered an .eligible row for a \
            shadow-mode class and the listener got NOTHING. Got \
            \(received.map { "\($0.windowId)/\($0.tier)" }).
            """
        )
        #expect(
            !(await orchestrator.activeWindowIDs().contains(row.id)),
            "and it must not also sit in the managed tier"
        )
    }

    // MARK: - 5. playhead-6akp: the restoration becomes reachable

    /// THE 6akp CLOSURE, end to end on the real orchestrator.
    ///
    /// playhead-u0vv restores `.rediffByteExact` to its show-independent seed
    /// once the class's `falseSkipWeight` decays to 0. The only production
    /// writer that lowers a weight through `restoredMode` is
    /// `TrustScoringService.recordCorrectObservation`, whose only production
    /// caller is `SkipOrchestrator.acceptSuggestedSkip` — a banner Yes. But the
    /// demotion population (byte-exact spans stamped `.eligible`, which is what
    /// the day-0 mint writes) went to the managed tier, where a demoted class
    /// produces no banner at all. The debt was incurred where it could not be
    /// paid.
    ///
    /// The fix makes the demoted class's own spans CARDS, so the listener keeps
    /// being asked and every Yes pays a unit off.
    @Test("6akp: a demoted byte-exact class still asks, so its debt can be discharged")
    func demotedByteExactClassStillReachesTheListener() async throws {
        let (orchestrator, trust, store) = try await Self.makeHarness(mode: .shadow)

        // Two ATTRIBUTED deterministic vetoes. The attribution is the whole
        // point: `recordFalseSkipSignal(podcastId:)` with no attributions is
        // the one call shape that does not fork the per-class ledger, and the
        // one shape production never uses. Without it this test would demote
        // nothing and prove nothing.
        for _ in 0..<2 {
            await trust.recordFalseSkipSignal(
                podcastId: Self.podcastId,
                attributions: [
                    DetectorVetoAttribution(
                        detector: .rediffByteExact,
                        tier: .deterministic
                    )
                ]
            )
        }

        let resolved = await trust.resolveDetectorModes(podcastId: Self.podcastId)
        try #require(
            resolved.mode(for: .rediffByteExact) == .manual,
            """
            two deterministic vetoes must demote the class to .manual — resolved \
            \(resolved.mode(for: .rediffByteExact)). Everything below is about \
            what a DEMOTED class does.
            """
        )

        let credited = CreditedDetectors()
        await orchestrator._setCorrectObservationHandlerForTesting { _, _, detector in
            await credited.record(detector)
        }
        var reader = LadderBannerReader(await orchestrator.bannerItemStream())
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )

        // The day-0 mint's stamp, verbatim: byte-exact edges, gate `.eligible`.
        // Inserted durably too, because the Yes below commits through
        // `persistAcceptedSuggestionIfCurrent`, which fences against the exact
        // row.
        let minted = Self.byteExactWindow(id: "wq34-6akp", gate: .eligible)
        try await store.insertAdWindow(minted)
        await orchestrator.receiveAdWindows([minted])
        await orchestrator.updatePlayheadTime(Self.spanStart + 5)

        let sentinelId = "wq34-6akp-sentinel"
        await orchestrator.receiveAdWindows([
            Self.aggregatedWindow(
                id: sentinelId,
                gate: .markOnly,
                start: Self.sentinelStart,
                end: Self.sentinelStart + 4
            )
        ])
        await orchestrator.updatePlayheadTime(Self.sentinelStart)
        let received = await reader.drain(until: sentinelId)

        #expect(
            received.contains { $0.windowId == "wq34-6akp" && $0.tier == .suggest },
            """
            playhead-6akp: the demoted byte-exact class emitted an .eligible \
            window and the listener was never offered it. There is no gesture \
            that can discharge the debt, so the class stays manual forever — \
            which is the state playhead-u0vv was opened to end. Got \
            \(received.map { "\($0.windowId)/\($0.tier)" }).
            """
        )

        // And the Yes credits the class that drew the span — the exact write
        // `restoredMode` reads.
        let accepted = await orchestrator.acceptSuggestedSkip(
            windowId: "wq34-6akp"
        )
        #expect(accepted, "the card must be answerable")
        let landed = await pollUntil(timeout: .seconds(10)) {
            await credited.count() > 0
        }
        #expect(landed, "the correct-observation write never landed")
        let recorded = await credited.snapshot()
        #expect(
            recorded == [.rediffByteExact],
            """
            a Yes on a byte-exact card must credit .rediffByteExact — that write \
            is the ONLY production path that lowers a falseSkipWeight through \
            restoredMode. Got \(recorded).
            """
        )
    }

    // MARK: - 5b. The pill still works on the episode in hand

    /// The regression wq34 would have shipped without
    /// `readmitModeDivertedSuggestions`, and the `.markOnly` exclusion that
    /// keeps the remedy from over-reaching.
    ///
    /// `setActiveSkipMode` is the production skip control
    /// (`PlayheadRuntime.setShowSkipMode`) and the only writer of
    /// `activeDetectorSkipModes` that does not clear `windows`. Before wq34 a
    /// row delivered on a shadow show waited silently in the managed tier and
    /// this call promoted it on the spot; after the diversion it is a card, and
    /// `evaluateAndPush` iterates `windows`, which no longer holds it. "Turn
    /// auto-skip on" would have stopped working for everything already
    /// delivered — which is precisely the "must not alter behaviour for a class
    /// that IS `.auto`" line this bead was told not to cross.
    ///
    /// The second window is the control, and it is not decoration: re-admitting
    /// on a mode change must NOT sweep up `.markOnly` rows, whose tier is a
    /// precision verdict about the row rather than a consequence of the mode.
    /// Without it, "re-deliver everything in the suggest tier" would pass the
    /// first three assertions and quietly promote a population no instruction
    /// was ever meant to reach.
    @Test("Turning the skip control to auto mid-episode still skips what was already delivered")
    func pillRestoresAutoSkipForAlreadyDeliveredRows() async throws {
        let (orchestrator, _, _) = try await Self.makeHarness(mode: .shadow)
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }

        await orchestrator.receiveAdWindows([
            Self.aggregatedWindow(id: "wq34-pill-eligible", gate: .eligible),
            Self.aggregatedWindow(
                id: "wq34-pill-markonly",
                gate: .markOnly,
                start: 200,
                end: 260
            ),
        ])
        try #require(
            await orchestrator.activeSuggestWindowIDs()
                == ["wq34-pill-eligible", "wq34-pill-markonly"],
            "both rows start in the suggest tier — the shadow show can act on neither"
        )
        try #require(pushedCues.isEmpty, "shadow mode skips nothing")

        // THE CONTROL'S OBSERVABLE, chosen because the obvious one does not
        // work. R1 measured it: deleting the `.markOnly` filter from
        // `readmitModeDivertedSuggestions` left every assertion below GREEN,
        // because a re-delivered `.markOnly` row is routed straight back to the
        // suggest tier by `receiveAdWindows`' own gate. "It is still in the
        // suggest tier" therefore cannot tell "it was never re-delivered" from
        // "it was re-delivered and bounced" — and the difference is not
        // cosmetic: re-delivery takes a fresh producer-mutation generation,
        // which invalidates any genuine revision for that id that is in flight.
        // The ingest census can tell them apart, because a swept-up row is
        // stamped a second time.
        let armedBeforePill = await orchestrator
            .adWindowIngestOutcomeCount(.armedSuggest)

        // The listener turns the control up.
        await orchestrator.setActiveSkipMode(.auto)

        #expect(
            pushedCues.count == 1,
            """
            the pill did not reach the row already delivered. Before wq34 this \
            promoted on the spot; a diversion that cannot be undone by an \
            explicit instruction is a regression, not a fix.
            """
        )
        #expect(
            await orchestrator.activeWindowIDs() == ["wq34-pill-eligible"],
            "the eligible row moves to the managed tier and skips"
        )
        #expect(
            await orchestrator.activeSuggestWindowIDs() == ["wq34-pill-markonly"],
            """
            a `.markOnly` row is in the suggest tier because of a precision \
            verdict about the ROW, not because of the mode. No instruction \
            promotes it, and a re-admission that swept it up would be \
            auto-skipping a population the gate deliberately excluded.
            """
        )
        #expect(
            await orchestrator.adWindowIngestOutcomeCount(.armedSuggest)
                == armedBeforePill,
            """
            THE CONTROL, MEASURED: the pill RE-DELIVERED the `.markOnly` row. \
            The assertion above cannot see that — `receiveAdWindows` puts it \
            straight back — but the census can, and a re-delivery costs a \
            producer-mutation generation for a row no instruction can ever \
            promote.
            """
        )
    }

    // MARK: - 5c. The mode gate the diversion took out of coverage

    /// THE `.shadow` / `.manual` ARMS OF `evaluateWindow`, pinned through the
    /// one door that still reaches them.
    ///
    /// Before this bead, three tests reddened when the `.manual` arm was made
    /// to return `.applied` — they delivered a row through an admission door on
    /// a non-`.auto` show and asserted no `.applied` decision, so the mode
    /// switch was the thing under test. After the diversion those rows never
    /// reach the switch, the repaired tests assert the diversion instead, and
    /// the mutation became invisible to the ENTIRE `PlayheadFastTests` plan
    /// (measured, R1: `RED (76 known / 4 NEW)`, all four NEW in the
    /// scheduler/grant families this box produces on a clean tree).
    ///
    /// The arm is still live production code. It USED to be reachable through
    /// `injectUserMarkedAd`, which wrote straight into `windows` and bypassed
    /// both admission doors — that was playhead-d2it, and it is closed: the
    /// live mark now takes `receiveAdWindows` like the reload does. What still
    /// reaches this arm in production is a row demoted mid-episode by the skip
    /// control (playhead-4xw4), so the probe stays, through a DEBUG-only seam
    /// that places exactly such a row: `_insertManagedWindowBypassingAdmission
    /// ForTesting`. Without it, closing the bypass would have made this arm's
    /// mutation invisible to the whole plan again. See playhead-l8c2.
    @Test(
        "A row that bypasses both admission doors is still held by the mode gate",
        arguments: [SkipMode.shadow, .manual]
    )
    func managedRowOnANonAutoShowIsNeverSkipped(mode: SkipMode) async throws {
        let (orchestrator, _, _) = try await Self.makeHarness(mode: mode)
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }

        let windowId = "wq34-modegate-\(mode.rawValue)"
        await orchestrator._insertManagedWindowBypassingAdmissionForTesting(
            start: Self.spanStart,
            end: Self.spanEnd,
            analysisAssetId: Self.assetId,
            windowId: windowId
        )

        try #require(
            await orchestrator.activeWindowIDs().contains(windowId),
            """
            setup: the seam writes into `windows` directly, so this row IS in \
            the managed tier whatever the mode — if that ever stops being true \
            the assertions below prove nothing
            """
        )
        #expect(
            pushedCues.isEmpty,
            "\(mode) is not an authorisation to cut audio, whatever door the row came through"
        )
        #expect(
            !(await orchestrator.getDecisionLog())
                .contains { $0.decision == .applied },
            "and no `.applied` decision was logged for it"
        )
    }

    // MARK: - 5d. The second door's diversion leaves the managed tier

    /// playhead-rfu-sad AT THE SECOND DOOR, which nothing proved until R1.
    ///
    /// `receiveAdDecisionResults`' diversion calls `retireManagedWindowIfPresent`
    /// before registering the suggestion. Deleting that call left all 133 tests
    /// in this bead's scope GREEN, because
    /// `decisionResultDoorAlsoDivertsToSuggest` above delivers a row the
    /// managed tier never held — there is nothing for the retire to do.
    ///
    /// The state it guards is reachable: a row admitted while its class was
    /// `.auto`, the listener turning the skip control DOWN mid-episode (which
    /// does not clear `windows` — playhead-4xw4), and then the backfill's
    /// fusion handoff arriving for the same id. A window in both maps can
    /// re-fire `acceptSuggestedSkip` and synthesize a duplicate managed window
    /// under a fresh UUID.
    ///
    /// The seek is the fixture, not the subject: it is the one branch of
    /// `evaluateWindow` that leaves a managed window at `.confirmed`, which is
    /// what keeps this row clear of the two `.applied` exclusions.
    @Test("The decision-result door's diversion empties the managed tier it took the row from")
    func decisionResultDoorDivertsOutOfTheManagedTier() async throws {
        let (orchestrator, _, _) = try await Self.makeHarness(mode: .auto)
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.recordUserSeek(to: 0)

        let row = Self.aggregatedWindow(
            id: "wq34-door2-both-tiers",
            gate: .eligible
        )
        await orchestrator.receiveAdWindows([row])
        try #require(
            await orchestrator.activeWindowIDs().contains(row.id),
            "setup: an auto-class row reaches the MANAGED tier"
        )

        // The listener turns the control down. playhead-4xw4: this does NOT
        // clear `windows`, so the row outlives the mode it was admitted under.
        await orchestrator.setActiveSkipMode(.manual)
        try #require(
            await orchestrator.activeWindowIDs().contains(row.id),
            "setup: the demotion leaves the already-admitted row managed (playhead-4xw4)"
        )

        // The backfill's fusion handoff arrives for the same id.
        await orchestrator.receiveAdDecisionResults([
            AdDecisionResult(
                id: row.id,
                analysisAssetId: row.analysisAssetId,
                startTime: row.startTime,
                endTime: row.endTime,
                skipConfidence: row.actuationConfidence,
                eligibilityGate: .eligible,
                recomputationRevision: 1
            ).withProducerRevision(row)
        ])

        #expect(
            await orchestrator.activeSuggestWindowIDs().contains(row.id),
            "the second door diverts a row its class can no longer act on"
        )
        #expect(
            !(await orchestrator.activeWindowIDs().contains(row.id)),
            """
            the row is in BOTH tiers — playhead-rfu-sad: a still-visible suggest \
            banner over a managed window can re-fire acceptSuggestedSkip and \
            synthesize a duplicate managed window under a fresh UUID
            """
        )
    }

    // MARK: - 6. The routing reads the anchors the STAMP will install

    /// The pre-stamp / post-stamp trap, which is why `admissionSkipMode` exists
    /// instead of a call to `skipMode(for:)`.
    ///
    /// `skipMode(for:)` reads the CURRENT `edgeAnchorsByWindowId` entry. The
    /// ingest stamp REPLACES that entry whenever the row is a materially
    /// changed producer revision — so for exactly the rows whose detector class
    /// has changed, the mode the router would see and the mode
    /// `evaluateWindow` will see are different values. Routing on the first and
    /// evaluating on the second is two expressions of one question
    /// (playhead-6qvf), and it puts the inversion back for the population it is
    /// hardest to notice: a window that was byte-exact and is not any more.
    ///
    /// The seek suppression is the fixture, not the subject. It is the one
    /// branch of `evaluateWindow` that leaves a managed window's state
    /// UNCHANGED, which is what keeps the first revision at `.confirmed` and
    /// therefore out of the two `.applied` exclusions.
    @Test("A revision that changes a window's detector class is re-routed on the NEW anchors")
    func revisionThatChangesDetectorClassIsRoutedOnTheNewAnchors() async throws {
        let (orchestrator, trust, _) = try await Self.makeHarness(mode: .shadow)
        var reader = LadderBannerReader(await orchestrator.bannerItemStream())
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }

        // The two classes must genuinely differ in mode, or the test proves
        // nothing about which anchors were read.
        let resolved = await trust.resolveDetectorModes(podcastId: Self.podcastId)
        try #require(resolved.mode(for: .rediffByteExact) == .auto)
        try #require(resolved.mode(for: .fusion) == .shadow)

        await orchestrator.recordUserSeek(to: 0)

        // Revision 1: byte-exact on both edges, so the class is `.auto` and the
        // row is admitted to the managed tier — which is what installs the
        // stamp this test is about.
        await orchestrator.receiveAdWindows([
            Self.byteExactWindow(id: "wq34-revision", gate: nil)
        ])
        try #require(
            await orchestrator.activeWindowIDs().contains("wq34-revision"),
            "revision 1 must reach the managed tier or there is no stale stamp"
        )
        try #require(pushedCues.isEmpty, "seek suppression held revision 1 at .confirmed")

        // Revision 2: same id, new geometry (so it is a material change) and
        // UNANCHORED edges — the row is now `.fusion`, which this show holds in
        // `.shadow`. The managed tier can do nothing with it.
        await orchestrator.receiveAdWindows([
            Self.aggregatedWindow(
                id: "wq34-revision",
                gate: .eligible,
                start: 200,
                end: 260,
                boundaryState: "lexical"
            )
        ])
        await orchestrator.updatePlayheadTime(205)

        let sentinelId = "wq34-revision-sentinel"
        await orchestrator.receiveAdWindows([
            Self.aggregatedWindow(
                id: sentinelId,
                gate: .markOnly,
                start: Self.sentinelStart,
                end: Self.sentinelStart + 4
            )
        ])
        await orchestrator.updatePlayheadTime(Self.sentinelStart)
        let received = await reader.drain(until: sentinelId)

        #expect(
            received.contains {
                $0.windowId == "wq34-revision" && $0.tier == .suggest
            },
            """
            the revision was routed on the PREVIOUS revision's anchors. Its own \
            edges are unanchored, so `evaluateWindow` will classify it .fusion \
            and return a silent .confirmed — the inversion, restored by a stale \
            stamp. Got \(received.map { "\($0.windowId)/\($0.tier)" }).
            """
        )
        #expect(
            !(await orchestrator.activeWindowIDs().contains("wq34-revision"))
        )
    }

    // MARK: - 7. A user's own mark

    /// playhead-527u's product-owner AC, revived. `AdDetectionService
    /// .recordUserMarkedAd` stamps a user's transcript mark `.eligible`
    /// precisely because it is the highest-certainty "this IS an ad" signal
    /// available — and `SkipDetectorClass.classify` tests `UserSpanAssertion`
    /// FIRST, so the row is `.userAsserted`: show-governed, `modeAuthority ==
    /// nil`, never `.auto` on the shipped default. The strongest stamp the app
    /// can write was the one the listener could not see.
    @Test("527u: a user's own marked region reaches them on reload instead of vanishing")
    func userMarkedRegionReachesTheListener() async throws {
        let (orchestrator, _, _) = try await Self.makeHarness(mode: .shadow)
        var reader = LadderBannerReader(await orchestrator.bannerItemStream())
        await orchestrator.beginEpisode(
            analysisAssetId: Self.assetId,
            episodeId: Self.episodeId,
            podcastId: Self.podcastId
        )

        let observed = await Self.listenerOutcome(
            orchestrator,
            delivering: Self.aggregatedWindow(
                id: "wq34-usermark",
                gate: .eligible,
                boundaryState: UserSpanAssertion.userMarked.rawValue
            ),
            reader: &reader
        )
        #expect(
            observed.outcome == .card,
            """
            a region the user marked themselves, reloaded, produced \(observed.outcome). \
            It is stamped .eligible and classifies as .userAsserted, which no show \
            can put in .auto — so before this bead it was silent.
            """
        )
    }
}
