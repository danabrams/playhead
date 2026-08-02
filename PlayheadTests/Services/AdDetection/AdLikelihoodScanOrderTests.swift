// AdLikelihoodScanOrderTests.swift
// playhead-lxkq: the FM coarse sweep must visit ad-likely neighbourhoods first.
//
// THE FIELD MEASUREMENT THIS FILE EXISTS FOR. Episode DE0784D8, 2026-08-01
// device pull. FM's 42 `semantic_scan_results` rows cover 0–2676 s, LINEARLY,
// front to back, ending `2581-2676 | abstain | cancelled`. Dan's missed pod is
// at 2838–2954 s. FM never reached it, having spent roughly fifteen hours of
// available wall clock sweeping the first 48% of the episode in episode order.
//
// The acoustic seam channel fired fragments at 2667–2702 and 2828–2836 on that
// very episode — immediately at the missed pod's edges. It found the seams and
// the pipeline emitted them as junk verdicts instead of using them as pointers.
//
// Every test here is a pure function call. No store, no FM, no runner.

#if DEBUG

import Foundation
import Testing
@testable import Playhead

@Suite("AdLikelihoodScanOrder (playhead-lxkq)")
struct AdLikelihoodScanOrderTests {

    // MARK: - Fixture

    /// A planned coarse window, reduced to what the ordering can see.
    private struct Window: Equatable {
        let index: Int
        let start: Double
        let end: Double
    }

    private static func span(_ window: Window) -> (start: Double, end: Double) {
        (window.start, window.end)
    }

    /// 60 s coarse windows tiling a 3,600 s episode — the shape of the
    /// DE0784D8 pull, whose 42 rows covered 2,676 s (≈64 s per window).
    private func tiledWindows(
        count: Int = 60,
        width: Double = 60
    ) -> [Window] {
        (0..<count).map { Window(index: $0, start: Double($0) * width, end: Double($0 + 1) * width) }
    }

    /// The two acoustic seam fragments the field pull recorded on DE0784D8.
    /// Strengths are plausible seam scores; the ordering never reads them as a
    /// verdict, only as relative attention.
    private var de0784d8SeamSeeds: [AdLikelihoodSeed] {
        [
            AdLikelihoodSeed(startTime: 2667, endTime: 2702, kind: .acousticSeam, strength: 0.55),
            AdLikelihoodSeed(startTime: 2828, endTime: 2836, kind: .acousticSeam, strength: 0.62),
        ]
    }

    /// Dan's missed pod.
    private let missedPod = (start: 2838.0, end: 2954.0)

    private func overlapsMissedPod(_ window: Window) -> Bool {
        window.start < missedPod.end && window.end > missedPod.start
    }

    /// Seconds of FM wall clock spent before the first window overlapping the
    /// missed pod is ATTEMPTED, at the measured 2.4x-slower-than-realtime rate.
    /// `nil` when the pod is never reached.
    private func fmWallClockSecondsBeforePod(_ order: [Window]) -> Double? {
        var audio = 0.0
        for window in order {
            if overlapsMissedPod(window) { return audio * 2.4 }
            audio += window.end - window.start
        }
        return nil
    }

    // MARK: - The acceptance

    @Test("lxkq: the 2828 seam pulls the missed pod into the first four FM windows")
    func seamPromotesTheMissedPod() {
        let windows = tiledWindows()
        let ordered = AdLikelihoodScanOrder.order(
            windows,
            seeds: de0784d8SeamSeeds,
            span: Self.span
        )
        let position = ordered.firstIndex(where: overlapsMissedPod)
        #expect(position != nil, "the pod must be reached at all")
        #expect(
            (position ?? .max) < 4,
            "expected a window overlapping 2838–2954 within the first 4 attempts, got \(position.map(String.init) ?? "never")"
        )
    }

    @Test("lxkq: reaching the missed pod costs under fifteen minutes of FM budget, not two hours")
    func seamCutsTheBudgetToThePod() {
        let windows = tiledWindows()
        let seeded = AdLikelihoodScanOrder.order(
            windows,
            seeds: de0784d8SeamSeeds,
            span: Self.span
        )
        let cost = fmWallClockSecondsBeforePod(seeded)
        #expect(cost != nil)
        #expect(
            (cost ?? .infinity) < 900,
            "expected < 15 min of FM wall clock before the pod, got \((cost ?? .infinity) / 60) min"
        )
    }

    /// The BEFORE measurement, and the vacuity control for the two tests above:
    /// on the same fixture with no seeds the pod is reached 47 windows in,
    /// which is where the field episode's budget ran out.
    @Test("lxkq control: the linear sweep reaches the missed pod only after ~113 minutes of FM budget")
    func linearSweepReachesThePodLast() {
        let windows = tiledWindows()
        let linear = AdLikelihoodScanOrder.order(windows, seeds: [], span: Self.span)
        #expect(linear.firstIndex(where: overlapsMissedPod) == 47)
        let cost = fmWallClockSecondsBeforePod(linear)
        #expect(cost != nil)
        #expect(
            (cost ?? 0) > 6_000,
            "the unseeded sweep must be the expensive one; got \((cost ?? 0) / 60) min"
        )
    }

    @Test("lxkq: nothing from the first half of the episode jumps the seeded prefix")
    func promotedPrefixIsInsideTheNeighbourhoods() {
        let windows = tiledWindows()
        let ordered = AdLikelihoodScanOrder.order(
            windows,
            seeds: de0784d8SeamSeeds,
            span: Self.span
        )
        // Seven windows intersect the two neighbourhoods ([2577, 2792] and
        // [2738, 2926]); all of them start at or after 2,520 s.
        let promoted = ordered.prefix(7)
        #expect(promoted.allSatisfy { $0.start >= 2_400 })
    }

    // MARK: - The fallback (and its vacuity control)

    @Test("lxkq: an episode with no seeds keeps the linear sweep exactly")
    func noSeedsIsTheIdentity() {
        let windows = tiledWindows()
        #expect(AdLikelihoodScanOrder.order(windows, seeds: [], span: Self.span) == windows)
    }

    /// Vacuity control for `noSeedsIsTheIdentity`: prove the identity assertion
    /// is not passing because `order` is the identity function.
    @Test("lxkq control: with seeds the order is NOT the identity")
    func withSeedsTheOrderChanges() {
        let windows = tiledWindows()
        #expect(AdLikelihoodScanOrder.order(windows, seeds: de0784d8SeamSeeds, span: Self.span) != windows)
    }

    @Test("lxkq: seeding never starves a window — the result is a permutation")
    func orderIsAPermutation() {
        let windows = tiledWindows()
        let ordered = AdLikelihoodScanOrder.order(
            windows,
            seeds: de0784d8SeamSeeds,
            span: Self.span
        )
        #expect(ordered.count == windows.count)
        #expect(ordered.map(\.index).sorted() == windows.map(\.index))
    }

    @Test("lxkq: the un-promoted remainder stays in episode order")
    func fillerKeepsEpisodeOrder() {
        let windows = tiledWindows()
        let ordered = AdLikelihoodScanOrder.order(
            windows,
            seeds: de0784d8SeamSeeds,
            span: Self.span
        )
        let promotedCount = 7
        let filler = Array(ordered.dropFirst(promotedCount)).map(\.index)
        #expect(filler == filler.sorted())
        // Non-vacuity: the filler must actually be the REMAINDER, not the whole
        // episode. Under the linear sweep `dropFirst(7)` starts at window 7 and
        // still contains every promoted index.
        #expect(filler.first == 0)
        #expect(!filler.contains(45))
    }

    @Test("lxkq: an episode whose only seed is unusable falls back to the linear sweep")
    func unusableSeedFallsBackToLinear() {
        let windows = tiledWindows()
        let episodeWide = AdLikelihoodSeed(
            startTime: 10,
            endTime: 3_000,
            kind: .evidenceAnchor,
            strength: 1.0
        )
        #expect(AdLikelihoodScanOrder.order(windows, seeds: [episodeWide], span: Self.span) == windows)
    }

    // MARK: - Ranking

    @Test("lxkq: the higher-scoring neighbourhood is attempted first")
    func higherScorePrecedesLower() {
        let windows = tiledWindows(count: 60)
        let weak = AdLikelihoodSeed(startTime: 600, endTime: 600, kind: .acousticSeam, strength: 0.2)
        let strong = AdLikelihoodSeed(startTime: 3_000, endTime: 3_000, kind: .acousticSeam, strength: 0.9)
        let ordered = AdLikelihoodScanOrder.order(
            windows,
            seeds: [weak, strong],
            span: Self.span
        )
        let firstStart = ordered.first?.start ?? -1
        #expect(firstStart > 2_800, "expected the 3,000 s neighbourhood first, got \(firstStart)")
    }

    @Test("lxkq: two seeds agreeing on one region outrank a single stronger seed elsewhere")
    func agreementOutranksASingleStrongerSeed() {
        let windows = tiledWindows()
        let agreeing = [
            AdLikelihoodSeed(startTime: 1_200, endTime: 1_200, kind: .acousticSeam, strength: 0.5),
            AdLikelihoodSeed(startTime: 1_205, endTime: 1_205, kind: .evidenceAnchor, strength: 1.0),
        ]
        let lone = AdLikelihoodSeed(startTime: 3_000, endTime: 3_000, kind: .acousticSeam, strength: 0.9)
        let ordered = AdLikelihoodScanOrder.order(
            windows,
            seeds: agreeing + [lone],
            span: Self.span
        )
        // Window 18 ([1080, 1140)) is the earliest window intersecting BOTH
        // neighbourhoods. Asserting the exact index (rather than a range) is
        // what stops the linear sweep — whose first window is 0 — from
        // satisfying this test by accident.
        #expect(ordered.first?.index == 18, "expected window 18, got \(ordered.first?.index ?? -1)")
    }

    @Test("lxkq: an acoustic seam outranks a lexical cue of equal strength")
    func acousticSeamOutranksLexicalCue() {
        #expect(
            AdLikelihoodScanOrder.weight(for: .acousticSeam)
                > AdLikelihoodScanOrder.weight(for: .lexicalCue)
        )
    }

    @Test("lxkq: equal scores break to the earlier window, deterministically")
    func tiesBreakToTheEarlierWindow() {
        let windows = tiledWindows()
        let seeds = [
            AdLikelihoodSeed(startTime: 3_000, endTime: 3_000, kind: .acousticSeam, strength: 0.5),
            AdLikelihoodSeed(startTime: 600, endTime: 600, kind: .acousticSeam, strength: 0.5),
        ]
        let ordered = AdLikelihoodScanOrder.order(windows, seeds: seeds, span: Self.span)
        // Window 8 ([480, 540)) is the earliest window in the 600 s
        // neighbourhood. The exact index matters: the linear sweep's first
        // window is 0, so a range assertion here would pass without any
        // reordering at all.
        #expect(ordered.first?.index == 8, "expected window 8, got \(ordered.first?.index ?? -1)")
    }

    // MARK: - Bounded promotion

    @Test("lxkq: the promoted prefix never exceeds the audio budget")
    func promotionIsBoundedByTheAudioBudget() {
        // A cue-dense episode: a seam every 60 s makes every window seeded.
        let windows = tiledWindows()
        let seeds = stride(from: 30.0, to: 3_600.0, by: 60.0).map {
            AdLikelihoodSeed(startTime: $0, endTime: $0, kind: .acousticSeam, strength: 0.5)
        }
        let ordered = AdLikelihoodScanOrder.order(
            windows,
            seeds: seeds,
            maxPromotedAudioSeconds: 600,
            span: Self.span
        )
        // Every interior window overlaps exactly three seams, so scores tie and
        // resolve to episode order. Ten 60 s windows exhaust the 600 s budget
        // and the eleventh is refused — leaving windows 11…59 (and window 0,
        // which overlaps only two seams and so scores lower) in episode order.
        let promotedAudio = ordered.prefix(10).reduce(0.0) { $0 + ($1.end - $1.start) }
        #expect(promotedAudio <= 600)
        #expect(ordered.prefix(10).map(\.index) == Array(1...10))
        #expect(ordered.count == windows.count)
        // The cap's OWN observable, and the only assertion here that can see it.
        // Element 10 is where the promoted prefix stops and the filler starts, so
        // it must be the filler's first element — window 0, which scored lower
        // (it overlaps two seams, not three) and so was never promoted.
        //
        // Every assertion above passes with the cap DELETED: uncapped, all sixty
        // windows are promoted in descending-score order, which for this fixture
        // is 1...58 then 0 then 59 — so `prefix(10)` is still `1...10`, the audio
        // is still 600 s, and the count is still 60. Mutation X07 proved exactly
        // that by surviving them. Uncapped, element 10 is window 11.
        #expect(
            ordered[10].index == 0,
            "the promoted prefix must STOP at the budget and hand over to the filler"
        )
    }

    @Test("lxkq: a single window wider than the whole budget is still promoted")
    func oversizeWindowIsStillPromoted() {
        // The device coarse lane really returns windows up to 1,183 s wide.
        let windows = [
            Window(index: 0, start: 0, end: 100),
            Window(index: 1, start: 100, end: 1_283),
        ]
        let seed = AdLikelihoodSeed(startTime: 700, endTime: 700, kind: .acousticSeam, strength: 0.9)
        let ordered = AdLikelihoodScanOrder.order(
            windows,
            seeds: [seed],
            maxPromotedAudioSeconds: 600,
            span: Self.span
        )
        #expect(ordered.first?.index == 1, "an oversize seeded window must not be excluded by the budget")
    }

    // MARK: - Degenerate input

    @Test("lxkq: a non-finite seed is dropped without taking the usable seeds with it")
    func nonFiniteSeedIsDroppedIndividually() {
        let windows = tiledWindows()
        let garbage = AdLikelihoodSeed(
            startTime: .nan,
            endTime: .infinity,
            kind: .acousticSeam,
            strength: 1.0
        )
        let ordered = AdLikelihoodScanOrder.order(
            windows,
            seeds: [garbage] + de0784d8SeamSeeds,
            span: Self.span
        )
        #expect(ordered.count == windows.count)
        #expect((ordered.firstIndex(where: overlapsMissedPod) ?? .max) < 4)
    }

    @Test("lxkq: a zero-strength seed promotes nothing")
    func zeroStrengthSeedPromotesNothing() {
        let windows = tiledWindows()
        let inert = AdLikelihoodSeed(startTime: 2_830, endTime: 2_830, kind: .acousticSeam, strength: 0)
        #expect(AdLikelihoodScanOrder.order(windows, seeds: [inert], span: Self.span) == windows)
    }

    @Test("lxkq: a window with a non-finite span is never promoted but is never dropped either")
    func nonFinitePlanSpanSurvivesAsFiller() {
        let windows = [
            Window(index: 0, start: 0, end: 60),
            Window(index: 1, start: .nan, end: .nan),
            Window(index: 2, start: 2_820, end: 2_880),
        ]
        let ordered = AdLikelihoodScanOrder.order(
            windows,
            seeds: de0784d8SeamSeeds,
            span: Self.span
        )
        #expect(ordered.count == 3)
        #expect(ordered.first?.index == 2)
        #expect(Set(ordered.map(\.index)) == [0, 1, 2])
    }

    @Test("lxkq: an empty plan list orders to an empty plan list")
    func emptyPlansStayEmpty() {
        #expect(AdLikelihoodScanOrder.order([Window](), seeds: de0784d8SeamSeeds, span: Self.span).isEmpty)
    }

    @Test("lxkq: strength outside [0,1] is clamped rather than trusted")
    func strengthIsClamped() {
        #expect(
            AdLikelihoodSeed(startTime: 0, endTime: 1, kind: .acousticSeam, strength: 42).strength == 1
        )
        #expect(
            AdLikelihoodSeed(startTime: 0, endTime: 1, kind: .acousticSeam, strength: -3).strength == 0
        )
        #expect(
            AdLikelihoodSeed(startTime: 0, endTime: 1, kind: .acousticSeam, strength: .nan).strength == 0
        )
    }

    // MARK: - Neighbourhoods

    @Test("lxkq: a seam opens a three-minute neighbourhood around itself")
    func seamOpensAThreeMinuteNeighbourhood() {
        let seed = AdLikelihoodSeed(startTime: 2_828, endTime: 2_828, kind: .acousticSeam, strength: 1)
        let hoods = AdLikelihoodScanOrder.neighbourhoods(from: [seed])
        #expect(hoods.count == 1)
        #expect(hoods.first?.lo == 2_738)
        #expect(hoods.first?.hi == 2_918)
    }

    @Test("lxkq: a seed wider than the width ceiling opens no neighbourhood at all")
    func episodeWideSeedOpensNoNeighbourhood() {
        let wide = AdLikelihoodSeed(
            startTime: 0,
            endTime: AdLikelihoodScanOrder.maxSeedWidthSeconds + 1,
            kind: .evidenceAnchor,
            strength: 1
        )
        #expect(AdLikelihoodScanOrder.neighbourhoods(from: [wide]).isEmpty)
        // Vacuity control: one second narrower and it IS a neighbourhood.
        let ok = AdLikelihoodSeed(
            startTime: 0,
            endTime: AdLikelihoodScanOrder.maxSeedWidthSeconds,
            kind: .evidenceAnchor,
            strength: 1
        )
        #expect(AdLikelihoodScanOrder.neighbourhoods(from: [ok]).count == 1)
    }

    // MARK: - Seed derivation

    @Test("lxkq: acoustic breaks become seam seeds carrying their break strength")
    func acousticBreaksBecomeSeamSeeds() {
        let breaks = [
            AcousticBreak(time: 2_828, breakStrength: 0.62, signals: [.energyDrop]),
            AcousticBreak(time: 2_667, breakStrength: 0.55, signals: [.pauseCluster]),
        ]
        let seeds = AdLikelihoodScanOrder.seeds(
            acousticBreaks: breaks,
            evidenceCatalog: nil,
            lexicalCandidates: []
        )
        #expect(seeds.count == 2)
        #expect(seeds.allSatisfy { $0.kind == .acousticSeam })
        #expect(seeds.first?.startTime == 2_828)
        #expect(seeds.first?.strength == 0.62)
    }

    @Test("lxkq: an evidence anchor seeds its own position, not its episode-wide coverage span")
    func evidenceAnchorSeedsItsOwnPosition() {
        // A recurring sponsor: mentioned at 120 s and again at 3,000 s, so the
        // entry's COVERAGE span is 120–3,000 — nearly the whole episode. Seeding
        // from that would name the episode, not a neighbourhood.
        let entry = EvidenceEntry(
            evidenceRef: 0,
            category: .brandSpan,
            matchedText: "BetterHelp",
            normalizedText: "betterhelp",
            atomOrdinal: 7,
            startTime: 120,
            endTime: 126,
            count: 2,
            firstTime: 120,
            lastTime: 3_000
        )
        let catalog = EvidenceCatalog(
            analysisAssetId: "asset-lxkq",
            transcriptVersion: "tx-v1",
            entries: [entry]
        )
        let seeds = AdLikelihoodScanOrder.seeds(
            acousticBreaks: [],
            evidenceCatalog: catalog,
            lexicalCandidates: []
        )
        #expect(seeds.count == 1)
        #expect(seeds.first?.startTime == 120)
        #expect(seeds.first?.endTime == 126)
        // And it therefore survives the width ceiling, unlike the coverage span.
        #expect(AdLikelihoodScanOrder.neighbourhoods(from: seeds).count == 1)
    }

    @Test("lxkq: no channels means no seeds, which means the linear sweep")
    func noChannelsMeansNoSeeds() {
        #expect(
            AdLikelihoodScanOrder.seeds(
                acousticBreaks: [],
                evidenceCatalog: nil,
                lexicalCandidates: []
            ).isEmpty
        )
    }

    // MARK: - Order restoration

    @Test("lxkq: restoring plan order sorts by the plan key")
    func restoreOrderSortsByKey() {
        let items = [(key: 4, tag: "d"), (key: 1, tag: "a"), (key: 3, tag: "c")]
        let restored = AdLikelihoodScanOrder.restoreOrder(items) { $0.key }
        #expect(restored.map(\.tag) == ["a", "c", "d"])
    }

    @Test("lxkq: restoring plan order is stable for rows sharing one plan")
    func restoreOrderIsStableWithinAPlan() {
        // A bounded permissive shrink produces two failure rows from ONE plan;
        // their relative order is the order they were discovered in.
        let items = [
            (key: 2, tag: "second-half"),
            (key: 1, tag: "other-plan"),
            (key: 2, tag: "first-half"),
        ]
        let restored = AdLikelihoodScanOrder.restoreOrder(items) { $0.key }
        #expect(restored.map(\.tag) == ["other-plan", "second-half", "first-half"])
    }
}

#endif
