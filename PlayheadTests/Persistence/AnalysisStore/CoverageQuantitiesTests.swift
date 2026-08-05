// CoverageQuantitiesTests.swift
// playhead-x0lb: the properties the types are supposed to buy, and the two
// properties the types could silently COST.
//
// The primary rail for this bead is `scripts/mutation-battery-untypeable.py`,
// which proves the substitutions do not COMPILE — a compile failure is the
// strongest kill available and no runtime test can express it. What lives here
// is everything a compile check cannot see: the on-disk wire format the cursor
// type must not change, the promotion rule's two field witnesses, the guards on
// the ratio constructors, and the measured separation between the two gap
// widths.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-x0lb — the persisted cursor's wire format is unchanged")
struct CoverageQuantityWireFormatTests {

    /// **The one thing this bead could have broken silently.**
    ///
    /// `BackfillProgressCursor.lastProcessedUpperBoundSec` became an
    /// ``EpisodeSeconds``, and the synthesised `Codable` for a
    /// `RawRepresentable` struct writes `{"rawValue": …}`. The rows on the
    /// 2026-08-03 device pull are bare numbers, so that would have orphaned
    /// every persisted cursor — the rows would decode to `nil` and every
    /// deferred job would resume from the beginning of its episode.
    ///
    /// Both fixtures are VERBATIM from `backfill_jobs.progressCursor` on the
    /// pull, including the two different key orders it actually contains
    /// (53FC53E3's is written keys-first, DE0784D8's is written value-first).
    @Test("the exact device JSON still decodes, in both key orders")
    func decodesTheDeviceRowsVerbatim() throws {
        let decoder = JSONDecoder()

        // 53FC53E3 — the completion cursor this whole bead is named after.
        let completed = try decoder.decode(
            BackfillProgressCursor.self,
            from: Data(#"{"processedUnitCount":1,"lastProcessedUpperBoundSec":2525.82}"#.utf8)
        )
        #expect(completed.processedPhaseCount == 1)
        #expect(completed.lastProcessedUpperBoundSec == EpisodeSeconds(2525.82))

        // DE0784D8 — a cancellation-salvage cursor, keys in the other order.
        let deferred = try decoder.decode(
            BackfillProgressCursor.self,
            from: Data(#"{"lastProcessedUpperBoundSec":683.58,"processedUnitCount":0}"#.utf8)
        )
        #expect(deferred.processedPhaseCount == 0)
        #expect(deferred.lastProcessedUpperBoundSec == EpisodeSeconds(683.58))

        // AD5F3A0A — an integer-valued cursor, which JSON writes without a
        // decimal point. A single-value container must still take it.
        let integral = try decoder.decode(
            BackfillProgressCursor.self,
            from: Data(#"{"processedUnitCount":1,"lastProcessedUpperBoundSec":900}"#.utf8)
        )
        #expect(integral.lastProcessedUpperBoundSec == EpisodeSeconds(900))
    }

    @Test("re-encoding writes a bare number, not a wrapper object")
    func encodesAsABareNumber() throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = .sortedKeys
        let json = String(
            decoding: try encoder.encode(
                BackfillProgressCursor(processedPhaseCount: 1, lastProcessedUpperBoundSec: 2525.82)
            ),
            as: UTF8.self
        )
        #expect(json == #"{"lastProcessedUpperBoundSec":2525.82,"processedUnitCount":1}"#)
        // Stated separately because it is the exact failure mode: a synthesised
        // `RawRepresentable` `Codable` produces this instead.
        #expect(!json.contains("rawValue"))
    }

    @Test("a missing cursor still round-trips as absent, not as zero")
    func nilCursorRoundTrips() throws {
        let encoded = try JSONEncoder().encode(
            BackfillProgressCursor(processedPhaseCount: 0, lastProcessedUpperBoundSec: nil)
        )
        let decoded = try JSONDecoder().decode(BackfillProgressCursor.self, from: encoded)
        #expect(decoded.lastProcessedUpperBoundSec == nil)
    }

    @Test("every quantity codes as a bare number")
    func everyQuantityCodesBare() throws {
        #expect(String(decoding: try JSONEncoder().encode(EpisodeSeconds(12.5)), as: UTF8.self) == "12.5")
        #expect(String(decoding: try JSONEncoder().encode(PlanListSeconds(12.5)), as: UTF8.self) == "12.5")
        #expect(String(decoding: try JSONEncoder().encode(CoveredSeconds(12.5)), as: UTF8.self) == "12.5")
        #expect(String(decoding: try JSONEncoder().encode(WatermarkSeconds(12.5)), as: UTF8.self) == "12.5")
        // playhead-x0lb R1: the three types the area/watermark split added.
        #expect(String(decoding: try JSONEncoder().encode(AnalyzedSeconds(12.5)), as: UTF8.self) == "12.5")
        #expect(String(decoding: try JSONEncoder().encode(AdScanSeconds(12.5)), as: UTF8.self) == "12.5")
        #expect(String(decoding: try JSONEncoder().encode(FrontierSeconds(12.5)), as: UTF8.self) == "12.5")
        #expect(String(decoding: try JSONEncoder().encode(ReachRatio(0.5)), as: UTF8.self) == "0.5")
        #expect(String(decoding: try JSONEncoder().encode(DensityRatio(0.5)), as: UTF8.self) == "0.5")
    }
}

@Suite("playhead-x0lb — promoting a plan-list bound to an episode cursor")
struct EpisodeSecondsPromotionTests {

    private let bridge = AnalysisCoverageMath.adScanBridgeableGapSec

    /// 53FC53E3, verbatim from the 2026-08-03 pull: the job was handed segments
    /// starting at 2,490 s and its walk reached 2,525.82 on a 2,528.4 s episode.
    /// Its measured `adScanFraction` is 0.0142 — re-derived for this bead as
    /// 35.8 s of examined area over 2,528.4 s of declared duration.
    @Test("a hole at the head refuses the promotion")
    func holeAtTheHeadRefuses() {
        #expect(EpisodeSeconds.promoting(
            2525.82, priorEpisodeCursor: nil, firstPlannedStart: 2490, bridge: bridge
        ) == nil)
        // A prior cursor does not license it either: the hole is measured from
        // the prior cursor, and 2,490 is 2,390 s above a cursor of 100.
        #expect(EpisodeSeconds.promoting(
            2525.82, priorEpisodeCursor: 100, firstPlannedStart: 2490, bridge: bridge
        ) == nil)
    }

    /// AD5F3A0A, verbatim: its first fast segment starts at 2.82 s — leading
    /// silence, not a hole — and its published cursor of 900 IS a genuine
    /// episode prefix. Re-derived: the asset's own 2,370.4 s hole sits ABOVE
    /// that cursor (the transcript does not resume until 3,270.42 s), which is
    /// why this rule is necessary and not sufficient — see playhead-a1x0.
    @Test("leading silence is not a hole, and a resume from the cursor is a prefix")
    func genuinePrefixPromotes() {
        #expect(EpisodeSeconds.promoting(
            900, priorEpisodeCursor: nil, firstPlannedStart: 2.82, bridge: bridge
        ) == EpisodeSeconds(900))
        #expect(EpisodeSeconds.promoting(
            1800, priorEpisodeCursor: 900, firstPlannedStart: 900, bridge: bridge
        ) == EpisodeSeconds(1800))
    }

    @Test("the boundary is the bridge tolerance exactly, in both directions")
    func boundaryIsTheBridgeTolerance() {
        #expect(EpisodeSeconds.promoting(
            500, priorEpisodeCursor: nil,
            firstPlannedStart: PlanListSeconds(bridge.rawValue), bridge: bridge
        ) == EpisodeSeconds(500))
        #expect(EpisodeSeconds.promoting(
            500, priorEpisodeCursor: nil,
            firstPlannedStart: PlanListSeconds(bridge.rawValue + 0.001), bridge: bridge
        ) == nil)
    }

    @Test("nothing scanned, or a non-finite bound, promotes nothing")
    func absentOrPoisonedBoundPromotesNothing() {
        #expect(EpisodeSeconds.promoting(
            nil, priorEpisodeCursor: 42, firstPlannedStart: 0, bridge: bridge
        ) == nil)
        for poison in [Double.nan, .infinity, -.infinity] {
            #expect(EpisodeSeconds.promoting(
                PlanListSeconds(poison), priorEpisodeCursor: nil, firstPlannedStart: 0, bridge: bridge
            ) == nil, "\(poison) is not a bound")
        }
        // An UNKNOWN first-plan start is not evidence of a hole — the rule
        // under-claims about the hole, not about the cursor, because a run that
        // planned nothing cannot have skipped anything either.
        #expect(EpisodeSeconds.promoting(
            500, priorEpisodeCursor: nil, firstPlannedStart: nil, bridge: bridge
        ) == EpisodeSeconds(500))
    }

    /// playhead-x0lb R2 review, closing limit L-B. `promoting` takes TWO
    /// ``PlanListSeconds`` and the compiler cannot tell them apart, so the one
    /// caller can hand them over swapped and it type-checks — R2 probe PR3
    /// planted exactly that and the tree built. Left alone the swap is silent
    /// and conservative (it promotes the list's START instead of its END), and
    /// "silent and conservative" is how six of this bead's eighteen instances
    /// survived review. The coherence guard makes it a refusal.
    ///
    /// The second pair is the one that earns the guard: on a first attempt the
    /// existing hole check happens to refuse a swap anyway, and on a RESUME it
    /// does not.
    @Test("the run's own two plan-list operands, swapped, are refused")
    func swappedOperandsAreRefused() {
        // AD5F3A0A on a first attempt: bound 900 (a genuine episode prefix),
        // first plan at 2.82 s. Right way round it promotes; swapped, both the
        // guard and the pre-existing hole check refuse it.
        #expect(EpisodeSeconds.promoting(
            900, priorEpisodeCursor: nil, firstPlannedStart: 2.82, bridge: bridge
        ) == EpisodeSeconds(900))
        #expect(EpisodeSeconds.promoting(
            2.82, priorEpisodeCursor: nil, firstPlannedStart: 900, bridge: bridge
        ) == nil)

        // THE SHAPE THE HOLE CHECK CANNOT SEE. `narrowedForResume` keeps every
        // segment ENDING past the cursor, so a resumed run's first plan
        // legitimately STARTS below it: prior 900, first plan at 898, walk
        // reaches 902. Right way round that promotes 902. Swapped, the hole
        // check is satisfied (902 − 900 = 2 s, inside the 5 s bridge) and only
        // the coherence guard refuses — without it `promoting` returns 898, a
        // cursor BELOW the prior one, and the caller's `monotonic(from:)` merge
        // then silently discards the run's whole 2 s of progress.
        #expect(EpisodeSeconds.promoting(
            902, priorEpisodeCursor: 900, firstPlannedStart: 898, bridge: bridge
        ) == EpisodeSeconds(902))
        #expect(EpisodeSeconds.promoting(
            898, priorEpisodeCursor: 900, firstPlannedStart: 902, bridge: bridge
        ) == nil)
    }

    /// The guard must not fire on anything production produces. A walk's
    /// contiguous upper bound is the END of a plan that was covered and the
    /// first planned start is the FIRST segment's start, so `bound >= start`
    /// always holds — including the degenerate equal case, which is what a
    /// single-plan run whose plan is zero-width would look like.
    @Test("the coherence guard is vacuous on well-ordered operands")
    func coherenceGuardIsVacuousWhenOrdered() {
        #expect(EpisodeSeconds.promoting(
            500, priorEpisodeCursor: nil, firstPlannedStart: 500, bridge: bridge
        ) == nil, "a 500 s hole at the head is still a hole")
        #expect(EpisodeSeconds.promoting(
            500, priorEpisodeCursor: 500, firstPlannedStart: 500, bridge: bridge
        ) == EpisodeSeconds(500), "bound == start, resumed exactly at the cursor")
        // A bound one ulp below its start is still a swap.
        #expect(EpisodeSeconds.promoting(
            PlanListSeconds(499.999), priorEpisodeCursor: 500, firstPlannedStart: 500, bridge: bridge
        ) == nil)
    }

    /// The unsound promotions are an INVENTORY, not a claim. playhead-hc7e's
    /// *"Every consumer now reads `canonicalChunks`"* was a completeness claim
    /// that was not complete and hid a P0 for months; this one is a
    /// `CaseIterable` enum a test can check.
    ///
    /// playhead-5pyq names four of these. The fifth —
    /// `specialistScanCompletion` — was found by this bead while typing the
    /// sites, and it is the same shape over the PRE-narrowing root list.
    ///
    /// **What this test proves, exactly** (R2 review). It reads `allCases`, so
    /// it is a property of the ENUM and says nothing whatever about the SOURCE
    /// — on its own it is the tautology the hc7e comparison was reaching for.
    /// Tying the enum to the sites is `--check-inventory` in
    /// `scripts/mutation-battery-untypeable.py`, and even that is lexical: R2
    /// probe PR5 wrote a promotion as `bound.map { EpisodeSeconds($0.rawValue) }`
    /// with this test still green. Limit L-F states that residue.
    @Test("the unsound-promotion inventory is exactly the five filed sites")
    func unsoundPromotionInventoryIsPinned() {
        #expect(Set(UnsoundCursorPromotionSite.allCases) == [
            .coarseCheckpoint,
            .rateLimitDefer,
            .cancellationSalvage,
            .segmentListCompletion,
            .specialistScanCompletion
        ])
        // Shrinking this set is what playhead-5pyq and playhead-a1x0 do.
        #expect(UnsoundCursorPromotionSite.allCases.count == 5)
    }

    @Test("an unsound promotion is behaviour-preserving — it is the value that was already written")
    func unsoundPromotionChangesNothing() {
        #expect(EpisodeSeconds.unsoundPlanListPromotion(
            PlanListSeconds(2525.82), site: .segmentListCompletion
        ) == EpisodeSeconds(2525.82))
        #expect(EpisodeSeconds.unsoundPlanListPromotion(
            nil, site: .coarseCheckpoint
        ) == nil)
    }
}

@Suite("playhead-x0lb — a ratio names both of its terms")
struct CoverageRatioTests {

    @Test("the reach constructor withholds a number rather than divide dishonestly")
    func reachGuards() {
        #expect(ReachRatio(examined: 500, ofDeclaredDuration: 1000) == ReachRatio(0.5))
        // Missing either term.
        #expect(ReachRatio(examined: nil, ofDeclaredDuration: 1000) == nil)
        #expect(ReachRatio(examined: 500, ofDeclaredDuration: nil) == nil)
        // A denominator that cannot divide.
        #expect(ReachRatio(examined: 500, ofDeclaredDuration: 0) == nil)
        #expect(ReachRatio(examined: 500, ofDeclaredDuration: -1) == nil)
        #expect(ReachRatio(examined: 500, ofDeclaredDuration: CoveredNaN.duration) == nil)
        // A numerator that is not a measurement.
        #expect(ReachRatio(examined: CoveredNaN.adScanArea, ofDeclaredDuration: 1000) == nil)
        #expect(ReachRatio(examined: -1, ofDeclaredDuration: 1000) == nil)
        // Clamped at full: an overshoot is a different problem, handled by
        // `adScanFraction`'s tolerance guard before it ever reaches here.
        #expect(ReachRatio(examined: 2000, ofDeclaredDuration: 1000) == ReachRatio(1))
    }

    @Test("the density constructor applies the same guards to its own terms")
    func densityGuards() {
        #expect(DensityRatio(transcribed: 500, ofDeclaredDuration: 1000) == DensityRatio(0.5))
        #expect(DensityRatio(transcribed: CoveredNaN.transcriptArea, ofDeclaredDuration: 1000) == nil)
        #expect(DensityRatio(transcribed: nil, ofDeclaredDuration: 1000) == nil)
        #expect(DensityRatio(transcribed: 500, ofDeclaredDuration: 0) == nil)
        #expect(DensityRatio(transcribed: 2000, ofDeclaredDuration: 1000) == DensityRatio(1))
    }

    /// **The substitution, priced.** 4FF3A238 on the 2026-08-03 pull, re-derived
    /// for this bead: its fast transcript bridged at the coverage reader's own
    /// 5 s tolerance covers 98.9 % of the declared 1,933.2 s, and it owns ZERO
    /// coverage-lane `semantic_scan_results` rows — so its honest ``ReachRatio``
    /// is ABSENT while a ``DensityRatio`` built from the same episode clears the
    /// 0.98 readiness floor: 1,911.0 s of bridged transcript over 1,933.176 s of
    /// declared duration is 0.9885.
    ///
    /// Reading the density as the reach therefore lights the calm ✓ on an
    /// episode nobody has read for ads. That is playhead-fil5 R3's defect, and
    /// nine of the twelve assets on the pull are in the same position (no
    /// coverage-lane row at all).
    @Test("density clears the floor on an episode whose reach is not even measurable")
    func densityIsNotReach() throws {
        let density = try #require(DensityRatio(transcribed: 1911.0, ofDeclaredDuration: 1933.176))
        #expect(abs(density.rawValue - 0.9885) < 0.0001)
        #expect(density.rawValue > episodePreparationCompleteThreshold.rawValue,
                "4FF3A238's bridged fast density clears the readiness floor")

        // The honest answer for the same asset: no coverage-lane row ⇒ no
        // fraction ⇒ still owed a scan. Every consumer agrees on that.
        let reach: ReachRatio? = nil
        #expect(SemanticScanClaim.isOwed(adScanFraction: reach))
        #expect(!episodePreparationAnalysisComplete(
            status: .done, adScanFraction: reach, isDegradedTerminal: false
        ))
        #expect(AnalysisWorkScheduler.shouldMintAdScanRedrive(
            adScanFraction: reach, resumableCoverageJobCount: 1
        ))
    }

    /// Absence and non-finiteness are ONE fact in this path, and
    /// ``Swift/Optional/finiteValue`` is where that is said once. playhead-41mu
    /// R1 is the instance: a non-finite `.measured` fraction reached `.complete`
    /// at the terminal while the other three readers of the same value read it
    /// as "not read".
    @Test("finiteValue collapses absence and non-finiteness")
    func finiteValueCollapsesAbsence() {
        let present: ReachRatio? = ReachRatio(0.5)
        #expect(present.finiteValue == ReachRatio(0.5))
        let absent: ReachRatio? = nil
        #expect(absent.finiteValue == nil)
        for poison in [Double.nan, .infinity, -.infinity] {
            let poisoned: ReachRatio? = ReachRatio(poison)
            #expect(poisoned.finiteValue == nil, "\(poison) is an absence wearing a quantity's clothes")
        }
    }

    /// Non-finite inputs written as named constants so the guard tests read as
    /// intent rather than as arithmetic.
    private enum CoveredNaN {
        static let adScanArea = AdScanSeconds(.nan)
        static let transcriptArea = CoveredSeconds(.nan)
        static let duration = EpisodeSeconds(.nan)
    }
}

@Suite("playhead-x0lb — the two gap widths answer opposite questions")
struct GapWidthTests {

    /// The empty band, re-derived for this bead against
    /// `scratchpad/db-aug3-work/analysis.sqlite`: over all 12 assets there are
    /// 668 fast-pass gaps wider than 1 s (p50 1.4 s, p75 1.9 s, p90 3.3 s,
    /// p95 4.5 s, p99 15.7 s, max 2,370.4 s), the widest breath-gap is
    /// DE0784D8's 24.3 s and the narrowest structural hole is 83592353's
    /// 150.2 s. NOTHING falls between them, and `sqrt(24 * 150) = 60.0`.
    @Test("5 s and 60 s straddle a band the field data leaves empty")
    func theTwoWidthsStraddleTheEmptyBand() {
        let bridge = AnalysisCoverageMath.adScanBridgeableGapSec
        let rescan = RescanThresholdSec.adScanRescanWorthyGapSec
        #expect(bridge == 5.0)
        #expect(rescan == 60.0)

        // DE0784D8's widest gap: too wide to bridge when MEASURING, too narrow
        // to be worth 12–45 min of FM wall-clock to RE-SCAN. Both answers are
        // "no", and they are different noes.
        #expect(!bridge.bridges(gapSec: 24.3))
        #expect(!rescan.warrantsRescan(gapSec: 24.3))

        // 83592353's structural hole: still not bridgeable, and now worth a
        // re-scan. This gap is the one that discriminates the two constants.
        #expect(!bridge.bridges(gapSec: 150.2))
        #expect(rescan.warrantsRescan(gapSec: 150.2))

        // An ordinary inter-utterance breath (the p90 of the pull): bridged,
        // and nowhere near worth a re-scan.
        #expect(bridge.bridges(gapSec: 3.3))
        #expect(!rescan.warrantsRescan(gapSec: 3.3))
    }

    /// Each predicate's own boundary, stated in the direction its question
    /// implies: bridging is inclusive at the width (a gap AT the tolerance is
    /// bridged) and re-scanning is exclusive (a gap AT the threshold is not
    /// yet worth the wall-clock).
    @Test("each predicate is pinned at its own boundary")
    func boundariesArePinned() {
        let bridge = AnalysisCoverageMath.adScanBridgeableGapSec
        let rescan = RescanThresholdSec.adScanRescanWorthyGapSec
        #expect(bridge.bridges(gapSec: 5.0))
        #expect(!bridge.bridges(gapSec: 5.0001))
        #expect(!rescan.warrantsRescan(gapSec: 60.0))
        #expect(rescan.warrantsRescan(gapSec: 60.0001))
    }

    /// The bridge must stay strictly under the shortest span any lane will call
    /// an ad, or bridging could conceal one. Restated here beside the re-scan
    /// threshold because playhead-a1x0's whole point is that the SECOND number
    /// is calibrated the other way — big enough to HOLD an ad.
    @Test("the bridge cannot conceal an ad; the re-scan threshold can hold several")
    func calibrationDirectionsAreOpposite() {
        let bridge = AnalysisCoverageMath.adScanBridgeableGapSec
        let rescan = RescanThresholdSec.adScanRescanWorthyGapSec
        let shortestAd = GlobalPriorDefaults.standard.typicalAdDuration.lowerBound
        #expect(shortestAd == 30)
        #expect(bridge.rawValue < shortestAd)
        #expect(rescan.rawValue > shortestAd)
    }
}

@Suite("playhead-x0lb R3 — the analyzed area names the bound that clips it")
struct AnalyzedSecondsClipConstructorTests {

    /// R3 review's probe PC2. `AnalysisCoverageMath.unionedSecondsClipped` is
    /// deliberately generic interval arithmetic, so its `upperBound` is a bare
    /// `Double`; the store used to hand it `frontier.rawValue`, and PC2 handed
    /// it the TRANSCRIPT's own watermark instead and the tree COMPILED.
    ///
    /// The rail that closes it is TY20 (the substitution no longer builds).
    /// What this test adds is the part a compile check cannot see: that routing
    /// the store through the named constructor did not change the arithmetic.
    /// The bound genuinely truncates, so a wrong bound is not cosmetic — it is
    /// the whole value of the AN quantity.
    @Test("clipping delegates to unionedSecondsClipped, bound and all")
    func delegatesWithoutChangingTheArithmetic() {
        let raw: [(start: Double, end: Double)] = [
            (start: 0, end: 100),
            (start: 200, end: 300)
        ]
        let transcribed = CoverageRegionFixtures.fast(raw)
        for bound in [0.0, 50.0, 100.0, 250.0, 1_000.0] {
            #expect(
                AnalyzedSeconds(clipping: transcribed, to: FrontierSeconds(bound)).rawValue
                    == AnalysisCoverageMath.unionedSecondsClipped(raw, upperBound: bound)
            )
        }
    }

    /// The bound is load-bearing in the direction that matters: clipping the
    /// same union to the transcript's own reach returns the whole union, which
    /// is exactly the "AN ≡ TX on every episode" reading PC2 would have shipped
    /// — the one shape of this family a reader cannot spot, because the two
    /// bars simply agree.
    @Test("a frontier below the union truncates; the transcript's own reach does not")
    func theBoundIsWhatMakesTheQuantityDifferentFromTX() {
        let transcribed = CoverageRegionFixtures.fast([
            (start: 0, end: 100),
            (start: 200, end: 300)
        ])
        let transcriptArea = transcribed.unionedSeconds.rawValue
        #expect(transcriptArea == 200)

        let atFrontier = AnalyzedSeconds(clipping: transcribed, to: FrontierSeconds(250))
        #expect(atFrontier.rawValue == 150)

        // The substitution PC2 wrote: clip to the transcript's own watermark.
        let atTranscriptReach = AnalyzedSeconds(clipping: transcribed, to: FrontierSeconds(300))
        #expect(atTranscriptReach.rawValue == transcriptArea)
    }
}

@Suite("playhead-x0lb R4 — the duration guards name which quantity may disprove a duration")
struct DurationContradictionPredicateTests {

    /// R4 probes PA2 and PA3. Both guards in `adScanFraction` were raw
    /// comparisons, and once both sides are `Double` they accept every quantity
    /// on the summary: PA2 drove the area-vs-duration guard from the TRANSCRIPT
    /// area and PA3 drove the reach-past-duration guard from the AD-SCAN area,
    /// and both COMPILED. Rails TY22/TY23 close the writing; this pins that
    /// naming the receiver did not change the arithmetic.
    @Test("both predicates are the comparison they replaced, at the boundary in both directions")
    func predicatesAreTheOldComparison() {
        let duration = EpisodeSeconds(100)
        for tolerance in [0.0, 5.0] {
            for value in [0.0, 99.0, 100.0, 100.0 + tolerance, 100.0 + tolerance + 0.5, 250.0] {
                #expect(
                    AdScanSeconds(value).exceeds(duration, byMoreThan: tolerance)
                        == (value > 100.0 + tolerance)
                )
                #expect(
                    WatermarkSeconds(value).reaches(past: duration, byMoreThan: tolerance)
                        == (value > 100.0 + tolerance)
                )
            }
        }
    }

    /// The asymmetry is the point, and it is why these are two predicates
    /// rather than one shared one. An AREA is legitimately small on a gappy
    /// transcript, so it can never disprove a duration by being small — only by
    /// exceeding it. A REACH past the declared end is the only evidence the
    /// denominator describes different audio. Same arithmetic, opposite
    /// admissible receivers, which is exactly what the compiler now enforces:
    /// `CoveredSeconds` has no `exceeds` and `AdScanSeconds` has no `reaches`.
    @Test("a gappy transcript's area never trips the guard its reach does")
    func areaAndReachDisagreeOnTheSameTranscript() {
        // AD5F3A0A's shape on the 2026-08-03 pull: the watermark over-reports
        // the area by 2.6x, so on a duration between them exactly one of the
        // two readings contradicts the declared duration.
        let duration = EpisodeSeconds(2_000)
        let area = AdScanSeconds(1_645.92)
        let reach = WatermarkSeconds(4_280.70)
        #expect(area.exceeds(duration, byMoreThan: 30) == false)
        #expect(reach.reaches(past: duration, byMoreThan: 30) == true)
    }
}

@Suite("playhead-x0lb R4 — the AN ratio is an operation, not a pair")
struct AnalyzedFractionTests {

    /// R4 probes PB1 and PB2. R3 typed both TERMS of the two
    /// `fraction(area:ofDeclaredDuration:)` helpers, which stops the wrong
    /// quantity arriving; inside the helper both were `Double` again and the
    /// reciprocal compiled at both copies. Rails TY24/TY25 close it. This pins
    /// that the delegation is behaviour-identical to the expression it replaced
    /// — the guard order, the clamp and the nil semantics.
    @Test("the fraction is the clamped quotient, and absence is preserved exactly")
    func matchesTheExpressionItReplaced() {
        let area = AnalyzedSeconds(150)
        #expect(area.fractionOfDeclaredDuration(EpisodeSeconds(300)) == 0.5)
        // The old spelling collapsed an absent duration with `?? 0`, which then
        // failed `> 0`; `nil` fails in exactly the same place.
        #expect(area.fractionOfDeclaredDuration(nil) == nil)
        #expect(area.fractionOfDeclaredDuration(EpisodeSeconds(0)) == nil)
        #expect(area.fractionOfDeclaredDuration(EpisodeSeconds(-10)) == nil)
        // Clamped into [0, 1] at both ends, as both call sites did.
        #expect(area.fractionOfDeclaredDuration(EpisodeSeconds(100)) == 1.0)
        #expect(AnalyzedSeconds(-5).fractionOfDeclaredDuration(EpisodeSeconds(100)) == 0.0)
    }

    /// WHY AN INVERTED RATIO IS NOT A COSMETIC DEFECT. It renders in the same
    /// `[0, 1]` bar, and on every episode whose analyzed area is under its
    /// declared duration — which is every episode that is not finished — the
    /// reciprocal exceeds 1 and clamps to a FULL bar. The wrong answer is not
    /// noisy; it is the most reassuring answer the surface can give.
    @Test("the reciprocal clamps to a full bar on exactly the episodes that are not done")
    func theReciprocalIsIndistinguishableFromComplete() {
        let duration = EpisodeSeconds(3_600)
        for analyzed in [1.0, 60.0, 1_800.0, 3_599.0] {
            let area = AnalyzedSeconds(analyzed)
            let honest = area.fractionOfDeclaredDuration(duration)
            #expect(honest != nil && honest! < 1.0)
            // What PB1/PB2 would have shipped, spelled by hand because the
            // types no longer admit it.
            let inverted = min(1.0, max(0.0, duration.rawValue / area.rawValue))
            #expect(inverted == 1.0)
        }
    }
}

/// playhead-x0lb R5: the region types keep their intervals `fileprivate`, so a
/// test builds one the way the coverage reader does — one row at a time, off the
/// SQL columns. That is deliberate: an `init(intervals:)` visible to the whole
/// module would be a second door into every region and would put back exactly
/// what probes PA7/PA8/PA9 walked through.
enum CoverageRegionFixtures {

    static func fast(_ intervals: [(start: Double, end: Double)]) -> FastTranscriptRegion {
        var region = FastTranscriptRegion()
        for interval in intervals { region.append(start: interval.start, end: interval.end) }
        return region
    }

    static func final(_ intervals: [(start: Double, end: Double)]) -> FinalTranscriptRegion {
        var region = FinalTranscriptRegion()
        for interval in intervals { region.append(start: interval.start, end: interval.end) }
        return region
    }

    static func scanned(_ intervals: [(start: Double, end: Double)]) -> ScannedRegion {
        var region = ScannedRegion()
        for interval in intervals { region.append(start: interval.start, end: interval.end) }
        return region
    }

    /// R5 review: the readable region built row-at-a-time, which is what
    /// ``AnalysisStore/fetchTranscribedRegion(assetId:)`` does — its query reads
    /// both passes at once and has no `pass` column to compose on.
    static func transcribed(_ intervals: [(start: Double, end: Double)]) -> TranscribedRegion {
        var region = TranscribedRegion()
        for interval in intervals { region.append(start: interval.start, end: interval.end) }
        return region
    }
}

@Suite("playhead-x0lb R5 — the interval REGIONS, one layer below every other rail")
struct CoverageRegionTests {

    /// Rails TY26–TY29 prove the four substitutions no longer COMPILE. What a
    /// compile check cannot see is that routing the store through the region
    /// types did not change the arithmetic — so each of these pins the new
    /// named operation against the generic helper it delegates to.
    @Test("the fast region's area is the interval union, unchanged")
    func fastRegionAreaIsTheUnion() {
        let raw: [(start: Double, end: Double)] = [
            (start: 0, end: 100),
            (start: 50, end: 120),
            (start: 300, end: 310)
        ]
        #expect(
            CoverageRegionFixtures.fast(raw).unionedSeconds.rawValue
                == AnalysisCoverageMath.unionedSeconds(raw)
        )
        #expect(CoverageRegionFixtures.fast(raw).unionedSeconds.rawValue == 130)
        #expect(FastTranscriptRegion().unionedSeconds.rawValue == 0)
    }

    /// The watermark stand-in. Probe PA5 built this span from the DSP frontier —
    /// which reaches the end of an episode nothing transcribed — and it compiled
    /// while the region was a bare interval array. Rail TY26 closes the writing;
    /// this pins that the span is still `[0, area]` and nothing else.
    @Test("the watermark stand-in is one contiguous span from zero")
    func watermarkStandInSpansFromZero() {
        let region = FastTranscriptRegion(spanningFromZeroTo: CoveredSeconds(842.5))
        #expect(region.unionedSeconds.rawValue == 842.5)
        #expect(AnalyzedSeconds(clipping: region, to: FrontierSeconds(400)).rawValue == 400)
        // A degenerate stand-in contributes nothing rather than a negative span.
        #expect(FastTranscriptRegion(spanningFromZeroTo: CoveredSeconds(0)).unionedSeconds.rawValue == 0)
    }

    /// playhead-9y9e's own property, now carried by a type: the readable region
    /// is a SUPERSET of the fast pass in every branch, so the measured ad-scan
    /// area can only ever move UP and no episode becomes less ready.
    @Test("the transcribed region is fast ∪ final, and never smaller than fast alone")
    func transcribedRegionIsMonotoneOverTheFastPass() {
        // 0C2FC22E's shape on the 2026-08-03 pull, re-derived for R5: the two
        // passes are DISJOINT — `final` holds 940 chunks spanning [0.0, 930.0]
        // and `fast` holds 1,272 spanning [930.0, 2085.7]. Collapsed to one span
        // each here, because what is under test is which REGION reaches the
        // bound, not the interval arithmetic (that is AnalysisCoverageMath's own
        // ~40 tests).
        let fast = CoverageRegionFixtures.fast([(start: 930, end: 2_085.7)])
        let final = CoverageRegionFixtures.final([(start: 0, end: 930)])
        let scan = CoverageRegionFixtures.scanned([(start: 0, end: 2_085.7)])

        let fastOnly = AdScanSeconds(
            examined: scan,
            within: TranscribedRegion(fastPass: fast, finalPass: FinalTranscriptRegion()),
            bridging: AnalysisCoverageMath.adScanBridgeableGapSec
        )
        let bothPasses = AdScanSeconds(
            examined: scan,
            within: TranscribedRegion(fastPass: fast, finalPass: final),
            bridging: AnalysisCoverageMath.adScanBridgeableGapSec
        )
        // Written as the difference, not as `1_155.7`: the union returns
        // `end - start` and `2085.7 - 930` is not the `Double` nearest 1155.7.
        // A literal here would be asserting the decimal, not the arithmetic.
        #expect(fastOnly.rawValue == 2_085.7 - 930)
        #expect(bothPasses.rawValue == 2_085.7)
        #expect(bothPasses > fastOnly)
        // The whole point of the widening: the fast pass alone reaches 55.4 % of
        // this episode's readable audio and both passes reach all of it.
        #expect(fastOnly.rawValue / bothPasses.rawValue < 0.555)
        #expect(fastOnly.rawValue / bothPasses.rawValue > 0.554)
    }

    /// The ad-scan area delegates to the same intersection the store used to
    /// spell inline, bridging included. The bridging is INSIDE the constructor
    /// precisely so there is no unbridged bound in scope to pass — an unbridged
    /// bound is not a different type, it is the same type with a step missed.
    @Test("the ad-scan area is the intersection of the windows with the BRIDGED region")
    func adScanAreaIsTheBridgedIntersection() {
        // A window whose bounds straddle a transcript hole: the raw span reads
        // [0, 3600] while only the two ends carry text.
        let fastRaw: [(start: Double, end: Double)] = [
            (start: 0, end: 60),
            (start: 3_540, end: 3_600)
        ]
        let scanRaw: [(start: Double, end: Double)] = [(start: 0, end: 3_600)]
        let area = AdScanSeconds(
            examined: CoverageRegionFixtures.scanned(scanRaw),
            within: TranscribedRegion(
                fastPass: CoverageRegionFixtures.fast(fastRaw),
                finalPass: FinalTranscriptRegion()
            ),
            bridging: AnalysisCoverageMath.adScanBridgeableGapSec
        )
        #expect(
            area.rawValue == AnalysisCoverageMath.unionedSecondsIntersecting(
                scanRaw,
                within: AnalysisCoverageMath.bridgingShortGaps(
                    fastRaw,
                    upTo: AnalysisCoverageMath.adScanBridgeableGapSec
                )
            )
        )
        // 120 s of text, not the 3,600 s the window's bounds claim.
        #expect(area.rawValue == 120)
    }

    /// The bridge is load-bearing in both directions, which is what makes the
    /// tolerance a quantity worth its own type (rail TY07): sub-ad-width holes
    /// close, a genuine untranscribed BLOCK does not.
    @Test("bridging closes inter-utterance holes and leaves a real block open")
    func bridgingClosesBreathsAndNotBlocks() {
        let scan = CoverageRegionFixtures.scanned([(start: 0, end: 200)])
        let breathy = TranscribedRegion(
            fastPass: CoverageRegionFixtures.fast([
                (start: 0, end: 99),
                (start: 100, end: 200)
            ]),
            finalPass: FinalTranscriptRegion()
        )
        let blocked = TranscribedRegion(
            fastPass: CoverageRegionFixtures.fast([
                (start: 0, end: 50),
                (start: 150, end: 200)
            ]),
            finalPass: FinalTranscriptRegion()
        )
        let bridge = AnalysisCoverageMath.adScanBridgeableGapSec
        #expect(AdScanSeconds(examined: scan, within: breathy, bridging: bridge).rawValue == 200)
        #expect(AdScanSeconds(examined: scan, within: blocked, bridging: bridge).rawValue == 100)
    }

    /// R5 review — the readable region is now published across the store's API
    /// (``AnalysisStore/fetchTranscribedRegion(assetId:)``, rails TY32–TY34), and
    /// the gate that consumes it must measure the SAME region the ad-scan area is
    /// intersected against. That commensurability is the whole argument in
    /// ``SemanticScanClaim/bridgedTranscriptCoveredSec(region:)``'s doc, and it
    /// was previously only an argument: both sides took a bare interval array.
    @Test("the readable region's bridged area IS the ad-scan bound")
    func bridgedSecondsIsTheAdScanBound() {
        // A breath-width hole at 60→63 and a real one at 120→600.
        let raw: [(start: Double, end: Double)] = [
            (start: 0, end: 60),
            (start: 63, end: 120),
            (start: 600, end: 700)
        ]
        let region = CoverageRegionFixtures.transcribed(raw)
        let bridge = AnalysisCoverageMath.adScanBridgeableGapSec

        #expect(region.unionedSeconds == 217)
        #expect(region.intervalCount == 3)
        #expect(region.isEmpty == false)
        #expect(TranscribedRegion().isEmpty)

        // Bridged: [0,120] ∪ [600,700]. The breath closes, the block does not.
        #expect(region.bridgedSeconds(bridging: bridge) == 220)
        #expect(
            region.bridgedSeconds(bridging: bridge)
                == AnalysisCoverageMath.unionedSeconds(
                    AnalysisCoverageMath.bridgingShortGaps(raw, upTo: bridge)
                )
        )
        // A scan that read everything measures exactly the bound and no more —
        // which is what makes the sweep's floor and the readiness fraction
        // comparable rather than two numbers over different denominators.
        #expect(
            AdScanSeconds(
                examined: CoverageRegionFixtures.scanned([(start: 0, end: 700)]),
                within: region,
                bridging: bridge
            ).rawValue == region.bridgedSeconds(bridging: bridge)
        )
    }

    /// No transcript evidence means nothing can have been read, however wide the
    /// persisted window bounds are. This is the direction that matters: a window
    /// taken at face value would report a fully screened episode.
    @Test("an empty readable region measures zero regardless of the windows")
    func nothingReadableMeasuresZero() {
        let area = AdScanSeconds(
            examined: CoverageRegionFixtures.scanned([(start: 0, end: 3_600)]),
            within: TranscribedRegion(
                fastPass: FastTranscriptRegion(),
                finalPass: FinalTranscriptRegion()
            ),
            bridging: AnalysisCoverageMath.adScanBridgeableGapSec
        )
        #expect(area.rawValue == 0)
    }
}
