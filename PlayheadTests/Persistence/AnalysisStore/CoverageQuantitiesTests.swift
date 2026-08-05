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

    /// The unsound promotions are an INVENTORY, not a claim. playhead-hc7e's
    /// *"Every consumer now reads `canonicalChunks`"* was a completeness claim
    /// that was not complete and hid a P0 for months; this one is a
    /// `CaseIterable` enum a test can check.
    ///
    /// playhead-5pyq names four of these. The fifth —
    /// `specialistScanCompletion` — was found by this bead while typing the
    /// sites, and it is the same shape over the PRE-narrowing root list.
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
        #expect(ReachRatio(examined: CoveredNaN.area, ofDeclaredDuration: 1000) == nil)
        #expect(ReachRatio(examined: -1, ofDeclaredDuration: 1000) == nil)
        // Clamped at full: an overshoot is a different problem, handled by
        // `adScanFraction`'s tolerance guard before it ever reaches here.
        #expect(ReachRatio(examined: 2000, ofDeclaredDuration: 1000) == ReachRatio(1))
    }

    @Test("the density constructor applies the same guards to its own terms")
    func densityGuards() {
        #expect(DensityRatio(transcribed: 500, ofDeclaredDuration: 1000) == DensityRatio(0.5))
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
        static let area = CoveredSeconds(.nan)
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
