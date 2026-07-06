// SpliceSlotResolverTests.swift
// playhead-xsdz.19: unit tests for the pure splice-slot-resolution engine.
//
// CALIBRATED FIXTURES
// -------------------
// The resolver scores each candidate edge with `AudioForensicsBoundaryDetector`'s
// σ-normalized step math. To exercise the champion-scan and qualification gates
// at PRECISE edge-score points, each fixture is a flat-feature episode — constant
// `rms` and `musicProbability` (so `loudnessSigma == musicProbSigma == 0` and
// only the spectral sub-signal can fire) — with a 2-window `spectralFlux` pulse
// placed at each candidate time. The pulse magnitudes (`delta` below) were
// calibrated offline against the exact detector math so each candidate edge
// scores the documented target; every test that depends on a score RE-ASSERTS the
// achieved value, so any drift in the detector math fails loudly here rather than
// silently changing which pair wins.
//
// With this construction a candidate edge's stepScore is
// `normalizedStep(delta, fluxSigma) / 2`, i.e. in `[0, 0.5]`. Two far-left anchor
// windows fix `fluxSigma` so the calibration is well-conditioned; they are never
// candidate times.

import Foundation
import Testing

@testable import Playhead

@Suite("SpliceSlotResolver (playhead-xsdz.19)")
struct SpliceSlotResolverTests {

    // MARK: - Fixture builder

    private enum Fixture {
        static let base = 0.05
        /// Far-left σ-anchor windows (times 8s / 12s); never candidate times.
        static let anchors: [(index: Int, flux: Double)] = [(4, 0.7), (6, 0.7)]

        /// Build a flat-feature episode with a 2-window spectral-flux pulse at
        /// each `(time, delta)`. rms / musicProbability are constant so only the
        /// spectral sub-signal fires.
        ///
        /// SPACING REQUIREMENT: a candidate at time `t` pulses windows
        /// `{t/2, t/2+1}`; the detector scores that edge from the medians of the
        /// two windows just OUTSIDE (`{t/2-2, t/2-1}`) and just INSIDE
        /// (`{t/2, t/2+1}`). So candidate times must be ≥ 4 windows (8s) apart —
        /// and clear of the anchor windows (4, 6) — or one candidate's pulse
        /// leaks into another's outside-median and corrupts the calibrated score.
        static func episode(
            count: Int,
            steps: [(time: Double, delta: Double)]
        ) -> [FeatureWindow] {
            var flux = [Double](repeating: base, count: count)
            for anchor in anchors { flux[anchor.index] = anchor.flux }
            for step in steps {
                let k = Int((step.time / 2.0).rounded())
                flux[k] = base + step.delta
                flux[k + 1] = base + step.delta
            }
            return (0..<count).map { i in
                AcousticFeatureFixtures.window(
                    startTime: Double(i) * 2.0,
                    endTime: Double(i + 1) * 2.0,
                    rms: 0.2,                // constant ⇒ loudnessSigma == 0
                    spectralFlux: flux[i],   // the only varying feature
                    musicProbability: 0.02   // constant ⇒ musicProbSigma == 0
                )
            }
        }

        static func breaks(_ times: [Double]) -> [AcousticBreak] {
            times.map { AcousticBreak(time: $0, breakStrength: 1.0, signals: [.energyDrop]) }
        }
    }

    // Calibrated pulse magnitudes -> documented target edge scores.
    // (base 0.05, anchors idx4/6 = 0.7; see file header.)
    private enum Delta {
        static let s030_c140 = 0.1809070216   // count 140, 2 candidates -> 0.30
        static let s010_c140 = 0.1105003219   // count 140, 2 candidates -> 0.10
        static let s030_c220 = 0.1414287634   // count 220, 2 candidates -> 0.30
        static let tie030 = 0.1871608785       // count 140, 3 candidates -> 0.30
        // 3-pair cycle (count 140, 4 candidates):
        static let p3_020 = 0.1629485845       // -> 0.20
        static let p3_028 = 0.1919172217       // -> 0.28
        static let p3_040 = 0.2353701776       // -> 0.40
        static let p3_045 = 0.2534755759       // -> 0.45
        // 2-pair 1.4x (count 140, 3 candidates):
        static let p14_020 = 0.1545049619      // -> 0.20
        static let p14_028 = 0.1819725107      // -> 0.28
        static let p14_045 = 0.2403410518      // -> 0.45
        // 2-pair 1.6x (count 140, 3 candidates):
        static let p16_020 = 0.1553966591      // -> 0.20
        static let p16_032 = 0.1968357682      // -> 0.32
        static let p16_045 = 0.2417281363      // -> 0.45
        // disqualified-champion-shadows (count 140, 3 candidates):
        static let sh_013 = 0.1278495663       // -> 0.13 (below 0.15 floor)
        static let sh_017 = 0.1413074154       // -> 0.17
        static let sh_045 = 0.2355123589       // -> 0.45
        // escalating-bar 3-pair (count 140, 4 candidates):
        static let esc_020 = 0.16398527        // -> 0.20
        static let esc_032 = 0.2077146754      // -> 0.32
        static let esc_040 = 0.2368676123      // -> 0.40
        static let esc_045 = 0.2550881978      // -> 0.45
    }

    private let resolver = SpliceSlotResolver()

    private func approx(_ a: Double, _ b: Double, tol: Double = 0.005) -> Bool {
        abs(a - b) < tol
    }

    // MARK: - Qualifying DAI-like fixture

    @Test("Qualifying splice pair -> exact slot AND bestGeometryValidPair == slot")
    func qualifyingDAISlot() throws {
        let windows = Fixture.episode(count: 140, steps: [
            (time: 90, delta: Delta.s030_c140),
            (time: 150, delta: Delta.s030_c140),
        ])
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 100, end: 140),
            vetoedRanges: [],
            breaks: Fixture.breaks([90, 150]),
            episodeWindows: windows
        )
        let s = try #require(slot)
        #expect(s.startTime == 90)
        #expect(s.endTime == 150)
        #expect(approx(s.coreCoverage, 1.0))
        #expect(approx(s.startEdge.stepScore, 0.30))
        #expect(approx(s.endEdge.stepScore, 0.30))
        #expect(approx(s.slotConfidence, 0.30))
        // Only the spectral sub-signal fires on these flat-feature fixtures.
        #expect(s.startEdge.contributingSignals == 1)
        #expect(s.endEdge.contributingSignals == 1)
        #expect(diag.failureReason == nil)
        // On a qualified outcome the champion equals the returned slot.
        #expect(diag.bestGeometryValidPair == s)
    }

    // MARK: - No candidate pairs

    @Test("No breaks -> (nil, .noCandidatePairs, nil champion)")
    func noBreaksNoCandidatePairs() {
        let windows = Fixture.episode(count: 140, steps: [(time: 90, delta: Delta.s030_c140)])
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 100, end: 140),
            vetoedRanges: [],
            breaks: [],
            episodeWindows: windows
        )
        #expect(slot == nil)
        #expect(diag.failureReason == .noCandidatePairs)
        #expect(diag.bestGeometryValidPair == nil)
    }

    @Test("Touching-only pair (shares only an endpoint with core) excluded -> .noCandidatePairs")
    func touchingOnlyPairExcluded() {
        // Narrow core [100,108] (width 8 == inwardTolerance). End range starts at
        // core.end - 8 = 100, so a break at 100 is a valid END candidate sitting
        // exactly on core.start; paired with a start break at 90 the slot [90,100]
        // shares ONLY the endpoint 100 with the core (zero-duration overlap).
        let windows = Fixture.episode(count: 140, steps: [
            (time: 90, delta: Delta.s030_c140),
            (time: 100, delta: Delta.s030_c140),
        ])
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 100, end: 108),
            vetoedRanges: [],
            breaks: Fixture.breaks([90, 100]),
            episodeWindows: windows
        )
        #expect(slot == nil)
        #expect(diag.failureReason == .noCandidatePairs)
        #expect(diag.bestGeometryValidPair == nil)
    }

    // MARK: - Duration diagnostic

    @Test("Only pair too long -> (nil, .durationOutOfRange, nil champion)")
    func onlyPairTooLongDurationOutOfRange() {
        // core [200,240]; start break 90 (within search), end break 290 -> slot
        // [90,290] duration 200 > 180, positive overlap. The ONLY pair fails only
        // the duration bound.
        let windows = Fixture.episode(count: 200, steps: [
            (time: 90, delta: Delta.s030_c140),
            (time: 290, delta: Delta.s030_c140),
        ])
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 200, end: 240),
            vetoedRanges: [],
            breaks: Fixture.breaks([90, 290]),
            episodeWindows: windows
        )
        #expect(slot == nil)
        #expect(diag.failureReason == .durationOutOfRange)
        #expect(diag.bestGeometryValidPair == nil)
    }

    // MARK: - Edge floor

    @Test("Edge below floor -> (nil, .edgeBelowFloor) with champion populated")
    func edgeBelowFloor() throws {
        let windows = Fixture.episode(count: 140, steps: [
            (time: 90, delta: Delta.s010_c140),
            (time: 150, delta: Delta.s010_c140),
        ])
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 100, end: 140),
            vetoedRanges: [],
            breaks: Fixture.breaks([90, 150]),
            episodeWindows: windows
        )
        #expect(slot == nil)
        #expect(diag.failureReason == .edgeBelowFloor)
        let champ = try #require(diag.bestGeometryValidPair)
        #expect(champ.startTime == 90)
        #expect(champ.endTime == 150)
        #expect(approx(champ.slotConfidence, 0.10))
    }

    @Test("Disqualified-champion-shadows: near-core sub-floor champion is NOT rescued by a wider strong pair")
    func disqualifiedChampionShadows() throws {
        // Near-core champion (start 100) scores 0.13 (< 0.15 floor). Wider TRUE
        // pair (start 90) scores 0.17 (>= floor) but 0.17 < 1.5 * 0.13 = 0.195, so
        // it does NOT replace the champion in the scan. Result: the disqualified
        // near-core champion stands -> .edgeBelowFloor, the true pair is shadowed.
        let windows = Fixture.episode(count: 140, steps: [
            (time: 100, delta: Delta.sh_013),
            (time: 90, delta: Delta.sh_017),
            (time: 142, delta: Delta.sh_045),
        ])
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 100, end: 140),
            vetoedRanges: [],
            breaks: Fixture.breaks([100, 90, 142]),
            episodeWindows: windows
        )
        #expect(slot == nil)
        #expect(diag.failureReason == .edgeBelowFloor)
        let champ = try #require(diag.bestGeometryValidPair)
        #expect(champ.startTime == 100, "The near-core (narrowest) pair is the champion, not the wider true pair")
        #expect(approx(champ.slotConfidence, 0.13))
    }

    @Test("slotConfidenceFloor is a live gate when configured above spliceEdgeFloor")
    func slotConfidenceBelowFloorReachable() throws {
        // Both edges clear spliceEdgeFloor (0.15) but the slot confidence (0.20)
        // is below a stricter slotConfidenceFloor (0.25) -> .slotConfidenceBelowFloor.
        let strict = SpliceSlotResolver(configuration: SpliceSlotResolver.Configuration(
            spliceEdgeFloor: 0.15,
            slotConfidenceFloor: 0.25
        ))
        let windows = Fixture.episode(count: 140, steps: [
            (time: 100, delta: Delta.p14_020),  // 0.20
            (time: 90, delta: Delta.p14_028),   // 0.28 (unused as champion)
            (time: 142, delta: Delta.p14_045),  // 0.45
        ])
        // Champion is the narrowest pair (start 100) with confidence 0.20.
        let (slot, diag) = strict.resolveWithDiagnostics(
            core: TimeRange(start: 100, end: 140),
            vetoedRanges: [],
            breaks: Fixture.breaks([100, 90, 142]),
            episodeWindows: windows
        )
        #expect(slot == nil)
        #expect(diag.failureReason == .slotConfidenceBelowFloor)
        let champ = try #require(diag.bestGeometryValidPair)
        #expect(champ.startTime == 100)
    }

    // MARK: - Coverage

    @Test("coreCoverage < minimum -> .coreCoverageBelowMinimum")
    func coreCoverageBelowMinimum() throws {
        // Both edges at their INWARD tolerance (start 108 = core.start+8, end 132
        // = core.end-8) -> slot [108,132] covers only 24/40 = 0.6 of the core.
        let windows = Fixture.episode(count: 140, steps: [
            (time: 108, delta: Delta.s030_c140),
            (time: 132, delta: Delta.s030_c140),
        ])
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 100, end: 140),
            vetoedRanges: [],
            breaks: Fixture.breaks([108, 132]),
            episodeWindows: windows
        )
        #expect(slot == nil)
        #expect(diag.failureReason == .coreCoverageBelowMinimum)
        let champ = try #require(diag.bestGeometryValidPair)
        #expect(approx(champ.coreCoverage, 0.6))
    }

    // MARK: - Veto

    @Test("Veto newly enclosed by slot (not intersecting core) -> .vetoNewlyEnclosed")
    func vetoNewlyEnclosed() {
        let windows = Fixture.episode(count: 140, steps: [
            (time: 90, delta: Delta.s030_c140),
            (time: 150, delta: Delta.s030_c140),
        ])
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 100, end: 140),
            vetoedRanges: [TimeRange(start: 92, end: 96)], // inside slot [90,150], outside core
            breaks: Fixture.breaks([90, 150]),
            episodeWindows: windows
        )
        #expect(slot == nil)
        #expect(diag.failureReason == .vetoNewlyEnclosed)
    }

    @Test("Veto already intersecting the core is allowed -> qualifies")
    func vetoAlreadyIntersectingCoreAllowed() throws {
        let windows = Fixture.episode(count: 140, steps: [
            (time: 90, delta: Delta.s030_c140),
            (time: 150, delta: Delta.s030_c140),
        ])
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 100, end: 140),
            vetoedRanges: [TimeRange(start: 110, end: 120)], // intersects the core
            breaks: Fixture.breaks([90, 150]),
            episodeWindows: windows
        )
        let s = try #require(slot)
        #expect(s.startTime == 90)
        #expect(diag.failureReason == nil)
    }

    // MARK: - Champion scan

    @Test("Champion scan: 2-pair 1.4x LOSES (narrow champion stands)")
    func championScan2Pair14Loses() throws {
        let windows = Fixture.episode(count: 140, steps: [
            (time: 100, delta: Delta.p14_020), // narrow champion, conf 0.20
            (time: 90, delta: Delta.p14_028),  // wide, conf 0.28 = 1.4x (< 1.5x)
            (time: 142, delta: Delta.p14_045),
        ])
        let slot = try #require(resolver.resolve(
            core: TimeRange(start: 100, end: 140),
            vetoedRanges: [],
            breaks: Fixture.breaks([100, 90, 142]),
            episodeWindows: windows
        ))
        #expect(slot.startTime == 100, "1.4x is below the 1.5x replacement bar: the narrow pair wins")
        #expect(approx(slot.slotConfidence, 0.20))
    }

    @Test("Champion scan: 2-pair 1.6x WINS (wide pair replaces champion)")
    func championScan2Pair16Wins() throws {
        let windows = Fixture.episode(count: 140, steps: [
            (time: 100, delta: Delta.p16_020), // narrow, conf 0.20
            (time: 90, delta: Delta.p16_032),  // wide, conf 0.32 = 1.6x (>= 1.5x)
            (time: 142, delta: Delta.p16_045),
        ])
        let slot = try #require(resolver.resolve(
            core: TimeRange(start: 100, end: 140),
            vetoedRanges: [],
            breaks: Fixture.breaks([100, 90, 142]),
            episodeWindows: windows
        ))
        #expect(slot.startTime == 90, "1.6x clears the 1.5x replacement bar: the wider pair wins")
        #expect(approx(slot.slotConfidence, 0.32))
    }

    @Test("Champion scan: 3-pair non-transitive cycle resolves deterministically to the widest via T->W")
    func championScan3PairCycle() throws {
        // T (start 100, nonCore 2, conf 0.20), M (start 92, nonCore 10, conf 0.28
        // = 1.4x T), W (start 82, nonCore 20, conf 0.40 = 2.0x T). Pairwise "wider
        // beats tighter at >=1.5x" is non-transitive (M !> T, W !> M, W > T); the
        // fixed-champion scan gives W: champion T, M fails (0.28 < 0.30), W
        // replaces (0.40 >= 0.30).
        let windows = Fixture.episode(count: 140, steps: [
            (time: 100, delta: Delta.p3_020),
            (time: 92, delta: Delta.p3_028),
            (time: 82, delta: Delta.p3_040),
            (time: 142, delta: Delta.p3_045),
        ])
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 100, end: 140),
            vetoedRanges: [],
            breaks: Fixture.breaks([100, 92, 82, 142]),
            episodeWindows: windows
        )
        let s = try #require(slot)
        #expect(s.startTime == 82)
        #expect(s.endTime == 142)
        #expect(approx(s.slotConfidence, 0.40))
        #expect(diag.failureReason == nil)
    }

    @Test("Champion scan: middle pair replaces champion and RAISES the bar, blocking the widest")
    func championScanEscalatingBar() throws {
        // Sorted by non-core ASC: P1 start 100 (conf 0.20), P2 start 92 (conf
        // 0.32), P3 start 82 (conf 0.40). Correct (running-champion) scan: P1 ->
        // P2 replaces (0.32 >= 1.5*0.20 = 0.30) -> P3 does NOT (0.40 < 1.5*0.32 =
        // 0.48) -> MIDDLE pair (92) wins. A regression that compared each pair to
        // the ORIGINAL champion would let P3 win (0.40 >= 1.5*0.20), so this test
        // pins the raised-bar semantics that no other fixture exercises.
        let windows = Fixture.episode(count: 140, steps: [
            (time: 100, delta: Delta.esc_020),
            (time: 92, delta: Delta.esc_032),
            (time: 82, delta: Delta.esc_040),
            (time: 142, delta: Delta.esc_045),
        ])
        let slot = try #require(resolver.resolve(
            core: TimeRange(start: 100, end: 140),
            vetoedRanges: [],
            breaks: Fixture.breaks([100, 92, 82, 142]),
            episodeWindows: windows
        ))
        #expect(slot.startTime == 92, "The middle pair must win: the widest is blocked by the raised bar")
        #expect(approx(slot.slotConfidence, 0.32))
    }

    @Test("Champion scan tie: equal non-core + equal pairScore -> earlier startTime wins")
    func tieEqualNonCoreEqualPairScoreEarlierStart() throws {
        // Single end (142); starts 100 (encloses, overlap 40) and 108 (inward,
        // overlap 32). Both pairs have nonCore == 2 and equal pairScore (0.30);
        // the tie breaks to the earlier startTime (100). The equal pairScore is
        // BIT-identical, not merely close: both starts run the same op sequence
        // over the same operands (base, `tie030`, episode-global fluxSigma), so
        // IEEE-754 yields identical bits and the comparator falls through the
        // confidence key to the startTime key.
        let windows = Fixture.episode(count: 140, steps: [
            (time: 100, delta: Delta.tie030),
            (time: 108, delta: Delta.tie030),
            (time: 142, delta: Delta.tie030),
        ])
        let slot = try #require(resolver.resolve(
            core: TimeRange(start: 100, end: 140),
            vetoedRanges: [],
            breaks: Fixture.breaks([100, 108, 142]),
            episodeWindows: windows
        ))
        #expect(slot.startTime == 100)
        #expect(slot.endTime == 142)
        #expect(approx(slot.slotConfidence, 0.30))
    }

    // MARK: - Degenerate core

    @Test("Zero-length core -> .degenerateCore before any coverage math")
    func zeroLengthCoreDegenerate() {
        let windows = Fixture.episode(count: 140, steps: [(time: 90, delta: Delta.s030_c140)])
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 100, end: 100),
            vetoedRanges: [],
            breaks: Fixture.breaks([90, 150]),
            episodeWindows: windows
        )
        #expect(slot == nil)
        #expect(diag.failureReason == .degenerateCore)
        #expect(diag.bestGeometryValidPair == nil)
    }

    @Test("Negative-length core -> .degenerateCore (no trap)")
    func negativeLengthCoreDegenerate() {
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 140, end: 100),
            vetoedRanges: [],
            breaks: Fixture.breaks([90, 150]),
            episodeWindows: []
        )
        #expect(slot == nil)
        #expect(diag.failureReason == .degenerateCore)
    }

    // MARK: - Inward tolerance

    @Test("Inward-tolerance edge accepted at coreEnd - 8; qualifies")
    func inwardToleranceAccepted() throws {
        // core [100,180]; end break at 172 = core.end - 8 is the deepest inward
        // candidate accepted. Slot [100,172] covers 72/80 = 0.9 of the core.
        let windows = Fixture.episode(count: 220, steps: [
            (time: 100, delta: Delta.s030_c220),
            (time: 172, delta: Delta.s030_c220),
        ])
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 100, end: 180),
            vetoedRanges: [],
            breaks: Fixture.breaks([100, 172]),
            episodeWindows: windows
        )
        let s = try #require(slot)
        #expect(s.endTime == 172)
        #expect(approx(s.coreCoverage, 0.9))
        #expect(diag.failureReason == nil)
    }

    @Test("Inward edge beyond tolerance (coreEnd - 9) rejected -> no end candidate")
    func inwardToleranceBeyondRejected() {
        // 171 = core.end - 9 is deeper than the 8s inward tolerance, so it is not
        // a valid end candidate; with no end candidate there are no pairs.
        let windows = Fixture.episode(count: 220, steps: [
            (time: 100, delta: Delta.s030_c220),
            (time: 172, delta: Delta.s030_c220),
        ])
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 100, end: 180),
            vetoedRanges: [],
            breaks: Fixture.breaks([100, 171]),
            episodeWindows: windows
        )
        #expect(slot == nil)
        #expect(diag.failureReason == .noCandidatePairs)
    }

    // MARK: - Determinism

    @Test("Determinism: identical inputs -> identical slot AND diagnostics")
    func determinismIncludingDiagnostics() {
        let windows = Fixture.episode(count: 140, steps: [
            (time: 100, delta: Delta.p3_020),
            (time: 92, delta: Delta.p3_028),
            (time: 82, delta: Delta.p3_040),
            (time: 142, delta: Delta.p3_045),
        ])
        let core = TimeRange(start: 100, end: 140)
        let breaks = Fixture.breaks([100, 92, 82, 142])
        let first = resolver.resolveWithDiagnostics(core: core, vetoedRanges: [], breaks: breaks, episodeWindows: windows)
        let second = resolver.resolveWithDiagnostics(core: core, vetoedRanges: [], breaks: breaks, episodeWindows: windows)
        #expect(first.slot == second.slot)
        #expect(first.diagnostics == second.diagnostics)
    }

    // MARK: - Degenerate inputs

    @Test("Empty episode windows -> champion has zero-score edges -> .edgeBelowFloor (no crash)")
    func degenerateEmptyWindows() throws {
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 100, end: 140),
            vetoedRanges: [],
            breaks: Fixture.breaks([90, 150]),
            episodeWindows: []
        )
        #expect(slot == nil)
        #expect(diag.failureReason == .edgeBelowFloor)
        let champ = try #require(diag.bestGeometryValidPair)
        #expect(champ.slotConfidence == 0)
        #expect(champ.startEdge.stepScore == 0)
        // An unscorable episode contributes no sub-signals.
        #expect(champ.startEdge.contributingSignals == 0)
        #expect(champ.endEdge.contributingSignals == 0)
    }

    @Test("Single break (one side only) -> .noCandidatePairs")
    func degenerateSingleBreak() {
        let windows = Fixture.episode(count: 140, steps: [(time: 90, delta: Delta.s030_c140)])
        let (slot, diag) = resolver.resolveWithDiagnostics(
            core: TimeRange(start: 100, end: 140),
            vetoedRanges: [],
            breaks: Fixture.breaks([90]), // only a start candidate; no end candidate
            episodeWindows: windows
        )
        #expect(slot == nil)
        #expect(diag.failureReason == .noCandidatePairs)
        #expect(diag.bestGeometryValidPair == nil)
    }

    // MARK: - Detector-refactor regression

    @Test("Detector refactor: exposed candidate scoring IS buildEntries' internal edge scoring (byte-identical fusion)")
    func detectorRefactorFusionEquivalence() throws {
        // A multi-modal loud insertion [30,50] whose edges both carry a real
        // discontinuity, mirroring AudioForensicsBoundaryDetectorTests' fixture.
        let windows: [FeatureWindow] = (0..<40).map { i in
            let inAd = i >= 15 && i < 25
            return AcousticFeatureFixtures.window(
                startTime: Double(i) * 2.0,
                endTime: Double(i + 1) * 2.0,
                rms: inAd ? 0.55 + Double(i % 2) * 0.01 : 0.20 + Double(i % 3) * 0.002,
                spectralFlux: inAd ? 0.30 : 0.05 + Double(i % 2) * 0.001,
                musicProbability: inAd ? 0.80 : 0.02
            )
        }
        let detector = AudioForensicsBoundaryDetector()
        let span = DecodedSpan(
            id: DecodedSpan.makeId(assetId: "regr", firstAtomOrdinal: 0, lastAtomOrdinal: 10),
            assetId: "regr",
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 10,
            startTime: 30.0,
            endTime: 50.0,
            anchorProvenance: [.classifierSeed(regionId: "r1", score: 0.85)]
        )
        let cfg = FusionWeightConfig()

        let entries = detector.buildEntries(span: span, episodeWindows: windows, fusionConfig: cfg)
        try #require(entries.count == 1)
        guard case .audioForensics(let score, let dominant, let contributing) = entries[0].detail else {
            Issue.record("Expected .audioForensics detail")
            return
        }

        // Independently score the two edges via the EXPOSED candidate API. Both
        // edges are scorable here.
        let startScore = try #require(detector.scoreCandidateEdge(at: span.startTime, episodeWindows: windows))
        let endScore = try #require(detector.scoreCandidateEdge(at: span.endTime, episodeWindows: windows))
        // buildEntries emits the STRONGER edge (ties -> start), byte for byte.
        let best = endScore.stepScore > startScore.stepScore ? endScore : startScore

        #expect(score == best.stepScore, "Ledger boundaryScore must equal the exposed candidate stepScore exactly")
        #expect(dominant == best.dominantSignal)
        #expect(contributing == best.contributingSignalCount)
        let expectedWeight = min(best.stepScore * cfg.audioForensicsCap, cfg.audioForensicsCap)
        #expect(entries[0].weight == expectedWeight, "Ledger weight must be unchanged by the refactor")
    }
}
