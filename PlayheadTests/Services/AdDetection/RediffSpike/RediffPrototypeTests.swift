// RediffPrototypeTests.swift
// SPIKE (playhead-xsdz.16): unit tests for the pure-Swift fingerprint-rediff
// prototype. TEST-TARGET-ONLY — nothing in production invokes the prototype.
//
// Two layers:
//   1. Pure-synthetic fingerprint sequences (deterministic SplitMix64
//      pseudo-random UInt32 arrays with fixed seeds) spliced so expected
//      slot indices are EXACT at fingerprint granularity. Two independent
//      random 32-bit fingerprints collide with probability 2^-32; over the
//      few thousand values used here an accidental cross-segment match is
//      astronomically unlikely — and the fixed seeds make every test run
//      bit-identical anyway.
//   2. Fixture parity: the 5 JSON fixtures in TestFixtures/RediffSpike/
//      pin exact behavioral equivalence with scripts/l2f-dai-rediff.py
//      (mergedRuns, slotsA, slotsB, minGapFps at fingerprint-index level).

import Foundation
import Testing

// MARK: - Deterministic synthetic fingerprints

/// SplitMix64 — tiny, deterministic, seedable PRNG (public-domain constants).
private struct SplitMix64 {
    private var state: UInt64

    init(seed: UInt64) {
        state = seed
    }

    mutating func next() -> UInt64 {
        state &+= 0x9E37_79B9_7F4A_7C15
        var z = state
        z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
        z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
        return z ^ (z >> 31)
    }

    mutating func nextFingerprint() -> UInt32 {
        UInt32(truncatingIfNeeded: next() >> 32)
    }
}

/// Deterministic pseudo-random fingerprint segment. Distinct seeds produce
/// segments that share no values (up to the 2^-32-per-pair collision odds
/// documented in the file header).
private func fps(_ count: Int, seed: UInt64) -> [UInt32] {
    var rng = SplitMix64(seed: seed)
    return (0..<count).map { _ in rng.nextFingerprint() }
}

// MARK: - Builders / assertion helpers

/// All synthetic tests use a power-of-two fingerprint period so every
/// derived seconds value is an exact Double (and minGapFps = 5.0/0.125 = 40).
private let syntheticSecondsPerFp = 0.125
private let syntheticMinGapFps = 40

private func rediff(a: [UInt32], b: [UInt32]) -> RediffPrototype.Result {
    RediffPrototype.rediff(
        fingerprintA: a,
        secondsPerFpA: syntheticSecondsPerFp,
        fingerprintB: b,
        secondsPerFpB: syntheticSecondsPerFp
    )
}

private func run(_ aStart: Int, _ bStart: Int, _ length: Int, _ errors: Int) -> RediffPrototype.Run {
    RediffPrototype.Run(aStart: aStart, bStart: bStart, length: length, errors: errors)
}

/// Assert slots match the expected exact fingerprint indices AND that every
/// derived seconds/confidence field is internally consistent with those
/// indices (off-by-one at fingerprint granularity is a named review concern).
private func expectSlots(
    _ slots: [RediffPrototype.Slot],
    _ expected: [(startFp: Int, endFp: Int, leftFps: Int, rightFps: Int)],
    secondsPerFp: Double = syntheticSecondsPerFp,
    sourceLocation: SourceLocation = #_sourceLocation
) {
    #expect(
        slots.count == expected.count,
        "expected \(expected.count) slots, got \(slots.map { ($0.startFp, $0.endFp) })",
        sourceLocation: sourceLocation
    )
    for (slot, want) in zip(slots, expected) {
        #expect(slot.startFp == want.startFp, sourceLocation: sourceLocation)
        #expect(slot.endFp == want.endFp, sourceLocation: sourceLocation)
        #expect(slot.leftRunFps == want.leftFps, sourceLocation: sourceLocation)
        #expect(slot.rightRunFps == want.rightFps, sourceLocation: sourceLocation)
        #expect(slot.startSeconds == Double(want.startFp) * secondsPerFp, sourceLocation: sourceLocation)
        #expect(slot.endSeconds == Double(want.endFp) * secondsPerFp, sourceLocation: sourceLocation)
        #expect(
            slot.durationSeconds == Double(want.endFp - want.startFp) * secondsPerFp,
            sourceLocation: sourceLocation
        )
        #expect(slot.leftRunSeconds == Double(want.leftFps) * secondsPerFp, sourceLocation: sourceLocation)
        #expect(slot.rightRunSeconds == Double(want.rightFps) * secondsPerFp, sourceLocation: sourceLocation)
        let expectedConfidence = 1 - exp(-min(slot.leftRunSeconds, slot.rightRunSeconds) / 60.0)
        #expect(abs(slot.confidence - expectedConfidence) < 1e-12, sourceLocation: sourceLocation)
    }
}

// MARK: - Synthetic ground-truth tests

@Suite("RediffPrototype synthetic (playhead-xsdz.16 SPIKE)")
struct RediffPrototypeTests {

    // MARK: Whole-pair scenarios

    @Test("identical A/B: one full-coverage run, zero slots on both sides")
    func identicalCopiesOneFullRunZeroSlots() {
        let content = fps(600, seed: 1)
        let result = rediff(a: content, b: content)
        #expect(result.mergedRuns == [run(0, 0, 600, 0)])
        #expect(result.slotsA.isEmpty)
        #expect(result.slotsB.isEmpty)
        #expect(result.minGapFpsA == syntheticMinGapFps)
        #expect(result.minGapFpsB == syntheticMinGapFps)
        #expect(result.alignedSecondsB == 600 * syntheticSecondsPerFp)
        #expect(result.alignedFractionB == 1.0)
    }

    @Test("insert in B only: exact slotB indices, slotsA empty (touching A intervals union)")
    func insertInBOnlyExactSlotBIndices() {
        let pre = fps(400, seed: 1)
        let post = fps(500, seed: 2)
        let ad = fps(160, seed: 3)
        let result = rediff(a: pre + post, b: pre + ad + post)
        #expect(result.mergedRuns == [run(0, 0, 400, 0), run(400, 560, 500, 0)])
        expectSlots(result.slotsB, [(startFp: 400, endFp: 560, leftFps: 400, rightFps: 500)])
        // A intervals [0,400) and [400,900) touch: the union covers all of A.
        expectSlots(result.slotsA, [])
        // 900 aligned of 1060 B fps: the inserted ad is the unaligned remainder.
        #expect(result.alignedFractionB == 900.0 / 1060.0)
    }

    @Test("insert in A only: exact slotA indices in the PLAYED timeline, slotsB empty")
    func insertInAOnlyExactSlotAIndices() {
        let pre = fps(400, seed: 1)
        let post = fps(500, seed: 2)
        let ad = fps(240, seed: 4)
        let result = rediff(a: pre + ad + post, b: pre + post)
        #expect(result.mergedRuns == [run(0, 0, 400, 0), run(640, 400, 500, 0)])
        expectSlots(result.slotsA, [(startFp: 400, endFp: 640, leftFps: 400, rightFps: 500)])
        expectSlots(result.slotsB, [])
    }

    @Test("rotation: 240-fp adA vs 160-fp adB at the same index — exact slots in BOTH timelines")
    func rotationExactSlotIndicesBothTimelines() {
        let pre = fps(400, seed: 1)
        let post = fps(500, seed: 2)
        let adA = fps(240, seed: 4)
        let adB = fps(160, seed: 3)
        let result = rediff(a: pre + adA + post, b: pre + adB + post)
        #expect(result.mergedRuns == [run(0, 0, 400, 0), run(640, 560, 500, 0)])
        expectSlots(result.slotsA, [(startFp: 400, endFp: 640, leftFps: 400, rightFps: 500)])
        expectSlots(result.slotsB, [(startFp: 400, endFp: 560, leftFps: 400, rightFps: 500)])
    }

    @Test("drift: different-length leading ads shift ALL downstream absolute offsets — boundaries still exact")
    func driftLeadingAdsAnchorAlignmentNotAbsoluteOffsets() {
        let pre = fps(400, seed: 1)
        let post = fps(500, seed: 2)
        let leadA = fps(100, seed: 5)
        let leadB = fps(50, seed: 6)
        let adA = fps(240, seed: 4)
        let adB = fps(160, seed: 3)
        // A: [leadA 0..100)[pre 100..500)[adA 500..740)[post 740..1240)
        // B: [leadB 0..50) [pre 50..450) [adB 450..610)[post 610..1110)
        let result = rediff(a: leadA + pre + adA + post, b: leadB + pre + adB + post)
        #expect(result.mergedRuns == [run(100, 50, 400, 0), run(740, 610, 500, 0)])
        expectSlots(result.slotsA, [
            (startFp: 0, endFp: 100, leftFps: 0, rightFps: 400),
            (startFp: 500, endFp: 740, leftFps: 400, rightFps: 500)
        ])
        expectSlots(result.slotsB, [
            (startFp: 0, endFp: 50, leftFps: 0, rightFps: 400),
            (startFp: 450, endFp: 610, leftFps: 400, rightFps: 500)
        ])
    }

    @Test("asymmetric fingerprint rates: each side uses its OWN secondsPerFp and minGapFps")
    func asymmetricRatesUseOwnTimelineEverywhere() {
        // spfA = 0.125 -> minGapFpsA = 40; spfB = 0.0625 -> minGapFpsB = 80.
        // Rotation with adA = 60 fps and adB = 70 fps: the A gap (60 >= 40)
        // is emitted, the B gap (70 < 80) is DROPPED. Any A/B swap — in the
        // minGap wiring, the slot seconds conversion, or alignedSecondsB —
        // flips one of these assertions.
        let pre = fps(400, seed: 1)
        let post = fps(500, seed: 2)
        let adA = fps(60, seed: 4)
        let adB = fps(70, seed: 3)
        let result = RediffPrototype.rediff(
            fingerprintA: pre + adA + post,
            secondsPerFpA: 0.125,
            fingerprintB: pre + adB + post,
            secondsPerFpB: 0.0625
        )
        #expect(result.mergedRuns == [run(0, 0, 400, 0), run(460, 470, 500, 0)])
        #expect(result.minGapFpsA == 40)
        #expect(result.minGapFpsB == 80)
        expectSlots(
            result.slotsA,
            [(startFp: 400, endFp: 460, leftFps: 400, rightFps: 500)],
            secondsPerFp: 0.125
        )
        #expect(result.slotsA.first?.startSeconds == 50.0)  // 400 * spfA, not 400 * spfB
        expectSlots(result.slotsB, [])
        // Aligned coverage is B-seconds: 900 fps * 0.0625, not 900 * 0.125.
        #expect(result.alignedSecondsB == 56.25)
    }

    // MARK: Noise robustness

    @Test("hamming noise: 1-2 flipped bits inside matched content stay one run, zero phantom slots")
    func hammingNoiseSingleRunNoPhantomSlots() {
        let content = fps(600, seed: 1)
        var noisy = content
        noisy[100] ^= 0b1                      // 1 bit
        noisy[250] ^= 0b1_0001                 // 2 bits
        noisy[400] ^= 0x8000_0000              // 1 bit
        let result = rediff(a: content, b: noisy)
        #expect(result.mergedRuns == [run(0, 0, 600, 4)])
        #expect(result.slotsA.isEmpty)
        #expect(result.slotsB.isEmpty)
    }

    @Test("equal-gap noise: 48 unmatchable fps of the SAME length on both sides merge across, zero phantom slots")
    func equalGapNoiseMergedAcross() {
        let pre = fps(300, seed: 1)
        let post = fps(300, seed: 2)
        let noiseA = fps(48, seed: 7)
        let noiseB = fps(48, seed: 8)
        let result = rediff(a: pre + noiseA + post, b: pre + noiseB + post)
        // find_runs yields (0,0,300) + (348,348,300); merge_runs (offset delta 0,
        // aGap == bGap == 48) folds them into one covering run.
        #expect(result.mergedRuns == [run(0, 0, 648, 0)])
        #expect(result.slotsA.isEmpty)
        #expect(result.slotsB.isEmpty)
        // Merged spans INCLUDE the folded noise gap (span semantics, python
        // alignedSecondsB parity): 648/648, not 600/648.
        #expect(result.alignedFractionB == 1.0)
    }

    // MARK: Head / tail / multiple / threshold slots

    @Test("head and tail slots in A (played timeline)")
    func headAndTailSlotsInA() {
        let content = fps(400, seed: 1)
        let adHead = fps(60, seed: 9)
        let adTail = fps(70, seed: 10)
        let result = rediff(a: adHead + content + adTail, b: content)
        #expect(result.mergedRuns == [run(60, 0, 400, 0)])
        expectSlots(result.slotsA, [
            (startFp: 0, endFp: 60, leftFps: 0, rightFps: 400),
            (startFp: 460, endFp: 530, leftFps: 400, rightFps: 0)
        ])
        expectSlots(result.slotsB, [])
        // Boundary slots have a zero flank on one side -> confidence exactly 0.
        #expect(result.slotsA.first?.confidence == 0.0)
        #expect(result.slotsA.last?.confidence == 0.0)
    }

    @Test("head and tail slots in B (fresh timeline)")
    func headAndTailSlotsInB() {
        let content = fps(400, seed: 1)
        let adHead = fps(60, seed: 9)
        let adTail = fps(70, seed: 10)
        let result = rediff(a: content, b: adHead + content + adTail)
        #expect(result.mergedRuns == [run(0, 60, 400, 0)])
        expectSlots(result.slotsB, [
            (startFp: 0, endFp: 60, leftFps: 0, rightFps: 400),
            (startFp: 460, endFp: 530, leftFps: 400, rightFps: 0)
        ])
        expectSlots(result.slotsA, [])
    }

    @Test("multiple insertions in one pair produce multiple exact slots")
    func multipleInsertionsMultipleSlots() {
        let c1 = fps(300, seed: 1)
        let c2 = fps(300, seed: 2)
        let c3 = fps(300, seed: 11)
        let ad1 = fps(80, seed: 12)
        let ad2 = fps(90, seed: 13)
        let result = rediff(a: c1 + c2 + c3, b: c1 + ad1 + c2 + ad2 + c3)
        #expect(result.mergedRuns == [run(0, 0, 300, 0), run(300, 380, 300, 0), run(600, 770, 300, 0)])
        expectSlots(result.slotsB, [
            (startFp: 300, endFp: 380, leftFps: 300, rightFps: 300),
            (startFp: 680, endFp: 770, leftFps: 300, rightFps: 300)
        ])
        expectSlots(result.slotsA, [])
    }

    @Test("a gap below minGapFps is dropped (30-fp insert < 40-fp threshold)")
    func gapBelowMinGapFpsDropped() {
        let pre = fps(300, seed: 1)
        let post = fps(300, seed: 2)
        let tinyAd = fps(30, seed: 14)
        let result = rediff(a: pre + post, b: pre + tinyAd + post)
        // Offset delta 30 > offsetSlack, so the runs stay separate — but the
        // 30-fp B gap is below minGapFps and must be dropped, not reported.
        #expect(result.mergedRuns.count == 2)
        #expect(result.slotsA.isEmpty)
        #expect(result.slotsB.isEmpty)
    }

    // MARK: A-interval union (the gapsInA design constraint)

    @Test("repeated content: two B regions matching the SAME A region are unioned, not treated as adjacent")
    func repeatedContentOverlappingAIntervalsUnioned() {
        let x = fps(300, seed: 1)
        let tail = fps(100, seed: 2)
        let noise = fps(50, seed: 15)
        // B contains X twice; both runs map to A's single X at aStart 0.
        // The interval UNION collapses both to [0,300), leaving only the true
        // tail gap. (This case alone does not discriminate a naive
        // adjacent-gap port — a naive port skips the negative middle "gap"
        // and happens to emit the same tail slot; the two tests below are
        // the discriminators.)
        let result = rediff(a: x + tail, b: x + noise + x)
        #expect(result.mergedRuns == [run(0, 0, 300, 0), run(0, 350, 300, 0)])
        expectSlots(result.slotsA, [(startFp: 300, endFp: 400, leftFps: 300, rightFps: 0)])
        expectSlots(result.slotsB, [(startFp: 300, endFp: 350, leftFps: 300, rightFps: 300)])
    }

    @Test("out-of-A-order matches: a later B match maps EARLIER in A — union kills the phantom head gap")
    func outOfOrderAMappingNoPhantomHeadGap() {
        let x = fps(300, seed: 1)
        let y = fps(300, seed: 2)
        let noise = fps(50, seed: 15)
        // A = X+Y but B plays Y first, so runs sorted by bStart are DESCENDING
        // in aStart. Naive adjacent-gap logic over the bStart order reports a
        // phantom A head gap [0, 300); the union of [300,600) and [0,300)
        // covers all of A, so slotsA must be empty.
        let result = rediff(a: x + y, b: y + noise + x)
        #expect(result.mergedRuns == [run(300, 0, 300, 0), run(0, 350, 300, 0)])
        expectSlots(result.slotsA, [])
        expectSlots(result.slotsB, [(startFp: 300, endFp: 350, leftFps: 300, rightFps: 300)])
    }

    @Test("partially overlapping A intervals: flanks come from the MERGED covered interval, not the last run")
    func partiallyOverlappingAIntervalsMergedFlankLengths() {
        let x = fps(350, seed: 1)
        let tail = fps(100, seed: 2)
        let noise = fps(50, seed: 15)
        // B re-plays X[100..<350] after X[0..<250]: covered A intervals
        // [0,250) and [100,350) overlap and union to [0,350). The A tail
        // gap's left flank must be the merged 350 — a no-union port gets
        // these slot indices right but reports the last run's 250.
        let result = rediff(a: x + tail, b: Array(x[0..<250]) + noise + Array(x[100..<350]))
        #expect(result.mergedRuns == [run(0, 0, 250, 0), run(100, 300, 250, 0)])
        expectSlots(result.slotsA, [(startFp: 350, endFp: 450, leftFps: 350, rightFps: 0)])
        expectSlots(result.slotsB, [(startFp: 250, endFp: 300, leftFps: 250, rightFps: 250)])
    }

    // MARK: Known semantic limitations (pinned, inherited from the python reference)

    @Test("LIMITATION: pool rotation swapping two fills' ORDER cross-matches ad<->ad — invisible on BOTH sides")
    func limitationOrderSwappedFillsAreInvisible() {
        let c1 = fps(300, seed: 1)
        let c2 = fps(300, seed: 2)
        let c3 = fps(300, seed: 11)
        let adZ = fps(80, seed: 12)
        let adW = fps(90, seed: 13)
        // A played Z then W; the re-fetch B serves W then Z (same pool, new
        // order). Every A interval is covered by SOME run — the fills
        // cross-match each other (Z_B<->Z_A, W_B<->W_A at wild offsets) — so
        // the union complement is empty, and B has no uncovered gap either.
        // The user heard two DAI ads and slotsA reports neither: slotsA is
        // high-precision, NOT exhaustive-recall (see RediffPrototype header).
        let result = rediff(a: c1 + adZ + c2 + adW + c3, b: c1 + adW + c2 + adZ + c3)
        #expect(result.mergedRuns == [
            run(0, 0, 300, 0),
            run(680, 300, 90, 0),
            run(380, 390, 300, 0),
            run(300, 690, 80, 0),
            run(770, 770, 300, 0)
        ])
        expectSlots(result.slotsA, [])
        expectSlots(result.slotsB, [])
    }

    @Test("LIMITATION: a SAME-length creative swap at the same slot merges away as noise (aGap == bGap)")
    func limitationEqualLengthRotationInvisible() {
        let pre = fps(400, seed: 1)
        let post = fps(500, seed: 2)
        let adA = fps(80, seed: 12)
        let adB = fps(80, seed: 13)
        // findRuns yields (0,0,400) + (480,480,500); mergeRuns sees offset
        // delta 0 and aGap == bGap == 80 — the python "noise on both sides"
        // rule — and folds everything into one covering span. Two different
        // same-length creatives rotated at the same boundary (e.g. two
        // standard 30s spots) are invisible on BOTH sides. Contrast the
        // rotation tests above, where differing ad lengths keep the runs
        // separate and both slots are reported.
        let result = rediff(a: pre + adA + post, b: pre + adB + post)
        #expect(result.mergedRuns == [run(0, 0, 980, 0)])
        expectSlots(result.slotsA, [])
        expectSlots(result.slotsB, [])
        #expect(result.alignedFractionB == 1.0)
    }

    // MARK: findRuns edges

    @Test("a would-be run shorter than minRunLen=8 is rejected; zero runs -> whole-side gaps")
    func runShorterThanMinRunLenRejected() {
        let common7 = fps(7, seed: 16)
        let a = fps(100, seed: 17) + common7 + fps(100, seed: 18)
        let b = fps(100, seed: 19) + common7 + fps(100, seed: 20)
        #expect(RediffPrototype.findRuns(fpA: a, fpB: b).isEmpty)
        // Zero runs: the python empty-runs branch reports the WHOLE side as a
        // gap when it clears minGapFps, with zero flanks (confidence 0).
        let result = rediff(a: a, b: b)
        #expect(result.mergedRuns.isEmpty)
        expectSlots(result.slotsA, [(startFp: 0, endFp: 207, leftFps: 0, rightFps: 0)])
        expectSlots(result.slotsB, [(startFp: 0, endFp: 207, leftFps: 0, rightFps: 0)])
        #expect(result.alignedSecondsB == 0)
        #expect(result.alignedFractionB == 0)
    }

    @Test("a run of exactly minRunLen=8 is accepted with exact indices")
    func runOfExactlyMinRunLenAccepted() {
        let common8 = fps(8, seed: 16)
        let a = fps(100, seed: 17) + common8 + fps(100, seed: 18)
        let b = fps(100, seed: 19) + common8 + fps(100, seed: 20)
        #expect(RediffPrototype.findRuns(fpA: a, fpB: b) == [run(100, 100, 8, 0)])
    }

    @Test("anchor choice keeps the FIRST longest run (strict >, ascending A position)")
    func anchorChoiceFirstLongestWins() {
        let common = fps(10, seed: 21)
        // Both A occurrences extend to the same length 10: the FIRST (lowest
        // aStart) must win because only strictly longer candidates replace it.
        let a = fps(50, seed: 22) + common + fps(50, seed: 23) + common + fps(50, seed: 24)
        let b = fps(30, seed: 25) + common + fps(30, seed: 26)
        #expect(RediffPrototype.findRuns(fpA: a, fpB: b) == [run(50, 30, 10, 0)])
    }

    @Test("anchor choice prefers a strictly longer later anchor")
    func anchorChoiceStrictlyLongerLaterAnchorWins() {
        let common = fps(20, seed: 21)
        // First A occurrence is truncated to 10 fps; the second is the full 20.
        let a = fps(50, seed: 22) + Array(common[0..<10]) + fps(50, seed: 23) + common + fps(50, seed: 24)
        let b = fps(30, seed: 25) + common + fps(30, seed: 26)
        #expect(RediffPrototype.findRuns(fpA: a, fpB: b) == [run(110, 30, 20, 0)])
    }

    @Test("backward extension never re-enters the previous run's covered B range")
    func backwardExtensionStopsAtPreviousRunCoverage() {
        // A carries P's 20-fp tail immediately before a full copy of P, so
        // the second run's anchor (a0 = 20) has a matching predecessor:
        // fpA[19] == P[299] == fpB[299]. But B[299] is already covered by
        // the first accepted run — python's `i - b > covered_until_b` bound
        // must stop the backward extension at B[300]. A `>=` port would
        // emit (19, 299, 301) and double-count B[299] into two runs.
        // (Added after mutation testing: no prior test killed this mutant.)
        let p = fps(300, seed: 29)
        let a = Array(p[280..<300]) + p
        let b = p + p
        #expect(RediffPrototype.findRuns(fpA: a, fpB: b) == [run(20, 0, 300, 0), run(20, 300, 300, 0)])
    }

    @Test("empty and tiny inputs do not crash and yield defined results")
    func emptyAndTinyInputs() {
        let empty = rediff(a: [], b: [])
        #expect(empty.mergedRuns.isEmpty)
        #expect(empty.slotsA.isEmpty)   // totalA 0 < minGapFps
        #expect(empty.slotsB.isEmpty)
        #expect(empty.alignedFractionB == 0)  // defined (not NaN) for empty B

        let content = fps(100, seed: 1)
        let emptyB = rediff(a: content, b: [])
        #expect(emptyB.mergedRuns.isEmpty)
        expectSlots(emptyB.slotsA, [(startFp: 0, endFp: 100, leftFps: 0, rightFps: 0)])
        expectSlots(emptyB.slotsB, [])

        let emptyA = rediff(a: [], b: content)
        expectSlots(emptyA.slotsA, [])
        expectSlots(emptyA.slotsB, [(startFp: 0, endFp: 100, leftFps: 0, rightFps: 0)])

        // Tiny identical inputs: below minRunLen -> zero runs; below minGapFps
        // -> no whole-side gap either.
        let tiny = rediff(a: [1, 2, 3], b: [1, 2, 3])
        #expect(tiny.mergedRuns.isEmpty)
        #expect(tiny.slotsA.isEmpty)
        #expect(tiny.slotsB.isEmpty)

        // Entirely unrelated inputs: zero runs -> whole-side gap on each side.
        let unrelated = rediff(a: fps(100, seed: 27), b: fps(120, seed: 28))
        #expect(unrelated.mergedRuns.isEmpty)
        expectSlots(unrelated.slotsA, [(startFp: 0, endFp: 100, leftFps: 0, rightFps: 0)])
        expectSlots(unrelated.slotsB, [(startFp: 0, endFp: 120, leftFps: 0, rightFps: 0)])
    }

    // MARK: mergeRuns unit behavior

    @Test("mergeRuns: offset delta at the slack boundary merges; beyond it does not")
    func mergeRunsOffsetSlackBoundary() {
        let r1 = run(10, 8, 20, 1)
        // Offset delta |2 - 4| = 2 == offsetSlack, gaps (b=2, a=4) within slack.
        let mergeable = run(34, 30, 10, 2)
        #expect(RediffPrototype.mergeRuns([r1, mergeable]) == [run(10, 8, 32, 3)])
        // Offset delta |2 - 5| = 3 > offsetSlack -> kept separate.
        let unmergeable = run(35, 30, 10, 2)
        #expect(RediffPrototype.mergeRuns([r1, unmergeable]) == [r1, unmergeable])
    }

    @Test("mergeRuns: |bGap - aGap| beyond gapDiffSlack keeps runs separate")
    func mergeRunsGapDiffSlackBoundary() {
        let r1 = run(0, 0, 20, 0)
        // bGap 0, aGap 2 -> |diff| == 2 == gapDiffSlack -> merged (B-length wins).
        #expect(RediffPrototype.mergeRuns([r1, run(22, 20, 10, 0)]) == [run(0, 0, 30, 0)])
        // bGap 0, aGap 5 -> |diff| 5 > 2, and offset delta 5 > 2 -> separate.
        #expect(RediffPrototype.mergeRuns([r1, run(25, 20, 10, 0)]) == [r1, run(25, 20, 10, 0)])
    }

    @Test("mergeRuns: a negative aGap is never merged even when offsets agree within slack")
    func mergeRunsNegativeAGapNotMerged() {
        // Offsets 10 vs 9 (delta 1 <= slack) but aGap = 29 - (10+20) = -1.
        let r1 = run(10, 0, 20, 0)
        let r2 = run(29, 20, 10, 0)
        #expect(RediffPrototype.mergeRuns([r1, r2]) == [r1, r2])
    }

    @Test("mergeRuns sorts by bStart before merging")
    func mergeRunsSortsByBStart() {
        let early = run(0, 0, 300, 0)
        let late = run(348, 348, 300, 0)
        #expect(RediffPrototype.mergeRuns([late, early]) == [run(0, 0, 648, 0)])
    }

    @Test("mergeRuns: a chained merge compares against the merged HEAD run, not the previous fragment")
    func mergeRunsChainedMergeUsesMergedHeadRun() {
        // Offsets 0, 2, 4: the second run merges into the first (offset delta
        // 2 == slack, aGap 4 vs bGap 2 within gapDiffSlack); the third run's
        // deltas vs the MERGED run (head offset 0, aGap 6 vs bGap 2) exceed
        // both slacks, so it must stay separate. A port that compares against
        // the previous ORIGINAL fragment (deltas of 2) would wrongly chain
        // all three into (0,0,34,0). Pinned against python merge_runs.
        let chained = [run(0, 0, 10, 0), run(14, 12, 10, 0), run(28, 24, 10, 0)]
        #expect(RediffPrototype.mergeRuns(chained) == [run(0, 0, 22, 0), run(28, 24, 10, 0)])
    }

    // MARK: Confidence + rounding formulas

    @Test("confidence: zero flank -> exactly 0.0")
    func confidenceZeroFlank() {
        #expect(RediffPrototype.confidence(leftRunSeconds: 0, rightRunSeconds: 300) == 0.0)
        #expect(RediffPrototype.confidence(leftRunSeconds: 300, rightRunSeconds: 0) == 0.0)
    }

    @Test("confidence: 60s flanks -> 1 - e^-1 (~0.632) to 1e-9")
    func confidenceSixtySecondFlanks() {
        let got = RediffPrototype.confidence(leftRunSeconds: 60, rightRunSeconds: 60)
        #expect(abs(got - (1 - exp(-1.0))) < 1e-9)
    }

    @Test("confidence uses the MIN flank")
    func confidenceUsesMinFlank() {
        let got = RediffPrototype.confidence(leftRunSeconds: 30, rightRunSeconds: 90)
        #expect(abs(got - (1 - exp(-0.5))) < 1e-9)
    }

    @Test("confidence at extreme flanks: exp underflows to 0 -> exactly 1.0, never NaN/overshoot")
    func confidenceExtremeFlanksUnderflowToOne() {
        // exp(-1e6/60) underflows to +0.0, so confidence is exactly 1.0.
        let extreme = RediffPrototype.confidence(leftRunSeconds: 1e6, rightRunSeconds: 1e6)
        #expect(extreme == 1.0)
        // Monotone and bounded on the way there (no overshoot past 1).
        let large = RediffPrototype.confidence(leftRunSeconds: 3600, rightRunSeconds: 3600)
        #expect(large > 0.99 && large <= 1.0)
    }

    @Test("minGapFps uses python banker's rounding (round-half-to-even) and floors at 1")
    func minGapFpsBankersRounding() {
        #expect(RediffPrototype.minGapFps(minAdSeconds: 5.0, secondsPerFp: 0.125) == 40)
        // 5.0625 / 0.125 = 40.5 -> banker's rounds to the even 40 (away-from-zero would give 41).
        #expect(RediffPrototype.minGapFps(minAdSeconds: 5.0625, secondsPerFp: 0.125) == 40)
        // 5.1875 / 0.125 = 41.5 -> banker's rounds to the even 42.
        #expect(RediffPrototype.minGapFps(minAdSeconds: 5.1875, secondsPerFp: 0.125) == 42)
        // max(1, ...) floor.
        #expect(RediffPrototype.minGapFps(minAdSeconds: 0.05, secondsPerFp: 0.125) == 1)
    }

    @Test("minGapFps extreme-but-finite inputs stay sane (python max(1, ...) parity)")
    func minGapFpsExtremeFiniteInputs() {
        // Huge secondsPerFp: ratio rounds to 0, floored to 1.
        #expect(RediffPrototype.minGapFps(minAdSeconds: 5.0, secondsPerFp: .greatestFiniteMagnitude) == 1)
        // Negative minAdSeconds: python max(1, int(round(-80.0))) == 1.
        #expect(RediffPrototype.minGapFps(minAdSeconds: -10.0, secondsPerFp: 0.125) == 1)
        // (secondsPerFp <= 0/NaN/inf and non-representable ratios fail loudly
        // via precondition — untestable here: Swift Testing exit tests are
        // unavailable on the iOS simulator.)
    }
}

// MARK: - Fixture parity tests

@Suite("RediffPrototype fixture parity (playhead-xsdz.16 SPIKE)")
struct RediffPrototypeFixtureParityTests {

    static let fixtureNames = [
        "identity",
        "insert-in-a",
        "insert-in-b",
        "rotation",
        "real-rotated-pair"
    ]

    private func runFixture(_ fixture: RediffSpikeFixture) -> RediffPrototype.Result {
        let params = fixture.algorithmParams
        return RediffPrototype.rediff(
            fingerprintA: fixture.fingerprintA,
            secondsPerFpA: fixture.secondsPerFpA,
            fingerprintB: fixture.fingerprintB,
            secondsPerFpB: fixture.secondsPerFpB,
            hammingTol: params.hammingTol,
            minRunLen: params.minRunFps,
            offsetSlack: params.offsetSlack,
            gapDiffSlack: params.gapDiffSlack,
            minAdSeconds: params.minAdSeconds
        )
    }

    /// Exact fingerprint-index parity for slots. The reference JSON's seconds
    /// were rounded to 2 decimals by python's round(); our unrounded values
    /// must land within that half-of-last-decimal window.
    private func expectSlotsMatchReference(
        _ slots: [RediffPrototype.Slot],
        _ reference: [RediffSpikeFixture.ReferenceSlot],
        sourceLocation: SourceLocation = #_sourceLocation
    ) {
        #expect(
            slots.count == reference.count,
            "expected \(reference.count) slots, got \(slots.map { ($0.startFp, $0.endFp) })",
            sourceLocation: sourceLocation
        )
        for (slot, ref) in zip(slots, reference) {
            #expect(slot.startFp == ref.startFp, sourceLocation: sourceLocation)
            #expect(slot.endFp == ref.endFp, sourceLocation: sourceLocation)
            #expect(slot.leftRunFps == ref.leftRunFps, sourceLocation: sourceLocation)
            #expect(slot.rightRunFps == ref.rightRunFps, sourceLocation: sourceLocation)
            #expect(abs(slot.startSeconds - ref.startSeconds) <= 0.005 + 1e-9, sourceLocation: sourceLocation)
            #expect(abs(slot.endSeconds - ref.endSeconds) <= 0.005 + 1e-9, sourceLocation: sourceLocation)
        }
    }

    @Test("fixture directory contains exactly the 5 pinned fixtures (none missing, no uncovered orphans)")
    func fixtureDirectoryMatchesPinnedNames() throws {
        let directory = RediffSpikeFixtureLoader.repoRoot()
            .appendingPathComponent(RediffSpikeFixtureLoader.fixturesRelativePath)
        let onDisk = try FileManager.default.contentsOfDirectory(atPath: directory.path)
            .filter { $0.hasSuffix(".json") }
            .map { String($0.dropLast(".json".count)) }
            .sorted()
        // Guards both directions: a missing fixture would already throw in the
        // parameterized tests; an orphan fixture on disk would silently never
        // be parity-checked without this.
        #expect(onDisk == Self.fixtureNames.sorted())
        #expect(onDisk.count == 5)
    }

    @Test("exact python-reference parity (mergedRuns, slotsA, slotsB, minGapFps)", arguments: fixtureNames)
    func pythonReferenceParity(fixtureName: String) throws {
        let fixture = try RediffSpikeFixtureLoader.load(fixtureName)
        let result = runFixture(fixture)

        let expectedRuns = fixture.pythonReference.mergedRuns.map {
            RediffPrototype.Run(aStart: $0.aStart, bStart: $0.bStart, length: $0.length, errors: $0.errors)
        }
        #expect(result.mergedRuns == expectedRuns)
        #expect(result.minGapFpsA == fixture.pythonReference.minGapFpsA)
        #expect(result.minGapFpsB == fixture.pythonReference.minGapFpsB)
        expectSlotsMatchReference(result.slotsA, fixture.pythonReference.slotsA)
        expectSlotsMatchReference(result.slotsB, fixture.pythonReference.slotsB)
    }

    @Test("ground-truth tolerance windows match exactly one slot each, no extras", arguments: fixtureNames)
    func groundTruthWindows(fixtureName: String) throws {
        let fixture = try RediffSpikeFixtureLoader.load(fixtureName)
        // real-rotated-pair carries no absolute ground truth (parity-only).
        guard let groundTruth = fixture.groundTruth else { return }
        let result = runFixture(fixture)
        if let windows = groundTruth.slotsA {
            expectWindows(result.slotsA, windows, side: "A")
        }
        if let windows = groundTruth.slotsB {
            expectWindows(result.slotsB, windows, side: "B")
        }
    }

    private func expectWindows(
        _ slots: [RediffPrototype.Slot],
        _ windows: [RediffSpikeFixture.GroundTruthWindow],
        side: String,
        sourceLocation: SourceLocation = #_sourceLocation
    ) {
        // Count equality doubles as the "no extra slots >= minAdSeconds" check
        // (every emitted slot already clears minGapFps). Empty array = must be empty.
        #expect(
            slots.count == windows.count,
            "side \(side): expected \(windows.count) slots, got \(slots.map { ($0.startSeconds, $0.endSeconds) })",
            sourceLocation: sourceLocation
        )
        // Windows must match DISTINCT slots (a window ↔ slot bijection):
        // without the distinctness check, two windows matching the same slot
        // would pass while another slot goes unmatched.
        var matchedSlotIndices: Set<Int> = []
        for window in windows {
            let matches = slots.indices.filter {
                let slot = slots[$0]
                return window.startSecondsRange[0] <= slot.startSeconds
                    && slot.startSeconds <= window.startSecondsRange[1]
                    && window.endSecondsRange[0] <= slot.endSeconds
                    && slot.endSeconds <= window.endSecondsRange[1]
            }
            #expect(
                matches.count == 1,
                "side \(side): window \(window.startSecondsRange)-\(window.endSecondsRange) matched \(matches.count) slots",
                sourceLocation: sourceLocation
            )
            for index in matches {
                #expect(
                    !matchedSlotIndices.contains(index),
                    "side \(side): slot \(index) matched more than one window",
                    sourceLocation: sourceLocation
                )
                matchedSlotIndices.insert(index)
            }
        }
    }
}
