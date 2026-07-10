// RediffPrototype.swift
// SPIKE (playhead-xsdz.16): pure-Swift port of the fingerprint-rediff
// algorithm from scripts/l2f-dai-rediff.py (the authoritative reference).
//
// TEST-TARGET-ONLY: this file lives in PlayheadTests and NOTHING in
// production invokes it. No production dependencies — Foundation only,
// Sendable value types, side-effect-free functions (mimicking the
// enum-namespace style of Playhead/Services/AdDetection/SpliceSlotDisposition.swift).
//
// SEMANTICS (pinned)
// ------------------
// A = the device's PLAYED copy of an episode; B = a fresh re-fetch of the
// same enclosure. DAI hosts rotate ad fills between downloads, and the
// fresh copy's absolute timeline drifts up to ±180s vs the played copy —
// so absolute-offset math is FORBIDDEN. Everything flows through aligned
// constant-offset runs of matching chromaprint fingerprints:
//   * `slotsA` (the PRIMARY output) are the DAI ad spans the user actually
//     heard, expressed in the PLAYED timeline as the complement of the
//     UNION of run-covered A-intervals.
//   * `slotsB` are the fresh copy's inserted spans (python-reference
//     parity; what scripts/l2f-dai-rediff.py reports as adSlots).
//
// KNOWN SEMANTIC LIMITATIONS (inherited from the reference algorithm and
// pinned by tests, NOT fixed here — python-fixture parity is the spike's
// bar; the oracle integration must treat slotsA as HIGH-PRECISION but
// INCOMPLETE-RECALL):
//   * Cross-slot ad matches: DAI pools recycle creatives, so the SAME ad
//     can appear in both copies at different positions. The ad↔ad run then
//     covers the A-side ad region and it vanishes from slotsA. In the
//     extreme, a pure ORDER-swap of two fills is invisible on BOTH sides
//     (pinned: limitationOrderSwappedFillsAreInvisible).
//   * Equal-length rotation: mergeRuns treats an inter-run interval with
//     aGap == bGap (within gapDiffSlack, ~0.25s at chromaprint rate) as
//     re-encode noise and merges across it. Two different SAME-length
//     creatives swapped at the same boundary — e.g. two standard 30s
//     spots — are invisible on BOTH sides (pinned:
//     limitationEqualLengthRotationInvisible).
//
// PORT FIDELITY NOTES
// -------------------
// * findRuns / mergeRuns / gapsInB are EXACT ports of the python
//   find_runs / merge_runs / gaps_in_b (fixture parity tests pin this at
//   fingerprint-index level). Hamming distance = popcount(a XOR b) via
//   UInt32.nonzeroBitCount.
// * gapsInA is NEW (not in the python reference): runs sorted by B-start
//   may OVERLAP in A (two B regions matching the same A region), so the
//   naive sorted-adjacent-gap logic used for B would be WRONG on the A
//   side. Covered A-intervals [aStart, aStart+length) are UNIONED first;
//   a gap's flanking lengths are the merged covered-interval lengths on
//   each side (0 at head/tail). This matches the union-based reference
//   that produced the fixtures' "slotsA".
// * minGapFps mirrors python `max(1, int(round(minAd / secPerFp)))`.
//   Python's round() is BANKER'S rounding (round-half-to-even), so we use
//   .toNearestOrEven — NOT Swift's default away-from-zero .rounded().
// * Seconds fields are UNROUNDED here (python rounds to 2-3 decimals only
//   when serializing its report JSON); the parity tests absorb that
//   presentation-rounding with a half-of-last-decimal tolerance.
//
// COMPLEXITY: findRuns is near-linear O(|A| + |B|) for realistic
// fingerprints (anchor lists are short because 32-bit values rarely
// repeat); the pathological worst case (one value repeated everywhere)
// is O(|A|·|B|). Fine for a spike; no perf assertions here (PerfGate
// territory).

import Foundation

enum RediffPrototype {

    // MARK: - Value types

    /// A constant-offset aligned run. findRuns output is maximal and
    /// pointwise matched: for k in 0..<length,
    /// popcount(fpA[aStart+k] ^ fpB[bStart+k]) <= hammingTol. mergeRuns
    /// output weakens that to a SPAN: a merged run can fold in noise gaps
    /// (aGap ≈ bGap) whose fingerprints match on NEITHER side.
    /// `errors` is the summed Hamming distance across the matched
    /// fragments (folded noise gaps contribute nothing).
    struct Run: Sendable, Equatable, Hashable {
        let aStart: Int
        let bStart: Int
        let length: Int
        let errors: Int
    }

    /// A run-uncovered index range [startFp, endFp) on one side, plus the
    /// flanking covered lengths (in fingerprints) used for confidence.
    struct Gap: Sendable, Equatable {
        let startFp: Int
        let endFp: Int
        let leftFlankFps: Int
        let rightFlankFps: Int
    }

    /// A reported ad slot. Indices are fingerprint positions in the slot's
    /// OWN timeline (A for slotsA, B for slotsB); `endFp` is EXCLUSIVE.
    /// Seconds are derived with that timeline's secondsPerFp, unrounded.
    struct Slot: Sendable, Equatable {
        let startFp: Int
        let endFp: Int
        let leftRunFps: Int
        let rightRunFps: Int
        let startSeconds: Double
        let endSeconds: Double
        let durationSeconds: Double
        let confidence: Double
        let leftRunSeconds: Double
        let rightRunSeconds: Double
    }

    struct Result: Sendable, Equatable {
        let mergedRuns: [Run]
        /// Total merged-run coverage expressed in B-seconds (python parity:
        /// run lengths are B-lengths and the reference uses secondsPerFpB).
        let alignedSecondsB: Double
        /// NEW vs the python reference: merged-run B-span fingerprints over
        /// total B fingerprints (0 for empty B). The re-encode guard input
        /// for the oracle integration: a wholesale re-encode collapses this
        /// toward 0 while a normal DAI rotation stays near 1. Note merged
        /// spans INCLUDE noise gaps folded in by mergeRuns (same span
        /// semantics as alignedSecondsB / the python alignedSecondsB).
        let alignedFractionB: Double
        let minGapFpsA: Int
        let minGapFpsB: Int
        /// PRIMARY output: ad spans in the PLAYED (A) timeline.
        let slotsA: [Slot]
        /// Fresh-copy (B) spans, for python-reference parity.
        let slotsB: [Slot]
    }

    // MARK: - Alignment (exact port of python find_runs)

    /// Find maximal consistent-offset runs aligning B to A.
    ///
    /// Exact port of `find_runs`: inverted index of A fingerprint values →
    /// ascending A positions; scan B left-to-right skipping covered indices;
    /// for each exact-match anchor extend forward then backward (backward
    /// never crosses `coveredUntilB`); reject runs shorter than `minRunLen`;
    /// among a B-index's anchors keep the FIRST longest (strict `>` in
    /// anchor iteration order, ascending A position); after accepting a run
    /// resume scanning at its end.
    static func findRuns(
        fpA: [UInt32],
        fpB: [UInt32],
        hammingTol: Int = 2,
        minRunLen: Int = 8
    ) -> [Run] {
        var indexA: [UInt32: [Int]] = [:]
        indexA.reserveCapacity(fpA.count)
        for (j, value) in fpA.enumerated() {
            indexA[value, default: []].append(j)
        }

        var runs: [Run] = []
        var coveredUntilB = -1
        var i = 0
        while i < fpB.count {
            if i <= coveredUntilB {
                i += 1
                continue
            }
            guard let anchors = indexA[fpB[i]] else {
                i += 1
                continue
            }
            var best: Run?
            for a0 in anchors {
                // Extend forward from the anchor.
                var k = 0
                var errors = 0
                while a0 + k < fpA.count && i + k < fpB.count {
                    let d = (fpA[a0 + k] ^ fpB[i + k]).nonzeroBitCount
                    if d > hammingTol { break }
                    errors += d
                    k += 1
                }
                // Extend backward, never crossing the last accepted run.
                var back = 1
                while a0 - back >= 0 && i - back >= 0 && i - back > coveredUntilB {
                    let d = (fpA[a0 - back] ^ fpB[i - back]).nonzeroBitCount
                    if d > hammingTol { break }
                    errors += d
                    back += 1
                }
                back -= 1
                let length = k + back
                if length < minRunLen { continue }
                // FIRST longest wins: strictly longer replaces (length is
                // always >= 0, so the nil case always accepts).
                if length > (best?.length ?? -1) {
                    best = Run(aStart: a0 - back, bStart: i - back, length: length, errors: errors)
                }
            }
            guard let accepted = best else {
                i += 1
                continue
            }
            runs.append(accepted)
            coveredUntilB = accepted.bStart + accepted.length - 1
            i = coveredUntilB + 1
        }
        return runs
    }

    // MARK: - Run merging (exact port of python merge_runs)

    /// Merge runs that share approximately the same offset (aStart-bStart)
    /// AND whose B-gap is matched by an equal-sized A-gap (the inter-run
    /// interval is noise on BOTH sides, not an insertion). Merged length is
    /// measured in B (cur.bStart + cur.length - last.bStart); errors summed.
    static func mergeRuns(
        _ runs: [Run],
        offsetSlack: Int = 2,
        gapDiffSlack: Int = 2
    ) -> [Run] {
        guard !runs.isEmpty else { return runs }
        // Stable sort by bStart (index tiebreak keeps this deterministic
        // for arbitrary caller-supplied runs; find_runs output is already
        // strictly increasing in bStart).
        let sorted = runs.enumerated()
            .sorted { ($0.element.bStart, $0.offset) < ($1.element.bStart, $1.offset) }
            .map(\.element)
        var merged = [sorted[0]]
        for cur in sorted.dropFirst() {
            let last = merged[merged.count - 1]
            let lastOffset = last.aStart - last.bStart
            let curOffset = cur.aStart - cur.bStart
            let bGap = cur.bStart - (last.bStart + last.length)
            let aGap = cur.aStart - (last.aStart + last.length)
            // bGap is non-negative for find_runs output (sorted by bStart);
            // aGap can be negative for a different offset — never merged.
            if abs(lastOffset - curOffset) <= offsetSlack
                && bGap >= 0
                && aGap >= 0
                && abs(bGap - aGap) <= gapDiffSlack {
                merged[merged.count - 1] = Run(
                    aStart: last.aStart,
                    bStart: last.bStart,
                    length: cur.bStart + cur.length - last.bStart,
                    errors: last.errors + cur.errors
                )
            } else {
                merged.append(cur)
            }
        }
        return merged
    }

    // MARK: - Gap extraction

    /// Inserted-segments-in-B = B indices not covered by any run (exact
    /// port of python gaps_in_b, including the empty-runs whole-side branch
    /// and head/tail flank handling).
    static func gapsInB(runs: [Run], totalB: Int, minGapFps: Int) -> [Gap] {
        guard !runs.isEmpty else {
            return totalB >= minGapFps
                ? [Gap(startFp: 0, endFp: totalB, leftFlankFps: 0, rightFlankFps: 0)]
                : []
        }
        let sorted = runs.enumerated()
            .sorted { ($0.element.bStart, $0.offset) < ($1.element.bStart, $1.offset) }
            .map(\.element)
        var gaps: [Gap] = []
        let first = sorted[0]
        if first.bStart >= minGapFps {
            gaps.append(Gap(startFp: 0, endFp: first.bStart, leftFlankFps: 0, rightFlankFps: first.length))
        }
        for (left, right) in zip(sorted, sorted.dropFirst()) {
            let leftEnd = left.bStart + left.length
            if right.bStart - leftEnd >= minGapFps {
                gaps.append(Gap(
                    startFp: leftEnd,
                    endFp: right.bStart,
                    leftFlankFps: left.length,
                    rightFlankFps: right.length
                ))
            }
        }
        let last = sorted[sorted.count - 1]
        let tailStart = last.bStart + last.length
        if totalB - tailStart >= minGapFps {
            gaps.append(Gap(startFp: tailStart, endFp: totalB, leftFlankFps: last.length, rightFlankFps: 0))
        }
        return gaps
    }

    /// Removed-segments-in-A = A indices not covered by any run — slots in
    /// the PLAYED copy's timeline. NEW vs the python reference (see header):
    /// runs sorted by B-start may OVERLAP in A, so the covered A-intervals
    /// [aStart, aStart+length) are UNIONED (touching intervals coalesce)
    /// before taking the complement. A gap's flanks are the merged
    /// covered-interval lengths on each side (0 at head/tail).
    static func gapsInA(runs: [Run], totalA: Int, minGapFps: Int) -> [Gap] {
        guard !runs.isEmpty else {
            return totalA >= minGapFps
                ? [Gap(startFp: 0, endFp: totalA, leftFlankFps: 0, rightFlankFps: 0)]
                : []
        }
        let intervals = runs
            .map { (start: $0.aStart, end: $0.aStart + $0.length) }
            .sorted { ($0.start, $0.end) < ($1.start, $1.end) }
        var covered = [intervals[0]]
        for interval in intervals.dropFirst() {
            if interval.start <= covered[covered.count - 1].end {
                covered[covered.count - 1].end = max(covered[covered.count - 1].end, interval.end)
            } else {
                covered.append(interval)
            }
        }
        var gaps: [Gap] = []
        let first = covered[0]
        if first.start >= minGapFps {
            gaps.append(Gap(
                startFp: 0,
                endFp: first.start,
                leftFlankFps: 0,
                rightFlankFps: first.end - first.start
            ))
        }
        for (left, right) in zip(covered, covered.dropFirst()) {
            if right.start - left.end >= minGapFps {
                gaps.append(Gap(
                    startFp: left.end,
                    endFp: right.start,
                    leftFlankFps: left.end - left.start,
                    rightFlankFps: right.end - right.start
                ))
            }
        }
        let last = covered[covered.count - 1]
        if totalA - last.end >= minGapFps {
            gaps.append(Gap(
                startFp: last.end,
                endFp: totalA,
                leftFlankFps: last.end - last.start,
                rightFlankFps: 0
            ))
        }
        return gaps
    }

    // MARK: - Scoring / thresholds

    /// conf = 1 - exp(-min(left, right) / 60): a 60s flank on each side
    /// gives ~0.632, a 5-minute flank ~0.99, a missing flank exactly 0.
    /// Unrounded (the python reference rounds to 3 decimals only for JSON).
    static func confidence(leftRunSeconds: Double, rightRunSeconds: Double) -> Double {
        1 - exp(-min(leftRunSeconds, rightRunSeconds) / 60.0)
    }

    /// Python parity: max(1, int(round(minAdSeconds / secondsPerFp))) with
    /// BANKER'S rounding (round-half-to-even), matching python round().
    /// Pathological inputs fail LOUDLY via precondition rather than
    /// trapping in the opaque Double→Int conversion. (Python is loud for
    /// most of these too — ZeroDivisionError for secondsPerFp == 0,
    /// ValueError/OverflowError from round() for a NaN/infinite ratio —
    /// but silently returns 1 for a NEGATIVE or INFINITE secondsPerFp;
    /// we reject those as nonsense rates instead.)
    static func minGapFps(minAdSeconds: Double, secondsPerFp: Double) -> Int {
        precondition(
            secondsPerFp > 0 && secondsPerFp.isFinite,
            "secondsPerFp must be positive and finite (got \(secondsPerFp))"
        )
        let fps = (minAdSeconds / secondsPerFp).rounded(.toNearestOrEven)
        precondition(
            fps.isFinite && fps.magnitude < 0x1p63,
            "minGapFps not representable: \(minAdSeconds)s / \(secondsPerFp)s-per-fp rounds to \(fps)"
        )
        return max(1, Int(fps))
    }

    // MARK: - Top-level entry

    /// Align A (played copy) against B (fresh re-fetch) and report the
    /// uncovered spans on both sides. `slotsA` — the played-timeline DAI ad
    /// spans — is the PRIMARY output; `slotsB` mirrors the python
    /// reference's adSlots. Zero aligned runs is NOT an error here (unlike
    /// the python batch tool): each side then reports one whole-side slot
    /// when it clears its minGapFps, per the gaps functions' empty-runs
    /// branch.
    static func rediff(
        fingerprintA: [UInt32],
        secondsPerFpA: Double,
        fingerprintB: [UInt32],
        secondsPerFpB: Double,
        hammingTol: Int = 2,
        minRunLen: Int = 8,
        offsetSlack: Int = 2,
        gapDiffSlack: Int = 2,
        minAdSeconds: Double = 5.0
    ) -> Result {
        let merged = mergeRuns(
            findRuns(fpA: fingerprintA, fpB: fingerprintB, hammingTol: hammingTol, minRunLen: minRunLen),
            offsetSlack: offsetSlack,
            gapDiffSlack: gapDiffSlack
        )
        let minGapA = minGapFps(minAdSeconds: minAdSeconds, secondsPerFp: secondsPerFpA)
        let minGapB = minGapFps(minAdSeconds: minAdSeconds, secondsPerFp: secondsPerFpB)
        let slotsA = gapsInA(runs: merged, totalA: fingerprintA.count, minGapFps: minGapA)
            .map { slot(from: $0, secondsPerFp: secondsPerFpA) }
        let slotsB = gapsInB(runs: merged, totalB: fingerprintB.count, minGapFps: minGapB)
            .map { slot(from: $0, secondsPerFp: secondsPerFpB) }
        let alignedFps = merged.reduce(0) { $0 + $1.length }
        return Result(
            mergedRuns: merged,
            alignedSecondsB: Double(alignedFps) * secondsPerFpB,
            alignedFractionB: fingerprintB.isEmpty ? 0 : Double(alignedFps) / Double(fingerprintB.count),
            minGapFpsA: minGapA,
            minGapFpsB: minGapB,
            slotsA: slotsA,
            slotsB: slotsB
        )
    }

    /// Materialize a gap as a slot in its own timeline's seconds.
    private static func slot(from gap: Gap, secondsPerFp: Double) -> Slot {
        let leftSeconds = Double(gap.leftFlankFps) * secondsPerFp
        let rightSeconds = Double(gap.rightFlankFps) * secondsPerFp
        return Slot(
            startFp: gap.startFp,
            endFp: gap.endFp,
            leftRunFps: gap.leftFlankFps,
            rightRunFps: gap.rightFlankFps,
            startSeconds: Double(gap.startFp) * secondsPerFp,
            endSeconds: Double(gap.endFp) * secondsPerFp,
            durationSeconds: Double(gap.endFp - gap.startFp) * secondsPerFp,
            confidence: confidence(leftRunSeconds: leftSeconds, rightRunSeconds: rightSeconds),
            leftRunSeconds: leftSeconds,
            rightRunSeconds: rightSeconds
        )
    }
}
