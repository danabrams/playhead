// RediffSegmentRecoveryPhantomTests.swift
// playhead-3zxd — THE PHANTOM SLOT. `segmentDivergentSlots` DROPPED any found
// run whose A-span overlapped an already-accepted one, and the dropped run's
// A-span then fell through to `addGap` and shipped as a divergent region — an
// ad — on audio the aligner had just proven MATCHED.
//
// playhead-pyq7 measured the consequence over 31 synthetic pairs: 7 of 20
// emitted slots contained ZERO gold ad seconds, 100 % show, at widths 120.01 /
// 240.01 / 240.01 / 299.99 / 359.99 / 420.00 / 469.99 s. The only guard was
// `maxSlotSeconds` (480 s), measured exact — 470 s shipped, 540 s rejected. The
// worst shape fused a real 30 s ad to 299.99 s of show inside ONE slot.
//
// These slots ship as `markOnly`, which is a banner, and Dan's ruling is that
// the banner IS a skip affordance ("when it enters so I can skip"). A listener
// who taps skip on a phantom loses up to 470 s of episode.
//
// THE INVARIANT INSTALLED HERE, and why it is the right one:
//
//     the A-seconds inside an emitted slot covered by any FOUND run is ZERO.
//
// A run is a byte-verified region present in BOTH copies. So the invariant is
// not a threshold anybody chose — it is the definition of "divergent" applied
// to itself. It is also GOLD-FREE: no transcript, no second detector, nothing
// the day-0 path (which mints before a single second has been transcribed)
// cannot compute. That is what makes it checkable on real audio, where gold
// does not exist, and what makes the device a capture harness.
//
// HOW THE FIX MAKES IT TRUE — by construction, not by filtering. The greedy
// A-order accept now CLIPS an A-overlapping run to its uncovered tail instead
// of dropping it whole. Clipping is sound at the byte level (if
// `A[s..e) == B[t..t+e-s)` then `A[s+k..e) == B[t+k..t+e-s)`), and it makes the
// accepted set's A-coverage equal the UNION of every found run's A-span. The
// inter-run gaps are then exactly the A-regions no run covers, which is the
// invariant restated. Nothing is dropped, so nothing is mislabelled — and the
// recall goes UP, not down: the pyq7 worst shape (a real 30 s ad fused to
// 299.99 s of show) resolves into the real 30 s ad on its own.
//
// MEASURED, same harness and same gold as pyq7, 31 synthetic pairs:
//
//                                   before        after
//   Σ showEatenSeconds            2449.9965 s   0.0000 s
//   pairs eating any show           8 of 31       0 of 31
//   emitted slots with NO gold ad   7             0
//   phantom widths (s)              120.01, 240.01, 240.01, 299.99,
//                                   359.99, 420.00, 469.99   -> none
//   Σ adLeftSeconds                 —             ≤ 0.0008 s (the 4-byte
//                                                 header bleed, safe direction)
//
// AND THE SEGMENTED COVERAGE RISES, because no run is discarded any more —
// `segmentedChainedFractionB`, per pair:
//
//   A/insertedInB30/x2      0.6154 -> 0.9231   (segChained 2 -> 3)
//   B/insertedInB/tail470s  0.6429 -> 0.9786   (segChained 1 -> 2)
//   D/inserted+removed      0.6452 -> 0.9678   (segChained 2 -> 3)
//   A/removedInB30/x3       1.0000 -> 1.0000   (no A-overlap; UNCHANGED)
//
// That last row is here because the first draft of this note cited it as
// "0.5000 -> 1.0000". 0.5000 is `chainedFractionB` — the STRICT chain's
// fraction, which this fix does not touch — and reading it as the segmented one
// is the exact confusion this codebase keeps paying for. The segmented figure
// for that pair was already 1.0000 before the fix.
//
// The rise is honest (each A-region is counted once, and it is genuinely
// aligned) but it is a real behaviour change beyond the phantom: the gate's
// re-encode floor is `segmentedChainedFractionB >= minAlignedFractionB` (0.5),
// so a fetch sitting just under 0.5 before could now clear it and be accepted
// where it was previously discarded as a re-encode. No pair here crosses that
// line — the lowest segmented fraction in the set is 0.6154 before and after —
// so the effect is unobserved rather than shown to be absent.
//
// FRAGMENT MERGE IS A SEPARATE, BOUNDED EFFECT — read the numbers with it in
// mind. `RediffSlotOwnership.mergedAndCapped` joins two slots separated by
// ≤ `fragmentMergeGapSeconds` (3 s), and the thing separating two SHIPPABLE
// gaps is always a run accepted WHOLE. At the default `minRunBytes` (65536)
// such a run is 4.1 s at 128 kbps CBR, so it cannot be joined over; at
// ≥192 kbps it is under 3 s and can be. That is a second, much smaller instance
// of the same class, bounded at 3 s per join versus this bead's minutes, and it
// is filed separately (playhead-yzra) rather than fixed here. The synthetic
// pairs below are all 128 kbps, so no merge fires and the post-merge assertions
// are exact.
//
// "ACCEPTED WHOLE" IS LOAD-BEARING AND WAS ADDED IN R2 REVIEW. An earlier draft
// of this note said "the thing separating two gaps is an accepted RUN", which
// this bead's own fix falsified: a CLIPPED run is `bytes - trim`, floored only
// by `guard aEnd > globalAEnd` — one byte — so the ≥ `minRunBytes` premise no
// longer holds of the accepted set. The conclusion survives for a different
// reason: a clipped run's `aStart` IS the previous accepted run's `aEnd`, so the
// gap before it has zero A-width and `minAdSeconds` drops it, and a clipped run
// can therefore never be what separates two shippable slots. § 5d pins that.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-3zxd — a byte-matched run is never reported as an ad")
struct RediffSegmentRecoveryPhantomTests {

    private static let ad30 = SegmentRecoveryFixture.frames(seconds: 30)
    private static let content240 = SegmentRecoveryFixture.frames(seconds: 240)
    private static let content300 = SegmentRecoveryFixture.frames(seconds: 300)

    /// Floating-point slack only. The invariant is EXACT — gaps are the
    /// set-complement of the accepted runs' A-coverage, so an honest alignment
    /// scores a hard 0 and anything that fails does so by seconds or minutes,
    /// not by an ulp. 1 ms is four orders below the 30 s ad these fixtures use.
    private static let epsilonSeconds = 0.001

    // MARK: - Probe helpers

    /// A-seconds of `slot` covered by a FOUND run — the invariant's quantity,
    /// read through the production helper so the test and the shipped
    /// instrumentation cannot measure two different things.
    private func alignedSeconds(
        _ start: Double, _ end: Double, _ alignment: RediffByteAligner.Alignment
    ) -> Double {
        RediffByteAligner.alignedSeconds(
            in: TimeRange(start: start, end: end), runASpans: alignment.foundRunASpans)
    }

    /// Every slot this alignment can SHIP, as `(label, start, end)` — pre-merge
    /// (the aligner's own emission) and post-merge (what reaches a banner).
    ///
    /// `alignment.slots` is included ONLY when the chain is monotonic-clean, and
    /// that exemption is a reachability fact, not a convenience. `chainRuns`
    /// picks a max-bytes monotonic SUBSEQUENCE, so on a non-monotonic alignment
    /// it too drops runs and their A-spans too fall into `slots` as gaps — the
    /// same shape this bead fixes. Nothing consumes them: `alignment.slots` has
    /// exactly one production reader, `gateAndDiffBytes`, and that read sits
    /// below a `guard alignment.monotonicClean`, which returns
    /// `.rejectedNonMonotonic` (flag off) or diverts to the SEGMENTED list (flag
    /// on) before reaching it. `strictSlotsAreUnreachableWhenTheChainDropsRuns`
    /// below pins that so the exemption cannot quietly become a hole.
    ///
    /// It is also why the fix is NOT applied to `chainRuns`: `align`'s strict
    /// path is a byte-exact port of `scripts/l2f-mp3-forensics.py cmd_align`,
    /// pinned by `RediffByteAlignerParityTests`, and a chain that dropped runs
    /// is rejected wholesale by contract rather than trusted and repaired.
    private func emittedSlots(
        _ alignment: RediffByteAligner.Alignment,
        config: RediffSlotOwnership.Configuration = .default
    ) -> [(label: String, start: Double, end: Double)] {
        var out: [(String, Double, Double)] = []
        var arms: [(String, [RediffByteAligner.Slot])] = [("segmented", alignment.segmentedSlots)]
        if alignment.monotonicClean { arms.append(("strict", alignment.slots)) }
        for (arm, slots) in arms {
            for slot in slots where slot.aSeconds >= config.minAdSeconds {
                out.append(("\(arm)/pre-merge", slot.aStartSeconds, slot.aEndSeconds))
            }
        }
        for recover in [false, true] {
            guard case .accepted(let acceptance) = RediffSlotOwnership.gateAndDiffBytes(
                alignment: alignment, config: config, recoverNonMonotonicSegments: recover
            ) else { continue }
            for slot in acceptance.playedSlots {
                out.append(("gate(recover=\(recover))", slot.startSeconds, slot.endSeconds))
            }
        }
        return out
    }

    /// THE RAIL. Assert the invariant over every slot either arm would emit.
    private func expectNoSlotContainsMatchedAudio(
        _ pair: SegmentRecoveryFixture.Pair,
        sourceLocation: SourceLocation = #_sourceLocation
    ) {
        let alignment = RediffByteAligner.align(aData: pair.aData, bData: pair.bData)
        #expect(
            alignment.runsFound > 0,
            "\(pair.name): VACUITY — the aligner found no runs, so this fixture proves nothing",
            sourceLocation: sourceLocation
        )
        for slot in emittedSlots(alignment) {
            let matched = alignedSeconds(slot.start, slot.end, alignment)
            #expect(
                matched <= Self.epsilonSeconds,
                """
                \(pair.name): \(slot.label) slot \(slot.start)..\(slot.end) \
                (\(slot.end - slot.start) s) contains \(matched) s of audio the \
                aligner PROVED matched — that is show reported as an ad
                """,
                sourceLocation: sourceLocation
            )
        }
    }

    // MARK: - 1. The invariant, over the pyq7 family matrix

    @Test("no emitted slot contains byte-matched audio — break-kind matrix (pyq7 family A)")
    func invariantHoldsAcrossBreakKinds() {
        let kinds: [(String, SegmentRecoveryFixture.Break)] = [
            ("removedInB30", .removedInB(Self.ad30)),
            ("insertedInB30", .insertedInB(Self.ad30)),
            ("replacedEqual30", .replaced(a: Self.ad30, b: Self.ad30)),
            ("replacedShorterB", .replaced(a: SegmentRecoveryFixture.frames(seconds: 60), b: Self.ad30)),
            ("replacedLongerB", .replaced(a: Self.ad30, b: SegmentRecoveryFixture.frames(seconds: 60)))
        ]
        for (label, brk) in kinds {
            for breakCount in 1...3 {
                expectNoSlotContainsMatchedAudio(SegmentRecoveryFixture.build(
                    name: "3zxd/A/\(label)/x\(breakCount)",
                    contentFrames: Array(repeating: Self.content240, count: breakCount + 1),
                    breaks: Array(repeating: brk, count: breakCount)
                ))
            }
        }
    }

    @Test("no emitted slot contains byte-matched audio — mixed multi-break structures (pyq7 family D)")
    func invariantHoldsAcrossMixedStructures() {
        let ad60 = SegmentRecoveryFixture.frames(seconds: 60)
        let cases: [(String, [SegmentRecoveryFixture.Break])] = [
            ("removed+replaced", [.removedInB(Self.ad30), .replaced(a: Self.ad30, b: ad60)]),
            ("replaced+inserted", [.replaced(a: Self.ad30, b: ad60), .insertedInB(Self.ad30)]),
            ("inserted+removed", [.insertedInB(Self.ad30), .removedInB(Self.ad30)]),
            ("inserted+inserted", [.insertedInB(Self.ad30), .insertedInB(Self.ad30)]),
            ("three-replaced-mixed", [
                .replaced(a: Self.ad30, b: ad60),
                .replaced(a: ad60, b: Self.ad30),
                .replaced(a: Self.ad30, b: Self.ad30)
            ])
        ]
        for (name, breaks) in cases {
            expectNoSlotContainsMatchedAudio(SegmentRecoveryFixture.build(
                name: "3zxd/D/\(name)",
                contentFrames: Array(repeating: Self.content300, count: breaks.count + 1),
                breaks: breaks
            ))
        }
    }

    @Test("no emitted slot contains byte-matched audio — pre-roll / post-roll (pyq7 family C)")
    func invariantHoldsAtOuterEdges() {
        let cases: [(String, [Int], [SegmentRecoveryFixture.Break])] = [
            ("preroll-removed", [0, Self.content300], [.removedInB(Self.ad30)]),
            ("postroll-removed", [Self.content300, 0], [.removedInB(Self.ad30)]),
            ("preroll-inserted", [0, Self.content300], [.insertedInB(Self.ad30)]),
            ("postroll-inserted", [Self.content300, 0], [.insertedInB(Self.ad30)]),
            ("pre+mid+post", [0, Self.content300, Self.content300, 0],
             [.removedInB(Self.ad30), .removedInB(Self.ad30), .removedInB(Self.ad30)])
        ]
        for (name, contentFrames, breaks) in cases {
            expectNoSlotContainsMatchedAudio(SegmentRecoveryFixture.build(
                name: "3zxd/C/\(name)", contentFrames: contentFrames, breaks: breaks))
        }
    }

    // MARK: - 2. The exact pyq7 phantom shapes, named

    @Test("an inserted-in-B break emits NO slot at all — the played copy gained no ad")
    func insertedInBEmitsNothing() {
        // A gains nothing and loses nothing; B simply carries an extra break.
        // There is no ad ANYWHERE in the played copy, so the only correct
        // emission is the empty one. Pre-3zxd this shipped the whole following
        // content block as one slot (pyq7 family A: 1 phantom, > 100 s wide).
        let pair = SegmentRecoveryFixture.build(
            name: "3zxd/insertedInB/x2",
            contentFrames: Array(repeating: Self.content240, count: 3),
            breaks: Array(repeating: .insertedInB(Self.ad30), count: 2)
        )
        #expect(pair.goldAdSpans.isEmpty, "control: an inserted-in-B break leaves NO ad in A")
        let alignment = RediffByteAligner.align(aData: pair.aData, bData: pair.bData)
        #expect(!alignment.monotonicClean, "an inserted-in-B break IS what drives the chain non-monotonic")
        #expect(alignment.segmentedRunsAOverlapping >= 1,
                "the defect's OPPORTUNITY: at least one run's A-span overlapped an accepted one")

        let outcome = RediffSlotOwnership.gateAndDiffBytes(
            alignment: alignment, recoverNonMonotonicSegments: true)
        guard case .accepted(let acceptance) = outcome else { return }
        #expect(acceptance.playedSlots.isEmpty,
                "no ad exists in the played copy, so nothing may be emitted — got \(acceptance.playedSlots)")
    }

    @Test("the pyq7 worst shape — a real ad fused to 300 s of show — resolves to the real ad alone")
    func fusedInsertedThenRemovedEmitsOnlyTheRealAd() throws {
        // pyq7 family D `inserted+removed`: ONE emitted slot carrying a genuine
        // 30 s ad AND 299.99 s of show, with an inner start edge 300 s early —
        // indistinguishable at the mint from an exact slot. The fix does not
        // merely reject it; it recovers the real ad, because the clipped run
        // reinstates the boundary the drop had erased.
        let pair = SegmentRecoveryFixture.build(
            name: "3zxd/D/inserted+removed",
            contentFrames: Array(repeating: Self.content300, count: 3),
            breaks: [.insertedInB(Self.ad30), .removedInB(Self.ad30)]
        )
        let gold = try #require(pair.goldAdSpans.first)
        #expect(pair.goldAdSpans.count == 1, "exactly one ad exists in the played copy")

        let alignment = RediffByteAligner.align(aData: pair.aData, bData: pair.bData)
        guard case .accepted(let acceptance) = RediffSlotOwnership.gateAndDiffBytes(
            alignment: alignment, recoverNonMonotonicSegments: true
        ) else {
            Issue.record("expected the recovery arm to accept"); return
        }
        #expect(acceptance.playedSlots.count == 1)
        let slot = try #require(acceptance.playedSlots.first)
        // Edge budget 10 ms: the only residual is the shared 4-byte CBR frame
        // header the greedy extension carries across the splice (4/417 of a
        // 26 ms frame ≈ 0.00025 s), which pyq7 measured and which errs in the
        // direction that leaves ad rather than eating show.
        #expect(abs(slot.startSeconds - gold.start) < 0.010,
                "start \(slot.startSeconds) vs gold \(gold.start) — pre-3zxd this was 300 s early")
        #expect(abs(slot.endSeconds - gold.end) < 0.010,
                "end \(slot.endSeconds) vs gold \(gold.end)")
        #expect(slot.durationSeconds < 31.0,
                "the slot is the 30 s ad, not the ad fused to a content block — got \(slot.durationSeconds) s")
    }

    // MARK: - 3. The duration cap is no longer what protects the listener

    @Test("phantom width sweep: nothing ships at ANY tail width, including well under the 480 s cap")
    func nothingShipsBelowOrAboveTheDurationCap() {
        // pyq7 family B drove an inserted-in-B break against a growing tail and
        // found `maxSlotSeconds` exact: 120/240/360/420/470 s ACCEPTED (pure
        // show, every second of it), 540/720 s rejected. The cap was the only
        // thing between a phantom and a tap. After the fix the whole sweep is
        // empty, which is the point: the protection is now the invariant, not a
        // number that a longer content block walks straight under.
        for tailSeconds in [120.0, 240.0, 360.0, 420.0, 470.0, 540.0, 720.0] {
            let pair = SegmentRecoveryFixture.build(
                name: "3zxd/B/tail\(Int(tailSeconds))s",
                contentFrames: [
                    SegmentRecoveryFixture.frames(seconds: 900),
                    SegmentRecoveryFixture.frames(seconds: tailSeconds)
                ],
                breaks: [.insertedInB(Self.ad30)]
            )
            #expect(pair.goldAdSpans.isEmpty, "control: no ad exists in the played copy")
            let alignment = RediffByteAligner.align(aData: pair.aData, bData: pair.bData)
            let outcome = RediffSlotOwnership.gateAndDiffBytes(
                alignment: alignment, recoverNonMonotonicSegments: true)
            if case .accepted(let acceptance) = outcome {
                #expect(acceptance.playedSlots.isEmpty,
                        "tail=\(tailSeconds)s shipped \(acceptance.playedSlots.count) pure-show slot(s)")
            }
        }
    }

    // MARK: - 4. No recall regression on the shapes 9s6q was built to unlock

    @Test("removed-in-B multi-break pairs still recover EXACTLY — one slot per ad, no show eaten")
    func removedInBStillRecoversExactly() throws {
        for breakCount in 1...3 {
            let pair = SegmentRecoveryFixture.build(
                name: "3zxd/removedInB/x\(breakCount)",
                contentFrames: Array(repeating: Self.content240, count: breakCount + 1),
                breaks: Array(repeating: .removedInB(Self.ad30), count: breakCount)
            )
            let alignment = RediffByteAligner.align(aData: pair.aData, bData: pair.bData)
            guard case .accepted(let acceptance) = RediffSlotOwnership.gateAndDiffBytes(
                alignment: alignment, recoverNonMonotonicSegments: true
            ) else {
                Issue.record("x\(breakCount): recovery arm must still accept"); return
            }
            #expect(acceptance.playedSlots.count == breakCount,
                    "x\(breakCount): one slot per rotated ad")
            for (index, slot) in acceptance.playedSlots.enumerated() {
                let gold = try #require(pair.goldAdSpans[safeIndex: index])
                #expect(abs(slot.startSeconds - gold.start) < 0.010)
                #expect(abs(slot.endSeconds - gold.end) < 0.010)
            }
        }
    }

    // MARK: - 5. The instrumentation reports the mechanism honestly

    @Test("the opportunity counter and the recovered-seconds figure size the averted damage")
    func instrumentationSizesTheAvertedDamage() {
        // 900 s + a 470 s tail: pyq7's widest SHIPPABLE phantom (469.99 s, 100 %
        // show, accepted because it sits just under the 480 s cap). The
        // recovered-seconds figure must account for a tail of that order — it
        // is the A-seconds of matched audio a pre-3zxd build would have left
        // uncovered and therefore reported as an ad.
        let pair = SegmentRecoveryFixture.build(
            name: "3zxd/instrumentation/tail470",
            contentFrames: [
                SegmentRecoveryFixture.frames(seconds: 900),
                SegmentRecoveryFixture.frames(seconds: 470)
            ],
            breaks: [.insertedInB(Self.ad30)]
        )
        let alignment = RediffByteAligner.align(aData: pair.aData, bData: pair.bData)
        #expect(alignment.foundRunASpans.count == alignment.runsFound,
                "one A-span per found run — the diagnostic describes THIS alignment")
        #expect(alignment.segmentedRunsAOverlapping == 1,
                "exactly one run had its A-span partly covered — got \(alignment.segmentedRunsAOverlapping)")
        #expect(alignment.segmentedOverlapSecondsRecovered > 460,
                "the clipped tail is the ~470 s block pyq7 measured shipping as an ad — got \(alignment.segmentedOverlapSecondsRecovered)")
        #expect(alignment.segmentedOverlapSecondsRecovered < 471)
    }

    /// R3 REVIEW (F5) — THE AVERTED-DAMAGE FIGURE WAS A SUM OF CLIPPED TAILS,
    /// AND THAT IS NOT THE QUANTITY ITS NAME CLAIMS.
    ///
    /// `overlapSecondsRecovered` is persisted as `lastOverlapSecondsRecovered`
    /// and read off a device pull as "the show a pre-3zxd build would have
    /// banner-marked on this episode". It was accumulated one clipped tail at a
    /// time, which is right for ONE overlap and wrong the moment two chain —
    /// the ordinary shape for two inserted-in-B breaks, i.e. exactly the
    /// multi-break population playhead-9s6q exists to recover. The pre-3zxd rule
    /// DROPPED the first overlapper and then ACCEPTED the next run whole,
    /// because that run's `aStart` sits past a cursor the drop never advanced.
    /// Only the first tail was ever reported as an ad; the tail sum counted
    /// both and inflated the averted damage by an entire content block.
    ///
    /// The counterfactual is computed here from the PRE-3zxd rule itself,
    /// written out longhand, so the rail cannot agree with production by sharing
    /// its arithmetic. The final expectation is the discriminating one: the
    /// naive tail sum is asserted to be STRICTLY LARGER than the truth on this
    /// fixture, so a revert to it cannot pass.
    ///
    /// Hand-built runs, for the reason § 5c gives: a real pair's only A-overlap
    /// is the 4-byte CBR header bleed, which is microseconds wide — the error
    /// would be real but unmeasurable. Here the blocks are hundreds of frames.
    @Test("the recovered-seconds figure is what the DROPPING rule would have lost, not a sum of tails")
    func recoveredSecondsIsTheDifferenceInCoverageNotTheSumOfTails() {
        let frame = SyntheticMP3.frameLength
        let parsedA = RediffByteAligner.parse(
            SyntheticMP3.file(SyntheticMP3.frames(count: 3000, seed: 0xF5_0001)))
        let parsedB = RediffByteAligner.parse(
            SyntheticMP3.file(SyntheticMP3.frames(count: 3000, seed: 0xF5_0002)))
        func run(_ span: Range<Int>, bStart: Int) -> RediffByteAligner.Run {
            RediffByteAligner.Run(
                aStart: span.lowerBound * frame,
                bStart: bStart,
                bytes: (span.upperBound - span.lowerBound) * frame
            )
        }
        // TWO CHAINED CLIPS. Under the dropping rule: run 1 accepted [0,400);
        // run 2 starts at 300 → DROPPED; run 3 starts at 600, which is past the
        // un-advanced cursor 400 → ACCEPTED WHOLE. So the only region that rule
        // left uncovered-but-run-covered is [400,600) — 200 frames.
        let runs = [
            run(0..<400, bStart: 0),
            run(300..<700, bStart: 500_000),
            run(600..<1000, bStart: 900_000),
            run(1400..<1800, bStart: 1_300_000)
        ]
        let recovery = RediffByteAligner.segmentDivergentSlots(
            runs: runs, pa: parsedA, pb: parsedB,
            bAudioBytes: max(1, parsedB.sizeBytes - parsedB.leadingID3Bytes))
        #expect(recovery.runsAOverlapping == 2,
                "control: the fixture must stage TWO chained clips — got \(recovery.runsAOverlapping)")

        func seconds(_ fromFrame: Int, _ toFrame: Int) -> Double {
            RediffByteAligner.timeAt(parsedA, byteOffset: toFrame * frame)
                - RediffByteAligner.timeAt(parsedA, byteOffset: fromFrame * frame)
        }
        // THE COUNTERFACTUAL, longhand: the pre-3zxd accept rule, and the
        // A-seconds it covered.
        var droppingRuleCovered = 0.0
        var droppingRuleAEnd = -1
        for one in runs.sorted(by: { $0.aStart < $1.aStart }) where one.aStart >= droppingRuleAEnd {
            droppingRuleCovered +=
                RediffByteAligner.timeAt(parsedA, byteOffset: one.aStart + one.bytes)
                    - RediffByteAligner.timeAt(parsedA, byteOffset: one.aStart)
            droppingRuleAEnd = one.aStart + one.bytes
        }
        #expect(abs(droppingRuleCovered - seconds(0, 1200)) < Self.epsilonSeconds,
                "control: the dropping rule covers 1200 frames — got \(droppingRuleCovered) s")
        // This rule's coverage is the found-run union, 1400 frames (§ 5c pins
        // that independently through `chainedFractionB`).
        let truth = seconds(0, 1400) - droppingRuleCovered

        #expect(abs(recovery.overlapSecondsRecovered - truth) < Self.epsilonSeconds,
                """
                averted damage must be the 200 frames [400,600) the dropping rule \
                lost — expected \(truth) s, got \(recovery.overlapSecondsRecovered) s
                """)
        #expect(abs(recovery.overlapSecondsRecovered - seconds(400, 600)) < Self.epsilonSeconds,
                "…which is exactly the region no accepted run of that rule covered")

        // THE DISCRIMINATOR. The superseded formula — Σ clipped tails — is
        // [400,700) + [700,1000) = 600 frames, three times the truth. Without
        // this the rail would pass on a revert.
        let sumOfClippedTails = seconds(400, 700) + seconds(700, 1000)
        #expect(sumOfClippedTails > recovery.overlapSecondsRecovered + 1.0,
                "control: the two formulas must actually disagree on this fixture")
    }

    @Test("a monotonic-clean alignment reports zero opportunity — segmenting never ran")
    func monotonicCleanReportsNoOpportunity() {
        let content = SyntheticMP3.frames(count: 60, seed: 0x3D_0001)
        let separator = SyntheticMP3.id3v2(payloadBytes: 32)
        var head = Array(content[0..<30])
        SyntheticMP3.pinTailByte(&head, to: 0xAA)
        var ad = SyntheticMP3.frames(count: 250, seed: 0x3D_0002)
        SyntheticMP3.pinTailByte(&ad, to: 0x55)
        let aData = SyntheticMP3.file(head + [separator] + ad + Array(content[30...]))
        let bData = SyntheticMP3.file(head + Array(content[30...]))
        let alignment = RediffByteAligner.align(
            aData: aData, bData: bData, config: SyntheticMP3.smallRunConfig)
        #expect(alignment.monotonicClean)
        #expect(alignment.segmentedRunsAOverlapping == 0)
        #expect(alignment.segmentedOverlapSecondsRecovered == 0)
        #expect(alignment.foundRunASpans.count == alignment.runsFound)
    }

    // MARK: - 5b. The accept rule itself, over run shapes bytes cannot stage

    /// The greedy accept has THREE branches and a real A/B pair only exercises
    /// two of them: accept-whole and clip. The third — a run whose A-span lies
    /// WHOLLY inside coverage already accepted — needs two runs at different
    /// byte deltas nested inside one another, which `byteRuns` prunes for the
    /// same-delta case and which no plausible stitch produces for the other. So
    /// it is driven directly, against a real parsed MP3 for the byte→time map.
    ///
    /// It is not decoration. Without the containment guard the clip arithmetic
    /// runs on a run that ends BEHIND the cursor: `bytes` goes negative and
    /// `globalAEnd` walks BACKWARD, and the next gap is then computed from a
    /// rewound cursor — emitting a slot that overlaps a run by tens of thousands
    /// of bytes. That is the same phantom this bead removes, arriving by a
    /// different door.
    @Test("the accept rule holds for all three run shapes: whole, clipped, and wholly contained")
    func acceptRuleHandlesContainedRunsWithoutRewindingTheCursor() {
        let frame = SyntheticMP3.frameLength
        let parsedA = RediffByteAligner.parse(
            SyntheticMP3.file(SyntheticMP3.frames(count: 3000, seed: 0xACC_0001)))
        let parsedB = RediffByteAligner.parse(
            SyntheticMP3.file(SyntheticMP3.frames(count: 3000, seed: 0xACC_0002)))

        func run(_ aFrames: ClosedRange<Int>, bStart: Int) -> RediffByteAligner.Run {
            RediffByteAligner.Run(
                aStart: aFrames.lowerBound * frame,
                bStart: bStart,
                bytes: (aFrames.upperBound - aFrames.lowerBound) * frame
            )
        }
        let runs = [
            run(0...400, bStart: 0),            // accepted whole
            run(50...100, bStart: 500_000),     // WHOLLY CONTAINED in the first
            run(380...800, bStart: 700_000),    // partial overlap → clipped
            run(1000...1500, bStart: 900_000)   // accepted whole
        ]
        let recovery = RediffByteAligner.segmentDivergentSlots(
            runs: runs,
            pa: parsedA,
            pb: parsedB,
            bAudioBytes: max(1, parsedB.sizeBytes - parsedB.leadingID3Bytes)
        )

        let runASpans = runs.map {
            TimeRange(
                start: RediffByteAligner.timeAt(parsedA, byteOffset: $0.aStart),
                end: RediffByteAligner.timeAt(parsedA, byteOffset: $0.aStart + $0.bytes)
            )
        }
        for slot in recovery.slots {
            let matched = RediffByteAligner.alignedSeconds(
                in: TimeRange(start: slot.aStartSeconds, end: slot.aEndSeconds), runASpans: runASpans)
            #expect(
                matched <= Self.epsilonSeconds,
                """
                slot \(slot.aStartSeconds)..\(slot.aEndSeconds) contains \(matched) s of \
                byte-matched audio — the cursor rewound
                """
            )
            #expect(slot.aEndSeconds >= slot.aStartSeconds, "a slot may never have negative width")
        }
        // The contained run contributes NO new A-region, so it is absorbed and
        // — unlike the clipped one — is NOT counted as an opportunity: dropping
        // it reports nothing as divergent, because an accepted run already
        // covers its whole span.
        #expect(recovery.runsChained == 3, "whole + clipped + whole; the contained one is absorbed")
        #expect(recovery.runsAOverlapping == 1, "only the CLIPPED run is an opportunity")
        #expect(recovery.overlapSecondsRecovered > 0, "…and its kept tail is real A-seconds")
        // VACUITY: the fixture must actually produce gaps for the loop above to
        // have examined anything. Frames 800–1000 and the tail past 1500 are
        // covered by no run, so two real slots are expected.
        #expect(recovery.slots.count >= 2, "the fixture must emit slots — got \(recovery.slots.count)")
    }

    // MARK: - 5c. The accept rule's stated POST-CONDITION, asserted

    /// THE OTHER HALF OF THE RAIL. Everything above forbids matched audio from
    /// landing INSIDE an emitted slot. Nothing above requires run-free audio to
    /// land inside one — so the rail was ONE-SIDED, and any change that merely
    /// SHRINKS the emitted set passed it. Three arithmetic mutants survived the
    /// whole delivered suite on that hole (R1 measured each at `GATE_EXIT=0`):
    ///
    ///   * clip by `k + 1` — a 1-byte A-gap, ≈ 2.4 µs, harmless but unpinned;
    ///   * the clipped run keeps its FULL `bytes` — it then claims `trim` bytes
    ///     it never verified, `Σ accepted bytes` stops being the A-union, and
    ///     the following gap starts late, so DIVERGENT AUDIO IS SWALLOWED. This
    ///     is the serious one, and it is invisible at `trim = 4` but grows in
    ///     proportion to `trim`;
    ///   * `globalAEnd` not advanced on the clip branch — needs two CONSECUTIVE
    ///     A-overlaps, which no A/B fixture in this file stages.
    ///
    /// The assertion is not invented for the mutants: it is
    /// `segmentDivergentSlots`'s own documented post-condition, quoted from the
    /// header above the accept loop —
    ///
    ///     the accepted set's A-coverage == the UNION of every found run's
    ///     A-span
    ///
    /// — plus its converse, which is what makes the inter-run gaps *exactly*
    /// the A-regions no run covers rather than merely a subset of them.
    ///
    /// It is driven with hand-built runs for the same reason § 5b is: a real
    /// pair's only A-overlap is the 4-byte CBR frame header the greedy
    /// extension bleeds across a splice, so every length error a synthetic pair
    /// can express is microseconds wide. Here the overlaps are 100 frames, at a
    /// magnitude a slot could actually be made of, and there are TWO of them
    /// back to back so the second clip runs against a cursor the first moved.
    @Test("accepted A-coverage EQUALS the found-run union, and run-free audio still ships")
    func acceptedCoverageEqualsFoundRunUnionAndGapsStillShip() {
        let frame = SyntheticMP3.frameLength
        let parsedA = RediffByteAligner.parse(
            SyntheticMP3.file(SyntheticMP3.frames(count: 3000, seed: 0x81_0001)))
        let parsedB = RediffByteAligner.parse(
            SyntheticMP3.file(SyntheticMP3.frames(count: 3000, seed: 0x81_0002)))
        func run(_ span: Range<Int>, bStart: Int) -> RediffByteAligner.Run {
            RediffByteAligner.Run(
                aStart: span.lowerBound * frame,
                bStart: bStart,
                bytes: (span.upperBound - span.lowerBound) * frame
            )
        }
        let runs = [
            run(0..<400, bStart: 0),
            run(300..<700, bStart: 500_000),      // clipped against run 1
            run(600..<1000, bStart: 900_000),     // clipped against run 2's TAIL
            run(1400..<1800, bStart: 1_300_000)
        ]
        let bAudioBytes = max(1, parsedB.sizeBytes - parsedB.leadingID3Bytes)
        let recovery = RediffByteAligner.segmentDivergentSlots(
            runs: runs, pa: parsedA, pb: parsedB, bAudioBytes: bAudioBytes)

        // CONTROL: the fixture must actually stage the shape under test. Two
        // consecutive clips, and a gap for the containment assertion to find —
        // without these a future change could satisfy the expectations below by
        // doing nothing at all.
        #expect(recovery.runsAOverlapping == 2,
                "the fixture must stage TWO consecutive clips — got \(recovery.runsAOverlapping)")
        #expect(recovery.slots.count >= 2, "the fixture must emit slots — got \(recovery.slots.count)")

        // The union, computed here by a sweep that shares no code with the
        // production accept loop.
        var unionBytes = 0
        var cursor = -1
        for one in runs.sorted(by: { $0.aStart < $1.aStart }) {
            let end = one.aStart + one.bytes
            guard end > cursor else { continue }
            unionBytes += end - max(one.aStart, cursor)
            cursor = end
        }
        #expect(unionBytes == 1400 * frame, "control: the union is 1400 frames of A")
        // `chainedFractionB` is `Σ accepted bytes / bAudioBytes`, and it is the
        // only window onto the accepted set's total from outside. The round trip
        // is exact to well under a byte at these magnitudes (~1.25 MB against a
        // 53-bit significand), so `.rounded()` recovers the integer and a
        // ONE-BYTE discrepancy is still visible.
        let acceptedBytes = Int((recovery.chainedFractionB * Double(bAudioBytes)).rounded())
        #expect(
            acceptedBytes == unionBytes,
            "accepted A-coverage \(acceptedBytes) B != found-run union \(unionBytes) B"
        )

        // THE CONVERSE. Frames 1000–1400 are covered by no run at all, so they
        // are divergent by definition and must ship as a slot. A clip that
        // over-claims its length starts the following gap late and eats into
        // this region — silently, because the one-sided rail above is satisfied
        // by emitting less.
        let holeStart = RediffByteAligner.timeAt(parsedA, byteOffset: 1000 * frame)
        let holeEnd = RediffByteAligner.timeAt(parsedA, byteOffset: 1400 * frame)
        let covered = recovery.slots.contains {
            $0.aStartSeconds <= holeStart + Self.epsilonSeconds
                && $0.aEndSeconds >= holeEnd - Self.epsilonSeconds
        }
        #expect(
            covered,
            """
            run-free A-region \(holeStart)..\(holeEnd) must ship as divergent — \
            emitted \(recovery.slots.map { ($0.aStartSeconds, $0.aEndSeconds) })
            """
        )
    }

    // MARK: - 5d. R2 REVIEW — a SHORT clipped run cannot be merged over

    /// R2 REVIEW (F4). The precision note above `segmentDivergentSlots` used to
    /// read "every run is already ≥ `minRunBytes` (from `byteRuns`)", and this
    /// file's header used that fact to bound fragment merge. THIS BEAD
    /// FALSIFIED THE PREMISE: a clipped run is `bytes - trim`, floored only by
    /// `guard aEnd > globalAEnd` — one byte. The aligner's own note even said
    /// so and then drew the wrong conclusion from it, that "in practice
    /// `mergedAndCapped` rejoins across it". If that happened it would be this
    /// bead's defect returning through the merge door: a merged slot spanning a
    /// byte-verified run is matched audio inside an emitted slot, which is
    /// exactly what § 1 forbids.
    ///
    /// IT CANNOT HAPPEN, and the reason is structural rather than numeric. A
    /// clipped run's `aStart` IS the previous accepted run's `aEnd`, so the gap
    /// before it has zero A-width and `minAdSeconds` drops it. Every SHIPPABLE
    /// gap is therefore followed immediately by a run accepted WHOLE, and the
    /// separation between two consecutive shipped slots is still at least
    /// `minRunBytes` of A. Nothing asserted that, so it is asserted here.
    ///
    /// The fixture composes the REAL `segmentDivergentSlots` with the REAL gate
    /// and merge; only `Alignment`'s plumbing fields are supplied. Its clipped
    /// tail is 1.31 s — well inside the 3 s merge window, so the shape the old
    /// note worried about is genuinely staged rather than assumed away.
    @Test("a clipped run shorter than the merge window still cannot be joined over")
    func aShortClippedRunIsNeverMergedOver() {
        let frame = SyntheticMP3.frameLength
        let config = RediffSlotOwnership.Configuration.default
        let parsedA = RediffByteAligner.parse(
            SyntheticMP3.file(SyntheticMP3.frames(count: 3000, seed: 0x5D_0001)))
        let parsedB = RediffByteAligner.parse(
            SyntheticMP3.file(SyntheticMP3.frames(count: 2000, seed: 0x5D_0002)))
        func run(_ span: Range<Int>, bStart: Int) -> RediffByteAligner.Run {
            RediffByteAligner.Run(
                aStart: span.lowerBound * frame,
                bStart: bStart,
                bytes: (span.upperBound - span.lowerBound) * frame
            )
        }
        // Every run is ≥ `minRunBytes` (200 frames = 83 400 B > 65 536), so each
        // is a length `byteRuns` could really have emitted. The THIRD overlaps
        // the second by 150 frames, leaving a clipped tail of 50 frames.
        let runs = [
            run(0..<400, bStart: 0),
            run(1000..<1400, bStart: 200_000),
            run(1250..<1450, bStart: 400_000),      // → clipped to [1400, 1450)
            run(2000..<2400, bStart: 600_000)
        ]
        let bAudioBytes = max(1, parsedB.sizeBytes - parsedB.leadingID3Bytes)
        let recovery = RediffByteAligner.segmentDivergentSlots(
            runs: runs, pa: parsedA, pb: parsedB, bAudioBytes: bAudioBytes)

        // CONTROLS: the fixture must stage a clipped run INSIDE the merge
        // window, and must clear the re-encode floor on its own numbers.
        let clippedSeconds = RediffByteAligner.timeAt(parsedA, byteOffset: 1450 * frame)
            - RediffByteAligner.timeAt(parsedA, byteOffset: 1400 * frame)
        #expect(clippedSeconds < config.fragmentMergeGapSeconds,
                "the clipped tail must be shorter than the merge window — got \(clippedSeconds) s")
        #expect(recovery.runsAOverlapping == 1, "exactly one run is clipped here")
        #expect(recovery.chainedFractionB >= config.minAlignedFractionB,
                "the fixture must clear the re-encode floor — got \(recovery.chainedFractionB)")

        let foundRunASpans = runs.map {
            TimeRange(
                start: RediffByteAligner.timeAt(parsedA, byteOffset: $0.aStart),
                end: RediffByteAligner.timeAt(parsedA, byteOffset: $0.aStart + $0.bytes)
            )
        }
        let alignment = RediffByteAligner.Alignment(
            runsFound: runs.count,
            chain: [runs[0]],
            runsDroppedNonMonotonic: 1,            // → the recovery arm
            chainedBytes: runs[0].bytes,
            chainedFractionB: 0,
            slots: [],
            aDurationSeconds: parsedA.durationSeconds,
            bDurationSeconds: parsedB.durationSeconds,
            segmentedSlots: recovery.slots,
            segmentedChainedFractionB: recovery.chainedFractionB,
            segmentedRunsChained: recovery.runsChained,
            foundRunASpans: foundRunASpans,
            segmentedRunsAOverlapping: recovery.runsAOverlapping,
            segmentedOverlapSecondsRecovered: recovery.overlapSecondsRecovered
        )
        guard case .accepted(let acceptance) = RediffSlotOwnership.gateAndDiffBytes(
            alignment: alignment, config: config, recoverNonMonotonicSegments: true
        ) else {
            Issue.record("the recovery arm must accept this fixture"); return
        }
        // CONTROL: more than one slot ships, so a merge is possible at all.
        #expect(acceptance.playedSlots.count == 3,
                "three run-free regions ship — got \(acceptance.playedSlots.map { ($0.startSeconds, $0.endSeconds) })")

        // THE STRUCTURAL PROPERTY. Separation is measured in A-seconds against
        // `minRunBytes`, because that is the bound the merge argument rests on.
        let minRunSeconds = Double(RediffByteAligner.Configuration.default.minRunBytes)
            / Double(frame) * SyntheticMP3.secondsPerFrame
        for (left, right) in zip(acceptance.playedSlots, acceptance.playedSlots.dropFirst()) {
            let separation = right.startSeconds - left.endSeconds
            #expect(separation >= minRunSeconds - Self.epsilonSeconds,
                    "slots \(separation) s apart — a CLIPPED run became a slot separator")
            #expect(separation > config.fragmentMergeGapSeconds,
                    "…which is what stops the merge joining across a byte-verified run")
        }

        // THE INVARIANT, read through the production witness, POST-merge.
        #expect(acceptance.diagnostics.alignedSecondsInSlots == 0,
                "a shipped slot carries \(acceptance.diagnostics.alignedSecondsInSlots) s of matched audio")
        #expect(acceptance.diagnostics.maxAlignedSecondsInSlot == 0)
    }

    // MARK: - 6. The one exemption, pinned

    @Test("the STRICT slot list is unreachable when the chain drops runs — the exemption is not a hole")
    func strictSlotsAreUnreachableWhenTheChainDropsRuns() {
        // `chainRuns` drops runs too, and their A-spans land in `alignment.slots`
        // as gaps — the SAME shape. The invariant rail exempts that list on a
        // non-monotonic alignment, which is only legitimate while nothing can
        // ship it. Both gate arms are checked here, on a fixture that carries a
        // strict-list phantom by construction, so if a future change ever routes
        // a non-monotonic strict slot to a listener this test names it.
        let pair = SegmentRecoveryFixture.build(
            name: "3zxd/exemption/insertedInB",
            contentFrames: Array(repeating: Self.content240, count: 2),
            breaks: [.insertedInB(Self.ad30)]
        )
        let alignment = RediffByteAligner.align(aData: pair.aData, bData: pair.bData)
        #expect(!alignment.monotonicClean, "control: this fixture must reach the exempt branch")
        let strictPhantomSeconds = alignment.slots
            .filter { $0.aSeconds >= RediffSlotOwnership.Configuration.default.minAdSeconds }
            .reduce(0.0) { $0 + alignedSeconds($1.aStartSeconds, $1.aEndSeconds, alignment) }
        #expect(strictPhantomSeconds > 1.0,
                "control: the exempt list really does carry matched audio — got \(strictPhantomSeconds) s")

        // Flag OFF: the whole fetch is rejected, so nothing is read.
        #expect(RediffSlotOwnership.gateAndDiffBytes(alignment: alignment)
                == .rejectedNonMonotonic(dropped: alignment.runsDroppedNonMonotonic))
        // Flag ON: whatever is emitted comes from the SEGMENTED list, which the
        // invariant covers — so nothing matched can ship either way.
        if case .accepted(let acceptance) = RediffSlotOwnership.gateAndDiffBytes(
            alignment: alignment, recoverNonMonotonicSegments: true
        ) {
            for slot in acceptance.playedSlots {
                #expect(alignedSeconds(slot.startSeconds, slot.endSeconds, alignment) <= Self.epsilonSeconds)
            }
        }
    }

    // MARK: - 7. The probe itself

    @Test("alignedSeconds unions before it measures — overlapping runs are never double-counted")
    func alignedSecondsIsUnionAware() {
        let runs = [
            TimeRange(start: 10, end: 30),
            TimeRange(start: 20, end: 40),   // overlaps the first by 10 s
            TimeRange(start: 100, end: 110)
        ]
        // A naive Σ-of-overlaps would report 40 s for [0, 200): 20 + 20 + 10.
        #expect(RediffByteAligner.alignedSeconds(in: TimeRange(start: 0, end: 200), runASpans: runs) == 40)
        // Clamped to the span, not to the run.
        #expect(RediffByteAligner.alignedSeconds(in: TimeRange(start: 25, end: 35), runASpans: runs) == 10)
        // Endpoint touch is NOT coverage (the pipeline-wide interval semantics).
        #expect(RediffByteAligner.alignedSeconds(in: TimeRange(start: 40, end: 100), runASpans: runs) == 0)
        // Degenerate and empty inputs are total, never a trap.
        #expect(RediffByteAligner.alignedSeconds(in: TimeRange(start: 50, end: 50), runASpans: runs) == 0)
        #expect(RediffByteAligner.alignedSeconds(in: TimeRange(start: 0, end: 200), runASpans: []) == 0)
        #expect(RediffByteAligner.alignedSeconds(in: TimeRange(start: 30, end: 10), runASpans: runs) == 0)
    }
}

private extension Array {
    subscript(safeIndex index: Int) -> Element? {
        indices.contains(index) ? self[index] : nil
    }
}
