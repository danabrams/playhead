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
// FRAGMENT MERGE IS A SEPARATE, BOUNDED EFFECT — read the numbers with it in
// mind. `RediffSlotOwnership.mergedAndCapped` joins two slots separated by
// ≤ `fragmentMergeGapSeconds` (3 s), and the thing separating two gaps is an
// accepted RUN. At the default `minRunBytes` (65536) a run is 4.1 s at 128 kbps
// CBR, so it cannot be joined over; at ≥192 kbps it is under 3 s and can be.
// That is a second, much smaller instance of the same class, bounded at 3 s per
// join versus this bead's minutes, and it is filed separately rather than fixed
// here. The synthetic pairs below are all 128 kbps, so no merge fires and the
// post-merge assertions are exact.

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
