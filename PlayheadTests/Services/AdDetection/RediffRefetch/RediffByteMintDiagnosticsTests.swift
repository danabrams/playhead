// RediffByteMintDiagnosticsTests.swift
// playhead-3zxd — the instrumentation that makes the phone the capture harness.
//
// The fix itself is proven on synthetic pairs in
// `RediffSegmentRecoveryPhantomTests`. This file proves the OTHER half: that a
// real day-0 diff leaves behind exactly the numbers a device pull needs, that
// they mean what their names say, and that the payload is bounded.
//
// THE VALIDATION QUERY these columns exist to serve (see the V48 migration note
// in `AnalysisStore.swift` for the column semantics):
//
//   SELECT analysisAssetId, datetime(lastAttemptAt,'unixepoch') AS at,
//          lastExit, lastMarkCount, lastDivergentSlotCount,
//          lastBSideCount, lastBSidesAccepted, lastBSidesGateRejected,
//          lastBSidesUnreadable,
//          lastRunsFound, lastRunsAOverlapping, lastOverlapSecondsRecovered,
//          lastAlignedSecondsInSlots, lastMaxAlignedSecondsInSlot,
//          lastAlignedRunSpans
//     FROM rediff_day_zero_attempts
//    ORDER BY lastAttemptAt DESC;
//
// read as — AND READ `lastExit` FIRST, because `lastRunsFound = 0` is two
// different facts and only the exit separates them (R2 review):
//   lastExit ∈ {marked, no_divergent_slot,
//               all_slots_already_covered,
//               no_accepted_byte_diff}    -> a diff RAN. The columns below are
//                                            about that diff.
//   any other lastExit                    -> no diff ran on this attempt, and
//                                            the six columns describe whichever
//                                            earlier attempt `lastAttemptAt`
//                                            names (a FREE exit carries both
//                                            forward together) or are the
//                                            never-measured defaults. They are
//                                            never zeroed to represent the
//                                            decline itself — see
//                                            `DayZeroRediffAttemptPolicy
//                                            .advance`.
//   lastRunsFound = 0                     -> VACUOUS: no column on the row is
//                                            evidence about an emitted slot.
//                                            R3 review — it does NOT establish a
//                                            re-encoding CDN, and reading it
//                                            that way would be the same
//                                            fabrication F1 was. The sum runs
//                                            over ACCEPTED personas only and an
//                                            accepted persona always has ≥ 1
//                                            run, so 0 means NO PERSONA WAS
//                                            ACCEPTED — i.e. it is
//                                            `lastExit = no_accepted_byte_diff`
//                                            restated, carrying nothing the exit
//                                            did not. The aligner may have found
//                                            plenty and been rejected on
//                                            non-monotonicity with no shippable
//                                            segment. `lastBSidesGateRejected`
//                                            vs `lastBSidesUnreadable` is what
//                                            separates "the gate refused every
//                                            copy" from "no copy was readable";
//                                            neither separates the gate's three
//                                            refusal reasons, which are not
//                                            persisted (playhead-vhuc).
//   lastRunsAOverlapping = 0              -> the defect had no OPPORTUNITY on
//                                            this episode. Unfalsified, not
//                                            confirmed.
//   lastRunsAOverlapping > 0 AND
//   lastAlignedSecondsInSlots = 0         -> the defect HAD its opportunity here
//                                            and the fix held. Read
//                                            `lastDivergentSlotCount` alongside:
//                                            > 0 means the invariant witness was
//                                            EXERCISED over shipped slots and
//                                            scored clean; = 0 means nothing
//                                            shipped, so the witness is VACUOUS
//                                            and it is the slot count, not the
//                                            witness, that carries the conclusion.
//                                            This bead's flagship shape — a 470 s
//                                            pure-show tail — lands in the second
//                                            case: post-fix it emits no slot at
//                                            all. (R4 review; same shape as the
//                                            `lastRunsFound = 0` rule above.)
//   lastOverlapSecondsRecovered           -> an UPPER BOUND on the show a pre-3zxd
//                                            build would have banner-marked, never
//                                            the realised figure — read it as a
//                                            magnitude, not as a saved-seconds
//                                            total. R4 review, MEASURED on this
//                                            branch's own width sweep
//                                            (`nothingShipsBelowOrAboveTheDuration
//                                            Cap`): at tails of 540 s and 720 s
//                                            the column reads 540.00 and 720.01
//                                            while the pre-3zxd gap EXCEEDED
//                                            `maxSlotSeconds` (480 s) and shipped
//                                            NOTHING — 2 of the 7 swept widths, so
//                                            the bound is loose, and loose in the
//                                            flattering direction. The gap also
//                                            had to clear `minAdSeconds` (5 s),
//                                            and a pre-3zxd build computed a LOWER
//                                            `segmentedChainedFractionB` that
//                                            could have failed the re-encode floor
//                                            outright. See
//                                            `RediffByteAligner.Alignment
//                                            .segmentedOverlapSecondsRecovered`,
//                                            which has said "not the realised
//                                            damage" all along.
//   lastAlignedSecondsInSlots > 0         -> a slot some persona emitted contains
//                                            audio that persona proved matched.
//                                            Check the MAGNITUDE first: ≤ 3 s per
//                                            join is fragment-merge; this bead's
//                                            phantom is minutes. R5 review — this
//                                            is a PER-PERSONA score, taken before
//                                            `unionedPlayedSlots` re-merges the
//                                            personas into the geometry that
//                                            ships, so it also UNDER-reports by
//                                            up to 3 s per cross-persona join.
//                                            Optimistic by seconds, never
//                                            pessimistic; see
//                                            `RediffSlotOwnership.ByteDiagnostics
//                                            .alignedSecondsInSlots`.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-3zxd — the day-0 byte diff leaves its own evidence")
struct RediffByteMintDiagnosticsTests {

    // MARK: - 1. End to end from real synthetic bytes

    @Test("a phantom-shaped diff records the opportunity, the averted damage, and a clean invariant")
    func aPhantomShapedDiffRecordsWhatTheQueryNeeds() throws {
        // pyq7's widest SHIPPABLE phantom: 900 s + a 470 s tail with a break the
        // FRESH copy has and the played copy lacks. Pre-3zxd this emitted one
        // 469.99 s slot of pure show, just under the 480 s cap.
        let pair = SegmentRecoveryFixture.build(
            name: "3zxd/diagnostics/tail470",
            contentFrames: [
                SegmentRecoveryFixture.frames(seconds: 900),
                SegmentRecoveryFixture.frames(seconds: 470)
            ],
            breaks: [.insertedInB(SegmentRecoveryFixture.frames(seconds: 30))]
        )
        let alignment = RediffByteAligner.align(aData: pair.aData, bData: pair.bData)
        let outcome = RediffSlotOwnership.gateAndDiffBytes(
            alignment: alignment, recoverNonMonotonicSegments: true)

        // The gate may accept (with no slots) or reject; either way the numbers
        // must be readable, so take whichever arm ran.
        let diagnostics: RediffSlotOwnership.ByteDiagnostics
        if case .accepted(let acceptance) = outcome {
            #expect(acceptance.playedSlots.isEmpty, "no ad exists in the played copy")
            diagnostics = acceptance.diagnostics
        } else {
            // A rejection means nothing shipped, which is also correct here —
            // but then there is no acceptance to read, so re-derive from the
            // alignment for the assertions below.
            diagnostics = RediffSlotOwnership.ByteDiagnostics(
                runsFound: alignment.runsFound,
                runsAOverlapping: alignment.segmentedRunsAOverlapping,
                overlapSecondsRecovered: alignment.segmentedOverlapSecondsRecovered,
                foundRunASpans: alignment.foundRunASpans
            )
        }

        #expect(diagnostics.runsFound == 2, "VACUITY CONTROL is satisfied — the aligner found structure")
        #expect(diagnostics.runsAOverlapping == 1,
                "THE OPPORTUNITY: one run's A-span was partly covered, which is the phantom's shape")
        #expect(diagnostics.overlapSecondsRecovered > 460 && diagnostics.overlapSecondsRecovered < 471,
                "THE AVERTED DAMAGE ≈ the 470 s tail — got \(diagnostics.overlapSecondsRecovered) s")
        #expect(diagnostics.alignedSecondsInSlots == 0, "THE INVARIANT")
        #expect(diagnostics.maxAlignedSecondsInSlot == 0)
        #expect(diagnostics.foundRunASpans.count == alignment.runsFound)
    }

    @Test("a genuine multi-break rotation records real slots with a clean invariant and no opportunity")
    func aGenuineRotationRecordsCleanNumbers() throws {
        // Three real rotated ads the played copy HAS and the re-fetch lacks —
        // the Fresh Air-class shape playhead-9s6q exists to recover. No run
        // A-overlaps another here, so the opportunity counter must read 0 and
        // the ads must still come back.
        let pair = SegmentRecoveryFixture.build(
            name: "3zxd/diagnostics/removedInB-x3",
            contentFrames: Array(repeating: SegmentRecoveryFixture.frames(seconds: 240), count: 4),
            breaks: Array(repeating: .removedInB(SegmentRecoveryFixture.frames(seconds: 30)), count: 3)
        )
        let alignment = RediffByteAligner.align(aData: pair.aData, bData: pair.bData)
        guard case .accepted(let acceptance) = RediffSlotOwnership.gateAndDiffBytes(
            alignment: alignment, recoverNonMonotonicSegments: true
        ) else {
            Issue.record("the recovery arm must accept a genuine multi-break rotation"); return
        }
        #expect(acceptance.playedSlots.count == 3, "all three real ads recovered")
        #expect(acceptance.diagnostics.runsFound == 4)
        #expect(acceptance.diagnostics.runsAOverlapping == 0,
                "no run overlapped another in A — this shape never had the defect")
        #expect(acceptance.diagnostics.overlapSecondsRecovered == 0)
        #expect(acceptance.diagnostics.alignedSecondsInSlots == 0,
                "and the emitted slots are pure divergence")
    }

    // MARK: - 1b. The witness can read NON-ZERO

    /// THE VACUITY QUESTION, asked of the instrument itself: what would
    /// `alignedSecondsInSlots` read if the thing it measures never happened?
    ///
    /// Every honest path now scores 0, which is the point of the fix — and it is
    /// also exactly how a broken instrument looks. A diagnostic that is
    /// structurally incapable of reporting the defect it names would sit at 0
    /// forever on the device and be read as "clean". So the alignment here is
    /// HAND-BUILT with an emitted slot that overlaps a found run, a shape the
    /// production aligner can no longer produce, purely to prove the meter
    /// deflects.
    @Test("the invariant witness is capable of reporting a violation — a zero on device means something")
    func theWitnessCanReadNonZero() throws {
        let run = RediffByteAligner.Run(aStart: 0, bStart: 0, bytes: 1)
        let slot = RediffByteAligner.Slot(
            kind: .removedInB,
            aStartByte: 0, aEndByte: 1,
            aStartSeconds: 100, aEndSeconds: 130,
            aBytes: 1, bBytes: 0,
            leftFlankSeconds: 60, rightFlankSeconds: 60
        )
        let alignment = RediffByteAligner.Alignment(
            runsFound: 1,
            chain: [run],
            runsDroppedNonMonotonic: 1,        // → the recovery arm
            chainedBytes: 1,
            chainedFractionB: 0.9,
            slots: [],
            aDurationSeconds: 600,
            bDurationSeconds: 600,
            segmentedSlots: [slot],
            segmentedChainedFractionB: 0.9,    // clears the re-encode floor
            segmentedRunsChained: 1,
            // 10 of the slot's 30 A-seconds are byte-verified matched audio.
            foundRunASpans: [TimeRange(start: 110, end: 120)],
            segmentedRunsAOverlapping: 1,
            segmentedOverlapSecondsRecovered: 42
        )
        guard case .accepted(let acceptance) = RediffSlotOwnership.gateAndDiffBytes(
            alignment: alignment, recoverNonMonotonicSegments: true
        ) else {
            Issue.record("the fixture must reach an acceptance for the witness to be read"); return
        }
        #expect(acceptance.playedSlots.count == 1, "control: a slot really was emitted")
        #expect(acceptance.diagnostics.alignedSecondsInSlots == 10,
                "the witness must DEFLECT — got \(acceptance.diagnostics.alignedSecondsInSlots)")
        #expect(acceptance.diagnostics.maxAlignedSecondsInSlot == 10)
        // And it is measured against THIS alignment's runs, not a re-derivation:
        // the pass-through fields come from the same object.
        #expect(acceptance.diagnostics.runsAOverlapping == 1)
        #expect(acceptance.diagnostics.overlapSecondsRecovered == 42)
    }

    /// R4 REVIEW RESIDUAL, closed here. `maxAlignedSecondsInSlot` is the field
    /// that exists so a large value cannot hide inside a sum spread over many
    /// slots — and until this test the only non-zero assertion on it
    /// (`theWitnessCanReadNonZero` above) used a SINGLE-slot fixture, where
    /// `worst = max(worst, matched)` and `worst += matched` are indistinguishable.
    /// The per-persona `max` in `RediffSlotOwnership.byteDiagnostics` was
    /// therefore unpinned as a MAX; only `combining`'s max-of-maxes was pinned.
    /// A rail nobody has watched redden is not a rail.
    ///
    /// THREE slots, not two, and the largest is in the MIDDLE on purpose. With
    /// two the max coincides with the first, so `worst = <the first slot's
    /// value>` would also survive. Here the four readings a mutation could
    /// plausibly produce are all distinct: Σ = 18, max = 10, first = 3, last = 5.
    ///
    /// The alignment is hand-built for the same reason `theWitnessCanReadNonZero`
    /// is: after playhead-3zxd the production aligner cannot emit a slot that
    /// contains a found run at all, so the only way to prove the meter reads
    /// correctly ACROSS slots is to hand it a shape it will never see in the
    /// field. Everything below the `Alignment` boundary — the gate, the
    /// `minAdSeconds` filter, `mergedAndCapped`, and the diagnostics fold — is
    /// the real production path.
    @Test("the per-slot worst case is a MAX across slots, not a running total")
    func maxAlignedSecondsIsAMaxAcrossSlotsNotASum() throws {
        func slot(_ start: Double, _ end: Double) -> RediffByteAligner.Slot {
            RediffByteAligner.Slot(
                kind: .removedInB,
                aStartByte: 0, aEndByte: 1,
                aStartSeconds: start, aEndSeconds: end,
                aBytes: 1, bBytes: 0,
                leftFlankSeconds: 60, rightFlankSeconds: 60
            )
        }
        let alignment = RediffByteAligner.Alignment(
            runsFound: 3,
            chain: [RediffByteAligner.Run(aStart: 0, bStart: 0, bytes: 1)],
            runsDroppedNonMonotonic: 1,        // → the recovery arm
            chainedBytes: 1,
            chainedFractionB: 0.9,
            slots: [],
            aDurationSeconds: 600,
            bDurationSeconds: 600,
            // Three emitted slots, each wider than `minAdSeconds` and separated
            // by far more than `fragmentMergeGapSeconds`, so `mergedAndCapped`
            // passes all three through unjoined.
            segmentedSlots: [slot(100, 130), slot(200, 240), slot(300, 330)],
            segmentedChainedFractionB: 0.9,    // clears the re-encode floor
            segmentedRunsChained: 3,
            // One run inside each slot: 3 s, 10 s, 5 s of byte-verified audio.
            foundRunASpans: [
                TimeRange(start: 110, end: 113),
                TimeRange(start: 210, end: 220),
                TimeRange(start: 310, end: 315)
            ],
            segmentedRunsAOverlapping: 3,
            segmentedOverlapSecondsRecovered: 42
        )
        guard case .accepted(let acceptance) = RediffSlotOwnership.gateAndDiffBytes(
            alignment: alignment, recoverNonMonotonicSegments: true
        ) else {
            Issue.record("the fixture must reach an acceptance for the witness to be read"); return
        }
        // CONTROLS: three slots really did ship, in this order, unmerged — the
        // whole discrimination below rests on there being more than one.
        #expect(acceptance.playedSlots.count == 3,
                "got \(acceptance.playedSlots.map { ($0.startSeconds, $0.endSeconds) })")
        #expect(acceptance.playedSlots.map(\.startSeconds) == [100, 200, 300])

        #expect(acceptance.diagnostics.alignedSecondsInSlots == 18,
                "the SUM over slots — got \(acceptance.diagnostics.alignedSecondsInSlots)")
        // 10, not 18 (a running total), not 3 (the first slot), not 5 (the last).
        #expect(acceptance.diagnostics.maxAlignedSecondsInSlot == 10,
                """
                the worst SINGLE slot — got \(acceptance.diagnostics.maxAlignedSecondsInSlot). \
                18 means the max became a sum, which is exactly the reading this \
                field exists to make impossible
                """)
        #expect(acceptance.diagnostics.maxAlignedSecondsInSlot
                < acceptance.diagnostics.alignedSecondsInSlots,
                "control: on this fixture a max and a sum MUST disagree")
    }

    // MARK: - 2. Aggregation across k-way personas

    @Test("combining sums what is additive and takes the MAX of what is a worst case")
    func combiningUsesTheRightOperatorPerQuantity() {
        let first = RediffSlotOwnership.ByteDiagnostics(
            runsFound: 3, runsAOverlapping: 1, overlapSecondsRecovered: 100,
            alignedSecondsInSlots: 2, maxAlignedSecondsInSlot: 2,
            foundRunASpans: [TimeRange(start: 0, end: 10)]
        )
        let second = RediffSlotOwnership.ByteDiagnostics(
            runsFound: 5, runsAOverlapping: 2, overlapSecondsRecovered: 50,
            alignedSecondsInSlots: 7, maxAlignedSecondsInSlot: 5,
            foundRunASpans: [TimeRange(start: 20, end: 30), TimeRange(start: 40, end: 50)]
        )
        let combined = RediffByteMintDiagnostics.combining([first, second])
        #expect(combined.runsFound == 8)
        #expect(combined.runsAOverlapping == 3)
        #expect(combined.overlapSecondsRecovered == 150)
        #expect(combined.alignedSecondsInSlots == 9)
        // 5, NOT 7. `maxAlignedSecondsInSlot` bounds ONE slot; summing it across
        // personas would turn a per-slot bound into a number that bounds nothing
        // and reads as though it did.
        #expect(combined.maxAlignedSecondsInSlot == 5)
        #expect(combined.alignedRunSpans == "v1;0:0.00-10.00;1:20.00-30.00,40.00-50.00")
    }

    @Test("combining nothing is empty, and an empty run set persists NULL rather than an empty string")
    func combiningNothingIsEmpty() {
        #expect(RediffByteMintDiagnostics.combining([]) == .empty)
        #expect(RediffByteMintDiagnostics.combining([]).alignedRunSpans == nil)
        // A persona that was accepted but found no run contributes no group —
        // "no spans recorded" and "an empty list of spans" are different facts
        // and only the first is representable, so NULL is the honest value.
        let noRuns = RediffSlotOwnership.ByteDiagnostics(runsFound: 0, foundRunASpans: [])
        #expect(RediffByteMintDiagnostics.combining([noRuns]).alignedRunSpans == nil)
    }

    // MARK: - 3. The payload is bounded, and says so when it is cut

    @Test("the span codec caps the payload and NAMES how many it dropped")
    func spanCodecCapsAndReportsTruncation() throws {
        let many = (0..<40).map { TimeRange(start: Double($0) * 10, end: Double($0) * 10 + 5) }
        // 80 spans offered, 48 kept.
        let spans = try #require(RediffAlignedRunSpanCodec.encode(perBSide: [many, many]))
        #expect(spans.hasPrefix("v1;0:0.00-5.00,"))
        #expect(spans.hasSuffix(";trunc=32"),
                "a truncated payload must never be mistakable for a complete one — got \(spans)")
        // Every span carries exactly one `-` and nothing else in the format
        // does (seconds are never negative), so hyphens COUNT the spans.
        let kept = spans.filter { $0 == "-" }.count
        #expect(kept == RediffAlignedRunSpanCodec.maxSpans,
                "exactly \(RediffAlignedRunSpanCodec.maxSpans) spans survive the cap — got \(kept)")
        // The documented size bound: 48 spans of ~18 characters plus labels sits
        // well under 1 KB, which is what makes this safe to hold per asset.
        #expect(spans.count < 1000, "payload was \(spans.count) characters")
    }

    @Test("an uncapped payload carries no truncation marker and round-trips its exact spans")
    func spanCodecIsExactWhenUncapped() {
        // Values chosen to format unambiguously at 2 dp — a fixture that sits on
        // a rounding boundary would test the C library, not the codec.
        let encoded = RediffAlignedRunSpanCodec.encode(perBSide: [
            [TimeRange(start: 0, end: 900.25), TimeRange(start: 900.25, end: 1370.5)],
            [TimeRange(start: 0, end: 1370.5)]
        ])
        #expect(encoded == "v1;0:0.00-900.25,900.25-1370.50;1:0.00-1370.50")
        #expect(encoded?.contains("trunc=") == false,
                "no marker when nothing was dropped — the marker must mean something")
    }

    @Test("a persona that found no runs is skipped, and its index is not reused")
    func spanCodecSkipsEmptyPersonasWithoutRenumbering() {
        // The index is the persona's position in the k-way fetch, so it must not
        // be compacted: a reader pairing spans back to personas would otherwise
        // attribute persona 2's runs to persona 1.
        //
        // THIS IS ONLY HALF THE PROPERTY, and R2 review found the other half
        // missing: the codec preserves whatever gaps it is given, but the mint
        // was handing it a list already compacted to the ACCEPTED personas, so
        // there was never a gap to preserve. The production side is pinned by
        // `RediffDayZeroMintExitTests.alignedRunSpanIndexIsTheKWayPosition`.
        let encoded = RediffAlignedRunSpanCodec.encode(perBSide: [
            [], [TimeRange(start: 5, end: 6)], []
        ])
        #expect(encoded == "v1;1:5.00-6.00")
    }

    // MARK: - 4. The record carries it to disk shape-intact

    @Test("advance() overwrites the diagnostics with THIS attempt's — it never accumulates or carries forward")
    func advanceOverwritesRatherThanAccumulates() {
        let prior = RediffDayZeroAttemptRecord(
            analysisAssetId: "asset",
            attemptCount: 1,
            lastAttemptAt: 100,
            lastExit: .noDivergentSlot,
            byteDiagnostics: RediffByteMintDiagnostics(
                runsFound: 9, runsAOverlapping: 4, overlapSecondsRecovered: 400,
                alignedSecondsInSlots: 250, maxAlignedSecondsInSlot: 250,
                alignedRunSpans: "v1;0:0.00-1.00"
            )
        )
        // A fresh attempt whose diffs were clean. If the record accumulated,
        // `alignedSecondsInSlots` could never fall back to 0 once a pre-fix row
        // had contributed to it — the invariant witness would be unable to say
        // the thing it exists to say.
        let outcome = RediffDayZeroMintOutcome(
            markCount: 2,
            exit: .marked,
            byteDiagnostics: RediffByteMintDiagnostics(
                runsFound: 4, runsAOverlapping: 1, overlapSecondsRecovered: 470,
                alignedSecondsInSlots: 0, maxAlignedSecondsInSlot: 0,
                alignedRunSpans: "v1;0:0.00-900.00"
            )
        )
        let advanced = DayZeroRediffAttemptPolicy.advance(
            record: prior, assetId: "asset", outcome: outcome, fullFetchBytes: 108_000_000, at: 200)
        #expect(advanced.byteDiagnostics.runsFound == 4)
        #expect(advanced.byteDiagnostics.alignedSecondsInSlots == 0,
                "the witness must be able to read clean again after a dirty attempt")
        #expect(advanced.byteDiagnostics.overlapSecondsRecovered == 470)
        #expect(advanced.byteDiagnostics.alignedRunSpans == "v1;0:0.00-900.00")
        // Cumulative fields are unaffected by this change.
        #expect(advanced.totalFullFetchBytes == 108_000_000)
        #expect(advanced.attemptCount == 2)
    }

    @Test("an exit that never reached a diff records the EMPTY diagnostics, which read as vacuous")
    func aFreeExitRecordsVacuousDiagnostics() {
        let advanced = DayZeroRediffAttemptPolicy.advance(
            record: nil,
            assetId: "asset",
            outcome: RediffDayZeroMintOutcome.blocked(.aSideNotAnchored),
            fullFetchBytes: 0,
            at: 100
        )
        #expect(advanced.byteDiagnostics == .empty)
        #expect(advanced.byteDiagnostics.runsFound == 0,
                "no diff ran, so the vacuity control is what the row must say")
        #expect(advanced.byteDiagnostics.alignedRunSpans == nil)
        // …and `lastAttemptAt` is `now` here, which is what makes `.empty`
        // honest rather than a hole: there IS no earlier attempt for the row to
        // be describing. See `aFreeExitNeverErasesAnEarlierDiff` below for the
        // case where there is one.
        #expect(advanced.lastAttemptAt == 100)
    }

    // MARK: - 4b. R2 REVIEW (F1) — a free exit must not ERASE a real diff

    /// THE PAIRING INVARIANT: `byteDiagnostics` describes the attempt
    /// `lastAttemptAt` names. `advance` deliberately does not move
    /// `attemptCount` or `lastAttemptAt` on a FREE exit — an exit that spent no
    /// bandwidth must consume no budget — but it used to write
    /// `outcome.byteDiagnostics` unconditionally, and a free exit carries
    /// `.empty`. Since `upsertRediffDayZeroAttempt`'s `ON CONFLICT` sets all six
    /// V48 columns from `excluded`, that ZEROED a prior real diff and left the
    /// row advertising that diff's timestamp beside six measurements of an event
    /// which ran no diff.
    ///
    /// The consequence is not a missing number, it is a FABRICATED one: the
    /// validation query's first branch reads `lastRunsFound = 0` as VACUOUS —
    /// "the aligner found nothing (re-encoding CDN, or the bytes are not MP3)".
    /// So a locally-blocked replay manufactures a re-encode observation about an
    /// asset a real diff had already measured, which is the standing defect
    /// class inside the instrument built to detect it.
    ///
    /// The exit staged here is deliberately the IN-MINT shape, with
    /// `bSideCount: 2`. `mintByteExactDayZeroMarks` resolves the A-side AFTER
    /// the k-way fetch and returns `.aSideNotAnchored` with
    /// `bSideCount: bSideURLs.count`, which is ≥ 2 there — so a `bSideCount > 0`
    /// discriminator would read TRUE on exactly the exit it was meant to
    /// exclude. `spentBandwidth` is the property that cannot drift.
    @Test("a FREE exit never erases the diff an earlier attempt measured")
    func aFreeExitNeverErasesAnEarlierDiff() {
        let measured = RediffByteMintDiagnostics(
            runsFound: 6, runsAOverlapping: 2, overlapSecondsRecovered: 469.99,
            alignedSecondsInSlots: 0, maxAlignedSecondsInSlot: 0,
            alignedRunSpans: "v1;0:0.00-900.00"
        )
        let prior = RediffDayZeroAttemptRecord(
            analysisAssetId: "asset",
            attemptCount: 1,
            lastAttemptAt: 1_000,
            lastExit: .noDivergentSlot,
            byteDiagnostics: measured
        )
        // The A-side was deleted after listening, so the next granted attempt is
        // blocked locally — past the fetch, hence two B-copies on hand.
        let blocked = RediffDayZeroMintOutcome(exit: .aSideNotAnchored, bSideCount: 2)
        #expect(!blocked.exit.spentBandwidth, "control: this exit is FREE")
        #expect(blocked.byteDiagnostics == .empty, "control: a free exit carries no measurement")
        #expect(blocked.bSideCount > 0,
                "control: `bSideCount > 0` is TRUE on this free exit — it cannot be the discriminator")

        let advanced = DayZeroRediffAttemptPolicy.advance(
            record: prior, assetId: "asset", outcome: blocked, fullFetchBytes: 0, at: 9_000)

        #expect(advanced.lastAttemptAt == 1_000, "control: a free exit does not move the timestamp")
        #expect(advanced.attemptCount == 1, "control: a free exit does not spend the budget")
        #expect(advanced.byteDiagnostics == measured,
                "the diagnostics must describe the attempt `lastAttemptAt` names")
        #expect(advanced.byteDiagnostics.runsFound == 6,
                "runsFound = 0 here would read as VACUOUS — a re-encode observation nobody made")
        #expect(advanced.byteDiagnostics.alignedRunSpans == "v1;0:0.00-900.00")
        // The decline itself is still fully diagnosable — this fix narrows what
        // a free exit overwrites, it does not make the row silent.
        #expect(advanced.lastExit == .aSideNotAnchored)
        #expect(advanced.lastBSideCount == 2)
    }

    /// The other direction, so the fix cannot be "carry forward always". An
    /// attempt that SPENT bytes moves `lastAttemptAt` to now, so its
    /// diagnostics must move too — even when they are empty, which is what a
    /// fetch whose every persona gate-rejected honestly measured. The row stays
    /// self-consistent: a fresh timestamp beside a fresh (empty) measurement,
    /// with `lastExit` naming why.
    @Test("an attempt that SPENT bytes overwrites the diagnostics, even with an empty measurement")
    func aBandwidthSpendingExitStillOverwrites() {
        let prior = RediffDayZeroAttemptRecord(
            analysisAssetId: "asset",
            attemptCount: 1,
            lastAttemptAt: 1_000,
            lastExit: .marked,
            byteDiagnostics: RediffByteMintDiagnostics(runsFound: 6, runsAOverlapping: 2)
        )
        let rejected = RediffDayZeroMintOutcome(
            exit: .noAcceptedByteDiff, bSideCount: 2, bSidesGateRejected: 2)
        #expect(rejected.exit.spentBandwidth, "control: this exit paid for the fetch")

        let advanced = DayZeroRediffAttemptPolicy.advance(
            record: prior, assetId: "asset", outcome: rejected,
            fullFetchBytes: 108_000_000, at: 9_000)

        #expect(advanced.lastAttemptAt == 9_000)
        #expect(advanced.byteDiagnostics == .empty,
                "a stale set would read as evidence about the diffs THIS attempt ran")
    }

    /// A record from a FOREIGN generation does not carry its `lastAttemptAt`
    /// forward — `advance` stamps `now` — so it must not carry its diagnostics
    /// forward either, or the pairing invariant re-opens one generation out.
    /// This is why the carry-forward reads `budgeted`, not `record`.
    @Test("a free exit over a FOREIGN-generation record records EMPTY, matching its fresh timestamp")
    func aFreeExitOverAForeignGenerationRecordsEmpty() {
        let prior = RediffDayZeroAttemptRecord(
            analysisAssetId: "asset",
            attemptCount: 3,
            lastAttemptAt: 1_000,
            lastExit: .noDivergentSlot,
            policyGeneration: DayZeroRediffAttemptPolicy.currentGeneration - 1,
            byteDiagnostics: RediffByteMintDiagnostics(runsFound: 6, runsAOverlapping: 2)
        )
        let advanced = DayZeroRediffAttemptPolicy.advance(
            record: prior, assetId: "asset",
            outcome: RediffDayZeroMintOutcome(exit: .aSideNotAnchored, bSideCount: 2),
            fullFetchBytes: 0, at: 9_000)

        #expect(advanced.lastAttemptAt == 9_000, "control: a foreign generation does not carry the timestamp")
        #expect(advanced.byteDiagnostics == .empty,
                "…so the diagnostics must not be carried either — they would describe another generation")
    }
}
