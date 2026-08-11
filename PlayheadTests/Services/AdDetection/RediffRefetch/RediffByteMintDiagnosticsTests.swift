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
//          lastRunsFound, lastRunsAOverlapping, lastOverlapSecondsRecovered,
//          lastAlignedSecondsInSlots, lastMaxAlignedSecondsInSlot,
//          lastAlignedRunSpans
//     FROM rediff_day_zero_attempts
//    ORDER BY lastAttemptAt DESC;
//
// read as:
//   lastRunsFound = 0                     -> VACUOUS. The aligner found nothing
//                                            (re-encoding CDN, or the bytes are
//                                            not MP3); no other column on the
//                                            row is evidence.
//   lastRunsAOverlapping = 0              -> the defect had no OPPORTUNITY on
//                                            this episode. Unfalsified, not
//                                            confirmed.
//   lastRunsAOverlapping > 0 AND
//   lastAlignedSecondsInSlots = 0         -> THE PHANTOM FIRED AND THE FIX HELD.
//                                            lastOverlapSecondsRecovered is the
//                                            show a pre-3zxd build would have
//                                            banner-marked on that episode.
//   lastAlignedSecondsInSlots > 0         -> a shipped slot contains audio the
//                                            aligner proved matched. Check the
//                                            MAGNITUDE first: ≤ 3 s per join is
//                                            the known fragment-merge effect;
//                                            this bead's phantom is minutes.

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
    }
}
