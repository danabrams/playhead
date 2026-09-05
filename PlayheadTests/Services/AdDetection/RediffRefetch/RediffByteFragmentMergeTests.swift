import Foundation
import Testing
@testable import Playhead

/// playhead-yzra — the byte path never bridges a gap, because on the byte
/// path a gap is an accepted run: bytes proven identical in both copies.
///
/// The fragment merge exists for the CHROMA path, where two slots a couple of
/// seconds apart are one ad whose fingerprints failed to align across a
/// splice. Applied identically to byte-derived slots it joins two real ads
/// across the show between them whenever that run is shorter than the merge
/// gap — arithmetic, not chance: a run is >= minRunBytes by construction, and
/// at 192+ kbps that is under 3.0 s. This fixture reproduces the geometry at
/// the synthetic 128 kbps with a small run floor.
@Suite("playhead-yzra: byte slots are never merged across a verified run")
struct RediffByteFragmentMergeTests {

    /// The claim is about the GATE, so the fixture is built at the gate's
    /// level, the way `RediffByteAlignerTests.gateCleaningParity` does it: two
    /// played slots in A-time with a 1.57 s gap — the length of one minimum
    /// run at 192 kbps — and, explicitly, the RUN the aligner proved between
    /// them as a found-run A-span. The first draft drove the real aligner over
    /// synthetic MP3 and reached the gate only after two geometry mistakes,
    /// and even then the claim it was proving was the aligner's, not the
    /// gate's. Stating the run explicitly is also what makes 3zxd's own
    /// instrument a witness here: `alignedSecondsInSlots` reads 0 when the two
    /// stay two, and ~1.57 when the gate joins them across it.
    private static let runBetween = TimeRange(start: 130.0, end: 131.57)

    private static func twoAdsAroundAVerifiedRun() -> RediffByteAligner.Alignment {
        func slot(_ start: Double, _ end: Double, left: Double, right: Double) -> RediffByteAligner.Slot {
            RediffByteAligner.Slot(
                kind: .replaced,
                aStartByte: Int(start * 16000), aEndByte: Int(end * 16000),
                aStartSeconds: start, aEndSeconds: end,
                aBytes: Int((end - start) * 16000), bBytes: 1,
                leftFlankSeconds: left, rightFlankSeconds: right
            )
        }
        let slots = [
            slot(100, 130, left: 100, right: 1.57),
            slot(131.57, 160, left: 1.57, right: 300),
        ]
        return RediffByteAligner.Alignment(
            runsFound: 3,
            chain: [RediffByteAligner.Run(aStart: 0, bStart: 0, bytes: 1_000_000)],
            runsDroppedNonMonotonic: 0,
            chainedBytes: 1_000_000,
            chainedFractionB: 0.95,
            slots: slots,
            aDurationSeconds: 3600,
            bDurationSeconds: 3600,
            segmentedSlots: slots,
            segmentedChainedFractionB: 0.95,
            segmentedRunsChained: 3,
            foundRunASpans: [
                TimeRange(start: 0, end: 100),
                runBetween,
                TimeRange(start: 160, end: 3600),
            ]
        )
    }

    @Test("two ads around a verified run stay two slots, with no matched audio inside either",
          .timeLimit(.minutes(1)))
    func twoAdsStayTwoSlots() throws {
        let alignment = Self.twoAdsAroundAVerifiedRun()
        let gap = alignment.slots[1].aStartSeconds - alignment.slots[0].aEndSeconds
        try #require(
            gap > 0.5 && gap < RediffSlotOwnership.Configuration.default.fragmentMergeGapSeconds,
            "setup: the run between them (\(gap) s) must be shorter than the merge gap, or this proves nothing"
        )

        let verdict = RediffSlotOwnership.gateAndDiffBytes(alignment: alignment)
        guard case .accepted(let acceptance) = verdict else {
            Issue.record("expected acceptance; the gate said \(verdict)"); return
        }
        #expect(
            acceptance.playedSlots.count == 2,
            """
            the byte gate merged two ads across \(gap) s of audio the aligner \
            PROVED identical in both copies — that is playhead-3zxd's shape
            """
        )
        #expect(
            acceptance.diagnostics.alignedSecondsInSlots < 1e-6,
            "no played slot may contain matched audio; got \(acceptance.diagnostics.alignedSecondsInSlots) s"
        )
        #expect(acceptance.diagnostics.maxAlignedSecondsInSlot < 1e-6)
    }

    @Test("the union across B-sides does not merge byte slots either", .timeLimit(.minutes(1)))
    func unionDoesNotMerge() {
        let s1 = RediffSlotOwnership.PlayedSlot(startSeconds: 10, endSeconds: 20, leftRunSeconds: 5, rightRunSeconds: 1.5)
        let s2 = RediffSlotOwnership.PlayedSlot(startSeconds: 21.5, endSeconds: 31.5, leftRunSeconds: 1.5, rightRunSeconds: 5)
        let unioned = RediffSlotOwnership.unionedPlayedSlots([[s1], [s2]])
        #expect(unioned.count == 2, "K-way union of byte slots must not bridge a verified run; got \(unioned.count)")
    }

    @Test("the control: the chroma configuration still bridges the same gap", .timeLimit(.minutes(1)))
    func chromaStillMerges() {
        let s1 = RediffSlotOwnership.PlayedSlot(startSeconds: 10, endSeconds: 20, leftRunSeconds: 5, rightRunSeconds: 1.5)
        let s2 = RediffSlotOwnership.PlayedSlot(startSeconds: 21.5, endSeconds: 31.5, leftRunSeconds: 1.5, rightRunSeconds: 5)
        #expect(RediffSlotOwnership.mergedAndCapped([s1, s2], config: .default).count == 1,
                "fingerprint dropouts are real on the chroma path; the merge must stay")
        #expect(RediffSlotOwnership.mergedAndCapped([s1, s2], config: .default.forByteDerivedSlots).count == 2)
    }
}
