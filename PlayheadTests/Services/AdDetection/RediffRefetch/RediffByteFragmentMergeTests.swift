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

    /// A = head + ad1 + X + ad2 + tail — the PLAYED copy carries both ads.
    /// B = head + X + tail — the re-fetch carries neither. X is show audio
    /// present in both, so the aligner proves it as a run BETWEEN two played
    /// slots, and its 60 frames (~1.57 s) sit under the 3.0 s merge gap.
    ///
    /// The first draft of this fixture put the ads in B, which is an INSERTION:
    /// zero width in A, and exactly what the gate's `minAdSeconds` filter
    /// removes (`RediffByteAlignerTests.insertionInB` says so in its title).
    /// A played slot is audio that is IN the played copy; each ad here is 200
    /// frames (~5.2 s) so it clears the 5 s floor.
    private static func twoAdsAroundAVerifiedRun() -> RediffByteAligner.Alignment {
        var head = SyntheticMP3.frames(count: 30, seed: 11)
        SyntheticMP3.pinTailByte(&head, to: 0xAA)
        let between = SyntheticMP3.frames(count: 60, seed: 44)
        let tail = SyntheticMP3.frames(count: 30, seed: 71)
        var ad1 = SyntheticMP3.frames(count: 200, seed: 998)
        SyntheticMP3.pinTailByte(&ad1, to: 0x55)
        var ad2 = SyntheticMP3.frames(count: 200, seed: 997)
        SyntheticMP3.pinTailByte(&ad2, to: 0x33)
        let aData = SyntheticMP3.file(head + ad1 + between + ad2 + tail)
        let bData = SyntheticMP3.file(head + between + tail)
        return RediffByteAligner.align(aData: aData, bData: bData, config: SyntheticMP3.smallRunConfig)
    }

    @Test("two ads around a verified run stay two slots, with no matched audio inside either",
          .timeLimit(.minutes(1)))
    func twoAdsStayTwoSlots() throws {
        let alignment = Self.twoAdsAroundAVerifiedRun()
        try #require(alignment.slots.count == 2, "setup: the aligner must see two inserted slots; got \(alignment.slots.count)")
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
