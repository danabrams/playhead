// RediffByteAlignerTests.swift
// playhead-xsdz.57: unit coverage for the byte-run aligner core — the Swift
// port of `scripts/l2f-mp3-forensics.py align` — over SYNTHETIC in-memory MP3
// bytes: frame-walk semantics (ID3 skips, resync, truncation, TAG trailer),
// anchor uniqueness, greedy byte extension, monotonic chaining (incl. the
// first-max tie rule), gap→slot semantics (head/tail/replaced/removed/
// inserted), and the `RediffSlotOwnership.gateAndDiffBytes` gate incl. the
// xsdz.34 §5 veto rule applied to BYTE-derived slots.
//
// The REAL-pair parity against the checked-in python reference lives in
// `RediffByteAlignerParityTests`.

import Foundation
import Testing

@testable import Playhead

// MARK: - Synthetic MP3 builder (shared with the byte-first e2e suite)

/// Deterministic synthetic MPEG1 Layer III mono files: 44100 Hz, 128 kbps,
/// no CRC, no padding ⇒ frameLength 417, 1152 samples/frame (~26.122 ms).
enum SyntheticMP3 {

    static let frameLength = 417  // 144000 * 128 / 44100
    static let secondsPerFrame = 1152.0 / 44100.0

    /// splitmix64 — the same deterministic generator the rediff e2e suite uses.
    struct Noise {
        var state: UInt64
        init(seed: UInt64) { state = seed }
        mutating func next() -> UInt64 {
            state &+= 0x9E37_79B9_7F4A_7C15
            var z = state
            z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
            z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
            return z ^ (z >> 31)
        }
    }

    /// One valid frame: FF FB 90 C0 header + 413 deterministic payload bytes.
    static func frame(rng: inout Noise) -> [UInt8] {
        var bytes: [UInt8] = [0xFF, 0xFB, 0x90, 0xC0]
        bytes.reserveCapacity(frameLength)
        for _ in 0..<(frameLength - 4) {
            bytes.append(UInt8(truncatingIfNeeded: rng.next()))
        }
        return bytes
    }

    static func frames(count: Int, seed: UInt64) -> [[UInt8]] {
        var rng = Noise(seed: seed)
        return (0..<count).map { _ in frame(rng: &rng) }
    }

    /// A syncsafe-sized ID3v2 block: 10-byte header + `payloadBytes` of 0x00.
    static func id3v2(payloadBytes: Int) -> [UInt8] {
        precondition(payloadBytes < 1 << 21)
        var bytes: [UInt8] = [0x49, 0x44, 0x33, 0x03, 0x00, 0x00]  // "ID3", v2.3, flags 0
        bytes.append(UInt8((payloadBytes >> 21) & 0x7F))
        bytes.append(UInt8((payloadBytes >> 14) & 0x7F))
        bytes.append(UInt8((payloadBytes >> 7) & 0x7F))
        bytes.append(UInt8(payloadBytes & 0x7F))
        bytes.append(contentsOf: [UInt8](repeating: 0, count: payloadBytes))
        return bytes
    }

    /// A 128-byte ID3v1 trailer ("TAG" + zeros).
    static func id3v1Trailer() -> [UInt8] {
        var bytes: [UInt8] = [0x54, 0x41, 0x47]  // "TAG"
        bytes.append(contentsOf: [UInt8](repeating: 0, count: 125))
        return bytes
    }

    static func file(_ parts: [[UInt8]]) -> Data {
        var out: [UInt8] = []
        out.reserveCapacity(parts.reduce(0) { $0 + $1.count })
        for part in parts { out.append(contentsOf: part) }
        return Data(out)
    }

    /// Small-run config for compact synthetic files: 5 frames ≈ 2085 bytes.
    static let smallRunConfig = RediffByteAligner.Configuration(minRunBytes: 2048)

    /// Pin the FINAL payload byte of the last frame in `frames` so a splice
    /// boundary is guaranteed to mismatch immediately (no coincidental 1/256
    /// backward extension across the boundary — which would make two runs
    /// overlap by a byte and drop one from the chain).
    static func pinTailByte(_ frames: inout [[UInt8]], to value: UInt8) {
        frames[frames.count - 1][frameLength - 1] = value
    }
}

@Suite("RediffByteAligner core (playhead-xsdz.57 byte-primary differ)")
struct RediffByteAlignerTests {

    // MARK: - Parse: frame walk

    @Test("plain frame sequence parses with exact offsets, lengths, and times")
    func parsePlainFrames() {
        let data = SyntheticMP3.file(SyntheticMP3.frames(count: 40, seed: 1))
        let parsed = RediffByteAligner.parse(data)
        #expect(parsed.frameOffsets.count == 40)
        #expect(parsed.leadingID3Bytes == 0)
        #expect(parsed.frameOffsets[0] == 0)
        #expect(parsed.frameOffsets[1] == SyntheticMP3.frameLength)
        #expect(parsed.frameLengths.allSatisfy { $0 == SyntheticMP3.frameLength })
        #expect(abs(parsed.frameTimes[1] - SyntheticMP3.secondsPerFrame) < 1e-12)
        #expect(abs(parsed.durationSeconds - 40 * SyntheticMP3.secondsPerFrame) < 1e-9)
        #expect(parsed.sizeBytes == data.count)
    }

    @Test("leading ID3v2 is skipped and recorded")
    func parseLeadingID3() {
        let id3 = SyntheticMP3.id3v2(payloadBytes: 300)
        let data = SyntheticMP3.file([id3] + SyntheticMP3.frames(count: 10, seed: 2))
        let parsed = RediffByteAligner.parse(data)
        #expect(parsed.leadingID3Bytes == id3.count)
        #expect(parsed.frameOffsets.first == id3.count)
        #expect(parsed.frameOffsets.count == 10)
    }

    @Test("mid-file ID3v2 block is skipped without losing sync")
    func parseMidfileID3() {
        let frames = SyntheticMP3.frames(count: 12, seed: 3)
        let data = SyntheticMP3.file(
            Array(frames[0..<6]) + [SyntheticMP3.id3v2(payloadBytes: 64)] + Array(frames[6...])
        )
        let parsed = RediffByteAligner.parse(data)
        #expect(parsed.frameOffsets.count == 12)
        // Frame 6 sits after the embedded block.
        #expect(parsed.frameOffsets[6] == 6 * SyntheticMP3.frameLength + 74)
    }

    @Test("junk bytes force a resync to the next double-validated header")
    func parseResyncOverJunk() {
        let frames = SyntheticMP3.frames(count: 10, seed: 4)
        // 100 junk bytes that cannot form a valid header pair.
        let junk = [UInt8](repeating: 0x00, count: 100)
        let data = SyntheticMP3.file(Array(frames[0..<5]) + [junk] + Array(frames[5...]))
        let parsed = RediffByteAligner.parse(data)
        #expect(parsed.frameOffsets.count == 10)
        #expect(parsed.frameOffsets[5] == 5 * SyntheticMP3.frameLength + 100)
    }

    @Test("truncated final frame stops the walk (partial frame not recorded)")
    func parseTruncatedFinalFrame() {
        var parts = SyntheticMP3.frames(count: 8, seed: 5)
        parts[7] = Array(parts[7][0..<100])  // cut the last frame short
        let parsed = RediffByteAligner.parse(SyntheticMP3.file(parts))
        #expect(parsed.frameOffsets.count == 7)
    }

    @Test("ID3v1 TAG trailer (exactly 128 bytes at EOF) stops the walk cleanly")
    func parseTagTrailer() {
        let data = SyntheticMP3.file(
            SyntheticMP3.frames(count: 8, seed: 6) + [SyntheticMP3.id3v1Trailer()]
        )
        let parsed = RediffByteAligner.parse(data)
        #expect(parsed.frameOffsets.count == 8)
    }

    @Test("timeAt interpolates linearly inside a frame and clamps at file edges")
    func timeAtInterpolation() {
        let data = SyntheticMP3.file(SyntheticMP3.frames(count: 4, seed: 7))
        let parsed = RediffByteAligner.parse(data)
        #expect(RediffByteAligner.timeAt(parsed, byteOffset: 0) == 0)
        // Midpoint of frame 1.
        let mid = SyntheticMP3.frameLength + SyntheticMP3.frameLength / 2
        let expected = SyntheticMP3.secondsPerFrame + 0.5 * SyntheticMP3.secondsPerFrame
        #expect(abs(RediffByteAligner.timeAt(parsed, byteOffset: mid) - expected) < 0.001)
        // Past EOF clamps to the final frame's end.
        let tail = RediffByteAligner.timeAt(parsed, byteOffset: parsed.sizeBytes + 999)
        #expect(abs(tail - 4 * SyntheticMP3.secondsPerFrame) < 0.001)
    }

    // MARK: - Alignment: insertion / uniqueness / extension

    /// A = content with an ID3-separated ad block inserted mid-way (the real
    /// DAI-stitcher shape — segment boundaries carry ID3 metadata, which is
    /// what keeps run edges from bleeding across identical frame headers);
    /// B = content. Expect one `removed_in_B` slot at byte-exact edges.
    @Test("insertion in A yields one removed_in_B slot with byte-exact edges")
    func insertionInA() throws {
        let content = SyntheticMP3.frames(count: 60, seed: 10)
        let separator = SyntheticMP3.id3v2(payloadBytes: 32)
        var head = Array(content[0..<30])
        SyntheticMP3.pinTailByte(&head, to: 0xAA)  // ≠ ad tail: exact run edges
        var ad = SyntheticMP3.frames(count: 20, seed: 999)
        SyntheticMP3.pinTailByte(&ad, to: 0x55)
        let aData = SyntheticMP3.file(head + [separator] + ad + Array(content[30...]))
        let bData = SyntheticMP3.file(head + Array(content[30...]))
        let alignment = RediffByteAligner.align(
            aData: aData, bData: bData, config: SyntheticMP3.smallRunConfig)
        #expect(alignment.monotonicClean)
        #expect(alignment.chain.count == 2)
        #expect(alignment.slots.count == 1)
        let slot = try #require(alignment.slots.first)
        #expect(slot.kind == .removedInB)
        let spliceByte = 30 * SyntheticMP3.frameLength
        let insertedBytes = separator.count + 20 * SyntheticMP3.frameLength
        // The ID3 separator pins the slot START byte-exactly ('I' ≠ 0xFF at
        // the boundary); the END can retreat a couple of bytes when the
        // pre-boundary payloads coincidentally match (seed-deterministic).
        #expect(slot.aStartByte == spliceByte)
        #expect(abs(slot.aEndByte - (spliceByte + insertedBytes)) <= 8)
        #expect(abs(slot.aStartSeconds - 30 * SyntheticMP3.secondsPerFrame) < 0.01)
        #expect(abs(slot.aEndSeconds - 50 * SyntheticMP3.secondsPerFrame) < 0.01)
        // Flanks: 30 content frames each side of the slot.
        #expect(abs(slot.leftFlankSeconds - 30 * SyntheticMP3.secondsPerFrame) < 0.05)
        #expect(abs(slot.rightFlankSeconds - 30 * SyntheticMP3.secondsPerFrame) < 0.05)
        // Full B coverage.
        #expect(alignment.chainedFractionB > 0.99)
    }

    @Test("insertion in B yields a zero-A-width inserted_in_B slot (filtered out of played slots by the gate)")
    func insertionInB() throws {
        var head = SyntheticMP3.frames(count: 30, seed: 11)
        SyntheticMP3.pinTailByte(&head, to: 0xAA)  // ≠ ad tail: no backward bleed
        let tail = SyntheticMP3.frames(count: 30, seed: 71)
        var ad = SyntheticMP3.frames(count: 20, seed: 998)
        SyntheticMP3.pinTailByte(&ad, to: 0x55)
        let aData = SyntheticMP3.file(head + tail)
        let bData = SyntheticMP3.file(head + [SyntheticMP3.id3v2(payloadBytes: 32)] + ad + tail)
        let alignment = RediffByteAligner.align(
            aData: aData, bData: bData, config: SyntheticMP3.smallRunConfig)
        #expect(alignment.monotonicClean)
        #expect(alignment.slots.count == 1)
        let slot = try #require(alignment.slots.first)
        #expect(slot.kind == .insertedInB)
        #expect(slot.aSeconds < 0.01)
        // The gate must NOT surface a zero-A-width slot as a played slot.
        guard case .accepted(let acceptance) =
            RediffSlotOwnership.gateAndDiffBytes(alignment: alignment) else {
            Issue.record("expected acceptance")
            return
        }
        #expect(acceptance.playedSlots.isEmpty)
    }

    @Test("rotated creative (replaced segment) yields a replaced slot spanning the A fill")
    func replacedSegment() throws {
        // The two flanking runs are separated by the full fill (never adjacent),
        // so no run-overlap / non-monotonic risk. Byte edges can shift by the
        // few identical frame-header bytes the greedy extension crosses between
        // the differently-seeded fills, so assert A/B fill sizes within a
        // small (seed-deterministic) tolerance.
        let content = SyntheticMP3.frames(count: 60, seed: 12)
        let fillA = SyntheticMP3.frames(count: 20, seed: 997)
        let fillB = SyntheticMP3.frames(count: 24, seed: 996)  // different length rotation
        let aData = SyntheticMP3.file(Array(content[0..<30]) + fillA + Array(content[30...]))
        let bData = SyntheticMP3.file(Array(content[0..<30]) + fillB + Array(content[30...]))
        let alignment = RediffByteAligner.align(
            aData: aData, bData: bData, config: SyntheticMP3.smallRunConfig)
        #expect(alignment.monotonicClean)
        #expect(alignment.slots.count == 1)
        let slot = try #require(alignment.slots.first)
        #expect(slot.kind == .replaced)
        #expect(abs(Double(slot.aBytes) - Double(20 * SyntheticMP3.frameLength)) <= 16)
        #expect(abs(Double(slot.bBytes) - Double(24 * SyntheticMP3.frameLength)) <= 16)
    }

    @Test("a frame duplicated within A is not an anchor (uniqueness in BOTH files)")
    func duplicatedFrameIsNotAnAnchor() {
        let content = SyntheticMP3.frames(count: 30, seed: 13)
        // A repeats frame 5 at the end; that frame's hash count in A is 2, so
        // it may not anchor — but unique neighbors still align everything.
        let aData = SyntheticMP3.file(content + [content[5]])
        let bData = SyntheticMP3.file(content)
        let alignment = RediffByteAligner.align(
            aData: aData, bData: bData, config: SyntheticMP3.smallRunConfig)
        #expect(alignment.monotonicClean)
        #expect(alignment.chain.count == 1)
        // The tail gap is A's duplicated trailing frame.
        #expect(alignment.slots.count == 1)
        #expect(alignment.slots[0].kind == .tail)
        #expect(alignment.slots[0].aBytes == SyntheticMP3.frameLength)
    }

    @Test("mid-frame splice: byte extension recovers the exact splice offset")
    func midFrameSpliceOffset() throws {
        let content = SyntheticMP3.frames(count: 40, seed: 14)
        // Corrupt A at an offset INSIDE frame 20 (100 bytes in): everything
        // before it must still be byte-verified into the left run.
        var aBytes = [UInt8](SyntheticMP3.file(content))
        let spliceAt = 20 * SyntheticMP3.frameLength + 100
        for i in spliceAt..<(spliceAt + 5 * SyntheticMP3.frameLength) {
            aBytes[i] = aBytes[i] &+ 0x55
        }
        let alignment = RediffByteAligner.align(
            aData: Data(aBytes), bData: SyntheticMP3.file(content),
            config: SyntheticMP3.smallRunConfig)
        #expect(alignment.monotonicClean)
        let slot = try #require(alignment.slots.first)
        // The left run's byte-exact end IS the splice offset.
        #expect(slot.aStartByte == spliceAt)
    }

    @Test("common regions below minRunBytes yield no runs (gate falls back)")
    func belowMinRunBytes() {
        // Only 3 common frames (1251 bytes) < 2048 minRunBytes.
        let common = SyntheticMP3.frames(count: 3, seed: 15)
        let aData = SyntheticMP3.file(SyntheticMP3.frames(count: 10, seed: 16) + common)
        let bData = SyntheticMP3.file(SyntheticMP3.frames(count: 10, seed: 17) + common)
        let alignment = RediffByteAligner.align(
            aData: aData, bData: bData, config: SyntheticMP3.smallRunConfig)
        #expect(alignment.runsFound == 0)
        #expect(alignment.chain.isEmpty)
        #expect(alignment.slots.isEmpty)  // python parity: no chain → no slots
        #expect(RediffSlotOwnership.gateAndDiffBytes(alignment: alignment)
            == .rejectedNoChainedRuns)
    }

    @Test("wholesale re-encode (disjoint bytes) rejects with no chained runs")
    func wholesaleReencode() {
        let aData = SyntheticMP3.file(SyntheticMP3.frames(count: 40, seed: 18))
        let bData = SyntheticMP3.file(SyntheticMP3.frames(count: 40, seed: 19))
        let alignment = RediffByteAligner.align(
            aData: aData, bData: bData, config: SyntheticMP3.smallRunConfig)
        #expect(alignment.runsFound == 0)
        #expect(RediffSlotOwnership.gateAndDiffBytes(alignment: alignment)
            == .rejectedNoChainedRuns)
    }

    @Test("swapped halves (non-monotonic structure) drop a run and reject")
    func swappedHalvesNonMonotonic() {
        let x = SyntheticMP3.frames(count: 20, seed: 20)
        let y = SyntheticMP3.frames(count: 20, seed: 21)
        let alignment = RediffByteAligner.align(
            aData: SyntheticMP3.file(x + y), bData: SyntheticMP3.file(y + x),
            config: SyntheticMP3.smallRunConfig)
        #expect(alignment.runsFound == 2)
        #expect(alignment.runsDroppedNonMonotonic == 1)
        #expect(!alignment.monotonicClean)
        guard case .rejectedNonMonotonic(let dropped) =
            RediffSlotOwnership.gateAndDiffBytes(alignment: alignment) else {
            Issue.record("expected non-monotonic rejection")
            return
        }
        #expect(dropped == 1)
    }

    @Test("low chained fraction (small island in a re-encoded B) rejects")
    func lowChainedFraction() {
        // 6 common frames inside otherwise-disjoint 60-frame files: fraction
        // ≈ 0.1 < 0.5 floor.
        let island = SyntheticMP3.frames(count: 6, seed: 22)
        let aData = SyntheticMP3.file(
            SyntheticMP3.frames(count: 27, seed: 23) + island + SyntheticMP3.frames(count: 27, seed: 24))
        let bData = SyntheticMP3.file(
            SyntheticMP3.frames(count: 27, seed: 25) + island + SyntheticMP3.frames(count: 27, seed: 26))
        let alignment = RediffByteAligner.align(
            aData: aData, bData: bData, config: SyntheticMP3.smallRunConfig)
        #expect(alignment.runsFound == 1)
        guard case .rejectedLowChainedFraction(let fraction) =
            RediffSlotOwnership.gateAndDiffBytes(alignment: alignment) else {
            Issue.record("expected low-fraction rejection, got \(RediffSlotOwnership.gateAndDiffBytes(alignment: alignment))")
            return
        }
        #expect(fraction < 0.5)
    }

    // MARK: - chainRuns: DP semantics + tie determinism

    private func run(_ a: Int, _ b: Int, _ bytes: Int) -> RediffByteAligner.Run {
        RediffByteAligner.Run(aStart: a, bStart: b, bytes: bytes)
    }

    @Test("chainRuns picks the max-bytes strictly-monotonic subsequence")
    func chainRunsMaxBytes() {
        // r2 conflicts with (r1, r3); r1+r3 (300) beats r2 (250).
        let runs = [run(0, 0, 100), run(50, 150, 250), run(200, 200, 200)]
        let (chain, total, dropped) = RediffByteAligner.chainRuns(runs)
        #expect(chain == [run(0, 0, 100), run(200, 200, 200)])
        #expect(total == 300)
        #expect(dropped == 1)
    }

    @Test("chainRuns requires monotonic non-overlap in BOTH files")
    func chainRunsBothSidesMonotonic() {
        // Second run precedes in B despite following in A — cannot chain.
        let runs = [run(0, 1000, 100), run(500, 0, 100)]
        let (chain, _, dropped) = RediffByteAligner.chainRuns(runs)
        #expect(chain.count == 1)
        #expect(dropped == 1)
    }

    @Test("chainRuns tie-case is deterministic (first maximal index wins)")
    func chainRunsTieDeterminism() {
        // Two disjoint equal-weight chains; the (aStart, bStart)-first one wins.
        let runs = [run(0, 500, 100), run(0, 900, 100)]
        let (chain, total, dropped) = RediffByteAligner.chainRuns(runs)
        #expect(total == 100)
        #expect(dropped == 1)
        #expect(chain == [run(0, 500, 100)])
    }

    // MARK: - gateAndDiffBytes: cleaning parity with the chroma acceptance

    private func alignmentFixture(
        slots: [RediffByteAligner.Slot],
        chained: Int = 1_000_000,
        dropped: Int = 0,
        fraction: Double = 0.95
    ) -> RediffByteAligner.Alignment {
        RediffByteAligner.Alignment(
            runsFound: slots.count + 1 + dropped,
            chain: [RediffByteAligner.Run(aStart: 0, bStart: 0, bytes: chained)],
            runsDroppedNonMonotonic: dropped,
            chainedBytes: chained,
            chainedFractionB: fraction,
            slots: slots,
            aDurationSeconds: 3600,
            bDurationSeconds: 3600
        )
    }

    private func byteSlot(
        _ start: Double, _ end: Double,
        left: Double = 300, right: Double = 300,
        kind: RediffByteAligner.SlotKind = .replaced
    ) -> RediffByteAligner.Slot {
        RediffByteAligner.Slot(
            kind: kind, aStartByte: Int(start * 16000), aEndByte: Int(end * 16000),
            aStartSeconds: start, aEndSeconds: end,
            aBytes: Int((end - start) * 16000), bBytes: 1,
            leftFlankSeconds: left, rightFlankSeconds: right
        )
    }

    @Test("gate filters sub-minAdSeconds slots, fragment-merges, and duration-caps like the chroma path")
    func gateCleaningParity() {
        let alignment = alignmentFixture(slots: [
            byteSlot(10, 12),                      // < 5 s → filtered
            byteSlot(100, 130, left: 200, right: 1),
            byteSlot(131, 160, left: 1, right: 400),  // 1 s gap → merged with previous
            byteSlot(300, 340),                    // separate
            byteSlot(1000, 1600),                  // 600 s > 480 s cap → dropped
        ])
        guard case .accepted(let acceptance) =
            RediffSlotOwnership.gateAndDiffBytes(alignment: alignment) else {
            Issue.record("expected acceptance")
            return
        }
        #expect(acceptance.playedSlots.count == 2)
        let merged = acceptance.playedSlots[0]
        #expect(merged.startSeconds == 100)
        #expect(merged.endSeconds == 160)
        // OUTER flanks carried through the merge.
        #expect(merged.leftRunSeconds == 200)
        #expect(merged.rightRunSeconds == 400)
        #expect(acceptance.playedSlots[1].startSeconds == 300)
    }

    @Test("acceptance surface is A-time only: no runs, no B coordinates")
    func acceptanceSurfaceIsATimeOnly() {
        // Structural pin for the xsdz.28 never-persist-B rule: ByteAcceptance
        // exposes exactly scalar diagnostics + played (A-time) slots.
        let alignment = alignmentFixture(slots: [byteSlot(100, 160)])
        guard case .accepted(let acceptance) =
            RediffSlotOwnership.gateAndDiffBytes(alignment: alignment) else {
            Issue.record("expected acceptance")
            return
        }
        let labels = Mirror(reflecting: acceptance).children.compactMap(\.label)
        #expect(labels == ["chainedFractionB", "runsFound", "runsChained", "playedSlots"])
    }

    // MARK: - xsdz.34 §5 veto gate on BYTE-derived slots (guardrail 2)

    @Test("a byte-derived widening that newly encloses a veto returns .vetoNewlyEnclosed (status quo)")
    func byteDerivedSlotHonorsVetoGate() throws {
        // Byte slot [90, 170] would widen core [100, 160] over a vetoed range
        // [165, 168] the core does NOT intersect → status-quo width.
        let alignment = alignmentFixture(slots: [byteSlot(90, 170)])
        guard case .accepted(let acceptance) =
            RediffSlotOwnership.gateAndDiffBytes(alignment: alignment) else {
            Issue.record("expected acceptance")
            return
        }
        let core = TimeRange(start: 100, end: 160)
        let veto = TimeRange(start: 165, end: 168)
        let (slot, diagnostics) = RediffSlotOwnership.resolveSpan(
            core: core, playedSlots: acceptance.playedSlots, vetoedRanges: [veto])
        #expect(slot == nil)
        #expect(diagnostics.failureReason == .vetoNewlyEnclosed)

        // Control: the SAME byte slot without the veto qualifies and widens.
        let (unvetoed, _) = RediffSlotOwnership.resolveSpan(
            core: core, playedSlots: acceptance.playedSlots, vetoedRanges: [])
        let widened = try #require(unvetoed)
        #expect(widened.startTime == 90)
        #expect(widened.endTime == 170)

        // A veto ALREADY inside the core does not fire (not newly enclosed).
        let insideVeto = TimeRange(start: 120, end: 125)
        let (kept, keptDiag) = RediffSlotOwnership.resolveSpan(
            core: core, playedSlots: acceptance.playedSlots, vetoedRanges: [insideVeto])
        #expect(kept != nil)
        #expect(keptDiag.failureReason == nil)

        // End-to-end through the candidate bundle — the SAME machinery the
        // service runs on byte-derived slots: the vetoed span synthesizes NO
        // slot (status quo) and the diagnostics carry `.vetoNewlyEnclosed`.
        let span = DecodedSpan(
            id: "span-veto", assetId: "asset-veto", firstAtomOrdinal: 0,
            lastAtomOrdinal: 3, startTime: 100, endTime: 160, anchorProvenance: [])
        let bundle = RediffSlotOwnership.candidates(
            decodedSpans: [span],
            atomEvidence: [],
            playedSlots: acceptance.playedSlots,
            vetoedRanges: [veto],
            coreBankMatch: [false],
            slotBankMatch: [false]
        )
        #expect(bundle.synthesizedSlots == [nil])
        #expect(bundle.diagnostics.first?.failureReason == .vetoNewlyEnclosed)
    }
}
