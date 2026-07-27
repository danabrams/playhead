// RediffByteAligner.swift
// playhead-xsdz.57: PRODUCTION port of the byte-run A/B alignment — the PRIMARY
// rediff differ (chroma `RediffDiffer` is the fallback).
//
// PROVENANCE: this is a faithful Swift port of the validated reference
// `scripts/l2f-mp3-forensics.py`, subcommand `align` (playhead-xsdz.44 spike,
// GO verdict 2026-07-17): per-frame content hashes unique in BOTH files are
// anchors; anchors sharing a byte delta are greedily extended to maximal
// BYTE-verified runs; runs are chained by max-bytes strictly-monotonic
// subsequence (non-overlapping in both files); inter-run gaps = inserted /
// removed / replaced spans = ad slots with byte-exact edges, expressed in
// A-time. Spike verdicts on the real corpus: 7/7 pairs monotonic (incl. the 4
// Morbid episodes chroma/fpcalc fail on), 11/11 gold breaks IoU ≥ 0.97,
// |dEnd| median 0.02 s, ~2 s/pair on 100 MB files with NO audio decode.
// `RediffByteAlignerParityTests` pins this port against a checked-in reference
// run of the Python on a real staged A/B pair, so the two cannot silently
// diverge.
//
// PORT FIDELITY NOTES
// -------------------
// * `parse` is the walk-affecting subset of python `parse_mp3`: leading-ID3v2
//   skip, MPEG1/2/2.5 Layer I/II/III header→frame-length tables (integer
//   division preserved), mid-file ID3v2 skip, ID3v1 "TAG" trailer stop,
//   truncated-final-frame stop, and the two-consecutive-valid-headers resync.
//   Side-info/mdb extraction and the anomaly ledger are scars-subcommand-only
//   (they never move `pos`) and are deliberately NOT ported.
// * Frame hashes: python uses blake2b(digest_size=8); CryptoKit has no BLAKE2,
//   so this port takes the first 8 bytes of SHA-256 (the hash the codebase
//   already standardizes on — `EpisodeFingerprintBlobCodec` etc.). The digest
//   is only an intra-pair content-equality proxy feeding the uniqueness rule,
//   so the choice of (cryptographic) hash cannot change semantics.
// * `extendRun` finds the identical maximal equal region as the python chunked
//   compare + binary search (both compute the longest matching prefix/suffix).
// * `chainRuns` ports the O(n²) max-bytes DP including python's first-max tie
//   rule (`max(range(n), key=...)` returns the FIRST maximal index).
// * Sorts that python performs with stable `list.sort` are made explicitly
//   stable here via the enumerated `(key, offset)` pattern `RediffDiffer`
//   already uses (Swift's sort is not stable).
// * Slots reproduce `cmd_align` exactly: NO slots when the chain is empty
//   (the gate rejects that alignment wholesale anyway), head/tail gap hints,
//   and the `ga <= 0 && gb <= 0` skip. The ONLY addition is per-slot flanking
//   run A-seconds (the chained runs adjacent to each gap), which the width
//   oracle's `PlayedSlot` needs for edge confidence.
//
// A-TIME CONTRACT (playhead-xsdz.28 never-persist-B): slot coordinates are
// A-timeline byte offsets/seconds ONLY. `Run.bStart` exists transiently inside
// `Alignment` because chaining needs both coordinates (exactly as the python
// does); the gate layer (`RediffSlotOwnership.gateAndDiffBytes`) strips every
// B coordinate before anything flows onward, and nothing here is persisted.
//
// PURITY: value types + static functions over in-memory bytes. No file I/O —
// the service reads the two files (memory-mapped) and hands `Data` in. No
// actor hops, deterministic, `Foundation` + `CryptoKit` only.

import CryptoKit
import Foundation

enum RediffByteAligner {

    // MARK: - Configuration

    struct Configuration: Sendable, Equatable {
        /// Minimum byte-verified run length to keep (python `--min-run-bytes`
        /// default 65536 — the spike's validated setting).
        var minRunBytes: Int
        /// Chunk size for the greedy byte extension (python `chunk=1<<16`).
        var extensionChunkBytes: Int

        static let `default` = Configuration(minRunBytes: 65536, extensionChunkBytes: 1 << 16)

        init(minRunBytes: Int = 65536, extensionChunkBytes: Int = 1 << 16) {
            self.minRunBytes = minRunBytes
            self.extensionChunkBytes = extensionChunkBytes
        }
    }

    // MARK: - Value types

    /// The walk-affecting subset of a parsed MP3: parallel per-frame arrays
    /// (python `frames.offset/length/time`) plus the file-level fields the
    /// aligner consumes.
    struct ParsedMP3: Sendable, Equatable {
        let frameOffsets: [Int]
        let frameLengths: [Int]
        let frameTimes: [Double]
        let leadingID3Bytes: Int
        let sizeBytes: Int
        let durationSeconds: Double
    }

    /// A maximal byte-verified common run. `bStart` is a transient B-side
    /// coordinate needed for chaining — see the A-TIME CONTRACT note above.
    struct Run: Sendable, Equatable {
        let aStart: Int
        let bStart: Int
        let bytes: Int
    }

    enum SlotKind: String, Sendable, Equatable {
        case head
        case tail
        case replaced
        case removedInB = "removed_in_B"
        case insertedInB = "inserted_in_B"
    }

    /// One inter-run gap in A-time (python `cmd_align` slot). `aBytes == 0`
    /// (pure B insertion) is representable — the gate layer filters by A-width.
    struct Slot: Sendable, Equatable {
        let kind: SlotKind
        let aStartByte: Int
        let aEndByte: Int
        let aStartSeconds: Double
        let aEndSeconds: Double
        let aBytes: Int
        let bBytes: Int
        /// A-seconds spanned by the chained run to the LEFT of this gap
        /// (0 for the head gap). Swift-side addition for edge confidence.
        let leftFlankSeconds: Double
        /// A-seconds spanned by the chained run to the RIGHT of this gap
        /// (0 for the tail gap).
        let rightFlankSeconds: Double

        var aSeconds: Double { aEndSeconds - aStartSeconds }
    }

    /// The full alignment result (python `cmd_align` output shape).
    struct Alignment: Sendable, Equatable {
        /// Runs surviving extension + min-run + containment pruning.
        let runsFound: Int
        /// The chained (strictly-monotonic, non-overlapping) runs.
        let chain: [Run]
        /// `runsFound - chain.count` (python `runs_dropped_nonmonotonic`).
        let runsDroppedNonMonotonic: Int
        /// Total bytes across the chained runs.
        let chainedBytes: Int
        /// `chainedBytes` over B's audio bytes (size minus leading ID3v2) —
        /// the byte-path analogue of the chroma differ's `alignedFractionB`.
        /// A re-encoding CDN (nikki-glaser) collapses this toward 0.
        let chainedFractionB: Double
        /// Inter-run gaps in A-time. EMPTY when the chain is empty (python
        /// parity — the gate rejects a chainless alignment wholesale).
        let slots: [Slot]
        let aDurationSeconds: Double
        let bDurationSeconds: Double

        // playhead-9s6q FIX A — NON-MONOTONIC SEGMENT RECOVERY (ADDITIVE; every
        // field ABOVE is byte-for-byte unchanged, so a consumer that ignores
        // these — i.e. the strict/lagged gate path — is byte-identical to
        // pre-9s6q). When an ad-LENGTH difference (or CBR header-bleed) makes a
        // later break's run overlap an earlier one in B, the max-bytes `chain`
        // DROPS runs and `monotonicClean` is false. Rather than discard a
        // high-`chainedFractionB` fetch full of real divergent ads, `align` ALSO
        // partitions the found runs into contiguous monotonic segments and
        // re-derives the inter-run A-gap slots over the FULL segmented
        // (A-ordered, A-non-overlapping) run set — the UNION of every segment's
        // divergent regions. `RediffSlotOwnership.gateAndDiffBytes` consumes
        // these ONLY behind its (default-OFF) non-monotonic-recovery flag; the
        // strict `slots`/`chainedFractionB` path is otherwise untouched.
        //
        // For a monotonic-clean alignment these MIRROR the single-chain values
        // (one segment == the chain), so recovery is a no-op there.

        /// Divergent A-time slots over the FULL monotonic-segmented run set (the
        /// union across segments). Equals `slots` when `monotonicClean`.
        let segmentedSlots: [Slot]
        /// Σ(segment run bytes) / B audio bytes — the re-encode floor computed
        /// over the SEGMENTED aligned coverage (≥ `chainedFractionB`, since
        /// segmenting keeps runs the single chain dropped). Equals
        /// `chainedFractionB` when `monotonicClean`.
        let segmentedChainedFractionB: Double
        /// Count of runs across all monotonic segments. Equals `chain.count`
        /// when `monotonicClean`.
        let segmentedRunsChained: Int

        var monotonicClean: Bool { runsDroppedNonMonotonic == 0 }

        init(
            runsFound: Int,
            chain: [Run],
            runsDroppedNonMonotonic: Int,
            chainedBytes: Int,
            chainedFractionB: Double,
            slots: [Slot],
            aDurationSeconds: Double,
            bDurationSeconds: Double,
            segmentedSlots: [Slot] = [],
            segmentedChainedFractionB: Double = 0,
            segmentedRunsChained: Int = 0
        ) {
            self.runsFound = runsFound
            self.chain = chain
            self.runsDroppedNonMonotonic = runsDroppedNonMonotonic
            self.chainedBytes = chainedBytes
            self.chainedFractionB = chainedFractionB
            self.slots = slots
            self.aDurationSeconds = aDurationSeconds
            self.bDurationSeconds = bDurationSeconds
            self.segmentedSlots = segmentedSlots
            self.segmentedChainedFractionB = segmentedChainedFractionB
            self.segmentedRunsChained = segmentedRunsChained
        }
    }

    // MARK: - MP3 frame header (exact port of python _parse_header)

    struct FrameHeader: Sendable, Equatable {
        let version: Int          // 0=MPEG2.5, 2=MPEG2, 3=MPEG1 (1=reserved → nil)
        let layer: Int            // 1=III, 2=II, 3=I (0 → nil)
        let crcPresent: Bool
        let kbps: Int
        let samplerate: Int
        let padding: Int
        let channelMode: Int      // 3 = mono
        let frameLength: Int
        let samplesPerFrame: Int
    }

    private static let bitrateV1L3: [Int?] = [nil, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, nil]
    private static let bitrateV2L3: [Int?] = [nil, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, nil]
    private static let bitrateV1L2: [Int?] = [nil, 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384, nil]
    private static let bitrateV1L1: [Int?] = [nil, 32, 64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 384, 416, 448, nil]
    private static let samplerateV1: [Int] = [44100, 48000, 32000]
    private static let samplerateV2: [Int] = [22050, 24000, 16000]
    private static let samplerateV25: [Int] = [11025, 12000, 8000]

    static func parseHeader(_ b0: UInt8, _ b1: UInt8, _ b2: UInt8, _ b3: UInt8) -> FrameHeader? {
        guard b0 == 0xFF, (b1 & 0xE0) == 0xE0 else { return nil }
        let version = Int((b1 >> 3) & 0x3)   // 0=2.5, 1=reserved, 2=MPEG2, 3=MPEG1
        let layer = Int((b1 >> 1) & 0x3)     // 1=III, 2=II, 3=I
        guard version != 1, layer != 0 else { return nil }
        let protection = b1 & 0x1            // 0 ⇒ 16-bit CRC follows header
        let brIdx = Int((b2 >> 4) & 0xF)
        let srIdx = Int((b2 >> 2) & 0x3)
        let padding = Int((b2 >> 1) & 0x1)
        guard srIdx != 3, brIdx != 0, brIdx != 15 else { return nil }
        let channelMode = Int((b3 >> 6) & 0x3)
        let kbpsOptional: Int?
        let spf: Int
        let flen: Int
        let sr: Int
        if version == 3 {
            sr = samplerateV1[srIdx]
            if layer == 1 {
                kbpsOptional = bitrateV1L3[brIdx]; spf = 1152
                flen = 144000 * (kbpsOptional ?? 0) / sr + padding
            } else if layer == 2 {
                kbpsOptional = bitrateV1L2[brIdx]; spf = 1152
                flen = 144000 * (kbpsOptional ?? 0) / sr + padding
            } else {
                kbpsOptional = bitrateV1L1[brIdx]; spf = 384
                flen = (12000 * (kbpsOptional ?? 0) / sr + padding) * 4
            }
        } else {
            sr = version == 2 ? samplerateV2[srIdx] : samplerateV25[srIdx]
            if layer == 1 {
                kbpsOptional = bitrateV2L3[brIdx]; spf = 576
                flen = 72000 * (kbpsOptional ?? 0) / sr + padding
            } else if layer == 2 {
                kbpsOptional = bitrateV2L3[brIdx]; spf = 1152
                flen = 144000 * (kbpsOptional ?? 0) / sr + padding
            } else {
                kbpsOptional = bitrateV1L1[brIdx]; spf = 384
                flen = (12000 * (kbpsOptional ?? 0) / sr + padding) * 4
            }
        }
        guard let kbps = kbpsOptional, flen >= 24 else { return nil }
        return FrameHeader(
            version: version, layer: layer, crcPresent: protection == 0,
            kbps: kbps, samplerate: sr, padding: padding,
            channelMode: channelMode, frameLength: flen, samplesPerFrame: spf
        )
    }

    /// Byte length of an ID3v2 block at `offset`, or 0 (python `_id3v2_size`).
    static func id3v2Size(_ buffer: UnsafeRawBufferPointer, at offset: Int) -> Int {
        guard buffer.count - offset >= 10,
              buffer[offset] == 0x49, buffer[offset + 1] == 0x44, buffer[offset + 2] == 0x33  // "ID3"
        else { return 0 }
        let flags = buffer[offset + 5]
        let sz = (buffer[offset + 6], buffer[offset + 7], buffer[offset + 8], buffer[offset + 9])
        guard sz.0 & 0x80 == 0, sz.1 & 0x80 == 0, sz.2 & 0x80 == 0, sz.3 & 0x80 == 0 else { return 0 }
        let size = (Int(sz.0) << 21) | (Int(sz.1) << 14) | (Int(sz.2) << 7) | Int(sz.3)
        return 10 + size + ((flags & 0x10) != 0 ? 10 : 0)
    }

    // MARK: - Parse (walk-affecting subset of python parse_mp3)

    static func parse(_ data: Data) -> ParsedMP3 {
        data.withUnsafeBytes { parse($0) }
    }

    static func parse(_ mm: UnsafeRawBufferPointer) -> ParsedMP3 {
        let n = mm.count
        var offsets: [Int] = []
        var lengths: [Int] = []
        var times: [Double] = []
        var pos = n >= 10 ? id3v2Size(mm, at: 0) : 0
        let leadingID3 = pos
        var t = 0.0
        while pos + 4 <= n {
            guard let hdr = parseHeader(mm[pos], mm[pos + 1], mm[pos + 2], mm[pos + 3]) else {
                // Mid-file ID3v2 block: skip it wholesale.
                let id3len = id3v2Size(mm, at: pos)
                if id3len > 0, pos + id3len <= n {
                    pos += id3len
                    continue
                }
                // ID3v1 trailer: exactly 128 bytes of "TAG..." at EOF.
                if n - pos == 128, mm[pos] == 0x54, mm[pos + 1] == 0x41, mm[pos + 2] == 0x47 {  // "TAG"
                    break
                }
                // Resync: need two consecutive valid headers (or valid + EOF).
                var scan = pos + 1
                var found: Int?
                while scan + 4 <= n {
                    if let h2 = parseHeader(mm[scan], mm[scan + 1], mm[scan + 2], mm[scan + 3]) {
                        let nxt = scan + h2.frameLength
                        if nxt + 4 > n
                            || parseHeader(mm[nxt], mm[nxt + 1], mm[nxt + 2], mm[nxt + 3]) != nil {
                            found = scan
                            break
                        }
                    }
                    scan += 1
                }
                guard let resyncedPos = found else { break }
                pos = resyncedPos
                continue
            }
            if pos + hdr.frameLength > n { break }  // truncated final frame
            offsets.append(pos)
            lengths.append(hdr.frameLength)
            times.append(t)
            t += Double(hdr.samplesPerFrame) / Double(hdr.samplerate)
            pos += hdr.frameLength
        }
        return ParsedMP3(
            frameOffsets: offsets,
            frameLengths: lengths,
            frameTimes: times,
            leadingID3Bytes: leadingID3,
            sizeBytes: n,
            durationSeconds: t
        )
    }

    // MARK: - Frame hashes (python _frame_hashes; SHA-256/8 stands in for blake2b/8)

    static func frameHashes(_ mm: UnsafeRawBufferPointer, parsed: ParsedMP3) -> [UInt64] {
        var out: [UInt64] = []
        out.reserveCapacity(parsed.frameOffsets.count)
        for (offset, length) in zip(parsed.frameOffsets, parsed.frameLengths) {
            var hasher = SHA256()
            hasher.update(bufferPointer: UnsafeRawBufferPointer(rebasing: mm[offset..<(offset + length)]))
            let digest = hasher.finalize()
            out.append(digest.withUnsafeBytes { $0.loadUnaligned(fromByteOffset: 0, as: UInt64.self) })
        }
        return out
    }

    // MARK: - Greedy byte extension (exact port of python _extend_run)

    /// Extend an already-equal region `[a0, a0+length) == [b0, b0+length)` to
    /// its maximal equal span. Returns the extended `(aStart, bStart, bytes)`.
    static func extendRun(
        _ ma: UnsafeRawBufferPointer,
        _ mb: UnsafeRawBufferPointer,
        aStart: Int,
        bStart: Int,
        length: Int,
        chunk: Int = 1 << 16
    ) -> Run {
        var a0 = aStart
        var b0 = bStart
        var ae = a0 + length
        var be = b0 + length
        let na = ma.count
        let nb = mb.count
        // Extend right.
        while ae < na, be < nb {
            let c = min(chunk, na - ae, nb - be)
            if memcmp(ma.baseAddress! + ae, mb.baseAddress! + be, c) == 0 {
                ae += c
                be += c
            } else {
                // Longest equal prefix within the mismatching chunk (identical
                // result to the python binary search over prefixes).
                var k = 0
                while k < c, ma[ae + k] == mb[be + k] { k += 1 }
                ae += k
                be += k
                break
            }
        }
        // Extend left.
        while a0 > 0, b0 > 0 {
            let c = min(chunk, a0, b0)
            if memcmp(ma.baseAddress! + (a0 - c), mb.baseAddress! + (b0 - c), c) == 0 {
                a0 -= c
                b0 -= c
            } else {
                // Longest equal suffix ending at a0/b0 within the chunk.
                var k = 0
                while k < c, ma[a0 - 1 - k] == mb[b0 - 1 - k] { k += 1 }
                a0 -= k
                b0 -= k
                break
            }
        }
        return Run(aStart: a0, bStart: b0, bytes: ae - a0)
    }

    // MARK: - Byte runs (exact port of python byte_runs, payload_only=False)

    static func byteRuns(
        _ ma: UnsafeRawBufferPointer,
        _ mb: UnsafeRawBufferPointer,
        parsedA: ParsedMP3,
        parsedB: ParsedMP3,
        config: Configuration = .default
    ) -> [Run] {
        let ha = frameHashes(ma, parsed: parsedA)
        let hb = frameHashes(mb, parsed: parsedB)
        var countA: [UInt64: Int] = [:]
        countA.reserveCapacity(ha.count)
        for h in ha { countA[h, default: 0] += 1 }
        var countB: [UInt64: Int] = [:]
        countB.reserveCapacity(hb.count)
        for h in hb { countB[h, default: 0] += 1 }
        // B frame index per hash, only for hashes unique in BOTH files.
        var posB: [UInt64: Int] = [:]
        for (j, h) in hb.enumerated() where countB[h] == 1 && countA[h] == 1 {
            posB[h] = j
        }
        // Anchors in A-frame order (python iterates ha in order).
        var anchors: [(aOff: Int, bOff: Int, frameLen: Int)] = []
        for (i, h) in ha.enumerated() {
            guard let j = posB[h] else { continue }
            anchors.append((parsedA.frameOffsets[i], parsedB.frameOffsets[j], parsedA.frameLengths[i]))
        }
        // Group by byte delta, preserving first-appearance order of deltas
        // (python dict insertion order) so run emission order — and therefore
        // the stable aStart sort below — matches the reference exactly.
        var deltaOrder: [Int] = []
        var byDelta: [Int: [(aOff: Int, bOff: Int, frameLen: Int)]] = [:]
        for anchor in anchors {
            let delta = anchor.aOff - anchor.bOff
            if byDelta[delta] == nil { deltaOrder.append(delta) }
            byDelta[delta, default: []].append(anchor)
        }
        var runs: [Run] = []
        for delta in deltaOrder {
            var group = byDelta[delta] ?? []
            group.sort { ($0.aOff, $0.bOff) < ($1.aOff, $1.bOff) }
            var coveredEnd = -1
            for (aOff, bOff, frameLen) in group {
                if aOff < coveredEnd { continue }
                let run = extendRun(
                    ma, mb, aStart: aOff, bStart: bOff, length: frameLen,
                    chunk: config.extensionChunkBytes
                )
                coveredEnd = run.aStart + run.bytes
                if run.bytes >= config.minRunBytes {
                    runs.append(run)
                }
            }
        }
        // Stable sort by aStart (python list.sort is stable; Swift's is not).
        let sorted = runs.enumerated()
            .sorted { ($0.element.aStart, $0.offset) < ($1.element.aStart, $1.offset) }
            .map(\.element)
        // Dedupe identical/contained same-delta runs (python pruning).
        var pruned: [Run] = []
        for run in sorted {
            if let last = pruned.last,
               run.aStart >= last.aStart,
               run.aStart + run.bytes <= last.aStart + last.bytes,
               run.aStart - run.bStart == last.aStart - last.bStart {
                continue
            }
            pruned.append(run)
        }
        return pruned
    }

    // MARK: - Chaining (exact port of python chain_runs)

    /// Max-total-bytes chain, strictly monotonic and non-overlapping in BOTH
    /// files. Returns the chain plus the dropped-run count.
    static func chainRuns(_ runs: [Run]) -> (chain: [Run], chainedBytes: Int, dropped: Int) {
        guard !runs.isEmpty else { return ([], 0, 0) }
        let sorted = runs.enumerated()
            .sorted { ($0.element.aStart, $0.element.bStart, $0.offset)
                < ($1.element.aStart, $1.element.bStart, $1.offset) }
            .map(\.element)
        let n = sorted.count
        var best = [Int](repeating: 0, count: n)
        var prev = [Int](repeating: -1, count: n)
        for i in 0..<n {
            best[i] = sorted[i].bytes
            for j in 0..<i {
                let q = sorted[j]
                if q.aStart + q.bytes <= sorted[i].aStart, q.bStart + q.bytes <= sorted[i].bStart {
                    let cand = best[j] + sorted[i].bytes
                    if cand > best[i] {
                        best[i] = cand
                        prev[i] = j
                    }
                }
            }
        }
        // First-max tie rule (python `max(range(n), key=...)`).
        var bestIndex = 0
        for i in 1..<n where best[i] > best[bestIndex] { bestIndex = i }
        let total = best[bestIndex]
        var chain: [Run] = []
        var i = bestIndex
        while i != -1 {
            chain.append(sorted[i])
            i = prev[i]
        }
        chain.reverse()
        return (chain, total, runs.count - chain.count)
    }

    // MARK: - Byte offset → A-time (exact port of python _time_at)

    static func timeAt(_ parsed: ParsedMP3, byteOffset: Int) -> Double {
        let offs = parsed.frameOffsets
        guard !offs.isEmpty else { return 0 }
        // bisect_right(offs, byte_off) - 1
        var lo = 0
        var hi = offs.count
        while lo < hi {
            let mid = (lo + hi) / 2
            if offs[mid] <= byteOffset { lo = mid + 1 } else { hi = mid }
        }
        let i = lo - 1
        guard i >= 0 else { return 0 }
        let length = parsed.frameLengths[i]
        let frac = min(max(Double(byteOffset - offs[i]) / Double(length), 0), 1)
        let spfSec = i + 1 < parsed.frameTimes.count
            ? parsed.frameTimes[i + 1] - parsed.frameTimes[i]
            : 1152.0 / 44100.0
        return parsed.frameTimes[i] + frac * spfSec
    }

    // MARK: - Top-level alignment (python cmd_align)

    static func align(aData: Data, bData: Data, config: Configuration = .default) -> Alignment {
        aData.withUnsafeBytes { ma in
            bData.withUnsafeBytes { mb in
                align(ma, mb, config: config)
            }
        }
    }

    static func align(
        _ ma: UnsafeRawBufferPointer,
        _ mb: UnsafeRawBufferPointer,
        config: Configuration = .default
    ) -> Alignment {
        let pa = parse(ma)
        let pb = parse(mb)
        let runs = byteRuns(ma, mb, parsedA: pa, parsedB: pb, config: config)
        let (chain, chainedBytes, dropped) = chainRuns(runs)

        var slots: [Slot] = []
        func addGap(a0: Int, a1: Int, b0: Int, b1: Int, kindHint: SlotKind?, leftFlank: Double, rightFlank: Double) {
            let ga = a1 - a0
            let gb = b1 - b0
            if ga <= 0 && gb <= 0 { return }
            let kind: SlotKind
            if let kindHint {
                kind = kindHint
            } else if ga > 0 && gb > 0 {
                kind = .replaced
            } else if ga > 0 {
                kind = .removedInB
            } else {
                kind = .insertedInB
            }
            slots.append(Slot(
                kind: kind,
                aStartByte: a0,
                aEndByte: a1,
                aStartSeconds: timeAt(pa, byteOffset: a0),
                aEndSeconds: timeAt(pa, byteOffset: a1),
                aBytes: ga,
                bBytes: gb,
                leftFlankSeconds: leftFlank,
                rightFlankSeconds: rightFlank
            ))
        }
        func runASeconds(_ run: Run) -> Double {
            timeAt(pa, byteOffset: run.aStart + run.bytes) - timeAt(pa, byteOffset: run.aStart)
        }
        if let first = chain.first, let last = chain.last {
            addGap(
                a0: pa.leadingID3Bytes, a1: first.aStart,
                b0: pb.leadingID3Bytes, b1: first.bStart,
                kindHint: .head, leftFlank: 0, rightFlank: runASeconds(first)
            )
            for (left, right) in zip(chain, chain.dropFirst()) {
                addGap(
                    a0: left.aStart + left.bytes, a1: right.aStart,
                    b0: left.bStart + left.bytes, b1: right.bStart,
                    kindHint: nil, leftFlank: runASeconds(left), rightFlank: runASeconds(right)
                )
            }
            addGap(
                a0: last.aStart + last.bytes, a1: pa.sizeBytes,
                b0: last.bStart + last.bytes, b1: pb.sizeBytes,
                kindHint: .tail, leftFlank: runASeconds(last), rightFlank: 0
            )
        }
        let bAudioBytes = max(1, pb.sizeBytes - pb.leadingID3Bytes)
        let chainedFractionB = Double(chainedBytes) / Double(bAudioBytes)

        // playhead-9s6q FIX A: when the chain had to DROP runs (non-monotonic),
        // ALSO derive the divergent slots over the FULL monotonic-segmented run
        // set so a high-coverage fetch's real ads are recoverable (behind the
        // gate's opt-in flag) instead of discarded wholesale. For a
        // monotonic-clean alignment the single chain IS the only segment, so
        // mirror the chain values and do no extra work.
        let segmentedSlots: [Slot]
        let segmentedChainedFractionB: Double
        let segmentedRunsChained: Int
        if dropped == 0 {
            segmentedSlots = slots
            segmentedChainedFractionB = chainedFractionB
            segmentedRunsChained = chain.count
        } else {
            let seg = segmentDivergentSlots(runs: runs, pa: pa, pb: pb, bAudioBytes: bAudioBytes)
            segmentedSlots = seg.slots
            segmentedChainedFractionB = seg.chainedFractionB
            segmentedRunsChained = seg.runsChained
        }

        return Alignment(
            runsFound: runs.count,
            chain: chain,
            runsDroppedNonMonotonic: dropped,
            chainedBytes: chainedBytes,
            chainedFractionB: chainedFractionB,
            slots: slots,
            aDurationSeconds: pa.durationSeconds,
            bDurationSeconds: pb.durationSeconds,
            segmentedSlots: segmentedSlots,
            segmentedChainedFractionB: segmentedChainedFractionB,
            segmentedRunsChained: segmentedRunsChained
        )
    }

    // MARK: - Non-monotonic segment recovery (playhead-9s6q FIX A)

    /// Re-derive divergent A-time slots when the max-bytes `chain` had to DROP
    /// runs (non-monotonic). Partition the found runs into contiguous monotonic
    /// segments and UNION their divergent regions: walk the runs in A-order,
    /// keep an A-non-overlapping accepted set (a later run that A-overlaps an
    /// already-kept run is dropped — the same conflict `chainRuns` resolves when
    /// it drops a run), then emit the inter-run A-gaps (plus head/tail) as slots
    /// EXACTLY as `align` does for a single chain. A segment BOUNDARY — where B
    /// jumps backward because an ad's length differs between A and B — simply
    /// appears as an inter-run gap whose B-width is ≤ 0 (a `removed_in_B` /
    /// `replaced` divergence), which is precisely the rotated ad that made the
    /// chain non-monotonic. The union of these gaps across segments is the
    /// recovered ad set.
    ///
    /// PRECISION (why segmenting cannot manufacture a spurious slot): every run
    /// is already ≥ `minRunBytes` (from `byteRuns`), so a segment cannot be
    /// built from sub-min-run noise; the returned `chainedFractionB`
    /// (Σ accepted run bytes / B audio bytes) lets the gate keep its re-encode
    /// floor over the segmented coverage; and the gate's `minAdSeconds` filter
    /// drops sub-ad gaps. B coordinates feed only the gap KIND and the flank
    /// seconds — the A-timeline slot edges are byte-exact off the aligned runs.
    static func segmentDivergentSlots(
        runs: [Run], pa: ParsedMP3, pb: ParsedMP3, bAudioBytes: Int
    ) -> (slots: [Slot], chainedFractionB: Double, runsChained: Int) {
        guard !runs.isEmpty else { return ([], 0, 0) }
        // The SAME stable order `chainRuns` uses.
        let sorted = runs.enumerated()
            .sorted { ($0.element.aStart, $0.element.bStart, $0.offset)
                < ($1.element.aStart, $1.element.bStart, $1.offset) }
            .map(\.element)
        // A-ordered, A-non-overlapping accepted set. Dropping a later
        // A-overlapper (the conflict `chainRuns` also resolves) keeps the gap
        // arithmetic below well-formed (every A-gap ≥ 0).
        var accepted: [Run] = []
        var globalAEnd = -1
        for run in sorted {
            if run.aStart < globalAEnd { continue }
            accepted.append(run)
            globalAEnd = run.aStart + run.bytes
        }
        let chainedBytes = accepted.reduce(0) { $0 + $1.bytes }

        var slots: [Slot] = []
        func addGap(a0: Int, a1: Int, b0: Int, b1: Int, kindHint: SlotKind?, leftFlank: Double, rightFlank: Double) {
            let ga = a1 - a0
            let gb = b1 - b0
            if ga <= 0 && gb <= 0 { return }
            let kind: SlotKind
            if let kindHint {
                kind = kindHint
            } else if ga > 0 && gb > 0 {
                kind = .replaced
            } else if ga > 0 {
                kind = .removedInB
            } else {
                kind = .insertedInB
            }
            slots.append(Slot(
                kind: kind,
                aStartByte: a0,
                aEndByte: a1,
                aStartSeconds: timeAt(pa, byteOffset: a0),
                aEndSeconds: timeAt(pa, byteOffset: a1),
                aBytes: ga,
                bBytes: gb,
                leftFlankSeconds: leftFlank,
                rightFlankSeconds: rightFlank
            ))
        }
        func runASeconds(_ run: Run) -> Double {
            timeAt(pa, byteOffset: run.aStart + run.bytes) - timeAt(pa, byteOffset: run.aStart)
        }
        if let first = accepted.first, let last = accepted.last {
            addGap(
                a0: pa.leadingID3Bytes, a1: first.aStart,
                b0: pb.leadingID3Bytes, b1: first.bStart,
                kindHint: .head, leftFlank: 0, rightFlank: runASeconds(first)
            )
            for (left, right) in zip(accepted, accepted.dropFirst()) {
                addGap(
                    a0: left.aStart + left.bytes, a1: right.aStart,
                    b0: left.bStart + left.bytes, b1: right.bStart,
                    kindHint: nil, leftFlank: runASeconds(left), rightFlank: runASeconds(right)
                )
            }
            addGap(
                a0: last.aStart + last.bytes, a1: pa.sizeBytes,
                b0: last.bStart + last.bytes, b1: pb.sizeBytes,
                kindHint: .tail, leftFlank: runASeconds(last), rightFlank: 0
            )
        }
        let fraction = Double(chainedBytes) / Double(max(1, bAudioBytes))
        return (slots, fraction, accepted.count)
    }
}
