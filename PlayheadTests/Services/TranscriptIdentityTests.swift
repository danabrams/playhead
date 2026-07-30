// TranscriptIdentityTests.swift
// Unit tests for Phase 1 transcript identity types: TranscriptAtomizer,
// TranscriptSegmenter, TranscriptQualityEstimator, and TranscriptChunk migration.

import CryptoKit
import Foundation
import SQLite3
import Testing
@testable import Playhead

// MARK: - Helpers

private func makeChunk(
    id: String = UUID().uuidString,
    assetId: String = "asset-1",
    chunkIndex: Int = 0,
    startTime: Double = 0,
    endTime: Double = 5,
    text: String = "hello world",
    pass: String = "final",
    weakAnchorMetadata: TranscriptWeakAnchorMetadata? = nil,
    avgConfidence: Float? = nil
) -> TranscriptChunk {
    TranscriptChunk(
        id: id,
        analysisAssetId: assetId,
        segmentFingerprint: "fp-\(id)",
        chunkIndex: chunkIndex,
        startTime: startTime,
        endTime: endTime,
        text: text,
        normalizedText: text.lowercased(),
        pass: pass,
        modelVersion: "speech-v1",
        transcriptVersion: nil,
        atomOrdinal: nil,
        weakAnchorMetadata: weakAnchorMetadata,
        avgConfidence: avgConfidence
    )
}

private func makeAtom(
    assetId: String = "asset-1",
    version: String = "abc123",
    ordinal: Int = 0,
    startTime: Double = 0,
    endTime: Double = 5,
    text: String = "hello world"
) -> TranscriptAtom {
    TranscriptAtom(
        atomKey: TranscriptAtomKey(
            analysisAssetId: assetId,
            transcriptVersion: version,
            atomOrdinal: ordinal
        ),
        contentHash: "deadbeef",
        startTime: startTime,
        endTime: endTime,
        text: text,
        chunkIndex: ordinal
    )
}

private func makeFeatureWindow(
    assetId: String = "asset-1",
    startTime: Double,
    endTime: Double,
    musicProbability: Double = 0.0,
    speakerChangeProxyScore: Double = 0.0,
    musicBedChangeScore: Double = 0.0,
    pauseProbability: Double = 0.1,
    speakerClusterId: Int?
) -> FeatureWindow {
    FeatureWindow(
        analysisAssetId: assetId,
        startTime: startTime,
        endTime: endTime,
        rms: 0.4,
        spectralFlux: 0.1,
        musicProbability: musicProbability,
        speakerChangeProxyScore: speakerChangeProxyScore,
        musicBedChangeScore: musicBedChangeScore,
        pauseProbability: pauseProbability,
        speakerClusterId: speakerClusterId,
        jingleHash: nil,
        featureVersion: 1
    )
}

// MARK: - TranscriptAtomizer Tests

@Suite("TranscriptAtomizer")
struct TranscriptAtomizerTests {

    @Test("Atomize produces one atom per chunk with correct ordinals")
    func atomizeBasic() {
        let chunks = (0..<5).map { i in
            makeChunk(chunkIndex: i, startTime: Double(i) * 10, endTime: Double(i) * 10 + 9)
        }

        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: "asset-1",
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )

        #expect(atoms.count == 5)
        for (i, atom) in atoms.enumerated() {
            #expect(atom.atomKey.atomOrdinal == i)
            #expect(atom.atomKey.analysisAssetId == "asset-1")
            #expect(atom.atomKey.transcriptVersion == version.transcriptVersion)
            #expect(atom.chunkIndex == i)
        }
        #expect(!version.transcriptVersion.isEmpty)
        #expect(version.normalizationHash == "norm-v1")
        #expect(version.sourceHash == "asr-v1")
    }

    @Test("Atomize is deterministic — same input produces same version hash")
    func atomizeDeterministic() {
        let chunks = [makeChunk(chunkIndex: 0, text: "foo"), makeChunk(chunkIndex: 1, text: "bar")]

        let (_, v1) = TranscriptAtomizer.atomize(chunks: chunks, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")
        let (_, v2) = TranscriptAtomizer.atomize(chunks: chunks, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")

        #expect(v1.transcriptVersion == v2.transcriptVersion)
    }

    @Test("Atomize sorts by time regardless of input order")
    func atomizeSortsInput() {
        // playhead-r5um: this used to be "sorts by chunkIndex", and passed for
        // the wrong reason — every chunk carried `makeChunk`'s default
        // 0…5 span, so the comparator never got past the tie. Real, distinct
        // times now, with chunkIndex DESCENDING against them, so the test
        // fails if the ordering key regresses to chunkIndex.
        let chunks = [
            makeChunk(chunkIndex: 0, startTime: 20, endTime: 30, text: "third"),
            makeChunk(chunkIndex: 2, startTime: 0, endTime: 10, text: "first"),
            makeChunk(chunkIndex: 1, startTime: 10, endTime: 20, text: "second"),
        ]

        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: chunks, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s"
        )

        #expect(atoms.map(\.text) == ["first", "second", "third"])
        #expect(atoms.map(\.startTime) == [0, 10, 20])
        // The persisted chunkIndex rides along untouched — it is diagnostic,
        // not the ordering key, and it is not the atom ordinal either.
        #expect(atoms.map(\.chunkIndex) == [2, 1, 0])
    }

    @Test("chunkIndex breaks ties only when the spans are identical")
    func atomizeUsesChunkIndexOnlyAsTiebreak() {
        // Same span, different chunkIndex ⇒ chunkIndex decides. This is the
        // behavior the old `atomizeSortsInput` was actually exercising, kept
        // deliberately: several sibling tests in this suite lean on it (they
        // all use the default 0…5 span), and without it the comparator would
        // fall through to a random UUID and make them flaky.
        let chunks = [
            makeChunk(chunkIndex: 2, text: "third"),
            makeChunk(chunkIndex: 0, text: "first"),
            makeChunk(chunkIndex: 1, text: "second"),
        ]

        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: chunks, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s"
        )

        #expect(atoms.map(\.text) == ["first", "second", "third"])
    }

    @Test("Atomize empty chunks returns empty")
    func atomizeEmpty() {
        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: [], analysisAssetId: "a", normalizationHash: "n", sourceHash: "s"
        )
        #expect(atoms.isEmpty)
    }

    @Test("Different content produces different version hashes")
    func atomizeDifferentContent() {
        let chunks1 = [makeChunk(chunkIndex: 0, text: "foo")]
        let chunks2 = [makeChunk(chunkIndex: 0, text: "bar")]

        let (_, v1) = TranscriptAtomizer.atomize(chunks: chunks1, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")
        let (_, v2) = TranscriptAtomizer.atomize(chunks: chunks2, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")

        #expect(v1.transcriptVersion != v2.transcriptVersion)
    }

    @Test("Content hashes are non-empty and differ for different content")
    func contentHashesCorrect() {
        let chunks = [
            makeChunk(chunkIndex: 0, text: "alpha"),
            makeChunk(chunkIndex: 1, text: "beta"),
        ]

        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: chunks, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s"
        )

        #expect(!atoms[0].contentHash.isEmpty)
        #expect(!atoms[1].contentHash.isEmpty)
        #expect(atoms[0].contentHash != atoms[1].contentHash)
    }

    @Test("Reordered input produces identical version hash")
    func reorderDeterminism() {
        let a = makeChunk(chunkIndex: 0, text: "first")
        let b = makeChunk(chunkIndex: 1, text: "second")
        let c = makeChunk(chunkIndex: 2, text: "third")

        let (_, v1) = TranscriptAtomizer.atomize(chunks: [a, b, c], analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")
        let (_, v2) = TranscriptAtomizer.atomize(chunks: [c, a, b], analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")

        #expect(v1.transcriptVersion == v2.transcriptVersion)
    }

    @Test("Different chunk boundaries produce different version hashes")
    func chunkBoundaryAmbiguity() {
        // ["ab", "cd"] vs ["a", "bcd"] — must NOT collide
        let chunks1 = [makeChunk(chunkIndex: 0, text: "ab"), makeChunk(chunkIndex: 1, text: "cd")]
        let chunks2 = [makeChunk(chunkIndex: 0, text: "a"), makeChunk(chunkIndex: 1, text: "bcd")]

        let (_, v1) = TranscriptAtomizer.atomize(chunks: chunks1, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")
        let (_, v2) = TranscriptAtomizer.atomize(chunks: chunks2, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s")

        #expect(v1.transcriptVersion != v2.transcriptVersion)
    }

    @Test("Single chunk produces single atom with ordinal 0")
    func singleChunk() {
        let chunks = [makeChunk(chunkIndex: 0, text: "only one")]

        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: chunks, analysisAssetId: "a", normalizationHash: "n", sourceHash: "s"
        )

        #expect(atoms.count == 1)
        #expect(atoms[0].atomKey.atomOrdinal == 0)
        #expect(!version.transcriptVersion.isEmpty)
    }
}

// MARK: - Atom time-order regression (playhead-r5um)

/// Device-shape regression for playhead-r5um: `TranscriptAtomizer.atomize`
/// ordered atoms by `chunkIndex`, which is not time order.
///
/// ROOT CAUSE, for anyone reading a failure here. `chunkIndex` comes from
/// `TranscriptEngineService.chunkCounter`, an in-memory counter incremented in
/// shard EMISSION order — and `prioritizeShards` emits by playhead proximity
/// (`shard0 + hotPath + coldAhead + behindWithoutShard0`, that last group
/// DESCENDING), not by time. The counter also resets to 0 on every engine
/// stop or asset switch, so one asset carries several rows numbered 0, 1, 2…
/// So `chunkIndex` is monotone in the order audio was TRANSCRIBED and has
/// never been monotone in the order audio is HEARD. `playhead-0sro` recorded
/// exactly this property when it made the coverage watermark monotonic; the
/// atom sequence was the second consumer to read the counter as a clock.
///
/// The fixtures below are verbatim `(chunkIndex, startTime, endTime, pass)`
/// tuples from the 2026-07-30 device pull, in `rowid` order. Text is
/// synthetic: ordering is what is under test, and readable labels make a
/// failure diagnosable.
@Suite("TranscriptAtom time order (playhead-r5um)")
struct TranscriptAtomTimeOrderTests {

    // MARK: - Helpers

    /// The pre-r5um atom order: ascending `chunkIndex`, ties left in the order
    /// SQLite returned the rows (`rowid`, i.e. insertion). Reproduced locally
    /// because production can no longer produce it — which is the point.
    private func legacyChunkIndexOrder(_ chunks: [TranscriptChunk]) -> [TranscriptChunk] {
        chunks.enumerated()
            .sorted { lhs, rhs in
                lhs.element.chunkIndex == rhs.element.chunkIndex
                    ? lhs.offset < rhs.offset
                    : lhs.element.chunkIndex < rhs.element.chunkIndex
            }
            .map(\.element)
    }

    /// Most negative step between consecutive start times; 0 when monotone.
    private func worstBackwardStep(_ starts: [Double]) -> Double {
        guard starts.count > 1 else { return 0 }
        return min(0, (1..<starts.count).map { starts[$0] - starts[$0 - 1] }.min() ?? 0)
    }

    private func atomize(_ chunks: [TranscriptChunk], asset: String = "asset-r5um")
        -> (atoms: [TranscriptAtom], version: TranscriptVersion) {
        TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: asset,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )
    }

    private func row(
        _ id: String,
        _ index: Int,
        _ start: Double,
        _ end: Double,
        _ pass: String
    ) -> TranscriptChunk {
        makeChunk(
            id: id,
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: "t=\(start)",
            pass: pass
        )
    }

    // MARK: - Fixtures

    /// Asset `8FECFDDE-BA12-4635-B1FC-4D7B775CCDF0` — MIXED fast/final, the
    /// worst backward step on the pull. Three rows are numbered `chunkIndex 0`:
    /// two byte-identical `(0.42, 1.08, fast)` rows (a duplicated child row,
    /// the playhead-6av0 family — same `segmentFingerprint`, distinct ids) and
    /// one at `(3540.0, 3540.42, fast)`, written after the counter restarted.
    /// The episode runs ~59 minutes, so reading this in `chunkIndex` order put
    /// its final 30 seconds at ordinal 2.
    private func mixedDeviceAsset() -> [TranscriptChunk] {
        [
            row("BD93A7C2", 0, 0.42, 1.08, "fast"),
            row("26155B83", 0, 0.42, 1.08, "fast"),
            row("66D39949", 0, 3540.00, 3540.42, "fast"),
            row("4E8791B8", 1, 1.08, 1.62, "fast"),
            row("34143C8D", 1, 1.08, 1.62, "fast"),
            row("BC1033EA", 1, 3540.42, 3540.72, "fast"),
            row("02EE7877", 2, 1.62, 2.16, "fast"),
            row("30641497", 2, 1.62, 2.16, "fast"),
            row("BD5DEB31", 2, 3540.78, 3540.90, "fast"),
            row("45ABD5D2", 3, 2.16, 3.36, "fast"),
            row("32022A12", 3, 2.16, 3.36, "fast"),
            row("13DDECE5", 3, 3540.90, 3541.14, "fast"),
            // The final pass re-transcribed [3538.62, 3539.16] and was
            // persisted far above every fast index, per
            // `FinalPassRetranscriptionRunner.nextFinalChunkIndex`.
            row("3709FAST", 3709, 3538.62, 3539.16, "fast"),
            row("3954FINL", 3954, 3538.62, 3539.16, "final"),
        ]
    }

    /// Asset `594732D7-012F-45ED-98B8-655F943173C8` — SINGLE-PASS (all
    /// `final`), and therefore the case `TranscriptChunkCanonicalizer` passes
    /// through untouched by design. Three transcription sessions each restarted
    /// the counter at 0, so every `chunkIndex` names three unrelated moments —
    /// ~2.6 s, ~270 s and ~1470 s into the episode. Chunk indices 4–7 are
    /// elided; they continue the same three-way interleave.
    private func singlePassDeviceAsset() -> [TranscriptChunk] {
        [
            row("50257", 0, 2.64, 3.30, "final"),
            row("52158", 0, 270.30, 270.72, "final"),
            row("53613", 0, 1470.48, 1471.98, "final"),
            row("50258", 1, 3.30, 4.50, "final"),
            row("52159", 1, 270.72, 270.78, "final"),
            row("53614", 1, 1471.98, 1472.46, "final"),
            row("50259", 2, 4.50, 6.78, "final"),
            row("52160", 2, 270.78, 270.84, "final"),
            row("53615", 2, 1472.46, 1477.98, "final"),
            row("50260", 3, 6.84, 12.30, "final"),
            row("52161", 3, 270.84, 271.02, "final"),
            row("53616", 3, 1477.98, 1479.48, "final"),
            row("50265", 8, 15.66, 16.50, "final"),
            row("52166", 8, 274.02, 274.32, "final"),
            row("53621", 8, 1486.68, 1487.64, "final"),
            row("50266", 9, 16.50, 16.86, "final"),
            row("52167", 9, 274.32, 274.56, "final"),
            row("53622", 9, 1487.64, 1488.30, "final"),
            row("50267", 10, 16.86, 17.34, "final"),
            row("52168", 10, 274.56, 274.86, "final"),
            row("53623", 10, 1488.30, 1488.72, "final"),
        ]
    }

    // MARK: - 1. The mixed-pass device shape (worst jump on the pull)

    @Test("mixed-pass device asset: the -3538.9 s backward jump is gone")
    func mixedDeviceAssetIsTimeOrdered() {
        let chunks = mixedDeviceAsset()

        // The fixture really does carry the device defect. Without this the
        // test could pass over a shape that was never broken.
        let legacyStarts = legacyChunkIndexOrder(chunks).map(\.startTime)
        #expect(abs(worstBackwardStep(legacyStarts) - (-3538.92)) < 0.005)

        let (atoms, _) = atomize(chunks)

        #expect(atoms.count == chunks.count)
        #expect(worstBackwardStep(atoms.map(\.startTime)) == 0)

        // Specifically: the episode's tail no longer sits at the head. Under
        // the old order the t=3540 s row was atom ordinal 2 of 14.
        let tailOrdinal = try? #require(
            atoms.first { $0.startTime == 3540.00 }?.atomKey.atomOrdinal
        )
        #expect(tailOrdinal == 10)
    }

    // MARK: - 2. The single-pass device shape (the unprotected case)

    @Test("single-pass device asset: canonicalizer passes it through, atomize orders it")
    func singlePassDeviceAssetIsTimeOrdered() {
        let chunks = singlePassDeviceAsset()

        // This is genuinely the unprotected shape: hc7e returns single-pass
        // input verbatim, so nothing upstream of `atomize` reorders it. If
        // this stops being a passthrough the test below proves nothing.
        let canonical = TranscriptChunkCanonicalizer.canonicalize(chunks)
        #expect(canonical.diagnostics.isPassthrough)
        #expect(canonical.chunks.map(\.id) == chunks.map(\.id))

        let legacyStarts = legacyChunkIndexOrder(canonical.chunks).map(\.startTime)
        #expect(abs(worstBackwardStep(legacyStarts) - (-1470.78)) < 0.005)

        // The passthrough array itself is NOT time-ordered — that is the trap
        // `AdDetectionService.runBackfill` now sorts around before handing
        // `canonicalChunks` to the consumers that read it raw instead of
        // atomizing (`LexicalAnchorRefiner.buildWordStream`, the
        // `RegionShadowPhase` input). Pin both halves: it is broken as
        // returned, and the shared comparator repairs it.
        #expect(worstBackwardStep(canonical.chunks.map(\.startTime)) < 0)
        #expect(worstBackwardStep(
            canonical.chunks
                .sorted(by: TranscriptChunkCanonicalizer.canonicalTimeOrder)
                .map(\.startTime)
        ) == 0)

        let (atoms, _) = atomize(canonical.chunks)

        #expect(atoms.count == chunks.count)
        #expect(worstBackwardStep(atoms.map(\.startTime)) == 0)
        // The three interleaved sessions are separated back out: everything
        // from the ~2.6 s session precedes the ~270 s one precedes the
        // ~1470 s one.
        #expect(atoms.map(\.startTime) == chunks.map(\.startTime).sorted())
    }

    // MARK: - 3. The no-op rail

    @Test("already-ordered input is returned byte-identically, hash included")
    func alreadyOrderedInputIsUnchanged() {
        // Ordinary well-behaved transcript: contiguous, ascending, chunkIndex
        // in lockstep with time. Nothing about it may move.
        let ordered = (0..<8).map { i in
            row("row-\(i)", i, Double(i) * 30, Double(i) * 30 + 30, "fast")
        }

        let (atoms, version) = atomize(ordered)

        #expect(atoms.map(\.text) == ordered.map(\.text))
        #expect(atoms.map(\.startTime) == ordered.map(\.startTime))
        #expect(atoms.map(\.endTime) == ordered.map(\.endTime))
        #expect(atoms.map(\.chunkIndex) == ordered.map(\.chunkIndex))

        // And the HASH is unchanged, not merely the sequence. Recomputed here
        // from the atomizer's documented recipe — length-prefixed
        // `normalizedText`, SHA-256, first 16 bytes hex — over the INPUT
        // order. r5um moved the ordering key and nothing else; if the digest
        // recipe ever changes, every persisted `transcriptVersion` silently
        // re-keys and this is the tripwire.
        var hasher = SHA256()
        for chunk in ordered {
            let textData = Data(chunk.normalizedText.utf8)
            withUnsafeBytes(of: UInt32(textData.count).bigEndian) {
                hasher.update(bufferPointer: $0)
            }
            hasher.update(data: textData)
        }
        let expected = hasher.finalize().prefix(16).map { String(format: "%02x", $0) }.joined()
        #expect(version.transcriptVersion == expected)
    }

    // MARK: - 4. Determinism where chunkIndex collides

    @Test("colliding chunkIndex still yields one deterministic atom sequence")
    func collidingChunkIndexIsDeterministic() {
        // 21 of 30 device assets carry duplicate chunkIndex values; the old
        // comparator (`chunkIndex` alone) left those ties unresolved, and
        // `Array.sorted(by:)` is not guaranteed stable — so the atom sequence,
        // and therefore `transcriptVersion`, was undefined for them.
        let chunks = mixedDeviceAsset()
        let (baseAtoms, baseVersion) = atomize(chunks)

        for permutation in [Array(chunks.reversed()), chunks.shuffled(), chunks.shuffled()] {
            let (atoms, version) = atomize(permutation)
            #expect(atoms.map(\.text) == baseAtoms.map(\.text))
            #expect(atoms.map(\.startTime) == baseAtoms.map(\.startTime))
            #expect(version.transcriptVersion == baseVersion.transcriptVersion)
        }

    }

    @Test("the id tiebreak decides rows that tie on span, pass AND chunkIndex")
    func idTiebreakResolvesOtherwiseIdenticalKeys() {
        // The device's two rows at (0.42, 1.08, fast, chunkIndex 0) are byte
        // duplicates, so their relative order is unobservable and a missing
        // tiebreak would hide there. It does not hide here: a re-transcription
        // that restarted the counter can re-emit the SAME span with DIFFERENT
        // text (DAI rotates the ad in that slot between fetches — device asset
        // C25FF8CF has "T. Rowe Price" and "Borgata" both covering [0,30]).
        // Then the tie is on span, pass and chunkIndex, the texts differ, and
        // an unresolved tie makes `transcriptVersion` depend on row arrival
        // order.
        let tied = [
            makeChunk(id: "bbb", chunkIndex: 0, startTime: 0, endTime: 30,
                      text: "borgata online", pass: "fast"),
            makeChunk(id: "aaa", chunkIndex: 0, startTime: 0, endTime: 30,
                      text: "t rowe price", pass: "fast"),
        ]

        let (atoms, version) = atomize(tied)
        #expect(atoms.map(\.text) == ["t rowe price", "borgata online"])

        let (reversedAtoms, reversedVersion) = atomize(Array(tied.reversed()))
        #expect(reversedAtoms.map(\.text) == atoms.map(\.text))
        #expect(reversedVersion.transcriptVersion == version.transcriptVersion)
    }

    // MARK: - 5. Segment/discourse spans cannot invert

    @Test("segment and discourse-unit bounds are min/max, not first/last")
    func spanProjectionsCannotInvert() {
        // The real csbq device span pair, in the order the model emitted it:
        // `first`/`last` reported 1978.86 -> 1934.00, a window ending 45 s
        // before it begins, and that reached `semantic_scan_results` with
        // status `success`.
        let inverted = [
            makeAtom(ordinal: 0, startTime: 1978.86, endTime: 1979.00, text: "later"),
            makeAtom(ordinal: 1, startTime: 1933.02, endTime: 1934.00, text: "earlier"),
        ]
        let segment = AdTranscriptSegment(atoms: inverted, segmentIndex: 0)
        #expect(segment.startTime == 1933.02)
        #expect(segment.endTime == 1979.00)
        #expect(segment.duration >= 0)
        #expect(DiscourseUnit(ref: "S0", atoms: inverted).duration >= 0)

        // NESTING, which the canonicalizer reaches by design: it retains a
        // fast chunk that only partially overlaps a final interval, so a long
        // atom can be followed by a short one. `atoms.last.endTime` would
        // understate the span by 38 s even though the atoms ARE time-ordered.
        let nested = [
            makeAtom(ordinal: 0, startTime: 10, endTime: 50, text: "long final"),
            makeAtom(ordinal: 1, startTime: 11, endTime: 12, text: "short fast"),
        ]
        #expect(AdTranscriptSegment(atoms: nested, segmentIndex: 0).endTime == 50)
        #expect(DiscourseUnit(ref: "S0", atoms: nested).endTime == 50)

        // NO-OP RAIL: for ordinary non-overlapping time-ordered atoms, min/max
        // must be byte-identical to first/last.
        let tidy = (0..<5).map { i in
            makeAtom(ordinal: i, startTime: Double(i) * 10, endTime: Double(i) * 10 + 9)
        }
        let tidySegment = AdTranscriptSegment(atoms: tidy, segmentIndex: 0)
        #expect(tidySegment.startTime == tidy.first?.startTime)
        #expect(tidySegment.endTime == tidy.last?.endTime)
        let tidyUnit = DiscourseUnit(ref: "S0", atoms: tidy)
        #expect(tidyUnit.startTime == tidy.first?.startTime)
        #expect(tidyUnit.endTime == tidy.last?.endTime)
    }

    @Test("the music-gate onset window reads in canonical order, not persisted chunkIndex")
    func musicGateOnsetWindowFollowsCanonicalOrder() {
        // Equal span, fast and final. `FinalPassRetranscriptionRunner
        // .nextFinalChunkIndex` persists the final row ABOVE every fast row,
        // so the gate's old `chunkIndex` tiebreak put fast first — while the
        // canonicalizer, and now the atom sequence, rank final first. The two
        // disagreed the moment r5um stopped renumbering `chunkIndex`, and the
        // 600-char cap makes a disagreement change the text that is scanned.
        // Both rows must sit inside the gate's `trailingEdge - 2 s` lead
        // window or they are filtered out before ordering matters at all.
        let chunks = [
            makeChunk(id: "fast-row", chunkIndex: 7, startTime: 108.5, endTime: 110,
                      text: "FAST", pass: "fast"),
            makeChunk(id: "final-row", chunkIndex: 900, startTime: 108.5, endTime: 110,
                      text: "FINAL", pass: "final"),
        ]

        let text = MusicOffsetLexicalGate.onsetWindowText(trailingEdge: 110, chunks: chunks)
        #expect(text.isEmpty == false)
        #expect(text == "FINAL FAST")

        // Same answer whichever way the rows arrive from the store.
        #expect(
            MusicOffsetLexicalGate.onsetWindowText(
                trailingEdge: 110, chunks: chunks.reversed()
            ) == text
        )
    }

    @Test("non-finite chunk times cannot make the comparator intransitive")
    func nonFiniteTimesAreOrderedDeterministically() {
        // Chunk times are ASR-derived doubles with no non-finite guard into
        // SQLite. NaN compares unequal to everything and less than nothing, so
        // a naive `!=` / `<` comparator would be intransitive — undefined
        // behaviour in `sorted(by:)`, and a trapped strict-weak-ordering
        // precondition in a debug build. The old `chunkIndex` (Int) comparator
        // could not reach this; ordering by time can.
        let chunks = [
            makeChunk(id: "nan", chunkIndex: 0, startTime: .nan, endTime: .nan, text: "nan"),
            makeChunk(id: "b", chunkIndex: 1, startTime: 20, endTime: 30, text: "second"),
            makeChunk(id: "inf", chunkIndex: 2, startTime: .infinity, endTime: .infinity,
                      text: "inf"),
            makeChunk(id: "a", chunkIndex: 3, startTime: 0, endTime: 10, text: "first"),
        ]

        let (atoms, version) = atomize(chunks)

        // Real rows keep their true order and sort ahead of the garbage.
        #expect(atoms.prefix(2).map(\.text) == ["first", "second"])
        #expect(atoms.count == 4)
        // And the result is stable across arrival order, so the hash is too.
        let (reversedAtoms, reversedVersion) = atomize(Array(chunks.reversed()))
        #expect(reversedAtoms.map(\.text) == atoms.map(\.text))
        #expect(reversedVersion.transcriptVersion == version.transcriptVersion)
    }

    @Test("segmenting the device shape yields forward, non-inverted segments")
    func segmentingDeviceShapeStaysForward() {
        let (atoms, _) = atomize(singlePassDeviceAsset())
        let segments = TranscriptSegmenter.segment(atoms: atoms)

        #expect(segments.isEmpty == false)
        // Every segment has non-negative width...
        #expect(segments.allSatisfy { $0.endTime >= $0.startTime })
        // ...and segments run forwards, so a scan window built as the hull of
        // a segment range claims only audio its prompt contained.
        let starts = segments.map(\.startTime)
        #expect(worstBackwardStep(starts) == 0)
        // Every atom still appears exactly once, in order.
        #expect(segments.flatMap { $0.atoms }.map(\.atomKey.atomOrdinal)
            == Array(0..<atoms.count))
    }
}

// MARK: - TranscriptSegmenter Tests

@Suite("TranscriptSegmenter")
struct TranscriptSegmenterTests {

    @Test("Segments split on pause threshold")
    func splitOnPause() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 5, text: "Hello."),
            makeAtom(ordinal: 1, startTime: 5, endTime: 10, text: "World."),
            // 3-second gap — exceeds 2s default threshold
            makeAtom(ordinal: 2, startTime: 13, endTime: 18, text: "New segment."),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms)

        #expect(segments.count == 2)
        #expect(segments[0].atoms.count == 2)
        #expect(segments[1].atoms.count == 1)
        #expect(segments[1].firstAtomOrdinal == 2)
    }

    @Test("Default pause threshold splits on gaps above 1.5 seconds")
    func defaultPauseThresholdMatchesBeadSpec() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 4, text: "First part"),
            makeAtom(ordinal: 1, startTime: 5.6, endTime: 9, text: "Second part after a 1.6 second gap")
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms)

        #expect(segments.count == 2)
        #expect(segments[0].firstAtomOrdinal == 0)
        #expect(segments[1].firstAtomOrdinal == 1)
    }

    @Test("Single atom produces single segment")
    func singleAtom() {
        let atoms = [makeAtom(ordinal: 0, startTime: 0, endTime: 5)]
        let segments = TranscriptSegmenter.segment(atoms: atoms)
        #expect(segments.count == 1)
        #expect(segments[0].atoms.count == 1)
    }

    @Test("Empty input produces no segments")
    func emptyInput() {
        let segments = TranscriptSegmenter.segment(atoms: [])
        #expect(segments.isEmpty)
    }

    @Test("Max duration forces hard break even below min segment duration")
    func maxDurationBreak() {
        // Create atoms spanning 130s with no pauses — should break at 120s
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
        var atoms: [TranscriptAtom] = []
        for i in 0..<26 {
            atoms.append(makeAtom(
                ordinal: i,
                startTime: Double(i) * 5,
                endTime: Double(i) * 5 + 4.9,
                text: "Word number \(i)."
            ))
        }

        let segments = TranscriptSegmenter.segment(atoms: atoms, config: config)

        #expect(segments.count == 2)
        // First segment should end at exactly the atom before 120s
        // Atom 24 starts at 120.0s, so it triggers the break. First segment = atoms 0-23.
        #expect(segments[0].atoms.count == 24)
        #expect(segments[1].atoms.count == 2)
        #expect(segments.allSatisfy { $0.duration <= config.maxSegmentDuration })
    }

    @Test("Discourse marker triggers break after minor pause")
    func discourseMarkerBreak() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10, text: "End of first topic."),
            makeAtom(ordinal: 1, startTime: 10, endTime: 20, text: "More content here."),
            // 0.6s gap + discourse marker
            makeAtom(ordinal: 2, startTime: 20.6, endTime: 30, text: "Anyway let me tell you about."),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms)

        #expect(segments.count == 2)
        #expect(segments[0].atoms.count == 2)
        #expect(segments[1].atoms.count == 1)
        #expect(segments[1].atoms.first?.text.hasPrefix("Anyway") == true)
    }

    @Test("Discourse marker prefix does not false-positive on common words")
    func discourseMarkerWordBoundary() {
        // Use text WITHOUT sentence-ending punctuation to isolate the discourse marker check
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10, text: "First part here"),
            // 0.6s gap but "somebody" should NOT trigger "so" marker
            makeAtom(ordinal: 1, startTime: 10.6, endTime: 20, text: "Somebody told me about this"),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms)

        // Should stay as one segment — "somebody" is not the discourse marker "so"
        #expect(segments.count == 1)
    }

    @Test("Sentence punctuation triggers soft break when min duration met")
    func sentencePunctuationBreak() {
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 8, text: "This is a good topic."),
            makeAtom(ordinal: 1, startTime: 8, endTime: 11, text: "End of thought."),
            // 0.4s gap + sentence punctuation + segment > 10s minDuration
            makeAtom(ordinal: 2, startTime: 11.4, endTime: 20, text: "New thought here."),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms, config: config)

        #expect(segments.count == 2)
        #expect(segments[0].atoms.count == 2)
        #expect(segments[1].firstAtomOrdinal == 2)
    }

    @Test("Sentence punctuation does not split when continuation is lowercase")
    func sentencePunctuationRequiresCapitalization() {
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 8, text: "This is a good topic."),
            makeAtom(ordinal: 1, startTime: 8, endTime: 11, text: "End of thought."),
            makeAtom(ordinal: 2, startTime: 11.4, endTime: 20, text: "and this keeps the same thought going."),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms, config: config)

        #expect(segments.count == 1)
    }

    @Test("Segment indices are sequential")
    func sequentialIndices() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 5),
            makeAtom(ordinal: 1, startTime: 8, endTime: 13), // 3s gap
            makeAtom(ordinal: 2, startTime: 16, endTime: 21), // 3s gap
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms)

        for (i, seg) in segments.enumerated() {
            #expect(seg.segmentIndex == i)
        }
    }

    @Test("Min duration prevents micro-segments from soft breaks")
    func minDurationPreventsFragments() {
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
        // Sentence punctuation + 0.4s gap at 3s — below minSegmentDuration
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 3, text: "Short sentence."),
            makeAtom(ordinal: 1, startTime: 3.4, endTime: 8, text: "Still same segment."),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms, config: config)

        #expect(segments.count == 1)
    }

    @Test("Speaker change triggers soft break when stable clusters differ")
    func speakerChangeBreak() {
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 6, text: "we are still talking here"),
            makeAtom(ordinal: 1, startTime: 6, endTime: 12, text: "same speaker keeps going"),
            makeAtom(ordinal: 2, startTime: 12, endTime: 18, text: "different voice starts now"),
        ]
        let featureWindows = [
            makeFeatureWindow(startTime: 0, endTime: 4, speakerClusterId: 1),
            makeFeatureWindow(startTime: 4, endTime: 8, speakerClusterId: 1),
            makeFeatureWindow(startTime: 8, endTime: 12, speakerClusterId: 1),
            makeFeatureWindow(startTime: 12, endTime: 15, speakerClusterId: 2),
            makeFeatureWindow(startTime: 15, endTime: 18, speakerClusterId: 2),
        ]

        let segments = TranscriptSegmenter.segment(
            atoms: atoms,
            featureWindows: featureWindows,
            config: config
        )

        #expect(segments.count == 2)
        #expect(segments[0].firstAtomOrdinal == 0)
        #expect(segments[0].lastAtomOrdinal == 1)
        #expect(segments[1].firstAtomOrdinal == 2)
        #expect(segments[1].boundaryReason == .speakerTurn)
        #expect(segments[1].boundaryConfidence >= 0.8)
        #expect(segments[1].segmentType == .speech)
    }

    @Test("Speaker change respects min segment duration to avoid micro segments")
    func speakerChangeRespectsMinDuration() {
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 4, text: "short opening"),
            makeAtom(ordinal: 1, startTime: 4, endTime: 8, text: "new speaker starts"),
        ]
        let featureWindows = [
            makeFeatureWindow(startTime: 0, endTime: 4, speakerClusterId: 1),
            makeFeatureWindow(startTime: 4, endTime: 8, speakerClusterId: 2),
        ]

        let segments = TranscriptSegmenter.segment(
            atoms: atoms,
            featureWindows: featureWindows,
            config: config
        )

        #expect(segments.count == 1)
        #expect(segments[0].atoms.count == 2)
    }

    @Test("High pause probability window triggers a hard break without a literal atom gap")
    func featurePauseProbabilityBreak() {
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 6, text: "first thought continues"),
            makeAtom(ordinal: 1, startTime: 6, endTime: 12, text: "new topic starts here"),
            makeAtom(ordinal: 2, startTime: 12, endTime: 18, text: "same topic continues"),
        ]
        let featureWindows = [
            makeFeatureWindow(startTime: 0, endTime: 4, pauseProbability: 0.1, speakerClusterId: 1),
            makeFeatureWindow(startTime: 4, endTime: 8, pauseProbability: 0.9, speakerClusterId: 1),
            makeFeatureWindow(startTime: 8, endTime: 12, pauseProbability: 0.1, speakerClusterId: 1),
            makeFeatureWindow(startTime: 12, endTime: 18, pauseProbability: 0.1, speakerClusterId: 1),
        ]

        let segments = TranscriptSegmenter.segment(
            atoms: atoms,
            featureWindows: featureWindows,
            config: config
        )

        #expect(segments.count == 2)
        #expect(segments[0].firstAtomOrdinal == 0)
        #expect(segments[0].lastAtomOrdinal == 0)
        #expect(segments[1].firstAtomOrdinal == 1)
        #expect(segments[1].lastAtomOrdinal == 2)
        #expect(segments[1].boundaryReason == .pause)
        #expect(segments[1].boundaryConfidence >= 0.9)
        #expect(segments[1].segmentType == .speech)
    }

    @Test("Default pause threshold pins segment count for synthetic ad-cluster transcript")
    func defaultPauseThresholdRegressionGuard() {
        // H12: pin the segment count produced by the default pauseThreshold
        // (1.5s) over a hand-built transcript with deliberately-placed
        // pauses. A future change to the default threshold (e.g. 1.5 → 2.0)
        // will reorder pauses across the threshold and shift the segment
        // count, failing this test loudly.
        //
        // Topology (gaps in seconds):
        //   atom 0  end=10  gap 1.6  (>1.5, hard break)
        //   atom 1  end=21  gap 0.8  (no break)
        //   atom 2  end=30  gap 1.4  (no break — sub-1.5)
        //   atom 3  end=42  gap 1.6  (>1.5, hard break)
        //   atom 4  end=55  gap 0.6  (no break)
        //   atom 5  end=68  gap 2.5  (>1.5, hard break)
        //   atom 6  end=80
        // Expected: 4 segments under default threshold 1.5.
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0,    endTime: 10,   text: "intro thought continues here"),
            makeAtom(ordinal: 1, startTime: 11.6, endTime: 21,   text: "second thought rolling along"),
            makeAtom(ordinal: 2, startTime: 21.8, endTime: 30,   text: "still in the same beat"),
            makeAtom(ordinal: 3, startTime: 31.4, endTime: 42,   text: "another sentence in cluster"),
            makeAtom(ordinal: 4, startTime: 43.6, endTime: 55,   text: "fresh topic begins now"),
            makeAtom(ordinal: 5, startTime: 55.6, endTime: 68,   text: "continuing the fresh topic"),
            makeAtom(ordinal: 6, startTime: 70.5, endTime: 80,   text: "final segment after long pause"),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms)

        #expect(segments.count == 4, "Expected 4 segments at default pauseThreshold 1.5; got \(segments.count). If you intentionally changed the default, update this test with the new pinned count.")
        // First segment must be only atom 0 (gap 1.6 > 1.5).
        #expect(segments[0].lastAtomOrdinal == 0)
        // Final segment must start at atom 6 (gap 2.5 > 1.5).
        #expect(segments.last?.firstAtomOrdinal == 6)
    }

    @Test("First segment boundaryReason is startOfTranscript")
    func firstSegmentBoundaryReasonIsStartOfTranscript() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 5, text: "first."),
            makeAtom(ordinal: 1, startTime: 8, endTime: 13, text: "Second."),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms)

        #expect(segments.count == 2)
        #expect(segments[0].boundaryReason == .startOfTranscript)
    }

    @Test("Speaker turn re-fires after segment grows past min duration")
    func speakerTurnSuppressedThenReFires() {
        // Reproduces M1: a speaker change at 4s is suppressed because the
        // segment is below minSegmentDuration (10s). A second speaker
        // change at 14s — once the running segment has grown beyond 10s —
        // must fire and produce a segment whose boundaryReason is
        // .speakerTurn (boundary metadata is correctly attributed to the
        // turn that actually emitted the break, not the suppressed one).
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0,  endTime: 4,  text: "speaker one starts"),
            makeAtom(ordinal: 1, startTime: 4,  endTime: 8,  text: "speaker two interjects"),
            makeAtom(ordinal: 2, startTime: 8,  endTime: 14, text: "speaker two keeps talking"),
            makeAtom(ordinal: 3, startTime: 14, endTime: 20, text: "speaker three takes over"),
        ]
        let featureWindows = [
            makeFeatureWindow(startTime: 0,  endTime: 4,  speakerClusterId: 1),
            makeFeatureWindow(startTime: 4,  endTime: 8,  speakerClusterId: 2),
            makeFeatureWindow(startTime: 8,  endTime: 14, speakerClusterId: 2),
            makeFeatureWindow(startTime: 14, endTime: 20, speakerClusterId: 3),
        ]

        let segments = TranscriptSegmenter.segment(
            atoms: atoms,
            featureWindows: featureWindows,
            config: config
        )

        #expect(segments.count == 2)
        #expect(segments[0].atoms.count == 3)
        #expect(segments[1].firstAtomOrdinal == 3)
        #expect(segments[1].boundaryReason == .speakerTurn)
    }

    @Test("Segmenting 100 atoms against 100 feature windows completes within budget")
    func segmenterScalesWithBinarySearch() {
        // M18 perf guard: 100 atom transitions x 100 windows = 10,000
        // candidate pairs. With linear scans this still completes quickly,
        // but the test ensures binary-search-based lookups stay correct.
        // Budget is generous to avoid CI flakes; the goal is to detect a
        // pathological regression (e.g. an O(N*M*log) becoming O(N*M*M)).
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 600.0,
            minSegmentDuration: 1.0
        )
        let atoms: [TranscriptAtom] = (0..<100).map { i in
            makeAtom(
                ordinal: i,
                startTime: Double(i) * 5,
                endTime: Double(i) * 5 + 4.9,
                text: "atom number \(i)"
            )
        }
        let windows: [FeatureWindow] = (0..<100).map { i in
            makeFeatureWindow(
                startTime: Double(i) * 5,
                endTime: Double(i) * 5 + 5,
                pauseProbability: 0.1,
                speakerClusterId: 1
            )
        }

        let start = Date()
        let segments = TranscriptSegmenter.segment(
            atoms: atoms,
            featureWindows: windows,
            config: config
        )
        let elapsedMs = Date().timeIntervalSince(start) * 1000

        #expect(!segments.isEmpty)
        #expect(elapsedMs < 100, "segment(atoms:featureWindows:) took \(elapsedMs) ms; budget is 100 ms")
    }

    @Test("Dominant speaker breaks ties by picking the lower cluster id")
    func dominantSpeakerTieBreakPicksLower() {
        // Two clusters overlap each atom equally. Picking the LOWER id is
        // more stable across reclustering: clusters tend to be assigned ids
        // in order of first appearance, so the lower id is the older,
        // less-volatile assignment.
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 1.0
        )
        // Atoms span [0, 10] and [10, 20]. Both atoms get equal overlap from
        // cluster 1 and cluster 2 — but for the boundary to trigger a turn,
        // the dominant speaker must DIFFER between atoms. Set up the prior
        // atom to have cluster 1 win and the second to also have cluster 1
        // win (lower-id tie-break) — therefore NO speaker turn fires.
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10, text: "first speaker block"),
            makeAtom(ordinal: 1, startTime: 10, endTime: 20, text: "second speaker block"),
        ]
        // Each atom: cluster 1 covers [start, start+5], cluster 2 covers
        // [start+5, end]. Equal 5s overlap each. Tie → pick lower (cluster 1).
        let featureWindows = [
            makeFeatureWindow(startTime: 0,  endTime: 5,  speakerClusterId: 1),
            makeFeatureWindow(startTime: 5,  endTime: 10, speakerClusterId: 2),
            makeFeatureWindow(startTime: 10, endTime: 15, speakerClusterId: 1),
            makeFeatureWindow(startTime: 15, endTime: 20, speakerClusterId: 2),
        ]

        let segments = TranscriptSegmenter.segment(
            atoms: atoms,
            featureWindows: featureWindows,
            config: config
        )

        // Both atoms resolve to cluster 1 (lower id), so no speaker turn.
        #expect(segments.count == 1)
    }

    @Test("Dominant speaker returns nil when all overlapping windows have nil cluster")
    func dominantSpeakerAllNilReturnsNil() {
        // Indirectly: with all clusters nil, the dominantSpeaker helper
        // returns nil for both sides, so the speaker-turn branch is never
        // entered and we get a single segment.
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 1.0
        )
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 6, text: "first"),
            makeAtom(ordinal: 1, startTime: 6, endTime: 12, text: "second"),
        ]
        let featureWindows = [
            makeFeatureWindow(startTime: 0, endTime: 6,  speakerClusterId: nil),
            makeFeatureWindow(startTime: 6, endTime: 12, speakerClusterId: nil),
        ]

        let segments = TranscriptSegmenter.segment(
            atoms: atoms,
            featureWindows: featureWindows,
            config: config
        )

        #expect(segments.count == 1)
    }

    @Test("Feature pause window covering only the gap interval still triggers a hard break")
    func featurePauseWindowCoversGapInterval() {
        // Regression for M19: previously the segmenter only sampled the
        // current atom's startTime, so a high-pause window covering [4, 8]
        // missed gap interval [8.0, 8.5] and no break was emitted.
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0, // gap is 0.5s — well below atom-gap threshold
            maxSegmentDuration: 120.0,
            minSegmentDuration: 1.0
        )
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 8.0, text: "first thought"),
            makeAtom(ordinal: 1, startTime: 8.5, endTime: 14.0, text: "second thought"),
        ]
        let featureWindows = [
            makeFeatureWindow(startTime: 4, endTime: 8, pauseProbability: 0.95, speakerClusterId: 1),
        ]

        let segments = TranscriptSegmenter.segment(
            atoms: atoms,
            featureWindows: featureWindows,
            config: config
        )

        #expect(segments.count == 2)
        #expect(segments[1].boundaryReason == .pause)
    }

    @Test("Feature pause window with edge exactly at boundary time still counts")
    func featurePauseWindowEdgeInclusive() {
        // Pin inclusive equality: a window ending exactly at the boundary
        // time (and a window starting exactly at it) should both fire.
        let config = TranscriptSegmenter.Config(
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 1.0
        )
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 6.0, text: "first"),
            makeAtom(ordinal: 1, startTime: 6.0, endTime: 12.0, text: "second"),
        ]
        // Window ends exactly at the gap interval [6.0, 6.0].
        let featureWindows = [
            makeFeatureWindow(startTime: 4.0, endTime: 6.0, pauseProbability: 0.9, speakerClusterId: 1),
        ]

        let segments = TranscriptSegmenter.segment(
            atoms: atoms,
            featureWindows: featureWindows,
            config: config
        )

        #expect(segments.count == 2)
        #expect(segments[1].boundaryReason == .pause)
    }

    @Test("Every atom appears exactly once across emitted segments")
    func exactAtomCoverage() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 5, text: "intro"),
            makeAtom(ordinal: 1, startTime: 5, endTime: 10, text: "still intro."),
            makeAtom(ordinal: 2, startTime: 13, endTime: 18, text: "new section"),
            makeAtom(ordinal: 3, startTime: 18.6, endTime: 24, text: "Anyway sponsor break"),
        ]

        let segments = TranscriptSegmenter.segment(atoms: atoms)
        let flattenedOrdinals = segments.flatMap(\.atoms).map(\.atomKey.atomOrdinal)

        #expect(flattenedOrdinals == atoms.map(\.atomKey.atomOrdinal))
    }
}

// MARK: - TranscriptQualityEstimator Tests

@Suite("TranscriptQualityEstimator")
struct TranscriptQualityEstimatorTests {

    private func makeSegment(
        text: String,
        startTime: Double = 0,
        duration: Double = 30,
        segmentIndex: Int = 0
    ) -> AdTranscriptSegment {
        // Create enough atoms to cover the duration
        let wordsPerAtom = 10
        let words = text.split(whereSeparator: \.isWhitespace)
        let atomCount = max(1, words.count / wordsPerAtom)
        let atomDuration = duration / Double(atomCount)

        let atoms = (0..<atomCount).map { i in
            let atomStart = startTime + Double(i) * atomDuration
            let atomEnd = atomStart + atomDuration
            let wordSlice = words[
                min(i * wordsPerAtom, words.count)..<min((i + 1) * wordsPerAtom, words.count)
            ]
            return makeAtom(
                ordinal: i,
                startTime: atomStart,
                endTime: atomEnd,
                text: wordSlice.joined(separator: " ")
            )
        }

        return AdTranscriptSegment(atoms: atoms, segmentIndex: segmentIndex)
    }

    @Test("Good quality transcript scores as good with reasonable signals")
    func goodQuality() {
        // Natural speech with punctuation, normal word density (~2.5 wps)
        let text = "Welcome back to the show. Today we have a really exciting guest. She has been working in artificial intelligence for over ten years. Let me introduce Doctor Sarah Chen."
        // 30 words in 12 seconds ≈ 2.5 wps (optimal range)
        let segment = makeSegment(text: text, duration: 12)

        let assessment = TranscriptQualityEstimator.assess(segment: segment)

        #expect(assessment.quality == .good)
        #expect(assessment.qualityScore == assessment.compositeScore)
        #expect(assessment.compositeScore >= 0.65)
        #expect(assessment.punctuationScore > 0.3)
        #expect(assessment.tokenDensityScore > 0.5)
        #expect(assessment.wordLengthScore > 0.5)
    }

    @Test("Repetitive text scores as degraded or unusable")
    func repetitiveText() {
        // ASR hallucination pattern — extreme repetition
        let text = (0..<30).map { _ in "the the the the" }.joined(separator: " ")
        let segment = makeSegment(text: text, duration: 30)

        let assessment = TranscriptQualityEstimator.assess(segment: segment)

        #expect(assessment.quality != .good)
        #expect(assessment.repetitionScore < 0.5)
    }

    @Test("Garbled text scores as unusable")
    func unusableQuality() {
        // Simulate garbled ASR: no punctuation, uniform short words, wrong density
        let text = (0..<80).map { _ in "xx" }.joined(separator: " ")
        // 80 words of "xx" in 5 seconds = 16 wps (way too fast)
        let segment = makeSegment(text: text, duration: 5)

        let assessment = TranscriptQualityEstimator.assess(segment: segment)

        #expect(assessment.quality == .unusable)
        #expect(assessment.compositeScore < 0.35)
    }

    @Test("Y-vowel English words score the same as a-vowel English words")
    func yVowelWordsDoNotTriggerPenalty() {
        // rhythm, myrrh, syzygy are valid English words whose only vowels are 'y'.
        // Including 'y' as a vowel should keep them out of the unusual-token bucket
        // and produce the same wordLengthScore as a structurally-equivalent a-vowel
        // sentence (matched word lengths so meanScore and stddevScore are identical).
        // y words: rhythm(6) myrrh(5) syzygy(6) — repeat 4x
        let yVowel = makeSegment(
            text: "rhythm myrrh syzygy rhythm myrrh syzygy rhythm myrrh syzygy rhythm myrrh syzygy",
            duration: 12
        )
        // Control: a words with matched lengths: cobalt(6) llama(5) banana(6)
        let aVowel = makeSegment(
            text: "cobalt llama banana cobalt llama banana cobalt llama banana cobalt llama banana",
            duration: 12
        )

        let yAssessment = TranscriptQualityEstimator.assess(segment: yVowel)
        let aAssessment = TranscriptQualityEstimator.assess(segment: aVowel)

        // y-vowel words must score at least as high as the a-vowel control.
        // (If 'y' is excluded from vowels, the y-vowel score would be ~20% lower.)
        #expect(yAssessment.wordLengthScore >= aAssessment.wordLengthScore - 0.001)
    }

    @Test("OOV-like noise does not score as good")
    func oovLikeNoiseDoesNotScoreAsGood() {
        let clean = makeSegment(
            text: "This conversation stays coherent and natural with complete sentences and clear transitions between each part of the discussion.",
            duration: 12
        )
        let noisy = makeSegment(
            text: "brxq9 tzzk4 qlmn8 vvvr rrrr zyxw7 pttt3 kqrx8 blorf99 snnn qrxl5 mmmn.",
            duration: 12
        )

        let cleanAssessment = TranscriptQualityEstimator.assess(segment: clean)
        let noisyAssessment = TranscriptQualityEstimator.assess(segment: noisy)

        #expect(noisyAssessment.quality != .good)
        #expect(noisyAssessment.qualityScore < cleanAssessment.qualityScore)
        #expect(noisyAssessment.wordLengthScore < cleanAssessment.wordLengthScore)
    }

    @Test("Signal agreement score is lower for noisy region than clean region with similar density")
    func signalAgreementIsLowerForNoisyRegion() {
        // This test pins behavior of `signalAgreementScore`: it computes
        // mean(other_signals) * (1 - variance(other_signals)). A noisy region
        // produces lower mean and higher variance than a clean region, so the
        // composite agreement score should be strictly smaller.
        let clean = makeSegment(
            text: "We are explaining the topic clearly with normal phrasing and enough punctuation to keep the transcript easy to follow throughout.",
            duration: 14
        )
        let noisy = makeSegment(
            text: "mrrp qzxv9 blrtt snnn qqqq vvvv tktk4 rxxm zplk9 and uh qrrt mnop7 tsss.",
            duration: 14
        )

        let cleanAssessment = TranscriptQualityEstimator.assess(segment: clean)
        let noisyAssessment = TranscriptQualityEstimator.assess(segment: noisy)

        #expect(noisyAssessment.qualityScore < cleanAssessment.qualityScore)
        #expect(noisyAssessment.quality != .good)
        #expect(noisyAssessment.signalAgreementScore < cleanAssessment.signalAgreementScore)
    }

    @Test("Signal agreement penalizes a single outlier signal among consistent signals")
    func signalAgreementPenalizesOutlier() {
        // Direct unit on the formula via the static `score` shape. We compute
        // signalAgreementScore via the static helper exposed for tests.
        let consistent = TranscriptQualityAssessment.signalAgreementScore(
            punctuationScore: 0.9,
            tokenDensityScore: 0.9,
            repetitionScore: 0.9,
            wordLengthScore: 0.9
        )
        let withOutlier = TranscriptQualityAssessment.signalAgreementScore(
            punctuationScore: 0.9,
            tokenDensityScore: 0.9,
            repetitionScore: 0.9,
            wordLengthScore: 0.1
        )

        // Consistent (low variance) → high output, near the mean.
        #expect(consistent > 0.85)
        // Outlier (high variance) → meaningfully lower output (>25% drop).
        #expect(withOutlier < consistent)
        #expect(withOutlier < consistent * 0.75)
    }

    @Test("Batch assess processes all segments with correct indices")
    func batchAssess() {
        let segments = (0..<3).map { i in
            makeSegment(text: "Segment \(i) has some text. It is fine.", duration: 15, segmentIndex: i)
        }

        let assessments = TranscriptQualityEstimator.assess(segments: segments)

        #expect(assessments.count == 3)
        for (i, assessment) in assessments.enumerated() {
            #expect(assessment.segmentIndex == i)
            #expect(assessment.compositeScore >= 0.0)
            #expect(assessment.compositeScore <= 1.0)
        }
    }

    @Test("Composite score is bounded 0-1 for extreme inputs")
    func compositeBounded() {
        // Normal text
        let normal = makeSegment(text: "Test text here.", duration: 10)
        let a1 = TranscriptQualityEstimator.assess(segment: normal)
        #expect(a1.compositeScore >= 0.0)
        #expect(a1.compositeScore <= 1.0)

        // Empty-ish text
        let minimal = makeSegment(text: "hi", duration: 1)
        let a2 = TranscriptQualityEstimator.assess(segment: minimal)
        #expect(a2.compositeScore >= 0.0)
        #expect(a2.compositeScore <= 1.0)

        // Very long repetitive text
        let long = makeSegment(text: (0..<200).map { _ in "word" }.joined(separator: " "), duration: 60)
        let a3 = TranscriptQualityEstimator.assess(segment: long)
        #expect(a3.compositeScore >= 0.0)
        #expect(a3.compositeScore <= 1.0)
    }
}

// MARK: - TranscriptChunk Migration Tests

@Suite("TranscriptChunk Schema Migration")
struct TranscriptChunkMigrationTests {

    @Test("New columns persist through insert and fetch")
    func newColumnsRoundTrip() async throws {
        let store = try await makeTestStore()

        let asset = AnalysisAsset(
            id: "asset-migration", episodeId: "ep-1",
            assetFingerprint: "fp", weakFingerprint: nil,
            sourceURL: "file:///test.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "running", analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)

        let chunk = TranscriptChunk(
            id: "chunk-1", analysisAssetId: "asset-migration",
            segmentFingerprint: "fp-chunk-1", chunkIndex: 0,
            startTime: 0, endTime: 10,
            text: "hello world", normalizedText: "hello world",
            pass: "final", modelVersion: "v1",
            transcriptVersion: "abc123def456",
            atomOrdinal: 42,
            weakAnchorMetadata: TranscriptWeakAnchorMetadata(
                averageConfidence: 0.52,
                minimumConfidence: 0.21,
                alternativeTexts: ["sponsored by betterhelp"],
                lowConfidencePhrases: [
                    WeakAnchorPhrase(
                        text: "better help",
                        startTime: 3.0,
                        endTime: 4.0,
                        confidence: 0.21
                    )
                ]
            ),
            avgConfidence: 0.83
        )
        try await store.insertTranscriptChunk(chunk)

        let fetched = try await store.fetchTranscriptChunks(assetId: "asset-migration")
        #expect(fetched.count == 1)
        #expect(fetched[0].transcriptVersion == "abc123def456")
        #expect(fetched[0].atomOrdinal == 42)
        #expect(fetched[0].weakAnchorMetadata?.averageConfidence == 0.52)
        #expect(fetched[0].weakAnchorMetadata?.minimumConfidence == 0.21)
        #expect(fetched[0].weakAnchorMetadata?.alternativeTexts == ["sponsored by betterhelp"])
        #expect(fetched[0].weakAnchorMetadata?.lowConfidencePhrases.first?.text == "better help")
        #expect(fetched[0].avgConfidence == 0.83)
    }

    @Test("avgConfidence clamps finite values and drops non-finite values on round-trip")
    func avgConfidenceDefensiveRoundTrip() async throws {
        let store = try await makeTestStore()

        let asset = AnalysisAsset(
            id: "asset-confidence", episodeId: "ep-confidence",
            assetFingerprint: "fp-confidence", weakFingerprint: nil,
            sourceURL: "file:///confidence.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "running", analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)

        try await store.insertTranscriptChunk(makeChunk(
            id: "chunk-negative-confidence",
            assetId: "asset-confidence",
            chunkIndex: 0,
            avgConfidence: -0.4
        ))
        try await store.insertTranscriptChunk(makeChunk(
            id: "chunk-high-confidence",
            assetId: "asset-confidence",
            chunkIndex: 1,
            avgConfidence: 1.4
        ))
        try await store.insertTranscriptChunk(makeChunk(
            id: "chunk-nan-confidence",
            assetId: "asset-confidence",
            chunkIndex: 2,
            avgConfidence: .nan
        ))

        let fetched = try await store.fetchTranscriptChunks(assetId: "asset-confidence")
        #expect(fetched.map(\.avgConfidence) == [0.0, 1.0, nil])
    }

    @Test("ALTER TABLE migration backfills legacy transcript chunks")
    func alterTableMigration() async throws {
        // Simulate a database created by an older app version without the new columns.
        let dir = try makeTempDir(prefix: "MigrationTest")
        defer { try? FileManager.default.removeItem(at: dir) }
        let dbURL = dir.appendingPathComponent("analysis.sqlite")

        // Create old-schema database directly
        var db: OpaquePointer?
        let rc = sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE, nil)
        #expect(rc == SQLITE_OK)

        // Create the old transcript_chunks table WITHOUT the new columns
        let oldDDL = """
            CREATE TABLE analysis_assets (
                id TEXT PRIMARY KEY, episodeId TEXT NOT NULL,
                assetFingerprint TEXT NOT NULL, weakFingerprint TEXT,
                sourceURL TEXT NOT NULL, featureCoverageEndTime REAL,
                fastTranscriptCoverageEndTime REAL, confirmedAdCoverageEndTime REAL,
                analysisState TEXT NOT NULL DEFAULT 'new',
                analysisVersion INTEGER NOT NULL DEFAULT 1,
                capabilitySnapshot TEXT,
                createdAt REAL NOT NULL DEFAULT (strftime('%s', 'now'))
            );
            CREATE TABLE transcript_chunks (
                id TEXT PRIMARY KEY,
                analysisAssetId TEXT NOT NULL REFERENCES analysis_assets(id) ON DELETE CASCADE,
                segmentFingerprint TEXT NOT NULL, chunkIndex INTEGER NOT NULL,
                startTime REAL NOT NULL, endTime REAL NOT NULL,
                text TEXT NOT NULL, normalizedText TEXT NOT NULL,
                pass TEXT NOT NULL DEFAULT 'fast', modelVersion TEXT NOT NULL
            );
            INSERT INTO analysis_assets (id, episodeId, assetFingerprint, sourceURL) VALUES ('a1', 'ep', 'fp', 'url');
            INSERT INTO transcript_chunks (id, analysisAssetId, segmentFingerprint, chunkIndex, startTime, endTime, text, normalizedText, pass, modelVersion)
            VALUES ('old-chunk-late', 'a1', 'fp-old-2', 10, 10.0, 20.0, 'late text', 'late text', 'final', 'v0');
            INSERT INTO transcript_chunks (id, analysisAssetId, segmentFingerprint, chunkIndex, startTime, endTime, text, normalizedText, pass, modelVersion)
            VALUES ('old-chunk-early', 'a1', 'fp-old-1', 2, 0.0, 10.0, 'early text', 'early text', 'final', 'v0');
            """
        var errMsg: UnsafeMutablePointer<CChar>?
        sqlite3_exec(db, oldDDL, nil, nil, &errMsg)
        sqlite3_close(db)

        // Now open via AnalysisStore which runs migration
        do {
            let store = try AnalysisStore(directory: dir)
            try await store.migrate()

            // Legacy rows should be backfilled deterministically by chunkIndex order.
            let fetched = try await store.fetchTranscriptChunks(assetId: "a1")
            #expect(fetched.count == 2)
            #expect(fetched.map(\.id) == ["old-chunk-early", "old-chunk-late"])
            #expect(fetched.map(\.chunkIndex) == [2, 10])
            #expect(fetched.map(\.atomOrdinal) == [0, 1])
            #expect(fetched[0].transcriptVersion == fetched[1].transcriptVersion)
            #expect(fetched[0].transcriptVersion?.isEmpty == false)
            #expect(fetched.allSatisfy { $0.weakAnchorMetadata == nil })
            #expect(fetched.allSatisfy { $0.avgConfidence == nil })

            // New row with non-nil values should round-trip
            let newChunk = TranscriptChunk(
                id: "new-chunk", analysisAssetId: "a1",
                segmentFingerprint: "fp-new", chunkIndex: 1,
                startTime: 10, endTime: 20,
                text: "new text", normalizedText: "new text",
                pass: "final", modelVersion: "v1",
                transcriptVersion: "version-hash",
                atomOrdinal: 7,
                weakAnchorMetadata: TranscriptWeakAnchorMetadata(
                    averageConfidence: 0.61,
                    minimumConfidence: 0.44,
                    alternativeTexts: ["visit betterhelp dot com"],
                    lowConfidencePhrases: []
                ),
                avgConfidence: 0.73
            )
            try await store.insertTranscriptChunk(newChunk)

            let all = try await store.fetchTranscriptChunks(assetId: "a1")
            #expect(all.count == 3)
            let newRow = all.first { $0.id == "new-chunk" }!
            #expect(newRow.transcriptVersion == "version-hash")
            #expect(newRow.atomOrdinal == 7)
            #expect(newRow.weakAnchorMetadata?.alternativeTexts == ["visit betterhelp dot com"])
            #expect(newRow.avgConfidence == 0.73)
        }
    }

    @Test("Nil values for new columns on fast-pass chunks")
    func nilColumnsRoundTrip() async throws {
        let dir = try makeTempDir(prefix: "NilMigrationTest")
        defer { try? FileManager.default.removeItem(at: dir) }

        do {
            let store = try AnalysisStore(directory: dir)
            try await store.migrate()

            let asset = AnalysisAsset(
                id: "asset-nil", episodeId: "ep-2",
                assetFingerprint: "fp2", weakFingerprint: nil,
                sourceURL: "file:///test.m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "running", analysisVersion: 1,
                capabilitySnapshot: nil
            )
            try await store.insertAsset(asset)

            let chunk = TranscriptChunk(
                id: "chunk-nil", analysisAssetId: "asset-nil",
                segmentFingerprint: "fp-nil", chunkIndex: 0,
                startTime: 0, endTime: 10,
                text: "fast pass text", normalizedText: "fast pass text",
                pass: "fast", modelVersion: "v1",
                transcriptVersion: nil,
                atomOrdinal: nil,
                weakAnchorMetadata: nil,
                avgConfidence: nil
            )
            try await store.insertTranscriptChunk(chunk)
        }

        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()

        let fetched = try await reopened.fetchTranscriptChunks(assetId: "asset-nil")
        #expect(fetched.count == 1)
        #expect(fetched[0].transcriptVersion == nil)
        #expect(fetched[0].atomOrdinal == nil)
        #expect(fetched[0].weakAnchorMetadata == nil)
        #expect(fetched[0].avgConfidence == nil)
    }
}
