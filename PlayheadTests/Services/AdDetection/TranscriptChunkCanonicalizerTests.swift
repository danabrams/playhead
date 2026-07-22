// TranscriptChunkCanonicalizerTests.swift
// playhead-hc7e — pins the canonical-transcript contract that
// `AdDetectionService.runBackfill` depends on:
//
//   1. A mixed-pass transcript with candidate-local `final` chunks retains
//      FULL episode coverage, uses `final` text ONLY in the overlapping
//      intervals, and produces NO duplicate lexical evidence.
//   2. An all-fast transcript and an all-final transcript pass through
//      byte-identically (no regression).
//
// FAIL-ON-MAIN: the pre-hc7e selection was
//     let filtered = chunks.filter { $0.pass == "final" }
//     return filtered.isEmpty ? chunks : filtered
// which collapses the whole timeline to the candidate-local `final` chunks
// the moment one `final` chunk exists (here: 2 chunks covering [30,60])
// while the lexical scanner still saw the raw mixed array and double-counted
// the ad text. Every assertion below (coverage 120 s retained, exactly one
// fast chunk dropped, three fast chunks kept, one "brought to you by" hit in
// the overlap instead of two) encodes behavior main cannot produce.

import Foundation
import Testing

@testable import Playhead

@Suite("TranscriptChunkCanonicalizer")
struct TranscriptChunkCanonicalizerTests {

    // MARK: - Fixtures

    private func chunk(
        id: String,
        index: Int,
        start: Double,
        end: Double,
        text: String,
        pass: String
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: id,
            analysisAssetId: "asset-hc7e",
            segmentFingerprint: "fp-\(id)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: text,
            normalizedText: text.lowercased(),
            pass: pass,
            modelVersion: pass == "final" ? "final-v1" : "fast-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    /// The ad-read text carrying several built-in lexical patterns
    /// (`brought to you by`, `promo code \w+`, `\d+ percent off`,
    /// `\w+ dot com`). Present in the OVERLAPPING fast chunk AND in the
    /// candidate-local final chunks that re-transcribe the same audio — the
    /// exact overlap that used to double-count evidence.
    private static let adReadFull =
        "This episode is brought to you by Squarespace. Use promo code SHOW for 20 percent off at squarespace dot com."
    private static let adReadHead =
        "This episode is brought to you by Squarespace."
    private static let adReadTail =
        "Use promo code SHOW for 20 percent off at squarespace dot com."

    /// Full-episode fast transcript (0..120), one ad-read fast chunk at
    /// [30,60].
    private func fastChunks() -> [TranscriptChunk] {
        [
            chunk(id: "f0", index: 0, start: 0, end: 30,
                  text: "Welcome to the show, today we talk about history.", pass: "fast"),
            chunk(id: "f1", index: 1, start: 30, end: 60,
                  text: Self.adReadFull, pass: "fast"),
            chunk(id: "f2", index: 2, start: 60, end: 90,
                  text: "And now back to our conversation about ancient rome.", pass: "fast"),
            chunk(id: "f3", index: 3, start: 90, end: 120,
                  text: "Thanks for listening, see you next week.", pass: "fast"),
        ]
    }

    /// Candidate-local final chunks the final-pass runner produces around
    /// the detected ad window [30,60]. Persisted with chunkIndex strictly
    /// greater than every fast chunk (mirrors
    /// `FinalPassRetranscriptionRunner.nextFinalChunkIndex`).
    private func finalChunksForAdWindow() -> [TranscriptChunk] {
        [
            chunk(id: "fin0", index: 4, start: 30, end: 45,
                  text: Self.adReadHead, pass: "final"),
            chunk(id: "fin1", index: 5, start: 45, end: 60,
                  text: Self.adReadTail, pass: "final"),
        ]
    }

    // MARK: - 1. Mixed: coverage retained, final-only-in-overlap, no dup evidence

    @Test("mixed pass retains full coverage and replaces fast with final only in the overlap")
    func mixedRetainsCoverageAndReplacesInOverlap() {
        let input = fastChunks() + finalChunksForAdWindow()
        let result = TranscriptChunkCanonicalizer.canonicalize(input)
        let diag = result.diagnostics

        // Exactly the overlapping fast chunk (f1 [30,60]) is dropped; the
        // three non-overlapping fast chunks survive.
        #expect(diag.isPassthrough == false)
        #expect(diag.finalCount == 2)
        #expect(diag.fastCount == 4)
        #expect(diag.droppedFastCount == 1)
        #expect(diag.retainedFastCount == 3)

        // Full-episode coverage (0..120 = 120 s) is preserved — dropping a
        // fully-covered fast chunk removes no audio.
        #expect(diag.coverageRetained)
        #expect(diag.inputCoverageSeconds == 120)
        #expect(diag.canonicalCoverageSeconds == 120)

        // Clean full replacement ⇒ no retained fast chunk still overlaps a
        // final interval ⇒ no residual duplicate-evidence risk.
        #expect(diag.residualFastFinalOverlapCount == 0)
        #expect(diag.hasResidualDuplicateEvidence == false)

        // f1 (the fast ad chunk) is gone; both final chunks are present.
        let ids = Set(result.chunks.map(\.id))
        #expect(ids == ["f0", "fin0", "fin1", "f2", "f3"])
        #expect(ids.contains("f1") == false)

        // Final text is used ONLY inside the overlap [30,60]: every canonical
        // chunk that intersects [30,60] is a final chunk; no fast chunk does.
        let overlapping = result.chunks.filter { $0.startTime < 60 && $0.endTime > 30 }
        #expect(overlapping.allSatisfy { $0.pass == "final" })
        #expect(result.chunks.filter { $0.pass == "fast" }
            .allSatisfy { $0.endTime <= 30 || $0.startTime >= 60 })

        // Re-indexed to time order so the atomizer (which sorts by
        // chunkIndex) yields a time-ordered atom sequence with the final
        // chunks interleaved at [30,45] and [45,60].
        let ordered = result.chunks.sorted { $0.chunkIndex < $1.chunkIndex }
        #expect(ordered.map(\.id) == ["f0", "fin0", "fin1", "f2", "f3"])
        #expect(ordered.map(\.startTime) == [0, 30, 45, 60, 90])
    }

    @Test("mixed pass produces no duplicate lexical evidence in the overlap")
    func mixedProducesNoDuplicateLexicalEvidence() {
        let input = fastChunks() + finalChunksForAdWindow()
        let canonical = TranscriptChunkCanonicalizer.canonicalize(input).chunks

        let scanner = LexicalScanner()
        let rawHits = scanner.collectHits(chunks: input)
        let canonicalHits = scanner.collectHits(chunks: canonical)

        // The raw mixed array scans the ad text twice (fast f1 + final
        // fin0/fin1), so canonicalization strictly reduces the hit count.
        #expect(canonicalHits.count < rawHits.count)

        // The "brought to you by" sponsor disclosure lives in BOTH f1 (fast)
        // and fin0 (final). Raw double-counts it; canonical counts it once.
        func broughtToYouByCount(_ hits: [LexicalHit]) -> Int {
            hits.filter { $0.matchedText.lowercased().contains("brought to you by") }.count
        }
        #expect(broughtToYouByCount(rawHits) == 2)
        #expect(broughtToYouByCount(canonicalHits) == 1)

        // The promo-code disclosure lives in BOTH f1 and fin1. Same story.
        func promoCodeCount(_ hits: [LexicalHit]) -> Int {
            hits.filter { $0.matchedText.lowercased().contains("promo code") }.count
        }
        #expect(promoCodeCount(rawHits) == 2)
        #expect(promoCodeCount(canonicalHits) == 1)

        // Every canonical hit inside the overlap [30,60] must originate from
        // the final pass — no fast interpolation survives there. (There is no
        // fast chunk left overlapping [30,60], so any hit there is final.)
        let overlapHits = canonicalHits.filter { $0.startTime >= 30 && $0.endTime <= 60 }
        #expect(overlapHits.isEmpty == false)
    }

    // MARK: - 2. No regression: all-fast and all-final pass through unchanged

    @Test("all-fast transcript passes through byte-identically")
    func allFastPassthrough() {
        let input = fastChunks()
        let result = TranscriptChunkCanonicalizer.canonicalize(input)
        let diag = result.diagnostics

        #expect(diag.isPassthrough)
        #expect(diag.finalCount == 0)
        #expect(diag.fastCount == 4)
        #expect(diag.droppedFastCount == 0)
        #expect(diag.residualFastFinalOverlapCount == 0)
        #expect(diag.coverageRetained)

        // Same chunks, same order, same indices — nothing is re-indexed.
        #expect(result.chunks.map(\.id) == input.map(\.id))
        #expect(result.chunks.map(\.chunkIndex) == input.map(\.chunkIndex))
        #expect(result.chunks.map(\.pass) == input.map(\.pass))

        // Lexical output is identical to scanning the raw input.
        let scanner = LexicalScanner()
        #expect(scanner.collectHits(chunks: result.chunks).count
            == scanner.collectHits(chunks: input).count)
    }

    @Test("all-final transcript passes through byte-identically")
    func allFinalPassthrough() {
        // A full-episode final transcript (e.g. after a whole-episode final
        // pass): every chunk is `final`, nothing to replace.
        let input = [
            chunk(id: "F0", index: 0, start: 0, end: 30, text: "Intro one.", pass: "final"),
            chunk(id: "F1", index: 1, start: 30, end: 60, text: Self.adReadFull, pass: "final"),
            chunk(id: "F2", index: 2, start: 60, end: 90, text: "Back to it.", pass: "final"),
        ]
        let result = TranscriptChunkCanonicalizer.canonicalize(input)
        let diag = result.diagnostics

        #expect(diag.isPassthrough)
        #expect(diag.finalCount == 3)
        #expect(diag.fastCount == 0)
        #expect(diag.droppedFastCount == 0)
        #expect(diag.coverageRetained)

        #expect(result.chunks.map(\.id) == input.map(\.id))
        #expect(result.chunks.map(\.chunkIndex) == input.map(\.chunkIndex))

        let scanner = LexicalScanner()
        #expect(scanner.collectHits(chunks: result.chunks).count
            == scanner.collectHits(chunks: input).count)
    }

    // MARK: - Partial-overlap guard (residual duplicate diagnostic)

    @Test("a fast chunk only partially covered by final is kept and flagged residual")
    func partialOverlapKeepsFastAndFlagsResidual() {
        // Final covers only [40,50], straddled by fast f1 [30,60]. Dropping
        // f1 would lose [30,40] and [50,60], so it is KEPT — and the residual
        // diagnostic flags the surviving overlap so partial replacement can't
        // silently recur.
        let input = [
            chunk(id: "f0", index: 0, start: 0, end: 30, text: "Intro.", pass: "fast"),
            chunk(id: "f1", index: 1, start: 30, end: 60, text: Self.adReadFull, pass: "fast"),
            chunk(id: "fin", index: 2, start: 40, end: 50, text: "brought to you by squarespace", pass: "final"),
        ]
        let result = TranscriptChunkCanonicalizer.canonicalize(input)
        let diag = result.diagnostics

        #expect(diag.isPassthrough == false)
        #expect(diag.droppedFastCount == 0)          // f1 straddles → kept
        #expect(diag.retainedFastCount == 2)
        #expect(diag.residualFastFinalOverlapCount == 1)
        #expect(diag.hasResidualDuplicateEvidence)
        #expect(diag.coverageRetained)               // no audio lost
        #expect(Set(result.chunks.map(\.id)) == ["f0", "f1", "fin"])
    }
}
