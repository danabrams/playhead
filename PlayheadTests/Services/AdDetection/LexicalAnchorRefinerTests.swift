// LexicalAnchorRefinerTests.swift
// playhead-xsdz.37: per-family matcher + invariant tests for the pure
// LexicalAnchorRefiner. Pins the EXACT-match + normalisation rules
// (contracted/uncontracted pre openers, resume phrases, attribution
// templates), the near-miss NEGATIVE traps that exact policy must reject, the
// move-cap gate, the clamp (end > start), and the revert-on-no-overlap guard.

import Foundation
import Testing
@testable import Playhead

@Suite("LexicalAnchorRefiner (playhead-xsdz.37)")
struct LexicalAnchorRefinerTests {

    private static let episodeDuration = 3600.0

    /// Build a word stream from raw transcript text, normalising each token the
    /// same way `buildWordStream` does. Words are laid out one per `step`
    /// seconds starting at `firstWordStart`, so the first token's start is
    /// exactly `firstWordStart` — the position the refiner reads.
    private static func words(
        _ text: String,
        firstWordStart: Double,
        step: Double = 1.0
    ) -> [LexicalWord] {
        text.split(separator: " ").enumerated().compactMap { index, token in
            let norm = LexicalAnchorNormalizer.normalizeWord(String(token))
            guard !norm.isEmpty else { return nil }
            let start = firstWordStart + Double(index) * step
            return LexicalWord(norm: norm, startSeconds: start, endSeconds: start + step * 0.9)
        }
    }

    // MARK: - Family (b) pre openers snap the START

    @Test("Contracted and uncontracted 'we('ll| will) be right back' snap the start (pre)")
    func preOpenersSnapStart() {
        for phrase in ["we'll be right back", "we will be right back"] {
            let anchor = LexicalAnchor.exact(phrase: phrase, side: .pre, edgeOffsetSeconds: 2.0)
            // Transcript uses the natural (un-normalised) casing/apostrophe.
            let raw = phrase == "we'll be right back"
                ? "We'll be right back."
                : "We will be right back."
            let stream = Self.words(raw + " Now a word from our sponsor.", firstWordStart: 100)
            let result = LexicalAnchorRefiner.refine(
                proposalStart: 105, proposalEnd: 160,
                anchors: [anchor], words: stream, episodeDuration: Self.episodeDuration
            )
            // firstWordStart 100 + offset 2.0 = 102.
            #expect(abs(result.startTime - 102.0) < 1e-6, "\(phrase): start must snap to 102")
            #expect(result.endTime == 160.0, "\(phrase): end untouched (no post anchor)")
            #expect(result.trace.startSnapped)
            #expect(!result.trace.endSnapped)
            #expect(result.trace.startAnchorPhrase == phrase)
            #expect(result.trace.startCandidateCount == 1)
            #expect(result.endCandidateCountIsNil)
            let delta = result.trace.startDeltaSeconds ?? .nan
            #expect(abs(delta - (102.0 - 105.0)) < 1e-6)
            #expect(result.endTime > result.startTime)
        }
    }

    // MARK: - Family (b) resume phrases snap the END

    @Test("'(and now|and) back to the show' snap the end (post/resume)")
    func resumePhrasesSnapEnd() {
        for phrase in ["and now back to the show", "and back to the show"] {
            let anchor = LexicalAnchor.exact(phrase: phrase, side: .post, edgeOffsetSeconds: -0.6)
            let stream = Self.words("okay " + phrase + " and here we go", firstWordStart: 200)
            // "okay" is word 0 at 200; the phrase's first word starts at 201.
            let firstPhraseWordStart = 201.0
            let result = LexicalAnchorRefiner.refine(
                proposalStart: 150, proposalEnd: 202,
                anchors: [anchor], words: stream, episodeDuration: Self.episodeDuration
            )
            #expect(result.startTime == 150.0, "\(phrase): start untouched (no pre anchor)")
            #expect(abs(result.endTime - (firstPhraseWordStart - 0.6)) < 1e-6, "\(phrase): end must snap")
            #expect(result.trace.endSnapped)
            #expect(!result.trace.startSnapped)
            #expect(result.trace.endAnchorPhrase == phrase)
            #expect(result.endTime > result.startTime)
        }
    }

    // MARK: - Family (a) attribution templates snap the START

    @Test("A '<station> is brought to you by' attribution template snaps the start (onset)")
    func attributionTemplateSnapsStart() {
        let anchor = LexicalAnchor.exact(
            phrase: "WNYC is brought to you by", side: .pre, edgeOffsetSeconds: -1.0
        )
        let stream = Self.words("WNYC is brought to you by Wise the money app", firstWordStart: 500)
        let result = LexicalAnchorRefiner.refine(
            proposalStart: 502, proposalEnd: 560,
            anchors: [anchor], words: stream, episodeDuration: Self.episodeDuration
        )
        // firstWordStart 500 + offset -1.0 = 499.
        #expect(abs(result.startTime - 499.0) < 1e-6)
        #expect(result.trace.startSnapped)
        #expect(result.trace.startAnchorPhrase == "WNYC is brought to you by")
    }

    // MARK: - NEGATIVE traps (exact policy must reject near-misses)

    @Test("'welcome to the show' must NOT match a resume phrase under exact policy")
    func welcomeToShowDoesNotMatchResume() {
        let resume = [
            LexicalAnchor.exact(phrase: "and now back to the show", side: .post, edgeOffsetSeconds: -0.6),
            LexicalAnchor.exact(phrase: "and back to the show", side: .post, edgeOffsetSeconds: -0.6),
        ]
        // The near-antonym show-open greeting — the report's 0.89 fuzzy trap.
        let stream = Self.words("hey everyone welcome to the show today", firstWordStart: 100)
        let result = LexicalAnchorRefiner.refine(
            proposalStart: 50, proposalEnd: 101,
            anchors: resume, words: stream, episodeDuration: Self.episodeDuration
        )
        #expect(!result.trace.endSnapped, "exact policy must not match the greeting")
        #expect(result.endTime == 101.0)
        #expect(result.trace == LexicalRefinementTrace(), "no qualifying match ⇒ pristine trace")
    }

    @Test("'is produced by' must NOT match a 'is sponsored by' attribution template")
    func producedByDoesNotMatchSponsoredBy() {
        let anchor = LexicalAnchor.exact(
            phrase: "Radiolab is sponsored by", side: .pre, edgeOffsetSeconds: -1.0
        )
        // The report's single family-a false positive, killed by exact match.
        let stream = Self.words("Radiolab is produced by WNYC Studios", firstWordStart: 300)
        let result = LexicalAnchorRefiner.refine(
            proposalStart: 302, proposalEnd: 360,
            anchors: [anchor], words: stream, episodeDuration: Self.episodeDuration
        )
        #expect(!result.trace.startSnapped, "produced-by must not match sponsored-by")
        #expect(result.startTime == 302.0)
    }

    // MARK: - Move cap

    @Test("A match whose snapped edge exceeds the move cap is ignored")
    func moveCapEnforced() {
        let anchor = LexicalAnchor.exact(phrase: "we will be right back", side: .pre, edgeOffsetSeconds: 0.0)
        // Phrase onset at 100, proposal start 200 ⇒ |100 - 200| = 100 > 15 cap.
        let stream = Self.words("we will be right back now", firstWordStart: 100)
        let result = LexicalAnchorRefiner.refine(
            proposalStart: 200, proposalEnd: 260,
            anchors: [anchor], words: stream, episodeDuration: Self.episodeDuration
        )
        #expect(!result.trace.startSnapped)
        #expect(result.startTime == 200.0, "beyond-cap match must not move the edge")
        #expect(result.trace.startCandidateCount == nil, "an out-of-cap match is not a qualifying candidate")
    }

    // MARK: - Closest-match selection + candidate count

    @Test("With multiple in-cap matches the refiner snaps to the one closest to the proposal")
    func closestMatchWins() {
        let anchor = LexicalAnchor.exact(phrase: "we will be right back", side: .pre, edgeOffsetSeconds: 0.0)
        // Two occurrences: onset 100 (dist 10) and onset 112 (dist 2) vs
        // proposal 110; both within the 15s cap.
        var stream = Self.words("we will be right back filler", firstWordStart: 100)
        stream += Self.words("we will be right back again", firstWordStart: 112)
        let result = LexicalAnchorRefiner.refine(
            proposalStart: 110, proposalEnd: 200,
            anchors: [anchor], words: stream, episodeDuration: Self.episodeDuration
        )
        #expect(abs(result.startTime - 112.0) < 1e-6, "snap to the nearer occurrence")
        #expect(result.trace.startCandidateCount == 2, "both occurrences counted as candidates")
    }

    // MARK: - No-match no-op

    @Test("No qualifying phrase near either edge is a pristine no-op")
    func noMatchNoOp() {
        let anchors = [
            LexicalAnchor.exact(phrase: "we will be right back", side: .pre, edgeOffsetSeconds: 0.0),
            LexicalAnchor.exact(phrase: "and now back to the show", side: .post, edgeOffsetSeconds: 0.0),
        ]
        let stream = Self.words("just ordinary conversation with no framing phrases here", firstWordStart: 100)
        let result = LexicalAnchorRefiner.refine(
            proposalStart: 100, proposalEnd: 130,
            anchors: anchors, words: stream, episodeDuration: Self.episodeDuration
        )
        #expect(result.startTime == 100.0)
        #expect(result.endTime == 130.0)
        #expect(result.trace == LexicalRefinementTrace())
    }

    // MARK: - Clamp + revert (end > start impossible)

    @Test("Crossing snaps that abandon proposal overlap revert both edges")
    func crossingSnapsRevert() {
        // Narrow proposal [100, 102]; a pre match drags the start forward past
        // the proposal end while a post match drags the end back before the
        // start — the clamp would produce a degenerate window, so both revert.
        let pre = LexicalAnchor.exact(phrase: "we will be right back", side: .pre, edgeOffsetSeconds: 14.0)
        let post = LexicalAnchor.exact(phrase: "and back to the show", side: .post, edgeOffsetSeconds: -1.0)
        var stream = Self.words("we will be right back", firstWordStart: 100)      // pre onset 100 → 114
        stream += Self.words("and back to the show", firstWordStart: 90)           // post onset 90 → 89
        let result = LexicalAnchorRefiner.refine(
            proposalStart: 100, proposalEnd: 102,
            anchors: [pre, post], words: stream, episodeDuration: Self.episodeDuration
        )
        #expect(result.startTime == 100.0, "revert restores the proposal start")
        #expect(result.endTime == 102.0, "revert restores the proposal end")
        #expect(result.trace.revertedNoOverlap)
        #expect(!result.trace.startSnapped)
        #expect(!result.trace.endSnapped)
        #expect(result.endTime > result.startTime)
    }

    @Test("Degenerate inputs (end <= start, non-finite, tiny episode) are pass-through no-ops")
    func degenerateInputsNoOp() {
        let anchor = LexicalAnchor.exact(phrase: "we will be right back", side: .pre, edgeOffsetSeconds: 0.0)
        let stream = Self.words("we will be right back", firstWordStart: 10)
        // end <= start
        let inverted = LexicalAnchorRefiner.refine(
            proposalStart: 50, proposalEnd: 50,
            anchors: [anchor], words: stream, episodeDuration: 3600
        )
        #expect(inverted.startTime == 50 && inverted.endTime == 50)
        #expect(inverted.trace == LexicalRefinementTrace())
        // tiny episode
        let tiny = LexicalAnchorRefiner.refine(
            proposalStart: 0, proposalEnd: 1,
            anchors: [anchor], words: stream, episodeDuration: 1.0
        )
        #expect(tiny.trace == LexicalRefinementTrace())
    }

    // MARK: - Word-stream construction

    @Test("buildWordStream interpolates per-word times across chunks and normalises")
    func buildWordStreamInterpolates() {
        let chunk = TranscriptChunk(
            id: "c0", analysisAssetId: "a", segmentFingerprint: "f", chunkIndex: 0,
            startTime: 30.0, endTime: 40.0,
            text: "We'll be right back",
            normalizedText: "well be right back",
            pass: "final", modelVersion: "v", transcriptVersion: nil, atomOrdinal: nil
        )
        let words = LexicalAnchorRefiner.buildWordStream(chunks: [chunk])
        #expect(words.map(\.norm) == ["well", "be", "right", "back"], "apostrophe folded")
        // "We'll" starts at char 0 ⇒ interpolated start is exactly the chunk start.
        #expect(abs(words[0].startSeconds - 30.0) < 1e-9)
        // Times are monotonically increasing and within the chunk span.
        for word in words {
            #expect(word.startSeconds >= 30.0 && word.endSeconds <= 40.0)
        }
        #expect(words[1].startSeconds > words[0].startSeconds)
    }
}

private extension LexicalAnchorRefiner.Result {
    /// Small readability shim so the pre-opener test can assert the end side
    /// carried no candidate count.
    var endCandidateCountIsNil: Bool { trace.endCandidateCount == nil }
}
