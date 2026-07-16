// LexicalAnchorRefiner.swift
// playhead-xsdz.37: lexical-anchor boundary refinement — pure matching logic.
//
// Design (mirrors StingerRefiner / FineBoundaryRefiner conventions):
//   • Pure, stateless — same inputs always produce the same output. All
//     transcript access happens in the caller (`AdDetectionService` builds the
//     word stream from the same `TranscriptChunk`s the `LexicalScanner`
//     consumes — NOT PCM; tests hand in synthetic word streams).
//   • Per-window contract: exactly one refined window out per window in — the
//     refiner NEVER splits or merges windows, and the clamp step makes
//     `end <= start` structurally impossible.
//   • EXACT match only. A pre-side anchor whose normalised tokens appear
//     word-for-word in the transcript snaps the break START; a post-side anchor
//     snaps the break END. `edgeOffsetSeconds` is applied AFTER the matched
//     phrase position (the first matched word's start). No fuzzy/difflib
//     matching (parity risk — see the bank header + the GO report).
//   • A `maxEdgeMoveSeconds` cap governs relevance: a match whose snapped edge
//     would move farther than the cap from the proposal is ignored, so passing
//     the whole-episode word stream is safe (the cap localises matching to
//     each edge). No match near an edge ⇒ that edge is left untouched.
//
// PRECEDENCE (owned by the wire-in, not this refiner): the refiner runs on the
// post-stinger/acoustic proposal and only moves the edges it fires on, so an
// exact lexical match takes precedence on its edge while every other edge keeps
// the prior result — no special-casing needed here.

import Foundation

// MARK: - LexicalWord

/// One transcript word with its (interpolated) episode-timeline span.
/// `norm` is already `LexicalAnchorNormalizer.normalizeWord`-normalised.
struct LexicalWord: Sendable, Equatable {
    let norm: String
    let startSeconds: Double
    let endSeconds: Double
}

// MARK: - LexicalRefinementTrace

/// Per-window refinement trace. Mirrors the StingerRefinementTrace convention
/// so the Catalyst dump and the gold scorer can attribute movement. Populated
/// only when the refiner is consulted (flag ON + the show has a bank entry);
/// a consult with no qualifying match leaves it pristine (default), preserving
/// the OFF-vs-consulted-no-snap distinction the wire-in tests pin.
struct LexicalRefinementTrace: Sendable, Equatable {
    /// The break-start edge snapped to a matched pre-side anchor.
    var startSnapped = false
    /// The break-end edge snapped to a matched post-side anchor.
    var endSnapped = false
    /// Refinement abandoned overlap with the proposal — both edges reverted
    /// (snap flags/phrases/deltas are cleared when this is set).
    var revertedNoOverlap = false
    /// Display phrase of the pre anchor the start edge snapped to (nil unless
    /// `startSnapped`).
    var startAnchorPhrase: String?
    /// Display phrase of the post anchor the end edge snapped to (nil unless
    /// `endSnapped`).
    var endAnchorPhrase: String?
    /// Applied start movement in seconds (`refined - proposal`); nil when the
    /// start edge is unchanged.
    var startDeltaSeconds: Double?
    /// Applied end movement in seconds (`refined - proposal`); nil when the end
    /// edge is unchanged.
    var endDeltaSeconds: Double?
    /// Qualifying pre-side matches within the move cap of the start edge. `nil`
    /// when zero — a flag-ON consult with no evidence anywhere leaves the trace
    /// pristine.
    var startCandidateCount: Int?
    /// Qualifying post-side matches within the move cap of the end edge. Same
    /// conventions as `startCandidateCount`.
    var endCandidateCount: Int?
}

// MARK: - LexicalAnchorRefiner

enum LexicalAnchorRefiner {
    /// Refuse snaps that move an edge farther than this from the proposal.
    /// Mirrors the offline prototype's ±15 s onset window — the validated
    /// tolerance for calling a lexical anchor a break-edge hit; a phrase whose
    /// snapped edge lands farther than this is not evidence for THIS edge.
    static let maxEdgeMoveSeconds = 15.0
    /// Refined windows keep at least this width (clamp floor); combined with the
    /// clamp ordering this makes `end <= start` impossible.
    static let minimumRefinedWidthSeconds = 1.0

    struct Result: Sendable, Equatable {
        let startTime: Double
        let endTime: Double
        let trace: LexicalRefinementTrace
    }

    /// Refine one candidate window against the show's effective anchor set.
    ///
    /// - Parameters:
    ///   - proposalStart/proposalEnd: the pipeline's current window bounds
    ///     (post stinger/acoustic snap — the refiner runs inside the existing
    ///     boundary-refinement block).
    ///   - anchors: the show's effective anchor set (family-a templates +
    ///     generic framing phrases; caller resolved it, no entry ⇒ caller
    ///     never calls).
    ///   - words: the transcript word stream (whole episode is fine — the move
    ///     cap localises matching to each edge).
    ///   - episodeDuration: clamp ceiling.
    static func refine(
        proposalStart: Double,
        proposalEnd: Double,
        anchors: [LexicalAnchor],
        words: [LexicalWord],
        episodeDuration: Double
    ) -> Result {
        var trace = LexicalRefinementTrace()
        guard episodeDuration > 2 * minimumRefinedWidthSeconds,
              proposalEnd > proposalStart,
              proposalStart.isFinite, proposalEnd.isFinite else {
            return Result(startTime: proposalStart, endTime: proposalEnd, trace: trace)
        }

        let preAnchors = anchors.filter { $0.side == .pre }
        let postAnchors = anchors.filter { $0.side == .post }
        let startMatches = edgeMatches(
            anchors: preAnchors, words: words, proposalEdge: proposalStart
        )
        let endMatches = edgeMatches(
            anchors: postAnchors, words: words, proposalEdge: proposalEnd
        )
        if !startMatches.isEmpty { trace.startCandidateCount = startMatches.count }
        if !endMatches.isEmpty { trace.endCandidateCount = endMatches.count }

        var newStart = proposalStart
        var newEnd = proposalEnd
        if let best = bestMatch(startMatches, proposalEdge: proposalStart) {
            newStart = best.edge
            trace.startSnapped = true
            trace.startAnchorPhrase = best.anchor.phrase
        }
        if let best = bestMatch(endMatches, proposalEdge: proposalEnd) {
            newEnd = best.edge
            trace.endSnapped = true
            trace.endAnchorPhrase = best.anchor.phrase
        }

        // No qualifying snap on EITHER edge ⇒ leave the proposal untouched and
        // return a pristine trace. Mirrors StingerRefiner's early return before
        // the clamp: the clamp exists only to keep a *snapped* edge inside the
        // episode, so an unmatched consult must not perturb a proposal that the
        // pipeline handed in already outside `[0, episodeDuration]` (which would
        // otherwise silently move an edge and record a phantom delta, breaking
        // the consulted-no-match == OFF byte-identity contract).
        guard trace.startSnapped || trace.endSnapped else {
            return Result(startTime: proposalStart, endTime: proposalEnd, trace: trace)
        }

        // Independent per-edge snaps can CROSS: a pre snap can land at or after
        // a post snap (a start edge dragged past an end edge on contradictory
        // evidence — e.g. a resume phrase transcribed before an opener phrase).
        // The minimum-width clamp below would otherwise "rescue" such a crossing
        // into a degenerate `minimumRefinedWidthSeconds` sliver that STILL
        // overlaps the proposal, hiding the crossing from the overlap check and
        // emitting an unsupported window with a misleading (endSnapped) trace.
        // Detect the crossing on the RAW snapped edges (pre-clamp) so it reverts
        // both edges, honouring the revert-on-crossing contract.
        let crossedRaw = newEnd <= newStart

        // Clamp to the episode. Order is load-bearing: the end clamp's
        // `max(newStart + minimumRefinedWidthSeconds, …)` floor runs after the
        // start clamp, so `end > start` holds unconditionally.
        newStart = max(0.0, min(newStart, episodeDuration - minimumRefinedWidthSeconds))
        newEnd = max(newStart + minimumRefinedWidthSeconds, min(newEnd, episodeDuration))

        // Revert guard: refinement must not emit a crossed window (caught on the
        // raw edges above) nor abandon overlap with the proposal evidence (an
        // out-of-range proposal the clamp can push clear of the proposal). With
        // both edges independently snapped (e.g. a start snapped forward crossing
        // an end snapped back on a narrow proposal), either condition reverts
        // BOTH edges (a one-sided keep would fabricate an unsupported window).
        let overlap = min(newEnd, proposalEnd) - max(newStart, proposalStart)
        if crossedRaw || overlap <= 0 {
            trace.revertedNoOverlap = true
            trace.startSnapped = false
            trace.endSnapped = false
            trace.startAnchorPhrase = nil
            trace.endAnchorPhrase = nil
            trace.startDeltaSeconds = nil
            trace.endDeltaSeconds = nil
            return Result(startTime: proposalStart, endTime: proposalEnd, trace: trace)
        }

        if newStart != proposalStart {
            trace.startDeltaSeconds = roundToMillis(newStart - proposalStart)
        }
        if newEnd != proposalEnd {
            trace.endDeltaSeconds = roundToMillis(newEnd - proposalEnd)
        }
        return Result(startTime: newStart, endTime: newEnd, trace: trace)
    }

    // MARK: - Word-stream construction

    /// Build the episode word stream from transcript chunks — the same chunk /
    /// word data `LexicalScanner` consumes. Each chunk's raw text is tokenised
    /// on whitespace and each word's start/end is linearly interpolated over
    /// the chunk's `[startTime, endTime]` by character position — the same
    /// linear char-fraction scheme `LexicalScanner.interpolateTiming` uses for
    /// regex hits (character-indexed here vs UTF-16-indexed there; identical
    /// for the ASCII transcripts and anchor phrases this cut targets), then
    /// normalised. Words that normalise to empty are dropped. The stream is
    /// flat across chunks (time-ordered), so phrases may span chunk edges —
    /// matching the offline prototype's cross-segment word stream.
    static func buildWordStream(chunks: [TranscriptChunk]) -> [LexicalWord] {
        var words: [LexicalWord] = []
        for chunk in chunks {
            let text = chunk.text.isEmpty ? chunk.normalizedText : chunk.text
            if text.isEmpty { continue }
            let chars = Array(text)
            let total = chars.count
            let start = chunk.startTime
            let duration = chunk.endTime - chunk.startTime
            var i = 0
            while i < total {
                while i < total, chars[i].isWhitespace { i += 1 }
                if i >= total { break }
                let wordStart = i
                while i < total, !chars[i].isWhitespace { i += 1 }
                let wordEnd = i
                let norm = LexicalAnchorNormalizer.normalizeWord(
                    String(chars[wordStart..<wordEnd])
                )
                if norm.isEmpty { continue }
                let startFraction = Double(wordStart) / Double(total)
                let endFraction = Double(wordEnd) / Double(total)
                words.append(LexicalWord(
                    norm: norm,
                    startSeconds: start + duration * startFraction,
                    endSeconds: start + duration * endFraction
                ))
            }
        }
        return words
    }

    // MARK: - Matching

    private struct EdgeMatch {
        /// The candidate snapped edge: `firstMatchedWordStart + edgeOffset`.
        let edge: Double
        let anchor: LexicalAnchor
    }

    /// Every EXACT match of any anchor whose snapped edge honours the move cap.
    private static func edgeMatches(
        anchors: [LexicalAnchor],
        words: [LexicalWord],
        proposalEdge: Double
    ) -> [EdgeMatch] {
        guard !anchors.isEmpty, !words.isEmpty else { return [] }
        var matches: [EdgeMatch] = []
        for anchor in anchors {
            let tokens = anchor.tokens
            let n = tokens.count
            guard n >= 2, words.count >= n else { continue }
            let lastStart = words.count - n
            var i = 0
            while i <= lastStart {
                var matched = true
                var k = 0
                while k < n {
                    if words[i + k].norm != tokens[k] {
                        matched = false
                        break
                    }
                    k += 1
                }
                if matched {
                    let edge = words[i].startSeconds + anchor.edgeOffsetSeconds
                    if abs(edge - proposalEdge) <= maxEdgeMoveSeconds {
                        matches.append(EdgeMatch(edge: edge, anchor: anchor))
                    }
                }
                i += 1
            }
        }
        return matches
    }

    /// Pick the match whose snapped edge sits closest to the proposal edge.
    /// Deterministic tie-break: nearest edge, then earliest edge time, then the
    /// more specific (longer) phrase, then phrase text.
    private static func bestMatch(
        _ matches: [EdgeMatch],
        proposalEdge: Double
    ) -> EdgeMatch? {
        matches.min { lhs, rhs in
            let dl = abs(lhs.edge - proposalEdge)
            let dr = abs(rhs.edge - proposalEdge)
            if dl != dr { return dl < dr }
            if lhs.edge != rhs.edge { return lhs.edge < rhs.edge }
            if lhs.anchor.tokens.count != rhs.anchor.tokens.count {
                return lhs.anchor.tokens.count > rhs.anchor.tokens.count
            }
            return lhs.anchor.phrase < rhs.anchor.phrase
        }
    }

    private static func roundToMillis(_ value: Double) -> Double {
        (value * 1000).rounded() / 1000
    }
}
