// TranscriptBoundaryCue.swift
// playhead-kgby: Transcript-aware boundary cue for TimeBoundaryResolver.
//
// Adds a probabilistic sentence-terminal cue to the multi-cue boundary
// resolver. Each hit represents a candidate sentence boundary derived from
// ASR punctuation, weighted by a confidence value that decays when the
// underlying transcript is noisy. The cue contributes additively to the
// existing cue blend (pauseVAD, speakerChange, musicBedChange, spectralChange,
// lexicalDensityDelta / explicitReturnMarker) and is gated by per-call config
// weights so that it is a no-op when weights are zero.
//
// Design contract — probabilistic, never deterministic:
//   * The cue value is `clamp01(confidence)` per overlapping hit; the
//     resolver multiplies by the configured weight (default 0.0) so the
//     cue is invisible until the call site explicitly tunes a non-zero
//     weight.
//   * When `transcriptHits` is empty the resolver short-circuits and the
//     cue contributes 0 to every candidate window — backward compatible
//     with the legacy snap behaviour.
//   * When ASR quality is poor (e.g. music under speech), confidence is
//     attenuated upstream so acoustic cues out-weigh transcript cues
//     automatically — the bead's "graceful degradation under poor ASR"
//     requirement.
//
// SCOPE: this file owns the value type + builder. Snap-time integration
// lives in `TimeBoundaryResolver`; the live wiring lives in
// `BoundaryRefiner` (gated by `AdDetectionConfig`).

import Foundation

// MARK: - SentenceTerminalKind

/// Which punctuation token produced a transcript boundary hit. Carried so
/// future tuning can weight `?` / `!` differently from `.` (e.g. an
/// interrogative is a cleaner cut than an ellipsis-style period). Today
/// every kind contributes equally; the distinction is preserved for
/// telemetry and future calibration.
enum SentenceTerminalKind: String, Sendable, Equatable, Codable {
    case period
    case question
    case exclamation
}

// MARK: - TranscriptBoundaryHit

/// A single sentence-terminal candidate derived from a `TranscriptChunk`.
///
/// `time` is the estimated wall-clock seconds at which the terminal token
/// occurs. Because iOS `SFSpeechRecognizer` does not expose per-word
/// timestamps for finalised chunks, we approximate by linearly apportioning
/// the chunk's `[startTime, endTime]` interval across the chunk text by
/// *word offset* (not character offset — character-offset apportionment
/// produced ±2-2.5s error at typical chunk midpoints, exceeding the
/// resolver's 1.5s match radius). Word offset is robust to density
/// variation because speech rate is roughly constant in words-per-second.
/// The resolver still consumes the cue probabilistically (weight ×
/// confidence) so any residual timing error degrades the score smoothly.
///
/// `confidence` is a per-hit weight in `[0, 1]` already attenuated by the
/// chunk's ASR-quality signal. A confidence near 0 means "the transcript
/// said there was a sentence break here, but the transcript itself is
/// noisy" — the resolver multiplies it by the configured weight so the
/// cue contributes very little. A confidence near 1 means "clean ASR,
/// strong sentence terminal" — the cue gets full weight.
struct TranscriptBoundaryHit: Sendable, Equatable {
    let time: Double
    let confidence: Double
    let terminalKind: SentenceTerminalKind

    init(
        time: Double,
        confidence: Double,
        terminalKind: SentenceTerminalKind
    ) {
        self.time = time
        // Defensive clamp: callers occasionally feed scores from upstream
        // assessors that are nominally [0,1] but can drift due to floating
        // point arithmetic.
        self.confidence = min(max(confidence, 0), 1)
        self.terminalKind = terminalKind
    }
}

// MARK: - TranscriptBoundaryCueBuilder

/// Builds `[TranscriptBoundaryHit]` from `[TranscriptChunk]` for use by
/// `TimeBoundaryResolver`. Stateless / value-typed so it composes with
/// the rest of the boundary refinement stack without ownership or
/// lifetime concerns.
enum TranscriptBoundaryCueBuilder {

    // MARK: - Configuration

    struct Config: Sendable, Equatable {
        /// Minimum chunk-quality score below which a chunk's hits are
        /// dropped entirely. Lifts the floor on the worst-quality chunks
        /// (effectively "music under speech") so they never produce a
        /// cue, matching the bead's graceful-degradation requirement.
        let minChunkQualityScore: Double

        /// Minimum word count per chunk required for a position estimate
        /// to be meaningful. Below this, character-offset apportionment
        /// is too noisy to be useful.
        let minWordsForApportionment: Int

        static let `default` = Config(
            minChunkQualityScore: 0.30,
            minWordsForApportionment: 4
        )
    }

    // MARK: - Public API

    /// Extract sentence-terminal hits from a list of transcript chunks.
    ///
    /// Caller is expected to pass the same chunk array that was used for
    /// classification (typically the final-pass chunks). The return value
    /// is sorted by `time` ascending so downstream binary searches /
    /// linear scans are O(N).
    ///
    /// Returns an empty array when:
    ///   - `chunks` is empty
    ///   - every chunk has zero duration (would produce divide-by-zero
    ///     in apportionment) or empty text
    ///   - no chunk text contains `.`, `?`, or `!`
    static func buildHits(
        from chunks: [TranscriptChunk],
        config: Config = .default
    ) -> [TranscriptBoundaryHit] {
        guard !chunks.isEmpty else { return [] }

        var hits: [TranscriptBoundaryHit] = []
        for chunk in chunks {
            let chunkHits = hitsForChunk(chunk, config: config)
            hits.append(contentsOf: chunkHits)
        }

        // Sort once at the end so the caller can rely on monotonic time
        // ordering. Stable sort is not required — duplicate-time hits are
        // rare and the resolver tolerates them.
        hits.sort { $0.time < $1.time }
        return hits
    }

    // MARK: - Per-Chunk

    private static func hitsForChunk(
        _ chunk: TranscriptChunk,
        config: Config
    ) -> [TranscriptBoundaryHit] {
        let text = chunk.text
        guard !text.isEmpty else { return [] }
        let duration = chunk.endTime - chunk.startTime
        guard duration > 0 else { return [] }

        // Reject the worst-quality chunks. We approximate per-chunk
        // quality from the same heuristics `TranscriptQualityEstimator`
        // uses; we re-derive locally rather than re-running the full
        // estimator pipeline because we only need the punctuation /
        // density signals here.
        let chunkQualityScore = approximateChunkQualityScore(
            text: text,
            duration: duration
        )
        guard chunkQualityScore >= config.minChunkQualityScore else {
            return []
        }

        // Apportion by word offset rather than character offset. Real ASR
        // chunks have variable word density (long words, numbers spelled
        // out, repeated short fillers) — character-offset apportionment
        // produces ±2-2.5s timing error at typical chunk midpoints, which
        // exceeds the 1.5s resolver radius and makes the cue actively
        // misleading. Word-offset apportionment is robust to density
        // variation because speech rate is roughly constant in
        // words-per-second.
        let scalarText = text

        // Quick word-count sanity check before doing any apportionment.
        // Single-word fragments don't carry meaningful sentence structure.
        let totalWords = text.split(whereSeparator: \.isWhitespace).count
        guard totalWords >= config.minWordsForApportionment else { return [] }

        var localHits: [TranscriptBoundaryHit] = []
        var inWord = false
        var wordsCompleted = 0
        for character in scalarText {
            if character.isWhitespace {
                if inWord {
                    wordsCompleted += 1
                    inWord = false
                }
                continue
            }
            inWord = true
            guard let kind = terminalKind(for: character) else { continue }
            // Word containing the terminator ends right here. The next
            // whitespace transition will not double-count because we keep
            // `wordsCompleted` driven by the whitespace transition only.
            let wordsThroughTerminator = wordsCompleted + 1
            let fraction = Double(wordsThroughTerminator) / Double(totalWords)
            let time = chunk.startTime + fraction * duration

            // Confidence = chunk quality score, scaled mildly down for
            // very short chunks. We deliberately don't overcomplicate
            // this — the resolver weight is the dominant lever.
            let confidence = chunkQualityScore
            localHits.append(
                TranscriptBoundaryHit(
                    time: time,
                    confidence: confidence,
                    terminalKind: kind
                )
            )
        }
        return localHits
    }

    /// Extract terminal kind for a sentence-ending character, or nil.
    /// Whitelisted to the three canonical English-prose terminators —
    /// other punctuation (commas, semicolons, colons, ellipses written
    /// as a single Unicode scalar U+2026) is not a sentence break.
    private static func terminalKind(for character: Character) -> SentenceTerminalKind? {
        switch character {
        case ".":
            return .period
        case "?":
            return .question
        case "!":
            return .exclamation
        default:
            return nil
        }
    }

    /// Lightweight chunk-quality proxy. Emphasises that legitimate ASR
    /// output has *some* sentence structure and reasonable word density;
    /// pathological output (1-word chunks, no punctuation, or punctuation
    /// pile-ups) lands far below the floor.
    ///
    /// Note: this intentionally doesn't reuse the full
    /// `TranscriptQualityEstimator.assess` pipeline because that
    /// requires `AdTranscriptSegment` (a different type) and runs five
    /// signals we don't all need. The two we care about — punctuation
    /// reasonableness + token density — are reproduced here for a
    /// per-chunk view that costs O(text.count) per chunk.
    static func approximateChunkQualityScore(
        text: String,
        duration: Double
    ) -> Double {
        let words = text.split(whereSeparator: \.isWhitespace)
        let wordCount = words.count
        guard wordCount > 0 else { return 0 }

        // Punctuation reasonableness: ratio of sentence terminators to
        // expected rate (~1 per 20 words). Score 1.0 at ratio = 1.0,
        // dropping linearly toward 0 at extremes.
        let terminators = text.filter { $0 == "." || $0 == "?" || $0 == "!" }.count
        let expectedTerminators = max(1.0, Double(wordCount) / 20.0)
        let punctuationRatio = Double(terminators) / expectedTerminators
        let punctuationScore = max(0.0, 1.0 - abs(punctuationRatio - 1.0) * 0.5)

        // Token density: words per second of audio. Natural speech sits
        // in [1.5, 4.0] wps. Outside that range, attenuate.
        let wps = duration > 0 ? Double(wordCount) / duration : 0
        let densityScore: Double
        if wps >= 1.5 && wps <= 4.0 {
            densityScore = 1.0
        } else if wps >= 1.0 && wps <= 5.0 {
            densityScore = 0.6
        } else if wps >= 0.5 && wps <= 6.0 {
            densityScore = 0.3
        } else {
            densityScore = 0.0
        }

        // Equal blend of the two signals. This is intentionally simpler
        // than `TranscriptQualityEstimator.score` (which weights five
        // signals) — for a per-chunk gate we only need a coarse "is
        // this chunk worth listening to?" decision.
        return min(1.0, max(0.0, (punctuationScore + densityScore) / 2.0))
    }
}
