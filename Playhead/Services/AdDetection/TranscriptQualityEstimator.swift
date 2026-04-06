import Foundation

// MARK: - TranscriptQualityLevel

/// Estimated transcript quality for a segment. Maps to FM schema TranscriptQuality.
enum TranscriptQualityLevel: String, Sendable, Codable {
    case good       // clean ASR output, reliable for FM analysis
    case degraded   // noisy but potentially usable with lower confidence
    case unusable   // too noisy for FM — skip or use coarse-only
}

// MARK: - TranscriptQualityAssessment

/// Quality assessment for a transcript segment.
struct TranscriptQualityAssessment: Sendable {
    let segmentIndex: Int
    let quality: TranscriptQualityLevel

    /// Individual signal scores (0.0 = worst, 1.0 = best)
    let punctuationScore: Double
    let tokenDensityScore: Double
    let repetitionScore: Double
    let wordLengthScore: Double
    /// Self-consistency score: high when the four other signals agree,
    /// low when they disagree (one outlier among consistent signals).
    /// This is NOT an ASR-confidence proxy — the underlying ASR stack does
    /// not expose per-word confidence on iOS, so this serves as a much
    /// weaker self-consistency penalty rather than independent confidence.
    let signalAgreementScore: Double

    /// Numeric quality score (0.0 = worst, 1.0 = best) used for finer-grained decisions.
    let qualityScore: Double

    /// Backward-compatible alias retained for existing callers/tests.
    var compositeScore: Double {
        qualityScore
    }

    // MARK: - Weight calibration
    //
    // The 0.30 / 0.25 / 0.20 / 0.15 / 0.10 weights below are hand-tuned
    // priors. They reflect the order in which the signals empirically
    // discriminate between good vs. degraded vs. unusable transcripts in
    // ad-hoc spot checks but they are NOT calibrated against ground-truth
    // labels. Re-evaluate (and ideally fit via logistic regression) once
    // SponsorKnowledgeStore (Phase 8) provides labeled positive/negative
    // ad-segment transcripts. The `signalAgreementScore` weight is
    // intentionally the smallest because it is a self-consistency
    // diagnostic, not an independent quality signal.
    static func score(
        punctuationScore: Double,
        tokenDensityScore: Double,
        repetitionScore: Double,
        wordLengthScore: Double,
        signalAgreementScore: Double
    ) -> Double {
        // Weighted combination
        0.30 * punctuationScore +
        0.25 * tokenDensityScore +
        0.20 * repetitionScore +
        0.15 * wordLengthScore +
        0.10 * signalAgreementScore
    }

    /// Self-consistency score: `mean * (1 - variance)` over the four
    /// independent signals. High when signals agree (low variance), low
    /// when one signal disagrees with the others. Not a substitute for
    /// per-word ASR confidence.
    static func signalAgreementScore(
        punctuationScore: Double,
        tokenDensityScore: Double,
        repetitionScore: Double,
        wordLengthScore: Double
    ) -> Double {
        let scores = [punctuationScore, tokenDensityScore, repetitionScore, wordLengthScore]
        let mean = scores.reduce(0, +) / Double(scores.count)
        let variance = scores
            .map { ($0 - mean) * ($0 - mean) }
            .reduce(0, +) / Double(scores.count)
        return max(0.0, min(1.0, mean * (1.0 - variance)))
    }
}

// MARK: - TranscriptQualityEstimator

enum TranscriptQualityEstimator {

    struct Thresholds: Sendable {
        let goodMinScore: Double
        let degradedMinScore: Double

        static let `default` = Thresholds(
            goodMinScore: 0.65,
            degradedMinScore: 0.35
        )
    }

    /// Estimate quality for a single segment.
    static func assess(
        segment: AdTranscriptSegment,
        thresholds: Thresholds = .default
    ) -> TranscriptQualityAssessment {
        let text = segment.text

        let punctuation = punctuationDensity(text)
        let tokenDensity = tokenDensityScore(segment)
        let repetition = repetitionScore(text)
        let wordLength = wordLengthDistributionScore(text)
        let agreement = TranscriptQualityAssessment.signalAgreementScore(
            punctuationScore: punctuation,
            tokenDensityScore: tokenDensity,
            repetitionScore: repetition,
            wordLengthScore: wordLength
        )

        let qualityScore = TranscriptQualityAssessment.score(
            punctuationScore: punctuation,
            tokenDensityScore: tokenDensity,
            repetitionScore: repetition,
            wordLengthScore: wordLength,
            signalAgreementScore: agreement
        )

        let quality: TranscriptQualityLevel
        if qualityScore >= thresholds.goodMinScore {
            quality = .good
        } else if qualityScore >= thresholds.degradedMinScore {
            quality = .degraded
        } else {
            quality = .unusable
        }

        return TranscriptQualityAssessment(
            segmentIndex: segment.segmentIndex,
            quality: quality,
            punctuationScore: punctuation,
            tokenDensityScore: tokenDensity,
            repetitionScore: repetition,
            wordLengthScore: wordLength,
            signalAgreementScore: agreement,
            qualityScore: qualityScore
        )
    }

    /// Batch assess all segments.
    static func assess(
        segments: [AdTranscriptSegment],
        thresholds: Thresholds = .default
    ) -> [TranscriptQualityAssessment] {
        segments.map { assess(segment: $0, thresholds: thresholds) }
    }

    // MARK: - Quality Signals

    /// Punctuation density: well-transcribed speech has sentence punctuation.
    /// Score: ratio of sentences (terminated by .!?) to expected rate (~1 per 15-25 words).
    private static func punctuationDensity(_ text: String) -> Double {
        let words = text.split(whereSeparator: \.isWhitespace)
        guard words.count >= 5 else { return 0.5 } // too short to assess

        let sentenceEnders = text.filter { $0 == "." || $0 == "!" || $0 == "?" }.count
        let expectedSentences = max(1.0, Double(words.count) / 20.0)
        let ratio = Double(sentenceEnders) / expectedSentences

        // Optimal: ~1.0 ratio. Penalize both extremes.
        return min(1.0, max(0.0, 1.0 - abs(ratio - 1.0) * 0.5))
    }

    /// Token density: words per second of audio. Very low density suggests
    /// gaps/silence misattributed as speech. Very high suggests garbled output.
    private static func tokenDensityScore(_ segment: AdTranscriptSegment) -> Double {
        let duration = segment.duration
        guard duration > 0 else { return 0.0 }

        let wordCount = segment.text.split(whereSeparator: \.isWhitespace).count
        let wps = Double(wordCount) / duration

        // Natural speech: ~2.0-3.5 words per second
        if wps >= 1.5 && wps <= 4.0 {
            return 1.0
        } else if wps >= 1.0 && wps <= 5.0 {
            return 0.6
        } else if wps >= 0.5 && wps <= 6.0 {
            return 0.3
        } else {
            return 0.0
        }
    }

    /// Repetition score: excessive repetition indicates ASR looping/hallucination.
    private static func repetitionScore(_ text: String) -> Double {
        let words = text.lowercased().split(whereSeparator: \.isWhitespace).map(String.init)
        guard words.count >= 10 else { return 0.8 } // too short to assess reliably

        // Check for repeated bigrams
        var bigramCounts: [String: Int] = [:]
        for i in 0..<(words.count - 1) {
            let bigram = "\(words[i]) \(words[i + 1])"
            bigramCounts[bigram, default: 0] += 1
        }

        let totalBigrams = max(1, words.count - 1)
        let repeatedBigrams = bigramCounts.values.filter { $0 > 2 }.reduce(0, +)
        let repetitionRatio = Double(repeatedBigrams) / Double(totalBigrams)

        // Low repetition is good. High repetition (>20%) is bad.
        return max(0.0, 1.0 - repetitionRatio * 3.0)
    }

    /// Word length distribution plus unusual-token screening. This acts as a
    /// lightweight OOV proxy when the ASR stack does not expose per-word confidence.
    private static func wordLengthDistributionScore(_ text: String) -> Double {
        let words = text.split(whereSeparator: \.isWhitespace)
        guard words.count >= 5 else { return 0.5 }

        let lengths = words.map { Double($0.count) }
        let mean = lengths.reduce(0, +) / Double(lengths.count)
        let variance = lengths.map { ($0 - mean) * ($0 - mean) }.reduce(0, +) / Double(lengths.count)
        let stddev = sqrt(variance)

        // English speech: mean ~4-5 chars, stddev ~2-3
        let meanScore: Double
        if mean >= 3.0 && mean <= 7.0 {
            meanScore = 1.0
        } else {
            meanScore = max(0.0, 1.0 - abs(mean - 5.0) * 0.2)
        }

        let stddevScore: Double
        if stddev >= 1.5 && stddev <= 4.0 {
            stddevScore = 1.0
        } else if stddev < 1.5 {
            stddevScore = stddev / 1.5 // too uniform
        } else {
            stddevScore = max(0.0, 1.0 - (stddev - 4.0) * 0.2)
        }

        let unusualTokenPenalty = unusualTokenPenalty(words: words.map(String.init))

        return max(0.0, min(1.0, ((meanScore + stddevScore) / 2.0) * (1.0 - unusualTokenPenalty)))
    }

    private static func unusualTokenPenalty(words: [String]) -> Double {
        guard !words.isEmpty else { return 0.0 }

        let penalties = words.map { word -> Double in
            let lower = word.lowercased()
            let letters = lower.filter(\.isLetter)
            let digits = lower.filter(\.isNumber)

            var penalty = 0.0
            if letters.isEmpty {
                penalty += 0.4
            }
            if !digits.isEmpty && !letters.isEmpty {
                penalty += 0.2
            }
            if lower.count > 18 {
                penalty += 0.3
            }
            if longRepeatedCharacterRun(in: lower) {
                penalty += 0.3
            }
            if letters.count >= 5 && vowelRatio(in: letters) < 0.2 {
                penalty += 0.2
            }
            return min(1.0, penalty)
        }

        return penalties.reduce(0, +) / Double(penalties.count)
    }

    private static func longRepeatedCharacterRun(in word: String) -> Bool {
        var last: Character?
        var runLength = 0
        for character in word {
            if character == last {
                runLength += 1
            } else {
                last = character
                runLength = 1
            }
            if runLength >= 4 {
                return true
            }
        }
        return false
    }

    private static func vowelRatio(in letters: String) -> Double {
        guard !letters.isEmpty else { return 0.0 }
        let vowelCount = letters.filter { "aeiou".contains($0) }.count
        return Double(vowelCount) / Double(letters.count)
    }
}
