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

    /// Composite score (0.0 = worst, 1.0 = best)
    var compositeScore: Double {
        // Weighted combination
        0.35 * punctuationScore +
        0.25 * tokenDensityScore +
        0.20 * repetitionScore +
        0.20 * wordLengthScore
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

        let assessment = TranscriptQualityAssessment(
            segmentIndex: segment.segmentIndex,
            quality: .good, // placeholder, computed below
            punctuationScore: punctuation,
            tokenDensityScore: tokenDensity,
            repetitionScore: repetition,
            wordLengthScore: wordLength
        )

        let score = assessment.compositeScore
        let quality: TranscriptQualityLevel
        if score >= thresholds.goodMinScore {
            quality = .good
        } else if score >= thresholds.degradedMinScore {
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
            wordLengthScore: wordLength
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

    /// Word length distribution: real speech has varied word lengths.
    /// ASR garbage tends toward either very short or uniform lengths.
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

        return (meanScore + stddevScore) / 2.0
    }
}
