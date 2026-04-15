// ChapterEvidence.swift
// ef2.2.4: Chapter-marker evidence for ad detection.
//
// Parses chapter markers from ID3 CHAP/CTOC frames and Podcasting 2.0
// chapters JSON, classifies each chapter's disposition via deterministic
// regex, and scores quality for downstream evidence fusion.
//
// Shadow mode only: evidence is logged but does not influence live decisions.

import Foundation

// MARK: - ChapterSource

/// Origin of a chapter marker.
enum ChapterSource: String, Sendable, Codable, Equatable {
    /// ID3 CHAP/CTOC frame embedded in audio file metadata.
    case id3
    /// Podcasting 2.0 `podcast:chapters` external JSON.
    case pc20
}

// MARK: - ChapterDisposition

/// Deterministic classification of a chapter's skip-worthiness.
enum ChapterDisposition: String, Sendable, Codable, Equatable {
    /// Chapter title matches ad/sponsor patterns.
    case adBreak
    /// Chapter title describes editorial content.
    case content
    /// Title is absent, generic, or otherwise unclassifiable.
    case ambiguous
}

// MARK: - ChapterEvidence

/// A single chapter marker with disposition classification and quality score.
///
/// High-quality `.adBreak` chapters produce strong evidence cues (Phase C activation).
/// `.content` chapters produce a soft crossing penalty (not a hard negative).
struct ChapterEvidence: Sendable, Equatable, Codable {
    /// Start time in seconds from episode beginning.
    let startTime: TimeInterval
    /// End time in seconds from episode beginning. `nil` if unknown (last chapter).
    let endTime: TimeInterval?
    /// Chapter title as provided by the source. May be `nil` for untitled chapters.
    let title: String?
    /// Where this chapter marker came from.
    let source: ChapterSource
    /// Deterministic classification based on title regex.
    let disposition: ChapterDisposition
    /// Quality score in [0, 1] reflecting title specificity and structural reliability.
    /// Higher values indicate more trustworthy evidence.
    let qualityScore: Float
}

// MARK: - ChapterDispositionClassifier

/// Deterministic regex-based classifier for chapter title disposition.
///
/// Thread-safe: all patterns are compiled once at init and the classifier is value-typed.
struct ChapterDispositionClassifier: Sendable {

    // MARK: - Ad Break Patterns

    /// Patterns that indicate a chapter is an ad or sponsor segment.
    /// Case-insensitive, applied to the full title.
    private static let adBreakPatterns: [NSRegularExpression] = {
        let patterns = [
            // Explicit ad/advertisement markers
            #"\bad(vert(isement)?|break|s)?\b"#,
            // Sponsor markers
            #"\bsponsor(ed|ship)?\b"#,
            #"\bbrought\s+to\s+you\s+by\b"#,
            #"\bpresented\s+by\b"#,
            // Commercial break markers
            #"\bcommercial\b"#,
            #"\bmid[\s-]?roll\b"#,
            #"\bpre[\s-]?roll\b"#,
            #"\bpost[\s-]?roll\b"#,
            // Promo markers
            #"\bpromo(tion)?\b"#,
            // Support/offer markers (common in podcast chapters)
            #"\bsupport(ed)?\s+by\b"#,
            #"\bspecial\s+offer\b"#,
        ]
        return patterns.compactMap { try? NSRegularExpression(pattern: $0, options: .caseInsensitive) }
    }()

    // MARK: - Content Patterns

    /// Patterns that indicate a chapter describes editorial/content segments.
    /// These are deliberately broad: titled chapters that are not ads are likely content.
    private static let contentPatterns: [NSRegularExpression] = {
        let patterns = [
            // Interview/discussion markers
            #"\binterview\b"#,
            #"\bdiscussion\b"#,
            #"\bconversation\b"#,
            // Topic/segment markers
            #"\btopic\b"#,
            #"\bsegment\b"#,
            #"\bchapter\b"#,
            // Question/answer
            #"\bq\s*&\s*a\b"#,
            #"\bquestion\b"#,
            // Introduction/conclusion
            #"\bintro(duction)?\b"#,
            #"\boutro\b"#,
            #"\bconclusion\b"#,
            #"\bwrap[\s-]?up\b"#,
            // Story/narrative
            #"\bstory\b"#,
            #"\bnews\b"#,
            #"\bupdate\b"#,
            #"\brecap\b"#,
            // Numbered content (e.g. "Part 1", "Chapter 3")
            #"\bpart\s+\d+\b"#,
            // Titles with 4+ words that don't match ad patterns are likely content descriptions
        ]
        return patterns.compactMap { try? NSRegularExpression(pattern: $0, options: .caseInsensitive) }
    }()

    /// Minimum word count for a title to be classified as content by length alone
    /// (when no explicit content pattern matches but also no ad pattern matches).
    private static let contentMinWordCount = 4

    // MARK: - Classification

    /// Classify a chapter title into a disposition.
    ///
    /// Priority: adBreak > content > ambiguous.
    /// `nil` or empty titles always return `.ambiguous`.
    func classify(_ title: String?) -> ChapterDisposition {
        guard let title, !title.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return .ambiguous
        }

        let range = NSRange(title.startIndex..., in: title)

        // Check ad patterns first (higher priority)
        for pattern in Self.adBreakPatterns {
            if pattern.firstMatch(in: title, range: range) != nil {
                return .adBreak
            }
        }

        // Check explicit content patterns
        for pattern in Self.contentPatterns {
            if pattern.firstMatch(in: title, range: range) != nil {
                return .content
            }
        }

        // Titles with enough words are likely descriptive content
        let wordCount = title.split(whereSeparator: { $0.isWhitespace || $0 == "-" }).count
        if wordCount >= Self.contentMinWordCount {
            return .content
        }

        return .ambiguous
    }
}

// MARK: - ChapterQualityScorer

/// Scores chapter evidence quality based on title specificity, structural
/// coverage, and source reliability.
struct ChapterQualityScorer: Sendable {

    /// Score a single chapter's quality.
    ///
    /// Factors:
    /// - Title specificity: titled chapters score higher than untitled.
    /// - Disposition confidence: explicit ad/content matches score higher than ambiguous.
    /// - Time bounds: chapters with both start and end times score higher.
    /// - Source: PC20 chapters are typically more curated than ID3.
    func score(
        title: String?,
        disposition: ChapterDisposition,
        hasEndTime: Bool,
        source: ChapterSource
    ) -> Float {
        var score: Float = 0.0

        // Base: titled vs untitled
        if let title, !title.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            score += 0.3
            // Bonus for longer, more specific titles
            let wordCount = title.split(whereSeparator: { $0.isWhitespace }).count
            if wordCount >= 3 {
                score += 0.1
            }
        }

        // Disposition confidence
        switch disposition {
        case .adBreak:
            score += 0.3
        case .content:
            score += 0.2
        case .ambiguous:
            score += 0.0
        }

        // Time bounds completeness
        if hasEndTime {
            score += 0.2
        }

        // Source reliability
        switch source {
        case .pc20:
            score += 0.1
        case .id3:
            score += 0.05
        }

        return min(score, 1.0)
    }
}
