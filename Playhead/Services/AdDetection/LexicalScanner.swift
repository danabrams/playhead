// LexicalScanner.swift
// Layer 1 of the ad detection pipeline: fast regex/keyword scanner.
//
// Scans TranscriptChunks for sponsor phrases, promo codes, URLs/CTAs,
// purchase language, and transition markers. Produces candidate ad regions
// with rough boundaries and a lexical confidence score.
//
// Runs in milliseconds per chunk — designed for hot-path use during playback.

import Foundation
import OSLog

// MARK: - Configuration

struct LexicalScannerConfig: Sendable {
    /// Maximum gap (in seconds) between adjacent hits that will be merged
    /// into a single candidate region.
    let mergeGapThreshold: TimeInterval

    /// Minimum number of distinct pattern hits required to emit a candidate.
    let minHitsForCandidate: Int

    /// Weight threshold for single-hit promotion. If any hit in a merge group
    /// has weight >= this value, the group bypasses `minHitsForCandidate` and
    /// is emitted as a candidate. Set to `.infinity` to disable the bypass.
    ///
    /// Default 0.95 targets strong signals (sponsor disclosures, promo codes,
    /// literal-TLD URLs) while excluding weaker purchase-language phrases
    /// (weight 0.9) and spoken "dot com" URL CTAs (weight 0.8).
    let highWeightBypassThreshold: Double

    /// Detector version tag written to each candidate.
    let detectorVersion: String

    /// Explicit initializer with a default for `highWeightBypassThreshold`,
    /// preserving source compatibility for existing call sites that were
    /// built against the older three-field config.
    init(
        mergeGapThreshold: TimeInterval,
        minHitsForCandidate: Int,
        highWeightBypassThreshold: Double = 0.95,
        detectorVersion: String
    ) {
        self.mergeGapThreshold = mergeGapThreshold
        self.minHitsForCandidate = minHitsForCandidate
        self.highWeightBypassThreshold = highWeightBypassThreshold
        self.detectorVersion = detectorVersion
    }

    static let `default` = LexicalScannerConfig(
        mergeGapThreshold: 30.0,
        minHitsForCandidate: 2,
        highWeightBypassThreshold: 0.95,
        detectorVersion: "lexical-v1"
    )
}

// MARK: - Pattern categories

/// Categories of lexical patterns that indicate ad content.
enum LexicalPatternCategory: String, Sendable, CaseIterable {
    case sponsor
    case promoCode
    case urlCTA
    case purchaseLanguage
    case transitionMarker
}

// MARK: - Pattern hit

/// A single regex match within a transcript chunk.
struct LexicalHit: Sendable {
    /// The category of pattern that matched.
    let category: LexicalPatternCategory
    /// The matched text.
    let matchedText: String
    /// Start time in episode audio seconds (interpolated from chunk).
    let startTime: Double
    /// End time in episode audio seconds (interpolated from chunk).
    let endTime: Double
    /// Weight of this pattern category for confidence scoring.
    let weight: Double
}

// MARK: - Candidate ad region

/// A candidate ad region produced by the lexical scanner.
/// Downstream layers (feature extraction, boundary snapping) refine these.
struct LexicalCandidate: Sendable {
    /// Unique identifier.
    let id: String
    /// Analysis asset this candidate belongs to.
    let analysisAssetId: String
    /// Rough start time in episode audio seconds.
    let startTime: Double
    /// Rough end time in episode audio seconds.
    let endTime: Double
    /// Lexical confidence score (0.0...1.0).
    let confidence: Double
    /// Number of pattern hits that contributed to this candidate.
    let hitCount: Int
    /// Categories of patterns that contributed.
    let categories: Set<LexicalPatternCategory>
    /// Representative evidence text (first significant hit).
    let evidenceText: String
    /// Detector version tag.
    let detectorVersion: String
}

// MARK: - LexicalScanner

/// Fast regex/keyword scanner on transcript chunks.
/// Catches ~60-70% of ads via lexical signals alone.
///
/// Thread-safe: all state is either immutable or isolated to method scope.
/// Patterns are compiled once at init for performance.
struct LexicalScanner: Sendable {

    private let logger = Logger(subsystem: "com.playhead", category: "LexicalScanner")
    private let config: LexicalScannerConfig

    /// Compiled regex patterns grouped by category.
    /// Built once at init — regex compilation is expensive.
    private let patternGroups: [LexicalPatternCategory: [NSRegularExpression]]

    /// Compiled "strong URL" patterns — literal-TLD matches like `cvs.com`
    /// or `teamcoco.io`. Emitted as `.urlCTA` hits with a boosted weight
    /// (`strongUrlWeight`) so a single match can bypass `minHitsForCandidate`
    /// via the `highWeightBypassThreshold` check.
    private let strongUrlPatterns: [NSRegularExpression]

    /// Weight used for literal-TLD URL hits. Set slightly above the default
    /// high-weight bypass threshold so a single URL promotes to a candidate.
    private static let strongUrlWeight: Double = 0.95

    /// Per-show sponsor terms parsed from PodcastProfile.sponsorLexicon.
    /// Empty array when no profile is available.
    private let showSponsorPatterns: [NSRegularExpression]

    /// Pre-compiled patterns from active SponsorKnowledgeStore entries.
    /// Nil when no knowledge store is available for this scan session.
    private let compiledLexicon: CompiledSponsorLexicon?

    // MARK: - Init

    init(
        config: LexicalScannerConfig = .default,
        podcastProfile: PodcastProfile? = nil,
        compiledLexicon: CompiledSponsorLexicon? = nil
    ) {
        self.config = config
        self.patternGroups = Self.compileBuiltInPatterns()
        self.strongUrlPatterns = Self.compileStrongUrlPatterns()
        self.showSponsorPatterns = Self.compileSponsorLexicon(
            from: podcastProfile
        )
        self.compiledLexicon = compiledLexicon
    }

    // MARK: - Public API

    /// Scan a batch of transcript chunks and return candidate ad regions.
    ///
    /// Hits from adjacent chunks within `mergeGapThreshold` are merged
    /// into a single candidate. Each candidate must have at least
    /// `minHitsForCandidate` pattern hits.
    ///
    /// - Parameters:
    ///   - chunks: Transcript chunks to scan (should be time-ordered).
    ///   - analysisAssetId: The analysis asset these chunks belong to.
    /// - Returns: Candidate ad regions sorted by start time.
    func scan(
        chunks: [TranscriptChunk],
        analysisAssetId: String
    ) -> [LexicalCandidate] {
        // 1. Collect all hits across chunks.
        var allHits: [LexicalHit] = []
        for chunk in chunks {
            let hits = scanChunk(chunk)
            allHits.append(contentsOf: hits)
        }

        guard !allHits.isEmpty else { return [] }

        // 2. Sort hits by start time.
        allHits.sort { $0.startTime < $1.startTime }

        // 3. Merge adjacent hits within the gap threshold.
        let candidates = mergeHits(
            allHits,
            analysisAssetId: analysisAssetId
        )

        logger.info("Scanned \(chunks.count) chunks, found \(allHits.count) hits, produced \(candidates.count) candidates")

        return candidates
    }

    /// Scan a single chunk. Useful for streaming hot-path processing
    /// where chunks arrive one at a time.
    ///
    /// Most pattern categories scan `chunk.normalizedText` because
    /// normalization (lowercasing, punctuation stripping, whitespace
    /// collapsing) makes the regexes simpler and more robust. The strong
    /// URL patterns are an exception: production normalization strips
    /// the literal `.` from `cvs.com`, so those patterns are run against
    /// `chunk.text` (the raw ASR output) instead. See
    /// `TranscriptEngineService.normalizeText` for the canonical
    /// normalization rules.
    func scanChunk(_ chunk: TranscriptChunk) -> [LexicalHit] {
        var normalizedText = chunk.normalizedText
        if normalizedText.isEmpty {
            // Fall back to raw text if normalizedText is not yet populated.
            normalizedText = chunk.text
            if normalizedText.isEmpty {
                return []
            }
            logger.warning("normalizedText empty for chunk at \(chunk.startTime, format: .fixed(precision: 1))s, falling back to raw text")
        }

        var hits: [LexicalHit] = []

        // Scan built-in patterns over normalized text.
        let normalizedNS = normalizedText as NSString
        let normalizedRange = NSRange(location: 0, length: normalizedNS.length)
        for (category, patterns) in patternGroups {
            let weight = Self.categoryWeight(category)
            for pattern in patterns {
                let matches = pattern.matches(in: normalizedText, range: normalizedRange)
                for match in matches {
                    let matchedText = normalizedNS.substring(with: match.range)
                    let (startTime, endTime) = interpolateTiming(
                        matchRange: match.range,
                        textLength: normalizedNS.length,
                        chunkStart: chunk.startTime,
                        chunkEnd: chunk.endTime
                    )
                    hits.append(LexicalHit(
                        category: category,
                        matchedText: matchedText,
                        startTime: startTime,
                        endTime: endTime,
                        weight: weight
                    ))
                }
            }
        }

        // Scan strong URL patterns (literal TLDs like `cvs.com`) over
        // the RAW chunk text. The production normalizer strips `.`, which
        // would prevent these patterns from ever matching the dot-bearing
        // domain tokens they target. Reading the raw text preserves
        // `cvs.com`, `siriusxm.com`, `teamcoco.com`, etc, and the
        // resulting hits are emitted with timing interpolated against
        // the raw text length and a boosted weight so a single hit
        // promotes to a candidate via the high-weight bypass.
        let rawText = chunk.text
        if !rawText.isEmpty {
            let rawNS = rawText as NSString
            let rawRange = NSRange(location: 0, length: rawNS.length)
            for pattern in strongUrlPatterns {
                let matches = pattern.matches(in: rawText, range: rawRange)
                for match in matches {
                    let matchedText = rawNS.substring(with: match.range)
                    let (startTime, endTime) = interpolateTiming(
                        matchRange: match.range,
                        textLength: rawNS.length,
                        chunkStart: chunk.startTime,
                        chunkEnd: chunk.endTime
                    )
                    hits.append(LexicalHit(
                        category: .urlCTA,
                        matchedText: matchedText,
                        startTime: startTime,
                        endTime: endTime,
                        weight: Self.strongUrlWeight
                    ))
                }
            }
        }

        // Scan per-show sponsor patterns (boosted weight) over normalized text.
        for pattern in showSponsorPatterns {
            let matches = pattern.matches(in: normalizedText, range: normalizedRange)
            for match in matches {
                let matchedText = normalizedNS.substring(with: match.range)
                let (startTime, endTime) = interpolateTiming(
                    matchRange: match.range,
                    textLength: normalizedNS.length,
                    chunkStart: chunk.startTime,
                    chunkEnd: chunk.endTime
                )
                hits.append(LexicalHit(
                    category: .sponsor,
                    matchedText: matchedText,
                    startTime: startTime,
                    endTime: endTime,
                    weight: 1.5 // Boosted: known sponsor for this show
                ))
            }
        }

        // Scan compiled sponsor knowledge patterns (boosted weight) over
        // normalized text. These come from active SponsorKnowledgeStore
        // entries and coexist with the per-show showSponsorPatterns above.
        if let lexicon = compiledLexicon {
            for pattern in lexicon.patterns {
                let matches = pattern.matches(in: normalizedText, range: normalizedRange)
                for match in matches {
                    let matchedText = normalizedNS.substring(with: match.range)
                    let (startTime, endTime) = interpolateTiming(
                        matchRange: match.range,
                        textLength: normalizedNS.length,
                        chunkStart: chunk.startTime,
                        chunkEnd: chunk.endTime
                    )
                    hits.append(LexicalHit(
                        category: .sponsor,
                        matchedText: matchedText,
                        startTime: startTime,
                        endTime: endTime,
                        weight: 1.5 // Boosted: known sponsor from knowledge store
                    ))
                }
            }
        }

        return hits
    }

    /// Re-run the scanner over a synthetic region-sized chunk while preserving
    /// the same normalization, pattern matching, and confidence rules as the
    /// regular transcript-chunk path.
    func rescoreRegionText(
        _ text: String,
        analysisAssetId: String,
        startTime: Double,
        endTime: Double
    ) -> LexicalCandidate? {
        guard !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return nil
        }

        // Synthetic ID and segmentFingerprint are safe here: scanChunk is a pure
        // regex pass and does not persist or memoize anything keyed on these
        // fields. The chunk never leaves this function — it is built, scanned,
        // and discarded — so minting fresh UUIDs cannot pollute any cache.
        let syntheticChunk = TranscriptChunk(
            id: UUID().uuidString,
            analysisAssetId: analysisAssetId,
            segmentFingerprint: UUID().uuidString,
            chunkIndex: 0,
            startTime: startTime,
            endTime: endTime,
            text: text,
            normalizedText: TranscriptEngineService.normalizeText(text),
            pass: "final",
            modelVersion: "region-feature-extractor",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
        let hits = scanChunk(syntheticChunk)
        guard !hits.isEmpty else { return nil }

        return buildCandidate(
            from: hits,
            startTime: startTime,
            endTime: endTime,
            analysisAssetId: analysisAssetId
        )
    }

    // MARK: - Pattern compilation

    /// Compile all built-in regex patterns. Called once at init.
    private static func compileBuiltInPatterns() -> [LexicalPatternCategory: [NSRegularExpression]] {
        var groups: [LexicalPatternCategory: [NSRegularExpression]] = [:]

        // Sponsor phrases
        groups[.sponsor] = compilePatterns([
            #"brought to you by"#,
            #"sponsored by"#,
            #"today s sponsor"#,
            #"thanks to our sponsor"#,
            #"this episode is sponsored"#,
            #"this podcast is brought"#,
            #"a word from our sponsor"#,
            #"message from our sponsor"#,
            #"supported by"#,
        ])

        // Promo codes
        groups[.promoCode] = compilePatterns([
            #"use code \w+"#,
            #"promo code \w+"#,
            #"discount code \w+"#,
            #"coupon code \w+"#,
            #"code \w+ at checkout"#,
            #"enter code \w+"#,
        ])

        // URLs and CTAs
        groups[.urlCTA] = compilePatterns([
            #"\w+ com slash \w+"#,
            #"dot com slash \w+"#,
            #"check out \w+"#,
            #"head to \w+"#,
            #"go to \w+ com"#,
            #"visit \w+ com"#,
            #"head over to"#,
            #"\w+ dot com"#,
            #"click the link"#,
            #"link in the description"#,
            #"link in the show notes"#,
        ])

        // Purchase language
        groups[.purchaseLanguage] = compilePatterns([
            #"free trial"#,
            #"money back guarantee"#,
            #"first month free"#,
            #"\d+ percent off"#,
            #"satisfaction guarantee"#,
            #"risk free"#,
            #"sign up today"#,
            #"sign up now"#,
            #"limited time offer"#,
            #"exclusive offer"#,
            #"special offer"#,
        ])

        // Transition markers (lower weight — these indicate ad boundaries,
        // not ad content themselves)
        groups[.transitionMarker] = compilePatterns([
            #"let s get back to"#,
            #"and now back to"#,
            #"back to the show"#,
            #"back to the episode"#,
            #"anyway\b"#,
            #"without further ado"#,
            #"moving on"#,
            // "so + pronoun/article" removed — too many false positives in normal speech
        ])

        return groups
    }

    /// Compile the "strong URL" pattern set: literal domain-like tokens
    /// with a recognizable TLD (`host.com`, `host.net`, `host.io`, etc).
    ///
    /// Uses explicit character classes rather than `\w` because `\w` does
    /// not consume the `.` and produces overlapping/partial matches. The
    /// leading `\b` anchor prevents matching inside larger words, and the
    /// trailing `(?![a-z0-9])` guard keeps us from matching a longer TLD
    /// prefix (e.g. ".coach" when looking for ".co").
    private static func compileStrongUrlPatterns() -> [NSRegularExpression] {
        let tlds = ["com", "net", "org", "io", "co", "app", "fm", "tv"]
        let tldAlternation = tlds.joined(separator: "|")
        // host: one or more dot-separated alnum/hyphen labels
        // (matches `cvs`, `team-coco`, `siriusxm`, `news.example`)
        let pattern = #"\b[a-z0-9][a-z0-9\-]*(?:\.[a-z0-9][a-z0-9\-]*)*\.(?:"# +
            tldAlternation +
            #")\b(?![a-z0-9])"#
        return compilePatterns([pattern])
    }

    /// Compile pattern strings into NSRegularExpression instances.
    /// Patterns that fail to compile are logged and skipped.
    private static func compilePatterns(_ patterns: [String]) -> [NSRegularExpression] {
        patterns.compactMap { pattern in
            try? NSRegularExpression(
                pattern: pattern,
                options: [.caseInsensitive]
            )
        }
    }

    /// Parse the sponsor lexicon from a PodcastProfile and compile
    /// per-show sponsor patterns. The lexicon is stored as a
    /// comma-separated string of sponsor names/phrases.
    private static func compileSponsorLexicon(
        from profile: PodcastProfile?
    ) -> [NSRegularExpression] {
        guard let lexicon = profile?.sponsorLexicon, !lexicon.isEmpty else {
            return []
        }

        return lexicon
            .components(separatedBy: ",")
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .filter { !$0.isEmpty }
            .compactMap { term in
                // Escape the term for regex safety, then compile.
                let escaped = NSRegularExpression.escapedPattern(for: term.lowercased())
                return try? NSRegularExpression(
                    pattern: #"\b"# + escaped + #"\b"#,
                    options: [.caseInsensitive]
                )
            }
    }

    // MARK: - Category weights

    /// Weight multiplier for each pattern category.
    /// Higher weight = stronger ad signal.
    private static func categoryWeight(
        _ category: LexicalPatternCategory
    ) -> Double {
        switch category {
        case .sponsor:          return 1.0
        case .promoCode:        return 1.2
        case .urlCTA:           return 0.8
        case .purchaseLanguage: return 0.9
        case .transitionMarker: return 0.3
        }
    }

    // MARK: - Time interpolation

    /// Estimate timing of a regex match within a chunk by linear
    /// interpolation over character position. Rough but sufficient
    /// for candidate boundaries — downstream layers snap to precise
    /// audio features.
    private func interpolateTiming(
        matchRange: NSRange,
        textLength: Int,
        chunkStart: Double,
        chunkEnd: Double
    ) -> (start: Double, end: Double) {
        guard textLength > 0 else { return (chunkStart, chunkEnd) }

        let duration = chunkEnd - chunkStart
        let startFraction = Double(matchRange.location) / Double(textLength)
        let endFraction = Double(matchRange.location + matchRange.length) / Double(textLength)

        let startTime = chunkStart + duration * startFraction
        let endTime = chunkStart + duration * endFraction

        return (startTime, endTime)
    }

    // MARK: - Hit merging

    /// Merge adjacent hits within the gap threshold into candidate regions.
    /// Hits must be pre-sorted by startTime.
    private func mergeHits(
        _ hits: [LexicalHit],
        analysisAssetId: String
    ) -> [LexicalCandidate] {
        guard let first = hits.first else { return [] }

        var candidates: [LexicalCandidate] = []

        // Accumulator for the current merge group.
        var groupStart = first.startTime
        var groupEnd = first.endTime
        var groupHits: [LexicalHit] = [first]

        for hit in hits.dropFirst() {
            if hit.startTime <= groupEnd + config.mergeGapThreshold {
                // Extend the current group.
                groupEnd = max(groupEnd, hit.endTime)
                groupHits.append(hit)
            } else {
                // Emit the current group if it meets the threshold.
                if let candidate = buildCandidate(
                    from: groupHits,
                    startTime: groupStart,
                    endTime: groupEnd,
                    analysisAssetId: analysisAssetId
                ) {
                    candidates.append(candidate)
                }

                // Start a new group.
                groupStart = hit.startTime
                groupEnd = hit.endTime
                groupHits = [hit]
            }
        }

        // Emit the final group.
        if let candidate = buildCandidate(
            from: groupHits,
            startTime: groupStart,
            endTime: groupEnd,
            analysisAssetId: analysisAssetId
        ) {
            candidates.append(candidate)
        }

        return candidates
    }

    /// Build a LexicalCandidate from a group of merged hits.
    /// Returns nil if the group doesn't meet the minimum hit count.
    private func buildCandidate(
        from hits: [LexicalHit],
        startTime: Double,
        endTime: Double,
        analysisAssetId: String
    ) -> LexicalCandidate? {
        // A merge group normally needs `minHitsForCandidate` hits to emit,
        // but a single sufficiently strong hit (e.g. a sponsor disclosure,
        // promo code, or literal-TLD URL) bypasses that threshold.
        let hasHighWeightHit = hits.contains { hit in
            hit.weight >= config.highWeightBypassThreshold
        }
        if hits.count < config.minHitsForCandidate && !hasHighWeightHit {
            return nil
        }

        let categories = Set(hits.map(\.category))
        let totalWeight = hits.reduce(0.0) { $0 + $1.weight }

        // Confidence: sigmoid-like scaling based on total weight.
        // 2 hits at weight 1.0 each gives ~0.50; 5+ hits saturates near 0.9.
        let rawConfidence = 1.0 - 1.0 / (1.0 + totalWeight * 0.3)
        let confidence = min(rawConfidence, 0.95)

        // Pick the most significant hit as evidence (highest weight).
        let bestHit = hits.max { $0.weight < $1.weight }
        let evidenceText = bestHit?.matchedText ?? hits[0].matchedText

        return LexicalCandidate(
            id: UUID().uuidString,
            analysisAssetId: analysisAssetId,
            startTime: startTime,
            endTime: endTime,
            confidence: confidence,
            hitCount: hits.count,
            categories: categories,
            evidenceText: evidenceText,
            detectorVersion: config.detectorVersion
        )
    }
}
