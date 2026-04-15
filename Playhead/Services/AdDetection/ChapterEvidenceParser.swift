// ChapterEvidenceParser.swift
// ef2.2.4: Parses chapter markers from ID3 metadata and Podcasting 2.0 JSON.
//
// Three entry points:
//   1. `parseID3Chapters(from:)` — extracts CHAP/CTOC from AVAsset metadata.
//   2. `parsePodcasting20Chapters(from:)` — decodes external JSON from a URL.
//   3. `fromParsedChapters(_:episodeDuration:)` — converts inline RSS chapters.
//
// All parsing is async, non-blocking, and produces [ChapterEvidence].
// Shadow mode: callers log evidence without influencing live ad decisions.

import AVFoundation
import Foundation
import OSLog

// MARK: - ChapterEvidenceParser

/// Parses chapter markers from multiple sources and produces classified
/// `ChapterEvidence` arrays.
///
/// Thread-safe: stateless, all methods are static or use only local state.
enum ChapterEvidenceParser {

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "ChapterEvidenceParser"
    )

    private static let classifier = ChapterDispositionClassifier()
    private static let scorer = ChapterQualityScorer()

    // MARK: - ID3 Chapter Parsing (AVFoundation)

    /// Extract chapter evidence from ID3 CHAP/CTOC frames in an audio asset.
    ///
    /// Uses AVFoundation's `AVAsset.chapterMetadataGroups(bestMatchingPreferredLanguages:)`
    /// to access chapter metadata without third-party dependencies.
    ///
    /// - Parameter asset: The audio asset (local file or remote URL).
    /// - Returns: Classified chapter evidence, sorted by start time.
    static func parseID3Chapters(from asset: AVAsset) async -> [ChapterEvidence] {
        // Load chapter metadata groups
        let languages = Locale.preferredLanguages
        let groups: [AVTimedMetadataGroup]
        do {
            groups = try await asset.loadChapterMetadataGroups(
                bestMatchingPreferredLanguages: languages
            )
        } catch {
            logger.debug("No ID3 chapter metadata available: \(error.localizedDescription)")
            return []
        }

        guard !groups.isEmpty else { return [] }

        var evidence: [ChapterEvidence] = []

        for group in groups {
            let startTime = group.timeRange.start.seconds
            // Validate startTime: skip malformed ID3 frames
            guard startTime.isFinite, startTime >= 0 else { continue }

            let duration = group.timeRange.duration.seconds
            let endTime = duration.isFinite && duration > 0
                ? startTime + duration
                : nil

            // Extract title from metadata items
            let title = extractTitle(from: group.items)

            let disposition = classifier.classify(title)
            let quality = scorer.score(
                title: title,
                disposition: disposition,
                hasEndTime: endTime != nil,
                source: .id3
            )

            evidence.append(ChapterEvidence(
                startTime: startTime,
                endTime: endTime,
                title: title,
                source: .id3,
                disposition: disposition,
                qualityScore: quality
            ))
        }

        logger.debug("Parsed \(evidence.count) ID3 chapters (\(evidence.filter { $0.disposition == .adBreak }.count) adBreak)")

        return evidence.sorted { $0.startTime < $1.startTime }
    }

    /// Extract the title string from AVMetadataItem array.
    private static func extractTitle(from items: [AVMetadataItem]) -> String? {
        // Try common title keys
        let titleItem = AVMetadataItem.metadataItems(
            from: items,
            filteredByIdentifier: .commonIdentifierTitle
        ).first

        if let titleItem {
            if let stringValue = titleItem.stringValue, !stringValue.isEmpty {
                return stringValue
            }
        }

        // Fallback: check items with known title-like keys only.
        // Avoid returning non-title metadata (image URLs, ISRC codes, etc.)
        // that could cause false-positive ad classifications.
        let titleKeys: Set<AVMetadataKey> = [
            .commonKeyTitle,
            .id3MetadataKeyTitleDescription,
        ]
        for item in items {
            if let key = item.commonKey, titleKeys.contains(key),
               let stringValue = item.stringValue, !stringValue.isEmpty {
                return stringValue
            }
        }

        return nil
    }

    // MARK: - Podcasting 2.0 JSON Parsing

    /// Podcasting 2.0 chapters JSON schema (https://github.com/Podcastindex-org/podcast-namespace/blob/main/chapters/jsonChapters.md)
    struct PC20ChaptersPayload: Decodable, Sendable {
        let version: String?
        let chapters: [PC20Chapter]
    }

    struct PC20Chapter: Decodable, Sendable {
        let startTime: Double
        let endTime: Double?
        let title: String?
        let img: String?
        let url: String?
        let toc: Bool?
    }

    /// Fetch and parse Podcasting 2.0 chapters JSON from a URL.
    ///
    /// - Parameters:
    ///   - url: The chapters JSON URL (from `<podcast:chapters>` tag).
    ///   - session: URLSession to use for the fetch. Defaults to `.shared`.
    /// - Returns: Classified chapter evidence, sorted by start time.
    /// - Note: Non-blocking. Network errors result in an empty array (logged, not thrown).
    static func parsePodcasting20Chapters(
        from url: URL,
        session: URLSession = .shared
    ) async -> [ChapterEvidence] {
        let data: Data
        do {
            let (responseData, response) = try await session.data(from: url)
            guard let httpResponse = response as? HTTPURLResponse,
                  (200..<300).contains(httpResponse.statusCode) else {
                logger.debug("PC20 chapters fetch failed: non-2xx status from \(url.absoluteString)")
                return []
            }
            data = responseData
        } catch {
            logger.debug("PC20 chapters fetch error: \(error.localizedDescription)")
            return []
        }

        return decodePC20ChaptersJSON(data)
    }

    /// Decode Podcasting 2.0 chapters from raw JSON data.
    ///
    /// Exposed separately from `parsePodcasting20Chapters(from:)` to allow
    /// unit testing without network calls.
    static func decodePC20ChaptersJSON(_ data: Data) -> [ChapterEvidence] {
        let payload: PC20ChaptersPayload
        do {
            payload = try JSONDecoder().decode(PC20ChaptersPayload.self, from: data)
        } catch {
            logger.debug("PC20 chapters JSON decode error: \(error.localizedDescription)")
            return []
        }

        var evidence: [ChapterEvidence] = []

        for chapter in payload.chapters {
            // Skip non-TOC chapters (toc: false means "not in table of contents")
            if chapter.toc == false { continue }

            // Validate time values: skip malformed entries
            guard chapter.startTime.isFinite, chapter.startTime >= 0 else { continue }
            if let end = chapter.endTime {
                guard end.isFinite, end > chapter.startTime else { continue }
            }

            let disposition = classifier.classify(chapter.title)
            let quality = scorer.score(
                title: chapter.title,
                disposition: disposition,
                hasEndTime: chapter.endTime != nil,
                source: .pc20
            )

            evidence.append(ChapterEvidence(
                startTime: chapter.startTime,
                endTime: chapter.endTime,
                title: chapter.title,
                source: .pc20,
                disposition: disposition,
                qualityScore: quality
            ))
        }

        logger.debug("Parsed \(evidence.count) PC20 chapters (\(evidence.filter { $0.disposition == .adBreak }.count) adBreak)")

        return evidence.sorted { $0.startTime < $1.startTime }
    }

    // MARK: - RSS Inline Chapter Conversion

    /// Convert already-parsed RSS inline chapters (`ParsedChapter`) to evidence.
    ///
    /// Uses episode duration to infer end times for the last chapter if needed.
    ///
    /// - Parameters:
    ///   - chapters: Inline chapters from RSS `<podcast:chapter>` elements.
    ///   - episodeDuration: Total episode duration for end-time inference. `nil` if unknown.
    /// - Returns: Classified chapter evidence, sorted by start time.
    static func fromParsedChapters(
        _ chapters: [ParsedChapter],
        episodeDuration: TimeInterval? = nil
    ) -> [ChapterEvidence] {
        guard !chapters.isEmpty else { return [] }

        // Filter out malformed entries before sorting
        let valid = chapters.filter { $0.startTime.isFinite && $0.startTime >= 0 }
        let sorted = valid.sorted { $0.startTime < $1.startTime }
        var evidence: [ChapterEvidence] = []

        for (index, chapter) in sorted.enumerated() {
            // Infer end time from next chapter's start, or episode duration for last chapter
            let endTime: TimeInterval?
            if index + 1 < sorted.count {
                endTime = sorted[index + 1].startTime
            } else if let duration = episodeDuration, duration.isFinite, duration > 0 {
                endTime = duration
            } else {
                endTime = nil
            }

            let disposition = classifier.classify(chapter.title)
            let quality = scorer.score(
                title: chapter.title,
                disposition: disposition,
                hasEndTime: endTime != nil,
                source: .rssInline
            )

            evidence.append(ChapterEvidence(
                startTime: chapter.startTime,
                endTime: endTime,
                title: chapter.title,
                source: .rssInline,
                disposition: disposition,
                qualityScore: quality
            ))
        }

        return evidence
    }
}
