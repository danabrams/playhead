// Podcast.swift
// Core data models: Podcast, Episode, and related types.
// Uses SwiftData for persistence.

import Foundation
import SwiftData

// MARK: - Podcast

@Model
final class Podcast {
    #Unique<Podcast>([\.feedURL])

    var feedURL: URL
    var title: String
    var author: String
    var artworkURL: URL?
    @Relationship(deleteRule: .cascade, inverse: \Episode.podcast)
    var episodes: [Episode]
    var subscribedAt: Date

    /// playhead-snp: per-show toggle for new-episode local notifications.
    /// Default ON (opt-out). When `false`, the new-episode notification
    /// scheduler skips this podcast entirely, regardless of the app-wide
    /// `UserPreferences.newEpisodeNotificationsEnabled` setting.
    /// Additive optional with a Swift default — existing rows decode
    /// with `true` so an upgrade preserves the spec'd opt-out behavior
    /// for already-subscribed shows.
    var notificationsEnabled: Bool = true

    init(
        feedURL: URL,
        title: String,
        author: String,
        artworkURL: URL? = nil,
        episodes: [Episode] = [],
        subscribedAt: Date = .now,
        notificationsEnabled: Bool = true
    ) {
        self.feedURL = feedURL
        self.title = title
        self.author = author
        self.artworkURL = artworkURL
        self.episodes = episodes
        self.subscribedAt = subscribedAt
        self.notificationsEnabled = notificationsEnabled
    }
}

// MARK: - Episode

@Model
final class Episode {
    #Unique<Episode>([\.canonicalEpisodeKey])

    var feedItemGUID: String
    var canonicalEpisodeKey: String
    var podcast: Podcast?
    var title: String
    var audioURL: URL
    var cachedAudioURL: URL?
    var downloadState: DownloadState
    var lastPlayedAnalysisAssetId: UUID?
    var analysisSummary: AnalysisSummary?
    var duration: TimeInterval?
    var publishedAt: Date?
    var playbackPosition: TimeInterval
    var isPlayed: Bool
    var feedMetadata: FeedDescriptionMetadata?

    /// Per-episode opt-in for the OptIn diagnostics bundle (playhead-ghon).
    /// Defaults to `false` so the additive SwiftData migration is
    /// non-destructive: existing rows decode with the property set to
    /// `false` and the OptIn bundle path remains a no-op until the user
    /// flips this flag through the Phase 2 Diagnostics screen
    /// (playhead-l274). Reset policy lives in
    /// ``DiagnosticsOptInResetPolicy`` — flag clears when the mail
    /// composer reports `.sent` or `.saved`, persists on `.cancelled`
    /// or `.failed`.
    var diagnosticsOptIn: Bool = false

    /// Phase 2 coverage record (playhead-cthe). JSON-encoded Codable
    /// field following the `analysisSummary` pattern so the additive
    /// SwiftData migration is non-destructive — existing rows decode
    /// with the property set to `nil` and the derivation pipeline
    /// returns `PlaybackReadiness.none` until the analysis pipeline
    /// (playhead-zp5y / playhead-quh7) starts writing records.
    ///
    /// Readiness is NEVER persisted — always re-derive via
    /// `derivePlaybackReadiness(coverage:anchor:)` so multiple UI
    /// surfaces at different anchors cannot diverge.
    var coverageSummary: CoverageSummary?

    /// Phase 2 readiness anchor (playhead-cthe). The time (seconds from
    /// episode start) from which readiness should be evaluated. Updated
    /// at the existing play-loop commit points (`PlayheadApp
    /// .persistPlaybackPosition`) alongside `playbackPosition`, so a
    /// force-quit mid-playback preserves the last persisted anchor as
    /// the spec requires ("on force-quit mid-playback, last persisted
    /// commit wins").
    ///
    /// Kept distinct from `playbackPosition` so a future scope can
    /// decouple "where the user is listening" from "where readiness is
    /// evaluated". For now the two are updated together.
    var playbackAnchor: TimeInterval?

    /// Persisted user ordering for the Activity screen's Up Next section
    /// (playhead-cjqq). `nil` means the user has never reordered this
    /// episode — it inherits the production provider's natural
    /// (scheduler-derived) ordering. After a drag, the visible Up Next
    /// rows are renumbered sequentially (0, 1, 2, …) so subsequent
    /// reorders compose deterministically and the Int domain cannot
    /// overflow.
    ///
    /// Sort rule used by `LiveActivitySnapshotProvider`:
    ///   `(queuePosition asc, nil-last, canonicalEpisodeKey tiebreak)`
    ///
    /// Additive optional field — defaults to `nil` so existing rows
    /// decode under the V1 schema without a migration stage. Do NOT
    /// promote to non-optional (would break decode of pre-cjqq rows).
    var queuePosition: Int?

    init(
        feedItemGUID: String,
        feedURL: URL,
        podcast: Podcast? = nil,
        title: String,
        audioURL: URL,
        cachedAudioURL: URL? = nil,
        downloadState: DownloadState = .notDownloaded,
        lastPlayedAnalysisAssetId: UUID? = nil,
        analysisSummary: AnalysisSummary? = nil,
        duration: TimeInterval? = nil,
        publishedAt: Date? = nil,
        playbackPosition: TimeInterval = 0,
        isPlayed: Bool = false,
        feedMetadata: FeedDescriptionMetadata? = nil,
        diagnosticsOptIn: Bool = false,
        coverageSummary: CoverageSummary? = nil,
        playbackAnchor: TimeInterval? = nil,
        queuePosition: Int? = nil
    ) {
        self.feedItemGUID = feedItemGUID
        self.canonicalEpisodeKey = Self.makeCanonicalKey(
            feedItemGUID: feedItemGUID, feedURL: feedURL
        )
        self.podcast = podcast
        self.title = title
        self.audioURL = audioURL
        self.cachedAudioURL = cachedAudioURL
        self.downloadState = downloadState
        self.lastPlayedAnalysisAssetId = lastPlayedAnalysisAssetId
        self.analysisSummary = analysisSummary
        self.duration = duration
        self.publishedAt = publishedAt
        self.playbackPosition = playbackPosition
        self.isPlayed = isPlayed
        self.feedMetadata = feedMetadata
        self.diagnosticsOptIn = diagnosticsOptIn
        self.coverageSummary = coverageSummary
        self.playbackAnchor = playbackAnchor
        self.queuePosition = queuePosition
    }

    /// Derives the canonical key from feedItemGUID + feedURL for preview budget tracking.
    static func makeCanonicalKey(feedItemGUID: String, feedURL: URL) -> String {
        "\(feedURL.absoluteString)::\(feedItemGUID)"
    }
}

// MARK: - DownloadState

enum DownloadState: Int, Codable, Sendable {
    case notDownloaded
    case downloading
    case downloaded
    case failed
}

// MARK: - AnalysisSummary

/// Denormalized struct so the UI never needs to query the SQLite analysis store.
struct AnalysisSummary: Codable, Sendable, Equatable {
    var hasAnalysis: Bool
    var adSegmentCount: Int
    var totalAdDuration: TimeInterval
    var lastAnalyzedAt: Date?
}

// MARK: - FeedDescriptionMetadata

/// Shadow-mode metadata from RSS description/summary fields.
/// Normalized text + source hashes for rebuild detection.
/// These fields are persisted but do not influence any live decisions.
struct FeedDescriptionMetadata: Codable, Sendable, Equatable {
    /// RSS `<description>` — HTML stripped, entities decoded, truncated.
    var feedDescription: String?
    /// iTunes `<itunes:summary>` or `<content:encoded>` — normalized.
    var feedSummary: String?
    /// Hashes of the raw source strings, enabling change detection without
    /// storing unbounded HTML blobs.
    var sourceHashes: SourceHashes
    /// playhead-gtt9.22: Chapter evidence parsed from RSS-feed inline
    /// `<podcast:chapter>` markers (zero-cost, deterministic). Optional
    /// so the additive Codable migration is non-destructive: existing
    /// rows decode with `nil` and the ad-detection pipeline simply sees
    /// no chapter-derived signal until the next feed refresh repopulates
    /// the field. Each entry already carries source/disposition/quality
    /// — the heavy lifting was done in `ChapterEvidenceParser`.
    /// Out of scope for this field: ID3 CHAP and Podcasting 2.0 JSON
    /// chapters (those are non-RSS sources; future beads can extend).
    var chapterEvidence: [ChapterEvidence]?
    /// playhead-gtt9.22: URL of the optional Podcasting 2.0
    /// `<podcast:chapters>` external JSON document. Captured at parse
    /// time so a future opt-in fetch path can hydrate it on demand. The
    /// parser does NOT fetch the URL (no new network calls during
    /// XMLParse); fetch is mediated by `ChapterEvidenceParser
    /// .parsePodcasting20Chapters(from:)` only when the runtime opts in.
    var chaptersFeedURL: URL?

    struct SourceHashes: Codable, Sendable, Equatable, Hashable {
        // Stored as Int64 (raw bit-pattern of the original FNV-1a 64-bit
        // UInt64). SwiftData persists `feedMetadata` as a Codable blob and
        // the NSNumber bridge traps on UInt64 values > Int64.max when the
        // getter deserializes — see the "feedMetadata UInt64 bridge" crash.
        // Consumers treat these as opaque equality tokens, so a bit-cast
        // preserves identity: equal UInt64s bit-cast to equal Int64s.
        var descriptionHash: Int64?
        var summaryHash: Int64?

        init(descriptionHash: Int64? = nil, summaryHash: Int64? = nil) {
            self.descriptionHash = descriptionHash
            self.summaryHash = summaryHash
        }

        // Custom Codable with legacy-row migration.
        //
        // Decode strategy:
        //   - New writes: Int64 numeric token — decode directly.
        //   - Legacy rows that wrote UInt64 and whose value was <= Int64.max:
        //     the JSON/plist number round-trips cleanly into an Int64 of the
        //     same numeric value; same bit pattern in that range.
        //   - Legacy rows whose UInt64 was > Int64.max: decoding as Int64
        //     overflows and throws. We swallow that error and store nil —
        //     the row simply loses its hash and gets re-synced on the next
        //     feed fetch. No user-facing data is lost.
        //   - Missing key: nil.
        //
        // Encode strategy: plain Int64 (or nil as absent key).

        private enum CodingKeys: String, CodingKey {
            case descriptionHash
            case summaryHash
        }

        init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            // `try?` collapses both "key missing" and "number overflows Int64"
            // into nil. The latter is the legacy-UInt64-with-high-bit case.
            self.descriptionHash = try? container.decodeIfPresent(Int64.self, forKey: .descriptionHash)
            self.summaryHash = try? container.decodeIfPresent(Int64.self, forKey: .summaryHash)
        }

        func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            try container.encodeIfPresent(descriptionHash, forKey: .descriptionHash)
            try container.encodeIfPresent(summaryHash, forKey: .summaryHash)
        }
    }
}
