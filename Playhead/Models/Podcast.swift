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

    init(
        feedURL: URL,
        title: String,
        author: String,
        artworkURL: URL? = nil,
        episodes: [Episode] = [],
        subscribedAt: Date = .now
    ) {
        self.feedURL = feedURL
        self.title = title
        self.author = author
        self.artworkURL = artworkURL
        self.episodes = episodes
        self.subscribedAt = subscribedAt
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
        feedMetadata: FeedDescriptionMetadata? = nil
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

    struct SourceHashes: Codable, Sendable, Equatable, Hashable {
        var descriptionHash: UInt64?
        var summaryHash: UInt64?
    }
}
