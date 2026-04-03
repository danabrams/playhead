// CorpusAnnotationTypes.swift
// Schema types for the labeled test corpus. Defines the annotation format
// for episodes with ground-truth sponsor boundaries and metadata.

import Foundation

// MARK: - Test Episode Annotation

/// Complete annotation for a single test episode, including all labeled
/// ad segments and episode metadata.
struct TestEpisodeAnnotation: Sendable, Codable, Equatable {
    /// Unique identifier for this annotation.
    let annotationId: String
    /// Reference to the audio file (relative path within Fixtures/Corpus/Audio/).
    let audioFileReference: String
    /// Podcast-level metadata.
    let podcast: TestPodcastMetadata
    /// Episode-level metadata.
    let episode: TestEpisodeMetadata
    /// All labeled ad segments in this episode.
    let adSegments: [TestAdSegment]
    /// Whether this episode has no ads at all (for false-positive testing).
    let isNoAdEpisode: Bool
    /// Edge case tags for filtering.
    let tags: [String]
    /// Annotation schema version for forward compatibility.
    let schemaVersion: Int
    /// Who created this annotation.
    let annotatedBy: String
    /// When the annotation was last updated.
    let lastUpdated: String

    static let currentSchemaVersion = 1
}

// MARK: - Test Podcast Metadata

struct TestPodcastMetadata: Sendable, Codable, Equatable {
    let podcastId: String
    let title: String
    let author: String
    let genre: String
    /// Whether this podcast uses dynamic ad insertion.
    let usesDynamicAdInsertion: Bool
}

// MARK: - Test Episode Metadata

struct TestEpisodeMetadata: Sendable, Codable, Equatable {
    let episodeId: String
    let title: String
    let duration: TimeInterval
    let publishedAt: String
    let feedURL: String
    let audioURL: String
}

// MARK: - Test Ad Segment

/// A single labeled ad segment within an episode.
struct TestAdSegment: Sendable, Codable, Equatable {
    /// Start time of the ad in seconds.
    let startTime: Double
    /// End time of the ad in seconds.
    let endTime: Double
    /// Advertiser name.
    let advertiser: String?
    /// Product being advertised.
    let product: String?
    /// Type of ad placement.
    let adType: AdType
    /// How the ad is read/delivered.
    let deliveryStyle: DeliveryStyle
    /// Difficulty level for detection.
    let difficulty: Difficulty
    /// Free-form notes about this segment.
    let notes: String?

    /// Duration in seconds.
    var duration: Double { endTime - startTime }

    enum AdType: String, Sendable, Codable, Equatable {
        case preRoll
        case midRoll
        case postRoll
    }

    enum DeliveryStyle: String, Sendable, Codable, Equatable {
        /// Pre-produced ad inserted dynamically.
        case dynamicInsertion
        /// Host reads an ad script.
        case hostRead
        /// Host weaves the ad into conversation (hardest to detect).
        case blendedHostRead
        /// Pre-produced jingle/segment.
        case producedSegment
    }

    enum Difficulty: String, Sendable, Codable, Equatable {
        /// Clear ad markers, distinct voice/music.
        case easy
        /// Some markers, but boundaries are fuzzy.
        case medium
        /// Blended into content, no clear markers.
        case hard
    }
}

// MARK: - Dynamic Ad Variant Annotation

/// Describes an alternative ad that can appear in the same slot via DAI.
struct TestDynamicAdVariant: Sendable, Codable, Equatable {
    let variantId: String
    /// Which segment index in the parent episode this replaces.
    let replacesSegmentIndex: Int
    /// The replacement ad segment.
    let segment: TestAdSegment
    /// Reference to variant audio file (if different from base).
    let audioFileReference: String?
}

// MARK: - Corpus Manifest

/// Top-level manifest listing all episodes in the test corpus.
struct CorpusManifest: Sendable, Codable {
    let corpusVersion: String
    let description: String
    let episodes: [CorpusEpisodeEntry]
    let lastUpdated: String

    struct CorpusEpisodeEntry: Sendable, Codable {
        let annotationFile: String
        let tags: [String]
    }
}
