// CorpusAnnotation.swift
// Schema for the labeled test corpus introduced by playhead-l2f.
//
// This is a NEW corpus format, distinct from the legacy
// `TestEpisodeAnnotation` used by the synthetic replay simulator.
// Annotations live on disk as JSON in `TestFixtures/Corpus/Annotations/`
// alongside their referenced audio files in `TestFixtures/Corpus/Audio/`.
//
// JSON shape mirrors the bead-l2f specification exactly:
//
// {
//   "episode_id": "corpus-001",
//   "show_name": "Example Podcast",
//   "duration_seconds": 3600,
//   "ad_windows": [
//     {
//       "start_seconds": 180.0,
//       "end_seconds": 240.0,
//       "advertiser": "Squarespace",
//       "product": "Website builder",
//       "ad_type": "host_read",
//       "transition_type": "explicit",
//       "confidence_notes": "Clear brought to you by intro"
//     }
//   ],
//   "content_windows": [
//     {
//       "start_seconds": 0.0,
//       "end_seconds": 180.0,
//       "notes": "Interview content — must NEVER be skipped"
//     }
//   ],
//   "variant_of": null,
//   "audio_fingerprint": "sha256:abc123..."
// }

import Foundation

// MARK: - CorpusAnnotation

/// Ground-truth annotation for a single test episode.
///
/// Decoded from a JSON file in `TestFixtures/Corpus/Annotations/`.
/// Snake-case JSON keys are mapped to camel-case Swift properties via
/// `JSONDecoder.KeyDecodingStrategy.convertFromSnakeCase` (see
/// ``CorpusAnnotation/decoder``).
struct CorpusAnnotation: Sendable, Codable, Equatable {
    /// Stable identifier for this episode (e.g. "corpus-001").
    let episodeId: String
    /// Human-readable show name (e.g. "Diary of a CEO").
    let showName: String
    /// Total episode duration in seconds.
    let durationSeconds: Double
    /// All labeled ad windows. Must not overlap each other or any content window.
    let adWindows: [AdWindow]
    /// All labeled content windows. Must partition the timeline together with `adWindows`.
    let contentWindows: [ContentWindow]
    /// If this is a dynamic-ad-insertion variant, the `episode_id` it varies from.
    let variantOf: String?
    /// SHA-256 hash of the referenced audio file, prefixed with `sha256:`.
    let audioFingerprint: String

    // MARK: AdWindow

    /// A labeled ad window with full metadata.
    struct AdWindow: Sendable, Codable, Equatable {
        /// Window start in seconds, ±0.5s precision.
        let startSeconds: Double
        /// Window end in seconds, ±0.5s precision.
        let endSeconds: Double
        /// Advertiser name (e.g. "Squarespace"). Optional for blended ads
        /// where the brand is implicit.
        let advertiser: String?
        /// Product mentioned (e.g. "Website builder").
        let product: String?
        /// How the ad is delivered.
        let adType: AdType
        /// How the transition into the ad is signalled.
        let transitionType: TransitionType
        /// Free-form annotator notes about why this confidence was assigned.
        let confidenceNotes: String?

        /// Window length in seconds.
        var durationSeconds: Double { endSeconds - startSeconds }
    }

    enum AdType: String, Sendable, Codable, Equatable {
        case hostRead = "host_read"
        case dynamicInsertion = "dynamic_insertion"
        case blendedHostRead = "blended_host_read"
        case producedSegment = "produced_segment"
        case promo = "promo"
    }

    enum TransitionType: String, Sendable, Codable, Equatable {
        /// Clear marker like "and now a word from our sponsor".
        case explicit
        /// Music or stinger, no spoken cue.
        case musical
        /// Direct cut with no signpost.
        case hardCut = "hard_cut"
        /// Host weaves the sponsor mention in mid-thought.
        case blended
    }

    // MARK: ContentWindow

    /// A labeled content window. The simulator must never skip into these.
    struct ContentWindow: Sendable, Codable, Equatable {
        let startSeconds: Double
        let endSeconds: Double
        /// Free-form notes for the annotator, e.g. "Interview content — must NEVER be skipped".
        let notes: String?

        var durationSeconds: Double { endSeconds - startSeconds }
    }

    // MARK: - Decoder / Encoder

    /// Shared JSON decoder configured for the corpus snake-case format.
    static var decoder: JSONDecoder {
        let d = JSONDecoder()
        d.keyDecodingStrategy = .convertFromSnakeCase
        return d
    }

    /// Shared JSON encoder. Used by tests and tooling that round-trip
    /// annotations.
    static var encoder: JSONEncoder {
        let e = JSONEncoder()
        e.keyEncodingStrategy = .convertToSnakeCase
        e.outputFormatting = [.prettyPrinted, .sortedKeys]
        return e
    }
}
