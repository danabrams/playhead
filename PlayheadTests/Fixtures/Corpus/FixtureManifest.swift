// FixtureManifest.swift
// Schema types for the device-lab fixture substrate defined in bead
// playhead-ym57. Backs fixtures-manifest.json which lists the locked-core 8
// deterministic fixtures plus any rotating fixtures selected by
// scripts/rotate-fixtures.swift.
//
// See FIXTURES_LICENSING.md for licensing policy and synthetic-placeholder
// disclosure. See FixtureManifestIntegrityTests for the on-disk integrity
// gate that runs in PlayheadFastTests.

import Foundation

// MARK: - Fixture Manifest

/// Top-level manifest describing a versioned set of device-lab fixtures.
struct FixtureManifest: Sendable, Codable, Equatable {
    let version: Int
    let fixtures: [FixtureDescriptor]

    static let currentVersion = 1
}

// MARK: - Fixture Descriptor

/// A single fixture entry in fixtures-manifest.json. `file` is relative to
/// PlayheadTests/Fixtures/Corpus/ (e.g. "Media/fixture-01-....wav").
struct FixtureDescriptor: Sendable, Codable, Equatable {

    /// Stable fixture ID (e.g. "fixture-01-30min-clean-speech").
    let id: String
    /// Relative path to the media file under Corpus/.
    let file: String
    /// SHA-256 of the file's raw bytes, 64 lowercase hex chars.
    let sha256: String
    /// Intended (taxonomic) duration of the fixture, in seconds. For
    /// synthetic-placeholder fixtures this describes the taxonomy axis, not
    /// the actual file duration; see `syntheticDurationSec` for the latter.
    let durationSec: Double
    /// The taxonomy axes this fixture covers.
    let taxonomy: FixtureTaxonomy
    /// Whether this fixture is part of the locked-core 8 (deterministic
    /// regression gate). Rotating fixtures set this to false.
    let locked: Bool
    /// Locked-core slot number in [1, 8] when `locked == true`, otherwise 0.
    let slot: Int
    /// Licensing anchor into FIXTURES_LICENSING.md (form "LICENSING.md#fixture-01").
    let licensingRef: String
    /// When set, records the actual on-disk duration of a synthetic placeholder
    /// (which is always much shorter than the taxonomic `durationSec`).
    let syntheticDurationSec: Double?
    /// When true, this fixture is a synthetic byte-deterministic placeholder
    /// produced by `SyntheticFixtureGenerator`. Marks the ym57 fallback posture.
    let synthetic: Bool
}

// MARK: - Taxonomy

/// Taxonomy axes for a fixture. Enum raw values match the bead spec strings.
struct FixtureTaxonomy: Sendable, Codable, Equatable {
    let durationBucket: DurationBucket
    let chapterRichness: ChapterRichness
    let adDensity: AdDensity
    let adPlacement: AdPlacement
    /// BCP-47 language tag (e.g. "en-US", "zh-Hans").
    let language: String
    let audioStructure: AudioStructure
    let dynamicInsertion: Bool

    enum DurationBucket: String, Sendable, Codable, Equatable {
        case m15 = "15m", m30 = "30m", m45 = "45m", m60 = "60m", m90 = "90m"
    }

    enum ChapterRichness: String, Sendable, Codable, Equatable {
        case none, sparse, rich
    }

    enum AdDensity: String, Sendable, Codable, Equatable {
        case none, low, medium, high
    }

    enum AdPlacement: String, Sendable, Codable, Equatable {
        case none
        case preRoll = "pre-roll"
        case midRoll = "mid-roll"
        case postRoll = "post-roll"
        case mixed
    }

    enum AudioStructure: String, Sendable, Codable, Equatable {
        case cleanSpeech = "clean-speech"
        case musicBed = "music-bed"
        case poorRemote = "poor-remote"
        case mixed
    }
}
