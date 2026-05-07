// DogfoodAnalysisHealthFixtureLoader.swift
// playhead-hygc.1.1: sanitized regression fixture for the May 6, 2026 dogfood
// export. Loads the scrubbed `analysis-health.json` snapshot used by Activity /
// diagnostics tests and downstream NARL / correction tests (hygc.1.2..1.9).
//
// The raw .xcappdata bundle and the raw dogfood-diagnostics JSON live ON-DEVICE
// only and MUST NOT be checked in. Regenerate the fixture with:
//
//     python3 scripts/build-dogfood-fixture-2026-05-06.py
//
// (See PlayheadTests/Fixtures/Dogfood/2026-05-06/README.md for the full
//  source-file inventory and scrubbing rules.)
//
// Path anchoring follows the project convention used by
// `FixtureManifestIntegrityTests`: we resolve fixtures via `#filePath` so the
// fixture directory does not need to be a Resources build phase entry.

import Foundation

// MARK: - Fixture model

/// One Activity-snapshot row — the per-episode strip the user sees in the
/// Activity surface. All identifiers are stable synthetic strings; the
/// original `episode_id_hash` was scrubbed.
struct DogfoodAnalysisHealthFixture: Codable, Equatable {

    let schemaVersion: Int
    let capturedOn: String
    let sourceDiagnosticsFilename: String
    let activitySnapshot: ActivitySnapshot
    let analysisAssets: [AnalysisAsset]
    let transcriptChunkMaxima: [TranscriptChunkMax]
    let adWindowSummaries: [AdWindowSummary]
    let correctionRows: [CorrectionRow]
    let backgroundTaskEvents: BackgroundTaskEvents
    let learningTableCounts: LearningTableCounts
    let shadowFmResponseCount: Int

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case capturedOn = "captured_on"
        case sourceDiagnosticsFilename = "source_diagnostics_filename"
        case activitySnapshot = "activity_snapshot"
        case analysisAssets = "analysis_assets"
        case transcriptChunkMaxima = "transcript_chunk_maxima"
        case adWindowSummaries = "ad_window_summaries"
        case correctionRows = "correction_rows"
        case backgroundTaskEvents = "background_task_events"
        case learningTableCounts = "learning_table_counts"
        case shadowFmResponseCount = "shadow_fm_response_count"
    }

    struct ActivitySnapshot: Codable, Equatable {
        let rowCount: Int
        let rows: [Row]

        enum CodingKeys: String, CodingKey {
            case rowCount = "row_count"
            case rows
        }

        struct Row: Codable, Equatable {
            let id: String
            let section: String
            let queuePosition: Int?
            let isRunning: Bool
            let cachedAudioPresent: Bool
            let status: Status
            let analysisState: String?
            let pipeline: Pipeline

            enum CodingKeys: String, CodingKey {
                case id
                case section
                case queuePosition = "queue_position"
                case isRunning = "is_running"
                case cachedAudioPresent = "cached_audio_present"
                case status
                case analysisState = "analysis_state"
                case pipeline
            }
        }

        struct Status: Codable, Equatable {
            let disposition: String?
            let reason: String?
            let hint: String?
            let playbackReadiness: String?

            enum CodingKeys: String, CodingKey {
                case disposition
                case reason
                case hint
                case playbackReadiness = "playback_readiness"
            }
        }

        struct Pipeline: Codable, Equatable {
            let downloadFraction: Double?
            let downloadPercent: String?
            let downloadSource: String?
            let analysisFraction: Double?
            let analysisPercent: String?
            let analysisSource: String?
            let analysisWatermarkSec: Double?
            let transcriptFraction: Double?
            let transcriptPercent: String?
            let transcriptSource: String?
            let transcriptWatermarkSec: Double?
            let transcriptCoveredSec: Double?
            let fastTranscriptWatermarkSec: Double?
            let featureCoverageEndSec: Double?
            let finalPassCoverageEndSec: Double?
            let episodeDurationSec: Double?

            enum CodingKeys: String, CodingKey {
                case downloadFraction = "download_fraction"
                case downloadPercent = "download_percent"
                case downloadSource = "download_source"
                case analysisFraction = "analysis_fraction"
                case analysisPercent = "analysis_percent"
                case analysisSource = "analysis_source"
                case analysisWatermarkSec = "analysis_watermark_sec"
                case transcriptFraction = "transcript_fraction"
                case transcriptPercent = "transcript_percent"
                case transcriptSource = "transcript_source"
                case transcriptWatermarkSec = "transcript_watermark_sec"
                case transcriptCoveredSec = "transcript_covered_sec"
                case fastTranscriptWatermarkSec = "fast_transcript_watermark_sec"
                case featureCoverageEndSec = "feature_coverage_end_sec"
                case finalPassCoverageEndSec = "final_pass_coverage_end_sec"
                case episodeDurationSec = "episode_duration_sec"
            }
        }
    }

    struct AnalysisAsset: Codable, Equatable {
        let id: String
        let analysisState: String
        let episodeDurationSec: Double?
        let fastTranscriptCoverageEndSec: Double?
        let featureCoverageEndSec: Double?
        let finalPassCoverageEndSec: Double?
        let confirmedAdCoverageEndSec: Double?
        let terminalReason: String?

        enum CodingKeys: String, CodingKey {
            case id
            case analysisState = "analysis_state"
            case episodeDurationSec = "episode_duration_sec"
            case fastTranscriptCoverageEndSec = "fast_transcript_coverage_end_sec"
            case featureCoverageEndSec = "feature_coverage_end_sec"
            case finalPassCoverageEndSec = "final_pass_coverage_end_sec"
            case confirmedAdCoverageEndSec = "confirmed_ad_coverage_end_sec"
            case terminalReason = "terminal_reason"
        }
    }

    struct TranscriptChunkMax: Codable, Equatable {
        let assetId: String
        let pass: String
        let maxEndTimeSec: Double
        let chunkCount: Int

        enum CodingKeys: String, CodingKey {
            case assetId = "asset_id"
            case pass
            case maxEndTimeSec = "max_end_time_sec"
            case chunkCount = "chunk_count"
        }
    }

    struct AdWindowSummary: Codable, Equatable {
        let assetId: String
        let totalCount: Int
        let userMarkedCount: Int
        let algorithmicCount: Int
        let maxEndTimeSec: Double?

        enum CodingKeys: String, CodingKey {
            case assetId = "asset_id"
            case totalCount = "total_count"
            case userMarkedCount = "user_marked_count"
            case algorithmicCount = "algorithmic_count"
            case maxEndTimeSec = "max_end_time_sec"
        }
    }

    struct CorrectionRow: Codable, Equatable {
        let correctionType: String
        let scope: String
        let count: Int

        enum CodingKeys: String, CodingKey {
            case correctionType = "correction_type"
            case scope
            case count
        }
    }

    struct BackgroundTaskEvents: Codable, Equatable {
        let overall: [String: Int]
        let byCategory: [String: [String: Int]]

        enum CodingKeys: String, CodingKey {
            case overall
            case byCategory = "by_category"
        }
    }

    struct LearningTableCounts: Codable, Equatable {
        let sponsorKnowledgeEntries: Int
        let trainingExamples: Int
        let adCopyFingerprints: Int
        let boundaryPriors: Int
        let implicitFeedbackEvents: Int
        let knowledgeCandidateEvents: Int
        let musicBracketTrust: Int

        enum CodingKeys: String, CodingKey {
            case sponsorKnowledgeEntries = "sponsor_knowledge_entries"
            case trainingExamples = "training_examples"
            case adCopyFingerprints = "ad_copy_fingerprints"
            case boundaryPriors = "boundary_priors"
            case implicitFeedbackEvents = "implicit_feedback_events"
            case knowledgeCandidateEvents = "knowledge_candidate_events"
            case musicBracketTrust = "music_bracket_trust"
        }
    }
}

// MARK: - Loader

enum DogfoodAnalysisHealthFixtureLoaderError: Error, CustomStringConvertible {
    case fixtureNotFound(URL)
    case decodeFailed(URL, Error)

    var description: String {
        switch self {
        case .fixtureNotFound(let url):
            "analysis-health.json not found at \(url.path)"
        case .decodeFailed(let url, let err):
            "Failed to decode analysis-health.json at \(url.path): \(err.localizedDescription)"
        }
    }
}

/// Stateless loader for the sanitized 2026-05-06 dogfood fixture. Reusable from
/// any test suite — no dependency on the live `.xcappdata` bundle.
enum DogfoodAnalysisHealthFixtureLoader {

    /// Capture date the fixture pertains to. Drives the on-disk subdir.
    static let captureDateStamp = "2026-05-06"

    /// Resolve the fixture directory from the source file path of this loader.
    /// Convention matches `FixtureManifestIntegrityTests` / `CorpusLoader`.
    static func fixtureDirectoryURL(filePath: String = #filePath) -> URL {
        URL(fileURLWithPath: filePath)
            .deletingLastPathComponent()                       // Fixtures/Dogfood/
            .appendingPathComponent(captureDateStamp, isDirectory: true)
    }

    /// Resolve the analysis-health.json URL.
    static func fixtureURL(filePath: String = #filePath) -> URL {
        fixtureDirectoryURL(filePath: filePath)
            .appendingPathComponent("analysis-health.json")
    }

    /// Load and decode the sanitized analysis-health snapshot.
    static func load(filePath: String = #filePath) throws -> DogfoodAnalysisHealthFixture {
        let url = fixtureURL(filePath: filePath)
        let data: Data
        do {
            data = try Data(contentsOf: url)
        } catch {
            throw DogfoodAnalysisHealthFixtureLoaderError.fixtureNotFound(url)
        }
        do {
            return try JSONDecoder().decode(DogfoodAnalysisHealthFixture.self, from: data)
        } catch {
            throw DogfoodAnalysisHealthFixtureLoaderError.decodeFailed(url, error)
        }
    }
}
