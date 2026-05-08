// DiagnosticsExportService.swift
// Support-safe diagnostics bundle mail-composer + iPad activity fallback,
// plus coordinator seam protocols for the orchestrator layer.
//
// Scope: playhead-ghon. UI placement ships in Phase 2 playhead-l274.

import Foundation

#if canImport(UIKit)
import UIKit
#endif

#if canImport(MessageUI)
import MessageUI
#endif

// MARK: - Service

@MainActor
final class DiagnosticsExportService {

    static let attachmentMIMEType = "application/json"
    static let filenamePrefix = "playhead-diagnostics"

    static func defaultSubject(buildType: BuildType) -> String {
        "Playhead diagnostics (\(buildType.rawValue))"
    }

    /// `playhead-diagnostics-<ISO8601>.json`. ":" replaced with "-" for
    /// portability; documented-only relaxation from strict ISO-8601.
    static func filename(for date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        let stamp = formatter.string(from: date).replacingOccurrences(of: ":", with: "-")
        return "\(filenamePrefix)-\(stamp).json"
    }

    static func encode(_ bundle: DiagnosticsBundleFile) throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .prettyPrinted]
        encoder.dateEncodingStrategy = .iso8601
        return try encoder.encode(bundle)
    }

    #if canImport(MessageUI) && os(iOS)
    /// `@unknown default → .failed` so a future Apple case never
    /// silently clears `diagnosticsOptIn`.
    static func map(_ result: MFMailComposeResult) -> DiagnosticsMailComposeResult {
        switch result {
        case .cancelled: return .cancelled
        case .saved:     return .saved
        case .sent:      return .sent
        case .failed:    return .failed
        @unknown default: return .failed
        }
    }
    #endif

    // MARK: - Composer construction

    #if canImport(MessageUI) && os(iOS)
    /// Returns `nil` if `canSendMail()` is false; caller should fall
    /// back to `makeActivityFallback(fileURL:)`. Caller retains the
    /// delegate.
    static func makeMailComposer(
        data: Data,
        filename: String,
        subject: String,
        delegate: MFMailComposeViewControllerDelegate
    ) -> MFMailComposeViewController? {
        guard MFMailComposeViewController.canSendMail() else { return nil }
        let composer = MFMailComposeViewController()
        composer.mailComposeDelegate = delegate
        composer.setSubject(subject)
        composer.addAttachmentData(data, mimeType: attachmentMIMEType, fileName: filename)
        return composer
    }
    #endif

    // MARK: - iPad / no-mail fallback

    #if canImport(UIKit) && os(iOS)
    /// Excluded from the iPad / no-mail fallback. Per spec the fallback
    /// is mail-only — every other activity filtered out so the user
    /// cannot route diagnostics to AirDrop/Notes/Messages (support
    /// needs the email artifact). Surfaced so tests can assert.
    ///
    /// The list is intentionally limited to activities iOS still vends
    /// on current SDKs. Legacy social-share types (Facebook, Twitter,
    /// Weibo, TencentWeibo, Vimeo, Flickr, iBooks, MarkupAsPDF,
    /// SharePlay) were previously listed here but are no longer user-
    /// reachable system activities — Apple retired the underlying
    /// services or moved them behind feature flags. Excluding a
    /// non-present activity type is a harmless no-op, but carrying the
    /// dead names forward muddles the contract, so they're dropped.
    /// If any of them resurface as a distinct routing target in a
    /// future iOS, add them back here.
    static let mailOnlyFallbackExcludedActivities: [UIActivity.ActivityType] = [
        .addToReadingList,
        .airDrop,
        .assignToContact,
        .copyToPasteboard,
        .message,
        .print,
        .saveToCameraRoll
    ]

    /// Mail-only `UIActivityViewController`; callers own presentation.
    static func makeActivityFallback(fileURL: URL) -> UIActivityViewController {
        let controller = UIActivityViewController(
            activityItems: [fileURL],
            applicationActivities: nil
        )
        controller.excludedActivityTypes = mailOnlyFallbackExcludedActivities
        return controller
    }

    /// Writes bundle to tmp for the activity-controller fallback.
    /// Caller is responsible for cleanup.
    static func writeBundle(
        data: Data,
        filename: String,
        directory: URL = FileManager.default.temporaryDirectory
    ) throws -> URL {
        let url = directory.appendingPathComponent(filename, isDirectory: false)
        try data.write(to: url, options: [.atomic])
        return url
    }
    #endif
}

// MARK: - Coordinator seams

/// Abstract presenter for the diagnostics export UI. Production uses
/// `UIKitDiagnosticsPresenter`; tests inject a fake that completes
/// synchronously with a canned `DiagnosticsMailComposeResult`.
///
/// The presenter owns:
///   * Mail-vs-activity selection (iPhone mail composer vs iPad activity
///     fallback, or activity fallback when `canSendMail()` is false).
///   * Presentation on the host view controller.
///   * Delivering the final `DiagnosticsMailComposeResult` back to the
///     coordinator via the completion handler.
@MainActor
protocol DiagnosticsExportPresenter {
    func present(
        data: Data,
        filename: String,
        subject: String,
        completion: @escaping @MainActor (Result<DiagnosticsMailComposeResult, Error>) -> Void
    )
}

/// Async fetch closure that returns the most-recent WorkJournalEntry
/// rows (newest-first, bounded by `DiagnosticsBundleBuilder.schedulerEventsCap`).
/// Kept as a closure so the coordinator does not depend on
/// `AnalysisStore` directly — tests supply a canned list.
typealias DiagnosticsJournalFetch = @Sendable () async throws -> [WorkJournalEntry]

/// Async fetch closure for the chapter-phase events stream
/// (playhead-au2v.1.3). Mirrors `DiagnosticsJournalFetch` so the
/// coordinator can stay decoupled from whichever persistence layer the
/// chapter-phase consumers eventually land (likely AnalysisStore in a
/// later bead). Until those consumers ship, the production wiring uses
/// the default `{ [] }` closure and tests supply a canned list directly.
typealias DiagnosticsChapterPhaseEventsFetch = @Sendable () async throws -> [ChapterPhaseEvent]

/// Seam for flipping `Episode.diagnosticsOptIn = false` on the rows
/// that actually shipped in the bundle. Abstracted so the coordinator
/// remains pure-logic and the SwiftData/ModelContext dependency lives in
/// the production adapter (`SwiftDataDiagnosticsOptInSink`).
@MainActor
protocol DiagnosticsOptInSink {
    func applyResetToEpisodes(matchingEpisodeIds: [String], newValue: Bool)
}

struct DiagnosticsExportEnvironment: Sendable {
    let appVersion: String
    let osVersion: String
    let deviceClass: DeviceClass
    let buildType: BuildType
    let eligibility: AnalysisEligibility
    let installID: UUID
    let now: Date

    init(
        appVersion: String,
        osVersion: String,
        deviceClass: DeviceClass,
        buildType: BuildType,
        eligibility: AnalysisEligibility,
        installID: UUID,
        now: Date = .now
    ) {
        self.appVersion = appVersion
        self.osVersion = osVersion
        self.deviceClass = deviceClass
        self.buildType = buildType
        self.eligibility = eligibility
        self.installID = installID
        self.now = now
    }
}

// MARK: - Errors

/// Errors surfaced by the `DiagnosticsExportCoordinator`. Kept deliberately
/// narrow — builder/service errors propagate untransformed so callers can
/// distinguish "we never got to presentation" (this enum) from "the
/// composer failed" (a `DiagnosticsMailComposeResult.failed`).
enum DiagnosticsExportError: Error, Equatable {
    /// Coordinator was asked to present without a host view controller
    /// (e.g. the UIKit root was torn down mid-flow). Non-recoverable at
    /// the coordinator layer; UI should surface a retry prompt.
    case missingHostViewController
}

// MARK: - Dogfood Diagnostics Export

/// Single-file archive for the Phase 1.5 dogfood audit logs.
///
/// This deliberately carries only the support-safe surface-status JSONL logs
/// from `Caches/Diagnostics/`: no audio cache, no transcripts, no raw episode
/// IDs, and no install-ID salt. The archive is JSON so it can travel through
/// ShareLink/Mail as one small file and still be unpacked by local scripts
/// without an iOS zip dependency.
struct DogfoodDiagnosticsArchive: Codable, Sendable, Equatable {
    let schemaVersion: Int
    let generatedAt: Date
    let files: [DogfoodDiagnosticsArchiveFile]
    let activitySnapshot: DogfoodDiagnosticsActivitySnapshot?
    /// Phase 1.5 playhead-hygc.1.9: optional structured "why did
    /// this not progress?" summary. When non-nil the outer
    /// `schema_version` is bumped to 2; older v1 readers see the
    /// field as an unknown key and skip it. Older v1 archives still
    /// decode against this struct because `analysisHealth` is
    /// optional.
    let analysisHealth: DogfoodDiagnosticsAnalysisHealth?

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case generatedAt = "generated_at"
        case files
        case activitySnapshot = "activity_snapshot"
        case analysisHealth = "analysis_health"
    }

    init(
        schemaVersion: Int,
        generatedAt: Date,
        files: [DogfoodDiagnosticsArchiveFile],
        activitySnapshot: DogfoodDiagnosticsActivitySnapshot? = nil,
        analysisHealth: DogfoodDiagnosticsAnalysisHealth? = nil
    ) {
        self.schemaVersion = schemaVersion
        self.generatedAt = generatedAt
        self.files = files
        self.activitySnapshot = activitySnapshot
        self.analysisHealth = analysisHealth
    }
}

struct DogfoodDiagnosticsArchiveFile: Codable, Sendable, Equatable {
    let filename: String
    let role: String
    let byteCount: Int
    let content: String

    enum CodingKeys: String, CodingKey {
        case filename
        case role
        case byteCount = "byte_count"
        case content
    }
}

struct DogfoodDiagnosticsActivitySnapshot: Codable, Sendable, Equatable {
    let generatedAt: Date
    let rows: [DogfoodDiagnosticsActivityRow]
    let captureError: String?

    enum CodingKeys: String, CodingKey {
        case generatedAt = "generated_at"
        case rows
        case captureError = "capture_error"
    }

    init(
        generatedAt: Date,
        rows: [DogfoodDiagnosticsActivityRow],
        captureError: String? = nil
    ) {
        self.generatedAt = generatedAt
        self.rows = rows
        self.captureError = captureError
    }
}

struct DogfoodDiagnosticsActivityRow: Codable, Sendable, Equatable {
    let episodeIdHash: String
    let section: String
    let status: DogfoodDiagnosticsStatusSnapshot
    let isRunning: Bool
    let finishedOutcome: String?
    let queuePosition: Int?
    let cachedAudioPresent: Bool
    let liveDownloadFraction: Double?
    let pipeline: DogfoodDiagnosticsPipelineSnapshot
    let analysisAsset: DogfoodDiagnosticsAnalysisAssetSnapshot
    let latestSession: DogfoodDiagnosticsAnalysisSessionSnapshot?
    let latestJob: DogfoodDiagnosticsAnalysisJobSnapshot?
    let latestTerminalWorkJournal: DogfoodDiagnosticsWorkJournalSnapshot?

    enum CodingKeys: String, CodingKey {
        case episodeIdHash = "episode_id_hash"
        case section
        case status
        case isRunning = "is_running"
        case finishedOutcome = "finished_outcome"
        case queuePosition = "queue_position"
        case cachedAudioPresent = "cached_audio_present"
        case liveDownloadFraction = "live_download_fraction"
        case pipeline
        case analysisAsset = "analysis_asset"
        case latestSession = "latest_session"
        case latestJob = "latest_job"
        case latestTerminalWorkJournal = "latest_terminal_work_journal"
    }
}

struct DogfoodDiagnosticsStatusSnapshot: Codable, Sendable, Equatable {
    let disposition: String
    let reason: String
    let hint: String
    let analysisUnavailableReason: String?
    let playbackReadiness: String
    let readinessAnchor: Double?

    enum CodingKeys: String, CodingKey {
        case disposition
        case reason
        case hint
        case analysisUnavailableReason = "analysis_unavailable_reason"
        case playbackReadiness = "playback_readiness"
        case readinessAnchor = "readiness_anchor"
    }
}

struct DogfoodDiagnosticsPipelineSnapshot: Codable, Sendable, Equatable {
    let downloadFraction: Double?
    let downloadPercent: String
    let downloadSource: String
    let transcriptFraction: Double?
    let transcriptPercent: String
    let transcriptSource: String
    let analysisFraction: Double?
    let analysisPercent: String
    let analysisSource: String
    let episodeDurationSec: Double?
    let transcriptCoveredSec: Double?
    let transcriptWatermarkSec: Double?
    let fastTranscriptWatermarkSec: Double?
    let analysisWatermarkSec: Double?
    let featureCoverageEndSec: Double?
    let confirmedAdCoverageEndSec: Double?
    let finalPassCoverageEndSec: Double?
    /// playhead-hygc.1.2: provenance for `fastTranscriptWatermarkSec`
    /// (`fast_transcript_chunks` when sourced from `MAX(endTime)` of fast
    /// chunks, `asset_watermark` when sourced from
    /// `analysis_assets.fastTranscriptCoverageEndTime`, `unknown` when
    /// neither is recorded). Decouples the watermark wire from the stale-
    /// vs-fresh narrative on stuck-device diagnostics.
    let fastTranscriptCoverageEndSource: String
    /// playhead-hygc.1.2: provenance for `finalPassCoverageEndSec`
    /// (`final_pass_chunks`, `asset_watermark`, or `unknown`). Surfaces
    /// whether final-pass coverage came from chunk MAX(endTime) or from
    /// the asset watermark column — the bead's "final-pass coverage
    /// appearing in dogfood provenance" acceptance criterion.
    let finalPassCoverageEndSource: String

    enum CodingKeys: String, CodingKey {
        case downloadFraction = "download_fraction"
        case downloadPercent = "download_percent"
        case downloadSource = "download_source"
        case transcriptFraction = "transcript_fraction"
        case transcriptPercent = "transcript_percent"
        case transcriptSource = "transcript_source"
        case analysisFraction = "analysis_fraction"
        case analysisPercent = "analysis_percent"
        case analysisSource = "analysis_source"
        case episodeDurationSec = "episode_duration_sec"
        case transcriptCoveredSec = "transcript_covered_sec"
        case transcriptWatermarkSec = "transcript_watermark_sec"
        case fastTranscriptWatermarkSec = "fast_transcript_watermark_sec"
        case analysisWatermarkSec = "analysis_watermark_sec"
        case featureCoverageEndSec = "feature_coverage_end_sec"
        case confirmedAdCoverageEndSec = "confirmed_ad_coverage_end_sec"
        case finalPassCoverageEndSec = "final_pass_coverage_end_sec"
        case fastTranscriptCoverageEndSource = "fast_transcript_coverage_end_source"
        case finalPassCoverageEndSource = "final_pass_coverage_end_source"
    }

    init(
        downloadFraction: Double?,
        downloadPercent: String,
        downloadSource: String,
        transcriptFraction: Double?,
        transcriptPercent: String,
        transcriptSource: String,
        analysisFraction: Double?,
        analysisPercent: String,
        analysisSource: String,
        episodeDurationSec: Double?,
        transcriptCoveredSec: Double?,
        transcriptWatermarkSec: Double?,
        fastTranscriptWatermarkSec: Double?,
        analysisWatermarkSec: Double?,
        featureCoverageEndSec: Double?,
        confirmedAdCoverageEndSec: Double?,
        finalPassCoverageEndSec: Double?,
        // playhead-hygc.1.2: defaulted to `unknown` so the existing
        // call-sites in tests that constructed the snapshot positionally
        // pre-hygc.1.2 keep compiling. Production wires the real
        // provenance from `AnalysisCoverageSummary`.
        fastTranscriptCoverageEndSource: String = "unknown",
        finalPassCoverageEndSource: String = "unknown"
    ) {
        self.downloadFraction = downloadFraction
        self.downloadPercent = downloadPercent
        self.downloadSource = downloadSource
        self.transcriptFraction = transcriptFraction
        self.transcriptPercent = transcriptPercent
        self.transcriptSource = transcriptSource
        self.analysisFraction = analysisFraction
        self.analysisPercent = analysisPercent
        self.analysisSource = analysisSource
        self.episodeDurationSec = episodeDurationSec
        self.transcriptCoveredSec = transcriptCoveredSec
        self.transcriptWatermarkSec = transcriptWatermarkSec
        self.fastTranscriptWatermarkSec = fastTranscriptWatermarkSec
        self.analysisWatermarkSec = analysisWatermarkSec
        self.featureCoverageEndSec = featureCoverageEndSec
        self.confirmedAdCoverageEndSec = confirmedAdCoverageEndSec
        self.finalPassCoverageEndSec = finalPassCoverageEndSec
        self.fastTranscriptCoverageEndSource = fastTranscriptCoverageEndSource
        self.finalPassCoverageEndSource = finalPassCoverageEndSource
    }
}

struct DogfoodDiagnosticsAnalysisAssetSnapshot: Codable, Sendable, Equatable {
    let analysisState: String
    let analysisVersion: Int
    let artifactClass: String
    let terminalReason: String?
    let capabilitySnapshotPresent: Bool

    enum CodingKeys: String, CodingKey {
        case analysisState = "analysis_state"
        case analysisVersion = "analysis_version"
        case artifactClass = "artifact_class"
        case terminalReason = "terminal_reason"
        case capabilitySnapshotPresent = "capability_snapshot_present"
    }
}

struct DogfoodDiagnosticsAnalysisSessionSnapshot: Codable, Sendable, Equatable {
    let state: String
    let startedAt: Double
    let updatedAt: Double
    let failureReason: String?
    let needsShadowRetry: Bool

    enum CodingKeys: String, CodingKey {
        case state
        case startedAt = "started_at"
        case updatedAt = "updated_at"
        case failureReason = "failure_reason"
        case needsShadowRetry = "needs_shadow_retry"
    }
}

struct DogfoodDiagnosticsAnalysisJobSnapshot: Codable, Sendable, Equatable {
    let jobType: String
    let state: String
    let priority: Int
    let desiredCoverageSec: Double
    let featureCoverageSec: Double
    let transcriptCoverageSec: Double
    let cueCoverageSec: Double
    let attemptCount: Int
    let nextEligibleAt: Double?
    let leasePresent: Bool
    let leaseExpiresAt: Double?
    let lastErrorCode: String?
    let createdAt: Double
    let updatedAt: Double
    let generationID: String
    let schedulerEpoch: Int
    let artifactClass: String
    let estimatedWriteBytes: Int64

    enum CodingKeys: String, CodingKey {
        case jobType = "job_type"
        case state
        case priority
        case desiredCoverageSec = "desired_coverage_sec"
        case featureCoverageSec = "feature_coverage_sec"
        case transcriptCoverageSec = "transcript_coverage_sec"
        case cueCoverageSec = "cue_coverage_sec"
        case attemptCount = "attempt_count"
        case nextEligibleAt = "next_eligible_at"
        case leasePresent = "lease_present"
        case leaseExpiresAt = "lease_expires_at"
        case lastErrorCode = "last_error_code"
        case createdAt = "created_at"
        case updatedAt = "updated_at"
        case generationID = "generation_id"
        case schedulerEpoch = "scheduler_epoch"
        case artifactClass = "artifact_class"
        case estimatedWriteBytes = "estimated_write_bytes"
    }
}

struct DogfoodDiagnosticsWorkJournalSnapshot: Codable, Sendable, Equatable {
    let eventType: String
    let cause: String?
    let timestamp: Double
    let generationID: String
    let schedulerEpoch: Int
    let artifactClass: String

    enum CodingKeys: String, CodingKey {
        case eventType = "event_type"
        case cause
        case timestamp
        case generationID = "generation_id"
        case schedulerEpoch = "scheduler_epoch"
        case artifactClass = "artifact_class"
    }
}

struct DogfoodDiagnosticsExportResult: Sendable, Equatable {
    let fileURL: URL
    let logFileCount: Int
    let totalBytes: Int
}

enum DogfoodDiagnosticsExportError: Error, Equatable, LocalizedError {
    case missingDiagnosticsDirectory
    case noSurfaceStatusLogs
    case nonUTF8File(String)

    var errorDescription: String? {
        switch self {
        case .missingDiagnosticsDirectory:
            "No diagnostics directory exists yet."
        case .noSurfaceStatusLogs:
            "No surface-status dogfood logs were found."
        case .nonUTF8File(let filename):
            "Dogfood diagnostics file is not UTF-8: \(filename)"
        }
    }
}

enum DogfoodDiagnosticsExporter {
    /// Schema version when no `analysis_health` is attached. Kept as
    /// the historical v1 wire format so existing tooling continues
    /// to read old exports byte-for-byte.
    static let schemaVersionV1 = 1
    /// Schema version when an `analysis_health` block is attached.
    /// Older v1 readers see the extra key and ignore it; v2 readers
    /// can rely on the field being present whenever the version is
    /// 2.
    static let schemaVersionV2 = 2
    /// Public alias for callers that don't care about the version
    /// boundary — points at the v1 baseline so test code that
    /// constructs an archive without analysis_health gets the
    /// historical shape unchanged.
    static let schemaVersion = schemaVersionV1
    static let filenamePrefix = "playhead-dogfood-diagnostics"

    static func export(
        sourceDirectory: URL? = nil,
        outputDirectory: URL? = nil,
        now: Date = Date(),
        fileManager: FileManager = .default,
        activitySnapshot: DogfoodDiagnosticsActivitySnapshot? = nil,
        analysisHealth: DogfoodDiagnosticsAnalysisHealth? = nil
    ) throws -> DogfoodDiagnosticsExportResult {
        let source: URL
        if let sourceDirectory {
            source = sourceDirectory
        } else {
            source = try defaultDiagnosticsDirectory(fileManager: fileManager)
        }
        var isDirectory: ObjCBool = false
        guard fileManager.fileExists(atPath: source.path, isDirectory: &isDirectory),
              isDirectory.boolValue else {
            throw DogfoodDiagnosticsExportError.missingDiagnosticsDirectory
        }

        let entries = try fileManager.contentsOfDirectory(
            at: source,
            includingPropertiesForKeys: [.fileSizeKey],
            options: [.skipsSubdirectoryDescendants]
        )

        let eligible = entries
            .filter { isExportableDogfoodDiagnosticsFile($0.lastPathComponent) }
            .sorted { $0.lastPathComponent < $1.lastPathComponent }
        let logFiles = eligible.filter { isSurfaceStatusLog($0.lastPathComponent) }
        guard !logFiles.isEmpty else {
            throw DogfoodDiagnosticsExportError.noSurfaceStatusLogs
        }

        var totalBytes = 0
        let archiveFiles = try eligible.map { url -> DogfoodDiagnosticsArchiveFile in
            let data = try Data(contentsOf: url)
            totalBytes += data.count
            guard let content = String(data: data, encoding: .utf8) else {
                throw DogfoodDiagnosticsExportError.nonUTF8File(url.lastPathComponent)
            }
            return DogfoodDiagnosticsArchiveFile(
                filename: url.lastPathComponent,
                role: "surface_status_jsonl",
                byteCount: data.count,
                content: content
            )
        }

        let effectiveSchemaVersion = analysisHealth == nil
            ? schemaVersionV1
            : schemaVersionV2
        let archive = DogfoodDiagnosticsArchive(
            schemaVersion: effectiveSchemaVersion,
            generatedAt: now,
            files: archiveFiles,
            activitySnapshot: activitySnapshot,
            analysisHealth: analysisHealth
        )
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        let data = try encoder.encode(archive)

        let output: URL
        if let outputDirectory {
            output = outputDirectory
        } else {
            output = try defaultOutputDirectory(fileManager: fileManager)
        }
        try fileManager.createDirectory(at: output, withIntermediateDirectories: true)
        let fileURL = output.appendingPathComponent(filename(for: now), isDirectory: false)
        try data.write(to: fileURL, options: [.atomic])

        return DogfoodDiagnosticsExportResult(
            fileURL: fileURL,
            logFileCount: logFiles.count,
            totalBytes: totalBytes
        )
    }

    static func filename(for date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        let stamp = formatter.string(from: date).replacingOccurrences(of: ":", with: "-")
        return "\(filenamePrefix)-\(stamp).json"
    }

    static func isExportableDogfoodDiagnosticsFile(_ filename: String) -> Bool {
        isSurfaceStatusLog(filename)
    }

    private static func isSurfaceStatusLog(_ filename: String) -> Bool {
        filename.hasPrefix(SurfaceStatusInvariantLogger.sessionFilenamePrefix)
            && filename.hasSuffix(".\(SurfaceStatusInvariantLogger.sessionFilenameExtension)")
    }

    private static func defaultDiagnosticsDirectory(fileManager: FileManager) throws -> URL {
        let caches = try fileManager.url(
            for: .cachesDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: false
        )
        return caches.appendingPathComponent(
            SurfaceStatusInvariantLogger.diagnosticsDirectoryName,
            isDirectory: true
        )
    }

    private static func defaultOutputDirectory(fileManager: FileManager) throws -> URL {
        let documents = try fileManager.url(
            for: .documentDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        return documents.appendingPathComponent("DogfoodDiagnostics", isDirectory: true)
    }
}
