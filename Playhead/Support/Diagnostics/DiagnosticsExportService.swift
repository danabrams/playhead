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

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case generatedAt = "generated_at"
        case files
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
    static let schemaVersion = 1
    static let filenamePrefix = "playhead-dogfood-diagnostics"

    static func export(
        sourceDirectory: URL? = nil,
        outputDirectory: URL? = nil,
        now: Date = Date(),
        fileManager: FileManager = .default
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

        let archive = DogfoodDiagnosticsArchive(
            schemaVersion: schemaVersion,
            generatedAt: now,
            files: archiveFiles
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
