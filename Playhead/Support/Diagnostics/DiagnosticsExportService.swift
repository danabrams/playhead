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
    static let mailOnlyFallbackExcludedActivities: [UIActivity.ActivityType] = [
        .addToReadingList,
        .airDrop,
        .assignToContact,
        .copyToPasteboard,
        .markupAsPDF,
        .message,
        .openInIBooks,
        .postToFacebook,
        .postToFlickr,
        .postToTencentWeibo,
        .postToTwitter,
        .postToVimeo,
        .postToWeibo,
        .print,
        .saveToCameraRoll,
        .sharePlay
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
    let fetchLimit: Int

    init(
        appVersion: String,
        osVersion: String,
        deviceClass: DeviceClass,
        buildType: BuildType,
        eligibility: AnalysisEligibility,
        installID: UUID,
        now: Date = .now,
        fetchLimit: Int = DiagnosticsBundleBuilder.schedulerEventsCap
    ) {
        self.appVersion = appVersion
        self.osVersion = osVersion
        self.deviceClass = deviceClass
        self.buildType = buildType
        self.eligibility = eligibility
        self.installID = installID
        self.now = now
        self.fetchLimit = fetchLimit
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
