// DiagnosticsExportService.swift
// Mail-composer entry point for the support-safe diagnostics bundle.
// Owns the file-write + composer presentation; the actual UI placement
// (Settings → Diagnostics screen) is delivered by Phase 2 playhead-l274.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// Why split out from the bundle builder: the builder is pure and runs
// on any thread; this service touches MessageUI / UIKit and must run on
// the main actor. Keeping the service thin means most of the bead's
// behavior is unit-testable without a UI host.
//
// iPad fallback: per spec, iPad falls back to `UIActivityViewController`
// with `.mail` excluded from other sharing surfaces (support requires
// the email artifact specifically). The fallback is selected at
// presentation time via `UIDevice.current.userInterfaceIdiom`.
//
// Reset hook: the composer's completion handler routes the
// `MFMailComposeResult` through ``DiagnosticsOptInResetPolicy`` and
// applies the new value to every `Episode.diagnosticsOptIn` that was
// set when the bundle was built. This bead delivers the data plumbing;
// the toggle UI itself ships in Phase 2 playhead-l274.

import Foundation

#if canImport(UIKit)
import UIKit
#endif

#if canImport(MessageUI)
import MessageUI
#endif

// MARK: - Service

/// Coordinates diagnostics-bundle generation and presents the iOS mail
/// composer. The service is intentionally `@MainActor` because every
/// surface it touches (UIKit presentation, MessageUI delegate callbacks,
/// SwiftData ModelContext mutations) is main-actor isolated.
@MainActor
final class DiagnosticsExportService {

    /// MIME type the mail composer attaches the bundle as. Surfaced as a
    /// constant so tests can grep-anchor it.
    static let attachmentMIMEType = "application/json"

    /// Filename prefix used when constructing
    /// `playhead-diagnostics-<ISO8601>.json`. Surfaced as a constant for
    /// the same reason.
    static let filenamePrefix = "playhead-diagnostics"

    /// Convenience builder for the composer subject line. Public so the
    /// Phase 2 UI can preview it.
    static func defaultSubject(buildType: BuildType) -> String {
        "Playhead diagnostics (\(buildType.rawValue))"
    }

    /// Constructs the canonical filename for a bundle generated at
    /// `date`. Splitting this out keeps the ISO8601 substitution
    /// auditable.
    static func filename(for date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        // ":" is illegal on FAT32 / acceptable on APFS — strip to keep
        // the filename portable across whatever the user's mail client
        // does with the attachment.
        let stamp = formatter.string(from: date).replacingOccurrences(of: ":", with: "-")
        return "\(filenamePrefix)-\(stamp).json"
    }

    /// Encodes the bundle file into JSON bytes ready for attachment.
    /// Centralized so the encoder configuration (date strategy, key
    /// formatting) is uniform across every emission site.
    static func encode(_ bundle: DiagnosticsBundleFile) throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .prettyPrinted]
        encoder.dateEncodingStrategy = .iso8601
        return try encoder.encode(bundle)
    }

    #if canImport(MessageUI) && os(iOS)
    /// Maps a real `MFMailComposeResult` into the test-friendly
    /// `DiagnosticsMailComposeResult`. The mapping is deliberately
    /// total — `@unknown default` returns `.failed` so a future Apple
    /// case never silently clears `diagnosticsOptIn`.
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
}
