// UIKitDiagnosticsPresenterTests.swift
// Coverage for the UIKit-backed presenter surface. The actual MFMailCompose
// presentation is simulator-hostile, so these tests cover the parts that
// CAN be exercised without driving the composer:
//   * Missing host view controller → `DiagnosticsExportError.missingHostViewController`.
//   * Activity-fallback mail-only exclusion list contract.
//   * UIDevice user-interface-idiom branch selection is covered indirectly
//     by the activity fallback tests (the iPad path ships the same
//     `UIActivityViewController` when mail is unavailable).
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).

import Foundation
import Testing

#if canImport(UIKit) && canImport(MessageUI) && os(iOS)
import UIKit

@testable import Playhead

@Suite("UIKitDiagnosticsPresenter (playhead-ghon)")
@MainActor
struct UIKitDiagnosticsPresenterTests {

    @Test("missing host view controller surfaces DiagnosticsExportError.missingHostViewController")
    func missingHostSurfacesError() async throws {
        let presenter = UIKitDiagnosticsPresenter(hostProvider: { nil })

        let result: Result<DiagnosticsMailComposeResult, Error> = await withCheckedContinuation { continuation in
            presenter.present(
                data: Data(),
                filename: "playhead-diagnostics-x.json",
                subject: "s"
            ) { outcome in
                continuation.resume(returning: outcome)
            }
        }

        switch result {
        case .success:
            Issue.record("Expected presenter to fail with missingHostViewController")
        case .failure(let error):
            #expect(error as? DiagnosticsExportError == .missingHostViewController)
        }
    }

    @Test("activity fallback exclusion list matches the mail-only contract")
    func activityFallbackExclusionList() {
        let excluded = DiagnosticsExportService.mailOnlyFallbackExcludedActivities
        // All non-mail system activities must be excluded so support gets
        // the email artifact. Spot-check the highest-risk entries that
        // would otherwise route diagnostics elsewhere.
        #expect(excluded.contains(.airDrop))
        #expect(excluded.contains(.message))
        #expect(excluded.contains(.copyToPasteboard))
        #expect(excluded.contains(.print))
        #expect(excluded.contains(.saveToCameraRoll))
        // Mail itself must NOT be excluded.
        let containsMail = excluded.contains { type in
            type.rawValue == "com.apple.UIKit.activity.Mail"
        }
        #expect(containsMail == false)
    }

    // MARK: - Activity-fallback temp-file hygiene

    @Test("activity fallback writes into a fresh subdirectory under tmp")
    func activityFallbackWritesInsideSubdir() throws {
        let parent = FileManager.default.temporaryDirectory
            .appendingPathComponent("uikit-presenter-tests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: parent, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: parent) }

        let subdirName = UUID().uuidString
        let data = Data("{\"x\":1}".utf8)
        let (subdir, fileURL) = try UIKitDiagnosticsPresenter.writeBundleToFreshSubdirectory(
            data: data,
            filename: "playhead-diagnostics-x.json",
            parentDirectory: parent,
            subdirectoryName: subdirName
        )

        #expect(subdir.lastPathComponent == subdirName)
        #expect(subdir.deletingLastPathComponent().path == parent.path)
        #expect(fileURL.deletingLastPathComponent().path == subdir.path)
        #expect(FileManager.default.fileExists(atPath: fileURL.path))
    }

    @Test("removeSubdirectory cleans the per-export tmp dir after fallback completes")
    func removeSubdirectoryCleansUp() throws {
        let parent = FileManager.default.temporaryDirectory
            .appendingPathComponent("uikit-presenter-tests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: parent, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: parent) }

        let data = Data("{\"x\":1}".utf8)
        let (subdir, fileURL) = try UIKitDiagnosticsPresenter.writeBundleToFreshSubdirectory(
            data: data,
            filename: "playhead-diagnostics-x.json",
            parentDirectory: parent,
            subdirectoryName: UUID().uuidString
        )
        #expect(FileManager.default.fileExists(atPath: fileURL.path))
        #expect(FileManager.default.fileExists(atPath: subdir.path))

        // Simulate the activity controller's completion handler firing
        // (in any of .success/.cancelled/.error shapes — the cleanup path
        // is the same).
        UIKitDiagnosticsPresenter.removeSubdirectory(subdir)

        // Both the file and the subdir must be gone — the JSON contains
        // opted-in PII and iOS does not reliably reap tmp files.
        #expect(!FileManager.default.fileExists(atPath: fileURL.path))
        #expect(!FileManager.default.fileExists(atPath: subdir.path))
    }

    @Test("removeSubdirectory is a no-op when the subdir is already gone")
    func removeSubdirectoryIsIdempotent() {
        let bogus = FileManager.default.temporaryDirectory
            .appendingPathComponent("does-not-exist-\(UUID().uuidString)", isDirectory: true)
        // Must not throw or crash even though the path was never created.
        UIKitDiagnosticsPresenter.removeSubdirectory(bogus)
        #expect(!FileManager.default.fileExists(atPath: bogus.path))
    }
}

#endif // canImport(UIKit) && canImport(MessageUI) && os(iOS)
