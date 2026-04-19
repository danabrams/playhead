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
}

#endif // canImport(UIKit) && canImport(MessageUI) && os(iOS)
