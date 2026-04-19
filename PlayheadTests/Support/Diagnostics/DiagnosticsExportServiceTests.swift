// DiagnosticsExportServiceTests.swift
// Pure-surface tests for the diagnostics export service. The
// `MFMailComposeViewController` presentation path is intentionally
// outside test scope (simulator-hostile); these tests cover the
// non-UI surface: filename, subject, encoding, and the
// `MFMailComposeResult` → `DiagnosticsMailComposeResult` adapter.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).

import Foundation
import Testing

#if canImport(MessageUI) && os(iOS)
import MessageUI
#endif

@testable import Playhead

@Suite("DiagnosticsExportService — pure surface (playhead-ghon)")
@MainActor
struct DiagnosticsExportServiceTests {

    @Test("filename has the playhead-diagnostics-<ISO8601>.json shape")
    func filenameShape() {
        let date = Date(timeIntervalSince1970: 1_700_000_000)
        let name = DiagnosticsExportService.filename(for: date)
        #expect(name.hasPrefix("playhead-diagnostics-"))
        #expect(name.hasSuffix(".json"))
        // ISO8601 stamps contain "T" — confirm the ":" replacement still
        // leaves a parseable structure.
        #expect(name.contains("T"))
        #expect(!name.contains(":"))
    }

    @Test("MIME type is application/json")
    func mimeType() {
        #expect(DiagnosticsExportService.attachmentMIMEType == "application/json")
    }

    @Test("default subject embeds the build type")
    func subjectIncludesBuildType() {
        let subject = DiagnosticsExportService.defaultSubject(buildType: .testFlight)
        #expect(subject.contains("test_flight"))
    }

    @Test("encode produces parseable JSON for a minimal bundle file")
    func encodeRoundTrips() throws {
        let bundle = DiagnosticsBundleFile(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            default: DefaultBundle(
                appVersion: "1.0.0",
                osVersion: "iOS 26.0",
                deviceClass: .iPhone17Pro,
                buildType: .release,
                eligibilitySnapshot: AnalysisEligibility(
                    hardwareSupported: true,
                    appleIntelligenceEnabled: true,
                    regionSupported: true,
                    languageSupported: true,
                    modelAvailableNow: true,
                    capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
                ),
                analysisUnavailableReason: nil,
                schedulerEvents: [],
                workJournalTail: []
            ),
            optIn: nil
        )
        let data = try DiagnosticsExportService.encode(bundle)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(DiagnosticsBundleFile.self, from: data)
        #expect(decoded == bundle)
    }

    #if canImport(MessageUI) && os(iOS)
    @Test("MFMailComposeResult maps 1:1 onto the test-friendly enum")
    func mailComposeResultMapping() {
        #expect(DiagnosticsExportService.map(.cancelled) == .cancelled)
        #expect(DiagnosticsExportService.map(.saved)     == .saved)
        #expect(DiagnosticsExportService.map(.sent)      == .sent)
        #expect(DiagnosticsExportService.map(.failed)    == .failed)
    }
    #endif
}
