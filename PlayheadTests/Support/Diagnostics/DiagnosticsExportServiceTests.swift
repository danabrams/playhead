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

    @Test("dogfood export writes a small JSON archive with only surface-status diagnostics")
    func dogfoodExportWritesSurfaceStatusArchive() throws {
        let source = try makeTempDir(prefix: "DogfoodDiagnosticsSource")
        let output = try makeTempDir(prefix: "DogfoodDiagnosticsOutput")
        defer {
            try? FileManager.default.removeItem(at: source)
            try? FileManager.default.removeItem(at: output)
        }

        let logName = "surface-status-20260506T221555Z-AB96209D-BC9E-498D-A346-1DE302031315.jsonl"
        let logBody = """
        {"timestamp":"2026-05-06T22:15:55Z","session_id":"AB96209D-BC9E-498D-A346-1DE302031315","new_disposition":"queued","new_reason":"none","event_type":"ready_entered","entry_trigger":"analysis_completed"}

        """
        try logBody.write(
            to: source.appendingPathComponent(logName),
            atomically: true,
            encoding: .utf8
        )
        try "install-salt".write(
            to: source.appendingPathComponent(".surface-status-install-id"),
            atomically: true,
            encoding: .utf8
        )
        try "ignore me".write(
            to: source.appendingPathComponent("transcript-shadow-gate.jsonl"),
            atomically: true,
            encoding: .utf8
        )

        let result = try DogfoodDiagnosticsExporter.export(
            sourceDirectory: source,
            outputDirectory: output,
            now: Date(timeIntervalSince1970: 1_700_000_000)
        )

        #expect(result.logFileCount == 1)
        #expect(result.totalBytes == Data(logBody.utf8).count)
        #expect(result.fileURL.lastPathComponent.hasPrefix("playhead-dogfood-diagnostics-"))

        let data = try Data(contentsOf: result.fileURL)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let archive = try decoder.decode(DogfoodDiagnosticsArchive.self, from: data)

        #expect(archive.schemaVersion == DogfoodDiagnosticsExporter.schemaVersion)
        #expect(archive.files.map(\.filename).sorted() == [
            logName
        ])
        #expect(archive.files.contains { $0.role == "surface_status_jsonl" && $0.content == logBody })
        #expect(!archive.files.contains { $0.filename == ".surface-status-install-id" })
        #expect(!archive.files.contains { $0.filename == "transcript-shadow-gate.jsonl" })
    }

    @Test("dogfood export fails clearly when no surface-status logs exist")
    func dogfoodExportRequiresSurfaceStatusLogs() throws {
        let source = try makeTempDir(prefix: "DogfoodDiagnosticsEmpty")
        let output = try makeTempDir(prefix: "DogfoodDiagnosticsOutput")
        defer {
            try? FileManager.default.removeItem(at: source)
            try? FileManager.default.removeItem(at: output)
        }

        try "install-salt".write(
            to: source.appendingPathComponent(".surface-status-install-id"),
            atomically: true,
            encoding: .utf8
        )

        do {
            _ = try DogfoodDiagnosticsExporter.export(
                sourceDirectory: source,
                outputDirectory: output
            )
            Issue.record("Expected dogfood export to require at least one surface-status log")
        } catch let error as DogfoodDiagnosticsExportError {
            #expect(error == .noSurfaceStatusLogs)
        } catch {
            Issue.record("Expected DogfoodDiagnosticsExportError.noSurfaceStatusLogs, got \(error)")
        }
    }
}
