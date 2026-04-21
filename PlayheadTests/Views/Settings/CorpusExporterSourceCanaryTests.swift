// CorpusExporterSourceCanaryTests.swift
// Compile-time canary for narE (playhead-dgzw): verifies the corpus-export
// machinery is gated by `#if DEBUG` in both its implementation file and the
// `SettingsView` call site.
//
// Rationale: the bead acceptance says "Dev-gate test: action is unavailable
// in release builds (same assertion as narL)". Xcode does not let us vary
// build configuration inside a single test plan, so we assert the mechanism
// — the `#if DEBUG` wrapper — with a source-level grep. The wrapper is the
// only guard between the corpus exporter and a Release build, so pinning it
// in source is the right invariant to lock in.

import Foundation
import Testing

// Intentionally does NOT `@testable import Playhead` — it operates purely on
// the source files so the test runs in any configuration without needing the
// DEBUG-gated symbols to be visible.
@Suite("CorpusExporter source canaries (playhead-dgzw)")
struct CorpusExporterSourceCanaryTests {

    // MARK: - Helpers

    private static let repoRoot: URL = {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent() // .../Settings/
            .deletingLastPathComponent() // .../Views/
            .deletingLastPathComponent() // .../PlayheadTests/
            .deletingLastPathComponent() // .../<repo root>/
    }()

    private func read(_ relative: String) throws -> String {
        let url = Self.repoRoot.appendingPathComponent(relative)
        return try String(contentsOf: url, encoding: .utf8)
    }

    // MARK: - Exporter file

    @Test("CorpusExporter.swift begins with a file-level #if DEBUG guard")
    func exporterFileGuardedByDebug() throws {
        let source = try read("Playhead/Views/Settings/CorpusExporter.swift")
        #expect(source.contains("#if DEBUG"))
        #expect(source.contains("#endif"))
        // The file's first non-comment, non-blank line must be `#if DEBUG`.
        let firstCode = source
            .split(separator: "\n", omittingEmptySubsequences: false)
            .first { line in
                let trimmed = line.trimmingCharacters(in: .whitespaces)
                return !trimmed.isEmpty && !trimmed.hasPrefix("//")
            }
        #expect(firstCode?.trimmingCharacters(in: .whitespaces) == "#if DEBUG")
    }

    @Test("CorpusExporter type lives inside the #if DEBUG region")
    func exporterTypeInsideDebugRegion() throws {
        let source = try read("Playhead/Views/Settings/CorpusExporter.swift")
        #expect(source.contains("enum CorpusExporter"))
        guard
            let debugOpen = source.range(of: "#if DEBUG"),
            let entry = source.range(of: "enum CorpusExporter")
        else {
            Issue.record("Expected both #if DEBUG and CorpusExporter in source")
            return
        }
        #expect(debugOpen.lowerBound < entry.lowerBound)
    }

    // MARK: - Settings call site

    @Test("SettingsView references CorpusExporter only under #if DEBUG")
    func settingsCallSiteGuarded() throws {
        let source = try read("Playhead/Views/Settings/SettingsView.swift")
        #expect(source.contains("CorpusExporter"),
                "Settings must reference CorpusExporter to expose the debug action")
        let lines = source.split(separator: "\n", omittingEmptySubsequences: false)
        var depth = 0
        var unguardedReference = false
        for line in lines {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if trimmed.hasPrefix("#if DEBUG") {
                depth += 1
            } else if trimmed.hasPrefix("#endif") && depth > 0 {
                depth -= 1
            }
            if line.contains("CorpusExporter") && depth == 0 {
                unguardedReference = true
            }
        }
        #expect(!unguardedReference,
                "CorpusExporter must only appear inside #if DEBUG regions of SettingsView")
    }
}
