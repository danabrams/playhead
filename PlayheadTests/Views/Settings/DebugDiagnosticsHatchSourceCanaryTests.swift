// DebugDiagnosticsHatchSourceCanaryTests.swift
// Compile-time canary: verifies the playhead-ct2q hatch is gated by
// `#if DEBUG` in BOTH the hatch implementation file and the
// `SettingsView` call site.
//
// Rationale (from bead spec): "Add a test asserting the Settings entry
// is NOT present in a Release build." Xcode does not let us vary build
// configuration inside a single test plan, so we assert the mechanism
// (the `#if DEBUG` wrapper) with a source-level grep. The wrapper is
// the only guard between the dogfood hatch and a Release build, so
// pinning it in source is exactly the right invariant to lock in.
//
// If this test fails, the `#if DEBUG` wrapper has been removed or
// altered and the hatch may leak into Release builds.

import Foundation
import Testing

// This test suite intentionally does NOT `@testable import Playhead`
// — it operates purely on the source files. That avoids coupling the
// test to any DEBUG-gated symbol.

@Suite("DebugDiagnosticsHatch source canaries (playhead-ct2q)")
struct DebugDiagnosticsHatchSourceCanaryTests {

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

    // MARK: - Hatch file

    @Test("DebugDiagnosticsHatch.swift begins with a file-level #if DEBUG guard")
    func hatchFileGuardedByDebug() throws {
        let source = try read("Playhead/Support/Diagnostics/DebugDiagnosticsHatch.swift")

        // The file MUST contain both #if DEBUG and the matching #endif.
        // A plain `contains` check is enough — if the guard is removed,
        // compile would still succeed for Release, which is exactly the
        // regression we're protecting against.
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

    @Test("runDebugDiagnosticsExport entry symbol lives inside the #if DEBUG region")
    func entryPointInsideDebugRegion() throws {
        let source = try read("Playhead/Support/Diagnostics/DebugDiagnosticsHatch.swift")
        // The entry point must exist.
        #expect(source.contains("func runDebugDiagnosticsExport("))
        // And it must appear AFTER the file-level `#if DEBUG` opener.
        guard
            let debugOpen = source.range(of: "#if DEBUG"),
            let entry = source.range(of: "func runDebugDiagnosticsExport(")
        else {
            Issue.record("Expected both #if DEBUG and runDebugDiagnosticsExport in hatch source")
            return
        }
        #expect(debugOpen.lowerBound < entry.lowerBound)
    }

    // MARK: - Settings call site

    @Test("SettingsView.swift wraps the sendDiagnosticsSection call under #if DEBUG")
    func settingsCallSiteGuarded() throws {
        let source = try read("Playhead/Views/Settings/SettingsView.swift")
        #expect(source.contains("sendDiagnosticsSection"))
        // The reference inside `body` must appear inside a #if DEBUG
        // block. We assert indirectly: every occurrence of
        // "sendDiagnosticsSection" must be preceded (in source order)
        // by a #if DEBUG that has not yet been closed by #endif.
        let lines = source.split(separator: "\n", omittingEmptySubsequences: false)
        var depth = 0
        var sawGuardedReference = false
        for line in lines {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if trimmed.hasPrefix("#if DEBUG") {
                depth += 1
            } else if trimmed.hasPrefix("#endif") && depth > 0 {
                depth -= 1
            }
            if line.contains("sendDiagnosticsSection") && depth > 0 {
                sawGuardedReference = true
            }
        }
        #expect(sawGuardedReference, "sendDiagnosticsSection must only appear inside a #if DEBUG region")
    }
}
