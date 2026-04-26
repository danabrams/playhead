// PlayheadRuntimeLoggerLazinessTests.swift
// playhead-jncn: Source-canary that asserts each of the 5 sync-loggers
// constructed inside `PlayheadRuntime.init` keep their `init` body free of
// FileManager / FileHandle / Data-write calls. The heavy I/O must move to
// an async `migrate()` (or first-use lazy path) so the synchronous
// `PlayheadRuntime.init` flow — which extends the launch-storyboard
// window — stays off the disk.
//
// Mirrors the style of
// `PermissiveClassifierBoxLazinessTests.testInitBodyDoesNotEagerlyConstructPermissiveClassifier`
// (playhead-jndk): walk the source file, isolate the brace-delimited
// body of the target init, and grep for forbidden tokens.
//
// XCTest (not Swift Testing) so the canary class is filterable through
// the Xcode test plan's `skippedTests` (`xctestplan` silently ignores
// Swift Testing identifiers; see PlayheadFastTests.xctestplan comment).

import Foundation
import XCTest
@testable import Playhead

final class PlayheadRuntimeLoggerLazinessSourceCanaryTests: XCTestCase {

    // MARK: - Per-logger canaries

    /// `FoundationModelsFeedbackStore.init` (audit #4 — DEBUG only) must
    /// not perform synchronous FileManager / FileHandle / Data-write work.
    /// The directory resolution + create-on-demand path is `migrate()`.
    func testFoundationModelsFeedbackStoreInitIsLazy() throws {
        try assertInitBodyHasNoFileSystemCalls(
            sourcePath: "Playhead/Services/AdDetection/FoundationModelsFeedbackStore.swift",
            initSignatures: [
                "init(\n        directory: URL? = nil,",
                "init(directory: URL? = nil,"
            ],
            symbolForMessages: "FoundationModelsFeedbackStore"
        )
    }

    /// `SurfaceStatusInvariantLogger.init` (audit #8) must not perform
    /// synchronous FileManager / FileHandle / Data-write work. Salt load
    /// + directory creation are deferred to first use through
    /// `LoggerState`.
    ///
    /// The shell `SurfaceStatusInvariantLogger.init(directory:)` is a
    /// thin forwarder — the heavyweight prior work lives in
    /// `LoggerState.init`. We canary BOTH bodies so a future regression
    /// that re-introduces sync I/O in either place fails here.
    func testSurfaceStatusInvariantLoggerInitIsLazy() throws {
        try assertInitBodyHasNoFileSystemCalls(
            sourcePath: "Playhead/SurfaceStatus/SurfaceStatusInvariantLogger.swift",
            initSignatures: [
                "init(directory: URL? = nil) {",
                "init(directory: URL?) {"
            ],
            symbolForMessages: "SurfaceStatusInvariantLogger / LoggerState"
        )
    }

    /// `DecisionLogger.init(directory:rotationThresholdBytes:)` (audit
    /// #10 — DEBUG only) must not perform synchronous FileManager
    /// directory create or `contentsOfDirectory` scans. Both move to
    /// `migrate()` / first-use.
    func testDecisionLoggerInitIsLazy() throws {
        try assertInitBodyHasNoFileSystemCalls(
            sourcePath: "Playhead/Services/AdDetection/DecisionLogger.swift",
            initSignatures: [
                "init(\n        directory: URL,",
                "init(directory: URL,"
            ],
            symbolForMessages: "DecisionLogger"
        )
    }

    /// `AssetLifecycleLogger.init(directory:rotationThresholdBytes:)`
    /// (audit #15) must not perform synchronous FileManager directory
    /// create or `contentsOfDirectory` scans. Both move to `migrate()` /
    /// first-use.
    func testAssetLifecycleLoggerInitIsLazy() throws {
        try assertInitBodyHasNoFileSystemCalls(
            sourcePath: "Playhead/Services/AnalysisCoordinator/AssetLifecycleLogger.swift",
            initSignatures: [
                "init(\n        directory: URL,",
                "init(directory: URL,"
            ],
            symbolForMessages: "AssetLifecycleLogger"
        )
    }

    /// `BGTaskTelemetryLogger.init(directory:rotationThresholdBytes:)`
    /// (audit #17) must not perform synchronous FileManager directory
    /// create or `contentsOfDirectory` scans. Both move to `migrate()` /
    /// first-use.
    func testBGTaskTelemetryLoggerInitIsLazy() throws {
        try assertInitBodyHasNoFileSystemCalls(
            sourcePath: "Playhead/Services/Diagnostics/BGTaskTelemetryLogger.swift",
            initSignatures: [
                "init(\n        directory: URL,",
                "init(directory: URL,"
            ],
            symbolForMessages: "BGTaskTelemetryLogger"
        )
    }

    // MARK: - Helper

    /// Forbidden tokens, per the bead spec. Each must NOT appear in the
    /// designated init body. The list captures every realistic synchronous
    /// FileManager / FileHandle / write surface used elsewhere in this
    /// codebase.
    private static let forbiddenTokens: [String] = [
        "FileManager.default.create",   // catches createFile, createDirectory
        "FileHandle(",                   // any FileHandle constructor
        ".write(to:",                    // Data / String write to URL
        ").write(",                      // chained Data().write(...)
        "fileManager.createDirectory",   // injected fileManager
        "fileManager.url(",              // injected fileManager url(create:true)
        "fileManager.createFile",
        "FileManager.default.url(",      // url(create: true) implicit dir create
        "contentsOfDirectory",           // scanNextRotationIndex pre-refactor
    ]

    /// Locate the first init signature in `signatures` that exists in the
    /// source file, isolate its brace-delimited body, and assert that
    /// none of the forbidden tokens appear inside.
    private func assertInitBodyHasNoFileSystemCalls(
        sourcePath: String,
        initSignatures: [String],
        symbolForMessages: String,
        file: StaticString = #file,
        line: UInt = #line
    ) throws {
        let url = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent() // .../Diagnostics/
            .deletingLastPathComponent() // .../Services/
            .deletingLastPathComponent() // .../PlayheadTests/
            .deletingLastPathComponent() // .../<repo root>/
            .appendingPathComponent(sourcePath)
        let source = try String(contentsOf: url, encoding: .utf8)

        // For SurfaceStatusInvariantLogger we audit BOTH the public
        // facade init AND `LoggerState.init` — the heavyweight work
        // historically lived in the latter. We do this by collecting all
        // init bodies that match any of the supplied signatures and
        // concatenating their bodies for the forbidden-token search.
        var combinedBody = ""
        var foundAny = false
        var searchStart = source.startIndex
        while searchStart < source.endIndex {
            // Find earliest occurrence of any signature on or after searchStart.
            var earliestRange: Range<String.Index>?
            for sig in initSignatures {
                if let r = source.range(of: sig, range: searchStart..<source.endIndex) {
                    if earliestRange == nil || r.lowerBound < earliestRange!.lowerBound {
                        earliestRange = r
                    }
                }
            }
            guard let r = earliestRange else { break }
            foundAny = true

            // Walk forward to the brace that opens the body. The
            // signature ends with `{`, so the open-brace index is the
            // last char (or one position back if our match ended after
            // it). To stay robust against signature variants we scan
            // forward from r.lowerBound for the first `{` outside string
            // / comment context.
            guard let openBraceIdx = Self.findOpenBrace(in: source, after: r.lowerBound) else {
                searchStart = r.upperBound
                continue
            }
            let body = Self.bracedBody(in: source, startingAt: openBraceIdx)
            combinedBody += "\n// === init body at offset \(source.distance(from: source.startIndex, to: r.lowerBound)) ===\n"
            combinedBody += body
            searchStart = r.upperBound
        }

        XCTAssertTrue(
            foundAny,
            "[\(symbolForMessages)] none of the init signatures \(initSignatures) found in \(sourcePath) — test must be updated alongside any rename.",
            file: file,
            line: line
        )

        for token in Self.forbiddenTokens {
            if combinedBody.contains(token) {
                XCTFail(
                    """
                    [\(symbolForMessages)] init body in \(sourcePath) still contains \
                    forbidden synchronous file-system token `\(token)`. The bead \
                    playhead-jncn requires this work to move to an async `migrate()` \
                    (or first-use lazy path) so PlayheadRuntime.init stays off-disk. \
                    Mirror the AdCatalogStore.ensureOpen() pattern (Playhead/Services/\
                    AdDetection/AdCatalogStore.swift).
                    """,
                    file: file,
                    line: line
                )
            }
        }
    }

    // MARK: - Source walker (mirrors PermissiveClassifierBoxLazinessTests)

    /// Find the first `{` at or after `position` that is not inside a
    /// string literal or comment. Returns nil if none.
    private static func findOpenBrace(
        in source: String,
        after position: String.Index
    ) -> String.Index? {
        var i = position
        var inLineComment = false
        var inBlockComment = false
        var inString = false
        let endIdx = source.endIndex

        while i < endIdx {
            let c = source[i]
            let next = source.index(after: i) < endIdx ? source[source.index(after: i)] : Character("\0")

            if inLineComment {
                if c == "\n" { inLineComment = false }
                i = source.index(after: i); continue
            }
            if inBlockComment {
                if c == "*" && next == "/" {
                    inBlockComment = false
                    i = source.index(i, offsetBy: 2); continue
                }
                i = source.index(after: i); continue
            }
            if inString {
                if c == "\\" && source.index(after: i) < endIdx {
                    i = source.index(i, offsetBy: 2); continue
                }
                if c == "\"" { inString = false }
                i = source.index(after: i); continue
            }

            if c == "/" && next == "/" {
                inLineComment = true
                i = source.index(i, offsetBy: 2); continue
            }
            if c == "/" && next == "*" {
                inBlockComment = true
                i = source.index(i, offsetBy: 2); continue
            }
            if c == "\"" {
                inString = true
                i = source.index(after: i); continue
            }
            if c == "{" {
                return i
            }
            i = source.index(after: i)
        }
        return nil
    }

    /// Returns the text of the brace-delimited block whose opening `{`
    /// is at `startIndex`. Tracks nesting depth so inner braces don't
    /// terminate the body early. Treats `//` line comments and `/* */`
    /// block comments as opaque (their braces don't count).
    private static func bracedBody(in source: String, startingAt startIndex: String.Index) -> String {
        precondition(source[startIndex] == "{")
        var depth = 0
        var i = startIndex
        var inLineComment = false
        var inBlockComment = false
        var inString = false
        let endIdx = source.endIndex
        var bodyStart: String.Index?

        while i < endIdx {
            let c = source[i]
            let next = source.index(after: i) < endIdx ? source[source.index(after: i)] : Character("\0")

            if inLineComment {
                if c == "\n" { inLineComment = false }
                i = source.index(after: i); continue
            }
            if inBlockComment {
                if c == "*" && next == "/" {
                    inBlockComment = false
                    i = source.index(i, offsetBy: 2); continue
                }
                i = source.index(after: i); continue
            }
            if inString {
                if c == "\\" && source.index(after: i) < endIdx {
                    i = source.index(i, offsetBy: 2); continue
                }
                if c == "\"" { inString = false }
                i = source.index(after: i); continue
            }

            if c == "/" && next == "/" {
                inLineComment = true
                i = source.index(i, offsetBy: 2); continue
            }
            if c == "/" && next == "*" {
                inBlockComment = true
                i = source.index(i, offsetBy: 2); continue
            }
            if c == "\"" {
                inString = true
                i = source.index(after: i); continue
            }

            if c == "{" {
                depth += 1
                if depth == 1 {
                    bodyStart = source.index(after: i)
                }
            } else if c == "}" {
                depth -= 1
                if depth == 0 {
                    if let start = bodyStart {
                        return String(source[start..<i])
                    }
                    return ""
                }
            }
            i = source.index(after: i)
        }
        return ""
    }
}
