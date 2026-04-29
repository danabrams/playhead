// SurfaceStatusUILintTests.swift
// Regression guard: forbids any mention of `InternalMissCause` from Swift
// source files under `Playhead/Views/`. The bead spec says:
//
//   "CI grep lint blocks direct InternalMissCause references from any
//    UI source file."
//
// The acceptance criterion is phrased as "no UI source references
// InternalMissCause directly." This test enforces exactly that: a walk
// of the repository's Swift source tree that fails if `InternalMissCause`
// appears in any `.swift` file under `Playhead/Views/`.
//
// Rationale: `InternalMissCause` is the raw engine-side reason enum
// (playhead-v11). The UI layer must never render a raw cause — it must
// consume the `EpisodeSurfaceStatus` struct produced by the reducer in
// this module, whose `reason: SurfaceReason` is the copy-stable
// user-visible bucket. Coupling UI to `InternalMissCause` would make
// internal taxonomy changes (e.g. adding a new cause) ripple into UI
// copy decisions, exactly what the SurfaceStatus boundary exists to
// prevent.
//
// Comment-stripping, block-comment continuation handling, and exempt-
// path semantics follow `SchedulerLaneUILintTests.swift`.

import XCTest

final class SurfaceStatusUILintTests: XCTestCase {

    /// The literal token we forbid in UI source files. Word-boundary
    /// regex prevents matching identifiers that merely share a prefix.
    private static let forbiddenToken = #"\bInternalMissCause\b"#

    /// Additional forbidden tokens — the playhead-ol05 module-boundary
    /// extension. Each pattern is a word-boundary regex matching one of
    /// the symbols UI files MUST NOT reference directly:
    ///
    ///   * `AnalysisStore` / `AnalysisSummary` — persistence-layer types
    ///     the reducer consumes through `AnalysisState`. UI must not
    ///     reach past the reducer into the store.
    ///   * `SurfaceAttribution` — the cause→triple struct the policy
    ///     emits. UI works with `EpisodeSurfaceStatus`, not the raw
    ///     attribution triple.
    ///   * `CauseAttributionPolicy` — the policy the reducer consults;
    ///     UI must not reach past the reducer to the policy.
    ///
    /// Note: `SurfaceDisposition`, `SurfaceReason`, `ResolutionHint`
    /// are defined in `CauseTaxonomy.swift` BUT they are intentionally
    /// part of the reducer's public output (they are `EpisodeSurfaceStatus`'s
    /// fields). The lint therefore allow-lists those three names — UI
    /// IS expected to switch on `disposition` / `reason` / `hint`.
    private static let extendedForbiddenTokens: [String] = [
        #"\bInternalMissCause\b"#,
        #"\bAnalysisStore\b"#,
        #"\bAnalysisSummary\b"#,
        #"\bCauseAttributionPolicy\b"#,
        #"\bSurfaceAttribution\b"#,
    ]

    /// Scope of the lint: any `.swift` file whose path contains one of
    /// these substrings is checked.
    ///
    /// Historically this was a single string (`/Playhead/Views/`) but
    /// that left SwiftUI `View`-conforming types that live in
    /// `Playhead/App/` (`ContentView`, `RootView`, `ReturningSplashView`)
    /// outside the lint's reach. Anything that ships pixels to the user
    /// is UI-layer and must consume `EpisodeSurfaceStatus` only.
    private static let uiPathSubstrings: [String] = [
        "/Playhead/Views/",
        "/Playhead/App/",
    ]

    /// Allow-list of service-wiring files that live under `Playhead/App/`
    /// but are NOT UI-layer. `PlayheadRuntime.swift` is where the DI
    /// graph is assembled (and must reference `AnalysisStore` etc. to
    /// wire the graph together); `PlayheadAppDelegate.swift` is the UIKit
    /// delegate that sets up the runtime at launch and legitimately
    /// references internal types in doc comments. These two files are
    /// not SwiftUI Views and never render user copy.
    ///
    /// `CorpusExporter.swift` (playhead-dgzw, narE) is a DEBUG-only
    /// developer tool that intentionally reads `AnalysisStore` rows to
    /// write a JSONL corpus for offline analysis. It is not a SwiftUI
    /// View and never renders user-facing copy — it lives under
    /// `Playhead/Views/Settings/` purely for file-system locality with
    /// the Settings-debug-section call site.
    ///
    /// playhead-fwvz: `DebugEpisodeExporter.swift` and
    /// `TranscriptPeekViewModel.swift` were previously allow-listed
    /// here while their refactor was deferred. Both now consume
    /// boundary types only (`DebugEpisodeExportService` /
    /// `TranscriptPeekDataSource`) and have been removed from this
    /// list — the lint covers them like any other UI-layer file.
    private static let uiPathExemptFilenames: Set<String> = [
        "PlayheadRuntime.swift",
        "PlayheadAppDelegate.swift",
        "CorpusExporter.swift",
    ]

    func testInternalMissCauseIsNotReferencedInUIViews() throws {
        let appRoot = try Self.appSourceRoot()
        let regex = try NSRegularExpression(pattern: Self.forbiddenToken)

        var violations: [String] = []
        try Self.scan(root: appRoot, regex: regex, into: &violations)

        if !violations.isEmpty {
            XCTFail(
                "InternalMissCause referenced in UI-layer file "
                + "(\(violations.count) occurrence(s)). UI must consume "
                + "`EpisodeSurfaceStatus` and render `SurfaceReason` — see "
                + "playhead-5bb3 bead spec:\n"
                + violations.sorted().joined(separator: "\n")
            )
        }
    }

    /// playhead-ol05 extension: forbid every UI-layer reference to the
    /// extended token list (AnalysisStore / AnalysisSummary /
    /// CauseAttributionPolicy / SurfaceAttribution / InternalMissCause).
    /// UI files MUST consume `EpisodeSurfaceStatus` only.
    func testNoSchedulerOrPersistenceTypesInUIViews() throws {
        let appRoot = try Self.appSourceRoot()
        var allViolations: [String] = []
        for pattern in Self.extendedForbiddenTokens {
            let regex = try NSRegularExpression(pattern: pattern)
            var violations: [String] = []
            try Self.scan(root: appRoot, regex: regex, into: &violations)
            allViolations.append(contentsOf: violations)
        }
        if !allViolations.isEmpty {
            XCTFail(
                "Forbidden module-boundary token referenced in UI-layer file "
                + "(\(allViolations.count) occurrence(s)). UI must consume "
                + "`EpisodeSurfaceStatus` only — see playhead-ol05 bead spec:\n"
                + allViolations.sorted().joined(separator: "\n")
            )
        }
    }

    // MARK: - Lint-logic unit tests
    //
    // Parallel to `SchedulerLaneUILintTests` — verify the comment-
    // stripping engine so a regression here fails LOUDLY rather than
    // silently passing a scan of the app target.

    func testLintIgnoresLineComments() throws {
        let regex = try NSRegularExpression(pattern: Self.forbiddenToken)
        let source = """
        // InternalMissCause in a comment must not be flagged.
        let x = 1
        """
        let violations = Self.scanSource(source, regex: regex)
        XCTAssertEqual(violations, [], "Line comments must not be flagged")
    }

    func testLintIgnoresBlockCommentContinuationLines() throws {
        let regex = try NSRegularExpression(pattern: Self.forbiddenToken)
        let source = """
        /**
         * InternalMissCause inside a doc-comment block must not be flagged.
         */
        let x = 1
        """
        let violations = Self.scanSource(source, regex: regex)
        XCTAssertEqual(violations, [],
                       "Block-comment continuation lines must not be flagged")
    }

    func testLintFlagsRealReference() throws {
        let regex = try NSRegularExpression(pattern: Self.forbiddenToken)
        let source = "let cause: InternalMissCause = .thermal"
        let violations = Self.scanSource(source, regex: regex)
        XCTAssertEqual(violations.count, 1,
                       "A bare InternalMissCause reference must be flagged")
    }

    func testLintDoesNotFlagPrefixedIdentifiers() throws {
        let regex = try NSRegularExpression(pattern: Self.forbiddenToken)
        // Word-boundary regex must reject partial matches: an identifier
        // that merely starts with `InternalMissCause` but continues as
        // a longer token must NOT trip the lint.
        let source = "let x = InternalMissCauseExtension.sentinel"
        let violations = Self.scanSource(source, regex: regex)
        XCTAssertEqual(violations, [],
                       "Word-boundary regex must not flag an identifier "
                       + "that merely starts with InternalMissCause.")
    }

    func testLintFlagsStringLiteralMention() throws {
        let regex = try NSRegularExpression(pattern: Self.forbiddenToken)
        let source = #"let msg = "InternalMissCause is internal""#
        let violations = Self.scanSource(source, regex: regex)
        XCTAssertEqual(violations.count, 1,
                       "String-literal mentions of InternalMissCause must be flagged")
    }

    // MARK: - Scanner (copied from SchedulerLaneUILintTests for
    // symmetry; see that file's header for rationale.)

    // String literals are deliberately NOT stripped — user-facing copy
    // that mentions forbidden symbols (e.g. "InternalMissCause" in a UI
    // string) is a leak of internal taxonomy and should be flagged.
    // Matches SchedulerLaneUILintTests convention.
    private static func effectiveCode(onLine line: String) -> String? {
        let trimmed = line.drop(while: { $0 == " " || $0 == "\t" })
        if trimmed.first == "*" {
            return nil
        }
        if let range = line.range(of: "//") {
            let head = line[..<range.lowerBound]
            return String(head)
        }
        return line
    }

    static func scanSource(_ source: String, regex: NSRegularExpression) -> [Int] {
        var violations: [Int] = []
        var lineNumber = 0
        source.enumerateLines { line, _ in
            lineNumber += 1
            guard let code = effectiveCode(onLine: line) else { return }
            let range = NSRange(code.startIndex..., in: code)
            if regex.firstMatch(in: code, range: range) != nil {
                violations.append(lineNumber)
            }
        }
        return violations
    }

    private static func scan(
        root: URL,
        regex: NSRegularExpression,
        into violations: inout [String]
    ) throws {
        let fm = FileManager.default
        guard let enumerator = fm.enumerator(
            at: root,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        ) else {
            XCTFail("Could not enumerate \(root.path)")
            return
        }

        for case let url as URL in enumerator {
            guard url.pathExtension == "swift" else { continue }
            // Only police UI-layer paths.
            guard Self.uiPathSubstrings.contains(where: { url.path.contains($0) }) else {
                continue
            }
            // Service-wiring files under Playhead/App/ are exempt — they
            // are not SwiftUI Views and legitimately reference scheduler
            // / persistence types to wire the DI graph.
            if Self.uiPathExemptFilenames.contains(url.lastPathComponent) {
                continue
            }

            let source = try String(contentsOf: url, encoding: .utf8)
            let lineNumbers = Self.scanSource(source, regex: regex)
            for lineNumber in lineNumbers {
                violations.append(
                    "\(url.lastPathComponent):\(lineNumber): "
                    + "forbidden module-boundary token referenced in UI layer"
                )
            }
        }
    }

    // MARK: - Path resolution

    private static func appSourceRoot(file: StaticString = #filePath) throws -> URL {
        let thisFile = URL(fileURLWithPath: String(describing: file))
        // .../PlayheadTests/Services/SurfaceStatus/SurfaceStatusUILintTests.swift
        //   -> .../PlayheadTests/Services/SurfaceStatus
        //     -> .../PlayheadTests/Services
        //       -> .../PlayheadTests
        //         -> .../<repo root>
        let repoRoot = thisFile
            .deletingLastPathComponent() // SurfaceStatus
            .deletingLastPathComponent() // Services
            .deletingLastPathComponent() // PlayheadTests
            .deletingLastPathComponent() // repo root
        let app = repoRoot.appendingPathComponent("Playhead", isDirectory: true)
        var isDir: ObjCBool = false
        guard FileManager.default.fileExists(atPath: app.path, isDirectory: &isDir),
              isDir.boolValue else {
            throw NSError(
                domain: "SurfaceStatusUILintTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "App source root not found at \(app.path)"]
            )
        }
        return app
    }
}


