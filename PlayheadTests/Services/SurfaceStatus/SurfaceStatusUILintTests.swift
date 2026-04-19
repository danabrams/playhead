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

    /// Scope of the lint: any `.swift` file whose path contains this
    /// substring is checked. Anchored on `/Playhead/Views/` so the UI
    /// layer (and only the UI layer) is policed; the cause type is
    /// legitimately used in `Services/` and the new `SurfaceStatus/`
    /// module.
    private static let uiPathSubstring = "/Playhead/Views/"

    func testInternalMissCauseIsNotReferencedInUIViews() throws {
        let appRoot = try Self.appSourceRoot()
        let regex = try NSRegularExpression(pattern: Self.forbiddenToken)

        var violations: [String] = []
        try Self.scan(root: appRoot, regex: regex, into: &violations)

        if !violations.isEmpty {
            XCTFail(
                "InternalMissCause referenced in Playhead/Views "
                + "(\(violations.count) occurrence(s)). UI must consume "
                + "`EpisodeSurfaceStatus` and render `SurfaceReason` — see "
                + "playhead-5bb3 bead spec:\n"
                + violations.sorted().joined(separator: "\n")
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
            // Only police the UI Views directory.
            guard url.path.contains(Self.uiPathSubstring) else { continue }

            let source = try String(contentsOf: url, encoding: .utf8)
            let lineNumbers = Self.scanSource(source, regex: regex)
            for lineNumber in lineNumbers {
                violations.append(
                    "\(url.lastPathComponent):\(lineNumber): "
                    + "`InternalMissCause` referenced in Playhead/Views"
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
