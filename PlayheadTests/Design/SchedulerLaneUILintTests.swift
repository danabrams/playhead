// SchedulerLaneUILintTests.swift
// Regression guard: forbids any mention of `SchedulerLane` — the internal
// three-lane scheduler type introduced in playhead-r835 — from Swift sources
// outside `Playhead/Services/`. The bead spec says:
//
//   "Lane names are scheduler-internal only. UI / Diagnostics / Activity copy
//   never renders 'Now'/'Soon'/'Background' verbatim. Do not add any UI."
//
// This test is the grep-based lint contract: it walks the repository's Swift
// source tree and fails if `SchedulerLane` appears in any `.swift` file
// under `Playhead/` that is not under `Playhead/Services/`. In short: every
// directory under `Playhead/` other than `Services/` is in scope. Tests
// (which reach into the scheduler with `@testable import Playhead`) are
// naturally allowed to reference the type for behavior verification.
//
// Line-comment robustness: the scanner strips `//`-style line comments
// before applying the regex so that a comment like `// SchedulerLane is
// off-limits outside Services` does not trip the lint. Block comments
// starting with `*` on a line (the /** ... */ continuation convention) are
// also skipped. String literals are NOT parsed — if someone writes a user-
// facing string containing the bare word `SchedulerLane`, that is exactly
// the kind of leakage this lint is meant to catch, so the literal should
// fail the test just like an unadorned type reference would.

import XCTest

final class SchedulerLaneUILintTests: XCTestCase {

    /// The literal token we forbid outside scheduler internals. Using a word-
    /// boundary regex prevents us from matching identifiers that merely share
    /// a prefix (e.g. `SchedulerLaneAdmission` would not match, but
    /// `SchedulerLane` alone would). The bead spec requires the *name* stay
    /// scheduler-internal, so the bare token is the right unit to police.
    private static let forbiddenToken = #"\bSchedulerLane\b"#

    /// Paths under `Playhead/` (the app target) where `SchedulerLane` is
    /// permitted to appear. This anchors on `/Playhead/Services/` specifically
    /// so that a new directory named `Services` elsewhere (e.g. a future
    /// `Playhead/Views/Services/`) would NOT be silently exempted from the
    /// lint. The scheduler's source file lives inside
    /// `Playhead/Services/PreAnalysis/`, so this covers it.
    private static let allowedAppSubstrings: [String] = [
        "/Playhead/Services/",
    ]

    /// This test file itself references `SchedulerLane` in string literals in
    /// order to detect it, so it must be exempted from the scan. We also
    /// exempt every other file inside `PlayheadTests/` — tests legitimately
    /// reference scheduler-internal types via `@testable import Playhead`,
    /// and the bead's prohibition is specifically against UI / App targets.
    private static let testsExemptFromScan: Bool = true

    func testSchedulerLaneIsNotReferencedOutsideScheduler() throws {
        let appRoot = try Self.appSourceRoot()
        let regex = try NSRegularExpression(pattern: Self.forbiddenToken)

        var violations: [String] = []
        try Self.scan(root: appRoot, regex: regex, into: &violations)

        if !violations.isEmpty {
            XCTFail(
                "SchedulerLane referenced outside Playhead/Services "
                + "(\(violations.count) occurrence(s)). The three-lane model "
                + "is scheduler-internal — see playhead-r835 bead spec:\n"
                + violations.sorted().joined(separator: "\n")
            )
        }
    }

    // MARK: - Lint-logic unit tests (FIX 3)
    //
    // These tests verify the comment-stripping logic itself so that a
    // regression in the lint engine is caught here rather than as a
    // silently-passing scan of the app target. They exercise `scanSource`
    // against synthetic strings and assert the reported violation count.

    func testLintIgnoresLineComments() throws {
        let regex = try NSRegularExpression(pattern: Self.forbiddenToken)
        let source = """
        // SchedulerLane is commented — this must not be flagged.
        let x = 1
        """
        let violations = Self.scanSource(source, regex: regex)
        XCTAssertEqual(violations, [], "Line comments must not be flagged")
    }

    func testLintIgnoresTrailingLineComments() throws {
        let regex = try NSRegularExpression(pattern: Self.forbiddenToken)
        // A real reference earlier in the line would fail (correctly), so
        // we test only the trailing-comment-only case here.
        let source = "let x = 1  // references SchedulerLane in prose"
        let violations = Self.scanSource(source, regex: regex)
        XCTAssertEqual(violations, [],
                       "Trailing line comments must not be flagged")
    }

    func testLintIgnoresBlockCommentContinuationLines() throws {
        let regex = try NSRegularExpression(pattern: Self.forbiddenToken)
        let source = """
        /**
         * SchedulerLane inside a doc-comment block must not be flagged.
         * Continuation lines begin with `*` after leading whitespace.
         */
        let x = 1
        """
        let violations = Self.scanSource(source, regex: regex)
        XCTAssertEqual(violations, [],
                       "Block-comment continuation lines must not be flagged")
    }

    func testLintFlagsRealReference() throws {
        let regex = try NSRegularExpression(pattern: Self.forbiddenToken)
        let source = "let lane = SchedulerLane.now"
        let violations = Self.scanSource(source, regex: regex)
        XCTAssertEqual(violations.count, 1,
                       "A bare SchedulerLane reference must be flagged exactly once")
        XCTAssertEqual(violations.first, 1,
                       "The violation must be reported on line 1")
    }

    func testLintFlagsReferenceFollowedByComment() throws {
        let regex = try NSRegularExpression(pattern: Self.forbiddenToken)
        let source = "let lane = SchedulerLane.now  // note"
        let violations = Self.scanSource(source, regex: regex)
        XCTAssertEqual(violations.count, 1,
                       "SchedulerLane before a trailing comment must still be flagged")
    }

    func testLintFlagsStringLiteralMention() throws {
        // Intentional: bare `SchedulerLane` inside a user-facing string
        // literal is exactly the kind of leakage this lint catches. We do
        // not try to parse string literals — the simple rule is easier to
        // audit and matches the bead spec more faithfully than a
        // lexer-based approach.
        let regex = try NSRegularExpression(pattern: Self.forbiddenToken)
        let source = #"let copy = "SchedulerLane is internal""#
        let violations = Self.scanSource(source, regex: regex)
        XCTAssertEqual(violations.count, 1,
                       "String-literal mentions of SchedulerLane must be flagged")
    }

    // MARK: - Scanner

    /// Strips line comments and block-comment continuations from `line`
    /// before the caller runs the forbidden-token regex. Returns nil if
    /// the resulting line is effectively empty (e.g. the entire line was
    /// a comment). We do NOT strip string literals — see the header
    /// comment for the rationale.
    private static func effectiveCode(onLine line: String) -> String? {
        // Block-comment continuation: a line whose first non-whitespace
        // character is `*` is part of a `/** ... */` block (or a plain
        // `/* ... */` spanning multiple lines). Treat the whole line as
        // comment.
        let trimmed = line.drop(while: { $0 == " " || $0 == "\t" })
        if trimmed.first == "*" {
            return nil
        }

        // Line comment: truncate at `//`.
        if let range = line.range(of: "//") {
            let head = line[..<range.lowerBound]
            return String(head)
        }
        return line
    }

    /// Apply the forbidden-token regex against `source` after
    /// comment-stripping. Returns the 1-based line numbers of every
    /// violation so tests can assert counts and positions.
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
            // pathExtension == "swift" is sufficient — directories and
            // symlinks do not carry a `.swift` extension, so no extra
            // `.isRegularFileKey` check is needed here.
            guard url.pathExtension == "swift" else { continue }

            // Allow the scheduler's own file and anything else under
            // `Playhead/Services/`. Any other path under `Playhead/` is
            // considered non-scheduler territory.
            if Self.allowedAppSubstrings.contains(where: { url.path.contains($0) }) {
                continue
            }

            let source = try String(contentsOf: url, encoding: .utf8)
            let lineNumbers = Self.scanSource(source, regex: regex)
            for lineNumber in lineNumbers {
                violations.append(
                    "\(url.lastPathComponent):\(lineNumber): `SchedulerLane` referenced outside Playhead/Services"
                )
            }
        }
    }

    // MARK: - Path resolution

    /// Resolves the `Playhead/` source root by walking up from this test file
    /// to the repository root. The test binary lives in DerivedData, so we
    /// anchor on `#filePath` which is stamped into the binary at compile time.
    private static func appSourceRoot(file: StaticString = #filePath) throws -> URL {
        let thisFile = URL(fileURLWithPath: String(describing: file))
        // .../PlayheadTests/Design/SchedulerLaneUILintTests.swift
        //   -> .../PlayheadTests/Design
        //     -> .../PlayheadTests
        //       -> .../<repo root>
        let repoRoot = thisFile
            .deletingLastPathComponent() // Design
            .deletingLastPathComponent() // PlayheadTests
            .deletingLastPathComponent() // repo root
        let app = repoRoot.appendingPathComponent("Playhead", isDirectory: true)
        var isDir: ObjCBool = false
        guard FileManager.default.fileExists(atPath: app.path, isDirectory: &isDir),
              isDir.boolValue else {
            throw NSError(
                domain: "SchedulerLaneUILintTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "App source root not found at \(app.path)"]
            )
        }
        return app
    }
}
