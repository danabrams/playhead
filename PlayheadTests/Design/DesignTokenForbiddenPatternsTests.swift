// DesignTokenForbiddenPatternsTests.swift
// Enforces "Quiet Instrument" prohibitions in two layers:
//   1. Token-name inventory: no token is *named* with a forbidden keyword.
//   2. Source-code regex sweep: no `.swift` file under the entire
//      `Playhead/` source tree *uses* a forbidden API (spring physics,
//      gradients, shimmer, purple).
//
// The source sweep is the load-bearing test — name-based assertions are
// trivially satisfied because nobody names a token "purpleGradient". The
// source sweep catches any change that introduced `LinearGradient` or
// `.spring(...)` anywhere in the app, not just in the design folder.

import XCTest
@testable import Playhead

final class DesignTokenForbiddenPatternsTests: XCTestCase {

    private static let forbidden: [String] = [
        "purple", "gradient", "shimmer", "sparkle",
        "spring", "bounce", "bouncy"
    ]

    func testInventoryContainsNoForbiddenNames() {
        for entry in DesignTokenInventory.all {
            let lower = entry.name.lowercased()
            for word in Self.forbidden {
                XCTAssertFalse(
                    lower.contains(word),
                    "Design token '\(entry.name)' (category \(entry.category)) contains forbidden keyword '\(word)'"
                )
            }
        }
    }

    func testInventoryIsNonEmpty() {
        XCTAssertFalse(DesignTokenInventory.all.isEmpty,
                       "DesignTokenInventory.all must enumerate every token")
    }

    func testInventoryIncludesAllCategories() {
        let categories = Set(DesignTokenInventory.all.map(\.category))
        XCTAssertTrue(categories.contains(.color))
        XCTAssertTrue(categories.contains(.spacing))
        XCTAssertTrue(categories.contains(.cornerRadius))
        XCTAssertTrue(categories.contains(.motion))
        XCTAssertTrue(categories.contains(.typography))
        XCTAssertTrue(categories.contains(.haptic))
    }

    // MARK: - Source-code regex sweep

    /// Forbidden API patterns. Each must NOT appear anywhere under the
    /// `Playhead/` source tree outside the explicit allowlist below.
    private static let forbiddenSourcePatterns: [(label: String, regex: String)] = [
        ("Animation.spring",          #"\.spring\("#),
        ("Animation.interpolatingSpring", #"\.interpolatingSpring"#),
        ("Animation.interactiveSpring",   #"\.interactiveSpring"#),
        ("Animation.bouncy",          #"\.bouncy"#),
        ("LinearGradient",            #"LinearGradient\("#),
        ("RadialGradient",            #"RadialGradient\("#),
        ("AngularGradient",           #"AngularGradient\("#),
        ("MeshGradient",              #"MeshGradient\("#),
        ("Color.purple",              #"Color\.purple"#),
        ("shimmer",                   #"shimmer"#),
        ("sparkle",                   #"sparkle"#),
    ]

    /// Exact lines that are allowed to mention forbidden tokens. These are
    /// the `MotionCurveKind` enum case declarations in `Theme.swift` and the
    /// switch arm that references them — declared on purpose so tests can
    /// assert their absence elsewhere. Whitelist by exact string content
    /// (after trimming whitespace) so reformatting can't accidentally
    /// expand the exemption.
    private static let allowedExactLines: Set<String> = [
        "case spring",
        "case interpolatingSpring",
        "case bouncy",
        "case .spring, .interpolatingSpring, .bouncy:",
    ]

    func testNoDesignSourceFileUsesAForbiddenAPI() throws {
        let appSources = try Self.playheadSourceFiles()
        XCTAssertFalse(appSources.isEmpty,
                       "Could not locate Playhead/**/*.swift sources from #filePath")

        for fileURL in appSources {
            let contents = try String(contentsOf: fileURL, encoding: .utf8)
            let lines = contents.components(separatedBy: "\n")

            var inBlockComment = false
            for (index, rawLine) in lines.enumerated() {
                // Strip comments (both `/* ... */` block comments spanning
                // lines and trailing `//` line comments) before pattern
                // matching so documentation mentioning forbidden APIs
                // doesn't trip the sweep. `inBlockComment` carries across
                // lines.
                let stripped = Self.stripComments(from: rawLine, inBlockComment: &inBlockComment)
                let trimmed = stripped.trimmingCharacters(in: .whitespaces)
                if trimmed.isEmpty {
                    continue
                }

                if Self.allowedExactLines.contains(trimmed) {
                    continue
                }

                for pattern in Self.forbiddenSourcePatterns {
                    if stripped.range(of: pattern.regex, options: .regularExpression) != nil {
                        XCTFail(
                            "\(fileURL.lastPathComponent):\(index + 1) — forbidden API '\(pattern.label)' "
                            + "matched on line: \(trimmed)"
                        )
                    }
                }
            }
        }
    }

    // MARK: - Comment stripping helper

    /// Strip Swift comments from a single source line. Handles:
    ///   - `/* ... */` block comments, including those that span multiple
    ///     lines. `inBlockComment` tracks open state across calls.
    ///   - `//` line comments (trailing or whole-line).
    ///
    /// This is intentionally NOT a full Swift lexer — it does not attempt
    /// to recognize `"//"` inside string literals. In practice, forbidden
    /// APIs don't appear inside strings, so the simplicity is fine.
    /// The goal is to prevent false positives from comments like
    /// `// replaced the old .spring(response:) call`, not to be
    /// bulletproof against adversarial input.
    static func stripComments(from line: String, inBlockComment: inout Bool) -> String {
        var result = ""
        let chars = Array(line)
        var i = 0
        while i < chars.count {
            if inBlockComment {
                // Look for closing "*/".
                if i + 1 < chars.count && chars[i] == "*" && chars[i + 1] == "/" {
                    inBlockComment = false
                    i += 2
                    continue
                }
                i += 1
                continue
            }
            // Not currently inside a block comment.
            if i + 1 < chars.count && chars[i] == "/" && chars[i + 1] == "*" {
                inBlockComment = true
                i += 2
                continue
            }
            if i + 1 < chars.count && chars[i] == "/" && chars[i + 1] == "/" {
                // Rest of line is a line comment — discard it.
                break
            }
            result.append(chars[i])
            i += 1
        }
        return result
    }

    // MARK: - Comment-stripping unit tests

    func testStripCommentsRemovesTrailingLineComment() {
        var inBlock = false
        let stripped = Self.stripComments(
            from: "let x = 1 // not a .spring(response:) reference",
            inBlockComment: &inBlock
        )
        XCTAssertFalse(inBlock)
        // The stripped line must no longer contain the forbidden token
        // fragment, but must still contain the real code.
        XCTAssertTrue(stripped.contains("let x = 1"))
        XCTAssertFalse(stripped.contains(".spring("))
    }

    func testStripCommentsKeepsRealCodeIntact() {
        var inBlock = false
        let stripped = Self.stripComments(
            from: "let g = LinearGradient(colors: [])",
            inBlockComment: &inBlock
        )
        XCTAssertFalse(inBlock)
        XCTAssertTrue(stripped.contains("LinearGradient("))
    }

    func testStripCommentsHandlesBlockCommentOnSingleLine() {
        var inBlock = false
        let stripped = Self.stripComments(
            from: "let y = 2 /* mentions .spring(response:) */ + 3",
            inBlockComment: &inBlock
        )
        XCTAssertFalse(inBlock, "block comment closed on same line")
        XCTAssertFalse(stripped.contains(".spring("))
        XCTAssertTrue(stripped.contains("let y = 2"))
        XCTAssertTrue(stripped.contains("+ 3"))
    }

    func testStripCommentsHandlesMultiLineBlockComment() {
        var inBlock = false
        let line1 = Self.stripComments(
            from: "let z = 4 /* open comment mentioning LinearGradient(",
            inBlockComment: &inBlock
        )
        XCTAssertTrue(inBlock, "block comment should remain open")
        XCTAssertFalse(line1.contains("LinearGradient("))
        XCTAssertTrue(line1.contains("let z = 4"))

        let line2 = Self.stripComments(
            from: "still inside with .spring(response: 1) mention",
            inBlockComment: &inBlock
        )
        XCTAssertTrue(inBlock, "still inside block comment")
        XCTAssertTrue(line2.trimmingCharacters(in: .whitespaces).isEmpty,
                      "entire line consumed by block comment")

        let line3 = Self.stripComments(
            from: "closing */ let w = 5",
            inBlockComment: &inBlock
        )
        XCTAssertFalse(inBlock, "block comment should close")
        XCTAssertTrue(line3.contains("let w = 5"))
    }

    /// End-to-end sanity: a line that mentions a forbidden API only in a
    /// trailing comment must NOT match any forbidden pattern after
    /// stripping, and a line that genuinely uses a forbidden API still
    /// must.
    func testForbiddenPatternSweepIgnoresCommentedMentions() {
        var inBlock = false
        let falsePositive = Self.stripComments(
            from: "doSomething() // replaces .spring(response:) with custom curve",
            inBlockComment: &inBlock
        )
        for pattern in Self.forbiddenSourcePatterns {
            XCTAssertNil(
                falsePositive.range(of: pattern.regex, options: .regularExpression),
                "pattern '\(pattern.label)' should NOT match stripped comment line"
            )
        }

        var inBlock2 = false
        let realUse = Self.stripComments(
            from: "let grad = LinearGradient(colors: [.red, .blue])",
            inBlockComment: &inBlock2
        )
        let matched = Self.forbiddenSourcePatterns.contains { pattern in
            realUse.range(of: pattern.regex, options: .regularExpression) != nil
        }
        XCTAssertTrue(matched, "real LinearGradient use should still trip the sweep")
    }

    /// Locate every `.swift` source file under `Playhead/` at test time by
    /// walking up from this test file's `#filePath`. The repo layout is
    /// `<repo>/PlayheadTests/Design/<thisFile>.swift` and
    /// `<repo>/Playhead/**/<sourceFile>.swift`, so 3 levels up from
    /// `#filePath` is the repo root. The sweep covers the entire app tree
    /// (App/, Views/, Services/, Models/, Persistence/, Support/,
    /// Resources/, Design/) because the forbidden-API ban applies app-wide,
    /// not just to the design folder.
    private static func playheadSourceFiles(filePath: String = #filePath) throws -> [URL] {
        let testFileURL = URL(fileURLWithPath: filePath)
        let repoRoot = testFileURL
            .deletingLastPathComponent()  // PlayheadTests/Design
            .deletingLastPathComponent()  // PlayheadTests
            .deletingLastPathComponent()  // <repo>
        let sourcesRoot = repoRoot.appendingPathComponent("Playhead", isDirectory: true)
        let fm = FileManager.default
        guard let enumerator = fm.enumerator(
            at: sourcesRoot,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            return []
        }
        var results: [URL] = []
        for case let url as URL in enumerator {
            guard url.pathExtension == "swift" else { continue }
            results.append(url)
        }
        return results.sorted { $0.path < $1.path }
    }
}
