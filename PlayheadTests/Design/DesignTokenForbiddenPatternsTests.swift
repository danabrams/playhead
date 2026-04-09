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

    // MARK: - Inventory completeness

    /// Namespaces that `DesignTokenInventory` claims to enumerate completely,
    /// paired with the inventory list they must match and an allowlist of
    /// static members that exist in the namespace but are intentionally NOT
    /// tracked as tokens (private helpers, internal descriptors, etc.).
    ///
    /// The sweep opens `Playhead/Design/<file>.swift`, locates the opening
    /// brace of `enum <namespace>`, walks forward brace-balanced to the
    /// matching close, and collects every non-`private` `static let <name>`
    /// / `static var <name>` declaration inside. Each collected name must
    /// either appear in the inventory list or be in the allowlist —
    /// otherwise a new token has slipped into the design system without a
    /// corresponding inventory entry and will silently escape the
    /// forbidden-name sweep above.
    private struct InventoryNamespace {
        let typeName: String
        let fileName: String
        let inventoryNames: Set<String>
        /// Non-token statics that live in the same namespace (descriptors,
        /// internal helpers, etc.).
        let allowlist: Set<String>
    }

    private static let inventoryNamespaces: [InventoryNamespace] = [
        InventoryNamespace(
            typeName: "Palette",
            fileName: "Colors.swift",
            inventoryNames: Set(DesignTokenInventory.colors.map(\.name)),
            allowlist: []
        ),
        InventoryNamespace(
            typeName: "AppColors",
            fileName: "Colors.swift",
            inventoryNames: Set(DesignTokenInventory.colors.map(\.name)),
            allowlist: []
        ),
        InventoryNamespace(
            typeName: "Spacing",
            fileName: "Theme.swift",
            inventoryNames: Set(DesignTokenInventory.spacing.map(\.name)),
            allowlist: []
        ),
        InventoryNamespace(
            typeName: "CornerRadius",
            fileName: "Theme.swift",
            inventoryNames: Set(DesignTokenInventory.cornerRadii.map(\.name)),
            allowlist: []
        ),
        InventoryNamespace(
            typeName: "Motion",
            fileName: "Theme.swift",
            inventoryNames: Set(DesignTokenInventory.motion.map(\.name)),
            // `Motion` exposes both the user-facing `Animation` tokens
            // (quick, standard, deliberate, preciseEase, transport) and
            // the parallel `*Descriptor` constants that tests introspect
            // to prove we aren't using spring physics. The descriptors
            // share the same stems as the animations and are deliberately
            // not duplicated in the inventory.
            allowlist: [
                "quickDescriptor",
                "standardDescriptor",
                "deliberateDescriptor",
                "preciseEaseDescriptor",
                "transportDescriptor"
            ]
        ),
        InventoryNamespace(
            typeName: "AppTypography",
            fileName: "Typography.swift",
            inventoryNames: Set(DesignTokenInventory.typography.map(\.name)),
            allowlist: []
        )
    ]

    func testInventoryMatchesDeclaredStaticTokens() throws {
        let designDir = try Self.designSourceDirectory()

        // First pass: collect declared static names from every tracked
        // namespace, and run the "declared must be in inventory" check.
        var declaredByNamespace: [String: Set<String>] = [:]
        // Union of declared names that belong to a given inventory category.
        // Keyed by a stable identity — we use "inventoryNames set identity"
        // encoded as a sorted comma-joined string, because multiple
        // namespaces (e.g. Palette + AppColors) can legitimately share the
        // same inventory list and between them cover all of its entries.
        var declaredForInventoryList: [String: Set<String>] = [:]

        for namespace in Self.inventoryNamespaces {
            let fileURL = designDir.appendingPathComponent(namespace.fileName)
            let contents = try String(contentsOf: fileURL, encoding: .utf8)
            let declared = try Self.extractStaticLetNames(
                from: contents,
                inTypeNamed: namespace.typeName
            )
            declaredByNamespace[namespace.typeName] = declared

            XCTAssertFalse(
                declared.isEmpty,
                "Could not locate any static members inside `\(namespace.typeName)` in \(namespace.fileName). "
                + "The brace-walking parser probably needs to be taught about a new declaration shape."
            )

            for name in declared {
                if namespace.allowlist.contains(name) { continue }
                XCTAssertTrue(
                    namespace.inventoryNames.contains(name),
                    "Design token `\(namespace.typeName).\(name)` exists in "
                    + "\(namespace.fileName) but is NOT listed in `DesignTokenInventory`. "
                    + "Either add it to the inventory (so the forbidden-name sweep "
                    + "covers it) or add it to the allowlist in "
                    + "DesignTokenForbiddenPatternsTests.inventoryNamespaces if it is "
                    + "intentionally not a public token."
                )
            }

            // Accumulate coverage for the reverse "inventory must be
            // declared somewhere" check below. Multiple namespaces that
            // share the same inventory list (Palette + AppColors both map
            // to colors) are unioned so each entry needs to show up in
            // at least ONE of them.
            let listKey = namespace.inventoryNames.sorted().joined(separator: ",")
            declaredForInventoryList[listKey, default: []].formUnion(declared)
        }

        // Reverse check: every inventory entry must be declared as a
        // `static let` in at least one of the namespaces that maps to
        // that inventory list. A missing declaration means the inventory
        // has gone stale (token deleted but entry left behind).
        for namespace in Self.inventoryNamespaces {
            let listKey = namespace.inventoryNames.sorted().joined(separator: ",")
            let covered = declaredForInventoryList[listKey] ?? []
            for invName in namespace.inventoryNames {
                XCTAssertTrue(
                    covered.contains(invName),
                    "Inventory lists `\(invName)` (category shared by "
                    + "\(namespace.typeName)) but no matching `static let \(invName)` "
                    + "exists in any of the namespaces that map to that inventory list. "
                    + "The inventory is stale."
                )
            }
        }
    }

    // MARK: Source-walk helpers

    /// Extracts the set of non-private `static let` / `static var` names
    /// declared directly inside the top-level `enum <typeName>` (or struct)
    /// in the given Swift source. The parser is intentionally simple: it
    /// finds the first `enum <typeName>` / `struct <typeName>` header,
    /// walks forward to the opening brace, then scans brace-balanced until
    /// it hits the matching close. Only declarations at brace-depth 1
    /// (directly inside the type body, not inside nested types or
    /// functions) are collected. Line comments and block comments are
    /// stripped via the same helper the forbidden-patterns sweep uses so
    /// a commented-out `// static let foo` doesn't trip the check.
    static func extractStaticLetNames(
        from source: String,
        inTypeNamed typeName: String
    ) throws -> Set<String> {
        // Strip comments line-by-line first so brace counting and the
        // static-let regex operate on clean code.
        let rawLines = source.components(separatedBy: "\n")
        var inBlockComment = false
        var cleanedLines: [String] = []
        cleanedLines.reserveCapacity(rawLines.count)
        for line in rawLines {
            cleanedLines.append(stripComments(from: line, inBlockComment: &inBlockComment))
        }
        let cleaned = cleanedLines.joined(separator: "\n")

        // Locate "enum <name>" or "struct <name>" header. We accept any
        // whitespace between the keyword and the name but require a word
        // boundary after the name so `enum Motion` doesn't match
        // `enum MotionCurveKind`.
        let headerPattern = #"(?:enum|struct)\s+\#(typeName)\b"#
        guard let headerRange = cleaned.range(of: headerPattern, options: .regularExpression) else {
            return []
        }
        // Walk forward to the first `{`.
        guard let openBraceIndex = cleaned[headerRange.upperBound...].firstIndex(of: "{") else {
            return []
        }

        // Brace-walk to the matching close, collecting `static let <name>`
        // declarations that occur at brace depth 1 (directly in the type
        // body) and are not prefixed with `private` / `fileprivate`.
        var depth = 0
        var currentLineStart = cleaned.index(after: openBraceIndex)
        var cursor = openBraceIndex
        var bodyLines: [(depth: Int, text: String)] = []
        var lineBuffer = ""
        var bodyDepthAtLineStart = 1  // just after the opening brace
        cursor = cleaned.index(after: openBraceIndex)
        depth = 1
        _ = currentLineStart
        while cursor < cleaned.endIndex {
            let ch = cleaned[cursor]
            if ch == "{" {
                depth += 1
            } else if ch == "}" {
                depth -= 1
                if depth == 0 {
                    // Flush final line.
                    bodyLines.append((bodyDepthAtLineStart, lineBuffer))
                    break
                }
            } else if ch == "\n" {
                bodyLines.append((bodyDepthAtLineStart, lineBuffer))
                lineBuffer = ""
                bodyDepthAtLineStart = depth
                cursor = cleaned.index(after: cursor)
                continue
            }
            lineBuffer.append(ch)
            cursor = cleaned.index(after: cursor)
        }

        var names: Set<String> = []
        // Match `static let foo` / `static var foo`, optionally preceded
        // by other modifiers (but NOT `private` / `fileprivate`). We use
        // a two-step regex: first filter out private lines, then extract
        // the identifier.
        let declPattern = #"^\s*(?!.*\bprivate\b)(?!.*\bfileprivate\b)(?:[a-zA-Z@]+\s+)*static\s+(?:let|var)\s+([A-Za-z_][A-Za-z0-9_]*)"#
        let regex = try NSRegularExpression(pattern: declPattern, options: [])
        for (lineDepth, line) in bodyLines {
            // Only direct members (depth 1 relative to the type body —
            // which is `bodyDepthAtLineStart == 1`).
            guard lineDepth == 1 else { continue }
            let range = NSRange(line.startIndex..<line.endIndex, in: line)
            guard let match = regex.firstMatch(in: line, options: [], range: range) else {
                continue
            }
            if match.numberOfRanges >= 2, let r = Range(match.range(at: 1), in: line) {
                names.insert(String(line[r]))
            }
        }
        return names
    }

    private static func designSourceDirectory(filePath: String = #filePath) throws -> URL {
        let testFileURL = URL(fileURLWithPath: filePath)
        let repoRoot = testFileURL
            .deletingLastPathComponent()  // PlayheadTests/Design
            .deletingLastPathComponent()  // PlayheadTests
            .deletingLastPathComponent()  // <repo>
        return repoRoot
            .appendingPathComponent("Playhead", isDirectory: true)
            .appendingPathComponent("Design", isDirectory: true)
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
