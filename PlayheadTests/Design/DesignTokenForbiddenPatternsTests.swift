// DesignTokenForbiddenPatternsTests.swift
// Enforces "Quiet Instrument" prohibitions in two layers:
//   1. Token-name inventory: no token is *named* with a forbidden keyword.
//   2. Source-code regex sweep: no `Playhead/Design/*.swift` file *uses* a
//      forbidden API (spring physics, gradients, shimmer, purple).
//
// The source sweep is the load-bearing test — name-based assertions are
// trivially satisfied because nobody names a token "purpleGradient". The
// source sweep would catch a future change that introduced `LinearGradient`
// in DesignTokenCatalog.swift or `.spring(...)` in any new file.

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

    /// Forbidden API patterns. Each must NOT appear anywhere in
    /// `Playhead/Design/*.swift` outside the explicit allowlist below.
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
        let designSources = try Self.designSourceFiles()
        XCTAssertFalse(designSources.isEmpty,
                       "Could not locate Playhead/Design/*.swift sources from #filePath")

        for fileURL in designSources {
            let contents = try String(contentsOf: fileURL, encoding: .utf8)
            let lines = contents.components(separatedBy: "\n")

            for (index, rawLine) in lines.enumerated() {
                let line = rawLine
                let trimmed = line.trimmingCharacters(in: .whitespaces)

                // Skip comment-only lines so doc/comment text that names a
                // forbidden API for explanatory purposes doesn't trip the
                // sweep. The whole point of the comment ban is on actual
                // API usage.
                if trimmed.hasPrefix("//") || trimmed.hasPrefix("///") {
                    continue
                }

                if Self.allowedExactLines.contains(trimmed) {
                    continue
                }

                for pattern in Self.forbiddenSourcePatterns {
                    if line.range(of: pattern.regex, options: .regularExpression) != nil {
                        XCTFail(
                            "\(fileURL.lastPathComponent):\(index + 1) — forbidden API '\(pattern.label)' "
                            + "matched on line: \(trimmed)"
                        )
                    }
                }
            }
        }
    }

    /// Locate `Playhead/Design/*.swift` source files at test time by walking
    /// up from this test file's `#filePath`. The repo layout is
    /// `<repo>/PlayheadTests/Design/<thisFile>.swift` and
    /// `<repo>/Playhead/Design/<sourceFile>.swift`, so 3 levels up from
    /// `#filePath` is the repo root.
    private static func designSourceFiles(filePath: String = #filePath) throws -> [URL] {
        let testFileURL = URL(fileURLWithPath: filePath)
        let repoRoot = testFileURL
            .deletingLastPathComponent()  // PlayheadTests/Design
            .deletingLastPathComponent()  // PlayheadTests
            .deletingLastPathComponent()  // <repo>
        let designDir = repoRoot
            .appendingPathComponent("Playhead")
            .appendingPathComponent("Design")
        let contents = try FileManager.default.contentsOfDirectory(
            at: designDir,
            includingPropertiesForKeys: nil
        )
        return contents
            .filter { $0.pathExtension == "swift" }
            .sorted { $0.lastPathComponent < $1.lastPathComponent }
    }
}
