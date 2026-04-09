// LegacyAliasUsageTests.swift
// Regression guard: forbids any usage of the legacy design-token names
// `AppColors.text/secondary/metadata` and `CornerRadius.sm/md/lg` in view code.
//
// The semantic names (`textPrimary`, `textSecondary`, `textTertiary`, `small`,
// `medium`, `large`) landed in bead playhead-8bb. Every call site has since
// been migrated and the alias declarations have been deleted from
// `Playhead/Design/`. This test prevents any future commit from
// accidentally reintroducing the legacy names.
//
// Scope:
// - Walks every `.swift` file under both the `Playhead/` source tree and the
//   `PlayheadTests/` test tree.
// - Excludes the `Playhead/Design/` folder so that source files inside it
//   (which historically held the alias declarations) don't false-positive.
// - Excludes this very test file, which contains the alias names as string
//   literals in order to detect them.

import XCTest

final class LegacyAliasUsageTests: XCTestCase {

    private static let forbiddenPatterns: [(pattern: String, replacement: String)] = [
        (#"AppColors\.text\b"#, "AppColors.textPrimary"),
        (#"AppColors\.secondary\b"#, "AppColors.textSecondary"),
        (#"AppColors\.metadata\b"#, "AppColors.textTertiary"),
        (#"CornerRadius\.sm\b"#, "CornerRadius.small"),
        (#"CornerRadius\.md\b"#, "CornerRadius.medium"),
        (#"CornerRadius\.lg\b"#, "CornerRadius.large"),
    ]

    /// Path substrings anchoring exclusions. We intentionally match on
    /// `/Playhead/Design/` rather than the bare folder name `Design` so that
    /// a future unrelated directory named "Design" elsewhere in the tree
    /// (e.g. `Playhead/Features/Design/...`) is NOT silently exempted from
    /// the legacy-alias sweep. The design system folder previously held the
    /// alias declarations and is kept exempted for historical continuity.
    ///
    /// This test file itself is also exempted: it mentions the alias names
    /// as regex string literals in order to detect them.
    private static let excludedPathSubstrings: [String] = [
        "/Playhead/Design/",
        "/PlayheadTests/Design/LegacyAliasUsageTests.swift",
    ]

    func testNoLegacyDesignTokenAliasesInAppSources() throws {
        let (appRoot, testsRoot) = try Self.sourceRoots()
        let regexes: [(NSRegularExpression, String)] = try Self.forbiddenPatterns.map {
            (try NSRegularExpression(pattern: $0.pattern), $0.replacement)
        }

        var violations: [String] = []
        try Self.scan(root: appRoot, regexes: regexes, into: &violations)
        try Self.scan(root: testsRoot, regexes: regexes, into: &violations)

        if !violations.isEmpty {
            XCTFail(
                "Legacy design-token aliases found in code (\(violations.count) "
                + "occurrence(s)). Replace with the semantic names:\n"
                + violations.sorted().joined(separator: "\n")
            )
        }
    }

    private static func scan(
        root: URL,
        regexes: [(NSRegularExpression, String)],
        into violations: inout [String]
    ) throws {
        let fm = FileManager.default
        guard let enumerator = fm.enumerator(
            at: root,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            XCTFail("Could not enumerate \(root.path)")
            return
        }

        for case let url as URL in enumerator {
            guard url.pathExtension == "swift" else { continue }

            // Skip excluded paths (anchored substring matches).
            if Self.excludedPathSubstrings.contains(where: { url.path.contains($0) }) {
                continue
            }

            let source = try String(contentsOf: url, encoding: .utf8)
            let range = NSRange(source.startIndex..., in: source)

            for (regex, replacement) in regexes {
                let matches = regex.matches(in: source, range: range)
                for match in matches {
                    guard let swiftRange = Range(match.range, in: source) else { continue }
                    let lineNumber = source[..<swiftRange.lowerBound]
                        .reduce(into: 1) { count, ch in if ch == "\n" { count += 1 } }
                    let hit = String(source[swiftRange])
                    violations.append(
                        "\(url.lastPathComponent):\(lineNumber): legacy alias `\(hit)` — use `\(replacement)` instead"
                    )
                }
            }
        }
    }

    // MARK: - Path resolution

    /// Resolves the `Playhead/` and `PlayheadTests/` source roots by walking
    /// up from this test file to the repository root. The test binary lives
    /// in DerivedData, so we anchor on `#filePath` which is stamped into the
    /// binary at compile time.
    private static func sourceRoots(file: StaticString = #filePath) throws -> (app: URL, tests: URL) {
        let thisFile = URL(fileURLWithPath: String(describing: file))
        // .../PlayheadTests/Design/LegacyAliasUsageTests.swift
        //   -> .../PlayheadTests/Design
        //     -> .../PlayheadTests
        //       -> .../<repo root>
        let repoRoot = thisFile
            .deletingLastPathComponent() // Design
            .deletingLastPathComponent() // PlayheadTests
            .deletingLastPathComponent() // repo root
        let app = repoRoot.appendingPathComponent("Playhead", isDirectory: true)
        let tests = repoRoot.appendingPathComponent("PlayheadTests", isDirectory: true)
        let fm = FileManager.default
        for dir in [app, tests] {
            var isDir: ObjCBool = false
            guard fm.fileExists(atPath: dir.path, isDirectory: &isDir), isDir.boolValue else {
                throw NSError(
                    domain: "LegacyAliasUsageTests",
                    code: 1,
                    userInfo: [NSLocalizedDescriptionKey: "Source root not found at \(dir.path)"]
                )
            }
        }
        return (app, tests)
    }
}
