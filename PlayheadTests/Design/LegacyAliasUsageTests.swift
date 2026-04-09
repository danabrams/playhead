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
// - Walks every `.swift` file under the `Playhead/` source tree.
// - Excludes the `Playhead/Design/` folder so that test source files inside
//   it (which mention the names as string literals) don't false-positive.

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

    /// Folders whose *contents* are exempted from the scan. Used for the
    /// `Playhead/Design/` directory which previously held the alias
    /// declarations themselves and so legitimately referenced the names.
    /// (The aliases have since been deleted, but the test history kept.)
    private static let excludedFolderComponents: Set<String> = [
        "Design",
    ]

    func testNoLegacyDesignTokenAliasesInAppSources() throws {
        let sourcesRoot = try Self.playheadSourcesRoot()
        let fm = FileManager.default

        guard let enumerator = fm.enumerator(
            at: sourcesRoot,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            XCTFail("Could not enumerate \(sourcesRoot.path)")
            return
        }

        let regexes: [(NSRegularExpression, String)] = try Self.forbiddenPatterns.map {
            (try NSRegularExpression(pattern: $0.pattern), $0.replacement)
        }

        var violations: [String] = []

        for case let url as URL in enumerator {
            guard url.pathExtension == "swift" else { continue }

            // Skip excluded folders anywhere under the Playhead/ tree.
            let components = Set(url.pathComponents)
            if !components.isDisjoint(with: Self.excludedFolderComponents) {
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

        if !violations.isEmpty {
            XCTFail(
                "Legacy design-token aliases found in view code (\(violations.count) "
                + "occurrence(s)). Replace with the semantic names:\n"
                + violations.sorted().joined(separator: "\n")
            )
        }
    }

    // MARK: - Path resolution

    /// Resolves the `Playhead/` sources root by walking up from this test file
    /// to the repository root. The test binary lives in DerivedData, so we
    /// anchor on `#filePath` which is stamped into the binary at compile time.
    private static func playheadSourcesRoot(file: StaticString = #filePath) throws -> URL {
        let thisFile = URL(fileURLWithPath: String(describing: file))
        // .../PlayheadTests/Design/LegacyAliasUsageTests.swift
        //   -> .../PlayheadTests/Design
        //     -> .../PlayheadTests
        //       -> .../<repo root>
        let repoRoot = thisFile
            .deletingLastPathComponent() // Design
            .deletingLastPathComponent() // PlayheadTests
            .deletingLastPathComponent() // repo root
        let sources = repoRoot.appendingPathComponent("Playhead", isDirectory: true)
        var isDir: ObjCBool = false
        guard FileManager.default.fileExists(atPath: sources.path, isDirectory: &isDir),
              isDir.boolValue else {
            throw NSError(
                domain: "LegacyAliasUsageTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "Playhead sources root not found at \(sources.path)"]
            )
        }
        return sources
    }
}
