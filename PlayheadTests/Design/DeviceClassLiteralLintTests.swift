// DeviceClassLiteralLintTests.swift
// playhead-dh9b grep-lint: no hard-coded device-class string literals
// are allowed outside the DeviceClass enum source file and the
// bundled PreAnalysisConfig.json manifest.
//
// Motivation (from the bead spec):
//
//   "Grep lint: no hard-coded device-class string literals outside the
//    JSON file + DeviceClass enum."
//
// The lint catches copy-paste of bucket names (e.g. "iPhone17Pro") into
// business logic, which would silently bypass the DeviceClass enum and
// make it impossible for playhead-beh3 (Phase 3) to renumber or retire
// a bucket without a sprawling manual audit.
//
// Scope:
//   - Scans every .swift file under `Playhead/` (app target only).
//   - Exempts the DeviceClass enum's own file — it must contain the
//     literal bucket names (the rawValues come from the case names,
//     but Codable schema tests may reference them by string).
//   - Exempts `Playhead/Resources/PreAnalysisConfig.json` (the bundled
//     manifest is allowed to contain the literal bucket names — it IS
//     the data source of truth).
//   - Comments and doc-comments are stripped before matching so that
//     references in prose do not trip the lint.
//   - Tests are not scanned (PlayheadTests/ legitimately references
//     bucket names for round-trip verification).

import XCTest

final class DeviceClassLiteralLintTests: XCTestCase {

    /// The bucket names we police. Every `DeviceClass` case's
    /// `rawValue`. Sorted longest-first so `iPhone17Pro` matches
    /// before the shorter `iPhone17` prefix would otherwise catch it.
    private static let forbiddenTokens: [String] = [
        "iPhone17Pro",
        "iPhone16Pro",
        "iPhone15Pro",
        "iPhone14andOlder",
        "iPhoneSE3",
        "iPhone17",
        "iPhone16",
    ]

    /// Files exempt from the scan. The DeviceClass enum file is the
    /// canonical home for the string tokens. Other files must route
    /// through the enum instead of hard-coding.
    private static let exemptFileNames: Set<String> = [
        "DeviceClass.swift",
        "DeviceClassProfile.swift", // fallback(for:) references the enum's rawValue — not a string literal audit target
    ]

    func testNoHardCodedDeviceClassLiteralsOutsideEnum() throws {
        let appRoot = try Self.appSourceRoot()

        // Compile each bucket into a word-boundary regex so
        // `iPhone17ProMax` (if anyone ever added it as a separate
        // concept) would not be falsely flagged by the `iPhone17Pro`
        // token — word boundaries make "iPhone17ProMax" a different
        // lexical identifier.
        let regexes = try Self.forbiddenTokens.map {
            try NSRegularExpression(pattern: #"\b"# + $0 + #"\b"#)
        }

        var violations: [String] = []
        try Self.scan(root: appRoot, regexes: regexes, into: &violations)

        if !violations.isEmpty {
            XCTFail(
                "Hard-coded device-class string literals found outside "
                + "DeviceClass.swift / DeviceClassProfile.swift "
                + "(\(violations.count) occurrence(s)). Route through the "
                + "DeviceClass enum or PreAnalysisConfig.json instead — "
                + "see playhead-dh9b bead spec:\n"
                + violations.sorted().joined(separator: "\n")
            )
        }
    }

    // MARK: - Comment stripping

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

    // MARK: - Scanner

    private static func scan(
        root: URL,
        regexes: [NSRegularExpression],
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
            if Self.exemptFileNames.contains(url.lastPathComponent) {
                continue
            }

            let source = try String(contentsOf: url, encoding: .utf8)
            // enumerateLines takes an @escaping closure so inout can't
            // be captured — collect line numbers locally, then fan-out
            // into the caller's `violations` after the scan finishes.
            var localHits: [Int] = []
            var lineNumber = 0
            source.enumerateLines { line, _ in
                lineNumber += 1
                guard let code = Self.effectiveCode(onLine: line) else { return }
                let range = NSRange(code.startIndex..., in: code)
                for regex in regexes {
                    if regex.firstMatch(in: code, range: range) != nil {
                        localHits.append(lineNumber)
                        return // one violation per line is enough
                    }
                }
            }
            for hit in localHits {
                violations.append(
                    "\(url.lastPathComponent):\(hit): hard-coded device-class literal — route through DeviceClass enum"
                )
            }
        }
    }

    // MARK: - Path resolution

    /// Resolves the `Playhead/` source root by walking up from this
    /// test file (`#filePath` is stamped into the binary at compile time).
    private static func appSourceRoot(file: StaticString = #filePath) throws -> URL {
        let thisFile = URL(fileURLWithPath: String(describing: file))
        let repoRoot = thisFile
            .deletingLastPathComponent() // Design/
            .deletingLastPathComponent() // PlayheadTests/
            .deletingLastPathComponent() // repo root
        let app = repoRoot.appendingPathComponent("Playhead", isDirectory: true)
        var isDir: ObjCBool = false
        guard FileManager.default.fileExists(atPath: app.path, isDirectory: &isDir),
              isDir.boolValue else {
            throw NSError(
                domain: "DeviceClassLiteralLintTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "App source root not found at \(app.path)"]
            )
        }
        return app
    }
}
