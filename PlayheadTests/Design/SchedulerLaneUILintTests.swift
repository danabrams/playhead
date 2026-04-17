// SchedulerLaneUILintTests.swift
// Regression guard: forbids any mention of `SchedulerLane` — the internal
// three-lane scheduler type introduced in playhead-r835 — from Swift sources
// outside `Playhead/Services/`. The bead spec says:
//
//   "Lane names are scheduler-internal only. UI / Diagnostics / Activity copy
//   never renders 'Now'/'Soon'/'Background' verbatim. Do not add any UI."
//
// This test is the grep-based lint contract: it walks the repository's Swift
// source tree and fails if `SchedulerLane` appears anywhere under
// `Playhead/Views/`, `Playhead/App/`, `Playhead/Models/`, `Playhead/Resources/`,
// `Playhead/Support/`, or `Playhead/Design/`. Tests (which reach into the
// scheduler with `@testable import Playhead`) are naturally allowed to
// reference the type for behavior verification.

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

    private static func scan(
        root: URL,
        regex: NSRegularExpression,
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

            // Allow the scheduler's own file and anything else under
            // `Playhead/Services/`. Any other path under `Playhead/` is
            // considered non-scheduler territory.
            if Self.allowedAppSubstrings.contains(where: { url.path.contains($0) }) {
                continue
            }

            let source = try String(contentsOf: url, encoding: .utf8)
            let range = NSRange(source.startIndex..., in: source)
            let matches = regex.matches(in: source, range: range)
            for match in matches {
                guard let swiftRange = Range(match.range, in: source) else { continue }
                let lineNumber = source[..<swiftRange.lowerBound]
                    .reduce(into: 1) { count, ch in if ch == "\n" { count += 1 } }
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
