// NetworkIsolationSchemeSourceCanaryTests.swift
//
// review/v0.5-head-polish M1 source canary: defense in depth for the
// `RecordingURLProtocol.networkSchemes` filter in
// `NetworkIsolationTests.swift`. That filter narrows the recorder to
// http/https/ws/wss so foreign-suite traffic on synthetic in-process
// schemes (e.g. `playhead-progressive://`) doesn't poison the gate.
// The narrowing is sound under today's URL-scheme inventory, but the
// contract "no production code introduces a network exfiltration via a
// non-allowlisted scheme" is invisible: a future commit adding
// `file://`, `data://`, `ftp://`, or a fresh custom scheme could ship
// without the recorder ever noticing.
//
// This canary scans `Playhead/**/*.swift` for any URL-scheme literal
// (any string of the form `<scheme>://`) and asserts the resulting
// unique set is a subset of two known buckets:
//
//   1. Real-network schemes that the recorder MUST catch.
//   2. Synthetic / in-process schemes that the recorder MUST ignore by
//      design (resource-loader stubs, deep links, doc-comment URLs).
//
// If a new scheme appears, this test fails loudly, forcing the author
// to either:
//   * Add it to the network-scheme allowlist AND extend
//     `RecordingURLProtocol.networkSchemes` accordingly, or
//   * Add it to the in-process allowlist AND verify the new scheme is
//     genuinely synthetic (no real-network round-trip).
//
// XCTest-shaped per project memory `xctestplan_swift_testing_limitation`
// (Swift Testing identifiers are silently ignored by xctestplan
// selectedTests/skippedTests).

import Foundation
import XCTest

final class NetworkIsolationSchemeSourceCanaryTests: XCTestCase {

    /// Schemes the privacy gate MUST observe. Each must also appear in
    /// `NetworkIsolationTests.RecordingURLProtocol.networkSchemes` so
    /// the recorder records traffic over them rather than ignoring it.
    private static let realNetworkSchemes: Set<String> = [
        "http",
        "https",
        "ws",
        "wss",
    ]

    /// Schemes the recorder MUST NOT record. Each is either
    /// in-process synthetic (URLProtocol-handled local), a deep-link
    /// scheme that never crosses URLSession, or a `file://` URL that
    /// references a local audio asset. Adding to this list is a
    /// deliberate act: a reviewer must confirm the new scheme is
    /// genuinely synthetic before extending it.
    ///
    /// Current entries:
    ///   * `playhead-progressive` — `AVAssetResourceLoader` shim used by
    ///     `PlaybackTransport` to drive a custom progressive download.
    ///     Loopback only; never leaves the device.
    ///   * `playhead` — universal-link scheme for transcript-quote deep
    ///     links (`playhead://episode/<id>?t=<sec>`). Routed by
    ///     SwiftUI's `.onOpenURL`, never via URLSession.
    ///   * `file` — local audio assets and doc-comment references to
    ///     `file://` paths. Local I/O only.
    private static let inProcessSchemes: Set<String> = [
        "playhead-progressive",
        "playhead",
        "file",
    ]

    /// Combined allowlist. The canary fails on any scheme outside this
    /// union.
    private static var allowedSchemes: Set<String> {
        realNetworkSchemes.union(inProcessSchemes)
    }

    /// Match `<scheme>://` where scheme is a sequence of letters,
    /// digits, `+`, `-`, or `.`. Anchored at a non-scheme boundary on
    /// the left so we don't accept fragments like `xfoo://` from
    /// `someprefixfoo://` runs inside identifiers. RFC 3986 says
    /// schemes start with a letter and may continue with letters,
    /// digits, `+`, `-`, `.`. We additionally require a leading
    /// non-letter to anchor at a real start.
    private static let schemePattern = #"(?:^|[^A-Za-z0-9+\-.])([A-Za-z][A-Za-z0-9+\-.]*)://"#

    func testProductionCodeUsesOnlyAllowlistedURLSchemes() throws {
        let production = try Self.collectSwiftFiles(under: Self.productionRoot())
        XCTAssertGreaterThan(
            production.count,
            10,
            """
            Source canary aborted: only \(production.count) production .swift \
            files were enumerated under `Playhead/`. The walker is broken; \
            this canary has nothing to scan and would silently pass.
            """
        )

        let (observedSchemes, firstSightingByScheme) = try Self.scanForSchemes(in: production)

        let unexpected = observedSchemes.subtracting(Self.allowedSchemes)
        XCTAssertTrue(
            unexpected.isEmpty,
            """
            Production code under `Playhead/` introduced URL scheme(s) not on \
            the allowlist: \(unexpected.sorted()) \
            (first-seen: \(unexpected.sorted().map { "\($0)→\(firstSightingByScheme[$0] ?? "?")" })).

            review/v0.5-head-polish M1: every URL scheme used in production \
            must be classified as either real-network (recorder MUST catch) \
            or in-process synthetic (recorder MUST ignore).

            If the new scheme is real-network: add it to BOTH \
            `realNetworkSchemes` here AND \
            `RecordingURLProtocol.networkSchemes` in NetworkIsolationTests.swift, \
            and verify the four phase tests still pass.

            If the new scheme is in-process synthetic: add it to \
            `inProcessSchemes` here with a one-line comment explaining what \
            handles it (URLProtocol class, AVAssetResourceLoader, deep-link \
            router, etc.) and why it cannot leave the device.

            Do NOT just add it to the allowlist without classifying — that \
            defeats the purpose of the canary.
            """
        )

        XCTAssertFalse(
            observedSchemes.isEmpty,
            """
            Source canary aborted: no URL schemes were observed in \(production.count) \
            production .swift files. The regex has gone blind; production code \
            uses URL literals like `URL(string: "https://...")` and at minimum \
            `https` should appear. Verify `schemePattern` and re-run.
            """
        )
    }

    /// Positive control: the regex MUST match the schemes we expect to
    /// see in production. Without this, a regex regression that lets
    /// the test silently pass on zero matches would slip through.
    func testRegexMatchesKnownProductionSchemeShapes() throws {
        let regex = try NSRegularExpression(pattern: Self.schemePattern, options: [])
        let positives: [(input: String, expected: String)] = [
            ("URL(string: \"https://itunes.apple.com/search\")!", "https"),
            ("let ns = \"http://www.itunes.com/dtds/podcast-1.0.dtd\"", "http"),
            ("URL(string: \"playhead-progressive://stub/sentinel.mp3\")!", "playhead-progressive"),
            ("playhead://episode/abc?t=42", "playhead"),
            ("file:///Users/foo.m4a", "file"),
            ("ws://localhost:8080", "ws"),
            ("wss://localhost:443", "wss"),
        ]
        for (input, expected) in positives {
            let range = NSRange(input.startIndex..., in: input)
            guard let match = regex.firstMatch(in: input, range: range),
                  match.numberOfRanges >= 2,
                  let schemeRange = Range(match.range(at: 1), in: input)
            else {
                XCTFail("schemePattern did not match positive fixture: \(input)")
                continue
            }
            XCTAssertEqual(
                String(input[schemeRange]).lowercased(),
                expected,
                "schemePattern matched \(input) but extracted the wrong scheme"
            )
        }
    }

    /// Negative controls: the regex MUST NOT match identifier-like
    /// runs that aren't real URL schemes. Otherwise a code chunk like
    /// `foo://bar` inside a comment-stripped string could slip past as
    /// a "scheme."
    func testRegexRejectsNonSchemeShapes() throws {
        let regex = try NSRegularExpression(pattern: Self.schemePattern, options: [])
        let negatives: [String] = [
            // No scheme prefix at all.
            "let url = \"//example.com/foo\"",
            // Trailing colon-slash without scheme.
            "://nope",
        ]
        for input in negatives {
            let range = NSRange(input.startIndex..., in: input)
            XCTAssertNil(
                regex.firstMatch(in: input, range: range),
                "schemePattern over-matched on negative fixture: \(input)"
            )
        }
    }

    /// review/v0.5-head-polish C2 L-D: pin the boundary-class semantics
    /// at identifier-adjacent positions. The regex's scheme-class
    /// `[A-Za-z][A-Za-z0-9+\-.]*` includes `.`, `+`, `-`, and digits.
    /// On inputs like `bar.https://baz` the regex MUST capture the
    /// *full* prefix (`bar.https`) — not the bare `https` that would
    /// silently slip onto the production allowlist (since `https` is
    /// already there). If a future commit shrinks the scheme-class,
    /// these fixtures change shape and force a reviewer to think about
    /// what was lost.
    func testRegexCapturesFullPrefixAtIdentifierBoundaries() throws {
        let regex = try NSRegularExpression(pattern: Self.schemePattern, options: [])
        let fixtures: [(input: String, expected: String)] = [
            // Period: property-access shape — full prefix must be captured
            // so the canary flags `bar.https` as unexpected (not waved
            // through as plain `https`).
            ("bar.https://example.com", "bar.https"),
            // Plus sign: RFC 3986 scheme-body char (e.g. `git+https`).
            ("git+https://example.com", "git+https"),
            // Hyphen: also a scheme-body char (e.g. `x-custom`).
            ("x-https://example.com", "x-https"),
            // Letter prefix: prevents `httpsxhttps://` style identifiers
            // from leaking as bare `https`.
            ("foohttps://example.com", "foohttps"),
            // Digit in scheme body (allowed after the leading letter).
            ("v2https://example.com", "v2https"),
        ]
        for (input, expected) in fixtures {
            let range = NSRange(input.startIndex..., in: input)
            guard let match = regex.firstMatch(in: input, range: range),
                  match.numberOfRanges >= 2,
                  let schemeRange = Range(match.range(at: 1), in: input)
            else {
                XCTFail("schemePattern did not match identifier-boundary fixture: \(input)")
                continue
            }
            XCTAssertEqual(
                String(input[schemeRange]).lowercased(),
                expected,
                """
                schemePattern shrank the captured scheme on \(input). \
                Expected `\(expected)` (full identifier-adjacent prefix), \
                got `\(String(input[schemeRange]))`. The boundary class \
                appears to have regressed — `https` would now leak through \
                the allowlist as if it were a real scheme reference.
                """
            )
        }
    }

    /// review/v0.5-head-polish C2 MT-2: end-to-end integration test
    /// that the *canary itself* — walker + regex + allowlist join —
    /// actually catches a forbidden scheme. Without this, a future
    /// refactor could break any one link (walker enumerates 0 files,
    /// regex compiles to a no-op, allowlist set inadvertently contains
    /// every scheme) and the production test would silently pass.
    ///
    /// Builds a synthetic `.swift` file under a temp directory whose
    /// only URL literal uses `ftp://` (deliberately *not* in the
    /// allowlist), runs the same scan helper the production test uses,
    /// and asserts the unexpected-scheme set contains `ftp`.
    func testCanaryCatchesForbiddenSchemeInSyntheticFile() throws {
        let tempRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("NetworkIsolationCanary-MT2-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tempRoot, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempRoot) }

        let fixturePath = tempRoot.appendingPathComponent("Leak.swift")
        let fixtureBody = """
            import Foundation
            // Synthetic exfil — must be flagged.
            let leak = URL(string: "ftp://leak.example.com/data")!
            """
        try fixtureBody.write(to: fixturePath, atomically: true, encoding: .utf8)

        let files = try Self.collectSwiftFiles(under: tempRoot)
        XCTAssertEqual(
            files.count,
            1,
            "Walker should have found exactly the synthetic Leak.swift, got \(files.count) files"
        )

        let (observed, _) = try Self.scanForSchemes(in: files)
        XCTAssertTrue(
            observed.contains("ftp"),
            "Scan helper failed to extract `ftp` from `URL(string: \"ftp://...\")` — regex or walker is broken"
        )

        let unexpected = observed.subtracting(Self.allowedSchemes)
        XCTAssertTrue(
            unexpected.contains("ftp"),
            """
            Allowlist join is broken: scanned schemes \(observed.sorted()) \
            against allowlist \(Self.allowedSchemes.sorted()) and `ftp` did \
            not show up as unexpected. Either the allowlist now contains \
            `ftp` (don't!) or the set-difference logic regressed.
            """
        )
    }

    /// Resolve the repo's `Playhead/` production root.
    private static func productionRoot() throws -> URL {
        guard let root = SwiftSourceInspector.repositoryRoot(from: #filePath) else {
            throw NSError(
                domain: "NetworkIsolationSchemeSourceCanary",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "could not locate repo root"]
            )
        }
        return root.appendingPathComponent("Playhead", isDirectory: true)
    }

    /// Walk an arbitrary directory and collect every `.swift` file.
    /// Mirrors the file-walker contract used by other source canaries
    /// (load by repo-relative path). Parametric on the directory URL so
    /// the production canary and the temp-dir integration test
    /// (review/v0.5-head-polish C2 MT-2) share one walker. Skips dot-
    /// directories so build-tree artifacts don't leak in.
    private static func collectSwiftFiles(under root: URL) throws -> [URL] {
        let fm = FileManager.default
        guard let enumerator = fm.enumerator(
            at: root,
            includingPropertiesForKeys: [.isRegularFileKey, .isDirectoryKey],
            options: [.skipsHiddenFiles]
        ) else {
            throw NSError(
                domain: "NetworkIsolationSchemeSourceCanary",
                code: 2,
                userInfo: [NSLocalizedDescriptionKey: "could not enumerate \(root.path)"]
            )
        }
        var results: [URL] = []
        for case let url as URL in enumerator {
            let resourceValues = try url.resourceValues(forKeys: [.isRegularFileKey])
            if resourceValues.isRegularFile == true,
               url.pathExtension == "swift" {
                results.append(url)
            }
        }
        return results
    }

    /// Run the comment-stripped scheme-scan over `files` and return the
    /// observed scheme set plus a first-sighting map (scheme →
    /// originating filename). Extracted so the production canary and
    /// the synthetic-fixture integration test share one scanner.
    private static func scanForSchemes(in files: [URL]) throws -> (Set<String>, [String: String]) {
        let regex = try NSRegularExpression(pattern: schemePattern, options: [])
        var observed: Set<String> = []
        var firstSighting: [String: String] = [:]
        for fileURL in files {
            let raw = try String(contentsOf: fileURL, encoding: .utf8)
            // Strip comments so a doc-line like
            // `/// see https://example.com` doesn't pin a scheme that
            // production code never actually uses. Strings are
            // preserved because that's where URL literals actually
            // live.
            let stripped = SwiftSourceInspector.strippingComments(raw)
            let range = NSRange(stripped.startIndex..., in: stripped)
            regex.enumerateMatches(in: stripped, range: range) { match, _, _ in
                guard let match,
                      match.numberOfRanges >= 2,
                      let schemeRange = Range(match.range(at: 1), in: stripped)
                else { return }
                let scheme = String(stripped[schemeRange]).lowercased()
                observed.insert(scheme)
                if firstSighting[scheme] == nil {
                    firstSighting[scheme] = fileURL.lastPathComponent
                }
            }
        }
        return (observed, firstSighting)
    }
}
