// QueueViewModelSourceCanaryTests.swift
//
// skeptical-review-cycle-18 L-5 source canary: pin the load-bearing
// `[String]` (Array) typing of the predicate-input parameter inside
// `QueueViewModel.fetchEpisodeMap(keys:)`.
//
// Cycle-7 H1 / cycle-8 M3 documented the underlying constraint:
// SwiftData's `#Predicate` translator handles `Array.contains` reliably
// across iOS / macOS Catalyst builds, but `Set.contains` can fall back
// to a per-row in-memory scan or fail to translate at all on certain
// toolchain combinations — silently emptying the dependent UI surface
// (Activity widget for the original case; the Queue list here). The
// cycle-8 M3 propagation comment in `QueueViewModel.swift:90` records
// the rationale; this canary turns that rationale into a build-time
// regression net mirror-matching the existing
// `ActivitySnapshotProviderSourceCanaryTests`.
//
// XCTest so the canary is filterable from the test plan
// (`xctestplan selectedTests/skippedTests` silently ignores Swift
// Testing identifiers — see project memory
// `xctestplan_swift_testing_limitation`).

import XCTest

final class QueueViewModelSourceCanaryTests: XCTestCase {

    /// Cycle-18 L-5: the `keys` parameter on `fetchEpisodeMap(keys:)`
    /// MUST be typed as `[String]` (Array), and the `#Predicate` body
    /// MUST consume that local via `keys.contains($0.canonicalEpisodeKey)`.
    /// We assert three things:
    ///   1. `keys: [String]` — the parameter declaration is whitespace-
    ///      tolerantly Array-typed.
    ///   2. `keys: Set<String>` is NOT present — guards against a
    ///      "tightening" refactor that flips the type.
    ///   3. The predicate body references `keys.contains(...)` — guards
    ///      against a future refactor that introduces a sibling `Set`
    ///      local and accidentally swaps it into the predicate.
    func testFetchEpisodeMapKeysParameterStaysArrayTyped() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Views/Queue/QueueViewModel.swift"
        )
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        let arrayParamRegex = try NSRegularExpression(pattern: Self.arrayParamPattern)
        XCTAssertNotNil(
            arrayParamRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `QueueViewModel.fetchEpisodeMap(...)` no longer declares \
            `keys: [String]` (Array typing). Cycle-7 H1 / cycle-8 M3: \
            SwiftData's `#Predicate` translator handles `Array.contains` \
            reliably across iOS / macOS Catalyst builds, but \
            `Set.contains` can silently fail to translate, emptying the \
            Queue list. Restore Array typing OR update this canary if \
            the predicate-input shape intentionally moved (and the new \
            shape has been validated against the same toolchain matrix).
            """
        )

        let setParamRegex = try NSRegularExpression(pattern: Self.setParamPattern)
        XCTAssertNil(
            setParamRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `QueueViewModel.fetchEpisodeMap(...)` re-introduced \
            `keys: Set<String>` (whitespace-tolerant match). Cycle-8 M3 \
            documented why this is unsafe: SwiftData's `#Predicate` \
            translation against a Swift `Set` is fragile across \
            toolchain revs and silently empties the Queue list on the \
            bad combinations. Use `keys: [String]` instead.
            """
        )

        let predicateRegex = try NSRegularExpression(pattern: Self.predicatePattern)
        XCTAssertNotNil(
            predicateRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `QueueViewModel.fetchEpisodeMap(...)` no longer references \
            `keys.contains($0.canonicalEpisodeKey)` in its `#Predicate` \
            (whitespace-tolerant match). The cycle-8 M3 fix relies on \
            the predicate consuming the Array-typed local so SwiftData's \
            translator takes the Array.contains branch. Restore the \
            predicate OR update this canary if the predicate \
            intentionally moved.
            """
        )
    }

    /// Cycle-19 missing-test (mirroring `TrustScoringServiceUpsertProfileCanaryTests
    /// .testUpsertProfileRegexCatchesItsOwnTarget`): run the THREE canary
    /// regexes against fixture strings and assert each catches the shape
    /// it was designed to catch (and ONLY that shape). Without this, a
    /// future swift-format reflow that breaks the patterns would leave
    /// the canary above silently passing — "0 set-typed found, predicate
    /// missing" — even after a real Array-→-Set regression.
    func testRegexesCatchTheirOwnTargets() throws {
        let arrayParamRegex = try NSRegularExpression(pattern: Self.arrayParamPattern)
        let arrayParamPositive: [String] = [
            "keys: [String]",
            "keys : [String]",
            "keys:[String]",
            "keys :  [ String ]",
            "keys: [\n  String\n]",
        ]
        for fixture in arrayParamPositive {
            let r = NSRange(fixture.startIndex..., in: fixture)
            XCTAssertNotNil(
                arrayParamRegex.firstMatch(in: fixture, range: r),
                "arrayParamRegex did not match positive fixture `\(fixture)` — pattern has gone blind."
            )
        }
        let arrayParamNegative: [String] = [
            "keys: Set<String>",
            "values: [String]",
            "keys: [Int]",
        ]
        for fixture in arrayParamNegative {
            let r = NSRange(fixture.startIndex..., in: fixture)
            XCTAssertNil(
                arrayParamRegex.firstMatch(in: fixture, range: r),
                "arrayParamRegex over-matched on `\(fixture)` — pattern is too loose."
            )
        }

        let setParamRegex = try NSRegularExpression(pattern: Self.setParamPattern)
        let setParamPositive: [String] = [
            "keys: Set<String>",
            "keys : Set<String>",
            "keys:Set<String>",
            "keys :  Set < String >",
        ]
        for fixture in setParamPositive {
            let r = NSRange(fixture.startIndex..., in: fixture)
            XCTAssertNotNil(
                setParamRegex.firstMatch(in: fixture, range: r),
                "setParamRegex did not match positive fixture `\(fixture)` — the regression we want to catch would now slip through."
            )
        }
        let setParamNegative: [String] = [
            "keys: [String]",
            "values: Set<String>",
            "keys: Set<Int>",
        ]
        for fixture in setParamNegative {
            let r = NSRange(fixture.startIndex..., in: fixture)
            XCTAssertNil(
                setParamRegex.firstMatch(in: fixture, range: r),
                "setParamRegex over-matched on `\(fixture)` — pattern is too loose."
            )
        }

        let predicateRegex = try NSRegularExpression(pattern: Self.predicatePattern)
        let predicatePositive: [String] = [
            "keys.contains($0.canonicalEpisodeKey)",
            "keys . contains ( $0 . canonicalEpisodeKey )",
            "keys.contains( $0.canonicalEpisodeKey )",
            "keys\n  .contains($0.canonicalEpisodeKey)",
        ]
        for fixture in predicatePositive {
            let r = NSRange(fixture.startIndex..., in: fixture)
            let regex = try NSRegularExpression(
                pattern: Self.predicatePattern,
                options: [.dotMatchesLineSeparators]
            )
            XCTAssertNotNil(
                regex.firstMatch(in: fixture, range: r),
                "predicateRegex did not match positive fixture `\(fixture)` — pattern has gone blind."
            )
        }
        let predicateNegative: [String] = [
            "keys.contains($0.episodeKey)",                    // wrong member name
            "values.contains($0.canonicalEpisodeKey)",         // wrong local
            "keys.contains(canonicalEpisodeKey)",              // missing $0
        ]
        for fixture in predicateNegative {
            let r = NSRange(fixture.startIndex..., in: fixture)
            XCTAssertNil(
                predicateRegex.firstMatch(in: fixture, range: r),
                "predicateRegex over-matched on `\(fixture)` — pattern is too loose."
            )
        }
    }

    // MARK: - Patterns (shared between the production scan and the
    //         positive/negative controls so both stay in lockstep)

    private static let arrayParamPattern = #"keys\s*:\s*\[\s*String\s*\]"#
    private static let setParamPattern = #"keys\s*:\s*Set\s*<\s*String\s*>"#
    private static let predicatePattern =
        #"keys\s*\.\s*contains\s*\(\s*\$0\s*\.\s*canonicalEpisodeKey\s*\)"#
}
