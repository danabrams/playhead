// ActivitySnapshotProviderSourceCanaryTests.swift
//
// skeptical-review-cycle-8 missing test (M3 / regression net for cycle-7
// H1): pin the load-bearing Array-vs-Set choice for the SwiftData
// `#Predicate` in `LiveActivitySnapshotProvider.loadInputs()`.
//
// Cycle-7 H1 fix at `ActivitySnapshotProvider.swift:166` flipped
// `eligibleEpisodeIds` from `Set(allAssets.keys)` to
// `Array(allAssets.keys)` because SwiftData's `#Predicate` translator
// handles `Array.contains` reliably across iOS / macOS Catalyst builds,
// while `Set.contains` falls back to a per-row in-memory scan, fails to
// translate at all, or crashes on certain toolchain combinations. The
// failure mode is silent — on the affected builds the Activity widget
// appears empty because the predicate matches nothing.
//
// Cycle-8 missing-test #5 (the reviewer's residual gap) — there is no
// behavioral test that pins this constraint, because reproducing the
// fragile-translation failure requires the exact toolchain combination
// that surfaced it, and the fast-test simulator typically does not
// trigger it. The next-best regression net is a source-level canary:
// assert that the body of `loadInputs()` constructs the eligible-id
// list as an Array, not as a Set, and that the predicate references
// that locally-named Array. A future refactor that "tightens" the
// type back to `Set<String>` will trip this test rather than silently
// breaking the widget on a future toolchain bump.

import XCTest

final class ActivitySnapshotProviderSourceCanaryTests: XCTestCase {

    /// Cycle-8 regression net for cycle-7 H1: the eligible-id list
    /// inside `LiveActivitySnapshotProvider.loadInputs()` MUST be an
    /// `Array`, not a `Set`. We assert two things:
    ///   1. The `let eligibleEpisodeIds = Array(allAssets.keys)`
    ///      construction is present in the file body — this is the
    ///      cycle-7 fix.
    ///   2. The `Set(allAssets.keys)` construction is NOT present.
    ///      A future refactor that flips the line back is the
    ///      regression we want to catch.
    func testEligibleEpisodeIdsBuildsAsArrayNotSet() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/Activity/ActivitySnapshotProvider.swift"
        )
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)

        // skeptical-review-cycle-14 L-2: positive check is now
        // whitespace-tolerant. The earlier literal-substring form
        // (`stripped.contains("Array(allAssets.keys)")`) would have
        // been falsified by a benign swift-format reflow that inserts
        // spaces inside the parens (e.g. `Array( allAssets.keys )`)
        // even though the production semantics are unchanged. Symmetry
        // with the negative `Set(...)` check below.
        let arrayConstructionRegex = try NSRegularExpression(
            pattern: #"Array\s*\(\s*allAssets\s*\.\s*keys\s*\)"#
        )
        let strippedRangeForArray = NSRange(stripped.startIndex..., in: stripped)
        XCTAssertNotNil(
            arrayConstructionRegex.firstMatch(in: stripped, range: strippedRangeForArray),
            """
            `LiveActivitySnapshotProvider.loadInputs()` no longer builds \
            `eligibleEpisodeIds` as `Array(allAssets.keys)` (whitespace-tolerant \
            match). Cycle-7 H1: SwiftData's `#Predicate` translator handles \
            `Array.contains` reliably across iOS / macOS Catalyst builds, \
            but `Set.contains` falls back to a per-row in-memory scan or \
            fails to translate on some toolchains, silently emptying the \
            Activity widget. Restore `Array(allAssets.keys)` OR update \
            this canary if the membership construction intentionally moved.
            """
        )

        // skeptical-review-cycle-10 L-1: match `Set(allAssets.keys)`
        // tolerantly so `Set( allAssets.keys )` or `Set(\nallAssets.keys\n)`
        // also trip. A literal substring check (cycle-7 H1's original
        // form) would slip past whitespace-reflowed regressions.
        //
        // skeptical-review-cycle-14 L-2: the positive `Array(...)` check
        // above and this negative `Set(...)` check are now both
        // whitespace-tolerant — symmetric. The whitespace-tolerant
        // pattern for the predicate-body is the analogue further below.
        let setConstructionRegex = try NSRegularExpression(
            pattern: #"Set\s*\(\s*allAssets\s*\.\s*keys\s*\)"#
        )
        let strippedRangeForSet = NSRange(stripped.startIndex..., in: stripped)
        XCTAssertNil(
            setConstructionRegex.firstMatch(in: stripped, range: strippedRangeForSet),
            """
            `LiveActivitySnapshotProvider.loadInputs()` re-introduced a \
            `Set(allAssets.keys)` construction (whitespace-tolerant match). \
            Cycle-7 H1 documented why this is unsafe: SwiftData's \
            `#Predicate` translation against a Swift `Set` is fragile \
            across toolchain revs and silently empties the Activity widget \
            on the bad combinations. Use `Array(allAssets.keys)` instead. \
            See `LiveActivitySnapshotProvider.loadInputs()` and \
            `PlayheadApp` for the documented constraint.
            """
        )

        // Pin that the `#Predicate` references the same locally-named
        // Array so a future refactor that adds a sibling `Set` local
        // (e.g. for de-duping) cannot accidentally swap into the
        // predicate body.
        //
        // skeptical-review-cycle-9 L-1: tolerate whitespace/newline
        // reflow (swift-format, manual line-break) by matching with a
        // regex rather than pinning the literal source byte sequence.
        let predicateRegex = try NSRegularExpression(
            pattern: #"eligibleEpisodeIds\s*\.\s*contains\s*\(\s*\$0\s*\.\s*canonicalEpisodeKey\s*\)"#
        )
        let range = NSRange(stripped.startIndex..., in: stripped)
        XCTAssertNotNil(
            predicateRegex.firstMatch(in: stripped, range: range),
            """
            `LiveActivitySnapshotProvider.loadInputs()` no longer references \
            `eligibleEpisodeIds.contains($0.canonicalEpisodeKey)` in its \
            `#Predicate` (whitespace-tolerant match). The cycle-7 H1 fix \
            relies on the predicate consuming the Array-typed local so \
            SwiftData's translator takes the Array.contains branch. \
            Restore the predicate OR update this canary if the predicate \
            intentionally moved.
            """
        )
    }
}
