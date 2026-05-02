// AnalysisStoreFinalPassINClauseSourceCanaryTests.swift
//
// skeptical-review-cycle-11 missing-test: source canary on the
// load-bearing IN-clause asymmetry across the four `markFinalPassJob*`
// methods in `AnalysisStore.swift`. The cycle-7 M1 / cycle-8 M2 /
// cycle-9 M-2 / cycle-10 M-1 doc series describes the contract:
//
//     markFinalPassJobRunning   IN-clause MUST   contain 'failed'
//     markFinalPassJobDeferred  IN-clause MUST   contain 'failed'
//     markFinalPassJobComplete  IN-clause MUST   contain 'failed'
//     markFinalPassJobFailed    IN-clause MUST NOT contain 'failed'
//
// The behavioral tests in `AnalysisStoreReviewFollowupTests` cover this
// probabilistically — a `failed → failed` no-op test, a re-promotion
// test, a cross-launch retry test, a reaper-skips-failed test. But a
// future SQL refactor that drops `'failed'` from one of the H2-allowed
// siblings (or adds it to `markFinalPassJobFailed`) might still pass
// most behavioral tests if the refactor also touched a sibling that
// camouflages the regression. A direct source-level pin is a stronger
// backstop, especially when the constraint is contract-level rather
// than implementation-level.
//
// XCTest so the canary is filterable from the test plan
// (`xctestplan selectedTests/skippedTests` silently ignores Swift
// Testing identifiers — see project memory `xctestplan_swift_testing_limitation`).

import Foundation
import XCTest

final class AnalysisStoreFinalPassINClauseSourceCanaryTests: XCTestCase {

    private func loadAnalysisStoreSource() throws -> String {
        try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Persistence/AnalysisStore/AnalysisStore.swift"
        )
    }

    /// Extract the body of `func <name>(...)` and return the
    /// comments-and-strings-stripped (well, comments-stripped — strings
    /// are preserved because the SQL we want to inspect IS a string)
    /// view. We use `strippingComments` (NOT `strippingCommentsAndStrings`)
    /// so that the embedded SQL string literal survives the strip; the
    /// IN-clause we audit lives inside a `"""..."""` literal.
    private func bodyForFunc(named name: String, in source: String) throws -> String {
        guard let funcRange = source.range(of: "func \(name)(") else {
            XCTFail("Could not locate `func \(name)(` in AnalysisStore.swift")
            return ""
        }
        guard let openBrace = source[funcRange.upperBound...].firstIndex(of: "{") else {
            XCTFail("Could not locate `{` after `func \(name)(`")
            return ""
        }
        let body = SwiftSourceInspector.bracedBody(in: source, startingAt: openBrace)
        return SwiftSourceInspector.strippingComments(body)
    }

    /// Returns the contents of the `IN (...)` clause in the body's SQL,
    /// or nil if no `IN (...)` is found. This matches the FIRST `IN (`
    /// in the body — the four methods we audit each contain exactly one
    /// IN-clause in their UPDATE statement.
    private func inClauseValues(in body: String) -> [String]? {
        guard let inRange = body.range(of: "IN (") else { return nil }
        let after = body[inRange.upperBound...]
        guard let close = after.firstIndex(of: ")") else { return nil }
        let inner = String(after[after.startIndex..<close])
        // Split on commas, trim quotes/whitespace.
        return inner
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines)
                .trimmingCharacters(in: CharacterSet(charactersIn: "'")) }
    }

    /// Pin: each H2-allowed sibling's IN-clause MUST contain `'failed'`.
    func testH2SiblingsIncludeFailedInINClause() throws {
        let source = try loadAnalysisStoreSource()
        for funcName in [
            "markFinalPassJobRunning",
            "markFinalPassJobDeferred",
            "markFinalPassJobComplete",
        ] {
            let body = try bodyForFunc(named: funcName, in: source)
            let values = inClauseValues(in: body)
            XCTAssertNotNil(values, """
                Could not parse IN-clause from `\(funcName)`. The cycle-7 M1 \
                / cycle-9 M-1 contract requires the H2-allowed siblings to \
                include `'failed'` in their WHERE...IN clause; if the SQL \
                shape changed, update this canary.
                """)
            XCTAssertTrue(
                values?.contains("failed") == true,
                """
                `\(funcName)` IN-clause is missing `'failed'`. The H2 \
                contract (cycle-7/8/9/10 docs on `markFinalPassJobRunning`) \
                requires `'failed'` to be allowed so a recovered retry can \
                land — without it, the runner's failed→running re-promotion \
                becomes a silent no-op and rows stay stuck `failed` forever. \
                Restore `'failed'` to the IN-clause OR update this canary if \
                the H2 fix has been intentionally rolled back.
                """
            )
        }
    }

    /// Pin: `markFinalPassJobFailed`'s IN-clause MUST NOT contain
    /// `'failed'` — the cycle-7 M1 asymmetry caps in-drain `retryCount`
    /// climb at +1 by making `failed → failed` a silent no-op.
    func testMarkFinalPassJobFailedExcludesFailedFromINClause() throws {
        let source = try loadAnalysisStoreSource()
        let body = try bodyForFunc(named: "markFinalPassJobFailed", in: source)
        let values = inClauseValues(in: body)
        XCTAssertNotNil(values, """
            Could not parse IN-clause from `markFinalPassJobFailed`. The \
            cycle-7 M1 contract requires `'failed'` to be EXCLUDED from \
            this clause; if the SQL shape changed, update this canary.
            """)
        XCTAssertFalse(
            values?.contains("failed") == true,
            """
            `markFinalPassJobFailed` IN-clause now contains `'failed'`. The \
            cycle-7 M1 / cycle-9 M-1 / cycle-10 M-1 docs explain the \
            asymmetry: this method increments `retryCount` unconditionally, \
            so allowing `failed → failed` re-entry would let `retryCount` \
            climb arbitrarily on every re-attempt within a single drain. \
            Either remove `'failed'` from this clause OR add a clamp on \
            `retryCount` (e.g. `MIN(retryCount + 1, MAX_RETRIES)`) and \
            update this canary.
            """
        )
        // Sanity-check: the H2-allowed states ARE present, so this isn't
        // a vacuous pass on a `IN ('xyz')` typo.
        for required in ["queued", "deferred", "running"] {
            XCTAssertTrue(
                values?.contains(required) == true,
                "`markFinalPassJobFailed` IN-clause unexpectedly missing `'\(required)'`."
            )
        }
    }
}
