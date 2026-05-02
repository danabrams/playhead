// AdDetectionServicePrecisionGateLabelCanaryTests.swift
//
// skeptical-review-cycle-3 missing-test: source canary on the
// load-bearing literal raw values returned by
// `AdDetectionService.precisionGateLabel`.
//
// The cycle-2 type-decode in `SkipOrchestrator.receiveAdWindows` and
// the cycle-2/3 `nonMarkOnlyGateEntersStandardSkipPath` test both
// depend on the producer continuing to emit the literal `"autoSkip"`
// (NOT `"eligible"`) for the auto-skip-eligible classification. The
// `AutoSkipPrecisionGate` enum case is `.autoSkipEligible`, but the
// historical doc-comment in `AutoSkipPrecisionGate.swift` previously
// claimed the persisted raw value was `"eligible"`. Reconciled in
// cycle-3.
//
// Why this canary matters even though the L3 decode would still
// route a hypothetical `"eligible"` through the standard path
// correctly: persisted `ad_windows` rows already stamped `"autoSkip"`
// continue to live in users' on-device databases. A future producer
// change that swaps the raw value WITHOUT a schema migration would
// silently mismatch existing rows, which any code that round-trips
// the literal (test fixtures, log parsing, debug exports, the
// AutoSkipPrecisionGate doc-comment itself) depends on.
//
// This canary forensic-locks the producer's choice so a future
// refactor MUST either keep the literal or come with an explicit
// schema migration plan and a coordinated update of every consumer
// that round-trips the value.
//
// XCTest so the canary is filterable from the test plan
// (`xctestplan selectedTests/skippedTests` silently ignores Swift
// Testing identifiers — see project memory
// `xctestplan_swift_testing_limitation`).

import Foundation
import XCTest

final class AdDetectionServicePrecisionGateLabelCanaryTests: XCTestCase {

    /// Cycle-3 missing-test: pin that `precisionGateLabel`'s body
    /// returns the literal `"autoSkip"` for the auto-skip-eligible
    /// classification and the literal `"markOnly"` for the
    /// medium-confidence classification, and that it does NOT return
    /// `"eligible"` (the stale doc-comment claim that was reconciled in
    /// cycle-3).
    func testPrecisionGateLabelReturnsAutoSkipNotEligible() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AdDetectionService.swift"
        )

        // Locate `private func precisionGateLabel(`. Anchor on
        // `private func` (rather than just `func`) so a future
        // wrapper/overload like `func precisionGateLabel(...)`
        // (without `private`) cannot make `firstIndex(of: "{")`
        // extract the wrapper's body and miss the implementation's
        // literal returns. There is exactly one definition today; if
        // an overload is added, the canary should be split per
        // overload rather than silently latch onto the wrong one.
        let funcAnchor = "private func precisionGateLabel("
        guard let funcRange = source.range(of: funcAnchor) else {
            XCTFail("Could not locate `\(funcAnchor)` in AdDetectionService.swift")
            return
        }
        guard let openBrace = source[funcRange.upperBound...].firstIndex(of: "{") else {
            XCTFail("Could not locate `{` after `\(funcAnchor)`")
            return
        }
        let body = SwiftSourceInspector.bracedBody(in: source, startingAt: openBrace)

        // For the positive checks we want the literal string
        // contents intact — strip comments only, NOT string literals.
        let strippedComments = SwiftSourceInspector.strippingComments(body)

        // Positive: `return "autoSkip"` must be present.
        let autoSkipReturnRegex = try NSRegularExpression(
            pattern: #"return\s+"autoSkip""#
        )
        let strippedRange = NSRange(strippedComments.startIndex..., in: strippedComments)
        XCTAssertNotNil(
            autoSkipReturnRegex.firstMatch(in: strippedComments, range: strippedRange),
            """
            `AdDetectionService.precisionGateLabel(...)` no longer returns \
            the literal `"autoSkip"` for the auto-skip-eligible \
            classification. The cross-launch preload routing in \
            `SkipOrchestrator.receiveAdWindows` and the \
            `nonMarkOnlyGateEntersStandardSkipPath` test depend on this \
            literal. A producer change must either keep the literal or \
            migrate the persisted `ad_windows.eligibilityGate` column.
            """
        )

        // Positive: `return "markOnly"` must be present.
        let markOnlyReturnRegex = try NSRegularExpression(
            pattern: #"return\s+"markOnly""#
        )
        XCTAssertNotNil(
            markOnlyReturnRegex.firstMatch(in: strippedComments, range: strippedRange),
            """
            `AdDetectionService.precisionGateLabel(...)` no longer returns \
            the literal `"markOnly"` for the uiCandidate classification. \
            The suggest-tier routing in `SkipOrchestrator.receiveAdWindows` \
            depends on this literal.
            """
        )

        // Negative: `return "eligible"` must NOT appear. The historical
        // doc-comment in `AutoSkipPrecisionGate.swift` claimed this was
        // the raw value; cycle-3 reconciled the doc to match the
        // producer. A regression that swaps the producer to "eligible"
        // (e.g. to align with `SkipEligibilityGate.eligible.rawValue`)
        // would break the cycle-2 routing contract.
        let eligibleReturnRegex = try NSRegularExpression(
            pattern: #"return\s+"eligible""#
        )
        XCTAssertNil(
            eligibleReturnRegex.firstMatch(in: strippedComments, range: strippedRange),
            """
            `AdDetectionService.precisionGateLabel(...)` now returns the \
            literal `"eligible"` — but the cross-launch preload routing in \
            `SkipOrchestrator.receiveAdWindows`, the persisted \
            `ad_windows.eligibilityGate` column, and the cycle-2/3 \
            `nonMarkOnlyGateEntersStandardSkipPath` test all assume \
            `"autoSkip"`. Reconcile the producer with the consumer/schema \
            before changing this literal.
            """
        )
    }

    /// Cycle-7 missing-test (T-1): symmetric canary on the
    /// `AutoSkipPrecisionGate.swift` doc-comment that describes the
    /// persisted raw value. Cycle-3 reconciled the doc-comment from
    /// the historical `"eligible"` to `"autoSkip"`. The producer
    /// canary above pins `AdDetectionService.precisionGateLabel`'s
    /// returns; this canary pins the AutoSkipPrecisionGate.swift doc
    /// alongside it so a future regression that flipped the doc back
    /// to `"eligible"` (without changing the producer) would fail
    /// fast instead of silently desyncing the documentation from the
    /// implementation.
    func testAutoSkipPrecisionGateDocReferencesAutoSkipNotEligible() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AutoSkipPrecisionGate.swift"
        )

        // Whole-file canary: do NOT strip strings (the literals
        // `"autoSkip"` / `"eligible"` we care about live in
        // doc-comments and in the enum-case doc-comments — comment
        // stripping would erase them). Use `strippingCommentsAndStrings`
        // is exactly wrong for this canary. We grep the raw file.

        // Positive: the file must mention `eligibilityGate = "autoSkip"`
        // (the canonical doc-string for the autoSkipEligible case).
        let autoSkipDocRegex = try NSRegularExpression(
            pattern: #"eligibilityGate\s*=\s*"autoSkip""#
        )
        let fullRange = NSRange(source.startIndex..., in: source)
        XCTAssertNotNil(
            autoSkipDocRegex.firstMatch(in: source, range: fullRange),
            """
            `AutoSkipPrecisionGate.swift` no longer contains the doc-string \
            `eligibilityGate = "autoSkip"`. The cycle-3 reconciliation \
            between the doc and the producer (`AdDetectionService\
            .precisionGateLabel`) requires this doc-string to match the \
            literal that the producer emits. Either restore the doc-string \
            or update both the producer canary and this canary together.
            """
        )

        // Positive: the file must mention `eligibilityGate = "markOnly"`
        // for the symmetric uiCandidate doc-string.
        let markOnlyDocRegex = try NSRegularExpression(
            pattern: #"eligibilityGate\s*=\s*"markOnly""#
        )
        XCTAssertNotNil(
            markOnlyDocRegex.firstMatch(in: source, range: fullRange),
            """
            `AutoSkipPrecisionGate.swift` no longer contains the doc-string \
            `eligibilityGate = "markOnly"`. The doc must enumerate both \
            persisted raw values the producer emits.
            """
        )

        // Negative: the file must NOT contain `eligibilityGate = "eligible"`.
        // That was the historical wrong value; cycle-3 reconciled it to
        // `"autoSkip"`. A regression that flipped the doc back would
        // silently desync from the producer (which still emits
        // `"autoSkip"`) and from the consumer's L3 decode.
        let eligibleDocRegex = try NSRegularExpression(
            pattern: #"eligibilityGate\s*=\s*"eligible""#
        )
        XCTAssertNil(
            eligibleDocRegex.firstMatch(in: source, range: fullRange),
            """
            `AutoSkipPrecisionGate.swift` now contains the doc-string \
            `eligibilityGate = "eligible"` — but the producer \
            (`AdDetectionService.precisionGateLabel`) still emits \
            `"autoSkip"`, and the persisted `ad_windows.eligibilityGate` \
            column on existing on-device databases stores `"autoSkip"`. \
            Reconcile the doc with the producer and the schema before \
            changing this literal.
            """
        )
    }
}
