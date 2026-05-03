// AdDetectionServicePriorHierarchyCanaryTests.swift
//
// playhead-084j (review-cycle 2): source canary on the load-bearing
// "resolve once per episode" perf invariant for the prior hierarchy
// wire-up in `AdDetectionService.runBackfill`.
//
// The wire-up tests in `PriorHierarchyWireUpTests` lock the *behavior*
// of `resolveEpisodePriors` and the show-local→DurationPrior path, but
// they cannot lock *where* the resolver is called. The bead explicitly
// requires the resolution to happen ONCE per episode, outside the
// per-span fusion loop, because:
//
//   • Resolution is a few arithmetic blends, but the per-span loop
//     runs O(N_decoded_spans) times. A future "let me just inline this
//     to keep the data flow obvious" refactor that moves the resolver
//     call inside the for-loop would multiply that small cost by every
//     candidate span — measurably wasted budget on long episodes with
//     many candidates, with zero behavioral benefit (priors are
//     per-episode, not per-span).
//   • The fix at p2iv was supposed to feed `DurationPrior` from
//     resolved priors. Until 084j the call site was hard-coded to
//     `DurationPrior.standard`. A regression that re-introduces
//     `.standard` would silently revert the fix without breaking any
//     behavioral test that doesn't measure show-local activation.
//
// XCTest so the canary is filterable from the test plan
// (`xctestplan` silently ignores Swift Testing identifiers — see
// project memory `xctestplan_swift_testing_limitation`).

import Foundation
import XCTest

final class AdDetectionServicePriorHierarchyCanaryTests: XCTestCase {

    /// Pin the perf invariant: `resolveEpisodePriors()` is called exactly
    /// once inside `runBackfill`'s body, and that single call site sits
    /// BEFORE the per-span `for span in decodedSpans` loop. A naive
    /// regression that moves the call into the loop would multiply
    /// per-episode resolution work by the candidate-span count.
    func testRunBackfillCallsResolveEpisodePriorsOnceBeforeSpanLoop() throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)

        // Find every call site of `resolveEpisodePriors(` in the function body.
        // Whitespace-tolerant for swift-format reflows.
        let callRegex = try NSRegularExpression(
            pattern: #"resolveEpisodePriors\s*\("#
        )
        let callMatches = callRegex.matches(
            in: stripped,
            range: NSRange(stripped.startIndex..., in: stripped)
        )
        XCTAssertEqual(
            callMatches.count, 1,
            """
            `AdDetectionService.runBackfill` should call \
            `resolveEpisodePriors()` exactly once. Found \(callMatches.count) \
            call site(s). Per-episode resolution must be hoisted out of the \
            per-span loop — duplicate calls multiply the work by the \
            candidate-span count for no behavioral benefit.
            """
        )
        guard let callRange = callMatches.first?.range else { return }

        // Find the per-span `for span in decodedSpans` loop opener.
        // Whitespace-tolerant.
        let loopRegex = try NSRegularExpression(
            pattern: #"for\s+span\s+in\s+decodedSpans"#
        )
        guard
            let loopMatch = loopRegex.firstMatch(
                in: stripped,
                range: NSRange(stripped.startIndex..., in: stripped)
            )
        else {
            XCTFail("""
            Could not locate `for span in decodedSpans` loop in \
            `runBackfill` body. Either the loop has been renamed (update \
            this canary) or the per-span fusion loop has been removed (in \
            which case the resolve-once invariant is moot — but verify \
            the reviewer agrees before deleting this test).
            """)
            return
        }

        // The single resolveEpisodePriors call must occur BEFORE the
        // for-loop. NSRange.location compares offsets directly, which is
        // what we want for "appears earlier in the function body".
        XCTAssertLessThan(
            callRange.location, loopMatch.range.location,
            """
            `AdDetectionService.runBackfill` calls `resolveEpisodePriors()` \
            INSIDE or AFTER the per-span `for span in decodedSpans` loop \
            (call at offset \(callRange.location), loop at offset \
            \(loopMatch.range.location)). Hoist the resolver call back \
            above the for-loop so resolution happens once per episode \
            instead of once per candidate span.
            """
        )
    }

    /// Pin the wire-up: the `DecisionMapper` instantiation inside
    /// `runBackfill` MUST pass an `episodeDurationPrior` (or any local
    /// captured from the resolved priors) — NOT `DurationPrior.standard`.
    /// The latter was the pre-084j behavior; a regression would silently
    /// disable the show-local override.
    func testRunBackfillFeedsResolvedDurationPriorNotStandard() throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        // Negative: the body must NOT pass `durationPrior: .standard` or
        // `durationPrior: DurationPrior.standard` to any call site.
        // Whitespace-tolerant for swift-format reflows.
        let standardRegex = try NSRegularExpression(
            pattern: #"durationPrior\s*:\s*(DurationPrior\s*\.)?\s*standard\b"#
        )
        XCTAssertNil(
            standardRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.runBackfill` re-introduced \
            `durationPrior: .standard` (whitespace-tolerant match). \
            Pre-084j every show ran with the global default \
            typicalAdDuration of 30...90s. Pass the resolved \
            `episodeDurationPrior` (built from `resolveEpisodePriors()`) \
            instead, OR update this canary if the prior hierarchy has \
            intentionally moved to a different feeder.
            """
        )

        // Positive: the body must contain at least one
        // `durationPrior:` argument label. If a future refactor renames
        // the parameter on `DecisionMapper`, this would catch it before
        // the silent-revert window opens — the test would force the
        // author to update this canary in lockstep.
        let argRegex = try NSRegularExpression(
            pattern: #"durationPrior\s*:"#
        )
        XCTAssertNotNil(
            argRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.runBackfill` no longer passes a \
            `durationPrior:` argument anywhere in its body. Either the \
            DecisionMapper parameter was renamed (update this canary) \
            or the prior wire-up was removed (in which case the bead's \
            acceptance criteria are violated and the wire-up tests in \
            `PriorHierarchyWireUpTests` should also be failing).
            """
        )
    }

    // MARK: - Helpers

    /// Loads `AdDetectionService.swift` and returns the brace-delimited
    /// body of the **actor's** `func runBackfill(` implementation —
    /// NOT the protocol declaration (line ~256, no body) and NOT the
    /// extension's default-implementation overload (line ~268, which
    /// just trampolines into the implementation).
    ///
    /// We anchor on `actor AdDetectionService {` first so the subsequent
    /// `func runBackfill(` search starts AFTER the protocol/extension
    /// declarations earlier in the file. There is exactly one
    /// `actor AdDetectionService {` in the source, so this anchor is
    /// unambiguous; if a future refactor renames the actor (or splits
    /// it into a class/struct), this helper's anchor will need to be
    /// updated and the `XCTFail`-style errors below will surface that
    /// drift loudly.
    private static func runBackfillImplementationBody() throws -> String {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AdDetectionService.swift"
        )

        // Step 1: find `actor AdDetectionService {` to skip past the
        // protocol declaration + extension default-impl earlier in the
        // file. Both of those contain `func runBackfill(` signatures
        // we explicitly do NOT want to inspect.
        let actorAnchor = "actor AdDetectionService {"
        guard let actorRange = source.range(of: actorAnchor) else {
            throw NSError(
                domain: "AdDetectionServicePriorHierarchyCanary",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: """
                Could not locate `\(actorAnchor)` in AdDetectionService.swift. \
                The actor was renamed or the canary's anchor needs an update.
                """]
            )
        }

        // Step 2: from inside the actor body, find the first
        // `func runBackfill(` — that is the implementation.
        let funcAnchor = "func runBackfill("
        guard let funcRange = source.range(
            of: funcAnchor,
            range: actorRange.upperBound..<source.endIndex
        ) else {
            throw NSError(
                domain: "AdDetectionServicePriorHierarchyCanary",
                code: 2,
                userInfo: [NSLocalizedDescriptionKey: """
                Could not locate `\(funcAnchor)` after `\(actorAnchor)`. \
                Either the implementation was renamed or removed; update \
                this helper's anchor or the canary itself.
                """]
            )
        }

        // Step 3: walk from the function signature to its opening `{`
        // (skipping argument list, return type, async/throws, etc.)
        // using the comment/string-aware brace finder. Plain
        // `firstIndex(of: "{")` would mis-fire on a `{` that appears
        // inside a default-argument expression in the parameter list.
        guard let openBrace = SwiftSourceInspector.findOpenBrace(
            in: source,
            after: funcRange.upperBound
        ) else {
            throw NSError(
                domain: "AdDetectionServicePriorHierarchyCanary",
                code: 3,
                userInfo: [NSLocalizedDescriptionKey: """
                Could not locate opening `{` of \
                `actor AdDetectionService.runBackfill(...)`. The signature \
                may have an unbalanced parameter list or the source file \
                was truncated.
                """]
            )
        }

        return SwiftSourceInspector.bracedBody(in: source, startingAt: openBrace)
    }
}
