// SkipOrchestratorBlockedGateGuardSourceCanaryTests.swift
//
// playhead-bq70 source canary: pin the symmetric blocked-gate guard
// in `SkipOrchestrator.receiveAdWindows` so a future refactor can't
// silently regress to the asymmetric "markOnly-only" form.
//
// Why a source canary (vs. only the runtime tests in
// `SkipOrchestratorBlockedGateGuardTests`):
//   • The runtime tests pin behaviour but a refactor that re-introduces
//     the asymmetry by removing the guard AND updating the tests
//     simultaneously is undetectable by the runtime layer alone.
//   • A regex pin against the source body is a third independent
//     witness: the guard's literal shape must remain present, so a
//     non-trivial deletion is forced to be deliberate (the canary
//     catches it at build time and the failure message routes the
//     reviewer to playhead-bq70's history).
//
// Pattern follows `QueueViewModelSourceCanaryTests` /
// `DebugDiagnosticsHatchSourceCanaryTests`. XCTest-shaped because
// `xctestplan selectedTests/skippedTests` filters silently ignore
// Swift Testing identifiers (project memory:
// `xctestplan_swift_testing_limitation`).

import XCTest

final class SkipOrchestratorBlockedGateGuardSourceCanaryTests: XCTestCase {

    /// The guard MUST be present in `receiveAdWindows` and MUST drop
    /// any decoded `SkipEligibilityGate` that is non-`.eligible` (the
    /// markOnly branch already returned earlier in the loop, so the
    /// `decoded != .eligible` form correctly excludes blocked cases
    /// while letting nil / unknown raw values fall through to the
    /// non-fusion producer path).
    ///
    /// We assert two things:
    ///   1. The guard literal is present (whitespace-tolerant).
    ///   2. The companion `continue` is present immediately after, so
    ///      a refactor that removes only the `continue` (turning the
    ///      guard into a no-op) is also caught.
    func testReceiveAdWindowsHasSymmetricBlockedGateGuard() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/SkipOrchestrator/SkipOrchestrator.swift"
        )
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        let guardRegex = try NSRegularExpression(
            pattern: Self.blockedGateGuardPattern,
            options: [.dotMatchesLineSeparators]
        )
        XCTAssertNotNil(
            guardRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `SkipOrchestrator.receiveAdWindows` no longer contains the \
            symmetric blocked-gate guard \
            `if let decoded = decodedGate, decoded != .eligible { ... continue }`. \
            playhead-bq70: this guard mirrors the \
            `result.eligibilityGate == .eligible` filter in \
            `receiveAdDecisionResults`. Without it, fusion-stamped windows \
            (`blockedByPolicy`, `blockedByEvidenceQuorum`, \
            `blockedByUserCorrection`, `cappedByFMSuppression`) silently \
            re-enter the auto-skip path on all three callers (cross-launch \
            preload, hot-path push, final-pass backfill push). Restore the \
            guard OR update this canary if the precision contract has \
            been deliberately re-shaped (and the new shape is reviewed \
            against the same three caller paths).
            """
        )
    }

    /// Cycle-19 pattern (mirroring the queue / diagnostics canaries):
    /// run the canary regex against fixture strings and assert it
    /// matches the shape it was designed to catch — and ONLY that
    /// shape. Without this, a future swift-format reflow that breaks
    /// the pattern leaves the canary above silently passing.
    func testRegexCatchesItsOwnTarget() throws {
        let regex = try NSRegularExpression(
            pattern: Self.blockedGateGuardPattern,
            options: [.dotMatchesLineSeparators]
        )

        // Positive fixtures: each represents a legitimate phrasing of
        // the symmetric blocked-gate guard the canary must accept.
        let positives: [String] = [
            // The exact form shipped in this bead.
            """
            if let decoded = decodedGate, decoded != .eligible {
                continue
            }
            """,
            // Different brace style.
            """
            if let decoded = decodedGate, decoded != .eligible
            {
                continue
            }
            """,
            // Extra interior whitespace.
            """
            if  let  decoded  =  decodedGate ,  decoded  !=  .eligible {
                continue
            }
            """,
            // A logger.debug between the if-line and the continue is
            // expected and must NOT defeat the match.
            """
            if let decoded = decodedGate, decoded != .eligible {
                logger.debug("blocked gate")
                continue
            }
            """,
        ]
        for fixture in positives {
            let range = NSRange(fixture.startIndex..., in: fixture)
            XCTAssertNotNil(
                regex.firstMatch(in: fixture, range: range),
                "blockedGateGuardPattern did not match positive fixture — pattern has gone blind:\n\(fixture)"
            )
        }

        // Negative fixtures: structurally similar code that is NOT the
        // guard. The canary must NOT match these or it would falsely
        // pass on a guard removal.
        let negatives: [String] = [
            // The markOnly guard alone (asymmetric pre-bq70 form).
            """
            if decodedGate == .markOnly {
                continue
            }
            """,
            // Re-binding to a different name — `decoded` is the
            // load-bearing local; an `if let foo = ...` form should
            // NOT register because the diff is non-trivial enough to
            // warrant explicit reviewer attention.
            """
            if let foo = decodedGate, foo != .eligible {
                continue
            }
            """,
            // Comparison flipped to `==` (matches eligible only) — the
            // opposite contract.
            """
            if let decoded = decodedGate, decoded == .eligible {
                continue
            }
            """,
            // Missing `continue` — guard with side-effects but no
            // skip would still let the row reach evaluateAndPush.
            """
            if let decoded = decodedGate, decoded != .eligible {
                logger.debug("noop")
            }
            """,
        ]
        for fixture in negatives {
            let range = NSRange(fixture.startIndex..., in: fixture)
            XCTAssertNil(
                regex.firstMatch(in: fixture, range: range),
                "blockedGateGuardPattern over-matched on negative fixture — the regression we want to catch would slip through:\n\(fixture)"
            )
        }
    }

    // MARK: - Pattern

    /// Whitespace-tolerant pattern for:
    ///
    ///   if let decoded = decodedGate, decoded != .eligible {
    ///       ... // optional intervening lines
    ///       continue
    ///   }
    ///
    /// `[^}]*?` is the body window — non-greedy, restricted to NOT
    /// jump out of the brace, so the pattern can't span past the
    /// guard's closing `}` and pick up an unrelated `continue` later
    /// in the loop.
    private static let blockedGateGuardPattern: String =
        #"if\s+let\s+decoded\s*=\s*decodedGate\s*,\s*decoded\s*!=\s*\.eligible\s*\{[^}]*?continue[^}]*?\}"#
}
