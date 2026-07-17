// AdDetectionServiceSelfPromoSuppressionCanaryTests.swift
//
// playhead-fl4j (review-cycle): source canary on the load-bearing
// wire-up invariants for the self-promo-suppression branch inside
// `AdDetectionService.runBackfill`.
//
// The evaluator (`PromoSuppressor`), the bank (`SelfPromoBank`), and the
// flag plumbing are all unit-tested in their own suites, and the
// wire-in behaviour (flag-OFF byte-identity, flag-ON demotion +
// suggest-tier routing, precision guard, and the severity guard against
// a `.blockedByUserCorrection` block) is covered behaviourally by
// `SelfPromoSuppressionWireInTests`. What NO behavioural test on the
// simulator can pin is the exact *severity-guard boundary*:
//
//   • The guard MUST be `< SkipEligibilityGate.markOnly.severity`
//     (severity 1), so the ONLY gate it can demote is `.eligible`
//     (severity 0). A regression to `< blockedByPolicy.severity`
//     (severity 2) would let the suppressor STOMP an equal-severity
//     `.cappedByFMSuppression` (severity 1) span down to `.markOnly` —
//     which routes DIFFERENTLY in `SkipOrchestrator` (`.markOnly` →
//     suggest banner; `.cappedByFMSuppression` → dropped), so an
//     FM-suppressed span would silently resurface as a banner. FM is
//     unavailable on the simulator, so there is no behavioural test that
//     can drive a span to `.cappedByFMSuppression`; this canary is the
//     only guard against that regression.
//   • The demotion target MUST be `.markOnly` (never a harder or weaker
//     gate, never the opposite-direction `.eligible`).
//   • The suppression site MUST forward `proposalConfidence` /
//     `skipConfidence` from the in-scope `decision` value unchanged (the
//     "scoring stays honest" invariant) — never `rawDecision`, never a
//     hard-coded score.
//
// This mirrors `AdDetectionServiceCreatorChapterFusionCanaryTests`,
// which pins the analogous shape for the creator-chapter suppression
// branch this change is modelled on.
//
// XCTest so the canary is filterable from the test plan (`xctestplan`
// silently ignores Swift Testing identifiers — see project memory
// `xctestplan_swift_testing_limitation`).

import Foundation
import XCTest

final class AdDetectionServiceSelfPromoSuppressionCanaryTests: XCTestCase {

    /// Pin the precision-side severity guard: the fl4j branch must gate
    /// on `decision.eligibilityGate.severity <
    /// SkipEligibilityGate.markOnly.severity` so the demotion can ONLY
    /// relax a fully-`.eligible` (severity 0) span — never an
    /// equal-severity `.markOnly` / `.cappedByFMSuppression` (severity 1)
    /// nor any harder block (severity >= 2), and never a promotion.
    func testRunBackfillGuardsSelfPromoDemotionWithMarkOnlySeverity() throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        // Whitespace-tolerant match for the exact guard the bead contract
        // requires. `markOnly.severity` (not `blockedByPolicy.severity` —
        // the value the creator-chapter branch uses) is load-bearing.
        let guardRegex = try NSRegularExpression(
            pattern: #"decision\s*\.\s*eligibilityGate\s*\.\s*severity\s*<\s*SkipEligibilityGate\s*\.\s*markOnly\s*\.\s*severity"#
        )
        XCTAssertNotNil(
            guardRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.runBackfill` no longer guards the \
            self-promo suppression branch with \
            `decision.eligibilityGate.severity < \
            SkipEligibilityGate.markOnly.severity`. Without EXACTLY this \
            bound the demotion can reach an equal-severity gate: \
            `< blockedByPolicy.severity` would stomp a \
            `.cappedByFMSuppression` span down to `.markOnly` (which \
            routes to a suggest banner instead of staying dropped). \
            Restore the `< markOnly.severity` guard or update this canary \
            if the gate taxonomy moved (and re-verify the equal-severity \
            invariant elsewhere — no simulator test can, FM is \
            unavailable there).
            """
        )
    }

    /// Pin the evaluator call + the demotion target. The same `if` block
    /// that invokes `PromoSuppressor.shouldSuppress(...)` must set
    /// `eligibilityGate: .markOnly` — a suggest-tier (play-by-default)
    /// gate, NEVER an auto-skip-capable one and never a harder block.
    func testRunBackfillSelfPromoDemotionTargetsMarkOnly() throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        let suppressRegex = try NSRegularExpression(
            pattern: #"PromoSuppressor\s*\.\s*shouldSuppress"#
        )
        XCTAssertNotNil(
            suppressRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.runBackfill` no longer invokes \
            `PromoSuppressor.shouldSuppress(...)`. Either the evaluator \
            was renamed (update this canary) or the self-promo \
            suppression branch was removed (in which case the bead's \
            eligibility signal is gone).
            """
        )

        let gateRegex = try NSRegularExpression(
            pattern: #"eligibilityGate\s*:\s*\.markOnly\b"#
        )
        XCTAssertNotNil(
            gateRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.runBackfill` no longer sets \
            `eligibilityGate: .markOnly` inside the self-promo \
            suppression branch. The bead contract demotes a self-promo \
            span specifically to `.markOnly` so it routes to a \
            play-by-default suggest banner (NOT auto-skip). If the target \
            gate intentionally moved, update this canary AND verify \
            SkipOrchestrator's gate routing still surfaces the new value \
            as a suggest banner rather than auto-skipping or dropping it.
            """
        )
    }

    /// Pin the attention→verification wiring: the fl4j branch must thread the
    /// show identity into `PromoSuppressor.shouldSuppress(...)` via
    /// `showIdentity:`. Without it an AMBIGUOUS self-promo phrase ("get
    /// tickets", "on tour") could only ever be corroborated by a first-person
    /// pronoun — the show-identity corroboration path (a show naming ITSELF)
    /// would be silently dead, weakening the verification the precision rework
    /// depends on.
    func testRunBackfillThreadsShowIdentityIntoSuppressor() throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        let identityRegex = try NSRegularExpression(
            pattern: #"showIdentity\s*:\s*selfPromoShowIdentity"#
        )
        XCTAssertNotNil(
            identityRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.runBackfill` no longer threads \
            `showIdentity: selfPromoShowIdentity` into \
            `PromoSuppressor.shouldSuppress(...)`. The attention→verification \
            rework requires the show's own identity tokens at the call site so \
            an AMBIGUOUS self-promo phrase can be corroborated by the show \
            naming itself (not only a first-person pronoun). Restore the \
            argument or update this canary if the identity plumbing moved.
            """
        )
    }

    /// Pin the flag gate: the self-promo branch must short-circuit on
    /// `config.selfPromoSuppressionEnabled`. Without this the branch
    /// could run on the flag-OFF path, breaking the byte-identity
    /// contract even before the bank/word-stream nil-resolution.
    func testRunBackfillSelfPromoBranchIsFlagGated() throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        let flagRegex = try NSRegularExpression(
            pattern: #"config\s*\.\s*selfPromoSuppressionEnabled"#
        )
        XCTAssertNotNil(
            flagRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.runBackfill` no longer reads \
            `config.selfPromoSuppressionEnabled` in its body. The \
            self-promo suppression branch must stay flag-gated so the \
            flag-OFF path is byte-identical to pre-fl4j; restore the \
            gate or update this canary.
            """
        )
    }

    /// Pin the "scores stay honest" invariant *at the fl4j site*: the
    /// `if` block anchored on `PromoSuppressor.shouldSuppress` must
    /// forward both `proposalConfidence: decision.proposalConfidence` and
    /// `skipConfidence: decision.skipConfidence` — never a hard-coded
    /// score, never `rawDecision`. Scoped to the fl4j block (not the
    /// whole body) so a regression that clamps a score HERE is caught
    /// even though other blocks legitimately forward the same shape.
    func testRunBackfillSelfPromoSuppressionPreservesDecisionScores() throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)

        // Isolate the fl4j block PRECISELY: slice from its unique evaluator
        // call to its unique `eligibilityGate: .markOnly` gate assignment.
        // Both score forwards live inside the fl4j `DecisionResult`, which
        // sits BEFORE that gate line — so this window contains the fl4j
        // forwards and NOTHING after them. A fixed-length window would be
        // unsafe: the fragility-diagnostic block immediately after the fl4j
        // branch ALSO forwards both confidences from `decision` (to
        // `config.fragilityScore` / the observer), so a too-generous window
        // would false-PASS even if the fl4j forwards were deleted. Bounding
        // at `.markOnly` guarantees the assertions below see only the fl4j
        // block's own forwards.
        guard let anchor = stripped.range(of: "PromoSuppressor.shouldSuppress")
            ?? stripped.range(of: "PromoSuppressor . shouldSuppress") else {
            XCTFail("""
            Could not locate the `PromoSuppressor.shouldSuppress` anchor \
            in `runBackfill` — the self-promo branch was removed or \
            renamed (update this canary).
            """)
            return
        }
        let gateMarkerRegex = try NSRegularExpression(
            pattern: #"eligibilityGate\s*:\s*\.markOnly\b"#
        )
        let afterAnchor = NSRange(anchor.upperBound..., in: stripped)
        guard
            let gateMatch = gateMarkerRegex.firstMatch(in: stripped, range: afterAnchor),
            let gateRange = Range(gateMatch.range, in: stripped)
        else {
            XCTFail("""
            Could not locate the fl4j block's `eligibilityGate: .markOnly` \
            gate assignment after `PromoSuppressor.shouldSuppress` — the \
            demotion target moved (see \
            testRunBackfillSelfPromoDemotionTargetsMarkOnly) or the block \
            was restructured (update this canary).
            """)
            return
        }
        let block = String(stripped[anchor.upperBound..<gateRange.lowerBound])
        let blockRange = NSRange(block.startIndex..., in: block)

        let proposalRegex = try NSRegularExpression(
            pattern: #"proposalConfidence\s*:\s*decision\s*\.\s*proposalConfidence\b"#
        )
        let skipRegex = try NSRegularExpression(
            pattern: #"skipConfidence\s*:\s*decision\s*\.\s*skipConfidence\b"#
        )
        XCTAssertNotNil(
            proposalRegex.firstMatch(in: block, range: blockRange),
            """
            The fl4j self-promo suppression branch no longer forwards \
            `proposalConfidence: decision.proposalConfidence` — it was \
            deleted, or now reaches for `rawDecision`/a hard-coded score. \
            Forwarding from `decision` keeps the eligibility-only \
            contract (scores stay honest); restore the forward.
            """
        )
        XCTAssertNotNil(
            skipRegex.firstMatch(in: block, range: blockRange),
            """
            The fl4j self-promo suppression branch no longer forwards \
            `skipConfidence: decision.skipConfidence`. The "scores stay \
            honest" invariant requires the suppression site to leave both \
            confidences unchanged; restore the forward.
            """
        )
    }

    // MARK: - Helpers

    /// Loads `AdDetectionService.swift` and returns the brace-delimited
    /// body of the actor's `func runBackfill(` implementation. Mirrors
    /// `AdDetectionServiceCreatorChapterFusionCanaryTests`' helper so a
    /// future restructure (actor → class, file rename) only needs the
    /// two canary files updated in lockstep.
    private static func runBackfillImplementationBody() throws -> String {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AdDetectionService.swift"
        )

        let actorAnchor = "actor AdDetectionService {"
        guard let actorRange = source.range(of: actorAnchor) else {
            throw NSError(
                domain: "AdDetectionServiceSelfPromoSuppressionCanary",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: """
                Could not locate `\(actorAnchor)` in AdDetectionService.swift. \
                The actor was renamed or the canary's anchor needs an update.
                """]
            )
        }

        let funcAnchor = "func runBackfill("
        guard let funcRange = source.range(
            of: funcAnchor,
            range: actorRange.upperBound..<source.endIndex
        ) else {
            throw NSError(
                domain: "AdDetectionServiceSelfPromoSuppressionCanary",
                code: 2,
                userInfo: [NSLocalizedDescriptionKey: """
                Could not locate `\(funcAnchor)` after `\(actorAnchor)`.
                """]
            )
        }

        guard let openBrace = SwiftSourceInspector.findOpenBrace(
            in: source,
            after: funcRange.upperBound
        ) else {
            throw NSError(
                domain: "AdDetectionServiceSelfPromoSuppressionCanary",
                code: 3,
                userInfo: [NSLocalizedDescriptionKey: """
                Could not locate opening `{` of \
                `actor AdDetectionService.runBackfill(...)`.
                """]
            )
        }

        return SwiftSourceInspector.bracedBody(in: source, startingAt: openBrace)
    }
}
