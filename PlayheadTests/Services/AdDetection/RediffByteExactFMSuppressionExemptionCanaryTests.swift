// RediffByteExactFMSuppressionExemptionCanaryTests.swift
//
// playhead-qs0d, item (2): "BYPASS the FM eligibility gate for byte-exact
// rediff spans — deterministic byte-exact slots self-certify and must NOT
// route through the non-deterministic FM pass that historically forced
// mark-only."
//
// The lagged sweep already stamps `.rediffByteExact` anchors correctly
// (`.rediffSlot` width ownership → `SpanExtentSupport.derive` → the persisted
// edge columns), so the ONE thing standing between a byte-verified DAI
// divergence and auto-skip on that path is `.cappedByFMSuppression`: an FM
// noAds consensus overriding the gate. That is precisely backwards — the byte
// differ observed that the origin served different bytes over this exact
// region; a language model's opinion that "there is no ad here" does not
// outrank it.
//
// WHY A SOURCE CANARY AND NOT A BEHAVIOURAL TEST. FoundationModels is
// unavailable on the simulator, so no test running in the gate can drive a span
// to `.cappedByFMSuppression` at all — the same constraint
// `AdDetectionServiceSelfPromoSuppressionCanaryTests` documents ("FM is
// unavailable on the simulator, so there is no behavioural test that can drive
// a span to `.cappedByFMSuppression`; this canary is the only guard against
// that regression"). This file follows that established precedent rather than
// inventing a fake FM seam. The exemption PREDICATE itself
// (`DecodedSpan.carriesRediffByteExactWidth`, incl. its splice-agnosticism) is
// covered behaviourally by `RediffByteExactDemotionExemptionTests`.
//
// XCTest so the canary is filterable from the test plan (`xctestplan` silently
// ignores Swift Testing identifiers — see project memory
// `xctestplan_swift_testing_limitation`).

import Foundation
import XCTest

final class RediffByteExactFMSuppressionExemptionCanaryTests: XCTestCase {

    /// The cap branch must be guarded by `refinedSpan.carriesRediffByteExactWidth`.
    /// Without it, an FM noAds consensus silently returns every byte-verified
    /// rediff span on that episode to mark-only and the promotion this bead
    /// ships is dead on exactly the episodes FM is least sure about.
    func testFMSuppressionCapDeclinesForByteExactRediffSpans() throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        let declineRegex = try NSRegularExpression(
            pattern: #"suppressionResult\s*\.\s*cappedToMarkOnly\s*,\s*refinedSpan\s*\.\s*carriesRediffByteExactWidth"#
        )
        XCTAssertNotNil(
            declineRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.runBackfill` no longer declines the \
            FM-suppression cap for byte-exact rediff spans. The guard must \
            read `suppressionResult.cappedToMarkOnly, \
            refinedSpan.carriesRediffByteExactWidth` — a deterministic byte \
            divergence outranks an FM noAds consensus (playhead-qs0d item 2). \
            Losing it silently reverts every lagged rediff span on an \
            FM-noAds episode to mark-only.
            """
        )
    }

    /// The exemption must be scoped to the BYTE-EXACT predicate, never widened
    /// to `isWidthOwnership`. `.spliceSlot` is ACOUSTIC width, not byte-exact
    /// — widening the predicate would let a non-deterministic acoustic guess
    /// override FM suppression, which is the precise leak
    /// `RediffByteExactDemotionExemptionTests` exists to prevent on the two
    /// sibling (pzy2) exemptions.
    func testFMSuppressionExemptionIsNotWidenedToAllWidthOwnership() throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        // The cap site itself must not be guarded by the broad marker.
        //
        // MUTATION-DERIVED (M12, review round 1): the first version of this
        // pattern was `cappedToMarkOnly[^\n]*isWidthOwnership`, and it SURVIVED
        // the exact mutation it exists to kill — swapping
        // `carriesRediffByteExactWidth` for
        // `anchorProvenance.contains(where: { $0.isWidthOwnership })` puts a
        // NEWLINE between the two tokens, which `[^\n]*` cannot cross. The
        // window is now newline-tolerant and bounded to 200 characters, which is
        // wide enough for any formatting of this one guard and far too narrow to
        // reach the unrelated `!$0.isWidthOwnership` filter several hundred
        // lines later in the same function.
        let broadRegex = try NSRegularExpression(
            pattern: #"cappedToMarkOnly[\s\S]{0,200}?isWidthOwnership"#
        )
        XCTAssertNil(
            broadRegex.firstMatch(in: stripped, range: strippedRange),
            """
            The FM-suppression cap exemption was widened from \
            `carriesRediffByteExactWidth` (`.rediffSlot` only) to the broad \
            `isWidthOwnership` marker, which also matches `.spliceSlot` — \
            ACOUSTIC width, not byte-exact. Only a byte-verified divergence \
            self-certifies against an FM noAds consensus.
            """
        )
    }

    /// The decline must forward the mapper's own verdict, never manufacture a
    /// promotion. `decision = rawDecision` is the whole action: it declines to
    /// APPLY a demotion. A regression that instead wrote
    /// `eligibilityGate: .eligible` would promote spans `DecisionMapper` had
    /// blocked on their own evidence (`.blockedByEvidenceQuorum` and friends),
    /// which is a materially different — and much larger — permission grant.
    func testFMSuppressionDeclineForwardsTheMapperVerdictVerbatim() throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)

        guard let capRange = stripped.range(of: "carriesRediffByteExactWidth") else {
            XCTFail("the FM-suppression exemption guard is gone — see the first canary")
            return
        }
        // The branch body immediately follows the guard; 600 characters is a
        // generous window over the log line + the single assignment.
        let windowEnd = stripped.index(
            capRange.upperBound,
            offsetBy: 600,
            limitedBy: stripped.endIndex
        ) ?? stripped.endIndex
        let branch = String(stripped[capRange.upperBound..<windowEnd])

        XCTAssertTrue(
            branch.contains("decision = rawDecision"),
            """
            The byte-exact FM-suppression exemption no longer forwards \
            `rawDecision` verbatim. The contract is to DECLINE a demotion, \
            never to promote: the span must keep whatever gate \
            `DecisionMapper` gave it, so a genuinely weak rediff-owned span \
            can still land `.blockedByEvidenceQuorum`.
            """
        )
    }

    // MARK: - Helpers

    /// Loads `AdDetectionService.swift` and returns the brace-delimited body of
    /// the actor's `func runBackfill(` implementation. Mirrors the helper in
    /// `AdDetectionServiceSelfPromoSuppressionCanaryTests` /
    /// `AdDetectionServiceCreatorChapterFusionCanaryTests` so a future
    /// restructure (actor → class, file rename) needs the canary files updated
    /// in lockstep.
    private static func runBackfillImplementationBody() throws -> String {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AdDetectionService.swift"
        )

        let actorAnchor = "actor AdDetectionService {"
        guard let actorRange = source.range(of: actorAnchor) else {
            throw NSError(
                domain: "RediffByteExactFMSuppressionExemptionCanary",
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
                domain: "RediffByteExactFMSuppressionExemptionCanary",
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
                domain: "RediffByteExactFMSuppressionExemptionCanary",
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
