// RediffByteExactDemotionExemptionCanaryTests.swift
//
// playhead-pzy2: source canary pinning that BOTH lexical / editorial demotion
// branches in `AdDetectionService.runBackfill` carry the byte-exact rediff
// exemption (`!refinedSpan.carriesRediffByteExactWidth`).
//
// The self-promo (fl4j) exemption is proven BEHAVIOURALLY end-to-end by
// `RediffByteExactDemotionExemptionTests`. The content-chapter (rxuv) exemption
// CANNOT be driven behaviourally on the simulator: `creatorChapterFusionEnabled`
// is read from a file-loaded `PreAnalysisConfig` (default OFF, no constructor
// override), so — exactly as the rxuv bead itself did with
// `AdDetectionServiceCreatorChapterFusionCanaryTests` — the structural invariant
// is pinned by source inspection instead.
//
// What this guards: a refactor that drops the exemption from EITHER branch would
// let a low-certainty lexical clue (a self-promo phrase, or a creator "content"
// chapter label) demote a byte-exact rediff span — a 100%-deterministic DAI
// divergence — violating "deterministic certainty outranks lexical clues". Both
// branches gate the suppressor call on the exemption condition, so it must appear
// in each `if` between the branch's opening condition and its `shouldSuppress`
// call.
//
// XCTest so the canary is filterable from the test plan (`xctestplan` silently
// ignores Swift Testing identifiers — see project memory
// `xctestplan_swift_testing_limitation`).

import Foundation
import XCTest

final class RediffByteExactDemotionExemptionCanaryTests: XCTestCase {

    /// The self-promo (fl4j) branch must gate `PromoSuppressor.shouldSuppress`
    /// on the byte-exact rediff exemption.
    func testSelfPromoBranchCarriesRediffByteExactExemption() throws {
        try assertExemptionPrecedes(
            suppressorCall: "PromoSuppressor.shouldSuppress",
            branchDescription: "self-promo (fl4j)"
        )
    }

    /// The content-chapter (rxuv) branch must gate
    /// `CreatorChapterSuppressionEvaluator.shouldSuppress` on the byte-exact
    /// rediff exemption. This branch is NOT behaviourally testable (its flag is
    /// file-loaded, default OFF), so this canary is the primary guard.
    func testContentChapterBranchCarriesRediffByteExactExemption() throws {
        try assertExemptionPrecedes(
            suppressorCall: "CreatorChapterSuppressionEvaluator.shouldSuppress",
            branchDescription: "content-chapter (rxuv)"
        )
    }

    // MARK: - Assertion

    /// Assert that `!refinedSpan.carriesRediffByteExactWidth` appears in the `if`
    /// condition list that guards `suppressorCall` — i.e. between the nearest
    /// preceding `if ` keyword and the suppressor call itself.
    private func assertExemptionPrecedes(
        suppressorCall: String,
        branchDescription: String
    ) throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)

        // Whitespace-tolerant anchor for the suppressor call.
        let anchorRegex = try NSRegularExpression(
            pattern: suppressorCall
                .replacingOccurrences(of: ".", with: #"\s*\.\s*"#)
        )
        guard
            let anchorMatch = anchorRegex.firstMatch(
                in: stripped, range: NSRange(stripped.startIndex..., in: stripped)
            ),
            let anchorRange = Range(anchorMatch.range, in: stripped)
        else {
            XCTFail("""
            Could not locate `\(suppressorCall)` in `runBackfill` — the \
            \(branchDescription) branch was removed or renamed (update this \
            canary, and confirm the exemption moved with it).
            """)
            return
        }

        // Slice back to the nearest preceding `if ` keyword: the exemption is a
        // condition in the SAME `if` as the suppressor call.
        let head = String(stripped[stripped.startIndex..<anchorRange.lowerBound])
        guard let ifRange = head.range(of: "if ", options: .backwards) else {
            XCTFail("""
            Could not locate the `if` opening the \(branchDescription) \
            suppression branch before `\(suppressorCall)` (update this canary).
            """)
            return
        }
        let condition = String(stripped[ifRange.lowerBound..<anchorRange.lowerBound])

        let exemptionRegex = try NSRegularExpression(
            pattern: #"!\s*refinedSpan\s*\.\s*carriesRediffByteExactWidth"#
        )
        XCTAssertNotNil(
            exemptionRegex.firstMatch(
                in: condition, range: NSRange(condition.startIndex..., in: condition)
            ),
            """
            The \(branchDescription) suppression branch in \
            `AdDetectionService.runBackfill` no longer guards on \
            `!refinedSpan.carriesRediffByteExactWidth` (playhead-pzy2). Without \
            it a byte-exact rediff span — a 100%-deterministic DAI divergence — \
            could be demoted by a low-certainty lexical clue. Restore the \
            exemption condition or update this canary if the provenance check \
            moved (and re-verify the exemption still holds behaviourally).
            """
        )
    }

    // MARK: - Helpers

    /// Loads `AdDetectionService.swift` and returns the brace-delimited body of
    /// the actor's `func runBackfill(` implementation. Mirrors
    /// `AdDetectionServiceSelfPromoSuppressionCanaryTests`' helper so a future
    /// restructure (actor → class, file rename) needs the canary files updated
    /// in lockstep.
    private static func runBackfillImplementationBody() throws -> String {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AdDetectionService.swift"
        )

        let actorAnchor = "actor AdDetectionService {"
        guard let actorRange = source.range(of: actorAnchor) else {
            throw NSError(
                domain: "RediffByteExactDemotionExemptionCanary", code: 1,
                userInfo: [NSLocalizedDescriptionKey: """
                Could not locate `\(actorAnchor)` in AdDetectionService.swift.
                """]
            )
        }

        let funcAnchor = "func runBackfill("
        guard let funcRange = source.range(
            of: funcAnchor, range: actorRange.upperBound..<source.endIndex
        ) else {
            throw NSError(
                domain: "RediffByteExactDemotionExemptionCanary", code: 2,
                userInfo: [NSLocalizedDescriptionKey: """
                Could not locate `\(funcAnchor)` after `\(actorAnchor)`.
                """]
            )
        }

        guard let openBrace = SwiftSourceInspector.findOpenBrace(
            in: source, after: funcRange.upperBound
        ) else {
            throw NSError(
                domain: "RediffByteExactDemotionExemptionCanary", code: 3,
                userInfo: [NSLocalizedDescriptionKey: """
                Could not locate opening `{` of \
                `actor AdDetectionService.runBackfill(...)`.
                """]
            )
        }

        return SwiftSourceInspector.bracedBody(in: source, startingAt: openBrace)
    }
}
