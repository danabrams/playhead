// AdDetectionServiceCreatorChapterFusionCanaryTests.swift
//
// playhead-rxuv (review-cycle): source canary on the load-bearing
// wire-up invariants for the creator-chapter-fusion branch inside
// `AdDetectionService.runBackfill`.
//
// The evaluator + builder + flag are all unit-tested in their own
// suites. What those suites cannot pin is the *call-site shape* — that
// the rxuv branch in `runBackfill` itself:
//
//   • snapshots `creatorChapterFusionEnabled` ONCE above the per-span
//     for-loop (per the bead's "snapshot at top of backfill" contract
//     so every iteration sees a stable value);
//   • passes `tagCreatorChapterSubSource:` to the recall-side builder;
//   • guards the precision-side branch on the severity check against
//     `SkipEligibilityGate.blockedByPolicy.severity` so a higher- or
//     equal-severity block (user correction, quorum) is preserved;
//   • only ever escalates the gate to `.blockedByPolicy` (never any
//     other value, never the opposite-direction `.eligible`);
//   • never mutates `proposalConfidence` / `skipConfidence` on the
//     suppression path (the "scoring stays honest" invariant).
//
// A future regression that, e.g., moves the flag-snapshot inside the
// loop (perf regression — re-reads UserDefaults per span via
// `preAnalysisConfig`), or replaces the severity guard with `==
// .eligible` (would lose the `.cappedByFMSuppression` upgrade), would
// not break any behavioral test that doesn't exercise the precise
// shape. The canaries below catch each such regression directly.
//
// XCTest so the canary is filterable from the test plan
// (`xctestplan` silently ignores Swift Testing identifiers — see
// project memory `xctestplan_swift_testing_limitation`).

import Foundation
import XCTest

final class AdDetectionServiceCreatorChapterFusionCanaryTests: XCTestCase {

    /// Pin the perf invariant: `creatorChapterFusionEnabled` is read
    /// exactly once from `preAnalysisConfig` inside `runBackfill`, and
    /// that single read sits BEFORE the per-span `for span in
    /// decodedSpans` loop. A regression that moves the read into the
    /// loop would re-read the config on every candidate span for no
    /// behavioral benefit.
    func testRunBackfillSnapshotsCreatorChapterFusionFlagOnceBeforeSpanLoop() throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)

        // Find every read of `preAnalysisConfig.creatorChapterFusionEnabled`
        // inside the function body. Whitespace-tolerant for swift-format reflows.
        let readRegex = try NSRegularExpression(
            pattern: #"preAnalysisConfig\s*\.\s*creatorChapterFusionEnabled"#
        )
        let readMatches = readRegex.matches(
            in: stripped,
            range: NSRange(stripped.startIndex..., in: stripped)
        )
        XCTAssertEqual(
            readMatches.count, 1,
            """
            `AdDetectionService.runBackfill` should read \
            `preAnalysisConfig.creatorChapterFusionEnabled` exactly \
            once. Found \(readMatches.count) read site(s). The bead \
            contract snapshots the flag ONCE at the top of the backfill \
            so every per-span iteration sees a stable value — duplicate \
            reads multiply the work and (more importantly) open a \
            torn-read window if the config is mutated mid-backfill.
            """
        )
        guard let readRange = readMatches.first?.range else { return }

        // Find the per-span `for span in decodedSpans` loop opener.
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
            this canary) or the per-span fusion loop has been removed.
            """)
            return
        }

        // The single read must occur BEFORE the for-loop.
        XCTAssertLessThan(
            readRange.location, loopMatch.range.location,
            """
            `AdDetectionService.runBackfill` reads \
            `preAnalysisConfig.creatorChapterFusionEnabled` INSIDE or \
            AFTER the per-span `for span in decodedSpans` loop (read at \
            offset \(readRange.location), loop at offset \
            \(loopMatch.range.location)). Hoist the snapshot back above \
            the for-loop so the flag is read once per backfill instead \
            of once per candidate span.
            """
        )
    }

    /// Pin the recall-side wiring: the builder call inside `runBackfill`
    /// MUST pass the snapshotted flag through
    /// `tagCreatorChapterSubSource:`. A regression that drops the
    /// argument would silently revert the entry to the pre-rxuv
    /// untagged shape on flag-ON episodes.
    func testRunBackfillFeedsTagCreatorChapterSubSourceArgument() throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        let argRegex = try NSRegularExpression(
            pattern: #"tagCreatorChapterSubSource\s*:\s*creatorChapterFusionEnabled\b"#
        )
        XCTAssertNotNil(
            argRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.runBackfill` no longer passes \
            `tagCreatorChapterSubSource: creatorChapterFusionEnabled` to \
            `ChapterMetadataEvidenceBuilder.buildEntries(...)`. Either \
            the parameter was renamed (update this canary) or the \
            recall-side stamp was unwired (in which case \
            `ChapterMetadataEvidenceBuilderTests` and the bead's \
            acceptance criteria are violated).
            """
        )
    }

    /// Pin the precision-side severity guard: the rxuv branch must
    /// gate on `decision.eligibilityGate.severity <
    /// SkipEligibilityGate.blockedByPolicy.severity` so that gates of
    /// equal or higher severity (`.blockedByEvidenceQuorum`,
    /// `.blockedByPolicy`, `.blockedByUserCorrection`) are preserved. A
    /// regression that drops the guard would let a content-chapter
    /// signal UNDO a user correction.
    func testRunBackfillGuardsSuppressionWithSeverityCheck() throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        // Whitespace-tolerant match for the exact guard the bead
        // contract requires.
        let guardRegex = try NSRegularExpression(
            pattern: #"decision\s*\.\s*eligibilityGate\s*\.\s*severity\s*<\s*SkipEligibilityGate\s*\.\s*blockedByPolicy\s*\.\s*severity"#
        )
        XCTAssertNotNil(
            guardRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.runBackfill` no longer guards the \
            creator-chapter suppression branch with \
            `decision.eligibilityGate.severity < \
            SkipEligibilityGate.blockedByPolicy.severity`. Without that \
            guard, a `.content`-chapter signal can undo a higher- or \
            equal-severity block (user correction, quorum). Restore the \
            guard or update this canary if the gate taxonomy moved \
            (and re-verify the user-correction invariant elsewhere).
            """
        )
    }

    /// Pin the precision-side outcome: when the rxuv suppression branch
    /// fires, it MUST set `eligibilityGate: .blockedByPolicy`. A
    /// regression that swaps to a weaker gate (e.g. `.markOnly`) or a
    /// totally different gate would silently weaken the suppression.
    func testRunBackfillSuppressionSetsBlockedByPolicy() throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        // Locate the rxuv suppression site by anchoring on the
        // evaluator call. The same `if` block must set the gate.
        let suppressRegex = try NSRegularExpression(
            pattern: #"CreatorChapterSuppressionEvaluator\s*\.\s*shouldSuppress"#
        )
        XCTAssertNotNil(
            suppressRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.runBackfill` no longer invokes \
            `CreatorChapterSuppressionEvaluator.shouldSuppress(...)`. \
            Either the evaluator was renamed (update this canary) or \
            the precision-side suppression branch was removed (in which \
            case the bead's primary value is gone and the acceptance \
            criteria are violated).
            """
        )

        let gateRegex = try NSRegularExpression(
            pattern: #"eligibilityGate\s*:\s*\.blockedByPolicy\b"#
        )
        XCTAssertNotNil(
            gateRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.runBackfill` no longer sets \
            `eligibilityGate: .blockedByPolicy` inside the rxuv \
            suppression branch. The bead contract demotes the gate \
            specifically to `.blockedByPolicy` so the proposal is \
            structurally indistinguishable from the existing \
            `applyFMSuppression` path. If the target gate has \
            intentionally moved, update this canary AND verify the \
            SkipOrchestrator's blocked-gate handling still does the \
            right thing for the new value.
            """
        )
    }

    /// Pin the "scores stay honest" invariant: the rxuv suppression
    /// site must thread `proposalConfidence` / `skipConfidence` from
    /// the existing `decision` value through unchanged. A regression
    /// that hard-codes a score (e.g. `proposalConfidence: 0.0`) or
    /// reaches for `rawDecision` would silently mutate the fusion
    /// scoring shape away from the documented contract.
    func testRunBackfillSuppressionPreservesDecisionScores() throws {
        let body = try Self.runBackfillImplementationBody()
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        // The suppression branch must include both score field
        // forwarders pointing at the in-scope `decision` value. The
        // alternative — `proposalConfidence: rawDecision.proposalConfidence`
        // — would silently revert any FM-suppression cap that happened
        // earlier in the same loop iteration.
        let proposalRegex = try NSRegularExpression(
            pattern: #"proposalConfidence\s*:\s*decision\s*\.\s*proposalConfidence\b"#
        )
        let skipRegex = try NSRegularExpression(
            pattern: #"skipConfidence\s*:\s*decision\s*\.\s*skipConfidence\b"#
        )
        XCTAssertNotNil(
            proposalRegex.firstMatch(in: stripped, range: strippedRange),
            """
            The rxuv suppression branch no longer forwards \
            `proposalConfidence: decision.proposalConfidence` — it \
            either was deleted, or now reaches for `rawDecision`/a \
            hard-coded score. Forwarding from `decision` preserves any \
            FM-suppression-capped score upstream; restore the forward.
            """
        )
        XCTAssertNotNil(
            skipRegex.firstMatch(in: stripped, range: strippedRange),
            """
            The rxuv suppression branch no longer forwards \
            `skipConfidence: decision.skipConfidence`. The "scores stay \
            honest" invariant requires the suppression site to leave \
            both confidences unchanged; restore the forward.
            """
        )
    }

    // MARK: - Helpers

    /// Loads `AdDetectionService.swift` and returns the brace-delimited
    /// body of the actor's `func runBackfill(` implementation — NOT the
    /// protocol declaration and NOT the extension's default-implementation
    /// overload. Mirrors `AdDetectionServicePriorHierarchyCanaryTests`'
    /// helper so a future restructure (actor → class, file rename) only
    /// needs one place updated.
    private static func runBackfillImplementationBody() throws -> String {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AdDetectionService.swift"
        )

        let actorAnchor = "actor AdDetectionService {"
        guard let actorRange = source.range(of: actorAnchor) else {
            throw NSError(
                domain: "AdDetectionServiceCreatorChapterFusionCanary",
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
                domain: "AdDetectionServiceCreatorChapterFusionCanary",
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
                domain: "AdDetectionServiceCreatorChapterFusionCanary",
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
