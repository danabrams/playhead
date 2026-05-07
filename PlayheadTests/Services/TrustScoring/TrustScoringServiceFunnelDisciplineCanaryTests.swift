// TrustScoringServiceFunnelDisciplineCanaryTests.swift
//
// q45f follow-up: source canary on the funnel-discipline rule documented
// at `TrustScoringService.evaluateDemotion`. The doc comment says
// "**no new production sites**" should call `evaluateDemotion` directly —
// production paths must funnel through `recordFalseSkipSignal` or
// `recordWeakFalseSkipSignal`, which serialize via the actor + run the
// state machine atomically with persistence. The symbol is `internal`
// (not `fileprivate`) because the replay-side q45f counterfactual gate
// (`Q45fReplayGate.replay` in the test target, reachable via
// `@testable import Playhead`) calls it directly. That carve-out is
// only safe if production never adds another caller. This canary
// enforces that contract by walking the production source tree.
//
// XCTest so the canary is filterable from the test plan
// (`xctestplan selectedTests/skippedTests` silently ignores Swift
// Testing identifiers — see project memory `xctestplan_swift_testing_limitation`).

import Foundation
import XCTest

final class TrustScoringServiceFunnelDisciplineCanaryTests: XCTestCase {

    /// Pin: the only production .swift file under `Playhead/` that
    /// references `evaluateDemotion` is `TrustScoringService.swift`
    /// itself. Any new production caller would silently bypass the
    /// actor's serialised mutation/persistence/logging coupling and
    /// re-open the q45f-class defect (state-machine drift between
    /// production and the parity-tested replay gate).
    func testNoProductionCallersOfEvaluateDemotionOutsideTrustScoringService() throws {
        guard let repoRoot = SwiftSourceInspector.repositoryRoot(from: #filePath) else {
            XCTFail("Could not locate repo root from \(#filePath)")
            return
        }
        let productionRoot = repoRoot.appendingPathComponent("Playhead")
        let allowedRelative = "Playhead/Services/TrustScoring/TrustScoringService.swift"

        let symbolRegex = try NSRegularExpression(pattern: #"\bevaluateDemotion\b"#)

        var offendingFiles: [String] = []

        guard let enumerator = FileManager.default.enumerator(
            at: productionRoot,
            includingPropertiesForKeys: [.isRegularFileKey],
            options: [.skipsHiddenFiles]
        ) else {
            XCTFail("FileManager could not enumerate \(productionRoot.path)")
            return
        }

        for case let url as URL in enumerator {
            guard url.pathExtension == "swift" else { continue }
            let relativePath = url.path.replacingOccurrences(of: repoRoot.path + "/", with: "")
            if relativePath == allowedRelative { continue }

            let source = try String(contentsOf: url, encoding: .utf8)
            // Strip both line comments AND string literals so a doc
            // comment mentioning `evaluateDemotion` (e.g. on
            // `recordWeakFalseSkipSignal`'s docstring) and a log-message
            // string literal don't false-trip this canary.
            let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
            let range = NSRange(stripped.startIndex..., in: stripped)
            if symbolRegex.firstMatch(in: stripped, range: range) != nil {
                offendingFiles.append(relativePath)
            }
        }

        XCTAssertTrue(
            offendingFiles.isEmpty,
            """
            New production caller(s) of `TrustScoringService.evaluateDemotion` \
            detected outside `\(allowedRelative)`. The doc comment on \
            `evaluateDemotion` documents why this is forbidden: production \
            mutation paths must funnel through `recordFalseSkipSignal` or \
            `recordWeakFalseSkipSignal` so the state-machine evaluation \
            stays coupled to the actor's serialised mutation, persistence, \
            and logging. The symbol is `internal` (not `fileprivate`) only \
            so the replay-side `Q45fReplayGate` parity suite can drive it \
            directly. Offending file(s): \(offendingFiles.joined(separator: ", ")). \
            Either route the new path through one of the two recorders, or, \
            if you really mean to add a third call site, update this \
            canary AND the parity gate to match.
            """
        )
    }

    /// Liveness check for the regex-based canary above: a rename refactor
    /// (e.g. `evaluateDemotion` → `applyDemotion`) would leave the
    /// `\bevaluateDemotion\b` walker matching zero files in production AND
    /// zero files outside `TrustScoringService.swift` — silently green,
    /// even though the funnel-discipline contract is no longer being
    /// enforced. This second test asserts the symbol still exists in the
    /// allowed source file. If `evaluateDemotion` is intentionally
    /// renamed, BOTH canaries must be updated together.
    func testEvaluateDemotionSymbolExistsInTrustScoringService() throws {
        guard let repoRoot = SwiftSourceInspector.repositoryRoot(from: #filePath) else {
            XCTFail("Could not locate repo root from \(#filePath)")
            return
        }
        let allowed = repoRoot
            .appendingPathComponent("Playhead")
            .appendingPathComponent("Services")
            .appendingPathComponent("TrustScoring")
            .appendingPathComponent("TrustScoringService.swift")
        let source = try String(contentsOf: allowed, encoding: .utf8)
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
        // Match a function declaration named `evaluateDemotion`, not just
        // any reference: a doc-comment mention can't survive comment
        // stripping, but a `@inlinable evaluateDemotion(...)` reference
        // could in principle appear without a `func` keyword. The
        // anchored `func\s+evaluateDemotion\b` pattern keeps the canary
        // honest about what we're protecting (the definition site).
        let defRegex = try NSRegularExpression(pattern: #"\bfunc\s+evaluateDemotion\b"#)
        let range = NSRange(stripped.startIndex..., in: stripped)
        XCTAssertNotNil(
            defRegex.firstMatch(in: stripped, range: range),
            """
            `func evaluateDemotion` not found in \
            Playhead/Services/TrustScoring/TrustScoringService.swift. \
            If you renamed the symbol, update BOTH this liveness test \
            and the regex pattern in \
            testNoProductionCallersOfEvaluateDemotionOutsideTrustScoringService \
            so the funnel-discipline canary continues to enforce the \
            single-call-site contract.
            """
        )
    }
}
