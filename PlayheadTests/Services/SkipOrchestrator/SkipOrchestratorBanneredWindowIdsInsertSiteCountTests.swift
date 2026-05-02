// SkipOrchestratorBanneredWindowIdsInsertSiteCountTests.swift
//
// Cycle-27 T-3: pin the production-writer count for the
// `banneredWindowIds` suppression gate. The cycle-23 H-1 / cycle-26
// L-1 / cycle-27 L-2 doc series establishes that this set is the
// SOLE production gate suppressing banner re-emission, and that it
// is written at exactly four code sites in `SkipOrchestrator.swift`:
//
//     1. `evaluateAndPush` terminal-state branch (the
//        decisionState == .applied arm)
//     2. `evaluateAndPush` promotion branch (decision becomes
//        .confirmed or .applied)
//     3. `injectUserMarkedAd` (manual user-correction entry point)
//     4. `beginEpisode` preload pre-population (suppresses cross-
//        launch banner re-fire for already-applied rows)
//
// A future contributor adding a fifth ad-hoc writer (e.g. a "silent
// skip" mode that bypasses `evaluateAndPush`) would silently weaken
// the suppression contract — the `emitBannerItem` comment guides
// readers but doesn't enforce. This canary enforces.
//
// Counts call expressions in CODE position only — `strippingCommentsAndStrings`
// is applied before the search. As of cycle-28 L-A, no comment in
// `SkipOrchestrator.swift` happens to contain the literal needle
// `banneredWindowIds.insert(`, so the stripper is currently a no-op
// for this file. The stripping step is retained as defensive future-
// proofing: a future contributor could legitimately mention the
// expression in a doc comment (e.g., quoting it in a contract note),
// and the canary must not double-count those mentions. The stripper's
// own correctness is pinned by `SwiftSourceInspectorStringStrippingTests`.
//
// XCTest (not Swift Testing) so the test is filterable from the
// project's xctestplan — see project memory
// `xctestplan_swift_testing_limitation`.

import Foundation
import XCTest

final class SkipOrchestratorBanneredWindowIdsInsertSiteCountTests: XCTestCase {

    func testBanneredWindowIdsInsertCallSiteCountIsExactlyFour() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/SkipOrchestrator/SkipOrchestrator.swift"
        )
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)

        let needle = "banneredWindowIds.insert("
        var count = 0
        var searchStart = stripped.startIndex
        while let range = stripped.range(of: needle, range: searchStart..<stripped.endIndex) {
            count += 1
            searchStart = range.upperBound
        }

        XCTAssertEqual(
            count, 4,
            """
            Cycle-27 T-3 regression: `banneredWindowIds.insert(` appears \
            \(count) times in code position in SkipOrchestrator.swift. The \
            documented contract (cycle-23 H-1 / cycle-26 L-1 / cycle-27 L-2) \
            is exactly 4 production writers:
              1. `evaluateAndPush` terminal-state branch
              2. `evaluateAndPush` promotion branch
              3. `injectUserMarkedAd`
              4. `beginEpisode` preload pre-population

            If a fifth writer was added intentionally, update this canary \
            AND the writer-list in `emitBannerItem`'s comment (search \
            `SkipOrchestrator.swift` for `Cycle-27 L-2`) AND add a per-site \
            anchor at the new insert (search `SkipOrchestrator.swift` for \
            `Cycle-27 T-3 production-writer site` to see the existing \
            anchors). If a writer was removed intentionally, do the same \
            in reverse. If neither is intentional, restore the missing site or \
            roll back the new one — the suppression contract is what \
            keeps the cross-launch banner from re-firing on every app \
            relaunch (cycle-21 H-1).
            """
        )
    }
}
