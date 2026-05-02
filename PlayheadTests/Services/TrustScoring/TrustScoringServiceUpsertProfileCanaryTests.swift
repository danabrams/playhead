// TrustScoringServiceUpsertProfileCanaryTests.swift
//
// skeptical-review-cycle-18 L-3 source canary: pin that
// `TrustScoringService.swift` never calls `store.upsertProfile(...)` —
// every read-modify-write of `podcast_profiles` from this service must
// route through one of the atomic AnalysisStore helpers
// (`mutateProfile`, `updateProfileIfExists`, or
// `updateProfileIfExistsCapturing`).
//
// Cycle-15 / cycle-17 closed two AdDetectionService callers that used
// the non-atomic `fetchProfile` → mutate → `upsertProfile` pair; that
// pair suspends the AnalysisStore actor between the read and the write
// and races against TrustScoringService writers (which run a tighter
// observation loop and so are the *typical* victim of the race).
// `TrustScoringService` itself was migrated to atomic helpers in
// cycle-1 (`recordSuccessfulObservation`, `setUserOverride`,
// `decayFalseSignals`, `recordFalseSkipSignal`,
// `recordFalseNegativeSignal`).
//
// Without this canary, a future "let me just split that for
// readability" refactor in any of those five methods would silently
// re-open the same lost-update window. The cycle-17 M-2 whole-file
// canary already pins the equivalent invariant on
// `AdDetectionService.swift`; this is the sibling pin on the other
// half of the writer pair.
//
// XCTest so the canary is filterable from the test plan
// (`xctestplan selectedTests/skippedTests` silently ignores Swift
// Testing identifiers — see project memory
// `xctestplan_swift_testing_limitation`).

import Foundation
import XCTest

final class TrustScoringServiceUpsertProfileCanaryTests: XCTestCase {

    /// Cycle-18 L-3: the body of `TrustScoringService.swift` MUST NOT
    /// contain ANY direct call to `store.upsertProfile(...)`. The
    /// service's five write paths
    /// (`recordSuccessfulObservation`, `recordFalseSkipSignal`,
    /// `recordFalseNegativeSignal`, `setUserOverride`,
    /// `decayFalseSignals`) all currently route through atomic
    /// helpers; this canary catches a regression in any of them — or
    /// in a future sibling write path — without needing per-method
    /// body scans (which a future caller could trivially evade by
    /// adding a new method).
    func testTrustScoringServiceNeverDirectlyCallsUpsertProfile() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/TrustScoring/TrustScoringService.swift"
        )
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        let upsertRegex = try NSRegularExpression(
            pattern: Self.upsertProfilePattern
        )
        let matches = upsertRegex.matches(in: stripped, range: strippedRange)

        XCTAssertEqual(
            matches.count, 0,
            """
            `TrustScoringService.swift` re-introduced \(matches.count) \
            direct call(s) to `store.upsertProfile(...)`. Every \
            podcast-profile mutation from this service must funnel \
            through an atomic AnalysisStore helper:
              • `store.mutateProfile(podcastId:create:update:)` — \
                lazy-creates the row if missing.
              • `store.updateProfileIfExists(podcastId:update:)` — \
                returns nil when the row is missing (no lazy-create).
              • `store.updateProfileIfExistsCapturing(podcastId:update:)` \
                — same as updateProfileIfExists but rides a Sendable \
                value back across the actor hop.
            Cycle-15 / cycle-17: the non-atomic `fetchProfile` → \
            `upsertProfile` pair suspends the AnalysisStore actor \
            between read and write, opening a lost-update race against \
            any concurrent writer (most commonly the AdDetectionService \
            backfill path). `TrustScoringService` is the *high-frequency* \
            writer in this race — restoring an `upsertProfile` here \
            would re-open the window the cycle-1/cycle-15/cycle-17 \
            sequence closed.
            """
        )
    }

    /// Cycle-18 L-1 positive control (mirrored on this canary): run
    /// the SAME regex against fixture strings and assert ≥1 match per
    /// positive fixture. Without a positive control, a future Swift
    /// formatter or rename refactor that breaks the pattern (e.g.
    /// `store` is renamed to `analysisStore`) would silently turn the
    /// canary into "0 expected, 0 found — but now blind".
    func testUpsertProfileRegexCatchesItsOwnTarget() throws {
        let regex = try NSRegularExpression(
            pattern: Self.upsertProfilePattern
        )

        let positiveFixtures: [String] = [
            // Original `await store.upsertProfile(` shape.
            "await store.upsertProfile(profile)",
            // Whitespace variants — `await   store .  upsertProfile  (`.
            "await   store .  upsertProfile  (profile)",
            // Multi-line break between `await` and `store`.
            "await\n    store.upsertProfile(profile)",
            // Synchronous variant — proves the broadened pattern
            // catches a `try store.upsertProfile(...)` regression where
            // `await` has been dropped.
            "try store.upsertProfile(profile)",
            // Bare `store.upsertProfile(...)` (e.g. inside a closure).
            "store.upsertProfile(profile)",
        ]

        for fixture in positiveFixtures {
            let range = NSRange(fixture.startIndex..., in: fixture)
            let count = regex.numberOfMatches(in: fixture, range: range)
            XCTAssertGreaterThanOrEqual(
                count, 1,
                """
                Cycle-18 L-3 positive control failed: the canary regex \
                `\(Self.upsertProfilePattern)` no longer matches the \
                fixture `\(fixture)` that it was designed to catch. \
                The whole-file canary above will still pass against the \
                production source, but only because the regex has gone \
                blind. Update the pattern (and this fixture together) so \
                the canary actually exercises its target shape.
                """
            )
        }

        let negativeFixtures: [String] = [
            // The atomic helpers are CORRECT — they must never be flagged.
            "await store.mutateProfile(podcastId: \"a\") { existing in existing }",
            "await store.updateProfileIfExists(podcastId: \"b\") { existing in existing }",
            "await store.updateProfileIfExistsCapturing(podcastId: \"c\") { existing in (existing, nil) }",
            // Different store entity.
            "await store.fetchProfile(podcastId: \"d\")",
        ]

        for fixture in negativeFixtures {
            let range = NSRange(fixture.startIndex..., in: fixture)
            let count = regex.numberOfMatches(in: fixture, range: range)
            XCTAssertEqual(
                count, 0,
                """
                Cycle-18 L-3 negative control failed: the canary regex \
                over-matched the fixture `\(fixture)`, which is NOT a \
                direct-upsertProfile pattern. A previous broadening of \
                the regex has gone too far. Tighten the pattern.
                """
            )
        }
    }

    /// Pattern shared by the production-source assertion and the
    /// positive/negative controls so the negative whole-file check and
    /// its proof of reach stay in lockstep. Whitespace-tolerant on
    /// both sides of the `.`, and broadened past `await\s+` per the
    /// cycle-18 L-2 lesson on the AdDetection sibling canary
    /// (synchronous overloads, multi-line breaks, and bare-closure
    /// callers all need to be caught).
    private static let upsertProfilePattern: String =
        #"\bstore\s*\.\s*upsertProfile\s*\("#
}
