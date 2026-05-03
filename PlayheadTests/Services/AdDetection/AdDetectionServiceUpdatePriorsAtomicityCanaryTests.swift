// AdDetectionServiceUpdatePriorsAtomicityCanaryTests.swift
//
// skeptical-review-cycle-16 L-2 missing-test: source canary on the
// load-bearing atomic-merge contract for
// `AdDetectionService.updatePriors(podcastId:nonSuppressedWindows:episodeDuration:)`.
//
// Cycle-15 M-1 documented why the body MUST funnel its read-modify-
// write of `podcast_profiles` through `AnalysisStore.mutateProfile`:
// the previous `fetchProfile` → mutate-locally → `upsertProfile` pair
// suspended the actor between the read and the write, opening a lost-
// update race against any concurrent `TrustScoringService` writer
// (today: `recordSuccessfulObservation`, `recordFalseSkipSignal`).
// Cycle-15 M-2 documented why the same closure MUST carry
// `existing.traitProfileJSON` forward inside the `update` closure:
// the `upsertProfile` SQL writes `traitProfileJSON = excluded.traitProfileJSON`
// (NOT COALESCE), so any naïve constructor that omits the field
// silently nils the persisted trait profile.
//
// A future "let me just split that for readability" refactor that
// reverts to the `await store.fetchProfile` / `await store.upsertProfile`
// pair would still compile and most behavioral tests would still
// pass — the race is timing-sensitive and the M-2 producer is currently
// dormant. A direct source-level pin is the cheapest regression net.
//
// XCTest so the canary is filterable from the test plan
// (`xctestplan selectedTests/skippedTests` silently ignores Swift
// Testing identifiers — see project memory `xctestplan_swift_testing_limitation`).

import Foundation
import XCTest

final class AdDetectionServiceUpdatePriorsAtomicityCanaryTests: XCTestCase {

    /// Cycle-16 L-2: pin that `AdDetectionService.updatePriors`'s body
    /// performs all `podcast_profiles` mutation through the atomic
    /// `store.mutateProfile(podcastId:create:update:)` helper, and that
    /// it does NOT call the non-atomic `store.fetchProfile` /
    /// `store.upsertProfile` pair (which would re-open the cycle-15 M-1
    /// lost-update race).
    func testUpdatePriorsBodyUsesMutateProfileNotFetchUpsertPair() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AdDetectionService.swift"
        )

        // Locate `func updatePriors(`. Anchor on the leading whitespace
        // / `private` qualifier to avoid matching a future `func
        // updatePriorsFromObservation(` or similar prefix-collision.
        let funcAnchor = "func updatePriors("
        guard let funcRange = source.range(of: funcAnchor) else {
            XCTFail("Could not locate `\(funcAnchor)` in AdDetectionService.swift")
            return
        }
        guard let openBrace = source[funcRange.upperBound...].firstIndex(of: "{") else {
            XCTFail("Could not locate `{` after `\(funcAnchor)`")
            return
        }
        let body = SwiftSourceInspector.bracedBody(in: source, startingAt: openBrace)
        // Strip both comments AND string literals — a doc comment
        // mentioning `await store.fetchProfile` (e.g. "we used to call
        // fetchProfile here") must not false-trip the negative checks
        // below, and a log message string literal must not either.
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)

        // Positive: the body MUST funnel through `mutateProfile`. The
        // call site is `try await store.mutateProfile(` — match
        // whitespace-tolerantly so a swift-format reflow or a
        // wrapped-arg variant (`store . mutateProfile (`) still trips
        // the canary on a regression rather than on cosmetic edits.
        let mutateCallRegex = try NSRegularExpression(
            pattern: #"store\s*\.\s*mutateProfile\s*\("#
        )
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)
        XCTAssertNotNil(
            mutateCallRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.updatePriors(...)` no longer routes its \
            podcast-profile read-modify-write through \
            `store.mutateProfile(podcastId:create:update:)` (whitespace-\
            tolerant match). Cycle-15 M-1 documented why the atomic helper \
            is required: a `fetchProfile` → mutate → `upsertProfile` pair \
            suspends the AnalysisStore actor between the read and the \
            write, opening a lost-update race against \
            `TrustScoringService.recordSuccessfulObservation` / \
            `.recordFalseSkipSignal`. Restore the `mutateProfile` call \
            OR update this canary if the atomic-merge entry point \
            intentionally moved.
            """
        )

        // Negative: the body MUST NOT contain `await store.fetchProfile`
        // or `await store.upsertProfile`. These are the two halves of
        // the non-atomic pair that cycle-15 M-1 explicitly closed.
        // Whitespace-tolerant so a reflow doesn't smuggle the regression
        // past the canary.
        let fetchProfileRegex = try NSRegularExpression(
            pattern: #"await\s+store\s*\.\s*fetchProfile\s*\("#
        )
        XCTAssertNil(
            fetchProfileRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.updatePriors(...)` re-introduced an \
            `await store.fetchProfile(...)` call (whitespace-tolerant \
            match). Cycle-15 M-1: the fetchProfile / upsertProfile pair \
            suspends the AnalysisStore actor between read and write, \
            re-opening the lost-update race against TrustScoringService. \
            Use `store.mutateProfile(podcastId:create:update:)` instead.
            """
        )

        let upsertProfileRegex = try NSRegularExpression(
            pattern: #"await\s+store\s*\.\s*upsertProfile\s*\("#
        )
        XCTAssertNil(
            upsertProfileRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.updatePriors(...)` re-introduced an \
            `await store.upsertProfile(...)` call (whitespace-tolerant \
            match). Cycle-15 M-1: the fetchProfile / upsertProfile pair \
            suspends the AnalysisStore actor between read and write, \
            re-opening the lost-update race against TrustScoringService. \
            Use `store.mutateProfile(podcastId:create:update:)` instead. \
            (Cycle-15 M-2 also relied on the `update` closure carrying \
            `existing.traitProfileJSON` forward — a bare `upsertProfile` \
            with a default-`nil` `traitProfileJSON` field would silently \
            clobber the persisted trait profile because `upsertProfile` \
            does NOT COALESCE that column.)
            """
        )
    }

    /// Cycle-16 L-2 / cycle-15 M-2: pin that the `update` closure inside
    /// `updatePriors` carries `existing.traitProfileJSON` forward in the
    /// new `PodcastProfile` constructor it returns. A future refactor
    /// that drops this carry-forward (e.g. by going back to the default
    /// `traitProfileJSON: nil` initializer arg) would silently nil the
    /// persisted trait profile because `AnalysisStore.upsertProfile`'s
    /// SQL writes `traitProfileJSON = excluded.traitProfileJSON` (NOT
    /// COALESCE — confirmed at `AnalysisStore.swift` upsert path).
    func testUpdatePriorsCarriesExistingTraitProfileJSONForward() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AdDetectionService.swift"
        )
        guard let funcRange = source.range(of: "func updatePriors(") else {
            XCTFail("Could not locate `func updatePriors(` in AdDetectionService.swift")
            return
        }
        guard let openBrace = source[funcRange.upperBound...].firstIndex(of: "{") else {
            XCTFail("Could not locate `{` after `func updatePriors(`")
            return
        }
        let body = SwiftSourceInspector.bracedBody(in: source, startingAt: openBrace)

        // Cycle-20 L-1: scope the carry-forward check to the `update:`
        // closure body specifically, not the whole function body. A
        // sibling assignment of `traitProfileJSON: <ident>.traitProfileJSON`
        // outside the closure (e.g. in a future journal-write helper)
        // would otherwise satisfy the regex while the load-bearing
        // carry-forward inside the closure could be silently removed.
        // The closure opens immediately after `existing in`; we find
        // that token and walk back to the preceding `{` (which is the
        // closure's opening brace).
        let updateBody = try Self.closureBodyContainingExistingIn(in: body)
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(updateBody)

        let traitCarryRegex = try NSRegularExpression(
            // Cycle-19 L-2: broadened past the literal `existing` LHS.
            // The canonical closure-arg name is `existing`, but a future
            // refactor that aliases (e.g.
            // `let snapshot = existing; ... traitProfileJSON: snapshot.traitProfileJSON`)
            // is still correct — the carry-forward is what matters, not
            // the identifier name. Match `<ident>.traitProfileJSON` where
            // `<ident>` is any single Swift identifier.
            pattern: Self.traitProfileCarryForwardPattern
        )
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)
        XCTAssertNotNil(
            traitCarryRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.updatePriors(...)`'s `update` closure no \
            longer carries `existing.traitProfileJSON` forward into the \
            new `PodcastProfile(...)` constructor (whitespace-tolerant \
            match, scoped to the closure body). Cycle-15 M-2: \
            `AnalysisStore.upsertProfile`'s SQL writes \
            `traitProfileJSON = excluded.traitProfileJSON` (NOT COALESCE), \
            so a default-`nil` value would silently nil the persisted \
            trait profile. Restore the carry-forward OR update this \
            canary if the closure has intentionally moved to a different \
            field-preservation strategy (e.g. a partial-update store \
            helper that COALESCEs).
            """
        )
    }

    /// Cycle-17 M-1: pin the sibling fix in `recordListenRewind`. The
    /// production "Listen" tap path (NowPlayingViewModel → PlayheadRuntime
    /// → AdDetectionService.recordListenRewind) had the same two cycle-15
    /// defects (lost-update race + traitProfileJSON clobber) until cycle-17.
    /// Pin both invariants here:
    ///   1. Body MUST funnel through `store.updateProfileIfExists` (the
    ///      no-lazy-create sibling of `mutateProfile` — this method
    ///      early-returns when the row is missing, by design).
    ///   2. Body MUST NOT contain the non-atomic `await store.fetchProfile`
    ///      / `await store.upsertProfile` pair.
    ///   3. The closure MUST carry `existing.traitProfileJSON` forward
    ///      into the new `PodcastProfile(...)` constructor.
    func testRecordListenRewindBodyUsesUpdateProfileIfExistsAndCarriesTraitJSON() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AdDetectionService.swift"
        )
        let funcAnchor = "func recordListenRewind("
        guard let funcRange = source.range(of: funcAnchor) else {
            XCTFail("Could not locate `\(funcAnchor)` in AdDetectionService.swift")
            return
        }
        guard let openBrace = source[funcRange.upperBound...].firstIndex(of: "{") else {
            XCTFail("Could not locate `{` after `\(funcAnchor)`")
            return
        }
        let body = SwiftSourceInspector.bracedBody(in: source, startingAt: openBrace)
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(body)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        // Positive: body MUST call `store.updateProfileIfExists`.
        let updateIfExistsRegex = try NSRegularExpression(
            pattern: #"store\s*\.\s*updateProfileIfExists\s*\("#
        )
        XCTAssertNotNil(
            updateIfExistsRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.recordListenRewind(...)` no longer routes \
            its podcast-profile read-modify-write through \
            `store.updateProfileIfExists(podcastId:update:)`. Cycle-17 M-1: \
            the prior `fetchProfile` → mutate → `upsertProfile` pair \
            suspended the AnalysisStore actor between read and write, \
            opening the same lost-update race against TrustScoringService \
            that cycle-15 closed in `updatePriors`. Restore the atomic \
            helper OR update this canary if the entry point intentionally \
            moved.
            """
        )

        // Negative: body MUST NOT contain `await store.fetchProfile(` or
        // `await store.upsertProfile(`.
        let fetchProfileRegex = try NSRegularExpression(
            pattern: #"await\s+store\s*\.\s*fetchProfile\s*\("#
        )
        XCTAssertNil(
            fetchProfileRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.recordListenRewind(...)` re-introduced an \
            `await store.fetchProfile(...)` call. Cycle-17 M-1: this is \
            the read half of the non-atomic pair that races against \
            TrustScoringService writers. Use \
            `store.updateProfileIfExists(podcastId:update:)` instead.
            """
        )

        let upsertProfileRegex = try NSRegularExpression(
            pattern: #"await\s+store\s*\.\s*upsertProfile\s*\("#
        )
        XCTAssertNil(
            upsertProfileRegex.firstMatch(in: stripped, range: strippedRange),
            """
            `AdDetectionService.recordListenRewind(...)` re-introduced an \
            `await store.upsertProfile(...)` call. Cycle-17 M-1: this is \
            the write half of the non-atomic pair (and a bare upsert with \
            a default-`nil` `traitProfileJSON` field would silently \
            clobber the persisted trait profile because `upsertProfile` \
            does NOT COALESCE that column). Use \
            `store.updateProfileIfExists(podcastId:update:)` instead.
            """
        )

        // Cycle-20 L-1: scope the carry-forward check to the closure
        // body specifically (the trailing closure on
        // `updateProfileIfExists`). See the rationale in
        // `testUpdatePriorsCarriesExistingTraitProfileJSONForward`.
        let updateClosureBody = try Self.closureBodyContainingExistingIn(in: body)
        let updateClosureStripped = SwiftSourceInspector.strippingCommentsAndStrings(updateClosureBody)
        let updateClosureRange = NSRange(updateClosureStripped.startIndex..., in: updateClosureStripped)

        // Positive: closure MUST carry `existing.traitProfileJSON` forward.
        let traitCarryRegex = try NSRegularExpression(
            // Cycle-19 L-2: broadened past the literal `existing` LHS.
            // The canonical closure-arg name is `existing`, but a future
            // refactor that aliases (e.g.
            // `let snapshot = existing; ... traitProfileJSON: snapshot.traitProfileJSON`)
            // is still correct — the carry-forward is what matters, not
            // the identifier name. Match `<ident>.traitProfileJSON` where
            // `<ident>` is any single Swift identifier.
            pattern: Self.traitProfileCarryForwardPattern
        )
        XCTAssertNotNil(
            traitCarryRegex.firstMatch(in: updateClosureStripped, range: updateClosureRange),
            """
            `AdDetectionService.recordListenRewind(...)`'s `update` closure \
            no longer carries `existing.traitProfileJSON` forward \
            (whitespace-tolerant match, scoped to the closure body). \
            Cycle-17 M-1: the same non-COALESCE `traitProfileJSON` \
            clobber that cycle-15 M-2 closed in `updatePriors` lived here \
            until cycle-17. Restore the carry-forward.
            """
        )
    }

    // MARK: - Cycle-20 L-1 helper

    /// Returns the body of the closure whose parameter binding is
    /// `existing in`. Anchors on the `existing in` token, walks back to
    /// the preceding `{` (which must be the closure's opening brace,
    /// because nothing else can legally precede the closure parameter
    /// list), and uses `bracedBody` to extract the body.
    ///
    /// To stay safe against `{` characters that might appear inside
    /// comments or string literals between the closure brace and
    /// `existing in`, we walk back on a comment- and string-stripped
    /// copy of the source (where comments/strings have been blanked to
    /// spaces, preserving offsets), then call `bracedBody` on the
    /// original source at the located index. `bracedBody` itself is
    /// already comment/string-aware, so it correctly walks the body.
    ///
    /// Throws if the source shape doesn't match (caller should treat
    /// that as a sign the canary's anchor signature has drifted).
    ///
    /// Cycle-21 M-1: throws when there is more than one `existing in`
    /// occurrence in `source`. The helper anchors on the FIRST match
    /// and walks back to the preceding `{`; if a sibling closure (e.g.
    /// `mergeSlotPositions` further down `AdDetectionService.swift`
    /// uses `existing in` inside a `.map` over slot positions) shares
    /// the token, the caller would silently get the body of the wrong
    /// closure and the canary would go blind. Forcing the caller to
    /// disambiguate (or restructure the fixture) preserves the L-1
    /// scoping guarantee.
    private static func closureBodyContainingExistingIn(in source: String) throws -> String {
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
        let occurrences = stripped.components(separatedBy: "existing in").count - 1
        guard occurrences == 1 else {
            throw NSError(domain: "ClosureScope", code: 3, userInfo: [
                NSLocalizedDescriptionKey: """
                Expected exactly one `existing in` token in the source, \
                found \(occurrences). The helper is not safe to use on \
                inputs that contain multiple sibling `existing in` \
                closures — it would silently anchor on the FIRST match \
                and let later regressions hide. Either narrow the input \
                to a single closure (preferred) or extend the helper to \
                accept a disambiguating selector.
                """
            ])
        }
        guard let token = stripped.range(of: "existing in") else {
            throw NSError(domain: "ClosureScope", code: 1, userInfo: [
                NSLocalizedDescriptionKey: "Could not locate `existing in` in body"
            ])
        }
        // Walk backward from the `existing` token to find the preceding
        // `{` in the stripped copy (so a `{` inside a leading comment
        // can't fool the walk-back). Stripped indices align with `source`
        // because stripping preserves length.
        var i = stripped.index(before: token.lowerBound)
        while i > stripped.startIndex && stripped[i] != "{" {
            i = stripped.index(before: i)
        }
        guard stripped[i] == "{" else {
            throw NSError(domain: "ClosureScope", code: 2, userInfo: [
                NSLocalizedDescriptionKey: "Could not locate preceding `{` for `existing in` closure"
            ])
        }
        // Translate the stripped index back to a source index by offset
        // (stripped and source are guaranteed to have identical lengths
        // by construction of strippingCommentsAndStrings).
        let offset = stripped.distance(from: stripped.startIndex, to: i)
        let sourceIndex = source.index(source.startIndex, offsetBy: offset)
        return SwiftSourceInspector.bracedBody(in: source, startingAt: sourceIndex)
    }

    /// Cycle-17 M-2 / cycle-18 L-2: whole-file scan for direct calls
    /// to `store.fetchProfile` or `store.upsertProfile` anywhere in
    /// `AdDetectionService.swift`. The two body-scoped canaries above
    /// pin the two known callers; this whole-file scan catches a future
    /// sibling method that re-introduces the pattern (which the
    /// scoped canaries cannot detect by construction).
    ///
    /// **Cycle-18 L-2 broadening:** the regex no longer requires `await`
    /// immediately before `store`. The cycle-17 form
    /// (`await\s+store\s*\.\s*(?:fetchProfile|upsertProfile)\s*\(`)
    /// would silently miss:
    ///   • a future synchronous overload (`try store.fetchProfile(...)`),
    ///   • an aliased capture (`let s = self.store; await s.fetchProfile(...)`),
    ///   • a multi-line break (`await\n    store.fetchProfile(`).
    /// Dropping the `await\s+` prefix and using `\b` to anchor the
    /// `store` token closes those silent-drift cases. Aliased captures
    /// (where the variable isn't named `store`) are still missed —
    /// covered as a residual risk in the cycle-18 review notes.
    ///
    /// If a future caller has a *legitimate* reason to use the pair —
    /// e.g. it must explicitly NOT lazy-create AND must capture
    /// arbitrary `Sendable` state across the hop in a shape that
    /// `updateProfileIfExistsCapturing` cannot express — extend the
    /// allow-list below with the function's name and rationale.
    func testAdDetectionServiceNeverDirectlyCallsFetchOrUpsertProfile() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AdDetectionService.swift"
        )
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        // Currently empty: every read-modify-write path goes through an
        // atomic store helper. Add a function name here ONLY with a
        // documented rationale for why the non-atomic pair is required.
        let allowedFunctionNames: Set<String> = []

        let pairRegex = try NSRegularExpression(
            pattern: Self.fetchOrUpsertProfilePairPattern
        )
        let matches = pairRegex.matches(in: stripped, range: strippedRange)

        if !allowedFunctionNames.isEmpty {
            // Future-proofing: if an allow-listed caller exists, the
            // canary author must hand-verify that every match falls
            // inside one of those bodies. We don't try to do that
            // automatically here because the function-body lookup is
            // already covered by the body-scoped canaries above.
            XCTFail(
                """
                Allow-list path is not implemented because no callers \
                are currently allow-listed. If you are adding the first \
                allow-listed caller, extend this test to verify each \
                match falls inside a body whose function name is in \
                `allowedFunctionNames`.
                """
            )
            return
        }

        XCTAssertEqual(
            matches.count, 0,
            """
            `AdDetectionService.swift` re-introduced \(matches.count) call(s) \
            to the non-atomic `store.fetchProfile(...)` / \
            `store.upsertProfile(...)` pair. Cycle-17 M-2: this \
            whole-file canary catches a sibling regression of the \
            cycle-15 / cycle-17 atomicity invariant in any future method. \
            Use `store.mutateProfile(...)` (lazy-create variant) or \
            `store.updateProfileIfExists(...)` (no-lazy-create variant) \
            instead. If the new caller has a legitimate need for the \
            non-atomic pair, add its function name to \
            `allowedFunctionNames` above with a documented rationale.
            """
        )
    }

    /// Cycle-18 L-1 positive control: the cycle-17 M-2 canary above
    /// asserts a count of *zero* matches against the production source.
    /// Without a positive control, a future Swift formatter or
    /// renaming refactor that breaks the pattern (e.g. `store` is
    /// renamed to `analysisStore`) would silently turn the canary into
    /// "0 expected, 0 found — but now blind". This test runs the SAME
    /// regex against a tiny fixture string that contains exactly the
    /// patterns the canary is meant to catch, and asserts ≥1 match for
    /// each variant. If this test fails, the canary regex no longer
    /// recognises its own target — fix the pattern (or, if the
    /// production code's call shape genuinely changed, update the
    /// pattern AND this fixture together).
    func testFetchOrUpsertProfilePairRegexCatchesItsOwnTarget() throws {
        let regex = try NSRegularExpression(
            pattern: Self.fetchOrUpsertProfilePairPattern
        )

        // Each fixture line is a call shape the canary MUST catch.
        // Cycle-18 L-2 broadened the pattern past `await\s+store` —
        // the synchronous, aliased-via-`store`, and multi-line variants
        // below verify that broadening has the intended reach.
        let positiveFixtures: [String] = [
            // Original cycle-17 shape.
            "await store.fetchProfile(podcastId: \"foo\")",
            "await store.upsertProfile(profile)",
            // Whitespace variants — `await   store .  fetchProfile  (`.
            "await   store .  fetchProfile  (podcastId: \"x\")",
            // Multi-line break between `await` and `store` (a future
            // swift-format reflow could legitimately produce this).
            "await\n    store.fetchProfile(podcastId: \"y\")",
            // Hypothetical synchronous variant — proves the broadened
            // pattern catches the case where `await` has been dropped.
            "try store.upsertProfile(profile)",
            "store.fetchProfile(podcastId: \"z\")",
        ]

        for fixture in positiveFixtures {
            let range = NSRange(fixture.startIndex..., in: fixture)
            let count = regex.numberOfMatches(in: fixture, range: range)
            XCTAssertGreaterThanOrEqual(
                count, 1,
                """
                Cycle-18 L-1 positive control failed: the canary regex \
                `\(Self.fetchOrUpsertProfilePairPattern)` no longer \
                matches the fixture `\(fixture)` that it was designed \
                to catch. The cycle-17 M-2 whole-file canary above \
                will still pass against the production source, but \
                only because the regex has gone blind. Update the \
                pattern (and this fixture together) so the canary \
                actually exercises its target shape.
                """
            )
        }

        // Negative fixtures: shapes that look superficially similar
        // but should NOT trip the canary. If a future broadening
        // accidentally over-matches, this catches it.
        let negativeFixtures: [String] = [
            // `mutateProfile` and `updateProfileIfExists` are the
            // *correct* atomic helpers — they must never be flagged.
            "await store.mutateProfile(podcastId: \"a\") { existing in existing }",
            "await store.updateProfileIfExists(podcastId: \"b\") { existing in existing }",
            // Different store entity — a `store.fetchAsset(` call
            // legitimately exists elsewhere and is not the target.
            "await store.fetchAsset(id: \"c\")",
            // `fetchProfile` on a name-suffixed identifier:
            // `profileStore.fetchProfile(` does NOT trip the canary
            // because `\b` requires a word boundary before `store` and
            // `e→s` is a word-to-word transition (no boundary). Pin
            // that intentional non-match here so a future broadening
            // that drops the `\b` (or switches to a case-insensitive
            // pattern that would catch the capital `Store` prefix)
            // gets caught by this control.
            "await otherService.profileStore.fetchProfile(podcastId: \"d\")",
        ]

        for fixture in negativeFixtures {
            let range = NSRange(fixture.startIndex..., in: fixture)
            let count = regex.numberOfMatches(in: fixture, range: range)
            XCTAssertEqual(
                count, 0,
                """
                Cycle-18 L-1 negative control failed: the canary \
                regex over-matched the fixture `\(fixture)`, which \
                is NOT a non-atomic-pair pattern. A previous \
                broadening of the regex has gone too far. Tighten \
                the pattern.
                """
            )
        }
    }

    /// Cycle-22 L-5 sibling canary: every closure body in
    /// `AdDetectionService.swift` whose parameter is `existing` AND
    /// whose body constructs a new `PodcastProfile(...)` MUST carry
    /// `existing.traitProfileJSON` forward.
    ///
    /// Background: cycles 15 M-2, 17 M-1, 18 L-2 etc. have repeatedly
    /// closed body-scoped instances of the same defect — a `PodcastProfile`
    /// constructor inside an `update` closure that omits the
    /// `traitProfileJSON:` field, which then silently nils the persisted
    /// trait profile because `upsertProfile`'s SQL writes
    /// `traitProfileJSON = excluded.traitProfileJSON` (NOT COALESCE).
    ///
    /// The two existing scoped canaries
    /// (`testUpdatePriorsCarriesExistingTraitProfileJSONForward`,
    /// `testRecordListenRewindBodyUsesUpdateProfileIfExistsAndCarriesTraitJSON`)
    /// pin the two known callers individually. This whole-file canary
    /// catches a future *third* caller — a new method that `existing in`-
    /// constructs a `PodcastProfile` and forgets the carry-forward —
    /// which neither scoped canary can see by construction.
    ///
    /// Closures whose body does NOT construct `PodcastProfile(` are
    /// skipped (e.g. AdWindow filters and numeric map closures that
    /// happen to use the parameter name `existing`); they have no
    /// trait-profile invariant to violate.
    func testEveryProfileConstructingExistingInClosureCarriesTraitJSON() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AdDetectionService.swift"
        )
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)

        let traitCarryRegex = try NSRegularExpression(
            pattern: Self.traitProfileCarryForwardPattern
        )

        var searchStart = stripped.startIndex
        var profileConstructingClosureCount = 0
        var totalExistingInCount = 0

        while let tokenRange = stripped.range(
            of: "existing in",
            range: searchStart..<stripped.endIndex
        ) {
            totalExistingInCount += 1
            // Cycle-23 L-6: shared walk-back helper. See
            // `closureBodyStripped(forExistingInTokenAt:...)` for the
            // L-5 nested-closure caveat and the L-7 Character-grapheme
            // alignment precondition.
            guard let bodyStripped = Self.closureBodyStripped(
                forExistingInTokenAt: tokenRange,
                sourceText: source,
                strippedText: stripped
            ) else {
                XCTFail(
                    """
                    Could not locate the opening `{` for an `existing in` \
                    occurrence at offset \
                    \(stripped.distance(from: stripped.startIndex, to: tokenRange.lowerBound)). \
                    The whole-file canary cannot run if any closure's \
                    bracket structure is unparseable; tighten the source \
                    or extend `bracedBody` / this walk-back.
                    """
                )
                return
            }

            // Only closures that actually construct `PodcastProfile(...)`
            // carry the trait-profile invariant. Filters, numeric maps,
            // and any future non-profile closures are correctly skipped.
            // Cycle-23 L-4: word-boundary anchor (`\bPodcastProfile\s*\(`)
            // so a hypothetical sibling type like `MyPodcastProfile(`
            // doesn't get pulled into scope by a substring match.
            if Self.bodyConstructsPodcastProfile(bodyStripped) {
                profileConstructingClosureCount += 1
                let bodyRange = NSRange(
                    bodyStripped.startIndex..., in: bodyStripped
                )
                XCTAssertNotNil(
                    traitCarryRegex.firstMatch(in: bodyStripped, range: bodyRange),
                    """
                    Cycle-22 L-5: a closure in AdDetectionService.swift \
                    constructs `PodcastProfile(...)` but does NOT carry \
                    `existing.traitProfileJSON` forward. This re-opens the \
                    cycle-15 M-2 / cycle-17 M-1 silent-clobber defect: \
                    `AnalysisStore.upsertProfile`'s SQL writes \
                    `traitProfileJSON = excluded.traitProfileJSON` (NOT \
                    COALESCE), so a default-`nil` value will silently nil \
                    the persisted trait profile.

                    Closure body (comment/string-stripped) was:
                    \(bodyStripped)

                    Restore the carry-forward (`traitProfileJSON: \
                    existing.traitProfileJSON`, or any `<ident>.\
                    traitProfileJSON` form via an alias). If this closure \
                    is intentionally writing `traitProfileJSON: nil` for \
                    a documented reason, extend this canary with an \
                    explicit allow-list and pin the rationale.
                    """
                )
            }

            searchStart = tokenRange.upperBound
        }

        // Sanity floor: today there are 4 `existing in` closures total
        // in AdDetectionService.swift; 2 of them construct
        // `PodcastProfile` (in `recordListenRewind` and `updatePriors`).
        // Cycle-23 L-8: line numbers were removed from the doc — they
        // rot the moment any unrelated edit shifts the file. Symbol
        // names (`recordListenRewind`, `updatePriors`) anchor the
        // intent durably. If a refactor drops the file to zero
        // `existing in` tokens, this canary would silently pass with no
        // work done — the floor below ensures the scan genuinely
        // exercises its target every run.
        XCTAssertGreaterThanOrEqual(
            totalExistingInCount, 2,
            """
            Cycle-22 L-5 floor: expected at least 2 `existing in` \
            closures in AdDetectionService.swift; found \
            \(totalExistingInCount). If the file genuinely no longer has \
            any `existing in` closures (e.g. all profile mutations moved \
            elsewhere), delete this canary; otherwise the whole-file \
            scan has gone blind.
            """
        )
        XCTAssertGreaterThanOrEqual(
            profileConstructingClosureCount, 2,
            """
            Cycle-22 L-5 floor: expected at least 2 `existing in` \
            closures that construct `PodcastProfile(...)` — one in \
            `recordListenRewind` and one in `updatePriors`. Found \
            \(profileConstructingClosureCount). The whole-file scan has \
            gone blind to its target; either restore the constructors \
            OR move this floor down with an explanation of where the \
            profile-mutation entry points have moved.
            """
        )
    }

    /// Cycle-22 L-5 positive control: confirm the whole-file canary
    /// above actually catches a profile-constructing closure that
    /// OMITS the carry-forward. Without this control, a future change
    /// that silently weakens the carry-forward regex would let the
    /// production-source canary pass with zero matches even though it
    /// has gone blind.
    func testProfileConstructingClosureWithoutCarryForwardWouldFire() throws {
        // A synthetic source that contains a single `existing in`
        // closure constructing `PodcastProfile(...)` BUT omits the
        // trait-profile carry-forward. Mirrors the exact regression
        // shape the canary above is designed to catch.
        let regressionFixture = """
        do {
            try await store.mutateProfile(podcastId: id) {
                fresh()
            } update: { existing in
                PodcastProfile(
                    podcastId: existing.podcastId,
                    name: existing.name
                )
            }
        }
        """
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(regressionFixture)

        let traitCarryRegex = try NSRegularExpression(
            pattern: Self.traitProfileCarryForwardPattern
        )

        // Cycle-23 L-6: same shared walk-back helper as the production
        // canary, so the parsing logic stays in lockstep.
        guard let tokenRange = stripped.range(of: "existing in") else {
            XCTFail("Fixture is missing `existing in` — pattern has drifted.")
            return
        }
        guard let bodyStripped = Self.closureBodyStripped(
            forExistingInTokenAt: tokenRange,
            sourceText: regressionFixture,
            strippedText: stripped
        ) else {
            XCTFail("Walk-back failed to find the closure brace in the fixture.")
            return
        }

        XCTAssertTrue(
            Self.bodyConstructsPodcastProfile(bodyStripped),
            "Fixture must contain a `PodcastProfile(` constructor for the canary to be in scope."
        )
        let bodyRange = NSRange(bodyStripped.startIndex..., in: bodyStripped)
        XCTAssertNil(
            traitCarryRegex.firstMatch(in: bodyStripped, range: bodyRange),
            """
            Cycle-22 L-5 positive-control failure: the carry-forward regex \
            `\(traitCarryRegex.pattern)` MATCHED a fixture that deliberately \
            omits the carry-forward. The production canary has gone blind \
            because the regex over-accepts; tighten it (and update this \
            fixture together).
            """
        )
    }

    /// cycle-2 L5 positive control: a fixture body that contains a
    /// `///` doc-comment mentioning `PodcastProfile(` MUST NOT cause
    /// `bodyConstructsPodcastProfile` to return `true`. Without the
    /// `strippingDocCommentLines` filter (and in a hypothetical world
    /// where the shared `strippingCommentsAndStrings` missed `///`),
    /// the regex would false-trigger on the doc reference and the
    /// whole-file canary would scope the trait-carry-forward check to
    /// a body that never actually constructs a `PodcastProfile`. We
    /// build the fixture by echoing the doc-comment text (raw, with
    /// `PodcastProfile(` literal) into a body that is OTHERWISE empty
    /// of profile constructors, then assert the result is `false`.
    func testBodyConstructsPodcastProfileSkipsDocCommentMentions() throws {
        // The "body" is the comment-and-string-stripped slice that
        // `closureBodyStripped` would normally hand us. Doc-comment
        // mentions of `PodcastProfile(` MUST NOT count.
        let bodyWithOnlyDocComment = """
        /// PodcastProfile( in this doc comment is not a real construction.
        let x = 1
        """
        XCTAssertFalse(
            Self.bodyConstructsPodcastProfile(bodyWithOnlyDocComment),
            """
            cycle-2 L5: a `///` doc-comment mentioning `PodcastProfile(` \
            slipped past `strippingDocCommentLines`. The whole-file canary \
            would scope the trait-carry-forward check to a body that does \
            not actually construct a profile, hiding regressions in \
            real construction sites.
            """
        )

        // Negative control: a real `PodcastProfile(` constructor on a
        // non-comment line MUST still match. Confirms the filter
        // doesn't strip too aggressively.
        let bodyWithRealConstructor = """
        /// PodcastProfile( in this doc comment is not real.
        return PodcastProfile(podcastId: "id")
        """
        XCTAssertTrue(
            Self.bodyConstructsPodcastProfile(bodyWithRealConstructor),
            """
            cycle-2 L5: a real `PodcastProfile(` constructor was \
            stripped along with the doc comment. The filter is too \
            aggressive; tighten `strippingDocCommentLines` to drop \
            ONLY lines whose trimmed prefix is `///`.
            """
        )
    }

    /// Cycle-19 L-1 sibling canary: pin that `AdDetectionService.swift`
    /// never aliases the `store` property to a different local
    /// (`let s = store`, `let analysisStore = self.store`, etc.). The
    /// whole-file `\bstore\s*\.\s*…` regex above anchors on the literal
    /// `store` token; an aliased capture would silently bypass it. This
    /// canary makes that assumption load-bearing — if a future caller
    /// has a legitimate reason to alias, it must justify the alias and
    /// extend the canary to walk through the alias name.
    ///
    /// Caveat: this regex matches the *declaration* of an alias, not
    /// every subsequent use. The whole-file canary above plus this
    /// declaration-side pin together cover the reachable aliasing
    /// surface for the file's current size and shape.
    func testAdDetectionServiceNeverAliasesStoreLocal() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/AdDetectionService.swift"
        )
        let stripped = SwiftSourceInspector.strippingCommentsAndStrings(source)
        let strippedRange = NSRange(stripped.startIndex..., in: stripped)

        let aliasRegex = try NSRegularExpression(pattern: Self.storeAliasDeclarationPattern)
        let matches = aliasRegex.matches(in: stripped, range: strippedRange)
        XCTAssertEqual(
            matches.count, 0,
            """
            `AdDetectionService.swift` introduced \(matches.count) \
            alias(es) of the `store` property to a local variable \
            (e.g. `let s = store` or `let analysisStore = self.store`). \
            Cycle-19 L-1: the cycle-17 M-2 / cycle-18 L-2 whole-file \
            canary anchors on the literal `store` token via `\\bstore`; \
            an aliased capture (`s.upsertProfile(...)`) would silently \
            bypass it. Either remove the alias OR — if there is a \
            documented reason to keep it — extend the whole-file canary \
            to walk through the alias name AND extend this canary's \
            allow-list correspondingly.
            """
        )
    }

    /// Cycle-19 L-1 positive/negative control for the alias canary.
    /// Mirrors the L-1 / L-3 fixture pattern used by sibling canaries:
    /// confirms the regex catches what it's meant to catch and rejects
    /// near-miss shapes.
    func testStoreAliasRegexCatchesItsOwnTarget() throws {
        let regex = try NSRegularExpression(pattern: Self.storeAliasDeclarationPattern)

        let positiveFixtures: [String] = [
            "let s = store",
            "let s=store",
            "let analysisStore = self.store",
            "let analysisStore=self.store",
            "let s = self . store",
            "var s = store",
        ]
        for fixture in positiveFixtures {
            let r = NSRange(fixture.startIndex..., in: fixture)
            XCTAssertGreaterThanOrEqual(
                regex.numberOfMatches(in: fixture, range: r), 1,
                "alias regex did not match positive fixture `\(fixture)` — pattern has gone blind."
            )
        }

        let negativeFixtures: [String] = [
            // Suffix collisions: `storeKey`, `storeURL` are not aliases.
            "let storeKey = \"k\"",
            "let storeURL = URL(string: \"u\")",
            // Prefix collisions: `profileStore`, `analysisStore` (without
            // `= store` / `= self.store`) are NOT aliases.
            "let profileStore = analysisStore",
            // Member access on a different object — not the `store` prop.
            "let snapshot = ad.store",
            // Calling .store as a function — not an alias.
            "let s = profile.store()",
            // Direct chained call through `store` — not an alias.
            "let r = await store.fetchAsset(id: \"a\")",
            "let r = self.store.fetchAsset(id: \"a\")",
            // Multi-line method chain — Swift allows the `.` on the next
            // line. Not an alias; the lookahead must look across the
            // newline to see the continuation `.`.
            "let r = store\n    .fetchAsset(id: \"a\")",
            // Subscript / optional-chaining variants — also not aliases.
            "let r = store[index]",
            "let r = store?.fetchAsset(id: \"a\")",
            "let r = store!.fetchAsset(id: \"a\")",
        ]
        for fixture in negativeFixtures {
            let r = NSRange(fixture.startIndex..., in: fixture)
            XCTAssertEqual(
                regex.numberOfMatches(in: fixture, range: r), 0,
                "alias regex over-matched on `\(fixture)` — pattern is too loose."
            )
        }
    }

    /// Cycle-20 L-1: positive/negative fixtures for the
    /// `closureBodyContainingExistingIn` helper. Without this control,
    /// a future change to the helper that returned the WHOLE function
    /// body (e.g. by walking back to the function-level `{` instead of
    /// the closure-level `{`) would let the caller's `XCTAssertNotNil`
    /// regex still match — the L-1 scoping would silently regress.
    func testClosureBodyHelperIsolatesUpdateClosure() throws {
        // Positive: a fixture shaped like the production update closure
        // returns ONLY the closure body, not surrounding sibling code.
        let positive = """
        do {
            let outerSibling = something
            traitProfileJSON: outerProfile.traitProfileJSON
            try store.mutateProfile(podcastId: id) {
                fresh()
            } update: { existing in
                Profile(
                    name: existing.name,
                    traitProfileJSON: existing.traitProfileJSON
                )
            }
            anotherOuterSibling()
        }
        """
        let body = try Self.closureBodyContainingExistingIn(in: positive)
        XCTAssertTrue(
            body.contains("existing in"),
            "Helper must return the closure body — `existing in` parameter binding should be present in the result. Got: \(body)"
        )
        XCTAssertTrue(
            body.contains("traitProfileJSON: existing.traitProfileJSON"),
            "Helper must include the load-bearing carry-forward inside the closure. Got: \(body)"
        )
        XCTAssertFalse(
            body.contains("outerSibling"),
            "Helper must NOT include sibling code outside the closure (the L-1 risk). Got: \(body)"
        )
        XCTAssertFalse(
            body.contains("anotherOuterSibling"),
            "Helper must NOT include sibling code AFTER the closure. Got: \(body)"
        )
        XCTAssertFalse(
            body.contains("outerProfile"),
            "Helper must NOT include any pre-closure sibling line, even one with a colliding `traitProfileJSON:` shape (the L-1 false-pass risk). Got: \(body)"
        )

        // Single-trailing-closure shape (mirrors recordListenRewind).
        let singleTrailing = """
        let merged = try await store.updateProfileIfExists(podcastId: id) { existing in
            Profile(
                traitProfileJSON: existing.traitProfileJSON
            )
        }
        let unrelated = "traitProfileJSON: x.traitProfileJSON"
        """
        let trailingBody = try Self.closureBodyContainingExistingIn(in: singleTrailing)
        XCTAssertTrue(
            trailingBody.contains("existing in"),
            "Helper must return the closure body for a single trailing closure too. Got: \(trailingBody)"
        )
        XCTAssertFalse(
            trailingBody.contains("unrelated"),
            "Helper must not bleed past the closure's matching `}`. Got: \(trailingBody)"
        )

        // Cycle-21 M-1: when the input contains MORE than one `existing
        // in` token (e.g. a future refactor that adds a sibling closure
        // — `mergeSlotPositions` already uses `existing in` further down
        // `AdDetectionService.swift`, and a future caller might pass a
        // wider source range), the helper MUST throw rather than
        // silently anchor on the first match. Without this check, the
        // canary would extract the wrong closure body and the
        // carry-forward regex would still succeed against the unrelated
        // body — the load-bearing assertion would go blind.
        let twoSiblings = """
        let one = .map { existing in existing }
        let two = store.updateProfileIfExists(podcastId: id) { existing in
            Profile(traitProfileJSON: existing.traitProfileJSON)
        }
        """
        XCTAssertThrowsError(
            try Self.closureBodyContainingExistingIn(in: twoSiblings),
            """
            Cycle-21 M-1: helper must throw when the input contains \
            more than one `existing in` token, so the caller cannot \
            silently anchor on the first match and let later sibling \
            regressions hide. If this assertion starts failing, the \
            helper has been weakened; either restore the multi-match \
            guard OR update the production callers (testUpdatePriors* / \
            testRecordListenRewind*) to disambiguate which closure they \
            want.
            """
        ) { error in
            // Sanity-check the error message mentions the count so the
            // failure mode is debuggable from CI logs alone (an opaque
            // throw would leave a future engineer re-running locally to
            // figure out which assert tripped).
            let message = (error as NSError).localizedDescription
            XCTAssertTrue(
                message.contains("found 2"),
                "Multi-match throw should mention the count it found. Got: \(message)"
            )
        }
    }

    /// Cycle-17 M-2 / cycle-18 L-2: the canonical regex used by
    /// `testAdDetectionServiceNeverDirectlyCallsFetchOrUpsertProfile`.
    /// Defined as a class constant so the cycle-18 L-1 positive
    /// control can run the same regex against fixture strings — that
    /// keeps the negative whole-file assertion and its positive proof
    /// of reach in lockstep.
    ///
    /// Cycle-18 L-2 broadened past the cycle-17 form by dropping the
    /// `await\s+` prefix. See the canary's doc-comment above for the
    /// full rationale (synchronous overloads, multi-line breaks).
    private static let fetchOrUpsertProfilePairPattern: String =
        #"\bstore\s*\.\s*(?:fetchProfile|upsertProfile)\s*\("#

    /// Cycle-19 L-1: matches a `let`/`var` binding whose RHS is the
    /// `store` property (either bare `store` or `self.store`). Anchored
    /// with `\b` so `let storeKey = …` is NOT matched, and word-boundary
    /// after `store` so `= storeKey` doesn't trigger either. The
    /// `(?:self\s*\.\s*)?` segment tolerates either bare or
    /// dotted-property RHS, with whitespace-tolerant `.`.
    ///
    /// The trailing lookahead `(?!\s*[\.\(\?!\[])` excludes shapes
    /// where `store` is followed by a method/property/optional/subscript
    /// continuation — those are NOT aliases (`let r = store.method()` is
    /// a direct call through `store`, already covered by the whole-file
    /// canary). The whitespace inside the lookahead lets a same-line OR
    /// next-line continuation be excluded uniformly (NSRegularExpression's
    /// `\s` includes newlines).
    ///
    /// The dot-form requires `self.store` — by convention this codebase
    /// does not reference `store` through any other receiver, so a
    /// future shape like `someActor.store` would not be flagged. That's
    /// an acceptable limitation: `someActor.store` would not bypass the
    /// whole-file canary (which anchors on the LITERAL `store` token
    /// before `.`), only intra-file aliases through bare `store` /
    /// `self.store` do.
    private static let storeAliasDeclarationPattern: String =
        #"\b(?:let|var)\s+\w+\s*=\s*(?:self\s*\.\s*)?\bstore\b(?!\s*[\.\(\?!\[])"#

    /// Cycle-23 M-3 / cycle-2 L1: shared regex for "this closure body
    /// carries the trait-profile JSON forward into a `PodcastProfile(...)`
    /// constructor". Matches a bare `<ident>.traitProfileJSON` anywhere
    /// in the closure body — the canonical forms are:
    ///
    ///   • Direct carry-forward in the constructor:
    ///     `traitProfileJSON: existing.traitProfileJSON`
    ///   • Nil-coalescing fallback (cycle-2 L1):
    ///     `let resolvedTraitProfileJSON = mergedTraitProfileJSON ?? existing.traitProfileJSON`
    ///     followed by `traitProfileJSON: resolvedTraitProfileJSON`. The
    ///     `?? existing.traitProfileJSON` half is what we lock in here —
    ///     the carry-forward is structurally guaranteed at the
    ///     fallback site even if the constructor argument names a local.
    ///
    /// In both shapes some `<ident>.traitProfileJSON` token appears
    /// inside the closure, which is what this regex matches. The
    /// negative-control fixture
    /// (`testProfileConstructingClosureWithoutCarryForwardWouldFire`)
    /// constructs `PodcastProfile(...)` without any `<ident>.traitProfileJSON`
    /// mention, so the regex correctly returns no match there.
    ///
    /// Used by the scoped canaries
    /// (`testUpdatePriorsCarriesExistingTraitProfileJSONForward`,
    /// `testRecordListenRewindBodyUsesUpdateProfileIfExistsAndCarriesTraitJSON`),
    /// the whole-file canary (`testEveryProfileConstructingExistingInClosureCarriesTraitJSON`),
    /// and the positive control (`testProfileConstructingClosureWithoutCarryForwardWouldFire`).
    /// Lifting it here keeps all four call sites in lockstep — a future
    /// tightening or broadening of the pattern is a one-line edit.
    fileprivate static let traitProfileCarryForwardPattern: String =
        #"\b[A-Za-z_][A-Za-z0-9_]*\s*\.\s*traitProfileJSON\b"#

    /// Cycle-23 L-4: word-boundary anchored "is this a `PodcastProfile`
    /// constructor invocation?" probe. Replaces a bare
    /// `body.contains("PodcastProfile(")` substring check, which would
    /// also match a hypothetical sibling type like `MyPodcastProfile(`
    /// or `RawPodcastProfile(` and quietly drag those into the trait-
    /// carry-forward canary's scope. The leading `\b` enforces a real
    /// word boundary, and `\s*` tolerates spacing before the `(`.
    fileprivate static let podcastProfileConstructorPattern: String =
        #"\bPodcastProfile\s*\("#

    fileprivate static func bodyConstructsPodcastProfile(_ bodyStripped: String) -> Bool {
        // Cycle-24 L-A / Cycle-25 M-1: surface compile failures loudly,
        // attributed to the calling test rather than crashing the
        // entire `xctest` process. The previous
        // `try? NSRegularExpression(...)` silently degraded the canary
        // to `false` if the pattern ever became malformed; the cycle-24
        // first-pass `preconditionFailure` was loud but blunt — it
        // SIGABRTs the process and reports unrelated tests as "did not
        // run". `XCTFail` + `return false` is loud AND attributes the
        // failure to the specific test case, with no impact on other
        // tests in the same suite invocation.
        let regex: NSRegularExpression
        do {
            regex = try NSRegularExpression(pattern: Self.podcastProfileConstructorPattern)
        } catch {
            XCTFail(
                """
                Cycle-24 L-A / Cycle-25 M-1: \
                `podcastProfileConstructorPattern` failed to compile \
                (\(error)). The whole-file canary cannot run with a \
                broken regex; fix the pattern at the constant \
                declaration site. Returning `false` keeps the suite \
                running so other tests still report their own outcomes.
                """
            )
            return false
        }
        // cycle-2 L5: defensive belt-and-suspenders strip of `///` doc-
        // comment lines on top of `strippingCommentsAndStrings`. The
        // shared stripper already blanks `//` line comments (which
        // covers `///`), but a future stripper change that misses the
        // triple-slash case would silently let doc-comment mentions of
        // `PodcastProfile(` slip into the regex match. Filtering here
        // makes the protection explicit per finding L5.
        let withoutDocComments = Self.strippingDocCommentLines(bodyStripped)
        let range = NSRange(withoutDocComments.startIndex..., in: withoutDocComments)
        return regex.firstMatch(in: withoutDocComments, range: range) != nil
    }

    /// cycle-2 L5: drop every line whose first non-whitespace characters
    /// are `///` (Swift doc-comment marker). The shared
    /// `strippingCommentsAndStrings` already blanks `//` line comments
    /// so doc-comments arrive here as runs of spaces, but this filter
    /// is the explicit defense the cycle-2 L5 finding asked for: even
    /// if the shared stripper later misses a doc-comment edge case,
    /// the `\bPodcastProfile\s*\(` scan won't false-trip on
    /// documentation references. Length is NOT preserved (lines
    /// dropped), so callers must not feed the result back into a
    /// walk-back that depends on `String.distance` alignment to the
    /// original source — `bodyConstructsPodcastProfile` only does a
    /// presence check, which is offset-independent.
    fileprivate static func strippingDocCommentLines(_ source: String) -> String {
        source
            .split(separator: "\n", omittingEmptySubsequences: false)
            .filter { !$0.trimmingCharacters(in: .whitespaces).hasPrefix("///") }
            .joined(separator: "\n")
    }

    /// Cycle-23 L-5 + L-6: shared walk-back from an `existing in` token
    /// to its containing closure body. Used by both the production
    /// whole-file canary (`testEveryProfileConstructingExistingInClosureCarriesTraitJSON`)
    /// and the positive-control fixture
    /// (`testProfileConstructingClosureWithoutCarryForwardWouldFire`),
    /// keeping their parsing logic identical.
    ///
    /// Caveat (L-5): the walk-back finds the immediately preceding `{`
    /// in `strippedText`. In normal Swift this IS the opener of the
    /// closure containing `existing in` — there is no valid shape that
    /// interposes a non-closure `{` (Swift's grammar requires `{` to
    /// open a closure or block, and a *child* closure such as
    /// `{ outer.method { ... } existing in ... }` is not a legal closure
    /// signature). Comment/string-stripping eliminates `{` inside
    /// literals further. If this assumption ever fails, the canary will
    /// extract the wrong body — preferring a false `XCTFail` over a
    /// silent miss, callers should treat a wrong-body match the same
    /// as a missing carry-forward.
    ///
    /// Cycle-23 L-7: asserts `strippedText.count == sourceText.count`
    /// at runtime, making the Character-grapheme alignment that
    /// `SwiftSourceInspector.strippingCommentsAndStrings` documents
    /// load-bearing instead of doc-only. If the source contains a
    /// grapheme that the stripper would normalize differently in a
    /// future change, this precondition fires loudly.
    fileprivate static func closureBodyStripped(
        forExistingInTokenAt tokenRange: Range<String.Index>,
        sourceText: String,
        strippedText: String
    ) -> String? {
        precondition(
            strippedText.count == sourceText.count,
            """
            Cycle-23 L-7: `SwiftSourceInspector.strippingCommentsAndStrings` \
            must preserve Character-by-Character length so `stripped` \
            indices align with `source`. Got stripped.count=\(strippedText.count) \
            source.count=\(sourceText.count). The walk-back below would \
            land on the wrong source index.
            """
        )
        var i = strippedText.index(before: tokenRange.lowerBound)
        while i > strippedText.startIndex && strippedText[i] != "{" {
            i = strippedText.index(before: i)
        }
        guard strippedText[i] == "{" else { return nil }
        let offset = strippedText.distance(from: strippedText.startIndex, to: i)
        let sourceIndex = sourceText.index(sourceText.startIndex, offsetBy: offset)
        let body = SwiftSourceInspector.bracedBody(in: sourceText, startingAt: sourceIndex)
        return SwiftSourceInspector.strippingCommentsAndStrings(body)
    }
}
