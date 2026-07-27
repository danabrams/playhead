// SkipOrchestratorRevertTests.swift
// Tests for revertByTimeRange and revertWindow methods added in playhead-gpi.
// Verifies that user corrections ("Not an ad" banner, "This isn't an ad" popover)
// properly revert in-memory state, remove skip cues, and broadcast segment updates.

import CoreMedia
import Foundation
import Testing
import XCTest

@testable import Playhead

// MARK: - Cycle 3 H4: TranscriptPeekView duplicate-signal regression rail
//
// XCTest source canary. The cycle-2 M2 fix removed an unconditional
// `recordFalseSkipSignal` call from `TranscriptPeekView.submitNotAdChunks`
// because `SkipOrchestrator.revertByTimeRange` now records the (weak or
// full) trust signal itself — see SkipOrchestrator.swift M2 routing.
// Without a regression rail, a future diff could re-add the duplicate call
// silently, restoring the 2x penalty on managed reverts and the 0.05+0.10
// asymmetry on suggest-only reverts. The orchestrator-level tests in this
// file pin the routing CONTRACT; this canary pins the SOURCE-LEVEL
// invariant on the view's veto handler.
//
// XCTest (not Swift Testing) so the canary is filterable from the test
// plan — see project memory `xctestplan_swift_testing_limitation`.
final class TranscriptPeekViewVetoSourceCanaryTests: XCTestCase {

    func testSubmitNotAdChunksDoesNotRecordTrustSignalDirectly() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Views/NowPlaying/TranscriptPeekView.swift"
        )
        guard let funcRange = source.range(of: "func submitNotAdChunks(") else {
            XCTFail("Could not locate `func submitNotAdChunks(` in TranscriptPeekView.swift")
            return
        }
        guard let openBrace = source[funcRange.upperBound...].firstIndex(of: "{") else {
            XCTFail("Could not locate `{` after `func submitNotAdChunks(`")
            return
        }
        let body = SwiftSourceInspector.bracedBody(in: source, startingAt: openBrace)
        let stripped = SwiftSourceInspector.strippingComments(body)
        XCTAssertFalse(
            stripped.contains("recordFalseSkipSignal"),
            """
            TranscriptPeekView.submitNotAdChunks must NOT call \
            recordFalseSkipSignal directly. SkipOrchestrator.revertByTimeRange \
            is the single source of trust signaling for revert gestures so \
            the cycle-1 M2 weak/strong routing applies correctly. Re-adding \
            this call would double-count the trust penalty (managed: 2x; \
            suggest-only: 0.05+0.10 asymmetry).
            """
        )
        XCTAssertFalse(
            stripped.contains("recordWeakFalseSkipSignal"),
            "submitNotAdChunks must not call recordWeakFalseSkipSignal directly either; see canary above."
        )
    }

    /// Episode identity alone is insufficient for deferred banner actions:
    /// replaying the same canonical episode advances the orchestrator
    /// lifecycle while preserving the ID. Pin generation capture + comparison
    /// in every episode-bound action that suspends on persistence/trust work.
    func testEpisodeBoundActionsGuardSameEpisodeLifecycleGeneration() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/SkipOrchestrator/SkipOrchestrator.swift"
        )
        let signatures = [
            "func denyAutoSkippedBanner(\n        windowId: String,",
            "func revertWindow(\n        windowId: String,\n        podcastId: String? = nil,\n        ifCurrentEpisodeId",
            "func acceptSuggestedSkip(\n        windowId: String,\n        ifCurrentEpisodeId",
            "func declineSuggestedSkip(\n        windowId: String,\n        isExplicitDenial: Bool = false,\n        ifCurrentEpisodeId",
        ]

        for signature in signatures {
            guard let body = SwiftSourceInspector.firstBody(
                in: source,
                after: signature
            ) else {
                XCTFail("Could not locate episode-bound action body for \(signature).")
                continue
            }
            XCTAssertTrue(
                body.contains("let sourceLifecycleGeneration = episodeLifecycleGeneration"),
                "\(signature) must capture the lifecycle generation before its first suspension"
            )
            XCTAssertTrue(
                body.contains("episodeLifecycleGeneration == sourceLifecycleGeneration"),
                "\(signature) must reject live-state work after a same-episode restart"
            )
        }
    }

    /// playhead-i08e: the lifecycle guards in the revert seams protect LIVE
    /// state only. A revert's durable receipt, hard-negative ingest, and
    /// threshold-control sample belong to the show captured at gesture time and
    /// are all fire-and-forget over pre-suspension values, so they must be
    /// issued BEFORE the live-state guard that gates `evaluateAndPush()`.
    /// Ordering them after it — which is how `recordListenRevert` and
    /// `revertByTimeRange` read between 5c1a167e and playhead-i08e — silently
    /// discards a valid old-episode correction AFTER that same gesture's trust
    /// penalty has already landed, leaving trust and corrections permanently
    /// out of step.
    ///
    /// Precision note: this pins each effect against its own function's
    /// live-state guard. It does NOT claim every seam issues its effects before
    /// its trust hop — `revertByTimeRange` and `revertWindow` do,
    /// `recordListenRevert` does not, and that is fine because no guard sits
    /// between `recordListenRevert`'s trust hop and its effects. Ordering
    /// against the guard is the invariant that actually prevents the drop.
    ///
    /// Source-level because the race needs an episode switch to interleave
    /// with an AnalysisStore/TrustScoringService suspension, and neither seam
    /// exposes a persistence barrier to make that deterministic.
    func testRevertCalibrationEffectsPrecedeTheirLiveStateGuard() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/SkipOrchestrator/SkipOrchestrator.swift"
        )

        // Strings are blanked as well as comments: every rail below greps for a
        // CODE token (`return`, a lifecycle identifier), and a future log line
        // that merely mentions one inside a literal must not redden the canary.
        // `strippingCommentsAndStrings` is length-preserving, so `braceDepth`'s
        // offset arithmetic is unaffected.
        guard let listenRevert = SwiftSourceInspector.firstBody(
            in: source,
            after: "func recordListenRevert("
        ).map(SwiftSourceInspector.strippingCommentsAndStrings) else {
            XCTFail("Could not locate the recordListenRevert body")
            return
        }
        guard let listenCapture = listenRevert.range(
                of: "let sourceLifecycleGeneration = episodeLifecycleGeneration"
              ),
              let listenReceipt = listenRevert.range(of: "persistManualCorrectionVeto("),
              let listenNegative = listenRevert.range(of: "ingestNegativeFingerprint("),
              let listenController = listenRevert.range(of: "recordThresholdControlSignal("),
              let listenGuard = listenRevert.range(
                of: "guard episodeLifecycleGeneration == sourceLifecycleGeneration"
              )
        else {
            XCTFail(
                "recordListenRevert must keep its receipt, hard-negative ingest, controller write, and lifecycle guard"
            )
            return
        }
        for (label, effect) in [
            ("the manualVeto receipt", listenReceipt),
            ("the hard-negative ingest", listenNegative),
            ("the threshold-control sample", listenController),
        ] {
            XCTAssertLessThan(
                effect.lowerBound,
                listenGuard.lowerBound,
                "recordListenRevert must issue \(label) before its live-lifecycle guard"
            )
            // Structural, not positional: each effect must also sit at the TOP
            // LEVEL of the function body. Any conditional wrapper — wherever it
            // is opened — nests it one brace deeper, so this rejects the whole
            // family of "gate the effects on the lifecycle" evasions at once,
            // including the ones a position-anchored scan cannot see.
            XCTAssertEqual(
                braceDepth(in: listenRevert, at: effect.lowerBound),
                0,
                """
                recordListenRevert must issue \(label) unconditionally, at the \
                top level of the function body. It is nested inside a block, \
                which means some condition can now suppress an effect that is \
                owed to the CAPTURED show whatever the live lifecycle is.
                """
            )
        }
        // The guard is only load-bearing if it still gates the live-state work.
        // Without this, moving it BELOW `evaluateAndPush()` would satisfy every
        // assertion above while restoring cross-episode cue republication.
        guard let listenPush = listenRevert.range(of: "evaluateAndPush(") else {
            XCTFail("recordListenRevert must still republish cues")
            return
        }
        XCTAssertLessThan(
            listenGuard.lowerBound,
            listenPush.lowerBound,
            "recordListenRevert's lifecycle guard must gate evaluateAndPush()"
        )
        // Belt-and-suspenders on the same contract: the span from the capture
        // to the guard must not name the captured generation at all. Anchored
        // at the CAPTURE rather than at the first effect — an `if` opened one
        // line above `persistManualCorrectionVeto(` lies outside a
        // first-effect-anchored region, which is how this assertion was
        // defeated. `recordListenRevert` has exactly two legitimate mentions of
        // the captured generation, this capture and the final guard, and the
        // bounds exclude both.
        assertNoLifecycleRecheck(
            in: listenRevert,
            from: listenCapture.upperBound,
            to: listenGuard.lowerBound,
            seam: "recordListenRevert"
        )
        guard let listenSuspension = listenRevert.range(
            of: "try await store.updateAdWindowDecision("
        ) else {
            XCTFail("recordListenRevert must still persist its decision-state change")
            return
        }
        assertNoEarlyExit(
            in: listenRevert,
            from: listenSuspension.upperBound,
            to: listenGuard.lowerBound,
            seam: "recordListenRevert"
        )

        guard let timeRangeRevert = SwiftSourceInspector.firstBody(
            in: source,
            after: "func revertByTimeRange("
        ).map(SwiftSourceInspector.strippingCommentsAndStrings) else {
            XCTFail("Could not locate the revertByTimeRange body")
            return
        }
        guard let rangeReceipt = timeRangeRevert.range(of: "persistManualCorrectionVeto("),
              let rangeController = timeRangeRevert.range(of: "recordThresholdControlSignal("),
              let rangeTrust = timeRangeRevert.range(of: "await trustService.recordFalseSkipSignal(")
        else {
            XCTFail(
                "revertByTimeRange must keep its receipt, controller write, and full-magnitude trust signal"
            )
            return
        }
        XCTAssertLessThan(
            rangeReceipt.lowerBound,
            rangeTrust.lowerBound,
            "revertByTimeRange must write its receipt before the trust hop suspends"
        )
        XCTAssertLessThan(
            rangeController.lowerBound,
            rangeTrust.lowerBound,
            "revertByTimeRange must record its threshold-control sample before the trust hop suspends"
        )

        // `revertByTimeRange` guards its lifecycle in three places: once inside
        // each revert loop (those protect LIVE dictionary mutation and are
        // deliberately EARLIER than the receipt) and once as the final gate in
        // front of `evaluateAndPush()`. Only the last one may follow the
        // durable/calibration effects, so anchor on it explicitly.
        guard let rangeFinalGuard = timeRangeRevert.range(
            of: "guard episodeLifecycleGeneration == sourceLifecycleGeneration",
            options: .backwards
        ) else {
            XCTFail("revertByTimeRange must keep a final live-state lifecycle guard")
            return
        }
        for (label, effect) in [
            ("the manualVeto receipt", rangeReceipt),
            ("the threshold-control sample", rangeController),
        ] {
            XCTAssertLessThan(
                effect.lowerBound,
                rangeFinalGuard.lowerBound,
                "revertByTimeRange must issue \(label) before its final live-state guard"
            )
        }
        guard let rangePush = timeRangeRevert.range(of: "evaluateAndPush(") else {
            XCTFail("revertByTimeRange must still republish cues")
            return
        }
        XCTAssertLessThan(
            rangeFinalGuard.lowerBound,
            rangePush.lowerBound,
            "revertByTimeRange's final guard must gate evaluateAndPush()"
        )
        // The structural landmarks of the function, in source order: the
        // managed loop's persistence hop, its in-loop lifecycle guard, the
        // suggest-tier work list, the loop that consumes it, and the effects
        // block. Everything below is expressed against these, so a refactor
        // that moves one reports as a single anchor-drift failure rather than a
        // scatter of unrelated ones.
        guard let rangeManagedPersist = timeRangeRevert.range(
                of: "try await store.updateAdWindowDecision("
              ),
              let rangeManagedGuard = timeRangeRevert.range(
                of: "guard episodeLifecycleGeneration == sourceLifecycleGeneration"
              ),
              let rangeSuggestBuild = timeRangeRevert.range(of: "suggestWindows.compactMap"),
              let rangeSuggestLoop = timeRangeRevert.range(
                of: "for (id, suggested) in suggestRevertTargets"
              ),
              let rangeEffectsBlock = timeRangeRevert.range(of: "if revertedAny"),
              rangeManagedPersist.upperBound < rangeManagedGuard.lowerBound,
              rangeManagedGuard.upperBound < rangeSuggestBuild.lowerBound,
              rangeSuggestBuild.upperBound < rangeSuggestLoop.lowerBound,
              rangeSuggestLoop.upperBound < rangeEffectsBlock.lowerBound
        else {
            XCTFail(
                """
                revertByTimeRange no longer has the shape this canary scans — \
                a managed-loop persistence hop, an in-loop lifecycle guard, a \
                suggest-tier work list, its consuming loop, and an \
                `if revertedAny` effects block, in that order. The canary's \
                anchors have drifted; re-derive them rather than deleting rails.
                """
            )
            return
        }
        // `revertByTimeRange`'s effects are legitimately nested (inside
        // `if revertedAny`, then `if let assetId` / `if revertedManagedAny`),
        // so the depth check anchors on the block instead of the statements:
        // `if revertedAny` itself must be top-level, which rejects any wrapper
        // opened ABOVE it, and the region scans below reject one opened INSIDE
        // it. Together those cover both sides of the anchor.
        XCTAssertEqual(
            braceDepth(in: timeRangeRevert, at: rangeEffectsBlock.lowerBound),
            0,
            """
            revertByTimeRange's `if revertedAny` effects block must sit at the \
            top level of the function body. Nesting it means something can \
            suppress the whole gesture's receipt and calibration — which is \
            the drop this canary exists to prevent.
            """
        )
        // Everything from that block's opening to the final guard — receipt,
        // controller sample, trust hop — is owed to the captured show
        // unconditionally, so no mention of the captured generation belongs in
        // it. (The two in-loop guards and the suggest-target ternary, which
        // legitimately mention it, are all upstream of this block.)
        assertNoLifecycleRecheck(
            in: timeRangeRevert,
            from: rangeEffectsBlock.lowerBound,
            to: rangeFinalGuard.lowerBound,
            seam: "revertByTimeRange"
        )
        // `revertByTimeRange` is scanned for early exits in THREE pieces rather
        // than one. A single region has to start below the
        // `suggestWindows.compactMap` closure — a `return` there exits the
        // closure, not the function, and a textual scan cannot tell them apart
        // — and that concession left the entire pre-effects span unscanned.
        //
        // playhead-i08e (fifth pass): that span was not, in fact, covered by
        // the `break`-not-`return` regex below, which only recognises the
        // literal `guard episodeLifecycleGeneration == …` form. A top-level
        //
        //     guard activePodcastId == podcastId else { return }
        //
        // placed just above `if revertedAny` — the "harmonise with
        // `revertWindow`/`denyAutoSkippedBanner`" refactor, spelled against a
        // different field — dropped the receipt, the controller sample AND the
        // trust penalty with every rail in this test green and every
        // behavioural test green too (they all pass a `podcastId` equal to the
        // active one). Verified by mutation before and after this split.
        //
        // The two added regions are closure-free by construction, so they need
        // no depth filtering: the only closure in the function lies strictly
        // between them.
        assertNoEarlyExit(
            in: timeRangeRevert,
            from: rangeManagedPersist.upperBound,
            to: rangeSuggestBuild.lowerBound,
            seam: "revertByTimeRange (managed loop → suggest work list)"
        )
        assertNoEarlyExit(
            in: timeRangeRevert,
            from: rangeSuggestLoop.lowerBound,
            to: rangeEffectsBlock.lowerBound,
            seam: "revertByTimeRange (suggest loop → effects block)"
        )
        assertNoEarlyExit(
            in: timeRangeRevert,
            from: rangeEffectsBlock.lowerBound,
            to: rangeFinalGuard.lowerBound,
            seam: "revertByTimeRange (effects block → final guard)"
        )

        // playhead-i08e: `revertByTimeRange` has exactly three lifecycle
        // guards, and BOTH halves of that census are load-bearing.
        //
        //   • The two in-loop guards must `break`, never `return`. A `return`
        //     abandons a gesture whose windows are already durably reverted,
        //     leaving no receipt and no calibration for a correction the user
        //     really made.
        //   • Both must also still EXIST. Counting only the `return` form (as
        //     this rail did through the fourth pass) let the SUGGEST loop's
        //     guard be deleted outright with the count unchanged at 1 — after
        //     which a lifecycle lost mid-loop keeps writing the OLD snapshot's
        //     ids into `suggestBanneredWindowIds` / `vetoedSuggestWindowIds`,
        //     which `beginEpisode` has just cleared for the replacement
        //     episode, and emits retirements stamped with the NEW episode's
        //     identity for OLD window ids. Mutation-verified as a live hole.
        //
        // `[^{}]*` rather than `\s*` before the keyword: requiring the keyword
        // to be the first thing inside the brace made a plain diagnostic —
        // `logger.debug(…)` above the `return` in the final guard — read as a
        // count of 0 and fail with a message naming the opposite defect ("the
        // in-loop guards must `break`"), sending the next author to the wrong
        // place entirely. Also mutation-verified.
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"guard\s+episodeLifecycleGeneration\s*==\s*sourceLifecycleGeneration\s+else\s*\{[^{}]*\breturn\b"#,
                in: timeRangeRevert
            ),
            1,
            """
            Exactly one lifecycle guard in revertByTimeRange may `return` (the \
            final live-state gate). The in-loop guards must `break` so the \
            gesture's receipt and calibration signals still reach the captured \
            source show.
            """
        )
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(
                of: #"guard\s+episodeLifecycleGeneration\s*==\s*sourceLifecycleGeneration\s+else\s*\{[^{}]*\bbreak\b"#,
                in: timeRangeRevert
            ),
            2,
            """
            revertByTimeRange must keep BOTH in-loop lifecycle guards — one in \
            the managed loop, one in the suggest-tier loop — and both must \
            `break`. Losing the suggest-tier one lets a gesture keep mutating \
            live suggest state (`suggestWindows`, `suggestBanneredWindowIds`, \
            `vetoedSuggestWindowIds`, banner retirements) after a replacement \
            episode has taken ownership of it.
            """
        )

        // playhead-i08e: the `break` above is what MAKES this next rail
        // load-bearing, so the two must move together. While the in-loop guard
        // `return`ed, losing the lifecycle inside the managed loop abandoned
        // the function outright and the suggest-tier pass below was simply
        // unreachable. Now the function keeps going — so that pass, which reads
        // and mutates the LIVE `suggestWindows` dictionary, has to carry its own
        // lifecycle gate. Without one it builds its work list from the
        // REPLACEMENT episode's suggestions and vetoes them: not a dropped
        // signal this time but cross-episode corruption, a strictly worse bug
        // than the one this canary was written for.
        //
        // Source-level because it is the one invariant in this seam that no
        // behavioural test can reach: reproducing it needs an episode switch to
        // interleave with an AnalysisStore suspension, and `revertByTimeRange`
        // exposes no persistence barrier to make that deterministic. Verified by
        // mutation — deleting the gate leaves every behavioural test in
        // SkipOrchestratorThresholdControlTests and SkipOrchestratorRevertTests
        // green.
        //
        // The region runs from the end of the managed loop's in-loop guard to
        // the loop that consumes the work list, so it accepts every reasonable
        // spelling of the gate — today's ternary, an `if` / `guard` wrapper
        // around the `compactMap`, or a check inside the closure — and only
        // rejects its absence. All three spellings verified.
        XCTAssertTrue(
            timeRangeRevert[rangeManagedGuard.upperBound..<rangeSuggestLoop.lowerBound]
                .contains("episodeLifecycleGeneration == sourceLifecycleGeneration"),
            """
            revertByTimeRange builds its suggest-tier work list without \
            re-checking the captured lifecycle. `suggestWindows` is LIVE state: \
            once the managed loop has `break`ed because a replacement episode \
            took ownership, reading it here collects the NEW episode's \
            suggestions and the loop below vetoes them. Gate the work list \
            (ternary, `if`, or `guard`) between the managed loop and this pass.
            """
        )
    }

    /// Brace nesting depth of `index` within a function body. Depth 0 means
    /// "a statement of the function itself" — not nested in any block.
    ///
    /// This is the structural half of the contract, and it is what makes the
    /// canary robust rather than merely well-anchored: a textual region has a
    /// start position, and every start position can be evaded by opening the
    /// offending conditional one line earlier. Depth has no such loophole —
    /// wrapping a statement in anything at all increases it, including a
    /// wrapper that never names the captured generation and so is invisible to
    /// `assertNoLifecycleRecheck` (verified by mutation with
    /// `if !windows.isEmpty { … }`).
    ///
    /// Depth 0 is deliberately STRICTER than "reached unconditionally". A
    /// behaviour-preserving wrapper such as
    /// `if let podcastId { recordThresholdControlSignal(…) }` — redundant,
    /// since that call already guards its own nil — is rejected too. That is
    /// the intended trade: "is THIS particular condition safe to gate the
    /// effect on?" is exactly the judgement call that produced this bug, so
    /// the rail refuses to adjudicate it and requires the effects to be
    /// unconditional statements.
    ///
    /// String-literal contents are blanked before counting, so a brace inside
    /// a literal cannot shift the result. `strippingCommentsAndStrings`
    /// preserves length exactly (pinned by
    /// `SwiftSourceInspectorStringStrippingTests`), so offsets computed on
    /// `body` address the same characters in the blanked copy.
    private func braceDepth(in body: String, at index: String.Index) -> Int {
        let scan = SwiftSourceInspector.strippingCommentsAndStrings(body)
        let offset = body.distance(from: body.startIndex, to: index)
        guard offset >= 0, offset <= scan.count else { return .min }
        let end = scan.index(scan.startIndex, offsetBy: offset)
        var depth = 0
        for character in scan[scan.startIndex..<end] {
            if character == "{" {
                depth += 1
            } else if character == "}" {
                depth -= 1
            }
        }
        return depth
    }

    /// Once a seam has SUSPENDED, nothing may abort it before its live-state
    /// guard: by then its trust penalty may already have landed, so an early
    /// exit drops the matching receipt and calibration.
    ///
    /// This closes the family `braceDepth` cannot see. Depth rejects *nesting*,
    /// but a top-level early exit is itself depth 0 and need not name the
    /// captured generation — e.g.
    ///
    ///     guard activeEpisodeId == sourceEpisodeId else { return }
    ///
    /// which is not hypothetical: `revertWindow` and `denyAutoSkippedBanner`
    /// both open with exactly that pair, so "harmonise `recordListenRevert`
    /// with its siblings" writes it verbatim. Mutation-verified to restore the
    /// silent drop with every other assertion here green.
    ///
    /// Callers anchor `start` as early as they can while keeping the span free
    /// of CLOSURE LITERALS — a `return` inside a closure exits the closure, not
    /// the function, and a textual scan cannot tell them apart. For
    /// `recordListenRevert` that is its first suspension: the preconditions
    /// above it (`guard var managed = windows[windowId] else { return }`) run
    /// before any await and so cannot be observing a lifecycle change, and
    /// nothing below it legitimately returns early. `revertByTimeRange` cannot
    /// be covered by one region for that reason — the span between its loops
    /// contains a `compactMap` closure — so it is scanned in three
    /// closure-free pieces instead. See the call sites.
    private func assertNoEarlyExit(
        in body: String,
        from start: String.Index,
        to end: String.Index,
        seam: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        guard start < end else {
            XCTFail(
                "\(seam): early-exit region anchors are inverted — the canary's anchors have drifted",
                file: file,
                line: line
            )
            return
        }
        let region = String(body[start..<end])
        XCTAssertEqual(
            SwiftSourceInspector.regexOccurrences(of: #"\breturn\b"#, in: region),
            0,
            """
            \(seam) can abort between its first suspension and its live-state \
            guard. Everything in that span is owed to the CAPTURED show \
            unconditionally — the gesture's rows are already durably reverted \
            and its trust penalty may already have landed — so an early exit \
            there drops the matching receipt and calibration, which is the \
            silent drop this canary exists to prevent. Offending region:
            \(region.trimmingCharacters(in: .whitespacesAndNewlines))
            """,
            file: file,
            line: line
        )
    }

    /// playhead-i08e (third-pass hardening): closes the residual gap the
    /// ordering assertions above cannot see on their own.
    ///
    /// Those assertions only prove each effect appears BEFORE the seam's
    /// live-state guard. They say nothing about what sits BETWEEN them — so an
    /// effect re-wrapped in its own equivalent lifecycle check, e.g.
    ///
    ///     if episodeLifecycleGeneration == sourceLifecycleGeneration {
    ///         recordThresholdControlSignal(.falsePositive, podcastId: podcastId)
    ///     }
    ///
    /// would restore the exact silent-drop bug this canary exists to prevent
    /// while every `XCTAssertLessThan` above still passed.
    ///
    /// The scan is by IDENTIFIER, so a re-check is rejected in every spelling —
    /// `guard`, `if`, ternary, or a clause folded into an existing condition —
    /// not just the `guard` form.
    ///
    /// playhead-i08e (fifth pass): the forbidden set is the whole
    /// lifecycle-identity family, not just `sourceLifecycleGeneration`. Pinning
    /// the one identifier left an equivalent test against a DIFFERENT field
    /// free to do the same damage, and folded into an existing condition it
    /// evades `braceDepth` as well (the guarded call's nesting is unchanged):
    ///
    ///     if revertedManagedAny, activePodcastId == podcastId {
    ///         recordThresholdControlSignal(.falsePositive, podcastId: podcastId)
    ///     }
    ///
    /// After an episode switch `activePodcastId` is the replacement show, so
    /// that drops the captured show's sample exactly as the original bug did —
    /// mutation-verified green against every other rail here. None of these
    /// identifiers appears in either scanned region today, and none has any
    /// business there: every value those regions need was captured before the
    /// first suspension. Re-checks that reach live state without naming any of
    /// them are covered by `braceDepth` (wrappers) and `assertNoEarlyExit`
    /// (top-level exits) instead; no one of the three is sufficient alone.
    ///
    /// The region bounds are load-bearing, and getting them wrong is how this
    /// assertion was defeated before: callers must anchor `start` ABOVE the
    /// first effect (at the generation capture, or at the top of the effects
    /// block), never at the first effect itself. An `if` opened one line
    /// earlier than the first effect wraps every effect while its own text lies
    /// outside a first-effect-anchored region — verified by mutation: wrapping
    /// all three `recordListenRevert` effects in a single lifecycle `if`
    /// reproduced the bug with this canary still green.
    private func assertNoLifecycleRecheck(
        in body: String,
        from start: String.Index,
        to end: String.Index,
        seam: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        // Anchors are located by literal search, so a refactor can make one of
        // them resolve to an unexpected occurrence and invert the bounds.
        // Report that as a canary failure; slicing `body[start..<end]` with
        // `start > end` traps the whole test runner instead.
        guard start < end else {
            XCTFail(
                "\(seam): lifecycle-recheck region anchors are inverted — the canary's anchors have drifted",
                file: file,
                line: line
            )
            return
        }
        let region = String(body[start..<end])
        for token in Self.lifecycleIdentityTokens {
            XCTAssertFalse(
                region.contains(token),
                """
                \(seam) re-checks live episode identity (`\(token)`) inside the \
                region that must be unconditional — everything between the \
                anchor above its durable/calibration effects and its \
                live-state guard. Those effects are owed to the CAPTURED show \
                unconditionally: gating any of them on the episode still being \
                current re-introduces the drop this canary guards (the trust \
                penalty has already landed by then), and every value this \
                region needs was captured before the first suspension. \
                Offending region:
                \(region.trimmingCharacters(in: .whitespacesAndNewlines))
                """,
                file: file,
                line: line
            )
        }
    }

    /// The live-episode-identity fields a seam must NOT consult between its
    /// first suspension and its live-state guard. See
    /// ``assertNoLifecycleRecheck(in:from:to:seam:file:line:)``.
    private static let lifecycleIdentityTokens = [
        "sourceLifecycleGeneration",
        "episodeLifecycleGeneration",
        "activeEpisodeId",
        "activePodcastId",
        "activeAssetId",
    ]

    /// Exact feedback receipts belong only in the durable correction store.
    /// Pin every explicit route against accidentally reusing the diagnostic
    /// decision logger or interpolating receipt fields into OSLog.
    func testExplicitBannerFeedbackRoutesDoNotWriteDetailedLogs() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/SkipOrchestrator/SkipOrchestrator.swift"
        )
        let signatures = [
            "func confirmAutoSkippedBanner(\n        windowId: String,",
            "func denyAutoSkippedBanner(\n        windowId: String,",
            "func acceptSuggestedSkip(\n        windowId: String,\n        ifCurrentEpisodeId",
            "func declineSuggestedSkip(\n        windowId: String,\n        isExplicitDenial: Bool = false,\n        ifCurrentEpisodeId",
        ]

        for signature in signatures {
            guard let rawBody = SwiftSourceInspector.firstBody(
                in: source,
                after: signature
            ) else {
                XCTFail("Could not locate explicit feedback route \(signature)")
                continue
            }
            let body = SwiftSourceInspector.strippingComments(rawBody)
            XCTAssertFalse(
                body.contains("logDecision("),
                "\(signature) must not append exact feedback to the decision log"
            )
            XCTAssertFalse(
                body.contains("localizedDescription"),
                "\(signature) must not copy persistence details into OSLog"
            )
            XCTAssertFalse(
                body.contains("String(describing:"),
                "\(signature) must not copy persistence details into OSLog"
            )
            if body.contains("recordFalseSkipSignal")
                || body.contains("recordFalseNegativeSignal")
            {
                XCTAssertTrue(
                    body.contains(
                        "privacy: .explicitBannerFeedback"
                    ),
                    "\(signature) must route every derived trust signal through the log-suppressed explicit-feedback branch"
                )
            }
        }
    }

    func testGenericRevertWindowRetainsManualVetoLoggingSemantics()
        throws
    {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath:
                "Playhead/Services/SkipOrchestrator/SkipOrchestrator.swift"
        )
        let signature =
            "func revertWindow(\n        windowId: String,\n        podcastId: String? = nil,\n        ifCurrentEpisodeId"
        guard let rawBody = SwiftSourceInspector.firstBody(
            in: source,
            after: signature
        ) else {
            XCTFail("Could not locate generic revertWindow body")
            return
        }
        let body = SwiftSourceInspector.strippingComments(rawBody)
        XCTAssertTrue(body.contains("source: .manualVeto"))
        XCTAssertTrue(body.contains("logDecision("))
        XCTAssertTrue(body.contains("recordFalseSkipSignal("))
        XCTAssertFalse(body.contains(".bannerAutoSkipDenied"))
        XCTAssertFalse(body.contains("privacy: .explicitBannerFeedback"))
    }

    func testExplicitTrustBranchesContainOnlyGenericFailureTelemetry()
        throws
    {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath:
                "Playhead/Services/TrustScoring/TrustScoringService.swift"
        )
        for signature in [
            "func recordFalseSkipSignal(",
            "func recordFalseNegativeSignal(",
        ] {
            guard let method = SwiftSourceInspector.firstBody(
                in: source,
                after: signature
            ), let branchRange = method.range(
                of: "if privacy == .explicitBannerFeedback"
            ), let openBrace = method[branchRange.upperBound...]
                .firstIndex(of: "{")
            else {
                XCTFail(
                    "Could not locate explicit trust branch for \(signature)"
                )
                continue
            }
            let branch = SwiftSourceInspector.strippingComments(
                SwiftSourceInspector.bracedBody(
                    in: method,
                    startingAt: openBrace
                )
            )
            XCTAssertTrue(
                branch.contains("Banner feedback trust update failed")
            )
            for forbidden in [
                "podcastId",
                "localizedDescription",
                "skipTrustScore",
                "recentFalseSkipSignals",
                "false-negative",
                "false-skip",
            ] {
                XCTAssertFalse(
                    branch.contains(forbidden),
                    "\(signature) explicit failure telemetry leaked \(forbidden)"
                )
            }
        }
    }

    func testExplicitBannerDerivedLearningFailureLogIsGeneric() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/AdDetection/UserCorrectionStore.swift"
        )
        guard let actorRange = source.range(
            of: "actor PersistentUserCorrectionStore"
        ) else {
            XCTFail("Could not locate PersistentUserCorrectionStore")
            return
        }
        let actorSource = String(source[actorRange.lowerBound...])
        guard let methodBody = SwiftSourceInspector.firstBody(
            in: actorSource,
            after: "func correctionDidPersistAtomically("
        ), let branchRange = methodBody.range(
            of: "if event.source?.isExplicitBannerFeedback == true"
        ), let openBrace = methodBody[branchRange.upperBound...]
            .firstIndex(of: "{")
        else {
            XCTFail("Could not locate explicit-feedback failure-log branch")
            return
        }
        let branch = SwiftSourceInspector.strippingComments(
            SwiftSourceInspector.bracedBody(
                in: methodBody,
                startingAt: openBrace
            )
        )
        XCTAssertTrue(branch.contains("Banner feedback follow-up failed"))
        XCTAssertFalse(branch.contains("analysisAssetId"))
        XCTAssertFalse(branch.contains("scope"))
        XCTAssertFalse(branch.contains("localizedDescription"))
        XCTAssertFalse(branch.contains("source"))
    }
}

/// Bounded stream reader for ordering tests. A missing event reports as `nil`
/// after the deadline instead of parking the entire test process forever.
private final class BoundedStreamProbe<Element: Sendable>: @unchecked Sendable {
    private actor State {
        private var buffered: [Element] = []
        private var waiters:
            [UUID: CheckedContinuation<Element?, Never>] = [:]
        private var isFinished = false

        func push(_ element: Element) {
            if let (id, waiter) = waiters.first {
                waiters.removeValue(forKey: id)
                waiter.resume(returning: element)
            } else {
                buffered.append(element)
            }
        }

        func finish() {
            isFinished = true
            let pending = waiters.values
            waiters.removeAll()
            for waiter in pending {
                waiter.resume(returning: nil)
            }
        }

        func next(timeout: Duration) async -> Element? {
            if !buffered.isEmpty {
                return buffered.removeFirst()
            }
            guard !isFinished else { return nil }

            let id = UUID()
            return await withCheckedContinuation { continuation in
                waiters[id] = continuation
                Task {
                    try? await Task.sleep(for: timeout)
                    self.timeout(id: id)
                }
            }
        }

        private func timeout(id: UUID) {
            guard let waiter = waiters.removeValue(forKey: id) else {
                return
            }
            waiter.resume(returning: nil)
        }
    }

    private let state: State
    private let timeout: Duration
    private let observationTask: Task<Void, Never>

    init(
        _ stream: AsyncStream<Element>,
        timeout: Duration = .seconds(2)
    ) {
        let state = State()
        self.state = state
        self.timeout = timeout
        observationTask = Task {
            for await element in stream {
                guard !Task.isCancelled else { break }
                await state.push(element)
            }
            await state.finish()
        }
    }

    deinit {
        observationTask.cancel()
    }

    func next() async -> Element? {
        await state.next(timeout: timeout)
    }
}

private actor ControlledAsyncGate {
    private var didStart = false
    private var startWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    func wait() async {
        didStart = true
        let waiters = startWaiters
        startWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
        await withCheckedContinuation { continuation in
            releaseContinuation = continuation
        }
    }

    func waitUntilStarted() async {
        if didStart { return }
        await withCheckedContinuation { continuation in
            startWaiters.append(continuation)
        }
    }

    func release() {
        releaseContinuation?.resume()
        releaseContinuation = nil
    }
}

/// Suspends fully-formed correction writes so lifecycle replacement can be
/// interleaved at the exact persistence boundary exercised by banner actions.
private actor GatedUserCorrectionStore: UserCorrectionStore {
    private let wrapped: any UserCorrectionStore
    private let recordGate: ControlledAsyncGate

    init(
        wrapped: any UserCorrectionStore,
        recordGate: ControlledAsyncGate
    ) {
        self.wrapped = wrapped
        self.recordGate = recordGate
    }

    func recordVeto(span: DecodedSpan) async {
        await wrapped.recordVeto(span: span)
    }

    func recordVeto(
        startTime: Double,
        endTime: Double,
        assetId: String,
        podcastId: String?,
        source: CorrectionSource
    ) async {
        await wrapped.recordVeto(
            startTime: startTime,
            endTime: endTime,
            assetId: assetId,
            podcastId: podcastId,
            source: source
        )
    }

    func record(_ event: CorrectionEvent) async throws {
        await recordGate.wait()
        try await wrapped.record(event)
    }

    func correctionDidPersistAtomically(
        _ event: CorrectionEvent,
        wasNewlyInserted: Bool
    ) async {
        await recordGate.wait()
        await wrapped.correctionDidPersistAtomically(
            event,
            wasNewlyInserted: wasNewlyInserted
        )
    }

    func correctionPassthroughFactor(
        for analysisAssetId: String
    ) async -> Double {
        await wrapped.correctionPassthroughFactor(
            for: analysisAssetId
        )
    }

    func correctionBoostFactor(
        for analysisAssetId: String
    ) async -> Double {
        await wrapped.correctionBoostFactor(for: analysisAssetId)
    }

    func correctionBoostFactor(
        for analysisAssetId: String,
        overlapping startTime: Double,
        endTime: Double
    ) async -> Double {
        await wrapped.correctionBoostFactor(
            for: analysisAssetId,
            overlapping: startTime,
            endTime: endTime
        )
    }
}

@Suite("SkipOrchestrator Revert - Time Range and Banner Paths")
struct SkipOrchestratorRevertTests {

    // MARK: - revertByTimeRange

    @Test("Episode-bound seek rejects a stale same-episode lifecycle token")
    func episodeBoundSeekRejectsStaleLifecycle() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)

        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1"
        )
        let staleGeneration =
            await orchestrator.episodeLifecycleGenerationSnapshot()
        // A replay can preserve the canonical episode ID while replacing all
        // of the orchestrator's in-memory state.
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1"
        )

        let acceptedStaleAction = await orchestrator.recordUserSeek(
            to: 42,
            ifEpisodeLifecycleGeneration: staleGeneration
        )
        #expect(!acceptedStaleAction)

        let currentGeneration =
            await orchestrator.episodeLifecycleGenerationSnapshot()
        let acceptedCurrentAction = await orchestrator.recordUserSeek(
            to: 42,
            ifEpisodeLifecycleGeneration: currentGeneration
        )
        #expect(acceptedCurrentAction)
    }

    @Test("revertByTimeRange reverts overlapping applied window")
    func revertByTimeRangeRevertsOverlapping() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues = ranges
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)

        let ad = makeSkipTestAdWindow(
            id: "ad-range-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])
        guard case .present = await probe.next() else {
            Issue.record("Expected the managed card before time-range revert")
            return
        }

        // Should have a skip cue before revert.
        // playhead-vn7n.2: cue end is pulled in by `adTrailingCushionSeconds`
        // (default 1.0 s) so the player lands slightly inside the ad rather
        // than risking a clip into program audio.
        let cushion = SkipPolicyConfig.default.adTrailingCushionSeconds
        #expect(!pushedCues.isEmpty)
        if let cue = pushedCues.first {
            #expect(CMTimeGetSeconds(cue.start) == 60)
            #expect(CMTimeGetSeconds(cue.end) == 120 - cushion)
        } else {
            Issue.record("Expected the pre-revert cue to preserve the finalized window boundaries")
        }

        // Revert by time range that overlaps the ad window.
        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: "podcast-1")
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record("Time-range revert must retire the managed card")
            return
        }
        #expect(retirement.windowId == ad.id)

        let log = await orchestrator.getDecisionLog()
        let reverted = log.filter {
            $0.decision == .reverted && $0.adWindowId == "ad-range-1"
        }
        #expect(!reverted.isEmpty)

        // Skip cues should be cleared after revert.
        #expect(pushedCues.isEmpty)
    }

    @Test("revertByTimeRange is a no-op when no windows overlap")
    func revertByTimeRangeNoOverlap() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues = ranges
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-range-noop",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        let cuesBefore = pushedCues

        // Revert a time range that does NOT overlap the ad window.
        await orchestrator.revertByTimeRange(start: 200, end: 300, podcastId: "podcast-1")

        let log = await orchestrator.getDecisionLog()
        let reverted = log.filter {
            $0.decision == .reverted && $0.adWindowId == "ad-range-noop"
        }
        #expect(reverted.isEmpty)

        // Cues should remain unchanged.
        #expect(pushedCues.count == cuesBefore.count)
    }

    @Test("revertByTimeRange reverts multiple overlapping windows in batch")
    func revertByTimeRangeMultipleWindows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues = ranges
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad1 = makeSkipTestAdWindow(
            id: "ad-batch-1",
            startTime: 60,
            endTime: 90,
            confidence: 0.9,
            decisionState: "confirmed"
        )
        let ad2 = makeSkipTestAdWindow(
            id: "ad-batch-2",
            startTime: 92,
            endTime: 120,
            confidence: 0.88,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad1)
        try await store.insertAdWindow(ad2)
        await orchestrator.receiveAdWindows([ad1, ad2])

        // Both windows should produce cues.
        #expect(!pushedCues.isEmpty)

        // Revert a broad time range covering both windows.
        await orchestrator.revertByTimeRange(start: 50, end: 130, podcastId: "podcast-1")

        let log = await orchestrator.getDecisionLog()
        let reverted = log.filter { $0.decision == .reverted }
        #expect(reverted.count >= 2)
        #expect(pushedCues.isEmpty)
    }

    @Test("revertByTimeRange skips already-reverted windows")
    func revertByTimeRangeSkipsAlreadyReverted() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-already-reverted",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        // First revert via windowId.
        await orchestrator.recordListenRevert(windowId: "ad-already-reverted", podcastId: "podcast-1")

        let logBefore = await orchestrator.getDecisionLog()
        let revertedBefore = logBefore.filter { $0.decision == .reverted }

        // Second revert by time range — should not produce an additional revert log entry.
        await orchestrator.revertByTimeRange(start: 60, end: 120, podcastId: "podcast-1")

        let logAfter = await orchestrator.getDecisionLog()
        let revertedAfter = logAfter.filter { $0.decision == .reverted }
        #expect(revertedAfter.count == revertedBefore.count)
    }

    // MARK: - revertWindow

    @Test("revertWindow records a public manual veto and generic decision log")
    func revertWindowRemovesCue() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues = ranges
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-generic-manual-veto",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        #expect(!pushedCues.isEmpty)
        let decisionCountBeforeFeedback =
            await orchestrator.getDecisionLog().count

        await orchestrator.revertWindow(
            windowId: ad.id,
            podcastId: "podcast-1"
        )

        let decisionLog = await orchestrator.getDecisionLog()
        #expect(decisionLog.count == decisionCountBeforeFeedback + 1)
        #expect(
            decisionLog.last?.decision == .reverted
                && decisionLog.last?.adWindowId == ad.id
        )
        let corrections = try await store.loadCorrectionEvents(
            analysisAssetId: ad.analysisAssetId
        )
        let correction = try #require(corrections.first)
        #expect(correction.source == .manualVeto)
        #expect(!correction.isPrivateExplicitFeedbackReceipt)
        #expect(pushedCues.isEmpty)
    }

    @Test("auto-skip Yes persists the exact validated receipt and emits no detailed decision record")
    func autoSkipYesPersistsValidatedReceipt() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 91
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let ad = makeSkipTestAdWindow(
            id: "auto-confirmed-window",
            startTime: 60.1234,
            endTime: 75.9876,
            confidence: 0.9
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])
        guard case let .present(item) = await probe.next() else {
            Issue.record("Expected auto-skipped banner material")
            return
        }
        let assetId = try #require(item.analysisAssetId)
        let materialToken = try #require(
            item.windowMaterialRevisionToken
        )
        let decisionCountBeforeFeedback =
            await orchestrator.getDecisionLog().count

        #expect(
            !(await orchestrator.confirmAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: assetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration: 90,
                ifWindowMaterialRevisionToken: materialToken
            )),
            "A stale playback lifecycle must fail closed"
        )
        #expect(
            !(await orchestrator.confirmAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: assetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken: materialToken + "-stale"
            )),
            "A stale same-window material revision must fail closed"
        )
        #expect(
            !(await orchestrator.confirmAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: assetId,
                startTime: item.adStartTime + 0.00001,
                endTime: item.adEndTime,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken: materialToken
            )),
            "A near-distinct span that shares the three-decimal scope must fail exactly"
        )
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: assetId
            ).isEmpty
        )

        #expect(
            await orchestrator.confirmAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: assetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken: materialToken
            )
        )
        let events = try await store.loadCorrectionEvents(
            analysisAssetId: assetId
        )
        let receipt = try #require(events.first)
        #expect(events.count == 1)
        #expect(receipt.source == .bannerAutoSkipConfirmed)
        #expect(receipt.correctionType == .falseNegative)
        #expect(receipt.targetRefs?.adWindowId == item.windowId)
        #expect(receipt.podcastId == "podcast-1")
        #expect(
            receipt.targetRefs?.exactFeedbackSpan?.matches(
                startTime: 60.1234,
                endTime: 75.9876
            ) == true
        )
        if case let .exactTimeSpan(receiptAssetId, start, end) =
            CorrectionScope.deserialize(receipt.scope) {
            #expect(receiptAssetId == assetId)
            #expect(start == 60.123)
            #expect(end == 75.988)
        } else {
            Issue.record("Expected exact auto-skip confirmation span")
        }
        #expect(
            await orchestrator.getDecisionLog().count
                == decisionCountBeforeFeedback,
            "Auto-skip Yes must not add a detailed decision-log record"
        )
    }

    @Test(
        "Auto-No commits while applied persistence is blocked and late apply cannot resurrect it",
        .timeLimit(.minutes(1))
    )
    func autoSkipNoWinsBlockedAppliedPersistenceRace() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        let appliedGate = ControlledAsyncGate()
        await orchestrator._setAppliedPersistenceBarrierForTesting {
            await appliedGate.wait()
        }
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 920
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let producer = makeSkipTestAdWindow(
            id: "auto-no-blocked-applied",
            startTime: 60.1234,
            endTime: 75.9876,
            confidence: 0.9
        )
        try await store.insertAdWindow(producer)
        await orchestrator.receiveAdWindows([producer])
        await appliedGate.waitUntilStarted()
        guard case let .present(item) = await probe.next() else {
            Issue.record("Expected auto-skip feedback card")
            return
        }
        let durableBeforeAnswer = try #require(
            try await store.fetchAdWindow(id: producer.id)
        )
        #expect(
            durableBeforeAnswer.decisionState
                == producer.decisionState,
            "The test must answer before the fire-and-forget applied write"
        )
        #expect(!durableBeforeAnswer.wasSkipped)

        #expect(
            !(await orchestrator.denyAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: item.analysisAssetId,
                startTime: item.adStartTime + 0.00001,
                endTime: item.adEndTime,
                podcastId: item.podcastId,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    item.windowMaterialRevisionToken
            )),
            "A near-distinct span that shares the three-decimal scope must fail exactly"
        )
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: producer.analysisAssetId
            ).isEmpty
        )
        #expect(
            await orchestrator.denyAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: item.analysisAssetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                podcastId: item.podcastId,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    item.windowMaterialRevisionToken
            )
        )
        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: producer.analysisAssetId
        )
        let receipt = try #require(receipts.first)
        #expect(receipts.count == 1)
        #expect(
            receipt.targetRefs?.exactFeedbackSpan?.matches(
                startTime: 60.1234,
                endTime: 75.9876
            ) == true
        )
        var finalRow = try #require(
            try await store.fetchAdWindow(id: producer.id)
        )
        #expect(
            finalRow.decisionState == AdDecisionState.reverted.rawValue
        )
        #expect(
            finalRow.wasSkipped,
            "The atomic No transaction must durably record the skip before reverting it"
        )

        let outward = try #require(
            try await store.responseIndependentAdWindows(
                analysisAssetId: producer.analysisAssetId
            )
        )
        #expect(outward.count == 1)
        #expect(outward[0].id == producer.id)
        #expect(outward[0].startTime == producer.startTime)
        #expect(outward[0].endTime == producer.endTime)
        #expect(!outward[0].wasSkipped)
        #expect(
            outward[0].decisionState == producer.decisionState,
            "The outward shape must match the response-independent producer baseline"
        )

        await appliedGate.release()
        #expect(
            try await store.persistAppliedAdWindowIfEligible(
                windowId: producer.id,
                analysisAssetId: producer.analysisAssetId,
                expectedProducerRevision: producer
            ) == false,
            "A late applied transition must be terminal-safe"
        )
        finalRow = try #require(
            try await store.fetchAdWindow(id: producer.id)
        )
        #expect(
            finalRow.decisionState == AdDecisionState.reverted.rawValue
        )
        #expect(finalRow.wasSkipped)
    }

    // MARK: - playhead-i08e: an unwired correction store must NOT veto feedback
    //
    // `UserCorrectionStore` is the POST-COMMIT derived-learning listener
    // (`schedulePostCommitCorrectionLearning`), not the durability owner — the
    // receipt is appended inside the same AnalysisStore transaction as the
    // AdWindow mutation. Between 5c1a167e and playhead-i08e the two correction
    // BUILDERS opened with `guard correctionStore != nil else { throw }`, so an
    // orchestrator without that optional listener aborted every explicit
    // response at its first statement: no receipt, no row flip, no trust or
    // threshold-control calibration. Production always injects the store
    // (`PlayheadRuntime` line ~1150, non-optional), so the precondition bought
    // nothing there while silently killing the gesture everywhere else.
    //
    // These three tests previously asserted the OPPOSITE contract ("rejects
    // without a correction store"). They kept passing after the precondition
    // was removed only because their fixture was incoherent: the asset row
    // carried `episodeId: "ep-1"` (the `makeSkipTestAnalysisAsset` default)
    // while `beginEpisode` declared `"episode-1"`, so `feedbackAssetMatches`
    // rejected the write for an unrelated ownership reason. Both the contract
    // and the fixture are corrected here — each seam now declares the real
    // episode id, so the ONLY variable under test is the absent listener.

    @Test("Auto-No persists its durable receipt with no correction store wired")
    func autoSkipNoPersistsWithoutCorrectionStore() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 201
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let applied = makeSkipTestAdWindow(
            id: "nil-store-auto-no",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.applied.rawValue
        )
        try await store.insertAdWindow(applied)
        await orchestrator.receiveAdWindows([applied])
        guard case let .present(item) = await probe.next() else {
            Issue.record("Expected applied auto-skip card")
            return
        }

        #expect(
            await orchestrator.denyAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: item.analysisAssetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                podcastId: item.podcastId,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    item.windowMaterialRevisionToken
            ),
            "an unwired derived-learning listener must not veto an explicit No"
        )
        let row = try #require(try await store.fetchAdWindow(id: applied.id))
        #expect(row.decisionState == AdDecisionState.reverted.rawValue)
        #expect(row.wasSkipped, "the denied skip is still recorded as having fired")
        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: applied.analysisAssetId
        )
        #expect(receipts.count == 1, "the No must commit exactly one durable receipt")
        #expect(receipts.first?.source == .bannerAutoSkipDenied)

        // A denial retires the window in place rather than evicting it, so
        // membership alone holds under BOTH the old rejection contract and this
        // one and would prove nothing. Re-answering the same card is the
        // discriminating check: the row is terminal now, so the retry must be
        // refused and must not mint a second receipt.
        #expect((await orchestrator.activeWindowIDs()).contains(applied.id))
        #expect(
            !(await orchestrator.denyAutoSkippedBanner(
                windowId: item.windowId,
                analysisAssetId: item.analysisAssetId,
                startTime: item.adStartTime,
                endTime: item.adEndTime,
                podcastId: item.podcastId,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    item.windowMaterialRevisionToken
            )),
            "a re-answered card must not write a second receipt"
        )
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: applied.analysisAssetId
            ).count == 1
        )
    }

    @Test("Suggest-Yes persists its durable receipt with no correction store wired")
    func suggestYesPersistsWithoutCorrectionStore() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 202
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let suggestion = makeSuggestWindow(id: "nil-store-suggest-yes")
        try await store.insertAdWindow(suggestion)
        await orchestrator.receiveAdWindows([suggestion])
        guard case let .present(item) = await probe.next() else {
            Issue.record("Expected suggestion card")
            return
        }

        #expect(
            await orchestrator.acceptSuggestedSkip(
                windowId: item.windowId,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifSuggestionRevisionToken:
                    item.suggestionRevisionToken
            ),
            "an unwired derived-learning listener must not veto an explicit Yes"
        )
        let row = try #require(
            try await store.fetchAdWindow(id: suggestion.id)
        )
        #expect(
            row.decisionState == AdDecisionState.suppressed.rawValue,
            "the original suggest row is retired in favour of the promoted window"
        )
        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: suggestion.analysisAssetId
        )
        #expect(receipts.count == 1, "the Yes must commit exactly one durable receipt")
        #expect(receipts.first?.source == .bannerSuggestionConfirmed)
        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(suggestion.id)
        )
        #expect(!(await orchestrator.activeWindowIDs()).isEmpty)
    }

    @Test("Suggest-No persists its durable receipt with no correction store wired")
    func suggestNoPersistsWithoutCorrectionStore() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 203
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let suggestion = makeSuggestWindow(id: "nil-store-suggest-no")
        try await store.insertAdWindow(suggestion)
        await orchestrator.receiveAdWindows([suggestion])
        guard case let .present(item) = await probe.next() else {
            Issue.record("Expected suggestion card")
            return
        }

        #expect(
            await orchestrator.declineSuggestedSkip(
                windowId: item.windowId,
                isExplicitDenial: true,
                ifCurrentEpisodeId: item.episodeId,
                ifPlaybackLifecycleGeneration:
                    item.playbackLifecycleGeneration,
                ifSuggestionRevisionToken:
                    item.suggestionRevisionToken
            ),
            "an unwired derived-learning listener must not veto an explicit No"
        )
        let row = try #require(
            try await store.fetchAdWindow(id: suggestion.id)
        )
        #expect(row.decisionState == AdDecisionState.reverted.rawValue)
        #expect(row.userDismissedBanner)
        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: suggestion.analysisAssetId
        )
        #expect(receipts.count == 1, "the No must commit exactly one durable receipt")
        #expect(receipts.first?.source == .bannerSuggestionDenied)
        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(suggestion.id)
        )
    }

    /// playhead-i08e: the guard that ACTUALLY refuses an explicit response is
    /// `AnalysisStore.feedbackAssetMatches` — the acting card's episode must own
    /// the asset row. All FOUR explicit-feedback transactions share it
    /// (`persistDeniedAutoSkip`, `persistConfirmedAutoSkip`,
    /// `persistAcceptedSuggestionIfCurrent`,
    /// `persistDeclinedSuggestionIfCurrent`) and each is asserted below, so
    /// deleting the clause from any one of them fails here.
    ///
    /// This is pinned deliberately: the three tests above used to exercise this
    /// rejection by ACCIDENT — their asset row carried the fixture's default
    /// `episodeId` while `beginEpisode` declared another — which is what let
    /// them keep passing under the name "rejects without a correction store"
    /// after that precondition was deleted. Fixing those fixtures would have
    /// left the ownership check with no coverage at this layer.
    @Test("explicit responses are refused when the card's episode does not own the asset")
    func explicitResponseRequiresAssetEpisodeOwnership() async throws {
        let store = try await makeTestStore()
        // The asset row belongs to "episode-owner"; the orchestrator (and so
        // every card it emits) claims "episode-impostor".
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-owner")
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-impostor",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 204
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        // Each seam gets its OWN window. Sharing one would let the first
        // refusal make the row terminal, so a later seam would then be refused
        // for a reason other than ownership and its assertion would stop
        // discriminating.
        let denyTarget = makeSkipTestAdWindow(
            id: "foreign-episode-auto-no",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.applied.rawValue
        )
        let confirmTarget = makeSkipTestAdWindow(
            id: "foreign-episode-auto-yes",
            startTime: 400,
            endTime: 460,
            confidence: 0.9,
            decisionState: AdDecisionState.applied.rawValue
        )
        let acceptTarget = makeSuggestWindow(
            id: "foreign-episode-suggest-yes",
            startTime: 200,
            endTime: 260
        )
        let declineTarget = makeSuggestWindow(
            id: "foreign-episode-suggest-no",
            startTime: 300,
            endTime: 360
        )
        let all = [denyTarget, confirmTarget, acceptTarget, declineTarget]
        for window in all {
            try await store.insertAdWindow(window)
        }
        await orchestrator.receiveAdWindows(all)

        // Several windows are ingested, so collect presentations until both
        // auto-skip cards have arrived rather than assuming an emission order.
        var cards: [String: AdSkipBannerItem] = [:]
        let autoSkipIds = Set([denyTarget.id, confirmTarget.id])
        for _ in 0..<8 where !autoSkipIds.isSubset(of: Set(cards.keys)) {
            guard case let .present(candidate) = await probe.next() else { break }
            cards[candidate.windowId] = candidate
        }
        guard let denyCard = cards[denyTarget.id],
              let confirmCard = cards[confirmTarget.id]
        else {
            Issue.record("Expected an applied auto-skip card for both windows")
            return
        }

        // 1/4 — persistDeniedAutoSkip.
        #expect(
            !(await orchestrator.denyAutoSkippedBanner(
                windowId: denyCard.windowId,
                analysisAssetId: denyCard.analysisAssetId,
                startTime: denyCard.adStartTime,
                endTime: denyCard.adEndTime,
                podcastId: denyCard.podcastId,
                ifCurrentEpisodeId: denyCard.episodeId,
                ifPlaybackLifecycleGeneration:
                    denyCard.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    denyCard.windowMaterialRevisionToken
            )),
            "a card whose episode does not own the asset may not write a No receipt"
        )
        // 2/4 — persistConfirmedAutoSkip.
        #expect(
            !(await orchestrator.confirmAutoSkippedBanner(
                windowId: confirmCard.windowId,
                analysisAssetId: confirmCard.analysisAssetId,
                startTime: confirmCard.adStartTime,
                endTime: confirmCard.adEndTime,
                ifCurrentEpisodeId: confirmCard.episodeId,
                ifPlaybackLifecycleGeneration:
                    confirmCard.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    confirmCard.windowMaterialRevisionToken
            )),
            "a card whose episode does not own the asset may not write a Yes receipt"
        )
        // 3/4 — persistAcceptedSuggestionIfCurrent.
        #expect(
            !(await orchestrator.acceptSuggestedSkip(
                windowId: acceptTarget.id,
                ifCurrentEpisodeId: "episode-impostor"
            )),
            "a suggest Yes may not promote a window the episode does not own"
        )
        // 4/4 — persistDeclinedSuggestionIfCurrent.
        #expect(
            !(await orchestrator.declineSuggestedSkip(
                windowId: declineTarget.id,
                isExplicitDenial: true,
                ifCurrentEpisodeId: "episode-impostor"
            )),
            "a suggest No may not veto a window the episode does not own"
        )

        // Every durable surface is untouched by all four refusals.
        for expected in all {
            let row = try #require(try await store.fetchAdWindow(id: expected.id))
            #expect(
                row.decisionState == expected.decisionState,
                "\(expected.id) must keep its pre-response decision state"
            )
            #expect(!row.userDismissedBanner)
        }
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: denyTarget.analysisAssetId
            ).isEmpty
        )
        // The rejected suggestions stay live for the surface that DOES own them.
        let suggestIds = await orchestrator.activeSuggestWindowIDs()
        #expect(suggestIds.contains(acceptTarget.id))
        #expect(suggestIds.contains(declineTarget.id))
    }

    @Test("materially revised applied same-ID cards retire stale Yes and No ownership")
    func appliedMaterialRevisionRetiresOldCardActions() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 204
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let revisionA = makeSkipTestAdWindow(
            id: "same-id-material-revision",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.applied.rawValue
        )
        try await store.insertAdWindow(revisionA)
        await orchestrator.receiveAdWindows([revisionA])
        guard case let .present(itemA) = await probe.next() else {
            Issue.record("Expected revision-A card")
            return
        }

        let revisionB = makeSkipTestAdWindow(
            id: revisionA.id,
            startTime: 75,
            endTime: 138,
            confidence: 0.94,
            decisionState: AdDecisionState.applied.rawValue
        )
        try await store.insertOrReplaceAdWindow(revisionB)
        await orchestrator.receiveAdWindows([revisionB])
        guard case let .retireWindow(retirement) = await probe.next()
        else {
            Issue.record("Expected revision-A retirement")
            return
        }
        #expect(retirement.windowId == revisionA.id)
        guard case let .present(itemB) = await probe.next() else {
            Issue.record("Expected revision-B card")
            return
        }
        #expect(
            itemA.windowMaterialRevisionToken
                != itemB.windowMaterialRevisionToken
        )

        #expect(
            !(await orchestrator.confirmAutoSkippedBanner(
                windowId: itemA.windowId,
                analysisAssetId: itemA.analysisAssetId,
                startTime: itemA.adStartTime,
                endTime: itemA.adEndTime,
                ifCurrentEpisodeId: itemA.episodeId,
                ifPlaybackLifecycleGeneration:
                    itemA.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    itemA.windowMaterialRevisionToken
            ))
        )
        #expect(
            !(await orchestrator.denyAutoSkippedBanner(
                windowId: itemA.windowId,
                analysisAssetId: itemA.analysisAssetId,
                startTime: itemA.adStartTime,
                endTime: itemA.adEndTime,
                podcastId: itemA.podcastId,
                ifCurrentEpisodeId: itemA.episodeId,
                ifPlaybackLifecycleGeneration:
                    itemA.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    itemA.windowMaterialRevisionToken
            ))
        )
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: revisionA.analysisAssetId
            ).isEmpty
        )
        #expect(
            try await store.fetchAdWindow(id: revisionB.id)?
                .decisionState == AdDecisionState.applied.rawValue
        )

        #expect(
            await orchestrator.denyAutoSkippedBanner(
                windowId: itemB.windowId,
                analysisAssetId: itemB.analysisAssetId,
                startTime: itemB.adStartTime,
                endTime: itemB.adEndTime,
                podcastId: itemB.podcastId,
                ifCurrentEpisodeId: itemB.episodeId,
                ifPlaybackLifecycleGeneration:
                    itemB.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    itemB.windowMaterialRevisionToken
            )
        )
        let finalRow = try #require(
            try await store.fetchAdWindow(id: revisionB.id)
        )
        #expect(finalRow.decisionState == AdDecisionState.reverted.rawValue)
        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: revisionB.analysisAssetId
        )
        #expect(receipts.count == 1)
        #expect(receipts.first?.source == .bannerAutoSkipDenied)
        #expect(receipts.first?.targetRefs?.adWindowId == revisionB.id)
    }

    @Test(
        "revertWindow removes its cue without awaiting trust calibration",
        .timeLimit(.minutes(1))
    )
    func revertWindowCueRemovalDoesNotWaitForTrust() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        let trustGate = ControlledAsyncGate()
        await orchestrator._setFalseSkipSignalHandlerForTesting { _ in
            await trustGate.wait()
        }
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        await orchestrator.setActiveSkipMode(.auto)

        let ad = makeSkipTestAdWindow(
            id: "ad-revert-held-trust",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])
        #expect(!pushedCues.isEmpty)

        let accepted = await orchestrator.revertWindow(
            windowId: ad.id,
            podcastId: "podcast-1"
        )

        #expect(accepted)
        #expect(
            pushedCues.isEmpty,
            "The stale playback cue must disappear before trust storage finishes"
        )
        await trustGate.waitUntilStarted()
        await trustGate.release()
    }

    @Test("revertWindow persistence failure preserves cue and rejects correction")
    func revertWindowPersistenceFailureIsRetryable() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-generic-veto-failure",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "applied"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])
        #expect(!pushedCues.isEmpty)

        try await store.execForTesting(
            """
            CREATE TRIGGER fail_generic_revert
            BEFORE UPDATE OF decisionState ON ad_windows
            WHEN NEW.id = 'ad-generic-veto-failure'
              AND NEW.decisionState = 'reverted'
            BEGIN
                SELECT RAISE(ABORT, 'injected banner revert failure');
            END
            """
        )

        let accepted = await orchestrator.revertWindow(
            windowId: ad.id,
            podcastId: "podcast-1"
        )
        #expect(
            accepted == false,
            "Production must keep the generic correction retryable when its durable revert fails"
        )
        #expect(
            !pushedCues.isEmpty,
            "A rejected correction must not pretend the active skip cue was corrected"
        )
        #expect((await orchestrator.activeWindowIDs()).contains(ad.id))
        #expect(
            !(await orchestrator.getDecisionLog()).contains {
                $0.adWindowId == ad.id && $0.decision == .reverted
            }
        )
        let row = try #require(
            (try await store.fetchAdWindows(assetId: "asset-1"))
                .first { $0.id == ad.id }
        )
        #expect(row.decisionState == AdDecisionState.applied.rawValue)
        #expect(
            try await correctionStore.activeCorrections(for: "asset-1")
                .isEmpty,
            "A rolled-back revert must not leave a premature correction"
        )

        try await store.execForTesting("DROP TRIGGER fail_generic_revert")
        #expect(
            await orchestrator.revertWindow(
                windowId: ad.id,
                podcastId: "podcast-1"
            )
        )
        let corrections = try await correctionStore.activeCorrections(
            for: "asset-1"
        )
        #expect(corrections.count == 1)
        #expect(corrections.first?.source == .manualVeto)
        #expect(
            corrections.first?.isPrivateExplicitFeedbackReceipt == false
        )
        #expect(
            corrections.first?.targetRefs?.adWindowId == ad.id
        )
        #expect(
            corrections.first?.submissionCount == 1,
            "The failed attempt must not count as a durable submission"
        )
    }

    @Test(
        "revertWindow persists episode A feedback across direct replacement",
        .timeLimit(.minutes(1))
    )
    func revertWindowPersistsSourceFeedbackAcrossEpisodeReplacement() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-1",
                episodeId: "episode-1"
            )
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-2",
                episodeId: "episode-2"
            )
        )
        let persistentCorrections = PersistentUserCorrectionStore(
            store: store
        )
        let correctionGate = ControlledAsyncGate()
        let gatedCorrections = GatedUserCorrectionStore(
            wrapped: persistentCorrections,
            recordGate: correctionGate
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: gatedCorrections
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 301
        )

        let sourceWindow = makeSkipTestAdWindow(
            id: "revert-during-replacement",
            assetId: "asset-1"
        )
        try await store.insertAdWindow(sourceWindow)
        await orchestrator.receiveAdWindows([sourceWindow])

        let primaryResultGate = ControlledAsyncGate()
        let response = Task {
            let result = await orchestrator.revertWindow(
                windowId: sourceWindow.id,
                podcastId: "podcast-1",
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 301
            )
            await primaryResultGate.wait()
            return result
        }
        await correctionGate.waitUntilStarted()
        await primaryResultGate.waitUntilStarted()
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2",
            episodeId: "episode-2",
            podcastId: "podcast-2",
            playbackLifecycleGeneration: 302
        )
        await correctionGate.release()
        await primaryResultGate.release()

        #expect(await response.value)
        let corrections = try await persistentCorrections.activeCorrections(
            for: "asset-1"
        )
        #expect(corrections.count == 1)
        #expect(corrections.first?.correctionType == .falsePositive)
        let sourceRow = try #require(
            (try await store.fetchAdWindows(assetId: "asset-1"))
                .first { $0.id == sourceWindow.id }
        )
        #expect(
            sourceRow.decisionState == AdDecisionState.reverted.rawValue
        )
        #expect((await orchestrator.activeWindowIDs()).isEmpty)
    }

    @Test("revertWindow rejects an in-memory window with no durable row")
    func revertWindowMissingDurableRowIsRetryable() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let windowId = "missing-durable-user-mark"
        await orchestrator.injectUserMarkedAd(
            start: 60,
            end: 120,
            analysisAssetId: "asset-1",
            windowId: windowId
        )
        #expect(!pushedCues.isEmpty)

        let accepted = await orchestrator.revertWindow(
            windowId: windowId,
            podcastId: "podcast-1"
        )

        #expect(!accepted)
        #expect(!pushedCues.isEmpty)
        #expect((await orchestrator.activeWindowIDs()).contains(windowId))
        #expect(
            !(await orchestrator.getDecisionLog()).contains {
                $0.adWindowId == windowId && $0.decision == .reverted
            }
        )
    }

    @Test("revertWindow is a no-op for unknown window ID")
    func revertWindowUnknownId() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        // Should not crash or log a decision for a nonexistent window.
        await orchestrator.revertWindow(windowId: "nonexistent", podcastId: "podcast-1")

        let log = await orchestrator.getDecisionLog()
        #expect(log.isEmpty)
    }

    @Test("revertWindow is idempotent — second call is a no-op")
    func revertWindowIdempotent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-double-revert",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        // First revert commits and logs the generic manual-veto correction.
        let firstAccepted = await orchestrator.revertWindow(
            windowId: "ad-double-revert",
            podcastId: "podcast-1"
        )
        #expect(firstAccepted)

        let logAfterFirst = await orchestrator.getDecisionLog()
        let revertedFirst = logAfterFirst.filter {
            $0.decision == .reverted && $0.adWindowId == "ad-double-revert"
        }
        #expect(revertedFirst.count == 1)

        // Second revert is rejected by the already-reverted durable state.
        let secondAccepted = await orchestrator.revertWindow(
            windowId: "ad-double-revert",
            podcastId: "podcast-1"
        )
        #expect(!secondAccepted)

        let logAfterSecond = await orchestrator.getDecisionLog()
        let revertedSecond = logAfterSecond.filter {
            $0.decision == .reverted && $0.adWindowId == "ad-double-revert"
        }
        #expect(
            revertedSecond.count == 1,
            "The rejected duplicate must not add another generic decision record"
        )
    }

    // MARK: - Segment broadcast after revert

    @Test("beginEpisode clears prior cues and segment markers before hydration")
    func beginEpisodePublishesClearedPlaybackState() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "episode-1")
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-2", episodeId: "episode-2")
        )
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var latestCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in
            latestCues = ranges
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1"
        )
        let segmentStream = await orchestrator.appliedSegmentsStream()
        let segmentProbe = BoundedStreamProbe(segmentStream)

        let ad = makeSkipTestAdWindow(
            id: "episode-1-cue",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])
        let activeSegments = await segmentProbe.next()
        #expect(activeSegments?.count == 1)
        #expect(latestCues.count == 1)

        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2",
            episodeId: "episode-2",
            podcastId: "podcast-2"
        )
        let clearedSegments = await segmentProbe.next()
        #expect(clearedSegments?.isEmpty == true)
        #expect(
            latestCues.isEmpty,
            "A direct episode switch must remove the prior item's transport cues"
        )
    }

    @Test("endEpisode clears cues and segment markers without a replacement asset")
    func endEpisodePublishesClearedPlaybackState() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "episode-1")
        )
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var latestCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { latestCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1"
        )
        let segmentStream = await orchestrator.appliedSegmentsStream()
        let segmentProbe = BoundedStreamProbe(segmentStream)

        let ad = makeSkipTestAdWindow(
            id: "episode-1-end-cue",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])
        #expect((await segmentProbe.next())?.count == 1)
        #expect(latestCues.count == 1)

        await orchestrator.endEpisode()

        #expect((await segmentProbe.next())?.isEmpty == true)
        #expect(
            latestCues.isEmpty,
            "a replacement with no asset must not retain the prior item's transport cues"
        )
    }

    @Test("revertByTimeRange broadcasts updated segments to listeners")
    func revertByTimeRangeBroadcastsSegments() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-broadcast",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        // Collect segment updates via the stream.
        let stream = await orchestrator.appliedSegmentsStream()
        nonisolated(unsafe) var receivedSegments: [(start: Double, end: Double)]?

        // Revert the window — this should trigger a broadcast with the window removed.
        await orchestrator.revertByTimeRange(start: 60, end: 120, podcastId: "podcast-1")

        // Read the first emitted value from the stream.
        for await segments in stream {
            receivedSegments = segments
            break
        }

        // The reverted window should no longer appear in segments.
        let overlapping = receivedSegments?.filter { $0.start < 120 && $0.end > 60 } ?? []
        #expect(overlapping.isEmpty)
    }

    // MARK: - playhead-zskc code review I5: one gesture, one correction event

    @Test("revertByTimeRange persists exactly one CorrectionEvent per gesture, even when N windows overlap")
    func revertByTimeRangeWritesOneCorrectionPerGesture() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Three adjacent ad windows, all overlapping the user's 50..130 gesture.
        let ads = [
            makeSkipTestAdWindow(id: "ad-dedupe-1", startTime: 55, endTime: 80,
                                 confidence: 0.9, decisionState: "confirmed"),
            makeSkipTestAdWindow(id: "ad-dedupe-2", startTime: 85, endTime: 105,
                                 confidence: 0.9, decisionState: "confirmed"),
            makeSkipTestAdWindow(id: "ad-dedupe-3", startTime: 110, endTime: 125,
                                 confidence: 0.9, decisionState: "confirmed"),
        ]
        for ad in ads { try await store.insertAdWindow(ad) }
        await orchestrator.receiveAdWindows(ads)

        // One gesture: "none of this is an ad" from 50..130.
        await orchestrator.revertByTimeRange(start: 50, end: 130, podcastId: "podcast-1")

        // The veto persistence is fire-and-forget via an unstructured Task —
        // poll (with a ceiling) until a CorrectionEvent appears.
        var corrections: [CorrectionEvent] = []
        for _ in 0..<20 {  // up to ~1s
            corrections = try await correctionStore.activeCorrections(for: "asset-1")
            if !corrections.isEmpty { break }
            try await Task.sleep(nanoseconds: 50_000_000)
        }

        #expect(corrections.count == 1,
                "Three overlapping windows reverted by one gesture must produce exactly one CorrectionEvent, got \(corrections.count)")
        // And the persisted scope must span the user's gesture, not a window's snapped boundary.
        if let scope = corrections.first.flatMap({ CorrectionScope.deserialize($0.scope) }),
           case .exactTimeSpan(_, let startTime, let endTime) = scope {
            #expect(startTime == 50.0, "persisted start must be the user's gesture start")
            #expect(endTime == 130.0, "persisted end must be the user's gesture end")
        } else {
            Issue.record("Expected exactTimeSpan scope from the revert, got \(corrections.first?.scope ?? "<none>")")
        }
    }

    // MARK: - playhead-hygc.1.8: revertByTimeRange must also handle suggest-tier (markOnly) windows

    @Test("revertByTimeRange reverts overlapping markOnly suggest-tier windows and persists decisionState")
    func revertByTimeRangeRevertsSuggestTierMarkOnlyWindow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Construct a markOnly AdWindow — the suggest-tier surface used by
        // boundary-singleton recall, correction-replay, and any algorithmic
        // path the precision gate demoted from auto-skip.
        let markOnly = AdWindow(
            id: "ad-suggest-1",
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.55,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "test-1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 60,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([markOnly])

        // Sanity: the window is in the suggest tier, not the auto-skip dict.
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-1"),
                "markOnly AdWindow must enter the suggest dictionary")

        // User vetoes via the time-range correction path.
        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: "podcast-1")

        // The suggest-tier entry must be cleared.
        #expect(!(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-1")),
                "vetoed markOnly window must be removed from suggestWindows")

        // A late producer may still hold the pre-veto candidate object. The
        // in-session presentation gate must reject that redelivery rather
        // than depending on a fresh persistence read.
        await orchestrator.receiveAdWindows([markOnly])
        #expect(!(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-1")),
                "late redelivery must not recreate a vetoed suggestion")

        // The persisted decisionState must reflect the user's veto so a
        // subsequent run / replay does not resurface the entry.
        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        let row = persisted.first { $0.id == "ad-suggest-1" }
        #expect(row?.decisionState == AdDecisionState.reverted.rawValue,
                "persisted markOnly window must be in .reverted state, got \(row?.decisionState ?? "<missing>")")

        // And exactly one CorrectionEvent was persisted (one gesture, one event).
        var corrections: [CorrectionEvent] = []
        for _ in 0..<20 {
            corrections = try await correctionStore.activeCorrections(for: "asset-1")
            if !corrections.isEmpty { break }
            try await Task.sleep(nanoseconds: 50_000_000)
        }
        #expect(corrections.count == 1,
                "one veto gesture against a markOnly window must produce exactly one CorrectionEvent")
    }

    @Test("revertByTimeRange does not increase auto-skip count when reverting a markOnly window")
    func revertByTimeRangeMarkOnlyDoesNotPromoteToAutoSkip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues = ranges
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let markOnly = AdWindow(
            id: "ad-suggest-noskip",
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.55,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "test-1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 60,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([markOnly])

        // markOnly must not produce a skip cue before the revert.
        #expect(pushedCues.isEmpty,
                "markOnly window must not emit auto-skip cues; got \(pushedCues.count)")

        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: "podcast-1")

        // After revert: still no skip cues. The veto must NEVER promote to auto-skip.
        #expect(pushedCues.isEmpty,
                "veto of markOnly window must not promote to auto-skip; got \(pushedCues.count) cues")
    }

    // R2 (hygc.1.8): hardening test. With multiple suggest-tier entries
    // present, a localized revert must clear ONLY the overlapping entry
    // and leave the others intact. This pins the iteration loop against
    // two failure modes:
    //   * dict-mutation-while-iterating skipping or duplicating entries
    //   * an over-zealous "clear all suggest entries on any revert" bug
    @Test("revertByTimeRange clears only the overlapping suggest-tier entry; non-overlapping entries survive")
    func revertByTimeRangeOnlyClearsOverlappingSuggestEntries() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Three markOnly entries at distinct, non-overlapping ranges.
        let markOnlyIds = ["ad-suggest-A", "ad-suggest-B", "ad-suggest-C"]
        let ranges: [(Double, Double)] = [(60, 120), (300, 360), (900, 960)]
        var markOnlyWindows: [AdWindow] = []
        for (id, range) in zip(markOnlyIds, ranges) {
            let window = AdWindow(
                id: id,
                analysisAssetId: "asset-1",
                startTime: range.0,
                endTime: range.1,
                confidence: 0.55,
                boundaryState: AdBoundaryState.segmentAggregated.rawValue,
                decisionState: AdDecisionState.candidate.rawValue,
                detectorVersion: "test-1",
                advertiser: nil, product: nil, adDescription: nil,
                evidenceText: nil, evidenceStartTime: range.0,
                metadataSource: "none",
                metadataConfidence: nil,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false,
                evidenceSources: nil,
                eligibilityGate: SkipEligibilityGate.markOnly.rawValue
            )
            try await store.insertAdWindow(window)
            markOnlyWindows.append(window)
        }
        await orchestrator.receiveAdWindows(markOnlyWindows)

        let beforeIds = await orchestrator.activeSuggestWindowIDs()
        for id in markOnlyIds {
            #expect(beforeIds.contains(id),
                    "all three markOnly windows must enter the suggest dict; missing \(id)")
        }

        // Veto a span that overlaps ONLY the middle entry (300..360).
        await orchestrator.revertByTimeRange(start: 320, end: 340, podcastId: "podcast-1")

        let afterIds = await orchestrator.activeSuggestWindowIDs()
        #expect(!afterIds.contains("ad-suggest-B"),
                "middle entry must be cleared; afterIds=\(afterIds)")
        #expect(afterIds.contains("ad-suggest-A"),
                "first non-overlapping entry must survive; afterIds=\(afterIds)")
        #expect(afterIds.contains("ad-suggest-C"),
                "third non-overlapping entry must survive; afterIds=\(afterIds)")
        #expect(afterIds.count == 2,
                "exactly one entry must be removed; afterIds=\(afterIds)")

        // Persistence reflects the same partition.
        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        let revertedRow = persisted.first { $0.id == "ad-suggest-B" }
        #expect(revertedRow?.decisionState == AdDecisionState.reverted.rawValue,
                "only middle entry must be persisted as .reverted; got \(revertedRow?.decisionState ?? "<missing>")")
        let untouchedA = persisted.first { $0.id == "ad-suggest-A" }
        let untouchedC = persisted.first { $0.id == "ad-suggest-C" }
        #expect(untouchedA?.decisionState == AdDecisionState.candidate.rawValue,
                "non-overlapping entry A must remain .candidate; got \(untouchedA?.decisionState ?? "<missing>")")
        #expect(untouchedC?.decisionState == AdDecisionState.candidate.rawValue,
                "non-overlapping entry C must remain .candidate; got \(untouchedC?.decisionState ?? "<missing>")")
    }

    // R3 (hygc.1.8): the R2 hardening test exercises the snapshot pattern
    // with only ONE matching entry — which can pass even when the
    // dict-mutation-while-iterating pattern is intact, because removing
    // a single key during a single-pass iteration rarely visibly fails
    // (even though Swift documents it as undefined behavior). To pin
    // R2's snapshot fix against actual regression we need a test where
    // the veto matches MULTIPLE suggest entries: removing N>1 keys
    // mid-iteration is the case where the bug actually manifests
    // (skipping or duplicating entries depending on stdlib hash table
    // state). This test feeds five suggest entries, vetoes a span that
    // overlaps three of them, and asserts every overlapping entry is
    // cleared and every non-overlapping entry survives.
    @Test("revertByTimeRange clears ALL overlapping suggest-tier entries when multiple match")
    func revertByTimeRangeClearsAllOverlappingSuggestEntries() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Five markOnly entries. The veto span 200..500 will overlap
        // entries B, C, D — three removals — so the snapshot pattern is
        // the only correct way to drive the loop. A naive
        // dict-mutation-while-iterating loop would skip an entry or
        // visit one twice depending on stdlib hash placement.
        let entries: [(id: String, range: (Double, Double))] = [
            ("ad-suggest-A", (50, 100)),    // before veto, must survive
            ("ad-suggest-B", (210, 250)),   // inside veto, must be reverted
            ("ad-suggest-C", (300, 350)),   // inside veto, must be reverted
            ("ad-suggest-D", (400, 450)),   // inside veto, must be reverted
            ("ad-suggest-E", (600, 660))    // after veto, must survive
        ]
        var windows: [AdWindow] = []
        for (id, range) in entries {
            let window = AdWindow(
                id: id,
                analysisAssetId: "asset-1",
                startTime: range.0,
                endTime: range.1,
                confidence: 0.55,
                boundaryState: AdBoundaryState.segmentAggregated.rawValue,
                decisionState: AdDecisionState.candidate.rawValue,
                detectorVersion: "test-1",
                advertiser: nil, product: nil, adDescription: nil,
                evidenceText: nil, evidenceStartTime: range.0,
                metadataSource: "none",
                metadataConfidence: nil,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false,
                evidenceSources: nil,
                eligibilityGate: SkipEligibilityGate.markOnly.rawValue
            )
            try await store.insertAdWindow(window)
            windows.append(window)
        }
        await orchestrator.receiveAdWindows(windows)

        let beforeIds = await orchestrator.activeSuggestWindowIDs()
        #expect(beforeIds.count == 5, "all five must enter the suggest dict; got \(beforeIds)")

        // Veto a span overlapping B, C, and D.
        await orchestrator.revertByTimeRange(start: 200, end: 500, podcastId: "podcast-1")

        let afterIds = await orchestrator.activeSuggestWindowIDs()
        #expect(afterIds == ["ad-suggest-A", "ad-suggest-E"],
                "only outside entries A and E must survive; got \(afterIds)")

        // Persistence: all three overlapping entries must be .reverted,
        // both non-overlapping must remain .candidate.
        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        let revertedIds: Set<String> = ["ad-suggest-B", "ad-suggest-C", "ad-suggest-D"]
        for id in revertedIds {
            let row = persisted.first { $0.id == id }
            #expect(row?.decisionState == AdDecisionState.reverted.rawValue,
                    "\(id) must persist as .reverted; got \(row?.decisionState ?? "<missing>")")
        }
        for id in ["ad-suggest-A", "ad-suggest-E"] {
            let row = persisted.first { $0.id == id }
            #expect(row?.decisionState == AdDecisionState.candidate.rawValue,
                    "\(id) must remain .candidate; got \(row?.decisionState ?? "<missing>")")
        }
    }

    // Cycle 1 M2: the strong/weak routing split the R7 docstring deferred
    // has now landed in `SkipOrchestrator.revertByTimeRange`. The behavior
    // is now: a revert that touches ONLY the suggest-tier (markOnly) loop
    // — i.e. no managed auto-skip window was vetoed — routes through
    // `recordWeakFalseSkipSignal` (`weakFalseSignalPenalty`), reflecting
    // that no playback was ever altered. A managed-window revert (with
    // or without a co-occurring suggest revert) still uses the full
    // `falseSignalPenalty`. This test pins the suggest-only path at the
    // weak magnitude; the parallel test below pins the managed path at
    // the full magnitude. Any future re-merge of the two paths must
    // update both tests in the same diff.
    @Test("revertByTimeRange against ONLY a markOnly window decrements trust by weakFalseSignalPenalty")
    func revertByTimeRangeMarkOnlyDecrementsTrustByWeakPenalty() async throws {
        // Inline trust-service construction so we can read skipTrustScore
        // back from the SAME store the service mutates. The default
        // `makeSkipTestTrustService` helper allocates an internal store
        // we can't observe.
        let trustStore = try await makeTestStore()
        let initialTrust: Double = 0.90
        try await trustStore.upsertProfile(
            PodcastProfile(
                podcastId: "podcast-1",
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: initialTrust,
                observationCount: 10,
                mode: "auto",
                recentFalseSkipSignals: 0
            )
        )
        let trustConfig = TrustScoringConfig.default
        let trustService = TrustScoringService(store: trustStore, config: trustConfig)

        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let markOnly = AdWindow(
            id: "ad-suggest-magnitude",
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.55,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "test-1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 60,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([markOnly])

        // Sanity: only the suggest tier carries the entry; auto-skip
        // dict is empty.
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-magnitude"))
        #expect(!(await orchestrator.activeWindowIDs().contains("ad-suggest-magnitude")))

        // Veto.
        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: "podcast-1")

        // The trust hit fires inside an unstructured Task — poll until
        // the signal is observed.
        var profile: PodcastProfile?
        for _ in 0..<20 {
            profile = try await trustStore.fetchProfile(podcastId: "podcast-1")
            if let p = profile, p.recentFalseSkipSignals == 1 { break }
            try await Task.sleep(nanoseconds: 50_000_000)
        }

        // Cycle 1 M2: suggest-only revert now uses the WEAK penalty.
        let expectedTrust = initialTrust - trustConfig.weakFalseSignalPenalty
        #expect(abs((profile?.skipTrustScore ?? -1) - expectedTrust) < 1e-6,
                "suggest-tier-only revert must decrement trust by weakFalseSignalPenalty (\(trustConfig.weakFalseSignalPenalty)); got skipTrustScore=\(profile?.skipTrustScore ?? -1) expected=\(expectedTrust)")
        #expect(profile?.recentFalseSkipSignals == 1,
                "exactly one false-skip signal must be recorded for one veto gesture")
    }

    // R9 (hygc.1.8): managed-window symmetric trust-magnitude pin. The
    // R7 test above pins suggest-tier-only revert at the full
    // `falseSignalPenalty`. Without a parallel pin on the managed-window
    // path, a future weak/strong split could quietly land on ONLY one
    // surface — the symmetric pair makes any such split fail loudly on
    // both tests in the same diff. This test mirrors the suggest-tier
    // shape exactly except the AdWindow has `eligibilityGate = nil` (so
    // it lands in the managed `windows` dict, not `suggestWindows`) and
    // therefore the suggest-tier loop in `revertByTimeRange` is a no-op
    // on this fixture — only the managed loop fires.
    @Test("revertByTimeRange against ONLY a managed (auto-skip) window decrements trust by full falseSignalPenalty")
    func revertByTimeRangeManagedDecrementsTrustByFullPenalty() async throws {
        let trustStore = try await makeTestStore()
        let initialTrust: Double = 0.90
        try await trustStore.upsertProfile(
            PodcastProfile(
                podcastId: "podcast-1",
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: initialTrust,
                observationCount: 10,
                mode: "auto",
                recentFalseSkipSignals: 0
            )
        )
        let trustConfig = TrustScoringConfig.default
        let trustService = TrustScoringService(store: trustStore, config: trustConfig)

        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // `makeSkipTestAdWindow` defaults `eligibilityGate = nil` →
        // non-fusion producer → flows through to the managed `windows`
        // dict (not `suggestWindows`).
        let managed = makeSkipTestAdWindow(
            id: "ad-managed-magnitude",
            assetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.75,
            decisionState: AdDecisionState.candidate.rawValue
        )
        try await store.insertAdWindow(managed)
        await orchestrator.receiveAdWindows([managed])

        // Sanity: this window is in the auto-skip dict, NOT suggest tier.
        #expect(await orchestrator.activeWindowIDs().contains("ad-managed-magnitude"),
                "managed AdWindow must enter the auto-skip windows dict")
        #expect(!(await orchestrator.activeSuggestWindowIDs().contains("ad-managed-magnitude")),
                "managed AdWindow must NOT enter the suggest dict")

        // Veto.
        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: "podcast-1")

        // Trust hit fires inside an unstructured Task — poll.
        var profile: PodcastProfile?
        for _ in 0..<20 {
            profile = try await trustStore.fetchProfile(podcastId: "podcast-1")
            if let p = profile, p.recentFalseSkipSignals == 1 { break }
            try await Task.sleep(nanoseconds: 50_000_000)
        }

        let expectedTrust = initialTrust - trustConfig.falseSignalPenalty
        #expect(abs((profile?.skipTrustScore ?? -1) - expectedTrust) < 1e-6,
                "managed-window-only revert must decrement trust by the full falseSignalPenalty (\(trustConfig.falseSignalPenalty)); got skipTrustScore=\(profile?.skipTrustScore ?? -1) expected=\(expectedTrust)")
        #expect(profile?.recentFalseSkipSignals == 1,
                "exactly one false-skip signal must be recorded for one veto gesture")
    }

    // Cycle 2 M3: mixed-revert pin. R9 covers managed-only; the cycle-1
    // M2 split routes suggest-only to the weak penalty. The mixed case
    // — a single gesture overlapping BOTH a managed AdWindow AND a
    // markOnly AdWindow — must still take the full penalty (a managed
    // auto-skip was vetoed, regardless of whether a suggest banner was
    // ALSO touched in the same gesture). Without this pin, a future
    // re-order of the two loops in `revertByTimeRange` could
    // accidentally invert the `revertedManagedAny` flag and silently
    // demote mixed reverts to the weak penalty — escaping both the
    // suggest-only (R7) and managed-only (R9) pins.
    @Test("revertByTimeRange against BOTH a managed AND a suggest window decrements trust by full falseSignalPenalty")
    func revertByTimeRangeMixedDecrementsTrustByFullPenalty() async throws {
        let trustStore = try await makeTestStore()
        let initialTrust: Double = 0.90
        try await trustStore.upsertProfile(
            PodcastProfile(
                podcastId: "podcast-1",
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: initialTrust,
                observationCount: 10,
                mode: "auto",
                recentFalseSkipSignals: 0
            )
        )
        let trustConfig = TrustScoringConfig.default
        let trustService = TrustScoringService(store: trustStore, config: trustConfig)

        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let managed = makeSkipTestAdWindow(
            id: "ad-mixed-managed",
            assetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.75,
            decisionState: AdDecisionState.candidate.rawValue
        )
        let markOnly = AdWindow(
            id: "ad-mixed-suggest",
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.55,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "test-1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 60,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(managed)
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([managed, markOnly])

        // Sanity: managed enters the auto-skip dict, suggest enters the
        // suggest dict. The veto gesture below overlaps both.
        #expect(await orchestrator.activeWindowIDs().contains("ad-mixed-managed"))
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-mixed-suggest"))

        await orchestrator.revertByTimeRange(start: 70, end: 110, podcastId: "podcast-1")

        var profile: PodcastProfile?
        for _ in 0..<20 {
            profile = try await trustStore.fetchProfile(podcastId: "podcast-1")
            if let p = profile, p.recentFalseSkipSignals == 1 { break }
            try await Task.sleep(nanoseconds: 50_000_000)
        }

        // Mixed revert must take the FULL penalty (managed was vetoed).
        let expectedTrust = initialTrust - trustConfig.falseSignalPenalty
        #expect(abs((profile?.skipTrustScore ?? -1) - expectedTrust) < 1e-6,
                "mixed revert (managed + suggest co-occurring) must decrement by full falseSignalPenalty (\(trustConfig.falseSignalPenalty)), NOT weakFalseSignalPenalty (\(trustConfig.weakFalseSignalPenalty)); got skipTrustScore=\(profile?.skipTrustScore ?? -1) expected=\(expectedTrust)")
        #expect(profile?.recentFalseSkipSignals == 1,
                "exactly one false-skip signal must be recorded for one veto gesture")
    }

    // R10 (hygc.1.8): symmetric "no overlap → no trust decrement" pin.
    // R7 pinned the suggest-tier full-magnitude decrement, R9 pinned the
    // managed-window full-magnitude decrement. Both fire on `revertedAny`.
    // Without this third pin, a future regression that lifts the
    // `if revertedAny` guard (e.g. by accident moving `recordFalseSkip
    // Signal` outside the conditional) would silently fire the trust
    // signal even on a no-op revert gesture — quietly poisoning a
    // podcast's trust score whenever the user taps "this isn't an ad"
    // on a region where no window exists. The two existing
    // full-magnitude pins do NOT catch this regression because they
    // both have an overlap. Pin the zero-magnitude case so the symmetric
    // pair is loud in both directions.
    @Test("revertByTimeRange with no overlapping windows does NOT decrement trust")
    func revertByTimeRangeNoOverlapDoesNotDecrementTrust() async throws {
        let trustStore = try await makeTestStore()
        let initialTrust: Double = 0.90
        try await trustStore.upsertProfile(
            PodcastProfile(
                podcastId: "podcast-1",
                sponsorLexicon: nil,
                normalizedAdSlotPriors: nil,
                repeatedCTAFragments: nil,
                jingleFingerprints: nil,
                implicitFalsePositiveCount: 0,
                skipTrustScore: initialTrust,
                observationCount: 10,
                mode: "auto",
                recentFalseSkipSignals: 0
            )
        )
        let trustConfig = TrustScoringConfig.default
        let trustService = TrustScoringService(store: trustStore, config: trustConfig)

        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Seed both surfaces with windows OUTSIDE the upcoming gesture range
        // so we exercise both loops without producing a match.
        let managed = makeSkipTestAdWindow(
            id: "ad-managed-far",
            assetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: AdDecisionState.candidate.rawValue
        )
        let markOnly = AdWindow(
            id: "ad-suggest-far",
            analysisAssetId: "asset-1",
            startTime: 200,
            endTime: 260,
            confidence: 0.55,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "test-1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: 200,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue
        )
        try await store.insertAdWindow(managed)
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([managed, markOnly])

        // Veto a region that overlaps NEITHER surface.
        await orchestrator.revertByTimeRange(start: 800, end: 900, podcastId: "podcast-1")

        // Give any in-flight unstructured task a fair chance to write.
        // (We expect no write, but we want to give the no-op enough time
        // to let any erroneous trust write reach the store.)
        for _ in 0..<5 {
            try await Task.sleep(nanoseconds: 50_000_000)
        }

        let profile = try await trustStore.fetchProfile(podcastId: "podcast-1")
        #expect(abs((profile?.skipTrustScore ?? -1) - initialTrust) < 1e-6,
                "no-overlap revert must NOT decrement trust; got skipTrustScore=\(profile?.skipTrustScore ?? -1) expected=\(initialTrust)")
        #expect(profile?.recentFalseSkipSignals == 0,
                "no false-skip signal must be recorded when no windows overlap; got \(profile?.recentFalseSkipSignals ?? -1)")

        // And neither persisted window must be in `.reverted` state — the
        // managed window may legitimately advance to `.applied` via the
        // auto-skip lifecycle, but neither should reach `.reverted`
        // because the gesture overlapped neither.
        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        let managedRow = persisted.first { $0.id == "ad-managed-far" }
        let suggestRow = persisted.first { $0.id == "ad-suggest-far" }
        #expect(managedRow?.decisionState != AdDecisionState.reverted.rawValue,
                "managed window outside the gesture must NOT be reverted; got \(managedRow?.decisionState ?? "<missing>")")
        #expect(suggestRow?.decisionState != AdDecisionState.reverted.rawValue,
                "markOnly window outside the gesture must NOT be reverted; got \(suggestRow?.decisionState ?? "<missing>")")
    }

    // MARK: - playhead-lc7z: explicit suggest denial persists a falsePositive correction

    /// Build a markOnly suggest-tier AdWindow with a brand + producer tag so
    /// the explicit-denial path has something to attribute (causalSource) and
    /// something to key hard-negative mining on (sponsorEntity).
    private func makeSuggestWindow(
        id: String,
        analysisAssetId: String = "asset-1",
        startTime: Double = 60,
        endTime: Double = 120,
        advertiser: String? = "Red Bull",
        metadataSource: String = SpecialistMarkComposer.metadataSource,
        confidence: Double = 0.55,
        decisionState: String = AdDecisionState.candidate.rawValue,
        eligibilityGate: String = SkipEligibilityGate.markOnly.rawValue
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: analysisAssetId,
            startTime: startTime,
            endTime: endTime,
            confidence: confidence,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: decisionState,
            detectorVersion: "test-1",
            advertiser: advertiser, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: startTime,
            metadataSource: metadataSource,
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: eligibilityGate
        )
    }

    @Test("gate flip retires suggestion before replacement presentation")
    func gateFlipOrdersSuggestionRetirementBeforeReplacement() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 17
        )
        await orchestrator.setActiveSkipMode(.auto)
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)

        let suggest = makeSuggestWindow(id: "gate-flip-window")
        await orchestrator.receiveAdWindows([suggest])
        guard case let .present(suggestItem) = await probe.next() else {
            Issue.record("Expected initial suggest presentation")
            return
        }
        #expect(suggestItem.tier == .suggest)

        let eligible = makeSuggestWindow(
            id: suggest.id,
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([eligible])

        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record("Gate flip must retire the stale suggestion first")
            return
        }
        #expect(retirement.windowId == suggest.id)
        #expect(retirement.episodeId == "episode-1")
        #expect(retirement.playbackLifecycleGeneration == 17)

        guard case let .present(replacement) = await probe.next() else {
            Issue.record("Expected replacement auto-tier presentation")
            return
        }
        #expect(replacement.windowId == suggest.id)
        #expect(replacement.tier == .autoSkipped)
    }

    @Test("blocked and inventory-rejected AdWindows retire stale suggestions")
    func adWindowInvalidationsRetireSuggestionsAndRejectStaleYes() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(
            store: store,
            inventoryFilter: InventorySanityFilter(isEnabled: true)
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 71
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)

        let blockedSuggestion = makeSuggestWindow(
            id: "ad-window-blocked-after-suggest"
        )
        await orchestrator.receiveAdWindows([blockedSuggestion])
        guard case .present = await probe.next() else {
            Issue.record("Expected blocked-path suggestion presentation")
            return
        }
        let blockedUpdate = makeSuggestWindow(
            id: blockedSuggestion.id,
            confidence: 0.99,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate:
                SkipEligibilityGate.blockedByPolicy.rawValue
        )
        await orchestrator.receiveAdWindows([blockedUpdate])
        guard case let .retireWindow(blockedRetirement) =
            await probe.next()
        else {
            Issue.record("Blocked AdWindow must retire its stale suggestion")
            return
        }
        #expect(blockedRetirement.windowId == blockedSuggestion.id)

        let rejectedSuggestion = makeSuggestWindow(
            id: "ad-window-rejected-after-suggest"
        )
        await orchestrator.receiveAdWindows([rejectedSuggestion])
        guard case .present = await probe.next() else {
            Issue.record("Expected inventory-path suggestion presentation")
            return
        }
        let rejectedUpdate = makeSuggestWindow(
            id: rejectedSuggestion.id,
            startTime: 60,
            endTime: 61,
            confidence: 0.99,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([rejectedUpdate])
        guard case let .retireWindow(rejectedRetirement) =
            await probe.next()
        else {
            Issue.record(
                "Inventory-rejected AdWindow must retire its stale suggestion"
            )
            return
        }
        #expect(rejectedRetirement.windowId == rejectedSuggestion.id)

        await orchestrator.acceptSuggestedSkip(
            windowId: blockedSuggestion.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 71
        )
        await orchestrator.acceptSuggestedSkip(
            windowId: rejectedSuggestion.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 71
        )

        let activeSuggestions =
            await orchestrator.activeSuggestWindowIDs()
        #expect(!activeSuggestions.contains(blockedSuggestion.id))
        #expect(!activeSuggestions.contains(rejectedSuggestion.id))
        #expect((await orchestrator.activeWindowIDs()).isEmpty)
        #expect(
            pushedCues.isEmpty,
            "A stale Yes must not promote a blocked or rejected AdWindow"
        )
    }

    @Test("blocked and inventory-rejected decisions retire stale suggestions")
    func decisionInvalidationsRetireSuggestionsAndRejectStaleYes() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(
            store: store,
            inventoryFilter: InventorySanityFilter(isEnabled: true)
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 72
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)

        let blockedSuggestion = makeSuggestWindow(
            id: "decision-blocked-after-suggest"
        )
        await orchestrator.receiveAdWindows([blockedSuggestion])
        guard case .present = await probe.next() else {
            Issue.record("Expected blocked-decision suggestion presentation")
            return
        }
        await orchestrator.receiveAdDecisionResults([
            AdDecisionResult(
                id: blockedSuggestion.id,
                analysisAssetId: "asset-1",
                startTime: 60,
                endTime: 120,
                skipConfidence: 0.99,
                eligibilityGate: .blocked,
                recomputationRevision: 2
            )
        ])
        guard case let .retireWindow(blockedRetirement) =
            await probe.next()
        else {
            Issue.record(
                "Blocked AdDecisionResult must retire its stale suggestion"
            )
            return
        }
        #expect(blockedRetirement.windowId == blockedSuggestion.id)

        let rejectedSuggestion = makeSuggestWindow(
            id: "decision-rejected-after-suggest"
        )
        await orchestrator.receiveAdWindows([rejectedSuggestion])
        guard case .present = await probe.next() else {
            Issue.record("Expected rejected-decision suggestion presentation")
            return
        }
        await orchestrator.receiveAdDecisionResults([
            AdDecisionResult(
                id: rejectedSuggestion.id,
                analysisAssetId: "asset-1",
                startTime: 60,
                endTime: 61,
                skipConfidence: 0.99,
                eligibilityGate: .eligible,
                recomputationRevision: 2
            )
        ])
        guard case let .retireWindow(rejectedRetirement) =
            await probe.next()
        else {
            Issue.record(
                "Inventory-rejected AdDecisionResult must retire its stale suggestion"
            )
            return
        }
        #expect(rejectedRetirement.windowId == rejectedSuggestion.id)

        await orchestrator.acceptSuggestedSkip(
            windowId: blockedSuggestion.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 72
        )
        await orchestrator.acceptSuggestedSkip(
            windowId: rejectedSuggestion.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 72
        )

        let activeSuggestions =
            await orchestrator.activeSuggestWindowIDs()
        #expect(!activeSuggestions.contains(blockedSuggestion.id))
        #expect(!activeSuggestions.contains(rejectedSuggestion.id))
        #expect((await orchestrator.activeWindowIDs()).isEmpty)
        #expect(
            pushedCues.isEmpty,
            "A stale Yes must not promote a blocked or rejected decision"
        )
    }

    @Test("explicit window retirement also invalidates a suggestion")
    func explicitRetirementRejectsStaleSuggestionYes() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 73
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let suggestion = makeSuggestWindow(
            id: "explicitly-retired-suggestion"
        )
        await orchestrator.receiveAdWindows([suggestion])
        guard case .present = await probe.next() else {
            Issue.record("Expected explicit-retirement presentation")
            return
        }

        await orchestrator.retireAdWindows(ids: [suggestion.id])
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record(
                "retireAdWindows must retire a matching suggestion"
            )
            return
        }
        #expect(retirement.windowId == suggestion.id)

        await orchestrator.acceptSuggestedSkip(
            windowId: suggestion.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 73
        )

        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(suggestion.id)
        )
        #expect((await orchestrator.activeWindowIDs()).isEmpty)
        #expect(
            pushedCues.isEmpty,
            "A stale Yes after explicit retirement must not install a cue"
        )

        let managed = makeSuggestWindow(
            id: "explicitly-retired-managed",
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.setActiveSkipMode(.auto)
        await orchestrator.receiveAdWindows([managed])
        guard case let .present(managedItem) = await probe.next() else {
            Issue.record("Expected explicit-retirement managed presentation")
            return
        }
        #expect(managedItem.tier == .autoSkipped)

        await orchestrator.retireAdWindows(ids: [managed.id])
        guard case let .retireWindow(managedRetirement) =
            await probe.next()
        else {
            Issue.record(
                "retireAdWindows must retire a matching managed card"
            )
            return
        }
        #expect(managedRetirement.windowId == managed.id)
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(managed.id)
        )
    }

    @Test("late inventory context retires a newly-invalid suggestion")
    func lateInventoryContextRejectsStaleSuggestionYes() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(
            store: store,
            inventoryFilter: InventorySanityFilter(isEnabled: true)
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 74
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let suggestion = makeSuggestWindow(
            id: "late-inventory-rejected-suggestion"
        )
        await orchestrator.receiveAdWindows([suggestion])
        guard case .present = await probe.next() else {
            Issue.record("Expected pre-context suggestion presentation")
            return
        }

        // Publisher chapters arrive after episode hydration in production.
        // The span was valid with no chapter context, but becomes invalid as
        // soon as the declared editorial chapter overlaps it.
        await orchestrator.setDeclaredChapters(
            [
                ChapterEvidence(
                    startTime: 50,
                    endTime: 130,
                    title: "Editorial interview",
                    source: .rssInline,
                    disposition: .content,
                    qualityScore: 1
                )
            ],
            analysisAssetId: "asset-1"
        )
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record(
                "Late inventory context must retire a newly-invalid suggestion"
            )
            return
        }
        #expect(retirement.windowId == suggestion.id)

        await orchestrator.acceptSuggestedSkip(
            windowId: suggestion.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 74
        )

        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(suggestion.id)
        )
        #expect((await orchestrator.activeWindowIDs()).isEmpty)
        #expect(
            pushedCues.isEmpty,
            "A stale Yes after late inventory rejection must not install a cue"
        )
    }

    @Test("late inventory context retires a managed card and cue")
    func lateInventoryContextRetiresManagedCardAndCue() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            inventoryFilter: InventorySanityFilter(isEnabled: true)
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 78
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let managed = makeSuggestWindow(
            id: "late-inventory-rejected-managed",
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([managed])
        guard case .present = await probe.next() else {
            Issue.record("Expected pre-context managed presentation")
            return
        }
        #expect(!pushedCues.isEmpty)

        await orchestrator.setDeclaredChapters(
            [
                ChapterEvidence(
                    startTime: 50,
                    endTime: 130,
                    title: "Editorial interview",
                    source: .rssInline,
                    disposition: .content,
                    qualityScore: 1
                )
            ],
            analysisAssetId: "asset-1"
        )
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record(
                "Late inventory context must retire a managed card"
            )
            return
        }
        #expect(retirement.windowId == managed.id)
        #expect(pushedCues.isEmpty)
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(managed.id)
        )
    }

    @Test("AdWindow applied to markOnly downgrade disarms cue and surfaces suggestion")
    func adWindowAppliedToMarkOnlyDowngradeDisarmsCue() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 75
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let eligible = makeSuggestWindow(
            id: "applied-to-mark-only",
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([eligible])
        guard case let .present(autoItem) = await probe.next() else {
            Issue.record("Expected initial auto-tier presentation")
            return
        }
        #expect(autoItem.tier == .autoSkipped)
        #expect(!pushedCues.isEmpty)
        #expect(
            (await orchestrator.activeWindowIDs()).contains(eligible.id)
        )

        let markOnly = makeSuggestWindow(id: eligible.id)
        await orchestrator.receiveAdWindows([markOnly])
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record("markOnly downgrade must retire the auto card")
            return
        }
        #expect(retirement.windowId == eligible.id)
        guard case let .present(suggestItem) = await probe.next() else {
            Issue.record("markOnly downgrade must surface a suggestion")
            return
        }
        #expect(suggestItem.tier == .suggest)
        #expect(suggestItem.windowId == eligible.id)
        #expect(pushedCues.isEmpty)
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(eligible.id)
        )
        #expect(
            (await orchestrator.activeSuggestWindowIDs())
                .contains(eligible.id)
        )
    }

    @Test("AdWindow applied to blocked downgrade disarms cue without replacement")
    func adWindowAppliedToBlockedDowngradeDisarmsCue() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 76
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let eligible = makeSuggestWindow(
            id: "applied-to-blocked",
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([eligible])
        guard case .present = await probe.next() else {
            Issue.record("Expected initial auto-tier presentation")
            return
        }
        #expect(!pushedCues.isEmpty)

        let blocked = makeSuggestWindow(
            id: eligible.id,
            confidence: 0.99,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate:
                SkipEligibilityGate.blockedByPolicy.rawValue
        )
        await orchestrator.receiveAdWindows([blocked])
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record("Blocked downgrade must retire the auto card")
            return
        }
        #expect(retirement.windowId == eligible.id)
        #expect(pushedCues.isEmpty)
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(eligible.id)
        )
        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(eligible.id)
        )
    }

    @Test("blocked decision disarms an already-applied window")
    func decisionAppliedToBlockedDowngradeDisarmsCue() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 77
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let eligible = AdDecisionResult(
            id: "decision-applied-to-blocked",
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            skipConfidence: 0.9,
            eligibilityGate: .eligible,
            recomputationRevision: 1
        )
        await orchestrator.receiveAdDecisionResults([eligible])
        guard case .present = await probe.next() else {
            Issue.record("Expected initial decision auto-tier presentation")
            return
        }
        #expect(!pushedCues.isEmpty)

        let blocked = AdDecisionResult(
            id: eligible.id,
            analysisAssetId: eligible.analysisAssetId,
            startTime: eligible.startTime,
            endTime: eligible.endTime,
            skipConfidence: 0.99,
            eligibilityGate: .blocked,
            recomputationRevision: 2
        )
        await orchestrator.receiveAdDecisionResults([blocked])
        guard case let .retireWindow(retirement) =
            await probe.next()
        else {
            Issue.record("Blocked decision must retire the auto card")
            return
        }
        #expect(retirement.windowId == eligible.id)
        #expect(pushedCues.isEmpty)
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(eligible.id)
        )
        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(eligible.id)
        )
    }

    @Test("suggest Yes applies a cue in shadow mode without a second banner")
    func acceptingSuggestionAppliesExplicitSkipWithoutDuplicateBanner() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 18
        )
        #expect(await orchestrator.currentSkipMode() == .shadow)

        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let suggest = makeSuggestWindow(id: "accept-shadow-window")
        try await store.insertAdWindow(suggest)
        await orchestrator.receiveAdWindows([suggest])
        guard case let .present(item) = await probe.next() else {
            Issue.record("Expected suggest presentation")
            return
        }
        await orchestrator.acknowledgeSuggestedBannerDelivery(
            windowId: item.windowId,
            episodeId: item.episodeId,
            playbackLifecycleGeneration:
                item.playbackLifecycleGeneration
        )

        await orchestrator.acceptSuggestedSkip(
            windowId: item.windowId,
            ifCurrentEpisodeId: item.episodeId,
            ifPlaybackLifecycleGeneration:
                item.playbackLifecycleGeneration
        )

        #expect(!pushedCues.isEmpty,
                "Explicit Yes must skip even when automatic mode is shadow")
        #expect(
            (await orchestrator.emittedAutoSkipBannersSnapshot()).isEmpty,
            "The accepted suggest card already supplied the feedback presentation"
        )
        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(
            persisted.first { $0.id == suggest.id }?.decisionState
                == AdDecisionState.suppressed.rawValue,
            "The original markOnly row must not resurrect after replay"
        )
        let promotedRows = persisted.filter { $0.id != suggest.id }
        #expect(promotedRows.count == 1)
        #expect(promotedRows.first?.decisionState
                == AdDecisionState.applied.rawValue)
        #expect(promotedRows.first?.wasSkipped == true)

        let revisedMarkOnly = makeSuggestWindow(
            id: suggest.id,
            startTime: 75,
            endTime: 135
        )
        await orchestrator.receiveAdWindows([revisedMarkOnly])
        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(suggest.id),
            "A same-ID markOnly revision after Yes must not create another suggestion"
        )
        #expect(
            await probe.next() == nil,
            "A same-ID markOnly revision after Yes must not emit another card"
        )
    }

    @Test("accepted suggestion IDs remain terminal for the whole lifecycle")
    func acceptedSuggestionTerminalSetDoesNotEvictOlderIDs() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 180
        )

        let firstID = "accepted-terminal-0"
        for index in 0...256 {
            let suggestion = makeSuggestWindow(
                id: "accepted-terminal-\(index)"
            )
            try await store.insertAdWindow(suggestion)
            await orchestrator.receiveAdWindows([suggestion])
            #expect(
                await orchestrator.acceptSuggestedSkip(
                    windowId: suggestion.id,
                    ifCurrentEpisodeId: "episode-1",
                    ifPlaybackLifecycleGeneration: 180
                ),
                "Every distinct suggestion should be accepted exactly once"
            )
        }

        let promotedCountBeforeReplay = try await store.fetchAdWindows(
            assetId: "asset-1"
        ).filter {
            $0.id != firstID && $0.wasSkipped
        }.count

        // The former 256-entry LRU evicted `firstID` here, allowing this late
        // producer replay to register and mint a second promoted UUID.
        let replayEvents = await orchestrator.bannerEventStream()
        let replayProbe = BoundedStreamProbe(replayEvents)
        let lateReplay = makeSuggestWindow(id: firstID)
        await orchestrator.receiveAdWindows([lateReplay])

        #expect(
            await replayProbe.next() == nil,
            "A terminal accepted ID must not receive a second presentation"
        )
        #expect(
            !(await orchestrator.activeSuggestWindowIDs()).contains(firstID),
            "An accepted producer ID must stay terminal until lifecycle end"
        )
        #expect(
            !(await orchestrator.acceptSuggestedSkip(
                windowId: firstID,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 180
            )),
            "A late replay must not create a second durable result"
        )
        let promotedCountAfterReplay = try await store.fetchAdWindows(
            assetId: "asset-1"
        ).filter {
            $0.id != firstID && $0.wasSkipped
        }.count
        #expect(promotedCountAfterReplay == promotedCountBeforeReplay)
    }

    @Test("suggest Yes persistence failure preserves the card and cue for retry")
    func acceptingSuggestionRequiresDurablePersistenceBeforeApplying() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 181
        )

        // The in-memory producer can deliver before its row becomes visible to
        // this store. That missing authoritative row forces the acceptance
        // transaction to fail deterministically.
        let suggest = makeSuggestWindow(id: "accept-persistence-retry")
        await orchestrator.receiveAdWindows([suggest])
        let decisionCountBeforeFeedback =
            await orchestrator.getDecisionLog().count

        let firstAccepted = await orchestrator.acceptSuggestedSkip(
            windowId: suggest.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 181
        )

        #expect(!firstAccepted)
        #expect(pushedCues.isEmpty)
        #expect(
            await orchestrator.activeSuggestWindowIDs().contains(suggest.id),
            "A failed durable write must leave the same suggestion retryable"
        )
        #expect(
            (try await store.fetchAdWindows(assetId: "asset-1")).isEmpty,
            "A failed transaction must not leave a promoted row behind"
        )
        #expect(
            try await correctionStore.activeCorrections(for: "asset-1")
                .isEmpty,
            "A failed acceptance must not leave a premature correction"
        )

        try await store.insertAdWindow(suggest)
        let retryAccepted = await orchestrator.acceptSuggestedSkip(
            windowId: suggest.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 181
        )

        #expect(retryAccepted)
        #expect(!pushedCues.isEmpty)
        #expect(
            !(await orchestrator.activeSuggestWindowIDs().contains(suggest.id))
        )
        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(
            persisted.first { $0.id == suggest.id }?.decisionState
                == AdDecisionState.suppressed.rawValue
        )
        #expect(
            persisted.contains {
                $0.id != suggest.id
                    && $0.decisionState == AdDecisionState.applied.rawValue
                    && $0.wasSkipped
                }
        )
        let corrections = try await correctionStore.activeCorrections(
            for: "asset-1"
        )
        #expect(corrections.count == 1)
        let correction = try #require(corrections.first)
        #expect(correction.source == .bannerSuggestionConfirmed)
        let promotedWindowId = try #require(
            persisted.first {
                $0.id != suggest.id
                    && $0.decisionState == AdDecisionState.applied.rawValue
            }?.id
        )
        #expect(correction.targetRefs?.adWindowId == promotedWindowId)
        #expect(
            await orchestrator.getDecisionLog().count
                == decisionCountBeforeFeedback,
            "Suggest Yes must not add a detailed decision-log record"
        )
        #expect(
            corrections.first?.submissionCount == 1,
            "The failed attempt must not count as a durable submission"
        )
    }

    @Test(
        "suggest Yes persists episode A feedback across direct replacement",
        .timeLimit(.minutes(1))
    )
    func suggestYesPersistsSourceFeedbackAcrossEpisodeReplacement() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-1",
                episodeId: "episode-1"
            )
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-2",
                episodeId: "episode-2"
            )
        )
        let persistentCorrections = PersistentUserCorrectionStore(
            store: store
        )
        let correctionGate = ControlledAsyncGate()
        let falseNegativeGate = ControlledAsyncGate()
        let gatedCorrections = GatedUserCorrectionStore(
            wrapped: persistentCorrections,
            recordGate: correctionGate
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: gatedCorrections
        )
        await orchestrator._setFalseNegativeSignalHandlerForTesting { _ in
            await falseNegativeGate.wait()
        }
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 311
        )

        let suggestion = makeSuggestWindow(
            id: "suggest-yes-during-replacement"
        )
        try await store.insertAdWindow(suggestion)
        await orchestrator.receiveAdWindows([suggestion])

        let primaryResultGate = ControlledAsyncGate()
        let response = Task {
            let result = await orchestrator.acceptSuggestedSkip(
                windowId: suggestion.id,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 311
            )
            await primaryResultGate.wait()
            return result
        }
        await correctionGate.waitUntilStarted()
        await falseNegativeGate.waitUntilStarted()
        await primaryResultGate.waitUntilStarted()
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2",
            episodeId: "episode-2",
            podcastId: "podcast-2",
            playbackLifecycleGeneration: 312
        )
        await correctionGate.release()
        await falseNegativeGate.release()
        await primaryResultGate.release()

        #expect(await response.value)
        let corrections = try await persistentCorrections.activeCorrections(
            for: "asset-1"
        )
        #expect(corrections.count == 1)
        #expect(corrections.first?.correctionType == .falseNegative)
        let sourceRows = try await store.fetchAdWindows(assetId: "asset-1")
        #expect(
            sourceRows.first { $0.id == suggestion.id }?.decisionState
                == AdDecisionState.suppressed.rawValue
        )
        #expect(
            sourceRows.contains {
                $0.id != suggestion.id
                    && $0.wasSkipped
                    && $0.decisionState
                        == AdDecisionState.applied.rawValue
            }
        )
        #expect((await orchestrator.activeWindowIDs()).isEmpty)
    }

    @Test(
        "failed suggest Yes restores the latest revision received while persistence waits",
        .timeLimit(.minutes(1))
    )
    func failedSuggestYesRestoresLatestBufferedRevision() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        let persistenceGate = ControlledAsyncGate()
        await orchestrator._setSuggestPersistenceBarrierForTesting {
            await persistenceGate.wait()
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1"
        )

        let original = makeSuggestWindow(
            id: "yes-buffered-revision",
            startTime: 60,
            endTime: 120
        )
        let latest = makeSuggestWindow(
            id: original.id,
            startTime: 70,
            endTime: 135
        )
        try await store.insertAdWindow(original)
        await orchestrator.receiveAdWindows([original])
        let responseTask = Task {
            await orchestrator.acceptSuggestedSkip(
                windowId: original.id,
                ifCurrentEpisodeId: "episode-1"
            )
        }
        await persistenceGate.waitUntilStarted()
        try await store.insertOrReplaceAdWindow(latest)
        await orchestrator.receiveAdWindows([latest])
        await persistenceGate.release()

        #expect(!(await responseTask.value))
        let restored = await orchestrator._suggestWindowForTesting(
            id: original.id
        )
        #expect(restored?.startTime == latest.startTime)
        #expect(restored?.endTime == latest.endTime)
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: original.analysisAssetId
            ).isEmpty
        )
        #expect(
            try await store.fetchAdWindow(id: latest.id)?
                .decisionState == AdDecisionState.candidate.rawValue
        )

        await orchestrator._setFeedbackPersistenceBarrierForTesting(nil)
        #expect(
            await orchestrator.acceptSuggestedSkip(
                windowId: latest.id,
                ifCurrentEpisodeId: "episode-1"
            ),
            "The replacement revision must remain actionable"
        )
        let rows = try await store.fetchAdWindows(
            assetId: latest.analysisAssetId
        )
        #expect(
            rows.contains {
                $0.id != latest.id
                    && $0.startTime == latest.startTime
                    && $0.endTime == latest.endTime
                    && $0.wasSkipped
            }
        )
    }

    @Test(
        "failed suggest No restores the latest revision received while persistence waits",
        .timeLimit(.minutes(1))
    )
    func failedSuggestNoRestoresLatestBufferedRevision() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        let persistenceGate = ControlledAsyncGate()
        await orchestrator._setSuggestPersistenceBarrierForTesting {
            await persistenceGate.wait()
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1"
        )

        let original = makeSuggestWindow(
            id: "no-buffered-revision",
            startTime: 60,
            endTime: 120
        )
        let latest = makeSuggestWindow(
            id: original.id,
            startTime: 75,
            endTime: 140
        )
        try await store.insertAdWindow(original)
        await orchestrator.receiveAdWindows([original])
        let responseTask = Task {
            await orchestrator.declineSuggestedSkip(
                windowId: original.id,
                isExplicitDenial: true,
                ifCurrentEpisodeId: "episode-1"
            )
        }
        await persistenceGate.waitUntilStarted()
        try await store.insertOrReplaceAdWindow(latest)
        await orchestrator.receiveAdWindows([latest])
        await persistenceGate.release()

        #expect(!(await responseTask.value))
        let restored = await orchestrator._suggestWindowForTesting(
            id: original.id
        )
        #expect(restored?.startTime == latest.startTime)
        #expect(restored?.endTime == latest.endTime)
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: original.analysisAssetId
            ).isEmpty
        )
        #expect(
            try await store.fetchAdWindow(id: latest.id)?
                .decisionState == AdDecisionState.candidate.rawValue
        )

        await orchestrator._setFeedbackPersistenceBarrierForTesting(nil)
        #expect(
            await orchestrator.declineSuggestedSkip(
                windowId: latest.id,
                isExplicitDenial: true,
                ifCurrentEpisodeId: "episode-1"
            ),
            "The replacement revision must remain actionable"
        )
        let persistedLatest = try #require(
            try await store.fetchAdWindow(id: latest.id)
        )
        #expect(
            persistedLatest.decisionState
                == AdDecisionState.reverted.rawValue
        )
        #expect(persistedLatest.userDismissedBanner)
    }

    @Test(
        "stale Auto-Yes cannot commit after durable material changes mid-suspension",
        .timeLimit(.minutes(1))
    )
    func staleAutoYesCannotCommitAfterMaterialReplacement()
        async throws
    {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 205
        )
        let stream = await orchestrator.bannerEventStream()
        let probe = BoundedStreamProbe(stream)
        let original = makeSkipTestAdWindow(
            id: "auto-yes-mid-suspension-revision",
            startTime: 60,
            endTime: 120,
            confidence: 0.91,
            decisionState: AdDecisionState.applied.rawValue
        )
        try await store.insertAdWindow(original)
        await orchestrator.receiveAdWindows([original])
        guard case let .present(originalItem) = await probe.next() else {
            Issue.record("Expected original Auto-Yes card")
            return
        }

        let persistenceGate = ControlledAsyncGate()
        await orchestrator._setFeedbackPersistenceBarrierForTesting {
            await persistenceGate.wait()
        }
        let staleResponse = Task {
            await orchestrator.confirmAutoSkippedBanner(
                windowId: originalItem.windowId,
                analysisAssetId: originalItem.analysisAssetId,
                startTime: originalItem.adStartTime,
                endTime: originalItem.adEndTime,
                ifCurrentEpisodeId: originalItem.episodeId,
                ifPlaybackLifecycleGeneration:
                    originalItem.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    originalItem.windowMaterialRevisionToken
            )
        }
        await persistenceGate.waitUntilStarted()

        let latest = makeSkipTestAdWindow(
            id: original.id,
            startTime: 72,
            endTime: 138,
            confidence: 0.95,
            decisionState: AdDecisionState.applied.rawValue
        )
        try await store.insertOrReplaceAdWindow(latest)
        await orchestrator.receiveAdWindows([latest])
        guard case .retireWindow = await probe.next() else {
            Issue.record("Expected stale card retirement")
            return
        }
        guard case let .present(latestItem) = await probe.next() else {
            Issue.record("Expected replacement Auto-Yes card")
            return
        }
        await persistenceGate.release()

        #expect(!(await staleResponse.value))
        #expect(
            try await store.loadCorrectionEvents(
                analysisAssetId: original.analysisAssetId
            ).isEmpty,
            "The stale receipt must not commit"
        )
        let durableLatest = try #require(
            try await store.fetchAdWindow(id: latest.id)
        )
        #expect(durableLatest.startTime == latest.startTime)
        #expect(durableLatest.endTime == latest.endTime)

        await orchestrator._setFeedbackPersistenceBarrierForTesting(nil)
        #expect(
            await orchestrator.confirmAutoSkippedBanner(
                windowId: latestItem.windowId,
                analysisAssetId: latestItem.analysisAssetId,
                startTime: latestItem.adStartTime,
                endTime: latestItem.adEndTime,
                ifCurrentEpisodeId: latestItem.episodeId,
                ifPlaybackLifecycleGeneration:
                    latestItem.playbackLifecycleGeneration,
                ifWindowMaterialRevisionToken:
                    latestItem.windowMaterialRevisionToken
            ),
            "The latest durable revision must remain actionable"
        )
        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: latest.analysisAssetId
        )
        #expect(receipts.count == 1)
        #expect(
            CorrectionScope.deserialize(receipts[0].scope)
                == .exactTimeSpan(
                    assetId: latest.analysisAssetId,
                    startTime: latest.startTime,
                    endTime: latest.endTime
                )
        )
    }

    @Test(
        "explicit receipt drives trust while replay, Corpus, Debug, and sharing remain response-independent",
        .timeLimit(.minutes(1))
    )
    func explicitReceiptEndToEndPrivacyFlow() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-r26-e2e"
        let episodeId = "episode-r26-e2e"
        try await store.insertAsset(
            AnalysisAsset(
                id: assetId,
                episodeId: episodeId,
                assetFingerprint:
                    "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                weakFingerprint: nil,
                sourceURL: "file:///test/r26-e2e.m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: 300
            )
        )
        let suggestion = makeSuggestWindow(
            id: "r26-e2e-suggestion",
            analysisAssetId: assetId,
            startTime: 60,
            endTime: 120
        )
        let unrelated = makeSkipTestAdWindow(
            id: "r26-e2e-unrelated",
            assetId: assetId,
            startTime: 180,
            endTime: 225,
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        try await store.insertAdWindow(suggestion)
        try await store.insertAdWindow(unrelated)

        let exportedAt = Date(timeIntervalSince1970: 1_800_000_000)
        let baselineDocs = try makeTempDir(prefix: "r26-e2e-before")
        let baselineCorpus = try await CorpusExporter.export(
            store: store,
            documentsURL: baselineDocs,
            now: exportedAt,
            dedupMemo: CorpusExportDedupMemo()
        )
        let baselineCorpusBytes = try Data(
            contentsOf: baselineCorpus.fileURL
        )
        let baselineDebug = try #require(
            await DebugEpisodeExportService.build(
                episodeTitle: "R26 E2E",
                podcastTitle: "R26 Podcast",
                analysisAssetId: assetId,
                episodeId: episodeId,
                store: store,
                exportedAt: exportedAt
            )
        )
        let baselineShare = try await store
            .exportCrossUserAnalysisSnapshot(
                assetId: assetId,
                podcastId: "podcast-r26-e2e",
                exportedAt: exportedAt,
                sourceAppBuild: "r26-e2e"
            )

        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        let trustDispatch = TestEventCounter()
        await orchestrator._setFalseNegativeSignalHandlerForTesting { _ in
            trustDispatch.increment()
        }
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: "podcast-r26-e2e",
            playbackLifecycleGeneration: 206
        )
        await orchestrator.receiveAdWindows([suggestion])
        #expect(
            await orchestrator.acceptSuggestedSkip(
                windowId: suggestion.id,
                ifCurrentEpisodeId: episodeId,
                ifPlaybackLifecycleGeneration: 206
            )
        )
        await trustDispatch.wait()

        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: assetId
        )
        let receipt = try #require(receipts.first)
        #expect(receipts.count == 1)
        #expect(receipt.source == .bannerSuggestionConfirmed)
        #expect(receipt.podcastId == "podcast-r26-e2e")
        let promotedId = try #require(
            receipt.targetRefs?.adWindowId
        )

        let responseDocs = try makeTempDir(prefix: "r26-e2e-after")
        let responseCorpus = try await CorpusExporter.export(
            store: store,
            documentsURL: responseDocs,
            now: exportedAt,
            dedupMemo: CorpusExportDedupMemo()
        )
        let responseCorpusBytes = try Data(
            contentsOf: responseCorpus.fileURL
        )
        #expect(responseCorpusBytes == baselineCorpusBytes)
        let corpusText = try #require(
            String(data: responseCorpusBytes, encoding: .utf8)
        )
        #expect(!corpusText.contains(receipt.id))
        #expect(!corpusText.contains(promotedId))

        let responseDebug = try #require(
            await DebugEpisodeExportService.build(
                episodeTitle: "R26 E2E",
                podcastTitle: "R26 Podcast",
                analysisAssetId: assetId,
                episodeId: episodeId,
                store: store,
                exportedAt: exportedAt
            )
        )
        #expect(responseDebug.content == baselineDebug.content)
        #expect(responseDebug.filename == baselineDebug.filename)

        let responseShare = try await store
            .exportCrossUserAnalysisSnapshot(
                assetId: assetId,
                podcastId: "podcast-r26-e2e",
                exportedAt: exportedAt,
                sourceAppBuild: "r26-e2e"
            )
        #expect(responseShare == baselineShare)

        let detection = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: .default
        )
        let replayResult = try await detection.runHotPath(
            chunks: [],
            analysisAssetId: assetId,
            episodeDuration: 300
        )
        #expect(
            !replayResult.contains {
                $0.boundaryState == "correctionReplay"
            },
            "The explicit receipt must not create a correction-replay suggestion"
        )
    }

    @Test("Listen retirement removes live cue and rejects stale playback lifecycle")
    func liveListenRetirementIsCueImmediateAndLifecycleBound() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 19
        )
        let ad = makeSkipTestAdWindow(
            id: "listen-live-cue",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        await orchestrator.receiveAdWindows([ad])
        #expect(!pushedCues.isEmpty)

        #expect(
            !(await orchestrator.retireLiveSkipForListen(
                windowId: ad.id,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 18
            ))
        )
        #expect(!pushedCues.isEmpty,
                "A stale banner must not mutate current cues")

        #expect(
            await orchestrator.retireLiveSkipForListen(
                windowId: ad.id,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 19
            )
        )
        #expect(pushedCues.isEmpty)
    }

    @Test("Listen retirement reservation permits a persistence retry only until completion")
    func liveListenRetirementReservationIsRetryable() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 27
        )
        let ad = makeSkipTestAdWindow(
            id: "listen-retry-reservation",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        await orchestrator.receiveAdWindows([ad])

        #expect(
            await orchestrator.retireLiveSkipForListen(
                windowId: ad.id,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 27
            )
        )
        #expect(
            await orchestrator.retireLiveSkipForListen(
                windowId: ad.id,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 27
            ),
            "a failed durable write must be able to retry after the cue was already retired"
        )

        await orchestrator.completeLiveSkipRetirementForListen(
            windowId: ad.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 27
        )
        #expect(
            !(await orchestrator.retireLiveSkipForListen(
                windowId: ad.id,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 27
            )),
            "once the receipt is durable, the reverted window must not be consumed again"
        )
    }

    @Test("late skip persistence cannot resurrect explicit or producer-terminal rows")
    func staleAppliedPersistenceIsConditionedOnEligibleState() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())

        let reverted = makeSkipTestAdWindow(
            id: "stale-apply-after-listen",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.reverted.rawValue
        )
        let suppressed = makeSkipTestAdWindow(
            id: "stale-apply-after-suppression",
            startTime: 180,
            endTime: 240,
            confidence: 0.9,
            decisionState: AdDecisionState.suppressed.rawValue
        )
        try await store.insertAdWindow(reverted)
        try await store.insertAdWindow(suppressed)

        #expect(
            try await store.persistAppliedAdWindowIfEligible(
                windowId: reverted.id,
                analysisAssetId: "different-asset",
                expectedProducerRevision: reverted
            ) == false,
            "A stale promotion must remain scoped to the originating analysis asset"
        )
        #expect(
            try await store.persistAppliedAdWindowIfEligible(
                windowId: reverted.id,
                analysisAssetId: reverted.analysisAssetId,
                expectedProducerRevision: reverted
            ) == false
        )
        #expect(
            try await store.persistAppliedAdWindowIfEligible(
                windowId: suppressed.id,
                analysisAssetId: suppressed.analysisAssetId,
                expectedProducerRevision: suppressed
            ) == false
        )

        let rows = try await store.fetchAdWindows(assetId: "asset-1")
        let revertedRow = try #require(
            rows.first { $0.id == reverted.id }
        )
        let suppressedRow = try #require(
            rows.first { $0.id == suppressed.id }
        )
        #expect(revertedRow.decisionState == AdDecisionState.reverted.rawValue)
        #expect(revertedRow.wasSkipped == false)
        #expect(
            suppressedRow.decisionState
                == AdDecisionState.suppressed.rawValue
        )
        #expect(suppressedRow.wasSkipped == false)
    }

    @Test("eligible skip persistence atomically stamps state and receipt")
    func eligibleAppliedPersistenceUpdatesBothFields() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let confirmed = makeSkipTestAdWindow(
            id: "eligible-atomic-apply",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        try await store.insertAdWindow(confirmed)

        #expect(
            try await store.persistAppliedAdWindowIfEligible(
                windowId: confirmed.id,
                analysisAssetId: confirmed.analysisAssetId,
                expectedProducerRevision: confirmed
            )
        )
        let row = try #require(
            (try await store.fetchAdWindows(assetId: "asset-1"))
                .first { $0.id == confirmed.id }
        )
        #expect(row.decisionState == AdDecisionState.applied.rawValue)
        #expect(row.wasSkipped)
    }

    @Test("late skip persistence cannot promote a materially revised same-ID row")
    func appliedPersistenceIsProducerRevisionFenced() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let scheduledRevision = makeSkipTestAdWindow(
            id: "same-id-reconciled-before-apply",
            startTime: 60,
            endTime: 120,
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        try await store.insertAdWindow(scheduledRevision)

        let replacementRevision = makeSkipTestAdWindow(
            id: scheduledRevision.id,
            startTime: 75,
            endTime: 150,
            confidence: 0.82,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        try await store.insertOrReplaceAdWindow(replacementRevision)

        #expect(
            try await store.persistAppliedAdWindowIfEligible(
                windowId: scheduledRevision.id,
                analysisAssetId: scheduledRevision.analysisAssetId,
                expectedProducerRevision: scheduledRevision
            ) == false
        )
        let row = try #require(
            try await store.fetchAdWindow(id: scheduledRevision.id)
        )
        #expect(row.startTime == replacementRevision.startTime)
        #expect(row.endTime == replacementRevision.endTime)
        #expect(
            row.decisionState == AdDecisionState.confirmed.rawValue
        )
        #expect(!row.wasSkipped)
    }

    @Test("suggestion produced without a banner host replays once when the host attaches")
    func pendingSuggestionReplaysWhenBannerHostAttaches() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1"
        )

        let suggest = makeSuggestWindow(id: "ad-suggest-while-away")
        await orchestrator.receiveAdWindows([suggest])
        #expect(
            await orchestrator.activeSuggestWindowIDs().contains(suggest.id),
            "The undecided window remains pending while no UI can answer it"
        )

        let stream = await orchestrator.bannerItemStream()
        let probe = BoundedStreamProbe(stream)
        let replayed = await probe.next()
        #expect(replayed?.windowId == suggest.id)
        #expect(replayed?.episodeId == "episode-1",
                "Replay must preserve the episode identity used by queue gating")
        await orchestrator.acknowledgeSuggestedBannerDelivery(
            windowId: suggest.id,
            episodeId: replayed?.episodeId,
            playbackLifecycleGeneration:
                replayed?.playbackLifecycleGeneration
        )

        await orchestrator.declineSuggestedSkip(windowId: suggest.id)
        await orchestrator.receiveAdWindows([suggest])
        #expect(
            !(await orchestrator.activeSuggestWindowIDs().contains(suggest.id)),
            "Once replayed and neutrally retired, the same in-session window must not reappear"
        )
    }

    @Test("stale same-episode delivery acknowledgement does not suppress replacement replay")
    func staleDeliveryAcknowledgementDoesNotSuppressReplacementReplay() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 41
        )

        let suggest = makeSuggestWindow(id: "same-id-replacement")
        try await store.insertAdWindow(suggest)
        await orchestrator.receiveAdWindows([suggest])

        let oldStream = await orchestrator.bannerItemStream()
        let oldProbe = BoundedStreamProbe(oldStream)
        let oldItem = await oldProbe.next()
        #expect(oldItem?.windowId == suggest.id)
        #expect(oldItem?.playbackLifecycleGeneration == 41)

        // Replay the same canonical episode and persisted window under a new
        // playback lifecycle before the old observer's actor-hop ack lands.
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 42
        )
        await orchestrator.receiveAdWindows([suggest])
        await orchestrator.acknowledgeSuggestedBannerDelivery(
            windowId: suggest.id,
            episodeId: oldItem?.episodeId,
            playbackLifecycleGeneration:
                oldItem?.playbackLifecycleGeneration
        )

        #expect(
            !(await orchestrator.acknowledgedSuggestWindowIDs())
                .contains(suggest.id),
            "A stale acknowledgement must not mark the replacement suggestion as already presented"
        )
        #expect(
            await orchestrator.activeSuggestWindowIDs()
                .contains(suggest.id),
            "The replacement suggestion must remain pending for replay to the new host"
        )

        await orchestrator.acknowledgeSuggestedBannerDelivery(
            windowId: suggest.id,
            episodeId: "episode-1",
            playbackLifecycleGeneration: 42
        )
        #expect(
            await orchestrator.acknowledgedSuggestWindowIDs()
                .contains(suggest.id),
            "The replacement lifecycle's own acknowledgement must still be accepted"
        )
    }

    @Test("same-ID suggestion revision rejects stale ack, Yes, and No")
    func sameIDSuggestionRevisionRejectsStalePresentationActions() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "episode-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 43
        )

        let stream = await orchestrator.bannerItemStream()
        let probe = BoundedStreamProbe(stream)
        let original = makeSuggestWindow(
            id: "same-id-same-lifecycle",
            startTime: 60.12341,
            endTime: 75.9876
        )
        try await store.insertAdWindow(original)
        await orchestrator.receiveAdWindows([original])
        let oldItem = try #require(await probe.next())
        let oldRevision = try #require(
            oldItem.suggestionRevisionToken
        )

        let revised = makeSuggestWindow(
            id: original.id,
            startTime: 60.1234,
            endTime: 75.9876
        )
        try await store.insertOrReplaceAdWindow(revised)
        await orchestrator.receiveAdWindows([revised])
        let revisedItem = try #require(await probe.next())
        let revisedToken = try #require(
            revisedItem.suggestionRevisionToken
        )
        #expect(revisedToken != oldRevision)
        #expect(revisedItem.adStartTime == 60.1234)
        #expect(revisedItem.adEndTime == 75.9876)

        await orchestrator.acknowledgeSuggestedBannerDelivery(
            windowId: original.id,
            episodeId: oldItem.episodeId,
            playbackLifecycleGeneration:
                oldItem.playbackLifecycleGeneration,
            suggestionRevisionToken: oldRevision
        )
        #expect(
            !(await orchestrator.acknowledgedSuggestWindowIDs())
                .contains(original.id),
            "The old revision's ack must not suppress the replacement"
        )

        await orchestrator.acceptSuggestedSkip(
            windowId: original.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 43,
            ifSuggestionRevisionToken: oldRevision
        )
        await orchestrator.declineSuggestedSkip(
            windowId: original.id,
            isExplicitDenial: true,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 43,
            ifSuggestionRevisionToken: oldRevision
        )
        #expect(
            await orchestrator.activeSuggestWindowIDs()
                .contains(original.id),
            "Stale Yes/No actions must leave the current revision pending"
        )
        let beforeCurrentAction = try await store.fetchAdWindows(
            assetId: "asset-1"
        )
        let originalRow = beforeCurrentAction.first {
            $0.id == original.id
        }
        #expect(originalRow?.userDismissedBanner == false)
        #expect(
            originalRow?.decisionState
                == AdDecisionState.candidate.rawValue
        )

        await orchestrator.acknowledgeSuggestedBannerDelivery(
            windowId: original.id,
            episodeId: revisedItem.episodeId,
            playbackLifecycleGeneration:
                revisedItem.playbackLifecycleGeneration,
            suggestionRevisionToken: revisedToken
        )
        await orchestrator.acceptSuggestedSkip(
            windowId: original.id,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 43,
            ifSuggestionRevisionToken: revisedToken
        )

        let persisted = try await store.fetchAdWindows(
            assetId: "asset-1"
        )
        let promoted = persisted.first {
            $0.id != original.id
                && $0.wasSkipped
                && $0.startTime == 60.1234
                && $0.endTime == 75.9876
        }
        #expect(
            promoted != nil,
            "The current revision's Yes must apply its revised span"
        )
        let receipts = try await store.loadCorrectionEvents(
            analysisAssetId: "asset-1"
        )
        #expect(receipts.count == 1)
        #expect(receipts.first?.source == .bannerSuggestionConfirmed)
        #expect(
            receipts.first?.targetRefs?.exactFeedbackSpan?.matches(
                startTime: 60.1234,
                endTime: 75.9876
            ) == true
        )
    }

    @Test("direct episode replacement clears accepted-suggestion race guards")
    func directEpisodeReplacementClearsAcceptedSuggestionIDs() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-1", episodeId: "episode-1")
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: "asset-2", episodeId: "episode-2")
        )
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            podcastId: "podcast-1",
            playbackLifecycleGeneration: 51
        )

        let reusedID = "producer-stable-window-id"
        let firstEpisodeSuggestion = makeSuggestWindow(id: reusedID)
        await orchestrator.receiveAdWindows([firstEpisodeSuggestion])
        await orchestrator.acceptSuggestedSkip(
            windowId: reusedID,
            ifCurrentEpisodeId: "episode-1",
            ifPlaybackLifecycleGeneration: 51
        )

        // Production can switch episodes by calling beginEpisode directly;
        // endEpisode is not guaranteed to run between those calls. A producer
        // may reuse an otherwise-stable window id under a different asset.
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2",
            episodeId: "episode-2",
            podcastId: "podcast-2",
            playbackLifecycleGeneration: 52
        )
        let secondEpisodeWindow = makeSkipTestAdWindow(
            id: reusedID,
            assetId: "asset-2",
            confidence: 0.9,
            decisionState: AdDecisionState.confirmed.rawValue
        )
        await orchestrator.receiveAdWindows([secondEpisodeWindow])

        #expect(
            await orchestrator.activeWindowIDs().contains(reusedID),
            "An accepted id from episode 1 must not suppress episode 2"
        )
    }

    @Test(
        "stale suggest completion cannot clear replacement provisional ownership",
        .timeLimit(.minutes(1))
    )
    func staleSuggestCompletionPreservesReplacementProvisionalOwnership()
        async throws
    {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-1",
                episodeId: "episode-1"
            )
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-2",
                episodeId: "episode-2"
            )
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        let sourceGate = ControlledAsyncGate()
        await orchestrator._setSuggestPersistenceBarrierForTesting {
            await sourceGate.wait()
        }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-1",
            playbackLifecycleGeneration: 61
        )

        let reusedID = "provisional-reused-id"
        let sourceSuggestion = makeSuggestWindow(id: reusedID)
        try await store.insertAdWindow(sourceSuggestion)
        await orchestrator.receiveAdWindows([sourceSuggestion])
        let sourceResponse = Task {
            await orchestrator.acceptSuggestedSkip(
                windowId: reusedID,
                ifCurrentEpisodeId: "episode-1",
                ifPlaybackLifecycleGeneration: 61
            )
        }
        await sourceGate.waitUntilStarted()

        await orchestrator.beginEpisode(
            analysisAssetId: "asset-2",
            episodeId: "episode-2",
            playbackLifecycleGeneration: 62
        )
        let replacementGate = ControlledAsyncGate()
        await orchestrator._setSuggestPersistenceBarrierForTesting {
            await replacementGate.wait()
        }
        let replacementSuggestion = makeSuggestWindow(
            id: reusedID,
            analysisAssetId: "asset-2",
            startTime: 180,
            endTime: 240
        )
        // Production persists the replacement producer row before delivering
        // it. Because IDs are producer-owned, that can replace Episode A's
        // globally-primary same-ID row while A's action is still suspended.
        try await store.insertOrReplaceAdWindow(replacementSuggestion)
        await orchestrator.receiveAdWindows([replacementSuggestion])
        let replacementResponse = Task {
            await orchestrator.acceptSuggestedSkip(
                windowId: reusedID,
                ifCurrentEpisodeId: "episode-2",
                ifPlaybackLifecycleGeneration: 62
            )
        }
        await replacementGate.waitUntilStarted()

        await sourceGate.release()
        #expect(
            !(await sourceResponse.value),
            "Episode A's transaction must roll back after its source row is replaced"
        )
        #expect(
            await orchestrator
                ._isSuggestResolutionProvisionalForTesting(id: reusedID),
            "Episode A's completion must not clear episode B's in-flight ownership for a reused producer ID"
        )

        await replacementGate.release()
        #expect(await replacementResponse.value)
        #expect(
            !(await orchestrator
                ._isSuggestResolutionProvisionalForTesting(id: reusedID))
        )
    }

    @Test("episode-bound suggest actions reject stale banner identities")
    func episodeBoundSuggestActionsRejectStaleIdentity() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "episode-current",
            podcastId: "podcast-current"
        )
        let acceptCandidate = makeSuggestWindow(id: "suggest-stale-accept")
        let declineCandidate = makeSuggestWindow(id: "suggest-stale-decline")
        await orchestrator.receiveAdWindows([
            acceptCandidate,
            declineCandidate
        ])

        await orchestrator.acceptSuggestedSkip(
            windowId: acceptCandidate.id,
            ifCurrentEpisodeId: "episode-stale"
        )
        await orchestrator.declineSuggestedSkip(
            windowId: declineCandidate.id,
            isExplicitDenial: true,
            ifCurrentEpisodeId: "episode-stale"
        )

        let remaining = await orchestrator.activeSuggestWindowIDs()
        #expect(remaining.contains(acceptCandidate.id))
        #expect(remaining.contains(declineCandidate.id))
    }

    @Test("explicit suggest No persists a falsePositive correction and sets userDismissedBanner=1")
    func suggestDenialPersistsFalsePositiveCorrection() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "asset-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: correctionStore
        )
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { pushedCues = $0 }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let suggest = makeSuggestWindow(
            id: "ad-suggest-fp",
            startTime: 60.1234,
            endTime: 75.9876
        )
        try await store.insertAdWindow(suggest)
        await orchestrator.receiveAdWindows([suggest])
        let decisionCountBeforeFeedback =
            await orchestrator.getDecisionLog().count
        #expect(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-fp"),
                "markOnly window must enter the suggest tier")

        // User answers No on the suggest banner.
        await orchestrator.declineSuggestedSkip(windowId: "ad-suggest-fp", isExplicitDenial: true)

        // The suggest entry is cleared.
        #expect(!(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-fp")),
                "denied suggest window must be removed from the suggest set")

        // A producer can redeliver a stale object after persistence completes,
        // or recompute that row in-place with a changed span. The explicit No
        // applies to the producer ID, so neither delivery may recreate it
        // during this episode.
        await orchestrator.receiveAdWindows([suggest])
        let revisedAfterDenial = makeSuggestWindow(
            id: suggest.id,
            startTime: 65,
            endTime: 125
        )
        await orchestrator.receiveAdWindows([revisedAfterDenial])
        #expect(
            !(await orchestrator.activeSuggestWindowIDs()
                .contains(suggest.id)),
            "an explicit No must suppress exact and revised same-ID redelivery"
        )

        let eligibleAfterDenial = makeSuggestWindow(
            id: suggest.id,
            confidence: 0.99,
            decisionState: AdDecisionState.confirmed.rawValue,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([eligibleAfterDenial])
        await orchestrator.receiveAdDecisionResults([
            AdDecisionResult(
                id: suggest.id,
                analysisAssetId: "asset-1",
                startTime: 75,
                endTime: 135,
                skipConfidence: 0.99,
                eligibilityGate: .eligible,
                recomputationRevision: 2
            )
        ])
        #expect(
            !(await orchestrator.activeWindowIDs()).contains(suggest.id),
            "An explicit No must block same-ID eligible AdWindow and fusion redelivery"
        )
        #expect(
            pushedCues.isEmpty,
            "An explicit No must prevent same-ID eligible redelivery from auto-skipping"
        )

        // Exactly one falsePositive correction is persisted, over the window's
        // span, with causal attribution and target refs.
        let corrections = try await correctionStore.activeCorrections(
            for: "asset-1"
        )
        #expect(corrections.count == 1,
                "one denial must persist exactly one correction; got \(corrections.count)")
        let correction = try #require(corrections.first)
        #expect(correction.correctionType == .falsePositive,
                "denial must record a .falsePositive correction; got \(String(describing: correction.correctionType))")
        #expect(correction.source == .bannerSuggestionDenied)
        #expect(correction.source?.kind == .falsePositive,
                "source must map to the false-positive kind so the materializer counts it as a revert")
        #expect(correction.targetRefs?.adWindowId == suggest.id)
        #expect(
            await orchestrator.getDecisionLog().count
                == decisionCountBeforeFeedback,
            "Suggest No must not add a detailed decision-log record"
        )
        #expect(correction.causalSource == .specialist,
                "specialist-composed mark must attribute to .specialist; got \(String(describing: correction.causalSource))")
        #expect(correction.targetRefs?.sponsorEntity == "red bull",
                "target refs must carry the normalized brand for hard-negative mining; got \(String(describing: correction.targetRefs?.sponsorEntity))")
        #expect(correction.podcastId == "podcast-1")
        #expect(correction.targetRefs?.domain == "podcast-1",
                "denial attribution must remain bound to the source podcast")
        #expect(
            correction.targetRefs?.exactFeedbackSpan?.matches(
                startTime: 60.1234,
                endTime: 75.9876
            ) == true
        )
        if case .exactTimeSpan(_, let s, let e) = CorrectionScope.deserialize(correction.scope) {
            #expect(s == 60.123, "scope remains a canonical three-decimal envelope")
            #expect(e == 75.988, "scope remains a canonical three-decimal envelope")
        } else {
            Issue.record("Expected exactTimeSpan scope from the denial, got \(correction.scope)")
        }

        // userDismissedBanner=1 and decisionState=.reverted on the row.
        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        let row = persisted.first { $0.id == "ad-suggest-fp" }
        #expect(row?.userDismissedBanner == true,
                "denied window row must carry userDismissedBanner=1")
        #expect(row?.decisionState == AdDecisionState.reverted.rawValue,
                "denied window must persist as .reverted so it does not resurface; got \(row?.decisionState ?? "<missing>")")
    }

    @Test("Suggest-No row mutation rolls back atomically on second-write failure")
    func suggestDenialRowMutationIsAtomic() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let suggest = makeSuggestWindow(id: "atomic-suggest-no")
        try await store.insertAdWindow(suggest)
        let correction = CorrectionEvent(
            analysisAssetId: suggest.analysisAssetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: suggest.analysisAssetId,
                startTime: suggest.startTime,
                endTime: suggest.endTime
            ).serialized,
            source: .manualVeto,
            correctionType: .falsePositive
        )
        try await store.execForTesting(
            """
            CREATE TRIGGER fail_atomic_suggest_no
            BEFORE UPDATE OF decisionState ON ad_windows
            WHEN NEW.id = 'atomic-suggest-no'
              AND NEW.decisionState = 'reverted'
            BEGIN
                SELECT RAISE(ABORT, 'injected second-write failure');
            END
            """
        )

        var didThrow = false
        do {
            try await store.persistDeclinedSuggestion(
                windowId: suggest.id,
                analysisAssetId: suggest.analysisAssetId,
                correction: correction
            )
        } catch {
            didThrow = true
        }
        #expect(didThrow)

        let row = try #require(
            (try await store.fetchAdWindows(assetId: "asset-1"))
                .first { $0.id == suggest.id }
        )
        #expect(
            row.userDismissedBanner == false,
            "The first write must roll back when the revert write fails"
        )
        #expect(
            row.decisionState == AdDecisionState.candidate.rawValue,
            "A failed transaction must leave the original row actionable"
        )
        #expect(
            try await correctionStore.activeCorrections(for: "asset-1")
                .isEmpty,
            "The correction append must roll back with the failed row mutation"
        )

        try await store.execForTesting("DROP TRIGGER fail_atomic_suggest_no")
        _ = try await store.persistDeclinedSuggestion(
            windowId: suggest.id,
            analysisAssetId: suggest.analysisAssetId,
            correction: correction
        )
        let corrections = try await correctionStore.activeCorrections(
            for: "asset-1"
        )
        #expect(corrections.count == 1)
        #expect(
            corrections.first?.submissionCount == 1,
            "The rolled-back attempt must not increment durable submissions"
        )
    }

    @Test("Suggest-No transaction cannot mutate a replacement asset's same-ID row")
    func suggestDenialIsAssetScoped() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-1",
                episodeId: "episode-1"
            )
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-2",
                episodeId: "episode-2"
            )
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let reusedID = "same-id-decline-scope"
        let source = makeSuggestWindow(id: reusedID)
        try await store.insertAdWindow(source)
        let replacement = makeSuggestWindow(
            id: reusedID,
            analysisAssetId: "asset-2",
            startTime: 180,
            endTime: 240
        )
        try await store.insertOrReplaceAdWindow(replacement)
        let sourceCorrection = CorrectionEvent(
            analysisAssetId: source.analysisAssetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: source.analysisAssetId,
                startTime: source.startTime,
                endTime: source.endTime
            ).serialized,
            source: .manualVeto,
            correctionType: .falsePositive
        )

        var didThrow = false
        do {
            _ = try await store.persistDeclinedSuggestion(
                windowId: reusedID,
                analysisAssetId: source.analysisAssetId,
                correction: sourceCorrection
            )
        } catch {
            didThrow = true
        }
        #expect(
            didThrow,
            "The stale source transaction must reject the replacement asset"
        )

        let replacementRow = try #require(
            (try await store.fetchAdWindows(assetId: "asset-2"))
                .first { $0.id == reusedID }
        )
        #expect(replacementRow.decisionState == replacement.decisionState)
        #expect(replacementRow.userDismissedBanner == false)
        #expect(
            try await correctionStore.activeCorrections(for: "asset-1")
                .isEmpty,
            "The source correction append must roll back with the scoped update"
        )
    }

    @Test("Auto-No transaction cannot mutate a replacement asset's same-ID row")
    func automaticDenialIsAssetScoped() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-1",
                episodeId: "episode-1"
            )
        )
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: "asset-2",
                episodeId: "episode-2"
            )
        )
        let reusedID = "same-id-auto-no-scope"
        let source = makeSuggestWindow(id: reusedID)
        try await store.insertAdWindow(source)
        let replacement = makeSuggestWindow(
            id: reusedID,
            analysisAssetId: "asset-2",
            startTime: 180,
            endTime: 240
        )
        try await store.insertOrReplaceAdWindow(replacement)

        var didThrow = false
        do {
            _ = try await store.persistRevertedAdWindow(
                windowId: reusedID,
                analysisAssetId: source.analysisAssetId
            )
        } catch {
            didThrow = true
        }

        #expect(didThrow)
        let replacementRow = try #require(
            try await store.fetchAdWindow(id: reusedID)
        )
        #expect(replacementRow.analysisAssetId == "asset-2")
        #expect(replacementRow.decisionState == replacement.decisionState)
    }

    @Test("declineSuggestedSkip auto-fade (isExplicitDenial:false) records NO correction and leaves userDismissedBanner=0")
    func suggestAutoFadeRecordsNothing() async throws {
        let store = try await makeTestStore()
        // playhead-i08e (third pass): the asset row must declare the SAME
        // episode `beginEpisode` announces below, exactly as this test's
        // explicit-denial sibling `suggestDenialPersistsFalsePositiveCorrection`
        // already does. With the fixture's default `"ep-1"` against
        // `beginEpisode(episodeId: "asset-1")`, `feedbackAssetMatches` refused
        // `persistDeclinedSuggestionIfCurrent` on ownership grounds — so a
        // regression that treated a passive auto-fade as an explicit denial
        // would ALSO have written nothing, and all three negative assertions
        // below would have held for a reason unrelated to the contract they
        // name. Aligning the ids makes the persistence path genuinely
        // reachable, so only `isExplicitDenial == false` keeps it silent.
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "asset-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(store: store, correctionStore: correctionStore)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let suggest = makeSuggestWindow(id: "ad-suggest-fade")
        try await store.insertAdWindow(suggest)
        await orchestrator.receiveAdWindows([suggest])

        // Banner auto-fades (no user tap) — the default path.
        await orchestrator.declineSuggestedSkip(windowId: "ad-suggest-fade")

        #expect(!(await orchestrator.activeSuggestWindowIDs().contains("ad-suggest-fade")),
                "faded suggest window must still be dropped from the suggest set")

        let corrections = try await correctionStore.activeCorrections(for: "asset-1")
        #expect(corrections.isEmpty,
                "a passive auto-fade must NOT mint a hard negative; got \(corrections.count) corrections")

        let row = (try await store.fetchAdWindows(assetId: "asset-1")).first { $0.id == "ad-suggest-fade" }
        #expect(row?.userDismissedBanner == false,
                "auto-fade must not set userDismissedBanner")
        #expect(row?.decisionState == AdDecisionState.candidate.rawValue,
                "auto-fade must not flip the row to .reverted; got \(row?.decisionState ?? "<missing>")")
    }

    @Test("two same-span suggest denials retain two exact private receipts and the unanswered projection")
    func suggestDenialRetainsExactWindowOwnership() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "asset-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(store: store, correctionStore: correctionStore)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Two distinct suggest windows over the SAME span. The generic v23
        // identity would collapse them, but private receipts must retain the
        // exact target ownership of both answers.
        let w1 = makeSuggestWindow(id: "ad-suggest-dup-1")
        let w2 = makeSuggestWindow(id: "ad-suggest-dup-2")
        try await store.insertAdWindow(w1)
        try await store.insertAdWindow(w2)
        await orchestrator.receiveAdWindows([w1, w2])

        await orchestrator.declineSuggestedSkip(windowId: "ad-suggest-dup-1", isExplicitDenial: true)
        await orchestrator.declineSuggestedSkip(windowId: "ad-suggest-dup-2", isExplicitDenial: true)

        let corrections = try await correctionStore.activeCorrections(
            for: "asset-1"
        )
        #expect(corrections.count == 2)
        #expect(corrections.allSatisfy { $0.submissionCount == 1 })
        #expect(
            Set(corrections.compactMap { $0.targetRefs?.adWindowId })
                == Set([w1.id, w2.id])
        )
        #expect(corrections.allSatisfy {
            $0.targetRefs?.explicitFeedbackDetectionProjection != nil
        })

        let responded = try await store.fetchAdWindows(assetId: "asset-1")
        let before = ExplicitBannerFeedbackPrivacy
            .responseIndependentProjection(
                windows: [w1, w2],
                corrections: []
            )
        let after = try await store.responseIndependentAdWindows(
            analysisAssetId: "asset-1"
        )
        #expect(
            responded.allSatisfy {
                $0.decisionState == AdDecisionState.reverted.rawValue
            }
        )
        #expect(
            after?.map(ExplicitFeedbackDetectionProjection.init)
                == before?.map(ExplicitFeedbackDetectionProjection.init)
        )
    }

    @Test("causalSource(forMetadataSource:) maps producer tags to the responsible source")
    func causalSourceMappingIsFaithful() {
        #expect(SkipOrchestrator.causalSource(forMetadataSource: SpecialistMarkComposer.metadataSource) == .specialist)
        #expect(SkipOrchestrator.causalSource(forMetadataSource: "foundationModels") == .foundationModel)
        #expect(SkipOrchestrator.causalSource(forMetadataSource: "none") == .foundationModel)
        #expect(SkipOrchestrator.causalSource(forMetadataSource: "fallback") == .foundationModel)
    }
}
