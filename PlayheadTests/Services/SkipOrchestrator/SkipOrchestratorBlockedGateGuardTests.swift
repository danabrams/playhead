// SkipOrchestratorBlockedGateGuardTests.swift
//
// playhead-bq70: symmetric blocked-gate guard in `SkipOrchestrator.receiveAdWindows`.
//
// Background:
//   `receiveAdDecisionResults` hard-filters its inputs via
//     `guard result.eligibilityGate == .eligible else { continue }`
//   so blocked fusion decisions never enter the active managed-window set.
//
//   `receiveAdWindows` is the parallel entry point for the AdWindow path.
//   Until playhead-bq70 it pinned ONLY the `.markOnly` branch (per
//   playhead-gtt9.11/L3) and let every other decoded `SkipEligibilityGate`
//   raw value fall through into `evaluateAndPush`. Fusion stamps
//   originate in `AdDetectionService.runBackfill` via `buildFusionAdWindow`
//   (which writes `decision.eligibilityGate.rawValue` directly and
//   persists `decisionState == .candidate` when
//   `policyAction == .autoSkipEligible AND decision.eligibilityGate != .eligible`).
//   Those rows surface to ALL THREE callers of `receiveAdWindows`:
//     1. Cross-launch preload (`SkipOrchestrator.beginEpisode`).
//     2. Hot-path post-classification push
//        (`AnalysisCoordinator.handlePersistedTranscriptChunks`).
//     3. Final-pass backfill push
//        (`AnalysisCoordinator.finalizeBackfill`).
//   Without the symmetric guard, a window stamped (e.g.) `blockedByPolicy`
//   would silently re-enter the auto-skip path on any of those callers,
//   violating the precision contract that `receiveAdDecisionResults`
//   already enforces for the AdDecisionResult path.
//
// What this suite pins:
//   • Each blocked SkipEligibilityGate raw value, when stamped on an
//     AdWindow, MUST NOT reach `evaluateAndPush` (verified via
//     `confirmedWindows` and the `auto_skip_fired` decision-log signal),
//     and MUST NOT emit an auto-skip banner.
//   • A negative control with `eligibilityGate = "eligible"` confirms
//     the guard is targeted, not over-broad.
//   • A negative control with `eligibilityGate = "markOnly"` confirms
//     the existing markOnly/suggest-tier path is unaffected.
//
// Companion canary:
//   `SkipOrchestratorBlockedGateGuardSourceCanaryTests` (XCTest) pins the
//   guard's source shape so a refactor that drops the guard fails fast
//   instead of regressing the runtime contract silently.

import Foundation
import Testing
@testable import Playhead

/// playhead-ar60: the SkipOrchestrator half of the detection/actuation split.
///
/// V47 gave `AdWindow` two confidences. Every gate in this actor that decides
/// whether a skip may FIRE must read `actuationConfidence`; the detection
/// number is deliberately higher for any span calibration or a user correction
/// discounted. Reverting any of those reads to `.confidence` must be visible,
/// and before this suite it was not: no test anywhere built an `AdWindow`
/// whose two confidences DIFFER and then drove the orchestrator with it.
///
/// The fixture is the shape the fusion path now produces for a discounted
/// span: detection 0.95 (well over every threshold in `SkipPolicyConfig`),
/// actuation 0.01 (under all of them).
@Suite("SkipOrchestrator reads ACTUATION, not detection (playhead-ar60)")
struct SkipOrchestratorActuationReadTests {

    private func splitWindow(
        id: String,
        confidence: Double,
        skipConfidence: Double?,
        gate: SkipEligibilityGate = .eligible,
        startTime: Double = 60,
        endTime: Double = 120
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: "asset-1",
            startTime: startTime,
            endTime: endTime,
            confidence: confidence,
            skipConfidence: skipConfidence,
            boundaryState: "acousticRefined",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "fusion-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: startTime,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: gate.rawValue
        )
    }

    /// Drive one window through `beginEpisode` under maximum auto-skip
    /// pressure and return the decision record `evaluateWindow` wrote for it.
    private func decisionRecord(
        for window: AdWindow
    ) async throws -> SkipDecisionRecord {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.95,
            observations: 50
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        await orchestrator.receiveAdWindows([window])
        try await Task.sleep(for: .milliseconds(100))
        let log = await orchestrator.getDecisionLog()
        return try #require(
            log.last { $0.adWindowId == window.id },
            "the window must reach evaluateWindow at all — got \(log.map(\.adWindowId))"
        )
    }

    @Test("a span with detection 0.95 but actuation 0.01 is judged on 0.01")
    func lowActuationDoesNotSkipDespiteHighDetection() async throws {
        // Under `SkipPolicyConfig`'s defaults (enterThreshold 0.65) a
        // confirmed, eligible window at 0.95 clears every gate. 0.01 clears
        // none. The record is the direct witness of WHICH number the
        // comparison used.
        let record = try await decisionRecord(
            for: splitWindow(
                id: "ar60-low-actuation",
                confidence: 0.95,
                skipConfidence: 0.01
            )
        )
        #expect(
            record.confidence == 0.01,
            """
            `evaluateWindow` must judge on the ACTUATION number. The decision             record says \(record.confidence); the row's detection number is             0.95 and its actuation number is 0.01.
            """
        )
        #expect(record.decision != .applied,
                "reason: \(record.reason)")

        // …and the number CHANGED THE OUTCOME. Pinned as a difference rather
        // than as a literal reason string: which arm a 0.95 window takes
        // depends on edge-padding and trust policy that this bead does not
        // own, but "0.01 and 0.95 are judged the same way" would mean the
        // comparison is not reading the field at all.
        let control = try await decisionRecord(
            for: splitWindow(
                id: "ar60-low-actuation-control",
                confidence: 0.95,
                skipConfidence: nil
            )
        )
        #expect(record.reason != control.reason,
                "0.01 and 0.95 must not reach the same verdict — both said \(record.reason)")
    }

    @Test("the SAME span with only its detection number present is judged on 0.95 — the control")
    func highSingleNumberStillSkips() async throws {
        // `skipConfidence: nil` — a one-number producer. `actuationConfidence`
        // falls back to `confidence`, so this row must be judged on 0.95 and
        // must NOT take the sub-threshold arm. Without this control the test
        // above would also pass if the orchestrator had simply stopped
        // evaluating anything.
        let record = try await decisionRecord(
            for: splitWindow(
                id: "ar60-single-number",
                confidence: 0.95,
                skipConfidence: nil
            )
        )
        #expect(record.confidence == 0.95,
                "the fallback must judge a one-number producer exactly as before")
        #expect(record.decision != .suppressed,
                "0.95 clears every default threshold — got \(record.reason)")
    }

    @Test("a non-finite actuation number is refused at the ingest door")
    func malformedActuationIsRefused() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        // playhead-wq34: this suite's subject is the GATE guard — which raw
        // values reach the managed tier — so the show has to be one whose
        // detector classes can use that tier. Without a trust profile the show
        // resolves `.shadow`, and every `.eligible` row is routed to the
        // suggest tier for a reason that has nothing to do with its gate.
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // `confidence` is perfectly valid; only the actuation number is not.
        // Before ar60 added the second clause, `hasValidRuntimeWindowMaterial`
        // checked `confidence` alone and this row was admitted.
        for (id, bad) in [("ar60-nan", Double.nan), ("ar60-over", 1.5)] {
            await orchestrator.receiveAdWindows([
                splitWindow(id: id, confidence: 0.9, skipConfidence: bad)
            ])
            let active = await orchestrator.activeWindowIDs()
            #expect(!active.contains(id),
                    "a row whose actuation number is \(bad) must be refused")
        }
    }
}

@Suite("SkipOrchestrator Blocked-Gate Guard (playhead-bq70)")
struct SkipOrchestratorBlockedGateGuardTests {

    /// Build a high-confidence AdWindow with the given `eligibilityGate`
    /// raw value. `confidence: 0.85` and `decisionState: "confirmed"`
    /// would, in the absence of the symmetric guard, sail straight into
    /// the auto-skip path under the default thresholds (uiCandidate=0.40,
    /// autoSkip=0.55). The guard's job is to drop these BEFORE
    /// `evaluateAndPush` regardless of confidence — the load-bearing
    /// signal is the gate stamp.
    private func makeBlockedGateAdWindow(
        id: String,
        gateRaw: String,
        startTime: Double = 60,
        endTime: Double = 120,
        decisionState: String = AdDecisionState.confirmed.rawValue
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: "asset-1",
            startTime: startTime,
            endTime: endTime,
            confidence: 0.85,
            boundaryState: "acousticRefined",
            decisionState: decisionState,
            detectorVersion: "fusion-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: startTime,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: gateRaw
        )
    }

    /// Parameter list spans every blocked `SkipEligibilityGate` case.
    /// Acceptance criterion calls for at least 2; we cover all 4 so a
    /// future enum addition that introduces another blocked case
    /// without updating this suite is the only blind spot — and the
    /// source canary catches the asymmetric-guard regression
    /// independently.
    ///
    /// playhead-avbn: the list also carries the PRE-RENAME raw value of
    /// `.blockedByFMConsensus`, because rows written by earlier builds still
    /// hold it in `ad_windows.eligibilityGate`. It must keep decoding to the
    /// same blocked case — if the alias were dropped, the row would fail the
    /// gate decode instead and be attributed to a decode fault rather than to
    /// the FM consensus that actually blocked it.
    @Test(
        "blocked eligibilityGate values do NOT enter active managed-window set",
        arguments: [
            "blockedByEvidenceQuorum",
            "blockedByPolicy",
            "blockedByUserCorrection",
            "blockedByFMConsensus",
            SkipEligibilityGate.legacyFMConsensusRawValue
        ]
    )
    func blockedGateValuesAreDroppedInReceiveAdWindows(gateRaw: String) async throws {
        // Cycle-1 L-2: validate that the parameter raw value is still a
        // recognised `SkipEligibilityGate` case BEFORE running the
        // scenario. Without this, a future rename (e.g.
        // `blockedByPolicy` → `blockedByContentPolicy`) would leave
        // this suite silently passing — the stale string would
        // `flatMap` to nil and fall through the production guard,
        // exercising the WRONG code path while the test still claimed
        // green. Asserting the decode here turns rename drift into a
        // loud test-time failure.
        let decoded = try #require(
            SkipEligibilityGate(rawValue: gateRaw),
            "Parameter `\(gateRaw)` no longer decodes to a SkipEligibilityGate case — enum case may have been renamed; update the parameter list to match."
        )
        #expect(decoded != .eligible,
            "Parameter `\(gateRaw)` decoded to .eligible — only blocked cases belong in this suite.")
        #expect(decoded != .markOnly,
            "Parameter `\(gateRaw)` decoded to .markOnly — that case is exercised by the suggest-tier suite, not this blocked-gate suite.")

        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        // Auto mode + high trust deliberately maximises the auto-skip
        // pressure. If the guard were missing, a confidence-0.85
        // confirmed window in auto/auto-skipping conditions would
        // promote into the cue path AND emit an `.autoSkipped` banner.
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.95,
            observations: 50
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Subscribe BEFORE delivery so an erroneously emitted
        // auto-skip banner can't slip past the test.
        let stream = await orchestrator.bannerItemStream()
        let collectTask = Task<[AdSkipBannerItem], Never> {
            var items: [AdSkipBannerItem] = []
            for await item in stream {
                items.append(item)
            }
            return items
        }

        let windowId = "ad-blocked-\(gateRaw)"
        let window = makeBlockedGateAdWindow(id: windowId, gateRaw: gateRaw)
        await orchestrator.receiveAdWindows([window])
        // playhead-bwxi: walk into [60, 120). Both tier assertions below are
        // NEGATIVE, and since the presentation moved to the position path a
        // negative taken with the playhead never moved would pass for a
        // perfectly healthy window too — it stops discriminating exactly where
        // this suite is supposed to.
        await orchestrator.updatePlayheadTime(70)

        try await Task.sleep(for: .milliseconds(100))
        collectTask.cancel()
        let received = await collectTask.value

        // 1. NOT in the confirmed/active managed-window set.
        let confirmed = await orchestrator.confirmedWindows()
        #expect(
            !confirmed.contains { $0.id == windowId },
            "[\(gateRaw)] blocked-gate window must NOT enter confirmed-windows skip path; got \(confirmed.map(\.id))"
        )

        // 2. NO active window of any decision-state.
        let activeIds = await orchestrator.activeWindowIDs()
        #expect(
            !activeIds.contains(windowId),
            "[\(gateRaw)] blocked-gate window must NOT register an active window id; got \(activeIds)"
        )

        // 3. NO auto-skip banner emitted (suggest-tier OR auto-skipped).
        let auto = received.filter { $0.tier == .autoSkipped && $0.windowId == windowId }
        #expect(
            auto.isEmpty,
            "[\(gateRaw)] blocked-gate window must NOT emit an auto-skip banner; got \(auto)"
        )
        let suggest = received.filter { $0.tier == .suggest && $0.windowId == windowId }
        #expect(
            suggest.isEmpty,
            "[\(gateRaw)] blocked-gate window must NOT emit a suggest-tier banner (only markOnly does); got \(suggest)"
        )

        // 4. emittedAutoSkipBannersSnapshot — the unambiguous emission
        //    witness — confirms no auto-skip banner reached the
        //    yield-to-subscriber path.
        let emitted = await orchestrator.emittedAutoSkipBannersSnapshot()
        #expect(
            !emitted.contains(windowId),
            "[\(gateRaw)] blocked-gate window must NOT register in emitted auto-skip banner snapshot; got \(emitted)"
        )

        // 5. No applied/confirmed decision in the log.
        let log = await orchestrator.getDecisionLog()
        let appliedOrConfirmed = log.filter {
            $0.adWindowId == windowId
                && ($0.decision == .applied || $0.decision == .confirmed)
        }
        #expect(
            appliedOrConfirmed.isEmpty,
            "[\(gateRaw)] blocked-gate window must NOT produce applied/confirmed decisions; got \(appliedOrConfirmed)"
        )

        // 6. playhead-avbn: the window was DROPPED, and dropped FOR THIS REASON.
        //
        // Steps 1-5 are all statements about what did not happen, and since
        // playhead-d3g0 that is no longer enough. A suggest banner is now ARMED
        // at delivery and EMITTED only when the playhead ENTERS the span, so a
        // regression that routed a blocked gate to the suggest tier arms a
        // banner while emitting nothing inside this test's 100 ms window —
        // every assertion above stays green and the span banners in the field
        // the moment playback reaches it. Found by the A11 mutation, which
        // survived the five assertions above.
        //
        // playhead-isp5's census row is the positive witness: it names the
        // terminal disposition and its cause, so "dropped at the blocked-gate
        // guard" and "armed as a suggestion" are no longer the same observation.
        let ingest = await orchestrator.lastAdWindowIngestOutcome(forWindowId: windowId)
        #expect(
            ingest?.outcome == .droppedBlockedGate,
            "[\(gateRaw)] blocked-gate window must record `droppedBlockedGate`; got \(String(describing: ingest?.outcome))"
        )
        // The detail carries the DECODED case, so a row written under the
        // pre-rename raw value is attributed to the gate that blocked it rather
        // than to the string it happened to be stored as.
        #expect(
            ingest?.detail == decoded.rawValue,
            "[\(gateRaw)] the census must name the decoded gate `\(decoded.rawValue)`; got \(String(describing: ingest?.detail))"
        )
        #expect(
            await orchestrator.adWindowIngestOutcomeCount(.armedSuggest) == 0,
            "[\(gateRaw)] blocked-gate window must NOT arm a suggestion — an armed banner fires when the playhead enters the span, long after this test's observation window"
        )
    }

    @Test(
        "malformed eligibilityGate revisions fail closed and disarm the same ID",
        arguments: [
            (label: "empty", gateRaw: ""),
            (label: "unknown", gateRaw: "futureGateName"),
            (label: "padded-legacy-literal", gateRaw: " autoSkip "),
        ]
    )
    func malformedGateRevisionFailsClosed(
        label: String,
        gateRaw: String
    ) async throws {
        #expect(SkipEligibilityGate(rawValue: gateRaw) == nil)
        #expect(gateRaw != "autoSkip")

        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        // playhead-wq34: this suite's subject is the GATE guard — which raw
        // values reach the managed tier — so the show has to be one whose
        // detector classes can use that tier. Without a trust profile the show
        // resolves `.shadow`, and every `.eligible` row is routed to the
        // suggest tier for a reason that has nothing to do with its gate.
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let windowId = "ad-malformed-\(label)"
        let valid = makeBlockedGateAdWindow(
            id: windowId,
            gateRaw: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([valid])
        let activeBefore = await orchestrator.activeWindowIDs()
        #expect(
            activeBefore.contains(windowId),
            "precondition: valid material must be active"
        )

        let malformed = makeBlockedGateAdWindow(
            id: windowId,
            gateRaw: gateRaw
        )
        await orchestrator.receiveAdWindows([malformed])

        let activeAfter = await orchestrator.activeWindowIDs()
        let suggestedAfter = await orchestrator.activeSuggestWindowIDs()
        let confirmedAfter = await orchestrator.confirmedWindows()
        #expect(!activeAfter.contains(windowId))
        #expect(!suggestedAfter.contains(windowId))
        #expect(!confirmedAfter.contains { $0.id == windowId })
    }

    @Test(
        "malformed decisionState revisions fail closed and disarm the same ID",
        arguments: ["", "futureDecisionState"]
    )
    func malformedDecisionStateRevisionFailsClosed(
        decisionState: String
    ) async throws {
        #expect(SkipDecisionState(rawValue: decisionState) == nil)

        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        // playhead-wq34: this suite's subject is the GATE guard — which raw
        // values reach the managed tier — so the show has to be one whose
        // detector classes can use that tier. Without a trust profile the show
        // resolves `.shadow`, and every `.eligible` row is routed to the
        // suggest tier for a reason that has nothing to do with its gate.
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let windowId = "ad-malformed-decision-\(decisionState.isEmpty ? "empty" : "unknown")"
        let valid = makeBlockedGateAdWindow(
            id: windowId,
            gateRaw: SkipEligibilityGate.eligible.rawValue
        )
        await orchestrator.receiveAdWindows([valid])
        let activeBefore = await orchestrator.activeWindowIDs()
        #expect(
            activeBefore.contains(windowId),
            "precondition: valid material must be active"
        )

        let malformed = makeBlockedGateAdWindow(
            id: windowId,
            gateRaw: SkipEligibilityGate.eligible.rawValue,
            decisionState: decisionState
        )
        await orchestrator.receiveAdWindows([malformed])

        let activeAfter = await orchestrator.activeWindowIDs()
        let suggestedAfter = await orchestrator.activeSuggestWindowIDs()
        #expect(!activeAfter.contains(windowId))
        #expect(!suggestedAfter.contains(windowId))
    }

    /// Negative control: the canonical eligible enum case MUST flow
    /// through to `evaluateAndPush` and produce an active managed window.
    /// Without this check, a bug that over-blocked (e.g. a `decoded != nil`
    /// guard that accidentally treated the `.eligible` case as blocked)
    /// would silently disable auto-skip for the only path it's supposed
    /// to be live on.
    @Test("eligible eligibilityGate value DOES enter active managed-window set")
    func eligibleGateFlowsThrough() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        // playhead-wq34: this suite's subject is the GATE guard — which raw
        // values reach the managed tier — so the show has to be one whose
        // detector classes can use that tier. Without a trust profile the show
        // resolves `.shadow`, and every `.eligible` row is routed to the
        // suggest tier for a reason that has nothing to do with its gate.
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let windowId = "ad-eligible-flow"
        let window = AdWindow(
            id: windowId,
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            boundaryState: "acousticRefined",
            decisionState: "confirmed",
            detectorVersion: "fusion-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: 60,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: "eligible"
        )

        await orchestrator.receiveAdWindows([window])

        // playhead-wq34: read the MANAGED DICTIONARY, which is what this test's
        // own name says it is about. `confirmedWindows()` filters to
        // `.confirmed`, and on the `.auto` show this suite now needs, an
        // eligible row promotes straight to `.applied` — so that accessor
        // cannot see the window it is describing (the same playhead-ugy4 lens
        // error called out in SkipOrchestratorCharacterizationTests).
        let active = await orchestrator.activeWindowIDs()
        #expect(
            active.contains(windowId),
            "eligible-gate window must enter the managed-window set; got \(active)"
        )
    }
}
