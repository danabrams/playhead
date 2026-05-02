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
        endTime: Double = 120
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: "asset-1",
            startTime: startTime,
            endTime: endTime,
            confidence: 0.85,
            boundaryState: "acousticRefined",
            decisionState: "confirmed",
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
    @Test(
        "blocked eligibilityGate values do NOT enter active managed-window set",
        arguments: [
            "blockedByEvidenceQuorum",
            "blockedByPolicy",
            "blockedByUserCorrection",
            "cappedByFMSuppression"
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
        nonisolated(unsafe) var received: [AdSkipBannerItem] = []
        let collectTask = Task {
            for await item in stream {
                received.append(item)
            }
        }

        let windowId = "ad-blocked-\(gateRaw)"
        let window = makeBlockedGateAdWindow(id: windowId, gateRaw: gateRaw)
        await orchestrator.receiveAdWindows([window])

        try await Task.sleep(for: .milliseconds(100))
        collectTask.cancel()

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
        let orchestrator = SkipOrchestrator(store: store)
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

        let confirmed = await orchestrator.confirmedWindows()
        #expect(
            confirmed.contains { $0.id == windowId },
            "eligible-gate window must enter confirmed-windows skip path; got \(confirmed.map(\.id))"
        )
    }
}
