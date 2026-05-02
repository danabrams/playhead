import CoreMedia
import Foundation
import Testing
@testable import Playhead

private func makeSkipTestFeatureWindow(
    assetId: String = "asset-1",
    startTime: Double = 0,
    endTime: Double = 1,
    musicProbability: Double = 0.0,
    speakerChangeProxyScore: Double = 0.0,
    musicBedChangeScore: Double = 0.0,
    pauseProbability: Double = 0.1,
    rms: Double = 0.05
) -> FeatureWindow {
    FeatureWindow(
        analysisAssetId: assetId,
        startTime: startTime,
        endTime: endTime,
        rms: rms,
        spectralFlux: 0.01,
        musicProbability: musicProbability,
        speakerChangeProxyScore: speakerChangeProxyScore,
        musicBedChangeScore: musicBedChangeScore,
        pauseProbability: pauseProbability,
        speakerClusterId: nil,
        jingleHash: nil,
        featureVersion: 1
    )
}

private func makeSkipTestPodcastProfile(
    podcastId: String = "podcast-1",
    mode: String = "shadow",
    trustScore: Double = 0.5,
    observations: Int = 0,
    falseSignals: Int = 0
) -> PodcastProfile {
    PodcastProfile(
        podcastId: podcastId,
        sponsorLexicon: nil,
        normalizedAdSlotPriors: nil,
        repeatedCTAFragments: nil,
        jingleFingerprints: nil,
        implicitFalsePositiveCount: 0,
        skipTrustScore: trustScore,
        observationCount: observations,
        mode: mode,
        recentFalseSkipSignals: falseSignals
    )
}

private func makePendingAdDecisionResult(
    id: String,
    analysisAssetId: String = "asset-1",
    startTime: Double = 60,
    endTime: Double = 120,
    skipConfidence: Double,
    eligibilityGate: AdDecisionEligibilityGate,
    recomputationRevision: Int = 1
) -> AdDecisionResult {
    AdDecisionResult(
        id: id,
        analysisAssetId: analysisAssetId,
        startTime: startTime,
        endTime: endTime,
        skipConfidence: skipConfidence,
        eligibilityGate: eligibilityGate,
        recomputationRevision: recomputationRevision
    )
}

@Suite("SkipOrchestrator Characterization - Hysteresis and Gap Merging")
struct SkipOrchestratorCharacterizationHysteresisTests {

    @Test("Window below enter threshold is suppressed in auto mode")
    func belowEnterThreshold() async throws {
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

        let lowConfidenceWindow = makeSkipTestAdWindow(
            id: "ad-low",
            confidence: 0.3,
            decisionState: "candidate"
        )
        await orchestrator.receiveAdWindows([lowConfidenceWindow])

        let log = await orchestrator.getDecisionLog()
        let suppressed = log.filter { $0.decision == .suppressed }
        #expect(!suppressed.isEmpty)
    }

    @Test("Window above enter threshold in auto mode is applied")
    func aboveEnterThreshold() async throws {
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

        let highConfidenceWindow = makeSkipTestAdWindow(
            id: "ad-high",
            startTime: 60,
            endTime: 120,
            confidence: 0.8,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([highConfidenceWindow])

        let log = await orchestrator.getDecisionLog()
        let applied = log.filter { $0.decision == .applied }
        #expect(!applied.isEmpty)
    }

    @Test("Retiring stale candidate ids removes them from active orchestration")
    func retireStaleCandidateIds() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "shadow",
            trustScore: 0.5,
            observations: 0
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let introWindow = makeSkipTestAdWindow(
            id: "ad-intro",
            startTime: 60,
            endTime: 90,
            confidence: 0.75,
            decisionState: "candidate"
        )
        let closeWindow = makeSkipTestAdWindow(
            id: "ad-close",
            startTime: 100,
            endTime: 120,
            confidence: 0.75,
            decisionState: "candidate"
        )
        await orchestrator.receiveAdWindows([introWindow, closeWindow])
        #expect(await orchestrator.activeWindowIDs() == Set(["ad-intro", "ad-close"]))

        await orchestrator.retireAdWindows(ids: ["ad-close"])

        #expect(await orchestrator.activeWindowIDs() == Set(["ad-intro"]))
    }

    @Test("Seek suppresses auto-skip temporarily")
    func seekSuppression() async throws {
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

        await orchestrator.recordUserSeek(to: 50)
        let ad = makeSkipTestAdWindow(
            id: "ad-seek",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        let log = await orchestrator.getDecisionLog()
        let applied = log.filter { $0.decision == .applied }
        #expect(applied.isEmpty)
    }

    @Test("Listen revert sets state to reverted")
    func listenRevert() async throws {
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
            id: "ad-revert",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        await orchestrator.recordListenRevert(
            windowId: "ad-revert",
            podcastId: "podcast-1"
        )

        let log = await orchestrator.getDecisionLog()
        let reverted = log.filter { $0.decision == .reverted }
        #expect(!reverted.isEmpty)
    }

    @Test("Shadow mode confirms but never applies")
    func shadowModeNoSkip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())

        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        let ad = makeSkipTestAdWindow(
            id: "ad-shadow",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        let log = await orchestrator.getDecisionLog()
        let applied = log.filter { $0.decision == .applied }
        let confirmed = log.filter { $0.decision == .confirmed }
        #expect(applied.isEmpty)
        #expect(!confirmed.isEmpty)
    }

    @Test("Manual mode confirms but does not auto-apply")
    func manualModeNoAutoSkip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "manual",
            trustScore: 0.6,
            observations: 5
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-manual",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        let log = await orchestrator.getDecisionLog()
        let applied = log.filter { $0.decision == .applied }
        #expect(applied.isEmpty)

        let confirmed = await orchestrator.confirmedWindows()
        #expect(!confirmed.isEmpty)
    }

    @Test("Manual skip applies a confirmed window")
    func manualSkipApplies() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "manual",
            trustScore: 0.6,
            observations: 5
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-mskip",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        await orchestrator.applyManualSkip(windowId: "ad-mskip")

        let log = await orchestrator.getDecisionLog()
        let applied = log.filter {
            $0.decision == .applied && $0.reason == "Manual skip by user"
        }
        #expect(!applied.isEmpty)
    }

    @Test("Short window below minimum span is suppressed")
    func shortWindowSuppressed() async throws {
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

        let shortAd = makeSkipTestAdWindow(
            id: "ad-short",
            startTime: 60,
            endTime: 65,
            confidence: 0.7,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([shortAd])

        let log = await orchestrator.getDecisionLog()
        let suppressed = log.filter { $0.decision == .suppressed }
        #expect(!suppressed.isEmpty)
    }

    @Test("Adjacent auto-mode windows within merge gap collapse into one cue")
    func adjacentWindowsMergeIntoSingleCue() async throws {
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

        let firstWindow = makeSkipTestAdWindow(
            id: "ad-merge-1",
            startTime: 60,
            endTime: 90,
            confidence: 0.9,
            decisionState: "confirmed"
        )
        let secondWindow = makeSkipTestAdWindow(
            id: "ad-merge-2",
            startTime: 92,
            endTime: 120,
            confidence: 0.88,
            decisionState: "confirmed"
        )

        await orchestrator.receiveAdWindows([firstWindow, secondWindow])

        let currentCues = pushedCues
        // playhead-vn7n.2: pod-end is pulled in by the trailing cushion.
        let cushion = SkipPolicyConfig.default.adTrailingCushionSeconds
        #expect(currentCues.count == 1)
        if let mergedCue = currentCues.first {
            #expect(CMTimeGetSeconds(mergedCue.start) == 60)
            #expect(CMTimeGetSeconds(mergedCue.start + mergedCue.duration) == 120 - cushion)
        } else {
            Issue.record("Expected a merged skip cue for adjacent windows inside the merge gap")
        }
    }

    @Test("Windows beyond the merge gap stay as separate cues")
    func windowsOutsideMergeGapStaySeparate() async throws {
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

        let firstWindow = makeSkipTestAdWindow(
            id: "ad-separate-1",
            startTime: 60,
            endTime: 90,
            confidence: 0.9,
            decisionState: "confirmed"
        )
        let secondWindow = makeSkipTestAdWindow(
            id: "ad-separate-2",
            startTime: 95,
            endTime: 120,
            confidence: 0.88,
            decisionState: "confirmed"
        )

        await orchestrator.receiveAdWindows([firstWindow, secondWindow])

        let currentCues = pushedCues
        // playhead-vn7n.2: each pod gets its own trailing cushion.
        let cushion = SkipPolicyConfig.default.adTrailingCushionSeconds
        #expect(currentCues.count == 2)
        if currentCues.count == 2 {
            #expect(CMTimeGetSeconds(currentCues[0].start) == 60)
            #expect(CMTimeGetSeconds(currentCues[0].start + currentCues[0].duration) == 90 - cushion)
            #expect(CMTimeGetSeconds(currentCues[1].start) == 95)
            #expect(CMTimeGetSeconds(currentCues[1].start + currentCues[1].duration) == 120 - cushion)
        }
    }
}

// MARK: - Phase 7.2: User Correction Store Wiring

@Suite("SkipOrchestrator - recordListenRevert writes CorrectionEvent")
struct SkipOrchestratorCorrectionStoreTests {

    @Test("recordListenRevert persists a listenRevert CorrectionEvent")
    func recordListenRevertWritesCorrectionEvent() async throws {
        let analysisStore = try await makeTestStore()
        try await analysisStore.insertAsset(makeSkipTestAnalysisAsset())

        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: analysisStore,
            trustService: trustService,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Feed a confirmed window so recordListenRevert has something to revert.
        let ad = makeSkipTestAdWindow(
            id: "ad-correction",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await analysisStore.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        // Revert the skip — this should write a CorrectionEvent.
        await orchestrator.recordListenRevert(
            windowId: "ad-correction",
            podcastId: "podcast-1"
        )

        // The correction store write happens in a fire-and-forget Task.
        // Poll briefly to let it complete.
        let found = try await pollUntil(timeout: .seconds(5)) {
            let events = try await correctionStore.activeCorrections(for: "asset-1")
            return !events.isEmpty
        }
        #expect(found, "Expected a CorrectionEvent to be written after recordListenRevert")

        let events = try await correctionStore.activeCorrections(for: "asset-1")
        #expect(events.count == 1)
        let event = events[0]
        #expect(event.source == .listenRevert)
        #expect(event.podcastId == "podcast-1")
        #expect(event.analysisAssetId == "asset-1")
    }
}

@Suite("SkipOrchestrator Characterization - Finalized Boundaries")
struct SkipOrchestratorCharacterizationFinalizedBoundaryTests {

    @Test("Boundaries are forwarded as-is (snapping is upstream)")
    func boundariesPassThroughWithoutModification() async throws {
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

        // Use non-round boundary times to verify they are not modified.
        let ad = makeSkipTestAdWindow(
            id: "ad-snap",
            startTime: 61.347,
            endTime: 119.892,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        let log = await orchestrator.getDecisionLog()
        let record = try #require(log.first, "Expected a decision log record for the window")
        #expect(record.originalStart == 61.347)
        #expect(record.snappedStart == 61.347, "SkipOrchestrator should not modify boundaries — snapping is upstream")
        #expect(record.originalEnd == 119.892)
        #expect(record.snappedEnd == 119.892, "SkipOrchestrator should not modify boundaries — snapping is upstream")
    }
}

// playhead-4my.6.4: tests re-enabled and implemented with real assertions.
@Suite("SkipOrchestrator Contract - AdDecisionResult")
struct SkipOrchestratorAdDecisionContractTests {

    @Test("Blocked gate never skips regardless of confidence")
    func blockedGateNeverSkips() async throws {
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

        let blockedDecision = makePendingAdDecisionResult(
            id: "blocked-high-confidence",
            skipConfidence: 0.99,
            eligibilityGate: .blocked
        )

        // Preconditions: the blocked span IS above the auto-skip threshold.
        #expect(blockedDecision.skipConfidence > SkipPolicyConfig.default.enterThreshold)
        #expect(blockedDecision.eligibilityGate == .blocked)

        await orchestrator.receiveAdDecisionResults([blockedDecision])

        // Blocked gate must never produce an applied decision.
        let log = await orchestrator.getDecisionLog()
        let applied = log.filter { $0.decision == .applied }
        #expect(applied.isEmpty, "Blocked gate must never produce an applied skip, even at skipConfidence=0.99")
    }

    @Test("Eligible gate uses skipConfidence through existing hysteresis")
    func eligibleGateUsesSkipConfidenceHysteresis() async throws {
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

        // Enter span: above enterThreshold → auto mode should apply.
        let enterSpan = makePendingAdDecisionResult(
            id: "eligible-enter",
            startTime: 60,
            endTime: 120,
            skipConfidence: 0.8,
            eligibilityGate: .eligible
        )

        #expect(enterSpan.skipConfidence > SkipPolicyConfig.default.enterThreshold)
        await orchestrator.receiveAdDecisionResults([enterSpan])

        let log = await orchestrator.getDecisionLog()
        let applied = log.filter { $0.decision == .applied }
        #expect(!applied.isEmpty, "Eligible span above enterThreshold in auto mode must be applied")
        if let record = applied.first(where: { $0.adWindowId == "eligible-enter" }) {
            #expect(record.originalStart == 60.0)
            #expect(record.snappedStart == 60.0)
            #expect(record.originalEnd == 120.0)
            #expect(record.snappedEnd == 120.0)
        } else {
            Issue.record("Expected an applied decision record for eligible-enter")
        }
    }

    @Test("Decision recomputation stays stable for unchanged spans")
    func decisionRecomputationStaysStable() async throws {
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

        let initialDecision = makePendingAdDecisionResult(
            id: "eligible-stable",
            startTime: 60,
            endTime: 120,
            skipConfidence: 0.82,
            eligibilityGate: .eligible,
            recomputationRevision: 1
        )
        let recomputedDecision = makePendingAdDecisionResult(
            id: "eligible-stable",
            startTime: 60,
            endTime: 120,
            skipConfidence: 0.82,
            eligibilityGate: .eligible,
            recomputationRevision: 2
        )

        #expect(initialDecision.id == recomputedDecision.id)
        #expect(initialDecision.skipConfidence == recomputedDecision.skipConfidence)
        #expect(initialDecision.eligibilityGate == recomputedDecision.eligibilityGate)
        #expect(initialDecision.recomputationRevision < recomputedDecision.recomputationRevision)

        // Send both initial and recomputed (same id, same confidence).
        // The second should not oscillate state — window ends in applied exactly once.
        await orchestrator.receiveAdDecisionResults([initialDecision, recomputedDecision])

        let log = await orchestrator.getDecisionLog()
        let appliedEntries = log.filter { $0.adWindowId == "eligible-stable" && $0.decision == .applied }
        // The window should end in applied state (not oscillating between applied/suppressed).
        #expect(!appliedEntries.isEmpty, "Recomputed span with same confidence must stay applied")
        // Suppress duplicates: the window should appear in applied at most once per evaluation cycle.
        // We allow one entry per evaluateAndPush call (2 calls = 2 entries max).
        #expect(appliedEntries.count <= 2, "Must not oscillate: applied log entries for same id should not multiply unboundedly")
    }

    @Test("Manual and shadow modes stay non-auto after AdDecisionResult contract")
    func manualAndShadowModesStayNonAuto() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())

        // Shadow orchestrator (no trust service → defaults to .shadow mode).
        let shadowOrchestrator = SkipOrchestrator(store: store)
        await shadowOrchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        let manualTrust = try await makeSkipTestTrustService(
            mode: "manual",
            trustScore: 0.6,
            observations: 5
        )
        let manualOrchestrator = SkipOrchestrator(store: store, trustService: manualTrust)
        await manualOrchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let eligibleDecision = makePendingAdDecisionResult(
            id: "eligible-non-auto",
            skipConfidence: 0.9,
            eligibilityGate: .eligible
        )

        await shadowOrchestrator.receiveAdDecisionResults([eligibleDecision])
        await manualOrchestrator.receiveAdDecisionResults([eligibleDecision])

        // Shadow mode: no auto-skip, window should be confirmed but not applied.
        let shadowLog = await shadowOrchestrator.getDecisionLog()
        let shadowApplied = shadowLog.filter { $0.decision == .applied }
        #expect(shadowApplied.isEmpty, "Shadow mode must never auto-apply, even for eligible AdDecisionResults")

        // Manual mode: no auto-skip, window should be confirmed awaiting user action.
        let manualLog = await manualOrchestrator.getDecisionLog()
        let manualApplied = manualLog.filter { $0.decision == .applied }
        #expect(manualApplied.isEmpty, "Manual mode must never auto-apply AdDecisionResults")
        let manualConfirmed = await manualOrchestrator.confirmedWindows()
        #expect(!manualConfirmed.isEmpty, "Manual mode must expose eligible AdDecisionResult spans as confirmed windows")
    }

    @Test("Fusion result with same id as an open suggest entry clears the suggest entry (playhead-rfu-sad)")
    func fusionResultClearsSharedIdSuggestEntry() async throws {
        // M2 race scenario: an AdWindow first arrives stamped
        // `markOnly` and lands in the suggest tier. Later the fusion
        // pipeline emits an `AdDecisionResult` with the SAME id and
        // `eligibilityGate = .eligible`. Without a symmetric clear in
        // `receiveAdDecisionResults`, `suggestWindows[id]` would stay
        // populated alongside the new managed window, and a still-
        // visible suggest banner could re-fire `acceptSuggestedSkip`
        // and synthesize a duplicate managed window via
        // `UUID().uuidString`.
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "manual",
            trustScore: 0.6,
            observations: 5
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // 1. Same-id markOnly arrives via the AdWindow path → suggest tier.
        let markOnly = AdWindow(
            id: "ad-shared-id",
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.45,
            boundaryState: "lexical",
            decisionState: "candidate",
            detectorVersion: "detection-v1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: 60,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: "markOnly"
        )
        await orchestrator.receiveAdWindows([markOnly])

        // 2. Fusion produces an eligible decision under the same id.
        let fusionDecision = makePendingAdDecisionResult(
            id: "ad-shared-id",
            startTime: 60,
            endTime: 120,
            skipConfidence: 0.85,
            eligibilityGate: .eligible
        )
        await orchestrator.receiveAdDecisionResults([fusionDecision])

        // 3. A late accept on the original (now-stale) suggest banner
        //    must be a no-op — the suggest entry was cleared by the
        //    fusion path. If the symmetric clear was missing, this
        //    call would synthesize a parallel UUID-keyed managed
        //    window.
        await orchestrator.acceptSuggestedSkip(windowId: "ad-shared-id")

        let confirmed = await orchestrator.confirmedWindows()
        let onSpan = confirmed.filter { $0.startTime == 60 && $0.endTime == 120 }
        #expect(onSpan.count == 1,
            "Exactly one managed window should cover the span (the fusion-managed one); got \(onSpan.count)")
        #expect(onSpan.first?.id == "ad-shared-id",
            "The surviving window must be the fusion-managed entry, not a UUID-keyed late promotion")
    }
}

// MARK: - Banner Item Stream Tests

@Suite("SkipOrchestrator Banner Item Stream")
struct SkipOrchestratorBannerItemStreamTests {

    @Test("Confirmed window in shadow mode emits a banner item")
    func confirmedWindowEmitsBanner() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Shadow mode is the default — windows reach .confirmed but not .applied.
        let stream = await orchestrator.bannerItemStream()

        let window = makeSkipTestAdWindow(
            id: "ad-banner-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.80,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([window])

        // Collect one item from the stream with a bounded timeout.
        nonisolated(unsafe) var received: AdSkipBannerItem?
        let collectTask = Task {
            for await item in stream {
                received = item
                break
            }
        }
        // Give the actor time to process and emit.
        try await Task.sleep(for: .milliseconds(100))
        collectTask.cancel()

        let item = try #require(received, "Expected a banner item for a confirmed window")
        #expect(item.windowId == "ad-banner-1")
        #expect(item.adStartTime == 60)
        #expect(item.adEndTime == 120)
        #expect(item.podcastId == "podcast-1")
    }

    @Test("Applied window in auto mode emits a banner item")
    func appliedWindowEmitsBanner() async throws {
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
            podcastId: "podcast-2"
        )

        let stream = await orchestrator.bannerItemStream()

        let window = makeSkipTestAdWindow(
            id: "ad-banner-auto",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([window])

        nonisolated(unsafe) var received: AdSkipBannerItem?
        let collectTask = Task {
            for await item in stream {
                received = item
                break
            }
        }
        try await Task.sleep(for: .milliseconds(100))
        collectTask.cancel()

        let item = try #require(received, "Expected a banner item for an applied window")
        #expect(item.windowId == "ad-banner-auto")
        #expect(item.adStartTime == 60)
        #expect(item.adEndTime == 120)
        #expect(item.podcastId == "podcast-2")
    }

    @Test("Banner is emitted only once per window across repeated evaluations")
    func bannerEmittedOnlyOnce() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let stream = await orchestrator.bannerItemStream()

        let window = makeSkipTestAdWindow(
            id: "ad-once",
            startTime: 60,
            endTime: 120,
            confidence: 0.80,
            decisionState: "confirmed"
        )

        // Deliver the same window twice (simulates re-evaluation from detection).
        await orchestrator.receiveAdWindows([window])
        await orchestrator.receiveAdWindows([window])

        // Collect up to two items, but expect exactly one.
        nonisolated(unsafe) var count = 0
        let collectTask = Task {
            for await _ in stream {
                count += 1
                if count >= 2 { break }
            }
        }
        try await Task.sleep(for: .milliseconds(150))
        collectTask.cancel()

        #expect(count == 1, "Banner must fire only once per window, got \(count)")
    }

    @Test("Suppressed window does not emit a banner")
    func suppressedWindowNoBanner() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let stream = await orchestrator.bannerItemStream()

        // Low confidence — will be suppressed by skip policy.
        let window = makeSkipTestAdWindow(
            id: "ad-suppressed",
            startTime: 60,
            endTime: 120,
            confidence: 0.2,
            decisionState: "candidate"
        )
        await orchestrator.receiveAdWindows([window])

        nonisolated(unsafe) var received: AdSkipBannerItem?
        let collectTask = Task {
            for await item in stream {
                received = item
                break
            }
        }
        try await Task.sleep(for: .milliseconds(100))
        collectTask.cancel()

        #expect(received == nil, "Suppressed windows must not emit banners")
    }
}

// MARK: - Suggest-Tier (markOnly) Banner Tests — playhead-gtt9.23
//
// Acceptance criterion for playhead-gtt9.23:
//   "Unit test fixture: medium-confidence detection produces banner +
//    skip-affordance, no auto-skip."
//
// `eligibilityGate == "markOnly"` is the gate stamp that the AutoSkipPrecisionGate
// applies to medium-confidence windows (between the uiCandidate and autoSkip
// thresholds). Before this bead these windows were silently dropped on the
// floor — the orchestrator logged "not adding to active windows" and that
// was the end of the story. The bead's job is to surface them as a
// `.suggest`-tier banner without putting them in the skip-cue path. These
// tests pin that behaviour from both directions: the suggest banner must
// fire AND no skip cue must be pushed.

@Suite("SkipOrchestrator Suggest-Tier (markOnly) Banner")
struct SkipOrchestratorSuggestTierTests {

    /// Build a markOnly AdWindow at medium confidence. The factory in
    /// TestHelpers does not expose `eligibilityGate`, so we inline the
    /// init here. `confidence: 0.45` sits in the suggest band (default
    /// uiCandidate=0.40, autoSkip=0.55) — the gate decision lives in
    /// AdDetectionService; once a window arrives at the orchestrator the
    /// stamp is what's load-bearing, not the score itself.
    private func makeMarkOnlyAdWindow(
        id: String = "ad-suggest-1",
        startTime: Double = 60,
        endTime: Double = 120
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: "asset-1",
            startTime: startTime,
            endTime: endTime,
            confidence: 0.45,
            boundaryState: "lexical",
            decisionState: "candidate",
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: startTime,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: "markOnly"
        )
    }

    @Test("markOnly window emits a suggest-tier banner")
    func markOnlyEmitsSuggestBanner() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let stream = await orchestrator.bannerItemStream()

        let window = makeMarkOnlyAdWindow(id: "ad-suggest-emit")
        await orchestrator.receiveAdWindows([window])

        nonisolated(unsafe) var received: AdSkipBannerItem?
        let collectTask = Task {
            for await item in stream {
                received = item
                break
            }
        }
        try await Task.sleep(for: .milliseconds(100))
        collectTask.cancel()

        let item = try #require(received,
            "markOnly windows must surface as a suggest-tier banner (playhead-gtt9.23)")
        #expect(item.windowId == "ad-suggest-emit")
        #expect(item.tier == .suggest,
            "Banner emitted for a markOnly window must be tier=.suggest, not .autoSkipped")
        #expect(item.adStartTime == 60)
        #expect(item.adEndTime == 120)
    }

    @Test("markOnly window does NOT auto-skip in auto trust mode")
    func markOnlyDoesNotAutoSkip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        // Auto mode + high trust would happily auto-skip an *eligible*
        // window. The gate stamp must be authoritative — even with the
        // most permissive trust, a markOnly window stays out of the
        // skip-cue path.
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

        let window = makeMarkOnlyAdWindow(id: "ad-suggest-noskip")
        await orchestrator.receiveAdWindows([window])

        // No active window should exist for a markOnly stamp — the
        // orchestrator stores it in the parallel `suggestWindows`
        // dictionary, not the skip-evaluation `windows` map.
        let confirmed = await orchestrator.confirmedWindows()
        #expect(!confirmed.contains { $0.id == "ad-suggest-noskip" },
            "markOnly window must not enter the confirmed-windows skip path")

        // No applied/confirmed decision should be in the log either.
        let log = await orchestrator.getDecisionLog()
        let appliedOrConfirmed = log.filter {
            $0.adWindowId == "ad-suggest-noskip"
                && ($0.decision == .applied || $0.decision == .confirmed)
        }
        #expect(appliedOrConfirmed.isEmpty,
            "markOnly window must not produce applied/confirmed decisions; got \(appliedOrConfirmed)")
    }

    // C27 cycle-2/3 missing test: positive control for the L3 fix in
    // `SkipOrchestrator.receiveAdWindows`. The fix decodes
    // `AdWindow.eligibilityGate` through `SkipEligibilityGate(rawValue:)`
    // and routes only `.markOnly` to the suggest tier. Every other value
    // (nil, empty string, the producer-specific "autoSkip" literal, and
    // unknown raw values) decodes to a non-`.markOnly` result and must
    // fall through to the standard managed-window path. A future
    // regression that stringly-treats one of these as a suggest-tier
    // marker would silently drop high-confidence ads out of the
    // auto-skip path.
    //
    // Cycle-3 L-1: parameterized over the non-`.markOnly` raw-value
    // space the L3 decode is supposed to handle. The fusion-path
    // blocked `SkipEligibilityGate` cases (`.blockedByPolicy` etc.) are
    // deliberately NOT included here — the symmetric blocked-gate guard
    // in `receiveAdWindows` (playhead-bq70) drops them BEFORE the
    // standard managed path, so they are exercised by the dedicated
    // blocked-gate suite (`SkipOrchestratorBlockedGateGuardTests`)
    // rather than this fall-through suite. The values here all decode
    // to nil (nil-stamp, "" stamp, "autoSkip", unknown future label) or
    // to `.eligible` (the canonical eligible enum case), and must
    // therefore continue to flow through to the standard managed path.
    //
    // Cycle-4 L-1: `"eligible"` (the legitimate
    // `SkipEligibilityGate.eligible.rawValue`) is included as the
    // canonical "valid non-markOnly enum case" — a future producer
    // change that emitted `"eligible"` in place of `"autoSkip"` (e.g. to
    // align the precision-gate label with the enum) MUST still flow
    // through the standard managed path; this parameter pins that.
    //
    // Cycle-6 missing test: pin the "unknown raw value" arm explicitly.
    // The L3 decode comment asserts that ANY non-`.markOnly` raw value
    // (including unknown future values) falls through. The other
    // parameter cases all happen to map to known shapes (nil → nil,
    // "" → nil, "autoSkip" → nil, "eligible" → .eligible). A
    // gibberish-but-non-empty value is the case that exercises the
    // `SkipEligibilityGate(rawValue:)` returning nil for an unknown
    // future label, which then `flatMap` collapses to nil and falls
    // through the `decodedGate == .markOnly` guard.
    @Test(
        "non-markOnly eligibilityGate values fall through to standard managed path, NOT suggest tier",
        arguments: [
            (label: "nil-stamp",                 gate: String?.none),
            (label: "empty-string-stamp",         gate: String?.some("")),
            (label: "autoSkip-stamp",             gate: String?.some("autoSkip")),
            (label: "eligible-stamp",             gate: String?.some("eligible")),
            (label: "unknown-future-value-stamp", gate: String?.some("futureGateName"))
        ]
    )
    func nonMarkOnlyGateEntersStandardSkipPath(label: String, gate: String?) async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Inline-build the AdWindow at high confidence with
        // `decisionState: "confirmed"` so a successful entry into the
        // standard path is observable via `confirmedWindows()`. The
        // factory in TestHelpers does not expose `eligibilityGate`.
        let windowId = "ad-non-markonly-\(label)"
        let window = AdWindow(
            id: windowId,
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            boundaryState: "lexical",
            decisionState: "confirmed",
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: 60,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: gate
        )

        // Subscribe to the banner stream BEFORE delivery so a (wrongly
        // emitted) suggest-tier banner can't slip through unnoticed.
        let stream = await orchestrator.bannerItemStream()
        nonisolated(unsafe) var receivedBanners: [AdSkipBannerItem] = []
        let collectTask = Task {
            for await item in stream {
                receivedBanners.append(item)
            }
        }

        await orchestrator.receiveAdWindows([window])

        // Allow any banner-stream yields to drain.
        try await Task.sleep(for: .milliseconds(100))
        collectTask.cancel()

        // Positive: the window IS in the standard managed path.
        let confirmed = await orchestrator.confirmedWindows()
        #expect(
            confirmed.contains { $0.id == windowId },
            "[\(label)] eligibilityGate=\(String(describing: gate)) must enter standard confirmed-windows path; got \(confirmed.map(\.id))"
        )

        // Negative: NO suggest-tier banner emitted.
        let suggestBanners = receivedBanners.filter { $0.tier == .suggest }
        #expect(
            suggestBanners.isEmpty,
            "[\(label)] eligibilityGate=\(String(describing: gate)) must NOT emit a suggest-tier banner; got \(suggestBanners)"
        )
    }

    @Test("acceptSuggestedSkip promotes window to confirmed and clears suggest state")
    func acceptSuggestedSkipConfirmsWindow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let window = makeMarkOnlyAdWindow(id: "ad-suggest-accept")
        await orchestrator.receiveAdWindows([window])

        // Pre-condition: not yet in the confirmed set.
        let confirmedBefore = await orchestrator.confirmedWindows()
        #expect(!confirmedBefore.contains { $0.id == "ad-suggest-accept" })

        await orchestrator.acceptSuggestedSkip(windowId: "ad-suggest-accept")

        // Post-condition: a confirmed window covering the same span now
        // exists. The orchestrator allocates a fresh promoted-window id so
        // the eligibilityGate stamp on the original markOnly window can't
        // re-block it; we match by span rather than id.
        let confirmedAfter = await orchestrator.confirmedWindows()
        let match = confirmedAfter.first {
            $0.startTime == window.startTime && $0.endTime == window.endTime
        }
        let promoted = try #require(match,
            "acceptSuggestedSkip must promote the suggest window into the confirmed set")
        #expect(promoted.confidence == 1.0,
            "User-confirmed skip should pin confidence to 1.0; got \(promoted.confidence)")
    }

    @Test("acceptSuggestedSkip is a no-op when window is unknown")
    func acceptSuggestedSkipUnknownIsNoOp() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // No suggest window has ever been registered — accepting a
        // phantom id must not crash, must not poison state, must not
        // synthesize a window.
        await orchestrator.acceptSuggestedSkip(windowId: "ad-never-existed")

        let confirmed = await orchestrator.confirmedWindows()
        #expect(confirmed.isEmpty,
            "acceptSuggestedSkip on an unknown windowId must be a clean no-op")
    }

    @Test("declineSuggestedSkip drops the window without confirming it")
    func declineSuggestedSkipDoesNotConfirm() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let window = makeMarkOnlyAdWindow(id: "ad-suggest-decline")
        await orchestrator.receiveAdWindows([window])

        await orchestrator.declineSuggestedSkip(windowId: "ad-suggest-decline")

        let confirmed = await orchestrator.confirmedWindows()
        #expect(confirmed.isEmpty,
            "declineSuggestedSkip must not promote the window into the skip path")

        // Subsequent accept on the same id is now a no-op — the suggest
        // entry has been cleared. (This protects against a stale tap
        // arriving after the user has dismissed the banner.)
        await orchestrator.acceptSuggestedSkip(windowId: "ad-suggest-decline")
        let confirmedAfter = await orchestrator.confirmedWindows()
        #expect(confirmedAfter.isEmpty,
            "Accept after decline must be a no-op — the suggest window is gone")
    }

    @Test("Suggest banner fires only once per markOnly window")
    func suggestBannerEmittedOnlyOnce() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let stream = await orchestrator.bannerItemStream()

        let window = makeMarkOnlyAdWindow(id: "ad-suggest-once")
        // Deliver the same markOnly window twice; only one banner should
        // fire. Same dedupe contract as the auto-skipped path.
        await orchestrator.receiveAdWindows([window])
        await orchestrator.receiveAdWindows([window])

        nonisolated(unsafe) var count = 0
        let collectTask = Task {
            for await _ in stream {
                count += 1
                if count >= 2 { break }
            }
        }
        try await Task.sleep(for: .milliseconds(150))
        collectTask.cancel()

        #expect(count == 1,
            "Suggest banner must dedupe across repeated markOnly deliveries; got \(count)")
    }

    @Test("Gate flip from markOnly clears suggest entry — accept after flip is a no-op (playhead-rfu-sad)")
    func gateFlipClearsSuggestEntry() async throws {
        // Race scenario: a window arrives first stamped `markOnly`
        // (suggest tier), then a later detection pass re-emits the same
        // window id with the gate cleared (eligible for auto-skip). If
        // the suggest entry isn't cleared, a still-visible suggest
        // banner could re-fire `acceptSuggestedSkip`, which would
        // synthesize a duplicate managed window via a fresh
        // `UUID().uuidString` and silently corrupt state.
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "manual",
            trustScore: 0.6,
            observations: 5
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // 1. Window arrives as markOnly → enters suggestWindows.
        let markOnly = makeMarkOnlyAdWindow(id: "ad-gate-flip")
        await orchestrator.receiveAdWindows([markOnly])

        // 2. Same id re-arrives, this time WITHOUT a markOnly stamp
        //    (eligibilityGate=nil simulates a gate clear). The
        //    orchestrator must drop the suggest entry before
        //    materializing the managed window.
        let promotedSameId = AdWindow(
            id: "ad-gate-flip",
            analysisAssetId: "asset-1",
            startTime: markOnly.startTime,
            endTime: markOnly.endTime,
            confidence: 0.85,
            boundaryState: "lexical",
            decisionState: "confirmed",
            detectorVersion: "detection-v1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: markOnly.startTime,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: nil
        )
        await orchestrator.receiveAdWindows([promotedSameId])

        // 3. Confirmed window for this id should now exist (manual mode
        //    keeps it at .confirmed).
        let confirmedAfterFlip = await orchestrator.confirmedWindows()
        #expect(confirmedAfterFlip.contains { $0.id == "ad-gate-flip" },
            "After gate flip, the same id must enter the managed-window set")

        // 4. A late `acceptSuggestedSkip` call (e.g. a stale banner tap
        //    arriving after the gate flip) must NOT synthesize a
        //    duplicate managed window — the suggest entry was cleared
        //    when the gate flipped, so this is a no-op.
        await orchestrator.acceptSuggestedSkip(windowId: "ad-gate-flip")

        let confirmedAfterAccept = await orchestrator.confirmedWindows()
        // Exactly one window covering the original span should exist —
        // the one created by the gate flip. No duplicate from
        // acceptSuggestedSkip's `promotedId = UUID().uuidString` path.
        let matching = confirmedAfterAccept.filter {
            $0.startTime == markOnly.startTime && $0.endTime == markOnly.endTime
        }
        #expect(matching.count == 1,
            "Stale acceptSuggestedSkip after gate flip must be a no-op; got \(matching.count) windows on the same span")
    }

    @Test("Tap before flip — accepted suggest id ignores a late non-markOnly ingest (playhead-rfu-sad)")
    func tapThenFlipSuggestIdIgnoresLateIngest() async throws {
        // Race scenario (the inverse of `gateFlipClearsSuggestEntry`):
        // the user taps the suggest banner BEFORE the gate flip lands.
        // `acceptSuggestedSkip` promotes the window under a fresh
        // UUID. A late-arriving non-markOnly AdWindow with the
        // ORIGINAL id must NOT register a second managed window —
        // that would emit a duplicate auto-skip banner and a duplicate
        // `auto_skip_fired` audit event for one user-initiated skip.
        let dir = try makeTempDir(prefix: "rfu-sad-tap-flip")
        let invariantLogger = SurfaceStatusInvariantLogger(directory: dir)
        let hasher: @Sendable (String) -> String = { [invariantLogger] in
            invariantLogger.hashEpisodeId($0)
        }

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
            invariantLogger: invariantLogger,
            episodeIdHasher: hasher
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Collect every banner emission so we can assert exactly one
        // span surface materialises end-to-end.
        let bannerStream = await orchestrator.bannerItemStream()
        nonisolated(unsafe) var receivedBanners: [AdSkipBannerItem] = []
        let collectTask = Task {
            for await item in bannerStream {
                receivedBanners.append(item)
                if receivedBanners.count >= 4 { break }
            }
        }

        // 1. markOnly arrives → suggest banner emitted, suggestWindows populated.
        let markOnly = makeMarkOnlyAdWindow(id: "ad-tap-flip", startTime: 30, endTime: 60)
        await orchestrator.receiveAdWindows([markOnly])

        // 2. User taps the suggest banner — promotes under a fresh UUID.
        await orchestrator.acceptSuggestedSkip(windowId: markOnly.id)

        // 3. LATE: gate flip arrives for the original id with the
        //    eligibilityGate cleared. Without the tap-then-flip
        //    guard, this would create a SECOND managed window
        //    keyed by the original id and re-fire everything.
        let lateFlipped = AdWindow(
            id: "ad-tap-flip",
            analysisAssetId: "asset-1",
            startTime: markOnly.startTime,
            endTime: markOnly.endTime,
            confidence: 0.85,
            boundaryState: "lexical",
            decisionState: "confirmed",
            detectorVersion: "detection-v1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: markOnly.startTime,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: nil
        )
        await orchestrator.receiveAdWindows([lateFlipped])

        try await Task.sleep(for: .milliseconds(150))
        collectTask.cancel()

        // Exactly one applied/confirmed managed window should exist
        // for the original span — the UUID-keyed promotion. The late
        // flipped ingest must NOT have registered a parallel entry
        // under "ad-tap-flip".
        let activeIDs = await orchestrator.activeWindowIDs()
        #expect(!activeIDs.contains("ad-tap-flip"),
            "Late non-markOnly ingest with the same id must NOT register a second managed window after acceptSuggestedSkip")

        let log = await orchestrator.getDecisionLog()
        let appliedOnSpan = log.filter {
            $0.decision == .applied
                && $0.snappedStart == markOnly.startTime
                && $0.snappedEnd == markOnly.endTime
        }
        #expect(appliedOnSpan.count == 1,
            "Exactly one applied decision should land on the span (one user skip → one decision); got \(appliedOnSpan.count)")

        // Banner stream: at most one `.autoSkipped` banner for the
        // promoted window. (A `.suggest` banner from the initial
        // markOnly delivery is allowed and expected.)
        let autoSkippedBanners = receivedBanners.filter { $0.tier == .autoSkipped }
        #expect(autoSkippedBanners.count == 1,
            "Exactly one auto-skip banner should fire for the promoted window; got \(autoSkippedBanners.count)")

        // Audit log: exactly one `auto_skip_fired` event. Drain the
        // logger's serial queue before reading.
        invariantLogger.flushForTesting()
        let sessionURL = try #require(invariantLogger.currentSessionFileURL)
        var autoSkipEntries: [SurfaceStateTransitionEntry] = []
        for _ in 0..<10 {
            let data = try Data(contentsOf: sessionURL)
            let lines = String(decoding: data, as: UTF8.self)
                .split(separator: "\n", omittingEmptySubsequences: true)
            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601
            let entries = try lines.map {
                try decoder.decode(SurfaceStateTransitionEntry.self, from: Data($0.utf8))
            }
            autoSkipEntries = entries.filter { $0.eventType == .autoSkipFired }
            if !autoSkipEntries.isEmpty { break }
            invariantLogger.flushForTesting()
        }
        #expect(autoSkipEntries.count == 1,
            "Exactly one auto_skip_fired audit event should fire for the user-tapped skip; got \(autoSkipEntries.count)")
    }
}
