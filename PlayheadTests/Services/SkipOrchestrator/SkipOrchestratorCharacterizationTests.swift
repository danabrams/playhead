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
        #expect(currentCues.count == 1)
        if let mergedCue = currentCues.first {
            #expect(CMTimeGetSeconds(mergedCue.start) == 60)
            #expect(CMTimeGetSeconds(mergedCue.start + mergedCue.duration) == 120)
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
        #expect(currentCues.count == 2)
        if currentCues.count == 2 {
            #expect(CMTimeGetSeconds(currentCues[0].start) == 60)
            #expect(CMTimeGetSeconds(currentCues[0].start + currentCues[0].duration) == 90)
            #expect(CMTimeGetSeconds(currentCues[1].start) == 95)
            #expect(CMTimeGetSeconds(currentCues[1].start + currentCues[1].duration) == 120)
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
