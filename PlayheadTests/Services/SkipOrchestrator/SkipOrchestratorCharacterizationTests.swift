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
    recomputationRevision: Int = 1,
    includeProducerRevision: Bool = true
) -> AdDecisionResult {
    let producerRevision =
        eligibilityGate == .eligible && includeProducerRevision
        ? makeSkipTestAdWindow(
            id: id,
            assetId: analysisAssetId,
            startTime: startTime,
            endTime: endTime,
            confidence: skipConfidence,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        : nil
    return AdDecisionResult(
        id: id,
        analysisAssetId: analysisAssetId,
        startTime: startTime,
        endTime: endTime,
        skipConfidence: skipConfidence,
        eligibilityGate: eligibilityGate,
        recomputationRevision: recomputationRevision,
        producerRevision: producerRevision
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
        // playhead-wq34: asked across BOTH tiers. The subject here is
        // RETIREMENT — that `retireAdWindows` removes a window from live
        // orchestration — and on this shadow-mode show the rows now live in the
        // suggest tier. A check phrased over `activeWindowIDs()` alone would
        // read "both retired" before `retireAdWindows` was even called, which
        // is a test that cannot fail for its own reason.
        func liveWindowIDs() async -> Set<String> {
            let managed = await orchestrator.activeWindowIDs()
            let suggested = await orchestrator.activeSuggestWindowIDs()
            return managed.union(suggested)
        }
        #expect(await liveWindowIDs() == Set(["ad-intro", "ad-close"]))

        await orchestrator.retireAdWindows(ids: ["ad-close"])

        #expect(await liveWindowIDs() == Set(["ad-intro"]))
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

    @Test("Non-finite seeks fail closed without changing playhead state")
    func nonFiniteSeeksFailClosed() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1"
        )
        await orchestrator.updatePlayheadTime(12)

        await orchestrator.recordUserSeek(to: .nan)
        #expect(await orchestrator._currentPlayheadTimeForTesting() == 12)

        let generation =
            await orchestrator.episodeLifecycleGenerationSnapshot()
        let accepted = await orchestrator.recordUserSeek(
            to: .infinity,
            ifEpisodeLifecycleGeneration: generation
        )
        #expect(!accepted)
        await orchestrator.recordUserSeek(to: -1)
        #expect(await orchestrator._currentPlayheadTimeForTesting() == 12)
        #expect(
            !(await orchestrator.recordUserSeek(
                to: -0.001,
                ifEpisodeLifecycleGeneration: generation
            ))
        )
        #expect(await orchestrator._currentPlayheadTimeForTesting() == 12)
    }

    @Test("Invalid same-ID runtime material disarms an existing cue")
    func invalidSameIDReplacementFailsClosed() async throws {
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
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let valid = makeSkipTestAdWindow(
            id: "same-id-invalid-replacement",
            startTime: 60,
            endTime: 90,
            confidence: 0.9,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([valid])
        #expect(
            await orchestrator.activeWindowIDs()
                .contains(valid.id)
        )

        let invalid = makeSkipTestAdWindow(
            id: valid.id,
            startTime: 60,
            endTime: 90,
            confidence: .nan,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([invalid])

        #expect(
            !(await orchestrator.activeWindowIDs())
                .contains(valid.id)
        )
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

    /// playhead-wq34 rewrote what this test asserts, because what it asserted
    /// WAS the defect. "Shadow mode confirms" named a `.confirmed` decision the
    /// listener could not see: no skip, no card, only a passive timeline block
    /// — strictly LESS than the identical row stamped `.markOnly` would have
    /// produced, in the mode a brand-new show starts in.
    ///
    /// The claim worth pinning survives and is now stated in full: shadow mode
    /// removes no audio, AND the row still reaches the listener as a card it
    /// can answer. Both halves matter — the first alone is what let the silence
    /// ship.
    @Test("Shadow mode never applies, and offers the span instead of swallowing it")
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
        #expect(applied.isEmpty, "shadow mode removes no audio")
        #expect(
            await orchestrator.activeSuggestWindowIDs() == ["ad-shadow"],
            "playhead-wq34: and the span is OFFERED rather than silently confirmed"
        )
        #expect(
            (await orchestrator.activeWindowIDs()).isEmpty,
            "the managed tier can do nothing with it, so it does not hold it"
        )
    }

    /// playhead-wq34: same rewrite, same reason as `shadowModeNoSkip` above —
    /// and `.manual` is the mode that made the old assertion most misleading,
    /// since `evaluateWindow`'s `.manual` arm logs "awaiting user tap" for a
    /// window there was no way to tap.
    @Test("Manual mode does not auto-apply, and offers the span for the tap it promises")
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
        #expect(applied.isEmpty, "manual mode removes no audio on its own")
        #expect(
            await orchestrator.activeSuggestWindowIDs() == ["ad-manual"],
            "playhead-wq34: the tap the mode's own log line promises is a real affordance"
        )
    }

    /// playhead-wq34 changed how this test reaches a managed `.confirmed`
    /// window, and the new route is the more honest one.
    ///
    /// It used to use a `.manual`-mode show, relying on `evaluateWindow`'s
    /// `.manual` arm to park the row in the managed tier — which is exactly the
    /// silent state wq34 removed, so the row is now a suggest card and
    /// `applyManualSkip` has nothing to act on. The seam still needs a managed
    /// window that the auto path declined to skip, and the production state
    /// that produces one is playhead-98co's edge-padding veto: with padding ON
    /// and no anchored start there is no late-safe window, so the row stays
    /// `.confirmed` rather than promoting. A user skipping that by hand is
    /// precisely what this seam is for — and `applyManualSkip` marks it
    /// user-initiated, which is what exempts it from the padding that vetoed it.
    ///
    /// (Both `applyManualSkip` and `confirmedWindows()` have no production
    /// callers today — measured — so this is a seam test either way. Reaching
    /// the seam through a state production can actually be in is what keeps it
    /// from becoming a test of nothing.)
    @Test("Manual skip applies a confirmed window")
    func manualSkipApplies() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.setEdgePaddingEnabled(true)
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

        // The padding veto held it at `.confirmed` — a managed window that did
        // NOT auto-skip, which is the only thing there is to manually skip.
        #expect(
            (await orchestrator.confirmedWindows()).contains { $0.id == "ad-mskip" },
            "setup: the unanchored span must be vetoed to .confirmed, not applied"
        )

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

    @Test("recordListenRevert persists a listenRevert CorrectionEvent",
          .timeLimit(.minutes(1)))
    func recordListenRevertWritesCorrectionEvent() async throws {
        let analysisStore = try await makeTestStore()
        try await analysisStore.insertAsset(makeSkipTestAnalysisAsset())

        // Await post-commit learning notification instead of polling SQLite
        // after the atomic correction transaction.
        let correctionStore = PersistentUserCorrectionStore(store: analysisStore)
        let vetoRecorded = TestEventCounter()
        let signalingStore = SignalingCorrectionStore(
            wrapping: correctionStore, vetoRecorded: vetoRecorded
        )
        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: analysisStore,
            trustService: trustService,
            correctionStore: signalingStore
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

        // Event-driven: resumes when post-commit notification lands.
        await vetoRecorded.wait(for: 1)

        let events = try await correctionStore.activeCorrections(for: "asset-1")
        #expect(!events.isEmpty, "Expected a CorrectionEvent to be written after recordListenRevert")
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

    @Test("Eligible decision without an exact persisted producer revision fails closed")
    func eligibleDecisionWithoutProducerRevisionFailsClosed() async throws {
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
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        let missingRevision = makePendingAdDecisionResult(
            id: "eligible-missing-producer",
            skipConfidence: 0.99,
            eligibilityGate: .eligible,
            includeProducerRevision: false
        )

        await orchestrator.receiveAdDecisionResults([missingRevision])

        #expect(
            !(await orchestrator.activeWindowIDs())
                .contains(missingRevision.id)
        )
        #expect(
            !(await orchestrator.activeSuggestWindowIDs())
                .contains(missingRevision.id)
        )
        #expect(
            await orchestrator.getDecisionLog().allSatisfy {
                $0.adWindowId != missingRevision.id
                    || $0.decision != .applied
            }
        )
    }

    @Test("Eligible decision with non-finite confidence fails closed")
    func eligibleDecisionWithNonFiniteConfidenceFailsClosed() async throws {
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
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        let invalid = makePendingAdDecisionResult(
            id: "eligible-infinite-confidence",
            skipConfidence: .infinity,
            eligibilityGate: .eligible
        )

        await orchestrator.receiveAdDecisionResults([invalid])

        #expect(
            !(await orchestrator.activeWindowIDs())
                .contains(invalid.id)
        )
        #expect(
            await orchestrator.getDecisionLog().allSatisfy {
                $0.adWindowId != invalid.id
                    || $0.decision != .applied
            }
        )
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

        // Manual mode: no auto-skip.
        let manualLog = await manualOrchestrator.getDecisionLog()
        let manualApplied = manualLog.filter { $0.decision == .applied }
        #expect(manualApplied.isEmpty, "Manual mode must never auto-apply AdDecisionResults")

        // playhead-wq34: "expose … as CONFIRMED windows" was the defect stated
        // as a requirement. `receiveAdDecisionResults` admits only `.eligible`
        // rows, so before wq34 every span it delivered to a non-`.auto` show
        // became a `.confirmed` the listener could not see — no skip and no
        // card, from the door that carries the pipeline's most-corroborated
        // output. Both orchestrators must now OFFER the span.
        #expect(
            await manualOrchestrator.activeSuggestWindowIDs() == ["eligible-non-auto"],
            "Manual mode must offer an eligible AdDecisionResult span, not swallow it"
        )
        #expect(
            await shadowOrchestrator.activeSuggestWindowIDs() == ["eligible-non-auto"],
            "and so must shadow — the suggest tier never consulted the mode"
        )
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
        // playhead-ugy4: the asset must be OWNED by the episode being played
        // and the markOnly row must exist durably, or step 3's
        // `acceptSuggestedSkip` aborts inside
        // `persistAcceptedSuggestionIfCurrent` no matter what the fusion path
        // did to the suggest entry — which would make this test pass with the
        // symmetric clear deleted.
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "asset-1")
        )
        // playhead-wq34: `.auto`, and the mode is load-bearing rather than
        // incidental. The race under test is a gate flip PROMOTING a span into
        // the managed tier while its suggest card is still on screen — and
        // after wq34 that promotion only happens when the detector class can
        // act, because a row the managed tier would silently confirm is now
        // routed to the suggest tier instead. Under the old `.manual` harness
        // the flip would leave the row exactly where it was and the test would
        // pass without the symmetric clear it is named for.
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
        try await store.insertAdWindow(markOnly)
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

        // playhead-wq34: counted over the managed DICTIONARY. On the `.auto`
        // show this test now needs, the fusion window promotes to `.applied`,
        // which `confirmedWindows()` filters out — the same playhead-ugy4 lens
        // error the assertion at the end of this test already names, applied to
        // these two as well.
        let onSpan = await orchestrator.activeWindowIDs()
        #expect(onSpan.count == 1,
            "Exactly one managed window should cover the span (the fusion-managed one); got \(onSpan.count)")
        #expect(onSpan.first == "ad-shared-id",
            "The surviving window must be the fusion-managed entry, not a UUID-keyed late promotion")
        // playhead-ugy4: `confirmedWindows()` filters to `.confirmed`, and a
        // late promotion lands as `.applied` — so the two assertions above
        // cannot see the duplicate they describe. `activeWindowIDs()` is the
        // dictionary the promotion actually writes into.
        #expect(await orchestrator.activeWindowIDs() == ["ad-shared-id"],
            "A late accept on the cleared suggest entry must not add a UUID-keyed managed window")
    }
}

// MARK: - Banner Item Stream Tests

@Suite("SkipOrchestrator Banner Item Stream")
struct SkipOrchestratorBannerItemStreamTests {

    @Test("Confirmed window in shadow mode does not claim an auto-skip")
    func confirmedShadowWindowDoesNotEmitAutoSkippedBanner() async throws {
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
        // playhead-bwxi: walk into [60, 120) so the silence below is a real
        // observation rather than an artefact of a playhead that never moved.
        await orchestrator.updatePlayheadTime(70)

        // Collect with a bounded timeout. Shadow mode is log-only, so an
        // auto-tier card would falsely claim playback skipped this span.
        let collectTask = Task<AdSkipBannerItem?, Never> {
            for await item in stream {
                return item
            }
            return nil
        }
        // Give the actor time to process and emit.
        try await Task.sleep(for: .milliseconds(100))
        collectTask.cancel()
        let received = await collectTask.value

        // playhead-bwxi: the claim is about the AUTO TIER, which is what this
        // test's own name and message say. `received == nil` was a stronger
        // assertion only because the playhead never moved: walk into the span
        // and playhead-wq34's monotonicity fallback correctly delivers a
        // SUGGEST card here — a row that would be silent on the managed tier in
        // `.shadow` is diverted, and that is shipped, deliberate behaviour
        // rather than a regression. Narrowing to the tier keeps the real claim
        // ("no completed-action copy for a skip that did not happen") and stops
        // asserting a silence the product does not promise.
        #expect(
            received?.tier != .autoSkipped,
            """
            A confirmed shadow window did not skip and must not render the \
            completed-action auto tier; got \(String(describing: received?.tier))
            """
        )
        let emitted = await orchestrator.emittedAutoSkipBannersSnapshot()
        #expect(!emitted.contains("ad-banner-1"))
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
            podcastId: "podcast-1"
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
        // playhead-bwxi: the auto tier presents on PLAYHEAD ENTRY, so the
        // listener has to be inside [60, 120) for anything to reach the
        // stream. Without this the assertion below is about a listener who is
        // nowhere.
        await orchestrator.updatePlayheadTime(70)

        let collectTask = Task<AdSkipBannerItem?, Never> {
            for await item in stream {
                return item
            }
            return nil
        }
        try await Task.sleep(for: .milliseconds(100))
        collectTask.cancel()
        let received = await collectTask.value

        let item = try #require(received, "Expected a banner item for an applied window")
        #expect(item.windowId == "ad-banner-auto")
        #expect(item.adStartTime == 60)
        #expect(item.adEndTime == 120)
        #expect(item.podcastId == "podcast-1")
    }

    @Test("Banner is emitted only once per window across repeated evaluations")
    func bannerEmittedOnlyOnce() async throws {
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
        // playhead-bwxi: and walk the span THREE times. Since the presentation
        // moved to the position path, "once per window" has to survive
        // repeated OBSERVATIONS inside the span as well as repeated
        // deliveries — a fresh way for this to break that did not exist when
        // the emitter lived in `evaluateAndPush`.
        await orchestrator.updatePlayheadTime(70)
        await orchestrator.updatePlayheadTime(80)
        await orchestrator.updatePlayheadTime(90)

        // Collect up to two items, but expect exactly one.
        let collectTask = Task<Int, Never> {
            var count = 0
            for await _ in stream {
                count += 1
                if count >= 2 { break }
            }
            return count
        }
        try await Task.sleep(for: .milliseconds(150))
        collectTask.cancel()
        let count = await collectTask.value

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
        // playhead-bwxi: walk INTO the span. A negative banner assertion taken
        // with the playhead nowhere near the window now passes for the wrong
        // reason — the presentation is position-gated, so "nothing arrived"
        // would be true of a perfectly healthy window too.
        await orchestrator.updatePlayheadTime(70)

        let collectTask = Task<AdSkipBannerItem?, Never> {
            for await item in stream {
                return item
            }
            return nil
        }
        try await Task.sleep(for: .milliseconds(100))
        collectTask.cancel()
        let received = await collectTask.value

        // playhead-bwxi: same narrowing as the shadow test above, same reason —
        // with the playhead inside the span, playhead-wq34 diverts this row to
        // the suggest tier rather than leaving it silent. What must not happen
        // is an AUTO-tier card claiming a skip that never fired.
        #expect(
            received?.tier != .autoSkipped,
            """
            Suppressed windows must not emit an auto-skip banner; got \
            \(String(describing: received?.tier))
            """
        )
        #expect(
            !(await orchestrator.emittedAutoSkipBannersSnapshot())
                .contains("ad-suppressed"),
            "and nothing may reach the auto-tier emission witness"
        )
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
        endTime: Double = 120,
        evidenceSources: String? = nil,
        // playhead-ynmk: whether the span's EXTENT is byte-verified. A
        // confirmation only skips when the derived per-edge policy has a
        // late-safe window, so the tests here that observe the acceptance
        // through `applied` / `wasSkipped` opt in explicitly. Default stays
        // `.unanchored` so the field case is not hidden from this file.
        startEdgeAnchor: AutoSkipEdgeAnchor = .unanchored,
        endEdgeAnchor: AutoSkipEdgeAnchor = .unanchored
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
            evidenceSources: evidenceSources,
            eligibilityGate: "markOnly",
            startEdgeAnchor: startEdgeAnchor.rawValue,
            endEdgeAnchor: endEdgeAnchor.rawValue
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
        // playhead-d3g0: detection delivery ARMS; the playhead entering the
        // span is what presents the card. This test's subject is unchanged —
        // "a markOnly window surfaces as tier `.suggest`" — but it now has to
        // satisfy the position precondition to observe it. The precondition
        // itself is pinned in `SuggestBannerEntryGateTests`.
        await orchestrator.updatePlayheadTime(window.startTime)

        let collectTask = Task<AdSkipBannerItem?, Never> {
            for await item in stream {
                return item
            }
            return nil
        }
        try await Task.sleep(for: .milliseconds(100))
        collectTask.cancel()
        let received = await collectTask.value

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

    // Positive controls for every supported automatic stamp on the AdWindow
    // path: legacy nil, the precision-gate `"autoSkip"` literal, and the
    // canonical fusion `.eligible` raw value. Unknown/blank non-nil values are
    // malformed persistence and are covered by the fail-closed gate suite.
    @Test(
        "supported automatic eligibilityGate values enter the managed path",
        arguments: [
            (label: "nil-stamp", gate: String?.none),
            (label: "autoSkip-stamp", gate: String?.some("autoSkip")),
            (label: "eligible-stamp", gate: String?.some("eligible")),
        ]
    )
    func supportedAutomaticGateEntersStandardSkipPath(
        label: String,
        gate: String?
    ) async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        // playhead-wq34: the three stamps under test are exactly the ones that
        // ADMIT a row to the managed tier, so the show must be one that can use
        // that tier. Without a trust profile it resolves `.shadow`, and all
        // three would be routed to the suggest tier for a reason that has
        // nothing to do with the stamp — turning three positive controls into
        // three assertions about the mode.
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Inline-build the AdWindow at high confidence with
        // `decisionState: "confirmed"`. The factory in TestHelpers does not
        // expose `eligibilityGate`.
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

        await orchestrator.receiveAdWindows([window])

        // Positive: the window IS in the standard managed path. Read off the
        // managed DICTIONARY (playhead-wq34 / playhead-ugy4): on an `.auto`
        // show these rows promote to `.applied`, which `confirmedWindows()`
        // filters out, so that accessor cannot see the thing being asserted.
        let managed = await orchestrator.activeWindowIDs()
        #expect(
            managed.contains(windowId),
            "[\(label)] eligibilityGate=\(String(describing: gate)) must enter the managed path; got \(managed)"
        )

        // Negative: no suggest-tier state was registered. This actor snapshot
        // is synchronous with ingestion and needs no timing assumptions.
        let suggestWindowIDs = await orchestrator.activeSuggestWindowIDs()
        #expect(
            !suggestWindowIDs.contains(windowId),
            "[\(label)] eligibilityGate=\(String(describing: gate)) must NOT enter the suggest tier; got \(suggestWindowIDs)"
        )
    }

    @Test("acceptSuggestedSkip immediately applies the promoted window")
    func acceptSuggestedSkipConfirmsWindow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "asset-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // playhead-ynmk: byte-exact on both edges. This test's subject is the
        // PROMOTION (one durable applied row, fresh id, evidence carried
        // across, no detailed-log copy) — it needs a span the confirmation is
        // permitted to skip. The unanchored outcome is a different contract and
        // lives in `BannerConfirmationExtentGateTests`.
        let window = makeMarkOnlyAdWindow(
            id: "ad-suggest-accept",
            evidenceSources: "classifier,catalog",
            startEdgeAnchor: .rediffByteExact,
            endEdgeAnchor: .rediffByteExact
        )
        try await store.insertAdWindow(window)
        await orchestrator.receiveAdWindows([window])

        // Pre-condition: not yet in the confirmed set.
        let confirmedBefore = await orchestrator.confirmedWindows()
        #expect(!confirmedBefore.contains { $0.id == "ad-suggest-accept" })

        let accepted = await orchestrator.acceptSuggestedSkip(
            windowId: "ad-suggest-accept"
        )
        #expect(accepted)

        // Post-condition: the explicit Yes is itself the skip command, so the
        // fresh UUID-keyed promotion must already be applied rather than left
        // in the confirmed/manual-action set. The durable row is the
        // authoritative assertion: explicit feedback must not be copied into
        // the detailed decision log.
        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        let appliedOnSpan = persisted.filter {
            $0.decisionState == AdDecisionState.applied.rawValue
                && $0.startTime == window.startTime
                && $0.endTime == window.endTime
        }
        let promoted = try #require(
            appliedOnSpan.count == 1 ? appliedOnSpan[0] : nil,
            "acceptSuggestedSkip must durably apply exactly one promoted window"
        )
        #expect(promoted.id != window.id)
        // playhead-ynmk INVERTED this assertion. It used to require 1.0, which
        // the tap synthesised: a span of pure show content confirmed by mistake
        // would have read 1.00 too, so the number recorded that a tap happened,
        // not that an ad exists. The DETECTOR's measured value survives the tap;
        // the assertion itself lives in `boundaryState`.
        #expect(promoted.confidence == window.confidence)
        #expect(promoted.boundaryState == "userConfirmedSuggested")
        #expect(promoted.evidenceSources == window.evidenceSources)

        let decisionLog = await orchestrator.getDecisionLog()
        let detailedFeedbackEntries = decisionLog.filter {
            $0.decision == .applied
                && $0.snappedStart == window.startTime
                && $0.snappedEnd == window.endTime
        }
        #expect(
            detailedFeedbackEntries.isEmpty,
            "Suggest Yes must not copy its exact span into the decision log"
        )
        let confirmedAfter = await orchestrator.confirmedWindows()
        #expect(
            confirmedAfter.allSatisfy {
                $0.startTime != window.startTime || $0.endTime != window.endTime
            },
            "The accepted suggestion must not wait for a second manual action"
        )
    }

    @Test("accepted suggestion clears untrusted catalog provenance")
    func acceptedSuggestionClearsUntrustedCatalogProvenance() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "asset-1")
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            correctionStore: correctionStore
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )
        let suggested = AdWindow(
            id: "ad-suggest-untrusted-catalog",
            analysisAssetId: "asset-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.45,
            boundaryState: "lexical",
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: 60,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: "classifier,catalog",
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            catalogStoreMatchSimilarity: 0.99,
            catalogFingerprintVersion:
                CatalogFingerprintVersion.currentCatalog.rawValue,
            catalogMatchedEntryId: UUID().uuidString,
            catalogMatchedShowId: "different-show",
            catalogMatchedLearningSource:
                CatalogLearningSource.userMarkedAd.rawValue,
            catalogMatchedLearningLifecycle:
                CatalogLearningLifecycle.explicitConfirmation.rawValue
        )
        try await store.insertAdWindow(suggested)
        await orchestrator.receiveAdWindows([suggested])

        #expect(
            await orchestrator.acceptSuggestedSkip(
                windowId: suggested.id
            )
        )

        let promoted = try #require(
            try await store.fetchAdWindows(assetId: "asset-1")
                .first {
                    $0.id != suggested.id
                        && $0.startTime == suggested.startTime
                        && $0.endTime == suggested.endTime
                }
        )
        #expect(!promoted.claimsCatalogMatch)
        #expect(promoted.catalogStoreMatchSimilarity == nil)
        #expect(promoted.catalogFingerprintVersion == nil)
        #expect(promoted.catalogMatchedEntryId == nil)
        #expect(promoted.catalogMatchedShowId == nil)
        #expect(promoted.catalogMatchedLearningSource == nil)
        #expect(promoted.catalogMatchedLearningLifecycle == nil)
    }

    @Test("acceptSuggestedSkip is a no-op when window is unknown")
    func acceptSuggestedSkipUnknownIsNoOp() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "asset-1")
        )
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
        // playhead-ugy4: the asset must be OWNED by the episode being played
        // and the markOnly row must exist durably, or the stale accept below
        // aborts inside `persistAcceptedSuggestionIfCurrent` whether or not
        // `declineSuggestedSkip` cleared the suggest entry.
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "asset-1")
        )
        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let window = makeMarkOnlyAdWindow(id: "ad-suggest-decline")
        try await store.insertAdWindow(window)
        await orchestrator.receiveAdWindows([window])

        await orchestrator.declineSuggestedSkip(windowId: "ad-suggest-decline")

        let confirmed = await orchestrator.confirmedWindows()
        #expect(confirmed.isEmpty,
            "declineSuggestedSkip must not promote the window into the skip path")
        #expect(!(await orchestrator.activeSuggestWindowIDs()).contains("ad-suggest-decline"),
            "declineSuggestedSkip must clear the suggest entry")

        // Subsequent accept on the same id is now a no-op — the suggest
        // entry has been cleared. (This protects against a stale tap
        // arriving after the user has dismissed the banner.)
        await orchestrator.acceptSuggestedSkip(windowId: "ad-suggest-decline")
        let confirmedAfter = await orchestrator.confirmedWindows()
        #expect(confirmedAfter.isEmpty,
            "Accept after decline must be a no-op — the suggest window is gone")
        // playhead-ugy4: a promotion lands as `.applied`, which
        // `confirmedWindows()` filters out — so the assertion above cannot
        // see the window it is describing. `activeWindowIDs()` can.
        #expect((await orchestrator.activeWindowIDs()).isEmpty,
            "Accept after decline must not install a managed window")
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
        // playhead-d3g0: entry is what presents the card, so the dedupe
        // contract is now exercised across BOTH axes at once — two producer
        // deliveries and several position observations inside the span still
        // owe the user exactly one question.
        await orchestrator.updatePlayheadTime(window.startTime)
        await orchestrator.updatePlayheadTime(window.startTime + 1)
        await orchestrator.updatePlayheadTime(window.startTime + 2)

        let collectTask = Task<Int, Never> {
            var count = 0
            for await _ in stream {
                count += 1
                if count >= 2 { break }
            }
            return count
        }
        try await Task.sleep(for: .milliseconds(150))
        collectTask.cancel()
        let count = await collectTask.value

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
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(episodeId: "asset-1")
        )
        // playhead-wq34: `.auto`, for the same reason as
        // `fusionResultClearsSharedIdSuggestEntry` — the race this test is
        // named for is a gate flip PROMOTING the span into the managed tier
        // while its card is still visible, and after wq34 a flip on a
        // non-`.auto` show simply re-registers the row in the suggest tier,
        // where a Yes is a legitimate answer rather than the stale tap this
        // guard exists to defuse.
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

        // 1. Window arrives as markOnly → enters suggestWindows.
        // playhead-auz3: the durable row is what step 4's accept validates
        // against. Without it `persistAcceptedSuggestionIfCurrent` refuses
        // before the gate-flip clear can matter, so the "no-op" this test
        // names would be proved by the missing fixture row rather than by
        // production. (The asset row above already agrees with
        // `beginEpisode`'s episodeId, which is the other half of that check.)
        let markOnly = makeMarkOnlyAdWindow(id: "ad-gate-flip")
        try await store.insertAdWindow(markOnly)
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

        // 3. The id must now be in the MANAGED set. playhead-wq34 moved this
        //    read off `confirmedWindows()`: in `.auto` the flip promotes to
        //    `.applied`, which that accessor filters out — the same
        //    playhead-ugy4 lens error the assertion at the end of this test
        //    already calls out.
        #expect((await orchestrator.activeWindowIDs()).contains("ad-gate-flip"),
            "After gate flip, the same id must enter the managed-window set")
        #expect(!(await orchestrator.activeSuggestWindowIDs()).contains("ad-gate-flip"),
            "and the suggest entry must be gone — that clear is what this test is named for")

        // 4. A late `acceptSuggestedSkip` call (e.g. a stale banner tap
        //    arriving after the gate flip) must NOT synthesize a
        //    duplicate managed window — the suggest entry was cleared
        //    when the gate flipped, so this is a no-op.
        await orchestrator.acceptSuggestedSkip(windowId: "ad-gate-flip")

        // Exactly one window covering the original span should exist — the one
        // created by the gate flip. No duplicate from acceptSuggestedSkip's
        // `promotedId = UUID().uuidString` path. Counted over the managed
        // dictionary rather than `confirmedWindows()` (playhead-wq34 /
        // playhead-ugy4: the duplicate lands as `.applied`, which that accessor
        // cannot see — the very blind spot the final assertion below was added
        // for, applied here too).
        let matchingIDs = await orchestrator.activeWindowIDs()
        #expect(matchingIDs.count == 1,
            "Stale acceptSuggestedSkip after gate flip must be a no-op; got \(matchingIDs.count) windows on the same span")
        // playhead-auz3: `confirmedWindows()` filters to `.confirmed`, and the
        // duplicate this test is named for lands as `.applied` — so the two
        // assertions above structurally cannot observe it. `activeWindowIDs()`
        // is the dictionary `acceptSuggestedSkip` actually writes into.
        #expect(await orchestrator.activeWindowIDs() == ["ad-gate-flip"],
            "A late accept on the cleared suggest entry must not add a UUID-keyed managed window")
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
            correctionStore: correctionStore,
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
        let collectTask = Task<[AdSkipBannerItem], Never> {
            var items: [AdSkipBannerItem] = []
            for await item in bannerStream {
                items.append(item)
                if items.count >= 4 { break }
            }
            return items
        }

        // 1. markOnly arrives → suggest banner emitted, suggestWindows populated.
        // playhead-ynmk: byte-exact edges — this test's subject is the
        // tap-then-flip race guard, which it observes through the single
        // durable applied row, so the confirmation has to be skippable.
        let markOnly = makeMarkOnlyAdWindow(
            id: "ad-tap-flip",
            startTime: 30,
            endTime: 60,
            startEdgeAnchor: .rediffByteExact,
            endEdgeAnchor: .rediffByteExact
        )
        try await store.insertAdWindow(markOnly)
        await orchestrator.receiveAdWindows([markOnly])

        // 2. User taps the suggest banner — promotes under a fresh UUID.
        await orchestrator.acceptSuggestedSkip(windowId: markOnly.id)
        // playhead-bwxi: the promoted window is the one that could double-card,
        // and since the presentation moved to the position path it can only do
        // so on an observation inside its span. [30, 60).
        await orchestrator.updatePlayheadTime(45)

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
        // playhead-bwxi: and again after the late flip, so the auto-tier
        // assertion below is a real observation rather than an artefact of a
        // playhead that never moved.
        await orchestrator.updatePlayheadTime(45)

        try await Task.sleep(for: .milliseconds(150))
        collectTask.cancel()
        let receivedBanners = await collectTask.value

        // Exactly one applied/confirmed managed window should exist
        // for the original span — the UUID-keyed promotion. The late
        // flipped ingest must NOT have registered a parallel entry
        // under "ad-tap-flip".
        let activeIDs = await orchestrator.activeWindowIDs()
        #expect(!activeIDs.contains("ad-tap-flip"),
            "Late non-markOnly ingest with the same id must NOT register a second managed window after acceptSuggestedSkip")

        let persisted = try await store.fetchAdWindows(assetId: "asset-1")
        let appliedOnSpan = persisted.filter {
            $0.decisionState == AdDecisionState.applied.rawValue
                && $0.id != markOnly.id
                && $0.startTime == markOnly.startTime
                && $0.endTime == markOnly.endTime
        }
        #expect(appliedOnSpan.count == 1,
            "Exactly one durable applied row should land on the span; got \(appliedOnSpan.count)")

        let log = await orchestrator.getDecisionLog()
        let detailedFeedbackEntries = log.filter {
            $0.decision == .applied
                && $0.snappedStart == markOnly.startTime
                && $0.snappedEnd == markOnly.endTime
        }
        #expect(
            detailedFeedbackEntries.isEmpty,
            "Suggest Yes must not copy its exact span into the decision log"
        )

        // The suggest card already asked for and received the user's answer.
        // Applying the promoted window must not ask the same question again
        // through a follow-up `.autoSkipped` card.
        let autoSkippedBanners = receivedBanners.filter { $0.tier == .autoSkipped }
        #expect(autoSkippedBanners.isEmpty,
            "No duplicate auto-skip banner should follow suggest Yes; got \(autoSkippedBanners.count)")

        // The exact explicit-feedback span must not enter the invariant audit
        // log either.
        //
        // playhead-isp5: this used to assert `currentSessionFileURL == nil` —
        // "with no unrelated event in this fixture, no session file should be
        // created at all". That premise no longer holds: `beginEpisode`'s
        // cross-launch preload now writes one routine `ad_window_ingest_census`
        // row per episode start, so a file exists in every fixture. The proxy
        // was never the claim; the claim is that no AUTO-SKIP event carries
        // this span. Assert that directly, which is also strictly stronger than
        // file-absence was — it survives any future routine event.
        invariantLogger.flushForTesting()
        let auditedSpanStarts: [Int] = {
            guard let url = invariantLogger.currentSessionFileURL,
                  let data = try? Data(contentsOf: url) else { return [] }
            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601
            return String(decoding: data, as: UTF8.self)
                .split(separator: "\n", omittingEmptySubsequences: true)
                .compactMap {
                    try? decoder.decode(
                        SurfaceStateTransitionEntry.self, from: Data($0.utf8)
                    )
                }
                .filter { $0.eventType == .autoSkipFired }
                .compactMap(\.windowStartMs)
        }()
        #expect(
            !auditedSpanStarts.contains(Int(markOnly.startTime * 1_000)),
            "Suggest Yes must not create an exact-span audit event"
        )
    }
}
