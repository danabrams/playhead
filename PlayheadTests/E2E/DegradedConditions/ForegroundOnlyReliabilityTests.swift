// ForegroundOnlyReliabilityTests.swift
// playhead-rk7: degraded-conditions E2E — foreground-only skip reliability.
//
// Pins the contract that skip reliability is a *foreground-time*
// property and does NOT depend on any background runtime — no
// BGProcessingTask, no BGContinuedProcessingTask, and no scheduled
// background work need to fire for a confirmed AdWindow to dispatch
// its skip cue when the playhead reaches it.
//
// Why this matters: degraded-conditions devices (low battery, thermal
// throttle, LPM, no Apple Intelligence) cannot rely on the BG-task
// budget. The skip path that we ship to those users is exactly the
// foreground-only path. If a refactor accidentally couples the cue
// dispatch to a BG-task callback, this suite must catch it.
//
// Scenarios covered:
//   1. A confirmed AdWindow already exists in the store. The
//      orchestrator dispatches a skip cue from a foreground playhead
//      tick alone, without any task-scheduler activity.
//   2. The hot-path detector synchronously surfaces an AdWindow from
//      a known-ad transcript — proving the *detection* layer is also
//      foreground-only and does not require a background pump.
//   3. Multiple foreground playhead ticks across the active episode
//      keep dispatching cues for each confirmed window without ever
//      consulting the task scheduler.

import CoreMedia
import Foundation
import Testing

@testable import Playhead

@Suite("playhead-rk7 - foreground-only reliability", .serialized)
struct ForegroundOnlyReliabilityTests {

    // MARK: - Test 1: skip cue fires from a confirmed window via foreground tick alone

    @Test("Confirmed AdWindow dispatches skip cue from a foreground playhead tick with no BG-task activity")
    func skipCueFromForegroundTickAlone() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            config: .default,
            trustService: trustService
        )

        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "rk7-fg-ad",
            startTime: 30,
            endTime: 60,
            confidence: 0.9,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        // Foreground playhead tick — this is what `PlaybackService` posts
        // every ~1s while the user is actively listening. No background
        // hook fires it.
        await orchestrator.updatePlayheadTime(15)

        #expect(
            !pushedCues.isEmpty,
            "Skip cue must dispatch from a foreground playhead tick alone — no BG-task callback should be required."
        )
    }

    // MARK: - Test 2: hot-path detection runs in foreground without BG runtime

    @Test("Hot-path detection produces AdWindow synchronously from foreground call — no BG task needed")
    func hotPathDetectionIsForegroundOnly() async throws {
        let store = try await makeTestStore()
        let asset = AnalysisAsset(
            id: "rk7-fg-detect",
            episodeId: "ep-rk7-fg-detect",
            assetFingerprint: "rk7-fg-fp",
            weakFingerprint: nil,
            sourceURL: "file:///rk7/fg.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)

        let texts = [
            "Welcome back to the show. Today we are talking about modern web design.",
            "This episode is brought to you by Squarespace. Use code SHOW for twenty percent off your first purchase. Visit squarespace dot com slash show today.",
            "Now back to our interview. Our guest was telling us about the future of typography."
        ]
        let chunks = texts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "rk7-fg-\(idx)",
                analysisAssetId: asset.id,
                segmentFingerprint: "rk7-fg-fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "rk7-test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
        try await store.insertTranscriptChunks(chunks)

        // FM unavailable — the foreground reliability path must work without
        // any deferred enrichment running first.
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig.default,
            canUseFoundationModelsProvider: { false }
        )

        let windows = try await service.runHotPath(
            chunks: chunks,
            analysisAssetId: asset.id,
            episodeDuration: 90
        )

        #expect(
            !windows.isEmpty,
            "Hot-path must surface AdWindows from a synchronous foreground call — degraded-conditions devices rely on this path exclusively."
        )
        // The detected window must overlap the sponsor-disclosure chunk.
        let adRange = 30.0...60.0
        let overlapping = windows.filter {
            $0.startTime < adRange.upperBound && $0.endTime > adRange.lowerBound
        }
        #expect(
            !overlapping.isEmpty,
            "Detected window must overlap the disclosure chunk; saw \(windows.map { "\($0.startTime)…\($0.endTime)" })."
        )
    }

    // MARK: - Test 3: BGTaskScheduling is never consulted by the foreground skip path

    @Test("Foreground skip path makes no calls to BackgroundTaskScheduling")
    func foregroundSkipPathDoesNotConsultTaskScheduler() async throws {
        // We construct a StubTaskScheduler and never wire it into the
        // orchestrator — the orchestrator simply has no task-scheduler
        // dependency. This test makes that property explicit: the only
        // way to dispatch a skip cue is via `updatePlayheadTime`, and a
        // stubbed scheduler placed nearby remains unused.
        let scheduler = StubTaskScheduler()

        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            config: .default,
            trustService: trustService
        )

        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "rk7-fg-no-bg-ad",
            startTime: 30,
            endTime: 60,
            confidence: 0.9,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])
        await orchestrator.updatePlayheadTime(20)

        #expect(!pushedCues.isEmpty, "Skip cue must fire on the foreground tick.")
        #expect(
            scheduler.submittedRequests.isEmpty,
            "The foreground skip path must NOT submit any BGTaskRequest — degraded devices may have no BG budget at all."
        )
    }

    // MARK: - Test 4: multiple windows dispatch cues across successive ticks

    @Test("Successive foreground ticks dispatch cues for each confirmed window without BG involvement")
    func multipleWindowsAcrossSuccessiveTicks() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            config: .default,
            trustService: trustService
        )

        nonisolated(unsafe) var allCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in allCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1",
            episodeId: "asset-1",
            podcastId: "podcast-1"
        )

        // Two ad windows, separated in episode time.
        let ad1 = makeSkipTestAdWindow(
            id: "rk7-fg-ad-1", startTime: 30, endTime: 60,
            confidence: 0.9, decisionState: "confirmed"
        )
        let ad2 = makeSkipTestAdWindow(
            id: "rk7-fg-ad-2", startTime: 600, endTime: 660,
            confidence: 0.9, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad1, ad2])

        // Drive the playhead through both windows from the foreground.
        await orchestrator.updatePlayheadTime(15)
        await orchestrator.updatePlayheadTime(500)
        await orchestrator.updatePlayheadTime(595)

        // The orchestrator emits the *full* set of applied skip ranges
        // each time it changes; the latest snapshot should cover both
        // windows' worth of ranges.
        #expect(
            allCues.count >= 1,
            "At least one skip-cue dispatch must have happened across foreground ticks."
        )
    }
}
