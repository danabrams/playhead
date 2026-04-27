// SkipOrchestratorTrailingCushionTests.swift
// playhead-vn7n.2: Skip-end trailing cushion.
//
// Skip ends should land slightly *earlier* than the detected ad-end when the
// next thing after the ad is program audio (or end of episode), trading a
// small sliver of ad-tail for protection against program-start clipping.
// Pods of adjacent ads share a single cushion at the pod's trailing edge —
// internal seams between ads in a pod are not cushioned.

import CoreMedia
import Foundation
import Testing

@testable import Playhead

@Suite("SkipOrchestrator Trailing Cushion - pod-level skip-end cushion")
struct SkipOrchestratorTrailingCushionTests {

    // MARK: - Helpers

    /// Build a SkipPolicyConfig with an explicit trailing cushion. Other knobs
    /// match `.default` so test behavior tracks production except for the
    /// cushion under test.
    private static func config(cushion: TimeInterval) -> SkipPolicyConfig {
        SkipPolicyConfig(
            enterThreshold: 0.65,
            stayThreshold: 0.45,
            mergeGapSeconds: 4.0,
            minimumSpanSeconds: 15.0,
            shortSpanOverrideConfidence: 0.85,
            seekSuppressionSeconds: 3.0,
            seekStabilitySeconds: 2.0,
            policyVersion: "skip-policy-v1",
            adTrailingCushionSec: cushion
        )
    }

    private static func makeOrchestrator(
        store: AnalysisStore,
        trustService: TrustScoringService,
        cushion: TimeInterval
    ) -> SkipOrchestrator {
        SkipOrchestrator(
            store: store,
            config: config(cushion: cushion),
            trustService: trustService
        )
    }

    private static func cueEnd(_ cue: CMTimeRange) -> Double {
        CMTimeGetSeconds(cue.start + cue.duration)
    }

    private static func cueStart(_ cue: CMTimeRange) -> Double {
        CMTimeGetSeconds(cue.start)
    }

    // MARK: - Tests

    @Test("Single ad followed by no further detected span: cushion applied (trailing edge of episode)")
    func singleAdNoNextSpan() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = Self.makeOrchestrator(store: store, trustService: trustService, cushion: 1.0)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-trailing",
            startTime: 60, endTime: 120,
            confidence: 0.9, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        let cues = pushedCues
        #expect(cues.count == 1)
        if let cue = cues.first {
            #expect(Self.cueStart(cue) == 60)
            #expect(Self.cueEnd(cue) == 119) // 120 - 1.0 cushion
        }
    }

    @Test("Pod of two adjacent ads (within merge gap) — single merged cue, cushion at pod end only")
    func podOfTwoAdsCushionAtEnd() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = Self.makeOrchestrator(store: store, trustService: trustService, cushion: 1.0)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )

        // Two ads with a 2s gap (< mergeGapSeconds=4) → merged into one pod.
        let ad1 = makeSkipTestAdWindow(
            id: "pod-1a",
            startTime: 60, endTime: 90,
            confidence: 0.9, decisionState: "confirmed"
        )
        let ad2 = makeSkipTestAdWindow(
            id: "pod-1b",
            startTime: 92, endTime: 122,
            confidence: 0.88, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad1, ad2])

        let cues = pushedCues
        #expect(cues.count == 1, "Adjacent ads should merge into a single pod cue")
        if let cue = cues.first {
            #expect(Self.cueStart(cue) == 60)
            // First ad's end (90) is mid-pod and gets no cushion; only the
            // pod's trailing edge (122) receives the cushion.
            #expect(Self.cueEnd(cue) == 121) // 122 - 1.0
        }
    }

    @Test("Pod of three adjacent ads — middle seams get no cushion; pod-end gets cushion")
    func podOfThreeAdsCushionAtEnd() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = Self.makeOrchestrator(store: store, trustService: trustService, cushion: 1.0)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )

        // Three ads, each separated by 2s (< merge gap) → one merged pod.
        let ad1 = makeSkipTestAdWindow(
            id: "pod-3-a",
            startTime: 60, endTime: 90,
            confidence: 0.9, decisionState: "confirmed"
        )
        let ad2 = makeSkipTestAdWindow(
            id: "pod-3-b",
            startTime: 92, endTime: 122,
            confidence: 0.88, decisionState: "confirmed"
        )
        let ad3 = makeSkipTestAdWindow(
            id: "pod-3-c",
            startTime: 124, endTime: 154,
            confidence: 0.86, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad1, ad2, ad3])

        let cues = pushedCues
        #expect(cues.count == 1, "Three adjacent ads should merge into one pod cue")
        if let cue = cues.first {
            #expect(Self.cueStart(cue) == 60)
            #expect(Self.cueEnd(cue) == 153) // 154 - 1.0
        }
    }

    @Test("Two pods separated by program — each pod gets its own trailing cushion")
    func twoPodsEachGetCushion() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = Self.makeOrchestrator(store: store, trustService: trustService, cushion: 1.0)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )

        // Pod A ends at 90, Pod B starts at 200 — gap >> mergeGapSeconds.
        let podA = makeSkipTestAdWindow(
            id: "pod-A",
            startTime: 60, endTime: 90,
            confidence: 0.9, decisionState: "confirmed"
        )
        let podB = makeSkipTestAdWindow(
            id: "pod-B",
            startTime: 200, endTime: 260,
            confidence: 0.9, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([podA, podB])

        let cues = pushedCues
        #expect(cues.count == 2)
        if cues.count == 2 {
            #expect(Self.cueStart(cues[0]) == 60)
            #expect(Self.cueEnd(cues[0]) == 89) // 90 - 1.0
            #expect(Self.cueStart(cues[1]) == 200)
            #expect(Self.cueEnd(cues[1]) == 259) // 260 - 1.0
        }
    }

    @Test("Cushion ≥ pod duration — clamped at adStart so end never precedes start")
    func cushionClampedAtStart() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        // 16s ad, 20s cushion → would push end before start; must clamp.
        let orchestrator = Self.makeOrchestrator(store: store, trustService: trustService, cushion: 20.0)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )

        // Span 16s (above minimumSpanSeconds=15 so it isn't suppressed).
        let ad = makeSkipTestAdWindow(
            id: "ad-tiny",
            startTime: 60, endTime: 76,
            confidence: 0.95, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        let cues = pushedCues
        #expect(cues.count == 1)
        if let cue = cues.first {
            // Clamp: end = max(start, end - cushion) → max(60, 76-20) → 60.
            #expect(Self.cueStart(cue) == 60)
            #expect(Self.cueEnd(cue) == 60)
        }
    }

    @Test("Zero cushion — behavior identical to pre-cushion (skip-end exactly at ad-end)")
    func zeroCushionIsNoOp() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset())
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = Self.makeOrchestrator(store: store, trustService: trustService, cushion: 0.0)
        nonisolated(unsafe) var pushedCues: [CMTimeRange] = []
        await orchestrator.setSkipCueHandler { ranges in pushedCues = ranges }
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-1", episodeId: "asset-1", podcastId: "podcast-1"
        )

        let ad = makeSkipTestAdWindow(
            id: "ad-zero",
            startTime: 60, endTime: 120,
            confidence: 0.9, decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([ad])

        let cues = pushedCues
        #expect(cues.count == 1)
        if let cue = cues.first {
            #expect(Self.cueStart(cue) == 60)
            #expect(Self.cueEnd(cue) == 120) // No cushion subtracted.
        }
    }

    @Test("Default config carries the production-default trailing cushion")
    func defaultConfigCushionMatchesSpec() {
        // The constant is intentionally exposed on SkipPolicyConfig.default so
        // production callers and tests share the same source of truth.
        #expect(SkipPolicyConfig.default.adTrailingCushionSec == 1.0)
    }
}
