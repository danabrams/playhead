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
}

@Suite("SkipOrchestrator Revert - Time Range and Banner Paths")
struct SkipOrchestratorRevertTests {

    // MARK: - revertByTimeRange

    @Test("revertByTimeRange reverts overlapping applied window")
    func revertByTimeRangeRevertsOverlapping() async throws {
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
            id: "ad-range-1",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

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

    @Test("revertWindow reverts a specific window by ID and removes cue")
    func revertWindowRemovesCue() async throws {
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
            id: "ad-banner-revert",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        try await store.insertAdWindow(ad)
        await orchestrator.receiveAdWindows([ad])

        #expect(!pushedCues.isEmpty)

        await orchestrator.revertWindow(windowId: "ad-banner-revert", podcastId: "podcast-1")

        let log = await orchestrator.getDecisionLog()
        let reverted = log.filter {
            $0.decision == .reverted
                && $0.adWindowId == "ad-banner-revert"
                && $0.reason.contains("banner")
        }
        #expect(!reverted.isEmpty)
        #expect(pushedCues.isEmpty)
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
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
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

        // First revert — should produce a decision log entry.
        await orchestrator.revertWindow(windowId: "ad-double-revert", podcastId: "podcast-1")

        let logAfterFirst = await orchestrator.getDecisionLog()
        let revertedFirst = logAfterFirst.filter {
            $0.decision == .reverted && $0.adWindowId == "ad-double-revert"
        }
        #expect(revertedFirst.count == 1)

        // Second revert — guard should prevent duplicate log entry.
        await orchestrator.revertWindow(windowId: "ad-double-revert", podcastId: "podcast-1")

        let logAfterSecond = await orchestrator.getDecisionLog()
        let revertedSecond = logAfterSecond.filter {
            $0.decision == .reverted && $0.adWindowId == "ad-double-revert"
        }
        #expect(revertedSecond.count == 1, "Double revert should not produce duplicate decision log entries")
    }

    // MARK: - Segment broadcast after revert

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
}
