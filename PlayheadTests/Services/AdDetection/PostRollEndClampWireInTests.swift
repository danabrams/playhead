// PostRollEndClampWireInTests.swift
// playhead-aqo9: end-to-end wire-in coverage for the post-roll end-at-EOF clamp
// inside `AdDetectionService.runBackfill`.
//
// The pure-engine contract is pinned in `PostRollEndClampTests`. This suite
// guards the WIRING: that `runBackfill` actually invokes the clamp with the
// production `postRollEndClampProximitySeconds` AND with the real
// `episodeDuration`, and that the widened end reaches the persisted `ad_windows`
// row. It runs the SAME deterministic transcript fixture twice — once with the
// clamp disabled (threshold 0) and once at the production default — and asserts
// the persisted last-window end moves from its detected value to the episode
// duration only in the enabled arm. Deleting the clamp call (the exact "ships
// inert" failure mode) fails the `firesAtDefault` assertion; passing a wrong
// duration (e.g. 0, or the pre-roll clamp's argument order) fails it too.

import Foundation
import Testing
@testable import Playhead

@Suite("PostRollEndClamp wire-in (playhead-aqo9)")
struct PostRollEndClampWireInTests {

    private static let podcastId = "podcast-postroll-test"

    /// Total episode length. The fixture's sponsor read ends at 154 s, i.e.
    /// 6 s short of this — inside the production proximity band.
    private static let episodeDuration: Double = 160.0

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    /// A transcript whose sponsor read sits in the FINAL chunk, 120–154 s, with
    /// the episode running to 160 s — so the detected end lands 6 s short of EOF,
    /// inside the proximity band. The read starts well past
    /// `preRollStartClampSeconds`, so the pre-roll clamp cannot reach it and the
    /// two clamps are observed independently. No feature windows, so the boundary
    /// refiners no-op and the persisted span keeps the conservative
    /// `.unanchored` end anchor.
    private func makeLateAdChunks(assetId: String) -> [TranscriptChunk] {
        let spans: [(start: Double, end: Double, text: String)] = [
            (0.0, 40.0, "Welcome back to the show today everyone, we have a lot to get through."),
            (40.0, 80.0, "So we were talking about the way that recording technology changed over the last decade."),
            (80.0, 120.0, "And that brings us right up to where the industry is standing today, which is fascinating."),
            (120.0, 154.0, "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show. Sign up today and make your website.")
        ]
        return spans.enumerated().map { idx, span in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: span.start,
                endTime: span.end,
                text: span.text,
                normalizedText: span.text.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    private func makeService(
        store: AnalysisStore,
        postRollEndClampProximitySeconds: Double
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off,
            postRollEndClampProximitySeconds: postRollEndClampProximitySeconds
        )
        return AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config
        )
    }

    private func lastWindow(
        postRollEndClampProximitySeconds: Double,
        assetId: String
    ) async throws -> AdWindow {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let service = makeService(
            store: store,
            postRollEndClampProximitySeconds: postRollEndClampProximitySeconds
        )
        try await service.runBackfill(
            chunks: makeLateAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: Self.podcastId,
            episodeDuration: Self.episodeDuration
        )
        let windows = try await store.fetchAdWindows(assetId: assetId)
            .filter { $0.decisionState != AdDecisionState.suppressed.rawValue }
            .sorted { $0.endTime < $1.endTime }
        return try #require(windows.last, "fixture must persist at least one visible ad window")
    }

    /// Clamp DISABLED (threshold 0): the persisted last-window end keeps its
    /// detected value, short of EOF but INSIDE the production proximity band.
    /// This is the baseline the enabled arm is compared against — and it proves
    /// the fixture really does land an unanchored post-roll close enough to EOF
    /// for the clamp to bite.
    @Test("clamp disabled: late ad persists at its detected (short-of-EOF) end")
    func disabledKeepsDetectedEnd() async throws {
        let window = try await lastWindow(
            postRollEndClampProximitySeconds: 0.0,
            assetId: "asset-postroll-off"
        )
        let shortfall = Self.episodeDuration - window.endTime
        #expect(shortfall > 0.0,
                "fixture post-roll must end before EOF for the clamp to have work to do")
        #expect(shortfall <= AdDetectionConfig.default.postRollEndClampProximitySeconds,
                "fixture post-roll must land inside the proximity band (short by \(shortfall)s)")
        #expect(window.startTime > 0.0,
                "fixture post-roll must not reach 0.0, or the whole-episode refusal would mask the clamp")
    }

    /// Clamp at the PRODUCTION DEFAULT: `runBackfill` widens the same late
    /// unanchored ad's persisted end to exactly `episodeDuration`. If the clamp
    /// call is removed from `runBackfill`, this persists the detected (~154 s)
    /// end and the assertion fails — the "ships inert" guard.
    @Test("clamp at production default: runBackfill widens the persisted post-roll end to EOF")
    func firesAtDefaultThroughRunBackfill() async throws {
        let window = try await lastWindow(
            postRollEndClampProximitySeconds:
                AdDetectionConfig.default.postRollEndClampProximitySeconds,
            assetId: "asset-postroll-on"
        )
        #expect(window.endTime == Self.episodeDuration)
        #expect(window.eligibilityGate == SkipEligibilityGate.markOnly.rawValue,
                "widened material carries no classifier authority")
    }

    /// The inner edge is NOT part of this change (bead scope note 3): the same
    /// run must leave the detected START exactly where it was. Comparing the two
    /// arms directly is what makes this non-vacuous — an implementation that
    /// widened both edges would pass `firesAtDefault` and fail here.
    @Test("the clamp moves only the outer edge: the detected start is identical in both arms")
    func innerEdgeIsUntouched() async throws {
        let off = try await lastWindow(
            postRollEndClampProximitySeconds: 0.0,
            assetId: "asset-postroll-inner-off"
        )
        let on = try await lastWindow(
            postRollEndClampProximitySeconds:
                AdDetectionConfig.default.postRollEndClampProximitySeconds,
            assetId: "asset-postroll-inner-on"
        )
        #expect(on.endTime == Self.episodeDuration, "precondition: the clamp must fire in the on arm")
        #expect(on.endTime > off.endTime, "precondition: the arms must actually differ")
        #expect(on.startTime == off.startTime,
                "the post-roll START must not move — inner edges are out of scope for playhead-aqo9")
    }
}
