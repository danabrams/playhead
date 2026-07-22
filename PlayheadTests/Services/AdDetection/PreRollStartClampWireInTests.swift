// PreRollStartClampWireInTests.swift
// playhead-xsdz.66: end-to-end wire-in coverage for the pre-roll start-at-zero
// clamp inside `AdDetectionService.runBackfill`.
//
// The pure-engine contract is pinned in `PreRollStartClampTests`. This suite
// guards the WIRING: that `runBackfill` actually invokes the clamp with the
// production `preRollStartClampSeconds` and that the widened start reaches the
// persisted `ad_windows` row. It runs the SAME deterministic transcript fixture
// twice — once with the clamp disabled (threshold 0) and once at the production
// default — and asserts the persisted first-window start moves from its detected
// value to 0.0 only in the enabled arm. Deleting the clamp call (the exact
// "ships inert" failure mode the bead warns against) fails the `firesAtDefault`
// assertion.

import Foundation
import Testing
@testable import Playhead

@Suite("PreRollStartClamp wire-in (playhead-xsdz.66)")
struct PreRollStartClampWireInTests {

    private static let podcastId = "podcast-preroll-test"

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

    /// A transcript whose sponsor read sits in an EARLY chunk starting at 6 s —
    /// inside the pre-roll zone — with no feature windows (so the boundary
    /// refiners no-op and the persisted span start stays at the chunk start,
    /// carrying the conservative `.unanchored` edge anchor).
    private func makeEarlyAdChunks(assetId: String) -> [TranscriptChunk] {
        let spans: [(start: Double, end: Double, text: String)] = [
            (0.0, 6.0, "Welcome back to the show today everyone."),
            (6.0, 40.0, "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show. Sign up today and make your website."),
            (40.0, 74.0, "Back to our conversation about technology and the future of podcasting.")
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
        preRollStartClampSeconds: Double
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off,
            preRollStartClampSeconds: preRollStartClampSeconds
        )
        return AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config
        )
    }

    private func firstWindowStart(
        preRollStartClampSeconds: Double,
        assetId: String
    ) async throws -> Double {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let service = makeService(store: store, preRollStartClampSeconds: preRollStartClampSeconds)
        try await service.runBackfill(
            chunks: makeEarlyAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: Self.podcastId,
            episodeDuration: 74.0
        )
        let windows = try await store.fetchAdWindows(assetId: assetId)
            .filter { $0.decisionState != AdDecisionState.suppressed.rawValue }
            .sorted { $0.startTime < $1.startTime }
        let first = try #require(windows.first, "fixture must persist at least one visible ad window")
        return first.startTime
    }

    /// Clamp DISABLED (threshold 0): the persisted first-window start keeps its
    /// detected value in the pre-roll zone (> 0, ≤ 20). This is the baseline the
    /// enabled arm is compared against — and it proves the fixture really does
    /// land an unanchored pre-roll early enough for the clamp to bite.
    @Test("clamp disabled: early pre-roll persists at its detected (non-zero) start")
    func disabledKeepsDetectedStart() async throws {
        let detected = try await firstWindowStart(
            preRollStartClampSeconds: 0.0,
            assetId: "asset-preroll-off"
        )
        #expect(detected > 0.0, "fixture pre-roll must start after 0 for the clamp to have work to do")
        #expect(detected <= AdDetectionConfig.default.preRollStartClampSeconds,
                "fixture pre-roll must land inside the pre-roll zone (got \(detected))")
    }

    /// Clamp at the PRODUCTION DEFAULT: `runBackfill` widens the same early
    /// unanchored pre-roll's persisted start to exactly 0.0. If the clamp call is
    /// removed from `runBackfill`, this persists the detected (~6s) start and the
    /// assertion fails — the "ships inert" guard.
    @Test("clamp at production default: runBackfill widens the persisted pre-roll start to 0.0")
    func firesAtDefaultThroughRunBackfill() async throws {
        let start = try await firstWindowStart(
            preRollStartClampSeconds: AdDetectionConfig.default.preRollStartClampSeconds,
            assetId: "asset-preroll-on"
        )
        #expect(start == 0.0)
    }
}
