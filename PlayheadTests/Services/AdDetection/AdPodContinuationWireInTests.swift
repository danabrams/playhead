// AdPodContinuationWireInTests.swift
// playhead-xsdz.65: end-to-end wire-in coverage for ad-pod continuation inside
// `AdDetectionService.runBackfill` (Step 18b).
//
// The pure contract is pinned in `AdPodContinuationTests`. This suite guards the
// WIRING, and it does so by running the SAME deterministic transcript twice —
// once with `podContinuationEnabled: false` and once `true` — over a fixture
// that contains a three-creative sponsor pod:
//
//   • FLAG OFF is inert: zero rows carry the continuation detector version.
//     Deleting the flag check (shipping this ON by accident) fails here.
//   • FLAG ON actually composes: at least one continuation row is persisted.
//     Deleting the Step 18b call (the "ships inert" failure mode) fails here.
//   • ADDITIVE ONLY: every row that is NOT a continuation row is byte-identical
//     between the two arms. This is the end-to-end form of "no existing window's
//     geometry, gate, anchors or id is modified" — the property that makes
//     playhead-ye0n (no per-window demotion for an edge change) and
//     playhead-2350 (the unanchored-edge auto-skip block) hold by construction.
//   • BANNER TIER: every persisted continuation row is `markOnly` / `candidate`
//     with both edge anchors `.unanchored`, so recovered pod material can never
//     become a silent skip.

import Foundation
import Testing
@testable import Playhead

@Suite("AdPodContinuation wire-in (playhead-xsdz.65)")
struct AdPodContinuationWireInTests {

    private static let podcastId = "podcast-pod-continuation"
    private static let assetId = "asset-pod-continuation"
    private static let episodeDuration = 140.0

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

    /// A three-creative DAI pod, shaped like the real failure. Creatives A and C
    /// carry a sponsor disclosure plus a promo code and a spoken URL, so the
    /// vetted lexical auto-ad rule fires on each. Creative B — between them —
    /// carries NO ad cue at all: it is a dialogue-format spot, which is precisely
    /// the ad the pipeline misses and the listener then hears play. Show narration
    /// brackets the pod on both sides.
    ///
    /// The pod's ad copy runs 30–80 s. Anything outside that is show.
    private func makePodChunks(assetId: String) -> [TranscriptChunk] {
        let spans: [(start: Double, end: Double, text: String)] = [
            (0.0, 30.0, "We were talking about the history of the neighborhood and how the old market building changed hands three times before the war."),
            (30.0, 45.0, "This episode is brought to you by Squarespace. Use code SHOW and head to squarespace com slash show."),
            (45.0, 65.0, "Buyers remorse. You bought a new car and now I am moving in with you. Sorry, I think there has been some mistake here. Terms and exclusions may apply."),
            (65.0, 80.0, "This break is sponsored by Rocket Money. Use code DEAL and visit rocketmoney com slash deal."),
            (80.0, 110.0, "You were telling me about the market building and the family that ran the produce stall on the corner for forty years."),
            (110.0, 140.0, "That says something about how these places hold a neighborhood together, and about who gets to decide what a street looks like.")
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

    /// Bounds of the cue-free middle creative — the ad the listener currently
    /// hears play after the banner fires on a neighbour.
    private static let missedCreative = (start: 45.0, end: 65.0)

    /// Bounds of the pod's ad copy. A recovered second outside this is show.
    private static let podBounds = (start: 30.0, end: 80.0)

    /// The seed the pass chains off: a CONFIRMED detector window on the pod's LAST
    /// creative only — the exact production failure this bead exists for ("we
    /// caught ad #3 and missed #2"). Pre-inserted rather than harvested from
    /// fusion so this wiring test does not silently become a test of fusion's
    /// confidence tuning: its `detectorVersion` differs from the service's, so the
    /// backfill reconcile leaves it alone, and `AdPodContinuation.isSeed` accepts
    /// it exactly as it accepts a real fused row.
    private static let seedWindowId = "pod-seed-window"

    private func makeSeedWindow() -> AdWindow {
        AdWindow(
            id: Self.seedWindowId,
            analysisAssetId: Self.assetId,
            startTime: 72.0,
            endTime: 80.0,
            confidence: 0.91,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "seed-detector-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: 72.0,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
    }

    private func makeService(
        store: AnalysisStore,
        podContinuationEnabled: Bool
    ) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1",
            fmBackfillMode: .off,
            podContinuationEnabled: podContinuationEnabled
        )
        return AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config
        )
    }

    private func runArm(
        podContinuationEnabled: Bool,
        extraWindows: [AdWindow] = []
    ) async throws -> [AdWindow] {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: Self.assetId))
        try await store.insertAdWindows([makeSeedWindow()] + extraWindows)
        let service = makeService(
            store: store,
            podContinuationEnabled: podContinuationEnabled
        )
        try await service.runBackfill(
            chunks: makePodChunks(assetId: Self.assetId),
            analysisAssetId: Self.assetId,
            podcastId: Self.podcastId,
            episodeDuration: Self.episodeDuration
        )
        return try await store.fetchAdWindows(assetId: Self.assetId)
            .sorted { lhs, rhs in
                lhs.startTime != rhs.startTime
                    ? lhs.startTime < rhs.startTime
                    : lhs.id < rhs.id
            }
    }

    private func continuationRows(_ windows: [AdWindow]) -> [AdWindow] {
        windows.filter { $0.detectorVersion == AdPodContinuation.detectorVersion }
    }

    /// A comparable projection of every field the continuation pass must not
    /// touch. String rather than a tuple so a mismatch prints readably, and
    /// `bitPattern` on the doubles so a sub-epsilon rewrite cannot slip through.
    private func fingerprint(_ window: AdWindow) -> String {
        [
            window.id,
            String(window.startTime.bitPattern),
            String(window.endTime.bitPattern),
            String(window.confidence.bitPattern),
            window.boundaryState,
            window.decisionState,
            window.detectorVersion,
            window.eligibilityGate ?? "nil",
            window.startEdgeAnchor,
            window.endEdgeAnchor,
            window.wasSkipped ? "skipped" : "notSkipped"
        ].joined(separator: "|")
    }

    @Test("flag OFF: no continuation rows are written")
    func flagOffIsInert() async throws {
        let windows = try await runArm(podContinuationEnabled: false)
        #expect(!windows.isEmpty, "fixture must produce ad windows for the arms to differ over")
        #expect(continuationRows(windows).isEmpty)
    }

    @Test("flag ON: runBackfill persists at least one continuation mark")
    func flagOnComposesThroughRunBackfill() async throws {
        let rows = continuationRows(try await runArm(podContinuationEnabled: true))
        #expect(
            !rows.isEmpty,
            "Step 18b must recover pod material on a three-creative pod fixture"
        )
        // The recovered material must land on the CUE-FREE middle creative — the
        // ad the listener currently hears play after the banner fires on its
        // neighbour. Reaching it is the felt-experience claim of this bead.
        let missed = Self.missedCreative
        let recoveredInsideMissed = rows.reduce(0.0) { total, row in
            total + max(0, min(row.endTime, missed.end) - max(row.startTime, missed.start))
        }
        #expect(
            recoveredInsideMissed > 10.0,
            "recovery must cover most of the missed creative (got \(recoveredInsideMissed)s of \(missed.end - missed.start)s)"
        )
        // ZERO NEW SHOW SECONDS: every recovered second must sit inside the pod's
        // ad copy, never in the narration either side of it.
        for row in rows {
            #expect(
                row.startTime >= Self.podBounds.start,
                "mark \(row.startTime)-\(row.endTime) reached into the opening narration"
            )
            #expect(
                row.endTime <= Self.podBounds.end,
                "mark \(row.startTime)-\(row.endTime) reached into the closing narration"
            )
        }
    }

    @Test("flag ON: every persisted continuation row is banner-tier")
    func continuationRowsAreBannerTier() async throws {
        let rows = continuationRows(try await runArm(podContinuationEnabled: true))
        #expect(!rows.isEmpty)
        for row in rows {
            #expect(row.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)
            #expect(row.decisionState == AdDecisionState.candidate.rawValue)
            #expect(row.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
            #expect(row.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue)
            #expect(row.boundaryState == AdPodContinuation.boundaryState)
            #expect(row.advertiser == nil)
            #expect(!row.wasSkipped)
        }
    }

    /// ADDITIVE ONLY. Same fixture, same config but for the flag: every row that
    /// is not a continuation row must be byte-identical across the two arms. If
    /// the pass ever starts rewriting an edge, a gate, or an id, this fails.
    @Test("flag ON changes nothing about the rows the pipeline already produced")
    func existingRowsAreUntouched() async throws {
        let off = try await runArm(podContinuationEnabled: false)
        let on = try await runArm(podContinuationEnabled: true)

        let offFingerprints = off.map(fingerprint)
        let onFingerprints = on
            .filter { $0.detectorVersion != AdPodContinuation.detectorVersion }
            .map(fingerprint)
        #expect(onFingerprints == offFingerprints)
        #expect(on.count > off.count, "the ON arm must ADD rows, not replace them")
    }

    /// A listener's hand-marked span is never crossed, end to end. The mark is
    /// inserted mid-pod; the continuation walk must stop at it rather than
    /// engulfing it — the persisted continuation rows must not span it.
    @Test("a persisted user mark is never engulfed by a continuation row")
    func userMarkIsNeverEngulfed() async throws {
        let mark = AdWindow(
            id: "user-mark-1",
            analysisAssetId: Self.assetId,
            startTime: 50.0,
            endTime: 56.0,
            confidence: 1.0,
            boundaryState: "userMarked",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "user-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "user",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )
        let windows = try await runArm(
            podContinuationEnabled: true,
            extraWindows: [mark]
        )
        for row in continuationRows(windows) {
            #expect(
                !(row.startTime < mark.endTime && row.endTime > mark.startTime),
                "continuation row \(row.startTime)-\(row.endTime) overlaps the listener's mark"
            )
        }
        let persistedMark = try #require(windows.first { $0.id == mark.id })
        #expect(persistedMark.startTime == mark.startTime)
        #expect(persistedMark.endTime == mark.endTime)
        #expect(persistedMark.boundaryState == "userMarked")
    }
}
