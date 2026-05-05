// AdDetectionServiceTrustScoreCarryForwardTests.swift
// Regression tests for Bug 4a (trust-score clobber):
//   AdDetectionService.updatePriors used to re-derive `skipTrustScore` every
//   episode from `observationCount` and `implicitFalsePositiveCount`. That
//   path clobbered every TrustScoringService decrement (recordFalseSkipSignal
//   subtracts 0.10 per user FP correction), so user FP corrections never
//   durably moved trust down.
//
// Captured user-bundle evidence: the `acast` feed had `skipTrustScore=1.0`
// despite `implicitFalsePositiveCount=8` and 8 user FP corrections recorded
// in `correction_events` — confirming the clobber.
//
// The fix splits responsibility:
//   - updatePriors owns lexical/slot priors and `observationCount`. It does
//     NOT write `skipTrustScore`; it carries forward
//     `existingProfile?.skipTrustScore ?? 0.5`.
//   - `TrustScoringService` and `AdDetectionService.recordListenRewind` are
//     the only two writers of `skipTrustScore` under the documented policy
//     (C26 H-1, playhead-od4j; see the `recordListenRewind` docstring in
//     AdDetectionService.swift for the full magnitude/state-machine
//     divergence). TrustScoringService writes via
//     `recordSuccessfulObservation`, `recordFalseSkipSignal`,
//     `recordFalseNegativeSignal`; recordListenRewind decrements by 0.05
//     (weaker signal, no demotion-evaluation).
//   - This contract pins carry-forward only — it does not assert which
//     paths may decrement.
//
// These tests pin that contract so a future refactor can't reintroduce the
// clobber.

import Foundation
import Testing
@testable import Playhead

@Suite("AdDetectionService trust-score carry-forward (Bug 4a)")
struct AdDetectionServiceTrustScoreCarryForwardTests {

    // MARK: - Fixture builders

    /// A 90-s, three-chunk episode whose middle chunk is unambiguously an ad
    /// (`Squarespace`/`promo code`). The same shape as
    /// `AdDetectionServiceShadowModeTests.makeChunks` so we know it produces
    /// a non-empty `nonSuppressedWindows` set and therefore drives
    /// `updatePriors`.
    private func makeChunks(assetId: String) -> [TranscriptChunk] {
        let texts = [
            "Welcome to the show. Today we're discussing podcasts and how to find them.",
            "This episode is brought to you by Squarespace. Use code SHOW for 20 percent off your first purchase at squarespace dot com slash show.",
            "Now back to our interview with our guest about technology trends."
        ]
        return texts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

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

    private func makeService(store: AnalysisStore) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .off
            )
        )
    }

    /// Floating-point tolerance for trust-score comparisons. Mirrors
    /// `TrustScoringServiceTests` (0.1 + 0.2 != 0.3 in IEEE 754).
    private let scoreTolerance: Double = 1e-10

    // MARK: - Test 1 — End-to-end regression

    @Test("recordFalseSkipSignal-decremented trust survives a runBackfill cycle (Bug 4a)")
    func userFalseSkipDecrementSurvivesUpdatePriors() async throws {
        // Setup: shared store so TrustScoringService and AdDetectionService
        // observe the same podcast_profiles row.
        let store = try await makeTestStore()
        let podcastId = "podcast-trust-carryforward-1"

        // Step 1: seed a profile with trust = 1.0 and a non-zero
        // observationCount so the post-run carry-forward isn't masked by
        // first-episode initialization.
        let seed = PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 1.0,
            observationCount: 10,
            mode: SkipMode.auto.rawValue,
            recentFalseSkipSignals: 0
        )
        try await store.upsertProfile(seed)

        // Step 2: drive trust 1.0 -> 0.90 via the user-FP path. Default
        // TrustScoringConfig.falseSignalPenalty is 0.10.
        let trust = TrustScoringService(store: store)
        await trust.recordFalseSkipSignal(podcastId: podcastId)

        let afterFP = try #require(await store.fetchProfile(podcastId: podcastId))
        #expect(
            abs(afterFP.skipTrustScore - 0.90) < scoreTolerance,
            "Pre-condition: recordFalseSkipSignal should land trust at 0.90 (got \(afterFP.skipTrustScore))"
        )
        #expect(afterFP.implicitFalsePositiveCount == 1)

        // Step 3: run a backfill so updatePriors fires. Use ad-bearing
        // chunks so `nonSuppressedWindows` is non-empty (otherwise
        // updatePriors short-circuits at its empty-windows guard and the
        // assertion below would pass for the wrong reason).
        let assetId = "asset-trust-carryforward-1"
        try await store.insertAsset(makeAsset(id: assetId))
        let service = makeService(store: store)
        try await service.runBackfill(
            chunks: makeChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: podcastId,
            episodeDuration: 90
        )

        // Sanity: the run produced confirmed windows (otherwise updatePriors
        // would have early-returned and the assertion below would pass for
        // the wrong reason).
        let windows = try await store.fetchAdWindows(assetId: assetId)
        try #require(!windows.isEmpty,
                     "Test fixture must produce non-empty AdWindows so updatePriors actually runs")

        // Step 4: re-fetch and assert the user FP decrement survived.
        let after = try #require(await store.fetchProfile(podcastId: podcastId))
        #expect(
            abs(after.skipTrustScore - 0.90) < scoreTolerance,
            "Bug 4a regression: skipTrustScore was clobbered by updatePriors. expected 0.90, got \(after.skipTrustScore)"
        )

        // Other priors that updatePriors DOES own should advance.
        #expect(after.observationCount == seed.observationCount + 1,
                "updatePriors retains responsibility for observationCount")
    }

    // MARK: - Test 2 — Unit-level carry-forward semantics

    @Test("updatePriors carries forward stored skipTrustScore instead of recomputing from FP counts")
    func updatePriorsCarriesForwardRatherThanRecomputing() async throws {
        // Pin the carry-forward contract directly: the OLD formula was
        //   rawTrust = obs / (obs + 5)
        //   trust    = max(0, min(1, rawTrust - fpCount * 0.02))
        // Seed obs=1 and fpCount=10 — pre-fix that produced trust ≈
        //   max(0, 1/(1+5+1) - 10*0.02) = max(0, 0.166 - 0.20) = 0.0.
        // With the fix, updatePriors must instead carry the stored
        // skipTrustScore (0.70) forward unchanged.
        let store = try await makeTestStore()
        let podcastId = "podcast-trust-carryforward-2"

        let storedTrust = 0.70
        let seed = PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 10,
            skipTrustScore: storedTrust,
            observationCount: 1,
            mode: SkipMode.manual.rawValue,
            recentFalseSkipSignals: 0
        )
        try await store.upsertProfile(seed)

        let assetId = "asset-trust-carryforward-2"
        try await store.insertAsset(makeAsset(id: assetId))
        let service = makeService(store: store)
        try await service.runBackfill(
            chunks: makeChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: podcastId,
            episodeDuration: 90
        )

        // Sanity: confirmed windows exist so updatePriors actually ran.
        let windows = try await store.fetchAdWindows(assetId: assetId)
        try #require(!windows.isEmpty,
                     "Test fixture must produce non-empty AdWindows so updatePriors actually runs")

        let after = try #require(await store.fetchProfile(podcastId: podcastId))

        // Carry-forward: stored value preserved exactly (not recomputed
        // from FP counts).
        #expect(
            abs(after.skipTrustScore - storedTrust) < scoreTolerance,
            "updatePriors must carry skipTrustScore forward from existingProfile (expected \(storedTrust), got \(after.skipTrustScore))"
        )

        // Belt-and-braces: under the OLD formula, trust would have been
        // pinned to 0.0 by the FP penalty. Assert we are not anywhere
        // near that, so a regression that re-introduces the formula is
        // unambiguously caught.
        #expect(after.skipTrustScore > 0.5,
                "Carry-forward must NOT collapse to the old fpCount-driven value (got \(after.skipTrustScore))")

        // updatePriors also must NOT touch implicitFalsePositiveCount —
        // that's also TrustScoringService territory.
        #expect(after.implicitFalsePositiveCount == seed.implicitFalsePositiveCount,
                "updatePriors must not mutate implicitFalsePositiveCount")
    }

    // MARK: - Test 3 — First-observation default

    @Test("updatePriors uses 0.5 default when no profile exists yet")
    func updatePriorsUsesDefaultTrustForNewProfile() async throws {
        // When AdDetectionService is the first writer for a podcast (no
        // prior TrustScoringService observation), updatePriors must fall
        // back to a sane default — 0.5 — that matches
        // `TrustScoringService.setUserOverride`'s new-profile default.
        let store = try await makeTestStore()
        let podcastId = "podcast-trust-carryforward-3"
        let assetId = "asset-trust-carryforward-3"

        try await store.insertAsset(makeAsset(id: assetId))
        let service = makeService(store: store)
        try await service.runBackfill(
            chunks: makeChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: podcastId,
            episodeDuration: 90
        )

        let windows = try await store.fetchAdWindows(assetId: assetId)
        try #require(!windows.isEmpty,
                     "Test fixture must produce non-empty AdWindows so updatePriors actually runs")

        let profile = try #require(await store.fetchProfile(podcastId: podcastId))
        #expect(
            abs(profile.skipTrustScore - 0.5) < scoreTolerance,
            "First-time updatePriors default should be 0.5 (got \(profile.skipTrustScore))"
        )
        #expect(profile.observationCount == 1)
    }
}
