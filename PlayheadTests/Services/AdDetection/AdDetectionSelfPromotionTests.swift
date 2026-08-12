// AdDetectionSelfPromotionTests.swift
// playhead-mn5e: a clean show promotes itself out of shadow.
//
// WHAT WAS BROKEN. `TrustScoringService.recordSuccessfulObservation` — the
// only method that raises trust AND runs `evaluatePromotion` on a
// non-user-gesture path — had ZERO production callers; its own doc comment
// at TrustScoringService.swift:870-872 said so. Meanwhile
// `AdDetectionService.updatePriors` incremented `observationCount` while
// carrying `skipTrustScore` and `mode` forward untouched. Both halves of the
// ladder shipped; nothing connected them. Measured consequence on Dan's
// device (scratchpad/db-morning7, 2026-08-12): 2 shows, obs=26 and obs=10,
// BOTH at `skipTrustScore = 0.5` and BOTH `mode = 'shadow'`; 45 ad_windows,
// 0 skipped. Across every device pull that carries the table, `skipTrustScore`
// takes exactly ONE distinct value — 0.5, its creation default. It has never
// moved.
//
// WHAT THESE TESTS PIN.
//   1. The caller EXISTS and runs the ladder: a backfill that confirms ad
//      windows promotes a clean shadow show to `.manual`.
//   2. Exactly ONE writer counts each backfill. Two incrementers would halve
//      every threshold in `TrustScoringConfig` without anybody choosing to.
//   3. The no-service fallback reproduces the pre-mn5e counter exactly.
//   4. An episode that confirmed NOTHING is not evidence — it must not move
//      trust. (`updatePriors` has always early-returned on an empty window
//      set; the new caller matches that guard so the two cannot drift.)
//   5. SAFETY: a promoted show can still be demoted, and a vetoed show cannot
//      climb back to `.auto` on self-observation alone.

import Foundation
import Testing
@testable import Playhead

@Suite("Self-promotion out of shadow (playhead-mn5e)", .serialized)
struct AdDetectionSelfPromotionTests {

    // MARK: - Fixtures

    /// A 90-s, three-chunk episode whose middle chunk is unambiguously an ad.
    /// Same shape as `AdDetectionServiceTrustScoreCarryForwardTests.makeChunks`
    /// so we know it produces a non-empty `nonSuppressedWindows` set and
    /// therefore reaches both the trust call and `updatePriors`.
    private func makeAdBearingChunks(assetId: String) -> [TranscriptChunk] {
        let texts = [
            "Welcome to the show. Today we're discussing podcasts and how to find them.",
            "This episode is brought to you by Squarespace. Use code SHOW for 20 percent off your first purchase at squarespace dot com slash show.",
            "Now back to our interview with our guest about technology trends."
        ]
        return makeChunks(assetId: assetId, texts: texts)
    }

    /// Three chunks with no ad language at all, so fusion confirms no windows.
    private func makeAdFreeChunks(assetId: String) -> [TranscriptChunk] {
        let texts = [
            "Welcome back to the program where we talk about migratory birds.",
            "The arctic tern travels further each year than any other animal alive.",
            "Next week we continue with the wandering albatross and its wingspan."
        ]
        return makeChunks(assetId: assetId, texts: texts)
    }

    private func makeChunks(assetId: String, texts: [String]) -> [TranscriptChunk] {
        texts.enumerated().map { idx, text in
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

    private func makeProfile(
        podcastId: String,
        mode: SkipMode,
        trust: Double,
        observations: Int,
        falseSignals: Int = 0
    ) -> PodcastProfile {
        PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: trust,
            observationCount: observations,
            mode: mode.rawValue,
            recentFalseSkipSignals: falseSignals
        )
    }

    private let scoreTolerance: Double = 1e-10

    /// Drive one backfill of an ad-bearing episode and return the resulting
    /// profile. `#require`s that windows were actually confirmed, so a fixture
    /// that silently stopped producing ads fails LOUDLY rather than making
    /// every assertion below pass for the wrong reason.
    @discardableResult
    private func runOneAdBearingBackfill(
        store: AnalysisStore,
        service: AdDetectionService,
        podcastId: String,
        assetId: String
    ) async throws -> PodcastProfile {
        try await store.insertAsset(makeAsset(id: assetId))
        try await service.runBackfill(
            chunks: makeAdBearingChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: podcastId,
            episodeDuration: 90
        )
        let windows = try await store.fetchAdWindows(assetId: assetId)
        try #require(
            !windows.isEmpty,
            "Fixture must confirm ad windows, otherwise both the trust call and updatePriors early-return and every assertion below passes vacuously"
        )
        return try #require(await store.fetchProfile(podcastId: podcastId))
    }

    // MARK: - 1. The caller exists and runs the ladder

    @Test("A clean shadow show promotes itself to manual when a backfill confirms ad windows")
    func cleanShadowShowSelfPromotesToManual() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-selfpromote"

        // obs=2 so this backfill is the third — `shadowToManualObservations`.
        // trust=0.5 is the value EVERY show on the device actually carries.
        try await store.upsertProfile(
            makeProfile(podcastId: podcastId, mode: .shadow, trust: 0.5, observations: 2)
        )

        let service = makeService(store: store)
        await service.setTrustScoringService(TrustScoringService(store: store))

        let after = try await runOneAdBearingBackfill(
            store: store, service: service,
            podcastId: podcastId, assetId: "asset-mn5e-selfpromote"
        )

        #expect(
            after.mode == SkipMode.manual.rawValue,
            "A clean show with 3 observations and trust >= 0.4 must leave shadow WITHOUT a banner tap. Got mode=\(after.mode) obs=\(after.observationCount) trust=\(after.skipTrustScore)"
        )
        #expect(after.observationCount == 3)
        #expect(
            abs(after.skipTrustScore - 0.6) < scoreTolerance,
            "correctObservationBonus (0.10) must have been applied; got \(after.skipTrustScore)"
        )
    }

    @Test("Without the new caller the same show would stay in shadow forever — trust never moves")
    func shadowShowWithoutTrustServiceNeverPromotes() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-noservice"

        try await store.upsertProfile(
            makeProfile(podcastId: podcastId, mode: .shadow, trust: 0.5, observations: 2)
        )

        // No `setTrustScoringService` — this reproduces the SHIPPED behaviour
        // and the device observation exactly.
        let service = makeService(store: store)

        let after = try await runOneAdBearingBackfill(
            store: store, service: service,
            podcastId: podcastId, assetId: "asset-mn5e-noservice"
        )

        #expect(after.mode == SkipMode.shadow.rawValue)
        #expect(
            abs(after.skipTrustScore - 0.5) < scoreTolerance,
            "This is the device's measured state: obs climbs, trust stays pinned at its creation value"
        )
        // The fallback still counts the observation, so the counter's meaning
        // is unchanged from pre-mn5e.
        #expect(
            after.observationCount == 3,
            "The no-service fallback must reproduce the pre-mn5e counter exactly"
        )
    }

    // MARK: - 2. Exactly one writer counts each backfill

    @Test("One backfill counts exactly ONE observation, not two")
    func oneBackfillCountsExactlyOneObservation() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-singlewriter"

        try await store.upsertProfile(
            makeProfile(podcastId: podcastId, mode: .shadow, trust: 0.5, observations: 7)
        )

        let service = makeService(store: store)
        await service.setTrustScoringService(TrustScoringService(store: store))

        let after = try await runOneAdBearingBackfill(
            store: store, service: service,
            podcastId: podcastId, assetId: "asset-mn5e-singlewriter"
        )

        #expect(
            after.observationCount == 8,
            "Two incrementers (updatePriors AND recordSuccessfulObservation) would land 9 and silently HALVE every observation threshold in TrustScoringConfig. Got \(after.observationCount)"
        )
    }

    // MARK: - 3. An episode that confirmed nothing is not evidence

    @Test("An episode that confirms no ad windows records no trust observation")
    func adFreeEpisodeRecordsNoObservation() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-noads"
        let assetId = "asset-mn5e-noads"

        try await store.upsertProfile(
            makeProfile(podcastId: podcastId, mode: .shadow, trust: 0.5, observations: 2)
        )

        let service = makeService(store: store)
        await service.setTrustScoringService(TrustScoringService(store: store))

        try await store.insertAsset(makeAsset(id: assetId))
        try await service.runBackfill(
            chunks: makeAdFreeChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: podcastId,
            episodeDuration: 90
        )

        let windows = try await store.fetchAdWindows(assetId: assetId)
        try #require(
            windows.isEmpty,
            "Fixture must confirm NO ad windows for this test to mean anything; got \(windows.count)"
        )

        let after = try #require(await store.fetchProfile(podcastId: podcastId))
        #expect(
            abs(after.skipTrustScore - 0.5) < scoreTolerance,
            "A show whose detector found nothing must not accrue trust — that is the quantity a BROKEN detector also produces"
        )
        #expect(after.mode == SkipMode.shadow.rawValue)
        #expect(
            after.observationCount == 2,
            "No window, no observation — matching updatePriors' own long-standing empty-window early return"
        )
    }

    // MARK: - 4. Safety: demotion still works after a promotion

    @Test("A show promoted to auto can still be demoted by user vetoes")
    func promotedShowCanStillBeDemoted() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-demote"

        // Stand where self-promotion can leave a show: `.auto`, clean record.
        try await store.upsertProfile(
            makeProfile(podcastId: podcastId, mode: .auto, trust: 0.9, observations: 20)
        )

        let trust = TrustScoringService(store: store)
        // `autoToManualFalseSignals` is 2.
        await trust.recordFalseSkipSignal(podcastId: podcastId)
        let afterOne = try #require(await store.fetchProfile(podcastId: podcastId))
        #expect(
            afterOne.mode == SkipMode.auto.rawValue,
            "One veto is under the threshold; got \(afterOne.mode)"
        )

        await trust.recordFalseSkipSignal(podcastId: podcastId)
        let afterTwo = try #require(await store.fetchProfile(podcastId: podcastId))
        #expect(
            afterTwo.mode == SkipMode.manual.rawValue,
            "Two vetoes must demote auto -> manual even after self-promotion; got \(afterTwo.mode)"
        )
        #expect(afterTwo.recentFalseSkipSignals == 2)
    }

    @Test("A vetoed show cannot climb back to auto on self-observation alone")
    func vetoedShowCannotSelfPromoteBackToAuto() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-vetoed"

        // A show the user has vetoed twice: demoted to manual, counter at 2.
        // Observations and trust are both already past the manual -> auto bar,
        // so `recentFalseSkipSignals == 0` is the ONLY thing holding it.
        try await store.upsertProfile(
            makeProfile(
                podcastId: podcastId, mode: .manual, trust: 0.9,
                observations: 20, falseSignals: 2
            )
        )

        let service = makeService(store: store)
        await service.setTrustScoringService(TrustScoringService(store: store))

        let after = try await runOneAdBearingBackfill(
            store: store, service: service,
            podcastId: podcastId, assetId: "asset-mn5e-vetoed"
        )

        #expect(
            after.mode == SkipMode.manual.rawValue,
            "manual -> auto requires recentFalseSkipSignals == 0. Self-observation must NOT relitigate a user veto; got \(after.mode)"
        )
        #expect(
            after.recentFalseSkipSignals == 2,
            "recordSuccessfulObservation must not decay the veto counter — only a banner Yes (recordCorrectObservation) may. Got \(after.recentFalseSkipSignals)"
        )
    }

    @Test("A clean manual show does reach auto, so the vetoed case above is not vacuous")
    func cleanManualShowReachesAuto() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-toauto"

        // IDENTICAL to `vetoedShowCannotSelfPromoteBackToAuto` except
        // falseSignals: 0. If this test and that one both pass, the veto
        // counter is provably the operative clause.
        try await store.upsertProfile(
            makeProfile(
                podcastId: podcastId, mode: .manual, trust: 0.9,
                observations: 20, falseSignals: 0
            )
        )

        let service = makeService(store: store)
        await service.setTrustScoringService(TrustScoringService(store: store))

        let after = try await runOneAdBearingBackfill(
            store: store, service: service,
            podcastId: podcastId, assetId: "asset-mn5e-toauto"
        )

        #expect(
            after.mode == SkipMode.auto.rawValue,
            "obs=21 >= 8, trust=1.0 >= 0.75, falseSignals=0 — the ladder must reach auto; got \(after.mode)"
        )
    }
}
