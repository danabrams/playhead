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
//   6. SAFETY (playhead-lqcp): `manual -> auto` does not fire AT ALL, at either
//      the show scalar or a per-detector ledger entry, while
//      `AutoPromotionConfidenceEvidence` has only its `.unavailable` case. This
//      is the finding wiring up (1) created: un-freezing `skipTrustScore`
//      un-freezes the top rung too, and that rung cuts audio with no gesture.
//      Section 6 also pins what the closure must NOT break — an explicit user
//      override still reaches auto.

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
        assetId: String,
        insertAsset: Bool = true
    ) async throws -> PodcastProfile {
        if insertAsset {
            try await store.insertAsset(makeAsset(id: assetId))
        }
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
            "This is the device's measured state: trust stays pinned at its creation value"
        )
        // playhead-2qz6: with no trust service nothing can record an episode
        // observation, so nothing may write the counter either. `updatePriors`
        // no longer increments — a path that cannot count episodes must not
        // write a per-backfill number into a column everything reads as
        // episodes.
        #expect(
            after.observationCount == 2,
            "No trust service means no observation and no claim — the counter must not move. Got \(after.observationCount)"
        )
        #expect(
            try await store.episodeTrustObservationCount(podcastId: podcastId) == 0,
            "and no claim may be taken, so a later backfill can still count this episode"
        )
    }

    // MARK: - 2. The counter counts EPISODES (playhead-2qz6)

    @Test("One episode counts exactly ONE observation, however many times it is backfilled")
    func oneEpisodeCountsOnceAcrossRepeatedBackfills() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-episodeunit"
        let assetId = "asset-mn5e-episodeunit"

        try await store.upsertProfile(
            makeProfile(podcastId: podcastId, mode: .shadow, trust: 0.5, observations: 7)
        )

        let service = makeService(store: store)
        await service.setTrustScoringService(TrustScoringService(store: store))

        let afterFirst = try await runOneAdBearingBackfill(
            store: store, service: service, podcastId: podcastId, assetId: assetId
        )
        #expect(afterFirst.observationCount == 8)

        // The SAME episode, backfilled twice more — the hot path, the final
        // pass, a re-drive, a transcript-version bump and the 15d0 resume path
        // all re-enter `runBackfill` for an asset already counted. On the
        // device this happened ~9 times per episode.
        for _ in 0..<2 {
            _ = try await runOneAdBearingBackfill(
                store: store, service: service,
                podcastId: podcastId, assetId: assetId, insertAsset: false
            )
        }

        let after = try #require(await store.fetchProfile(podcastId: podcastId))
        #expect(
            after.observationCount == 8,
            "Three backfills of ONE episode is ONE observation. Pre-2qz6 this landed 10 and 'shadowToManualObservations: 3' meant a third of one episode. Got \(after.observationCount)"
        )
        #expect(
            try await store.episodeTrustObservationCount(podcastId: podcastId) == 1,
            "exactly one episode claim"
        )
        #expect(
            abs(after.skipTrustScore - 0.6) < scoreTolerance,
            "and the trust bonus is paid once per episode too, not once per backfill; got \(after.skipTrustScore)"
        )
    }

    @Test("Two distinct episodes count twice — the dedupe is per episode, not per show")
    func distinctEpisodesEachCount() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-twoeps"

        try await store.upsertProfile(
            makeProfile(podcastId: podcastId, mode: .shadow, trust: 0.5, observations: 0)
        )

        let service = makeService(store: store)
        await service.setTrustScoringService(TrustScoringService(store: store))

        for idx in 0..<2 {
            _ = try await runOneAdBearingBackfill(
                store: store, service: service,
                podcastId: podcastId, assetId: "asset-mn5e-twoeps-\(idx)"
            )
        }

        let after = try #require(await store.fetchProfile(podcastId: podcastId))
        #expect(
            after.observationCount == 2,
            "A per-show claim rather than a per-episode one would freeze this at 1; got \(after.observationCount)"
        )
        #expect(try await store.episodeTrustObservationCount(podcastId: podcastId) == 2)
    }

    @Test("The claim ledger and observationCount agree — two numbers that must be equal")
    func claimLedgerAgreesWithObservationCount() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-agree"

        try await store.upsertProfile(
            makeProfile(podcastId: podcastId, mode: .shadow, trust: 0.5, observations: 0)
        )
        let service = makeService(store: store)
        await service.setTrustScoringService(TrustScoringService(store: store))

        // Three episodes, one of them backfilled twice.
        for idx in 0..<3 {
            _ = try await runOneAdBearingBackfill(
                store: store, service: service,
                podcastId: podcastId, assetId: "asset-mn5e-agree-\(idx)"
            )
        }
        _ = try await runOneAdBearingBackfill(
            store: store, service: service,
            podcastId: podcastId, assetId: "asset-mn5e-agree-0", insertAsset: false
        )

        let after = try #require(await store.fetchProfile(podcastId: podcastId))
        let claims = try await store.episodeTrustObservationCount(podcastId: podcastId)
        #expect(after.observationCount == claims,
                "observationCount=\(after.observationCount) but \(claims) episodes are claimed — the counter has drifted from what it names")
        #expect(claims == 3)
        #expect(
            after.mode == SkipMode.manual.rawValue,
            "3 real episodes is what shadowToManualObservations: 3 has always said; got \(after.mode)"
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

    @Test("A show sitting in auto can still be demoted by user vetoes")
    func promotedShowCanStillBeDemoted() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-demote"

        // `.auto` with a clean record. Under playhead-lqcp self-observation can
        // no longer put a show here — an explicit user override can, and a row
        // written by an older binary already is — so this is the posture the
        // demotion path has to keep working for, and the seed is written
        // directly rather than promoted into.
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
        // Observations and trust are both already past the manual -> auto bar.
        //
        // playhead-lqcp made this test WEAKER than it was written to be, and
        // saying so is the honest move: with the auto rung closed outright,
        // `recentFalseSkipSignals` is no longer the operative clause here —
        // `cleanManualShowDoesNotReachAuto` proves the identical profile with
        // the counter at 0 also stays manual. What survives is the claim in the
        // second assertion, which is about the counter itself and is unchanged:
        // self-observation must not decay a user's veto.
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
            "self-observation must not move a vetoed show up; got \(after.mode)"
        )
        #expect(
            after.recentFalseSkipSignals == 2,
            "recordSuccessfulObservation must not decay the veto counter — only a banner Yes (recordCorrectObservation) may. Got \(after.recentFalseSkipSignals)"
        )
    }

    // MARK: - 5. The never-run exceptionalFirstEpisodeConfidence branch
    //
    // Before playhead-mn5e, `recordSuccessfulObservation` had no production
    // caller, so its CREATE path — and with it
    // `exceptionalFirstEpisodeConfidence` (0.92) — had never executed in a
    // shipped build. It is now reachable on a brand-new show, which is exactly
    // the state of Dan's next install. These pin what it does.

    @Test("A first episode at average confidence >= 0.92 creates the show already in manual")
    func exceptionalFirstEpisodeCreatesManual() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-exceptional"
        let trust = TrustScoringService(store: store)

        // No profile exists — this drives the CREATE closure.
        await trust.recordSuccessfulObservation(
            podcastId: podcastId, averageConfidence: 0.95,
            detectors: [.fusion]
        )

        let profile = try #require(await store.fetchProfile(podcastId: podcastId))
        #expect(
            profile.mode == SkipMode.manual.rawValue,
            "0.95 >= exceptionalFirstEpisodeConfidence (0.92) creates in manual, skipping shadow entirely; got \(profile.mode)"
        )
        #expect(abs(profile.skipTrustScore - 0.5) < scoreTolerance)
        #expect(profile.observationCount == 1)
        #expect(
            profile.mode != SkipMode.auto.rawValue,
            "SAFETY: manual shows a banner. One episode must never reach auto, which cuts audio unasked"
        )
    }

    @Test("A first episode below 0.92 creates the show in shadow at trust 0.2")
    func ordinaryFirstEpisodeCreatesShadow() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-ordinary"
        let trust = TrustScoringService(store: store)

        await trust.recordSuccessfulObservation(
            podcastId: podcastId, averageConfidence: 0.91,
            detectors: [.fusion]
        )

        let profile = try #require(await store.fetchProfile(podcastId: podcastId))
        #expect(profile.mode == SkipMode.shadow.rawValue)
        #expect(
            abs(profile.skipTrustScore - 0.2) < scoreTolerance,
            "the non-exceptional seed is 0.2, so shadowToManualTrustScore (0.4) finally binds — two more clean episodes reach it exactly as observations reach 3; got \(profile.skipTrustScore)"
        )
    }

    @Test("SAFETY: a brand-new show cannot reach auto on its first real episode")
    func brandNewShowNeverReachesAutoOnEpisodeOne() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-brandnew"

        // No seeded profile at all — the real first-install path.
        let service = makeService(store: store)
        await service.setTrustScoringService(TrustScoringService(store: store))

        let after = try await runOneAdBearingBackfill(
            store: store, service: service,
            podcastId: podcastId, assetId: "asset-mn5e-brandnew"
        )

        #expect(
            after.mode != SkipMode.auto.rawValue,
            "One episode must never buy an auto-skip; got \(after.mode)"
        )
        // R5: `!= .auto` alone does not pin the CREATE closure's confidence
        // branch, and that branch is the only consumer of the second quantity
        // `recordConfirmedWindowObservation` derives. Measured: replacing the
        // whole production expression
        // `confirmedWindows.reduce(0.0){ $0 + $1.confidence } / count` with the
        // constant `1.0` — which flips every brand-new show from `shadow` to
        // `manual`, i.e. a banner from episode one on every show a listener
        // adds — passed the entire trust plan (R5 mutant M5, 95 of 95, and the
        // only failure in that run belonged to a different mutant). So the
        // ordinary branch is asserted exactly: `shadow` at the 0.2 seed, which
        // is what an average BELOW `exceptionalFirstEpisodeConfidence` (0.92)
        // produces and nothing else does. This also pins the playhead-ar60
        // half of the choice — `AdWindow.confidence` is the DETECTION number,
        // and reading `skipConfidence` (actuation, which folds in the
        // user-correction factor) here would describe the wrong thing.
        #expect(
            after.mode == SkipMode.shadow.rawValue,
            "real corpus ad-window confidences are 0.38-0.63, far below the 0.92 exceptional floor, so a first episode must create in shadow; got \(after.mode)"
        )
        #expect(
            abs(after.skipTrustScore - 0.2) < scoreTolerance,
            "the ordinary create branch seeds trust at 0.2; the exceptional one seeds 0.5, so this distinguishes them; got \(after.skipTrustScore)"
        )
        #expect(
            after.observationCount == 1,
            "and it counts as exactly one episode; got \(after.observationCount)"
        )
        #expect(try await store.episodeTrustObservationCount(podcastId: podcastId) == 1)
    }

    // MARK: - 6. playhead-lqcp: the auto rung is CLOSED
    //
    // This is the safety finding this bead's own wiring created. Un-freezing
    // `skipTrustScore` un-freezes the whole ladder, and its top rung cuts audio
    // with no gesture: from a fresh install, +0.10 trust and +1 episode each
    // time, `manual -> auto` cleared at EPISODE 8 with the user never having
    // touched anything.
    //
    // Dan's ruling permits that, conditionally — "it should go all the way to
    // auto IF IT IS HIGH CONFIDENCE" — and the condition has no quantity behind
    // it: `averageConfidence` is read once in the create closure and discarded
    // on every update, `manualToAutoTrustScore` (0.75) is cleared by the
    // observation counter itself one episode early and so can never withhold a
    // promotion, and measured real ad confidences on this corpus are 0.38-0.63.
    // A conditional whose condition cannot be evaluated is not satisfied.
    //
    // These tests pin the closure at the two places `evaluatePromotion` is
    // reachable — the SHOW scalar and a per-detector LEDGER entry — because
    // gating only the first leaves the second promoting one class to auto off
    // the same self-observed numbers, one layer down.

    @Test("playhead-lqcp: the exact profile that used to reach auto stays manual")
    func cleanManualShowDoesNotReachAuto() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-toauto"

        // IDENTICAL to `vetoedShowCannotSelfPromoteBackToAuto` except
        // falseSignals: 0 — i.e. every legacy clause of the auto rung is
        // satisfied and the veto counter is not what is holding it.
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
            after.mode == SkipMode.manual.rawValue,
            "obs=21 >= 8, trust=1.0 >= 0.75, falseSignals=0 — every legacy clause clears, and the rung is closed anyway; got \(after.mode)"
        )
        // NOT VACUOUS: the observation really was recorded, so this is the
        // closed rung and not a backfill that quietly did nothing.
        #expect(
            after.observationCount == 21,
            "the episode still counts; got \(after.observationCount)"
        )
        #expect(
            abs(after.skipTrustScore - 1.0) < scoreTolerance,
            "and trust still moved; got \(after.skipTrustScore)"
        )
    }

    /// The same closure, one layer down. `DetectorTrustLedger.seed` copies the
    /// show's trust, episode count and mode straight into a class's entry, so a
    /// show carried to (trust 1.0, obs 20) by self-observation would hand a
    /// single banner Yes everything the WEIGHTED `evaluatePromotion` needs to
    /// promote that one class to `.auto` — the same unasked skip, reached
    /// through `SkipOrchestrator`'s per-detector mode instead of the scalar.
    @Test("playhead-lqcp: a per-detector entry cannot reach auto either")
    func perDetectorEntryDoesNotReachAuto() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-detector-auto"

        try await store.upsertProfile(
            makeProfile(
                podcastId: podcastId, mode: .manual, trust: 0.9,
                observations: 20, falseSignals: 0
            )
        )

        let trust = TrustScoringService(store: store)
        await trust.recordCorrectObservation(
            podcastId: podcastId,
            analysisAssetId: "asset-mn5e-detector-auto",
            detector: .fusion
        )

        let modes = await trust.resolveDetectorModes(podcastId: podcastId)
        #expect(
            modes.mode(for: .fusion) == .manual,
            "the class the Yes credited must not promote itself to auto; got \(String(describing: modes.mode(for: .fusion)))"
        )
        let profile = try #require(await store.fetchProfile(podcastId: podcastId))
        #expect(profile.mode == SkipMode.manual.rawValue)
        // Non-vacuous: the entry exists and really did earn its bonus.
        let entry = try #require(
            profile.detectorTrustLedger.entries[SkipDetectorClass.fusion.rawValue]
        )
        #expect(entry.observationCount == 21, "got \(entry.observationCount)")
        #expect(abs(entry.trustScore - 1.0) < scoreTolerance)
    }

    /// The closure must not be mistaken for "modes never change". A user who
    /// explicitly asks for auto still gets it — `setUserOverride` writes the
    /// mode directly and does not go through `evaluatePromotion`, which is the
    /// correct asymmetry: the ruling is about what the app may conclude on its
    /// own, not about what the listener may instruct.
    @Test("playhead-lqcp: an explicit user override still reaches auto")
    func userOverrideStillReachesAuto() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-override"
        try await store.upsertProfile(
            makeProfile(podcastId: podcastId, mode: .manual, trust: 0.9, observations: 20)
        )

        let trust = TrustScoringService(store: store)
        await trust.setUserOverride(podcastId: podcastId, mode: .auto)

        let after = try #require(await store.fetchProfile(podcastId: podcastId))
        #expect(after.mode == SkipMode.auto.rawValue, "got \(after.mode)")
    }

    // MARK: - 7. The credit reaches the PER-CLASS gate, end to end (R5)

    /// R4 gave `recordSuccessfulObservation` a `detectors:` argument so a
    /// promotion could reach the value the skip gate actually reads —
    /// `SkipOrchestrator.skipMode(for:)` resolves `activeDetectorSkipModes`,
    /// a PER-CLASS verdict, and a forked ledger stops tracking the show
    /// scalar. It covered that argument with unit calls that pass the set BY
    /// HAND (`SelfObservationReachesTheGateTests`), and every end-to-end test
    /// in this suite runs on a VIRGIN ledger, where the seed still tracks the
    /// scalar and moving the scalar alone is enough.
    ///
    /// So nothing connected PRODUCTION's derivation of that set —
    /// `Set(confirmedWindows.map(\.detectorClass))` in
    /// `AdDetectionService.recordConfirmedWindowObservation` — to the ladder.
    /// R5 measured it: replacing that expression with `[]`, which is EXACTLY
    /// the pre-R4 behaviour R4 exists to fix ("moves only the scalar"), passed
    /// the whole trust plan — 94 of 94, exit 0. The argument existed and
    /// nothing proved the caller filled it.
    ///
    /// This is the missing link, and the fork is what makes it bite: until an
    /// attributed gesture materializes the ledger, a class with no stored
    /// entry reads through `DetectorTrustLedger.seed` and the show scalar is
    /// the answer. The veto below names `.userAsserted` — a class a backfill
    /// can never draw, since every fusion row carries
    /// `AdBoundaryState.acousticRefined` — so the fork is created without
    /// charging the class whose credit is under test.
    @Test("R5: a real backfill credits the classes ITS OWN windows were drawn by")
    func backfillCreditsTheClassesItsWindowsWereDrawnBy() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-mn5e-r5-forked"
        let assetId = "asset-mn5e-r5-forked"

        // obs=2 so this backfill is the third — `shadowToManualObservations`.
        try await store.upsertProfile(
            makeProfile(podcastId: podcastId, mode: .shadow, trust: 0.5, observations: 2)
        )
        let trust = TrustScoringService(store: store)
        await trust.recordFalseSkipSignal(
            podcastId: podcastId,
            attributions: [
                DetectorVetoAttribution(detector: .userAsserted, tier: .none)
            ]
        )
        let forked = try #require(await store.fetchProfile(podcastId: podcastId))
        try #require(
            forked.detectorTrustJSON != nil,
            "the veto must have forked the ledger, or the seed still tracks the scalar and this test proves nothing"
        )

        let service = makeService(store: store)
        await service.setTrustScoringService(trust)
        let after = try await runOneAdBearingBackfill(
            store: store, service: service,
            podcastId: podcastId, assetId: assetId
        )

        // The classes production actually named, read the way production reads
        // them: the non-suppressed rows' own `detectorClass`.
        let drawnBy = Set(
            try await store.fetchAdWindows(assetId: assetId)
                .filter { $0.decisionState != AdDecisionState.suppressed.rawValue }
                .map(\.detectorClass)
        )
        try #require(
            !drawnBy.isEmpty,
            "the fixture must leave non-suppressed windows, or the observation never runs"
        )
        try #require(
            !drawnBy.contains(.userAsserted),
            "premise: the vetoed class must not be one a backfill draws; got \(drawnBy)"
        )

        #expect(
            after.mode == SkipMode.manual.rawValue,
            "the show scalar must promote; got \(after.mode) obs=\(after.observationCount)"
        )
        let ledger = after.detectorTrustLedger
        let modes = await trust.resolveDetectorModes(podcastId: podcastId)
        for detector in drawnBy.sorted(by: { $0.rawValue < $1.rawValue }) {
            let entry = try #require(
                ledger.entries[detector.rawValue],
                "\(detector.rawValue) drew a confirmed window and must have a stored entry"
            )
            #expect(
                entry.observationCount == 3,
                "\(detector.rawValue) must have been credited this episode; got \(entry.observationCount)"
            )
            #expect(
                modes.mode(for: detector) == .manual,
                "\(detector.rawValue) is what the skip gate reads, and it must have moved with the scalar; got \(String(describing: modes.mode(for: detector)))"
            )
        }
    }
}
