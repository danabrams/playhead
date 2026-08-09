// CoarseScanShowIdentityTests.swift
// playhead-vtjx: the coarse-first ad-scan phase asks BOTH stores which show it
// is scanning, and repairs the one that did not know.
//
// The field case (build 2a027a48, db-overnight/db-evening 2026-08-08): four
// fully transcribed backlog assets — DE0784D8, AD5F3A0A, 53FC53E3, 83592353 —
// reached the scan-claim step and deferred with `scan_claim:podcast_id_missing`
// while ad-scan reach stayed pinned at 143 rows. The refusal was CORRECT;
// `AnalysisCoordinator.coarseScanOneAsset` asked one store, `analysis_jobs`
// .podcastId, and rendered its absence as `""`.
//
// Why that column is NULL, measured: 53 of 72 rows / 12 of 19 assets. All 19
// attributed rows were created at or after 2026-08-03 17:48:58 — playhead-kkzu,
// which made the show a compulsory spelled-out field of `DownloadContext`. kkzu
// could not reach rows already written, and every re-mint copies
// `job.podcastId` verbatim, so the NULL is inherited indefinitely: 53FC53E3's
// `…:adScanRedrive:1` row minted 2026-08-08 07:55:05 on the CURRENT build
// carries NULL because its pre-kkzu parent did, four hours before F2F2FC4C's
// identical re-drive carried a real feed URL.
//
// The identity was never missing from the DEVICE, only from that table.

import Foundation
import Testing

@testable import Playhead

// MARK: - The rule

@Suite("playhead-vtjx: which show a coarse scan runs under")
struct CoarseScanShowIdentityRuleTests {

    private static let show = "https://rss.libsyn.com/shows/101338/destinations/535577.xml"
    private static let otherShow = "https://feeds.simplecast.com/dHoohVNH"

    @Test("the analysis lane's own record is used, and reported as the lane's")
    func laneIdentityIsUsedAndNeedsNoRepair() {
        let identity = AnalysisCoordinator.resolveCoarseScanShowIdentity(
            jobPodcastId: Self.show,
            episodeStoreIdentity: nil
        )
        #expect(identity == .fromAnalysisLane(Self.show))
        #expect(identity.podcastIdArgument == Self.show)
    }

    @Test("the lane wins over the episode store when both answer")
    func laneOutranksTheEpisodeStore() {
        // The analysis lane's row is a RECORD of what the download path
        // resolved. A second observation of the same fact may fill a hole in it
        // (that is the whole of the repair) but must never re-point an episode
        // at a different show — which is what taking the episode store first
        // would silently permit.
        #expect(
            AnalysisCoordinator.resolveCoarseScanShowIdentity(
                jobPodcastId: Self.show,
                episodeStoreIdentity: Self.otherShow
            ) == .fromAnalysisLane(Self.show)
        )
    }

    /// THE BEAD'S CASE. Pre-fix there was no second source at all, so this
    /// returned the empty string and the scan was refused.
    @Test("an unattributed lane row falls back to the episode store, and owes a repair")
    func episodeStoreRecoversTheIdentity() {
        let identity = AnalysisCoordinator.resolveCoarseScanShowIdentity(
            jobPodcastId: nil,
            episodeStoreIdentity: Self.show
        )
        #expect(identity == .recoveredFromEpisodeStore(Self.show))
        #expect(identity.podcastIdArgument == Self.show)
    }

    @Test("neither store knowing is UNKNOWN, and hands runBackfill the empty string")
    func bothSilentIsUnknown() {
        let identity = AnalysisCoordinator.resolveCoarseScanShowIdentity(
            jobPodcastId: nil,
            episodeStoreIdentity: nil
        )
        #expect(identity == .unknown)
        #expect(
            identity.podcastIdArgument.isEmpty,
            """
            `.unknown` must remain the ABSENCE the playhead-fil5 gate is written \
            to catch. Inventing any non-empty value here would walk straight \
            past that gate carrying a key that joins to nothing.
            """
        )
    }

    /// The standing diagnostic: what would this read if the podcast were
    /// unknown? Not "the same as a real podcast" — from EITHER source.
    @Test(
        "a blank or non-canonical id is an absence, not a podcast, from either source",
        arguments: [
            "",
            " ",
            "   ",
            "\n",
            " https://rss.libsyn.com/shows/101338/destinations/535577.xml",
            "https://rss.libsyn.com/shows/101338/destinations/535577.xml "
        ]
    )
    func blankOrNonCanonicalIsNotAShow(_ spelling: String) {
        #expect(
            AnalysisCoordinator.resolveCoarseScanShowIdentity(
                jobPodcastId: spelling,
                episodeStoreIdentity: nil
            ) == .unknown,
            "an unusable lane value must not be adopted as a show"
        )
        #expect(
            AnalysisCoordinator.resolveCoarseScanShowIdentity(
                jobPodcastId: nil,
                episodeStoreIdentity: spelling
            ) == .unknown,
            "an unusable resolver answer must not be adopted as a show"
        )
    }

    @Test("an unusable lane value still lets a usable episode-store one through")
    func blankLaneValueFallsThroughRatherThanTerminating() {
        // `""` in the column is not "this episode has no show", it is the same
        // absence a NULL is — so it must not shadow the store that does know.
        #expect(
            AnalysisCoordinator.resolveCoarseScanShowIdentity(
                jobPodcastId: "",
                episodeStoreIdentity: Self.show
            ) == .recoveredFromEpisodeStore(Self.show)
        )
    }
}

// MARK: - The repair

@Suite("playhead-vtjx: repairing analysis_jobs.podcastId")
struct BackfillJobPodcastIdTests {

    private static let episodeId = "https://rss.libsyn.com/shows/101338/x.xml::guid-1"
    private static let otherEpisodeId = "https://rss.libsyn.com/shows/101338/x.xml::guid-2"
    private static let show = "https://rss.libsyn.com/shows/101338/x.xml"

    private func seed(
        _ store: AnalysisStore,
        jobId: String,
        episodeId: String,
        podcastId: String?,
        updatedAt: Double
    ) async throws {
        try await store.insertJob(makeAnalysisJob(
            jobId: jobId,
            jobType: "preAnalysis",
            episodeId: episodeId,
            podcastId: podcastId,
            workKey: "wk-\(jobId)",
            sourceFingerprint: "fp-\(jobId)",
            createdAt: 1_000,
            updatedAt: updatedAt
        ))
    }

    @Test("every unattributed row for the episode is filled, and counted")
    func fillsEveryNullRowForTheEpisode() async throws {
        let store = try await makeTestStore()
        try await seed(store, jobId: "j1", episodeId: Self.episodeId, podcastId: nil, updatedAt: 1)
        try await seed(store, jobId: "j2", episodeId: Self.episodeId, podcastId: nil, updatedAt: 2)

        let repaired = try await store.backfillJobPodcastId(
            episodeId: Self.episodeId, podcastId: Self.show
        )
        #expect(repaired == 2)
        #expect(try await store.fetchJob(byId: "j1")?.podcastId == Self.show)
        #expect(try await store.fetchJob(byId: "j2")?.podcastId == Self.show)
    }

    @Test("idempotent — a second call repairs nothing")
    func secondCallIsANoOp() async throws {
        let store = try await makeTestStore()
        try await seed(store, jobId: "j1", episodeId: Self.episodeId, podcastId: nil, updatedAt: 1)

        #expect(try await store.backfillJobPodcastId(
            episodeId: Self.episodeId, podcastId: Self.show
        ) == 1)
        #expect(try await store.backfillJobPodcastId(
            episodeId: Self.episodeId, podcastId: Self.show
        ) == 0)
    }

    /// The repair is a SECOND observation of a fact the lane already recorded.
    /// It may fill a hole; it may not overrule.
    @Test("an already-attributed row is never re-pointed at another show")
    func neverOverwritesAnExistingIdentity() async throws {
        let store = try await makeTestStore()
        let recorded = "https://feeds.simplecast.com/dHoohVNH"
        try await seed(
            store, jobId: "j1", episodeId: Self.episodeId, podcastId: recorded, updatedAt: 1
        )

        let repaired = try await store.backfillJobPodcastId(
            episodeId: Self.episodeId, podcastId: Self.show
        )
        #expect(repaired == 0)
        #expect(try await store.fetchJob(byId: "j1")?.podcastId == recorded)
    }

    @Test("scoped to the episode — a neighbour's unattributed rows are untouched")
    func doesNotReachOtherEpisodes() async throws {
        let store = try await makeTestStore()
        try await seed(store, jobId: "j1", episodeId: Self.episodeId, podcastId: nil, updatedAt: 1)
        try await seed(
            store, jobId: "j2", episodeId: Self.otherEpisodeId, podcastId: nil, updatedAt: 1
        )

        #expect(try await store.backfillJobPodcastId(
            episodeId: Self.episodeId, podcastId: Self.show
        ) == 1)
        #expect(try await store.fetchJob(byId: "j2")?.podcastId == nil)
    }

    /// `updatedAt` is the clock `fetchLatestJobForEpisode` orders by and the
    /// stranded-lease reaper keys on. This write records something that was
    /// always true of the row; bumping every row of an episode to one instant
    /// would re-order "latest" and disturb the reaper for no state change.
    @Test("the lifecycle clock is not disturbed")
    func doesNotBumpUpdatedAt() async throws {
        let store = try await makeTestStore()
        try await seed(store, jobId: "j1", episodeId: Self.episodeId, podcastId: nil, updatedAt: 7)
        try await seed(store, jobId: "j2", episodeId: Self.episodeId, podcastId: nil, updatedAt: 9)

        try await store.backfillJobPodcastId(episodeId: Self.episodeId, podcastId: Self.show)

        #expect(try await store.fetchJob(byId: "j1")?.updatedAt == 7)
        #expect(try await store.fetchJob(byId: "j2")?.updatedAt == 9)
    }
}

// MARK: - The field case, end to end

/// Drives the shipped `runPendingCoarseScans` over an asset in exactly the
/// shape the four backlog assets are in — transcribed, no coverage-lane row,
/// and an `analysis_jobs` row whose `podcastId` is SQL NULL — and reads the
/// claim the phase leaves behind.
///
/// The verdict is legible because the `deferReason` NAMES the gate that
/// refused. With the FM runner factory absent, an asset that clears the
/// identity gate lands on `runner_factory_missing` instead; so the assertion
/// "the reason is no longer `podcast_id_missing`" is a positive statement that
/// the scan got PAST the identity, not merely that nothing was written.
@Suite("playhead-vtjx: the four backlog assets obtain a scan claim")
struct CoarseScanShowIdentityWireInTests {

    private static let assetId = "asset-vtjx"
    private static let episodeId = "https://rss.libsyn.com/shows/101338/x.xml::guid-vtjx"
    private static let show = "https://rss.libsyn.com/shows/101338/x.xml"
    private static let episodeDuration: Double = 90

    private func chunks() -> [TranscriptChunk] {
        let texts = [
            "Welcome to the show. Today we're discussing podcasts and how to find them.",
            "This episode is brought to you by Squarespace. Use code SHOW for 20 percent off.",
            "Now back to our interview with our guest about technology trends."
        ]
        return texts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "c\(idx)-\(Self.assetId)",
                analysisAssetId: Self.assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "fast",
                modelVersion: "test-v1",
                transcriptVersion: nil,
                atomOrdinal: idx
            )
        }
    }

    /// The 53FC53E3 shape as it stood BEFORE its fil5 claim was minted:
    /// transcribed, no `backfill_jobs` row, and an `analysis_jobs` row carrying
    /// SQL NULL where the show should be.
    ///
    /// On the pull each of the four assets now owns exactly one backfill row —
    /// its own `deferred` claim — so in the field they are reached by
    /// `fetchAssetIdsWithResumableBackfillJobs` rather than by the zero-row
    /// query. Both candidate queries funnel into the same `coarseScanOneAsset`,
    /// so the fixture exercises the identical path; it models the earlier state
    /// only because a store with no claim row is the smaller fixture.
    private func seededStore() async throws -> AnalysisStore {
        let store = try await makeTestStore()
        try await store.insertAsset(
            AnalysisAsset(
                id: Self.assetId,
                episodeId: Self.episodeId,
                assetFingerprint: "fp-\(Self.assetId)",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/\(Self.assetId).m4a",
                featureCoverageEndTime: Self.episodeDuration,
                fastTranscriptCoverageEndTime: Self.episodeDuration,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: Self.episodeDuration
            )
        )
        try await store.insertTranscriptChunks(chunks())
        try await store.insertJob(makeAnalysisJob(
            jobId: "job-vtjx",
            jobType: "preAnalysis",
            episodeId: Self.episodeId,
            podcastId: nil,
            analysisAssetId: Self.assetId,
            workKey: "wk-vtjx",
            sourceFingerprint: "fp-\(Self.assetId)",
            state: "complete"
        ))
        return store
    }

    private func makeCoordinator(store: AnalysisStore) -> AnalysisCoordinator {
        AnalysisCoordinator(
            store: store,
            audioService: AnalysisAudioService(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: SpeechService(recognizer: StubSpeechRecognizer()),
                store: store
            ),
            capabilitiesService: CapabilitiesService(),
            adDetectionService: AdDetectionService(
                store: store,
                classifier: RuleBasedClassifier(),
                metadataExtractor: FallbackExtractor(),
                config: AdDetectionConfig(
                    candidateThreshold: 0.40,
                    confirmationThreshold: 0.70,
                    suppressionThreshold: 0.25,
                    hotPathLookahead: 90.0,
                    detectorVersion: "detection-v1",
                    fmBackfillMode: .shadow
                ),
                backfillJobRunnerFactory: nil,
                canUseFoundationModelsProvider: { true }
            ),
            skipOrchestrator: SkipOrchestrator(store: store)
        )
    }

    private func runCoarsePhase(_ coordinator: AnalysisCoordinator) async -> Int {
        await coordinator.runPendingCoarseScans(
            deadline: ContinuousClock.now.advanced(by: .seconds(120)),
            minimumWindowBudget: .seconds(1)
        )
    }

    private func claim(_ store: AnalysisStore) async throws -> BackfillJob? {
        try await store.fetchBackfillJob(
            byId: SemanticScanClaim.jobId(analysisAssetId: Self.assetId)
        )
    }

    /// THE PRE-FIX BEHAVIOUR, pinned so the fix cannot be silently reverted.
    /// With no resolver installed the phase has only the analysis lane to ask,
    /// which is exactly the state every build before this bead was in.
    @Test("with no episode-store resolver the scan is still refused for a missing show",
          .timeLimit(.minutes(1)))
    func withoutAResolverTheClaimIsPodcastIdMissing() async throws {
        let store = try await seededStore()
        let coordinator = makeCoordinator(store: store)

        #expect(await runCoarsePhase(coordinator) == 1, "the asset must be a candidate")

        let row = try #require(try await claim(store))
        #expect(row.deferReason == SemanticScanClaim.Gate.podcastIdMissing.deferReason)
        #expect(row.podcastId == nil, "an absent podcast must not persist as an empty-string id")
    }

    /// THE FIX. The identity the episode store holds is enough to get past the
    /// gate, and the refusal that remains names a DIFFERENT, truthful cause.
    @Test("the episode store's identity gets the scan past the gate",
          .timeLimit(.minutes(1)))
    func theEpisodeStoreIdentityAdmitsTheScan() async throws {
        let store = try await seededStore()
        let coordinator = makeCoordinator(store: store)
        await coordinator.setShowIdentityResolver { episodeId in
            episodeId == Self.episodeId ? Self.show : nil
        }

        #expect(await runCoarsePhase(coordinator) == 1)

        let row = try #require(try await claim(store))
        #expect(
            row.deferReason != SemanticScanClaim.Gate.podcastIdMissing.deferReason,
            """
            The identity gate must no longer be what refuses this asset — this \
            is the field defect (DE0784D8, AD5F3A0A, 53FC53E3, 83592353 all \
            deferred here while ad-scan reach sat at 143 rows).
            """
        )
        #expect(
            row.deferReason == SemanticScanClaim.Gate.runnerFactoryMissing.deferReason,
            """
            …and it must be refused by the NEXT gate for a true reason. This \
            fixture supplies no runner factory, so `runner_factory_missing` is \
            the honest verdict; any other reason means the scan took a path \
            this test does not describe.
            """
        )
    }

    /// The durable half: the lane is repaired, so every OTHER reader of the
    /// column — the reconciler's claim mint, the scheduler's
    /// `job.podcastId ?? ""`, rediff's latest-job read, and every re-mint that
    /// inherits it — sees the show too, without a second cross-store hop.
    @Test("the analysis lane is repaired with the recovered identity",
          .timeLimit(.minutes(1)))
    func theAnalysisLaneIsRepaired() async throws {
        let store = try await seededStore()
        let coordinator = makeCoordinator(store: store)
        await coordinator.setShowIdentityResolver { _ in Self.show }

        #expect(try await store.fetchRecordedPodcastId(forEpisodeId: Self.episodeId) == nil)
        _ = await runCoarsePhase(coordinator)

        #expect(try await store.fetchJob(byId: "job-vtjx")?.podcastId == Self.show)
        #expect(
            try await store.fetchRecordedPodcastId(forEpisodeId: Self.episodeId) == Self.show,
            "the repair must be visible to the recovery reader every other lane uses"
        )
    }

    /// A resolver that answers with a blank string is answering "I don't know"
    /// in the shape of an id. Persisting it would pool unrelated episodes under
    /// one fake show — the very thing the claim refusal exists to prevent — and
    /// it would do so while looking exactly like a repair.
    @Test("a blank resolver answer repairs nothing and refuses honestly",
          .timeLimit(.minutes(1)))
    func aBlankResolverAnswerIsNotPersisted() async throws {
        let store = try await seededStore()
        let coordinator = makeCoordinator(store: store)
        await coordinator.setShowIdentityResolver { _ in "   " }

        _ = await runCoarsePhase(coordinator)

        #expect(try await store.fetchJob(byId: "job-vtjx")?.podcastId == nil)
        let row = try #require(try await claim(store))
        #expect(row.deferReason == SemanticScanClaim.Gate.podcastIdMissing.deferReason)
    }

    /// The lane is the record. An asset that already names its show must not
    /// pay a MainActor SwiftData hop on every coarse window — and must not be
    /// re-pointed by one either.
    @Test("an attributed lane row is used as-is and the resolver is never asked",
          .timeLimit(.minutes(1)))
    func anAttributedLaneRowShortCircuitsTheResolver() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            AnalysisAsset(
                id: Self.assetId,
                episodeId: Self.episodeId,
                assetFingerprint: "fp-\(Self.assetId)",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/\(Self.assetId).m4a",
                featureCoverageEndTime: Self.episodeDuration,
                fastTranscriptCoverageEndTime: Self.episodeDuration,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: Self.episodeDuration
            )
        )
        try await store.insertTranscriptChunks(chunks())
        try await store.insertJob(makeAnalysisJob(
            jobId: "job-vtjx",
            jobType: "preAnalysis",
            episodeId: Self.episodeId,
            podcastId: Self.show,
            analysisAssetId: Self.assetId,
            workKey: "wk-vtjx",
            sourceFingerprint: "fp-\(Self.assetId)",
            state: "complete"
        ))

        let asked = AskedBox()
        let coordinator = makeCoordinator(store: store)
        await coordinator.setShowIdentityResolver { _ in
            asked.markAsked()
            return "https://feeds.simplecast.com/dHoohVNH"
        }

        _ = await runCoarsePhase(coordinator)

        #expect(asked.wasAsked == false, "the episode store must not be consulted needlessly")
        #expect(try await store.fetchJob(byId: "job-vtjx")?.podcastId == Self.show)
        let row = try #require(try await claim(store))
        #expect(row.deferReason == SemanticScanClaim.Gate.runnerFactoryMissing.deferReason)
    }

    /// **The rail on WHICH lane query is read**, and without it the choice is
    /// free: every other fixture here gives an episode one `analysis_jobs` row,
    /// and against a single row `fetchRecordedPodcastId` and
    /// `fetchLatestJobForEpisode(_:)?.podcastId` are indistinguishable — so
    /// reverting that one line passes the whole rest of this file.
    ///
    /// The distinguishing shape is the one the device is actually in: an
    /// ATTRIBUTED row plus a NEWER unattributed one. `analysis_jobs` collects
    /// several rows per episode (3–6 for the four field assets), and NULL rows
    /// keep arriving — `AnalysisJobReconciler.discoverUnEnqueuedDownloads`
    /// mints them on every reconcile pass (playhead-7ba4), 15 of them after the
    /// first attributed row on the 2026-08-08 pull. `fetchLatestJobForEpisode`
    /// orders by `updatedAt DESC` with no `podcastId IS NOT NULL` filter, so
    /// one such row shadows the show and the episode is refused a scan again —
    /// a fix that worked once and then stopped.
    @Test("a newer unattributed row must not shadow the show the lane recorded",
          .timeLimit(.minutes(1)))
    func aNewerNullRowDoesNotShadowTheRecordedIdentity() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            AnalysisAsset(
                id: Self.assetId,
                episodeId: Self.episodeId,
                assetFingerprint: "fp-\(Self.assetId)",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/\(Self.assetId).m4a",
                featureCoverageEndTime: Self.episodeDuration,
                fastTranscriptCoverageEndTime: Self.episodeDuration,
                confirmedAdCoverageEndTime: nil,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: Self.episodeDuration
            )
        )
        try await store.insertTranscriptChunks(chunks())
        // The attributed row: OLDER on both clocks.
        try await store.insertJob(makeAnalysisJob(
            jobId: "job-attributed",
            jobType: "preAnalysis",
            episodeId: Self.episodeId,
            podcastId: Self.show,
            analysisAssetId: Self.assetId,
            workKey: "wk-attributed",
            sourceFingerprint: "fp-\(Self.assetId)",
            state: "complete",
            createdAt: 1_000,
            updatedAt: 1_000
        ))
        // The reconciler's later, identity-less mint — newer `updatedAt`.
        try await store.insertJob(makeAnalysisJob(
            jobId: "job-sweep",
            jobType: "preAnalysis",
            episodeId: Self.episodeId,
            podcastId: nil,
            analysisAssetId: Self.assetId,
            workKey: "wk-sweep",
            sourceFingerprint: "fp-\(Self.assetId)",
            createdAt: 2_000,
            updatedAt: 2_000
        ))

        let asked = AskedBox()
        let coordinator = makeCoordinator(store: store)
        await coordinator.setShowIdentityResolver { _ in
            asked.markAsked()
            return nil
        }

        _ = await runCoarsePhase(coordinator)

        let row = try #require(try await claim(store))
        #expect(
            row.deferReason == SemanticScanClaim.Gate.runnerFactoryMissing.deferReason,
            """
            The recorded show must still be found with a newer unattributed row \
            present. A `podcast_id_missing` verdict here means the lane read \
            takes the LATEST row rather than the latest ATTRIBUTED one.
            """
        )
        #expect(
            asked.wasAsked == false,
            "the lane answered, so nothing should have crossed the store boundary"
        )
        // playhead-vtjx REVIEW: and the shadowing row itself is repaired, so
        // the readers that DO take the latest row stop being shadowed. See
        // `aRepairedEpisodeStaysRepairedWhenANewNullRowArrives` for why this is
        // the steady state rather than a one-off.
        #expect(
            try await store.fetchJob(byId: "job-sweep")?.podcastId == Self.show,
            "the newer unattributed row must be filled in, not merely stepped over"
        )
    }

    /// **THE CONVERGENCE RAIL (review round R1).** The first implementation
    /// repaired the lane only on the cross-store recovery branch, so a repaired
    /// episode drifted straight back to a shadowed state and nothing in the
    /// suite noticed — every other fixture here reaches the repair through
    /// `.recoveredFromEpisodeStore`.
    ///
    /// The shape is the one the device enters IMMEDIATELY AFTER a successful
    /// repair: `AnalysisJobReconciler.discoverUnEnqueuedDownloads` holds no show
    /// identity and mints a fresh NULL row on every reconcile pass
    /// (playhead-7ba4), which makes the episode MIXED. `fetchRecordedPodcastId`
    /// still answers, so the coarse lane is fine — but
    /// `fetchLatestJobForEpisode` (the reconciler's `noCoverageLaneRow` claim
    /// mint) and `job.podcastId ?? ""` (the full-pass Stage-4 dispatch) read the
    /// NULL row and reproduce this bead's own `podcast_id_missing` refusal in
    /// the sibling lane.
    @Test("a later unattributed row is repaired too, so the lane stays repaired",
          .timeLimit(.minutes(1)))
    func aRepairedEpisodeStaysRepairedWhenANewNullRowArrives() async throws {
        let store = try await seededStore()
        let coordinator = makeCoordinator(store: store)
        await coordinator.setShowIdentityResolver { _ in Self.show }

        // Pass 1: the cross-store recovery repairs the only row there is.
        _ = await runCoarsePhase(coordinator)
        #expect(try await store.fetchJob(byId: "job-vtjx")?.podcastId == Self.show)

        // The reconciler's next identity-less mint — NEWER on both clocks.
        // Derived from the seeded row rather than a literal: `makeAnalysisJob`
        // defaults both stamps to `Date()`, so a small constant here would be
        // OLDER than the row it is supposed to shadow and the precondition
        // below would pass for the wrong reason.
        let seeded = try #require(try await store.fetchJob(byId: "job-vtjx"))
        try await store.insertJob(makeAnalysisJob(
            jobId: "job-reconciled",
            jobType: "preAnalysis",
            episodeId: Self.episodeId,
            podcastId: nil,
            analysisAssetId: Self.assetId,
            workKey: "wk-reconciled",
            sourceFingerprint: "fp-\(Self.assetId)",
            createdAt: seeded.createdAt + 60,
            updatedAt: seeded.updatedAt + 60
        ))
        #expect(
            try await store.fetchLatestJobForEpisode(Self.episodeId)?.podcastId == nil,
            "precondition: the new row shadows the show for every latest-row reader"
        )

        // Pass 2: the lane answers on its own now, and must STILL repair.
        _ = await runCoarsePhase(coordinator)

        #expect(
            try await store.fetchJob(byId: "job-reconciled")?.podcastId == Self.show,
            """
            A repair that fires only when the episode store rescues the lane \
            converges for exactly one reconcile pass. Every reader that takes \
            the LATEST row — AnalysisJobReconciler's claim mint, the full-pass \
            Stage-4 `job.podcastId ?? ""` — is shadowed again the moment this \
            row lands, which is this bead's symptom in the other lane.
            """
        )
        #expect(
            try await store.fetchLatestJobForEpisode(Self.episodeId)?.podcastId == Self.show
        )
    }

    /// Lock-protected flag recording whether the injected resolver ran.
    private final class AskedBox: @unchecked Sendable {
        private let lock = NSLock()
        private var asked = false
        func markAsked() {
            lock.lock(); defer { lock.unlock() }
            asked = true
        }
        var wasAsked: Bool {
            lock.lock(); defer { lock.unlock() }
            return asked
        }
    }
}

// MARK: - The wiring

/// **THE WIRING RAIL (review round R2).** Every suite above injects the
/// resolver by hand, so all of them prove that the recovery MECHANISM works —
/// and none of them proves that anything in the app ever installs one.
/// Deleting the `setShowIdentityResolver` block from `PlayheadRuntime.init`
/// leaves this file's other 15 tests green while the shipped app reverts
/// exactly to the pre-bead behaviour: `showIdentityResolver` stays nil, the
/// episode store is never asked, and the four backlog assets keep deferring on
/// `podcast_id_missing`. That is the "a rail proves the mechanism, not that
/// anyone uses it" failure this repo has shipped before.
///
/// **What this pins, and what it deliberately does not.** It drives the REAL
/// `PlayheadRuntime`'s own coordinator through the REAL box that
/// `setEpisodePodcastIdResolver` writes, so it covers the runtime half of the
/// seam end to end — including the late-read requirement, since the box is
/// empty while `PlayheadRuntime.init` runs and is filled only afterwards here,
/// exactly as `PlayheadApp` fills it from its `WindowGroup` `.task`. The
/// remaining edge — that the `.task` itself calls
/// `setEpisodePodcastIdResolver` — is a SwiftUI scene body and is not
/// reachable without a scene; it is also PRE-EXISTING shared machinery (the
/// final-pass launch sweep depends on the same install), not something this
/// bead introduced. playhead-1shd is where making that install
/// scene-independent lives.
@MainActor
@Suite("playhead-vtjx: PlayheadRuntime wires its coordinator to the episode store")
struct CoarseScanShowIdentityRuntimeWiringTests {

    @Test("the runtime's coordinator recovers a show through the runtime's own resolver",
          .timeLimit(.minutes(1)))
    func theRuntimeInstallsTheResolverOnItsCoordinator() async throws {
        // Unique per run so no row in the runtime's real analysis store can
        // answer for it — the lane must be silent, which is what forces the
        // question through to the episode-store resolver.
        let episodeId = "https://example.com/vtjx-\(UUID().uuidString).xml::\(UUID().uuidString)"
        let show = "https://example.com/vtjx-wiring-feed.xml"

        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            // Installed AFTER construction, which is the ordering that matters:
            // the box is empty while `PlayheadRuntime.init` runs, so a wiring
            // that captured the box's CONTENTS instead of the box itself would
            // answer nil here forever.
            runtime.setEpisodePodcastIdResolver { requested in
                requested == episodeId ? show : nil
            }

            // The install is an unstructured `Task` in `init`, so wait for it
            // instead of assuming it has already run. `.unknown` is what a
            // MISSING wiring returns on every iteration, so a deleted call site
            // spends the whole budget and then fails the assertion below.
            //
            // 3s, not the 1s that was enough to prove the mutation RED on a
            // quiet box: this suite runs inside the full plan, where a freshly
            // created `Task` waits behind ~8,300 tests' worth of runnable work
            // on the global executor. The budget is the FLAKE margin, and it
            // costs nothing in the green direction — the loop exits on the
            // first iteration that sees the resolver.
            var identity = AnalysisCoordinator.CoarseScanShowIdentity.unknown
            for _ in 0..<300 {
                identity = await runtime.analysisCoordinator
                    .resolveShowIdentity(forEpisode: episodeId)
                if identity != .unknown { break }
                try await Task.sleep(for: .milliseconds(10))
            }

            #expect(
                identity == .recoveredFromEpisodeStore(show),
                """
                `PlayheadRuntime.init` must install the episode-store resolver \
                on its own `analysisCoordinator`. Without it the coordinator's \
                recovery path is dead code in the shipped app and the four \
                backlog assets (DE0784D8, AD5F3A0A, 53FC53E3, 83592353) keep \
                deferring on `podcast_id_missing`.
                """
            )
        }
    }
}
