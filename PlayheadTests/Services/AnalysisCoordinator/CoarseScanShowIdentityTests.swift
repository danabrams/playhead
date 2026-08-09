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

    /// The 53FC53E3 shape: transcribed, zero `backfill_jobs` rows, and an
    /// `analysis_jobs` row carrying SQL NULL where the show should be.
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
