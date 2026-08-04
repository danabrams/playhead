// SemanticScanClaimWireInTests.swift
// playhead-fil5, the half that matters in the field: the claim has to be
// written by the code paths that actually drop the work, and it has to turn
// into a dispatchable pass without anybody looking at a log.
//
// Two populations, two mechanisms:
//   * Episodes being analysed NOW — one of four `runShadowFMPhase` gates
//     closes, and the bail leaves a named row instead of a log line.
//   * Episodes ALREADY stranded — FCDDB309, 4FF3A238 and 48E903D7 have zero
//     coverage-lane rows and terminal analysis jobs, so nothing calls
//     `runBackfill` for them ever again. The reconciler sweep is the only
//     thing that can reach them, and `playhead-onn6`'s could not: it selects
//     assets by the state of rows they do not have.

import Foundation
import Testing

@testable import Playhead

// MARK: - The four gates

@Suite("Semantic scan claim — runShadowFMPhase gates (playhead-fil5)")
struct SemanticScanClaimGateWireInTests {

    private static let assetId = "asset-fil5"
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

    private func seededStore() async throws -> AnalysisStore {
        let store = try await makeTestStore()
        try await store.insertAsset(
            AnalysisAsset(
                id: Self.assetId,
                episodeId: "ep-\(Self.assetId)",
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
        return store
    }

    private func makeService(
        store: AnalysisStore,
        mode: FMBackfillMode,
        factory: (@Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner)?,
        canUseFM: Bool
    ) -> AdDetectionService {
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
                fmBackfillMode: mode
            ),
            backfillJobRunnerFactory: factory,
            canUseFoundationModelsProvider: { canUseFM }
        )
    }

    private func liveFactory() -> @Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner {
        { store, mode in
            BackfillJobRunner(
                store: store,
                admissionController: AdmissionController(),
                classifier: FoundationModelClassifier(runtime: TestFMRuntime().runtime),
                coveragePlanner: CoveragePlanner(),
                mode: mode,
                capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                batteryLevelProvider: { 1.0 },
                scanCohortJSON: makeTestScanCohortJSON()
            )
        }
    }

    /// The gate this bead was measured on. `FoundationModelsUsabilityProbe`
    /// caches a false for 15 minutes, so one daemon throttle can swallow every
    /// completion landing in that window — and on the sessionless callers the
    /// bd-3bz retry marker is skipped too, which made the drop permanent.
    @Test("an FM-unavailable bail leaves a durable claim")
    func fmUnavailableMintsClaim() async throws {
        let store = try await seededStore()
        let service = makeService(
            store: store, mode: .shadow, factory: liveFactory(), canUseFM: false
        )

        try await service.runBackfill(
            chunks: chunks(),
            analysisAssetId: Self.assetId,
            podcastId: "pod-1",
            episodeDuration: Self.episodeDuration,
            // No session — the AnalysisJobRunner warmup and the final-pass hook
            // both pass nil, which is exactly when the session marker is skipped.
            sessionId: nil
        )

        let row = try #require(try await claimRow(store))
        #expect(row.deferReason == SemanticScanClaim.Gate.foundationModelsUnavailable.deferReason)
        #expect(row.status == .deferred)
        #expect(row.podcastId == "pod-1")
        // The structural payoff: the asset is now a re-drive candidate.
        #expect(try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 10)
                == [Self.assetId])
    }

    /// A `knownBad` cohort resolves to `.off`, and the drop was silent.
    @Test("an fm-mode-off bail leaves a durable claim")
    func fmModeOffMintsClaim() async throws {
        let store = try await seededStore()
        let service = makeService(
            store: store, mode: .off, factory: liveFactory(), canUseFM: true
        )
        try await service.runBackfill(
            chunks: chunks(), analysisAssetId: Self.assetId,
            podcastId: "pod-1", episodeDuration: Self.episodeDuration, sessionId: nil
        )
        let row = try #require(try await claimRow(store))
        #expect(row.deferReason == SemanticScanClaim.Gate.fmModeOff.deferReason)
    }

    /// **The OTHER mode-off site, and the only caller that can reach it.**
    /// `runBackfill` now answers `.off` itself, before `runShadowFMPhase` is
    /// ever called, so the guard inside the phase is reachable from exactly one
    /// place in a shipped build: the shadow-retry drain, which does not consult
    /// the mode before dispatching. A cohort demoted to `knownBad` between the
    /// bail that flagged the session and the capability transition that drains
    /// it lands precisely here — and without this test the recording call in
    /// that guard could be deleted with every suite still green.
    @Test("a mode-off bail on the shadow-RETRY path leaves a durable claim")
    func fmModeOffOnRetryPathMintsClaim() async throws {
        let store = try await seededStore()
        _ = try await store.insertTranscriptChunks(chunks())
        try await store.insertSession(
            AnalysisSession(
                id: "sess-fil5-off",
                analysisAssetId: Self.assetId,
                state: "complete",
                startedAt: Date().timeIntervalSince1970,
                updatedAt: Date().timeIntervalSince1970,
                failureReason: nil,
                needsShadowRetry: true,
                shadowRetryPodcastId: "pod-1"
            )
        )
        let service = makeService(
            store: store, mode: .off, factory: liveFactory(), canUseFM: true
        )

        let didRun = await service.retryShadowFMPhaseForSession(sessionId: "sess-fil5-off")
        #expect(didRun == false, "a mode-off phase does not execute")

        let row = try #require(try await claimRow(store),
                               "the retry drain's mode-off bail must still leave a claim")
        #expect(row.deferReason == SemanticScanClaim.Gate.fmModeOff.deferReason)
        #expect(row.status == .deferred)
        #expect(try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 10)
                == [Self.assetId])
    }

    /// `request.podcastId ?? ""` at the final-pass hook renders an ABSENT
    /// podcast as a podcast whose id is the empty string, and the gate reads
    /// that emptiness as "skip". The claim records the absence rather than
    /// persisting "" as if it were an id.
    @Test("a missing-podcastId bail leaves a claim with no podcast, not an empty one")
    func podcastIdMissingMintsClaim() async throws {
        let store = try await seededStore()
        let service = makeService(
            store: store, mode: .shadow, factory: liveFactory(), canUseFM: true
        )
        try await service.runBackfill(
            chunks: chunks(), analysisAssetId: Self.assetId,
            podcastId: "", episodeDuration: Self.episodeDuration, sessionId: nil
        )
        let row = try #require(try await claimRow(store))
        #expect(row.deferReason == SemanticScanClaim.Gate.podcastIdMissing.deferReason)
        #expect(row.podcastId == nil, "an absent podcast must not persist as an empty-string id")
    }

    /// A nil factory is a wiring defect in production. It used to produce one
    /// `logger.warning` and no evidence anything was owed.
    @Test("a missing-runner-factory bail leaves a durable claim")
    func runnerFactoryMissingMintsClaim() async throws {
        let store = try await seededStore()
        let service = makeService(
            store: store, mode: .shadow, factory: nil, canUseFM: true
        )
        try await service.runBackfill(
            chunks: chunks(), analysisAssetId: Self.assetId,
            podcastId: "pod-1", episodeDuration: Self.episodeDuration, sessionId: nil
        )
        let row = try #require(try await claimRow(store))
        #expect(row.deferReason == SemanticScanClaim.Gate.runnerFactoryMissing.deferReason)
    }

    /// **The vacuity control.** When every gate is open the runner mints its own
    /// rows and runs them; a claim row appearing here would mean the gate wiring
    /// fires on the happy path, and the whole `deferReason` ledger would be
    /// noise. The id is the same one the claim would have used, so this also
    /// proves the claim and the runner share an identity rather than racing for
    /// two rows.
    @Test("an open gate mints no claim — the runner's own row does the work")
    func openGateMintsNoClaim() async throws {
        let store = try await seededStore()
        let service = makeService(
            store: store, mode: .shadow, factory: liveFactory(), canUseFM: true
        )
        try await service.runBackfill(
            chunks: chunks(), analysisAssetId: Self.assetId,
            podcastId: "pod-1", episodeDuration: Self.episodeDuration, sessionId: nil
        )
        let row = try #require(try await claimRow(store),
                               "the runner must still have minted its fullCoverage row")
        #expect((row.deferReason ?? "").hasPrefix(SemanticScanClaim.deferReasonPrefix) == false,
                "a row on the happy path must not carry a scan-claim reason")
        #expect(row.status != .deferred || row.deferReason != nil)
    }

    /// The row the claim would name, whichever wrote it.
    private func claimRow(_ store: AnalysisStore) async throws -> BackfillJob? {
        try await store.fetchBackfillJob(byId: SemanticScanClaim.jobId(
            analysisAssetId: Self.assetId,
            transcriptVersion: SemanticScanClaim.transcriptVersion(forPersistedChunks: chunks())
        ))
    }
}

// MARK: - Reaching the episodes with no rows at all

@Suite("Semantic scan claim — reconciler sweep (playhead-fil5)")
struct SemanticScanClaimReconcilerTests {

    private static let episodeId = "ep-FCDDB309"
    private static let assetId = "asset-FCDDB309"
    private static let fingerprint = "fp-FCDDB309"
    private static let durationSec: Double = 2_113

    /// FCDDB309's exact shape: fully transcribed, ZERO `backfill_jobs` rows,
    /// zero `semantic_scan_results`, and an `analysis_jobs` row that is already
    /// terminal — so `runBackfill` will never be called for it again and the
    /// coverage-lane row that would make it a re-drive candidate can never
    /// appear on its own.
    private func seedZeroRowAsset(
        _ store: AnalysisStore,
        transcriptEndSec: Double = durationSec,
        scannedSec: Double? = nil,
        jobState: String = "complete"
    ) async throws {
        try await store.insertAsset(
            AnalysisAsset(
                id: Self.assetId,
                episodeId: Self.episodeId,
                assetFingerprint: Self.fingerprint,
                weakFingerprint: nil,
                sourceURL: "file:///tmp/FCDDB309.m4a",
                featureCoverageEndTime: transcriptEndSec,
                fastTranscriptCoverageEndTime: transcriptEndSec,
                confirmedAdCoverageEndTime: nil,
                analysisState: SessionState.completeFull.rawValue,
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: Self.durationSec
            )
        )
        _ = try await store.insertTranscriptChunks([
            claimTestChunk(assetId: Self.assetId, index: 0, start: 0, end: transcriptEndSec)
        ])
        if let scannedSec {
            try await store.insertSemanticScanResult(
                claimTestScan(assetId: Self.assetId, index: 0, start: 0, end: scannedSec)
            )
        }
        try await store.insertJob(
            makeAnalysisJob(
                jobId: "job-terminal",
                jobType: "preAnalysis",
                episodeId: Self.episodeId,
                podcastId: "pod-FCDDB309",
                analysisAssetId: Self.assetId,
                workKey: AnalysisJob.computeWorkKey(
                    fingerprint: Self.fingerprint,
                    analysisVersion: PreAnalysisConfig.analysisVersion,
                    jobType: "preAnalysis"
                ),
                sourceFingerprint: Self.fingerprint,
                desiredCoverageSec: Self.durationSec,
                state: jobState
            )
        )
    }

    /// One more zero-row candidate, distinct from `seedZeroRowAsset`'s single
    /// FCDDB309 fixture, for the tests that need a POPULATION rather than a
    /// shape. `qualifying: false` gives it a half-length transcript, so it is
    /// still a candidate the sweep must EXAMINE but never one it may claim —
    /// which is what separates the candidate window from the mint cap.
    private func seedExtraCandidate(
        _ store: AnalysisStore,
        ordinal: Int,
        qualifying: Bool = true
    ) async throws {
        let id = String(format: "bulk-%03d", ordinal)
        let transcriptEnd = qualifying ? Self.durationSec : 0.5 * Self.durationSec
        try await store.insertAsset(
            AnalysisAsset(
                id: id,
                episodeId: "ep-\(id)",
                assetFingerprint: "fp-\(id)",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/\(id).m4a",
                featureCoverageEndTime: transcriptEnd,
                fastTranscriptCoverageEndTime: transcriptEnd,
                confirmedAdCoverageEndTime: nil,
                analysisState: SessionState.completeFull.rawValue,
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: Self.durationSec
            )
        )
        _ = try await store.insertTranscriptChunks([
            claimTestChunk(assetId: id, index: 0, start: 0, end: transcriptEnd)
        ])
        try await store.insertJob(
            makeAnalysisJob(
                jobId: "job-\(id)",
                jobType: "preAnalysis",
                episodeId: "ep-\(id)",
                podcastId: "pod-\(id)",
                analysisAssetId: id,
                workKey: AnalysisJob.computeWorkKey(
                    fingerprint: "fp-\(id)",
                    analysisVersion: PreAnalysisConfig.analysisVersion,
                    jobType: "preAnalysis"
                ),
                sourceFingerprint: "fp-\(id)",
                desiredCoverageSec: Self.durationSec,
                state: "complete"
            )
        )
    }

    private func cachedDownloads() -> StubDownloadProvider {
        let downloads = StubDownloadProvider()
        downloads.cachedURLs[Self.episodeId] = URL(fileURLWithPath: "/tmp/FCDDB309.m4a")
        downloads.fingerprints[Self.episodeId] = AudioFingerprint(
            weak: Self.fingerprint, strong: Self.fingerprint
        )
        return downloads
    }

    private func makeReconciler(
        store: AnalysisStore,
        downloads: StubDownloadProvider
    ) -> AnalysisJobReconciler {
        AnalysisJobReconciler(
            store: store,
            downloadManager: downloads,
            capabilitiesService: StubCapabilitiesProvider()
        )
    }

    private func redriveJobs(_ store: AnalysisStore) async throws -> [AnalysisJob] {
        try await store.fetchJobsByState("queued").filter {
            AnalysisWorkScheduler.adScanRedriveOrdinal(workKey: $0.workKey) != nil
        }
    }

    /// **The headline.** One reconcile turns a zero-row asset into a
    /// dispatchable ad-scan pass. Before this bead the same fixture produced
    /// nothing at all, forever, at any coverage.
    @Test("a zero-row transcribed asset gets a claim AND a re-drive in one pass")
    func zeroRowAssetIsReached() async throws {
        let store = try await makeTestStore()
        try await seedZeroRowAsset(store)

        // Before: nothing dispatchable, and the onn6 selector cannot see it.
        #expect(try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 10).isEmpty)
        #expect(try await store.fetchNextEligibleJob(
            deferredWorkAllowed: true, t0ThresholdSec: 300,
            now: Date().timeIntervalSince1970
        ) == nil)

        let report = try await makeReconciler(
            store: store, downloads: cachedDownloads()
        ).reconcile()

        #expect(report.semanticScanClaimsMinted == 1)
        #expect(report.adScanRedrivesMinted == 1,
                "the claim must be actionable in the SAME pass, not the next launch")

        let claim = try #require(try await store.fetchBackfillJob(byId: SemanticScanClaim.jobId(
            analysisAssetId: Self.assetId,
            transcriptVersion: SemanticScanClaim.transcriptVersion(
                forPersistedChunks: try await store.fetchTranscriptChunks(assetId: Self.assetId)
            )
        )))
        #expect(claim.deferReason == SemanticScanClaim.Gate.noCoverageLaneRow.deferReason)
        #expect(claim.podcastId == "pod-FCDDB309")

        let minted = try await redriveJobs(store)
        #expect(minted.count == 1)
        #expect(try await store.fetchNextEligibleJob(
            deferredWorkAllowed: true, t0ThresholdSec: 300,
            now: Date().timeIntervalSince1970
        )?.jobId == minted.first?.jobId)
    }

    /// The claim removes the asset from its own candidate set on the first
    /// pass, so relaunching cannot manufacture claims. What happens after is
    /// the re-drive sweep's existing, budgeted problem.
    @Test("repeated reconciles mint exactly one claim, ever")
    func claimMintingIsOnceOnly() async throws {
        let store = try await makeTestStore()
        try await seedZeroRowAsset(store)
        let reconciler = makeReconciler(store: store, downloads: cachedDownloads())

        var minted: [Int] = []
        for _ in 0..<6 {
            for job in try await redriveJobs(store) {
                try await store.updateJobState(
                    jobId: job.jobId, state: "complete",
                    nextEligibleAt: nil, lastErrorCode: nil
                )
            }
            minted.append(try await reconciler.reconcile().semanticScanClaimsMinted)
        }
        #expect(minted == [1, 0, 0, 0, 0, 0], "claims minted per pass: \(minted)")
        #expect(try await store.countResumableBackfillJobs(assetId: Self.assetId) == 1)
    }

    /// **The field shape, end to end, and the one the sweep silently refused
    /// until R3.** Every previous fixture in this suite gives the asset ONE
    /// chunk spanning the whole episode, which is the only kind of transcript
    /// whose raw interval union equals its reach. A real one is thousands of
    /// speech runs separated by breaths: FCDDB309 is 2,240 fast chunks with 620
    /// gaps of median 0.120 s, and its raw union is 87.3 % of a duration it has
    /// transcribed to 99.9 %.
    ///
    /// So a contiguous-chunk fixture cannot tell a correct gate from one that
    /// mints nothing, and the sweep measured **0 of 12** assets over the floor
    /// on the 2026-08-03 pull while every test in this file passed. This is the
    /// test that fails when the numerator goes back to the raw union.
    @Test("an asset transcribed in real speech runs — breaths, not blocks — is claimed")
    func breathGappedTranscriptIsClaimed() async throws {
        let runs = 400
        let ranges = SemanticScanClaimPredicateTests.speechRanges(count: runs)
        let duration = SemanticScanClaimPredicateTests.speechSpan(count: runs)

        let store = try await makeTestStore()
        try await store.insertAsset(
            AnalysisAsset(
                id: Self.assetId,
                episodeId: Self.episodeId,
                assetFingerprint: Self.fingerprint,
                weakFingerprint: nil,
                sourceURL: "file:///tmp/FCDDB309.m4a",
                featureCoverageEndTime: duration,
                fastTranscriptCoverageEndTime: duration,
                confirmedAdCoverageEndTime: nil,
                analysisState: SessionState.completeFull.rawValue,
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: duration
            )
        )
        _ = try await store.insertTranscriptChunks(ranges.enumerated().map { index, range in
            claimTestChunk(assetId: Self.assetId, index: index, start: range.start, end: range.end)
        })
        try await store.insertJob(
            makeAnalysisJob(
                jobId: "job-terminal",
                jobType: "preAnalysis",
                episodeId: Self.episodeId,
                podcastId: "pod-FCDDB309",
                analysisAssetId: Self.assetId,
                workKey: AnalysisJob.computeWorkKey(
                    fingerprint: Self.fingerprint,
                    analysisVersion: PreAnalysisConfig.analysisVersion,
                    jobType: "preAnalysis"
                ),
                sourceFingerprint: Self.fingerprint,
                desiredCoverageSec: duration,
                state: "complete"
            )
        )

        // The fixture must genuinely straddle the floor, or it proves nothing:
        // the store's own raw union has to read UNDER 0.95 while the transcript
        // is complete. This is the assertion that makes the test non-vacuous.
        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds([Self.assetId])[Self.assetId]
        )
        #expect(SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: summary.fastTranscriptCoveredSec,
            episodeDurationSec: summary.episodeDurationSec
        ) == false, "the RAW union must fail the floor, else this fixture is contiguous")

        let report = try await makeReconciler(
            store: store, downloads: cachedDownloads()
        ).reconcile()
        #expect(report.semanticScanClaimsMinted == 1,
                "a fully transcribed episode must be claimable")
        #expect(report.adScanRedrivesMinted == 1)
    }

    /// A half-transcribed asset belongs to the transcript lane. Claiming here
    /// would spend an ad-scan re-drive on a pass whose real job is
    /// transcription — and the ad-scan budget is only two.
    @Test("an asset short of the transcript finalize floor gets no claim")
    func shortTranscriptGetsNoClaim() async throws {
        let store = try await makeTestStore()
        try await seedZeroRowAsset(store, transcriptEndSec: 0.5 * Self.durationSec)
        let report = try await makeReconciler(
            store: store, downloads: cachedDownloads()
        ).reconcile()
        #expect(report.semanticScanClaimsMinted == 0)
        #expect(try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 10).isEmpty)
    }

    /// **The numerator, not the constant.** `finalizeBackfill`'s own verdict
    /// divides `max(endTime)` — a watermark — by the duration, and this
    /// transcript's watermark is the full episode. The sweep divides the
    /// interval UNION instead, which is 60 % here, so the asset is refused.
    ///
    /// That divergence is the whole point: a semantic scan can only read text
    /// that exists, and the watermark-reads-100 %-over-holes shape is the same
    /// one playhead-sd71 removed from the Activity screen. A fixture with a
    /// contiguous transcript cannot tell the two apart, so without this test
    /// the sweep could be switched to `fastTranscriptCoverageEndSec` with every
    /// suite still green.
    @Test("a GAPPY transcript whose watermark reads 100% still gets no claim")
    func gappyTranscriptGetsNoClaim() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            AnalysisAsset(
                id: Self.assetId,
                episodeId: Self.episodeId,
                assetFingerprint: Self.fingerprint,
                weakFingerprint: nil,
                sourceURL: "file:///tmp/FCDDB309.m4a",
                featureCoverageEndTime: Self.durationSec,
                fastTranscriptCoverageEndTime: Self.durationSec,
                confirmedAdCoverageEndTime: nil,
                analysisState: SessionState.completeFull.rawValue,
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: Self.durationSec
            )
        )
        // Union = 0.50 + 0.10 = 0.60 of the episode; watermark = 1.00 of it.
        _ = try await store.insertTranscriptChunks([
            claimTestChunk(assetId: Self.assetId, index: 0,
                           start: 0, end: 0.50 * Self.durationSec),
            claimTestChunk(assetId: Self.assetId, index: 1,
                           start: 0.90 * Self.durationSec, end: Self.durationSec)
        ])
        try await store.insertJob(
            makeAnalysisJob(
                jobId: "job-terminal",
                jobType: "preAnalysis",
                episodeId: Self.episodeId,
                podcastId: "pod-FCDDB309",
                analysisAssetId: Self.assetId,
                workKey: AnalysisJob.computeWorkKey(
                    fingerprint: Self.fingerprint,
                    analysisVersion: PreAnalysisConfig.analysisVersion,
                    jobType: "preAnalysis"
                ),
                sourceFingerprint: Self.fingerprint,
                desiredCoverageSec: Self.durationSec,
                state: "complete"
            )
        )

        // The fixture has to actually exercise the difference, or the test is
        // vacuous: the two measures must disagree ACROSS the 0.95 floor.
        let summary = try #require(
            try await store.fetchCoverageSummariesByAssetIds([Self.assetId])[Self.assetId]
        )
        #expect(SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: summary.fastTranscriptCoverageEndSec,
            episodeDurationSec: summary.episodeDurationSec
        ), "the watermark must clear the floor, else this proves nothing")

        let report = try await makeReconciler(
            store: store, downloads: cachedDownloads()
        ).reconcile()
        #expect(report.semanticScanClaimsMinted == 0)
        #expect(try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 10).isEmpty)
    }

    /// An episode whose scan already read the audio is not owed one. Without
    /// this the sweep would claim every asset whose coverage-lane rows had been
    /// garbage-collected after a successful scan.
    @Test("an already-scanned asset gets no claim")
    func scannedAssetGetsNoClaim() async throws {
        let store = try await makeTestStore()
        try await seedZeroRowAsset(store, scannedSec: Self.durationSec)
        let report = try await makeReconciler(
            store: store, downloads: cachedDownloads()
        ).reconcile()
        #expect(report.semanticScanClaimsMinted == 0)
    }

    /// An asset that already owns coverage-lane work is `playhead-onn6`'s
    /// population, not this one. Claiming for it would insert a second row for
    /// work already represented.
    @Test("an asset that already has a coverage-lane row gets no claim")
    func existingRowSuppressesClaim() async throws {
        let store = try await makeTestStore()
        try await seedZeroRowAsset(store)
        try await store.insertBackfillJob(makeBackfillJob(
            jobId: "pre-existing", analysisAssetId: Self.assetId,
            phase: .scanLikelyAdSlots, status: .queued
        ))
        let report = try await makeReconciler(
            store: store, downloads: cachedDownloads()
        ).reconcile()
        #expect(report.semanticScanClaimsMinted == 0)
        // Even a COMPLETE row means the lane exists and this sweep is not its owner.
        try await store.markBackfillJobComplete(jobId: "pre-existing", progressCursor: nil)
        #expect(try await makeReconciler(
            store: store, downloads: cachedDownloads()
        ).reconcile().semanticScanClaimsMinted == 0)
    }

    /// **The candidate query names one set and this sweep reads it as another.**
    /// "No coverage-lane row" is not "nobody will call `runBackfill` again":
    /// `runPendingBackfill` is step 6 of a pass, so an episode being analysed
    /// RIGHT NOW is a zero-row asset from the moment its transcript crosses the
    /// finalize floor until the shadow phase runs. `reconcile()` reaches that
    /// state for real — playhead-qk44 documents `BGProcessingTaskHandler`
    /// running inside the live app process while the foreground runner drains.
    ///
    /// Claiming there is wrong twice over: it spends the per-pass mint budget on
    /// an episode that was about to request a scan by itself, ahead of the
    /// stranded ones that never will, and it stamps a gate reason asserting
    /// nothing asked while a request is in flight.
    @Test("an episode with an analysis pass in flight gets no claim")
    func inFlightEpisodeGetsNoClaim() async throws {
        let store = try await makeTestStore()
        try await seedZeroRowAsset(store, jobState: "queued")

        let firstPass = try await makeReconciler(
            store: store, downloads: cachedDownloads()
        ).reconcile()
        #expect(firstPass.semanticScanClaimsMinted == 0,
                "the pass that will mint this asset's rows has not run yet")
        #expect(try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 10).isEmpty)

        // The fixture is only meaningful if the SAME asset claims once its pass
        // is terminal — otherwise this would pass for any unrelated reason.
        try await store.updateJobState(
            jobId: "job-terminal", state: "complete",
            nextEligibleAt: nil, lastErrorCode: nil
        )
        #expect(try await makeReconciler(
            store: store, downloads: cachedDownloads()
        ).reconcile().semanticScanClaimsMinted == 1)
    }

    /// The MINT cap. Without it one `reconcile()` empties the whole candidate
    /// window into a queue the very next step can only drain 8 of
    /// (``AnalysisJobReconciler/maxAdScanRedrivesPerReconcile``), so the surplus
    /// buys nothing and the claims would have been there next launch anyway.
    ///
    /// The second and third passes are the other half of the contract: a cap is
    /// only a cap if the backlog DRAINS across launches rather than being
    /// dropped.
    @Test("one pass mints at most the cap, and the rest drain next launch")
    func mintCapBoundsOnePass() async throws {
        let store = try await makeTestStore()
        let total = AnalysisJobReconciler.maxSemanticScanClaimsPerReconcile + 4
        for ordinal in 0..<total {
            try await seedExtraCandidate(store, ordinal: ordinal)
        }
        let reconciler = makeReconciler(store: store, downloads: StubDownloadProvider())

        var minted: [Int] = []
        for _ in 0..<3 {
            minted.append(try await reconciler.reconcile().semanticScanClaimsMinted)
        }
        #expect(minted == [AnalysisJobReconciler.maxSemanticScanClaimsPerReconcile, 4, 0],
                "claims minted per pass: \(minted)")
        #expect(try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 100).count == total,
                "every candidate must end up visible to the re-drive sweep")
    }

    /// The CANDIDATE window, which is a different bound from the mint cap and
    /// cannot be observed through it: the mint cap stops the loop at 8 MINTS, so
    /// it never reaches the 24th candidate unless the ones ahead are all
    /// REJECTED. That is the shape here — a wall of half-transcribed assets,
    /// oldest-first, with the one claimable asset behind them.
    ///
    /// Both halves are needed. The first alone would pass if the sweep were
    /// broken outright; the second proves the same asset claims the moment it
    /// fits inside the window.
    @Test("a claimable asset beyond the candidate window waits for the next pass")
    func candidateWindowBoundsOnePass() async throws {
        let window = AnalysisJobReconciler.maxSemanticScanClaimCandidatesPerReconcile

        let blocked = try await makeTestStore()
        for ordinal in 0..<window {
            try await seedExtraCandidate(blocked, ordinal: ordinal, qualifying: false)
        }
        try await seedZeroRowAsset(blocked)
        #expect(try await makeReconciler(
            store: blocked, downloads: cachedDownloads()
        ).reconcile().semanticScanClaimsMinted == 0,
                "the claimable asset sorts behind a full window of rejects")

        let reached = try await makeTestStore()
        for ordinal in 0..<(window - 1) {
            try await seedExtraCandidate(reached, ordinal: ordinal, qualifying: false)
        }
        try await seedZeroRowAsset(reached)
        #expect(try await makeReconciler(
            store: reached, downloads: cachedDownloads()
        ).reconcile().semanticScanClaimsMinted == 1,
                "one fewer reject and the same asset is inside the window")
    }

    /// **A window is a rate limit only if it MOVES.** A rejected candidate
    /// writes nothing, so it is still a candidate — in the same oldest-first
    /// position — next pass. A full window of assets the sweep can never accept
    /// therefore starves everything behind it FOREVER on a fixed `LIMIT`, and
    /// "never" is a real state rather than a limit case: on the 2026-08-03 pull
    /// two of the four zero-row assets are half-transcribed with terminal
    /// analysis jobs, so their transcripts will not grow and their rejection is
    /// permanent.
    ///
    /// The same fixture `candidateWindowBoundsOnePass` proves is blocked on pass
    /// one must therefore be REACHED on pass two. Deleting the cursor leaves
    /// that suite green and this test red, which is the whole point of it being
    /// a separate test: one asserts the bound, this asserts liveness.
    @Test("a claimable asset behind a full window of permanent rejects is reached next pass")
    func candidateWindowRotatesPastPermanentRejects() async throws {
        let window = AnalysisJobReconciler.maxSemanticScanClaimCandidatesPerReconcile
        let store = try await makeTestStore()
        // Every one of these is half-transcribed with a `complete` analysis job:
        // rejected this pass, rejected every pass, never removed from the set.
        for ordinal in 0..<window {
            try await seedExtraCandidate(store, ordinal: ordinal, qualifying: false)
        }
        try await seedZeroRowAsset(store)

        // ONE reconciler across both passes — the cursor is its state, and a
        // fresh instance would legitimately restart at the oldest candidate.
        let reconciler = makeReconciler(store: store, downloads: cachedDownloads())
        var minted: [Int] = []
        for _ in 0..<3 {
            minted.append(try await reconciler.reconcile().semanticScanClaimsMinted)
        }
        #expect(minted == [0, 1, 0], "claims minted per pass: \(minted)")
        #expect(try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 100)
                == [Self.assetId],
                "the one claimable asset, and only it, reached the re-drive sweep")
    }

    /// A claim is a request, not a dispatchable row, and the pass it unblocks is
    /// already counted as `adScanRedrivesMinted`. Counting both would report the
    /// same repair twice to the background-task ledger.
    @Test("claims are excluded from recoveredWorkCount")
    func claimsDoNotInflateRecoveredWork() async throws {
        let store = try await makeTestStore()
        try await seedZeroRowAsset(store)
        let report = try await makeReconciler(
            store: store, downloads: cachedDownloads()
        ).reconcile()
        #expect(report.semanticScanClaimsMinted == 1)
        let withoutClaims = report.expiredLeasesRecovered
            + report.recoveredStrandedSessionJobs
            + report.missingFilesUnblocked
            + report.modelsUnblocked
            + report.staleVersionsReenqueued
            + report.unEnqueuedDownloadsCreated
            + report.strandedBackfillJobsReset
            + report.strandedFinalPassJobsReset
            + report.adScanRedrivesMinted
            + report.capOutRetriesMinted
        #expect(report.recoveredWorkCount == withoutClaims)
    }
}

// MARK: - The selector

@Suite("Semantic scan claim — zero-row selector (playhead-fil5)")
struct MissingCoverageLaneSelectorTests {

    private func seedAsset(
        _ store: AnalysisStore,
        id: String,
        withChunk: Bool = true
    ) async throws {
        try await store.insertAsset(
            AnalysisAsset(
                id: id,
                episodeId: "ep-\(id)",
                assetFingerprint: "fp-\(id)",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/\(id).m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: SessionState.completeFull.rawValue,
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: 2_113
            )
        )
        if withChunk {
            _ = try await store.insertTranscriptChunks([
                claimTestChunk(assetId: id, index: 0, start: 0, end: 2_113)
            ])
        }
    }

    /// A row of ANY status means the coverage lane exists for this asset and
    /// `playhead-onn6` owns it. `complete` matters most: an asset whose scan
    /// finished must not be re-claimed on every launch.
    @Test("any coverage-lane row excludes the asset, whatever its status")
    func anyRowExcludes() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, id: "a-none")
        for status in BackfillJobStatus.allCases {
            let id = "a-\(status.rawValue)"
            try await seedAsset(store, id: id)
            try await store.insertBackfillJob(makeBackfillJob(
                jobId: "bf-\(id)", analysisAssetId: id, status: status
            ))
        }
        #expect(try await store.fetchAssetIdsMissingCoverageLaneJobs(limit: 50) == ["a-none"])
    }

    /// No transcript means nothing for a semantic scan to read, and no
    /// transcript version to name a job with.
    @Test("an asset with no transcript is not a candidate")
    func noTranscriptExcludes() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, id: "a-transcribed")
        try await seedAsset(store, id: "a-bare", withChunk: false)
        #expect(try await store.fetchAssetIdsMissingCoverageLaneJobs(limit: 50)
                == ["a-transcribed"])
    }

    /// Oldest first, so a backlog drains in the order it stranded rather than
    /// whatever the query planner returns; and the limit is honoured so the
    /// sweep's cost is bounded per launch.
    ///
    /// The ids are deliberately in REVERSE alphabetical order of insertion, so
    /// an implementation that tiebreaks on `id` (the obvious choice, and the
    /// wrong one — `analysis_assets.createdAt` is whole seconds, so a test's
    /// three inserts share one timestamp) returns them backwards and fails.
    @Test("candidates come back oldest-first and respect the limit")
    func orderingAndLimit() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, id: "zz-first")
        try await seedAsset(store, id: "mm-second")
        try await seedAsset(store, id: "aa-third")
        #expect(try await store.fetchAssetIdsMissingCoverageLaneJobs(limit: 50)
                == ["zz-first", "mm-second", "aa-third"])
        #expect(try await store.fetchAssetIdsMissingCoverageLaneJobs(limit: 2)
                == ["zz-first", "mm-second"])
        #expect(try await store.fetchAssetIdsMissingCoverageLaneJobs(limit: 0).isEmpty)
    }

    /// The offset pages the SAME total order, so the caller can walk past
    /// candidates it can never accept instead of re-reading them forever. Off
    /// the end returns empty rather than wrapping — wrapping is the caller's
    /// decision, and a query that silently restarted would make "I have seen
    /// everything" unobservable.
    @Test("the offset pages the candidate order and clamps a negative")
    func offsetPagesTheOrder() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, id: "zz-first")
        try await seedAsset(store, id: "mm-second")
        try await seedAsset(store, id: "aa-third")
        #expect(try await store.fetchAssetIdsMissingCoverageLaneJobs(limit: 2, offset: 1)
                == ["mm-second", "aa-third"])
        #expect(try await store.fetchAssetIdsMissingCoverageLaneJobs(limit: 2, offset: 2)
                == ["aa-third"])
        #expect(try await store.fetchAssetIdsMissingCoverageLaneJobs(limit: 2, offset: 3).isEmpty)
        // A negative offset is clamped, not passed through: SQLite reads it as
        // "no offset", and relying on that would be an invariant held by a
        // dependency rather than by this function.
        #expect(try await store.fetchAssetIdsMissingCoverageLaneJobs(limit: 1, offset: -5)
                == ["zz-first"])
    }
}
