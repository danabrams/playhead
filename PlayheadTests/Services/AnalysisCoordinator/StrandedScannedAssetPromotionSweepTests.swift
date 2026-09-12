// StrandedScannedAssetPromotionSweepTests.swift
// playhead-rxbm1: the SWEEP behind the stranded-scan promotion, against a real
// store.
//
// `StrandedScannedAssetPromotionTests` rails the pure verdict. This file rails
// the thing that feeds it — `AnalysisCoordinator.promoteStrandedFullyScannedAssets()`:
// the `hasQueuedAssets` pre-check, the `analysis_assets` pagination, the
// transcript maxima read, the in-flight backfill-job guard
// (`hasInFlightBackfillJob`) and the `updateAssetState` write — through a
// temp-directory `AnalysisStore`.
//
// It has to be railed HERE rather than through a `PlayheadRuntime`, because the
// runtime's launch call is gated off under the XCTest host
// (`PlayheadRuntime.shouldRunStrandedScanSweepAtLaunch(underTest:)`): every
// runtime a test constructs opens the app's real `analysis.sqlite`, and this
// sweep writes. A rail that only existed on the runtime path would now be a
// rail nobody reaches.

import Foundation
import Testing

@testable import Playhead

/// Parks the sweep's Task at a known point so the test can cancel it BEFORE
/// the sweep runs. Same shape as `RuntimePlaybackObserverGate` in
/// `RuntimeShutdownLifecycleTests`: `hold()` suspends until `release()`.
private actor SweepStartGate {
    private var held = false
    private var heldWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    func hold() async {
        held = true
        let waiters = heldWaiters
        heldWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
        await withCheckedContinuation { continuation in
            releaseContinuation = continuation
        }
    }

    func waitUntilHeld() async {
        if held { return }
        await withCheckedContinuation { continuation in
            heldWaiters.append(continuation)
        }
    }

    func release() {
        releaseContinuation?.resume()
        releaseContinuation = nil
    }
}

@Suite(
    "AnalysisCoordinator – promoteStrandedFullyScannedAssets (playhead-rxbm1)",
    .serialized,
    .timeLimit(.minutes(3))
)
struct StrandedScannedAssetPromotionSweepTests {

    // MARK: - Construction helpers (the PersistedTerminalStateReconcileTests shape)

    private func makeStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "StrandedScannedAssetPromotionSweepTests")
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        return store
    }

    private func makeCoordinator(store: AnalysisStore) -> AnalysisCoordinator {
        let speechService = SpeechService(
            vocabularyProvider: ASRVocabularyProvider(store: store)
        )
        return AnalysisCoordinator(
            store: store,
            audioService: AnalysisAudioService(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: speechService,
                store: store
            ),
            capabilitiesService: CapabilitiesService(),
            adDetectionService: AdDetectionService(
                store: store,
                metadataExtractor: FallbackExtractor(),
                backfillJobRunnerFactory: nil,
                canUseFoundationModelsProvider: { false }
            ),
            skipOrchestrator: SkipOrchestrator(store: store)
        )
    }

    /// `state` is the raw column value: the test host's leftovers are the legacy
    /// `"new"`, which `SessionState` does not name.
    private func makeAsset(id: String, state: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(id).m4a",
            featureCoverageEndTime: 970,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: state,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 1000
        )
    }

    private func makeChunk(assetId: String, chunkIndex: Int, startTime: Double, endTime: Double) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-chunk-\(chunkIndex)",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-\(assetId)-\(chunkIndex)",
            chunkIndex: chunkIndex,
            startTime: startTime,
            endTime: endTime,
            text: "x",
            normalizedText: "x",
            pass: TranscriptPassType.fast.rawValue,
            modelVersion: "speech-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            weakAnchorMetadata: nil
        )
    }

    /// The device shape: `queued`, duration known (1000 s), feature coverage
    /// 970 s (0.97) and transcript chunks reaching 980 s (0.98) — both axes
    /// clear the 0.95 floor — with no session row.
    private func seedStrandedAsset(store: AnalysisStore, id: String) async throws {
        try await store.insertAsset(makeAsset(id: id, state: SessionState.queued.rawValue))
        try await store.insertTranscriptChunks([
            makeChunk(assetId: id, chunkIndex: 0, startTime: 0, endTime: 500),
            makeChunk(assetId: id, chunkIndex: 1, startTime: 500, endTime: 980),
        ])
    }

    private func makeJob(assetId: String, status: BackfillJobStatus) -> BackfillJob {
        BackfillJob(
            jobId: "job-\(assetId)-\(status.rawValue)",
            analysisAssetId: assetId,
            podcastId: nil,
            phase: .fullEpisodeScan,
            coveragePolicy: .fullCoverage,
            priority: 0,
            progressCursor: nil,
            // `failed` at the retry cap is exactly the row the eleven stranded
            // episodes carried on the 2026-09-12 pull.
            retryCount: status == .failed ? AdmissionController.maxRetries : 0,
            deferReason: nil,
            status: status,
            scanCohortJSON: nil,
            createdAt: Date().timeIntervalSince1970
        )
    }

    // MARK: - The case the bead is about

    @Test("a queued asset scanned to 98 %/97 % with nothing in flight is promoted on disk")
    func promotesAStrandedFullyScannedQueuedAsset() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)
        try await seedStrandedAsset(store: store, id: "stranded")

        let summary = await coordinator.promoteStrandedFullyScannedAssets()

        #expect(summary.promotedAssetIds == ["stranded"])
        #expect(summary.writeFailures == 0)
        #expect(!summary.cancelled)
        let after = try await store.fetchAsset(id: "stranded")
        #expect(after?.analysisState == SessionState.completeAdScanPartial.rawValue)
        #expect(after?.terminalReason?.contains("promoted from queued") == true)
    }

    // MARK: - The backfill-job guard, read from the table

    /// `queued` / `running` / `deferred` mean scanning is not finished and the
    /// asset must be left alone; `complete` / `failed` are terminal — `failed`
    /// at the retry cap IS the stranded shape, and must promote.
    @Test(
        "the in-flight backfill-job guard is read from backfill_jobs",
        arguments: BackfillJobStatus.allCases
    )
    func backfillJobStatusDecidesWhetherTheAssetIsTouched(_ status: BackfillJobStatus) async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)
        try await seedStrandedAsset(store: store, id: "stranded")
        try await store.insertBackfillJob(makeJob(assetId: "stranded", status: status))

        let summary = await coordinator.promoteStrandedFullyScannedAssets()
        let after = try await store.fetchAsset(id: "stranded")

        let inFlight: Set<BackfillJobStatus> = [.queued, .running, .deferred]
        if inFlight.contains(status) {
            #expect(summary.promotedAssetIds.isEmpty, "\(status.rawValue) is in flight; must not promote")
            #expect(summary.leftUnchanged == 1)
            #expect(after?.analysisState == SessionState.queued.rawValue)
        } else {
            #expect(summary.promotedAssetIds == ["stranded"], "\(status.rawValue) is terminal; must promote")
            #expect(after?.analysisState == SessionState.completeAdScanPartial.rawValue)
        }
    }

    // MARK: - The pre-check on the shape the test host actually leaves behind

    /// The XCTest host's shared store ends a full plan holding only `new`
    /// assets (17 of them, measured 2026-09-12). With nothing `queued` the
    /// sweep must be one EXISTS query and no write: the summary is all zeros
    /// and the `new` row is untouched.
    @Test("nothing queued: the pre-check returns before any page is read and nothing is written")
    func nothingQueuedIsANoOp() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)
        try await store.insertAsset(makeAsset(id: "fresh", state: "new"))

        let summary = await coordinator.promoteStrandedFullyScannedAssets()

        #expect(summary.promotedAssetIds.isEmpty)
        #expect(summary.leftUnchanged == 0)
        #expect(summary.writeFailures == 0)
        #expect(!summary.cancelled)
        let after = try await store.fetchAsset(id: "fresh")
        #expect(after?.analysisState == "new")
        #expect(after?.terminalReason == nil)
    }

    // MARK: - Cancellation: what `PlayheadRuntime.shutdown()` relies on

    /// `shutdown()` cancels the bootstrap Task and then joins it. The sweep is
    /// parked behind a gate, cancelled while parked, then released — so the
    /// cancellation is decided BEFORE the sweep's first check runs, not raced
    /// against it. The positive control is
    /// `promotesAStrandedFullyScannedQueuedAsset`: the identical seed promotes
    /// when the Task is not cancelled, so an empty result here is the
    /// cancellation and not the seed.
    @Test("a cancelled sweep promotes nothing, leaves the asset queued, and says it was cancelled")
    func aCancelledSweepPromotesNothing() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)
        try await seedStrandedAsset(store: store, id: "stranded")

        let gate = SweepStartGate()
        let sweep = Task<AnalysisCoordinator.StrandedPromotionSummary, Never> {
            await gate.hold()
            return await coordinator.promoteStrandedFullyScannedAssets()
        }
        await gate.waitUntilHeld()
        sweep.cancel()
        await gate.release()

        let summary = await sweep.value
        #expect(summary.cancelled)
        #expect(summary.promotedAssetIds.isEmpty)
        let after = try await store.fetchAsset(id: "stranded")
        #expect(after?.analysisState == SessionState.queued.rawValue, "a cancelled sweep must leave the asset for the next launch")
    }
}
