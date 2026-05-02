// FinalPassRetranscriptionRunnerTests.swift
// Tests for the Bug 9 charge-gated final-pass re-transcription phase.
// These tests pin the runner's gating, idempotency, and watermark
// invariants without booting the live Speech framework — the speech
// service runs against `StubSpeechRecognizer` (returns empty transcripts),
// the audio provider returns canned shards, and the AnalysisStore is a
// real on-disk SQLite under a temp directory.

import Foundation
import Testing

@testable import Playhead

@Suite("FinalPassRetranscriptionRunner")
struct FinalPassRetranscriptionRunnerTests {

    // MARK: - Fixtures

    private func makeAsset(
        id: String = "asset-fp",
        finalPassCoverageEndTime: Double? = nil
    ) -> AnalysisAsset {
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
            capabilitySnapshot: nil,
            finalPassCoverageEndTime: finalPassCoverageEndTime
        )
    }

    private func makeAdWindow(
        id: String,
        analysisAssetId: String,
        startTime: Double,
        endTime: Double,
        confidence: Double
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: analysisAssetId,
            startTime: startTime,
            endTime: endTime,
            confidence: confidence,
            boundaryState: "tentative",
            decisionState: "pending",
            detectorVersion: "v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "fixture",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }

    private func makeSnapshot(
        thermal: ThermalState = .nominal,
        isCharging: Bool = true,
        isLowPowerMode: Bool = false
    ) -> CapabilitySnapshot {
        CapabilitySnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: true,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            thermalState: thermal,
            isLowPowerMode: isLowPowerMode,
            isCharging: isCharging,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 10 * 1024 * 1024 * 1024,
            capturedAt: .now
        )
    }

    private func makeRunner(
        store: AnalysisStore,
        snapshot: CapabilitySnapshot,
        batteryLevel: Float = 0.85,
        isCharging: Bool = true,
        confidenceFloor: Double = 0.5,
        audioProvider: AnalysisAudioProviding? = nil
    ) -> FinalPassRetranscriptionRunner {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let provider = audioProvider ?? StubAnalysisAudioProvider()
        return FinalPassRetranscriptionRunner(
            store: store,
            speechService: speechService,
            audioProvider: provider,
            capabilitySnapshotProvider: { snapshot },
            batteryLevelProvider: { batteryLevel },
            chargeStateProvider: { isCharging },
            confidenceFloor: confidenceFloor,
            modelVersion: "test-final-v1"
        )
    }

    private func makeInput(
        assetId: String = "asset-fp",
        episodeId: String = "ep-asset-fp",
        podcastId: String? = "pod-1"
    ) -> FinalPassRetranscriptionRunner.AssetInput {
        let url = LocalAudioURL(URL(fileURLWithPath: "/tmp/\(episodeId).m4a"))!
        return FinalPassRetranscriptionRunner.AssetInput(
            analysisAssetId: assetId,
            podcastId: podcastId,
            audioURL: url,
            episodeId: episodeId
        )
    }

    // MARK: - Gating

    @Test("not on charge defers with batteryTooLow")
    func testNotChargingDefers() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 0, endTime: 30, confidence: 0.9)
        )
        let runner = makeRunner(
            store: store,
            snapshot: makeSnapshot(),
            isCharging: false
        )
        let result = try await runner.runFinalPassBackfill(for: makeInput())
        #expect(result.topLevelDeferReason == .batteryTooLow)
        #expect(result.admittedJobIds.isEmpty)
    }

    @Test("non-nominal thermal defers with thermalThrottled")
    func testFairThermalDefers() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 0, endTime: 30, confidence: 0.9)
        )
        let runner = makeRunner(
            store: store,
            snapshot: makeSnapshot(thermal: .fair)
        )
        let result = try await runner.runFinalPassBackfill(for: makeInput())
        #expect(result.topLevelDeferReason == .thermalThrottled)
    }

    @Test("low-power mode defers with lowPowerMode")
    func testLowPowerModeDefers() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 0, endTime: 30, confidence: 0.9)
        )
        let runner = makeRunner(
            store: store,
            snapshot: makeSnapshot(isLowPowerMode: true)
        )
        let result = try await runner.runFinalPassBackfill(for: makeInput())
        #expect(result.topLevelDeferReason == .lowPowerMode)
    }

    // MARK: - Eligibility

    @Test("low-confidence windows are skipped (< floor)")
    func testLowConfidenceSkipped() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        // Confidence below default floor (0.5).
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 0, endTime: 30, confidence: 0.3)
        )
        let runner = makeRunner(
            store: store,
            snapshot: makeSnapshot()
        )
        let result = try await runner.runFinalPassBackfill(for: makeInput())
        #expect(result.topLevelDeferReason == nil)
        #expect(result.admittedJobIds.isEmpty)
        #expect(result.reTranscribedWindowIds.isEmpty)
    }

    @Test("watermark short-circuits already-covered windows")
    func testWatermarkSkipsCoveredWindows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.advanceFinalPassCoverage(id: "asset-fp", endTime: 100.0)
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 10, endTime: 30, confidence: 0.9)
        )
        try await store.insertAdWindow(
            makeAdWindow(id: "w2", analysisAssetId: "asset-fp", startTime: 60, endTime: 90, confidence: 0.9)
        )
        let runner = makeRunner(
            store: store,
            snapshot: makeSnapshot()
        )
        let result = try await runner.runFinalPassBackfill(for: makeInput())
        #expect(result.admittedJobIds.isEmpty)
        #expect(result.reTranscribedWindowIds.isEmpty)
    }

    // MARK: - Job lifecycle

    @Test("eligible window enqueues a final_pass_jobs row and runs")
    func testEligibleWindowRuns() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 0, endTime: 30, confidence: 0.9)
        )
        // Audio provider returns a single shard intersecting the window.
        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 30, samples: [])
        ]
        let runner = makeRunner(
            store: store,
            snapshot: makeSnapshot(),
            audioProvider: audio
        )
        let result = try await runner.runFinalPassBackfill(for: makeInput())
        #expect(result.topLevelDeferReason == nil)
        #expect(result.admittedJobIds.count == 1)
        #expect(result.admittedJobIds.first == "fpj-asset-fp-w1")

        // The job row should be persisted as `complete`.
        let job = try await store.fetchFinalPassJob(byId: "fpj-asset-fp-w1")
        #expect(job?.status == .complete)
    }

    @Test("watermark advances to max retranscribed window endTime")
    func testWatermarkAdvances() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 0, endTime: 30, confidence: 0.9)
        )
        try await store.insertAdWindow(
            makeAdWindow(id: "w2", analysisAssetId: "asset-fp", startTime: 60, endTime: 90, confidence: 0.9)
        )
        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 30, samples: []),
            AnalysisShard(id: 2, episodeID: "ep-asset-fp", startTime: 60, duration: 30, samples: [])
        ]
        let runner = makeRunner(
            store: store,
            snapshot: makeSnapshot(),
            audioProvider: audio
        )
        _ = try await runner.runFinalPassBackfill(for: makeInput())

        let asset = try await store.fetchAsset(id: "asset-fp")
        #expect(asset?.finalPassCoverageEndTime == 90.0)
    }

    // MARK: - Idempotency

    @Test("second run with same inputs is a no-op")
    func testIdempotentRerun() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 0, endTime: 30, confidence: 0.9)
        )
        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 30, samples: [])
        ]
        let runner = makeRunner(
            store: store,
            snapshot: makeSnapshot(),
            audioProvider: audio
        )
        let first = try await runner.runFinalPassBackfill(for: makeInput())
        #expect(first.reTranscribedWindowIds.count == 1)

        // Second run: watermark advanced past w1.endTime, so no eligible
        // windows remain — empty result.
        let second = try await runner.runFinalPassBackfill(for: makeInput())
        #expect(second.admittedJobIds.isEmpty)
        #expect(second.reTranscribedWindowIds.isEmpty)
        #expect(second.topLevelDeferReason == nil)
    }

    // MARK: - Persistence

    @Test("FinalPassJob CRUD round-trip")
    func testFinalPassJobCRUD() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let job = FinalPassJob(
            jobId: "fpj-asset-fp-w-test",
            analysisAssetId: "asset-fp",
            podcastId: "pod-1",
            adWindowId: "w-test",
            windowStartTime: 0,
            windowEndTime: 30,
            status: .queued,
            retryCount: 0,
            deferReason: nil,
            createdAt: 1_000.0
        )
        try await store.insertOrIgnoreFinalPassJob(job)

        let fetched = try await store.fetchFinalPassJob(byId: job.jobId)
        #expect(fetched == job)

        // Lifecycle transitions.
        try await store.markFinalPassJobRunning(jobId: job.jobId)
        let running = try await store.fetchFinalPassJob(byId: job.jobId)
        #expect(running?.status == .running)

        try await store.markFinalPassJobComplete(jobId: job.jobId)
        let complete = try await store.fetchFinalPassJob(byId: job.jobId)
        #expect(complete?.status == .complete)
    }

    @Test("insertOrIgnoreFinalPassJob is idempotent on duplicate jobId")
    func testInsertOrIgnoreIdempotent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let job = FinalPassJob(
            jobId: "fpj-dup",
            analysisAssetId: "asset-fp",
            podcastId: nil,
            adWindowId: "w-dup",
            windowStartTime: 0,
            windowEndTime: 30,
            status: .queued,
            retryCount: 0,
            deferReason: nil,
            createdAt: 1_000.0
        )
        try await store.insertOrIgnoreFinalPassJob(job)
        // Second insert with same ID must not throw.
        try await store.insertOrIgnoreFinalPassJob(job)
    }

    // MARK: - Migration safety

    @Test("advanceFinalPassCoverage is monotonic")
    func testAdvanceCoverageMonotonic() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        try await store.advanceFinalPassCoverage(id: "asset-fp", endTime: 60.0)
        var asset = try await store.fetchAsset(id: "asset-fp")
        #expect(asset?.finalPassCoverageEndTime == 60.0)

        // Lower value must NOT clobber the higher watermark.
        try await store.advanceFinalPassCoverage(id: "asset-fp", endTime: 30.0)
        asset = try await store.fetchAsset(id: "asset-fp")
        #expect(asset?.finalPassCoverageEndTime == 60.0)

        // Higher value advances.
        try await store.advanceFinalPassCoverage(id: "asset-fp", endTime: 90.0)
        asset = try await store.fetchAsset(id: "asset-fp")
        #expect(asset?.finalPassCoverageEndTime == 90.0)
    }
}
