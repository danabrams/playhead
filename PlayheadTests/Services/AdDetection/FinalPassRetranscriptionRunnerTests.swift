// FinalPassRetranscriptionRunnerTests.swift
// Tests for the Bug 9 charge-gated final-pass re-transcription phase.
// These tests pin the runner's gating, idempotency, and resume-guard
// invariants without booting the live Speech framework — the speech
// service runs against `StubSpeechRecognizer` (returns empty transcripts),
// the audio provider returns canned shards, and the AnalysisStore is a
// real on-disk SQLite under a temp directory.

import CryptoKit
import Foundation
import os
import Testing

@testable import Playhead

// MARK: - Per-shard test recognizer (playhead-5147)

/// Recognizer that returns one segment per shard with text derived from
/// the shard's `startTime`. Used by the per-shard cooperative thermal
/// check tests so each shard produces persistable chunks (the default
/// `StubSpeechRecognizer` returns empty transcripts and would never
/// exercise the "partial chunks landed on mid-window defer" branch).
private final class CountingShardRecognizer: SpeechRecognizer, @unchecked Sendable {
    private let lock = OSAllocatedUnfairLock(initialState: State())
    private let speakerId: Int?
    private struct State {
        var loaded = false
        var transcribeCount = 0
    }

    init(speakerId: Int? = nil) {
        self.speakerId = speakerId
    }

    var transcribeCount: Int {
        lock.withLock { $0.transcribeCount }
    }

    func loadModel() async throws {
        lock.withLock { $0.loaded = true }
    }

    func unloadModel() async {
        lock.withLock { $0.loaded = false }
    }

    func isModelLoaded() async -> Bool {
        lock.withLock { $0.loaded }
    }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        guard lock.withLock({ $0.loaded }) else { throw TranscriptEngineError.modelNotLoaded }
        lock.withLock { $0.transcribeCount += 1 }
        let text = "shard-\(Int(shard.startTime))"
        let word = TranscriptWord(
            text: text,
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration,
            confidence: 0.9
        )
        return [
            TranscriptSegment(
                id: shard.id,
                words: [word],
                text: text,
                startTime: shard.startTime,
                endTime: shard.startTime + shard.duration,
                avgConfidence: 0.9,
                passType: .final_,
                speakerId: speakerId
            )
        ]
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        [VADResult(
            isSpeech: true,
            speechProbability: 1.0,
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration
        )]
    }
}

/// Mutable snapshot box used by the mid-window flip tests. Multiple
/// concurrent reads from the runner can race a write from the test, so
/// the storage is locked.
private final class SnapshotBox: @unchecked Sendable {
    private let lock: OSAllocatedUnfairLock<CapabilitySnapshot>
    init(_ initial: CapabilitySnapshot) {
        self.lock = OSAllocatedUnfairLock(initialState: initial)
    }
    func read() -> CapabilitySnapshot { lock.withLock { $0 } }
    func write(_ next: CapabilitySnapshot) { lock.withLock { $0 = next } }
}

/// Recognizer that flips a `SnapshotBox` at a configured shard call
/// count, simulating a thermal/charge change that lands during the
/// per-shard inner loop. The flip happens AFTER the shard returns so
/// the gate is observed on the NEXT shard's check (which is the real-
/// world ordering: the OS posts a thermal-state change between
/// transcribes, not while a single shard is mid-flight).
private final class FlippingRecognizer: SpeechRecognizer, @unchecked Sendable {
    private let lock = OSAllocatedUnfairLock(initialState: State())
    private struct State {
        var loaded = false
        var transcribeCount = 0
    }
    private let box: SnapshotBox
    private let flipAfterShard: Int
    private let flipTo: CapabilitySnapshot

    init(box: SnapshotBox, flipAfterShard: Int, flipTo: CapabilitySnapshot) {
        self.box = box
        self.flipAfterShard = flipAfterShard
        self.flipTo = flipTo
    }

    var transcribeCount: Int {
        lock.withLock { $0.transcribeCount }
    }

    func loadModel() async throws { lock.withLock { $0.loaded = true } }
    func unloadModel() async { lock.withLock { $0.loaded = false } }
    func isModelLoaded() async -> Bool { lock.withLock { $0.loaded } }

    func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
        guard lock.withLock({ $0.loaded }) else { throw TranscriptEngineError.modelNotLoaded }
        let count = lock.withLock { state -> Int in
            state.transcribeCount += 1
            return state.transcribeCount
        }
        let text = "shard-\(Int(shard.startTime))"
        let word = TranscriptWord(
            text: text,
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration,
            confidence: 0.9
        )
        let segments = [
            TranscriptSegment(
                id: shard.id,
                words: [word],
                text: text,
                startTime: shard.startTime,
                endTime: shard.startTime + shard.duration,
                avgConfidence: 0.9,
                passType: .final_
            )
        ]
        if count == flipAfterShard {
            box.write(flipTo)
        }
        return segments
    }

    func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] {
        [VADResult(
            isSpeech: true,
            speechProbability: 1.0,
            startTime: shard.startTime,
            endTime: shard.startTime + shard.duration
        )]
    }
}

/// playhead-hc7e — records post-final-pass revalidation-hook invocations so
/// a test can assert the final-ASR loop is closed (fired once when new final
/// chunks land) and NOT re-fired on an idempotent no-op re-run.
private final class RevalidationHookSpy: @unchecked Sendable {
    private let lock = OSAllocatedUnfairLock(initialState: [FinalPassRevalidationRequest]())
    var requests: [FinalPassRevalidationRequest] { lock.withLock { $0 } }
    var callCount: Int { lock.withLock { $0.count } }
    func record(_ request: FinalPassRevalidationRequest) {
        lock.withLock { $0.append(request) }
    }
}

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

    /// Variant that takes a SnapshotBox + recognizer so tests can flip
    /// the gating state mid-window via the recognizer side-effect.
    private func makeRunnerWithBox(
        store: AnalysisStore,
        box: SnapshotBox,
        recognizer: any SpeechRecognizer,
        batteryLevel: Float = 0.85,
        confidenceFloor: Double = 0.5,
        audioProvider: AnalysisAudioProviding
    ) -> FinalPassRetranscriptionRunner {
        let speechService = SpeechService(recognizer: recognizer)
        return FinalPassRetranscriptionRunner(
            store: store,
            speechService: speechService,
            audioProvider: audioProvider,
            capabilitySnapshotProvider: { box.read() },
            batteryLevelProvider: { batteryLevel },
            chargeStateProvider: { box.read().isCharging },
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

    // MARK: - playhead-jzj0: the resume guard is per-window, not a frontier
    //
    // What these four tests are pinning, and why the shape matters.
    //
    // `analysis_assets.finalPassCoverageEndTime` is the MAX END of any span
    // this runner has drained — a forward-progress marker. The pre-jzj0
    // filter used `endTime > watermark` as the resume guard, which reads
    // that marker as a COMPLETION claim: "everything at or before this is
    // done". Those two readings agree only while candidates arrive in
    // increasing order of `endTime`, which is false in production — fusion
    // backfill, semantic sweep and the user's own manual marks mint windows
    // hours after the launch batch, at arbitrary positions.
    //
    // The pair below is deliberately a POSITIVE and a NEGATIVE over the
    // same fixture shape, because either one alone is passable by a broken
    // runner: a runner that ignores every guard passes the positive, and a
    // runner that runs nothing at all passes the negative.

    @Test("a completed job for the window's own span short-circuits it")
    func testCompletedSpanJobSkipsWindow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 10, endTime: 30, confidence: 0.9)
        )
        // The window's OWN span already has a `complete` job. The watermark
        // is left nil on purpose: nothing but the per-window record can be
        // producing the skip.
        try await store.insertOrIgnoreFinalPassJob(
            FinalPassJob(
                jobId: "fpj-asset-fp-w1",
                analysisAssetId: "asset-fp",
                podcastId: "pod-1",
                adWindowId: "w1",
                windowStartTime: 10,
                windowEndTime: 30,
                status: .queued,
                retryCount: 0,
                deferReason: nil,
                createdAt: 1_000.0
            )
        )
        try await store.markFinalPassJobComplete(jobId: "fpj-asset-fp-w1")

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 30, samples: [])
        ]
        let recognizer = CountingShardRecognizer()
        let runner = makeRunnerWithBox(
            store: store,
            box: SnapshotBox(makeSnapshot()),
            recognizer: recognizer,
            audioProvider: audio
        )
        let result = try await runner.runFinalPassBackfill(for: makeInput())

        #expect(result.topLevelDeferReason == nil)
        #expect(result.admittedJobIds.isEmpty)
        #expect(result.reTranscribedWindowIds.isEmpty)
        // The load-bearing assertions. The three counters above are all
        // satisfied by a runner that never runs anything at all; these two
        // say the expensive work specifically did not happen, and they are
        // what would catch a future refactor that let a settled window
        // reach `retranscribeWindow` and rely on the weak `coversWindow`
        // chunk-containment test to bail after paying for the decode.
        #expect(recognizer.transcribeCount == 0)
        #expect(audio.decodeCallCount == 0)
    }

    @Test("a window minted BEHIND the coverage frontier is still re-transcribed")
    func testWindowBehindFrontierIsEligible() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        // The bead's witness in miniature: an earlier drain drove the
        // frontier to 100 (in production, to EOF via a post-roll window),
        // and a candidate for [10, 30] arrives afterwards — a fusion
        // backfill window or one of Dan's manual marks. It has never had a
        // final-pass job of its own.
        try await store.advanceFinalPassCoverage(id: "asset-fp", endTime: 100.0)
        try await store.insertAdWindow(
            makeAdWindow(id: "w-late", analysisAssetId: "asset-fp", startTime: 10, endTime: 30, confidence: 0.9)
        )

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 30, samples: [])
        ]
        let recognizer = CountingShardRecognizer()
        let runner = makeRunnerWithBox(
            store: store,
            box: SnapshotBox(makeSnapshot()),
            recognizer: recognizer,
            audioProvider: audio
        )
        let result = try await runner.runFinalPassBackfill(for: makeInput())

        #expect(result.topLevelDeferReason == nil)
        #expect(result.reTranscribedWindowIds == ["w-late"])
        #expect(recognizer.transcribeCount == 1)
        // And the work is durably recorded, so the NEXT drain skips it —
        // the fix must not trade a permanent skip for a permanent redo.
        let job = try await store.fetchFinalPassJob(byId: "fpj-asset-fp-w-late")
        #expect(job?.status == .complete)
        let second = try await runner.runFinalPassBackfill(for: makeInput())
        #expect(second.reTranscribedWindowIds.isEmpty)
        #expect(recognizer.transcribeCount == 1)
    }

    @Test("a FAILED job behind the frontier is retried, not stranded")
    func testFailedJobBehindFrontierIsRetried() async throws {
        // The second witness on the 2026-08-02 V47 pull: asset F7BB38BB
        // holds a `failed` job for span 0.000-72.180 and a `complete` one
        // for 87.9-150.0. The completed neighbour pushed the frontier to
        // 150, so the failed span — which produced NO final-pass audio —
        // could never be re-admitted. Only a `complete` job may retire a
        // span; `failed`, `deferred` and `queued` must all stay eligible.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w-failed", analysisAssetId: "asset-fp", startTime: 0, endTime: 30, confidence: 0.9)
        )
        try await store.insertOrIgnoreFinalPassJob(
            FinalPassJob(
                jobId: "fpj-asset-fp-w-failed",
                analysisAssetId: "asset-fp",
                podcastId: "pod-1",
                adWindowId: "w-failed",
                windowStartTime: 0,
                windowEndTime: 30,
                status: .queued,
                retryCount: 0,
                deferReason: nil,
                createdAt: 1_000.0
            )
        )
        try await store.markFinalPassJobFailed(jobId: "fpj-asset-fp-w-failed", reason: "retranscribeFailed")
        // A later, unrelated span completed and drove the frontier past it.
        try await store.advanceFinalPassCoverage(id: "asset-fp", endTime: 150.0)

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 30, samples: [])
        ]
        let recognizer = CountingShardRecognizer()
        let runner = makeRunnerWithBox(
            store: store,
            box: SnapshotBox(makeSnapshot()),
            recognizer: recognizer,
            audioProvider: audio
        )
        let result = try await runner.runFinalPassBackfill(for: makeInput())

        #expect(result.reTranscribedWindowIds == ["w-failed"])
        #expect(recognizer.transcribeCount == 1)
        let job = try await store.fetchFinalPassJob(byId: "fpj-asset-fp-w-failed")
        #expect(job?.status == .complete)
    }

    @Test("eligibility does not depend on the order candidates arrive in")
    func testEligibilityIsArrivalOrderIndependent() async throws {
        // The defect was an ORDER dependence, so the invariant worth pinning
        // is order-independence itself, not another instance of the fix.
        // Two runs over the same three spans, differing only in which span
        // was present at the first drain, must converge on the same set of
        // completed spans. Numerator: spans with a `complete` job at
        // convergence. Denominator: the three spans, in both runs.
        func completedSpans(lateArrival: Bool) async throws -> [String] {
            let store = try await makeTestStore()
            let assetId = lateArrival ? "asset-late" : "asset-early"
            try await store.insertAsset(makeAsset(id: assetId))
            let input = makeInput(assetId: assetId, episodeId: "ep-\(assetId)")
            let audio = StubAnalysisAudioProvider()
            audio.shardsToReturn = [
                AnalysisShard(id: 0, episodeID: "ep-\(assetId)", startTime: 0, duration: 30, samples: []),
                AnalysisShard(id: 1, episodeID: "ep-\(assetId)", startTime: 30, duration: 30, samples: []),
                AnalysisShard(id: 2, episodeID: "ep-\(assetId)", startTime: 60, duration: 30, samples: [])
            ]
            let runner = makeRunner(store: store, snapshot: makeSnapshot(), audioProvider: audio)

            // `mid` is the span that arrives out of order in the late case.
            let early = ["lo": (0.0, 20.0), "hi": (60.0, 90.0)]
            let mid = ("mid", 30.0, 50.0)
            for (id, span) in early.sorted(by: { $0.key < $1.key }) {
                try await store.insertAdWindow(
                    makeAdWindow(id: id, analysisAssetId: assetId, startTime: span.0, endTime: span.1, confidence: 0.9)
                )
            }
            if !lateArrival {
                try await store.insertAdWindow(
                    makeAdWindow(id: mid.0, analysisAssetId: assetId, startTime: mid.1, endTime: mid.2, confidence: 0.9)
                )
            }
            _ = try await runner.runFinalPassBackfill(for: input)
            if lateArrival {
                // Minted AFTER the frontier has been driven to 90 by `hi`.
                try await store.insertAdWindow(
                    makeAdWindow(id: mid.0, analysisAssetId: assetId, startTime: mid.1, endTime: mid.2, confidence: 0.9)
                )
                _ = try await runner.runFinalPassBackfill(for: input)
            }
            return try await store.canonicalCompleteFinalPassSpans(forAsset: assetId)
                .map(\.canonicalSpanKey)
                .sorted()
        }

        let early = try await completedSpans(lateArrival: false)
        let late = try await completedSpans(lateArrival: true)
        #expect(early == ["0.000-20.000", "30.000-50.000", "60.000-90.000"])
        #expect(late == early)
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

    @Test("final-pass retranscription preserves recognizer speakerId in chunks")
    func testFinalPassPersistsSpeakerId() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 0, endTime: 30, confidence: 0.9)
        )

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 30, samples: [])
        ]
        let recognizer = CountingShardRecognizer(speakerId: 42)
        let box = SnapshotBox(makeSnapshot())
        let runner = makeRunnerWithBox(
            store: store,
            box: box,
            recognizer: recognizer,
            audioProvider: audio
        )

        let result = try await runner.runFinalPassBackfill(for: makeInput())

        #expect(result.reTranscribedWindowIds == ["w1"])
        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-fp")
        #expect(chunks.count == 1)
        #expect(chunks[0].pass == TranscriptPassType.final_.rawValue)
        #expect(chunks[0].speakerId == 42)
        #expect(chunks[0].avgConfidence == 0.9)
    }

    @Test("final-pass duplicate segment fills missing speakerId and avgConfidence")
    func testFinalPassDuplicateFillsMissingSpeakerIdAndAvgConfidence() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 0, endTime: 30, confidence: 0.9)
        )
        try await store.insertTranscriptChunk(TranscriptChunk(
            id: "existing-final",
            analysisAssetId: "asset-fp",
            segmentFingerprint: FinalPassRetranscriptionRunner.computeFinalPassFingerprint(
                text: "shard-0",
                startTime: 0,
                endTime: 10
            ),
            chunkIndex: 0,
            startTime: 0,
            endTime: 10,
            text: "shard-0",
            normalizedText: "shard-0",
            pass: TranscriptPassType.final_.rawValue,
            modelVersion: "test-final-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            speakerId: nil
        ))

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 10, samples: [])
        ]
        let recognizer = CountingShardRecognizer(speakerId: 42)
        let runner = makeRunnerWithBox(
            store: store,
            box: SnapshotBox(makeSnapshot()),
            recognizer: recognizer,
            audioProvider: audio
        )

        let result = try await runner.runFinalPassBackfill(for: makeInput())

        #expect(result.reTranscribedWindowIds == ["w1"])
        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-fp")
        #expect(chunks.count == 1)
        #expect(chunks[0].id == "existing-final")
        #expect(chunks[0].speakerId == 42)
        #expect(chunks[0].avgConfidence == 0.9)
    }

    /// playhead-jc42 — THE SHIPPED DEFECT, at the write path that caused it.
    ///
    /// `TranscriptEngineService` writes `pass='final'` rows too (whenever
    /// `activeModelRole == .asrFinal`), fingerprinting `"\(text)|\(start)|\(end)"`
    /// WITHOUT the `fp-final-` prefix. So the runner's pre-insert read — which
    /// asked for its OWN prefixed fingerprint — could only ever find rows the
    /// runner itself had written, and appended a second `pass='final'` row for
    /// every span the engine had already finalised. 3,496 of those on the
    /// 2026-08-15 pull, all byte-identical in text and timing, all retained by
    /// `TranscriptChunkCanonicalizer` (it de-overlaps fast against final, never
    /// final against final) and therefore all drawn twice in the overlay.
    ///
    /// The engine row here is spelled the way the engine spells it, and the
    /// fixture deliberately does NOT use `computeFinalPassFingerprint` — that is
    /// the whole point. Before this fix the asset ends with two rows.
    @Test("an ENGINE-written final row is recognised — the runner does not append a second copy of the same span")
    func testFinalPassDoesNotDuplicateEngineWrittenFinalRow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 0, endTime: 30, confidence: 0.9)
        )
        // `TranscriptEngineService.computeFingerprint`: SHA-256 of the bare
        // `"\(text)|\(start)|\(end)"`, no prefix.
        let engineFingerprint = SHA256.hash(data: Data("shard-0|0.0|10.0".utf8))
            .prefix(16)
            .map { String(format: "%02x", $0) }
            .joined()
        #expect(engineFingerprint != FinalPassRetranscriptionRunner.computeFinalPassFingerprint(
            text: "shard-0", startTime: 0, endTime: 10
        ), "the two producers cannot collide — that is the defect, not a coincidence")

        try await store.insertTranscriptChunk(TranscriptChunk(
            id: "engine-final",
            analysisAssetId: "asset-fp",
            segmentFingerprint: engineFingerprint,
            chunkIndex: 0,
            startTime: 0,
            endTime: 10,
            text: "shard-0",
            normalizedText: "shard-0",
            pass: TranscriptPassType.final_.rawValue,
            modelVersion: "apple-speech-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            speakerId: nil
        ))

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 10, samples: [])
        ]
        let runner = makeRunnerWithBox(
            store: store,
            box: SnapshotBox(makeSnapshot()),
            recognizer: CountingShardRecognizer(speakerId: 42),
            audioProvider: audio
        )

        let result = try await runner.runFinalPassBackfill(for: makeInput())

        #expect(result.reTranscribedWindowIds == ["w1"])
        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-fp")
        #expect(chunks.count == 1, "the engine's row IS this span — appending beside it is the duplication")
        #expect(chunks[0].id == "engine-final")
        // The metadata upgrade still runs, and it is keyed on the row we FOUND
        // rather than on `chunk.segmentFingerprint`, which addresses nothing
        // here. Reading `chunk.segmentFingerprint` would update zero rows and
        // leave both fields nil.
        #expect(chunks[0].speakerId == 42)
        #expect(chunks[0].avgConfidence == 0.9)
    }

    /// playhead-jc42 — THE PLAUSIBLE WRONG FIX, pinned at the layer where it
    /// does harm (mutation rail JC02's consequence half).
    ///
    /// Dropping `pass` from the content lookup looks like a simplification and
    /// "removes duplicates" just as well, because a FAST row over the same span
    /// answers "already stored". It is a silent COVERAGE regression wearing a
    /// fix's clothes: the runner concludes it has persisted a final row it never
    /// wrote, no `pass='final'` row is ever created for that span, and
    /// `AnalysisStore.readFinalTranscriptRegions` — which filters `pass='final'`
    /// — stops growing. Nothing throws and nothing is duplicated, so only a test
    /// that asserts the final row EXISTS can see it.
    ///
    /// The store-level rail (`contentLookupIsPassScoped`) pins the QUERY. This
    /// pins what the query is for.
    @Test("a FAST row over the same span does not suppress the final row — final-pass coverage keeps growing")
    func testFinalPassStillWritesWhenOnlyAFastRowCoversTheSpan() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 0, endTime: 30, confidence: 0.9)
        )
        // The fast pass already transcribed this exact span, with the exact text
        // the recognizer is about to produce again — the 3,751-pair shape on the
        // 2026-08-15 pull, and the one V53 deliberately KEEPS.
        try await store.insertTranscriptChunk(TranscriptChunk(
            id: "fast-row",
            analysisAssetId: "asset-fp",
            segmentFingerprint: "fp-fast-row",
            chunkIndex: 0,
            startTime: 0,
            endTime: 10,
            text: "shard-0",
            normalizedText: "shard-0",
            pass: TranscriptPassType.fast.rawValue,
            modelVersion: "apple-speech-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            speakerId: nil
        ))

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 10, samples: [])
        ]
        let runner = makeRunnerWithBox(
            store: store,
            box: SnapshotBox(makeSnapshot()),
            recognizer: CountingShardRecognizer(speakerId: 42),
            audioProvider: audio
        )

        let result = try await runner.runFinalPassBackfill(for: makeInput())

        #expect(result.reTranscribedWindowIds == ["w1"])
        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-fp")
        #expect(chunks.count == 2, "the fast row is a DIFFERENT fact — it must not stand in for the final row")
        #expect(chunks.contains { $0.pass == TranscriptPassType.fast.rawValue && $0.id == "fast-row" })
        #expect(chunks.contains { $0.pass == TranscriptPassType.final_.rawValue },
                "readFinalTranscriptRegions filters pass='final'; without this row that union stops growing")
    }

    /// playhead-6av0 REWRITE, for the same reason as
    /// `TranscriptEngineTests.duplicateFingerprintFillsSpeakerIdAndAvgConfidenceAcrossMatchingRows`:
    /// `transcript_chunks` is now UNIQUE on
    /// `(analysisAssetId, pass, segmentFingerprint)`, so a SECOND final-pass row
    /// under one fingerprint cannot be created and the "reach every duplicate"
    /// premise is unreachable. What is worth pinning is that the second insert
    /// is refused and the survivor — which here already HAS a speakerId and an
    /// avgConfidence — is left exactly as it was by the idempotent re-run.
    @Test("a second final-pass row for one fingerprint is refused, and the survivor is left intact")
    func testFinalPassDuplicateFillsMissingSpeakerIdAndAvgConfidenceAcrossDuplicateRows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 0, endTime: 30, confidence: 0.9)
        )
        let fingerprint = FinalPassRetranscriptionRunner.computeFinalPassFingerprint(
            text: "shard-0",
            startTime: 0,
            endTime: 10
        )
        try await store.insertTranscriptChunk(TranscriptChunk(
            id: "existing-final-speaker",
            analysisAssetId: "asset-fp",
            segmentFingerprint: fingerprint,
            chunkIndex: 0,
            startTime: 0,
            endTime: 10,
            text: "shard-0",
            normalizedText: "shard-0",
            pass: TranscriptPassType.final_.rawValue,
            modelVersion: "test-final-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            speakerId: 42,
            avgConfidence: 0.61
        ))
        let insertedDuplicate = try await store.insertTranscriptChunk(TranscriptChunk(
            id: "existing-final-missing-speaker",
            analysisAssetId: "asset-fp",
            segmentFingerprint: fingerprint,
            chunkIndex: 1,
            startTime: 0,
            endTime: 10,
            text: "shard-0",
            normalizedText: "shard-0",
            pass: TranscriptPassType.final_.rawValue,
            modelVersion: "test-final-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            speakerId: nil,
            avgConfidence: nil
        ))
        #expect(!insertedDuplicate,
                "the UNIQUE (analysisAssetId, pass, segmentFingerprint) index refuses the second row")

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 10, samples: [])
        ]
        let recognizer = CountingShardRecognizer(speakerId: 42)
        let runner = makeRunnerWithBox(
            store: store,
            box: SnapshotBox(makeSnapshot()),
            recognizer: recognizer,
            audioProvider: audio
        )

        let result = try await runner.runFinalPassBackfill(for: makeInput())

        #expect(result.reTranscribedWindowIds == ["w1"])
        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-fp")
            .filter { $0.segmentFingerprint == fingerprint }
        #expect(chunks.count == 1,
                "one row per (asset, pass, fingerprint) — the duplicate never landed")
        #expect(chunks.allSatisfy { $0.speakerId == 42 })
        #expect(abs((chunks[0].avgConfidence ?? 0) - 0.61) < 0.001,
                "the survivor already had both fields, so the idempotent re-run leaves them alone")
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

        // Second run: w1's span now carries a `complete` final_pass_jobs
        // row, so no eligible windows remain — empty result. (Pre-jzj0 the
        // advanced watermark is what produced this; the outcome is the
        // same, the reason is now per-window.)
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

    // MARK: - playhead-5147: per-shard cooperative thermal/charge/LPM check

    /// Simulates a device that warms mid-window (nominal → fair after 1
    /// shard transcribes). Asserts:
    ///   1. The currently-running job lands in `.deferred`, NOT `.failed`.
    ///   2. Partial chunks for the shard that did complete are persisted
    ///      (so the next admission wave does not redo wasted work).
    ///   3. Sibling jobs that hadn't started are also marked `.deferred`,
    ///      not orphaned in `running`.
    ///   4. `retryCount` is NOT incremented (clean defer ≠ failure).
    @Test("thermal flip mid-window: row deferred, partial chunks landed, siblings deferred, retryCount preserved")
    func testThermalFlipMidWindowDefersCleanly() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        // Two eligible windows so we can verify the OUTER sibling-defer
        // path. Window 1 has multiple shards so the inner per-shard
        // gate has somewhere to fire mid-flight; window 2 is left
        // untouched and must end up `.deferred`.
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp",
                         startTime: 0, endTime: 90, confidence: 0.9)
        )
        try await store.insertAdWindow(
            makeAdWindow(id: "w2", analysisAssetId: "asset-fp",
                         startTime: 120, endTime: 150, confidence: 0.9)
        )

        let audio = StubAnalysisAudioProvider()
        // Three shards intersect window 1 (0..90); window 2's lone
        // shard (120..150) is also returned but the runner should
        // never reach it because the gate trips during window 1.
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 30, samples: []),
            AnalysisShard(id: 1, episodeID: "ep-asset-fp", startTime: 30, duration: 30, samples: []),
            AnalysisShard(id: 2, episodeID: "ep-asset-fp", startTime: 60, duration: 30, samples: []),
            AnalysisShard(id: 4, episodeID: "ep-asset-fp", startTime: 120, duration: 30, samples: [])
        ]

        let nominal = makeSnapshot()
        let fair = makeSnapshot(thermal: .fair)
        let box = SnapshotBox(nominal)
        // Flip after the FIRST shard. The runner's per-shard check
        // sits at the TOP of the loop, so:
        //   • Shard 0: gate=nominal, transcribes, post-shard flip→fair
        //   • Shard 1: gate=fair, throws FinalPassDeferredMidWindow
        //              after persisting shard 0's chunk
        let recognizer = FlippingRecognizer(
            box: box,
            flipAfterShard: 1,
            flipTo: fair
        )
        let runner = makeRunnerWithBox(
            store: store,
            box: box,
            recognizer: recognizer,
            audioProvider: audio
        )

        let result = try await runner.runFinalPassBackfill(for: makeInput())

        // No top-level defer (we got past the pre-loop gate).
        #expect(result.topLevelDeferReason == nil)
        // The mid-window job AND the un-started sibling are deferred.
        #expect(result.deferredJobIds.contains("fpj-asset-fp-w1"))
        #expect(result.deferredJobIds.contains("fpj-asset-fp-w2"))
        // No window completed.
        #expect(result.admittedJobIds.isEmpty)
        #expect(result.reTranscribedWindowIds.isEmpty)

        // Job rows: both deferred, neither failed.
        let job1 = try #require(await store.fetchFinalPassJob(byId: "fpj-asset-fp-w1"))
        let job2 = try #require(await store.fetchFinalPassJob(byId: "fpj-asset-fp-w2"))
        #expect(job1.status == .deferred)
        #expect(job2.status == .deferred)
        #expect(job1.deferReason == "thermalThrottled")
        #expect(job2.deferReason == "thermalThrottled")
        // retryCount must NOT have been bumped on either row.
        #expect(job1.retryCount == 0)
        #expect(job2.retryCount == 0)

        // Partial chunks: exactly one shard transcribed before the
        // flip, so exactly one final-pass chunk landed.
        let chunks = try await store.fetchTranscriptChunks(assetId: "asset-fp")
        let finalChunks = chunks.filter { $0.pass == TranscriptPassType.final_.rawValue }
        #expect(finalChunks.count == 1)
        #expect(finalChunks.first?.text == "shard-0")
        // Only one shard produced segments (recognizer count agrees).
        #expect(recognizer.transcribeCount == 1)

        // Watermark must NOT have advanced — the window did not fully
        // re-transcribe.
        let asset = try await store.fetchAsset(id: "asset-fp")
        #expect(asset?.finalPassCoverageEndTime == nil)
    }

    /// Resume after a mid-window defer: the second backfill run must
    /// re-enter the same window, dedupe the chunk persisted by the
    /// first run via `segmentFingerprint`, and finish the remaining
    /// shards. Pins the dedupe contract that the partial-persist path
    /// depends on.
    @Test("mid-window defer then resume: dedupe holds, no duplicate chunks, retry completes")
    func testResumeAfterMidWindowDeferDeduplicatesPriorChunks() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp",
                         startTime: 0, endTime: 90, confidence: 0.9)
        )

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 30, samples: []),
            AnalysisShard(id: 1, episodeID: "ep-asset-fp", startTime: 30, duration: 30, samples: []),
            AnalysisShard(id: 2, episodeID: "ep-asset-fp", startTime: 60, duration: 30, samples: [])
        ]

        let nominal = makeSnapshot()
        let fair = makeSnapshot(thermal: .fair)
        let box = SnapshotBox(nominal)
        let recognizer = FlippingRecognizer(
            box: box,
            flipAfterShard: 1,
            flipTo: fair
        )
        let runner = makeRunnerWithBox(
            store: store,
            box: box,
            recognizer: recognizer,
            audioProvider: audio
        )

        // First run: defers after shard 0 persists.
        let first = try await runner.runFinalPassBackfill(for: makeInput())
        #expect(first.deferredJobIds.contains("fpj-asset-fp-w1"))
        let firstChunks = try await store.fetchTranscriptChunks(assetId: "asset-fp")
            .filter { $0.pass == TranscriptPassType.final_.rawValue }
        #expect(firstChunks.count == 1)

        // Second run: device is back to nominal, fresh recognizer that
        // never flips. The runner re-enters window 1 (still eligible:
        // watermark didn't advance, confidence still ≥ floor) and
        // re-transcribes ALL three shards. The shard-0 segment is
        // produced again with identical text/timing so its fingerprint
        // matches the previously-persisted row — `hasTranscriptChunk`
        // returns true and dedupe drops it.
        let calmBox = SnapshotBox(nominal)
        let calmRecognizer = CountingShardRecognizer()
        let calmRunner = makeRunnerWithBox(
            store: store,
            box: calmBox,
            recognizer: calmRecognizer,
            audioProvider: audio
        )
        let second = try await calmRunner.runFinalPassBackfill(for: makeInput())
        #expect(second.topLevelDeferReason == nil)
        #expect(second.admittedJobIds == ["fpj-asset-fp-w1"])
        #expect(second.reTranscribedWindowIds == ["w1"])

        // The recognizer was called for each shard in the resume run.
        #expect(calmRecognizer.transcribeCount == 3)

        // Final chunk count: shard-0 (carried over), shard-30,
        // shard-60. Shard-0 was deduped on the second run — exactly
        // ONE row per shard, no duplicates.
        let allFinal = try await store.fetchTranscriptChunks(assetId: "asset-fp")
            .filter { $0.pass == TranscriptPassType.final_.rawValue }
        #expect(allFinal.count == 3)
        let texts = Set(allFinal.map { $0.text })
        #expect(texts == ["shard-0", "shard-30", "shard-60"])

        // Job is now `.complete` and watermark is at the window's end.
        let job = try #require(await store.fetchFinalPassJob(byId: "fpj-asset-fp-w1"))
        #expect(job.status == .complete)
        let asset = try await store.fetchAsset(id: "asset-fp")
        #expect(asset?.finalPassCoverageEndTime == 90.0)
    }

    /// Cancellation INSIDE the inner per-shard loop must still take
    /// precedence over mid-window defer. When the surrounding Task is
    /// cancelled while a window is mid-flight, the runner throws
    /// `CancellationError` and marks the row deferred with reason
    /// `"cancelled"`, NOT `"thermalThrottled"`. This is the existing
    /// contract from cycle-1 / cycle-4 M2 — the new per-shard thermal
    /// check must not regress it.
    ///
    /// Pre-condition: cancellation must be observed AFTER the runner
    /// has called `markFinalPassJobRunning`, otherwise the row is
    /// still `.queued` when the outer `Task.checkCancellation()` at
    /// the top of the for-loop trips. We arrange this by injecting a
    /// recognizer that cancels the surrounding task as a side effect
    /// of its first transcribe call.
    @Test("cancellation mid-window: row marked deferred(cancelled), not failed")
    func testCancellationMidWindowMarksDeferredCancelled() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp",
                         startTime: 0, endTime: 60, confidence: 0.9)
        )

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 30, samples: []),
            AnalysisShard(id: 1, episodeID: "ep-asset-fp", startTime: 30, duration: 30, samples: [])
        ]

        // Recognizer whose first shard transcribes successfully; a
        // sentinel box flips after that, and the test cancels the
        // surrounding task before invoking `runFinalPassBackfill`'s
        // resume after the first shard. We use a TaskHandleBox to
        // pass the in-flight Task into the recognizer at construction
        // time: classic chicken/egg, but the box is mutable and is
        // populated immediately after `Task { ... }` is created.
        final class TaskBox: @unchecked Sendable {
            private let lock = OSAllocatedUnfairLock<Task<Void, Error>?>(initialState: nil)
            func set(_ t: Task<Void, Error>?) { lock.withLock { $0 = t } }
            func cancelHeld() { lock.withLock { $0?.cancel() } }
            var hasTask: Bool { lock.withLock { $0 != nil } }
        }
        final class CancellingRecognizer: SpeechRecognizer, @unchecked Sendable {
            private let lock = OSAllocatedUnfairLock(initialState: false)
            let taskBox: TaskBox
            init(taskBox: TaskBox) { self.taskBox = taskBox }
            func loadModel() async throws { lock.withLock { $0 = true } }
            func unloadModel() async { lock.withLock { $0 = false } }
            func isModelLoaded() async -> Bool { lock.withLock { $0 } }
            func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
                guard lock.withLock({ $0 }) else { throw TranscriptEngineError.modelNotLoaded }
                // Cycle-3 M-2: defensively wait until the test has
                // populated `taskBox` (the `Task { ... }` body could
                // theoretically begin executing before the test
                // continues to `taskBox.set(task)`, which would make
                // `taskBox.cancelHeld()` a no-op against `nil`). A few
                // yields gives the spawning continuation a chance to
                // resume. Bounded by 100 iterations so a genuine bug
                // (test forgot to set the box) still fails loudly.
                for _ in 0..<100 where !taskBox.hasTask {
                    await Task.yield()
                }
                // Cancel the surrounding task BEFORE returning so the
                // runner observes cancellation on the NEXT shard's
                // `try Task.checkCancellation()` at the top of the
                // inner loop.
                taskBox.cancelHeld()
                let text = "shard-\(Int(shard.startTime))"
                let word = TranscriptWord(text: text,
                                          startTime: shard.startTime,
                                          endTime: shard.startTime + shard.duration,
                                          confidence: 0.9)
                return [TranscriptSegment(
                    id: shard.id, words: [word], text: text,
                    startTime: shard.startTime,
                    endTime: shard.startTime + shard.duration,
                    avgConfidence: 0.9, passType: .final_
                )]
            }
            func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] { [] }
        }

        let nominal = makeSnapshot()
        let box = SnapshotBox(nominal)
        let taskBox = TaskBox()
        let runner = makeRunnerWithBox(
            store: store,
            box: box,
            recognizer: CancellingRecognizer(taskBox: taskBox),
            audioProvider: audio
        )
        let task = Task {
            _ = try await runner.runFinalPassBackfill(for: makeInput())
        }
        taskBox.set(task)
        do {
            _ = try await task.value
            Issue.record("Expected CancellationError")
        } catch is CancellationError {
            // expected
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        let job = try #require(await store.fetchFinalPassJob(byId: "fpj-asset-fp-w1"))
        #expect(job.status == .deferred)
        #expect(job.deferReason == "cancelled")
        // Cancellation is also a clean defer — retryCount stays at 0.
        #expect(job.retryCount == 0)
    }

    /// Differentiation: a generic transcribe failure (e.g.
    /// `transcriptionFailed`) MUST still hit the `markFinalPassJobFailed`
    /// path and bump retryCount, even though we now have a sibling
    /// `FinalPassDeferredMidWindow` path. Pins that the new typed
    /// sentinel did not accidentally swallow the failure branch.
    @Test("generic transcribe failure mid-window still marks .failed and bumps retryCount")
    func testGenericFailureMidWindowMarksFailedNotDeferred() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp",
                         startTime: 0, endTime: 30, confidence: 0.9)
        )

        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 30, samples: [])
        ]

        // Recognizer that throws on transcribe so the runner's
        // generic catch block is exercised.
        final class ThrowingRecognizer: SpeechRecognizer, @unchecked Sendable {
            private var loaded = false
            func loadModel() async throws { loaded = true }
            func unloadModel() async { loaded = false }
            func isModelLoaded() async -> Bool { loaded }
            func transcribe(shard: AnalysisShard, podcastId: String?) async throws -> [TranscriptSegment] {
                throw TranscriptEngineError.transcriptionFailed("boom")
            }
            func detectVoiceActivity(shard: AnalysisShard) async throws -> [VADResult] { [] }
        }
        let nominal = makeSnapshot()
        let box = SnapshotBox(nominal)
        let runner = makeRunnerWithBox(
            store: store,
            box: box,
            recognizer: ThrowingRecognizer(),
            audioProvider: audio
        )

        _ = try await runner.runFinalPassBackfill(for: makeInput())

        let job = try #require(await store.fetchFinalPassJob(byId: "fpj-asset-fp-w1"))
        #expect(job.status == .failed)
        #expect(job.deferReason == "retranscribeFailed")
        // `markFinalPassJobFailed` bumps retryCount by 1 on the queued
        // → failed transition.
        #expect(job.retryCount == 1)
    }

    // MARK: - cycle-1 L5: heartbeat jitter

    @Test("computeHeartbeatInterval returns base at roll=0")
    func heartbeatJitterCenter() {
        let interval = FinalPassRetranscriptionRunner.computeHeartbeatInterval(
            base: 300,
            jitter: 30,
            roll: 0
        )
        #expect(interval == .seconds(300))
    }

    @Test("computeHeartbeatInterval reaches exact bounds at extreme rolls")
    func heartbeatJitterBounds() {
        let lower = FinalPassRetranscriptionRunner.computeHeartbeatInterval(
            base: 300,
            jitter: 30,
            roll: -1
        )
        let upper = FinalPassRetranscriptionRunner.computeHeartbeatInterval(
            base: 300,
            jitter: 30,
            roll: +1
        )
        #expect(lower == .seconds(270))
        #expect(upper == .seconds(330))
    }

    @Test("computeHeartbeatInterval clamps out-of-range rolls")
    func heartbeatJitterClampsOutOfRange() {
        // A buggy caller supplying roll=2.0 must not produce a 360s
        // interval that crosses the reaper-floor safety margin.
        let interval = FinalPassRetranscriptionRunner.computeHeartbeatInterval(
            base: 300,
            jitter: 30,
            roll: 2.0
        )
        #expect(interval == .seconds(330))

        // Symmetric clamp on the negative side prevents a negative
        // interval that would heartbeat every shard.
        let lower = FinalPassRetranscriptionRunner.computeHeartbeatInterval(
            base: 300,
            jitter: 30,
            roll: -2.0
        )
        #expect(lower == .seconds(270))
    }

    @Test("jitteredHeartbeatInterval stays within ±jitter of base")
    func heartbeatJitterBoundedByConstants() {
        // The runtime variant uses Double.random; sample 50 rolls and
        // assert every one stays in [base-jitter, base+jitter]. Pure
        // sanity check that the random-draw range matches the
        // deterministic helper's contract.
        for _ in 0..<50 {
            let interval = FinalPassRetranscriptionRunner.jitteredHeartbeatInterval()
            let lower = Duration.seconds(
                FinalPassRetranscriptionRunner.heartbeatBaseSeconds
                    - FinalPassRetranscriptionRunner.heartbeatJitterSeconds
            )
            let upper = Duration.seconds(
                FinalPassRetranscriptionRunner.heartbeatBaseSeconds
                    + FinalPassRetranscriptionRunner.heartbeatJitterSeconds
            )
            #expect(interval >= lower)
            #expect(interval <= upper)
        }
    }

    // MARK: - playhead-hc7e: final-ASR loop close (revalidation trigger)

    @Test("completing final-pass transcription schedules a revalidation")
    func testFinalPassSchedulesRevalidation() async throws {
        let store = try await makeTestStore()
        // Asset carries a durable episode duration so the request payload can
        // be asserted end-to-end.
        try await store.insertAsset(AnalysisAsset(
            id: "asset-fp",
            episodeId: "ep-asset-fp",
            assetFingerprint: "fp-asset-fp",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/asset-fp.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 150
        ))
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 0, endTime: 30, confidence: 0.9)
        )
        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = [
            AnalysisShard(id: 0, episodeID: "ep-asset-fp", startTime: 0, duration: 30, samples: [])
        ]
        let spy = RevalidationHookSpy()
        let snapshot = makeSnapshot()
        let speechService = SpeechService(recognizer: CountingShardRecognizer())
        let runner = FinalPassRetranscriptionRunner(
            store: store,
            speechService: speechService,
            audioProvider: audio,
            capabilitySnapshotProvider: { snapshot },
            batteryLevelProvider: { 0.85 },
            chargeStateProvider: { true },
            confidenceFloor: 0.5,
            modelVersion: "test-final-v1",
            onFinalPassRetranscribed: { request in spy.record(request) }
        )

        let result = try await runner.runFinalPassBackfill(for: makeInput())
        #expect(result.reTranscribedWindowIds == ["w1"])

        // Loop closed exactly once, carrying the affected asset + duration.
        #expect(spy.callCount == 1)
        #expect(spy.requests.first?.analysisAssetId == "asset-fp")
        #expect(spy.requests.first?.reTranscribedWindowIds == ["w1"])
        #expect(spy.requests.first?.episodeDuration == 150)

        // Idempotent re-run (watermark already covers the window, nothing new
        // re-transcribed) must NOT re-schedule detection.
        let second = try await runner.runFinalPassBackfill(for: makeInput())
        #expect(second.reTranscribedWindowIds.isEmpty)
        #expect(spy.callCount == 1)
    }

    @Test("a top-level-deferred final-pass run does not schedule revalidation")
    func testDeferredRunDoesNotScheduleRevalidation() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        try await store.insertAdWindow(
            makeAdWindow(id: "w1", analysisAssetId: "asset-fp", startTime: 0, endTime: 30, confidence: 0.9)
        )
        let spy = RevalidationHookSpy()
        let snapshot = makeSnapshot()
        let speechService = SpeechService(recognizer: CountingShardRecognizer())
        let runner = FinalPassRetranscriptionRunner(
            store: store,
            speechService: speechService,
            audioProvider: StubAnalysisAudioProvider(),
            capabilitySnapshotProvider: { snapshot },
            batteryLevelProvider: { 0.85 },
            chargeStateProvider: { false },   // not on charge → top-level defer
            confidenceFloor: 0.5,
            modelVersion: "test-final-v1",
            onFinalPassRetranscribed: { request in spy.record(request) }
        )
        let result = try await runner.runFinalPassBackfill(for: makeInput())
        #expect(result.topLevelDeferReason == .batteryTooLow)
        #expect(result.reTranscribedWindowIds.isEmpty)
        #expect(spy.callCount == 0)
    }
}
