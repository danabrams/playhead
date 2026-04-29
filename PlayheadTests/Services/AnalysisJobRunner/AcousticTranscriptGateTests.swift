// AcousticTranscriptGateTests.swift
// playhead-gtt9.1: shadow-mode coverage for the acoustic-likelihood
// transcript gate evaluator wired into `AnalysisJobRunner`.
//
// These tests exercise `AnalysisJobRunner.run(_:)` end-to-end with a
// recording shadow-gate logger injected so we can assert on the
// per-shard rows the runner emits. Because feature extraction runs
// before the gate, every test seeds `feature_windows` with controlled
// values and parks the asset's `featureCoverageEndTime` past the shard
// span — that way the live `FeatureExtractionService` short-circuits
// (`shardEnd <= effectiveCoverage` skip), preserving our seeded windows
// without us having to mock the service. Repair-coverage is also a
// no-op because the seeded rows already carry the current feature
// version.
//
// The deterministic safety-sample RNG is the lever that pins
// would-skip vs. safety-sample-keep without flake. With `coin = 0.0`
// every below-threshold shard hits the keep arm; with `coin = 0.99`
// every below-threshold shard hits the would-skip arm. The 10%-default
// production fraction is exercised by spot-check rather than coverage
// math.

import CryptoKit
import Foundation
import Testing
@testable import Playhead

// MARK: - Recording logger

/// Captures every `record(_:)` call so tests can assert the runner
/// emitted the expected per-shard rows. Actor-isolated so concurrent
/// shadow-gate writes from the runner serialize cleanly.
private actor RecordingTranscriptShadowGateLogger: TranscriptShadowGateLogging {
    private var entries: [TranscriptShadowGateEntry] = []

    func record(_ entry: TranscriptShadowGateEntry) async {
        entries.append(entry)
    }

    func snapshot() -> [TranscriptShadowGateEntry] {
        entries
    }
}

// MARK: - Helpers

private func makeGateRequest(
    desiredCoverageSec: Double,
    assetId: String = "test-asset"
) -> AnalysisRangeRequest {
    let tmpDir = try! makeTempDir(prefix: "AcousticTranscriptGateTests")
    let audioFile = tmpDir.appendingPathComponent("episode.m4a")
    FileManager.default.createFile(atPath: audioFile.path, contents: Data())
    let localURL = LocalAudioURL(audioFile)!
    return AnalysisRangeRequest(
        jobId: UUID().uuidString,
        episodeId: "test-ep",
        podcastId: "test-pod",
        analysisAssetId: assetId,
        audioURL: localURL,
        desiredCoverageSec: desiredCoverageSec,
        mode: .preRollWarmup,
        outputPolicy: .writeWindowsAndCues,
        priority: .medium
    )
}

private func seedAssetWithFeatureCoverage(
    store: AnalysisStore,
    assetId: String = "test-asset",
    featureCoverageEndTime: Double,
    fastTranscriptCoverageEndTime: Double? = nil
) async throws {
    let asset = AnalysisAsset(
        id: assetId,
        episodeId: "test-ep",
        assetFingerprint: assetId,
        weakFingerprint: nil,
        sourceURL: "",
        featureCoverageEndTime: featureCoverageEndTime,
        fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime,
        confirmedAdCoverageEndTime: nil,
        analysisState: SessionState.queued.rawValue,
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
    try await store.insertAsset(asset)
}

/// Synthesize a "clean host conversation" feature window — likelihood
/// scores at ~0 against the default scorer priors. Used to drive the
/// gate's wouldSkip arm.
private func cleanSpeechWindow(
    assetId: String,
    startTime: Double,
    endTime: Double
) -> FeatureWindow {
    FeatureWindow(
        analysisAssetId: assetId,
        startTime: startTime,
        endTime: endTime,
        rms: 0.08,
        spectralFlux: 0.05,
        musicProbability: 0.05,
        speakerChangeProxyScore: 0.0,
        musicBedChangeScore: 0.0,
        musicBedOnsetScore: 0.0,
        musicBedOffsetScore: 0.0,
        musicBedLevel: .none,
        pauseProbability: 0.1,
        speakerClusterId: 1,
        jingleHash: nil,
        featureVersion: 4
    )
}

/// Synthesize a "clear ad onset" feature window — likelihood scores
/// well above the default 0.30 threshold. Used to drive the gate's
/// aboveThreshold arm.
private func adOnsetWindow(
    assetId: String,
    startTime: Double,
    endTime: Double
) -> FeatureWindow {
    FeatureWindow(
        analysisAssetId: assetId,
        startTime: startTime,
        endTime: endTime,
        rms: 0.18,
        spectralFlux: 0.40,
        musicProbability: 0.85,
        speakerChangeProxyScore: 0.7,
        musicBedChangeScore: 0.8,
        musicBedOnsetScore: 0.9,
        musicBedOffsetScore: 0.0,
        musicBedLevel: .foreground,
        pauseProbability: 0.2,
        speakerClusterId: 2,
        jingleHash: "ad-jingle-x",
        featureVersion: 4
    )
}

/// Seeds a contiguous run of feature windows of the given shape across
/// `[start, end)` with `windowDuration` second windows. Bumps the
/// asset's `featureCoverageEndTime` to `end` so the live extractor
/// skips re-extraction during the run.
private func seedFeatureWindows(
    store: AnalysisStore,
    assetId: String,
    start: Double,
    end: Double,
    windowDuration: Double = 2.0,
    shape: (Double, Double) -> FeatureWindow
) async throws {
    var windows: [FeatureWindow] = []
    var cursor = start
    while cursor < end {
        let next = min(cursor + windowDuration, end)
        windows.append(shape(cursor, next))
        cursor = next
    }
    try await store.persistFeatureExtractionBatch(
        assetId: assetId,
        windows: windows,
        priorWindowUpdate: nil,
        checkpoint: nil,
        coverageEndTime: end
    )
}

private func makeRunner(
    store: AnalysisStore,
    audioStub: StubAnalysisAudioProvider,
    gateConfig: AcousticTranscriptGateConfig,
    shadowLogger: RecordingTranscriptShadowGateLogger,
    safetySampleCoin: Double = 0.99
) async throws -> AnalysisJobRunner {
    let featureService = FeatureExtractionService(store: store)
    let speechService = SpeechService(recognizer: StubSpeechRecognizer())
    try await speechService.loadFastModel()
    let transcriptEngine = TranscriptEngineService(
        speechService: speechService,
        store: store
    )
    let adStub = StubAdDetectionProvider()
    let materializer = SkipCueMaterializer(store: store)

    let coin = safetySampleCoin
    return AnalysisJobRunner(
        store: store,
        audioProvider: audioStub,
        featureService: featureService,
        transcriptEngine: transcriptEngine,
        adDetection: adStub,
        cueMaterializer: materializer,
        acousticGateConfig: gateConfig,
        transcriptShadowGateLogger: shadowLogger,
        safetySampleRNG: { coin }
    )
}

private func makeShards4() -> [AnalysisShard] {
    // 4 shards × 30s = 0–120s
    (0..<4).map { i in
        makeShard(
            id: i,
            episodeID: "test-ep",
            startTime: Double(i) * 30,
            duration: 30
        )
    }
}

// MARK: - Tests

@Suite("AcousticTranscriptGate (playhead-gtt9.1)")
struct AcousticTranscriptGateTests {

    @Test("Disabled config produces no shadow logs and transcribes every shard")
    func testDisabledConfigEmitsNoShadowRows() async throws {
        let store = try await makeTestStore()
        // Asset starts with both feature + transcript coverage already
        // populated so feature extraction is a no-op and the test
        // never depends on real DSP.
        try await seedAssetWithFeatureCoverage(
            store: store,
            featureCoverageEndTime: 120,
            fastTranscriptCoverageEndTime: 120
        )
        try await seedFeatureWindows(
            store: store,
            assetId: "test-asset",
            start: 0,
            end: 120
        ) { s, e in cleanSpeechWindow(assetId: "test-asset", startTime: s, endTime: e) }

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards4()
        let logger = RecordingTranscriptShadowGateLogger()
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            gateConfig: .disabled,
            shadowLogger: logger
        )

        _ = await runner.run(makeGateRequest(desiredCoverageSec: 120))

        let entries = await logger.snapshot()
        #expect(entries.isEmpty)
    }

    @Test("Shadow mode logs but does not skip — every shard reaches the engine")
    func testShadowModeLogsButDoesNotSkip() async throws {
        let store = try await makeTestStore()
        try await seedAssetWithFeatureCoverage(
            store: store,
            featureCoverageEndTime: 120,
            // Leave fastTranscriptCoverageEndTime nil so shards do not
            // hit the M1 quality precondition; we want them to score
            // and emit `wouldSkip` rows.
            fastTranscriptCoverageEndTime: nil
        )
        // Seed clean speech windows — every shard scores well below
        // the default 0.30 threshold.
        try await seedFeatureWindows(
            store: store,
            assetId: "test-asset",
            start: 0,
            end: 120
        ) { s, e in cleanSpeechWindow(assetId: "test-asset", startTime: s, endTime: e) }

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards4()
        let logger = RecordingTranscriptShadowGateLogger()
        // Default config: enabled=true, skipEnabled=false. Coin = 0.99
        // so the safety sample never fires and every below-threshold
        // shard tags `wouldSkip` (yet still transcribed in shadow mode).
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            gateConfig: .default,
            shadowLogger: logger,
            safetySampleCoin: 0.99
        )

        _ = await runner.run(makeGateRequest(desiredCoverageSec: 120))

        let entries = await logger.snapshot()
        #expect(entries.count == 4)
        // Every entry tags wouldGate and (because shadow mode) was
        // still transcribed — that's the bead's central invariant.
        for entry in entries {
            #expect(entry.decision == .wouldSkip)
            #expect(entry.wouldGate == true)
            #expect(entry.transcribed == true)
        }
        // Transcript chunks would only land if the engine saw the
        // shards. The stub recognizer returns `[]` so chunk count
        // remains zero — but the engine *received* the shards. The
        // strongest assertion is via the logged `transcribed=true`
        // above; the rest is downstream-engine territory.
    }

    @Test("Below-threshold shards tagged would-skip")
    func testBelowThresholdTagsWouldSkip() async throws {
        let store = try await makeTestStore()
        try await seedAssetWithFeatureCoverage(
            store: store,
            featureCoverageEndTime: 120
        )
        try await seedFeatureWindows(
            store: store,
            assetId: "test-asset",
            start: 0,
            end: 120
        ) { s, e in cleanSpeechWindow(assetId: "test-asset", startTime: s, endTime: e) }

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards4()
        let logger = RecordingTranscriptShadowGateLogger()
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            gateConfig: .default,
            shadowLogger: logger,
            safetySampleCoin: 0.99
        )

        _ = await runner.run(makeGateRequest(desiredCoverageSec: 120))

        let entries = await logger.snapshot()
        #expect(entries.count == 4)
        for entry in entries {
            #expect(entry.wouldGate == true)
            #expect(entry.decision == .wouldSkip)
            // The recorded likelihood must round-trip: it's the number
            // the eval pipeline will use to recompute would-skip recall.
            #expect(entry.likelihood != nil)
            if let s = entry.likelihood {
                #expect(s < 0.30)
            }
        }
    }

    @Test("Above-threshold shards tagged keep")
    func testAboveThresholdTagsAboveThreshold() async throws {
        let store = try await makeTestStore()
        try await seedAssetWithFeatureCoverage(
            store: store,
            featureCoverageEndTime: 120
        )
        try await seedFeatureWindows(
            store: store,
            assetId: "test-asset",
            start: 0,
            end: 120
        ) { s, e in adOnsetWindow(assetId: "test-asset", startTime: s, endTime: e) }

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards4()
        let logger = RecordingTranscriptShadowGateLogger()
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            gateConfig: .default,
            shadowLogger: logger
        )

        _ = await runner.run(makeGateRequest(desiredCoverageSec: 120))

        let entries = await logger.snapshot()
        #expect(entries.count == 4)
        for entry in entries {
            #expect(entry.wouldGate == false)
            #expect(entry.decision == .aboveThreshold)
            #expect(entry.likelihood != nil)
            if let s = entry.likelihood {
                #expect(s >= 0.30)
            }
            #expect(entry.transcribed == true)
        }
    }

    @Test("Safety sample fraction = 1.0 keeps every below-threshold shard")
    func testSafetySampleAlwaysKeeps() async throws {
        let store = try await makeTestStore()
        try await seedAssetWithFeatureCoverage(
            store: store,
            featureCoverageEndTime: 120
        )
        try await seedFeatureWindows(
            store: store,
            assetId: "test-asset",
            start: 0,
            end: 120
        ) { s, e in cleanSpeechWindow(assetId: "test-asset", startTime: s, endTime: e) }

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards4()
        let logger = RecordingTranscriptShadowGateLogger()
        let alwaysKeep = AcousticTranscriptGateConfig(
            enabled: true,
            skipEnabled: false,
            likelihoodThreshold: 0.30,
            safetySampleFraction: 1.0
        )
        // Coin 0.5 < 1.0 → keep arm fires every time.
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            gateConfig: alwaysKeep,
            shadowLogger: logger,
            safetySampleCoin: 0.5
        )

        _ = await runner.run(makeGateRequest(desiredCoverageSec: 120))

        let entries = await logger.snapshot()
        #expect(entries.count == 4)
        for entry in entries {
            #expect(entry.decision == .safetySampleKeep)
            // wouldGate stays true because the *score* is still below
            // threshold — the safety sample is only the keep override.
            #expect(entry.wouldGate == true)
            #expect(entry.transcribed == true)
        }
    }

    @Test("Safety sample fraction = 0.0 never keeps below-threshold shards")
    func testSafetySampleNeverKeeps() async throws {
        let store = try await makeTestStore()
        try await seedAssetWithFeatureCoverage(
            store: store,
            featureCoverageEndTime: 120
        )
        try await seedFeatureWindows(
            store: store,
            assetId: "test-asset",
            start: 0,
            end: 120
        ) { s, e in cleanSpeechWindow(assetId: "test-asset", startTime: s, endTime: e) }

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards4()
        let logger = RecordingTranscriptShadowGateLogger()
        let neverKeep = AcousticTranscriptGateConfig(
            enabled: true,
            skipEnabled: false,
            likelihoodThreshold: 0.30,
            safetySampleFraction: 0.0
        )
        // Coin 0.0 — the only draw value that could *ever* hit the
        // sample fraction == 0 keep arm if we used `<=` semantics. We
        // use strict `<`, so this still falls through to `wouldSkip`.
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            gateConfig: neverKeep,
            shadowLogger: logger,
            safetySampleCoin: 0.0
        )

        _ = await runner.run(makeGateRequest(desiredCoverageSec: 120))

        let entries = await logger.snapshot()
        #expect(entries.count == 4)
        for entry in entries {
            #expect(entry.decision == .wouldSkip)
        }
    }

    // MARK: - Production-skip teeth (playhead-wu88)
    //
    // The 8 tests above all run with `skipEnabled=false` (shadow mode). The
    // production-skip teeth — `transcribed = !acousticGateConfig.isProductionSkipActive`
    // and `if transcribed { keptShards.append(shard) }` in
    // `AnalysisJobRunner.evaluateAcousticTranscriptGate` — are unexercised
    // by those tests. The next pair pins the two corner cases the future
    // `skipEnabled=true` flip must satisfy:
    //   * sample=0 → every below-threshold shard is dropped from the engine
    //     (transcribed=false, keptShards omits them).
    //   * sample=1 → every below-threshold shard is kept via the safety-sample
    //     override (transcribed=true on every row).
    // The shadow log's `transcribed` field is the runner's source of truth
    // for `keptShards.append(shard)` — they're set in the same conditional —
    // so asserting the log row's `transcribed` flag is equivalent to
    // asserting which shards reach the transcript engine.
    //
    // Implementation note: every production-skip test seeds shard 3 (90–120s)
    // with ad-onset features so at least one shard always reaches the
    // transcript engine. This is required because `runTranscriptionLoop`
    // returns early without emitting `.completed` when handed an empty shard
    // list (see TranscriptEngineService.swift line ~488), which would
    // otherwise stall each test for the runner's 5-minute completion
    // timeout. Including the above-threshold shard makes the test stronger
    // by exercising the gate's *selective* behaviour: would-skip shards are
    // dropped while above-threshold shards are kept in the same run.

    /// Seed three clean-speech feature windows in [0, 90) and one
    /// ad-onset window in [90, 120) so the gate categorises shards 0–2 as
    /// `wouldSkip` candidates and shard 3 as `aboveThreshold`. The
    /// above-threshold shard guarantees `keptShards` is non-empty so the
    /// transcript engine completes promptly even when the production-skip
    /// teeth drop the others.
    private func seedMixedSpeechAndAdOnsetWindows(
        store: AnalysisStore,
        assetId: String
    ) async throws {
        try await seedFeatureWindows(
            store: store,
            assetId: assetId,
            start: 0,
            end: 90
        ) { s, e in cleanSpeechWindow(assetId: assetId, startTime: s, endTime: e) }
        try await seedFeatureWindows(
            store: store,
            assetId: assetId,
            start: 90,
            end: 120
        ) { s, e in adOnsetWindow(assetId: assetId, startTime: s, endTime: e) }
    }

    // playhead-wu88: production skip ON, no safety sample. Below-threshold
    // shards must be tagged `wouldSkip` AND `transcribed=false` — the
    // production teeth then drop them from `keptShards` so the transcript
    // engine never sees them. The above-threshold shard at 90–120s is the
    // only shard that should reach the engine.
    @Test("skipEnabled=true with sample=0 drops every below-threshold shard from the engine")
    func testProductionSkipDropsWouldSkipShards() async throws {
        let store = try await makeTestStore()
        try await seedAssetWithFeatureCoverage(
            store: store,
            featureCoverageEndTime: 120
        )
        try await seedMixedSpeechAndAdOnsetWindows(
            store: store,
            assetId: "test-asset"
        )

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards4()
        let logger = RecordingTranscriptShadowGateLogger()
        let productionSkip = AcousticTranscriptGateConfig(
            enabled: true,
            skipEnabled: true,
            likelihoodThreshold: 0.30,
            safetySampleFraction: 0.0
        )
        // Coin 0.0 with strict `<` against fraction 0.0 still falls through
        // to `wouldSkip` — the safety sample never fires. With production
        // skip active, the runner sets `transcribed=false` and the shard is
        // dropped from `keptShards`.
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            gateConfig: productionSkip,
            shadowLogger: logger,
            safetySampleCoin: 0.0
        )

        _ = await runner.run(makeGateRequest(desiredCoverageSec: 120))

        let entries = await logger.snapshot()
        #expect(entries.count == 4)
        // Sort by shard start so we can address rows positionally.
        let sorted = entries.sorted { $0.shardStart < $1.shardStart }

        // Shards 0–2: clean speech → wouldSkip → dropped from engine.
        for entry in sorted.prefix(3) {
            #expect(entry.decision == .wouldSkip)
            #expect(entry.wouldGate == true)
            // Production teeth: `transcribed=false` is what causes the
            // shard to be omitted from `keptShards` and therefore never
            // reach the transcript engine.
            #expect(entry.transcribed == false)
            if let s = entry.likelihood {
                #expect(s < 0.30)
            }
        }
        // Shard 3: ad-onset features → aboveThreshold → kept regardless of
        // skipEnabled. Confirms the gate is *selective*, not a blanket drop.
        let last = sorted.last!
        #expect(last.decision == .aboveThreshold)
        #expect(last.wouldGate == false)
        #expect(last.transcribed == true)
        if let s = last.likelihood {
            #expect(s >= 0.30)
        }
    }

    // playhead-wu88: production skip ON, safety sample = 1.0. Every below-
    // threshold shard hits the keep arm and reaches the engine — verifying
    // the safety-sample override is wired correctly under production skip.
    @Test("skipEnabled=true with sample=1.0 keeps every below-threshold shard via safety sample")
    func testProductionSkipSafetySampleAllKept() async throws {
        let store = try await makeTestStore()
        try await seedAssetWithFeatureCoverage(
            store: store,
            featureCoverageEndTime: 120
        )
        try await seedFeatureWindows(
            store: store,
            assetId: "test-asset",
            start: 0,
            end: 120
        ) { s, e in cleanSpeechWindow(assetId: "test-asset", startTime: s, endTime: e) }

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards4()
        let logger = RecordingTranscriptShadowGateLogger()
        let productionSkipAlwaysSample = AcousticTranscriptGateConfig(
            enabled: true,
            skipEnabled: true,
            likelihoodThreshold: 0.30,
            safetySampleFraction: 1.0
        )
        // Coin 0.5 < 1.0 → safety-sample keep arm fires every time. Even
        // though `skipEnabled=true`, the keep arm runs *before* the
        // production-skip gate, so every shard is transcribed. (No empty-
        // engine stall risk here — every shard is kept.)
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            gateConfig: productionSkipAlwaysSample,
            shadowLogger: logger,
            safetySampleCoin: 0.5
        )

        _ = await runner.run(makeGateRequest(desiredCoverageSec: 120))

        let entries = await logger.snapshot()
        #expect(entries.count == 4)
        for entry in entries {
            #expect(entry.decision == .safetySampleKeep)
            #expect(entry.wouldGate == true)
            // Safety sample keeps the shard regardless of `skipEnabled`.
            #expect(entry.transcribed == true)
        }
    }

    // playhead-wu88: parameterised property-style sweep over the two
    // production-skip corners plus a 0.5 mid-point with the coin pinned on
    // either side of the fraction. Uses Swift Testing arguments to stamp
    // each invocation distinctly in the test report.
    //
    // Each case seeds shards 0–2 (0–90s) with clean speech and shard 3
    // (90–120s) with ad-onset features. Shard 3 is always kept
    // (`aboveThreshold`) so the transcript engine has at least one shard
    // and emits `.completed` promptly. The sweep asserts only the
    // *would-skip eligibility* outcome on shards 0–2:
    //
    //   - fraction=0.0, coin=0.0  → wouldSkip arm  → 0 of 3 kept
    //   - fraction=1.0, coin=0.5  → safetySample   → 3 of 3 kept
    //   - fraction=0.5, coin=0.25 → safetySample   → 3 of 3 kept (coin < fraction)
    //   - fraction=0.5, coin=0.75 → wouldSkip arm  → 0 of 3 kept (coin >= fraction)
    @Test(
        "skipEnabled=true safety-sample sweep matches expected kept count",
        arguments: [
            (fraction: 0.0, coin: 0.0,  expectedKept: 0, expectedDecision: TranscriptShadowGateEntry.Decision.wouldSkip),
            (fraction: 1.0, coin: 0.5,  expectedKept: 3, expectedDecision: TranscriptShadowGateEntry.Decision.safetySampleKeep),
            (fraction: 0.5, coin: 0.25, expectedKept: 3, expectedDecision: TranscriptShadowGateEntry.Decision.safetySampleKeep),
            (fraction: 0.5, coin: 0.75, expectedKept: 0, expectedDecision: TranscriptShadowGateEntry.Decision.wouldSkip),
        ]
    )
    func testProductionSkipSafetySampleSweep(
        fraction: Double,
        coin: Double,
        expectedKept: Int,
        expectedDecision: TranscriptShadowGateEntry.Decision
    ) async throws {
        let assetId = "test-asset-\(Int(fraction * 1000))-\(Int(coin * 1000))"
        let store = try await makeTestStore()
        try await seedAssetWithFeatureCoverage(
            store: store,
            assetId: assetId,
            featureCoverageEndTime: 120
        )
        try await seedMixedSpeechAndAdOnsetWindows(
            store: store,
            assetId: assetId
        )

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = (0..<4).map { i in
            makeShard(
                id: i,
                episodeID: "test-ep",
                startTime: Double(i) * 30,
                duration: 30
            )
        }
        let logger = RecordingTranscriptShadowGateLogger()
        let config = AcousticTranscriptGateConfig(
            enabled: true,
            skipEnabled: true,
            likelihoodThreshold: 0.30,
            safetySampleFraction: fraction
        )
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            gateConfig: config,
            shadowLogger: logger,
            safetySampleCoin: coin
        )

        _ = await runner.run(makeGateRequest(desiredCoverageSec: 120, assetId: assetId))

        let entries = await logger.snapshot()
        #expect(entries.count == 4)
        let sorted = entries.sorted { $0.shardStart < $1.shardStart }

        // Shards 0–2 are below-threshold and exercise the gate.
        let belowThreshold = Array(sorted.prefix(3))
        let kept = belowThreshold.filter { $0.transcribed }.count
        #expect(kept == expectedKept)
        for entry in belowThreshold {
            #expect(entry.decision == expectedDecision)
            #expect(entry.wouldGate == true)
        }
        // Shard 3 is ad-onset; it always reaches the engine regardless of
        // the safety-sample sweep parameters. It exists primarily to keep
        // the engine from stalling on an empty shard list.
        #expect(sorted.last?.decision == .aboveThreshold)
        #expect(sorted.last?.transcribed == true)
    }

    @Test("Quality precondition bypasses gating for already-transcribed shards")
    func testQualityPreconditionBypassesGating() async throws {
        let store = try await makeTestStore()
        // fastTranscriptCoverageEndTime = 120 covers every 0–120s shard
        // we'll feed in. Even though the windows are pure clean speech
        // (would otherwise score wouldSkip), the M1 precondition must
        // tag every shard `qualityPreconditionKeep`.
        try await seedAssetWithFeatureCoverage(
            store: store,
            featureCoverageEndTime: 120,
            fastTranscriptCoverageEndTime: 120
        )
        try await seedFeatureWindows(
            store: store,
            assetId: "test-asset",
            start: 0,
            end: 120
        ) { s, e in cleanSpeechWindow(assetId: "test-asset", startTime: s, endTime: e) }

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards4()
        let logger = RecordingTranscriptShadowGateLogger()
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            gateConfig: .default,
            shadowLogger: logger
        )

        _ = await runner.run(makeGateRequest(desiredCoverageSec: 120))

        let entries = await logger.snapshot()
        #expect(entries.count == 4)
        for entry in entries {
            #expect(entry.decision == .qualityPreconditionKeep)
            #expect(entry.wouldGate == false)
            // M1 mitigation: never call the scorer at all, so likelihood
            // stays nil on these rows.
            #expect(entry.likelihood == nil)
            #expect(entry.transcribed == true)
        }
    }

    @Test("Shards lacking pre-computed features tagged scoreUnknown and never gated")
    func testMissingFeaturesScoreUnknown() async throws {
        let store = try await makeTestStore()
        // Set featureCoverageEndTime to 120 so the live extractor's
        // `shardEnd <= effectiveCoverage` skip fires for every shard,
        // but DO NOT seed any feature_windows. The gate then sees an
        // empty windows array for every shard span.
        try await seedAssetWithFeatureCoverage(
            store: store,
            featureCoverageEndTime: 120
        )

        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = makeShards4()
        let logger = RecordingTranscriptShadowGateLogger()
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            gateConfig: .default,
            shadowLogger: logger
        )

        _ = await runner.run(makeGateRequest(desiredCoverageSec: 120))

        let entries = await logger.snapshot()
        #expect(entries.count == 4)
        for entry in entries {
            #expect(entry.decision == .scoreUnknown)
            #expect(entry.wouldGate == false)
            #expect(entry.likelihood == nil)
            #expect(entry.transcribed == true)
        }
    }
}
