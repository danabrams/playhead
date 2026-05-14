// AnalysisJobRunnerRevalidationShortCircuitTests.swift
// playhead-zx6i — Structural tests for the B4 fast revalidation
// short-circuit. The contract under test is "when the flag is ON,
// persisted chunks exist, a completed-versions stamp exists, AND the
// stamped versions differ from the current versions, the runner
// MUST NOT call audioProvider.decode / featureService.extract /
// transcriptEngine.startTranscription, and MUST call
// adDetection.revalidateFromFeatures". Every test pins one axis at
// a time so a regression that breaks any individual gate condition
// (flag-OFF byte-identical, cold-start asset, no stamp, no bump)
// can be diagnosed from the failing test name.

import CryptoKit
import Foundation
import Testing
@testable import Playhead

// MARK: - Local recording stubs

/// Counts every `decode` call so we can assert "decode was skipped".
/// Returns an empty shard list (the test does not progress beyond
/// stage 1 on the revalidation path; on the full-analysis path the
/// runner exits with a `decode: no shards within desired coverage`
/// failure, which is fine for these tests — we never inspect the
/// outcome on that branch).
private final class RecordingAudioProvider: AnalysisAudioProviding, @unchecked Sendable {
    var decodeCallCount = 0
    var shardsToReturn: [AnalysisShard] = []

    func decode(fileURL: LocalAudioURL, episodeID: String, shardDuration: TimeInterval) async throws -> [AnalysisShard] {
        decodeCallCount += 1
        return shardsToReturn
    }
}

// MARK: - Helpers

private func makeTempAudioFile() -> LocalAudioURL {
    let tmpDir = try! makeTempDir(prefix: "AnalysisJobRunnerRevalidationShortCircuitTests")
    let audioFile = tmpDir.appendingPathComponent("episode.m4a")
    FileManager.default.createFile(atPath: audioFile.path, contents: Data())
    return LocalAudioURL(audioFile)!
}

private func makeRequest(
    assetId: String = "test-asset-zx6i",
    desiredCoverageSec: Double = 120
) -> AnalysisRangeRequest {
    AnalysisRangeRequest(
        jobId: UUID().uuidString,
        episodeId: "ep-zx6i",
        podcastId: "pod-zx6i",
        analysisAssetId: assetId,
        audioURL: makeTempAudioFile(),
        desiredCoverageSec: desiredCoverageSec,
        mode: .preRollWarmup,
        outputPolicy: .writeWindowsAndCues,
        priority: .medium
    )
}

private func seedAssetWithDuration(
    store: AnalysisStore,
    assetId: String,
    episodeDurationSec: Double = 120,
    featureCoverage: Double? = 120,
    transcriptCoverage: Double? = 120
) async throws {
    let asset = AnalysisAsset(
        id: assetId,
        episodeId: "ep-zx6i",
        assetFingerprint: assetId,
        weakFingerprint: nil,
        sourceURL: "",
        featureCoverageEndTime: featureCoverage,
        fastTranscriptCoverageEndTime: transcriptCoverage,
        confirmedAdCoverageEndTime: nil,
        analysisState: SessionState.queued.rawValue,
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
    try await store.insertAsset(asset)
    try await store.updateEpisodeDuration(id: assetId, episodeDurationSec: episodeDurationSec)
}

private func seedOneChunk(store: AnalysisStore, assetId: String) async throws {
    let text = "hello"
    let input = "\(text)|0.0|0.5"
    let digest = SHA256.hash(data: Data(input.utf8))
    let fp = digest.prefix(16).map { String(format: "%02x", $0) }.joined()
    let chunk = TranscriptChunk(
        id: UUID().uuidString,
        analysisAssetId: assetId,
        segmentFingerprint: fp,
        chunkIndex: 0,
        startTime: 0,
        endTime: 0.5,
        text: text,
        normalizedText: text.lowercased(),
        pass: "final",
        modelVersion: "apple-speech-v1",
        transcriptVersion: nil,
        atomOrdinal: nil
    )
    try await store.insertTranscriptChunks([chunk])
}

private func makeRunner(
    store: AnalysisStore,
    audioStub: RecordingAudioProvider,
    adStub: StubAdDetectionProvider,
    flagEnabled: Bool,
    completedVersions: PipelineVersions?,
    currentVersions: PipelineVersions
) async throws -> AnalysisJobRunner {
    let featureService = FeatureExtractionService(store: store)
    let speechService = SpeechService(recognizer: StubSpeechRecognizer())
    try await speechService.loadFastModel()
    let transcriptEngine = TranscriptEngineService(
        speechService: speechService,
        store: store
    )
    return AnalysisJobRunner(
        store: store,
        audioProvider: audioStub,
        featureService: featureService,
        transcriptEngine: transcriptEngine,
        adDetection: adStub,
        b4RevalidationEnabledProvider: { flagEnabled },
        currentPipelineVersionsProvider: { currentVersions },
        completedPipelineVersionsLoader: { _ in completedVersions }
    )
}

// MARK: - Tests

@Suite("AnalysisJobRunner zx6i revalidation short-circuit")
struct AnalysisJobRunnerRevalidationShortCircuitTests {

    private static let baseVersions = PipelineVersions(
        modelVersion: "detection-v1",
        policyVersion: "skip-policy-v1",
        featureSchemaVersion: 1
    )
    private static let bumpedVersions = PipelineVersions(
        modelVersion: "detection-v2", // bumped
        policyVersion: "skip-policy-v1",
        featureSchemaVersion: 1
    )

    @Test("flag ON + chunks + stamp + version bump → revalidation path is taken (decode skipped, revalidate called)")
    func bumpTakesRevalidationPath() async throws {
        let assetId = "asset-bump-revalidate"
        let store = try await makeTestStore()
        try await seedAssetWithDuration(store: store, assetId: assetId)
        try await seedOneChunk(store: store, assetId: assetId)

        let audioStub = RecordingAudioProvider()
        let adStub = StubAdDetectionProvider()
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            adStub: adStub,
            flagEnabled: true,
            completedVersions: Self.baseVersions,
            currentVersions: Self.bumpedVersions
        )

        let outcome = await runner.run(makeRequest(assetId: assetId))

        // The hot full-analysis stages MUST NOT have run. decode is
        // the easiest to assert because it's the first thing
        // `run(_:)` would call on the fall-through path.
        #expect(audioStub.decodeCallCount == 0)
        // The revalidation entry point MUST have been called exactly
        // once, with the request's asset id and podcast id threaded
        // through.
        #expect(adStub.revalidateFromFeaturesCallCount == 1)
        #expect(adStub.revalidateFromFeaturesCalls.first?.assetId == assetId)
        #expect(adStub.revalidateFromFeaturesCalls.first?.podcastId == "pod-zx6i")
        #expect(adStub.revalidateFromFeaturesCalls.first?.episodeDuration == 120)
        // Hot path / backfill on the stub MUST NOT have fired (those
        // are part of the full-analysis path, which the short-circuit
        // skipped).
        #expect(adStub.hotPathCallCount == 0)
        #expect(adStub.backfillCallCount == 0)
        // The outcome should report success.
        if case .reachedTarget = outcome.stopReason { /* ok */ }
        else { Issue.record("Expected .reachedTarget but got \(outcome.stopReason)") }
    }

    @Test("flag OFF → full analysis path runs even with a stamp and a version bump")
    func flagOffTakesFullPath() async throws {
        let assetId = "asset-flag-off"
        let store = try await makeTestStore()
        try await seedAssetWithDuration(store: store, assetId: assetId)
        try await seedOneChunk(store: store, assetId: assetId)

        let audioStub = RecordingAudioProvider()
        let adStub = StubAdDetectionProvider()
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            adStub: adStub,
            flagEnabled: false,                     // ← flag OFF
            completedVersions: Self.baseVersions,   // would otherwise trigger
            currentVersions: Self.bumpedVersions
        )

        _ = await runner.run(makeRequest(assetId: assetId))

        // With the flag OFF the short-circuit branch is structurally
        // unreachable — decode MUST have been called.
        #expect(audioStub.decodeCallCount == 1)
        // The revalidation entry point MUST NOT have been called.
        #expect(adStub.revalidateFromFeaturesCallCount == 0)
    }

    @Test("flag ON + matching versions → revalidation NOT triggered (no bump)")
    func matchingVersionsDoNotRevalidate() async throws {
        let assetId = "asset-no-bump"
        let store = try await makeTestStore()
        try await seedAssetWithDuration(store: store, assetId: assetId)
        try await seedOneChunk(store: store, assetId: assetId)

        let audioStub = RecordingAudioProvider()
        let adStub = StubAdDetectionProvider()
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            adStub: adStub,
            flagEnabled: true,
            completedVersions: Self.baseVersions,   // same as current → no bump
            currentVersions: Self.baseVersions
        )

        _ = await runner.run(makeRequest(assetId: assetId))

        // Versions match → no revalidation; the runner falls through
        // to the existing no-op skip-hot-path / skip-backfill
        // branches (handled by the full-path code, which is correct
        // behaviour — see runner doc comment). The revalidation
        // entry point MUST NOT have fired.
        #expect(adStub.revalidateFromFeaturesCallCount == 0)
        // Decode runs as part of the fall-through (full path).
        #expect(audioStub.decodeCallCount == 1)
    }

    @Test("flag ON + no stamp → revalidation NOT triggered (pre-zx6i asset takes full path)")
    func noStampDoesNotRevalidate() async throws {
        let assetId = "asset-no-stamp"
        let store = try await makeTestStore()
        try await seedAssetWithDuration(store: store, assetId: assetId)
        try await seedOneChunk(store: store, assetId: assetId)

        let audioStub = RecordingAudioProvider()
        let adStub = StubAdDetectionProvider()
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            adStub: adStub,
            flagEnabled: true,
            completedVersions: nil,                 // ← pre-zx6i asset
            currentVersions: Self.bumpedVersions
        )

        _ = await runner.run(makeRequest(assetId: assetId))

        #expect(adStub.revalidateFromFeaturesCallCount == 0)
        #expect(audioStub.decodeCallCount == 1)
    }

    @Test("flag ON + no persisted chunks → revalidation NOT triggered (cold start takes full path)")
    func noChunksDoesNotRevalidate() async throws {
        let assetId = "asset-no-chunks"
        let store = try await makeTestStore()
        try await seedAssetWithDuration(store: store, assetId: assetId)
        // Deliberately do not seed any chunks.

        let audioStub = RecordingAudioProvider()
        let adStub = StubAdDetectionProvider()
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            adStub: adStub,
            flagEnabled: true,
            completedVersions: Self.baseVersions,
            currentVersions: Self.bumpedVersions
        )

        _ = await runner.run(makeRequest(assetId: assetId))

        #expect(adStub.revalidateFromFeaturesCallCount == 0)
        #expect(audioStub.decodeCallCount == 1)
    }

    @Test("flag ON + bump + missing episodeDurationSec → falls back to full analysis")
    func missingDurationFallsBackToFullPath() async throws {
        let assetId = "asset-no-duration"
        let store = try await makeTestStore()
        // Seed the asset row but do NOT call updateEpisodeDuration.
        let asset = AnalysisAsset(
            id: assetId,
            episodeId: "ep-zx6i",
            assetFingerprint: assetId,
            weakFingerprint: nil,
            sourceURL: "",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)
        try await seedOneChunk(store: store, assetId: assetId)

        let audioStub = RecordingAudioProvider()
        let adStub = StubAdDetectionProvider()
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            adStub: adStub,
            flagEnabled: true,
            completedVersions: Self.baseVersions,
            currentVersions: Self.bumpedVersions
        )

        _ = await runner.run(makeRequest(assetId: assetId))

        // Without a duration the classifier's position priors degrade;
        // the runner must NOT take the revalidation path. Decode runs.
        #expect(adStub.revalidateFromFeaturesCallCount == 0)
        #expect(audioStub.decodeCallCount == 1)
    }
}
