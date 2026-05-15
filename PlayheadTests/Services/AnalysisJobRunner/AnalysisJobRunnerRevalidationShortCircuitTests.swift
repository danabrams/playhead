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

private final class RecordingCrossUserAnalysisSharingProvider: CrossUserAnalysisSharingProviding, @unchecked Sendable {
    let isEnabled = true
    var snapshot: CrossUserAnalysisSnapshot?
    private(set) var requestedKeys: [CrossUserAnalysisShareKey] = []
    private(set) var importedWindows: [AdWindow] = []
    private(set) var publishedSnapshots: [CrossUserAnalysisSnapshot] = []

    func matchingSnapshot(for key: CrossUserAnalysisShareKey) async -> CrossUserAnalysisSnapshot? {
        requestedKeys.append(key)
        guard snapshot?.key == key else { return nil }
        return snapshot
    }

    func publish(_ snapshot: CrossUserAnalysisSnapshot) async throws {
        publishedSnapshots.append(snapshot)
    }

    func didImportSharedAdWindows(_ windows: [AdWindow]) async {
        importedWindows.append(contentsOf: windows)
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
    desiredCoverageSec: Double = 120,
    outputPolicy: OutputPolicy = .writeWindowsAndCues
) -> AnalysisRangeRequest {
    AnalysisRangeRequest(
        jobId: UUID().uuidString,
        episodeId: "ep-zx6i",
        podcastId: "pod-zx6i",
        analysisAssetId: assetId,
        audioURL: makeTempAudioFile(),
        desiredCoverageSec: desiredCoverageSec,
        mode: .preRollWarmup,
        outputPolicy: outputPolicy,
        priority: .medium
    )
}

private func seedAssetWithDuration(
    store: AnalysisStore,
    assetId: String,
    episodeDurationSec: Double = 120,
    featureCoverage: Double? = 120,
    transcriptCoverage: Double? = 120,
    assetFingerprint: String? = nil
) async throws {
    let asset = AnalysisAsset(
        id: assetId,
        episodeId: "ep-zx6i",
        assetFingerprint: assetFingerprint ?? assetId,
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

private func makeSharedAnalysisSnapshot(
    key: CrossUserAnalysisShareKey,
    analysisCoverageEndSec: Double = 60
) -> CrossUserAnalysisSnapshot {
    CrossUserAnalysisSnapshot(
        key: key,
        provenance: CrossUserAnalysisProvenance(
            exportedAt: 1_800_000_000,
            sourceAnalysisVersion: key.analysisVersion,
            sourceAppBuild: "revalidation-test"
        ),
        analysisCoverageEndSec: analysisCoverageEndSec,
        measurements: CrossUserAnalysisMeasurements(),
        windows: [
            CrossUserAnalysisSnapshot.Window(
                sourceWindowId: "peer-revalidation-window",
                startTime: 10,
                endTime: analysisCoverageEndSec,
                confidence: 0.9,
                boundaryState: AdBoundaryState.acousticRefined.rawValue,
                decisionState: AdDecisionState.confirmed.rawValue,
                detectorVersion: "fm-test-v1",
                advertiser: "Acme",
                product: "Widget",
                adDescription: "Imported promo",
                metadataSource: "foundation-model",
                metadataConfidence: 0.82,
                metadataPromptVersion: "prompt-v1",
                evidenceSources: "semantic,fusion",
                eligibilityGate: "ready",
                catalogStoreMatchSimilarity: nil
            ),
        ]
    )
}

private func makeRunner(
    store: AnalysisStore,
    audioStub: RecordingAudioProvider,
    adStub: StubAdDetectionProvider,
    flagEnabled: Bool,
    completedVersions: PipelineVersions?,
    currentVersions: PipelineVersions,
    analysisSharingProvider: CrossUserAnalysisSharingProviding = NoOpCrossUserAnalysisSharingProvider()
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
        completedPipelineVersionsLoader: { _ in completedVersions },
        analysisSharingProvider: analysisSharingProvider
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

    @Test("flag ON + chunks + stamp + version bump → shared import is not queried before revalidation")
    func bumpDoesNotShortCircuitThroughSharedImport() async throws {
        let assetId = "asset-bump-shared-import-gated"
        let fullFileSHA = "8888888888888888888888888888888888888888888888888888888888888888"
        let store = try await makeTestStore()
        try await seedAssetWithDuration(
            store: store,
            assetId: assetId,
            assetFingerprint: fullFileSHA
        )
        try await seedOneChunk(store: store, assetId: assetId)

        let key = CrossUserAnalysisShareKey(
            podcastId: "pod-zx6i",
            fileSHA: fullFileSHA,
            analysisVersion: 1
        )
        let sharingProvider = RecordingCrossUserAnalysisSharingProvider()
        sharingProvider.snapshot = makeSharedAnalysisSnapshot(key: key)

        let audioStub = RecordingAudioProvider()
        let adStub = StubAdDetectionProvider()
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            adStub: adStub,
            flagEnabled: true,
            completedVersions: Self.baseVersions,
            currentVersions: Self.bumpedVersions,
            analysisSharingProvider: sharingProvider
        )

        let outcome = await runner.run(makeRequest(assetId: assetId))

        if case .reachedTarget = outcome.stopReason {
            // expected
        } else {
            Issue.record("Expected .reachedTarget but got \(outcome.stopReason)")
        }
        #expect(audioStub.decodeCallCount == 0)
        #expect(adStub.revalidateFromFeaturesCallCount == 1)
        #expect(sharingProvider.requestedKeys.isEmpty)
        #expect(sharingProvider.importedWindows.isEmpty)
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

    @Test("flag ON + chunks + no stamp → shared import is not queried before baseline full analysis")
    func noStampDoesNotShortCircuitThroughSharedImport() async throws {
        let assetId = "asset-no-stamp-shared-import-gated"
        let fullFileSHA = "9999999999999999999999999999999999999999999999999999999999999999"
        let store = try await makeTestStore()
        try await seedAssetWithDuration(
            store: store,
            assetId: assetId,
            assetFingerprint: fullFileSHA
        )
        try await seedOneChunk(store: store, assetId: assetId)

        let key = CrossUserAnalysisShareKey(
            podcastId: "pod-zx6i",
            fileSHA: fullFileSHA,
            analysisVersion: 1
        )
        let sharingProvider = RecordingCrossUserAnalysisSharingProvider()
        sharingProvider.snapshot = makeSharedAnalysisSnapshot(key: key)

        let audioStub = RecordingAudioProvider()
        let adStub = StubAdDetectionProvider()
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            adStub: adStub,
            flagEnabled: true,
            completedVersions: nil,
            currentVersions: Self.bumpedVersions,
            analysisSharingProvider: sharingProvider
        )

        _ = await runner.run(makeRequest(assetId: assetId))

        #expect(audioStub.decodeCallCount == 1)
        #expect(adStub.revalidateFromFeaturesCallCount == 0)
        #expect(sharingProvider.requestedKeys.isEmpty)
        #expect(sharingProvider.importedWindows.isEmpty)
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

    @Test("flag ON + bump → outcome.cueCoverageSec reflects persisted AdWindow max-endTime; newCueCount = 0 (R2 parity pin)")
    func revalidationOutcomeReportsHonestCueCoverageAndZeroNewCount() async throws {
        // R2 audit pin: the R1 fix replaced `cueCoverageSec = 0` on
        // the revalidation success path with a live re-fetch of the
        // persisted `AdWindow` rows, filtered by the same
        // (confidence >= 0.7, endTime > startTime) predicate the
        // full-path return uses. The fix is correct only if (a) the
        // filter matches the full-path filter, and (b) the value
        // propagates into the returned `AnalysisOutcome`. Pin both
        // by seeding a known set of windows pre-revalidation and
        // asserting the outcome.
        //
        // Companion assertion: `newCueCount == 0` on the revalidation
        // path is intentional (every "new" window on the revalidation
        // path is a re-classification of an existing span; counting
        // those as "new ad detections" would be misleading). Pin the
        // 0 explicitly so a future refactor cannot silently start
        // counting re-classifications without updating this test.
        let assetId = "asset-cue-coverage-parity"
        let store = try await makeTestStore()
        try await seedAssetWithDuration(store: store, assetId: assetId)
        try await seedOneChunk(store: store, assetId: assetId)

        // Seed four pre-revalidation AdWindow rows. Two pass the
        // cue filter; one is below the confidence threshold; one is a
        // high-confidence suppressed/non-ad decision. Neither rejected
        // row may contribute to the cue-coverage watermark. The highest
        // endTime among the passing rows is 90, so the outcome must
        // report exactly that.
        let passingHigh = makeAdWindow(startTime: 30, endTime: 90, confidence: 0.85)
        let passingLow = makeAdWindow(startTime: 60, endTime: 75, confidence: 0.95)
        let belowThreshold = makeAdWindow(startTime: 0, endTime: 120, confidence: 0.5)
        let suppressedHigh = makeAdWindow(startTime: 95, endTime: 120, confidence: 0.99)
        // makeAdWindow uses a fixed "test-asset" analysisAssetId; we
        // need the real assetId for these rows so the runner can
        // fetch them through `store.fetchAdWindows(assetId:)`.
        let windowsToSeed = [passingHigh, passingLow, belowThreshold, suppressedHigh].map { w in
            AdWindow(
                id: w.id,
                analysisAssetId: assetId,
                startTime: w.startTime,
                endTime: w.endTime,
                confidence: w.confidence,
                boundaryState: w.boundaryState,
                decisionState: w.id == suppressedHigh.id
                    ? AdDecisionState.suppressed.rawValue
                    : w.decisionState,
                detectorVersion: w.detectorVersion,
                advertiser: nil,
                product: nil,
                adDescription: nil,
                evidenceText: nil,
                evidenceStartTime: w.startTime,
                metadataSource: "none",
                metadataConfidence: nil,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false
            )
        }
        try await store.insertAdWindows(windowsToSeed)

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

        // Sanity: the revalidation path was taken.
        #expect(audioStub.decodeCallCount == 0)
        #expect(adStub.revalidateFromFeaturesCallCount == 1)

        // Honest cue coverage: highest endTime among confidence-passing,
        // non-degenerate ad windows. The `belowThreshold` and
        // `suppressedHigh` rows both end at 120 and must NOT be picked
        // up.
        #expect(outcome.cueCoverageSec == 90, "outcome.cueCoverageSec must equal the max endTime of cue-eligible windows (90), not the unfiltered max (120) or the legacy hard-coded 0")

        // Zero new-cue count on revalidation is intentional — every
        // window on this path is a re-classification, not a new ad
        // detection. Pin so a future change cannot silently start
        // reporting non-zero values without updating this test.
        #expect(outcome.newCueCount == 0, "outcome.newCueCount must be 0 on the revalidation success path (re-classifications are not new cues)")

        // Feature + transcript coverage propagate from the persisted
        // asset row (seeded to 120 in `seedAssetWithDuration`); pin
        // them so a future refactor that drops the asset-row reload
        // does not silently regress to 0.
        #expect(outcome.featureCoverageSec == 120)
        #expect(outcome.transcriptCoverageSec == 120)
    }

    @Test("revalidation writeWindowsAndCues does not publish shared analysis snapshots")
    func revalidationWriteWindowsAndCuesDoesNotPublishSharedSnapshot() async throws {
        let assetId = "asset-revalidation-share-publish"
        let fullFileSHA = "7777777777777777777777777777777777777777777777777777777777777777"
        let store = try await makeTestStore()
        try await seedAssetWithDuration(
            store: store,
            assetId: assetId,
            assetFingerprint: fullFileSHA
        )
        try await seedOneChunk(store: store, assetId: assetId)
        try await store.insertAdWindow(
            AdWindow(
                id: "revalidated-window",
                analysisAssetId: assetId,
                startTime: 15,
                endTime: 45,
                confidence: 0.91,
                boundaryState: AdBoundaryState.acousticRefined.rawValue,
                decisionState: AdDecisionState.confirmed.rawValue,
                detectorVersion: "fm-test-v1",
                advertiser: "Acme",
                product: "Widget",
                adDescription: "Revalidated promo",
                evidenceText: nil,
                evidenceStartTime: nil,
                metadataSource: "foundation-model",
                metadataConfidence: 0.82,
                metadataPromptVersion: "prompt-v1",
                wasSkipped: false,
                userDismissedBanner: false
            )
        )

        let audioStub = RecordingAudioProvider()
        let adStub = StubAdDetectionProvider()
        let sharingProvider = RecordingCrossUserAnalysisSharingProvider()
        let runner = try await makeRunner(
            store: store,
            audioStub: audioStub,
            adStub: adStub,
            flagEnabled: true,
            completedVersions: Self.baseVersions,
            currentVersions: Self.bumpedVersions,
            analysisSharingProvider: sharingProvider
        )

        let outcome = await runner.run(makeRequest(
            assetId: assetId,
            outputPolicy: .writeWindowsAndCues
        ))

        if case .reachedTarget = outcome.stopReason {
            // expected
        } else {
            Issue.record("Expected .reachedTarget but got \(outcome.stopReason)")
        }
        #expect(audioStub.decodeCallCount == 0)
        #expect(adStub.revalidateFromFeaturesCallCount == 1)
        #expect(sharingProvider.publishedSnapshots.isEmpty)
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
