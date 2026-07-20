// RediffActivationWiringTests.swift
// playhead-xsdz.36: end-to-end coverage for the ACTIVATION wiring — the full
// production loop `RediffRefetchService` sweep → rotation → B-copy handoff
// (`RevalidatingRediffBSideConsumer` stage → `revalidateFromFeatures` →
// unstage) → `computeRediffSlotPass` byte differ → persisted `.rediffSlot`
// width marks → B-copy DELETED — plus the OFF/empty byte-identity claims:
//
//   • activation switch pinned ON (the xsdz.36 mark-only ship state);
//   • an injected-but-EMPTY staging provider (every ordinary analysis run
//     under activation) produces spans identical to the no-provider run;
//   • the A-side capture branch in `AnalysisJobRunner` is opt-in per
//     instance: default constructions persist nothing (byte-identity for
//     every existing call site), the activation flag persists the exact
//     extractor stream, and the duration cap skips over-long episodes.
//
// Synthetic A/B MP3 fixtures mirror `RediffByteFirstEndToEndTests`.

import Foundation
import Testing

@testable import Playhead

@Suite("Rediff activation wiring end-to-end (playhead-xsdz.36)")
struct RediffActivationWiringTests {

    typealias Policy = RediffRefetchPolicy
    static let day = Policy.Configuration.secondsPerDay

    // MARK: - Fixtures (mirroring RediffByteFirstEndToEndTests)

    private func chunks(assetId: String) -> [TranscriptChunk] {
        let specs: [(Double, Double, String)] = [
            (0, 100, "Welcome to the show. We talk at length about science and history here."),
            (100, 160, "This segment is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show. Build your website today."),
            (160, 280, "Back to the conversation about the future and what comes next for all of us.")
        ]
        return specs.enumerated().map { idx, s in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)", analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)", chunkIndex: idx,
                startTime: s.0, endTime: s.1, text: s.2,
                normalizedText: s.2.lowercased(), pass: "final",
                modelVersion: "test-v1", transcriptVersion: nil, atomOrdinal: nil
            )
        }
    }

    /// A/B synthetic MP3 pair: A carries an ID3-separated ad block over
    /// [~95, ~165] s; B is the same content without it (same construction as
    /// the byte-first e2e suite).
    private struct BytePair {
        let aURL: URL
        let bURL: URL
        static func stage(in directory: URL) throws -> BytePair {
            let adStartFrame = 3637   // ≈ 95.008 s
            let adFrames = 2680       // ≈ 70.008 s
            let contentFrames = 10719 // ≈ 280.0 s of played (A) audio
            let c1 = SyntheticMP3.frames(count: adStartFrame, seed: 0xC0FFEE)
            let c2 = SyntheticMP3.frames(count: contentFrames - adStartFrame - adFrames, seed: 0xFACADE)
            let ad = SyntheticMP3.frames(count: adFrames, seed: 0xAD_B10C)
            let aData = SyntheticMP3.file(c1 + [SyntheticMP3.id3v2(payloadBytes: 32)] + ad + c2)
            let bData = SyntheticMP3.file(c1 + c2)
            let aURL = directory.appendingPathComponent("act-a.mp3", isDirectory: false)
            let bURL = directory.appendingPathComponent("act-b.fresh.mp3", isDirectory: false)
            try aData.write(to: aURL)
            try bData.write(to: bURL)
            return BytePair(aURL: aURL, bURL: bURL)
        }
    }

    private final class StubDecoder: AudioFileDecoding, @unchecked Sendable {
        func decodeMono16kHz(fileURL: URL) async throws -> [Float] { [] }
    }

    private func makeService(store: AnalysisStore, provider: RediffBSideProvider?) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70,
            suppressionThreshold: 0.25, hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1", fmBackfillMode: .off,
            rediffSlotOwnershipEnabled: true
        )
        return AdDetectionService(
            store: store, classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(), config: config,
            rediffBSideProvider: provider
        )
    }

    private func insertActivationAsset(
        store: AnalysisStore,
        assetId: String,
        sourceURL: String,
        duration: Double = 280
    ) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: assetId, episodeId: "ep-\(assetId)", assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil, sourceURL: sourceURL,
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil,
            episodeDurationSec: duration
        ))
    }

    // MARK: - Ship-state pins

    @Test("the single activation switch is ON (xsdz.36 mark-only rung) and the legacy compile-time flag stays OFF")
    func activationSwitchPins() {
        #expect(RediffActivation.isEnabledByDefault == true)
        // The pinned xsdz.27 constant is NOT the activation vehicle — the
        // runner's injected flag is (EpisodeFingerprintCaptureTests pins the
        // constant itself).
        #expect(EpisodeFingerprintCapture.captureEnabledByDefault == false)
        #expect(RediffActivation.maxASideCaptureDurationSeconds == 3 * 60 * 60)
    }

    // MARK: - The full ON loop

    @Test("sweep → rotation → staged B → revalidate → byte-exact .rediffSlot marks persisted; B deleted; staging empty")
    func fullActivationLoopProducesWidthMarks() async throws {
        let assetId = "asset-activation"
        let dir = try makeTempDir(prefix: "RediffActivation-\(assetId)")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try BytePair.stage(in: dir)

        // The "downloaded fresh copy" the stub fetcher hands the service —
        // a distinct file so the sweep's deletion is observable.
        let downloadedB = dir.appendingPathComponent("downloaded-b.mp3")
        try FileManager.default.copyItem(at: pair.bURL, to: downloadedB)

        let store = try await makeTestStore()
        try await insertActivationAsset(store: store, assetId: assetId, sourceURL: pair.aURL.absoluteString)
        try await store.insertTranscriptChunks(chunks(assetId: assetId))

        let staging = RediffBSideStagingProvider(decoder: StubDecoder(), durationProbe: { _ in nil })
        let adService = makeService(store: store, provider: staging)
        let consumer = RevalidatingRediffBSideConsumer(staging: staging, store: store, adDetection: adService)

        // Refetch service with production-shaped stubs: rotated pre-check,
        // stub full-fetch serving the downloaded copy, spy recorder.
        let sampler = StubRangedSampler()
        sampler.defaultSample = RemoteAudioSample(
            fingerprint: Policy.sampleFingerprint(head: Data("fresh".utf8), tail: Data("fresh".utf8), totalLength: 2),
            bytesTransferred: 131_072
        )
        let local = StubLocalSampler()
        local.defaultFingerprint = Policy.sampleFingerprint(head: Data("played".utf8), tail: Data("played".utf8), totalLength: 1)
        let full = StubFullFetcher()
        full.fileToReturn = downloadedB
        full.byteCount = 54_000_000
        let recorder = SpyRefetchRecorder()
        let enumerator = StubRefetchEnumerator()
        enumerator.candidatesToReturn = [RediffRefetchCandidate(
            assetId: assetId,
            enclosureURL: URL(string: "https://cdn.example.com/current.mp3")!,
            downloadedAt: 0,
            localAudioURL: pair.aURL,
            attemptState: .initial
        )]

        let refetch = RediffRefetchService(
            enabled: true,
            config: .production,
            enumerator: enumerator,
            rangedSampler: sampler,
            localSampler: local,
            fullFetcher: full,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: recorder,
            fileRemover: FileManagerTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            bsideConsumer: consumer,
            now: { 100 * Self.day }
        )

        let summary = await refetch.runRefetchSweep()

        // Sweep bookkeeping: one rotation, resolved state, bytes accounted.
        #expect(summary.rotatedCount == 1)
        #expect(summary.fullFetchBytes == 54_000_000)
        guard case let .rotated(_, cost, fingerprintCount, newState) = recorder.outcomes.first else {
            Issue.record("expected .rotated, got \(String(describing: recorder.outcomes.first))"); return
        }
        #expect(cost.fullFetchBytes == 54_000_000)
        #expect(fingerprintCount == 0, "consumer path skips the standalone fingerprint")
        #expect(newState.resolved, "consumed rotation is terminal")

        // The actual product outcome: byte-exact .rediffSlot width marks.
        let spans = try await store.fetchDecodedSpans(assetId: assetId)
        let rediffOwned = spans.filter { $0.anchorProvenance.contains(.rediffSlot) }
        #expect(rediffOwned.count == 1, "exactly the ad span is rediff-width-owned, got \(spans.map(\.anchorProvenance))")
        if let span = rediffOwned.first {
            #expect(span.startTime >= 94.5 && span.startTime <= 95.5, "start ≈ 95, got \(span.startTime)")
            #expect(span.endTime >= 164.5 && span.endTime <= 165.5, "end ≈ 165, got \(span.endTime)")
        }

        // Hygiene: the fetched copy is gone; no stale staging mapping.
        #expect(!FileManager.default.fileExists(atPath: downloadedB.path), "B-copy must be deleted after consumption")
        #expect(await staging.stagedCount == 0)
    }

    @Test("a consume failure records .failed (no resolve, R2 state advanced), deletes the B-copy, and leaves no marks")
    func consumeFailureIsRetriedNotResolved() async throws {
        let dir = try makeTempDir(prefix: "RediffActivation-fail")
        defer { try? FileManager.default.removeItem(at: dir) }
        let downloadedB = dir.appendingPathComponent("downloaded-b.mp3")
        try Data(repeating: 9, count: 2048).write(to: downloadedB)

        // Store WITHOUT the asset row → consumer throws .assetMissing.
        let store = try await makeTestStore()
        let staging = RediffBSideStagingProvider(decoder: StubDecoder(), durationProbe: { _ in nil })
        let adService = makeService(store: store, provider: staging)
        let consumer = RevalidatingRediffBSideConsumer(staging: staging, store: store, adDetection: adService)

        let sampler = StubRangedSampler()
        sampler.defaultSample = RemoteAudioSample(
            fingerprint: Policy.sampleFingerprint(head: Data("f".utf8), tail: Data("f".utf8), totalLength: 2),
            bytesTransferred: 131_072
        )
        let local = StubLocalSampler()
        local.defaultFingerprint = Policy.sampleFingerprint(head: Data("p".utf8), tail: Data("p".utf8), totalLength: 1)
        let full = StubFullFetcher()
        full.fileToReturn = downloadedB
        full.byteCount = 2_048
        let recorder = SpyRefetchRecorder()
        let enumerator = StubRefetchEnumerator()
        enumerator.candidatesToReturn = [RediffRefetchCandidate(
            assetId: "ghost",
            enclosureURL: URL(string: "https://cdn.example.com/ghost.mp3")!,
            downloadedAt: 0,
            localAudioURL: dir.appendingPathComponent("nonexistent-a.mp3"),
            attemptState: .initial
        )]

        let refetch = RediffRefetchService(
            enabled: true,
            enumerator: enumerator,
            rangedSampler: sampler,
            localSampler: local,
            fullFetcher: full,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: recorder,
            fileRemover: FileManagerTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            bsideConsumer: consumer,
            now: { 100 * Self.day }
        )
        _ = await refetch.runRefetchSweep()

        guard case let .failed(_, cost, failureClass, newState, _) = recorder.outcomes.first else {
            Issue.record("expected .failed, got \(String(describing: recorder.outcomes.first))"); return
        }
        #expect(failureClass == .staleAsset)
        #expect(newState.resolved == false, "a failed consume must stay retryable")
        #expect(newState.sameClassFailureStreak == 1)
        #expect(cost.fullFetchBytes == 2_048, "bytes spent are still accounted")
        #expect(!FileManager.default.fileExists(atPath: downloadedB.path), "B-copy deleted on the failure path too")
        #expect(await staging.stagedCount == 0)
    }

    // MARK: - Byte-identity: provider injected but nothing staged

    @Test("an EMPTY staging provider yields spans identical to the no-provider run (every ordinary backfill under activation)")
    func emptyStagingProviderIsByteIdenticalToNoProvider() async throws {
        func runAndProject(provider: RediffBSideProvider?) async throws -> [String] {
            let assetId = "asset-identity"
            let store = try await makeTestStore()
            try await insertActivationAsset(
                store: store, assetId: assetId,
                sourceURL: "file:///tmp/nonexistent-played.mp3"
            )
            let service = makeService(store: store, provider: provider)
            try await service.runBackfill(
                chunks: chunks(assetId: assetId), analysisAssetId: assetId,
                podcastId: "podcast-identity", episodeDuration: 280.0
            )
            // Project to content fields — row ids are per-run UUIDs.
            return try await store.fetchDecodedSpans(assetId: assetId)
                .map { span in
                    let provenance = span.anchorProvenance
                        .map { String(describing: $0) }
                        .sorted()
                        .joined(separator: ",")
                    return "\(span.startTime)|\(span.endTime)|\(provenance)"
                }
                .sorted()
        }

        let withoutProvider = try await runAndProject(provider: nil)
        let withEmptyStaging = try await runAndProject(
            provider: RediffBSideStagingProvider(decoder: StubDecoder(), durationProbe: { _ in nil })
        )
        #expect(withEmptyStaging == withoutProvider)
        #expect(!withEmptyStaging.contains { $0.contains("rediffSlot") },
                "no staged B-side ⇒ no rediff-owned spans")
    }

    // MARK: - A-side capture wiring in AnalysisJobRunner

    /// A 16 kHz multi-tone shard pair long enough to fingerprint.
    private func toneShards(episodeID: String) -> [AnalysisShard] {
        let samples = (0..<120_000).map { k -> Float in
            let t = Double(k) / 16_000.0
            return Float(0.4 * sin(2 * .pi * 220.0 * t) + 0.3 * sin(2 * .pi * 523.25 * t))
        }
        let mid = samples.count / 2
        return [
            AnalysisShard(id: 0, episodeID: episodeID, startTime: 0, duration: 3.75,
                          samples: Array(samples[0..<mid])),
            AnalysisShard(id: 1, episodeID: episodeID, startTime: 3.75, duration: 3.75,
                          samples: Array(samples[mid...])),
        ]
    }

    private func makeRunner(
        store: AnalysisStore,
        shards: [AnalysisShard],
        rediffASideCaptureEnabled: Bool?
    ) async throws -> AnalysisJobRunner {
        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(speechService: speechService, store: store)
        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = shards
        if let rediffASideCaptureEnabled {
            return AnalysisJobRunner(
                store: store,
                audioProvider: audioStub,
                featureService: featureService,
                transcriptEngine: transcriptEngine,
                adDetection: StubAdDetectionProvider(),
                rediffASideCaptureEnabled: rediffASideCaptureEnabled
            )
        }
        // DEFAULT construction — byte-identity for every existing call site.
        return AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: StubAdDetectionProvider()
        )
    }

    private func makeRunnerRequest(dir: URL, assetId: String) -> AnalysisRangeRequest {
        let audioFile = dir.appendingPathComponent("episode.m4a")
        FileManager.default.createFile(atPath: audioFile.path, contents: Data())
        return AnalysisRangeRequest(
            jobId: UUID().uuidString,
            episodeId: "ep-\(assetId)",
            podcastId: "test-pod",
            analysisAssetId: assetId,
            audioURL: LocalAudioURL(audioFile)!,
            desiredCoverageSec: 120,
            mode: .preRollWarmup,
            outputPolicy: .writeWindowsAndCues,
            priority: .medium
        )
    }

    @Test("default runner construction captures nothing (byte-identity); the activation flag persists the exact extractor stream")
    func runnerCaptureFlagWiring() async throws {
        // OFF (default): no fingerprint row.
        do {
            let assetId = "asset-cap-off"
            let dir = try makeTempDir(prefix: "RediffActCap-off")
            defer { try? FileManager.default.removeItem(at: dir) }
            let store = try await makeTestStore()
            try await insertActivationAsset(store: store, assetId: assetId, sourceURL: "")
            let shards = toneShards(episodeID: "ep-\(assetId)")
            let runner = try await makeRunner(store: store, shards: shards, rediffASideCaptureEnabled: nil)
            _ = await runner.run(makeRunnerRequest(dir: dir, assetId: assetId))
            #expect(try await store.fetchEpisodeFingerprints(assetId: assetId) == nil)
        }

        // ON: the persisted stream equals the canonical extractor output.
        do {
            let assetId = "asset-cap-on"
            let dir = try makeTempDir(prefix: "RediffActCap-on")
            defer { try? FileManager.default.removeItem(at: dir) }
            let store = try await makeTestStore()
            try await insertActivationAsset(store: store, assetId: assetId, sourceURL: "")
            let shards = toneShards(episodeID: "ep-\(assetId)")
            let runner = try await makeRunner(store: store, shards: shards, rediffASideCaptureEnabled: true)
            _ = await runner.run(makeRunnerRequest(dir: dir, assetId: assetId))
            let record = try #require(try await store.fetchEpisodeFingerprints(assetId: assetId))
            var mono = [Float]()
            for shard in shards { mono.append(contentsOf: shard.samples) }
            #expect(record.fingerprints == EpisodeFingerprintCapture.fingerprints(mono16kHz: mono))
            #expect(record.sourceAudioIdentity == "fp-\(assetId)")
        }

        // ON but over the duration cap: capture skipped. NOTE (R4): with no
        // A-side stream row the episode also drops out of re-fetch candidacy
        // entirely (candidacy = current-version row in `episode_fingerprints`)
        // — see `RediffActivation.maxASideCaptureDurationSeconds`.
        do {
            let assetId = "asset-cap-long"
            let dir = try makeTempDir(prefix: "RediffActCap-long")
            defer { try? FileManager.default.removeItem(at: dir) }
            let store = try await makeTestStore()
            try await insertActivationAsset(store: store, assetId: assetId, sourceURL: "")
            // Duration fields exceed the cap; sample payloads stay tiny.
            var shards = toneShards(episodeID: "ep-\(assetId)")
            shards = shards.map {
                AnalysisShard(id: $0.id, episodeID: $0.episodeID, startTime: $0.startTime,
                              duration: RediffActivation.maxASideCaptureDurationSeconds,
                              samples: $0.samples)
            }
            let runner = try await makeRunner(store: store, shards: shards, rediffASideCaptureEnabled: true)
            _ = await runner.run(makeRunnerRequest(dir: dir, assetId: assetId))
            #expect(try await store.fetchEpisodeFingerprints(assetId: assetId) == nil,
                    "over-cap episodes must skip the chroma A-side capture")
        }
    }

    @Test("recapture guard (R3): a matching-identity stream is not recomputed on later passes (capturedAt stable); a stale identity IS recaptured")
    func runnerSkipsRecaptureForMatchingIdentity() async throws {
        let assetId = "asset-cap-guard"
        let dir = try makeTempDir(prefix: "RediffActCap-guard")
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = try await makeTestStore()
        try await insertActivationAsset(store: store, assetId: assetId, sourceURL: "")
        let shards = toneShards(episodeID: "ep-\(assetId)")
        let runner = try await makeRunner(store: store, shards: shards, rediffASideCaptureEnabled: true)
        let request = makeRunnerRequest(dir: dir, assetId: assetId)

        // A pre-existing CURRENT-version stream whose identity does NOT match
        // the asset (a re-downloaded copy under a reused id) must be replaced.
        try await store.upsertEpisodeFingerprints(EpisodeFingerprintRecord(
            analysisAssetId: assetId,
            algorithmVersion: ChromaFingerprinter.algorithmVersion,
            secondsPerFingerprint: ChromaFingerprinter.secondsPerFingerprint,
            fingerprints: [1, 2, 3],
            sourceAudioIdentity: "stale-identity",
            capturedAt: 123
        ))
        _ = await runner.run(request)
        let first = try #require(try await store.fetchEpisodeFingerprints(assetId: assetId))
        #expect(first.sourceAudioIdentity == "fp-\(assetId)", "stale identity must be recaptured")
        #expect(first.capturedAt != 123)
        #expect(first.fingerprints != [1, 2, 3])

        // Second pass over the SAME audio identity: capture skipped — the
        // record (including `capturedAt`, the re-fetch enumerator's
        // downloaded-at baseline) is byte-identical, so repeat passes cannot
        // re-arm the ~3d first-attempt gate or re-spend the extractor walk.
        _ = await runner.run(request)
        let second = try #require(try await store.fetchEpisodeFingerprints(assetId: assetId))
        #expect(second.capturedAt == first.capturedAt, "a repeat pass must not bump capturedAt")
        #expect(second.fingerprints == first.fingerprints)
    }
}
