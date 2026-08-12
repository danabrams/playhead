// DownloadTimeAssetRegistrationTests.swift
// playhead-fzrw: the `analysis_assets` row for a completed download is
// registered AT ENQUEUE, not at first job RUN.
//
// THE MEASUREMENT THIS FILE EXISTS FOR (scratchpad/db-prewipe6/analysis.sqlite,
// the 2026-08-10 overnight pull). Five downloads landed as one batch — five
// `preAnalysis` jobs created inside a 37-second window, 09:02:21 → 09:02:58.
// The `analysis_assets` rows they need arrived at:
//
//     asset      job created   asset row registered   LAG
//     66D32039   09:02:21      09:02:21                    0 s
//     F7BB38BB   09:02:24      09:08:46                  382 s
//     B80D3EE0   09:02:26      09:18:23                  957 s
//     60DFF7B4   09:02:33      12:48:53               13,580 s
//     590D6656   09:02:58      12:50:56               13,678 s
//
// The day-0 kickoff's readiness budget is
// `dayZeroPreparationReadinessMaxAttempts (40) × pollNanos (10 s)` = 400 s, so
// THREE OF FIVE could not have reached the trigger by any drain strategy, on any
// network, in any process that survived. They would give up `no_analysis_asset`
// — which is exactly what playhead-kxgh's pull recorded five times, every row
// `lastPollCount=40`.
//
// WHY THE LAG WAS MINUTES TO HOURS. The row is minted by
// `AnalysisWorkScheduler.resolveAnalysisAssetId`, and its only caller is
// `processJob` — so the row appears when the SERIAL analysis lane reaches the
// episode. On that pull the lane was 3h46m behind by episode four.
//
// WHY REGISTERING EARLY IS SOUND AND NOT A PLACEHOLDER. The day-0 path reads
// exactly two things off the row: its `id` (the mint key) and its `sourceURL`
// (the read-only pinned A-side — `AdDetectionService.resolveDayZeroASide`, the
// single resolver shared by the mint and its free pre-fetch blocker). It reads
// no duration, no fingerprint, no transcript — nothing the analysis lane
// produces. And the row registered here is not a stub of the one the lane would
// have written: it is the same row, built by the same rules, earlier.

@preconcurrency import AVFoundation
import Foundation
import Testing

@testable import Playhead

@Suite("AnalysisWorkScheduler — download-time asset registration (fzrw)", .serialized)
struct DownloadTimeAssetRegistrationTests {

    // MARK: - Construction helpers

    private func makeRunner(store: AnalysisStore) -> AnalysisJobRunner {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        return AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
    }

    private func makeScheduler(
        store: AnalysisStore,
        downloadProvider: StubDownloadProvider
    ) -> AnalysisWorkScheduler {
        AnalysisWorkScheduler(
            store: store,
            jobRunner: makeRunner(store: store),
            capabilitiesService: StubCapabilitiesProvider(),
            downloadManager: downloadProvider,
            batteryProvider: {
                let battery = StubBatteryProvider()
                battery.level = 0.9
                battery.charging = true
                return battery
            }(),
            transportStatusProvider: StubTransportStatusProvider(),
            config: PreAnalysisConfig()
        )
    }

    /// A real, hashable, duration-probeable audio file — the enqueue path needs
    /// all three (SHA-256 identity, AVAsset duration, an mmap-able A-side).
    private func writeSynthAudio(
        seconds: TimeInterval,
        sampleRate: Double = 44_100
    ) throws -> URL {
        let tempDir = URL(fileURLWithPath: NSTemporaryDirectory())
        let fileURL = tempDir.appendingPathComponent("fzrw-\(UUID().uuidString).caf")

        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: sampleRate,
            channels: 1,
            interleaved: false
        ) else {
            throw NSError(domain: "DownloadTimeAssetRegistrationTests", code: -1)
        }

        let file = try AVAudioFile(
            forWriting: fileURL,
            settings: format.settings,
            commonFormat: .pcmFormatFloat32,
            interleaved: false
        )

        let totalFrames = AVAudioFramePosition(seconds * sampleRate)
        let chunkFrames = AVAudioFrameCount(sampleRate)
        var written = AVAudioFramePosition(0)
        while written < totalFrames {
            let remaining = AVAudioFrameCount(totalFrames - written)
            let frames = min(chunkFrames, remaining)
            guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frames) else {
                throw NSError(domain: "DownloadTimeAssetRegistrationTests", code: -2)
            }
            buffer.frameLength = frames
            try file.write(from: buffer)
            written += AVAudioFramePosition(frames)
        }

        return fileURL
    }

    // MARK: - THE BEAD

    @Test("a completed download has its analysis_assets row BEFORE the analysis lane ever runs")
    func assetRowRegisteredAtEnqueue() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        let url = try writeSynthAudio(seconds: 9.0)
        defer { try? FileManager.default.removeItem(at: url) }
        let sha = try FileHasher.sha256(fileURL: url)
        provider.cachedURLs["ep-fzrw"] = url
        provider.fingerprints["ep-fzrw"] = AudioFingerprint(weak: "weak-fzrw", strong: sha)

        await scheduler.enqueue(
            episodeId: "ep-fzrw",
            podcastId: "pod-fzrw",
            downloadId: "dl-fzrw",
            sourceFingerprint: sha,
            isExplicitDownload: false
        )

        // NO dispatch. This is the whole point: on the overnight pull the lane
        // was three hours and forty-six minutes from reaching episode four.
        let asset = try #require(
            try await store.fetchAssetByEpisodeId("ep-fzrw"),
            "the row the day-0 readiness probe waits for must exist at enqueue"
        )
        #expect(asset.assetFingerprint == sha)
        #expect(asset.analysisState == "queued")
    }

    @Test("the registered row carries a RESOLVABLE A-side, not just an id")
    func registeredRowCarriesUsableASide() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        let url = try writeSynthAudio(seconds: 4.0)
        defer { try? FileManager.default.removeItem(at: url) }
        let sha = try FileHasher.sha256(fileURL: url)
        provider.cachedURLs["ep-aside"] = url
        provider.fingerprints["ep-aside"] = AudioFingerprint(weak: "weak-aside", strong: sha)

        await scheduler.enqueue(
            episodeId: "ep-aside",
            podcastId: "pod-aside",
            downloadId: "dl-aside",
            sourceFingerprint: sha,
            isExplicitDownload: false
        )

        let asset = try #require(try await store.fetchAssetByEpisodeId("ep-aside"))
        // The day-0 mint's ONLY column read is `sourceURL`, resolved through
        // `AudioCacheLocation` and then mmapped. A row whose `sourceURL` does
        // not round-trip to readable bytes moves the failure rather than fixing
        // it — the kickoff would fire and the mint would exit `aSideNotAnchored`.
        let resolved = try #require(
            AudioCacheLocation.resolve(asset.sourceURL) { candidate in
                (try? candidate.resourceValues(forKeys: [.isRegularFileKey]).isRegularFile) == true
            },
            "sourceURL must resolve against the current container"
        )
        let bytes = try Data(contentsOf: resolved, options: .mappedIfSafe)
        #expect(!bytes.isEmpty, "the A-side must be mmap-able the moment the row exists")
    }

    @Test("the registered row carries the probed duration and the observed title")
    func registeredRowCarriesEnqueueTimeFacts() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        let url = try writeSynthAudio(seconds: 6.5)
        defer { try? FileManager.default.removeItem(at: url) }
        let sha = try FileHasher.sha256(fileURL: url)
        provider.cachedURLs["ep-facts"] = url
        provider.fingerprints["ep-facts"] = AudioFingerprint(weak: "weak-facts", strong: sha)

        await scheduler.enqueue(
            episodeId: "ep-facts",
            podcastId: "pod-facts",
            downloadId: "dl-facts",
            sourceFingerprint: sha,
            isExplicitDownload: true,
            podcastTitle: "Pod Facts",
            episodeTitle: "Episode Facts"
        )

        let asset = try #require(try await store.fetchAssetByEpisodeId("ep-facts"))
        let duration = try #require(asset.episodeDurationSec)
        #expect(abs(duration - 6.5) < 0.5, "the enqueue-time probe must land on the row it registers")
        #expect(asset.episodeTitle == "Episode Facts")
        #expect(asset.weakFingerprint == "weak-facts")
    }

    // MARK: - The day-0 readiness probe, end to end

    @Test("the day-0 readiness probe FIRES on its first poll after a download enqueue")
    func dayZeroReadinessProbeFiresOnFirstPoll() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        let url = try writeSynthAudio(seconds: 3.0)
        defer { try? FileManager.default.removeItem(at: url) }
        let sha = try FileHasher.sha256(fileURL: url)
        provider.cachedURLs["ep-probe"] = url
        provider.fingerprints["ep-probe"] = AudioFingerprint(weak: "weak-probe", strong: sha)

        // The scheduler's enqueue is what `DownloadManager` awaits at download
        // completion, BEFORE it announces the completion that requests the
        // kickoff (`DownloadManager.swift`: enqueueAnalysisIfNeeded then
        // notifyBackgroundDownloadCompleted).
        await scheduler.enqueue(
            episodeId: "ep-probe",
            podcastId: "pod-probe",
            downloadId: "dl-probe",
            sourceFingerprint: sha,
            isExplicitDownload: false
        )

        let spy = ReadinessSpy()
        let coordinator = Self.makeProductionShapedCoordinator(
            store: store,
            downloads: provider,
            spy: spy
        )
        await coordinator.requestKickoff(Self.request("ep-probe"))
        await coordinator.drainForTesting()

        let record = try #require(await spy.records.first)
        #expect(record.outcome == .fired)
        #expect(record.pollCount == 1, "the row is already there — no waiting at all")
    }

    @Test("CONTROL: the same probe gives up `no_analysis_asset` when nothing registered a row")
    func dayZeroReadinessProbeStarvesWithoutTheRow() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()

        let url = try writeSynthAudio(seconds: 3.0)
        defer { try? FileManager.default.removeItem(at: url) }
        provider.cachedURLs["ep-starved"] = url

        // Deliberately NO enqueue — this is the shape the pull recorded five
        // times: bytes on disk, no asset row, the wait polling to exhaustion.
        let spy = ReadinessSpy()
        let coordinator = Self.makeProductionShapedCoordinator(
            store: store,
            downloads: provider,
            spy: spy,
            maxAttempts: 3
        )
        await coordinator.requestKickoff(Self.request("ep-starved"))
        await coordinator.drainForTesting()

        let record = try #require(await spy.records.first)
        #expect(record.outcome == .noAnalysisAsset)
        #expect(record.pollCount == 3, "polled to exhaustion, exactly as the device did")
    }

    // MARK: - Rails on the three guards

    @Test("no row is registered when the cached bytes are NOT this job's bytes")
    func noRegistrationWithoutFingerprintProof() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        let onDisk = try writeSynthAudio(seconds: 5.0)
        defer { try? FileManager.default.removeItem(at: onDisk) }
        provider.cachedURLs["ep-stale"] = onDisk

        // A canonical SHA that is NOT the SHA of the file on disk: a feed
        // correction or a re-download landed different audio. Registering here
        // would write a `sourceURL` pointing at audio the fingerprint does not
        // describe — a row that lies about which bytes it is.
        let otherSHA = String(repeating: "a", count: 64)
        await scheduler.enqueue(
            episodeId: "ep-stale",
            podcastId: "pod-stale",
            downloadId: "dl-stale",
            sourceFingerprint: otherSHA,
            isExplicitDownload: false
        )

        let row = try await store.fetchAssetByEpisodeId("ep-stale")
        #expect(row == nil, "unproven bytes must fall through to the lazy path")
    }

    @Test("no row is registered when there is no cached file to be the A-side")
    func noRegistrationWithoutACachedFile() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        // Nothing in `cachedURLs` — an enqueue whose download has not landed.
        await scheduler.enqueue(
            episodeId: "ep-nofile",
            podcastId: "pod-nofile",
            downloadId: "dl-nofile",
            sourceFingerprint: String(repeating: "b", count: 64),
            isExplicitDownload: false
        )

        let row = try await store.fetchAssetByEpisodeId("ep-nofile")
        #expect(row == nil, "no A-side means no honest row to register")
    }

    @Test("no row is registered for a non-canonical source fingerprint")
    func noRegistrationForNonCanonicalFingerprint() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        let url = try writeSynthAudio(seconds: 2.0)
        defer { try? FileManager.default.removeItem(at: url) }
        provider.cachedURLs["ep-weak"] = url

        // A WEAK (URL/ETag/length/date) fingerprint — the play-time enqueue
        // shape. `resolveAnalysisAssetId`'s identity arms all key off the
        // canonical full-file SHA, so registering under a weak identity would
        // mint a row that the later dispatch cannot recognise as the same audio.
        await scheduler.enqueue(
            episodeId: "ep-weak",
            podcastId: "pod-weak",
            downloadId: "dl-weak",
            sourceFingerprint: "https://example.com/a.mp3|etag|123|Tue, 19 May 2026 00:00:00 GMT",
            isExplicitDownload: false
        )

        let row = try await store.fetchAssetByEpisodeId("ep-weak")
        #expect(row == nil, "a weak identity is not the identity this registration can prove")
    }

    @Test("an episode that ALREADY has a row is never given a second one at enqueue")
    func existingRowIsNeverDuplicated() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        let url = try writeSynthAudio(seconds: 7.25)
        defer { try? FileManager.default.removeItem(at: url) }
        let sha = try FileHasher.sha256(fileURL: url)
        let weak = "https://example.com/dup.mp3|etag-dup|12345|Tue, 19 May 2026 00:00:00 GMT"
        provider.cachedURLs["ep-dup"] = url

        // The playhead-0hi9 shape: Pipeline A minted a WEAK-identity row at
        // play time; the download then completes with the canonical SHA. The
        // fingerprint PROOF is deliberately not available yet, so the upgrade
        // cannot be decided at enqueue — and a registration that ran anyway
        // would mint the duplicate row 0hi9 exists to prevent.
        try await store.insertAsset(AnalysisAsset(
            id: "asset-dup-weak",
            episodeId: "ep-dup",
            assetFingerprint: weak,
            weakFingerprint: nil,
            sourceURL: url.absoluteString,
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: nil
        ))

        await scheduler.enqueue(
            episodeId: "ep-dup",
            podcastId: "pod-dup",
            downloadId: "dl-dup",
            sourceFingerprint: sha,
            isExplicitDownload: false
        )

        let rows = try await store.fetchAllAssets().filter { $0.episodeId == "ep-dup" }
        #expect(rows.count == 1, "enqueue must never add a second row for an episode that has one")
        #expect(rows.first?.id == "asset-dup-weak")
    }

    @Test("the job row carries the registered asset id, so dispatch reuses it instead of minting")
    func dispatchReusesTheRegisteredRow() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        let url = try writeSynthAudio(seconds: 5.75)
        defer { try? FileManager.default.removeItem(at: url) }
        let sha = try FileHasher.sha256(fileURL: url)
        provider.cachedURLs["ep-reuse"] = url
        provider.fingerprints["ep-reuse"] = AudioFingerprint(weak: "weak-reuse", strong: sha)

        await scheduler.enqueue(
            episodeId: "ep-reuse",
            podcastId: "pod-reuse",
            downloadId: "dl-reuse",
            sourceFingerprint: sha,
            isExplicitDownload: false
        )

        let registered = try #require(try await store.fetchAssetByEpisodeId("ep-reuse"))
        let jobs = try await store.fetchJobsByState("queued")
            .filter { $0.episodeId == "ep-reuse" }
        let job = try #require(jobs.first)
        #expect(
            job.analysisAssetId == registered.id,
            "the job must already name its asset — otherwise dispatch re-resolves it"
        )

        let processed = await scheduler.processNextDispatchableJobForTesting()
        #expect(processed)

        let rows = try await store.fetchAllAssets().filter { $0.episodeId == "ep-reuse" }
        #expect(rows.count == 1, "dispatch must reuse the registered row, not mint a second")
        #expect(rows.first?.id == registered.id)
    }

    // MARK: - Coordinator harness

    /// The PRODUCTION probe closure, verbatim from `PlayheadRuntime.init` — the
    /// pinned file first, then the asset row, so a give-up names which one was
    /// missing. Copied rather than injected because nothing in the suite builds
    /// a real `PlayheadRuntime`; a probe that drifts from production is the one
    /// way this whole file could be green about the wrong thing.
    private static func makeProductionShapedCoordinator(
        store: AnalysisStore,
        downloads: any DownloadProviding,
        spy: ReadinessSpy,
        maxAttempts: Int = 4
    ) -> RediffDayZeroKickoffCoordinator {
        RediffDayZeroKickoffCoordinator(
            maxAttempts: maxAttempts,
            pollNanos: 1,
            probe: { episodeId in
                guard let playedFileURL = await downloads.cachedFileURL(for: episodeId) else {
                    return .awaitingPinnedFile
                }
                guard let asset = (try? await store.fetchAssetByEpisodeId(episodeId)) ?? nil else {
                    return .awaitingAnalysisAsset
                }
                return .ready(DayZeroKickoffReady(
                    analysisAssetId: asset.id,
                    playedFileURL: playedFileURL
                ))
            },
            fire: { _, _ in },
            claimKickoff: { _ in },
            recordKickoff: { await spy.note($0) },
            reportViolation: { _, _ in },
            episodeIdHasher: { $0 },
            sleep: { _ in },
            now: { 1_000 }
        )
    }

    private static func request(_ episodeId: String) -> RediffDayZeroKickoffRequest {
        RediffDayZeroKickoffRequest(
            episodeId: episodeId,
            enclosureURL: URL(string: "https://example.com/\(episodeId).mp3")!,
            publishedAt: 1_000,
            source: .backgroundDownload,
            enqueuedAt: 1_000
        )
    }

    actor ReadinessSpy {
        private(set) var records: [RediffDayZeroKickoffRecordUpdate] = []
        func note(_ update: RediffDayZeroKickoffRecordUpdate) { records.append(update) }
    }
}
