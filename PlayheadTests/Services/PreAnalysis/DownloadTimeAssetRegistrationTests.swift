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

    private func makeRunner(
        store: AnalysisStore,
        audioProvider: StubAnalysisAudioProvider = StubAnalysisAudioProvider()
    ) -> AnalysisJobRunner {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        return AnalysisJobRunner(
            store: store,
            audioProvider: audioProvider,
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
    }

    /// - Parameter audioProvider: injectable so a test can assert the runner
    ///   NEVER decoded. `decodeCallCount` is the only observation that
    ///   distinguishes "the cancel-before-runner-start arm fired" from "the job
    ///   ran and happened to leave the row alone", which is what keeps
    ///   `cancelBeforeRunnerStartLeavesTheRegisteredRowResting` non-vacuous.
    private func makeScheduler(
        store: AnalysisStore,
        downloadProvider: any DownloadProviding,
        capabilities: any CapabilitiesProviding = StubCapabilitiesProvider(),
        audioProvider: StubAnalysisAudioProvider = StubAnalysisAudioProvider()
    ) -> AnalysisWorkScheduler {
        AnalysisWorkScheduler(
            store: store,
            jobRunner: makeRunner(store: store, audioProvider: audioProvider),
            capabilitiesService: capabilities,
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
        sampleRate: Double = 44_100,
        destination: URL? = nil
    ) throws -> URL {
        let tempDir = URL(fileURLWithPath: NSTemporaryDirectory())
        let fileURL = destination
            ?? tempDir.appendingPathComponent("fzrw-\(UUID().uuidString).caf")

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
        // R2: REGISTERED, not `SessionState.queued` — nothing has queued this
        // episode for analysis. See `registeredRowLeavesTheControlRestingAndTappable`
        // for what the other spelling did to the library row.
        #expect(asset.analysisState == AnalysisAsset.registeredNotQueuedState)
    }

    // MARK: - R2 / F1 — what the row SAYS about itself

    @Test("the registered row leaves the library control RESTING and TAPPABLE")
    func registeredRowLeavesTheControlRestingAndTappable() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        let url = try writeSynthAudio(seconds: 4.5)
        defer { try? FileManager.default.removeItem(at: url) }
        let sha = try FileHasher.sha256(fileURL: url)
        provider.cachedURLs["ep-bar"] = url
        provider.fingerprints["ep-bar"] = AudioFingerprint(weak: "weak-bar", strong: sha)

        await scheduler.enqueue(
            episodeId: "ep-bar",
            podcastId: "pod-bar",
            downloadId: "dl-bar",
            sourceFingerprint: sha,
            isExplicitDownload: false
        )

        let asset = try #require(try await store.fetchAssetByEpisodeId("ep-bar"))

        // THE POPULATION THIS BEAD SERVES: downloaded by the auto/background
        // lane, never tapped, no transfer in flight, no coverage summary yet.
        // Registering the row must not change one pixel of what the library row
        // says — it is a day-0 enabler, not a UI event.
        let analysis = episodePreparationAnalysisInputs(asset: asset, coverage: nil)
        #expect(
            !analysis.analysisActive,
            "a REGISTERED row must not claim the analysis lane is working on this episode"
        )

        let readiness = deriveEpisodePreparationReadiness(EpisodePreparationInputs(
            isDownloaded: true,
            downloadInFlight: false,
            downloadFraction: 1,
            analysisActive: analysis.analysisActive,
            analysisComplete: analysis.analysisComplete,
            analysisTerminatedComplete: analysis.analysisTerminatedComplete,
            analysisFailed: analysis.analysisFailed,
            adScanFraction: analysis.adScanFraction,
            userInitiated: false,
            downloadPermitted: true
        ))

        // `.idle` is the resting ✦ — and it is the ONLY outcome under which
        // `EpisodePreparationStatusModel.isActionable` is true for a downloaded
        // episode, so it is also the statement that the tap still reaches
        // `prepareEpisodeForAnalysis` → `enqueueUserIntentAnalysis` (playhead-kanf's
        // promote-to-`.now` escape hatch). `.analyzing` would disable it for
        // exactly the episodes the lane is hours away from.
        #expect(readiness.state == .idle)
        #expect(readiness.state != .analyzing)
        #expect(
            episodePreparationCaption(readiness) == nil,
            "no caption at all — never a frozen \"Downloaded · analyzing 0%\""
        )
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

    // MARK: - R3 / F3 — the registration token must not outlive the wait

    @Test("once the lane RUNS the episode, the control shows the working bar again")
    func dispatchPromotesTheRegisteredRowOffTheRestingToken() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        let url = try writeSynthAudio(seconds: 5.25)
        defer { try? FileManager.default.removeItem(at: url) }
        let sha = try FileHasher.sha256(fileURL: url)
        provider.cachedURLs["ep-promote"] = url
        provider.fingerprints["ep-promote"] = AudioFingerprint(weak: "weak-promote", strong: sha)

        await scheduler.enqueue(
            episodeId: "ep-promote",
            podcastId: "pod-promote",
            downloadId: "dl-promote",
            sourceFingerprint: sha,
            isExplicitDownload: false
        )

        // WHILE WAITING: resting, tappable — R2's F1 fix, unchanged.
        let waiting = try #require(try await store.fetchAssetByEpisodeId("ep-promote"))
        #expect(waiting.analysisState == AnalysisAsset.registeredNotQueuedState)
        #expect(!episodePreparationAnalysisInputs(asset: waiting, coverage: nil).analysisActive)

        let processed = await scheduler.processNextDispatchableJobForTesting()
        #expect(processed)

        // ONCE THE LANE HAS IT: the registration token is gone.
        //
        // Nothing else could remove it. Five statements write this column (R5 —
        // an earlier draft of this comment named only `AnalysisCoordinator`):
        // the two INSERTs, both already spent on this row; the two
        // `updateAssetState` overloads, reached only from `AnalysisCoordinator`
        // on the PLAY path — no playback here — and from the duplicate-fold
        // sweep, which needs a second row; and `markRegisteredAssetQueued`,
        // which is the one under test. Meanwhile `resolveAnalysisAssetId`
        // returns early on the stamped `job.analysisAssetId` without touching
        // state. So before R3 this row read `new` for the entire analysis and
        // the library drew the resting ✦ with no caption: F1's lie pointing the
        // other way.
        let running = try #require(try await store.fetchAssetByEpisodeId("ep-promote"))
        #expect(
            running.analysisState != AnalysisAsset.registeredNotQueuedState,
            "a row the lane has taken in hand must not still read as merely registered"
        )

        let analysis = episodePreparationAnalysisInputs(asset: running, coverage: nil)
        #expect(
            analysis.analysisActive,
            "the lane is working on this episode; the control must say so"
        )
        let readiness = deriveEpisodePreparationReadiness(EpisodePreparationInputs(
            isDownloaded: true,
            downloadInFlight: false,
            downloadFraction: 1,
            analysisActive: analysis.analysisActive,
            analysisComplete: analysis.analysisComplete,
            analysisTerminatedComplete: analysis.analysisTerminatedComplete,
            analysisFailed: analysis.analysisFailed,
            adScanFraction: analysis.adScanFraction,
            userInitiated: false,
            downloadPermitted: true
        ))
        #expect(readiness.state == .analyzing)
        #expect(episodePreparationCaption(readiness) != nil)
    }

    // MARK: - R5 / F4 — the promote's PLACEMENT, not just its existence

    @Test("a job cancelled BEFORE its runner starts leaves the registered row resting and TAPPABLE")
    func cancelBeforeRunnerStartLeavesTheRegisteredRowResting() async throws {
        // WHY THIS TEST EXISTS. R3 put `markRegisteredAssetQueued` AFTER
        // `processJob`'s cancel guard and said in a comment that the placement
        // was the point. R4 moved it three lines up, above the guard, and all
        // 275 tests in this population still passed — so the sentence was
        // load-bearing and unwitnessed. Above the guard, a job cancelled before
        // its runner ever started promotes the row to `queued` and then
        // returns: the library row lights a "Downloaded · analyzing 0%" bar for
        // an episode with nothing running, and because `.analyzing` is not
        // actionable it DISABLES the tap — playhead-kanf's promote-to-`.now`
        // escape hatch, removed from an episode that just lost its slot. The
        // bar never moves, because the next dispatch's promote is conditional
        // on a token this row no longer carries.
        //
        // The arm is otherwise undrivable: the guard and the runner sit
        // back-to-back inside one actor message, which is why
        // `AnalysisWorkSchedulerJournalEmissionTests` declares
        // `cancelRace.releaseLease` unreachable in stub form. The DEBUG-only
        // `cancelBeforeRunnerStart` seam raises the cancel the way a real
        // canceller would, at the last instant before the guard reads it.
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let audio = StubAnalysisAudioProvider()
        let scheduler = makeScheduler(
            store: store,
            downloadProvider: provider,
            audioProvider: audio
        )

        let url = try writeSynthAudio(seconds: 5.0)
        defer { try? FileManager.default.removeItem(at: url) }
        let sha = try FileHasher.sha256(fileURL: url)
        provider.cachedURLs["ep-cancelrace"] = url
        provider.fingerprints["ep-cancelrace"] = AudioFingerprint(weak: "weak-cancelrace", strong: sha)

        await scheduler.enqueue(
            episodeId: "ep-cancelrace",
            podcastId: "pod-cancelrace",
            downloadId: "dl-cancelrace",
            sourceFingerprint: sha,
            isExplicitDownload: false
        )

        let registered = try #require(try await store.fetchAssetByEpisodeId("ep-cancelrace"))
        #expect(registered.analysisState == AnalysisAsset.registeredNotQueuedState)

        let processed = await scheduler.processNextDispatchableJobForTesting(
            cancelBeforeRunnerStart: .userCancelled
        )
        #expect(processed, "the pass must have dispatched — otherwise nothing was cancelled")

        // ANTI-VACUITY. Without these, a run that simply never reached the
        // guard would satisfy every assertion below for the wrong reason.
        #expect(
            audio.decodeCallCount == 0,
            "the runner must never have started; this is the cancel-BEFORE-runner arm"
        )
        let job = try #require(
            try await store.fetchJobsByState("queued").first { $0.episodeId == "ep-cancelrace" },
            "the arm reverts 'running' to 'queued' — a job stuck at 'running' with no lease is invisible to every recovery path"
        )
        #expect(job.leaseOwner == nil, "the arm releases the lease")
        #expect(
            job.attemptCount == 0,
            "cancel-before-runner-start deliberately spends no attempt — no decode work was performed"
        )

        // THE PROPERTY. The lane gave the episode back; the row must say so.
        let after = try #require(try await store.fetchAssetByEpisodeId("ep-cancelrace"))
        #expect(
            after.analysisState == AnalysisAsset.registeredNotQueuedState,
            "a job that never started its runner must leave the registration token in place"
        )

        let analysis = episodePreparationAnalysisInputs(asset: after, coverage: nil)
        #expect(
            !analysis.analysisActive,
            "nothing is running; the control must not claim the lane is working on this episode"
        )
        let readiness = deriveEpisodePreparationReadiness(EpisodePreparationInputs(
            isDownloaded: true,
            downloadInFlight: false,
            downloadFraction: 1,
            analysisActive: analysis.analysisActive,
            analysisComplete: analysis.analysisComplete,
            analysisTerminatedComplete: analysis.analysisTerminatedComplete,
            analysisFailed: analysis.analysisFailed,
            adScanFraction: analysis.adScanFraction,
            userInitiated: false,
            downloadPermitted: true
        ))
        // `.idle` is the resting ✦ AND the only state under which
        // `EpisodePreparationStatusModel.isActionable` is true for a downloaded
        // episode — i.e. the statement that the tap still reaches
        // `enqueueUserIntentAnalysis`. A cancelled episode is precisely the one
        // that needs it.
        #expect(readiness.state == .idle)
        #expect(readiness.state != .analyzing)
        #expect(
            episodePreparationCaption(readiness) == nil,
            "no caption at all — never a frozen \"Downloaded · analyzing 0%\" on an episode with nothing running"
        )
    }

    @Test("the promotion is CONDITIONAL — it cannot overwrite a state the coordinator wrote")
    func promotionNeverClobbersACoordinatorWrittenState() async throws {
        let store = try await makeTestStore()

        // The registration token: promoted, exactly once.
        let registered = AnalysisAsset(
            id: "asset-registered",
            episodeId: "ep-registered",
            assetFingerprint: String(repeating: "a", count: 64),
            weakFingerprint: "weak-registered",
            sourceURL: "Library/Caches/audio/ep-registered.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: AnalysisAsset.registeredNotQueuedState,
            analysisVersion: PreAnalysisConfig.analysisVersion,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(registered)
        #expect(try await store.markRegisteredAssetQueued(id: "asset-registered"))
        #expect(
            try await store.fetchAsset(id: "asset-registered")?.analysisState
                == SessionState.queued.rawValue
        )
        #expect(
            try await store.markRegisteredAssetQueued(id: "asset-registered") == false,
            "the second call has nothing to promote — the predicate no longer matches"
        )

        // A TERMINAL the coordinator wrote. An unconditional UPDATE here would
        // rewind a finished episode to `queued` and relight the working bar on
        // an episode that is done — which is why the predicate is in the SQL
        // rather than in a caller's `if`.
        let finished = AnalysisAsset(
            id: "asset-finished",
            episodeId: "ep-finished",
            assetFingerprint: String(repeating: "b", count: 64),
            weakFingerprint: "weak-finished",
            sourceURL: "Library/Caches/audio/ep-finished.mp3",
            featureCoverageEndTime: 100,
            fastTranscriptCoverageEndTime: 100,
            confirmedAdCoverageEndTime: 100,
            analysisState: SessionState.completeFull.rawValue,
            analysisVersion: PreAnalysisConfig.analysisVersion,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(finished)
        #expect(try await store.markRegisteredAssetQueued(id: "asset-finished") == false)
        #expect(
            try await store.fetchAsset(id: "asset-finished")?.analysisState
                == SessionState.completeFull.rawValue,
            "a completion terminal must survive the promotion untouched"
        )
    }

    // MARK: - R2 / F2 — the check-then-insert window

    @Test("a placeholder minted DURING the registration cannot produce a second row")
    func concurrentPlaceholderCannotProduceASecondRow() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let racer = PlaceholderRacer(
            store: store,
            episodeId: "ep-race",
            placeholderId: "asset-race-placeholder"
        )
        let scheduler = makeScheduler(
            store: store,
            downloadProvider: provider,
            capabilities: racer
        )

        let url = try writeSynthAudio(seconds: 3.5)
        defer { try? FileManager.default.removeItem(at: url) }
        let sha = try FileHasher.sha256(fileURL: url)
        provider.cachedURLs["ep-race"] = url
        provider.fingerprints["ep-race"] = AudioFingerprint(weak: "weak-race", strong: sha)

        await scheduler.enqueue(
            episodeId: "ep-race",
            podcastId: "pod-race",
            downloadId: "dl-race",
            sourceFingerprint: sha,
            isExplicitDownload: false
        )

        // ANTI-VACUITY: the seam must actually have fired INSIDE the window. If
        // the registration ever stops reaching `currentSnapshot` — or bails at
        // an earlier guard — every assertion below is trivially satisfiable.
        #expect(racer.didPlantPlaceholder, "the competing writer never ran; the race was not exercised")

        let rows = try await store.fetchAllAssets().filter { $0.episodeId == "ep-race" }
        // Two rows here is playhead-0hi9 returning, and the V39 uniqueness index
        // cannot stop it: it is on `(episodeId, assetFingerprint)`, and the
        // placeholder's fingerprint is its own UUID, so the pair differs and
        // BOTH rows survive. The one-shot repair sweep
        // (`did_duplicate_asset_reconcile_v1`) is already spent on shipped devices.
        #expect(rows.count == 1, "the registration must lose the race, not duplicate the episode")
        #expect(rows.first?.id == "asset-race-placeholder")

        // And the job must be left UNSTAMPED. `resolveAnalysisAssetId` returns on
        // `job.analysisAssetId` before any identity work, so a job stamped with a
        // row that lost would permanently skip the weak-upgrade arm that folds
        // the placeholder onto the canonical SHA.
        let jobs = try await store.fetchJobsByState("queued").filter { $0.episodeId == "ep-race" }
        let job = try #require(jobs.first)
        #expect(
            job.analysisAssetId == nil,
            "a lost registration must not stamp the job with a row it did not write"
        )
    }

    /// A `CapabilitiesProviding` that mints a COMPETING `analysis_assets` row
    /// from inside `currentSnapshot` — the FIRST suspension point between the
    /// registration's "does this episode have a row?" check
    /// (`AnalysisWorkScheduler.swift`, guard 3) and its insert.
    ///
    /// WHY THIS SEAM AND NOT `DownloadProviding.fingerprint(for:)`, which is the
    /// window's other suspension point. `enqueue` already calls
    /// `downloadManager.fingerprint(for:)` twice BEFORE the registration runs
    /// (`AnalysisWorkScheduler.swift:1379`, `:1413`), so a racer hung on that
    /// method lands its placeholder before guard 3 rather than after it — which
    /// exercises the guard, not the window, and passes whether or not the insert
    /// is atomic. `capabilitiesService.currentSnapshot` is reached exactly once
    /// on this path and only from inside the registration.
    ///
    /// The row it writes is `AnalysisCoordinator.resolveSession`'s placeholder
    /// verbatim: `assetFingerprint` is the row's OWN UUID, which is what makes
    /// it invisible to the V39 `(episodeId, assetFingerprint)` uniqueness index.
    /// Production reaches this interleaving whenever a download completes while
    /// the user presses play, which is routine.
    final class PlaceholderRacer: CapabilitiesProviding, @unchecked Sendable {
        private let inner = StubCapabilitiesProvider()
        private let store: AnalysisStore
        private let episodeId: String
        private let placeholderId: String
        private let lock = NSLock()
        private var didRace = false

        init(store: AnalysisStore, episodeId: String, placeholderId: String) {
            self.store = store
            self.episodeId = episodeId
            self.placeholderId = placeholderId
        }

        /// Whether the placeholder was actually planted. Asserted by the test so
        /// a seam that stops being reached fails LOUDLY instead of turning the
        /// race assertions vacuous.
        var didPlantPlaceholder: Bool {
            lock.lock()
            defer { lock.unlock() }
            return didRace
        }

        var currentSnapshot: CapabilitySnapshot {
            get async {
                let shouldRace: Bool = {
                    lock.lock()
                    defer { lock.unlock() }
                    guard !didRace else { return false }
                    didRace = true
                    return true
                }()
                if shouldRace {
                    try? await store.insertAsset(AnalysisAsset(
                        id: placeholderId,
                        episodeId: episodeId,
                        assetFingerprint: placeholderId,
                        weakFingerprint: nil,
                        sourceURL: "complete/\(placeholderId).mp3",
                        featureCoverageEndTime: nil,
                        fastTranscriptCoverageEndTime: nil,
                        confirmedAdCoverageEndTime: nil,
                        analysisState: SessionState.queued.rawValue,
                        analysisVersion: 1,
                        capabilitySnapshot: nil
                    ))
                }
                return inner.currentSnapshot
            }
        }

        func capabilityUpdates() async -> AsyncStream<CapabilitySnapshot> {
            await inner.capabilityUpdates()
        }
    }

    // MARK: - R2 / M7 — playhead-b8hj container portability at the new site

    @Test("the registered row's sourceURL is CONTAINER-PORTABLE, not an absolute path")
    func registeredRowSourceURLIsContainerPortable() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        // The fixture MUST live under the audio-cache root. `portableString`
        // stores anything outside the cache verbatim, so for a file in
        // `NSTemporaryDirectory()` — which is where every other fixture in this
        // suite lives — it and `absoluteString` return the identical bytes and
        // the b8hj property is untestable. That is why the pre-existing A-side
        // test could not see `portableString` -> `absoluteString` (mutant M7).
        let cacheRoot = DownloadManager.defaultCacheDirectory()
        let completeDir = cacheRoot.appendingPathComponent("complete", isDirectory: true)
        try FileManager.default.createDirectory(at: completeDir, withIntermediateDirectories: true)
        let name = "fzrw-portable-\(UUID().uuidString).caf"
        let url = try writeSynthAudio(
            seconds: 2.0,
            destination: completeDir.appendingPathComponent(name)
        )
        defer { try? FileManager.default.removeItem(at: url) }

        let sha = try FileHasher.sha256(fileURL: url)
        provider.cachedURLs["ep-portable"] = url
        provider.fingerprints["ep-portable"] = AudioFingerprint(weak: "weak-portable", strong: sha)

        await scheduler.enqueue(
            episodeId: "ep-portable",
            podcastId: "pod-portable",
            downloadId: "dl-portable",
            sourceFingerprint: sha,
            isExplicitDownload: false
        )

        let asset = try #require(try await store.fetchAssetByEpisodeId("ep-portable"))

        // (a) The stored FORM. `sourceURL` is write-once, so a path carrying the
        // Data-container UUID is permanently dead once iOS re-creates the
        // container on reinstall or restore (playhead-b8hj measured 12 distinct
        // container UUIDs across 36 rows in 9 days).
        #expect(asset.sourceURL == "complete/\(name)")
        #expect(!asset.sourceURL.hasPrefix("/"), "an absolute path dies with the container")
        #expect(!asset.sourceURL.contains("://"), "not a file:// URL either")

        // (b) What the form BUYS, stated operationally: the same string resolves
        // against a DIFFERENT container root. An `absoluteString` row resolves
        // against step 1 (a usable absolute path is taken as-is) and lands back
        // in the old container instead.
        let otherRoot = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("fzrw-other-container-\(UUID().uuidString)", isDirectory: true)
        let otherComplete = otherRoot.appendingPathComponent("complete", isDirectory: true)
        try FileManager.default.createDirectory(at: otherComplete, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: otherRoot) }
        try FileManager.default.copyItem(at: url, to: otherComplete.appendingPathComponent(name))

        let resolved = try #require(
            AudioCacheLocation.resolve(asset.sourceURL, cacheRoot: otherRoot) { candidate in
                (try? candidate.resourceValues(forKeys: [.isRegularFileKey]).isRegularFile) == true
            },
            "the stored reference must re-root onto the current container"
        )
        #expect(
            resolved.path.hasPrefix(otherRoot.path),
            "resolution must land in the container it was asked about, not the one that wrote the row"
        )
    }

    // MARK: - playhead-1216 — the identity minted twice, and the ✦ that follows

    /// A `DownloadProviding` that mints the REGISTRATION's row — same episode,
    /// same canonical SHA — from inside `fingerprint(for:)`.
    ///
    /// WHY THAT SEAM AND NOT `capabilitiesService.currentSnapshot`, which is the
    /// window's other suspension point and the one ``PlaceholderRacer`` uses.
    /// On the DISPATCH path `currentSnapshot` is reached twice before
    /// `processJob` gets anywhere near asset resolution
    /// (`currentLaneAdmission` and `evaluateAdmissionGate`), so a racer hung on
    /// it plants the row BEFORE `resolveAnalysisAssetId`'s exact-identity read
    /// — which then finds it and returns early. That exercises the early-return
    /// arm, not the window, and passes whether or not the insert is atomic.
    /// `downloadManager.fingerprint(for:)` is first reached at
    /// `AnalysisWorkScheduler.swift:6794`, i.e. AFTER the exact-identity read at
    /// `:6779` and BEFORE the fall-through insert.
    ///
    /// This models `playhead-fzrw`'s `registerDownloadedAssetRowIfAbsent`
    /// landing in the lane's own window, which is not hypothetical: `enqueue`
    /// and the run loop are messages on ONE actor, `insertJob` makes the job
    /// dispatchable, and the registration then suspends three times before it
    /// stamps the job. On the 2026-08-13 device pull the lane won that race on
    /// FOUR of the FIVE jobs enqueued that day.
    ///
    /// Unlike ``PlaceholderRacer``, the row planted here shares the job's
    /// `(episodeId, assetFingerprint)` pair exactly — so the V39 uniqueness
    /// index DOES see it, and the fall-through insert is a guaranteed
    /// `UNIQUE constraint failed`, not a second surviving row.
    final class RegistrationRacer: DownloadProviding, @unchecked Sendable {
        private let inner: StubDownloadProvider
        private let store: AnalysisStore
        private let episodeId: String
        private let assetId: String
        private let canonicalFingerprint: String
        private let lock = NSLock()
        private var didRace = false

        init(
            inner: StubDownloadProvider,
            store: AnalysisStore,
            episodeId: String,
            assetId: String,
            canonicalFingerprint: String
        ) {
            self.inner = inner
            self.store = store
            self.episodeId = episodeId
            self.assetId = assetId
            self.canonicalFingerprint = canonicalFingerprint
        }

        /// Whether the competing registration was actually planted. Asserted by
        /// the test so a seam that stops being reached fails LOUDLY rather than
        /// turning every assertion below into a tautology.
        var didPlantRegistration: Bool {
            lock.lock()
            defer { lock.unlock() }
            return didRace
        }

        func cachedFileURL(for episodeId: String) async -> URL? {
            await inner.cachedFileURL(for: episodeId)
        }

        func allCachedEpisodeIds() async -> Set<String> {
            await inner.allCachedEpisodeIds()
        }

        func fingerprint(for episodeId: String) async -> AudioFingerprint? {
            let shouldRace: Bool = {
                lock.lock()
                defer { lock.unlock() }
                guard !didRace, episodeId == self.episodeId else { return false }
                didRace = true
                return true
            }()
            if shouldRace {
                _ = try? await store.insertAssetIfEpisodeHasNone(AnalysisAsset(
                    id: assetId,
                    episodeId: self.episodeId,
                    assetFingerprint: canonicalFingerprint,
                    weakFingerprint: "weak-1216",
                    sourceURL: "complete/\(assetId).caf",
                    featureCoverageEndTime: nil,
                    fastTranscriptCoverageEndTime: nil,
                    confirmedAdCoverageEndTime: nil,
                    analysisState: AnalysisAsset.registeredNotQueuedState,
                    analysisVersion: PreAnalysisConfig.analysisVersion,
                    capabilitySnapshot: nil
                ))
            }
            return await inner.fingerprint(for: episodeId)
        }
    }

    @Test("a registration that lands mid-resolve is ADOPTED, so the library keeps saying downloaded")
    func laneAdoptsARegistrationThatLandedInsideItsOwnWindow() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()

        let url = try writeSynthAudio(seconds: 4.0)
        defer { try? FileManager.default.removeItem(at: url) }
        let sha = try FileHasher.sha256(fileURL: url)
        provider.cachedURLs["ep-1216"] = url
        provider.fingerprints["ep-1216"] = AudioFingerprint(weak: "weak-1216", strong: sha)

        let racer = RegistrationRacer(
            inner: provider,
            store: store,
            episodeId: "ep-1216",
            assetId: "asset-1216-registered",
            canonicalFingerprint: sha
        )
        let scheduler = makeScheduler(store: store, downloadProvider: racer)

        // The job the lane picked up BEFORE the registration stamped it: a real
        // `preAnalysis` row with no `analysisAssetId`, which is exactly what
        // `registerDownloadedAssetRowIfAbsent` leaves behind for the window
        // between `insertJob` and `updateJobAnalysisAssetId`.
        try await store.insertJob(makeAnalysisJob(
            jobId: "job-1216",
            jobType: "preAnalysis",
            episodeId: "ep-1216",
            podcastId: "pod-1216",
            analysisAssetId: nil,
            sourceFingerprint: sha,
            downloadId: "dl-1216",
            priority: 20,
            desiredCoverageSec: 60
        ))

        let processed = await scheduler.processNextDispatchableJobForTesting()
        #expect(processed, "the job must actually dispatch, or nothing below is exercised")

        // ANTI-VACUITY: the competing writer has to have run INSIDE the window.
        #expect(
            racer.didPlantRegistration,
            "the competing registration never ran; the collision was not exercised"
        )

        // (1) The identity is minted ONCE. Before this fix the lane's
        //     fall-through insert threw
        //     `UNIQUE constraint failed: analysis_assets.episodeId,
        //     analysis_assets.assetFingerprint`.
        let rows = try await store.fetchAllAssets().filter { $0.episodeId == "ep-1216" }
        #expect(rows.count == 1)
        #expect(
            rows.first?.id == "asset-1216-registered",
            "the incumbent owns the identity; the lane must adopt it, not mint beside it"
        )

        // (2) Asset resolution SUCCEEDED. The throw used to land in
        //     `processJob`'s asset-resolution catch, which stamps
        //     `lastErrorCode` with the `assetResolution:` prefix, spends an
        //     attempt and requeues with a backoff — on the device, four of the
        //     five jobs enqueued on 2026-08-13, every one of them carrying
        //     `assetResolution: Insert failed: UNIQUE constraint failed`.
        //
        //     The assertions here are on the RESOLUTION arm, not on the job's
        //     terminal state: what the stub runner then makes of the episode is
        //     this suite's existing (deliberate) silence — `dispatchReusesTheRegisteredRow`
        //     and `dispatchPromotesTheRegisteredRowOffTheRestingToken` assert
        //     nothing about it either, because a synthetic 4-second CAF through
        //     a stub recogniser has no meaningful analysis outcome. The
        //     `assetResolution:` prefix is written by exactly one arm and is
        //     therefore the precise signature of the defect.
        let job = try #require(try await store.fetchJob(byId: "job-1216"))
        #expect(
            job.lastErrorCode?.contains("assetResolution") != true,
            "asset resolution must not fail when the incumbent IS this job's asset"
        )
        #expect(
            job.analysisAssetId == "asset-1216-registered",
            "the job must be stamped with the row that actually exists — the throw happened BEFORE any stamping, so an unstamped job is the defect's other fingerprint"
        )

        // (3) THE USER-VISIBLE PROPERTY, and the reason this is a P0. A failed
        //     resolution leaves the row on the registration token, whose whole
        //     purpose is that it does NOT light the working bar — so the library
        //     control falls back to the resting ✦, the identical glyph an
        //     episode with no audio at all shows. Dan read that as "not
        //     downloaded" and tapped download again.
        let asset = try #require(rows.first)
        #expect(
            asset.analysisState != AnalysisAsset.registeredNotQueuedState,
            "the lane has this episode in hand; the row must say so"
        )
        let analysis = episodePreparationAnalysisInputs(asset: asset, coverage: nil)
        #expect(analysis.analysisActive)
        let readiness = deriveEpisodePreparationReadiness(EpisodePreparationInputs(
            isDownloaded: true,
            downloadInFlight: false,
            downloadFraction: 1,
            analysisActive: analysis.analysisActive,
            analysisComplete: analysis.analysisComplete,
            analysisTerminatedComplete: analysis.analysisTerminatedComplete,
            analysisFailed: analysis.analysisFailed,
            adScanFraction: analysis.adScanFraction,
            userInitiated: false,
            downloadPermitted: true
        ))
        #expect(
            readiness.state == .analyzing,
            "a downloaded episode the lane is working on must show the working bar, not ✦"
        )
        #expect(
            readiness.downloadFraction == 1,
            "the download zone is what tells the user the audio is already on the device"
        )
    }

    @Test("the store adopts an incumbent identity instead of throwing, and never duplicates it")
    func insertAssetAdoptingIdentityIsIdempotentOnTheIndexedPair() async throws {
        let store = try await makeTestStore()
        let sha = String(repeating: "c", count: 64)

        func asset(id: String, title: String?) -> AnalysisAsset {
            AnalysisAsset(
                id: id,
                episodeId: "ep-adopt",
                assetFingerprint: sha,
                weakFingerprint: "weak-adopt",
                sourceURL: "complete/\(id).mp3",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: AnalysisAsset.registeredNotQueuedState,
                analysisVersion: PreAnalysisConfig.analysisVersion,
                capabilitySnapshot: nil,
                episodeTitle: title
            )
        }

        let first = try await store.insertAssetAdoptingIdentity(asset(id: "asset-a", title: "A"))
        #expect(first.didInsert)
        #expect(first.assetId == "asset-a")

        // The SECOND caller holds a different `id` for the same identity — which
        // is the whole shape of the defect: two writers each minted a UUID for
        // one `(episodeId, assetFingerprint)`.
        let second = try await store.insertAssetAdoptingIdentity(asset(id: "asset-b", title: "B"))
        #expect(!second.didInsert, "the conflict clause must swallow the write")
        #expect(
            second.assetId == "asset-a",
            "the caller stamps this onto analysis_jobs — it must name the row that EXISTS"
        )

        let rows = try await store.fetchAllAssets().filter { $0.episodeId == "ep-adopt" }
        #expect(rows.count == 1)
        #expect(rows.first?.episodeTitle == "A", "adoption must not overwrite the incumbent")

        // A DIFFERENT fingerprint for the same episode is a different identity
        // and still inserts — the conflict clause names one index and nothing
        // else, so a genuine re-stitch is untouched.
        let other = try await store.insertAssetAdoptingIdentity(AnalysisAsset(
            id: "asset-c",
            episodeId: "ep-adopt",
            assetFingerprint: String(repeating: "d", count: 64),
            weakFingerprint: "weak-adopt",
            sourceURL: "complete/asset-c.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: AnalysisAsset.registeredNotQueuedState,
            analysisVersion: PreAnalysisConfig.analysisVersion,
            capabilitySnapshot: nil
        ))
        #expect(other.didInsert)
        #expect(other.assetId == "asset-c")
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
