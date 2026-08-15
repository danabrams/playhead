// FullCoverageTargetTests.swift
// playhead-rh69: A FULL-COVERAGE REQUEST IS RESOLVED AGAINST THE AUDIO, NEVER
// AGAINST THE FEED.
//
// `analysis_jobs.desiredCoverageSec` is the shard filter in `AnalysisJobRunner`
// (`allShards.filter { $0.startTime < request.desiredCoverageSec }`), and that
// one line bounds feature extraction, transcription AND ad detection. Until
// this bead the user-intent path put the publisher's `<itunes:duration>` in it.
//
// Those are two different numbers whenever the file carries dynamically
// inserted ads — which is the product premise. Measured (playhead-by07, 14
// device assets, feeds re-fetched): `jobWant == feedDur` on 14 of 14 to the
// decimal, and the excluded tail is ~5x as ad-dense as the body, five of ten
// answerable tails being 93-101 % advertising by transcribed second. Witnessed
// on Dan's hand-marked episode (playhead-3gzp, asset F4CE7F47): feed 1197.0 s,
// real audio 1316.728125 s, furthest point ANY scan ever reached 1199.82 s, and
// the ad he heard at 1256.64-1316.34 — 56.82 s past it, examined by nothing.
//
// Three kinds of rail here:
//   * `fullCoverageTargetSec` — the pure resolution rule.
//   * the WIRING — that `enqueue` actually consults it. A bound nothing
//     consults passes every unit test of the bound.
//   * the CONSEQUENCE — that the resolved target admits the shard the missed
//     ad lives in, which is the only thing the user experiences.

@preconcurrency import AVFoundation
import Foundation
import Testing

@testable import Playhead

@Suite("playhead-rh69 — the full-coverage target is measured, not declared", .serialized)
struct FullCoverageTargetTests {

    // The F4CE7F47 witness (playhead-3gzp), to the digit.
    private static let f4ceFeedDeclaredSec = 1197.0
    private static let f4ceRealAudioSec = 1316.728125
    private static let f4ceMissedAdStartSec = 1256.64
    private static let f4ceMissedAdEndSec = 1316.34

    // MARK: - The pure rule

    @Test("a measurement of the audio beats the feed's declaration — the F4CE7F47 numbers")
    func measurementWins() {
        #expect(
            AnalysisWorkScheduler.fullCoverageTargetSec(
                measuredAudioDurationSec: Self.f4ceRealAudioSec,
                feedDeclaredDurationSec: Self.f4ceFeedDeclaredSec
            ) == Self.f4ceRealAudioSec
        )
    }

    @Test("the measurement wins even when it is SHORTER than the declaration")
    func measurementWinsWhenShorter() {
        // The anti-`max` rail. A `max(measured, declared)` would keep the feed's
        // number for a truncated download or a trailer swapped in for the
        // episode, and set a rung the audio can never satisfy — the ladder then
        // burns a pass per attempt and terminates `coverageInsufficient`.
        #expect(
            AnalysisWorkScheduler.fullCoverageTargetSec(
                measuredAudioDurationSec: 600.0,
                feedDeclaredDurationSec: Self.f4ceFeedDeclaredSec
            ) == 600.0
        )
    }

    @Test("with no measurement the declaration is the FALLBACK, not a preference")
    func declarationIsTheFallback() {
        #expect(
            AnalysisWorkScheduler.fullCoverageTargetSec(
                measuredAudioDurationSec: nil,
                feedDeclaredDurationSec: Self.f4ceFeedDeclaredSec
            ) == Self.f4ceFeedDeclaredSec
        )
    }

    @Test(
        "an unusable measurement degrades to the declaration",
        arguments: [0.0, -1.0, Double.nan, Double.infinity, 25.0 * 60.0 * 60.0]
    )
    func unusableMeasurementDegrades(measured: Double) {
        #expect(
            AnalysisWorkScheduler.fullCoverageTargetSec(
                measuredAudioDurationSec: measured,
                feedDeclaredDurationSec: Self.f4ceFeedDeclaredSec
            ) == Self.f4ceFeedDeclaredSec
        )
    }

    @Test(
        "an unusable declaration never displaces a good measurement",
        arguments: [0.0, -1.0, Double.nan, Double.infinity, 25.0 * 60.0 * 60.0]
    )
    func unusableDeclarationIsIgnored(declared: Double) {
        #expect(
            AnalysisWorkScheduler.fullCoverageTargetSec(
                measuredAudioDurationSec: Self.f4ceRealAudioSec,
                feedDeclaredDurationSec: declared
            ) == Self.f4ceRealAudioSec
        )
    }

    @Test("neither quantity usable yields nil — the caller falls back to a T0 rung, it does not guess")
    func neitherUsableYieldsNil() {
        #expect(
            AnalysisWorkScheduler.fullCoverageTargetSec(
                measuredAudioDurationSec: nil,
                feedDeclaredDurationSec: nil
            ) == nil
        )
        #expect(
            AnalysisWorkScheduler.fullCoverageTargetSec(
                measuredAudioDurationSec: .nan,
                feedDeclaredDurationSec: 0
            ) == nil
        )
    }

    // MARK: - The consequence: which shards the filter admits

    /// `AnalysisJobRunner` keeps `allShards.filter { $0.startTime < target }`
    /// over 30 s shards, so this is the exact set of audio a pass may read.
    private func admittedShardStarts(
        targetSec: Double,
        audioLengthSec: Double,
        shardSec: Double = AnalysisAudioService.defaultShardDuration
    ) -> [Double] {
        var starts: [Double] = []
        var t = 0.0
        while t < audioLengthSec {
            if t < targetSec { starts.append(t) }
            t += shardSec
        }
        return starts
    }

    @Test("the feed's declaration excludes the shard Dan's missed ad lives in; the measurement admits it")
    func theFilterReachesTheMissedAd() {
        let capped = admittedShardStarts(
            targetSec: Self.f4ceFeedDeclaredSec,
            audioLengthSec: Self.f4ceRealAudioSec
        )
        let uncapped = admittedShardStarts(
            targetSec: Self.f4ceRealAudioSec,
            audioLengthSec: Self.f4ceRealAudioSec
        )

        func covers(_ starts: [Double], _ time: Double) -> Bool {
            starts.contains { $0 <= time && time < $0 + AnalysisAudioService.defaultShardDuration }
        }

        // Under the feed's number the last admitted shard is [1170, 1200) —
        // exactly what the device DB shows — and the ad at 1256.64-1316.34 is
        // in no admitted shard at all.
        #expect(capped.last == 1170.0)
        #expect(!covers(capped, Self.f4ceMissedAdStartSec))
        #expect(!covers(capped, Self.f4ceMissedAdEndSec))

        // Under the measured length every second of the ad is inside an
        // admitted shard.
        #expect(covers(uncapped, Self.f4ceMissedAdStartSec))
        #expect(covers(uncapped, Self.f4ceMissedAdEndSec))
        #expect(uncapped.count - capped.count == 4)
    }

    // MARK: - The wiring: does `enqueue` actually consult the rule?

    private func makeScheduler(
        store: AnalysisStore,
        downloadProvider: StubDownloadProvider,
        config: PreAnalysisConfig = PreAnalysisConfig()
    ) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(),
            downloadManager: downloadProvider,
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider(),
            config: config
        )
    }

    /// Mirrors `AnalysisWorkSchedulerDurationProbeTests.writeSynthAudio` — a
    /// real container `AudioFileDurationProbe` can read.
    private func writeSynthAudio(
        seconds: TimeInterval,
        sampleRate: Double = 44_100
    ) throws -> URL {
        let fileURL = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("rh69-\(UUID().uuidString).caf")
        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: sampleRate,
            channels: 1,
            interleaved: false
        ) else {
            throw NSError(domain: "FullCoverageTargetTests", code: -1)
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
            let frames = min(chunkFrames, AVAudioFrameCount(totalFrames - written))
            guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frames) else {
                throw NSError(domain: "FullCoverageTargetTests", code: -2)
            }
            buffer.frameLength = frames
            try file.write(from: buffer)
            written += AVAudioFramePosition(frames)
        }
        return fileURL
    }

    private func queuedJob(
        for episodeId: String,
        in store: AnalysisStore
    ) async throws -> AnalysisJob? {
        try await store.fetchJobsByState("queued").first { $0.episodeId == episodeId }
    }

    /// Every device asset in `db-pull11` was minted with the audio ALREADY on
    /// disk (the enqueue is fired by download completion), so the measurement
    /// was available at the mint on all eleven.
    @Test("the tap route: the persisted target is the AUDIO's length, not the feed's")
    func tapRoutePersistsTheMeasuredTarget() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        let url = try writeSynthAudio(seconds: 9.0)
        defer { try? FileManager.default.removeItem(at: url) }
        provider.cachedURLs["ep-tap"] = url

        // The feed under-declares, exactly as a DAI feed does.
        await scheduler.enqueueUserIntentAnalysis(
            episodeId: "ep-tap",
            podcastId: "pod",
            sourceFingerprint: "fp-tap",
            feedDeclaredDurationSec: 4.0,
            podcastTitle: "Pod",
            episodeTitle: "Ep"
        )

        let job = try #require(try await queuedJob(for: "ep-tap", in: store))
        #expect(
            abs(job.desiredCoverageSec - 9.0) < 0.5,
            "the target must be the measured ~9 s, not the declared 4 s; got \(job.desiredCoverageSec)"
        )
        #expect(job.desiredCoverageSec > 4.0)
    }

    /// The route that minted ten of the eleven `db-pull11` rows: the user taps
    /// while the audio is still downloading, `markEpisodeUserIntent` stashes the
    /// declaration, and the download's completion fires the enqueue.
    @Test("the download-completion route: the stashed DECLARATION is resolved at the mint")
    func downloadCompletionRouteResolvesTheStash() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        // Tap first — nothing is on disk yet, so nothing can be measured yet.
        await scheduler.markEpisodeUserIntent(
            episodeId: "ep-dl",
            feedDeclaredDurationSec: 4.0
        )

        // …then the download lands and fires the auto-shaped enqueue.
        let url = try writeSynthAudio(seconds: 9.0)
        defer { try? FileManager.default.removeItem(at: url) }
        provider.cachedURLs["ep-dl"] = url

        await scheduler.enqueue(
            episodeId: "ep-dl",
            podcastId: "pod",
            downloadId: "ep-dl",
            sourceFingerprint: "fp-dl",
            isExplicitDownload: false
        )

        let job = try #require(try await queuedJob(for: "ep-dl", in: store))
        #expect(job.priority == 20, "still the user-intent lane")
        #expect(
            abs(job.desiredCoverageSec - 9.0) < 0.5,
            "the stash carries a DECLARATION; the mint must measure. got \(job.desiredCoverageSec)"
        )
    }

    @Test("with no cached audio the target degrades to the declaration rather than guessing")
    func noCachedAudioDegradesToTheDeclaration() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()   // no cached URLs at all
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        await scheduler.enqueueUserIntentAnalysis(
            episodeId: "ep-nofile",
            podcastId: "pod",
            sourceFingerprint: "fp-nofile",
            feedDeclaredDurationSec: 1800,
            podcastTitle: "Pod",
            episodeTitle: "Ep"
        )

        let job = try #require(try await queuedJob(for: "ep-nofile", in: store))
        #expect(job.desiredCoverageSec == 1800)
    }

    // MARK: - Vacuity controls: the tier ladder's own rungs are untouched

    /// This test must SURVIVE. It is what separates "a full-coverage request is
    /// resolved against the audio" from "every job now transcribes the whole
    /// episode in one pass", which is a scheduling-policy change and is Dan's
    /// call, not this bead's.
    @Test("an auto-download with no declaration still starts at the configured T0 rung")
    func autoDownloadStillStartsAtT0() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let config = PreAnalysisConfig()
        let scheduler = makeScheduler(
            store: store,
            downloadProvider: provider,
            config: config
        )

        // A cached file long enough to be measurable — the point is that a
        // measurement present does NOT by itself widen the target.
        let url = try writeSynthAudio(seconds: 9.0)
        defer { try? FileManager.default.removeItem(at: url) }
        provider.cachedURLs["ep-auto"] = url

        await scheduler.enqueue(
            episodeId: "ep-auto",
            podcastId: "pod",
            downloadId: "ep-auto",
            sourceFingerprint: "fp-auto",
            isExplicitDownload: false
        )

        let job = try #require(try await queuedJob(for: "ep-auto", in: store))
        #expect(job.desiredCoverageSec == config.defaultT0DepthSeconds)
        #expect(job.priority == 0)
    }

    @Test("an explicit (non-user-intent) download also keeps its T0 rung")
    func explicitDownloadStillStartsAtT0() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let config = PreAnalysisConfig()
        let scheduler = makeScheduler(
            store: store,
            downloadProvider: provider,
            config: config
        )

        let url = try writeSynthAudio(seconds: 9.0)
        defer { try? FileManager.default.removeItem(at: url) }
        provider.cachedURLs["ep-explicit"] = url

        await scheduler.enqueue(
            episodeId: "ep-explicit",
            podcastId: "pod",
            downloadId: "ep-explicit",
            sourceFingerprint: "fp-explicit",
            isExplicitDownload: true
        )

        let job = try #require(try await queuedJob(for: "ep-explicit", in: store))
        #expect(job.desiredCoverageSec == config.defaultT0DepthSeconds)
        #expect(job.priority == 10)
    }
}
