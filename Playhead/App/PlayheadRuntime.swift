// PlayheadRuntime.swift
// Shared app-level composition root for long-lived services.

import Foundation
import OSLog

@MainActor
@Observable
final class PlayheadRuntime {

    let playbackService: PlaybackService
    let capabilitiesService: CapabilitiesService
    let analysisStore: AnalysisStore
    let modelInventory: ModelInventory
    let assetProvider: AssetProvider
    let entitlementManager: EntitlementManager
    let audioService: AnalysisAudioService
    let featureService: FeatureExtractionService
    let transcriptEngine: TranscriptEngineService
    let trustService: TrustScoringService
    let adDetectionService: AdDetectionService
    let skipOrchestrator: SkipOrchestrator
    let analysisCoordinator: AnalysisCoordinator
    let backgroundProcessingService: BackgroundProcessingService
    let downloadManager: DownloadManager

    private let isPreviewRuntime: Bool
    private let logger = Logger(subsystem: "com.playhead", category: "Runtime")

    private(set) var currentEpisodeId: String?
    private(set) var currentPodcastId: String?
    private(set) var currentAnalysisAssetId: String?
    private(set) var currentEpisodeTitle: String?
    private(set) var currentPodcastTitle: String?
    private(set) var currentArtworkURL: URL?

    /// Background download task for the current episode's audio cache.
    private var audioCacheTask: Task<Void, Never>?

    /// True when an episode is actively loaded for playback.
    var isPlayingEpisode: Bool {
        currentEpisodeId != nil
    }

    /// Error state flag for catastrophic initialization failures.
    var initializationError: String?

    init(isPreviewRuntime: Bool = false) {
        self.isPreviewRuntime = isPreviewRuntime
        self.playbackService = PlaybackService()
        self.capabilitiesService = CapabilitiesService()

        // AnalysisStore: attempt creation with graceful recovery.
        // 1. Normal open  2. Delete + retry  3. Temp directory (ephemeral)
        var resolvedStore: AnalysisStore
        var storeError: String?
        if let store = try? AnalysisStore() {
            resolvedStore = store
        } else {
            // Delete corrupted store and retry
            try? FileManager.default.removeItem(at: AnalysisStore.defaultDirectory())
            if let store = try? AnalysisStore() {
                resolvedStore = store
            } else {
                // Fallback to temp directory — analysis won't persist across launches
                let tmpDir = FileManager.default.temporaryDirectory
                    .appendingPathComponent("PlayheadAnalysis-\(ProcessInfo.processInfo.globallyUniqueString)", isDirectory: true)
                // If even temp directory fails, the device is in severe trouble.
                // Use force-try as last resort — the app cannot function without any store.
                resolvedStore = try! AnalysisStore(directory: tmpDir)
                storeError = "Analysis database could not be opened. Ad detection may not persist across launches."
            }
        }
        self.analysisStore = resolvedStore

        let manifest: ModelManifest
        do {
            manifest = try ModelInventory.loadBundledManifest()
        } catch {
            manifest = ModelManifest(version: 1, generatedAt: .now, models: [])
        }
        self.modelInventory = ModelInventory(manifest: manifest)
        self.assetProvider = AssetProvider(inventory: modelInventory)
        self.entitlementManager = EntitlementManager()

        self.audioService = AnalysisAudioService()

        let speechService = SpeechService()

        self.featureService = FeatureExtractionService(store: analysisStore)
        self.transcriptEngine = TranscriptEngineService(
            speechService: speechService,
            store: analysisStore
        )
        self.trustService = TrustScoringService(store: analysisStore)
        self.adDetectionService = AdDetectionService(
            store: analysisStore,
            metadataExtractor: FallbackExtractor()
        )
        self.skipOrchestrator = SkipOrchestrator(
            store: analysisStore,
            trustService: trustService
        )
        self.analysisCoordinator = AnalysisCoordinator(
            store: analysisStore,
            audioService: audioService,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            capabilitiesService: capabilitiesService,
            adDetectionService: adDetectionService,
            skipOrchestrator: skipOrchestrator
        )
        self.backgroundProcessingService = BackgroundProcessingService(
            coordinator: analysisCoordinator,
            capabilitiesService: capabilitiesService
        )
        self.downloadManager = DownloadManager()

        // Set error state after all stored properties are initialized.
        if let storeError {
            self.initializationError = storeError
        }

        Task { await capabilitiesService.startObserving() }

        Task { [playbackService, analysisCoordinator, skipOrchestrator] in
            await skipOrchestrator.setSkipCueHandler { cues in
                Task { @PlaybackServiceActor in
                    playbackService.setSkipCues(cues)
                }
            }

            var lastStatus: PlaybackState.Status = .idle
            var lastSpeed: Float = 1.0

            let stateStream = await playbackService.observeStates()
            for await state in stateStream {
                await skipOrchestrator.updatePlayheadTime(state.currentTime)

                if state.playbackSpeed != lastSpeed {
                    lastSpeed = state.playbackSpeed
                    await analysisCoordinator.handlePlaybackEvent(
                        .speedChanged(rate: state.playbackSpeed, time: state.currentTime)
                    )
                }

                switch state.status {
                case .playing:
                    let rate = state.rate > 0 ? state.rate : state.playbackSpeed
                    await analysisCoordinator.handlePlaybackEvent(
                        .timeUpdate(time: state.currentTime, rate: rate)
                    )

                case .paused:
                    if lastStatus != .paused {
                        await analysisCoordinator.handlePlaybackEvent(.paused(time: state.currentTime))
                    }

                default:
                    break
                }

                lastStatus = state.status
            }
        }

        guard !isPreviewRuntime else { return }

        backgroundProcessingService.registerBackgroundTasks()

        Task { [downloadManager] in
            do {
                try await analysisStore.migrate()
            } catch {
                // The app can still launch, but analysis persistence will be degraded.
            }

            do {
                try await downloadManager.bootstrap()
            } catch {
                // Non-fatal: downloads will fail but playback still works.
            }

            do {
                try await modelInventory.scan()
            } catch {
                // Settings can still render, but model lifecycle reporting will be empty.
            }

            // Request speech recognition authorization early so the prompt
            // appears at launch, not mid-playback. No microphone access needed.
            let speechAuthorized = await AppleSpeechRecognizer.ensureAuthorized()
            if !speechAuthorized {
                Logger(subsystem: "com.playhead", category: "Runtime")
                    .warning("Speech recognition not authorized — transcription unavailable")
            }

            do {
                try await speechService.loadFastModel(from: modelInventory.activeDirectory)
            } catch {
                // Speech asset preparation is best-effort at launch; the
                // transcript engine will surface failures when first used.
            }

            await backgroundProcessingService.start()
            await entitlementManager.start()
            await capabilitiesService.runSelfTest()
        }
    }

    /// Capture and expose the current playback position for persistence.
    /// Called from scene phase handling to save state on backgrounding.
    /// Returns the position in seconds, or nil if nothing is playing.
    func capturePlaybackPosition() async -> (episodeId: String, position: TimeInterval)? {
        guard let episodeId = currentEpisodeId else { return nil }
        let snapshot = await playbackService.snapshot()
        logger.info("Captured playback position for backgrounding: \(snapshot.currentTime)s")
        return (episodeId, snapshot.currentTime)
    }

    func playEpisode(_ episode: Episode) async {
        let episodeId = episode.canonicalEpisodeKey
        let podcastId = episode.podcast?.feedURL.absoluteString
        let playbackURL = episode.cachedAudioURL ?? episode.audioURL
        let position = episode.playbackPosition

        currentEpisodeId = episodeId
        currentPodcastId = podcastId
        currentEpisodeTitle = episode.title
        currentPodcastTitle = episode.podcast?.title
        currentArtworkURL = episode.podcast?.artworkURL
        currentAnalysisAssetId = nil

        // Start playback immediately (AVPlayer handles remote streams).
        await backgroundProcessingService.playbackDidStart()
        await playbackService.load(url: playbackURL, startPosition: position)
        await playbackService.play()
        await playbackService.setNowPlayingMetadata(
            title: episode.title,
            artist: episode.podcast?.author,
            albumTitle: episode.podcast?.title
        )

        // Resolve a local audio URL for analysis. The analysis pipeline
        // requires a LocalAudioURL — remote URLs are rejected at compile time.
        let localAudioURL: LocalAudioURL?
        if let cached = episode.cachedAudioURL, let local = LocalAudioURL(cached) {
            localAudioURL = local
        } else if let cached = await downloadManager.cachedFileURL(for: episodeId),
                  let local = LocalAudioURL(cached) {
            localAudioURL = local
        } else {
            localAudioURL = nil
        }

        if let analysisURL = localAudioURL {
            // Audio is local — start the analysis pipeline immediately.
            let resolvedAssetId = await analysisCoordinator.handlePlaybackEvent(
                .playStarted(
                    episodeId: episodeId,
                    podcastId: podcastId,
                    audioURL: analysisURL,
                    time: position,
                    rate: 1.0
                )
            )
            currentAnalysisAssetId = resolvedAssetId

            if let assetId = resolvedAssetId {
                await skipOrchestrator.beginEpisode(
                    analysisAssetId: assetId,
                    podcastId: podcastId
                )
            }
        } else {
            // Resolve the analysis asset ID now so the transcript button
            // appears while the download is in progress.
            let preResolvedAssetId = await analysisCoordinator.resolveAssetId(episodeId: episodeId)
            currentAnalysisAssetId = preResolvedAssetId

            if let assetId = preResolvedAssetId {
                await skipOrchestrator.beginEpisode(
                    analysisAssetId: assetId,
                    podcastId: podcastId
                )
            }

            // Download in background, then kick the analysis pipeline.
            logger.info("Episode not cached — downloading for analysis: \(episodeId)")
            audioCacheTask?.cancel()
            audioCacheTask = Task { [weak self, downloadManager, analysisCoordinator] in
                guard let self else { return }
                do {
                    let localURL = try await downloadManager.progressiveDownload(
                        episodeId: episodeId,
                        from: episode.audioURL
                    )
                    guard !Task.isCancelled, self.currentEpisodeId == episodeId else { return }
                    guard let analysisURL = LocalAudioURL(localURL) else {
                        self.logger.error("Download returned non-local URL: \(localURL)")
                        return
                    }

                    self.logger.info("Download complete — starting analysis pipeline")
                    let _ = await analysisCoordinator.handlePlaybackEvent(
                        .playStarted(
                            episodeId: episodeId,
                            podcastId: podcastId,
                            audioURL: analysisURL,
                            time: await self.playbackService.snapshot().currentTime,
                            rate: 1.0
                        )
                    )
                } catch {
                    guard !Task.isCancelled else { return }
                    self.logger.error("Background audio download failed: \(error)")
                }
            }
        }
    }

    func stopPlayback() async {
        audioCacheTask?.cancel()
        audioCacheTask = nil
        await backgroundProcessingService.playbackDidStop()
        await analysisCoordinator.handlePlaybackEvent(.stopped)
        await skipOrchestrator.endEpisode()
        await playbackService.pause()
        currentEpisodeId = nil
        currentPodcastId = nil
        currentAnalysisAssetId = nil
        currentEpisodeTitle = nil
        currentPodcastTitle = nil
        currentArtworkURL = nil
    }

    func recordListenRewind(windowId: String, podcastId: String) async {
        do {
            try await adDetectionService.recordListenRewind(
                windowId: windowId,
                podcastId: podcastId
            )
        } catch {
            // Rewinds are user-facing; trust scoring remains best-effort.
        }
    }

    func togglePlayPause(isPlaying: Bool) async {
        if isPlaying {
            let snapshot = await playbackService.snapshot()
            await playbackService.pause()
            await analysisCoordinator.handlePlaybackEvent(.paused(time: snapshot.currentTime))
        } else {
            let snapshot = await playbackService.snapshot()
            await playbackService.play()
            await analysisCoordinator.handlePlaybackEvent(
                .timeUpdate(
                    time: snapshot.currentTime,
                    rate: snapshot.playbackSpeed
                )
            )
        }
    }

    func skipForward() async {
        let snapshot = await playbackService.snapshot()
        let target = min(
            snapshot.currentTime + PlaybackService.skipForwardSeconds,
            snapshot.duration
        )
        await seek(to: target)
    }

    func skipBackward() async {
        let snapshot = await playbackService.snapshot()
        let target = max(
            snapshot.currentTime - PlaybackService.skipBackwardSeconds,
            0
        )
        await seek(to: target)
    }

    func seek(to seconds: TimeInterval) async {
        let snapshot = await playbackService.snapshot()
        await skipOrchestrator.recordUserSeek(to: seconds)
        await playbackService.seek(to: seconds)
        await analysisCoordinator.handlePlaybackEvent(
            .scrubbed(to: seconds, rate: snapshot.playbackSpeed)
        )
    }

    func setSpeed(_ speed: Float) async {
        let snapshot = await playbackService.snapshot()
        await playbackService.setSpeed(speed)
        await analysisCoordinator.handlePlaybackEvent(
            .speedChanged(rate: speed, time: snapshot.currentTime)
        )
    }

}
