// PlayheadRuntime.swift
// Shared app-level composition root for long-lived services.

@preconcurrency import AVFoundation
import Foundation
import OSLog
import UIKit

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
    let analysisJobRunner: AnalysisJobRunner
    let analysisWorkScheduler: AnalysisWorkScheduler
    let analysisJobReconciler: AnalysisJobReconciler

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
    /// Retains the progressive resource loader outside PlaybackServiceActor.
    private var activeProgressiveLoader: ProgressiveResourceLoader?

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

        // Phase 3 shadow-mode wiring. The factory receives the store and the
        // config-selected FMBackfillMode and returns a fully-wired runner that
        // uses:
        //   • FoundationModelClassifier's live on-device runtime (default init)
        //   • CapabilitiesService.currentSnapshot for thermal/charging state
        //   • UIDeviceBatteryProvider for battery level (same source as BPS)
        //   • ScanCohort.productionJSON() as the reuse-cache key
        // When AdDetectionConfig.fmBackfillMode == .disabled, the factory is
        // never invoked. When .shadow (default), FM runs and writes telemetry
        // to semantic_scan_results / evidence_events but never influences
        // AdWindow rows (shadow invariant pinned by
        // AdDetectionServiceShadowModeTests).
        let capabilitiesServiceForFactory = capabilitiesService
        let batteryProvider = UIDeviceBatteryProvider()
        let scanCohortJSON = ScanCohort.productionJSON()
        let backfillJobRunnerFactory: @Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner = { store, mode in
            BackfillJobRunner(
                store: store,
                admissionController: AdmissionController(),
                classifier: FoundationModelClassifier(),
                coveragePlanner: CoveragePlanner(),
                mode: mode,
                capabilitySnapshotProvider: { await capabilitiesServiceForFactory.currentSnapshot },
                batteryLevelProvider: { await batteryProvider.currentBatteryState().level },
                scanCohortJSON: scanCohortJSON
            )
        }
        self.adDetectionService = AdDetectionService(
            store: analysisStore,
            metadataExtractor: FallbackExtractor(),
            backfillJobRunnerFactory: backfillJobRunnerFactory
        )
        self.skipOrchestrator = SkipOrchestrator(
            store: analysisStore,
            trustService: trustService
        )
        self.downloadManager = DownloadManager()
        self.analysisCoordinator = AnalysisCoordinator(
            store: analysisStore,
            audioService: audioService,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            capabilitiesService: capabilitiesService,
            adDetectionService: adDetectionService,
            skipOrchestrator: skipOrchestrator,
            downloadManager: downloadManager
        )
        self.backgroundProcessingService = BackgroundProcessingService(
            coordinator: analysisCoordinator,
            capabilitiesService: capabilitiesService
        )

        let cueMaterializer = SkipCueMaterializer(store: analysisStore)
        self.analysisJobRunner = AnalysisJobRunner(
            store: analysisStore,
            audioProvider: audioService,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adDetectionService,
            cueMaterializer: cueMaterializer
        )
        self.analysisWorkScheduler = AnalysisWorkScheduler(
            store: analysisStore,
            jobRunner: analysisJobRunner,
            capabilitiesService: capabilitiesService,
            downloadManager: downloadManager
        )
        self.analysisJobReconciler = AnalysisJobReconciler(
            store: analysisStore,
            downloadManager: downloadManager,
            capabilitiesService: capabilitiesService
        )

        // Set error state after all stored properties are initialized.
        if let storeError {
            self.initializationError = storeError
        }

        Task { await capabilitiesService.startObserving() }

        Task { [analysisStore, downloadManager, analysisWorkScheduler, analysisJobReconciler, backgroundProcessingService] in
            // Migrate the analysis store before any component queries its tables.
            do {
                try await analysisStore.migrate()
            } catch {
                Logger(subsystem: "com.playhead", category: "Runtime")
                    .fault("Analysis store migration failed — pre-analysis pipeline disabled: \(error)")
                return  // Don't start the pipeline if tables don't exist
            }
            await downloadManager.setAnalysisWorkScheduler(analysisWorkScheduler)
            await backgroundProcessingService.setPreAnalysisServices(
                scheduler: analysisWorkScheduler,
                reconciler: analysisJobReconciler
            )
            do {
                _ = try await analysisJobReconciler.reconcile()
            } catch {
                Logger(subsystem: "com.playhead", category: "Runtime")
                    .error("Job reconciliation failed at startup: \(error)")
            }
            await analysisWorkScheduler.startSchedulerLoop()
        }

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
                try await downloadManager.bootstrap()
            } catch {
                // Non-fatal: downloads will fail but playback still works.
            }

            do {
                try await modelInventory.scan()
            } catch {
                // Settings can still render, but model lifecycle reporting will be empty.
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
        let position = episode.playbackPosition

        currentEpisodeId = episodeId
        currentPodcastId = podcastId
        currentEpisodeTitle = episode.title
        currentPodcastTitle = episode.podcast?.title
        currentArtworkURL = episode.podcast?.artworkURL
        currentAnalysisAssetId = nil

        await backgroundProcessingService.playbackDidStart()

        // Load artwork for lock screen / CarPlay Now Playing.
        var artwork: UIImage?
        if let artworkURL = episode.podcast?.artworkURL {
            if let (data, _) = try? await URLSession.shared.data(from: artworkURL),
               let image = UIImage(data: data) {
                artwork = image
            }
        }

        await playbackService.setNowPlayingMetadata(
            title: episode.title,
            artist: episode.podcast?.author,
            albumTitle: episode.podcast?.title,
            artworkImage: artwork
        )

        // Resolve a local audio file. Both playback and analysis use the
        // same file to avoid dynamic ad insertion serving different ads
        // on separate HTTP requests.
        let localURL: URL
        if let cached = episode.cachedAudioURL {
            localURL = cached
        } else if let cached = await downloadManager.cachedFileURL(for: episodeId) {
            localURL = cached
        } else {
            // Not cached — stream-download and play once enough is buffered.
            logger.info("Episode not cached — streaming download: \(episodeId)")
            audioCacheTask?.cancel()
            audioCacheTask = Task { [weak self, downloadManager, analysisCoordinator, analysisWorkScheduler] in
                guard let self else { return }
                do {
                    let result = try await downloadManager.streamingDownload(
                        episodeId: episodeId,
                        from: episode.audioURL
                    )
                    guard !Task.isCancelled, self.currentEpisodeId == episodeId else { return }

                    // Build the progressive player item outside the actor.
                    // The ProgressiveResourceLoader must NOT live on
                    // PlaybackServiceActor — its AVFoundation delegate callbacks
                    // run on a separate dispatch queue and trigger actor executor
                    // assertions during Siri/phone call interruptions.
                    if let totalBytes = result.totalBytes {
                        let loader = ProgressiveResourceLoader(
                            fileURL: result.fileURL,
                            totalBytes: totalBytes,
                            contentType: result.contentType
                        )
                        // Hold a strong reference so the loader outlives this scope.
                        self.activeProgressiveLoader = loader

                        var components = URLComponents()
                        components.scheme = "playhead-progressive"
                        components.host = "audio"
                        components.path = "/\(result.fileURL.lastPathComponent)"
                        if let proxyURL = components.url {
                            let asset = AVURLAsset(url: proxyURL)
                            asset.resourceLoader.setDelegate(loader, queue: loader.queue)
                            let item = AVPlayerItem(asset: asset)
                            await self.playbackService.loadItem(item, startPosition: position)
                        } else {
                            await self.playbackService.load(url: result.fileURL, startPosition: position)
                        }
                    } else {
                        await self.playbackService.load(url: result.fileURL, startPosition: position)
                    }
                    await self.playbackService.play()

                    // Resolve the analysis asset ID early so pre-materialized
                    // skip cues can be loaded before the download finishes.
                    guard let analysisURL = LocalAudioURL(result.fileURL) else {
                        self.logger.error("Download returned non-local URL: \(result.fileURL)")
                        return
                    }
                    let resolvedAssetId = await analysisCoordinator.handlePlaybackEvent(
                        .playStarted(
                            episodeId: episodeId,
                            podcastId: podcastId,
                            audioURL: analysisURL,
                            time: position,
                            rate: 1.0
                        )
                    )
                    self.currentAnalysisAssetId = resolvedAssetId
                    if let assetId = resolvedAssetId {
                        await analysisWorkScheduler.playbackStarted(episodeId: episodeId)
                        await self.skipOrchestrator.beginEpisode(
                            analysisAssetId: assetId,
                            podcastId: podcastId
                        )
                    }

                    // Wait for the full download before starting analysis —
                    // the decoder needs the complete file to get all shards.
                    try await result.downloadComplete()
                    guard !Task.isCancelled, self.currentEpisodeId == episodeId else { return }

                    // Release the progressive loader — download is complete,
                    // all bytes are on disk and already served.
                    self.activeProgressiveLoader = nil

                    // Evict any stale shard cache from a prior partial decode
                    // (the truncated 2MB file had different shard count/content).
                    await self.audioService.evictCache(episodeID: episodeId)
                } catch {
                    guard !Task.isCancelled else { return }
                    self.logger.error("Episode download failed: \(error)")
                }
            }
            return
        }

        // Audio is local — play and analyze from the same file.
        await playbackService.load(url: localURL, startPosition: position)
        await playbackService.play()

        guard let analysisURL = LocalAudioURL(localURL) else { return }
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
            await analysisWorkScheduler.playbackStarted(episodeId: episodeId)
            await skipOrchestrator.beginEpisode(
                analysisAssetId: assetId,
                podcastId: podcastId
            )
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
