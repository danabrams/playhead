// PlayheadRuntime.swift
// Shared app-level composition root for long-lived services.

@preconcurrency import AVFoundation
import Foundation
import os
import OSLog
import UIKit

enum PlaybackPositionPersistenceTrigger: String, Sendable {
    case periodic
    case paused
    case seek
    case stopped
    case background
}

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

    /// bd-fmfb: DEBUG-only sink for `LanguageModelSession.logFeedbackAttachment`
    /// payloads. Apple's iOS 26.4 on-device safety classifier rejects benign
    /// podcast advertising; we capture machine-readable feedback so the
    /// FoundationModels team can fix it. Release builds intentionally leave
    /// this `nil` — production users should not have feedback attachments
    /// piling up in their app sandbox, and `FoundationModelClassifier` skips
    /// the entire capture path when this is `nil`.
    #if DEBUG
    let feedbackStore: FoundationModelsFeedbackStore?
    #else
    let feedbackStore: FoundationModelsFeedbackStore? = nil
    #endif

    private let isPreviewRuntime: Bool
    private let logger = Logger(subsystem: "com.playhead", category: "Runtime")
    @ObservationIgnored
    private var playbackPositionPersistenceHandler: (@MainActor (PlaybackPositionPersistenceTrigger) async -> Void)?

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
    /// bd-3bz (Phase 4): observer that watches `canUseFoundationModels` for
    /// false→true transitions and, after a 60s-stable-true debounce, drains
    /// any `analysis_sessions` rows flagged with `needsShadowRetry = 1`.
    /// Held strongly by the runtime so its observer loop survives until the
    /// runtime is torn down. Stopped explicitly via `shutdown()`; tests that
    /// spin up transient runtimes should call that to avoid leaking
    /// AsyncStream subscriptions across tests. `deinit` also makes a
    /// best-effort stop so an abandoned runtime does not keep the observer
    /// alive forever.
    @ObservationIgnored
    private let shadowRetryObserver: ShadowRetryObserver?
    @ObservationIgnored
    private var shadowRetryObserverStartupTask: Task<Void, Never>?
    private var isShutdown = false

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

        // bd-fmfb: instantiate the feedback store ONLY in DEBUG. Release
        // builds get a nil store and `FoundationModelClassifier` short-
        // circuits the capture path so production users never accumulate
        // attachment files in the sandbox.
        #if DEBUG
        let feedbackStore = FoundationModelsFeedbackStore()
        self.feedbackStore = feedbackStore
        #else
        let feedbackStore: FoundationModelsFeedbackStore? = nil
        #endif

        // HIGH-1: round-2's M-B hoist shared one AdmissionController across
        // every factory invocation. AdDetectionService is actor-reentrant on
        // `await`, so two concurrent `runBackfill` calls would contend on the
        // same controller: the second call saw `runningJob != nil` and
        // mass-deferred its entire batch with `serialBusy`. Per-call
        // controllers correctly give each `runBackfill` invocation its own
        // queue. The original M-B concern (parallel FM inference) is not
        // solved by sharing the controller — it would require a separate FM
        // session manager, which is out of scope here. The concurrent
        // no-mass-defer invariant is pinned by
        // `concurrentRunBackfillsDoNotMassDeferEachOther` in
        // BackfillJobRunnerTests.
        // bd-1en Phase 1: build the sensitive-window router and the
        // permissive-path classifier once and capture them in the
        // factory closure. Both are opt-in: if `RedactionRules.json`
        // fails to load the router falls back to `.noop` and dispatch
        // is a no-op (production behavior is byte-identical to the
        // pre-bd-1en path). The permissive classifier is constructed
        // only on iOS 26+ since `SystemLanguageModel(guardrails:)` is
        // gated.
        let bd1enRedactor = PromptRedactor.loadDefault()
        let bd1enRouter: SensitiveWindowRouter = bd1enRedactor.map { SensitiveWindowRouter(redactor: $0) }
            ?? .noop
        let bd1enPermissiveBox: BackfillJobRunner.PermissiveClassifierBox? = {
            if #available(iOS 26.0, *) {
                return BackfillJobRunner.PermissiveClassifierBox(PermissiveAdClassifier())
            }
            return nil
        }()

        let backfillJobRunnerFactory: @Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner = {
            [capabilitiesServiceForFactory, batteryProvider, feedbackStore, bd1enRouter, bd1enPermissiveBox] store, mode in
            BackfillJobRunner(
                store: store,
                admissionController: AdmissionController(),
                classifier: FoundationModelClassifier(feedbackStore: feedbackStore),
                coveragePlanner: CoveragePlanner(),
                mode: mode,
                capabilitySnapshotProvider: { await capabilitiesServiceForFactory.currentSnapshot },
                batteryLevelProvider: { await batteryProvider.currentBatteryState().level },
                // H-A: compute the scan cohort JSON per factory invocation
                // rather than freezing it at runtime init. Locale changes
                // mid-process were previously invisible to the cohort hash,
                // leaving reuse-cache lookups keyed off stale values. The
                // per-call cost is microseconds (a handful of string fields
                // JSON-encoded with sorted keys) and buys cohort freshness.
                scanCohortJSON: ScanCohort.productionJSON(),
                sensitiveRouter: bd1enRouter,
                permissiveClassifier: bd1enPermissiveBox
            )
        }
        // bd-3bz (Phase 4) / H7 (cycle 2): when the shadow phase bails on
        // FM unavailability, mark the explicit session id supplied by the
        // shadow phase. The marker no longer does an asset→session lookup
        // (see H7 in AdDetectionService.runShadowFMPhase): the session id
        // is captured at the START of the shadow phase to fix a race
        // window where concurrent reprocessing on the same asset could
        // create a newer session and the marker would tag the wrong row.
        //
        // H2 (cycle 2): after marking, immediately wake the shadow retry
        // observer. Without this, a session flagged while capability is
        // already stable-true would never re-drain — the observer's
        // capability stream only fires on transitions, not on flags.
        // The observer is constructed below (after AdDetectionService,
        // since the observer depends on the service as its drainer), so
        // we capture a Sendable holder here and populate it once the
        // observer exists. The closure reads through the holder lazily.
        let analysisStoreForMarker = analysisStore
        let observerHolder = ShadowRetryObserverHolder()
        let shadowSkipMarker: @Sendable (String, String) async -> Void = { sessionId, podcastId in
            do {
                try await analysisStoreForMarker.markSessionNeedsShadowRetry(
                    id: sessionId,
                    podcastId: podcastId
                )
                if let observer = observerHolder.observer {
                    await observer.wake()
                }
            } catch {
                Logger(subsystem: "com.playhead", category: "Runtime")
                    .warning("bd-3bz: failed to mark session \(sessionId) for shadow retry: \(error.localizedDescription)")
            }
        }

        self.adDetectionService = AdDetectionService(
            store: analysisStore,
            metadataExtractor: FallbackExtractor(),
            backfillJobRunnerFactory: backfillJobRunnerFactory,
            // M-D: capabilities provider lets the shadow phase short-circuit
            // before building atom/segment/catalog inputs on devices that
            // cannot run Foundation Models. The closure hits the actor on
            // every invocation so the result stays current with runtime
            // capability changes (FM became unavailable, AI disabled, etc.).
            canUseFoundationModelsProvider: { [capabilitiesServiceForFactory] in
                await capabilitiesServiceForFactory.currentSnapshot.canUseFoundationModels
            },
            shadowSkipMarker: shadowSkipMarker
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
            downloadManager: downloadManager,
            batteryProvider: batteryProvider
        )
        self.analysisJobReconciler = AnalysisJobReconciler(
            store: analysisStore,
            downloadManager: downloadManager,
            capabilitiesService: capabilitiesService
        )

        // bd-3bz (Phase 4): construct the shadow-retry observer once all
        // dependencies (capabilitiesService, analysisStore, adDetectionService)
        // are initialized. The observer is skipped in preview runtimes — the
        // SwiftUI preview canvas spins up many transient runtimes and we don't
        // want stray AsyncStream subscriptions piling up there. The observer
        // task is started below after `super`-style initialization completes.
        if !isPreviewRuntime {
            let observer = ShadowRetryObserver(
                capabilities: capabilitiesService,
                store: analysisStore,
                drainer: adDetectionService
            )
            self.shadowRetryObserver = observer
            // H2: publish the observer through the holder captured by the
            // shadow-skip marker closure so subsequent marker invocations
            // can wake() the observer.
            observerHolder.observer = observer
        } else {
            self.shadowRetryObserver = nil
        }

        // Set error state after all stored properties are initialized.
        if let storeError {
            self.initializationError = storeError
        }

        // H-B: enable battery monitoring exactly once at runtime init rather
        // than paying a MainActor hop every time `UIDeviceBatteryProvider`
        // reads the level. The provider's per-call setter becomes a harmless
        // no-op and the warning about "every refinement window pays a hop
        // for the setter" is mitigated. The actual battery read still
        // requires a MainActor hop but that's unavoidable on UIKit.
        Task { @MainActor in
            UIDevice.current.isBatteryMonitoringEnabled = true
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

            // bd-200: prune scan rows under stale cohort hashes (locale change,
            // app upgrade, prompt/schema/plan/normalization revs). Best-effort —
            // failures are logged but don't block app launch. Must run AFTER
            // migrate() succeeds but BEFORE any production code reads the store,
            // and must use the SAME ScanCohort.productionJSON() value the
            // BackfillJobRunner factory uses so the reuse-cache invariant holds.
            do {
                let pruned = try await analysisStore.pruneOrphanedScansForCurrentCohort(
                    currentScanCohortJSON: ScanCohort.productionJSON()
                )
                if pruned > 0 {
                    Logger(subsystem: "com.playhead", category: "Runtime")
                        .info("Pruned \(pruned, privacy: .public) orphan scan/evidence rows under prior cohorts")
                }
            } catch {
                Logger(subsystem: "com.playhead", category: "Runtime")
                    .warning("Cohort orphan prune failed: \(error.localizedDescription, privacy: .public)")
            }

            // bd-3bz (Phase 4): only start the shadow-retry observer after
            // the store is definitely migrated. The observer immediately
            // queries the sessions table, so starting it earlier races the
            // schema bootstrap on cold launch.
            if let shadowRetryObserver {
                await self.startShadowRetryObserverIfNeeded(observer: shadowRetryObserver)
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

    deinit {
        // C7b: previously this `deinit` spawned `Task { await observer.stop() }`
        // to chase the async observer teardown. That was racy: the spawned
        // task could outlive the runtime by an unbounded amount, and a new
        // PlayheadRuntime construction back-to-back could find a stale
        // observer task still draining the wake stream from the prior
        // instance. Calling `shutdown()` is now mandatory (`withTestRuntime`
        // enforces this in tests). The `deinit` only does the synchronous
        // safety nets — cancelling the startup task and the drain timer —
        // and leaves the observer loop teardown to `shutdown()`.
        shadowRetryObserverStartupTask?.cancel()
    }

    #if DEBUG
    /// Cycle 4 H3: DEBUG-only accessor so `RuntimeTeardownTests` can
    /// assert that the shadow retry observer's loop actually exited
    /// after `shutdown()`. Exposed as a test seam rather than making the
    /// stored property internal to keep production access patterns
    /// unchanged.
    func _shadowRetryObserverForTesting() -> ShadowRetryObserver? {
        shadowRetryObserver
    }
    #endif

    /// Stops long-lived runtime observers and cancels any pending startup
    /// task. Safe to call more than once.
    func shutdown() async {
        guard !isShutdown else { return }
        isShutdown = true
        shadowRetryObserverStartupTask?.cancel()
        shadowRetryObserverStartupTask = nil
        await shadowRetryObserver?.stop()
    }

    @MainActor
    private func startShadowRetryObserverIfNeeded(observer: ShadowRetryObserver) {
        guard !isShutdown, shadowRetryObserverStartupTask == nil else { return }
        shadowRetryObserverStartupTask = Task { [observer] in
            guard !Task.isCancelled else { return }
            await observer.start()
        }
    }

    /// Capture and expose the current playback position for persistence.
    /// Called from playback and scene handlers to save state opportunistically.
    /// Returns the position in seconds, or nil if nothing is playing.
    func capturePlaybackPosition() async -> (episodeId: String, position: TimeInterval)? {
        guard let episodeId = currentEpisodeId else { return nil }
        let snapshot = await playbackService.snapshot()
        logger.info("Captured playback position: \(snapshot.currentTime)s")
        return (episodeId, snapshot.currentTime)
    }

    func setPlaybackPositionPersistenceHandler(
        _ handler: @escaping @MainActor (PlaybackPositionPersistenceTrigger) async -> Void
    ) {
        playbackPositionPersistenceHandler = handler
    }

    private func requestPlaybackPositionPersistence(
        _ trigger: PlaybackPositionPersistenceTrigger
    ) async {
        guard let handler = playbackPositionPersistenceHandler else { return }
        await handler(trigger)
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
            audioCacheTask = Task { [weak self, downloadManager, analysisCoordinator] in
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
                    await self.analysisWorkScheduler.playbackStarted(episodeId: episodeId)

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
        await analysisWorkScheduler.playbackStarted(episodeId: episodeId)

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
            await skipOrchestrator.beginEpisode(
                analysisAssetId: assetId,
                podcastId: podcastId
            )
        }
    }

    func stopPlayback() async {
        await requestPlaybackPositionPersistence(.stopped)
        audioCacheTask?.cancel()
        audioCacheTask = nil
        await analysisWorkScheduler.playbackStopped()
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
            await requestPlaybackPositionPersistence(.paused)
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
        await requestPlaybackPositionPersistence(.seek)
    }

    func setSpeed(_ speed: Float) async {
        let snapshot = await playbackService.snapshot()
        await playbackService.setSpeed(speed)
        await analysisCoordinator.handlePlaybackEvent(
            .speedChanged(rate: speed, time: snapshot.currentTime)
        )
    }

}

/// Thread-safe holder for the lazily-initialized `ShadowRetryObserver`.
///
/// The shadow-skip-marker closure is built BEFORE the observer is
/// constructed (the observer depends on `AdDetectionService`, which
/// already captured the marker closure). The closure therefore needs a
/// box it can read through at call time. The write happens exactly
/// once, on the MainActor during runtime init, but reads happen from
/// arbitrary actor contexts whenever a shadow session is flagged —
/// that's a cross-thread read of a mutable field and must be
/// synchronized.
///
/// Cycle-4 M1: previously implemented as `@unchecked Sendable` with a
/// plain `var`, justified by "writes happen before the first marker
/// call can reach the closure". That argument is functionally correct
/// but brittle — a future refactor that hoists a marker call earlier in
/// init would silently race. `OSAllocatedUnfairLock` makes the
/// synchronization explicit and lets the type drop the `@unchecked`
/// escape hatch.
private final class ShadowRetryObserverHolder: Sendable {
    private let storage = OSAllocatedUnfairLock<ShadowRetryObserver?>(initialState: nil)

    var observer: ShadowRetryObserver? {
        get { storage.withLock { $0 } }
        set { storage.withLock { $0 = newValue } }
    }
}
