import SwiftUI
import SwiftData
import OSLog

@main
struct PlayheadApp: App {
    let modelContainer: ModelContainer
    @State private var runtime = PlayheadRuntime()
    @Environment(\.scenePhase) private var scenePhase
    /// playhead-24cm: attaches `PlayheadAppDelegate` so iOS can deliver
    /// `application(_:handleEventsForBackgroundURLSession:completionHandler:)`
    /// when the OS wakes the app to drain background URLSession events.
    @UIApplicationDelegateAdaptor(PlayheadAppDelegate.self) private var appDelegate

    private static let logger = Logger(subsystem: "com.playhead", category: "App")
    private static let playbackPositionSaveInterval: TimeInterval = 15
    private static let playbackPositionMeaningfulDelta: TimeInterval = 0.5

    init() {
        // playhead-8em9 (narL): DEBUG builds honor `-MetadataActivationOverride
        // allEnabled` passed via Xcode scheme arguments so personal dogfooding
        // can flip the counterfactual gate without editing source. Release
        // builds strip this via `#if DEBUG` inside the override type.
        MetadataActivationOverride.applyLaunchArguments(CommandLine.arguments)

        // Attempt to create the SwiftData container. On failure, delete the
        // store and retry once. If that also fails, fall back to in-memory.
        do {
            modelContainer = try SwiftDataStore.makeContainer()
        } catch {
            Self.logger.error("SwiftData container failed: \(error). Attempting recovery...")
            // Delete corrupted store and retry.
            Self.deleteSwiftDataStore()
            do {
                modelContainer = try SwiftDataStore.makeContainer()
                Self.logger.info("SwiftData container recovered after store reset.")
            } catch {
                Self.logger.error("SwiftData recovery failed: \(error). Using in-memory store.")
                // Last resort: in-memory container so the app can still launch.
                do {
                    let config = ModelConfiguration(
                        "Playhead",
                        schema: SwiftDataStore.schema,
                        isStoredInMemoryOnly: true
                    )
                    modelContainer = try ModelContainer(
                        for: SwiftDataStore.schema,
                        configurations: [config]
                    )
                } catch {
                    fatalError("Cannot create even an in-memory SwiftData container: \(error)")
                }
            }
        }
    }

    /// playhead-zp0x: live notification service used by the
    /// `BatchNotificationCoordinator`. Constructed once at App scope so
    /// the `@MainActor` Task that drives periodic reductions has a
    /// stable reference. The service wraps `UNUserNotificationCenter`
    /// via `SystemNotificationScheduler`.
    @State private var batchNotificationService = BatchNotificationService()

    var body: some Scene {
        WindowGroup {
            RootView()
                .environment(runtime)
                .task {
                    // playhead-zp0x: drive the batch-notification
                    // coordinator on a periodic tick. v1 uses a simple
                    // hourly Task timer; if a richer scheduler-pass
                    // signal is later added, this can be migrated to an
                    // event-driven loop without changing the
                    // coordinator's contract.
                    //
                    // The coordinator runs against an open-batch fetch
                    // and exits cheaply when no rows match — safe to
                    // tick aggressively. Cold-start eviction also runs
                    // here so closed-and-aged rows do not accumulate.
                    let context = modelContainer.mainContext
                    DownloadBatchEvictor.evict(modelContext: context, now: .now)
                    let summaryBuilder = runtime.makeBatchSummaryBuilder(
                        modelContainer: modelContainer
                    )
                    let coordinator = BatchNotificationCoordinator(
                        modelContext: context,
                        service: batchNotificationService,
                        summaryBuilder: summaryBuilder
                    )
                    while !Task.isCancelled {
                        await coordinator.runOncePass(now: .now)
                        // 1-hour interval — balances responsiveness
                        // (overnight downloads still fire promptly on
                        // first foreground) against battery / wakeups.
                        try? await Task.sleep(nanoseconds: UInt64(60 * 60) * 1_000_000_000)
                    }
                }
                .task {
                    // playhead-24cm: register the live DownloadManager
                    // and AppDelegate for background URLSession wake-up
                    // routing. Safe to call repeatedly.
                    DownloadManager.registerAppDelegate(appDelegate)
                    DownloadManager.registerShared(runtime.downloadManager)
                    runtime.setPlaybackPositionPersistenceHandler { trigger in
                        await Self.persistPlaybackPosition(
                            runtime: runtime,
                            modelContainer: modelContainer,
                            trigger: trigger
                        )
                    }
                    // playhead-z3ch: install the SwiftData-backed
                    // EpisodeMetadataProvider so the fusion pipeline can
                    // pre-seed metadata-derived ledger entries (capped at
                    // 0.15, gated by corroboration). Wired here — not in
                    // PlayheadRuntime.init — because the SwiftData
                    // ModelContainer is only available once the App scene
                    // has constructed both the runtime and the container.
                    let analysisStore = runtime.analysisStore
                    let provider = SwiftDataEpisodeMetadataProvider(
                        assetLookup: { assetId in
                            try? await analysisStore.fetchAsset(id: assetId)
                        },
                        metadataLookup: { episodeId in
                            let context = modelContainer.mainContext
                            let descriptor = FetchDescriptor<Episode>(
                                predicate: #Predicate { $0.canonicalEpisodeKey == episodeId }
                            )
                            return (try? context.fetch(descriptor).first)?.feedMetadata
                        }
                    )
                    await runtime.adDetectionService.setEpisodeMetadataProvider(provider)

                    // playhead-fv2q: construct and attach the periodic
                    // feed-refresh service now that the ModelContainer is
                    // available. The BGAppRefreshTask identifier itself
                    // was registered earlier in `PlayheadRuntime.init`
                    // (BGTaskScheduler requires that before launch ends);
                    // this path attaches the real service instance to the
                    // shared holder the early-registered handler reads
                    // from, then kicks off the first reschedule. A BGTask
                    // fire that lands between runtime init and this
                    // attach simply reschedules and bails gracefully —
                    // see `registerTaskHandler` for that fallback.
                    let feedRefreshService = BackgroundFeedRefreshService(
                        enumerator: ProductionPodcastEnumerator(
                            modelContainer: modelContainer
                        ),
                        refresher: ProductionFeedRefresher(
                            discoveryService: PodcastDiscoveryService(),
                            modelContainer: modelContainer
                        ),
                        downloader: ProductionAutoDownloadEnqueuer(
                            downloadManager: runtime.downloadManager
                        ),
                        settingsProvider: ProductionDownloadsSettingsProvider()
                    )
                    BackgroundFeedRefreshService.attachSharedService(feedRefreshService)
                    feedRefreshService.start()
                }
                .task {
                    let stateStream = await runtime.playbackService.observeStates()
                    var lastStatus: PlaybackState.Status = .idle
                    var lastPeriodicCheckpoint: TimeInterval?

                    for await state in stateStream {
                        switch state.status {
                        case .playing:
                            if lastStatus != .playing ||
                                (lastPeriodicCheckpoint != nil && state.currentTime < (lastPeriodicCheckpoint ?? 0)) {
                                lastPeriodicCheckpoint = state.currentTime
                            }

                            if let checkpoint = lastPeriodicCheckpoint,
                               state.currentTime - checkpoint >= Self.playbackPositionSaveInterval {
                                await Self.persistPlaybackPosition(
                                    runtime: runtime,
                                    modelContainer: modelContainer,
                                    trigger: .periodic
                                )
                                lastPeriodicCheckpoint = state.currentTime
                            }

                        case .paused:
                            lastPeriodicCheckpoint = state.currentTime

                        case .idle:
                            lastPeriodicCheckpoint = nil

                        default:
                            break
                        }

                        lastStatus = state.status
                    }
                }
        }
        .modelContainer(modelContainer)
        .onChange(of: scenePhase) { oldPhase, newPhase in
            switch newPhase {
            case .background:
                Self.logger.info("Scene phase: background — persisting playback position")
                Task {
                    await Self.persistPlaybackPosition(
                        runtime: runtime,
                        modelContainer: modelContainer,
                        trigger: .background
                    )
                }
            case .active:
                Self.logger.info("Scene phase: active")
            case .inactive:
                break
            @unknown default:
                break
            }
        }
    }

    /// Deletes the on-disk SwiftData store files to allow recovery.
    private static func deleteSwiftDataStore() {
        let fm = FileManager.default
        guard let appSupport = fm.urls(for: .applicationSupportDirectory, in: .userDomainMask).first else { return }
        let storeName = "Playhead"
        let extensions = ["store", "store-shm", "store-wal"]
        for ext in extensions {
            let url = appSupport.appendingPathComponent("\(storeName).\(ext)")
            try? fm.removeItem(at: url)
        }
    }

    @MainActor
    private static func persistPlaybackPosition(
        runtime: PlayheadRuntime,
        modelContainer: ModelContainer,
        trigger: PlaybackPositionPersistenceTrigger
    ) async {
        guard let captured = await runtime.capturePlaybackPosition() else { return }

        let context = modelContainer.mainContext
        let episodeId = captured.episodeId
        let descriptor = FetchDescriptor<Episode>(
            predicate: #Predicate { $0.canonicalEpisodeKey == episodeId }
        )

        guard let episode = try? context.fetch(descriptor).first else {
            logger.warning(
                "Playback position persistence skipped: episode \(episodeId) not found for trigger \(trigger.rawValue)"
            )
            return
        }

        guard abs(episode.playbackPosition - captured.position) >= playbackPositionMeaningfulDelta else {
            return
        }

        episode.playbackPosition = captured.position
        // playhead-cthe: the readiness anchor tracks the play-loop commit
        // point 1:1. Updating it here means a force-quit mid-playback
        // preserves the last persisted commit as the readiness anchor
        // (per the bead spec's "last persisted commit wins" rule), and
        // the Library cell's ✓ derivation reflects real playback
        // progress without any separate subscription.
        episode.playbackAnchor = captured.position
        let episodeDuration = episode.duration
        do {
            try context.save()
            logger.info(
                "Saved position \(captured.position)s for episode \(episodeId), trigger=\(trigger.rawValue)"
            )
        } catch {
            logger.error(
                "Failed to save position \(captured.position)s for episode \(episodeId), trigger=\(trigger.rawValue): \(error)"
            )
        }

        // playhead-vhha: notify the candidate-window cascade of the
        // committed playhead so the resumed-window selection re-latches
        // when the user has seeked > 30 s away from the prior anchor.
        // Runs after the SwiftData save (success or failure): the
        // cascade is purely advisory state and shouldn't be skipped on
        // a transient persistence error — the commit point still
        // represents the user's intended playhead.
        await runtime.noteCommittedPlayhead(
            episodeId: episodeId,
            position: captured.position,
            episodeDuration: episodeDuration
        )
    }

}

// MARK: - Root View

/// Switches between onboarding and main content based on first-launch state.
private struct RootView: View {

    @AppStorage("hasCompletedOnboarding") private var hasCompletedOnboarding = false
    @AppStorage(OnboardingFlags.firstSubscriptionOnboardingSeenKey)
    private var hasSeenFirstSubscriptionOnboarding = false
    @Environment(PlayheadRuntime.self) private var runtime
    @Query private var podcasts: [Podcast]
    @State private var showSplash = true
    @State private var presentFirstSubscriptionOnboarding = false

    var body: some View {
        Group {
            if let error = runtime.initializationError {
                errorView(error)
            } else if hasCompletedOnboarding {
                ZStack {
                    ContentView()

                    if showSplash {
                        ReturningSplashView()
                            .transition(.opacity)
                            .zIndex(1)
                    }
                }
                .onAppear {
                    withAnimation(.easeOut(duration: 0.4).delay(0.3)) {
                        showSplash = false
                    }
                    evaluateFirstSubscriptionOnboarding()
                }
                .onChange(of: podcasts.count) { _, _ in
                    evaluateFirstSubscriptionOnboarding()
                }
                .fullScreenCover(isPresented: $presentFirstSubscriptionOnboarding) {
                    FirstSubscriptionOnboardingView(onDismiss: {
                        presentFirstSubscriptionOnboarding = false
                    })
                }
            } else {
                OnboardingView()
            }
        }
    }

    /// Show the one-screen first-subscription onboarding if (a) the user
    /// has completed first-launch onboarding, (b) at least one podcast
    /// is subscribed, and (c) the user has not yet tapped "Got it".
    /// Gate logic lives in `OnboardingGating` (pure) for testability.
    private func evaluateFirstSubscriptionOnboarding() {
        if OnboardingGating.shouldPresentFirstSubscriptionOnboarding(
            hasCompletedOnboarding: hasCompletedOnboarding,
            hasSeenFirstSubscriptionOnboarding: hasSeenFirstSubscriptionOnboarding,
            podcastCount: podcasts.count
        ) {
            presentFirstSubscriptionOnboarding = true
        }
    }

    private func errorView(_ message: String) -> some View {
        VStack(spacing: Spacing.md) {
            Image(systemName: "exclamationmark.triangle")
                .font(.system(size: 48, weight: .thin))
                .foregroundStyle(AppColors.textSecondary)

            Text("Something went wrong")
                .font(AppTypography.sans(size: 20, weight: .semibold))
                .foregroundStyle(AppColors.textPrimary)

            Text(message)
                .font(AppTypography.body)
                .foregroundStyle(AppColors.textSecondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, Spacing.xl)
        }
    }
}

// MARK: - Returning User Splash

/// Static branded splash shown briefly over ContentView for returning users.
/// Matches the launch screen background, then fades out to reveal the app.
private struct ReturningSplashView: View {

    var body: some View {
        GeometryReader { geometry in
            ZStack {
                AppColors.background
                    .ignoresSafeArea()

                VStack(spacing: Spacing.lg) {
                    // Static playhead line — same 60% width as onboarding welcome
                    ZStack(alignment: .leading) {
                        Rectangle()
                            .fill(AppColors.accent)
                            .frame(height: 2)
                            .frame(width: geometry.size.width * 0.6)

                        Circle()
                            .fill(AppColors.accent)
                            .frame(width: 8, height: 8)
                            .offset(x: geometry.size.width * 0.6 - 4)
                    }
                    .frame(height: 8)

                    Text("Playhead")
                        .font(AppTypography.sans(size: 36, weight: .semibold))
                        .foregroundStyle(AppColors.textPrimary)
                }
            }
        }
    }
}
