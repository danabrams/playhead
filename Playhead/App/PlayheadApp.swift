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

    /// playhead-05i: live playback queue. Constructed once here so
    /// every view that reaches into the environment for an enqueue
    /// affordance shares one persistent queue (rather than each view
    /// spinning its own actor). The auto-advance observer
    /// (`PlaybackQueueFinishObserver`) is wired in `.task` below — same
    /// scene scope, so it lives as long as the WindowGroup.
    @State private var playbackQueueController = PlaybackQueueController()

    var body: some Scene {
        WindowGroup {
            RootView()
                .environment(runtime)
                // playhead-05i: expose the live queue service to the
                // view tree so Library swipe actions and the NowPlaying
                // "Up Next" sheet share one persistent queue. When the
                // controller hasn't started yet (very first scene tick
                // before the `.task` below has run), the environment
                // value is `nil` and `makeEnqueueLast` / `makeEnqueueNext`
                // become no-ops — same null-safety the EnvironmentKey's
                // default value provides.
                .environment(\.playbackQueueService, playbackQueueController.service)
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
                    // playhead-snp: build the new-episode notification
                    // scheduler + SwiftData announcer adapter that the
                    // feed-refresh service will hop through after each
                    // refresh discovers new items. Holding it on the
                    // App scope (not in PlayheadRuntime) keeps the
                    // ModelContainer dependency local to the App-scope
                    // wiring step and matches the pattern used by the
                    // playback-queue controller above.
                    let newEpisodeScheduler = NewEpisodeNotificationScheduler(
                        scheduler: SystemNewEpisodeNotificationScheduler(),
                        authorizer: SystemNewEpisodeAuthorizationProvider(),
                        ledger: UserDefaultsNewEpisodeLedger()
                    )
                    let appWideEnabledProvider: @Sendable @MainActor () -> Bool = {
                        let context = modelContainer.mainContext
                        let prefs = (try? context.fetch(FetchDescriptor<UserPreferences>()).first)
                        return prefs?.newEpisodeNotificationsEnabled ?? true
                    }
                    let newEpisodeAnnouncer = SwiftDataNewEpisodeAnnouncer(
                        modelContainer: modelContainer,
                        scheduler: newEpisodeScheduler,
                        appWideEnabledProvider: appWideEnabledProvider
                    )

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
                        settingsProvider: ProductionDownloadsSettingsProvider(),
                        // playhead-5uvz.4 (Gap-5): rearm the backfill
                        // BGProcessingTask after a refresh that enqueues
                        // downloads so iOS wakes us to drain analysis
                        // even when the user never presses play.
                        backfillScheduler: ProductionBackfillScheduler(
                            backgroundProcessingService: runtime.backgroundProcessingService
                        ),
                        // playhead-shpy: share the BPS-owned telemetry
                        // logger so feed-refresh and backfill events
                        // land in the same `bg-task-log.jsonl` and a
                        // jq query can correlate by ts.
                        bgTelemetry: runtime.bgTaskTelemetryLogger,
                        newEpisodeAnnouncer: newEpisodeAnnouncer
                    )
                    BackgroundFeedRefreshService.attachSharedService(feedRefreshService)
                    feedRefreshService.start()

                    // playhead-05i: wire the playback queue + auto-advance.
                    // The play handler resolves a `canonicalEpisodeKey`
                    // back to a SwiftData `Episode` row and asks the
                    // runtime to play it — same path the user-initiated
                    // tap uses. If the episode no longer resolves (rare:
                    // dropped on a feed refresh), the handler is a
                    // silent no-op and the queue moves on.
                    playbackQueueController.start(
                        modelContainer: modelContainer,
                        playHandler: { @Sendable [runtime, modelContainer] episodeKey in
                            await MainActor.run {
                                let context = modelContainer.mainContext
                                let descriptor = FetchDescriptor<Episode>(
                                    predicate: #Predicate { $0.canonicalEpisodeKey == episodeKey }
                                )
                                guard let episode = try? context.fetch(descriptor).first else { return }
                                Task { await runtime.playEpisode(episode) }
                            }
                        }
                    )
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
            // playhead-gtt9.14: forward scene-phase transitions into the
            // scheduler so its admission filter distinguishes foreground
            // (user engaged with the app) from background (BPS owns the
            // wake window). `.inactive` folds into `.foreground` because
            // the user is still holding the device and a `.active` ↔
            // `.inactive` flicker during a system sheet must not strand
            // deferred work. Only a true `.background` transition flips
            // the scheduler's gate.
            let mappedPhase: AnalysisWorkScheduler.SchedulerScenePhase =
                (newPhase == .background) ? .background : .foreground
            Task {
                await runtime.analysisWorkScheduler.updateScenePhase(mappedPhase)
            }

            // playhead-shpy: stamp the phase transition into the
            // BG-task telemetry stream so jq queries can correlate
            // `submit` / `start` / `complete` rows against the
            // foreground/background boundary that immediately preceded
            // them. The from→to pair is captured here (not from
            // UIApplication.applicationState) because SwiftUI's
            // `scenePhase` is the canonical signal — the UIKit phase
            // can lag by one runloop tick around the transition.
            let bgTelemetry = runtime.bgTaskTelemetryLogger
            let oldPhaseString = Self.scenePhaseString(oldPhase)
            let newPhaseString = Self.scenePhaseString(newPhase)
            Task {
                await bgTelemetry.record(
                    .appPhase(from: oldPhaseString, to: newPhaseString)
                )
            }

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
                // playhead-fuo6: submit a backfill BGProcessingTask on
                // every `.background` transition so iOS has a registered
                // task to wake us with even when the user queued
                // episodes without ever pressing play. Pre-fix, the
                // only `scheduleBackfillIfNeeded` callers were
                // `playbackDidStop()` and the backfill handler's own
                // self-rearm, neither of which fires on a queued-but-
                // never-played session. Capture
                // `.captures/2026-04-25/...07:43.49.095` shows the
                // 12-hour overnight blackout this caused.
                Task {
                    await runtime.backgroundProcessingService.appDidEnterBackground()
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

    /// playhead-shpy: stable string encoding for SwiftUI's `ScenePhase`
    /// so the BG-task telemetry log uses a consistent vocabulary across
    /// every event regardless of which surface produced it.
    private static func scenePhaseString(_ phase: ScenePhase) -> String {
        switch phase {
        case .active:     return "active"
        case .inactive:   return "inactive"
        case .background: return "background"
        @unknown default: return "unknown"
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

// MARK: - Splash Controller

/// playhead-5nwy: hard splash dismiss. Schedules a fixed main-runloop
/// timer at construction so the splash flips off independent of any
/// async work, runtime readiness, or `@Published` state. Uses
/// `Timer.scheduledTimer` on the current (main) runloop — NOT a `Task`
/// — because Tasks ride the cooperative thread pool and can be starved
/// by saturated background work, which is exactly the failure mode
/// this defense exists to neutralize.
@MainActor
@Observable
final class SplashController {

    /// Fixed dismissal delay. Pinned as a named constant so tests and
    /// future reviewers can find the single source of truth for the
    /// "splash never lingers past N seconds" invariant.
    static let dismissDelay: TimeInterval = 1.2

    private(set) var isVisible: Bool = true

    @ObservationIgnored
    private var timer: Timer?

    init(dismissDelay: TimeInterval = SplashController.dismissDelay, autostart: Bool = true) {
        if autostart {
            scheduleDismissTimer(delay: dismissDelay)
        }
    }

    // The Timer is invalidated explicitly via `forceDismiss()` from
    // tests; in production the timer fires once and self-deallocates
    // (`repeats: false`), so a deinit-time invalidate is unnecessary.
    // Swift 6's nonisolated deinit cannot touch the MainActor-isolated
    // `timer` property anyway — keeping the lifecycle simple sidesteps
    // the isolation dance.

    private func scheduleDismissTimer(delay: TimeInterval) {
        // Timer.scheduledTimer attaches to the current runloop in
        // `.default` mode. The splash MUST also dismiss while the
        // runloop is in `.tracking` mode (e.g. user touch interaction)
        // and during modal sheets that switch to `.modal`, so we add
        // the timer to `.common` modes explicitly.
        let timer = Timer(timeInterval: delay, repeats: false) { [weak self] _ in
            guard let self else { return }
            MainActor.assumeIsolated {
                self.isVisible = false
            }
        }
        RunLoop.main.add(timer, forMode: .common)
        self.timer = timer
    }

    /// Force-dismiss the splash synchronously. Intended for tests; the
    /// production path always relies on the timer.
    func forceDismiss() {
        timer?.invalidate()
        timer = nil
        isVisible = false
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
    // playhead-5nwy: SplashController owns the fixed-timer dismiss path.
    // Constructed inline as @State so the timer fires the moment the
    // RootView struct is realized — the previous approach hung the
    // dismiss off `.onAppear`, which itself can be delayed by slow body
    // construction.
    @State private var splash = SplashController()
    @State private var presentFirstSubscriptionOnboarding = false

    var body: some View {
        Group {
            if let error = runtime.initializationError {
                errorView(error)
            } else if hasCompletedOnboarding {
                ZStack {
                    ContentView()

                    if splash.isVisible {
                        ReturningSplashView()
                            .transition(.opacity)
                            .zIndex(1)
                    }
                }
                .animation(.easeOut(duration: 0.4), value: splash.isVisible)
                .onAppear {
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
