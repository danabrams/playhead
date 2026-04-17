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

    var body: some Scene {
        WindowGroup {
            RootView()
                .environment(runtime)
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
    }
}

// MARK: - Root View

/// Switches between onboarding and main content based on first-launch state.
private struct RootView: View {

    @AppStorage("hasCompletedOnboarding") private var hasCompletedOnboarding = false
    @Environment(PlayheadRuntime.self) private var runtime
    @State private var showSplash = true

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
                }
            } else {
                OnboardingView()
            }
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
