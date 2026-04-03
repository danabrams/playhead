import SwiftUI
import SwiftData
import OSLog

@main
struct PlayheadApp: App {
    let modelContainer: ModelContainer
    @State private var runtime = PlayheadRuntime()
    @Environment(\.scenePhase) private var scenePhase

    private static let logger = Logger(subsystem: "com.playhead", category: "App")

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
        }
        .modelContainer(modelContainer)
        .onChange(of: scenePhase) { oldPhase, newPhase in
            switch newPhase {
            case .background:
                Self.logger.info("Scene phase: background — persisting playback position")
                Task { @MainActor in
                    guard let captured = await runtime.capturePlaybackPosition() else { return }
                    let context = modelContainer.mainContext
                    let episodeId = captured.episodeId
                    let descriptor = FetchDescriptor<Episode>(
                        predicate: #Predicate { $0.canonicalEpisodeKey == episodeId }
                    )
                    if let episode = try? context.fetch(descriptor).first {
                        episode.playbackPosition = captured.position
                        try? context.save()
                        Self.logger.info("Saved position \(captured.position)s for episode \(episodeId)")
                    }
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
}

// MARK: - Root View

/// Switches between onboarding and main content based on first-launch state.
private struct RootView: View {

    @AppStorage("hasCompletedOnboarding") private var hasCompletedOnboarding = false
    @Environment(PlayheadRuntime.self) private var runtime

    var body: some View {
        Group {
            if let error = runtime.initializationError {
                errorView(error)
            } else if hasCompletedOnboarding {
                ContentView()
            } else {
                OnboardingView()
            }
        }
    }

    private func errorView(_ message: String) -> some View {
        VStack(spacing: Spacing.md) {
            Image(systemName: "exclamationmark.triangle")
                .font(.system(size: 48, weight: .thin))
                .foregroundStyle(AppColors.secondary)

            Text("Something went wrong")
                .font(AppTypography.sans(size: 20, weight: .semibold))
                .foregroundStyle(AppColors.text)

            Text(message)
                .font(AppTypography.body)
                .foregroundStyle(AppColors.secondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, Spacing.xl)
        }
    }
}
