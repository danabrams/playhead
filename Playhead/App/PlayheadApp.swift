import SwiftUI
import SwiftData
import BackgroundTasks

@main
struct PlayheadApp: App {
    let modelContainer: ModelContainer
    let capabilitiesService = CapabilitiesService()

    init() {
        do {
            modelContainer = try SwiftDataStore.makeContainer()
        } catch {
            fatalError("Failed to initialize SwiftData container: \(error)")
        }

        capabilitiesService.startObserving()

        // Register background task identifiers before first scene render.
        // Handlers will be attached when BackgroundProcessingService is
        // fully initialized with its AnalysisCoordinator dependency.
        BackgroundProcessingService.registerTaskIdentifiers()

        let service = capabilitiesService
        Task {
            await service.runSelfTest()
        }
    }

    var body: some Scene {
        WindowGroup {
            RootView()
        }
        .modelContainer(modelContainer)
    }
}

// MARK: - Root View

/// Switches between onboarding and main content based on first-launch state.
private struct RootView: View {

    @AppStorage("hasCompletedOnboarding") private var hasCompletedOnboarding = false

    var body: some View {
        if hasCompletedOnboarding {
            ContentView()
        } else {
            OnboardingView()
        }
    }
}
