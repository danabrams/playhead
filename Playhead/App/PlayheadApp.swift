import SwiftUI
import SwiftData

@main
struct PlayheadApp: App {
    let modelContainer: ModelContainer
    let runtime = PlayheadRuntime()

    init() {
        do {
            modelContainer = try SwiftDataStore.makeContainer()
        } catch {
            fatalError("Failed to initialize SwiftData container: \(error)")
        }
    }

    var body: some Scene {
        WindowGroup {
            RootView()
                .environmentObject(runtime)
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
