import SwiftUI
import SwiftData

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

        let service = capabilitiesService
        Task {
            await service.runSelfTest()
        }
    }

    var body: some Scene {
        WindowGroup {
            ContentView()
        }
        .modelContainer(modelContainer)
    }
}
