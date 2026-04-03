import SwiftUI
import SwiftData

@main
struct PlayheadApp: App {
    let modelContainer: ModelContainer

    init() {
        do {
            modelContainer = try SwiftDataStore.makeContainer()
        } catch {
            fatalError("Failed to initialize SwiftData container: \(error)")
        }
    }

    var body: some Scene {
        WindowGroup {
            ContentView()
        }
        .modelContainer(modelContainer)
    }
}
