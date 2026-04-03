import SwiftUI

struct ContentView: View {
    @EnvironmentObject private var runtime: PlayheadRuntime

    var body: some View {
        TabView {
            LibraryView()
                .tabItem {
                    Label("Library", systemImage: "square.stack")
                }

            BrowseView()
                .tabItem {
                    Label("Browse", systemImage: "magnifyingglass")
                }

            SettingsView(
                inventory: runtime.modelInventory,
                assetProvider: runtime.assetProvider,
                entitlementManager: runtime.entitlementManager
            )
                .tabItem {
                    Label("Settings", systemImage: "gearshape")
                }
        }
        .tint(AppColors.accent)
    }
}

#Preview {
    ContentView()
        .environmentObject(PlayheadRuntime(isPreviewRuntime: true))
        .preferredColorScheme(.dark)
        .modelContainer(for: [Podcast.self, Episode.self, UserPreferences.self], inMemory: true)
}
