import SwiftUI

struct ContentView: View {
    @Environment(PlayheadRuntime.self) private var runtime
    @State private var showNowPlaying = false
    @State private var nowPlayingViewModel: NowPlayingViewModel?

    var body: some View {
        ZStack(alignment: .bottom) {
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

            // NowPlayingBar overlay — shown when an episode is loaded
            if runtime.isPlayingEpisode, let vm = nowPlayingViewModel {
                NowPlayingBar(
                    viewModel: vm,
                    onTap: {
                        showNowPlaying = true
                    }
                )
                .transition(.move(edge: .bottom).combined(with: .opacity))
                .safeAreaInset(edge: .bottom) { Color.clear.frame(height: 0) }
                .accessibilityElement(children: .combine)
                .accessibilityLabel("Now playing: \(runtime.currentEpisodeTitle ?? "Episode")")
                .accessibilityHint("Tap to open full player")
            }
        }
        .animation(Motion.standard, value: runtime.isPlayingEpisode)
        .onChange(of: runtime.isPlayingEpisode) { _, isPlaying in
            if isPlaying, nowPlayingViewModel == nil {
                nowPlayingViewModel = NowPlayingViewModel(runtime: runtime)
            } else if !isPlaying {
                nowPlayingViewModel = nil
            }
        }
        .fullScreenCover(isPresented: $showNowPlaying) {
            if let vm = nowPlayingViewModel {
                NowPlayingView(runtime: runtime, viewModel: vm)
            }
        }
    }
}

#Preview {
    ContentView()
        .environment(PlayheadRuntime(isPreviewRuntime: true))
        .preferredColorScheme(.dark)
        .modelContainer(for: [Podcast.self, Episode.self, UserPreferences.self], inMemory: true)
}
