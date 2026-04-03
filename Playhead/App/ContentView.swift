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
        .task {
            syncNowPlayingViewModel(isPlaying: runtime.isPlayingEpisode)
        }
        .onChange(of: runtime.isPlayingEpisode) { _, isPlaying in
            syncNowPlayingViewModel(isPlaying: isPlaying)
        }
        .fullScreenCover(isPresented: $showNowPlaying) {
            if let vm = nowPlayingViewModel {
                NowPlayingView(runtime: runtime, viewModel: vm)
            }
        }
    }

    private func syncNowPlayingViewModel(isPlaying: Bool) {
        if isPlaying {
            if let vm = nowPlayingViewModel {
                vm.startObserving()
            } else {
                let vm = NowPlayingViewModel(runtime: runtime)
                vm.startObserving()
                nowPlayingViewModel = vm
            }
        } else {
            nowPlayingViewModel?.stopObserving()
            nowPlayingViewModel = nil
            showNowPlaying = false
        }
    }
}

#Preview {
    ContentView()
        .environment(PlayheadRuntime(isPreviewRuntime: true))
        .preferredColorScheme(.dark)
        .modelContainer(for: [Podcast.self, Episode.self, UserPreferences.self], inMemory: true)
}
