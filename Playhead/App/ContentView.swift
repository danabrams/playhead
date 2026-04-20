import SwiftUI

struct ContentView: View {
    @Environment(PlayheadRuntime.self) private var runtime
    @State private var showNowPlaying = false
    @State private var nowPlayingViewModel: NowPlayingViewModel?

    /// playhead-l274: shared deep-link router. Lives at the tab root so
    /// Library (hkg8 "Free up space" CTA) and Settings (consumer) share
    /// a single instance. Installed into the SwiftUI environment below.
    @State private var settingsRouter = SettingsRouter()

    var body: some View {
        TabView {
            tabRoot {
                LibraryView()
            }
                .tabItem {
                    Label("Library", systemImage: "square.stack")
                }

            tabRoot {
                BrowseView()
            }
                .tabItem {
                    Label("Browse", systemImage: "magnifyingglass")
                }

            tabRoot {
                SettingsView(
                    inventory: runtime.modelInventory,
                    assetProvider: runtime.assetProvider,
                    entitlementManager: runtime.entitlementManager,
                    router: settingsRouter
                )
            }
                .tabItem {
                    Label("Settings", systemImage: "gearshape")
                }
        }
        .environment(\.settingsRouter, settingsRouter)
        .tint(AppColors.accent)
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

    private func tabRoot<Content: View>(
        @ViewBuilder content: () -> Content
    ) -> some View {
        content()
            .safeAreaInset(edge: .bottom, spacing: 0) {
                miniPlayerInset
            }
    }

    @ViewBuilder
    private var miniPlayerInset: some View {
        if runtime.isPlayingEpisode, let vm = nowPlayingViewModel {
            NowPlayingBar(
                viewModel: vm,
                onTap: {
                    showNowPlaying = true
                }
            )
            .padding(.bottom, Spacing.xs)
            .transition(.move(edge: .bottom).combined(with: .opacity))
            .accessibilityElement(children: .combine)
            .accessibilityLabel("Now playing: \(runtime.currentEpisodeTitle ?? "Episode")")
            .accessibilityHint("Tap to open full player")
        }
    }
}

#Preview {
    ContentView()
        .environment(PlayheadRuntime(isPreviewRuntime: true))
        .preferredColorScheme(.dark)
        .modelContainer(for: [Podcast.self, Episode.self, UserPreferences.self], inMemory: true)
}
