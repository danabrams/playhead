import OSLog
import SwiftData
import SwiftUI

/// Top-level tab routes. The raw values are persisted across launches
/// only as a one-shot hint from onboarding (see
/// `OnboardingFlags.requestedInitialTabKey`); they are not the canonical
/// tab identifier in any other flow.
enum AppTab: Int, Hashable {
    case library = 0
    case browse = 1
    case activity = 2
    case settings = 3
}

struct ContentView: View {
    private static let logger = Logger(subsystem: "com.playhead", category: "Activity")

    @Environment(PlayheadRuntime.self) private var runtime
    @Environment(\.modelContext) private var modelContext
    @State private var showNowPlaying = false
    @State private var nowPlayingViewModel: NowPlayingViewModel?
    @State private var selectedTab: AppTab

    /// playhead-l274: shared deep-link router. Lives at the tab root so
    /// Library (hkg8 "Free up space" CTA) and Settings (consumer) share
    /// a single instance. Installed into the SwiftUI environment below.
    @State private var settingsRouter = SettingsRouter()

    /// playhead-1v8: ContentView reads a one-shot tab hint set by the
    /// onboarding search-prompt CTA. The hint is consumed exactly once
    /// (cleared during init); a future launch falls back to the
    /// default (Library).
    init(defaults: UserDefaults = .standard) {
        let initial = ContentView.consumeInitialTabHint(defaults: defaults)
        _selectedTab = State(initialValue: initial)
    }

    /// Reads-and-clears the tab hint set by `OnboardingFlowViewModel`.
    /// Static so the State initializer can call it without `self`.
    static func consumeInitialTabHint(defaults: UserDefaults) -> AppTab {
        guard let raw = defaults.string(forKey: OnboardingFlags.requestedInitialTabKey),
              let hint = OnboardingInitialTab(rawValue: raw) else {
            return .library
        }
        defaults.removeObject(forKey: OnboardingFlags.requestedInitialTabKey)
        switch hint {
        case .library: return .library
        case .browse: return .browse
        }
    }

    var body: some View {
        TabView(selection: $selectedTab) {
            tabRoot {
                LibraryView()
            }
                .tag(AppTab.library)
                .tabItem {
                    Label("Library", systemImage: "square.stack")
                }

            tabRoot {
                BrowseView()
            }
                .tag(AppTab.browse)
                .tabItem {
                    Label("Browse", systemImage: "magnifyingglass")
                }

            // playhead-quh7: Activity tab. Sibling of Library / Settings;
            // SF Symbol matches the design doc §E suggestion
            // (`chart.bar.doc.horizontal`).
            //
            // playhead-cjqq: persistQueueOrder writes drag-reorders
            // back to `Episode.queuePosition` so the next refresh
            // notification observes the new order. Fetches by
            // `canonicalEpisodeKey` (the same id the provider hands
            // out) and saves synchronously; the next
            // `ActivityRefreshNotification` post then re-reads the
            // updated rows.
            tabRoot {
                ActivityView(
                    inputProvider: { [runtime, modelContext] in
                        // playhead-hkn1: provider runs `loadInputs()`
                        // off-main from a fresh `ModelContext` it
                        // builds out of the container. We pass the
                        // container (Sendable) rather than the view's
                        // main-actor `modelContext`.
                        let provider = runtime.makeActivitySnapshotProvider(
                            modelContainer: modelContext.container
                        )
                        return await provider.loadInputs()
                    },
                    persistQueueOrder: { [modelContext] ordering in
                        for entry in ordering {
                            let episodeId = entry.episodeId
                            let descriptor = FetchDescriptor<Episode>(
                                predicate: #Predicate {
                                    $0.canonicalEpisodeKey == episodeId
                                }
                            )
                            do {
                                if let row = try modelContext.fetch(descriptor).first {
                                    row.queuePosition = entry.queuePosition
                                }
                            } catch {
                                Self.logger.error("queuePosition fetch failed for \(episodeId, privacy: .public): \(error.localizedDescription, privacy: .public)")
                            }
                        }
                        do {
                            try modelContext.save()
                        } catch {
                            // Silent failure would leave the in-memory snapshot
                            // ahead of persistence — drag would visually succeed
                            // but revert on next refresh. Log so a future
                            // telemetry consumer can surface this.
                            Self.logger.error("queuePosition save failed: \(error.localizedDescription, privacy: .public)")
                        }
                    }
                )
            }
                .tag(AppTab.activity)
                .tabItem {
                    Label("Activity", systemImage: "chart.bar.doc.horizontal")
                }

            tabRoot {
                SettingsView(
                    entitlementManager: runtime.entitlementManager,
                    router: settingsRouter
                )
            }
                .tag(AppTab.settings)
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
