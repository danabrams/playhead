// EpisodeListView.swift
// Per-podcast episode list with text-led layout.
// Sorted newest-first, with swipe actions for play, queue, and mark played.

import SwiftUI
import SwiftData

// MARK: - EpisodeRefreshing

/// Narrow seam over the feed-refresh behaviour the per-show list needs
/// for pull-to-refresh (playhead-riu8). Keeps the SwiftUI layer free
/// of URLSession / provider mocking when tests exercise the refresh
/// path — prod wires in `PodcastDiscoveryService`; tests inject a
/// recording double. Mirrors the `HapticPlaying` seam pattern already
/// used by this view.
protocol EpisodeRefreshing: Sendable {
    @MainActor
    func refreshEpisodes(
        for podcast: Podcast,
        in context: ModelContext
    ) async throws -> [Episode]
}

// MARK: - EpisodeListView

struct EpisodeListView: View {

    let podcast: Podcast

    /// Injected haptic player — defaults to `SystemHapticPlayer` in
    /// production, tests swap in a `RecordingHapticPlayer`.
    var hapticPlayer: any HapticPlaying = SystemHapticPlayer()

    /// Injected feed refresher — defaults to a shared
    /// `PodcastDiscoveryService` in production, tests swap in a
    /// recording double. Drives the pull-to-refresh closure below.
    var episodeRefresher: any EpisodeRefreshing = PodcastDiscoveryService()

    @Query private var episodes: [Episode]

    @Environment(\.modelContext) private var modelContext
    @Environment(PlayheadRuntime.self) private var runtime

    /// playhead-l274: shared deep-link router for Settings. When the
    /// amber "Free up space →" CTA is tapped we push `.storage`; the
    /// Settings tab observes and scrolls on next appearance.
    @Environment(\.settingsRouter) private var settingsRouter

    /// playhead-05i: live playback-queue service exposed at the App
    /// scene scope. When `nil` (preview / test contexts that don't set
    /// the environment), the swipe actions become no-ops with haptic
    /// feedback rather than crashing.
    @Environment(\.playbackQueueService) private var playbackQueueService

    @State private var navigateToNowPlaying = false
    @State private var selectedEpisode: Episode?

    /// playhead-jzik: per-row expansion state. The set holds the
    /// canonicalEpisodeKeys whose row is currently expanded — the row
    /// view consults this to render the optional summary block. Kept
    /// at the list level (rather than inside `EpisodeRow`) because
    /// SwiftData rebuilds row instances on @Query updates and a
    /// row-local @State would forget the user's expansion across
    /// re-renders.
    @State private var expandedEpisodeKeys: Set<String> = []

    /// Tracks whether the user has already dismissed the first-✓
    /// tooltip. Persisted via UserDefaults; see
    /// `OnboardingFlags.firstCheckmarkTooltipSeenKey`.
    @AppStorage(OnboardingFlags.firstCheckmarkTooltipSeenKey)
    private var hasSeenFirstCheckmarkTooltip: Bool = false

    /// Drives the tooltip overlay visibility. Separate from the
    /// persisted flag so a fade-out animation can run before the
    /// tooltip is removed from the view hierarchy.
    @State private var showsFirstCheckmarkTooltip: Bool = false

    init(
        podcast: Podcast,
        hapticPlayer: any HapticPlaying = SystemHapticPlayer(),
        episodeRefresher: any EpisodeRefreshing = PodcastDiscoveryService()
    ) {
        self.podcast = podcast
        self.hapticPlayer = hapticPlayer
        self.episodeRefresher = episodeRefresher
        let podcastID = podcast.persistentModelID
        _episodes = Query(
            filter: #Predicate<Episode> { episode in
                episode.podcast?.persistentModelID == podcastID
            },
            sort: [SortDescriptor(\Episode.publishedAt, order: .reverse)]
        )
    }

    /// playhead-05i: append `episode` to the tail of the playback
    /// queue. Fire-and-forget — the swipe gesture should never block.
    /// Errors are swallowed (the queue service surfaces only
    /// programmer-error throws like duplicate-key races).
    func queueEpisode(_ episode: Episode) {
        hapticPlayer.play(.save)
        let key = episode.canonicalEpisodeKey
        guard let service = playbackQueueService else { return }
        Task { try? await service.addLast(episodeKey: key) }
    }

    /// playhead-05i: insert `episode` at the head of the queue so it
    /// plays immediately after the current episode finishes. Same
    /// fire-and-forget shape as `queueEpisode(_:)`.
    func playNextEpisode(_ episode: Episode) {
        hapticPlayer.play(.save)
        let key = episode.canonicalEpisodeKey
        guard let service = playbackQueueService else { return }
        Task { try? await service.addNext(episodeKey: key) }
    }

    var body: some View {
        ZStack {
            AppColors.background
                .ignoresSafeArea()

            if episodes.isEmpty {
                emptyState
            } else {
                episodeList
            }

            if showsFirstCheckmarkTooltip {
                FirstCheckmarkTooltipView(onDismiss: dismissFirstCheckmarkTooltip)
                    .zIndex(1)
            }
        }
        .animation(Motion.standard, value: showsFirstCheckmarkTooltip)
        .navigationTitle(podcast.title)
        .navigationBarTitleDisplayMode(.large)
        .fullScreenCover(isPresented: $navigateToNowPlaying) {
            NowPlayingView(runtime: runtime)
        }
        .onAppear {
            evaluateFirstCheckmarkTooltip()
        }
        .onChange(of: anyEpisodeHasAnalysis) { _, _ in
            evaluateFirstCheckmarkTooltip()
        }
    }

    // MARK: - First ✓ Tooltip

    /// True iff at least one episode in the current list has a ready
    /// checkmark badge (`analysisSummary.hasAnalysis == true`). Drives
    /// the first-✓ tooltip trigger.
    private var anyEpisodeHasAnalysis: Bool {
        episodes.contains { $0.analysisSummary?.hasAnalysis == true }
    }

    /// Shows the tooltip on list appear (and on state changes) if the
    /// user has never dismissed it and a ✓ badge is visible. The
    /// boolean gate lives in `OnboardingGating` (pure) for testability;
    /// the `showsFirstCheckmarkTooltip` bookkeeping stays here because
    /// it is SwiftUI view state.
    private func evaluateFirstCheckmarkTooltip() {
        if hasSeenFirstCheckmarkTooltip {
            if showsFirstCheckmarkTooltip { showsFirstCheckmarkTooltip = false }
            return
        }
        let shouldShow = OnboardingGating.shouldPresentFirstCheckmarkTooltip(
            hasSeenFirstCheckmarkTooltip: hasSeenFirstCheckmarkTooltip,
            anyEpisodeHasAnalysis: anyEpisodeHasAnalysis
        )
        if shouldShow, !showsFirstCheckmarkTooltip {
            showsFirstCheckmarkTooltip = true
        }
    }

    /// Persists the dismissal and hides the overlay. Called from the
    /// tooltip's `onDismiss` callback.
    private func dismissFirstCheckmarkTooltip() {
        hasSeenFirstCheckmarkTooltip = true
        showsFirstCheckmarkTooltip = false
    }
}

// MARK: - Subviews

private extension EpisodeListView {

    // MARK: Empty State

    var emptyState: some View {
        VStack(spacing: Spacing.md) {
            Image(systemName: "waveform")
                .font(.system(size: 48, weight: .thin))
                .foregroundStyle(AppColors.textSecondary)
                .accessibilityHidden(true)

            Text("No Episodes")
                .font(AppTypography.sans(size: 20, weight: .semibold))
                .foregroundStyle(AppColors.textPrimary)

            Text("Pull to refresh, or episodes will appear after the next feed sync.")
                .font(AppTypography.body)
                .foregroundStyle(AppColors.textSecondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal, Spacing.xl)
        }
    }

    // MARK: Episode List

    var episodeList: some View {
        List {
            Section {
                DownloadNextView(
                    episodes: episodes,
                    mediaCapBytes: StorageBudgetSettings.load().mediaCapBytes,
                    onDownload: { picked, context in
                        // playhead-zp0x: non-Generic submits create a
                        // persistent `DownloadBatch` row and trigger
                        // the single notification-permission ask
                        // (gated by `UserPreferences.notificationPermissionAsked`).
                        // Generic submits are unchanged (no row, no
                        // permission ask) — the v1 scheduler-behavior
                        // contract from playhead-hkg8 still holds.
                        if context != .generic {
                            DownloadBatchAdmission.admit(
                                episodes: picked,
                                context: context,
                                modelContext: modelContext
                            )
                        }
                        Task {
                            for episode in picked {
                                await runtime.downloadManager.backgroundDownload(
                                    episodeId: episode.canonicalEpisodeKey,
                                    from: episode.audioURL
                                )
                            }
                        }
                    },
                    onFreeUpSpace: {
                        // playhead-l274: push `.storage` into the shared
                        // `SettingsRouter`. The Settings tab's
                        // `SettingsView` observes `pending` and scrolls
                        // the list to the Storage group anchor; the
                        // router is cleared via `consume()` once the
                        // scroll is honored.
                        settingsRouter?.request(.storage)
                    }
                )
                .listRowBackground(Color.clear)
                .listRowSeparator(.hidden)
                .listRowInsets(EdgeInsets(
                    top: Spacing.sm,
                    leading: Spacing.md,
                    bottom: Spacing.sm,
                    trailing: Spacing.md
                ))
            }

            ForEach(episodes) { episode in
                EpisodeRow(
                    episode: episode,
                    isExpanded: expandedEpisodeKeys.contains(episode.canonicalEpisodeKey),
                    onPlay: { playEpisode(episode) }
                )
                    .listRowBackground(AppColors.background)
                    .listRowSeparatorTint(AppColors.textSecondary.opacity(0.2))
                    .contentShape(Rectangle())
                    .onTapGesture {
                        // playhead-jzik: tap expands/collapses the row.
                        // The play action moves to the leading swipe (and
                        // the explicit Play button rendered when the row
                        // is expanded). Same gesture is unaffected by the
                        // accessibility label change below.
                        toggleExpansion(for: episode)
                    }
                    .swipeActions(edge: .leading) {
                        Button {
                            playEpisode(episode)
                        } label: {
                            Label("Play", systemImage: "play.fill")
                        }
                        .tint(AppColors.accent)
                    }
                    .swipeActions(edge: .trailing) {
                        Button {
                            togglePlayed(episode)
                        } label: {
                            Label(
                                episode.isPlayed ? "Unplayed" : "Played",
                                systemImage: episode.isPlayed
                                    ? "circle" : "checkmark.circle.fill"
                            )
                        }
                        .tint(AppColors.textSecondary)

                        // playhead-05i: "Play Last" = append to the
                        // tail of the queue.
                        Button {
                            queueEpisode(episode)
                        } label: {
                            Label("Play Last", systemImage: "text.badge.plus")
                        }
                        .tint(Palette.mutedSage)

                        // playhead-05i: "Play Next" = insert at the
                        // head of the queue so the user's current
                        // episode is followed by this one.
                        Button {
                            playNextEpisode(episode)
                        } label: {
                            Label("Play Next", systemImage: "text.insert")
                        }
                        .tint(AppColors.accent)
                    }
                    .accessibilityElement(children: .combine)
                    .accessibilityLabel("\(episode.title)\(episode.isPlayed ? ", played" : "")")
                    .accessibilityHint("Tap to expand. Swipe right to play.")
            }
        }
        .listStyle(.plain)
        .scrollContentBackground(.hidden)
        .refreshable {
            await performEpisodeRefresh(
                refresher: episodeRefresher,
                podcast: podcast,
                modelContext: modelContext
            )
        }
    }

    // MARK: - Actions

    func playEpisode(_ episode: Episode) {
        selectedEpisode = episode
        Task {
            await runtime.playEpisode(episode)
        }
        navigateToNowPlaying = true
    }

    func togglePlayed(_ episode: Episode) {
        episode.isPlayed.toggle()
    }

    /// playhead-jzik: toggle the expansion state for the supplied
    /// episode's row. Animated with `Motion.standard` so the disclosure
    /// reads as a smooth reveal of the editorial summary block.
    func toggleExpansion(for episode: Episode) {
        let key = episode.canonicalEpisodeKey
        withAnimation(Motion.standard) {
            if expandedEpisodeKeys.contains(key) {
                expandedEpisodeKeys.remove(key)
            } else {
                expandedEpisodeKeys.insert(key)
            }
        }
    }
}

// MARK: - Episode Row

private struct EpisodeRow: View {

    let episode: Episode
    /// playhead-jzik: whether this row is currently expanded. Owned by
    /// `EpisodeListView` so the set survives @Query rebuilds; the row
    /// itself is purely a function of (episode, isExpanded).
    let isExpanded: Bool
    /// playhead-jzik: invoked when the user taps the explicit "Play"
    /// affordance inside the expanded body. The row itself doesn't have
    /// the runtime (the list does), so we route the action up.
    let onPlay: () -> Void

    /// playhead-jzik: lazy-loaded persisted summary. Populated on first
    /// expansion via the AnalysisStore lookup chain
    /// `episodeId → analysisAssetId → episode_summaries`. Cached for the
    /// life of this row instance so subsequent collapse/expand cycles
    /// stay within the <50ms render budget. Stays `nil` when no summary
    /// has been written yet (the backfill coordinator hasn't gotten to
    /// this asset, or coverage is still <80%).
    @State private var fetchedSummary: EpisodeSummary?
    /// playhead-9u0: analysis asset id for this episode, resolved alongside
    /// the summary lookup. Drives the "Read transcript" navigation link in
    /// the expanded body — the link becomes available once the row's analysis
    /// asset has been located. Stays nil for episodes whose analysis hasn't
    /// produced an asset row yet.
    @State private var resolvedAnalysisAssetId: String?
    @Environment(PlayheadRuntime.self) private var runtime

    var body: some View {
        VStack(alignment: .leading, spacing: Spacing.xs) {
            // Title (serif per spec)
            Text(episode.title)
                .font(AppTypography.serif(size: 17, weight: .regular))
                .foregroundStyle(episode.isPlayed ? AppColors.textSecondary : AppColors.textPrimary)
                .lineLimit(2)

            // Date and duration (mono)
            HStack(spacing: Spacing.sm) {
                if let date = episode.publishedAt {
                    Text(Self.formatEpisodeDate(date))
                        .font(AppTypography.timestamp)
                        .foregroundStyle(AppColors.textTertiary)
                }

                if let duration = episode.duration {
                    Text(TimeFormatter.formatDuration(duration))
                        .font(AppTypography.timestamp)
                        .foregroundStyle(AppColors.textTertiary)
                }

                Spacer()

                // Readiness status (playhead-cthe)
                //
                // The ✓ affordance is a DERIVED view of
                // `(coverageSummary, playbackAnchor)`. Per the Phase 2
                // spec, we render the checkmark only for `.proximal` or
                // `.complete` — the two states where starting playback
                // now yields a usable skip-prepared experience. A
                // `.deferredOnly` episode has analysis somewhere, but
                // not near the current playback point, so showing a ✓
                // would mislead the user into thinking ads will be
                // skipped from the start.
                if libraryRowShouldShowReadinessCheckmark(episode: episode) {
                    Image(systemName: "checkmark.circle")
                        .font(.system(size: 12, weight: .medium))
                        .foregroundStyle(Palette.mutedSage)
                        .accessibilityLabel("Analysis complete")
                }

                // Ad count — small copper numeral (not a badge)
                if let summary = episode.analysisSummary, summary.adSegmentCount > 0 {
                    Text("\(summary.adSegmentCount)")
                        .font(AppTypography.mono(size: 12, weight: .semibold))
                        .foregroundStyle(AppColors.accent)
                        .accessibilityLabel("\(summary.adSegmentCount) ad segments detected")
                }
            }

            // Status line (playhead-zp5y).
            //
            // Sourced from `EpisodeSurfaceStatus` via the pure copy
            // resolver — never from raw scheduler internals. Surfaced
            // only when the status carries a meaningful analysis signal
            // (complete / proximal / deferredOnly); suppressed for the
            // default `.none` readiness to avoid adding a "Queued ·
            // waiting" line to every un-analyzed row. The full 7-case
            // rendering is exercised in EpisodeStatusLineCopyTests and
            // will be surfaced end-to-end when a proper episode detail
            // screen lands in a subsequent bead.
            if let inputs = libraryRowStatusLineInputs(episode: episode) {
                EpisodeStatusLineView(
                    status: inputs.status,
                    coverage: inputs.coverage,
                    anchor: inputs.anchor
                )
            }

            // Progress bar for partially played episodes
            if !episode.isPlayed, episode.playbackPosition > 0,
               let duration = episode.duration, duration > 0
            {
                GeometryReader { geo in
                    let fraction = min(episode.playbackPosition / duration, 1.0)
                    ZStack(alignment: .leading) {
                        RoundedRectangle(cornerRadius: 1)
                            .fill(AppColors.textSecondary.opacity(0.2))
                            .frame(height: 2)
                        RoundedRectangle(cornerRadius: 1)
                            .fill(AppColors.accent)
                            .frame(width: geo.size.width * fraction, height: 2)
                    }
                }
                .frame(height: 2)
                .accessibilityValue("Progress: \(Int(min(episode.playbackPosition / (episode.duration ?? 1), 1.0) * 100)) percent")
            }

            // playhead-jzik: expanded summary body. Only mounted when
            // `isExpanded` is true; the lazy `.task` below is responsible
            // for populating `fetchedSummary` on first reveal.
            if isExpanded {
                expandedBody
            }
        }
        .padding(.vertical, Spacing.xs)
        // playhead-jzik: load the summary the first time this row
        // expands. `.task(id:)` handles both initial mount and the
        // collapse→expand transition; SwiftUI tears the task down on
        // collapse so the in-flight read can be discarded mid-fetch
        // without leaking. We guard on `fetchedSummary == nil` so that
        // expanding the same row twice (after a collapse) doesn't
        // re-issue the SQLite query.
        .task(id: isExpanded) {
            guard isExpanded, fetchedSummary == nil else { return }
            await loadSummary()
        }
    }

    // MARK: - Expanded Body (playhead-jzik)

    /// Editorial expanded section rendered when the row is open. Three
    /// stacked elements:
    ///
    ///   1. Summary prose (serif). Either the FM-generated 2–3 sentence
    ///      blurb or, while the backfill coordinator hasn't gotten to
    ///      this asset, a quiet placeholder line.
    ///   2. Topic pills (Soft Steel, capped at `EpisodeSummary.visibleTopicCap`).
    ///      Hidden entirely when there are no topics — we don't want an
    ///      empty pill row to draw the eye.
    ///   3. Explicit "Play" affordance (Copper). The tap target moved
    ///      from the row to the disclosure because the tap gesture now
    ///      handles expansion; this restores the always-available play
    ///      action without forcing the user to swipe.
    @ViewBuilder
    private var expandedBody: some View {
        VStack(alignment: .leading, spacing: Spacing.sm) {
            if let summary = fetchedSummary, !summary.summary.isEmpty {
                Text(summary.summary)
                    .font(AppTypography.serif(size: 15, weight: .regular))
                    .foregroundStyle(AppColors.textPrimary)
                    .fixedSize(horizontal: false, vertical: true)

                let topics = Array(summary.mainTopics.prefix(EpisodeSummary.visibleTopicCap))
                if !topics.isEmpty {
                    HStack(spacing: Spacing.xs) {
                        ForEach(topics, id: \.self) { topic in
                            topicPill(topic)
                        }
                    }
                }
            } else {
                Text("Summary will appear once analysis is ready.")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textSecondary)
            }

            HStack(spacing: Spacing.lg) {
                Button(action: onPlay) {
                    Label("Play", systemImage: "play.fill")
                        .font(AppTypography.sans(size: 14, weight: .semibold))
                        .foregroundStyle(AppColors.accent)
                }
                .buttonStyle(.plain)
                .accessibilityIdentifier("EpisodeRow.expanded.playButton")

                // playhead-9u0: full-screen transcript reader. Available
                // once the row has resolved an analysis asset id (so the
                // FullTranscriptViewModel has something to fetch). When
                // resolution is still pending the link is omitted rather
                // than rendered disabled — the row collapses cleanly to
                // just "Play" until analysis catches up.
                if let assetId = resolvedAnalysisAssetId {
                    NavigationLink {
                        LibraryFullTranscriptHost(
                            episode: episode,
                            analysisAssetId: assetId
                        )
                    } label: {
                        Label("Transcript", systemImage: "text.justify.left")
                            .font(AppTypography.sans(size: 14, weight: .semibold))
                            .foregroundStyle(AppColors.textSecondary)
                    }
                    .buttonStyle(.plain)
                    .accessibilityIdentifier("EpisodeRow.expanded.transcriptLink")
                }
            }
        }
        .padding(.top, Spacing.xs)
        .transition(.opacity.combined(with: .move(edge: .top)))
    }

    /// Single topic pill — Soft Steel background, sans label. Sized to
    /// hug the text so the row's three pills wrap naturally rather than
    /// creating a fixed grid.
    @ViewBuilder
    private func topicPill(_ text: String) -> some View {
        Text(text)
            .font(AppTypography.sans(size: 12, weight: .medium))
            .foregroundStyle(AppColors.textPrimary)
            .padding(.horizontal, Spacing.sm)
            .padding(.vertical, 4)
            .background(
                Capsule().fill(Palette.softSteel.opacity(0.25))
            )
            .accessibilityLabel("Topic: \(text)")
    }

    /// playhead-jzik: walk the lookup chain
    /// `episodeId → analysisAssetId → episode_summaries`. All three hops
    /// are cheap (indexed lookups) so a single async chain on .task is
    /// fine — no need to push work onto a queue. Failures are silent;
    /// the placeholder copy carries the empty-state message.
    private func loadSummary() async {
        let store = runtime.analysisStore
        let key = episode.canonicalEpisodeKey
        let summary: EpisodeSummary?
        let assetId: String
        do {
            guard let asset = try await store.fetchAssetByEpisodeId(key) else {
                return
            }
            assetId = asset.id
            summary = try await store.fetchEpisodeSummary(assetId: asset.id)
        } catch {
            return
        }
        // Resolve the asset id even when no summary has been written yet
        // (the transcript may exist before the summary backfill catches up).
        await MainActor.run { self.resolvedAnalysisAssetId = assetId }
        if let summary {
            await MainActor.run { self.fetchedSummary = summary }
        }
    }

    // MARK: - Formatting

    /// Compact episode date: "Mar 15" for current year, "Mar 15, 2024" for older.
    private static func formatEpisodeDate(_ date: Date) -> String {
        let calendar = Calendar.current
        let episodeYear = calendar.component(.year, from: date)
        let currentYear = calendar.component(.year, from: .now)

        if episodeYear < currentYear {
            return date.formatted(.dateTime.month(.abbreviated).day().year())
        }
        return date.formatted(.dateTime.month(.abbreviated).day())
    }
}

// MARK: - Library Row Readiness (playhead-cthe)

/// `true` when a Library row should render the ✓ affordance for the
/// supplied episode. Routes through
/// `derivePlaybackReadiness(coverage:anchor:)` so every readiness
/// decision in the app uses the same pure function (NowPlaying /
/// Activity / Library cannot drift).
///
/// Exposed at file scope (rather than nested inside the private
/// `EpisodeRow`) so the behavioral readiness test can exercise it
/// directly without instantiating SwiftUI's @Query / ModelContext
/// environment. The function has no SwiftUI dependency — it reads two
/// Codable attributes off the Episode and computes a Bool.
func libraryRowShouldShowReadinessCheckmark(episode: Episode) -> Bool {
    let readiness = derivePlaybackReadiness(
        coverage: episode.coverageSummary,
        anchor: episode.playbackAnchor
    )
    switch readiness {
    case .proximal, .complete:
        return true
    case .none, .deferredOnly:
        return false
    }
}

// MARK: - Library Row Status Line (playhead-zp5y)

/// Inputs the library row needs in order to mount
/// `EpisodeStatusLineView` for a given episode. Bundles the synthesized
/// `EpisodeSurfaceStatus` with the same `coverage` / `anchor` pair the
/// reducer consumed so the view's copy resolver can compute the
/// "first|next X min" branch without re-deriving inputs.
struct LibraryRowStatusLineInputs: Equatable {
    let status: EpisodeSurfaceStatus
    let coverage: CoverageSummary?
    let anchor: TimeInterval?
}

/// Compute the optional status-line inputs a library row should surface
/// for the supplied episode. Returns `nil` for the default
/// `.none` readiness so the row stays visually quiet for every
/// un-analyzed episode.
///
/// Routes through the canonical reducer — the UI layer never reaches
/// past the `EpisodeSurfaceStatus` boundary to the raw scheduler cause
/// taxonomy. Because library rows don't have live eligibility or cause
/// data in hand, the reducer is invoked with conservative defaults
/// (fully-eligible device, no live cause) which land in the
/// readiness-driven branch — exactly the one library rows care about.
///
/// String resolution lives inside `EpisodeStatusLineView`; this function
/// stops at the inputs so the view can own its own copy lookup. Exposed
/// at file scope (like `libraryRowShouldShowReadinessCheckmark`) so a
/// behavioural test can exercise it without SwiftUI's environment.
func libraryRowStatusLineInputs(episode: Episode) -> LibraryRowStatusLineInputs? {
    // Only render the line for episodes carrying an analysis signal.
    // The `.none` branch (no coverage yet) would say "Queued · waiting"
    // for every un-analyzed episode in the list; suppress it at the
    // row boundary instead.
    let readiness = derivePlaybackReadiness(
        coverage: episode.coverageSummary,
        anchor: episode.playbackAnchor
    )
    switch readiness {
    case .none:
        return nil
    case .deferredOnly, .proximal, .complete:
        break
    }

    let state = AnalysisState(
        persistedStatus: .queued,
        hasUserPreemptedJob: false,
        hasAppForceQuitFlag: false,
        pendingSinceEnqueuedAt: nil,
        hasAnyConfirmedAnalysis: episode.coverageSummary != nil
    )
    let eligibility = AnalysisEligibility(
        hardwareSupported: true,
        appleIntelligenceEnabled: true,
        regionSupported: true,
        languageSupported: true,
        modelAvailableNow: true,
        capturedAt: Date()
    )
    let status = episodeSurfaceStatus(
        state: state,
        cause: nil,
        eligibility: eligibility,
        coverage: episode.coverageSummary,
        readinessAnchor: episode.playbackAnchor
    )
    return LibraryRowStatusLineInputs(
        status: status,
        coverage: episode.coverageSummary,
        anchor: episode.playbackAnchor
    )
}

// MARK: - Full Transcript Host (playhead-9u0)

/// Hosts the `FullTranscriptView` from a Library row's NavigationLink
/// destination. The wrapper exists to bridge a *frozen* call-site value
/// (the row was created at one point in the @Query render) and the
/// *live* playback time — when the row's episode happens to be the one
/// currently playing, we observe `PlaybackService.observeStates()` so
/// the transcript can auto-follow. When the row's episode is NOT the
/// currently-playing one (the common Library "browse" case) we leave
/// `currentTime` at 0 and the view simply renders without a highlight,
/// matching the bead's spec for browse-mode entry.
private struct LibraryFullTranscriptHost: View {

    let episode: Episode
    let analysisAssetId: String
    @Environment(PlayheadRuntime.self) private var runtime

    /// Live playback time in seconds. Only updates while the row's
    /// episode matches `runtime.currentEpisodeId`; otherwise stays at 0.
    @State private var currentTime: TimeInterval = 0
    @State private var observationTask: Task<Void, Never>?

    var body: some View {
        FullTranscriptView(
            viewModel: FullTranscriptViewModel(
                analysisAssetId: analysisAssetId,
                dataSource: LiveTranscriptPeekDataSource(
                    store: runtime.analysisStore
                )
            ),
            currentTime: currentTime,
            duration: episode.duration ?? 0,
            onSeek: { seekTime in
                Task {
                    await runtime.playEpisode(episode)
                    await runtime.seek(to: seekTime)
                }
            }
        )
        .task {
            await beginObservingIfCurrent()
        }
        .onDisappear {
            observationTask?.cancel()
            observationTask = nil
        }
    }

    /// Start a playback-state observation task, but only when this row's
    /// episode is the runtime's current episode. We compare against
    /// `Episode.canonicalEpisodeKey` because that's the same key the
    /// runtime uses (`setCurrentEpisodeId`).
    private func beginObservingIfCurrent() async {
        let isCurrent = runtime.currentEpisodeId == episode.canonicalEpisodeKey
        guard isCurrent else { return }
        // Seed with the current snapshot so the first paint is correct.
        let snapshot = await runtime.playbackService.snapshot()
        currentTime = snapshot.currentTime
        observationTask?.cancel()
        observationTask = Task { @MainActor in
            let stream = await runtime.playbackService.observeStates()
            for await state in stream {
                guard !Task.isCancelled else { return }
                currentTime = state.currentTime
            }
        }
    }
}

// MARK: - Pull-to-Refresh Helper (playhead-riu8)

/// Dispatches one feed-refresh for the supplied podcast via the
/// injected `EpisodeRefreshing` seam and silently tolerates errors.
/// Silent partial-refresh mirrors `LibraryView.refreshAllFeeds` — the
/// user's intent is "show me anything new", not "fail loudly on a
/// flaky network".
///
/// Exposed at file scope (rather than nested in the view) so tests can
/// exercise the call-once invariant and error-swallowing without
/// standing up the SwiftUI `@Query` / `@Environment` machinery.
@MainActor
func performEpisodeRefresh(
    refresher: any EpisodeRefreshing,
    podcast: Podcast,
    modelContext: ModelContext
) async {
    do {
        _ = try await refresher.refreshEpisodes(for: podcast, in: modelContext)
    } catch {
        // Silent — partial refresh is better than none (mirrors LibraryView).
    }
}

// MARK: - Preview

#Preview("Episode List") {
    NavigationStack {
        EpisodeListView(
            podcast: Podcast(
                feedURL: URL(string: "https://example.com/feed")!,
                title: "The Daily",
                author: "The New York Times"
            )
        )
    }
    .environment(PlayheadRuntime(isPreviewRuntime: true))
    .preferredColorScheme(.dark)
    .modelContainer(for: [Podcast.self, Episode.self], inMemory: true)
}
