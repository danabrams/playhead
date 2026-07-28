// NowPlayingView.swift
// Full-screen now-playing experience. Stamp-sized artwork, copper playhead,
// full-width timeline rail, transport controls, speed selector.
// "Quiet Instrument" aesthetic — precise, minimal chrome, typographic hierarchy.

import OSLog
import SwiftData
import SwiftUI

private struct BannerPlaybackContext: Equatable {
    let episodeId: String?
    let playbackLifecycleGeneration: UInt64
}

/// Immutable ownership captured when the transcript sheet is presented.
/// SwiftUI may recompute sheet content while it remains mounted, so values
/// read directly from `runtime` inside the sheet closure are not a lifecycle
/// token: a same-asset replay could silently replace them with the new
/// episode generation.
private struct TranscriptPeekPresentationContext: Identifiable, Equatable {
    let id = UUID()
    let analysisAssetId: String
    let episodeId: String?
    let podcastId: String?
    let playbackLifecycleGeneration: UInt64
}

/// Maps banner-tier answers onto the production correction seams without
/// teaching the reusable banner view about `PlayheadRuntime`.
///
/// The initializer accepts narrow sinks so tests can exercise the same ID and
/// explicit-denial routing that production uses.
@MainActor
struct BannerFeedbackProductionActions {
    let onAutoSkipConfirmed: (AdSkipBannerItem) async -> Bool
    let onNotAnAd: (AdSkipBannerItem) async -> Bool
    let onSuggestSkip: (AdSkipBannerItem) async -> Bool
    let onSuggestDecline: (AdSkipBannerItem) async -> Bool
    let onSuggestExitWithoutSkip: (AdSkipBannerItem, Bool) -> Void

    init(
        confirmAutoSkippedBanner: @escaping (
            _ windowId: String,
            _ analysisAssetId: String?,
            _ startTime: Double,
            _ endTime: Double,
            _ expectedEpisodeId: String?,
            _ expectedPlaybackLifecycleGeneration: UInt64?,
            _ expectedWindowMaterialRevisionToken: String?
        ) async -> Bool = { _, _, _, _, _, _, _ in false },
        revertWindow: @escaping (
            _ windowId: String,
            _ podcastId: String,
            _ analysisAssetId: String?,
            _ startTime: Double,
            _ endTime: Double,
            _ expectedEpisodeId: String?,
            _ expectedPlaybackLifecycleGeneration: UInt64?,
            _ expectedWindowMaterialRevisionToken: String?
        ) async -> Bool,
        acceptSuggestedSkip: @escaping (
            _ windowId: String,
            _ expectedEpisodeId: String?,
            _ expectedPlaybackLifecycleGeneration: UInt64?,
            _ expectedSuggestionRevisionToken: String?
        ) async -> Bool,
        declineSuggestedSkip: @escaping (
            _ windowId: String,
            _ isExplicitDenial: Bool,
            _ expectedEpisodeId: String?,
            _ expectedPlaybackLifecycleGeneration: UInt64?,
            _ expectedSuggestionRevisionToken: String?
        ) async -> Bool
    ) {
        onAutoSkipConfirmed = { item in
            await confirmAutoSkippedBanner(
                item.windowId,
                item.analysisAssetId,
                item.adStartTime,
                item.adEndTime,
                item.episodeId,
                item.playbackLifecycleGeneration,
                item.windowMaterialRevisionToken
            )
        }
        onNotAnAd = { item in
            await revertWindow(
                item.windowId,
                item.podcastId,
                item.analysisAssetId,
                item.adStartTime,
                item.adEndTime,
                item.episodeId,
                item.playbackLifecycleGeneration,
                item.windowMaterialRevisionToken
            )
        }
        onSuggestSkip = { item in
            await acceptSuggestedSkip(
                item.windowId,
                item.episodeId,
                item.playbackLifecycleGeneration,
                item.suggestionRevisionToken
            )
        }
        onSuggestDecline = { item in
            await declineSuggestedSkip(
                item.windowId,
                true,
                item.episodeId,
                item.playbackLifecycleGeneration,
                item.suggestionRevisionToken
            )
        }
        onSuggestExitWithoutSkip = { item, isExplicitDenial in
            Task { @MainActor in
                _ = await declineSuggestedSkip(
                    item.windowId,
                    isExplicitDenial,
                    item.episodeId,
                    item.playbackLifecycleGeneration,
                    item.suggestionRevisionToken
                )
            }
        }
    }

    /// Builds the production queue with both durable aggregate storage and the
    /// suggest-exit route installed as one composition step. Tests inject an
    /// isolated store and narrow sinks into this same path, avoiding
    /// source-text canaries for production wiring.
    ///
    /// playhead-bfq7: `tallyStore` is optional so existing test call
    /// sites keep their isolation (no shared `UserDefaults` write);
    /// production passes the process-wide handle.
    func makeQueue(
        feedbackCounterStore: BannerFeedbackCounterStore,
        tallyStore: BannerTallyStore? = nil
    ) -> AdBannerQueue {
        let queue = AdBannerQueue(
            feedbackCounterStore: feedbackCounterStore,
            tallyStore: tallyStore
        )
        queue.onSuggestExitWithoutSkip = onSuggestExitWithoutSkip
        return queue
    }
}

// MARK: - NowPlayingView

struct NowPlayingView: View {

    /// playhead-3bv.4: shared logger for non-fatal correction-persistence
    /// failures (e.g. the "Always skip this sponsor" sink). Defined at
    /// view scope so the wiring closures inside `body` can reach it via
    /// `Self.logger` without capturing a per-instance `let`.
    fileprivate static let logger = Logger(
        subsystem: "com.playhead",
        category: "NowPlayingView"
    )

    private var runtime: PlayheadRuntime
    private let ownsViewModel: Bool
    @State private var viewModel: NowPlayingViewModel
    private let bannerFeedbackActions: BannerFeedbackProductionActions
    @State private var bannerQueue: AdBannerQueue
    @State private var transcriptPeekContext:
        TranscriptPeekPresentationContext?
    /// playhead-05i: drives the "Up Next" sheet presentation. The
    /// sheet hosts a `QueueView` whose VM reads the same
    /// `PlaybackQueueService` injected at App scene scope.
    @State private var showQueueSheet = false
    /// playhead-3bv.4: drives the Activity sheet that opens scoped to
    /// the currently-playing episode when the user taps the status
    /// line below the timeline.
    @State private var showActivitySheet = false
    @Environment(\.dismiss) private var dismiss
    @Environment(\.playbackQueueService) private var playbackQueueService
    @Environment(\.modelContext) private var modelContext

    /// Accepts an optional external ViewModel for shared state with NowPlayingBar.
    /// Falls back to creating its own if none provided.
    init(runtime: PlayheadRuntime, viewModel: NowPlayingViewModel? = nil) {
        self.runtime = runtime
        let resolvedViewModel = viewModel ?? NowPlayingViewModel(runtime: runtime)
        self.ownsViewModel = viewModel == nil
        self._viewModel = State(wrappedValue: resolvedViewModel)
        let actions = BannerFeedbackProductionActions(
            confirmAutoSkippedBanner: {
                [weak runtime = runtime]
                windowId,
                analysisAssetId,
                startTime,
                endTime,
                expectedEpisodeId,
                expectedPlaybackGeneration,
                expectedMaterialToken in
                guard let runtime else { return false }
                let orchestrator = runtime.skipOrchestrator
                guard runtime.currentEpisodeId == expectedEpisodeId,
                      runtime.playEpisodeGeneration
                        == expectedPlaybackGeneration
                else {
                    return false
                }
                return await orchestrator.confirmAutoSkippedBanner(
                    windowId: windowId,
                    analysisAssetId: analysisAssetId,
                    startTime: startTime,
                    endTime: endTime,
                    ifCurrentEpisodeId: expectedEpisodeId,
                    ifPlaybackLifecycleGeneration:
                        expectedPlaybackGeneration,
                    ifWindowMaterialRevisionToken: expectedMaterialToken
                )
            },
            revertWindow: {
                [weak runtime = runtime]
                windowId,
                podcastId,
                analysisAssetId,
                startTime,
                endTime,
                expectedEpisodeId,
                expectedPlaybackGeneration,
                expectedMaterialToken in
                guard let runtime else { return false }
                let orchestrator = runtime.skipOrchestrator
                guard runtime.currentEpisodeId == expectedEpisodeId,
                      runtime.playEpisodeGeneration
                        == expectedPlaybackGeneration
                else {
                    return false
                }
                return await orchestrator.denyAutoSkippedBanner(
                    windowId: windowId,
                    analysisAssetId: analysisAssetId,
                    startTime: startTime,
                    endTime: endTime,
                    podcastId: podcastId,
                    ifCurrentEpisodeId: expectedEpisodeId,
                    ifPlaybackLifecycleGeneration:
                        expectedPlaybackGeneration,
                    ifWindowMaterialRevisionToken: expectedMaterialToken
                )
            },
            acceptSuggestedSkip: {
                [weak runtime = runtime]
                windowId,
                expectedEpisodeId,
                expectedPlaybackGeneration,
                expectedSuggestionRevisionToken in
                guard let runtime else { return false }
                let orchestrator = runtime.skipOrchestrator
                guard runtime.currentEpisodeId == expectedEpisodeId,
                      runtime.playEpisodeGeneration
                        == expectedPlaybackGeneration
                else {
                    return false
                }
                return await orchestrator.acceptSuggestedSkip(
                    windowId: windowId,
                    ifCurrentEpisodeId: expectedEpisodeId,
                    ifPlaybackLifecycleGeneration:
                        expectedPlaybackGeneration,
                    ifSuggestionRevisionToken:
                        expectedSuggestionRevisionToken
                )
            },
            declineSuggestedSkip: {
                [weak runtime = runtime]
                windowId,
                isExplicitDenial,
                expectedEpisodeId,
                expectedPlaybackGeneration,
                expectedSuggestionRevisionToken in
                guard let runtime else { return false }
                let orchestrator = runtime.skipOrchestrator
                guard runtime.currentEpisodeId == expectedEpisodeId,
                      runtime.playEpisodeGeneration
                        == expectedPlaybackGeneration
                else {
                    return false
                }
                return await orchestrator.declineSuggestedSkip(
                    windowId: windowId,
                    isExplicitDenial: isExplicitDenial,
                    ifCurrentEpisodeId: expectedEpisodeId,
                    ifPlaybackLifecycleGeneration:
                        expectedPlaybackGeneration,
                    ifSuggestionRevisionToken:
                        expectedSuggestionRevisionToken
                )
            }
        )
        self.bannerFeedbackActions = actions
        self._bannerQueue = State(
            wrappedValue: actions.makeQueue(
                feedbackCounterStore: .shared,
                tallyStore: .shared
            )
        )
    }

    private var analysisAssetId: String? {
        runtime.currentAnalysisAssetId
    }

    private var bannerPlaybackContext: BannerPlaybackContext {
        BannerPlaybackContext(
            episodeId: runtime.currentEpisodeId,
            playbackLifecycleGeneration:
                runtime.playEpisodeGeneration
        )
    }

    var body: some View {
        ZStack {
            AppColors.background
                .ignoresSafeArea()

            VStack(spacing: 0) {
                // MARK: Top Chrome
                topBar
                    .padding(.horizontal, Spacing.md)
                    .padding(.top, Spacing.sm)

                Spacer(minLength: Spacing.xl)

                // MARK: Artwork
                artworkSection
                    .padding(.horizontal, Spacing.xxl)

                Spacer(minLength: Spacing.lg)

                // MARK: Titles
                titleSection
                    .padding(.horizontal, Spacing.md)

                Spacer(minLength: Spacing.lg)

                // MARK: Timeline
                timelineSection
                    .padding(.horizontal, Spacing.md)

                // MARK: Status line (playhead-3bv.4, UI design §C-2)
                //
                // Slim one-line status that mirrors the episode-detail
                // status reducer (`EpisodeSurfaceStatus`). Hidden when
                // the current episode is fully analyzed. Tap → Activity
                // scoped to this episode. The host child view owns the
                // SwiftData `@Query` so it re-renders cleanly when the
                // playing episode changes (id-keyed identity below).
                if let episodeId = runtime.currentEpisodeId {
                    PlayerStatusLineHost(
                        episodeId: episodeId,
                        onTap: { showActivitySheet = true }
                    )
                    .id(episodeId)
                    .padding(.horizontal, Spacing.md)
                    .padding(.top, Spacing.xs)
                }

                Spacer(minLength: Spacing.lg)

                // MARK: Transport
                transportSection
                    .padding(.horizontal, Spacing.xl)

                Spacer(minLength: Spacing.md)

                // MARK: Hearing an Ad
                if analysisAssetId != nil {
                    hearingAdButton
                        .padding(.horizontal, Spacing.md)
                }

                // MARK: Speed
                speedSection
                    .padding(.horizontal, Spacing.md)
                    .padding(.bottom, Spacing.lg)
            }

            // Ad skip banner — slides in at bottom, single lane, auto-dismiss.
            AdBannerView(
                queue: bannerQueue,
                isPresentationVisible:
                    !showQueueSheet
                    && !showActivitySheet
                    && transcriptPeekContext == nil,
                isItemCurrent: { item in
                    item.episodeId == runtime.currentEpisodeId
                        && item.playbackLifecycleGeneration
                            == runtime.playEpisodeGeneration
                },
                onListenAsync: { item in
                    await viewModel.handleListenRewindAwaitingAction(item: item)
                },
                onAutoSkipConfirmedAsync:
                    bannerFeedbackActions.onAutoSkipConfirmed,
                // A No response to an automatic skip is the "Not an ad"
                // correction. Route through `revertWindow(windowId:)` rather than
                // constructing a CorrectionEvent directly — the orchestrator
                // path writes a precise `.exactTimeSpan` correction (covering
                // the window's snapped start/end) and handles the state,
                // trust signal, and persistence atomically. Previously this
                // site bypassed the orchestrator and persisted an
                // `exactSpan:0:Int.max` whole-episode veto.
                onNotAnAdAsync: bannerFeedbackActions.onNotAnAd,
                // playhead-gtt9.23: Yes on a suggest-tier banner.
                // Promotes the suggested span into the active skip path
                // and records a falseNegative correction (the user just
                // told us "this WAS an ad" — exactly the calibration
                // signal future threshold tuning needs).
                onSuggestSkipAsync: bannerFeedbackActions.onSuggestSkip,
                onSuggestDeclineAsync:
                    bannerFeedbackActions.onSuggestDecline,
                // playhead-3bv.4: "Always skip this sponsor" on an
                // auto-skipped banner. Records a `sponsorOnShow` scope
                // correction so the SponsorKnowledgeStore's negative-
                // memory pass filters this advertiser out of future
                // episodes of the same show. Mirrors the normalization
                // SponsorKnowledgeStore uses on its
                // `normalizedValue` field — `entityValue.lowercased()
                // .trimmingCharacters(in: .whitespaces)` — so the scope
                // serializer produces an identity that the downstream
                // lookup actually matches. Using `.whitespaces` (not
                // `.whitespacesAndNewlines`) is deliberate: this is a
                // contract drift guard against the knowledge-store's
                // exact character-set choice.
                onAlwaysSkipSponsorAsync: { item in
                    guard runtime.currentEpisodeId == item.episodeId,
                          runtime.playEpisodeGeneration
                            == item.playbackLifecycleGeneration,
                          let advertiser = item.advertiser
                    else {
                        return false
                    }
                    let normalized = AdBannerView
                        .normalizedAlwaysSkipSponsor(advertiser)
                    guard !normalized.isEmpty else { return false }
                    let podcastId = item.podcastId
                    let assetId = runtime.currentAnalysisAssetId
                        ?? "podcast:\(podcastId)"
                    let scope = CorrectionScope.sponsorOnShow(
                        podcastId: podcastId,
                        sponsor: normalized
                    )
                    let event = CorrectionEvent(
                        analysisAssetId: assetId,
                        scope: scope.serialized,
                        source: .manualVeto,
                        podcastId: podcastId,
                        correctionType: .falsePositive,
                        targetRefs: CorrectionTargetRefs(
                            sponsorEntity: normalized
                        )
                    )
                    let store = runtime.correctionStore
                    do {
                        try await store.record(event)
                        return true
                    } catch {
                        Self.logger.error(
                            "alwaysSkipSponsor: failed to persist sponsorOnShow event for \(normalized, privacy: .public) on \(podcastId, privacy: .public): \(error.localizedDescription, privacy: .public)"
                        )
                        return false
                    }
                },
                // playhead-jw63.5: banner-context entry into the feedback
                // channel, reached by long-pressing the card. Adds no
                // visible control, so the one-tap Yes/No stays the primary
                // (and cheapest) way to answer. The note carries the
                // moment's offset plus a SALTED reference token — never the
                // episode title, never anything about its content.
                onTellUsWhatHappened: { item in
                    Task { @MainActor in
                        let reference = listenerFeedbackReference(
                            modelContext: modelContext,
                            episodeId: item.episodeId
                        )
                        _ = try? await runListenerFeedback(
                            runtime: runtime,
                            modelContext: modelContext,
                            context: .moment(
                                atSeconds: item.adStartTime,
                                reference: reference
                            )
                        )
                    }
                }
            )
        }
        .onAppear {
            let bannerHostGeneration = bannerQueue.activateHost(
                for: runtime.currentEpisodeId,
                playbackLifecycleGeneration:
                    runtime.playEpisodeGeneration
            )
            viewModel.startObserving()
            viewModel.observeAdSegments(from: runtime.skipOrchestrator)
            viewModel.observeBanners(
                from: runtime.skipOrchestrator,
                into: bannerQueue,
                hostGeneration: bannerHostGeneration
            )
            Task { await viewModel.loadSkipMode(from: runtime.skipOrchestrator) }
        }
        .onChange(of: bannerPlaybackContext) {
            previousContext,
            currentContext in
            // A transcript selection belongs to the playback transaction that
            // presented it. Close the sheet on episode or same-episode replay
            // instead of allowing its @State model to survive with newly
            // recomputed callback identities.
            transcriptPeekContext = nil
            // Now Playing remains mounted across autoplay/queue advancement.
            // Retire banners at both episode and same-episode replacement
            // boundaries so an old presentation cannot act on new state.
            let bannerHostGeneration =
                bannerQueue.discardAllOnPlaybackContextChange(
                    fromEpisodeId: previousContext.episodeId,
                    toEpisodeId: currentContext.episodeId,
                    fromPlaybackLifecycleGeneration:
                        previousContext.playbackLifecycleGeneration,
                    toPlaybackLifecycleGeneration:
                        currentContext.playbackLifecycleGeneration
            )
            // Reattach with the new generation. The orchestrator may still be
            // serving the previous episode until PlayheadRuntime finishes its
            // asynchronous episode setup; item-level episode identity rejects
            // those late old emissions during that interval.
            viewModel.observeBanners(
                from: runtime.skipOrchestrator,
                into: bannerQueue,
                hostGeneration: bannerHostGeneration
            )
        }
        .onDisappear {
            // This is the lifecycle boundary for the queue owner, not a
            // temporary sheet/background obscuration. Retire all outstanding
            // suggestions neutrally so the orchestrator cannot retain windows
            // that no visible banner can answer.
            bannerQueue.discardAllOnHostDisappear()
            if ownsViewModel {
                viewModel.stopObserving()
            } else {
                viewModel.stopObservingAdSegments()
                viewModel.stopObservingBanners()
            }
        }
        .sheet(isPresented: $showQueueSheet) {
            // playhead-05i: queue sheet. The VM is constructed
            // inside the sheet builder so it pulls the live service
            // from the environment at presentation time. When the
            // service is `nil` (e.g. preview / test runtime), the
            // VM still constructs but `refresh()` returns no rows.
            if let service = playbackQueueService {
                QueueView(
                    viewModel: QueueViewModel(
                        queueService: service,
                        modelContainer: modelContext.container
                    )
                )
                .presentationDetents([.medium, .large])
                .presentationDragIndicator(.visible)
                .presentationBackground(AppColors.surface)
            } else {
                // No service: render an empty placeholder rather than
                // a half-broken queue. The real path always has a
                // service because `PlayheadApp` wires it before any
                // view tree is realized.
                Text("Queue unavailable")
                    .font(AppTypography.body)
                    .foregroundStyle(AppColors.textSecondary)
                    .padding()
                    .presentationDetents([.medium])
            }
        }
        .sheet(isPresented: $showActivitySheet) {
            // playhead-3bv.4: Activity sheet opened from the player's
            // status line. Scoped to the currently-playing episode
            // via `focusedEpisodeId` — the View filters every section
            // down to that one episode's row(s) so the user lands on
            // exactly the affordances that match what they're hearing.
            // The provider closure mirrors the `ContentView` Activity
            // tab wiring so the snapshot composition stays identical.
            ActivityView(
                inputProvider: { [runtime, modelContext] in
                    let provider = runtime.makeActivitySnapshotProvider(
                        modelContainer: modelContext.container
                    )
                    return await provider.loadInputs()
                },
                focusedEpisodeId: runtime.currentEpisodeId
            )
            .presentationDetents([.medium, .large])
            .presentationDragIndicator(.visible)
            .presentationBackground(AppColors.surface)
        }
        .sheet(item: $transcriptPeekContext) { sourceContext in
            TranscriptPeekView(
                peekViewModel: TranscriptPeekViewModel(
                    analysisAssetId: sourceContext.analysisAssetId,
                    dataSource: LiveTranscriptPeekDataSource(
                        store: runtime.analysisStore
                    )
                ),
                currentTime: viewModel.currentTime,
                // playhead-m1l9: episode duration seeds the coverage-free
                // "mark the untranscribed post-roll/tail" affordance.
                episodeDuration: viewModel.duration,
                trustService: runtime.trustService,
                podcastId: sourceContext.podcastId,
                // The callback is the sole correction writer. It atomically
                // persists the exact producer rows and correction before the
                // popover/sheet may report success.
                onRevertAdWindows: { span in
                    guard span.assetId
                            == sourceContext.analysisAssetId
                    else {
                        return false
                    }
                    return await runtime.revertAdWindows(
                        span: span,
                        ifCurrentAnalysisAssetId:
                            sourceContext.analysisAssetId,
                        ifCurrentEpisodeId:
                            sourceContext.episodeId,
                        ifPlaybackLifecycleGeneration:
                            sourceContext.playbackLifecycleGeneration,
                        podcastId: sourceContext.podcastId
                    )
                },
                onMarkAd: { startTime, endTime in
                    await runtime.injectUserMarkedAd(
                        start: startTime,
                        end: endTime,
                        ifCurrentAnalysisAssetId:
                            sourceContext.analysisAssetId,
                        ifCurrentEpisodeId:
                            sourceContext.episodeId,
                        ifPlaybackLifecycleGeneration:
                            sourceContext.playbackLifecycleGeneration,
                        podcastId: sourceContext.podcastId
                    )
                }
            )
            .presentationDetents([.medium, .large])
            .presentationDragIndicator(.visible)
            .presentationBackground(AppColors.surface)
        }
    }
}

// MARK: - Subviews

private extension NowPlayingView {

    // MARK: Top Bar

    var topBar: some View {
        HStack {
            Button {
                dismiss()
            } label: {
                Image(systemName: "chevron.down")
                    .font(.system(size: 18, weight: .medium))
                    .foregroundStyle(AppColors.textSecondary)
            }
            .accessibilityLabel("Dismiss")
            .accessibilityHint("Closes the now playing screen")

            Spacer()

            VStack(spacing: 2) {
                Text("PLAYING FROM")
                    .font(AppTypography.sans(size: 10, weight: .semibold))
                    .foregroundStyle(AppColors.textTertiary)
                    .tracking(1.2)

                Text(viewModel.podcastTitle)
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.textSecondary)
                    .lineLimit(1)
            }
            .accessibilityElement(children: .combine)

            Spacer()

            HStack(spacing: Spacing.md) {
                // playhead-05i: "Up Next" — opens the queue sheet.
                // Always visible (the queue is a global concept), even
                // when no analysis is available.
                Button {
                    showQueueSheet = true
                } label: {
                    Image(systemName: "list.bullet")
                        .font(.system(size: 18, weight: .medium))
                        .foregroundStyle(AppColors.textSecondary)
                }
                .accessibilityLabel("Up Next")
                .accessibilityHint("Opens the playback queue")

                // Transcript peek — visible when analysis is available
                if let assetId = analysisAssetId {
                    Button {
                        transcriptPeekContext =
                            TranscriptPeekPresentationContext(
                                analysisAssetId: assetId,
                                episodeId: runtime.currentEpisodeId,
                                podcastId: runtime.currentPodcastId,
                                playbackLifecycleGeneration:
                                    runtime.playEpisodeGeneration
                            )
                    } label: {
                        Image(systemName: "text.quote")
                            .font(.system(size: 18, weight: .medium))
                            .foregroundStyle(AppColors.textSecondary)
                    }
                    .accessibilityLabel("Transcript")
                    .accessibilityHint("Opens the transcript peek sheet")
                } else {
                    // Balance the chevron when transcript unavailable
                    Color.clear
                        .frame(width: 18, height: 18)
                }
            }
        }
    }

    // MARK: Artwork

    var artworkSection: some View {
        RoundedRectangle(cornerRadius: CornerRadius.medium)
            .fill(AppColors.surface)
            .aspectRatio(1, contentMode: .fit)
            .frame(maxWidth: 140, maxHeight: 140)
            .overlay(
                Group {
                    if let artworkURL = viewModel.artworkURL {
                        AsyncImage(url: artworkURL) { phase in
                            switch phase {
                            case .success(let image):
                                image
                                    .resizable()
                                    .scaledToFill()
                            case .failure:
                                artworkPlaceholder
                            case .empty:
                                ProgressView()
                                    .tint(AppColors.textSecondary)
                            @unknown default:
                                artworkPlaceholder
                            }
                        }
                    } else {
                        artworkPlaceholder
                    }
                }
                .clipShape(RoundedRectangle(cornerRadius: CornerRadius.medium))
            )
            .overlay(
                RoundedRectangle(cornerRadius: CornerRadius.medium)
                    .stroke(AppColors.textSecondary.opacity(0.2), lineWidth: 1)
            )
            .themeShadow(AppShadow.card)
            .accessibilityLabel("Episode artwork")
    }

    private var artworkPlaceholder: some View {
        Image(systemName: "mic.fill")
            .font(.title2)
            .foregroundStyle(AppColors.textSecondary.opacity(0.4))
    }

    // MARK: Titles

    var titleSection: some View {
        VStack(spacing: Spacing.xxs) {
            Text(viewModel.episodeTitle)
                .font(AppTypography.sans(size: 20, weight: .semibold))
                .foregroundStyle(AppColors.textPrimary)
                .lineLimit(2)
                .multilineTextAlignment(.center)
                .accessibilityAddTraits(.isHeader)

            Text(viewModel.podcastTitle)
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textSecondary)
                .lineLimit(1)

            skipModePill
        }
    }

    @ViewBuilder
    var skipModePill: some View {
        if !viewModel.podcastTitle.isEmpty {
            Menu {
                ForEach(SkipMode.allCases, id: \.self) { mode in
                    Button(mode.pillLabel) {
                        viewModel.setSkipMode(mode, orchestrator: runtime.skipOrchestrator)
                    }
                }
            } label: {
                Text(viewModel.activeSkipMode.pillLabel)
                    .font(AppTypography.sans(size: 10, weight: .semibold))
                    .foregroundStyle(viewModel.activeSkipMode.pillForeground)
                    .tracking(0.8)
                    .padding(.horizontal, 8)
                    .padding(.vertical, 3)
                    .background(viewModel.activeSkipMode.pillBackground)
                    .clipShape(Capsule())
                    .contentShape(Rectangle().size(width: 80, height: 44))
                    .frame(minWidth: 44, minHeight: 44)
            }
            .accessibilityLabel("Skip mode: \(viewModel.activeSkipMode.pillLabel)")
            .accessibilityHint("Tap to change skip mode for this show")
        }
    }

    // MARK: Timeline

    var timelineSection: some View {
        VStack(spacing: Spacing.xs) {
            TimelineRailView(
                progress: viewModel.progress,
                adSegments: viewModel.adSegmentRanges,
                onSeek: { fraction in
                    let target = fraction * viewModel.duration
                    viewModel.seek(to: target)
                }
            )

            HStack {
                Text(viewModel.elapsedFormatted)
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.textTertiary)
                    .accessibilityLabel("Elapsed: \(viewModel.elapsedFormatted)")

                Spacer()

                Text(viewModel.remainingFormatted)
                    .font(AppTypography.timestamp)
                    .foregroundStyle(AppColors.textTertiary)
                    .accessibilityLabel("Remaining: \(viewModel.remainingFormatted)")
            }
        }
    }

    // MARK: Transport

    var transportSection: some View {
        HStack(spacing: Spacing.xl) {
            Spacer()

            // Skip backward 15s
            TransportButton(
                systemName: "gobackward.15",
                size: 28,
                accessibilityText: "Skip back 15 seconds"
            ) {
                viewModel.skipBackward()
            }

            // Play / Pause
            TransportButton(
                systemName: viewModel.isPlaying
                    ? "pause.fill"
                    : "play.fill",
                size: 42,
                accessibilityText: viewModel.isPlaying ? "Pause" : "Play"
            ) {
                viewModel.togglePlayPause()
            }

            // Skip forward 30s
            TransportButton(
                systemName: "goforward.30",
                size: 28,
                accessibilityText: "Skip forward 30 seconds"
            ) {
                viewModel.skipForward()
            }

            Spacer()
        }
    }

    // MARK: Hearing an Ad

    var hearingAdButton: some View {
        Button {
            viewModel.reportHearingAd()
        } label: {
            HStack(spacing: Spacing.xs) {
                Image(systemName: "ear.fill")
                    .font(.system(size: 13, weight: .medium))
                Text("Hearing an ad")
                    .font(AppTypography.sans(size: 13, weight: .medium))
            }
            .foregroundStyle(AppColors.textSecondary)
            .padding(.horizontal, Spacing.sm)
            .padding(.vertical, Spacing.xs)
            .background(
                Capsule()
                    .fill(AppColors.textSecondary.opacity(0.10))
            )
            .contentShape(Capsule())
        }
        .buttonStyle(TransportButtonStyle())
        .frame(minWidth: 44, minHeight: 44)
        .accessibilityLabel("Hearing an ad")
        .accessibilityHint("Reports that you are currently hearing an ad that was not detected")
    }

    // MARK: Speed

    var speedSection: some View {
        HStack {
            Spacer()

            SpeedSelectorView(
                currentSpeed: viewModel.playbackSpeed,
                onSpeedChanged: { speed in
                    viewModel.setSpeed(speed)
                }
            )

            Spacer()
        }
    }
}

// MARK: - SkipMode Pill Style

private extension SkipMode {
    var pillLabel: String {
        switch self {
        case .shadow: "Shadow"
        case .manual: "Manual"
        case .auto:   "Auto"
        }
    }

    var pillForeground: Color {
        switch self {
        case .shadow: AppColors.textTertiary
        case .manual: AppColors.textSecondary
        case .auto:   AppColors.accent
        }
    }

    var pillBackground: Color {
        switch self {
        case .shadow: AppColors.textTertiary.opacity(0.12)
        case .manual: AppColors.textSecondary.opacity(0.12)
        case .auto:   AppColors.accent.opacity(0.18)
        }
    }
}

// MARK: - Transport Button

/// A single transport control with haptic feedback.
struct TransportButton: View {
    let systemName: String
    let size: CGFloat
    var accessibilityText: String = ""
    var hapticPlayer: any HapticPlaying = SystemHapticPlayer()
    let action: () -> Void

    /// Factored tap handler so tests can drive the haptic + action path
    /// without rendering a live SwiftUI hierarchy.
    func handleTap() {
        hapticPlayer.play(.control)
        action()
    }

    var body: some View {
        Button {
            handleTap()
        } label: {
            Image(systemName: systemName)
                .font(.system(size: size, weight: .medium))
                .foregroundStyle(AppColors.textPrimary)
                .frame(width: size + 20, height: size + 20)
                .contentShape(Rectangle())
        }
        .buttonStyle(TransportButtonStyle())
        .accessibilityLabel(accessibilityText)
    }
}

/// Subtle scale-down on press. No bounce — precise, mechanical.
private struct TransportButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed ? 0.92 : 1.0)
            .animation(Motion.quick, value: configuration.isPressed)
    }
}

// MARK: - PlayerStatusLineHost

/// playhead-3bv.4: SwiftData-backed host for the player's status row.
///
/// Owns a `@Query` keyed on `canonicalEpisodeKey == episodeId` so the
/// row re-renders whenever the underlying `Episode` row changes
/// (coverage progression, anchor moves). The host is mounted with
/// `.id(episodeId)` by `NowPlayingView`, which forces a fresh query
/// when the playing episode changes — `@Query` with a predicate that
/// captures `let episodeId` is parameter-stable for the lifetime of
/// an identity, so this is the idiomatic SwiftData pattern.
struct PlayerStatusLineHost: View {
    @Query private var episodes: [Episode]

    let onTap: () -> Void

    init(episodeId: String, onTap: @escaping () -> Void) {
        // FetchDescriptor with `fetchLimit: 1` keeps the query bounded
        // even if the canonicalEpisodeKey uniqueness invariant is ever
        // violated (defensive — the schema constraint should already
        // guarantee at most one match).
        let key = episodeId
        var descriptor = FetchDescriptor<Episode>(
            predicate: #Predicate<Episode> { $0.canonicalEpisodeKey == key }
        )
        descriptor.fetchLimit = 1
        self._episodes = Query(descriptor)
        self.onTap = onTap
    }

    var body: some View {
        if let episode = episodes.first {
            PlayerStatusLineRow(
                inputs: playerStatusLineInputs(episode: episode),
                onTap: onTap
            )
        }
    }
}

// MARK: - Preview

#Preview("Now Playing") {
    NowPlayingView(runtime: PlayheadRuntime(isPreviewRuntime: true))
        .preferredColorScheme(.dark)
}
