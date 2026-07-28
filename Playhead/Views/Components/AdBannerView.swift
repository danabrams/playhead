// AdBannerView.swift
// Ad skip banner — slides in at bottom of Now Playing when an ad is skipped.
//
// Styled as a calm margin note, not an alert. Long horizontal proportions
// (cue sheet style). Ink background, Bone text, Copper accent on "Listen".
// Auto-dismisses after 8 seconds. Single banner lane with queue — rapid
// sequential skips are coalesced, never stacked.
//
// ┌─────────────────────────────────────────────────┐
// │  Skipped · Squarespace · "Build your website"   │
// │  Was this right?       [Yes] [No] [Listen]  [x] │
// └─────────────────────────────────────────────────┘

import SwiftUI

// MARK: - Banner Tier (playhead-gtt9.23)

/// The skip-worthiness tier this banner represents. Drives copy and
/// affordance choice in `AdBannerView`. Auto-skip / suggest tiers correspond
/// to the high / medium bands in the gradient UX spec; the silent tier
/// (low confidence) never produces a banner and therefore is not modelled
/// here.
///
/// Voice rules (per `feedback_peace_of_mind_not_metrics`):
/// - No quantified language anywhere in tier-driven copy.
/// - Auto-skip copy describes a completed action ("Skipped …").
/// - Suggest copy describes an observation + an actionable affordance,
///   never a probability.
enum AdBannerTier: String, Sendable, Equatable, Codable {
    /// High-confidence tier (≥ `segmentAutoSkipThreshold`, default 0.55).
    /// Banner reports a skip that has already happened; user can rewind via
    /// "Listen" or answer No to correct the skip.
    case autoSkipped

    /// Medium-confidence tier ([uiCandidate, autoSkip), default [0.40, 0.55)).
    /// Banner asks the user to confirm a skip that has NOT happened. The
    /// auto-skip path is deliberately suppressed; the user is the gate.
    /// Answering Yes records a `.falseNegative` user correction (calibration
    /// signal) and skips; neutral dismiss / auto-fade leaves playback alone.
    case suggest
}

// MARK: - Banner Data

/// Data for a single ad skip banner notification.
struct AdSkipBannerItem: Identifiable, Equatable {
    let id: String
    /// The ad window ID from AnalysisStore (for revert feedback).
    let windowId: String
    /// Advertiser name, if known and above confidence threshold.
    let advertiser: String?
    /// Short product/tagline, if known and above confidence threshold.
    let product: String?
    /// Timestamp in episode seconds where the skipped ad started (snapped boundary).
    let adStartTime: Double
    /// Timestamp in episode seconds where the skipped ad ended.
    let adEndTime: Double
    /// Confidence of the metadata extraction (nil = no metadata).
    let metadataConfidence: Double?
    /// Where the metadata came from. Known values: "foundationModels", "fallback", "none".
    let metadataSource: String
    /// The podcast ID, needed for trust scoring on revert.
    let podcastId: String
    /// Episode identity captured by the orchestrator at emission time.
    ///
    /// The Now Playing surface survives autoplay and queue advancement. A
    /// banner produced by the previous episode can therefore arrive after the
    /// UI has already switched episodes. Production queueing requires this
    /// identity to match the host's current episode before presenting it.
    var episodeId: String? = nil
    /// Playback request generation captured when the orchestrator lifecycle
    /// that emitted this item began.
    ///
    /// Canonical episode identity is not a lifecycle identity: replaying the
    /// same episode replaces its transport and orchestrator state without
    /// changing `episodeId`. Production hosts require this token to match the
    /// runtime's current request before presenting or acting on the item.
    var playbackLifecycleGeneration: UInt64? = nil
    /// Asset identity captured with the exact window material shown.
    var analysisAssetId: String? = nil
    /// Opaque producer-material revision for action-time validation.
    ///
    /// Auto-skipped Yes uses this in addition to episode/lifecycle identity:
    /// a same-ID window whose span or attribution was recomputed cannot
    /// receive a receipt intended for an older card.
    var windowMaterialRevisionToken: String? = nil
    /// Revision identity for a suggest-tier window presentation.
    ///
    /// Producer windows can retain the same `windowId` while their span or
    /// attribution is recomputed within one playback lifecycle. Suggest
    /// acknowledgements and actions must carry this token back to the
    /// orchestrator so an older card cannot acknowledge, accept, or decline
    /// the newer revision. Auto-skipped banners do not use this field.
    var suggestionRevisionToken: String? = nil
    /// Evidence catalog entries associated with this ad window.
    /// Used by Phase 7's UserCorrectionStore to infer correction scopes
    /// (e.g. phraseOnShow) when the user taps "Listen" to revert a skip.
    /// Empty when no catalog data is available — callers must handle [] gracefully.
    let evidenceCatalogEntries: [EvidenceEntry]
    /// playhead-gtt9.23: skip-worthiness tier this banner is rendered as.
    /// Defaults to `.autoSkipped` to preserve every existing call site
    /// (the historical banner emitter is the high-confidence path).
    var tier: AdBannerTier = .autoSkipped
}

// MARK: - Banner Queue (ViewModel)

/// Manages banner display queue. Coalesces adjacent skips into a single
/// banner. Ensures only one banner is visible at a time.
@MainActor
@Observable
final class AdBannerQueue {

    private(set) var currentBanner: AdSkipBannerItem?

    /// Pending banners waiting to display.
    private var queue: [AdSkipBannerItem] = []

    /// Auto-dismiss timer handle.
    private var dismissTask: Task<Void, Never>?

    /// playhead-dd7d: Deferred queue-advance handle. `dismiss()` /
    /// `dismissAfterAccept()` clear `currentBanner` immediately, then spawn
    /// this task to slide the next banner in after a brief pause. Held (not
    /// fire-and-forget) so tests can await the advance deterministically via
    /// `advanceTaskForTesting()` instead of racing the 350 ms slide-in delay
    /// against a fixed wall-clock sleep. No behavioral change to the app: the
    /// task runs identically; it is merely retained by this property until it
    /// completes or is superseded by the next advance.
    private var advanceTask: Task<Void, Never>?

    /// Duration before auto-dismiss for an auto-skipped (high-tier) banner.
    /// 8 s is a calm dwell that leaves time to read but never lingers.
    private static let defaultAutoDismissSeconds: TimeInterval = 8.0

    /// playhead-gtt9.23: Duration before auto-fade for a suggest-tier
    /// (medium-confidence) banner. Slightly longer than the auto-skipped
    /// dwell because the user is being asked to make a choice — they
    /// need a beat or two more to read the line and decide whether to
    /// answer. Auto-fade with no user action clears the transient suggestion
    /// but is not recorded as explicit banner feedback.
    private static let defaultSuggestAutoDismissSeconds: TimeInterval = 12.0

    /// Maximum gap (seconds) between skipped ads to coalesce into one banner.
    private static let defaultCoalesceGap: TimeInterval = 10.0

    private let autoDismissSeconds: TimeInterval
    private let suggestAutoDismissSeconds: TimeInterval
    private let coalesceGap: TimeInterval
    private let autoDismissSleep: @Sendable (TimeInterval) async -> Void
    private let feedbackCounterStore: BannerFeedbackCounterStore?
    /// playhead-bfq7: per-episode card tally. Shares the exact
    /// presentation boundary the durable `banners_shown` aggregate uses
    /// (`recordBannerShown(for:)`), so the two counts can never diverge.
    /// `nil` in previews and in tests that do not assert on the tally.
    private let tallyStore: BannerTallyStore?
    private var isAutoDismissPaused = false

    /// Production hosts opt into an episode-scoped generation before
    /// observing banners. The generation rejects buffered events from an old
    /// observation task, while `hostEpisodeId` rejects genuinely later
    /// emissions from an orchestrator that has not yet switched episodes.
    ///
    /// Queues remain unscoped by default so isolated previews and unit tests
    /// can enqueue fixture items without manufacturing host lifecycle state.
    private var isHostScopeConfigured = false
    private var isHostActive = true
    private var hostEpisodeId: String?
    private var hostPlaybackLifecycleGeneration: UInt64?
    private var hostGeneration: UInt64 = 0

    /// Guards the current visible presentation against rapid/repeated actions.
    /// This remains presentation-scoped for synchronous controls. Explicit
    /// feedback also records its presentation ID below so an accepted async
    /// action can finish after the host disappears without being mislabeled as
    /// a neutral exit or losing its aggregate receipt.
    private var didClaimActionForCurrentPresentation = false
    private var didRecordShownForCurrentPresentation = false
    private var inFlightFeedbackItemIDs: Set<String> = []
    private var inFlightUtilityActionItemIDs: Set<String> = []

    /// playhead-gtt9.23: Invoked when a suggest-tier banner exits without an
    /// accepted skip — via a neutral dismiss, an explicit No, or the auto-fade
    /// timer. Wired by `NowPlayingView` to
    /// `SkipOrchestrator.declineSuggestedSkip` so the orchestrator can
    /// drop the suggested window from its in-memory set. `nil` when the
    /// view does not need this signal (tests, previews).
    ///
    /// playhead-lc7z / playhead-jw63.1: the `Bool` distinguishes an explicit
    /// No response (`true`) from a neutral x dismissal or passive auto-fade
    /// (`false`). Only the explicit No is treated downstream as a
    /// `.falsePositive` correction — a banner the user ignored for the full
    /// dwell is too weak a signal to mint a hard negative.
    var onSuggestExitWithoutSkip: ((AdSkipBannerItem, Bool) -> Void)?

    init(
        autoDismissSeconds: TimeInterval = AdBannerQueue.defaultAutoDismissSeconds,
        suggestAutoDismissSeconds: TimeInterval = AdBannerQueue.defaultSuggestAutoDismissSeconds,
        coalesceGap: TimeInterval = AdBannerQueue.defaultCoalesceGap,
        autoDismissSleep: @escaping @Sendable (TimeInterval) async -> Void = { seconds in
            try? await Task.sleep(for: .seconds(seconds))
        },
        feedbackCounterStore: BannerFeedbackCounterStore? = nil,
        tallyStore: BannerTallyStore? = nil
    ) {
        self.autoDismissSeconds = autoDismissSeconds
        self.suggestAutoDismissSeconds = suggestAutoDismissSeconds
        self.coalesceGap = coalesceGap
        self.autoDismissSleep = autoDismissSleep
        self.feedbackCounterStore = feedbackCounterStore
        self.tallyStore = tallyStore
    }

    // MARK: - Public API

    /// Enqueue a new ad skip banner. If the skip is adjacent to the current
    /// or last queued item, coalesce instead of adding a new entry.
    func enqueue(_ item: AdSkipBannerItem) {
        guard acceptsHostScopedItem(item) else { return }
        enqueueAccepted(item)
    }

    /// Production enqueue path. In addition to episode identity, require the
    /// observation generation captured when the stream was attached. This
    /// prevents a buffered item from a cancelled stream from crossing a
    /// disappear/reappear or episode-transition boundary.
    @discardableResult
    func enqueue(
        _ item: AdSkipBannerItem,
        hostGeneration generation: UInt64
    ) -> Bool {
        guard generation == hostGeneration,
              acceptsHostScopedItem(item)
        else {
            return false
        }
        enqueueAccepted(item)
        return true
    }

    /// Removes an orchestrator-invalidated presentation without invoking
    /// feedback or changing aggregate counters.
    @discardableResult
    func retireWindow(
        _ retirement: AdBannerRetirement,
        hostGeneration generation: UInt64
    ) -> Bool {
        guard generation == hostGeneration,
              isHostActive,
              retirement.episodeId == hostEpisodeId,
              retirement.playbackLifecycleGeneration
                == hostPlaybackLifecycleGeneration
        else {
            return false
        }

        let matches: (AdSkipBannerItem) -> Bool = { item in
            item.windowId == retirement.windowId
                && item.episodeId == retirement.episodeId
                && item.playbackLifecycleGeneration
                    == retirement.playbackLifecycleGeneration
        }

        let priorCount = queue.count
        queue.removeAll(where: matches)
        var didRetire = queue.count != priorCount

        if let currentBanner, matches(currentBanner) {
            didRetire = true
            dismissTask?.cancel()
            dismissTask = nil
            advanceTask?.cancel()
            advanceTask = nil
            self.currentBanner = nil
            if !queue.isEmpty {
                advanceTask = Task {
                    try? await Task.sleep(for: .milliseconds(350))
                    guard !Task.isCancelled else { return }
                    advanceTask = nil
                    showNext()
                }
            }
        }
        return didRetire
    }

    private func enqueueAccepted(_ item: AdSkipBannerItem) {
        // Stream reconnection or producer replay can assign a fresh transport
        // UUID to the same logical revision. Deduplicate across the entire
        // single presentation lane, not just its tail: A may be current while
        // an unrelated B is pending when A' is replayed. Appending A' in that
        // state would surface a stale second prompt after both A and B.
        if let currentBanner,
           isDuplicateEmission(currentBanner, item) {
            return
        }
        if queue.contains(where: { isDuplicateEmission($0, item) }) {
            return
        }

        // Try to coalesce with the most recent item (current or last in queue).
        if let last = queue.last, canCoalesce(last, item) {
            // Replace with the newer item (it has the broader time range).
            queue[queue.count - 1] = item
        } else if let current = currentBanner,
                  queue.isEmpty,
                  canCoalesce(current, item) {
            // A stream replay can deliver the exact same logical presentation
            // with a fresh transport UUID while its first delivery is already
            // being answered. Do not queue that replay behind the claimed card:
            // it would surface a second, now-stale feedback prompt after the
            // durable action completes. A genuinely adjacent extension remains
            // queued so it can receive its own stable presentation after the
            // claimed receipt.
            if didClaimActionForCurrentPresentation {
                queue.append(item)
                return
            }
            // Coalesce with the currently displayed banner — update in place.
            currentBanner = item
            restartAutoDismiss()
            return
        } else {
            queue.append(item)
        }

        // If nothing is showing, pop the next one.
        // When a dismissal's slide-out pause is already advancing the lane,
        // leave new arrivals queued for that retained task. Showing one here
        // would let the delayed task overwrite it 350 ms later.
        if currentBanner == nil, advanceTask == nil {
            showNext()
        }
    }

    private func acceptsHostScopedItem(_ item: AdSkipBannerItem) -> Bool {
        guard isHostScopeConfigured else { return true }
        return isHostActive
            && item.episodeId == hostEpisodeId
            && item.playbackLifecycleGeneration
                == hostPlaybackLifecycleGeneration
    }

    /// Dismiss the current banner (user tapped dismiss or auto-dismiss fired).
    ///
    /// playhead-lc7z / playhead-jw63.1: `isExplicitDenial` is `true` only for
    /// the explicit No feedback path. Neutral x dismissal, the auto-fade
    /// timer, and internal queue-advance calls use the default `false`. The
    /// flag is forwarded to `onSuggestExitWithoutSkip` so No becomes a
    /// `.falsePositive` correction while dismissal/fade does not.
    func dismiss(isExplicitDenial: Bool = false) {
        dismissTask?.cancel()
        dismissTask = nil
        advanceTask?.cancel()
        advanceTask = nil
        // playhead-gtt9.23: notify any suggest-tier exit handler so the
        // orchestrator can clean up its in-memory suggest set when a
        // suggest banner leaves WITHOUT a Yes response (auto-fade, neutral
        // dismiss, explicit No, or queue advance). The Yes path bypasses this
        // callback by setting `currentBanner` to nil before invoking
        // `dismiss()` — see `dismissAfterAccept(_:)`.
        if let banner = currentBanner, banner.tier == .suggest {
            onSuggestExitWithoutSkip?(banner, isExplicitDenial)
        }
        currentBanner = nil

        // Show next queued banner after a brief pause so the exit animation
        // finishes before the next slide-in.
        if !queue.isEmpty {
            advanceTask = Task {
                try? await Task.sleep(for: .milliseconds(350))
                guard !Task.isCancelled else { return }
                advanceTask = nil
                showNext()
            }
        }
    }

    /// playhead-gtt9.23: Special dismiss path used after the user taps
    /// "Skip" on a suggest banner. Suppresses the
    /// `onSuggestExitWithoutSkip` callback (the orchestrator will be
    /// notified via `acceptSuggestedSkip` instead) but otherwise behaves
    /// exactly like `dismiss()`.
    func dismissAfterAccept(_ item: AdSkipBannerItem) {
        // Clear the current banner reference BEFORE invoking dismiss so
        // the suggest-exit callback short-circuits.
        if currentBanner?.id == item.id {
            currentBanner = nil
        }
        dismissTask?.cancel()
        dismissTask = nil
        advanceTask?.cancel()
        advanceTask = nil
        if !queue.isEmpty {
            advanceTask = Task {
                try? await Task.sleep(for: .milliseconds(350))
                guard !Task.isCancelled else { return }
                advanceTask = nil
                showNext()
            }
        }
    }

    /// Completes an accepted Yes/No response only if its original card still
    /// owns the lane. An in-flight response deliberately survives host cleanup,
    /// but its eventual completion must never dismiss a replacement card or
    /// cancel that replacement's dwell timer.
    @discardableResult
    func dismissAfterAcceptedFeedback(
        for item: AdSkipBannerItem
    ) -> Bool {
        guard currentBanner?.id == item.id else {
            return false
        }
        if item.tier == .suggest {
            dismissAfterAccept(item)
        } else {
            dismiss()
        }
        return true
    }

    /// Claims the one explicit feedback slot before an asynchronous production
    /// action begins. Claiming prevents double taps but deliberately does not
    /// increment an aggregate or dismiss the card.
    func claimFeedback(for item: AdSkipBannerItem) -> Bool {
        guard claimPresentationAction(
            for: item,
            cancelAutoDismiss: true
        ) else {
            return false
        }
        inFlightFeedbackItemIDs.insert(item.id)
        return true
    }

    /// Finalizes an already-claimed response after the lifecycle/revision-bound
    /// production action reports acceptance.
    func finalizeFeedback(
        _ response: BannerFeedbackResponse,
        for item: AdSkipBannerItem
    ) -> Bool {
        guard inFlightFeedbackItemIDs.remove(item.id) != nil else {
            return false
        }
        switch response {
        case .confirmed:
            feedbackCounterStore?.recordConfirmed()
        case .denied:
            feedbackCounterStore?.recordDenied()
        }
        return true
    }

    /// Releases a failed/rejected async action so the still-current card can
    /// be answered again. A stale or retired card is never resurrected.
    func releaseFeedbackClaim(for item: AdSkipBannerItem) {
        guard inFlightFeedbackItemIDs.remove(item.id) != nil else {
            return
        }
        guard currentBanner?.id == item.id,
              didClaimActionForCurrentPresentation
        else {
            // Host/lifecycle cleanup removed the presentation while the action
            // was suspended. A failed suggest response still needs the same
            // neutral cleanup that disappearance would have performed had it
            // not been protected by the in-flight claim.
            if item.tier == .suggest {
                onSuggestExitWithoutSkip?(item, false)
            }
            return
        }
        didClaimActionForCurrentPresentation = false
        restartAutoDismiss()
    }

    /// Claims an asynchronous non-feedback action, such as the durable
    /// sponsor-on-show preference, without incrementing Yes/No aggregates.
    /// Tracking the item separately lets the write finish after host cleanup
    /// while preserving retry when the same card remains current.
    func claimUtilityAction(
        for item: AdSkipBannerItem,
        cancelAutoDismiss: Bool = false
    ) -> Bool {
        guard claimPresentationAction(
            for: item,
            cancelAutoDismiss: cancelAutoDismiss
        ) else {
            return false
        }
        inFlightUtilityActionItemIDs.insert(item.id)
        return true
    }

    func finalizeUtilityAction(for item: AdSkipBannerItem) -> Bool {
        inFlightUtilityActionItemIDs.remove(item.id) != nil
    }

    func releaseUtilityActionClaim(for item: AdSkipBannerItem) {
        guard inFlightUtilityActionItemIDs.remove(item.id) != nil else {
            return
        }
        guard currentBanner?.id == item.id,
              didClaimActionForCurrentPresentation
        else {
            return
        }
        didClaimActionForCurrentPresentation = false
        restartAutoDismiss()
    }

    var hasClaimedCurrentPresentation: Bool {
        didClaimActionForCurrentPresentation
    }

    /// Atomically claims the single durable/replay action slot for a visible
    /// presentation without recording a Yes/No aggregate. Existing actions
    /// such as Listen and "Always skip this sponsor" use this so a rapid
    /// conflicting tap cannot persist two incompatible corrections.
    func claimPresentationAction(
        for item: AdSkipBannerItem,
        cancelAutoDismiss: Bool = false
    ) -> Bool {
        guard currentBanner?.id == item.id,
              !didClaimActionForCurrentPresentation
        else {
            return false
        }

        didClaimActionForCurrentPresentation = true
        if cancelAutoDismiss {
            dismissTask?.cancel()
            dismissTask = nil
        }
        return true
    }

    /// Records an impression only after the current banner card is on screen.
    ///
    /// Queue-current is not the same as user-visible: pending banners can
    /// advance and expire while `NowPlayingView` is off screen. The view calls
    /// this from the card's `onAppear` or when an obscured card becomes exposed,
    /// keeping the durable denominator tied to actual presentations. The
    /// presentation guard also absorbs repeated SwiftUI exposure callbacks or
    /// a current-item coalescing update.
    @discardableResult
    func recordBannerShown(for item: AdSkipBannerItem) -> Bool {
        guard currentBanner?.id == item.id,
              !didRecordShownForCurrentPresentation
        else {
            return false
        }

        didRecordShownForCurrentPresentation = true
        feedbackCounterStore?.recordBannerShown()
        // playhead-bfq7: instrumentation only — it shares this method's
        // existing presentation guard and returns a value nothing here
        // branches on, so it cannot move a presentation, a tier, or a
        // dismissal.
        tallyStore?.recordPresentation(of: item)
        return true
    }

    /// Pauses transient banner timers while an assistive control is active.
    ///
    /// VoiceOver and Switch Control users need an unbounded amount of time to
    /// discover and activate the explicit feedback controls. Resuming starts a
    /// fresh dwell for the current banner instead of immediately consuming the
    /// timer interval that elapsed while accessibility navigation was active.
    func setAutoDismissPaused(_ isPaused: Bool) {
        guard isAutoDismissPaused != isPaused else { return }
        isAutoDismissPaused = isPaused
        dismissTask?.cancel()
        dismissTask = nil
        if !isPaused, currentBanner != nil {
            restartAutoDismiss()
        }
    }

    /// Dismisses an inline confirmation only when transient timers are active.
    ///
    /// The "Always skip this sponsor" receipt uses a separate two-second
    /// delayed task rather than the queue's normal dwell. Route that task
    /// through the same assistive-control pause state so VoiceOver and Switch
    /// Control users do not lose the receipt while navigating it.
    @discardableResult
    func dismissConfirmationIfAllowed(for item: AdSkipBannerItem) -> Bool {
        guard !isAutoDismissPaused, currentBanner?.id == item.id else {
            return false
        }
        dismiss()
        return true
    }

    /// Retires every transient banner when its owning Now Playing surface is
    /// permanently removed. Pending suggestions also need a neutral exit
    /// callback so the orchestrator does not retain windows that no UI can
    /// answer. This is intentionally distinct from temporary exposure pauses
    /// for sheets and inactive scenes.
    func discardAllOnHostDisappear() {
        isHostScopeConfigured = true
        isHostActive = false
        hostEpisodeId = nil
        hostPlaybackLifecycleGeneration = nil
        hostGeneration &+= 1
        discardAllNeutrally()
    }

    /// Activates production enqueue gating for a mounted Now Playing host and
    /// returns the generation its banner observer must carry.
    @discardableResult
    func activateHost(
        for episodeId: String?,
        playbackLifecycleGeneration: UInt64? = nil
    ) -> UInt64 {
        isHostScopeConfigured = true
        isHostActive = true
        hostEpisodeId = episodeId
        hostPlaybackLifecycleGeneration = playbackLifecycleGeneration
        hostGeneration &+= 1
        return hostGeneration
    }

    /// Retires stale presentations when the mounted player advances to a
    /// different episode. `NowPlayingView` survives autoplay/queue advancement,
    /// so its disappearance hook alone is not an episode-lifetime boundary.
    /// Treating this as a neutral exit prevents an old suggestion from being
    /// confirmed after `SkipOrchestrator.beginEpisode` has already discarded
    /// the corresponding window.
    @discardableResult
    func discardAllOnEpisodeChange(
        from previousEpisodeId: String?,
        to currentEpisodeId: String?
    ) -> UInt64 {
        discardAllOnPlaybackContextChange(
            fromEpisodeId: previousEpisodeId,
            toEpisodeId: currentEpisodeId,
            fromPlaybackLifecycleGeneration:
                hostPlaybackLifecycleGeneration,
            toPlaybackLifecycleGeneration:
                hostPlaybackLifecycleGeneration
        )
    }

    /// Retires stale presentations whenever either the episode identity or
    /// the playback request lifecycle changes. The latter catches replay and
    /// replacement of the same canonical episode.
    @discardableResult
    func discardAllOnPlaybackContextChange(
        fromEpisodeId previousEpisodeId: String?,
        toEpisodeId currentEpisodeId: String?,
        fromPlaybackLifecycleGeneration previousLifecycleGeneration: UInt64?,
        toPlaybackLifecycleGeneration currentLifecycleGeneration: UInt64?
    ) -> UInt64 {
        guard previousEpisodeId != currentEpisodeId
                || previousLifecycleGeneration != currentLifecycleGeneration
        else {
            return hostGeneration
        }
        isHostScopeConfigured = true
        isHostActive = true
        hostEpisodeId = currentEpisodeId
        hostPlaybackLifecycleGeneration = currentLifecycleGeneration
        hostGeneration &+= 1
        discardAllNeutrally()
        return hostGeneration
    }

    private func discardAllNeutrally() {
        dismissTask?.cancel()
        dismissTask = nil
        advanceTask?.cancel()
        advanceTask = nil

        let discarded = [currentBanner].compactMap { $0 } + queue
        let claimedCurrentID = currentBanner.flatMap {
            inFlightFeedbackItemIDs.contains($0.id) ? $0.id : nil
        }
        currentBanner = nil
        queue.removeAll()
        didClaimActionForCurrentPresentation = false
        didRecordShownForCurrentPresentation = false

        for banner in discarded
        where banner.tier == .suggest && banner.id != claimedCurrentID {
            onSuggestExitWithoutSkip?(banner, false)
        }
    }

    // MARK: - Private

    private func showNext() {
        // A retained advance task may wake after a new event has arrived.
        // Never replace an already-presented banner in the single lane.
        guard currentBanner == nil, !queue.isEmpty else { return }
        didClaimActionForCurrentPresentation = false
        didRecordShownForCurrentPresentation = false
        currentBanner = queue.removeFirst()
        restartAutoDismiss()
    }

    private func restartAutoDismiss() {
        dismissTask?.cancel()
        dismissTask = nil
        guard !isAutoDismissPaused, currentBanner != nil else { return }
        let dwell: TimeInterval = {
            switch currentBanner?.tier {
            case .suggest: return suggestAutoDismissSeconds
            case .autoSkipped, .none: return autoDismissSeconds
            }
        }()
        let sleep = autoDismissSleep
        dismissTask = Task {
            await sleep(dwell)
            guard !Task.isCancelled else { return }
            dismiss()
        }
    }

    /// Internal hook so tier-aware tests can read the dwell choice without
    /// driving the live SwiftUI hierarchy.
    static func dwellSeconds(for tier: AdBannerTier) -> TimeInterval {
        switch tier {
        case .suggest: return defaultSuggestAutoDismissSeconds
        case .autoSkipped: return defaultAutoDismissSeconds
        }
    }

    /// Test seam for capturing the exact auto-dismiss task scheduled by
    /// `restartAutoDismiss()` before the test releases its controlled
    /// sleep.
    func autoDismissTaskForTesting() -> Task<Void, Never>? {
        dismissTask
    }

    /// playhead-dd7d: Test seam for awaiting the deferred queue-advance
    /// spawned by `dismiss()` / `dismissAfterAccept()`. Awaiting the
    /// returned task's `.value` blocks until the 350 ms slide-in delay has
    /// elapsed AND `showNext()` has advanced `currentBanner` — a
    /// deterministic completion signal that replaces racing a fixed
    /// wall-clock sleep against the internal delay in tests. Returns nil if
    /// no advance was scheduled (the queue was empty at dismiss time).
    func advanceTaskForTesting() -> Task<Void, Never>? {
        advanceTask
    }

    /// Two banners coalesce if they are close in time (adjacent/near-adjacent skips).
    ///
    /// playhead-gtt9.23: tier mismatch suppresses coalescing. An auto-skipped
    /// banner ("we just skipped") and a suggest banner ("skip this?") have
    /// different intents and different copy; collapsing them into one cell
    /// would lose the distinction the user relies on to know what happened.
    private func canCoalesce(_ a: AdSkipBannerItem, _ b: AdSkipBannerItem) -> Bool {
        guard a.tier == b.tier,
              a.episodeId == b.episodeId,
              a.playbackLifecycleGeneration
                == b.playbackLifecycleGeneration,
              a.suggestionRevisionToken == b.suggestionRevisionToken
        else {
            return false
        }
        // Every response targets one precise orchestrator window. Folding
        // distinct auto or suggest windows into one card would discard the
        // first identity: the eventual Yes/No could act only on the
        // replacement. Duplicate emissions for the same window may still
        // coalesce safely.
        if a.windowId != b.windowId {
            return false
        }
        // Interval distance is zero for an exact replay or overlapping update.
        // The previous one-directional `abs(a.end - b.start)` check treated a
        // normal 30-second replay as 30 seconds apart, queuing a duplicate card
        // for the same revision.
        let intervalGap = max(
            0,
            max(a.adStartTime, b.adStartTime)
                - min(a.adEndTime, b.adEndTime)
        )
        return intervalGap <= coalesceGap
    }

    private func isDuplicateEmission(
        _ a: AdSkipBannerItem,
        _ b: AdSkipBannerItem
    ) -> Bool {
        a.windowId == b.windowId
            && a.tier == b.tier
            && a.episodeId == b.episodeId
            && a.playbackLifecycleGeneration
                == b.playbackLifecycleGeneration
            && a.suggestionRevisionToken == b.suggestionRevisionToken
            && a.adStartTime == b.adStartTime
            && a.adEndTime == b.adEndTime
    }
}

// MARK: - AdBannerView

/// The banner overlay. Positioned at the bottom of the Now Playing screen.
/// Slides in from below, slides out on dismiss.
struct AdBannerView: View {

    var queue: AdBannerQueue
    /// Whether the Now Playing surface is unobscured by an app-owned modal.
    /// Scene activity is folded in separately below.
    var isPresentationVisible: Bool = true

    /// Production action-time episode guard. Queue gating rejects late
    /// arrivals after SwiftUI observes an episode transition; this second
    /// check covers the brief interval after the runtime changes episodes but
    /// before the view's `onChange` cleanup has rendered.
    var isItemCurrent: ((AdSkipBannerItem) -> Bool)?

    /// Called when the user taps "Listen" to jump back to the skipped ad.
    var onListen: ((AdSkipBannerItem) -> Void)?
    /// Production acceptance contract for Listen. A failed durable transaction
    /// releases the action slot and leaves a still-current card retryable.
    var onListenAsync: ((AdSkipBannerItem) async -> Bool)?

    /// Production persistence contract for Yes on an auto-skipped banner.
    /// The answer is unavailable without a durable sink.
    var onAutoSkipConfirmed: ((AdSkipBannerItem) -> Void)?
    var onAutoSkipConfirmedAsync: ((AdSkipBannerItem) async -> Bool)?

    /// Phase 7.2: Called when the user answers No on an auto-skipped banner.
    /// Production routes this through the existing precise correction path.
    var onNotAnAd: ((AdSkipBannerItem) -> Void)?
    /// Production acceptance contract for the same action. The aggregate and
    /// dismissal are finalized only when this lifecycle-bound operation
    /// returns `true`. The synchronous seam above remains for previews and
    /// isolated tests.
    var onNotAnAdAsync: ((AdSkipBannerItem) async -> Bool)?

    /// playhead-gtt9.23 / playhead-jw63.1: Called when the user answers Yes on
    /// a suggest-tier banner. The orchestrator promotes the suggested span
    /// into the active skip path and records a `.falseNegative` correction.
    /// Auto-skipped banners ignore the callback.
    var onSuggestSkip: ((AdSkipBannerItem) -> Void)?
    /// Production acceptance contract for suggest Yes.
    var onSuggestSkipAsync: ((AdSkipBannerItem) async -> Bool)?

    /// Production acceptance contract for an explicit suggest No. Neutral
    /// dismiss/fade still uses the queue's fire-and-forget cleanup callback.
    var onSuggestDeclineAsync: ((AdSkipBannerItem) async -> Bool)?

    /// playhead-3bv.4: Called when the user taps "Always skip this sponsor"
    /// on an auto-skipped banner. The host records a `sponsorOnShow`
    /// scope correction in `UserCorrectionStore` so future episodes of
    /// the same show veto this advertiser proactively. The button is
    /// hidden when this callback is nil, when the banner has no
    /// advertiser name to scope against, or when the banner is
    /// suggest-tier (the action only applies to confirmed auto-skips —
    /// "always skip" presupposes we just successfully skipped it).
    var onAlwaysSkipSponsor: ((AdSkipBannerItem) -> Void)?
    /// Production persistence contract for "Always skip this sponsor." The
    /// inline success receipt is shown only after the correction store accepts
    /// the write; a rejected write leaves the current card retryable.
    var onAlwaysSkipSponsorAsync: ((AdSkipBannerItem) async -> Bool)?

    /// Injected haptic player — defaults to `SystemHapticPlayer` in
    /// production, tests swap in a `RecordingHapticPlayer`.
    var hapticPlayer: any HapticPlaying = SystemHapticPlayer()

    /// playhead-jw63.5: banner-context entry into the feedback channel.
    ///
    /// Reached by LONG-PRESSING the card, never by a visible control. That
    /// is the whole design: the banner is the moment a listener actually
    /// feels "this was wrong", so it is the highest-value place to start a
    /// note — but the card already carries Yes / No / Listen / Always skip /
    /// evidence / dismiss, and the one-tap Yes/No (playhead-jw63.1) is the
    /// signal we most want. A seventh visible control would compete with it
    /// and cost every listener attention to ignore. A context menu costs
    /// zero pixels and zero taps on the primary path, and is the standard
    /// iOS idiom for "more about this specific thing".
    ///
    /// No lifecycle/identity guard: unlike every other action here this one
    /// mutates nothing, and requiring the card to still be current would
    /// disable the channel exactly when something has gone wrong.
    ///
    /// Known bound: the auto-dismiss timer is NOT paused while the menu is
    /// open (SwiftUI's `.contextMenu` exposes no presentation callback to
    /// hang that on). A long press landing in the last half-second of the
    /// 8 s / 12 s dwell can therefore close the menu with the card. No state
    /// is left inconsistent — the handler simply never fires — and the
    /// Settings entry is the always-available path, so this is a bounded
    /// miss rather than a dead end.
    ///
    /// `nil` in previews and isolated tests — the menu is then not attached
    /// at all, so a long press behaves as it did before this bead.
    var onTellUsWhatHappened: ((AdSkipBannerItem) -> Void)?

    /// playhead-vjxc: Tracks whether the user has tapped the disclosure
    /// chevron to expand the evidence detail. Keyed by banner id so the
    /// expansion never carries over when the queue advances to the next
    /// banner — every new banner starts collapsed (default ergonomics).
    @State private var expandedBannerId: String?

    /// playhead-3bv.4: Tracks the banner id that just received an
    /// "Always skip this sponsor" tap. Drives the inline confirmation
    /// replacement of the action row ("Will always skip this sponsor")
    /// without dismissing the entire banner — the confirmation is the
    /// receipt the user needs. A short delayed dismiss closes the
    /// banner after the user has had time to read the line.
    @State private var confirmedAlwaysSkipBannerId: String?
    @Environment(\.accessibilityVoiceOverEnabled)
    private var accessibilityVoiceOverEnabled
    @Environment(\.accessibilitySwitchControlEnabled)
    private var accessibilitySwitchControlEnabled
    @Environment(\.scenePhase)
    private var scenePhase
    @Environment(\.dynamicTypeSize)
    private var dynamicTypeSize

    /// Duration before the banner auto-dismisses after the inline
    /// "Will always skip this sponsor" confirmation appears. Short
    /// enough that it never feels like a modal; long enough to read.
    static let alwaysSkipConfirmationSeconds: TimeInterval = 2.0

    /// Shared, deliberately plain copy for both banner tiers. Keeping the
    /// question and answers identical makes the interaction learnable without
    /// introducing confidence language or dashboard-like terminology.
    static let feedbackPrompt = "Was this right?"
    static let confirmFeedbackLabel = "Yes"
    static let denyFeedbackLabel = "No"
    static let feedbackMinimumTapSize: CGFloat = 44

    /// Semantic roles used by every essential banner text surface. Unlike the
    /// fixed-size font factories, these roles are created relative to a text
    /// style and therefore scale with Dynamic Type.
    static let primaryCopyTypographyRole: TypographyRole = .caption
    static let detailCopyTypographyRole: TypographyRole = .timestamp
    static let evidenceTypographyRole: TypographyRole = .caption
    static let confirmationTypographyRole: TypographyRole = .caption

    /// Accessibility categories always receive the stacked utility layout.
    /// Smaller categories still use `ViewThatFits` below, which falls back to
    /// the same layout whenever the available card width is too narrow.
    static func autoSkippedUtilityUsesStackedLayout(
        for dynamicTypeSize: DynamicTypeSize
    ) -> Bool {
        dynamicTypeSize.isAccessibilitySize
    }

    /// Essential subject copy follows the same accessibility-size stacking
    /// rule as the utility actions. At smaller categories `ViewThatFits`
    /// still selects the stacked fallback when a compact card cannot fit the
    /// full single-line treatment.
    static func bannerHeaderUsesStackedLayout(
        for dynamicTypeSize: DynamicTypeSize
    ) -> Bool {
        dynamicTypeSize.isAccessibilitySize
    }

    /// The feedback question and both answers are essential content. At
    /// accessibility sizes they receive a dedicated question row so neither
    /// 44-point answer target is compressed or pushed outside the card.
    /// Standard sizes still use `ViewThatFits` to select the same fallback on
    /// compact devices or unusually long localized copy.
    static func feedbackChoiceUsesStackedLayout(
        for dynamicTypeSize: DynamicTypeSize
    ) -> Bool {
        dynamicTypeSize.isAccessibilitySize
    }

    /// Expanded evidence stays concise at standard sizes but must not truncate
    /// the explanation a low-vision user explicitly requested.
    static func expandedEvidenceLineLimit(
        for dynamicTypeSize: DynamicTypeSize
    ) -> Int? {
        dynamicTypeSize.isAccessibilitySize ? nil : 2
    }

    /// Canonical sponsor key shared by visibility, action eligibility, and
    /// production persistence. Keeping one normalizer prevents a whitespace-
    /// only label from showing a receipt for an action the sink rejects.
    static func normalizedAlwaysSkipSponsor(_ advertiser: String) -> String {
        let normalized = advertiser
            .lowercased()
            .trimmingCharacters(in: .whitespaces)
        // SponsorKnowledgeStore intentionally defines identity by trimming
        // `.whitespaces`, so preserve that exact non-empty key. Use the wider
        // set only as a blankness probe: a newline-only model value must not
        // expose a durable action, while a non-empty key keeps matching the
        // downstream store byte-for-byte.
        return normalized
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .isEmpty ? "" : normalized
    }

    struct FeedbackChoiceContent: Equatable {
        let prompt: String
        let confirmLabel: String
        let denyLabel: String
        let confirmAccessibilityLabel: String
        let confirmAccessibilityHint: String
        let denyAccessibilityLabel: String
        let denyAccessibilityHint: String
    }

    /// Tier-specific content for the one shared feedback row rendered by
    /// `bannerCard`. Keeping the visible and accessibility copy in one value
    /// lets tests verify the actual content consumed by the controls without
    /// inspecting Swift source text.
    static func feedbackChoiceContent(for tier: AdBannerTier) -> FeedbackChoiceContent {
        switch tier {
        case .suggest:
            return FeedbackChoiceContent(
                prompt: feedbackPrompt,
                confirmLabel: confirmFeedbackLabel,
                denyLabel: denyFeedbackLabel,
                confirmAccessibilityLabel: "Yes, skip this sponsor break",
                confirmAccessibilityHint: "Confirms this is an ad and skips it",
                denyAccessibilityLabel: "No, this was not an ad",
                denyAccessibilityHint: "Marks this suggestion wrong and leaves playback unchanged"
            )
        case .autoSkipped:
            return FeedbackChoiceContent(
                prompt: feedbackPrompt,
                confirmLabel: confirmFeedbackLabel,
                denyLabel: denyFeedbackLabel,
                confirmAccessibilityLabel: "Yes, the skip was right",
                confirmAccessibilityHint: "Confirms Playhead skipped an ad",
                denyAccessibilityLabel: "No, this was not an ad",
                // `revertWindow` records the correction and removes the skip
                // cue, but deliberately does not rewind playback. Keep this
                // about the correction rather than promising Listen's rewind.
                denyAccessibilityHint: "Records that this skipped segment was not an ad"
            )
        }
    }

    private var isAssistiveControlActive: Bool {
        accessibilityVoiceOverEnabled || accessibilitySwitchControlEnabled
    }

    private var isPresentationExposed: Bool {
        isPresentationVisible && scenePhase == .active
    }

    private func isCurrentHostItem(_ item: AdSkipBannerItem) -> Bool {
        isItemCurrent?(item) ?? true
    }

    /// Factored handler for the banner-appear haptic so tests can drive
    /// it without rendering a live SwiftUI hierarchy.
    func handleBannerAppear() {
        hapticPlayer.play(.notice)
    }

    /// Card-appearance handler used by production and behavioral tests.
    ///
    /// The queue accepts the appearance once per presentation. This couples
    /// the haptic and durable shown count to the same real display boundary.
    func handleBannerAppear(for item: AdSkipBannerItem) {
        guard isCurrentHostItem(item),
              queue.recordBannerShown(for: item)
        else {
            return
        }
        handleBannerAppear()
    }

    /// Synchronizes timers and the impression boundary with actual exposure.
    ///
    /// A mounted SwiftUI card may still be hidden beneath a sheet or belong to
    /// an inactive/background scene. Those states pause the transient timer and
    /// defer the shown count until the surface is exposed again. Assistive
    /// controls pause timing without suppressing a genuinely visible
    /// impression.
    func handlePresentationExposureChange(
        isExposed: Bool,
        isAssistiveControlActive: Bool
    ) {
        queue.setAutoDismissPaused(
            !isExposed || isAssistiveControlActive
        )
        if isExposed, let item = queue.currentBanner {
            handleBannerAppear(for: item)
        }
    }

    /// Records a replacement that becomes current while the banner surface is
    /// already mounted and exposed. SwiftUI can coalesce a rapid
    /// `current -> nil -> replacement` transition into one render pass, so the
    /// replacement card's `onAppear` is not a reliable presentation boundary
    /// on its own. The queue's presentation-scoped guard keeps this idempotent
    /// when `onAppear` also fires.
    func handleCurrentBannerIdentityChange(isExposed: Bool) {
        if isExposed, let item = queue.currentBanner {
            handleBannerAppear(for: item)
        }
    }

    /// The existing Listen path is itself a precise false-positive correction.
    /// Consume and dismiss the presentation so a subsequent Yes cannot record
    /// a contradictory aggregate label after that revert.
    @discardableResult
    func handleListen(for item: AdSkipBannerItem) -> Bool {
        guard isCurrentHostItem(item),
              queue.currentBanner?.id == item.id,
              let onListen,
              onListenAsync == nil
        else {
            return false
        }
        guard queue.claimUtilityAction(
            for: item,
            cancelAutoDismiss: true
        ) else {
            return false
        }
        onListen(item)
        guard queue.finalizeUtilityAction(for: item) else {
            return false
        }
        queue.dismissAfterAcceptedFeedback(for: item)
        return true
    }

    @discardableResult
    func handleListenAwaitingAction(
        for item: AdSkipBannerItem
    ) async -> Bool {
        guard isCurrentHostItem(item),
              queue.currentBanner?.id == item.id,
              onListenAsync != nil || onListen != nil,
              queue.claimUtilityAction(
                for: item,
                cancelAutoDismiss: true
              )
        else {
            return false
        }

        let accepted: Bool
        if let onListenAsync {
            accepted = await onListenAsync(item)
        } else if let onListen {
            onListen(item)
            accepted = true
        } else {
            accepted = false
        }
        guard accepted else {
            queue.releaseUtilityActionClaim(for: item)
            return false
        }
        guard queue.finalizeUtilityAction(for: item) else {
            return false
        }
        queue.dismissAfterAcceptedFeedback(for: item)
        return true
    }

    /// Performs a neutral x dismissal only while the control's captured item
    /// is still the active presentation. SwiftUI can deliver a queued tap after
    /// an episode/generation replacement; that stale gesture must not dismiss
    /// the newer card now occupying the shared lane.
    @discardableResult
    func handleNeutralDismiss(for item: AdSkipBannerItem) -> Bool {
        guard isCurrentHostItem(item),
              queue.currentBanner?.id == item.id,
              !queue.hasClaimedCurrentPresentation
        else {
            return false
        }
        queue.dismiss()
        return true
    }

    /// Persists the durable sponsor-on-show action at most once for this
    /// presentation and reserves the same action slot used by feedback and
    /// Listen. The banner remains visible briefly to show its inline receipt.
    @discardableResult
    func handleAlwaysSkipSponsor(for item: AdSkipBannerItem) -> Bool {
        guard isCurrentHostItem(item),
              item.tier == .autoSkipped,
              let advertiser = item.advertiser,
              !Self.normalizedAlwaysSkipSponsor(advertiser).isEmpty,
              let onAlwaysSkipSponsor,
              onAlwaysSkipSponsorAsync == nil,
              queue.claimUtilityAction(
                for: item,
                cancelAutoDismiss: true
              )
        else {
            return false
        }

        onAlwaysSkipSponsor(item)
        guard queue.finalizeUtilityAction(for: item) else {
            return false
        }
        confirmedAlwaysSkipBannerId = item.id
        return true
    }

    /// Persistence-first production path for the durable sponsor preference.
    /// The action slot is claimed before suspension for tap deduplication, but
    /// rejection neither shows a receipt nor consumes a still-current card.
    @discardableResult
    func handleAlwaysSkipSponsorAwaitingPersistence(
        for item: AdSkipBannerItem
    ) async -> Bool {
        guard isCurrentHostItem(item),
              item.tier == .autoSkipped,
              let advertiser = item.advertiser,
              !Self.normalizedAlwaysSkipSponsor(advertiser).isEmpty,
              onAlwaysSkipSponsorAsync != nil || onAlwaysSkipSponsor != nil,
              queue.claimUtilityAction(
                for: item,
                cancelAutoDismiss: true
              )
        else {
            return false
        }

        let accepted: Bool
        if let onAlwaysSkipSponsorAsync {
            accepted = await onAlwaysSkipSponsorAsync(item)
        } else if let onAlwaysSkipSponsor {
            onAlwaysSkipSponsor(item)
            accepted = true
        } else {
            accepted = false
        }

        guard accepted else {
            queue.releaseUtilityActionClaim(for: item)
            return false
        }
        guard queue.finalizeUtilityAction(for: item) else {
            return false
        }
        confirmedAlwaysSkipBannerId = item.id
        return true
    }

    /// Whether this view has the existing tier action required to honor a
    /// response. Production wires every path; previews and isolated hosts can
    /// omit callbacks, so their unavailable control is disabled rather than
    /// recording a label that cannot perform its advertised action.
    func isFeedbackResponseAvailable(
        _ response: BannerFeedbackResponse,
        for item: AdSkipBannerItem
    ) -> Bool {
        guard isCurrentHostItem(item),
              !queue.hasClaimedCurrentPresentation
        else {
            return false
        }
        switch (item.tier, response) {
        case (.autoSkipped, .confirmed):
            return onAutoSkipConfirmedAsync != nil
                || onAutoSkipConfirmed != nil
        case (.autoSkipped, .denied):
            return onNotAnAdAsync != nil || onNotAnAd != nil
        case (.suggest, .confirmed):
            return onSuggestSkipAsync != nil || onSuggestSkip != nil
        case (.suggest, .denied):
            return onSuggestDeclineAsync != nil
                || queue.onSuggestExitWithoutSkip != nil
        }
    }

    /// Synchronous feedback seam retained for previews and isolated tests.
    /// Production supplies async acceptance callbacks and uses
    /// `handleFeedbackAwaitingAction`.
    @discardableResult
    func handleFeedback(
        _ response: BannerFeedbackResponse,
        for item: AdSkipBannerItem
    ) -> Bool {
        guard isFeedbackResponseAvailable(response, for: item) else {
            return false
        }

        // Never report synchronous success when production installed an async
        // contract for this route.
        if (item.tier == .autoSkipped
                && response == .confirmed
                && onAutoSkipConfirmedAsync != nil)
            || (item.tier == .autoSkipped
                && response == .denied
                && onNotAnAdAsync != nil)
            || (item.tier == .suggest
                && response == .confirmed
                && onSuggestSkipAsync != nil)
            || (item.tier == .suggest
                && response == .denied
                && onSuggestDeclineAsync != nil) {
            return false
        }

        guard queue.claimFeedback(for: item) else { return false }
        switch (item.tier, response) {
        case (.autoSkipped, .confirmed):
            guard let onAutoSkipConfirmed else {
                queue.releaseFeedbackClaim(for: item)
                return false
            }
            onAutoSkipConfirmed(item)

        case (.autoSkipped, .denied):
            guard let onNotAnAd else {
                queue.releaseFeedbackClaim(for: item)
                return false
            }
            onNotAnAd(item)

        case (.suggest, .confirmed):
            guard let onSuggestSkip else {
                queue.releaseFeedbackClaim(for: item)
                return false
            }
            onSuggestSkip(item)

        case (.suggest, .denied):
            guard let onExit = queue.onSuggestExitWithoutSkip else {
                queue.releaseFeedbackClaim(for: item)
                return false
            }
            onExit(item, true)
        }

        guard queue.finalizeFeedback(response, for: item) else {
            return false
        }
        queue.dismissAfterAcceptedFeedback(for: item)
        return true
    }

    /// Handles the explicit Yes/No choice through the production acceptance
    /// contract. The presentation is claimed immediately for deduplication,
    /// but its aggregate and dismissal are committed only after the
    /// lifecycle/revision-bound actor operation accepts the response.
    @discardableResult
    func handleFeedbackAwaitingAction(
        _ response: BannerFeedbackResponse,
        for item: AdSkipBannerItem
    ) async -> Bool {
        guard isFeedbackResponseAvailable(response, for: item),
              queue.claimFeedback(for: item)
        else {
            return false
        }

        let accepted: Bool
        switch (item.tier, response) {
        case (.autoSkipped, .confirmed):
            if let onAutoSkipConfirmedAsync {
                accepted = await onAutoSkipConfirmedAsync(item)
            } else if let onAutoSkipConfirmed {
                onAutoSkipConfirmed(item)
                accepted = true
            } else {
                accepted = false
            }

        case (.autoSkipped, .denied):
            if let onNotAnAdAsync {
                accepted = await onNotAnAdAsync(item)
            } else if let onNotAnAd {
                onNotAnAd(item)
                accepted = true
            } else {
                accepted = false
            }

        case (.suggest, .confirmed):
            if let onSuggestSkipAsync {
                accepted = await onSuggestSkipAsync(item)
            } else if let onSuggestSkip {
                onSuggestSkip(item)
                accepted = true
            } else {
                accepted = false
            }

        case (.suggest, .denied):
            if let onSuggestDeclineAsync {
                accepted = await onSuggestDeclineAsync(item)
            } else if let onExit = queue.onSuggestExitWithoutSkip {
                onExit(item, true)
                accepted = true
            } else {
                accepted = false
            }
        }

        guard accepted else {
            queue.releaseFeedbackClaim(for: item)
            return false
        }
        guard queue.finalizeFeedback(response, for: item) else {
            return false
        }
        queue.dismissAfterAcceptedFeedback(for: item)
        return true
    }

    var body: some View {
        VStack {
            Spacer()

            if let banner = queue.currentBanner {
                bannerCard(banner)
                    .transition(
                        .move(edge: .bottom)
                        .combined(with: .opacity)
                    )
                    .padding(.horizontal, Spacing.md)
                    .padding(.bottom, Spacing.sm)
            }
        }
        .animation(Motion.standard, value: queue.currentBanner?.id)
        .onAppear {
            handlePresentationExposureChange(
                isExposed: isPresentationExposed,
                isAssistiveControlActive: isAssistiveControlActive
            )
        }
        .onDisappear {
            handlePresentationExposureChange(
                isExposed: false,
                isAssistiveControlActive: isAssistiveControlActive
            )
        }
        .onChange(of: isPresentationExposed) { _, isExposed in
            handlePresentationExposureChange(
                isExposed: isExposed,
                isAssistiveControlActive: isAssistiveControlActive
            )
        }
        .onChange(of: isAssistiveControlActive) { _, isActive in
            handlePresentationExposureChange(
                isExposed: isPresentationExposed,
                isAssistiveControlActive: isActive
            )
        }
        .onChange(of: queue.currentBanner?.id) { _, _ in
            // Always start each new banner collapsed so the default
            // ergonomics (compact, low-attention margin note) survive
            // queued skips.
            expandedBannerId = nil
            // playhead-3bv.4: drop any stale "Always skip this sponsor"
            // confirmation state when the queue advances. Without this,
            // the delayed auto-dismiss task scheduled for the previous
            // banner would see `confirmedAlwaysSkipBannerId == item.id`
            // still true (because @State doesn't reset on its own) and
            // call `queue.dismiss()` on the WRONG (newer) banner.
            confirmedAlwaysSkipBannerId = nil
            handleCurrentBannerIdentityChange(
                isExposed: isPresentationExposed
            )
        }
    }

    // MARK: - Copy Logic

    /// Minimum metadata confidence required to surface advertiser/product.
    /// Below this, the banner falls back to generic "Skipped sponsor segment".
    static let metadataConfidenceThreshold: Double = 0.60

    /// playhead-b6jq PR 5: whether to render the subtle specialist-provenance
    /// glyph on this banner. TRUE only for a suggest-tier banner whose window was
    /// composed by the on-device specialist (`metadataSource == "specialist-v1"`,
    /// stamped by `SpecialistMarkComposer`). Auto-skipped banners and non-specialist
    /// suggest banners never show it. Factored out as a pure static so the predicate
    /// is unit-testable without rendering SwiftUI.
    static func showsSpecialistGlyph(for item: AdSkipBannerItem) -> Bool {
        item.tier == .suggest && item.metadataSource == SpecialistMarkComposer.metadataSource
    }

    /// Resolve the banner copy line from metadata, applying strict
    /// evidence-bound rules. Never surfaces a brand solely from a model guess.
    ///
    /// playhead-gtt9.23: branches on the banner's tier. Auto-skipped banners
    /// keep the existing "Skipped …" voice (a completed observation).
    /// Suggest banners use a calm "Sounds like a sponsor break." voice
    /// paired with the shared Yes/No feedback choice — never quantified, never
    /// "X% confidence." Per `feedback_peace_of_mind_not_metrics`,
    /// suggest copy describes what was heard, not how sure we are.
    static func bannerCopy(for item: AdSkipBannerItem) -> BannerCopyLine {
        // Only surface specific copy when:
        // 1. metadataSource is not "none" (metadata was actually extracted)
        // 2. metadataConfidence exceeds the threshold
        // 3. evidenceText was present (advertiser came from transcript, not a guess)
        let hasStrongEvidence: Bool = {
            guard item.metadataSource != "none",
                  let confidence = item.metadataConfidence,
                  confidence >= metadataConfidenceThreshold
            else { return false }
            return true
        }()

        switch item.tier {
        case .autoSkipped:
            if hasStrongEvidence, let advertiser = item.advertiser {
                return BannerCopyLine(
                    prefix: "Skipped",
                    advertiser: advertiser,
                    detail: item.product
                )
            }
            // Weak or missing evidence: generic copy, never hallucinated names.
            return BannerCopyLine(
                prefix: "Skipped sponsor segment",
                advertiser: nil,
                detail: nil
            )

        case .suggest:
            if hasStrongEvidence, let advertiser = item.advertiser {
                return BannerCopyLine(
                    prefix: "Sounds like a sponsor break",
                    advertiser: advertiser,
                    detail: item.product
                )
            }
            // No reliable advertiser → calm generic prompt. Phrasing is a
            // declarative observation ("Sounds like …") rather than a
            // hedged probability claim ("might be …" / "X% confident") —
            // the voice is the thing we're protecting from quantified
            // language.
            return BannerCopyLine(
                prefix: "Sounds like a sponsor break",
                advertiser: nil,
                detail: nil
            )
        }
    }

    /// Template-driven banner copy. Never free-form.
    struct BannerCopyLine: Equatable {
        let prefix: String
        let advertiser: String?
        let detail: String?
    }

    // MARK: - Evidence Copy (playhead-vjxc)

    /// Maximum number of evidence lines surfaced when the banner is expanded.
    /// Caps the list at a glanceable height — power users still get the gist
    /// without the banner becoming a full-page transcript.
    static let evidenceLineLimit: Int = 3

    /// Translate a single deterministic evidence entry into a calm,
    /// user-facing line. Pure function — no side effects, no localized
    /// strings (yet — playhead is en-US only at the MVP).
    ///
    /// Voice: every line should read as a quiet observation, never a metric
    /// or a counter. The verbatim transcript text is preserved in quotes
    /// where it is short and self-explanatory; brand names and codes are
    /// surfaced unquoted as they read more naturally that way.
    static func evidenceLine(for entry: EvidenceEntry) -> String {
        let cleaned = entry.matchedText.trimmingCharacters(in: .whitespacesAndNewlines)
        switch entry.category {
        case .disclosurePhrase:
            return "Sponsor disclosure: \u{201C}\(cleaned)\u{201D}"
        case .url:
            return "Sponsor link: \(cleaned)"
        case .promoCode:
            return "Promo code: \(cleaned)"
        case .ctaPhrase:
            return "Sponsor cue: \u{201C}\(cleaned)\u{201D}"
        case .brandSpan:
            // Preserve the matched casing — the catalog already canonicalizes
            // common variants, and ASR-lowercased brand names ("betterhelp")
            // still read clearly in this context. We deliberately do NOT
            // re-titlecase here: a forced "Hellofresh" would look uglier
            // than the verbatim "hellofresh" the user actually heard.
            return "Sponsor mention: \(cleaned)"
        }
    }

    /// Build the ordered list of evidence lines surfaced in the expanded
    /// banner detail. Deduplicates by line text (cheap defense against the
    /// same brand or URL surfacing twice through different category passes)
    /// and caps at `evidenceLineLimit`.
    ///
    /// Ordering priority (most concrete first):
    /// 1. promoCode — the line a listener is most likely to recognize
    /// 2. url — the second most concrete signal
    /// 3. disclosurePhrase — names the read explicitly
    /// 4. brandSpan — names the advertiser when no disclosure landed
    /// 5. ctaPhrase — softest signal, most likely to be a false positive
    static func evidenceLines(for entries: [EvidenceEntry]) -> [String] {
        guard !entries.isEmpty else { return [] }
        let priority: [EvidenceCategory: Int] = [
            .promoCode: 0,
            .url: 1,
            .disclosurePhrase: 2,
            .brandSpan: 3,
            .ctaPhrase: 4,
        ]
        let sorted = entries.sorted { lhs, rhs in
            let l = priority[lhs.category] ?? Int.max
            let r = priority[rhs.category] ?? Int.max
            if l != r { return l < r }
            // Stable secondary key: earlier in the audio first.
            return lhs.startTime < rhs.startTime
        }
        var seen = Set<String>()
        var lines: [String] = []
        for entry in sorted {
            let line = evidenceLine(for: entry)
            // Case-insensitive dedup so "BetterHelp" and "betterhelp" don't
            // surface twice (the catalog can produce both via different
            // capture paths).
            let key = line.lowercased()
            if seen.insert(key).inserted {
                lines.append(line)
                if lines.count >= evidenceLineLimit { break }
            }
        }
        return lines
    }

    // MARK: - Banner Card

    @ViewBuilder
    private func bannerCard(_ item: AdSkipBannerItem) -> some View {
        let copy = Self.bannerCopy(for: item)
        // playhead-vjxc: only build the evidence detail strings once per
        // render so we can both decide whether to show the chevron and
        // populate the expanded list from a single source of truth.
        let evidenceLines = Self.evidenceLines(for: item.evidenceCatalogEntries)
        let isExpanded = expandedBannerId == item.id && !evidenceLines.isEmpty

        VStack(alignment: .leading, spacing: Spacing.xs) {
            bannerHeader(copy, item: item)

            // playhead-vjxc: Expanded evidence detail. Renders below the
            // top line and above the action row so the actions remain in
            // the same screen position whether collapsed or expanded.
            // Hidden entirely (graceful absence) when no catalog entries
            // overlap the skipped span.
            if isExpanded {
                VStack(alignment: .leading, spacing: Spacing.xxs) {
                    ForEach(Array(evidenceLines.enumerated()), id: \.offset) { _, line in
                        Text(line)
                            .font(
                                AppTypography.font(
                                    for: Self.evidenceTypographyRole
                                )
                            )
                            .foregroundStyle(boneText.opacity(0.75))
                            .lineLimit(
                                Self.expandedEvidenceLineLimit(
                                    for: dynamicTypeSize
                                )
                            )
                            .fixedSize(horizontal: false, vertical: true)
                    }
                }
                .accessibilityElement(children: .combine)
                .accessibilityLabel(
                    "Why we skipped: " + evidenceLines.joined(separator: ", ")
                )
                .transition(.opacity.combined(with: .move(edge: .top)))
            }

            // Every active banner gets this one shared feedback row by
            // construction; only the utility row below varies by tier. The
            // sponsor receipt is post-action state, so it replaces both rows.
            if item.tier == .autoSkipped,
               confirmedAlwaysSkipBannerId == item.id {
                alwaysSkipConfirmation(item: item)
            } else {
                VStack(spacing: Spacing.xxs) {
                    feedbackChoice(for: item)

                    switch item.tier {
                    case .autoSkipped:
                        autoSkippedActions(
                            item: item,
                            evidenceLines: evidenceLines,
                            isExpanded: isExpanded
                        )
                    case .suggest:
                        suggestActions(
                            item: item,
                            evidenceLines: evidenceLines,
                            isExpanded: isExpanded
                        )
                    }
                }
            }
        }
        .padding(.horizontal, Spacing.md)
        .padding(.vertical, Spacing.sm)
        .background(
            RoundedRectangle(cornerRadius: CornerRadius.medium)
                .fill(Palette.ink)
                .themeShadow(AppShadow.elevated)
        )
        .animation(Motion.standard, value: isExpanded)
        .accessibilityElement(children: .contain)
        .modifier(
            BannerFeedbackChannelMenu(
                label: ListenerFeedbackCopy.bannerMenuLabel,
                action: Self.showsFeedbackChannelAffordance(
                    hasHandler: onTellUsWhatHappened != nil
                ) ? { onTellUsWhatHappened?(item) } : nil
            )
        )
        .onAppear {
            // Count and haptic only when the card is actually presented.
            if isPresentationExposed {
                handleBannerAppear(for: item)
            }
        }
    }

    /// Whether the long-press feedback entry is attached to this card.
    ///
    /// Pure and separated from the view body so the coexistence rule with
    /// playhead-jw63.1 is assertable: the affordance depends ONLY on whether
    /// a handler is wired — never on tier, feedback state, or the claim
    /// flags that gate Yes/No — so it can neither disable nor be disabled by
    /// the one-tap choice.
    static func showsFeedbackChannelAffordance(hasHandler: Bool) -> Bool {
        hasHandler
    }

    @ViewBuilder
    private func bannerHeader(
        _ copy: BannerCopyLine,
        item: AdSkipBannerItem
    ) -> some View {
        if Self.bannerHeaderUsesStackedLayout(for: dynamicTypeSize) {
            stackedBannerHeader(copy, item: item)
        } else {
            ViewThatFits(in: .horizontal) {
                horizontalBannerHeader(copy, item: item)
                stackedBannerHeader(copy, item: item)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    private func horizontalBannerHeader(
        _ copy: BannerCopyLine,
        item: AdSkipBannerItem
    ) -> some View {
        HStack(spacing: 0) {
            Text(copy.prefix)
                .font(
                    AppTypography.font(
                        for: Self.primaryCopyTypographyRole
                    )
                )
                .fontWeight(.semibold)
                .foregroundStyle(AppColors.accent)

            if let advertiser = copy.advertiser {
                Text(" \u{00B7} ")
                    .font(
                        AppTypography.font(
                            for: Self.primaryCopyTypographyRole
                        )
                    )
                    .foregroundStyle(boneText)
                Text(advertiser)
                    .font(
                        AppTypography.font(
                            for: Self.primaryCopyTypographyRole
                        )
                    )
                    .fontWeight(.medium)
                    .foregroundStyle(boneText)
            }

            if let detail = copy.detail {
                Text(" \u{00B7} ")
                    .font(
                        AppTypography.font(
                            for: Self.primaryCopyTypographyRole
                        )
                    )
                    .foregroundStyle(boneText.opacity(0.6))
                Text("\"\(detail)\"")
                    .font(
                        AppTypography.font(
                            for: Self.detailCopyTypographyRole
                        )
                    )
                    .foregroundStyle(boneText.opacity(0.7))
                    .lineLimit(1)
            }

            specialistGlyph(for: item)
                .padding(.leading, Spacing.xxs)
        }
        // Expose the complete ideal width to ViewThatFits. Compact cards then
        // select the stacked alternative instead of compressing/truncating.
        .fixedSize(horizontal: true, vertical: false)
    }

    private func stackedBannerHeader(
        _ copy: BannerCopyLine,
        item: AdSkipBannerItem
    ) -> some View {
        VStack(alignment: .leading, spacing: Spacing.xxs) {
            HStack(alignment: .firstTextBaseline, spacing: Spacing.xxs) {
                Text(copy.prefix)
                    .font(
                        AppTypography.font(
                            for: Self.primaryCopyTypographyRole
                        )
                    )
                    .fontWeight(.semibold)
                    .foregroundStyle(AppColors.accent)
                    .fixedSize(horizontal: false, vertical: true)
                specialistGlyph(for: item)
            }

            if let advertiser = copy.advertiser {
                Text(advertiser)
                    .font(
                        AppTypography.font(
                            for: Self.primaryCopyTypographyRole
                        )
                    )
                    .fontWeight(.medium)
                    .foregroundStyle(boneText)
                    .fixedSize(horizontal: false, vertical: true)
            }

            if let detail = copy.detail {
                Text("\"\(detail)\"")
                    .font(
                        AppTypography.font(
                            for: Self.detailCopyTypographyRole
                        )
                    )
                    .foregroundStyle(boneText.opacity(0.7))
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    @ViewBuilder
    private func specialistGlyph(for item: AdSkipBannerItem) -> some View {
        // A quiet source badge, never confidence language. The fixed icon size
        // is deliberate; all essential textual content uses semantic roles.
        if Self.showsSpecialistGlyph(for: item) {
            Image(systemName: "waveform.badge.magnifyingglass")
                .font(AppTypography.sans(size: 11, weight: .regular))
                .foregroundStyle(AppColors.accent.opacity(0.55))
                .accessibilityLabel("Detected by on-device sponsor scan")
        }
    }

    // MARK: - Action Rows (tier-aware, playhead-gtt9.23)

    @ViewBuilder
    private func autoSkippedActions(
        item: AdSkipBannerItem,
        evidenceLines: [String],
        isExpanded: Bool
    ) -> some View {
        if Self.autoSkippedUtilityUsesStackedLayout(
            for: dynamicTypeSize
        ) {
            stackedAutoSkippedActions(
                item: item,
                evidenceLines: evidenceLines,
                isExpanded: isExpanded
            )
        } else {
            ViewThatFits(in: .horizontal) {
                HStack(spacing: Spacing.xxs) {
                    alwaysSkipSponsorAction(item: item)
                        .fixedSize(horizontal: true, vertical: false)

                    Spacer(minLength: Spacing.xs)

                    autoSkippedSecondaryActions(
                        item: item,
                        evidenceLines: evidenceLines,
                        isExpanded: isExpanded
                    )
                    .fixedSize(horizontal: true, vertical: false)
                }

                stackedAutoSkippedActions(
                    item: item,
                    evidenceLines: evidenceLines,
                    isExpanded: isExpanded
                )
            }
        }
    }

    private func stackedAutoSkippedActions(
        item: AdSkipBannerItem,
        evidenceLines: [String],
        isExpanded: Bool
    ) -> some View {
        VStack(alignment: .leading, spacing: Spacing.xxs) {
            alwaysSkipSponsorAction(item: item)
                .frame(maxWidth: .infinity, alignment: .leading)

            HStack(spacing: Spacing.xxs) {
                Spacer(minLength: 0)
                autoSkippedSecondaryActions(
                    item: item,
                    evidenceLines: evidenceLines,
                    isExpanded: isExpanded
                )
            }
        }
    }

    /// playhead-3bv.4: "Always skip this sponsor" sits on the quiet utility
    /// row, separate from the explicit Yes/No learning choice.
    @ViewBuilder
    private func alwaysSkipSponsorAction(
        item: AdSkipBannerItem
    ) -> some View {
        if onAlwaysSkipSponsorAsync != nil || onAlwaysSkipSponsor != nil,
           let advertiser = item.advertiser,
           !Self.normalizedAlwaysSkipSponsor(advertiser).isEmpty {
            Button {
                Task { @MainActor in
                    guard await handleAlwaysSkipSponsorAwaitingPersistence(
                        for: item
                    ) else {
                        return
                    }
                    // Schedule a calm auto-dismiss so the confirmation reads
                    // as a receipt, not a modal.
                    try? await Task.sleep(
                        for: .seconds(Self.alwaysSkipConfirmationSeconds)
                    )
                    // Guard against dismissing a newer banner.
                    if confirmedAlwaysSkipBannerId == item.id,
                       queue.dismissConfirmationIfAllowed(for: item) {
                        confirmedAlwaysSkipBannerId = nil
                    }
                }
            } label: {
                Text("Always skip this sponsor")
                    .font(AppTypography.caption)
                    .foregroundStyle(boneText.opacity(0.5))
                    .lineLimit(2)
                    .fixedSize(horizontal: false, vertical: true)
                    .frame(minHeight: Self.feedbackMinimumTapSize)
                    .contentShape(Rectangle())
            }
            .buttonStyle(BannerButtonStyle())
            .accessibilityLabel("Always skip this sponsor")
            .accessibilityHint(
                "Tells Playhead to skip \(advertiser) on this show without asking again"
            )
        }
    }

    private func autoSkippedSecondaryActions(
        item: AdSkipBannerItem,
        evidenceLines: [String],
        isExpanded: Bool
    ) -> some View {
        HStack(spacing: Spacing.xxs) {
            // playhead-vjxc: optional evidence chevron.
            if !evidenceLines.isEmpty {
                Button {
                    if expandedBannerId == item.id {
                        expandedBannerId = nil
                    } else {
                        expandedBannerId = item.id
                    }
                } label: {
                    Image(systemName: isExpanded ? "chevron.up" : "chevron.down")
                        .font(AppTypography.caption)
                        .fontWeight(.semibold)
                        .foregroundStyle(boneText.opacity(0.5))
                        .frame(
                            width: Self.feedbackMinimumTapSize,
                            height: Self.feedbackMinimumTapSize
                        )
                        .contentShape(Rectangle())
                }
                .buttonStyle(BannerButtonStyle())
                .accessibilityLabel(isExpanded ? "Hide evidence" : "Show evidence")
                .accessibilityHint(
                    "Reveals the signals that led Playhead to skip this segment"
                )
            }

            // Listen button — copper accent
            Button {
                Task {
                    await handleListenAwaitingAction(for: item)
                }
            } label: {
                Text("Listen")
                    .font(AppTypography.caption)
                    .fontWeight(.semibold)
                    .foregroundStyle(AppColors.accent)
                    .padding(.horizontal, Spacing.sm)
                    .padding(.vertical, Spacing.xxs)
                    .background(
                        RoundedRectangle(cornerRadius: CornerRadius.small)
                            .fill(AppColors.accent.opacity(0.12))
                    )
                    .frame(
                        minWidth: Self.feedbackMinimumTapSize,
                        minHeight: Self.feedbackMinimumTapSize
                    )
                    .contentShape(Rectangle())
            }
            .buttonStyle(BannerButtonStyle())
            .accessibilityLabel("Listen to skipped ad")
            .accessibilityHint("Rewinds to the start of the skipped ad segment")

            Button {
                handleNeutralDismiss(for: item)
            } label: {
                Image(systemName: "xmark")
                    .font(AppTypography.caption)
                    .fontWeight(.semibold)
                    .foregroundStyle(boneText.opacity(0.5))
                    .frame(
                        width: Self.feedbackMinimumTapSize,
                        height: Self.feedbackMinimumTapSize
                    )
                    .contentShape(Rectangle())
            }
            .buttonStyle(BannerButtonStyle())
            .accessibilityLabel("Dismiss banner")
        }
    }

    /// playhead-3bv.4: inline confirmation surfaced after the user taps
    /// "Always skip this sponsor". Replaces the entire action row for
    /// a short dwell so the user sees an unambiguous receipt without a
    /// modal dialog interrupting playback.
    @ViewBuilder
    private func alwaysSkipConfirmation(item: AdSkipBannerItem) -> some View {
        HStack(spacing: Spacing.xs) {
            Image(systemName: "checkmark")
                .font(.system(size: 11, weight: .semibold))
                .foregroundStyle(AppColors.accent)
                .accessibilityHidden(true)
            Text("Will always skip this sponsor")
                .font(
                    AppTypography.font(
                        for: Self.confirmationTypographyRole
                    )
                )
                .foregroundStyle(boneText.opacity(0.75))
            Spacer(minLength: 0)
        }
        .accessibilityElement(children: .combine)
        .accessibilityLabel("Will always skip this sponsor")
        .transition(.opacity)
    }

    /// Suggest-tier choice plus quiet evidence/dismiss utilities. Yes uses the
    /// existing accepted-skip path, No uses the explicit false-positive path,
    /// and fade/x remain unlabeled.
    @ViewBuilder
    private func suggestActions(
        item: AdSkipBannerItem,
        evidenceLines: [String],
        isExpanded: Bool
    ) -> some View {
        HStack {
            // Optional evidence chevron (same as auto-skipped path) so
            // the user can inspect signals without answering.
            if !evidenceLines.isEmpty {
                Button {
                    if expandedBannerId == item.id {
                        expandedBannerId = nil
                    } else {
                        expandedBannerId = item.id
                    }
                } label: {
                    Image(systemName: isExpanded ? "chevron.up" : "chevron.down")
                        .font(AppTypography.caption)
                        .fontWeight(.semibold)
                        .foregroundStyle(boneText.opacity(0.5))
                        .frame(
                            width: Self.feedbackMinimumTapSize,
                            height: Self.feedbackMinimumTapSize
                        )
                        .contentShape(Rectangle())
                }
                .buttonStyle(BannerButtonStyle())
                .accessibilityLabel(isExpanded ? "Hide evidence" : "Show evidence")
                .accessibilityHint("Reveals the signals Playhead heard for this segment")
            }

            Spacer()

            // The x is now explicitly neutral because the adjacent No
            // button is the unambiguous false-positive signal.
            Button {
                handleNeutralDismiss(for: item)
            } label: {
                Image(systemName: "xmark")
                    .font(AppTypography.caption)
                    .fontWeight(.semibold)
                    .foregroundStyle(boneText.opacity(0.5))
                    .frame(
                        width: Self.feedbackMinimumTapSize,
                        height: Self.feedbackMinimumTapSize
                    )
                    .contentShape(Rectangle())
            }
            .buttonStyle(BannerButtonStyle())
            .accessibilityLabel("Dismiss without feedback")
            .accessibilityHint("Leaves playback unchanged without answering")
        }
    }

    /// The same compact, accessible learning choice appears on every tier.
    @ViewBuilder
    private func feedbackChoice(for item: AdSkipBannerItem) -> some View {
        let content = Self.feedbackChoiceContent(for: item.tier)

        if Self.feedbackChoiceUsesStackedLayout(for: dynamicTypeSize) {
            stackedFeedbackChoice(content: content, item: item)
        } else {
            ViewThatFits(in: .horizontal) {
                horizontalFeedbackChoice(content: content, item: item)
                stackedFeedbackChoice(content: content, item: item)
            }
        }
    }

    private func horizontalFeedbackChoice(
        content: FeedbackChoiceContent,
        item: AdSkipBannerItem
    ) -> some View {
        HStack(spacing: Spacing.xs) {
            Text(content.prompt)
                .font(AppTypography.caption)
                .foregroundStyle(boneText.opacity(0.72))

            Spacer(minLength: Spacing.xs)

            feedbackAnswerButtons(content: content, item: item)
                .fixedSize(horizontal: true, vertical: false)
        }
        // Expose the complete ideal width to `ViewThatFits`; otherwise SwiftUI
        // may compress the prompt until it truncates instead of selecting the
        // stacked alternative.
        .fixedSize(horizontal: true, vertical: false)
        .accessibilityElement(children: .contain)
    }

    private func stackedFeedbackChoice(
        content: FeedbackChoiceContent,
        item: AdSkipBannerItem
    ) -> some View {
        VStack(alignment: .leading, spacing: Spacing.xxs) {
            Text(content.prompt)
                .font(AppTypography.caption)
                .foregroundStyle(boneText.opacity(0.72))
                .fixedSize(horizontal: false, vertical: true)

            feedbackAnswerButtons(content: content, item: item)
                .frame(maxWidth: .infinity, alignment: .trailing)
        }
        .accessibilityElement(children: .contain)
    }

    private func feedbackAnswerButtons(
        content: FeedbackChoiceContent,
        item: AdSkipBannerItem
    ) -> some View {
        HStack(spacing: Spacing.xs) {
            Button {
                Task {
                    await handleFeedbackAwaitingAction(.confirmed, for: item)
                }
            } label: {
                Text(content.confirmLabel)
                    .font(AppTypography.caption)
                    .fontWeight(.semibold)
                    .foregroundStyle(AppColors.accent)
                    .padding(.horizontal, Spacing.sm)
                    .padding(.vertical, Spacing.xxs)
                    .background(
                        RoundedRectangle(cornerRadius: CornerRadius.small)
                            .fill(AppColors.accent.opacity(0.14))
                    )
                    .frame(
                        minWidth: Self.feedbackMinimumTapSize,
                        minHeight: Self.feedbackMinimumTapSize
                    )
                    .contentShape(Rectangle())
            }
            .buttonStyle(BannerButtonStyle())
            .disabled(!isFeedbackResponseAvailable(.confirmed, for: item))
            .accessibilityLabel(content.confirmAccessibilityLabel)
            .accessibilityHint(content.confirmAccessibilityHint)

            Button {
                Task {
                    await handleFeedbackAwaitingAction(.denied, for: item)
                }
            } label: {
                Text(content.denyLabel)
                    .font(AppTypography.caption)
                    .foregroundStyle(boneText.opacity(0.62))
                    .padding(.horizontal, Spacing.sm)
                    .padding(.vertical, Spacing.xxs)
                    .frame(
                        minWidth: Self.feedbackMinimumTapSize,
                        minHeight: Self.feedbackMinimumTapSize
                    )
                    .contentShape(Rectangle())
            }
            .buttonStyle(BannerButtonStyle())
            .disabled(!isFeedbackResponseAvailable(.denied, for: item))
            .accessibilityLabel(content.denyAccessibilityLabel)
            .accessibilityHint(content.denyAccessibilityHint)
        }
    }

    // MARK: - Constants

    /// Bone text color for use on ink background (always light, regardless of mode).
    private var boneText: Color { Palette.bone }
}

// MARK: - Banner Button Style

/// Subtle scale-down on press — consistent with transport button style.
private struct BannerButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed ? 0.92 : 1.0)
            .animation(Motion.quick, value: configuration.isPressed)
    }
}

// MARK: - Feedback Channel Menu (playhead-jw63.5)

/// Attaches the long-press "Something felt wrong" entry — and, when no
/// handler is wired, attaches nothing at all.
///
/// The conditional matters: `.contextMenu` with an empty builder still
/// arms a long press and then presents an empty menu, which would be a
/// worse regression than the missing feature. Previews and isolated banner
/// tests leave the handler `nil`, so they keep their pre-bead behaviour.
private struct BannerFeedbackChannelMenu: ViewModifier {
    let label: String
    let action: (() -> Void)?

    @ViewBuilder
    func body(content: Content) -> some View {
        if let action {
            content.contextMenu {
                Button(action: action) {
                    Label(label, systemImage: "envelope")
                }
            }
        } else {
            content
        }
    }
}

// MARK: - Preview

#Preview("Ad Banner — High Confidence") {
    ZStack {
        AppColors.background
            .ignoresSafeArea()

        AdBannerView(
            queue: {
                let q = AdBannerQueue()
                q.enqueue(AdSkipBannerItem(
                    id: "preview-1",
                    windowId: "w-1",
                    advertiser: "Squarespace",
                    product: "Build your website",
                    adStartTime: 120.0,
                    adEndTime: 180.0,
                    metadataConfidence: 0.85,
                    metadataSource: "foundationModels",
                    podcastId: "podcast-1",
                    evidenceCatalogEntries: []
                ))
                return q
            }()
        )
    }
    .preferredColorScheme(.dark)
}

#Preview("Ad Banner — Low Confidence") {
    ZStack {
        AppColors.background
            .ignoresSafeArea()

        AdBannerView(
            queue: {
                let q = AdBannerQueue()
                q.enqueue(AdSkipBannerItem(
                    id: "preview-2",
                    windowId: "w-2",
                    advertiser: "Maybe Corp",
                    product: nil,
                    adStartTime: 300.0,
                    adEndTime: 345.0,
                    metadataConfidence: 0.3,
                    metadataSource: "foundationModels",
                    podcastId: "podcast-1",
                    evidenceCatalogEntries: []
                ))
                return q
            }()
        )
    }
    .preferredColorScheme(.dark)
}

#Preview("Ad Banner — No Metadata") {
    ZStack {
        AppColors.background
            .ignoresSafeArea()

        AdBannerView(
            queue: {
                let q = AdBannerQueue()
                q.enqueue(AdSkipBannerItem(
                    id: "preview-3",
                    windowId: "w-3",
                    advertiser: nil,
                    product: nil,
                    adStartTime: 400.0,
                    adEndTime: 450.0,
                    metadataConfidence: nil,
                    metadataSource: "none",
                    podcastId: "podcast-1",
                    evidenceCatalogEntries: []
                ))
                return q
            }()
        )
    }
    .preferredColorScheme(.dark)
}

#Preview("Ad Banner — Evidence Detail") {
    ZStack {
        AppColors.background
            .ignoresSafeArea()

        AdBannerView(
            queue: {
                let q = AdBannerQueue()
                q.enqueue(AdSkipBannerItem(
                    id: "preview-4",
                    windowId: "w-4",
                    advertiser: "BetterHelp",
                    product: nil,
                    adStartTime: 240.0,
                    adEndTime: 300.0,
                    metadataConfidence: 0.82,
                    metadataSource: "foundationModels",
                    podcastId: "podcast-1",
                    evidenceCatalogEntries: [
                        EvidenceEntry(
                            evidenceRef: 0,
                            category: .disclosurePhrase,
                            matchedText: "sponsored by",
                            normalizedText: "sponsored by",
                            atomOrdinal: 12,
                            startTime: 245.0,
                            endTime: 246.0
                        ),
                        EvidenceEntry(
                            evidenceRef: 1,
                            category: .url,
                            matchedText: "betterhelp.com/podcast",
                            normalizedText: "betterhelp.com/podcast",
                            atomOrdinal: 14,
                            startTime: 270.0,
                            endTime: 271.0
                        ),
                        EvidenceEntry(
                            evidenceRef: 2,
                            category: .promoCode,
                            matchedText: "use code PODCAST",
                            normalizedText: "podcast",
                            atomOrdinal: 15,
                            startTime: 285.0,
                            endTime: 286.0
                        ),
                    ]
                ))
                return q
            }()
        )
    }
    .preferredColorScheme(.dark)
}
