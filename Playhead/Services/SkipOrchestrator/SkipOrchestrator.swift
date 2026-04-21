// SkipOrchestrator.swift
// Decision layer between ad detection and playback transport.
//
// Consumes AdWindows from AdDetectionService, applies skip policy
// (hysteresis, merging, suppression after seek),
// and pushes skip cues to PlaybackService as CMTimeRanges.
//
// Every skip decision is idempotent, keyed by
//   analysisAssetId + adWindowId + policyVersion.
//
// NEVER queries SQLite synchronously from the playback callback path.
// All state is maintained in-memory; SQLite writes are fire-and-forget
// for the decision log.

import CoreMedia
import Foundation
import OSLog

// MARK: - Future Contract Scaffold

/// Phase 6 contract scaffold introduced by bead 6.7 so the pending
/// AdDecisionResult-based tests compile against the planned production
/// symbol names before bead 6.4 wires the real behavior.
///
/// Naming note: `AdDecisionResult` (this type) is the **runtime per-window decision**
/// that `SkipOrchestrator` consumes during active playback. It is distinct from
/// `DecisionResultArtifact` (in AdDecisionResult.swift), which is the SQLite persistence
/// container that stores an array of these decisions as JSON. The separation is intentional:
/// one type is optimized for live evaluation, the other for durable storage.
enum AdDecisionEligibilityGate: String, Sendable {
    case eligible
    case blocked
}

struct AdDecisionResult: Sendable {
    let id: String
    let analysisAssetId: String
    let startTime: Double
    let endTime: Double
    let skipConfidence: Double
    let eligibilityGate: AdDecisionEligibilityGate
    let recomputationRevision: Int
}

// MARK: - Skip Decision State

/// Lifecycle of an AdWindow through the skip orchestrator.
/// Extends the detection-side states (candidate, confirmed, suppressed)
/// with skip-execution states.
enum SkipDecisionState: String, Sendable {
    /// Detection produced a candidate -- not yet actionable.
    case candidate
    /// Detection confirmed the window -- eligible for skip policy.
    case confirmed
    /// Skip policy accepted and skip cue was fired.
    case applied
    /// Skip was suppressed by policy (too short, ambiguous, etc.).
    case suppressed
    /// User tapped "Listen" -- revert the skip.
    case reverted
}

// MARK: - Skip Policy Configuration

struct SkipPolicyConfig: Sendable {
    /// Hysteresis: probability threshold to enter ad state.
    let enterThreshold: Double
    /// Hysteresis: probability threshold to stay in ad state (lower).
    let stayThreshold: Double
    /// Merge adjacent ad windows with gaps smaller than this (seconds).
    let mergeGapSeconds: TimeInterval
    /// Ignore ad windows shorter than this unless sponsor evidence is strong.
    let minimumSpanSeconds: TimeInterval
    /// Confidence threshold for short-span override (strong sponsor evidence).
    let shortSpanOverrideConfidence: Double
    /// Seconds after a user seek during which auto-skip is suppressed.
    let seekSuppressionSeconds: TimeInterval
    /// Seconds of stability required after seek before re-enabling skip.
    let seekStabilitySeconds: TimeInterval
    /// Policy version tag for idempotency keys.
    let policyVersion: String

    static let `default` = SkipPolicyConfig(
        enterThreshold: 0.65,
        stayThreshold: 0.45,
        mergeGapSeconds: 4.0,
        minimumSpanSeconds: 15.0,
        shortSpanOverrideConfidence: 0.85,
        seekSuppressionSeconds: 3.0,
        seekStabilitySeconds: 2.0,
        policyVersion: "skip-policy-v1"
    )
}

// MARK: - Skip Decision Record

/// Immutable record of a skip decision for the evaluation harness.
struct SkipDecisionRecord: Sendable {
    let idempotencyKey: String
    let adWindowId: String
    let analysisAssetId: String
    let policyVersion: String
    let decision: SkipDecisionState
    let reason: String
    let originalStart: Double
    let originalEnd: Double
    let snappedStart: Double
    let snappedEnd: Double
    let confidence: Double
    let timestamp: Double
}

// MARK: - Managed Ad Window

/// In-memory representation of an AdWindow with skip orchestrator state.
private struct ManagedWindow: Sendable {
    let adWindow: AdWindow
    var decisionState: SkipDecisionState
    var snappedStart: Double
    var snappedEnd: Double
    var idempotencyKey: String
    /// Whether the skip cue has been pushed to PlaybackService.
    var cueActive: Bool
}

// MARK: - SkipOrchestrator

/// Consumes ad detection events and produces skip cues for PlaybackService.
/// Maintains hysteresis state, merges short gaps, and suppresses skips
/// after user seeks.
///
/// All decisions are logged for the evaluation harness.
actor SkipOrchestrator {

    private let logger = Logger(subsystem: "com.playhead", category: "SkipOrchestrator")

    // MARK: - Dependencies

    private let store: AnalysisStore
    private let config: SkipPolicyConfig
    private let trustService: TrustScoringService?

    // MARK: - Phase 7.2: User Correction Store

    /// Injected by PlayheadRuntime after init. Fire-and-forget writes; never throws.
    /// Optional so existing test setups that don't inject the store remain unaffected.
    private(set) var correctionStore: (any UserCorrectionStore)?

    // MARK: - State

    /// All managed windows for the current episode, keyed by adWindowId.
    private var windows: [String: ManagedWindow] = [:]

    /// Current analysis asset ID.
    private var activeAssetId: String?

    /// Current episode ID (canonical episode key). Used for the
    /// `episode_id_hash` stamped on `auto_skip_fired` events so the hash
    /// byte-matches the one `EpisodeSurfaceStatusObserver` stamps on
    /// `ready_entered`. Windows/decisions remain keyed by `activeAssetId`.
    private var activeEpisodeId: String?

    /// Whether we are currently "in ad state" (hysteresis tracking).
    private var inAdState: Bool = false

    /// Timestamp of the most recent user-initiated seek.
    private var lastSeekTime: Date?

    /// Whether skip is currently suppressed due to recent seek.
    private var skipSuppressedAfterSeek: Bool = false

    /// Latest known playhead position.
    private var currentPlayheadTime: TimeInterval = 0

    /// Decision log for evaluation harness. Capped to prevent unbounded growth.
    private var decisionLog: [SkipDecisionRecord] = []
    private let decisionLogCapacity = 500

    /// Callback to push skip cues to PlaybackService.
    /// Set via `setSkipCueHandler`. Avoids direct PlaybackServiceActor coupling.
    private var skipCueHandler: (([CMTimeRange]) -> Void)?

    /// Per-show skip mode for the current episode. Loaded from TrustScoringService
    /// at episode start. Defaults to `.shadow` if no trust service is wired.
    private var activeSkipMode: SkipMode = .shadow

    /// Continuation-backed stream of applied ad segment time ranges (seconds).
    /// Consumers receive the full set of applied segments whenever the set changes.
    private var segmentContinuations: [UUID: AsyncStream<[(start: Double, end: Double)]>.Continuation] = [:]

    /// Continuation-backed stream of banner items.
    /// Emits once per window the first time it reaches .confirmed or .applied state.
    private var bannerContinuations: [UUID: AsyncStream<AdSkipBannerItem>.Continuation] = [:]

    /// Window IDs for which a banner has already been emitted. Prevents re-fires.
    private var banneredWindowIds: Set<String> = []

    /// The podcast ID for the current episode. Needed to populate banner items.
    private var activePodcastId: String?

    /// Hasher used to stamp `auto_skip_fired` events with a per-install
    /// episode ID hash. Production passes a closure bound to the shared
    /// `SurfaceStatusInvariantLogger` instance so the hash is byte-
    /// identical to the one `EpisodeSurfaceStatusObserver` stamps on
    /// `ready_entered`. Tests can pin the hash to a known value
    /// independent of the logger's installId.
    private let episodeIdHasher: @Sendable (String) -> String

    /// The audit logger instance that `auto_skip_fired` events are
    /// written to. Shared with `EpisodeSurfaceStatusObserver` so both
    /// producers of the false_ready_rate pair land on the same file
    /// with the same installId.
    private let invariantLogger: SurfaceStatusInvariantLogger

    // MARK: - Init

    /// - Parameters:
    ///   - invariantLogger: The audit logger instance this orchestrator
    ///     writes `auto_skip_fired` events to. Defaults to a fresh
    ///     instance — test suites that don't inspect the log get an
    ///     isolated logger per orchestrator (no cross-test file races).
    ///     Production passes the runtime-shared instance so the companion
    ///     `ready_entered` producer (EpisodeSurfaceStatusObserver) lands
    ///     on the same file with the same installId.
    ///   - episodeIdHasher: Hasher for the `episode_id_hash` field.
    ///     When `nil`, derived from `invariantLogger.hashEpisodeId` so
    ///     production events naturally pair with the observer's. Tests
    ///     that want a pinned hash pass a deterministic closure.
    init(
        store: AnalysisStore,
        config: SkipPolicyConfig = .default,
        trustService: TrustScoringService? = nil,
        correctionStore: (any UserCorrectionStore)? = nil,
        invariantLogger: SurfaceStatusInvariantLogger = SurfaceStatusInvariantLogger(),
        episodeIdHasher: (@Sendable (String) -> String)? = nil
    ) {
        self.store = store
        self.config = config
        self.trustService = trustService
        self.correctionStore = correctionStore
        self.invariantLogger = invariantLogger
        self.episodeIdHasher = episodeIdHasher ?? { [invariantLogger] episodeId in
            invariantLogger.hashEpisodeId(episodeId)
        }
    }

    // MARK: - Configuration

    /// Set the callback that pushes skip cues to PlaybackService.
    func setSkipCueHandler(_ handler: @escaping @Sendable ([CMTimeRange]) -> Void) {
        skipCueHandler = handler
    }

    // MARK: - Ad Segment Stream

    /// Returns an AsyncStream of applied ad segment ranges (in seconds).
    /// Each emission is the full current set. The stream ends when the
    /// continuation is cancelled or the orchestrator is deallocated.
    func appliedSegmentsStream() -> AsyncStream<[(start: Double, end: Double)]> {
        let id = UUID()
        return AsyncStream { continuation in
            self.segmentContinuations[id] = continuation
            continuation.onTermination = { @Sendable _ in
                Task { [weak self] in
                    await self?.removeSegmentContinuation(id: id)
                }
            }
        }
    }

    private func removeSegmentContinuation(id: UUID) {
        segmentContinuations.removeValue(forKey: id)
    }

    // MARK: - Banner Item Stream

    /// Returns an AsyncStream that emits an AdSkipBannerItem the first time
    /// each ad window transitions to .confirmed or .applied state.
    /// Each window fires at most once per episode, regardless of subsequent state changes.
    func bannerItemStream() -> AsyncStream<AdSkipBannerItem> {
        let id = UUID()
        return AsyncStream { continuation in
            self.bannerContinuations[id] = continuation
            continuation.onTermination = { @Sendable _ in
                Task { [weak self] in
                    await self?.removeBannerContinuation(id: id)
                }
            }
        }
    }

    private func removeBannerContinuation(id: UUID) {
        bannerContinuations.removeValue(forKey: id)
    }

    /// Emit a banner item for the given managed window to all banner listeners.
    private func emitBannerItem(for managed: ManagedWindow) {
        guard !bannerContinuations.isEmpty else { return }
        let adWindow = managed.adWindow
        let podcastId = activePodcastId ?? ""
        let item = AdSkipBannerItem(
            id: UUID().uuidString,
            windowId: adWindow.id,
            advertiser: adWindow.advertiser,
            product: adWindow.product,
            adStartTime: managed.snappedStart,
            adEndTime: managed.snappedEnd,
            metadataConfidence: adWindow.metadataConfidence,
            metadataSource: adWindow.metadataSource,
            podcastId: podcastId,
            // EvidenceCatalog entries are not carried on ManagedWindow/AdWindow today.
            // Phase 7's UserCorrectionStore wires catalog data at the call site;
            // until then the banner carries an empty array and correction inference
            // falls back to windowId-scoped reverts.
            evidenceCatalogEntries: []
        )
        for (_, continuation) in bannerContinuations {
            continuation.yield(item)
        }
    }

    /// Broadcast the current set of applied segments to all listeners.
    private func broadcastAppliedSegments() {
        let applied = windows.values
            .filter { $0.decisionState == .applied || $0.decisionState == .confirmed }
            .sorted { $0.snappedStart < $1.snappedStart }
            .map { (start: $0.snappedStart, end: $0.snappedEnd) }
        for (_, continuation) in segmentContinuations {
            continuation.yield(applied)
        }
    }

    // MARK: - Episode Lifecycle

    /// Begin orchestration for a new episode. Clears all prior state.
    /// - Parameters:
    ///   - analysisAssetId: The analysis asset being played. Continues to
    ///     key windows, decisions, and pre-materialized cue lookups.
    ///   - episodeId: The canonical episode key (the identity unit that
    ///     `EpisodeSurfaceStatusObserver` hashes onto `ready_entered`).
    ///     Required so `auto_skip_fired.episode_id_hash` byte-matches
    ///     `ready_entered.episode_id_hash` for the same episode —
    ///     `false_ready_rate` pairs the two by that hash.
    ///   - podcastId: The podcast's ID, used to load the per-show trust mode.
    func beginEpisode(
        analysisAssetId: String,
        episodeId: String,
        podcastId: String? = nil
    ) async {
        windows.removeAll()
        activeAssetId = analysisAssetId
        activeEpisodeId = episodeId
        activePodcastId = podcastId
        inAdState = false
        lastSeekTime = nil
        skipSuppressedAfterSeek = false
        currentPlayheadTime = 0
        decisionLog.removeAll()
        banneredWindowIds.removeAll()

        // Load per-show trust mode.
        if let podcastId, let trustService {
            activeSkipMode = await trustService.effectiveMode(podcastId: podcastId)
        } else {
            activeSkipMode = .shadow
        }

        // Pre-load materialized skip cues from prior analysis.
        do {
            let preCues = try await store.fetchSkipCues(for: analysisAssetId)
            if !preCues.isEmpty {
                let syntheticWindows = preCues.map { cue in
                    AdWindow(
                        id: cue.id,
                        analysisAssetId: analysisAssetId,
                        startTime: cue.startTime,
                        endTime: cue.endTime,
                        confidence: cue.confidence,
                        boundaryState: "confirmed",
                        decisionState: "confirmed",
                        detectorVersion: "preAnalysis",
                        advertiser: nil, product: nil, adDescription: nil,
                        evidenceText: nil, evidenceStartTime: nil,
                        metadataSource: "preAnalysis",
                        metadataConfidence: nil, metadataPromptVersion: nil,
                        wasSkipped: false, userDismissedBanner: false
                    )
                }
                await receiveAdWindows(syntheticWindows)
            }
        } catch {
            logger.warning("Failed to load pre-materialized cues: \(error.localizedDescription)")
        }

        logger.info("Begin episode: asset=\(analysisAssetId)")
    }

    /// End orchestration for the current episode.
    func endEpisode() {
        let windowCount = windows.count
        let appliedCount = windows.values.filter { $0.decisionState == .applied }.count
        logger.info("End episode: \(windowCount) windows, \(appliedCount) applied, \(self.decisionLog.count) decisions logged")

        windows.removeAll()
        activeAssetId = nil
        activeEpisodeId = nil
        activePodcastId = nil
        inAdState = false
        banneredWindowIds.removeAll()
        pushSkipCues()
    }

    // MARK: - Ad Window Event Stream

    /// Receive new or updated AdWindows from AdDetectionService.
    /// This is the primary event-stream entry point. Called whenever
    /// the detection pipeline produces or updates windows.
    func receiveAdWindows(_ adWindows: [AdWindow]) async {
        guard let assetId = activeAssetId else { return }

        for adWindow in adWindows {
            guard adWindow.analysisAssetId == assetId else { continue }

            let existingState = windows[adWindow.id]?.decisionState

            // Never process a window that was already applied or reverted.
            if existingState == .applied || existingState == .reverted {
                continue
            }

            let incomingState = SkipDecisionState(rawValue: adWindow.decisionState) ?? .candidate

            // Build or update the managed window.
            let key = idempotencyKey(assetId: assetId, windowId: adWindow.id)

            let managed = ManagedWindow(
                adWindow: adWindow,
                decisionState: incomingState,
                snappedStart: adWindow.startTime,
                snappedEnd: adWindow.endTime,
                idempotencyKey: key,
                cueActive: false
            )
            windows[adWindow.id] = managed
        }

        // Re-evaluate all windows and push updated cues.
        evaluateAndPush()
    }

    func retireAdWindows(ids: Set<String>) async {
        guard !ids.isEmpty else { return }

        for id in ids {
            guard let existing = windows[id] else { continue }
            if existing.decisionState == .applied || existing.decisionState == .reverted {
                continue
            }
            windows.removeValue(forKey: id)
            banneredWindowIds.remove(id)
        }

        evaluateAndPush()
    }

    /// Receive fusion-based AdDecisionResults from AdDetectionService.
    ///
    /// This is the Phase 6 production entry point (playhead-4my.6.4). Replaces the
    /// raw AdWindow path for backfill-sourced decisions. The eligibility gate is
    /// checked before adding windows; blocked results are never promoted to applied.
    ///
    /// - Parameter results: Fusion decisions from BackfillEvidenceFusion + DecisionMapper.
    func receiveAdDecisionResults(_ results: [AdDecisionResult]) async {
        guard !results.isEmpty, let assetId = activeAssetId else { return }

        for result in results {
            guard result.analysisAssetId == assetId else { continue }

            let existingState = windows[result.id]?.decisionState

            // Never process a window that was already applied or reverted.
            if existingState == .applied || existingState == .reverted { continue }

            // Blocked gate: never add blocked results to the active window set.
            guard result.eligibilityGate == .eligible else {
                logger.debug(
                    "AdDecisionResult \(result.id, privacy: .public) gate=blocked — not adding to active windows"
                )
                continue
            }

            let key = idempotencyKey(assetId: assetId, windowId: result.id)

            // Build a synthetic AdWindow from the fusion decision so the existing
            // ManagedWindow + evaluateWindow machinery can handle it unchanged.
            let syntheticWindow = AdWindow(
                id: result.id,
                analysisAssetId: assetId,
                startTime: result.startTime,
                endTime: result.endTime,
                confidence: result.skipConfidence,
                boundaryState: "acousticRefined",
                decisionState: AdDecisionState.confirmed.rawValue,
                detectorVersion: "fusion-v1",
                advertiser: nil, product: nil, adDescription: nil,
                evidenceText: nil, evidenceStartTime: result.startTime,
                metadataSource: "fusion-v1", metadataConfidence: nil,
                metadataPromptVersion: nil,
                wasSkipped: false, userDismissedBanner: false
            )

            let managed = ManagedWindow(
                adWindow: syntheticWindow,
                decisionState: .confirmed,
                snappedStart: result.startTime,
                snappedEnd: result.endTime,
                idempotencyKey: key,
                cueActive: false
            )
            windows[result.id] = managed
        }

        evaluateAndPush()
    }

    // MARK: - Playback State Updates

    /// Update the current playhead position. Called from playback observer.
    func updatePlayheadTime(_ time: TimeInterval) {
        currentPlayheadTime = time

        // Check if seek suppression should be lifted.
        if skipSuppressedAfterSeek, let seekTime = lastSeekTime {
            let elapsed = Date().timeIntervalSince(seekTime)
            if elapsed >= config.seekStabilitySeconds {
                skipSuppressedAfterSeek = false
                logger.info("Skip suppression lifted after \(elapsed, format: .fixed(precision: 1))s stability")
                evaluateAndPush()
            }
        }
    }

    /// Record a user-initiated seek. Suppresses auto-skip until confidence
    /// re-stabilizes.
    func recordUserSeek(to time: TimeInterval) {
        lastSeekTime = Date()
        skipSuppressedAfterSeek = true
        currentPlayheadTime = time
        logger.info("User seek to \(time, format: .fixed(precision: 1))s -- skip suppressed")

        // Do NOT remove existing cues ahead of the new position.
        // Just suppress firing new ones until stability returns.
    }

    /// Record that the user tapped "Listen" to revert a skip.
    /// Also signals the trust engine (if wired) as a false-skip.
    func recordListenRevert(windowId: String, podcastId: String? = nil) async {
        guard var managed = windows[windowId] else { return }
        guard managed.decisionState != .reverted,
              managed.decisionState != .suppressed else { return }

        managed.decisionState = .reverted
        managed.cueActive = false
        windows[windowId] = managed

        logDecision(
            managed: managed,
            decision: .reverted,
            reason: "User tapped Listen"
        )

        // Persist decision state change.
        do {
            try await store.updateAdWindowDecision(
                id: windowId,
                decisionState: SkipDecisionState.reverted.rawValue
            )
        } catch {
            logger.warning("Failed to persist revert for \(windowId): \(error.localizedDescription)")
        }

        // Signal the trust engine about the false skip.
        if let podcastId, let trustService {
            await trustService.recordFalseSkipSignal(podcastId: podcastId)
        }

        // Phase 7.2 / playhead-zskc: persist a listenRevert CorrectionEvent
        // with window-precise time scope (fire-and-forget). AdWindow does not
        // carry atom ordinals, so we use the snapped start/end times directly
        // via the `.exactTimeSpan` correction scope.
        persistManualCorrectionVeto(
            startTime: managed.snappedStart,
            endTime: managed.snappedEnd,
            assetId: managed.adWindow.analysisAssetId,
            podcastId: podcastId,
            source: .listenRevert
        )

        // Remove the cue and re-push.
        evaluateAndPush()
    }

    /// Revert all managed windows overlapping the given time range.
    /// Used by the "Not an ad" banner and "This isn't an ad" popover paths,
    /// which identify the ad by its time span rather than a specific windowId.
    func revertByTimeRange(start: Double, end: Double, podcastId: String?) async {
        var revertedAny = false
        // playhead-zskc: one user gesture produces one correction event — not
        // N events per overlapping window. Capture the analysisAssetId of any
        // reverted window so we can write a single CorrectionEvent after the
        // loop. (All managed windows on the orchestrator share the current
        // episode's assetId; if they ever diverge mid-transition, attributing
        // to the first-matched window is still more correct than writing N
        // duplicates.)
        var assetIdForVeto: String?

        for (id, var managed) in windows {
            // Skip already-terminal states that aren't active.
            guard managed.decisionState != .reverted,
                  managed.decisionState != .suppressed else { continue }

            // Check overlap: window overlaps [start, end] if
            // windowStart < end && windowEnd > start.
            guard managed.snappedStart < end, managed.snappedEnd > start else { continue }

            managed.decisionState = .reverted
            managed.cueActive = false
            windows[id] = managed
            revertedAny = true
            if assetIdForVeto == nil {
                assetIdForVeto = managed.adWindow.analysisAssetId
            }

            logDecision(
                managed: managed,
                decision: .reverted,
                reason: "User correction: not an ad (time range)"
            )

            // Persist decision state change.
            do {
                try await store.updateAdWindowDecision(
                    id: id,
                    decisionState: SkipDecisionState.reverted.rawValue
                )
            } catch {
                logger.warning("Failed to persist revert for \(id): \(error.localizedDescription)")
            }
        }

        if revertedAny {
            // Persist a single manualVeto CorrectionEvent with precise time
            // scope per gesture. playhead-zskc: use the user-supplied
            // `start`/`end` (the time range the user identified) rather than
            // the managed window's snapped boundaries, so the correction
            // matches what the user actually gestured against when multiple
            // overlapping windows intersect the range.
            if let assetId = assetIdForVeto {
                persistManualCorrectionVeto(
                    startTime: start,
                    endTime: end,
                    assetId: assetId,
                    podcastId: podcastId,
                    source: .manualVeto
                )
            }

            // Signal trust engine once per user correction, not per window.
            if let podcastId, let trustService {
                await trustService.recordFalseSkipSignal(podcastId: podcastId)
            }
            evaluateAndPush()
        }
    }

    /// Revert a specific window by ID using the manualVeto source.
    /// Same as recordListenRevert but uses .manualVeto correction source
    /// and does not imply a playback rewind.
    func revertWindow(windowId: String, podcastId: String? = nil) async {
        guard var managed = windows[windowId] else { return }
        guard managed.decisionState != .reverted,
              managed.decisionState != .suppressed else { return }

        managed.decisionState = .reverted
        managed.cueActive = false
        windows[windowId] = managed

        logDecision(
            managed: managed,
            decision: .reverted,
            reason: "User correction: not an ad (banner)"
        )

        // Persist decision state change.
        do {
            try await store.updateAdWindowDecision(
                id: windowId,
                decisionState: SkipDecisionState.reverted.rawValue
            )
        } catch {
            logger.warning("Failed to persist revert for \(windowId): \(error.localizedDescription)")
        }

        // Signal the trust engine about the false skip.
        if let podcastId, let trustService {
            await trustService.recordFalseSkipSignal(podcastId: podcastId)
        }

        // Persist a manualVeto CorrectionEvent with precise time scope.
        // playhead-zskc: use the managed window's snapped start/end so the
        // correction carries per-window precision rather than whole-episode.
        persistManualCorrectionVeto(
            startTime: managed.snappedStart,
            endTime: managed.snappedEnd,
            assetId: managed.adWindow.analysisAssetId,
            podcastId: podcastId,
            source: .manualVeto
        )

        evaluateAndPush()
    }

    // MARK: - Correction persistence helper (playhead-zskc)

    /// Fire-and-forget a `.exactTimeSpan` CorrectionEvent through the
    /// injected correction store. Centralises the three manual-veto call
    /// sites (`recordListenRevert`, `revertByTimeRange`, `revertWindow`) so
    /// actor-isolated capture ritual and nil-store guard live in one place.
    private func persistManualCorrectionVeto(
        startTime: Double,
        endTime: Double,
        assetId: String,
        podcastId: String?,
        source: CorrectionSource
    ) {
        guard let correctionStore else { return }
        let store = correctionStore
        let pid = podcastId
        Task {
            await store.recordVeto(
                startTime: startTime,
                endTime: endTime,
                assetId: assetId,
                podcastId: pid,
                source: source
            )
        }
    }

    /// User tapped "Skip Ad" in manual mode. Promotes a confirmed window
    /// to applied and fires the skip cue.
    func applyManualSkip(windowId: String) async {
        guard var managed = windows[windowId] else { return }
        guard managed.decisionState == .confirmed else { return }

        managed.decisionState = .applied
        managed.cueActive = true
        windows[windowId] = managed

        logDecision(
            managed: managed,
            decision: .applied,
            reason: "Manual skip by user"
        )

        // Persist.
        let id = managed.adWindow.id
        Task { [store, logger] in
            do {
                try await store.updateAdWindowDecision(
                    id: id,
                    decisionState: SkipDecisionState.applied.rawValue
                )
                try await store.updateAdWindowWasSkipped(id: id, wasSkipped: true)
            } catch {
                logger.warning("Failed to persist manual skip for \(id): \(error.localizedDescription)")
            }
        }

        evaluateAndPush()
    }

    /// The active skip mode for the current episode.
    func currentSkipMode() -> SkipMode {
        activeSkipMode
    }

    /// Override the active skip mode for the current episode and re-evaluate pending windows.
    func setActiveSkipMode(_ mode: SkipMode) {
        activeSkipMode = mode
        evaluateAndPush()
    }

    /// Windows in the confirmed state (available for manual skip UI).
    func confirmedWindows() -> [AdWindow] {
        windows.values
            .filter { $0.decisionState == .confirmed }
            .sorted { $0.snappedStart < $1.snappedStart }
            .map(\.adWindow)
    }

    // MARK: - Decision Log Access

    /// Return the decision log for the evaluation harness.
    func getDecisionLog() -> [SkipDecisionRecord] {
        decisionLog
    }

    func activeWindowIDs() -> Set<String> {
        Set(windows.keys)
    }

    // MARK: - Core Skip Policy

    /// Evaluate all managed windows and determine which should have active
    /// skip cues. Applies hysteresis, merging, minimum span, seek suppression.
    private func evaluateAndPush() {
        guard activeAssetId != nil else { return }

        // 1. Collect eligible windows (confirmed or candidate with sufficient confidence).
        //    Sort by snappedStart so hysteresis (inAdState) is evaluated in temporal order.
        var eligible: [ManagedWindow] = []
        let sortedWindows = windows.sorted { $0.value.snappedStart < $1.value.snappedStart }
        for (id, var managed) in sortedWindows {
            // Skip already-terminal states.
            if managed.decisionState == .applied
                || managed.decisionState == .suppressed
                || managed.decisionState == .reverted {
                // Keep applied windows as active cues.
                if managed.decisionState == .applied {
                    // Emit a banner on first encounter (e.g. after applyManualSkip).
                    if !banneredWindowIds.contains(managed.adWindow.id) {
                        banneredWindowIds.insert(managed.adWindow.id)
                        emitBannerItem(for: managed)
                    }
                    eligible.append(managed)
                }
                continue
            }

            let previousState = managed.decisionState
            let decision = evaluateWindow(&managed)
            if decision != previousState {
                managed.decisionState = decision
                windows[id] = managed
            }

            // Emit a banner the first time a window reaches .confirmed or .applied.
            if (decision == .confirmed || decision == .applied),
               !banneredWindowIds.contains(managed.adWindow.id) {
                banneredWindowIds.insert(managed.adWindow.id)
                emitBannerItem(for: managed)
            }

            if decision == .applied {
                eligible.append(managed)
            }
        }

        // 2. Merge adjacent windows with small gaps.
        let merged = mergeAdjacentWindows(eligible)

        // 3. Push skip cues to PlaybackService.
        pushMergedCues(merged)

        // 4. Broadcast updated segments to UI listeners.
        broadcastAppliedSegments()
    }

    /// Evaluate a single window against skip policy. Returns the decision.
    private func evaluateWindow(_ managed: inout ManagedWindow) -> SkipDecisionState {
        let confidence = managed.adWindow.confidence
        let span = managed.snappedEnd - managed.snappedStart

        // Late detection: if the playhead is already past this window, never skip.
        if managed.snappedEnd <= currentPlayheadTime {
            let decision = SkipDecisionState.suppressed
            logDecision(managed: managed, decision: decision, reason: "Late detection -- playhead past window end")
            return decision
        }

        // Seek suppression: if user recently seeked, suppress new skips.
        if skipSuppressedAfterSeek {
            // Don't change state -- just don't promote to applied yet.
            return managed.decisionState
        }

        // Hysteresis: different thresholds for entering vs staying in ad state.
        let threshold = inAdState ? config.stayThreshold : config.enterThreshold

        if confidence < threshold {
            // Below threshold -- suppress if it was candidate.
            if managed.decisionState == .candidate {
                let decision = SkipDecisionState.suppressed
                logDecision(managed: managed, decision: decision, reason: "Below hysteresis threshold (\(confidence) < \(threshold))")
                return decision
            }
            // Confirmed but below stay threshold -- exit ad state.
            if managed.decisionState == .confirmed && confidence < config.stayThreshold {
                inAdState = false
                let decision = SkipDecisionState.suppressed
                logDecision(managed: managed, decision: decision, reason: "Exiting ad state: confidence dropped below stay threshold")
                return decision
            }
            return managed.decisionState
        }

        // Minimum span check.
        if span < config.minimumSpanSeconds {
            // Allow short spans only with very strong evidence.
            if confidence < config.shortSpanOverrideConfidence {
                let decision = SkipDecisionState.suppressed
                logDecision(managed: managed, decision: decision, reason: "Span too short (\(span)s < \(config.minimumSpanSeconds)s) without strong evidence")
                return decision
            }
        }

        // Boundary stability: only skip if the window boundary is stable
        // (not still being refined by incoming detection events).
        // Confirmed windows are considered stable; candidates must wait
        // for confirmation unless confidence is exceptionally high.
        if managed.decisionState == .candidate {
            // Candidates need confirmation before skipping.
            // In auto mode (trusted show), promote candidates above the
            // enter threshold without waiting for backfill confirmation.
            // Otherwise, only override if confidence is very high.
            if activeSkipMode == .auto && confidence >= config.enterThreshold {
                // Promote to confirmed — fall through to trust mode gate.
                managed.decisionState = .confirmed
            } else if confidence < config.shortSpanOverrideConfidence {
                return managed.decisionState
            }
        }

        // Trust mode gate: shadow mode logs only; manual mode marks confirmed
        // but does not auto-skip (UI shows a manual "Skip Ad" button instead).
        switch activeSkipMode {
        case .shadow:
            let decision = SkipDecisionState.confirmed
            logDecision(managed: managed, decision: decision, reason: "Shadow mode -- detection logged, no skip fired")
            return decision
        case .manual:
            let decision = SkipDecisionState.confirmed
            logDecision(managed: managed, decision: decision, reason: "Manual mode -- confirmed, awaiting user tap")
            return decision
        case .auto:
            break // Proceed to auto-skip below.
        }

        // All checks passed -- apply the skip.
        inAdState = true
        let decision = SkipDecisionState.applied
        logDecision(managed: managed, decision: decision, reason: "Skip policy accepted (auto mode)")

        // playhead-o45p: emit an auto_skip_fired event to the ol05 state-
        // transition log. Paired with readyEntered events on the same
        // episode_id_hash, this is the numerator/denominator source for
        // the Wave 4 false_ready_rate dogfood metric. Hashing happens
        // through the shared logger salt on the CANONICAL EPISODE KEY
        // (not the analysis asset ID) so the two event sites produce
        // byte-identical episode hashes —
        // `EpisodeSurfaceStatusObserver.runReducerFor` hashes the same
        // episode key onto `ready_entered`, and `false_ready_rate.swift`
        // pairs events by that hash.
        if let episodeId = activeEpisodeId {
            let hashed = episodeIdHasher(episodeId)
            let startMs = Int((managed.snappedStart * 1000.0).rounded())
            let endMs = Int((managed.snappedEnd * 1000.0).rounded())
            invariantLogger.recordAutoSkipFired(
                episodeIdHash: hashed,
                windowStartMs: startMs,
                windowEndMs: endMs
            )
        }

        // Persist to SQLite (fire-and-forget from the actor).
        let windowId = managed.adWindow.id
        Task { [store, logger] in
            do {
                try await store.updateAdWindowDecision(
                    id: windowId,
                    decisionState: SkipDecisionState.applied.rawValue
                )
                try await store.updateAdWindowWasSkipped(id: windowId, wasSkipped: true)
            } catch {
                logger.warning("Failed to persist skip state for \(windowId): \(error.localizedDescription)")
            }
        }

        return decision
    }

    // MARK: - Window Merging

    /// Merge adjacent applied windows with gaps smaller than mergeGapSeconds.
    private func mergeAdjacentWindows(_ windows: [ManagedWindow]) -> [(start: Double, end: Double)] {
        let sorted = windows.sorted { $0.snappedStart < $1.snappedStart }
        guard let first = sorted.first else { return [] }

        var merged: [(start: Double, end: Double)] = []
        var currentStart = first.snappedStart
        var currentEnd = first.snappedEnd

        for window in sorted.dropFirst() {
            if window.snappedStart <= currentEnd + config.mergeGapSeconds {
                // Merge: extend the current range.
                currentEnd = max(currentEnd, window.snappedEnd)
            } else {
                // Gap too large: emit current range, start new one.
                merged.append((start: currentStart, end: currentEnd))
                currentStart = window.snappedStart
                currentEnd = window.snappedEnd
            }
        }

        merged.append((start: currentStart, end: currentEnd))
        return merged
    }

    // MARK: - Cue Pushing

    /// Convert merged ranges to CMTimeRanges and push to PlaybackService.
    private func pushMergedCues(_ ranges: [(start: Double, end: Double)]) {
        let cues = ranges.map { range in
            let start = CMTime(seconds: range.start, preferredTimescale: 600)
            let duration = CMTime(seconds: range.end - range.start, preferredTimescale: 600)
            return CMTimeRange(start: start, duration: duration)
        }
        pushSkipCues(cues)
    }

    /// Push skip cues to PlaybackService via the handler. Defaults to empty.
    private func pushSkipCues(_ cues: [CMTimeRange] = []) {
        skipCueHandler?(cues)
    }

    // MARK: - User Correction Injection

    /// Inject a user-marked ad segment immediately into the skip orchestrator.
    /// Creates a ManagedWindow with confidence=1.0 and .confirmed state, then
    /// evaluates and pushes skip cues so the segment takes effect in real time.
    ///
    /// Called from PlayheadRuntime when the user taps "Hearing an ad" or marks
    /// transcript chunks as an ad.
    func injectUserMarkedAd(start: Double, end: Double, analysisAssetId: String) {
        // Synthesize an AdWindow for the user-marked region.
        let windowId = UUID().uuidString
        let adWindow = AdWindow(
            id: windowId,
            analysisAssetId: analysisAssetId,
            startTime: start,
            endTime: end,
            confidence: 1.0,
            boundaryState: "userMarked",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "userCorrection",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: start,
            metadataSource: "userCorrection",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false
        )

        let key = idempotencyKey(assetId: analysisAssetId, windowId: windowId)

        let managed = ManagedWindow(
            adWindow: adWindow,
            decisionState: .confirmed,
            snappedStart: start,
            snappedEnd: end,
            idempotencyKey: key,
            cueActive: false
        )
        windows[windowId] = managed

        // Emit banner before evaluateAndPush so listeners see the banner
        // even in shadow/manual mode where evaluateAndPush may not promote
        // to .applied.
        if !banneredWindowIds.contains(windowId) {
            banneredWindowIds.insert(windowId)
            emitBannerItem(for: managed)
        }

        evaluateAndPush()
    }

    // MARK: - Idempotency

    /// Build the idempotency key for a skip decision.
    private func idempotencyKey(assetId: String, windowId: String) -> String {
        "\(assetId):\(windowId):\(config.policyVersion)"
    }

    // MARK: - Decision Logging

    private func logDecision(
        managed: ManagedWindow,
        decision: SkipDecisionState,
        reason: String
    ) {
        let record = SkipDecisionRecord(
            idempotencyKey: managed.idempotencyKey,
            adWindowId: managed.adWindow.id,
            analysisAssetId: managed.adWindow.analysisAssetId,
            policyVersion: config.policyVersion,
            decision: decision,
            reason: reason,
            originalStart: managed.adWindow.startTime,
            originalEnd: managed.adWindow.endTime,
            snappedStart: managed.snappedStart,
            snappedEnd: managed.snappedEnd,
            confidence: managed.adWindow.confidence,
            timestamp: Date().timeIntervalSince1970
        )
        decisionLog.append(record)
        if decisionLog.count > decisionLogCapacity {
            decisionLog.removeFirst(decisionLog.count - decisionLogCapacity)
        }

        logger.info("Decision: \(decision.rawValue) window=\(managed.adWindow.id) [\(managed.snappedStart, format: .fixed(precision: 1))s-\(managed.snappedEnd, format: .fixed(precision: 1))s] reason=\(reason)")
    }
}
