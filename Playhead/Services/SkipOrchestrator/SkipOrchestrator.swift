// SkipOrchestrator.swift
// Decision layer between ad detection and playback transport.
//
// Consumes AdWindows from AdDetectionService, applies skip policy
// (hysteresis, merging, boundary snapping, suppression after seek),
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
    /// Maximum boundary snap distance (seconds) to nearest silence point.
    let boundarySnapMaxDistance: TimeInterval
    /// Pause probability threshold for silence detection in boundary snapping.
    let silenceThreshold: Double
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
        boundarySnapMaxDistance: 3.0,
        silenceThreshold: 0.6,
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
/// Maintains hysteresis state, merges short gaps, snaps boundaries to silence,
/// and suppresses skips after user seeks.
///
/// All decisions are logged for the evaluation harness.
actor SkipOrchestrator {

    private let logger = Logger(subsystem: "com.playhead", category: "SkipOrchestrator")

    // MARK: - Dependencies

    private let store: AnalysisStore
    private let config: SkipPolicyConfig
    private let trustService: TrustScoringService?

    // MARK: - State

    /// All managed windows for the current episode, keyed by adWindowId.
    private var windows: [String: ManagedWindow] = [:]

    /// Current analysis asset ID.
    private var activeAssetId: String?

    /// Whether we are currently "in ad state" (hysteresis tracking).
    private var inAdState: Bool = false

    /// Timestamp of the most recent user-initiated seek.
    private var lastSeekTime: Date?

    /// Whether skip is currently suppressed due to recent seek.
    private var skipSuppressedAfterSeek: Bool = false

    /// Latest known playhead position.
    private var currentPlayheadTime: TimeInterval = 0

    /// Decision log for evaluation harness.
    private var decisionLog: [SkipDecisionRecord] = []

    /// Feature windows cache for boundary snapping (loaded per-asset).
    private var cachedFeatureWindows: [FeatureWindow] = []

    /// Callback to push skip cues to PlaybackService.
    /// Set via `setSkipCueHandler`. Avoids direct PlaybackServiceActor coupling.
    private var skipCueHandler: (([CMTimeRange]) -> Void)?

    /// Per-show skip mode for the current episode. Loaded from TrustScoringService
    /// at episode start. Defaults to `.shadow` if no trust service is wired.
    private var activeSkipMode: SkipMode = .shadow

    /// Continuation-backed stream of applied ad segment time ranges (seconds).
    /// Consumers receive the full set of applied segments whenever the set changes.
    private var segmentContinuations: [UUID: AsyncStream<[(start: Double, end: Double)]>.Continuation] = [:]

    // MARK: - Init

    init(
        store: AnalysisStore,
        config: SkipPolicyConfig = .default,
        trustService: TrustScoringService? = nil
    ) {
        self.store = store
        self.config = config
        self.trustService = trustService
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
    ///   - analysisAssetId: The analysis asset being played.
    ///   - podcastId: The podcast's ID, used to load the per-show trust mode.
    func beginEpisode(analysisAssetId: String, podcastId: String? = nil) async {
        windows.removeAll()
        activeAssetId = analysisAssetId
        inAdState = false
        lastSeekTime = nil
        skipSuppressedAfterSeek = false
        currentPlayheadTime = 0
        decisionLog.removeAll()

        // Load per-show trust mode.
        if let podcastId, let trustService {
            activeSkipMode = await trustService.effectiveMode(podcastId: podcastId)
        } else {
            activeSkipMode = .shadow
        }

        // Pre-load feature windows for boundary snapping.
        do {
            cachedFeatureWindows = try await store.fetchAllFeatureWindows(assetId: analysisAssetId)
        } catch {
            logger.warning("Failed to load feature windows for snapping: \(error.localizedDescription)")
            cachedFeatureWindows = []
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
        inAdState = false
        cachedFeatureWindows = []
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
            let snappedStart = snapBoundary(time: adWindow.startTime, direction: .start)
            let snappedEnd = snapBoundary(time: adWindow.endTime, direction: .end)
            let key = idempotencyKey(assetId: assetId, windowId: adWindow.id)

            let managed = ManagedWindow(
                adWindow: adWindow,
                decisionState: incomingState,
                snappedStart: snappedStart,
                snappedEnd: snappedEnd,
                idempotencyKey: key,
                cueActive: false
            )
            windows[adWindow.id] = managed
        }

        // Re-evaluate all windows and push updated cues.
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

        // Remove the cue and re-push.
        evaluateAndPush()
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
        Task { [store] in
            try? await store.updateAdWindowDecision(
                id: id,
                decisionState: SkipDecisionState.applied.rawValue
            )
            try? await store.updateAdWindowWasSkipped(id: id, wasSkipped: true)
        }

        evaluateAndPush()
    }

    /// The active skip mode for the current episode.
    func currentSkipMode() -> SkipMode {
        activeSkipMode
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

    // MARK: - Core Skip Policy

    /// Evaluate all managed windows and determine which should have active
    /// skip cues. Applies hysteresis, merging, minimum span, seek suppression.
    private func evaluateAndPush() {
        guard activeAssetId != nil else { return }

        // 1. Collect eligible windows (confirmed or candidate with sufficient confidence).
        var eligible: [ManagedWindow] = []
        for (id, var managed) in windows {
            // Skip already-terminal states.
            if managed.decisionState == .applied
                || managed.decisionState == .suppressed
                || managed.decisionState == .reverted {
                // Keep applied windows as active cues.
                if managed.decisionState == .applied {
                    eligible.append(managed)
                }
                continue
            }

            let decision = evaluateWindow(&managed)
            if decision != managed.decisionState {
                managed.decisionState = decision
                windows[id] = managed
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

        // Persist to SQLite (fire-and-forget from the actor).
        let windowId = managed.adWindow.id
        Task { [store] in
            try? await store.updateAdWindowDecision(
                id: windowId,
                decisionState: SkipDecisionState.applied.rawValue
            )
            try? await store.updateAdWindowWasSkipped(id: windowId, wasSkipped: true)
        }

        return decision
    }

    // MARK: - Boundary Snapping

    private enum SnapDirection {
        case start, end
    }

    /// Snap a boundary time to the nearest silence/low-energy point
    /// using cached FeatureWindows.
    private func snapBoundary(time: Double, direction: SnapDirection) -> Double {
        guard !cachedFeatureWindows.isEmpty else { return time }

        let maxDist = config.boundarySnapMaxDistance
        let nearby = cachedFeatureWindows.filter { fw in
            let center = (fw.startTime + fw.endTime) / 2.0
            return abs(center - time) <= maxDist
        }

        guard !nearby.isEmpty else { return time }

        // Find the window with the highest pause probability (most silence-like).
        var bestTime = time
        var bestScore: Double = -1

        for fw in nearby {
            let pauseScore = fw.pauseProbability
            // Prefer low RMS (quiet) windows as well.
            let quietScore = max(0, 1.0 - fw.rms * 10.0)
            let combined = pauseScore * 0.7 + quietScore * 0.3

            if combined > bestScore {
                bestScore = combined
                switch direction {
                case .start:
                    bestTime = fw.startTime
                case .end:
                    bestTime = fw.endTime
                }
            }
        }

        // Only snap if we found a meaningful silence point.
        if bestScore >= config.silenceThreshold {
            return bestTime
        }

        return time
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

        logger.info("Decision: \(decision.rawValue) window=\(managed.adWindow.id) [\(managed.snappedStart, format: .fixed(precision: 1))s-\(managed.snappedEnd, format: .fixed(precision: 1))s] reason=\(reason)")
    }
}
