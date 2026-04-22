// ShadowCaptureCoordinator.swift
// playhead-narl.2: Two-lane coordinator for dual-run FM shadow capture.
//
// Hosts both capture lanes and gates every shadow FM dispatch against the
// kill switch. Environment signals (audio session playing/paused, thermal
// state, charging state) are injected as protocols so the coordinator and
// both lanes are unit-testable without UIKit / AVFAudio.
//
// # Separation of concerns
//
// - **This type** owns policy: when is playback "strict", when is the
//   device "idle + nominal + charging", per-minute rate limiting.
// - **`ShadowFMDispatcher`** owns the FM call itself. The live
//   implementation bridges to `FoundationModelClassifier` — the protocol
//   decouples this coordinator from the model runner's concurrency model
//   and lets tests inject a deterministic stub.
// - **`ShadowWindowSource`** enumerates candidate windows the harness
//   cares about. The live implementation queries the planner for
//   `.allEnabled` FM scheduling decisions that `.default` skipped; tests
//   inject a static list.
//
// Nothing here participates in production gate decisions — the harness
// in `playhead-narl.1` is the only consumer of persisted shadow rows.

import Foundation
import OSLog

// MARK: - Environment signal protocols

/// Audio session playing/paused signal. Implementation wraps the app's
/// ``AVAudioSession`` observer; tests inject a deterministic stub.
///
/// "Strict playback" means:
///   - app's audio session is in an active playing state
///   - NOT merely foreground-with-ready-transport
///   - NOT paused
protocol ShadowPlaybackSignalProvider: Sendable {
    /// Snapshot of the playback signal at the moment of the call. The
    /// coordinator samples this at each tick — there is no subscription
    /// model, so the implementation need not publish changes.
    func isStrictlyPlaying() -> Bool

    /// Current playhead position within the active asset, in seconds.
    /// Returns `nil` when no asset is loaded / active. When `nil`, Lane A
    /// no-ops for the tick.
    func currentAsset() -> ShadowActiveAsset?
}

/// Lightweight description of the asset currently playing.
struct ShadowActiveAsset: Sendable, Equatable {
    let assetId: String
    /// Playhead position, in asset seconds.
    let playheadSeconds: TimeInterval
}

/// Thermal + charging signal. Implementation wraps
/// ``ProcessInfo.thermalState`` + ``UIDevice.batteryState`` (with
/// `isBatteryMonitoringEnabled = true` set once at app launch and left on
/// per bead spec guidance — don't churn the flag).
///
/// Lane B only runs when ``isIdleForBackfill()`` returns `true`.
protocol ShadowEnvironmentSignalProvider: Sendable {
    /// `true` iff `thermalState == .nominal` AND device is charging
    /// (battery state `charging` or `full`). `.fair` thermal is deliberately
    /// insufficient — Lane B's "thorough pass" should run only under
    /// genuinely cold conditions.
    func isIdleForBackfill() -> Bool
}

// MARK: - Shadow FM dispatcher

/// Dispatches one shadow FM call for a window under a given config variant.
/// Returns the serialized FM response plus the model version identifier in
/// use at dispatch time.
///
/// Implementations MUST NOT participate in production gate decisions or
/// write back into the production fusion/gate/policy path — shadow calls
/// are side-effect-free on the live pipeline.
///
/// The live implementation bridges to ``FoundationModelClassifier``;
/// tests inject a stub that returns synthetic bytes.
protocol ShadowFMDispatcher: Sendable {
    func dispatchShadowCall(
        assetId: String,
        window: ShadowWindow,
        configVariant: ShadowConfigVariant
    ) async throws -> ShadowFMDispatchResult
}

/// A window in the asset timeline. Plain-data — no transcript payload —
/// because this bead doesn't own the FM call's input shape; the dispatcher
/// resolves the window into whatever the underlying model runner needs.
struct ShadowWindow: Sendable, Equatable, Hashable {
    let start: TimeInterval
    let end: TimeInterval
}

/// Result of a dispatched shadow FM call.
struct ShadowFMDispatchResult: Sendable {
    /// Serialized FM response payload, opaque to the coordinator.
    let fmResponse: Data
    /// FM model version identifier at dispatch time. Persisted per-row so
    /// downstream consumers can version-gate.
    let fmModelVersion: String?
}

// MARK: - Candidate window source

/// Enumerates candidate shadow windows. The live implementation asks the
/// scheduler for windows that `.allEnabled` would schedule but `.default`
/// did not, filtered to `[playhead, playhead + N]` for Lane A or to the
/// uncovered tail of the asset for Lane B.
protocol ShadowWindowSource: Sendable {
    /// For Lane A: windows in `[fromSeconds, fromSeconds + lookahead]` that
    /// `.allEnabled` would schedule and that do not appear in
    /// `alreadyCaptured`. Implementation is free to return an empty list.
    func laneACandidates(
        assetId: String,
        fromSeconds: TimeInterval,
        lookaheadSeconds: TimeInterval,
        alreadyCaptured: Set<ShadowWindowKey>
    ) async throws -> [ShadowWindow]

    /// For Lane B: all un-captured `.allEnabled` windows for the asset,
    /// ordered to prioritize the earliest gap first.
    func laneBCandidates(
        assetId: String,
        alreadyCaptured: Set<ShadowWindowKey>
    ) async throws -> [ShadowWindow]

    /// Ordered list of asset ids with incomplete shadow coverage, for
    /// Lane B to walk on each tick. Returning an empty list signals
    /// "nothing to do".
    func assetsWithIncompleteCoverage() async throws -> [String]
}

// MARK: - Persistence seam

/// Narrow writer seam the coordinator uses. ``AnalysisStore`` conforms
/// via the CRUD methods defined in its main file.
protocol ShadowCaptureStore: Sendable {
    func upsertShadowFMResponse(_ row: ShadowFMResponse) async throws
    func capturedShadowWindows(
        assetId: String,
        configVariant: ShadowConfigVariant
    ) async throws -> Set<ShadowWindowKey>
}

extension AnalysisStore: ShadowCaptureStore {
    // The actor-isolated CRUD methods already match the protocol shape once
    // invoked through `async`. Nothing to add here — the actor forwards the
    // calls via implicit `async` through-pointers.
}

// MARK: - ShadowCaptureCoordinator

/// Coordinates both shadow capture lanes.
///
/// Usage:
/// ```
/// let coord = ShadowCaptureCoordinator(store: store, dispatcher: dispatcher,
///                                      windowSource: windowSource,
///                                      playbackSignal: playbackSignal,
///                                      environmentSignal: envSignal)
/// await coord.tickLaneA()     // called on the playback heartbeat
/// await coord.tickLaneB()     // called by a background worker
/// ```
/// Neither tick blocks; both no-op when the kill switch is off or the
/// relevant environment predicates don't hold.
///
/// The coordinator is an actor so concurrent Lane A + Lane B ticks on a
/// shared rate-limit state don't race.
actor ShadowCaptureCoordinator {

    // MARK: - Collaborators

    private let store: any ShadowCaptureStore
    private let dispatcher: any ShadowFMDispatcher
    private let windowSource: any ShadowWindowSource
    private let playbackSignal: any ShadowPlaybackSignalProvider
    private let environmentSignal: any ShadowEnvironmentSignalProvider
    private let clock: () -> TimeInterval
    private let logger: Logger

    /// Snapshot-read config. The caller passes `{ ShadowCaptureConfig.default }`
    /// in production so a hot-flip of a debug override is picked up on the
    /// next tick without restarting the coordinator.
    private let readConfig: @Sendable () -> ShadowCaptureConfig

    // MARK: - Rate-limit state (Lane A)

    /// Timestamps (unix seconds) of the last N Lane-A dispatches. Oldest
    /// entries are dropped when they fall outside the per-minute window.
    /// Simple O(N) — N is small (cap is `laneAMaxCallsPerMinute`).
    private var laneARecentDispatchTimes: [TimeInterval] = []

    /// Current in-flight count for Lane A.
    private var laneAInFlight: Int = 0

    // MARK: - Lane B state

    /// Current in-flight count for Lane B.
    private var laneBInFlight: Int = 0

    // MARK: - Init

    init(
        store: any ShadowCaptureStore,
        dispatcher: any ShadowFMDispatcher,
        windowSource: any ShadowWindowSource,
        playbackSignal: any ShadowPlaybackSignalProvider,
        environmentSignal: any ShadowEnvironmentSignalProvider,
        clock: @escaping () -> TimeInterval = { Date().timeIntervalSince1970 },
        readConfig: @escaping @Sendable () -> ShadowCaptureConfig = { .default }
    ) {
        self.store = store
        self.dispatcher = dispatcher
        self.windowSource = windowSource
        self.playbackSignal = playbackSignal
        self.environmentSignal = environmentSignal
        self.clock = clock
        self.readConfig = readConfig
        self.logger = Logger(subsystem: "com.playhead", category: "ShadowCapture")
    }

    // MARK: - Lane A: JIT during strict playback

    /// Drive one Lane-A capture cycle. Call on the playback heartbeat (the
    /// existing transport tick is a reasonable cadence — e.g. ~1 Hz).
    ///
    /// Guards, in order:
    ///   1. Kill switch off → no-op.
    ///   2. Not strictly playing → no-op.
    ///   3. No active asset → no-op.
    ///   4. Per-minute rate limit exhausted → no-op.
    ///   5. Max in-flight reached → no-op.
    ///
    /// On success: dispatches at most one shadow FM call per tick and
    /// persists the result. Limiting to one per tick is the simplest honest
    /// bound; with a ~1 Hz tick and `laneAMaxCallsPerMinute = 4`, the
    /// rate-limit predicate is the binding constraint, not this per-tick
    /// cap.
    @discardableResult
    func tickLaneA() async -> ShadowCaptureTickOutcome {
        let config = readConfig()
        guard config.dualFMCaptureEnabled else { return .killSwitchOff }
        guard playbackSignal.isStrictlyPlaying() else { return .notPlaying }
        guard let active = playbackSignal.currentAsset() else { return .noActiveAsset }
        guard laneAInFlight < config.laneAMaxInFlight else { return .laneBusy }

        // Rate-limit check.
        let now = clock()
        let windowOpens = now - 60.0
        laneARecentDispatchTimes.removeAll(where: { $0 < windowOpens })
        guard laneARecentDispatchTimes.count < config.laneAMaxCallsPerMinute else {
            return .rateLimited
        }

        // Resolve candidates.
        let alreadyCaptured: Set<ShadowWindowKey>
        do {
            alreadyCaptured = try await store.capturedShadowWindows(
                assetId: active.assetId,
                configVariant: .allEnabledShadow
            )
        } catch {
            logger.warning("laneA: capturedShadowWindows failed: \(String(describing: error), privacy: .public)")
            return .dispatchFailed
        }
        let candidates: [ShadowWindow]
        do {
            candidates = try await windowSource.laneACandidates(
                assetId: active.assetId,
                fromSeconds: active.playheadSeconds,
                lookaheadSeconds: config.laneALookaheadSeconds,
                alreadyCaptured: alreadyCaptured
            )
        } catch {
            logger.warning("laneA: candidate lookup failed: \(String(describing: error), privacy: .public)")
            return .dispatchFailed
        }
        guard let window = candidates.first else { return .noCandidates }

        laneAInFlight += 1
        laneARecentDispatchTimes.append(now)
        defer { laneAInFlight -= 1 }

        return await dispatchAndPersist(
            assetId: active.assetId,
            window: window,
            capturedBy: .laneA
        )
    }

    // MARK: - Lane B: background thorough pass

    /// Drive one Lane-B capture cycle. Call on a background worker's tick
    /// (e.g. a timer fired by `BackgroundProcessingService`).
    ///
    /// Guards, in order:
    ///   1. Kill switch off → no-op.
    ///   2. Thermal not nominal OR device not charging → no-op.
    ///   3. Max in-flight reached → no-op.
    ///   4. No assets with incomplete coverage → no-op.
    ///
    /// On success: dispatches up to `laneBCallsPerTick` shadow FM calls
    /// across one asset and persists the results.
    @discardableResult
    func tickLaneB() async -> ShadowCaptureTickOutcome {
        let config = readConfig()
        guard config.dualFMCaptureEnabled else { return .killSwitchOff }
        guard environmentSignal.isIdleForBackfill() else { return .notIdle }
        guard laneBInFlight < config.laneBMaxInFlight else { return .laneBusy }

        let assetIds: [String]
        do {
            assetIds = try await windowSource.assetsWithIncompleteCoverage()
        } catch {
            logger.warning("laneB: incomplete-coverage lookup failed: \(String(describing: error), privacy: .public)")
            return .dispatchFailed
        }
        guard let assetId = assetIds.first else { return .noCandidates }

        let alreadyCaptured: Set<ShadowWindowKey>
        do {
            alreadyCaptured = try await store.capturedShadowWindows(
                assetId: assetId,
                configVariant: .allEnabledShadow
            )
        } catch {
            logger.warning("laneB: capturedShadowWindows failed: \(String(describing: error), privacy: .public)")
            return .dispatchFailed
        }
        let candidates: [ShadowWindow]
        do {
            candidates = try await windowSource.laneBCandidates(
                assetId: assetId,
                alreadyCaptured: alreadyCaptured
            )
        } catch {
            logger.warning("laneB: candidate lookup failed: \(String(describing: error), privacy: .public)")
            return .dispatchFailed
        }
        guard !candidates.isEmpty else { return .noCandidates }

        laneBInFlight += 1
        defer { laneBInFlight -= 1 }

        var lastOutcome: ShadowCaptureTickOutcome = .noCandidates
        for window in candidates.prefix(config.laneBCallsPerTick) {
            lastOutcome = await dispatchAndPersist(
                assetId: assetId,
                window: window,
                capturedBy: .laneB
            )
            if lastOutcome == .dispatchFailed { break }
        }
        return lastOutcome
    }

    // MARK: - Shared dispatch + persist

    private func dispatchAndPersist(
        assetId: String,
        window: ShadowWindow,
        capturedBy: ShadowCapturedBy
    ) async -> ShadowCaptureTickOutcome {
        let result: ShadowFMDispatchResult
        do {
            result = try await dispatcher.dispatchShadowCall(
                assetId: assetId,
                window: window,
                configVariant: .allEnabledShadow
            )
        } catch {
            logger.warning(
                "shadow dispatch failed: asset=\(assetId, privacy: .public) window=\(window.start, privacy: .public)..\(window.end, privacy: .public) error=\(String(describing: error), privacy: .public)"
            )
            return .dispatchFailed
        }

        let row = ShadowFMResponse(
            assetId: assetId,
            windowStart: window.start,
            windowEnd: window.end,
            configVariant: .allEnabledShadow,
            fmResponse: result.fmResponse,
            capturedAt: clock(),
            capturedBy: capturedBy,
            fmModelVersion: result.fmModelVersion
        )

        do {
            try await store.upsertShadowFMResponse(row)
        } catch {
            logger.warning(
                "shadow persist failed: asset=\(assetId, privacy: .public) error=\(String(describing: error), privacy: .public)"
            )
            return .dispatchFailed
        }

        return .dispatched
    }
}

// MARK: - ShadowCaptureTickOutcome

/// Result of a single tick. Exposed so tests can assert on the exact
/// termination reason (e.g. "killSwitchOff" vs "notPlaying" vs
/// "rateLimited"), and so future telemetry can count outcomes per hour.
enum ShadowCaptureTickOutcome: String, Sendable, Equatable {
    case killSwitchOff
    case notPlaying
    case notIdle
    case noActiveAsset
    case noCandidates
    case rateLimited
    case laneBusy
    case dispatchFailed
    case dispatched
}
