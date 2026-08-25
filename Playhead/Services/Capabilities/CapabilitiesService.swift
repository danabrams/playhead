// CapabilitiesService.swift
// Detects runtime capabilities and publishes changes via AsyncStream.
// Persists a CapabilitySnapshot with each analysis run so failures
// can be diagnosed after the fact.

import Foundation
import OSLog
import UIKit

#if canImport(FoundationModels)
import FoundationModels
#endif

// MARK: - CapabilitiesProviding

/// Protocol abstraction for capability queries, enabling test stubs.
protocol CapabilitiesProviding: Sendable {
    var currentSnapshot: CapabilitySnapshot { get async }
    func capabilityUpdates() async -> AsyncStream<CapabilitySnapshot>
}

struct FoundationModelsCapabilityState: Sendable, Equatable {
    let available: Bool
    let appleIntelligenceEnabled: Bool
    let localeSupported: Bool

    /// The on-device model's context window size in tokens.
    ///
    /// playhead-xx7m.2 (Phase B): threaded durably through the capability
    /// layer so a real-device run confirms the iOS 27 model reports the
    /// expected ~32k (vs iOS 26's 4096). The ad-classifier's per-window
    /// prompt budget scales linearly with this value, so it drives the
    /// boundary-undersizing retune. Holds the raw
    /// `SystemLanguageModel.default.contextSize`; `0` only when the OS predates
    /// iOS 26 or the compiler predates the API. It reflects the API's own value
    /// regardless of `availability`, so on the simulator it may read 0 or 4096
    /// (model unavailable / warming) — that 4096 is the API's base value, NOT
    /// the classifier's 4096 budget fallback (`fallbackFoundationModelContextSize`,
    /// which lives in the classifier's math, not here).
    ///
    /// **playhead-xul6: `0` now ALSO means "nobody has asked yet".** The read
    /// is no longer performed by `CapabilitiesService.captureSnapshot()` — it
    /// is deferred to `CapabilitiesService.foundationModelsContextSize()`,
    /// which is called from the Settings dogfood-diagnostics export. Until
    /// that call happens this field reads `0` on every snapshot, exactly as it
    /// does on a device that cannot report the value. A reader that needs the
    /// live number must ask the service for it rather than fishing it out of a
    /// snapshot.
    let contextSize: Int

    init(
        available: Bool,
        appleIntelligenceEnabled: Bool,
        localeSupported: Bool,
        contextSize: Int = 0
    ) {
        self.available = available
        self.appleIntelligenceEnabled = appleIntelligenceEnabled
        self.localeSupported = localeSupported
        self.contextSize = contextSize
    }

    #if canImport(FoundationModels)
    @available(iOS 26.0, *)
    init(
        availability: SystemLanguageModel.Availability,
        localeSupported: Bool,
        contextSize: Int = 0
    ) {
        self.localeSupported = localeSupported
        self.contextSize = contextSize

        switch availability {
        case .available:
            self.available = true
            self.appleIntelligenceEnabled = true
        case .unavailable(.modelNotReady):
            self.available = false
            self.appleIntelligenceEnabled = true
        case .unavailable(.appleIntelligenceNotEnabled), .unavailable(.deviceNotEligible):
            self.available = false
            self.appleIntelligenceEnabled = false
        @unknown default:
            self.available = false
            self.appleIntelligenceEnabled = false
        }
    }
    #endif

    /// Returns a copy carrying `contextSize`, leaving every other field alone.
    ///
    /// playhead-xul6: the availability/locale reading and the context-window
    /// reading are now taken at DIFFERENT times — availability on the actor's
    /// own refresh, the window only when something asks for it. This is how
    /// the second one is folded into the first without a second
    /// `SystemLanguageModel` touch.
    func withContextSize(_ contextSize: Int) -> FoundationModelsCapabilityState {
        FoundationModelsCapabilityState(
            available: available,
            appleIntelligenceEnabled: appleIntelligenceEnabled,
            localeSupported: localeSupported,
            contextSize: contextSize
        )
    }
}

// MARK: - CapabilitiesService

/// Actor that detects device capabilities and publishes changes.
///
/// Consumers subscribe to ``capabilityUpdates`` for reactive capability
/// changes (e.g., thermal throttling, Low Power Mode toggled).
actor CapabilitiesService {
    private let logger = Logger(subsystem: "com.playhead", category: "Capabilities")

    /// The most recent capability snapshot.
    private(set) var currentSnapshot: CapabilitySnapshot

    /// At most one readiness probe may run at a time.
    private var foundationModelsProbeTask: Task<Void, Never>?

    /// The most recent `SystemLanguageModel` availability/locale reading, or
    /// `nil` when nobody has taken one yet.
    ///
    /// playhead-xul6: this is `nil` for the whole of `init`, and is filled by
    /// the first `refreshSnapshot()` — which runs on THIS ACTOR's executor,
    /// never on the main actor. See the initialiser's comment.
    private var foundationModelsState: FoundationModelsCapabilityState?

    /// The model's context window in tokens, once something has asked for it.
    /// `nil` means "not read yet", which is a different statement from the
    /// snapshot's `0` ("read and unavailable" OR "not read yet" — the snapshot
    /// cannot tell those apart, which is why the service keeps this).
    private var foundationModelsContextSizeIfRead: Int?

    // AsyncStream plumbing
    private var continuations: [UUID: AsyncStream<CapabilitySnapshot>.Continuation] = [:]

    /// Notification observers kept alive for the actor's lifetime.
    private var observerTokens: [any NSObjectProtocol] = []

    /// **playhead-xul6: NOTHING in this initialiser may touch
    /// `SystemLanguageModel`, directly or one call deeper.**
    ///
    /// `PlayheadRuntime` is `@MainActor` and constructs this service inside
    /// its own synchronous `init` body, so everything here runs ON THE MAIN
    /// ACTOR, before `RootView` can resolve. This initialiser used to call
    /// `Self.captureSnapshot()`, which called `checkFoundationModelsState()`,
    /// which read `SystemLanguageModel.default` and `.contextSize` — measured
    /// at 473–2130 ms of held main actor on the simulator, where the repeated
    /// 2003 ms readings are a 2-second timeout being waited out rather than
    /// work being done. Device cost is unknown IN BOTH DIRECTIONS (playhead-
    /// jndk was a multi-minute device freeze from this same API), which is why
    /// the fix is "never on the main actor, whatever it costs" rather than
    /// "it is fast enough on a device".
    ///
    /// The FoundationModels fields are therefore ABSENT in the seed snapshot
    /// (`available`/`appleIntelligenceEnabled`/`localeSupported` false,
    /// `contextSize` 0 — byte-identical to what `checkFoundationModelsState()`
    /// returns on a build without the framework) and are filled by the
    /// `refreshSnapshot()` below, which runs on this actor's own executor.
    /// That seed is not observable from outside: `currentSnapshot` is
    /// actor-isolated, so every reader must enqueue a job on this actor, and
    /// the refresh below is enqueued first — before the service has been
    /// handed to anybody.
    init() {
        self.currentSnapshot = Self.captureSnapshot(foundationModels: nil)
        Task { await self.refreshSnapshot() }
    }

    // MARK: - Public API

    /// Takes a fresh snapshot, logs it, and publishes to all subscribers.
    /// Call this at first launch and whenever a significant state change occurs.
    ///
    /// Runs on the service actor's executor (a cooperative-pool thread), never
    /// on the main actor — `CapabilitiesService` is a plain `actor`, so every
    /// caller reaches it through an `await`. That is what makes the
    /// `SystemLanguageModel` read below a main-actor SUSPENSION rather than a
    /// main-actor BLOCK. See the initialiser's comment (playhead-xul6).
    func refreshSnapshot() {
        let modelState = Self.checkFoundationModelsState(
            contextSize: foundationModelsContextSizeIfRead ?? 0
        )
        foundationModelsState = modelState
        publish(Self.captureSnapshot(foundationModels: modelState))
    }

    /// The on-device model's context window in tokens, read on FIRST ASK and
    /// cached for the service's lifetime.
    ///
    /// playhead-xul6: this read used to happen inside the synchronous
    /// `captureSnapshot()` on every refresh — including the one in `init`, on
    /// the main actor. It has exactly one production consumer (the Settings
    /// dogfood-diagnostics export) and nothing on the launch path needs it, so
    /// it is deferred to here. The ad-classifier does NOT read this: it takes
    /// its own live `SystemLanguageModel.default.contextSize` per run through
    /// `FoundationModelClassifier`'s `contextSize` closure, and keeps a 4096
    /// budget fallback of its own.
    ///
    /// Once read, the value is folded into the published snapshot so the
    /// capability log line and any persisted snapshot carry it — without a
    /// second `SystemLanguageModel` availability touch.
    func foundationModelsContextSize() -> Int {
        if let known = foundationModelsContextSizeIfRead {
            return known
        }
        let value = Self.readFoundationModelsContextSize()
        foundationModelsContextSizeIfRead = value

        if let state = foundationModelsState, state.contextSize != value {
            let merged = state.withContextSize(value)
            foundationModelsState = merged
            publish(Self.captureSnapshot(foundationModels: merged))
        }
        return value
    }

    /// Stores, logs and broadcasts `snapshot`, then schedules the readiness
    /// probe if the snapshot warrants one. Split out of `refreshSnapshot()`
    /// (playhead-xul6) so the lazy context-size read can republish without
    /// re-reading availability.
    private func publish(_ snapshot: CapabilitySnapshot) {
        currentSnapshot = snapshot

        logger.info("""
        Capability snapshot captured: \
        foundationModels=\(snapshot.foundationModelsAvailable), \
        foundationModelsUsable=\(snapshot.foundationModelsUsable), \
        appleIntelligence=\(snapshot.appleIntelligenceEnabled), \
        localeSupported=\(snapshot.foundationModelsLocaleSupported), \
        contextSize=\(snapshot.foundationModelsContextSize), \
        thermal=\(snapshot.thermalState.description), \
        lowPower=\(snapshot.isLowPowerMode), \
        charging=\(snapshot.isCharging), \
        bgProcessing=\(snapshot.backgroundProcessingSupported), \
        diskSpace=\(snapshot.availableDiskSpaceBytes / (1024 * 1024))MB
        """)

        for (_, continuation) in continuations {
            continuation.yield(snapshot)
        }

        scheduleFoundationModelsProbeIfNeeded(for: snapshot)
    }

    /// Returns an AsyncStream that emits capability snapshots whenever
    /// device state changes (thermal, power mode, etc.).
    func capabilityUpdates() -> AsyncStream<CapabilitySnapshot> {
        let id = UUID()
        return AsyncStream { continuation in
            // Yield current state immediately so consumers don't wait.
            continuation.yield(currentSnapshot)

            continuation.onTermination = { [weak self] _ in
                guard let self else { return }
                Task { await self.removeContinuation(id: id) }
            }

            // playhead-xul6 (review): REGISTER SYNCHRONOUSLY. This closure runs
            // on this actor — the `yield` above reads actor state — so the hop
            // was never needed, and it was a hole. Between `capabilityUpdates()`
            // returning and a deferred `storeContinuation` landing, a
            // `publish(...)` walks a `continuations` map that does not yet hold
            // this subscriber and the emission is DROPPED.
            //
            // That was harmless while `init` captured the real FoundationModels
            // reading, because the seed and the first `refreshSnapshot()` agreed.
            // It is not harmless now: the seed reports FM ABSENT and the first
            // refresh is the emission that corrects it. A subscriber that loses
            // exactly that one leaves `CapabilitySnapshotCache` — and the
            // 4-hour `AnalysisEligibilityEvaluator` verdict computed from it —
            // holding the absent reading until the next device-state change,
            // which on a quiet device is not soon. The whole safety case for the
            // deferred read is "every consumer re-evaluates on the next
            // emission"; this is what makes that true rather than likely.
            storeContinuation(id: id, continuation: continuation)
        }
    }

    /// Starts observing system notifications for capability-relevant changes.
    /// Safe to call multiple times — removes old observers before adding new ones.
    func startObserving() {
        UIDevice.current.isBatteryMonitoringEnabled = true

        // Remove any previously registered observers to prevent leaks on double-call.
        removeObservers()

        let center = NotificationCenter.default

        let thermalToken = center.addObserver(
            forName: ProcessInfo.thermalStateDidChangeNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            guard let self else { return }
            Task { await self.refreshSnapshot() }
        }

        let powerToken = center.addObserver(
            forName: Notification.Name.NSProcessInfoPowerStateDidChange,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            guard let self else { return }
            Task { await self.refreshSnapshot() }
        }

        let batteryToken = center.addObserver(
            forName: UIDevice.batteryStateDidChangeNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            guard let self else { return }
            Task { await self.refreshSnapshot() }
        }

        observerTokens = [thermalToken, powerToken, batteryToken]
        refreshSnapshot()
    }

    // MARK: - Snapshot Capture

    /// Captures a point-in-time snapshot of all device capabilities.
    ///
    /// **This function does not read `SystemLanguageModel` — the caller
    /// supplies the FoundationModels reading, or `nil` for "not read yet"
    /// (playhead-xul6).** The device-state reads that remain here (thermal,
    /// low-power, disk, battery) are all single-digit milliseconds and are
    /// wanted synchronously; the FoundationModels read was 97–99 % of the
    /// cost and is the only one that had to move.
    private static func captureSnapshot(
        foundationModels modelState: FoundationModelsCapabilityState?
    ) -> CapabilitySnapshot {
        let processInfo = ProcessInfo.processInfo

        let thermalState = ThermalState(from: processInfo.thermalState)
        let isLowPowerMode = processInfo.isLowPowerModeEnabled
        let backgroundProcessingSupported = checkBackgroundProcessingSupported()
        let availableDiskSpace = queryAvailableDiskSpace()

        let batteryState = UIDevice.current.batteryState
        let isCharging = batteryState == .charging || batteryState == .full

        return CapabilitySnapshot(
            foundationModelsAvailable: modelState?.available ?? false,
            foundationModelsUsable: FoundationModelsUsabilityProbe.cachedUsability() ?? false,
            appleIntelligenceEnabled: modelState?.appleIntelligenceEnabled ?? false,
            foundationModelsLocaleSupported: modelState?.localeSupported ?? false,
            foundationModelsContextSize: modelState?.contextSize ?? 0,
            thermalState: thermalState,
            isLowPowerMode: isLowPowerMode,
            isCharging: isCharging,
            backgroundProcessingSupported: backgroundProcessingSupported,
            availableDiskSpaceBytes: availableDiskSpace,
            capturedAt: .now
        )
    }

    // MARK: - Capability Checks

    /// Reads `SystemLanguageModel` availability and locale support.
    ///
    /// **Never call this from a main-actor context.** It is only reached from
    /// `refreshSnapshot()`, which is actor-isolated (playhead-xul6). The
    /// context window is NOT read here — it is passed in from whatever
    /// `foundationModelsContextSize()` has already established, which is 0
    /// until something asks.
    private static func checkFoundationModelsState(
        contextSize: Int
    ) -> FoundationModelsCapabilityState {
        #if canImport(FoundationModels)
        if #available(iOS 26.0, *) {
            let model = SystemLanguageModel.default
            return FoundationModelsCapabilityState(
                availability: model.availability,
                localeSupported: model.supportsLocale(),
                contextSize: contextSize
            )
        }
        return FoundationModelsCapabilityState(
            available: false,
            appleIntelligenceEnabled: false,
            localeSupported: false
        )
        #else
        return FoundationModelsCapabilityState(
            available: false,
            appleIntelligenceEnabled: false,
            localeSupported: false
        )
        #endif
    }

    /// Reads the on-device model's context window.
    ///
    /// playhead-xx7m.2 (Phase B): read the on-device model's context window so
    /// a real-device run confirms the iOS 27 model reports the expected ~32k
    /// (vs iOS 26's 4096). FM only runs on device, so this is the measurement
    /// hook — one clean breadcrumb, then the value is threaded durably into
    /// the capability state. `contextSize` is unavailable on compilers
    /// predating the API, in which case it stays 0 (the classifier keeps its
    /// own 4096 budget fallback).
    ///
    /// **Never call this from a main-actor context** — it is reached only from
    /// the actor-isolated `foundationModelsContextSize()` (playhead-xul6).
    /// Adding `let contextSize = model.contextSize` to the synchronous
    /// snapshot path is what put 0.47–2.13 s on the launch path.
    private static func readFoundationModelsContextSize() -> Int {
        #if canImport(FoundationModels)
        if #available(iOS 26.0, *) {
            #if compiler(>=6.3)
            let contextSize = SystemLanguageModel.default.contextSize
            Logger(subsystem: "com.playhead", category: "Capabilities")
                .notice("fm.capability.context_window contextSize=\(contextSize, privacy: .public)")
            return contextSize
            #else
            return 0
            #endif
        }
        return 0
        #else
        return 0
        #endif
    }

    private static func checkBackgroundProcessingSupported() -> Bool {
        // BGTaskScheduler is always available on iOS 13+.
        // Registration success is the real gate, but we report platform support here.
        return true
    }

    private static func queryAvailableDiskSpace() -> Int64 {
        let fileManager = FileManager.default
        guard let homeURL = fileManager.urls(for: .documentDirectory, in: .userDomainMask).first else {
            return 0
        }
        do {
            let values = try homeURL.resourceValues(forKeys: [.volumeAvailableCapacityForImportantUsageKey])
            return values.volumeAvailableCapacityForImportantUsage ?? 0
        } catch {
            return 0
        }
    }

    // MARK: - Internal Helpers

    private func storeContinuation(id: UUID, continuation: AsyncStream<CapabilitySnapshot>.Continuation) {
        continuations[id] = continuation
    }

    private func removeContinuation(id: UUID) {
        continuations.removeValue(forKey: id)
    }

    private func scheduleFoundationModelsProbeIfNeeded(for snapshot: CapabilitySnapshot) {
        guard snapshot.foundationModelsAvailable,
              snapshot.appleIntelligenceEnabled,
              snapshot.foundationModelsLocaleSupported,
              !snapshot.foundationModelsUsable,
              FoundationModelsUsabilityProbe.cachedUsability() == nil,
              foundationModelsProbeTask == nil else {
            return
        }

        foundationModelsProbeTask = Task { [weak self] in
            #if canImport(FoundationModels)
            if #available(iOS 26.0, *) {
                _ = await FoundationModelsUsabilityProbe.probeIfNeeded(logger: self?.logger ?? Logger(subsystem: "com.playhead", category: "Capabilities"))
            }
            #endif

            guard let self else { return }
            await self.finishFoundationModelsProbe()
        }
    }

    private func finishFoundationModelsProbe() {
        foundationModelsProbeTask = nil
        refreshSnapshot()
    }

    /// playhead-882eg: teardown counterpart to ``startObserving()``.
    ///
    /// The observers themselves are `[weak self]` with stored tokens, so they
    /// never retained this actor — but they were never removed either, and one
    /// set of three accumulated in `NotificationCenter` per constructed
    /// ``PlayheadRuntime``. ``startObserving()`` already calls
    /// ``removeObservers()`` for the double-call case; this exposes the same
    /// operation to the runtime's terminal owner boundary.
    func stopObserving() {
        removeObservers()
    }

    /// Remove all registered notification observers and clear the token list.
    private func removeObservers() {
        let center = NotificationCenter.default
        for token in observerTokens {
            center.removeObserver(token)
        }
        observerTokens.removeAll()
    }
}

// MARK: - CapabilitiesProviding Conformance

extension CapabilitiesService: CapabilitiesProviding {}
