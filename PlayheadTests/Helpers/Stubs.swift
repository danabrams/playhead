// Stubs.swift
// Configurable test doubles for service protocols used in pre-analysis tests.

import BackgroundTasks
import Foundation
@testable import Playhead

// MARK: - StubAnalysisAudioProvider

final class StubAnalysisAudioProvider: AnalysisAudioProviding, @unchecked Sendable {
    var shardsToReturn: [AnalysisShard] = []
    var errorToThrow: Error?
    var decodeCallCount = 0

    func decode(fileURL: LocalAudioURL, episodeID: String, shardDuration: TimeInterval) async throws -> [AnalysisShard] {
        decodeCallCount += 1
        if let error = errorToThrow { throw error }
        return shardsToReturn
    }
}

// MARK: - StubAdDetectionProvider

final class StubAdDetectionProvider: AdDetectionProviding, @unchecked Sendable {
    var hotPathResult: [AdWindow] = []
    var hotPathError: Error?
    var backfillError: Error?
    var hotPathCallCount = 0
    var backfillCallCount = 0
    /// Cycle 4 H5: records the sessionId passed on each `runBackfill` call
    /// so regression tests can assert the coordinator threaded it through.
    var backfillSessionIds: [String?] = []
    /// playhead-zx6i — counts every `revalidateFromFeatures` call so the
    /// B4 short-circuit tests can assert the runner picked the
    /// revalidation path over the full-analysis path.
    var revalidateFromFeaturesCallCount = 0
    /// playhead-zx6i — error to throw on the next `revalidateFromFeatures`
    /// call, mirroring `backfillError` for failure-path coverage.
    var revalidateFromFeaturesError: Error?
    /// playhead-zx6i — records every `(assetId, podcastId, episodeDuration, sessionId)`
    /// tuple passed to `revalidateFromFeatures` so tests can assert the
    /// runner forwarded the parameters intact.
    var revalidateFromFeaturesCalls: [(assetId: String, podcastId: String, episodeDuration: Double, sessionId: String?)] = []

    func runHotPath(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        episodeDuration: Double,
        podcastId: String?
    ) async throws -> [AdWindow] {
        hotPathCallCount += 1
        if let error = hotPathError { throw error }
        return hotPathResult
    }

    func runBackfill(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double,
        sessionId: String?
    ) async throws {
        backfillCallCount += 1
        backfillSessionIds.append(sessionId)
        if let error = backfillError { throw error }
    }

    func revalidateFromFeatures(
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double,
        sessionId: String?
    ) async throws {
        revalidateFromFeaturesCallCount += 1
        revalidateFromFeaturesCalls.append((
            assetId: analysisAssetId,
            podcastId: podcastId,
            episodeDuration: episodeDuration,
            sessionId: sessionId
        ))
        if let error = revalidateFromFeaturesError { throw error }
    }
}

// MARK: - StubCapabilitiesProvider

final class StubCapabilitiesProvider: CapabilitiesProviding, @unchecked Sendable {
    var currentSnapshot: CapabilitySnapshot

    init(snapshot: CapabilitySnapshot? = nil) {
        self.currentSnapshot = snapshot ?? CapabilitySnapshot(
            foundationModelsAvailable: false,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: false,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: false,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 10 * 1024 * 1024 * 1024,
            capturedAt: .now
        )
    }

    func capabilityUpdates() -> AsyncStream<CapabilitySnapshot> {
        AsyncStream { continuation in
            continuation.yield(currentSnapshot)
            continuation.finish()
        }
    }
}

// MARK: - StubDownloadProvider

final class StubDownloadProvider: DownloadProviding, @unchecked Sendable {
    var cachedURLs: [String: URL] = [:]
    var fingerprints: [String: AudioFingerprint] = [:]

    func cachedFileURL(for episodeId: String) -> URL? {
        cachedURLs[episodeId]
    }

    func fingerprint(for episodeId: String) -> AudioFingerprint? {
        fingerprints[episodeId]
    }

    func allCachedEpisodeIds() -> Set<String> {
        Set(cachedURLs.keys)
    }
}

// MARK: - StubAnalysisStore

/// Wraps a real AnalysisStore backed by a temp directory for test isolation.
/// Use `makeTestStore()` from TestHelpers.swift to create instances.
final class StubAnalysisStore: @unchecked Sendable {
    let store: AnalysisStore
    let directory: URL

    init(store: AnalysisStore, directory: URL) {
        self.store = store
        self.directory = directory
    }
}

// MARK: - TestEventCounter

/// playhead-vsot: lock-protected counting event for event-driven test
/// synchronization. Replaces the "poll a flag every 10 ms until a
/// real-time deadline" pattern, which under full-suite contention turns
/// behavior assertions into scheduler assertions (the awaited work IS
/// still coming — the deadline just expires first).
///
/// `increment()` is called by the code path under observation;
/// `wait(for:)` suspends the test until the count reaches the
/// threshold and returns IMMEDIATELY if it already has. There is
/// deliberately no timeout parameter: the awaited signal is the actual
/// completion event, and the test's `.timeLimit` trait is the backstop
/// for genuine regressions (a hang becomes a deterministic time-limit
/// failure instead of a load-dependent flake).
final class TestEventCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var count = 0
    private var waiters: [(threshold: Int, continuation: CheckedContinuation<Void, Never>)] = []

    /// Current count. For post-wait assertions only — never poll this.
    var current: Int {
        lock.lock()
        defer { lock.unlock() }
        return count
    }

    func increment() {
        lock.lock()
        count += 1
        let reached = count
        let ready = waiters.filter { $0.threshold <= reached }
        waiters.removeAll { $0.threshold <= reached }
        lock.unlock()
        for waiter in ready { waiter.continuation.resume() }
    }

    func wait(for threshold: Int = 1) async {
        if hasReached(threshold) { return }
        await withCheckedContinuation { continuation in
            register(threshold: threshold, continuation: continuation)
        }
    }

    // NSLock's lock()/unlock() are unavailable directly inside async
    // functions, so the locking work lives in these synchronous helpers
    // (the withCheckedContinuation body is synchronous).

    private func hasReached(_ threshold: Int) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return count >= threshold
    }

    private func register(
        threshold: Int,
        continuation: CheckedContinuation<Void, Never>
    ) {
        lock.lock()
        if count >= threshold {
            lock.unlock()
            continuation.resume()
            return
        }
        waiters.append((threshold, continuation))
        lock.unlock()
    }
}

// MARK: - StubBackgroundTask

final class StubBackgroundTask: BackgroundProcessingTaskProtocol, @unchecked Sendable {
    var completedSuccess: Bool?
    var expirationHandler: (() -> Void)? {
        didSet {
            // playhead-vsot: signal installation so tests can await the
            // handler being armed instead of polling under a deadline.
            if expirationHandler != nil {
                expirationHandlerInstalls.increment()
            }
        }
    }
    /// Total count of `setTaskCompleted(success:)` calls. iOS terminates
    /// on a second call, so the idempotence guard in
    /// `BackgroundFeedRefreshService.completeTaskOnce` asserts this
    /// stays at exactly 1 across an expired-then-finished handler fire.
    private(set) var setTaskCompletedCallCount: Int = 0

    /// playhead-vsot: event-driven completion/installation signals.
    let completions = TestEventCounter()
    let expirationHandlerInstalls = TestEventCounter()

    func setTaskCompleted(success: Bool) {
        completedSuccess = success
        setTaskCompletedCallCount += 1
        completions.increment()
    }

    /// Simulate iOS firing the expiration handler.
    func simulateExpiration() {
        expirationHandler?()
    }

    /// Suspend until `setTaskCompleted` has been called at least once.
    /// Returns immediately if it already was.
    func awaitCompletion() async {
        await completions.wait(for: 1)
    }

    /// Suspend until the handler under test has installed its
    /// `expirationHandler`. Returns immediately if already installed.
    func awaitExpirationHandlerInstalled() async {
        await expirationHandlerInstalls.wait(for: 1)
    }
}

/// playhead-44h1: stub for `BGContinuedProcessingTask`. Carries the
/// wildcard identifier so the handler's parsing logic can be
/// exercised without an actual BG task instance.
final class StubContinuedProcessingTask: ContinuedProcessingTaskProtocol, @unchecked Sendable {
    let identifier: String
    var completedSuccess: Bool?
    var expirationHandler: (() -> Void)? {
        didSet {
            if expirationHandler != nil {
                expirationHandlerInstalls.increment()
            }
        }
    }

    /// playhead-vsot: event-driven completion/installation signals.
    let completions = TestEventCounter()
    let expirationHandlerInstalls = TestEventCounter()

    init(identifier: String) {
        self.identifier = identifier
    }

    func setTaskCompleted(success: Bool) {
        completedSuccess = success
        completions.increment()
    }

    /// Simulate iOS firing the expiration handler.
    func simulateExpiration() {
        expirationHandler?()
    }

    /// Suspend until `setTaskCompleted` has been called at least once.
    func awaitCompletion() async {
        await completions.wait(for: 1)
    }

    /// Suspend until the handler under test has installed its
    /// `expirationHandler`.
    func awaitExpirationHandlerInstalled() async {
        await expirationHandlerInstalls.wait(for: 1)
    }
}

// MARK: - StubTaskScheduler

final class StubTaskScheduler: BackgroundTaskScheduling, @unchecked Sendable {
    /// Append-only log of every `submit` call. Records submit calls even
    /// when a same-identifier request was already pending (a submit always
    /// gets logged here), so tests can assert exact submit counts.
    var submittedRequests: [BGTaskRequest] = []
    var shouldThrowOnSubmit = false

    /// Identifiers of currently-pending requests — models
    /// `BGTaskScheduler`'s dedupe-by-identifier: a `submit` marks that
    /// identifier pending, and `pendingTaskRequestIdentifiers()` returns
    /// the set. Tests may also pre-seed via `seedPending(_:)` to simulate a
    /// request left pending by a prior launch (playhead-y5mk).
    private var pendingIdentifiers: Set<String> = []

    func submit(_ taskRequest: BGTaskRequest) throws {
        if shouldThrowOnSubmit {
            throw NSError(domain: "StubTaskScheduler", code: 1)
        }
        submittedRequests.append(taskRequest)
        pendingIdentifiers.insert(taskRequest.identifier)
    }

    func pendingTaskRequestIdentifiers() async -> [String] {
        Array(pendingIdentifiers)
    }

    /// Pre-seed a pending request without a `submit` (models a request the
    /// OS still holds pending from a prior launch). Used to prove
    /// `scheduleNextRefresh` skips the submit when one is already pending.
    func seedPending(_ request: BGTaskRequest) {
        pendingIdentifiers.insert(request.identifier)
    }

    /// Drop an identifier from the pending set without running its handler —
    /// models iOS CONSUMING a request when it launches the task. Needed to
    /// observe a guarded scheduler across more than one arm/consume cycle:
    /// without it every second call is legitimately skipped as "already
    /// pending", and a reentrancy latch that never releases is
    /// indistinguishable from a working guard (playhead-mk6z, the failure
    /// mode playhead-c25o documents).
    func clearPending(_ identifier: String) {
        pendingIdentifiers.remove(identifier)
    }
}

// MARK: - StubAnalysisCoordinator

final class StubAnalysisCoordinator: AnalysisCoordinating, @unchecked Sendable {
    var startCapabilityObserverCallCount = 0
    var stopCallCount = 0
    var runPendingBackfillCallCount = 0
    /// If set, `startCapabilityObserver()` will sleep this long to simulate work.
    /// Retained for tests that exercise the observer lifecycle path.
    var startCapabilityObserverDuration: Duration?
    /// If set, `runPendingBackfill()` will sleep this long to simulate work.
    /// Used by background-task expiration tests to keep the BG task open
    /// long enough for the expiration handler to fire. The sleep is
    /// cancellation-responsive (`Task.sleep` throws on cancel), so
    /// expiration-driven cancellation cuts it short deterministically.
    var runPendingBackfillDuration: Duration?

    /// playhead-vsot: event-driven signals.
    /// `stopCalls` increments on every `stop()` — the observable tail of
    /// the backfill expiration-handler chain (… → finishRun →
    /// handleExpiredProcessingTask → stop() → markComplete), so tests
    /// await it instead of polling `stopCallCount` under a deadline.
    let stopCalls = TestEventCounter()
    /// Increments when `runPendingBackfill()` is ENTERED, so tests can
    /// deterministically establish "backfill work is in flight".
    let runPendingBackfillEntries = TestEventCounter()
    /// When true, `runPendingBackfill()` suspends until
    /// `runPendingBackfillReleases.increment()` is called. Gives tests a
    /// guaranteed overlap window with no wall-clock stall duration.
    /// NOT cancellation-responsive — use `runPendingBackfillDuration`
    /// for expiration/cancellation tests instead.
    var runPendingBackfillHoldsUntilReleased = false
    let runPendingBackfillReleases = TestEventCounter()

    // MARK: playhead-44h1 hooks
    /// Captures every `continueForegroundAssist(episodeId:deadline:)` call
    /// so tests can assert the episode id and deadline were threaded.
    private(set) var continueForegroundAssistCalls: [(episodeId: String, deadline: Date)] = []
    /// Captures every `pauseAtNextCheckpoint(episodeId:cause:)` call so
    /// tests can assert the cause propagates from the expiration handler.
    private(set) var pauseAtNextCheckpointCalls: [(episodeId: String, cause: InternalMissCause)] = []
    /// If set, `continueForegroundAssist` throws this error so tests
    /// can cover the failure-mapping path.
    var continueForegroundAssistError: Error?
    /// If set, `continueForegroundAssist` blocks this long before
    /// returning so tests can exercise the expiration-handler race.
    var continueForegroundAssistDuration: Duration?
    /// If set, `continueForegroundAssist` waits for
    /// `pauseAtNextCheckpoint(episodeId:...)` to fire (checking the
    /// `pauseAtNextCheckpointCalls` array) before returning. Lets tests
    /// drive the expiration → pause → task-failed ordering without
    /// wall-clock races.
    var continueForegroundAssistWaitsForPause: Bool = false
    /// Captures every `recordForegroundAssistOutcome(episodeId:
    /// eventType:cause:)` call so tests can assert the expire /
    /// complete paths wrote the expected WorkJournal row (playhead-
    /// 44h1 fix — spec state-machine step 5).
    private(set) var recordForegroundAssistOutcomeCalls: [(
        episodeId: String,
        eventType: WorkJournalEntry.EventType,
        cause: InternalMissCause?
    )] = []
    /// playhead-vsot: event signal for the journal append above. In the
    /// NON-expiration continued-processing paths, production intentionally
    /// calls `markComplete` BEFORE `appendTerminal` (race-gating against
    /// the expiration path), so task completion is NOT the journal-row
    /// signal — tests asserting journal rows must await THIS event.
    let recordedOutcomes = TestEventCounter()

    func startCapabilityObserver() async {
        startCapabilityObserverCallCount += 1
        if let duration = startCapabilityObserverDuration {
            try? await Task.sleep(for: duration)
        }
    }

    func stop() async {
        stopCallCount += 1
        stopCalls.increment()
    }

    /// playhead-lmrx: every deadline the handler hands this method, captured so
    /// a test can assert the budget it was spending — the pre-fix value was
    /// `now + 25 minutes` inside a measured 294 s grant.
    private(set) var runPendingBackfillDeadlines: [ContinuousClock.Instant] = []

    func runPendingBackfill(deadline: ContinuousClock.Instant) async {
        runPendingBackfillCallCount += 1
        grantDriverCallOrder.append("poll")
        runPendingBackfillDeadlines.append(deadline)
        runPendingBackfillEntries.increment()
        if let duration = runPendingBackfillDuration {
            try? await Task.sleep(for: duration)
        }
        if runPendingBackfillHoldsUntilReleased {
            await runPendingBackfillReleases.wait(for: 1)
        }
    }

    // MARK: playhead-13kf hooks

    /// Shared call-order journal across the two grant-spending coordinator
    /// drivers ("coarse", "poll"), because the reorder's contract is an ORDER
    /// and two independent counters cannot express one.
    private(set) var grantDriverCallOrder: [String] = []

    /// Every `(deadline, minimumWindowBudget)` pair the handler handed the
    /// FM-first coarse phase, so tests can assert the phase spends the SAME
    /// grant instant as the drivers behind it and is gated by the measured
    /// coarse-window floor.
    private(set) var runPendingCoarseScansCalls:
        [(deadline: ContinuousClock.Instant, minimumWindowBudget: Duration)] = []
    var runPendingCoarseScansCallCount = 0
    /// If set, `runPendingCoarseScans` sleeps this long — cancellation-
    /// responsive, like `runPendingBackfillDuration` — so tests can burn a
    /// slice of the grant inside the coarse phase and observe what the
    /// later phases were handed.
    var runPendingCoarseScansDuration: Duration?
    /// If set, invoked ON ENTRY of `runPendingCoarseScans`, BEFORE any sleep.
    /// The ordering pin uses this to snapshot scheduler-visible state at the
    /// exact moment the coarse phase runs — the only observation point that
    /// can tell "before the drain" from "after the drain".
    var onRunPendingCoarseScans: (@Sendable () async -> Void)?
    /// The value `runPendingCoarseScans` reports as assets driven.
    var runPendingCoarseScansResult = 0
    /// playhead-8ljj: what the stub phase reports about ITSELF. Published
    /// BEFORE the optional sleep, so a test that expires the grant while the
    /// coarse phase is parked sees exactly what a real reclaimed window sees —
    /// a census with no terminal verdict.
    var coarseScanPhaseReport: CoarseScanPhaseReport?
    /// playhead-8ljj: what `semanticScanRowsRecorded(since:)` answers. `nil`
    /// models a store read that FAILED, which the ledger must render `banked=?`
    /// rather than `banked=0`.
    var semanticScanRowsRecordedResult: Int? = 0
    /// Every `since` the handler asked about, so a test can pin that the count
    /// is scoped to the grant rather than to the whole table.
    private(set) var semanticScanRowsRecordedCalls: [Double] = []

    func runPendingCoarseScans(
        deadline: ContinuousClock.Instant,
        minimumWindowBudget: Duration,
        report: @Sendable (CoarseScanPhaseReport) -> Void
    ) async -> Int {
        runPendingCoarseScansCallCount += 1
        grantDriverCallOrder.append("coarse")
        runPendingCoarseScansCalls.append(
            (deadline: deadline, minimumWindowBudget: minimumWindowBudget)
        )
        if let phaseReport = coarseScanPhaseReport {
            report(phaseReport)
        }
        if let hook = onRunPendingCoarseScans {
            await hook()
        }
        if let duration = runPendingCoarseScansDuration {
            try? await Task.sleep(for: duration)
        }
        return runPendingCoarseScansResult
    }

    func semanticScanRowsRecorded(since wallClock: Double) async -> Int? {
        semanticScanRowsRecordedCalls.append(wallClock)
        return semanticScanRowsRecordedResult
    }

    func continueForegroundAssist(episodeId: String, deadline: Date) async throws {
        continueForegroundAssistCalls.append((episodeId: episodeId, deadline: deadline))
        if let error = continueForegroundAssistError {
            throw error
        }
        if let duration = continueForegroundAssistDuration {
            try? await Task.sleep(for: duration)
        }
        if continueForegroundAssistWaitsForPause {
            // Poll for a matching pause request. Yields between checks
            // so the actor-serialized pause call can land.
            while !Task.isCancelled {
                if pauseAtNextCheckpointCalls.contains(where: { $0.episodeId == episodeId }) {
                    return
                }
                try? await Task.sleep(for: .milliseconds(5))
            }
        }
    }

    func pauseAtNextCheckpoint(episodeId: String, cause: InternalMissCause) async {
        pauseAtNextCheckpointCalls.append((episodeId: episodeId, cause: cause))
    }

    func recordForegroundAssistOutcome(
        episodeId: String,
        eventType: WorkJournalEntry.EventType,
        cause: InternalMissCause?
    ) async {
        recordForegroundAssistOutcomeCalls.append(
            (episodeId: episodeId, eventType: eventType, cause: cause)
        )
        recordedOutcomes.increment()
    }
}

// MARK: - StubBatteryProvider

final class StubBatteryProvider: BatteryStateProviding, @unchecked Sendable {
    var level: Float = 1.0
    var charging: Bool = true

    func currentBatteryState() async -> (level: Float, isCharging: Bool) {
        (level, charging)
    }
}

// MARK: - StubTransportStatusProvider
//
// skeptical-review-cycle-18 M-1: a deterministic transport stub that
// pins reachability to .wifi (and `userAllowsCellular` to true) so
// scheduler tests cannot flake on `LiveTransportStatusProvider`'s
// NWPathMonitor first-update latency under parallel load. Every test
// that constructs an `AnalysisWorkScheduler` MUST pass an explicit
// `transportStatusProvider:` argument — the source canary at
// `AnalysisWorkSchedulerTransportStubSourceCanaryTests.swift`
// enforces this. If a new test suite needs to vary the transport
// axis (e.g. exercising `.unreachable` or `allowsCellular = false`),
// initialize this stub with the appropriate values rather than
// reaching for `LiveTransportStatusProvider()`.
//
// **Cycle-19 L-4 caveat — permissive defaults can mask cellular bugs:**
// the default `(reachability: .wifi, allowsCellular: true)` is safe
// for the vast majority of scheduler tests (they don't care about the
// transport axis and just want admission to succeed). It is NOT safe
// for any test that exercises a cellular-rejection branch — e.g.
// "scheduler must defer this lane when `userAllowsCellular = false`".
// Those tests MUST construct the stub with an EXPLICIT cellular value:
//
//     let stub = StubTransportStatusProvider(
//         reachability: .cellular,
//         allowsCellular: false   // ← required: don't lean on the default
//     )
//
// If the default `true` is used by accident in a cellular test, the
// scheduler will accept the lane and the test will still pass, even if
// the production cellular-rejection logic is broken. Reviewer cycle-19
// L-4 flagged this as a silent-blindness risk; this paragraph is the
// documented mitigation. If you are writing a cellular-axis test and
// in doubt, prefer building the stub via the parameter labels above
// rather than relying on the defaults.
struct StubTransportStatusProvider: TransportStatusProviding {
    let reachability: TransportSnapshot.Reachability
    let allowsCellular: Bool
    /// playhead-4dqe: iOS Low Data Mode. Defaults to `false` — a stub device is
    /// not in Low Data Mode — but it is an explicit parameter rather than a
    /// hardcoded `false` so a test that wants the constrained path can ask for
    /// it, for the same reason `allowsCellular` is a parameter.
    let isLowData: Bool

    init(
        reachability: TransportSnapshot.Reachability = .wifi,
        allowsCellular: Bool = true,
        isLowData: Bool = false
    ) {
        self.reachability = reachability
        self.allowsCellular = allowsCellular
        self.isLowData = isLowData
    }

    func currentReachability() async -> TransportSnapshot.Reachability {
        reachability
    }
    func userAllowsCellular() async -> Bool { allowsCellular }
    func isLowDataMode() async -> Bool { isLowData }
}

// MARK: - Day-0 transport snapshots (playhead-4dqe)

extension DayZeroTransportSnapshot {
    /// The PRE-4dqe world, reproduced exactly: a reachability, no Low Data
    /// Mode, and the shipping default of WiFi-only.
    ///
    /// Used by the suites written before the transport setting existed, so they
    /// keep asserting what they always asserted — WiFi passes, cellular is
    /// refused. A test that wants the NEW behavior (cellular admitted by the
    /// setting) states `allowsCellular: true` itself, which is what makes the
    /// difference visible in the diff rather than inherited from a helper.
    static func testSnapshot(_ reachability: TransportSnapshot.Reachability) -> Self {
        DayZeroTransportSnapshot(
            reachability: reachability,
            isLowDataMode: false,
            allowsCellular: false
        )
    }

    static let testWifi = testSnapshot(.wifi)
    static let testCellularNotAllowed = testSnapshot(.cellular)
}
