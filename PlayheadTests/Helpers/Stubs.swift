// Stubs.swift
// Configurable test doubles for service protocols used in pre-analysis tests.

import BackgroundTasks
import Foundation
@testable import Playhead

// MARK: - StubAnalysisAudioProvider

final class StubAnalysisAudioProvider: AnalysisAudioProviding, @unchecked Sendable {
    var shardsToReturn: [AnalysisShard] = []
    var errorToThrow: Error?

    func decode(fileURL: LocalAudioURL, episodeID: String, shardDuration: TimeInterval) async throws -> [AnalysisShard] {
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

    func runHotPath(chunks: [TranscriptChunk], analysisAssetId: String, episodeDuration: Double) async throws -> [AdWindow] {
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

// MARK: - StubBackgroundTask

final class StubBackgroundTask: BackgroundProcessingTaskProtocol, @unchecked Sendable {
    var completedSuccess: Bool?
    var expirationHandler: (() -> Void)?
    /// Total count of `setTaskCompleted(success:)` calls. iOS terminates
    /// on a second call, so the idempotence guard in
    /// `BackgroundFeedRefreshService.completeTaskOnce` asserts this
    /// stays at exactly 1 across an expired-then-finished handler fire.
    private(set) var setTaskCompletedCallCount: Int = 0

    func setTaskCompleted(success: Bool) {
        completedSuccess = success
        setTaskCompletedCallCount += 1
    }

    /// Simulate iOS firing the expiration handler.
    func simulateExpiration() {
        expirationHandler?()
    }
}

/// playhead-44h1: stub for `BGContinuedProcessingTask`. Carries the
/// wildcard identifier so the handler's parsing logic can be
/// exercised without an actual BG task instance.
final class StubContinuedProcessingTask: ContinuedProcessingTaskProtocol, @unchecked Sendable {
    let identifier: String
    var completedSuccess: Bool?
    var expirationHandler: (() -> Void)?

    init(identifier: String) {
        self.identifier = identifier
    }

    func setTaskCompleted(success: Bool) {
        completedSuccess = success
    }

    /// Simulate iOS firing the expiration handler.
    func simulateExpiration() {
        expirationHandler?()
    }
}

// MARK: - StubTaskScheduler

final class StubTaskScheduler: BackgroundTaskScheduling, @unchecked Sendable {
    var submittedRequests: [BGTaskRequest] = []
    var shouldThrowOnSubmit = false

    func submit(_ taskRequest: BGTaskRequest) throws {
        if shouldThrowOnSubmit {
            throw NSError(domain: "StubTaskScheduler", code: 1)
        }
        submittedRequests.append(taskRequest)
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
    /// long enough for the expiration handler to fire.
    var runPendingBackfillDuration: Duration?

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

    func startCapabilityObserver() async {
        startCapabilityObserverCallCount += 1
        if let duration = startCapabilityObserverDuration {
            try? await Task.sleep(for: duration)
        }
    }

    func stop() async {
        stopCallCount += 1
    }

    func runPendingBackfill() async {
        runPendingBackfillCallCount += 1
        if let duration = runPendingBackfillDuration {
            try? await Task.sleep(for: duration)
        }
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

    init(
        reachability: TransportSnapshot.Reachability = .wifi,
        allowsCellular: Bool = true
    ) {
        self.reachability = reachability
        self.allowsCellular = allowsCellular
    }

    func currentReachability() async -> TransportSnapshot.Reachability {
        reachability
    }
    func userAllowsCellular() async -> Bool { allowsCellular }
}
