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

    func setTaskCompleted(success: Bool) {
        completedSuccess = success
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
}

// MARK: - StubBatteryProvider

final class StubBatteryProvider: BatteryStateProviding, @unchecked Sendable {
    var level: Float = 1.0
    var charging: Bool = true

    func currentBatteryState() async -> (level: Float, isCharging: Bool) {
        (level, charging)
    }
}
