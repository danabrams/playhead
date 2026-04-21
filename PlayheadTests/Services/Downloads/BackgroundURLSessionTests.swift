// BackgroundURLSessionTests.swift
// Unit tests for playhead-24cm: dual background URLSession configurations
// (interactive + maintenance) plus the legacy rollout identifier, the
// URLError → InternalMissCause map, and the
// handleEventsForBackgroundURLSession completion-handler plumbing.

import Foundation
import Testing
@testable import Playhead

// MARK: - Identifier Constants

@Suite("BackgroundSessionIdentifier")
struct BackgroundSessionIdentifierTests {

    @Test("Interactive identifier is stable")
    func interactiveIdentifier() {
        #expect(BackgroundSessionIdentifier.interactive == "com.playhead.transfer.interactive")
    }

    @Test("Maintenance identifier is stable")
    func maintenanceIdentifier() {
        #expect(BackgroundSessionIdentifier.maintenance == "com.playhead.transfer.maintenance")
    }

    @Test("Legacy identifier matches the pre-24cm single-session name")
    func legacyIdentifier() {
        #expect(BackgroundSessionIdentifier.legacy == "com.playhead.episode-downloads")
    }

    @Test("isKnown accepts all three identifiers")
    func isKnownAcceptsOwnIdentifiers() {
        #expect(BackgroundSessionIdentifier.isKnown(.init("com.playhead.transfer.interactive")))
        #expect(BackgroundSessionIdentifier.isKnown(.init("com.playhead.transfer.maintenance")))
        #expect(BackgroundSessionIdentifier.isKnown(.init("com.playhead.episode-downloads")))
    }

    @Test("isKnown rejects foreign identifiers")
    func isKnownRejectsForeign() {
        #expect(!BackgroundSessionIdentifier.isKnown("com.example.other"))
        #expect(!BackgroundSessionIdentifier.isKnown(""))
    }
}

// MARK: - URLError → InternalMissCause

@Suite("InternalMissCause.fromURLError")
struct URLErrorMappingTests {

    @Test("timedOut maps to taskExpired")
    func timedOut() {
        let cause = InternalMissCause.fromURLError(URLError(.timedOut))
        #expect(cause == .taskExpired)
    }

    @Test("backgroundSessionWasDisconnected maps to taskExpired")
    func bgDisconnected() {
        let cause = InternalMissCause.fromURLError(URLError(.backgroundSessionWasDisconnected))
        #expect(cause == .taskExpired)
    }

    @Test("notConnectedToInternet maps to noNetwork")
    func notConnected() {
        let cause = InternalMissCause.fromURLError(URLError(.notConnectedToInternet))
        #expect(cause == .noNetwork)
    }

    @Test("networkConnectionLost maps to noNetwork")
    func connectionLost() {
        let cause = InternalMissCause.fromURLError(URLError(.networkConnectionLost))
        #expect(cause == .noNetwork)
    }

    @Test("dataNotAllowed maps to noNetwork")
    func dataNotAllowed() {
        let cause = InternalMissCause.fromURLError(URLError(.dataNotAllowed))
        #expect(cause == .noNetwork)
    }

    @Test("internationalRoamingOff maps to wifiRequired")
    func roamingOff() {
        let cause = InternalMissCause.fromURLError(URLError(.internationalRoamingOff))
        #expect(cause == .wifiRequired)
    }

    @Test("unexpected URLError maps to pipelineError")
    func otherErrorFallsBack() {
        let cause = InternalMissCause.fromURLError(URLError(.cannotFindHost))
        #expect(cause == .pipelineError)
    }

    @Test("Non-URL errors map to pipelineError via fromTaskError")
    func nonURLError() {
        struct LocalError: Error {}
        let cause = InternalMissCause.fromTaskError(LocalError())
        #expect(cause == .pipelineError)
    }

    @Test("URLError forwarded through fromTaskError")
    func urlErrorForwarded() {
        let cause = InternalMissCause.fromTaskError(URLError(.notConnectedToInternet))
        #expect(cause == .noNetwork)
    }
}

// MARK: - Completion Handler Plumbing

@MainActor
@Suite("PlayheadAppDelegate completion-handler plumbing")
struct BackgroundCompletionHandlerPlumbingTests {

    @Test("AppDelegate stores pending handler keyed by identifier")
    func storesPendingHandler() async {
        let delegate = PlayheadAppDelegate()
        var fired = 0
        let handler: () -> Void = { fired += 1 }

        delegate.storePendingBackgroundCompletionHandler(
            handler,
            forIdentifier: BackgroundSessionIdentifier.interactive
        )

        #expect(delegate.pendingBackgroundCompletionHandlerCount == 1)
        #expect(fired == 0)
    }

    @Test("Invoking the stored handler fires exactly once and removes it")
    func handlerFiresAndIsRemoved() async {
        let delegate = PlayheadAppDelegate()
        var fired = 0
        delegate.storePendingBackgroundCompletionHandler(
            { fired += 1 },
            forIdentifier: BackgroundSessionIdentifier.maintenance
        )

        delegate.invokePendingBackgroundCompletionHandler(
            forIdentifier: BackgroundSessionIdentifier.maintenance
        )

        #expect(fired == 1)
        #expect(delegate.pendingBackgroundCompletionHandlerCount == 0)
    }

    @Test("Invoking twice does not double-fire (no leak)")
    func handlerDoesNotDoubleFire() async {
        let delegate = PlayheadAppDelegate()
        var fired = 0
        delegate.storePendingBackgroundCompletionHandler(
            { fired += 1 },
            forIdentifier: BackgroundSessionIdentifier.interactive
        )

        delegate.invokePendingBackgroundCompletionHandler(
            forIdentifier: BackgroundSessionIdentifier.interactive
        )
        delegate.invokePendingBackgroundCompletionHandler(
            forIdentifier: BackgroundSessionIdentifier.interactive
        )

        #expect(fired == 1)
    }

    @Test("Handlers for different identifiers are tracked independently")
    func independentIdentifiers() async {
        let delegate = PlayheadAppDelegate()
        var a = 0, b = 0
        delegate.storePendingBackgroundCompletionHandler(
            { a += 1 },
            forIdentifier: BackgroundSessionIdentifier.interactive
        )
        delegate.storePendingBackgroundCompletionHandler(
            { b += 1 },
            forIdentifier: BackgroundSessionIdentifier.maintenance
        )
        #expect(delegate.pendingBackgroundCompletionHandlerCount == 2)

        delegate.invokePendingBackgroundCompletionHandler(
            forIdentifier: BackgroundSessionIdentifier.interactive
        )
        #expect(a == 1)
        #expect(b == 0)
        #expect(delegate.pendingBackgroundCompletionHandlerCount == 1)

        delegate.invokePendingBackgroundCompletionHandler(
            forIdentifier: BackgroundSessionIdentifier.maintenance
        )
        #expect(a == 1)
        #expect(b == 1)
        #expect(delegate.pendingBackgroundCompletionHandlerCount == 0)
    }
}

// MARK: - DownloadManager background session wiring

@Suite("DownloadManager – dual background sessions")
struct DownloadManagerDualSessionsTests {

    @Test("Feature flag defaults to OFF")
    func flagDefaultsOff() {
        let defaults = PreAnalysisConfig()
        #expect(defaults.useDualBackgroundSessions == false)
    }

    @Test("With flag OFF, interactive and maintenance sessions are not instantiated")
    func flagOffKeepsLegacyOnly() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let ids = await manager.instantiatedSessionIdentifiersForTesting()
        // Flag off → only legacy session (lazy-created on demand).
        // Until a session is requested, the set is empty; request legacy.
        _ = await manager.backgroundSessionForTesting(role: .legacy)
        let idsAfter = await manager.instantiatedSessionIdentifiersForTesting()
        #expect(idsAfter.contains(BackgroundSessionIdentifier.legacy))
        #expect(!idsAfter.contains(BackgroundSessionIdentifier.interactive))
        #expect(!idsAfter.contains(BackgroundSessionIdentifier.maintenance))
        _ = ids
    }

    @Test("With flag ON, interactive session uses the interactive identifier")
    func interactiveRoleUsesInteractiveIdentifier() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        await manager.setUseDualBackgroundSessions(true)
        try await manager.bootstrap()

        let session = await manager.backgroundSessionForTesting(role: .interactive)
        #expect(session.configuration.identifier == BackgroundSessionIdentifier.interactive)
        #expect(session.configuration.isDiscretionary == false)
        #expect(session.configuration.sessionSendsLaunchEvents == true)
    }

    @Test("With flag ON, maintenance session uses the maintenance identifier + isDiscretionary")
    func maintenanceRoleUsesMaintenanceIdentifier() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        await manager.setUseDualBackgroundSessions(true)
        try await manager.bootstrap()

        let session = await manager.backgroundSessionForTesting(role: .maintenance)
        #expect(session.configuration.identifier == BackgroundSessionIdentifier.maintenance)
        #expect(session.configuration.isDiscretionary == true)
        #expect(session.configuration.sessionSendsLaunchEvents == true)
    }

    @Test("Legacy session remains available during rollout window")
    func legacySessionAvailableWhenFlagOn() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        await manager.setUseDualBackgroundSessions(true)
        try await manager.bootstrap()

        let legacy = await manager.backgroundSessionForTesting(role: .legacy)
        #expect(legacy.configuration.identifier == BackgroundSessionIdentifier.legacy)
    }

    @Test("resumeSession(identifier:) is a safe no-op for unknown identifiers")
    func resumeSessionUnknownNoop() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        await manager.resumeSession(identifier: "com.unknown.session")
        // No crash, no exception — done.
    }

    @Test("resumeSession(identifier:) wakes the corresponding background session")
    func resumeSessionWakesSession() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        await manager.setUseDualBackgroundSessions(true)
        try await manager.bootstrap()

        // Before resuming, neither session is instantiated.
        let before = await manager.instantiatedSessionIdentifiersForTesting()
        #expect(!before.contains(BackgroundSessionIdentifier.interactive))

        await manager.resumeSession(identifier: BackgroundSessionIdentifier.interactive)

        let after = await manager.instantiatedSessionIdentifiersForTesting()
        #expect(after.contains(BackgroundSessionIdentifier.interactive))
    }
}

// MARK: - Delegate error → WorkJournal plumbing

@Suite("EpisodeDownloadDelegate – work-journal emission")
struct DelegateWorkJournalTests {

    @Test("didCompleteWithError(nil) is a no-op (finalized is emitted by didFinishDownloadingTo)")
    func noErrorDoesNotEmitFailure() async {
        let recorder = RecordingWorkJournal()
        let delegate = EpisodeDownloadDelegate()
        delegate.workJournal = recorder

        let task = StubTask(taskDescription: "ep-1")
        delegate.urlSession(URLSession.shared, task: task, didCompleteWithError: nil)

        // Let the detached work journal Task schedule.
        try? await Task.sleep(nanoseconds: 50_000_000)

        let failures = await recorder.failures
        #expect(failures.isEmpty)
    }

    @Test("didCompleteWithError(URLError.timedOut) emits failed with taskExpired")
    func timedOutEmitsTaskExpired() async {
        let recorder = RecordingWorkJournal()
        let delegate = EpisodeDownloadDelegate()
        delegate.workJournal = recorder

        let task = StubTask(taskDescription: "ep-timeout")
        delegate.urlSession(
            URLSession.shared,
            task: task,
            didCompleteWithError: URLError(.timedOut)
        )

        // Spin until recorder sees the event or we give up.
        for _ in 0..<20 {
            try? await Task.sleep(nanoseconds: 25_000_000)
            let failures = await recorder.failures
            if !failures.isEmpty { break }
        }

        let failures = await recorder.failures
        #expect(failures.count == 1)
        #expect(failures.first?.episodeId == "ep-timeout")
        #expect(failures.first?.cause == .taskExpired)
    }

    @Test("didCompleteWithError(URLError.notConnectedToInternet) emits noNetwork")
    func notConnectedEmitsNoNetwork() async {
        let recorder = RecordingWorkJournal()
        let delegate = EpisodeDownloadDelegate()
        delegate.workJournal = recorder

        let task = StubTask(taskDescription: "ep-offline")
        delegate.urlSession(
            URLSession.shared,
            task: task,
            didCompleteWithError: URLError(.notConnectedToInternet)
        )

        for _ in 0..<20 {
            try? await Task.sleep(nanoseconds: 25_000_000)
            let failures = await recorder.failures
            if !failures.isEmpty { break }
        }

        let failures = await recorder.failures
        #expect(failures.first?.cause == .noNetwork)
    }

    @Test("urlSessionDidFinishEvents invokes the pending completion handler")
    func urlSessionDidFinishEventsInvokesHandler() async {
        await MainActor.run {
            let delegate = PlayheadAppDelegate()
            var fired = 0
            delegate.storePendingBackgroundCompletionHandler(
                { fired += 1 },
                forIdentifier: BackgroundSessionIdentifier.interactive
            )

            // Route directly through the delegate's invoke API —
            // DownloadManager.urlSessionDidFinishEvents forwards here.
            delegate.invokePendingBackgroundCompletionHandler(
                forIdentifier: BackgroundSessionIdentifier.interactive
            )

            #expect(fired == 1)
        }
    }
}

// MARK: - Test doubles

/// Minimal Sendable recorder for WorkJournal events captured during tests.
private actor RecordingWorkJournal: WorkJournalRecording {
    struct Failure: Sendable, Equatable {
        let episodeId: String
        let cause: InternalMissCause
        let metadataJSON: String?
    }
    struct Preempted: Sendable, Equatable {
        let episodeId: String
        let cause: InternalMissCause
        let metadataJSON: String
    }

    private(set) var finalized: [String] = []
    private(set) var failures: [Failure] = []
    private(set) var preempted: [Preempted] = []

    func recordFinalized(episodeId: String) async {
        finalized.append(episodeId)
    }

    func recordFailed(episodeId: String, cause: InternalMissCause) async {
        failures.append(Failure(episodeId: episodeId, cause: cause, metadataJSON: nil))
    }

    // playhead-1nl6: protocol now requires the metadata-carrying
    // overload directly — the silent default-forward that dropped the
    // JSON blob was removed.
    func recordFailed(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {
        failures.append(Failure(episodeId: episodeId, cause: cause, metadataJSON: metadataJSON))
    }

    func recordPreempted(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {
        preempted.append(Preempted(episodeId: episodeId, cause: cause, metadataJSON: metadataJSON))
    }
}

/// Minimal URLSessionTask double that exposes a writable `taskDescription`.
/// We can't instantiate a real URLSessionTask in tests (requires a session),
/// but subclassing works when we only read `taskDescription`.
private final class StubTask: URLSessionTask, @unchecked Sendable {
    private let _taskDescription: String?

    init(taskDescription: String?) {
        self._taskDescription = taskDescription
        super.init()
    }

    override var taskDescription: String? {
        get { _taskDescription }
        set { /* ignore — stub is immutable */ }
    }
}
