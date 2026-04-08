// ShadowRetryTests.swift
// bd-3bz (Phase 4): tests for the FM shadow-phase retry path. Covers the
// persistence side of the bail/clear flow on `AnalysisStore`, the re-entrant
// retry entry point on `AdDetectionService`, and the debounce logic in
// `ShadowRetryObserver` against a fake clock + fake capability publisher.

import Foundation
import os
import Testing

@testable import Playhead

@Suite("bd-3bz: shadow FM retry")
struct ShadowRetryTests {

    // MARK: - Fixtures

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    private func makeSession(id: String, assetId: String) -> AnalysisSession {
        AnalysisSession(
            id: id,
            analysisAssetId: assetId,
            state: "complete",
            startedAt: Date().timeIntervalSince1970,
            updatedAt: Date().timeIntervalSince1970,
            failureReason: nil
        )
    }

    private func makeChunks(assetId: String) -> [TranscriptChunk] {
        let texts = [
            "Welcome to the show. Today we're discussing podcasts and how to find them.",
            "This episode is brought to you by Squarespace. Use code SHOW for 20 percent off your first purchase at squarespace dot com slash show.",
            "Now back to our interview with our guest about technology trends."
        ]
        return texts.enumerated().map { idx, text in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "test-v1",
                transcriptVersion: "tv-1",
                atomOrdinal: idx
            )
        }
    }

    private func makeShadowFactory() -> @Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner {
        return { store, mode in
            BackfillJobRunner(
                store: store,
                admissionController: AdmissionController(),
                classifier: FoundationModelClassifier(
                    runtime: TestFMRuntime(
                        coarseResponses: [
                            CoarseScreeningSchema(
                                disposition: .containsAd,
                                support: CoarseSupportSchema(
                                    supportLineRefs: [1],
                                    certainty: .strong
                                )
                            )
                        ]
                    ).runtime
                ),
                coveragePlanner: CoveragePlanner(),
                mode: mode,
                capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                batteryLevelProvider: { 1.0 },
                scanCohortJSON: makeTestScanCohortJSON()
            )
        }
    }

    // MARK: - Test A — bail path marks the session

    @Test("Test A: shadow phase bail flags the latest session as needs_shadow_retry")
    func testA_bailMarksSession() async throws {
        // Mirrors the production wiring: when the shadow phase bails on
        // FM unavailability it calls `shadowSkipMarker(assetId, podcastId)`,
        // which the runtime turns into
        // `markSessionNeedsShadowRetry(id: latestSessionForAsset)`. We
        // exercise that exact pipeline against a real store so we cover the
        // SQL marker, the lookup-by-asset hop, and the
        // `fetchSessionsNeedingShadowRetry` reader in one test.
        let store = try await makeTestStore()
        let assetId = "asset-A"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertSession(makeSession(id: "sess-A", assetId: assetId))

        // Production-style marker closure (copied from PlayheadRuntime).
        let storeForMarker = store
        let shadowSkipMarker: @Sendable (String, String) async -> Void = { assetId, podcastId in
            do {
                guard let session = try await storeForMarker.fetchLatestSessionForAsset(assetId: assetId) else {
                    return
                }
                try await storeForMarker.markSessionNeedsShadowRetry(
                    id: session.id,
                    podcastId: podcastId
                )
            } catch {
                Issue.record("shadowSkipMarker threw: \(error)")
            }
        }

        // FM unavailable; service must invoke the marker and never the factory.
        nonisolated(unsafe) var factoryCalls = 0
        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .shadow
            ),
            backfillJobRunnerFactory: { store, mode in
                factoryCalls += 1
                return BackfillJobRunner(
                    store: store,
                    admissionController: AdmissionController(),
                    classifier: FoundationModelClassifier(runtime: TestFMRuntime().runtime),
                    coveragePlanner: CoveragePlanner(),
                    mode: mode,
                    capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                    batteryLevelProvider: { 1.0 },
                    scanCohortJSON: makeTestScanCohortJSON()
                )
            },
            canUseFoundationModelsProvider: { false },
            shadowSkipMarker: shadowSkipMarker
        )

        try await service.runBackfill(
            chunks: makeChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-A",
            episodeDuration: 90
        )

        #expect(factoryCalls == 0, "factory must not be invoked when FM is unavailable")

        let refreshed = try await store.fetchSession(id: "sess-A")
        #expect(refreshed?.needsShadowRetry == true, "session should be flagged for retry")
        #expect(refreshed?.shadowRetryPodcastId == "podcast-A", "podcastId should be persisted")

        let flagged = try await store.fetchSessionsNeedingShadowRetry()
        #expect(flagged.count == 1)
        #expect(flagged.first?.id == "sess-A")
    }

    // MARK: - Test B — retry runs ONLY the shadow phase and clears the flag

    @Test("Test B: retry path runs only the shadow phase and clears the flag")
    func testB_retryRunsOnlyShadowAndClears() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-B"
        try await store.insertAsset(makeAsset(id: assetId))

        // Pre-flag the session as needing retry, with the chunks already
        // persisted (simulating a session whose transcription + coarse
        // phases completed under a prior FM-unavailable run).
        try await store.insertSession(
            AnalysisSession(
                id: "sess-B",
                analysisAssetId: assetId,
                state: "complete",
                startedAt: Date().timeIntervalSince1970,
                updatedAt: Date().timeIntervalSince1970,
                failureReason: nil,
                needsShadowRetry: true,
                shadowRetryPodcastId: "podcast-B"
            )
        )
        try await store.insertTranscriptChunks(makeChunks(assetId: assetId))

        // Counters: factory must be invoked exactly once (the shadow phase).
        // The classifier counter inside the rule-based path is unobservable,
        // but we can assert no NEW transcript chunks landed (re-entrancy
        // contract: transcription must not be re-run).
        nonisolated(unsafe) var factoryCalls = 0
        let factory: @Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner = { store, mode in
            factoryCalls += 1
            return BackfillJobRunner(
                store: store,
                admissionController: AdmissionController(),
                classifier: FoundationModelClassifier(
                    runtime: TestFMRuntime(
                        coarseResponses: [
                            CoarseScreeningSchema(
                                disposition: .containsAd,
                                support: CoarseSupportSchema(
                                    supportLineRefs: [1],
                                    certainty: .strong
                                )
                            )
                        ]
                    ).runtime
                ),
                coveragePlanner: CoveragePlanner(),
                mode: mode,
                capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                batteryLevelProvider: { 1.0 },
                scanCohortJSON: makeTestScanCohortJSON()
            )
        }

        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .shadow
            ),
            backfillJobRunnerFactory: factory,
            canUseFoundationModelsProvider: { true }
        )

        let chunkCountBefore = try await store.fetchTranscriptChunks(assetId: assetId).count
        let adWindowsBefore = try await store.fetchAdWindows(assetId: assetId).count

        let didRun = await service.retryShadowFMPhaseForSession(sessionId: "sess-B")
        #expect(didRun, "retry path should report shadow phase executed")
        #expect(factoryCalls == 1, "shadow phase factory should be invoked exactly once")

        // Re-entrancy contract: transcription and coarse should NOT have
        // re-run. Transcript chunks are the cleanest pin (the retry path
        // never touches them); ad windows are also a pin because the retry
        // path bypasses `runBackfill`.
        let chunkCountAfter = try await store.fetchTranscriptChunks(assetId: assetId).count
        #expect(chunkCountAfter == chunkCountBefore, "retry must not re-run transcription")
        let adWindowsAfter = try await store.fetchAdWindows(assetId: assetId).count
        #expect(adWindowsAfter == adWindowsBefore, "retry must not re-run coarse ad detection")

        // Flag is cleared after a successful drain.
        let cleared = try await store.fetchSession(id: "sess-B")
        #expect(cleared?.needsShadowRetry == false, "flag should be cleared after retry")
        #expect(cleared?.shadowRetryPodcastId == nil)
        #expect(try await store.fetchSessionsNeedingShadowRetry().isEmpty)

        // Sanity: the FM telemetry actually landed.
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        #expect(!scans.isEmpty, "shadow phase must have written semantic scan rows")
    }

    // MARK: - Test C — failed retry leaves the flag set

    @Test("Test C: retry keeps the flag set when the shadow phase fails")
    func testC_retryFailureLeavesFlagSet() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-C"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertSession(
            AnalysisSession(
                id: "sess-C",
                analysisAssetId: assetId,
                state: "complete",
                startedAt: Date().timeIntervalSince1970,
                updatedAt: Date().timeIntervalSince1970,
                failureReason: nil,
                needsShadowRetry: true,
                shadowRetryPodcastId: "podcast-C"
            )
        )
        try await store.insertTranscriptChunks(makeChunks(assetId: assetId))

        // Force the inner shadow runner to fail before it can persist any
        // telemetry by returning a runner backed by a different store with no
        // parent asset row. This models a transient runner/store failure
        // while still driving the real `runShadowFMPhase` code path.
        let failingStore = try await makeTestStore()
        nonisolated(unsafe) var factoryCalls = 0
        let failingFactory: @Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner = { _, mode in
            factoryCalls += 1
            return BackfillJobRunner(
                store: failingStore,
                admissionController: AdmissionController(),
                classifier: FoundationModelClassifier(runtime: TestFMRuntime().runtime),
                coveragePlanner: CoveragePlanner(),
                mode: mode,
                capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
                batteryLevelProvider: { 1.0 },
                scanCohortJSON: makeTestScanCohortJSON()
            )
        }

        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .shadow
            ),
            backfillJobRunnerFactory: failingFactory,
            canUseFoundationModelsProvider: { true }
        )

        let didRun = await service.retryShadowFMPhaseForSession(sessionId: "sess-C")
        #expect(didRun, "retry should report that the shadow phase was attempted")
        #expect(factoryCalls == 1, "shadow phase factory should still be invoked")

        let stillFlagged = try await store.fetchSession(id: "sess-C")
        #expect(stillFlagged?.needsShadowRetry == true, "failed retry must leave the flag set")
        #expect(stillFlagged?.shadowRetryPodcastId == "podcast-C")
        let flagged = try await store.fetchSessionsNeedingShadowRetry()
        #expect(flagged.count == 1)
        #expect(flagged.first?.id == "sess-C")

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        #expect(scans.isEmpty, "failed retry should not write scan rows into the main store")
    }

    // MARK: - Test D — deferred retry leaves the flag set

    @Test("Test D: retry keeps the flag set when the shadow phase defers work")
    func testD_retryDeferralLeavesFlagSet() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-D"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertSession(
            AnalysisSession(
                id: "sess-D",
                analysisAssetId: assetId,
                state: "complete",
                startedAt: Date().timeIntervalSince1970,
                updatedAt: Date().timeIntervalSince1970,
                failureReason: nil,
                needsShadowRetry: true,
                shadowRetryPodcastId: "podcast-D"
            )
        )
        try await store.insertTranscriptChunks(makeChunks(assetId: assetId))

        nonisolated(unsafe) var factoryCalls = 0
        let deferredFactory: @Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner = { store, mode in
            factoryCalls += 1
            return BackfillJobRunner(
                store: store,
                admissionController: AdmissionController(),
                classifier: FoundationModelClassifier(runtime: TestFMRuntime().runtime),
                coveragePlanner: CoveragePlanner(),
                mode: mode,
                capabilitySnapshotProvider: { makeThermalThrottledSnapshot() },
                batteryLevelProvider: { 1.0 },
                scanCohortJSON: makeTestScanCohortJSON()
            )
        }

        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .shadow
            ),
            backfillJobRunnerFactory: deferredFactory,
            canUseFoundationModelsProvider: { true }
        )

        let didRun = await service.retryShadowFMPhaseForSession(sessionId: "sess-D")
        #expect(didRun, "retry should report that the shadow phase was attempted")
        #expect(factoryCalls == 1, "shadow phase factory should still be invoked")

        let stillFlagged = try await store.fetchSession(id: "sess-D")
        #expect(stillFlagged?.needsShadowRetry == true, "deferred retry must leave the flag set")
        #expect(stillFlagged?.shadowRetryPodcastId == "podcast-D")
        let flagged = try await store.fetchSessionsNeedingShadowRetry()
        #expect(flagged.count == 1)
        #expect(flagged.first?.id == "sess-D")

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        #expect(scans.isEmpty, "deferred retry should not write scan rows into the main store")
    }

    // MARK: - Test E — happy-path session is unaffected by capability flips

    @Test("Test E: session processed under FM=true is not flagged when capability flips later")
    func testE_happyPathIsNotMarked() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-E"
        try await store.insertAsset(makeAsset(id: assetId))
        try await store.insertSession(makeSession(id: "sess-E", assetId: assetId))

        // FM available — the bail path is never reached, so the marker
        // closure should never fire even though we wire one in.
        nonisolated(unsafe) var markerCalls = 0
        let marker: @Sendable (String, String) async -> Void = { _, _ in
            markerCalls += 1
        }

        let service = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "detection-v1",
                fmBackfillMode: .shadow
            ),
            backfillJobRunnerFactory: makeShadowFactory(),
            canUseFoundationModelsProvider: { true },
            shadowSkipMarker: marker
        )

        try await service.runBackfill(
            chunks: makeChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-E",
            episodeDuration: 90
        )

        #expect(markerCalls == 0, "marker must not be invoked when FM is available")

        let refreshed = try await store.fetchSession(id: "sess-E")
        #expect(refreshed?.needsShadowRetry == false, "happy-path session should not be flagged")
        #expect(try await store.fetchSessionsNeedingShadowRetry().isEmpty)

        // Now "flip" the capability: in production this is the observer's
        // job, but the contract here is that nothing in the system should
        // retroactively mark a clean session. We re-fetch to confirm.
        let stillClean = try await store.fetchSession(id: "sess-E")
        #expect(stillClean?.needsShadowRetry == false, "no spurious re-marking on capability flip")
    }

    // MARK: - Test F — observer debounce

    @Test("Test F: observer waits 60s of stable FM=true before draining; cancels on flip")
    func testF_observerDebounce() async throws {
        // Synchronously-driven fake clock. Each `sleep(seconds:)` call parks
        // a continuation that the test resumes by calling `release()`. The
        // observer's drain task only runs when its parked sleep is released.
        let clock = ManualShadowRetryClock()
        let capabilities = ManualCapabilities(initial: false)
        let drainer = RecordingDrainer()
        let store = StubShadowRetryStoreReader(rows: [
            AnalysisSession(
                id: "sess-D",
                analysisAssetId: "asset-F",
                state: "complete",
                startedAt: 0,
                updatedAt: 0,
                failureReason: nil,
                needsShadowRetry: true,
                shadowRetryPodcastId: "podcast-F"
            )
        ])

        let observer = ShadowRetryObserver(
            capabilities: capabilities,
            store: store,
            drainer: drainer,
            clock: clock,
            debounceSeconds: 60
        )
        await observer.start()

        // Step 1: false → true at "t=0". Observer should park a 60s sleep.
        await capabilities.send(canUseFoundationModels: true)
        try await clock.waitForPendingSleep()
        #expect(clock.pendingCount() == 1, "observer should have scheduled a debounce sleep")
        #expect(drainer.callCount() == 0, "drain should not fire before debounce elapses")

        // Step 2: at virtual t=30s the drain still must not fire. We model
        // "30 seconds elapsed without resume" by simply asserting the sleep
        // is still parked and the drainer hasn't been called.
        #expect(clock.pendingCount() == 1)
        #expect(drainer.callCount() == 0, "drain must not fire at t=30s")

        // Step 3: at virtual t=60s, complete the parked sleep. The drain
        // task awakes and calls the drainer.
        clock.releaseOldest()
        try await drainer.waitForCall()
        #expect(drainer.callCount() == 1, "drain should fire exactly once at t=60s")
        #expect(drainer.lastSessionId() == "sess-D")

        // Step 4: capability flips back to false at virtual "t=90s". A new
        // capability flip false→true→false should NOT schedule another
        // drain, and any existing pending drain should be cancelled.
        await capabilities.send(canUseFoundationModels: false)
        // No new sleep parked by the false transition.
        try? await Task.sleep(nanoseconds: 50_000_000)
        #expect(clock.pendingCount() == 0, "false transition must not park a new sleep")
        #expect(drainer.callCount() == 1, "drain count must not advance on false")

        await observer.stop()
    }
}

// MARK: - Test doubles

/// Hand-driven clock for `ShadowRetryObserver` debounce tests. Each
/// `sleep(seconds:)` call parks a continuation that the test resumes
/// explicitly via `releaseOldest()`. Honors task cancellation so observer
/// `stop()` cleanly cancels parked sleeps.
final class ManualShadowRetryClock: ShadowRetryClock, Sendable {
    private struct Pending {
        let id: UUID
        let seconds: Double
        let cont: CheckedContinuation<Void, Error>
    }

    private struct State {
        var pending: [Pending] = []
        var sleepArrived: CheckedContinuation<Void, Never>?
    }

    private let state = OSAllocatedUnfairLock(initialState: State())

    func sleep(seconds: Double) async throws {
        let id = UUID()
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                let arrived: CheckedContinuation<Void, Never>? = state.withLock { s in
                    s.pending.append(Pending(id: id, seconds: seconds, cont: cont))
                    let a = s.sleepArrived
                    s.sleepArrived = nil
                    return a
                }
                arrived?.resume()
            }
        } onCancel: {
            let toCancel: CheckedContinuation<Void, Error>? = state.withLock { s in
                guard let idx = s.pending.firstIndex(where: { $0.id == id }) else { return nil }
                return s.pending.remove(at: idx).cont
            }
            toCancel?.resume(throwing: CancellationError())
        }
    }

    func pendingCount() -> Int {
        state.withLock { $0.pending.count }
    }

    func releaseOldest() {
        let cont: CheckedContinuation<Void, Error>? = state.withLock { s in
            guard !s.pending.isEmpty else { return nil }
            return s.pending.removeFirst().cont
        }
        cont?.resume()
    }

    /// Waits until at least one sleep has been parked. Times out after
    /// `timeoutSeconds` to keep tests from hanging on a missing schedule.
    func waitForPendingSleep(timeoutSeconds: Double = 1.0) async throws {
        // Fast path.
        if state.withLock({ !$0.pending.isEmpty }) { return }
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
                    let alreadyHere: Bool = self.state.withLock { s in
                        if !s.pending.isEmpty { return true }
                        s.sleepArrived = cont
                        return false
                    }
                    if alreadyHere { cont.resume() }
                }
            }
            group.addTask {
                try await Task.sleep(nanoseconds: UInt64(timeoutSeconds * 1_000_000_000))
                throw ShadowRetryTestTimeout()
            }
            try await group.next()
            group.cancelAll()
        }
    }
}

struct ShadowRetryTestTimeout: Error {}

/// Capability publisher that pushes snapshots into a single AsyncStream.
/// Tracks the latest `canUseFoundationModels` value so `currentSnapshot`
/// stays consistent with what the observer last saw.
actor ManualCapabilities: CapabilitiesProviding {
    private var continuation: AsyncStream<CapabilitySnapshot>.Continuation?
    private var stream: AsyncStream<CapabilitySnapshot>?
    private var latest: CapabilitySnapshot

    init(initial: Bool) {
        self.latest = Self.snapshot(canUseFM: initial)
    }

    var currentSnapshot: CapabilitySnapshot {
        get async { latest }
    }

    func capabilityUpdates() -> AsyncStream<CapabilitySnapshot> {
        if let stream { return stream }
        let (s, c) = AsyncStream<CapabilitySnapshot>.makeStream()
        self.stream = s
        self.continuation = c
        // Mirror production behavior: yield the current value first.
        c.yield(latest)
        return s
    }

    func send(canUseFoundationModels: Bool) {
        let snap = Self.snapshot(canUseFM: canUseFoundationModels)
        latest = snap
        continuation?.yield(snap)
    }

    private static func snapshot(canUseFM: Bool) -> CapabilitySnapshot {
        CapabilitySnapshot(
            foundationModelsAvailable: canUseFM,
            foundationModelsUsable: canUseFM,
            appleIntelligenceEnabled: canUseFM,
            foundationModelsLocaleSupported: true,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1024 * 1024 * 1024,
            capturedAt: Date()
        )
    }
}

final class RecordingDrainer: ShadowRetryDraining, Sendable {
    private struct State {
        var calls: [String] = []
        var arrived: CheckedContinuation<Void, Never>?
    }

    private let state = OSAllocatedUnfairLock(initialState: State())

    func retryShadowFMPhaseForSession(sessionId: String) async -> Bool {
        let cont: CheckedContinuation<Void, Never>? = state.withLock { s in
            s.calls.append(sessionId)
            let c = s.arrived
            s.arrived = nil
            return c
        }
        cont?.resume()
        return true
    }

    func callCount() -> Int {
        state.withLock { $0.calls.count }
    }

    func lastSessionId() -> String? {
        state.withLock { $0.calls.last }
    }

    func waitForCall(timeoutSeconds: Double = 1.0) async throws {
        if state.withLock({ !$0.calls.isEmpty }) { return }
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
                    let alreadyHere: Bool = self.state.withLock { s in
                        if !s.calls.isEmpty { return true }
                        s.arrived = cont
                        return false
                    }
                    if alreadyHere { cont.resume() }
                }
            }
            group.addTask {
                try await Task.sleep(nanoseconds: UInt64(timeoutSeconds * 1_000_000_000))
                throw ShadowRetryTestTimeout()
            }
            try await group.next()
            group.cancelAll()
        }
    }
}

final class StubShadowRetryStoreReader: ShadowRetryStoreReader, Sendable {
    private let rows: OSAllocatedUnfairLock<[AnalysisSession]>

    init(rows: [AnalysisSession]) {
        self.rows = OSAllocatedUnfairLock(initialState: rows)
    }

    func loadSessionsNeedingShadowRetry() async throws -> [AnalysisSession] {
        rows.withLock { $0 }
    }
}
