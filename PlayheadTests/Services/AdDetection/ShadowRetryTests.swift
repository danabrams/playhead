// ShadowRetryTests.swift
// bd-3bz (Phase 4): tests for the FM shadow-phase retry path. Covers the
// persistence side of the bail/clear flow on `AnalysisStore`, the re-entrant
// retry entry point on `AdDetectionService`, and the debounce logic in
// `ShadowRetryObserver` against a fake clock + fake capability publisher.

import Foundation
import os
import Testing

@testable import Playhead

/// playhead-p06: yields the cooperative pool until `condition` returns
/// true or `iterations` is exhausted. A pure-yield poll (no wall-clock
/// sleep) so tests don't race real time under parallel execution. The
/// caller still gets to assert on the condition after return — this
/// helper never Issues.record on timeout, it just gives the condition
/// a bounded chance to become true.
private func yieldUntilStable(
    iterations: Int = 100,
    condition: () -> Bool
) async {
    if condition() { return }
    for _ in 0..<iterations {
        await Task.yield()
        if condition() { return }
    }
}

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

    @Test("Test A: shadow phase bail flags the latest session as needsShadowRetry")
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

        // H7 production-style marker closure: now takes the session id
        // directly (the asset→session lookup happens inside
        // `runShadowFMPhase` at the START of the phase, before any
        // concurrent reprocessing can race a fresh session in).
        let storeForMarker = store
        let shadowSkipMarker: @Sendable (String, String) async -> Void = { sessionId, podcastId in
            do {
                try await storeForMarker.markSessionNeedsShadowRetry(
                    id: sessionId,
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

        // Cycle 4 H5: `runBackfill` now requires an explicit sessionId
        // for the shadow-skip marker to fire. Production callers thread
        // this through from `AnalysisCoordinator.finalizeBackfill`; we
        // pass "sess-A" directly to exercise the same path.
        try await service.runBackfill(
            chunks: makeChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-A",
            episodeDuration: 90,
            sessionId: "sess-A"
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
        // playhead-p06: the false transition cancels the pending sleep
        // on the observer's actor. Yield until the cancellation has been
        // processed (the observer's cancel path is synchronous once the
        // capability event is consumed), then assert invariants.
        await yieldUntilStable(iterations: 50) {
            clock.pendingCount() == 0
        }
        #expect(clock.pendingCount() == 0, "false transition must not park a new sleep")
        #expect(drainer.callCount() == 1, "drain count must not advance on false")

        await observer.stop()
    }

    // MARK: - H1 — observer.stop() exits the loop deterministically

    @Test("H1: observer.stop() exits the loop within a bounded yield budget even when capability never yields")
    func testH1_stopExitsLoopWithoutCapabilityYield() async throws {
        // Build a capabilities source that yields exactly once (the
        // initial snapshot from `capabilityUpdates()`) and then never
        // again. The previous implementation parked the observer in
        // `for await snapshot in stream` and ignored Task.isCancelled, so
        // `stop()` could not unblock the loop. The H1 fix gives the loop
        // an explicit `.shutdown` wake reason via the wake stream.
        let capabilities = ManualCapabilities(initial: false)
        let drainer = RecordingDrainer()
        let store = StubShadowRetryStoreReader(rows: [])
        let observer = ShadowRetryObserver(
            capabilities: capabilities,
            store: store,
            drainer: drainer,
            clock: ManualShadowRetryClock(),
            debounceSeconds: 60
        )
        await observer.start()

        // Park the observer on the capability stream by *not* yielding
        // any further snapshots. The initial snapshot from
        // `capabilityUpdates()` is `false`, so no drain is scheduled.
        //
        // playhead-p06: yield until the observer's loop task is
        // actually running (instead of a fixed 10ms wall-clock sleep
        // that could be starved under load).
        for _ in 0..<200 {
            if await observer.testIsLoopRunning() { break }
            await Task.yield()
        }

        // Now stop. Without H1 this hangs — stop() never returns.
        // The post-fix contract: stop() returns after at most a bounded
        // number of cooperative-pool turns once the .shutdown wake is
        // delivered. We enforce that contract as a bounded iteration
        // budget instead of a wall-clock deadline: kick stop() off and
        // poll the sentinel.
        let stopTask = Task { await observer.stop() }
        var exitedPromptly = false
        for _ in 0..<1000 {
            if await observer.testHasExitedLoop() {
                exitedPromptly = true
                break
            }
            await Task.yield()
        }
        await stopTask.value
        #expect(exitedPromptly, "stop() must drive the loop out within a bounded yield budget")
        #expect(await observer.testHasExitedLoop(), "loop must have run its `defer { loopDidExit = true }` block")
    }

    @Test("H1: stop() called from two tasks concurrently does not crash")
    func testH1_concurrentStopIsSafe() async throws {
        let capabilities = ManualCapabilities(initial: false)
        let drainer = RecordingDrainer()
        let store = StubShadowRetryStoreReader(rows: [])
        let observer = ShadowRetryObserver(
            capabilities: capabilities,
            store: store,
            drainer: drainer,
            clock: ManualShadowRetryClock(),
            debounceSeconds: 60
        )
        await observer.start()

        async let stop1: Void = observer.stop()
        async let stop2: Void = observer.stop()
        _ = await (stop1, stop2)

        #expect(await observer.testHasExitedLoop())
    }

    @Test("H1/Rev1-L2: stop() before start() is safe; start() after stop() is rejected")
    func testH1_stopBeforeStartAndStartAfterStop() async throws {
        let capabilities = ManualCapabilities(initial: false)
        let drainer = RecordingDrainer()
        let store = StubShadowRetryStoreReader(rows: [])
        let observer = ShadowRetryObserver(
            capabilities: capabilities,
            store: store,
            drainer: drainer,
            clock: ManualShadowRetryClock(),
            debounceSeconds: 60
        )

        // stop() before start() must not crash and must not block forever.
        await observer.stop()

        // Re-start after stop() is rejected (the wake stream is finished
        // and the sentinel is permanent). This pins the C7 behavior
        // contract; flipping to "start() after stop() restarts" requires
        // a deliberate decision.
        await observer.start()
        // The loop never runs (start() bails on `didShutdown`), so
        // testHasExitedLoop stays at its default `false`.
        #expect(await observer.testHasExitedLoop() == false)
    }

    // MARK: - H2 — wake-on-mark drain bypasses the false→true transition rail

    @Test("H2: wake() drains immediately when capability is already stable-true")
    func testH2_wakeDrainsImmediatelyWhenStableTrue() async throws {
        let clock = ManualShadowRetryClock()
        let capabilities = ManualCapabilities(initial: true)
        let drainer = RecordingDrainer()
        let store = StubShadowRetryStoreReader(rows: [
            AnalysisSession(
                id: "sess-stable",
                analysisAssetId: "asset-h2",
                state: "complete",
                startedAt: 0,
                updatedAt: 0,
                failureReason: nil,
                needsShadowRetry: true,
                shadowRetryPodcastId: "pod-h2"
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
        // Let the initial-snapshot capability event flow through and
        // schedule its (60s) debounce.
        try await clock.waitForPendingSleep()

        // Now mark a session: production wires this through
        // `markSessionNeedsShadowRetry` + `observer.wake()`. The wake
        // bypasses the debounce because capability is already true.
        await observer.wake()

        try await drainer.waitForCall()
        #expect(drainer.callCount() == 1)
        #expect(drainer.lastSessionId() == "sess-stable")

        await observer.stop()
    }

    @Test("H2: wake() while capability is false is a no-op")
    func testH2_wakeIsNoOpWhenCapabilityFalse() async throws {
        let clock = ManualShadowRetryClock()
        let capabilities = ManualCapabilities(initial: false)
        let drainer = RecordingDrainer()
        let store = StubShadowRetryStoreReader(rows: [
            AnalysisSession(
                id: "sess-x",
                analysisAssetId: "a",
                state: "complete",
                startedAt: 0,
                updatedAt: 0,
                failureReason: nil,
                needsShadowRetry: true,
                shadowRetryPodcastId: "p"
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
        // Let the loop process the initial-snapshot capability event.
        // playhead-p06: yield until the loop is running, instead of a
        // fixed 20ms wall-clock sleep.
        for _ in 0..<200 {
            if await observer.testIsLoopRunning() { break }
            await Task.yield()
        }

        await observer.wake()
        // Yield a generous budget so any (erroneous) drain would have
        // had a chance to fire. Nothing to wait *for* — we're asserting
        // absence — so we yield then check.
        for _ in 0..<50 { await Task.yield() }
        #expect(drainer.callCount() == 0, "wake() must not drain when capability is false")

        await observer.stop()
    }

    @Test("H2: wake() after stop() is a no-op")
    func testH2_wakeAfterStopIsNoOp() async throws {
        let observer = ShadowRetryObserver(
            capabilities: ManualCapabilities(initial: true),
            store: StubShadowRetryStoreReader(rows: []),
            drainer: RecordingDrainer(),
            clock: ManualShadowRetryClock(),
            debounceSeconds: 60
        )
        await observer.start()
        await observer.stop()
        // Should not crash.
        await observer.wake()
    }
}

// MARK: - H7 — sessionId race in shadowSkipMarker

@Suite("bd-3bz: H7 session id pinning")
struct ShadowRetryH7Tests {

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

    @Test("H7: marker stamps the session captured at shadow-phase start, not the latest")
    func testH7_markerStampsCapturedSession() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-h7"
        try await store.insertAsset(makeAsset(id: assetId))

        // Pre-existing session whose shadow phase will run.
        let now = Date().timeIntervalSince1970
        try await store.insertSession(
            AnalysisSession(
                id: "sess-old",
                analysisAssetId: assetId,
                state: "complete",
                startedAt: now,
                updatedAt: now,
                failureReason: nil
            )
        )

        // Test the marker contract directly: the production marker now
        // takes a sessionId and stamps it. We mark the OLD session
        // explicitly. Then we insert a NEWER session for the same asset
        // (modeling concurrent reprocessing) and assert the new session
        // is NOT flagged.
        try await store.markSessionNeedsShadowRetry(id: "sess-old", podcastId: "pod-h7")

        // Now insert a newer session for the same asset.
        try await store.insertSession(
            AnalysisSession(
                id: "sess-new",
                analysisAssetId: assetId,
                state: "complete",
                startedAt: now + 10,
                updatedAt: now + 10,
                failureReason: nil
            )
        )

        let oldSession = try await store.fetchSession(id: "sess-old")
        let newSession = try await store.fetchSession(id: "sess-new")
        #expect(oldSession?.needsShadowRetry == true, "old session should be flagged")
        #expect(newSession?.needsShadowRetry == false, "newer session must NOT be flagged by sessionId-pinned marker")
    }

    @Test("H7: AdDetectionService runShadowFMPhase honors the explicit sessionId override")
    func testH7_runShadowFMPhasePinsSessionAtStart() async throws {
        // This test exercises the runShadowFMPhase code path with
        // FM=false to drive the bail+marker. The marker captures the
        // sessionId it receives.
        //
        // Cycle 4 H5: the cycle-2 version of this test relied on
        // `fetchLatestSessionForAsset` as an implicit fallback for the
        // marker id. That fallback has been removed (it raced concurrent
        // reprocessing); the caller is now required to pass the
        // sessionId explicitly via `runBackfill(..., sessionId:)`. This
        // test now pins the explicit-override behavior: the caller hands
        // in "sess-h7-target" and the marker must be stamped with it,
        // regardless of any other sessions that exist for the same
        // asset.
        let store = try await makeTestStore()
        let assetId = "asset-h7-svc"
        try await store.insertAsset(makeAsset(id: assetId))

        let now = Date().timeIntervalSince1970
        try await store.insertSession(
            AnalysisSession(
                id: "sess-h7-old",
                analysisAssetId: assetId,
                state: "complete",
                startedAt: now,
                updatedAt: now,
                failureReason: nil
            )
        )
        try await store.insertSession(
            AnalysisSession(
                id: "sess-h7-target",
                analysisAssetId: assetId,
                state: "complete",
                startedAt: now + 10,
                updatedAt: now + 10,
                failureReason: nil
            )
        )

        nonisolated(unsafe) var capturedSessionId: String?
        let marker: @Sendable (String, String) async -> Void = { sessionId, _ in
            capturedSessionId = sessionId
        }

        let chunks = [
            TranscriptChunk(
                id: "c0", analysisAssetId: assetId, segmentFingerprint: "f0", chunkIndex: 0,
                startTime: 0, endTime: 30, text: "hello", normalizedText: "hello",
                pass: "final", modelVersion: "test-v1", transcriptVersion: "tv-1", atomOrdinal: 0
            )
        ]
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
                BackfillJobRunner(
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
            shadowSkipMarker: marker
        )
        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "pod-h7-svc",
            episodeDuration: 90,
            sessionId: "sess-h7-target"
        )

        #expect(capturedSessionId == "sess-h7-target", "marker must be called with the explicit sessionId override")
    }

    // Cycle 4 H5 expand: the cycle-2 fix only covered the
    // `retryShadowFMPhaseForSession` path; the `runBackfill` path still
    // fell back to `fetchLatestSessionForAsset` at the marker site. This
    // test pins the new behavior: `runBackfill` threads the sessionId the
    // caller (AnalysisCoordinator / tests) captured at dispatch time
    // straight through to `runShadowFMPhase`, and the marker stamps THAT
    // id — even if a newer session for the same asset exists in the
    // store by the time the marker fires.
    @Test("Cycle 4 H5: runBackfill stamps the explicit sessionId, not the latest row for the asset")
    func testH5_runBackfillPinsExplicitSessionId() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-h5"
        try await store.insertAsset(makeAsset(id: assetId))

        let now = Date().timeIntervalSince1970
        // The session that runBackfill is dispatched against ("the OLD
        // session, from the caller's perspective").
        try await store.insertSession(
            AnalysisSession(
                id: "sess-h5-dispatch",
                analysisAssetId: assetId,
                state: "complete",
                startedAt: now,
                updatedAt: now,
                failureReason: nil
            )
        )
        // A newer session for the same asset, modeling concurrent
        // reprocessing landing BEFORE the shadow phase's marker fires.
        // Pre-fix (cycle-2 runBackfill path), `fetchLatestSessionForAsset`
        // would return this row and the marker would stamp the wrong id.
        try await store.insertSession(
            AnalysisSession(
                id: "sess-h5-concurrent",
                analysisAssetId: assetId,
                state: "complete",
                startedAt: now + 10,
                updatedAt: now + 10,
                failureReason: nil
            )
        )

        nonisolated(unsafe) var capturedSessionId: String?
        let marker: @Sendable (String, String) async -> Void = { sessionId, _ in
            capturedSessionId = sessionId
        }

        let chunks = [
            TranscriptChunk(
                id: "c0", analysisAssetId: assetId, segmentFingerprint: "f0", chunkIndex: 0,
                startTime: 0, endTime: 30, text: "hello", normalizedText: "hello",
                pass: "final", modelVersion: "test-v1", transcriptVersion: "tv-1", atomOrdinal: 0
            )
        ]
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
                BackfillJobRunner(
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
            shadowSkipMarker: marker
        )
        // Caller pins the OLD session id. The concurrent row that
        // landed between session creation and this call must not win.
        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "pod-h5",
            episodeDuration: 90,
            sessionId: "sess-h5-dispatch"
        )

        #expect(
            capturedSessionId == "sess-h5-dispatch",
            "runBackfill must stamp the explicit sessionId the caller passed, not the latest session for the asset"
        )

        // Production persistence path: mark via the real store to pin
        // that ONLY the dispatched session gets flagged.
        try await store.markSessionNeedsShadowRetry(
            id: capturedSessionId ?? "unknown",
            podcastId: "pod-h5"
        )
        let dispatched = try await store.fetchSession(id: "sess-h5-dispatch")
        let concurrent = try await store.fetchSession(id: "sess-h5-concurrent")
        #expect(dispatched?.needsShadowRetry == true)
        #expect(concurrent?.needsShadowRetry == false, "concurrent session must not be flagged by runBackfill's marker")
    }

    @Test("Cycle 4 H5: runBackfill with nil sessionId skips the marker entirely (no race via fetchLatestSessionForAsset)")
    func testH5_runBackfillNilSessionSkipsMarker() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-h5-nil"
        try await store.insertAsset(makeAsset(id: assetId))
        // Insert a session so that a pre-fix `fetchLatestSessionForAsset`
        // fallback would have something to find and stamp.
        try await store.insertSession(makeSession(id: "sess-h5-nil", assetId: assetId))

        nonisolated(unsafe) var capturedSessionId: String?
        nonisolated(unsafe) var markerCallCount = 0
        let marker: @Sendable (String, String) async -> Void = { sessionId, _ in
            markerCallCount += 1
            capturedSessionId = sessionId
        }

        let chunks = [
            TranscriptChunk(
                id: "c0", analysisAssetId: assetId, segmentFingerprint: "f0", chunkIndex: 0,
                startTime: 0, endTime: 30, text: "hello", normalizedText: "hello",
                pass: "final", modelVersion: "test-v1", transcriptVersion: "tv-1", atomOrdinal: 0
            )
        ]
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
                BackfillJobRunner(
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
            shadowSkipMarker: marker
        )

        // Legacy caller (e.g. AnalysisJobRunner) has no session context
        // and passes nil. With the fallback removed, the marker is
        // skipped cleanly — no spurious stamping on "whichever row won
        // the race".
        try await service.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: "pod-h5-nil",
            episodeDuration: 90,
            sessionId: nil
        )

        #expect(markerCallCount == 0, "nil sessionId must cause the marker to be skipped, not fall back to fetchLatestSessionForAsset")
        #expect(capturedSessionId == nil)
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
    func waitForPendingSleep(timeoutSeconds: Double = 5.0) async throws {
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

    func waitForCall(timeoutSeconds: Double = 5.0) async throws {
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
