// BackfillRateLimitDeferTests.swift
// playhead-pmp9: an FM `fullEpisodeScan` window that survives the full
// rate-limit backoff budget must DEFER the job (non-terminal, resumable) with
// an HONEST progress cursor — NOT mark it `complete` with permanent coverage
// holes that the M-5 idempotency gate then skips forever. A re-drive of the
// deferred job must RESUME from the cursor, scanning only the un-scanned
// remainder. These tests pin that contract end-to-end (defer → resume →
// complete) plus the capped-exponential backoff schedule and the
// no-regression paths. None boot the real Foundation Models stack.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-pmp9: rate-limit defer + resume")
struct BackfillRateLimitDeferTests {

    // MARK: - Fixtures

    /// Three single-segment coarse windows at [0,10], [10,20], [20,30]. The
    /// windowing math (contextSize / schema tokens / token rule / config
    /// budget) is the same proven setup that yields exactly one window per
    /// segment in `FoundationModelClassifierTests`.
    private static let contextSize = 431
    private static let coarseSchemaTokenCount = 4

    private func windowingTokenRule() -> @Sendable (String) -> Int {
        { prompt in prompt.split(separator: "\n", omittingEmptySubsequences: false).count * 8 }
    }

    private func windowingConfig(interWindowPacingNanos: UInt64 = 0) -> FoundationModelClassifier.Config {
        FoundationModelClassifier.Config(
            safetyMarginTokens: 5,
            coarseMaximumResponseTokens: 6,
            refinementMaximumResponseTokens: 12,
            interWindowPacingNanos: interWindowPacingNanos
        )
    }

    private func makeAsset(id: String = "asset-pmp9") -> AnalysisAsset {
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

    private func makeThreeWindowInputs(
        assetId: String = "asset-pmp9",
        podcastId: String = "podcast-pmp9",
        transcriptVersion: String = "tx-pmp9-v1"
    ) -> BackfillJobRunner.AssetInputs {
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: [
                (0, 10, "Window zero editorial content about the topic."),
                (10, 20, "Window one sponsor break maybe present here."),
                (20, 30, "Window two back to the show conversation.")
            ]
        )
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments.flatMap(\.atoms),
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion
        )
        let plannerContext = CoveragePlannerContext(
            observedEpisodeCount: 0,
            stableRecall: false,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 0,
            periodicFullRescanIntervalEpisodes: 10
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: podcastId,
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            transcriptVersion: transcriptVersion,
            plannerContext: plannerContext
        )
    }

    private func makeRunner(
        store: AnalysisStore,
        runtime: FoundationModelClassifier.Runtime
    ) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime, config: windowingConfig()),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )
    }

    /// Coarse-failure queue for a 3-window pass where the MIDDLE window is
    /// abandoned only after the full backoff budget: window 0 succeeds (1 call),
    /// window 1 rate-limits on the initial call + every retry (budget calls),
    /// window 2 succeeds (1 call).
    private func midWindowRateLimitQueue() -> [TestFMRuntimeFailure?] {
        let budget = 1 + FoundationModelClassifier.rateLimitBackoffBaseNanos.count
        var queue: [TestFMRuntimeFailure?] = [nil]
        queue.append(contentsOf: Array(repeating: .rateLimited, count: budget))
        queue.append(nil)
        return queue
    }

    // MARK: - Test 1: mid-episode rate-limit DEFERS with an honest cursor

    @available(iOS 26.0, *)
    @Test("a mid-episode rate-limited window DEFERS the job with an HONEST cursor (last success end, NOT episode end)")
    func midEpisodeRateLimitDefersWithHonestCursor() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseFailures: midWindowRateLimitQueue(),
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule()
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeThreeWindowInputs())

        // The job ran (admitted) but ended DEFERRED, not complete-with-holes.
        let jobId = try #require(result.admittedJobIds.first)
        #expect(result.deferredJobIds.contains(jobId))

        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .deferred)
        #expect(row.deferReason == "rateLimited-backoff")
        // HONEST cursor: the contiguous scanned prefix ends at window 0's end
        // (10s) — the hole is window 1 (10..20). It is NOT the episode end (30s)
        // even though window 2 (20..30) happened to succeed AFTER the hole.
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == 10)
        #expect(row.retryCount == 0)

        // The rate-limited window is persisted as a failure row (coverage is
        // observable), and it is NOT silently swallowed.
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-pmp9")
        #expect(scans.contains { $0.status == .rateLimited })

        // The full backoff budget was exhausted on the middle window.
        let expectedCalls = 1 + (1 + FoundationModelClassifier.rateLimitBackoffBaseNanos.count) + 1
        #expect(await fmRuntime.coarseCallCount == expectedCalls)
    }

    // MARK: - Test 2: a deferred job RESUMES from the cursor and completes

    @available(iOS 26.0, *)
    @Test("re-driving a deferred job RESUMES from the cursor — scans only the remainder — then completes with full coverage")
    func deferredJobResumesFromCursorAndCompletes() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let inputs = makeThreeWindowInputs()

        // Run 1: middle window rate-limits → job DEFERS at cursor 10.
        let rt1 = TestFMRuntime(
            coarseFailures: midWindowRateLimitQueue(),
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule()
        )
        let run1 = try await makeRunner(store: store, runtime: rt1.runtime).runPendingBackfill(for: inputs)
        let jobId = try #require(run1.admittedJobIds.first)
        let deferredRow = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(deferredRow.status == .deferred)
        #expect(deferredRow.progressCursor?.lastProcessedUpperBoundSec == 10)

        // Run 2: no rate-limiting. The M-5 gate re-drives the deferred row; the
        // runner RESUMES from cursor 10 and scans ONLY the remainder (windows 1
        // and 2 = segments at end 20 and 30), skipping window 0 (end 10).
        let rt2 = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule()
        )
        let run2 = try await makeRunner(store: store, runtime: rt2.runtime).runPendingBackfill(for: inputs)

        // Only the two remaining windows were scanned — NOT all three.
        #expect(await rt2.coarseCallCount == 2, "resume must scan only the un-scanned remainder (2 windows), not re-window the whole episode")
        #expect(run2.admittedJobIds.contains(jobId))
        #expect(run2.deferredJobIds.isEmpty)

        // The job is now COMPLETE with a genuine full-coverage cursor (episode
        // end = 30), not the partial 10 it deferred at.
        let completedRow = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(completedRow.status == .complete)
        #expect(completedRow.progressCursor?.lastProcessedUpperBoundSec == 30)
    }

    // MARK: - Test 3: backoff is capped-exponential, not a single 50ms

    @Test("rate-limit backoff schedule is capped-exponential (0.5s→1s→2s→4s), not a single 50ms retry")
    func backoffScheduleIsCappedExponential() {
        let schedule = FoundationModelClassifier.rateLimitBackoffBaseNanos

        // Exactly the documented capped-exponential schedule.
        #expect(schedule == [500_000_000, 1_000_000_000, 2_000_000_000, 4_000_000_000])
        // More than one retry (the old behavior was a single fixed retry).
        #expect(schedule.count > 1)
        // NOT the old single fixed 50ms delay.
        #expect(!schedule.contains(50_000_000))
        #expect(schedule.first != 50_000_000)
        // Doubling until the cap; monotonically non-decreasing; capped at 4s.
        for idx in 1..<schedule.count {
            #expect(schedule[idx] >= schedule[idx - 1])
        }
        #expect(schedule[1] == schedule[0] * 2)
        #expect(schedule[2] == schedule[1] * 2)
        #expect(schedule.last == 4_000_000_000)

        // Jitter stays within ±20% of the base and never underflows.
        for base in schedule {
            for _ in 0..<64 {
                let jittered = FoundationModelClassifier.jitteredBackoffNanos(base)
                #expect(Double(jittered) >= Double(base) * 0.8 - 1)
                #expect(Double(jittered) <= Double(base) * 1.2 + 1)
            }
        }
    }

    // MARK: - Test 5/7: no-regression — a clean pass still completes honestly

    @available(iOS 26.0, *)
    @Test("no-regression: a fully-successful pass COMPLETES with an honest full-coverage cursor and no defer")
    func fullySuccessfulPassCompletesWithFullCoverageCursor() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        // No failures at all — the default no-rate-limit path.
        let fmRuntime = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule()
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeThreeWindowInputs())

        let jobId = try #require(result.admittedJobIds.first)
        #expect(result.deferredJobIds.isEmpty, "a clean pass must never defer")

        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .complete)
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == 30, "full coverage cursor = episode end")
        // Three windows scanned exactly once each — byte-identical call count.
        #expect(await fmRuntime.coarseCallCount == 3)
    }

    // MARK: - Test 6: non-rate-limit graceful failure does NOT defer

    @available(iOS 26.0, *)
    @Test("no-regression: a non-rate-limit graceful failure (guardrail) COMPLETES, it does not defer")
    func nonRateLimitGracefulFailureCompletesNotDeferred() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        // A guardrail violation is a terminal graceful failure whose taxonomy
        // pmp9 does NOT touch — it aborts the pass (guardrail is not in the
        // tolerate list) with no retry. The job must behave exactly as before:
        // persist a failure row, mark COMPLETE, never defer.
        let fmRuntime = TestFMRuntime(
            coarseFailures: [.guardrailViolation],
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule()
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeThreeWindowInputs())

        let jobId = try #require(result.admittedJobIds.first)
        #expect(result.deferredJobIds.isEmpty, "guardrail failures must not route through the rate-limit defer path")

        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .complete)

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-pmp9")
        #expect(scans.contains { $0.status == .guardrailViolation })
        // Guardrail is NOT retried (persistFailure policy) and aborts the pass
        // on the first window — a single coarse call, no backoff loop.
        #expect(await fmRuntime.coarseCallCount == 1)
    }

    // MARK: - playhead-t1kq: a BG-window EXPIRY (task cancellation) checkpoints the honest cursor

    /// A minimal SUCCESSFUL refinement window (one paid third-party span) so
    /// `refinement.windows` is non-empty and the runner reaches the
    /// post-refinement `Task.checkCancellation()` (BackfillJobRunner.swift:1606
    /// at time of writing) that surfaces the cancellation.
    private static func oneSpanRefinement() -> RefinementWindowSchema {
        RefinementWindowSchema(spans: [
            SpanRefinementSchema(
                commercialIntent: .paid,
                ownership: .thirdParty,
                firstLineRef: 0,
                lastLineRef: 0,
                certainty: .strong,
                boundaryPrecision: .precise,
                evidenceAnchors: [
                    EvidenceAnchorSchema(
                        evidenceRef: 0,
                        lineRef: 0,
                        kind: .url,
                        certainty: .strong
                    )
                ],
                alternativeExplanation: .none,
                reasonTags: [.callToAction]
            )
        ])
    }

    @available(iOS 26.0, *)
    @Test("a BG-window expiry (task cancellation) during refinement DEFERS with the honest PARTIAL coarse cursor checkpointed — not re-scanned")
    func cancellationDuringRefinementCheckpointsHonestCursor() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let inputs = makeThreeWindowInputs()

        // Coarse: window 0 (0..10) contains an ad (so refinement zooms it);
        // window 1 (10..20) REFUSES — a tolerated hole that keeps the pass
        // `.success` (so it does NOT take the rate-limit defer branch); window
        // 2 (20..30) succeeds. The honest CONTIGUOUS coarse cursor is therefore
        // window 0's end = 10 (the hole is window 1). The gate then lets us
        // cancel the Task (a BG-window EXPIRY) once execution reaches the
        // refinement respond — AFTER the honest cursor is captured — so the
        // post-refinement `Task.checkCancellation()` throws. The runner must
        // convert that into a DEFER with the honest cursor checkpointed.
        let gate = RefinementGate()
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(supportLineRefs: [0], certainty: .strong)
                )
            ],
            refinementResponses: [Self.oneSpanRefinement()],
            coarseFailures: [nil, .refusal, nil],
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule(),
            onRefinementRespond: { await gate.arriveAndWait() }
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let task = Task { try await runner.runPendingBackfill(for: inputs) }
        await gate.awaitReached()   // coarse done + honest cursor (10) captured; blocked at refinement
        task.cancel()               // BG window expires
        await gate.release()        // refinement proceeds → post-refinement checkCancellation throws

        // The runner re-throws CancellationError after deferring (H-2 contract).
        await #expect(throws: CancellationError.self) { _ = try await task.value }
        #expect(await fmRuntime.refinementCallCount >= 1)

        // The (deterministic-id) job is DEFERRED with the honest PARTIAL cursor
        // (10) — NOT episode end (30), and NOT nil (which would re-scan the
        // coarse prefix on the next BG window).
        let jobId = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: inputs.analysisAssetId,
            transcriptVersion: inputs.transcriptVersion,
            phase: .fullEpisodeScan,
            offset: 0
        )
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .deferred)
        #expect(row.deferReason == "cancelled-during-fullEpisodeScan")
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == 10)
    }

    @available(iOS 26.0, *)
    @Test("a task cancellation after FULL coarse coverage does NOT checkpoint an episode-end cursor (would strand refinement)")
    func fullCoverageCancellationDoesNotCheckpoint() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let inputs = makeThreeWindowInputs()

        // Every coarse window succeeds (fully covered); window 0 contains an ad
        // so refinement runs; the Task is cancelled during refinement. Because
        // coverage is COMPLETE, the honest cursor would be episode-end (30) —
        // which `narrowedForResume` collapses to an empty resume that marks the
        // job complete and STRANDS pending refinement. So the runner must NOT
        // checkpoint: the deferred row keeps a nil cursor and the next BG
        // window re-drives coarse + refinement cleanly.
        let gate = RefinementGate()
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(supportLineRefs: [0], certainty: .strong)
                )
            ],
            refinementResponses: [Self.oneSpanRefinement()],
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule(),
            onRefinementRespond: { await gate.arriveAndWait() }
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let task = Task { try await runner.runPendingBackfill(for: inputs) }
        await gate.awaitReached()
        task.cancel()
        await gate.release()

        await #expect(throws: CancellationError.self) { _ = try await task.value }
        #expect(await fmRuntime.refinementCallCount >= 1)

        let jobId = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: inputs.analysisAssetId,
            transcriptVersion: inputs.transcriptVersion,
            phase: .fullEpisodeScan,
            offset: 0
        )
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .deferred)
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == nil,
                "a full-coverage cancellation must not checkpoint an episode-end cursor")
    }

    // MARK: - Item 4: inter-window pacing invokes the injected sleep

    @available(iOS 26.0, *)
    @Test("inter-window pacing, when configured, sleeps between (not before) per-window respond calls")
    func interWindowPacingInvokesSleepBetweenWindows() async throws {
        let recorder = SleepRecorder()
        let pacingNanos: UInt64 = 4242
        let fmRuntime = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            backoffSleep: { nanos in await recorder.record(nanos) },
            tokenCountRule: windowingTokenRule()
        )
        let classifier = FoundationModelClassifier(
            runtime: fmRuntime.runtime,
            config: windowingConfig(interWindowPacingNanos: pacingNanos)
        )
        let segments = makeFMSegments(
            analysisAssetId: "asset-pmp9",
            transcriptVersion: "tx-pmp9-v1",
            lines: [
                (0, 10, "Window zero editorial content about the topic."),
                (10, 20, "Window one sponsor break maybe present here."),
                (20, 30, "Window two back to the show conversation.")
            ]
        )

        _ = try await classifier.coarsePassA(segments: segments)

        // Three windows, no rate-limiting → pacing fires before windows 1 and 2
        // only (never before the first), each at the configured delay.
        let sleeps = await recorder.sleeps
        #expect(sleeps == [pacingNanos, pacingNanos])
    }
}

/// Records the nanosecond delays passed to an injected `backoffSleep`.
private actor SleepRecorder {
    private(set) var sleeps: [UInt64] = []
    func record(_ nanos: UInt64) { sleeps.append(nanos) }
}

/// playhead-t1kq: a deterministic (continuation-synchronized, NOT sleep-based)
/// rendezvous so a test can cancel the runner's `Task` exactly when execution
/// reaches the refinement respond — i.e. AFTER the coarse pass captured the
/// honest cursor but BEFORE the post-refinement `Task.checkCancellation()`.
/// `arriveAndWait()` is called from inside the runner (via
/// `TestFMRuntime.onRefinementRespond`); the test drives `awaitReached()` then
/// `release()`.
private actor RefinementGate {
    private var reached = false
    private var released = false
    private var reachedContinuation: CheckedContinuation<Void, Never>?
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    /// Runner-side: signal arrival, then block until the test releases.
    func arriveAndWait() async {
        reached = true
        reachedContinuation?.resume()
        reachedContinuation = nil
        if released { return }
        await withCheckedContinuation { releaseContinuation = $0 }
    }

    /// Test-side: resume once the runner has reached the refinement respond.
    func awaitReached() async {
        if reached { return }
        await withCheckedContinuation { reachedContinuation = $0 }
    }

    /// Test-side: let the gated refinement respond proceed.
    func release() {
        released = true
        releaseContinuation?.resume()
        releaseContinuation = nil
    }
}
