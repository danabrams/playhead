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
        // pmp9 does NOT touch — there is no retry and no backoff. The job must
        // persist a failure row, mark COMPLETE, and never defer.
        //
        // playhead-qbib: what changed is the BLAST RADIUS, not the defer
        // taxonomy. A guardrail is now a per-window outcome, so the two
        // windows after it are still scanned instead of being silently
        // dropped; the assertion below moved from "1 coarse call (the pass
        // aborted)" to "3 coarse calls (every planned window was attempted)".
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
        // Guardrail is NOT retried (persistFailure policy) — no backoff loop,
        // exactly one call per planned window.
        #expect(await fmRuntime.coarseCallCount == 3)
        // playhead-qbib: the two windows AFTER the guardrail were still
        // scanned. Every planned window is accounted for, and the guardrailed
        // one is reported as audio we could not look at.
        let passA = scans.filter { $0.scanPass == "passA" }
        #expect(passA.count == 3)
        #expect(passA.filter { $0.status.didExamineWindow }.count == 2)
        #expect(passA.filter { $0.status == .guardrailViolation }.count == 1)
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
        // playhead-bkhc: and because it checkpoints NOTHING, the next window
        // must redo the whole coarse pass — so this window genuinely retained
        // no progress and DOES spend one unit of the attempt budget. Pinned
        // explicitly: it is the one case where a leg did real FM work and is
        // still charged, and that is a deliberate consequence of t1kq's
        // "never checkpoint an episode-end cursor" rule, not an oversight.
        #expect(row.retryCount == 1)
    }

    // MARK: - playhead-bkhc: expiry DURING THE COARSE PASS must not discard the pass

    /// Total seconds of audio a run's persisted passA rows actually examined,
    /// measured as the length of the UNION of their `[windowStartTime,
    /// windowEndTime)` intervals. Union, not sum, so a re-scan of already-
    /// covered audio cannot inflate the number — which is the whole point: the
    /// question "did the second window continue where the first stopped" is a
    /// question about audio, not about row counts or status columns.
    private func examinedAudioSeconds(_ rows: [SemanticScanResult]) -> Double {
        let intervals = rows
            .filter { $0.scanPass == "passA" && $0.status.didExamineWindow }
            .map { ($0.windowStartTime, $0.windowEndTime) }
            .sorted { $0.0 < $1.0 }
        var total = 0.0
        var cursor = -Double.infinity
        for (start, end) in intervals where end > cursor {
            total += end - max(start, cursor)
            cursor = end
        }
        return total
    }

    @available(iOS 26.0, *)
    @Test("a BG-window expiry DURING the coarse pass keeps the audio it already screened, and the next window resumes from there")
    func expiryDuringCoarsePassCheckpointsAudioAndResumes() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let inputs = makeThreeWindowInputs()

        // BG window 1: windows 0 (0..10) and 1 (10..20) are screened, then the
        // OS expires the task. `coarsePassA` returns its partial output; the
        // runner must persist that work and checkpoint 20s of covered audio.
        let gate = CoarseCallGate(triggerOnCall: 2)
        let rt1 = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule(),
            onCoarseRespond: { ordinal in await gate.arriveAndWait(ordinal: ordinal) }
        )
        let task = Task { try await makeRunner(store: store, runtime: rt1.runtime).runPendingBackfill(for: inputs) }
        await gate.awaitReached()   // window 1's respond is about to produce
        task.cancel()               // BG window expires
        await gate.release()
        await #expect(throws: CancellationError.self) { _ = try await task.value }

        let jobId = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: inputs.analysisAssetId,
            transcriptVersion: inputs.transcriptVersion,
            phase: .fullEpisodeScan,
            offset: 0
        )
        #expect(await rt1.coarseCallCount == 2, "two windows were screened before expiry")

        // THE ASSERTION THAT MATTERS: seconds of audio, not a status column.
        let afterRun1 = try await store.fetchSemanticScanResults(analysisAssetId: "asset-pmp9")
        #expect(examinedAudioSeconds(afterRun1) == 20,
                "the 20s of audio the expired window paid FM for must be durable")

        let deferredRow = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(deferredRow.status == .deferred)
        #expect(deferredRow.progressCursor?.lastProcessedUpperBoundSec == 20,
                "checkpoint must be the honest contiguous prefix (20s), not nil and not episode-end")
        #expect(deferredRow.retryCount == 0,
                "a window that MADE progress must not consume the attempt budget")

        // BG window 2: no expiry. The job must resume at 20s.
        let rt2 = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule()
        )
        let run2 = try await makeRunner(store: store, runtime: rt2.runtime).runPendingBackfill(for: inputs)
        #expect(run2.admittedJobIds.contains(jobId))
        #expect(await rt2.coarseCallCount == 1,
                "resume must screen ONLY the remaining 10s window, not re-screen the whole episode")

        let afterRun2 = try await store.fetchSemanticScanResults(analysisAssetId: "asset-pmp9")
        #expect(examinedAudioSeconds(afterRun2) == 30,
                "two BG windows must together cover the whole 30s episode")
        let completedRow = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(completedRow.status == .complete)
        #expect(completedRow.progressCursor?.lastProcessedUpperBoundSec == 30)
    }

    // MARK: - playhead-8d5r x playhead-bkhc: a per-call deadline must bank too

    /// The composition risk this test exists for: playhead-8d5r added a hard
    /// per-call deadline INSIDE the coarse pass, and playhead-bkhc's entire
    /// contribution is that the pass banks what it finished rather than
    /// discarding it. If a timeout ever escaped as a throw — the way the
    /// cancellation check used to, before bkhc moved it past the digest loop —
    /// the FM time already paid for would be discarded again.
    ///
    /// So: windows 1 and 2 hang after window 0 has been screened, which trips
    /// the two-consecutive no-progress guard and ends the pass early. The
    /// assertion is in SECONDS OF AUDIO, the same currency the bkhc tests
    /// above use, because that is the quantity a discard actually destroys.
    @available(iOS 26.0, *)
    @Test("playhead-8d5r: a coarse pass ended by the timeout guard still banks the audio it screened")
    func inferenceTimeoutAbortBanksScreenedAudio() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let inputs = makeThreeWindowInputs()

        // Calls 2 and 3 (windows 1 and 2) report a deadline; call 3 spends a
        // deliberate 50ms first so the two attempts have plainly different
        // costs — see the latency assertion below. Injected rather than hung:
        // racing a real deadline against ~8,300 concurrent tests measures the
        // suite's load, not this code (playhead-zx0l).
        let runtime = TestFMRuntime(
            coarseFailures: [nil, .inferenceTimeout, .inferenceTimeout],
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule(),
            onCoarseRespond: { ordinal in
                if ordinal == 3 {
                    try? await Task.sleep(for: .milliseconds(50))
                }
            }
        )

        // A run that ends on the guard must RETURN, not throw: a throw would
        // unwind past the persistence the pass already earned.
        _ = try await makeRunner(
            store: store,
            runtime: runtime.runtime
        ).runPendingBackfill(for: inputs)

        #expect(
            await runtime.coarseCallCount == 3,
            "the guard must stop the pass after two consecutive timeouts, not attempt more"
        )

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-pmp9")
        #expect(
            examinedAudioSeconds(rows) == 10,
            "the 10s of audio the pass paid FM for before the guard tripped must be durable"
        )

        // The holes are named, not guessed at.
        let timeouts = rows.filter { $0.status == .inferenceTimeout }
        #expect(timeouts.count == 2)
        #expect(timeouts.allSatisfy { !$0.status.didExamineWindow })

        // And each names its OWN cost. Before playhead-8d5r every failure row
        // inherited the whole pass's wall-clock, so two rows from one pass
        // reported the same number and summing the column double-counted it.
        let passATimeouts = timeouts.filter { $0.scanPass == "passA" }
        #expect(passATimeouts.count == 2)
        #expect(passATimeouts.allSatisfy { ($0.latencyMs ?? 0) > 0 })
        // THE PRECISE SIGNATURE OF THE OLD DEFECT. When every failure row
        // inherited the pass total, N rows from one pass carried the BIT-
        // IDENTICAL number — which is why summing the column on the device pull
        // reported 213.8 minutes of a quantity that was really 115.9. Two
        // independently-measured attempts cannot collide on a Double.
        #expect(
            Set(passATimeouts.compactMap(\.latencyMs)).count == 2,
            """
            both timeout rows reported the same latency \
            (\(passATimeouts.compactMap(\.latencyMs))) — that is the pass-total \
            stamp, not each attempt's own cost.
            """
        )
    }

    @available(iOS 26.0, *)
    @Test("resuming after a coarse-pass expiry duplicates no window: one row per window, no overlapping audio")
    func resumeAfterCoarseExpiryIsIdempotent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let inputs = makeThreeWindowInputs()

        let gate = CoarseCallGate(triggerOnCall: 2)
        let rt1 = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule(),
            onCoarseRespond: { ordinal in await gate.arriveAndWait(ordinal: ordinal) }
        )
        let task = Task { try await makeRunner(store: store, runtime: rt1.runtime).runPendingBackfill(for: inputs) }
        await gate.awaitReached()
        task.cancel()
        await gate.release()
        await #expect(throws: CancellationError.self) { _ = try await task.value }

        let rt2 = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule()
        )
        _ = try await makeRunner(store: store, runtime: rt2.runtime).runPendingBackfill(for: inputs)

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-pmp9")
        let passA = rows.filter { $0.scanPass == "passA" }
        // Exactly one row per planned window across BOTH passes. The window
        // straddling the checkpoint is REPLACED (same reuseKeyHash), never
        // duplicated, and the pre-checkpoint windows are not re-emitted.
        #expect(passA.count == 3, "one passA row per window across the resumed pass, no duplicates")
        let geometries = Set(passA.map { "\($0.windowFirstAtomOrdinal)-\($0.windowLastAtomOrdinal)" })
        #expect(geometries.count == 3, "no two passA rows may cover the same window")
        // Sum == union ⇒ no audio was examined twice across the resume.
        let examined = passA.filter { $0.status.didExamineWindow }
        let summed = examined.reduce(0.0) { $0 + ($1.windowEndTime - $1.windowStartTime) }
        #expect(summed == examinedAudioSeconds(rows),
                "summed window durations must equal the union — any excess is double-scanned audio")
        // Evidence events are content-addressed; a resumed pass must not
        // multiply them either.
        let events = try await store.fetchEvidenceEvents(analysisAssetId: "asset-pmp9")
        #expect(events.count == Set(events.map(\.id)).count, "no duplicated evidence events across the resume")
    }

    /// Six 10s windows — an episode NO single background window can finish,
    /// which is the device situation: 26–295 s granted against 12–45 min of
    /// work. Before this bead the answer to "how much audio do N such windows
    /// process" was 0 s for every N; the episode could never complete.
    private func makeSixWindowInputs(
        assetId: String = "asset-pmp9",
        podcastId: String = "podcast-pmp9",
        transcriptVersion: String = "tx-pmp9-v1"
    ) -> BackfillJobRunner.AssetInputs {
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: (0..<6).map { index in
                (Double(index) * 10, Double(index + 1) * 10, "Window \(index) of the conversation, some words here.")
            }
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: podcastId,
            segments: segments,
            evidenceCatalog: EvidenceCatalogBuilder.build(
                atoms: segments.flatMap(\.atoms),
                analysisAssetId: assetId,
                transcriptVersion: transcriptVersion
            ),
            transcriptVersion: transcriptVersion,
            plannerContext: CoveragePlannerContext(
                observedEpisodeCount: 0,
                stableRecall: false,
                isFirstEpisodeAfterCohortInvalidation: false,
                recallDegrading: false,
                sponsorDriftDetected: false,
                auditMissDetected: false,
                episodesSinceLastFullRescan: 0,
                periodicFullRescanIntervalEpisodes: 10
            )
        )
    }

    /// Runs one background window that expires after `expireAfterCoarseCalls`
    /// coarse windows have been screened, and returns how many FM coarse calls
    /// it spent.
    @available(iOS 26.0, *)
    private func runExpiringWindow(
        store: AnalysisStore,
        inputs: BackfillJobRunner.AssetInputs,
        expireAfterCoarseCalls: Int
    ) async -> Int {
        let gate = CoarseCallGate(triggerOnCall: expireAfterCoarseCalls + 1)
        let runtime = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule(),
            onCoarseRespond: { ordinal in await gate.arriveAndWait(ordinal: ordinal) }
        )
        let task = Task { try await makeRunner(store: store, runtime: runtime.runtime).runPendingBackfill(for: inputs) }
        await gate.awaitReached()
        task.cancel()
        await gate.release()
        _ = try? await task.value
        return await runtime.coarseCallCount
    }

    @available(iOS 26.0, *)
    @Test("MEASURED: a fixed number of expiring background windows now processes a growing amount of audio, and finishes")
    func expiringBackgroundWindowsAccumulateAudio() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let inputs = makeSixWindowInputs()
        let jobId = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: inputs.analysisAssetId,
            transcriptVersion: inputs.transcriptVersion,
            phase: .fullEpisodeScan,
            offset: 0
        )
        func coveredSeconds() async throws -> Double {
            examinedAudioSeconds(try await store.fetchSemanticScanResults(analysisAssetId: "asset-pmp9"))
        }

        // Window 1: three coarse windows screened, then expiry.
        let calls1 = await runExpiringWindow(store: store, inputs: inputs, expireAfterCoarseCalls: 2)
        let covered1 = try await coveredSeconds()
        #expect(covered1 == 30)
        var row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .deferred)
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == 30)
        #expect(row.retryCount == 0, "a converging job must never consume the attempt budget")

        // Window 2: resumes at 30s, screens two more, expires again.
        let calls2 = await runExpiringWindow(store: store, inputs: inputs, expireAfterCoarseCalls: 1)
        let covered2 = try await coveredSeconds()
        #expect(covered2 == 50, "audio processed must GROW across background windows")
        row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .deferred)
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == 50)

        // Window 3: one window left; the episode finishes.
        let rt3 = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule()
        )
        _ = try await makeRunner(store: store, runtime: rt3.runtime).runPendingBackfill(for: inputs)
        let calls3 = await rt3.coarseCallCount
        let covered3 = try await coveredSeconds()
        #expect(covered3 == 60, "three background windows cover the whole episode")
        let final = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(final.status == .complete)

        // And the episode cost exactly six coarse windows in total: every
        // expiry banked its work, so nothing was ever screened twice. Before
        // this bead each window restarted from zero and the totals diverged
        // while covered audio stayed at 0.
        #expect(calls1 + calls2 + calls3 == 6,
                "no window may be re-screened across resumptions (got \(calls1)+\(calls2)+\(calls3))")
    }

    /// Full row state in one line. Every assertion about the attempt budget
    /// carries it, because `retryCount` alone cannot say WHICH branch wrote the
    /// row — `deferReason` is the discriminator between the cancellation
    /// branch, the rate-limit defer, and a terminal transition.
    private static func describe(_ row: BackfillJob) -> String {
        let cursor: Double = row.progressCursor?.lastProcessedUpperBoundSec ?? -1
        let reason: String = row.deferReason ?? "nil"
        let status: String = row.status.rawValue
        return "[status=\(status) retry=\(row.retryCount) cursor=\(cursor) reason=\(reason)]"
    }

    /// One background window that expires before its FIRST coarse window can
    /// return — the short-grant case the device sees constantly (26–295 s
    /// granted against minutes per window). It covers no new audio, so it is
    /// barren by definition.
    @available(iOS 26.0, *)
    private func runBarrenWindow(
        store: AnalysisStore,
        inputs: BackfillJobRunner.AssetInputs
    ) async {
        let gate = CoarseCallGate(triggerOnCall: 1)
        let runtime = TestFMRuntime(
            coarseFailures: [.refusal],
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule(),
            onCoarseRespond: { ordinal in await gate.arriveAndWait(ordinal: ordinal) }
        )
        let task = Task { try await makeRunner(store: store, runtime: runtime.runtime).runPendingBackfill(for: inputs) }
        await gate.awaitReached()
        task.cancel()
        await gate.release()
        _ = try? await task.value
    }

    @available(iOS 26.0, *)
    @Test("the attempt budget counts CONSECUTIVE barren windows — a window that covers new audio RESETS it")
    func productiveWindowResetsTheBarrenBudget() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let inputs = makeSixWindowInputs()
        let jobId = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: inputs.analysisAssetId,
            transcriptVersion: inputs.transcriptVersion,
            phase: .fullEpisodeScan,
            offset: 0
        )

        // Two barren windows take the job to the edge of the budget…
        await runBarrenWindow(store: store, inputs: inputs)
        await runBarrenWindow(store: store, inputs: inputs)
        var row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.retryCount == AdmissionController.maxRetries - 1, "\(Self.describe(row))")
        #expect(row.status == .deferred, "\(Self.describe(row))")

        // …then ONE window that actually covers audio. A converging job must
        // not be killed by unlucky short windows earlier in its history, so
        // the barren budget goes back to zero.
        let before = Self.describe(row)
        let productiveCalls = await runExpiringWindow(store: store, inputs: inputs, expireAfterCoarseCalls: 2)
        row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == 30,
                "the productive window banked 30s — before=\(before) after=\(Self.describe(row)) calls=\(productiveCalls)")
        #expect(row.retryCount == 0,
                "covering new audio must RESET the consecutive-barren budget — before=\(before) after=\(Self.describe(row)) calls=\(productiveCalls)")
        #expect(row.status == .deferred, "\(Self.describe(row))")

        // And the full budget is available again: two more barren windows
        // still leave the job resumable rather than failed.
        await runBarrenWindow(store: store, inputs: inputs)
        await runBarrenWindow(store: store, inputs: inputs)
        row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .deferred, "a job that recently progressed is still resumable — \(Self.describe(row))")
        #expect(row.retryCount == AdmissionController.maxRetries - 1, "\(Self.describe(row))")
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == 30,
                "barren windows must never regress the cursor — \(Self.describe(row))")
    }

    @available(iOS 26.0, *)
    @Test("a job that makes NO progress across repeated expiries TERMINATES with a named cause instead of resuming forever")
    func repeatedNoProgressExpiriesTerminateWithNamedCause() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let inputs = makeThreeWindowInputs()
        let jobId = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: inputs.analysisAssetId,
            transcriptVersion: inputs.transcriptVersion,
            phase: .fullEpisodeScan,
            offset: 0
        )

        // Every BG window: window 0 refuses, then the OS expires the task —
        // so the contiguous covered prefix never leaves 0s. This is the
        // treadmill shape; it must not run forever.
        for attempt in 1...AdmissionController.maxRetries {
            let gate = CoarseCallGate(triggerOnCall: 1)
            let runtime = TestFMRuntime(
                coarseFailures: [.refusal],
                contextSize: Self.contextSize,
                coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
                tokenCountRule: windowingTokenRule(),
                onCoarseRespond: { ordinal in await gate.arriveAndWait(ordinal: ordinal) }
            )
            let task = Task { try await makeRunner(store: store, runtime: runtime.runtime).runPendingBackfill(for: inputs) }
            await gate.awaitReached()
            task.cancel()
            await gate.release()
            await #expect(throws: CancellationError.self) { _ = try await task.value }

            let row = try #require(await store.fetchBackfillJob(byId: jobId))
            #expect(row.progressCursor?.lastProcessedUpperBoundSec == nil, "no audio was ever covered")
            #expect(row.retryCount == attempt, "a zero-progress expiry must consume one attempt")
            if attempt < AdmissionController.maxRetries {
                #expect(row.status == .deferred, "still within the attempt budget")
            }
        }

        let terminal = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(terminal.status == .failed, "the treadmill must terminate, not resume forever")
        #expect(terminal.deferReason == BackfillJobRunner.noProgressExpiryReason(phase: .fullEpisodeScan))
        #expect(terminal.retryCount == AdmissionController.maxRetries)

        // And it stays terminated: a further drain admits nothing and spends
        // no FM calls on it.
        let rt = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule()
        )
        let after = try await makeRunner(store: store, runtime: rt.runtime).runPendingBackfill(for: inputs)
        #expect(!after.admittedJobIds.contains(jobId))
        #expect(await rt.coarseCallCount == 0)
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

/// playhead-bkhc: the coarse-pass analogue of `RefinementGate`. Blocks the Nth
/// coarse respond so a test can cancel the runner's `Task` mid-coarse-pass —
/// the real BG-window expiry shape, since the coarse pass is where the FM
/// wall-clock is spent. Continuation-synchronized, never sleep-based.
private actor CoarseCallGate {
    private let triggerOnCall: Int
    private var reached = false
    private var released = false
    private var reachedContinuation: CheckedContinuation<Void, Never>?
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    init(triggerOnCall: Int) {
        self.triggerOnCall = triggerOnCall
    }

    /// Runner-side: on the trigger ordinal, signal arrival then block until the
    /// test releases. Every other ordinal passes straight through.
    func arriveAndWait(ordinal: Int) async {
        guard ordinal == triggerOnCall else { return }
        reached = true
        reachedContinuation?.resume()
        reachedContinuation = nil
        if released { return }
        await withCheckedContinuation { releaseContinuation = $0 }
    }

    func awaitReached() async {
        if reached { return }
        await withCheckedContinuation { reachedContinuation = $0 }
    }

    func release() {
        released = true
        releaseContinuation?.resume()
        releaseContinuation = nil
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
