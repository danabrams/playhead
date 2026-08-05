// FMDaemonThrottleTests.swift
// playhead-kvs8 — an FM daemon THROTTLE is not a model failure.
//
// THE FIELD ROW THIS FILE EXISTS FOR. From Dan's 2026-08-01 03:37 device pull,
// `backfill_jobs`, episode DE0784D8, phase `fullEpisodeScan`:
//
//     status      = failed
//     retryCount  = 1
//     deferReason = "Request has been rate limited. Please try again later.
//                    If you are using streaming responses in a background
//                    request, consider using non-streaming requests in
//                    background activities to reduce the likelihood of rate
//                    limiting."
//
// Three separate defects are legible in those three fields:
//
//   1. `status = failed`. A throttle is TEMPORARY and the daemon says so in its
//      own message ("try again later"). Marking the job terminal is the
//      permanent-coverage-hole bug playhead-pmp9 removed for rate-limited
//      WINDOWS, reintroduced one level up for a rate-limited PASS.
//   2. `retryCount = 1`. The job spent one of its `AdmissionController.maxRetries`
//      lifetime attempts on an event that says nothing about the job. Three
//      throttles and an episode is disqualified forever having never been
//      scanned once.
//   3. A raw framework string in `deferReason`. Every other stop-short cause in
//      this runner has a NAMED token — `rateLimited-backoff`,
//      `cancelled-during-<phase>`, `expiredWithoutProgress-<phase>` — precisely
//      so an operator can group and count them from a database pull. A
//      `String(describing: error)` is unattributable and ungroupable.
//
// WHERE IT COMES FROM, and why no existing test caught it. `coarsePassA` folds
// per-WINDOW failures into `failedWindowStatuses`; nothing about a window
// throws. But the pass PROLOGUE — `promptBudget()` → `coarseSchemaTokenCount()`
// and then `planPassA` → `tokenCount(_:)` — is a series of XPC round trips to
// the SAME daemon, made before the first window is planned, and those calls
// `throw`. A throttle there escapes `coarsePassA`, escapes `runJob`, and lands
// in `BackfillJobRunner`'s generic catch-all, which marks the job `failed` with
// `String(describing: error)`. Zero windows attempted, zero
// `semantic_scan_results` rows — exactly the 2.7 hours of silence the pull
// recorded on a healthy, charging device.
//
// NOT THE STREAMING FIX THE DAEMON'S MESSAGE ADVERTISES. The message's advice
// ("consider using non-streaming requests in background activities") is already
// followed everywhere: there is no `streamResponse` / `ResponseStream` /
// `PartiallyGenerated` call site anywhere in the app, and the
// `playhead-pmp9` DOC-GUARD in `FoundationModelClassifier.swift` forbids adding
// one. `streamingCanaryTests` below pins that so the guarantee is checkable
// rather than merely asserted in a comment.

import Foundation
import Testing
import XCTest

@testable import Playhead

#if canImport(FoundationModels)
import FoundationModels
#endif

// MARK: - The single definition

@Suite("playhead-kvs8: FMDaemonThrottle is the one definition of a daemon throttle")
struct FMDaemonThrottleClassificationTests {

    @available(iOS 26.0, *)
    @Test("an explicit rate-limit from the daemon is a throttle")
    func rateLimitedIsAThrottle() {
        #expect(FMDaemonThrottle.isThrottle(TestFMRuntimeFailure.rateLimited.error))
    }

    #if canImport(FoundationModels)
    @available(iOS 26.0, *)
    @Test("concurrentRequests is a throttle too — it is the daemon refusing load, not the model failing")
    func concurrentRequestsIsAThrottle() {
        let context = LanguageModelSession.GenerationError.Context(debugDescription: "kvs8")
        #expect(FMDaemonThrottle.isThrottle(LanguageModelSession.GenerationError.concurrentRequests(context)))
    }
    #endif

    @available(iOS 26.0, *)
    @Test("a refusal, a guardrail block, a context overflow, a deadline and a cancellation are NOT throttles")
    func nonThrottlesAreNotThrottles() {
        // Each of these has its own named cause and its own remedy. Folding any
        // of them into the throttle path would give a real, durable failure an
        // "it will heal on its own" disposition.
        #expect(!FMDaemonThrottle.isThrottle(TestFMRuntimeFailure.refusal.error))
        #expect(!FMDaemonThrottle.isThrottle(TestFMRuntimeFailure.guardrailViolation.error))
        #expect(!FMDaemonThrottle.isThrottle(TestFMRuntimeFailure.exceededContextWindow.error))
        #expect(!FMDaemonThrottle.isThrottle(FMInferenceTimeoutError(deadline: .seconds(300))))
        #expect(!FMDaemonThrottle.isThrottle(CancellationError()))
        #expect(!FMDaemonThrottle.isThrottle(ThrottleTestUnexpectedError()))
    }

    @Test("the pass-prologue defer cause is a NAMED token, distinct from pmp9's window token")
    func deferCausesAreNamedAndDistinct() {
        // pmp9's window-level token is preserved verbatim: a device pull that
        // greps for it must keep matching, and the two causes must not merge —
        // a window throttle means "this window lost its retries", a prologue
        // throttle means "the daemon refused before a single window was
        // planned". Different coverage stories, different remedies.
        #expect(FMDaemonThrottle.DeferCause.window.rawValue == "rateLimited-backoff")
        #expect(FMDaemonThrottle.DeferCause.passPrologue.rawValue == "rateLimited-prologue")
        #expect(FMDaemonThrottle.DeferCause.batchSibling.rawValue == "rateLimited-batchSibling")

        // No two causes share a token, and none of them is empty — an empty or
        // duplicated token is exactly as unattributable as the raw framework
        // string this bead removes.
        let tokens = FMDaemonThrottle.DeferCause.allCases.map(\.rawValue)
        #expect(Set(tokens).count == tokens.count)
        #expect(tokens.allSatisfy { !$0.isEmpty })

        // Every token is greppable as a rate-limit cause without knowing the
        // suffix, which is how an operator counts them in aggregate.
        #expect(tokens.allSatisfy { $0.hasPrefix("rateLimited-") })
    }

    @Test("the drain stops CONSECUTIVELY, not on a lifetime tally")
    func consecutiveStopThreshold() {
        // Mirrors `consecutiveInferenceTimeoutAbortThreshold`'s rule, which is
        // the pattern this repo has landed on: one throttle is an event, N
        // back-to-back with nothing succeeding in between is a device fact.
        #expect(FMDaemonThrottle.consecutiveDeferStopThreshold >= 2)
        #expect(!FMDaemonThrottle.shouldStopDraining(consecutiveThrottles: 0))
        #expect(!FMDaemonThrottle.shouldStopDraining(consecutiveThrottles: 1))
        #expect(FMDaemonThrottle.shouldStopDraining(
            consecutiveThrottles: FMDaemonThrottle.consecutiveDeferStopThreshold
        ))
        #expect(FMDaemonThrottle.shouldStopDraining(
            consecutiveThrottles: FMDaemonThrottle.consecutiveDeferStopThreshold + 1
        ))
    }
}

private struct ThrottleTestUnexpectedError: Error {}

// MARK: - The runner: a throttled PROLOGUE defers, it does not fail

@Suite("playhead-kvs8: a throttled coarse-pass prologue DEFERS with a named cause")
struct FMThrottledPrologueRunnerTests {

    private static let contextSize = 431
    private static let coarseSchemaTokenCount = 4

    private func windowingTokenRule() -> @Sendable (String) -> Int {
        { prompt in prompt.split(separator: "\n", omittingEmptySubsequences: false).count * 8 }
    }

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

    private func makeInputs(assetId: String) -> BackfillJobRunner.AssetInputs {
        let transcriptVersion = "tx-kvs8-v1"
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: [
                (0, 10, "Window zero editorial content about the topic."),
                (10, 20, "Window one sponsor break maybe present here."),
                (20, 30, "Window two back to the show conversation.")
            ]
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-kvs8",
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

    private func makeRunner(store: AnalysisStore, runtime: FoundationModelClassifier.Runtime) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(
                runtime: runtime,
                config: FoundationModelClassifier.Config(
                    safetyMarginTokens: 5,
                    coarseMaximumResponseTokens: 6,
                    refinementMaximumResponseTokens: 12,
                    interWindowPacingNanos: 0
                )
            ),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )
    }

    // MARK: The field row, reproduced and repaired

    @available(iOS 26.0, *)
    @Test("a daemon throttle in the coarse-pass PROLOGUE defers the job — it must not mark it failed")
    func throttledPrologueDefersRatherThanFails() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-kvs8-prologue"
        try await store.insertAsset(makeAsset(id: assetId))
        let fmRuntime = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            coarseSchemaTokenCountFailure: .rateLimited,
            tokenCountRule: windowingTokenRule()
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs(assetId: assetId))

        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))

        // NOT terminal. The daemon said "try again later"; the row must be
        // re-drivable, which `.failed` past `maxRetries` is not.
        #expect(row.status == .deferred, "throttled prologue left the job \(row.status.rawValue)")
        #expect(row.status != .failed)

        // The run reports the job as deferred so the caller's own accounting
        // agrees with the durable row.
        #expect(result.deferredJobIds.contains(jobId))
    }

    // The field row's other two columns get their OWN tests rather than riding
    // along in the one above. A mutation battery verdict is per-TEST: a rail
    // whose expectation names a test that asserts three unrelated claims is
    // killed by any one of them, so "the retry is not burned" and "the cause is
    // named" would both have been provable only by hand-reading issue lists.
    // One claim per test makes each rail's KILL mean exactly what it says.

    @available(iOS 26.0, *)
    @Test("a throttle does not spend a lifetime retry — the field row's retryCount=1")
    func throttleDoesNotBurnARetry() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-kvs8-retry"
        try await store.insertAsset(makeAsset(id: assetId))
        let fmRuntime = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            coarseSchemaTokenCountFailure: .rateLimited,
            tokenCountRule: windowingTokenRule()
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs(assetId: assetId))
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))

        // `AdmissionController.maxRetries` disqualifies a job permanently. It
        // exists for jobs failing on their own merits; a throttle says nothing
        // about the job, so three unlucky ones must not disqualify an episode
        // that was never scanned once.
        #expect(row.retryCount == 0, "a throttle spent a lifetime retry; got \(row.retryCount)")
    }

    @available(iOS 26.0, *)
    @Test("a throttled prologue records a NAMED cause, not the daemon's raw prose")
    func throttledPrologueRecordsANamedCause() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-kvs8-cause"
        try await store.insertAsset(makeAsset(id: assetId))
        let fmRuntime = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            coarseSchemaTokenCountFailure: .rateLimited,
            tokenCountRule: windowingTokenRule()
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs(assetId: assetId))
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))

        // The PROLOGUE cause, specifically — not pmp9's window cause. The two
        // read differently to an operator: this one means the daemon refused
        // before a single window was planned, so the episode is untouched.
        #expect(row.deferReason == FMDaemonThrottle.DeferCause.passPrologue.rawValue,
                "expected the prologue cause; got \(row.deferReason ?? "nil")")
        #expect(row.deferReason != FMDaemonThrottle.DeferCause.window.rawValue)

        // And never the framework's prose, which is what the field row carried.
        let reason = row.deferReason ?? ""
        #expect(!reason.contains("rate limited"), "deferReason is the raw daemon string: \(reason)")
        #expect(!reason.contains("streaming"))
    }

    @available(iOS 26.0, *)
    @Test("a throttled prologue leaves coverage accounting untouched — no cursor, no scan rows")
    func throttledPrologueDoesNotPenaliseCoverage() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-kvs8-coverage"
        try await store.insertAsset(makeAsset(id: assetId))
        let fmRuntime = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            coarseSchemaTokenCountFailure: .rateLimited,
            tokenCountRule: windowingTokenRule()
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs(assetId: assetId))
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))

        // Nothing was scanned, so nothing may claim to have been scanned. A
        // cursor here would mark unscanned audio as covered forever (the
        // pmp9 stranding shape); a "scanned and found clean" row would be
        // worse still.
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == nil)
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        // playhead-e75l R2 vacuity sweep: `allSatisfy` is TRUE on the empty
        // array this path actually produces, so the assertion could not fail.
        // `isEmpty` is the real state and also rejects a fabricated FAILURE
        // row, which `allSatisfy` permitted.
        #expect(scans.isEmpty,
                "a throttled prologue examined nothing, so it may claim nothing; got \(scans.map(\.status.rawValue))")
        #expect(await fmRuntime.coarseCallCount == 0, "the prologue throttle precedes every window")
    }

    @available(iOS 26.0, *)
    @Test("VACUITY CONTROL: a non-throttle prologue throw still marks the job failed and burns a retry")
    func nonThrottlePrologueStillFails() async throws {
        // Without this control, `throttledPrologueDefersRatherThanFails` would
        // also pass against an implementation that simply stopped marking jobs
        // failed at all. This proves the harness CAN still reach `.failed`
        // through the same seam, so the throttle test is about the throttle.
        let store = try await makeTestStore()
        let assetId = "asset-kvs8-vacuity"
        try await store.insertAsset(makeAsset(id: assetId))
        let fmRuntime = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            coarseSchemaTokenCountFailure: .guardrailViolation,
            tokenCountRule: windowingTokenRule()
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs(assetId: assetId))
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))

        #expect(row.status == .failed, "a non-throttle prologue throw is a real failure")
        #expect(row.retryCount == 1)
        #expect(row.deferReason != FMDaemonThrottle.DeferCause.passPrologue.rawValue)
    }

    @available(iOS 26.0, *)
    @Test("no-regression: a clean prologue still completes with a full-coverage cursor")
    func cleanPrologueStillCompletes() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-kvs8-clean"
        try await store.insertAsset(makeAsset(id: assetId))
        let fmRuntime = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule()
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs(assetId: assetId))
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))

        #expect(row.status == .complete)
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == 30)
        #expect(result.deferredJobIds.isEmpty)
        #expect(await fmRuntime.coarseCallCount == 3)
    }

    @available(iOS 26.0, *)
    @Test("a job deferred by a throttle is re-drivable and completes once the daemon relents")
    func throttledJobResumesOnceTheDaemonRelents() async throws {
        // The whole point of a defer over a `failed`: the episode is still
        // scannable. On the pre-kvs8 code this second run re-drives a `.failed`
        // row with retryCount already spent — and after three throttles it is
        // not re-driven at all.
        let store = try await makeTestStore()
        let assetId = "asset-kvs8-resume"
        try await store.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId)

        let throttled = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            coarseSchemaTokenCountFailure: .rateLimited,
            tokenCountRule: windowingTokenRule()
        )
        let run1 = try await makeRunner(store: store, runtime: throttled.runtime)
            .runPendingBackfill(for: inputs)
        let jobId = try #require(run1.admittedJobIds.first)

        let healthy = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule()
        )
        let run2 = try await makeRunner(store: store, runtime: healthy.runtime)
            .runPendingBackfill(for: inputs)

        #expect(run2.admittedJobIds.contains(jobId), "a throttled job must be re-driven")
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .complete)
        // The whole episode is scanned — the throttle cost nothing but time.
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == 30)
        #expect(await healthy.coarseCallCount == 3)
    }

    @available(iOS 26.0, *)
    @Test("a throttled batch leaves no job stranded in queued and none marked failed")
    func throttledBatchStrandsNothing() async throws {
        // A multi-phase plan enqueues three jobs. Whatever the drain does with
        // the siblings of a throttled job, it must not leave them `.queued`
        // (the H-1 stranding shape) and must not mark any of them `.failed`
        // (the field row). Both halves are false against pre-kvs8 code, which
        // marks the throttled job failed.
        let store = try await makeTestStore()
        let assetId = "asset-kvs8-batch"
        let transcriptVersion = "tx-kvs8-batch"
        try await store.insertAsset(makeAsset(id: assetId))

        let verbose = Array(
            repeating: "Detailed editorial discussion without sponsor language.",
            count: 60
        ).joined(separator: "\n")
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: (0..<40).map { index in
                let start = Double(index) * 10
                return (start, start + 10, "\(verbose)\nSegment \(index).")
            }
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-kvs8-batch",
            segments: segments,
            evidenceCatalog: EvidenceCatalog(
                analysisAssetId: assetId,
                transcriptVersion: transcriptVersion,
                entries: []
            ),
            transcriptVersion: transcriptVersion,
            plannerContext: CoveragePlannerContext(
                observedEpisodeCount: 20,
                stableRecall: true,
                isFirstEpisodeAfterCohortInvalidation: false,
                recallDegrading: false,
                sponsorDriftDetected: false,
                auditMissDetected: false,
                episodesSinceLastFullRescan: 1,
                periodicFullRescanIntervalEpisodes: 10
            )
        )

        let fmRuntime = TestFMRuntime(coarseSchemaTokenCountFailure: .rateLimited)
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )

        _ = try await runner.runPendingBackfill(for: inputs)

        // The plan is `targetedWithAudit`, whose three phases are enqueued with
        // deterministic ids; enumerate them rather than trusting the RunResult,
        // which is precisely the accounting under test.
        let phases: [BackfillJobPhase] = [
            .scanHarvesterProposals, .scanLikelyAdSlots, .scanRandomAuditWindows
        ]
        var rows: [BackfillJob] = []
        for (offset, phase) in phases.enumerated() {
            let jobId = BackfillJobRunner.makeJobIdForTesting(
                analysisAssetId: assetId,
                transcriptVersion: transcriptVersion,
                phase: phase,
                offset: offset
            )
            if let row = try await store.fetchBackfillJob(byId: jobId) {
                rows.append(row)
            }
        }
        #expect(rows.count == phases.count, "vacuity: the plan must have enqueued all three phases")
        #expect(rows.allSatisfy { $0.status != .failed },
                "a throttle marked a job failed: \(rows.map { "\($0.phase.rawValue)=\($0.status.rawValue)" })")
        #expect(rows.allSatisfy { $0.status != .queued },
                "a throttle stranded a job in queued: \(rows.map { "\($0.phase.rawValue)=\($0.status.rawValue)" })")
    }
}

// MARK: - The permissive lane: a throttle is not a permissive failure

@Suite("playhead-kvs8: the permissive lane records a throttle as a throttle")
struct FMThrottlePermissiveLaneTests {

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

    private func makeTriggeringInputs(assetId: String) -> BackfillJobRunner.AssetInputs {
        let transcriptVersion = "tx-kvs8-permissive"
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: [
                (0, 10, "Welcome to the show — today's sponsor mention."),
                (10, 20, "Ask your doctor about TRIGGERWORD for your condition."),
                (20, 30, "Back to the show.")
            ]
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-kvs8-permissive",
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

    @available(iOS 26.0, *)
    private func makeRunner(
        store: AnalysisStore,
        permissive: PermissiveAdClassifier
    ) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: TestFMRuntime().runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            sensitiveRouter: SensitiveWindowRouter(
                triggerRules: [PromptRedactor.RedactionRule(pattern: "TRIGGERWORD", isRegex: true)]
            ),
            permissiveClassifier: BackfillJobRunner.PermissiveClassifierBox(permissive)
        )
    }

    @available(iOS 26.0, *)
    @Test("a daemon throttle on the permissive path persists as rateLimited, never as a permissive refusal")
    func permissiveThrottleIsNotRelabelledAsRefusal() async throws {
        // The defensive catch-all in the coarse permissive dispatch files every
        // unrecognised error as `.permissiveRefusal` — "Apple's safety layer
        // declined this window" — for a call the safety layer never saw. Worse
        // than mislabelling: `.permissiveRefusal`'s retry policy is
        // `.persistFailure`, so a throttled window becomes a PERMANENT coverage
        // hole, while `.rateLimited`'s is `.backoffAndRetry`.
        let store = try await makeTestStore()
        let assetId = "asset-kvs8-perm-throttle"
        try await store.insertAsset(makeAsset(id: assetId))

        let permissive = PermissiveAdClassifier()
        await permissive.installArbitraryFaultInjectionForTesting { _ in
            TestFMRuntimeFailure.rateLimited.error
        }
        let runner = makeRunner(store: store, permissive: permissive)

        _ = try await runner.runPendingBackfill(for: makeTriggeringInputs(assetId: assetId))

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        #expect(scans.contains { $0.status == .rateLimited },
                "expected a rateLimited row; got \(scans.map(\.status.rawValue))")
        #expect(!scans.contains { $0.status == .permissiveRefusal },
                "a throttle was filed as a safety refusal")
        #expect(!scans.contains { $0.status == .permissiveDecodingFailure },
                "a throttle was filed as a decode failure")

        // And it must not be charged to the model's judgment telemetry. Nothing
        // about the model's judgment happened.
        let snapshot = await runner.snapshotPermissiveTelemetry()
        #expect(snapshot.refusal == 0, "a throttle bumped the permissive refusal counter")
        #expect(snapshot.decodingFailure == 0, "a throttle bumped the permissive decode counter")
        #expect(snapshot.contextOverflow == 0)
    }

    @available(iOS 26.0, *)
    @Test("VACUITY CONTROL: the same seam still files a genuine unexpected error as permissiveRefusal")
    func nonThrottleStillFilesAsPermissiveRefusal() async throws {
        // Proves the assertions above are about the THROTTLE, not about the
        // defensive arm having been disabled wholesale.
        let store = try await makeTestStore()
        let assetId = "asset-kvs8-perm-vacuity"
        try await store.insertAsset(makeAsset(id: assetId))

        let permissive = PermissiveAdClassifier()
        await permissive.installArbitraryFaultInjectionForTesting { _ in
            ThrottleTestUnexpectedError()
        }
        let runner = makeRunner(store: store, permissive: permissive)

        _ = try await runner.runPendingBackfill(for: makeTriggeringInputs(assetId: assetId))

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        #expect(scans.contains { $0.status == .permissiveRefusal },
                "vacuity control: the defensive arm no longer files anything; got \(scans.map(\.status.rawValue))")
        let snapshot = await runner.snapshotPermissiveTelemetry()
        #expect(snapshot.refusal >= 1)
    }

    @available(iOS 26.0, *)
    @Test("a throttle Reason maps to rateLimited and is charged to no permissive counter")
    func throttleReasonMapsToRateLimited() {
        #expect(
            FoundationModelClassifier.permissiveStatus(for: .rateLimited) == .rateLimited
        )
        // The three genuine permissive failures keep their own statuses.
        #expect(FoundationModelClassifier.permissiveStatus(for: .permissiveRefusal) == .permissiveRefusal)
        #expect(FoundationModelClassifier.permissiveStatus(for: .permissiveDecodingFailure) == .permissiveDecodingFailure)
        #expect(FoundationModelClassifier.permissiveStatus(for: .permissiveContextOverflow) == .permissiveContextOverflow)

        // A throttle leaves every permissive counter exactly where it was.
        var counts = PermissiveFailureCounts.zero
        counts.increment(reason: .rateLimited)
        #expect(counts == PermissiveFailureCounts.zero, "a throttle was charged to a permissive counter")

        // Vacuity: the same counter DOES move for a real permissive failure.
        var moved = PermissiveFailureCounts.zero
        moved.increment(reason: .permissiveRefusal)
        #expect(moved != PermissiveFailureCounts.zero)
    }

    @Test("rateLimited is retried with backoff and is window-scoped; it never examined the window")
    func rateLimitedRetryPolicy() {
        // The properties that make a defer meaningful: a throttled window is
        // retried rather than persisted as terminal, does not poison its
        // siblings, and is never counted as audio that was screened.
        #expect(SemanticScanStatus.rateLimited.retryPolicy == .backoffAndRetry)
        #expect(SemanticScanStatus.rateLimited.failureScope == .window)
        #expect(!SemanticScanStatus.rateLimited.didExamineWindow)
        #expect(!SemanticScanStatus.rateLimited.isSafetyBlock)
    }
}

// MARK: - The readiness probe: a throttle is not an unusable model

@Suite("playhead-kvs8: a throttled readiness probe must not be cached as an unusable model")
struct FMThrottleUsabilityProbeTests {

    @available(iOS 26.0, *)
    @Test("a throttle is not a usability verdict, so it must not be cached")
    func throttleIsNotCachedAsUnusable() {
        // `probeIfNeeded` gates the WHOLE ad-detection lane: `liveRuntime`'s
        // `availabilityStatus` awaits it before `coarsePassA` plans a window,
        // and a `false` verdict is cached for `falseCacheTTL` (15 minutes).
        // Caching a throttle therefore converts one momentary daemon refusal
        // into a quarter-hour with no FM scanning at all — on a device that is
        // fine. Every OTHER probe failure is a real usability signal and must
        // still be cached.
        #expect(!FoundationModelsUsabilityProbe.shouldCacheFailure(for: TestFMRuntimeFailure.rateLimited.error))
        #expect(FoundationModelsUsabilityProbe.shouldCacheFailure(for: TestFMRuntimeFailure.refusal.error))
        #expect(FoundationModelsUsabilityProbe.shouldCacheFailure(for: TestFMRuntimeFailure.guardrailViolation.error))
        #expect(FoundationModelsUsabilityProbe.shouldCacheFailure(for: FMInferenceTimeoutError(deadline: .seconds(300))))
        #expect(FoundationModelsUsabilityProbe.shouldCacheFailure(for: ThrottleTestUnexpectedError()))
    }
}

// MARK: - Source canaries

/// The streaming claim, and the guard on the cache call, are both structural:
/// no runtime test on the simulator can reach the live daemon, so the only
/// checkable form of "we never stream from a background request" is the source
/// itself. Same technique, and same rationale, as `FMUnboundedCallCanaryTests`.
final class FMDaemonThrottleCanaryTests: XCTestCase {

    private func productionRoot() -> URL {
        var root = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        for _ in 0..<3 {
            root.deleteLastPathComponent()
        }
        return root.appendingPathComponent("Playhead", isDirectory: true)
    }

    private func swiftFiles() throws -> [URL] {
        let root = productionRoot()
        guard let walker = FileManager.default.enumerator(
            at: root,
            includingPropertiesForKeys: nil
        ) else {
            return []
        }
        return walker.compactMap { $0 as? URL }.filter { $0.pathExtension == "swift" }
    }

    /// Strips whole-line comments — the `playhead-pmp9` DOC-GUARD names
    /// `streamResponse` in prose precisely to forbid it, and a canary that
    /// fires on its own rationale is a canary people delete.
    private func codeLines(_ url: URL) throws -> [String] {
        try String(contentsOf: url, encoding: .utf8)
            .split(separator: "\n", omittingEmptySubsequences: false)
            .map(String.init)
            .filter { !$0.trimmingCharacters(in: .whitespaces).hasPrefix("//") }
    }

    /// The daemon's own advice, pinned. Apple's rate-limit message says
    /// background requests should not stream; this asserts none of ours does.
    func testNoFoundationModelsStreamingCallSiteExistsInProduction() throws {
        let needles = ["streamResponse(", "ResponseStream", "PartiallyGenerated"]
        var offenders: [String] = []
        let files = try swiftFiles()

        // Vacuity guard: if the walk finds nothing, the canary is inert.
        XCTAssertGreaterThan(files.count, 100, "source walk found only \(files.count) Swift files")

        for file in files {
            for (index, line) in try codeLines(file).enumerated() where needles.contains(where: line.contains) {
                offenders.append("\(file.lastPathComponent):\(index + 1): \(line.trimmingCharacters(in: .whitespaces))")
            }
        }

        XCTAssertTrue(
            offenders.isEmpty,
            """
            Foundation Models STREAMING call site(s) found. The daemon rate-limits \
            streaming responses issued from background requests and says so in its \
            own error text; the whole ad-detection lane runs under a BGProcessingTask. \
            See the playhead-pmp9 DOC-GUARD in FoundationModelClassifier.swift.
            \(offenders.joined(separator: "\n"))
            """
        )
    }

    /// The probe's `cache(usable: false)` on the failure path must be guarded by
    /// the throttle predicate rather than called unconditionally.
    func testProbeFailureCacheIsGuardedByTheThrottlePredicate() throws {
        let url = productionRoot()
            .appendingPathComponent("Services/Capabilities/FoundationModelsUsabilityProbe.swift")
        let lines = try codeLines(url)

        let cacheFalseSites = lines.enumerated().filter { $0.element.contains("cache(usable: false)") }
        XCTAssertEqual(
            cacheFalseSites.count,
            1,
            "expected exactly one `cache(usable: false)` call; found \(cacheFalseSites.count). If it moved, move this canary with it."
        )

        let index = try XCTUnwrap(cacheFalseSites.first?.offset)
        let window = lines[max(0, index - 6)...index]
        XCTAssertTrue(
            window.contains { $0.contains("shouldCacheFailure(") },
            "`cache(usable: false)` is not guarded by `shouldCacheFailure(for:)` — a throttled probe would be cached as an unusable model for falseCacheTTL."
        )
    }
}
