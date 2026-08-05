// FMDaemonMetadataStallTests.swift
// playhead-e75l — a TOKENIZER round trip that times out is a daemon condition,
// not a verdict on the job.
//
// THE FIELD ROW THIS FILE EXISTS FOR. From the 2026-08-03 device pull,
// `backfill_jobs`, asset 0C2FC22E, phase `fullEpisodeScan`:
//
//     status      = failed
//     retryCount  = 1
//     deferReason = "FMInferenceTimeoutError(deadline: 30.0 seconds)"
//     createdAt   = 2026-08-03 15:21:38
//     updatedAt   = 2026-08-03 15:22:31      <- 52.2 s later
//
// and zero `semantic_scan_results` rows for that asset. Fifty-two seconds is
// the prologue: a healthy coarse pass takes 12–45 minutes, and the episode had
// not been screened at all.
//
// `30.0 seconds` is `FMInferenceDeadline.metadata`, which bounds
// `SystemLanguageModel.tokenCount(for:)` and the `@Generable` schema sizings —
// XPC round trips that do no generation whatsoever. `FMInferenceDeadline
// .standard` (300 s) is the bound on inference. So this row records the daemon
// failing to answer a tokenizer question in thirty seconds, which is evidence
// about the DAEMON and says nothing about this episode, this transcript or this
// model window.
//
// WHY IT WAS RECORDED AS A FAILURE. Exactly the escape playhead-kvs8 documented
// for the throttle sibling. `promptBudget()` awaits `coarseSchemaTokenCount()`
// and `planPassA` awaits `tokenCount(_:)` once per candidate window, all before
// the first window is planned and all of them `throws`. Window-scoped inference
// timeouts never throw — `coarsePassA` and `refinePassB` fold them into
// `failedWindowStatuses` and RETURN, because `.inferenceTimeout` has
// `failureScope == .window` — so the ONLY `FMInferenceTimeoutError` that can
// reach `BackfillJobRunner`'s drain loop is a metadata one. It landed in the
// generic catch-all, whose job is to record genuine failures:
//
//     try await store.markBackfillJobFailed(
//         jobId: job.jobId,
//         reason: String(describing: error),
//         retryCount: job.retryCount + 1
//     )
//
// which is the field row, field for field.
//
// WHAT THE COST IS. `AdmissionController.maxRetries` is 3 and
// `runPendingBackfill` refuses to re-enqueue a row at or above it. Three wedged
// -daemon moments — no relationship to each other, to the episode, or to
// anything the app did — disqualify an episode from ad scanning forever. Both
// terminal `failed` rows in the 2026-08-03 pull are daemon conditions (one
// throttle, one metadata timeout); zero of two are the job's own merits.
//
// THE DEADLINE IS NOT THE DEFECT. 30 s is two orders of magnitude above any
// honest tokenizer cost and must stay 30 s. What is wrong is the DISPOSITION.

import Foundation
import Testing

@testable import Playhead

// MARK: - The single definition

@Suite("playhead-e75l: FMDaemonRefusal is the one definition of a daemon condition")
struct FMDaemonRefusalDefinitionTests {

    @Test("a metadata-deadline timeout is a refusal; a standard-deadline one is not")
    func theDiscriminatorIsTheBudget() {
        // Same type, same initializer, different budget. If the predicate keyed
        // on the type, these two lines could not disagree.
        #expect(FMDaemonRefusal.isMetadataStall(FMInferenceTimeoutError(deadline: FMInferenceDeadline.metadata)))
        #expect(!FMDaemonRefusal.isMetadataStall(FMInferenceTimeoutError(deadline: FMInferenceDeadline.standard)))
        #expect(FMDaemonRefusal.classify(FMInferenceTimeoutError(deadline: FMInferenceDeadline.metadata)) == .metadataStall)
        #expect(FMDaemonRefusal.classify(FMInferenceTimeoutError(deadline: FMInferenceDeadline.standard)) == nil)
    }

    @Test("the two budgets are actually distinct — the discriminator is not vacuous")
    func theTwoBudgetsDiffer() {
        // Without this, every expectation above would hold trivially if
        // someone set `metadata = standard`, and the runner would defer every
        // inference timeout while the suite stayed green.
        #expect(FMInferenceDeadline.metadata != FMInferenceDeadline.standard)
        #expect(FMInferenceDeadline.metadata == .seconds(30), "the field row's deadline")
    }

    @available(iOS 26.0, *)
    @Test("a throttle is still a refusal — kvs8's arm is subsumed, not replaced")
    func aThrottleIsStillARefusal() {
        #expect(FMDaemonRefusal.classify(TestFMRuntimeFailure.rateLimited.error) == .throttle)
    }

    @available(iOS 26.0, *)
    @Test("nothing else is a refusal")
    func nonRefusalsAreNotRefusals() {
        // Each of these has its own named cause and its own remedy. Folding any
        // of them in would give a real, durable failure an "it will heal on its
        // own" disposition and an unbounded retry.
        #expect(FMDaemonRefusal.classify(TestFMRuntimeFailure.refusal.error) == nil)
        #expect(FMDaemonRefusal.classify(TestFMRuntimeFailure.guardrailViolation.error) == nil)
        #expect(FMDaemonRefusal.classify(TestFMRuntimeFailure.exceededContextWindow.error) == nil)
        #expect(FMDaemonRefusal.classify(CancellationError()) == nil)
        #expect(FMDaemonRefusal.classify(FMNoProgressError(interval: .seconds(180), consecutiveIntervals: 3)) == nil)
        #expect(FMDaemonRefusal.classify(
            AnalysisStoreError.invalidStateTransition(jobId: "e75l", fromStatus: "failed", toStatus: "running")
        ) == nil)
    }

    @Test("every cause is a NAMED token, and no two kinds share one")
    func causesAreNamedAndDistinct() {
        #expect(FMDaemonRefusal.metadataStall.passPrologueCause == "inferenceTimeout-metadata")
        #expect(FMDaemonRefusal.metadataStall.batchSiblingCause == "inferenceTimeout-batchSibling")

        // kvs8's tokens are preserved byte-for-byte: device pulls and support
        // bundles already grep for them.
        #expect(FMDaemonRefusal.throttle.passPrologueCause == "rateLimited-prologue")
        #expect(FMDaemonRefusal.throttle.batchSiblingCause == "rateLimited-batchSibling")

        let tokens = FMDaemonRefusal.allCases.flatMap { [$0.passPrologueCause, $0.batchSiblingCause] }
        #expect(Set(tokens).count == tokens.count, "two causes share a token: \(tokens)")
        #expect(tokens.allSatisfy { !$0.isEmpty })

        // The two kinds must not share a prefix. An operator counting
        // `rateLimited-` is counting rate limits; a stall answering to that
        // prefix would inflate the count with an event that never happened.
        #expect(!FMDaemonRefusal.metadataStall.passPrologueCause.hasPrefix("rateLimited-"))
        #expect(!FMDaemonRefusal.metadataStall.batchSiblingCause.hasPrefix("rateLimited-"))
        #expect(FMDaemonRefusal.metadataStall.logEvent != FMDaemonRefusal.throttle.logEvent)
    }

    @Test("the drain-stop rule is kvs8's rule, shared rather than duplicated")
    func stopRuleIsShared() {
        for consecutive in 0...(FMDaemonThrottle.consecutiveDeferStopThreshold + 2) {
            #expect(
                FMDaemonRefusal.shouldStopDraining(consecutiveRefusals: consecutive)
                    == FMDaemonThrottle.shouldStopDraining(consecutiveThrottles: consecutive),
                "the two rules disagreed at \(consecutive)"
            )
        }
        #expect(!FMDaemonRefusal.shouldStopDraining(consecutiveRefusals: 1))
        #expect(FMDaemonRefusal.shouldStopDraining(
            consecutiveRefusals: FMDaemonThrottle.consecutiveDeferStopThreshold
        ))
    }
}

// MARK: - The runner: a stalled metadata round trip DEFERS, it does not fail

@Suite("playhead-e75l: a metadata-deadline timeout in the prologue DEFERS with a named cause")
struct FMDaemonMetadataStallRunnerTests {

    private static let contextSize = 431
    private static let coarseSchemaTokenCount = 4

    /// The token an operator greps a device pull for. Taken from the production
    /// symbol here; its literal spelling is pinned in
    /// `FMDaemonRefusalDefinitionTests.causesAreNamedAndDistinct`, so a rename
    /// fails there rather than silently changing what these tests assert.
    private static let expectedCause = FMDaemonRefusal.metadataStall.passPrologueCause

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
        let transcriptVersion = "tx-e75l-v1"
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
            podcastId: "podcast-e75l",
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

    private func makeStalledRuntime() -> TestFMRuntime {
        TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            coarseSchemaTokenCountFailure: .metadataTimeout,
            tokenCountRule: windowingTokenRule()
        )
    }

    // The field row's three columns get three tests rather than one test with
    // three expectations: a mutation-battery verdict is per-TEST, so a rail
    // whose expectation names a test asserting three unrelated claims is killed
    // by any one of them. One claim per test makes each KILL mean what it says.

    @available(iOS 26.0, *)
    @Test("a metadata-deadline timeout in the PROLOGUE defers the job — it must not mark it failed")
    func metadataStallDefersRatherThanFails() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-e75l-defer"
        try await store.insertAsset(makeAsset(id: assetId))
        let runner = makeRunner(store: store, runtime: makeStalledRuntime().runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs(assetId: assetId))

        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))

        // NOT terminal. The daemon was wedged for thirty seconds; that is a
        // moment, and the row must still be re-drivable when it passes.
        #expect(row.status == .deferred, "a metadata stall left the job \(row.status.rawValue)")
        #expect(row.status != .failed)

        // The run's own accounting agrees with the durable row.
        #expect(result.deferredJobIds.contains(jobId))
    }

    @available(iOS 26.0, *)
    @Test("a metadata stall does not spend a lifetime retry — the field row's retryCount=1")
    func metadataStallDoesNotBurnARetry() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-e75l-retry"
        try await store.insertAsset(makeAsset(id: assetId))
        let runner = makeRunner(store: store, runtime: makeStalledRuntime().runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs(assetId: assetId))
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))

        // `AdmissionController.maxRetries` disqualifies an episode permanently.
        // That budget exists for jobs failing on their own merits; three wedged
        // -daemon moments must not spend it.
        #expect(row.retryCount == 0, "a metadata stall spent a lifetime retry; got \(row.retryCount)")
        #expect(row.retryCount < AdmissionController.maxRetries)
    }

    @available(iOS 26.0, *)
    @Test("a metadata stall records a NAMED cause, not the error's Swift description")
    func metadataStallRecordsANamedCause() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-e75l-cause"
        try await store.insertAsset(makeAsset(id: assetId))
        let runner = makeRunner(store: store, runtime: makeStalledRuntime().runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs(assetId: assetId))
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))

        #expect(row.deferReason == Self.expectedCause,
                "expected the metadata-stall cause; got \(row.deferReason ?? "nil")")

        // Never the Swift description, which is what the field row carried and
        // which no operator can group or count.
        let reason = row.deferReason ?? ""
        #expect(!reason.contains("FMInferenceTimeoutError"), "deferReason is the raw Swift description: \(reason)")
        #expect(!reason.contains("30.0 seconds"))

        // And never a rate-limit token: the daemon did not say "try again
        // later", it said nothing at all. Merging the two would make the
        // throttle count in a device pull unreadable.
        #expect(!reason.hasPrefix("rateLimited-"))
    }

    @available(iOS 26.0, *)
    @Test("a metadata stall leaves coverage accounting untouched — no cursor, no scan rows")
    func metadataStallDoesNotPenaliseCoverage() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-e75l-coverage"
        try await store.insertAsset(makeAsset(id: assetId))
        let fmRuntime = makeStalledRuntime()
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs(assetId: assetId))
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))

        // Nothing was screened, so nothing may claim to have been. A cursor here
        // would mark unscanned audio covered forever — the pmp9 stranding shape
        // this whole arc exists to kill.
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == nil)
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        #expect(scans.allSatisfy { $0.status != .success && $0.status != .noAds },
                "a stalled prologue examined nothing; got \(scans.map(\.status.rawValue))")
        #expect(await fmRuntime.coarseCallCount == 0, "the stall precedes every window")
    }

    @available(iOS 26.0, *)
    @Test("DISCRIMINATOR: a STANDARD-deadline timeout through the same seam still fails and burns a retry")
    func standardDeadlineTimeoutThroughTheSameSeamStillFails() async throws {
        // The load-bearing control. `.inferenceTimeout` and `.metadataTimeout`
        // are the SAME Swift type thrown through the SAME seam; only the
        // `deadline` differs. If the runner keyed on the type instead of the
        // budget, this test and `metadataStallDefersRatherThanFails` could not
        // both pass — so together they prove the discriminator is which BOUND
        // elapsed, which is the only thing that distinguishes "the daemon did
        // not complete a tokenizer round trip" from "the model did not answer".
        //
        // Keeping `.standard` on the failing side is deliberate rather than
        // incidental: an inference timeout IS evidence about the model, it is
        // already escalated in-window by
        // `consecutiveInferenceTimeoutAbortThreshold`, and deferring it would
        // buy an unbounded retry loop costing up to 300 s per window per drain.
        let store = try await makeTestStore()
        let assetId = "asset-e75l-standard"
        try await store.insertAsset(makeAsset(id: assetId))
        let fmRuntime = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            coarseSchemaTokenCountFailure: .inferenceTimeout,
            tokenCountRule: windowingTokenRule()
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs(assetId: assetId))
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))

        #expect(row.status == .failed, "a standard-deadline timeout is not a metadata stall")
        #expect(row.retryCount == 1)
        #expect(row.deferReason != Self.expectedCause)
    }

    @available(iOS 26.0, *)
    @Test("VACUITY CONTROL: a non-timeout prologue throw still marks the job failed and burns a retry")
    func nonTimeoutPrologueStillFails() async throws {
        // Without this, `metadataStallDefersRatherThanFails` would also pass
        // against an implementation that simply stopped marking jobs failed.
        // This proves the harness can still reach `.failed` through the same
        // seam, so the stall test is about the stall.
        let store = try await makeTestStore()
        let assetId = "asset-e75l-vacuity"
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

        #expect(row.status == .failed, "a non-timeout prologue throw is a real failure")
        #expect(row.retryCount == 1)
        #expect(row.deferReason != Self.expectedCause)
    }

    @available(iOS 26.0, *)
    @Test("no-regression: a clean prologue still completes with a full-coverage cursor")
    func cleanPrologueStillCompletes() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-e75l-clean"
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
    @Test("a job deferred by a metadata stall is re-drivable and completes once the daemon answers")
    func stalledJobResumesOnceTheDaemonAnswers() async throws {
        // The whole point of a defer over a `failed`: the episode is still
        // scannable. Against the pre-e75l code the second run re-drives a
        // `.failed` row with a retry already spent — and after three stalls it
        // is not re-driven at all.
        let store = try await makeTestStore()
        let assetId = "asset-e75l-resume"
        try await store.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId)

        let run1 = try await makeRunner(store: store, runtime: makeStalledRuntime().runtime)
            .runPendingBackfill(for: inputs)
        let jobId = try #require(run1.admittedJobIds.first)

        let healthy = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule()
        )
        let run2 = try await makeRunner(store: store, runtime: healthy.runtime)
            .runPendingBackfill(for: inputs)

        #expect(run2.admittedJobIds.contains(jobId), "a stalled job must be re-driven")
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .complete)
        // The whole episode is scanned — the stall cost nothing but time.
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == 30)
        #expect(await healthy.coarseCallCount == 3)
    }

    @available(iOS 26.0, *)
    @Test("THE COST, DIRECTLY: three metadata stalls must not disqualify an episode forever")
    func threeStallsDoNotDisqualifyTheEpisode() async throws {
        // `runPendingBackfill` refuses to re-enqueue a row whose `retryCount`
        // has reached `AdmissionController.maxRetries` (3). Drive exactly that
        // many stalled runs and then a healthy one: pre-e75l the healthy run
        // finds nothing to admit and the episode is unscannable for the life of
        // the install. This is the reach cost the bead is about, asserted end to
        // end rather than inferred from `retryCount`.
        let store = try await makeTestStore()
        let assetId = "asset-e75l-lifetime"
        try await store.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId)

        for _ in 0..<AdmissionController.maxRetries {
            _ = try await makeRunner(store: store, runtime: makeStalledRuntime().runtime)
                .runPendingBackfill(for: inputs)
        }

        let healthy = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            tokenCountRule: windowingTokenRule()
        )
        let recovery = try await makeRunner(store: store, runtime: healthy.runtime)
            .runPendingBackfill(for: inputs)

        #expect(!recovery.admittedJobIds.isEmpty,
                "the episode was disqualified by \(AdmissionController.maxRetries) daemon stalls")
        let jobId = try #require(recovery.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .complete)
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == 30)
    }

    @available(iOS 26.0, *)
    @Test("a stalled batch leaves no job stranded in queued and none marked failed")
    func stalledBatchStrandsNothing() async throws {
        // A multi-phase plan enqueues three jobs. Whatever the drain does with
        // the siblings of a stalled job, it must not leave them `.queued` (the
        // H-1 stranding shape) and must not mark any of them `.failed` (the
        // field row).
        let store = try await makeTestStore()
        let assetId = "asset-e75l-batch"
        let transcriptVersion = "tx-e75l-batch"
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
            podcastId: "podcast-e75l-batch",
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

        let fmRuntime = TestFMRuntime(coarseSchemaTokenCountFailure: .metadataTimeout)
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
                "a metadata stall marked a job failed: \(rows.map { "\($0.phase.rawValue)=\($0.status.rawValue)" })")
        #expect(rows.allSatisfy { $0.status != .queued },
                "a metadata stall stranded a job in queued: \(rows.map { "\($0.phase.rawValue)=\($0.status.rawValue)" })")

        // Every sibling the drain swept must carry a token that names what
        // actually stopped the drain. `rateLimited-batchSibling` here would say
        // the daemon rate-limited us, which it did not.
        let siblingReasons = rows.compactMap(\.deferReason).filter { $0.contains("batchSibling") }
        #expect(siblingReasons.allSatisfy { !$0.hasPrefix("rateLimited-") },
                "a metadata stall deferred siblings as rate-limited: \(siblingReasons)")
    }
}
