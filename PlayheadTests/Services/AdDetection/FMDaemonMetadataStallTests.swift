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
import XCTest

@testable import Playhead

// MARK: - The single definition

@Suite("playhead-e75l: FMDaemonRefusal is the one definition of a daemon condition")
struct FMDaemonRefusalDefinitionTests {

    @Test("a metadata-deadline timeout is a refusal — a standard-deadline one is not")
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
        #expect(FMDaemonRefusal.metadataStall.passPrologueCause == "metadataStall-refused")
        #expect(FMDaemonRefusal.metadataStall.batchSiblingCause == "metadataStall-batchSibling")

        // kvs8's tokens are preserved byte-for-byte: device pulls and support
        // bundles already grep for them.
        #expect(FMDaemonRefusal.throttle.passPrologueCause == "rateLimited-prologue")
        #expect(FMDaemonRefusal.throttle.batchSiblingCause == "rateLimited-batchSibling")

        let tokens = FMDaemonRefusal.allCases.flatMap { [$0.passPrologueCause, $0.batchSiblingCause] }
        #expect(tokens.count == 4, "vacuity: the collection assertions below run on \(tokens.count) tokens")
        #expect(Set(tokens).count == tokens.count, "two causes share a token: \(tokens)")
        #expect(tokens.allSatisfy { !$0.isEmpty })

        // The two kinds must not share a prefix. An operator counting
        // `rateLimited-` is counting rate limits; a stall answering to that
        // prefix would inflate the count with an event that never happened.
        #expect(!FMDaemonRefusal.metadataStall.passPrologueCause.hasPrefix("rateLimited-"))
        #expect(!FMDaemonRefusal.metadataStall.batchSiblingCause.hasPrefix("rateLimited-"))
        #expect(FMDaemonRefusal.metadataStall.logEvent != FMDaemonRefusal.throttle.logEvent)

        // R2-Fix1 — and not a FOREIGN family either. `inferenceTimeout-` is
        // taken: playhead-8d5r writes `inferenceTimeout-noProgress` for a
        // coarse pass aborted by a run of 300 s inference timeouts, which the
        // runner documents as "the model is not answering on this device". The
        // first spelling of these two tokens answered to that prefix, so a
        // device pull could not tell a wedged tokenizer from a mute model —
        // the one distinction this whole type exists to hold. The source
        // canary below anchors the foreign token; this pins the rule.
        #expect(!FMDaemonRefusal.metadataStall.passPrologueCause.hasPrefix("inferenceTimeout-"))
        #expect(!FMDaemonRefusal.metadataStall.batchSiblingCause.hasPrefix("inferenceTimeout-"))
        #expect(tokens.allSatisfy { !$0.hasPrefix("cancelled-") })
    }

    @Test("R1-Fix1: the DRAIN-STOP event is named per kind too, and kvs8's spelling is preserved")
    func drainStopEventIsNamedPerKind() {
        // A log EVENT NAME is the unit a support-bundle grep counts, so it is
        // subject to exactly the rule the defer tokens are: an operator
        // counting `drain_stopped_by_throttle` is counting rate limits. Before
        // this fix a drain stopped by two wedged tokenizer round trips emitted
        // kvs8's event verbatim, with only a `cause=` field to disagree with
        // its own name.
        #expect(FMDaemonRefusal.throttle.drainStoppedEvent == "fm.backfill.drain_stopped_by_throttle")
        #expect(FMDaemonRefusal.metadataStall.drainStoppedEvent
            == "fm.backfill.drain_stopped_by_daemon_metadata_stall")
        #expect(!FMDaemonRefusal.metadataStall.drainStoppedEvent.contains("throttle"))

        // R5-Fix2: the PER-JOB event names are pinned as literals too, and until
        // R5 neither was. `drainStoppedEvent` was pinned for BOTH kinds three
        // lines up while `logEvent` had only
        // `metadataStall.logEvent != throttle.logEvent` a few lines down — which
        // a rename satisfies, because renaming one still leaves them different.
        //
        // The asymmetry mattered because this bead MOVED `fm.backfill.job_throttled`
        // out of a hard-coded literal in `BackfillJobRunner`'s log line and into
        // this enum. It is kvs8's shipped event name and what an existing
        // support-bundle grep counts, so a mis-spelling in the move makes that
        // grep read zero with nothing anywhere going red — the source canary
        // below derives its forbidden set from `allCases`, so it dutifully
        // forbids whatever the NEW spelling happens to be. Probe PB1 renamed
        // this to `fm.backfill.job_daemon_throttled` and all eight suites in
        // this bead's scope passed. Mutation DR17.
        #expect(FMDaemonRefusal.throttle.logEvent == "fm.backfill.job_throttled")
        #expect(FMDaemonRefusal.metadataStall.logEvent == "fm.backfill.job_daemon_metadata_stalled")

        let events = FMDaemonRefusal.allCases.flatMap { [$0.logEvent, $0.drainStoppedEvent] }
        #expect(Set(events).count == events.count, "two events share a name: \(events)")
    }

    @Test("the drain-stop rule is kvs8's rule, shared rather than duplicated")
    func stopRuleIsShared() {
        for consecutive in 0...(FMDaemonThrottle.consecutiveDeferStopThreshold + 2) {
            #expect(
                FMDaemonRefusal.shouldStopDraining(consecutiveRefusals: consecutive)
                    == FMDaemonThrottle.shouldStopDraining(consecutiveDaemonRefusals: consecutive),
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

    /// A `targetedWithAudit` fixture whose THREE phases all reach the daemon.
    ///
    /// R1: the batch fixture inherited from kvs8 passes an EMPTY
    /// `EvidenceCatalog`, and `TargetedWindowNarrower` narrows
    /// `.scanHarvesterProposals` on `evidenceLineRefs` — so that phase planned
    /// zero windows, never made a prologue round trip, and COMPLETED. A job
    /// that reached the daemon successfully resets `consecutiveDaemonRefusals`,
    /// so the drain could only ever reach the stop threshold on its LAST job,
    /// where there is no sibling left to sweep. The sibling-token assertions
    /// were therefore running against an empty array. Building a real catalog
    /// from sponsor-bearing lines gives the harvester phase windows, which puts
    /// a third refusable job ahead of the threshold and makes the sweep
    /// observable.
    private func makeBatchInputs(assetId: String, transcriptVersion: String) -> BackfillJobRunner.AssetInputs {
        let verbose = Array(
            repeating: "Detailed editorial discussion without sponsor language.",
            count: 60
        ).joined(separator: "\n")
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: (0..<40).map { index in
                let start = Double(index) * 10
                let sponsor = index % 4 == 0
                    ? "\nThis episode is brought to you by our sponsor — use promo code SAVE at checkout."
                    : ""
                return (start, start + 10, "\(verbose)\nSegment \(index).\(sponsor)")
            }
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-e75l-batch",
            segments: segments,
            evidenceCatalog: EvidenceCatalogBuilder.build(
                atoms: segments.flatMap(\.atoms),
                analysisAssetId: assetId,
                transcriptVersion: transcriptVersion
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
    }

    /// The three `targetedWithAudit` phases, in the order they are enqueued.
    private static let batchPhases: [BackfillJobPhase] = [
        .scanHarvesterProposals, .scanLikelyAdSlots, .scanRandomAuditWindows
    ]

    private func fetchBatchRows(
        store: AnalysisStore,
        assetId: String,
        transcriptVersion: String
    ) async throws -> [BackfillJob] {
        var rows: [BackfillJob] = []
        for (offset, phase) in Self.batchPhases.enumerated() {
            let jobId = BackfillJobRunner.makeJobId(
                analysisAssetId: assetId,
                phase: phase,
                offset: offset
            )
            if let row = try await store.fetchBackfillJob(byId: jobId) {
                rows.append(row)
            }
        }
        return rows
    }

    private func makeBatchRunner(store: AnalysisStore, runtime: FoundationModelClassifier.Runtime) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
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

        // playhead-ezmv: the cause now carries the census reading from the
        // instant the stalled call was issued, in parentheses AFTER the token
        // so `grep -c 'metadataStall-refused'` still counts these rows. The
        // fixture's error carries the default reading (no peers), so the
        // suffix is deterministic here; the peers>0 path has its own test
        // below.
        #expect(row.deferReason == Self.expectedCause + "(peers=0)",
                "expected the metadata-stall cause with its census suffix; got \(row.deferReason ?? "nil")")
        #expect(row.deferReason?.hasPrefix(Self.expectedCause) == true,
                "the greppable token must stay a PREFIX of the recorded cause")

        // Never the Swift description, which is what the field row carried and
        // which no operator can group or count.
        let reason = row.deferReason ?? ""
        #expect(!reason.contains("FMInferenceTimeoutError"), "deferReason is the raw Swift description: \(reason)")
        #expect(!reason.contains("30.0 seconds"))

        // And never a rate-limit token: the daemon did not say "try again
        // later", it said nothing at all. Merging the two would make the
        // throttle count in a device pull unreadable.
        #expect(!reason.hasPrefix("rateLimited-"))
        // R2-Fix1: nor playhead-8d5r's family, which counts a model that is
        // not answering — the opposite claim to the one this row makes.
        #expect(!reason.hasPrefix("inferenceTimeout-"))
    }

    @available(iOS 26.0, *)
    @Test("playhead-ezmv: the defer cause carries the ERROR's census reading, not a reading taken at catch time")
    func metadataStallCauseCarriesThePeersReadingFromTheError() async throws {
        // The value must come off the caught `FMInferenceTimeoutError` —
        // the census AT THE INSTANT THE CALL WAS ISSUED — because by catch
        // time the peer that caused the stall may have finished. A runner
        // that re-read the live census here would write `peers=0` for
        // exactly the stalls the column exists to explain.
        let store = try await makeTestStore()
        let assetId = "asset-ezmv-peers"
        try await store.insertAsset(makeAsset(id: assetId))
        let runtime = TestFMRuntime(
            contextSize: Self.contextSize,
            coarseSchemaTokenCount: Self.coarseSchemaTokenCount,
            coarseSchemaTokenCountFailure: .metadataTimeoutWithPeers(2),
            tokenCountRule: windowingTokenRule()
        )
        let runner = makeRunner(store: store, runtime: runtime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs(assetId: assetId))
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))

        #expect(row.status == .deferred)
        #expect(row.deferReason == Self.expectedCause + "(peers=2)",
                "expected the error's census reading in the cause; got \(row.deferReason ?? "nil")")
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
        // R2 vacuity sweep: this was `allSatisfy { $0.status != .success && … }`,
        // which is TRUE on the empty array the refusal path actually produces —
        // the F2 shape, one test over. `isEmpty` is the state the code is in and
        // is strictly stronger: it also rejects a FAILURE row invented for a
        // window nobody called, which is the shape playhead-8d5r had to fix
        // once already ("no row may claim a timeout for a window nobody
        // called").
        #expect(scans.isEmpty,
                "a stalled prologue examined nothing, so it may claim nothing; got \(scans.map(\.status.rawValue))")
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

        // R3-Fix2: the POSITIVE CONTROL for `metadataStallDoesNotPenaliseCoverage`'s
        // `#expect(scans.isEmpty)`. That assertion is an ABSENCE, and R2 replaced
        // a vacuous `allSatisfy` with it without ever showing that this query can
        // observe a row for this asset in this fixture. Ask the diagnostic
        // question of it — what would `scans.isEmpty` read if
        // `fetchSemanticScanResults` could never return anything here, wrong
        // asset id, wrong store, shadow mode persisting nothing? — and the answer
        // is `true`, the passing value. This is the same query, the same store,
        // the same asset and the same runner with a healthy daemon, so a row here
        // is what makes the empty result over there mean "the prologue examined
        // nothing" rather than "nobody was looking".
        //
        // The claim is not idle: this runner has a live mechanism that writes a
        // `.noAds` sentinel row for work it did not do
        // (`makeNoWorkSentinelScanResult`, three call sites), and a refusal
        // routed through it would mint exactly the false coverage qbib and pmp9
        // fought.
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        #expect(!scans.isEmpty,
                "the query the stall test reads as EMPTY returns nothing on the HEALTHY path either")
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

        let inputs = makeBatchInputs(assetId: assetId, transcriptVersion: transcriptVersion)
        let fmRuntime = TestFMRuntime(coarseSchemaTokenCountFailure: .metadataTimeout)
        let runner = makeBatchRunner(store: store, runtime: fmRuntime.runtime)

        _ = try await runner.runPendingBackfill(for: inputs)

        // The plan is `targetedWithAudit`, whose three phases are enqueued with
        // deterministic ids; enumerate them rather than trusting the RunResult,
        // which is precisely the accounting under test.
        let rows = try await fetchBatchRows(
            store: store,
            assetId: assetId,
            transcriptVersion: transcriptVersion
        )
        #expect(rows.count == Self.batchPhases.count, "vacuity: the plan must have enqueued all three phases")
        #expect(rows.allSatisfy { $0.status != .failed },
                "a metadata stall marked a job failed: \(rows.map { "\($0.phase.rawValue)=\($0.status.rawValue)" })")
        #expect(rows.allSatisfy { $0.status != .queued },
                "a metadata stall stranded a job in queued: \(rows.map { "\($0.phase.rawValue)=\($0.status.rawValue)" })")

        // Every sibling the drain swept must carry a token that names what
        // actually stopped the drain. `rateLimited-batchSibling` here would say
        // the daemon rate-limited us, which it did not.
        let siblingReasons = rows.compactMap(\.deferReason).filter { $0.contains("batchSibling") }
        // R1-Fix3: `allSatisfy` is TRUE on an empty array, so without this the
        // token assertion below would pass on a build where the drain never
        // stopped and no sibling was ever swept — i.e. exactly the build where
        // the sweep it is testing does not run.
        #expect(!siblingReasons.isEmpty,
                "vacuity: no sibling was swept, so the token assertion proves nothing")
        // R4-Fix8: the token must BE the stall's, not merely avoid two families
        // somebody thought of. This was `allSatisfy { !hasPrefix("rateLimited-") }`
        // plus `!hasPrefix("inferenceTimeout-")` — a hand-written list of the two
        // wrong answers already known, which is the shape R2-Fix2 and R3-Fix1
        // both replaced elsewhere in this bead and which passes for a THIRD wrong
        // family. Its sibling test (`aThrottleAndAStallShareOneConsecutiveCounter`)
        // already asserted exact equality; this one did not. Set equality rather
        // than array equality so the claim is about the TOKEN and not about how
        // many siblings the fixture happens to leave.
        #expect(Set(siblingReasons) == [FMDaemonRefusal.metadataStall.batchSiblingCause],
                "the swept siblings must carry the STALL's token: \(siblingReasons)")
    }

    @available(iOS 26.0, *)
    @Test("ONE counter: a throttle then a metadata stall stops the drain, and the sibling names the stall")
    func aThrottleAndAStallShareOneConsecutiveCounter() async throws {
        // The load-bearing test for "kvs8's arm was GENERALIZED, not copied".
        // `FMDaemonThrottle.consecutiveDeferStopThreshold` is 2, so with two
        // per-kind counters neither would ever reach it here — job 1 is a
        // throttle and job 2 is a metadata stall, one apiece — and job 3 would
        // be dispatched rather than swept. Reaching the sweep at all is the
        // proof that both populations advance the SAME counter.
        //
        // It also pins the "most recent refusal wins" rule: the sibling token
        // must name the STALL, which is what crossed the threshold.
        // `rateLimited-batchSibling` here would record a rate limit for a batch
        // the daemon stopped serving for a different reason.
        let store = try await makeTestStore()
        let assetId = "asset-e75l-mixed"
        let transcriptVersion = "tx-e75l-mixed"
        try await store.insertAsset(makeAsset(id: assetId))

        let inputs = makeBatchInputs(assetId: assetId, transcriptVersion: transcriptVersion)

        // First prologue round trip is a throttle; every later one is a stall.
        let fmRuntime = TestFMRuntime(
            coarseSchemaTokenCountFailure: .metadataTimeout,
            coarseSchemaTokenCountFailures: [.rateLimited]
        )
        let runner = makeBatchRunner(store: store, runtime: fmRuntime.runtime)

        _ = try await runner.runPendingBackfill(for: inputs)

        let rows = try await fetchBatchRows(
            store: store,
            assetId: assetId,
            transcriptVersion: transcriptVersion
        )
        #expect(rows.count == Self.batchPhases.count, "vacuity: the plan must have enqueued all three phases")
        var reasons: [String] = []
        for row in rows {
            #expect(row.status == .deferred, "\(row.phase.rawValue) is \(row.status.rawValue)")
            reasons.append(try #require(row.deferReason))
        }

        #expect(reasons.contains(FMDaemonRefusal.throttle.passPrologueCause),
                "the throttled job did not record kvs8's cause: \(reasons)")
        #expect(reasons.contains(FMDaemonRefusal.metadataStall.passPrologueCause),
                "the stalled job did not record the stall cause: \(reasons)")

        let siblings = reasons.filter { $0.contains("batchSibling") }
        #expect(siblings == [FMDaemonRefusal.metadataStall.batchSiblingCause],
                "the swept sibling must name the refusal that crossed the threshold: \(reasons)")
    }

    @available(iOS 26.0, *)
    @Test("a THROTTLE-terminated drain sweeps its siblings with kvs8's rateLimited-batchSibling")
    func aThrottleTerminatedDrainSweepsWithTheThrottleToken() async throws {
        // R5-Fix3, and it is the mirror that was missing. This bead moved kvs8's
        // swept-sibling token out of a literal
        // (`FMDaemonThrottle.DeferCause.batchSibling.rawValue`) and behind a
        // per-kind property on `FMDaemonRefusal`. Both e75l batch tests are
        // STALL-terminated and both expect the stall's token, so every rail on
        // that property proved only "not always the THROTTLE's" — which is
        // mutation DR04. Nothing proved "not always the STALL's", and probe PB5
        // hard-coded `FMDaemonRefusal.metadataStall.batchSiblingCause` at the
        // sweep and every suite in this bead's scope stayed green. On a device
        // that is a drain the daemon RATE-LIMITED recording a wedged tokenizer
        // that never happened, in the durable column an operator greps — the
        // exact substitution the per-kind tokens exist to prevent, running in
        // the direction nobody had run it.
        //
        // kvs8's own `throttledBatchStrandsNothing` cannot cover it and it is
        // worth saying why rather than assuming: it passes an EMPTY
        // `EvidenceCatalog`, so `.scanHarvesterProposals` narrows to zero
        // windows and COMPLETES, which resets the consecutive counter, so the
        // drain never reaches the stop threshold and no sibling is ever swept.
        // This bead's R1 found exactly that and built `makeBatchInputs` to fix
        // it — the fixture that makes the sweep observable already existed here
        // and had only ever been pointed at a stall.
        let store = try await makeTestStore()
        let assetId = "asset-e75l-throttle-batch"
        let transcriptVersion = "tx-e75l-throttle-batch"
        try await store.insertAsset(makeAsset(id: assetId))

        let inputs = makeBatchInputs(assetId: assetId, transcriptVersion: transcriptVersion)
        let fmRuntime = TestFMRuntime(coarseSchemaTokenCountFailure: .rateLimited)
        let runner = makeBatchRunner(store: store, runtime: fmRuntime.runtime)

        _ = try await runner.runPendingBackfill(for: inputs)

        let rows = try await fetchBatchRows(
            store: store,
            assetId: assetId,
            transcriptVersion: transcriptVersion
        )
        #expect(rows.count == Self.batchPhases.count, "vacuity: the plan must have enqueued all three phases")
        var reasons: [String] = []
        for row in rows {
            #expect(row.status == .deferred, "\(row.phase.rawValue) is \(row.status.rawValue)")
            reasons.append(try #require(row.deferReason))
        }

        let siblings = reasons.filter { $0.contains("batchSibling") }
        // The R1-Fix3 guard, for the same reason it exists next door: an empty
        // array makes the token assertion prove nothing, and an empty array is
        // exactly what a build where the drain never stops produces.
        #expect(!siblings.isEmpty,
                "vacuity: no sibling was swept, so the token assertion proves nothing")
        #expect(Set(siblings) == [FMDaemonRefusal.throttle.batchSiblingCause],
                "a throttle-stopped drain must sweep with kvs8's token: \(reasons)")
        #expect(reasons.contains(FMDaemonRefusal.throttle.passPrologueCause),
                "the throttled jobs did not record kvs8's own cause: \(reasons)")
    }
}

// MARK: - Source canaries

/// The three claims about this bead that NO runtime assertion on this harness
/// can observe, because each is about something outside a test's reach: a log
/// line, a token nobody parses, and a call site in another file.
///
///  1. **Event wiring.** A log line is not readable from a test here, so the
///     enum test above can prove `drainStoppedEvent` returns two names and
///     still say nothing about whether the runner emits them. It did not: the
///     drain-stop line carried kvs8's `fm.backfill.drain_stopped_by_throttle`
///     as a hard-coded literal and fired verbatim for a batch stopped by a
///     wedged tokenizer (R1-Fix1).
///  2. **Token families.** `deferReason` has no Swift parser by design, so the
///     only consumer of a durable cause token is a human grepping a device
///     pull — and the rule that a prefix family holds one population can only
///     be checked against the OTHER families this runner writes, which are
///     bare literals in its source (R2-Fix1).
///  3. **Budget enumeration.** `isMetadataStall` is sound exactly while no
///     other budget reaching `FMInferenceDeadline.run` is 30 s, and that is a
///     property of call sites in two other files (R2-Fix5).
///
/// Same technique and same rationale as `FMDaemonThrottleCanaryTests`. The
/// class was called `…EventWiringCanaryTests` when it held only the first.
///
/// R4 REVIEW: every finder in here was a literal substring over ONE LINE, and
/// a source canary that cannot read a respelling is a canary that reports on
/// formatting. Eight probes were planted; SEVEN walked past — two hard-coded
/// event names split across a `\` continuation or a `+` join, one that
/// SWAPPED the two event properties and used no literal at all, and four
/// third-30 s-budget call sites (`standard / 10`, a site split over two lines,
/// `Type . run(`, and a budget in a local with the allowed name in a trailing
/// comment). The one that failed was killed by SwiftLint, not by this class.
/// The normalizations below, the exactly-once-and-in-order rule, and the
/// exact-argument match are what those probes bought.
final class FMDaemonRefusalSourceCanaryTests: XCTestCase {

    /// One line of source, carrying the number it actually has in the FILE.
    ///
    /// R4-Fix5: the pre-R4 helper returned a FILTERED array and reported
    /// `offset + 1` as a line number. `BackfillJobRunner` is two-thirds
    /// comment, so every offender this class has ever printed named a line
    /// several hundred short of the real one — a value that names one thing
    /// (an index into the survivors) read as though it named another (a file
    /// line). The number is carried now rather than recomputed.
    typealias SourceLine = (number: Int, text: String)

    private func runnerSource() throws -> [SourceLine] {
        var root = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        for _ in 0..<3 {
            root.deleteLastPathComponent()
        }
        let url = root
            .appendingPathComponent("Playhead/Services/AdDetection/BackfillJobRunner.swift")
        return Self.codeLines(of: try String(contentsOf: url, encoding: .utf8))
    }

    // MARK: - Source normalization (R4-Fix1 / R4-Fix2 / R4-Fix3)
    //
    // EVERY finder in this class used to be a literal substring over ONE line.
    // R4 planted eight respellings and seven of them walked past the rails
    // untouched — the CN10 shape playhead-x0lb hit twice. The normalizations
    // below are what those probes cost:
    //
    //   CN-A2  the drain-stop event, hard-coded but split by a `\` line
    //          continuation INSIDE the multiline string the runner already
    //          uses that idiom in. No single line held the forbidden name.
    //   CN-A3  the per-job event, hard-coded as `"fm.backfill.job_" + "..."`.
    //   CN-C2  a NEW call site written `FMInferenceDeadline` / newline /
    //          `.run(.seconds(30))`, invisible to a per-line finder — and free,
    //          because the site count is a FLOOR, so adding an unseen site
    //          never trips it.
    //   CN-C3b `FMInferenceDeadline . run(` — spaces around the member dot.
    //          (`run (`, a space before the paren, is the ONE evasion that
    //          failed, and SwiftLint's `no_space_in_method_call` is what
    //          stopped it, not this class.)
    //   CN-C4  the budget bound to a local, with an allowed name present only
    //          in a TRAILING COMMENT on the call line.
    //
    // So: comments are stripped to end-of-line rather than only whole-line,
    // continuations are joined, and `"a" + "b"` is collapsed before any
    // forbidden name is looked for.

    /// A source line with any trailing `//` comment removed.
    ///
    /// Whole-line comment filtering is not enough: a trailing comment is code
    /// as far as a substring finder is concerned, which is how CN-C4 satisfied
    /// an allow-list by mentioning an allowed budget in prose.
    static func withoutTrailingComment(_ line: String) -> String {
        var out = ""
        var inString = false
        var escaped = false
        var index = line.startIndex
        while index < line.endIndex {
            let character = line[index]
            if escaped {
                escaped = false
            } else if character == "\\" {
                escaped = true
            } else if character == "\"" {
                inString.toggle()
            } else if !inString, character == "/" {
                let next = line.index(after: index)
                if next < line.endIndex, line[next] == "/" {
                    break
                }
            }
            out.append(character)
            index = line.index(after: index)
        }
        return out
    }

    /// Numbered code lines: whole-line comments dropped, trailing comments cut.
    static func codeLines(of text: String) -> [SourceLine] {
        text.split(separator: "\n", omittingEmptySubsequences: false)
            .enumerated()
            .filter { !$0.element.trimmingCharacters(in: .whitespaces).hasPrefix("//") }
            .map { (number: $0.offset + 1, text: withoutTrailingComment(String($0.element))) }
    }

    /// The same code read as ONE string, so a name spelled across a line break
    /// or a `+` join is the same name.
    ///
    /// Lines are trimmed and a trailing `\` dropped before joining, because
    /// that is exactly what Swift does to a multiline-string continuation whose
    /// indentation matches its closing delimiter — which is the shape the
    /// runner's log lines are already written in.
    static func collapsedCode(_ lines: [SourceLine]) -> String {
        var joined = ""
        for line in lines {
            var text = line.text.trimmingCharacters(in: .whitespaces)
            if text.hasSuffix("\\") {
                text.removeLast()
            }
            joined += text
        }
        return collapsingLiteralConcatenation(joined)
    }

    /// `"a" + "b"` is one literal to everything that matters here.
    ///
    /// Scans right to left over a character array so each deletion cannot
    /// disturb the indices still to be visited.
    private static func collapsingLiteralConcatenation(_ text: String) -> String {
        var characters = Array(text)
        var position = characters.count - 1
        while position >= 0 {
            guard characters[position] == "+" else {
                position -= 1
                continue
            }
            var left = position - 1
            while left >= 0, characters[left] == " " { left -= 1 }
            var right = position + 1
            while right < characters.count, characters[right] == " " { right += 1 }
            if left >= 0, right < characters.count, characters[left] == "\"", characters[right] == "\"" {
                characters.removeSubrange(left...right)
                position = left - 1
            } else {
                position -= 1
            }
        }
        return String(characters)
    }

    /// The same code with EVERY whitespace character removed, so a call spelled
    /// `Type` / newline / `. run(` is the same call.
    static func denseCode(_ lines: [SourceLine]) -> String {
        var joined = ""
        for line in lines {
            joined += line.text.filter { !$0.isWhitespace }
        }
        return joined
    }

    /// Both daemon-refusal event names must reach the log through the enum. A
    /// literal here is how the name and the condition drift apart.
    ///
    /// R2-Fix2: the forbidden set is DERIVED FROM `FMDaemonRefusal.allCases`,
    /// not written out. R1's version named `fm.backfill.job_throttled` and
    /// `fm.backfill.drain_stopped_by` — the two kvs8-era spellings it had just
    /// seen go wrong — and was therefore blind to a literal of the event this
    /// bead itself ADDED. Mutation DR08 (the per-job line emitting
    /// `fm.backfill.job_daemon_metadata_stalled` verbatim, so every throttle
    /// logs the stall's name and a support bundle's rate-limit count reads
    /// zero) SURVIVED against it. A blacklist covers the mistakes already made;
    /// deriving from `allCases` covers the third kind nobody has added yet.
    func testDaemonRefusalLogEventsAreNotHardCodedInTheRunner() throws {
        let lines = try runnerSource()

        // Vacuity guard: the file must have been found and be the real one.
        XCTAssertGreaterThan(lines.count, 1_000, "source read found only \(lines.count) code lines")
        XCTAssertTrue(
            lines.contains { $0.text.contains("FMDaemonRefusal.classify(") },
            "BackfillJobRunner no longer classifies daemon refusals; move this canary with the code."
        )

        // Every event name the enum can produce, whichever kind owns it.
        let forbidden = FMDaemonRefusal.allCases.flatMap { [$0.logEvent, $0.drainStoppedEvent] }
        XCTAssertEqual(
            forbidden.count,
            Set(forbidden).count,
            "two FMDaemonRefusal kinds share an event name; the enum test should have caught this first."
        )
        XCTAssertGreaterThanOrEqual(forbidden.count, 4, "vacuity: the forbidden set must not be empty")

        let offenders = lines
            .filter { line in forbidden.contains { line.text.contains($0) } }
            .map { "\($0.number): \($0.text.trimmingCharacters(in: .whitespaces))" }

        XCTAssertTrue(
            offenders.isEmpty,
            """
            A daemon-refusal log EVENT NAME is hard-coded in BackfillJobRunner. It must come from \
            `FMDaemonRefusal.logEvent` / `.drainStoppedEvent`, because an event name is the unit a \
            support-bundle grep counts: a literal `..._by_throttle` fires for a drain stopped by a \
            metadata stall and inflates the rate-limit count with an event that never happened — \
            and a literal of the STALL's name fires for a throttle, which empties the same count.
            \(offenders.joined(separator: "\n"))
            """
        )

        // R4-Fix2: the SAME rule against a name spelled across a line break or a
        // `+` join. Probes CN-A2 and CN-A3 hard-coded both events past the check
        // above without a single line containing either name — CN-A2 using the
        // very `\` continuation idiom these log lines are already written in.
        // A per-line finder cannot see a respelling; the collapsed read can.
        let collapsed = Self.collapsedCode(lines)
        let respelled = forbidden.filter { collapsed.contains($0) }
        XCTAssertTrue(
            respelled.isEmpty,
            """
            A daemon-refusal log EVENT NAME is hard-coded in BackfillJobRunner and SPLIT so no one \
            line holds it — across a `\\` continuation, or as `"a" + "b"`. The emitted name is the \
            same name and a support-bundle grep counts it the same way. Names found only in the \
            collapsed read: \(respelled.sorted())
            """
        )

        // Both properties must actually be READ, EXACTLY ONCE EACH, and in the
        // order the control flow emits them.
        //
        // R4-Fix1: "is read at all" was satisfiable by a decoy — a one-line
        // `logger.debug` naming the property, next to a hard-coded literal
        // emitting it. And "both are read" says nothing about WHICH SITE reads
        // WHICH, so probe CN-A1 simply SWAPPED them: the per-job line emitted
        // `drain_stopped_by_…` and the drain-stop line emitted `job_…`. No
        // literal anywhere, both properties read, every rail green — and every
        // refused JOB counted as a drain stop in a support bundle. That is this
        // bead's own defect class committed inside the check written for it.
        //
        // The order is not an accident to be pinned: the per-job event is
        // emitted INSIDE the drain loop's catch arm and the drain-stop event
        // AFTER the loop breaks, so `logEvent` precedes `drainStoppedEvent` in
        // the source as a consequence of the control flow. Swapping the two
        // reads inverts it.
        var firstRead: [String: Int] = [:]
        for property in ["logEvent", "drainStoppedEvent"] {
            let reads = lines.filter { $0.text.contains(property) }
            XCTAssertEqual(
                reads.count,
                1,
                """
                BackfillJobRunner reads `\(property)` \(reads.count) times; expected exactly one. \
                Zero means the line cannot be naming the refusal it reports. More than one means a \
                read can be a DECOY satisfying this rail while the emitted name is hard-coded \
                somewhere else. Sites: \(reads.map(\.number))
                """
            )
            firstRead[property] = reads.first?.number
        }
        let jobEventLine = try XCTUnwrap(firstRead["logEvent"], "no `logEvent` read to order")
        let stopEventLine = try XCTUnwrap(firstRead["drainStoppedEvent"], "no `drainStoppedEvent` read to order")
        XCTAssertLessThan(
            jobEventLine,
            stopEventLine,
            """
            The per-job event (`logEvent`, line \(jobEventLine)) must be read BEFORE the drain-stop \
            event (`drainStoppedEvent`, line \(stopEventLine)): the first is emitted inside the \
            drain loop, the second after it breaks. Reading them in this order means one of the two \
            sites is naming the OTHER site's event — the per-job line reporting a drain stop, or \
            the drain-stop line reporting a job. Neither is observable from any runtime assertion \
            on this harness, which is why it is checked here.
            """
        )
    }

    /// R6-Fix1: each log line's DURABLE-TOKEN field must name the token the
    /// write beside it actually persisted.
    ///
    /// R4-Fix1 found that "both properties are READ" says nothing about WHICH
    /// SITE reads WHICH, and probe CN-A1 simply SWAPPED `logEvent` and
    /// `drainStoppedEvent` between the two log lines — no literal anywhere,
    /// every rail green, and every daemon-refused JOB counted as a drain stop in
    /// a support bundle. That fix pinned the EVENT pair. The same two log lines
    /// carry a second pair of exactly that shape and it was pinned by nothing:
    /// the per-job line's `cause=` (`passPrologueCause`, the token written to
    /// THIS job's row) and the drain-stop line's `siblingCause=`
    /// (`batchSiblingCause`, the token written to the jobs the sweep touched).
    /// Probe R6-PB1 gave `cause=` the SIBLING token and R6-PB2 gave
    /// `siblingCause=` the refused job's token; both SURVIVED every suite in
    /// this bead's scope, because the durable columns are asserted by the runner
    /// tests and the log fields are asserted by nobody. On a device that is an
    /// operator reading, off a line whose event name already survived being
    /// wrong once, that a job refused on its own run carried the token reserved
    /// for jobs that were never asked.
    ///
    /// DERIVED, not pinned. The test names neither property. It extracts the
    /// `reason:` argument of every `markBackfillJobDeferred` in the runner that
    /// writes an `FMDaemonRefusal` cause, and requires each log field to be
    /// TEXTUALLY the argument of the write it describes. So a rename moves the
    /// rule with the code, and the check fails rather than silently emptying.
    ///
    /// The one structural assumption is stated rather than hidden: the refused
    /// job's OWN defer precedes the sibling sweep in the source because it is
    /// emitted inside the drain loop's catch arm and the sweep runs after the
    /// loop breaks — the same control-flow consequence R4-Fix1 relies on for the
    /// event ordering, not an accident being frozen.
    func testDaemonRefusalCauseFieldsNameTheTokenTheyDescribe() throws {
        let lines = try runnerSource()
        XCTAssertGreaterThan(lines.count, 1_000, "source read found only \(lines.count) code lines")

        let dense = Self.denseCode(lines)

        // The durable writes, in source order. Every `reason:` label in the
        // runner is scanned and only those naming an `FMDaemonRefusal` cause
        // property are kept, so nothing here is a list of expected spellings.
        let refusalWrites = Self.firstArguments(after: "reason:", in: dense)
            .filter { $0.hasSuffix("Cause") }
        XCTAssertEqual(
            refusalWrites.count,
            2,
            """
            Expected exactly two durable writes of an `FMDaemonRefusal` cause in BackfillJobRunner — \
            the refused job's own defer and the sibling sweep. Found \(refusalWrites). A third means \
            this rule no longer knows which write each log field describes; zero means the tokens \
            stopped reaching the store through the enum.
            """
        )
        let ownDeferToken = try XCTUnwrap(refusalWrites.first)
        let sweepToken = try XCTUnwrap(refusalWrites.last)
        XCTAssertNotEqual(
            ownDeferToken,
            sweepToken,
            "the refused job and the swept siblings must not be deferred with the same token"
        )

        // `cause=` is matched case-sensitively, so it cannot also match
        // `siblingCause=`; the two fields are extracted independently.
        let causeFields = Self.firstArguments(after: "cause=\\(", in: dense)
        let siblingCauseFields = Self.firstArguments(after: "siblingCause=\\(", in: dense)
        XCTAssertEqual(
            causeFields.count,
            1,
            "expected exactly one `cause=` field, on the per-job refusal line; found \(causeFields)"
        )
        XCTAssertEqual(
            siblingCauseFields.count,
            1,
            "expected exactly one `siblingCause=` field, on the drain-stop line; found \(siblingCauseFields)"
        )

        XCTAssertEqual(
            causeFields.first,
            ownDeferToken,
            """
            The per-job refusal line reports `cause=\\(\(causeFields.first ?? "<none>"))` while the \
            defer beside it persists `\(ownDeferToken)`. The field names the token written to THIS \
            job's row; naming the sibling token instead reports, for a job the daemon refused on its \
            own run, the cause reserved for jobs that were never asked.
            """
        )
        XCTAssertEqual(
            siblingCauseFields.first,
            sweepToken,
            """
            The drain-stop line reports `siblingCause=\\(\(siblingCauseFields.first ?? "<none>"))` \
            while the sweep beside it persists `\(sweepToken)`. The field names the token the SWEPT \
            jobs carry; naming the refused job's token instead reports a cause no swept row holds.
            """
        )

        // R7-Fix1: and the EVENT on each line must be read off the SAME VALUE as
        // the cause beside it.
        //
        // R1 found the drain-stop event hard-coded to kvs8's
        // `fm.backfill.drain_stopped_by_throttle` for a drain stopped by metadata
        // stalls, and the rail it bought forbids that LITERAL (derived from
        // `allCases`, R2-Fix2) and pins the read order (R4-Fix1). Probe R7-PC3
        // wrote `FMDaemonRefusal.throttle.drainStoppedEvent` instead: no literal,
        // `drainStoppedEvent` still read exactly once and still after `logEvent`,
        // every rail green — and the emitted name is byte-identical to the one R1
        // removed. Fixing a value by forbidding its SPELLING is what R2-Fix2,
        // R3-Fix1 and R4-Fix8 each had to replace; the value itself is what an
        // operator's `grep -c` counts.
        //
        // Derived from the receivers already extracted above, so nothing here is
        // a list of expected spellings: the per-job line's event must hang off
        // whatever value that line's own defer wrote its cause from, and the
        // drain-stop line's off the sweep's. The `\\(` anchor is what makes it a
        // whole-identifier match rather than a suffix one — `refusalThatStoppedUs`
        // has `refusal` as a prefix, and only the anchor separates them.
        for (token, property, role) in [
            (ownDeferToken, "logEvent", "per-job refusal"),
            (sweepToken, "drainStoppedEvent", "drain-stop"),
        ] {
            let receiver = String(token.prefix(while: { $0 != "." }))
            XCTAssertFalse(
                receiver.isEmpty || receiver == token,
                "vacuity: `\(token)` has no receiver to bind `\(property)` to."
            )
            XCTAssertTrue(
                dense.contains("\\(\(receiver).\(property)"),
                """
                The \(role) line's `\(property)` is not read off `\(receiver)` — the same value the \
                defer beside it wrote `\(token)` from. Binding it to a fixed kind instead (or to the \
                other line's value) emits an event name for a condition that did not occur, with no \
                literal anywhere for the hard-coding rail above to see, and a support-bundle grep \
                counts the event NAME. Probe R7-PC3 did exactly this and every rail stayed green.
                """
            )
        }
    }

    /// R7-Fix2: both log lines' `consecutive=` must name the counter the STOP
    /// RULE reads.
    ///
    /// This is the fifth field on the drain-stop line to be examined and the
    /// first that had no rule at all. R1 corrected its event NAME, R2 its
    /// `cause=` -> `siblingCause=`, R5 its `deferredSiblings=`, R6 the cause
    /// pair's binding — and `consecutive=` was carried through all four
    /// untouched, on both that line and the per-job line.
    ///
    /// Two probes ran it. R7-PC1 gave the drain-stop field `deferred.count`, the
    /// drain-wide accumulator R5 had just removed from `deferredSiblings=` on the
    /// same line. R7-PC2 gave the per-job field `job.retryCount` — and that one
    /// is the standing defect class in its sharpest form, because `retryCount` is
    /// the quantity THIS BEAD EXISTS TO HOLD CONSTANT: preserved across every
    /// daemon refusal, so the field would read the same number whether the daemon
    /// refused this job once or four times running. Ask the diagnostic — what
    /// would it read if the thing it names had never happened? The same value,
    /// and normally zero. Both SURVIVED every suite in this bead's scope.
    ///
    /// Derived rather than pinned, for the R2-Fix2 reason: the expected
    /// identifier is read out of the `shouldStopDraining` call, so the rule is
    /// "the number you report is the number you decide on" and a rename moves it
    /// with the code.
    func testConsecutiveFieldsNameTheCounterTheStopRuleReads() throws {
        let lines = try runnerSource()
        XCTAssertGreaterThan(lines.count, 1_000, "source read found only \(lines.count) code lines")

        let dense = Self.denseCode(lines)

        // The counter the drain actually decides on. One call site, asserted, so
        // this cannot silently start reading a second one.
        let stopRuleArguments = Self.firstArguments(after: "consecutiveRefusals:", in: dense)
        XCTAssertEqual(
            stopRuleArguments.count,
            1,
            """
            Expected exactly one `shouldStopDraining(consecutiveRefusals:)` call in BackfillJobRunner \
            to derive the reported counter from; found \(stopRuleArguments). Zero means the drain no \
            longer delegates the stop rule and this canary is reading nothing.
            """
        )
        let counter = try XCTUnwrap(stopRuleArguments.first)
        XCTAssertFalse(
            counter.contains("."),
            "vacuity: expected a bare counter identifier at the stop rule; found `\(counter)`."
        )

        // Both refusal log lines report it: the per-job line and the drain-stop
        // line. An exact count, not a floor — a field that DISAPPEARS is as much
        // a regression as one that lies (R4's DR14 lesson).
        let reported = Self.firstArguments(after: "consecutive=\\(", in: dense)
        XCTAssertEqual(
            reported.count,
            2,
            """
            Expected exactly two `consecutive=` fields — one on the per-job refusal line, one on the \
            drain-stop line; found \(reported).
            """
        )
        for field in reported {
            XCTAssertEqual(
                field,
                counter,
                """
                A daemon-refusal log line reports `consecutive=\\(\(field))` while the drain decides \
                to stop on `\(counter)`. The field names the run of back-to-back refusals an operator \
                reads to tell a wedged daemon from an unlucky job; anything else on that line either \
                spans the whole drain (`deferred` accumulates admission defers, coverage-hole defers \
                and under-coverage terminals) or is held DELIBERATELY CONSTANT by this bead \
                (`job.retryCount` is preserved across every refusal, so the field could never move). \
                Report the number you decided on.
                """
            )
        }
    }

    /// R5-Fix1: the drain-stop line's `deferredSiblings=` must count THE SWEEP.
    ///
    /// It counted `deferred.count`, and `deferred` is the drain-WIDE
    /// accumulator — the admission-defer branch, the rate-limited
    /// coarse-coverage-hole defer, playhead-41mu's under-coverage terminal and
    /// the refused job's OWN defer have all appended to it before the drain-stop
    /// line runs. So the field named for the siblings reported every job the
    /// sweep did not touch: measured on the mixed-refusal fixture, 3 for one
    /// swept sibling.
    ///
    /// The sharpest form of it is that the old value could never read ZERO.
    /// Reaching that line requires `consecutiveDaemonRefusals >= 2` and each of
    /// those refusals appended, so the one thing an operator most wants the
    /// field to be able to say — the stop landed on the last job and cost the
    /// batch nothing — was unrepresentable. This is the fourth field on the line
    /// whose event name R1 corrected and whose `siblingCause=` R2 corrected, and
    /// it is the same defect class all three times.
    ///
    /// A log line's argument is not observable from any runtime assertion on
    /// this harness, which is why it is checked here rather than in the runner
    /// suite. The rule is deliberately not "do not write `deferred.count`" — a
    /// blacklist of the spelling that was wrong is the shape R2-Fix2, R3-Fix1
    /// and R4-Fix8 each had to replace. It judges the ARGUMENT, extracted by
    /// balanced-paren scan over the dense read so a spelling split across lines
    /// is the same argument, and then requires the identifier it names to occur
    /// EXACTLY THREE times in the runner's code: declared, incremented, read. No
    /// drain-wide accumulator can satisfy that — `deferred` occurs nine times.
    func testDrainStopSiblingCountIsTheSweepNotTheDrainTotal() throws {
        let lines = try runnerSource()
        XCTAssertGreaterThan(lines.count, 1_000, "source read found only \(lines.count) code lines")

        let dense = Self.denseCode(lines)
        let arguments = Self.firstArguments(after: "deferredSiblings=\\(", in: dense)
        // Vacuity: the field must exist, and exactly once, or this test would
        // pass by looking at nothing — the failure direction a count FLOOR alone
        // cannot cover (R4's DR14 lesson).
        XCTAssertEqual(
            arguments.count,
            1,
            "expected exactly one `deferredSiblings=` field on the drain-stop line; found \(arguments)"
        )
        let argument = try XCTUnwrap(arguments.first)

        // Every collection in scope at that line spans the whole drain rather
        // than the sweep, so a `.count` of any of them is the defect.
        XCTAssertFalse(
            argument.hasSuffix(".count"),
            """
            The drain-stop line reports `deferredSiblings=\\(\(argument))`. Every collection in \
            scope there is drain-WIDE — `deferred` also holds admission defers, rate-limited \
            coverage-hole defers, under-coverage terminals and the refused job's own defer — so a \
            field named for the sweep would be holding the drain's total, and could never read \
            zero. Count the sweep.
            """
        )

        // And what it does name must be a counter local to the sweep: declared
        // once, incremented once, read once. This is what makes the check a rule
        // about the QUANTITY rather than a memory of the spelling that was wrong.
        let reads = lines.filter { $0.text.contains(argument) }
        XCTAssertEqual(
            reads.count,
            3,
            """
            `\(argument)` appears on \(reads.count) code lines in BackfillJobRunner; expected 3 — \
            the declaration, the increment inside the sweep loop, and the drain-stop line that \
            reports it. More than three means the quantity is shared with something other than the \
            sibling sweep, which is exactly how a field named for the sweep comes to hold the \
            drain's total. Sites: \(reads.map(\.number))
            """
        )

        // R6-Fix2: three sites in the right places is not three sites. The claim
        // R5 wrote into the runner is "incremented only on a SUCCESSFUL defer,
        // so a sibling skipped by the terminal-row guard is correctly not
        // counted as swept" — and that claim lives entirely in WHERE the
        // increment sits. Probe R6-PB3 moved it above the `do` and SURVIVED: the
        // count is still declared once, incremented once and read once, and no
        // fixture can make the sweep's store write throw, so nothing runtime can
        // see it either. It is reachable in the field — the M-5 path re-enqueues
        // `.failed` rows under the retry budget, and the sweep's
        // `invalidStateTransition` guard exists precisely because that happens —
        // and the result is `deferredSiblings=` counting siblings that were NOT
        // deferred, which is the defect R5 had just removed from this field.
        let increment = try XCTUnwrap(
            reads.first { $0.text.contains("+=") },
            "no `\(argument) +=` site found; the sweep is not counting anything"
        )
        // The sweep's own durable write, located by the two things that are true
        // only of it: it passes a `reason:` and the token is the sibling cause.
        let sweepWrite = lines.filter {
            $0.text.contains("reason:") && $0.text.contains("batchSiblingCause")
        }
        XCTAssertEqual(
            sweepWrite.count,
            1,
            "expected exactly one sibling-sweep defer write to order the increment against; found \(sweepWrite.map(\.number))"
        )
        let sweepWriteLine = try XCTUnwrap(sweepWrite.first).number
        XCTAssertGreaterThan(
            increment.number,
            sweepWriteLine,
            """
            `\(argument)` is incremented at line \(increment.number), BEFORE the sibling defer write \
            at line \(sweepWriteLine). A `throws` call cannot skip an increment that already \
            happened, so a sibling rejected by the terminal-row guard would be counted as swept and \
            `deferredSiblings=` would over-report the sweep — the same "a value that names one thing \
            read as though it named another" this field was fixed for one round ago.
            """
        )
    }

    /// R2-Fix1: the durable cause tokens must not join a FOREIGN prefix family.
    ///
    /// The runner writes `inferenceTimeout-noProgress` (playhead-8d5r) when the
    /// coarse pass aborts on a run of `standard`-deadline timeouts — "the model
    /// is not answering on this device", in the runner's own words. The first
    /// spelling of this bead's tokens was `inferenceTimeout-metadata` /
    /// `inferenceTimeout-batchSibling`, so `grep -c 'inferenceTimeout-'` would
    /// have counted wedged tokenizer round trips as evidence about the model.
    ///
    /// This is a SOURCE canary rather than a pure test because the foreign
    /// token is a bare literal in the runner: if playhead-8d5r's spelling ever
    /// moves, the rule has to be re-derived rather than silently passing.
    ///
    /// R3-Fix1: the foreign families are now DERIVED FROM THE RUNNER'S SOURCE
    /// rather than written out. R2's version named exactly two —
    /// `inferenceTimeout-noProgress` and `cancelled-during-`, the two it had
    /// just looked at — while this runner writes SIX durable token families to
    /// the same column. `expiredWithoutProgress-` (line ~2044),
    /// `underCoverage-` and `underCoverageBudgetSpent-` (playhead-41mu) and
    /// `noWork-` were outside the rule by construction, so a third
    /// `FMDaemonRefusal` kind named into any of them would pass. That is DR08's
    /// shape one test over: a rail written as a list of known-bad spellings is a
    /// memory of the last bug, not a rule. Mutation DR10 plants a refusal token
    /// in one of the families the list forgot.
    func testDaemonRefusalCausesDoNotJoinAForeignTokenFamily() throws {
        let lines = try runnerSource()

        // The two families R2 named are kept as ANCHORS — a rename of either
        // must surface here rather than quietly emptying the check — but they
        // are now a floor on the derivation, not the rule itself.
        let anchorTokens = ["inferenceTimeout-noProgress", "cancelled-during-"]
        for token in anchorTokens {
            XCTAssertTrue(
                lines.contains { $0.text.contains("\"\(token)") },
                "`\(token)` is no longer written by BackfillJobRunner. Re-derive this canary's foreign families."
            )
        }

        // kvs8's family lives in FMDaemonThrottle rather than in this runner,
        // and the throttle kind deliberately BELONGS to it — that is the one
        // legitimate overlap, and it is why the derivation reads the runner's
        // own literals rather than every token in the repo.
        let foreignFamilies = Set(Self.durableTokenLiterals(in: lines).map { family(of: $0) })
        for token in anchorTokens {
            XCTAssertTrue(
                foreignFamilies.contains(family(of: token)),
                "the derivation missed `\(family(of: token))`; it found \(foreignFamilies.sorted())"
            )
        }
        // Vacuity floor: measured at SIX at R3 review. A derivation that
        // collapses to the two anchors is the rail this fix replaced.
        XCTAssertGreaterThanOrEqual(
            foreignFamilies.count,
            5,
            "vacuity: the derivation found only \(foreignFamilies.sorted())"
        )

        let ownTokens = FMDaemonRefusal.allCases.flatMap { [$0.passPrologueCause, $0.batchSiblingCause] }
        for token in ownTokens {
            XCTAssertFalse(
                foreignFamilies.contains(family(of: token)),
                """
                `\(token)` shares the `\(family(of: token))` family with a durable token this \
                runner writes for an unrelated population. A cause token's prefix IS its \
                condition — an operator counting a family must be counting one thing. \
                (If the collision is because the runner now spells this very token as a bare \
                literal, that is the same defect from the other side: the cause must come from \
                `FMDaemonRefusal`, the way the log events already do.)
                """
            )

            // The one family that is not hyphen-shaped, so `family(of:)` cannot
            // see it: playhead-fil5's scan claims live in the SAME column and
            // are read by a documented device-pull query
            // (`WHERE deferReason LIKE 'scan_claim:%'`).
            XCTAssertFalse(
                token.hasPrefix(SemanticScanClaim.deferReasonPrefix),
                "`\(token)` would be counted as a dropped-scan CLAIM by fil5's device-pull query."
            )
        }

        // And the two kinds still do not share a family with EACH OTHER.
        XCTAssertNotEqual(
            family(of: FMDaemonRefusal.throttle.passPrologueCause),
            family(of: FMDaemonRefusal.metadataStall.passPrologueCause)
        )
    }

    /// Everything up to and including the first `-`; the unit an operator greps.
    private func family(of token: String) -> String {
        guard let index = token.firstIndex(of: "-") else { return token }
        return String(token[...index])
    }

    /// Every DURABLE-TOKEN-shaped string literal in a source file: quoted, and
    /// beginning with a word followed by a hyphen, with no whitespace and no
    /// colon anywhere in it.
    ///
    /// The shape is what separates a cause token from the log-format literals in
    /// the same file. `"bd-m8k: planner-state observation failed (suppressed): …"`
    /// also begins with a word and a hyphen and is prose; a token an operator
    /// groups by never contains a space. Measured at R3 review: the filter keeps
    /// six families (`cancelled-`, `expiredWithoutProgress-`, `underCoverage-`,
    /// `underCoverageBudgetSpent-`, `inferenceTimeout-`, `noWork-`) and drops the
    /// two prose lines.
    /// R4-Fix4: a leading interpolation is resolved ONE level through a string
    /// constant declared in the same file.
    ///
    /// Probe CN-B1 rewrote playhead-41mu's token as
    /// `"\(Self.underCoverageFamily)-\(phase.rawValue)"` — behaviour-identical,
    /// still the same family on the device — and the derivation lost it,
    /// because `isDurableTokenShaped` requires a LETTER first and a backslash
    /// is not one. Six families fell to five, which still cleared the floor,
    /// and mutation DR10's exact token then passed. The rail was not evaded by
    /// touching the rail; it was evaded by refactoring the writer it reads.
    ///
    /// Following one level of naming is where this stops: an interpolation that
    /// resolves to a computed property, to a constant in another file, or to
    /// anything but a plain string literal is still invisible. That is the
    /// honest limit, and the anchors plus the floor below are what keep it from
    /// silently collapsing to nothing.
    private static func resolvingLeadingInterpolation(
        _ literal: String,
        constants: [String: String]
    ) -> String? {
        guard literal.hasPrefix("\\(") else { return nil }
        guard let close = literal.firstIndex(of: ")") else { return nil }
        let inner = String(literal[literal.index(literal.startIndex, offsetBy: 2)..<close])
            .replacingOccurrences(of: "Self.", with: "")
            .trimmingCharacters(in: .whitespaces)
        guard let value = constants[inner] else { return nil }
        return value + String(literal[literal.index(after: close)...])
    }

    /// `static let NAME = "value"` declarations, by name. Any binding form —
    /// `let`, `static let`, `nonisolated static let`, `private static let` —
    /// since all this needs is the name and the literal.
    private static func stringConstants(in lines: [SourceLine]) -> [String: String] {
        var constants: [String: String] = [:]
        for line in lines {
            let text = line.text
            guard let letRange = text.range(of: "let ") else { continue }
            let afterLet = text[letRange.upperBound...]
            let name = String(afterLet.prefix { $0.isLetter || $0.isNumber || $0 == "_" })
            guard !name.isEmpty else { continue }
            guard let equals = afterLet.firstIndex(of: "=") else { continue }
            let rhs = afterLet[afterLet.index(after: equals)...].trimmingCharacters(in: .whitespaces)
            guard rhs.hasPrefix("\""), rhs.count >= 2 else { continue }
            let body = rhs.dropFirst()
            guard let close = body.firstIndex(of: "\"") else { continue }
            constants[name] = String(body[body.startIndex..<close])
        }
        return constants
    }

    private static func durableTokenLiterals(in lines: [SourceLine]) -> [String] {
        let constants = stringConstants(in: lines)
        var found: [String] = []
        for line in lines {
            var rest = Substring(line.text)
            while let open = rest.firstIndex(of: "\"") {
                let afterOpen = rest.index(after: open)
                guard let close = rest[afterOpen...].firstIndex(of: "\"") else { break }
                let literal = String(rest[afterOpen..<close])
                if isDurableTokenShaped(literal) {
                    found.append(literal)
                } else if let resolved = resolvingLeadingInterpolation(literal, constants: constants),
                          isDurableTokenShaped(resolved) {
                    found.append(resolved)
                }
                rest = rest[rest.index(after: close)...]
            }
        }
        return found
    }

    private static func isDurableTokenShaped(_ literal: String) -> Bool {
        guard let hyphen = literal.firstIndex(of: "-"), hyphen != literal.startIndex else {
            return false
        }
        guard literal[literal.startIndex].isLetter else { return false }
        guard literal[literal.startIndex..<hyphen].allSatisfy({ $0.isLetter || $0.isNumber }) else {
            return false
        }
        return !literal.contains(where: { $0.isWhitespace || $0 == ":" })
    }

    /// R2-Fix5: R1 left the budget discriminator "uncovered by construction"
    /// against a THIRD 30 s budget. This closes it by ENUMERATION rather than
    /// by argument.
    ///
    /// `FMDaemonRefusal.isMetadataStall` asks `deadline == FMInferenceDeadline
    /// .metadata`, so its soundness is exactly the claim "no other budget
    /// reaching `FMInferenceDeadline.run` is 30 s". That set is small and
    /// closed: every call site in production passes `FMInferenceDeadline
    /// .metadata`, `FMInferenceDeadline.standard`, or the injected
    /// `inferenceDeadline` whose default is `standard`. If a fourth spelling
    /// appears — a literal `.seconds(30)`, a new constant, a config default
    /// moved off `standard` — a genuine 300 s inference timeout would start
    /// deferring with an unbounded retry, and nothing else in the suite would
    /// notice. This test is what notices.
    /// R4-Fix3: judge the ARGUMENT, over a whitespace-free read of the file.
    ///
    /// The pre-R4 form asked two questions per line — "does this line contain
    /// `FMInferenceDeadline.run(`" and "does this line also mention an allowed
    /// name" — and four probes got a third 30 s budget past it:
    ///
    ///   CN-C1  `FMInferenceDeadline.run(FMInferenceDeadline.standard / 10)`.
    ///          Thirty seconds exactly, on a line that names an allowed budget,
    ///          so the allow-check said yes. `contains` cannot tell a budget
    ///          from an expression that merely mentions one.
    ///   CN-C2  a new site split over two lines. Invisible — and FREE, because
    ///          the site count is a floor: a site that DISAPPEARS is red, a site
    ///          nobody can see is not.
    ///   CN-C3b `FMInferenceDeadline . run(`.
    ///   CN-C4  the budget in a local, the allowed name only in a trailing
    ///          comment on the call line.
    ///
    /// So the finder now runs over the file with all whitespace removed, and
    /// the first argument is extracted by balanced-paren scan and matched
    /// EXACTLY. `.seconds(30)`, `shortBudget` and `FMInferenceDeadline.standard/10`
    /// are all offenders; nothing that merely mentions an allowed name passes.
    /// The FIRST argument of every `marker`-introduced call or interpolation in a
    /// dense read, by balanced-paren scan.
    ///
    /// R4-Fix3 wrote this scan for the budget canary. R5 lifted it out rather
    /// than writing a second copy for the sibling-count canary below: judging the
    /// ARGUMENT — not the line, and not whether the line merely mentions
    /// something — is what four R4 probes bought, and it should be bought once.
    static func firstArguments(after marker: String, in dense: String) -> [String] {
        var arguments: [String] = []
        var searchRange = dense.startIndex..<dense.endIndex
        while let call = dense.range(of: marker, range: searchRange) {
            var depth = 1
            var index = call.upperBound
            let start = index
            var end: String.Index?
            while index < dense.endIndex {
                let character = dense[index]
                if character == "(" || character == "[" {
                    depth += 1
                } else if character == ")" || character == "]" {
                    depth -= 1
                    if depth == 0 { end = index; break }
                } else if character == "," && depth == 1 {
                    end = index
                    break
                }
                index = dense.index(after: index)
            }
            arguments.append(String(dense[start..<(end ?? dense.endIndex)]))
            searchRange = call.upperBound..<dense.endIndex
        }
        return arguments
    }

    private static func inferenceBudgetArguments(in dense: String) -> [String] {
        firstArguments(after: "FMInferenceDeadline.run(", in: dense)
    }

    func testEveryProductionInferenceBudgetIsEnumerated() throws {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Playhead", isDirectory: true)
        let walker = try XCTUnwrap(
            FileManager.default.enumerator(at: root, includingPropertiesForKeys: nil),
            "could not walk \(root.path)"
        )
        let files = walker.compactMap { $0 as? URL }.filter { $0.pathExtension == "swift" }
        XCTAssertGreaterThan(files.count, 100, "source walk found only \(files.count) Swift files")

        // Measured, not guessed: 9 call sites at R2 review, re-counted at R4 —
        // 4 `metadata` (tokenCount plus three schema sizings), 4 injected
        // `inferenceDeadline` (defaulted to `standard`, pinned below), 1
        // literal `standard` in the readiness probe.
        let allowed: Set<String> = [
            "FMInferenceDeadline.metadata",
            "FMInferenceDeadline.standard",
            "inferenceDeadline"
        ]
        var callSites: [String] = []
        var offenders: [String] = []
        for file in files {
            let lines = Self.codeLines(of: try String(contentsOf: file, encoding: .utf8))
            for argument in Self.inferenceBudgetArguments(in: Self.denseCode(lines)) {
                let site = "\(file.lastPathComponent): FMInferenceDeadline.run(\(argument))"
                callSites.append(site)
                if !allowed.contains(argument) {
                    offenders.append(site)
                }
            }
        }

        // Vacuity guard: the walk must have found the call sites it judges.
        XCTAssertGreaterThanOrEqual(callSites.count, 9, "found only \(callSites.count) FMInferenceDeadline.run call sites")
        XCTAssertTrue(
            offenders.isEmpty,
            """
            An inference budget outside the enumerated set reaches `FMInferenceDeadline.run`. \
            `FMDaemonRefusal.isMetadataStall` discriminates on `deadline == FMInferenceDeadline.metadata`, \
            so a third budget of 30 s silently turns a real inference timeout into a deferrable daemon \
            refusal — an unbounded retry on evidence about the MODEL. The argument must BE one of \
            \(allowed.sorted()), not merely mention one: `FMInferenceDeadline.standard / 10` is thirty \
            seconds. Add the budget to the enumeration here and give it a rail, or route it through an \
            existing one.
            \(offenders.joined(separator: "\n"))
            """
        )

        // R2-Fix5 called this "the one budget a caller can move without touching
        // a call site". `Config.default` omits `inferenceDeadline`, so this pin
        // does reach that init's default transitively.
        XCTAssertEqual(
            FoundationModelClassifier.Config.default.inferenceDeadline,
            FMInferenceDeadline.standard,
            "the classifier's default inference budget moved off `standard`; if it is now 30 s, every inference timeout defers."
        )
        XCTAssertNotEqual(
            FoundationModelClassifier.Config.default.inferenceDeadline,
            FMInferenceDeadline.metadata
        )

        // R4-Fix6: THERE ARE THREE, and R2 pinned one.
        //
        // The enumeration above is derived — every `FMInferenceDeadline.run`
        // call site, judged — but the DEFAULTS were not: `PermissiveAdClassifier
        // .init` and the classifier's inner FM-call type each declare their own
        // `inferenceDeadline: Duration = …`, and nothing looked at either. Move
        // one to 30 s and every genuine inference timeout through that seam
        // becomes a daemon refusal with an unbounded retry, while the call site
        // still reads `inferenceDeadline` and stays allowed. That is R3-Fix1's
        // shape one layer out: a set closed by enumeration on one axis and by
        // hand on the other.
        //
        // Derived from source, floor measured at THREE at R4, and each default
        // must BE `FMInferenceDeadline.standard` rather than merely mention it.
        var deadlineDefaults: [String] = []
        var movedDefaults: [String] = []
        for file in files {
            let dense = Self.denseCode(Self.codeLines(of: try String(contentsOf: file, encoding: .utf8)))
            var searchRange = dense.startIndex..<dense.endIndex
            while let hit = dense.range(of: "inferenceDeadline:Duration=", range: searchRange) {
                let value = String(dense[hit.upperBound...].prefix { $0 != "," && $0 != ")" && $0 != "{" })
                let site = "\(file.lastPathComponent): inferenceDeadline: Duration = \(value)"
                deadlineDefaults.append(site)
                if value != "FMInferenceDeadline.standard" {
                    movedDefaults.append(site)
                }
                searchRange = hit.upperBound..<dense.endIndex
            }
        }
        XCTAssertGreaterThanOrEqual(
            deadlineDefaults.count,
            3,
            "found only \(deadlineDefaults.count) injected-deadline defaults; the derivation lost one"
        )
        XCTAssertTrue(
            movedDefaults.isEmpty,
            """
            An injected inference budget DEFAULTS to something other than `FMInferenceDeadline.standard`. \
            Every `FMInferenceDeadline.run` call site taking `inferenceDeadline` is allowed precisely \
            because that is what it resolves to; a default of 30 s makes `FMDaemonRefusal.isMetadataStall` \
            true for every genuine inference timeout through that seam, which then DEFERS with `retryCount` \
            preserved and retries without bound.
            \(movedDefaults.joined(separator: "\n"))
            """
        )
    }
}
