// BackfillJobRunnerTests.swift
// Phase 3 shadow-mode runner. These tests pin the orchestration contract:
// plan -> enqueue via AdmissionController -> run FM coarse pass -> persist
// SemanticScanResult / EvidenceEvent rows. None of the tests boot the real
// Foundation Models stack; they use TestFMRuntime.

import Foundation
import Testing

@testable import Playhead

@Suite("BackfillJobRunner")
struct BackfillJobRunnerTests {

    // MARK: - Fixtures

    private func makeAsset(id: String = "asset-runner") -> AnalysisAsset {
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

    private func makeInputs(
        assetId: String = "asset-runner",
        podcastId: String = "podcast-runner",
        transcriptVersion: String = "tx-runner-v1"
    ) -> BackfillJobRunner.AssetInputs {
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: [
                (0, 30, "Welcome to the show. Today we're discussing podcasts."),
                (30, 60, "Use code SHOW for 20 percent off at example dot com."),
                (60, 90, "Now back to the interview with our guest.")
            ]
        )
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments.flatMap(\.atoms),
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion
        )
        let plannerContext = CoveragePlannerContext(
            observedEpisodeCount: 0,
            stablePrecision: false,
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
        runtime: FoundationModelClassifier.Runtime,
        snapshot: CapabilitySnapshot = makePermissiveCapabilitySnapshot(),
        mode: FMBackfillMode = .shadow
    ) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime),
            coveragePlanner: CoveragePlanner(),
            mode: mode,
            capabilitySnapshotProvider: { snapshot },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )
    }

    // MARK: - Tests

    @Test("disabled mode runs no FM jobs and writes nothing")
    func disabledModeIsNoOp() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime()
        let runner = makeRunner(
            store: store,
            runtime: fmRuntime.runtime,
            mode: .disabled
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(result.admittedJobIds.isEmpty)
        #expect(result.scanResultIds.isEmpty)
        #expect(result.evidenceEventIds.isEmpty)
        #expect(await fmRuntime.coarseCallCount == 0)
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        #expect(scans.isEmpty)
        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        #expect(evidence.isEmpty)
    }

    @Test("shadow mode admits planned jobs, runs FM, persists scan results")
    func shadowModePersistsResults() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    transcriptQuality: .good,
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [1],
                        certainty: .strong
                    )
                )
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty)
        #expect(!result.scanResultIds.isEmpty)
        let coarseCalls = await fmRuntime.coarseCallCount
        #expect(coarseCalls >= 1)

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        #expect(!scans.isEmpty)
        #expect(scans.allSatisfy { $0.scanPass == "passA" || $0.scanPass == "passB" })
        // Shadow mode never inserts AdWindows -- that path is owned by lexical.
        let windows = try await store.fetchAdWindows(assetId: "asset-runner")
        #expect(windows.isEmpty)
    }

    @Test("admission throttling defers the job and records the reason")
    func thermalThrottleIsDeferred() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime()
        let runner = makeRunner(
            store: store,
            runtime: fmRuntime.runtime,
            snapshot: makeThermalThrottledSnapshot()
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(result.admittedJobIds.isEmpty)
        #expect(!result.deferredJobIds.isEmpty)
        let coarseCalls = await fmRuntime.coarseCallCount
        #expect(coarseCalls == 0)
        // Persisted jobs should be marked deferred with a reason.
        let job = try await store.fetchBackfillJob(byId: result.deferredJobIds.first!)
        #expect(job?.status == .deferred)
        #expect(job?.deferReason == "thermalThrottled")
    }

    @Test("task cancellation between jobs aborts the run")
    func cancellationBetweenJobsAborts() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime()
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let task = Task { [runner] in
            try await runner.runPendingBackfill(for: makeInputs())
        }
        task.cancel()

        do {
            _ = try await task.value
        } catch is CancellationError {
            // expected
            return
        } catch {
            // Acceptable: also valid for runner to bail with a thrown error
            // wrapping cancellation, but we expect the canonical CancellationError.
            #expect(Bool(false), "Expected CancellationError, got \(error)")
        }
    }

    @Test("refinement-pass persists evidence events with JSON-array atomOrdinals")
    func refinementPassPersistsEvidenceEvents() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [1],
                        certainty: .strong
                    )
                )
            ],
            refinementResponses: [
                RefinementWindowSchema(spans: [
                    SpanRefinementSchema(
                        commercialIntent: .paid,
                        ownership: .thirdParty,
                        firstLineRef: 1,
                        lastLineRef: 1,
                        certainty: .strong,
                        boundaryPrecision: .precise,
                        evidenceAnchors: [
                            EvidenceAnchorSchema(
                                evidenceRef: nil,
                                lineRef: 1,
                                kind: .ctaPhrase,
                                certainty: .strong
                            )
                        ],
                        alternativeExplanation: .none,
                        reasonTags: [.callToAction]
                    )
                ])
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty)
        // The refinement pass must actually persist evidence rows. Before the
        // C-1 fix this failed because the runner emitted comma-joined ordinals
        // like "1,2,3" which AnalysisStore.validateAtomOrdinalsJSON rejected.
        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        #expect(!evidence.isEmpty, "refinement pass must persist evidence_events rows")
        #expect(result.evidenceEventIds.count == evidence.count)
        // Every persisted row's atomOrdinals must be a JSON array parseable as [Int].
        for event in evidence {
            let data = Data(event.atomOrdinals.utf8)
            let parsed = try JSONDecoder().decode([Int].self, from: data)
            #expect(!parsed.isEmpty)
        }
    }

    @Test("refinement writes scan row and evidence events atomically")
    func refinementPassWritesAtomically() async throws {
        // C-3 regression. The runner previously wrote Pass-B scan rows and
        // evidence events with separate `insertSemanticScanResult` /
        // `insertEvidenceEvent` calls across `await` points, so a crash
        // between them would leave orphan scan rows. After the fix the
        // runner calls `recordSemanticScanResult(_:evidenceEvents:)` which
        // wraps both writes in a single SQLite transaction with rollback on
        // failure.
        //
        // We pin the happy-path invariant here: for each persisted passB
        // scan result, the count of evidence events attributed to the same
        // asset is non-zero, and every evidence row parses back to a valid
        // JSON array of integers (also pinning C-1).
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [1],
                        certainty: .strong
                    )
                )
            ],
            refinementResponses: [
                RefinementWindowSchema(spans: [
                    SpanRefinementSchema(
                        commercialIntent: .paid,
                        ownership: .thirdParty,
                        firstLineRef: 1,
                        lastLineRef: 1,
                        certainty: .strong,
                        boundaryPrecision: .precise,
                        evidenceAnchors: [
                            EvidenceAnchorSchema(
                                evidenceRef: nil,
                                lineRef: 1,
                                kind: .ctaPhrase,
                                certainty: .strong
                            )
                        ],
                        alternativeExplanation: .none,
                        reasonTags: [.callToAction]
                    )
                ])
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        _ = try await runner.runPendingBackfill(for: makeInputs())

        let passB = try await store.fetchSemanticScanResults(
            analysisAssetId: "asset-runner",
            scanPass: "passB"
        )
        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        #expect(!passB.isEmpty, "passB scan row must persist")
        #expect(evidence.count >= passB.count, "each passB row must have at least one evidence event")
    }

    @Test("runPendingBackfill is idempotent across invocations for the same asset")
    func runPendingBackfillIsIdempotent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .noAds,
                    support: nil
                ),
                CoarseScreeningSchema(
                    disposition: .noAds,
                    support: nil
                )
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        // First run should enqueue and admit the planned jobs.
        let first = try await runner.runPendingBackfill(for: makeInputs())
        #expect(!first.admittedJobIds.isEmpty, "first run must admit jobs")

        // Second run must not throw `duplicateJobId` — it should reuse the
        // existing rows. Jobs already completed stay off the queue; anything
        // deferred can be re-driven.
        let second = try await runner.runPendingBackfill(for: makeInputs())
        // After the fix we expect zero *new* admitted jobs, because the first
        // run completed every planned job.
        #expect(second.admittedJobIds.isEmpty, "completed jobs must not be re-admitted")
    }

    @Test("C-2: failing classifier persists deferReason on .failed status")
    func failedClassifierPersistsDeferReason() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        struct CoarseFailure: Error, CustomStringConvertible {
            let description = "synthetic classifier failure"
        }

        // Build a Runtime whose coarse pass always throws. We cannot mutate
        // TestFMRuntime without crossing ownership boundaries, so build the
        // Runtime struct inline.
        // We make `tokenCount` throw so `planPassA` → `coarsePassA` throws
        // out of the runtime entirely (bypassing the per-window failure
        // mapping) and trips the runner's terminal-failure catch branch.
        let failingRuntime = FoundationModelClassifier.Runtime(
            availabilityStatus: { _ in nil },
            contextSize: { 4_096 },
            tokenCount: { _ in throw CoarseFailure() },
            coarseSchemaTokenCount: { 16 },
            refinementSchemaTokenCount: { 32 },
            makeSession: {
                FoundationModelClassifier.Runtime.Session(
                    prewarm: { _ in },
                    respondCoarse: { _ in
                        CoarseScreeningSchema(disposition: .noAds, support: nil)
                    },
                    respondRefinement: { _ in
                        RefinementWindowSchema(spans: [])
                    }
                )
            }
        )
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: failingRuntime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty, "runner must have admitted at least one job")
        // After all admitted jobs failed, the stored row must reflect the
        // failure with a non-nil deferReason describing the cause.
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .failed)
        #expect(row.deferReason != nil)
        #expect(row.deferReason?.contains("synthetic classifier failure") == true)
    }

    @Test("C-B: runs do not re-admit jobs that have exhausted the retry budget")
    func exhaustedRetryBudgetIsNotReAdmitted() async throws {
        // The factory in PlayheadRuntime allocates a fresh AdmissionController
        // per invocation, so the controller's in-memory retry budget resets
        // between runs. The persisted retryCount on the backfill_jobs row is
        // the only source of truth. When a prior run has left a row in
        // `.failed` with `retryCount >= AdmissionController.maxRetries`, the
        // runner must skip it entirely — no re-enqueue, no FM call, no status
        // change.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Seed a failed job matching the deterministic jobId format the
        // runner synthesizes for a cold-start fullEpisodeScan plan.
        let exhausted = BackfillJob(
            jobId: "fm-asset-runner-fullEpisodeScan-0",
            analysisAssetId: "asset-runner",
            podcastId: "podcast-runner",
            phase: .fullEpisodeScan,
            coveragePolicy: .fullCoverage,
            priority: 5,
            progressCursor: nil,
            retryCount: AdmissionController.maxRetries,
            deferReason: "prior failure",
            status: .failed,
            scanCohortJSON: makeTestScanCohortJSON(),
            createdAt: Date().timeIntervalSince1970
        )
        try await store.insertBackfillJob(exhausted)

        let fmRuntime = TestFMRuntime()
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(result.admittedJobIds.isEmpty, "exhausted job must not be admitted")
        #expect(result.deferredJobIds.isEmpty, "exhausted job must not be re-deferred")
        #expect(result.scanResultIds.isEmpty)
        #expect(await fmRuntime.coarseCallCount == 0, "FM must not be called")
        let row = try #require(await store.fetchBackfillJob(byId: exhausted.jobId))
        #expect(row.status == .failed, "status must remain .failed")
        #expect(row.retryCount == AdmissionController.maxRetries)
    }

    @Test("C3-1: invalidStateTransition on a pre-failed row is logged and skipped, not re-failed")
    func preFailedRowTriggersInvalidStateTransition_runnerSkips() async throws {
        // Setup: pre-insert a `.failed` row with the deterministic jobId the
        // runner will synthesize, and retryCount=0 so the M-5 idempotency
        // path re-enqueues it (retryCount < maxRetries). Once the drain
        // calls `markBackfillJobRunning`, the C-2 guard rejects
        // `.failed -> .running` and throws `invalidStateTransition`.
        //
        // The C3-1 catch arm recognises the typed store error, logs, and
        // continues the drain. It must NOT route the error through the
        // generic `catch` branch that calls `markBackfillJobFailed` — the
        // row is already terminal and the secondary write is wasted work
        // (and, pre-fix-1, would double-bump retryCount). This test pins
        // the no-op: the row must stay at exactly the state we seeded.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let failedJob = BackfillJob(
            jobId: "fm-asset-runner-fullEpisodeScan-0",
            analysisAssetId: "asset-runner",
            podcastId: "podcast-runner",
            phase: .fullEpisodeScan,
            coveragePolicy: .fullCoverage,
            priority: 5,
            progressCursor: nil,
            retryCount: 0,
            deferReason: "seeded failure",
            status: .failed,
            scanCohortJSON: makeTestScanCohortJSON(),
            createdAt: Date().timeIntervalSince1970
        )
        try await store.insertBackfillJob(failedJob)

        let fmRuntime = TestFMRuntime()
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        // Must not throw: the C3-1 path handles the invalidStateTransition.
        let result = try await runner.runPendingBackfill(for: makeInputs())

        // FM must not have been called — the transition failed before
        // reaching the classifier.
        #expect(await fmRuntime.coarseCallCount == 0, "FM must not run for a terminal row")
        #expect(result.scanResultIds.isEmpty)
        #expect(result.evidenceEventIds.isEmpty)

        let row = try #require(await store.fetchBackfillJob(byId: failedJob.jobId))
        #expect(row.status == .failed, "row must remain .failed")
        #expect(row.retryCount == 0, "retryCount must not be bumped by the C3-1 skip path")
        #expect(row.deferReason == "seeded failure",
                "the seeded failure reason must be preserved, not overwritten by the invalidStateTransition cascade")
    }

    @Test("H-1: thermal defer marks ALL planned jobs, not just the first")
    func thermalDeferMarksAllPlannedJobs() async throws {
        // Use a planner context that emits the 3-phase targeted plan
        // (harvester, likely-ad-slots, audit). Previously the runner broke
        // out of the drain loop after marking only the first queued job as
        // deferred, leaving the other two stuck in `.queued` forever.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime()
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makeThermalThrottledSnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )

        let plannerContext = CoveragePlannerContext(
            observedEpisodeCount: 20,
            stablePrecision: true,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 0,
            periodicFullRescanIntervalEpisodes: 10
        )
        let base = makeInputs()
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: base.analysisAssetId,
            podcastId: base.podcastId,
            segments: base.segments,
            evidenceCatalog: base.evidenceCatalog,
            transcriptVersion: base.transcriptVersion,
            plannerContext: plannerContext
        )

        let result = try await runner.runPendingBackfill(for: inputs)

        #expect(result.admittedJobIds.isEmpty)
        #expect(result.deferredJobIds.count == 3, "all 3 planned jobs must be marked deferred (got \(result.deferredJobIds.count))")

        for jobId in result.deferredJobIds {
            let row = try #require(await store.fetchBackfillJob(byId: jobId))
            #expect(row.status == .deferred, "job \(jobId) must be .deferred, got \(row.status)")
            #expect(row.deferReason == "thermalThrottled")
        }
    }

    @Test("H-3: re-run with deterministic inputs produces no duplicate scan rows")
    func rerunProducesNoDuplicateScanRows() async throws {
        // Deterministic fake: same asset, same transcript, two runs in
        // sequence. The persisted scan_results count must equal the unique
        // (assetId, scanPass, windowIndex) triples. Before the fix the
        // runner stamped a random UUID suffix into each row id, so a crash
        // mid-run could leave an orphan row under a different id.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(disposition: .noAds, support: nil),
                CoarseScreeningSchema(disposition: .noAds, support: nil)
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        _ = try await runner.runPendingBackfill(for: makeInputs())
        let firstRows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")

        // Force the job row back to `.queued` so the second run actually
        // reprocesses it rather than skipping via the `.complete` fast path.
        // This simulates the orphan-recovery scenario the fix targets: a
        // prior run wrote scan rows but did not mark the job complete.
        for jobId in firstRows.map(\.id) {
            _ = jobId // silence warning; we reuse the variable below
        }
        // We need a job row that still allows re-enqueue. Use the deprecated
        // combined checkpoint API to force the status back to .queued —
        // direct `markBackfillJobDeferred` can no longer demote a terminal
        // row after the C-R3-1 guard fix.
        let jobId = "fm-asset-runner-fullEpisodeScan-0"
        try await store.checkpointBackfillJob(
            jobId: jobId,
            progressCursor: nil,
            status: .queued
        )

        _ = try await runner.runPendingBackfill(for: makeInputs())
        let secondRows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")

        // Count unique logical keys across all persisted rows.
        var uniqueKeys = Set<String>()
        for row in secondRows {
            uniqueKeys.insert("\(row.analysisAssetId)|\(row.scanPass)|\(row.windowFirstAtomOrdinal)|\(row.windowLastAtomOrdinal)")
        }
        #expect(secondRows.count == uniqueKeys.count,
                "expected no duplicate rows: \(secondRows.count) rows vs \(uniqueKeys.count) unique keys")
        // And the re-run must not have grown the table beyond the first
        // run's unique-key count.
        var firstKeys = Set<String>()
        for row in firstRows {
            firstKeys.insert("\(row.analysisAssetId)|\(row.scanPass)|\(row.windowFirstAtomOrdinal)|\(row.windowLastAtomOrdinal)")
        }
        #expect(uniqueKeys == firstKeys, "second run introduced new logical keys")
    }

    @Test("H-R3-2: permanent store errors exhaust retries immediately, not after maxRetries attempts")
    func permanentStoreErrorsExhaustRetriesImmediately() async throws {
        // H-R3-2: `AnalysisStoreError.invalidEvidenceEvent`,
        // `.evidenceEventBodyMismatch`, `.invalidScanCohortJSON`, and
        // `.invalidRow` are permanent — replaying the same inputs against
        // the same schema will always fail the same validator. Burning
        // through the retry budget on them is wasted work. The runner must
        // classify them as permanent and short-circuit the retry counter
        // to `maxRetries`, so the next run's C-B gate skips the row.
        //
        // We inject a classifier whose refinement pass returns a span with
        // an evidence anchor whose line refs point outside the segment
        // window. The runner's `makeEvidenceEvents` builder encodes this
        // into an atomOrdinals JSON array that the store's validator
        // rejects with `invalidEvidenceEvent` when the transcript version
        // mismatches the catalog's expected version (H-1 integrity check).
        //
        // Simpler path: use an invalid scanCohortJSON. That is rejected by
        // `insertSemanticScanResult` as `invalidScanCohortJSON` and is
        // genuinely permanent — the cohort is fixed at runner init, so
        // every retry will hit the same error.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(disposition: .noAds, support: nil)
            ]
        )
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            // Malformed cohort JSON: not a valid JSON object. The store's
            // `validateScanCohortJSON` rejects this with
            // `AnalysisStoreError.invalidScanCohortJSON`.
            scanCohortJSON: "not-json"
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())
        #expect(!result.admittedJobIds.isEmpty, "runner must have admitted at least one job")

        // After the permanent failure path the row must be `.failed` with
        // `retryCount == maxRetries`, so the C-B gate skips it on re-runs.
        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .failed)
        #expect(row.retryCount == AdmissionController.maxRetries,
                "permanent error must short-circuit retryCount to maxRetries, got \(row.retryCount)")

        // Second run must skip the exhausted row entirely.
        let fmCallsBefore = await fmRuntime.coarseCallCount
        let second = try await runner.runPendingBackfill(for: makeInputs())
        let fmCallsAfter = await fmRuntime.coarseCallCount
        #expect(second.admittedJobIds.isEmpty, "exhausted row must not be re-admitted")
        #expect(fmCallsAfter == fmCallsBefore,
                "FM must not be called again for an exhausted permanent failure")
    }

    @Test("C-R3-2: scan results from different transcript versions coexist under distinct ids")
    func scanResultsFromDifferentTranscriptVersionsCoexist() async throws {
        // C-R3-2: the deterministic scan id was `scan-{assetId}-{pass}-{idx}`,
        // which omitted the transcriptVersion. The `semantic_scan_results`
        // PK is `id`; UNIQUE is on `reuseKeyHash`, which DOES include the
        // transcriptVersion. Two runs with different transcript versions
        // therefore produced distinct reuseKeyHash values but a colliding
        // PK — the INSERT OR REPLACE then silently nuked the prior run's
        // row, defeating H-1's success-protection guard (which only probes
        // by reuseKeyHash).
        //
        // Fix: include the transcriptVersion in the id itself. Two runs of
        // the same asset under different transcriptVersions must produce
        // two persisted rows, neither overwriting the other.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(disposition: .noAds, support: nil),
                CoarseScreeningSchema(disposition: .noAds, support: nil)
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        // First run under transcriptVersion "v1".
        _ = try await runner.runPendingBackfill(
            for: makeInputs(transcriptVersion: "tx-runner-v1")
        )
        let v1Rows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        #expect(!v1Rows.isEmpty, "v1 run must persist at least one scan row")

        // Force the job row back so the second run actually re-runs the
        // passA pipeline; the job ids don't depend on transcriptVersion.
        let jobId = "fm-asset-runner-fullEpisodeScan-0"
        try await store.checkpointBackfillJob(
            jobId: jobId,
            progressCursor: nil,
            status: .queued
        )

        // Second run under a new transcriptVersion. Same asset, same window
        // indices — only the transcriptVersion differs.
        _ = try await runner.runPendingBackfill(
            for: makeInputs(transcriptVersion: "tx-runner-v2")
        )
        let allRows = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")

        // Every v1 row must still exist after the v2 run.
        let idsAfter = Set(allRows.map(\.id))
        for v1 in v1Rows {
            #expect(idsAfter.contains(v1.id),
                    "v1 row \(v1.id) must survive a v2 re-run (C-R3-2 regression)")
        }

        // Both transcript versions must be represented in the stored rows.
        let versions = Set(allRows.map(\.transcriptVersion))
        #expect(versions.contains("tx-runner-v1"), "v1 rows must be present after v2 run")
        #expect(versions.contains("tx-runner-v2"), "v2 rows must be present after v2 run")
    }

    @Test("HIGH-1: concurrent runBackfill calls with per-call controllers do not mass-defer each other")
    func concurrentRunBackfillsDoNotMassDeferEachOther() async throws {
        // HIGH-1 regression: the round-2 M-B hoist made the runtime factory
        // capture a single shared AdmissionController. Because
        // AdDetectionService is actor-reentrant on `await`, two concurrent
        // `runBackfill` calls on different episodes would each hit the
        // shared controller. The second call saw `runningJob != nil`,
        // mass-deferred its whole batch with `serialBusy`, and lost
        // telemetry. The correct wiring is per-call controllers (one per
        // `runBackfill` invocation), which this test pins by mimicking the
        // runtime factory: allocate a fresh controller for each runner.
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-concurrent-A"))
        try await store.insertAsset(makeAsset(id: "asset-concurrent-B"))

        let fmRuntimeA = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(disposition: .noAds, support: nil)
            ]
        )
        let fmRuntimeB = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(disposition: .noAds, support: nil)
            ]
        )

        // Per-call admission controllers, matching the corrected factory.
        let runnerA = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntimeA.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )
        let runnerB = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntimeB.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )

        async let a = runnerA.runPendingBackfill(
            for: makeInputs(assetId: "asset-concurrent-A", transcriptVersion: "tx-A")
        )
        async let b = runnerB.runPendingBackfill(
            for: makeInputs(assetId: "asset-concurrent-B", transcriptVersion: "tx-B")
        )
        let (resultA, resultB) = try await (a, b)

        #expect(!resultA.admittedJobIds.isEmpty, "runner A must admit its jobs")
        #expect(!resultB.admittedJobIds.isEmpty, "runner B must admit its jobs")
        #expect(resultA.deferredJobIds.isEmpty, "runner A must not mass-defer")
        #expect(resultB.deferredJobIds.isEmpty, "runner B must not mass-defer")

        // Neither run should have left a `serialBusy` defer reason behind.
        for jobId in resultA.admittedJobIds + resultB.admittedJobIds {
            let row = try #require(await store.fetchBackfillJob(byId: jobId))
            #expect(row.deferReason != "serialBusy",
                    "concurrent runs must not poison each other with serialBusy")
        }
    }

    @Test("enabled mode falls back to shadow behavior (no AdWindow writes)")
    func enabledModeFallsBackToShadow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    transcriptQuality: .good,
                    disposition: .containsAd,
                    support: CoarseSupportSchema(supportLineRefs: [1], certainty: .strong)
                )
            ]
        )
        let runner = makeRunner(
            store: store,
            runtime: fmRuntime.runtime,
            mode: .enabled
        )

        _ = try await runner.runPendingBackfill(for: makeInputs())

        let windows = try await store.fetchAdWindows(assetId: "asset-runner")
        #expect(windows.isEmpty, "enabled mode must not yet write AdWindows")
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        #expect(!scans.isEmpty, "FM must still run and persist results in fallback")
    }
}
