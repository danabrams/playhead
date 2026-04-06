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
