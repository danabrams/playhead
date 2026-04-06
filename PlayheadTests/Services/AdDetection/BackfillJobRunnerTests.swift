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
            scanCohortJSON: makeTestScanCohortJSON(),
            decisionCohortJSON: nil
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
