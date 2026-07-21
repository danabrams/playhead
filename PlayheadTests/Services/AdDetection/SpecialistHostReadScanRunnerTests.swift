// SpecialistHostReadScanRunnerTests.swift
// playhead-b6jq PR 4: pin the backfill-runner wiring for the specialist
// host-read scan phase — the two-key gate, raw-verdict persistence, default-OFF
// byte-identity, the phone-gate (nil runtime) path, and thermal deferral.
//
// None of these boot a real model: a STUB `SpecialistAdClassifier.Runtime`
// returns a fixed verdict, and a `TestFMRuntime` drives the FM phases.

import Foundation
import Testing

@testable import Playhead

@Suite("BackfillJobRunner specialist host-read scan (playhead-b6jq PR4)")
struct SpecialistHostReadScanRunnerTests {

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

    private func makePlannerContext() -> CoveragePlannerContext {
        CoveragePlannerContext(
            observedEpisodeCount: 0,
            stableRecall: false,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 0,
            periodicFullRescanIntervalEpisodes: 10
        )
    }

    /// Inputs whose transcript carries real ad copy at 30..60s so the planner's
    /// catalog + lexical gate selects a non-empty candidate set.
    private func makeInputs(
        assetId: String,
        transcriptVersion: String = "tx-spec-v1"
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
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-spec",
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            transcriptVersion: transcriptVersion,
            plannerContext: makePlannerContext()
        )
    }

    /// A deterministic stub runtime returning a fixed verdict for every window.
    private func makeStubRuntime(
        isAd: Bool = true,
        confidence: Double = 0.83,
        adClass: String? = "hostRead"
    ) -> SpecialistAdClassifier.Runtime {
        SpecialistAdClassifier.Runtime(makeSession: {
            SpecialistAdClassifier.Runtime.Session(classify: { _ in
                SpecialistVerdict(isAd: isAd, confidence: confidence, adClass: adClass)
            })
        })
    }

    private func makeRunner(
        store: AnalysisStore,
        fmRuntime: FoundationModelClassifier.Runtime,
        specialistRuntime: SpecialistAdClassifier.Runtime?,
        specialistScanEnabled: Bool,
        snapshot: @escaping @Sendable () -> CapabilitySnapshot = { makePermissiveCapabilitySnapshot() }
    ) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntime, config: .default),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { snapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            specialistRuntime: specialistRuntime,
            specialistScanEnabled: specialistScanEnabled
        )
    }

    private func windowKey(_ start: Double, _ end: Double) -> String {
        "\(start)-\(end)"
    }

    // MARK: - Config default guard

    @Test("AdDetectionConfig.default.specialistScanEnabled is OFF")
    func configDefaultIsOff() {
        #expect(AdDetectionConfig.default.specialistScanEnabled == false)
        // Memberwise default is also OFF (a caller that omits it gets inert).
        let config = AdDetectionConfig(
            candidateThreshold: 0.4,
            confirmationThreshold: 0.7,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90,
            detectorVersion: "detection-v1",
            fmBackfillMode: .full,
            fmScanBudgetSeconds: 300,
            fmConsensusThreshold: 2
        )
        #expect(config.specialistScanEnabled == false)
    }

    // MARK: - Phase persists raw verdicts

    @Test("flag ON + stub runtime: persists a raw verdict per selected window; jobPhase tagged; NO ad_windows")
    func persistsRawVerdictsForSelectedWindows() async throws {
        let assetId = "asset-spec-persist"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId)

        // What the planner will select (the scan should persist exactly these).
        let expected = SpecialistScanPlanner().selectWindows(
            segments: inputs.segments,
            evidenceCatalog: inputs.evidenceCatalog,
            featureWindows: [],
            budget: SpecialistScanPlanner.defaultBudget
        )
        #expect(!expected.isEmpty, "test setup: the fixture must yield candidate windows")

        let runner = makeRunner(
            store: store,
            fmRuntime: TestFMRuntime().runtime,
            specialistRuntime: makeStubRuntime(isAd: true, confidence: 0.83, adClass: "hostRead"),
            specialistScanEnabled: true
        )
        _ = try await runner.runPendingBackfill(for: inputs)

        let rows = try await store.fetchSpecialistScanResults(analysisAssetId: assetId)
        #expect(rows.count == expected.count)

        let expectedKeys = Set(expected.map { windowKey($0.startTime, $0.endTime) })
        let rowKeys = Set(rows.map { windowKey($0.windowStartTime, $0.windowEndTime) })
        #expect(rowKeys == expectedKeys, "persisted windows must match the planner's selection verbatim")

        for row in rows {
            #expect(row.probabilityOfAd == 0.83)
            #expect(row.isAd == true)
            #expect(row.adClass == "hostRead")
            #expect(row.modelVersion == SpecialistModelResources.modelFolderName)
            #expect(row.jobPhase == BackfillJobPhase.specialistHostReadScan.rawValue)
        }

        // Acts on nothing: no ad_windows / skip rows are written by the scan.
        let adWindows = try await store.fetchAdWindows(assetId: assetId)
        #expect(adWindows.isEmpty, "the specialist scan must not write ad_windows")

        // The specialist job exists and completed.
        let plan = CoveragePlanner().plan(for: inputs.plannerContext)
        let specialistJobId = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: assetId,
            transcriptVersion: inputs.transcriptVersion,
            phase: .specialistHostReadScan,
            offset: plan.phases.count
        )
        let job = try await store.fetchBackfillJob(byId: specialistJobId)
        #expect(job?.status == .complete)
    }

    @Test("re-running is idempotent: identical inputs collapse onto the same rows")
    func rerunIsIdempotent() async throws {
        let assetId = "asset-spec-idem"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId)

        let runner = makeRunner(
            store: store,
            fmRuntime: TestFMRuntime().runtime,
            specialistRuntime: makeStubRuntime(),
            specialistScanEnabled: true
        )
        _ = try await runner.runPendingBackfill(for: inputs)
        let firstCount = try await store.fetchSpecialistScanResults(analysisAssetId: assetId).count
        #expect(firstCount > 0)

        // A second identical run must not double the rows (reuseKeyHash + INSERT
        // OR REPLACE), and the completed job is skipped by M-5 idempotency.
        _ = try await runner.runPendingBackfill(for: inputs)
        let secondCount = try await store.fetchSpecialistScanResults(analysisAssetId: assetId).count
        #expect(secondCount == firstCount)
    }

    // MARK: - Default-OFF byte identity

    @Test("default-OFF (flag false, runtime non-nil): zero rows, no specialist job, FM admits unchanged")
    func defaultOffIsByteIdentical() async throws {
        let assetId = "asset-spec-off"
        let transcriptVersion = "tx-off-v1"

        // Baseline: a runner with NO specialist wiring at all (production default
        // shape).
        let baselineStore = try await makeTestStore()
        try await baselineStore.insertAsset(makeAsset(id: assetId))
        let baselineRunner = BackfillJobRunner(
            store: baselineStore,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: TestFMRuntime().runtime, config: .default),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )
        let baseline = try await baselineRunner.runPendingBackfill(
            for: makeInputs(assetId: assetId, transcriptVersion: transcriptVersion))

        // Flag OFF but runtime PRESENT: the two-key gate is not satisfied, so
        // the behavior must match the baseline byte-for-byte.
        let offStore = try await makeTestStore()
        try await offStore.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId, transcriptVersion: transcriptVersion)
        let offRunner = makeRunner(
            store: offStore,
            fmRuntime: TestFMRuntime().runtime,
            specialistRuntime: makeStubRuntime(),
            specialistScanEnabled: false
        )
        let offResult = try await offRunner.runPendingBackfill(for: inputs)

        // Zero specialist rows, no specialist backfill_job.
        #expect(try await offStore.fetchSpecialistScanResults(analysisAssetId: assetId).isEmpty)
        let plan = CoveragePlanner().plan(for: inputs.plannerContext)
        let specialistJobId = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            phase: .specialistHostReadScan,
            offset: plan.phases.count
        )
        #expect(try await offStore.fetchBackfillJob(byId: specialistJobId) == nil)
        #expect(!offResult.admittedJobIds.contains(specialistJobId))

        // Byte identity vs baseline: the admitted FM job set is identical — the
        // FM path is reached and enqueued exactly as before (job ids are
        // content-hashed from asset/version/phase/offset, so they match across
        // the two fresh stores).
        #expect(Set(offResult.admittedJobIds) == Set(baseline.admittedJobIds))
        #expect(!offResult.admittedJobIds.isEmpty, "the FM full-episode job must still admit")
    }

    @Test("flag ON but runtime nil (phone gate): zero rows, no specialist job")
    func flagOnRuntimeNilIsNoOp() async throws {
        let assetId = "asset-spec-nilrt"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId)

        // This mirrors the simulator / unstaged-model production path, where
        // `makeLiveRuntime` returns nil: even with the flag ON, nothing runs.
        let runner = makeRunner(
            store: store,
            fmRuntime: TestFMRuntime().runtime,
            specialistRuntime: nil,
            specialistScanEnabled: true
        )
        let result = try await runner.runPendingBackfill(for: inputs)

        #expect(try await store.fetchSpecialistScanResults(analysisAssetId: assetId).isEmpty)
        let plan = CoveragePlanner().plan(for: inputs.plannerContext)
        let specialistJobId = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: assetId,
            transcriptVersion: inputs.transcriptVersion,
            phase: .specialistHostReadScan,
            offset: plan.phases.count
        )
        #expect(try await store.fetchBackfillJob(byId: specialistJobId) == nil)
        #expect(!result.admittedJobIds.contains(specialistJobId))
    }

    // MARK: - Thermal deferral

    @Test("thermal-throttled admission defers the specialist job; zero rows written")
    func thermalDefersSpecialistJob() async throws {
        let assetId = "asset-spec-thermal"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId)

        let runner = makeRunner(
            store: store,
            fmRuntime: TestFMRuntime().runtime,
            specialistRuntime: makeStubRuntime(),
            specialistScanEnabled: true,
            snapshot: { makeThermalThrottledSnapshot() }
        )
        let result = try await runner.runPendingBackfill(for: inputs)

        // Nothing admitted, everything deferred — including the specialist job.
        #expect(result.admittedJobIds.isEmpty)
        let plan = CoveragePlanner().plan(for: inputs.plannerContext)
        let specialistJobId = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: assetId,
            transcriptVersion: inputs.transcriptVersion,
            phase: .specialistHostReadScan,
            offset: plan.phases.count
        )
        #expect(result.deferredJobIds.contains(specialistJobId))
        let job = try await store.fetchBackfillJob(byId: specialistJobId)
        #expect(job?.status == .deferred)

        // A deferred scan runs nothing => zero rows.
        #expect(try await store.fetchSpecialistScanResults(analysisAssetId: assetId).isEmpty)
    }
}
