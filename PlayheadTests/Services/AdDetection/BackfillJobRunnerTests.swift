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

    private func makeTargetedInputs(
        assetId: String = "asset-targeted",
        podcastId: String = "podcast-targeted",
        transcriptVersion: String = "tx-targeted-v1",
        plannerContext: CoveragePlannerContext
    ) -> BackfillJobRunner.AssetInputs {
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: [
                (0, 10, "Welcome back to the show and today's interview."),
                (10, 20, "Before we continue, this episode is brought to you by ExampleCo."),
                (20, 30, "Visit example.com slash deal and use promo code PLAYHEAD."),
                (30, 40, "Now back to the conversation with our guest."),
                (40, 50, "We discuss the roadmap and next release milestones."),
                (50, 60, "The panel shares lessons learned from production incidents."),
                (60, 70, "Audience questions focus on reliability and performance."),
                (70, 80, "Thanks for listening and see you next week.")
            ]
        )
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments.flatMap(\.atoms),
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion
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
        mode: FMBackfillMode = .shadow,
        classifierConfig: FoundationModelClassifier.Config = .default
    ) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime, config: classifierConfig),
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

    @Test("playhead-nlh: targeted phases persist distinct passA rows on narrowed subsets")
    func targetedPhasesRunOnNarrowedSubsets() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-targeted-narrow"
        try await store.insertAsset(makeAsset(id: assetId))

        let runtime = TestFMRuntime()
        let runner = makeRunner(store: store, runtime: runtime.runtime)
        let targetedContext = CoveragePlannerContext(
            observedEpisodeCount: 20,
            stableRecall: true,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 1,
            periodicFullRescanIntervalEpisodes: 10
        )
        let inputs = makeTargetedInputs(
            assetId: assetId,
            podcastId: "podcast-targeted-narrow",
            transcriptVersion: "tx-targeted-narrow-v1",
            plannerContext: targetedContext
        )

        let result = try await runner.runPendingBackfill(for: inputs)

        #expect(result.admittedJobIds.count == 3, "targetedWithAudit should admit one job per targeted phase")
        let passA = try await store.fetchSemanticScanResults(
            analysisAssetId: assetId,
            scanPass: "passA"
        )
        #expect(passA.count == 3, "targeted phases must persist distinct passA rows (no cross-phase row collisions)")

        let fullFirst = try #require(inputs.segments.first?.firstAtomOrdinal)
        let fullLast = try #require(inputs.segments.last?.lastAtomOrdinal)
        let fullWidth = fullLast - fullFirst
        let fullLineRefs = Set(inputs.segments.map(\.segmentIndex))
        let submittedLineRefs = await runtime.snapshotSubmittedCoarseLineRefs()
        #expect(submittedLineRefs.count == 3, "targeted mode should submit one coarse window per targeted phase for this fixture")

        for refs in submittedLineRefs {
            let scannedSet = Set(refs)
            #expect(!refs.isEmpty)
            #expect(scannedSet.isSubset(of: fullLineRefs))
            #expect(scannedSet.count < fullLineRefs.count, "targeted phase should submit a strict subset of transcript lines")
            let sorted = refs.sorted()
            let contiguous = Array((sorted.first ?? 0)...(sorted.last ?? -1))
            #expect(sorted == contiguous, "targeted phase should submit a contiguous envelope, got \(sorted)")
        }

        for row in passA {
            #expect(row.windowFirstAtomOrdinal >= fullFirst)
            #expect(row.windowLastAtomOrdinal <= fullLast)
            #expect(
                row.windowLastAtomOrdinal - row.windowFirstAtomOrdinal < fullWidth,
                "targeted phase should scan a strict subset, got full-episode range \(row.windowFirstAtomOrdinal)-\(row.windowLastAtomOrdinal)"
            )
        }
    }

    @Test("playhead-nlh: full-rescan path records non-nil precision samples and unlocks targetedWithAudit")
    func fullRescanPersistsPrecisionSamplesAndUnlocksTargetedCoverage() async throws {
        let store = try await makeTestStore()
        let podcastId = "podcast-planner-live"
        let coarseResponses = (0..<5).map { _ in
            CoarseScreeningSchema(
                disposition: .containsAd,
                support: CoarseSupportSchema(
                    supportLineRefs: [2],
                    certainty: .strong
                )
            )
        }
        let runtime = TestFMRuntime(coarseResponses: coarseResponses)
        let runner = makeRunner(store: store, runtime: runtime.runtime)

        for episode in 1...5 {
            let assetId = "asset-planner-live-\(episode)"
            try await store.insertAsset(makeAsset(id: assetId))

            let state = try await store.fetchPodcastPlannerState(podcastId: podcastId)
            let context = CoveragePlannerContext(
                observedEpisodeCount: state?.observedEpisodeCount ?? 0,
                stableRecall: state?.stableRecallFlag ?? false,
                isFirstEpisodeAfterCohortInvalidation: false,
                recallDegrading: false,
                sponsorDriftDetected: false,
                auditMissDetected: false,
                episodesSinceLastFullRescan: state?.episodesSinceLastFullRescan ?? 0,
                periodicFullRescanIntervalEpisodes: 10
            )

            let inputs = makeTargetedInputs(
                assetId: assetId,
                podcastId: podcastId,
                transcriptVersion: "tx-planner-live-v\(episode)",
                plannerContext: context
            )
            _ = try await runner.runPendingBackfill(for: inputs)
        }

        let finalState = try #require(await store.fetchPodcastPlannerState(podcastId: podcastId))
        #expect(finalState.observedEpisodeCount == 5)
        #expect(!finalState.recallSamples.isEmpty, "live full-rescan path should persist precision samples")
        #expect(finalState.stableRecallFlag, "stable precision should flip true once sample and episode thresholds are met")

        let plannerContext = CoveragePlannerContext(
            observedEpisodeCount: finalState.observedEpisodeCount,
            stableRecall: finalState.stableRecallFlag,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: finalState.episodesSinceLastFullRescan,
            periodicFullRescanIntervalEpisodes: 10
        )
        let plan = CoveragePlanner().plan(for: plannerContext)
        #expect(plan.policy == .targetedWithAudit)
    }

    @available(iOS 26.0, *)
    @Test("coarse guardrail failures persist a terminal passA row even without windows")
    func coarseGuardrailFailuresPersistFailureRow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(coarseFailures: [.guardrailViolation])
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty)
        #expect(result.scanResultIds.count == 1)
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        #expect(scans.count == 1, "runner must persist a synthetic failure row for blocked coarse scans")
        let failure = try #require(scans.first)
        #expect(failure.scanPass == "passA")
        #expect(failure.status == .guardrailViolation)
        #expect(failure.disposition == .abstain)
        #expect(failure.windowFirstAtomOrdinal == 0)
        #expect(failure.windowLastAtomOrdinal == 2)
        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        #expect(evidence.isEmpty)
    }

    @available(iOS 26.0, *)
    @Test("coarse rate limits degrade to passA failure rows without failing the FM job")
    func coarseRateLimitsPersistFailureRowsWithoutFailingJob() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(coarseFailures: [.rateLimited, .rateLimited])
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty)
        #expect(result.deferredJobIds.isEmpty)
        #expect(result.scanResultIds.count == 1)
        #expect(await fmRuntime.coarseCallCount == 2, "runner should make the initial coarse request and one backoff retry")

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        #expect(scans.count == 1)
        let failure = try #require(scans.first)
        #expect(failure.scanPass == "passA")
        #expect(failure.status == .rateLimited)
        #expect(failure.disposition == .abstain)

        let jobId = try #require(result.admittedJobIds.first)
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .complete)
        #expect(row.retryCount == 0)
    }

    @available(iOS 26.0, *)
    @Test("partial coarse guardrail persists success rows and a blocking failure row")
    func partialCoarseGuardrailPersistsBlockingFailureRow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseFailures: [nil, .guardrailViolation],
            contextSize: 431,
            coarseSchemaTokenCount: 4,
            refinementSchemaTokenCount: 8,
            tokenCountRule: { prompt in
                prompt.split(separator: "\n", omittingEmptySubsequences: false).count * 8
            }
        )
        let runner = makeRunner(
            store: store,
            runtime: fmRuntime.runtime,
            classifierConfig: .init(safetyMarginTokens: 5, maximumResponseTokens: 6)
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty)
        #expect(result.scanResultIds.count == 2)
        #expect(await fmRuntime.coarseCallCount == 2)

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        #expect(scans.count == 2)
        #expect(scans.filter { $0.scanPass == "passA" && $0.status == .success }.count == 1)
        #expect(scans.filter { $0.scanPass == "passA" && $0.status == .guardrailViolation }.count == 1)

        let success = try #require(scans.first { $0.scanPass == "passA" && $0.status == .success })
        #expect(success.disposition == .noAds)

        let failure = try #require(scans.first { $0.scanPass == "passA" && $0.status == .guardrailViolation })
        #expect(failure.disposition == .abstain)
        #expect(failure.windowFirstAtomOrdinal > success.windowLastAtomOrdinal)
    }

    @available(iOS 26.0, *)
    @Test("partial refinement guardrail persists success rows and a blocking failure row")
    func partialRefinementGuardrailPersistsBlockingFailureRow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(supportLineRefs: [0], certainty: .strong)
                ),
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(supportLineRefs: [1], certainty: .strong)
                )
            ],
            refinementResponses: [
                RefinementWindowSchema(spans: [])
            ],
            refinementFailures: [
                nil,
                .guardrailViolation
            ],
            contextSize: 431,
            coarseSchemaTokenCount: 4,
            refinementSchemaTokenCount: 8,
            tokenCountRule: { prompt in
                prompt.split(separator: "\n", omittingEmptySubsequences: false).count * 8
            }
        )
        let runner = makeRunner(
            store: store,
            runtime: fmRuntime.runtime,
            classifierConfig: .init(safetyMarginTokens: 5, maximumResponseTokens: 6)
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty)
        #expect(await fmRuntime.refinementCallCount == 2)

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: "asset-runner")
        let passAScans = scans.filter { $0.scanPass == "passA" }
        let passBScans = scans.filter { $0.scanPass == "passB" }

        #expect(passAScans.count == 3)
        #expect(passAScans.allSatisfy { $0.status == .success })
        #expect(passBScans.count == 2)
        #expect(passBScans.filter { $0.status == .success }.count == 1)
        #expect(passBScans.filter { $0.status == .guardrailViolation }.count == 1)

        let failure = try #require(passBScans.first { $0.status == .guardrailViolation })
        #expect(failure.disposition == .abstain)
        #expect(result.scanResultIds.count == scans.count)
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
        #expect(Set(result.evidenceEventIds) == Set(evidence.map(\.id)))
        // Every persisted row's atomOrdinals must be a JSON array parseable as [Int].
        for event in evidence {
            let data = Data(event.atomOrdinals.utf8)
            let parsed = try JSONDecoder().decode([Int].self, from: data)
            #expect(!parsed.isEmpty)
        }
    }

    // R4-Fix7: The catch arms in the drain loop called
    // `markBackfillJobFailed` with no surrounding try/catch. If the typed
    // store guard threw `invalidStateTransition` (e.g. another runner
    // marked the row `.complete` first), the throw escaped the catch arm,
    // aborted the for-loop, and stranded the rest of the batch.
    //
    // We engineer the race deterministically: the classifier marks the
    // row `.complete` BEFORE throwing. By the time the catch arm calls
    // `markBackfillJobFailed`, the C-R3-2 guard sees the row in
    // `.complete` and throws `invalidStateTransition`. After R4-Fix7 the
    // wrap absorbs that throw, finishes the admission ticket, and the
    // runner returns normally instead of propagating the throw out of
    // `runPendingBackfill`.
    @Test("R4-Fix7: markBackfillJobFailed throw is absorbed and the runner returns normally")
    func markFailedThrowIsAbsorbedByCatchArm() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let inputs = makeInputs()
        let jobId = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: "asset-runner",
            transcriptVersion: "tx-runner-v1",
            phase: .fullEpisodeScan,
            offset: 0
        )

        // Custom Runtime: respondCoarse marks the row .complete BEFORE
        // returning a successful screening. Combined with a malformed
        // scanCohortJSON, the runner's `insertSemanticScanResult` then
        // throws `invalidScanCohortJSON`, the catch arm fires, and the
        // catch arm's `markBackfillJobFailed` hits a `.complete` row
        // and throws `invalidStateTransition`. Without R4-Fix7 the throw
        // escapes the for-loop and `runPendingBackfill` re-throws.
        let runtime = FoundationModelClassifier.Runtime(
            availabilityStatus: { _ in nil },
            contextSize: { 4_096 },
            tokenCount: { prompt in
                max(1, prompt.split(whereSeparator: \.isWhitespace).count)
            },
            coarseSchemaTokenCount: { 16 },
            refinementSchemaTokenCount: { 32 },
            makeSession: {
                FoundationModelClassifier.Runtime.Session(
                    prewarm: { _ in },
                    respondCoarse: { _ in
                        // Race: flip the row to `.complete` before
                        // returning. The runner's subsequent store write
                        // will fail (malformed cohort) and the catch
                        // arm's markBackfillJobFailed will then see a
                        // `.complete` row and throw.
                        try? await store.markBackfillJobComplete(
                            jobId: jobId,
                            progressCursor: nil
                        )
                        return CoarseScreeningSchema(disposition: .noAds, support: nil)
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
            classifier: FoundationModelClassifier(runtime: runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            // Malformed cohort: insertSemanticScanResult will throw
            // invalidScanCohortJSON, sending the runner into the catch arm.
            scanCohortJSON: "not-json"
        )

        // Must NOT throw — the wrap absorbs the invalidStateTransition
        // from the racing markBackfillJobFailed call and lets the loop
        // wind down cleanly.
        let result = try await runner.runPendingBackfill(for: inputs)

        // The classifier ran for the only planned job.
        #expect(result.admittedJobIds.contains(jobId))

        // The row reflects the racing classifier write. The fix
        // tolerates the markBackfillJobFailed throw and does not wedge
        // the loop.
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .complete,
                "row reflects the racing classifier write; runner must not have crashed before returning")
    }

    // R4-Fix4: `memoryWriteEligible` was computed on RefinedAdSpan but never
    // serialized into the persisted EvidencePayload. The H-R3-1 in-memory
    // protection had no production consumer. Persist the flag so a future
    // sponsor-memory writer reading evidence_events.evidenceJSON sees the
    // eligibility decision.
    @Test("R4-Fix4: persisted evidence JSON encodes memoryWriteEligible (true case)")
    func evidenceJSONIncludesMemoryWriteEligibleTrue() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let inputs = makeInputs()
        // The default segments include "Use code SHOW for 20 percent off at
        // example dot com." which yields catalog entries. Pick one to cite.
        let entry = try #require(inputs.evidenceCatalog.entries.first,
                                  "test fixture must yield at least one catalog entry")
        // Map EvidenceCategory -> EvidenceAnchorKind (raw values match).
        let anchorKind: EvidenceAnchorKind
        switch entry.category {
        case .url: anchorKind = .url
        case .promoCode: anchorKind = .promoCode
        case .ctaPhrase: anchorKind = .ctaPhrase
        case .disclosurePhrase: anchorKind = .disclosurePhrase
        case .brandSpan: anchorKind = .brandSpan
        }

        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [entry.atomOrdinal],
                        certainty: .strong
                    )
                )
            ],
            refinementResponses: [
                RefinementWindowSchema(spans: [
                    SpanRefinementSchema(
                        commercialIntent: .paid,
                        ownership: .thirdParty,
                        firstLineRef: entry.atomOrdinal,
                        lastLineRef: entry.atomOrdinal,
                        certainty: .strong,
                        boundaryPrecision: .precise,
                        evidenceAnchors: [
                            EvidenceAnchorSchema(
                                evidenceRef: entry.evidenceRef,
                                lineRef: entry.atomOrdinal,
                                kind: anchorKind,
                                certainty: .strong
                            )
                        ],
                        alternativeExplanation: .none,
                        reasonTags: [.promoCode]
                    )
                ])
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        _ = try await runner.runPendingBackfill(for: inputs)

        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        let refinementRows = evidence.filter { $0.eventType == "fm.spanRefinement" }
        #expect(!refinementRows.isEmpty, "expected at least one refinement evidence row")
        for row in refinementRows {
            #expect(row.evidenceJSON.contains("\"memoryWriteEligible\":true"),
                    "evidenceJSON must encode memoryWriteEligible=true; got: \(row.evidenceJSON)")
        }
    }

    @Test("R4-Fix4: persisted evidence JSON encodes memoryWriteEligible (false case)")
    func evidenceJSONIncludesMemoryWriteEligibleFalse() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Anchor with evidenceRef=nil resolves via the lineRefFallback path,
        // which (per the C8 contract) marks the span as memoryWriteEligible=false.
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

        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        let refinementRows = evidence.filter { $0.eventType == "fm.spanRefinement" }
        #expect(!refinementRows.isEmpty)
        for row in refinementRows {
            #expect(row.evidenceJSON.contains("\"memoryWriteEligible\":false"),
                    "evidenceJSON must encode memoryWriteEligible=false; got: \(row.evidenceJSON)")
        }
    }

    @available(iOS 26.0, *)
    @Test("refinement refusal persists a terminal passB row when no spans are returned")
    func refinementRefusalPersistsFailureRow() async throws {
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
            refinementFailures: [.refusal]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        let result = try await runner.runPendingBackfill(for: makeInputs())

        #expect(!result.admittedJobIds.isEmpty)
        let passB = try await store.fetchSemanticScanResults(
            analysisAssetId: "asset-runner",
            scanPass: "passB"
        )
        #expect(passB.count == 1, "runner must persist a synthetic passB failure row")
        let failure = try #require(passB.first)
        #expect(failure.status == .refusal)
        #expect(failure.disposition == .abstain)
        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        #expect(evidence.isEmpty, "no refinement evidence should be written for a refused prompt")
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
            jobId: BackfillJobRunner.makeJobIdForTesting(
                analysisAssetId: "asset-runner",
                transcriptVersion: "tx-runner-v1",
                phase: .fullEpisodeScan,
                offset: 0
            ),
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
            jobId: BackfillJobRunner.makeJobIdForTesting(
                analysisAssetId: "asset-runner",
                transcriptVersion: "tx-runner-v1",
                phase: .fullEpisodeScan,
                offset: 0
            ),
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
            stableRecall: true,
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

    // R4-Fix1: The H-1 defer-all-jobs loop unconditionally called
    // markBackfillJobDeferred for every non-admitted candidate. When the M-5
    // idempotency path re-enqueued a `.failed` row (retryCount<maxRetries),
    // the C-R3-1 status guard rejected the `.failed -> .deferred` write with
    // `invalidStateTransition`. The throw was unhandled inside the loop and
    // aborted mid-iteration, leaving subsequent jobs stranded in `.queued`.
    @Test("R4-Fix1: defer loop tolerates terminal pre-failed rows and continues marking the rest")
    func deferLoopHandlesTerminalRowsGracefully() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        // Pre-insert a `.failed` row at the deterministic jobId the runner
        // will synthesize for the first targeted phase. retryCount=0 keeps
        // it under the C-B exhaustion gate, so the M-5 idempotency path
        // re-enqueues it instead of skipping.
        // Targeted plan emits phases in order: scanHarvesterProposals(0),
        // scanLikelyAdSlots(1), scanRandomAuditWindows(2). Seed the FIRST
        // phase as `.failed` so the M-5 idempotency probe matches by jobId
        // and re-enqueues the existing terminal row.
        let failedJob = BackfillJob(
            jobId: BackfillJobRunner.makeJobIdForTesting(
                analysisAssetId: "asset-runner",
                transcriptVersion: "tx-runner-v1",
                phase: .scanHarvesterProposals,
                offset: 0
            ),
            analysisAssetId: "asset-runner",
            podcastId: "podcast-runner",
            phase: .scanHarvesterProposals,
            coveragePolicy: .targetedWithAudit,
            priority: 20,
            progressCursor: nil,
            retryCount: 0,
            deferReason: "seeded prior failure",
            status: .failed,
            scanCohortJSON: makeTestScanCohortJSON(),
            createdAt: Date().timeIntervalSince1970
        )
        try await store.insertBackfillJob(failedJob)

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
            stableRecall: true,
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

        // Must not throw: the defer-loop must absorb the C-R3-1
        // invalidStateTransition on the seeded `.failed` row and keep going.
        let result = try await runner.runPendingBackfill(for: inputs)

        // The pre-failed row stays exactly as seeded.
        let preFailed = try #require(await store.fetchBackfillJob(byId: failedJob.jobId))
        #expect(preFailed.status == .failed, "seeded .failed row must remain .failed")
        #expect(preFailed.deferReason == "seeded prior failure",
                "seeded deferReason must be preserved")
        #expect(preFailed.retryCount == 0)

        // The other 2 planned phases (auditWindows + harvesterProposals)
        // must have been marked deferred — proving the loop did not abort
        // after the invalidStateTransition on the .failed row.
        let otherDeferredIds = result.deferredJobIds.filter { $0 != failedJob.jobId }
        #expect(otherDeferredIds.count == 2,
                "expected 2 sibling phases marked deferred, got \(otherDeferredIds.count)")
        for jobId in otherDeferredIds {
            let row = try #require(await store.fetchBackfillJob(byId: jobId))
            #expect(row.status == .deferred, "sibling \(jobId) must be .deferred")
            #expect(row.deferReason == "thermalThrottled")
        }
    }

    // R4-Fix6: jobId did not include `inputs.transcriptVersion`. After a
    // transcript regeneration the M-5 idempotency check found the prior
    // `.complete` job under the same id and skipped FM entirely against
    // the new transcript. Add transcriptVersion to the jobId tuple so a
    // version bump produces a fresh row and re-invokes the classifier.
    @Test("R4-Fix6: a transcriptVersion bump produces a new jobId and reprocesses the asset")
    func transcriptVersionBumpReprocessesAsset() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(disposition: .noAds, support: nil),
                CoarseScreeningSchema(disposition: .noAds, support: nil)
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        // First run: v1 transcript. Expect FM invoked, job marked complete.
        _ = try await runner.runPendingBackfill(
            for: makeInputs(transcriptVersion: "tx-runner-v1")
        )
        let coarseAfterV1 = await fmRuntime.coarseCallCount
        #expect(coarseAfterV1 >= 1, "v1 must invoke the classifier")

        // Second run: v2 transcript. Without R4-Fix6 the M-5 idempotency
        // probe finds the prior v1 .complete row at the same jobId and
        // skips FM entirely. With the fix, the v2 jobId is distinct, so a
        // new row is inserted and FM runs again.
        _ = try await runner.runPendingBackfill(
            for: makeInputs(transcriptVersion: "tx-runner-v2")
        )
        let coarseAfterV2 = await fmRuntime.coarseCallCount
        #expect(coarseAfterV2 > coarseAfterV1,
                "v2 must invoke the classifier again; v1=\(coarseAfterV1) v2=\(coarseAfterV2)")

        // The store must contain a job whose id encodes "tx-runner-v2".
        let v2Job = try await store.fetchBackfillJob(
            byId: BackfillJobRunner.makeJobIdForTesting(
                analysisAssetId: "asset-runner",
                transcriptVersion: "tx-runner-v2",
                phase: .fullEpisodeScan,
                offset: 0
            )
        )
        #expect(v2Job != nil, "expected a v2-tagged backfill job row")
        #expect(v2Job?.status == .complete)
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
        // We need a job row that still allows re-enqueue. Use the
        // DEBUG-only force helper to drop the status back to .queued —
        // direct `markBackfillJobDeferred` can no longer demote a terminal
        // row after the C-R3-1 guard fix.
        let jobId = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: "asset-runner",
            transcriptVersion: "tx-runner-v1",
            phase: .fullEpisodeScan,
            offset: 0
        )
        try await store.forceBackfillJobStateForTesting(
            jobId: jobId,
            status: .queued,
            progressCursor: nil
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
        let jobId = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: "asset-runner",
            transcriptVersion: "tx-runner-v1",
            phase: .fullEpisodeScan,
            offset: 0
        )
        try await store.forceBackfillJobStateForTesting(
            jobId: jobId,
            status: .queued,
            progressCursor: nil
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

    // MARK: - R7-Fix11: scan id / job id hashing

    @Test("R7-Fix11: scan IDs are stable hashes immune to separator collision")
    func scanIdsHashCollisionImmune() {
        // Two distinct tuples that would collide under naive `-` joining:
        //   ("abc", "def-123") vs ("abc-def", "123")
        // both produce "scan-abc-def-123-passA-0" with the old format.
        let a = BackfillJobRunner.makeScanResultIdForTesting(
            assetId: "abc", transcriptVersion: "def-123", pass: "passA", windowIndex: 0
        )
        let b = BackfillJobRunner.makeScanResultIdForTesting(
            assetId: "abc-def", transcriptVersion: "123", pass: "passA", windowIndex: 0
        )
        #expect(a != b, "distinct tuples must produce distinct hashed ids")
    }

    @Test("R7-Fix11: scan ID is deterministic for same inputs")
    func scanIdsAreDeterministic() {
        let a = BackfillJobRunner.makeScanResultIdForTesting(
            assetId: "asset-1", transcriptVersion: "v1", pass: "passA", windowIndex: 0
        )
        let b = BackfillJobRunner.makeScanResultIdForTesting(
            assetId: "asset-1", transcriptVersion: "v1", pass: "passA", windowIndex: 0
        )
        #expect(a == b)
        #expect(a.hasPrefix("scan-"))
        #expect(a.count == "scan-".count + 16) // 16-char hex hash
    }

    @Test("R7-Fix11: job IDs are stable hashes immune to separator collision")
    func jobIdsHashCollisionImmune() {
        // Analogous to the scan-id collision test: a hyphen drifting
        // between assetId and transcriptVersion must not collapse two
        // logical tuples onto the same jobId.
        let a = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: "abc",
            transcriptVersion: "def-v1",
            phase: .fullEpisodeScan,
            offset: 0
        )
        let b = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: "abc-def",
            transcriptVersion: "v1",
            phase: .fullEpisodeScan,
            offset: 0
        )
        #expect(a != b, "distinct tuples must produce distinct hashed jobIds")
        #expect(a.hasPrefix("fm-"))
        #expect(a.count == "fm-".count + 16)
    }

    @Test("R7-Fix11: job ID is deterministic for same inputs")
    func jobIdsAreDeterministic() {
        let a = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: "asset-1",
            transcriptVersion: "v1",
            phase: .scanHarvesterProposals,
            offset: 2
        )
        let b = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: "asset-1",
            transcriptVersion: "v1",
            phase: .scanHarvesterProposals,
            offset: 2
        )
        #expect(a == b)
    }

    @Test("bd-1tl: caseName covers every AnalysisStoreError case with a stable token")
    func caseNameCoversEveryCase() {
        // bd-1tl: the on-device run reported `AnalysisStoreError error 9`
        // — Swift's NSError-bridge ordinal — which is unhelpful for triage.
        // Production telemetry now logs the case name via
        // `BackfillJobRunner.caseName(of:)`. This test pins every case to
        // its stable token and exercises every switch arm so a future
        // case addition fails compilation here (the switch is exhaustive)
        // before it can ship a "case=unknown" log line.
        let cases: [(AnalysisStoreError, String)] = [
            (.openFailed(code: 1, message: "x"), "openFailed"),
            (.migrationFailed("x"), "migrationFailed"),
            (.queryFailed("x"), "queryFailed"),
            (.insertFailed("x"), "insertFailed"),
            (.notFound, "notFound"),
            (.duplicateJobId("x"), "duplicateJobId"),
            (.invalidRow(column: 0), "invalidRow"),
            (.invalidEvidenceEvent("x"), "invalidEvidenceEvent"),
            (.invalidScanCohortJSON("x"), "invalidScanCohortJSON"),
            (.invalidStateTransition(jobId: "j", fromStatus: nil, toStatus: "running"), "invalidStateTransition"),
            (.evidenceEventBodyMismatch(id: "x"), "evidenceEventBodyMismatch"),
        ]
        for (error, expected) in cases {
            #expect(BackfillJobRunner.caseName(of: error) == expected,
                    "caseName(\(expected)) returned the wrong token")
        }
    }

    @Test("bd-1tl: backfill runner persists results when refinement evidence shares a natural key")
    func runnerPersistsAcrossEvidenceNaturalKeyCollision() async throws {
        // bd-1tl: end-to-end repro of the on-device persistence failure.
        // Two refined spans returned by the FM cover the same line range
        // (firstLineRef == lastLineRef == 1) but with different bodies
        // (`commercialIntent`, `certainty`). Both spans flow through
        // `BackfillJobRunner.makeEvidenceEvents` and produce evidence events
        // with the same atomOrdinals JSON, the same scanCohortJSON, and
        // different evidenceJSON. Pre-playhead-fn0 this aborted the entire
        // refinement-pass batch or silently collapsed the second span.
        // Post-fix the scan row commits and BOTH distinct evidence rows
        // survive.
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
                    ),
                    // Second span: same line range, different body. The
                    // FM is well within its rights to emit overlapping
                    // refined spans for the same atoms — H3-1's throw
                    // turned that into a P0 persistence failure.
                    SpanRefinementSchema(
                        commercialIntent: .affiliate,
                        ownership: .thirdParty,
                        firstLineRef: 1,
                        lastLineRef: 1,
                        certainty: .moderate,
                        boundaryPrecision: .usable,
                        evidenceAnchors: [
                            EvidenceAnchorSchema(
                                evidenceRef: nil,
                                lineRef: 1,
                                kind: .ctaPhrase,
                                certainty: .moderate
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
        let passB = try await store.fetchSemanticScanResults(
            analysisAssetId: "asset-runner",
            scanPass: "passB"
        )
        #expect(passB.count >= 1, "passB scan row must persist; pre-fix this was 0")
        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        #expect(evidence.count == 2, "distinct same-range evidence rows must both persist")
        #expect(result.evidenceEventIds.count == 2)
        #expect(Set(result.evidenceEventIds) == Set(evidence.map(\.id)))
    }

    @Test("playhead-fn0: partial refinement refusals persist failure rows alongside surviving success rows")
    func partialRefinementRefusalsPersistFailureRows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let segments = makeFMSegments(
            analysisAssetId: "asset-runner",
            transcriptVersion: "tx-runner-v1",
            lines: [
                (0, 8, "The hosts catch up before the break."),
                (8, 16, "This episode is brought to you by ExampleCo."),
                (16, 24, "Use code SAVE for twenty percent off.")
            ]
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: "asset-runner",
            podcastId: "podcast-runner",
            segments: segments,
            evidenceCatalog: EvidenceCatalogBuilder.build(
                atoms: segments.flatMap(\.atoms),
                analysisAssetId: "asset-runner",
                transcriptVersion: "tx-runner-v1"
            ),
            transcriptVersion: "tx-runner-v1",
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

        let runtime = WindowedTestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [1],
                        certainty: .strong
                    )
                ),
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [2],
                        certainty: .strong
                    )
                )
            ],
            refinementResponses: [
                RefinementWindowSchema(spans: [])
            ],
            refinementFailures: [
                .refusal,
                nil
            ]
        ).runtime

        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(
                runtime: runtime,
                config: .init(
                    safetyMarginTokens: 5,
                    coarseMaximumResponseTokens: 6,
                    refinementMaximumResponseTokens: 16
                )
            ),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )

        let result = try await runner.runPendingBackfill(for: inputs)

        let passB = try await store.fetchSemanticScanResults(
            analysisAssetId: "asset-runner",
            scanPass: "passB"
        )
        #expect(passB.count == 2, "expected one surviving success row and one persisted refusal row")
        #expect(passB.contains { $0.status == .success })
        #expect(passB.contains { $0.status == .refusal && $0.disposition == .abstain })
        #expect(result.scanResultIds.count >= 3, "passA rows plus both passB outcomes should be reported")
    }

    @Test("partial coarse refusals persist failure rows alongside surviving success rows")
    func partialCoarseRefusalsPersistFailureRows() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let runtime = WindowedTestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .noAds,
                    support: nil
                )
            ],
            coarseFailures: [
                .refusal,
                nil
            ]
        ).runtime

        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(
                runtime: runtime,
                config: .init(
                    safetyMarginTokens: 5,
                    coarseMaximumResponseTokens: 6,
                    refinementMaximumResponseTokens: 16
                )
            ),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )

        let result = try await runner.runPendingBackfill(for: makeInputs())

        let passA = try await store.fetchSemanticScanResults(
            analysisAssetId: "asset-runner",
            scanPass: "passA"
        )
        #expect(passA.count == 2, "expected one surviving success row and one persisted refusal row")
        #expect(passA.contains { $0.status == .success })
        #expect(passA.contains { $0.status == .refusal && $0.disposition == .abstain })
        #expect(result.scanResultIds.count >= 2)
    }

    // MARK: - bd-3vm: anchor encoding in spansJSON / EvidencePayload

    /// bd-3vm: round-trip an encoded refined span with anchors. The encoder
    /// must persist per-anchor identity tuples matching
    /// `BackfillJobRunner.anchorIdentityKey` (evidenceRef, lineRef, kind,
    /// resolutionSource) plus certainty, so downstream analytics and
    /// debugging can observe exactly which anchors justified a span.
    @Test("bd-3vm: encodeRefinedSpans round-trips anchor identity tuples")
    func encodeRefinedSpansRoundTripsAnchorIdentity() throws {
        let entry = EvidenceEntry(
            evidenceRef: 7,
            category: .url,
            matchedText: "example.com",
            normalizedText: "example.com",
            atomOrdinal: 3,
            startTime: 12,
            endTime: 15
        )
        let anchor1 = ResolvedEvidenceAnchor(
            entry: entry,
            lineRef: 3,
            kind: .url,
            certainty: .strong,
            resolutionSource: .evidenceRef,
            memoryWriteEligible: true
        )
        let anchor2 = ResolvedEvidenceAnchor(
            entry: nil,
            lineRef: 4,
            kind: .ctaPhrase,
            certainty: .moderate,
            resolutionSource: .lineRefFallback,
            memoryWriteEligible: false
        )
        let span = RefinedAdSpan(
            commercialIntent: .paid,
            ownership: .thirdParty,
            firstLineRef: 3,
            lastLineRef: 4,
            firstAtomOrdinal: 3,
            lastAtomOrdinal: 4,
            certainty: .strong,
            boundaryPrecision: .precise,
            resolvedEvidenceAnchors: [anchor1, anchor2],
            memoryWriteEligible: false,
            alternativeExplanation: .none,
            reasonTags: [.promoCode]
        )

        let json = BackfillJobRunner.encodeRefinedSpansForTesting([span])
        let decoded = try BackfillJobRunner.decodeRefinedSpansForTesting(json)

        #expect(decoded.count == 1)
        let encodedSpan = try #require(decoded.first)
        #expect(encodedSpan.firstLineRef == 3)
        #expect(encodedSpan.lastLineRef == 4)
        #expect(encodedSpan.commercialIntent == "paid")
        #expect(encodedSpan.ownership == "thirdParty")
        #expect(encodedSpan.certainty == "strong")

        let anchors = try #require(encodedSpan.anchors)
        #expect(anchors.count == 2)

        let first = anchors[0]
        #expect(first.evidenceRef == 7)
        #expect(first.lineRef == 3)
        #expect(first.kind == "url")
        #expect(first.resolutionSource == "evidenceRef")
        #expect(first.certainty == "strong")

        let second = anchors[1]
        #expect(second.evidenceRef == nil)
        #expect(second.lineRef == 4)
        #expect(second.kind == "ctaPhrase")
        #expect(second.resolutionSource == "lineRefFallback")
        #expect(second.certainty == "moderate")
    }

    /// bd-3vm: rows persisted before this change lack the `anchors` field.
    /// The decoder must parse them without throwing, with anchors == nil.
    @Test("bd-3vm: decodeRefinedSpans accepts legacy JSON without anchors")
    func decodeRefinedSpansAcceptsLegacyJSONWithoutAnchors() throws {
        let legacy = #"""
        [{"firstLineRef":10,"lastLineRef":12,"commercialIntent":"paid","ownership":"thirdParty","certainty":"moderate"}]
        """#
        let decoded = try BackfillJobRunner.decodeRefinedSpansForTesting(legacy)
        #expect(decoded.count == 1)
        let span = try #require(decoded.first)
        #expect(span.firstLineRef == 10)
        #expect(span.lastLineRef == 12)
        #expect(span.commercialIntent == "paid")
        #expect(span.ownership == "thirdParty")
        #expect(span.certainty == "moderate")
        #expect(span.anchors == nil,
                "legacy rows have no anchors field; decoder must produce nil")
    }

    /// bd-3vm: same back-compat, but for EvidencePayload. Pre-change
    /// evidence_events rows never encoded an `anchors` field.
    @Test("bd-3vm: EvidencePayload decodes legacy JSON without anchors")
    func evidencePayloadDecodesLegacyJSONWithoutAnchors() throws {
        let legacy = #"""
        {"commercialIntent":"paid","ownership":"thirdParty","certainty":"strong","boundaryPrecision":"precise","firstLineRef":1,"lastLineRef":2,"jobId":"job-42","memoryWriteEligible":true}
        """#
        let payload = try BackfillJobRunner.decodeEvidencePayloadForTesting(legacy)
        #expect(payload.commercialIntent == "paid")
        #expect(payload.jobId == "job-42")
        #expect(payload.memoryWriteEligible == true)
        #expect(payload.anchors == nil)
    }

    /// bd-3vm: end-to-end. A refinement pass with a catalog-backed anchor
    /// must produce a persisted passB row whose spansJSON carries the
    /// anchor identity tuple. This is the observability path bd-1my's
    /// anchor-upgrade merge fix unblocked.
    @Test("bd-3vm: persisted spansJSON encodes anchor identity tuple")
    func persistedSpansJSONEncodesAnchorIdentityTuple() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())

        let inputs = makeInputs()
        let entry = try #require(inputs.evidenceCatalog.entries.first,
                                 "test fixture must yield at least one catalog entry")
        let anchorKind: EvidenceAnchorKind
        switch entry.category {
        case .url: anchorKind = .url
        case .promoCode: anchorKind = .promoCode
        case .ctaPhrase: anchorKind = .ctaPhrase
        case .disclosurePhrase: anchorKind = .disclosurePhrase
        case .brandSpan: anchorKind = .brandSpan
        }

        let fmRuntime = TestFMRuntime(
            coarseResponses: [
                CoarseScreeningSchema(
                    disposition: .containsAd,
                    support: CoarseSupportSchema(
                        supportLineRefs: [entry.atomOrdinal],
                        certainty: .strong
                    )
                )
            ],
            refinementResponses: [
                RefinementWindowSchema(spans: [
                    SpanRefinementSchema(
                        commercialIntent: .paid,
                        ownership: .thirdParty,
                        firstLineRef: entry.atomOrdinal,
                        lastLineRef: entry.atomOrdinal,
                        certainty: .strong,
                        boundaryPrecision: .precise,
                        evidenceAnchors: [
                            EvidenceAnchorSchema(
                                evidenceRef: entry.evidenceRef,
                                lineRef: entry.atomOrdinal,
                                kind: anchorKind,
                                certainty: .strong
                            )
                        ],
                        alternativeExplanation: .none,
                        reasonTags: [.promoCode]
                    )
                ])
            ]
        )
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime)

        _ = try await runner.runPendingBackfill(for: inputs)

        let passB = try await store.fetchSemanticScanResults(
            analysisAssetId: "asset-runner",
            scanPass: "passB"
        )
        let successRows = passB.filter { $0.status == .success && $0.disposition == .containsAd }
        #expect(!successRows.isEmpty, "expected at least one containsAd passB row")
        let row = try #require(successRows.first)
        let decoded = try BackfillJobRunner.decodeRefinedSpansForTesting(row.spansJSON)
        #expect(!decoded.isEmpty, "spansJSON must contain at least one encoded span")
        let encodedSpan = try #require(decoded.first)
        let anchors = try #require(encodedSpan.anchors,
                                    "spansJSON must encode anchors field after bd-3vm")
        #expect(!anchors.isEmpty, "encoded span must carry at least one anchor")
        let firstAnchor = try #require(anchors.first)
        #expect(firstAnchor.evidenceRef == entry.evidenceRef)
        #expect(firstAnchor.lineRef == entry.atomOrdinal)
        #expect(firstAnchor.kind == anchorKind.rawValue)
        #expect(firstAnchor.resolutionSource == "evidenceRef")
        #expect(firstAnchor.certainty == "strong")

        // And the same anchor identity must reach evidence_events.evidenceJSON
        // (the EvidencePayload path).
        let evidence = try await store.fetchEvidenceEvents(analysisAssetId: "asset-runner")
        let refinementRows = evidence.filter { $0.eventType == "fm.spanRefinement" }
        #expect(!refinementRows.isEmpty)
        let evRow = try #require(refinementRows.first)
        let payload = try BackfillJobRunner.decodeEvidencePayloadForTesting(evRow.evidenceJSON)
        let payloadAnchors = try #require(payload.anchors,
                                           "EvidencePayload must encode anchors field after bd-3vm")
        #expect(!payloadAnchors.isEmpty)
        let payloadAnchor = try #require(payloadAnchors.first)
        #expect(payloadAnchor.evidenceRef == entry.evidenceRef)
        #expect(payloadAnchor.lineRef == entry.atomOrdinal)
        #expect(payloadAnchor.kind == anchorKind.rawValue)
        #expect(payloadAnchor.resolutionSource == "evidenceRef")
        #expect(payloadAnchor.certainty == "strong")
    }
}

private actor WindowedTestFMRuntime {
    private var coarseQueue: [CoarseScreeningSchema]
    private var refinementQueue: [RefinementWindowSchema]
    private var coarseFailureQueue: [TestFMRuntimeFailure?]
    private var refinementFailureQueue: [TestFMRuntimeFailure?]

    init(
        coarseResponses: [CoarseScreeningSchema] = [],
        refinementResponses: [RefinementWindowSchema] = [],
        coarseFailures: [TestFMRuntimeFailure?] = [],
        refinementFailures: [TestFMRuntimeFailure?] = []
    ) {
        self.coarseQueue = coarseResponses
        self.refinementQueue = refinementResponses
        self.coarseFailureQueue = coarseFailures
        self.refinementFailureQueue = refinementFailures
    }

    nonisolated var runtime: FoundationModelClassifier.Runtime {
        FoundationModelClassifier.Runtime(
            availabilityStatus: { _ in nil },
            contextSize: { 295 },
            tokenCount: { prompt in
                prompt.split(separator: "\n", omittingEmptySubsequences: false).count * 5
            },
            coarseSchemaTokenCount: { 4 },
            refinementSchemaTokenCount: { 8 },
            makeSession: {
                FoundationModelClassifier.Runtime.Session(
                    prewarm: { _ in },
                    respondCoarse: { _ in try await self.nextCoarse() },
                    respondRefinement: { _ in try await self.nextRefinement() }
                )
            }
        )
    }

    private func nextCoarse() throws -> CoarseScreeningSchema {
        if !coarseFailureQueue.isEmpty {
            let failure = coarseFailureQueue.removeFirst()
            if let failure {
                throw failure.error
            }
        }
        if coarseQueue.isEmpty {
            return CoarseScreeningSchema(disposition: .noAds, support: nil)
        }
        return coarseQueue.removeFirst()
    }

    private func nextRefinement() throws -> RefinementWindowSchema {
        if !refinementFailureQueue.isEmpty {
            let failure = refinementFailureQueue.removeFirst()
            if let failure {
                throw failure.error
            }
        }
        if refinementQueue.isEmpty {
            return RefinementWindowSchema(spans: [])
        }
        return refinementQueue.removeFirst()
    }
}
