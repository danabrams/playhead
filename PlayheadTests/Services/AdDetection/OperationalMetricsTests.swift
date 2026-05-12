import Foundation
import Testing

@testable import Playhead

@Suite("OperationalMetrics")
struct OperationalMetricsTests {
    @Test("model computes required rates and round-trips through Codable")
    func computesRatesAndRoundTrips() throws {
        let counters = OperationalMetrics.Counters(
            episodeCount: 2,
            fmPassCount: 3,
            fmWindowCount: 7,
            persistedScanResultCount: 5,
            persistedEvidenceEventCount: 4,
            estimatedEnergyUnits: 42,
            cacheLookupCount: 4,
            cacheReuseCount: 3,
            resumeAttemptCount: 2,
            resumeSuccessCount: 1,
            cohortDriftEvaluationCount: 4,
            cohortDriftSignalCount: 1,
            admissionDecisionCount: 5,
            thermalDeferralCount: 2,
            randomAuditCandidateCount: 100,
            randomAuditSelectedCount: 12
        )

        let metrics = OperationalMetrics(
            jobId: "job-1",
            analysisAssetId: "asset-1",
            jobPhase: "fullEpisodeScan",
            scanCohortJSON: makeTestScanCohortJSON(),
            wallTimeSeconds: 180,
            audioDurationSeconds: 3_600,
            counters: counters
        )

        #expect(metrics.wallTimePerAudioHour == 180)
        #expect(metrics.energyPerEpisode == 21)
        #expect(metrics.cacheReuseRate == 0.75)
        #expect(metrics.resumeSuccessRate == 0.5)
        #expect(metrics.perCohortDrift == 0.25)
        #expect(metrics.thermalDeferralRate == 0.4)
        #expect(!metrics.scanCohortIdentity.isEmpty)

        let encoded = try JSONEncoder().encode(metrics)
        let decoded = try JSONDecoder().decode(OperationalMetrics.self, from: encoded)
        #expect(decoded == metrics)
    }

    @Test("scan cohort identity ignores runtime OS build")
    func scanCohortIdentityIgnoresRuntimeOSBuild() throws {
        let cohortA = ScanCohort(
            promptLabel: "phase3-shadow-v1",
            promptHash: "prompt",
            schemaHash: "schema",
            scanPlanHash: "plan",
            normalizationHash: "norm",
            osBuild: "26.4",
            locale: "en_US",
            appBuild: "100"
        )
        let cohortB = ScanCohort(
            promptLabel: cohortA.promptLabel,
            promptHash: cohortA.promptHash,
            schemaHash: cohortA.schemaHash,
            scanPlanHash: cohortA.scanPlanHash,
            normalizationHash: cohortA.normalizationHash,
            osBuild: "26.5",
            locale: cohortA.locale,
            appBuild: cohortA.appBuild
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]

        let metricsA = OperationalMetrics(
            jobId: "job-a",
            analysisAssetId: "asset-a",
            jobPhase: "fullEpisodeScan",
            scanCohortJSON: try #require(String(data: encoder.encode(cohortA), encoding: .utf8)),
            wallTimeSeconds: 1,
            audioDurationSeconds: 1,
            counters: OperationalMetrics.Counters()
        )
        let metricsB = OperationalMetrics(
            jobId: "job-b",
            analysisAssetId: "asset-b",
            jobPhase: "fullEpisodeScan",
            scanCohortJSON: try #require(String(data: encoder.encode(cohortB), encoding: .utf8)),
            wallTimeSeconds: 1,
            audioDurationSeconds: 1,
            counters: OperationalMetrics.Counters()
        )

        #expect(metricsA.scanCohortIdentity == metricsB.scanCohortIdentity)
        #expect(metricsA.scanCohortIdentity == ApprovedCohortRegistry.CohortKey.canonicalIdentity(for: cohortA))
    }

    @Test("runner records operational metrics as an EvidenceEvent per admitted job")
    func runnerRecordsOperationalMetricsEvent() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-operational-metrics"
        let transcriptVersion = "tx-operational-metrics"
        try await store.insertAsset(makeTestAsset(id: assetId))

        let runtime = TestFMRuntime()
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            clock: { Date(timeIntervalSince1970: 1_000) }
        )
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: [
                (0, 30, "Opening editorial discussion."),
                (30, 60, "A normal non-ad segment."),
                (60, 90, "Closing editorial discussion."),
            ]
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-operational-metrics",
            segments: segments,
            evidenceCatalog: EvidenceCatalog(
                analysisAssetId: assetId,
                transcriptVersion: transcriptVersion,
                entries: []
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

        let result = try await runner.runPendingBackfill(for: inputs)

        let events = try await store.fetchEvidenceEvents(analysisAssetId: assetId)
        let metricEvents = events.filter { $0.eventType == OperationalMetrics.eventType }
        #expect(metricEvents.count == result.admittedJobIds.count)

        let event = try #require(metricEvents.first)
        #expect(event.sourceType == .operational)
        #expect(event.atomOrdinals == "[]")

        let metrics = try JSONDecoder().decode(
            OperationalMetrics.self,
            from: Data(event.evidenceJSON.utf8)
        )
        #expect(metrics.jobId == result.admittedJobIds.first)
        #expect(metrics.analysisAssetId == assetId)
        #expect(metrics.jobPhase == BackfillJobPhase.fullEpisodeScan.rawValue)
        #expect(metrics.audioDurationSeconds == 90)
        #expect(metrics.wallTimeSeconds >= 0)
        #expect(metrics.counters.fmPassCount == 1)
        #expect(metrics.counters.persistedScanResultCount >= 1)
        #expect(metrics.counters.cacheLookupCount == 1)
        #expect(metrics.counters.admissionDecisionCount == 1)
        #expect(metrics.counters.cohortDriftEvaluationCount == 1)
        #expect(metrics.counters.thermalDeferralCount == 0)
    }

    @Test("runner records operational metrics for thermal admission deferrals")
    func runnerRecordsOperationalMetricsForThermalDeferrals() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-operational-thermal"
        let transcriptVersion = "tx-operational-thermal"
        try await store.insertAsset(makeTestAsset(id: assetId))

        let runtime = TestFMRuntime()
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makeThermalThrottledSnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            clock: { Date(timeIntervalSince1970: 3_000) }
        )
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: (0..<30).map { index in
                let start = Double(index) * 10
                return (start, start + 10, "Editorial segment \(index).")
            }
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-operational-thermal",
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

        let result = try await runner.runPendingBackfill(for: inputs)

        #expect(result.admittedJobIds.isEmpty)
        #expect(result.deferredJobIds.count == 3)
        #expect(await runtime.coarseCallCount == 0)

        let events = try await store.fetchEvidenceEvents(analysisAssetId: assetId)
        let metricEvents = events.filter { $0.eventType == OperationalMetrics.eventType }
        #expect(metricEvents.count == result.deferredJobIds.count)

        let decoded = try metricEvents.map {
            try JSONDecoder().decode(
                OperationalMetrics.self,
                from: Data($0.evidenceJSON.utf8)
            )
        }
        #expect(Set(decoded.map(\.jobId)) == Set(result.deferredJobIds))
        #expect(decoded.allSatisfy { $0.thermalDeferralRate == 1 })
        #expect(decoded.allSatisfy { $0.counters.admissionDecisionCount == 1 })
        #expect(decoded.allSatisfy { $0.counters.thermalDeferralCount == 1 })
        #expect(decoded.allSatisfy { $0.counters.fmPassCount == 0 })
        #expect(decoded.allSatisfy { $0.counters.persistedScanResultCount == 0 })
    }

    @Test("runner reports scanned audio duration as segment sum for disjoint narrowed phases")
    func runnerReportsDisjointNarrowedAudioDurationAsSegmentSum() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-operational-disjoint"
        let transcriptVersion = "tx-operational-disjoint"
        try await store.insertAsset(makeTestAsset(id: assetId))

        let runtime = TestFMRuntime()
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            clock: { Date(timeIntervalSince1970: 4_000) }
        )
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: (0..<100).map { index in
                let start = Double(index) * 10
                return (start, start + 10, "Editorial segment \(index).")
            }
        )
        let evidenceCatalog = EvidenceCatalog(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            entries: [
                EvidenceEntry(
                    evidenceRef: 0,
                    category: .url,
                    matchedText: "first.example",
                    normalizedText: "first.example",
                    atomOrdinal: 10,
                    startTime: 100,
                    endTime: 110
                ),
                EvidenceEntry(
                    evidenceRef: 1,
                    category: .url,
                    matchedText: "second.example",
                    normalizedText: "second.example",
                    atomOrdinal: 80,
                    startTime: 800,
                    endTime: 810
                ),
            ]
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-operational-disjoint",
            segments: segments,
            evidenceCatalog: evidenceCatalog,
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

        _ = try await runner.runPendingBackfill(for: inputs)

        let events = try await store.fetchEvidenceEvents(analysisAssetId: assetId)
        let harvesterEvent = try #require(events.first { event in
            event.eventType == OperationalMetrics.eventType &&
            event.jobPhase == BackfillJobPhase.scanHarvesterProposals.rawValue
        })
        let metrics = try JSONDecoder().decode(
            OperationalMetrics.self,
            from: Data(harvesterEvent.evidenceJSON.utf8)
        )

        #expect(metrics.audioDurationSeconds == 470)
        #expect(metrics.audioDurationSeconds < 860)
    }

    @Test("completion-transition failures record one operational event with completed work counters")
    func completionTransitionFailureRecordsAttemptedWorkOnce() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-operational-complete-race"
        let transcriptVersion = "tx-operational-complete-race"
        try await store.insertAsset(makeTestAsset(id: assetId))

        let jobId = BackfillJobRunner.makeJobIdForTesting(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            phase: .fullEpisodeScan,
            offset: 0
        )
        let runtime = FoundationModelClassifier.Runtime(
            availabilityStatus: { _ in nil },
            contextSize: { 4_096 },
            tokenCount: { prompt in
                max(1, prompt.split(whereSeparator: \.isWhitespace).count)
            },
            coarseSchemaTokenCount: { 16 },
            refinementSchemaTokenCount: { 32 },
            boundarySchemaTokenCount: { 32 },
            makeSession: {
                FoundationModelClassifier.Runtime.Session(
                    prewarm: { _ in },
                    respondCoarse: { _ in
                        try await store.markBackfillJobFailed(
                            jobId: jobId,
                            reason: "simulatedCompleteTransitionFailure",
                            retryCount: 1
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
            scanCohortJSON: makeTestScanCohortJSON(),
            clock: { Date(timeIntervalSince1970: 5_000) }
        )
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: [
                (0, 30, "Opening editorial discussion."),
                (30, 60, "A normal non-ad segment."),
                (60, 90, "Closing editorial discussion."),
            ]
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-operational-complete-race",
            segments: segments,
            evidenceCatalog: EvidenceCatalog(
                analysisAssetId: assetId,
                transcriptVersion: transcriptVersion,
                entries: []
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

        let result = try await runner.runPendingBackfill(for: inputs)

        #expect(result.admittedJobIds == [jobId])
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .failed)

        let metricEvents = try await store.fetchEvidenceEvents(analysisAssetId: assetId)
            .filter { $0.eventType == OperationalMetrics.eventType }
        #expect(metricEvents.count == 1)

        let event = try #require(metricEvents.first)
        let metrics = try JSONDecoder().decode(
            OperationalMetrics.self,
            from: Data(event.evidenceJSON.utf8)
        )
        #expect(metrics.jobId == jobId)
        #expect(metrics.counters.fmPassCount == 1)
        #expect(metrics.counters.fmWindowCount >= 1)
        #expect(metrics.counters.persistedScanResultCount >= 1)
    }
}
