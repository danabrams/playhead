// OperationalMetricsTests.swift
//
// playhead-kvi1: `cacheReuseRate` was 1.0 by construction, and the rails at the
// bottom of this file exist to keep it deleted.
//
// THE DEFECT. `Counters.recordFMOutput` incremented `cacheLookupCount`
// unconditionally and `cacheReuseCount` under the PASS-level `prewarmHit` flag,
// which `FoundationModelClassifier` writes from a compile-time constant. So the
// two counters were equal for every pass that reached the model and the derived
// rate was 1.0; on the arms where the flag is `false` the pass ran no FM work at
// all, which `fmWindowCount` already reports. Measured over all 26 events on the
// 2026-08-11 virgin-DB overnight pull: `cacheReuseCount == cacheLookupCount` in
// every single row — 1.0 on the six with FM work, and 0.0 (the zero-denominator
// reading) on the other twenty. Both numbers were about to be read off a device
// pull as cache behaviour; one says "perfect", the other says "broken", and
// neither was a measurement.
//
// WHY DELETED AND NOT REPAIRED. There is no warmth signal to count:
// `LanguageModelSession.prewarm` is fire-and-forget and never awaited, and
// nothing in the tree times it, so a prewarm that hit and one that missed are
// indistinguishable to every column this code writes. Making the rate `Double?`
// would have fixed the twenty zeroes and left the six 1.0s standing, which is
// the worse half. The app's real cache — `RepeatedAdCacheService` — already
// counts genuine hits and misses, already publishes `hitRate` as a `Double?`
// with nil for "no samples", and already has a consumer.
//
// THE TWO DIRECTIONS THE RAILS COVER, because closing one leaves the other open:
//
//   1. THE WIRE. `payloadKeySetIsPinned` fixes the exact key set of the encoded
//      payload, so a re-added counter fails whatever it is named, and a silently
//      dropped field fails too. `oldV1PayloadStillDecodes` proves the events
//      already on Dan's phone survive the removal.
//   2. THE SOURCE. `operationalMetricsDeclaresNoCacheQuantity` is a source
//      canary, because the direction a behavioural test cannot reach is a
//      counter that is declared and incremented but not yet wired to anything a
//      test decodes — exactly the shape that let this one sit unread for months.

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

        // playhead-kvi1, the wire rail on the PRODUCTION path: whatever the type
        // declares, what actually lands in `evidence_events` — the only thing a
        // device pull can read — must carry no cache quantity. Lower-cased so a
        // re-added `CacheHitRate` cannot pass on capitalisation.
        #expect(
            !event.evidenceJSON.lowercased().contains("cache"),
            "The persisted operational-metrics payload names a cache again: \(event.evidenceJSON)"
        )

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

        let jobId = BackfillJobRunner.makeJobId(
            analysisAssetId: assetId,
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

    // MARK: - playhead-kvi1 rails: no fabricated cache quantity, in either direction

    /// The payload's key set, pinned exactly. Closed in BOTH directions on
    /// purpose: an added key fails (whatever it is spelled, so this cannot be
    /// out-spelled the way a `contains("cache")` filter can), and a removed key
    /// fails too (this blob is the only thing a device pull has to read, and a
    /// field vanishing between pulls is not something an analyst can detect).
    ///
    /// If you are here because you legitimately added a field: add it to the
    /// list, and say in the commit message what would be read if the thing it
    /// names had never happened.
    @Test("the persisted payload's key set is pinned, so a re-added cache counter cannot slip back in")
    func payloadKeySetIsPinned() throws {
        let metrics = OperationalMetrics(
            jobId: "job-keys",
            analysisAssetId: "asset-keys",
            jobPhase: "fullEpisodeScan",
            scanCohortJSON: makeTestScanCohortJSON(),
            wallTimeSeconds: 1,
            audioDurationSeconds: 1,
            counters: OperationalMetrics.Counters()
        )

        let object = try #require(
            try JSONSerialization.jsonObject(with: JSONEncoder().encode(metrics)) as? [String: Any]
        )
        #expect(
            Set(object.keys) == [
                "schemaVersion",
                "jobId",
                "analysisAssetId",
                "jobPhase",
                "scanCohortIdentity",
                "scanCohortJSON",
                "wallTimeSeconds",
                "audioDurationSeconds",
                "wallTimePerAudioHour",
                "energyPerEpisode",
                "resumeSuccessRate",
                "perCohortDrift",
                "thermalDeferralRate",
                "counters",
            ],
            "Top-level key set changed: \(Set(object.keys).sorted())"
        )

        let counters = try #require(object["counters"] as? [String: Any])
        #expect(
            Set(counters.keys) == [
                "episodeCount",
                "fmPassCount",
                "fmWindowCount",
                "persistedScanResultCount",
                "persistedEvidenceEventCount",
                "estimatedEnergyUnits",
                "resumeAttemptCount",
                "resumeSuccessCount",
                "cohortDriftEvaluationCount",
                "cohortDriftSignalCount",
                "admissionDecisionCount",
                "thermalDeferralCount",
                "randomAuditCandidateCount",
                "randomAuditSelectedCount",
            ],
            "Counters key set changed: \(Set(counters.keys).sorted())"
        )

        // The version is what tells a reader which of the two shapes they hold.
        #expect(object["schemaVersion"] as? Int == 2)
        #expect(OperationalMetrics.schemaVersion == 2)
    }

    /// The twenty-six events already sitting in `evidence_events` on Dan's phone
    /// were written by schema v1 and carry the three deleted keys. Removing a
    /// field from a `Codable` struct is only safe because Swift's synthesized
    /// `Decodable` ignores unknown keys — which is a property of the compiler,
    /// not of this code, so it is pinned rather than assumed. No SQL migration
    /// was needed for the same reason: `evidenceJSON` is an opaque TEXT column.
    @Test("a schema-v1 payload still decodes, and its cache keys are ignored")
    func oldV1PayloadStillDecodes() throws {
        // Byte-for-byte shape of a real v1 event, trimmed to the fields that
        // matter. `cacheReuseRate: 1` is the reading this bead exists to remove.
        let v1 = """
            {"analysisAssetId":"590D6656","audioDurationSeconds":457.86,\
            "cacheReuseRate":1,\
            "counters":{"admissionDecisionCount":1,"cacheLookupCount":2,"cacheReuseCount":2,\
            "cohortDriftEvaluationCount":1,"cohortDriftSignalCount":0,"episodeCount":1,\
            "estimatedEnergyUnits":272.77,"fmPassCount":2,"fmWindowCount":7,\
            "persistedEvidenceEventCount":0,"persistedScanResultCount":8,\
            "randomAuditCandidateCount":0,"randomAuditSelectedCount":0,\
            "resumeAttemptCount":1,"resumeSuccessCount":0,"thermalDeferralCount":0},\
            "energyPerEpisode":272.77,"jobId":"fm-df75eb5558560ce2",\
            "jobPhase":"fullEpisodeScan","perCohortDrift":0,"resumeSuccessRate":0,\
            "scanCohortIdentity":"{}","scanCohortJSON":"{}","schemaVersion":1,\
            "thermalDeferralRate":0,"wallTimePerAudioHour":2309.83,\
            "wallTimeSeconds":293.77}
            """

        let decoded = try JSONDecoder().decode(OperationalMetrics.self, from: Data(v1.utf8))

        #expect(decoded.schemaVersion == 1, "the stored version must survive, or a pull cannot be dated")
        #expect(decoded.jobId == "fm-df75eb5558560ce2")
        #expect(decoded.counters.fmPassCount == 2)
        #expect(decoded.counters.fmWindowCount == 7)
        #expect(decoded.resumeSuccessRate == 0)

        // And re-encoding it drops the fabrication rather than carrying it forward.
        let reencoded = String(data: try JSONEncoder().encode(decoded), encoding: .utf8) ?? ""
        #expect(!reencoded.lowercased().contains("cache"))
    }

    /// The direction no behavioural test reaches: a counter that is declared and
    /// incremented but not yet read by anything a test decodes. That is how the
    /// deleted one survived — it was on the wire for months and nothing in the
    /// app ever read it, so no assertion anywhere could have failed.
    @Test("OperationalMetrics.swift declares no cache quantity at all")
    func operationalMetricsDeclaresNoCacheQuantity() throws {
        let source = try Self.appSourceRoot()
            .appendingPathComponent("Services/AdDetection/OperationalMetrics.swift")
        let text = try String(contentsOf: source, encoding: .utf8)

        // The file's own header explains the deletion and necessarily says
        // "cache" several times, so comments are dropped before matching. A
        // comment is not a declaration.
        let offenders = text
            .split(separator: "\n", omittingEmptySubsequences: false)
            .enumerated()
            .filter { !$0.element.trimmingCharacters(in: .whitespaces).hasPrefix("//") }
            .filter { $0.element.lowercased().contains("cache") }
            .map { "OperationalMetrics.swift:\($0.offset + 1): \($0.element.trimmingCharacters(in: .whitespaces))" }

        #expect(
            offenders.isEmpty,
            """
            A cache quantity is back in OperationalMetrics. There is no cache on \
            this path to measure — `prewarm` is fire-and-forget and FoundationModels \
            exposes no hit/miss — so whatever is counted here is a proxy for \
            something else wearing the word "cache", and a device pull will read it \
            as cache behaviour. The real one is `RepeatedAdCacheService`, which \
            counts actual hits and misses and reports `hitRate` as `Double?`. \
            Offenders:
            \(offenders.joined(separator: "\n"))
            """
        )
    }

    /// Resolves the `Playhead/` source root by walking up from this test file
    /// (`#filePath` is stamped into the binary at compile time).
    private static func appSourceRoot(file: StaticString = #filePath) throws -> URL {
        let thisFile = URL(fileURLWithPath: String(describing: file))
        let repoRoot = thisFile
            .deletingLastPathComponent() // AdDetection/
            .deletingLastPathComponent() // Services/
            .deletingLastPathComponent() // PlayheadTests/
            .deletingLastPathComponent() // repo root
        let app = repoRoot.appendingPathComponent("Playhead", isDirectory: true)
        var isDir: ObjCBool = false
        guard FileManager.default.fileExists(atPath: app.path, isDirectory: &isDir), isDir.boolValue else {
            throw NSError(
                domain: "OperationalMetricsTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "App source root not found at \(app.path)"]
            )
        }
        return app
    }
}
