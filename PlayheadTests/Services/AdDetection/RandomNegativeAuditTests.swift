import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("RandomNegativeAuditSampler")
struct RandomNegativeAuditTests {
    @Test("sampler deterministically selects 10-15 percent of eligible unflagged windows")
    func samplerSelectsExpectedCoverage() {
        let candidates = (0..<100).map { index in
            RandomNegativeAuditSampler.Candidate(
                stableId: "window-\(index)",
                firstAtomOrdinal: index * 10,
                lastAtomOrdinal: index * 10 + 9,
                fmDisposition: "noAds",
                wasFlagged: false
            )
        }

        let first = RandomNegativeAuditSampler.select(
            candidates: candidates,
            sampleRate: 0.12,
            seedMaterial: "asset|transcript|run"
        )
        let second = RandomNegativeAuditSampler.select(
            candidates: candidates,
            sampleRate: 0.12,
            seedMaterial: "asset|transcript|run"
        )

        #expect(first.selected == second.selected)
        #expect(first.selected.count == 12)
        #expect(first.eligibleCount == 100)
        #expect(first.sampleRate == 0.12)
        #expect(first.selected.allSatisfy { !$0.wasFlagged })
    }

    @Test("sampler admits only unflagged noAds windows and payload keeps nullable review fields")
    func samplerFiltersToUnflaggedNoAdsAndEncodesNullablePayloadFields() throws {
        let candidates = [
            RandomNegativeAuditSampler.Candidate(
                stableId: "clean-no-ads",
                firstAtomOrdinal: 10,
                lastAtomOrdinal: 20,
                fmDisposition: "noAds",
                wasFlagged: false
            ),
            RandomNegativeAuditSampler.Candidate(
                stableId: "flagged-no-ads",
                firstAtomOrdinal: 21,
                lastAtomOrdinal: 29,
                fmDisposition: "noAds",
                wasFlagged: true
            ),
            RandomNegativeAuditSampler.Candidate(
                stableId: "uncertain-unflagged",
                firstAtomOrdinal: 30,
                lastAtomOrdinal: 40,
                fmDisposition: "uncertain",
                wasFlagged: false
            ),
            RandomNegativeAuditSampler.Candidate(
                stableId: "ad",
                firstAtomOrdinal: 41,
                lastAtomOrdinal: 50,
                fmDisposition: "containsAd",
                wasFlagged: true
            ),
            RandomNegativeAuditSampler.Candidate(
                stableId: "no-fm-disposition",
                firstAtomOrdinal: 51,
                lastAtomOrdinal: 60,
                fmDisposition: nil,
                wasFlagged: false
            ),
        ]

        let selection = RandomNegativeAuditSampler.select(
            candidates: candidates,
            sampleRate: 0.15,
            seedMaterial: "seed"
        )

        #expect(selection.eligibleCount == 2)
        let selected = try #require(selection.selected.first)
        #expect(["clean-no-ads", "no-fm-disposition"].contains(selected.stableId))
        #expect(!selected.wasFlagged)
        #expect(selected.fmDisposition == "noAds" || selected.fmDisposition == nil)

        let nullablePayload = RandomNegativeAuditSampler.payload(
            for: RandomNegativeAuditSampler.Candidate(
                stableId: "payload-nullability",
                firstAtomOrdinal: 61,
                lastAtomOrdinal: 70,
                fmDisposition: nil,
                wasFlagged: false
            ),
            jobId: "job-random-audit",
            jobPhase: BackfillJobPhase.scanRandomAuditWindows.rawValue
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(nullablePayload)
        let object = try #require(
            JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        #expect(object["fmDisposition"] is NSNull)
        #expect(object["manualReviewFoundAd"] is NSNull)

        let decoded = try JSONDecoder().decode(
            RandomNegativeAuditSampler.EvidencePayload.self,
            from: data
        )
        #expect(decoded.fmDisposition == nil)
        #expect(decoded.manualReviewFoundAd == nil)
    }

    @Test("sampler treats unrun windows with null FM disposition as eligible")
    func samplerAllowsNullFMDispositionForUnrunWindows() {
        let candidates = (0..<100).map { index in
            RandomNegativeAuditSampler.Candidate(
                stableId: "unrun-window-\(index)",
                firstAtomOrdinal: index * 10,
                lastAtomOrdinal: index * 10 + 9,
                fmDisposition: nil,
                wasFlagged: false
            )
        }

        let selection = RandomNegativeAuditSampler.select(
            candidates: candidates,
            sampleRate: 0.12,
            seedMaterial: "asset|transcript|unrun"
        )

        #expect(selection.eligibleCount == 100)
        #expect(selection.selected.count == 12)
        #expect(selection.selected.allSatisfy { $0.fmDisposition == nil && !$0.wasFlagged })
    }

    @Test("runner persists randomAudit EvidenceEvent rows for sampled no-ad audit windows")
    func runnerPersistsRandomAuditEvidenceEvents() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-random-audit"
        let transcriptVersion = "tx-random-audit"
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
            clock: { Date(timeIntervalSince1970: 2_000) }
        )
        let verboseEditorialText = Array(
            repeating: "Detailed editorial discussion without sponsor language.",
            count: 60
        ).joined(separator: "\n")
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: (0..<40).map { index in
                let start = Double(index) * 10
                return (start, start + 10, "\(verboseEditorialText)\nSegment \(index).")
            }
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-random-audit",
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

        _ = try await runner.runPendingBackfill(for: inputs)

        let events = try await store.fetchEvidenceEvents(analysisAssetId: assetId)
        let auditEvents = events.filter { $0.eventType == "randomAudit" }
        #expect(!auditEvents.isEmpty)
        #expect((4...6).contains(auditEvents.count))
        #expect(auditEvents.allSatisfy { $0.sourceType == .audit })
        #expect(auditEvents.allSatisfy { $0.jobPhase == BackfillJobPhase.scanRandomAuditWindows.rawValue })

        let event = try #require(auditEvents.first)
        let object = try #require(
            JSONSerialization.jsonObject(with: Data(event.evidenceJSON.utf8)) as? [String: Any]
        )
        #expect(object["manualReviewFoundAd"] is NSNull)
        let payload = try JSONDecoder().decode(
            RandomNegativeAuditSampler.EvidencePayload.self,
            from: Data(event.evidenceJSON.utf8)
        )
        #expect(payload.fmDisposition == "noAds")
        #expect(payload.manualReviewFoundAd == nil)
        #expect(payload.atomRange.firstAtomOrdinal <= payload.atomRange.lastAtomOrdinal)
    }

    @Test("observability event write failures do not fail random-audit reruns")
    func observabilityPersistenceFailuresAreBestEffort() async throws {
        let (store, directory) = try await makeTestStoreWithDirectory()
        let assetId = "asset-random-audit-observability-failure"
        let transcriptVersion = "tx-random-audit-observability-failure"
        try await store.insertAsset(makeTestAsset(id: assetId))

        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: (0..<40).map { index in
                let start = Double(index) * 10
                return (start, start + 10, "Editorial segment \(index).")
            }
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-random-audit-observability-failure",
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

        let firstRunner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: TestFMRuntime().runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            clock: { Date(timeIntervalSince1970: 2_300) }
        )
        let firstRun = try await firstRunner.runPendingBackfill(for: inputs)
        let jobId = try #require(firstRun.admittedJobIds.first)

        let initialEvents = try await store.fetchEvidenceEvents(analysisAssetId: assetId)
        let observabilityEventIds = initialEvents
            .filter { event in
                event.eventType == "randomAudit" ||
                (
                    event.eventType == OperationalMetrics.eventType &&
                    event.jobPhase == BackfillJobPhase.scanRandomAuditWindows.rawValue
                )
            }
            .map(\.id)
        #expect(!observabilityEventIds.isEmpty)

        try tamperEvidenceEventBodies(ids: observabilityEventIds, in: directory)
        try requeueBackfillJob(jobId: jobId, in: directory)

        let secondRunner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: TestFMRuntime().runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            clock: { Date(timeIntervalSince1970: 2_300) }
        )
        let secondRun = try await secondRunner.runPendingBackfill(for: inputs)

        #expect(secondRun.admittedJobIds.contains(jobId))
        let row = try #require(try await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .complete)
        #expect(row.retryCount == 0)
    }

    @Test("runner does not persist randomAudit rows for uncertain audit windows")
    func runnerSkipsUncertainRandomAuditWindows() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-random-audit-uncertain"
        let transcriptVersion = "tx-random-audit-uncertain"
        try await store.insertAsset(makeTestAsset(id: assetId))

        let runtime = TestFMRuntime(
            coarseResponses: Array(
                repeating: CoarseScreeningSchema(disposition: .uncertain, support: nil),
                count: 40
            )
        )
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            clock: { Date(timeIntervalSince1970: 2_100) }
        )
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: (0..<40).map { index in
                let start = Double(index) * 10
                return (start, start + 10, "Editorial segment \(index).")
            }
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-random-audit-uncertain",
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

        _ = try await runner.runPendingBackfill(for: inputs)

        let events = try await store.fetchEvidenceEvents(analysisAssetId: assetId)
        let auditEvents = events.filter { $0.eventType == "randomAudit" }
        #expect(auditEvents.isEmpty)
    }

    @Test("runner persists null FM disposition for audit windows deferred before FM runs")
    func runnerPersistsNullDispositionForDeferredAuditWindows() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-random-audit-deferred"
        let transcriptVersion = "tx-random-audit-deferred"
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
            clock: { Date(timeIntervalSince1970: 2_200) }
        )
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: (0..<40).map { index in
                let start = Double(index) * 10
                return (start, start + 10, "Editorial segment \(index).")
            }
        )
        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-random-audit-deferred",
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

        _ = try await runner.runPendingBackfill(for: inputs)

        #expect(await runtime.coarseCallCount == 0)
        let events = try await store.fetchEvidenceEvents(analysisAssetId: assetId)
        let auditEvents = events.filter { $0.eventType == "randomAudit" }
        #expect((4...6).contains(auditEvents.count))

        let payloads = try auditEvents.map {
            try JSONDecoder().decode(
                RandomNegativeAuditSampler.EvidencePayload.self,
                from: Data($0.evidenceJSON.utf8)
            )
        }
        #expect(payloads.allSatisfy { $0.fmDisposition == nil })
        #expect(payloads.allSatisfy { $0.manualReviewFoundAd == nil })

        let metricEvent = try #require(events.first { event in
            event.eventType == OperationalMetrics.eventType &&
            event.jobPhase == BackfillJobPhase.scanRandomAuditWindows.rawValue
        })
        let metrics = try JSONDecoder().decode(
            OperationalMetrics.self,
            from: Data(metricEvent.evidenceJSON.utf8)
        )
        #expect(metrics.counters.randomAuditCandidateCount == auditEvents.count)
        #expect(metrics.counters.randomAuditSelectedCount == auditEvents.count)
    }

    private func tamperEvidenceEventBodies(ids: [String], in directory: URL) throws {
        let db = try openRawDatabase(in: directory)
        defer { sqlite3_close_v2(db) }

        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(
            db,
            "UPDATE evidence_events SET evidenceJSON = ? WHERE id = ?",
            -1,
            &statement,
            nil
        ) == SQLITE_OK else {
            throw sqliteError(db, domain: "TamperEvidencePrepare")
        }
        defer { sqlite3_finalize(statement) }

        let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
        for id in ids {
            sqlite3_reset(statement)
            sqlite3_clear_bindings(statement)
            sqlite3_bind_text(statement, 1, "{\"tampered\":true}", -1, transient)
            sqlite3_bind_text(statement, 2, id, -1, transient)
            guard sqlite3_step(statement) == SQLITE_DONE else {
                throw sqliteError(db, domain: "TamperEvidenceStep")
            }
        }
    }

    private func requeueBackfillJob(jobId: String, in directory: URL) throws {
        let db = try openRawDatabase(in: directory)
        defer { sqlite3_close_v2(db) }

        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(
            db,
            """
            UPDATE backfill_jobs
            SET status = 'queued',
                retryCount = 0,
                deferReason = NULL,
                progressCursor = NULL,
                updatedAt = createdAt
            WHERE jobId = ?
            """,
            -1,
            &statement,
            nil
        ) == SQLITE_OK else {
            throw sqliteError(db, domain: "RequeueBackfillPrepare")
        }
        defer { sqlite3_finalize(statement) }

        let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
        sqlite3_bind_text(statement, 1, jobId, -1, transient)
        guard sqlite3_step(statement) == SQLITE_DONE else {
            throw sqliteError(db, domain: "RequeueBackfillStep")
        }
        #expect(sqlite3_changes(db) == 1)
    }

    private func openRawDatabase(in directory: URL) throws -> OpaquePointer {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK,
              let handle = db else {
            throw sqliteError(db, domain: "OpenRawDatabase")
        }
        return handle
    }

    private func sqliteError(_ db: OpaquePointer?, domain: String) -> NSError {
        NSError(
            domain: domain,
            code: Int(sqlite3_errcode(db)),
            userInfo: [
                NSLocalizedDescriptionKey: db
                    .map { String(cString: sqlite3_errmsg($0)) } ?? "unknown sqlite error"
            ]
        )
    }
}
