// SemanticScanAttributionWireInTests.swift
// playhead-hx6n — the attribution seam, exercised through the real runner.
//
// `SemanticScanRunAttributionTests` proves the schema, the join and the
// unknown-stays-unknown contract against hand-built rows. This file proves the
// part that hand-built rows cannot: that the PRODUCTION write path actually
// stamps every row it persists, and that the id it stamps is the one that joins.
//
// Without this, every insert site in `BackfillJobRunner` could be passing
// unattributed rows and the whole suite above would still be green — the columns
// would exist, the join would work in the abstract, and the device would keep
// producing NULLs.
//
// EVIDENCE NOTE: fixture only. `TestFMRuntime` supplies canned model responses
// and the clock and scene phase are injected, so nothing here is a device
// measurement.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("playhead-hx6n: BackfillJobRunner stamps run attribution")
struct SemanticScanAttributionWireInTests {

    private static let pinnedClock = Date(timeIntervalSince1970: 1_700_123_456)

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
        let transcriptVersion = "tx-\(assetId)-v1"
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: [
                (0, 30, "Welcome to the show. Today we're discussing podcasts."),
                (30, 60, "Use code SHOW for 20 percent off at example dot com."),
                (60, 90, "Now back to the interview with our guest.")
            ]
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "pod-\(assetId)",
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

    private func makeRunner(
        store: AnalysisStore,
        runtime: FoundationModelClassifier.Runtime,
        scenePhase: String
    ) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime, config: .default),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            clock: { Self.pinnedClock },
            scenePhaseProvider: { scenePhase }
        )
    }

    /// Count rows matching a predicate, straight out of SQLite, so the assertion
    /// is about what is ON DISK rather than about what the reader chose to
    /// surface.
    private func scalar(_ sql: String, in directory: URL) throws -> Int {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw AnalysisStoreError.queryFailed("open failed")
        }
        defer { sqlite3_close_v2(db) }
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            let message = sqlite3_errmsg(db).map { String(cString: $0) } ?? "unknown"
            throw AnalysisStoreError.queryFailed("prepare failed: \(message) — \(sql)")
        }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else {
            throw AnalysisStoreError.queryFailed("no row — \(sql)")
        }
        return Int(sqlite3_column_int(stmt, 0))
    }

    /// **The acceptance test: demonstrate the join, do not assert the column.**
    ///
    /// Run a real backfill through the real runner, then ask SQLite how many
    /// persisted scan rows fail to resolve to a `backfill_jobs` row through
    /// `runCorrelationId`. The answer has to be zero — and it is a stronger
    /// statement than "the column is non-null", because an id that names no job
    /// is exactly as useless as no id at all.
    @Test("every row the runner persists carries attribution, and the id resolves to a real job")
    func runnerStampsEveryRowAndTheCorrelationIdJoins() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        let assetId = "asset-hx6n-wirein"
        try await store.insertAsset(makeAsset(id: assetId))

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
            scenePhase: ScanScenePhase.background.rawValue
        )

        let outcome = try await runner.runPendingBackfill(for: makeInputs(assetId: assetId))
        try #require(
            !outcome.scanResultIds.isEmpty,
            "fixture precondition: the run must persist at least one scan row"
        )

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        try #require(!rows.isEmpty)

        for row in rows {
            #expect(
                row.createdAt == Self.pinnedClock.timeIntervalSince1970,
                """
                Row \(row.id) carries createdAt \(String(describing: row.createdAt)) \
                but the runner's clock is pinned to \
                \(Self.pinnedClock.timeIntervalSince1970). A row that misses the \
                attribution seam is undateable, which is the state this bead ends.
                """
            )
            #expect(
                row.scenePhase == .background,
                """
                Row \(row.id) carries scenePhase \(String(describing: row.scenePhase)) \
                where the injected provider always answers "background". An \
                unstamped row is not a small gap: it is indistinguishable on disk \
                from a pre-V42 row, and it silently shrinks the attributable corpus.
                """
            )
            #expect(row.runCorrelationId != nil, "row \(row.id) has no run correlation id")
        }

        // The id is not merely present — it JOINS.
        let orphans = try scalar(
            """
            SELECT COUNT(*)
            FROM semantic_scan_results s
            LEFT JOIN backfill_jobs j ON j.jobId = s.runCorrelationId
            WHERE s.analysisAssetId = '\(assetId)'
              AND j.jobId IS NULL
            """,
            in: dir
        )
        #expect(
            orphans == 0,
            """
            \(orphans) persisted scan row(s) carry a runCorrelationId that names \
            no backfill_jobs row. A correlation id that joins to nothing is \
            exactly as useless as no id at all — the measurement is still \
            impossible, it just looks like it should work.
            """
        )

        let joined = try scalar(
            """
            SELECT COUNT(*)
            FROM semantic_scan_results s
            JOIN backfill_jobs j ON j.jobId = s.runCorrelationId
            WHERE s.analysisAssetId = '\(assetId)'
            """,
            in: dir
        )
        #expect(joined == rows.count, "the join must return every row, not a subset")

        // And the split now has something to say.
        let split = try await store.fetchSemanticScanThroughputSplit()
        #expect(split.unattributed.scanCount == 0)
        #expect(split.foreground.scanCount == 0)
        #expect(split.background.scanCount > 0)
        #expect(split.attributedFraction == 1.0)
    }

    /// The same run in the foreground lands on the other side of the split. Two
    /// runs, one code path, two different answers — which is the property that
    /// makes the kvs8 measurement possible at all. If the phase were captured
    /// from anything other than the injected provider (a constant, the job's
    /// own record, the store's guess) both runs would report the same side and
    /// this goes red.
    @Test("a foreground run lands on the foreground side of the same split")
    func foregroundRunLandsInTheForegroundBucket() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-hx6n-foreground"
        try await store.insertAsset(makeAsset(id: assetId))

        let fmRuntime = TestFMRuntime()
        let runner = makeRunner(
            store: store,
            runtime: fmRuntime.runtime,
            scenePhase: ScanScenePhase.active.rawValue
        )
        _ = try await runner.runPendingBackfill(for: makeInputs(assetId: assetId))

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        try #require(!rows.isEmpty)
        #expect(rows.allSatisfy { $0.scenePhase == .active })

        let split = try await store.fetchSemanticScanThroughputSplit()
        #expect(split.background.scanCount == 0)
        #expect(split.unattributed.scanCount == 0)
    }

    /// A provider that answers with a string outside the vocabulary — a broken
    /// contract, not a normal state — must produce `nil`, i.e. UNATTRIBUTED. It
    /// must not fall back to a phase, and it must not throw away the row.
    @Test("a provider that breaks the vocabulary yields unattributed rows, not guessed ones")
    func brokenProviderContractYieldsUnattributed() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-hx6n-badphase"
        try await store.insertAsset(makeAsset(id: assetId))

        let fmRuntime = TestFMRuntime()
        let runner = makeRunner(store: store, runtime: fmRuntime.runtime, scenePhase: "not-a-phase")
        _ = try await runner.runPendingBackfill(for: makeInputs(assetId: assetId))

        let rows = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        try #require(!rows.isEmpty)
        #expect(rows.allSatisfy { $0.scenePhase == nil })
        // The rest of the attribution still lands — one broken field does not
        // cost the row its timestamp or its join key.
        #expect(rows.allSatisfy { $0.createdAt == Self.pinnedClock.timeIntervalSince1970 })
        #expect(rows.allSatisfy { $0.runCorrelationId != nil })
    }
}
