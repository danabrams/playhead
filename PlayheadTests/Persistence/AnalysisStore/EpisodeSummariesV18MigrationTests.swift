// EpisodeSummariesV18MigrationTests.swift
// playhead-jzik: schema v18 introduces the `episode_summaries` table —
// 1:1 with `analysis_assets`, holds the on-device verbatim summary
// produced by `EpisodeSummaryExtractor`.
//
// Coverage:
//   1. Fresh-migrate shape: table + index land at v18.
//   2. v17-seeded ladder: a DB rewound to v17 picks up the new table on
//      the next `migrate()`.
//   3. Idempotence: a second `migrate()` is a no-op (version stays at
//      18, table not duplicated).
//   4. Round-trip: upsert + fetch preserves all fields including the
//      JSON-encoded topic / guest arrays.
//   5. ON DELETE CASCADE: deleting the parent asset row removes the
//      summary too.
//   6. Backfill candidate query: returns assets above coverage threshold
//      that lack a summary or carry a stale schemaVersion.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("AnalysisStore episode_summaries migration v18 (playhead-jzik)")
struct EpisodeSummariesV18MigrationTests {

    // MARK: - 1. Fresh-migrate shape

    @Test("Fresh DB: episode_summaries table + index land at v18")
    func freshDbAddsEpisodeSummariesTable() async throws {
        let dir = try makeTempDir(prefix: "EpisodeSummariesV18-Fresh")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: "episode_summaries"))
        #expect(try probeColumnExists(in: dir, table: "episode_summaries", column: "analysisAssetId"))
        #expect(try probeColumnExists(in: dir, table: "episode_summaries", column: "summary"))
        #expect(try probeColumnExists(in: dir, table: "episode_summaries", column: "mainTopicsJSON"))
        #expect(try probeColumnExists(in: dir, table: "episode_summaries", column: "notableGuestsJSON"))
        #expect(try probeColumnExists(in: dir, table: "episode_summaries", column: "schemaVersion"))
        #expect(try probeColumnExists(in: dir, table: "episode_summaries", column: "transcriptVersion"))
        #expect(try probeColumnExists(in: dir, table: "episode_summaries", column: "createdAt"))
        #expect(try probeIndexExists(in: dir, indexName: "idx_episode_summaries_transcript_version"))
    }

    // MARK: - 2. v17-seeded ladder

    @Test("v17-seeded DB picks up episode_summaries at v18")
    func v17ChainsToV18() async throws {
        let dir = try makeTempDir(prefix: "EpisodeSummariesV18-FromV17")
        defer { try? FileManager.default.removeItem(at: dir) }

        // Phase 1: real migrate to current.
        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        #expect(try await bootstrap.schemaVersion() == AnalysisStore.currentSchemaVersion)

        // Phase 2: rewind to v17 — drop the new table + reset _meta.
        let dbURL = dir.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        #expect(sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK)
        let rewind = """
            DROP INDEX IF EXISTS idx_episode_summaries_transcript_version;
            DROP TABLE IF EXISTS episode_summaries;
            UPDATE _meta SET value = '17' WHERE key = 'schema_version';
            """
        #expect(sqlite3_exec(db, rewind, nil, nil, nil) == SQLITE_OK)
        sqlite3_close_v2(db)

        // Sanity: the rewind actually removed the table.
        #expect(!(try probeTableExists(in: dir, table: "episode_summaries")))

        // Phase 3: re-migrate. The v17 → v18 block must re-add the table.
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: "episode_summaries"))
    }

    // MARK: - 3. Idempotence

    @Test("Second migrate() is a no-op — version stays at 18, table not duplicated")
    func secondMigrateIsNoOp() async throws {
        let dir = try makeTempDir(prefix: "EpisodeSummariesV18-Idempotent")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: "episode_summaries"))
    }

    // MARK: - 4. Round-trip

    @Test("upsert + fetch preserves all fields including JSON arrays")
    func upsertFetchRoundTrip() async throws {
        let store = try await makeTestStore()

        // Parent asset row is a FK precondition.
        try await store.insertAsset(makeAsset(id: "asset-jzik-rt"))

        let original = EpisodeSummary(
            analysisAssetId: "asset-jzik-rt",
            summary: "A 2-3 sentence verbatim grounded summary of the episode.",
            mainTopics: ["leadership", "burnout", "remote work"],
            notableGuests: ["Jane Doe"],
            transcriptVersion: "v-2026-04-29-abc123",
            createdAt: Date(timeIntervalSince1970: 1_714_000_000)
        )
        try await store.upsertEpisodeSummary(original)

        let reloaded = try await store.fetchEpisodeSummary(assetId: "asset-jzik-rt")
        #expect(reloaded?.analysisAssetId == "asset-jzik-rt")
        #expect(reloaded?.summary == original.summary)
        #expect(reloaded?.mainTopics == original.mainTopics)
        #expect(reloaded?.notableGuests == original.notableGuests)
        #expect(reloaded?.schemaVersion == EpisodeSummary.currentSchemaVersion)
        #expect(reloaded?.transcriptVersion == "v-2026-04-29-abc123")
        #expect(reloaded?.createdAt.timeIntervalSince1970 == 1_714_000_000)
    }

    @Test("upsert overwrites previous row for same asset id")
    func upsertOverwritesExistingRow() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-jzik-overwrite"))

        let first = EpisodeSummary(
            analysisAssetId: "asset-jzik-overwrite",
            summary: "First.",
            mainTopics: ["a", "b"],
            notableGuests: [],
            transcriptVersion: "v1",
            createdAt: Date(timeIntervalSince1970: 1_000_000)
        )
        try await store.upsertEpisodeSummary(first)

        let second = EpisodeSummary(
            analysisAssetId: "asset-jzik-overwrite",
            summary: "Second — replaces first entirely.",
            mainTopics: ["c"],
            notableGuests: ["Guest"],
            transcriptVersion: "v2",
            createdAt: Date(timeIntervalSince1970: 2_000_000)
        )
        try await store.upsertEpisodeSummary(second)

        let reloaded = try await store.fetchEpisodeSummary(assetId: "asset-jzik-overwrite")
        #expect(reloaded?.summary == "Second — replaces first entirely.")
        #expect(reloaded?.mainTopics == ["c"])
        #expect(reloaded?.notableGuests == ["Guest"])
        #expect(reloaded?.transcriptVersion == "v2")
    }

    @Test("delete removes the row idempotently")
    func deleteRemovesRowIdempotently() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-jzik-del"))

        try await store.upsertEpisodeSummary(
            EpisodeSummary(
                analysisAssetId: "asset-jzik-del",
                summary: "to be deleted",
                mainTopics: [],
                notableGuests: [],
                transcriptVersion: nil,
                createdAt: Date()
            )
        )
        #expect(try await store.fetchEpisodeSummary(assetId: "asset-jzik-del") != nil)

        try await store.deleteEpisodeSummary(assetId: "asset-jzik-del")
        #expect(try await store.fetchEpisodeSummary(assetId: "asset-jzik-del") == nil)

        // Idempotent — second delete must not throw.
        try await store.deleteEpisodeSummary(assetId: "asset-jzik-del")
    }

    // MARK: - 5. ON DELETE CASCADE

    @Test("Deleting parent asset cascades to episode_summaries row")
    func deletingAssetCascadesSummary() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "asset-jzik-cascade"))
        try await store.upsertEpisodeSummary(
            EpisodeSummary(
                analysisAssetId: "asset-jzik-cascade",
                summary: "cascaded summary",
                mainTopics: ["x"],
                notableGuests: [],
                transcriptVersion: "v1",
                createdAt: Date()
            )
        )

        try await store.deleteAsset(id: "asset-jzik-cascade")
        #expect(try await store.fetchEpisodeSummary(assetId: "asset-jzik-cascade") == nil)
    }

    // MARK: - 6. Backfill candidate query

    @Test("backfill candidates: covered + summary-missing assets are returned")
    func backfillCandidatesMissingSummary() async throws {
        let store = try await makeTestStore()

        // Eligible: 90% real union coverage, ad detection complete, no summary.
        try await store.insertAsset(
            makeAsset(
                id: "asset-jzik-eligible",
                fastTranscriptCoverageEndTime: 90.0,
                episodeDurationSec: 100.0
            )
        )
        try await insertFastChunk(store, assetId: "asset-jzik-eligible", start: 0, end: 90)

        // Ineligible: 50% union coverage.
        try await store.insertAsset(
            makeAsset(
                id: "asset-jzik-low-coverage",
                fastTranscriptCoverageEndTime: 50.0,
                episodeDurationSec: 100.0
            )
        )
        try await insertFastChunk(store, assetId: "asset-jzik-low-coverage", start: 0, end: 50)

        // Eligible coverage but already has current-schema summary.
        try await store.insertAsset(
            makeAsset(
                id: "asset-jzik-already-summarized",
                fastTranscriptCoverageEndTime: 95.0,
                episodeDurationSec: 100.0
            )
        )
        try await insertFastChunk(store, assetId: "asset-jzik-already-summarized", start: 0, end: 95)
        try await store.upsertEpisodeSummary(
            EpisodeSummary(
                analysisAssetId: "asset-jzik-already-summarized",
                summary: "already done",
                mainTopics: [],
                notableGuests: [],
                transcriptVersion: "v1",
                createdAt: Date()
            )
        )

        let candidates = try await store.fetchEpisodeSummaryBackfillCandidates(
            coverageFraction: 0.8,
            currentSchemaVersion: EpisodeSummary.currentSchemaVersion,
            limit: 10
        )
        #expect(candidates.contains("asset-jzik-eligible"))
        #expect(!candidates.contains("asset-jzik-low-coverage"))
        #expect(!candidates.contains("asset-jzik-already-summarized"))
    }

    @Test("backfill candidates: stale schemaVersion forces re-extract")
    func backfillCandidatesStaleSchema() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeAsset(
                id: "asset-jzik-stale",
                fastTranscriptCoverageEndTime: 90.0,
                episodeDurationSec: 100.0
            )
        )
        try await insertFastChunk(store, assetId: "asset-jzik-stale", start: 0, end: 90)
        try await store.upsertEpisodeSummary(
            EpisodeSummary(
                analysisAssetId: "asset-jzik-stale",
                summary: "stale summary",
                mainTopics: [],
                notableGuests: [],
                schemaVersion: 0,  // forced stale
                transcriptVersion: "v1",
                createdAt: Date()
            )
        )

        let candidates = try await store.fetchEpisodeSummaryBackfillCandidates(
            coverageFraction: 0.8,
            currentSchemaVersion: EpisodeSummary.currentSchemaVersion,
            limit: 10
        )
        #expect(candidates.contains("asset-jzik-stale"))
    }

    @Test("backfill candidates: assets without duration are excluded")
    func backfillCandidatesMissingDuration() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeAsset(
                id: "asset-jzik-no-duration",
                fastTranscriptCoverageEndTime: 60.0,
                episodeDurationSec: nil
            )
        )
        try await insertFastChunk(store, assetId: "asset-jzik-no-duration", start: 0, end: 60)

        let candidates = try await store.fetchEpisodeSummaryBackfillCandidates(
            coverageFraction: 0.8,
            currentSchemaVersion: EpisodeSummary.currentSchemaVersion,
            limit: 10
        )
        #expect(!candidates.contains("asset-jzik-no-duration"))
    }

    // MARK: - 7. Real-coverage gate (playhead-g4dk)

    @Test("backfill candidates: high watermark but low real (union) coverage is rejected")
    func backfillCandidatesHighWatermarkLowUnionRejected() async throws {
        let store = try await makeTestStore()

        // The g4dk bug shape: the watermark REACHES 90/100 (90%) so the old
        // `fastTranscriptCoverageEndTime / duration` gate admitted it, but
        // the transcript is gappy — only ~39s of fast text is actually
        // present. The union gate must reject it.
        try await store.insertAsset(
            makeAsset(
                id: "asset-g4dk-gappy",
                fastTranscriptCoverageEndTime: 90.0,
                episodeDurationSec: 100.0
            )
        )
        // Two short fast chunks near the start and end, unioning to 39s of
        // real coverage even though the reach (max endTime) is 90s.
        try await insertFastChunk(store, assetId: "asset-g4dk-gappy", start: 0, end: 20, index: 0)
        try await insertFastChunk(store, assetId: "asset-g4dk-gappy", start: 71, end: 90, index: 1)

        // Control: a genuinely 90%-covered sibling that MUST still pass, so
        // the rejection above is attributable to real coverage, not the gate
        // rejecting everything.
        try await store.insertAsset(
            makeAsset(
                id: "asset-g4dk-dense",
                fastTranscriptCoverageEndTime: 90.0,
                episodeDurationSec: 100.0
            )
        )
        try await insertFastChunk(store, assetId: "asset-g4dk-dense", start: 0, end: 90)

        let candidates = try await store.fetchEpisodeSummaryBackfillCandidates(
            coverageFraction: 0.8,
            currentSchemaVersion: EpisodeSummary.currentSchemaVersion,
            limit: 10
        )
        #expect(!candidates.contains("asset-g4dk-gappy"))
        #expect(candidates.contains("asset-g4dk-dense"))
    }

    @Test("backfill candidates: episode where ad detection has not completed is rejected")
    func backfillCandidatesAdDetectionIncompleteRejected() async throws {
        let store = try await makeTestStore()

        // Fully covered transcript (90/100) but analysis is still in
        // progress and no confirmed-ad coverage exists yet — summarizing now
        // would run on a pre-ad-detection transcript and leak a sponsor read.
        try await store.insertAsset(
            makeAsset(
                id: "asset-g4dk-pre-addetect",
                fastTranscriptCoverageEndTime: 90.0,
                episodeDurationSec: 100.0,
                analysisState: "backfill",
                confirmedAdCoverageEndTime: nil
            )
        )
        try await insertFastChunk(store, assetId: "asset-g4dk-pre-addetect", start: 0, end: 90)

        let rejected = try await store.fetchEpisodeSummaryBackfillCandidates(
            coverageFraction: 0.8,
            currentSchemaVersion: EpisodeSummary.currentSchemaVersion,
            limit: 10
        )
        #expect(!rejected.contains("asset-g4dk-pre-addetect"))

        // Once ad detection confirms coverage (confirmedAdCoverageEndTime set)
        // the SAME still-in-progress row becomes eligible — proving the
        // rejection above was the completion gate, not the coverage gate.
        try await store.updateConfirmedAdCoverage(id: "asset-g4dk-pre-addetect", endTime: 88.0)
        let admitted = try await store.fetchEpisodeSummaryBackfillCandidates(
            coverageFraction: 0.8,
            currentSchemaVersion: EpisodeSummary.currentSchemaVersion,
            limit: 10
        )
        #expect(admitted.contains("asset-g4dk-pre-addetect"))
    }

    @Test("backfill candidates: no-ad episode is eligible once full analysis completes")
    func backfillCandidatesNoAdEpisodeEligibleWhenComplete() async throws {
        let store = try await makeTestStore()

        // An episode with NO detected ads never gets confirmedAdCoverageEndTime
        // (it only advances to max(nonSuppressedWindows.endTime)). It must
        // still be summary-eligible once full analysis completes, otherwise
        // ad-free episodes would never get a summary.
        try await store.insertAsset(
            makeAsset(
                id: "asset-g4dk-no-ads",
                fastTranscriptCoverageEndTime: 95.0,
                episodeDurationSec: 100.0,
                analysisState: "completeFull",
                confirmedAdCoverageEndTime: nil
            )
        )
        try await insertFastChunk(store, assetId: "asset-g4dk-no-ads", start: 0, end: 95)

        let candidates = try await store.fetchEpisodeSummaryBackfillCandidates(
            coverageFraction: 0.8,
            currentSchemaVersion: EpisodeSummary.currentSchemaVersion,
            limit: 10
        )
        #expect(candidates.contains("asset-g4dk-no-ads"))
    }

    @Test("backfill candidates: no-ad completeTranscriptPartial episode is still eligible")
    func backfillCandidatesNoAdPartialTranscriptEligible() async throws {
        let store = try await makeTestStore()

        // completeTranscriptPartial is a terminal-completion state (ad
        // detection ran when the session left `.backfill`) whose transcript
        // reach fell short of the 0.95 finalize ratio — a COMMON band for
        // shows with music/applause/outro tails. A no-ad episode there never
        // gets confirmedAdCoverageEndTime, so gating on the confirmed-ad
        // watermark alone would deny it a summary forever. Its 90% union
        // coverage clears the coverage gate, so it must be eligible.
        try await store.insertAsset(
            makeAsset(
                id: "asset-g4dk-partial-noads",
                fastTranscriptCoverageEndTime: 90.0,
                episodeDurationSec: 100.0,
                analysisState: "completeTranscriptPartial",
                confirmedAdCoverageEndTime: nil
            )
        )
        try await insertFastChunk(store, assetId: "asset-g4dk-partial-noads", start: 0, end: 90)

        let candidates = try await store.fetchEpisodeSummaryBackfillCandidates(
            coverageFraction: 0.8,
            currentSchemaVersion: EpisodeSummary.currentSchemaVersion,
            limit: 10
        )
        #expect(candidates.contains("asset-g4dk-partial-noads"))
    }

    // MARK: - Fixtures

    /// playhead-g4dk: eligibility now gates on ad-detection completion, so
    /// tests default the asset to a full-analysis terminal. Individual
    /// tests override `analysisState` / `confirmedAdCoverageEndTime` to
    /// exercise the completion gate.
    private func makeAsset(
        id: String,
        fastTranscriptCoverageEndTime: Double? = nil,
        episodeDurationSec: Double? = nil,
        analysisState: String = "completeFull",
        confirmedAdCoverageEndTime: Double? = nil
    ) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime,
            confirmedAdCoverageEndTime: confirmedAdCoverageEndTime,
            analysisState: analysisState,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDurationSec
        )
    }

    /// playhead-g4dk: the candidate gate now decides on interval-unioned
    /// `pass='fast'` coverage, so tests must land real transcript chunks
    /// (the watermark alone no longer qualifies an asset). Inserts one
    /// fast chunk spanning `[start, end)` — union coverage == `end - start`.
    private func insertFastChunk(
        _ store: AnalysisStore,
        assetId: String,
        start: Double,
        end: Double,
        index: Int = 0,
        text: String = "editorial content"
    ) async throws {
        try await store.insertTranscriptChunk(
            TranscriptChunk(
                id: "\(assetId)-chunk-\(index)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(assetId)-\(index)",
                chunkIndex: index,
                startTime: start,
                endTime: end,
                text: text,
                normalizedText: text,
                pass: "fast",
                modelVersion: "test",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        )
    }
}
