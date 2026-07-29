// DuplicateAssetReconcileTests.swift
// playhead-0hi9 parts 3 & 4 — repair the pairs already on disk, then make the
// pairing structurally impossible.
//
// The shape being repaired, measured on the owner's device: 14 of 25 episodes
// held two `analysis_assets` rows, 13 of those pointing at the SAME file.
//   * the NEWER row carried the correct duration, all 14 fingerprints, most
//     scan rows and windows — and sat at `analysisState = 'queued'` with
//     90–420 s of transcript;
//   * the older row carried duration 528–561 s (an 8 MiB download prefix),
//     zero fingerprints, and every terminal state, with 630–1746 s of
//     transcript.
//
// Prevention alone leaves those 13 pairs broken forever: both existing launch
// sweeps (`did_duration_backfill_v1`, `did_terminal_state_reconcile_v1`) are
// one-shot, already marked done on that install, and per-row duplicate-blind.

@preconcurrency import AVFoundation
import Foundation
import Testing

@testable import Playhead

@Suite("playhead-0hi9 — duplicate analysis_assets reconciliation", .serialized)
struct DuplicateAssetReconcileTests {

    private static let tempDirs = TestTempDirTracker()

    // MARK: - Fixtures

    private let canonicalSHA = String(repeating: "c", count: 64)

    private func insertPlaceholder(
        store: AnalysisStore,
        id: String,
        episodeId: String,
        state: String = SessionState.completeFull.rawValue,
        featureCoverage: Double? = nil,
        transcriptCoverage: Double? = nil,
        duration: Double? = nil,
        title: String? = nil,
        sourceURL: String? = nil
    ) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: id,
            episodeId: episodeId,
            assetFingerprint: id, // the Pipeline A signature: self-reference
            weakFingerprint: nil,
            // Default: the 13-of-14 shape — both rows name the SAME artifact,
            // reached by different container paths.
            sourceURL: sourceURL ?? "file:///container-A/partials/\(episodeId).mp3",
            featureCoverageEndTime: featureCoverage,
            fastTranscriptCoverageEndTime: transcriptCoverage,
            confirmedAdCoverageEndTime: nil,
            analysisState: state,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: duration,
            episodeTitle: title
        ))
    }

    private func insertCanonical(
        store: AnalysisStore,
        id: String,
        episodeId: String,
        fingerprint: String,
        state: String = SessionState.queued.rawValue,
        featureCoverage: Double? = nil,
        transcriptCoverage: Double? = nil,
        duration: Double? = nil,
        sourceURL: String? = nil
    ) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: id,
            episodeId: episodeId,
            assetFingerprint: fingerprint,
            weakFingerprint: nil,
            sourceURL: sourceURL ?? "file:///container-B/complete/\(episodeId).mp3",
            featureCoverageEndTime: featureCoverage,
            fastTranscriptCoverageEndTime: transcriptCoverage,
            confirmedAdCoverageEndTime: nil,
            analysisState: state,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: duration
        ))
    }

    private func insertSession(store: AnalysisStore, id: String, assetId: String, state: String) async throws {
        let now = Date().timeIntervalSince1970
        try await store.insertSession(AnalysisSession(
            id: id,
            analysisAssetId: assetId,
            state: state,
            startedAt: now,
            updatedAt: now,
            failureReason: nil
        ))
    }

    private func insertChunk(
        store: AnalysisStore,
        id: String,
        assetId: String,
        index: Int,
        start: Double,
        end: Double
    ) async throws {
        try await store.insertTranscriptChunks([TranscriptChunk(
            id: id,
            analysisAssetId: assetId,
            segmentFingerprint: "seg-\(id)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: "chunk \(index)",
            normalizedText: "chunk \(index)",
            pass: TranscriptPassType.fast.rawValue,
            modelVersion: "speech-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            weakAnchorMetadata: nil
        )])
    }

    private func allAssets(store: AnalysisStore, episodeId: String) async throws -> [AnalysisAsset] {
        var out: [AnalysisAsset] = []
        var cursor: Int64 = 0
        while true {
            let page = try await store.fetchAssetsKeysetByRowId(afterRowId: cursor, limit: 200)
            if page.isEmpty { break }
            for (rowId, asset) in page {
                cursor = max(cursor, rowId)
                if asset.episodeId == episodeId { out.append(asset) }
            }
            if page.count < 200 { break }
        }
        return out
    }

    // MARK: - Schema introspection

    @Test("child tables are enumerated from the LIVE schema, not a hand-written list")
    func childColumnsComeFromSchema() async throws {
        let store = try await makeTestStore()
        let columns = try await store.assetReferencingChildColumns()
        let pairs = Set(columns.map { "\($0.table).\($0.column)" })

        // A representative slice: cascade children, the durable job pin, the
        // `assetId`-named columns, and `ad_listen_rewinds`, whose column
        // arrived via ALTER TABLE in V34 and appears in no CREATE TABLE body —
        // the exact case a hand-written list gets wrong.
        for expected in [
            "analysis_sessions.analysisAssetId",
            "transcript_chunks.analysisAssetId",
            "ad_windows.analysisAssetId",
            "semantic_scan_results.analysisAssetId",
            "analysis_jobs.analysisAssetId",
            "episode_fingerprints.analysisAssetId",
            "decoded_spans.assetId",
            "background_task_runs.assetId",
            "ad_listen_rewinds.analysisAssetId",
        ] {
            #expect(pairs.contains(expected), "schema scan missed \(expected)")
        }
        #expect(!pairs.contains { $0.hasPrefix("analysis_assets.") },
                "the parent table must never be re-pointed at itself")
    }

    // MARK: - Survivor selection

    @Test("the canonical-SHA row survives even when the placeholder is newer")
    func canonicalSHARowIsTheSurvivor() {
        let placeholder = AssetMergeRow(
            rowId: 9, id: "ph", assetFingerprint: "ph",
            createdAt: 2_000, analysisState: "completeFull", terminalReason: nil,
            sourceURL: "file:///a/shared.mp3"
        )
        let canonical = AssetMergeRow(
            rowId: 1, id: "sha-row", assetFingerprint: String(repeating: "d", count: 64),
            createdAt: 1_000, analysisState: "queued", terminalReason: nil,
            sourceURL: "file:///b/shared.mp3"
        )
        let chosen = AnalysisStore.chooseMergeSurvivor([placeholder, canonical])
        #expect(chosen?.id == "sha-row")
    }

    /// R1: `chooseMergeSurvivor` breaks a `createdAt` tie on `rowId DESC` to
    /// match `fetchAssetByEpisodeId`'s ordering — the merge must keep whichever
    /// row the rest of the app already treats as current. Every other survivor
    /// test uses distinct timestamps, so nothing pinned the tiebreak, and
    /// `createdAt` ties are the norm on a fixture-seeded or fast-inserting
    /// database (the column defaults to whole seconds).
    @Test("a createdAt tie is broken on rowid, matching fetchAssetByEpisodeId")
    func createdAtTieIsBrokenOnRowId() {
        let lowerRow = AssetMergeRow(
            rowId: 3, id: "low", assetFingerprint: "low",
            createdAt: 1_000, analysisState: "queued", terminalReason: nil,
            sourceURL: "file:///a/shared.mp3"
        )
        let higherRow = AssetMergeRow(
            rowId: 7, id: "high", assetFingerprint: "high",
            createdAt: 1_000, analysisState: "queued", terminalReason: nil,
            sourceURL: "file:///a/shared.mp3"
        )
        #expect(AnalysisStore.chooseMergeSurvivor([lowerRow, higherRow])?.id == "high")
        #expect(AnalysisStore.chooseMergeSurvivor([higherRow, lowerRow])?.id == "high",
                "the verdict must not depend on the order rows came out of SQLite")
    }

    @Test("with no SHA row at all, the newest row survives")
    func newestRowSurvivesWithoutSHA() {
        let older = AssetMergeRow(
            rowId: 1, id: "a", assetFingerprint: "a",
            createdAt: 1_000, analysisState: "queued", terminalReason: nil,
            sourceURL: "file:///b/shared.mp3"
        )
        let newer = AssetMergeRow(
            rowId: 2, id: "b", assetFingerprint: "b",
            createdAt: 2_000, analysisState: "queued", terminalReason: nil,
            sourceURL: "file:///a/shared.mp3"
        )
        #expect(AnalysisStore.chooseMergeSurvivor([older, newer])?.id == "b")
    }

    // MARK: - The merge

    @Test("the device shape converges: one row, child rows moved, watermarks maxed")
    func devicePairConverges() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-device"
        let placeholderId = UUID().uuidString
        let canonicalId = "canonical-row"

        // Older row: terminal state, deep transcript, poisoned 543 s duration.
        try await insertPlaceholder(
            store: store,
            id: placeholderId,
            episodeId: episodeId,
            state: SessionState.completeFull.rawValue,
            featureCoverage: 1_700,
            transcriptCoverage: 1_746,
            duration: 543,
            title: "Episode 12"
        )
        // Newer row: correct duration, shallow transcript, still `queued`.
        try await insertCanonical(
            store: store,
            id: canonicalId,
            episodeId: episodeId,
            fingerprint: canonicalSHA,
            state: SessionState.queued.rawValue,
            featureCoverage: 400,
            transcriptCoverage: 420,
            duration: 2_933
        )

        try await insertSession(store: store, id: "sess-old", assetId: placeholderId, state: SessionState.completeFull.rawValue)
        try await insertSession(store: store, id: "sess-new", assetId: canonicalId, state: SessionState.queued.rawValue)
        try await insertChunk(store: store, id: "chunk-old", assetId: placeholderId, index: 0, start: 1_700, end: 1_746)
        try await insertChunk(store: store, id: "chunk-new", assetId: canonicalId, index: 0, start: 0, end: 420)

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.episodesInspected == 1)
        #expect(summary.placeholdersMerged == 1)
        #expect(summary.survivingAssetIds == [canonicalId])
        #expect(summary.childRowsRepointed > 0)

        let rows = try await allAssets(store: store, episodeId: episodeId)
        #expect(rows.count == 1, "the pair must converge to one row (got \(rows.count))")
        let survivor = try #require(rows.first)
        #expect(survivor.id == canonicalId)

        // Watermarks are the max of the pair.
        #expect(survivor.featureCoverageEndTime == 1_700)
        #expect(survivor.fastTranscriptCoverageEndTime == 1_746)
        // Duration keeps the survivor's real value, NOT a max that would let
        // the 543 s download-prefix artifact win.
        #expect(survivor.episodeDurationSec == 2_933)
        // NULLs filled from the folded-in row.
        #expect(survivor.episodeTitle == "Episode 12")

        // Every child row followed, none orphaned, none cascaded away.
        let chunks = try await store.fetchTranscriptChunks(assetId: canonicalId)
        #expect(chunks.count == 2, "both transcripts must end up on the surviving row")
        let movedSession = try #require(try await store.fetchSession(id: "sess-old"))
        #expect(movedSession.analysisAssetId == canonicalId,
                "session history must follow the merge, not cascade away with the placeholder")
    }

    @Test("a completion terminal is adopted onto a survivor that had none")
    func terminalStateIsAdopted() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-adopt"
        try await insertPlaceholder(
            store: store, id: "ph-adopt", episodeId: episodeId,
            state: SessionState.completeFull.rawValue, duration: 543
        )
        try await insertCanonical(
            store: store, id: "canon-adopt", episodeId: episodeId,
            fingerprint: canonicalSHA, state: SessionState.queued.rawValue
        )

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.adoptedTerminalStates == 1)
        let survivor = try #require(try await store.fetchAsset(id: "canon-adopt"))
        #expect(survivor.analysisState == SessionState.completeFull.rawValue)
    }

    @Test("a mid-pipeline state is NOT adopted — the survivor stays resumable")
    func midPipelineStateIsNotAdopted() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-spooling"
        try await insertPlaceholder(
            store: store, id: "ph-spool", episodeId: episodeId,
            state: SessionState.spooling.rawValue
        )
        try await insertCanonical(
            store: store, id: "canon-spool", episodeId: episodeId,
            fingerprint: canonicalSHA, state: SessionState.queued.rawValue
        )

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.placeholdersMerged == 1)
        #expect(summary.adoptedTerminalStates == 0)
        let survivor = try #require(try await store.fetchAsset(id: "canon-spool"))
        #expect(survivor.analysisState == SessionState.queued.rawValue,
                "`spooling` describes work in flight against a row that no longer exists")
    }

    @Test("a child row that collides on a UNIQUE asset key is discarded, never orphaned")
    func conflictingChildRowIsDiscarded() async throws {
        let (store, directory) = try await makeTestStoreWithDirectory()
        let episodeId = "ep-conflict"
        try await insertPlaceholder(store: store, id: "ph-conflict", episodeId: episodeId)
        try await insertCanonical(
            store: store, id: "canon-conflict", episodeId: episodeId, fingerprint: canonicalSHA
        )

        // `episode_fingerprints.analysisAssetId` is a PRIMARY KEY, so both rows
        // holding one is a genuine collision the UPDATE cannot resolve.
        for (assetId, identity) in [("ph-conflict", "placeholder"), ("canon-conflict", "canonical")] {
            try await store.execForTesting("""
                INSERT INTO episode_fingerprints
                (analysisAssetId, algorithmVersion, secondsPerFingerprint,
                 fingerprintCount, fingerprintBlob, sourceAudioIdentity, capturedAt)
                VALUES ('\(assetId)', 1, 1.0, 0, X'00', '\(identity)', 0.0)
                """)
        }

        #expect(try probeRowCount(in: directory, table: "episode_fingerprints") == 2)

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.childRowsDiscarded >= 1,
                "the placeholder's colliding row cannot move and must be deleted explicitly")

        // Exactly one row left, and it is the SURVIVOR's — the row the rest of
        // the app has been reading. Nothing orphaned, nothing duplicated.
        #expect(try probeRowCount(in: directory, table: "episode_fingerprints") == 1)
        #expect(try probeRowCount(
            in: directory,
            table: "episode_fingerprints WHERE sourceAudioIdentity = 'canonical'"
        ) == 1)
        #expect(try probeRowCount(
            in: directory,
            table: "episode_fingerprints WHERE analysisAssetId = 'ph-conflict'"
        ) == 0)
        let rows = try await allAssets(store: store, episodeId: episodeId)
        #expect(rows.count == 1)
    }

    @Test("running twice is a no-op — the sweep is idempotent")
    func sweepIsIdempotent() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-idem"
        try await insertPlaceholder(store: store, id: "ph-idem", episodeId: episodeId)
        try await insertCanonical(
            store: store, id: "canon-idem", episodeId: episodeId, fingerprint: canonicalSHA
        )

        let first = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(first.placeholdersMerged == 1)
        let second = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(second == DuplicateAssetMergeSummary(),
                "a second pass over reconciled data must change nothing")
    }

    @Test("two legitimate rows under different SHAs are left alone")
    func distinctContentIdentitiesAreNotMerged() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-refeed"
        try await insertCanonical(
            store: store, id: "canon-a", episodeId: episodeId,
            fingerprint: String(repeating: "a", count: 64)
        )
        try await insertCanonical(
            store: store, id: "canon-b", episodeId: episodeId,
            fingerprint: String(repeating: "b", count: 64)
        )

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.episodesInspected == 1)
        #expect(summary.placeholdersMerged == 0,
                "a feed correction or re-download legitimately produces two content identities")
        #expect(try await allAssets(store: store, episodeId: episodeId).count == 2)
    }

    @Test("all 13 pairs converge in one pass with no orphaned child rows")
    func thirteenPairsConverge() async throws {
        let store = try await makeTestStore()
        var canonicalIds: [String] = []
        for index in 0..<13 {
            let episodeId = "ep-batch-\(index)"
            let placeholderId = UUID().uuidString
            let canonicalId = "canon-batch-\(index)"
            canonicalIds.append(canonicalId)
            try await insertPlaceholder(
                store: store, id: placeholderId, episodeId: episodeId,
                state: SessionState.completeTranscriptPartial.rawValue,
                transcriptCoverage: Double(600 + index), duration: 528 + Double(index)
            )
            try await insertCanonical(
                store: store, id: canonicalId, episodeId: episodeId,
                fingerprint: String(format: "%064x", index + 1),
                transcriptCoverage: Double(90 + index), duration: Double(1_700 + index * 100)
            )
            try await insertSession(
                store: store, id: "sess-batch-\(index)", assetId: placeholderId,
                state: SessionState.completeTranscriptPartial.rawValue
            )
            try await insertChunk(
                store: store, id: "chunk-batch-\(index)", assetId: placeholderId,
                index: 0, start: 0, end: Double(600 + index)
            )
        }

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.episodesInspected == 13)
        #expect(summary.placeholdersMerged == 13)
        #expect(Set(summary.survivingAssetIds) == Set(canonicalIds))

        for index in 0..<13 {
            let rows = try await allAssets(store: store, episodeId: "ep-batch-\(index)")
            #expect(rows.count == 1)
            let survivor = try #require(rows.first)
            #expect(survivor.fastTranscriptCoverageEndTime == Double(600 + index))
            let chunks = try await store.fetchTranscriptChunks(assetId: survivor.id)
            #expect(chunks.count == 1, "the placeholder's transcript must have moved, not cascaded")
        }
    }

    // MARK: - The 14th pair, and atomicity

    @Test("container-path drift alone does not block the merge (13-of-14 shape)")
    func containerPathDriftStillMerges() {
        let victim = AssetMergeRow(
            rowId: 1, id: "ph", assetFingerprint: "ph", createdAt: 1,
            analysisState: "completeFull", terminalReason: nil,
            sourceURL: "file:///var/mobile/Containers/Data/Application/AAAA/Library/Caches/partials/9f2c.mp3"
        )
        let survivor = AssetMergeRow(
            rowId: 2, id: "sha", assetFingerprint: String(repeating: "e", count: 64), createdAt: 2,
            analysisState: "queued", terminalReason: nil,
            sourceURL: "file:///var/mobile/Containers/Data/Application/BBBB/Library/Caches/complete/9F2C.MP3"
        )
        #expect(AnalysisStore.mergeGuard(victim: victim, survivor: survivor) == .merge,
                "playhead-9x4c rewrites everything left of the hashed filename; that is not an identity change")
    }

    @Test("the 14th pair — different artifacts — is SKIPPED with a recorded reason, never merged")
    func differingSourceArtifactIsSkipped() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-14th"
        try await insertPlaceholder(
            store: store, id: "ph-14th", episodeId: episodeId,
            state: SessionState.completeFull.rawValue,
            sourceURL: "file:///container-A/complete/aaaa1111.mp3"
        )
        try await insertCanonical(
            store: store, id: "canon-14th", episodeId: episodeId, fingerprint: canonicalSHA,
            sourceURL: "file:///container-A/complete/bbbb2222.mp3"
        )
        try await insertChunk(store: store, id: "chunk-14th", assetId: "ph-14th", index: 0, start: 0, end: 100)

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.episodesInspected == 1)
        #expect(summary.placeholdersMerged == 0, "two different artifacts must never be fused")
        #expect(summary.skippedDifferentSource == 1, "and the refusal must be counted, not silent")
        #expect(summary.survivingAssetIds.isEmpty)

        // Nothing moved, nothing deleted — the episode is exactly as it was.
        #expect(try await allAssets(store: store, episodeId: episodeId).count == 2)
        let chunks = try await store.fetchTranscriptChunks(assetId: "ph-14th")
        #expect(chunks.count == 1)
    }

    @Test("a row with no usable sourceURL is skipped rather than merged on faith")
    func unknownSourceIsSkipped() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-nosource"
        try await insertPlaceholder(
            store: store, id: "ph-nosource", episodeId: episodeId, sourceURL: ""
        )
        try await insertCanonical(
            store: store, id: "canon-nosource", episodeId: episodeId, fingerprint: canonicalSHA
        )

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.placeholdersMerged == 0)
        #expect(summary.skippedUnknownSource == 1)
        #expect(try await allAssets(store: store, episodeId: episodeId).count == 2)
    }

    @Test("an interruption mid-sweep rolls the WHOLE pass back — no half-merged episode")
    func interruptedSweepRollsBack() async throws {
        let (store, directory) = try await makeTestStoreWithDirectory()
        for index in 0..<3 {
            let episodeId = "ep-atomic-\(index)"
            try await insertPlaceholder(
                store: store, id: "ph-atomic-\(index)", episodeId: episodeId,
                state: SessionState.completeFull.rawValue, transcriptCoverage: 900
            )
            try await insertCanonical(
                store: store, id: "canon-atomic-\(index)", episodeId: episodeId,
                fingerprint: String(format: "%064x", index + 100), transcriptCoverage: 100
            )
            try await insertSession(
                store: store, id: "sess-atomic-\(index)", assetId: "ph-atomic-\(index)",
                state: SessionState.completeFull.rawValue
            )
            try await insertChunk(
                store: store, id: "chunk-atomic-\(index)", assetId: "ph-atomic-\(index)",
                index: 0, start: 0, end: 900
            )
        }
        let assetsBefore = try probeRowCount(in: directory, table: "analysis_assets")
        let chunksBefore = try probeRowCount(in: directory, table: "transcript_chunks")
        let sessionsBefore = try probeRowCount(in: directory, table: "analysis_sessions")

        // Die after the second placeholder — the shape of a jetsam during a
        // background launch sweep.
        await store.setDuplicateAssetMergeFaultInjectionForTesting(afterPlaceholders: 2)
        await #expect(throws: (any Error).self) {
            _ = try await store.reconcileDuplicatePlaceholderAssets()
        }
        await store.setDuplicateAssetMergeFaultInjectionForTesting(afterPlaceholders: nil)

        #expect(try probeRowCount(in: directory, table: "analysis_assets") == assetsBefore,
                "a rolled-back sweep must not have deleted any asset row")
        #expect(try probeRowCount(in: directory, table: "transcript_chunks") == chunksBefore)
        #expect(try probeRowCount(in: directory, table: "analysis_sessions") == sessionsBefore)
        for index in 0..<3 {
            #expect(try await allAssets(store: store, episodeId: "ep-atomic-\(index)").count == 2,
                    "episode \(index) must be exactly as it was before the interrupted pass")
            let moved = try await store.fetchSession(id: "sess-atomic-\(index)")
            #expect(moved?.analysisAssetId == "ph-atomic-\(index)",
                    "child rows must still point where they did before the pass")
        }

        // And the retry that a real relaunch would perform completes cleanly.
        let retry = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(retry.placeholdersMerged == 3)
        for index in 0..<3 {
            #expect(try await allAssets(store: store, episodeId: "ep-atomic-\(index)").count == 1)
        }
    }

    // MARK: - Part 4: the unique index

    @Test("a populated pre-migration database reaches v39 and gains the unique index")
    func migrationSucceedsOnPopulatedDatabase() async throws {
        // A store born at head is not a migration test. Build the v38 shape
        // with real rows in it, THEN migrate.
        let store = try await makeTestStore()
        try await insertPlaceholder(store: store, id: "pre-ph", episodeId: "ep-pre")
        try await insertCanonical(
            store: store, id: "pre-canon", episodeId: "ep-pre", fingerprint: canonicalSHA
        )
        try await store.execForTesting("DROP INDEX IF EXISTS idx_assets_episode_fingerprint")
        try await store.setMetaValue(forKey: "schema_version", value: "38")
        #expect(try await store.schemaVersion() == 38)

        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == 39)
        // The placeholder/SHA pair holds two DIFFERENT fingerprints, so the
        // index does not touch it — repairing that pair is the launch sweep's
        // job, and the migration must not have destroyed either row.
        #expect(try await allAssets(store: store, episodeId: "ep-pre").count == 2)
        // The index is live: an exact-identity duplicate is now rejected.
        await #expect(throws: (any Error).self) {
            try await self.insertCanonical(
                store: store, id: "post-migrate-dup", episodeId: "ep-pre",
                fingerprint: self.canonicalSHA
            )
        }
    }

    @Test("the unique index rejects a second row for the same (episodeId, assetFingerprint)")
    func uniqueIndexRejectsExactDuplicate() async throws {
        let store = try await makeTestStore()
        try await insertCanonical(
            store: store, id: "dup-1", episodeId: "ep-dup", fingerprint: canonicalSHA
        )
        await #expect(throws: (any Error).self) {
            try await self.insertCanonical(
                store: store, id: "dup-2", episodeId: "ep-dup", fingerprint: self.canonicalSHA
            )
        }
    }

    @Test("the v39 migration deduplicates exact-fingerprint collisions before creating the index")
    func migrationDeduplicatesExactCollisions() async throws {
        let store = try await makeTestStore()
        // Get behind the index, plant a violation, then migrate forward.
        try await store.execForTesting("DROP INDEX IF EXISTS idx_assets_episode_fingerprint")
        try await store.setMetaValue(forKey: "schema_version", value: "38")
        try await insertCanonical(
            store: store, id: "collide-old", episodeId: "ep-collide", fingerprint: canonicalSHA
        )
        try await insertCanonical(
            store: store, id: "collide-new", episodeId: "ep-collide", fingerprint: canonicalSHA
        )
        #expect(try await allAssets(store: store, episodeId: "ep-collide").count == 2)

        try await store.migrateOnlyForTesting()

        let rows = try await allAssets(store: store, episodeId: "ep-collide")
        #expect(rows.count == 1, "the migration must clear its own violations before CREATE UNIQUE INDEX")
        #expect(rows.first?.id == "collide-new", "newest-wins on an exact-identity collision")
        #expect(try await store.schemaVersion() == 39)
    }

    // MARK: - R1 findings

    /// R1 finding 1. The v39 dedup used to be a bare
    /// `DELETE FROM analysis_assets`, which is two defects at once:
    ///
    ///   * `training_examples.analysisAssetId` is `ON DELETE RESTRICT`, so the
    ///     DELETE raises `FOREIGN KEY constraint failed`, which aborts the
    ///     whole migration transaction. A failed `analysisStore.migrate()` at
    ///     launch is not a crash — `PlayheadRuntime` responds by
    ///     `removeItem(at: AnalysisStore.defaultDirectory())` and retrying, so
    ///     the user's ENTIRE analysis database is silently deleted.
    ///   * every other child table is `ON DELETE CASCADE`, so the loser's
    ///     transcript chunks, windows and scan rows were destroyed rather than
    ///     re-pointed — the opposite of what the launch sweep does two steps
    ///     later for the placeholder/SHA shape.
    ///
    /// Both are fixed by re-pointing children onto the winner first, reusing
    /// the same runtime schema discovery the sweep uses.
    @Test("v39 dedup re-points the loser's children and survives ON DELETE RESTRICT")
    func migrationDedupPreservesChildRows() async throws {
        let store = try await makeTestStore()
        try await store.execForTesting("DROP INDEX IF EXISTS idx_assets_episode_fingerprint")
        try await store.setMetaValue(forKey: "schema_version", value: "38")
        try await insertCanonical(
            store: store, id: "collide-loser", episodeId: "ep-children", fingerprint: canonicalSHA
        )
        try await insertCanonical(
            store: store, id: "collide-winner", episodeId: "ep-children", fingerprint: canonicalSHA
        )
        try await insertChunk(
            store: store, id: "chunk-loser", assetId: "collide-loser", index: 0, start: 0, end: 100
        )
        try await store.createTrainingExample(TrainingExample(
            id: "te-loser",
            analysisAssetId: "collide-loser",
            startAtomOrdinal: 1,
            endAtomOrdinal: 2,
            transcriptVersion: "tv-1",
            startTime: 10,
            endTime: 20,
            textSnapshotHash: "h-loser",
            textSnapshot: nil,
            bucket: .positive,
            commercialIntent: "paid",
            ownership: "thirdParty",
            evidenceSources: ["fm"],
            fmCertainty: 0.9,
            classifierConfidence: 0.7,
            userAction: nil,
            eligibilityGate: nil,
            scanCohortJSON: "{}",
            decisionCohortJSON: nil,
            transcriptQuality: "good",
            createdAt: 1_700_000_000,
            privacyClassification: .onDeviceLocal
        ))

        // The migration must COMPLETE. Before the fix this threw
        // `FOREIGN KEY constraint failed`, which at launch means the analysis
        // directory is deleted and re-created empty.
        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == 39)
        let rows = try await allAssets(store: store, episodeId: "ep-children")
        #expect(rows.count == 1)
        #expect(rows.first?.id == "collide-winner", "newest-wins on an exact-identity collision")
        let chunks = try await store.fetchTranscriptChunks(assetId: "collide-winner")
        #expect(chunks.count == 1,
                "the loser's transcript must be re-pointed onto the winner, not cascaded away")
        #expect(try await store.loadTrainingExamples(forAsset: "collide-winner").count == 1,
                "the RESTRICT child must follow the merge too")
        #expect(try await store.loadTrainingExamples(forAsset: "collide-loser").isEmpty)
    }

    /// R1 finding 3. `episodeIdsWithMultipleAssets` groups by `episodeId`, so
    /// every row in a group shares an episode identity and the basename guard
    /// is meaningful — EXCEPT for the empty string, which groups every
    /// blank-id row together. `DownloadManager.safeFilename(for:)` is
    /// `SHA256(episodeId)`, so two rows born of two DIFFERENT episodes but
    /// written with a blank id land on the SAME basename and the guard votes
    /// `.merge`. That is precisely the irreversible false merge
    /// ``AssetMergeGuard`` exists to prevent, so a blank group key is not a
    /// merge key.
    @Test("rows grouped under a blank episodeId are never merged")
    func blankEpisodeIdIsNeverMerged() async throws {
        let store = try await makeTestStore()
        // Same basename on both — `safeFilename("")` is a constant, so this is
        // exactly what two different episodes would produce.
        try await insertPlaceholder(
            store: store, id: "ph-blank", episodeId: "",
            sourceURL: "file:///container-A/e3b0c442.mp3"
        )
        try await insertCanonical(
            store: store, id: "canon-blank", episodeId: "", fingerprint: canonicalSHA,
            sourceURL: "file:///container-B/e3b0c442.mp3"
        )

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.placeholdersMerged == 0,
                "a blank episodeId is not an episode identity — merging on it fuses unrelated episodes")
        #expect(try await allAssets(store: store, episodeId: "").count == 2)
    }

    /// The duration fold is a PREFERENCE, not a max — that is the whole point
    /// of the comment on `foldAssetRow`, and nothing pinned it: every other
    /// fixture happens to give the survivor the larger value, so `MAX(...)`
    /// would pass them all. Here the placeholder's poisoned duration is the
    /// bigger number, which is exactly the case a max gets wrong.
    @Test("a LARGER placeholder duration still loses to the survivor's own")
    func largerPlaceholderDurationDoesNotWin() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-bigger-poison"
        try await insertPlaceholder(
            store: store, id: "ph-bigger", episodeId: episodeId,
            state: SessionState.completeFull.rawValue, duration: 5_000
        )
        try await insertCanonical(
            store: store, id: "canon-bigger", episodeId: episodeId,
            fingerprint: canonicalSHA, duration: 2_933
        )

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.placeholdersMerged == 1)
        #expect(summary.survivorsWithInheritedDuration.isEmpty)
        let survivor = try #require(try await store.fetchAsset(id: "canon-bigger"))
        #expect(survivor.episodeDurationSec == 2_933,
                "a MAX would hand the merged row the placeholder's artefact whenever it happens to be larger")
    }

    /// R1: the `createdAt` guard in the v39 dedup is what stops the migration
    /// from aborting on a database that predates the column — and an aborted
    /// migration at launch means `PlayheadRuntime` deletes the analysis
    /// directory. The guard only matters when such a database ALSO has a
    /// collision to resolve, which no fixture had.
    @Test("v39 dedup resolves a collision on a schema with no createdAt column")
    func migrationDedupWithoutCreatedAtColumn() async throws {
        let (store, directory) = try await makeTestStoreWithDirectory()
        try await store.execForTesting("DROP INDEX IF EXISTS idx_assets_episode_fingerprint")
        try await insertCanonical(
            store: store, id: "nocreated-old", episodeId: "ep-nocreated", fingerprint: canonicalSHA
        )
        try await insertCanonical(
            store: store, id: "nocreated-new", episodeId: "ep-nocreated", fingerprint: canonicalSHA
        )
        // Model the pre-v9 shape: the ordering column simply is not there.
        try await store.execForTesting("ALTER TABLE analysis_assets DROP COLUMN createdAt")
        try await store.setMetaValue(forKey: "schema_version", value: "38")

        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == 39)
        #expect(try probeRowCount(in: directory, table: "analysis_assets") == 1)
        #expect(try probeRowCount(
            in: directory,
            table: "analysis_assets WHERE id = 'nocreated-new'"
        ) == 1, "rowid DESC alone is a correct, if coarser, newest-wins")
    }

    /// R1 finding 2, at the store seam: the merge has to SAY when a survivor's
    /// duration is only there because the placeholder had one, because after
    /// the `COALESCE` the two cases are indistinguishable from the row alone.
    @Test("the merge reports survivors whose duration came from the placeholder")
    func inheritedDurationIsReported() async throws {
        let store = try await makeTestStore()
        // Survivor with no duration of its own — inherits, must be flagged.
        try await insertPlaceholder(
            store: store, id: "ph-inh", episodeId: "ep-inh", duration: 543
        )
        try await insertCanonical(
            store: store, id: "canon-inh", episodeId: "ep-inh",
            fingerprint: canonicalSHA, duration: nil
        )
        // Survivor that brought its own — nothing inherited, must NOT be flagged.
        try await insertPlaceholder(
            store: store, id: "ph-not", episodeId: "ep-not", duration: 543
        )
        try await insertCanonical(
            store: store, id: "canon-not", episodeId: "ep-not",
            fingerprint: String(repeating: "f", count: 64), duration: 2_933
        )

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.placeholdersMerged == 2)
        #expect(summary.survivorsWithInheritedDuration == ["canon-inh"],
                "only the survivor that had nothing of its own inherited a duration")
        let inherited = try #require(try await store.fetchAsset(id: "canon-inh"))
        #expect(inherited.episodeDurationSec == 543,
                "the value is still carried across — it is VERIFIED afterwards, not discarded")
    }

    // MARK: - R1 finding 2: an inherited duration is never trusted

    private func writeSynthAudio(seconds: TimeInterval) throws -> URL {
        let dir = try makeTempDir(prefix: "Bd0hi9Merge")
        Self.tempDirs.track(dir)
        let fileURL = dir.appendingPathComponent("synth-\(UUID().uuidString).caf")
        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: 44_100,
            channels: 1,
            interleaved: false
        ) else {
            throw NSError(domain: "DuplicateAssetReconcileTests", code: -1)
        }
        let file = try AVAudioFile(
            forWriting: fileURL,
            settings: format.settings,
            commonFormat: .pcmFormatFloat32,
            interleaved: false
        )
        let totalFrames = AVAudioFramePosition(seconds * 44_100)
        var written = AVAudioFramePosition(0)
        while written < totalFrames {
            let frames = AVAudioFrameCount(min(AVAudioFramePosition(44_100), totalFrames - written))
            guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frames) else {
                throw NSError(domain: "DuplicateAssetReconcileTests", code: -2)
            }
            buffer.frameLength = frames
            try file.write(from: buffer)
            written += AVAudioFramePosition(frames)
        }
        return fileURL
    }

    private func makeCoordinator(store: AnalysisStore) -> AnalysisCoordinator {
        AnalysisCoordinator(
            store: store,
            audioService: AnalysisAudioService(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: SpeechService(recognizer: StubSpeechRecognizer()),
                store: store
            ),
            capabilitiesService: CapabilitiesService(),
            adDetectionService: AdDetectionService(
                store: store,
                metadataExtractor: FallbackExtractor(),
                backfillJobRunnerFactory: nil,
                canUseFoundationModelsProvider: { false }
            ),
            skipOrchestrator: SkipOrchestrator(store: store)
        )
    }

    /// R1 finding 2. `foldAssetRow` fills the survivor's NULL
    /// `episodeDurationSec` from the placeholder — and the placeholder's
    /// duration is the one this whole bead declares untrustworthy (the shard
    /// sum of an 8 MiB mid-download prefix). The repair step then decides
    /// whether to re-probe with `duration <= 0 || watermark > duration`, so a
    /// poisoned 543 s that happens to EXCEED the merged coverage looks
    /// plausible and is kept forever — the sweep is one-shot.
    ///
    /// An inherited duration must always be verified against the audio file.
    @Test("a duration inherited from the placeholder is always re-probed, even when it looks plausible")
    func inheritedDurationIsAlwaysReProbed() async throws {
        let store = try await makeTestStore()
        let coordinator = makeCoordinator(store: store)
        let episodeId = "ep-inherit"
        let audioURL = try writeSynthAudio(seconds: 30)

        // Placeholder: the poisoned mid-download prefix, plus a transcript
        // watermark BELOW it so the existing contradiction check stays quiet.
        try await insertPlaceholder(
            store: store, id: "ph-inherit", episodeId: episodeId,
            state: SessionState.completeTranscriptPartial.rawValue,
            transcriptCoverage: 300, duration: 543
        )
        // Survivor: no duration of its own, so the fold hands it the 543.
        try await insertCanonical(
            store: store, id: "canon-inherit", episodeId: episodeId,
            fingerprint: canonicalSHA, transcriptCoverage: 100, duration: nil
        )

        let summary = await coordinator.reconcileDuplicateAnalysisAssetsIfNeeded(
            cachedFileURL: { _ in audioURL }
        )
        #expect(summary.merge.placeholdersMerged == 1)

        let survivor = try #require(try await store.fetchAsset(id: "canon-inherit"))
        let duration = try #require(survivor.episodeDurationSec)
        #expect(abs(duration - 30) < 1.0,
                "the merged row must carry a MEASURED duration, not the placeholder's 543 s prefix (got \(duration))")
    }

    /// R1: the sweep is ONE-SHOT, gated on a `_meta` key. If a failed pass
    /// wrote that key the user's library would stay split forever — which is
    /// precisely how the two existing sweeps (`did_duration_backfill_v1`,
    /// `did_terminal_state_reconcile_v1`) came to be useless here. Nothing
    /// covered the marker at all: not the failure case, not the short-circuit.
    @Test("a failed merge leaves the one-shot marker unset; the retry completes and then short-circuits")
    func failedPassDoesNotMarkItselfDone() async throws {
        let store = try await makeTestStore()
        let coordinator = makeCoordinator(store: store)
        let metaKey = AnalysisCoordinator.duplicateAssetReconcileV1MetaKey
        try await insertPlaceholder(
            store: store, id: "ph-mark", episodeId: "ep-mark",
            state: SessionState.completeFull.rawValue
        )
        try await insertCanonical(
            store: store, id: "canon-mark", episodeId: "ep-mark", fingerprint: canonicalSHA
        )

        await store.setDuplicateAssetMergeFaultInjectionForTesting(afterPlaceholders: 1)
        let failed = await coordinator.reconcileDuplicateAnalysisAssetsIfNeeded(
            cachedFileURL: { _ in nil }
        )
        await store.setDuplicateAssetMergeFaultInjectionForTesting(afterPlaceholders: nil)

        #expect(failed.failed)
        #expect(try await store.fetchMetaValue(forKey: metaKey) == nil,
                "marking a rolled-back pass done would strand the split library forever")
        #expect(try await allAssets(store: store, episodeId: "ep-mark").count == 2)

        // The retry a real relaunch performs.
        let retry = await coordinator.reconcileDuplicateAnalysisAssetsIfNeeded(
            cachedFileURL: { _ in nil }
        )
        #expect(retry.merge.placeholdersMerged == 1)
        #expect(!retry.alreadyDone)
        #expect(try await store.fetchMetaValue(forKey: metaKey) == "1")

        // And every launch after that is free.
        let third = await coordinator.reconcileDuplicateAnalysisAssetsIfNeeded(
            cachedFileURL: { _ in nil }
        )
        #expect(third.alreadyDone)
        #expect(third.merge == DuplicateAssetMergeSummary())
    }

    /// The counterpart: a survivor that brought its own duration is left
    /// alone. Without this the fix could degenerate into "always re-probe",
    /// which would let a partial file on disk overwrite a good value.
    @Test("a survivor's own duration is not re-probed when nothing contradicts it")
    func survivorOwnDurationIsLeftAlone() async throws {
        let store = try await makeTestStore()
        let coordinator = makeCoordinator(store: store)
        let episodeId = "ep-own-duration"
        let audioURL = try writeSynthAudio(seconds: 30)

        try await insertPlaceholder(
            store: store, id: "ph-own", episodeId: episodeId,
            state: SessionState.completeTranscriptPartial.rawValue,
            transcriptCoverage: 300, duration: 543
        )
        try await insertCanonical(
            store: store, id: "canon-own", episodeId: episodeId,
            fingerprint: canonicalSHA, transcriptCoverage: 100, duration: 2_933
        )

        let summary = await coordinator.reconcileDuplicateAnalysisAssetsIfNeeded(
            cachedFileURL: { _ in audioURL }
        )
        #expect(summary.merge.placeholdersMerged == 1)
        #expect(summary.durationsProbed == 0, "nothing contradicted the survivor's own duration")

        let survivor = try #require(try await store.fetchAsset(id: "canon-own"))
        #expect(survivor.episodeDurationSec == 2_933)
    }
}
