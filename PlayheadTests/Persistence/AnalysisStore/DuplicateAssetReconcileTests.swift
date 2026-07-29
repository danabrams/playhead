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

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-0hi9 — duplicate analysis_assets reconciliation", .serialized)
struct DuplicateAssetReconcileTests {

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
}
