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
        sourceURL: String? = nil,
        weakFingerprint: String? = nil
    ) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: id,
            episodeId: episodeId,
            assetFingerprint: id, // the Pipeline A signature: self-reference
            weakFingerprint: weakFingerprint,
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
            title: "Episode 12",
            weakFingerprint: "https://cdn.example.com/ep12.mp3|\"etag-12\"|8388608|"
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
        // Including the weak identity — the merged row has to stay recognisable
        // to `canUpgradeWeakAssetToCanonicalSHA` and
        // `fetchAssetsByEpisodeId(_:weakFingerprint:)`, which is the machinery
        // part 2 exists to make reachable. Dropping it here would quietly undo
        // part 2 for exactly the rows this sweep touched.
        #expect(survivor.weakFingerprint == "https://cdn.example.com/ep12.mp3|\"etag-12\"|8388608|")

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

    // MARK: - R2 findings

    /// R2 finding 1. `assetReferencingChildColumns` discovered child rows by
    /// COLUMN NAME (`analysisAssetId` / `assetId`). Every reference in today's
    /// schema happens to use one of those two spellings, so the rule looked
    /// complete — but nothing enforces it, and the failure mode of getting it
    /// wrong is not a missed re-point:
    ///
    ///   * under `ON DELETE CASCADE` the undiscovered rows are DESTROYED by
    ///     `deleteAssetRow` instead of moved;
    ///   * under `ON DELETE RESTRICT` `deleteAssetRow` raises
    ///     `FOREIGN KEY constraint failed`, which inside the V39 migration
    ///     aborts `migrate()` — and `PlayheadRuntime` answers a thrown
    ///     `migrate()` by deleting `AnalysisStore.defaultDirectory()` and
    ///     retrying, which succeeds on an empty directory.
    ///
    /// So discovery now also asks the schema itself, via
    /// `PRAGMA foreign_key_list`. This test uses the RESTRICT shape because it
    /// is the one that reaches the data-loss path.
    @Test("a foreign key into analysis_assets is followed whatever the column is called")
    func foreignKeyUnderAnyColumnNameIsFollowed() async throws {
        let (store, directory) = try await makeTestStoreWithDirectory()
        // A column name neither discovery-by-name spelling would ever match.
        try await store.execForTesting("""
            CREATE TABLE rogue_asset_children (
                id TEXT PRIMARY KEY,
                ownerAssetId TEXT NOT NULL
                    REFERENCES analysis_assets(id) ON DELETE RESTRICT
            )
            """)

        let columns = try await store.assetReferencingChildColumns()
        #expect(
            columns.contains(
                ChildAssetColumn(table: "rogue_asset_children", column: "ownerAssetId")
            ),
            "the schema is the authority on what references analysis_assets — not a naming convention"
        )

        // And the discovery has to be load-bearing, not just reported: plant a
        // v39-visible collision whose loser owns one of these rows and migrate.
        try await store.execForTesting("DROP INDEX IF EXISTS idx_assets_episode_fingerprint")
        try await insertCanonical(
            store: store, id: "rogue-loser", episodeId: "ep-rogue", fingerprint: canonicalSHA
        )
        try await insertCanonical(
            store: store, id: "rogue-winner", episodeId: "ep-rogue", fingerprint: canonicalSHA
        )
        try await store.execForTesting(
            "INSERT INTO rogue_asset_children VALUES ('rogue-child', 'rogue-loser')"
        )
        try await store.setMetaValue(forKey: "schema_version", value: "38")

        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == 39,
                "the migration must COMPLETE — a throw here is answered at launch by deleting the store")
        #expect(try probeRowCount(
            in: directory,
            table: "analysis_assets WHERE episodeId = 'ep-rogue'"
        ) == 1)
        #expect(try probeRowCount(
            in: directory,
            table: "rogue_asset_children WHERE ownerAssetId = 'rogue-winner'"
        ) == 1, "the row must have been MOVED to the winner, not cascaded away or left dangling")
    }

    /// R2 finding 2. Containment. Every throw site the V39 rung can reach —
    /// a constraint it did not clear, a delete blocked by something the
    /// re-point could not follow, a corrupt index, a future trigger — used to
    /// take `migrate()` down with it, and a thrown `migrate()` at launch is
    /// answered by `PlayheadRuntime` with
    /// `removeItem(at: AnalysisStore.defaultDirectory())` + retry, which
    /// succeeds on an empty directory. Silent, total loss of the user's
    /// analysis database, to protect an INDEX.
    ///
    /// The rung now runs in a SAVEPOINT: on any failure it rolls back, logs a
    /// fault, and leaves `schema_version` at 38 so the next launch retries.
    /// The degraded state is exactly what `main` ships today.
    ///
    /// The fixture is a `BEFORE DELETE` trigger rather than any one specific
    /// hazard, precisely because the point is to be indifferent to WHICH throw
    /// site fires.
    @Test("a V39 failure is contained: migrate() still succeeds and the database is untouched")
    func v39FailureIsContainedRatherThanAbortingMigrate() async throws {
        let (store, directory) = try await makeTestStoreWithDirectory()
        try await store.execForTesting("DROP INDEX IF EXISTS idx_assets_episode_fingerprint")
        try await insertCanonical(
            store: store, id: "contained-loser", episodeId: "ep-contained", fingerprint: canonicalSHA
        )
        try await insertCanonical(
            store: store, id: "contained-winner", episodeId: "ep-contained", fingerprint: canonicalSHA
        )
        try await insertChunk(
            store: store, id: "chunk-contained", assetId: "contained-loser",
            index: 0, start: 0, end: 100
        )
        try await store.execForTesting("""
            CREATE TRIGGER bd0hi9_v39_guard BEFORE DELETE ON analysis_assets
            BEGIN SELECT RAISE(ABORT, 'v39 containment fixture'); END
            """)
        try await store.setMetaValue(forKey: "schema_version", value: "38")

        // MUST NOT THROW.
        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == 38,
                "an unfinished V39 must stay retryable, not mark itself done")
        #expect(try probeRowCount(in: directory, table: "analysis_assets") == 2,
                "the database must be exactly as it was — nothing deleted")
        let strandedChunks = try await store.fetchTranscriptChunks(assetId: "contained-loser")
        #expect(strandedChunks.count == 1,
                "the half-finished re-point must roll back, not leave children moved onto a row that still has a sibling")
        // R3: the SCHEMA has to roll back with the data. The two halves of
        // "unchanged" are separable — DDL is transactional in SQLite, but only
        // because the index is built inside the savepoint, and building it
        // outside would leave a live UNIQUE constraint on a database whose
        // `schema_version` says 38 and whose duplicates are still there. Every
        // later insert for those episodes would then fail against a rail
        // nothing records as installed.
        #expect(try probeRowCount(
            in: directory,
            table: "sqlite_master WHERE type = 'index' AND name = 'idx_assets_episode_fingerprint'"
        ) == 0, "a rolled-back rung must leave no index behind")

        // The retry a later launch performs, once whatever blocked it is gone.
        try await store.execForTesting("DROP TRIGGER bd0hi9_v39_guard")
        try await store.migrateOnlyForTesting()
        #expect(try await store.schemaVersion() == 39)
        #expect(try probeRowCount(in: directory, table: "analysis_assets") == 1)
        #expect(try await store.fetchTranscriptChunks(assetId: "contained-winner").count == 1)
        #expect(try probeRowCount(
            in: directory,
            table: "sqlite_master WHERE type = 'index' AND name = 'idx_assets_episode_fingerprint'"
        ) == 1, "and the retry that completes must install it")
    }

    /// R2 finding 3. `episodeIdsWithMultipleAssets` filters
    /// `TRIM(episodeId) != ''`, and the `TRIM` is the whole guard for anything
    /// that is blank without being empty. Nothing pinned it: the R1 test uses
    /// `""`, which `episodeId != ''` alone already rejects. A whitespace-only
    /// id is the same defect — `DownloadManager.safeFilename(for:)` is
    /// `SHA256(episodeId)`, so every row written with that id lands on one
    /// basename and ``AssetMergeGuard`` votes `.merge` on rows from unrelated
    /// episodes.
    @Test("rows grouped under a whitespace-only episodeId are never merged")
    func whitespaceOnlyEpisodeIdIsNeverMerged() async throws {
        let store = try await makeTestStore()
        let blankish = "   "
        try await insertPlaceholder(
            store: store, id: "ph-ws", episodeId: blankish,
            sourceURL: "file:///container-A/e3b0c442.mp3"
        )
        try await insertCanonical(
            store: store, id: "canon-ws", episodeId: blankish, fingerprint: canonicalSHA,
            sourceURL: "file:///container-B/e3b0c442.mp3"
        )

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.placeholdersMerged == 0,
                "whitespace is not an episode identity any more than the empty string is")
        #expect(summary.episodesInspected == 0)
        #expect(try await allAssets(store: store, episodeId: blankish).count == 2)
    }

    /// R2 finding 4. ``AssetMergeRow/artifactBasename`` trims before testing
    /// for empty, and that trim is the only thing standing between a
    /// whitespace-only `sourceURL` and a `.merge` verdict: `URL(string: "   ")`
    /// is NOT nil, so the value survives to become a basename of spaces, and
    /// two such rows compare equal. Nothing covered it — the existing unknown
    /// -source test uses `""`, which the `isEmpty` check rejects on its own.
    /// The `"/"` rejection is pinned for the same reason.
    @Test("a blank-but-not-empty sourceURL is unknown, never a basename")
    func blankSourceURLIsUnknownRatherThanMerged() {
        func row(_ id: String, _ sourceURL: String) -> AssetMergeRow {
            AssetMergeRow(
                rowId: 1, id: id, assetFingerprint: id, createdAt: 1,
                analysisState: "queued", terminalReason: nil, sourceURL: sourceURL
            )
        }
        #expect(row("a", "   ").artifactBasename == nil)
        #expect(row("a", "\t\n").artifactBasename == nil)
        #expect(row("a", "/").artifactBasename == nil)
        #expect(row("a", "//").artifactBasename == nil)

        #expect(
            AnalysisStore.mergeGuard(victim: row("ph", "   "), survivor: row("sha", "\t")) == .skipUnknownSource,
            "two rows whose sourceURL is only whitespace name NOTHING in common — merging them is the irreversible false merge"
        )
        #expect(
            AnalysisStore.mergeGuard(victim: row("ph", "/"), survivor: row("sha", "/")) == .skipUnknownSource
        )
        // And the guard still says yes to the shape it exists to allow.
        #expect(
            AnalysisStore.mergeGuard(
                victim: row("ph", "file:///A/9f2c.mp3"),
                survivor: row("sha", "file:///B/9f2c.mp3")
            ) == .merge
        )
    }

    /// R2 finding 5, found by a SURVIVING mutation: `foldAssetRow`'s adoption
    /// guard has two halves — the victim must be a completion terminal AND the
    /// survivor must not already be one. Only the first half was covered
    /// (`terminalStateIsAdopted` uses a `queued` survivor,
    /// `midPipelineStateIsNotAdopted` mutates the victim). Dropping
    /// `!survivorState.isTerminalCompletion` survived the whole suite.
    ///
    /// It is not a harmless mutant. Both `completeFull` and
    /// `completeTranscriptPartial` are terminal completions, so a survivor that
    /// genuinely finished would have its verdict — and its `terminalReason`,
    /// which carries the coverage numbers — REPLACED by the placeholder's.
    /// The placeholder's terminal claim was scored against the poisoned ~543 s
    /// denominator this whole bead exists to remove, and
    /// `reconcilePersistedTerminalAssetVerdict` only repairs claims that
    /// coverage CONTRADICTS, so a downgrade sticks.
    @Test("a survivor that already completed keeps its own terminal claim")
    func survivorOwnTerminalClaimIsNotReplaced() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-both-terminal"
        try await insertPlaceholder(
            store: store, id: "ph-both", episodeId: episodeId,
            state: SessionState.completeTranscriptPartial.rawValue,
            transcriptCoverage: 600, duration: 543
        )
        try await insertCanonical(
            store: store, id: "canon-both", episodeId: episodeId,
            fingerprint: canonicalSHA, state: SessionState.completeFull.rawValue,
            featureCoverage: 2_900, transcriptCoverage: 2_900, duration: 2_933
        )
        try await store.execForTesting("""
            UPDATE analysis_assets SET terminalReason = 'survivor-own-reason'
            WHERE id = 'canon-both'
            """)
        try await store.execForTesting("""
            UPDATE analysis_assets SET terminalReason = 'placeholder-543s-reason'
            WHERE id = 'ph-both'
            """)

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.placeholdersMerged == 1)
        #expect(summary.adoptedTerminalStates == 0,
                "the survivor's own completion is not the placeholder's to overwrite")

        let survivor = try #require(try await store.fetchAsset(id: "canon-both"))
        #expect(survivor.analysisState == SessionState.completeFull.rawValue,
                "a completeFull survivor must not be downgraded to the placeholder's partial verdict")
        #expect(survivor.terminalReason == "survivor-own-reason",
                "the reason carries the coverage numbers the verdict was scored against")
    }

    /// R2 finding 6, found by a SURVIVING mutation: `repairMergedAsset` decides
    /// to re-probe on `forceDurationReprobe || duration <= 0 || watermarkEnd >
    /// duration`, and only the first two arms were covered. Deleting the
    /// `watermarkEnd > duration` arm survived the whole suite.
    ///
    /// That arm is the one the DEVICE data actually needs. The observed shape
    /// is a placeholder holding 630–1746 s of transcript next to a survivor
    /// holding a 528–561 s duration of its OWN — so nothing is inherited, the
    /// force-reprobe arm never fires, and `duration <= 0` is false. The merge
    /// is what exposes it: raising the survivor's watermark to the max of the
    /// pair puts 1746 s of proven coverage on a row claiming to be 560 s long.
    /// Without this arm that contradiction is written and then kept forever,
    /// because the sweep is one-shot.
    @Test("a duration the MERGED coverage contradicts is re-probed even though the survivor brought it")
    func mergedCoverageContradictingDurationIsReProbed() async throws {
        let store = try await makeTestStore()
        let coordinator = makeCoordinator(store: store)
        let episodeId = "ep-contradicted"
        let audioURL = try writeSynthAudio(seconds: 30)

        try await insertPlaceholder(
            store: store, id: "ph-contradicted", episodeId: episodeId,
            state: SessionState.completeTranscriptPartial.rawValue,
            transcriptCoverage: 1_746, duration: 543
        )
        try await insertCanonical(
            store: store, id: "canon-contradicted", episodeId: episodeId,
            fingerprint: canonicalSHA, transcriptCoverage: 90, duration: 560
        )

        let summary = await coordinator.reconcileDuplicateAnalysisAssetsIfNeeded(
            cachedFileURL: { _ in audioURL }
        )
        #expect(summary.merge.placeholdersMerged == 1)
        #expect(summary.merge.survivorsWithInheritedDuration.isEmpty,
                "the survivor brought its own value — the force-reprobe arm is deliberately not what is under test")
        #expect(summary.durationsProbed == 1,
                "a merged watermark above the row's own duration is a contradiction only the audio can settle")

        let survivor = try #require(try await store.fetchAsset(id: "canon-contradicted"))
        let duration = try #require(survivor.episodeDurationSec)
        #expect(abs(duration - 30) < 1.0,
                "the merged row must carry a MEASURED duration, not the 560 s the merge just contradicted (got \(duration))")
    }

    /// R2 finding 7, found by a SURVIVING mutation. Step 3 of the sweep
    /// re-scores the merged row's terminal claim, and it deliberately re-reads
    /// the row first — the whole point is to score against the duration step 2
    /// just probed. Replacing that re-read with the pre-probe snapshot survived
    /// every test, because no fixture made the two verdicts differ.
    ///
    /// They differ in exactly the shape this sweep creates. Here the survivor
    /// inherits the placeholder's 5000 s artefact, adopts its `completeFull`,
    /// and is then measured at 30 s. Against the REAL duration the claim holds
    /// (29.5 s of coverage over 30 s = 0.98, clear of
    /// `finalizeBackfillMinCoverageRatio`); against the stale 5000 it reads as
    /// 0.6 % coverage and the row is wrongly torn down. The sweep is one-shot,
    /// so that mistaken repair is permanent.
    @Test("the terminal re-score runs against the RE-PROBED duration, not the pre-probe row")
    func terminalReScoreUsesTheReProbedDuration() async throws {
        let store = try await makeTestStore()
        let coordinator = makeCoordinator(store: store)
        let episodeId = "ep-rescore"
        let audioURL = try writeSynthAudio(seconds: 30)

        try await insertPlaceholder(
            store: store, id: "ph-rescore", episodeId: episodeId,
            state: SessionState.completeFull.rawValue,
            featureCoverage: 29.5, transcriptCoverage: 29.5, duration: 5_000
        )
        try await insertCanonical(
            store: store, id: "canon-rescore", episodeId: episodeId,
            fingerprint: canonicalSHA, state: SessionState.queued.rawValue,
            featureCoverage: nil, transcriptCoverage: nil, duration: nil
        )
        try await insertChunk(
            store: store, id: "chunk-rescore", assetId: "ph-rescore",
            index: 0, start: 0, end: 29.5
        )

        let summary = await coordinator.reconcileDuplicateAnalysisAssetsIfNeeded(
            cachedFileURL: { _ in audioURL }
        )
        #expect(summary.merge.placeholdersMerged == 1)
        #expect(summary.merge.adoptedTerminalStates == 1)
        #expect(summary.durationsRewritten == 1)
        #expect(summary.terminalStatesRepaired == 0,
                "29.5 s of coverage over a MEASURED 30 s episode is a claim that holds; only the stale 5000 s makes it look false")

        let survivor = try #require(try await store.fetchAsset(id: "canon-rescore"))
        #expect(survivor.analysisState == SessionState.completeFull.rawValue,
                "a verdict scored against the pre-probe duration tears down a row that is actually complete")
        let duration = try #require(survivor.episodeDurationSec)
        #expect(abs(duration - 30) < 1.0, "got \(duration)")
    }

    // MARK: - R3: more than two rows, and rows that are all placeholders

    /// R3 finding 1. Every fixture in this suite before now was a PAIR, and the
    /// adoption guard reads a survivor snapshot taken before the first fold —
    /// so with two placeholders each carrying a completion terminal, the second
    /// fold sees a survivor that is still `queued` in the snapshot and
    /// overwrites the terminal the first fold just adopted.
    ///
    /// That is precisely the rule R2 finding 5 established
    /// (``survivorOwnTerminalClaimIsNotReplaced``): a completion terminal
    /// already on the survivor is not another row's to replace. The guard
    /// enforced it against terminals the survivor BROUGHT and leaked for
    /// terminals it had just ADOPTED, which is the same downgrade —
    /// `completeFull` replaced by `completeTranscriptPartial`, and with it the
    /// `terminalReason` carrying the coverage numbers. The re-score does not
    /// contain it: a downgrade is not a claim coverage contradicts, so
    /// ``AnalysisCoordinator/reconcilePersistedTerminalAssetVerdict`` leaves it
    /// standing, and the sweep is one-shot.
    ///
    /// Three rows is not a hypothetical shape: nothing about the placeholder
    /// mint is once-per-episode, and the V39 unique index cannot collapse them
    /// because each placeholder's `assetFingerprint` is its own distinct UUID.
    @Test("with two placeholders, the terminal adopted first is not overwritten by the second")
    func firstAdoptedTerminalSurvivesASecondPlaceholder() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-multi-placeholder"
        // Insert order fixes the fold order (`assetMergeRows` orders by rowid).
        try await insertCanonical(
            store: store, id: "canon-multi", episodeId: episodeId,
            fingerprint: canonicalSHA, state: SessionState.queued.rawValue
        )
        try await insertPlaceholder(
            store: store, id: "ph-multi-first", episodeId: episodeId,
            state: SessionState.completeFull.rawValue,
            featureCoverage: 2_900, transcriptCoverage: 2_900, duration: 2_933
        )
        try await insertPlaceholder(
            store: store, id: "ph-multi-second", episodeId: episodeId,
            state: SessionState.completeTranscriptPartial.rawValue,
            featureCoverage: 100, transcriptCoverage: 120, duration: 543
        )
        try await store.execForTesting(
            "UPDATE analysis_assets SET terminalReason = 'first-full' WHERE id = 'ph-multi-first'"
        )
        try await store.execForTesting(
            "UPDATE analysis_assets SET terminalReason = 'second-partial' WHERE id = 'ph-multi-second'"
        )
        try await insertChunk(
            store: store, id: "chunk-multi-first", assetId: "ph-multi-first",
            index: 0, start: 0, end: 2_900
        )
        try await insertChunk(
            store: store, id: "chunk-multi-second", assetId: "ph-multi-second",
            index: 1, start: 0, end: 120
        )

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.episodesInspected == 1)
        #expect(summary.placeholdersMerged == 2, "both placeholders must fold, not just the first")
        #expect(summary.adoptedTerminalStates == 1,
                "the survivor acquires a completion terminal ONCE; the second fold has nothing left to adopt")

        let rows = try await allAssets(store: store, episodeId: episodeId)
        #expect(rows.count == 1, "three rows must converge to one (got \(rows.count))")
        let survivor = try #require(rows.first)
        #expect(survivor.id == "canon-multi")
        #expect(survivor.analysisState == SessionState.completeFull.rawValue,
                "a later placeholder must not downgrade the terminal an earlier one supplied")
        #expect(survivor.terminalReason == "first-full",
                "the reason carries the coverage numbers the adopted verdict was scored against")
        // The rest of the fold still accumulates across BOTH placeholders.
        #expect(survivor.featureCoverageEndTime == 2_900)
        #expect(survivor.fastTranscriptCoverageEndTime == 2_900)
        #expect(survivor.episodeDurationSec == 2_933,
                "the poisoned 543 s must not win the COALESCE either")
        #expect(try await store.fetchTranscriptChunks(assetId: "canon-multi").count == 2,
                "both placeholders' transcripts must move, not cascade away")
    }

    /// R3. The fold order decides which terminal the merged row keeps, so it
    /// must not be whatever order SQLite happened to return. `assetMergeRows`
    /// has no natural key to sort on but `rowid`, which is insert order and
    /// therefore the same order the device wrote the rows in.
    @Test("the fold order is deterministic, not whatever order the scan returned")
    func mergeRowsAreOrderedByRowId() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-fold-order"
        for index in 0..<4 {
            try await insertPlaceholder(
                store: store, id: "ph-order-\(index)", episodeId: episodeId,
                state: SessionState.queued.rawValue
            )
        }
        let rows = try await store.assetMergeRowsForTesting(episodeId: episodeId)
        #expect(rows.map(\.id) == ["ph-order-0", "ph-order-1", "ph-order-2", "ph-order-3"])
        #expect(rows.map(\.rowId) == rows.map(\.rowId).sorted())
    }

    /// R3. No canonical row has ever existed for this episode — it was played
    /// but never fully downloaded, which is exactly the state
    /// ``AnalysisStore/chooseMergeSurvivor(_:)`` falls back for. The duplicates
    /// still have to collapse: two placeholders is two split coverage
    /// watermarks and two sets of child rows for one episode.
    @Test("an episode with nothing but placeholders still converges to one row")
    func allPlaceholderEpisodeStillConverges() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-all-placeholder"
        try await insertPlaceholder(
            store: store, id: "ph-only-old", episodeId: episodeId,
            state: SessionState.queued.rawValue, transcriptCoverage: 400
        )
        try await insertPlaceholder(
            store: store, id: "ph-only-new", episodeId: episodeId,
            state: SessionState.queued.rawValue, transcriptCoverage: 90
        )
        // `chooseMergeSurvivor` falls back to newest, and `createdAt` ties are
        // the norm on a fast-inserting fixture — so the tiebreak is `rowid`.
        try await insertChunk(
            store: store, id: "chunk-only-old", assetId: "ph-only-old",
            index: 0, start: 0, end: 400
        )

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.placeholdersMerged == 1)
        let rows = try await allAssets(store: store, episodeId: episodeId)
        #expect(rows.count == 1, "got \(rows.count)")
        let survivor = try #require(rows.first)
        #expect(survivor.id == "ph-only-new", "newest wins when no row carries a canonical SHA")
        #expect(survivor.fastTranscriptCoverageEndTime == 400, "watermarks still take the max of the pair")
        #expect(try await store.fetchTranscriptChunks(assetId: "ph-only-new").count == 1)
    }

    /// R3. The counterpart: one row, and it is a placeholder. The sweep must
    /// not touch it — there is nothing to merge it into, and deleting it would
    /// take the episode's whole analysis with it.
    @Test("an episode whose only row is a placeholder is left completely alone")
    func lonePlaceholderIsNeverTouched() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-lone-placeholder"
        try await insertPlaceholder(
            store: store, id: "ph-lone", episodeId: episodeId,
            state: SessionState.completeFull.rawValue, transcriptCoverage: 900, duration: 543
        )
        try await insertChunk(
            store: store, id: "chunk-lone", assetId: "ph-lone", index: 0, start: 0, end: 900
        )

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary == DuplicateAssetMergeSummary(),
                "a single-row episode is not a duplicate and must not be inspected at all")
        let rows = try await allAssets(store: store, episodeId: episodeId)
        #expect(rows.count == 1)
        #expect(try #require(rows.first).id == "ph-lone")
        #expect(try await store.fetchTranscriptChunks(assetId: "ph-lone").count == 1)
    }

    /// R3. Two placeholders, only one of which names the survivor's artifact.
    /// The matching one must fold and the other must be left exactly where it
    /// is — a per-victim decision, not a per-episode one.
    @Test("one placeholder merges while a sibling naming a different artifact is skipped")
    func onePlaceholderMergesWhileAnotherIsSkipped() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-mixed-artifacts"
        try await insertCanonical(
            store: store, id: "canon-mixed", episodeId: episodeId, fingerprint: canonicalSHA,
            sourceURL: "file:///container-B/complete/9f2c.mp3"
        )
        try await insertPlaceholder(
            store: store, id: "ph-mixed-same", episodeId: episodeId,
            sourceURL: "file:///container-A/partials/9f2c.mp3"
        )
        try await insertPlaceholder(
            store: store, id: "ph-mixed-other", episodeId: episodeId,
            sourceURL: "file:///container-A/partials/deadbeef.mp3"
        )
        try await insertChunk(
            store: store, id: "chunk-mixed-other", assetId: "ph-mixed-other",
            index: 0, start: 0, end: 60
        )

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.placeholdersMerged == 1)
        #expect(summary.skippedDifferentSource == 1)
        #expect(summary.survivingAssetIds == ["canon-mixed"])
        let rows = try await allAssets(store: store, episodeId: episodeId)
        #expect(Set(rows.map(\.id)) == ["canon-mixed", "ph-mixed-other"],
                "the refusal is per-victim; it must not block the pair that does match")
        #expect(try await store.fetchTranscriptChunks(assetId: "ph-mixed-other").count == 1,
                "a skipped placeholder keeps its children")
    }

    /// R3, found by a SURVIVING mutation. ``AssetMergeRow/isPlaceholder`` is
    /// the SELF-REFERENCE test, `assetFingerprint == id`, and every fixture in
    /// this suite gives its non-placeholder rows a 64-hex canonical SHA — so
    /// loosening the predicate to `!hasCanonicalSHA` (a plausible-looking
    /// simplification) passed every test.
    ///
    /// It is not a simplification, it is a deletion rule. `AnalysisJobReconciler`
    /// mints Pipeline B rows with `sourceFingerprint: fp.strong ?? fp.weak`, so
    /// an asset downloaded before a full-file SHA existed carries the WEAK
    /// fingerprint — `url|etag|length|last-modified` — as its
    /// `assetFingerprint`. That is a real content identity, not a placeholder,
    /// and under the loosened predicate the merge would fold it into a sibling
    /// and delete the row. Only the self-reference cannot arise by accident,
    /// which is the whole argument for using it.
    @Test("a row fingerprinted with a weak identity is NOT a placeholder and is never folded away")
    func weakFingerprintedRowIsNotAPlaceholder() async throws {
        let weakIdentity = "https://cdn.example.com/ep9.mp3|\"etag-9\"|8388608|"
        #expect(!AssetMergeRow(
            rowId: 1, id: "weak-row", assetFingerprint: weakIdentity, createdAt: 1,
            analysisState: "queued", terminalReason: nil, sourceURL: "file:///a/9f2c.mp3"
        ).isPlaceholder, "a weak fingerprint is a content identity, not the Pipeline A self-reference")
        #expect(AssetMergeRow(
            rowId: 2, id: "self-row", assetFingerprint: "self-row", createdAt: 1,
            analysisState: "queued", terminalReason: nil, sourceURL: "file:///a/9f2c.mp3"
        ).isPlaceholder)

        // And end to end: the weak-fingerprinted row must still be standing.
        let store = try await makeTestStore()
        let episodeId = "ep-weak-identity"
        try await insertCanonical(
            store: store, id: "canon-weak", episodeId: episodeId, fingerprint: canonicalSHA
        )
        try await insertCanonical(
            store: store, id: "weak-identity-row", episodeId: episodeId, fingerprint: weakIdentity
        )
        try await insertChunk(
            store: store, id: "chunk-weak", assetId: "weak-identity-row", index: 0, start: 0, end: 300
        )

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.placeholdersMerged == 0,
                "neither row is a placeholder — there is nothing here to fold")
        #expect(Set(try await allAssets(store: store, episodeId: episodeId).map(\.id))
                == ["canon-weak", "weak-identity-row"])
        #expect(try await store.fetchTranscriptChunks(assetId: "weak-identity-row").count == 1,
                "and its children are still its own")
    }

    // MARK: - R3: degenerate identifiers

    /// R3 finding 2. R2 pinned `"   "` and called the rule "whitespace is not
    /// an episode identity". SQLite's one-argument `TRIM` strips ASCII SPACE
    /// and nothing else — a tab- or newline-only id sails through the filter,
    /// is grouped, and (because `DownloadManager.safeFilename(for:)` is
    /// `SHA256(episodeId)`, so every row written under that id lands on ONE
    /// basename) ``AssetMergeGuard`` votes `.merge` on rows from unrelated
    /// episodes. Same defect R1 and R2 each found once; the character class was
    /// just too narrow.
    @Test("tab- and newline-only episodeIds are rejected exactly like spaces are")
    func nonSpaceWhitespaceEpisodeIdIsNeverMerged() async throws {
        for (label, blankish) in [("tab", "\t"), ("newline", "\n"), ("mixed", " \t\r\n ")] {
            let store = try await makeTestStore()
            try await insertPlaceholder(
                store: store, id: "ph-\(label)", episodeId: blankish,
                sourceURL: "file:///container-A/e3b0c442.mp3"
            )
            try await insertCanonical(
                store: store, id: "canon-\(label)", episodeId: blankish, fingerprint: canonicalSHA,
                sourceURL: "file:///container-B/e3b0c442.mp3"
            )

            let summary = try await store.reconcileDuplicatePlaceholderAssets()
            #expect(summary.placeholdersMerged == 0,
                    "\(label): SHA256 of a blank id is one basename, so this merges unrelated episodes")
            #expect(summary.episodesInspected == 0, "\(label)")
            #expect(try await allAssets(store: store, episodeId: blankish).count == 2, "\(label)")
        }
    }

    /// R3. A real id that merely CONTAINS the characters a blank-id filter or a
    /// string-interpolated query would choke on must still be grouped and
    /// merged normally. `%` and `_` are LIKE wildcards, `'` and `"` end SQL
    /// literals and identifiers, and `--` opens a comment.
    @Test("an episodeId full of SQL metacharacters is bound, not interpreted")
    func metacharacterEpisodeIdMergesNormally() async throws {
        let store = try await makeTestStore()
        let nasty = "ep-'\"--%_;drop"
        try await insertPlaceholder(store: store, id: "ph-nasty", episodeId: nasty)
        try await insertCanonical(
            store: store, id: "canon-nasty", episodeId: nasty, fingerprint: canonicalSHA
        )
        // A neighbour the LIKE wildcards would sweep up if the id were ever
        // interpolated into a pattern.
        try await insertPlaceholder(store: store, id: "ph-plain", episodeId: "ep-xy--ab;drop")

        let summary = try await store.reconcileDuplicatePlaceholderAssets()
        #expect(summary.episodesInspected == 1, "only the id with two rows is a group")
        #expect(summary.placeholdersMerged == 1)
        #expect(try await allAssets(store: store, episodeId: nasty).count == 1)
        #expect(try await allAssets(store: store, episodeId: "ep-xy--ab;drop").count == 1,
                "the neighbour must be untouched")
    }

    // MARK: - R3: launch ordering

    /// R3 finding 3. R2 disposed of the launch ordering by reading, on the
    /// grounds that this sweep re-probes duration and re-scores terminal state
    /// itself and so depends on neither sweep in front of it. That is provable
    /// by running it first — and running it first is the only order that
    /// works, which is the opposite of "the ordering carries no invariant".
    ///
    /// Behind `reconcilePersistedTerminalStatesIfNeeded` the two sweeps fight.
    /// The reconcile scores the PLACEHOLDER's `completeFull` against the
    /// placeholder's own poisoned 543 s duration, finds 29 s of transcript
    /// against it, and repairs the claim away. The merge then has no completion
    /// terminal left to adopt, so the merged row lands on `queued` — even
    /// though the measured 30 s duration makes 29 s of coverage a claim that
    /// holds (0.967, clear of `finalizeBackfillMinCoverageRatio`). The episode
    /// is re-analysed from scratch for nothing, and both sweeps are one-shot.
    ///
    /// So `PlayheadRuntime` now runs the merge FIRST, and this test is what
    /// says so: it asserts the invariants that hold in either order, then pins
    /// the divergence that makes the order load-bearing. If the sibling sweeps
    /// are ever taught to agree, this last expectation is the one to revisit.
    @Test("the merge must run BEFORE the terminal reconcile or the adopted terminal is lost")
    func sweepMustRunBeforeTheTerminalReconcile() async throws {
        let audioURL = try writeSynthAudio(seconds: 30)

        func buildFixture(_ store: AnalysisStore) async throws {
            try await insertPlaceholder(
                store: store, id: "ph-order", episodeId: "ep-order",
                state: SessionState.completeFull.rawValue,
                featureCoverage: 29, transcriptCoverage: 29, duration: 543
            )
            try await insertCanonical(
                store: store, id: "canon-order", episodeId: "ep-order",
                fingerprint: canonicalSHA, state: SessionState.queued.rawValue,
                featureCoverage: nil, transcriptCoverage: nil, duration: nil
            )
            try await insertChunk(
                store: store, id: "chunk-order", assetId: "ph-order", index: 0, start: 0, end: 29
            )
        }

        // The shipped order: merge, then duration backfill, then reconcile.
        let firstStore = try await makeTestStore()
        try await buildFixture(firstStore)
        let firstCoordinator = makeCoordinator(store: firstStore)
        _ = await firstCoordinator.reconcileDuplicateAnalysisAssetsIfNeeded(cachedFileURL: { _ in audioURL })
        _ = await firstCoordinator.runEpisodeDurationBackfillIfNeeded(cachedFileURL: { _ in audioURL })
        _ = await firstCoordinator.reconcilePersistedTerminalStatesIfNeeded()

        // The order this bead originally shipped with.
        let lastStore = try await makeTestStore()
        try await buildFixture(lastStore)
        let lastCoordinator = makeCoordinator(store: lastStore)
        _ = await lastCoordinator.runEpisodeDurationBackfillIfNeeded(cachedFileURL: { _ in audioURL })
        _ = await lastCoordinator.reconcilePersistedTerminalStatesIfNeeded()
        _ = await lastCoordinator.reconcileDuplicateAnalysisAssetsIfNeeded(cachedFileURL: { _ in audioURL })

        // What the merge delivers regardless of where it sits: one row, the
        // union of the pair's coverage, and a MEASURED duration.
        for (label, store) in [("merge-first", firstStore), ("merge-last", lastStore)] {
            let rows = try await allAssets(store: store, episodeId: "ep-order")
            #expect(rows.count == 1, "\(label): the pair converges either way (got \(rows.count))")
            let survivor = try #require(rows.first)
            #expect(survivor.id == "canon-order", "\(label)")
            #expect(survivor.fastTranscriptCoverageEndTime == 29,
                    "\(label): watermarks are the max of the pair")
            let duration = try #require(survivor.episodeDurationSec, "\(label)")
            #expect(abs(duration - 30) < 1.0,
                    "\(label): the merged row carries a measured duration, not the 543 s artefact (got \(duration))")
        }

        let mergedFirst = try #require(try await firstStore.fetchAsset(id: "canon-order"))
        #expect(mergedFirst.analysisState == SessionState.completeFull.rawValue,
                "merging first preserves a completion the measured duration shows is true")

        let mergedLast = try #require(try await lastStore.fetchAsset(id: "canon-order"))
        #expect(mergedLast.analysisState == SessionState.queued.rawValue,
                "and merging last loses it — the reconcile repaired the placeholder's claim against the poisoned denominator before the merge could adopt it. This is why PlayheadRuntime runs the merge first.")
    }
}
