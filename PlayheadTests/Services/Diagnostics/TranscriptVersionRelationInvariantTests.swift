// TranscriptVersionRelationInvariantTests.swift
// playhead-llne — `semantic_scan_results.transcriptVersion` and
// `transcript_chunks.transcriptVersion` share a NAME and are different
// quantities BY CONSTRUCTION, and nothing detected it.
//
// ----- The two quantities -----
//
//   scan side   TranscriptAtomizer.transcriptVersionHash over the CANONICAL
//               chunk set (final replaces the fast rows it covers, then time
//               order). One hash over ALL of an asset's rows. Written at every
//               AdDetectionService scan-row write site.
//   chunk side  Written by NO producer — TranscriptEngineService and
//               FinalPassRetranscriptionRunner both persist nil. The schema
//               ladder's legacy backfill stamps every FINAL row with the SAME
//               hash function over the FINAL-ONLY subset in frozen
//               chunkIndex/id order. Fast rows stay NULL for ever.
//
// Same function, different population, different order. The naive join
// `ON (analysisAssetId, transcriptVersion)` returned ZERO rows for 13 of 13
// scan-bearing assets on the 2026-08-16 pull and for 39 of 41 on 2026-09-08
// (the other 2 are final-only assets — the one coincidence where the two
// hashes agree). It returns nothing SILENTLY, which is the dangerous
// direction. The recoverable relation is to RECOMPUTE the canonical hash over
// the asset's current rows and compare it with the scan row's.
//
// ----- What reddens what -----
//
//   * `measuredShapeFires` — the bead's condition MUST fire. Delete the
//     predicate, compare against the chunk stamps instead of the recompute,
//     or drop the `guard atCurrent == 0`, and this reads violations=0.
//   * `scanAtCurrentHashIsClean` — the ANTI-VACUITY control: a scan row at the
//     recomputed hash read today's text and must NOT fire, even though the
//     column join for that asset is still empty. A predicate keyed on the
//     column join fires here and is wrong.
//   * `columnJoinIsNotEvidence` — the other direction of the same control: a
//     NON-empty column join (a stale final-only stamp) may not clear an asset
//     the recompute refutes.
//   * `withoutRecomputeAbstains` — at launch the recompute is unpaid; the
//     asset must ABSTAIN, never read clean, and the reason must carry the
//     column-join count so the bead's number is on every pull.
//   * The store suite drives the REAL writers: the producer path persists no
//     stamp, the legacy backfill stamps final rows with a value the canonical
//     recompute does not equal while a fast row survives, and on a final-only
//     asset the two coincide (so the difference is population and order, not
//     the function). The naive join is probed with raw SQL against the
//     store's own file, so the zero is the zero a device pull would show.

import Foundation
import SQLite3
import Testing

@testable import Playhead

// MARK: - The evaluator, both directions

@Suite("The two transcriptVersion columns never join, and the rail says so (playhead-llne)")
struct TranscriptVersionRelationInvariantTests {

    // AA6CD430 on the 2026-08-16 pull: two scan versions, one chunk stamp,
    // no overlap. The current-hash value is a stand-in of the right shape.
    static let scanV1 = "9b22acf689e9a4ababe5f185b3cc9f75"
    static let scanV2 = "08db881d7e736baa4e9862e8de28704b"
    static let legacyStamp = "aa1e5aa7f7347e1fa87231e828c66a44"
    static let currentHash = "cd175ee9ffd1d7fc91be0c9d2a8fb51e"

    private func snapshot(
        _ rows: [PersistedStateSnapshot.TranscriptVersionRelationRow]
    ) -> PersistedStateSnapshot {
        PersistedStateSnapshot(
            backfillJobs: [],
            assets: [],
            eligibilityGatedAdWindows: [],
            coverageLaneRetryCap: 3,
            transcriptVersionRelations: rows
        )
    }

    private func row(
        asset: String = "AA6CD430",
        scans: [String: Int],
        stamps: [String: Int],
        nulls: Int,
        hash: String?
    ) -> PersistedStateSnapshot.TranscriptVersionRelationRow {
        PersistedStateSnapshot.TranscriptVersionRelationRow(
            assetId: asset,
            scanRowsByVersion: scans,
            chunkRowsByStamp: stamps,
            chunkNullRows: nulls,
            chunkSetHash: hash
        )
    }

    private func judge(
        _ rows: [PersistedStateSnapshot.TranscriptVersionRelationRow]
    ) throws -> PersistedStateInvariantFinding {
        try #require(
            PersistedStateInvariantEvaluator.evaluate(snapshot(rows))
                .finding(.semanticScanVersionUnrelatedToChunkSet))
    }

    @Test("llne — the measured shape FIRES: scan versions, chunk stamps and the recompute pairwise disjoint")
    func measuredShapeFires() throws {
        let finding = try judge([
            row(scans: [Self.scanV1: 30, Self.scanV2: 19],
                stamps: [Self.legacyStamp: 1_500],
                nulls: 755,
                hash: Self.currentHash),
        ])
        #expect(finding.violations == 1)
        #expect(finding.population == 1)
        #expect(finding.abstained == 0)
        #expect(finding.abstainReason == nil)
        let witness = try #require(finding.witnesses.first)
        #expect(witness.contains("asset=AA6CD430"))
        #expect(witness.contains("scan_rows=49"))
        #expect(witness.contains("scan_versions=2"))
        #expect(witness.contains("chunk_rows=2255"))
        #expect(witness.contains("chunk_stamps=1"))
        #expect(witness.contains("chunk_null_rows=755"))
        // The false join's own answer, on the witness line, as a number.
        #expect(witness.contains("column_join_rows=0"))
        #expect(witness.contains("chunk_set_hash=\(Self.currentHash)"))
        #expect(witness.contains("scan_rows_at_chunk_set_hash=0"))
    }

    @Test("llne — CONTROL: a scan row at the recomputed chunk-set hash does NOT fire, with the column join still empty")
    func scanAtCurrentHashIsClean() throws {
        // CD2976E6's shape on the 2026-08-19 pull: chunks stamped one value,
        // current segmentation hashing to another, and a coarse row AT the
        // current version. The text that row read IS on disk.
        let control = row(
            scans: [Self.scanV1: 30, Self.currentHash: 1],
            stamps: [Self.legacyStamp: 1_500],
            nulls: 755,
            hash: Self.currentHash)
        // The column join is empty for this asset — cleanliness can only have
        // come from the recompute. State it, so a predicate that quietly
        // switched to the column relation could not pass this test by luck.
        #expect(control.columnJoinRowCount == 0)
        #expect(control.scanRowsAtChunkSetHash == 1)

        let finding = try judge([control])
        #expect(finding.violations == 0)
        #expect(finding.population == 1)
        #expect(finding.abstained == 0)
        #expect(finding.witnesses.isEmpty)
    }

    @Test("llne — CONTROL: a non-empty column join does not clear an asset the recompute refutes")
    func columnJoinIsNotEvidence() throws {
        // A final-only asset whose stamp went stale (5 of 37 stamped assets on
        // the 2026-09-08 pull): the naive join returns rows, and every one of
        // them names text that has since changed.
        let stale = row(
            scans: [Self.legacyStamp: 4],
            stamps: [Self.legacyStamp: 4_054],
            nulls: 0,
            hash: Self.currentHash)
        #expect(stale.columnJoinRowCount == 4 * 4_054)

        let finding = try judge([stale])
        #expect(finding.violations == 1)
        #expect(finding.population == 1)
        let witness = try #require(finding.witnesses.first)
        #expect(witness.contains("column_join_rows=16216"))
        #expect(witness.contains("scan_rows_at_chunk_set_hash=0"))
    }

    @Test("llne — without the recompute the asset ABSTAINS, never reads clean, and the reason carries the column-join count")
    func withoutRecomputeAbstains() throws {
        let finding = try judge([
            // the 39-of-41 shape: join empty
            row(asset: "a", scans: [Self.scanV1: 1], stamps: [Self.legacyStamp: 1], nulls: 0, hash: nil),
            // the 0FF7EFF3 coincidence: join non-empty — STILL abstained, the
            // column relation is not consulted as a substitute
            row(asset: "b", scans: [Self.legacyStamp: 1], stamps: [Self.legacyStamp: 1], nulls: 0, hash: nil),
        ])
        #expect(finding.violations == 0)
        #expect(finding.population == 0)
        #expect(finding.abstained == 2)
        #expect(finding.abstainReason == "chunk_set_hash_not_computed;column_join_empty=1/2")
        #expect(finding.witnesses.isEmpty)
    }

    @Test("llne — an asset with scan rows but no chunk rows, or chunks but no scans, is outside the population")
    func outsidePopulation() throws {
        let finding = try judge([
            row(asset: "scans-only", scans: [Self.scanV1: 3], stamps: [:], nulls: 0, hash: Self.currentHash),
            row(asset: "chunks-only", scans: [:], stamps: [Self.legacyStamp: 2], nulls: 5, hash: Self.currentHash),
        ])
        #expect(finding.violations == 0)
        // EMPTY population, not a clean one: population=0 abstained=0.
        #expect(finding.population == 0)
        #expect(finding.abstained == 0)
        #expect(finding.abstainReason == nil)
    }

    @Test("llne — the row's derived counts are the naive SQL join's own arithmetic")
    func columnJoinArithmetic() {
        let mixed = row(scans: ["x": 2, "y": 3], stamps: ["x": 10, "z": 7], nulls: 4, hash: "y")
        #expect(mixed.scanRowCount == 5)
        #expect(mixed.chunkRowCount == 21)
        #expect(mixed.scanVersions == ["x", "y"])
        #expect(mixed.chunkStamps == ["x", "z"])
        // Only `x` is on both sides: 2 scan rows × 10 chunk rows.
        #expect(mixed.columnJoinRowCount == 20)
        #expect(mixed.scanRowsAtChunkSetHash == 3)
        // Not computed is nil, never zero.
        #expect(row(scans: ["x": 2], stamps: [:], nulls: 1, hash: nil).scanRowsAtChunkSetHash == nil)
    }

    @Test("llne — the reporter carries the relation rows through its rebuild, so the census reads them at launch")
    func reporterCarriesRelationsThrough() async throws {
        // `resolvingAudioPresence` reconstructs the snapshot field by field.
        // The init defaults this field to `[]`; a rebuild that forgot it would
        // read population=0 abstained=0 here — an empty world, silently.
        let logger = SurfaceStatusInvariantLogger(directory: try makeTempDir(prefix: "llne-reporter"))
        let snap = snapshot([
            row(scans: [Self.scanV1: 1], stamps: [Self.legacyStamp: 1], nulls: 0, hash: Self.currentHash),
        ])
        let findings = await PersistedStateInvariantReporter(
            snapshotProvider: { snap },
            audioPresenceProbe: { _ in false },
            logger: logger
        ).report()
        let finding = try #require(findings.finding(.semanticScanVersionUnrelatedToChunkSet))
        #expect(finding.violations == 1)
        #expect(finding.population == 1)

        logger.flushForTesting()
        let url = try #require(logger.currentSessionFileURL)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let entries = try String(decoding: try Data(contentsOf: url), as: UTF8.self)
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { try decoder.decode(SurfaceStateTransitionEntry.self, from: Data($0.utf8)) }
        let violations = entries.compactMap(\.invariantViolation)
        #expect(violations.contains {
            $0.code == .persistedStateInvariantCensus
                && $0.description == "invariant=semantic_scan_version_unrelated_to_chunk_set"
                    + " violations=1 population=1 witnesses=1/1"
        })
        #expect(violations.contains {
            $0.code == .persistedStateInvariantViolation
                && $0.description.hasPrefix("invariant=semantic_scan_version_unrelated_to_chunk_set asset=AA6CD430")
        })
    }

    @Test("llne — the licence is a refusal blocked on the remedy decision, and repairs nothing at launch")
    func licenceIsARefusalBlockedOnTheRemedy() throws {
        let licence = PersistedStateInvariant.semanticScanVersionUnrelatedToChunkSet.healLicence
        #expect(!licence.repairsAtLaunch)
        #expect(licence.blockingBead == "playhead-llne")
        guard case let .noRuleChanged(reason, _) = licence else {
            Issue.record("llne's licence stopped being a no-rule-changed refusal: \(licence)")
            return
        }
        // The reason must say WHY no repair can exist here, not merely that
        // none is licensed: the relation is recomputable, and writing either
        // column's value onto the other would fabricate one.
        #expect(reason.contains("recomputable"))
        #expect(reason.contains("rename"))
    }
}

// MARK: - The real writers

@Suite("fetchTranscriptVersionRelations reads the real writers (playhead-llne)")
struct TranscriptVersionRelationStoreTests {

    private static let assetId = "llne-asset"

    private func makeAsset(id: String = TranscriptVersionRelationStoreTests.assetId) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: 40,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 40,
            finalPassCoverageEndTime: 40
        )
    }

    /// A chunk as BOTH producers persist it: `transcriptVersion: nil`.
    private func chunk(
        index: Int,
        start: Double,
        end: Double,
        pass: String,
        text: String,
        asset: String = TranscriptVersionRelationStoreTests.assetId
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(asset)-chunk-\(index)",
            analysisAssetId: asset,
            segmentFingerprint: "seg-\(asset)-\(index)",
            chunkIndex: index,
            startTime: start,
            endTime: end,
            text: text,
            normalizedText: text.lowercased(),
            pass: pass,
            modelVersion: "asr-test",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }

    /// Mixed fast + final, where the final rows only PARTIALLY cover the fast
    /// rows they touch, so the canonicalizer keeps all four and the canonical
    /// set (4 rows, time order) differs from the final-only set (2 rows).
    private func mixedChunks() -> [TranscriptChunk] {
        [
            chunk(index: 0, start: 0, end: 10, pass: "fast", text: "Fast one"),
            chunk(index: 1, start: 10, end: 20, pass: "fast", text: "Fast two"),
            chunk(index: 2, start: 5, end: 15, pass: "final", text: "Final one"),
            chunk(index: 3, start: 30, end: 40, pass: "final", text: "Final two"),
        ]
    }

    private func scan(
        index: Int,
        version: String,
        asset: String = TranscriptVersionRelationStoreTests.assetId
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: "\(asset)-scan-\(index)",
            analysisAssetId: asset,
            windowFirstAtomOrdinal: index * 10,
            windowLastAtomOrdinal: index * 10 + 9,
            windowStartTime: Double(index) * 10,
            windowEndTime: Double(index) * 10 + 9,
            scanPass: SemanticScanResult.presenceScanPass,
            transcriptQuality: .good,
            disposition: .noAds,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: nil,
            scanCohortJSON: makeTestScanCohortJSON(),
            transcriptVersion: version,
            reuseScope: "\(asset)-passA-\(index)"
        )
    }

    /// THE FALSE JOIN, spelled the way a coverage query would spell it, run
    /// with a second read-only handle on the store's own file — so the number
    /// is the number a pull would show, not one the store computed for us.
    private func naiveJoinRowCount(in directory: URL) throws -> Int {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "llne.naiveJoin", code: 1)
        }
        defer { sqlite3_close_v2(db) }
        let sql = """
            SELECT COUNT(*)
              FROM semantic_scan_results s
              JOIN transcript_chunks c
                ON c.analysisAssetId = s.analysisAssetId
               AND c.transcriptVersion = s.transcriptVersion
            """
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(domain: "llne.naiveJoin", code: 2)
        }
        defer { sqlite3_finalize(stmt) }
        guard sqlite3_step(stmt) == SQLITE_ROW else { return 0 }
        return Int(sqlite3_column_int64(stmt, 0))
    }

    private func evaluate(
        _ rows: [PersistedStateSnapshot.TranscriptVersionRelationRow]
    ) throws -> PersistedStateInvariantFinding {
        let snapshot = PersistedStateSnapshot(
            backfillJobs: [],
            assets: [],
            eligibilityGatedAdWindows: [],
            coverageLaneRetryCap: 3,
            transcriptVersionRelations: rows)
        return try #require(
            PersistedStateInvariantEvaluator.evaluate(snapshot)
                .finding(.semanticScanVersionUnrelatedToChunkSet))
    }

    @Test("llne — the producer path persists NO chunk stamp, the scan side carries the canonical hash, and the naive join returns ZERO rows")
    func producersWriteNoStampAndTheJoinIsEmpty() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        try await store.insertAsset(makeAsset())
        _ = try await store.insertTranscriptChunks(mixedChunks())

        let persisted = try await store.fetchTranscriptChunks(assetId: Self.assetId)
        #expect(persisted.count == 4)
        // NO producer writes the column. Every row is NULL after the real
        // insert path, fast and final alike.
        #expect(persisted.allSatisfy { $0.transcriptVersion == nil })

        // What the classifier stamps on its row: the canonical chunk-set hash
        // over exactly these persisted rows.
        let current = SemanticScanClaim.transcriptVersion(forPersistedChunks: persisted)
        try await store.insertSemanticScanResult(scan(index: 0, version: current))

        // The bead's measurement, reproduced against the real writers.
        #expect(try naiveJoinRowCount(in: dir) == 0)

        let rows = try await store.fetchTranscriptVersionRelations(computingChunkSetHash: true)
        #expect(rows.count == 1)
        let row = try #require(rows.first)
        #expect(row.assetId == Self.assetId)
        #expect(row.scanRowsByVersion == [current: 1])
        #expect(row.chunkRowsByStamp.isEmpty)
        #expect(row.chunkNullRows == 4)
        #expect(row.columnJoinRowCount == 0)
        #expect(row.chunkSetHash == current)
        #expect(row.scanRowsAtChunkSetHash == 1)

        // The control through the store: the scan read today's text.
        let finding = try evaluate(rows)
        #expect(finding.violations == 0)
        #expect(finding.population == 1)
        #expect(finding.abstained == 0)
    }

    @Test("llne — the legacy backfill stamps every FINAL row with the final-only hash, which is not the canonical hash while a fast row survives")
    func legacyBackfillStampIsNotTheCanonicalHash() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        try await store.insertAsset(makeAsset())
        _ = try await store.insertTranscriptChunks(mixedChunks())
        let before = try await store.fetchTranscriptChunks(assetId: Self.assetId)
        let current = SemanticScanClaim.transcriptVersion(forPersistedChunks: before)
        try await store.insertSemanticScanResult(scan(index: 0, version: current))

        // The ONLY writer of transcript_chunks.transcriptVersion: the schema
        // ladder's Phase-1 backfill, which `migrate()` runs at every open.
        try await store.migrateOnlyForTesting()

        let after = try await store.fetchTranscriptChunks(assetId: Self.assetId)
        let finals = after.filter { $0.pass == "final" }
        let fasts = after.filter { $0.pass == "fast" }
        #expect(finals.count == 2)
        #expect(fasts.count == 2)
        #expect(finals.allSatisfy { $0.transcriptVersion != nil })
        #expect(fasts.allSatisfy { $0.transcriptVersion == nil })
        let stamp = try #require(finals.first?.transcriptVersion)
        #expect(finals.allSatisfy { $0.transcriptVersion == stamp })

        // SAME hash function, DIFFERENT population: the stamp is the hash over
        // the two final rows alone, and the scan side is the hash over all
        // four canonical rows. The backfill changed no text, so the canonical
        // recompute after it still equals the scan row's version.
        #expect(stamp == TranscriptAtomizer.transcriptVersionHash(chunks: finals))
        #expect(stamp != current)
        #expect(SemanticScanClaim.transcriptVersion(forPersistedChunks: after) == current)

        // The false join is empty for this asset even though both columns are
        // now populated — the bead's 39-of-41 shape.
        #expect(try naiveJoinRowCount(in: dir) == 0)

        let rows = try await store.fetchTranscriptVersionRelations(computingChunkSetHash: true)
        let row = try #require(rows.first)
        #expect(row.scanRowsByVersion == [current: 1])
        #expect(row.chunkRowsByStamp == [stamp: 2])
        #expect(row.chunkNullRows == 2)
        #expect(row.columnJoinRowCount == 0)
        #expect(row.chunkSetHash == current)

        // …and the invariant does not fire: the text this scan read is on disk.
        let finding = try evaluate(rows)
        #expect(finding.violations == 0)
        #expect(finding.population == 1)
    }

    @Test("llne — ANTI-VACUITY: on a final-only asset in time order the two hashes coincide and the join returns rows")
    func finalOnlyAssetIsTheCoincidence() async throws {
        // 0FF7EFF3 and E30D13AB on the 2026-09-08 pull: 0 fast rows. The
        // canonicalizer passes an all-final set through, the legacy sort and
        // the time sort agree, and the same function over the same rows in the
        // same order is the same value. The difference between the columns is
        // population and order — NOT the hash function — and this is the test
        // that would fail if it were the function.
        let (store, dir) = try await makeTestStoreWithDirectory()
        try await store.insertAsset(makeAsset())
        _ = try await store.insertTranscriptChunks([
            chunk(index: 0, start: 0, end: 10, pass: "final", text: "Final a"),
            chunk(index: 1, start: 10, end: 20, pass: "final", text: "Final b"),
            chunk(index: 2, start: 20, end: 30, pass: "final", text: "Final c"),
        ])
        let before = try await store.fetchTranscriptChunks(assetId: Self.assetId)
        let current = SemanticScanClaim.transcriptVersion(forPersistedChunks: before)
        try await store.insertSemanticScanResult(scan(index: 0, version: current))
        try await store.migrateOnlyForTesting()

        let after = try await store.fetchTranscriptChunks(assetId: Self.assetId)
        #expect(after.allSatisfy { $0.transcriptVersion == current })
        // One scan row × three stamped chunk rows.
        #expect(try naiveJoinRowCount(in: dir) == 3)

        let rows = try await store.fetchTranscriptVersionRelations(computingChunkSetHash: true)
        let row = try #require(rows.first)
        #expect(row.chunkRowsByStamp == [current: 3])
        #expect(row.chunkNullRows == 0)
        #expect(row.columnJoinRowCount == 3)
        #expect(row.chunkSetHash == current)
        #expect(try evaluate(rows).violations == 0)
    }

    @Test("llne — a scan row whose version no current chunk set hashes to FIRES through the store, and abstains without the recompute")
    func staleScanFiresThroughTheStore() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        try await store.insertAsset(makeAsset())
        _ = try await store.insertTranscriptChunks(mixedChunks())
        // A version no chunk set on this store hashes to: the transcript this
        // scan read no longer exists in that form.
        let superseded = "0123456789abcdef0123456789abcdef"
        try await store.insertSemanticScanResult(scan(index: 0, version: superseded))
        #expect(try naiveJoinRowCount(in: dir) == 0)

        let judged = try await store.fetchTranscriptVersionRelations(computingChunkSetHash: true)
        let fired = try evaluate(judged)
        #expect(fired.violations == 1)
        #expect(fired.population == 1)
        let witness = try #require(fired.witnesses.first)
        #expect(witness.contains("asset=\(Self.assetId)"))
        #expect(witness.contains("scan_rows=1"))
        #expect(witness.contains("chunk_rows=4"))
        #expect(witness.contains("column_join_rows=0"))
        #expect(witness.contains("scan_rows_at_chunk_set_hash=0"))

        // The same rows without the recompute: NOT judged, and the reason
        // carries the column-join count.
        let unpaid = try await store.fetchTranscriptVersionRelations(computingChunkSetHash: false)
        #expect(unpaid.first?.chunkSetHash == nil)
        #expect(unpaid.first?.scanRowsByVersion == [superseded: 1])
        #expect(unpaid.first?.chunkNullRows == 4)
        let abstained = try evaluate(unpaid)
        #expect(abstained.violations == 0)
        #expect(abstained.population == 0)
        #expect(abstained.abstained == 1)
        #expect(abstained.abstainReason == "chunk_set_hash_not_computed;column_join_empty=1/1")
    }

    @Test("llne — fetchPersistedStateSnapshot carries the relation rows WITHOUT the recompute, and the read never writes")
    func launchSnapshotAbstains() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        try await store.insertAsset(makeAsset())
        _ = try await store.insertTranscriptChunks(mixedChunks())
        let current = SemanticScanClaim.transcriptVersion(
            forPersistedChunks: try await store.fetchTranscriptChunks(assetId: Self.assetId))
        try await store.insertSemanticScanResult(scan(index: 0, version: current))

        let snapshot = try await store.fetchPersistedStateSnapshot()
        #expect(snapshot.transcriptVersionRelations.count == 1)
        let row = try #require(snapshot.transcriptVersionRelations.first)
        #expect(row.assetId == Self.assetId)
        #expect(row.scanRowsByVersion == [current: 1])
        #expect(row.chunkNullRows == 4)
        // The launch reader does not pay for the recompute…
        #expect(row.chunkSetHash == nil)
        // …so the launch census abstains and says why.
        let finding = try #require(
            PersistedStateInvariantEvaluator.evaluate(snapshot)
                .finding(.semanticScanVersionUnrelatedToChunkSet))
        #expect(finding.population == 0)
        #expect(finding.abstained == 1)
        #expect(finding.abstainReason == "chunk_set_hash_not_computed;column_join_empty=1/1")

        // And it wrote nothing: no stamp appeared on any chunk row.
        let after = try await store.fetchTranscriptChunks(assetId: Self.assetId)
        #expect(after.allSatisfy { $0.transcriptVersion == nil })
    }

    @Test("llne — a store with scan rows and no chunk rows at all yields a row outside the population")
    func scansWithoutChunksAreOutsideThePopulation() async throws {
        let (store, _) = try await makeTestStoreWithDirectory()
        try await store.insertAsset(makeAsset())
        try await store.insertSemanticScanResult(scan(index: 0, version: "0123456789abcdef0123456789abcdef"))

        let rows = try await store.fetchTranscriptVersionRelations(computingChunkSetHash: true)
        let row = try #require(rows.first)
        #expect(row.scanRowCount == 1)
        #expect(row.chunkRowCount == 0)
        // No chunks ⇒ no recompute is attempted (a hash of nothing is a value,
        // and it would be a fabricated one).
        #expect(row.chunkSetHash == nil)
        let finding = try evaluate(rows)
        #expect(finding.population == 0)
        #expect(finding.abstained == 0)
        #expect(finding.violations == 0)
    }
}
