// AnalysisStoreDecisionReadFailureTests.swift
// playhead-nintb — the transcript read was ONE of 74. This suite pins the
// converted DECISION-gating reads: a mid-scan SQLite fault must arrive as a
// THROW, not as an empty result a caller reads as a fact.
//
// playhead-0bpb0 converted the transcript read and proved the class with
// `TranscriptReadFailureTests`. This bead swept the reads whose emptiness
// changes a DECISION rather than a display — the correction/veto read, the
// repeated-ad-cache reads (fingerprints + sponsor knowledge), the scheduler's
// job reads, and the coverage read that gates ad-scan eligibility. Each read
// below now advances its loop through the file's throwing `nextRow(_:)` helper,
// so a terminal code that is neither ROW nor DONE (an I/O fault, a torn write,
// a malformed page) propagates instead of ending the loop as "no rows".
//
// HOW THE FAULT IS FORCED — identical in spirit to playhead-0bpb0. After the
// rows are committed and the WAL is folded in, the target table's b-tree ROOT
// PAGE is zeroed on disk. `prepare` reads only `sqlite_master` (page 1) and
// still succeeds; the first `sqlite3_step` fails reaching the zeroed page, so
// the fault lands in the ROW LOOP, which is exactly where the conversion lives.
// The root page is looked up from `sqlite_master` (robust to schema layout —
// see the note in playhead-0bpb0's helper), and only the NAMED table is
// corrupted, so a sibling read on any other table still completes: that is the
// anti-vacuity control, proving `prepare`/schema are intact and the throw comes
// from the step, not from a broken header.
//
// A rail here is evidence only once it has fired: each `#expect(throws:)` fails
// today's converted code exactly when the read stops throwing (reverting the
// loop to `while sqlite3_step(stmt) == SQLITE_ROW` makes the read return empty
// and reddens the corresponding test), and each control fails if the schema
// itself broke instead of the row loop.

import Foundation
import SQLite3
import Testing
@testable import Playhead

@Suite("playhead-nintb — a DECISION read that cannot be READ is not an empty cache")
struct AnalysisStoreDecisionReadFailureTests {

    // MARK: - Shared corruption primitive

    /// Fold the WAL into the main file and zero the ROOT PAGE of `table`.
    /// Only that one table's b-tree is damaged; page 1 (the schema) is left
    /// intact so `prepare` still succeeds and the failure is forced into the
    /// row loop. Returns after `synchronize()` so the next connection reads the
    /// corrupted bytes.
    private func corruptRootPage(of table: String, at directory: URL) throws {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")

        var raw: OpaquePointer?
        #expect(sqlite3_open(dbURL.path, &raw) == SQLITE_OK)
        _ = sqlite3_exec(raw, "PRAGMA wal_checkpoint(TRUNCATE)", nil, nil, nil)
        // `table` is a controlled test constant, never user input, so it is
        // interpolated into the SQL rather than bound — a bound C string from
        // `withCString` would dangle across the `sqlite3_step` below.
        var rootPage = 0
        var stmt: OpaquePointer?
        if sqlite3_prepare_v2(
            raw,
            "SELECT rootpage FROM sqlite_master WHERE type='table' AND name='\(table)'",
            -1, &stmt, nil
        ) == SQLITE_OK, sqlite3_step(stmt) == SQLITE_ROW {
            rootPage = Int(sqlite3_column_int(stmt, 0))
        }
        sqlite3_finalize(stmt)
        sqlite3_close(raw)
        #expect(rootPage > 1, "\(table) must have a root page past the header; got \(rootPage)")

        let handle = try FileHandle(forUpdating: dbURL)
        defer { try? handle.close() }
        let header = try handle.read(upToCount: 100) ?? Data()
        #expect(header.count == 100, "the fixture must have a readable SQLite header")
        // Bytes 16..17, big-endian, are the page size; the value 1 means 65536.
        let raw16 = Int(header[16]) << 8 | Int(header[17])
        let pageSize = raw16 == 1 ? 65_536 : raw16
        try handle.seek(toOffset: UInt64((rootPage - 1) * pageSize))
        try handle.write(contentsOf: Data(repeating: 0, count: pageSize))
        try handle.synchronize()
    }

    /// Rows enough to push a table past a single page, so its root becomes an
    /// interior page whose loss stops the scan on the first step.
    private static let rowCount = 200

    // MARK: - Coverage / ad-scan eligibility: transcript_chunks
    //
    // `fetchTranscribedRegion` feeds `SemanticScanClaim.bridgedTranscriptCoveredSec`
    // in `AnalysisJobReconciler`/`AnalysisJobRunner`: a false-empty reads coverage
    // as ~0 %, which drops BELOW the 0.95 finalize floor, and the ad scan is gated
    // OFF for that asset — the podcast plays with its ads unscanned. This is
    // playhead-9y9e's shipped defect direction.

    private func seedTranscript(store: AnalysisStore, assetId: String) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: "ep-nintb",
            assetFingerprint: assetId,
            weakFingerprint: nil,
            sourceURL: "file:///nintb/episode.mp3",
            featureCoverageEndTime: 1_200.0,
            fastTranscriptCoverageEndTime: 1_200.0,
            confirmedAdCoverageEndTime: nil,
            analysisState: "completeAdScanPartial",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 1_200.0,
            episodeTitle: "nintb fixture"
        ))
        let chunks = (0..<Self.rowCount).map { index in
            TranscriptChunk(
                id: "c-\(index)",
                analysisAssetId: assetId,
                segmentFingerprint: "seg-\(index)",
                chunkIndex: index,
                startTime: Double(index) * 3.0,
                endTime: Double(index) * 3.0 + 3.0,
                text: String(repeating: "transcript body \(index) ", count: 20),
                normalizedText: "transcript body \(index)",
                pass: TranscriptPassType.fast.rawValue,
                modelVersion: "speech-v1",
                transcriptVersion: nil,
                atomOrdinal: nil,
                weakAnchorMetadata: nil
            )
        }
        _ = try await store.insertTranscriptChunks(chunks)
    }

    @Test("a coverage read that fails THROWS instead of reporting a zero-coverage asset")
    func transcribedRegionReadThrowsOnFault() async throws {
        let dir = try makeTempDir(prefix: "nintb-coverage")
        defer { try? FileManager.default.removeItem(at: dir) }
        let assetId = "A-nintb-coverage"

        do {
            let store = try AnalysisStore(directory: dir)
            try await store.migrate()
            try await seedTranscript(store: store, assetId: assetId)
            let before = try await store.fetchTranscribedRegion(assetId: assetId)
            #expect(!before.isEmpty, "premise: the transcript is there to read")
        }

        try corruptRootPage(of: "transcript_chunks", at: dir)

        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        await #expect(
            throws: AnalysisStoreError.self,
            """
            A failed transcript-coverage read returned normally. Before playhead-nintb it \
            returned an EMPTY region, which SemanticScanClaim reads as ~0 % coverage — below \
            the finalize floor — so the ad scan is gated OFF and the episode plays unscanned.
            """
        ) {
            _ = try await store.fetchTranscribedRegion(assetId: assetId)
        }

        // Anti-vacuity control: a sibling read on an UNcorrupted table completes,
        // proving the schema is intact and the throw came from the row loop.
        let ads = try await store.fetchAdWindows(assetId: assetId)
        #expect(ads.isEmpty, "the fixture seeds no ad windows; the point is that the READ completed")
    }

    // MARK: - Correction / veto: correction_events
    //
    // `suppressingCorrectionScopesPresent` answers "which of these scopes carry a
    // user suppression (a keep-this / veto)?" A false-empty reads as "no
    // suppression present" and BYPASSES revocation — the exact "bypass revocation
    // and capacity checks" the `nextRow` doc names, and the field-witness class:
    // a correction the listener made is silently not honored.

    private func seedCorrections(store: AnalysisStore, assetId: String) async throws -> [String] {
        try await store.insertAsset(makeTestAsset(id: assetId))
        var scopes: [String] = []
        for index in 0..<Self.rowCount {
            let scope = CorrectionScope.exactSpan(
                assetId: assetId,
                ordinalRange: index...(index + 1)
            ).serialized
            scopes.append(scope)
            // `.manualVeto` is a suppress-direction (falsePositive-kind) source and
            // is NOT explicit-banner feedback, so it inserts cleanly and IS returned
            // by the reader — a boost (`.falseNegative`) source would be filtered out.
            _ = try await store.appendCorrectionEvent(CorrectionEvent(
                analysisAssetId: assetId,
                scope: scope,
                createdAt: 1_700_000_000 + Double(index),
                source: .manualVeto,
                podcastId: "pod-nintb"
            ))
        }
        return scopes
    }

    @Test("a correction/veto read that fails THROWS instead of reporting no suppression")
    func suppressingScopesReadThrowsOnFault() async throws {
        let dir = try makeTempDir(prefix: "nintb-correction")
        defer { try? FileManager.default.removeItem(at: dir) }
        let assetId = "A-nintb-correction"
        var scopes: [String] = []

        do {
            let store = try AnalysisStore(directory: dir)
            try await store.migrate()
            scopes = try await seedCorrections(store: store, assetId: assetId)
            let before = try await store.suppressingCorrectionScopesPresent(from: scopes)
            #expect(before.count == scopes.count, "premise: every seeded veto scope is present")
        }

        try corruptRootPage(of: "correction_events", at: dir)

        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        await #expect(
            throws: AnalysisStoreError.self,
            """
            A failed correction read returned normally. An empty set reads as "no user \
            suppression present," which bypasses revocation — the listener's own keep-this \
            correction is silently dropped and the region is treated as skippable again.
            """
        ) {
            _ = try await store.suppressingCorrectionScopesPresent(from: scopes)
        }

        let ads = try await store.fetchAdWindows(assetId: assetId)
        #expect(ads.isEmpty, "the fixture seeds no ad windows; the point is that the READ completed")
    }

    // MARK: - Repeated-ad cache: ad_copy_fingerprints
    //
    // `loadFingerprintEntries` returns the confirmed ad-copy fingerprints a new
    // episode is matched against, and `loadAllFingerprintEntries` backs
    // `atomicConfirmFingerprint`'s near-duplicate check. A false-empty reads as
    // "no known ad fingerprints," so a repeated ad already confirmed for this show
    // is not recognized and not skipped — the cache that exists to skip it
    // silently reports itself empty. (The reader's per-row `do/catch` tolerance
    // for a corrupt DECODE is a different thing from the loop terminal this pins.)

    private func seedFingerprints(store: AnalysisStore, podcastId: String) async throws {
        for index in 0..<Self.rowCount {
            try await store.upsertFingerprintEntry(FingerprintEntry(
                id: "fp-\(index)",
                podcastId: podcastId,
                fingerprintHash: "hash-\(index)",
                normalizedText: "ad copy body \(index)",
                state: .active,
                confirmationCount: 3,
                firstSeenAt: 1_000 + Double(index),
                lastConfirmedAt: 1_000 + Double(index)
            ))
        }
    }

    @Test("a repeated-ad-cache read that fails THROWS instead of reporting no known ads")
    func fingerprintReadThrowsOnFault() async throws {
        let dir = try makeTempDir(prefix: "nintb-fingerprint")
        defer { try? FileManager.default.removeItem(at: dir) }
        let podcastId = "pod-nintb-fp"

        do {
            let store = try AnalysisStore(directory: dir)
            try await store.migrate()
            try await seedFingerprints(store: store, podcastId: podcastId)
            let before = try await store.loadFingerprintEntries(podcastId: podcastId, state: .active)
            #expect(before.count == Self.rowCount, "premise: the fingerprints are there to read")
        }

        try corruptRootPage(of: "ad_copy_fingerprints", at: dir)

        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        await #expect(
            throws: AnalysisStoreError.self,
            """
            A failed fingerprint read returned normally. An empty list reads as "no known ad \
            fingerprints," so a repeated ad already confirmed for this show is not recognized \
            and not skipped.
            """
        ) {
            _ = try await store.loadFingerprintEntries(podcastId: podcastId, state: .active)
        }

        // Control: a different UNcorrupted table reads to completion.
        let knowledge = try await store.loadKnowledgeEntries(podcastId: podcastId, state: .active)
        #expect(knowledge.isEmpty, "no knowledge entries seeded; the point is that the READ completed")
    }

    // MARK: - Scheduler: analysis_jobs
    //
    // `fetchJobsByState("queued")` is how the scheduler finds work to dispatch. A
    // false-empty reads as "no queued jobs" — nothing to do — and analysis
    // stalls. Unlike a display that recovers on the next poll, a scheduler that
    // reads "no rows" and stops does not come back on its own. (Six production
    // callers `try?` this read and would still see the throw as an empty result;
    // this rail pins the STORE, which no longer lies — propagating through those
    // callers' own error handling is a separate scheduler decision, noted below.)

    private func seedQueuedJobs(store: AnalysisStore) async throws {
        for index in 0..<Self.rowCount {
            _ = try await store.insertJob(makeAnalysisJob(
                jobId: "job-\(index)",
                episodeId: "ep-\(index)",
                workKey: "wk-\(index)",
                state: "queued"
            ))
        }
    }

    @Test("a scheduler job read that fails THROWS instead of reporting no queued work")
    func fetchJobsByStateThrowsOnFault() async throws {
        let dir = try makeTempDir(prefix: "nintb-jobs")
        defer { try? FileManager.default.removeItem(at: dir) }

        do {
            let store = try AnalysisStore(directory: dir)
            try await store.migrate()
            try await seedQueuedJobs(store: store)
            let before = try await store.fetchJobsByState("queued")
            #expect(before.count == Self.rowCount, "premise: the queued jobs are there to read")
        }

        try corruptRootPage(of: "analysis_jobs", at: dir)

        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        await #expect(
            throws: AnalysisStoreError.self,
            """
            A failed job read returned normally. An empty list reads as "no queued jobs — \
            nothing to do," and the scheduler stalls; a display recovers on the next poll, a \
            scheduler that stops does not.
            """
        ) {
            _ = try await store.fetchJobsByState("queued")
        }

        // Control: a different UNcorrupted table reads to completion.
        let ads = try await store.fetchAdWindows(assetId: "no-such-asset")
        #expect(ads.isEmpty, "no ad windows seeded; the point is that the READ completed")
    }
}
