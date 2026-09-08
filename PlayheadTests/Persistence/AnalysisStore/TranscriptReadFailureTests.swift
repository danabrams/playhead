// TranscriptReadFailureTests.swift
// playhead-0bpb0 — Dan, 2026-09-08: "During the midroll, I tried to load the
// transcript and it said the transcript could not be found."
//
// The data was there. Pull 2026-09-08, asset F88B3C34: 2,219 transcript chunks
// covering 3.8 s -> 1796.0 s against an episode duration of 1796.2, and he had
// marked ads through that same transcript 30 minutes earlier and would do it
// again 15 minutes later. A wrong key would have been wrong all three times.
//
// WHAT THIS SUITE PINS. `fetchTranscriptChunks` used to read its rows with
//
//     while sqlite3_step(stmt) == SQLITE_ROW { ... }
//
// which ends identically on SQLITE_DONE (there are no more rows) and on every
// failure code there is (we could not read them). The two sibling reads in the
// same snapshot — `fetchAdWindows` and `fetchDecodedSpansIncludingUserVetoed` —
// have always thrown on a non-DONE terminal code. The transcript, the one row
// set the listener actually reads, was the one that did not, so a failed read
// arrived at the surface as an empty episode and the surface said so.
//
// HOW THE FAILURE IS FORCED. An interior page of the database file is zeroed
// after the rows are committed and checkpointed. SQLite reaches it through the
// table b-tree and reports a malformed image. This is not a contrived error
// code: it is what an I/O fault or a torn write looks like from inside the
// read, and this device has 15 MetricKit `disk_write_exception` records, one of
// them timestamped 15:46 inside the very session Dan is describing.
//
// The read is `ORDER BY chunkIndex` with no index on that column, so SQLite
// sorts before returning row one and the failure lands on the FIRST step. That
// is why the old code returned an EMPTY list rather than a short one, and why
// the listener got the "no transcript yet" screen rather than a truncated
// transcript.

import Foundation
import SQLite3
import Testing
@testable import Playhead

@Suite("playhead-0bpb0 — a transcript that cannot be READ is not a transcript that is ABSENT")
struct TranscriptReadFailureTests {

    // MARK: - Fixture

    /// Rows enough to push the table past a single page, so zeroing an
    /// interior page lands inside the table rather than in free space.
    private static let rowCount = 600

    private func seed(store: AnalysisStore, assetId: String) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: "ep-0bpb0",
            assetFingerprint: assetId,
            weakFingerprint: nil,
            sourceURL: "file:///0bpb0/episode.mp3",
            featureCoverageEndTime: 1_796.2,
            fastTranscriptCoverageEndTime: 1_796.2,
            confirmedAdCoverageEndTime: nil,
            analysisState: "completeAdScanPartial",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 1_796.2,
            episodeTitle: "0bpb0 fixture"
        ))
        let chunks = (0..<Self.rowCount).map { index in
            TranscriptChunk(
                id: "c-\(index)",
                analysisAssetId: assetId,
                segmentFingerprint: "seg-\(index)",
                chunkIndex: index,
                startTime: Double(index) * 3.0,
                endTime: Double(index) * 3.0 + 3.0,
                // Long enough that 600 rows span many pages.
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

    /// Fold the WAL into the main file and zero one interior page of it.
    /// Page 1 carries the schema and is left alone on purpose: corrupting it
    /// would fail at `prepare`, which throws already and would prove nothing
    /// about the step loop this suite is about.
    private func corruptInteriorPage(at directory: URL) throws {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")

        var raw: OpaquePointer?
        #expect(sqlite3_open(dbURL.path, &raw) == SQLITE_OK)
        _ = sqlite3_exec(raw, "PRAGMA wal_checkpoint(TRUNCATE)", nil, nil, nil)
        sqlite3_close(raw)

        let handle = try FileHandle(forUpdating: dbURL)
        defer { try? handle.close() }
        let header = try handle.read(upToCount: 100) ?? Data()
        #expect(header.count == 100, "the fixture must have a readable SQLite header")
        // Bytes 16..17, big-endian, are the page size; the value 1 means 65536.
        let raw16 = Int(header[16]) << 8 | Int(header[17])
        let pageSize = raw16 == 1 ? 65_536 : raw16
        let size = Int(try FileHandle(forReadingFrom: dbURL).seekToEnd())
        let pageCount = size / pageSize
        #expect(pageCount > 4, "the fixture must span several pages; got \(pageCount)")

        // Two thirds in: past the schema and the indexes, inside the table.
        let victim = max(2, (pageCount * 2) / 3)
        try handle.seek(toOffset: UInt64((victim - 1) * pageSize))
        try handle.write(contentsOf: Data(repeating: 0, count: pageSize))
        try handle.synchronize()
    }

    // MARK: - The rail

    @Test("a transcript read that fails THROWS instead of reporting an empty episode")
    func failedReadThrows() async throws {
        let dir = try makeTempDir(prefix: "0bpb0-read")
        defer { try? FileManager.default.removeItem(at: dir) }
        let assetId = "A-0bpb0"

        do {
            let store = try AnalysisStore(directory: dir)
            try await store.migrate()
            try await seed(store: store, assetId: assetId)

            // Premise: the rows are readable before the fault. Without this the
            // test below could pass on a fixture that never had a transcript.
            let before = try await store.fetchTranscriptChunks(assetId: assetId)
            #expect(before.count == Self.rowCount, "premise: \(Self.rowCount) rows are there to read")
        }

        try corruptInteriorPage(at: dir)

        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        await #expect(
            throws: AnalysisStoreError.self,
            """
            A failed transcript read returned normally. Before playhead-0bpb0 it returned an \
            EMPTY array, which the transcript surface renders as "No transcript yet" — the \
            sentence that told Dan a complete 1796-second transcript did not exist.
            """
        ) {
            _ = try await store.fetchTranscriptChunks(assetId: assetId)
        }
    }

    @Test("the sibling reads in the same snapshot have always thrown, and still do")
    func siblingReadsThrowToo() async throws {
        // The value of this test is the CONTRAST. `fetchAdWindows` reads the
        // same database through the same connection and reports the same fault
        // as an error. That is what made the transcript's silence a defect
        // rather than a house style: two of the snapshot's three reads were
        // already honest.
        let dir = try makeTempDir(prefix: "0bpb0-sibling")
        defer { try? FileManager.default.removeItem(at: dir) }
        let assetId = "A-0bpb0-sib"

        do {
            let store = try AnalysisStore(directory: dir)
            try await store.migrate()
            try await seed(store: store, assetId: assetId)
        }

        try corruptInteriorPage(at: dir)

        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        await #expect(throws: AnalysisStoreError.self) {
            _ = try await store.fetchAdWindows(assetId: assetId)
        }
    }
}
