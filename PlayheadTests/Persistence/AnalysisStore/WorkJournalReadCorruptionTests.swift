// WorkJournalReadCorruptionTests.swift
// playhead-uzdq.1: read-path hardening for `AnalysisStore.readWorkJournalEntry`.
//
// Fix #3 — a non-UUID `generation_id` used to be silently substituted
// with a fresh `UUID()`, hiding corruption AND breaking the
// `{episode_id, generation_id}` identity orphan recovery joins on.
// The read path now throws `AnalysisStoreError.queryFailed`.
//
// Fix #4 — an unknown `cause` rawValue used to be silently downgraded
// to `.pipelineError`, poisoning cause-taxonomy telemetry whenever the
// enum evolves. The read path now promotes to
// `InternalMissCause.unknown(rawCause)`, a new forward-compat case that
// round-trips the raw string verbatim.
//
// These tests seed rows directly via raw SQLite (so we can inject
// intentionally-invalid strings that the typed Swift API refuses to
// build) and then assert the read path's new behavior.

import Foundation
import SQLite3
import Testing
@testable import Playhead

@Suite("WorkJournal read-path corruption handling (playhead-uzdq.1)")
struct WorkJournalReadCorruptionTests {

    // MARK: - Fix #3: non-UUID generation_id

    @Test("readWorkJournalEntry throws on non-UUID generation_id")
    func readThrowsOnNonUUIDGenerationID() async throws {
        let storeDir = try makeTempDir(prefix: "uzdq1-genid")
        defer { try? FileManager.default.removeItem(at: storeDir) }
        let store = try AnalysisStore(directory: storeDir)
        try await store.migrate()

        // A valid entry already on this {episode, generation} proves the
        // query infrastructure works; we then add a corrupt sibling row
        // so the `fetchWorkJournalEntries` path has to read it and
        // throw.
        let episodeId = "ep-uzdq1-genid"
        let badGenerationID = "not-a-uuid"

        try seedWorkJournalRow(
            in: storeDir,
            id: UUID().uuidString,
            episodeId: episodeId,
            generationID: badGenerationID,
            schedulerEpoch: 1,
            timestamp: 1_000_000,
            eventType: "acquired",
            cause: nil,
            metadata: "{}",
            artifactClass: "scratch"
        )

        await #expect(throws: AnalysisStoreError.self) {
            _ = try await store.fetchWorkJournalEntries(
                episodeId: episodeId,
                generationID: badGenerationID
            )
        }
    }

    @Test("Valid UUID generation_id reads through without error")
    func readSucceedsOnValidUUIDGenerationID() async throws {
        let storeDir = try makeTempDir(prefix: "uzdq1-genid-ok")
        defer { try? FileManager.default.removeItem(at: storeDir) }
        let store = try AnalysisStore(directory: storeDir)
        try await store.migrate()

        let episodeId = "ep-uzdq1-genid-ok"
        let gen = UUID()
        try seedWorkJournalRow(
            in: storeDir,
            id: UUID().uuidString,
            episodeId: episodeId,
            generationID: gen.uuidString,
            schedulerEpoch: 1,
            timestamp: 1_000_000,
            eventType: "acquired",
            cause: nil,
            metadata: "{}",
            artifactClass: "scratch"
        )

        let entries = try await store.fetchWorkJournalEntries(
            episodeId: episodeId,
            generationID: gen.uuidString
        )
        #expect(entries.count == 1)
        #expect(entries.first?.generationID == gen)
    }

    // MARK: - Fix #4: unknown cause rawValue

    @Test("readWorkJournalEntry surfaces an unknown cause as .unknown(rawValue)")
    func readSurfacesUnknownCauseAsUnknownCase() async throws {
        let storeDir = try makeTempDir(prefix: "uzdq1-cause")
        defer { try? FileManager.default.removeItem(at: storeDir) }
        let store = try AnalysisStore(directory: storeDir)
        try await store.migrate()

        let episodeId = "ep-uzdq1-unknown-cause"
        let gen = UUID()
        let futureCause = "futureCauseXYZ"
        try seedWorkJournalRow(
            in: storeDir,
            id: UUID().uuidString,
            episodeId: episodeId,
            generationID: gen.uuidString,
            schedulerEpoch: 1,
            timestamp: 1_000_000,
            eventType: "preempted",
            cause: futureCause,
            metadata: "{}",
            artifactClass: "scratch"
        )

        let entries = try await store.fetchWorkJournalEntries(
            episodeId: episodeId,
            generationID: gen.uuidString
        )
        #expect(entries.count == 1)
        let entry = try #require(entries.first)
        #expect(entry.cause == .unknown(futureCause))
        // The old behaviour was to silently downgrade to `.pipelineError`
        // — assert we no longer do that so future refactors don't
        // regress the fix.
        #expect(entry.cause != .pipelineError)
    }

    @Test(".unknown(s).rawValue round-trips verbatim through a rewrite")
    func unknownCauseRoundTripsThroughRewrite() async throws {
        let storeDir = try makeTempDir(prefix: "uzdq1-roundtrip")
        defer { try? FileManager.default.removeItem(at: storeDir) }
        let store = try AnalysisStore(directory: storeDir)
        try await store.migrate()

        let episodeId = "ep-uzdq1-roundtrip"
        let gen = UUID()
        let futureCause = "futureCauseXYZ"

        // First pass: seed the row with the raw future-cause string.
        try seedWorkJournalRow(
            in: storeDir,
            id: UUID().uuidString,
            episodeId: episodeId,
            generationID: gen.uuidString,
            schedulerEpoch: 1,
            timestamp: 1_000_000,
            eventType: "preempted",
            cause: futureCause,
            metadata: "{}",
            artifactClass: "scratch"
        )
        let first = try await store.fetchWorkJournalEntries(
            episodeId: episodeId,
            generationID: gen.uuidString
        )
        let firstCause = try #require(first.first?.cause)
        #expect(firstCause == .unknown(futureCause))
        #expect(firstCause.rawValue == futureCause)

        // Second pass: write `.unknown(futureCause)` back through the
        // struct API and confirm the on-disk raw string is still the
        // same bare cause token.
        let rewriteEntry = WorkJournalEntry(
            id: UUID().uuidString,
            episodeId: episodeId,
            generationID: gen,
            schedulerEpoch: 2,
            timestamp: 2_000_000,
            eventType: .preempted,
            cause: firstCause,
            metadata: "{}",
            artifactClass: .scratch
        )
        try await store.appendWorkJournalEntry(rewriteEntry)

        let rewrittenRaw = try readCauseRaw(
            in: storeDir,
            entryId: rewriteEntry.id
        )
        #expect(rewrittenRaw == futureCause)

        // Read it back via the typed path too — should land back on
        // `.unknown(futureCause)`.
        let all = try await store.fetchWorkJournalEntries(
            episodeId: episodeId,
            generationID: gen.uuidString
        )
        let rewrittenRead = all.first(where: { $0.id == rewriteEntry.id })?.cause
        #expect(rewrittenRead == .unknown(futureCause))
    }

    // MARK: - Integration-style: every canonical cause round-trips

    @Test("Every canonical InternalMissCause round-trips write -> read")
    func everyCanonicalCauseRoundTrips() async throws {
        let storeDir = try makeTempDir(prefix: "uzdq1-canonical")
        defer { try? FileManager.default.removeItem(at: storeDir) }
        let store = try AnalysisStore(directory: storeDir)
        try await store.migrate()

        // Write one row per canonical case, then read them back and
        // assert byte-for-byte equality on the `cause` column.
        var expectedById: [String: InternalMissCause] = [:]
        for (index, cause) in InternalMissCause.allCases.enumerated() {
            let episodeId = "ep-uzdq1-canon-\(index)"
            let gen = UUID()
            let entry = WorkJournalEntry(
                id: UUID().uuidString,
                episodeId: episodeId,
                generationID: gen,
                schedulerEpoch: 1,
                timestamp: Double(1_000_000 + index),
                eventType: .preempted,
                cause: cause,
                metadata: "{}",
                artifactClass: .scratch
            )
            expectedById[entry.id] = cause
            try await store.appendWorkJournalEntry(entry)

            let readBack = try await store.fetchWorkJournalEntries(
                episodeId: episodeId,
                generationID: gen.uuidString
            )
            let got = try #require(readBack.first(where: { $0.id == entry.id }))
            #expect(got.cause == cause, "case \(cause.rawValue) did not round-trip")
            #expect(got.cause?.rawValue == cause.rawValue)
        }
        // Sanity: we actually exercised 16 distinct cases.
        #expect(expectedById.count == 16)
    }

    @Test("InternalMissCause.unknown round-trips through single-value Codable")
    func unknownCauseRoundTripsThroughCodable() throws {
        let original = InternalMissCause.unknown("futureCauseXYZ")
        let encoded = try JSONEncoder().encode(original)
        // Expect a bare JSON string — no enum-case wrapping.
        let asString = String(data: encoded, encoding: .utf8)
        #expect(asString == "\"futureCauseXYZ\"")

        let decoded = try JSONDecoder().decode(
            InternalMissCause.self,
            from: encoded
        )
        #expect(decoded == original)
    }

    @Test("InternalMissCause.allCases omits .unknown and has exactly 16 entries")
    func allCasesOmitsUnknownSentinel() {
        #expect(InternalMissCause.allCases.count == 16)
        for c in InternalMissCause.allCases {
            // None of the canonical cases should equal any `.unknown(_)`
            // value — the sentinel is strictly a read-path creation.
            if case .unknown = c {
                Issue.record("\(c) should not appear in allCases")
            }
        }
    }

    // MARK: - Raw SQLite helpers

    /// Inserts a single row into `work_journal` by opening the sqlite
    /// file directly. Exists so tests can inject strings the typed
    /// Swift API (`WorkJournalEntry`) refuses to construct — notably a
    /// non-UUID `generation_id` and a future-schema `cause` token.
    private func seedWorkJournalRow(
        in directory: URL,
        id: String,
        episodeId: String,
        generationID: String,
        schedulerEpoch: Int,
        timestamp: Double,
        eventType: String,
        cause: String?,
        metadata: String,
        artifactClass: String
    ) throws {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READWRITE, nil) == SQLITE_OK else {
            throw NSError(
                domain: "SeedWorkJournal",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "open failed"]
            )
        }
        defer { sqlite3_close_v2(db) }

        let sql = """
            INSERT INTO work_journal
            (id, episode_id, generation_id, scheduler_epoch, timestamp,
             event_type, cause, metadata, artifact_class)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            let msg = sqlite3_errmsg(db).map { String(cString: $0) } ?? "unknown"
            throw NSError(
                domain: "SeedWorkJournal",
                code: 2,
                userInfo: [NSLocalizedDescriptionKey: "prepare: \(msg)"]
            )
        }
        defer { sqlite3_finalize(stmt) }

        // SQLITE_TRANSIENT (-1) tells sqlite to copy the string —
        // required for locally-scoped Swift `String`s that may deallocate
        // before `sqlite3_step` runs.
        let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

        sqlite3_bind_text(stmt, 1, id, -1, transient)
        sqlite3_bind_text(stmt, 2, episodeId, -1, transient)
        sqlite3_bind_text(stmt, 3, generationID, -1, transient)
        sqlite3_bind_int(stmt, 4, Int32(schedulerEpoch))
        sqlite3_bind_double(stmt, 5, timestamp)
        sqlite3_bind_text(stmt, 6, eventType, -1, transient)
        if let cause {
            sqlite3_bind_text(stmt, 7, cause, -1, transient)
        } else {
            sqlite3_bind_null(stmt, 7)
        }
        sqlite3_bind_text(stmt, 8, metadata, -1, transient)
        sqlite3_bind_text(stmt, 9, artifactClass, -1, transient)

        let rc = sqlite3_step(stmt)
        guard rc == SQLITE_DONE else {
            let msg = sqlite3_errmsg(db).map { String(cString: $0) } ?? "unknown"
            throw NSError(
                domain: "SeedWorkJournal",
                code: 3,
                userInfo: [NSLocalizedDescriptionKey: "step (\(rc)): \(msg)"]
            )
        }
    }

    /// Read the raw `cause` column for a given row id.
    private func readCauseRaw(in directory: URL, entryId: String) throws -> String? {
        let dbURL = directory.appendingPathComponent("analysis.sqlite")
        var db: OpaquePointer?
        guard sqlite3_open_v2(dbURL.path, &db, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            throw NSError(domain: "ReadCauseRaw", code: 1)
        }
        defer { sqlite3_close_v2(db) }

        let sql = "SELECT cause FROM work_journal WHERE id = ?"
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else {
            throw NSError(domain: "ReadCauseRaw", code: 2)
        }
        defer { sqlite3_finalize(stmt) }

        let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
        sqlite3_bind_text(stmt, 1, entryId, -1, transient)

        let rc = sqlite3_step(stmt)
        guard rc == SQLITE_ROW else {
            return nil
        }
        if sqlite3_column_type(stmt, 0) == SQLITE_NULL {
            return nil
        }
        guard let cstr = sqlite3_column_text(stmt, 0) else { return nil }
        return String(cString: cstr)
    }
}
