// WorkJournalTailFetchTests.swift
// Verifies the diagnostics tail-fetch helper added to `AnalysisStore`.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// Spec contract under test:
//   `fetchRecentWorkJournalEntries(limit:)` returns AT MOST `limit` rows,
//   ordered newest-first by `(timestamp DESC, rowid DESC)`. The returned
//   set is the absolute-newest `limit` rows across ALL episodes /
//   generations — there is no per-episode partitioning.

import Foundation
import Testing

@testable import Playhead

@Suite("AnalysisStore.fetchRecentWorkJournalEntries (playhead-ghon)")
struct WorkJournalTailFetchTests {

    // MARK: - Helpers

    private func appendEntries(
        _ store: AnalysisStore,
        episodeId: String,
        startTimestamp: Double,
        count: Int
    ) async throws {
        // Seed an analysis_jobs row first — work_journal has FK only on
        // implicit appends from the lease lifecycle, but the
        // free-standing append used here doesn't require one.
        for i in 0..<count {
            let entry = WorkJournalEntry(
                id: UUID().uuidString,
                episodeId: episodeId,
                generationID: UUID(),
                schedulerEpoch: 0,
                timestamp: startTimestamp + Double(i),
                eventType: .acquired,
                cause: nil,
                metadata: "{}",
                artifactClass: .scratch
            )
            try await store.appendWorkJournalEntry(entry)
        }
    }

    // MARK: - Empty store

    @Test("returns empty array on empty store")
    func emptyStore() async throws {
        let store = try await makeTestStore()
        let rows = try await store.fetchRecentWorkJournalEntries(limit: 50)
        #expect(rows.isEmpty)
    }

    // MARK: - Cap honored, newest-first ordering

    @Test("limit caps the row count")
    func limitCapsRows() async throws {
        let store = try await makeTestStore()
        try await appendEntries(store, episodeId: "ep-1", startTimestamp: 1_000, count: 10)
        let rows = try await store.fetchRecentWorkJournalEntries(limit: 3)
        #expect(rows.count == 3)
    }

    @Test("rows are returned newest-first by timestamp")
    func newestFirst() async throws {
        let store = try await makeTestStore()
        try await appendEntries(store, episodeId: "ep-1", startTimestamp: 1_000, count: 5)
        let rows = try await store.fetchRecentWorkJournalEntries(limit: 5)
        let stamps = rows.map(\.timestamp)
        #expect(stamps == stamps.sorted(by: >))
        #expect(rows.first?.timestamp == 1_004)
        #expect(rows.last?.timestamp == 1_000)
    }

    // MARK: - Cross-episode mixing

    @Test("rows from multiple episodes are interleaved by timestamp")
    func crossEpisodeMixing() async throws {
        let store = try await makeTestStore()
        try await appendEntries(store, episodeId: "ep-old", startTimestamp: 1_000, count: 2)
        try await appendEntries(store, episodeId: "ep-new", startTimestamp: 2_000, count: 2)
        let rows = try await store.fetchRecentWorkJournalEntries(limit: 10)
        #expect(rows.count == 4)
        // First two rows should be from "ep-new" (timestamps 2001, 2000).
        #expect(rows.prefix(2).allSatisfy { $0.episodeId == "ep-new" })
    }

    // MARK: - Edge: limit == 0

    @Test("limit == 0 returns empty array (no SQL roundtrip required)")
    func zeroLimit() async throws {
        let store = try await makeTestStore()
        try await appendEntries(store, episodeId: "ep-1", startTimestamp: 1_000, count: 5)
        let rows = try await store.fetchRecentWorkJournalEntries(limit: 0)
        #expect(rows.isEmpty)
    }
}
