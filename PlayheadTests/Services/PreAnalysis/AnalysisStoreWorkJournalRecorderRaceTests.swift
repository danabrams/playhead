// AnalysisStoreWorkJournalRecorderRaceTests.swift
//
// File-name note: the `RaceTests` suffix is kept for git-history
// continuity with the cycle-31 L1 finding it traces back to. The
// actual scope is the `fetchLatestJobForEpisode` SQL ORDER BY
// contract, NOT a concurrent-writer race — see suite docstring below.
//
// L1 (cycle-31 follow-up): pin the recorder's `fetchLatestJobForEpisode`
// resolution contract.
//
// `AnalysisStoreWorkJournalRecorder.persist(...)` resolves the live
// `{generationID, schedulerEpoch}` for the episode by calling
// `fetchLatestJobForEpisode(_:)` at write time, NOT at outcome-arm time.
// That choice is deliberate (the scheduler's outcome arms commit state
// before invoking the recorder, so a journal-append failure cannot
// corrupt job state), but it has two race-class implications a test
// suite should pin.
//
// Scope of *these* tests: we pin the deterministic SQL ordering and
// guard contracts that `persist()` relies on. We do NOT exercise a
// concurrent writer race (no parallel insert + persist). The two
// guarantees pinned here are:
//
//   1. **Latest-wins (SQL ORDER BY contract).** When two
//      `analysis_jobs` rows exist for the same episode with distinct
//      `updatedAt`, the recorder MUST bind to the row whose
//      `updatedAt` is largest. The store's SQL
//      (`ORDER BY updatedAt DESC, rowid DESC LIMIT 1`) is the contract;
//      this test pins the ordering, not the concurrency.
//
//   2. **Skip-on-corrupt.** If the latest row has a non-UUID
//      `generationID` (the column DEFAULT is `''`, and a future rollback
//      could leave one stranded), the recorder must skip rather than
//      write an orphan row whose `generation_id` would never match a
//      valid `{episode_id, generation_id}` lookup downstream.
//
// Both behaviors are guarded by warn-logs and silent skips inside
// `persist()`; the tests below pin the observable side-effects on the
// `work_journal` table.

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisStoreWorkJournalRecorder fetchLatestJobForEpisode contract")
struct AnalysisStoreWorkJournalRecorderRaceTests {

    @Test("recordFinalized writes a journal row with the latest generationID")
    func writesEntryWithLatestGenerationID() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-race-happy"
        let generationID = UUID().uuidString
        let job = makeAnalysisJob(
            jobId: "j-happy",
            episodeId: episodeId,
            generationID: generationID
        )
        let inserted = try await store.insertJob(job)
        #expect(inserted == true,
                "Premise: the single analysis_jobs row must insert cleanly. A silent INSERT OR IGNORE dedup against bleed-over state would make the journal lookup a meaningless test of nothing.")

        let recorder = AnalysisStoreWorkJournalRecorder(store: store)
        await recorder.recordFinalized(episodeId: episodeId)

        let entries = try await store.fetchWorkJournalEntries(
            episodeId: episodeId,
            generationID: generationID
        )
        #expect(entries.count == 1,
                "Expected exactly one journal entry for the latest generationID; got \(entries.count)")
        #expect(entries.first?.eventType == .finalized,
                "Journal entry should be tagged .finalized")
        #expect(entries.first?.generationID.uuidString == generationID,
                "Journal entry must carry the same generationID returned by fetchLatestJobForEpisode")
    }

    @Test("recordFinalized binds to the most-recent updatedAt row when multiple jobs exist (SQL ORDER BY contract)")
    func bindsToLatestUpdatedAtWhenMultipleJobsExist() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-race-latest-wins"
        let oldGenID = UUID().uuidString
        let newGenID = UUID().uuidString
        let now = Date().timeIntervalSince1970

        // Insert older row first.
        let older = makeAnalysisJob(
            jobId: "j-older",
            episodeId: episodeId,
            createdAt: now - 60,
            updatedAt: now - 60,
            generationID: oldGenID
        )
        let olderInserted = try await store.insertJob(older)
        #expect(olderInserted == true,
                "Older row must insert cleanly; if INSERT OR IGNORE silently dedup'd, the rest of the test is meaningless")

        // Insert a newer row for the same episode (mirrors what a fresh
        // schedule cycle would produce after re-leasing). Distinct
        // `workKey` is required so `INSERT OR IGNORE` does not silently
        // dedup against the older row — we assert the boolean return
        // value to pin that invariant.
        let newer = makeAnalysisJob(
            jobId: "j-newer",
            episodeId: episodeId,
            workKey: "wk-race-newer",
            createdAt: now,
            updatedAt: now,
            generationID: newGenID
        )
        let newerInserted = try await store.insertJob(newer)
        #expect(newerInserted == true,
                "Newer row must insert as a NEW row (not silently IGNORE'd by workKey collision); the multi-row precondition is the entire test premise")

        let recorder = AnalysisStoreWorkJournalRecorder(store: store)
        await recorder.recordFinalized(episodeId: episodeId)

        // Journal must bind to the newer generationID — that's the
        // `ORDER BY updatedAt DESC, rowid DESC LIMIT 1` contract that
        // `fetchLatestJobForEpisode` enforces and that `persist()`
        // relies on.
        let newEntries = try await store.fetchWorkJournalEntries(
            episodeId: episodeId,
            generationID: newGenID
        )
        let oldEntries = try await store.fetchWorkJournalEntries(
            episodeId: episodeId,
            generationID: oldGenID
        )
        #expect(newEntries.count == 1,
                "Recorder must bind the journal entry to the latest-updatedAt row's generationID")
        #expect(oldEntries.isEmpty,
                "Recorder must NOT write a journal entry against an older generationID")
    }

    @Test("recordFinalized is a no-op when no analysis_jobs row exists for the episode")
    func skipsWhenNoJobRowExists() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-race-no-row"

        let recorder = AnalysisStoreWorkJournalRecorder(store: store)
        // Should warn-log and return without throwing.
        await recorder.recordFinalized(episodeId: episodeId)

        // Strong negative: scan the entire work_journal tail and confirm
        // *no* row references this episode. The earlier sentinel-genID
        // lookup couldn't distinguish "skipped" from "lookup miss";
        // this scan asserts the absence directly.
        let recent = try await store.fetchRecentWorkJournalEntries(limit: 100)
        let matching = recent.filter { $0.episodeId == episodeId }
        #expect(matching.isEmpty,
                "Recorder must NOT write any work_journal row for an episode with no analysis_jobs row; found \(matching.count)")
    }

    @Test("recordFinalized skips when the latest job has a non-UUID generationID")
    func skipsWhenGenerationIDIsNotAUUID() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-race-bad-genid"
        // `generationID` defaults to `''` on legacy rows. Anything that
        // fails `UUID(uuidString:)` must trip the second guard in
        // `persist()`.
        let job = makeAnalysisJob(
            jobId: "j-bad-genid",
            episodeId: episodeId,
            generationID: ""
        )
        let inserted = try await store.insertJob(job)
        #expect(inserted == true,
                "Premise: the bad-genID row must actually be present. A silent INSERT OR IGNORE dedup would make the negative scan vacuously pass.")

        let recorder = AnalysisStoreWorkJournalRecorder(store: store)
        await recorder.recordFinalized(episodeId: episodeId)

        // Strong negative: as in the no-row test, scan the full tail
        // rather than querying by sentinel generationID. This avoids
        // false-pass risk when the lookup-by-generationID API would
        // never return rows whose stored value isn't a UUID anyway.
        let recent = try await store.fetchRecentWorkJournalEntries(limit: 100)
        let matching = recent.filter { $0.episodeId == episodeId }
        #expect(matching.isEmpty,
                "Recorder must NOT write a journal entry when the live job's generationID is not a valid UUID — orphan rows would never match downstream {episode_id, generation_id} lookups; found \(matching.count)")
    }
}
