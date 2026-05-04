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

    /// review/v0.5-head-polish missing-test: an interleaved-writer
    /// scenario against the `fetchLatestJobForEpisode` ORDER-BY-DESC
    /// contract. The earlier tests in this suite pin sequential
    /// ordering; this one pins what happens when many recorder calls
    /// interleave with many `analysis_jobs` upserts on the same
    /// episode.
    ///
    /// **Concurrency model — read this before tightening the test.**
    /// `AnalysisStore` is an `actor`, so `insertJob(_:)` and the
    /// recorder's internal `fetchLatestJobForEpisode` calls are
    /// serialized through the actor's executor. The "race" exercised
    /// here is therefore *interleaved* execution, not lock-step
    /// concurrency: arbitrary suspension points between actor hops
    /// produce unpredictable orderings of insert/fetch/append, and
    /// that's the regression surface we're pinning. A future SQL
    /// refactor that drops the actor wrapper (replacing it with
    /// explicit locks or a sync API) would shift this from
    /// "interleaved" to "truly concurrent" — at that point this test's
    /// `writerCount` should be raised and a stress runner added.
    ///
    /// The recorder is best-effort by design (a stale generationID is
    /// acceptable diagnostics-loss), so we don't assert "every recorder
    /// call sees the absolute latest row." We DO assert four
    /// invariants that must hold under interleaved load:
    ///
    ///   1. **No crash / no thrown error escapes the recorder.** The
    ///      recorder swallows store errors at the boundary — under
    ///      interleaved SQLite writes, none of those errors should
    ///      leak.
    ///   2. **No orphan rows.** Every journal entry's generationID
    ///      must match a row that actually exists in `analysis_jobs`
    ///      at the end of the test (a stale write isn't an orphan —
    ///      the row was real at write time and we never delete jobs).
    ///   3. **Bounded write count (upper).** Number of journal rows ≤
    ///      number of recorder calls. A future SQL refactor that
    ///      introduces a retry-on-busy loop without idempotency would
    ///      inflate this count.
    ///   4. **Forward progress (lower).** At least one journal entry
    ///      must be appended. Without this, a regression that breaks
    ///      `recordFinalized` so it never writes (e.g. a future
    ///      schema mismatch swallowed at the boundary) would silently
    ///      satisfy invariants 1–3.
    @Test("concurrent recordFinalized + insertJob produces no orphans or duplicates")
    func concurrentRecorderAndInsertProducesNoOrphans() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-race-concurrent"
        let recorder = AnalysisStoreWorkJournalRecorder(store: store)

        // Seed one job so the very first recorder call has something
        // to bind to.
        let seedGenID = UUID().uuidString
        _ = try await store.insertJob(makeAnalysisJob(
            jobId: "j-seed",
            episodeId: episodeId,
            workKey: "wk-seed",
            generationID: seedGenID
        ))

        let writerCount = 8
        var validGenerationIDs: Set<String> = [seedGenID]

        // Bound the test deterministically: writers and recorders run in
        // matched pairs.
        await withTaskGroup(of: Void.self) { group in
            for i in 0..<writerCount {
                let genID = UUID().uuidString
                validGenerationIDs.insert(genID)
                let workKey = "wk-race-\(i)"
                let jobId = "j-race-\(i)"

                let job = makeAnalysisJob(
                    jobId: jobId,
                    episodeId: episodeId,
                    workKey: workKey,
                    generationID: genID
                )
                group.addTask {
                    // Writer: insert a fresh job row.
                    _ = try? await store.insertJob(job)
                }
                group.addTask {
                    // Recorder: append a finalized row whatever the
                    // latest job is at SQL-fetch time.
                    await recorder.recordFinalized(episodeId: episodeId)
                }
            }
        }

        // Invariant 2: every journal entry references a generationID
        // that we intended to insert (i.e. no synthetic / mutated IDs
        // crept into the journal). validGenerationIDs is the set of
        // every UUID handed to insertJob in this test.
        let recent = try await store.fetchRecentWorkJournalEntries(limit: 200)
        let entries = recent.filter { $0.episodeId == episodeId }

        for entry in entries {
            #expect(
                validGenerationIDs.contains(entry.generationID.uuidString),
                "Orphan journal row: generationID=\(entry.generationID.uuidString) was never inserted by this test"
            )
        }

        // Invariant 3: bounded write count (upper).
        #expect(
            entries.count <= writerCount,
            "Recorder produced more journal entries (\(entries.count)) than recorder invocations (\(writerCount)) — implies a duplicate write somewhere"
        )

        // Invariant 4: forward progress (lower bound). The seed job
        // is inserted synchronously *before* the task group begins,
        // so every recorder call has at least the seed row to bind
        // to. A regression where `recordFinalized` is silently a
        // no-op would still satisfy the upper bound and the orphan
        // check, but would fail this lower bound and surface the
        // regression.
        #expect(
            entries.count >= 1,
            "Recorder produced ZERO journal entries despite \(writerCount) recorder calls and a pre-seeded job — recordFinalized appears to be a silent no-op"
        )

        // Sanity: validGenerationIDs is a snapshot of every UUID we
        // intended to insert; this guards against the test being
        // accidentally trivialised by a Swift collection bug.
        #expect(validGenerationIDs.count == writerCount + 1)
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
