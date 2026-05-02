// AnalysisStoreReviewFollowupTests.swift
// Review-followup (csp / persistence L2 + L3): pin two contracts that
// were previously test-uncovered:
//
//   - `vacuumInto(destinationURL:)` must produce a valid sqlite file
//     that can be opened independently and round-trip a SELECT (L2).
//   - The `assetSelectColumns` ordering contract — the SELECT column
//     list must agree with `readAsset`'s field-by-field decode (L3).
//     A future migration that appends a column to one side without
//     updating the other would silently shift indices and corrupt
//     decode.

#if DEBUG

import Foundation
import Testing
import SQLite3
@testable import Playhead

@Suite("AnalysisStore review-followup csp")
struct AnalysisStoreReviewFollowupTests {

    // MARK: - vacuumInto (persistence L2)

    @Test("vacuumInto produces a valid standalone sqlite file")
    func vacuumIntoProducesValidFile() async throws {
        let store = try await makeTestStore()

        // Seed a known row so the snapshot has something to verify.
        let assetId = "asset-vacuum-l2"
        let episodeId = "ep-vacuum-l2"
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "fp-vacuum",
            weakFingerprint: nil,
            sourceURL: "file:///vacuum.m4a",
            featureCoverageEndTime: 12.5,
            fastTranscriptCoverageEndTime: 8.0,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 300
        ))

        // Snapshot into a temp directory. The destination file must
        // not exist beforehand (vacuumInto's contract).
        let dir = try makeTempDir(prefix: "VacuumIntoSmoke")
        let destURL = dir.appendingPathComponent("snapshot.sqlite")

        try await store.vacuumInto(destinationURL: destURL)

        // The file must exist and be non-empty.
        let attrs = try FileManager.default.attributesOfItem(atPath: destURL.path)
        let size = (attrs[.size] as? NSNumber)?.intValue ?? 0
        #expect(size > 0, "vacuumInto produced an empty file at \(destURL.path)")

        // Open the file with raw sqlite3 and run a sanity SELECT —
        // proves the bytes are a valid sqlite DB and the seeded row
        // round-tripped through VACUUM INTO.
        var rawDB: OpaquePointer?
        let openRC = sqlite3_open_v2(destURL.path, &rawDB, SQLITE_OPEN_READONLY, nil)
        defer { if rawDB != nil { sqlite3_close(rawDB) } }
        try #require(openRC == SQLITE_OK,
                     "sqlite3_open_v2 on snapshot returned rc=\(openRC)")

        var stmt: OpaquePointer?
        let prepareRC = sqlite3_prepare_v2(
            rawDB,
            "SELECT id, episodeId FROM analysis_assets WHERE id = ?",
            -1,
            &stmt,
            nil
        )
        defer { if stmt != nil { sqlite3_finalize(stmt) } }
        try #require(prepareRC == SQLITE_OK,
                     "sqlite3_prepare_v2 returned rc=\(prepareRC)")

        sqlite3_bind_text(stmt, 1, assetId, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self))
        let stepRC = sqlite3_step(stmt)
        try #require(stepRC == SQLITE_ROW, "expected one row, got rc=\(stepRC)")

        let readId = String(cString: sqlite3_column_text(stmt, 0))
        let readEp = String(cString: sqlite3_column_text(stmt, 1))
        #expect(readId == assetId, "snapshot lost the seeded asset id")
        #expect(readEp == episodeId, "snapshot lost the seeded episode id")
    }

    // MARK: - assetSelectColumns ordering (persistence L3)

    @Test("Asset round-trips field-by-field through the assetSelectColumns ORDER contract")
    func assetColumnOrderingRoundTrips() async throws {
        // The ordering contract under test: the column list in
        // `assetSelectColumns` (private) must match `readAsset`'s
        // index-positional decode. We exercise it indirectly via the
        // public `insertAsset` + `fetchAsset(id:)` path — both go
        // through the same column list, so a mismatch surfaces as a
        // round-trip field corruption.
        //
        // Set every nullable column to a non-default value so a swap
        // between any two same-typed columns shows up. (E.g. if
        // `featureCoverageEndTime` and `fastTranscriptCoverageEndTime`
        // were transposed, the original test that left
        // fastTranscript=nil would be silently green.)
        // NOTE: `insertAsset` does NOT write `terminalReason` — that
        // column is populated separately by `markAssetTerminal`. So we
        // exercise the ordering contract for the 15 columns that DO go
        // through the insert path. `terminalReason` round-trips on a
        // separate column index path that the SELECT decode also has
        // to keep aligned, but proving _that_ would require a separate
        // markAssetTerminal call in the test.
        //
        // Cycle-30 M-1: `finalPassCoverageEndTime` was added to the
        // insert path in this cycle (previously `insertAsset` silently
        // dropped non-nil values from the parameter). It is asserted
        // here as the 15th bound column.
        let store = try await makeTestStore()
        let original = AnalysisAsset(
            id: "asset-roundtrip",
            episodeId: "ep-roundtrip",
            assetFingerprint: "fp-roundtrip",
            weakFingerprint: "weak-fp-roundtrip",
            sourceURL: "file:///roundtrip.m4a",
            featureCoverageEndTime: 11.0,
            fastTranscriptCoverageEndTime: 22.0,
            confirmedAdCoverageEndTime: 33.0,
            analysisState: "transcribing",
            analysisVersion: 7,
            capabilitySnapshot: "{\"thermal\":\"nominal\"}",
            artifactClass: .media,
            episodeDurationSec: 999.5,
            episodeTitle: "Roundtrip Episode",
            finalPassCoverageEndTime: 44.0
        )
        try await store.insertAsset(original)

        let fetched = try #require(await store.fetchAsset(id: original.id),
                                   "round-trip asset must be readable")

        #expect(fetched.id == original.id)
        #expect(fetched.episodeId == original.episodeId)
        #expect(fetched.assetFingerprint == original.assetFingerprint)
        #expect(fetched.weakFingerprint == original.weakFingerprint)
        #expect(fetched.sourceURL == original.sourceURL)
        #expect(fetched.featureCoverageEndTime == original.featureCoverageEndTime)
        #expect(fetched.fastTranscriptCoverageEndTime == original.fastTranscriptCoverageEndTime)
        #expect(fetched.confirmedAdCoverageEndTime == original.confirmedAdCoverageEndTime)
        #expect(fetched.analysisState == original.analysisState)
        #expect(fetched.analysisVersion == original.analysisVersion)
        #expect(fetched.capabilitySnapshot == original.capabilitySnapshot)
        #expect(fetched.artifactClass == original.artifactClass)
        #expect(fetched.episodeDurationSec == original.episodeDurationSec)
        #expect(fetched.episodeTitle == original.episodeTitle)
        #expect(fetched.finalPassCoverageEndTime == original.finalPassCoverageEndTime,
                "Cycle-30 M-1: insertAsset must bind finalPassCoverageEndTime")
    }

    // MARK: - markFinalPassJobFailed retry-runaway invariant (cycle 8 M2)

    /// Pins the load-bearing IN-clause asymmetry documented at
    /// `AnalysisStore.swift:7160-7180`: `markFinalPassJobFailed` MUST
    /// exclude `'failed'` from its WHERE clause so a `failed → failed`
    /// re-entry is a silent no-op. Otherwise `retryCount = retryCount + 1`
    /// runs every time, the runner-side max-retry gate (which reads
    /// `retryCount`) is silently disabled, and recovered jobs spin in a
    /// retry loop until the user resets the database.
    ///
    /// A future reviewer who reads the H2-allowed siblings
    /// (`markFinalPassJobRunning` / `*Deferred` / `*Complete`) and
    /// "fixes" the asymmetry by adding `'failed'` to this clause would
    /// silently re-introduce the H2-class bug. This test catches that.
    @Test("markFinalPassJobFailed: failed → failed is a no-op; retryCount does not climb")
    func markFinalPassJobFailedRetryRunawayProtection() async throws {
        let store = try await makeTestStore()

        // Seed an asset so the FK to analysis_assets is satisfied.
        let assetId = "asset-c8m2"
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: "ep-c8m2",
            assetFingerprint: "fp-c8m2",
            weakFingerprint: nil,
            sourceURL: "file:///c8m2.m4a",
            featureCoverageEndTime: 30.0,
            fastTranscriptCoverageEndTime: 30.0,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 600
        ))

        let jobId = "fpj-c8m2-window1"
        let job = FinalPassJob(
            jobId: jobId,
            analysisAssetId: assetId,
            podcastId: nil,
            adWindowId: "win-c8m2",
            windowStartTime: 100.0,
            windowEndTime: 130.0,
            status: .running,
            retryCount: 0,
            deferReason: nil,
            createdAt: Date().timeIntervalSince1970
        )
        try await store.insertOrIgnoreFinalPassJob(job)

        // First failure: row is in `running`, IN clause matches, status
        // flips to `failed`, retryCount climbs from 0 → 1.
        try await store.markFinalPassJobFailed(jobId: jobId, reason: "transient")
        let afterFirst = try #require(await store.fetchFinalPassJob(byId: jobId))
        #expect(afterFirst.status == .failed)
        #expect(afterFirst.retryCount == 1, "first failed transition must bump retryCount")

        // Second failure: row is now `failed`, IN clause does NOT match
        // (failed is excluded), so the UPDATE affects 0 rows. retryCount
        // must stay at 1.
        try await store.markFinalPassJobFailed(jobId: jobId, reason: "transient-again")
        let afterSecond = try #require(await store.fetchFinalPassJob(byId: jobId))
        #expect(afterSecond.status == .failed)
        #expect(
            afterSecond.retryCount == 1,
            """
            failed → failed re-entry must be a no-op — retryCount must NOT \
            climb. If this fails, someone added 'failed' to the IN clause \
            in `AnalysisStore.markFinalPassJobFailed` without first adding \
            a clamp on retryCount, re-introducing the H2-class runaway-retry \
            bug. See the cycle-7 M1 / cycle-9 M-1 doc on \
            `markFinalPassJobFailed` for the full rationale.
            """
        )

        // Third failure with the same outcome: still no-op.
        try await store.markFinalPassJobFailed(jobId: jobId, reason: "transient-third")
        let afterThird = try #require(await store.fetchFinalPassJob(byId: jobId))
        #expect(afterThird.retryCount == 1)
    }

    /// Counterpart to the runaway-protection test: `failed → running`
    /// re-promotion via `markFinalPassJobRunning` MUST succeed so a
    /// recovered retry can land. This is the H2-allowed sibling — its
    /// IN-clause includes `'failed'`. A future "tighten" that drops
    /// `'failed'` from `markFinalPassJobRunning`'s IN clause would
    /// silently strand every retried failed row forever — the runner
    /// would call `markFinalPassJobRunning` (silent no-op), do the
    /// transcription work anyway (idempotent on chunk fingerprint), then
    /// call `markFinalPassJobComplete` (which would land because its
    /// IN-clause includes `'failed'`), and the cycle-9 cross-launch
    /// climb test would still see retryCount progress on subsequent
    /// failures. This test pins the re-promotion contract directly.
    @Test("markFinalPassJobRunning: failed → running re-promotion succeeds")
    func markFinalPassJobRunningRepromotionFromFailed() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-c8m2-repro"
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: "ep-c8m2-repro",
            assetFingerprint: "fp-c8m2-repro",
            weakFingerprint: nil,
            sourceURL: "file:///c8m2-repro.m4a",
            featureCoverageEndTime: 30.0,
            fastTranscriptCoverageEndTime: 30.0,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 600
        ))
        let jobId = "fpj-c8m2-repro-window1"
        try await store.insertOrIgnoreFinalPassJob(FinalPassJob(
            jobId: jobId,
            analysisAssetId: assetId,
            podcastId: nil,
            adWindowId: "win-c8m2-repro",
            windowStartTime: 100.0,
            windowEndTime: 130.0,
            status: .running,
            retryCount: 0,
            deferReason: nil,
            createdAt: Date().timeIntervalSince1970
        ))

        // Drive into `failed`, then back to `running`.
        try await store.markFinalPassJobFailed(jobId: jobId, reason: "transient")
        let afterFailed = try #require(await store.fetchFinalPassJob(byId: jobId))
        #expect(afterFailed.status == .failed)

        try await store.markFinalPassJobRunning(jobId: jobId)
        let afterRepromote = try #require(await store.fetchFinalPassJob(byId: jobId))
        #expect(
            afterRepromote.status == .running,
            """
            failed → running re-promotion must succeed. If this fails, \
            someone removed 'failed' from `markFinalPassJobRunning`'s \
            IN clause, stranding every retried job.
            """
        )
    }

    /// skeptical-review-cycle-9 M-2: pin the production retry path. The
    /// cycle-7/8 IN-clause asymmetry caps in-drain `retryCount` climb at
    /// +1, but the realistic production sequence (which spans launches)
    /// is `failed → markFinalPassJobRunning (re-promote) →
    /// markFinalPassJobFailed (failure recurs)`, which DOES bump
    /// `retryCount` again. This test pins that semantics so a future
    /// "tighten" that drops `'failed'` from the
    /// `markFinalPassJobRunning` IN-clause (re-introducing the H2 bug
    /// from a different angle) trips here even though the
    /// `failed → failed` no-op test still passes.
    ///
    /// skeptical-review-cycle-10 L-2: this test exercises the IN-clause
    /// behavior in-process, not a real launch boundary. It does NOT
    /// involve `resetStrandedFinalPassJobs`. The cycle-9 name implied
    /// otherwise; renamed to reflect what it actually covers.
    @Test("markFinalPassJobFailed: repromoted retry bumps retryCount each cycle")
    func markFinalPassJobFailedRepromotedRetryBumpsRetryCount() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-c9m2-cross"
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: "ep-c9m2-cross",
            assetFingerprint: "fp-c9m2-cross",
            weakFingerprint: nil,
            sourceURL: "file:///c9m2-cross.m4a",
            featureCoverageEndTime: 30.0,
            fastTranscriptCoverageEndTime: 30.0,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 600
        ))
        let jobId = "fpj-c9m2-cross-window1"
        try await store.insertOrIgnoreFinalPassJob(FinalPassJob(
            jobId: jobId,
            analysisAssetId: assetId,
            podcastId: nil,
            adWindowId: "win-c9m2-cross",
            windowStartTime: 100.0,
            windowEndTime: 130.0,
            status: .running,
            retryCount: 0,
            deferReason: nil,
            createdAt: Date().timeIntervalSince1970
        ))

        // Launch 1: running → failed (retryCount 0 → 1).
        try await store.markFinalPassJobFailed(jobId: jobId, reason: "transient-1")
        let afterLaunch1 = try #require(await store.fetchFinalPassJob(byId: jobId))
        #expect(afterLaunch1.status == .failed)
        #expect(afterLaunch1.retryCount == 1)

        // Launch 2: re-promote failed → running directly via
        // `markFinalPassJobRunning` (whose IN-clause includes 'failed').
        // Note: `resetStrandedFinalPassJobs` is NOT part of this path —
        // that reaper only resets stranded `running` rows post-crash.
        // See cycle-10 M-1 test below for that invariant.
        try await store.markFinalPassJobRunning(jobId: jobId)
        let afterRepromote = try #require(await store.fetchFinalPassJob(byId: jobId))
        #expect(afterRepromote.status == .running)

        try await store.markFinalPassJobFailed(jobId: jobId, reason: "transient-2")
        let afterLaunch2 = try #require(await store.fetchFinalPassJob(byId: jobId))
        #expect(afterLaunch2.status == .failed)
        #expect(
            afterLaunch2.retryCount == 2,
            """
            Cross-launch retry must bump retryCount on each new failure. \
            If this fails, either (a) `markFinalPassJobRunning` no longer \
            re-promotes failed rows (regression of the H2-allowed sibling), \
            or (b) `markFinalPassJobFailed` no longer matches `running` \
            rows (regression of its IN-clause). Both break the cycle-7/8 \
            doc contract that says `retryCount` climbs 1 per launch on \
            persistent failure.
            """
        )

        // Launch 3: same loop again. retryCount should reach 3.
        try await store.markFinalPassJobRunning(jobId: jobId)
        try await store.markFinalPassJobFailed(jobId: jobId, reason: "transient-3")
        let afterLaunch3 = try #require(await store.fetchFinalPassJob(byId: jobId))
        #expect(afterLaunch3.retryCount == 3)
    }

    /// skeptical-review-cycle-10 M-1 / missing-test: pin the contract
    /// that `resetStrandedFinalPassJobs` reaper ONLY touches stranded
    /// `running` rows, NOT `failed` rows. The cycle-7/8/9 doc series
    /// claims this but no existing test would catch a future "fix" that
    /// adds `OR status = 'failed'` to the reaper's WHERE clause: rows
    /// would still flow through `failed → queued → running` and the
    /// existing retry-climb test would still pass, but the reaper would
    /// silently bypass the cycle-7 M1 in-drain retry cap (because
    /// `markFinalPassJobFailed` allows `queued → failed` so retryCount
    /// could climb on every reaper sweep, not just on every actual
    /// re-attempt).
    @Test("resetStrandedFinalPassJobs: failed rows are NOT touched by the reaper")
    func resetStrandedFinalPassJobsSkipsFailedRows() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-c10m1-skipfailed"
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: "ep-c10m1-skipfailed",
            assetFingerprint: "fp-c10m1-skipfailed",
            weakFingerprint: nil,
            sourceURL: "file:///c10m1-skipfailed.m4a",
            featureCoverageEndTime: 30.0,
            fastTranscriptCoverageEndTime: 30.0,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 600
        ))

        // skeptical-review-cycle-11 M-1: drive the row to `failed`, THEN
        // backdate `updatedAt` via `forceFinalPassJobStateForTesting`.
        // Earlier (cycle-10) attempt seeded `createdAt: now - 3600` and
        // assumed that satisfied the reaper's freshness floor — but
        // `markFinalPassJobFailed` sets `updatedAt = strftime('%s', 'now')`,
        // so the row's `updatedAt` was fresh and the reaper skipped it
        // for the WRONG reason (freshness, not status). A regression
        // adding `OR status = 'failed'` to the reaper's WHERE clause
        // would have slipped past the test silently. Backdating
        // `updatedAt` here makes the freshness gate transparent so the
        // status-filter is the only remaining reason the row stays
        // `failed`.
        let jobId = "fpj-c10m1-skipfailed-window1"
        try await store.insertOrIgnoreFinalPassJob(FinalPassJob(
            jobId: jobId,
            analysisAssetId: assetId,
            podcastId: nil,
            adWindowId: "win-c10m1-skipfailed",
            windowStartTime: 100.0,
            windowEndTime: 130.0,
            status: .running,
            retryCount: 0,
            deferReason: nil,
            createdAt: Date().timeIntervalSince1970
        ))
        try await store.markFinalPassJobFailed(jobId: jobId, reason: "transient")
        try await store.forceFinalPassJobStateForTesting(
            jobId: jobId,
            status: .failed,
            updatedAtOverride: Int(Date().timeIntervalSince1970) - 3600
        )
        let afterFail = try #require(await store.fetchFinalPassJob(byId: jobId))
        #expect(afterFail.status == .failed)
        let retryCountBefore = afterFail.retryCount

        // Run the reaper. The row's `updatedAt` is now 1 hour in the
        // past — well beyond `strandedJobFreshnessSeconds` (600) — so
        // the freshness gate is satisfied. The ONLY remaining reason
        // the reaper should skip this row is the `status = 'running'`
        // filter. A regression adding `OR status = 'failed'` to the
        // WHERE clause would now flip our row to `queued`, tripping
        // the assertion below.
        let resetCount = try await store.resetStrandedFinalPassJobs()

        let afterReap = try #require(await store.fetchFinalPassJob(byId: jobId))
        #expect(
            afterReap.status == .failed,
            """
            `resetStrandedFinalPassJobs` MUST NOT touch `failed` rows. \
            If this fails, someone added `OR status = 'failed'` to the \
            reaper's WHERE clause believing it's a safety net for \
            failed→running re-promotion. It is NOT — re-promotion is \
            handled by `markFinalPassJobRunning`'s IN-clause directly. \
            Adding 'failed' to the reaper would silently bypass the \
            cycle-7 M1 in-drain retry cap.
            """
        )
        #expect(
            afterReap.retryCount == retryCountBefore,
            "Reaper must not bump retryCount on a `failed` row."
        )
        // The reaper may have legitimately reset other rows in the
        // store fixture, but our seeded `failed` row contributing to
        // resetCount would indicate the bug.
        #expect(resetCount >= 0)
    }
}

#endif
