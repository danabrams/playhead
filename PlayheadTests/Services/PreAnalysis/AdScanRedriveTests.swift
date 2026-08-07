// AdScanRedriveTests.swift
// playhead-onn6: an episode that finished its tiers under-scanned must get
// another ad-scan pass — bounded, and only when another pass can actually do
// something.
//
// **The stranding.** `AnalysisJobRunner.shouldSkipSemanticBackfill`
// (playhead-i7qe) governs a run that is already being dispatched; it cannot
// cause one. Once the tiers finish, `analysis_jobs.state` is `'complete'`,
// `workKey` is `UNIQUE` and `insertJob` is `INSERT OR IGNORE`, so no new job is
// ever minted at the same key and only `queued`/`paused`/retryable-`failed` rows
// dispatch. Measured on the product owner's device (analysis.sqlite, pulled
// 2026-07-29): of the 34 episodes over 15 minutes, 20 had NO dispatchable job at
// all, and 35 `backfill_jobs` rows sat `queued` across 12 assets — real,
// still-resumable scan work that nothing anywhere selected.
//
// **Why the coverage lane is the trigger.** `BackfillJobRunner.runPendingBackfill`
// cannot be invoked standalone (it needs `AssetInputs` only the analysis lane
// assembles), so "drain the queued rows" and "re-drive the episode" are the same
// action. Resumable coverage-lane rows are therefore both the reason to mint a
// pass AND the proof the pass will not be a no-op: with zero of them, a fresh
// pass re-derives the same deterministic jobIds, finds them `complete`, skips
// every one, and produces no coverage. That is the no-progress guard, and it is
// structural rather than historical.
//
// **Why bounded.** playhead-gqx4 terminates an under-scanned episode into a
// degraded state rather than declining to terminate, because an unbounded retry
// is a worse bug than the one it fixed. Two independent guards hold: the ordinal
// carried in the UNIQUE `workKey` (absolute cap), and the resumable-row count
// (stops on its own as soon as a pass drains or exhausts the lane).

import Foundation
import Testing

@testable import Playhead

// MARK: - The pure decision

@Suite("Ad-scan re-drive — decision matrix (playhead-onn6)")
struct AdScanRedriveDecisionTests {

    /// Asset 820134BF, the audited episode: transcript 100%, coverage-lane scan
    /// 47% of the audio, one `queued` `fullEpisodeScan` row that has been sitting
    /// since 2026-07-28. The scan can advance and nothing was going to run it.
    @Test("820134BF's exact shape mints a re-drive")
    func auditedEpisodeShapeMintsRedrive() {
        #expect(
            AnalysisWorkScheduler.shouldMintAdScanRedrive(
                adScanFraction: 0.47,
                resumableCoverageJobCount: 1
            )
        )
    }

    /// The rest of the device's stranded cohort, with their real resumable-row
    /// counts. Every one of them is an episode the pipeline had declared done.
    @Test(
        "every stranded device asset with outstanding coverage work mints",
        arguments: [
            (0.394, 4),  // 8FECFDDE — completeFull, 4 rows queued
            (0.026, 3),  // B10C7BC8 — completeFull at 2.6%
            (0.387, 3),  // 144C8A80 — completeFull
            (0.095, 1),  // 1A9616D1
            (0.211, 1),  // D75D7584
            (0.544, 5),  // B5786B41
            (0.053, 8),  // E71CF852
            (0.095, 2),  // 1E32428C
            (0.000, 2),  // 4E4730D8 — scan examined nothing
        ]
    )
    func strandedDeviceAssetsMint(adScanFraction: ReachRatio, resumable: Int) {
        #expect(
            AnalysisWorkScheduler.shouldMintAdScanRedrive(
                adScanFraction: adScanFraction,
                resumableCoverageJobCount: resumable
            )
        )
    }

    /// **The no-progress guard, at the device's own counter-example.** Asset
    /// 644F2551 is 7% scanned and every one of its eight `backfill_jobs` rows is
    /// `complete`. A re-drive there would re-derive the same jobIds, hit the M-5
    /// `complete → continue` branch for all of them, and finish having read no
    /// audio at all. A job that runs and achieves nothing is the bug, not the
    /// fix — so a low fraction alone must never license a pass.
    @Test("a drained coverage lane never mints, however low the coverage")
    func drainedCoverageLaneNeverMints() {
        for fraction: ReachRatio in [0.0, 0.067, 0.5, 0.9] {
            #expect(
                AnalysisWorkScheduler.shouldMintAdScanRedrive(
                    adScanFraction: fraction,
                    resumableCoverageJobCount: 0
                ) == false,
                "fraction \(fraction) with no resumable rows must not mint"
            )
        }
    }

    /// The complement of the runner's skip. If these two floors ever diverge the
    /// scheduler starts minting passes the runner then declines to run (or stops
    /// minting for episodes the runner would still scan) — either way, motion
    /// without coverage.
    @Test("the mint floor is exactly the runner's skip floor")
    func mintFloorMatchesSkipFloor() {
        #expect(
            AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
                == episodePreparationCompleteThreshold
        )
        #expect(
            AnalysisWorkScheduler.shouldMintAdScanRedrive(
                adScanFraction: episodePreparationCompleteThreshold,
                resumableCoverageJobCount: 1
            ) == false
        )
        #expect(
            AnalysisWorkScheduler.shouldMintAdScanRedrive(
                adScanFraction: ReachRatio(episodePreparationCompleteThreshold.rawValue - 0.001),
                resumableCoverageJobCount: 1
            )
        )
    }

    /// Stated as a property rather than at sample points: with outstanding work,
    /// "mint a re-drive" and "skip the semantic backfill" must be exact opposites
    /// across the whole range. A future edit to either floor breaks this.
    @Test("mint and skip are complementary across the coverage range")
    func mintAndSkipAreComplementary() {
        for step in 0...100 {
            let fraction = ReachRatio(Double(step) / 100.0)
            let mints = AnalysisWorkScheduler.shouldMintAdScanRedrive(
                adScanFraction: fraction,
                resumableCoverageJobCount: 1
            )
            let skips = AnalysisJobRunner.shouldSkipSemanticBackfill(
                wroteNewChunks: false,
                hasExistingWindows: true,
                hasCandidateWindows: false,
                adScanFraction: fraction
            )
            #expect(mints == !skips, "disagreement at fraction \(fraction)")
        }
    }

    /// `nil` arrives for real: no coverage-lane row at all (the scan never ran —
    /// the most under-scanned state there is), or a duration the transcript's own
    /// reach contradicts. Unmeasured is not sufficient. It cannot spin, because
    /// this arm is only reachable when the lane provably still holds work and the
    /// ordinal budget still applies.
    @Test("unmeasured coverage with outstanding work still mints")
    func unmeasuredCoverageMints() {
        #expect(
            AnalysisWorkScheduler.shouldMintAdScanRedrive(
                adScanFraction: nil,
                resumableCoverageJobCount: 1
            )
        )
        for poisoned: ReachRatio in [ReachRatio(.nan), ReachRatio(.infinity), ReachRatio(-.infinity)] {
            #expect(
                AnalysisWorkScheduler.shouldMintAdScanRedrive(
                    adScanFraction: poisoned,
                    resumableCoverageJobCount: 1
                ),
                "non-finite \(poisoned) must not read as sufficient coverage"
            )
        }
    }

    /// …but unmeasured coverage still cannot conjure work out of an empty lane.
    @Test("unmeasured coverage with a drained lane does not mint")
    func unmeasuredCoverageWithDrainedLaneDoesNotMint() {
        #expect(
            AnalysisWorkScheduler.shouldMintAdScanRedrive(
                adScanFraction: nil,
                resumableCoverageJobCount: 0
            ) == false
        )
    }
}

// MARK: - The budget, carried in the work key

@Suite("Ad-scan re-drive — work-key budget (playhead-onn6)")
struct AdScanRedriveWorkKeyTests {

    private func job(workKey: String, jobType: String = "preAnalysis") -> AnalysisJob {
        makeAnalysisJob(
            jobType: jobType,
            workKey: workKey,
            sourceFingerprint: "fp-onn6"
        )
    }

    @Test("a plain tier job starts the chain at ordinal 1")
    func plainJobStartsAtOne() {
        let key = AnalysisWorkScheduler.nextAdScanRedriveWorkKey(
            for: job(workKey: "fp-onn6:1:preAnalysis")
        )
        #expect(key == "fp-onn6:\(PreAnalysisConfig.analysisVersion):preAnalysis:adScanRedrive:1")
    }

    /// The key is rebuilt from the fingerprint, not appended to — so a job that
    /// reached the 600 s tier does not produce `…:600:adScanRedrive:1`, which
    /// would make the ordinal unreadable on the next hop.
    @Test("a tier-advanced job does not nest the marker under the tier suffix")
    func tierSuffixIsReplacedNotNested() {
        let key = AnalysisWorkScheduler.nextAdScanRedriveWorkKey(
            for: job(workKey: "fp-onn6:1:preAnalysis:600")
        )
        #expect(key?.contains(":600:") == false)
        #expect(AnalysisWorkScheduler.adScanRedriveOrdinal(workKey: key ?? "") == 1)
    }

    /// Pinned as a literal so the bound cannot be widened by editing the constant
    /// alone — `chainIsBounded` below uses the symbol as its own oracle and would
    /// otherwise pass at any value.
    @Test("the re-drive budget is two")
    func budgetIsTwo() {
        #expect(AnalysisWorkScheduler.maxAdScanRedrives == 2)
    }

    @Test("the chain advances 1 → 2 and then stops")
    func chainIsBounded() {
        var current = "fp-onn6:\(PreAnalysisConfig.analysisVersion):preAnalysis"
        var seen: [String] = []
        // Drive far past the budget: a bug that lets the ordinal keep climbing
        // shows up as a chain longer than `maxAdScanRedrives`.
        for _ in 0..<50 {
            guard let next = AnalysisWorkScheduler.nextAdScanRedriveWorkKey(
                for: job(workKey: current)
            ) else {
                break
            }
            seen.append(next)
            current = next
        }
        #expect(seen.count == AnalysisWorkScheduler.maxAdScanRedrives)
        #expect(Set(seen).count == seen.count, "each hop must produce a distinct key")
        #expect(AnalysisWorkScheduler.adScanRedriveOrdinal(workKey: seen.last ?? "")
                == AnalysisWorkScheduler.maxAdScanRedrives)
    }

    @Test("the playback lane takes no re-drives")
    func playbackLaneIsExcluded() {
        #expect(
            AnalysisWorkScheduler.nextAdScanRedriveWorkKey(
                for: job(workKey: "fp-onn6:1:playback", jobType: "playback")
            ) == nil
        )
    }

    /// A retired stale-fingerprint tombstone (`…:staleFingerprint:<jobId>`) is not
    /// a re-drive key. That row has been superseded and is never processed again;
    /// its live successor carries the real ordinal, because
    /// `replacementWorkKeyForCurrentCanonicalAudio` preserves the suffix past the
    /// job type when it rebases onto the new fingerprint.
    @Test("a retired tombstone key does not read as a re-drive")
    func tombstoneKeyIsNotARedrive() {
        #expect(
            AnalysisWorkScheduler.adScanRedriveOrdinal(
                workKey: "fp-onn6:1:preAnalysis:adScanRedrive:1:staleFingerprint:job-7"
            ) == nil
        )
    }

    @Test("non-re-drive keys parse as no ordinal")
    func nonRedriveKeysHaveNoOrdinal() {
        for key in [
            "fp:1:preAnalysis",
            "fp:1:preAnalysis:600",
            "fp:1:preAnalysis:adScanRedrive:0",
            "fp:1:preAnalysis:adScanRedrive:x",
            "adScanRedrive",
            "",
        ] {
            #expect(
                AnalysisWorkScheduler.adScanRedriveOrdinal(workKey: key) == nil,
                "\(key) must not parse as a re-drive ordinal"
            )
        }
    }
}

// MARK: - The selector that did not exist

@Suite("Ad-scan re-drive — coverage-lane selector (playhead-onn6)")
struct ResumableBackfillJobSelectorTests {

    private func seedAsset(_ store: AnalysisStore, id: String) async throws {
        try await store.insertAsset(
            AnalysisAsset(
                id: id,
                episodeId: "ep-\(id)",
                assetFingerprint: "fp-\(id)",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/\(id).m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: SessionState.completeFull.rawValue,
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: 2113
            )
        )
    }

    /// The count is the exact mirror of the runner's M-5 resume predicate. Any
    /// status but `complete` counts, any retryCount at or past
    /// `AdmissionController.maxRetries` does not, and `running` is excluded so a
    /// concurrent drain is not mistaken for outstanding work.
    @Test("resumability mirrors the runner's M-5 predicate")
    func countMirrorsM5() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, id: "a1")
        let rows: [(BackfillJobStatus, Int)] = [
            (.queued, 0),
            (.deferred, 0),
            (.failed, AdmissionController.maxRetries - 1),
            (.failed, AdmissionController.maxRetries),      // exhausted — M-5 skips
            (.complete, 0),                                  // done
            (.running, 0),                                   // someone else's
        ]
        for (index, row) in rows.enumerated() {
            try await store.insertBackfillJob(
                makeBackfillJob(
                    jobId: "bf-\(index)",
                    analysisAssetId: "a1",
                    retryCount: row.1,
                    status: row.0,
                    // One batch: a single `runPendingBackfill` stamps every row it
                    // inserts from one clock reading.
                    createdAt: 5_000
                )
            )
        }
        #expect(try await store.countResumableBackfillJobs(assetId: "a1") == 3)
    }

    /// **The orphan-batch guard.** `runPendingBackfill` re-derives jobIds from
    /// `(assetId, transcriptVersion, phase, offset)` rather than iterating rows, so
    /// a row whose tuple the current invocation no longer regenerates — a
    /// superseded transcript version, a `CoveragePlanner` policy flip — can never
    /// be resumed and its `retryCount` never moves. Counting it would license a
    /// pass that re-derives the current ids, finds them all `complete`, and reads
    /// no audio at all.
    @Test("a stale earlier batch does not count as resumable work")
    func staleBatchIsNotResumable() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, id: "a-batches")
        // Batch 1 (an older transcript version): left queued forever.
        for (index, phase) in [BackfillJobPhase.scanHarvesterProposals,
                               .scanLikelyAdSlots,
                               .scanRandomAuditWindows].enumerated() {
            try await store.insertBackfillJob(
                makeBackfillJob(
                    jobId: "old-\(index)", analysisAssetId: "a-batches",
                    phase: phase, status: .queued, createdAt: 1_000 + Double(index) * 0.0001
                )
            )
        }
        // Batch 2 (the current version): finished.
        try await store.insertBackfillJob(
            makeBackfillJob(
                jobId: "new-0", analysisAssetId: "a-batches",
                phase: .fullEpisodeScan, status: .complete, createdAt: 9_000
            )
        )
        #expect(try await store.countResumableBackfillJobs(assetId: "a-batches") == 0,
                "three orphaned rows from a superseded batch are not work the runner can do")

        // …but an unfinished row in the NEWEST batch is real work.
        try await store.insertBackfillJob(
            makeBackfillJob(
                jobId: "new-1", analysisAssetId: "a-batches",
                phase: .specialistHostReadScan, status: .queued, createdAt: 9_000.0003
            )
        )
        #expect(try await store.countResumableBackfillJobs(assetId: "a-batches") == 1)
    }

    /// The batch window must absorb the sub-millisecond spread one invocation
    /// stamps across its phases without swallowing a genuinely older batch.
    @Test("the batch window covers one plan's phase spread")
    func batchWindowCoversOnePlan() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, id: "a-spread")
        for index in 0..<5 {
            try await store.insertBackfillJob(
                makeBackfillJob(
                    jobId: "spread-\(index)", analysisAssetId: "a-spread",
                    status: .queued, createdAt: 7_000 + Double(index) * 0.0001
                )
            )
        }
        #expect(try await store.countResumableBackfillJobs(assetId: "a-spread") == 5)
        #expect(AnalysisStore.backfillEnqueueBatchWindowSec > 0.0005 * 100)
    }

    /// playhead-wxsv SPEC 3: the re-drive window must not be evicted.
    ///
    /// `countResumableBackfillJobs` scopes to a 0.5 s window below an
    /// UNFILTERED `MAX(createdAt)`, so any write that moves a row's timestamp
    /// forward pushes that row's own siblings out of the window — and if the
    /// moved row is itself `complete` or `running` (both excluded from the
    /// count) the answer becomes ZERO while real pending work sits there, and
    /// `AnalysisWorkScheduler.shouldMintAdScanRedrive` shuts the gate.
    ///
    /// This bead adds a new writer to these rows —
    /// `reopenBackfillJob` — so the invariant needs a witness rather than a
    /// convention. The fixture is the exact shape that breaks: one asset, a
    /// complete row and a still-queued sibling in the same batch. An
    /// `INSERT OR REPLACE` re-open (the tempting spelling) restamps `createdAt`
    /// to now and this reads 0.
    @Test("playhead-wxsv: re-opening a row does not evict its batch from the re-drive window")
    func reopenDoesNotEvictTheBatch() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, id: "a-window")

        // One enqueue batch: a phase that finished and a phase still waiting.
        try await store.insertBackfillJob(
            makeBackfillJob(
                jobId: "win-done", analysisAssetId: "a-window",
                phase: .fullEpisodeScan, status: .queued, createdAt: 5_000
            )
        )
        try await store.insertBackfillJob(
            makeBackfillJob(
                jobId: "win-pending", analysisAssetId: "a-window",
                phase: .scanLikelyAdSlots, status: .queued, createdAt: 5_000.0001
            )
        )
        try await store.markBackfillJobRunning(jobId: "win-done", transcriptVersion: "tx-v1")
        try await store.markBackfillJobComplete(jobId: "win-done", progressCursor: nil)
        #expect(try await store.countResumableBackfillJobs(assetId: "a-window") == 1,
                "the pending sibling is the asset's remaining work")

        #expect(try await store.reopenBackfillJob(jobId: "win-done", forTranscriptVersion: "tx-v2"))

        #expect(try await store.countResumableBackfillJobs(assetId: "a-window") == 2,
                "re-opening adds work; it must never subtract the sibling by moving MAX(createdAt)")
        #expect(AnalysisWorkScheduler.shouldMintAdScanRedrive(
                    adScanFraction: 0.1,
                    resumableCoverageJobCount: try await store.countResumableBackfillJobs(assetId: "a-window")
                ),
                "the playhead-onn6 gate must still open")
    }

    @Test("an asset with no coverage-lane rows counts zero")
    func emptyLaneCountsZero() async throws {
        let store = try await makeTestStore()
        try await seedAsset(store, id: "a-empty")
        #expect(try await store.countResumableBackfillJobs(assetId: "a-empty") == 0)
    }

    /// `deferred` is the thermal/battery outcome and the most common non-terminal
    /// state on a warm device — it MUST qualify. `running` must not: that row
    /// belongs to whoever holds the admission ticket.
    @Test("the set selector returns only assets with resumable work, capped")
    func setSelectorFiltersAndCaps() async throws {
        let store = try await makeTestStore()
        for id in ["a-queued", "a-deferred", "a-done", "a-exhausted", "a-running"] {
            try await seedAsset(store, id: id)
        }
        try await store.insertBackfillJob(
            makeBackfillJob(jobId: "bf-q", analysisAssetId: "a-queued",
                            status: .queued, createdAt: 1_000)
        )
        try await store.insertBackfillJob(
            makeBackfillJob(jobId: "bf-df", analysisAssetId: "a-deferred",
                            status: .deferred, createdAt: 2_000)
        )
        try await store.insertBackfillJob(
            makeBackfillJob(jobId: "bf-d", analysisAssetId: "a-done",
                            status: .complete, createdAt: 3_000)
        )
        try await store.insertBackfillJob(
            makeBackfillJob(
                jobId: "bf-x",
                analysisAssetId: "a-exhausted",
                retryCount: AdmissionController.maxRetries,
                status: .failed,
                createdAt: 4_000
            )
        )
        try await store.insertBackfillJob(
            makeBackfillJob(jobId: "bf-r", analysisAssetId: "a-running",
                            status: .running, createdAt: 5_000)
        )

        let all = try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 100)
        #expect(all == ["a-queued", "a-deferred"])
        #expect(try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 0).isEmpty)
        // And the per-asset authority agrees on each.
        #expect(try await store.countResumableBackfillJobs(assetId: "a-deferred") == 1)
        #expect(try await store.countResumableBackfillJobs(assetId: "a-running") == 0)
        #expect(try await store.countResumableBackfillJobs(assetId: "a-exhausted") == 0)
    }

    /// Oldest stranded work first — and explicitly NOT by `priority`.
    ///
    /// `backfill_jobs.priority` ranks PHASES inside one asset's plan
    /// (`scanLikelyAdSlots` 30, `fullEpisodeScan` 5). Reusing it across assets
    /// inverts the thing that matters: `fullEpisodeScan` reads the whole episode
    /// and moves coverage most, yet carries the lowest phase priority. On the
    /// 2026-07-29 device pull that ordering pushed 8 of the 11 eligible assets —
    /// including the audited 820134BF — past the sweep's cap. The fixture below
    /// is that exact trap: the high-priority row is the NEWEST, so a
    /// priority-ordered implementation returns it first and fails.
    @Test("the set selector orders oldest-first, not by phase priority")
    func setSelectorOrdering() async throws {
        let store = try await makeTestStore()
        for id in ["full-oldest", "targeted-newest", "full-middle"] {
            try await seedAsset(store, id: id)
        }
        try await store.insertBackfillJob(
            makeBackfillJob(
                jobId: "bf-full-oldest", analysisAssetId: "full-oldest",
                phase: .fullEpisodeScan, priority: 5, status: .queued, createdAt: 1_000
            )
        )
        try await store.insertBackfillJob(
            makeBackfillJob(
                jobId: "bf-full-middle", analysisAssetId: "full-middle",
                phase: .fullEpisodeScan, priority: 5, status: .queued, createdAt: 2_000
            )
        )
        try await store.insertBackfillJob(
            makeBackfillJob(
                jobId: "bf-targeted-newest", analysisAssetId: "targeted-newest",
                phase: .scanLikelyAdSlots, priority: 30, status: .queued, createdAt: 3_000
            )
        )
        let ordered = try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 10)
        #expect(ordered == ["full-oldest", "full-middle", "targeted-newest"])
        #expect(try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 2)
                == ["full-oldest", "full-middle"])
    }
}

// MARK: - Reaching the episodes already stranded

@Suite("Ad-scan re-drive — reconciler mint (playhead-onn6)")
struct AdScanRedriveReconcilerTests {

    private static let episodeId = "ep-820134BF"
    private static let assetId = "asset-820134BF"
    private static let fingerprint = "fp-820134BF"

    /// Asset 820134BF's shape: 2,113 s of audio, a fast transcript that reaches
    /// the end, a coverage-lane scan that examined roughly 47% of it, one
    /// `queued` `fullEpisodeScan` row, and an `analysis_jobs` row that is already
    /// `complete` — so nothing was ever going to run again.
    private func seedStrandedDeviceAsset(
        _ store: AnalysisStore,
        scannedSeconds: Double = 1_000,
        analysisState: SessionState = .completeFull
    ) async throws {
        try await store.insertAsset(
            AnalysisAsset(
                id: Self.assetId,
                episodeId: Self.episodeId,
                assetFingerprint: Self.fingerprint,
                weakFingerprint: nil,
                sourceURL: "file:///tmp/820134BF.m4a",
                featureCoverageEndTime: 2_113,
                fastTranscriptCoverageEndTime: 2_113,
                confirmedAdCoverageEndTime: nil,
                analysisState: analysisState.rawValue,
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: 2_113
            )
        )
        _ = try await store.insertTranscriptChunks([
            makeCoverageChunk(assetId: Self.assetId, index: 0, start: 0, end: 2_113)
        ])
        try await store.insertSemanticScanResult(
            makeCoverageScan(assetId: Self.assetId, index: 0, start: 0, end: scannedSeconds)
        )
        try await store.insertBackfillJob(
            makeBackfillJob(
                jobId: "fm-f65a371a7",
                analysisAssetId: Self.assetId,
                phase: .fullEpisodeScan,
                status: .queued
            )
        )
        try await store.insertJob(
            makeAnalysisJob(
                jobId: "job-terminal",
                jobType: "preAnalysis",
                episodeId: Self.episodeId,
                analysisAssetId: Self.assetId,
                workKey: AnalysisJob.computeWorkKey(
                    fingerprint: Self.fingerprint,
                    analysisVersion: PreAnalysisConfig.analysisVersion,
                    jobType: "preAnalysis"
                ),
                sourceFingerprint: Self.fingerprint,
                desiredCoverageSec: 2_113,
                state: "complete"
            )
        )
    }

    private func makeReconciler(
        store: AnalysisStore,
        downloads: StubDownloadProvider
    ) -> AnalysisJobReconciler {
        AnalysisJobReconciler(
            store: store,
            downloadManager: downloads,
            capabilitiesService: StubCapabilitiesProvider()
        )
    }

    private func cachedDownloads() -> StubDownloadProvider {
        let downloads = StubDownloadProvider()
        downloads.cachedURLs[Self.episodeId] = URL(fileURLWithPath: "/tmp/820134BF.m4a")
        // The SAME fingerprint the terminal job carries, so step 7's
        // "discover un-enqueued download" insert collides on the UNIQUE work key
        // and is ignored — which is precisely the collision this bead is about.
        downloads.fingerprints[Self.episodeId] = AudioFingerprint(
            weak: Self.fingerprint,
            strong: Self.fingerprint
        )
        return downloads
    }

    private func redriveJobs(_ store: AnalysisStore) async throws -> [AnalysisJob] {
        try await store.fetchJobsByState("queued").filter {
            AnalysisWorkScheduler.adScanRedriveOrdinal(workKey: $0.workKey) != nil
        }
    }

    @Test("a stranded device asset gets a dispatchable re-drive")
    func strandedAssetGetsRedrive() async throws {
        let store = try await makeTestStore()
        try await seedStrandedDeviceAsset(store)
        let downloads = cachedDownloads()

        // Before: the episode's only job is terminal. Nothing dispatchable.
        #expect(try await store.fetchNextEligibleJob(
            deferredWorkAllowed: true,
            t0ThresholdSec: 300,
            now: Date().timeIntervalSince1970
        ) == nil)

        let report = try await makeReconciler(store: store, downloads: downloads).reconcile()

        #expect(report.adScanRedrivesMinted == 1)
        let minted = try await redriveJobs(store)
        #expect(minted.count == 1)
        let job = try #require(minted.first)
        #expect(job.analysisAssetId == Self.assetId)
        #expect(job.sourceFingerprint == Self.fingerprint)
        #expect(job.priority == 0, "repair work must stay in the background lane")
        #expect(AnalysisWorkScheduler.adScanRedriveOrdinal(workKey: job.workKey) == 1)
        // And it is genuinely dispatchable — the whole point.
        #expect(try await store.fetchNextEligibleJob(
            deferredWorkAllowed: true,
            t0ThresholdSec: 300,
            now: Date().timeIntervalSince1970
        )?.jobId == job.jobId)
    }

    /// **Termination across launches.** Reconcile runs at every app launch. If
    /// the coverage lane never drains (a stuck `queued` row that no pass can
    /// finish), repeated launches must not manufacture unbounded work: the
    /// ordinal in the UNIQUE work key caps the chain at `maxAdScanRedrives` and
    /// every later launch is a no-op.
    @Test("repeated reconciles stop at the budget and never oscillate")
    func repeatedReconcilesTerminate() async throws {
        let store = try await makeTestStore()
        try await seedStrandedDeviceAsset(store)
        let downloads = cachedDownloads()
        let reconciler = makeReconciler(store: store, downloads: downloads)

        var mintedPerPass: [Int] = []
        for _ in 0..<12 {
            // Each pass simulates the next launch: the previous re-drive ran,
            // achieved nothing, and terminated `complete`.
            for job in try await redriveJobs(store) {
                try await store.updateJobState(
                    jobId: job.jobId,
                    state: "complete",
                    nextEligibleAt: nil,
                    lastErrorCode: nil
                )
            }
            mintedPerPass.append(try await reconciler.reconcile().adScanRedrivesMinted)
        }

        #expect(mintedPerPass.reduce(0, +) == AnalysisWorkScheduler.maxAdScanRedrives)
        #expect(mintedPerPass.suffix(8).allSatisfy { $0 == 0 },
                "the chain must go quiet, not keep minting: \(mintedPerPass)")
        // No oscillation: the coverage-lane row is untouched and the original
        // terminal job is still terminal.
        #expect(try await store.fetchBackfillJob(byId: "fm-f65a371a7")?.status == .queued)
        #expect(try await store.fetchJob(byId: "job-terminal")?.state == "complete")
    }

    /// A drained coverage lane is the 644F2551 shape: nothing left to resume, so
    /// a pass would read no audio. No mint, at any coverage.
    @Test("an asset whose coverage lane is drained gets no re-drive")
    func drainedLaneGetsNoRedrive() async throws {
        let store = try await makeTestStore()
        try await seedStrandedDeviceAsset(store, scannedSeconds: 140)
        try await store.markBackfillJobComplete(jobId: "fm-f65a371a7", progressCursor: nil)
        let report = try await makeReconciler(
            store: store,
            downloads: cachedDownloads()
        ).reconcile()
        #expect(report.adScanRedrivesMinted == 0)
        #expect(try await redriveJobs(store).isEmpty)
    }

    /// A stale coverage-lane row whose media is gone must not mint work that can
    /// only fail. Same guard `enqueueReplacement` applies.
    @Test("no re-drive when the episode's audio is no longer cached")
    func missingAudioGetsNoRedrive() async throws {
        let store = try await makeTestStore()
        try await seedStrandedDeviceAsset(store)
        let report = try await makeReconciler(
            store: store,
            downloads: StubDownloadProvider()   // nothing cached
        ).reconcile()
        #expect(report.adScanRedrivesMinted == 0)
    }

    /// An episode that already has non-terminal work will be scanned without our
    /// help — the runner's M-5 branch resumes its coverage-lane rows when that
    /// pass lands. Minting here would only add a redundant FM pass.
    @Test("no re-drive when the episode already has a pending job")
    func pendingJobSuppressesRedrive() async throws {
        let store = try await makeTestStore()
        try await seedStrandedDeviceAsset(store)
        try await store.insertJob(
            makeAnalysisJob(
                jobId: "job-pending",
                jobType: "preAnalysis",
                episodeId: Self.episodeId,
                analysisAssetId: Self.assetId,
                workKey: "\(Self.fingerprint):\(PreAnalysisConfig.analysisVersion):preAnalysis:600",
                sourceFingerprint: Self.fingerprint,
                state: "queued"
            )
        )
        let report = try await makeReconciler(
            store: store,
            downloads: cachedDownloads()
        ).reconcile()
        #expect(report.adScanRedrivesMinted == 0)
        #expect(try await redriveJobs(store).isEmpty)
    }

    /// **Cross-asset identity.** An episode can carry several assets — a
    /// re-download mints a new one while the old keeps its coverage-lane rows —
    /// and `fetchLatestJobForEpisode` is keyed by EPISODE. Minting from another
    /// asset's job would stamp the row with that asset's `sourceFingerprint` and
    /// `downloadId` (so the stale-fingerprint detector would compare the wrong
    /// audio and never fire) and would charge the ordinal to that asset's work
    /// key, silently spending its budget.
    @Test("no re-drive when the episode's newest job belongs to a different asset")
    func crossAssetLatestJobIsRejected() async throws {
        let store = try await makeTestStore()
        try await seedStrandedDeviceAsset(store)
        // A re-download: same episode, different asset and fingerprint, and its
        // job row is the most recently updated.
        try await store.insertAsset(
            AnalysisAsset(
                id: "asset-redownload",
                episodeId: Self.episodeId,
                assetFingerprint: "fp-redownload",
                weakFingerprint: nil,
                sourceURL: "file:///tmp/redownload.m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: SessionState.completeFull.rawValue,
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: 2_113
            )
        )
        try await store.insertJob(
            makeAnalysisJob(
                jobId: "job-redownload",
                jobType: "preAnalysis",
                episodeId: Self.episodeId,
                analysisAssetId: "asset-redownload",
                workKey: "fp-redownload:\(PreAnalysisConfig.analysisVersion):preAnalysis",
                sourceFingerprint: "fp-redownload",
                state: "complete",
                updatedAt: Date().timeIntervalSince1970 + 60
            )
        )

        let report = try await makeReconciler(
            store: store,
            downloads: cachedDownloads()
        ).reconcile()
        #expect(report.adScanRedrivesMinted == 0)
        #expect(try await redriveJobs(store).isEmpty)
    }

    /// A fully-scanned asset does not get a pass even with a stuck row: the
    /// audio has demonstrably been read, and burning an FM pass on it is exactly
    /// the waste the i7qe skip exists to prevent.
    @Test("a fully-scanned asset gets no re-drive")
    func fullyScannedAssetGetsNoRedrive() async throws {
        let store = try await makeTestStore()
        try await seedStrandedDeviceAsset(store, scannedSeconds: 2_113)
        let report = try await makeReconciler(
            store: store,
            downloads: cachedDownloads()
        ).reconcile()
        #expect(report.adScanRedrivesMinted == 0)
    }

    /// Seeds one stranded asset whose coverage lane holds a resumable row.
    /// `withJobHistory == false` reproduces the device's garbage-collected shape:
    /// the asset survives but every `analysis_jobs` row for its episode is gone,
    /// so there is no fingerprint to mint against.
    private func seedStrandedAsset(
        _ store: AnalysisStore,
        _ downloads: StubDownloadProvider,
        index: Int,
        createdAt: Double,
        withJobHistory: Bool = true
    ) async throws {
        let assetId = "asset-\(index)"
        let episodeId = "ep-\(index)"
        let fingerprint = "fp-\(index)"
        try await store.insertAsset(
            AnalysisAsset(
                id: assetId,
                episodeId: episodeId,
                assetFingerprint: fingerprint,
                weakFingerprint: nil,
                sourceURL: "file:///tmp/\(assetId).m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: nil,
                analysisState: SessionState.completeFull.rawValue,
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: 2_113
            )
        )
        try await store.insertBackfillJob(
            makeBackfillJob(
                jobId: "bf-\(index)",
                analysisAssetId: assetId,
                status: .queued,
                createdAt: createdAt
            )
        )
        if withJobHistory {
            try await store.insertJob(
                makeAnalysisJob(
                    jobId: "job-\(index)",
                    jobType: "preAnalysis",
                    episodeId: episodeId,
                    analysisAssetId: assetId,
                    workKey: AnalysisJob.computeWorkKey(
                        fingerprint: fingerprint,
                        analysisVersion: PreAnalysisConfig.analysisVersion,
                        jobType: "preAnalysis"
                    ),
                    sourceFingerprint: fingerprint,
                    state: "complete"
                )
            )
        }
        downloads.cachedURLs[episodeId] = URL(fileURLWithPath: "/tmp/\(assetId).m4a")
        downloads.fingerprints[episodeId] = AudioFingerprint(
            weak: fingerprint,
            strong: fingerprint
        )
    }

    /// A sweep whose ONLY yield was minting re-drives has recovered real work.
    /// Before this bead the background-task ledger's `recovered` sum was spelled
    /// out inline in `BackgroundProcessingService` and did not know about the new
    /// counter, so such a run reported `.noOp` with `jobsCompleted: 0` — the
    /// ledger built to make background recovery visible would have hidden it.
    @Test("minted re-drives count as recovered work in the background ledger")
    func mintedRedrivesCountAsRecoveredWork() async throws {
        let empty = ReconciliationReport(
            expiredLeasesRecovered: 0,
            recoveredStrandedSessionJobs: 0,
            missingFilesUnblocked: 0,
            missingFilesStillBlocked: 3,
            modelsUnblocked: 0,
            staleVersionsSuperseded: 2,
            staleVersionsReenqueued: 0,
            completedJobsGarbageCollected: 7,
            failedJobsBackedOff: 1,
            unEnqueuedDownloadsCreated: 0,
            strandedBackfillJobsReset: 0,
            strandedFinalPassJobsReset: 0,
            queuedJobEpochsRestamped: 4,
            scarcityReprioritizedJobs: 5,
            adScanRedrivesMinted: 0,
            capOutRetriesMinted: 0,
            // playhead-y8f3: also an excluded counter — a swallowed re-enqueue
            // is work that did NOT happen.
            reEnqueuesSwallowed: 9,
            // playhead-fil5: also excluded — a scan CLAIM is a request, and the
            // pass it unblocks is already counted as `adScanRedrivesMinted`.
            semanticScanClaimsMinted: 8
        )
        // The excluded counters are loaded above and must still sum to nothing:
        // they diagnose, retire, delay or re-rank, they do not recover.
        #expect(empty.recoveredWorkCount == 0)

        let withRedrives = ReconciliationReport(
            expiredLeasesRecovered: 0,
            recoveredStrandedSessionJobs: 0,
            missingFilesUnblocked: 0,
            missingFilesStillBlocked: 0,
            modelsUnblocked: 0,
            staleVersionsSuperseded: 0,
            staleVersionsReenqueued: 0,
            completedJobsGarbageCollected: 0,
            failedJobsBackedOff: 0,
            unEnqueuedDownloadsCreated: 0,
            strandedBackfillJobsReset: 0,
            strandedFinalPassJobsReset: 0,
            queuedJobEpochsRestamped: 0,
            scarcityReprioritizedJobs: 0,
            adScanRedrivesMinted: 6,
            capOutRetriesMinted: 0,
            reEnqueuesSwallowed: 0,
            semanticScanClaimsMinted: 0
        )
        #expect(withRedrives.recoveredWorkCount == 6)
    }

    /// One reconcile does not dump the whole backlog into the queue.
    @Test("a reconcile pass is capped")
    func reconcilePassIsCapped() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        for index in 0..<(AnalysisJobReconciler.maxAdScanRedrivesPerReconcile + 4) {
            try await seedStrandedAsset(
                store, downloads, index: index, createdAt: 1_000 + Double(index)
            )
        }
        let report = try await makeReconciler(store: store, downloads: downloads).reconcile()
        #expect(report.adScanRedrivesMinted == AnalysisJobReconciler.maxAdScanRedrivesPerReconcile)
    }

    /// **Skips must not consume mint slots.** The three oldest assets on the
    /// device pull are permanently unmintable — every `analysis_jobs` row for
    /// their episode was garbage-collected, so `discoverUnEnqueuedDownloads`
    /// owns them, not this step — and oldest-first ordering puts them at the
    /// head of the queue. If the cap counted candidates rather than mints they
    /// would silently eat three slots on every launch, forever.
    @Test("permanently-skipped candidates at the head of the queue do not starve the sweep")
    func skippedCandidatesDoNotConsumeMintBudget() async throws {
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        // Three unmintable assets, OLDEST — they sort first.
        for index in 0..<3 {
            try await seedStrandedAsset(
                store, downloads,
                index: index, createdAt: 1_000 + Double(index), withJobHistory: false
            )
        }
        // Then a full cap's worth of genuinely mintable ones.
        for index in 3..<(3 + AnalysisJobReconciler.maxAdScanRedrivesPerReconcile) {
            try await seedStrandedAsset(
                store, downloads, index: index, createdAt: 2_000 + Double(index)
            )
        }

        let report = try await makeReconciler(store: store, downloads: downloads).reconcile()
        #expect(report.adScanRedrivesMinted == AnalysisJobReconciler.maxAdScanRedrivesPerReconcile,
                "skipped head-of-queue candidates must not reduce the mint yield")
    }
}

// MARK: - The forward-looking path, through the real scheduler

@Suite("Ad-scan re-drive — scheduler terminal arms (playhead-onn6)")
struct AdScanRedriveSchedulerTests {

    private static let episodeId = "ep-onn6-sched"
    private static let assetId = "asset-onn6-sched"
    private static let fingerprint = "fp-onn6-sched"
    private static let durationSec: Double = 2_113

    /// An ad-detection double that does what a real coverage-lane drain does:
    /// writes `semantic_scan_results` rows for the audio it read and completes
    /// the `backfill_jobs` row it consumed. `scanTo == nil` is the pathological
    /// case — a pass that runs and reads nothing.
    private final class CoverageWritingAdDetectionStub: AdDetectionProviding, @unchecked Sendable {
        let store: AnalysisStore
        /// Seconds of audio each `runBackfill` call reads, or `nil` for "reads
        /// nothing and leaves the coverage-lane row exactly as it found it".
        let scanTo: Double?
        let backfillJobId: String
        /// When true, the double reads nothing until a re-drive row exists — so
        /// any coverage it produces is attributable to the re-drive rather than
        /// to the episode's original pass.
        let armedOnlyAfterRedriveExists: Bool
        private(set) var backfillCallCount = 0

        init(
            store: AnalysisStore,
            scanTo: Double?,
            backfillJobId: String,
            armedOnlyAfterRedriveExists: Bool = false
        ) {
            self.store = store
            self.scanTo = scanTo
            self.backfillJobId = backfillJobId
            self.armedOnlyAfterRedriveExists = armedOnlyAfterRedriveExists
        }

        func runHotPath(
            chunks: [TranscriptChunk],
            analysisAssetId: String,
            episodeDuration: Double,
            podcastId: String?
        ) async throws -> [AdWindow] { [] }

        func runBackfill(
            chunks: [TranscriptChunk],
            analysisAssetId: String,
            podcastId: String,
            episodeDuration: Double,
            sessionId: String?
        ) async throws {
            backfillCallCount += 1
            guard let scanTo else { return }
            if armedOnlyAfterRedriveExists {
                var redriveExists = false
                for state in ["queued", "running", "complete"] {
                    for job in try await store.fetchJobsByState(state)
                    where AnalysisWorkScheduler.adScanRedriveOrdinal(workKey: job.workKey) != nil {
                        redriveExists = true
                    }
                }
                guard redriveExists else { return }
            }
            try await store.insertSemanticScanResult(
                makeCoverageScan(
                    assetId: analysisAssetId,
                    index: 100 + backfillCallCount,
                    start: 0,
                    end: scanTo
                )
            )
            try await store.markBackfillJobComplete(jobId: backfillJobId, progressCursor: nil)
        }

        func revalidateFromFeatures(
            analysisAssetId: String,
            podcastId: String,
            episodeDuration: Double,
            sessionId: String?
        ) async throws {}
    }

    private func seedUnderScannedAsset(
        _ store: AnalysisStore,
        scannedSeconds: Double
    ) async throws {
        try await store.insertAsset(
            AnalysisAsset(
                id: Self.assetId,
                episodeId: Self.episodeId,
                assetFingerprint: Self.fingerprint,
                weakFingerprint: nil,
                sourceURL: "file:///tmp/onn6.m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: Self.durationSec,
                confirmedAdCoverageEndTime: nil,
                analysisState: SessionState.backfill.rawValue,
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: Self.durationSec
            )
        )
        _ = try await store.insertTranscriptChunks([
            makeCoverageChunk(assetId: Self.assetId, index: 0, start: 0, end: Self.durationSec)
        ])
        try await store.insertSemanticScanResult(
            makeCoverageScan(assetId: Self.assetId, index: 0, start: 0, end: scannedSeconds)
        )
        try await store.insertBackfillJob(
            makeBackfillJob(
                jobId: "bf-onn6",
                analysisAssetId: Self.assetId,
                phase: .fullEpisodeScan,
                status: .queued
            )
        )
        try await store.insertJob(
            makeAnalysisJob(
                jobId: "job-onn6-base",
                jobType: "preAnalysis",
                episodeId: Self.episodeId,
                analysisAssetId: Self.assetId,
                workKey: AnalysisJob.computeWorkKey(
                    fingerprint: Self.fingerprint,
                    analysisVersion: PreAnalysisConfig.analysisVersion,
                    jobType: "preAnalysis"
                ),
                sourceFingerprint: Self.fingerprint,
                priority: 10,
                desiredCoverageSec: 120,
                state: "queued"
            )
        )
    }

    private func makeScheduler(
        store: AnalysisStore,
        adDetection: any AdDetectionProviding,
        clock: @escaping @Sendable () -> Date
    ) async throws -> (AnalysisWorkScheduler, StubDownloadProvider) {
        let downloads = StubDownloadProvider()
        downloads.cachedURLs[Self.episodeId] = URL(fileURLWithPath: "/tmp/onn6.m4a")
        let audio = StubAnalysisAudioProvider()
        audio.shardsToReturn = (0..<4).map {
            makeShard(id: $0, episodeID: Self.episodeId, startTime: Double($0) * 30, duration: 30)
        }
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel()
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audio,
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: adDetection
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        let scheduler = AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(),
            downloadManager: downloads,
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider(),
            config: PreAnalysisConfig(),
            clock: clock
        )
        return (scheduler, downloads)
    }

    private func redriveWorkKeys(_ store: AnalysisStore) async throws -> [String] {
        var keys: [String] = []
        for state in ["queued", "paused", "complete", "failed", "superseded", "running"] {
            for job in try await store.fetchJobsByState(state)
            where AnalysisWorkScheduler.adScanRedriveOrdinal(workKey: job.workKey) != nil {
                keys.append(job.workKey)
            }
        }
        return keys
    }

    private func adScanFraction(_ store: AnalysisStore) async throws -> ReachRatio? {
        try await store.fetchCoverageSummariesByAssetIds([Self.assetId])[Self.assetId]?
            .adScanFraction
    }

    /// Runs the fixture to quiescence and reports what it cost.
    /// `coverageLaneDrained` seeds the control: the same episode, equally
    /// under-scanned, but with nothing left for a pass to resume.
    private func driveToQuiescence(
        coverageLaneDrained: Bool
    ) async throws -> (dispatches: Int, backfillCalls: Int, store: AnalysisStore) {
        let store = try await makeTestStore()
        try await seedUnderScannedAsset(store, scannedSeconds: 300)
        if coverageLaneDrained {
            try await store.markBackfillJobComplete(jobId: "bf-onn6", progressCursor: nil)
        }
        // The scheduler's own clock, so a re-queue's exponential backoff can be
        // stepped over deterministically instead of slept through.
        let now = AdScanRedriveClock(start: Date(timeIntervalSince1970: 1_700_000_000))
        let adDetection = CoverageWritingAdDetectionStub(
            store: store,
            scanTo: nil,
            backfillJobId: "bf-onn6"
        )
        let (scheduler, _) = try await makeScheduler(
            store: store,
            adDetection: adDetection,
            clock: { now.value }
        )
        var dispatches = 0
        for _ in 0..<15 {
            if await scheduler.processNextDispatchableJobForTesting() {
                dispatches += 1
            }
            now.advance(by: 7_200)
        }
        return (dispatches, adDetection.backfillCallCount, store)
    }

    /// **Termination, measured differentially.** Many cycles against a scan that
    /// never advances must stop. Each dispatch runs the whole real pipeline; the
    /// ad-detection double reads nothing, so the coverage lane stays resumable
    /// forever and only the ordinal budget can end the chain.
    ///
    /// The cost assertion is the difference against a control episode whose
    /// coverage lane is drained (so no re-drive is ever minted) rather than an
    /// absolute count, because how many passes the ORIGINAL job needs before it
    /// terminates is fixture detail this bead does not own. What this bead owns is
    /// exactly how many EXTRA full pipeline passes a re-drive costs the user's
    /// battery: `maxAdScanRedrives`, and not one more.
    @Test("a never-progressing scan terminates at the re-drive budget")
    func nonProgressingScanTerminates() async throws {
        let control = try await driveToQuiescence(coverageLaneDrained: true)
        let (dispatches, backfillCalls, store) = try await driveToQuiescence(
            coverageLaneDrained: false
        )
        let before = try await adScanFraction(store)
        let now = AdScanRedriveClock(start: Date(timeIntervalSince1970: 1_700_000_000))
        now.advance(by: 15 * 7_200)

        #expect(dispatches == control.dispatches + AnalysisWorkScheduler.maxAdScanRedrives,
                "re-drives cost \(dispatches - control.dispatches) extra passes, expected \(AnalysisWorkScheduler.maxAdScanRedrives)")
        #expect(backfillCalls == dispatches,
                "every dispatch must reach the semantic backfill exactly once")
        #expect(try await redriveWorkKeys(control.store).isEmpty,
                "the control must not mint — otherwise the difference measures nothing")

        let keys = try await redriveWorkKeys(store)
        #expect(Set(keys).count == AnalysisWorkScheduler.maxAdScanRedrives,
                "expected exactly \(AnalysisWorkScheduler.maxAdScanRedrives) re-drives, got \(keys)")
        // The minted rows are background-lane repair work and carry the
        // predecessor's coverage, so a no-op pass reads as no-progress rather than
        // as fresh advancement (which would re-queue it instead of terminating).
        let base = try #require(try await store.fetchJob(byId: "job-onn6-base"))
        for job in try await store.fetchJobsByState("complete")
        where AnalysisWorkScheduler.adScanRedriveOrdinal(workKey: job.workKey) != nil {
            #expect(job.priority == 0, "a re-drive must never preempt user-facing work")
            #expect(job.analysisAssetId == Self.assetId)
            #expect(job.featureCoverageSec == base.featureCoverageSec)
            #expect(job.transcriptCoverageSec == base.transcriptCoverageSec)
            #expect(job.cueCoverageSec == base.cueCoverageSec)
        }
        // Nothing dispatchable remains, and the coverage-lane row is untouched.
        #expect(try await store.fetchNextEligibleJob(
            deferredWorkAllowed: true,
            t0ThresholdSec: 300,
            now: now.value.timeIntervalSince1970
        ) == nil)
        #expect(try await store.fetchBackfillJob(byId: "bf-onn6")?.status == .queued)
        #expect(try await adScanFraction(store) == before,
                "a scan that read nothing must not move measured coverage")
    }

    /// **Coverage genuinely moves, and the re-drive is what moved it.**
    ///
    /// Asserting only "coverage is higher at the end" would pass even if the
    /// re-drive did nothing, because the episode's ORIGINAL pass also calls
    /// `runBackfill`. So the double is armed to read the audio only once a
    /// re-drive row exists, and the test checks the attribution directly: on
    /// every pass where measured `adScanFraction` rose, a re-drive had already
    /// been minted. A job that runs and achieves nothing is the bug, not the fix.
    ///
    /// It also pins the other half — once the audio HAS been read, the chain
    /// stops on its own without spending the ordinal budget.
    @Test("the re-drive is what moves measured coverage, and then the chain stops")
    func progressingRedriveMovesCoverageThenStops() async throws {
        let store = try await makeTestStore()
        try await seedUnderScannedAsset(store, scannedSeconds: 300)
        let initial = try #require(try await adScanFraction(store))
        #expect(initial < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction)

        let now = AdScanRedriveClock(start: Date(timeIntervalSince1970: 1_700_000_000))
        let adDetection = CoverageWritingAdDetectionStub(
            store: store,
            scanTo: Self.durationSec,
            backfillJobId: "bf-onn6",
            // The original pass reads nothing (the thermal/rate-limited shape
            // that strands an episode in the first place). Only a pass licensed
            // by a re-drive reads the audio.
            armedOnlyAfterRedriveExists: true
        )
        let (scheduler, _) = try await makeScheduler(
            store: store,
            adDetection: adDetection,
            clock: { now.value }
        )

        var sawAttributedIncrease = false
        for _ in 0..<10 {
            let redrivesBefore = try await redriveWorkKeys(store).count
            let fractionBefore = try await adScanFraction(store)
            _ = await scheduler.processNextDispatchableJobForTesting()
            let fractionAfter = try await adScanFraction(store)
            if let fractionAfter, let fractionBefore, fractionAfter > fractionBefore {
                sawAttributedIncrease = true
                #expect(redrivesBefore >= 1,
                        "coverage rose on a pass no re-drive had licensed")
            }
            now.advance(by: 7_200)
        }

        #expect(sawAttributedIncrease, "the re-drive never produced any coverage")
        let after = try #require(try await adScanFraction(store))
        #expect(after > initial, "coverage must actually increase: \(initial) → \(after)")
        #expect(after >= AnalysisJobRunner.semanticBackfillSufficientAdScanFraction)
        #expect(try await store.fetchBackfillJob(byId: "bf-onn6")?.status == .complete,
                "the orphaned coverage-lane row must have been drained")
        // One re-drive sufficed: the structural guard ended the chain before the
        // absolute cap was reached.
        #expect(try await redriveWorkKeys(store).count == 1)
        #expect(try await store.fetchNextEligibleJob(
            deferredWorkAllowed: true,
            t0ThresholdSec: 300,
            now: now.value.timeIntervalSince1970
        ) == nil, "a fully-scanned asset must not keep re-driving")
    }

    /// The scheduler tests above drive the `coverageInsufficient.noProgress` arm
    /// end-to-end. The other two terminal-success arms (`allTiersDone`,
    /// `coverageInsufficient.maxAttempts`) need a full cue-coverage pipeline to
    /// reach in stub form — see the header of
    /// `AnalysisWorkSchedulerJournalEmissionTests`. They are pinned the way
    /// playhead-beh3's grant-window wiring is pinned: a source canary, so a
    /// refactor that drops the mint from one arm regresses here instead of
    /// silently re-stranding a third of the terminal shapes.
    @Test("every terminal-success arm carries the re-drive mint")
    func everyTerminalArmMintsRedrive() throws {
        let url = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()   // PreAnalysis
            .deletingLastPathComponent()   // Services
            .deletingLastPathComponent()   // PlayheadTests
            .deletingLastPathComponent()   // repo root
            .appendingPathComponent("Playhead/Services/PreAnalysis/AnalysisWorkScheduler.swift")
        let source = try String(contentsOf: url, encoding: .utf8)

        // Exactly three call sites plus the definition, and exactly three
        // insertions. Counting globally is what makes the per-arm check below
        // honest: a window-based check alone can be satisfied by a NEIGHBOURING
        // arm's mint bleeding into the window.
        #expect(source.components(separatedBy: "adScanRedriveJob(").count - 1 == 4,
                "expected 1 definition + 3 terminal-arm call sites")
        #expect(source.components(separatedBy: "insertNextJob: redrive").count - 1 == 3,
                "expected the mint threaded into all three terminal-success arms")

        for tag in [
            "\"allTiersDone\"",
            "\"coverageInsufficient.noProgress\"",
            "\"coverageInsufficient.maxAttempts\"",
        ] {
            guard let tagRange = source.range(of: tag) else {
                Issue.record("terminal arm tag \(tag) missing from AnalysisWorkScheduler.swift")
                continue
            }
            // The mint is computed just before the arm tag and threaded into the
            // commit that follows it. Bound the forward window at that commit's
            // own `stateUpdate:` so it cannot reach the next arm.
            let rest = source[tagRange.upperBound...]
            guard let stateUpdate = rest.range(of: "stateUpdate:") else {
                Issue.record("no stateUpdate found after arm tag \(tag)")
                continue
            }
            #expect(rest[..<stateUpdate.lowerBound].contains("insertNextJob: redrive"),
                    "expected the \(tag) arm to insert the minted re-drive")

            let leadIn = source.index(tagRange.lowerBound, offsetBy: -1_200, limitedBy: source.startIndex)
                ?? source.startIndex
            #expect(source[leadIn..<tagRange.lowerBound].contains("adScanRedriveJob("),
                    "expected the ad-scan re-drive decision just before the \(tag) arm")
        }
    }

    /// The 644F2551 shape driven through the real scheduler: coverage is short,
    /// but the lane is drained, so a further pass could read nothing. No re-drive
    /// may be minted.
    @Test("a drained coverage lane mints nothing through the scheduler either")
    func drainedLaneMintsNothingThroughScheduler() async throws {
        let store = try await makeTestStore()
        try await seedUnderScannedAsset(store, scannedSeconds: 140)
        try await store.markBackfillJobComplete(jobId: "bf-onn6", progressCursor: nil)

        let now = AdScanRedriveClock(start: Date(timeIntervalSince1970: 1_700_000_000))
        let adDetection = CoverageWritingAdDetectionStub(
            store: store,
            scanTo: nil,
            backfillJobId: "bf-onn6"
        )
        let (scheduler, _) = try await makeScheduler(
            store: store,
            adDetection: adDetection,
            clock: { now.value }
        )
        for _ in 0..<6 {
            _ = await scheduler.processNextDispatchableJobForTesting()
            now.advance(by: 7_200)
        }
        #expect(try await redriveWorkKeys(store).isEmpty)
    }
}

/// A settable clock so scheduler tests can step over exponential backoff
/// deterministically rather than sleeping through it.
private final class AdScanRedriveClock: @unchecked Sendable {
    private let lock = NSLock()
    private var current: Date

    init(start: Date) { current = start }

    var value: Date {
        lock.lock()
        defer { lock.unlock() }
        return current
    }

    func advance(by seconds: TimeInterval) {
        lock.lock()
        defer { lock.unlock() }
        current = current.addingTimeInterval(seconds)
    }
}

// MARK: - Shared fixtures

/// A fast-pass transcript chunk spanning `[start, end]`. The ad-scan area is the
/// scan windows INTERSECTED with the transcribed region, so a fixture that wants
/// measurable coverage has to state both.
private func makeCoverageChunk(
    assetId: String,
    index: Int,
    start: Double,
    end: Double
) -> TranscriptChunk {
    TranscriptChunk(
        id: "\(assetId)-chunk-\(index)",
        analysisAssetId: assetId,
        segmentFingerprint: "\(assetId)-seg-\(index)",
        chunkIndex: index,
        startTime: start,
        endTime: end,
        text: "segment \(index)",
        normalizedText: "segment \(index)",
        pass: "fast",
        modelVersion: "test-asr",
        transcriptVersion: "tx-v1",
        atomOrdinal: index
    )
}

/// A coverage-lane (`passA`) semantic scan row spanning `[start, end]`.
private func makeCoverageScan(
    assetId: String,
    index: Int,
    start: Double,
    end: Double
) -> SemanticScanResult {
    SemanticScanResult(
        id: "\(assetId)-scan-\(index)",
        analysisAssetId: assetId,
        windowFirstAtomOrdinal: index * 10,
        windowLastAtomOrdinal: index * 10 + 9,
        windowStartTime: start,
        windowEndTime: end,
        scanPass: SemanticScanCoverage.coverageScanPass,
        transcriptQuality: .good,
        disposition: .noAds,
        spansJSON: "[]",
        status: .success,
        attemptCount: 1,
        errorContext: nil,
        inputTokenCount: nil,
        outputTokenCount: nil,
        latencyMs: nil,
        prewarmHit: false,
        scanCohortJSON: makeCohortJSON(promptLabel: "onn6"),
        transcriptVersion: "tx-v1",
        reuseScope: "\(assetId)-scan-\(index)"
    )
}
