// DayZeroDownloadTimeStoreTests.swift
// playhead-4dqe: the DURABLE half — the kickoff ledger that makes a silent
// give-up impossible, and the rolling day-0 byte window that bounds the spend
// once the transport setting can open the cellular door.
//
// Two tables, two questions, deliberately not one:
//
//   `rediff_day_zero_kickoffs`  — per EPISODE. "was a kickoff requested, and did
//                                 it ever reach the trigger?" Requires nothing
//                                 but an episode id, which is the whole point:
//                                 the give-up we most need to record is the one
//                                 where no `analysis_assets` row exists to hang
//                                 a record off (playhead-p70f's
//                                 `assetRowMissing` documents that tautology).
//   `rediff_bandwidth_ledger`   — the rolling 24 h day-0 window. HOW MUCH, on
//                                 BOTH transports.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("Day-0 kickoff ledger + daily budget persistence (playhead-4dqe)")
struct DayZeroDownloadTimeStoreTests {

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    // MARK: - The kickoff ledger

    @Test("A KICKOFF FOR AN EPISODE WITH NO ASSET ROW IS RECORDABLE — the whole reason this table is episode-keyed")
    func recordsKickoffWithoutAnyAsset() async throws {
        let store = try await makeTestStore()
        // Deliberately NO `insertAsset`. This is the pre-ewag state: the file
        // downloaded, nothing registered an asset, and `rediff_day_zero_attempts`
        // could not have held the evidence because its FK had nothing to point at.
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-orphan",
            source: .backgroundDownload,
            outcome: .noAnalysisAsset,
            pollCount: 40,
            waitedSeconds: 390,
            at: 1_700_000_000
        )
        let record = try #require(try await store.fetchRediffDayZeroKickoff(episodeId: "ep-orphan"))
        #expect(record.lastOutcome == .noAnalysisAsset)
        #expect(record.kickoffCount == 1)
        #expect(record.firedCount == 0)
        #expect(record.gaveUpCount == 1)
        #expect(record.lastSource == .backgroundDownload)
        #expect(record.lastPollCount == 40)
        #expect(record.lastWaitedSeconds == 390)
    }

    @Test("counts ACCUMULATE per episode so `kickoffCount` large + `firedCount` zero reads as the pre-ewag failure")
    func countsAccumulate() async throws {
        let store = try await makeTestStore()
        for index in 0..<3 {
            // playhead-kg8h: a real kickoff is a CLAIM followed by a settle, and
            // `kickoffCount` now counts the claims (which is what its name and
            // its own doc comment have always said). Driving three settles with
            // no claims would be a state the coordinator cannot produce.
            try await store.noteRediffDayZeroKickoffClaim(
                episodeId: "ep-1", source: .backgroundDownload,
                enclosureURL: nil, publishedAt: nil,
                at: 1_000 + Double(index)
            )
            try await store.noteRediffDayZeroKickoff(
                episodeId: "ep-1",
                source: .backgroundDownload,
                outcome: .noAnalysisAsset,
                pollCount: 40,
                waitedSeconds: 390,
                at: 1_000 + Double(index)
            )
        }
        let record = try #require(try await store.fetchRediffDayZeroKickoff(episodeId: "ep-1"))
        #expect(record.kickoffCount == 3)
        #expect(record.gaveUpCount == 3)
        #expect(record.firedCount == 0)
        #expect(record.updatedAt == 1_002)
    }

    @Test("a kickoff that FIRED increments only `firedCount` — a success is never counted as a give-up")
    func firedIsNotAGiveUp() async throws {
        let store = try await makeTestStore()
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-ok", source: .downloadAndAnalyzeTap,
            outcome: .fired, pollCount: 2, waitedSeconds: 10, at: 500
        )
        let record = try #require(try await store.fetchRediffDayZeroKickoff(episodeId: "ep-ok"))
        #expect(record.firedCount == 1)
        #expect(record.gaveUpCount == 0)
        #expect(record.kickoffCount == 1)
    }

    @Test("a mixed history keeps BOTH numbers — a device that recovered is distinguishable from one that never worked")
    func mixedHistoryKeepsBothNumbers() async throws {
        let store = try await makeTestStore()
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-mix", source: .backgroundDownload,
                enclosureURL: nil, publishedAt: nil,
                at: 100
        )
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-mix", source: .backgroundDownload,
            outcome: .noAnalysisAsset, pollCount: 40, waitedSeconds: 390, at: 100
        )
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-mix", source: .backgroundDownload,
                enclosureURL: nil, publishedAt: nil,
                at: 200
        )
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-mix", source: .backgroundDownload,
            outcome: .fired, pollCount: 3, waitedSeconds: 20, at: 200
        )
        let record = try #require(try await store.fetchRediffDayZeroKickoff(episodeId: "ep-mix"))
        #expect(record.kickoffCount == 2)
        #expect(record.firedCount == 1)
        #expect(record.gaveUpCount == 1)
        #expect(record.lastOutcome == .fired)
    }

    // MARK: - playhead-kg8h: the durable claim

    @Test("THE ACCEPTANCE, PERSISTED: a claim alone is a queryable row — a kickoff that never settled")
    func claimAloneLeavesAQueryableRow() async throws {
        let store = try await makeTestStore()
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-claim", source: .backgroundDownload,
                enclosureURL: nil, publishedAt: nil,
                at: 1_700_000_000
        )
        let record = try #require(try await store.fetchRediffDayZeroKickoff(episodeId: "ep-claim"))
        #expect(record.lastOutcome == .requested)
        #expect(record.kickoffCount == 1)
        #expect(record.firedCount == 0)
        #expect(record.gaveUpCount == 0,
                "a kickoff still owed is not a kickoff that gave up — conflating them is how the in-memory queue's losses would read as network failures")
        #expect(record.lastSource == .backgroundDownload)
        #expect(record.updatedAt == 1_700_000_000)
    }

    @Test("a CLAIM then a SETTLE is ONE kickoff, not two — the claim owns `kickoffCount`")
    func claimThenSettleCountsOneKickoff() async throws {
        let store = try await makeTestStore()
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-cs", source: .downloadAndAnalyzeTap,
                enclosureURL: nil, publishedAt: nil,
                at: 100
        )
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-cs", source: .downloadAndAnalyzeTap,
            outcome: .fired, pollCount: 2, waitedSeconds: 15.8, at: 116
        )
        let record = try #require(try await store.fetchRediffDayZeroKickoff(episodeId: "ep-cs"))
        #expect(record.kickoffCount == 1, "double-counting would break the pre-ewag reading this table exists for")
        #expect(record.firedCount == 1)
        #expect(record.gaveUpCount == 0)
        #expect(record.lastOutcome == .fired)
        #expect(record.lastPollCount == 2)
        #expect(record.lastWaitedSeconds == 15.8)
    }

    @Test("`kickoffCount - (firedCount + gaveUpCount)` counts the kickoffs the process never lived to settle")
    func unsettledKickoffsAreCountable() async throws {
        let store = try await makeTestStore()
        // Two claims, one settle: the second kickoff is still owed — the shape a
        // serial drain (playhead-kxgh) leaves behind when the wake window ends.
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-owed", source: .backgroundDownload,
                enclosureURL: nil, publishedAt: nil,
                at: 100
        )
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-owed", source: .backgroundDownload,
            outcome: .fired, pollCount: 1, waitedSeconds: 3, at: 110
        )
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-owed", source: .backgroundDownload,
                enclosureURL: nil, publishedAt: nil,
                at: 200
        )
        let record = try #require(try await store.fetchRediffDayZeroKickoff(episodeId: "ep-owed"))
        #expect(record.kickoffCount == 2)
        #expect(record.firedCount + record.gaveUpCount == 1)
        #expect(record.kickoffCount - (record.firedCount + record.gaveUpCount) == 1)
        #expect(record.lastOutcome == .requested, "the row's last word is that a kickoff is owed")
    }

    @Test("a settle whose claim write FAILED still records the kickoff rather than a row claiming zero of them")
    func settleWithoutAClaimStillCountsOne() async throws {
        let store = try await makeTestStore()
        // The claim goes through `try?` in production, so a transient SQLite
        // failure is possible. The settle must not then write kickoffCount = 0.
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-noclaim", source: .backgroundDownload,
            outcome: .noPinnedFile, pollCount: 40, waitedSeconds: 390, at: 500
        )
        let record = try #require(try await store.fetchRediffDayZeroKickoff(episodeId: "ep-noclaim"))
        #expect(record.kickoffCount == 1)
        #expect(record.gaveUpCount == 1)
    }

    @Test("a claim never RESURRECTS a settled row's evidence — the poll count and wait reset with it")
    func aFreshClaimResetsTheSettledEvidence() async throws {
        let store = try await makeTestStore()
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-reset", source: .backgroundDownload,
                enclosureURL: nil, publishedAt: nil,
                at: 100
        )
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-reset", source: .backgroundDownload,
            outcome: .noAnalysisAsset, pollCount: 40, waitedSeconds: 390, at: 490
        )
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-reset", source: .downloadAndAnalyzeTap,
                enclosureURL: nil, publishedAt: nil,
                at: 600
        )
        let record = try #require(try await store.fetchRediffDayZeroKickoff(episodeId: "ep-reset"))
        #expect(record.lastPollCount == 0,
                "carrying the PRIOR kickoff's 40 polls onto an outstanding claim would report a wait that this kickoff has not run")
        #expect(record.lastWaitedSeconds == 0)
        #expect(record.lastSource == .downloadAndAnalyzeTap)
        #expect(record.gaveUpCount == 1, "the prior give-up is history and stays counted")
    }

    // MARK: - playhead-kg8h R2 (F1): the counters cannot disagree

    @Test("""
    F1 — THE FIX: a settle whose claim write FAILED on an ALREADY-SETTLED \
    episode accounts for the claim it never saw, instead of driving the owed \
    count NEGATIVE
    """)
    func failedClaimOnASettledEpisodeCannotGoNegative() async throws {
        let store = try await makeTestStore()
        // One healthy kickoff: claim, then fire. `k=1, f=1`.
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-f1", source: .backgroundDownload,
                enclosureURL: nil, publishedAt: nil,
                at: 100
        )
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-f1", source: .backgroundDownload,
            outcome: .fired, pollCount: 1, waitedSeconds: 4, at: 110
        )

        // A SECOND genuine kickoff whose CLAIM WRITE FAILS. The design already
        // anticipates this — it is why the settle's INSERT writes 1 — but that
        // branch only runs when NO ROW EXISTS. Here a row does, so pre-fix the
        // settle folded in through ON CONFLICT adding only to `firedCount`,
        // leaving `k=1, f=2, g=0`.
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-f1", source: .backgroundDownload,
            outcome: .fired, pollCount: 1, waitedSeconds: 4, at: 200
        )

        let record = try #require(try await store.fetchRediffDayZeroKickoff(episodeId: "ep-f1"))
        #expect(record.firedCount == 2)
        #expect(record.kickoffCount == 2, """
            The settle must account for the claim it never saw, exactly as its own \
            INSERT branch already does for a fresh episode. Two kickoffs happened; a \
            `kickoffCount` of 1 says one did.
            """)
        #expect(record.kickoffCount - (record.firedCount + record.gaveUpCount) == 0, """
            TWO kickoffs, BOTH settled: nothing is owed. Pre-fix this read -1 — not \
            "kickoffs owed" under any interpretation, and it CANCELS a genuinely-owed \
            +1 in any fleet roll-up.
            """)
    }

    @Test("""
    F1 — THE CONSEQUENCE: a fleet roll-up cannot cancel, so one episode's lost \
    claim can no longer hide another episode's owed kickoff
    """)
    func fleetRollUpCannotCancelAnOwedKickoff() async throws {
        let store = try await makeTestStore()

        // Episode A: a kickoff that is GENUINELY OWED — claimed, never settled.
        // This is the loss the whole bead exists to make visible.
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-owed-a", source: .backgroundDownload,
                enclosureURL: nil, publishedAt: nil,
                at: 100
        )

        // Episode B: settled twice, the second kickoff's claim write having failed.
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-lostclaim-b", source: .backgroundDownload,
                enclosureURL: nil, publishedAt: nil,
                at: 200
        )
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-lostclaim-b", source: .backgroundDownload,
            outcome: .fired, pollCount: 1, waitedSeconds: 4, at: 210
        )
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-lostclaim-b", source: .backgroundDownload,
            outcome: .noAnalysisAsset, pollCount: 40, waitedSeconds: 390, at: 300
        )

        let rows = try await store.fetchRediffDayZeroKickoffs()
        let owed = rows.reduce(0) { $0 + $1.kickoffCount - ($1.firedCount + $1.gaveUpCount) }
        #expect(owed == 1, """
            The fleet number a device pull reads is \
            `SUM(kickoffCount) - SUM(firedCount + gaveUpCount)`. Pre-fix episode B \
            contributed -1 and cancelled episode A's real +1, reporting a fleet with \
            NOTHING owed while a kickoff sat lost. A silently-cancelling roll-up is \
            worse than a wrong per-episode row: nobody goes looking.
            """)
        #expect(rows.allSatisfy { $0.kickoffCount >= $0.firedCount + $0.gaveUpCount }, """
            `kickoffCount >= firedCount + gaveUpCount` is an invariant of the two \
            writers, not a coincidence: a claim raises `kickoffCount` and marks the \
            row `requested`, and a settle either CONSUMES that marker (adding no \
            kickoff) or adds its own.
            """)
    }

    @Test("""
    F1 — ANTI-VACUITY: the settle still does NOT double-count a kickoff whose \
    claim DID land, for every settled outcome
    """)
    func settleDoesNotDoubleCountAClaimedKickoff() async throws {
        // Without this, "always increment in the update branch" would pass the two
        // tests above while inflating `kickoffCount` on every healthy kickoff —
        // breaking the "large `kickoffCount`, zero `firedCount`" reading this table
        // exists for, in the opposite direction.
        for (index, outcome) in [
            RediffDayZeroKickoffOutcome.fired,
            .noPinnedFile,
            .noAnalysisAsset,
            .cancelled
        ].enumerated() {
            let store = try await makeTestStore()
            let episodeId = "ep-nodouble-\(index)"
            try await store.noteRediffDayZeroKickoffClaim(
                episodeId: episodeId, source: .backgroundDownload,
                enclosureURL: nil, publishedAt: nil,
                at: 100
            )
            try await store.noteRediffDayZeroKickoff(
                episodeId: episodeId, source: .backgroundDownload,
                outcome: outcome, pollCount: 3, waitedSeconds: 30, at: 110
            )
            let record = try #require(
                try await store.fetchRediffDayZeroKickoff(episodeId: episodeId)
            )
            #expect(record.kickoffCount == 1,
                    "one claim + one settle is ONE kickoff, whatever the outcome (\(outcome.rawValue))")
            #expect(record.kickoffCount - (record.firedCount + record.gaveUpCount) == 0,
                    "a settled kickoff owes nothing (\(outcome.rawValue))")
        }
    }

    @Test("""
    F1 — the owed count never goes negative across a long mixed history, \
    including repeated claim-write failures
    """)
    func owedCountNeverGoesNegativeOverAMixedHistory() async throws {
        let store = try await makeTestStore()
        // `C` = the claim lands; `F`/`G` = a settle. A settle with no outstanding
        // claim is a kickoff whose claim write failed — the F1 case — and there
        // are three here, two of them back to back. Pre-fix this history reaches
        // -2 part-way through and ends at -1.
        let script: [Character] = ["C", "F", "F", "C", "C", "G", "F", "G", "C"]
        var stamp = 100.0
        for step in script {
            stamp += 10
            switch step {
            case "C":
                try await store.noteRediffDayZeroKickoffClaim(
                    episodeId: "ep-mixed", source: .backgroundDownload,
                enclosureURL: nil, publishedAt: nil,
                at: stamp
                )
            case "F":
                try await store.noteRediffDayZeroKickoff(
                    episodeId: "ep-mixed", source: .backgroundDownload,
                    outcome: .fired, pollCount: 1, waitedSeconds: 3, at: stamp
                )
            default:
                try await store.noteRediffDayZeroKickoff(
                    episodeId: "ep-mixed", source: .backgroundDownload,
                    outcome: .cancelled, pollCount: 2, waitedSeconds: 8, at: stamp
                )
            }
            let step = try #require(
                try await store.fetchRediffDayZeroKickoff(episodeId: "ep-mixed")
            )
            #expect(step.kickoffCount >= step.firedCount + step.gaveUpCount, """
                the owed count went NEGATIVE part-way through the history. The \
                invariant has to hold at EVERY step, not only at the end, because a \
                device pull reads whatever the row says at the moment it is taken.
                """)
        }

        let record = try #require(
            try await store.fetchRediffDayZeroKickoff(episodeId: "ep-mixed")
        )
        // 4 claims that landed + 3 settles whose claim write failed = 7 kickoffs,
        // 5 of them settled.
        #expect(record.kickoffCount == 7)
        #expect(record.firedCount == 3)
        #expect(record.gaveUpCount == 2)
        #expect(record.kickoffCount - (record.firedCount + record.gaveUpCount) == 2)
    }

    @Test("an unknown episode reads nil, not a zeroed row that would look like a healthy kickoff")
    func unknownEpisodeIsNil() async throws {
        let store = try await makeTestStore()
        #expect(try await store.fetchRediffDayZeroKickoff(episodeId: "never") == nil)
    }

    @Test("the kickoff table survives an asset being inserted later — it is NOT foreign-keyed")
    func kickoffOutlivesAssetRegistration() async throws {
        let store = try await makeTestStore()
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-a1", source: .backgroundDownload,
            outcome: .noAnalysisAsset, pollCount: 40, waitedSeconds: 390, at: 100
        )
        try await store.insertAsset(makeAsset(id: "a1"))
        let record = try #require(try await store.fetchRediffDayZeroKickoff(episodeId: "ep-a1"))
        #expect(record.gaveUpCount == 1)
    }

    // MARK: - The rolling day-0 byte window

    @Test("a fresh ledger has NEVER spent — not a window that started at time 0")
    func freshBudgetWindowIsUnstarted() async throws {
        let store = try await makeTestStore()
        let window = try await store.fetchRediffDayZeroBudgetWindow()
        #expect(window.startedAt == nil)
        #expect(window.spentBytes == 0)
    }

    @Test("a spend starts the window and accumulates into it")
    func spendAccumulates() async throws {
        let store = try await makeTestStore()
        try await store.recordRediffDayZeroBudgetSpend(bytes: 120_300_000, at: 1_000)
        try await store.recordRediffDayZeroBudgetSpend(bytes: 131_800_000, at: 2_000)
        let window = try await store.fetchRediffDayZeroBudgetWindow()
        #expect(window.startedAt == 1_000)
        #expect(window.spentBytes == 120_300_000 + 131_800_000)
    }

    @Test("a spend more than 24 h after the window started ROLLS it — yesterday's bytes do not bind today")
    func windowRollsInTheStore() async throws {
        let store = try await makeTestStore()
        try await store.recordRediffDayZeroBudgetSpend(
            bytes: RediffDayZeroDailyBudget.dailyCapBytes, at: 1_000
        )
        try await store.recordRediffDayZeroBudgetSpend(
            bytes: 65_000_000, at: 1_000 + RediffDayZeroDailyBudget.windowSeconds + 1
        )
        let window = try await store.fetchRediffDayZeroBudgetWindow()
        #expect(window.startedAt == 1_000 + RediffDayZeroDailyBudget.windowSeconds + 1)
        #expect(window.spentBytes == 65_000_000)
    }

    @Test("the budget window shares the ledger row and does NOT disturb the cumulative counters")
    func budgetWindowCoexistsWithTotals() async throws {
        let store = try await makeTestStore()
        try await store.accumulateRediffBandwidth(
            precheckBytes: 1_000, fullFetchBytes: 54_000_000,
            unchangedCount: 0, rotatedCount: 1, failedCount: 0, parkedCount: 0,
            at: 900
        )
        try await store.recordRediffDayZeroBudgetSpend(bytes: 130_000_000, at: 1_000)
        let totals = try await store.fetchRediffBandwidthTotals()
        #expect(totals.fullFetchBytesTotal == 54_000_000,
                "the day-0 WINDOW is a different quantity from the cumulative total")
        #expect(totals.rotatedCount == 1)
        let window = try await store.fetchRediffDayZeroBudgetWindow()
        #expect(window.spentBytes == 130_000_000)
    }

    // MARK: - Migration

    @Test("THE UPGRADE: a populated pre-4dqe database gains the table and the window with nothing lost")
    func populatedDatabaseUpgrades() async throws {
        let (bootstrap, dir) = try await makeTestStoreWithDirectory()
        try await bootstrap.accumulateRediffBandwidth(
            precheckBytes: 0, fullFetchBytes: 299_600_000,
            unchangedCount: 0, rotatedCount: 0, failedCount: 0, parkedCount: 0,
            at: 1_700_000_000
        )
        try await rewindToPre4dqe(bootstrap)

        #expect(!(try probeTableExists(in: dir, table: "rediff_day_zero_kickoffs")))
        #expect(!(try probeColumnExists(
            in: dir, table: "rediff_bandwidth_ledger", column: "dayZeroBudgetSpentBytes"
        )))

        AnalysisStore.resetMigratedPathsForTesting()
        let upgraded = try AnalysisStore(directory: dir)
        try await upgraded.migrate()

        #expect(try await upgraded.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: "rediff_day_zero_kickoffs"))
        #expect(try probeColumnExists(
            in: dir, table: "rediff_bandwidth_ledger", column: "dayZeroBudgetSpentBytes"
        ))
        #expect(try probeColumnExists(
            in: dir, table: "rediff_bandwidth_ledger", column: "dayZeroBudgetWindowStartedAt"
        ))

        let totals = try await upgraded.fetchRediffBandwidthTotals()
        #expect(totals.fullFetchBytesTotal == 299_600_000, "the pre-existing ledger row survived")
        let window = try await upgraded.fetchRediffDayZeroBudgetWindow()
        #expect(window.startedAt == nil, "a backfilled window must not invent a day that was never spent")
        #expect(window.spentBytes == 0)
    }

    @Test("the migration is idempotent")
    func migrationIsIdempotent() async throws {
        let (store, dir) = try await makeTestStoreWithDirectory()
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-idem", source: .backgroundDownload,
            outcome: .noPinnedFile, pollCount: 5, waitedSeconds: 50, at: 10
        )
        try await store.recordRediffDayZeroBudgetSpend(bytes: 1_234, at: 20)

        AnalysisStore.resetMigratedPathsForTesting()
        let reopened = try AnalysisStore(directory: dir)
        try await reopened.migrate()
        try await reopened.migrate()

        #expect(try await reopened.fetchRediffDayZeroKickoff(episodeId: "ep-idem")?.gaveUpCount == 1)
        #expect(try await reopened.fetchRediffDayZeroBudgetWindow().spentBytes == 1_234)
    }

    /// Drop everything playhead-4dqe adds, and rewind the schema version, so the
    /// migration under test genuinely runs on the next open.
    private func rewindToPre4dqe(_ store: AnalysisStore) async throws {
        try await store.execForTesting("DROP TABLE IF EXISTS rediff_day_zero_kickoffs")
        try await store.execForTesting(
            "ALTER TABLE rediff_bandwidth_ledger DROP COLUMN dayZeroBudgetWindowStartedAt"
        )
        try await store.execForTesting(
            "ALTER TABLE rediff_bandwidth_ledger DROP COLUMN dayZeroBudgetSpentBytes"
        )
        // playhead-hx6n: pinned to the LITERAL 40, not `currentSchemaVersion - 1`.
        // "Pre-4dqe" is v40 — a fixed historical fact — and expressing it
        // relative to head silently stopped meaning that the moment head moved
        // past 41: the rewind then landed ON 41, V41's `observed < 41` guard
        // declined to run, and the test asserted the absence of artifacts it had
        // just prevented from being created. It went red on the V42 bump having
        // been correct for exactly one schema version.
        try await store.execForTesting(
            "UPDATE _meta SET value = '40' WHERE key = 'schema_version'"
        )
    }

    private func probeTableExists(in dir: URL, table: String) throws -> Bool {
        try probeScalarExists(
            in: dir,
            sql: "SELECT 1 FROM sqlite_master WHERE type='table' AND name='\(table)'"
        )
    }

    private func probeColumnExists(in dir: URL, table: String, column: String) throws -> Bool {
        try probeScalarExists(
            in: dir,
            sql: "SELECT 1 FROM pragma_table_info('\(table)') WHERE name='\(column)'"
        )
    }

    private func probeScalarExists(in dir: URL, sql: String) throws -> Bool {
        var handle: OpaquePointer?
        let path = dir.appendingPathComponent("analysis.sqlite").path
        guard sqlite3_open_v2(path, &handle, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            return false
        }
        defer { sqlite3_close(handle) }
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(handle, sql, -1, &statement, nil) == SQLITE_OK else { return false }
        defer { sqlite3_finalize(statement) }
        return sqlite3_step(statement) == SQLITE_ROW
    }
}

// MARK: - The trigger: transport + budget, both recorded

/// Captures the suppressions the trigger records so a refusal is provably not
/// silent.
final class SuppressionSpy: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [(assetId: String, exit: RediffDayZeroExit, at: Double)] = []

    func record(_ assetId: String, _ exit: RediffDayZeroExit, _ at: Double) {
        lock.lock(); defer { lock.unlock() }
        storage.append((assetId, exit, at))
    }

    var recorded: [(assetId: String, exit: RediffDayZeroExit, at: Double)] {
        lock.lock(); defer { lock.unlock() }
        return storage
    }
}

/// Captures the byte spends folded into the rolling window.
final class BudgetSpendSpy: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [(bytes: Int, at: Double)] = []

    func record(_ bytes: Int, _ at: Double) {
        lock.lock(); defer { lock.unlock() }
        storage.append((bytes, at))
    }

    var spends: [(bytes: Int, at: Double)] {
        lock.lock(); defer { lock.unlock() }
        return storage
    }
}

@Suite("Day-0 trigger: transport + budget are recorded, never silent (playhead-4dqe)")
struct DayZeroTriggerTransportBudgetTests {

    static let enclosure = URL(string: "https://cdn.example.com/current.mp3")!
    static let played = URL(fileURLWithPath: "/tmp/played-a.mp3")
    static let now: Double = 100 * 86_400

    private func makeTrigger(
        transport: DayZeroTransportSnapshot,
        isCharging: Bool = true,
        budgetWindow: RediffDayZeroBudgetWindow = .empty,
        fetcher: KWaySpyFullFetcher,
        minter: SpyDayZeroMinter,
        suppressions: SuppressionSpy,
        spends: BudgetSpendSpy
    ) -> DayZeroRediffTrigger {
        let service = RediffRefetchService(
            enabled: true,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: fetcher,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: SpyRefetchRecorder(),
            fileRemover: SpyTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            dayZeroMinter: minter,
            now: { Self.now }
        )
        return DayZeroRediffTrigger(
            service: service,
            enabled: true,
            kWayFetchCount: RediffActivation.dayZeroKWayFetchCount,
            transportProvider: { transport },
            chargeStateProvider: { isCharging },
            deepScanOptInProvider: { false },
            attemptContextProvider: { _ in .never },
            suppressionRecorder: { assetId, exit, at in
                suppressions.record(assetId, exit, at)
            },
            mintedMarkDelivery: { _ in },
            budgetWindowProvider: { budgetWindow },
            budgetSpendRecorder: { bytes, at in spends.record(bytes, at) },
            // playhead-3oyz: the retry lane is owned by DayZeroSameSessionRetryTests — opt out.
            retryClaimRecorder: { _ in }
        )
    }

    @Test("A CELLULAR REFUSAL IS RECORDED — before playhead-4dqe the gate returned an empty summary and wrote NOTHING")
    func cellularRefusalIsRecorded() async {
        let fetcher = KWaySpyFullFetcher()
        let suppressions = SuppressionSpy()
        let spends = BudgetSpendSpy()
        let trigger = makeTrigger(
            transport: DayZeroTransportSnapshot(
                reachability: .cellular, isLowDataMode: false, allowsCellular: false
            ),
            fetcher: fetcher, minter: SpyDayZeroMinter(),
            suppressions: suppressions, spends: spends
        )
        _ = await trigger.triggerIfEligible(
            analysisAssetId: "asset-cell", enclosureURL: Self.enclosure,
            playedFileURL: Self.played, at: Self.now
        )
        #expect(fetcher.calls.isEmpty, "no bytes on a refusal")
        #expect(suppressions.recorded.count == 1)
        #expect(suppressions.recorded.first?.exit == .deniedCellularNotAllowed)
        #expect(spends.spends.isEmpty, "a refusal spends no budget")
    }

    @Test("LOW DATA MODE ON WIFI is recorded under its OWN exit — not folded into the cellular refusal")
    func lowDataModeRefusalIsItsOwnExit() async {
        let fetcher = KWaySpyFullFetcher()
        let suppressions = SuppressionSpy()
        let trigger = makeTrigger(
            transport: DayZeroTransportSnapshot(
                reachability: .wifi, isLowDataMode: true, allowsCellular: true
            ),
            fetcher: fetcher, minter: SpyDayZeroMinter(),
            suppressions: suppressions, spends: BudgetSpendSpy()
        )
        _ = await trigger.triggerIfEligible(
            analysisAssetId: "asset-ldm", enclosureURL: Self.enclosure,
            playedFileURL: Self.played, at: Self.now
        )
        #expect(fetcher.calls.isEmpty)
        #expect(suppressions.recorded.first?.exit == .deniedLowDataMode)
    }

    @Test("CELLULAR WITH THE SETTING ON ACTUALLY FETCHES — the setting is not decorative")
    func cellularWithSettingFetches() async {
        let fetcher = KWaySpyFullFetcher()
        let suppressions = SuppressionSpy()
        let trigger = makeTrigger(
            transport: DayZeroTransportSnapshot(
                reachability: .cellular, isLowDataMode: false, allowsCellular: true
            ),
            fetcher: fetcher, minter: SpyDayZeroMinter(),
            suppressions: suppressions, spends: BudgetSpendSpy()
        )
        _ = await trigger.triggerIfEligible(
            analysisAssetId: "asset-cell-ok", enclosureURL: Self.enclosure,
            playedFileURL: Self.played, at: Self.now
        )
        #expect(fetcher.calls.count == RediffActivation.dayZeroKWayFetchCount)
        #expect(suppressions.recorded.isEmpty)
    }

    @Test("AN EXHAUSTED DAILY BUDGET REFUSES AND IS RECORDED — on WiFi, where nothing else would have stopped it")
    func exhaustedBudgetRefusesOnWifi() async {
        let fetcher = KWaySpyFullFetcher()
        let suppressions = SuppressionSpy()
        let spends = BudgetSpendSpy()
        let trigger = makeTrigger(
            transport: DayZeroTransportSnapshot(
                reachability: .wifi, isLowDataMode: false, allowsCellular: false
            ),
            budgetWindow: RediffDayZeroBudgetWindow(
                startedAt: Self.now - 60,
                spentBytes: RediffDayZeroDailyBudget.dailyCapBytes
            ),
            fetcher: fetcher, minter: SpyDayZeroMinter(),
            suppressions: suppressions, spends: spends
        )
        _ = await trigger.triggerIfEligible(
            analysisAssetId: "asset-broke", enclosureURL: Self.enclosure,
            playedFileURL: Self.played, at: Self.now
        )
        #expect(fetcher.calls.isEmpty, "the budget binds a WiFi user too")
        #expect(suppressions.recorded.first?.exit == .deniedDailyBudget)
        #expect(spends.spends.isEmpty)
    }

    @Test("an attempt that RAN folds the bytes it ACTUALLY spent into the window — not the pre-flight estimate")
    func spendsAreRecordedFromTheRealCost() async {
        let fetcher = KWaySpyFullFetcher()
        let spends = BudgetSpendSpy()
        let trigger = makeTrigger(
            transport: DayZeroTransportSnapshot(
                reachability: .wifi, isLowDataMode: false, allowsCellular: false
            ),
            fetcher: fetcher, minter: SpyDayZeroMinter(),
            suppressions: SuppressionSpy(), spends: spends
        )
        let summary = await trigger.triggerIfEligible(
            analysisAssetId: "asset-spend", enclosureURL: Self.enclosure,
            playedFileURL: Self.played, at: Self.now
        )
        #expect(spends.spends.count == 1)
        #expect(spends.spends.first?.bytes == summary.fullFetchBytes)
        #expect(spends.spends.first?.bytes == 2 * 54_000_000,
                "the spy fetcher's real byte count, not `estimatedBytesPerBCopy`")
        #expect(spends.spends.first?.at == Self.now)
    }

    @Test("the budget is consulted AFTER the transport gate — a refused transport costs no budget read")
    func budgetIsNotConsultedOnARefusedTransport() async {
        let reads = DayZeroProbeCounter()
        let service = RediffRefetchService(
            enabled: true,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: KWaySpyFullFetcher(),
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: SpyRefetchRecorder(),
            fileRemover: SpyTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            dayZeroMinter: SpyDayZeroMinter(),
            now: { Self.now }
        )
        let trigger = DayZeroRediffTrigger(
            service: service,
            enabled: true,
            kWayFetchCount: RediffActivation.dayZeroKWayFetchCount,
            transportProvider: {
                DayZeroTransportSnapshot(
                    reachability: .cellular, isLowDataMode: false, allowsCellular: false
                )
            },
            chargeStateProvider: { true },
            deepScanOptInProvider: { false },
            attemptContextProvider: { _ in .never },
            suppressionRecorder: { _, _, _ in },
            mintedMarkDelivery: { _ in },
            budgetWindowProvider: {
                await reads.increment()
                return .empty
            },
            budgetSpendRecorder: { _, _ in },
            // playhead-3oyz: the retry lane is owned by DayZeroSameSessionRetryTests — opt out.
            retryClaimRecorder: { _ in }
        )
        _ = await trigger.triggerIfEligible(
            analysisAssetId: "asset-x", enclosureURL: Self.enclosure,
            playedFileURL: Self.played, at: Self.now
        )
        #expect(await reads.current() == 0)
    }
}


// MARK: - playhead-jra6: reading back what was owed

@Suite("rediff_day_zero_kickoffs — the owed set is readable (playhead-jra6)")
struct DayZeroKickoffOwedSetTests {

    private static let url = URL(string: "https://cdn.example.com/ep.mp3")!

    @Test("THE ACCEPTANCE: a claimed-never-settled kickoff comes back as a resumable candidate")
    func owedKickoffIsReadable() async throws {
        let store = try await makeTestStore()
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-owed", source: .backgroundDownload,
            enclosureURL: Self.url, publishedAt: 42, at: 100
        )
        let owed = try await store.fetchUnsettledRediffDayZeroKickoffs(limit: 10, giveUpAfter: 3)
        #expect(owed.count == 1)
        #expect(owed.first?.episodeId == "ep-owed")
        #expect(owed.first?.enclosureURL == Self.url)
        #expect(owed.first?.publishedAt == 42)
        #expect(owed.first?.owed == 1)
        #expect(owed.first?.source == .backgroundDownload)
    }

    @Test("a SETTLED kickoff is not owed — the surplus is what the predicate reads")
    func settledKickoffIsNotOwed() async throws {
        let store = try await makeTestStore()
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-done", source: .backgroundDownload,
            enclosureURL: Self.url, publishedAt: nil, at: 100
        )
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-done", source: .backgroundDownload,
            outcome: .fired, pollCount: 1, waitedSeconds: 1, at: 101
        )
        #expect(try await store.fetchUnsettledRediffDayZeroKickoffs(limit: 10, giveUpAfter: 3).isEmpty)
    }

    /// The reason the predicate is the SURPLUS and not `lastOutcome`. This row's
    /// last word is `fired`, and it still owes one — reading the outcome column
    /// would skip exactly the re-download case the sweep exists to rescue.
    @Test("a row whose LAST outcome is `fired` can still be owed one, and is")
    func firedRowCanStillBeOwed() async throws {
        let store = try await makeTestStore()
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-two", source: .backgroundDownload,
            enclosureURL: Self.url, publishedAt: nil, at: 100
        )
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-two", source: .backgroundDownload,
            outcome: .fired, pollCount: 1, waitedSeconds: 1, at: 101
        )
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-two", source: .backgroundDownload,
            enclosureURL: Self.url, publishedAt: nil, at: 102
        )
        let owed = try await store.fetchUnsettledRediffDayZeroKickoffs(limit: 10, giveUpAfter: 3)
        #expect(owed.count == 1, "the surplus is 1 even though lastOutcome reads fired")
        #expect(owed.first?.owed == 1)
    }

    @Test("a PRE-V67 row with no URL is skipped rather than re-driven against a guess")
    func rowWithoutURLIsSkipped() async throws {
        let store = try await makeTestStore()
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-legacy", source: .backgroundDownload,
            enclosureURL: nil, publishedAt: nil, at: 100
        )
        #expect(try await store.fetchUnsettledRediffDayZeroKickoffs(limit: 10, giveUpAfter: 3).isEmpty)
    }

    @Test("a later claim that resolves NO url cannot erase one an earlier claim resolved")
    func nullDoesNotOverwriteAGoodURL() async throws {
        let store = try await makeTestStore()
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-keep", source: .backgroundDownload,
            enclosureURL: Self.url, publishedAt: 7, at: 100
        )
        try await store.noteRediffDayZeroKickoffClaim(
            episodeId: "ep-keep", source: .backgroundDownload,
            enclosureURL: nil, publishedAt: nil, at: 101
        )
        let owed = try await store.fetchUnsettledRediffDayZeroKickoffs(limit: 10, giveUpAfter: 3)
        #expect(owed.first?.enclosureURL == Self.url, "COALESCE, not overwrite")
        #expect(owed.first?.publishedAt == 7)
    }

    @Test("the retry budget bites: an episode past giveUpAfter stops being resumed")
    func retryBudgetBites() async throws {
        let store = try await makeTestStore()
        for tick in 0..<4 {
            try await store.noteRediffDayZeroKickoffClaim(
                episodeId: "ep-broken", source: .backgroundDownload,
                enclosureURL: Self.url, publishedAt: nil, at: 100 + Double(tick)
            )
        }
        #expect(try await store.fetchUnsettledRediffDayZeroKickoffs(limit: 10, giveUpAfter: 3).isEmpty,
                "4 owed is past a budget of 3 — a permanently broken episode stops being re-driven")
        #expect(try await store.fetchUnsettledRediffDayZeroKickoffs(limit: 10, giveUpAfter: 4).count == 1)
    }

    @Test("the limit caps one launch's sweep")
    func limitCapsTheSweep() async throws {
        let store = try await makeTestStore()
        for index in 0..<5 {
            try await store.noteRediffDayZeroKickoffClaim(
                episodeId: "ep-\(index)", source: .backgroundDownload,
                enclosureURL: Self.url, publishedAt: nil, at: 100 + Double(index)
            )
        }
        #expect(try await store.fetchUnsettledRediffDayZeroKickoffs(limit: 2, giveUpAfter: 3).count == 2)
        #expect(try await store.fetchUnsettledRediffDayZeroKickoffs(limit: 0, giveUpAfter: 3).isEmpty)
    }
}
