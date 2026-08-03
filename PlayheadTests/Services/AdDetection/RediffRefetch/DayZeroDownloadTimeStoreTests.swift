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
        try await store.noteRediffDayZeroKickoff(
            episodeId: "ep-mix", source: .backgroundDownload,
            outcome: .noAnalysisAsset, pollCount: 40, waitedSeconds: 390, at: 100
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
            budgetSpendRecorder: { bytes, at in spends.record(bytes, at) }
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
            budgetSpendRecorder: { _, _ in }
        )
        _ = await trigger.triggerIfEligible(
            analysisAssetId: "asset-x", enclosureURL: Self.enclosure,
            playedFileURL: Self.played, at: Self.now
        )
        #expect(await reads.current() == 0)
    }
}
