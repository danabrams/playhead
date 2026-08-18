// RediffDayZeroByteDiagnosticsV48MigrationTests.swift
// playhead-3zxd: pin the V48 migration that adds the day-0 byte-diff
// instrumentation columns to `rediff_day_zero_attempts` — the six columns a
// device pull reads to answer "did the phantom fire, and did the fix prevent
// it?" on REAL audio.
//
// WHY THIS FILE EXISTS SEPARATELY from the writer's own tests: a store built by
// `createTables()` has the columns unconditionally, so every writer assertion
// would still pass with `migrateRediffDayZeroByteDiagnosticsV48IfNeeded` deleted
// outright. A device upgrading from v47 is a database that predates the columns,
// and that upgrade is a different claim needing its own evidence.
//
// Coverage targets, matching the V43/V46 sibling rungs:
//   1. Fresh-DB `migrate()` reaches head with all six columns, head pinned to
//      the LITERAL 48.
//   2. A v47-shaped table upgrades in place: columns added, an existing row
//      survives with every other field intact — including `retryClaimCount`
//      (SELECT index 17) and `lastRetryClaimAt` (18), the two reads immediately
//      ahead of the appended six — and the new columns arrive at their
//      DOCUMENTED defaults, which is the truth for every pre-v48 row.
//   3. The migration is idempotent.
//   4. The ISOLATED ladder reaches v48 and adds the columns.
//   5. The rung refuses to step over a rolled-back V39.

import Foundation
import Testing

@testable import Playhead

@Suite("rediff_day_zero_attempts byte-diagnostics V48 migration (playhead-3zxd)")
struct RediffDayZeroByteDiagnosticsV48MigrationTests {

    private static let table = "rediff_day_zero_attempts"
    private static let columns = [
        "lastRunsFound",
        "lastRunsAOverlapping",
        "lastOverlapSecondsRecovered",
        "lastAlignedSecondsInSlots",
        "lastMaxAlignedSecondsInSlot",
        "lastAlignedRunSpans"
    ]

    private func freshTempDir() throws -> URL {
        try makeTempDir(prefix: "RediffDayZeroByteDiagnosticsV48")
    }

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

    /// Rewind an at-head database to the v47 shape: the six columns genuinely
    /// GONE, not merely the version stamped back. A rewind that only touches
    /// `_meta` proves nothing — the rung would run against a table that already
    /// has everything it adds.
    private func rewindToV47(_ store: AnalysisStore) async throws {
        for column in Self.columns {
            try await store.execForTesting(
                "ALTER TABLE rediff_day_zero_attempts DROP COLUMN \(column);")
        }
        // Pinned to the LITERAL 47 — "pre-3zxd" is a fixed historical fact.
        // Written as `currentSchemaVersion - 1` it would stop meaning that the
        // moment head moved past 48: the rewind would land ON 48, the
        // `observed < 48` guard would decline, and the test would assert the
        // absence of columns it had itself prevented from being added.
        try await store.setMetaValue(forKey: "schema_version", value: "47")
    }

    private func columnsPresent(in dir: URL) throws -> Bool {
        try Self.columns.allSatisfy {
            try probeColumnExists(in: dir, table: Self.table, column: $0)
        }
    }

    private func columnsAbsent(in dir: URL) throws -> Bool {
        try Self.columns.allSatisfy {
            !(try probeColumnExists(in: dir, table: Self.table, column: $0))
        }
    }

    // MARK: - Migration ladder

    @Test("fresh DB migrate() lands all six byte-diagnostics columns at head")
    func freshDbHasV48Columns() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        // Drift guard, pinned to the LITERAL head, so whoever bumps the schema
        // next has to come here and read this rung.
        //
        // 48 → 49 (playhead-mn5e/2qz6). This suite is the rung DIRECTLY below
        // the new one, so it is the one that would notice first if V49 had been
        // spliced in ahead of V48 rather than after it: the seeded-v47 test
        // below climbs 47 → 48 → 49, and V49's `guard observed >= 48` means it
        // can only land once V48 has set the version. The six byte-diagnostics
        // columns live on `rediff_day_zero_attempts`, which V49 never names.
        // …and then 49 → 50 (playhead-e6d3), which carries the same
        // `guard observed >= 49` shape and so can only land once V49 has set the
        // version. The seeded-v47 test below now climbs 47 → 48 → 49 → 50. V50
        // names only `backfill_jobs`.
        // 50 → 51 read for this rung (playhead-wogi): V51 lowers
        // `backfill_jobs.progressCursor` to the prefix each asset's own
        // `semantic_scan_results` passA rows support, and touches no other
        // column and no other table. Nothing this rung asserts is named by it.
        #expect(AnalysisStore.currentSchemaVersion == 56)
        #expect(try columnsPresent(in: dir))
    }

    @Test("v47-shaped rediff_day_zero_attempts upgrades in place: columns added, the pre-3zxd row survives at the documented defaults")
    func seededV47RowUpgradesWithoutDataLoss() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let bootstrap = try AnalysisStore(directory: dir)
        try await bootstrap.migrate()
        try await bootstrap.insertAsset(makeAsset(id: "asset-v47"))

        // Non-default values in every field a v47 binary could write, so
        // "survives" is a real claim rather than "all zeroes stayed zero".
        // `retryClaimCount` / `lastRetryClaimAt` are the load-bearing pair: they
        // sit at SELECT indices 17 / 18, immediately ahead of the six appended
        // columns at 19–24, so they come back wrong if the append shifted the
        // positional reader.
        //
        // The diagnostics block is seeded non-zero DELIBERATELY: the rewind
        // DROPS those columns, destroying these values, which is what makes the
        // post-migration read of 0 / nil evidence that the DEFAULTs supplied
        // them rather than that a seeded value merely survived.
        try await bootstrap.upsertRediffDayZeroAttempt(
            RediffDayZeroAttemptRecord(
                analysisAssetId: "asset-v47",
                attemptCount: 2,
                lastAttemptAt: 1_786_000_000,
                lastExit: .marked,
                lastMarkCount: 3,
                lastBSideCount: 4,
                lastBSidesAccepted: 5,
                lastBSidesGateRejected: 6,
                lastBSidesUnreadable: 7,
                lastDivergentSlotCount: 8,
                lastFullFetchBytes: 108_000_000,
                totalFullFetchBytes: 216_000_000,
                suppressedCount: 0,
                lastSuppressedAt: nil,
                lastDetail: "seeded",
                policyGeneration: 9,
                rescueAttemptCount: 1,
                retryClaimCount: 77,
                lastRetryClaimAt: 1_786_000_030,
                byteDiagnostics: RediffByteMintDiagnostics(
                    runsFound: 11,
                    runsAOverlapping: 2,
                    overlapSecondsRecovered: 470.5,
                    alignedSecondsInSlots: 3.25,
                    maxAlignedSecondsInSlot: 2.75,
                    alignedRunSpans: "v1;0:0.00-93.21"
                )
            )
        )

        try await rewindToV47(bootstrap)
        #expect(try columnsAbsent(in: dir), "the fixture must genuinely predate the columns")

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try columnsPresent(in: dir))

        let row = try #require(try await store.fetchRediffDayZeroAttempt(assetId: "asset-v47"))
        #expect(row.attemptCount == 2)
        #expect(row.lastExit == .marked)
        #expect(row.lastMarkCount == 3)
        #expect(row.lastBSidesAccepted == 5)
        #expect(row.lastDivergentSlotCount == 8)
        #expect(row.totalFullFetchBytes == 216_000_000)
        #expect(row.policyGeneration == 9)
        #expect(row.rescueAttemptCount == 1)
        #expect(row.retryClaimCount == 77,
                "retryClaimCount (idx 17) must survive the appended columns — no positional shift")
        #expect(row.lastRetryClaimAt == 1_786_000_030,
                "lastRetryClaimAt (idx 18) must survive the appended columns — no positional shift")

        // ADDITIVE, NO BACKFILL. A pre-3zxd row had no instrumented diff, and
        // `runsFound == 0` is exactly the VACUITY signal that says so — which is
        // why the zeros are the honest value and not a silent "clean" reading.
        #expect(row.byteDiagnostics.runsFound == 0,
                "0 is the truth for every pre-v48 row, and it reads as VACUOUS, not as clean")
        #expect(row.byteDiagnostics.runsAOverlapping == 0)
        #expect(row.byteDiagnostics.overlapSecondsRecovered == 0)
        #expect(row.byteDiagnostics.alignedSecondsInSlots == 0)
        #expect(row.byteDiagnostics.maxAlignedSecondsInSlot == 0)
        #expect(row.byteDiagnostics.alignedRunSpans == nil,
                "nullable with no backfill — an invented span list would assert a diff nobody ran")

        // …and the upgraded row is immediately WRITABLE, so the migration
        // produced a working column and not merely a present one. This is also
        // THE JOB-3 ROUND TRIP: exactly what a device pull reads back.
        try await store.upsertRediffDayZeroAttempt(
            RediffDayZeroAttemptRecord(
                analysisAssetId: "asset-v47",
                attemptCount: 3,
                lastAttemptAt: 1_786_000_100,
                lastExit: .marked,
                byteDiagnostics: RediffByteMintDiagnostics(
                    runsFound: 6,
                    runsAOverlapping: 1,
                    overlapSecondsRecovered: 469.99,
                    alignedSecondsInSlots: 0,
                    maxAlignedSecondsInSlot: 0,
                    alignedRunSpans: "v1;0:0.00-900.01,900.01-1370.00;1:0.00-1370.00"
                )
            )
        )
        let written = try #require(try await store.fetchRediffDayZeroAttempt(assetId: "asset-v47"))
        #expect(written.byteDiagnostics.runsFound == 6)
        #expect(written.byteDiagnostics.runsAOverlapping == 1,
                "the OPPORTUNITY counter: a pre-3zxd build could have emitted a phantom here")
        #expect(abs(written.byteDiagnostics.overlapSecondsRecovered - 469.99) < 1e-9,
                "the AVERTED DAMAGE, in A-seconds of matched audio")
        #expect(written.byteDiagnostics.alignedSecondsInSlots == 0,
                "THE INVARIANT: no emitted slot contains audio the aligner proved matched")
        #expect(written.byteDiagnostics.maxAlignedSecondsInSlot == 0)
        #expect(written.byteDiagnostics.alignedRunSpans
                == "v1;0:0.00-900.01,900.01-1370.00;1:0.00-1370.00")
    }

    @Test("V48 migration is idempotent across resetMigratedPathsForTesting")
    func v48MigrationIsIdempotent() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "asset-idem"))
        try await store.upsertRediffDayZeroAttempt(
            RediffDayZeroAttemptRecord(
                analysisAssetId: "asset-idem",
                attemptCount: 1,
                lastAttemptAt: 500,
                lastExit: .marked,
                byteDiagnostics: RediffByteMintDiagnostics(runsFound: 4, runsAOverlapping: 1)
            )
        )
        let v1 = try await store.schemaVersion()

        AnalysisStore.resetMigratedPathsForTesting()
        try await store.migrate()
        let v2 = try await store.schemaVersion()

        #expect(v1 == AnalysisStore.currentSchemaVersion)
        #expect(v2 == AnalysisStore.currentSchemaVersion)
        #expect(try columnsPresent(in: dir))
        let row = try #require(try await store.fetchRediffDayZeroAttempt(assetId: "asset-idem"))
        #expect(row.byteDiagnostics.runsFound == 4, "a re-run must not clear the recorded diagnostics")
        #expect(row.byteDiagnostics.runsAOverlapping == 1)
    }

    @Test("isolated ladder (migrateOnlyForTesting) reaches v48 and adds the columns")
    func isolatedLadderReachesV48() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        try await rewindToV47(store)
        #expect(try columnsAbsent(in: dir))

        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try columnsPresent(in: dir),
                "the RUNG, not createTables(), is what adds the columns on an upgrade")
    }

    /// A v39 rollback leaves `_meta` at 38 so the next launch retries it. Every
    /// rung added after V39 has to refuse to step over that, and the ladder does
    /// not enforce it structurally — so this rung's `guard observed >= 47` needs
    /// its own witness, exactly as V40–V47 have.
    ///
    /// A rewound `_meta` alone does NOT reproduce the condition: on a healthy DB
    /// V39 simply succeeds and the ladder climbs back to head. The fixture has to
    /// make V39 genuinely ROLL BACK — the duplicate asset pair plus the ABORT
    /// trigger, the shape first paid for in `MergedChildRowDedupeV40MigrationTests`.
    @Test("playhead-3zxd: v48 does not step over a rolled-back v39")
    func v48DoesNotStepOverARolledBackV39() async throws {
        let dir = try freshTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        for column in Self.columns {
            try await store.execForTesting(
                "ALTER TABLE rediff_day_zero_attempts DROP COLUMN \(column);")
        }

        try await store.execForTesting("""
            DROP INDEX IF EXISTS idx_assets_episode_fingerprint;
            DROP INDEX IF EXISTS idx_chunks_asset_pass_fingerprint;
            INSERT INTO analysis_assets
              (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
              VALUES ('dupe-old', 'ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 1.0);
            INSERT INTO analysis_assets
              (id, episodeId, assetFingerprint, sourceURL, analysisState, createdAt)
              VALUES ('dupe-new', 'ep-collide', 'ffee', 'file:///tmp/x.mp3', 'pending', 2.0);
            CREATE TRIGGER zxd_v39_guard BEFORE DELETE ON analysis_assets
              BEGIN SELECT RAISE(ABORT, 'v48 step-over fixture'); END;
            UPDATE _meta SET value = '38' WHERE key = 'schema_version';
            """)

        try await store.migrateOnlyForTesting()

        #expect(try await store.schemaVersion() == 38,
                "a DB held at 38 by a rolled-back v39 must not be stamped 48")
        #expect(try columnsAbsent(in: dir),
                "…and the V48 rung must not have run against a database it had no business touching")
    }
}
