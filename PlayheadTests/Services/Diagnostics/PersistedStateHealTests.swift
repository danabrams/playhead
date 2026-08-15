// PersistedStateHealTests.swift
// playhead-gyhw — the HEALING half of playhead-dgly's reporter/healer split.
//
// ----- What this bead decided, and the measurement that decided it -----
//
// All five invariants were taken one at a time and NO NEW REPAIR was licensed.
// The reasons are in the code, on `PersistedStateInvariant.healLicence`, so they
// survive the commit message; this file is the evidence for the two that turn on
// a number rather than on an argument.
//
// THE NUMBER THAT WOULD HAVE LICENSED A REPAIR WAS WRONG, and it is the standing
// defect class: a value that names one thing read as though it named another.
// The reading in circulation for `retry_budget_spent_with_work_remaining` on a
// device running merged main was **3 of 8** — three dead rows with reachable
// work, which is exactly the population a healer would sweep. That figure is the
// reading after **V51 alone**. The ladder runs **V50 first**, and V50 restores
// the budget of every row carrying `underCoverageBudgetSpent-%` — seven of the
// nine on `db-pull10`, including two of the three the 3/8 reading names. The
// honest post-ladder reading is **1 of 1**: A9F6DF05, and it alone.
//
// Both numbers are measured here rather than argued:
//   * `v51OnlyReadingIsThreeOfEight` reproduces where 3/8 came from, so the
//     correction cannot be mistaken for a disagreement about arithmetic.
//   * `ladderReadingIsOneOfOne` drives the REAL rungs over a store seeded from
//     the pull and reads the REAL reporter.
//
// A healer built on 3/8 would have swept in AA6CD430 — whose examined scan rows
// already support a prefix equal to its whole transcript reach, so there is
// nothing above its (absent) cursor at all — and 3C2FFE10, which V50 had already
// revived. That is `playhead-hii7`'s finding arriving from the other side: the
// invariant's `remaining` is "what a resume would PLAN", not "what no scan has
// READ", and a repair keyed on it is keyed on a consequence.
//
// ----- What DID ship -----
//
// The two repairs that already exist (V50, V51) now RECORD what they did. Both
// destroy the value they replace and both announced themselves only through
// `Logger.notice`, which no device pull collects — so a phone could arrive with
// seven retry budgets restored and a cursor withdrawn by 7,339.26 s and nothing
// on it able to say so. `PersistedStateRepairRecord` carries WHAT changed, on
// WHICH row, FROM what TO what, and WHICH invariant licensed it, onto the
// channel `playhead-dgly` already established.

import Foundation
import Testing
import XCTest

@testable import Playhead

// MARK: - The licence

@Suite("Every invariant states why it is or is not repaired (playhead-gyhw)")
struct PersistedStateHealLicenceTests {

    @Test("No invariant is repaired at launch — the whole enumeration refused")
    func nothingIsHealedAtLaunch() {
        for invariant in PersistedStateInvariant.allCases {
            #expect(
                !invariant.healLicence.repairsAtLaunch,
                """
                \(invariant.rawValue) claims a launch-time repair. playhead-gyhw licensed \
                NONE — if that changed, the acceptance is: the forward fix is merged, the \
                predicate is keyed on the CAUSE whose rule changed, the repair is \
                idempotent, and the row records why it was repaired.
                """
            )
        }
    }

    @Test("The two invariants whose rules DID change name the rung that repaired them")
    func shippedRepairsNameTheirMigration() throws {
        // These are the only two shapes in the set whose retirement rule
        // changed, and in both the repair shipped WITH the forward fix rather
        // than instead of it — playhead-e6d3's template.
        guard case let .shippedWithTheForwardFix(bead, migration, residue) =
            PersistedStateInvariant.coarseCursorBeyondScannedPrefix.healLicence else {
            Issue.record("wogi's licence is no longer a shipped-with-the-forward-fix claim")
            return
        }
        #expect(bead == "playhead-wogi")
        #expect(migration == "v51")
        #expect(!residue.isEmpty)

        guard case let .shippedWithTheForwardFix(e6d3Bead, e6d3Migration, e6d3Residue) =
            PersistedStateInvariant.retryBudgetSpentWithWorkRemaining.healLicence else {
            Issue.record("e6d3's licence is no longer a shipped-with-the-forward-fix claim")
            return
        }
        #expect(e6d3Bead == "playhead-e6d3")
        #expect(e6d3Migration == "v50")
        // The residue is the whole reason no FURTHER repair is licensed, so it
        // must name the row it leaves behind rather than claim a clean sweep.
        #expect(e6d3Residue.contains("A9F6DF05"))
    }

    @Test("A refusal blocked on an OPEN bead names it, so the block is followable")
    func blockedRefusalsNameTheirBead() {
        #expect(
            PersistedStateInvariant.strandedRunningBackfillJob.healLicence.blockingBead
                == "playhead-1e86")
        // The other refusals are not blocked on anything — 1216's population is
        // empty and exy0's reading is not a defect — so a bead name there would
        // be an invitation to wait for something that is not coming.
        #expect(
            PersistedStateInvariant.newAssetWithAudioAndFailedJob.healLicence.blockingBead == nil)
        #expect(
            PersistedStateInvariant.eligibleAutoWindowNeverOffered.healLicence.blockingBead == nil)
    }

    @Test("exy0's reading is recorded as NOT a defect — a repair there would fabricate a skip")
    func exy0IsRefutedRatherThanUnhealed() {
        guard case let .readingIsNotADefect(reason) =
            PersistedStateInvariant.eligibleAutoWindowNeverOffered.healLicence else {
            Issue.record(
                """
                exy0's licence stopped saying its reading is not a defect. It was REFUTED by \
                measurement: driven through `beginEpisode` those rows reach `.applied`. \
                Repairing a `candidate` row would record a delivery the listener never got.
                """)
            return
        }
        #expect(reason.contains("exy0"))
    }
}

// MARK: - The two readings

@Suite("The post-migration reading of invariant #3 (playhead-gyhw)")
struct PersistedStateLadderReadingTests {

    /// The db-pull10 snapshot with 3C2FFE10's cursor lowered to the prefix its
    /// own examined rows support — V51's effect, and ONLY V51's.
    private func afterV51Only() -> PersistedStateSnapshot {
        let base = DevicePullFixture.snapshot()
        let jobs = base.backfillJobs.map { job -> PersistedStateSnapshot.BackfillJobRow in
            guard job.jobId == "fm-c42dc1a029b38e37" else { return job }
            return PersistedStateSnapshot.BackfillJobRow(
                jobId: job.jobId,
                assetId: job.assetId,
                status: job.status,
                retryCount: job.retryCount,
                deferReason: job.deferReason,
                updatedAt: job.updatedAt,
                claimedUpperBoundSec: 659.46
            )
        }
        return PersistedStateSnapshot(
            backfillJobs: jobs,
            assets: base.assets,
            eligibilityGatedAdWindows: base.eligibilityGatedAdWindows,
            coverageLaneRetryCap: base.coverageLaneRetryCap
        )
    }

    @Test("WHERE 3/8 CAME FROM: V51 applied, V50 not — and it names three rows")
    func v51OnlyReadingIsThreeOfEight() throws {
        let findings = PersistedStateInvariantEvaluator.evaluate(afterV51Only())

        let cursor = try #require(findings.finding(.coarseCursorBeyondScannedPrefix))
        #expect(cursor.violations == 0)
        #expect(cursor.population == 8)

        let retry = try #require(findings.finding(.retryBudgetSpentWithWorkRemaining))
        #expect(retry.violations == 3)
        #expect(retry.population == 8)
        let named = retry.witnesses.joined(separator: " || ")
        // A9F6DF05 — 59c8's row. AA6CD430 — no cursor at all. 3C2FFE10 — newly
        // visible because V51 lowered its cursor by 7,339.26 s.
        #expect(named.contains("fm-9330e821aeb36a0d"))
        #expect(named.contains("fm-b93dd4f616ecfba8"))
        #expect(named.contains("fm-c42dc1a029b38e37"))
    }
}

// MARK: - The real ladder

@Suite("V50 and V51 over a db-pull10-shaped store (playhead-gyhw)", .serialized)
struct PersistedStateLadderRepairTests {

    /// (assetId, jobId, retryCount, cursor, deferReason) — `db-pull10`'s nine
    /// coverage-lane rows, reproduced from `DevicePullFixture` so the two
    /// cannot drift.
    private static let jobs = DevicePullFixture.jobs

    private func makeAsset(id: String, reach: Double) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 8_000,
            finalPassCoverageEndTime: reach
        )
    }

    private func makeScan(assetId: String, index: Int, start: Double, end: Double)
        -> SemanticScanResult {
        SemanticScanResult(
            id: "\(assetId)-scan-\(index)",
            analysisAssetId: assetId,
            windowFirstAtomOrdinal: index * 10,
            windowLastAtomOrdinal: index * 10 + 9,
            windowStartTime: start,
            windowEndTime: end,
            scanPass: SemanticScanResult.presenceScanPass,
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
            scanCohortJSON: makeTestScanCohortJSON(),
            transcriptVersion: "tx-v1",
            reuseScope: "\(assetId)-passA-\(index)"
        )
    }

    /// A store carrying the pull's nine rows, rewound to v49 so the real V50 and
    /// V51 rungs run against it.
    ///
    /// The rewind is a `schema_version` write only, which is sound HERE and is
    /// not in general: neither rung performs DDL, so there is no column to
    /// remove and nothing for the rewind to fake. A rung that added a column
    /// would need the column genuinely dropped — see
    /// `RediffDayZeroByteDiagnosticsV48MigrationTests.rewindToV47`.
    private func seededStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "gyhw-ladder")
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        for (assetId, _, reach, prefix) in DevicePullFixture.assets {
            try await store.insertAsset(makeAsset(id: assetId, reach: reach))
            if assetId == "3C2FFE10" {
                // The witness's real shape: a contiguous head to 659.46, a
                // 7,279.68 s hole nothing has read, then a 59.58 s tail. It is
                // the hole that makes the cursor a lie.
                try await store.insertSemanticScanResult(
                    makeScan(assetId: assetId, index: 0, start: 0.78, end: 659.46))
                try await store.insertSemanticScanResult(
                    makeScan(assetId: assetId, index: 1, start: 7_939.14, end: 7_998.72))
            } else {
                try await store.insertSemanticScanResult(
                    makeScan(assetId: assetId, index: 0, start: 0, end: prefix))
            }
        }
        for (jobId, assetId, status, retry, cursor, deferReason) in Self.jobs {
            try await store.insertBackfillJob(makeBackfillJob(
                jobId: jobId,
                analysisAssetId: assetId,
                progressCursor: cursor.map {
                    BackfillProgressCursor(
                        processedPhaseCount: 1,
                        lastProcessedUpperBoundSec: EpisodeSeconds($0))
                },
                retryCount: retry,
                deferReason: deferReason,
                status: BackfillJobStatus(rawValue: status) ?? .failed))
        }

        // Drain anything the fresh-store migration itself recorded, so the
        // assertions below measure the REWOUND ladder rather than a sum of two.
        _ = await store.drainPersistedStateRepairRecords()
        try await store.setMetaValue(forKey: "schema_version", value: "49")
        return store
    }

    @Test("PREDICTED BEFORE RUNNING: V50 repairs 7 rows, V51 repairs 1")
    func ladderRepairCountsAreSevenAndOne() async throws {
        let store = try await seededStore()
        try await store.migrateOnlyForTesting()

        let records = await store.drainPersistedStateRepairRecords()
        let v50 = records.filter { $0.migration == "v50" }
        let v51 = records.filter { $0.migration == "v51" }

        // SEVEN: the eight `failed` rows minus A9F6DF05, whose deferReason is
        // the FoundationModels error rather than `underCoverageBudgetSpent-%`.
        // B97B8779 is `complete` and was never at the cap.
        #expect(v50.count == 7)
        #expect(!v50.contains { $0.rowId == "fm-9330e821aeb36a0d" })
        #expect(!v50.contains { $0.rowId == "fm-8064bbfb378e0bed" })
        #expect(v50.allSatisfy { $0.from == "3" && $0.to == "0" })
        #expect(v50.allSatisfy { $0.licensedBy == "playhead-e6d3" })
        #expect(v50.allSatisfy { $0.cause.hasPrefix("underCoverageBudgetSpent-") })

        // ONE: 3C2FFE10 is the only cursor above the prefix its own examined
        // rows support. The five that sit AT their transcript's reach are honest
        // — under-covered because under-transcribed — and are left alone.
        #expect(v51.count == 1)
        let withdrawn = try #require(v51.first)
        #expect(withdrawn.rowId == "fm-c42dc1a029b38e37")
        #expect(withdrawn.from == "7998.72")
        #expect(withdrawn.to == "659.46")
        #expect(withdrawn.licensedBy == "playhead-wogi")
        #expect(withdrawn.invariant == .coarseCursorBeyondScannedPrefix)
    }

    @Test("PREDICTED BEFORE RUNNING: the post-ladder reading is 1 of 1, not 3 of 8")
    func ladderReadingIsOneOfOne() async throws {
        let store = try await seededStore()
        try await store.migrateOnlyForTesting()

        let snapshot = try await store.fetchPersistedStateSnapshot()
        let findings = PersistedStateInvariantEvaluator.evaluate(snapshot)

        let cursor = try #require(findings.finding(.coarseCursorBeyondScannedPrefix))
        #expect(cursor.violations == 0)
        #expect(cursor.population == 8)

        let retry = try #require(findings.finding(.retryBudgetSpentWithWorkRemaining))
        // ONE of ONE. The denominator collapses because V50 took seven rows off
        // the cap — the population of this invariant is `retryCount >= cap`, so
        // a repair does not merely lower the numerator, it removes the row from
        // the question entirely. THAT is why 3/8 and 1/1 are not two readings of
        // one quantity.
        #expect(retry.violations == 1)
        #expect(retry.population == 1)
        let witness = try #require(retry.witnesses.first)
        #expect(witness.contains("fm-9330e821aeb36a0d"))
        // AA6CD430 is NOT here, and it is the row a 3/8-licensed healer would
        // have swept: its cursor is absent, so the invariant reads the whole
        // transcript as remaining, while its own examined rows already cover
        // every second of it.
        #expect(!witness.contains("fm-b93dd4f616ecfba8"))

        let stranded = try #require(findings.finding(.strandedRunningBackfillJob))
        #expect(stranded.violations == 0)
        #expect(stranded.population == 9)
    }

    @Test("The ledger drains ONCE — a repair that happened once is reported once")
    func theLedgerDrainsExactlyOnce() async throws {
        let store = try await seededStore()
        try await store.migrateOnlyForTesting()

        #expect(await store.drainPersistedStateRepairRecords().count == 8)
        #expect(await store.drainPersistedStateRepairRecords().isEmpty)
    }

    @Test("Re-running the ladder on a repaired store records NOTHING — it is idempotent")
    func repairIsIdempotent() async throws {
        let store = try await seededStore()
        try await store.migrateOnlyForTesting()
        _ = await store.drainPersistedStateRepairRecords()

        // The version guard is the first line of defence, so rewind past it and
        // make the rungs actually re-execute against already-repaired rows. A
        // second repair here would mean the predicates key on something the
        // first pass did not remove.
        try await store.setMetaValue(forKey: "schema_version", value: "49")
        try await store.migrateOnlyForTesting()
        #expect(await store.drainPersistedStateRepairRecords().isEmpty)
    }
}

// MARK: - The record a pull sees

@Suite("The repair record reaches the device pull (playhead-gyhw)")
struct PersistedStateRepairRecordEmissionTests {

    private static func lines(_ logger: SurfaceStatusInvariantLogger) throws
        -> [InvariantViolation] {
        logger.flushForTesting()
        guard let url = logger.currentSessionFileURL else { return [] }
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try String(decoding: try Data(contentsOf: url), as: UTF8.self)
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { try decoder.decode(SurfaceStateTransitionEntry.self, from: Data($0.utf8)) }
            .compactMap(\.invariantViolation)
    }

    private static func emptySnapshot() -> PersistedStateSnapshot {
        PersistedStateSnapshot(
            backfillJobs: [], assets: [], eligibilityGatedAdWindows: [], coverageLaneRetryCap: 3)
    }

    private static let sampleRepair = PersistedStateRepairRecord(
        migration: "v51",
        invariant: .coarseCursorBeyondScannedPrefix,
        licensedBy: "playhead-wogi",
        rowId: "fm-c42dc1a029b38e37",
        field: "backfill_jobs.progressCursor.lastProcessedUpperBoundSec",
        from: "7998.72",
        to: "659.46",
        cause: "unsupported_by_examined_passA_rows"
    )

    @Test("A launch that repaired NOTHING still says so — repairs=0 is a claim")
    func zeroRepairsIsStillRecorded() async throws {
        let logger = SurfaceStatusInvariantLogger(
            directory: try makeTempDir(prefix: "gyhw-emit"))
        let snapshot = Self.emptySnapshot()
        await PersistedStateInvariantReporter(
            snapshotProvider: { snapshot },
            audioPresenceProbe: { _ in false },
            logger: logger
        ).report()

        let census = try Self.lines(logger).filter { $0.code == .persistedStateRepairCensus }
        #expect(census.count == 1)
        // `migrations=none`, not an absent value: a key whose value is missing
        // reads as a truncated line rather than as a claim of zero.
        #expect(census.first?.description == "repairs=0 migrations=none")
    }

    @Test("Every repaired row is named with its four answers, and the census counts them")
    func repairsAreNamedAndCounted() async throws {
        let logger = SurfaceStatusInvariantLogger(
            directory: try makeTempDir(prefix: "gyhw-emit"))
        let snapshot = Self.emptySnapshot()
        let budget = PersistedStateRepairRecord(
            migration: "v50",
            invariant: .retryBudgetSpentWithWorkRemaining,
            licensedBy: "playhead-e6d3",
            rowId: "fm-b93dd4f616ecfba8",
            field: "backfill_jobs.retryCount",
            from: "3",
            to: "0",
            cause: "underCoverageBudgetSpent-fullEpisodeScan"
        )
        await PersistedStateInvariantReporter(
            snapshotProvider: { snapshot },
            audioPresenceProbe: { _ in false },
            repairRecordProvider: { [Self.sampleRepair, budget] },
            logger: logger
        ).report()

        let violations = try Self.lines(logger)
        let census = try #require(
            violations.first { $0.code == .persistedStateRepairCensus })
        #expect(census.description == "repairs=2 migrations=v50:1,v51:1")

        let applied = violations.filter { $0.code == .persistedStateRepairApplied }
        #expect(applied.count == 2)
        let cursorLine = try #require(
            applied.first { $0.description.contains("migration=v51") })
        // WHAT changed, on WHICH row, FROM what TO what, and WHICH invariant
        // licensed it — all four, on one line, without reading the code.
        #expect(cursorLine.description == """
            migration=v51 invariant=coarse_cursor_beyond_scanned_prefix \
            licensed_by=playhead-wogi row=fm-c42dc1a029b38e37 \
            field=backfill_jobs.progressCursor.lastProcessedUpperBoundSec \
            from=7998.72 to=659.46 cause=unsupported_by_examined_passA_rows
            """)
    }

    @Test("A snapshot read that THROWS still records the repairs that already happened")
    func repairsSurviveAFailedRead() async throws {
        struct Boom: Error {}
        let logger = SurfaceStatusInvariantLogger(
            directory: try makeTempDir(prefix: "gyhw-emit"))
        await PersistedStateInvariantReporter(
            snapshotProvider: { throw Boom() },
            audioPresenceProbe: { _ in false },
            repairRecordProvider: { [Self.sampleRepair] },
            logger: logger
        ).report()

        let violations = try Self.lines(logger)
        // The repairs are in the PAST by the time the reporter runs — they
        // happened inside `openAtLaunch`. Losing their record because the read
        // that follows them failed would make an already-invisible repair
        // invisible for a second, unrelated reason.
        #expect(violations.contains { $0.code == .persistedStateRepairApplied })
        #expect(violations.contains { $0.code == .persistedStateRepairCensus })
        #expect(violations.contains { $0.code == .persistedStateInvariantReadFailed })
        // …and dgly's own property, re-pinned from this side because this test
        // is the reason its assertion was narrowed: a failed read still emits
        // NO invariant census, so "the check ran and found nothing" and "the
        // check could not run" stay distinguishable.
        #expect(!violations.contains { $0.code == .persistedStateInvariantCensus })
    }

    @Test("A defer reason carrying whitespace or `=` cannot split the line into fields")
    func hostileCauseIsSanitized() {
        let record = PersistedStateRepairRecord(
            migration: "v50",
            invariant: .retryBudgetSpentWithWorkRemaining,
            licensedBy: "playhead-e6d3",
            rowId: "fm-x",
            field: "backfill_jobs.retryCount",
            from: "3",
            to: "0",
            // The real thing: 300 characters of NSError description with
            // embedded `=`, quotes and newlines.
            cause: "Error Domain=FoundationModels.LanguageModelError Code=-1\n\"boom\""
        )
        let fields = record.wireDescription.split(separator: " ")
        #expect(fields.count == 8)
        #expect(fields.allSatisfy { $0.contains("=") })
        #expect(!record.wireDescription.contains("\n"))
    }
}

// MARK: - The does-it-run rail

/// playhead-dgly's DG16 lesson, one bead on: deleting the launch call left all
/// thirty of its unit tests green. A drain nobody invokes is exactly the same
/// failure — the repair census would keep printing `repairs=0` forever, because
/// the default provider returns an empty array and a census of nothing looks
/// identical to a census of a ledger nobody asked for.
final class PersistedStateRepairDrainWiringCanaryTests: XCTestCase {

    private static func executableSource(_ source: String) -> String {
        source
            .split(separator: "\n", omittingEmptySubsequences: false)
            .map { $0.trimmingCharacters(in: .whitespaces).hasPrefix("//") ? "" : String($0) }
            .joined(separator: "\n")
    }

    func testLaunchReporterIsGivenTheRepairLedgerDrain() throws {
        let source = Self.executableSource(
            try SwiftSourceInspector.loadSource(
                repoRelativePath: "Playhead/App/PlayheadRuntime.swift"))

        // Anti-vacuity for the stripper: pin one line that is code and one that
        // is only ever prose, so a stripper that removed everything fails loudly
        // rather than passing on an empty world.
        XCTAssertTrue(
            source.contains("await PersistedStateInvariantReporter("),
            "the comment stripper removed executable source")
        XCTAssertFalse(
            source.contains("// playhead-gyhw: the schema ladder repaired rows inside"),
            "the comment stripper did not remove comments, so this canary reads prose")

        XCTAssertTrue(
            source.contains("repairRecordProvider:"),
            """
            PlayheadRuntime no longer passes a repair-record provider to the persisted-state \
            reporter. The parameter DEFAULTS to an empty array, so removing it does not break \
            the build and does not fail a unit test — it silently returns V50 and V51 to \
            announcing themselves only through OSLog, which no device pull collects, while \
            the census line keeps printing `repairs=0`.
            """)
        XCTAssertTrue(
            source.contains("analysisStore.drainPersistedStateRepairRecords()"),
            """
            the repair provider no longer DRAINS the store's ledger. A provider wired to \
            anything else reports a repair that did not happen, or none that did.
            """)
    }
}
