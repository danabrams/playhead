// PersistedStateInvariantReporterTests.swift
// playhead-dgly — rails for the persisted-state invariant REPORTER.
//
// ----- What these tests have to prove, and why that is the hard part -----
//
// The bead's acceptance is that "a clean device reports zero violations, and
// REPORTING ZERO IS A POSITIVE CLAIM, NOT AN ABSENCE OF CHECKING". A check
// that inspects nothing passes everything: `playhead-wwbr`'s canary read
// `object["testTargets"] as? [[String: Any]] ?? []`, so renaming one key made
// it pass on an empty world for four months, and `playhead-8ljj` found four
// distinct endings all returning the same `0`.
//
// So every invariant here is exercised in BOTH directions — a violating
// fixture that must fire, and the adjacent clean fixture that must not — and
// the census is asserted to be present even when the count is zero.
//
// The device fixture is the 2026-08-14 pull (`db-pull10`), reproduced
// field-for-field from its own rows, on `playhead-exy0`'s precedent: a rail
// built from a hypothetical population is a statement about the author's
// imagination, and a rail built from the device's own numbers is a statement
// about the phone.

import Foundation
import Testing
import XCTest

@testable import Playhead

// MARK: - The launch ORDER, which nothing else can see

/// The reporter's value is entirely in WHERE it runs. Every repair in
/// `PlayheadRuntime`'s bootstrap chain rewrites a population it counts, so a
/// call site that drifts below one of them turns a real finding into a healthy
/// zero — silently, with every unit test still green, because a unit test
/// supplies its own snapshot and knows nothing about the chain.
///
/// XCTest-shaped, matching the repo's other source canaries: a canary that a
/// test plan may need to filter must be filterable, and `skippedTests` only
/// honours XCTest class names.
final class PersistedStateInvariantLaunchOrderCanaryTests: XCTestCase {

    /// Each repair, with the reason its position matters. A renamed entry here
    /// is caught by the not-found assertion below rather than silently
    /// dropping out of the check — that is `playhead-wwbr`'s hole, where a
    /// filter that resolved to nothing passed on an empty world.
    private static let repairsThatMustComeAfter: [(marker: String, why: String)] = [
        ("backgroundTaskRunLedger.reapOrphansAtLaunch(",
         "reaps orphan `.running` ledger rows"),
        ("analysisCoordinator.reconcileDuplicateAnalysisAssetsIfNeeded(",
         "merges duplicate analysis_assets rows"),
        ("analysisCoordinator.runEpisodeDurationBackfillIfNeeded(",
         "rewrites episodeDurationSec"),
        ("analysisCoordinator.reconcilePersistedTerminalStatesIfNeeded(",
         "rewrites analysisState, which invariant 4's population is"),
        ("analysisStore.pruneOrphanedScansForCurrentCohort(",
         "DELETES semantic_scan_results rows, which invariant 2 measures against"),
        ("analysisJobReconciler.reconcile(",
         "clears stranded backfill rows, which IS invariant 1's population")
    ]

    /// `PlayheadRuntime` DOCUMENTS this chain in a comment block 100 kB above
    /// the code that runs it, naming every one of these calls in order. A
    /// naive `range(of:)` finds the comment first and the canary then measures
    /// the prose instead of the program — the standing defect class, in the
    /// rail meant to catch it. Whole-line comments are removed before the
    /// search; trailing comments are left alone because every marker sits on
    /// its own line.
    private static func executableSource(_ source: String) -> String {
        source
            .split(separator: "\n", omittingEmptySubsequences: false)
            .map { $0.trimmingCharacters(in: .whitespaces).hasPrefix("//") ? "" : String($0) }
            .joined(separator: "\n")
    }

    func testReporterRunsBeforeEveryLaunchRepair() throws {
        let source = Self.executableSource(
            try SwiftSourceInspector.loadSource(
                repoRelativePath: "Playhead/App/PlayheadRuntime.swift"))

        // Anti-vacuity for the stripper itself: if it ever removed everything,
        // every marker would be "not found" and the loop below would fail
        // loudly rather than pass — but a stripper that removed only the CODE
        // would leave the comment offsets and pass. Pin one line that is code
        // and one that is only ever a comment.
        XCTAssertTrue(source.contains("let storeOutcome = await analysisStoreRecovery.openAtLaunch"),
                      "the comment stripper removed executable source")
        XCTAssertFalse(source.contains("//   - analysisJobReconciler.reconcile()"),
                       "the comment stripper did not remove the launch-chain doc block, so this "
                       + "canary is measuring the prose instead of the program")

        // The CALL, not the declaration — `Self.` distinguishes them.
        let callRange = try XCTUnwrap(
            source.range(of: "Self.reportPersistedStateInvariantsAtLaunch("),
            "PlayheadRuntime no longer calls the persisted-state invariant reporter at all. "
            + "playhead-dgly's whole deliverable is that call; removing it makes every "
            + "census line in the device pull vanish with no test noticing.")
        let callOffset = source.distance(from: source.startIndex, to: callRange.lowerBound)

        // Anti-vacuity: the reporter must sit AFTER the store is open, or it
        // reads a store that has not migrated.
        let openRange = try XCTUnwrap(
            source.range(of: "analysisStoreRecovery.openAtLaunch("),
            "the launch chain no longer opens the store where this canary expects it")
        XCTAssertLessThan(
            source.distance(from: source.startIndex, to: openRange.lowerBound),
            callOffset,
            "the reporter must run AFTER openAtLaunch — before it the schema ladder has not run")

        for repair in Self.repairsThatMustComeAfter {
            let range = try XCTUnwrap(
                source.range(of: repair.marker),
                "launch repair `\(repair.marker)` was not found in PlayheadRuntime.swift. "
                + "If it was renamed, rename it here too: a marker that matches nothing "
                + "removes one ordering guarantee and reports nothing.")
            let repairOffset = source.distance(from: source.startIndex, to: range.lowerBound)
            XCTAssertLessThan(
                callOffset, repairOffset,
                "the persisted-state invariant reporter runs AFTER `\(repair.marker)`, which "
                + "\(repair.why). Everything it would have reported has already been repaired, "
                + "so the census reads a healthy zero that is a fact about ordering rather "
                + "than about the device. Move the reporter above it.")
        }
    }

    /// The reporter must not be detached: a `Task { }` around it would race the
    /// repairs and make the reading non-deterministic, which is the defect
    /// class the reporter exists to find.
    func testReporterIsAwaitedInline() throws {
        let source = Self.executableSource(
            try SwiftSourceInspector.loadSource(
                repoRelativePath: "Playhead/App/PlayheadRuntime.swift"))
        XCTAssertTrue(
            source.contains("await Self.reportPersistedStateInvariantsAtLaunch("),
            "the reporter must be AWAITED in the bootstrap chain. Detaching it lets the "
            + "five launch repairs run concurrently with the read.")
    }
}

// MARK: - The shared ruler

@Suite("supportedScannedPrefix — one ruler for the cursor (playhead-dgly)")
struct SupportedScannedPrefixTests {

    private let threshold = RescanThresholdSec.adScanRescanWorthyGapSec

    @Test("The 2026-08-14 witness: 3C2FFE10's own spans support 659.46 s, not 7,998.72")
    func deviceWitnessPrefix() {
        // The device's real coverage-lane rows: a contiguous head that stops at
        // 659.46 s, then NOTHING until 7,939.14 s. The 7,279.68 s between them
        // has never been read by anything.
        let prefix = AnalysisCoverageMath.supportedScannedPrefix(
            examinedSpans: [
                (start: 0.78, end: 59.04),
                (start: 60.0, end: 125.28),
                (start: 131.46, end: 216.18),
                (start: 216.54, end: 304.92),
                (start: 305.28, end: 374.16),
                (start: 374.52, end: 401.88),
                (start: 402.48, end: 487.8),
                (start: 488.76, end: 583.74),
                (start: 584.28, end: 659.46),
                (start: 7_939.14, end: 7_998.72)
            ],
            rescanThreshold: threshold
        )
        #expect(abs(prefix.rawValue - 659.46) < 0.001)
    }

    @Test("No examined span at all supports nothing")
    func emptySupportsZero() {
        #expect(
            AnalysisCoverageMath.supportedScannedPrefix(
                examinedSpans: [], rescanThreshold: threshold
            ).rawValue == 0
        )
    }

    @Test("It measures FROM ZERO: a run that legitimately began mid-episode supports no prefix")
    func measuresFromZero() {
        // This is the property that separates it from
        // `BackfillJobRunner.contiguousPlannedReach`, which seeds from the
        // first span it was handed. A cursor claims `[0, x]`, so a scan that
        // starts at 300 s on a 60 s threshold has proven nothing about `[0, x]`.
        let prefix = AnalysisCoverageMath.supportedScannedPrefix(
            examinedSpans: [(start: 300.0, end: 900.0)],
            rescanThreshold: threshold
        )
        #expect(prefix.rawValue == 0)
    }

    @Test("A gap AT the threshold bridges; one over it stops the walk")
    func thresholdIsStrictlyGreater() {
        // `warrantsRescan` is `gap > rawValue`, so exactly 60 s bridges.
        let atThreshold = AnalysisCoverageMath.supportedScannedPrefix(
            examinedSpans: [(start: 0, end: 100), (start: 160.0, end: 200)],
            rescanThreshold: threshold
        )
        #expect(atThreshold.rawValue == 200)

        let overThreshold = AnalysisCoverageMath.supportedScannedPrefix(
            examinedSpans: [(start: 0, end: 100), (start: 160.01, end: 200)],
            rescanThreshold: threshold
        )
        #expect(overThreshold.rawValue == 100)
    }

    @Test("Unsorted input and degenerate spans do not move the answer")
    func normalizesInput() {
        let prefix = AnalysisCoverageMath.supportedScannedPrefix(
            examinedSpans: [
                (start: 60, end: 120),
                (start: .nan, end: 500),
                (start: 0, end: 50),
                (start: 200, end: 200),
                (start: 300, end: .infinity)
            ],
            rescanThreshold: threshold
        )
        // 0-50, 50→60 bridges, 60-120, 120→200 bridges but the span is
        // degenerate, 120→300 is 180 s and stops the walk.
        #expect(prefix.rawValue == 120)
    }
}

// MARK: - Fixtures

/// The 2026-08-14 device pull, reproduced from its own rows.
enum DevicePullFixture {

    /// (assetId, analysisState, transcript reach, supported scanned prefix)
    static let assets: [(String, String, Double, Double)] = [
        ("3C2FFE10", "queued", 7_999.007346926723, 659.46),
        ("561CEF5B", "queued", 1_952.2089795895793, 1_952.16),
        ("7DD870DC", "queued", 4_293.9820625, 4_293.96),
        ("A9F6DF05", "queued", 6_874.2530625, 6_036.84),
        ("AA6CD430", "queued", 1_625.9134693860262, 1_625.91),
        ("B97B8779", "queued", 1_750.712375, 1_750.62),
        ("C0610BF9", "queued", 1_930.8408163243078, 1_930.74),
        ("C065AD03", "backfill", 1_518.6808163249755, 1_518.48),
        ("F4A9D2BD", "queued", 1_993.1951020408164, 1_992.90)
    ]

    /// (jobId, assetId, status, retryCount, cursor, deferReason)
    static let jobs: [(String, String, String, Int, Double?, String)] = [
        ("fm-c42dc1a029b38e37", "3C2FFE10", "failed", 3, 7_998.72,
         "underCoverageBudgetSpent-fullEpisodeScan"),
        ("fm-8eef0f63c4f5d924", "561CEF5B", "failed", 3, 1_952.16,
         "underCoverageBudgetSpent-fullEpisodeScan"),
        ("fm-239aeb804665a0c0", "7DD870DC", "failed", 3, 4_293.96,
         "underCoverageBudgetSpent-fullEpisodeScan"),
        ("fm-9330e821aeb36a0d", "A9F6DF05", "failed", 3, 2_882.94,
         "Error Domain=FoundationModels.LanguageModelError Code=-1 \"ModelManagerError 1001\""),
        ("fm-b93dd4f616ecfba8", "AA6CD430", "failed", 3, nil,
         "underCoverageBudgetSpent-fullEpisodeScan"),
        ("fm-8064bbfb378e0bed", "B97B8779", "complete", 0, 1_750.62,
         "underCoverageBudgetSpent-fullEpisodeScan"),
        ("fm-05a702d681ca687b", "C0610BF9", "failed", 3, 1_930.74,
         "underCoverageBudgetSpent-fullEpisodeScan"),
        ("fm-5488ec4b73ec1314", "C065AD03", "failed", 3, 1_518.48,
         "underCoverageBudgetSpent-fullEpisodeScan"),
        ("fm-2c1a21707331fda7", "F4A9D2BD", "failed", 3, 1_992.90,
         "underCoverageBudgetSpent-fullEpisodeScan")
    ]

    /// The five `eligibilityGate='eligible'` rows. Four are the day-0
    /// byte-exact marks `playhead-exy0` established were never offered; the
    /// fifth is the span Dan drew by hand, which WAS applied.
    static let eligibleWindows: [PersistedStateSnapshot.AdWindowRow] = [
        window("478D063E", "3C2FFE10", 0.0, 30.055291823736166,
               "dayZeroRediffByteExact", "candidate", "rediffByteExact", "rediffByteExact", false),
        window("5FAE94E0", "3C2FFE10", 7_939.030454444587, 7_999.007346926723,
               "dayZeroRediffByteExact", "candidate", "rediffByteExact", "rediffByteExact", false),
        window("18829EC5", "561CEF5B", 0.0, 60.13346025950383,
               "dayZeroRediffByteExact", "candidate", "rediffByteExact", "rediffByteExact", false),
        window("84E493B1", "561CEF5B", 1_892.1539701354156, 1_952.2089795895793,
               "dayZeroRediffByteExact", "candidate", "rediffByteExact", "rediffByteExact", false),
        window("E2062903", "C065AD03", 0.0, 46.55245386192805,
               "userMarked", "applied", "unanchored", "unanchored", true)
    ]

    static func window(
        _ id: String,
        _ assetId: String,
        _ start: Double,
        _ end: Double,
        _ boundaryState: String,
        _ decisionState: String,
        _ startAnchor: String,
        _ endAnchor: String,
        _ wasSkipped: Bool
    ) -> PersistedStateSnapshot.AdWindowRow {
        PersistedStateSnapshot.AdWindowRow(
            windowId: id,
            assetId: assetId,
            startTime: start,
            endTime: end,
            boundaryState: boundaryState,
            decisionState: decisionState,
            eligibilityGate: "eligible",
            startEdgeAnchor: startAnchor,
            endEdgeAnchor: endAnchor,
            wasSkipped: wasSkipped,
            userDismissedBanner: false
        )
    }

    static func snapshot() -> PersistedStateSnapshot {
        PersistedStateSnapshot(
            backfillJobs: jobs.map { jobId, assetId, status, retry, cursor, defer_ in
                PersistedStateSnapshot.BackfillJobRow(
                    jobId: jobId,
                    assetId: assetId,
                    status: status,
                    retryCount: retry,
                    deferReason: defer_,
                    updatedAt: 1_786_692_918,
                    claimedUpperBoundSec: cursor
                )
            },
            assets: assets.map { assetId, state, reach, prefix in
                PersistedStateSnapshot.AssetRow(
                    assetId: assetId,
                    episodeId: "ep-\(assetId)",
                    analysisState: state,
                    supportedScannedPrefixSec: prefix,
                    transcriptReachSec: reach
                )
            },
            eligibilityGatedAdWindows: eligibleWindows,
            coverageLaneRetryCap: 3
        )
    }
}

extension Array where Element == PersistedStateInvariantFinding {
    func finding(_ invariant: PersistedStateInvariant) -> PersistedStateInvariantFinding? {
        first { $0.invariant == invariant }
    }
}

// MARK: - The device reading

@Suite("Persisted-state invariants on the 2026-08-14 device pull (playhead-dgly)")
struct PersistedStateInvariantDevicePullTests {

    @Test("Every invariant returns a finding, whether or not it fired")
    func everyInvariantIsJudged() {
        let findings = PersistedStateInvariantEvaluator.evaluate(DevicePullFixture.snapshot())
        #expect(findings.count == PersistedStateInvariant.allCases.count)
        #expect(findings.map(\.invariant) == PersistedStateInvariant.allCases)
    }

    @Test("1e86 — no stranded running row on this pull: 0 of 9")
    func strandedRunningReadsZeroOfNine() throws {
        let findings = PersistedStateInvariantEvaluator.evaluate(DevicePullFixture.snapshot())
        let finding = try #require(findings.finding(.strandedRunningBackfillJob))
        #expect(finding.violations == 0)
        #expect(finding.population == 9)
        // A zero over a NON-EMPTY denominator is the positive claim. A zero
        // over zero would be the vacuous one, and the census says which it is.
        #expect(finding.witnesses.isEmpty)
    }

    @Test("wogi — 1 of 8 cursors claims more than its own scan rows support")
    func cursorOverclaimReadsOneOfEight() throws {
        let findings = PersistedStateInvariantEvaluator.evaluate(DevicePullFixture.snapshot())
        let finding = try #require(findings.finding(.coarseCursorBeyondScannedPrefix))
        // Eight of the nine rows carry a cursor; AA6CD430's carries none, so it
        // makes no claim and is not judged.
        #expect(finding.population == 8)
        #expect(finding.violations == 1)
        #expect(finding.abstained == 0)
        let witness = try #require(finding.witnesses.first)
        #expect(witness.contains("job=fm-c42dc1a029b38e37"))
        #expect(witness.contains("claimed=7998.72"))
        #expect(witness.contains("supported=659.46"))
        #expect(witness.contains("excess=7339.26"))
    }

    @Test("e6d3 — 2 of 8 at-cap rows are dead with audio still above the cursor")
    func retryBudgetReadsTwoOfEight() throws {
        let findings = PersistedStateInvariantEvaluator.evaluate(DevicePullFixture.snapshot())
        let finding = try #require(findings.finding(.retryBudgetSpentWithWorkRemaining))
        // Eight rows are at `retryCount >= 3`; B97B8779's is `complete` at 0.
        #expect(finding.population == 8)
        #expect(finding.violations == 2)
        let named = finding.witnesses.joined(separator: " || ")
        // A9F6DF05 is playhead-59c8's row: retired on an unclassified
        // FoundationModels error with 3,991 s of transcript above its cursor.
        #expect(named.contains("fm-9330e821aeb36a0d"))
        // AA6CD430 published no cursor at all and is still at the cap.
        #expect(named.contains("fm-b93dd4f616ecfba8"))
        // The six honestly-saturated rows are NOT named. Their cursors sit
        // within 0.30 s of their transcript's reach — they are under-covered
        // because they are under-TRANSCRIBED, which is a different bead.
        #expect(!named.contains("fm-2c1a21707331fda7"))
        #expect(!named.contains("fm-c42dc1a029b38e37"))
    }

    @Test("1216 — no asset on this pull is still in the registration state: 0 of 0")
    func registrationStateReadsZeroOfZero() throws {
        let findings = PersistedStateInvariantEvaluator.evaluate(DevicePullFixture.snapshot())
        let finding = try #require(findings.finding(.newAssetWithAudioAndFailedJob))
        #expect(finding.violations == 0)
        // ZERO OVER ZERO, AND THE CENSUS SAYS SO. Every asset on this pull has
        // been advanced out of `new`, so there is nothing to judge — which is a
        // weaker statement than "nine assets were judged and all nine were
        // fine", and the reader can tell them apart because the denominator is
        // printed.
        #expect(finding.population == 0)
    }

    @Test("exy0 — 4 of 4 eligible auto-seeded windows were never offered")
    func neverOfferedReadsFourOfFour() throws {
        let findings = PersistedStateInvariantEvaluator.evaluate(DevicePullFixture.snapshot())
        let finding = try #require(findings.finding(.eligibleAutoWindowNeverOffered))
        // The user-marked row is `.userAsserted`, which consults the show's
        // trust history, so it is not in this population at all — and it was
        // applied anyway.
        #expect(finding.population == 4)
        #expect(finding.violations == 4)
        let named = finding.witnesses.joined(separator: " || ")
        for id in ["478D063E", "5FAE94E0", "18829EC5", "84E493B1"] {
            #expect(named.contains("window=\(id)"))
        }
        #expect(!named.contains("window=E2062903"))
        #expect(named.contains("class=rediffByteExact"))
    }
}

// MARK: - Both directions, per invariant

@Suite("Persisted-state invariants fire when violated (playhead-dgly)")
struct PersistedStateInvariantFiringTests {

    private func snapshot(
        jobs: [PersistedStateSnapshot.BackfillJobRow] = [],
        assets: [PersistedStateSnapshot.AssetRow] = [],
        windows: [PersistedStateSnapshot.AdWindowRow] = [],
        retryCap: Int = 3
    ) -> PersistedStateSnapshot {
        PersistedStateSnapshot(
            backfillJobs: jobs,
            assets: assets,
            eligibilityGatedAdWindows: windows,
            coverageLaneRetryCap: retryCap
        )
    }

    private func job(
        _ jobId: String,
        asset: String = "a1",
        status: String = "failed",
        retryCount: Int = 0,
        cursor: Double? = nil
    ) -> PersistedStateSnapshot.BackfillJobRow {
        PersistedStateSnapshot.BackfillJobRow(
            jobId: jobId,
            assetId: asset,
            status: status,
            retryCount: retryCount,
            deferReason: nil,
            updatedAt: 1_000,
            claimedUpperBoundSec: cursor
        )
    }

    private func asset(
        _ assetId: String = "a1",
        state: String = "queued",
        prefix: Double? = nil,
        reach: Double? = nil,
        audio: Bool? = nil,
        jobState: String? = nil,
        jobError: String? = nil
    ) -> PersistedStateSnapshot.AssetRow {
        PersistedStateSnapshot.AssetRow(
            assetId: assetId,
            episodeId: "ep-\(assetId)",
            analysisState: state,
            supportedScannedPrefixSec: prefix,
            transcriptReachSec: reach,
            hasAudioOnDisk: audio,
            newestJobState: jobState,
            newestJobLastErrorCode: jobError
        )
    }

    // --- Invariant 1 -----------------------------------------------------

    @Test("A row at status='running' fires; every other status does not")
    func strandedRunningBothDirections() throws {
        let statuses = ["queued", "deferred", "failed", "complete"]
        let clean = snapshot(jobs: statuses.map { job("j-\($0)", status: $0) })
        let cleanFinding = try #require(
            PersistedStateInvariantEvaluator.evaluate(clean)
                .finding(.strandedRunningBackfillJob))
        #expect(cleanFinding.violations == 0)
        #expect(cleanFinding.population == statuses.count)

        let dirty = snapshot(
            jobs: statuses.map { job("j-\($0)", status: $0) } + [job("j-run", status: "running")]
        )
        let dirtyFinding = try #require(
            PersistedStateInvariantEvaluator.evaluate(dirty)
                .finding(.strandedRunningBackfillJob))
        #expect(dirtyFinding.violations == 1)
        #expect(dirtyFinding.population == statuses.count + 1)
        #expect(try #require(dirtyFinding.witnesses.first).contains("job=j-run"))
    }

    // --- Invariant 2 -----------------------------------------------------

    @Test("A cursor above its prefix fires; at, below, and unsupported do not")
    func cursorOverclaimBothDirections() throws {
        let over = snapshot(
            jobs: [job("j-over", cursor: 900)],
            assets: [asset(prefix: 500)])
        #expect(
            try #require(PersistedStateInvariantEvaluator.evaluate(over)
                .finding(.coarseCursorBeyondScannedPrefix)).violations == 1)

        let equal = snapshot(
            jobs: [job("j-equal", cursor: 500)],
            assets: [asset(prefix: 500)])
        #expect(
            try #require(PersistedStateInvariantEvaluator.evaluate(equal)
                .finding(.coarseCursorBeyondScannedPrefix)).violations == 0)

        // A cursor BELOW its evidence is not a violation — the walk simply has
        // not caught up. Raising a cursor is not a repair, it is the defect.
        let under = snapshot(
            jobs: [job("j-under", cursor: 100)],
            assets: [asset(prefix: 500)])
        #expect(
            try #require(PersistedStateInvariantEvaluator.evaluate(under)
                .finding(.coarseCursorBeyondScannedPrefix)).violations == 0)
    }

    @Test("An asset with NO examined scan row is abstained on, not counted clean")
    func cursorOverclaimAbstains() throws {
        let noEvidence = snapshot(
            jobs: [job("j-blind", cursor: 900)],
            assets: [asset(prefix: nil)])
        let finding = try #require(
            PersistedStateInvariantEvaluator.evaluate(noEvidence)
                .finding(.coarseCursorBeyondScannedPrefix))
        #expect(finding.violations == 0)
        #expect(finding.population == 0)
        #expect(finding.abstained == 1)
        #expect(finding.abstainReason == "no_examined_scan_row")
    }

    // --- Invariant 3 -----------------------------------------------------

    @Test("At the cap with audio above the cursor fires; saturated and under-cap do not")
    func retryBudgetBothDirections() throws {
        // At the cap, 900 s of transcript above the cursor.
        let dead = snapshot(
            jobs: [job("j-dead", retryCount: 3, cursor: 100)],
            assets: [asset(reach: 1_000)])
        let deadFinding = try #require(
            PersistedStateInvariantEvaluator.evaluate(dead)
                .finding(.retryBudgetSpentWithWorkRemaining))
        #expect(deadFinding.violations == 1)
        #expect(try #require(deadFinding.witnesses.first).contains("remaining=900.00"))

        // At the cap but SATURATED — e6d3's legitimate exit.
        let saturated = snapshot(
            jobs: [job("j-sat", retryCount: 3, cursor: 999.9)],
            assets: [asset(reach: 1_000)])
        #expect(
            try #require(PersistedStateInvariantEvaluator.evaluate(saturated)
                .finding(.retryBudgetSpentWithWorkRemaining)).violations == 0)

        // Under the cap: still alive, not this invariant's business.
        let alive = snapshot(
            jobs: [job("j-alive", retryCount: 2, cursor: 100)],
            assets: [asset(reach: 1_000)])
        let aliveFinding = try #require(
            PersistedStateInvariantEvaluator.evaluate(alive)
                .finding(.retryBudgetSpentWithWorkRemaining))
        #expect(aliveFinding.violations == 0)
        #expect(aliveFinding.population == 0)
    }

    @Test("The remainder is judged against the 60 s rescan threshold, not against zero")
    func retryBudgetUsesRescanThreshold() throws {
        // 59 s left is a breath, not a hole; 61 s is worth paying FM
        // wall-clock for. The boundary is `RescanThresholdSec`'s, so a retune
        // of that constant retunes this invariant with it.
        let narrow = snapshot(
            jobs: [job("j-narrow", retryCount: 3, cursor: 941)],
            assets: [asset(reach: 1_000)])
        #expect(
            try #require(PersistedStateInvariantEvaluator.evaluate(narrow)
                .finding(.retryBudgetSpentWithWorkRemaining)).violations == 0)

        let wide = snapshot(
            jobs: [job("j-wide", retryCount: 3, cursor: 939)],
            assets: [asset(reach: 1_000)])
        #expect(
            try #require(PersistedStateInvariantEvaluator.evaluate(wide)
                .finding(.retryBudgetSpentWithWorkRemaining)).violations == 1)
    }

    @Test("The cap is the admission model's, carried by value")
    func retryBudgetReadsTheCapFromTheSnapshot() throws {
        let snap = snapshot(
            jobs: [job("j", retryCount: 3, cursor: 0)],
            assets: [asset(reach: 1_000)],
            retryCap: 5)
        let finding = try #require(
            PersistedStateInvariantEvaluator.evaluate(snap)
                .finding(.retryBudgetSpentWithWorkRemaining))
        #expect(finding.population == 0)
        #expect(finding.violations == 0)
    }

    // --- Invariant 4 -----------------------------------------------------

    @Test("new + audio + a failed job fires; each missing clause does not")
    func registrationStateBothDirections() throws {
        let dirty = snapshot(assets: [
            asset(state: "new", audio: true, jobState: "failed", jobError: "download:gone")
        ])
        let dirtyFinding = try #require(
            PersistedStateInvariantEvaluator.evaluate(dirty)
                .finding(.newAssetWithAudioAndFailedJob))
        #expect(dirtyFinding.violations == 1)
        #expect(try #require(dirtyFinding.witnesses.first).contains("audio_on_disk=1"))

        // No audio: `new` is the honest state for an asset whose bytes are gone.
        let noAudio = snapshot(assets: [
            asset(state: "new", audio: false, jobState: "failed", jobError: "download:gone")
        ])
        #expect(
            try #require(PersistedStateInvariantEvaluator.evaluate(noAudio)
                .finding(.newAssetWithAudioAndFailedJob)).violations == 0)

        // Job still queued and carrying no error: nothing has failed yet.
        let queued = snapshot(assets: [
            asset(state: "new", audio: true, jobState: "queued", jobError: nil)
        ])
        #expect(
            try #require(PersistedStateInvariantEvaluator.evaluate(queued)
                .finding(.newAssetWithAudioAndFailedJob)).violations == 0)

        // Advanced out of `new`: not this population.
        let advanced = snapshot(assets: [
            asset(state: "backfill", audio: true, jobState: "failed", jobError: "x")
        ])
        let advancedFinding = try #require(
            PersistedStateInvariantEvaluator.evaluate(advanced)
                .finding(.newAssetWithAudioAndFailedJob))
        #expect(advancedFinding.violations == 0)
        #expect(advancedFinding.population == 0)
    }

    @Test("An unanswered audio question abstains rather than assuming either answer")
    func registrationStateAbstains() throws {
        let unresolved = snapshot(assets: [
            asset(state: "new", audio: nil, jobState: "failed", jobError: "x")
        ])
        let finding = try #require(
            PersistedStateInvariantEvaluator.evaluate(unresolved)
                .finding(.newAssetWithAudioAndFailedJob))
        #expect(finding.violations == 0)
        #expect(finding.population == 0)
        #expect(finding.abstained == 1)
        #expect(finding.abstainReason == "audio_presence_unresolved")
    }

    // --- Invariant 5 -----------------------------------------------------

    @Test("An eligible byte-exact candidate fires; every recorded door mark clears it")
    func neverOfferedBothDirections() throws {
        func row(
            decision: String,
            skipped: Bool = false,
            dismissed: Bool = false
        ) -> PersistedStateSnapshot.AdWindowRow {
            PersistedStateSnapshot.AdWindowRow(
                windowId: "w",
                assetId: "a1",
                startTime: 0,
                endTime: 30,
                boundaryState: "dayZeroRediffByteExact",
                decisionState: decision,
                eligibilityGate: "eligible",
                startEdgeAnchor: "rediffByteExact",
                endEdgeAnchor: "rediffByteExact",
                wasSkipped: skipped,
                userDismissedBanner: dismissed
            )
        }
        #expect(
            try #require(PersistedStateInvariantEvaluator.evaluate(
                snapshot(windows: [row(decision: "candidate")]))
                .finding(.eligibleAutoWindowNeverOffered)).violations == 1)

        // Each of the three marks a delivery door leaves.
        for cleared in [
            row(decision: "applied"),
            row(decision: "confirmed"),
            row(decision: "suppressed"),
            row(decision: "candidate", skipped: true),
            row(decision: "candidate", dismissed: true)
        ] {
            let finding = try #require(
                PersistedStateInvariantEvaluator.evaluate(snapshot(windows: [cleared]))
                    .finding(.eligibleAutoWindowNeverOffered))
            #expect(finding.violations == 0)
            #expect(finding.population == 1)
        }
    }

    @Test("markOnly rows and show-governed classes are outside the population")
    func neverOfferedPopulationIsNarrow() throws {
        let markOnly = PersistedStateSnapshot.AdWindowRow(
            windowId: "w-mark", assetId: "a1", startTime: 0, endTime: 30,
            boundaryState: "dayZeroRediffByteExact", decisionState: "candidate",
            eligibilityGate: "markOnly",
            startEdgeAnchor: "rediffByteExact", endEdgeAnchor: "rediffByteExact",
            wasSkipped: false, userDismissedBanner: false)
        let unanchored = PersistedStateSnapshot.AdWindowRow(
            windowId: "w-unanchored", assetId: "a1", startTime: 0, endTime: 30,
            boundaryState: "acousticRefined", decisionState: "candidate",
            eligibilityGate: "eligible",
            startEdgeAnchor: "unanchored", endEdgeAnchor: "unanchored",
            wasSkipped: false, userDismissedBanner: false)
        let finding = try #require(
            PersistedStateInvariantEvaluator.evaluate(
                snapshot(windows: [markOnly, unanchored]))
                .finding(.eligibleAutoWindowNeverOffered))
        #expect(finding.population == 0)
        #expect(finding.violations == 0)
    }
}

// MARK: - The record

@Suite("The reporter's durable record (playhead-dgly)")
struct PersistedStateInvariantReporterEmissionTests {

    private static func makeTempDirectory() throws -> URL {
        try makeTempDir(prefix: "dgly-reporter")
    }

    private static func lines(_ logger: SurfaceStatusInvariantLogger) throws
        -> [SurfaceStateTransitionEntry] {
        logger.flushForTesting()
        guard let url = logger.currentSessionFileURL else { return [] }
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try String(decoding: try Data(contentsOf: url), as: UTF8.self)
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map { try decoder.decode(SurfaceStateTransitionEntry.self, from: Data($0.utf8)) }
    }

    private static func reporter(
        snapshot: @escaping @Sendable () async throws -> PersistedStateSnapshot,
        logger: SurfaceStatusInvariantLogger
    ) -> PersistedStateInvariantReporter {
        PersistedStateInvariantReporter(
            snapshotProvider: snapshot,
            audioPresenceProbe: { _ in false },
            logger: logger
        )
    }

    @Test("A CLEAN store still emits one census line per invariant — zero is a claim")
    func cleanStoreStillReports() async throws {
        let logger = SurfaceStatusInvariantLogger(directory: try Self.makeTempDirectory())
        let empty = PersistedStateSnapshot(
            backfillJobs: [], assets: [], eligibilityGatedAdWindows: [], coverageLaneRetryCap: 3)
        await Self.reporter(snapshot: { empty }, logger: logger).report()

        let entries = try Self.lines(logger)
        let census = entries.compactMap(\.invariantViolation)
            .filter { $0.code == .persistedStateInvariantCensus }
        #expect(census.count == PersistedStateInvariant.allCases.count)
        // …and nothing claims a violation.
        #expect(entries.compactMap(\.invariantViolation)
            .filter { $0.code == .persistedStateInvariantViolation }.isEmpty)
        for invariant in PersistedStateInvariant.allCases {
            #expect(census.contains {
                $0.description.contains("invariant=\(invariant.rawValue)")
                    && $0.description.contains("violations=0")
            })
        }
    }

    @Test("The device pull emits its numerator AND denominator on every census line")
    func devicePullCensusCarriesBothTerms() async throws {
        let logger = SurfaceStatusInvariantLogger(directory: try Self.makeTempDirectory())
        let snapshot = DevicePullFixture.snapshot()
        await Self.reporter(snapshot: { snapshot }, logger: logger).report()

        let violations = try Self.lines(logger).compactMap(\.invariantViolation)
        let census = violations.filter { $0.code == .persistedStateInvariantCensus }
        #expect(census.contains {
            $0.description == "invariant=coarse_cursor_beyond_scanned_prefix"
                + " violations=1 population=8 witnesses=1/1"
        })
        #expect(census.contains {
            $0.description == "invariant=eligible_auto_window_never_offered"
                + " violations=4 population=4 witnesses=4/4"
        })
        #expect(census.contains {
            $0.description == "invariant=stranded_running_backfill_job"
                + " violations=0 population=9 witnesses=0/0"
        })
        // Seven witnesses in total: one cursor, two retry rows, four windows.
        let witnessed = violations.filter { $0.code == .persistedStateInvariantViolation }
        #expect(witnessed.count == 7)
        #expect(witnessed.allSatisfy { $0.description.hasPrefix("invariant=") })
    }

    @Test("A truncated witness list SAYS it is truncated, and the count stays honest")
    func witnessCapIsDeclared() async throws {
        let logger = SurfaceStatusInvariantLogger(directory: try Self.makeTempDirectory())
        let overCap = PersistedStateInvariantEvaluator.maxWitnessesPerInvariant + 5
        let snapshot = PersistedStateSnapshot(
            backfillJobs: (0..<overCap).map {
                PersistedStateSnapshot.BackfillJobRow(
                    jobId: "j-\($0)", assetId: "a", status: "running", retryCount: 0,
                    deferReason: nil, updatedAt: 0, claimedUpperBoundSec: nil)
            },
            assets: [], eligibilityGatedAdWindows: [], coverageLaneRetryCap: 3)
        await Self.reporter(snapshot: { snapshot }, logger: logger).report()

        let violations = try Self.lines(logger).compactMap(\.invariantViolation)
        #expect(violations.contains {
            $0.code == .persistedStateInvariantCensus
                && $0.description == "invariant=stranded_running_backfill_job"
                    + " violations=\(overCap) population=\(overCap)"
                    + " witnesses=\(PersistedStateInvariantEvaluator.maxWitnessesPerInvariant)/\(overCap)"
        })
        #expect(
            violations.filter {
                $0.code == .persistedStateInvariantViolation
            }.count == PersistedStateInvariantEvaluator.maxWitnessesPerInvariant)
    }

    @Test("A read that THROWS says so — silence must not be readable as 'clean'")
    func readFailureIsRecorded() async throws {
        struct Boom: Error {}
        let logger = SurfaceStatusInvariantLogger(directory: try Self.makeTempDirectory())
        let findings = await Self.reporter(snapshot: { throw Boom() }, logger: logger).report()
        #expect(findings.isEmpty)

        let violations = try Self.lines(logger).compactMap(\.invariantViolation)
        // playhead-gyhw narrowed this from `violations.count == 1` to the codes
        // this test is ABOUT. The reporter now also emits the always-present
        // REPAIR census, which is a claim about what the schema ladder did
        // before the read was attempted and is therefore correct on a launch
        // whose read then threw — see `repairsSurviveAFailedRead`. The property
        // the assertion exists for is unchanged and is spelled out below:
        // exactly one read-failure line, and NOT ONE invariant census or
        // violation line, so a reader cannot mistake the failure for a clean
        // sweep. That is the anti-vacuity control — the two silences stay
        // distinguishable.
        #expect(violations.filter { $0.code == .persistedStateInvariantReadFailed }.count == 1)
        #expect(!violations.contains { $0.code == .persistedStateInvariantCensus })
        #expect(!violations.contains { $0.code == .persistedStateInvariantViolation })
    }

    @Test("The audio oracle is asked ONLY about registration-state assets")
    func audioProbeIsScoped() async throws {
        let logger = SurfaceStatusInvariantLogger(directory: try Self.makeTempDirectory())
        let asked = OSAllocatedUnfairLockBox<[String]>([])
        let snapshot = PersistedStateSnapshot(
            backfillJobs: [],
            assets: [
                PersistedStateSnapshot.AssetRow(
                    assetId: "fresh", episodeId: "ep-fresh", analysisState: "new",
                    supportedScannedPrefixSec: nil, transcriptReachSec: nil,
                    newestJobState: "failed", newestJobLastErrorCode: "boom"),
                PersistedStateSnapshot.AssetRow(
                    assetId: "working", episodeId: "ep-working", analysisState: "backfill",
                    supportedScannedPrefixSec: 10, transcriptReachSec: 10)
            ],
            eligibilityGatedAdWindows: [], coverageLaneRetryCap: 3)
        let reporter = PersistedStateInvariantReporter(
            snapshotProvider: { snapshot },
            audioPresenceProbe: { episodeId in
                asked.mutate { $0.append(episodeId) }
                return true
            },
            logger: logger)
        let findings = await reporter.report()

        #expect(asked.value == ["ep-fresh"])
        let finding = try #require(findings.finding(.newAssetWithAudioAndFailedJob))
        #expect(finding.violations == 1)
        #expect(finding.population == 1)
    }
}

/// A tiny Sendable box so a test closure can record what it was asked.
final class OSAllocatedUnfairLockBox<Value: Sendable>: @unchecked Sendable {
    private var storage: Value
    private let lock = NSLock()

    init(_ initial: Value) { self.storage = initial }

    var value: Value {
        lock.lock()
        defer { lock.unlock() }
        return storage
    }

    func mutate(_ body: (inout Value) -> Void) {
        lock.lock()
        defer { lock.unlock() }
        body(&storage)
    }
}

// MARK: - The store read

@Suite("fetchPersistedStateSnapshot reads what is actually persisted (playhead-dgly)")
struct PersistedStateSnapshotStoreTests {

    private func makeStore() throws -> (AnalysisStore, URL) {
        let dir = try makeTempDir(prefix: "dgly-store")
        return (try AnalysisStore(directory: dir), dir)
    }

    private func makeAsset(id: String, state: String, fast: Double?, final: Double?)
        -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: fast,
            confirmedAdCoverageEndTime: nil,
            analysisState: state,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 8_000,
            finalPassCoverageEndTime: final
        )
    }

    private func storeWindow(
        id: String,
        boundaryState: String,
        eligibilityGate: String?,
        anchor: String
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: "a",
            startTime: 0,
            endTime: 30,
            confidence: 1.0,
            boundaryState: boundaryState,
            decisionState: "candidate",
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: eligibilityGate,
            startEdgeAnchor: anchor,
            endEdgeAnchor: anchor
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

    @Test("The device witness survives the round trip through SQLite")
    func deviceWitnessThroughTheStore() async throws {
        let (store, _) = try makeStore()
        try await store.migrate()
        try await store.insertAsset(
            makeAsset(id: "3C2FFE10", state: "queued", fast: 7_920, final: 7_999.007346926723))
        // The head, then the 7,280 s hole, then the tail.
        try await store.insertSemanticScanResult(
            makeScan(assetId: "3C2FFE10", index: 0, start: 0.78, end: 659.46))
        try await store.insertSemanticScanResult(
            makeScan(assetId: "3C2FFE10", index: 1, start: 7_939.14, end: 7_998.72))
        try await store.insertBackfillJob(makeBackfillJob(
            jobId: "fm-c42dc1a029b38e37",
            analysisAssetId: "3C2FFE10",
            progressCursor: BackfillProgressCursor(
                processedPhaseCount: 0,
                lastProcessedUpperBoundSec: EpisodeSeconds(7_998.72)),
            retryCount: 3,
            deferReason: "underCoverageBudgetSpent-fullEpisodeScan",
            status: .failed))

        let snapshot = try await store.fetchPersistedStateSnapshot()
        #expect(snapshot.backfillJobs.count == 1)
        #expect(snapshot.backfillJobs.first?.claimedUpperBoundSec == 7_998.72)
        #expect(snapshot.backfillJobs.first?.retryCount == 3)
        let asset = try #require(snapshot.assets.first)
        #expect(abs((asset.supportedScannedPrefixSec ?? -1) - 659.46) < 0.001)
        #expect(abs((asset.transcriptReachSec ?? -1) - 7_999.007346926723) < 0.001)
        #expect(snapshot.coverageLaneRetryCap == AdmissionController.maxRetries)

        let findings = PersistedStateInvariantEvaluator.evaluate(snapshot)
        #expect(try #require(findings.finding(.coarseCursorBeyondScannedPrefix)).violations == 1)
    }

    @Test("An asset with no examined scan row reads nil, NOT a prefix of zero")
    func absenceIsNotZero() async throws {
        let (store, _) = try makeStore()
        try await store.migrate()
        try await store.insertAsset(
            makeAsset(id: "blind", state: "queued", fast: 100, final: nil))
        try await store.insertBackfillJob(makeBackfillJob(
            jobId: "fm-blind",
            analysisAssetId: "blind",
            progressCursor: BackfillProgressCursor(
                processedPhaseCount: 0,
                lastProcessedUpperBoundSec: EpisodeSeconds(90)),
            status: .failed))

        let snapshot = try await store.fetchPersistedStateSnapshot()
        #expect(snapshot.assets.first?.supportedScannedPrefixSec == nil)
        let finding = try #require(
            PersistedStateInvariantEvaluator.evaluate(snapshot)
                .finding(.coarseCursorBeyondScannedPrefix))
        // A prefix of zero would have made this a 90 s violation. It abstains.
        #expect(finding.violations == 0)
        #expect(finding.abstained == 1)
    }

    @Test("The read never writes: the store is byte-identical afterwards")
    func theReadIsReadOnly() async throws {
        let (store, dir) = try makeStore()
        try await store.migrate()
        try await store.insertAsset(
            makeAsset(id: "a", state: "new", fast: 100, final: nil))
        try await store.insertBackfillJob(makeBackfillJob(
            jobId: "j", analysisAssetId: "a", retryCount: 3, status: .failed))
        _ = try await store.fetchPersistedStateSnapshot()

        // The invariant that matters most about this bead: it REPORTS. Read the
        // rows back through the store's own accessors and assert nothing moved.
        let job = try #require(try await store.fetchBackfillJob(byId: "j"))
        #expect(job.status == .failed)
        #expect(job.retryCount == 3)
        #expect(job.progressCursor == nil)
        let asset = try #require(try await store.fetchAsset(id: "a"))
        #expect(asset.analysisState == "new")
        #expect(FileManager.default.fileExists(atPath: dir.path))
    }

    @Test("Only eligibility-gated ad windows are carried, and the gate value survives")
    func adWindowPopulationIsFiltered() async throws {
        let (store, _) = try makeStore()
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: "a", state: "queued", fast: 100, final: nil))
        try await store.insertAdWindow(storeWindow(
            id: "gated", boundaryState: "dayZeroRediffByteExact",
            eligibilityGate: "eligible", anchor: "rediffByteExact"))
        try await store.insertAdWindow(storeWindow(
            id: "ungated", boundaryState: "acousticRefined",
            eligibilityGate: nil, anchor: "unanchored"))

        let snapshot = try await store.fetchPersistedStateSnapshot()
        #expect(snapshot.eligibilityGatedAdWindows.map(\.windowId) == ["gated"])
        #expect(snapshot.eligibilityGatedAdWindows.first?.eligibilityGate == "eligible")
        #expect(
            try #require(PersistedStateInvariantEvaluator.evaluate(snapshot)
                .finding(.eligibleAutoWindowNeverOffered)).violations == 1)
    }

    /// THE LAUNCH COST, MEASURED. The reporter is AWAITED in the bootstrap
    /// chain — that ordering is the whole bead — so a cost nobody measured is a
    /// cost nobody can defend. This seeds a library an order of magnitude
    /// larger than the 2026-08-14 device (9 assets / 533 `passA` rows) and
    /// prints what one snapshot read costs.
    ///
    /// **No wall-clock ASSERTION, deliberately.** This box runs ~8,000 tests
    /// against 16 GB, so an absolute budget here would measure machine load
    /// while being read as code correctness — this repo's standing defect
    /// class, living in the harness. The assertion is on the RESULT; the
    /// duration is printed so the order of magnitude is on the record and can
    /// be re-measured by anyone who doubts it.
    @Test("Launch cost: one snapshot read over a 90-asset / 5,400-row library")
    func launchCostOverALargeLibrary() async throws {
        let (store, _) = try makeStore()
        try await store.migrate()
        let assetCount = 90
        let scansPerAsset = 60
        for assetIndex in 0..<assetCount {
            let assetId = String(format: "asset-%03d", assetIndex)
            try await store.insertAsset(
                makeAsset(id: assetId, state: "queued", fast: 3_600, final: 3_600))
            for scanIndex in 0..<scansPerAsset {
                let start = Double(scanIndex) * 60
                try await store.insertSemanticScanResult(
                    makeScan(assetId: assetId, index: scanIndex, start: start, end: start + 59))
            }
            try await store.insertBackfillJob(makeBackfillJob(
                jobId: "job-\(assetId)",
                analysisAssetId: assetId,
                progressCursor: BackfillProgressCursor(
                    processedPhaseCount: 0,
                    lastProcessedUpperBoundSec: EpisodeSeconds(3_540)),
                retryCount: 3,
                status: .failed))
        }

        let started = ContinuousClock.now
        let snapshot = try await store.fetchPersistedStateSnapshot()
        let elapsed = ContinuousClock.now - started

        #expect(snapshot.assets.count == assetCount)
        #expect(snapshot.backfillJobs.count == assetCount)
        // 60 windows of 59 s separated by 1 s gaps: every gap bridges, so the
        // supported prefix is the last window's end for every asset. If the
        // walk ever stopped early this would read 59.
        #expect(snapshot.assets.allSatisfy {
            abs(($0.supportedScannedPrefixSec ?? -1) - 3_599) < 0.001
        })
        print("[playhead-dgly] fetchPersistedStateSnapshot over \(assetCount) assets / "
              + "\(assetCount * scansPerAsset) passA rows: \(elapsed)")
    }
}
