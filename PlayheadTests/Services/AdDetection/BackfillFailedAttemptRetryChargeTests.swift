// BackfillFailedAttemptRetryChargeTests.swift
// playhead-ronl — `backfill_jobs.retryCount` is charged by FOUR arms, and two of
// them counted a different thing from the other two.
//
// THE COLUMN AND ITS TWO RULERS. `AdmissionController.maxRetries` is documented
// as "the number of FAILED attempts allowed". Enumerated on `c9d0eb36`, every
// write of `backfill_jobs.retryCount` in `BackfillJobRunner`:
//
//   arm                                rule BEFORE this bead
//   1 under-coverage terminal          CURSOR-AWARE (playhead-41mu / e6d3)
//   2 cancellation / window expiry     CURSOR-AWARE (playhead-bkhc)
//   3 typed AnalysisStoreError         FLAT: isPermanent ? maxRetries : prior + 1
//   4 generic / unclassified throw     FLAT: prior + 1
//
// So on arms 1 and 2 the column counted CONSECUTIVE ATTEMPTS THAT BANKED NO NEW
// AUDIO, and on arms 3 and 4 it counted ATTEMPTS — one column, two quantities,
// and nothing at either site said so. A job that banked forty minutes of durable
// coarse scanning and THEN threw something unclassifiable was charged exactly as
// much as a job that threw in its prologue having examined nothing. That is this
// repo's standing defect class in the admission model: a value that names one
// thing read as though it named another.
//
// WHAT THE FIX IS, AND WHAT IT IS NOT. The ARITHMETIC on arms 3 and 4 now comes
// from the same rule the other two use. The DISPOSITION is untouched: the row is
// still `failed`, never `deferred`; `FMDaemonRefusal` gains no case; the cause
// tokens are byte-identical. playhead-59c8 established that an unclassified
// `ModelManagerError 1001` must NOT be reclassified as a daemon condition,
// because that would grant a possibly-permanent failure an "it will heal on its
// own" reading forever. Charging an attempt for what it FAILED TO DO is a
// different claim from excusing an error, and `unclassifiedThrowIsChargedWhenItBanksNothing`
// below is where the difference is visible: the same error, thrown three times
// from the pass prologue, still retires the job.
//
// THE QUESTION THE BEAD ASKED. "What cursor-advance evidence exists when the
// throw ESCAPED the pass?" `BackfillHonestCursorBox` cannot answer it — the
// end-of-pass digest fills it, so a throw from inside `coarsePassA` (where
// essentially all of a 12-45 minute pass's wall clock is) finds it empty.
// playhead-26od's mid-flight checkpoint can: it writes `backfill_jobs
// .progressCursor` AS the pass banks windows, and `CoarseCheckpointBox
// .lastDurableProgressCursor` is the value it handed the store. That the witness
// is already IN THE ROW is not incidental — it is what keeps the budget bounded,
// because a forgiven attempt has left the next one strictly less audio to plan.

import Foundation
import Testing

@testable import Playhead

// MARK: - The rule

@Suite("playhead-ronl: what a FAILING attempt charges the retry budget")
struct FailedAttemptRetryChargeRuleTests {

    /// The unchanged half, stated first because the whole fix depends on it
    /// staying unchanged: an attempt that banked nothing costs exactly what it
    /// always cost.
    @Test("a barren attempt costs one, at every point in the budget")
    func aBarrenAttemptCostsOne() {
        for prior in 0..<AdmissionController.maxRetries {
            #expect(
                BackfillJobRunner.failedAttemptRetryCount(
                    priorRetryCount: prior,
                    bankedNewAudio: false
                ) == prior + 1
            )
        }
    }

    /// A RESET to zero, not a "do not increment" — and the two differ on exactly
    /// the attempt that matters. A job at 2 that banks audio must land on 0: the
    /// quantity is a RUN of consecutive barren attempts, and one that banked
    /// audio ends the run. Leaving it at 2 would retire the job on its next
    /// barren attempt, having forgiven nothing.
    @Test("an attempt that banked new audio costs NOTHING, at every point in the budget")
    func aBankingAttemptCostsNothing() {
        for prior in 0...AdmissionController.maxRetries {
            #expect(
                BackfillJobRunner.failedAttemptRetryCount(
                    priorRetryCount: prior,
                    bankedNewAudio: true
                ) == 0,
                "a job at \(prior) that banked audio must land on 0, not on \(prior)"
            )
        }
    }

    /// THE BOUND. The counter-argument the flat rule was defending — a job that
    /// can never converge must still terminate — survives, because a barren
    /// attempt is charged exactly as it was.
    @Test("three consecutive barren attempts still reach the cap")
    func threeBarrenAttemptsReachTheCap() {
        var retryCount = 0
        for _ in 0..<AdmissionController.maxRetries {
            retryCount = BackfillJobRunner.failedAttemptRetryCount(
                priorRetryCount: retryCount,
                bankedNewAudio: false
            )
        }
        #expect(retryCount == AdmissionController.maxRetries)
    }

    /// CONSECUTIVE, and the word is load-bearing. A job whose barren attempts are
    /// scattered through a history of productive ones is not a job that failed
    /// three times; it is a job that is converging slowly, and the flat rule
    /// killed it for the shape of its history rather than for its progress.
    @Test("a banking attempt in the middle restarts the run of barren ones")
    func aBankingAttemptRestartsTheRun() {
        var retryCount = 0
        retryCount = BackfillJobRunner.failedAttemptRetryCount(priorRetryCount: retryCount, bankedNewAudio: false)
        retryCount = BackfillJobRunner.failedAttemptRetryCount(priorRetryCount: retryCount, bankedNewAudio: false)
        #expect(retryCount == 2, "two barren attempts: one away from the cap")
        retryCount = BackfillJobRunner.failedAttemptRetryCount(priorRetryCount: retryCount, bankedNewAudio: true)
        #expect(retryCount == 0, "the productive attempt ended the run")
        retryCount = BackfillJobRunner.failedAttemptRetryCount(priorRetryCount: retryCount, bankedNewAudio: false)
        #expect(retryCount == 1, "and the count starts again from there")
        #expect(retryCount < AdmissionController.maxRetries, "under the flat rule this job was already dead")
    }

    /// The vacuity guard for every loop above. With `maxRetries` at 1 or 0 the
    /// "at every point in the budget" tests would assert about nothing, and
    /// "three consecutive" would be a different claim.
    @Test("vacuity: the budget is wide enough for these claims to mean something")
    func theBudgetIsWideEnoughToTest() {
        #expect(AdmissionController.maxRetries >= 3)
    }
}

// MARK: - The rule on the typed store arm

@Suite("playhead-ronl: the typed store arm takes the same rule, behind H-R3-2's short-circuit")
struct StoreFailureRetryChargeRuleTests {

    /// H-R3-2, unchanged. A permanent store error will reproduce byte for byte
    /// on the same inputs against the same schema, so the row is short-circuited
    /// to the cap and the C-B gate skips it on the next run.
    @Test("a PERMANENT store error goes straight to the cap")
    func permanentGoesToTheCap() {
        for prior in 0...AdmissionController.maxRetries {
            #expect(
                BackfillJobRunner.storeFailureRetryCount(
                    priorRetryCount: prior,
                    isPermanent: true,
                    bankedNewAudio: false
                ) == AdmissionController.maxRetries
            )
        }
    }

    /// PERMANENCE OUTRANKS PROGRESS, and the order is the argument.
    /// `isPermanent` asks whether replaying these inputs reproduces this
    /// failure. Banked audio does not change that answer, so it does not get a
    /// vote — a malformed cohort will still be malformed on the fourth attempt.
    ///
    /// This is the direction a naive "make it cursor-aware" edit gets wrong: it
    /// would forgive a schema validator failure for a pass that happened to bank
    /// a window first, and the row would then burn the whole budget proving the
    /// validator still says no.
    @Test("a PERMANENT store error goes to the cap even when the attempt banked audio")
    func permanentOutranksProgress() {
        #expect(
            BackfillJobRunner.storeFailureRetryCount(
                priorRetryCount: 0,
                isPermanent: true,
                bankedNewAudio: true
            ) == AdmissionController.maxRetries
        )
    }

    /// The recoverable half is now the SHARED rule, asserted as an equality with
    /// it rather than as a re-statement of its arithmetic — a re-statement is a
    /// second ruler, which is the defect this bead exists to remove.
    @Test("the RECOVERABLE half is the shared rule, not a copy of it")
    func recoverableIsTheSharedRule() {
        for prior in 0...AdmissionController.maxRetries {
            for banked in [true, false] {
                #expect(
                    BackfillJobRunner.storeFailureRetryCount(
                        priorRetryCount: prior,
                        isPermanent: false,
                        bankedNewAudio: banked
                    ) == BackfillJobRunner.failedAttemptRetryCount(
                        priorRetryCount: prior,
                        bankedNewAudio: banked
                    )
                )
            }
        }
    }

    /// WHAT THE RECOVERABLE HALF ACTUALLY IS — read off the production
    /// classifier rather than off a table this test wrote, so the answer cannot
    /// drift from `isPermanent`'s switch.
    ///
    /// The bead asked whether a typed `AnalysisStoreError` is retryable at all:
    /// if the non-permanent half is genuinely transient — a locked database, a
    /// busy writer — then charging it may be wrong rather than merely flat. The
    /// answer is that `isPermanent` splits PROVABLY REPRODUCIBLE from EVERYTHING
    /// ELSE, and "everything else" is not "transient": `openFailed` and
    /// `migrationFailed` on a corrupt database fail identically forever, while
    /// `staleAdWindowRevision` and `duplicateJobId` really are moments. Nothing
    /// at this layer can tell them apart, so the recoverable half is UNCLASSIFIED
    /// AS TO DURABILITY — exactly like the generic arm's throw — and is charged
    /// rather than excused. `FMDaemonRefusal` preserves the count for conditions
    /// whose own API says "try again later"; no such statement exists here.
    @Test("every recoverable case takes the shared rule; every permanent one takes the cap")
    func theProductionClassificationDecidesWhichClause() {
        let cases: [AnalysisStoreError] = [
            .openFailed(code: 1, message: "unable to open database file"),
            .migrationFailed("v51 step failed"),
            .queryFailed("database is locked"),
            .insertFailed("database is locked"),
            .insertFailed("payloadTooLarge: 999"),
            .notFound,
            .duplicateJobId("fm-ronl"),
            .invalidRow(column: 0),
            .invalidEvidenceEvent("x"),
            .invalidScanCohortJSON("not-json"),
            .invalidStateTransition(jobId: "fm-ronl", fromStatus: "failed", toStatus: "running"),
            .evidenceEventBodyMismatch(id: "x"),
            .staleAdWindowRevision(id: "window"),
            .encodingFailure("x"),
        ]
        var recoverableCount = 0
        var permanentCount = 0
        for error in cases {
            let isPermanent = BackfillJobRunner.isPermanentForTesting(error)
            let charged = BackfillJobRunner.storeFailureRetryCount(
                priorRetryCount: 1,
                isPermanent: isPermanent,
                bankedNewAudio: true
            )
            if isPermanent {
                permanentCount += 1
                #expect(charged == AdmissionController.maxRetries, "\(error) is permanent")
            } else {
                recoverableCount += 1
                #expect(charged == 0, "\(error) is recoverable and this attempt banked audio")
            }
        }
        // Vacuity: both clauses have to be exercised, or one of the expectations
        // above is asserting about an empty set.
        #expect(recoverableCount >= 7, "saw \(recoverableCount) recoverable cases")
        #expect(permanentCount >= 5, "saw \(permanentCount) permanent cases")
    }
}

// MARK: - The rule, wired

/// End-to-end, against a real store and a real drain loop.
///
/// The seam is the two PROLOGUE round trips, which is what makes the pair
/// comparable: both throw the SAME error out of the SAME catch arm, and the only
/// thing that differs is whether the coarse pass had already banked windows when
/// it happened.
///
///   * `coarseSchemaTokenCountFailure` throws inside `promptBudget()`, before
///     `planPassA` plans a single window — nothing durable, nothing banked.
///   * `refinementSchemaTokenCountFailure` throws inside `planAdaptiveZoom`,
///     which `runJob` reaches only after `coarsePassA` returned and the digest
///     persisted every window — so playhead-26od's mid-flight checkpoint is
///     already in the row.
///
/// Both are XPC round trips to the same daemon that do no generation at all, so
/// the field error `TestFMRuntimeFailure.modelManagerInferenceError` reproduces
/// (the 2026-08-14 pull's asset A9F6DF05) can genuinely arrive at either.
@Suite("playhead-ronl: the generic arm, end to end")
struct UnclassifiedThrowRetryChargeWireInTests {

    private static let segmentSeconds = 30.0

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: nil
        )
    }

    private func makeInputs(
        assetId: String,
        lineCount: Int
    ) -> BackfillJobRunner.AssetInputs {
        let transcriptVersion = "tx-ronl-v1"
        var lines: [(Double, Double, String)] = []
        for idx in 0..<lineCount {
            let start = Double(idx) * Self.segmentSeconds
            lines.append(
                (
                    start,
                    start + Self.segmentSeconds,
                    "Editorial line \(idx) about the topic of the day."
                )
            )
        }
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: lines
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-ronl",
            segments: segments,
            evidenceCatalog: EvidenceCatalogBuilder.build(
                atoms: segments.flatMap(\.atoms),
                analysisAssetId: assetId,
                transcriptVersion: transcriptVersion
            ),
            transcriptVersion: transcriptVersion,
            plannerContext: CoveragePlannerContext(
                observedEpisodeCount: 0,
                stableRecall: false,
                isFirstEpisodeAfterCohortInvalidation: false,
                recallDegrading: false,
                sponsorDriftDetected: false,
                auditMissDetected: false,
                episodesSinceLastFullRescan: 0,
                periodicFullRescanIntervalEpisodes: 10
            )
        )
    }

    private func makeRunner(
        store: AnalysisStore,
        runtime: FoundationModelClassifier.Runtime
    ) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON()
        )
    }

    /// Finds the coverage-lane row for this asset without guessing at it — the
    /// job id is a pure function of (asset, phase, offset).
    private func coverageJob(
        _ store: AnalysisStore,
        assetId: String
    ) async throws -> BackfillJob? {
        let jobId = BackfillJobRunner.makeJobId(
            analysisAssetId: assetId,
            phase: .fullEpisodeScan,
            offset: 0
        )
        return try await store.fetchBackfillJob(byId: jobId)
    }

    // MARK: THE FIX

    /// THE HEADLINE. A pass that banked durable coarse windows and then threw
    /// something unclassifiable spends NO retry, and the row stays a candidate.
    ///
    /// Before this bead the same run wrote `retryCount = 1`, and three such runs
    /// — on an episode whose coverage was climbing the whole time — took the
    /// asset out of both coarse candidate queries and out of the ad-scan redrive
    /// permanently, because all three read `retryCount < maxRetries`.
    ///
    /// The cursor assertion is the load-bearing one and is not a number this
    /// test picked: it is the checkpoint's own value, and it is what makes the
    /// forgiveness bounded — the next attempt starts strictly further along.
    @Test("a throw AFTER the pass banked audio spends NO retry")
    func unclassifiedThrowSpendsNoRetryWhenTheAttemptBanked() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-ronl-banked"
        try await store.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId, lineCount: 40)
        let fmRuntime = TestFMRuntime(
            refinementSchemaTokenCountFailure: .modelManagerInferenceError,
            tokenCountRule: { $0.count }
        )

        _ = try await makeRunner(store: store, runtime: fmRuntime.runtime)
            .runPendingBackfill(for: inputs)

        let row = try #require(await coverageJob(store, assetId: assetId))
        #expect(row.status == .failed, "the DISPOSITION is unchanged — this is not a deferral")
        #expect(row.retryCount == 0, "the attempt banked audio, so the budget is untouched")

        // The witness, read from the row rather than from the box: this attempt
        // really did make new coverage durable, which is what the forgiveness is
        // paid for with.
        let cursor = try #require(row.progressCursor?.lastProcessedUpperBoundSec)
        #expect(cursor.rawValue > 0, "the mid-flight checkpoint advanced the row")
        let durableRows = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
            .filter { $0.scanPass == "passA" && $0.status == .success }
        #expect(!durableRows.isEmpty, "vacuity: the pass really did bank windows")
        #expect(
            durableRows.map(\.windowEndTime).max().map { $0 >= cursor.rawValue } == true,
            "the cursor never claims audio no row covers"
        )

        // And it stays reachable: `fetchAssetIdsWithResumableBackfillJobs` is the
        // coarse phase's first candidate query and the ad-scan redrive's only
        // source. This is the whole point of not charging.
        let resumable = try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 50)
        #expect(resumable.contains(assetId))
    }

    // MARK: NOT A RECLASSIFICATION

    /// The mirror, and the proof that nothing was excused. The SAME error from
    /// the SAME catch arm, thrown before the pass banked anything, is charged
    /// exactly as it was before this bead — and the row is `failed` with the
    /// `unclassifiedModelError-` token, not `deferred` with the count preserved,
    /// which is what a reclassification into `FMDaemonRefusal` would have made
    /// it.
    @Test("a throw BEFORE the pass banks anything is charged, and is still a FAILURE")
    func unclassifiedThrowIsChargedWhenItBanksNothing() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-ronl-barren"
        try await store.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId, lineCount: 40)
        let fmRuntime = TestFMRuntime(
            coarseSchemaTokenCountFailure: .modelManagerInferenceError,
            tokenCountRule: { $0.count }
        )

        let result = try await makeRunner(store: store, runtime: fmRuntime.runtime)
            .runPendingBackfill(for: inputs)

        let row = try #require(await coverageJob(store, assetId: assetId))
        #expect(row.retryCount == 1, "nothing was banked, so the budget is charged")
        #expect(row.status == .failed, "NOT deferred — the error is not a daemon refusal")
        #expect(result.deferredJobIds.isEmpty, "and the drain does not report it as deferred")
        let reason = try #require(row.deferReason)
        #expect(
            reason.hasPrefix("\(UnclassifiedModelFailure.causePrefix)-"),
            "the cause token is unchanged: \(reason)"
        )
        #expect(row.progressCursor?.lastProcessedUpperBoundSec == nil, "vacuity: nothing was banked")
    }

    /// THE BOUND, end to end. Three prologue throws retire the job, exactly as
    /// they did before, and the row leaves the coarse phase's candidate query.
    ///
    /// Read together with the headline test this is the bead's whole claim: the
    /// budget is spent by attempts that did not advance coverage, and an attempt
    /// that advances nothing still spends it.
    @Test("three barren attempts still retire the job")
    func threeBarrenAttemptsRetireTheJob() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-ronl-retire"
        try await store.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId, lineCount: 40)

        var observed: [Int] = []
        for _ in 0..<AdmissionController.maxRetries {
            let fmRuntime = TestFMRuntime(
                coarseSchemaTokenCountFailure: .modelManagerInferenceError,
                tokenCountRule: { $0.count }
            )
            _ = try await makeRunner(store: store, runtime: fmRuntime.runtime)
                .runPendingBackfill(for: inputs)
            let row = try #require(await coverageJob(store, assetId: assetId))
            observed.append(row.retryCount)
        }

        #expect(observed == Array(1...AdmissionController.maxRetries), "saw \(observed)")
        let resumable = try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 50)
        #expect(
            !resumable.contains(assetId),
            "a job that never advanced anything must leave the candidate set"
        )
    }

    /// THE TYPED STORE ARM, end to end, on the half of it that a real store can
    /// be made to produce.
    ///
    /// A malformed `scanCohortJSON` makes `insertSemanticScanResult` throw
    /// `invalidScanCohortJSON`, which `isPermanent` classifies as PERMANENT —
    /// replaying the same rows against the same validator hits the same
    /// rejection. H-R3-2's short-circuit is unchanged by this bead, and this is
    /// where that claim is checked rather than argued: the row goes straight to
    /// the cap on ONE attempt, not to `1`.
    ///
    /// LIMIT, STATED RATHER THAN PAPERED OVER. The RECOVERABLE half of this arm
    /// has no end-to-end trigger on this harness, and the cause is structural:
    /// `BackfillJobRunner` holds a concrete `AnalysisStore`, not a protocol, so
    /// no store call can be made to throw on demand. Every store error reachable
    /// from the drain's own writes is either permanent (`invalidScanCohortJSON`,
    /// `invalidEvidenceEvent`, `evidenceEventBodyMismatch`, `payloadTooLarge`,
    /// an impossible window geometry) or is `invalidStateTransition`, which the
    /// arm above logs and `continue`s past before any charge is computed. The
    /// FK on `backfill_jobs.analysisAssetId` means an orphan asset cannot even
    /// enqueue a row, and `insertSemanticScanResult` is `INSERT OR REPLACE`, so
    /// neither of the two obvious routes to a plain SQLite failure exists. So
    /// the recoverable branch is covered by the pure rule and by nothing at the
    /// call site; closing that needs a store-fault seam, which is the same
    /// architecture change `aFailedCursorWriteIsRetried` declines two functions
    /// over.
    @Test("a PERMANENT store failure still goes straight to the cap, end to end")
    func permanentStoreFailureStillGoesStraightToTheCap() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-ronl-permanent"
        try await store.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId, lineCount: 40)
        let fmRuntime = TestFMRuntime(tokenCountRule: { $0.count })
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            // The seam: every `insertSemanticScanResult` rejects this cohort.
            scanCohortJSON: "not-json"
        )

        _ = try await runner.runPendingBackfill(for: inputs)

        let row = try #require(await coverageJob(store, assetId: assetId))
        #expect(row.status == .failed)
        #expect(
            row.retryCount == AdmissionController.maxRetries,
            "a permanent store error is retired on the FIRST attempt, not after three"
        )
        let resumable = try await store.fetchAssetIdsWithResumableBackfillJobs(limit: 50)
        #expect(!resumable.contains(assetId), "and it leaves the candidate set immediately")
    }

    /// A NON-CONVERGING JOB STILL TERMINATES — the constraint the flat rule was
    /// defending, checked against the fix rather than argued.
    ///
    /// Every attempt here banks new audio and then throws, so every attempt is
    /// forgiven. That cannot loop forever, and the reason is that a reset is only
    /// ever issued together with a cursor that is ALREADY IN THE ROW: each
    /// forgiven attempt leaves `narrowedForResume` strictly less audio to plan.
    /// Once the cursor saturates, `runJob`'s empty-segments short-circuit returns
    /// BEFORE any FM round trip can throw — so the row leaves this arm entirely
    /// and is disposed of by the coverage terminal.
    ///
    /// The loop is bounded by the number of coarse windows in the episode, which
    /// is what the cap below stands for. It is asserted as a strict inequality
    /// so a regression that stopped advancing the cursor would exhaust it.
    @Test("a job that banks on EVERY attempt still terminates — the cursor saturates")
    func aForgivenJobStillTerminates() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-ronl-bounded"
        try await store.insertAsset(makeAsset(id: assetId))
        let inputs = makeInputs(assetId: assetId, lineCount: 12)

        // Generous: the episode has 12 segments, so the coarse plan list cannot
        // support more than 12 strict cursor advances however it is windowed.
        let attemptCap = 40
        var attempts = 0
        var cursors: [Double] = []
        var leftTheArm = false

        while attempts < attemptCap {
            attempts += 1
            let fmRuntime = TestFMRuntime(
                refinementSchemaTokenCountFailure: .modelManagerInferenceError,
                tokenCountRule: { $0.count }
            )
            _ = try await makeRunner(store: store, runtime: fmRuntime.runtime)
                .runPendingBackfill(for: inputs)
            guard let row = try await coverageJob(store, assetId: assetId) else { break }
            cursors.append(row.progressCursor?.lastProcessedUpperBoundSec?.rawValue ?? 0)
            // The arm is left either by the row retiring (the budget was charged
            // to the cap) or by the row reaching a terminal the coverage decision
            // owns, which is what the saturating case produces.
            if row.retryCount >= AdmissionController.maxRetries || row.status == .complete {
                leftTheArm = true
                break
            }
            // A row still under the budget must have paid for it: either the
            // cursor advanced this attempt, or the count did.
            if cursors.count >= 2, cursors[cursors.count - 1] <= cursors[cursors.count - 2] {
                #expect(
                    row.retryCount > 0,
                    "attempt \(attempts) neither advanced the cursor nor spent budget — that is the unbounded case"
                )
                leftTheArm = true
                break
            }
        }

        #expect(leftTheArm, "the job never left the generic-failure arm in \(attemptCap) attempts")
        #expect(attempts < attemptCap, "took \(attempts) attempts; cursors=\(cursors)")
        // Vacuity: the loop has to have actually run the productive path, or
        // "still terminates" is a claim about a job that never started.
        #expect(cursors.contains { $0 > 0 }, "no attempt ever banked anything: \(cursors)")
    }
}
