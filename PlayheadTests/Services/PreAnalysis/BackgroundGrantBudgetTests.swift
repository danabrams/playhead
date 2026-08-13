// BackgroundGrantBudgetTests.swift
// playhead-lmrx: a ~295 s background grant must be spent on work that can
// checkpoint inside it, an interrupted job must survive the expiry resumable,
// and what the grant achieved must be readable afterwards.
//
// Every number here is anchored to `scratchpad/db-new-t2h/analysis.sqlite`
// (2026-08-06 device pull); the derivation of each lives in
// `BackgroundGrantBudget`'s doc comments. Four defects are under test:
//
//   1. Both work drivers were bounded by `25 * 60` s — 5.07x the p90 of the 203
//      expired backfill grants in that pull.
//   2. The expiration path wrote outcome/cause/expiration and nothing else, so
//      `jobsSeen`/`jobsAdmitted`/`jobsCompleted` are NULL on ALL 203 expired
//      rows and the ledger cannot say what 80 % of grants achieved.
//   3. A BGTask expiry cancelled the handler's own task but never told
//      `AnalysisWorkScheduler` the grant was over, so the analysis job the grant
//      dispatched ran on unaware into process suspension — no checkpoint, no
//      requeue, a leased `running` row for the reaper.
//   4. And the fix for (3) exposed a fourth: the cancel arm charges an attempt
//      and supersedes at five. With 80 % of grants expiring, that would abandon
//      every long episode permanently — by the OS doing exactly what the OS
//      does. The pull already carries two rows at
//      `maxAttemptsReached:cancelMidRun`.

import BackgroundTasks
import Foundation
import Testing

@testable import Playhead

// MARK: - The budget arithmetic

@Suite("playhead-lmrx: the background grant budget")
struct BackgroundGrantBudgetArithmeticTests {

    private func budget(
        grant: Duration = .seconds(255),
        reserve: Duration = .seconds(36),
        floor: Duration = .seconds(60),
        drainFloor: Duration = .zero,
        grace: Duration = .seconds(3)
    ) -> BackgroundGrantBudget {
        BackgroundGrantBudget(
            designGrant: grant,
            teardownReserve: reserve,
            minimumCheckpointBudget: floor,
            minimumDrainCheckpointBudget: drainFloor,
            expirationSettleGrace: grace,
            provenance: .measured(
                grantObservations: 132,
                teardownObservations: 30,
                checkpointObservations: 142
            )
        )
    }

    @Test("workBudget is the grant minus the teardown reserve")
    func workBudgetSubtractsReserve() {
        #expect(budget().workBudget == .seconds(219))
    }

    @Test("workBudget clamps at zero rather than going negative")
    func workBudgetClampsAtZero() {
        // A reserve larger than the grant is a coherent (useless) budget. A
        // NEGATIVE `Duration` here would make `workDeadline(from:)` return an
        // instant in the PAST, which every caller reads as "already expired" —
        // the right outcome for the wrong reason, and one that would mask a
        // mis-specified budget instead of surfacing it.
        #expect(budget(grant: .seconds(10), reserve: .seconds(60), floor: .seconds(1))
            .workBudget == .zero)
    }

    @Test("workDeadline is measured from the grant's start, not from the call")
    func workDeadlineIsAnchoredToGrantStart() {
        let shipped = BackgroundGrantBudget.backfillProcessing
        let grantStart = ContinuousClock.now
        #expect(grantStart.duration(to: shipped.workDeadline(from: grantStart)) == shipped.workBudget)
    }

    // The two `canReachCheckpoint` tests that stood here went with the method
    // (playhead-lmrx review round 7): it had no production caller, and the gate
    // it described is spelled inline in `AnalysisWorkScheduler.drainEligible`.
    // See `BackgroundGrantBudget.swift` for the argument and for why the `>=`
    // boundary LX08 pinned is not observable at the surviving gate.

    @Test("the teardown reserve is spent DOWN by teardown, not re-granted per step")
    func teardownReserveIsOneBudgetForTheWholeTeardown() {
        // The reserve pays for the whole teardown — the ledger write AND the
        // wait for the interrupted job's resume point. The first draft handed
        // the entire reserve to the wait alone, which is the handler's
        // 25-minute deadline defect at a smaller scale: a step that may spend
        // the whole of a shared budget is not bounded by it.
        let subject = budget(reserve: .seconds(36))
        let start = ContinuousClock.now
        #expect(subject.remainingTeardownReserve(since: start, now: start) == .seconds(36))
        #expect(subject.remainingTeardownReserve(since: start, now: start + .seconds(10))
            == .seconds(26))
        #expect(subject.remainingTeardownReserve(since: start, now: start + .seconds(36))
            == .zero)
        #expect(subject.remainingTeardownReserve(since: start, now: start + .seconds(90))
            == .zero,
                "an overrun clamps at zero rather than going negative")
    }

    @Test("the post-reclaim wait is bounded by the OS grace, not by the teardown reserve")
    func expirationSettleBudgetIsBoundedByTheGrace() {
        // playhead-lmrx review round 2. `teardownReserve` is wall clock carved
        // OUT of `designGrant`. Once the OS has fired `expirationHandler` there
        // is no grant left for it to be carved from, so spending it there asks
        // the diagnostic question backwards: what would
        // `remainingTeardownReserve` read if the window had already run to its
        // full 295 s limit before being reclaimed? The same 36 s it reads for a
        // window reclaimed at t+5 s. A bound that survives the thing it bounds
        // not existing is not a bound — and the consequence is concrete, since
        // iOS terminates an app that does not complete its task promptly after
        // expiration and penalises its future scheduling.
        let subject = budget(reserve: .seconds(36), grace: .seconds(3))
        let start = ContinuousClock.now
        #expect(subject.expirationSettleBudget(since: start, now: start) == .seconds(3),
                "the grace is the ceiling even with the whole reserve nominally unspent")
        #expect(subject.expirationSettleBudget(since: start, now: start + .seconds(1))
            == .seconds(3),
                """
                and it stays the ceiling while the reserve has more than the \
                grace left — a second of teardown does not buy a second back \
                from a bound that was never about the reserve.
                """)
        #expect(subject.expirationSettleBudget(since: start, now: start + .seconds(34))
            == .seconds(2),
                "the reserve takes over once its remainder falls below the grace")
        #expect(subject.expirationSettleBudget(since: start, now: start + .seconds(36)) == .zero)
        #expect(subject.expirationSettleBudget(since: start, now: start + .seconds(90)) == .zero,
                "an overrun clamps at zero rather than going negative")

        // The other operand still binds when it is the smaller one: a teardown
        // that has already spent most of a SHORT reserve gets what is left of
        // it, not the full grace.
        let tight = budget(reserve: .seconds(2), grace: .seconds(3))
        #expect(tight.expirationSettleBudget(since: start, now: start) == .seconds(2),
                "the min is a min in both directions, or one of the two operands is decoration")
    }

    @Test("the shipped grace is far under the reserve, and under the OS grace it is bounded by")
    func shippedGraceIsSmall() {
        // The number itself, pinned so a "tidy-up" that unifies it with the
        // teardown reserve has to argue with a test. 3 s sits inside the ~5 s
        // this repo already records as iOS's tightest suspension grace
        // (`BGTaskScheduler.pendingTaskRequestsTimeout`'s own justification),
        // with room left to emit telemetry and call `setTaskCompleted`.
        for shipped in [
            BackgroundGrantBudget.backfillProcessing,
            BackgroundGrantBudget.backfillProcessingCharged,
            BackgroundGrantBudget.preAnalysisRecovery,
        ] {
            #expect(shipped.expirationSettleGrace <= .seconds(5),
                    "a post-reclaim wait longer than iOS's grace risks the process, not just the job")
            #expect(shipped.expirationSettleGrace < shipped.teardownReserve,
                    "the grace is not the reserve; collapsing them is the defect this pins")
            #expect(shipped.expirationSettleGrace > .zero,
                    "and it is not zero either — a wait of nothing cannot bank a requeue")
        }
    }
}

// MARK: - The shipped constants, against the measurement they came from

@Suite("playhead-lmrx: the shipped budget is inside the measured grant")
struct BackgroundGrantBudgetMeasurementTests {

    /// p95 of `finishedAt - startedAt` over the 203 `background_task_runs` rows
    /// with `taskIdentifier = 'com.playhead.app.analysis.backfill'` and
    /// `outcome = 'expired'` in the 2026-08-06 pull. p50 294.0, p90 295.9;
    /// 296.0 is the point past which a "grant" is not evidence but a guess.
    private static let measuredGrantP95: Duration = .seconds(296)

    /// The largest end-to-end duration of a backfill run that did NO work
    /// (`no_eligible_work` + `deferred_capability`, n = 30). p50 1.4 s,
    /// p90 17.3 s, p95 19.4 s, max 36.0 s.
    private static let measuredTeardownMax: Duration = .seconds(36)

    /// p95 of the 142 `semantic_scan_results.latencyMs` values — one FM coarse
    /// window, the smallest artifact playhead-26od makes durable. p50 6.0 s,
    /// p90 49.0 s, p95 57.5 s.
    private static let measuredWindowP95: Duration = .seconds(58)

    /// Every budget that CLAIMS to be measured, and only those.
    private static var measuredBudgets: [(String, BackgroundGrantBudget)] {
        [
            ("backfillProcessing", .backfillProcessing),
            ("backfillProcessingCharged", .backfillProcessingCharged),
            ("preAnalysisRecovery", .preAnalysisRecovery),
        ].filter {
            if case .measured = $0.1.provenance { return true }
            return false
        }
    }

    @Test("a budget that claims to be measured fits the grant that was measured")
    func measuredBudgetsFitTheMeasurement() {
        // Driven off `provenance` rather than a hardcoded list, so a future
        // budget cannot be added without either fitting the measurement or
        // declaring itself an assumption. The specific implementation this
        // kills is the one that shipped — `ContinuousClock.now + .seconds(25 *
        // 60)`, 5.07x this bound — and any later inflation past what the OS was
        // observed to give, which is the same defect with a smaller multiplier.
        #expect(!Self.measuredBudgets.isEmpty,
                "if nothing claims to be measured, this suite is vacuous")
        for (name, budget) in Self.measuredBudgets {
            #expect(budget.designGrant <= Self.measuredGrantP95,
                    "\(name) designGrant \(budget.designGrant) exceeds the measured grant")
        }
    }

    @Test("a measured budget names the denominator each of its numbers came from")
    func measuredProvenanceNamesTheRightDenominators() {
        // THE STANDING DEFECT CLASS, one level up from the values. The first
        // draft carried a single `sampleSize: 203` — the number of expired
        // backfill rows. Ask the diagnostic question of it: what would 203 read
        // if nobody had derived the p05 of the full-length grants at all? The
        // same, because it is just how many rows carry that outcome. None of
        // the three shipped numbers has 203 as its denominator:
        //
        //   designGrant             p05 of the 132 expiries that reached 200 s
        //   teardownReserve         max of the 30 no-work runs
        //   minimumCheckpointBudget p95 of the 142 `latencyMs` values
        //
        // A provenance that stays true when its own values were never measured
        // is not provenance, and this bead is about a ledger that said things
        // it had no basis for.
        #expect(BackgroundGrantBudget.backfillProcessing.provenance
            == .measured(
                grantObservations: 132,
                teardownObservations: 30,
                checkpointObservations: 142
            ))
    }

    @Test("the charged sibling is declared an assumption, not a measurement")
    func chargedSiblingIsNotPassedOffAsMeasured() {
        // The 2026-08-06 pull has ZERO rows for
        // `com.playhead.app.analysis.backfill.charged`. Spending the plain
        // identifier's 219 s there would be the wrong-population error this type
        // exists to stop, and would risk surrendering most of an overnight
        // charger-class grant. It must stay BOTH labelled `.assumed` and
        // materially different from the measured sibling — a future edit that
        // "tidies" them into one constant fails here.
        #expect(BackgroundGrantBudget.backfillProcessingCharged.provenance == .assumed)
        #expect(BackgroundGrantBudget.backfillProcessingCharged.designGrant
            > BackgroundGrantBudget.backfillProcessing.designGrant,
                "the charger class is expected to grant LONGER windows, not shorter")
    }

    @Test("the teardown reserve covers the worst handler teardown actually observed")
    func teardownReserveCoversTheObservedMax() {
        // A reserve smaller than the worst observed teardown reproduces this
        // bead's defect from the other end: a handler reclaimed mid-teardown
        // leaves an `expired` row with no counters. The reserve is also what
        // bounds the expiry path's wait for the in-flight job to settle, so
        // shrinking it silently shortens that wait too.
        for budget in [BackgroundGrantBudget.backfillProcessing,
                       .backfillProcessingCharged,
                       .preAnalysisRecovery] {
            #expect(budget.teardownReserve >= Self.measuredTeardownMax)
        }
    }

    @Test("the checkpoint floor is large enough to bank one durable window")
    func checkpointFloorCoversOneWindow() {
        #expect(BackgroundGrantBudget.backfillProcessing.minimumCheckpointBudget
            >= Self.measuredWindowP95)
    }

    @Test("the budget still leaves room to start work")
    func workBudgetExceedsTheFloor() {
        // Bounding the grant is only half the contract. A budget shrunk below
        // the checkpoint floor would satisfy every assertion above and dispatch
        // NOTHING, ever — trading 199-of-203 barren windows for 203-of-203.
        for budget in [BackgroundGrantBudget.backfillProcessing,
                       .backfillProcessingCharged,
                       .preAnalysisRecovery] {
            #expect(budget.workBudget > budget.minimumCheckpointBudget)
        }
    }
}

// MARK: - The counters box

@Suite("playhead-lmrx: background grant counters")
struct BackgroundGrantCountersTests {

    @Test("an untouched box reports nil, not zero")
    func untouchedBoxIsNil() {
        // NULL and 0 are different findings — "the handler never got far enough
        // to count" versus "the handler counted zero" — and `finishRun` binds
        // through `COALESCE(?, col)`, so writing a fabricated 0 would also
        // overwrite a counter an earlier path legitimately measured.
        let counters = BackgroundGrantCounters()
        #expect(counters.snapshot.jobsSeen == nil)
        #expect(counters.snapshot.jobsCompleted == nil)
    }

    @Test("noteBaseline publishes only the denominator")
    func baselineDoesNotClaimCompletion() {
        let counters = BackgroundGrantCounters()
        counters.noteBaseline(pending: 7)
        #expect(counters.snapshot.jobsSeen == 7)
        #expect(counters.snapshot.jobsCompleted == nil,
                "a baseline is a denominator; it says nothing about completion")
    }

    @Test("noteCompleted records zero as a measurement, not as absence")
    func completedZeroIsRecorded() {
        let counters = BackgroundGrantCounters()
        counters.noteBaseline(pending: 3)
        counters.noteCompleted(0)
        #expect(counters.snapshot.jobsCompleted == 0,
                "0 completed is the bead's headline finding; it must not read as NULL")
    }

    @Test("the baseline id box distinguishes 'never read' from 'empty queue'")
    func baselineIdsDistinguishUnreadFromEmpty() {
        // The expiry path only computes completions when it has a baseline. If
        // "never read" and "the queue was empty" collapsed, a window that
        // expired before reading anything would report `jobsCompleted = 0`,
        // which is a claim it has no basis for.
        let box = BackgroundGrantBaselineIds()
        #expect(box.value == nil)
        box.store([])
        #expect(box.value == [])
    }
}

// MARK: - jobsCompleted counts the right population

@Suite("playhead-lmrx: jobsCompleted is a measurement, not a queue delta")
struct GrantCompletionPopulationTests {

    private func makeScheduler(store: AnalysisStore) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(),
            downloadManager: StubDownloadProvider(),
            batteryProvider: StubBatteryProvider(),
            transportStatusProvider: StubTransportStatusProvider()
        )
    }

    @discardableResult
    private func insertJob(_ store: AnalysisStore, id: String) async throws -> AnalysisJob {
        let job = makeAnalysisJob(
            jobId: id,
            jobType: "preAnalysis",
            episodeId: "ep-\(id)",
            analysisAssetId: nil,
            workKey: AnalysisJob.computeWorkKey(
                fingerprint: "fp-\(id)",
                analysisVersion: PreAnalysisConfig.analysisVersion,
                jobType: "preAnalysis"
            ),
            sourceFingerprint: "fp-\(id)",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)
        return job
    }

    @Test("a FAILED job is not credited as completed", .timeLimit(.minutes(1)))
    func failureIsNotCompletion() async throws {
        // THE STANDING DEFECT CLASS, caught in this bead's own first draft. The
        // original implementation was `baselinePending - residualPending`: how
        // much the global queue SHRANK. Ask the diagnostic question — what would
        // that read if this grant completed nothing? Greater than zero, whenever
        // a row leaves the pending set for any other reason. And failure is the
        // COMMONER exit: the 2026-08-06 pull holds 112 `failed` work-journal
        // events against 45 `finalized`.
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)
        try await insertJob(store, id: "done")
        try await insertJob(store, id: "broke")

        let baseline = await scheduler.pendingJobIdsForLedger()
        #expect(baseline == ["done", "broke"])

        try await store.updateJobState(jobId: "done", state: "complete")
        try await store.updateJobState(jobId: "broke", state: "failed")

        let completed = await scheduler.completedJobIdsForLedger(among: baseline)
        #expect(completed == ["done"],
                """
                Only terminal SUCCESS counts. A count delta would say 2 here — \
                both rows left the pending set — and would report a window that \
                broke two episodes as one that finished two.
                """)
    }

    @Test("work minted during the window cannot mask a completion", .timeLimit(.minutes(1)))
    func midWindowEnqueueDoesNotMaskCompletion() async throws {
        // The other direction, and it is not hypothetical: the drain MINTS work
        // while it runs — a tier-advance commits `insertNextJob` inside
        // `processJob`. Finish one, mint one, and a count delta reads zero for a
        // window that did real work. Counting over the baseline POPULATION is
        // immune by construction.
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)
        try await insertJob(store, id: "first")

        let baseline = await scheduler.pendingJobIdsForLedger()
        #expect(baseline == ["first"])

        try await store.updateJobState(jobId: "first", state: "complete")
        try await insertJob(store, id: "minted-mid-window")

        #expect(await scheduler.pendingJobCountForLedger() == 1,
                "precondition: the queue is the same DEPTH it started at")
        let completed = await scheduler.completedJobIdsForLedger(among: baseline)
        #expect(completed == ["first"],
                "a count delta would read 0 here, for a window that completed a job")
    }

    @Test("a graceful coverage give-up IS counted, and that is deliberate",
          .timeLimit(.minutes(1)))
    func gracefulGiveUpIsCounted() async throws {
        // The other edge of the predicate, pinned so the choice is a decision
        // rather than an accident. `coverageInsufficient`'s no-progress and
        // retry-exhaustion arms both terminate `state = 'complete'` and both
        // emit `work_journal.finalized` — a job that ran end-to-end under our
        // lease, will never requeue, and did not fail.
        //
        // The narrower reading ("reached its coverage TARGET") is a different
        // question with its own instrument in the coverage program. Splitting it
        // out HERE would give one ledger two definitions of done, which is worse
        // than a permissive one. If that call is ever revisited, this test is
        // where it is written down.
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)
        try await insertJob(store, id: "gave-up")

        let baseline = await scheduler.pendingJobIdsForLedger()
        // Stamped with the arm's OWN constant, not a hand-written string, so
        // this reads the writer rather than a copy of it: if
        // `coverageInsufficient.noProgress` ever stops terminating `complete`,
        // that constant moves and this test moves with it.
        try await store.updateJobState(
            jobId: "gave-up",
            state: "complete",
            lastErrorCode: AnalysisWorkScheduler.noProgressTerminalErrorCode
        )
        let stamped = try #require(try await store.fetchJob(byId: "gave-up"))
        #expect(stamped.lastErrorCode == AnalysisWorkScheduler.noProgressTerminalErrorCode,
                "precondition: the row carries the give-up arm's own terminal code")

        #expect(await scheduler.completedJobIdsForLedger(among: baseline) == ["gave-up"],
                """
                `complete` is this codebase's terminal-success classification, \
                give-ups included. It is the same set `work_journal` calls \
                finalized, and the ledger must not disagree with the journal.
                """)
    }

    @Test("an empty baseline completes nothing", .timeLimit(.minutes(1)))
    func emptyBaselineCompletesNothing() async throws {
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)
        try await insertJob(store, id: "not-in-baseline")
        try await store.updateJobState(jobId: "not-in-baseline", state: "complete")
        #expect(await scheduler.completedJobIdsForLedger(among: []).isEmpty,
                "a job the window never saw is not this window's achievement")
    }
}

// MARK: - The drain start-gate

@Suite("playhead-lmrx: drainEligible will not start a pass that cannot checkpoint")
struct DrainEligibleStartGateTests {

    /// Always throws, so one dispatch drives a pre-stamped high-`attemptCount`
    /// job to a terminal state — a deterministic fixed point for the drain.
    private final class FailingDecodeStub: AnalysisAudioProviding, @unchecked Sendable {
        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            throw AnalysisAudioError.decodingFailed("Operation Interrupted")
        }
    }

    private func makeScheduler(
        store: AnalysisStore,
        downloads: StubDownloadProvider
    ) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: FailingDecodeStub(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(
                snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
            ),
            downloadManager: downloads,
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider()
        )
    }

    @discardableResult
    private func insertComputeOnlyJob(
        store: AnalysisStore,
        downloads: StubDownloadProvider,
        jobId: String,
        episodeId: String
    ) async throws -> AnalysisJob {
        downloads.cachedURLs[episodeId] = URL(fileURLWithPath: "/tmp/\(episodeId).m4a")
        let job = makeAnalysisJob(
            jobId: jobId,
            jobType: "preAnalysis",
            episodeId: episodeId,
            analysisAssetId: nil,
            workKey: AnalysisJob.computeWorkKey(
                fingerprint: "fp-\(jobId)",
                analysisVersion: PreAnalysisConfig.analysisVersion,
                jobType: "preAnalysis"
            ),
            sourceFingerprint: "fp-\(jobId)",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: 4
        )
        try await store.insertJob(job)
        return job
    }

    @Test("a pass is NOT started when the remaining grant is below the checkpoint floor",
          .timeLimit(.minutes(1)))
    func belowFloorDispatchesNothing() async throws {
        // The pre-fix loop condition was a bare `now < deadline`, which admits a
        // whole analysis job with a millisecond left. The floor is the cost of
        // the smallest DURABLE ARTIFACT (one FM coarse window, p95 57.5 s of the
        // 142 measured `latencyMs` values) — below it, a pass that starts cannot
        // bank anything before the window closes.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await insertComputeOnlyJob(
            store: store, downloads: downloads,
            jobId: "gate-below", episodeId: "ep-gate-below"
        )
        let scheduler = makeScheduler(store: store, downloads: downloads)

        await scheduler.drainEligible(
            deadline: ContinuousClock.now + .seconds(10),
            minimumCheckpointBudget: .seconds(60)
        )

        let stillQueued = try await store.fetchJobsByState("queued")
        #expect(
            stillQueued.contains { $0.jobId == "gate-below" },
            """
            With less remaining grant than one durable artifact costs, the drain \
            must start NOTHING. Pre-fix it dispatched, and the job was abandoned \
            unfinished when the OS reclaimed.
            """
        )
    }

    @Test("a pass IS started when the remaining grant clears the checkpoint floor",
          .timeLimit(.minutes(1)))
    func aboveFloorStillDispatches() async throws {
        // The complement, and the reason it is here: a start-gate that refuses
        // everything satisfies the test above and dispatches nothing, ever.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await insertComputeOnlyJob(
            store: store, downloads: downloads,
            jobId: "gate-above", episodeId: "ep-gate-above"
        )
        let scheduler = makeScheduler(store: store, downloads: downloads)

        await scheduler.drainEligible(
            deadline: ContinuousClock.now + .seconds(600),
            minimumCheckpointBudget: .seconds(60)
        )

        let stillQueued = try await store.fetchJobsByState("queued")
        #expect(
            !stillQueued.contains { $0.jobId == "gate-above" },
            "with ample grant remaining the drain must still dispatch"
        )
    }

    @Test("the default floor is zero, so non-grant callers are unchanged",
          .timeLimit(.minutes(1)))
    func defaultFloorPreservesPreFixBehaviour() async throws {
        // The parameter defaults to `.zero` precisely so foreground drains and
        // the existing suites keep the pre-lmrx condition. If that default ever
        // became non-zero, every caller that omits it would silently acquire a
        // budget nobody measured for it.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await insertComputeOnlyJob(
            store: store, downloads: downloads,
            jobId: "gate-default", episodeId: "ep-gate-default"
        )
        let scheduler = makeScheduler(store: store, downloads: downloads)

        await scheduler.drainEligible(deadline: ContinuousClock.now + .seconds(10))

        let stillQueued = try await store.fetchJobsByState("queued")
        #expect(
            !stillQueued.contains { $0.jobId == "gate-default" },
            "with the default floor the drain must dispatch exactly as before"
        )
    }
}

// MARK: - An expired window is not an attempt

@Suite("playhead-lmrx: an expired window does not spend the job's retry budget")
struct ExpiredWindowAttemptAccountingTests {

    /// Hangs in `decode` until cancelled. Lets a test reach the scheduler's
    /// CancellationError-cleanup arm deterministically.
    private final class CancellableAudioStub: AnalysisAudioProviding, @unchecked Sendable {
        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            try await Task.sleep(for: .seconds(60))
            return []
        }
    }

    private func makeScheduler(
        store: AnalysisStore,
        downloads: StubDownloadProvider,
        clock: @escaping @Sendable () -> Date = { Date() }
    ) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: CancellableAudioStub(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(
                snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
            ),
            downloadManager: downloads,
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider(),
            clock: clock
        )
    }

    @discardableResult
    private func insertJob(
        store: AnalysisStore,
        downloads: StubDownloadProvider,
        jobId: String,
        attemptCount: Int
    ) async throws -> AnalysisJob {
        let episodeId = "ep-\(jobId)"
        downloads.cachedURLs[episodeId] = URL(fileURLWithPath: "/tmp/\(episodeId).m4a")
        let job = makeAnalysisJob(
            jobId: jobId,
            jobType: "preAnalysis",
            episodeId: episodeId,
            analysisAssetId: "asset-\(jobId)",
            workKey: "fp-\(jobId):1:preAnalysis",
            sourceFingerprint: "fp-\(jobId)",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: attemptCount
        )
        try await store.insertJob(job)
        return job
    }

    @Test("an expired background window leaves the job queued and spends no attempt",
          .timeLimit(.minutes(1)))
    func expiryDoesNotSpendAnAttempt() async throws {
        // THE RESUME POINT, at the layer that writes it. A grant ending is
        // evidence about the WINDOW, not about the job — so it requeues at a
        // flat floor with `attemptCount` untouched, exactly as playhead-ngev
        // already does for playback displacement.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let fixedNow = Date(timeIntervalSince1970: 1_800_000_000)
        try await insertJob(store: store, downloads: downloads, jobId: "expired-once", attemptCount: 2)
        let scheduler = makeScheduler(store: store, downloads: downloads, clock: { fixedNow })

        let processed = await scheduler.processNextDispatchableJobForTesting(
            cancelAfterRunnerStart: .taskExpired
        )
        #expect(processed)

        let after = try #require(try await store.fetchJob(byId: "expired-once"))
        #expect(after.state == "queued", "an expired window must leave the job resumable")
        #expect(after.attemptCount == 2,
                """
                attemptCount must be UNCHANGED. Pre-fix this arm charged one \
                attempt per cancellation and superseded at five — and with 80 % \
                of backfill grants expiring, wiring the expiry here would have \
                abandoned every long episode permanently on its fifth window.
                """)
        #expect(after.lastErrorCode == AnalysisWorkScheduler.backgroundWindowExpiredErrorCode)
        // A FLAT floor, not the exponential ladder: an expiry never escalates,
        // because there is nothing to escalate.
        #expect(after.nextEligibleAt == fixedNow.timeIntervalSince1970 + 60)
    }

    /// A wall clock the test can move, so consecutive expiries can be driven
    /// past the flat `interruptedRequeueDelaySeconds` requeue floor without
    /// sleeping for real.
    private final class MutableClock: @unchecked Sendable {
        private let lock = NSLock()
        private var value: TimeInterval
        init(_ value: TimeInterval) { self.value = value }
        var now: TimeInterval {
            lock.lock(); defer { lock.unlock() }
            return value
        }
        func advance(by seconds: TimeInterval) {
            lock.lock(); value += seconds; lock.unlock()
        }
    }

    @Test("five expired windows in a row still leave the job alive",
          .timeLimit(.minutes(1)))
    func repeatedExpiryNeverSupersedes() async throws {
        // Seeded at 4, one below `maxAttemptCount`. Pre-fix ONE cancel at this
        // input superseded the job with `nextEligibleAt: nil` and
        // `maxAttemptsReached:cancelMidRun` — and a superseded row NEVER comes
        // back, because `workKey` is UNIQUE and `insertJob` is `INSERT OR
        // IGNORE` over a key stable across launches.
        //
        // FIVE windows, actually driven, because the claim is about a SEQUENCE:
        // an implementation that exempted only the first expiry, or that
        // charged on some later pass, satisfies a single-dispatch version of
        // this test and still abandons every long episode — just later. Each
        // pass advances the clock past the flat requeue floor so the job is
        // genuinely re-dispatched rather than skipped as ineligible, which is
        // the failure mode that would make this vacuous.
        //
        // The 2026-08-06 pull contains two episodes killed at this terminal, so
        // it is real rather than theoretical — though both carry
        // `original_cancel_cause = pipeline_error`, not `task_expired`. The
        // terminal was already reachable; what this bead would have added,
        // without the exemption under test, is a road to it that 80 % of grants
        // travel.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await insertJob(store: store, downloads: downloads, jobId: "long-episode", attemptCount: 4)
        let clock = MutableClock(1_800_000_000)
        let scheduler = makeScheduler(
            store: store,
            downloads: downloads,
            clock: { Date(timeIntervalSince1970: clock.now) }
        )

        for window in 1...5 {
            let dispatched = await scheduler.processNextDispatchableJobForTesting(
                cancelAfterRunnerStart: .taskExpired
            )
            #expect(dispatched,
                    "window \(window): the job must still be dispatchable, or the loop proves nothing")

            let after = try #require(try await store.fetchJob(byId: "long-episode"))
            #expect(after.state == "queued",
                    "window \(window): the OS reclaiming a window must never permanently abandon an episode")
            #expect(after.attemptCount == 4,
                    "window \(window): attemptCount must still be untouched")
            #expect(after.lastErrorCode?.contains("maxAttemptsReached") != true,
                    "window \(window): no expiry may reach a terminal error code")

            // Past the flat requeue floor so the next pass can pick it up.
            clock.advance(by: 120)
        }
    }

    @Test("awaitJobsSettled waits for a running job and times out rather than lying",
          .timeLimit(.minutes(1)))
    func settleWaitDoesNotLie() async throws {
        // The expiration handler spends what is left of its teardown reserve
        // here so that `setTaskCompleted` — after which iOS may suspend the
        // process — does not land while the requeue write is still in flight. A
        // wait that reports success without observing anything is worse than no
        // wait: it makes the handler *look* like it protects the write.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await insertJob(store: store, downloads: downloads, jobId: "settle", attemptCount: 0)
        let scheduler = makeScheduler(store: store, downloads: downloads)

        // Nothing running: the cancel names nothing, and an empty population is
        // settled by definition rather than a thing to wait for. No elapsed
        // assertion here — the `guard !jobIds.isEmpty` returns without reading a
        // clock, so one could not fail. The non-vacuous version of that question
        // lives on the `unrelated` block below, where the method genuinely has
        // to look at something before it can return.
        #expect(await scheduler.awaitJobsSettled([], within: .seconds(5)))

        // A job parked in `decode` is NOT settled, and the wait must say so by
        // timing out rather than returning true.
        let dispatch = Task {
            await scheduler.processNextDispatchableJobForTesting()
        }
        // Give the dispatch a moment to take the job and enter decode. Bounded
        // explicitly rather than leaning on `.timeLimit`: a hang here would
        // otherwise read as a load flake instead of as "the job never
        // dispatched", which is a different bug.
        var spins = 0
        while await !scheduler.hasCurrentRunningTaskForTesting() {
            spins += 1
            try #require(spins < 500, "the seeded job never entered decode")
            try await Task.sleep(for: .milliseconds(20))
        }
        let settled = await scheduler.awaitJobsSettled(
            ["settle"],
            within: .milliseconds(300),
            pollInterval: .milliseconds(20)
        )
        #expect(!settled,
                "a job still parked in decode has not settled; reporting true loses the requeue")

        // THE IDENTITY SCOPING, and the reason this method takes ids at all.
        // The first draft waited on `currentRunningTask == nil` — "is ANYTHING
        // running" — which reads the same whether the job the handler cancelled
        // has settled or a sibling has since been picked up by `runLoop()`
        // (which polls every 5 s for the whole grant). With that signal this
        // assertion is false and the handler burns its entire reserve waiting
        // for a requeue that already committed.
        let unrelatedStart = ContinuousClock.now
        let unrelated = await scheduler.awaitJobsSettled(
            ["a-job-nobody-cancelled"],
            within: .seconds(5),
            pollInterval: .milliseconds(20)
        )
        #expect(unrelatedStart.duration(to: ContinuousClock.now) < .seconds(2),
                "and it must return AT ONCE. A 5 s budget with a job in flight is the shape the unscoped signal burned the whole reserve on.")
        #expect(unrelated,
                """
                The wait must be over the population the cancel named. A job \
                this handler never cancelled being in flight says nothing about \
                whether the cancelled one settled.
                """)

        let cancelled = await scheduler.cancelCurrentJob(cause: .taskExpired)
        #expect(cancelled == ["settle"],
                "the cancel must name what it acted on, or the wait cannot be scoped")
        #expect(await scheduler.awaitJobsSettled(cancelled, within: .seconds(10)),
                "once cancelled, the job must settle and the wait must observe it")
        _ = await dispatch.value
    }

    @Test("control: a genuine mid-run cancel still spends an attempt and still supersedes",
          .timeLimit(.minutes(1)))
    func genuineCancelStillEscalates() async throws {
        // The vacuity control. Exempting `.taskExpired` must not disarm the
        // poisoned-job escape valve — a job that keeps dying mid-run for its own
        // reasons still has to reach `maxAttemptsReached` and free the slot.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await insertJob(store: store, downloads: downloads, jobId: "poisoned", attemptCount: 4)
        let scheduler = makeScheduler(store: store, downloads: downloads)

        _ = await scheduler.processNextDispatchableJobForTesting(
            cancelAfterRunnerStart: .pipelineError
        )

        let after = try #require(try await store.fetchJob(byId: "poisoned"))
        #expect(after.state == "superseded")
        #expect(after.attemptCount == 5)
        #expect(after.lastErrorCode?.contains("maxAttemptsReached") == true)
    }
}

// MARK: - The handler spends a grant-shaped budget

@Suite("playhead-lmrx: the backfill handler bounds its grant")
struct BackfillGrantBoundingTests {

    @Test("the poll loop is handed a deadline inside the measured grant, not 25 minutes",
          .timeLimit(.minutes(1)))
    func pollLoopDeadlineIsGrantShaped() async throws {
        let coordinator = StubAnalysisCoordinator()
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider()
        )
        let task = StubBackgroundTask()

        let before = ContinuousClock.now
        await bps.handleBackfillTask(task)
        await task.awaitCompletion()
        let after = ContinuousClock.now

        let deadline = try #require(coordinator.runPendingBackfillDeadlines.first,
                                    "the handler must hand the coordinator a deadline")
        let budget = BackgroundGrantBudget.backfillProcessing

        // The handler anchors the deadline at the instant the grant opened,
        // which it reads somewhere in `[before, after]`. So the deadline lies in
        // `[before + workBudget, after + workBudget]`, and BOTH ends matter:
        //
        //   upper — measured from `after`, the horizon is at most the whole work
        //           budget. This kills the shipped defect: `.seconds(25 * 60)`
        //           gives 1500 s here against a budget of 219 s.
        //   lower — measured from `before`, it is at least the whole budget.
        //           Without this a deadline of `now` satisfies the upper bound
        //           while polling for nothing at all.
        #expect(after.duration(to: deadline) <= budget.workBudget,
                """
                The poll loop's deadline must be inside the measured grant. Got \
                \(after.duration(to: deadline)) past the handler's return, budget \
                is \(budget.workBudget); the pre-fix value was 1500 s against a \
                294 s (p50, n=203) grant.
                """)
        #expect(before.duration(to: deadline) >= budget.workBudget,
                "a bounded deadline must still hand over the whole work budget")
        #expect(before.duration(to: deadline) > budget.minimumCheckpointBudget,
                "and that budget must leave room to start at least one pass")
    }

    @Test("the charged sibling's handler spends its own budget", .timeLimit(.minutes(1)))
    func chargedSiblingUsesItsOwnBudget() async throws {
        // The handler is shared by two identifiers and only one of them has ever
        // been observed. Passing the budget in is what keeps the measured
        // number off the unmeasured class.
        let coordinator = StubAnalysisCoordinator()
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider()
        )
        let task = StubBackgroundTask()

        let before = ContinuousClock.now
        await bps.handleBackfillTask(task, budget: .backfillProcessingCharged)
        await task.awaitCompletion()

        let deadline = try #require(coordinator.runPendingBackfillDeadlines.first)
        #expect(before.duration(to: deadline)
            >= BackgroundGrantBudget.backfillProcessingCharged.workBudget)
        #expect(before.duration(to: deadline)
            > BackgroundGrantBudget.backfillProcessing.workBudget,
                "the charged class must not inherit the plain identifier's measured cap")
    }
}

// MARK: - The expiry leaves a durable, readable record

@Suite("playhead-lmrx: a backfill expiry is durable")
struct BackfillExpiryDurabilityTests {

    /// Hangs in `decode` until cancelled, so a job can be held in flight across
    /// a handler's exit.
    private final class CancellableDecodeAudioStub: AnalysisAudioProviding, @unchecked Sendable {
        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            try await Task.sleep(for: .seconds(120))
            return []
        }
    }

    /// Snapshots an arbitrary probe at the moment `finishRun` lands, so the
    /// ORDER of the expiration handler's steps is directly assertable rather
    /// than inferred from wall clock. Same shape as
    /// `HandlerOrderSpyTempFileRemover` in the rediff suite.
    private final class OrderProbeRunLedger: BackgroundTaskRunLedger, @unchecked Sendable {
        private let probe: @Sendable () async -> Bool
        private let lock = NSLock()
        private var finishes: [(BackgroundTaskRunOutcome, Bool)] = []

        init(probe: @escaping @Sendable () async -> Bool) { self.probe = probe }

        /// `(outcome, probe result)` for every terminal write, in order.
        var recordedFinishes: [(BackgroundTaskRunOutcome, Bool)] {
            lock.lock(); defer { lock.unlock() }
            return finishes
        }

        func startRun(
            entryPoint: BackgroundTaskRunEntryPoint,
            taskIdentifier: String,
            taskInstanceID: String?,
            scenePhase: String?
        ) async -> String { UUID().uuidString }

        func recordRunStart(
            runId: String,
            entryPoint: BackgroundTaskRunEntryPoint,
            taskIdentifier: String,
            taskInstanceID: String?,
            scenePhase: String?
        ) async {}

        /// Non-async so the lock is taken outside an async context — `NSLock`'s
        /// `lock()`/`unlock()` are unavailable directly from one.
        private func record(_ outcome: BackgroundTaskRunOutcome, observed: Bool) {
            lock.lock(); defer { lock.unlock() }
            finishes.append((outcome, observed))
        }

        @discardableResult
        func finishRun(runId: String, update: BackgroundTaskRunOutcomeUpdate) async -> Bool {
            record(update.outcome, observed: await probe())
            return true
        }

        func fetchLatestRun(for entryPoint: BackgroundTaskRunEntryPoint) async -> BackgroundTaskRunRecord? { nil }
        func fetchRecentRuns(limit: Int) async -> [BackgroundTaskRunRecord] { [] }
        func fetchLatestRun(forAssetId assetId: String) async -> BackgroundTaskRunRecord? { nil }
        @discardableResult
        func reapOrphansAtLaunch(startedBefore: Double) async -> Int { 0 }
    }

    private func makeScheduler(
        store: AnalysisStore,
        audio: any AnalysisAudioProviding = StubAnalysisAudioProvider(),
        downloads: StubDownloadProvider = StubDownloadProvider()
    ) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audio,
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(
                snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
            ),
            downloadManager: downloads,
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider()
        )
    }

    private func makeReconciler(store: AnalysisStore) -> AnalysisJobReconciler {
        AnalysisJobReconciler(
            store: store,
            downloadManager: StubDownloadProvider(),
            capabilitiesService: StubCapabilitiesProvider(
                snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
            )
        )
    }

    @discardableResult
    private func insertQueuedJob(_ store: AnalysisStore, id: String) async throws -> AnalysisJob {
        let job = makeAnalysisJob(
            jobId: id,
            jobType: "preAnalysis",
            episodeId: "ep-\(id)",
            analysisAssetId: nil,
            workKey: AnalysisJob.computeWorkKey(
                fingerprint: "fp-\(id)",
                analysisVersion: PreAnalysisConfig.analysisVersion,
                jobType: "preAnalysis"
            ),
            sourceFingerprint: "fp-\(id)",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)
        return job
    }

    @Test("expiry tells the SCHEDULER the grant is over, so the in-flight job requeues",
          .timeLimit(.minutes(1)))
    func expiryCancelsTheInFlightAnalysisJob() async throws {
        // THE WIRING half of the resume point (the scheduler-side half is
        // `ExpiredWindowAttemptAccountingTests`). `workTask.cancel()` reaches
        // this handler's own task, but the analysis job the drain dispatched
        // lives on `AnalysisWorkScheduler`'s task tree and never sees it.
        // Pre-fix the backfill expiration handler did not call
        // `cancelCurrentJob`, so the job ran on unaware into process
        // suspension: nothing checkpointed, nothing requeued, and a `running`
        // row with a stale lease. The healthy sibling — pre-analysis recovery,
        // 86 of 97 wakes at `recovered_work` — has called it since
        // playhead-1nl6.
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)

        let coordinator = StubAnalysisCoordinator()
        coordinator.runPendingBackfillDuration = .seconds(30)
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider()
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: makeReconciler(store: store))

        #expect(await scheduler.pendingCancelCauseForTesting() == nil, "nothing has expired yet")

        let task = StubBackgroundTask()
        let workTask = Task { await bps.handleBackfillTask(task) }
        await task.awaitExpirationHandlerInstalled()
        task.simulateExpiration()
        _ = await workTask.value
        await task.awaitCompletion()

        #expect(await scheduler.pendingCancelCauseForTesting() == .taskExpired,
                """
                A backfill BGTask expiry must tell the scheduler its grant \
                ended, with cause .taskExpired — that cause is exactly what the \
                cancel arm consumes to requeue the job without spending an \
                attempt. Pre-fix it was nil.
                """)
    }

    @Test("an expired run records what the window achieved, not only that it ended",
          .timeLimit(.minutes(1)))
    func expiredRunPersistsItsCounters() async throws {
        // Pre-fix: `finishRun` on the expiration path wrote outcome, cause and
        // expiration only, so all 203 expired rows in the 2026-08-06 pull carry
        // NULL `jobsSeen`/`jobsCompleted`. The "4 of 203 wrote a durable scan
        // row" figure in the bead had to be reconstructed by joining
        // `semantic_scan_results` timestamps against run windows — the
        // reconstruction this makes unnecessary.
        //
        // A REAL scheduler with a KNOWN queue depth, so the assertion is
        // `jobsSeen == 2` rather than merely non-nil: an implementation that
        // wrote a literal 0 would pass the weaker form.
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)
        try await insertQueuedJob(store, id: "pending-a")
        try await insertQueuedJob(store, id: "pending-b")

        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let coordinator = StubAnalysisCoordinator()
        coordinator.runPendingBackfillDuration = .seconds(30)
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            runLedger: ledger
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: makeReconciler(store: store))

        let task = StubBackgroundTask()
        let workTask = Task { await bps.handleBackfillTask(task) }
        // The baseline is read before `runPendingBackfill` parks, so waiting for
        // the poll loop to be ENTERED proves the counters were published before
        // expiration fires. Without this the test would race the very
        // publication it asserts.
        await coordinator.runPendingBackfillEntries.wait(for: 1)
        task.simulateExpiration()
        _ = await workTask.value
        await task.awaitCompletion()

        let latest = try #require(await ledger.fetchLatestRun(for: .backfill))
        #expect(latest.outcome == .expired)
        #expect(latest.expiration == true)
        #expect(latest.jobsSeen == 2,
                """
                An expired run must record the queue depth it observed. NULL is \
                the state the bead measured on 203 of 254 backfill wakes; a \
                hardcoded 0 would be worse.
                """)
        #expect(latest.jobsCompleted == 0,
                "nothing completed in this window, and 0 is a finding — not NULL, not absent")
        // playhead-lmrx (review round 5): the EXPIRY path's door wiring. Same
        // argument as the work-deadline twin — the door's mechanism is pinned
        // by `dispatchClosesForTeardownAndReopens`, its USE by nothing until
        // here. The closure lasts the shipped 36 s reserve, so this read is
        // well inside the test's own lifetime.
        #expect(await scheduler.isDispatchClosedForTesting(),
                "the expiration teardown must shut the dispatch door before it cancels")
    }

    @Test("an expired run's completion count is measured, not a literal zero",
          .timeLimit(.minutes(1)))
    func expiredRunCountsRealCompletions() async throws {
        // The complement of the test above, and the reason it exists: nothing
        // else drives the HANDLER to a non-zero completion, so a
        // `counters.noteCompleted(0)` literal inside `recordGrantCompletions`
        // would satisfy every other assertion in this file. Zero is the bead's
        // headline finding precisely because it is a MEASUREMENT, and a
        // measurement that can only ever read zero is not one.
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)
        try await insertQueuedJob(store, id: "pending-a")
        try await insertQueuedJob(store, id: "pending-b")

        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let coordinator = StubAnalysisCoordinator()
        coordinator.runPendingBackfillDuration = .seconds(30)
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            runLedger: ledger
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: makeReconciler(store: store))

        let task = StubBackgroundTask()
        let workTask = Task { await bps.handleBackfillTask(task) }
        // Entry into the poll loop proves the baseline of TWO was already read,
        // so the completion below lands inside the window and is scored against
        // the population the window named.
        await coordinator.runPendingBackfillEntries.wait(for: 1)
        try await store.updateJobState(jobId: "pending-a", state: "complete")
        task.simulateExpiration()
        _ = await workTask.value
        await task.awaitCompletion()

        let latest = try #require(await ledger.fetchLatestRun(for: .backfill))
        #expect(latest.outcome == .expired)
        #expect(latest.jobsSeen == 2, "the denominator is still the queue depth at grant open")
        #expect(latest.jobsCompleted == 1,
                """
                One of the two jobs this window found pending reached a terminal \
                complete inside it. A hardcoded 0 — or a count taken over the \
                whole `complete` table rather than the baseline — fails here.
                """)
    }

    @Test("the work-deadline return MEASURES its completions too, and writes them",
          .timeLimit(.minutes(1)))
    func normalReturnCountsRealCompletions() async throws {
        // playhead-lmrx (review round 6): THE THIRD INSTANCE OF THE ROUND-5
        // DEFECT — a capability whose CALLEE is pinned and whose CALL SITE is
        // not.
        //
        // `recordGrantCompletions` has two call sites. The expiration one is
        // driven by the two tests above; the work-deadline one at
        // `BackgroundProcessingService.swift:1626` was driven by nothing —
        // every ledger-reading test in this file reaches `finishRun` through
        // the EXPIRY, and the two normal-return tests
        // (`normalReturnAlsoRequeuesTheInFlightJob`,
        // `earlyDrainReturnLeavesTheRunLoopsJobAlone`) construct no `runLedger`
        // at all, so they hold a `NoOpBackgroundTaskRunLedger` and cannot
        // observe a ledger write of any kind. Delete that one line and every
        // `admitted_work` row goes back to `jobsCompleted = NULL` — which is
        // half of this bead's headline claim, "26 jobs admitted / 0 completed,
        // and all 12 rows read NULL" — with the whole suite green.
        //
        // Asked the diagnostic way: what would the suite read if the normal
        // return never counted anything? Exactly what it read before this test.
        //
        // Same fixture and same argument as `expiredRunCountsRealCompletions`,
        // one exit further along: TWO baseline jobs so the denominator is
        // known, ONE of them driven to terminal `complete` inside the window,
        // so a literal `0` and a count taken over the wrong population both
        // fail. The difference is only which ending is reached — this one takes
        // `deadlineAlreadyPast`, the state a real window is in at t+219 s, and
        // returns normally with `Task.isCancelled` still false.
        //
        // THE BASELINE ROWS CARRY A FUTURE `nextEligibleAt`, and that is load
        // insurance rather than decoration. The handler starts and wakes the
        // long-lived `runLoop()`, which selects a queued row, finds no cached
        // audio and writes `state = 'blocked'` — and with this budget
        // `drainEligible` breaks immediately, so unlike the expiry twin that
        // blocking is NOT done and finished before the window's own gate. It
        // raced the `complete` write and overwrote it, and the counter read 0:
        // observed on the first mutation-battery baseline of this round, green
        // in a scoped run, red under the full focused set. A future
        // `nextEligibleAt` keeps the rows out of `fetchNextEligibleJob`
        // entirely (they are still `queued`, so they are still the baseline
        // population `pendingJobIdsForLedger` reads) so the only writer of
        // these two rows is the test.
        let store = try await makeTestStore()
        let scheduler = makeScheduler(store: store)
        let ineligibleUntil = Date().timeIntervalSince1970 + 3600
        for id in ["pending-a", "pending-b"] {
            try await store.insertJob(makeAnalysisJob(
                jobId: id,
                jobType: "preAnalysis",
                episodeId: "ep-\(id)",
                analysisAssetId: nil,
                workKey: AnalysisJob.computeWorkKey(
                    fingerprint: "fp-\(id)",
                    analysisVersion: PreAnalysisConfig.analysisVersion,
                    jobType: "preAnalysis"
                ),
                sourceFingerprint: "fp-\(id)",
                priority: 10,
                desiredCoverageSec: 90,
                state: "queued",
                nextEligibleAt: ineligibleUntil
            ))
        }

        let ledger = AnalysisStoreBackgroundTaskRunLedger(store: store)
        let coordinator = StubAnalysisCoordinator()
        // A gate the TEST opens rather than a duration it hopes for: the poll
        // loop is held inside `runPendingBackfill` until the completion below
        // has landed, so "the job completed after the baseline was read and
        // before the counters were taken" is a fact, not a race.
        coordinator.runPendingBackfillHoldsUntilReleased = true
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            runLedger: ledger
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: makeReconciler(store: store))

        let task = StubBackgroundTask()
        let workTask = Task { await bps.handleBackfillTask(task, budget: Self.deadlineAlreadyPast) }
        // Entry into the poll loop proves the baseline of TWO was already read,
        // so the completion below is scored against the population this window
        // named rather than against whatever the queue happens to hold later.
        await coordinator.runPendingBackfillEntries.wait(for: 1)
        try await store.updateJobState(jobId: "pending-a", state: "complete")
        coordinator.runPendingBackfillReleases.increment()
        await workTask.value
        await task.awaitCompletion()

        let latest = try #require(await ledger.fetchLatestRun(for: .backfill))
        #expect(latest.outcome == .admittedWork,
                """
                This must be the WORK-DEADLINE return, not the expiry — the \
                expiration handler writes `.expired`, and a test that reached \
                it would be a third copy of the two above rather than the \
                missing one.
                """)
        #expect(latest.expiration == false, "and nothing about this ending was an OS reclaim")
        #expect(latest.jobsSeen == 2, "the denominator is the queue depth at grant open")
        #expect(latest.jobsCompleted == 1,
                """
                One of the two jobs this window found pending reached terminal \
                `complete` inside it. NULL means the normal return never \
                measured anything — the state all 12 `admitted_work` rows in \
                the 2026-08-06 pull are in. A hardcoded 0, or a count taken \
                over the queue as it stands at teardown rather than over the \
                baseline, also fails here.
                """)
    }

    @Test("the work-deadline return does not strand the job it leaves running",
          .timeLimit(.minutes(2)))
    func normalReturnAlsoRequeuesTheInFlightJob() async throws {
        // playhead-lmrx review round: THE EXIT THE FIX CREATED.
        //
        // Pre-lmrx both work drivers ran to `now + 25 * 60`, so inside a ~295 s
        // grant `handleBackfillTask`'s normal return was unreachable and the OS
        // expiry always drove teardown. Bounding them at 219 s makes that return
        // the common exit — with `Task.isCancelled` still FALSE, and with the
        // scheduler's long-lived `runLoop()` (started and woken by this handler)
        // still dispatching on its own 5 s poll. The handler then calls
        // `setTaskCompleted`, after which iOS may suspend at once AND after
        // which the expirationHandler can never fire. A job in flight at that
        // moment is stranded exactly as the bead describes.
        //
        // Driven with NO timing dependence: the job is put in flight by another
        // driver BEFORE the handler starts. Its lease then makes it invisible
        // to `fetchNextEligibleJob`, so the handler's own drain dispatches
        // nothing and its poll loop returns at once — reproducing "the handler
        // reaches its normal return while a job it did not dispatch is still
        // running", which is the shape `runLoop()` produces in production.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let scheduler = makeScheduler(
            store: store,
            audio: CancellableDecodeAudioStub(),
            downloads: downloads
        )

        downloads.cachedURLs["ep-late-dispatch"] = URL(fileURLWithPath: "/tmp/ep-late-dispatch.m4a")
        try await store.insertJob(makeAnalysisJob(
            jobId: "late-dispatch",
            jobType: "preAnalysis",
            episodeId: "ep-late-dispatch",
            analysisAssetId: "asset-late-dispatch",
            workKey: "fp-late-dispatch:1:preAnalysis",
            sourceFingerprint: "fp-late-dispatch",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        ))
        let dispatch = Task { await scheduler.processNextDispatchableJobForTesting() }
        var spins = 0
        while await !scheduler.hasCurrentRunningTaskForTesting() {
            spins += 1
            try #require(spins < 1000, "the seeded job never entered decode")
            try await Task.sleep(for: .milliseconds(20))
        }

        let coordinator = StubAnalysisCoordinator()
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider()
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: makeReconciler(store: store))

        let task = StubBackgroundTask()
        // A budget whose work deadline is ALREADY past when the handler runs,
        // which is the state the 219 s bound puts a real window into at t+219 s.
        // `workBudget` clamps to zero, so the drain admits nothing and the poll
        // loop returns at once — no sleeping, no racing.
        await bps.handleBackfillTask(task, budget: Self.deadlineAlreadyPast)
        await task.awaitCompletion()

        let after = try #require(try await store.fetchJob(byId: "late-dispatch"))
        #expect(after.state == "queued",
                """
                The work-deadline return must cancel and requeue what it leaves \
                running, exactly as the expiry does. Finding this row still \
                `running` means the handler called setTaskCompleted over a live \
                job — no checkpoint, no requeue, a stale lease for the reaper, \
                and no expirationHandler left to rescue it.
                """)
        #expect(after.lastErrorCode == AnalysisWorkScheduler.backgroundWindowExpiredErrorCode)
        #expect(after.attemptCount == 0,
                "the grant ending is not the job's fault on this path either")
        // playhead-lmrx (review round 5): THE DOOR'S WIRING, not just its
        // mechanism. `dispatchClosesForTeardownAndReopens` drives
        // `closeDispatchForTeardown` directly, so it proves the door WORKS and
        // says nothing about whether any handler uses it — delete the three
        // `await scheduler.closeDispatchForTeardown(...)` lines from the
        // handlers and every other assertion in this file stays green, while
        // `runLoop()`'s 5 s poll is free again to dispatch a job into
        // `setTaskCompleted`. Asked the diagnostic way: what would this suite
        // read if no handler ever shut the door? Exactly what it reads now.
        #expect(await scheduler.isDispatchClosedForTesting(),
                """
                the work-deadline teardown must SHUT THE DOOR before it \
                cancels, or the cancel is a one-shot edge against a loop that \
                is still dispatching
                """)
        _ = await dispatch.value
    }

    @Test("a teardown cancelled mid-settle leaves the ending to the expiration handler",
          .timeLimit(.minutes(2)))
    func backfillTeardownCancelledMidSettleDefersToTheExpiry() async throws {
        // playhead-lmrx (review round 7): THE `guard !Task.isCancelled` AFTER
        // THE SETTLE WAIT, WHICH NO TEST COULD SEE UNTIL NOW.
        //
        // `awaitJobsSettled` returns `false` IMMEDIATELY on cancellation — that
        // guard is what stops it busy-spinning to its deadline. So an OS reclaim
        // landing while the work-deadline teardown is waiting drops the work
        // task straight out of the wait, and without this guard it walks on to
        // `markComplete(success: true)` and RACES the expiration handler to
        // `setTaskCompleted` — winning, because the handler still has its own
        // cancel, ledger write and settle to do. The task is then completed
        // `.admittedWork` over a live job: no checkpoint, no requeue, a leased
        // `running` row, and no expirationHandler left to rescue it. That is
        // precisely the stranding this teardown block exists to prevent,
        // arrived at THROUGH the block itself.
        //
        // WHY THIS IS NOT A RACE DRESSED AS A CLAIM (round 6 declined to build
        // it for exactly that reason). Two seams make the observation a fact:
        //
        //  * the expiration handler is PARKED at its own `finishRun` — which is
        //    upstream of `handleExpiredProcessingTask` on this path — so from
        //    that moment the work task is the only party that CAN complete the
        //    task. Nobody is racing it;
        //  * and `workTaskForTesting(_:).value` is the happens-before edge. It
        //    returns on BOTH paths — the guard's `return` and the fall-through
        //    to `markComplete` — so the assertion below is taken after the work
        //    task has finished deciding, not after a duration.
        //
        // WHAT PINS THE CANCEL'S PLACEMENT, stated precisely because an earlier
        // draft of this comment claimed more than it had (adversarial pass).
        // `ledger.finishes` fires INSIDE `finishRun`, so `simulateExpiration()`
        // below is not ordered against the work task's next few statements. It
        // does not need to be: the job is held by `unwind`, so `inFlightJobIds`
        // never becomes disjoint and `awaitJobsSettled` CANNOT return `true`.
        // The work task is therefore parked until it is cancelled, whichever
        // side of the settle the cancel arrives on — and it reaches the guard
        // with `Task.isCancelled` true either way. The only thing the 30 s
        // reserve buys is a ceiling on that wait; it is not what orders the
        // test. (The wait exists at all because `cancelCurrentJob` returned a
        // non-empty set, which the spin loop above guarantees.)
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-mid-settle"] = URL(fileURLWithPath: "/tmp/ep-mid-settle.m4a")
        try await store.insertJob(makeAnalysisJob(
            jobId: "mid-settle",
            jobType: "preAnalysis",
            episodeId: "ep-mid-settle",
            analysisAssetId: "asset-mid-settle",
            workKey: "fp-mid-settle:1:preAnalysis",
            sourceFingerprint: "fp-mid-settle",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        ))

        // Gated, not merely slow: the job must still be in flight when the
        // teardown's settle wait runs, or the wait returns `true` at once and
        // the cancellation never lands inside it.
        let unwind = UnwindGate()
        defer { unwind.open() }
        let scheduler = makeScheduler(
            store: store,
            audio: GatedUnwindAudioStub(gate: unwind),
            downloads: downloads
        )

        // Put the job in flight from ANOTHER driver, as
        // `normalReturnAlsoRequeuesTheInFlightJob` does: its lease then hides it
        // from the handler's own drain, so nothing here depends on the handler
        // dispatching anything.
        let dispatch = Task { await scheduler.processNextDispatchableJobForTesting() }
        var spins = 0
        while await !scheduler.hasCurrentRunningTaskForTesting() {
            spins += 1
            try #require(spins < 1000, "the seeded job never entered decode")
            try await Task.sleep(for: .milliseconds(20))
        }

        let expiryGate = UnwindGate()
        defer { expiryGate.open() }
        let ledger = GatedRunLedger(parkOn: .expired, gate: expiryGate)
        let bps = BackgroundProcessingService(
            coordinator: StubAnalysisCoordinator(),
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            runLedger: ledger
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: makeReconciler(store: store))

        let task = StubBackgroundTask()
        await bps.handleBackfillTask(task, budget: Self.deadlineAlreadyPast)
        let workTask = try #require(
            await bps.workTaskForTesting(task),
            "the handler must leave its work task registered until something completes the BGTask"
        )

        // The work task's OWN terminal write — the statement immediately before
        // the settle wait. Waiting on it is what puts the cancellation inside
        // the wait rather than before it.
        await ledger.finishes.wait(for: 1)
        task.simulateExpiration()

        // And now the expiration handler is committed to its ledger write and
        // parked there. It cannot reach `markComplete` while this holds.
        await ledger.reachedPark.wait(for: 1)

        await workTask.value

        #expect(task.completedSuccess == nil,
                """
                The work task must NOT complete the BGTask after the OS \
                cancelled it mid-teardown. Finding `true` here means it fell \
                through the settle wait into `markComplete(success: true)` and \
                told iOS the window was finished over a job that is still \
                running — the exact stranding the teardown exists to prevent, \
                reached through the teardown itself.
                """)
        #expect(task.setTaskCompletedCallCount == 0,
                "and it must not have touched setTaskCompleted at all")

        // The complement: once the expiration handler is released it DOES end
        // the run, and ends it as an expiry.
        unwind.open()
        expiryGate.open()
        await task.awaitCompletion()
        #expect(task.completedSuccess == false,
                "the expiration handler owns the ending, and records it as one")
        // NOT `recordedOutcomes.contains(.expired)` — that is implied by
        // `reachedPark.wait(for: 1)` above, since `GatedRunLedger` records the
        // outcome before incrementing the park counter, so it could not fail
        // (adversarial pass). What is NOT implied is the ORDER: the work task's
        // own terminal write lands first and the expiry's second, which is what
        // makes the expiry the row that survives.
        #expect(ledger.recordedOutcomes.last == .expired,
                "the expiry must be the LAST terminal write, so it is the outcome that stands")
        _ = await dispatch.value
    }

    /// A budget whose `workBudget` clamps to zero, so `workDeadline(from:)`
    /// returns the grant's own start instant and every deadline test reads
    /// "already elapsed". The reserve is real so the settle has something to
    /// spend.
    private static let deadlineAlreadyPast = BackgroundGrantBudget(
        designGrant: .zero,
        teardownReserve: .seconds(30),
        minimumCheckpointBudget: .zero,
        minimumDrainCheckpointBudget: .zero,
        expirationSettleGrace: .seconds(30),
        provenance: .assumed
    )

    /// The shipped backfill budget with a deliberately generous post-reclaim
    /// grace.
    ///
    /// `expiredRowLandsBeforeTheSettleWait` below pins an ORDER — the durable
    /// `expired` row before anything that can block — and asserts, as its
    /// complement, that the wait then actually observes the requeue. The
    /// shipped grace is 3 s, which is the right ceiling in production (see
    /// `BackgroundGrantBudget.expirationSettleGrace`) and the wrong thing for
    /// this test to depend on: it would make an ordering assertion fail
    /// intermittently under the mutation battery's own suite load, which is the
    /// flake this file already fought once. The grace's own value is pinned
    /// arithmetically, and without a clock, in
    /// `BackgroundGrantBudgetArithmeticTests`.
    private static let generousSettleGrace = BackgroundGrantBudget(
        designGrant: BackgroundGrantBudget.backfillProcessing.designGrant,
        teardownReserve: BackgroundGrantBudget.backfillProcessing.teardownReserve,
        minimumCheckpointBudget: BackgroundGrantBudget.backfillProcessing.minimumCheckpointBudget,
        minimumDrainCheckpointBudget: BackgroundGrantBudget.backfillProcessing.minimumDrainCheckpointBudget,
        expirationSettleGrace: .seconds(30),
        provenance: .assumed
    )

    @Test("a drain that returned EARLY does not cancel the run loop's job",
          .timeLimit(.minutes(2)))
    func earlyDrainReturnLeavesTheRunLoopsJobAlone() async throws {
        // THE COMPLEMENT, and the reason the cancel above is gated on the
        // deadline rather than fired unconditionally.
        //
        // `runBackfillPollingLoop` also returns on two consecutive EMPTY polls
        // — the 51 normal returns in the 2026-08-06 pull — seconds into a
        // window with the whole grant still ahead. Cancelling there reaches
        // whatever `runLoop()` happens to be running, which can be a
        // `playback`-lane catch-up job for the episode the user is listening to
        // right now, and costs it a 60 s requeue for nothing: the grant has not
        // ended, so nothing is about to be suspended.
        //
        // Identical fixture to the test above; the ONLY difference is the
        // budget, so what this pair isolates is the gate and not the wiring.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let scheduler = makeScheduler(
            store: store,
            audio: CancellableDecodeAudioStub(),
            downloads: downloads
        )

        downloads.cachedURLs["ep-early-return"] = URL(fileURLWithPath: "/tmp/ep-early-return.m4a")
        try await store.insertJob(makeAnalysisJob(
            jobId: "early-return",
            jobType: "preAnalysis",
            episodeId: "ep-early-return",
            analysisAssetId: "asset-early-return",
            workKey: "fp-early-return:1:preAnalysis",
            sourceFingerprint: "fp-early-return",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        ))
        let dispatch = Task { await scheduler.processNextDispatchableJobForTesting() }
        var spins = 0
        while await !scheduler.hasCurrentRunningTaskForTesting() {
            spins += 1
            try #require(spins < 1000, "the seeded job never entered decode")
            try await Task.sleep(for: .milliseconds(20))
        }

        let coordinator = StubAnalysisCoordinator()
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider()
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: makeReconciler(store: store))

        let task = StubBackgroundTask()
        // The SHIPPED budget: 219 s of work, none of it spent, so the handler
        // reaches its normal return with the deadline far in the future.
        await bps.handleBackfillTask(task)
        await task.awaitCompletion()

        let after = try #require(try await store.fetchJob(byId: "early-return"))
        #expect(after.state == "running",
                """
                A drain that finished early has not spent the grant, so it must \
                leave the run loop's job alone. Finding it `queued` means the \
                handler cancelled a live job — possibly a playback-lane \
                catch-up — and bought it a 60 s delay for a window that had not \
                ended.
                """)
        #expect(await scheduler.pendingCancelCauseForTesting() == nil,
                "and it must not even ARM a cancel cause for the next job to inherit")
        // playhead-lmrx (review round 5): and it must not shut the DOOR either.
        // The close sits inside the same `deadlineElapsed` gate as the cancel,
        // so this is the complement of the assertion in the twin above — and it
        // is the half that would bite a listening user: the door is checked
        // ABOVE the T0 playback bypass, so a close fired on an EARLY drain
        // return refuses the playback-lane catch-up for the episode being
        // played, for the whole teardown reserve, inside a grant that has not
        // ended.
        #expect(!(await scheduler.isDispatchClosedForTesting()),
                "a drain that finished early has not spent the grant and must leave dispatch open")

        await scheduler.cancelCurrentJob(cause: .userCancelled)
        _ = await dispatch.value
    }

    @Test("the expired row is durable BEFORE the handler waits for anything",
          .timeLimit(.minutes(2)))
    func expiredRowLandsBeforeTheSettleWait() async throws {
        // playhead-lmrx review round. playhead-hygc.1.4 ordered the expiration
        // `finishRun` first on purpose — "so a subsequent crash (or OS-forced
        // termination) still leaves a durable audit trail". The first draft of
        // this bead put a wait of up to the WHOLE teardown reserve in front of
        // that write, to give the cancelled analysis job time to commit its
        // resume point. Both obligations are real; the order that satisfies
        // both is cancel, write, THEN wait.
        //
        // Get it wrong and the regression is silent and strictly worse than the
        // defect being fixed: a process killed during the wait leaves the row at
        // `running` — no outcome at all — where before it at least reached
        // `expired` with NULL counters.
        //
        // The probe is taken INSIDE `finishRun`, and the cancelled job is held
        // by a gate the ledger write itself opens — so "still unwinding when
        // the row was written" is a fact this test controls, not a duration it
        // races. An earlier draft used a fixed 2 s hold and went red under the
        // mutation battery's own suite load. What is left is not zero timing:
        // the 30 s deadlock timer armed just before `simulateExpiration()` is a
        // budget for the teardown PREFIX — a cancel, a telemetry emit, a
        // MainActor hop and two SQLite statements. It is 15x the window that
        // failed, and on the correct ordering the gate is opened by the ledger
        // write itself, so the timer is never reached at all.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-slow-unwind"] = URL(fileURLWithPath: "/tmp/ep-slow-unwind.m4a")
        let job = makeAnalysisJob(
            jobId: "slow-unwind",
            jobType: "preAnalysis",
            episodeId: "ep-slow-unwind",
            analysisAssetId: "asset-slow-unwind",
            workKey: "fp-slow-unwind:1:preAnalysis",
            sourceFingerprint: "fp-slow-unwind",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let gate = UnwindGate()
        defer { gate.open() }
        let scheduler = makeScheduler(
            store: store,
            audio: GatedUnwindAudioStub(gate: gate),
            downloads: downloads
        )
        let ledger = OrderProbeRunLedger {
            // `inFlightJobIds`, not `currentRunningTask` — the probe must
            // measure the same population `awaitJobsSettled` waits on, or it is
            // asserting about a quantity production does not use.
            let stillUnwinding = await scheduler.inFlightJobIdsForTesting()
                .contains("slow-unwind")
            // The write has landed; let the cancelled job finish unwinding so
            // the settle below has something to observe.
            gate.open()
            return stillUnwinding
        }
        let coordinator = StubAnalysisCoordinator()
        coordinator.runPendingBackfillDuration = .seconds(30)
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            runLedger: ledger
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: makeReconciler(store: store))

        let task = StubBackgroundTask()
        // The shipped budget with a wider post-reclaim grace — see
        // `generousSettleGrace`. This test is about the ORDER of the durable
        // write against the wait; the grace's length is pinned without a clock
        // in `BackgroundGrantBudgetArithmeticTests`.
        let workTask = Task { await bps.handleBackfillTask(task, budget: Self.generousSettleGrace) }
        await task.awaitExpirationHandlerInstalled()

        // Wait until the drain has the job genuinely in flight, so the
        // expiration has something to cancel. Bounded explicitly: a hang here
        // means the job never dispatched, which is a different bug from the one
        // under test and must not read as a load flake.
        var spins = 0
        while await !scheduler.hasCurrentRunningTaskForTesting() {
            spins += 1
            try #require(spins < 1000, "the seeded job never entered decode")
            try await Task.sleep(for: .milliseconds(20))
        }

        // Deadlock insurance, armed HERE and not a line earlier: from this
        // point the only thing that has to fit is the handler's teardown up to
        // its ledger write, which is a cancel, a telemetry emit and two SQLite
        // statements. On the correct ordering the gate is opened by that write,
        // so this timer is never reached; on the wrong one it is what turns a
        // hang into a named failure.
        gate.openAfter(seconds: 30)
        task.simulateExpiration()
        _ = await workTask.value
        await task.awaitCompletion()

        let expiredWrites = ledger.recordedFinishes.filter { $0.0 == .expired }
        try #require(!expiredWrites.isEmpty, "the expiration path must write a terminal row")
        #expect(expiredWrites.allSatisfy { $0.1 },
                """
                The `expired` row must be written while the cancelled job is \
                STILL unwinding. Observing a settled scheduler here means the \
                handler waited first and the durable write is behind a wait \
                that an OS kill can cut short — which leaves no row at all.
                """)

        // And the wait still happened: by the time teardown finished, the job
        // had unwound and been requeued. Without the wait the handler would
        // reach `setTaskCompleted` with this row still `running`.
        let after = try #require(try await store.fetchJob(byId: "slow-unwind"))
        #expect(after.state == "queued",
                "the handler must not complete the task until the requeue has committed")
    }

    /// The shipped backfill budget with NO post-reclaim grace and a large
    /// teardown reserve. The two numbers disagree on purpose: only an
    /// implementation that spends the grace stops at once.
    private static let noGraceLargeReserve = BackgroundGrantBudget(
        designGrant: BackgroundGrantBudget.backfillProcessing.designGrant,
        teardownReserve: .seconds(60),
        minimumCheckpointBudget: BackgroundGrantBudget.backfillProcessing.minimumCheckpointBudget,
        minimumDrainCheckpointBudget: BackgroundGrantBudget.backfillProcessing.minimumDrainCheckpointBudget,
        expirationSettleGrace: .zero,
        provenance: .assumed
    )

    /// PerfGate'd on measurement, playhead-o89d. The comment below is right
    /// that 20 s is a BOUND rather than a measurement — but a bound whose two
    /// arms are "about 0 s" and "60 s" only separates them while the box is
    /// quiet. Measured 2026-08-13: this passes 5/5 in a 443-test scoped run
    /// and has failed 100 % of recorded full-plan runs, where the correct
    /// path's "about 0 s" is itself past 20 s. The discrimination the test
    /// exists for survives only in the serial pass, so that is where it runs.
    @Test("the expiration wait spends the post-reclaim grace, not the teardown reserve",
          .timeLimit(.minutes(2)),
          .enabled(if: PerfGate.runsMeasurementTests, "perf pass only — see playhead-zx0l"))
    func expirationWaitIsBoundedByTheGraceNotTheReserve() async throws {
        // playhead-lmrx review round 2. `teardownReserve` is wall clock carved
        // OUT of `designGrant`; by the time this handler runs the OS has taken
        // the grant back, so there is none left to carve from.
        // `remainingTeardownReserve` cannot see that — it returns the reserve's
        // full length at the instant of reclaim, identically for a window that
        // ran to its 295 s limit (57 % of them, measured) and one reclaimed at
        // t+5 s. iOS terminates an app that does not complete its task promptly
        // after `expirationHandler` and penalises its future scheduling, which
        // is exactly the resource this bead exists to spend well, so the
        // overrun costs windows to buy a rescue it may not get.
        //
        // The budget below sets the grace to zero and the reserve to 60 s, and
        // the job it cancels NEVER settles (its decode is parked on a gate this
        // test does not open). Correct code skips the wait entirely and
        // completes the task at once; an implementation that reached for the
        // reserve would poll for a minute first. The threshold is 20 s — 3x
        // over the noise on the correct path and 3x under the wrong one — so
        // this is a bound, not a measurement.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        downloads.cachedURLs["ep-never-settles"] = URL(fileURLWithPath: "/tmp/ep-never-settles.m4a")
        let job = makeAnalysisJob(
            jobId: "never-settles",
            jobType: "preAnalysis",
            episodeId: "ep-never-settles",
            analysisAssetId: "asset-never-settles",
            workKey: "fp-never-settles:1:preAnalysis",
            sourceFingerprint: "fp-never-settles",
            priority: 10,
            desiredCoverageSec: 90,
            state: "queued"
        )
        try await store.insertJob(job)

        let gate = UnwindGate()
        defer { gate.open() }
        let scheduler = makeScheduler(
            store: store,
            audio: GatedUnwindAudioStub(gate: gate),
            downloads: downloads
        )
        let coordinator = StubAnalysisCoordinator()
        coordinator.runPendingBackfillDuration = .seconds(30)
        let bps = BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider()
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: makeReconciler(store: store))

        let task = StubBackgroundTask()
        let workTask = Task {
            await bps.handleBackfillTask(task, budget: Self.noGraceLargeReserve)
        }
        defer { workTask.cancel() }
        await task.awaitExpirationHandlerInstalled()

        // The job must be genuinely in flight, or the cancel names nothing and
        // there is no wait to bound — the test would pass vacuously.
        var spins = 0
        while await !scheduler.hasCurrentRunningTaskForTesting() {
            spins += 1
            try #require(spins < 1000, "the seeded job never entered decode")
            try await Task.sleep(for: .milliseconds(20))
        }

        let expiredAt = ContinuousClock.now
        task.simulateExpiration()
        await task.awaitCompletion()
        let elapsed = expiredAt.duration(to: ContinuousClock.now)

        #expect(elapsed < .seconds(20),
                """
                With a zero post-reclaim grace the handler must complete the \
                task at once. Taking \(elapsed) means it spent the 60 s \
                teardown reserve instead — a budget carved out of a grant the \
                OS has already reclaimed, waiting for a job that never settles, \
                while iOS waits to terminate the process for not completing.
                """)
        // And the cancel really was aimed at something, so the wait it skipped
        // was a real one.
        #expect(await scheduler.inFlightJobIdsForTesting().contains("never-settles"),
                "the job must still be unwinding — otherwise there was nothing to wait for")
    }
}

// MARK: - A cancel is aimed at jobs, not at a slot (review round 3)

@Suite("playhead-lmrx: a cancel is aimed at jobs, not at a slot")
struct CancelAimIdentityTests {

    /// Sleeps in `decode` and OBSERVES CANCELLATION, which is the whole point:
    /// a job the cancel reached leaves promptly, a job it missed sits here.
    /// The finite sleep is teardown insurance, not the mechanism — on the
    /// failing path the assertion has already fired by the time it elapses.
    private final class CancellableSleepAudioStub: AnalysisAudioProviding, @unchecked Sendable {
        func decode(
            fileURL: LocalAudioURL,
            episodeID: String,
            shardDuration: TimeInterval
        ) async throws -> [AnalysisShard] {
            try await Task.sleep(for: .seconds(20))
            return []
        }
    }

    private func makeScheduler(
        store: AnalysisStore,
        downloads: StubDownloadProvider,
        audio: any AnalysisAudioProviding = CancellableSleepAudioStub()
    ) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: audio,
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(
                snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
            ),
            downloadManager: downloads,
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider()
        )
    }

    /// Now-lane (`priority >= 20`), because `nowCap` is 2 and every other lane
    /// caps at 1 — a test that seeds `.soon` jobs can never get two in flight
    /// and would pass against the single-slot code it is meant to kill.
    @discardableResult
    private func insertNowLaneJob(
        store: AnalysisStore,
        downloads: StubDownloadProvider,
        jobId: String,
        priority: Int,
        createdAt: Double,
        attemptCount: Int = 0
    ) async throws -> AnalysisJob {
        let episodeId = "ep-\(jobId)"
        downloads.cachedURLs[episodeId] = URL(fileURLWithPath: "/tmp/\(episodeId).m4a")
        let job = makeAnalysisJob(
            jobId: jobId,
            jobType: "preAnalysis",
            episodeId: episodeId,
            analysisAssetId: "asset-\(jobId)",
            workKey: "fp-\(jobId):1:preAnalysis",
            sourceFingerprint: "fp-\(jobId)",
            priority: priority,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: attemptCount,
            createdAt: createdAt
        )
        try await store.insertJob(job)
        return job
    }

    /// Puts exactly two jobs in flight at once and returns the two dispatch
    /// tasks. `fetchNextEligibleJob` skips leased rows, so the second dispatch
    /// can only see the second job after the first has committed its lease —
    /// which is why this is staged rather than fired in parallel.
    private func driveTwoInFlight(
        scheduler: AnalysisWorkScheduler,
        first: String,
        second: String
    ) async throws -> [Task<Bool, Never>] {
        let a = Task { await scheduler.processNextDispatchableJobForTesting() }
        var spins = 0
        while await !scheduler.inFlightJobIdsForTesting().contains(first) {
            spins += 1
            try #require(spins < 1000, "the first job never entered processJob")
            try await Task.sleep(for: .milliseconds(20))
        }
        let b = Task { await scheduler.processNextDispatchableJobForTesting() }
        spins = 0
        while await scheduler.inFlightJobIdsForTesting() != [first, second] {
            spins += 1
            let seen = await scheduler.inFlightJobIdsForTesting()
            try #require(spins < 1000, "the two dispatches never overlapped; in flight: \(seen.sorted())")
            try await Task.sleep(for: .milliseconds(20))
        }
        // BOTH runners started, so the `runTask` half of the aim is exercised
        // and not only the `cancelRequested` half.
        //
        // playhead-lmrx (review round 8): ASKED BY IDENTITY, AND IT WAS NOT.
        // This loop spun on `hasCurrentRunningTaskForTesting()`, which reads
        // `runningJobs.values.contains { $0.runTask != nil }` — satisfied by
        // EITHER job. `first` is dispatched a whole `processJob` prologue ahead
        // of `second`, so it is normally already running when `second` has only
        // just been inserted into the registry, and the loop fell through with
        // `second` still between its insert and its `runTask` assignment
        // (several DB suspension points later). Round 7 fixed exactly this
        // shape at `recoveryExpiryShutsTheDispatchDoor` and did not look at the
        // helper that drives three tests.
        //
        // The consequence is not a weakened assertion, it is a RED on correct
        // code: a cancel landing in that gap is caught by `cancelRequested`, so
        // the job takes the CANCEL-RACE arm rather than the cancel-catch arm,
        // and that arm's `updateJobState(jobId:state: "queued")` binds
        // `lastErrorCode = NULL`. `siblingsDoNotStealEachOthersCancelCause`
        // then reads NULL where it requires
        // `backgroundWindowExpiredErrorCode`, and an intermittently-red rail is
        // what round 6 refused to accept.
        spins = 0
        while await scheduler.runnerStartedJobIdsForTesting() != [first, second] {
            spins += 1
            let started = await scheduler.runnerStartedJobIdsForTesting()
            try #require(spins < 1000,
                         "both runners never started; started: \(started.sorted())")
            try await Task.sleep(for: .milliseconds(20))
        }
        return [a, b]
    }

    @Test("the cancel reaches every job it names", .timeLimit(.minutes(2)))
    func cancelReachesEveryJobItNames() async throws {
        // THE DEFECT, stated as the diagnostic question. `cancelCurrentJob`
        // returned `inFlightJobIds` as "what this cancel was aimed at" while
        // cancelling only `currentRunningTask` — a single slot with no owner id,
        // overwritten by the second concurrent `processJob`. What would the
        // returned set have read if the cancel had reached nothing at all? The
        // same ids. The caller then waits on a job nobody asked to stop, times
        // out with certainty, and calls `setTaskCompleted` over it.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloads: downloads)
        let t0 = Date().timeIntervalSince1970
        try await insertNowLaneJob(store: store, downloads: downloads, jobId: "aim-a", priority: 21, createdAt: t0 - 10)
        try await insertNowLaneJob(store: store, downloads: downloads, jobId: "aim-b", priority: 20, createdAt: t0)

        let dispatches = try await driveTwoInFlight(scheduler: scheduler, first: "aim-a", second: "aim-b")

        let cancelled = await scheduler.cancelCurrentJob(cause: .taskExpired)
        #expect(cancelled == ["aim-a", "aim-b"],
                "the return value is the caller's whole picture of what it must wait for")
        #expect(await scheduler.cancelRequestedJobIdsForTesting() == cancelled,
                """
                What the cancel CLAIMS and what it REACHED must be the same set. \
                These were allowed to disagree, and the claim was the one the \
                caller acted on.
                """)

        // The load-bearing assertion. A job the cancel missed is parked in a
        // 20 s sleep, so this budget cannot be met by luck.
        #expect(await scheduler.awaitJobsSettled(cancelled, within: .seconds(10)),
                """
                Every named job must actually unwind. A missed one makes the \
                handler's settle wait a certain timeout and leaves that job \
                running, unrequeued, into setTaskCompleted — the stranding this \
                bead exists to remove.
                """)

        for jobId in ["aim-a", "aim-b"] {
            let after = try #require(try await store.fetchJob(byId: jobId))
            #expect(after.state == "queued", "\(jobId) must be resumable, not left leased at `running`")
        }
        for dispatch in dispatches { _ = await dispatch.value }
    }

    @Test("each cancelled job keeps its own cause, so neither spends an attempt",
          .timeLimit(.minutes(2)))
    func siblingsDoNotStealEachOthersCancelCause() async throws {
        // THE SAME DEFECT ONE LAYER IN, and reachable only because the cancel
        // now genuinely reaches both jobs. The cancel-catch arm read
        // `pendingCancelCause ?? .pipelineError` and then nil'd it
        // unconditionally: a DESTRUCTIVE read of a single global slot. The first
        // job to unwind took `.taskExpired`; the second read `nil`, fell back to
        // `.pipelineError`, took the ATTEMPT-SPENDING arm, and at
        // `attemptCount == 4` superseded with `nextEligibleAt: nil` — a row that
        // never comes back, because `workKey` is UNIQUE and `insertJob` is
        // `INSERT OR IGNORE`.
        //
        // Seeded at 4 on BOTH jobs so either ordering is fatal: whichever loses
        // the race supersedes, and the test cannot pass by getting lucky.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloads: downloads)
        let t0 = Date().timeIntervalSince1970
        try await insertNowLaneJob(store: store, downloads: downloads,
                                   jobId: "cause-a", priority: 21, createdAt: t0 - 10, attemptCount: 4)
        try await insertNowLaneJob(store: store, downloads: downloads,
                                   jobId: "cause-b", priority: 20, createdAt: t0, attemptCount: 4)

        let dispatches = try await driveTwoInFlight(scheduler: scheduler, first: "cause-a", second: "cause-b")

        let cancelled = await scheduler.cancelCurrentJob(cause: .taskExpired)
        #expect(await scheduler.awaitJobsSettled(cancelled, within: .seconds(10)))
        for dispatch in dispatches { _ = await dispatch.value }

        for jobId in ["cause-a", "cause-b"] {
            let after = try #require(try await store.fetchJob(byId: jobId))
            #expect(after.state == "queued",
                    """
                    \(jobId) was superseded by an expiry. One cancel, one cause, \
                    two jobs — and the loser of that race is abandoned forever.
                    """)
            #expect(after.attemptCount == 4,
                    "\(jobId): an OS-reclaimed window is evidence about the window, not about the job")
            #expect(after.lastErrorCode == AnalysisWorkScheduler.backgroundWindowExpiredErrorCode,
                    "\(jobId): the requeue must carry the expiry's own error code, not a pipeline error")
        }
    }

    @Test("a job finishing does not cancel a sibling's lease heartbeat",
          .timeLimit(.minutes(2)))
    func siblingLeaseHeartbeatSurvivesAnotherJobsExit() async throws {
        // The third consequence of the single slot, and the quietest.
        // `leaseRenewalTask` was one slot too, so B's dispatch overwrote A's
        // heartbeat handle and A's `defer` then cancelled B's — leaving a LIVE
        // job with no lease renewal, to be reclaimed out from under itself by
        // the reaper, while A's own heartbeat ran on forever.
        //
        // Asserted structurally rather than by waiting out a renewal interval:
        // after one job leaves, the other must still hold a heartbeat and its
        // lease must still be its own.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloads: downloads)
        let t0 = Date().timeIntervalSince1970
        try await insertNowLaneJob(store: store, downloads: downloads, jobId: "beat-a", priority: 21, createdAt: t0 - 10)
        try await insertNowLaneJob(store: store, downloads: downloads, jobId: "beat-b", priority: 20, createdAt: t0)

        let dispatches = try await driveTwoInFlight(scheduler: scheduler, first: "beat-a", second: "beat-b")

        // Retire only A's episode: its job is cancelled by identity and leaves,
        // running its `defer` while B is still in flight.
        await scheduler.retireDownloadAnalysis(episodeId: "ep-beat-a", downloadId: "dl-1")
        var spins = 0
        while await scheduler.inFlightJobIdsForTesting().contains("beat-a") {
            spins += 1
            try #require(spins < 1000, "the retired job never unwound")
            try await Task.sleep(for: .milliseconds(20))
        }

        #expect(await scheduler.inFlightJobIdsForTesting() == ["beat-b"],
                "the surviving job must still be in flight — a sibling leaving is not its business")
        // playhead-lmrx (review round 4): THE ASSERTION THAT ACTUALLY SEES THE
        // DEFECT, and the reason the two below cannot.
        //
        // `hasCurrentRunningTaskForTesting()` is `runTask != nil`. Ask the
        // diagnostic question of it: what would it read if beat-a's `defer` HAD
        // killed beat-b's runner? `Task.cancel()` does not clear the handle, so
        // it reads exactly the same `true`. It names "a runner handle exists"
        // and was being read as "the runner is alive" — the standing defect
        // class, in the observable rather than in the code.
        //
        // The store read below is no better on its own: beat-b reverts to
        // `queued` only after its cancel arm has run several actor hops and two
        // SQLite writes, so against the single-slot defect this test's outcome
        // was a coin flip on whether that finished inside the 20 ms poll gap —
        // a rail on a coin flip is worse than no rail.
        //
        // Cancellation is observable the instant it is requested, and the pair
        // of assertions is total: either beat-b is still in the registry, in
        // which case its cancelled runner is visible here, or it has already
        // unwound, in which case the in-flight assertion above has fired.
        #expect(await scheduler.cancelledRunTaskJobIdsForTesting().isEmpty,
                """
                beat-b's runner must not have been CANCELLED. The single slot \
                meant beat-a's `defer` cancelled whatever the slot then held, \
                which is beat-b — a job finishing NORMALLY killing an unrelated \
                one.
                """)
        #expect(await scheduler.hasCurrentRunningTaskForTesting(),
                "and beat-b's runner handle must still be there at all")
        let survivor = try #require(try await store.fetchJob(byId: "beat-b"))
        #expect(survivor.state == "running" && survivor.leaseOwner != nil,
                "and it must still own its lease")

        await scheduler.cancelCurrentJob(cause: .taskExpired)
        for dispatch in dispatches { _ = await dispatch.value }
    }

    @Test("dispatch is closed for a teardown, and re-opens by itself",
          .timeLimit(.minutes(2)))
    func dispatchClosesForTeardownAndReopens() async throws {
        // F7: nothing quiesced `runLoop()`, whose 5 s poll can dispatch a fresh
        // job AFTER the handler's cancel has gone past — aimed at by nothing,
        // absent from the set the handler waits on, live at `setTaskCompleted`.
        //
        // The re-open is asserted because the alternative design is a starvation
        // bug: this scheduler's only external re-openers are called solely by
        // the BGTask handlers, so a "closed" flag that leaked on any early
        // return would leave a foregrounded app with a scheduler that never
        // dispatches again.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloads: downloads)
        try await insertNowLaneJob(store: store, downloads: downloads,
                                   jobId: "closed-door", priority: 21,
                                   createdAt: Date().timeIntervalSince1970)

        await scheduler.closeDispatchForTeardown(lasting: .milliseconds(250))
        // Extending is a `max`: a shorter close must not re-open a longer one.
        await scheduler.closeDispatchForTeardown(lasting: .milliseconds(1))
        #expect(await scheduler.isDispatchClosedForTesting())
        #expect(await scheduler.processNextDispatchableJobForTesting() == false,
                "no job may START while a handler is tearing its grant down")
        // playhead-lmrx (review round 4): the door's PLACEMENT, not just its
        // existence. It sits above `canAdmit`'s T0 playback bypass, and nothing
        // pinned that — every dispatch this test drives is `preAnalysis`, so a
        // later reader moving the check below the bypass (it is, after all, in a
        // method named for lane capacity) keeps this test green while restoring
        // the stranding for the one job class where it is worst: a T0 job left
        // at `running` with a live lease when the process suspends. "The
        // process may be suspended within seconds" is not a lane cap, and it is
        // as true of a playback job as of any other.
        let playbackJob = makeAnalysisJob(
            jobId: "closed-door-playback",
            jobType: "playback",
            episodeId: "ep-closed-door-playback",
            analysisAssetId: "asset-closed-door-playback",
            workKey: "fp-closed-door-playback:1:playback",
            sourceFingerprint: "fp-closed-door-playback",
            priority: 21,
            desiredCoverageSec: 90,
            state: "queued",
            attemptCount: 0,
            createdAt: Date().timeIntervalSince1970
        )
        #expect(await scheduler.canAdmit(job: playbackJob) == false,
                "the T0 playback bypass skips the LANE CAP, not the teardown door")
        let untouched = try #require(try await store.fetchJob(byId: "closed-door"))
        #expect(untouched.state == "queued", "and the refused job must be left exactly where it was")

        // Deliberately far past the 250 ms close, so oversleeping under load can
        // only help. The claim is that the door opens WITHOUT anyone opening it.
        try await Task.sleep(for: .seconds(2))
        #expect(await scheduler.isDispatchClosedForTesting() == false)
        // Dispatched in a Task and observed by its arrival in flight, rather
        // than awaited: the pass runs a whole job and this stub's decode sleeps.
        let reopened = Task { await scheduler.processNextDispatchableJobForTesting() }
        var spins = 0
        while await scheduler.inFlightJobIdsForTesting().isEmpty {
            spins += 1
            try #require(spins < 500,
                         """
                         An expired close must let work through again. A close that \
                         has to be cleared is one an early return can leak, and \
                         nothing outside a BGTask handler ever calls the re-openers.
                         """)
            try await Task.sleep(for: .milliseconds(20))
        }
        await scheduler.cancelCurrentJob(cause: .taskExpired)
        _ = await reopened.value
    }

    /// A budget whose `workBudget` clamps to zero, so `workDeadline(from:)`
    /// returns the grant's own start and every deadline test reads "already
    /// elapsed" — the state a real window is in at t+219 s, without waiting
    /// 219 s for it. The reserve is real so the settle has something to spend.
    private static let deadlineAlreadyPast = BackgroundGrantBudget(
        designGrant: .zero,
        teardownReserve: .seconds(30),
        minimumCheckpointBudget: .zero,
        minimumDrainCheckpointBudget: .zero,
        expirationSettleGrace: .seconds(30),
        provenance: .assumed
    )

    @Test("recovery's work-deadline return does not strand the job it leaves running",
          .timeLimit(.minutes(2)))
    func recoveryNormalReturnRequeuesTheInFlightJob() async throws {
        // F5: THE HOLE THIS BEAD'S OWN CHANGE OPENED, in the handler nobody
        // re-read after opening it.
        //
        // LX21 closed exactly this on backfill. Recovery's drain moved from
        // `now + 300 s` read AT THE CALL — unreachable inside a ~295 s grant, so
        // the OS expiry always drove teardown — to `grantStart + 219 s`, which a
        // normal grant reaches with `Task.isCancelled` still FALSE while the
        // long-lived `runLoop()` this handler started keeps dispatching on its
        // own 5 s poll. The handler then wrote `finishRun` and
        // `markComplete(success: true)` with no cancel and no settle. The
        // in-file justification for the budget change argued only the EXPIRY
        // asymmetry and never mentioned the exit it created.
        //
        // Driven with no timing dependence, exactly as the backfill twin is: the
        // job is put in flight by another driver first, and its lease then makes
        // it invisible to `fetchNextEligibleJob`, so the handler's own drain
        // dispatches nothing and returns at once — reproducing "the handler
        // reaches its normal return while a job it did not dispatch is running".
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloads: downloads)
        try await insertNowLaneJob(store: store, downloads: downloads,
                                   jobId: "recovery-strand", priority: 21,
                                   createdAt: Date().timeIntervalSince1970)

        let dispatch = Task { await scheduler.processNextDispatchableJobForTesting() }
        var spins = 0
        while await !scheduler.hasCurrentRunningTaskForTesting() {
            spins += 1
            try #require(spins < 1000, "the seeded job never entered decode")
            try await Task.sleep(for: .milliseconds(20))
        }

        let bps = BackgroundProcessingService(
            coordinator: StubAnalysisCoordinator(),
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider()
        )
        let reconciler = AnalysisJobReconciler(
            store: store,
            downloadManager: StubDownloadProvider(),
            capabilitiesService: StubCapabilitiesProvider(
                snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
            )
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: reconciler)

        let task = StubBackgroundTask()
        await bps.handlePreAnalysisRecovery(task, budget: Self.deadlineAlreadyPast)
        await task.awaitCompletion()

        let after = try #require(try await store.fetchJob(byId: "recovery-strand"))
        #expect(after.state == "queued",
                """
                Recovery's work-deadline return must cancel and requeue what it \
                leaves running, exactly as backfill's does. Finding this row \
                still `running` means the handler called setTaskCompleted over a \
                live job — no checkpoint, no requeue, a stale lease for the \
                reaper, and no expirationHandler left to rescue it.
                """)
        #expect(after.lastErrorCode == AnalysisWorkScheduler.backgroundWindowExpiredErrorCode)
        #expect(after.attemptCount == 0,
                "the grant ending is not the job's fault on this path either")
        // playhead-lmrx (review round 5): recovery's door wiring, for the same
        // reason as backfill's — three handler call sites, none of them
        // observed by any test until this round.
        #expect(await scheduler.isDispatchClosedForTesting(),
                "recovery's work-deadline teardown must shut the dispatch door before it cancels")
        _ = await dispatch.value
    }

    @Test("recovery's teardown cancelled mid-settle leaves the ending to the expiration handler",
          .timeLimit(.minutes(2)))
    func recoveryTeardownCancelledMidSettleDefersToTheExpiry() async throws {
        // playhead-lmrx (review round 7): the backfill twin's guard, one
        // handler over. Read
        // `backfillTeardownCancelledMidSettleDefersToTheExpiry` for the full
        // argument — the defect, the two seams that make it a fact rather than
        // a race, and why round 6 declined to build it.
        //
        // Recovery's version is if anything nastier, because its expiration
        // handler has NO settle wait: the moment the work task loses that guard
        // it wins the sprint to `setTaskCompleted` by a wider margin, and the
        // run is recorded `recovered_work` over a job that is still running.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        let unwind = UnwindGate()
        defer { unwind.open() }
        let scheduler = makeScheduler(
            store: store,
            downloads: downloads,
            audio: GatedUnwindAudioStub(gate: unwind)
        )
        try await insertNowLaneJob(store: store, downloads: downloads,
                                   jobId: "recovery-mid-settle", priority: 21,
                                   createdAt: Date().timeIntervalSince1970)

        let dispatch = Task { await scheduler.processNextDispatchableJobForTesting() }
        var spins = 0
        while await !scheduler.hasCurrentRunningTaskForTesting() {
            spins += 1
            try #require(spins < 1000, "the seeded job never entered decode")
            try await Task.sleep(for: .milliseconds(20))
        }

        let expiryGate = UnwindGate()
        defer { expiryGate.open() }
        let ledger = GatedRunLedger(parkOn: .expired, gate: expiryGate)
        let bps = BackgroundProcessingService(
            coordinator: StubAnalysisCoordinator(),
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider(),
            runLedger: ledger
        )
        let reconciler = AnalysisJobReconciler(
            store: store,
            downloadManager: StubDownloadProvider(),
            capabilitiesService: StubCapabilitiesProvider(
                snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
            )
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: reconciler)

        let task = StubBackgroundTask()
        await bps.handlePreAnalysisRecovery(task, budget: Self.deadlineAlreadyPast)
        let workTask = try #require(
            await bps.workTaskForTesting(task),
            "the handler must leave its work task registered until something completes the BGTask"
        )

        await ledger.finishes.wait(for: 1)
        task.simulateExpiration()
        await ledger.reachedPark.wait(for: 1)

        await workTask.value

        #expect(task.completedSuccess == nil,
                """
                Recovery's work task must NOT complete the BGTask after the OS \
                cancelled it mid-teardown. `true` here means it fell through \
                the settle wait into `markComplete(success: true)`, telling iOS \
                the window finished over a live job — with recovery's expiry \
                carrying no settle wait of its own, there is nothing behind it \
                to make the requeue durable.
                """)
        #expect(task.setTaskCompletedCallCount == 0,
                "and it must not have touched setTaskCompleted at all")

        unwind.open()
        expiryGate.open()
        await task.awaitCompletion()
        #expect(task.completedSuccess == false,
                "the expiration handler owns the ending, and records it as one")
        // NOT `recordedOutcomes.contains(.expired)` — that is implied by
        // `reachedPark.wait(for: 1)` above, since `GatedRunLedger` records the
        // outcome before incrementing the park counter, so it could not fail
        // (adversarial pass). What is NOT implied is the ORDER: the work task's
        // own terminal write lands first and the expiry's second, which is what
        // makes the expiry the row that survives.
        #expect(ledger.recordedOutcomes.last == .expired,
                "the expiry must be the LAST terminal write, so it is the outcome that stands")
        _ = await dispatch.value
    }

    @Test("recovery's EXPIRY shuts the dispatch door too", .timeLimit(.minutes(2)))
    func recoveryExpiryShutsTheDispatchDoor() async throws {
        // playhead-lmrx (review round 6): THE FOURTH DOOR CALL SITE.
        //
        // `closeDispatchForTeardown` has four call sites in
        // `BackgroundProcessingService` — backfill's work-deadline return
        // (:1619), backfill's expiry (:1739), recovery's work-deadline return
        // (:2386) and recovery's EXPIRY (:2500). Round 5 counted three, pinned
        // three, and this is the one it did not count: delete :2500 and nothing
        // in the suite goes red, while `runLoop()`'s 5 s poll is free again to
        // start a job between the cancel one line below it and
        // `setTaskCompleted` one line below that — aimed at by nothing, live at
        // suspension. That is LX27's defect, on the path the round-3 fix added
        // the door to.
        //
        // BEHAVIOURAL, not a source canary: the expiry is reached with the job
        // genuinely in flight, so the door's state at `markComplete` is a fact
        // about the handler rather than about the file.
        //
        // The drain is what parks this handler. Seeded with a now-lane job and
        // the SHIPPED budget (219 s of work, 60 s floor), `drainEligible`
        // dispatches it and awaits the runner, so the expiration below fires
        // while the work task is genuinely live — which is the state 5 of
        // recovery's 97 recorded runs ended in.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        // playhead-lmrx (review round 7): GATED, not merely slow — see the
        // cancel assertion at the bottom for why the difference is load-bearing.
        let unwind = UnwindGate()
        defer { unwind.open() }
        let scheduler = makeScheduler(
            store: store,
            downloads: downloads,
            audio: GatedUnwindAudioStub(gate: unwind)
        )
        try await insertNowLaneJob(store: store, downloads: downloads,
                                   jobId: "recovery-expiry-door", priority: 21,
                                   createdAt: Date().timeIntervalSince1970)

        let bps = BackgroundProcessingService(
            coordinator: StubAnalysisCoordinator(),
            capabilitiesService: CapabilitiesService(),
            taskScheduler: StubTaskScheduler(),
            batteryProvider: StubBatteryProvider()
        )
        let reconciler = AnalysisJobReconciler(
            store: store,
            downloadManager: StubDownloadProvider(),
            capabilitiesService: StubCapabilitiesProvider(
                snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
            )
        )
        await bps.setPreAnalysisServices(scheduler: scheduler, reconciler: reconciler)

        let task = StubBackgroundTask()
        // The handler installs its expiration handler and returns; the work
        // task it left behind is what parks in the drain.
        await bps.handlePreAnalysisRecovery(task)
        let workTask = await bps.workTaskForTesting(task)
        var spins = 0
        // playhead-lmrx (review round 7, adversarial pass): SPIN TO THE RUNNER,
        // NOT TO REGISTRY MEMBERSHIP — the two are not the same instant, and
        // the gap is exactly where this test's cancel assertions would go red
        // against CORRECT code.
        //
        // `runningJobs[job.jobId]` is created at the top of `processJob`, but
        // `runTask` is not assigned until after `cachedCanonicalFingerprintStatus`
        // and `resolveAnalysisAssetId` — two DB suspension points later. A
        // cancel landing in that window marks `cancelRequested` with no runner
        // to cancel, `processJob` then fails its cancel-race guard and RETURNS,
        // and the `defer` removes the entry. Both assertions at the bottom
        // would then read an empty set on unmutated code. The `UnwindGate` does
        // not cover this: it only bites inside `decode`, which is downstream of
        // the `runTask` assignment.
        //
        // `hasCurrentRunningTaskForTesting()` is `runTask != nil`, so once it
        // holds the job is parked in the gated `decode` and cannot leave the
        // registry. Both new tests in this file already spin this way; this one
        // did not, and that was the defect.
        // Two separate `await`s rather than one over `&&`: the operator takes
        // its right operand as an autoclosure, which is a synchronous
        // nonisolated context, so a single leading `await` does not cover it.
        while true {
            let runnerStarted = await scheduler.hasCurrentRunningTaskForTesting()
            let isOurs = await scheduler.inFlightJobIdsForTesting()
                .contains("recovery-expiry-door")
            if runnerStarted && isOurs { break }
            spins += 1
            try #require(spins < 1000, "recovery's own drain never got the seeded job into decode")
            try await Task.sleep(for: .milliseconds(20))
        }

        task.simulateExpiration()
        await task.awaitCompletion()

        #expect(await scheduler.isDispatchClosedForTesting(),
                """
                Recovery's expiration teardown must shut the dispatch door \
                before it cancels. Without it the cancel is a one-shot edge \
                against a run loop that is still polling every 5 s, and a job \
                it starts before `setTaskCompleted` is named by nothing and \
                live when iOS suspends the process.
                """)
        // playhead-lmrx (review round 7): AND THE CANCEL ONE LINE BELOW THE
        // DOOR, which round 6 left unasserted.
        //
        // `cancelCurrentJob` at this call site is pre-existing (playhead-1nl6)
        // and its EFFECT was pinned nowhere: round 6 drove the path for the
        // first time and deliberately stopped at the door, because the effect
        // it reached for — `state == 'queued'` — lands several actor hops and
        // two SQLite writes after the cancel and is not ordered against
        // `markComplete`. Round 6 was right to decline that assertion. It is
        // the wrong OBSERVABLE, not an unpinnable claim.
        //
        // Cancellation is requested SYNCHRONOUSLY inside `cancelCurrentJob`,
        // one statement before `markComplete`, so by the time `awaitCompletion`
        // above has returned the marks are already set. The only thing that
        // could hide them is the job leaving the registry first — which the
        // gate makes impossible. Deleting the cancel therefore reddens this
        // deterministically, with no wait and no race.
        //
        // Both halves of the aim are checked because they cover disjoint
        // stretches of `processJob`: `cancelRequested` reaches a job dispatched
        // but not yet past its cancel-race check, `runTask.isCancelled` a job
        // whose runner has started.
        #expect(await scheduler.cancelRequestedJobIdsForTesting().contains("recovery-expiry-door"),
                """
                Recovery's expiration teardown must CANCEL the job it found in \
                flight, not merely shut the door on new ones. Without it the \
                live job runs on unaware into `setTaskCompleted` and process \
                suspension: no checkpoint, no requeue, and a leased `running` \
                row left for mk6z's reaper.
                """)
        #expect(await scheduler.cancelledRunTaskJobIdsForTesting().contains("recovery-expiry-door"),
                "and the cancel must reach the RUNNER, not only the registry mark")
        #expect(task.completedSuccess == false,
                "and it must be the EXPIRATION that ended this run, not the work task's normal return")
        unwind.open()
        await workTask?.value
    }
}

// MARK: - Shared teardown harness (file scope, review round 7)
//
// `UnwindGate` and `GatedUnwindAudioStub` were nested inside
// `BackfillExpiryDurabilityTests`. They moved out unchanged because the
// recovery handler's teardown needs the same two seams, and duplicating them
// per suite is how two copies of one harness start disagreeing about what
// "held in flight" means.

/// A one-shot gate the TEST opens, so "the cancelled job is still
/// unwinding" is a fact the test controls rather than a duration it hopes
/// for. Awaiting a continuation somebody else resumes ignores task
/// cancellation by construction, which is exactly the state under test.
///
/// The safety timer is why this is not simply a sleep in disguise: on the
/// correct ordering the gate is opened by the ledger write itself, so the
/// test is insensitive to how long the handler takes to get there. It is
/// only the WRONG ordering — where the write is behind the wait and the
/// wait is behind this gate — that ever reaches the timer, and it then
/// observes a settled scheduler and fails, which is the point.
private final class UnwindGate: @unchecked Sendable {
    private let lock = NSLock()
    private var waiters: [CheckedContinuation<Void, Never>] = []
    private var isOpen = false

    func wait() async {
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            lock.lock()
            if isOpen {
                lock.unlock()
                continuation.resume()
                return
            }
            waiters.append(continuation)
            lock.unlock()
        }
    }

    /// Idempotent, and non-async so `NSLock` is taken outside an async
    /// context.
    func open() {
        lock.lock()
        if isOpen {
            lock.unlock()
            return
        }
        isOpen = true
        let pending = waiters
        waiters.removeAll()
        lock.unlock()
        for continuation in pending { continuation.resume() }
    }

    /// Deadlock insurance: opens the gate after `seconds` no matter what,
    /// so a test that never reaches its own `open()` fails on an assertion
    /// rather than hanging.
    ///
    /// **Arm this as LATE as possible.** An earlier draft armed it at the
    /// top of the test, so setup and the spin-to-in-flight loop spent the
    /// same budget the handler's teardown needs — and it went red under the
    /// mutation battery's suite load twice. The window that has to fit is
    /// only "expiration fired" → "the ledger row was written"; everything
    /// before that is unbounded and must not be charged here.
    func openAfter(seconds: TimeInterval) {
        DispatchQueue.global().asyncAfter(deadline: .now() + seconds) { [self] in
            open()
        }
    }
}

/// Blocks in `decode` on an ``UnwindGate``, ignoring cancellation until the
/// gate opens.
private final class GatedUnwindAudioStub: AnalysisAudioProviding, @unchecked Sendable {
    private let gate: UnwindGate
    init(gate: UnwindGate) { self.gate = gate }
    func decode(
        fileURL: LocalAudioURL,
        episodeID: String,
        shardDuration: TimeInterval
    ) async throws -> [AnalysisShard] {
        await gate.wait()
        try Task.checkCancellation()
        return []
    }
}

/// playhead-lmrx (review round 7): a run ledger that PARKS its caller inside
/// `finishRun` for one nominated outcome, and signals every terminal write.
///
/// **This is what makes the teardown `guard !Task.isCancelled` pinnable at
/// all.** That guard's correct behaviour is a bare `return` — no row, no state,
/// nothing. Its only observable consequence is negative: the work task does not
/// call `setTaskCompleted`, leaving the ending to the expiration handler. Ask
/// that question directly and you are timing a sprint between two tasks to a
/// first-writer-wins flag, which is why review round 6 declined to build the
/// rail: an intermittently-surviving rail is worse than none.
///
/// Parking the EXPIRATION handler at its own `finishRun` — which is upstream of
/// its `markComplete` on both handlers — removes the other runner from the
/// race. From that moment the work task is the ONLY party that can complete the
/// task, so `completedSuccess` is a fact about the guard.
private final class GatedRunLedger: BackgroundTaskRunLedger, @unchecked Sendable {
    private let parkOn: BackgroundTaskRunOutcome
    private let gate: UnwindGate
    private let lock = NSLock()
    private var seen: [BackgroundTaskRunOutcome] = []
    /// Fires when a `finishRun` for ``parkOn`` has ARRIVED — i.e. the parked
    /// caller is committed and cannot reach anything downstream of the write.
    let reachedPark = TestEventCounter()
    /// Fires on every terminal write, parked or not.
    let finishes = TestEventCounter()

    init(parkOn: BackgroundTaskRunOutcome, gate: UnwindGate) {
        self.parkOn = parkOn
        self.gate = gate
    }

    var recordedOutcomes: [BackgroundTaskRunOutcome] {
        lock.lock(); defer { lock.unlock() }
        return seen
    }

    func startRun(
        entryPoint: BackgroundTaskRunEntryPoint,
        taskIdentifier: String,
        taskInstanceID: String?,
        scenePhase: String?
    ) async -> String { UUID().uuidString }

    func recordRunStart(
        runId: String,
        entryPoint: BackgroundTaskRunEntryPoint,
        taskIdentifier: String,
        taskInstanceID: String?,
        scenePhase: String?
    ) async {}

    /// Non-async so `NSLock` is taken outside an async context.
    private func record(_ outcome: BackgroundTaskRunOutcome) {
        lock.lock(); defer { lock.unlock() }
        seen.append(outcome)
    }

    @discardableResult
    func finishRun(runId: String, update: BackgroundTaskRunOutcomeUpdate) async -> Bool {
        record(update.outcome)
        finishes.increment()
        if update.outcome == parkOn {
            reachedPark.increment()
            await gate.wait()
        }
        return true
    }

    func fetchLatestRun(for entryPoint: BackgroundTaskRunEntryPoint) async -> BackgroundTaskRunRecord? { nil }
    func fetchRecentRuns(limit: Int) async -> [BackgroundTaskRunRecord] { [] }
    func fetchLatestRun(forAssetId assetId: String) async -> BackgroundTaskRunRecord? { nil }
    @discardableResult
    func reapOrphansAtLaunch(startedBefore: Double) async -> Int { 0 }
}
