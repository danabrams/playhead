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
        floor: Duration = .seconds(60)
    ) -> BackgroundGrantBudget {
        BackgroundGrantBudget(
            designGrant: grant,
            teardownReserve: reserve,
            minimumCheckpointBudget: floor,
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

    @Test("canReachCheckpoint admits exactly the floor and refuses below it")
    func canReachCheckpointBoundary() {
        let subject = budget()
        #expect(subject.canReachCheckpoint(remaining: .seconds(61)))
        #expect(subject.canReachCheckpoint(remaining: .seconds(60)),
                "an artifact that costs exactly its own cost fits")
        #expect(!subject.canReachCheckpoint(remaining: .seconds(59)))
        #expect(!subject.canReachCheckpoint(remaining: .zero))
        #expect(!subject.canReachCheckpoint(remaining: .seconds(-5)),
                "a deadline already in the past yields a negative remainder")
    }

    @Test("canReachCheckpoint(before:now:) agrees with the duration form")
    func canReachCheckpointDeadlineForm() {
        let subject = budget()
        let now = ContinuousClock.now
        #expect(subject.canReachCheckpoint(before: now + .seconds(120), now: now))
        #expect(!subject.canReachCheckpoint(before: now + .seconds(5), now: now))
        #expect(!subject.canReachCheckpoint(before: now - .seconds(5), now: now))
    }

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
        try await store.updateJobState(jobId: "gave-up", state: "complete")

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
        // settled by definition rather than a thing to wait for.
        let idleStart = ContinuousClock.now
        #expect(await scheduler.awaitJobsSettled([], within: .seconds(5)))
        #expect(idleStart.duration(to: ContinuousClock.now) < .seconds(2),
                "an empty population must settle at once, not wait out the budget")

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
        let unrelated = await scheduler.awaitJobsSettled(
            ["a-job-nobody-cancelled"],
            within: .milliseconds(300),
            pollInterval: .milliseconds(20)
        )
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
        await bps.handleBackfillTask(task)
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
        // mutation battery's own suite load, which is exactly the kind of
        // timing assertion this repo gates behind `PerfGate`; this one has no
        // timing assertion left to gate.
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
            let stillUnwinding = await scheduler.hasCurrentRunningTaskForTesting()
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
        let workTask = Task { await bps.handleBackfillTask(task) }
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
}
