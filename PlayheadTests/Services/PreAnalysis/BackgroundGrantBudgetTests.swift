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
            provenance: .measured(sampleSize: 203)
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

    @Test("five expired windows in a row still leave the job alive",
          .timeLimit(.minutes(1)))
    func repeatedExpiryNeverSupersedes() async throws {
        // Seeded at 4, one below `maxAttemptCount`. Pre-fix this exact input
        // superseded the job with `nextEligibleAt: nil` and
        // `maxAttemptsReached:cancelMidRun` — and a superseded row NEVER comes
        // back, because `workKey` is UNIQUE and `insertJob` is `INSERT OR
        // IGNORE` over a key stable across launches.
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
        let scheduler = makeScheduler(store: store, downloads: downloads)

        _ = await scheduler.processNextDispatchableJobForTesting(cancelAfterRunnerStart: .taskExpired)

        let after = try #require(try await store.fetchJob(byId: "long-episode"))
        #expect(after.state == "queued",
                "the OS reclaiming a window must never permanently abandon an episode")
        #expect(after.attemptCount == 4)
        #expect(after.lastErrorCode?.contains("maxAttemptsReached") != true)
    }

    @Test("awaitCurrentJobSettled waits for a running job and times out rather than lying",
          .timeLimit(.minutes(1)))
    func settleWaitDoesNotLie() async throws {
        // The expiration handler spends part of its teardown reserve here so
        // that `setTaskCompleted` — after which iOS may suspend the process —
        // does not land while the requeue write is still in flight. A wait that
        // reports success without observing anything is worse than no wait: it
        // makes the handler *look* like it protects the write.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await insertJob(store: store, downloads: downloads, jobId: "settle", attemptCount: 0)
        let scheduler = makeScheduler(store: store, downloads: downloads)

        // Nothing running: settles immediately, and must not burn the budget.
        let idleStart = ContinuousClock.now
        #expect(await scheduler.awaitCurrentJobSettled(within: .seconds(5)))
        #expect(idleStart.duration(to: ContinuousClock.now) < .seconds(2),
                "an idle scheduler must settle at once, not wait out the budget")

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
        let settled = await scheduler.awaitCurrentJobSettled(
            within: .milliseconds(300),
            pollInterval: .milliseconds(20)
        )
        #expect(!settled,
                "a job still parked in decode has not settled; reporting true loses the requeue")

        await scheduler.cancelCurrentJob(cause: .taskExpired)
        #expect(await scheduler.awaitCurrentJobSettled(within: .seconds(10)),
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

    private func makeScheduler(store: AnalysisStore) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
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
            downloadManager: StubDownloadProvider(),
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
}
