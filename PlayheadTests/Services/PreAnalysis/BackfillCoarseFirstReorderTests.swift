// BackfillCoarseFirstReorderTests.swift
// playhead-13kf: a granted backfill window runs the FM / coarse ad-scan phase
// FIRST — directly, not through the analysis-job queue — and hands the
// transcription drain the REMAINDER of the same measured budget.
//
// The measured case for the order: 108 of 203 expired backfill windows in the
// 2026-08-06 pull contain exactly one `acquired` event and nothing else — the
// transcription drain dispatched one pass that outlived the whole grant, and
// the ad-scan behind it never ran. PassA coarse windows cost p50 6.2 s / p90
// 51.1 s (n=113 `semantic_scan_results.latencyMs`), so a 219 s work budget
// banks ~35 windows at the median where it used to bank zero.
//
// Also under test: the playhead-lmrx F6 floor question, deferred to this bead.
// The 60 s `minimumCheckpointBudget` prices ONE FM COARSE WINDOW, so it moved
// with the FM phase (per-asset start gate); the drain behind it takes the new
// `minimumDrainCheckpointBudget`, derived to ZERO for the reordered handler —
// see `BackgroundGrantBudget.minimumDrainCheckpointBudget` for the numerator/
// denominator argument.

import BackgroundTasks
import Foundation
import OSLog
import Testing

@testable import Playhead

// MARK: - The order, pinned behaviourally

@Suite("playhead-13kf: the FM/coarse phase runs before the transcription drain")
struct BackfillCoarseFirstOrderTests {

    /// Decode stub that always throws, so a dispatched job reaches a terminal
    /// state in one pass — the same deterministic fixed point the lmrx floor
    /// suite uses.
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

    private func makeReconciler(store: AnalysisStore) -> AnalysisJobReconciler {
        AnalysisJobReconciler(
            store: store,
            downloadManager: StubDownloadProvider(),
            capabilitiesService: StubCapabilitiesProvider(
                snapshot: makeCapabilitySnapshot(thermalState: .nominal, isCharging: true)
            )
        )
    }

    /// A queued compute-only job the drain can dispatch to a terminal state in
    /// one pass (`FailingDecodeStub` + attemptCount 4 → superseded).
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

    /// Lock-protected slot for the queued-job snapshot the coarse-phase hook
    /// takes. `nil` distinguishes "the hook never ran" (the deleted-call-site
    /// direction) from "it ran and saw an empty queue" (the wrong-order
    /// direction).
    private final class SnapshotBox: @unchecked Sendable {
        private let lock = NSLock()
        private var ids: Set<String>?
        func store(_ value: Set<String>) {
            lock.lock(); defer { lock.unlock() }
            ids = value
        }
        var value: Set<String>? {
            lock.lock(); defer { lock.unlock() }
            return ids
        }
    }

    @Test("the coarse phase observes the queue BEFORE the drain has dispatched anything",
          .timeLimit(.minutes(1)))
    func coarsePhaseRunsBeforeTheDrainDispatchesAnything() async throws {
        // THE ORDERING PIN, and the delete-the-call-site test in one. The
        // coarse-phase hook snapshots the analysis queue at the instant the
        // phase runs — the only observation point that can tell "before the
        // drain" from "after the drain":
        //
        //   * delete the `runPendingCoarseScans` call     → snapshot is nil
        //   * restore the old order (coarse after drain)  → snapshot is empty
        //   * drop the drain instead                      → the job stays queued
        //
        // All three directions are asserted, so no single reversion of this
        // bead leaves the suite green.
        let store = try await makeTestStore()
        let downloads = StubDownloadProvider()
        try await insertComputeOnlyJob(
            store: store, downloads: downloads,
            jobId: "order-probe", episodeId: "ep-order-probe"
        )
        let scheduler = makeScheduler(store: store, downloads: downloads)

        let snapshot = SnapshotBox()
        let coordinator = StubAnalysisCoordinator()
        coordinator.onRunPendingCoarseScans = {
            let queued = (try? await store.fetchJobsByState("queued")) ?? []
            snapshot.store(Set(queued.map(\.jobId)))
        }
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

        let seenAtCoarseTime = try #require(
            snapshot.value,
            """
            handleBackfillTask must run the FM/coarse phase — a granted window \
            that never reaches it is the 108-of-203 shape this bead removes.
            """
        )
        #expect(
            seenAtCoarseTime.contains("order-probe"),
            """
            The coarse phase must run BEFORE the transcription drain. It \
            observed a queue the drain had already emptied, which is the \
            pre-playhead-13kf order.
            """
        )
        let queuedAfter = try await store.fetchJobsByState("queued")
        #expect(
            !queuedAfter.contains { $0.jobId == "order-probe" },
            """
            The drain must still run AFTER the coarse phase, on the remaining \
            budget — the reorder funds the scan, it does not defund \
            transcription.
            """
        )
        await scheduler.stop()
    }

    @Test("both phases and the poll spend ONE deadline, anchored at the grant's start",
          .timeLimit(.minutes(1)))
    func coarsePhaseSharesTheGrantDeadlineWithTheDrainAndPoll() async throws {
        // "The drain's budget is derived from what is actually left" is a
        // property of the deadline being an INSTANT the phases share. The
        // coarse phase burns 1.2 s here; an implementation that re-anchored
        // the later drivers at `now + workBudget` after the coarse phase would
        // hand them a deadline ≥ 1.2 s later than the coarse phase's own, and
        // the equality below would fail.
        let coordinator = StubAnalysisCoordinator()
        coordinator.runPendingCoarseScansDuration = .milliseconds(1200)
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

        let coarse = try #require(coordinator.runPendingCoarseScansCalls.first,
                                  "the handler must drive the coarse phase")
        let pollDeadline = try #require(coordinator.runPendingBackfillDeadlines.first,
                                        "the handler must still drive the poll loop")
        #expect(coarse.deadline == pollDeadline,
                """
                One grant, one deadline: every phase must spend the SAME \
                instant, so a phase that runs long leaves less for the next — \
                the truth about the window — rather than each phase opening a \
                fresh clock.
                """)

        let budget = BackgroundGrantBudget.backfillProcessing
        #expect(after.duration(to: coarse.deadline) <= budget.workBudget,
                "the coarse deadline must be inside the measured grant")
        #expect(before.duration(to: coarse.deadline) >= budget.workBudget,
                "and it must hand the phase the whole work budget")
        #expect(coarse.minimumWindowBudget == budget.minimumCheckpointBudget,
                """
                The per-asset start gate is the measured cost of one coarse \
                window — the artifact this phase banks. That is where the 60 s \
                floor MOVED; it did not disappear (playhead-lmrx F6).
                """)
        #expect(coordinator.grantDriverCallOrder == ["coarse", "poll"],
                "the coarse phase precedes the poll keep-alive")
    }

    @Test("both drain call sites pass the DRAIN floor field, and the coarse phase the coarse-window price — counted")
    func floorThreadingIsCountedInSource() throws {
        // WHY A SOURCE CANARY AND NOT A BEHAVIOURAL PIN — recorded because the
        // behavioural version was tried first and its own rail (RO03) SURVIVED
        // it. That version seeded a job, gave the handler a budget whose
        // remainder was under the old 60 s floor, and asserted the job was
        // still dispatched. But `handleBackfillTask` starts the long-lived
        // `runLoop()` before the drain, and the run loop dispatches the same
        // job with NO floor at all — a second dispatcher with identical gates
        // masks the first, so "the job left queued" cannot distinguish the
        // drain refusing at 60 s from the drain admitting at 0.
        //
        // The chain that replaces it, each link killable on its own:
        //   * the VALUE (backfill drain floor = 0, recovery = 60) is pinned
        //     arithmetically in `DrainFloorDerivationTests` (rails RO07/RO08);
        //   * the MECHANISM (drainEligible honours whatever floor it is
        //     handed) is pinned behaviourally in `DrainEligibleStartGateTests`
        //     (rail LX04);
        //   * the CALL SITE — which budget field each caller passes — is THIS
        //     test, counted per the repo's canary rule
        //     (`FMUnboundedCallCanaryTests`) so a third drain call site cannot
        //     appear unexamined (rail RO03).
        let lines = try Self.backgroundProcessingServiceCodeLines()
        let drainField = lines.filter {
            $0.contains("minimumCheckpointBudget: budget.minimumDrainCheckpointBudget")
        }.count
        let coarsePriceAtDrain = lines.filter {
            $0.contains("minimumCheckpointBudget: budget.minimumCheckpointBudget")
        }.count
        let coarseGate = lines.filter {
            $0.contains("minimumWindowBudget: budget.minimumCheckpointBudget")
        }.count
        #expect(drainField == 2,
                "backfill AND recovery drains must both read the drain-floor field; found \(drainField)")
        #expect(coarsePriceAtDrain == 0,
                "no drain may be handed the coarse-window price — that is the F6 revert (playhead-13kf); found \(coarsePriceAtDrain)")
        #expect(coarseGate == 1,
                "the FM-first phase is gated by the coarse-window price, exactly once; found \(coarseGate)")
    }

    /// `BackgroundProcessingService.swift` as code lines, whole-line comments
    /// stripped — the canary is about what the code does, and prose that
    /// legitimately names the fields must not satisfy it (the
    /// `FMUnboundedCallCanaryTests` rule).
    private static func backgroundProcessingServiceCodeLines() throws -> [String] {
        var root = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        for _ in 0..<3 {
            root.deleteLastPathComponent()
        }
        let path = root
            .appendingPathComponent("Playhead", isDirectory: true)
            .appendingPathComponent("Services/AnalysisCoordinator/BackgroundProcessingService.swift")
        let text = try String(contentsOf: path, encoding: .utf8)
        return text
            .split(separator: "\n", omittingEmptySubsequences: false)
            .map(String.init)
            .map { $0.trimmingCharacters(in: .whitespaces).hasPrefix("//") ? "" : $0 }
    }
}

// MARK: - The coarse-scan loop's own gates

@Suite("playhead-13kf: the coarse-scan loop's gates")
struct CoarseScanLoopTests {

    private static let logger = Logger(subsystem: "com.playhead.tests", category: "13kf")

    /// Lock-protected recorder for which assets the loop actually started.
    private final class ScanRecorder: @unchecked Sendable {
        private let lock = NSLock()
        private var started: [String] = []
        func record(_ id: String) {
            lock.lock(); defer { lock.unlock() }
            started.append(id)
        }
        var value: [String] {
            lock.lock(); defer { lock.unlock() }
            return started
        }
    }

    /// Lock-protected mutable instant, so a test can move "now" between
    /// iterations without sleeping.
    private final class ClockBox: @unchecked Sendable {
        private let lock = NSLock()
        private var instant: ContinuousClock.Instant
        init(_ instant: ContinuousClock.Instant) { self.instant = instant }
        func advance(by duration: Duration) {
            lock.lock(); defer { lock.unlock() }
            instant = instant + duration
        }
        var now: ContinuousClock.Instant {
            lock.lock(); defer { lock.unlock() }
            return instant
        }
    }

    @Test("the window floor is a START gate between assets, not a kill switch")
    func floorGatesTheNextAssetOnly() async {
        // First asset admitted at 70 s remaining; the scan consumes 20 s, so
        // the second is asked at 50 s — under the 60 s floor — and must not
        // start. A mutant that weakens the gate to `remaining > .zero` starts
        // both.
        let start = ContinuousClock.now
        let clock = ClockBox(start)
        let recorder = ScanRecorder()
        let scanned = await AnalysisCoordinator.runCoarseScanLoop(
            deadline: start + .seconds(70),
            minimumWindowBudget: .seconds(60),
            candidates: ["asset-a", "asset-b"],
            isStopRequested: { false },
            scanAsset: { id in
                recorder.record(id)
                clock.advance(by: .seconds(20))
            },
            isTaskCancelled: { false },
            now: { clock.now },
            logger: Self.logger
        )
        #expect(scanned == 1)
        #expect(recorder.value == ["asset-a"],
                "below the one-window price, starting another asset converts grant tail into nothing")
    }

    @Test("a deadline already passed starts nothing")
    func passedDeadlineStartsNothing() async {
        let start = ContinuousClock.now
        let recorder = ScanRecorder()
        let scanned = await AnalysisCoordinator.runCoarseScanLoop(
            deadline: start,
            minimumWindowBudget: .zero,
            candidates: ["asset-a"],
            isStopRequested: { false },
            scanAsset: { recorder.record($0) },
            isTaskCancelled: { false },
            now: { start },
            logger: Self.logger
        )
        #expect(scanned == 0)
        #expect(recorder.value.isEmpty)
    }

    @Test("a stop request between assets ends the phase")
    func stopRequestEndsThePhase() async {
        // stop() is the thermal brake; the loop must honour it between assets
        // exactly as the poll loop honours it between polls.
        let start = ContinuousClock.now
        let recorder = ScanRecorder()
        let stopAfterFirst = ScanRecorder()
        let scanned = await AnalysisCoordinator.runCoarseScanLoop(
            deadline: start + .seconds(600),
            minimumWindowBudget: .zero,
            candidates: ["asset-a", "asset-b"],
            isStopRequested: { !stopAfterFirst.value.isEmpty },
            scanAsset: { id in
                recorder.record(id)
                stopAfterFirst.record(id)
            },
            isTaskCancelled: { false },
            now: { start },
            logger: Self.logger
        )
        #expect(scanned == 1)
        #expect(recorder.value == ["asset-a"])
    }

    @Test("a CancellationError ends the phase — the grant was reclaimed")
    func cancellationEndsThePhase() async {
        let start = ContinuousClock.now
        let recorder = ScanRecorder()
        let scanned = await AnalysisCoordinator.runCoarseScanLoop(
            deadline: start + .seconds(600),
            minimumWindowBudget: .zero,
            candidates: ["asset-a", "asset-b"],
            isStopRequested: { false },
            scanAsset: { id in
                recorder.record(id)
                throw CancellationError()
            },
            isTaskCancelled: { false },
            now: { start },
            logger: Self.logger
        )
        #expect(scanned == 0, "a cancelled asset was not driven to a Stage-4 return")
        #expect(recorder.value == ["asset-a"], "the second asset must not start after a reclaim")
    }

    @Test("one asset's failure does not starve the rest of the window")
    func perAssetErrorContinues() async {
        struct StoreHiccup: Error {}
        let start = ContinuousClock.now
        let recorder = ScanRecorder()
        let scanned = await AnalysisCoordinator.runCoarseScanLoop(
            deadline: start + .seconds(600),
            minimumWindowBudget: .zero,
            candidates: ["asset-a", "asset-b"],
            isStopRequested: { false },
            scanAsset: { id in
                recorder.record(id)
                if id == "asset-a" { throw StoreHiccup() }
            },
            isTaskCancelled: { false },
            now: { start },
            logger: Self.logger
        )
        #expect(scanned == 1)
        #expect(recorder.value == ["asset-a", "asset-b"])
    }

    @Test("candidates keep BOTH populations, resumable first, deduplicated")
    func candidateMembershipAndOrder() {
        // The fil5 population (transcribed, zero coverage-lane rows) is
        // invisible to the resumable query BY CONSTRUCTION — dropping it here
        // silently recreates the blind spot playhead-fil5 was filed about,
        // which is why membership is a pinned rule and not an implementation
        // detail.
        let merged = AnalysisCoordinator.coarseScanCandidates(
            resumable: ["a", "b"],
            missingRows: ["b", "c"]
        )
        #expect(merged == ["a", "b", "c"])
        #expect(AnalysisCoordinator.coarseScanCandidates(resumable: [], missingRows: ["x"]) == ["x"],
                "an asset with no coverage-lane rows at all must still be a candidate")
    }

    // MARK: - playhead-8ljj: every exit names itself

    /// A `Sendable` sink for the reports the loop publishes.
    private final class ReportLog: @unchecked Sendable {
        private let lock = NSLock()
        private var entries: [CoarseScanPhaseReport] = []
        func note(_ report: CoarseScanPhaseReport) {
            lock.lock(); defer { lock.unlock() }
            entries.append(report)
        }
        var value: [CoarseScanPhaseReport] {
            lock.lock(); defer { lock.unlock() }
            return entries
        }
        var last: CoarseScanPhaseReport? { value.last }
    }

    /// Every way out of the loop must leave a DIFFERENT name behind. Driven as
    /// one table because the property under test is the partition — a fix that
    /// published a single constant on every exit would satisfy any one of these
    /// rows on its own.
    @Test("each exit publishes its own verdict")
    func everyExitNamesItself() async {
        struct StoreHiccup: Error {}
        let start = ContinuousClock.now

        // Ran out of candidates, having driven them: `drove`.
        let drove = ReportLog()
        _ = await AnalysisCoordinator.runCoarseScanLoop(
            deadline: start + .seconds(600), minimumWindowBudget: .zero,
            candidates: ["a", "b"],
            isStopRequested: { false }, scanAsset: { _ in },
            isTaskCancelled: { false }, now: { start },
            report: { drove.note($0) }, logger: Self.logger
        )
        #expect(drove.last == CoarseScanPhaseReport(verdict: .drove, scanned: 2, candidates: 2))

        // Ran out of candidates, having driven NONE of them because every one
        // threw: `refused`. This is "there was work and every attempt at it was
        // turned away", which is not the same finding as an empty queue and
        // must not read as one.
        let refused = ReportLog()
        _ = await AnalysisCoordinator.runCoarseScanLoop(
            deadline: start + .seconds(600), minimumWindowBudget: .zero,
            candidates: ["a", "b"],
            isStopRequested: { false }, scanAsset: { _ in throw StoreHiccup() },
            isTaskCancelled: { false }, now: { start },
            report: { refused.note($0) }, logger: Self.logger
        )
        #expect(refused.last == CoarseScanPhaseReport(verdict: .refused, scanned: 0, candidates: 2))

        // Ran out of BUDGET: `floor`. Distinct from `drove` even though the
        // scanned count can be identical.
        let clock = ClockBox(start)
        let floor = ReportLog()
        _ = await AnalysisCoordinator.runCoarseScanLoop(
            deadline: start + .seconds(70), minimumWindowBudget: .seconds(60),
            candidates: ["a", "b"],
            isStopRequested: { false },
            scanAsset: { _ in clock.advance(by: .seconds(20)) },
            isTaskCancelled: { false }, now: { clock.now },
            report: { floor.note($0) }, logger: Self.logger
        )
        #expect(floor.last == CoarseScanPhaseReport(verdict: .floor, scanned: 1, candidates: 2))

        // The OS reclaimed the window: `cancelled`.
        let cancelled = ReportLog()
        _ = await AnalysisCoordinator.runCoarseScanLoop(
            deadline: start + .seconds(600), minimumWindowBudget: .zero,
            candidates: ["a", "b"],
            isStopRequested: { false }, scanAsset: { _ in throw CancellationError() },
            isTaskCancelled: { false }, now: { start },
            report: { cancelled.note($0) }, logger: Self.logger
        )
        #expect(cancelled.last == CoarseScanPhaseReport(verdict: .cancelled, scanned: 0, candidates: 2))

        // The thermal brake: `stopped`.
        let stopped = ReportLog()
        _ = await AnalysisCoordinator.runCoarseScanLoop(
            deadline: start + .seconds(600), minimumWindowBudget: .zero,
            candidates: ["a", "b"],
            isStopRequested: { true }, scanAsset: { _ in },
            isTaskCancelled: { false }, now: { start },
            report: { stopped.note($0) }, logger: Self.logger
        )
        #expect(stopped.last == CoarseScanPhaseReport(verdict: .stopped, scanned: 0, candidates: 2))

        // All five verdicts are distinct — the partition, asserted as one.
        let verdicts = [drove, refused, floor, cancelled, stopped].compactMap { $0.last?.verdict }
        #expect(Set(verdicts).count == 5, "each exit must be separately nameable")
    }

    /// **The property the return value cannot carry.** A window reclaimed while
    /// an asset is mid-scan never returns from this loop, so its account has to
    /// have been published BEFORE the scan started. The progress publish after
    /// each asset is what makes the count current when the window dies.
    @Test("the count is current before each asset, so a reclaim mid-asset has a number")
    func progressIsPublishedBeforeTheWindowCanEnd() async {
        let start = ContinuousClock.now
        let log = ReportLog()
        // Never returns from the third asset — the shape of a real reclaim.
        _ = await AnalysisCoordinator.runCoarseScanLoop(
            deadline: start + .seconds(600), minimumWindowBudget: .zero,
            candidates: ["a", "b", "c"],
            isStopRequested: { false },
            scanAsset: { id in
                if id == "c" { throw CancellationError() }
            },
            isTaskCancelled: { false }, now: { start },
            report: { log.note($0) }, logger: Self.logger
        )
        // After "a" and after "b", each as `inflight` carrying the count so far.
        #expect(log.value.prefix(2).map(\.verdict) == [.inflight, .inflight])
        #expect(log.value.prefix(2).map(\.scanned) == [1, 2])
        #expect(log.last == CoarseScanPhaseReport(verdict: .cancelled, scanned: 2, candidates: 3))
    }

    /// The read-failure set travels with EVERY report the loop publishes, not
    /// just the census. A window reclaimed mid-loop must still record that its
    /// candidate list was built from a half-readable population — otherwise the
    /// short list it worked from reads as the whole truth.
    @Test("a read failure rides every report the loop publishes")
    func readFailureRidesEveryReport() async {
        let start = ContinuousClock.now
        let log = ReportLog()
        _ = await AnalysisCoordinator.runCoarseScanLoop(
            deadline: start + .seconds(600), minimumWindowBudget: .zero,
            candidates: ["a"],
            unreadable: .missingCoverageLaneRows,
            isStopRequested: { false }, scanAsset: { _ in },
            isTaskCancelled: { false }, now: { start },
            report: { log.note($0) }, logger: Self.logger
        )
        #expect(!log.value.isEmpty)
        #expect(log.value.allSatisfy { $0.unreadable == .missingCoverageLaneRows })
        #expect(log.last?.ledgerReason == "coarse=drove(1/1) banked=? unread=missingRows",
                "the banked count is not this layer's to know — it stays unmeasured here")
    }

    /// The rendered string is what lands in `background_task_runs.deferReason`,
    /// so its shape is a contract with every future reader of that column.
    @Test("the ledger reason renders as greppable key=value pairs")
    func ledgerReasonShape() {
        #expect(CoarseScanPhaseReport(verdict: .empty, scanned: 0, candidates: 0, bankedRows: 0)
                    .ledgerReason == "coarse=empty(0/0) banked=0")
        #expect(CoarseScanPhaseReport(verdict: .drove, scanned: 3, candidates: 4, bankedRows: 12)
                    .ledgerReason == "coarse=drove(3/4) banked=12")
        // Not measured is not zero.
        #expect(CoarseScanPhaseReport(verdict: .empty, scanned: 0, candidates: 0, bankedRows: nil)
                    .ledgerReason == "coarse=empty(0/0) banked=?")
        #expect(CoarseScanPhaseReport(verdict: .empty, scanned: 0, candidates: 0,
                                      unreadable: .missingCoverageLaneRows, bankedRows: 0)
                    .ledgerReason == "coarse=empty(0/0) banked=0 unread=missingRows")
        // A read failure with candidates still found: the modifier is
        // independent of the verdict, because a half-readable population is
        // neither an absence nor a full census.
        #expect(CoarseScanPhaseReport(verdict: .drove, scanned: 1, candidates: 1,
                                      unreadable: .resumable, bankedRows: 5)
                    .ledgerReason == "coarse=drove(1/1) banked=5 unread=resumable")
    }
}

// MARK: - The derived floor values

@Suite("playhead-13kf: the drain floor is derived, not inherited")
struct DrainFloorDerivationTests {

    @Test("the reordered handler's drain floor is zero, and zero is a derivation")
    func backfillDrainFloorIsZero() {
        // Numerator/denominator, per the BackgroundGrantBudget rule: the 60 s
        // floor prices one FM coarse window (p95 57.5 s, n=142 scan rows) —
        // the artifact the FM-FIRST phase now banks before the drain runs. The
        // drain's own artifact (a persisted transcript-chunk batch) is
        // unpriceable from the ledger: `transcript_chunks` carries no
        // timestamps, and 96 of the 115 measured transcription attempts
        // persisted ZERO chunks at any length (playhead-i2am). A nonzero value
        // would be a guess wearing a unit.
        #expect(BackgroundGrantBudget.backfillProcessing.minimumDrainCheckpointBudget == .zero)
        #expect(BackgroundGrantBudget.backfillProcessingCharged.minimumDrainCheckpointBudget == .zero,
                "the charged sibling shares the reordered handler")
    }

    @Test("recovery keeps the coarse-window price on its drain — it was NOT reordered")
    func recoveryDrainFloorKeepsItsMeaning() {
        // handlePreAnalysisRecovery still reaches Stage-4 FM work only through
        // its drain, so the artifact at stake there is still one coarse window
        // and the floor must stay COUPLED to that price. An independent 60
        // here would drift silently the day the checkpoint budget is
        // re-measured.
        let recovery = BackgroundGrantBudget.preAnalysisRecovery
        #expect(recovery.minimumDrainCheckpointBudget == recovery.minimumCheckpointBudget)
        #expect(recovery.minimumDrainCheckpointBudget == .seconds(60))
    }

    @Test("every budget still leaves room to start drain work")
    func workBudgetExceedsTheDrainFloor() {
        for budget in [BackgroundGrantBudget.backfillProcessing,
                       .backfillProcessingCharged,
                       .preAnalysisRecovery] {
            #expect(budget.workBudget > budget.minimumDrainCheckpointBudget)
        }
    }
}
