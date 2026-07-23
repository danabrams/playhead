// RediffRefetchTests.swift
// playhead-xsdz.28: offline coverage for the rediff RE-FETCH policy — the pure
// decision core (≥24h gate, backoff + retry budget, sample compare, bandwidth)
// AND the `RediffRefetchService` sweep behind its default-OFF flag.
//
// The acceptance criteria are pinned here:
//   • pre-check demonstrably SKIPS non-rotators (same-bytes sample → no full
//     fetch)                                   → `precheckSkipsNonRotator`
//   • the ≥24h gate is enforced                → `under24hGateSkipsFetch` +
//                                                 `eligibility*` policy tests
//   • no HEAD/ETag reliance ANYWHERE           → `productionSamplerIssuesOnly*`
//   • fetched audio provably DELETED           → `rotatorFullFetchDeletesRealBCopy`
//                                                 + `fingerprintFailureStill*`
//   • bandwidth accounting logged              → the outcome `cost` assertions
//   • default-OFF byte identity (no schedule,  → `disabledSchedulesNothing*` +
//     no network/fetch)                          `disabledSweepTouchesNothing`
//   • BGProcessingTask WiFi+charging policy     → `scheduleUsesNetworkAndPower*`

import BackgroundTasks
import Foundation
import Testing
@testable import Playhead

@Suite("RediffRefetch (playhead-xsdz.28 re-fetch policy)")
struct RediffRefetchTests {

    typealias Policy = RediffRefetchPolicy
    static let day = Policy.Configuration.secondsPerDay

    // MARK: - Fixtures

    private static func fingerprint(_ seed: String, total: Int64) -> Policy.AudioSampleFingerprint {
        Policy.sampleFingerprint(
            head: Data("\(seed)-head".utf8),
            tail: Data("\(seed)-tail".utf8),
            totalLength: total
        )
    }

    private static func candidate(
        assetId: String = "asset-1",
        enclosure: String = "https://cdn.example.com/ep1.mp3",
        downloadedAt: Double,
        local: URL? = nil,
        state: Policy.AttemptState = .initial
    ) -> RediffRefetchCandidate {
        RediffRefetchCandidate(
            assetId: assetId,
            enclosureURL: URL(string: enclosure)!,
            downloadedAt: downloadedAt,
            localAudioURL: local ?? URL(fileURLWithPath: "/tmp/nonexistent-\(assetId).mp3"),
            attemptState: state
        )
    }

    // MARK: - Policy: ≥24h gate

    @Test("First attempt is gated until ≥24h after download")
    func eligibilityTooSoonBefore24h() {
        let now = 1_000_000.0
        let e = Policy.eligibility(now: now, downloadedAt: now - (23 * 3600), state: .initial)
        guard case let .tooSoonSinceDownload(age) = e else {
            Issue.record("expected tooSoonSinceDownload, got \(e)"); return
        }
        #expect(abs(age - 23 * 3600) < 0.001)
    }

    @Test("First attempt becomes eligible exactly at the 24h boundary")
    func eligibilityAtExactly24h() {
        let now = 1_000_000.0
        let e = Policy.eligibility(now: now, downloadedAt: now - Self.day, state: .initial)
        #expect(e == .eligible)
    }

    // MARK: - Policy: backoff + retry budget

    @Test("Backoff after one unchanged attempt is 1 day, then eligible")
    func eligibilityBackoffFirstStep() {
        let last = 5_000_000.0
        let state = Policy.AttemptState(unchangedAttempts: 1, lastAttemptAt: last, resolved: false)
        // Before +1d → not elapsed; the reported next-eligible time is last+1d.
        let early = Policy.eligibility(now: last + Self.day - 1, downloadedAt: 0, state: state)
        guard case let .backoffNotElapsed(next) = early else {
            Issue.record("expected backoffNotElapsed, got \(early)"); return
        }
        #expect(abs(next - (last + Self.day)) < 0.001)
        // At +1d → eligible.
        #expect(Policy.eligibility(now: last + Self.day, downloadedAt: 0, state: state) == .eligible)
    }

    @Test("Backoff after two unchanged attempts is 2 days")
    func eligibilityBackoffSecondStep() {
        let last = 5_000_000.0
        let state = Policy.AttemptState(unchangedAttempts: 2, lastAttemptAt: last, resolved: false)
        #expect(Policy.eligibility(now: last + (2 * Self.day) - 1, downloadedAt: 0, state: state)
            == .backoffNotElapsed(nextEligibleAt: last + 2 * Self.day))
        #expect(Policy.eligibility(now: last + 2 * Self.day, downloadedAt: 0, state: state) == .eligible)
    }

    @Test("Retry budget is exhausted after 3 unchanged attempts")
    func eligibilityRetryBudgetExhausted() {
        let state = Policy.AttemptState(unchangedAttempts: 3, lastAttemptAt: 5_000_000, resolved: false)
        #expect(Policy.eligibility(now: 5_000_000 + 100 * Self.day, downloadedAt: 0, state: state)
            == .retryBudgetExhausted)
    }

    @Test("A resolved episode is terminal — never re-fetched again")
    func eligibilityResolvedIsTerminal() {
        let state = Policy.AttemptState(unchangedAttempts: 1, lastAttemptAt: 5_000_000, resolved: true)
        #expect(Policy.eligibility(now: 5_000_000 + 100 * Self.day, downloadedAt: 0, state: state)
            == .alreadyResolved)
    }

    @Test("advanceUnchanged bumps count + stamps time; markResolved is terminal")
    func stateTransitions() {
        let s0 = Policy.AttemptState.initial
        let s1 = Policy.advanceUnchanged(s0, at: 42)
        #expect(s1 == Policy.AttemptState(unchangedAttempts: 1, lastAttemptAt: 42, resolved: false))
        let s2 = Policy.markResolved(s1, at: 99)
        #expect(s2 == Policy.AttemptState(unchangedAttempts: 1, lastAttemptAt: 99, resolved: true))
    }

    // MARK: - Policy: sample compare

    @Test("Identical head/tail/length → not rotated; any difference → rotated")
    func sampleRotationCompare() {
        let a = Self.fingerprint("x", total: 1000)
        let aSame = Self.fingerprint("x", total: 1000)
        #expect(!Policy.isRotated(local: a, remote: aSame))

        #expect(Policy.isRotated(local: a, remote: Self.fingerprint("y", total: 1000)))      // head/tail differ
        #expect(Policy.isRotated(local: a, remote: Self.fingerprint("x", total: 1001)))      // only length differs
    }

    @Test("BandwidthCost sums pre-check + full-fetch bytes")
    func bandwidthTotals() {
        let c = Policy.BandwidthCost(precheckBytes: 131_072, fullFetchBytes: 54_000_000)
        #expect(c.totalBytes == 131_072 + 54_000_000)
        #expect(Policy.BandwidthCost.zero.totalBytes == 0)
    }

    // MARK: - Service: pre-check SKIPS non-rotators (acceptance)

    @Test("Non-rotator (same-bytes sample) → NO full fetch, outcome unchanged")
    func precheckSkipsNonRotator() async {
        let sameFP = Self.fingerprint("stable", total: 42_000_000)
        let sampler = StubRangedSampler()
        sampler.defaultSample = RemoteAudioSample(fingerprint: sameFP, bytesTransferred: 131_072)
        let local = StubLocalSampler()
        local.defaultFingerprint = sameFP
        let full = StubFullFetcher()
        let recorder = SpyRefetchRecorder()

        let cand = Self.candidate(downloadedAt: 0)  // clock defaults far in the future
        let service = makeService(
            candidates: [cand],
            rangedSampler: sampler, localSampler: local, fullFetcher: full, recorder: recorder,
            now: { 100 * Self.day }
        )
        await service.runRefetchSweep()

        #expect(full.calls.isEmpty, "non-rotator must NOT trigger a full fetch")
        #expect(recorder.outcomes.count == 1)
        guard case let .unchanged(assetId, cost, newState) = recorder.outcomes.first else {
            Issue.record("expected .unchanged, got \(String(describing: recorder.outcomes.first))"); return
        }
        #expect(assetId == "asset-1")
        #expect(cost.precheckBytes == 131_072)
        #expect(cost.fullFetchBytes == 0)                 // bandwidth: no full fetch spent
        #expect(newState.unchangedAttempts == 1)          // backoff advanced
        #expect(newState.resolved == false)
    }

    // MARK: - Service: rotator full-fetch + DELETE the B-copy (acceptance)

    @Test("Rotator → full fetch, B-side fingerprinted, real B-copy file DELETED")
    func rotatorFullFetchDeletesRealBCopy() async throws {
        // A real temp file stands in for the fetched B-copy.
        let bcopy = FileManager.default.temporaryDirectory
            .appendingPathComponent("rediff-test-bcopy-\(UUID().uuidString).mp3")
        try Data(repeating: 7, count: 4096).write(to: bcopy)
        #expect(FileManager.default.fileExists(atPath: bcopy.path))

        let sampler = StubRangedSampler()
        sampler.defaultSample = RemoteAudioSample(fingerprint: Self.fingerprint("fresh", total: 55_000_000), bytesTransferred: 131_072)
        let local = StubLocalSampler()
        local.defaultFingerprint = Self.fingerprint("played", total: 54_000_000)  // differs → rotated
        let full = StubFullFetcher()
        full.fileToReturn = bcopy
        full.byteCount = 54_000_000
        let fingerprinter = StubBSideFingerprinter()
        fingerprinter.fingerprintsToReturn = [11, 22, 33, 44]
        let recorder = SpyRefetchRecorder()

        // Default FileManagerTempFileRemover — the REAL removal path.
        let service = makeService(
            candidates: [Self.candidate(downloadedAt: 0)],
            rangedSampler: sampler, localSampler: local, fullFetcher: full,
            bsideFingerprinter: fingerprinter, recorder: recorder,
            now: { 100 * Self.day }
        )
        await service.runRefetchSweep()

        #expect(full.calls.count == 1, "rotator must trigger exactly one full fetch")
        #expect(fingerprinter.calls == [bcopy], "B-side must be fingerprinted from the fetched copy")
        #expect(!FileManager.default.fileExists(atPath: bcopy.path), "B-copy must be DELETED, never persisted")

        guard case let .rotated(_, cost, fpCount, newState) = recorder.outcomes.first else {
            Issue.record("expected .rotated, got \(String(describing: recorder.outcomes.first))"); return
        }
        #expect(cost.precheckBytes == 131_072)
        #expect(cost.fullFetchBytes == 54_000_000)        // bandwidth accounts the full fetch
        #expect(fpCount == 4)
        #expect(newState.resolved, "a detected rotation is terminal")
    }

    @Test("B-copy is DELETED even when the B-side fingerprint throws")
    func fingerprintFailureStillDeletesBCopy() async throws {
        let bcopy = FileManager.default.temporaryDirectory
            .appendingPathComponent("rediff-test-bcopy-\(UUID().uuidString).mp3")
        try Data(repeating: 3, count: 2048).write(to: bcopy)

        let sampler = StubRangedSampler()
        sampler.defaultSample = RemoteAudioSample(fingerprint: Self.fingerprint("fresh", total: 10), bytesTransferred: 131_072)
        let local = StubLocalSampler()
        local.defaultFingerprint = Self.fingerprint("played", total: 20)
        let full = StubFullFetcher()
        full.fileToReturn = bcopy
        full.byteCount = 1_000
        let fingerprinter = StubBSideFingerprinter()
        fingerprinter.errorToThrow = NSError(domain: "decode", code: 1)
        let recorder = SpyRefetchRecorder()

        let service = makeService(
            candidates: [Self.candidate(downloadedAt: 0)],
            rangedSampler: sampler, localSampler: local, fullFetcher: full,
            bsideFingerprinter: fingerprinter, recorder: recorder,
            now: { 100 * Self.day }
        )
        await service.runRefetchSweep()

        #expect(!FileManager.default.fileExists(atPath: bcopy.path), "B-copy must be deleted even on fingerprint failure")
        guard case let .failed(_, cost, failureClass, newState, _) = recorder.outcomes.first else {
            Issue.record("expected .failed, got \(String(describing: recorder.outcomes.first))"); return
        }
        #expect(cost.precheckBytes == 131_072)            // pre-check bytes still accounted
        #expect(cost.fullFetchBytes == 1_000)             // full-fetch bytes still accounted
        // playhead-xsdz.36 (R2): a post-download failure is decode-class and
        // the outcome now carries the ADVANCED state (streak started).
        #expect(failureClass == .decodeFailure)
        #expect(newState.lastFailureClass == .decodeFailure)
        #expect(newState.sameClassFailureStreak == 1)
        #expect(newState.resolved == false)
    }

    // MARK: - Service: ≥24h gate enforced end-to-end (acceptance)

    @Test("An episode downloaded <24h ago is skipped — no sampling, no fetch")
    func under24hGateSkipsFetch() async {
        let sampler = StubRangedSampler()
        let local = StubLocalSampler()
        let full = StubFullFetcher()
        let recorder = SpyRefetchRecorder()
        let now = 1_000_000.0

        let cand = Self.candidate(downloadedAt: now - (12 * 3600))  // 12h old
        let service = makeService(
            candidates: [cand],
            rangedSampler: sampler, localSampler: local, fullFetcher: full, recorder: recorder,
            now: { now }
        )
        await service.runRefetchSweep()

        #expect(sampler.calls.isEmpty, "under-24h episode must not be sampled")
        #expect(full.calls.isEmpty)
        guard case let .skippedIneligible(_, reason) = recorder.outcomes.first else {
            Issue.record("expected .skippedIneligible, got \(String(describing: recorder.outcomes.first))"); return
        }
        if case .tooSoonSinceDownload = reason {} else { Issue.record("expected tooSoonSinceDownload, got \(reason)") }
    }

    // MARK: - Service: BGProcessingTask WiFi+charging policy (acceptance)

    @Test("scheduleNextRefetch submits a BGProcessingTask requiring network + external power")
    func scheduleUsesNetworkAndPowerPolicy() async {
        let scheduler = StubTaskScheduler()
        let service = makeService(candidates: [], scheduler: scheduler, enabled: true)
        await service.scheduleNextRefetch()

        #expect(scheduler.submittedRequests.count == 1)
        guard let request = scheduler.submittedRequests.first as? BGProcessingTaskRequest else {
            Issue.record("expected a BGProcessingTaskRequest"); return
        }
        #expect(request.identifier == "com.playhead.app.rediff-refetch")
        #expect(request.requiresNetworkConnectivity, "WiFi/charging policy: network required")
        #expect(request.requiresExternalPower, "WiFi/charging policy: external power required")
    }

    // MARK: - Service: default-OFF byte identity (acceptance)

    @Test("Default flag is OFF")
    func defaultFlagIsOff() {
        #expect(RediffRefetchService.isEnabledByDefault == false)
    }

    @Test("Disabled service schedules NO BGTask and touches no network")
    func disabledSchedulesNothingAndNoNetwork() async {
        let scheduler = StubTaskScheduler()
        let sampler = StubRangedSampler()
        let full = StubFullFetcher()
        let enumerator = StubRefetchEnumerator()
        enumerator.candidatesToReturn = [Self.candidate(downloadedAt: 0)]

        // enabled defaults to RediffRefetchService.isEnabledByDefault (false).
        let service = RediffRefetchService(
            enumerator: enumerator, rangedSampler: sampler, localSampler: StubLocalSampler(),
            fullFetcher: full, bsideFingerprinter: StubBSideFingerprinter(),
            recorder: SpyRefetchRecorder(), fileRemover: SpyTempFileRemover(),
            taskScheduler: scheduler, now: { 100 * Self.day }
        )

        await service.scheduleNextRefetch()
        #expect(scheduler.submittedRequests.isEmpty, "disabled → nothing scheduled")

        await service.runRefetchSweep()
        #expect(enumerator.callCount == 0, "disabled → no enumeration")
        #expect(sampler.calls.isEmpty, "disabled → no network")
        #expect(full.calls.isEmpty, "disabled → no fetch")

        // The BGTask handler, if fired while disabled, completes without work.
        let task = StubBackgroundTask()
        await service.handleRefetchTask(task)
        #expect(task.completedSuccess == true)
        #expect(sampler.calls.isEmpty)
        #expect(scheduler.submittedRequests.isEmpty)
    }

    // MARK: - Service: handler completion + expiration

    @Test("Enabled handler runs the sweep, reschedules, and completes exactly once")
    func enabledHandlerCompletesAndReschedules() async {
        let scheduler = StubTaskScheduler()
        let sampler = StubRangedSampler()
        let sameFP = Self.fingerprint("s", total: 10)
        sampler.defaultSample = RemoteAudioSample(fingerprint: sameFP, bytesTransferred: 131_072)
        let local = StubLocalSampler()
        local.defaultFingerprint = sameFP

        let service = makeService(
            candidates: [Self.candidate(downloadedAt: 0)],
            rangedSampler: sampler, localSampler: local, scheduler: scheduler, enabled: true,
            now: { 100 * Self.day }
        )
        let task = StubBackgroundTask()
        await service.handleRefetchTask(task)

        #expect(task.completedSuccess == true)
        #expect(scheduler.submittedRequests.count == 1, "handler reschedules the next fire")
        #expect(sampler.calls.count == 1, "handler ran the sweep")
    }

    @Test("A post-completion expiration is a no-op — setTaskCompleted fires exactly once")
    func postCompletionExpirationIsIdempotent() async {
        let task = StubBackgroundTask()
        let service = makeService(candidates: [], enabled: true, now: { 100 * Self.day })
        await service.handleRefetchTask(task)      // normal completion wins (success=true)
        task.simulateExpiration()                  // late OS expiration must not double-complete
        await Task.yield()                         // let the expiration hop run
        #expect(task.setTaskCompletedCallCount == 1, "setTaskCompleted must be called exactly once")
        #expect(task.completedSuccess == true)
    }

    @Test("An overlapping fire completes immediately and never poisons the in-flight sweep")
    func overlappingFireCompletesImmediately() async {
        let gated = GatedRefetchEnumerator()
        let scheduler = StubTaskScheduler()
        let service = RediffRefetchService(
            enabled: true,
            enumerator: gated,
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: StubFullFetcher(),
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: SpyRefetchRecorder(),
            fileRemover: SpyTempFileRemover(),
            taskScheduler: scheduler,
            now: { 100 * Self.day }
        )

        let taskA = StubBackgroundTask()
        let fireA = Task { await service.handleRefetchTask(taskA) }
        // A suspended on the gate ⇒ it claimed the fire and rescheduled.
        while !(await gated.gate.hasWaiters) { await Task.yield() }

        // B fires while A's sweep is in flight: completed immediately (once,
        // unsuccessfully), no second reschedule, A untouched.
        let taskB = StubBackgroundTask()
        await service.handleRefetchTask(taskB)
        #expect(taskB.setTaskCompletedCallCount == 1, "overlapping fire must still be completed")
        #expect(taskB.completedSuccess == false)
        #expect(scheduler.submittedRequests.count == 1, "the overlapping fire must not double-schedule")

        await gated.gate.open()
        await fireA.value
        #expect(taskA.setTaskCompletedCallCount == 1, "the in-flight fire completes exactly once")
        #expect(taskA.completedSuccess == true, "the overlapping fire must not expire/poison A's sweep")
    }

    @Test("A late expiration from a completed fire neither double-completes it nor expires the successor fire")
    func lateExpirationDoesNotPoisonSuccessorFire() async {
        let gated = GatedRefetchEnumerator()
        let scheduler = StubTaskScheduler()
        let service = RediffRefetchService(
            enabled: true,
            enumerator: gated,
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: StubFullFetcher(),
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: SpyRefetchRecorder(),
            fileRemover: SpyTempFileRemover(),
            taskScheduler: scheduler,
            now: { 100 * Self.day }
        )

        // Fire A runs to normal completion.
        await gated.gate.open()
        let taskA = StubBackgroundTask()
        await service.handleRefetchTask(taskA)
        #expect(taskA.setTaskCompletedCallCount == 1)

        // Fire B is mid-sweep (suspended on the gate) when A's expiration
        // handler fires LATE. With shared per-instance flags this would
        // double-complete A and mark B's fire expired; per-task tracking must
        // do neither.
        await gated.gate.close()
        let taskB = StubBackgroundTask()
        let fireB = Task { await service.handleRefetchTask(taskB) }
        while !(await gated.gate.hasWaiters) { await Task.yield() }

        taskA.simulateExpiration()
        // Let the expiration hop land on the actor (B's sweep is suspended on
        // the gate actor, so the service actor is free to run it).
        for _ in 0..<50 { await Task.yield() }

        await gated.gate.open()
        await fireB.value

        #expect(taskA.setTaskCompletedCallCount == 1, "late expiration must not double-complete A")
        #expect(taskB.setTaskCompletedCallCount == 1)
        #expect(taskB.completedSuccess == true, "A's late expiration must not poison B's fire")
    }

    // MARK: - Service: orphaned B-copy hygiene (xsdz.36 R1)

    @Test("Each enabled fire sweeps orphaned B-copies through the remover seam; disabled fires do not")
    func handlerSweepsOrphanedBCopies() async {
        let remover = SpyTempFileRemover()
        let enumerator = StubRefetchEnumerator()
        func fire(enabled: Bool) async {
            let service = RediffRefetchService(
                enabled: enabled,
                enumerator: enumerator,
                rangedSampler: StubRangedSampler(),
                localSampler: StubLocalSampler(),
                fullFetcher: StubFullFetcher(),
                bsideFingerprinter: StubBSideFingerprinter(),
                recorder: SpyRefetchRecorder(),
                fileRemover: remover,
                taskScheduler: StubTaskScheduler(),
                now: { 100 * Self.day }
            )
            await service.handleRefetchTask(StubBackgroundTask())
        }
        await fire(enabled: false)
        #expect(remover.orphanSweepAges.isEmpty, "disabled fire must not touch the filesystem seam")
        await fire(enabled: true)
        #expect(remover.orphanSweepAges == [RediffRefetchService.orphanedBCopyMinimumAge])
    }

    @Test("R3: the expiration handler is installed BEFORE the per-fire orphan sweep and reschedule")
    func expirationHandlerInstalledBeforeOrphanSweep() async {
        // The synchronous orphan sweep can spend real time deleting a
        // stranded multi-hundred-MB shard directory; the OS reclaim window is
        // already open by then. If the handler is not yet installed, an early
        // expiration is unobservable and the fire can neither bail nor
        // complete. The spy remover snapshots the install state at the exact
        // moment the sweep runs.
        let task = StubBackgroundTask()
        let scheduler = StubTaskScheduler()
        let remover = HandlerOrderSpyTempFileRemover(
            probe: { [weak task] in task?.expirationHandler != nil }
        )
        let service = RediffRefetchService(
            enabled: true,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: StubFullFetcher(),
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: SpyRefetchRecorder(),
            fileRemover: remover,
            taskScheduler: scheduler,
            now: { 100 * Self.day }
        )
        await service.handleRefetchTask(task)
        #expect(remover.handlerInstalledAtSweepTime == true,
                "expiration handler must be live before the orphan sweep runs")
        #expect(scheduler.submittedRequests.count == 1)
        #expect(task.setTaskCompletedCallCount == 1)
    }

    @Test("FileManagerTempFileRemover removes stale rediff-bcopy orphans and spares fresh + unrelated files")
    func orphanSweepRemovesOnlyStaleBCopies() throws {
        let fileManager = FileManager.default
        let tmp = fileManager.temporaryDirectory
        let prefix = URLSessionFullEpisodeFetcher.bcopyFilenamePrefix
        let stale = tmp.appendingPathComponent(prefix + "test-stale-\(UUID().uuidString)")
        let fresh = tmp.appendingPathComponent(prefix + "test-fresh-\(UUID().uuidString)")
        let unrelated = tmp.appendingPathComponent("rediff-unrelated-\(UUID().uuidString)")
        try Data(repeating: 1, count: 64).write(to: stale)
        try Data(repeating: 2, count: 64).write(to: fresh)
        try Data(repeating: 3, count: 64).write(to: unrelated)
        defer {
            try? fileManager.removeItem(at: stale)
            try? fileManager.removeItem(at: fresh)
            try? fileManager.removeItem(at: unrelated)
        }
        // Age the stale one past the floor.
        try fileManager.setAttributes(
            [.modificationDate: Date(timeIntervalSinceNow: -2 * 60 * 60)],
            ofItemAtPath: stale.path
        )

        FileManagerTempFileRemover().removeOrphanedBCopies(olderThan: 60 * 60)

        #expect(!fileManager.fileExists(atPath: stale.path), "stale orphan must be removed")
        #expect(fileManager.fileExists(atPath: fresh.path), "a fresh B-copy (possibly live) must be spared")
        #expect(fileManager.fileExists(atPath: unrelated.path), "non-prefix files are not ours to delete")
    }

    @Test("orphan sweep also reclaims stale rediff-bside shard-cache directories, sparing fresh + real-episode entries (R2)")
    func orphanSweepRemovesStaleBSideShardCacheDirectories() throws {
        let fileManager = FileManager.default
        let root = AnalysisAudioService.shardCacheRootDirectory
        try fileManager.createDirectory(at: root, withIntermediateDirectories: true)
        let prefix = AnalysisAudioBSideDecoder.syntheticEpisodeIDPrefix
        let stale = root.appendingPathComponent(prefix + "test-stale-\(UUID().uuidString)", isDirectory: true)
        let fresh = root.appendingPathComponent(prefix + "test-fresh-\(UUID().uuidString)", isDirectory: true)
        let episode = root.appendingPathComponent("real-episode-\(UUID().uuidString)", isDirectory: true)
        for dir in [stale, fresh, episode] {
            try fileManager.createDirectory(at: dir, withIntermediateDirectories: true)
            try Data(repeating: 7, count: 64).write(to: dir.appendingPathComponent("shard_0.pcm"))
        }
        defer {
            try? fileManager.removeItem(at: stale)
            try? fileManager.removeItem(at: fresh)
            try? fileManager.removeItem(at: episode)
        }
        // Age the stale decode dir past the floor (a dead process's leftover).
        try fileManager.setAttributes(
            [.modificationDate: Date(timeIntervalSinceNow: -2 * 60 * 60)],
            ofItemAtPath: stale.path
        )

        FileManagerTempFileRemover().removeOrphanedBCopies(olderThan: 60 * 60)

        #expect(!fileManager.fileExists(atPath: stale.path), "stale B-side decode dir must be removed")
        #expect(fileManager.fileExists(atPath: fresh.path), "a fresh decode dir (possibly live) must be spared")
        #expect(fileManager.fileExists(atPath: episode.path), "real-episode cache entries are not ours to delete")
    }

    // MARK: - Service: durable run ledger (xsdz.36, R5 coverage)

    @Test("an enabled fire writes one ledger run (rediff_refetch entry point, outcome + counters + bandwidth annotation); a disabled fire writes none")
    func handlerRecordsLedgerRun() async {
        // Disabled fire: no ledger traffic at all (OFF byte-identity).
        do {
            let ledger = SpyRunLedger()
            let service = RediffRefetchService(
                enabled: false,
                enumerator: StubRefetchEnumerator(),
                rangedSampler: StubRangedSampler(),
                localSampler: StubLocalSampler(),
                fullFetcher: StubFullFetcher(),
                bsideFingerprinter: StubBSideFingerprinter(),
                recorder: SpyRefetchRecorder(),
                fileRemover: SpyTempFileRemover(),
                taskScheduler: StubTaskScheduler(),
                runLedger: ledger,
                now: { 100 * Self.day }
            )
            await service.handleRefetchTask(StubBackgroundTask())
            #expect(ledger.startCalls.isEmpty, "disabled fire must not touch the ledger")
            #expect(ledger.finishCalls.isEmpty)
        }

        // Enabled fire, zero candidates → one run, .noEligibleWork, zeroed
        // counters, the bandwidth annotation in deferReason.
        do {
            let ledger = SpyRunLedger()
            let service = RediffRefetchService(
                enabled: true,
                enumerator: StubRefetchEnumerator(),
                rangedSampler: StubRangedSampler(),
                localSampler: StubLocalSampler(),
                fullFetcher: StubFullFetcher(),
                bsideFingerprinter: StubBSideFingerprinter(),
                recorder: SpyRefetchRecorder(),
                fileRemover: SpyTempFileRemover(),
                taskScheduler: StubTaskScheduler(),
                runLedger: ledger,
                now: { 100 * Self.day }
            )
            await service.handleRefetchTask(StubBackgroundTask())
            #expect(ledger.startCalls.count == 1)
            #expect(ledger.startCalls.first?.entryPoint == .rediffRefetch)
            #expect(ledger.startCalls.first?.taskIdentifier == RediffRefetchService.taskIdentifier)
            #expect(ledger.finishCalls.count == 1)
            let finish = ledger.finishCalls.first
            #expect(finish?.runId == ledger.startCalls.first?.runId, "finish must resolve the run startRun opened")
            #expect(finish?.update.outcome == .noEligibleWork)
            #expect(finish?.update.jobsSeen == 0)
            #expect(finish?.update.jobsAdmitted == 0)
            #expect(finish?.update.jobsCompleted == 0)
            #expect(finish?.update.expiration == false)
            #expect(finish?.update.deferReason == "precheckBytes=0 fullFetchBytes=0",
                    "bandwidth accounting rides the deferReason annotation")
        }

        // Enabled fire, one eligible UNCHANGED candidate → .admittedWork with
        // the pre-check bytes annotated (seen 1, admitted 1, completed 0 — no
        // rotation).
        do {
            let ledger = SpyRunLedger()
            let sampler = StubRangedSampler()
            sampler.defaultSample = RemoteAudioSample(
                fingerprint: RediffRefetchPolicy.sampleFingerprint(head: Data("s".utf8), tail: Data("s".utf8), totalLength: 1),
                bytesTransferred: 131_072
            )
            let local = StubLocalSampler()
            local.defaultFingerprint = RediffRefetchPolicy.sampleFingerprint(head: Data("s".utf8), tail: Data("s".utf8), totalLength: 1)
            let enumerator = StubRefetchEnumerator()
            enumerator.candidatesToReturn = [RediffRefetchCandidate(
                assetId: "asset-ledger",
                enclosureURL: URL(string: "https://cdn.example.com/ledger.mp3")!,
                downloadedAt: 0,
                localAudioURL: URL(fileURLWithPath: "/tmp/ledger.mp3"),
                attemptState: .initial
            )]
            let service = RediffRefetchService(
                enabled: true,
                enumerator: enumerator,
                rangedSampler: sampler,
                localSampler: local,
                fullFetcher: StubFullFetcher(),
                bsideFingerprinter: StubBSideFingerprinter(),
                recorder: SpyRefetchRecorder(),
                fileRemover: SpyTempFileRemover(),
                taskScheduler: StubTaskScheduler(),
                runLedger: ledger,
                now: { 100 * Self.day }
            )
            await service.handleRefetchTask(StubBackgroundTask())
            let finish = ledger.finishCalls.first
            #expect(finish?.update.outcome == .admittedWork)
            #expect(finish?.update.jobsSeen == 1)
            #expect(finish?.update.jobsAdmitted == 1)
            #expect(finish?.update.jobsCompleted == 0, "unchanged pre-check is not a completed rotation")
            #expect(finish?.update.deferReason == "precheckBytes=131072 fullFetchBytes=0")
        }
    }

    @Test("an expired fire resolves its ledger run as .expired with the expiration flag set")
    func handlerRecordsExpiredLedgerRun() async {
        let ledger = SpyRunLedger()
        let gated = GatedRefetchEnumerator()
        let service = RediffRefetchService(
            enabled: true,
            enumerator: gated,
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: StubFullFetcher(),
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: SpyRefetchRecorder(),
            fileRemover: SpyTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            runLedger: ledger,
            now: { 100 * Self.day }
        )

        let task = StubBackgroundTask()
        let fire = Task { await service.handleRefetchTask(task) }
        // The sweep is in flight (suspended on the gate) ⇒ the expiration
        // handler is installed and the run row is already open.
        while !(await gated.gate.hasWaiters) { await Task.yield() }

        task.simulateExpiration()
        // Let the expiration hop land on the actor before releasing the sweep.
        for _ in 0..<50 { await Task.yield() }
        await gated.gate.open()
        await fire.value

        #expect(task.setTaskCompletedCallCount == 1)
        #expect(task.completedSuccess == false)
        let finish = ledger.finishCalls.first
        #expect(finish?.update.outcome == .expired)
        #expect(finish?.update.expiration == true)
    }

    @Test("parseTotalLength reads the total after the slash and rejects an unknown total")
    func parseTotalLength() throws {
        #expect(try URLSessionRangedAudioSampler.parseTotalLength("bytes 0-65535/84496614") == 84_496_614)
        // The total after the slash is authoritative even for a 416-style
        // unsatisfiable-range header — the `*` is in the RANGE position, not the
        // total (and the sampler only parses on a 206 anyway).
        #expect(try URLSessionRangedAudioSampler.parseTotalLength("bytes */84496614") == 84_496_614)
        // A genuinely unknown total (`/*`) has nothing to compute a tail from.
        #expect(throws: (any Error).self) {
            try URLSessionRangedAudioSampler.parseTotalLength("bytes 0-100/*")
        }
    }

    // MARK: - Production local sampler: real-file head/tail

    @Test("FileHandleLocalAudioSampler hashes real head/tail bytes + file length")
    func localSamplerRealFile() throws {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("rediff-local-\(UUID().uuidString).bin")
        // 300 KB so head (64K) and tail (64K) do not overlap.
        var bytes = Data(count: 300 * 1024)
        for i in 0..<bytes.count { bytes[i] = UInt8(i & 0xFF) }
        try bytes.write(to: url)
        defer { try? FileManager.default.removeItem(at: url) }

        let sampler = FileHandleLocalAudioSampler()
        let fp = try sampler.sample(fileURL: url, headBytes: 64 * 1024, tailBytes: 64 * 1024)

        let expected = Policy.sampleFingerprint(
            head: bytes.prefix(64 * 1024),
            tail: bytes.suffix(64 * 1024),
            totalLength: Int64(bytes.count)
        )
        #expect(fp == expected)
        #expect(fp.totalLength == Int64(300 * 1024))
    }

    // MARK: - Production B-side fingerprinter: EXACT xsdz.27 extractor

    @Test("EpisodeCaptureBSideFingerprinter applies the SAME resample+fingerprint extractor as A-side")
    func bsideFingerprinterUsesCaptureExtractor() async throws {
        // 4 s of a varied 16 kHz mono signal so the extractor emits fingerprints.
        let sampleCount = 16_000 * 4
        var pcm = [Float](repeating: 0, count: sampleCount)
        for i in 0..<sampleCount {
            pcm[i] = Float(sin(Double(i) * 0.03) * 0.5 + sin(Double(i) * 0.011) * 0.3)
        }
        let decoder = StubAudioDecoder(pcm: pcm)
        let fingerprinter = EpisodeCaptureBSideFingerprinter(decoder: decoder)

        let url = URL(fileURLWithPath: "/tmp/whatever.mp3")  // decoder is stubbed; path unused
        let produced = try await fingerprinter.fingerprint(fileURL: url)

        // Must equal the A-side capture extractor applied to the SAME PCM — one
        // versioned (resampler+fingerprinter) unit for both sides (xsdz.27/.29).
        #expect(produced == EpisodeFingerprintCapture.fingerprints(mono16kHz: pcm))
        #expect(!produced.isEmpty, "a 4s signal must yield subfingerprints")
        #expect(decoder.calls == [url])
    }

    // MARK: - k-way fetch (playhead-xsdz.36.2)

    @Test("k-way default is 1 (single-fetch bandwidth) in every config + the production activation constant")
    func kWayDefaultsAreOne() {
        #expect(RediffRefetchPolicy.Configuration.default.kWayFetchCount == 1)
        #expect(RediffRefetchPolicy.Configuration.production.kWayFetchCount == 1)
        #expect(RediffActivation.productionKWayFetchCount == 1,
                "production stays single-fetch until a deliberate bandwidth go/no-go")
    }

    @Test("K=1 (default): a rotator triggers EXACTLY ONE fetch under the iPhone persona; the consumer gets one copy")
    func kWayK1SingleFetchBackwardCompat() async {
        let (fetcher, consumer, remover, recorder) = kWayDoubles()
        await runKWaySweep(kWayFetchCount: 1, fetcher: fetcher, consumer: consumer,
                           remover: remover, recorder: recorder)

        #expect(fetcher.calls.count == 1, "K=1 → exactly today's single B-side fetch")
        #expect(fetcher.calls.first?.persona?.name == "applecoremedia-iphone")
        #expect(consumer.consumedFileURLs.count == 1)
        #expect(consumer.consumedFileURLs.first == [URL(fileURLWithPath: "/tmp/kway-bcopy-0.mp3")])
        #expect(remover.removed == [URL(fileURLWithPath: "/tmp/kway-bcopy-0.mp3")], "the one B-copy is deleted")
        guard case let .rotated(_, cost, _, _) = recorder.outcomes.first else {
            Issue.record("expected .rotated"); return
        }
        #expect(cost.fullFetchBytes == 54_000_000, "K=1 bandwidth = one full fetch")
    }

    @Test("K=3: three DISTINCT-persona fetches in iPhone→Mac→Overcast order; consumer gets all 3; all 3 deleted; bandwidth is 3×")
    func kWayK3FetchesThreeDistinctPersonasAndDeletesAll() async {
        let (fetcher, consumer, remover, recorder) = kWayDoubles()
        await runKWaySweep(kWayFetchCount: 3, fetcher: fetcher, consumer: consumer,
                           remover: remover, recorder: recorder)

        // Three fetches, drawn in the divergence-reliable order, all distinct,
        // none curl/generic.
        #expect(fetcher.calls.count == 3, "K=3 → three B-side fetches")
        #expect(fetcher.calls.map { $0.persona?.name }
            == ["applecoremedia-iphone", "applecoremedia-macintosh", "overcast"])
        #expect(Set(fetcher.calls.compactMap { $0.persona?.name }).count == 3, "no persona reused within a batch")
        for call in fetcher.calls {
            let ua = (call.persona?.userAgent ?? "").lowercased()
            #expect(!ua.contains("curl") && !ua.contains("wget"))
        }

        // The consumer stages ALL three at once (one k-way handoff).
        #expect(consumer.consumedFileURLs.count == 1)
        #expect(consumer.consumedFileURLs.first?.count == 3)

        // Never-persist-B: every fetched copy is deleted.
        let expectedFiles = (0..<3).map { URL(fileURLWithPath: "/tmp/kway-bcopy-\($0).mp3") }
        #expect(Set(remover.removed) == Set(expectedFiles), "all K B-copies deleted")

        // Bandwidth accounts K× the full fetch.
        guard case let .rotated(_, cost, _, _) = recorder.outcomes.first else {
            Issue.record("expected .rotated"); return
        }
        #expect(cost.fullFetchBytes == 3 * 54_000_000)
    }

    @Test("k-way: a mid-batch fetch throw still DELETES the already-fetched copies and does NOT consume (never-persist-B)")
    func kWayMidBatchFetchThrowDeletesFetchedCopies() async {
        let sampler = StubRangedSampler()
        sampler.defaultSample = RemoteAudioSample(
            fingerprint: Self.fingerprint("fresh", total: 55_000_000), bytesTransferred: 131_072)
        let local = StubLocalSampler()
        local.defaultFingerprint = Self.fingerprint("played", total: 54_000_000)  // rotated
        let fetcher = KWaySpyFullFetcher()
        fetcher.throwOnCallIndex = 1  // first fetch succeeds, the SECOND throws mid-batch
        let consumer = SpyKWayBSideConsumer()
        let remover = SpyTempFileRemover()
        let recorder = SpyRefetchRecorder()
        let enumerator = StubRefetchEnumerator()
        enumerator.candidatesToReturn = [Self.candidate(downloadedAt: 0)]
        let service = RediffRefetchService(
            enabled: true,
            config: RediffRefetchPolicy.Configuration(kWayFetchCount: 3),
            enumerator: enumerator,
            rangedSampler: sampler,
            localSampler: local,
            fullFetcher: fetcher,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: recorder,
            fileRemover: remover,
            taskScheduler: StubTaskScheduler(),
            bsideConsumer: consumer,
            now: { 100 * Self.day }
        )
        await service.runRefetchSweep()

        // The first copy WAS fetched before the throw; it must be deleted.
        #expect(remover.removed == [URL(fileURLWithPath: "/tmp/kway-bcopy-0.mp3")],
                "the already-fetched B-copy is deleted even when a later fetch in the batch throws")
        // A partial batch is never consumed → the candidate retries under the R2 policy.
        #expect(consumer.consumedFileURLs.isEmpty, "a mid-batch fetch throw must not stage/consume a partial batch")
        guard case .failed = recorder.outcomes.first else {
            Issue.record("expected .failed, got \(String(describing: recorder.outcomes.first))"); return
        }
    }

    /// Fresh k-way doubles for a rotator sweep.
    private func kWayDoubles() -> (KWaySpyFullFetcher, SpyKWayBSideConsumer, SpyTempFileRemover, SpyRefetchRecorder) {
        (KWaySpyFullFetcher(), SpyKWayBSideConsumer(), SpyTempFileRemover(), SpyRefetchRecorder())
    }

    /// Drive one sweep of a single ROTATED, eligible candidate at the given K.
    private func runKWaySweep(
        kWayFetchCount: Int,
        fetcher: KWaySpyFullFetcher,
        consumer: SpyKWayBSideConsumer,
        remover: SpyTempFileRemover,
        recorder: SpyRefetchRecorder
    ) async {
        let sampler = StubRangedSampler()
        sampler.defaultSample = RemoteAudioSample(
            fingerprint: Self.fingerprint("fresh", total: 55_000_000), bytesTransferred: 131_072)
        let local = StubLocalSampler()
        local.defaultFingerprint = Self.fingerprint("played", total: 54_000_000)  // differs → rotated
        let enumerator = StubRefetchEnumerator()
        enumerator.candidatesToReturn = [Self.candidate(downloadedAt: 0)]
        let service = RediffRefetchService(
            enabled: true,
            config: RediffRefetchPolicy.Configuration(kWayFetchCount: kWayFetchCount),
            enumerator: enumerator,
            rangedSampler: sampler,
            localSampler: local,
            fullFetcher: fetcher,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: recorder,
            fileRemover: remover,
            taskScheduler: StubTaskScheduler(),
            bsideConsumer: consumer,
            now: { 100 * Self.day }
        )
        await service.runRefetchSweep()
    }

    // MARK: - Helpers

    private func makeService(
        candidates: [RediffRefetchCandidate],
        rangedSampler: any RangedAudioSampling = StubRangedSampler(),
        localSampler: any LocalAudioSampling = StubLocalSampler(),
        fullFetcher: any FullEpisodeFetching = StubFullFetcher(),
        bsideFingerprinter: any RediffBSideFingerprinting = StubBSideFingerprinter(),
        recorder: any RediffRefetchRecording = SpyRefetchRecorder(),
        scheduler: any BackgroundTaskScheduling = StubTaskScheduler(),
        enabled: Bool = true,
        now: @escaping @Sendable () -> Double = { RediffRefetchTests.day * 100 }
    ) -> RediffRefetchService {
        let enumerator = StubRefetchEnumerator()
        enumerator.candidatesToReturn = candidates
        return RediffRefetchService(
            enabled: enabled,
            enumerator: enumerator,
            rangedSampler: rangedSampler,
            localSampler: localSampler,
            fullFetcher: fullFetcher,
            bsideFingerprinter: bsideFingerprinter,
            recorder: recorder,
            fileRemover: FileManagerTempFileRemover(),
            taskScheduler: scheduler,
            now: now
        )
    }
}

// MARK: - Persona + fetch-hygiene unit suite (playhead-xsdz.45 / xsdz.36.3)

/// Pure, deterministic coverage of the request-context persona and the shared
/// cache-buster/request builder — no network. The on-the-wire counterparts
/// (persona headers + `_cb` actually sent) live in `RediffRefetchNetworkTests`.
@Suite("RediffFetchPersona + fetch hygiene (playhead-xsdz.45 / xsdz.36.3)")
struct RediffFetchPersonaTests {

    @Test("apply stamps UA + Accept + Accept-Language when the persona supplies them")
    func applyStampsHeaders() {
        let persona = RediffFetchPersona(name: "p", userAgent: "UA/1", accept: "audio/*", acceptLanguage: "en-US")
        var request = URLRequest(url: URL(string: "https://cdn.example.com/y.mp3")!)
        persona.apply(to: &request)
        #expect(request.value(forHTTPHeaderField: "User-Agent") == "UA/1")
        #expect(request.value(forHTTPHeaderField: "Accept") == "audio/*")
        #expect(request.value(forHTTPHeaderField: "Accept-Language") == "en-US")
    }

    @Test("a nil/absent persona AND the empty-UA persona both leave NO User-Agent header")
    func nilAndEmptyLeaveNoUA() {
        // Absent persona via the shared builder == today's exact request.
        let base = RediffFetchRequest.makeBaseRequest(
            cacheBustedURL: URL(string: "https://cdn.example.com/y.mp3?_cb=t")!, persona: nil
        )
        #expect(base.value(forHTTPHeaderField: "User-Agent") == nil, "absent persona ⇒ no UA header")

        // The empty-UA persona is a NON-nil persona whose empty UA still sets
        // no header (the non-empty guard) — the AdsWizz-safe "system default".
        var request = URLRequest(url: URL(string: "https://cdn.example.com/y.mp3")!)
        RediffFetchPersona.emptyUA.apply(to: &request)
        #expect(request.value(forHTTPHeaderField: "User-Agent") == nil, "empty-UA persona ⇒ no UA header")
        #expect(RediffFetchPersona.emptyUA.userAgent == "")
    }

    @Test("makeBaseRequest sets a cache-ignoring policy + GET + no-cellular, and applies the persona UA")
    func baseRequestCachePolicy() {
        let request = RediffFetchRequest.makeBaseRequest(
            cacheBustedURL: URL(string: "https://cdn.example.com/y.mp3?_cb=t")!, persona: .default
        )
        #expect(request.cachePolicy == .reloadIgnoringLocalCacheData, "rediff fetches never read a stale local cache")
        #expect(request.httpMethod == "GET")
        #expect(request.allowsCellularAccess == false, "WiFi-only half of the policy is preserved")
        #expect(request.value(forHTTPHeaderField: "User-Agent") == RediffFetchPersona.appleCoreMediaIPhone.userAgent)
    }

    @Test("makeWiFiOnlySession disables the URL cache (xsdz.36.3) and stays WiFi-only (xsdz.28)")
    func wifiOnlySessionDisablesCache() {
        let config = URLSessionRangedAudioSampler.makeWiFiOnlySession().configuration
        // playhead-xsdz.36.3: the third cache-defeating guard (with the `_cb`
        // query item and the reload cache policy) — the session must hold NO
        // URL cache so a rediff body can never be served from a stale entry.
        #expect(config.urlCache == nil, "rediff session must disable the URL cache entirely")
        // playhead-xsdz.28: cache-busting must NOT relax the WiFi-only pins.
        #expect(config.allowsCellularAccess == false)
        #expect(config.allowsConstrainedNetworkAccess == false)
        #expect(config.allowsExpensiveNetworkAccess == false)
    }

    @Test("cacheBustedURL appends a unique _cb, preserves an existing query string, and never clobbers")
    func cacheBustedURLAppends() {
        // No existing query → the sole query item is _cb.
        let a = RediffFetchRequest.cacheBustedURL(URL(string: "https://cdn.example.com/ep.mp3")!, token: "t1")
        #expect(URLComponents(url: a, resolvingAgainstBaseURL: false)?.queryItems == [URLQueryItem(name: "_cb", value: "t1")])

        // An existing query is preserved; _cb is appended (foo/baz untouched).
        let b = RediffFetchRequest.cacheBustedURL(URL(string: "https://cdn.example.com/ep.mp3?foo=bar&baz=1")!, token: "t2")
        let bItems = URLComponents(url: b, resolvingAgainstBaseURL: false)?.queryItems ?? []
        #expect(bItems.contains(URLQueryItem(name: "foo", value: "bar")))
        #expect(bItems.contains(URLQueryItem(name: "baz", value: "1")))
        #expect(bItems.contains(URLQueryItem(name: "_cb", value: "t2")))
        #expect(bItems.count == 3, "existing items are kept, not clobbered")

        // Distinct tokens → distinct URLs (uniqueness at the URL level).
        let c = RediffFetchRequest.cacheBustedURL(URL(string: "https://cdn.example.com/ep.mp3")!, token: "t3")
        #expect(a != c)
    }

    @Test("cacheBustedURL preserves pre-existing percent-encoding BYTE-FOR-BYTE (no queryItems round-trip mangling) and keeps the fragment")
    func cacheBustedURLPreservesEncodingByteForByte() {
        // A `queryItems` round-trip decodes `%2F`→`/`, `%2B`→`+`, `%3A`→`:`,
        // silently changing WHICH object a redirect/tracking/signed param
        // resolves to. The cache-buster must leave the existing query verbatim.
        let signed = URL(string: "https://cdn.example.com/ep.mp3?sig=aB%2FcD%3D%3D&exp=123")!
        let out = RediffFetchRequest.cacheBustedURL(signed, token: "TOK")
        #expect(
            out.absoluteString == "https://cdn.example.com/ep.mp3?sig=aB%2FcD%3D%3D&exp=123&_cb=TOK",
            "existing %2F/%3D are preserved byte-for-byte; only _cb is appended"
        )

        // A nested percent-encoded URL param — the worst mangling case.
        let redirect = URL(string: "https://cdn.example.com/ep.mp3?u=https%3A%2F%2Fx.com%2Fy")!
        #expect(
            RediffFetchRequest.cacheBustedURL(redirect, token: "T").absoluteString
                == "https://cdn.example.com/ep.mp3?u=https%3A%2F%2Fx.com%2Fy&_cb=T",
            "nested encoded URL param is not decoded"
        )

        // A `%2B` (encoded plus) must NOT collapse to a bare `+` (form-space).
        let plus = URL(string: "https://cdn.example.com/ep.mp3?t=a%2Bb")!
        #expect(RediffFetchRequest.cacheBustedURL(plus, token: "T").absoluteString
            == "https://cdn.example.com/ep.mp3?t=a%2Bb&_cb=T")

        // The URL fragment is preserved and _cb lands in the query, not the frag.
        let frag = URL(string: "https://cdn.example.com/ep.mp3?a=b#chapter")!
        #expect(RediffFetchRequest.cacheBustedURL(frag, token: "T").absoluteString
            == "https://cdn.example.com/ep.mp3?a=b&_cb=T#chapter")

        // A custom token carrying query-reserved chars is encoded, never
        // corrupting the query with a stray `&`/`=`.
        let messy = RediffFetchRequest.cacheBustedURL(URL(string: "https://cdn.example.com/ep.mp3")!, token: "a&b=c")
        let items = URLComponents(url: messy, resolvingAgainstBaseURL: false)?.queryItems ?? []
        #expect(items == [URLQueryItem(name: "_cb", value: "a&b=c")], "the token round-trips as a single value")
    }

    @Test("cacheBustedURL percent-encodes a non-ASCII / reserved injected token to a valid all-ASCII query (never a raw byte, never a silently-dropped _cb)")
    func cacheBustedURLEncodesNonASCIIToken() {
        // RFC 3986 "unreserved" is ASCII-only. A token carrying a non-ASCII
        // char (`é`) plus query-reserved chars (`#`, `/`, space) must be fully
        // percent-encoded: leaving `é` RAW would either void `URLComponents.url`
        // (dropping the whole cache-buster — a rediff served a stale cached
        // stitch) or emit a non-ASCII query. `CharacterSet.alphanumerics` (the
        // Unicode set) would leave `é` raw; the ASCII unreserved set encodes it.
        let out = RediffFetchRequest.cacheBustedURL(
            URL(string: "https://cdn.example.com/ep.mp3?a=b")!, token: "café#/ x"
        )
        // (1) the busted URL is emitted and is fully ASCII (percent-encoded) —
        //     fails if `é` is left raw in the query.
        #expect(out.absoluteString.canBeConverted(to: .ascii),
                "a non-ASCII token must be percent-encoded, not left as a raw byte")
        let items = URLComponents(url: out, resolvingAgainstBaseURL: false)?.queryItems ?? []
        // (2) the `_cb` item is PRESENT (never silently dropped by a nil-`url`
        //     fallback) and round-trips back to exactly the injected token.
        #expect(items.first(where: { $0.name == "_cb" })?.value == "café#/ x",
                "the token decodes back to exactly what was injected")
        // (3) the pre-existing query is untouched.
        #expect(items.contains(URLQueryItem(name: "a", value: "b")))
    }

    @Test("the curated bank is the divergence-reliable set, default = AppleCoreMedia-iPhone, and excludes curl/generic UAs")
    func curatedBankMembership() {
        #expect(Set(RediffFetchPersona.curatedBank.map(\.name))
            == ["applecoremedia-iphone", "applecoremedia-macintosh", "overcast", "empty-ua"])
        #expect(RediffFetchPersona.default == .appleCoreMediaIPhone, "the designated default persona")

        // AppleCoreMedia UAs are the iOS media-stack form (the divergence core).
        #expect(RediffFetchPersona.appleCoreMediaIPhone.userAgent?.contains("AppleCoreMedia") == true)
        #expect(RediffFetchPersona.appleCoreMediaIPhone.userAgent?.contains("iPhone") == true)
        #expect(RediffFetchPersona.appleCoreMediaMac.userAgent?.contains("AppleCoreMedia") == true)
        #expect(RediffFetchPersona.appleCoreMediaMac.userAgent?.contains("Macintosh") == true)

        // AdsWizz p_f_skip gotcha: curl/generic UAs are DELIBERATELY excluded
        // (AdsWizz classifies them "unclassified" and serves no ad stitch).
        for persona in RediffFetchPersona.curatedBank {
            let ua = (persona.userAgent ?? "").lowercased()
            #expect(!ua.contains("curl"), "curl/generic UAs must not be in the bank")
            #expect(!ua.contains("wget"))
        }
    }

    // MARK: - k-way persona selection (playhead-xsdz.36.2)

    @Test("kWayPersonas draws K DISTINCT personas in iPhone→Mac→Overcast→empty order, K=1 == default, clamped, never curl")
    func kWayPersonaSelection() {
        #expect(RediffFetchPersona.kWayPersonas(count: 1).map(\.name) == ["applecoremedia-iphone"])
        #expect(RediffFetchPersona.kWayPersonas(count: 2).map(\.name)
            == ["applecoremedia-iphone", "applecoremedia-macintosh"])
        #expect(RediffFetchPersona.kWayPersonas(count: 3).map(\.name)
            == ["applecoremedia-iphone", "applecoremedia-macintosh", "overcast"])
        #expect(RediffFetchPersona.kWayPersonas(count: 4).map(\.name)
            == ["applecoremedia-iphone", "applecoremedia-macintosh", "overcast", "empty-ua"])

        // K=1 is EXACTLY the default persona → a K=1 batch is byte-identical to
        // today's single default-persona fetch.
        #expect(RediffFetchPersona.kWayPersonas(count: 1) == [.default])

        // The iPhone+Mac divergence CORE always leads.
        let three = RediffFetchPersona.kWayPersonas(count: 3)
        #expect(three.first == .appleCoreMediaIPhone)
        #expect(three[1] == .appleCoreMediaMac)

        // DISTINCT within a batch (never reused).
        #expect(Set(three.map(\.name)).count == 3)

        // Clamped to the bank size above it, and to ≥1 below it.
        #expect(RediffFetchPersona.kWayPersonas(count: 9) == RediffFetchPersona.curatedBank)
        #expect(RediffFetchPersona.kWayPersonas(count: 0) == [.appleCoreMediaIPhone], "clamped to ≥1")

        // Never a curl/generic UA (the bank excludes them).
        for persona in RediffFetchPersona.kWayPersonas(count: 4) {
            let ua = (persona.userAgent ?? "").lowercased()
            #expect(!ua.contains("curl") && !ua.contains("wget"))
        }
    }
}

// MARK: - Seam stubs

/// Deterministic, thread-safe cache-buster token source for the network tests:
/// "cb-0", "cb-1", … so per-call uniqueness (and head/tail sharing) is exact.
final class TokenCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var n = 0
    func next() -> String {
        lock.lock(); defer { lock.unlock() }
        defer { n += 1 }
        return "cb-\(n)"
    }
}

final class StubRefetchEnumerator: RediffRefetchEnumerating, @unchecked Sendable {
    var candidatesToReturn: [RediffRefetchCandidate] = []
    private(set) var callCount = 0
    func candidates() async -> [RediffRefetchCandidate] {
        callCount += 1
        return candidatesToReturn
    }
}

final class StubRangedSampler: RangedAudioSampling, @unchecked Sendable {
    var defaultSample: RemoteAudioSample?
    var errorToThrow: Error?
    private(set) var calls: [(url: URL, headBytes: Int, tailBytes: Int)] = []
    func sample(url: URL, headBytes: Int, tailBytes: Int) async throws -> RemoteAudioSample {
        calls.append((url, headBytes, tailBytes))
        if let errorToThrow { throw errorToThrow }
        guard let defaultSample else { throw NSError(domain: "StubRangedSampler", code: 1) }
        return defaultSample
    }
}

final class StubLocalSampler: LocalAudioSampling, @unchecked Sendable {
    var defaultFingerprint: RediffRefetchPolicy.AudioSampleFingerprint?
    var errorToThrow: Error?
    private(set) var calls: [URL] = []
    func sample(fileURL: URL, headBytes: Int, tailBytes: Int) throws -> RediffRefetchPolicy.AudioSampleFingerprint {
        calls.append(fileURL)
        if let errorToThrow { throw errorToThrow }
        guard let defaultFingerprint else { throw NSError(domain: "StubLocalSampler", code: 1) }
        return defaultFingerprint
    }
}

final class StubFullFetcher: FullEpisodeFetching, @unchecked Sendable {
    var fileToReturn: URL?
    var byteCount = 0
    var errorToThrow: Error?
    private(set) var calls: [URL] = []
    func download(url: URL) async throws -> (fileURL: URL, byteCount: Int) {
        calls.append(url)
        if let errorToThrow { throw errorToThrow }
        guard let fileToReturn else { throw NSError(domain: "StubFullFetcher", code: 1) }
        return (fileToReturn, byteCount)
    }
}

/// playhead-xsdz.36.2 (k-way): records the (url, persona) of every fetch and
/// returns a DISTINCT fake temp URL per call (`/tmp/kway-bcopy-<n>.mp3`), so a
/// batch's persona ordering, distinctness, per-copy deletion, and bandwidth are
/// directly assertable. Implements BOTH `download` overloads so the persona the
/// service passes is captured (not swallowed by the default extension).
final class KWaySpyFullFetcher: FullEpisodeFetching, @unchecked Sendable {
    struct Call: Sendable { let url: URL; let persona: RediffFetchPersona? }
    private(set) var calls: [Call] = []
    var byteCountPerFetch = 54_000_000
    /// If set, the fetch at this 0-based call index THROWS (models a mid-batch
    /// network failure) — nothing is returned for that call, so the earlier
    /// copies are the only ones the caller must delete.
    var throwOnCallIndex: Int?
    func download(url: URL) async throws -> (fileURL: URL, byteCount: Int) {
        try await download(url: url, persona: nil)
    }
    func download(url: URL, persona: RediffFetchPersona?) async throws -> (fileURL: URL, byteCount: Int) {
        let index = calls.count
        if index == throwOnCallIndex {
            throw NSError(domain: "KWaySpyFullFetcher", code: 1)
        }
        calls.append(Call(url: url, persona: persona))
        return (URL(fileURLWithPath: "/tmp/kway-bcopy-\(index).mp3"), byteCountPerFetch)
    }
}

/// playhead-xsdz.36.2 (k-way): records the file-URL LIST handed to each
/// `consumeRotatedBSides` so the "stage ALL K copies at once" handoff is
/// assertable. Implements the k-way method directly (not the single-file default).
final class SpyKWayBSideConsumer: RediffBSideConsuming, @unchecked Sendable {
    private(set) var consumedFileURLs: [[URL]] = []
    var errorToThrow: Error?
    func consumeRotatedBSide(assetId: String, fileURL: URL) async throws {
        try await consumeRotatedBSides(assetId: assetId, fileURLs: [fileURL])
    }
    func consumeRotatedBSides(assetId: String, fileURLs: [URL]) async throws {
        consumedFileURLs.append(fileURLs)
        if let errorToThrow { throw errorToThrow }
    }
}

/// playhead-xsdz.36.4 (day-0 byte-exact mint): records the FLAT B-side URL list
/// handed to each `mintByteExactDayZeroMarks` call and returns a configurable
/// mark count, so the day-0 marked/unmarked outcome split (and the never-persist
/// deletion + bandwidth accounting around it) is directly assertable without a
/// real byte differ / store.
final class SpyDayZeroMinter: RediffDayZeroMinting, @unchecked Sendable {
    private(set) var calls: [(assetId: String, bSideURLs: [URL])] = []
    /// Marks to report per call. Default `1` (a marked day-0 run); set `0` to
    /// exercise the poisoning-safe unmarked path (no resolve, no state advance).
    var markCountToReturn = 1
    func mintByteExactDayZeroMarks(assetId: String, bSideURLs: [URL]) async -> Int {
        calls.append((assetId, bSideURLs))
        return markCountToReturn
    }
}

/// playhead-xsdz.36.4: returns a provided list of REAL on-disk file URLs, one
/// per fetch call (by index, cycling if fewer files than fetches), so the day-0
/// byte-exact mint can read genuine A/B bytes off disk. Records the (url,
/// persona) of every fetch like `KWaySpyFullFetcher`.
final class RealFilesKWayFetcher: FullEpisodeFetching, @unchecked Sendable {
    struct Call: Sendable { let url: URL; let persona: RediffFetchPersona? }
    private(set) var calls: [Call] = []
    let files: [URL]
    var byteCountPerFetch = 54_000_000
    init(files: [URL]) { self.files = files }
    func download(url: URL) async throws -> (fileURL: URL, byteCount: Int) {
        try await download(url: url, persona: nil)
    }
    func download(url: URL, persona: RediffFetchPersona?) async throws -> (fileURL: URL, byteCount: Int) {
        let index = calls.count
        calls.append(Call(url: url, persona: persona))
        return (files[index % files.count], byteCountPerFetch)
    }
}

final class StubBSideFingerprinter: RediffBSideFingerprinting, @unchecked Sendable {
    var fingerprintsToReturn: [UInt32] = [1, 2, 3]
    var errorToThrow: Error?
    private(set) var calls: [URL] = []
    func fingerprint(fileURL: URL) async throws -> [UInt32] {
        calls.append(fileURL)
        if let errorToThrow { throw errorToThrow }
        return fingerprintsToReturn
    }
}

final class SpyRefetchRecorder: RediffRefetchRecording, @unchecked Sendable {
    private(set) var outcomes: [RediffRefetchPolicy.Outcome] = []
    func recordOutcome(_ outcome: RediffRefetchPolicy.Outcome) async {
        outcomes.append(outcome)
    }
}

final class SpyTempFileRemover: RediffTempFileRemoving, @unchecked Sendable {
    private(set) var removed: [URL] = []
    private(set) var orphanSweepAges: [TimeInterval] = []
    func remove(_ fileURL: URL) { removed.append(fileURL) }
    func removeOrphanedBCopies(olderThan age: TimeInterval) { orphanSweepAges.append(age) }
}

/// R5: records the `handleRefetchTask` ledger traffic (start/finish pairs)
/// so the rediff run-row wiring — entry point, outcome mapping, counters,
/// bandwidth annotation — is pinned by test, not just by dogfood forensics.
final class SpyRunLedger: BackgroundTaskRunLedger, @unchecked Sendable {
    struct StartCall {
        let runId: String
        let entryPoint: BackgroundTaskRunEntryPoint
        let taskIdentifier: String
    }
    struct FinishCall {
        let runId: String
        let update: BackgroundTaskRunOutcomeUpdate
    }
    private(set) var startCalls: [StartCall] = []
    private(set) var finishCalls: [FinishCall] = []

    func startRun(
        entryPoint: BackgroundTaskRunEntryPoint,
        taskIdentifier: String,
        taskInstanceID: String?,
        scenePhase: String?
    ) async -> String {
        let runId = UUID().uuidString
        startCalls.append(StartCall(runId: runId, entryPoint: entryPoint, taskIdentifier: taskIdentifier))
        return runId
    }

    func recordRunStart(
        runId: String,
        entryPoint: BackgroundTaskRunEntryPoint,
        taskIdentifier: String,
        taskInstanceID: String?,
        scenePhase: String?
    ) async {
        startCalls.append(StartCall(runId: runId, entryPoint: entryPoint, taskIdentifier: taskIdentifier))
    }

    @discardableResult
    func finishRun(runId: String, update: BackgroundTaskRunOutcomeUpdate) async -> Bool {
        finishCalls.append(FinishCall(runId: runId, update: update))
        return true
    }

    func fetchLatestRun(for entryPoint: BackgroundTaskRunEntryPoint) async -> BackgroundTaskRunRecord? { nil }
    func fetchRecentRuns(limit: Int) async -> [BackgroundTaskRunRecord] { [] }
    func fetchLatestRun(forAssetId assetId: String) async -> BackgroundTaskRunRecord? { nil }
    @discardableResult
    func reapOrphansAtLaunch(startedBefore: Double) async -> Int { 0 }
}

/// R3: remover that snapshots an arbitrary probe (e.g. "is the task's
/// expiration handler installed?") at the moment the orphan sweep runs, so
/// ordering inside `handleRefetchTask` is directly assertable.
final class HandlerOrderSpyTempFileRemover: RediffTempFileRemoving, @unchecked Sendable {
    private let probe: @Sendable () -> Bool
    private(set) var handlerInstalledAtSweepTime: Bool?
    init(probe: @escaping @Sendable () -> Bool) { self.probe = probe }
    func remove(_ fileURL: URL) {}
    func removeOrphanedBCopies(olderThan age: TimeInterval) {
        handlerInstalledAtSweepTime = probe()
    }
}

/// Reusable open/close latch for suspending a stub mid-call.
actor TestGate {
    private var opened = false
    private var waiters: [CheckedContinuation<Void, Never>] = []
    func open() {
        opened = true
        for waiter in waiters { waiter.resume() }
        waiters.removeAll()
    }
    func close() { opened = false }
    func wait() async {
        if opened { return }
        await withCheckedContinuation { waiters.append($0) }
    }
    /// True once a caller is suspended on the gate — the race-free "the sweep
    /// is in flight" probe for tests.
    var hasWaiters: Bool { !waiters.isEmpty }
}

/// Enumerator whose `candidates()` suspends on a gate — lets a test hold a
/// sweep in flight while it drives overlapping fires / late expirations.
final class GatedRefetchEnumerator: RediffRefetchEnumerating, @unchecked Sendable {
    let gate = TestGate()
    var candidatesToReturn: [RediffRefetchCandidate] = []
    func candidates() async -> [RediffRefetchCandidate] {
        await gate.wait()
        return candidatesToReturn
    }
}

final class StubAudioDecoder: AudioFileDecoding, @unchecked Sendable {
    let pcm: [Float]
    private(set) var calls: [URL] = []
    init(pcm: [Float]) { self.pcm = pcm }
    func decodeMono16kHz(fileURL: URL) async throws -> [Float] {
        calls.append(fileURL)
        return pcm
    }
}

// MARK: - Network sub-suite (serialized: shares RediffMockURLProtocol static)

/// The URLSession-backed production conformers, driven through an in-memory
/// URLProtocol. Serialized because the mock records requests in shared static
/// state — two of these running concurrently would interleave their captures.
@Suite("RediffRefetch network conformers (playhead-xsdz.28)", .serialized)
struct RediffRefetchNetworkTests {

    // NO HEAD / NO ETag ANYWHERE (acceptance).
    @Test("URLSessionRangedAudioSampler issues only range GETs — no HEAD, no conditional headers")
    func productionSamplerIssuesOnlyRangeGets() async throws {
        RediffMockURLProtocol.reset(total: 84_496_614)
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [RediffMockURLProtocol.self]
        let sampler = URLSessionRangedAudioSampler(session: URLSession(configuration: config))

        let sample = try await sampler.sample(
            url: URL(string: "https://cdn.example.com/ep.mp3")!,
            headBytes: 64 * 1024, tailBytes: 64 * 1024
        )

        let recorded = RediffMockURLProtocol.snapshot()
        #expect(recorded.count == 2, "exactly a head + tail request")
        for r in recorded {
            #expect(r.method == "GET", "must be GET, never HEAD (Acast HEAD is broken)")
            #expect(r.range != nil, "every request carries a byte Range")
            #expect(r.ifNoneMatch == nil, "no ETag/conditional-GET (ETags are effectively absent on podcast CDNs)")
            #expect(r.ifModifiedSince == nil, "no Last-Modified conditional")
        }
        #expect(recorded.first?.range == "bytes=0-65535")
        #expect(recorded.last?.range == "bytes=84431078-84496613", "tail range = last 64KB of the parsed total")
        #expect(sample.fingerprint.totalLength == 84_496_614, "total length parsed from Content-Range, not HEAD")
        #expect(sample.bytesTransferred == 131_072)

        // playhead-xsdz.36.3: every rediff request is cache-busted; the head
        // and tail of ONE sample share the per-sample token (coherent pair).
        #expect(recorded.allSatisfy { $0.cacheBuster != nil }, "every ranged GET carries a _cb cache-buster")
        #expect(recorded[0].cacheBuster == recorded[1].cacheBuster, "head + tail share one per-sample token")
        // playhead-xsdz.45: a nil (absent) persona sets NO explicit UA header —
        // byte-identical to the xsdz.28 request (system default UA).
        #expect(recorded.allSatisfy { $0.userAgent == nil }, "absent persona ⇒ no explicit User-Agent header")
    }

    @Test("URLSessionFullEpisodeFetcher downloads to a temp file the caller can delete")
    func productionFullFetcherDownloadsToTemp() async throws {
        RediffMockURLProtocol.reset(total: 4_096)
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [RediffMockURLProtocol.self]
        let fetcher = URLSessionFullEpisodeFetcher(session: URLSession(configuration: config))

        let (fileURL, byteCount) = try await fetcher.download(url: URL(string: "https://cdn.example.com/ep.mp3")!)
        defer { try? FileManager.default.removeItem(at: fileURL) }

        #expect(FileManager.default.fileExists(atPath: fileURL.path))
        #expect(byteCount == 4_096)
        #expect(fileURL.lastPathComponent.hasPrefix("rediff-bcopy-"), "downloaded into a caller-owned temp")

        let recorded = RediffMockURLProtocol.snapshot()
        #expect(recorded.count == 1)
        #expect(recorded.first?.method == "GET")
        #expect(recorded.first?.range == nil, "a full fetch is a plain GET (no Range)")
        // playhead-xsdz.36.3 / xsdz.45: the full B-side fetch is cache-busted,
        // and an absent persona sets no explicit UA header.
        #expect(recorded.first?.cacheBuster != nil, "full B-side fetch carries a _cb cache-buster")
        #expect(recorded.first?.userAgent == nil, "absent persona ⇒ no explicit User-Agent header")
    }

    // MARK: - Persona + cache-buster on the wire (playhead-xsdz.45 / xsdz.36.3)

    @Test("the default persona stamps the AppleCoreMedia-iPhone UA on both the ranged pre-check and the full fetch")
    func defaultPersonaStampsAppleCoreMediaIPhone() async throws {
        RediffMockURLProtocol.reset(total: 200_000)
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [RediffMockURLProtocol.self]
        let session = URLSession(configuration: config)
        let ua = RediffFetchPersona.appleCoreMediaIPhone.userAgent

        let sampler = URLSessionRangedAudioSampler(session: session, persona: .default)
        _ = try await sampler.sample(
            url: URL(string: "https://cdn.example.com/ep.mp3")!,
            headBytes: 64 * 1024, tailBytes: 64 * 1024
        )
        let fetcher = URLSessionFullEpisodeFetcher(session: session, persona: .default)
        let (fileURL, _) = try await fetcher.download(url: URL(string: "https://cdn.example.com/ep.mp3")!)
        defer { try? FileManager.default.removeItem(at: fileURL) }

        let recorded = RediffMockURLProtocol.snapshot()
        #expect(recorded.count == 3, "head + tail + full")
        #expect(recorded.allSatisfy { $0.userAgent == ua }, "every rediff request goes out under the default persona UA")
        #expect(recorded.allSatisfy { $0.cacheBuster != nil }, "and every request is cache-busted")
    }

    @Test("the production sweep (service → seams) fetches every rediff request under the default persona")
    func serviceSweepFetchesUnderDefaultPersona() async throws {
        RediffMockURLProtocol.reset(total: 200_000)
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [RediffMockURLProtocol.self]
        let session = URLSession(configuration: config)

        // Rotating, eligible candidate: the local sample differs from the
        // remote one so the sweep proceeds past the pre-check into the full
        // B-side fetch — exercising BOTH seams via the service.
        let local = StubLocalSampler()
        local.defaultFingerprint = RediffRefetchPolicy.sampleFingerprint(
            head: Data("played".utf8), tail: Data("played".utf8), totalLength: 1
        )
        let fingerprinter = StubBSideFingerprinter()
        fingerprinter.fingerprintsToReturn = [1, 2, 3]
        let enumerator = StubRefetchEnumerator()
        enumerator.candidatesToReturn = [RediffRefetchCandidate(
            assetId: "asset-persona",
            enclosureURL: URL(string: "https://cdn.example.com/persona.mp3")!,
            downloadedAt: 0,
            localAudioURL: URL(fileURLWithPath: "/tmp/persona-played.mp3"),
            attemptState: .initial
        )]
        let service = RediffRefetchService(
            enabled: true,
            enumerator: enumerator,
            rangedSampler: URLSessionRangedAudioSampler(session: session, persona: .default),
            localSampler: local,
            fullFetcher: URLSessionFullEpisodeFetcher(session: session, persona: .default),
            bsideFingerprinter: fingerprinter,
            recorder: SpyRefetchRecorder(),
            fileRemover: FileManagerTempFileRemover(),  // deletes the real B-copy
            taskScheduler: StubTaskScheduler(),
            now: { 100 * RediffRefetchTests.day }
        )
        await service.runRefetchSweep()

        let recorded = RediffMockURLProtocol.snapshot()
        #expect(recorded.count == 3, "pre-check head + tail, then the full B-side fetch")
        let ua = RediffFetchPersona.appleCoreMediaIPhone.userAgent
        #expect(recorded.allSatisfy { $0.userAgent == ua }, "sweep → seams applies the default persona to every request")
        #expect(recorded[0].cacheBuster == recorded[1].cacheBuster, "head + tail share one per-sample token")
        #expect(recorded[2].cacheBuster != recorded[0].cacheBuster, "the full fetch is a fresh, distinct request")
        #expect(recorded.allSatisfy { $0.cacheBuster != nil })
    }

    @Test("the cache-buster is unique per fetch call and shared within one sample's head+tail")
    func cacheBusterUniquePerCall() async throws {
        RediffMockURLProtocol.reset(total: 200_000)
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [RediffMockURLProtocol.self]
        let session = URLSession(configuration: config)

        let counter = TokenCounter()
        let sampler = URLSessionRangedAudioSampler(session: session, cacheBuster: { counter.next() })
        for _ in 0..<2 {
            _ = try await sampler.sample(
                url: URL(string: "https://cdn.example.com/ep.mp3")!,
                headBytes: 64 * 1024, tailBytes: 64 * 1024
            )
        }

        let recorded = RediffMockURLProtocol.snapshot()
        #expect(recorded.count == 4)
        #expect(recorded[0].cacheBuster == "cb-0")
        #expect(recorded[1].cacheBuster == "cb-0", "head + tail of sample #1 share one token")
        #expect(recorded[2].cacheBuster == "cb-1", "sample #2 gets a fresh, distinct token")
        #expect(recorded[3].cacheBuster == "cb-1")
    }
}

// MARK: - Mock URLProtocol (proves no HEAD / no conditional headers)

final class RediffMockURLProtocol: URLProtocol, @unchecked Sendable {
    struct Recorded: Sendable {
        let method: String
        let range: String?
        let ifNoneMatch: String?
        let ifModifiedSince: String?
        /// playhead-xsdz.45 / xsdz.36.3: the request context + cache-buster as
        /// they actually went out on the wire, so persona-header application
        /// and cache-busting are asserted end-to-end (not just at build time).
        let userAgent: String?
        let url: URL?
        /// The `_cb` cache-buster query-item value on `url`, if present.
        var cacheBuster: String? {
            guard let url,
                  let items = URLComponents(url: url, resolvingAgainstBaseURL: false)?.queryItems
            else { return nil }
            return items.first(where: { $0.name == "_cb" })?.value
        }
    }

    nonisolated(unsafe) private static var recorded: [Recorded] = []
    nonisolated(unsafe) private static var totalLength: Int64 = 0
    nonisolated(unsafe) private static let lock = NSLock()

    static func reset(total: Int64) {
        lock.lock(); defer { lock.unlock() }
        recorded = []
        totalLength = total
    }

    static func snapshot() -> [Recorded] {
        lock.lock(); defer { lock.unlock() }
        return recorded
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let range = request.value(forHTTPHeaderField: "Range")
        let total: Int64
        Self.lock.lock()
        Self.recorded.append(Recorded(
            method: request.httpMethod ?? "?",
            range: range,
            ifNoneMatch: request.value(forHTTPHeaderField: "If-None-Match"),
            ifModifiedSince: request.value(forHTTPHeaderField: "If-Modified-Since"),
            userAgent: request.value(forHTTPHeaderField: "User-Agent"),
            url: request.url
        ))
        total = Self.totalLength
        Self.lock.unlock()

        // Parse "bytes=START-END".
        var start: Int64 = 0
        var end: Int64 = total - 1
        if let range, let eq = range.firstIndex(of: "="), let dash = range.firstIndex(of: "-") {
            let startStr = range[range.index(after: eq)..<dash]
            let endStr = range[range.index(after: dash)...]
            start = Int64(startStr) ?? 0
            end = Int64(endStr) ?? (total - 1)
        }
        let length = Int(max(0, end - start + 1))
        var body = Data(count: length)
        for i in 0..<length { body[i] = UInt8((Int(start) + i) & 0xFF) }

        // A full GET (no Range) → 200 full body; a ranged GET → 206 + Content-Range.
        let status = range == nil ? 200 : 206
        var headers = [
            "Content-Length": "\(length)",
            "Accept-Ranges": "bytes",
        ]
        if range != nil { headers["Content-Range"] = "bytes \(start)-\(end)/\(total)" }
        let response = HTTPURLResponse(
            url: request.url!, statusCode: status, httpVersion: "HTTP/1.1", headerFields: headers
        )!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: body)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}
}
