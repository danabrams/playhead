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

// MARK: - Seam stubs

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
    }
}

// MARK: - Mock URLProtocol (proves no HEAD / no conditional headers)

final class RediffMockURLProtocol: URLProtocol, @unchecked Sendable {
    struct Recorded: Sendable {
        let method: String
        let range: String?
        let ifNoneMatch: String?
        let ifModifiedSince: String?
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
            ifModifiedSince: request.value(forHTTPHeaderField: "If-Modified-Since")
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
