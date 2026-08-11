// BackgroundSessionIOTests.swift
// playhead-nsjn: rails for the off-pool, bounded background-session
// executor and for the two recovery branches it makes reachable.
//
// The defect these cover is not "a download failed". It is that
// `URLSession.downloadTask(with:)` on a BACKGROUND session blocks the
// calling thread in a synchronous XPC round-trip to `nsurlsessiond`, and
// `DownloadManager` is an actor — so the block lands on the fixed-width
// Swift Concurrency cooperative pool. A sampled full-plan run had all 10
// of 10 cooperative threads parked in that one call chain; the process had
// no runnable concurrency left, 10,022 Swift Testing tests had started and
// none finished, and no `.timeLimit` fired because a time limit's own timer
// also needs a cooperative thread.
//
// So the property under test is structural, not behavioural: WHERE the
// blocking call runs, and whether the caller can stop waiting for it.
// Deliberately NOT tested by wall-clock throughput assertions — a
// latency rail here would join the load-flake families this bead exists to
// stop producing.

import Foundation
import Testing
import os
@testable import Playhead

// MARK: - The executor

@Suite("BackgroundSessionIO (playhead-nsjn)")
struct BackgroundSessionIOTests {

    /// Every test gets its own queue. The instance-per-queue design is what
    /// lets a rail below block a queue for a full second without stalling
    /// `.shared` — or a sibling test — in the same process.
    private static func makeIO(
        timeout: TimeInterval,
        line: Int = #line
    ) -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: .dedicatedThread,
            timeout: timeout,
            queueLabel: "nsjn.test.\(line).\(UUID().uuidString)"
        )
    }

    @Test("A call that answers inside its bound returns the body's result")
    func healthyCallReturnsItsResult() async {
        let io = Self.makeIO(timeout: 5)
        let result = await io.perform(label: "healthy") { 41 + 1 }
        #expect(result == 42)
    }

    /// THE containment rail. An implementation that simply called the body
    /// inline — which is what the shipped code did — would report `nil`
    /// here, because `currentQueueLabel` is nil on a cooperative thread.
    /// Nothing about the returned VALUE distinguishes the two, which is
    /// exactly why the defect survived a suite this large.
    @Test("The blocking body runs on the dedicated queue, not on the caller's thread")
    func bodyRunsOffTheCallersThread() async {
        let io = Self.makeIO(timeout: 5)

        // The caller is on a Swift Concurrency cooperative thread, where no
        // dedicated-queue label is stamped. Without this the assertion
        // below could pass on an implementation that stamps the key
        // process-wide.
        #expect(
            BackgroundSessionIO.currentQueueLabel == nil,
            "the test's own context must not already look like the IO queue"
        )

        let observed = await io.perform(label: "probe") {
            BackgroundSessionIO.currentQueueLabel
        }
        #expect(observed == io.queueLabel)
    }

    /// The caller must stop waiting. A body parked in `mach_msg2_trap` is
    /// beyond the reach of `Task` cancellation, so the bound is the only
    /// mechanism that can end the wait.
    @Test("A body that outlives its bound resumes the caller with nil")
    func aBodyThatOutlivesItsBoundReleasesTheCaller() async {
        let io = Self.makeIO(timeout: 0.2)
        let result = await io.perform(label: "stalled") {
            // Models the daemon not answering. Chosen 5x longer than the
            // bound so the verdict is about the bound firing, not about how
            // loaded the box is; and short enough that a REGRESSION fails
            // this rail in a second rather than hanging the suite.
            Thread.sleep(forTimeInterval: 1.0)
            return 7
        }
        #expect(result == nil)
    }

    /// A late answer must not be silently dropped: for a URLSession task,
    /// dropping it leaves a live transfer nobody in this process is
    /// tracking. An implementation that raced the body against a sleep and
    /// simply abandoned the loser passes every other rail here and fails
    /// this one.
    @Test("A result produced after the bound is handed to the discard hook")
    func aLateResultIsDiscardedRatherThanDropped() async {
        let io = Self.makeIO(timeout: 0.2)
        let discarded = OSAllocatedUnfairLock<[Int]>(initialState: [])

        let result = await io.perform(
            label: "late",
            discardingLateResult: { value in discarded.withLock { $0.append(value) } },
            running: {
                Thread.sleep(forTimeInterval: 0.6)
                return 7
            }
        )
        #expect(result == nil)

        // The body is still running at this point; poll rather than sleep a
        // fixed interval so the rail is not a latency measurement.
        var remaining = 60
        while discarded.withLock({ $0.isEmpty }), remaining > 0 {
            try? await Task.sleep(for: .milliseconds(100))
            remaining -= 1
        }
        #expect(discarded.withLock { $0 } == [7])
    }

    /// Work that reaches the front of the queue after its caller gave up
    /// must NOT run. Starting a transfer whose caller has already reported
    /// failure is how an untracked background download is born — the exact
    /// residue class that poisons the simulator between runs.
    @Test("A submission whose caller already gave up never starts its body")
    func anAbandonedSubmissionNeverStartsItsBody() async {
        let io = Self.makeIO(timeout: 0.2)
        let bodyRan = OSAllocatedUnfairLock<Bool>(initialState: false)

        // Occupy the serial queue so the second submission cannot start.
        async let blocker: Int? = io.perform(label: "blocker") {
            Thread.sleep(forTimeInterval: 1.0)
            return 1
        }
        // Let the blocker actually take the queue before queueing behind it.
        try? await Task.sleep(for: .milliseconds(100))

        let starved = await io.perform(label: "starved") {
            bodyRan.withLock { $0 = true }
            return 2
        }
        #expect(starved == nil)

        _ = await blocker
        // Give the drained queue a chance to (wrongly) run the abandoned
        // block before asserting that it did not.
        var remaining = 20
        while remaining > 0, !bodyRan.withLock({ $0 }) {
            try? await Task.sleep(for: .milliseconds(50))
            remaining -= 1
        }
        #expect(
            bodyRan.withLock { $0 } == false,
            "a submission whose caller already reported failure must not start work"
        )
    }

    #if DEBUG
    @Test("The neverAnswers seam reports unavailability without running the body")
    func neverAnswersDoesNotRunTheBody() async {
        let io = BackgroundSessionIO(
            behavior: .neverAnswers,
            timeout: 0.1,
            queueLabel: "nsjn.test.never.\(UUID().uuidString)"
        )
        let bodyRan = OSAllocatedUnfairLock<Bool>(initialState: false)
        let result = await io.perform(label: "never") {
            bodyRan.withLock { $0 = true }
            return 1
        }
        #expect(result == nil)
        #expect(bodyRan.withLock { $0 } == false)
    }
    #endif
}

// MARK: - The recovery branches the bound makes reachable

@Suite("DownloadManager – unanswering transfer daemon (playhead-nsjn)")
struct DownloadManagerDaemonUnavailableTests {

    private static func stalledIO() -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: .neverAnswers,
            timeout: 0.1,
            queueLabel: "nsjn.test.manager.\(UUID().uuidString)"
        )
    }

    /// Before this bead the only outcomes were "started" and "hung". The
    /// reservation matters because `backgroundDownload` now suspends
    /// between its in-flight guard and the handoff: a manager that left the
    /// episode reserved would refuse every later attempt for the life of
    /// the process, turning a transient daemon stall into a permanent
    /// per-episode outage.
    @Test("A background download the daemon never answers leaves the episode retryable")
    func unstartedBackgroundDownloadReleasesItsReservation() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir, sessionIO: Self.stalledIO())
        try await manager.bootstrap()

        let episodeId = "nsjn-unanswered"
        await manager.backgroundDownload(
            episodeId: episodeId,
            from: URL(string: "https://cdn.example.com/\(episodeId).mp3")!,
            context: DownloadContext(
                podcastId: "show-1",
                isExplicitDownload: false,
                podcastTitle: "Show",
                episodeTitle: "Episode"
            )
        )

        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId) == false,
            "a transfer that was never started must not hold the in-flight slot"
        )
        // Nothing was admitted to the OS, so the admission counter must not
        // have moved — otherwise the rail above could pass on an
        // implementation that started a transfer and then forgot it.
        #expect(await manager._backgroundDownloadAdmissionCountForTesting() == 0)
        // The attribution sidecar describes a transfer; with no transfer it
        // must not outlive the attempt.
        #expect(await manager.loadDownloadAttribution(episodeId: episodeId) == nil)
    }

    /// The blob is the ONLY copy of the bytes already fetched. `.corrupted`
    /// deletes it; `.daemonUnavailable` must not, or a recoverable daemon
    /// stall costs the user a full re-download of an episode they had
    /// nearly finished fetching.
    @Test("A resume the daemon never answers reports .daemonUnavailable and KEEPS the blob")
    func unansweredResumeRetainsItsBlob() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir, sessionIO: Self.stalledIO())
        try await manager.bootstrap()

        let episodeId = "nsjn-resume-unanswered"
        let source = URL(string: "https://cdn.example.com/\(episodeId).mp3")!
        try await manager.persistResumeData(
            episodeId: episodeId,
            data: Data([0xAB, 0xCD]),
            sourceURL: source,
            validator: HTTPAssetMetadata(
                etag: "\"A\"", contentLength: 100, lastModified: nil
            )
        )
        // Freshness gate passes, so the run reaches the handoff rather than
        // diverting to the re-download branch — without this the rail would
        // be testing `.redownloadedFresh` instead.
        await manager.setResumeValidatorProviderForTesting { _ in
            HTTPAssetMetadata(etag: "\"A\"", contentLength: 100, lastModified: nil)
        }

        let outcome = try await manager.resumeSuspendedTransfer(episodeId: episodeId)
        #expect(outcome == .daemonUnavailable)
        #expect(
            try await manager.loadResumeData(episodeId: episodeId) != nil,
            "the resume blob must survive a daemon stall — it is the only copy of the fetched bytes"
        )
    }

    /// A task the daemon created but never started is SUSPENDED: no
    /// delegate callback will ever fire for it, so nothing on the normal
    /// path releases the episode's in-flight slot. Without this release the
    /// bounded-wait fix would trade an indefinite hang for an indefinite
    /// per-episode outage — every later attempt refused by the in-flight
    /// guard for the life of the process.
    ///
    /// Uses a task from `URLSession.shared` on purpose: it is a plain task
    /// with no `nsurlsessiond` registration, so the rail exercises the slot
    /// bookkeeping without leaving background-transfer residue in the
    /// simulator (the residue class that poisons the next run).
    @Test("Abandoning an unstarted transfer releases the episode's in-flight slot")
    func abandoningAnUnstartedTransferReleasesTheSlot() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "nsjn-unstarted"
        let session = URLSession.shared
        let orphan = session.downloadTask(
            with: URL(string: "https://cdn.example.com/\(episodeId).mp3")!
        )
        // playhead-7l6n: register the identity that will actually be
        // abandoned. Registering some OTHER identity and then abandoning
        // this one would leave a live claimant behind, and asserting the
        // slot is released in THAT state pins the over-release as correct.
        _ = await manager._registerBackgroundTransferForTesting(
            episodeId: episodeId,
            identity: BackgroundTransferIdentity(
                sessionIdentifier: session.configuration.identifier ?? "",
                taskIdentifier: orphan.taskIdentifier
            )
        )
        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId),
            "the rail is vacuous unless the slot was actually taken first"
        )

        await manager.abandonUnstartedTransfer(
            task: orphan,
            session: session,
            episodeId: episodeId
        )

        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId) == false
        )
    }

    /// playhead-7l6n: the counterpart to the rail above — the slot must be
    /// released when this transfer was the last claimant, and NOT released
    /// when it was not.
    ///
    /// The second identity models `handleBackgroundDownloadComplete`, which
    /// admits a task reattached from a prior process with no in-flight guard
    /// at all and can do so while `backgroundDownload` is suspended inside a
    /// `sessionIO` call. `bgInFlightEpisodes` is also the eviction-protection
    /// set, so an over-release there exposes the reattached transfer's
    /// artifact to eviction while its completion is still running.
    @Test("Abandoning one transfer does not release a slot another live transfer still holds")
    func abandonKeepsTheSlotAnotherLiveTransferStillHolds() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "nsjn-two-claimants"
        let session = URLSession.shared
        let orphan = session.downloadTask(
            with: URL(string: "https://cdn.example.com/\(episodeId).mp3")!
        )
        _ = await manager._registerBackgroundTransferForTesting(
            episodeId: episodeId,
            identity: BackgroundTransferIdentity(
                sessionIdentifier: session.configuration.identifier ?? "",
                taskIdentifier: orphan.taskIdentifier
            )
        )
        let reattached = await manager._registerBackgroundTransferForTesting(
            episodeId: episodeId
        )

        await manager.abandonUnstartedTransfer(
            task: orphan,
            session: session,
            episodeId: episodeId
        )
        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId),
            "the reattached transfer still names this episode — abandoning a DIFFERENT identity must not strip its eviction protection"
        )

        await manager._finishBackgroundTransferForTesting(
            identity: reattached,
            episodeId: episodeId
        )
        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId) == false,
            "and once the last claimant completes, the slot is released — the retain must be a deferral, not a leak"
        )
    }

    /// Releasing the slot is only half of it. Both call sites REGISTER the
    /// task before resuming it, so an abandoned transfer leaves an admitted
    /// identity behind — and `finishBackgroundTransfer` releases the episode
    /// only when no admitted identity still names it. So a manager that
    /// released the slot but kept the entry would let the retry START and
    /// then fail to release the episode when that retry COMPLETED,
    /// reinstating the permanent per-episode outage one attempt later, with
    /// no daemon stall left in sight to explain it.
    ///
    /// The identity is built the way production builds it — from the task
    /// and the session — so an implementation that drains some other
    /// identity does not pass.
    @Test("An abandoned transfer does not wedge the episode's NEXT attempt")
    func abandonedTransferDoesNotWedgeTheNextAttempt() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "nsjn-abandon-then-retry"
        let session = URLSession.shared
        let orphan = session.downloadTask(
            with: URL(string: "https://cdn.example.com/\(episodeId).mp3")!
        )
        _ = await manager._registerBackgroundTransferForTesting(
            episodeId: episodeId,
            identity: BackgroundTransferIdentity(
                sessionIdentifier: session.configuration.identifier ?? "",
                taskIdentifier: orphan.taskIdentifier
            )
        )
        await manager.abandonUnstartedTransfer(
            task: orphan,
            session: session,
            episodeId: episodeId
        )
        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId) == false,
            "the rail is vacuous unless the abandon released the slot at all"
        )

        // The retry the release exists to invite.
        let retryIdentity = await manager._registerBackgroundTransferForTesting(
            episodeId: episodeId
        )
        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId),
            "the retry must be admitted"
        )
        await manager._finishBackgroundTransferForTesting(
            identity: retryIdentity,
            episodeId: episodeId
        )
        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId) == false,
            "the abandoned identity must not outlive its transfer — the retry's own completion has to be able to release the episode"
        )
    }

    /// The gap this closes: every rail above drives `abandonUnstartedTransfer`
    /// by hand. Nothing proved `backgroundDownload` REACHES it, because the
    /// `neverAnswers` seam refuses the creation call and the resume-timeout
    /// branch lives behind a creation that succeeded. So the one path on
    /// which a transfer really is created and really is never started was
    /// read, not run — and that is where the identity-map leak this round
    /// fixed was living.
    @Test("A transfer backgroundDownload created but could not resume is abandoned by backgroundDownload itself")
    func backgroundDownloadAbandonsATransferItCouldNotResume() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(
            cacheDirectory: dir,
            sessionIO: BackgroundSessionIO(
                behavior: .refusesCallsLabelled("resume() for"),
                timeout: 0.1,
                queueLabel: "nsjn.test.resume-refused.\(UUID().uuidString)"
            )
        )
        try await manager.bootstrap()

        let episodeId = "nsjn-created-not-resumed"
        await manager.backgroundDownload(
            episodeId: episodeId,
            from: URL(string: "https://cdn.example.com/\(episodeId).mp3")!,
            context: DownloadContext(
                podcastId: "show-2",
                isExplicitDownload: false,
                podcastTitle: "Show",
                episodeTitle: "Episode"
            )
        )

        // Distinguishes this rail from the creation-timeout one above: the
        // task really WAS admitted, so the abandon path — not the
        // never-started path — is what ran.
        #expect(await manager._backgroundDownloadAdmissionCountForTesting() == 1)
        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId) == false,
            "a transfer that was created but never resumed must not hold the slot"
        )
        #expect(await manager.loadDownloadAttribution(episodeId: episodeId) == nil)

        // And the identity it registered before resuming must be gone, or
        // the retry this release invites cannot release the episode when it
        // finishes.
        let retryIdentity = await manager._registerBackgroundTransferForTesting(
            episodeId: episodeId
        )
        await manager._finishBackgroundTransferForTesting(
            identity: retryIdentity,
            episodeId: episodeId
        )
        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId) == false,
            "the abandoned registration must not survive to wedge the next attempt"
        )
    }

    /// The freshness gate's re-download branch runs THROUGH
    /// `backgroundDownload`, so it inherits the same stall. The blob is
    /// gone by design on that branch (the bytes were provably stale), and
    /// the outcome stays `.redownloadedFresh` because that branch's
    /// contract is "the stale blob was discarded", which did happen.
    @Test("A stale-validator resume still reports .redownloadedFresh when the daemon stalls")
    func staleResumeStillReportsFreshRedownload() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir, sessionIO: Self.stalledIO())
        try await manager.bootstrap()

        let episodeId = "nsjn-resume-rotated"
        try await manager.persistResumeData(
            episodeId: episodeId,
            data: Data([0x01, 0x02]),
            sourceURL: URL(string: "https://dai.example.com/\(episodeId).mp3")!,
            validator: HTTPAssetMetadata(
                etag: "\"A\"", contentLength: 100, lastModified: nil
            )
        )
        await manager.setResumeValidatorProviderForTesting { _ in
            HTTPAssetMetadata(etag: "\"B\"", contentLength: 80, lastModified: nil)
        }

        let outcome = try await manager.resumeSuspendedTransfer(episodeId: episodeId)
        #expect(outcome == .redownloadedFresh)
        #expect(try await manager.loadResumeData(episodeId: episodeId) == nil)
        // And the failed fresh start left nothing reserved.
        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId) == false
        )
    }
}
