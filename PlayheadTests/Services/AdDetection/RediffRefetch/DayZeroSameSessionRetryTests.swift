// DayZeroSameSessionRetryTests.swift
// playhead-3oyz: ONE -1001 fetch timeout at download time used to darken the
// deterministic pre-roll lane for the whole first-day listen.
//
// THE WITNESS (device pull 2026-08-06, asset F4CE7F47, re-derived for this
// bead): download 01:01, kickoff fired 01:02:28 after waiting 120.4 s, the
// k-way fetch died with NSURLErrorDomain -1001 having landed ZERO B-copies
// (lastFullFetchBytes = 0) — and `DayZeroRediffAttemptPolicy.baseBackoff`
// treats that exactly like a full failed diff, so both re-drive requests
// during the 09:26–09:39 listen were suppressed (suppressedCount = 2,
// lastSuppressedAt mid-listen). The lane's only conf-1.0 detector for OUTER
// edges went dark on a timeout that cost nothing.
//
// THE FIX (Dan's shape (i), 2026-08-08): a SAME-SESSION retry, granted only
// when the exit is `.fetchFailed` AND the MEASURED landed bytes are zero —
// never from the error code alone (the standing defect class: an exit that
// names "failure" read as if it named "bandwidth spent"). Bounded to ONE
// retry after a 30 s delay; a failure that landed real bytes keeps the p70f
// 24 h backoff untouched, so the ~108 MB replay bleed cannot reopen.

import Foundation
import Testing

@testable import Playhead

// MARK: - Scripted fetcher

/// A fetcher whose behavior is scripted PER INVOCATION — unlike
/// `KWaySpyFullFetcher.throwOnCallIndex`, which indexes over SUCCESSFUL calls
/// only (a throw does not append) and therefore cannot express "the first run
/// times out before anything lands, the second run succeeds".
final class ScriptedKWayFetcher: FullEpisodeFetching, @unchecked Sendable {
    enum Step { case land, throwTimeout }
    private let lock = NSLock()
    private let script: [Step]
    private var invocationCount = 0
    private var landed: [(url: URL, persona: RediffFetchPersona?)] = []
    var byteCountPerFetch = 54_000_000
    /// Steps beyond the script's end LAND — "the network recovered".
    init(script: [Step]) { self.script = script }

    var invocations: Int {
        lock.lock(); defer { lock.unlock() }
        return invocationCount
    }
    var landedCalls: [(url: URL, persona: RediffFetchPersona?)] {
        lock.lock(); defer { lock.unlock() }
        return landed
    }

    func download(url: URL) async throws -> (fileURL: URL, byteCount: Int) {
        try await download(url: url, persona: nil)
    }

    func download(url: URL, persona: RediffFetchPersona?) async throws -> (fileURL: URL, byteCount: Int) {
        lock.lock(); defer { lock.unlock() }
        let index = invocationCount
        invocationCount += 1
        let step = index < script.count ? script[index] : .land
        switch step {
        case .throwTimeout:
            // The witness error, shape-faithful: -1001 with zero landed bytes.
            throw NSError(
                domain: NSURLErrorDomain, code: NSURLErrorTimedOut,
                userInfo: [NSLocalizedDescriptionKey: "The request timed out."]
            )
        case .land:
            landed.append((url, persona))
            return (URL(fileURLWithPath: "/tmp/retry-bcopy-\(index).mp3"), byteCountPerFetch)
        }
    }
}

/// Ordered event log shared across the trigger's seams, so ORDER — the claim
/// must precede the sleep (playhead-fil5) — is assertable, not assumed.
final class RetryEventLog: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [String] = []
    func append(_ event: String) {
        lock.lock(); defer { lock.unlock() }
        storage.append(event)
    }
    var events: [String] {
        lock.lock(); defer { lock.unlock() }
        return storage
    }
}

/// Transport snapshots served in sequence — the retry re-reads the gate
/// across its delay, and this is how a test flips Low Data Mode "while the
/// trigger sleeps". The last snapshot repeats once the sequence is drained.
final class SequencedTransportProvider: @unchecked Sendable {
    private let lock = NSLock()
    private var remaining: [DayZeroTransportSnapshot]
    init(_ snapshots: [DayZeroTransportSnapshot]) { self.remaining = snapshots }
    func next() -> DayZeroTransportSnapshot {
        lock.lock(); defer { lock.unlock() }
        return remaining.count > 1 ? remaining.removeFirst() : remaining[0]
    }
}

// MARK: - Pure policy

@Suite("Day-0 same-session retry policy (playhead-3oyz)")
struct DayZeroSameSessionRetryPolicyTests {

    @Test("THE WITNESS GRANT: fetch_failed with ZERO measured landed bytes earns the retry")
    func zeroByteFetchFailureIsRetryable() {
        #expect(DayZeroRediffAttemptPolicy.grantsSameSessionRetry(
            exit: .fetchFailed, measuredFullFetchBytes: 0, retriesUsed: 0
        ))
    }

    @Test("A FAILURE THAT LANDED A B-COPY IS NOT RETRIED — the p70f replay bleed stays closed")
    func byteSpendingFailureIsNotRetryable() {
        // One landed ~54 MB copy is the SMALLEST non-zero value the measured
        // quantity can take (it sums completed copies), and it must decline.
        #expect(!DayZeroRediffAttemptPolicy.grantsSameSessionRetry(
            exit: .fetchFailed, measuredFullFetchBytes: 54_000_000, retriesUsed: 0
        ))
        // And the gate is the MEASURED bytes, not a magnitude judgment: even
        // one byte proves the fetch was not free.
        #expect(!DayZeroRediffAttemptPolicy.grantsSameSessionRetry(
            exit: .fetchFailed, measuredFullFetchBytes: 1, retriesUsed: 0
        ))
    }

    @Test("the grant is scoped to .fetchFailed — every other zero-cost exit declines")
    func otherExitsAreNotRetryable() {
        // A zero-byte value alone must never grant: the exits below all read
        // 0 measured bytes, and none of them is a transient network failure a
        // 30 s delay could outlive.
        for exit in RediffDayZeroExit.allCases where exit != .fetchFailed {
            #expect(!DayZeroRediffAttemptPolicy.grantsSameSessionRetry(
                exit: exit, measuredFullFetchBytes: 0, retriesUsed: 0
            ), "exit \(exit.rawValue) must not earn a same-session retry")
        }
        #expect(!DayZeroRediffAttemptPolicy.grantsSameSessionRetry(
            exit: nil, measuredFullFetchBytes: 0, retriesUsed: 0
        ), "a run with no day-0 exit (lagged vocabulary) must not retry")
    }

    @Test("the bound: one retry per trigger invocation, then stop")
    func retryBudgetIsOne() {
        #expect(!DayZeroRediffAttemptPolicy.grantsSameSessionRetry(
            exit: .fetchFailed, measuredFullFetchBytes: 0,
            retriesUsed: DayZeroRediffAttemptPolicy.maxSameSessionRetries
        ))
        // The bound itself is contract, not tuning: retries are recorded as
        // ATTEMPTS, so first attempt + 1 retry leaves exactly one budgeted
        // attempt (maxAttempts = 3) for a later ≥24 h session. Raising this
        // silently converts one bad-network session into a generation-dead
        // asset.
        #expect(DayZeroRediffAttemptPolicy.maxSameSessionRetries == 1)
        #expect(DayZeroRediffAttemptPolicy.sameSessionRetryDelaySeconds == 30)
    }

    @Test("advance carries the retry-claim history — an attempt never resets the fil5 evidence")
    func advanceCarriesClaimHistory() {
        let prior = RediffDayZeroAttemptRecord(
            analysisAssetId: "a", attemptCount: 1, lastAttemptAt: 1_000,
            lastExit: .fetchFailed, retryClaimCount: 3, lastRetryClaimAt: 1_030
        )
        let advanced = DayZeroRediffAttemptPolicy.advance(
            record: prior, assetId: "a",
            outcome: RediffDayZeroMintOutcome(markCount: 1, exit: .marked),
            fullFetchBytes: 108_000_000, at: 2_000
        )
        #expect(advanced.retryClaimCount == 3)
        #expect(advanced.lastRetryClaimAt == 1_030)
    }
}

// MARK: - Trigger behavior

@Suite("Day-0 same-session retry trigger (playhead-3oyz)")
struct DayZeroSameSessionRetryTriggerTests {

    static let enclosure = URL(string: "https://cdn.example.com/current.mp3")!
    static let played = URL(fileURLWithPath: "/tmp/played-a.mp3")
    /// The witness attempt instant (2026-08-06 01:02:28 UTC).
    static let witnessNow: Double = 1_785_978_148

    private struct Harness {
        let trigger: DayZeroRediffTrigger
        let fetcher: ScriptedKWayFetcher
        let minter: SpyDayZeroMinter
        let log: RetryEventLog
        let suppressions: SuppressionSpy
        let spends: BudgetSpendSpy
    }

    private func makeHarness(
        script: [ScriptedKWayFetcher.Step],
        markCountToReturn: Int = 1,
        prefetchBlocker: RediffDayZeroExit? = nil,
        transports: [DayZeroTransportSnapshot] = [.testWifi],
        budgetWindows: [RediffDayZeroBudgetWindow] = [.empty],
        cancelDuringDelay: Bool = false
    ) -> Harness {
        let fetcher = ScriptedKWayFetcher(script: script)
        let minter = SpyDayZeroMinter()
        minter.markCountToReturn = markCountToReturn
        minter.prefetchBlockerToReturn = prefetchBlocker
        let log = RetryEventLog()
        let suppressions = SuppressionSpy()
        let spends = BudgetSpendSpy()
        let transportSequence = SequencedTransportProvider(transports)
        let windowSequence = SequencedWindowProvider(budgetWindows)
        let service = RediffRefetchService(
            enabled: true,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: fetcher,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: SpyRefetchRecorder(),
            fileRemover: SpyTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            dayZeroMinter: minter,
            now: { Self.witnessNow }
        )
        let trigger = DayZeroRediffTrigger(
            service: service,
            enabled: true,
            kWayFetchCount: 2,
            transportProvider: { transportSequence.next() },
            chargeStateProvider: { true },
            deepScanOptInProvider: { false },
            attemptContextProvider: { _ in .never },
            suppressionRecorder: { assetId, exit, at in
                suppressions.record(assetId, exit, at)
            },
            mintedMarkDelivery: { _ in log.append("deliver") },
            budgetWindowProvider: { windowSequence.next() },
            budgetSpendRecorder: { bytes, at in
                spends.record(bytes, at)
                log.append("spend:\(bytes)")
            },
            retryClaimRecorder: { _ in log.append("claim") },
            // INSTANT sleeper (the cooperative-time-bound rule): records the
            // requested duration, never parks the suite on 30 s of wall clock.
            retryDelay: { seconds in
                log.append("sleep:\(Int(seconds))")
                if cancelDuringDelay {
                    // Model app teardown mid-delay: cancel the surrounding
                    // task, exactly what a real `Task.sleep` would surface.
                    withUnsafeCurrentTask { $0?.cancel() }
                }
            }
        )
        return Harness(
            trigger: trigger, fetcher: fetcher, minter: minter,
            log: log, suppressions: suppressions, spends: spends
        )
    }

    private func fire(_ harness: Harness, at now: Double = witnessNow) async -> RediffRefetchService.SweepSummary {
        // A child task, so the cancellation variants poison the TRIGGER's
        // task and never the test's own.
        await Task { [trigger = harness.trigger] in
            await trigger.triggerIfEligible(
                analysisAssetId: "asset-F4CE7F47",
                enclosureURL: Self.enclosure,
                playedFileURL: Self.played,
                at: now
            )
        }.value
    }

    @Test("THE WITNESS, FIXED: a -1001 that landed nothing is retried once and the pre-roll lane lights up")
    func witnessTimeoutRetriesAndMarks() async {
        // Run 1: the first B-copy fetch times out before anything lands.
        // Run 2 (the retry): both copies land, the mint marks.
        let harness = makeHarness(script: [.throwTimeout])

        let summary = await fire(harness)

        // Pre-fix, the fetcher was invoked ONCE (the throw) and the session
        // ended markless with the asset under a 24 h backoff — every line
        // below fails on that behavior.
        #expect(harness.fetcher.invocations == 3, "1 timed-out invocation + 2 landed on the retry")
        #expect(summary.dayZeroMarkCount == 1, "the retry minted the pre-roll mark in-session")
        #expect(summary.dayZeroExit == .marked)
        #expect(harness.log.events.contains("deliver"), "the retry's marks reach the live session (96ot)")
        // The fil5 ordering: the durable claim precedes the delay, so a
        // process killed mid-sleep has already left its evidence.
        #expect(harness.log.events == ["claim", "sleep:30", "spend:108000000", "deliver"],
                "claim → 30 s delay → the retry spends and delivers: \(harness.log.events)")
        #expect(harness.spends.spends.map(\.bytes) == [108_000_000],
                "only the retry landed bytes, and they are charged to the rolling window")
        #expect(harness.suppressions.recorded.isEmpty, "nothing about this flow is a suppression")
    }

    @Test("A MID-BATCH FAILURE THAT LANDED A COPY IS NOT RETRIED — measured bytes, not the error code, decide")
    func byteSpendingFailureDoesNotRetry() async {
        // Run 1: copy 1 lands (~54 MB billed), copy 2 times out. Same error
        // code as the witness — different measured bytes, different verdict.
        let harness = makeHarness(script: [.land, .throwTimeout])

        let summary = await fire(harness)

        #expect(harness.fetcher.invocations == 2, "no second run: the failure spent real bytes")
        #expect(summary.dayZeroExit == .fetchFailed)
        #expect(summary.fullFetchBytes == 54_000_000)
        #expect(!harness.log.events.contains("claim"), "no retry claim for a byte-spending failure")
        #expect(!harness.log.events.contains { $0.hasPrefix("sleep") })
        #expect(harness.spends.spends.map(\.bytes) == [54_000_000],
                "the landed copy is still charged — accounting is untouched")
    }

    @Test("THE BOUND: a retry that times out again is NOT retried a second time")
    func retryIsBoundedToOne() async {
        let harness = makeHarness(script: [.throwTimeout, .throwTimeout])

        let summary = await fire(harness)

        #expect(harness.fetcher.invocations == 2, "first run + exactly one retry, both timed out")
        #expect(summary.dayZeroExit == .fetchFailed)
        #expect(harness.log.events == ["claim", "sleep:30"],
                "one claim, one delay, no second loop: \(harness.log.events)")
    }

    @Test("a zero-cost PREFETCH BLOCK does not retry — zero bytes alone is not the grant")
    func prefetchBlockDoesNotRetry() async {
        let harness = makeHarness(script: [], prefetchBlocker: .aSideNotAnchored)

        let summary = await fire(harness)

        #expect(harness.fetcher.invocations == 0)
        #expect(summary.dayZeroExit == .aSideNotAnchored)
        #expect(harness.log.events.isEmpty, "no claim, no sleep, no spend")
    }

    @Test("CANCELLATION MID-DELAY drops the retry — after the claim, never before it")
    func cancellationMidDelayDropsRetryAfterClaim() async {
        let harness = makeHarness(script: [.throwTimeout], cancelDuringDelay: true)

        let summary = await fire(harness)

        #expect(harness.fetcher.invocations == 1, "the dropped retry never fetched")
        #expect(summary.dayZeroExit == .fetchFailed, "the first run's summary is what returns")
        #expect(harness.log.events == ["claim", "sleep:30"],
                "the claim landed BEFORE the teardown — the fil5 evidence survives")
    }

    @Test("LOW DATA MODE flipped during the delay stops the retry and is recorded under its own exit")
    func lowDataModeFlippedDuringDelayStopsRetry() async {
        let ldmOn = DayZeroTransportSnapshot(
            reachability: .wifi, isLowDataMode: true, allowsCellular: false
        )
        let harness = makeHarness(script: [.throwTimeout], transports: [.testWifi, ldmOn])

        _ = await fire(harness)

        #expect(harness.fetcher.invocations == 1, "the retry must not spend against the user's OS-level instruction")
        #expect(harness.suppressions.recorded.map(\.exit) == [.deniedLowDataMode])
        #expect(harness.log.events == ["claim", "sleep:30"],
                "the claim still stands — the retry was owed and the gate, not teardown, declined it")
    }

    @Test("a DAILY WINDOW drained during the delay stops the retry — a concurrent kickoff's spend is honored")
    func drainedBudgetDuringDelayStopsRetry() async {
        let drained = RediffDayZeroBudgetWindow(
            startedAt: Self.witnessNow - 3_600,
            spentBytes: RediffDayZeroDailyBudget.dailyCapBytes
        )
        let harness = makeHarness(script: [.throwTimeout], budgetWindows: [.empty, drained])

        _ = await fire(harness)

        #expect(harness.fetcher.invocations == 1)
        #expect(harness.suppressions.recorded.map(\.exit) == [.deniedDailyBudget])
    }
}

/// Budget windows served in sequence, mirroring `SequencedTransportProvider`:
/// the first read admits the first run, the second read is what the retry
/// re-checks after its delay.
final class SequencedWindowProvider: @unchecked Sendable {
    private let lock = NSLock()
    private var remaining: [RediffDayZeroBudgetWindow]
    init(_ windows: [RediffDayZeroBudgetWindow]) { self.remaining = windows }
    func next() -> RediffDayZeroBudgetWindow {
        lock.lock(); defer { lock.unlock() }
        return remaining.count > 1 ? remaining.removeFirst() : remaining[0]
    }
}

// MARK: - Durable rows (real store, production recorder)

@Suite("Day-0 same-session retry durability (playhead-3oyz, V46)")
struct DayZeroSameSessionRetryStoreTests {

    static let enclosure = URL(string: "https://cdn.example.com/current.mp3")!
    static let played = URL(fileURLWithPath: "/tmp/played-a.mp3")
    /// The witness instants, verbatim from the device pull.
    static let downloadListenGap: Double = 1_786_009_115 - 1_785_978_148
    static let attemptAt: Double = 1_785_978_148

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id, episodeId: "ep-\(id)", assetFingerprint: "fp-\(id)",
            weakFingerprint: nil, sourceURL: "file:///tmp/\(id).mp3",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil, episodeDurationSec: 100
        )
    }

    /// A trigger wired the way `PlayheadRuntime` wires it: attempt context,
    /// suppressions and the retry claim all through ONE real store, outcomes
    /// through the PRODUCTION recorder — so what these tests read back is the
    /// row a device pull would show.
    private func makeStoreBackedTrigger(
        store: AnalysisStore,
        fetcher: ScriptedKWayFetcher,
        minter: SpyDayZeroMinter,
        recorderNow: @escaping @Sendable () -> Double,
        claimAt: @escaping @Sendable () -> Double,
        cancelDuringDelay: Bool = false
    ) -> DayZeroRediffTrigger {
        let service = RediffRefetchService(
            enabled: true,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: fetcher,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: AnalysisStoreRediffRefetchRecorder(store: store, now: recorderNow),
            fileRemover: SpyTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            dayZeroMinter: minter,
            now: recorderNow
        )
        return DayZeroRediffTrigger(
            service: service,
            enabled: true,
            kWayFetchCount: 2,
            transportProvider: { .testWifi },
            chargeStateProvider: { true },
            deepScanOptInProvider: { false },
            attemptContextProvider: { assetId in
                (try? await store.fetchDayZeroAttemptContext(assetId: assetId)) ?? .never
            },
            suppressionRecorder: { assetId, reason, at in
                try? await store.noteRediffDayZeroSuppression(
                    assetId: assetId, reason: reason, at: at
                )
            },
            mintedMarkDelivery: { _ in },
            budgetWindowProvider: { .empty },
            budgetSpendRecorder: { _, _ in },
            retryClaimRecorder: { assetId in
                try? await store.noteRediffDayZeroRetryClaim(
                    assetId: assetId, at: claimAt()
                )
            },
            retryDelay: { _ in
                if cancelDuringDelay { withUnsafeCurrentTask { $0?.cancel() } }
            }
        )
    }

    @Test("F4CE7F47 END-TO-END: timeout at download, retry marks, and the hours-later listen finds the lane LIT")
    func witnessScenarioEndToEnd() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "F4CE7F47"))
        let fetcher = ScriptedKWayFetcher(script: [.throwTimeout])
        let minter = SpyDayZeroMinter()
        let trigger = makeStoreBackedTrigger(
            store: store, fetcher: fetcher, minter: minter,
            recorderNow: { Self.attemptAt },
            claimAt: { Self.attemptAt + 30 }
        )

        // 01:02 — the download-time kickoff. The fetch times out landing
        // nothing; the same-session retry lands both copies and marks.
        let summary = await Task {
            await trigger.triggerIfEligible(
                analysisAssetId: "F4CE7F47", enclosureURL: Self.enclosure,
                playedFileURL: Self.played, at: Self.attemptAt
            )
        }.value
        #expect(summary.dayZeroMarkCount == 1)

        // The durable record a device pull would show. Pre-fix this row read
        // attemptCount=1 / lastExit=fetch_failed / no claim — the witness row.
        let afterRetry = try #require(try await store.fetchRediffDayZeroAttempt(assetId: "F4CE7F47"))
        #expect(afterRetry.attemptCount == 2, "the retry is an ATTEMPT with its own row advance")
        #expect(afterRetry.lastExit == .marked, "the retry's own exit, not the timeout's")
        #expect(afterRetry.retryClaimCount == 1, "the claim survives the retry that honored it")
        #expect(afterRetry.lastRetryClaimAt == Self.attemptAt + 30)
        #expect(afterRetry.lastFullFetchBytes == 108_000_000)
        #expect(afterRetry.totalFullFetchBytes == 108_000_000,
                "the timed-out run contributed ZERO to the cumulative spend — the measured quantity")

        // 09:26 — the listen, hours later. Pre-fix: suppressedByBackoff over a
        // markless asset (the witness's suppressedCount=2). Post-fix: the
        // asset is terminal-marked, and the suppression says so.
        _ = await trigger.triggerIfEligible(
            analysisAssetId: "F4CE7F47", enclosureURL: Self.enclosure,
            playedFileURL: Self.played, at: Self.attemptAt + Self.downloadListenGap
        )
        let midListen = try #require(try await store.fetchRediffDayZeroAttempt(assetId: "F4CE7F47"))
        #expect(midListen.suppressedCount == 1)
        #expect(midListen.lastExit == .marked,
                "the mid-listen decline is 'already done', no longer 'backing off a free failure'")
        #expect(fetcher.invocations == 3, "the listen re-drive spends nothing")
    }

    @Test("A DROPPED RETRY IS QUERYABLE: lastRetryClaimAt > lastAttemptAt with no attempt after it (fil5)")
    func droppedRetryLeavesDurableClaim() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "drop-1"))
        let fetcher = ScriptedKWayFetcher(script: [.throwTimeout, .throwTimeout])
        let trigger = makeStoreBackedTrigger(
            store: store, fetcher: fetcher, minter: SpyDayZeroMinter(),
            recorderNow: { Self.attemptAt },
            claimAt: { Self.attemptAt + 30 },
            cancelDuringDelay: true
        )

        _ = await Task {
            await trigger.triggerIfEligible(
                analysisAssetId: "drop-1", enclosureURL: Self.enclosure,
                playedFileURL: Self.played, at: Self.attemptAt
            )
        }.value

        let row = try #require(try await store.fetchRediffDayZeroAttempt(assetId: "drop-1"))
        #expect(row.attemptCount == 1, "the retry never ran")
        #expect(row.lastExit == .fetchFailed)
        #expect(row.retryClaimCount == 1, "…but the CLAIM is on disk")
        let claimAt = try #require(row.lastRetryClaimAt)
        #expect(claimAt > row.lastAttemptAt,
                "the queryable dropped-retry signature: claimed after the last attempt, no attempt followed")
        #expect(fetcher.invocations == 1)
    }

    @Test("noteRediffDayZeroRetryClaim owns ONLY its pair — attempt budget, exit and suppressions untouched")
    func claimWriterTouchesOnlyItsColumns() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "c1"))
        let attempt = RediffDayZeroAttemptRecord(
            analysisAssetId: "c1", attemptCount: 1, lastAttemptAt: 1_000,
            lastExit: .fetchFailed, suppressedCount: 2, lastSuppressedAt: 900,
            lastDetail: "-1001"
        )
        try await store.upsertRediffDayZeroAttempt(attempt)

        try await store.noteRediffDayZeroRetryClaim(assetId: "c1", at: 1_030)
        try await store.noteRediffDayZeroRetryClaim(assetId: "c1", at: 1_090)

        let row = try #require(try await store.fetchRediffDayZeroAttempt(assetId: "c1"))
        #expect(row.retryClaimCount == 2, "claims accumulate in SQL, race-free")
        #expect(row.lastRetryClaimAt == 1_090)
        #expect(row.attemptCount == 1, "a claim is not an attempt")
        #expect(row.lastExit == .fetchFailed, "the failed attempt's exit survives the claim")
        #expect(row.suppressedCount == 2, "the suppression pair belongs to its own writer")
        #expect(row.lastSuppressedAt == 900)
        #expect(row.lastDetail == "-1001")
    }

    @Test("the claim's INSERT arm seeds a recordable row even when no attempt row exists")
    func claimInsertArmIsDefensive() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "c2"))
        try await store.noteRediffDayZeroRetryClaim(assetId: "c2", at: 500)
        let row = try #require(try await store.fetchRediffDayZeroAttempt(assetId: "c2"))
        #expect(row.retryClaimCount == 1)
        #expect(row.lastRetryClaimAt == 500)
        #expect(row.attemptCount == 0, "seeded immediately eligible — a claim must not spend the budget")
    }
}
