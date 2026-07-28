// RediffDayZeroAccountabilityTests.swift
// playhead-p70f: the day-0 rediff path spent 299.6 MB on the owner's device and
// produced zero ad windows, zero state rows, and four outcome counters reading
// 0. These tests pin the two mechanisms that make that impossible to repeat:
//
//   1. Every day-0 exit is DISTINGUISHABLE (`RediffDayZeroExit`) and every
//      attempt leaves a durable record naming which one fired.
//   2. A repeat play does NOT re-spend the ~108 MB k-way fetch
//      (`DayZeroRediffAttemptPolicy`).
//
// Pure over `(record, now)` — no store, no clock, no network.

import Foundation
import SQLite3
import Testing

@testable import Playhead

@Suite("Rediff day-0 accountability (playhead-p70f)")
struct RediffDayZeroAccountabilityTests {

    typealias Policy = DayZeroRediffAttemptPolicy
    typealias Exit = RediffDayZeroExit
    static let hour: Double = 3600
    static let day: Double = 24 * 3600

    private static func record(
        attempts: Int,
        at lastAttemptAt: Double,
        exit: Exit,
        totalBytes: Int = 0
    ) -> RediffDayZeroAttemptRecord {
        RediffDayZeroAttemptRecord(
            analysisAssetId: "asset-1",
            attemptCount: attempts,
            lastAttemptAt: lastAttemptAt,
            lastExit: exit,
            totalFullFetchBytes: totalBytes
        )
    }

    // MARK: - Exit taxonomy

    @Test("every formerly silent mint exit has its own raw value")
    func exitRawValuesAreDistinct() {
        let raws = Exit.allCases.map(\.rawValue)
        #expect(Set(raws).count == raws.count, "raw values are persisted — collisions would merge two exits in the DB")
        // The exits the p70f trace could not tell apart. If any two of these
        // ever collapse to one case again, this fails.
        let indistinguishableBefore: [Exit] = [
            .assetRowMissing, .assetFetchFailed, .aSideNotAnchored, .aSideReadFailed,
            .fetchFailed, .tooFewBCopies, .noAcceptedByteDiff, .noDivergentSlot,
            .allSlotsAlreadyCovered, .persistFailed
        ]
        #expect(Set(indistinguishableBefore).count == indistinguishableBefore.count)
    }

    @Test("the AAC-behind-an-.mp3-suffix hypothesis is distinguishable from clean-but-identical copies")
    func gateRejectVersusNoDivergence() {
        // If RediffByteAligner (an MP3-frame parser) finds no frames because the
        // bytes are AAC, EVERY B-copy gate-rejects.
        let allRejected = RediffDayZeroMintOutcome(
            exit: .noAcceptedByteDiff, bSideCount: 2, bSidesAccepted: 0,
            bSidesGateRejected: 2, bSidesUnreadable: 0, divergentSlotCount: 0
        )
        // If the bytes ARE MP3 and the CDN simply served identical copies, the
        // diffs are accepted and the union is empty.
        let diffedButIdentical = RediffDayZeroMintOutcome(
            exit: .noDivergentSlot, bSideCount: 2, bSidesAccepted: 2,
            bSidesGateRejected: 0, bSidesUnreadable: 0, divergentSlotCount: 0
        )
        #expect(allRejected.exit != diffedButIdentical.exit)
        #expect(allRejected.bSidesGateRejected == allRejected.bSideCount)
        #expect(diffedButIdentical.bSidesAccepted == diffedButIdentical.bSideCount)
    }

    @Test("pre-fetch exits are marked free; post-fetch exits are marked as having spent bandwidth")
    func spentBandwidthClassification() {
        for exit in [Exit.minterUnavailable, .assetRowMissing, .assetFetchFailed,
                     .aSideNotAnchored, .aSideReadFailed, .suppressedByBackoff, .alreadyInFlight] {
            #expect(exit.spentBandwidth == false, "\(exit.rawValue) must be reachable before the ~108 MB fetch")
        }
        for exit in [Exit.fetchFailed, .tooFewBCopies, .noAcceptedByteDiff,
                     .noDivergentSlot, .allSlotsAlreadyCovered, .persistFailed, .marked] {
            #expect(exit.spentBandwidth, "\(exit.rawValue) only happens after the fetch")
        }
    }

    // MARK: - Idempotency: the replay bleed

    @Test("a first attempt is always allowed")
    func firstAttemptAllowed() {
        #expect(Policy.decide(record: nil, now: 1_000) == .attempt(attemptNumber: 1))
    }

    @Test("REPLAY BLEED: replaying the same episode inside the backoff window spends nothing")
    func replayWithinBackoffIsSuppressed() {
        // The exact device scenario: one day-0 run diffed cleanly and found no
        // divergence, then the user replayed the episode.
        let after = Self.record(attempts: 1, at: 1_000, exit: .noDivergentSlot, totalBytes: 108_000_000)
        // Minutes later — a replay in the same listening session.
        let decision = Policy.decide(record: after, now: 1_000 + 10 * Self.hour)
        #expect(decision == .suppress(reason: .suppressedByBackoff, nextEligibleAt: 1_000 + Self.day))
        #expect(decision.isAttempt == false, "a replay must not re-spend ~108 MB")
    }

    @Test("after the backoff elapses the next attempt is allowed and numbered")
    func attemptAllowedAfterBackoff() {
        let after = Self.record(attempts: 1, at: 1_000, exit: .noDivergentSlot)
        #expect(Policy.decide(record: after, now: 1_000 + Self.day) == .attempt(attemptNumber: 2))
        #expect(Policy.decide(record: after, now: 1_000 + Self.day - 1).isAttempt == false)
    }

    @Test("backoff escalates 24h → 48h → 96h and is capped at 7 days")
    func backoffEscalates() {
        #expect(Policy.backoff(afterAttempts: 0) == 0)
        #expect(Policy.backoff(afterAttempts: 1) == Self.day)
        #expect(Policy.backoff(afterAttempts: 2) == 2 * Self.day)
        #expect(Policy.backoff(afterAttempts: 3) == 4 * Self.day)
        #expect(Policy.backoff(afterAttempts: 10) == Policy.maxBackoff)
        // A corrupted/absurd count must not trap on the shift.
        #expect(Policy.backoff(afterAttempts: Int.max) == Policy.maxBackoff)
    }

    @Test("the attempt budget is a hard cap — never eligible again once spent")
    func attemptBudgetIsTerminal() {
        let spent = Self.record(attempts: Policy.maxAttempts, at: 1_000, exit: .noDivergentSlot)
        let decision = Policy.decide(record: spent, now: 1_000 + 365 * Self.day)
        #expect(decision == .suppress(reason: .suppressedByBackoff, nextEligibleAt: nil))
    }

    // MARK: - The budget only counts BANDWIDTH (review round 1)

    /// The budget exists to bound ~108 MB fetches. A pre-fetch exit spends
    /// nothing, so it must consume neither an attempt nor a backoff window.
    ///
    /// This is not hypothetical. `runDayZeroRefetch` now runs the A-side checks
    /// BEFORE the fetch (change 3), and on the STREAMING play path day-0 fires
    /// before `downloadComplete()` — so `.aSideNotAnchored` is the routine,
    /// transient, zero-cost decline. If it consumed the budget, three streaming
    /// starts would permanently lock out an episode day-0 never attempted.
    @Test("a FREE pre-fetch exit consumes no attempt and no backoff window")
    func freeExitDoesNotConsumeTheAttemptBudget() {
        var folded: RediffDayZeroAttemptRecord?
        for tick in 1...(Policy.maxAttempts + 3) {
            folded = Policy.advance(
                record: folded, assetId: "a",
                outcome: .blocked(.aSideNotAnchored),
                fullFetchBytes: 0, at: Double(tick)
            )
        }
        #expect(folded?.attemptCount == 0, "a zero-byte exit is not an attempt")
        #expect(folded?.totalFullFetchBytes == 0)
        // …and the exit is still NAMED, so the decline stays diagnosable.
        #expect(folded?.lastExit == .aSideNotAnchored)
        // The asset is still immediately eligible — the A-side that
        // materializes during the next play gets its chance.
        #expect(Policy.decide(record: folded, now: 10) == .attempt(attemptNumber: 1))
    }

    @Test("a free exit AFTER a real attempt does not push the backoff window forward")
    func freeExitDoesNotExtendTheBackoffWindow() {
        let real = Policy.advance(
            record: nil, assetId: "a",
            outcome: RediffDayZeroMintOutcome(exit: .noDivergentSlot, bSideCount: 2, bSidesAccepted: 2),
            fullFetchBytes: 108_000_000, at: 1_000
        )
        #expect(real.attemptCount == 1)
        // A streaming replay 23 h later blocks for free…
        let blocked = Policy.advance(
            record: real, assetId: "a",
            outcome: .blocked(.aSideNotAnchored), fullFetchBytes: 0, at: 1_000 + 23 * Self.hour
        )
        #expect(blocked.attemptCount == 1, "still one real attempt")
        #expect(blocked.lastAttemptAt == 1_000, "the free block is not an attempt time")
        // …and the ORIGINAL 24 h window still expires on schedule.
        #expect(Policy.decide(record: blocked, now: 1_000 + Self.day) == .attempt(attemptNumber: 2))
    }

    // MARK: - Exhaustion is recoverable when day-0 itself changes

    /// THE TRAP THIS CLOSES. playhead-p70f deliberately does NOT fix why day-0
    /// mints nothing, so every replayed episode burns its budget on a failure
    /// mode that is about to be repaired. Without a generation stamp the owner
    /// would fix the mint and observe no change at all.
    @Test("an EXHAUSTED asset becomes eligible again when the day-0 generation moves")
    func exhaustedBudgetIsRecoverableAcrossGenerations() {
        let exhausted = RediffDayZeroAttemptRecord(
            analysisAssetId: "a", attemptCount: Policy.maxAttempts, lastAttemptAt: 1_000,
            lastExit: .noDivergentSlot, totalFullFetchBytes: 324_000_000,
            policyGeneration: Policy.currentGeneration
        )
        #expect(Policy.decide(record: exhausted, now: 1_000 + 365 * Self.day).isAttempt == false)

        // The SAME row read by a build whose day-0 behavior has moved on.
        let stale = RediffDayZeroAttemptRecord(
            analysisAssetId: "a", attemptCount: Policy.maxAttempts, lastAttemptAt: 1_000,
            lastExit: .noDivergentSlot, totalFullFetchBytes: 324_000_000,
            policyGeneration: Policy.currentGeneration - 1
        )
        #expect(Policy.decide(record: stale, now: 1_000 + 60) == .attempt(attemptNumber: 1),
                "fixing the mint must actually reach the episodes that failed under the old one")

        // And the budget genuinely restarts, rather than folding onto the old count.
        let folded = Policy.advance(
            record: stale, assetId: "a",
            outcome: RediffDayZeroMintOutcome(exit: .noDivergentSlot, bSideCount: 2, bSidesAccepted: 2),
            fullFetchBytes: 108_000_000, at: 2_000
        )
        #expect(folded.attemptCount == 1, "a new generation earns a whole budget, not one more attempt")
        #expect(folded.policyGeneration == Policy.currentGeneration)
        #expect(folded.totalFullFetchBytes == 432_000_000, "cumulative cost is a fact and still carries")
    }

    @Test("a MARKED asset stays terminal even across a generation change")
    func markedSurvivesGenerationChange() {
        let marked = RediffDayZeroAttemptRecord(
            analysisAssetId: "a", attemptCount: 1, lastAttemptAt: 1_000,
            lastExit: .marked, lastMarkCount: 2,
            policyGeneration: Policy.currentGeneration - 1
        )
        // The marks are already on disk; re-fetching would spend ~108 MB to
        // mint nothing, so the generation reset must NOT reach this case.
        #expect(Policy.decide(record: marked, now: 1_000 + 365 * Self.day)
                == .suppress(reason: .marked, nextEligibleAt: nil))
    }

    @Test("a SUCCESSFUL day-0 never re-fetches, no matter how much time passes")
    func markedIsPermanentlyTerminal() {
        let marked = RediffDayZeroAttemptRecord(
            analysisAssetId: "asset-1", attemptCount: 1, lastAttemptAt: 1_000,
            lastExit: .marked, lastMarkCount: 3
        )
        #expect(Policy.decide(record: marked, now: 1_000 + 365 * Self.day)
                == .suppress(reason: .marked, nextEligibleAt: nil))
    }

    // MARK: - Record folding

    @Test("advance accumulates attempts AND cumulative bytes so per-asset bleed is visible in one row")
    func advanceAccumulates() {
        let first = Policy.advance(
            record: nil, assetId: "a",
            outcome: RediffDayZeroMintOutcome(exit: .noDivergentSlot, bSideCount: 2, bSidesAccepted: 2),
            fullFetchBytes: 108_000_000, at: 100
        )
        #expect(first.attemptCount == 1)
        #expect(first.totalFullFetchBytes == 108_000_000)
        #expect(first.lastExit == .noDivergentSlot)
        #expect(first.lastBSidesAccepted == 2)

        let second = Policy.advance(
            record: first, assetId: "a",
            outcome: RediffDayZeroMintOutcome(exit: .noAcceptedByteDiff, bSideCount: 2, bSidesGateRejected: 2),
            fullFetchBytes: 108_000_000, at: 200
        )
        #expect(second.attemptCount == 2)
        #expect(second.totalFullFetchBytes == 216_000_000, "cumulative bytes are the bleed signal")
        #expect(second.lastFullFetchBytes == 108_000_000)
        #expect(second.lastExit == .noAcceptedByteDiff)
        #expect(second.lastAttemptAt == 200)
    }

    @Test("a suppressed attempt records ZERO bytes")
    func suppressedAttemptRecordsNoBytes() {
        let prior = Self.record(attempts: 1, at: 100, exit: .noDivergentSlot, totalBytes: 108_000_000)
        let folded = Policy.advance(
            record: prior, assetId: "a",
            outcome: .blocked(.suppressedByBackoff), fullFetchBytes: 0, at: 200
        )
        #expect(folded.totalFullFetchBytes == 108_000_000, "suppression must not add bytes")
        #expect(folded.lastFullFetchBytes == 0)
    }

    @Test("persisted detail is truncated — the database is not a log")
    func detailIsTruncated() {
        let huge = String(repeating: "x", count: 5_000)
        let folded = Policy.advance(
            record: nil, assetId: "a",
            outcome: RediffDayZeroMintOutcome(exit: .fetchFailed, detail: huge),
            fullFetchBytes: 0, at: 1
        )
        #expect(folded.lastDetail?.count == Policy.detailCharCap)
    }
}

// MARK: - Service level: free pre-checks, in-flight guard, named fetch failure

/// The behaviors that only exist once the policy is wired into the real
/// `RediffRefetchService`. These use the SAME spy doubles the xsdz.36.4 suites
/// use, so a regression here is a regression in production wiring, not in a
/// bespoke harness.
@Suite("Rediff day-0 service accountability (playhead-p70f)")
struct RediffDayZeroServiceAccountabilityTests {

    static let enclosure = URL(string: "https://cdn.example.com/current.mp3")!
    static let played = URL(fileURLWithPath: "/tmp/played-a.mp3")

    private func makeService(
        fetcher: KWaySpyFullFetcher,
        minter: SpyDayZeroMinter,
        recorder: SpyRefetchRecorder,
        remover: SpyTempFileRemover = SpyTempFileRemover()
    ) -> RediffRefetchService {
        RediffRefetchService(
            enabled: true,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: fetcher,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: recorder,
            fileRemover: remover,
            taskScheduler: StubTaskScheduler(),
            dayZeroMinter: minter,
            now: { 1_000 }
        )
    }

    private func candidate(_ assetId: String = "asset-day0") -> RediffRefetchCandidate {
        RediffRefetchCandidate(
            assetId: assetId, enclosureURL: Self.enclosure,
            downloadedAt: 0, localAudioURL: Self.played, attemptState: .initial
        )
    }

    @Test("CHANGE 3: a doomed local pre-check costs ZERO bytes — the fetcher is never called")
    func prefetchBlockerSkipsTheFetchEntirely() async {
        let fetcher = KWaySpyFullFetcher()
        let minter = SpyDayZeroMinter()
        // The A-side is missing. Before playhead-p70f this was discovered only
        // AFTER ~108 MB had been downloaded and billed.
        minter.prefetchBlockerToReturn = .aSideNotAnchored
        let recorder = SpyRefetchRecorder()
        let service = makeService(fetcher: fetcher, minter: minter, recorder: recorder)

        let summary = await service.runDayZeroRefetch(for: candidate(), kWayFetchCount: 2)

        #expect(fetcher.calls.isEmpty, "THE fix: not one byte is fetched for a doomed attempt")
        #expect(summary.fullFetchBytes == 0)
        #expect(minter.calls.isEmpty, "the mint is not attempted without an A-side")
        #expect(minter.prefetchBlockerCalls == ["asset-day0"])
        guard case let .dayZeroUnmarked(_, cost, mint) = recorder.outcomes.first else {
            Issue.record("expected .dayZeroUnmarked, got \(String(describing: recorder.outcomes.first))"); return
        }
        #expect(mint.exit == .aSideNotAnchored, "the exit is NAMED, not an anonymous zero")
        #expect(cost.fullFetchBytes == 0)
    }

    @Test("an unblocked pre-check proceeds to the fetch as before")
    func unblockedPrefetchStillFetches() async {
        let fetcher = KWaySpyFullFetcher()
        let minter = SpyDayZeroMinter()          // prefetchBlockerToReturn == nil
        let recorder = SpyRefetchRecorder()
        let service = makeService(fetcher: fetcher, minter: minter, recorder: recorder)

        _ = await service.runDayZeroRefetch(for: candidate(), kWayFetchCount: 2)

        #expect(fetcher.calls.count == 2, "a viable attempt still fetches K copies")
        #expect(minter.calls.count == 1)
    }

    /// Suspends the FIRST download until released, so a second kickoff really
    /// does overlap the first — the on-device shape, where the k-way fetch is a
    /// multi-second network round trip.
    ///
    /// This matters: an earlier version of this test used two bare `async let`s
    /// and PASSED WRONGLY in the other direction — the spy fetcher never
    /// suspends, so the actor ran the two calls strictly in sequence and four
    /// fetches was the correct answer. Without a real suspension point the test
    /// asserts nothing about the guard.
    private actor DownloadGate {
        private var waiters: [CheckedContinuation<Void, Never>] = []
        private var arrivalWaiters: [CheckedContinuation<Void, Never>] = []
        private var arrived = false
        private var released = false

        func wait() async {
            arrived = true
            for waiter in arrivalWaiters { waiter.resume() }
            arrivalWaiters = []
            if released { return }
            await withCheckedContinuation { waiters.append($0) }
        }

        func waitUntilArrived() async {
            if arrived { return }
            await withCheckedContinuation { arrivalWaiters.append($0) }
        }

        func release() {
            released = true
            for waiter in waiters { waiter.resume() }
            waiters = []
        }
    }

    private final class GatedFullFetcher: FullEpisodeFetching, @unchecked Sendable {
        let gate: DownloadGate
        private(set) var callCount = 0
        init(gate: DownloadGate) { self.gate = gate }
        func download(url: URL) async throws -> (fileURL: URL, byteCount: Int) {
            try await download(url: url, persona: nil)
        }
        func download(url: URL, persona: RediffFetchPersona?) async throws -> (fileURL: URL, byteCount: Int) {
            let index = callCount
            callCount += 1
            if index == 0 { await gate.wait() }   // hold the actor open
            return (URL(fileURLWithPath: "/tmp/gated-bcopy-\(index).mp3"), 54_000_000)
        }
    }

    private func makeGatedService(
        fetcher: GatedFullFetcher,
        minter: SpyDayZeroMinter,
        recorder: SpyRefetchRecorder
    ) -> RediffRefetchService {
        RediffRefetchService(
            enabled: true,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: fetcher,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: recorder,
            fileRemover: SpyTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            dayZeroMinter: minter,
            now: { 1_000 }
        )
    }

    @Test("CHANGE 2 (in-process): a kickoff that OVERLAPS an in-flight one fetches nothing")
    func overlappingKickoffDoesNotDoubleFetch() async {
        let gate = DownloadGate()
        let fetcher = GatedFullFetcher(gate: gate)
        let minter = SpyDayZeroMinter()
        let recorder = SpyRefetchRecorder()
        let service = makeGatedService(fetcher: fetcher, minter: minter, recorder: recorder)

        // `kickOffDayZeroRediff` fires from both play paths and the
        // "Download & Analyze" preparation kickoff; none of them coordinate.
        let inFlight = Task { await service.runDayZeroRefetch(for: candidate(), kWayFetchCount: 2) }
        await gate.waitUntilArrived()   // the first attempt is genuinely mid-fetch

        let overlapping = await service.runDayZeroRefetch(for: candidate(), kWayFetchCount: 2)
        #expect(overlapping.eligibleProcessed == 0, "the overlapping kickoff was declined")
        // The declined one is RECORDED — a silent decline is what made the
        // original 299.6 MB invisible.
        let declined = recorder.outcomes.contains { outcome in
            if case let .dayZeroUnmarked(_, cost, mint) = outcome {
                return mint.exit == .alreadyInFlight && cost.fullFetchBytes == 0
            }
            return false
        }
        #expect(declined, "the duplicate kickoff leaves a zero-byte trace")

        await gate.release()
        let first = await inFlight.value

        #expect(fetcher.callCount == 2, "exactly ONE k-way batch, not two — got \(fetcher.callCount) fetches")
        #expect(minter.calls.count == 1)
        #expect(first.eligibleProcessed == 1)
    }

    @Test("a DIFFERENT episode is not blocked by another episode's in-flight attempt")
    func inFlightGuardIsPerAsset() async {
        let gate = DownloadGate()
        let fetcher = GatedFullFetcher(gate: gate)
        let minter = SpyDayZeroMinter()
        let recorder = SpyRefetchRecorder()
        let service = makeGatedService(fetcher: fetcher, minter: minter, recorder: recorder)

        let inFlight = Task { await service.runDayZeroRefetch(for: candidate("asset-A"), kWayFetchCount: 2) }
        await gate.waitUntilArrived()

        let other = await service.runDayZeroRefetch(for: candidate("asset-B"), kWayFetchCount: 2)
        await gate.release()
        _ = await inFlight.value

        #expect(other.eligibleProcessed == 1, "a DIFFERENT episode must not be blocked")
        #expect(fetcher.callCount == 4, "two distinct episodes each get their own k-way batch")
    }

    @Test("the in-flight claim is RELEASED — a later attempt for the same asset is admitted")
    func inFlightClaimIsReleased() async {
        let fetcher = KWaySpyFullFetcher()
        let minter = SpyDayZeroMinter()
        let recorder = SpyRefetchRecorder()
        let service = makeService(fetcher: fetcher, minter: minter, recorder: recorder)

        _ = await service.runDayZeroRefetch(for: candidate(), kWayFetchCount: 2)
        _ = await service.runDayZeroRefetch(for: candidate(), kWayFetchCount: 2)

        // The DURABLE backoff (not this guard) is what suppresses a replay; the
        // in-flight set must not leak a permanent claim.
        #expect(fetcher.calls.count == 4, "sequential attempts are both admitted by the in-flight guard")
    }

    @Test("a THROWN fetch records .fetchFailed — distinguishable from a clean no-mark run")
    func thrownFetchIsNamed() async {
        let fetcher = KWaySpyFullFetcher()
        fetcher.throwOnCallIndex = 1        // mid-batch network failure
        let minter = SpyDayZeroMinter()
        let recorder = SpyRefetchRecorder()
        let service = makeService(fetcher: fetcher, minter: minter, recorder: recorder)

        let summary = await service.runDayZeroRefetch(for: candidate(), kWayFetchCount: 2)

        guard case let .dayZeroUnmarked(_, cost, mint) = recorder.outcomes.first else {
            Issue.record("expected .dayZeroUnmarked, got \(String(describing: recorder.outcomes.first))"); return
        }
        // THE regression this pins: a thrown fetch and a clean "nothing
        // diverged" run used to be byte-identical in the database (both
        // `.dayZeroUnmarked`, neither incrementing any counter).
        #expect(mint.exit == .fetchFailed)
        #expect(mint.detail != nil, "the error text is carried")
        #expect(cost.fullFetchBytes == 54_000_000, "the copy fetched before the throw is still billed")
        #expect(summary.failedCount == 1)
    }

    @Test("a nil minter records .minterUnavailable instead of returning silently")
    func nilMinterIsRecorded() async {
        let recorder = SpyRefetchRecorder()
        let fetcher = KWaySpyFullFetcher()
        let service = RediffRefetchService(
            enabled: true,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: StubRangedSampler(),
            localSampler: StubLocalSampler(),
            fullFetcher: fetcher,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: recorder,
            fileRemover: SpyTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            dayZeroMinter: nil,
            now: { 1_000 }
        )

        _ = await service.runDayZeroRefetch(for: candidate(), kWayFetchCount: 2)

        #expect(fetcher.calls.isEmpty)
        guard case let .dayZeroUnmarked(_, _, mint) = recorder.outcomes.first else {
            Issue.record("expected a recorded outcome, got \(String(describing: recorder.outcomes.first))"); return
        }
        #expect(mint.exit == .minterUnavailable)
    }
}

// MARK: - Trigger level: the durable replay suppression

@Suite("Rediff day-0 trigger idempotency (playhead-p70f)")
struct RediffDayZeroTriggerIdempotencyTests {

    static let enclosure = URL(string: "https://cdn.example.com/current.mp3")!
    static let played = URL(fileURLWithPath: "/tmp/played-a.mp3")
    static let day: Double = 24 * 3600

    /// Records what the trigger asked for and what it declined, over a REAL
    /// service wired with a spy fetcher — so "did it spend bytes?" is answered
    /// by the fetcher, not by a mock of the trigger's own decision.
    private final class SuppressionSpy: @unchecked Sendable {
        var recorded: [(assetId: String, reason: RediffDayZeroExit, at: Double)] = []
    }

    private func makeTrigger(
        prior: RediffDayZeroAttemptRecord?,
        fetcher: KWaySpyFullFetcher,
        spy: SuppressionSpy
    ) -> DayZeroRediffTrigger {
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
            dayZeroMinter: SpyDayZeroMinter(),
            now: { 0 }
        )
        return DayZeroRediffTrigger(
            service: service,
            enabled: true,
            kWayFetchCount: 2,
            reachabilityProvider: { .wifi },
            chargeStateProvider: { true },
            deepScanOptInProvider: { false },
            attemptRecordProvider: { _ in prior },
            suppressionRecorder: { assetId, reason, at in
                spy.recorded.append((assetId, reason, at))
            }
        )
    }

    private func fire(_ trigger: DayZeroRediffTrigger, at now: Double) async -> RediffRefetchService.SweepSummary {
        await trigger.triggerIfEligible(
            analysisAssetId: "asset-day0",
            enclosureURL: Self.enclosure,
            playedFileURL: Self.played,
            at: now
        )
    }

    @Test("THE BLEED: replaying an episode whose day-0 already ran spends ZERO bytes")
    func replayInsideBackoffFetchesNothing() async {
        let fetcher = KWaySpyFullFetcher()
        let spy = SuppressionSpy()
        let prior = RediffDayZeroAttemptRecord(
            analysisAssetId: "asset-day0", attemptCount: 1, lastAttemptAt: 1_000,
            lastExit: .noDivergentSlot, totalFullFetchBytes: 108_000_000
        )
        let trigger = makeTrigger(prior: prior, fetcher: fetcher, spy: spy)

        let summary = await fire(trigger, at: 1_000 + 3600)   // an hour later

        #expect(fetcher.calls.isEmpty, "a replay must not re-spend ~108 MB")
        #expect(summary == RediffRefetchService.SweepSummary())
        #expect(spy.recorded.count == 1, "the decline is recorded, not silent")
        #expect(spy.recorded.first?.reason == .suppressedByBackoff)
    }

    @Test("an episode day-0 ALREADY MARKED is never re-fetched")
    func markedEpisodeIsNeverRefetched() async {
        let fetcher = KWaySpyFullFetcher()
        let spy = SuppressionSpy()
        let prior = RediffDayZeroAttemptRecord(
            analysisAssetId: "asset-day0", attemptCount: 1, lastAttemptAt: 1_000,
            lastExit: .marked, lastMarkCount: 3
        )
        let trigger = makeTrigger(prior: prior, fetcher: fetcher, spy: spy)

        _ = await fire(trigger, at: 1_000 + 365 * Self.day)

        #expect(fetcher.calls.isEmpty)
        #expect(spy.recorded.first?.reason == .marked)
    }

    @Test("once the backoff elapses the trigger fires again (the suppression is not permanent)")
    func firesAfterBackoff() async {
        let fetcher = KWaySpyFullFetcher()
        let spy = SuppressionSpy()
        let prior = RediffDayZeroAttemptRecord(
            analysisAssetId: "asset-day0", attemptCount: 1, lastAttemptAt: 1_000,
            lastExit: .noDivergentSlot
        )
        let trigger = makeTrigger(prior: prior, fetcher: fetcher, spy: spy)

        _ = await fire(trigger, at: 1_000 + Self.day)

        #expect(fetcher.calls.count == 2, "the k-way fetch runs once the window elapses")
        #expect(spy.recorded.isEmpty)
    }

    @Test("with NO prior record the trigger fires — a first listen is never suppressed")
    func firstListenFires() async {
        let fetcher = KWaySpyFullFetcher()
        let spy = SuppressionSpy()
        let trigger = makeTrigger(prior: nil, fetcher: fetcher, spy: spy)

        _ = await fire(trigger, at: 5_000)

        #expect(fetcher.calls.count == 2)
        #expect(spy.recorded.isEmpty)
    }

    @Test("the suppression check runs AFTER the WiFi gate — a cellular play never reads the store")
    func cellularNeverConsultsTheRecord() async {
        let fetcher = KWaySpyFullFetcher()
        let spy = SuppressionSpy()
        let reads = SuppressionSpy()
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
            dayZeroMinter: SpyDayZeroMinter(),
            now: { 0 }
        )
        let trigger = DayZeroRediffTrigger(
            service: service, enabled: true, kWayFetchCount: 2,
            reachabilityProvider: { .cellular },
            chargeStateProvider: { true },
            deepScanOptInProvider: { false },
            attemptRecordProvider: { assetId in
                reads.recorded.append((assetId, .marked, 0))
                return nil
            },
            suppressionRecorder: { assetId, reason, at in spy.recorded.append((assetId, reason, at)) }
        )

        _ = await fire(trigger, at: 1)

        #expect(fetcher.calls.isEmpty)
        #expect(reads.recorded.isEmpty, "a cellular play short-circuits before touching the store")
        #expect(spy.recorded.isEmpty, "a gate rejection is not an attempt suppression")
    }
}

// MARK: - Store: the durable record

@Suite("Rediff day-0 attempt persistence (playhead-p70f, V38)")
struct RediffDayZeroAttemptStoreTests {

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id, episodeId: "ep-\(id)", assetFingerprint: "fp-\(id)",
            weakFingerprint: nil, sourceURL: "file:///tmp/\(id).mp3",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil, episodeDurationSec: 100
        )
    }

    @Test("the V38 table round-trips every diagnostic field")
    func roundTrip() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1"))
        let record = RediffDayZeroAttemptRecord(
            analysisAssetId: "a1", attemptCount: 2, lastAttemptAt: 1_234.5,
            lastExit: .noAcceptedByteDiff, lastMarkCount: 0, lastBSideCount: 2,
            lastBSidesAccepted: 0, lastBSidesGateRejected: 2, lastBSidesUnreadable: 0,
            lastDivergentSlotCount: 0, lastFullFetchBytes: 108_000_000,
            totalFullFetchBytes: 216_000_000, lastDetail: "boom"
        )
        try await store.upsertRediffDayZeroAttempt(record)

        let read = try await store.fetchRediffDayZeroAttempt(assetId: "a1")
        #expect(read == record, "every field survives the round trip: \(String(describing: read))")
    }

    @Test("ACCEPTANCE: after a run, one query names WHICH exit fired — without a sysdiagnose")
    func exitIsQueryable() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1"))
        try await store.insertAsset(makeAsset(id: "a2"))
        let recorder = AnalysisStoreRediffRefetchRecorder(store: store, config: .production, now: { 500 })

        await recorder.recordOutcome(.dayZeroUnmarked(
            assetId: "a1",
            cost: RediffRefetchPolicy.BandwidthCost(precheckBytes: 0, fullFetchBytes: 108_000_000),
            mint: RediffDayZeroMintOutcome(exit: .noAcceptedByteDiff, bSideCount: 2,
                                           bSidesAccepted: 0, bSidesGateRejected: 2)
        ))
        await recorder.recordOutcome(.dayZeroUnmarked(
            assetId: "a2", cost: .zero, mint: .blocked(.aSideNotAnchored)
        ))

        let rows = try await store.fetchRediffDayZeroAttempts()
        #expect(rows.count == 2)
        let byAsset = Dictionary(uniqueKeysWithValues: rows.map { ($0.analysisAssetId, $0) })
        // The MP3-parser-found-nothing diagnosis…
        #expect(byAsset["a1"]?.lastExit == .noAcceptedByteDiff)
        #expect(byAsset["a1"]?.lastBSidesGateRejected == 2)
        #expect(byAsset["a1"]?.lastBSidesAccepted == 0)
        // …versus a pre-fetch block that cost nothing.
        #expect(byAsset["a2"]?.lastExit == .aSideNotAnchored)
        #expect(byAsset["a2"]?.totalFullFetchBytes == 0)
        #expect(byAsset["a2"]?.attemptCount == 0,
                "a zero-byte block is recorded but must not consume the bandwidth budget")
        #expect(byAsset["a1"]?.attemptCount == 1, "a run that fetched IS an attempt")
        // A pre-fetch block spends nothing, so it must NOT inflate the
        // bandwidth-spent counter.
        let totals = try await store.fetchRediffBandwidthTotals()
        #expect(totals.dayZeroUnmarkedCount == 1,
                "only the run that actually spent bytes moves the byte-spend counter")
        #expect(totals.fullFetchBytesTotal == 108_000_000)
    }

    @Test("a suppression increments its own counter WITHOUT consuming the attempt budget or clobbering the last exit")
    func suppressionDoesNotConsumeBudget() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1"))
        try await store.upsertRediffDayZeroAttempt(RediffDayZeroAttemptRecord(
            analysisAssetId: "a1", attemptCount: 1, lastAttemptAt: 1_000,
            lastExit: .noDivergentSlot, lastBSideCount: 2, lastBSidesAccepted: 2,
            totalFullFetchBytes: 108_000_000
        ))

        try await store.noteRediffDayZeroSuppression(assetId: "a1", reason: .suppressedByBackoff, at: 2_000)
        try await store.noteRediffDayZeroSuppression(assetId: "a1", reason: .suppressedByBackoff, at: 3_000)

        let read = try await store.fetchRediffDayZeroAttempt(assetId: "a1")
        #expect(read?.suppressedCount == 2, "the evidence that two replays were declined")
        #expect(read?.lastSuppressedAt == 3_000)
        // The load-bearing invariants: a suppression is NOT an attempt.
        #expect(read?.attemptCount == 1, "a suppression must never consume the attempt budget")
        #expect(read?.lastExit == .noDivergentSlot, "a suppression must not overwrite the last real exit")
        #expect(read?.totalFullFetchBytes == 108_000_000, "a suppression spends nothing")
    }

    @Test("an unmarked day-0 run still writes NO rediff_refetch_state (the xsdz.36.4 poisoning fix is intact)")
    func poisoningFixSurvives() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1"))
        let recorder = AnalysisStoreRediffRefetchRecorder(store: store, config: .production, now: { 500 })

        await recorder.recordOutcome(.dayZeroUnmarked(
            assetId: "a1",
            cost: RediffRefetchPolicy.BandwidthCost(precheckBytes: 0, fullFetchBytes: 108_000_000),
            mint: RediffDayZeroMintOutcome(exit: .noDivergentSlot, bSideCount: 2, bSidesAccepted: 2)
        ))

        // The new accountability row exists…
        #expect(try await store.fetchRediffDayZeroAttempt(assetId: "a1") != nil)
        // …and the LAGGED table is still untouched, so the sweep re-enumerates it.
        #expect(try await store.fetchRediffRefetchStates().isEmpty,
                "day-0 accountability must not resurrect the poisoning bug")
    }

    /// REVIEW ROUND 1. The recorder folds an attempt with a read-modify-write
    /// (`fetch` → `advance` → `upsert`) while `noteRediffDayZeroSuppression`
    /// increments the same row in place. If the upsert wrote the suppression
    /// columns back from its own stale snapshot, a suppression landing between
    /// the two would be lost — and that interleaving is the EXPECTED one, since
    /// an `.alreadyInFlight` suppression happens precisely while the attempt it
    /// collided with is in flight.
    @Test("an attempt upsert must not clobber a suppression recorded since it read")
    func attemptUpsertDoesNotClobberSuppressionCount() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1"))
        let stale = RediffDayZeroAttemptRecord(
            analysisAssetId: "a1", attemptCount: 1, lastAttemptAt: 1_000,
            lastExit: .noDivergentSlot, totalFullFetchBytes: 108_000_000
        )
        try await store.upsertRediffDayZeroAttempt(stale)

        // A concurrent suppression lands…
        try await store.noteRediffDayZeroSuppression(assetId: "a1", reason: .alreadyInFlight, at: 1_500)
        #expect(try await store.fetchRediffDayZeroAttempt(assetId: "a1")?.suppressedCount == 1)

        // …and the in-flight attempt now writes back a record folded from the
        // snapshot it read BEFORE the suppression (suppressedCount == 0).
        try await store.upsertRediffDayZeroAttempt(DayZeroRediffAttemptPolicy.advance(
            record: stale, assetId: "a1",
            outcome: RediffDayZeroMintOutcome(exit: .noAcceptedByteDiff, bSideCount: 2, bSidesGateRejected: 2),
            fullFetchBytes: 108_000_000, at: 2_000
        ))

        let read = try await store.fetchRediffDayZeroAttempt(assetId: "a1")
        #expect(read?.suppressedCount == 1, "the suppression survived the attempt's write-back")
        #expect(read?.lastSuppressedAt == 1_500)
        // …and the attempt itself still landed.
        #expect(read?.attemptCount == 2)
        #expect(read?.lastExit == .noAcceptedByteDiff)
    }

    /// REVIEW ROUND 1. `suppressionDoesNotConsumeBudget` proves the STORE
    /// primitive is safe, but nothing proved the RECORDER routes
    /// `.alreadyInFlight` to it rather than through the attempt fold. That
    /// routing is the whole reason a duplicate kickoff does not look like a
    /// third failed attempt.
    @Test("the RECORDER routes .alreadyInFlight to the suppression counter, not the attempt fold")
    func alreadyInFlightIsRoutedToSuppression() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1"))
        try await store.upsertRediffDayZeroAttempt(RediffDayZeroAttemptRecord(
            analysisAssetId: "a1", attemptCount: 1, lastAttemptAt: 1_000,
            lastExit: .noDivergentSlot, totalFullFetchBytes: 108_000_000
        ))
        let recorder = AnalysisStoreRediffRefetchRecorder(store: store, config: .production, now: { 2_000 })

        await recorder.recordOutcome(.dayZeroUnmarked(
            assetId: "a1", cost: .zero, mint: .blocked(.alreadyInFlight)
        ))

        let read = try await store.fetchRediffDayZeroAttempt(assetId: "a1")
        #expect(read?.suppressedCount == 1, "a duplicate kickoff is a SUPPRESSION")
        #expect(read?.lastSuppressedAt == 2_000)
        #expect(read?.attemptCount == 1, "…and must not consume the attempt budget")
        #expect(read?.lastExit == .noDivergentSlot,
                "…nor overwrite the exit of the attempt it collided with")
        #expect(read?.lastAttemptAt == 1_000)
    }

    /// REVIEW ROUND 1. `.assetRowMissing` is the one exit that structurally
    /// cannot be persisted: the table is FK'd to `analysis_assets` and there is
    /// no asset row by definition. Pinned so the taxonomy's documentation stays
    /// honest — the recorder must SWALLOW the failure (never propagate), leave
    /// no row, and cost nothing.
    @Test("recording .assetRowMissing for an unknown asset is swallowed, not persisted")
    func assetRowMissingCannotBePersisted() async throws {
        let store = try await makeTestStore()
        let recorder = AnalysisStoreRediffRefetchRecorder(store: store, config: .production, now: { 500 })

        await recorder.recordOutcome(.dayZeroUnmarked(
            assetId: "ghost-asset", cost: .zero, mint: .blocked(.assetRowMissing)
        ))

        #expect(try await store.fetchRediffDayZeroAttempt(assetId: "ghost-asset") == nil,
                "an FK'd per-asset table cannot hold a row for an asset that does not exist")
        // The free exit still must not inflate the byte-spend counter.
        let totals = try await store.fetchRediffBandwidthTotals()
        #expect(totals.dayZeroUnmarkedCount == 0)
        #expect(totals.fullFetchBytesTotal == 0)
    }

    /// REVIEW ROUND 1, END-TO-END. The pure-policy proof
    /// (`freeExitDoesNotConsumeTheAttemptBudget`) is only worth something if
    /// the recorder actually routes a free exit through it.
    @Test("a pre-fetch block recorded three times leaves the asset ELIGIBLE, not locked out")
    func repeatedFreeBlocksNeverExhaustTheBudget() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1"))
        let recorder = AnalysisStoreRediffRefetchRecorder(store: store, config: .production, now: { 500 })

        for _ in 0..<(DayZeroRediffAttemptPolicy.maxAttempts + 2) {
            await recorder.recordOutcome(.dayZeroUnmarked(
                assetId: "a1", cost: .zero, mint: .blocked(.aSideNotAnchored)
            ))
        }

        let read = try await store.fetchRediffDayZeroAttempt(assetId: "a1")
        #expect(read?.attemptCount == 0, "zero-byte declines are not attempts")
        #expect(read?.lastExit == .aSideNotAnchored, "…but the decline is still recorded")
        #expect(DayZeroRediffAttemptPolicy.decide(record: read, now: 9_999).isAttempt == true,
                "a streaming A-side that materializes later must still get its chance")
    }

    @Test("the policy generation round-trips and a foreign generation reopens the budget")
    func policyGenerationRoundTrips() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1"))
        try await store.upsertRediffDayZeroAttempt(RediffDayZeroAttemptRecord(
            analysisAssetId: "a1", attemptCount: DayZeroRediffAttemptPolicy.maxAttempts,
            lastAttemptAt: 1_000, lastExit: .noDivergentSlot
        ))
        let atHead = try await store.fetchRediffDayZeroAttempt(assetId: "a1")
        #expect(atHead?.policyGeneration == DayZeroRediffAttemptPolicy.currentGeneration)
        #expect(DayZeroRediffAttemptPolicy.decide(record: atHead, now: 9_999_999).isAttempt == false)

        // A row written by a build with different day-0 behavior.
        try await store.execForTesting(
            "UPDATE rediff_day_zero_attempts SET policyGeneration = 0 WHERE analysisAssetId = 'a1'"
        )
        let stale = try await store.fetchRediffDayZeroAttempt(assetId: "a1")
        #expect(stale?.policyGeneration == 0)
        #expect(DayZeroRediffAttemptPolicy.decide(record: stale, now: 9_999_999).isAttempt == true,
                "an exhausted budget from a different generation must not outlive it")
    }

    @Test("an unknown persisted exit decodes as RETRYABLE, never as a permanent park")
    func unknownExitIsConservative() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: "a1"))
        try await store.upsertRediffDayZeroAttempt(RediffDayZeroAttemptRecord(
            analysisAssetId: "a1", attemptCount: 1, lastAttemptAt: 1_000, lastExit: .marked
        ))
        // Simulate a FUTURE build's exit value landing in an older build's read.
        try await store.execForTesting(
            "UPDATE rediff_day_zero_attempts SET lastExit = 'some_future_exit' WHERE analysisAssetId = 'a1'"
        )

        let read = try await store.fetchRediffDayZeroAttempt(assetId: "a1")
        #expect(read?.lastExit.isRetryable == true,
                "an unreadable exit must back off, never permanently park an episode")
    }
}

// MARK: - V38 against a POPULATED pre-V38 database (review round 1)

/// The V38 tests the implementer shipped all run against a database that was
/// born at head. The owner's device is not: it carries a populated
/// `rediff_bandwidth_ledger` written by a V28..V37 build, with 299.6 MB already
/// in it. These reconstruct that exact shape — the ledger in its pre-V38 column
/// set, with real totals, and no `rediff_day_zero_attempts` table at all — and
/// prove the upgrade is additive, idempotent, and loses nothing.
@Suite("AnalysisStore — V38 day-0 attempts upgrade path (playhead-p70f)")
struct RediffDayZeroV38MigrationTests {

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id, episodeId: "ep-\(id)", assetFingerprint: "fp-\(id)",
            weakFingerprint: nil, sourceURL: "file:///tmp/\(id).mp3",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil, episodeDurationSec: 100
        )
    }

    /// Rewrites the on-disk schema back to its V37 shape THROUGH THE STORE'S OWN
    /// CONNECTION (no second sqlite handle racing the WAL): drop the V38 table
    /// outright and rebuild the ledger without `dayZeroUnmarkedCount`, carrying
    /// its rows across so the totals under test are genuinely pre-existing data.
    private func rewindToV37(_ store: AnalysisStore) async throws {
        try await store.execForTesting("DROP TABLE IF EXISTS rediff_day_zero_attempts")
        try await store.execForTesting("ALTER TABLE rediff_bandwidth_ledger RENAME TO rediff_bandwidth_ledger_v37tmp")
        try await store.execForTesting("""
            CREATE TABLE rediff_bandwidth_ledger (
                id                  INTEGER PRIMARY KEY CHECK (id = 1),
                precheckBytesTotal  INTEGER NOT NULL DEFAULT 0,
                fullFetchBytesTotal INTEGER NOT NULL DEFAULT 0,
                unchangedCount      INTEGER NOT NULL DEFAULT 0,
                rotatedCount        INTEGER NOT NULL DEFAULT 0,
                failedCount         INTEGER NOT NULL DEFAULT 0,
                parkedCount         INTEGER NOT NULL DEFAULT 0,
                lastUpdatedAt       REAL
            )
            """)
        try await store.execForTesting("""
            INSERT INTO rediff_bandwidth_ledger
            (id, precheckBytesTotal, fullFetchBytesTotal, unchangedCount,
             rotatedCount, failedCount, parkedCount, lastUpdatedAt)
            SELECT id, precheckBytesTotal, fullFetchBytesTotal, unchangedCount,
                   rotatedCount, failedCount, parkedCount, lastUpdatedAt
            FROM rediff_bandwidth_ledger_v37tmp
            """)
        try await store.execForTesting("DROP TABLE rediff_bandwidth_ledger_v37tmp")
        try await store.execForTesting("UPDATE _meta SET value = '37' WHERE key = 'schema_version'")
    }

    private func reopen(_ dir: URL) async throws -> AnalysisStore {
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        return store
    }

    @Test("THE UPGRADE: a populated V37 ledger reaches V38 with its bytes intact")
    func populatedV37DatabaseUpgrades() async throws {
        let (bootstrap, dir) = try await makeTestStoreWithDirectory()
        try await bootstrap.insertAsset(makeAsset(id: "a1"))
        // The device shape: ~300 MB spent, every outcome counter at zero.
        try await bootstrap.accumulateRediffBandwidth(
            precheckBytes: 0, fullFetchBytes: 299_600_000,
            unchangedCount: 0, rotatedCount: 0, failedCount: 0, parkedCount: 0,
            at: 1_700_000_000
        )
        try await rewindToV37(bootstrap)

        // Pre-migration state is genuinely pre-V38.
        #expect(!(try probeTableExists(in: dir, table: "rediff_day_zero_attempts")))
        #expect(!(try probeColumnExists(in: dir, table: "rediff_bandwidth_ledger", column: "dayZeroUnmarkedCount")))

        let upgraded = try await reopen(dir)

        #expect(try await upgraded.schemaVersion() == AnalysisStore.currentSchemaVersion)
        #expect(try probeTableExists(in: dir, table: "rediff_day_zero_attempts"))
        #expect(try probeColumnExists(in: dir, table: "rediff_bandwidth_ledger", column: "dayZeroUnmarkedCount"))
        #expect(try probeColumnExists(in: dir, table: "rediff_day_zero_attempts", column: "policyGeneration"))

        // NOTHING was lost, and the new counter starts honest.
        let totals = try await upgraded.fetchRediffBandwidthTotals()
        #expect(totals.fullFetchBytesTotal == 299_600_000, "the pre-existing ledger row survived the ALTER")
        #expect(totals.lastUpdatedAt == 1_700_000_000)
        #expect(totals.dayZeroUnmarkedCount == 0, "a backfilled column must not invent history")

        // …and both new writes work against the upgraded schema.
        try await upgraded.upsertRediffDayZeroAttempt(RediffDayZeroAttemptRecord(
            analysisAssetId: "a1", attemptCount: 1, lastAttemptAt: 2_000, lastExit: .noDivergentSlot
        ))
        #expect(try await upgraded.fetchRediffDayZeroAttempt(assetId: "a1")?.lastExit == .noDivergentSlot)
        try await upgraded.accumulateRediffBandwidth(
            precheckBytes: 0, fullFetchBytes: 108_000_000,
            unchangedCount: 0, rotatedCount: 0, failedCount: 0, parkedCount: 0,
            dayZeroUnmarkedCount: 1, at: 1_700_000_100
        )
        let after = try await upgraded.fetchRediffBandwidthTotals()
        #expect(after.fullFetchBytesTotal == 299_600_000 + 108_000_000, "the new write ACCUMULATES onto the old total")
        #expect(after.dayZeroUnmarkedCount == 1)
    }

    @Test("the V38 step is idempotent — re-running it neither throws nor disturbs data")
    func v38IsIdempotent() async throws {
        let (bootstrap, dir) = try await makeTestStoreWithDirectory()
        try await bootstrap.insertAsset(makeAsset(id: "a1"))
        try await bootstrap.accumulateRediffBandwidth(
            precheckBytes: 1_000, fullFetchBytes: 54_000_000,
            unchangedCount: 0, rotatedCount: 0, failedCount: 0, parkedCount: 0,
            dayZeroUnmarkedCount: 2, at: 900
        )
        try await bootstrap.upsertRediffDayZeroAttempt(RediffDayZeroAttemptRecord(
            analysisAssetId: "a1", attemptCount: 2, lastAttemptAt: 800,
            lastExit: .noAcceptedByteDiff, totalFullFetchBytes: 54_000_000
        ))

        // Reopen at HEAD (the migration short-circuits) …
        let head = try await reopen(dir)
        #expect(try await head.schemaVersion() == AnalysisStore.currentSchemaVersion)
        // … and again after a rewind that leaves the V38 objects in place, so
        // `CREATE TABLE IF NOT EXISTS` + `addColumnIfNeeded` re-run for real.
        try await head.execForTesting("UPDATE _meta SET value = '37' WHERE key = 'schema_version'")
        let rerun = try await reopen(dir)

        #expect(try await rerun.schemaVersion() == AnalysisStore.currentSchemaVersion)
        let totals = try await rerun.fetchRediffBandwidthTotals()
        #expect(totals.fullFetchBytesTotal == 54_000_000)
        #expect(totals.dayZeroUnmarkedCount == 2, "a re-run must not reset the counter")
        let row = try await rerun.fetchRediffDayZeroAttempt(assetId: "a1")
        #expect(row?.attemptCount == 2, "a re-run must not drop the attempt history")
        #expect(row?.lastExit == .noAcceptedByteDiff)
        #expect(row?.policyGeneration == DayZeroRediffAttemptPolicy.currentGeneration)
    }
}

// MARK: - Real bytes: the diagnosis the p70f trace could not make

/// The whole reason `RediffDayZeroExit` exists is that the trace ranked M9
/// ("diffs ran, produced no divergent slot") most likely but could not
/// DISTINGUISH it from the byte gate rejecting everything — which is what
/// non-MP3 bytes behind a normalized `.mp3` suffix would look like. These run
/// the REAL `AdDetectionService` mint over REAL bytes and prove the two are now
/// separate exits with separate counters.
@Suite("Rediff day-0 mint exits over real bytes (playhead-p70f)")
struct RediffDayZeroMintExitTests {

    private func makeService(store: AnalysisStore) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40, confirmationThreshold: 0.70,
                suppressionThreshold: 0.25, hotPathLookahead: 90.0,
                detectorVersion: "test-detection-v1", fmBackfillMode: .off,
                rediffSlotOwnershipEnabled: true
            ),
            rediffBSideProvider: nil
        )
    }

    private func insertAsset(store: AnalysisStore, assetId: String, sourceURL: String) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: assetId, episodeId: "ep-\(assetId)", assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil, sourceURL: sourceURL,
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil, episodeDurationSec: 280
        ))
    }

    /// 280 s of synthetic MP3 — the SAME construction the byte-first e2e suite
    /// uses for its A-side.
    private func writeMP3(to url: URL) throws {
        try SyntheticMP3.file(SyntheticMP3.frames(count: 10_719, seed: 0xC0FFEE)).write(to: url)
    }

    @Test("BYTE-IDENTICAL copies: diffs are ACCEPTED, union is empty → .noDivergentSlot")
    func identicalCopiesReportNoDivergentSlot() async throws {
        let dir = try makeTempDir(prefix: "RediffP70fNoDiverge")
        defer { try? FileManager.default.removeItem(at: dir) }
        let aURL = dir.appendingPathComponent("a.mp3")
        try writeMP3(to: aURL)
        let b0 = dir.appendingPathComponent("b0.mp3")
        let b1 = dir.appendingPathComponent("b1.mp3")
        try FileManager.default.copyItem(at: aURL, to: b0)
        try FileManager.default.copyItem(at: aURL, to: b1)

        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: aURL.absoluteString)
        let service = makeService(store: store)

        let outcome = await service.mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [b0, b1])

        #expect(outcome.exit == .noDivergentSlot)
        #expect(outcome.bSidesAccepted == 2, "the aligner DID get traction — the bytes really are MP3")
        #expect(outcome.bSidesGateRejected == 0)
        #expect(outcome.markCount == 0)
    }

    @Test("NON-MP3 bytes behind an .mp3 name: every diff gate-rejects → .noAcceptedByteDiff")
    func nonMP3BytesReportNoAcceptedByteDiff() async throws {
        let dir = try makeTempDir(prefix: "RediffP70fNoChain")
        defer { try? FileManager.default.removeItem(at: dir) }
        let aURL = dir.appendingPathComponent("a.mp3")
        try writeMP3(to: aURL)
        // The hypothesis under test: `DownloadManager` warns that a routing
        // suffix is normalized to `.mp3` and "the filename is not evidence that
        // the bytes are MP3". `RediffByteAligner` is an MP3-FRAME parser, so
        // non-MP3 bytes yield no chained runs at all.
        let notMP3 = Data((0..<200_000).map { UInt8(truncatingIfNeeded: $0 &* 7 &+ 13) })
        let b0 = dir.appendingPathComponent("b0.mp3")
        let b1 = dir.appendingPathComponent("b1.mp3")
        try notMP3.write(to: b0)
        try notMP3.write(to: b1)

        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: aURL.absoluteString)
        let service = makeService(store: store)

        let outcome = await service.mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [b0, b1])

        #expect(outcome.exit == .noAcceptedByteDiff,
                "the byte aligner found nothing to work with — NOT the same as 'the copies agreed'")
        #expect(outcome.bSidesGateRejected == 2)
        #expect(outcome.bSidesAccepted == 0)
        // The two diagnoses have DIFFERENT signatures. That is the deliverable.
        #expect(outcome.exit != RediffDayZeroExit.noDivergentSlot)
    }

    @Test("a MISSING B-copy counts as unreadable, not as a gate rejection")
    func missingBCopyCountsAsUnreadable() async throws {
        let dir = try makeTempDir(prefix: "RediffP70fUnreadable")
        defer { try? FileManager.default.removeItem(at: dir) }
        let aURL = dir.appendingPathComponent("a.mp3")
        try writeMP3(to: aURL)
        let b0 = dir.appendingPathComponent("b0.mp3")
        try FileManager.default.copyItem(at: aURL, to: b0)
        let missing = dir.appendingPathComponent("gone.mp3")

        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: aURL.absoluteString)
        let service = makeService(store: store)

        let outcome = await service.mintByteExactDayZeroMarks(analysisAssetId: "a1", bSideURLs: [b0, missing])

        #expect(outcome.bSidesUnreadable == 1)
        #expect(outcome.bSidesAccepted == 1)
        #expect(outcome.bSideCount == 2)
    }

    @Test("CHANGE 3: the pre-fetch blocker names the same exits the mint would, for FREE")
    func prefetchBlockerMatchesMintExits() async throws {
        let store = try await makeTestStore()
        let service = makeService(store: store)

        // No asset row at all.
        #expect(await service.dayZeroPrefetchBlocker(analysisAssetId: "ghost") == .assetRowMissing)

        // Asset row present, but `sourceURL` points at nothing on disk.
        try await insertAsset(store: store, assetId: "a1", sourceURL: "file:///tmp/definitely-not-here-p70f.mp3")
        #expect(await service.dayZeroPrefetchBlocker(analysisAssetId: "a1") == .aSideNotAnchored)

        // And the mint agrees — one implementation, so they cannot drift.
        let outcome = await service.mintByteExactDayZeroMarks(
            analysisAssetId: "a1",
            bSideURLs: [URL(fileURLWithPath: "/tmp/b0.mp3"), URL(fileURLWithPath: "/tmp/b1.mp3")]
        )
        #expect(outcome.exit == .aSideNotAnchored)
    }

    @Test("a viable A-side is NOT blocked — the pre-check does not suppress real work")
    func viableASideIsNotBlocked() async throws {
        let dir = try makeTempDir(prefix: "RediffP70fViable")
        defer { try? FileManager.default.removeItem(at: dir) }
        let aURL = dir.appendingPathComponent("a.mp3")
        try writeMP3(to: aURL)

        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: aURL.absoluteString)
        let service = makeService(store: store)

        #expect(await service.dayZeroPrefetchBlocker(analysisAssetId: "a1") == nil)
    }

    @Test("fewer than the collision-recovery floor of B-copies is its own exit")
    func tooFewBCopiesIsNamed() async throws {
        let store = try await makeTestStore()
        try await insertAsset(store: store, assetId: "a1", sourceURL: "file:///tmp/whatever.mp3")
        let service = makeService(store: store)

        let outcome = await service.mintByteExactDayZeroMarks(
            analysisAssetId: "a1", bSideURLs: [URL(fileURLWithPath: "/tmp/b0.mp3")]
        )
        #expect(outcome.exit == .tooFewBCopies)
        #expect(outcome.bSideCount == 1)
    }
}
