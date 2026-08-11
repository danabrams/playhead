// RediffDayZeroKickoffCoordinator.swift
// playhead-4dqe: the ONE place a download becomes a day-0 attempt.
//
// Dan, 2026-08-01: "yes rediff on background" — day-0 must run for plain and
// auto downloads, not only for the explicit "Download & Analyze" tap. And:
// "detection and analysis also beeds to be timely. the app must feel almost
// like magic." Magic means the marks exist BEFORE he presses play, which means
// the work has to start when the bytes land, not 19 seconds into the first
// listen.
//
// WHY A COORDINATOR AND NOT ANOTHER `Task.detached`. Before this bead there was
// exactly one download-time entry point (the tap), and it was a detached task
// that waited, fired, and — if the wait expired — returned. Adding a second
// entry point (every completed background download) to that shape would have
// multiplied three latent problems instead of fixing them:
//
//   1. NO SERIALIZATION. A subscription batch can land five downloads inside a
//      minute. Five detached tasks would run five ~130 MB k-way fetches
//      concurrently, each reading a daily byte budget none of the others had
//      written to yet — every one of them admitted, the cap exceeded by 5x. The
//      drain here is strictly serial, so the budget a fetch reads already
//      includes every fetch that preceded it.
//   2. NO ORDERING. Dan's decision says newest-episode-first when the budget is
//      contended. Detached tasks have no order at all.
//   3. NO ACCOUNTABILITY. The give-up was a bare `return`. Here every settled
//      kickoff produces a record, and the two give-up causes are counted
//      separately and surfaced as distinct invariant codes.
//
// The coordinator owns none of the policy. It waits, hands off, and records;
// the transport gate, the per-asset backoff and the byte budget all live in
// `DayZeroRediffTrigger`, which the `fire` closure calls.

import Foundation

/// Serial, newest-first drain of download-time day-0 kickoffs.
///
/// An `actor` because the pending set, the in-flight guard and the per-cause
/// counters are all mutated from arbitrary background contexts (URLSession
/// delegate callbacks, the preparation tap, the runtime's MainActor) and the
/// serialization guarantee is the whole point.
actor RediffDayZeroKickoffCoordinator {

    // MARK: Seams

    private let maxAttempts: Int
    private let pollNanos: UInt64
    /// Probes the two preconditions for an episode.
    private let probe: @Sendable (String) async -> DayZeroReadinessProbe<DayZeroKickoffReady>
    /// Hands a ready kickoff to `DayZeroRediffTrigger.triggerIfEligible`.
    private let fire: @Sendable (DayZeroKickoffReady, RediffDayZeroKickoffRequest) async -> Void
    /// playhead-kg8h: durably CLAIMS one requested kickoff BEFORE it is queued
    /// (production: `AnalysisStore.noteRediffDayZeroKickoffClaim`).
    ///
    /// This is the difference between "every downloaded episode gets a row" and
    /// "every downloaded episode whose process survived the next 6.5 minutes
    /// plus a 66 MB re-fetch gets a row". See `.requested`.
    private let claimKickoff: @Sendable (RediffDayZeroKickoffClaim) async -> Void
    /// Persists one settled kickoff (production: `rediff_day_zero_kickoffs`).
    private let recordKickoff: @Sendable (RediffDayZeroKickoffRecordUpdate) async -> Void
    /// Surfaces a give-up on the JSON Lines session file the device pull reads.
    private let reportViolation: @Sendable (InvariantViolation.Code, String) async -> Void
    /// Hashes the episode id before it reaches the violation stream — the same
    /// discipline every other producer on that stream follows.
    private let episodeIdHasher: @Sendable (String) -> String
    private let sleep: @Sendable (UInt64) async -> Void
    private let now: @Sendable () -> Double

    // MARK: State

    private var pending: [RediffDayZeroKickoffRequest] = []
    /// Episodes queued or currently draining — the dedupe guard.
    private var inFlight: Set<String> = []
    /// Episodes whose kickoff already FIRED in this process.
    ///
    /// Deliberately NOT extended to give-ups. A kickoff that fired has handed
    /// the episode to the trigger, whose own per-asset backoff owns every
    /// further decision; re-running the wait for it would be pure waste. A
    /// kickoff that GAVE UP has produced nothing, so a later trigger (a retry
    /// download, the tap after a background attempt) is allowed to try again —
    /// and it must be, or one early failure while dispatch is still warming up
    /// would lock the episode out for the whole session.
    private var fired: Set<String> = []
    private var giveUps: [RediffDayZeroKickoffOutcome: Int] = [:]
    private var drainTask: Task<Void, Never>?
    private var drainSuspended = false

    init(
        maxAttempts: Int,
        pollNanos: UInt64,
        probe: @escaping @Sendable (String) async -> DayZeroReadinessProbe<DayZeroKickoffReady>,
        fire: @escaping @Sendable (DayZeroKickoffReady, RediffDayZeroKickoffRequest) async -> Void,
        // DELIBERATELY NOT DEFAULTED (playhead-kg8h), the same discipline
        // `DayZeroRediffTrigger`'s recorders are held to. A `{ _ in }` default
        // reproduces this bead's defect exactly — every kickoff the process does
        // not survive leaves nothing — and it would do so with the whole suite
        // green, because a missing claim is only visible as an absence. A
        // required parameter makes dropping the wiring a compile error.
        claimKickoff: @escaping @Sendable (RediffDayZeroKickoffClaim) async -> Void,
        recordKickoff: @escaping @Sendable (RediffDayZeroKickoffRecordUpdate) async -> Void,
        reportViolation: @escaping @Sendable (InvariantViolation.Code, String) async -> Void,
        episodeIdHasher: @escaping @Sendable (String) -> String,
        sleep: @escaping @Sendable (UInt64) async -> Void = {
            try? await Task.sleep(nanoseconds: $0)
        },
        now: @escaping @Sendable () -> Double = { Date().timeIntervalSince1970 }
    ) {
        self.maxAttempts = maxAttempts
        self.pollNanos = pollNanos
        self.probe = probe
        self.fire = fire
        self.claimKickoff = claimKickoff
        self.recordKickoff = recordKickoff
        self.reportViolation = reportViolation
        self.episodeIdHasher = episodeIdHasher
        self.sleep = sleep
        self.now = now
    }

    // MARK: Entry point

    /// Ask for a day-0 kickoff for an episode whose download just completed (or
    /// whose "Download & Analyze" was just tapped). Non-blocking in the sense
    /// that matters — it never waits for the readiness poll or the re-fetch —
    /// but it DOES await one bounded store write before returning: the durable
    /// claim.
    ///
    /// playhead-kg8h — WHY THE CLAIM IS AWAITED HERE AND NOT INSIDE THE DRAIN.
    /// Three losses sit between "a download completed" and "a row exists", and
    /// only the first is covered by claiming at the head of `process`:
    ///
    ///   1. the ~6.5-minute readiness wait,
    ///   2. `fire`, which is the entire day-0 re-fetch (~66 MB k-way, plus a
    ///      possible 30 s same-session retry and a second fetch),
    ///   3. the PENDING QUEUE itself — the drain is strictly serial
    ///      (playhead-kxgh measured five requests taking 33 minutes), and
    ///      `pending` is in-memory (playhead-jra6), so a batch of five leaves
    ///      four requests holding nothing durable for half an hour.
    ///
    /// Claiming before `pending.append` covers all three. The dedupe guards and
    /// the `inFlight` insert run BEFORE the await, so a concurrent duplicate is
    /// still rejected while the claim is in flight; the append and the drain
    /// start AFTER it, so `settle` can never overtake the claim it updates.
    ///
    /// Both callers already invoke this from a detached `Task`, so no download
    /// completion callback and no playback path waits on the write.
    func requestKickoff(_ request: RediffDayZeroKickoffRequest) async {
        guard !fired.contains(request.episodeId) else { return }
        guard !inFlight.contains(request.episodeId) else { return }
        inFlight.insert(request.episodeId)
        await claimKickoff(RediffDayZeroKickoffClaim(
            episodeId: request.episodeId,
            source: request.source,
            at: now()
        ))
        pending.append(request)
        startDrainIfNeeded()
    }

    /// Per-cause give-up counts. `.fired` is not a give-up and always reads 0 —
    /// stated rather than left implicit, because a single "failures" total is
    /// exactly the conflation this bead exists to remove.
    func giveUpCount(_ outcome: RediffDayZeroKickoffOutcome) -> Int {
        giveUps[outcome] ?? 0
    }

    // MARK: Drain

    private func startDrainIfNeeded() {
        guard !drainSuspended, drainTask == nil, !pending.isEmpty else { return }
        drainTask = Task { [weak self] in
            await self?.runDrain()
        }
    }

    private func runDrain() async {
        while !drainSuspended, let request = takeNext() {
            await process(request)
        }
        drainTask = nil
        // Work can arrive while the last `process` is suspended; the loop above
        // has already exited by then, so kick a fresh drain rather than leaving
        // it stranded until the next request.
        startDrainIfNeeded()
    }

    /// Pop the highest-priority pending request — newest episode first.
    private func takeNext() -> RediffDayZeroKickoffRequest? {
        guard !pending.isEmpty else { return nil }
        pending = RediffDayZeroKickoffOrdering.drainOrder(pending)
        return pending.removeFirst()
    }

    private func process(_ request: RediffDayZeroKickoffRequest) async {
        let startedAt = now()
        let episodeId = request.episodeId
        let outcome = await DayZeroReadinessWait.run(
            maxAttempts: maxAttempts,
            pollNanos: pollNanos,
            sleep: sleep,
            probe: { await self.probe(episodeId) }
        )
        if let ready = outcome.ready {
            await fire(ready, request)
        }
        await settle(request, outcome: outcome, startedAt: startedAt)
    }

    /// Count it, record it, surface it — the three things the bare `return`
    /// did none of.
    private func settle(
        _ request: RediffDayZeroKickoffRequest,
        outcome: DayZeroReadinessOutcome<DayZeroKickoffReady>,
        startedAt: Double
    ) async {
        let settledAt = now()
        inFlight.remove(request.episodeId)
        if outcome.outcome == .fired {
            fired.insert(request.episodeId)
        } else {
            giveUps[outcome.outcome, default: 0] += 1
        }

        await recordKickoff(RediffDayZeroKickoffRecordUpdate(
            episodeId: request.episodeId,
            source: request.source,
            outcome: outcome.outcome,
            pollCount: outcome.pollCount,
            waitedSeconds: max(0, settledAt - startedAt),
            at: settledAt
        ))

        guard let code = outcome.outcome.invariantCode else { return }
        await reportViolation(
            code,
            """
            day-0 kickoff gave up as \(outcome.outcome.rawValue) after \
            \(outcome.pollCount) polls from \(request.source.rawValue) \
            (episode \(episodeIdHasher(request.episodeId)))
            """
        )
    }

    // MARK: Test seams

    /// Run the queue to completion. Test-only: production never waits on the
    /// drain — the whole point is that a download completion returns instantly.
    func drainForTesting() async {
        while true {
            startDrainIfNeeded()
            guard let task = drainTask else { return }
            await task.value
            if pending.isEmpty { return }
        }
    }

    /// Hold the drain so a test can build a CONTENDED batch before any of it
    /// runs — otherwise the first request drains before the second is enqueued
    /// and there is no ordering to observe.
    func suspendDrainForTesting() {
        drainSuspended = true
    }

    func resumeDrainForTesting() {
        drainSuspended = false
        startDrainIfNeeded()
    }
}
