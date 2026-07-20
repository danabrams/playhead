// RediffRefetchFailurePolicyTests.swift
// playhead-xsdz.36: the xsdz.28 R2 failure-state persistence policy —
// transient vs deterministic classification, the same-class-twice
// confirmation, the 1d → 3d → 7d deterministic backoff, the 3-attempt park,
// and the service-level flow that advances + surfaces the state through
// `.failed` outcomes. Pure-policy tests need no network or device.

import Foundation
import Testing

@testable import Playhead

@Suite("RediffRefetch failure policy (playhead-xsdz.36 / xsdz.28 R2)")
struct RediffRefetchFailurePolicyTests {

    typealias Policy = RediffRefetchPolicy
    static let day = Policy.Configuration.secondsPerDay

    // MARK: - Classification

    @Test("network / cancellation / unknown pre-fetch errors are transient")
    func classifyTransient() {
        #expect(Policy.classifyFailure(URLError(.timedOut), stage: .precheck) == .transient)
        #expect(Policy.classifyFailure(URLError(.notConnectedToInternet), stage: .fetch) == .transient)
        #expect(Policy.classifyFailure(CancellationError(), stage: .postDownload) == .transient)
        #expect(Policy.classifyFailure(NSError(domain: "whatever", code: 1), stage: .precheck) == .transient)
        #expect(Policy.classifyFailure(NSError(domain: "whatever", code: 1), stage: .fetch) == .transient)
    }

    @Test("HTTP 404/410 is resource-gone (terminal); other statuses transient")
    func classifyHTTPStatuses() {
        #expect(Policy.classifyFailure(URLSessionFullEpisodeFetcher.FetchError.notOK(status: 404), stage: .fetch) == .resourceGone)
        #expect(Policy.classifyFailure(URLSessionFullEpisodeFetcher.FetchError.notOK(status: 410), stage: .fetch) == .resourceGone)
        #expect(Policy.classifyFailure(URLSessionFullEpisodeFetcher.FetchError.notOK(status: 500), stage: .fetch) == .transient)
        #expect(Policy.classifyFailure(URLSessionRangedAudioSampler.SampleError.notPartialContent(status: 404), stage: .precheck) == .resourceGone)
        #expect(Policy.classifyFailure(URLSessionRangedAudioSampler.SampleError.notPartialContent(status: 410), stage: .precheck) == .resourceGone)
        #expect(Policy.classifyFailure(URLSessionRangedAudioSampler.SampleError.notPartialContent(status: 200), stage: .precheck) == .transient)
    }

    @Test("unknown post-download errors are decode-class; empty stream is fingerprint-mismatch; consume errors carry their own class")
    func classifyPostDownloadAndConformance() {
        #expect(Policy.classifyFailure(NSError(domain: "decode", code: 7), stage: .postDownload) == .decodeFailure)
        #expect(Policy.classifyFailure(RediffBSideEmptyStreamError(), stage: .postDownload) == .fingerprintMismatch)
        #expect(Policy.classifyFailure(RediffBSideConsumeError.assetMissing(assetId: "a"), stage: .postDownload) == .staleAsset)
        #expect(Policy.classifyFailure(RediffBSideConsumeError.episodeDurationUnknown(assetId: "a"), stage: .postDownload) == .staleAsset)
        #expect(Policy.classifyFailure(RediffBSideConsumeError.storeUnavailable("x"), stage: .postDownload) == .transient)
    }

    // MARK: - advanceFailed transitions

    @Test("transient failure stamps the attempt but resets the streak")
    func advanceFailedTransient() {
        let s0 = Policy.AttemptState(unchangedAttempts: 1, lastAttemptAt: 10, resolved: false,
                                     lastFailureClass: .decodeFailure, sameClassFailureStreak: 1)
        let s1 = Policy.advanceFailed(s0, failureClass: .transient, at: 42)
        #expect(s1.unchangedAttempts == 1)
        #expect(s1.lastAttemptAt == 42)
        #expect(s1.lastFailureClass == .transient)
        #expect(s1.sameClassFailureStreak == 0, "transient breaks the consecutive terminal chain")
    }

    @Test("same terminal class increments the streak; a different terminal class restarts it")
    func advanceFailedStreaks() {
        var s = Policy.AttemptState.initial
        s = Policy.advanceFailed(s, failureClass: .decodeFailure, at: 1)
        #expect(s.sameClassFailureStreak == 1)
        s = Policy.advanceFailed(s, failureClass: .decodeFailure, at: 2)
        #expect(s.sameClassFailureStreak == 2)
        s = Policy.advanceFailed(s, failureClass: .resourceGone, at: 3)
        #expect(s.sameClassFailureStreak == 1, "class switch restarts the consecutive count")
        #expect(s.lastFailureClass == .resourceGone)
    }

    @Test("successful attempts (unchanged / resolved) clear the failure fields")
    func successResetsFailureFields() {
        let failed = Policy.advanceFailed(.initial, failureClass: .decodeFailure, at: 5)
        let unchanged = Policy.advanceUnchanged(failed, at: 6)
        #expect(unchanged.lastFailureClass == nil)
        #expect(unchanged.sameClassFailureStreak == 0)
        let resolved = Policy.markResolved(failed, at: 7)
        #expect(resolved.lastFailureClass == nil)
        #expect(resolved.sameClassFailureStreak == 0)
    }

    // MARK: - Eligibility under failures

    @Test("a transient failure retries at the very next sweep — no backoff")
    func transientFailureEligibleNextSweep() {
        let s = Policy.advanceFailed(.initial, failureClass: .transient, at: 1_000_000)
        // One second later (i.e. "the next sweep") the episode is due again.
        #expect(Policy.eligibility(now: 1_000_001, downloadedAt: 0, state: s) == .eligible)
    }

    @Test("a single terminal-class failure also retries next sweep (not yet confirmed deterministic)")
    func singleTerminalFailureEligibleNextSweep() {
        let s = Policy.advanceFailed(.initial, failureClass: .decodeFailure, at: 1_000_000)
        #expect(Policy.eligibility(now: 1_000_001, downloadedAt: 0, state: s) == .eligible)
    }

    @Test("a transient failure after an unchanged attempt does NOT inherit the unchanged backoff")
    func transientAfterUnchangedIsNotBackoffGated() {
        var s = Policy.advanceUnchanged(.initial, at: 1_000_000)   // unchanged → 1d backoff normally
        s = Policy.advanceFailed(s, failureClass: .transient, at: 1_000_100)
        #expect(Policy.eligibility(now: 1_000_101, downloadedAt: 0, state: s) == .eligible)
    }

    @Test("second consecutive same-class failure confirms deterministic → 1d backoff")
    func deterministicConfirmationBacksOff1d() {
        var s = Policy.advanceFailed(.initial, failureClass: .decodeFailure, at: 0)
        s = Policy.advanceFailed(s, failureClass: .decodeFailure, at: 1_000_000)
        // streak == 2 → backoff index 0 → 1 day from the last attempt.
        let early = Policy.eligibility(now: 1_000_000 + Self.day - 1, downloadedAt: 0, state: s)
        guard case let .deterministicBackoffNotElapsed(next) = early else {
            Issue.record("expected deterministicBackoffNotElapsed, got \(early)"); return
        }
        #expect(abs(next - (1_000_000 + Self.day)) < 0.001)
        #expect(Policy.eligibility(now: 1_000_000 + Self.day, downloadedAt: 0, state: s) == .eligible)
    }

    @Test("deterministic backoff escalates 1d → 3d → 7d and parks after 3 deterministic attempts")
    func deterministicEscalationAndPark() {
        // F1, F2 confirm; retries R1 (+1d), R2 (+3d), R3 (+7d) all fail → park.
        var s = Policy.advanceFailed(.initial, failureClass: .resourceGone, at: 0)
        s = Policy.advanceFailed(s, failureClass: .resourceGone, at: 100)      // streak 2 → confirmed
        s = Policy.advanceFailed(s, failureClass: .resourceGone, at: 200)      // streak 3 (R1 failed)
        #expect(Policy.eligibility(now: 200 + 3 * Self.day - 1, downloadedAt: 0, state: s)
            == .deterministicBackoffNotElapsed(nextEligibleAt: 200 + 3 * Self.day))
        #expect(Policy.eligibility(now: 200 + 3 * Self.day, downloadedAt: 0, state: s) == .eligible)

        s = Policy.advanceFailed(s, failureClass: .resourceGone, at: 300)      // streak 4 (R2 failed)
        #expect(Policy.eligibility(now: 300 + 7 * Self.day - 1, downloadedAt: 0, state: s)
            == .deterministicBackoffNotElapsed(nextEligibleAt: 300 + 7 * Self.day))
        #expect(Policy.eligibility(now: 300 + 7 * Self.day, downloadedAt: 0, state: s) == .eligible)

        s = Policy.advanceFailed(s, failureClass: .resourceGone, at: 400)      // streak 5 (R3 failed) → PARK
        #expect(Policy.isParked(s))
        #expect(Policy.eligibility(now: 400 + 1_000 * Self.day, downloadedAt: 0, state: s)
            == .parkedDeterministicFailure, "parked is terminal at any later time")
    }

    @Test("a transient failure between terminal failures breaks the confirmation chain")
    func transientBreaksConfirmation() {
        var s = Policy.advanceFailed(.initial, failureClass: .decodeFailure, at: 0)
        s = Policy.advanceFailed(s, failureClass: .transient, at: 100)
        s = Policy.advanceFailed(s, failureClass: .decodeFailure, at: 200)
        #expect(s.sameClassFailureStreak == 1, "not consecutive → back to 1")
        #expect(Policy.eligibility(now: 201, downloadedAt: 0, state: s) == .eligible)
    }

    @Test("legacy states (no failure fields) behave exactly as before")
    func legacyStatesUnchanged() {
        // Mirrors the pre-xsdz.36 eligibility pins with defaulted new fields.
        let now = 9_000_000.0
        #expect(Policy.eligibility(now: now, downloadedAt: now - Self.day, state: .initial) == .eligible)
        let unchanged1 = Policy.AttemptState(unchangedAttempts: 1, lastAttemptAt: now - Self.day, resolved: false)
        #expect(Policy.eligibility(now: now, downloadedAt: 0, state: unchanged1) == .eligible)
        let resolved = Policy.AttemptState(unchangedAttempts: 0, lastAttemptAt: now, resolved: true)
        #expect(Policy.eligibility(now: now + Self.day, downloadedAt: 0, state: resolved) == .alreadyResolved)
    }

    // MARK: - Production preset (xsdz.30)

    @Test("production config waits ~3 days before the first attempt; everything else matches default")
    func productionPresetIs3Days() {
        let production = Policy.Configuration.production
        #expect(production.minimumAgeBeforeFirstAttempt == 3 * Self.day)
        #expect(production.backoffSchedule == Policy.Configuration.default.backoffSchedule)
        #expect(production.maxUnchangedAttempts == Policy.Configuration.default.maxUnchangedAttempts)
        #expect(production.deterministicFailureBackoffSchedule == [Self.day, 3 * Self.day, 7 * Self.day])
        #expect(production.deterministicConfirmationCount == 2)
        #expect(production.maxDeterministicAttempts == 3)

        // The 3d gate end-to-end: 2.9d-old episode not yet due, 3d-old due.
        let now = 10_000_000.0
        #expect(Policy.eligibility(now: now, downloadedAt: now - (2.9 * Self.day), state: .initial, config: production)
            != .eligible)
        #expect(Policy.eligibility(now: now, downloadedAt: now - (3 * Self.day), state: .initial, config: production)
            == .eligible)
    }

    // MARK: - Service-level failure flow

    @Test("a deterministic full-fetch failure advances persisted state sweep-over-sweep and parks; parked sweeps never touch the network")
    func serviceFlowConfirmsBacksOffAndParks() async {
        // Sweep N: drive the SAME candidate through repeated 404 full-fetch
        // failures, feeding each sweep the state the recorder captured from
        // the previous one — exactly what the store-backed enumerator does.
        var state = RediffRefetchPolicy.AttemptState.initial
        var clock = 100 * Self.day

        func runOnce(_ state: RediffRefetchPolicy.AttemptState, at now: Double) async -> (RediffRefetchPolicy.Outcome?, sampled: Bool) {
            let sampler = StubRangedSampler()
            sampler.defaultSample = RemoteAudioSample(
                fingerprint: RediffRefetchPolicy.sampleFingerprint(head: Data("f".utf8), tail: Data("f".utf8), totalLength: 2),
                bytesTransferred: 131_072
            )
            let local = StubLocalSampler()
            local.defaultFingerprint = RediffRefetchPolicy.sampleFingerprint(head: Data("p".utf8), tail: Data("p".utf8), totalLength: 1)
            let full = StubFullFetcher()
            full.errorToThrow = URLSessionFullEpisodeFetcher.FetchError.notOK(status: 404)
            let recorder = SpyRefetchRecorder()
            let enumerator = StubRefetchEnumerator()
            enumerator.candidatesToReturn = [RediffRefetchCandidate(
                assetId: "asset-park",
                enclosureURL: URL(string: "https://cdn.example.com/park.mp3")!,
                downloadedAt: 0,
                localAudioURL: URL(fileURLWithPath: "/tmp/park.mp3"),
                attemptState: state
            )]
            let service = RediffRefetchService(
                enabled: true,
                enumerator: enumerator,
                rangedSampler: sampler,
                localSampler: local,
                fullFetcher: full,
                bsideFingerprinter: StubBSideFingerprinter(),
                recorder: recorder,
                fileRemover: SpyTempFileRemover(),
                taskScheduler: StubTaskScheduler(),
                now: { now }
            )
            await service.runRefetchSweep()
            return (recorder.outcomes.first, sampled: !sampler.calls.isEmpty)
        }

        // F1 + F2: back-to-back sweeps, both fail 404 → confirmed.
        for expectedStreak in 1...2 {
            let (outcome, sampled) = await runOnce(state, at: clock)
            #expect(sampled)
            guard case let .failed(_, _, cls, newState, _) = outcome else {
                Issue.record("expected .failed, got \(String(describing: outcome))"); return
            }
            #expect(cls == .resourceGone)
            #expect(newState.sameClassFailureStreak == expectedStreak)
            state = newState
            clock += 60  // next sweep, a minute later
        }

        // Immediately after confirmation the 1d backoff gates the retry.
        do {
            let (outcome, sampled) = await runOnce(state, at: clock)
            #expect(!sampled, "backoff-gated candidate must not touch the network")
            guard case let .skippedIneligible(_, reason) = outcome else {
                Issue.record("expected .skippedIneligible, got \(String(describing: outcome))"); return
            }
            if case .deterministicBackoffNotElapsed = reason {} else {
                Issue.record("expected deterministicBackoffNotElapsed, got \(reason)")
            }
        }

        // R1 (+1d), R2 (+3d), R3 (+7d): each fails → streak 3, 4, 5.
        let waits: [Double] = [Self.day, 3 * Self.day, 7 * Self.day]
        for (i, wait) in waits.enumerated() {
            clock = (state.lastAttemptAt ?? clock) + wait
            let (outcome, _) = await runOnce(state, at: clock)
            guard case let .failed(_, _, _, newState, _) = outcome else {
                Issue.record("expected .failed, got \(String(describing: outcome))"); return
            }
            #expect(newState.sameClassFailureStreak == 3 + i)
            state = newState
        }

        // Parked: any later sweep skips without sampling.
        #expect(RediffRefetchPolicy.isParked(state))
        clock += 1_000 * Self.day
        let (outcome, sampled) = await runOnce(state, at: clock)
        #expect(!sampled, "parked candidate must never touch the network again")
        guard case let .skippedIneligible(_, reason) = outcome else {
            Issue.record("expected .skippedIneligible, got \(String(describing: outcome))"); return
        }
        #expect(reason == .parkedDeterministicFailure)
    }
}
