//
//  SleepTimerServiceTests.swift
//  PlayheadTests
//
//  playhead-g21: the plain sleep timer, driven through an injected sleeper and
//  pauser so every rail runs in milliseconds.
//

import Foundation
import Testing
@testable import Playhead

@Suite("SleepTimerService (playhead-g21)")
struct SleepTimerServiceTests {
    private actor Recorder {
        var pauses = 0
        var sleeps: [Duration] = []
        func pause() { pauses += 1 }
        func slept(_ d: Duration) { sleeps.append(d) }
    }

    /// A sleeper that returns only when released, so a test can hold the
    /// countdown open and cancel or re-arm underneath it.
    private actor Gate {
        private var continuations: [CheckedContinuation<Void, Error>] = []
        func wait() async throws {
            try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { continuations.append($0) }
            } onCancel: {
                Task { await self.cancelAll() }
            }
        }
        func release() { let c = continuations; continuations = []; c.forEach { $0.resume() } }
        func cancelAll() { let c = continuations; continuations = []; c.forEach { $0.resume(throwing: CancellationError()) } }
    }

    @Test("SleepDuration persists by case name and round-trips, including end of episode")
    func rawNameRoundTrip() {
        for duration in SleepDuration.allCases {
            #expect(SleepDuration(rawName: duration.rawName) == duration)
        }
        #expect(SleepDuration(rawName: "900") == nil)
        #expect(SleepDuration.endOfEpisode.seconds == nil)
        #expect(SleepDuration.fifteenMinutes.seconds == 900)
    }

    @Test("arming runs the countdown for the duration, then fades, pauses once, and rests at .paused")
    func armFiresAfterTheDuration() async throws {
        let recorder = Recorder()
        let timer = SleepTimerService(
            pauser: { await recorder.pause() },
            sleeper: { await recorder.slept($0) },
            now: { Date(timeIntervalSince1970: 1_000) }
        )
        await timer.arm(.thirtyMinutes)
        // The sleeper returns immediately, so the fire is a hop away.
        for _ in 0..<200 {
            if await timer.state == .paused { break }
            try await Task.sleep(for: .milliseconds(5))
        }
        #expect(await timer.state == .paused)
        #expect(await recorder.pauses == 1)
        #expect(await recorder.sleeps == [.seconds(1_800)])
    }

    @Test("the running state names when it fires")
    func runningStateCarriesFireAt() async {
        let gate = Gate()
        let timer = SleepTimerService(
            pauser: {},
            sleeper: { _ in try await gate.wait() },
            now: { Date(timeIntervalSince1970: 1_000) }
        )
        await timer.arm(.fifteenMinutes)
        #expect(await timer.state == .running(fireAt: Date(timeIntervalSince1970: 1_900)))
        await timer.cancel()
    }

    @Test("cancel mid-countdown returns to .idle and the pauser never runs")
    func cancelMidCountdown() async throws {
        let recorder = Recorder(); let gate = Gate()
        let timer = SleepTimerService(pauser: { await recorder.pause() }, sleeper: { _ in try await gate.wait() })
        await timer.arm(.sixtyMinutes)
        #expect(await timer.state.isArmed)
        await timer.cancel()
        #expect(await timer.state == .idle)
        await gate.release()
        try await Task.sleep(for: .milliseconds(30))
        #expect(await recorder.pauses == 0)
        #expect(await timer.state == .idle)
    }

    @Test("re-arming replaces the countdown: the first one's expiry fires nothing")
    func rearmReplacesTheCountdown() async throws {
        let recorder = Recorder(); let first = Gate(); let second = Gate()
        let turn = Recorder()
        let timer = SleepTimerService(pauser: { await recorder.pause() }, sleeper: { d in
            await turn.slept(d)
            if await turn.sleeps.count == 1 { try await first.wait() } else { try await second.wait() }
        })
        await timer.arm(.fifteenMinutes)
        await timer.arm(.thirtyMinutes)
        await first.release()          // the replaced countdown "expires"
        try await Task.sleep(for: .milliseconds(30))
        #expect(await recorder.pauses == 0)
        #expect(await timer.state.isArmed)
        await second.release()
        for _ in 0..<200 {
            if await timer.state == .paused { break }
            try await Task.sleep(for: .milliseconds(5))
        }
        #expect(await recorder.pauses == 1)
    }

    @Test("end of episode: the hold is consumed exactly once, and only while armed for it")
    func endOfEpisodeHold() async {
        let timer = SleepTimerService(pauser: {})
        #expect(await timer.consumeEndOfEpisodeHold() == false, "nothing armed: the queue proceeds")
        await timer.arm(.endOfEpisode)
        #expect(await timer.state == .untilEndOfEpisode)
        #expect(await timer.consumeEndOfEpisodeHold() == true)
        #expect(await timer.state == .paused)
        #expect(await timer.consumeEndOfEpisodeHold() == false, "consumed: the next finish advances")
    }

    @Test("a running countdown does not hold the queue")
    func runningCountdownDoesNotHoldTheQueue() async {
        let gate = Gate()
        let timer = SleepTimerService(pauser: {}, sleeper: { _ in try await gate.wait() })
        await timer.arm(.fifteenMinutes)
        #expect(await timer.consumeEndOfEpisodeHold() == false)
        await timer.cancel()
    }

    @Test("observers see every transition, starting from the current state")
    func observersSeeTransitions() async throws {
        let gate = Gate()
        let timer = SleepTimerService(pauser: {}, sleeper: { _ in try await gate.wait() }, now: { Date(timeIntervalSince1970: 0) })
        let stream = await timer.observeStates()
        var iterator = stream.makeAsyncIterator()
        #expect(await iterator.next() == .idle)
        await timer.arm(.fifteenMinutes)
        #expect(await iterator.next() == .running(fireAt: Date(timeIntervalSince1970: 900)))
        await timer.cancel()
        #expect(await iterator.next() == .idle)
    }

    @Test("countdown text: mm:ss under an hour, h:mm:ss past it, never negative")
    func countdownText() {
        let now = Date(timeIntervalSince1970: 0)
        #expect(SleepTimerButton.countdownText(until: now.addingTimeInterval(65), now: now) == "1:05")
        #expect(SleepTimerButton.countdownText(until: now.addingTimeInterval(3_661), now: now) == "1:01:01")
        #expect(SleepTimerButton.countdownText(until: now.addingTimeInterval(-5), now: now) == "0:00")
    }
}
