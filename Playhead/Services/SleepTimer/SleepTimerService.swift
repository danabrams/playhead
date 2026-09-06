//
//  SleepTimerService.swift
//  Playhead
//
//  playhead-g21 (plain v1): a standard sleep timer. Pick a duration, playback
//  fades out over two seconds and pauses when it elapses; "End of episode"
//  lets the episode finish and holds the queue's auto-advance. The
//  transcript-aware pause point (sentence boundary, ad-aware) is v2.
//

import Foundation

/// The durations the picker offers. Persisted by `rawName`, never by seconds,
/// so `.endOfEpisode` round-trips.
enum SleepDuration: String, CaseIterable, Sendable, Equatable {
    case fifteenMinutes
    case thirtyMinutes
    case fortyFiveMinutes
    case sixtyMinutes
    case endOfEpisode

    var rawName: String { rawValue }

    init?(rawName: String) {
        self.init(rawValue: rawName)
    }

    /// nil for `.endOfEpisode` — it has no clock, it has the episode's end.
    var seconds: TimeInterval? {
        switch self {
        case .fifteenMinutes: return 15 * 60
        case .thirtyMinutes: return 30 * 60
        case .fortyFiveMinutes: return 45 * 60
        case .sixtyMinutes: return 60 * 60
        case .endOfEpisode: return nil
        }
    }

    var label: String {
        switch self {
        case .fifteenMinutes: return "15 minutes"
        case .thirtyMinutes: return "30 minutes"
        case .fortyFiveMinutes: return "45 minutes"
        case .sixtyMinutes: return "1 hour"
        case .endOfEpisode: return "End of episode"
        }
    }
}

enum SleepTimerState: Sendable, Equatable {
    /// No timer set.
    case idle
    /// Counting down to `fireAt`.
    case running(fireAt: Date)
    /// Armed for the end of the current episode.
    case untilEndOfEpisode
    /// The countdown elapsed; the fade-out is in progress.
    case fadingOut
    /// Playback was paused by the timer. Cleared by the next arm/cancel.
    case paused

    var isArmed: Bool {
        switch self {
        case .running, .untilEndOfEpisode, .fadingOut: return true
        case .idle, .paused: return false
        }
    }
}

/// One timer per process. Injected sleeper and pauser so the rails run in
/// milliseconds against a recorder, not against `AVPlayer` and a wall clock.
actor SleepTimerService {
    typealias Sleeper = @Sendable (Duration) async throws -> Void
    typealias Pauser = @Sendable () async -> Void

    private let sleeper: Sleeper
    private let pauser: Pauser
    private let now: @Sendable () -> Date
    private(set) var state: SleepTimerState = .idle
    private var countdown: Task<Void, Never>?
    private var generation: UInt64 = 0
    private var continuations: [UUID: AsyncStream<SleepTimerState>.Continuation] = [:]

    init(
        pauser: @escaping Pauser,
        sleeper: @escaping Sleeper = { try await Task.sleep(for: $0) },
        now: @escaping @Sendable () -> Date = { Date() }
    ) {
        self.pauser = pauser
        self.sleeper = sleeper
        self.now = now
    }

    func observeStates() -> AsyncStream<SleepTimerState> {
        let id = UUID()
        let (stream, continuation) = AsyncStream<SleepTimerState>.makeStream()
        continuations[id] = continuation
        continuation.yield(state)
        continuation.onTermination = { [weak self] _ in
            Task { await self?.removeContinuation(id) }
        }
        return stream
    }

    private func removeContinuation(_ id: UUID) {
        continuations[id] = nil
    }

    private func transition(to newState: SleepTimerState) {
        state = newState
        for continuation in continuations.values {
            continuation.yield(newState)
        }
    }

    /// Arm (or re-arm) the timer. A running countdown is replaced.
    func arm(_ duration: SleepDuration) {
        countdown?.cancel()
        countdown = nil
        generation &+= 1
        let myGeneration = generation
        guard let seconds = duration.seconds else {
            transition(to: .untilEndOfEpisode)
            return
        }
        let fireAt = now().addingTimeInterval(seconds)
        transition(to: .running(fireAt: fireAt))
        let sleeper = self.sleeper
        countdown = Task { [weak self] in
            do {
                try await sleeper(.seconds(seconds))
            } catch {
                return // cancelled: whoever cancelled already moved the state
            }
            await self?.fire(generation: myGeneration)
        }
    }

    func cancel() {
        countdown?.cancel()
        countdown = nil
        generation &+= 1
        transition(to: .idle)
    }

    /// The countdown elapsed. Fade out, pause, and rest at `.paused`.
    private func fire(generation firedGeneration: UInt64) async {
        guard firedGeneration == generation, case .running = state else { return }
        transition(to: .fadingOut)
        await pauser()
        guard firedGeneration == generation else { return }
        transition(to: .paused)
    }

    /// Called by the queue's auto-advancer when the episode finishes. `true`
    /// consumes an end-of-episode hold: the advancer must NOT start the next
    /// entry. Anything else lets the queue proceed as usual.
    func consumeEndOfEpisodeHold() -> Bool {
        guard case .untilEndOfEpisode = state else { return false }
        generation &+= 1
        transition(to: .paused)
        return true
    }
}
