// PlaybackQueueController.swift
// Lazily-constructed bundle of the queue service, auto-advancer, and
// finish observer — one instance per app scene. The bundle exists so
// `PlayheadApp` can hold a single `@State` value rather than juggling
// five `@State` references and their construction order.
//
// The bundle exposes:
//   * `service` — the actor backing the queue.
//   * `start(modelContainer:notificationCenter:playHandler:countdown:)` —
//     wires up the advancer + observer once the runtime is available.
//     Idempotent; subsequent calls are no-ops.
//   * `stop()` — tears the observer down (e.g. on scene-phase background
//     when the runtime is shutting down).
//
// The controller is `@MainActor` because it reaches into a `ModelContainer`
// at start time. The advancer / observer themselves are independent of
// the main actor.

import Foundation
import Observation
import SwiftData

@MainActor
@Observable
final class PlaybackQueueController {

    private(set) var service: PlaybackQueueService?
    private var advancer: PlaybackQueueAutoAdvancer?
    private var observer: PlaybackQueueFinishObserver?
    private(set) var isStarted: Bool = false

    init() {}

    /// One-shot wiring. Subsequent calls are no-ops so the App's `.task`
    /// can call `start()` defensively without paying attention to
    /// scene-phase cycles.
    func start(
        modelContainer: ModelContainer,
        notificationCenter: NotificationCenter = .default,
        countdown: Duration = .seconds(3),
        playHandler: @escaping @Sendable (String) async -> Void
    ) {
        guard !isStarted else { return }
        let queueService = PlaybackQueueService(modelContainer: modelContainer)
        let advancer = PlaybackQueueAutoAdvancer(
            queue: queueService,
            countdown: countdown,
            playHandler: playHandler
        )
        let observer = PlaybackQueueFinishObserver(
            center: notificationCenter,
            advancer: advancer
        )
        observer.start()

        self.service = queueService
        self.advancer = advancer
        self.observer = observer
        self.isStarted = true
    }

    /// Cancel the in-flight auto-advance (e.g. from a "Cancel" button on
    /// a future countdown overlay). No-op when nothing is in flight.
    func cancelAdvance() async {
        await advancer?.cancel()
    }

    func stop() {
        observer?.stop()
        observer = nil
        isStarted = false
    }

    deinit {
        // The observer's own deinit cancels its task — no main-actor work
        // needed here.
    }
}
