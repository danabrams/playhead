// SurfaceStatusReadyTransitionEmitterTests.swift
// Exercises the emission contract of `SurfaceStatusReadyTransitionEmitter`:
// `ready_entered` fires exactly once per transition INTO a ready-for-
// playback disposition, and never on repeated ready reductions or on
// non-ready reductions.
//
// Scope: playhead-o45p (false_ready_rate instrumentation — Wave 4 pass
// criterion 3).

import Foundation
import Testing

@testable import Playhead

@Suite("SurfaceStatusReadyTransitionEmitter — ready_entered emission (playhead-o45p)")
struct SurfaceStatusReadyTransitionEmitterTests {

    // MARK: - Fixtures

    private static let eligible = AnalysisEligibility(
        hardwareSupported: true,
        appleIntelligenceEnabled: true,
        regionSupported: true,
        languageSupported: true,
        modelAvailableNow: true,
        capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
    )

    private static let queuedState = AnalysisState(
        persistedStatus: .queued,
        hasUserPreemptedJob: false,
        hasAppForceQuitFlag: false,
        pendingSinceEnqueuedAt: Date(timeIntervalSince1970: 1_700_000_000),
        hasAnyConfirmedAnalysis: false
    )

    /// Captures invocations of the logger sink for assertion.
    private final class Sink {
        struct Invocation: Equatable {
            let episodeIdHash: String?
            let trigger: SurfaceStateTransitionEntryTrigger?
        }
        private(set) var invocations: [Invocation] = []
        lazy var record: SurfaceStatusReadyTransitionEmitter.LoggerSink = { [weak self] hash, trigger in
            self?.invocations.append(Invocation(episodeIdHash: hash, trigger: trigger))
        }
    }

    // MARK: - Tests

    @Test("readyEntered fires on the first ready reduction (coldStart)")
    func readyEnteredFiresOnFirstReadyReduction() {
        let sink = Sink()
        let emitter = SurfaceStatusReadyTransitionEmitter(loggerSink: sink.record)

        _ = emitter.reduceAndEmit(
            episodeIdHash: "ep-1",
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )

        #expect(sink.invocations.count == 1)
        #expect(sink.invocations.first?.episodeIdHash == "ep-1")
        #expect(sink.invocations.first?.trigger == .coldStart)
    }

    @Test("readyEntered does NOT fire on repeated ready reductions (same episode)")
    func readyEnteredIsIdempotentOnRepeatedReady() {
        let sink = Sink()
        let emitter = SurfaceStatusReadyTransitionEmitter(loggerSink: sink.record)

        // First reduction: should emit.
        _ = emitter.reduceAndEmit(
            episodeIdHash: "ep-1",
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        // Second and third reductions on the same ready state: must NOT emit.
        _ = emitter.reduceAndEmit(
            episodeIdHash: "ep-1",
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        _ = emitter.reduceAndEmit(
            episodeIdHash: "ep-1",
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )

        #expect(sink.invocations.count == 1)
    }

    @Test("readyEntered fires again after a non-ready intermediate reduction (unblocked)")
    func readyEnteredRefiresAfterTransitioningOutAndBackIn() {
        let sink = Sink()
        let emitter = SurfaceStatusReadyTransitionEmitter(loggerSink: sink.record)

        // Reduction 1: ready (cold start).
        _ = emitter.reduceAndEmit(
            episodeIdHash: "ep-1",
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        // Reduction 2: not ready (thermal cause blocks).
        _ = emitter.reduceAndEmit(
            episodeIdHash: "ep-1",
            state: Self.queuedState,
            cause: .thermal,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        // Reduction 3: back to ready — must emit with trigger=unblocked.
        _ = emitter.reduceAndEmit(
            episodeIdHash: "ep-1",
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )

        #expect(sink.invocations.count == 2)
        #expect(sink.invocations[0].trigger == .coldStart)
        #expect(sink.invocations[1].trigger == .unblocked)
    }

    @Test("readyEntered does NOT fire when the reduction produces a non-ready disposition")
    func readyEnteredDoesNotFireOnNonReadyReduction() {
        let sink = Sink()
        let emitter = SurfaceStatusReadyTransitionEmitter(loggerSink: sink.record)

        // Thermal cause ⇒ .paused ⇒ not ready.
        _ = emitter.reduceAndEmit(
            episodeIdHash: "ep-1",
            state: Self.queuedState,
            cause: .thermal,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )

        #expect(sink.invocations.isEmpty)
    }

    @Test("caller-supplied trigger overrides the inferred one")
    func callerSuppliedTriggerOverridesInferred() {
        let sink = Sink()
        let emitter = SurfaceStatusReadyTransitionEmitter(loggerSink: sink.record)

        _ = emitter.reduceAndEmit(
            episodeIdHash: "ep-1",
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil,
            trigger: .analysisCompleted
        )

        #expect(sink.invocations.count == 1)
        #expect(sink.invocations.first?.trigger == .analysisCompleted)
    }

    // MARK: - playhead-2v1r — bounded LRU regressions

    /// Capacity literal mirrored from the production type. If the
    /// constant in `SurfaceStatusReadyTransitionEmitter` changes, this
    /// test will fail loudly at the eviction boundary.
    private static let lastReadyCapacityForTests = 128

    @Test("LRU evicts the oldest entries once capacity+N distinct hashes have been seen")
    func lruEvictsOldestPastCapacity() {
        let sink = Sink()
        let emitter = SurfaceStatusReadyTransitionEmitter(loggerSink: sink.record)
        let capacity = Self.lastReadyCapacityForTests

        // Seed `capacity + 5` distinct hashes with a ready reduction
        // each. Every reduction is a cold-start ⇒ each one emits.
        for index in 0..<(capacity + 5) {
            _ = emitter.reduceAndEmit(
                episodeIdHash: "ep-\(index)",
                state: Self.queuedState,
                cause: nil,
                eligibility: Self.eligible,
                coverage: nil,
                readinessAnchor: nil
            )
        }

        // Sanity: each unique hash on a ready reduction emitted once.
        #expect(sink.invocations.count == capacity + 5)
        let baselineCount = sink.invocations.count

        // Re-querying the 5 oldest hashes (ep-0 ... ep-4) on another
        // ready reduction must emit again — they were evicted, so the
        // emitter treats them as never-seen ⇒ coldStart trigger.
        for index in 0..<5 {
            _ = emitter.reduceAndEmit(
                episodeIdHash: "ep-\(index)",
                state: Self.queuedState,
                cause: nil,
                eligibility: Self.eligible,
                coverage: nil,
                readinessAnchor: nil
            )
        }
        let newInvocations = Array(sink.invocations.dropFirst(baselineCount))
        #expect(newInvocations.count == 5)
        for invocation in newInvocations {
            #expect(invocation.trigger == .coldStart)
        }
    }

    @Test("LRU continues to suppress emits for hashes still within the recency window")
    func lruSuppressesEmitForRecentHash() {
        let sink = Sink()
        let emitter = SurfaceStatusReadyTransitionEmitter(loggerSink: sink.record)
        let capacity = Self.lastReadyCapacityForTests

        // Insert ep-target on a ready reduction, then push exactly
        // `capacity - 1` more distinct hashes — ep-target is still the
        // oldest BUT remains within capacity.
        _ = emitter.reduceAndEmit(
            episodeIdHash: "ep-target",
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        for index in 0..<(capacity - 1) {
            _ = emitter.reduceAndEmit(
                episodeIdHash: "ep-other-\(index)",
                state: Self.queuedState,
                cause: nil,
                eligibility: Self.eligible,
                coverage: nil,
                readinessAnchor: nil
            )
        }
        let baselineCount = sink.invocations.count

        // ep-target must still be remembered as ready ⇒ another ready
        // reduction must NOT emit (idempotence preserved).
        _ = emitter.reduceAndEmit(
            episodeIdHash: "ep-target",
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(sink.invocations.count == baselineCount)
    }

    @Test("LRU promotes recency on touch — accessed hashes survive eviction")
    func lruMoveToFrontProtectsTouchedHash() {
        let sink = Sink()
        let emitter = SurfaceStatusReadyTransitionEmitter(loggerSink: sink.record)
        let capacity = Self.lastReadyCapacityForTests

        // Seed ep-touched as the oldest entry.
        _ = emitter.reduceAndEmit(
            episodeIdHash: "ep-touched",
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        // Fill up to capacity-1 more entries.
        for index in 0..<(capacity - 1) {
            _ = emitter.reduceAndEmit(
                episodeIdHash: "ep-filler-\(index)",
                state: Self.queuedState,
                cause: nil,
                eligibility: Self.eligible,
                coverage: nil,
                readinessAnchor: nil
            )
        }
        // Touch ep-touched again (idempotent ready ⇒ move-to-back, no emit).
        _ = emitter.reduceAndEmit(
            episodeIdHash: "ep-touched",
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        // Now insert 5 more hashes — these push out the OLDEST entries,
        // which are now ep-filler-0 ... ep-filler-4 (NOT ep-touched).
        for index in 0..<5 {
            _ = emitter.reduceAndEmit(
                episodeIdHash: "ep-late-\(index)",
                state: Self.queuedState,
                cause: nil,
                eligibility: Self.eligible,
                coverage: nil,
                readinessAnchor: nil
            )
        }
        let preProbeCount = sink.invocations.count

        // ep-touched survived: re-emit on ready must NOT fire.
        _ = emitter.reduceAndEmit(
            episodeIdHash: "ep-touched",
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(sink.invocations.count == preProbeCount)

        // ep-filler-0 was evicted: re-emit must fire as coldStart.
        let beforeEvicted = sink.invocations.count
        _ = emitter.reduceAndEmit(
            episodeIdHash: "ep-filler-0",
            state: Self.queuedState,
            cause: nil,
            eligibility: Self.eligible,
            coverage: nil,
            readinessAnchor: nil
        )
        #expect(sink.invocations.count == beforeEvicted + 1)
        #expect(sink.invocations.last?.trigger == .coldStart)
    }
}
