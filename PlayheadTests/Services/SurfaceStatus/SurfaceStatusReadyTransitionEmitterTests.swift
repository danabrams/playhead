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
}
