// Q45fReplayGateTests.swift
// playhead-q45f.2: TDD pin for the replay-side q45f counterfactual gate.
//
// The production trust-mode demotion state machine
// (TrustScoringService.recordWeakFalseSkipSignal) mutates a real
// PodcastProfile and runs `evaluateDemotion` on each rewind. The replay
// gate mirrors that math without touching storage so the NARL eval
// harness can answer counterfactuals like "given these listenRewindEvents
// from a fixture, when would trust have flipped this show out of auto?"
//
// These tests pin the contract so any future production retune surfaces
// here as a failing replay test, not as a silent eval drift. The parity
// suite below ties the gate directly to a real `TrustScoringService`
// driven through `recordWeakFalseSkipSignal`, which is the structural
// guarantee — pure-math tests pin intent, the parity test pins coupling.

import Foundation
import Testing
@testable import Playhead

@Suite("Q45fReplayGate")
struct Q45fReplayGateTests {

    private static let pod = "pod-q45f-test"
    private static let cfg = TrustScoringConfig.default

    private static func event(_ time: Double, podcastId: String = pod, windowId: String = "win-A") -> FrozenTrace.FrozenListenRewindEvent {
        FrozenTrace.FrozenListenRewindEvent(time: time, windowId: windowId, podcastId: podcastId)
    }

    private static func autoState(trust: Double = 0.90, falseSignals: Int = 0) -> Q45fReplayGate.State {
        Q45fReplayGate.State(trustScore: trust, recentFalseSkipSignals: falseSignals, mode: .auto)
    }

    @Test("Empty events → no demotions, final state == initial state")
    func emptyEvents() {
        let initial = Self.autoState()
        let result = Q45fReplayGate.replay(initialState: initial, events: [], config: Self.cfg)
        #expect(result.demotions.isEmpty)
        #expect(result.finalState == initial)
    }

    @Test("One rewind on auto → trust drops by weakFalseSignalPenalty, mode unchanged")
    func oneRewindNoDemotion() {
        let initial = Self.autoState()
        let result = Q45fReplayGate.replay(
            initialState: initial,
            events: [Self.event(60.0)],
            config: Self.cfg
        )
        #expect(result.demotions.isEmpty)
        #expect(result.finalState.mode == .auto)
        #expect(abs(result.finalState.trustScore - 0.85) < 1e-9)
        #expect(result.finalState.recentFalseSkipSignals == 1)
    }

    @Test("Two consecutive rewinds on auto → demotion to manual at second event time")
    func twoRewindsAutoDemotesToManual() {
        let initial = Self.autoState()
        let result = Q45fReplayGate.replay(
            initialState: initial,
            events: [Self.event(60.0), Self.event(120.0)],
            config: Self.cfg
        )
        #expect(result.demotions.count == 1)
        let demo = result.demotions[0]
        #expect(demo.from == .auto)
        #expect(demo.to == .manual)
        #expect(demo.time == 120.0,
                "Demotion timestamp must be the event time at which the threshold is crossed.")
        #expect(demo.falseSignalsAfter == 2)
        #expect(abs(demo.trustAfter - 0.80) < 1e-9,
                "trustAfter must reflect post-decrement, post-floor trust at the moment of demotion.")
        #expect(result.finalState.mode == .manual)
    }

    @Test("Four rewinds on manual → demotion to shadow at fourth event time")
    func fourRewindsManualDemotesToShadow() {
        let initial = Q45fReplayGate.State(
            trustScore: 0.90,
            recentFalseSkipSignals: 0,
            mode: .manual
        )
        let result = Q45fReplayGate.replay(
            initialState: initial,
            events: [Self.event(10), Self.event(20), Self.event(30), Self.event(40)],
            config: Self.cfg
        )
        #expect(result.demotions.count == 1, "Manual stays manual at 1/2/3 signals; demotes only at 4.")
        let demo = result.demotions[0]
        #expect(demo.from == .manual)
        #expect(demo.to == .shadow)
        #expect(demo.time == 40.0)
        #expect(demo.falseSignalsAfter == 4)
        #expect(abs(demo.trustAfter - 0.70) < 1e-9,
                "trustAfter at demotion = 0.90 - 4*0.05 = 0.70.")
        #expect(result.finalState.mode == .shadow)
    }

    @Test("Initial mode shadow → no further demotion regardless of rewind count")
    func shadowNeverDemotes() {
        let initial = Q45fReplayGate.State(
            trustScore: 0.30,
            recentFalseSkipSignals: 0,
            mode: .shadow
        )
        let result = Q45fReplayGate.replay(
            initialState: initial,
            events: (1...10).map { Self.event(Double($0) * 10) },
            config: Self.cfg
        )
        #expect(result.demotions.isEmpty,
                "Shadow is the floor mode — no demotion path below it.")
        #expect(result.finalState.mode == .shadow)
        #expect(result.finalState.recentFalseSkipSignals == 10)
    }

    @Test("Trust score floors at 0 across many rewinds")
    func trustScoreFloorsAtZero() {
        // 50 rewinds × 0.05 = 2.5 demotion; trust must clamp at 0, not go negative.
        let initial = Q45fReplayGate.State(
            trustScore: 0.50,
            recentFalseSkipSignals: 0,
            mode: .auto
        )
        let result = Q45fReplayGate.replay(
            initialState: initial,
            events: (1...50).map { Self.event(Double($0) * 5) },
            config: Self.cfg
        )
        #expect(result.finalState.trustScore == 0.0,
                "Trust must clamp at 0 to mirror max(0, ...) in TrustScoringService.")
    }

    @Test("Auto state already at threshold (signals=1) demotes on the very first replayed rewind")
    func autoAlreadyOneSignalAwayFromDemotion() {
        // Captures the case where a session begins with state already
        // carrying 1 prior signal — the next rewind crosses the threshold.
        let initial = Q45fReplayGate.State(
            trustScore: 0.85,
            recentFalseSkipSignals: 1,
            mode: .auto
        )
        let result = Q45fReplayGate.replay(
            initialState: initial,
            events: [Self.event(75.0)],
            config: Self.cfg
        )
        #expect(result.demotions.count == 1)
        #expect(result.demotions[0].from == .auto)
        #expect(result.demotions[0].to == .manual)
        #expect(result.demotions[0].time == 75.0)
    }

    // MARK: - Multi-demotion + skip-level safety

    @Test("Auto → manual → shadow chains across one replay; produces both demotions in order")
    func chainsAutoToManualToShadow() {
        // Starting from auto/0 signals, 4 rewinds should chain:
        //   event 1: signals=1, mode=.auto (no demotion)
        //   event 2: signals=2, mode=.auto → .manual (1st demotion)
        //   event 3: signals=3, mode=.manual (no demotion)
        //   event 4: signals=4, mode=.manual → .shadow (2nd demotion)
        let initial = Self.autoState()
        let result = Q45fReplayGate.replay(
            initialState: initial,
            events: [Self.event(10), Self.event(20), Self.event(30), Self.event(40)],
            config: Self.cfg
        )
        #expect(result.demotions.count == 2)
        #expect(result.demotions[0].from == .auto)
        #expect(result.demotions[0].to == .manual)
        #expect(result.demotions[0].time == 20.0)
        #expect(result.demotions[0].falseSignalsAfter == 2,
                "First demotion captures the post-increment signal count.")
        #expect(result.demotions[1].from == .manual)
        #expect(result.demotions[1].to == .shadow)
        #expect(result.demotions[1].time == 40.0)
        #expect(result.demotions[1].falseSignalsAfter == 4)
        #expect(result.finalState.mode == .shadow)
        // Demotion direction monotonically downward (no upward "demotions").
        for demo in result.demotions {
            #expect(demo.from != demo.to)
            // .auto > .manual > .shadow lexically encodes the tier order.
            let order: [SkipMode: Int] = [.auto: 2, .manual: 1, .shadow: 0]
            #expect((order[demo.from] ?? -1) > (order[demo.to] ?? -1),
                    "Demotion must always move down the tier.")
        }
    }

    @Test("trustAfter at demotion exactly equals 0.0 when the threshold-crossing event drives trust to zero")
    func trustAfterAtZeroOnDemotion() {
        let initial = Q45fReplayGate.State(
            trustScore: 0.05,
            recentFalseSkipSignals: 1,
            mode: .auto
        )
        let result = Q45fReplayGate.replay(
            initialState: initial,
            events: [Self.event(99.0)],
            config: Self.cfg
        )
        #expect(result.demotions.count == 1)
        #expect(result.demotions[0].trustAfter == 0.0,
                "When the threshold-crossing rewind also drives trust to 0, trustAfter must be exactly 0.")
    }

    // MARK: - Initial-state sanitization is reflected in result.initialState

    @Test("ReplayResult.initialState reflects the sanitized (clamped) trust score, not the raw input")
    func initialStateInResultIsSanitized() {
        let raw = Q45fReplayGate.State(trustScore: -0.5, recentFalseSkipSignals: 0, mode: .auto)
        let result = Q45fReplayGate.replay(
            initialState: raw,
            events: [],
            config: Self.cfg
        )
        #expect(result.initialState.trustScore == 0.0,
                "Consumers compute deltas like (final - initial); initialState must be clamped so the delta is meaningful.")
        #expect(result.finalState == result.initialState)
    }

    @Test("Threshold crossing cannot skip a level: signals=3, mode=.auto, 1 event → stops at .manual")
    func cannotSkipLevels() {
        // evaluateDemotion is called once per event with the *current*
        // mode; a single event can therefore only demote one tier even
        // when signals jump past multiple thresholds.
        let initial = Q45fReplayGate.State(
            trustScore: 0.80,
            recentFalseSkipSignals: 3,
            mode: .auto
        )
        let result = Q45fReplayGate.replay(
            initialState: initial,
            events: [Self.event(50.0)],
            config: Self.cfg
        )
        #expect(result.demotions.count == 1)
        #expect(result.demotions[0].to == .manual,
                "First event from auto must land on manual, even if signals=4 would otherwise suggest shadow.")
        #expect(result.finalState.mode == .manual)
        #expect(result.finalState.recentFalseSkipSignals == 4)
    }

    // MARK: - Config wiring

    @Test("Custom TrustScoringConfig is respected (penalty + thresholds)")
    func customConfigChangesMagnitudeAndThreshold() {
        // weakFalseSignalPenalty=0.20, autoToManualFalseSignals=1 →
        // a single rewind both decrements 0.20 AND demotes.
        let custom = TrustScoringConfig(
            shadowToManualObservations: 3,
            shadowToManualTrustScore: 0.4,
            manualToAutoObservations: 8,
            manualToAutoTrustScore: 0.75,
            autoToManualFalseSignals: 1,
            manualToShadowFalseSignals: 2,
            falseSignalPenalty: 0.10,
            correctObservationBonus: 0.10,
            exceptionalFirstEpisodeConfidence: 0.92,
            weakFalseSignalPenalty: 0.20
        )
        let initial = Self.autoState(trust: 0.80)
        let result = Q45fReplayGate.replay(
            initialState: initial,
            events: [Self.event(10.0)],
            config: custom
        )
        #expect(result.demotions.count == 1)
        #expect(result.demotions[0].from == .auto)
        #expect(result.demotions[0].to == .manual)
        #expect(abs(result.finalState.trustScore - 0.60) < 1e-9)
    }

    // MARK: - Input sanitization

    @Test("Non-finite initial trust score is clamped to 0 on entry")
    func nanInitialTrustClampsToZero() {
        let initial = Q45fReplayGate.State(
            trustScore: .nan,
            recentFalseSkipSignals: 0,
            mode: .auto
        )
        let result = Q45fReplayGate.replay(
            initialState: initial,
            events: [Self.event(10.0)],
            config: Self.cfg
        )
        #expect(result.finalState.trustScore == 0.0,
                "NaN must clamp to 0 on entry — max(0, NaN - 0.05) is NaN, which would propagate forever.")
        #expect(result.finalState.recentFalseSkipSignals == 1)
    }

    @Test("Negative initial trust score is clamped to 0 on entry")
    func negativeInitialTrustClampsToZero() {
        let initial = Q45fReplayGate.State(
            trustScore: -0.5,
            recentFalseSkipSignals: 0,
            mode: .auto
        )
        let result = Q45fReplayGate.replay(
            initialState: initial,
            events: [],
            config: Self.cfg
        )
        #expect(result.finalState.trustScore == 0.0)
    }
}

// MARK: - Production parity (H1)
//
// This suite ties Q45fReplayGate directly to a live `TrustScoringService`
// driven through `recordWeakFalseSkipSignal`. If anyone changes either the
// gate or the production state machine without keeping the other in
// lockstep, the parity assertions fail. This is the structural guarantee
// behind the "single source of truth" claim — the pure-math tests above
// pin intent, this suite pins coupling.

@Suite("Q45fReplayGate production parity", .serialized)
struct Q45fReplayGateParityTests {

    private static let pod = "parity-pod"

    /// Seeds shared between production setup and gate input so the two
    /// sides cannot drift. A single source-of-truth tuple keeps the
    /// parity claim honest (cycle 2 review M3).
    private struct ParitySeed {
        let mode: SkipMode
        let trustScore: Double
        let falseSignals: Int
        let eventCount: Int
    }

    private func seedProduction(_ seed: ParitySeed) async throws -> (TrustScoringService, AnalysisStore) {
        let store = try await makeTestStore()
        try await store.upsertProfile(PodcastProfile(
            podcastId: Self.pod,
            sponsorLexicon: nil,
            normalizedAdSlotPriors: nil,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: seed.trustScore,
            // observationCount is irrelevant to the weak-signal demotion
            // path; pinned at a benign non-zero for fixture stability.
            observationCount: 20,
            mode: seed.mode.rawValue,
            recentFalseSkipSignals: seed.falseSignals
        ))
        return (TrustScoringService(store: store), store)
    }

    private func gateInitial(_ seed: ParitySeed) -> Q45fReplayGate.State {
        Q45fReplayGate.State(
            trustScore: seed.trustScore,
            recentFalseSkipSignals: seed.falseSignals,
            mode: seed.mode
        )
    }

    private func gateEvents(_ seed: ParitySeed) -> [FrozenTrace.FrozenListenRewindEvent] {
        (0..<seed.eventCount).map { i in
            FrozenTrace.FrozenListenRewindEvent(time: Double(i) * 10, windowId: "w", podcastId: Self.pod)
        }
    }

    /// Runs `seed.eventCount` weak-signal records through production, then
    /// asserts `Q45fReplayGate.replay(seed)` produces the same final
    /// {mode, trustScore, recentFalseSkipSignals}. Both sides use the same
    /// `TrustScoringConfig.default` (production via the service's default,
    /// the gate explicitly).
    private func assertParity(
        seed: ParitySeed,
        sourceLocation: SourceLocation = #_sourceLocation
    ) async throws {
        let (svc, store) = try await seedProduction(seed)
        for _ in 0..<seed.eventCount {
            await svc.recordWeakFalseSkipSignal(podcastId: Self.pod)
        }
        let prod = try #require(try await store.fetchProfile(podcastId: Self.pod), sourceLocation: sourceLocation)

        let gate = Q45fReplayGate.replay(
            initialState: gateInitial(seed),
            events: gateEvents(seed),
            config: .default
        )

        #expect(prod.mode == gate.finalState.mode.rawValue,
                "mode parity: prod=\(prod.mode) gate=\(gate.finalState.mode.rawValue)",
                sourceLocation: sourceLocation)
        #expect(prod.recentFalseSkipSignals == gate.finalState.recentFalseSkipSignals,
                "signals parity: prod=\(prod.recentFalseSkipSignals) gate=\(gate.finalState.recentFalseSkipSignals)",
                sourceLocation: sourceLocation)
        #expect(abs(prod.skipTrustScore - gate.finalState.trustScore) < 1e-10,
                "trust parity: prod=\(prod.skipTrustScore) gate=\(gate.finalState.trustScore)",
                sourceLocation: sourceLocation)
    }

    @Test("4 rewinds from auto: production end-state matches gate end-state")
    func parityAutoFourRewinds() async throws {
        try await assertParity(seed: ParitySeed(mode: .auto, trustScore: 0.90, falseSignals: 0, eventCount: 4))
    }

    @Test("4 rewinds from manual: production end-state matches gate end-state")
    func parityManualFourRewinds() async throws {
        try await assertParity(seed: ParitySeed(mode: .manual, trustScore: 0.50, falseSignals: 0, eventCount: 4))
    }

    @Test("Trust floor parity: many rewinds drive both production and gate to trust=0")
    func parityFloorAtZero() async throws {
        try await assertParity(seed: ParitySeed(mode: .shadow, trustScore: 0.10, falseSignals: 0, eventCount: 10))
    }

    @Test("Mid-session resume parity: signals already at 1, one event crosses threshold")
    func parityMidSessionResume() async throws {
        try await assertParity(seed: ParitySeed(mode: .auto, trustScore: 0.85, falseSignals: 1, eventCount: 1))
    }
}
