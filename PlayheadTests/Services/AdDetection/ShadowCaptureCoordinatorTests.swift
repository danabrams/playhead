// ShadowCaptureCoordinatorTests.swift
// playhead-narl.2: Lane A + Lane B semantics tests.
//
// Coverage:
//   Kill switch:
//     - dualFMCaptureEnabled=false makes both lanes no-op.
//   Lane A (JIT):
//     - Only fires when strictly playing.
//     - Budget: bounded calls per simulated wall-clock minute.
//     - Dedupes against already-captured windows (via store read).
//     - Records rows with capturedBy=laneA.
//   Lane B (background thorough):
//     - Only fires when thermal nominal + charging.
//     - Records rows with capturedBy=laneB.
//     - Walks multiple candidates per tick up to laneBCallsPerTick.

import Foundation
import os
import Testing

@testable import Playhead

@Suite("Shadow capture coordinator (playhead-narl.2)")
struct ShadowCaptureCoordinatorTests {

    // MARK: - Kill switch

    @Test("Kill switch off: Lane A no-ops without invoking the dispatcher")
    func killSwitchOffLaneA() async throws {
        let store = try await makeTestStore()
        let dispatcher = RecordingDispatcher()
        let playback = StubPlaybackSignal(isPlaying: true,
                                          asset: ShadowActiveAsset(assetId: "a", playheadSeconds: 0))
        let environment = StubEnvironmentSignal(idle: true)
        let windows = StubWindowSource(
            laneA: [ShadowWindow(start: 0, end: 10)],
            laneB: [:],
            assetsWithGaps: []
        )
        let coord = ShadowCaptureCoordinator(
            store: store,
            dispatcher: dispatcher,
            windowSource: windows,
            playbackSignal: playback,
            environmentSignal: environment,
            clock: { 1_700_000_000 },
            readConfig: { .disabled }
        )
        let outcome = await coord.tickLaneA()
        #expect(outcome == .killSwitchOff)
        #expect(await dispatcher.callCount == 0)
        #expect(try await store.shadowFMResponseCount() == 0)
    }

    @Test("Kill switch off: Lane B no-ops without invoking the dispatcher")
    func killSwitchOffLaneB() async throws {
        let store = try await makeTestStore()
        let dispatcher = RecordingDispatcher()
        let playback = StubPlaybackSignal(isPlaying: false, asset: nil)
        let environment = StubEnvironmentSignal(idle: true)
        let windows = StubWindowSource(
            laneA: [],
            laneB: ["a": [ShadowWindow(start: 0, end: 10)]],
            assetsWithGaps: ["a"]
        )
        let coord = ShadowCaptureCoordinator(
            store: store,
            dispatcher: dispatcher,
            windowSource: windows,
            playbackSignal: playback,
            environmentSignal: environment,
            clock: { 1_700_000_000 },
            readConfig: { .disabled }
        )
        let outcome = await coord.tickLaneB()
        #expect(outcome == .killSwitchOff)
        #expect(await dispatcher.callCount == 0)
        #expect(try await store.shadowFMResponseCount() == 0)
    }

    // MARK: - Lane A: strict playback gate

    @Test("Lane A no-ops when not strictly playing")
    func laneANoOpWhenNotPlaying() async throws {
        let store = try await makeTestStore()
        let dispatcher = RecordingDispatcher()
        let playback = StubPlaybackSignal(isPlaying: false, asset: nil)
        let environment = StubEnvironmentSignal(idle: true)
        let windows = StubWindowSource(
            laneA: [ShadowWindow(start: 0, end: 10)],
            laneB: [:], assetsWithGaps: []
        )
        let coord = ShadowCaptureCoordinator(
            store: store, dispatcher: dispatcher, windowSource: windows,
            playbackSignal: playback, environmentSignal: environment,
            clock: { 1_700_000_000 },
            readConfig: { .default }
        )
        #expect(await coord.tickLaneA() == .notPlaying)
        #expect(await dispatcher.callCount == 0)
    }

    @Test("Lane A dispatches when playing; row persisted with capturedBy=laneA")
    func laneADispatchesAndPersists() async throws {
        let store = try await makeTestStore()
        let dispatcher = RecordingDispatcher()
        let playback = StubPlaybackSignal(isPlaying: true,
                                          asset: ShadowActiveAsset(assetId: "pod1", playheadSeconds: 5.0))
        let environment = StubEnvironmentSignal(idle: true)
        let windows = StubWindowSource(
            laneA: [ShadowWindow(start: 5, end: 15)],
            laneB: [:], assetsWithGaps: []
        )
        let coord = ShadowCaptureCoordinator(
            store: store, dispatcher: dispatcher, windowSource: windows,
            playbackSignal: playback, environmentSignal: environment,
            clock: { 1_700_000_500 },
            readConfig: { .default }
        )

        let outcome = await coord.tickLaneA()
        #expect(outcome == .dispatched)
        #expect(await dispatcher.callCount == 1)

        let rows = try await store.fetchShadowFMResponses(assetId: "pod1")
        #expect(rows.count == 1)
        let row = rows.first
        #expect(row?.capturedBy == .laneA)
        #expect(row?.configVariant == .allEnabledShadow)
        #expect(row?.windowStart == 5)
        #expect(row?.windowEnd == 15)
    }

    @Test("Lane A rate limit: does not exceed laneAMaxCallsPerMinute per 60s")
    func laneABoundedBudgetPerMinute() async throws {
        let store = try await makeTestStore()
        let dispatcher = RecordingDispatcher()
        // Simulate a playback progression: tick every second for 30 ticks.
        // Each tick has a new candidate window (playhead moves by 1s).
        let clock = MutableClock(initial: 1_700_000_000)
        let playback = MutablePlaybackSignal(isPlaying: true,
                                             assetId: "pod2",
                                             playheadSecondsRef: 0)
        let environment = StubEnvironmentSignal(idle: true)

        // Build a dynamic source that returns a window per tick.
        let dynamic = DynamicWindowSource()
        let config = ShadowCaptureConfig(
            dualFMCaptureEnabled: true,
            laneALookaheadSeconds: 60,
            laneAMaxCallsPerMinute: 4,
            laneAMaxInFlight: 1,
            laneBCallsPerTick: 1,
            laneBMaxCallsPerMinute: 8,
            laneBMaxInFlight: 1
        )
        let coord = ShadowCaptureCoordinator(
            store: store,
            dispatcher: dispatcher,
            windowSource: dynamic,
            playbackSignal: playback,
            environmentSignal: environment,
            clock: { clock.now },
            readConfig: { config }
        )

        // Within the first 30s of simulated time we should see at most
        // `laneAMaxCallsPerMinute = 4` dispatches. Additional ticks return
        // .rateLimited.
        var dispatched = 0
        var rateLimited = 0
        for i in 0..<30 {
            clock.now = 1_700_000_000 + TimeInterval(i)
            await playback.advancePlayheadTo(Double(i))
            dynamic.nextWindow = ShadowWindow(start: Double(i * 100), end: Double(i * 100 + 10))
            let outcome = await coord.tickLaneA()
            switch outcome {
            case .dispatched: dispatched += 1
            case .rateLimited: rateLimited += 1
            default: break
            }
        }

        #expect(dispatched == 4, "expected exactly 4 dispatches in 30s, got \(dispatched)")
        #expect(rateLimited >= 1)
    }

    @Test("Lane A skips windows already present in the store")
    func laneASkipsAlreadyCaptured() async throws {
        let store = try await makeTestStore()
        // Pre-seed a row for (0, 10) on this asset.
        try await store.upsertShadowFMResponse(ShadowFMResponse(
            assetId: "pod3",
            windowStart: 0, windowEnd: 10,
            configVariant: .allEnabledShadow,
            fmResponse: Data([0x99]),
            capturedAt: 1_700_000_000,
            capturedBy: .laneB,
            fmModelVersion: "fm-prior"
        ))

        let dispatcher = RecordingDispatcher()
        let playback = StubPlaybackSignal(isPlaying: true,
                                          asset: ShadowActiveAsset(assetId: "pod3", playheadSeconds: 0))
        let environment = StubEnvironmentSignal(idle: true)
        // Window source excludes already-captured (0,10) — the dedupe
        // is the source's responsibility given the `alreadyCaptured` set.
        let windows = StubWindowSource(
            laneA: [],  // nothing left to schedule
            laneB: [:], assetsWithGaps: []
        )
        let coord = ShadowCaptureCoordinator(
            store: store, dispatcher: dispatcher, windowSource: windows,
            playbackSignal: playback, environmentSignal: environment,
            clock: { 1_700_000_050 },
            readConfig: { .default }
        )
        #expect(await coord.tickLaneA() == .noCandidates)
        #expect(await dispatcher.callCount == 0)

        // Verify the source was given the store's already-captured set.
        #expect(await windows.lastLaneAAlreadyCaptured?.count == 1)
    }

    // MARK: - Lane B: thermal + charging gate

    @Test("Lane B no-ops when not idle for backfill")
    func laneBNoOpWhenNotIdle() async throws {
        let store = try await makeTestStore()
        let dispatcher = RecordingDispatcher()
        let playback = StubPlaybackSignal(isPlaying: false, asset: nil)
        let environment = StubEnvironmentSignal(idle: false)
        let windows = StubWindowSource(
            laneA: [],
            laneB: ["b": [ShadowWindow(start: 0, end: 10)]],
            assetsWithGaps: ["b"]
        )
        let coord = ShadowCaptureCoordinator(
            store: store, dispatcher: dispatcher, windowSource: windows,
            playbackSignal: playback, environmentSignal: environment,
            clock: { 1_700_000_000 },
            readConfig: { .default }
        )
        #expect(await coord.tickLaneB() == .notIdle)
        #expect(await dispatcher.callCount == 0)
    }

    // AC-2: thermal and charging predicates must gate Lane B *independently*.
    // Both legs gating at the same boundary is fine (that's what .default
    // does), but one failing without the other must still short-circuit
    // the tick. These two tests flip each leg with the other held true.

    @Test("Lane B .notIdle when thermal nominal but not charging")
    func laneBNotIdleWhenThermalOkButNotCharging() async throws {
        let store = try await makeTestStore()
        let dispatcher = RecordingDispatcher()
        let playback = StubPlaybackSignal(isPlaying: false, asset: nil)
        let environment = StubEnvironmentSignal(thermalNominal: true, charging: false)
        let windows = StubWindowSource(
            laneA: [],
            laneB: ["b": [ShadowWindow(start: 0, end: 10)]],
            assetsWithGaps: ["b"]
        )
        let coord = ShadowCaptureCoordinator(
            store: store, dispatcher: dispatcher, windowSource: windows,
            playbackSignal: playback, environmentSignal: environment,
            clock: { 1_700_000_000 },
            readConfig: { .default }
        )
        #expect(await coord.tickLaneB() == .notIdle)
        #expect(await dispatcher.callCount == 0)
    }

    @Test("Lane B .notIdle when charging but thermal not nominal")
    func laneBNotIdleWhenChargingButThermalSerious() async throws {
        let store = try await makeTestStore()
        let dispatcher = RecordingDispatcher()
        let playback = StubPlaybackSignal(isPlaying: false, asset: nil)
        let environment = StubEnvironmentSignal(thermalNominal: false, charging: true)
        let windows = StubWindowSource(
            laneA: [],
            laneB: ["b": [ShadowWindow(start: 0, end: 10)]],
            assetsWithGaps: ["b"]
        )
        let coord = ShadowCaptureCoordinator(
            store: store, dispatcher: dispatcher, windowSource: windows,
            playbackSignal: playback, environmentSignal: environment,
            clock: { 1_700_000_000 },
            readConfig: { .default }
        )
        #expect(await coord.tickLaneB() == .notIdle)
        #expect(await dispatcher.callCount == 0)
    }

    @Test("Lane B fires when idle; walks up to laneBCallsPerTick windows")
    func laneBFiresWhenIdleWalksMultiple() async throws {
        let store = try await makeTestStore()
        let dispatcher = RecordingDispatcher()
        let playback = StubPlaybackSignal(isPlaying: false, asset: nil)
        let environment = StubEnvironmentSignal(idle: true)
        let windows = StubWindowSource(
            laneA: [],
            laneB: ["ep": [
                ShadowWindow(start: 0, end: 10),
                ShadowWindow(start: 10, end: 20),
                ShadowWindow(start: 20, end: 30),
                ShadowWindow(start: 30, end: 40),
            ]],
            assetsWithGaps: ["ep"]
        )
        let config = ShadowCaptureConfig(
            dualFMCaptureEnabled: true,
            laneALookaheadSeconds: 60,
            laneAMaxCallsPerMinute: 4,
            laneAMaxInFlight: 1,
            laneBCallsPerTick: 2,
            laneBMaxCallsPerMinute: 8,
            laneBMaxInFlight: 1
        )
        let coord = ShadowCaptureCoordinator(
            store: store, dispatcher: dispatcher, windowSource: windows,
            playbackSignal: playback, environmentSignal: environment,
            clock: { 1_700_000_000 },
            readConfig: { config }
        )
        let outcome = await coord.tickLaneB()
        #expect(outcome == .dispatched)
        #expect(await dispatcher.callCount == 2)

        let rows = try await store.fetchShadowFMResponses(assetId: "ep")
        #expect(rows.count == 2)
        for row in rows {
            #expect(row.capturedBy == .laneB)
        }
    }
}

// MARK: - Stubs

private actor RecordingDispatcher: ShadowFMDispatcher {
    private(set) var callCount: Int = 0
    func dispatchShadowCall(
        assetId: String,
        window: ShadowWindow,
        configVariant: ShadowConfigVariant
    ) async throws -> ShadowFMDispatchResult {
        callCount += 1
        // Return a tiny unique-looking payload per call — the test doesn't
        // decode it but we want dedupe logic to not accidentally compare
        // payloads.
        let payload = Data([0xA0, UInt8(truncatingIfNeeded: callCount)])
        return ShadowFMDispatchResult(fmResponse: payload, fmModelVersion: "fm-test")
    }
}

private struct StubPlaybackSignal: ShadowPlaybackSignalProvider {
    let isPlaying: Bool
    let asset: ShadowActiveAsset?
    func isStrictlyPlaying() -> Bool { isPlaying }
    func currentAsset() -> ShadowActiveAsset? { asset }
}

/// Playback stub with a mutable playhead so the rate-limit test can advance
/// the playhead across many ticks without re-wiring.
///
/// Uses `OSAllocatedUnfairLock` so synchronous protocol requirements
/// (`isStrictlyPlaying`, `currentAsset`) remain callable without hopping to
/// an actor, while async test helpers mutate state safely.
private final class MutablePlaybackSignal: ShadowPlaybackSignalProvider, @unchecked Sendable {
    let isPlaying: Bool
    let assetId: String
    private let state: OSAllocatedUnfairLock<Double>
    init(isPlaying: Bool, assetId: String, playheadSecondsRef: Double) {
        self.isPlaying = isPlaying
        self.assetId = assetId
        self.state = OSAllocatedUnfairLock(initialState: playheadSecondsRef)
    }
    func advancePlayheadTo(_ seconds: Double) async {
        state.withLock { $0 = seconds }
    }
    func isStrictlyPlaying() -> Bool { isPlaying }
    func currentAsset() -> ShadowActiveAsset? {
        let seconds = state.withLock { $0 }
        return ShadowActiveAsset(assetId: assetId, playheadSeconds: seconds)
    }
}

/// Test stub that independently controls the two legs of the split env
/// protocol. The `idle:` convenience initializer keeps the legacy
/// `AND-composed` shape (both legs return the same value) for call sites
/// that don't care about the split; tests that specifically target AC-2
/// (thermal vs charging independence) use the designated init.
private struct StubEnvironmentSignal: ShadowEnvironmentSignalProvider {
    let thermalNominal: Bool
    let charging: Bool
    init(thermalNominal: Bool, charging: Bool) {
        self.thermalNominal = thermalNominal
        self.charging = charging
    }
    init(idle: Bool) {
        self.thermalNominal = idle
        self.charging = idle
    }
    func thermalStateIsNominal() -> Bool { thermalNominal }
    func deviceIsCharging() -> Bool { charging }
}

/// Mutable test clock that stays safe across the @Sendable `clock` closure
/// the coordinator captures. Uses `OSAllocatedUnfairLock` so the rate-limit
/// test can advance simulated wall-clock time between ticks.
private final class MutableClock: @unchecked Sendable {
    private let state: OSAllocatedUnfairLock<TimeInterval>
    init(initial: TimeInterval) {
        self.state = OSAllocatedUnfairLock(initialState: initial)
    }
    var now: TimeInterval {
        get { state.withLock { $0 } }
        set { state.withLock { $0 = newValue } }
    }
}

private actor StubWindowSource: ShadowWindowSource {
    let laneA: [ShadowWindow]?                 // nil = return "no candidates"
    let laneB: [String: [ShadowWindow]]         // assetId -> windows
    let assetsWithGaps: [String]
    private(set) var lastLaneAAlreadyCaptured: Set<ShadowWindowKey>?

    init(laneA: [ShadowWindow]?,
         laneB: [String: [ShadowWindow]],
         assetsWithGaps: [String]) {
        self.laneA = laneA
        self.laneB = laneB
        self.assetsWithGaps = assetsWithGaps
    }

    func laneACandidates(
        assetId: String,
        fromSeconds: TimeInterval,
        lookaheadSeconds: TimeInterval,
        alreadyCaptured: Set<ShadowWindowKey>
    ) async throws -> [ShadowWindow] {
        lastLaneAAlreadyCaptured = alreadyCaptured
        return laneA ?? []
    }

    func laneBCandidates(
        assetId: String,
        alreadyCaptured: Set<ShadowWindowKey>
    ) async throws -> [ShadowWindow] {
        return laneB[assetId] ?? []
    }

    func assetsWithIncompleteCoverage() async throws -> [String] {
        return assetsWithGaps
    }
}

/// Window source for the Lane A rate-limit test: returns a single moving
/// candidate whose (start, end) changes per tick.
///
/// The test mutates `nextWindow` between ticks; guard state with
/// `OSAllocatedUnfairLock` so reads and writes are async-safe without
/// needing an actor hop at every call site.
private final class DynamicWindowSource: ShadowWindowSource, @unchecked Sendable {
    private let state: OSAllocatedUnfairLock<ShadowWindow?>
    init() {
        self.state = OSAllocatedUnfairLock(initialState: nil)
    }
    var nextWindow: ShadowWindow? {
        get { state.withLock { $0 } }
        set { state.withLock { $0 = newValue } }
    }
    func laneACandidates(
        assetId: String,
        fromSeconds: TimeInterval,
        lookaheadSeconds: TimeInterval,
        alreadyCaptured: Set<ShadowWindowKey>
    ) async throws -> [ShadowWindow] {
        let window = state.withLock { $0 }
        return window.map { [$0] } ?? []
    }
    func laneBCandidates(
        assetId: String,
        alreadyCaptured: Set<ShadowWindowKey>
    ) async throws -> [ShadowWindow] { [] }
    func assetsWithIncompleteCoverage() async throws -> [String] { [] }
}
