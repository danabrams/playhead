// PerShowThresholdControllerTests.swift
// playhead-xsdz.11: Hermetic unit tests for the per-show auto-skip threshold
// controller — the pure PI controller math (`PerShowThresholdController`), its
// per-show state, the effective-threshold clamp, and the SQLite-backed
// `PerShowThresholdControllerStore` (per-show persistence, isolation,
// determinism).
//
// These are FULLY hermetic (no FM, no audio, no corpus). The store tests use a
// temp-dir SQLite file and tear it down per test.

import Foundation
import Testing
@testable import Playhead

// MARK: - Controller math

@Suite("PerShowThresholdController PI math (playhead-xsdz.11)")
struct PerShowThresholdControllerMathTests {

    private let params = PerShowThresholdControllerParameters.default

    @Test("Below min-samples the controller emits a zero offset (cold-start)")
    func minSampleGate() {
        // minSamples is 5 by default. Four FP corrections must NOT move the
        // offset; the integral and sampleCount still advance (so the gate opens
        // on the next correction).
        var state = PerShowThresholdControllerState.zero
        for _ in 0..<(params.minSamples - 1) {
            state = PerShowThresholdController.apply(signal: .falsePositive, to: state, parameters: params)
            #expect(state.offset == 0, "below the min-sample gate the offset must stay 0")
        }
        #expect(state.sampleCount == params.minSamples - 1)
        #expect(state.integral == params.minSamples - 1)

        // The very next correction crosses the gate and produces a non-zero offset.
        state = PerShowThresholdController.apply(signal: .falsePositive, to: state, parameters: params)
        #expect(state.sampleCount == params.minSamples)
        #expect(state.offset > 0, "at the min-sample count the FP offset must become positive")
    }

    @Test("FP corrections RAISE the threshold toward the cap; integral accumulates")
    func falsePositivesRaise() {
        // Drive a long stream of FP corrections; the offset should climb
        // monotonically (until saturation) and stay strictly positive.
        var state = PerShowThresholdControllerState.zero
        var lastOffset = -1.0
        for i in 0..<40 {
            state = PerShowThresholdController.apply(signal: .falsePositive, to: state, parameters: params)
            if i >= params.minSamples - 1 {
                #expect(state.offset > 0)
                #expect(state.offset >= lastOffset - 1e-12, "FP stream must not LOWER the offset")
                lastOffset = state.offset
            }
        }
        // Integral is the running sum of +1 errors == sampleCount here.
        #expect(state.integral == 40)
        #expect(state.sampleCount == 40)
        // Saturates at the positive offset cap.
        #expect(abs(state.offset - params.maxOffset) < 1e-9, "a long FP stream saturates at +maxOffset")
    }

    @Test("Miss corrections LOWER the threshold toward the floor")
    func missesLower() {
        var state = PerShowThresholdControllerState.zero
        for _ in 0..<40 {
            state = PerShowThresholdController.apply(signal: .miss, to: state, parameters: params)
        }
        #expect(state.integral == -40)
        #expect(state.offset < 0, "a miss stream must drive a negative offset")
        #expect(abs(state.offset - (-params.maxOffset)) < 1e-9, "a long miss stream saturates at −maxOffset")
    }

    @Test("Offset is bounded by ±maxOffset under saturation")
    func boundedOffset() {
        let fp = PerShowThresholdController.replay(signals: Array(repeating: .falsePositive, count: 1000), parameters: params)
        let miss = PerShowThresholdController.replay(signals: Array(repeating: .miss, count: 1000), parameters: params)
        #expect(fp.offset <= params.maxOffset + 1e-12)
        #expect(miss.offset >= -params.maxOffset - 1e-12)
    }

    @Test("Effective threshold is clamped to [0.55, 0.95]")
    func effectiveClamp() {
        // A huge positive offset against a high base still clamps at 0.95.
        let high = PerShowThresholdController.effectiveThreshold(globalThreshold: 0.90, offset: 0.50, parameters: params)
        #expect(high == 0.95)
        // A huge negative offset against a low base still clamps at 0.55.
        let low = PerShowThresholdController.effectiveThreshold(globalThreshold: 0.60, offset: -0.50, parameters: params)
        #expect(low == 0.55)
        // A modest offset within the band passes through.
        let mid = PerShowThresholdController.effectiveThreshold(globalThreshold: 0.80, offset: 0.05, parameters: params)
        #expect(abs(mid - 0.85) < 1e-9)
    }

    @Test("Zero offset leaves the global threshold unchanged (within the band)")
    func zeroOffsetIdentity() {
        let t = PerShowThresholdController.effectiveThreshold(globalThreshold: 0.80, offset: 0, parameters: params)
        #expect(t == 0.80, "a zero offset must not move a within-band global threshold")
    }

    @Test("Non-finite inputs degrade safely (no NaN gate)")
    func nonFiniteSafe() {
        let nanOffset = PerShowThresholdController.effectiveThreshold(globalThreshold: 0.80, offset: .nan, parameters: params)
        #expect(nanOffset == 0.80, "a NaN offset is treated as zero")
        let nanGlobal = PerShowThresholdController.effectiveThreshold(globalThreshold: .infinity, offset: 0.05, parameters: params)
        #expect(nanGlobal.isFinite, "a non-finite global threshold must not yield a non-finite gate")
    }

    @Test("Deterministic: the same history yields the same state")
    func determinism() {
        let history: [ThresholdControlSignal] = [
            .falsePositive, .falsePositive, .miss, .falsePositive, .miss,
            .miss, .falsePositive, .falsePositive, .miss, .falsePositive,
        ]
        let a = PerShowThresholdController.replay(signals: history, parameters: params)
        let b = PerShowThresholdController.replay(signals: history, parameters: params)
        #expect(a == b)
        // Incremental folding equals batch replay (the store's write path folds
        // one at a time; this proves it converges to the same state).
        var incremental = PerShowThresholdControllerState.zero
        for s in history {
            incremental = PerShowThresholdController.apply(signal: s, to: incremental, parameters: params)
        }
        #expect(incremental == a)
    }

    @Test("Conflicting corrections net out via the integral")
    func conflictingCorrections() {
        // Equal FP and miss counts: the integral returns to 0, so the offset is
        // driven only by the last proportional term (tiny), not a runaway.
        let balanced = PerShowThresholdController.replay(
            signals: [.falsePositive, .miss, .falsePositive, .miss, .falsePositive, .miss],
            parameters: params
        )
        #expect(balanced.integral == 0)
        // With integral 0, the offset is Kp * last error. Last signal is .miss
        // (error −1) so the offset is a small negative number, bounded.
        #expect(abs(balanced.offset) <= params.maxOffset + 1e-12)
        #expect(abs(balanced.offset - (params.proportionalGain * -1.0)) < 1e-9)
    }

    @Test("No corrections ⇒ zero state ⇒ unmodified global threshold")
    func noCorrections() {
        let state = PerShowThresholdController.replay(signals: [], parameters: params)
        #expect(state == .zero)
        let t = PerShowThresholdController.effectiveThreshold(globalThreshold: 0.80, offset: state.offset, parameters: params)
        #expect(t == 0.80)
    }
}

// MARK: - Store

@Suite("PerShowThresholdControllerStore (playhead-xsdz.11)")
struct PerShowThresholdControllerStoreTests {

    private func makeStore() throws -> PerShowThresholdControllerStore {
        let dir = try makeTempDir(prefix: "xsdz11-store")
        return try PerShowThresholdControllerStore(directoryURL: dir)
    }

    @Test("Unknown show returns the cold-start zero state / zero offset")
    func coldStart() async throws {
        let store = try makeStore()
        let state = await store.state(forShow: "never-seen")
        #expect(state == .zero)
        #expect(await store.offset(forShow: "never-seen") == 0)
        await store.close()
    }

    @Test("Recording FP signals raises the persisted offset; miss lowers it")
    func recordsAndPersists() async throws {
        let store = try makeStore()
        let params = PerShowThresholdControllerParameters.default
        // Fold enough FP corrections to cross the min-sample gate.
        for _ in 0..<(params.minSamples + 2) {
            _ = try await store.record(signal: .falsePositive, forShow: "showA")
        }
        let offset = await store.offset(forShow: "showA")
        #expect(offset > 0, "FP corrections must raise the persisted offset")

        // A fresh store reading the SAME file sees the persisted state — proving
        // it survives, not just an in-memory cache.
        let reopened = try PerShowThresholdControllerStore(directoryURL: store.dbURL.deletingLastPathComponent())
        #expect(await reopened.offset(forShow: "showA") == offset)
        await reopened.close()
        await store.close()
    }

    @Test("Per-show isolation: one show's corrections do not move another's")
    func perShowIsolation() async throws {
        let store = try makeStore()
        let params = PerShowThresholdControllerParameters.default
        for _ in 0..<(params.minSamples + 5) {
            _ = try await store.record(signal: .falsePositive, forShow: "showA")
        }
        // showB never corrected → still cold-start.
        #expect(await store.offset(forShow: "showA") > 0)
        #expect(await store.offset(forShow: "showB") == 0)
        await store.close()
    }

    @Test("Incremental store writes match a from-scratch replay (determinism)")
    func storeMatchesReplay() async throws {
        let store = try makeStore()
        let params = PerShowThresholdControllerParameters.default
        let history: [ThresholdControlSignal] = [
            .falsePositive, .miss, .falsePositive, .falsePositive, .miss,
            .falsePositive, .falsePositive, .miss,
        ]
        for s in history {
            _ = try await store.record(signal: s, forShow: "showX")
        }
        let stored = await store.state(forShow: "showX")
        let replayed = PerShowThresholdController.replay(signals: history, parameters: params)
        #expect(stored == replayed, "incremental persisted folding must equal a batch replay")
        await store.close()
    }

    @Test("Empty podcastId is rejected on write")
    func rejectsEmptyShow() async throws {
        let store = try makeStore()
        await #expect(throws: PerShowThresholdControllerStoreError.self) {
            _ = try await store.record(signal: .falsePositive, forShow: "")
        }
        #expect(try await store.count() == 0)
        await store.close()
    }
}
