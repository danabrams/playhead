// StingerRefinerTests.swift
// playhead-l2f.6: unit tests for the pure stinger-refinement logic on
// synthetic envelopes. No audio, no actor, no bundle — every case hands the
// refiner hand-built 50 Hz envelopes and asserts the snap/gate/cap/revert/
// grid contracts documented on `StingerRefiner`.

import Foundation
import Testing
@testable import Playhead

@Suite("StingerRefiner (playhead-l2f.6)")
struct StingerRefinerTests {

    // MARK: - Synthetic fixtures

    /// Deterministic pseudo-random template values in [0, 3] (seeded LCG —
    /// stable across runs and platforms). Shaped like a log-RMS envelope.
    private static func syntheticTemplate(count: Int = 350, seed: UInt64 = 9) -> [Float] {
        var state = seed
        return (0..<count).map { _ in
            state = state &* 6_364_136_223_846_793_005 &+ 1_442_695_040_888_963_407
            return Float(state >> 40) / Float(1 << 24) * 3.0
        }
    }

    /// A quiet target envelope with `template` planted at `offset`.
    /// Quiet-floor frames are constant, so their centered correlation with
    /// the template is ~0 and the planted copy is the unambiguous peak
    /// (NCC exactly 1.0 up to floating-point noise).
    private static func envelope(
        planting template: [Float],
        at offset: Int,
        totalFrames: Int,
        startSeconds: Double
    ) -> StingerSearchEnvelope {
        var values = [Float](repeating: 0.05, count: totalFrames)
        for (i, v) in template.enumerated() {
            values[offset + i] = v
        }
        return StingerSearchEnvelope(values: values, startSeconds: startSeconds)
    }

    private static func entry(
        pre: StingerTemplate? = nil,
        post: StingerTemplate? = nil,
        grid: Double? = nil
    ) -> StingerShowEntry {
        StingerShowEntry(
            showKeys: ["test-show"],
            showName: "Test Show",
            pre: pre,
            post: post,
            podWidthGridSeconds: grid
        )
    }

    private static func template(
        _ values: [Float],
        edgeIndex: Int,
        offsetSeconds: Double = 0,
        confidence: Double = 0.80
    ) -> StingerTemplate {
        StingerTemplate(
            template: values,
            edgeSampleIndex: edgeIndex,
            edgeOffsetSeconds: offsetSeconds,
            confidence: confidence,
            support: 3
        )
    }

    // MARK: - Successful snap

    @Test("Planted template snaps the start edge to the exact learned position")
    func successfulSnapToPlantedTemplate() throws {
        let values = Self.syntheticTemplate()
        // Plant at frame 700 of an envelope anchored at 0s: edge index 300
        // → snapped = (700 + 300)/50 + 0.5s learned offset = 20.5s.
        let pre = Self.template(values, edgeIndex: 300, offsetSeconds: 0.5, confidence: 0.80)
        let envelope = Self.envelope(planting: values, at: 700, totalFrames: 9000, startSeconds: 0)

        let result = StingerRefiner.refine(
            proposalStart: 30.0,
            proposalEnd: 60.0,
            entry: Self.entry(pre: pre),
            startEnvelope: envelope,
            endEnvelope: nil,
            episodeDuration: 600.0
        )

        #expect(abs(result.startTime - 20.5) < 0.001, "snap must land on the planted edge (got \(result.startTime))")
        #expect(result.endTime == 60.0, "end edge has no post template and must not move")
        #expect(result.trace.startSnapped)
        #expect(!result.trace.endSnapped)
        #expect(!result.trace.gridApplied)
        #expect(!result.trace.revertedNoOverlap)
        let peak = try #require(result.trace.startPeak)
        #expect(peak > 0.99, "planted copy must correlate at ~1.0 (got \(peak))")
        let delta = try #require(result.trace.startDeltaSeconds)
        #expect(abs(delta - (-9.5)) < 0.001)
        #expect(result.trace.endDeltaSeconds == nil)
    }

    @Test("No envelope (no PCM available) means the side cannot snap")
    func noEnvelopeMeansNoSnap() {
        let values = Self.syntheticTemplate()
        let pre = Self.template(values, edgeIndex: 300)
        let result = StingerRefiner.refine(
            proposalStart: 30.0,
            proposalEnd: 60.0,
            entry: Self.entry(pre: pre),
            startEnvelope: nil,
            endEnvelope: nil,
            episodeDuration: 600.0
        )
        #expect(result.startTime == 30.0)
        #expect(result.endTime == 60.0)
        #expect(result.trace == StingerRefinementTrace())
    }

    // MARK: - Gate rejection

    @Test("Per-show confidence gate rejects a degraded match that a low-confidence show would accept")
    func gateRejectionBelowConfidence() throws {
        let values = Self.syntheticTemplate()
        // Degrade the planted copy: flatten its second half to the mean so
        // the correlation is genuinely partial. Pre-flight the resulting
        // peak into the (0.50 + margin, 0.85 − margin) band so both arms
        // of the assertion are meaningful rather than magic constants.
        let mean = values.reduce(0, +) / Float(values.count)
        var degraded = values
        for i in (values.count / 2)..<values.count {
            degraded[i] = mean
        }
        let envelope = Self.envelope(planting: degraded, at: 700, totalFrames: 9000, startSeconds: 0)
        let measured = try #require(StingerRefiner.normalizedCrossCorrelationPeak(
            template: values,
            target: envelope.values
        ))
        try #require(measured.peak > 0.55 && measured.peak < 0.80,
                     "fixture must land between the two gates (got \(measured.peak))")

        // Arm 1: confidence 1.0 → gate max(0.50, 0.85) = 0.85 → REJECT.
        let strict = StingerRefiner.refine(
            proposalStart: 30.0,
            proposalEnd: 60.0,
            entry: Self.entry(pre: Self.template(values, edgeIndex: 300, confidence: 1.0)),
            startEnvelope: envelope,
            endEnvelope: nil,
            episodeDuration: 600.0
        )
        #expect(!strict.trace.startSnapped, "peak \(measured.peak) must not clear the 0.85 gate")
        #expect(strict.startTime == 30.0)
        #expect(strict.trace.startPeak == nil, "rejected matches record no peak (mirrors the offline trace)")

        // Arm 2: confidence 0.60 → gate max(0.50, 0.45) = 0.50 → ACCEPT.
        // Pins the floor half of `max(0.50, confidence − 0.15)`.
        let loose = StingerRefiner.refine(
            proposalStart: 30.0,
            proposalEnd: 60.0,
            entry: Self.entry(pre: Self.template(values, edgeIndex: 300, confidence: 0.60)),
            startEnvelope: envelope,
            endEnvelope: nil,
            episodeDuration: 600.0
        )
        #expect(loose.trace.startSnapped, "the same peak must clear the 0.50 floor gate")
    }

    // MARK: - Move cap

    @Test("Snaps moving an edge farther than 75s are refused")
    func moveCapRejection() {
        let values = Self.syntheticTemplate()
        // Planted edge at 20.0s, proposal start at 130.0s → 110s move.
        let pre = Self.template(values, edgeIndex: 300)
        let envelope = Self.envelope(planting: values, at: 700, totalFrames: 9500, startSeconds: 0)
        let result = StingerRefiner.refine(
            proposalStart: 130.0,
            proposalEnd: 160.0,
            entry: Self.entry(pre: pre),
            startEnvelope: envelope,
            endEnvelope: nil,
            episodeDuration: 600.0
        )
        #expect(!result.trace.startSnapped, "a 110s move must be refused by the 75s cap")
        #expect(result.startTime == 130.0)
        #expect(result.endTime == 160.0)
    }

    // MARK: - Revert guard

    @Test("Refinement that abandons overlap with the proposal reverts both edges")
    func revertGuardWhenRefinementAbandonsOverlap() {
        let preValues = Self.syntheticTemplate(seed: 11)
        let postValues = Self.syntheticTemplate(seed: 23)
        // Pre snaps start to 20.0s, post snaps end to 25.0s — a [20, 25]
        // window with zero overlap against the [80, 95] proposal (both
        // moves individually clear the 75s cap: 60s and 70s).
        let pre = Self.template(preValues, edgeIndex: 300)
        let post = Self.template(postValues, edgeIndex: 50)
        let startEnvelope = Self.envelope(planting: preValues, at: 700, totalFrames: 9000, startSeconds: 0)
        let endEnvelope = Self.envelope(planting: postValues, at: 1200, totalFrames: 9000, startSeconds: 0)

        let result = StingerRefiner.refine(
            proposalStart: 80.0,
            proposalEnd: 95.0,
            entry: Self.entry(pre: pre, post: post),
            startEnvelope: startEnvelope,
            endEnvelope: endEnvelope,
            episodeDuration: 600.0
        )

        #expect(result.startTime == 80.0, "revert guard must restore the proposal start")
        #expect(result.endTime == 95.0, "revert guard must restore the proposal end")
        #expect(result.trace.revertedNoOverlap)
        #expect(!result.trace.startSnapped, "revert clears the snap flags")
        #expect(!result.trace.endSnapped, "revert clears the snap flags")
        #expect(!result.trace.gridApplied)
        #expect(result.trace.startDeltaSeconds == nil)
        #expect(result.trace.endDeltaSeconds == nil)
    }

    // MARK: - Grid

    @Test("Grid sets the other edge when exactly one edge snapped")
    func gridApplicationWhenOneEdgeSnaps() throws {
        let values = Self.syntheticTemplate()
        // Snap start to 5 + (4200 + 300)/50 = 95.0s; proposal [100, 152]
        // → width 57 → nearest 30s multiple 60 → end = 155.0.
        let pre = Self.template(values, edgeIndex: 300)
        let envelope = Self.envelope(planting: values, at: 4200, totalFrames: 9000, startSeconds: 5.0)
        let result = StingerRefiner.refine(
            proposalStart: 100.0,
            proposalEnd: 152.0,
            entry: Self.entry(pre: pre, grid: 30.0),
            startEnvelope: envelope,
            endEnvelope: nil,
            episodeDuration: 600.0
        )
        #expect(abs(result.startTime - 95.0) < 0.001, "got \(result.startTime)")
        #expect(abs(result.endTime - 155.0) < 0.001, "grid must set end = start + 60 (got \(result.endTime))")
        #expect(result.trace.startSnapped)
        #expect(!result.trace.endSnapped)
        #expect(result.trace.gridApplied)
        let endDelta = try #require(result.trace.endDeltaSeconds)
        #expect(abs(endDelta - 3.0) < 0.001)
    }

    @Test("Grid snaps a sub-grid width up to one full grid multiple")
    func gridMinimumPositiveMultiple() {
        let values = Self.syntheticTemplate()
        // Snap start to 95.0s; proposal [100, 104] → width 9 →
        // max(1, round(9/30)) = 1 → end = 125.0.
        let pre = Self.template(values, edgeIndex: 300)
        let envelope = Self.envelope(planting: values, at: 4200, totalFrames: 9000, startSeconds: 5.0)
        let result = StingerRefiner.refine(
            proposalStart: 100.0,
            proposalEnd: 104.0,
            entry: Self.entry(pre: pre, grid: 30.0),
            startEnvelope: envelope,
            endEnvelope: nil,
            episodeDuration: 600.0
        )
        #expect(abs(result.startTime - 95.0) < 0.001)
        #expect(abs(result.endTime - 125.0) < 0.001, "width must snap UP to one grid (got \(result.endTime))")
        #expect(result.trace.gridApplied)
    }

    @Test("Grid rounds half-grid widths to even multiples, matching the offline oracle's banker's rounding")
    func gridRoundsHalfToEvenLikeTheOfflineOracle() {
        let values = Self.syntheticTemplate()
        // Snap start to 95.0s; proposal [100, 170] → width exactly 75 =
        // 2.5 grid units. Python's round() is round-half-even → 2 → 60s
        // pod → end = 155.0. Swift's default half-away-from-zero rounding
        // would produce 3 → 90s → end = 185.0 — a full grid step of
        // divergence from the recipe the spike measured. Pin the parity.
        let pre = Self.template(values, edgeIndex: 300)
        let envelope = Self.envelope(planting: values, at: 4200, totalFrames: 9000, startSeconds: 5.0)
        let result = StingerRefiner.refine(
            proposalStart: 100.0,
            proposalEnd: 170.0,
            entry: Self.entry(pre: pre, grid: 30.0),
            startEnvelope: envelope,
            endEnvelope: nil,
            episodeDuration: 600.0
        )
        #expect(abs(result.startTime - 95.0) < 0.001)
        #expect(
            abs(result.endTime - 155.0) < 0.001,
            "half-grid width must round to the EVEN multiple like the offline oracle (got \(result.endTime))"
        )
        #expect(result.trace.gridApplied)
    }

    @Test("Grid does not apply when both edges snapped")
    func gridSkippedWhenBothEdgesSnap() {
        let preValues = Self.syntheticTemplate(seed: 11)
        let postValues = Self.syntheticTemplate(seed: 23)
        // Pre → 95.0s, post → 152.0s (width 57, NOT a grid multiple; both
        // deltas small and overlapping the proposal).
        let pre = Self.template(preValues, edgeIndex: 300)
        let post = Self.template(postValues, edgeIndex: 50)
        let startEnvelope = Self.envelope(planting: preValues, at: 4200, totalFrames: 9000, startSeconds: 5.0)
        let endEnvelope = Self.envelope(planting: postValues, at: 7300, totalFrames: 9000, startSeconds: 5.0)
        let result = StingerRefiner.refine(
            proposalStart: 100.0,
            proposalEnd: 150.0,
            entry: Self.entry(pre: pre, post: post, grid: 30.0),
            startEnvelope: startEnvelope,
            endEnvelope: endEnvelope,
            episodeDuration: 600.0
        )
        #expect(result.trace.startSnapped)
        #expect(result.trace.endSnapped)
        #expect(!result.trace.gridApplied, "grid only fires when exactly ONE edge snapped")
        #expect(abs(result.startTime - 95.0) < 0.001)
        #expect(abs(result.endTime - 152.0) < 0.001)
    }

    // MARK: - end <= start impossibility

    @Test("A post snap landing before the start edge can never produce end <= start")
    func endLEStartImpossibility() {
        let values = Self.syntheticTemplate()
        // Post template planted so the snapped END = 10.0s, far BEFORE the
        // 50.0s proposal start (move 50s, within the cap). The clamp floor
        // must force end > start, and the survivor must still overlap the
        // proposal (so no revert) — pinning the exact interaction that
        // could otherwise emit an inverted window.
        let post = Self.template(values, edgeIndex: 50)
        let envelope = Self.envelope(planting: values, at: 450, totalFrames: 9000, startSeconds: 0)
        let result = StingerRefiner.refine(
            proposalStart: 50.0,
            proposalEnd: 60.0,
            entry: Self.entry(post: post),
            startEnvelope: nil,
            endEnvelope: envelope,
            episodeDuration: 600.0
        )
        #expect(result.endTime > result.startTime, "end <= start must be impossible (got [\(result.startTime), \(result.endTime)])")
    }

    @Test("Refined bounds always satisfy end > start across adversarial snap/grid combinations")
    func endAlwaysExceedsStartAcrossAdversarialCombinations() {
        let values = Self.syntheticTemplate()
        let envelope = Self.envelope(planting: values, at: 450, totalFrames: 9000, startSeconds: 0)
        // Sweep proposals (including near-zero and near-duration windows)
        // against pre-only / post-only / both / gridded entries. Every
        // output must keep end > start — the structural invariant the
        // wire-in relies on before handing bounds to fusion.
        let entries: [StingerShowEntry] = [
            Self.entry(pre: Self.template(values, edgeIndex: 300)),
            Self.entry(post: Self.template(values, edgeIndex: 50)),
            Self.entry(
                pre: Self.template(values, edgeIndex: 300),
                post: Self.template(values, edgeIndex: 50),
                grid: 30.0
            ),
            Self.entry(pre: Self.template(values, edgeIndex: 300), grid: 30.0),
        ]
        let proposals: [(Double, Double)] = [
            (0.5, 2.0), (10.0, 11.5), (50.0, 60.0), (0.0, 90.0),
            (598.0, 599.5), (14.0, 15.0),
        ]
        for entry in entries {
            for (start, end) in proposals {
                let result = StingerRefiner.refine(
                    proposalStart: start,
                    proposalEnd: end,
                    entry: entry,
                    startEnvelope: envelope,
                    endEnvelope: envelope,
                    episodeDuration: 600.0
                )
                #expect(
                    result.endTime > result.startTime,
                    "inverted window for proposal [\(start), \(end)] entry sides pre=\(entry.pre != nil) post=\(entry.post != nil) grid=\(entry.podWidthGridSeconds != nil): got [\(result.startTime), \(result.endTime)]"
                )
            }
        }
    }

    // MARK: - NCC guards

    @Test("NCC refuses sub-second templates, oversized templates, and zero-variance templates")
    func nccGuards() {
        let target = [Float](repeating: 0.5, count: 500)
        // Sub-second template (< 50 frames).
        #expect(StingerRefiner.normalizedCrossCorrelationPeak(
            template: [Float](repeating: 1, count: 49),
            target: target
        ) == nil)
        // Template longer than the target.
        #expect(StingerRefiner.normalizedCrossCorrelationPeak(
            template: [Float](repeating: 1, count: 501),
            target: target
        ) == nil)
        // Zero-variance template has no defined correlation.
        #expect(StingerRefiner.normalizedCrossCorrelationPeak(
            template: [Float](repeating: 1, count: 350),
            target: target
        ) == nil)
    }

    // MARK: - Envelope computation

    @Test("StingerEnvelope computes 50 Hz log1p(rms*100) frames and drops the partial tail")
    func envelopeComputation() {
        // One second of constant 0.5 at 16 kHz → 50 frames of
        // log1p(0.5 * 100) = ln(51).
        let oneSecond = [Float](repeating: 0.5, count: 16_000)
        let envelope = StingerEnvelope.compute(samples: oneSecond)
        #expect(envelope.count == 50)
        let expected = Float(log1p(50.0))
        for value in envelope {
            #expect(abs(value - expected) < 0.0001)
        }

        // 703 samples = 2 full 320-sample hops + 63 dropped.
        let short = [Float](repeating: 0.25, count: 703)
        #expect(StingerEnvelope.compute(samples: short).count == 2)

        // Below one hop → empty.
        #expect(StingerEnvelope.compute(samples: [Float](repeating: 1, count: 319)).isEmpty)
    }
}
