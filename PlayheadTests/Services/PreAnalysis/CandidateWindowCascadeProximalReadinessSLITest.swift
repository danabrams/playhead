// CandidateWindowCascadeProximalReadinessSLITest.swift
// playhead-swws: deterministic, in-process simulation that the
// defended `time_to_proximal_skip_ready` SLI thresholds
// (P50 ≤ 45 min, P90 ≤ 4 h, pinned in
// `TimeToProximalSkipReadyThresholds`) are satisfied by a
// representative-shape synthetic latency model.
//
// HONEST SCOPE — read this before tightening or relying on this test
// ------------------------------------------------------------------
// This test is a SIMULATION of latency under a representative load
// model. It does NOT measure the candidate-window cascade's
// contribution to that latency. The synthetic samples are generated
// from a log-normal model independently of the cascade; the
// cascade is exercised inside the loop only to confirm it produces
// a `.proximal` window per seeded episode (a no-op assertion on the
// cascade, NOT a timing measurement).
//
// In particular, this test will continue to pass even if the
// cascade's seed/relatch latency regresses, because the latency
// numbers come from the synthetic model, not from clock-driven
// observation of the cascade. A real cascade-attributed P90 SLI
// requires:
//   1. An injected clock,
//   2. End-to-end driving of the production explicit-download →
//      first-proximal-window-ready event sequence (download
//      complete → enqueue → cascade seed → scheduler dispatch →
//      first proximal slice ready),
//   3. Aggregation across many synthetic episodes with that real
//      timing.
// That harness is its own bead-sized chunk and is tracked as the
// follow-up bead `playhead-e2vw`
// (discovered-from:playhead-swws):
// "B2-followup: real cascade-attributed proximal-readiness SLI from
// production timing".
//
// What this test IS useful for
// ----------------------------
// * Anchoring the defended P50 / P90 thresholds in a runnable
//   assertion so that tightening
//   `TimeToProximalSkipReadyThresholds` without updating the model
//   is loud (the model's P50/P90 fall above the new threshold and
//   the test fails).
// * Documenting the latency-component decomposition (download,
//   queue-wait + admission gating, ASR slice) the d99 SLI was
//   defended against, so that future cascade work can revisit the
//   model when it shifts.
// * Smoke-testing that the cascade still emits a `.proximal`
//   window across the 30–90 min episode cohort (the inner cascade
//   call is genuine and would fail loudly if the cascade regressed
//   to no-window).
//
// What this test is NOT
// ---------------------
// * A production telemetry pipeline test. Phase 1's SLI emitter
//   (playhead-1nl6) writes only to `WorkJournal.cause`; there is
//   no `time_to_proximal_skip_ready` sample-recording
//   infrastructure today (Phase 2 work).
// * A causal measurement of the cascade's contribution to the
//   measured latency. The cascade is exercised but not timed.
//
// The simulated latency model (representative-shape, not derived)
// ---------------------------------------------------------------
// The d99 SLI scope is "explicit download, eligible-and-available
// mode, 30–90 min episode" with P50 ≤ 45 min, P90 ≤ 4 h. The
// dominant latency contributors on this path are:
//   - Download time (transport-bound, weakly bimodal across Wi-Fi
//     vs. constrained networks)
//   - Pre-roll warm-up + first proximal slice ASR cost
//     (capability-bound — bigger devices finish faster)
//   - Scheduler queue-wait + admission gating
// We model the per-episode latency as the sum of three log-normally
// distributed components with means/sigmas chosen so that the
// resulting empirical P50 lands near 30 minutes and P90 stays
// comfortably below 4 hours under the documented load model. The
// model is deterministic (seeded RNG) so the test is not flaky.
//
// Sample size
// -----------
// 50 synthetic episodes — enough that P90 = sample[44] is a stable
// estimator of the 90th percentile (5 episodes above the cut, 45
// below) and the test runs in well under a second. The d99 bead
// itself does not pin a sample size; this is sized for harness
// determinism.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-swws: time_to_proximal_skip_ready P50/P90 — SYNTHETIC LATENCY MODEL (cascade-decoupled, see file header)")
struct CandidateWindowCascadeProximalReadinessSLITest {

    // MARK: - Simulation parameters

    /// Number of synthetic episode samples. Sized for stable P90 with
    /// fast test runtime.
    private static let sampleCount = 50

    /// Seeded RNG for reproducibility — flake-free across CI runs.
    /// The seed is arbitrary; changing it should not flip the P90
    /// assertion if the cascade behaves correctly.
    private static let rngSeed: UInt64 = 0x5005_5115_C0DE_F00D

    // MARK: - Test

    @Test("Synthetic latency model satisfies defended P50/P90 thresholds (NOT a cascade timing measurement — see file header)")
    func syntheticLatencyModelSatisfiesDefendedThresholds() async throws {
        var rng = SeededRNG(seed: Self.rngSeed)
        var samples: [TimeInterval] = []
        samples.reserveCapacity(Self.sampleCount)

        // For each synthetic episode: generate its model latency,
        // then run the cascade once as a sanity check that the
        // cascade still emits a `.proximal` window for the 30–90
        // min cohort. The cascade call is NOT timed and does NOT
        // contribute to the latency sample — that is the
        // limitation called out in the file header. The cascade is
        // per-test (not shared) so anchor/window state from one
        // episode does not leak into another.
        for index in 0..<Self.sampleCount {
            let cascade = CandidateWindowCascade(config: PreAnalysisConfig())

            // Generate this episode's synthetic wall-clock latency
            // from the representative-shape model (see file
            // header). This number is what feeds the P50/P90
            // assertion below; it is independent of the cascade.
            let simulatedLatencySec = simulatedTimeToProximalReadySeconds(rng: &rng)

            // Cascade smoke test: confirm the cascade still emits
            // a `.proximal` window for an unplayed 30–90 min
            // episode. This guards against cascade regressions
            // that would silently break the proximal-readiness
            // surface; it does NOT measure cascade timing.
            let episodeId = "ep-swws-sli-\(index)"
            let windows = await cascade.seed(
                episodeId: episodeId,
                episodeDuration: simulatedEpisodeDurationSeconds(rng: &rng),
                playbackAnchor: nil,
                chapterEvidence: []
            )
            #expect(
                windows.contains(where: { $0.kind == .proximal }),
                "cascade did not produce a proximal window for episode \(episodeId) — cascade regression, not a latency-model failure"
            )

            samples.append(simulatedLatencySec)
        }

        // Compute P90 (90th percentile) of the latency samples
        // using the standard nearest-rank method: ceil(0.9 * n) - 1
        // index into the sorted array.
        samples.sort()
        let p90Index = Int(ceil(0.9 * Double(samples.count))) - 1
        let p90 = samples[p90Index]
        let p50Index = Int(ceil(0.5 * Double(samples.count))) - 1
        let p50 = samples[p50Index]

        // The defended SLI threshold from playhead-d99.
        let p90Threshold = TimeToProximalSkipReadyThresholds.p90Seconds
        let p50Threshold = TimeToProximalSkipReadyThresholds.p50Seconds

        #expect(
            p90 <= p90Threshold,
            "Synthetic-model P90 = \(p90)s exceeds defended threshold \(p90Threshold)s (4 h). Sample size = \(samples.count). NOTE: this is a model assertion, not a cascade timing measurement (see file header). Either the model has been re-tuned without regard to the threshold, or `TimeToProximalSkipReadyThresholds.p90Seconds` has been tightened without updating the model."
        )

        // P50 sanity check — the simulated load model is calibrated
        // so the central tendency is well within the median
        // threshold. If P50 starts blowing the threshold the model
        // has shifted enough that the P90 bound is probably
        // accidentally satisfied by extreme tails — fail so the
        // operator looks.
        #expect(
            p50 <= p50Threshold,
            "Synthetic-model P50 = \(p50)s exceeds defended threshold \(p50Threshold)s (45 min). Model has been re-tuned or `TimeToProximalSkipReadyThresholds.p50Seconds` has tightened. NOTE: this is a model assertion, not a cascade timing measurement (see file header)."
        )
    }

    // MARK: - Latency model

    /// Simulated wall-clock seconds from explicit-download tap to
    /// first-proximal-window-ready. Sums three log-normally
    /// distributed components so the resulting distribution has a
    /// realistic right-tail without a hard cap that would make the
    /// P90 assertion vacuous.
    ///
    /// Tuned so the empirical median lands near 30 minutes (well
    /// inside P50 ≤ 45 min) and the 90th percentile lands well
    /// under 4 hours (the defended P90 ceiling). The components
    /// are not derived from real device telemetry — they are a
    /// representative-shape model whose only purpose is to make
    /// this test non-trivially exercise the cascade across a range
    /// of latencies.
    private func simulatedTimeToProximalReadySeconds(rng: inout SeededRNG) -> TimeInterval {
        // Component 1: download time. Mean ~ exp(6.0) = ~400 s
        // (~7 min) with sigma 0.6 → some Wi-Fi-fast samples, some
        // slow-cellular tails.
        let download = logNormal(mu: 6.0, sigma: 0.6, rng: &rng)

        // Component 2: scheduler queue-wait + admission gating.
        // Mean ~ exp(5.0) = ~150 s (~2.5 min) with sigma 0.8 →
        // includes the rare longer waits when the device is in a
        // constrained execution condition.
        let queueWait = logNormal(mu: 5.0, sigma: 0.8, rng: &rng)

        // Component 3: warm-up + first proximal slice ASR cost.
        // Mean ~ exp(6.5) = ~665 s (~11 min) with sigma 0.5 →
        // capability-bound, less variance than download.
        let asrSlice = logNormal(mu: 6.5, sigma: 0.5, rng: &rng)

        return download + queueWait + asrSlice
    }

    /// Simulated episode duration, uniformly distributed across
    /// the 30–90 min d99 cohort.
    private func simulatedEpisodeDurationSeconds(rng: inout SeededRNG) -> TimeInterval {
        let minSec: TimeInterval = 30 * 60
        let maxSec: TimeInterval = 90 * 60
        let u = rng.nextUnitDouble()
        return minSec + (maxSec - minSec) * u
    }

    // MARK: - Sampling primitives

    /// Box–Muller transform to a standard normal sample.
    private func standardNormal(rng: inout SeededRNG) -> Double {
        // Avoid u1 == 0 (log(0) = -inf).
        let u1 = max(rng.nextUnitDouble(), 1e-12)
        let u2 = rng.nextUnitDouble()
        return sqrt(-2.0 * log(u1)) * cos(2.0 * .pi * u2)
    }

    /// Log-normal sample: exp(mu + sigma * Z) where Z is standard normal.
    private func logNormal(mu: Double, sigma: Double, rng: inout SeededRNG) -> Double {
        let z = standardNormal(rng: &rng)
        return exp(mu + sigma * z)
    }
}

// MARK: - SeededRNG

/// Deterministic PRNG (xorshift64*) used by the SLI simulation so
/// the empirical P90 is reproducible across CI runs. Not
/// cryptographically secure, intentionally — this is a test fixture.
private struct SeededRNG {
    private var state: UInt64

    init(seed: UInt64) {
        // xorshift64* refuses zero-state; coerce.
        self.state = seed == 0 ? 0xDEAD_BEEF_CAFE_F00D : seed
    }

    mutating func nextUInt64() -> UInt64 {
        var x = state
        x ^= x >> 12
        x ^= x << 25
        x ^= x >> 27
        state = x
        return x &* 0x2545_F491_4F6C_DD1D
    }

    /// Next double in [0, 1).
    mutating func nextUnitDouble() -> Double {
        // 53 high bits → uniform in [0, 1).
        let bits = nextUInt64() >> 11
        return Double(bits) / Double(1 << 53)
    }
}
