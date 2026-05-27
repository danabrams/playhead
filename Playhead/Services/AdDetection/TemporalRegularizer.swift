// TemporalRegularizer.swift
// playhead-xsdz.10: Lightweight temporal regularization for on-device ad
// detection.
//
// Insight (cross-model idea duel): real ads are CONTIGUOUS and CLUSTERED —
// 2-4 back-to-back creatives in a single break — whereas false positives are
// typically ISOLATED one-off editorial mentions ("...as our sponsor showed
// last week..."). This pass applies two DETERMINISTIC, post-fusion temporal
// constraints across an episode's candidate detections, computed purely from
// each span's time interval and its (post-fragility / post-negative-bank)
// `skipConfidence` — no new evidence source, no sequence model:
//
//   1. ISOLATION PENALTY — a detection with NO high-confidence neighbor inside
//      a ±window (default 120s edge-to-edge) gets a soft multiplicative penalty
//      on `skipConfidence`. A lonely detection is more likely an FP.
//   2. MINIMUM-DWELL — an ad "island" shorter than a minimum duration AND
//      uncorroborated (no high-confidence neighbor) is additionally
//      down-weighted. A blink-length uncorroborated span is FP-shaped.
//
// CORRECTNESS REQUIREMENT — ANTI-CONTAGION / ONE-SIDED:
//   The neighbor-support signal is HIGH-CONFIDENCE-GATED and ONE-SIDED. A
//   neighbor only counts as "support" if ITS OWN `skipConfidence` is at or
//   above `highConfidenceNeighborThreshold` (default 0.80). There is NO
//   symmetric naive neighbor-boost: this pass can only ever LOWER a span's
//   confidence, never raise it. Two adjacent WEAK FPs therefore do NOT rescue
//   each other — neither qualifies as the other's high-confidence neighbor, so
//   both are penalized as isolated. This is the anti-contagion guarantee both
//   duel models demanded (naive contagion cements correlated FPs).
//
// This is deliberately NOT a full HMM/CRF (both models rejected a sequence
// model for a 12-episode corpus): just a deterministic isolation-penalty +
// min-dwell with a few tunable scalars. Cost is O(n log n) (one sort + a
// windowed scan).
//
// OFF by default: the service only invokes this pass when
// `AdDetectionConfig.temporalRegularizationEnabled == true`. When the flag is
// off the service never calls it, so behaviour is byte-identical to pre-xsdz.10.
// Even so, every method here is a pure no-op for a degenerate corpus (0 or 1
// detection) and the penalty factors default to values < 1 that lower nothing
// for clustered, long spans.

import Foundation

// MARK: - TemporalRegularizer

enum TemporalRegularizer {

    /// A lightweight per-detection descriptor fed to the regularizer. Carries
    /// only what the temporal constraints need: a stable identity, the span's
    /// time interval, and the pre-temporal `skipConfidence` (the exact value
    /// the hard auto-skip gate would compare against the threshold).
    struct Detection: Sendable, Equatable {
        let id: String
        let startTime: Double
        let endTime: Double
        /// The decision's current skip confidence BEFORE temporal regularization
        /// (i.e. after the xsdz.7 fragility penalty and xsdz.9 negative-bank
        /// suppression). Used BOTH as the value to penalize for this detection
        /// AND — for OTHER detections — as the neighbor-support signal.
        let skipConfidence: Double

        var duration: Double { endTime - startTime }
    }

    /// Tunable scalars for the temporal pass. Conservative defaults: penalties
    /// shave at most a small fraction off a lonely / too-short span without
    /// hard-blocking anything.
    struct Parameters: Sendable, Equatable {
        /// Edge-to-edge time window (seconds) within which a neighbor can count
        /// as supporting context. A neighbor whose interval is within this gap
        /// of the subject span (0 if they overlap) is "in window".
        let neighborWindowSeconds: Double
        /// A neighbor counts as SUPPORT only when its own `skipConfidence` is at
        /// or above this threshold (the anti-contagion gate). Default 0.80 — the
        /// production auto-skip floor — so only a confident detection lends
        /// support; weak FPs cannot prop each other up.
        let highConfidenceNeighborThreshold: Double
        /// Multiplicative penalty applied to an ISOLATED detection's
        /// `skipConfidence` (no high-confidence neighbor in the window). Default
        /// 0.85 — matches the xsdz.7 fragility-penalty magnitude. Clamped to
        /// [0, 1]; never boosts.
        let isolationPenaltyFactor: Double
        /// A detection shorter than this (seconds) AND uncorroborated is treated
        /// as a too-short "island" and additionally down-weighted. Default 10s.
        let minDwellSeconds: Double
        /// Multiplicative penalty applied to a too-short, uncorroborated island.
        /// Default 0.90. Clamped to [0, 1]; never boosts.
        let minDwellPenaltyFactor: Double

        static let `default` = Parameters(
            neighborWindowSeconds: 120.0,
            highConfidenceNeighborThreshold: 0.80,
            isolationPenaltyFactor: 0.85,
            minDwellSeconds: 10.0,
            minDwellPenaltyFactor: 0.90
        )

        init(
            neighborWindowSeconds: Double = 120.0,
            highConfidenceNeighborThreshold: Double = 0.80,
            isolationPenaltyFactor: Double = 0.85,
            minDwellSeconds: Double = 10.0,
            minDwellPenaltyFactor: Double = 0.90
        ) {
            self.neighborWindowSeconds = neighborWindowSeconds
            self.highConfidenceNeighborThreshold = highConfidenceNeighborThreshold
            self.isolationPenaltyFactor = isolationPenaltyFactor
            self.minDwellSeconds = minDwellSeconds
            self.minDwellPenaltyFactor = minDwellPenaltyFactor
        }
    }

    /// The per-detection outcome of the pass. `adjustedSkipConfidence` is the
    /// value the caller should substitute for the span's `skipConfidence`
    /// BEFORE the hard auto-skip gate. The flags are diagnostic (logging).
    struct Adjustment: Sendable, Equatable {
        let id: String
        let originalSkipConfidence: Double
        let adjustedSkipConfidence: Double
        /// True when no high-confidence neighbor was found in the window.
        let isIsolated: Bool
        /// True when the isolation penalty was actually applied (i.e. isolated
        /// AND the factor changed the value).
        let isolationPenaltyApplied: Bool
        /// True when the min-dwell penalty was actually applied.
        let minDwellPenaltyApplied: Bool

        /// True iff a penalty actually moved the confidence. Derived from the
        /// penalty flags rather than `adjusted != original`: a passed-through
        /// non-finite confidence has `adjusted == original` (both NaN), but
        /// `NaN != NaN` is `true` in IEEE 754 — a value comparison would
        /// spuriously report a NaN passthrough as "changed", triggering a no-op
        /// rebuild and a misleading "NaN → NaN" log line at the call site. The
        /// flags are set only on the two code paths that mutate `adjusted` (and
        /// only when the new value actually differs), so this is exactly the
        /// finite-case semantics with correct NaN behaviour.
        var changed: Bool { isolationPenaltyApplied || minDwellPenaltyApplied }
    }

    /// Edge-to-edge temporal gap between two intervals: 0 when they overlap or
    /// touch, otherwise the distance between the nearest edges. Always >= 0 and
    /// finite for finite inputs. Symmetric in its arguments.
    static func gap(_ a: Detection, _ b: Detection) -> Double {
        // Overlap (or touch) ⇒ 0. Otherwise the positive distance between the
        // later start and the earlier end.
        let lo = max(a.startTime, b.startTime)
        let hi = min(a.endTime, b.endTime)
        // If `lo <= hi` the intervals overlap/touch ⇒ gap 0; else `lo - hi` is
        // the separation.
        return max(0.0, lo - hi)
    }

    /// Apply the temporal constraints to a collection of detections and return
    /// one `Adjustment` per input detection.
    ///
    /// DETERMINISTIC / ORDER-INDEPENDENT: the input is sorted by
    /// `(startTime, endTime, id)` before scanning, and the high-confidence
    /// neighbor test for every detection reads the ORIGINAL (pre-penalty)
    /// `skipConfidence` of its neighbors — so the result does not depend on
    /// input order and penalties never cascade (one span's penalty cannot
    /// change whether it counts as another span's neighbor).
    ///
    /// ONE-SIDED: each returned `adjustedSkipConfidence` is `<=` the input
    /// `skipConfidence` (a product of factors clamped to [0, 1]); the pass can
    /// never raise a confidence.
    ///
    /// - Returns: adjustments keyed by detection, in sorted order. A 0- or
    ///   1-element input returns the input(s) unchanged (no neighbors possible).
    static func regularize(
        detections: [Detection],
        parameters: Parameters = .default
    ) -> [Adjustment] {
        // A 0- or 1-detection episode has no neighbor structure: isolation is
        // only meaningful RELATIVE to other detections. A lone detection is the
        // whole episode's only candidate, not a span "isolated" from a cluster,
        // so it must be returned unchanged (no penalty). This also makes the
        // pass self-consistent with the service's `count > 1` invocation guard.
        guard detections.count > 1 else {
            return detections.map { subject in
                Adjustment(
                    id: subject.id,
                    originalSkipConfidence: subject.skipConfidence,
                    adjustedSkipConfidence: subject.skipConfidence,
                    isIsolated: false,
                    isolationPenaltyApplied: false,
                    minDwellPenaltyApplied: false
                )
            }
        }

        // Deterministic, stable ordering. `id` breaks ties so two spans with
        // identical bounds still order reproducibly.
        let sorted = detections.sorted { lhs, rhs in
            if lhs.startTime != rhs.startTime { return lhs.startTime < rhs.startTime }
            if lhs.endTime != rhs.endTime { return lhs.endTime < rhs.endTime }
            return lhs.id < rhs.id
        }

        let window = max(0.0, parameters.neighborWindowSeconds)
        let highConf = parameters.highConfidenceNeighborThreshold
        let isolationFactor = clampUnit(parameters.isolationPenaltyFactor)
        let dwellFactor = clampUnit(parameters.minDwellPenaltyFactor)
        let minDwell = parameters.minDwellSeconds

        return sorted.map { subject in
            // A non-finite confidence is a data-integrity problem; leave it
            // untouched so downstream non-finite guards stay in control (same
            // contract as the xsdz.7 fragility penalty).
            guard subject.skipConfidence.isFinite else {
                return Adjustment(
                    id: subject.id,
                    originalSkipConfidence: subject.skipConfidence,
                    adjustedSkipConfidence: subject.skipConfidence,
                    isIsolated: false,
                    isolationPenaltyApplied: false,
                    minDwellPenaltyApplied: false
                )
            }

            // ANTI-CONTAGION: a neighbor supports the subject only if its OWN
            // skipConfidence clears `highConfidenceNeighborThreshold`. The test
            // reads the neighbor's ORIGINAL confidence, so it is independent of
            // the subject's (or any other span's) penalty.
            let hasHighConfNeighbor = sorted.contains { other in
                guard other.id != subject.id else { return false }
                guard other.skipConfidence.isFinite else { return false }
                guard other.skipConfidence >= highConf else { return false }
                return gap(subject, other) <= window
            }

            var adjusted = subject.skipConfidence
            var isolationApplied = false
            var dwellApplied = false

            if !hasHighConfNeighbor {
                // ISOLATION PENALTY: lonely detection ⇒ soft down-weight.
                let next = clampUnit(adjusted * isolationFactor)
                if next != adjusted {
                    adjusted = next
                    isolationApplied = true
                }

                // MINIMUM-DWELL: a too-short, uncorroborated island is
                // additionally down-weighted. Only uncorroborated islands are
                // touched — a short creative inside a real cluster (high-conf
                // neighbor present) keeps full confidence.
                if subject.duration < minDwell {
                    let dwelled = clampUnit(adjusted * dwellFactor)
                    if dwelled != adjusted {
                        adjusted = dwelled
                        dwellApplied = true
                    }
                }
            }

            return Adjustment(
                id: subject.id,
                originalSkipConfidence: subject.skipConfidence,
                adjustedSkipConfidence: adjusted,
                isIsolated: !hasHighConfNeighbor,
                isolationPenaltyApplied: isolationApplied,
                minDwellPenaltyApplied: dwellApplied
            )
        }
    }

    /// Clamp to the unit interval. Non-finite inputs collapse to 0 — but the
    /// caller never feeds a non-finite factor through here for a finite
    /// confidence (factors are config scalars), and a non-finite confidence is
    /// short-circuited above before any multiplication.
    private static func clampUnit(_ value: Double) -> Double {
        guard value.isFinite else { return 0.0 }
        return max(0.0, min(1.0, value))
    }
}
