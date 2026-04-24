// SegmentAggregator.swift
// playhead-gtt9.10: Segment-level candidate aggregator with hysteresis.
//
// Why this exists
// ---------------
// Per-window classifier scoring is inherently local — each N-second window
// sees only its own evidence. But ads are multi-window phenomena: a
// 60 s ad break is built out of 30-60 individually-classified windows, each
// of which may sit just under the promotion threshold individually. On the
// 2026-04-23 dogfood capture the confidence mode was `[0.30, 0.40)` — a
// lot of weak-but-consistent evidence that the old per-window gate
// (threshold = 0.40) discarded entirely.
//
// This file turns the window-score stream into COHERENT SEGMENTS via a
// hysteresis state machine:
//
//     window scores → candidate run → merged segment → promotion gate
//
// The aggregator does NOT change per-window scores, does NOT persist
// AdWindows, and does NOT grant auto-skip authority. It produces
// `AdSegmentCandidate`s whose `promoted` flag is a recommendation for a
// downstream promotion gate (owned by `DecisionMapper` / `SkipPolicyMatrix`);
// gtt9.11 will split detection recall from auto-skip precision and route
// this output through the right gate per layer.
//
// Contract
// --------
// • Input: a list of `WindowScore`s sorted by `startTime` ASC. Unsorted
//   input is a programmer error (enforced via `assert` in debug; undefined
//   behavior in release). Callers that produce windows from parallel Tier 1
//   + Tier 2 streams must sort before calling.
// • Window widths are heterogeneous: Tier 1 uses 30 s slots, Tier 2 lexical
//   candidates are 2 s wide. The aggregator treats every window as an
//   atomic `[startTime, endTime)` interval with a single score, and weighs
//   its contribution to the segment's score by its duration. That matches
//   what a calibrator would want: a 30 s 0.40 window counts more than a
//   2 s 0.40 window.
// • Overlapping windows from different tiers are NOT deduplicated — the
//   caller is expected to pre-merge heterogeneous streams as it sees fit.
//   If two simultaneous start conditions fire inside one run, the segment
//   machine only tracks one active segment at a time (the second firing
//   is absorbed into the existing segment).
//
// segmentScore aggregation
// ------------------------
// Choice: **duration-weighted arithmetic mean** of included window scores.
// Weight = window duration in seconds. No time decay.
//
// Rationale (documented per bead acceptance #2):
//   - Mean (vs max): max over-credits an isolated spike. A 0.90 window
//     surrounded by 0.20s is the C22D6EC6 FP shape — we want that segment
//     to look like 0.30-ish, not 0.90.
//   - Mean (vs median): median ignores weight of an individual strong
//     signal (e.g., a clean sponsor-read spike). Weighted mean respects it
//     without letting it dominate.
//   - Mean (vs sum): sum grows with duration, so long low-evidence
//     segments would promote purely on length. Mean is length-invariant.
//   - Duration-weighting (vs equal weighting): honors the heterogeneous
//     Tier 1 (30 s) / Tier 2 (2 s) mix. Without it, a dense Tier 2 lexical
//     burst could out-vote the surrounding Tier 1 classifier evidence for
//     the same span.
//   - No time decay (vs exponential time-decay): the segment is modeling
//     a contiguous event in wall time, not a recency-biased signal. Decay
//     would bias towards the tail of the segment, which has no semantic
//     justification for ads (beginnings are just as ad-like as endings).
//
// Monotonicity property
// ---------------------
// Adding a window whose score is ≥ the running duration-weighted mean
// cannot decrease the mean. (Adding a window with score ≥ μ and weight w
// gives μ' = (μ·W + s·w) / (W + w) ≥ (μ·W + μ·w) / (W + w) = μ.)
// Conversely, adding a window below the running mean may decrease it —
// that's how the [0.30]-tail can drag a segment below `promotionThreshold`.
// Unit-tested in `segmentScoreMonotonicityWhenAddingAboveMeanWindow`.
//
// Thresholds & magic numbers
// --------------------------
// All starting numbers below come directly from the bead spec. gtt9.3 owns
// calibration; this file MUST NOT retune them.
//
//   candidateThreshold      = 0.35
//     (bead spec; individual windows below this never count toward an
//      N-nearby segment start).
//   continuationThreshold    = 0.28
//     (bead spec; hysteresis — once a segment is open, sub-candidate
//      windows down to this floor keep it alive).
//   promotionThreshold       = 0.40
//     (bead spec; "keep current" — unchanged from existing per-window gate).
//   highConfidenceThreshold  = 0.60
//     (proposed; a single window this strong is rare enough to be treated
//      as seed evidence on its own. 0.60 matches the existing
//      `markOnlyThreshold` in AdDetectionConfig, which already classifies
//      0.60+ as "likely sponsor segment" territory.).
//   N (nNearbyWindowsForStart) = 2
//     (proposed; two independent sub-threshold windows close in time is
//      the weakest corroboration worth acting on. N=1 degenerates to per-
//      window gating — exactly what this bead is solving. N=3 is too
//      conservative given Tier 1's 30 s slot width: three adjacent 30 s
//      windows is 90 s of required corroboration, excluding most short ads.).
//   nearbyWindowSecondsForStart = 90.0 s
//     (proposed; "nearby" for the N-window start criterion is a wall-
//      clock window matching `typicalAdDuration.upperBound` (90 s).
//      Rationale: two candidate-strength windows within a single ad-
//      duration's span are corroboration for that ad. This is explicitly
//      LARGER than `maxInternalGapSeconds` because start corroboration
//      reasons about the ad-scale region, while mid-segment continuation
//      reasons about momentary evidence dips. DF5C1832 on 2026-04-23
//      had spikes 64 s apart inside a single ad — with 5 s nearby they
//      wouldn't corroborate; with 90 s nearby they do.).
//   M (belowContinuationSecondsToEnd) = 3.0 s
//     (proposed; ~1-2 windows of below-continuation evidence ends a
//      segment. Shorter would close too aggressively on normal speech
//      gaps; longer would glue unrelated segments together.).
//   maxInternalGapSeconds    = 5.0 s
//     (proposed; clock gaps up to 5 s inside a candidate run are bridged.
//      This covers the common case of a single silent break between two
//      sponsor reads inside one ad break, while stopping well short of
//      merging two adjacent-but-distinct ad pods. Note: once a segment
//      is OPEN, continuation windows ≥ `continuationThreshold` keep it
//      alive regardless of maxInternalGapSeconds — the gap rule only
//      polices *silent* regions with no window at all in them. On the
//      DF5C1832 shape, the 0.30-baseline windows between the two spikes
//      all exceed 0.28 continuation, so they keep the segment alive.).
//   minAdDurationSeconds     = 30.0 s
//     (matches `GlobalPriorDefaults.standard.typicalAdDuration.lowerBound`.
//      Reusing the existing prior keeps the two knobs in sync; no new
//      constant introduced.).

import Foundation

// MARK: - Config

/// Hysteresis configuration for `SegmentAggregator`.
///
/// All defaults are sourced from the gtt9.10 bead spec. See the file header
/// comment for rationale on each value. Calibration of these is gtt9.3's
/// job and MUST NOT be retuned here.
struct SegmentAggregatorConfig: Sendable, Equatable {

    /// Minimum per-window score that counts as candidate-strength evidence
    /// toward an N-nearby segment start. Spec default: 0.35.
    let candidateThreshold: Double

    /// While a segment is open, windows down to this floor continue the
    /// segment. Below this floor contributes to the end countdown. Spec
    /// default: 0.28.
    let continuationThreshold: Double

    /// A segment's duration-weighted mean score must be ≥ this to earn
    /// `promoted = true`. Spec default: 0.40 (unchanged from the existing
    /// per-window gate).
    let promotionThreshold: Double

    /// Single-window score that can seed a segment on its own, bypassing
    /// the N-nearby start requirement. Proposed: 0.60 (matches
    /// `AdDetectionConfig.default.markOnlyThreshold`).
    let highConfidenceThreshold: Double

    /// How many ≥ `candidateThreshold` windows within
    /// `nearbyWindowSecondsForStart` of each other are required to open a
    /// segment via the corroboration branch. Proposed: 2.
    let nNearbyWindowsForStart: Int

    /// Wall-clock window (seconds) within which `nNearbyWindowsForStart`
    /// candidate windows corroborate each other for start purposes. This
    /// is deliberately larger than `maxInternalGapSeconds`: start
    /// corroboration reasons about the ad-scale region, continuation
    /// reasons about momentary evidence gaps. Proposed: 90.0 s (matches
    /// `typicalAdDuration.upperBound`).
    let nearbyWindowSecondsForStart: Double

    /// Cumulative seconds of below-continuation evidence required to close
    /// an open segment. Proposed: 3.0 s.
    let belowContinuationSecondsToEnd: Double

    /// Maximum clock gap (seconds) between windows that the continuation
    /// state machine will bridge when there is NO window at all in that
    /// gap. Proposed: 5.0 s. (Continuation-threshold windows in between
    /// simply extend the segment — this only polices silent regions.)
    let maxInternalGapSeconds: Double

    /// A segment whose duration is below this does not promote, even when
    /// its segmentScore clears `promotionThreshold`. Matches
    /// `GlobalPriorDefaults.standard.typicalAdDuration.lowerBound` (30 s).
    let minAdDurationSeconds: Double

    /// Canonical defaults from the bead spec.
    static let `default` = SegmentAggregatorConfig(
        candidateThreshold: 0.35,
        continuationThreshold: 0.28,
        promotionThreshold: 0.40,
        highConfidenceThreshold: 0.60,
        nNearbyWindowsForStart: 2,
        nearbyWindowSecondsForStart: GlobalPriorDefaults.standard.typicalAdDuration.upperBound,
        belowContinuationSecondsToEnd: 3.0,
        maxInternalGapSeconds: 5.0,
        minAdDurationSeconds: GlobalPriorDefaults.standard.typicalAdDuration.lowerBound
    )
}

// MARK: - Output

/// A merged candidate-run that the aggregator has identified from the
/// per-window score stream. `promoted` is this aggregator's recommendation
/// for promotion. Downstream gates (`DecisionMapper` / `SkipPolicyMatrix`)
/// retain final authority over auto-skip; this struct only reports what
/// the aggregator itself considers eligible.
struct AdSegmentCandidate: Sendable, Equatable {
    /// Segment start in episode-relative seconds (earliest included
    /// window's `startTime`).
    let startTime: Double
    /// Segment end in episode-relative seconds (latest included window's
    /// `endTime` — snapped back to the last ≥ continuation window, NOT
    /// the end of a trailing below-continuation tail).
    let endTime: Double
    /// Duration-weighted mean of per-window scores (see file header
    /// comment for rationale).
    let segmentScore: Double
    /// Number of input windows included in this segment.
    let windowCount: Int
    /// `true` iff `segmentScore >= promotionThreshold` AND
    /// `(endTime - startTime) >= minAdDurationSeconds`. Safety signals
    /// (gtt9.11) are NOT evaluated here — callers integrate them.
    let promoted: Bool
}

// MARK: - Aggregator

/// Pure, stateless segment aggregator.
///
/// Call `SegmentAggregator.aggregate(windows:config:)` with a sorted stream
/// of per-window scores. Returns the set of merged candidate segments
/// discovered by a single left-to-right pass of the hysteresis state
/// machine.
enum SegmentAggregator {

    /// One per-window score. The aggregator is intentionally decoupled
    /// from `DecisionLogEntry` so it can be unit-tested without any
    /// pipeline wiring; callers map their own decision-stream element to
    /// this struct.
    struct WindowScore: Sendable, Equatable {
        let startTime: Double
        let endTime: Double
        let score: Double

        /// Duration of this window in seconds. Guarded against 0-width
        /// inputs — a 0-width window is treated as contributing no
        /// weight to the segment's duration-weighted mean.
        var durationSeconds: Double {
            max(0.0, endTime - startTime)
        }
    }

    /// Aggregate a sorted stream of window scores into merged candidate
    /// segments. Non-allocating in the hot path: one output per emitted
    /// segment.
    ///
    /// - Parameters:
    ///   - windows: Per-window scores, sorted by `startTime` ASC. Unsorted
    ///     input is a programmer error (asserts in debug, undefined
    ///     behavior in release).
    ///   - config: Hysteresis configuration. Usually `.default`.
    /// - Returns: Merged segments discovered by one left-to-right pass.
    static func aggregate(
        windows: [WindowScore],
        config: SegmentAggregatorConfig = .default
    ) -> [AdSegmentCandidate] {
        guard !windows.isEmpty else { return [] }

        // Sorted-input contract. Debug-only check — keeps hot-path cost
        // at zero in release.
        #if DEBUG
        for i in 1..<windows.count {
            assert(windows[i - 1].startTime <= windows[i].startTime,
                   "SegmentAggregator.aggregate requires windows sorted by startTime ASC")
        }
        #endif

        var segments: [AdSegmentCandidate] = []
        var state = MachineState(config: config)

        for window in windows {
            state.ingest(window)
            if let finished = state.takeFinishedSegment() {
                segments.append(finished)
            }
        }
        // Stream ended while a segment was still open — flush it.
        if let tail = state.flushOpenSegment() {
            segments.append(tail)
        }
        return segments
    }

    // MARK: - State machine

    /// Internal state of the single-segment-at-a-time hysteresis machine.
    /// An instance processes windows left-to-right; on each `ingest`, at
    /// most one segment may finish. `takeFinishedSegment()` drains that
    /// output slot.
    private struct MachineState {

        /// One accumulator for an open (or pending-start) segment.
        struct OpenSegment {
            /// `startTime` of the earliest included window.
            var startTime: Double
            /// `endTime` of the most recently included window that was
            /// ≥ `continuationThreshold`. The emitted `endTime` snaps to
            /// this — NOT to a trailing below-continuation tail that has
            /// not yet triggered the end countdown.
            var lastQualifyingEndTime: Double
            /// Sum of (score · duration) over all included windows.
            var weightedScoreSum: Double = 0
            /// Sum of durations of all included windows. Denominator for
            /// the duration-weighted mean.
            var totalDuration: Double = 0
            /// Count of included windows.
            var windowCount: Int = 0
            /// Rolling cumulative seconds of below-continuation evidence
            /// since the last qualifying window. Resets on any
            /// ≥ continuation window. Closes the segment when ≥
            /// `belowContinuationSecondsToEnd`.
            var belowContinuationSeconds: Double = 0

            mutating func include(_ w: WindowScore) {
                weightedScoreSum += w.score * w.durationSeconds
                totalDuration += w.durationSeconds
                windowCount += 1
            }

            var meanScore: Double {
                // Numerical guard: duration-weighted mean is undefined for
                // a zero-duration window list (can happen if every input
                // has startTime == endTime). Defer to 0.0 — such a segment
                // will never clear `promotionThreshold` so it's safe.
                totalDuration > 0 ? weightedScoreSum / totalDuration : 0.0
            }
        }

        let config: SegmentAggregatorConfig
        /// Trailing buffer of "pending start" candidate-threshold windows.
        /// When this grows to `nNearbyWindowsForStart` AND all of them
        /// are within `nearbyWindowSecondsForStart` of each other, a
        /// segment opens and absorbs them (plus the continuation-grade
        /// context buffered between them). A window ≥
        /// `highConfidenceThreshold` short-circuits this and opens a
        /// segment on its own.
        private var pendingStarts: [WindowScore] = []
        /// Buffer of continuation-strength windows seen while idle. When
        /// an N-nearby cluster fires, the segment is back-dated to include
        /// both the candidate seeds AND every continuation-strength window
        /// between them — that's what makes a 0.30-baseline run (ad bed
        /// evidence) count toward the segment even though none of the
        /// individual 0.30 windows was strong enough to seed a start.
        /// Entries outside the ad-scale nearby window of the most recent
        /// pending start are evicted on every ingest to keep the buffer
        /// bounded.
        private var pendingContext: [WindowScore] = []
        /// Currently-open segment accumulator, or nil.
        private var open: OpenSegment?
        /// Slot holding the most-recently finished segment. Drained by
        /// `takeFinishedSegment()`.
        private var finishedSlot: AdSegmentCandidate?

        init(config: SegmentAggregatorConfig) {
            self.config = config
        }

        /// Public: take (and clear) the last-finished segment, if any.
        mutating func takeFinishedSegment() -> AdSegmentCandidate? {
            defer { finishedSlot = nil }
            return finishedSlot
        }

        /// Public: flush an open segment at end-of-stream.
        mutating func flushOpenSegment() -> AdSegmentCandidate? {
            guard let seg = open else { return nil }
            open = nil
            return Self.materialize(segment: seg, config: config)
        }

        /// Public: consume one window.
        mutating func ingest(_ w: WindowScore) {
            if open != nil {
                ingestIntoOpenSegment(w)
            } else {
                ingestWhileIdle(w)
            }
        }

        // MARK: private

        /// While no segment is open, decide whether `w` opens one —
        /// either by itself (high-confidence branch) or by completing an
        /// N-nearby cluster of candidate windows.
        ///
        /// Two buffers run in parallel while idle:
        ///   - `pendingStarts`: candidate-strength (≥ candidateThreshold)
        ///     seeds that, at N of them, fire an open.
        ///   - `pendingContext`: continuation-strength (≥ continuation)
        ///     windows that back-fill the segment when an open fires —
        ///     this is what lets a long run of 0.30 ad-bed evidence
        ///     bordered by two isolated 0.45 / 0.46 spikes coalesce into
        ///     one coherent DF5C1832-shaped segment.
        private mutating func ingestWhileIdle(_ w: WindowScore) {
            // Branch 1: single high-confidence window opens a segment.
            if w.score >= config.highConfidenceThreshold {
                // Discard pending state — this window anchors on its own.
                pendingStarts.removeAll(keepingCapacity: true)
                pendingContext.removeAll(keepingCapacity: true)
                openSegment(startingWith: [w], context: [])
                return
            }

            // Evict both buffers outside the ad-scale nearby window. Uses
            // the incoming window's startTime as the "now" edge — any
            // buffered entry ending more than `nearbyWindowSecondsForStart`
            // ago is no longer near this window.
            pendingStarts.removeAll { pending in
                (w.startTime - pending.endTime) > config.nearbyWindowSecondsForStart
            }
            pendingContext.removeAll { pending in
                (w.startTime - pending.endTime) > config.nearbyWindowSecondsForStart
            }

            // Branch 2: N-nearby candidate windows.
            if w.score >= config.candidateThreshold {
                pendingStarts.append(w)
                if pendingStarts.count >= config.nNearbyWindowsForStart {
                    let seeds = pendingStarts
                    let ctx = pendingContext
                    pendingStarts.removeAll(keepingCapacity: true)
                    pendingContext.removeAll(keepingCapacity: true)
                    openSegment(startingWith: seeds, context: ctx)
                    return
                }
                // `w` is also continuation-grade; keep it in context too
                // so a later cluster that drops the first pending seed
                // can still leverage it.
                pendingContext.append(w)
                return
            }

            // Below candidate, but possibly ≥ continuation — buffer it as
            // context that will be swept up if an open fires later. Only
            // windows ≥ continuation carry signal; strict-below-continuation
            // windows are discarded (they are noise-grade).
            if w.score >= config.continuationThreshold {
                pendingContext.append(w)
            }
        }

        /// While a segment is open, include the window (if it meets the
        /// continuation criterion or bridges a tolerable gap) and track
        /// the below-continuation countdown toward segment end.
        private mutating func ingestIntoOpenSegment(_ w: WindowScore) {
            guard var seg = open else { return }

            // Compute the clock gap from the last qualifying window's end.
            let gap = max(0.0, w.startTime - seg.lastQualifyingEndTime)

            if w.score >= config.continuationThreshold {
                // Qualifying window — continues the segment and resets
                // the below-continuation countdown. Reset applies even
                // after a sub-threshold gap, provided the gap is within
                // `maxInternalGapSeconds` (otherwise the segment should
                // have closed already — see the gap branch below).
                if gap > config.maxInternalGapSeconds {
                    // Gap too large to bridge. Close the open segment and
                    // then treat `w` as a fresh ingest from idle state.
                    finishedSlot = Self.materialize(segment: seg, config: config)
                    open = nil
                    ingestWhileIdle(w)
                    return
                }
                seg.include(w)
                seg.lastQualifyingEndTime = w.endTime
                seg.belowContinuationSeconds = 0
                open = seg
                return
            }

            // Below continuation. Include the window in the score
            // (it's still evidence about the region), but advance the
            // end countdown. Note: the emitted endTime still snaps to
            // `lastQualifyingEndTime`, so sub-continuation tail does
            // NOT extend the reported segment boundary.
            seg.include(w)
            seg.belowContinuationSeconds += w.durationSeconds

            if gap > config.maxInternalGapSeconds ||
               seg.belowContinuationSeconds >= config.belowContinuationSecondsToEnd {
                // End condition met. Close the segment.
                finishedSlot = Self.materialize(segment: seg, config: config)
                open = nil
                // A sub-continuation window cannot itself re-seed a new
                // segment; no need to call ingestWhileIdle with `w`.
            } else {
                open = seg
            }
        }

        /// Seed a new open segment with the given starting seeds PLUS any
        /// continuation-grade context windows that fall between the
        /// earliest seed and the latest seed. Both seeds and context are
        /// included in the segment's duration-weighted mean.
        ///
        /// Rationale: the N-nearby branch can fire with two candidate
        /// spikes spanning tens of seconds (e.g., DF5C1832's 1612 / 1676).
        /// Ignoring the ~63 intermediate continuation-grade windows would
        /// produce a tiny segment anchored on the two spikes alone, whose
        /// segmentScore reflects only the spikes, not the full ad-bed
        /// region they corroborate.
        private mutating func openSegment(
            startingWith seeds: [WindowScore],
            context: [WindowScore]
        ) {
            guard let first = seeds.first else { return }
            var seg = OpenSegment(
                startTime: first.startTime,
                lastQualifyingEndTime: first.endTime
            )
            // Merge seeds + context, sort by startTime (context and seeds
            // may interleave), then include in order. Deduplicate on
            // (startTime,endTime,score) to avoid double-counting a seed
            // that was also buffered as context.
            var combined = seeds + context
            combined.sort { $0.startTime < $1.startTime }
            var lastIncludedStart: Double = -.infinity
            var lastIncludedEnd: Double = -.infinity
            var lastIncludedScore: Double = -.infinity
            for w in combined {
                if w.startTime == lastIncludedStart
                    && w.endTime == lastIncludedEnd
                    && w.score == lastIncludedScore {
                    continue
                }
                seg.include(w)
                if w.endTime > seg.lastQualifyingEndTime {
                    seg.lastQualifyingEndTime = w.endTime
                }
                if w.startTime < seg.startTime {
                    seg.startTime = w.startTime
                }
                lastIncludedStart = w.startTime
                lastIncludedEnd = w.endTime
                lastIncludedScore = w.score
            }
            open = seg
        }

        // MARK: materialize

        private static func materialize(
            segment seg: OpenSegment,
            config: SegmentAggregatorConfig
        ) -> AdSegmentCandidate {
            let mean = seg.meanScore
            let endTime = seg.lastQualifyingEndTime
            let duration = endTime - seg.startTime
            let promoted = mean >= config.promotionThreshold
                && duration >= config.minAdDurationSeconds
            return AdSegmentCandidate(
                startTime: seg.startTime,
                endTime: endTime,
                segmentScore: mean,
                windowCount: seg.windowCount,
                promoted: promoted
            )
        }
    }
}
