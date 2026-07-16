// StingerRefiner.swift
// playhead-l2f.6: stinger-anchored boundary refinement — pure matching
// logic.
//
// Design (mirrors the FineBoundaryRefiner / SpanFinalizer conventions):
//   • Pure, stateless — same inputs always produce the same output. All
//     audio access happens in the caller (`AdDetectionService` reads the
//     persisted 16 kHz analysis-shard PCM; tests hand in synthetic
//     envelopes).
//   • Per-window contract: exactly one refined window out per window in —
//     the refiner NEVER splits or merges windows, and the clamp step makes
//     `end <= start` structurally impossible.
//   • The algorithm is the production port (v4, playhead-xsdz.38) of the
//     offline JOINT recipe `refine_edges_joint` in
//     `scripts/l2f-boundary-stinger-prototype.py` (commit 5dc1b961,
//     JointConfig defaults = the J-q08 sweep winner): instead of
//     independent per-edge argmax snaps, enumerate candidate (start, end)
//     pairs — every qualifying NCC local maximum per edge (per-show
//     confidence gate `max(0.50, learning_confidence - 0.15)`, 75 s move
//     cap), grid-derived partners at k·grid from confident (≥ 0.65) peaks
//     (k capped at the show's learned max pod multiple), and the untouched
//     proposal edge — then pick the highest-scoring feasible pair:
//       score = peak sum
//             + 0.5 on-grid bonus     (grid shows; both edges evidence-backed)
//             − 0.08 · gridDistance · widening / 75   (grid shows, OFF-grid)
//             − 1e-4 · movement       (pure tie-break)
//     The no-snap pair scores exactly 0, so any pair driven negative by the
//     off-grid penalty loses to leaving the proposal alone (this is what
//     kills the morbid-05-29-class ~38 s content eat). Pairs that abandon
//     overlap with the proposal are infeasible; if the clamped result still
//     abandons overlap, BOTH edges revert (belt-and-braces).

import Foundation

// MARK: - StingerPCMSlice

/// A ranged slice of decoded 16 kHz mono PCM handed to the envelope
/// computation. `startSeconds` anchors `samples[0]` on the episode
/// timeline.
struct StingerPCMSlice: Sendable {
    let samples: [Float]
    let startSeconds: Double
}

// MARK: - StingerSearchEnvelope

/// A 50 Hz log-RMS envelope over a search span. `startSeconds` anchors
/// `values[0]` on the episode timeline.
struct StingerSearchEnvelope: Sendable {
    let values: [Float]
    let startSeconds: Double
}

// MARK: - StingerEnvelope

/// 50 Hz log-RMS envelope computation — the runtime twin of the offline
/// extraction in `scripts/l2f-boundary-stinger-prototype.py` (`log1p(rms *
/// 100)` over 20 ms hops), so bundled templates and runtime search
/// envelopes live in the same acoustic space.
enum StingerEnvelope {
    /// Frames per second. Must match `StingerBank.requiredEnvelopeHz`.
    static let envelopeHz = 50

    /// Compute the envelope of 16 kHz mono PCM. A trailing partial hop is
    /// dropped (same as the offline extraction). Empty result when fewer
    /// samples than one hop.
    static func compute(
        samples: [Float],
        sampleRate: Int = StingerBank.requiredPCMSampleRate
    ) -> [Float] {
        let hop = sampleRate / envelopeHz
        guard hop > 0, samples.count >= hop else { return [] }
        let frameCount = samples.count / hop
        var envelope = [Float]()
        envelope.reserveCapacity(frameCount)
        for frame in 0..<frameCount {
            let base = frame * hop
            var sumSquares = 0.0
            for i in base..<(base + hop) {
                let sample = Double(samples[i])
                sumSquares += sample * sample
            }
            let rms = (sumSquares / Double(hop)).squareRoot()
            envelope.append(Float(log1p(rms * 100.0)))
        }
        return envelope
    }
}

// MARK: - StingerRefinementTrace

/// Per-window refinement trace. Mirrors the offline prototype's trace dict
/// so the Catalyst dump and the gold scorer can attribute movement to the
/// same fields the spike reported. Deliberately NOT Codable: the dump wire
/// shape is owned by the dedicated `DumpStingerRefinement` mirror in
/// `PipelineDumpLiveTests`, so a trace refactor breaks there first instead
/// of silently rewriting the dump schema.
struct StingerRefinementTrace: Sendable, Equatable {
    /// The break-start edge snapped to a qualifying pre-stinger peak
    /// (the chosen pair's start candidate is a peak).
    var startSnapped = false
    /// The break-end edge snapped to a qualifying post-stinger peak
    /// (the chosen pair's end candidate is a peak).
    var endSnapped = false
    /// The chosen pair used a grid-DERIVED candidate on either edge
    /// (v4 joint semantics — mirrors the oracle's `grid_applied`).
    var gridApplied = false
    /// Refinement abandoned overlap with the proposal — both edges
    /// reverted (snap/grid flags are cleared when this is set).
    var revertedNoOverlap = false
    /// NCC peak accepted on the start side (recorded only on snap).
    var startPeak: Double?
    /// NCC peak accepted on the end side (recorded only on snap).
    var endPeak: Double?
    /// Applied start movement in seconds (`refined - proposal`); nil when
    /// the start edge is unchanged.
    var startDeltaSeconds: Double?
    /// Applied end movement in seconds (`refined - proposal`); nil when
    /// the end edge is unchanged.
    var endDeltaSeconds: Double?
    /// playhead-xsdz.38: evidence candidates enumerated for the start edge
    /// (qualifying NCC maxima + grid-derived; the untouched proposal edge
    /// is not counted). `nil` when zero — a flag-ON consult with no
    /// evidence anywhere leaves the trace pristine, preserving the
    /// OFF-vs-no-snap distinction the wire-in tests pin.
    var startCandidateCount: Int?
    /// playhead-xsdz.38: evidence candidates enumerated for the end edge.
    /// Same conventions as `startCandidateCount`.
    var endCandidateCount: Int?
    /// playhead-xsdz.38: the chosen pair's joint score (rounded to 1e-4,
    /// banker's rounding like the oracle's `round(score, 4)`). Recorded
    /// whenever any evidence candidate existed — 0.0 means the no-snap
    /// floor won (e.g. the eat-class candidate was driven negative). `nil`
    /// when no evidence candidates existed or no pair was feasible.
    var pairScore: Double?
    /// playhead-xsdz.38: which grid term the chosen pair's score carried —
    /// `"bonus"` (on-grid pair, both edges evidence-backed) or `"penalty"`
    /// (off-grid pair paying a nonzero inconsistency penalty). `nil` when
    /// neither term moved the score.
    var gridTermApplied: String?
}

// MARK: - StingerRefiner

enum StingerRefiner {
    /// Search span half-width around each proposed edge.
    static let searchRadiusSeconds = 90.0
    /// Derived-anchor gate (oracle `GRID_MIN_PEAK`): grid-derived
    /// candidates may only anchor on a peak at least this confident. The
    /// v2 production lesson (barely-gated grid anchors breached the 90 s
    /// false-widening budget) held in joint form too: the xsdz.38 sweep
    /// showed lowering this to 0.50 lets the 0.525 eat peak derive its own
    /// on-grid partner and resurrect the content eat.
    static let gridMinimumPeak = 0.65
    /// Refuse snaps that move an edge farther than this from the proposal.
    static let maxEdgeMoveSeconds = 75.0
    /// Refined windows keep at least this width (clamp floor); combined
    /// with the ordering of the clamps this makes `end <= start`
    /// impossible. Doubles as the oracle's minimum pair width
    /// (`e.time - s.time < 1.0` ⇒ infeasible).
    static let minimumRefinedWidthSeconds = 1.0

    // MARK: v4 joint constants (oracle `JointConfig` defaults — the J-q08
    // xsdz.38 sweep winner; see playhead-baselines/
    // xsdz38-joint-sweep-20260716.md). No per-show hand-tuning anywhere:
    // everything per-show is learned (grid, max multiple, gates, offsets).

    /// A pair width within this of a positive grid multiple counts as
    /// on-grid (oracle `GRID_SNAP_TOLERANCE`).
    static let gridSnapToleranceSeconds = 3.0
    /// On-grid pair bonus (oracle `grid_bonus = SNAP_NCC_FLOOR`): an
    /// on-grid partner is worth as much as a gate-floor acoustic peak.
    /// Scoped to pairs where BOTH edges carry evidence — never the
    /// untouched proposal edge, whose width against a snap is proposal
    /// noise, not structure (oracle `grid_bonus_scope = "both"`).
    static let gridBonus = 0.50
    /// The eat killer (oracle `grid_inconsistency_rate`): off-grid pairs
    /// pay `rate * gridDistance * widening / moveCap`. Plateau-stable
    /// across 0.05–0.20 in the sweep; 0.08 is the recorded winner.
    static let gridInconsistencyRate = 0.08
    /// Pure movement tie-break (oracle `JOINT_TIEBREAK_MOVE_RATE`).
    static let jointTiebreakMoveRate = 1e-4

    struct Result: Sendable, Equatable {
        let startTime: Double
        let endTime: Double
        let trace: StingerRefinementTrace
    }

    /// Refine one candidate window against the show's bank entry.
    ///
    /// Direct port of the oracle's `refine_edges_joint` at `JointConfig`
    /// defaults, including its candidate enumeration order and the strict
    /// lexicographic best-key comparison `(score, -moved, sTime, eTime)`,
    /// so exact ties resolve to the SAME pair the oracle picks.
    ///
    /// - Parameters:
    ///   - proposalStart/proposalEnd: the pipeline's current window bounds
    ///     (post acoustic snap — the refiner runs inside the existing
    ///     boundary-refinement block).
    ///   - entry: the show's bank entry (caller resolved it; no entry ⇒
    ///     caller never calls).
    ///   - startEnvelope/endEnvelope: 50 Hz search envelopes around each
    ///     edge; nil when the side has no template or no PCM was
    ///     available (that side simply cannot contribute peak candidates).
    ///   - episodeDuration: clamp ceiling + derived-candidate bound.
    static func refine(
        proposalStart: Double,
        proposalEnd: Double,
        entry: StingerShowEntry,
        startEnvelope: StingerSearchEnvelope?,
        endEnvelope: StingerSearchEnvelope?,
        episodeDuration: Double
    ) -> Result {
        var trace = StingerRefinementTrace()
        guard episodeDuration > 2 * minimumRefinedWidthSeconds,
              proposalEnd > proposalStart,
              proposalStart.isFinite, proposalEnd.isFinite else {
            return Result(startTime: proposalStart, endTime: proposalEnd, trace: trace)
        }

        // Candidate enumeration. Order mirrors the oracle exactly: the
        // untouched proposal edge first, then qualifying NCC maxima in
        // ascending time, then grid-derived candidates (anchor order ×
        // ascending k, first-insertion dedupe).
        var startCandidates: [EdgeCandidate] = [
            EdgeCandidate(time: proposalStart, peak: 0.0, kind: .none)
        ]
        var endCandidates: [EdgeCandidate] = [
            EdgeCandidate(time: proposalEnd, peak: 0.0, kind: .none)
        ]
        if let template = entry.pre, let envelope = startEnvelope {
            startCandidates.append(contentsOf: peakCandidates(
                template: template, envelope: envelope, proposalEdge: proposalStart
            ))
        }
        if let template = entry.post, let envelope = endEnvelope {
            endCandidates.append(contentsOf: peakCandidates(
                template: template, envelope: envelope, proposalEdge: proposalEnd
            ))
        }

        let grid = entry.podWidthGridSeconds
        let maxMultiple = entry.gridMaxPodMultiple
        if let grid {
            // Start deriveds come from the CURRENT end candidates and vice
            // versa; derived anchors are peak-kind only, so the freshly
            // appended start deriveds never anchor end deriveds (same
            // two-step order as the oracle).
            startCandidates.append(contentsOf: derivedCandidates(
                anchors: endCandidates, grid: grid, maxMultiple: maxMultiple,
                direction: -1.0, center: proposalStart, episodeDuration: episodeDuration
            ))
            endCandidates.append(contentsOf: derivedCandidates(
                anchors: startCandidates, grid: grid, maxMultiple: maxMultiple,
                direction: 1.0, center: proposalEnd, episodeDuration: episodeDuration
            ))
        }

        let startEvidenceCount = startCandidates.count - 1
        let endEvidenceCount = endCandidates.count - 1
        if startEvidenceCount > 0 { trace.startCandidateCount = startEvidenceCount }
        if endEvidenceCount > 0 { trace.endCandidateCount = endEvidenceCount }

        // Pair scoring: pick the highest-scoring feasible (start, end)
        // pair under the strict lexicographic key (score, -moved, sTime,
        // eTime) — identical iteration order and tie behavior to the
        // oracle's `key > best_key`.
        var bestKey: (Double, Double, Double, Double)?
        var bestPair: (start: EdgeCandidate, end: EdgeCandidate)?
        var bestGridTerm: String?
        for startCandidate in startCandidates {
            for endCandidate in endCandidates {
                if endCandidate.time - startCandidate.time < minimumRefinedWidthSeconds {
                    continue
                }
                // Derived candidates must anchor on a real partner peak; a
                // derived-vs-derived pair would be structure hallucinated
                // from structure.
                if startCandidate.kind == .derived && endCandidate.kind != .peak {
                    continue
                }
                if endCandidate.kind == .derived && startCandidate.kind != .peak {
                    continue
                }
                // Feasibility mirrors the revert guard: refinement must
                // not abandon the presence evidence.
                let pairOverlap = min(endCandidate.time, proposalEnd)
                    - max(startCandidate.time, proposalStart)
                if pairOverlap <= 0 { continue }

                var moved = 0.0
                if startCandidate.kind != .none {
                    moved += abs(startCandidate.time - proposalStart)
                }
                if endCandidate.kind != .none {
                    moved += abs(endCandidate.time - proposalEnd)
                }
                var score = startCandidate.peak + endCandidate.peak
                let hasPeak = startCandidate.kind == .peak || endCandidate.kind == .peak
                var gridTerm: String?
                if let grid {
                    let width = endCandidate.time - startCandidate.time
                    // `.toNearestOrEven` matches the oracle's banker's-
                    // rounding `round(width / grid)`.
                    var multiple = max(1.0, (width / grid).rounded(.toNearestOrEven))
                    if let maxMultiple {
                        // Widths beyond the show's largest observed pod are
                        // off-grid by construction: an uncapped bonus would
                        // stitch neighboring breaks' stingers into one
                        // super-window.
                        multiple = min(multiple, Double(maxMultiple))
                    }
                    let gridDistance = abs(width - multiple * grid)
                    let onGrid = gridDistance <= gridSnapToleranceSeconds
                    if onGrid && hasPeak
                        && startCandidate.kind != .none && endCandidate.kind != .none {
                        score += gridBonus
                        gridTerm = "bonus"
                    }
                    if !onGrid && hasPeak {
                        let widening = max(0.0, proposalStart - startCandidate.time)
                            + max(0.0, endCandidate.time - proposalEnd)
                        let penalty = gridInconsistencyRate * gridDistance
                            * widening / maxEdgeMoveSeconds
                        score -= penalty
                        if penalty > 0 { gridTerm = "penalty" }
                    }
                }
                score -= jointTiebreakMoveRate * moved
                let key = (score, -moved, startCandidate.time, endCandidate.time)
                if bestKey == nil || key > bestKey! {
                    bestKey = key
                    bestPair = (startCandidate, endCandidate)
                    bestGridTerm = gridTerm
                }
            }
        }

        guard let bestPair, let bestKey else {
            // No feasible pair (e.g. a sub-1s proposal with no candidates):
            // leave the proposal alone.
            return Result(startTime: proposalStart, endTime: proposalEnd, trace: trace)
        }

        var newStart = bestPair.start.time
        var newEnd = bestPair.end.time
        trace.startSnapped = bestPair.start.kind == .peak
        trace.endSnapped = bestPair.end.kind == .peak
        trace.gridApplied = bestPair.start.kind == .derived || bestPair.end.kind == .derived
        if bestPair.start.kind == .peak {
            trace.startPeak = roundToMillis(bestPair.start.peak)
        }
        if bestPair.end.kind == .peak {
            trace.endPeak = roundToMillis(bestPair.end.peak)
        }
        if startEvidenceCount > 0 || endEvidenceCount > 0 {
            trace.pairScore = roundToTenThousandths(bestKey.0)
            trace.gridTermApplied = bestGridTerm
        }

        // Clamp to the episode. Order is load-bearing: the end clamp's
        // `max(newStart + minimumRefinedWidthSeconds, …)` floor runs after
        // the start clamp, so `end > start` holds unconditionally.
        newStart = max(0.0, min(newStart, episodeDuration - minimumRefinedWidthSeconds))
        newEnd = max(newStart + minimumRefinedWidthSeconds, min(newEnd, episodeDuration))

        // Revert guard: refinement must not abandon the presence evidence.
        // Pair feasibility already enforces overlap on the raw pair; the
        // clamps cannot break it for in-range proposals, but out-of-range
        // inputs (proposal beyond the episode) can — belt-and-braces, same
        // as the oracle. Zero overlap reverts BOTH edges (a one-sided keep
        // would fabricate a window no evidence supports).
        let overlap = min(newEnd, proposalEnd) - max(newStart, proposalStart)
        if overlap <= 0 {
            trace.revertedNoOverlap = true
            trace.startSnapped = false
            trace.endSnapped = false
            trace.gridApplied = false
            return Result(startTime: proposalStart, endTime: proposalEnd, trace: trace)
        }

        if newStart != proposalStart {
            trace.startDeltaSeconds = roundToMillis(newStart - proposalStart)
        }
        if newEnd != proposalEnd {
            trace.endDeltaSeconds = roundToMillis(newEnd - proposalEnd)
        }
        return Result(startTime: newStart, endTime: newEnd, trace: trace)
    }

    // MARK: - Candidate enumeration

    private enum EdgeCandidateKind {
        /// The untouched proposal edge (always feasible fallback).
        case none
        /// A qualifying NCC local maximum.
        case peak
        /// A grid-derived partner (k·grid from a confident peak anchor).
        case derived
    }

    private struct EdgeCandidate {
        let time: Double
        let peak: Double
        let kind: EdgeCandidateKind
    }

    /// Peak candidates for one edge: every qualifying local maximum of the
    /// NCC curve (≥ the per-show gate) whose snapped time honors the move
    /// cap. Port of the oracle's `joint_peak_candidates`.
    private static func peakCandidates(
        template: StingerTemplate,
        envelope: StingerSearchEnvelope,
        proposalEdge: Double
    ) -> [EdgeCandidate] {
        guard let curve = normalizedCrossCorrelationCurve(
            template: template.template,
            target: envelope.values
        ) else { return [] }
        var candidates: [EdgeCandidate] = []
        for (index, value) in nccQualifyingMaxima(curve: curve, gate: template.snapGate) {
            // Divide (not multiply by a reciprocal) — bitwise-identical to
            // the oracle's `(offset + edge_sample) / ENVELOPE_HZ`.
            let snapped = envelope.startSeconds
                + Double(index + template.edgeSampleIndex) / Double(StingerEnvelope.envelopeHz)
                + template.edgeOffsetSeconds
            if abs(snapped - proposalEdge) <= maxEdgeMoveSeconds {
                candidates.append(EdgeCandidate(time: snapped, peak: value, kind: .peak))
            }
        }
        return candidates
    }

    /// Grid-derived candidates for one edge: k·grid from each confident
    /// (≥ `gridMinimumPeak`) partner-edge peak, k capped at the show's
    /// observed pod multiple and bounded by the search reach. Zero peak
    /// contribution — they earn their place only through the pair's grid
    /// consistency. Port of the oracle's `_joint_derived_candidates`
    /// (including the first-insertion millisecond dedupe).
    private static func derivedCandidates(
        anchors: [EdgeCandidate],
        grid: Double,
        maxMultiple: Int?,
        direction: Double,
        center: Double,
        episodeDuration: Double
    ) -> [EdgeCandidate] {
        var seenKeys = Set<Int64>()
        var derived: [EdgeCandidate] = []
        for anchor in anchors {
            guard anchor.kind == .peak, anchor.peak >= gridMinimumPeak else { continue }
            var k = 1
            while Double(k) * grid <= maxEdgeMoveSeconds + searchRadiusSeconds,
                  maxMultiple.map({ k <= $0 }) ?? true {
                let time = anchor.time + direction * Double(k) * grid
                k += 1
                guard time >= 0.0, time <= episodeDuration else { continue }
                guard abs(time - center) <= maxEdgeMoveSeconds else { continue }
                let key = Int64((time * 1000).rounded(.toNearestOrEven))
                if seenKeys.insert(key).inserted {
                    derived.append(EdgeCandidate(time: time, peak: 0.0, kind: .derived))
                }
            }
        }
        return derived
    }

    // MARK: - Normalized cross-correlation

    /// Normalized cross-correlation of `template` at every valid offset
    /// inside `target`. Nil when the template is shorter than one second
    /// of frames, longer than the target, or has zero variance. Direct
    /// port of the offline `ncc_curve` (prefix-sum local variance, 1e-12
    /// floor).
    static func normalizedCrossCorrelationCurve(
        template: [Float],
        target: [Float]
    ) -> [Double]? {
        let n = template.count
        guard n >= StingerEnvelope.envelopeHz, target.count >= n else { return nil }

        let templateMean = template.reduce(0.0) { $0 + Double($1) } / Double(n)
        var centered = [Double](repeating: 0, count: n)
        var templateNormSquared = 0.0
        for i in 0..<n {
            let value = Double(template[i]) - templateMean
            centered[i] = value
            templateNormSquared += value * value
        }
        guard templateNormSquared > 0 else { return nil }
        let templateNorm = templateNormSquared.squareRoot()

        // Prefix sums over the target for O(1) windowed sum / sum-of-squares.
        let m = target.count
        var prefixSum = [Double](repeating: 0, count: m + 1)
        var prefixSquares = [Double](repeating: 0, count: m + 1)
        for i in 0..<m {
            let value = Double(target[i])
            prefixSum[i + 1] = prefixSum[i] + value
            prefixSquares[i + 1] = prefixSquares[i] + value * value
        }

        let offsets = m - n + 1
        var curve = [Double](repeating: 0, count: offsets)
        for offset in 0..<offsets {
            var raw = 0.0
            for i in 0..<n {
                raw += Double(target[offset + i]) * centered[i]
            }
            let windowSum = prefixSum[offset + n] - prefixSum[offset]
            let windowSquares = prefixSquares[offset + n] - prefixSquares[offset]
            let localVariance = max(windowSquares - windowSum * windowSum / Double(n), 1e-12)
            curve[offset] = raw / (localVariance.squareRoot() * templateNorm)
        }
        return curve
    }

    /// Strongest normalized cross-correlation of `template` inside
    /// `target`: `(offset, peak)` at the first index of the maximum
    /// (matches the oracle's `ncc_align` argmax). Nil under the same
    /// conditions as `normalizedCrossCorrelationCurve`.
    static func normalizedCrossCorrelationPeak(
        template: [Float],
        target: [Float]
    ) -> (offset: Int, peak: Double)? {
        guard let curve = normalizedCrossCorrelationCurve(
            template: template, target: target
        ) else { return nil }
        var bestOffset = 0
        var bestPeak = -Double.infinity
        for (offset, value) in curve.enumerated() where value > bestPeak {
            bestPeak = value
            bestOffset = offset
        }
        return (bestOffset, bestPeak)
    }

    /// All qualifying local maxima of an NCC curve: `(index, value)` where
    /// the value clears `gate` and is a local maximum (plateaus report
    /// their right edge). Port of the oracle's `ncc_qualifying_maxima`
    /// (`value >= left && value > right`, boundaries count as -inf).
    static func nccQualifyingMaxima(
        curve: [Double],
        gate: Double
    ) -> [(index: Int, value: Double)] {
        var maxima: [(index: Int, value: Double)] = []
        for i in curve.indices {
            let left = i > 0 ? curve[i - 1] : -Double.infinity
            let right = i + 1 < curve.count ? curve[i + 1] : -Double.infinity
            if curve[i] >= gate && curve[i] >= left && curve[i] > right {
                maxima.append((i, curve[i]))
            }
        }
        return maxima
    }

    private static func roundToMillis(_ value: Double) -> Double {
        (value * 1000).rounded() / 1000
    }

    /// Banker's rounding to 1e-4 — mirrors the oracle's `round(score, 4)`
    /// for the recorded pair score.
    private static func roundToTenThousandths(_ value: Double) -> Double {
        (value * 10_000).rounded(.toNearestOrEven) / 10_000
    }
}
