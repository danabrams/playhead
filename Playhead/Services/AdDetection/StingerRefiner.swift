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
//   • The algorithm is the production port of the offline recipe measured
//     by `scripts/l2f-boundary-stinger-prototype.py` (2026-07-15 spike):
//     normalized cross-correlation of a per-show 50 Hz log-RMS stinger
//     template over a ±90 s search envelope around each proposed edge;
//     per-show confidence gate `max(0.50, learning_confidence - 0.15)`;
//     strongest peak wins; snaps moving an edge > 75 s are refused; when
//     exactly one edge snapped and the show has a pod-width grid, the other
//     edge is set by snapping the width to the nearest positive grid
//     multiple; if the refined window no longer overlaps the proposal, BOTH
//     edges revert (the presence evidence must not be abandoned).

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
    /// The break-start edge snapped to a qualifying pre-stinger peak.
    var startSnapped = false
    /// The break-end edge snapped to a qualifying post-stinger peak.
    var endSnapped = false
    /// Exactly one edge snapped and the show grid set the other edge.
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
}

// MARK: - StingerRefiner

enum StingerRefiner {
    /// Search span half-width around each proposed edge.
    static let searchRadiusSeconds = 90.0
    /// Refuse snaps that move an edge farther than this from the proposal.
    static let maxEdgeMoveSeconds = 75.0
    /// Refined windows keep at least this width (clamp floor); combined
    /// with the ordering of the clamps this makes `end <= start`
    /// impossible.
    static let minimumRefinedWidthSeconds = 1.0
    /// Grid width snapping: `grid * max(1, round(width / grid))`.
    /// (No tolerance — the grid only fires when the show earned one.)

    struct Result: Sendable, Equatable {
        let startTime: Double
        let endTime: Double
        let trace: StingerRefinementTrace
    }

    /// Refine one candidate window against the show's bank entry.
    ///
    /// - Parameters:
    ///   - proposalStart/proposalEnd: the pipeline's current window bounds
    ///     (post acoustic snap — the refiner runs inside the existing
    ///     boundary-refinement block).
    ///   - entry: the show's bank entry (caller resolved it; no entry ⇒
    ///     caller never calls).
    ///   - startEnvelope/endEnvelope: 50 Hz search envelopes around each
    ///     edge; nil when the side has no template or no PCM was
    ///     available (that side simply cannot snap).
    ///   - episodeDuration: clamp ceiling.
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

        var newStart = proposalStart
        var newEnd = proposalEnd

        if let template = entry.pre, let envelope = startEnvelope,
           let snap = snapEdge(template: template, envelope: envelope, proposalEdge: proposalStart) {
            newStart = snap.time
            trace.startSnapped = true
            trace.startPeak = roundToMillis(snap.peak)
        }
        if let template = entry.post, let envelope = endEnvelope,
           let snap = snapEdge(template: template, envelope: envelope, proposalEdge: proposalEnd) {
            newEnd = snap.time
            trace.endSnapped = true
            trace.endPeak = roundToMillis(snap.peak)
        }

        // Grid: when exactly one edge snapped and the show earned a
        // pod-width grid, set the OTHER edge by snapping the width to the
        // nearest positive multiple of the grid. `.toNearestOrEven` matches
        // Python's banker's-rounding `round()` in the offline oracle
        // (`round(width / grid)` in l2f-boundary-stinger-prototype.py) so a
        // half-grid width (e.g. exactly 75 s on a 30 s grid) resolves to
        // the SAME pod count the spike measured — `.rounded()`'s
        // half-away-from-zero default would diverge by a full grid step in
        // precisely that case.
        if let grid = entry.podWidthGridSeconds,
           trace.startSnapped != trace.endSnapped {
            let width = newEnd - newStart
            let snappedWidth = grid * max(1.0, (width / grid).rounded(.toNearestOrEven))
            if trace.startSnapped {
                newEnd = newStart + snappedWidth
            } else {
                newStart = newEnd - snappedWidth
            }
            trace.gridApplied = true
        }

        // Clamp to the episode. Order is load-bearing: the end clamp's
        // `max(newStart + minimumRefinedWidthSeconds, …)` floor runs after
        // the start clamp, so `end > start` holds unconditionally.
        newStart = max(0.0, min(newStart, episodeDuration - minimumRefinedWidthSeconds))
        newEnd = max(newStart + minimumRefinedWidthSeconds, min(newEnd, episodeDuration))

        // Revert guard: refinement must not abandon the presence evidence.
        // Zero overlap with the proposal reverts BOTH edges (a one-sided
        // keep would fabricate a window no evidence supports).
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

    // MARK: - Edge snapping

    private struct EdgeSnap {
        let time: Double
        let peak: Double
    }

    /// Match one side's template into its search envelope. Returns the
    /// snapped edge time when the strongest peak clears the per-show gate
    /// AND the snap stays within the move cap; nil otherwise.
    private static func snapEdge(
        template: StingerTemplate,
        envelope: StingerSearchEnvelope,
        proposalEdge: Double
    ) -> EdgeSnap? {
        guard let match = normalizedCrossCorrelationPeak(
            template: template.template,
            target: envelope.values
        ) else { return nil }
        guard match.peak >= template.snapGate else { return nil }
        let frameSeconds = 1.0 / Double(StingerEnvelope.envelopeHz)
        let snapped = envelope.startSeconds
            + Double(match.offset + template.edgeSampleIndex) * frameSeconds
            + template.edgeOffsetSeconds
        guard abs(snapped - proposalEdge) <= maxEdgeMoveSeconds else { return nil }
        return EdgeSnap(time: snapped, peak: match.peak)
    }

    // MARK: - Normalized cross-correlation

    /// Strongest normalized cross-correlation of `template` at every valid
    /// offset inside `target`: `(offset, peak)`. Nil when the template is
    /// shorter than one second of frames, longer than the target, or has
    /// zero variance. Direct port of the offline `ncc_curve`/`ncc_align`
    /// pair (prefix-sum local variance, 1e-12 floor).
    static func normalizedCrossCorrelationPeak(
        template: [Float],
        target: [Float]
    ) -> (offset: Int, peak: Double)? {
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

        var bestOffset = 0
        var bestPeak = -Double.infinity
        let offsets = m - n + 1
        for offset in 0..<offsets {
            var raw = 0.0
            for i in 0..<n {
                raw += Double(target[offset + i]) * centered[i]
            }
            let windowSum = prefixSum[offset + n] - prefixSum[offset]
            let windowSquares = prefixSquares[offset + n] - prefixSquares[offset]
            let localVariance = max(windowSquares - windowSum * windowSum / Double(n), 1e-12)
            let ncc = raw / (localVariance.squareRoot() * templateNorm)
            if ncc > bestPeak {
                bestPeak = ncc
                bestOffset = offset
            }
        }
        return (bestOffset, bestPeak)
    }

    private static func roundToMillis(_ value: Double) -> Double {
        (value * 1000).rounded() / 1000
    }
}
