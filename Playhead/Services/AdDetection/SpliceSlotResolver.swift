// SpliceSlotResolver.swift
// playhead-xsdz.19 (Bead A of xsdz.15): the pure slot-resolution engine for the
// boundary-authority inversion.
//
// WHY (playhead-4xqf)
// -------------------
// The pipeline covers only ~18% of true DAI ad WIDTH. Root cause: span width is
// minted at `MinimalContiguousSpanDecoder.formRuns` as the anchored-atom run
// extent, and downstream can only nudge edges a few seconds. DAI ads are spliced
// server-side, so the SPLICES are the true boundaries. Approved inversion: when a
// qualifying acoustic-splice pair encloses enough of a presence core, the SLOT
// owns ad-span WIDTH; transcript/FM keep owning PRESENCE.
//
// SCOPE
// -----
// This file is the pure computation ONLY. There is NO pipeline wiring here and
// ZERO production call sites — the post-decode span rewrite is playhead-xsdz.20
// (Bead B), which also layers the negative-bank veto and pins the input sources.
// Everything below is deterministic, `Sendable`, token-free, with no I/O and no
// actor hops.
//
// INTERVAL SEMANTICS (pinned, pipeline-wide — mirrors xsdz.20)
// ------------------------------------------------------------
// Interval intersection means POSITIVE-DURATION overlap: two intervals that share
// ONLY an endpoint do NOT intersect.

import Foundation

// MARK: - TimeRange

/// A time interval (seconds into episode) for the presence core and veto ranges.
///
/// Intersection is POSITIVE-DURATION overlap (see file header): sharing only an
/// endpoint does not count. Unlike `ClosedRange`, this value tolerates zero- and
/// negative-length inputs (`start >= end`) without trapping, so the resolver can
/// diagnose a degenerate core instead of crashing on it.
struct TimeRange: Sendable, Equatable {
    let start: Double
    let end: Double

    var length: Double { end - start }

    /// Length of the POSITIVE-DURATION overlap with `other` (0 when the two
    /// ranges only touch at an endpoint or are disjoint).
    func overlapLength(with other: TimeRange) -> Double {
        max(0, min(end, other.end) - max(start, other.start))
    }

    /// True when the two ranges share a POSITIVE-DURATION sub-interval.
    func intersects(_ other: TimeRange) -> Bool {
        overlapLength(with: other) > 0
    }
}

// MARK: - SpliceEdgeEvidence

/// Evidence for one edge (start or end) of a candidate splice slot: the acoustic
/// break time and the σ-normalized boundary-discontinuity score measured there by
/// `AudioForensicsBoundaryDetector`.
struct SpliceEdgeEvidence: Sendable, Equatable {
    /// Candidate boundary time (an `AcousticBreak` time).
    let time: Double
    /// Merged σ-normalized boundary-step score in `[0, 1]`. `0` when the episode
    /// could not be scored (too few / perfectly flat windows) — an unscorable
    /// edge carries no boundary evidence.
    let stepScore: Double
    /// How many audio-forensics sub-signals cleared their per-signal σ-floor at
    /// this edge (diagnostics parity with the `.audioForensics` ledger detail).
    let contributingSignals: Int
}

// MARK: - SpliceSlot

/// A resolved (or candidate) splice slot: the width-owning interval bracketed by
/// two acoustic-splice edges around a presence core.
struct SpliceSlot: Sendable, Equatable {
    let startTime: Double
    let endTime: Double
    let startEdge: SpliceEdgeEvidence
    let endEdge: SpliceEdgeEvidence
    /// The WEAKER (min) of the two edge stepScores — a slot is only as
    /// trustworthy as its softest seam.
    let slotConfidence: Double
    /// `|slot ∩ core| / |core|` in `[0, 1]`.
    let coreCoverage: Double
}

// MARK: - SpliceSlotDiagnostics

/// Why a resolve did (or did not) produce a slot, plus the pre-qualification
/// champion for callers that want to inspect the best geometry-valid pair.
///
/// POPULATION CONTRACT (pinned):
///   • `failureReason` is set on EVERY non-qualifying outcome and `nil` when the
///     resolve qualified.
///   • `bestGeometryValidPair` is populated whenever ≥1 geometry-valid pair
///     exists (the champion, taken PRE-qualification), and `nil` otherwise
///     (`.noCandidatePairs`, `.degenerateCore`, `.durationOutOfRange`). On a
///     QUALIFIED outcome it equals the returned slot.
struct SpliceSlotDiagnostics: Sendable, Equatable {
    /// The champion geometry-valid pair, before qualification gates were applied.
    let bestGeometryValidPair: SpliceSlot?
    /// The disqualifying reason, or `nil` when the resolve qualified.
    let failureReason: FailureReason?

    enum FailureReason: Sendable, Equatable {
        /// Core interval has zero or negative length.
        case degenerateCore
        /// No geometry-valid pair and none merely out of the duration bound.
        case noCandidatePairs
        /// No geometry-valid pair, but ≥1 pair failed ONLY the duration bound.
        case durationOutOfRange
        /// Champion has an edge below `spliceEdgeFloor`.
        case edgeBelowFloor
        /// Champion's slot confidence (min edge) below `slotConfidenceFloor`.
        case slotConfidenceBelowFloor
        /// Champion covers less of the core than `minCoreCoverage`.
        case coreCoverageBelowMinimum
        /// Champion newly encloses a vetoed range that the core did not already
        /// intersect.
        case vetoNewlyEnclosed
    }
}

// MARK: - SpliceSlotResolver

/// Pure engine that resolves a presence core + acoustic breaks into a
/// width-owning `SpliceSlot`, or a diagnosed non-result. See the file header for
/// scope and interval semantics.
struct SpliceSlotResolver: Sendable {

    // MARK: Configuration

    struct Configuration: Sendable, Equatable {
        /// How far OUTWARD (per side) to search for a splice edge beyond the
        /// core, in seconds. DAI ads can run far past the transcript-anchored
        /// presence core, so the outward reach is generous.
        let searchRadiusSeconds: Double

        /// How far INWARD (per side) a splice edge may sit relative to the core
        /// edge, in seconds. Mirrors the decoder's `snapRadiusSeconds` (8): a
        /// minted extent can OVERSHOOT the true splice, so the true edge may lie
        /// slightly inside the core.
        let inwardToleranceSeconds: Double

        /// Allowed slot duration (`endTime − startTime`), in seconds. Mirrors the
        /// decoder's universal caps (`DecoderConstants.minDurationSeconds` …
        /// `maxDurationSeconds`).
        let slotDuration: ClosedRange<Double>

        /// Minimum stepScore EACH edge must clear to qualify. Mirrors
        /// `AudioForensicsBoundaryDetector.Config.minBoundaryScore` (0.15): a
        /// splice edge must clear the same physical-discontinuity floor the
        /// audio-forensics channel uses to emit any entry at all — below it the
        /// seam is content-like jitter, not an insertion boundary.
        let spliceEdgeFloor: Double

        /// Minimum slot confidence (the WEAKER edge's stepScore). An independent
        /// knob from `spliceEdgeFloor`: defaults equal to it (so the per-edge
        /// floor is the operative gate) but can be raised to demand that even the
        /// softer seam be strong before trusting the whole slot.
        let slotConfidenceFloor: Double

        /// Minimum fraction of the core the slot must cover. A slot that owns
        /// WIDTH must still contain the PRESENCE it is widening.
        let minCoreCoverage: Double

        /// Champion-scan replacement multiplier: a wider pair replaces the
        /// running champion only if its pairScore is at least this factor of the
        /// champion's. Mirrors `AsymmetricSnapScorer.editorialClipPenalty` (1.5) —
        /// clipping editorial content is 1.5× worse than leaking ad audio, so a
        /// wider slot must be clearly (1.5×) stronger to justify the extra reach.
        let contentCutPenalty: Double

        static let `default` = Configuration(
            searchRadiusSeconds: 120,
            inwardToleranceSeconds: 8,
            slotDuration: DecoderConstants.minDurationSeconds...DecoderConstants.maxDurationSeconds,
            spliceEdgeFloor: 0.15,
            slotConfidenceFloor: 0.15,
            minCoreCoverage: 0.8,
            contentCutPenalty: 1.5
        )

        init(
            searchRadiusSeconds: Double = 120,
            inwardToleranceSeconds: Double = 8,
            slotDuration: ClosedRange<Double> = DecoderConstants.minDurationSeconds...DecoderConstants.maxDurationSeconds,
            spliceEdgeFloor: Double = 0.15,
            slotConfidenceFloor: Double = 0.15,
            minCoreCoverage: Double = 0.8,
            contentCutPenalty: Double = 1.5
        ) {
            self.searchRadiusSeconds = searchRadiusSeconds
            self.inwardToleranceSeconds = inwardToleranceSeconds
            self.slotDuration = slotDuration
            self.spliceEdgeFloor = spliceEdgeFloor
            self.slotConfidenceFloor = slotConfidenceFloor
            self.minCoreCoverage = minCoreCoverage
            self.contentCutPenalty = contentCutPenalty
        }
    }

    let configuration: Configuration

    init(configuration: Configuration = .default) {
        self.configuration = configuration
    }

    // MARK: Public API

    /// Resolve a splice slot, or `nil`. Thin wrapper over
    /// `resolveWithDiagnostics` that discards the diagnostics.
    ///
    /// - Parameters:
    ///   - core: the presence-core interval (transcript/FM authority).
    ///   - vetoedRanges: time ranges that must not be NEWLY enclosed by the slot.
    ///   - breaks: acoustic breaks whose times seed candidate edges.
    ///   - episodeWindows: per-window features used to score each candidate edge.
    func resolve(
        core: TimeRange,
        vetoedRanges: [TimeRange],
        breaks: [AcousticBreak],
        episodeWindows: [FeatureWindow]
    ) -> SpliceSlot? {
        resolveWithDiagnostics(
            core: core,
            vetoedRanges: vetoedRanges,
            breaks: breaks,
            episodeWindows: episodeWindows
        ).slot
    }

    /// Resolve a splice slot with full diagnostics. See `SpliceSlotDiagnostics`
    /// for the population contract.
    func resolveWithDiagnostics(
        core: TimeRange,
        vetoedRanges: [TimeRange],
        breaks: [AcousticBreak],
        episodeWindows: [FeatureWindow]
    ) -> (slot: SpliceSlot?, diagnostics: SpliceSlotDiagnostics) {
        // (0) GUARD: a zero-/negative-length core has no coverage math to do.
        // Diagnosed BEFORE any coverage arithmetic.
        guard core.length > 0 else {
            return (nil, SpliceSlotDiagnostics(bestGeometryValidPair: nil, failureReason: .degenerateCore))
        }

        // (a) Candidate edges per side. The end side searches OUTWARD (later) up
        // to searchRadius and INWARD (earlier) up to inwardTolerance; the start
        // side is the mirror. Times are deduplicated and sorted for determinism.
        let detector = AudioForensicsBoundaryDetector()
        let startRange = TimeRange(
            start: core.start - configuration.searchRadiusSeconds,
            end: core.start + configuration.inwardToleranceSeconds
        )
        let endRange = TimeRange(
            start: core.end - configuration.inwardToleranceSeconds,
            end: core.end + configuration.searchRadiusSeconds
        )
        let times = Set(breaks.map(\.time)).sorted()
        // Each candidate re-preps the episode (sort + per-feature σ) inside
        // `scoreCandidateEdge`. Candidate counts are small — bounded by the
        // breaks falling in a ±searchRadius window around each core edge — and
        // this pure engine carries no perf gate, so a shared-prep batch API is
        // deliberately deferred to keep the detector's exposed surface a single
        // per-time scorer.
        func edge(at time: Double) -> SpliceEdgeEvidence {
            let scored = detector.scoreCandidateEdge(at: time, episodeWindows: episodeWindows)
            return SpliceEdgeEvidence(
                time: time,
                stepScore: scored?.stepScore ?? 0,
                contributingSignals: scored?.contributingSignalCount ?? 0
            )
        }
        let startEdges = times
            .filter { $0 >= startRange.start && $0 <= startRange.end }
            .map(edge)
        let endEdges = times
            .filter { $0 >= endRange.start && $0 <= endRange.end }
            .map(edge)

        // (c) Enumerate pairs. A geometry-valid pair has slotStart < slotEnd, a
        // POSITIVE-DURATION overlap with the core (touching-only is excluded),
        // and a duration within range. Track whether any pair failed ONLY the
        // duration bound, for the duration diagnostic.
        var geometryValid: [SpliceSlot] = []
        var anyDurationOnlyFailure = false
        for startEdge in startEdges {
            for endEdge in endEdges {
                guard startEdge.time < endEdge.time else { continue }
                let slotRange = TimeRange(start: startEdge.time, end: endEdge.time)
                let overlap = core.overlapLength(with: slotRange)
                guard overlap > 0 else { continue } // touching-only / disjoint
                let duration = endEdge.time - startEdge.time
                guard configuration.slotDuration.contains(duration) else {
                    anyDurationOnlyFailure = true
                    continue
                }
                geometryValid.append(SpliceSlot(
                    startTime: startEdge.time,
                    endTime: endEdge.time,
                    startEdge: startEdge,
                    endEdge: endEdge,
                    slotConfidence: min(startEdge.stepScore, endEdge.stepScore),
                    coreCoverage: overlap / core.length
                ))
            }
        }

        // DURATION DIAGNOSTIC (pinned): geometry-valid empty but ≥1 pair failed
        // ONLY the duration bound ⇒ .durationOutOfRange; empty otherwise ⇒
        // .noCandidatePairs. Both leave bestGeometryValidPair nil.
        guard !geometryValid.isEmpty else {
            let reason: SpliceSlotDiagnostics.FailureReason =
                anyDurationOnlyFailure ? .durationOutOfRange : .noCandidatePairs
            return (nil, SpliceSlotDiagnostics(bestGeometryValidPair: nil, failureReason: reason))
        }

        // (c) PINNED CHAMPION-SCAN. Total order — the naive pairwise "wider beats
        // tighter at ≥1.5×" relation is NON-TRANSITIVE with 3+ pairs and is NOT
        // the spec. Sort by non-core seconds ASC, ties by higher pairScore, then
        // earlier startTime, then earlier endTime; the first is the champion.
        // Scan the rest IN ORDER — a candidate REPLACES the running champion iff
        // its pairScore ≥ contentCutPenalty × the champion's pairScore.
        let sorted = geometryValid.sorted { lhs, rhs in
            let lNonCore = nonCoreSeconds(lhs, core: core)
            let rNonCore = nonCoreSeconds(rhs, core: core)
            if lNonCore != rNonCore { return lNonCore < rNonCore }
            if lhs.slotConfidence != rhs.slotConfidence { return lhs.slotConfidence > rhs.slotConfidence }
            if lhs.startTime != rhs.startTime { return lhs.startTime < rhs.startTime }
            return lhs.endTime < rhs.endTime
        }
        var champion = sorted[0]
        for candidate in sorted.dropFirst()
        where candidate.slotConfidence >= configuration.contentCutPenalty * champion.slotConfidence {
            // The `>=` is inclusive by spec. One degenerate consequence: if the
            // running champion has slotConfidence 0 (a fully unscorable episode),
            // the bar is 0 and every 0-confidence pair replaces it, so the champion
            // walks to the WIDEST such pair. That is spec-consistent and harmless —
            // a 0-confidence champion always fails the edge floor below, so only
            // which pair surfaces as `bestGeometryValidPair` differs, never a
            // qualified result.
            champion = candidate
        }

        // (d) Qualification gates on the FINAL CHAMPION ONLY. A disqualified
        // champion means fallback BY DESIGN: we do NOT pre-filter sub-floor edges
        // and do NOT search for another qualifying pair. bestGeometryValidPair is
        // the champion in every gate-failure case.
        let diagnosticsBase = { (reason: SpliceSlotDiagnostics.FailureReason?) in
            SpliceSlotDiagnostics(bestGeometryValidPair: champion, failureReason: reason)
        }
        if champion.startEdge.stepScore < configuration.spliceEdgeFloor
            || champion.endEdge.stepScore < configuration.spliceEdgeFloor {
            return (nil, diagnosticsBase(.edgeBelowFloor))
        }
        if champion.slotConfidence < configuration.slotConfidenceFloor {
            return (nil, diagnosticsBase(.slotConfidenceBelowFloor))
        }
        if champion.coreCoverage < configuration.minCoreCoverage {
            return (nil, diagnosticsBase(.coreCoverageBelowMinimum))
        }
        let championRange = TimeRange(start: champion.startTime, end: champion.endTime)
        for veto in vetoedRanges
        where championRange.intersects(veto) && !core.intersects(veto) {
            return (nil, diagnosticsBase(.vetoNewlyEnclosed))
        }

        // Qualified: bestGeometryValidPair equals the returned slot.
        return (champion, diagnosticsBase(nil))
    }

    // MARK: Helpers

    /// Non-core seconds of a slot: its duration minus the core it covers. Smaller
    /// is tighter around the core. Computed from times (not the stored coverage
    /// fraction) to avoid a divide-then-multiply round-trip.
    private func nonCoreSeconds(_ slot: SpliceSlot, core: TimeRange) -> Double {
        let slotRange = TimeRange(start: slot.startTime, end: slot.endTime)
        return slotRange.length - core.overlapLength(with: slotRange)
    }
}
