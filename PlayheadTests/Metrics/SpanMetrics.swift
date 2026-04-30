// SpanMetrics.swift
// playhead-352 — A7: Metrics Split + Live Skip-Usefulness Metrics.
//
// Pure metric-computation framework for evaluating ad-detection quality
// against ground-truth spans. Produces 9 metrics across two families
// (offline span quality + live skip usefulness), all sliceable by
// ad format, podcast, and detection path (live vs backfill).
//
// This is test-target-only code: it lives in PlayheadTests so it does
// not bloat the shipped app binary, and so it can reuse the existing
// fixture types (TestAdSegment, TestEpisodeAnnotation) without needing
// to expose a parallel "ground truth" type from production.
//
// Design intent:
//   - Pure functions: every metric is `(MetricsBatch) -> Result`. No
//     hidden state, no async, no side effects. Trivial to unit-test.
//   - Composable slicing: `MetricsBatch.sliced(by:...)` returns a new
//     batch; metrics simply consume whatever batch is handed in.
//   - Defined behavior on empty input: every metric returns either
//     `nil` (when undefined — e.g. "median of zero samples") or a
//     well-defined zero (e.g. seed-recall over zero GT ads = nil, not
//     NaN, not 0.0). Callers always know when a slice is empty.
//
// Per the bead: this PR builds the framework. Baseline value capture
// against the real corpus is deferred to a follow-up bead — the
// fixture-driven integration test in MetricsCorpusIntegrationTests
// exercises the wiring with synthetic data.

import Foundation

// MARK: - Domain types

/// Format a ground-truth ad was delivered in. Maps onto the slicing
/// dimension named in the design doc; intentionally narrower than the
/// fixture's `TestAdSegment.DeliveryStyle` enum (which has a 4th value
/// "blendedHostRead" we fold into `.hostRead` for slicing purposes —
/// blended host-reads are still host-reads from a delivery standpoint).
enum AdFormat: String, Sendable, Codable, Hashable, CaseIterable {
    case hostRead
    case produced
    case dynamic

    /// Bridge from the fixture's `DeliveryStyle` to this slicing dimension.
    /// Folds `blendedHostRead` into `.hostRead` because we slice by *delivery*
    /// channel, not by detection difficulty (difficulty has its own field).
    static func from(_ style: TestAdSegment.DeliveryStyle) -> AdFormat {
        switch style {
        case .hostRead, .blendedHostRead: return .hostRead
        case .producedSegment:            return .produced
        case .dynamicInsertion:           return .dynamic
        }
    }
}

/// Which detection path produced a `MetricDetectedAd`. The bead calls out
/// `live-path vs backfill-path` as a slicing dimension because the two
/// have very different latency/coverage tradeoffs in production.
enum DetectionPath: String, Sendable, Codable, Hashable, CaseIterable {
    case live
    case backfill
}

/// Ground-truth ad span used by the metrics framework. Named
/// `MetricGroundTruthAd` to disambiguate from the existing
/// `GroundTruthAd` type in `ConanFanhausenRevisitedFixture` (which
/// has a different shape — `expectedSignals`, `skipConfidence`, etc.).
/// Carries the fields needed to slice by (podcast, format) and to
/// compute IoU/coverage against detected spans. `seedFired` is a
/// separate signal from "did we produce a detected span" — it asks
/// specifically whether an *anchor* fired on this GT ad, even if the
/// resulting hypothesis never confirmed.
struct MetricGroundTruthAd: Sendable, Hashable {
    let id: String
    let podcastId: String
    let episodeId: String
    let startTime: Double
    let endTime: Double
    let format: AdFormat
    /// Did at least one anchor fire inside this GT span? Surfaces from
    /// the detector via its anchor stream, independent of whether a
    /// hypothesis was eventually confirmed and emitted as a MetricDetectedAd.
    let seedFired: Bool

    init(
        id: String,
        podcastId: String,
        episodeId: String,
        startTime: Double,
        endTime: Double,
        format: AdFormat,
        seedFired: Bool
    ) {
        self.id = id
        self.podcastId = podcastId
        self.episodeId = episodeId
        self.startTime = startTime
        self.endTime = endTime
        self.format = format
        self.seedFired = seedFired
    }

    var duration: Double { max(0, endTime - startTime) }
}

/// A detected ad span as emitted by the detection pipeline, used by
/// the metrics framework. Named `MetricDetectedAd` for symmetry with
/// `MetricGroundTruthAd`; the production code does not currently
/// expose a top-level `DetectedAd` type, but the prefix keeps the
/// metrics framework self-contained and immune to future name
/// collisions.
///
/// The `firstConfirmationTime` field captures *when* the hypothesis
/// crossed skip-eligibility — that's what the live skip-usefulness
/// metric (lead time) measures, not when the span was finally closed.
struct MetricDetectedAd: Sendable, Hashable {
    let id: String
    let podcastId: String
    let episodeId: String
    let startTime: Double
    let endTime: Double
    let path: DetectionPath
    /// Wall-clock-equivalent timestamp (seconds in episode time) when
    /// the detector first marked this span skip-eligible. May predate
    /// the GT ad start (positive lead time) or postdate it (negative
    /// lead time = we noticed too late to be useful).
    let firstConfirmationTime: Double?
    let confidence: Double

    init(
        id: String,
        podcastId: String,
        episodeId: String,
        startTime: Double,
        endTime: Double,
        path: DetectionPath,
        firstConfirmationTime: Double?,
        confidence: Double
    ) {
        self.id = id
        self.podcastId = podcastId
        self.episodeId = episodeId
        self.startTime = startTime
        self.endTime = endTime
        self.path = path
        self.firstConfirmationTime = firstConfirmationTime
        self.confidence = confidence
    }

    var duration: Double { max(0, endTime - startTime) }
}

/// One paired evaluation row. Either side may be nil:
///   - `gt` nil + `detected` non-nil  = false-positive detection
///   - `gt` non-nil + `detected` nil  = missed GT ad
///   - both non-nil                   = true positive (matched)
///
/// Pairing is the caller's responsibility; this struct only carries the
/// pair through the metric functions. See `MetricsBatch.pair(...)` for
/// a default greedy IoU-maximizing pairing helper.
struct MetricsPair: Sendable, Hashable {
    let gt: MetricGroundTruthAd?
    let detected: MetricDetectedAd?

    init(gt: MetricGroundTruthAd?, detected: MetricDetectedAd?) {
        precondition(gt != nil || detected != nil,
                     "MetricsPair requires at least one of gt/detected")
        self.gt = gt
        self.detected = detected
    }

    var isTruePositive: Bool { gt != nil && detected != nil }
    var isMiss:          Bool { gt != nil && detected == nil }
    var isFalsePositive: Bool { gt == nil && detected != nil }
}

// MARK: - Batch + slicing

/// A collection of `MetricsPair` rows from one or more episodes.
/// Slicing produces a *new* `MetricsBatch` whose pairs all match the
/// requested predicate; the resulting batch is what every metric
/// function consumes, so slicing composes trivially:
///
///     batch.sliced(byFormat: .hostRead)
///          .sliced(byPodcast: "diary-of-a-ceo")
///          .computeSeedRecall()
struct MetricsBatch: Sendable, Hashable {
    let pairs: [MetricsPair]

    init(pairs: [MetricsPair]) {
        self.pairs = pairs
    }

    /// Pairs where a GT ad is present. Convenience for metrics that
    /// only make sense per-GT (recall, IoU, signed bias, …).
    var gtPairs: [MetricsPair] { pairs.filter { $0.gt != nil } }

    /// Pairs where a detected span is present. Convenience for metrics
    /// keyed off the detection side (coverage precision).
    var detectedPairs: [MetricsPair] { pairs.filter { $0.detected != nil } }

    var isEmpty: Bool { pairs.isEmpty }
    var count: Int { pairs.count }
}

extension MetricsBatch {
    // MARK: Slicing

    /// Slice by ad format (host-read / produced / dynamic). A pair is
    /// kept iff its GT ad has the requested format. Pure-FP detections
    /// (gt == nil) cannot be sliced by format and are dropped — by
    /// design, FP false-positives don't have a "ground-truth format".
    func sliced(byFormat format: AdFormat) -> MetricsBatch {
        MetricsBatch(pairs: pairs.filter { $0.gt?.format == format })
    }

    /// Slice by podcast. We prefer the GT side (since it's the more
    /// reliable identity), but fall back to the detection side so that
    /// FP detections are still attributable to a podcast.
    func sliced(byPodcast podcastId: String) -> MetricsBatch {
        MetricsBatch(pairs: pairs.filter { pair in
            (pair.gt?.podcastId ?? pair.detected?.podcastId) == podcastId
        })
    }

    /// Slice by detection path (live vs backfill). A pair is kept iff
    /// it has a detection on that path. Misses (no detection at all)
    /// have no path, so they fall out — the caller usually wants to
    /// look at recall holistically *before* slicing by path, or use
    /// `slicedKeepingMisses(byPath:)` to retain misses explicitly.
    func sliced(byPath path: DetectionPath) -> MetricsBatch {
        MetricsBatch(pairs: pairs.filter { $0.detected?.path == path })
    }

    /// Variant that retains GT misses regardless of path. Useful for
    /// per-path recall analysis where misses must remain visible.
    func slicedKeepingMisses(byPath path: DetectionPath) -> MetricsBatch {
        MetricsBatch(pairs: pairs.filter { pair in
            pair.detected.map { $0.path == path } ?? (pair.gt != nil)
        })
    }

    /// All distinct podcast IDs referenced by the batch (either GT or
    /// detection side). Stable order = sorted ascending.
    var podcasts: [String] {
        let ids = pairs.flatMap { pair -> [String] in
            [pair.gt?.podcastId, pair.detected?.podcastId].compactMap { $0 }
        }
        return Array(Set(ids)).sorted()
    }
}

// MARK: - Metric results

/// Result of computing seed recall (and similar count-based metrics).
/// We return the raw numerator/denominator alongside the ratio so
/// callers can roll up across slices without losing information.
struct CountRatio: Sendable, Hashable {
    let numerator: Int
    let denominator: Int
    /// Ratio in [0, 1], or `nil` when denominator == 0.
    var ratio: Double? {
        denominator == 0 ? nil : Double(numerator) / Double(denominator)
    }

    init(numerator: Int, denominator: Int) {
        precondition(numerator >= 0 && denominator >= 0)
        precondition(numerator <= denominator,
                     "CountRatio numerator (\(numerator)) cannot exceed denominator (\(denominator))")
        self.numerator = numerator
        self.denominator = denominator
    }
}

/// Result of a per-pair time metric (IoU, start error, end error,
/// signed biases). Carries the per-pair samples plus aggregate stats
/// so slicing can drill in without recomputing.
struct SampleStats: Sendable, Hashable {
    let samples: [Double]

    init(samples: [Double]) { self.samples = samples }

    var count: Int { samples.count }
    var isEmpty: Bool { samples.isEmpty }

    /// Median of samples, or `nil` for empty input.
    var median: Double? { Self.median(samples) }
    /// Mean of samples, or `nil` for empty input.
    var mean: Double? {
        samples.isEmpty ? nil : samples.reduce(0, +) / Double(samples.count)
    }

    /// Robust median — handles even and odd counts, NaN-free input only.
    /// Internal helper exposed for tests.
    static func median(_ values: [Double]) -> Double? {
        guard !values.isEmpty else { return nil }
        let sorted = values.sorted()
        let n = sorted.count
        if n.isMultiple(of: 2) {
            return (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
        } else {
            return sorted[n / 2]
        }
    }
}

// MARK: - Metric computers (offline span quality)

extension MetricsBatch {
    /// Metric 1 — Seed recall. Numerator: GT ads with `seedFired == true`.
    /// Denominator: total GT ads in the slice. `ratio == nil` when there
    /// are no GT ads (slice is empty or only contains false positives).
    func computeSeedRecall() -> CountRatio {
        let gts = gtPairs.compactMap { $0.gt }
        let fired = gts.filter { $0.seedFired }.count
        return CountRatio(numerator: fired, denominator: gts.count)
    }

    /// Metric 2 — Span IoU samples. Per true-positive pair, computes
    /// |intersection| / |union|. Returned as a `SampleStats` so the
    /// caller can ask for median, mean, raw distribution, etc. We
    /// intentionally only emit a sample for true positives — IoU is
    /// undefined for misses and FPs.
    func computeSpanIoU() -> SampleStats {
        SampleStats(samples: pairs.compactMap { pair in
            guard let gt = pair.gt, let det = pair.detected else { return nil }
            return Self.iou(
                gtStart: gt.startTime, gtEnd: gt.endTime,
                detStart: det.startTime, detEnd: det.endTime
            )
        })
    }

    /// Metric 3 — Median start error in seconds (absolute value).
    /// `|detectedStart - gtStart|` per matched pair.
    func computeMedianStartError() -> Double? {
        let samples = pairs.compactMap { pair -> Double? in
            guard let gt = pair.gt, let det = pair.detected else { return nil }
            return abs(det.startTime - gt.startTime)
        }
        return SampleStats(samples: samples).median
    }

    /// Metric 4 — Median end error in seconds (absolute value).
    func computeMedianEndError() -> Double? {
        let samples = pairs.compactMap { pair -> Double? in
            guard let gt = pair.gt, let det = pair.detected else { return nil }
            return abs(det.endTime - gt.endTime)
        }
        return SampleStats(samples: samples).median
    }

    /// Metric 5 — Signed start bias = median(detectedStart - gtStart).
    /// Positive: detection is systematically *late* (we miss the head
    /// of the ad). Negative: detection starts before the GT ad — a
    /// usually-benign over-conservatism.
    func computeSignedStartBias() -> Double? {
        let samples = pairs.compactMap { pair -> Double? in
            guard let gt = pair.gt, let det = pair.detected else { return nil }
            return det.startTime - gt.startTime
        }
        return SampleStats(samples: samples).median
    }

    /// Metric 6 — Signed end bias = median(detectedEnd - gtEnd).
    /// Negative: we exit the ad too early (the user catches the tail
    /// of the sponsor read). Positive: we run past the GT exit, eating
    /// content seconds.
    func computeSignedEndBias() -> Double? {
        let samples = pairs.compactMap { pair -> Double? in
            guard let gt = pair.gt, let det = pair.detected else { return nil }
            return det.endTime - gt.endTime
        }
        return SampleStats(samples: samples).median
    }

    /// Metric 7 — Coverage recall = total seconds of GT ads covered
    /// by *any* detected span, divided by total GT ad seconds.
    ///
    /// Computed *per-episode* and then aggregated, because each episode
    /// has its own time axis. Naively unioning intervals across episodes
    /// would treat e.g. "episode A, t=60s" and "episode B, t=60s" as the
    /// same point in time, polluting both numerator and denominator.
    /// Within an episode, overlapping detections are flattened.
    func computeCoverageRecall() -> Double? {
        let (totalGT, covered) = perEpisodeCoverageTotals(
            measureNumerator: { gt, det in Self.intersectionLength(gt, det) },
            denominatorBasis: .gt
        )
        guard totalGT > 0 else { return nil }
        return covered / totalGT
    }

    /// Metric 8 — Coverage precision = total seconds of detected spans
    /// that fall *inside* GT ads, divided by total detected seconds.
    /// Same per-episode aggregation as recall — see `computeCoverageRecall`.
    func computeCoveragePrecision() -> Double? {
        let (totalDet, inside) = perEpisodeCoverageTotals(
            measureNumerator: { gt, det in Self.intersectionLength(gt, det) },
            denominatorBasis: .detection
        )
        guard totalDet > 0 else { return nil }
        return inside / totalDet
    }

    /// Internal — picks which side's union forms the denominator.
    private enum CoverageDenominator { case gt, detection }

    /// Internal — group intervals by (podcastId, episodeId), apply
    /// `measureNumerator` and the appropriate union per episode, sum
    /// across episodes. Numerator and denominator are each summed
    /// separately so the ratio is a proper micro-average.
    private func perEpisodeCoverageTotals(
        measureNumerator: ([(Double, Double)], [(Double, Double)]) -> Double,
        denominatorBasis: CoverageDenominator
    ) -> (denominator: Double, numerator: Double) {
        struct Key: Hashable { let podcastId: String; let episodeId: String }
        var gtByEpisode: [Key: [(Double, Double)]] = [:]
        var detByEpisode: [Key: [(Double, Double)]] = [:]
        for pair in pairs {
            if let gt = pair.gt {
                let key = Key(podcastId: gt.podcastId, episodeId: gt.episodeId)
                gtByEpisode[key, default: []].append((gt.startTime, gt.endTime))
            }
            if let det = pair.detected {
                let key = Key(podcastId: det.podcastId, episodeId: det.episodeId)
                detByEpisode[key, default: []].append((det.startTime, det.endTime))
            }
        }
        let allKeys = Set(gtByEpisode.keys).union(detByEpisode.keys)
        var denom: Double = 0
        var numer: Double = 0
        for key in allKeys {
            let gt = gtByEpisode[key] ?? []
            let det = detByEpisode[key] ?? []
            switch denominatorBasis {
            case .gt:        denom += Self.unionLength(gt)
            case .detection: denom += Self.unionLength(det)
            }
            numer += measureNumerator(gt, det)
        }
        return (denominator: denom, numerator: numer)
    }

    // MARK: - Live skip-usefulness

    /// Metric 9 — Lead time at first confirmation, in seconds.
    /// `gtStart - firstConfirmationTime` per matched pair where the
    /// detection has a confirmation timestamp.
    /// Positive = confirmed *before* the ad started (good — useful for skip).
    /// Negative = confirmed *after* the ad started (we noticed too late to
    ///            preemptively skip; user already heard part of the ad).
    /// Returns a `SampleStats` so callers can take median, distribution, etc.
    func computeLeadTimeAtFirstConfirmation() -> SampleStats {
        SampleStats(samples: pairs.compactMap { pair in
            guard let gt = pair.gt,
                  let det = pair.detected,
                  let conf = det.firstConfirmationTime else { return nil }
            return gt.startTime - conf
        })
    }
}

// MARK: - Interval math (pure helpers)

extension MetricsBatch {

    /// Intersection-over-union for two single intervals.
    /// Defined as 0.0 when both intervals are zero-length AND disjoint;
    /// 1.0 when both are zero-length and coincident; standard formula
    /// otherwise. Returns 0 (not NaN) for any other zero-union case.
    static func iou(
        gtStart: Double, gtEnd: Double,
        detStart: Double, detEnd: Double
    ) -> Double {
        let gtA = min(gtStart, gtEnd), gtB = max(gtStart, gtEnd)
        let dtA = min(detStart, detEnd), dtB = max(detStart, detEnd)

        let interStart = max(gtA, dtA)
        let interEnd   = min(gtB, dtB)
        let inter = max(0, interEnd - interStart)

        let gtLen = gtB - gtA
        let dtLen = dtB - dtA
        let union = gtLen + dtLen - inter

        if union > 0 {
            return inter / union
        }
        // Both intervals are zero-length: 1.0 if they coincide, 0.0 otherwise.
        return (gtA == dtA) ? 1.0 : 0.0
    }

    /// Total length of the union of a set of intervals (in seconds).
    /// Handles overlapping/adjacent intervals correctly via merge.
    /// Reverse-order intervals are normalized to canonical form.
    static func unionLength(_ intervals: [(Double, Double)]) -> Double {
        let normalized = intervals.map { (min($0.0, $0.1), max($0.0, $0.1)) }
        let merged = mergedIntervals(normalized)
        return merged.reduce(0) { $0 + ($1.1 - $1.0) }
    }

    /// Total length (in seconds) of the intersection of two interval
    /// sets. Both sides are flattened to their union first, so
    /// duplicate/overlapping inputs behave the same as their merged
    /// equivalent. Time complexity: O((m + n) log(m + n)) — sort each
    /// side, then linear sweep.
    static func intersectionLength(
        _ a: [(Double, Double)],
        _ b: [(Double, Double)]
    ) -> Double {
        let aMerged = mergedIntervals(a.map { (min($0.0, $0.1), max($0.0, $0.1)) })
        let bMerged = mergedIntervals(b.map { (min($0.0, $0.1), max($0.0, $0.1)) })
        var i = 0, j = 0
        var total = 0.0
        while i < aMerged.count && j < bMerged.count {
            let (aStart, aEnd) = aMerged[i]
            let (bStart, bEnd) = bMerged[j]
            let interStart = max(aStart, bStart)
            let interEnd   = min(aEnd, bEnd)
            if interEnd > interStart {
                total += interEnd - interStart
            }
            // Advance the one that ends first.
            if aEnd < bEnd { i += 1 } else { j += 1 }
        }
        return total
    }

    /// Merge a list of intervals (assumed already normalized to
    /// `(low, high)` form) into a sorted, non-overlapping list.
    /// Adjacent intervals (`a.end == b.start`) are merged.
    static func mergedIntervals(_ intervals: [(Double, Double)]) -> [(Double, Double)] {
        guard !intervals.isEmpty else { return [] }
        let sorted = intervals.sorted { $0.0 < $1.0 }
        var out: [(Double, Double)] = []
        out.reserveCapacity(sorted.count)
        for interval in sorted {
            if let last = out.last, interval.0 <= last.1 {
                out[out.count - 1] = (last.0, max(last.1, interval.1))
            } else {
                out.append(interval)
            }
        }
        return out
    }
}

// MARK: - Default greedy pairing

extension MetricsBatch {

    /// Convenience helper: pair a flat list of GT ads with a flat list
    /// of detected ads, greedy by IoU. Each GT ad and each detection
    /// is consumed at most once. Unpaired GT → miss; unpaired detection
    /// → false positive. Pairing is intentionally constrained to the
    /// same `(podcastId, episodeId)` to prevent cross-episode leakage.
    ///
    /// This is `O(n*m)` in the per-episode size which is fine for the
    /// scales we expect (<= a few dozen ads per episode); the global
    /// pair list scales linearly with episodes.
    static func pair(
        groundTruth: [MetricGroundTruthAd],
        detections:  [MetricDetectedAd]
    ) -> MetricsBatch {
        // Bucket by (podcast, episode) so we only ever pair within an episode.
        struct Key: Hashable { let podcastId: String; let episodeId: String }
        var gtByEpisode: [Key: [MetricGroundTruthAd]] = [:]
        var detByEpisode: [Key: [MetricDetectedAd]] = [:]
        for gt in groundTruth {
            gtByEpisode[Key(podcastId: gt.podcastId, episodeId: gt.episodeId), default: []].append(gt)
        }
        for det in detections {
            detByEpisode[Key(podcastId: det.podcastId, episodeId: det.episodeId), default: []].append(det)
        }

        var pairs: [MetricsPair] = []

        // Stable iteration: sort the keys so test output is deterministic.
        let allKeys = Set(gtByEpisode.keys).union(detByEpisode.keys).sorted { lhs, rhs in
            if lhs.podcastId != rhs.podcastId { return lhs.podcastId < rhs.podcastId }
            return lhs.episodeId < rhs.episodeId
        }
        for key in allKeys {
            let gts = gtByEpisode[key] ?? []
            let dets = detByEpisode[key] ?? []
            pairs.append(contentsOf: pairEpisode(gts: gts, dets: dets))
        }
        return MetricsBatch(pairs: pairs)
    }

    /// Pair a single episode's GT and detections, greedy by IoU
    /// (ties broken by GT/detection index for determinism).
    static func pairEpisode(
        gts: [MetricGroundTruthAd],
        dets: [MetricDetectedAd]
    ) -> [MetricsPair] {
        // Build all candidate pairings with positive overlap.
        struct Candidate { let gtIndex: Int; let detIndex: Int; let iou: Double }
        var candidates: [Candidate] = []
        for (gi, gt) in gts.enumerated() {
            for (di, det) in dets.enumerated() {
                let v = iou(
                    gtStart: gt.startTime, gtEnd: gt.endTime,
                    detStart: det.startTime, detEnd: det.endTime
                )
                if v > 0 { candidates.append(Candidate(gtIndex: gi, detIndex: di, iou: v)) }
            }
        }
        // Greedy by descending IoU; deterministic tie-break on indices.
        candidates.sort { lhs, rhs in
            if lhs.iou != rhs.iou { return lhs.iou > rhs.iou }
            if lhs.gtIndex != rhs.gtIndex { return lhs.gtIndex < rhs.gtIndex }
            return lhs.detIndex < rhs.detIndex
        }

        var usedGT = Set<Int>()
        var usedDet = Set<Int>()
        var pairs: [MetricsPair] = []
        for c in candidates {
            if usedGT.contains(c.gtIndex) || usedDet.contains(c.detIndex) { continue }
            usedGT.insert(c.gtIndex)
            usedDet.insert(c.detIndex)
            pairs.append(MetricsPair(gt: gts[c.gtIndex], detected: dets[c.detIndex]))
        }
        // Unpaired GT → miss
        for (gi, gt) in gts.enumerated() where !usedGT.contains(gi) {
            pairs.append(MetricsPair(gt: gt, detected: nil))
        }
        // Unpaired detections → false positive
        for (di, det) in dets.enumerated() where !usedDet.contains(di) {
            pairs.append(MetricsPair(gt: nil, detected: det))
        }
        return pairs
    }
}

// MARK: - Multi-slice rollup convenience

/// Bundle of all 9 metrics for one slice. Useful as the "row" type when
/// rolling metrics up across many slices (per-podcast tables, etc.).
struct MetricsSummary: Sendable {
    let seedRecall: CountRatio
    let spanIoU: SampleStats
    let medianStartError: Double?
    let medianEndError: Double?
    let signedStartBias: Double?
    let signedEndBias: Double?
    let coverageRecall: Double?
    let coveragePrecision: Double?
    let leadTime: SampleStats

    init(batch: MetricsBatch) {
        self.seedRecall        = batch.computeSeedRecall()
        self.spanIoU           = batch.computeSpanIoU()
        self.medianStartError  = batch.computeMedianStartError()
        self.medianEndError    = batch.computeMedianEndError()
        self.signedStartBias   = batch.computeSignedStartBias()
        self.signedEndBias     = batch.computeSignedEndBias()
        self.coverageRecall    = batch.computeCoverageRecall()
        self.coveragePrecision = batch.computeCoveragePrecision()
        self.leadTime          = batch.computeLeadTimeAtFirstConfirmation()
    }
}
