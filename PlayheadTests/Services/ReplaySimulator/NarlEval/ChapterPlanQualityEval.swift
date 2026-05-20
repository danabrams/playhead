// ChapterPlanQualityEval.swift
// playhead-au2v.1.21: Test-target-only evaluator that scores a
// `ChapterPlan` (the orchestration envelope produced by the chapter
// generation phase) against a hand-labeled `GoldenChapterSet`.
//
// Why test-target-only: this evaluator is tooling for offline quality
// gates. It reads `ChapterPlan` (a production type) and emits a
// `ChapterPlanQualityReport` (a test-target-only type). It does not
// influence production behavior at runtime. Bead 22 will consume this
// runner against real curated golden sets; this bead delivers the
// infrastructure and exercises it via synthetic fixtures.
//
// Metric definitions (see also ChapterPlanQualityReport doc comments):
//
//   Boundary recall:
//     For each ground-truth boundary, did the plan emit a candidate
//     whose `startTime` falls within ±toleranceSeconds? Numerator =
//     ground-truth boundaries with at least one candidate within
//     tolerance; denominator = total ground-truth boundaries.
//
//   Boundary precision:
//     For each plan candidate, was there a ground-truth boundary
//     within ±toleranceSeconds? Numerator = candidates with at least
//     one ground-truth match within tolerance; denominator = total
//     candidates.
//
//   Disposition accuracy:
//     Pairs are formed via a deterministic greedy nearest-pair matcher.
//     Step 1: enumerate every (golden, candidate) cross-product where
//     |golden.start - candidate.start| <= toleranceSeconds. Step 2:
//     repeatedly take the smallest-distance pair from the remaining
//     candidates, marking both sides used; ties break by smaller
//     golden index, then smaller candidate index, deterministically.
//     Step 3: accuracy = pairs whose dispositions agree / total
//     matched pairs. Note: this matcher is for disposition pairing
//     ONLY — recall and precision use a simpler "is there ANY
//     within-tolerance counterpart?" criterion (see above) and
//     therefore can report higher matched counts than this pair set
//     when goldens or candidates have multiple within-tolerance
//     neighbors.
//
//   Topic overlap (sanity):
//     Lightweight keyword overlap. Both labels are tokenized to
//     lowercased alphanumeric words (length >= 1); a shared token is
//     a match. Overlap = |intersection| / |union| (Jaccard). The
//     matcher returns `.match` when both descriptors tokenize to
//     a non-empty set and overlap >= `topicOverlapMinimum`, `.miss`
//     when both tokenize to non-empty sets but overlap is below
//     threshold, and `.notApplicable` when EITHER side tokenizes to
//     the empty set (nil, empty string, or any string composed
//     entirely of non-alphanumeric characters such as whitespace
//     or punctuation).
//
// Zero-denominator contract (documented; tests pin):
//   When a denominator is zero (empty plan or empty golden set or no
//   matched pairs), the metric resolves to 0.0 — never NaN — so the
//   report is always JSON-Codable. Consumers that need to distinguish
//   "vacuously zero" from "zero-out-of-N" can read the matched/total
//   counts in `BoundaryCounts` and the matched-pair count in
//   `dispositionMatchedPairs`.

import Foundation
@testable import Playhead

// MARK: - Golden-set schema (test-target only)

/// One hand-labeled chapter boundary in a golden set. The
/// `startTimeSeconds` defines the boundary; the disposition and
/// optional topic label define the expected labeler output for the
/// chapter that begins at that boundary.
struct GoldenChapter: Codable, Sendable, Equatable {
    let startTimeSeconds: Double
    let expectedDisposition: ChapterDisposition
    let expectedTopicLabel: String?
}

/// Envelope around a list of golden chapters for a single episode.
///
/// `episodeContentHash` matches the field on `ChapterPlan` and is the
/// bind point between a frozen-trace plan on disk and its labels.
/// `notes` is freeform documentation (e.g. "synthetic happy path"),
/// never user data.
struct GoldenChapterSet: Codable, Sendable, Equatable {
    let episodeId: String
    let episodeContentHash: String
    let chapters: [GoldenChapter]
    let notes: String?
}

// MARK: - Report

/// Thresholds the evaluator was configured with for a single run.
/// Captured in the report so a stored report is self-describing.
struct ChapterPlanQualityThresholds: Codable, Sendable, Equatable {
    /// Symmetric window (in seconds) around each ground-truth boundary
    /// inside which a plan candidate is considered a match.
    let boundaryToleranceSeconds: Double
    /// Minimum Jaccard overlap (0...1) between the tokenized expected
    /// and observed topic descriptors for the topic-label matcher to
    /// return `.match`.
    let topicOverlapMinimum: Double
}

/// Raw counts behind a precision/recall fraction. Stored alongside
/// the derived metric so consumers can recompute or aggregate.
struct BoundaryCounts: Codable, Sendable, Equatable {
    let matched: Int
    let total: Int
    /// `matched / total`, or `0.0` when `total == 0` (documented
    /// zero-denominator contract).
    var fraction: Double {
        total > 0 ? Double(matched) / Double(total) : 0.0
    }
}

/// Per-episode subset of the global metrics, scoped to one
/// `episodeId`. The matched/false-positive/missed counts are reported
/// in absolute integers (not just as ratios) so a downstream
/// aggregate can reconstruct global metrics by summing across
/// episodes.
struct PerEpisodeQuality: Codable, Sendable, Equatable {
    let episodeId: String
    let boundaryRecall: BoundaryCounts
    let boundaryPrecision: BoundaryCounts
    /// Number of matched (ground-truth, candidate) pairs that agreed
    /// on disposition.
    let dispositionMatchedAgreed: Int
    /// Number of matched (ground-truth, candidate) pairs total.
    let dispositionMatchedPairs: Int
    /// `dispositionMatchedAgreed / dispositionMatchedPairs`, or `0.0`
    /// when there were zero matched pairs (documented zero-denominator
    /// contract).
    var dispositionAccuracy: Double {
        dispositionMatchedPairs > 0
            ? Double(dispositionMatchedAgreed) / Double(dispositionMatchedPairs)
            : 0.0
    }
    /// Ground-truth boundaries with no plan candidate inside tolerance.
    let missedBoundaries: Int
    /// Plan candidates with no ground-truth boundary inside tolerance.
    let falsePositiveBoundaries: Int
    /// Per-disposition confusion limited to this episode. Outer key is
    /// the ground-truth disposition, inner key the labeler's emitted
    /// disposition. Counts only matched pairs.
    let perDispositionConfusion: [ChapterDisposition: [ChapterDisposition: Int]]
    /// Topic-label match counts limited to this episode.
    let topicLabelMatches: TopicLabelMatchCounts
}

/// Aggregate counts of topic-label matcher outcomes.
struct TopicLabelMatchCounts: Codable, Sendable, Equatable {
    /// Pairs where both descriptors were present and overlap met the
    /// threshold.
    let matched: Int
    /// Pairs where both descriptors were present but overlap was below
    /// threshold.
    let mismatched: Int
    /// Pairs where at least one descriptor was nil or empty (the
    /// matcher returned `.notApplicable`).
    let notApplicable: Int
}

/// Top-level report emitted by `ChapterPlanQualityEval`. Codable so it
/// can be archived alongside a frozen plan on disk; Sendable so it can
/// cross actor boundaries in tests.
struct ChapterPlanQualityReport: Codable, Sendable, Equatable {
    /// Aggregate boundary recall across every episode in the report.
    let boundaryRecall: BoundaryCounts
    /// Aggregate boundary precision across every episode in the
    /// report.
    let boundaryPrecision: BoundaryCounts
    /// Aggregate disposition accuracy across all matched pairs.
    let dispositionMatchedAgreed: Int
    let dispositionMatchedPairs: Int
    /// `dispositionMatchedAgreed / dispositionMatchedPairs`, or `0.0`
    /// when there were zero matched pairs (documented zero-denominator
    /// contract).
    var dispositionAccuracy: Double {
        dispositionMatchedPairs > 0
            ? Double(dispositionMatchedAgreed) / Double(dispositionMatchedPairs)
            : 0.0
    }
    /// Aggregate per-disposition confusion across every matched pair
    /// in the report. Outer key = ground-truth disposition; inner key
    /// = labeler's emitted disposition.
    let perDispositionConfusion: [ChapterDisposition: [ChapterDisposition: Int]]
    /// Aggregate topic-label match counts.
    let topicLabelMatches: TopicLabelMatchCounts
    /// Per-episode breakdown. The outer dict is keyed by `episodeId`
    /// (the same id stored on the input `GoldenChapterSet`).
    let perEpisode: [String: PerEpisodeQuality]
    /// Thresholds the evaluator was configured with at run time.
    let thresholdsUsed: ChapterPlanQualityThresholds
}

// MARK: - Evaluator

/// Pure evaluator. All inputs are values; the evaluator is stateless,
/// so multiple runs with the same inputs are byte-identical (modulo
/// dictionary ordering, which the report's `Equatable` conformance
/// is insensitive to).
struct ChapterPlanQualityEval: Sendable {

    /// Default boundary tolerance: ±15 seconds. Locked into the bead
    /// spec; consumers may override for sweeps.
    static let defaultBoundaryToleranceSeconds: Double = 15.0
    /// Default topic-overlap minimum: 0.5 (half the merged token set
    /// must be shared). Locked into the bead spec.
    static let defaultTopicOverlapMinimum: Double = 0.5

    let thresholds: ChapterPlanQualityThresholds

    init(
        boundaryToleranceSeconds: Double = ChapterPlanQualityEval.defaultBoundaryToleranceSeconds,
        topicOverlapMinimum: Double = ChapterPlanQualityEval.defaultTopicOverlapMinimum
    ) {
        // Defensive guards: a non-finite or negative tolerance would
        // make every candidate either trivially match or trivially
        // miss; rather than silently swallow the bug, clamp to 0 and
        // let the test surface the misconfiguration via assertions.
        let safeTolerance: Double
        if boundaryToleranceSeconds.isFinite, boundaryToleranceSeconds >= 0 {
            safeTolerance = boundaryToleranceSeconds
        } else {
            safeTolerance = 0
        }
        let safeOverlap: Double
        if topicOverlapMinimum.isFinite {
            safeOverlap = max(0.0, min(1.0, topicOverlapMinimum))
        } else {
            safeOverlap = ChapterPlanQualityEval.defaultTopicOverlapMinimum
        }
        self.thresholds = ChapterPlanQualityThresholds(
            boundaryToleranceSeconds: safeTolerance,
            topicOverlapMinimum: safeOverlap
        )
    }

    // MARK: - Single-episode evaluation

    /// Evaluate a single (`ChapterPlan`, `GoldenChapterSet`) pair into
    /// a `PerEpisodeQuality`. The `episodeId` field of the result is
    /// taken verbatim from `golden.episodeId` (NOT from the plan,
    /// which doesn't store one).
    func evaluateEpisode(
        plan: ChapterPlan,
        golden: GoldenChapterSet
    ) -> PerEpisodeQuality {
        let candidates = plan.chapters
        let goldens = golden.chapters

        // Per the bead spec: the boundary detector emits candidate
        // boundaries; we treat each chapter's `startTime` as one
        // boundary. We do not double-count chapter `endTime`s — the
        // end of one chapter is the start of the next (or the end of
        // the episode), and the spec's metric definition is per-
        // boundary, not per-chapter-edge.
        let candidateStarts = candidates.map(\.startTime)
        let goldenStarts = goldens.map(\.startTimeSeconds)

        // -- Boundary recall (per ground-truth boundary)
        var matchedGoldens = 0
        for goldenStart in goldenStarts {
            if Self.anyWithinTolerance(
                target: goldenStart,
                values: candidateStarts,
                tolerance: thresholds.boundaryToleranceSeconds
            ) {
                matchedGoldens += 1
            }
        }
        let recallCounts = BoundaryCounts(
            matched: matchedGoldens,
            total: goldenStarts.count
        )

        // -- Boundary precision (per candidate)
        var matchedCandidates = 0
        for candidateStart in candidateStarts {
            if Self.anyWithinTolerance(
                target: candidateStart,
                values: goldenStarts,
                tolerance: thresholds.boundaryToleranceSeconds
            ) {
                matchedCandidates += 1
            }
        }
        let precisionCounts = BoundaryCounts(
            matched: matchedCandidates,
            total: candidateStarts.count
        )

        // Counts the user-facing report needs even when nothing matched.
        let missedBoundaries = goldenStarts.count - matchedGoldens
        let falsePositiveBoundaries = candidateStarts.count - matchedCandidates

        // -- Greedy nearest-pair matching for disposition accuracy.
        // Build (goldenIndex, candidateIndex, distance) for every pair
        // within tolerance, sort by (distance ascending, goldenIndex
        // ascending, candidateIndex ascending) so ties are broken
        // deterministically, then walk the list and assign each pair
        // greedily, marking both endpoints used.
        //
        // Why greedy (and not minimum-weight bipartite matching):
        // boundaries inside `boundaryToleranceSeconds` of one another
        // are rare in real episodes (the boundary detector deduplicates
        // at the candidate level upstream). Greedy by ascending
        // distance produces the optimal pairing whenever no two pairs
        // can both gain by swapping, which holds for any case where
        // each candidate's nearest in-tolerance golden is also that
        // golden's nearest in-tolerance candidate. We accept a small
        // sub-optimality penalty for the (rare) interleaved case in
        // exchange for a simple deterministic implementation; bead 22
        // can swap the matcher for Hungarian if real-data goldens
        // surface such cases.
        struct PairCandidate {
            let goldenIndex: Int
            let candidateIndex: Int
            let distance: Double
        }
        var pairs: [PairCandidate] = []
        for (gi, gStart) in goldenStarts.enumerated() {
            // Skip non-finite goldens up-front for symmetry with
            // `anyWithinTolerance` and to make the NaN-handling
            // contract explicit (rather than relying on the implicit
            // `NaN <= tolerance == false` semantics of the comparison
            // below).
            guard gStart.isFinite else { continue }
            for (ci, cStart) in candidateStarts.enumerated() {
                guard cStart.isFinite else { continue }
                let d = abs(gStart - cStart)
                if d <= thresholds.boundaryToleranceSeconds {
                    pairs.append(PairCandidate(goldenIndex: gi, candidateIndex: ci, distance: d))
                }
            }
        }
        pairs.sort { lhs, rhs in
            if lhs.distance != rhs.distance { return lhs.distance < rhs.distance }
            if lhs.goldenIndex != rhs.goldenIndex { return lhs.goldenIndex < rhs.goldenIndex }
            return lhs.candidateIndex < rhs.candidateIndex
        }
        var usedGolden = Set<Int>()
        var usedCandidate = Set<Int>()
        var matchedPairs: [(Int, Int)] = []
        for pair in pairs {
            if usedGolden.contains(pair.goldenIndex) { continue }
            if usedCandidate.contains(pair.candidateIndex) { continue }
            usedGolden.insert(pair.goldenIndex)
            usedCandidate.insert(pair.candidateIndex)
            matchedPairs.append((pair.goldenIndex, pair.candidateIndex))
        }

        // -- Confusion matrix + agreement count from matched pairs.
        var confusion = Self.emptyConfusionMatrix()
        var agreed = 0
        var topicMatched = 0
        var topicMismatched = 0
        var topicNA = 0
        for (gi, ci) in matchedPairs {
            let expected = goldens[gi].expectedDisposition
            let observed = candidates[ci].disposition
            confusion[expected, default: [:]][observed, default: 0] += 1
            if expected == observed { agreed += 1 }

            // Topic comparison: for inferred chapters produced by
            // the chapter-generation phase, the FM-emitted
            // `topicDescriptor` (see `ChapterLabel.topicDescriptor`)
            // is what the labeler stores into `ChapterEvidence.title`
            // — `title` is the only string field available on the
            // production type, and the assembler in
            // `ChapterPlanAssembler` writes the descriptor into
            // `title` for inferred-source chapters. We compare
            // golden's `expectedTopicLabel` against plan's `title`
            // here. For creator-source chapters (`.id3`/`.pc20`/
            // `.rssInline`) the title is the creator-supplied
            // chapter title and the matcher's output should be
            // interpreted as "label-text overlap" rather than
            // strictly "FM topic descriptor agreement".
            switch Self.topicLabelOutcome(
                expected: goldens[gi].expectedTopicLabel,
                observed: candidates[ci].title,
                threshold: thresholds.topicOverlapMinimum
            ) {
            case .match: topicMatched += 1
            case .miss: topicMismatched += 1
            case .notApplicable: topicNA += 1
            }
        }

        return PerEpisodeQuality(
            episodeId: golden.episodeId,
            boundaryRecall: recallCounts,
            boundaryPrecision: precisionCounts,
            dispositionMatchedAgreed: agreed,
            dispositionMatchedPairs: matchedPairs.count,
            missedBoundaries: missedBoundaries,
            falsePositiveBoundaries: falsePositiveBoundaries,
            perDispositionConfusion: confusion,
            topicLabelMatches: TopicLabelMatchCounts(
                matched: topicMatched,
                mismatched: topicMismatched,
                notApplicable: topicNA
            )
        )
    }

    // MARK: - Multi-episode evaluation

    /// Evaluate a list of (plan, golden) pairs into a single report.
    ///
    /// Episode ids must be unique inside `pairs`; if a duplicate is
    /// passed, the later entry wins in `perEpisode`. Aggregates are
    /// summed across every pair as supplied (so a duplicate pair is
    /// double-counted in aggregates but appears once in `perEpisode`).
    /// Tests assert single-episode runs only, so this duplicate
    /// behavior is documented but not exercised in production.
    func evaluate(
        pairs: [(plan: ChapterPlan, golden: GoldenChapterSet)]
    ) -> ChapterPlanQualityReport {
        var perEpisode: [String: PerEpisodeQuality] = [:]
        var aggMatchedRecall = 0
        var aggTotalRecall = 0
        var aggMatchedPrecision = 0
        var aggTotalPrecision = 0
        var aggMatchedAgreed = 0
        var aggMatchedPairs = 0
        var aggConfusion = Self.emptyConfusionMatrix()
        var aggTopicMatched = 0
        var aggTopicMismatched = 0
        var aggTopicNA = 0

        for pair in pairs {
            let perEp = evaluateEpisode(plan: pair.plan, golden: pair.golden)
            perEpisode[perEp.episodeId] = perEp

            aggMatchedRecall += perEp.boundaryRecall.matched
            aggTotalRecall += perEp.boundaryRecall.total
            aggMatchedPrecision += perEp.boundaryPrecision.matched
            aggTotalPrecision += perEp.boundaryPrecision.total
            aggMatchedAgreed += perEp.dispositionMatchedAgreed
            aggMatchedPairs += perEp.dispositionMatchedPairs
            for (expected, row) in perEp.perDispositionConfusion {
                for (observed, count) in row {
                    aggConfusion[expected, default: [:]][observed, default: 0] += count
                }
            }
            aggTopicMatched += perEp.topicLabelMatches.matched
            aggTopicMismatched += perEp.topicLabelMatches.mismatched
            aggTopicNA += perEp.topicLabelMatches.notApplicable
        }

        return ChapterPlanQualityReport(
            boundaryRecall: BoundaryCounts(matched: aggMatchedRecall, total: aggTotalRecall),
            boundaryPrecision: BoundaryCounts(matched: aggMatchedPrecision, total: aggTotalPrecision),
            dispositionMatchedAgreed: aggMatchedAgreed,
            dispositionMatchedPairs: aggMatchedPairs,
            perDispositionConfusion: aggConfusion,
            topicLabelMatches: TopicLabelMatchCounts(
                matched: aggTopicMatched,
                mismatched: aggTopicMismatched,
                notApplicable: aggTopicNA
            ),
            perEpisode: perEpisode,
            thresholdsUsed: thresholds
        )
    }

    // MARK: - Topic overlap (exposed internal so tests can pin)

    /// Outcome of the topic-label matcher.
    enum TopicLabelOutcome: Sendable, Equatable {
        case match
        case miss
        case notApplicable
    }

    /// Compute the topic-label match outcome.
    /// Public-visibility so the tests can pin the matcher in isolation.
    static func topicLabelOutcome(
        expected: String?,
        observed: String?,
        threshold: Double
    ) -> TopicLabelOutcome {
        let expTokens = tokenize(expected)
        let obsTokens = tokenize(observed)
        if expTokens.isEmpty || obsTokens.isEmpty {
            return .notApplicable
        }
        // Both token sets are non-empty here, so the union is also
        // non-empty (size >= max(|expTokens|, |obsTokens|)) and the
        // division below cannot divide by zero. A defensive
        // `!union.isEmpty` guard would be dead code given the
        // earlier `isEmpty` check; we omit it to avoid implying a
        // contract the function does not actually enforce.
        let intersection = expTokens.intersection(obsTokens)
        let union = expTokens.union(obsTokens)
        let overlap = Double(intersection.count) / Double(union.count)
        return overlap >= threshold ? .match : .miss
    }

    /// Tokenize a topic descriptor into a set of lowercased
    /// alphanumeric words. Returns an empty set for nil/empty inputs.
    /// Internal so tests can pin the canonicalization contract.
    static func tokenize(_ raw: String?) -> Set<String> {
        guard let raw, !raw.isEmpty else { return [] }
        let lowered = raw.lowercased()
        var result: Set<String> = []
        var current = ""
        for scalar in lowered.unicodeScalars {
            if CharacterSet.alphanumerics.contains(scalar) {
                current.unicodeScalars.append(scalar)
            } else {
                if !current.isEmpty { result.insert(current); current = "" }
            }
        }
        if !current.isEmpty { result.insert(current) }
        return result
    }

    // MARK: - Helpers

    /// `true` iff at least one entry of `values` is within `tolerance`
    /// of `target`. NaN/Inf entries are skipped (a corrupt timestamp
    /// cannot match anything).
    private static func anyWithinTolerance(
        target: Double,
        values: [Double],
        tolerance: Double
    ) -> Bool {
        guard target.isFinite else { return false }
        for value in values {
            guard value.isFinite else { continue }
            if abs(value - target) <= tolerance { return true }
        }
        return false
    }

    /// All-zero confusion matrix with one row per `ChapterDisposition`
    /// case. Producing a fully-populated matrix (not a sparse one)
    /// keeps `Equatable` comparisons in tests obvious — every cell is
    /// always present with an explicit count.
    private static func emptyConfusionMatrix() -> [ChapterDisposition: [ChapterDisposition: Int]] {
        var matrix: [ChapterDisposition: [ChapterDisposition: Int]] = [:]
        let cases: [ChapterDisposition] = [.adBreak, .content, .ambiguous]
        for expected in cases {
            var row: [ChapterDisposition: Int] = [:]
            for observed in cases {
                row[observed] = 0
            }
            matrix[expected] = row
        }
        return matrix
    }
}

// MARK: - ChapterDisposition Codable-as-dictionary-key support

// `[ChapterDisposition: Int]` does not Codable as a JSON dictionary by
// default in Swift (only String and Int keys do). We do not currently
// archive `ChapterPlanQualityReport` to JSON in production — the bead
// scope is in-memory test consumption — but we keep the report Codable
// so bead 22's frozen-trace runner can persist it. Provide the
// conversion via a custom Codable conformance on the report fields
// that use `ChapterDisposition` as a key.
//
// The wire format is: every dictionary keyed by `ChapterDisposition`
// is encoded as `[String: Value]` using the enum's `rawValue`
// (`"adBreak"`, `"content"`, `"ambiguous"`). Unknown keys on decode
// throw `DecodingError.dataCorrupted`.

private struct DispositionConfusionWire: Codable, Sendable, Equatable {
    let value: [ChapterDisposition: [ChapterDisposition: Int]]

    init(_ value: [ChapterDisposition: [ChapterDisposition: Int]]) {
        self.value = value
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let raw = try container.decode([String: [String: Int]].self)
        var result: [ChapterDisposition: [ChapterDisposition: Int]] = [:]
        for (rawExpected, row) in raw {
            guard let expected = ChapterDisposition(rawValue: rawExpected) else {
                throw DecodingError.dataCorruptedError(
                    in: container,
                    debugDescription: "Unknown ChapterDisposition raw value: \(rawExpected)"
                )
            }
            var rowOut: [ChapterDisposition: Int] = [:]
            for (rawObserved, count) in row {
                guard let observed = ChapterDisposition(rawValue: rawObserved) else {
                    throw DecodingError.dataCorruptedError(
                        in: container,
                        debugDescription: "Unknown ChapterDisposition raw value: \(rawObserved)"
                    )
                }
                rowOut[observed] = count
            }
            result[expected] = rowOut
        }
        self.value = result
    }

    func encode(to encoder: Encoder) throws {
        var raw: [String: [String: Int]] = [:]
        for (expected, row) in value {
            var rowOut: [String: Int] = [:]
            for (observed, count) in row {
                rowOut[observed.rawValue] = count
            }
            raw[expected.rawValue] = rowOut
        }
        var container = encoder.singleValueContainer()
        try container.encode(raw)
    }
}

extension PerEpisodeQuality {
    enum CodingKeys: String, CodingKey {
        case episodeId
        case boundaryRecall
        case boundaryPrecision
        case dispositionMatchedAgreed
        case dispositionMatchedPairs
        case missedBoundaries
        case falsePositiveBoundaries
        case perDispositionConfusion
        case topicLabelMatches
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.episodeId = try c.decode(String.self, forKey: .episodeId)
        self.boundaryRecall = try c.decode(BoundaryCounts.self, forKey: .boundaryRecall)
        self.boundaryPrecision = try c.decode(BoundaryCounts.self, forKey: .boundaryPrecision)
        self.dispositionMatchedAgreed = try c.decode(Int.self, forKey: .dispositionMatchedAgreed)
        self.dispositionMatchedPairs = try c.decode(Int.self, forKey: .dispositionMatchedPairs)
        self.missedBoundaries = try c.decode(Int.self, forKey: .missedBoundaries)
        self.falsePositiveBoundaries = try c.decode(Int.self, forKey: .falsePositiveBoundaries)
        let wire = try c.decode(DispositionConfusionWire.self, forKey: .perDispositionConfusion)
        self.perDispositionConfusion = wire.value
        self.topicLabelMatches = try c.decode(TopicLabelMatchCounts.self, forKey: .topicLabelMatches)
    }

    func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(episodeId, forKey: .episodeId)
        try c.encode(boundaryRecall, forKey: .boundaryRecall)
        try c.encode(boundaryPrecision, forKey: .boundaryPrecision)
        try c.encode(dispositionMatchedAgreed, forKey: .dispositionMatchedAgreed)
        try c.encode(dispositionMatchedPairs, forKey: .dispositionMatchedPairs)
        try c.encode(missedBoundaries, forKey: .missedBoundaries)
        try c.encode(falsePositiveBoundaries, forKey: .falsePositiveBoundaries)
        try c.encode(DispositionConfusionWire(perDispositionConfusion), forKey: .perDispositionConfusion)
        try c.encode(topicLabelMatches, forKey: .topicLabelMatches)
    }
}

extension ChapterPlanQualityReport {
    enum CodingKeys: String, CodingKey {
        case boundaryRecall
        case boundaryPrecision
        case dispositionMatchedAgreed
        case dispositionMatchedPairs
        case perDispositionConfusion
        case topicLabelMatches
        case perEpisode
        case thresholdsUsed
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.boundaryRecall = try c.decode(BoundaryCounts.self, forKey: .boundaryRecall)
        self.boundaryPrecision = try c.decode(BoundaryCounts.self, forKey: .boundaryPrecision)
        self.dispositionMatchedAgreed = try c.decode(Int.self, forKey: .dispositionMatchedAgreed)
        self.dispositionMatchedPairs = try c.decode(Int.self, forKey: .dispositionMatchedPairs)
        let wire = try c.decode(DispositionConfusionWire.self, forKey: .perDispositionConfusion)
        self.perDispositionConfusion = wire.value
        self.topicLabelMatches = try c.decode(TopicLabelMatchCounts.self, forKey: .topicLabelMatches)
        self.perEpisode = try c.decode([String: PerEpisodeQuality].self, forKey: .perEpisode)
        self.thresholdsUsed = try c.decode(ChapterPlanQualityThresholds.self, forKey: .thresholdsUsed)
    }

    func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(boundaryRecall, forKey: .boundaryRecall)
        try c.encode(boundaryPrecision, forKey: .boundaryPrecision)
        try c.encode(dispositionMatchedAgreed, forKey: .dispositionMatchedAgreed)
        try c.encode(dispositionMatchedPairs, forKey: .dispositionMatchedPairs)
        try c.encode(DispositionConfusionWire(perDispositionConfusion), forKey: .perDispositionConfusion)
        try c.encode(topicLabelMatches, forKey: .topicLabelMatches)
        try c.encode(perEpisode, forKey: .perEpisode)
        try c.encode(thresholdsUsed, forKey: .thresholdsUsed)
    }
}

// MARK: - Frozen-trace runner

/// Helper used by bead 22 to bind a real frozen `ChapterPlan` from a
/// `ChapterPlanCache` to a hand-labeled `GoldenChapterSet` loaded from
/// disk and emit a quality report.
///
/// The runner is stateless and exposes two entry points: one that
/// fetches the plan from a cache (intended for the dogfood path bead
/// 22 will set up), and one that takes a plan + golden directly
/// (intended for synthetic-fixture tests in this bead). Keeping both
/// surfaces means the tests in this bead exercise the same code path
/// bead 22 will use.
enum ChapterPlanQualityRunner {

    /// Errors the runner can surface to callers. Kept minimal — this
    /// is a test helper, not a production failure point.
    enum RunnerError: Error, Equatable, CustomStringConvertible {
        /// The cache returned `nil` for the requested content hash.
        /// Either the plan was never produced or the schema-version
        /// gate dropped it (see `ChapterPlanCache.get`).
        case planNotInCache(contentHash: String)
        /// The plan's `episodeContentHash` does not match the
        /// golden set's `episodeContentHash`. The caller almost
        /// certainly mis-paired a plan with a golden from a different
        /// episode.
        case contentHashMismatch(planHash: String, goldenHash: String)

        var description: String {
            switch self {
            case .planNotInCache(let h):
                return "ChapterPlanQualityRunner: plan not in cache for hash=\(h)"
            case .contentHashMismatch(let p, let g):
                return "ChapterPlanQualityRunner: contentHash mismatch plan=\(p) golden=\(g)"
            }
        }
    }

    /// Evaluate a plan loaded from `cache` against the supplied
    /// `golden`. Throws if the plan is missing or its content hash
    /// disagrees with the golden's.
    static func runFromCache(
        cache: ChapterPlanCache,
        golden: GoldenChapterSet,
        evaluator: ChapterPlanQualityEval = ChapterPlanQualityEval()
    ) async throws -> ChapterPlanQualityReport {
        guard let plan = await cache.get(contentHash: golden.episodeContentHash) else {
            throw RunnerError.planNotInCache(contentHash: golden.episodeContentHash)
        }
        return try run(plan: plan, golden: golden, evaluator: evaluator)
    }

    /// Evaluate a plan against a golden directly. Throws on content-hash
    /// mismatch; returns the report otherwise.
    static func run(
        plan: ChapterPlan,
        golden: GoldenChapterSet,
        evaluator: ChapterPlanQualityEval = ChapterPlanQualityEval()
    ) throws -> ChapterPlanQualityReport {
        guard plan.episodeContentHash == golden.episodeContentHash else {
            throw RunnerError.contentHashMismatch(
                planHash: plan.episodeContentHash,
                goldenHash: golden.episodeContentHash
            )
        }
        return evaluator.evaluate(pairs: [(plan: plan, golden: golden)])
    }
}

// MARK: - Golden-set loader

/// Test-target helper to load `GoldenChapterSet` JSON files from disk.
/// Uses `#filePath` to locate the fixtures directory (the same
/// convention `ChapterSignalCaseStudyTests` and the dated NARL
/// FrozenTrace fixtures already use; the directory is excluded from
/// the test bundle's resource copy in `project.yml` so the JSON files
/// are not bundled and not subject to the "Multiple commands produce"
/// resource-copy collision).
enum ChapterPlanGoldenSetLoader {

    /// `PlayheadTests/Fixtures/ChapterPlanGoldenSet/synthetic/` resolved
    /// from the location of THIS source file. The file lives at
    /// `PlayheadTests/Services/ReplaySimulator/NarlEval/ChapterPlanQualityEval.swift`,
    /// so we ascend four directories — file → NarlEval → ReplaySimulator
    /// → Services → PlayheadTests — and descend into Fixtures.
    static func syntheticDirectory(_ filePath: String = #filePath) -> URL {
        URL(fileURLWithPath: filePath)
            .deletingLastPathComponent() // NarlEval
            .deletingLastPathComponent() // ReplaySimulator
            .deletingLastPathComponent() // Services
            .deletingLastPathComponent() // PlayheadTests
            .appendingPathComponent("Fixtures", isDirectory: true)
            .appendingPathComponent("ChapterPlanGoldenSet", isDirectory: true)
            .appendingPathComponent("synthetic", isDirectory: true)
    }

    /// Load a single golden-set fixture by basename (without `.json`).
    static func loadSynthetic(
        named name: String,
        filePath: String = #filePath
    ) throws -> GoldenChapterSet {
        let url = syntheticDirectory(filePath)
            .appendingPathComponent("\(name).json", isDirectory: false)
        let data = try Data(contentsOf: url)
        return try JSONDecoder().decode(GoldenChapterSet.self, from: data)
    }

    /// Enumerate every `.json` file in the synthetic fixtures
    /// directory, sorted by filename for determinism.
    ///
    /// Returns labeled tuples (`url`, `set`) so call-sites can use
    /// `.url`/`.set` rather than `$0.0`/`$0.1`. Positional destructuring
    /// (`for (url, set) in ...`) continues to work.
    static func allSyntheticFixtures(
        _ filePath: String = #filePath
    ) throws -> [(url: URL, set: GoldenChapterSet)] {
        let dir = syntheticDirectory(filePath)
        return try loadFixtures(in: dir)
    }

    /// `PlayheadTests/Fixtures/ChapterPlanGoldenSet/dogfood/` — sibling
    /// of `synthetic/`. Holds golden sets auto-converted from the
    /// hand-labeled real-podcast annotations under
    /// `TestFixtures/Corpus/Annotations/` (see
    /// `Scripts/convert_annotations_to_chapter_goldens.py`). Topic
    /// labels are anonymized to ad_type / "editorial content"; no
    /// advertiser, product, or confidence-note text is committed
    /// (au2v.1.22 privacy rule).
    static func dogfoodDirectory(_ filePath: String = #filePath) -> URL {
        URL(fileURLWithPath: filePath)
            .deletingLastPathComponent() // NarlEval
            .deletingLastPathComponent() // ReplaySimulator
            .deletingLastPathComponent() // Services
            .deletingLastPathComponent() // PlayheadTests
            .appendingPathComponent("Fixtures", isDirectory: true)
            .appendingPathComponent("ChapterPlanGoldenSet", isDirectory: true)
            .appendingPathComponent("dogfood", isDirectory: true)
    }

    /// Load a single dogfood golden-set fixture by basename (without `.json`).
    static func loadDogfood(
        named name: String,
        filePath: String = #filePath
    ) throws -> GoldenChapterSet {
        let url = dogfoodDirectory(filePath)
            .appendingPathComponent("\(name).json", isDirectory: false)
        let data = try Data(contentsOf: url)
        return try JSONDecoder().decode(GoldenChapterSet.self, from: data)
    }

    /// Enumerate every `.json` file in the dogfood fixtures directory,
    /// sorted by filename for determinism. Returns an empty array when
    /// the directory does not yet exist (so tests can run in checkouts
    /// where the converter has not been executed locally).
    static func allDogfoodFixtures(
        _ filePath: String = #filePath
    ) throws -> [(url: URL, set: GoldenChapterSet)] {
        let dir = dogfoodDirectory(filePath)
        guard FileManager.default.fileExists(atPath: dir.path) else { return [] }
        return try loadFixtures(in: dir)
    }

    private static func loadFixtures(
        in dir: URL
    ) throws -> [(url: URL, set: GoldenChapterSet)] {
        let entries = try FileManager.default.contentsOfDirectory(
            at: dir,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        )
        let decoder = JSONDecoder()
        return try entries
            .filter { $0.pathExtension == "json" }
            .sorted { $0.lastPathComponent < $1.lastPathComponent }
            .map { url in
                let data = try Data(contentsOf: url)
                return (url: url, set: try decoder.decode(GoldenChapterSet.self, from: data))
            }
    }
}
