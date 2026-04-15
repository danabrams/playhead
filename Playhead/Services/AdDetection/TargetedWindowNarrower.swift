import CryptoKit
import Foundation
import OSLog

private let narrowerLogger = Logger(
    subsystem: "com.playhead",
    category: "TargetedWindowNarrower"
)

/// Cycle 2 C5: per-phase narrowing config. Replaces the legacy hull model
/// (one contiguous range from leftmost to rightmost anchor) with per-anchor
/// windows that get merged when overlapping/adjacent and aborted when the
/// merged total exceeds `maxNarrowedSegmentsPerPhase`.
struct NarrowingConfig: Sendable, Equatable {
    /// Number of segments to include on each side of each anchor.
    let perAnchorPaddingSegments: Int
    /// Hard cap on the merged segment count for a single phase. When exceeded
    /// the runner aborts narrowing for that phase and falls back to the full
    /// segment list (root inputs).
    let maxNarrowedSegmentsPerPhase: Int
    /// playhead-7q3 (Phase 4): maximum absolute time distance, in seconds,
    /// allowed between a merged per-anchor window's outer edge and an
    /// `AcousticBreak` for the edge to be snapped to that break. Chosen as
    /// 2.0s from first principles: `AcousticBreakDetector` uses the
    /// `FeatureWindow` duration (2.0s at HEAD) as the grouping tolerance
    /// inside `mergeSignals`, so two break-worthy events within 2.0s are
    /// already considered "the same transition" upstream. Keeping this
    /// snap distance equal to that grouping tolerance means the narrower
    /// never pulls an edge toward a break that the detector itself would
    /// not consider co-located with the edge. Zero-shot: this constant is
    /// the feature-window width and is not per-show tuned.
    let acousticBreakSnapMaxDistanceSeconds: Double

    static let `default` = NarrowingConfig(
        perAnchorPaddingSegments: 5,
        maxNarrowedSegmentsPerPhase: 60,
        acousticBreakSnapMaxDistanceSeconds: 2.0
    )

    init(
        perAnchorPaddingSegments: Int,
        maxNarrowedSegmentsPerPhase: Int,
        acousticBreakSnapMaxDistanceSeconds: Double = 2.0
    ) {
        self.perAnchorPaddingSegments = perAnchorPaddingSegments
        self.maxNarrowedSegmentsPerPhase = maxNarrowedSegmentsPerPhase
        self.acousticBreakSnapMaxDistanceSeconds = acousticBreakSnapMaxDistanceSeconds
    }
}

/// Cycle 2 H13: a phase narrowing now distinguishes "narrow to N segments"
/// from "no anchors found, skip this phase entirely". The runner uses
/// `wasEmpty == true` to skip the phase without dispatching FM work and
/// without contributing to the recall sample numerator. The legacy
/// `fallbackIfEmpty` deterministic-seed segment was deleted: it wasted FM
/// budget and hid the diagnostic signal that a phase produces no anchors
/// for a given show.
///
/// Cycle 2 C5: when `aborted == true` the merged window count exceeded
/// `NarrowingConfig.maxNarrowedSegmentsPerPhase` and the narrower bailed
/// out so the caller can fall back to the full segment list (root inputs).
/// `narrowedSegments` is `nil` in that case.
struct PhaseNarrowingResult: Sendable {
    let narrowedSegments: [AdTranscriptSegment]?
    let wasEmpty: Bool
    let aborted: Bool

    static let empty = PhaseNarrowingResult(
        narrowedSegments: nil,
        wasEmpty: true,
        aborted: false
    )

    static func narrowed(_ segments: [AdTranscriptSegment]) -> PhaseNarrowingResult {
        PhaseNarrowingResult(narrowedSegments: segments, wasEmpty: false, aborted: false)
    }

    static let abortedExceededCap = PhaseNarrowingResult(
        narrowedSegments: nil,
        wasEmpty: false,
        aborted: true
    )
}

enum TargetedWindowNarrower {
    struct Inputs: Sendable {
        let analysisAssetId: String
        let podcastId: String
        let transcriptVersion: String
        let segments: [AdTranscriptSegment]
        let evidenceCatalog: EvidenceCatalog
        let auditWindowSampleRate: Double
        /// Cycle 2 Rev3-M3: included in `auditSeed` so consecutive
        /// observations of the same `(podcastId, transcriptVersion)` rotate
        /// across distinct audit windows. Defaults to 0 to preserve
        /// determinism for callers that have not yet wired the planner
        /// counter through. The runner passes
        /// `PodcastPlannerState.episodesSinceLastFullRescan` here.
        let episodesSinceLastFullRescan: Int
        /// playhead-7q3 (Phase 4): acoustic break points for the episode,
        /// produced by `AcousticBreakDetector.detectBreaks(in:)`. When
        /// non-empty, the narrower snaps the outer edges of each merged
        /// per-anchor window to the nearest break within
        /// `NarrowingConfig.acousticBreakSnapMaxDistanceSeconds`. When
        /// empty (the default, and the behavior for any caller that does
        /// not yet have feature windows available), the narrower falls
        /// back to the fixed `perAnchorPaddingSegments` behavior — so
        /// opting in to break-shaping is strictly additive.
        let acousticBreaks: [AcousticBreak]

        init(
            analysisAssetId: String,
            podcastId: String,
            transcriptVersion: String,
            segments: [AdTranscriptSegment],
            evidenceCatalog: EvidenceCatalog,
            auditWindowSampleRate: Double,
            episodesSinceLastFullRescan: Int = 0,
            acousticBreaks: [AcousticBreak] = []
        ) {
            self.analysisAssetId = analysisAssetId
            self.podcastId = podcastId
            self.transcriptVersion = transcriptVersion
            self.segments = segments
            self.evidenceCatalog = evidenceCatalog
            self.auditWindowSampleRate = auditWindowSampleRate
            self.episodesSinceLastFullRescan = episodesSinceLastFullRescan
            self.acousticBreaks = acousticBreaks
        }
    }

    /// Cycle 2 C5/H13: produce a `PhaseNarrowingResult` for `phase`.
    ///
    /// - `.fullEpisodeScan` always returns the full ordered segment list
    ///   (never empty, never aborted).
    /// - The two anchor-producing phases (`.scanHarvesterProposals`,
    ///   `.scanLikelyAdSlots`) build per-anchor windows of width
    ///   `2 * perAnchorPaddingSegments + 1`, clip to episode bounds, merge
    ///   overlapping/adjacent intervals, and:
    ///     - return `.empty` if there are no anchors (the phase is skipped),
    ///     - return `.abortedExceededCap` if the merged segment count
    ///       exceeds `maxNarrowedSegmentsPerPhase` (the caller falls back
    ///       to root inputs and bumps a telemetry counter),
    ///     - otherwise return `.narrowed(...)` with the merged segments.
    /// - `.scanRandomAuditWindows` is unaffected by C5/H13: it picks a
    ///   contiguous block via `auditSeed`. Audit can never be empty as
    ///   long as `orderedSegments` is non-empty.
    static func narrow(
        phase: BackfillJobPhase,
        inputs: Inputs,
        config: NarrowingConfig = .default
    ) -> PhaseNarrowingResult {
        let ordered = orderedSegments(inputs.segments)
        guard !ordered.isEmpty else { return .empty }

        switch phase {
        case .fullEpisodeScan:
            return .narrowed(ordered)
        case .scanHarvesterProposals:
            return narrowedResult(
                lineRefs: evidenceLineRefs(inputs: inputs),
                orderedSegments: ordered,
                acousticBreaks: inputs.acousticBreaks,
                config: config,
                phaseLabel: "scanHarvesterProposals"
            )
        case .scanLikelyAdSlots:
            return narrowedResult(
                lineRefs: lexicalCandidateLineRefs(inputs: inputs),
                orderedSegments: ordered,
                acousticBreaks: inputs.acousticBreaks,
                config: config,
                phaseLabel: "scanLikelyAdSlots"
            )
        case .scanRandomAuditWindows:
            return .narrowed(auditSegments(orderedSegments: ordered, inputs: inputs))
        case .metadataSeededRegion:
            // ef2.4.7: metadata-seeded regions use the same narrowing as harvester
            // proposals — evidence line refs define the window around seeded regions.
            return narrowedResult(
                lineRefs: evidenceLineRefs(inputs: inputs),
                orderedSegments: ordered,
                acousticBreaks: inputs.acousticBreaks,
                config: config,
                phaseLabel: "metadataSeededRegion"
            )
        }
    }

    /// Cycle 2 H13: union the segment indices across all non-empty
    /// non-fullEpisodeScan phases. Empty phases contribute nothing to the
    /// numerator of the recall sample. The full-rescan denominator is
    /// supplied separately by the runner from
    /// `fullRescanDetectedAdLineRefs.count`.
    static func predictedTargetedLineRefs(
        inputs: Inputs,
        config: NarrowingConfig = .default
    ) -> Set<Int> {
        var union = Set<Int>()
        for phase in BackfillJobPhase.allCases where phase != .fullEpisodeScan {
            let result = narrow(phase: phase, inputs: inputs, config: config)
            // Empty phases (wasEmpty) contribute nothing.
            // Aborted phases fall back to the full segment list at the
            // runner; for the predicted-coverage union we expand to that
            // same fallback so the recall metric agrees with what the
            // runner actually scanned.
            if result.aborted {
                union.formUnion(orderedSegments(inputs.segments).map(\.segmentIndex))
                continue
            }
            if let narrowed = result.narrowedSegments {
                union.formUnion(narrowed.map(\.segmentIndex))
            }
        }
        return union
    }

    /// Cycle 2 C4: this is **recall**, not precision — the field name was
    /// historically wrong. Formula is unchanged:
    ///
    ///     covered = |predictedTargetedLineRefs ∩ actualAdLineRefs|
    ///     recall  = covered / |actualAdLineRefs|
    ///
    /// In words: of the lines the full rescan flagged as ads
    /// (denominator), what fraction would the targeted phases also have
    /// scanned (numerator). Returns `nil` when `actualAdLineRefs.isEmpty`
    /// — an ad-free episode has no ground truth to recall against, so the
    /// caller must NOT advance the planner ring.
    ///
    /// Persistence keeps the historical `precision*` column / JSON-key
    /// names so a v4 row written before the rename still decodes; see the
    /// matching `// historical: stored as "precision"; semantically recall`
    /// comments at the storage boundaries in `AnalysisStore`.
    static func recallSample(
        predictedTargetedLineRefs: Set<Int>,
        actualAdLineRefs: Set<Int>
    ) -> Double? {
        guard !actualAdLineRefs.isEmpty else { return nil }
        let covered = predictedTargetedLineRefs.intersection(actualAdLineRefs).count
        return Double(covered) / Double(actualAdLineRefs.count)
    }

    private static func orderedSegments(_ segments: [AdTranscriptSegment]) -> [AdTranscriptSegment] {
        segments.sorted { lhs, rhs in
            if lhs.segmentIndex == rhs.segmentIndex {
                return lhs.startTime < rhs.startTime
            }
            return lhs.segmentIndex < rhs.segmentIndex
        }
    }

    private static func evidenceLineRefs(inputs: Inputs) -> Set<Int> {
        // Cycle 2 Rev3-L1: the previous implementation used
        // `uniquingKeysWith: { first, _ in first }` to silently dedupe
        // duplicate atom ordinals. A duplicate atom ordinal across
        // segments is an upstream invariant violation
        // (TranscriptAtomizer assigns globally-unique ordinals); silent
        // dedup hid the bug. Fail loud via `precondition` so the test
        // suite catches a regression at the point of injection rather
        // than three layers downstream.
        var lineRefByAtomOrdinal: [Int: Int] = [:]
        for segment in inputs.segments {
            for atom in segment.atoms {
                let ordinal = atom.atomKey.atomOrdinal
                precondition(
                    lineRefByAtomOrdinal[ordinal] == nil,
                    "TargetedWindowNarrower: duplicate atomOrdinal \(ordinal) violates atomizer invariant"
                )
                lineRefByAtomOrdinal[ordinal] = segment.segmentIndex
            }
        }
        let catalogRefs = Set(
            inputs.evidenceCatalog.entries.compactMap { entry in
                lineRefByAtomOrdinal[entry.atomOrdinal]
            }
        )

        // Expand backward from each catalog anchor to capture the pre-CTA product pitch.
        // Evidence extraction (Phase 3 in EvidenceCatalogBuilder) only fires on atoms with
        // explicit anchors (URLs, promo codes, disclosure phrases), which appear at the END
        // of an ad. Without expansion, FM receives only the final 5–10s CTA slice and misses
        // the preceding 20–30s product pitch entirely.
        //
        // Expansion is per-entry (not per-episode-minimum) so every ad in a multi-ad
        // episode gets its own lookback, not just the first one.
        // 20 atoms ≈ 40s at ~2s/atom — covers 30s ads fully and 60s ads partially.
        // FM's own boundary detection corrects for any non-ad content included here.
        guard !catalogRefs.isEmpty else { return catalogRefs }

        let lookbackAtoms = 20
        var preAnchorRefs = Set<Int>()
        for entry in inputs.evidenceCatalog.entries {
            let lookbackStart = max(0, entry.atomOrdinal - lookbackAtoms)
            for ordinal in lookbackStart..<entry.atomOrdinal {
                if let ref = lineRefByAtomOrdinal[ordinal] {
                    preAnchorRefs.insert(ref)
                }
            }
        }
        return catalogRefs.union(preAnchorRefs)
    }

    private static func lexicalCandidateLineRefs(inputs: Inputs) -> Set<Int> {
        let chunks = orderedSegments(inputs.segments).map { segment in
            TranscriptChunk(
                id: "targeted-\(inputs.analysisAssetId)-\(segment.segmentIndex)",
                analysisAssetId: inputs.analysisAssetId,
                segmentFingerprint: "targeted-\(segment.segmentIndex)",
                chunkIndex: segment.segmentIndex,
                startTime: segment.startTime,
                endTime: segment.endTime,
                text: segment.text,
                normalizedText: TranscriptEngineService.normalizeText(segment.text),
                pass: "final",
                modelVersion: "targeted-window-narrower",
                transcriptVersion: inputs.transcriptVersion,
                atomOrdinal: segment.firstAtomOrdinal
            )
        }

        let scanner = LexicalScanner()
        let candidates = scanner.scan(
            chunks: chunks,
            analysisAssetId: inputs.analysisAssetId
        )
        return Set(
            candidates.flatMap { candidate in
                inputs.segments.compactMap { segment in
                    overlaps(
                        startTime: segment.startTime,
                        endTime: segment.endTime,
                        withStart: candidate.startTime,
                        end: candidate.endTime
                    ) ? segment.segmentIndex : nil
                }
            }
        )
    }

    private static func overlaps(
        startTime: Double,
        endTime: Double,
        withStart otherStart: Double,
        end otherEnd: Double
    ) -> Bool {
        startTime <= otherEnd && endTime >= otherStart
    }

    /// Cycle 2 C5: per-anchor windows + interval merge + cap.
    /// Each anchor produces a `[anchor - padding, anchor + padding]` range,
    /// clipped to the episode bounds, then overlapping/adjacent ranges
    /// merge. If the merged segment count exceeds `maxNarrowedSegmentsPerPhase`
    /// the function returns `.abortedExceededCap` so the caller can fall
    /// back to the full segment list and bump the
    /// `narrowing.aborted.exceededCap` counter for the phase.
    private static func narrowedResult(
        lineRefs: Set<Int>,
        orderedSegments: [AdTranscriptSegment],
        acousticBreaks: [AcousticBreak],
        config: NarrowingConfig,
        phaseLabel: String
    ) -> PhaseNarrowingResult {
        guard !lineRefs.isEmpty else { return .empty }

        let availableLineRefs = orderedSegments.map(\.segmentIndex).sorted()
        guard let lower = availableLineRefs.first,
              let upper = availableLineRefs.last else {
            return .empty
        }

        // Build per-anchor closed intervals clipped to episode bounds.
        // Each anchor that exists in `availableLineRefs` becomes
        // [anchor - padding, anchor + padding] ∩ [lower, upper].
        // Anchors that fall outside the available line refs are skipped
        // — they cannot contribute scannable segments.
        let availableSet = Set(availableLineRefs)
        var rawIntervals: [(Int, Int)] = []
        for anchor in lineRefs.sorted() where availableSet.contains(anchor) {
            let lo = max(lower, anchor - config.perAnchorPaddingSegments)
            let hi = min(upper, anchor + config.perAnchorPaddingSegments)
            if lo <= hi {
                rawIntervals.append((lo, hi))
            }
        }
        if rawIntervals.isEmpty {
            return .empty
        }

        // Interval merge: sort by start, fold adjacent/overlapping into
        // a single range. Two intervals [a,b] and [c,d] merge when
        // c <= b + 1 (touching counts as adjacent).
        rawIntervals.sort { $0.0 < $1.0 }
        var merged: [(Int, Int)] = [rawIntervals[0]]
        for interval in rawIntervals.dropFirst() {
            let last = merged[merged.count - 1]
            if interval.0 <= last.1 + 1 {
                merged[merged.count - 1] = (last.0, max(last.1, interval.1))
            } else {
                merged.append(interval)
            }
        }

        // playhead-7q3 (Phase 4, Option D): after per-anchor merging, snap
        // each merged interval's outer edges to the nearest acoustic break
        // within `acousticBreakSnapMaxDistanceSeconds`. Real ad transitions
        // tend to be marked by RMS drops, music spikes, and pause clusters;
        // aligning the window we hand to the Foundation Model with those
        // natural boundaries should produce more accurate refinement than a
        // fixed-padding chunk. This step is additive-only — if no breaks
        // are provided (the default for callers without feature windows),
        // or no break is within snap distance of an edge, the interval is
        // left exactly as the merge step produced it.
        //
        // The snap never moves an edge by more than `perAnchorPaddingSegments`
        // to bound drift on short-duration segments, and it never causes the
        // merged count to exceed the per-phase cap: the cap check below
        // still fires, and callers still fall back to root inputs if the
        // snap widens the windows past the cap. Merging of intervals that
        // become adjacent after snapping is not re-run on purpose — the
        // original merge step already folded overlapping/adjacent intervals,
        // and the snap moves edges by at most one padding width, so at
        // worst two originally-disjoint intervals may end up with a
        // one-segment gap that was deliberately left as a gap.
        merged = snapIntervalsToAcousticBreaks(
            merged,
            orderedSegments: orderedSegments,
            availableLineRefs: availableLineRefs,
            acousticBreaks: acousticBreaks,
            config: config
        )

        // Convert merged ranges into the actual segment objects we have
        // available in the ordered list. Use a Set lookup for the union
        // of all line refs covered.
        var covered = Set<Int>()
        for (lo, hi) in merged {
            for value in lo...hi where availableSet.contains(value) {
                covered.insert(value)
            }
        }

        if covered.count > config.maxNarrowedSegmentsPerPhase {
            narrowerLogger.debug(
                "narrowing aborted: phase=\(phaseLabel, privacy: .public) merged=\(covered.count) cap=\(config.maxNarrowedSegmentsPerPhase, privacy: .public)"
            )
            return .abortedExceededCap
        }

        let resultSegments = orderedSegments.filter { covered.contains($0.segmentIndex) }
        if resultSegments.isEmpty {
            return .empty
        }
        return .narrowed(resultSegments)
    }

    /// Adjust each merged per-anchor interval's outer edges by routing the
    /// acoustic break evidence through `TimeBoundaryResolver`.
    ///
    /// The algorithm is intentionally conservative:
    ///   1. For each merged interval `[loIdx, hiIdx]`, compute its time
    ///      bounds from the corresponding segments.
    ///   2. For the LEFT edge, look for the acoustic break whose `time`
    ///      is closest to `segment(loIdx).startTime` and within the
    ///      snap-distance window. If a break exists, resolve the segment
    ///      index whose timespan contains that break (or the nearest
    ///      boundary if the break falls in a gap), and move the left edge
    ///      to that index — but only by at most `perAnchorPaddingSegments`.
    ///   3. Repeat for the RIGHT edge.
    ///
    /// The snap is symmetric: an edge may be pulled inward (tightening
    /// the window) or pushed outward (widening it). In either direction
    /// the drift is bounded, and widening is naturally re-bounded by the
    /// per-phase cap check the caller runs immediately after this step.
    ///
    /// Determinism: `acousticBreaks` is consulted with a stable "closest
    /// time" tiebreak (index order from the input). `orderedSegments` is
    /// already sorted by `segmentIndex` at the caller. Running this helper
    /// twice on the same inputs produces the same output.
    private static func snapIntervalsToAcousticBreaks(
        _ merged: [(Int, Int)],
        orderedSegments: [AdTranscriptSegment],
        availableLineRefs: [Int],
        acousticBreaks: [AcousticBreak],
        config: NarrowingConfig
    ) -> [(Int, Int)] {
        guard !acousticBreaks.isEmpty, !orderedSegments.isEmpty else {
            return merged
        }

        // Index the ordered segments by their `segmentIndex` for O(1)
        // lookup when resolving interval endpoints to time ranges.
        var segmentByIndex: [Int: AdTranscriptSegment] = [:]
        segmentByIndex.reserveCapacity(orderedSegments.count)
        for segment in orderedSegments {
            segmentByIndex[segment.segmentIndex] = segment
        }

        guard let lower = availableLineRefs.first,
              let upper = availableLineRefs.last else {
            return merged
        }

        let maxDrift = config.perAnchorPaddingSegments
        let snapDistance = config.acousticBreakSnapMaxDistanceSeconds
        let resolver = TimeBoundaryResolver()
        let resolverConfig = boundaryResolverConfig(snapDistance: snapDistance)
        let featureWindows = syntheticFeatureWindows(from: acousticBreaks)

        return merged.map { interval -> (Int, Int) in
            var (lo, hi) = interval
            let originalLo = lo
            let originalHi = hi
            guard let loSegment = segmentByIndex[lo],
                  let hiSegment = segmentByIndex[hi] else {
                return interval
            }

            // LEFT edge snap: find the nearest break to the interval's
            // start time. If one is within the snap distance, resolve it
            // to a segment index and update `lo`.
            if let snap = snapEdge(
                targetTime: loSegment.startTime,
                boundaryType: .start,
                resolver: resolver,
                featureWindows: featureWindows,
                resolverConfig: resolverConfig,
                orderedSegments: orderedSegments,
                availableLineRefs: availableLineRefs
            ) {
                let clamped = min(max(snap.segmentIndex, lo - maxDrift), lo + maxDrift)
                let candidate = max(lower, clamped)
                if candidate != lo {
                    narrowerLogger.debug(
                        "snap: left edge moved from segment \(originalLo, privacy: .public) to segment \(candidate, privacy: .public) (delta \(candidate - originalLo, privacy: .public) segments, \(snap.breakDistanceSeconds, privacy: .public)s break distance)"
                    )
                    lo = candidate
                }
            }

            // RIGHT edge snap: same logic for the interval's end time.
            if let snap = snapEdge(
                targetTime: hiSegment.endTime,
                boundaryType: .end,
                resolver: resolver,
                featureWindows: featureWindows,
                resolverConfig: resolverConfig,
                orderedSegments: orderedSegments,
                availableLineRefs: availableLineRefs
            ) {
                let clamped = min(max(snap.segmentIndex, hi - maxDrift), hi + maxDrift)
                let candidate = min(upper, clamped)
                if candidate != hi {
                    narrowerLogger.debug(
                        "snap: right edge moved from segment \(originalHi, privacy: .public) to segment \(candidate, privacy: .public) (delta \(candidate - originalHi, privacy: .public) segments, \(snap.breakDistanceSeconds, privacy: .public)s break distance)"
                    )
                    hi = candidate
                }
            }

            if lo > hi {
                // Snapping should never invert the interval; bail back to
                // the original merge output if it does.
                return interval
            }
            return (lo, hi)
        }
    }

    /// Result of a single-edge snap lookup. `segmentIndex` is the resolved
    /// segment the edge should move to; `breakDistanceSeconds` is the
    /// absolute time distance from the edge to the chosen break (carried
    /// through so the caller can log observability without re-scanning).
    private struct SnapResult {
        let segmentIndex: Int
        let breakDistanceSeconds: Double
    }

    /// Resolve `targetTime` to a segment index by (a) letting
    /// `TimeBoundaryResolver` choose a snapped boundary time from the synthetic
    /// break windows, then (b) mapping that snapped time to the segment whose
    /// timespan contains it. Returns `nil` if no resolver-qualified boundary is
    /// in range or if the snapped time cannot be placed inside any available
    /// segment.
    private static func snapEdge(
        targetTime: Double,
        boundaryType: BoundaryType,
        resolver: TimeBoundaryResolver,
        featureWindows: [FeatureWindow],
        resolverConfig: BoundarySnappingConfig,
        orderedSegments: [AdTranscriptSegment],
        availableLineRefs: [Int]
    ) -> SnapResult? {
        let snappedTime = resolver.snap(
            candidateTime: targetTime,
            boundaryType: boundaryType,
            anchorType: .fmPositive,
            featureWindows: featureWindows,
            lexicalHits: [],
            config: resolverConfig
        )
        guard abs(snappedTime - targetTime) > 0.000_001 else {
            return nil
        }
        let breakDistance = abs(snappedTime - targetTime)

        // Find the segment whose [startTime, endTime] contains the snapped
        // boundary time. If it falls in a gap between segments, pick the segment
        // whose boundary is closest to breakTime.
        var containingIndex: Int?
        var closestIndex: Int?
        var closestDistance = Double.infinity
        for segment in orderedSegments {
            if snappedTime >= segment.startTime && snappedTime <= segment.endTime {
                containingIndex = segment.segmentIndex
                break
            }
            let distanceToStart = abs(segment.startTime - snappedTime)
            let distanceToEnd = abs(segment.endTime - snappedTime)
            let distance = min(distanceToStart, distanceToEnd)
            if distance < closestDistance {
                closestDistance = distance
                closestIndex = segment.segmentIndex
            }
        }
        let resolved = containingIndex ?? closestIndex
        guard let resolved else { return nil }
        // Clip to the available line-ref bounds so callers never see an
        // out-of-range segment index.
        guard let lower = availableLineRefs.first,
              let upper = availableLineRefs.last else {
            return nil
        }
        let clipped = min(max(resolved, lower), upper)
        return SnapResult(segmentIndex: clipped, breakDistanceSeconds: breakDistance)
    }

    private static func syntheticFeatureWindows(
        from acousticBreaks: [AcousticBreak]
    ) -> [FeatureWindow] {
        acousticBreaks.sorted { $0.time < $1.time }.map { acousticBreak in
            let proxyStrength = acousticBreak.signals.contains(.energyDrop) || acousticBreak.signals.contains(.energyRise)
                ? acousticBreak.breakStrength
                : 0.0
            let pauseStrength = acousticBreak.signals.contains(.pauseCluster)
                ? acousticBreak.breakStrength
                : 0.0
            let spectralStrength = acousticBreak.signals.contains(.spectralSpike)
                ? max(acousticBreak.breakStrength, 0.001)
                : 0.0

            return FeatureWindow(
                analysisAssetId: "targeted-window-narrower-breaks",
                startTime: acousticBreak.time,
                endTime: acousticBreak.time,
                rms: 0.0,
                spectralFlux: spectralStrength,
                musicProbability: 0.0,
                speakerChangeProxyScore: proxyStrength,
                musicBedChangeScore: 0.0,
                pauseProbability: pauseStrength,
                speakerClusterId: nil,
                jingleHash: nil,
                featureVersion: 1
            )
        }
    }

    private static func boundaryResolverConfig(
        snapDistance: Double
    ) -> BoundarySnappingConfig {
        BoundarySnappingConfig(
            startWeights: StartBoundaryCueWeights(
                pauseVAD: 0.35,
                speakerChangeProxy: 0.45,
                musicBedChange: 0.0,
                spectralChange: 0.20,
                lexicalDensityDelta: 0.0
            ),
            endWeights: EndBoundaryCueWeights(
                pauseVAD: 0.35,
                speakerChangeProxy: 0.45,
                musicBedChange: 0.0,
                spectralChange: 0.20,
                explicitReturnMarker: 0.0
            ),
            maxSnapDistanceByAnchorType: [.fmPositive: BoundarySnapDistance(start: snapDistance, end: snapDistance)],
            lambda: 0.1,
            // AcousticBreakDetector's single-signal breaks top out at 0.3-0.4
            // before distance penalty, so the resolver floor must stay low
            // enough for realistic nearby edges to qualify.
            minBoundaryScore: 0.1,
            minImprovementOverOriginal: 0.05
        )
    }

    private static func auditSegments(
        orderedSegments: [AdTranscriptSegment],
        inputs: Inputs
    ) -> [AdTranscriptSegment] {
        guard orderedSegments.count > 1 else { return orderedSegments }

        let requestedCount = Int(round(Double(orderedSegments.count) * inputs.auditWindowSampleRate))
        let targetCount = max(1, min(orderedSegments.count - 1, requestedCount))
        let maxStart = orderedSegments.count - targetCount
        let startIndex = maxStart == 0 ? 0 : Int(auditSeed(inputs: inputs) % UInt64(maxStart + 1))
        return Array(orderedSegments[startIndex..<(startIndex + targetCount)])
    }

    /// Cycle 2 Rev3-M3: include `episodesSinceLastFullRescan` in the seed
    /// material so consecutive observations of the same
    /// `(podcastId, analysisAssetId, transcriptVersion)` rotate across
    /// distinct audit windows instead of producing the same audit segment
    /// forever.
    private static func auditSeed(inputs: Inputs) -> UInt64 {
        let material = "\(inputs.podcastId)|\(inputs.analysisAssetId)|\(inputs.transcriptVersion)|audit|\(inputs.episodesSinceLastFullRescan)"
        return deterministicSeed(material)
    }

    private static func deterministicSeed(_ material: String) -> UInt64 {
        let digest = SHA256.hash(data: Data(material.utf8))
        return digest.prefix(8).reduce(into: UInt64(0)) { partial, byte in
            partial = (partial << 8) | UInt64(byte)
        }
    }
}
