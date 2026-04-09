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

    static let `default` = NarrowingConfig(
        perAnchorPaddingSegments: 5,
        maxNarrowedSegmentsPerPhase: 60
    )
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

        init(
            analysisAssetId: String,
            podcastId: String,
            transcriptVersion: String,
            segments: [AdTranscriptSegment],
            evidenceCatalog: EvidenceCatalog,
            auditWindowSampleRate: Double,
            episodesSinceLastFullRescan: Int = 0
        ) {
            self.analysisAssetId = analysisAssetId
            self.podcastId = podcastId
            self.transcriptVersion = transcriptVersion
            self.segments = segments
            self.evidenceCatalog = evidenceCatalog
            self.auditWindowSampleRate = auditWindowSampleRate
            self.episodesSinceLastFullRescan = episodesSinceLastFullRescan
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
                config: config,
                phaseLabel: "scanHarvesterProposals"
            )
        case .scanLikelyAdSlots:
            return narrowedResult(
                lineRefs: lexicalCandidateLineRefs(inputs: inputs),
                orderedSegments: ordered,
                config: config,
                phaseLabel: "scanLikelyAdSlots"
            )
        case .scanRandomAuditWindows:
            return .narrowed(auditSegments(orderedSegments: ordered, inputs: inputs))
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
        return Set(
            inputs.evidenceCatalog.entries.compactMap { entry in
                lineRefByAtomOrdinal[entry.atomOrdinal]
            }
        )
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
