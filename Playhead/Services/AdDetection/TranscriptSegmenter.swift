import Foundation

// MARK: - AdTranscriptSegment

/// A coherent region of transcript atoms grouped by natural boundaries.
/// Named `AdTranscriptSegment` to avoid collision with the ASR-layer `TranscriptSegment`.
struct AdTranscriptSegment: Sendable {
    /// The atoms in this segment, ordered by ordinal.
    let atoms: [TranscriptAtom]
    /// Index of this segment within the episode.
    let segmentIndex: Int
    /// Why this segment started where it did.
    let boundaryReason: AdTranscriptBoundaryReason
    /// Confidence in the boundary that started this segment, 0.0...1.0.
    let boundaryConfidence: Double
    /// Semantic type of the segment. Phase 1 segments are speech-only.
    let segmentType: AdTranscriptSegmentType

    init(
        atoms: [TranscriptAtom],
        segmentIndex: Int,
        boundaryReason: AdTranscriptBoundaryReason = .startOfTranscript,
        boundaryConfidence: Double = 1.0,
        segmentType: AdTranscriptSegmentType = .speech
    ) {
        self.atoms = atoms
        self.segmentIndex = segmentIndex
        self.boundaryReason = boundaryReason
        self.boundaryConfidence = boundaryConfidence
        self.segmentType = segmentType
    }

    var startTime: Double { atoms.first?.startTime ?? 0 }
    var endTime: Double { atoms.last?.endTime ?? 0 }
    var duration: Double { endTime - startTime }
    var firstAtomOrdinal: Int { atoms.first?.atomKey.atomOrdinal ?? 0 }
    var lastAtomOrdinal: Int { atoms.last?.atomKey.atomOrdinal ?? 0 }
    var text: String { atoms.map(\.text).joined(separator: " ") }
}

enum AdTranscriptBoundaryReason: String, Sendable, Codable {
    case startOfTranscript
    case maxDuration
    case pause
    case speakerTurn
    case discourseMarker
    case sentenceBoundary
}

enum AdTranscriptSegmentType: String, Sendable, Codable {
    case speech
}

// MARK: - AdTranscriptSegmenter

enum TranscriptSegmenter {

    struct Config: Sendable {
        /// Minimum pause (seconds) between atoms to force a segment break.
        let pauseThreshold: Double
        /// Maximum segment duration (seconds) before forcing a break.
        let maxSegmentDuration: Double
        /// Minimum segment duration (seconds) — avoid micro-segments.
        let minSegmentDuration: Double

        static let `default` = Config(
            pauseThreshold: 1.5,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
    }

    /// Segment atoms into coherent regions.
    static func segment(
        atoms: [TranscriptAtom],
        featureWindows: [FeatureWindow] = [],
        config: Config = .default
    ) -> [AdTranscriptSegment] {
        guard !atoms.isEmpty else { return [] }

        let sorted = atoms.sorted { $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal }
        let windows = featureWindows.sorted { $0.startTime < $1.startTime }

        var segments: [AdTranscriptSegment] = []
        var currentAtoms: [TranscriptAtom] = [sorted[0]]
        var segmentIndex = 0
        var currentBoundary = BoundaryMetadata.startOfTranscript

        for i in 1..<sorted.count {
            let prev = sorted[i - 1]
            let curr = sorted[i]

            let boundary = Self.breakType(
                previous: prev,
                current: curr,
                currentSegmentStart: currentAtoms.first!.startTime,
                featureWindows: windows,
                config: config
            )

            if boundary.strength == .hard {
                // Hard breaks (max duration, significant pause) always emit
                segments.append(AdTranscriptSegment(
                    atoms: currentAtoms,
                    segmentIndex: segmentIndex,
                    boundaryReason: currentBoundary.reason,
                    boundaryConfidence: currentBoundary.confidence
                ))
                segmentIndex += 1
                currentAtoms = [curr]
                currentBoundary = boundary.metadata
                continue
            }

            if boundary.strength == .soft {
                // Soft breaks are suppressed if segment is below min duration
                let segDuration = prev.endTime - currentAtoms.first!.startTime
                if segDuration >= config.minSegmentDuration {
                    segments.append(AdTranscriptSegment(
                        atoms: currentAtoms,
                        segmentIndex: segmentIndex,
                        boundaryReason: currentBoundary.reason,
                        boundaryConfidence: currentBoundary.confidence
                    ))
                    segmentIndex += 1
                    currentAtoms = [curr]
                    currentBoundary = boundary.metadata
                    continue
                }
            }

            currentAtoms.append(curr)
        }

        // Emit final segment
        if !currentAtoms.isEmpty {
            segments.append(AdTranscriptSegment(
                atoms: currentAtoms,
                segmentIndex: segmentIndex,
                boundaryReason: currentBoundary.reason,
                boundaryConfidence: currentBoundary.confidence
            ))
        }

        return segments
    }

    // MARK: - Private

    private enum BreakStrength {
        case none, soft, hard
    }

    private static let featurePauseProbabilityThreshold = 0.7

    private struct BoundaryMetadata {
        let reason: AdTranscriptBoundaryReason
        let confidence: Double

        static let startOfTranscript = BoundaryMetadata(
            reason: .startOfTranscript,
            confidence: 1.0
        )
    }

    private struct BreakType {
        let strength: BreakStrength
        let metadata: BoundaryMetadata

        static let none = BreakType(strength: .none, metadata: .startOfTranscript)
    }

    private static func breakType(
        previous: TranscriptAtom,
        current: TranscriptAtom,
        currentSegmentStart: Double,
        featureWindows: [FeatureWindow],
        config: Config
    ) -> BreakType {
        let gap = current.startTime - previous.endTime

        // Hard break: max duration exceeded
        if current.startTime - currentSegmentStart >= config.maxSegmentDuration {
            return BreakType(
                strength: .hard,
                metadata: BoundaryMetadata(reason: .maxDuration, confidence: 1.0)
            )
        }

        // Hard break: significant pause
        if gap >= config.pauseThreshold {
            return BreakType(
                strength: .hard,
                metadata: BoundaryMetadata(reason: .pause, confidence: 1.0)
            )
        }

        // Hard break: feature windows indicate a strong pause boundary even if
        // atom timestamps are contiguous. Check the entire gap interval
        // [previous.endTime, current.startTime] for overlap with a high-pause
        // window — sampling only the boundary point misses windows that end
        // exactly at the previous atom's endTime.
        if let featurePauseConfidence = featurePauseConfidence(
            gapStart: previous.endTime,
            gapEnd: current.startTime,
            featureWindows: featureWindows
        ) {
            return BreakType(
                strength: .hard,
                metadata: BoundaryMetadata(
                    reason: .pause,
                    confidence: featurePauseConfidence
                )
            )
        }

        // Soft break: stable speaker change across the boundary.
        if let previousSpeaker = dominantSpeaker(overlapping: previous, featureWindows: featureWindows),
           let currentSpeaker = dominantSpeaker(overlapping: current, featureWindows: featureWindows),
           previousSpeaker != currentSpeaker {
            return BreakType(
                strength: .soft,
                metadata: BoundaryMetadata(reason: .speakerTurn, confidence: 0.85)
            )
        }

        // Soft break: discourse marker at start of current atom after a minor pause
        if gap >= 0.5, startsWithDiscourseMarker(current.text) {
            return BreakType(
                strength: .soft,
                metadata: BoundaryMetadata(reason: .discourseMarker, confidence: 0.65)
            )
        }

        // Soft break: previous atom ends with sentence punctuation + minor pause
        if gap >= 0.3,
           endsWithSentencePunctuation(previous.text),
           startsWithCapitalizedContinuation(current.text) {
            return BreakType(
                strength: .soft,
                metadata: BoundaryMetadata(reason: .sentenceBoundary, confidence: 0.55)
            )
        }

        return .none
    }

    private static let discourseMarkers: Set<String> = [
        "anyway", "so", "now", "alright", "okay", "ok",
        "moving on", "back to", "let's get back", "speaking of",
        "but first", "before we", "after the break",
        "and now", "real quick", "one more thing",
    ]

    private static func startsWithDiscourseMarker(_ text: String) -> Bool {
        let lower = text.lowercased().trimmingCharacters(in: .whitespaces)
        return discourseMarkers.contains(where: { marker in
            guard lower.hasPrefix(marker) else { return false }
            // Require word boundary after marker to avoid "so" matching "somebody"
            let afterMarker = lower.index(lower.startIndex, offsetBy: marker.count)
            if afterMarker >= lower.endIndex { return true }
            let nextChar = lower[afterMarker]
            return nextChar.isWhitespace || nextChar.isPunctuation
        })
    }

    private static func endsWithSentencePunctuation(_ text: String) -> Bool {
        let trimmed = text.trimmingCharacters(in: .whitespaces)
        guard let last = trimmed.last else { return false }
        return last == "." || last == "!" || last == "?"
    }

    private static func startsWithCapitalizedContinuation(_ text: String) -> Bool {
        for scalar in text.unicodeScalars {
            if CharacterSet.letters.contains(scalar) {
                return CharacterSet.uppercaseLetters.contains(scalar)
            }
        }
        return false
    }

    private static func dominantSpeaker(
        overlapping atom: TranscriptAtom,
        featureWindows: [FeatureWindow]
    ) -> Int? {
        guard !featureWindows.isEmpty else { return nil }

        var durationsBySpeaker: [Int: Double] = [:]
        for window in featureWindows {
            guard let speakerClusterId = window.speakerClusterId else { continue }
            let overlapStart = max(atom.startTime, window.startTime)
            let overlapEnd = min(atom.endTime, window.endTime)
            let overlap = overlapEnd - overlapStart
            guard overlap > 0 else { continue }
            durationsBySpeaker[speakerClusterId, default: 0] += overlap
        }

        return durationsBySpeaker.max { lhs, rhs in
            if lhs.value == rhs.value {
                return lhs.key > rhs.key
            }
            return lhs.value < rhs.value
        }?.key
    }

    /// Returns the strongest pauseProbability of any feature window whose
    /// time range overlaps the gap interval `[gapStart, gapEnd]` (inclusive
    /// on both endpoints), provided that probability meets the configured
    /// threshold. Returns nil otherwise.
    ///
    /// Two intervals `[a, b]` and `[c, d]` overlap iff `a <= d && c <= b`.
    private static func featurePauseConfidence(
        gapStart: Double,
        gapEnd: Double,
        featureWindows: [FeatureWindow]
    ) -> Double? {
        let lo = min(gapStart, gapEnd)
        let hi = max(gapStart, gapEnd)
        let strongestPause = featureWindows
            .filter { window in
                window.startTime <= hi && lo <= window.endTime
            }
            .map(\.pauseProbability)
            .max()

        guard let strongestPause, strongestPause >= featurePauseProbabilityThreshold else {
            return nil
        }

        return min(1.0, max(featurePauseProbabilityThreshold, strongestPause))
    }
}
