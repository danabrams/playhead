import Foundation

// MARK: - AdTranscriptSegment

/// A coherent region of transcript atoms grouped by natural boundaries.
/// Named `AdTranscriptSegment` to avoid collision with the ASR-layer `TranscriptSegment`.
struct AdTranscriptSegment: Sendable {
    /// The atoms in this segment, ordered by ordinal.
    let atoms: [TranscriptAtom]
    /// Index of this segment within the episode.
    let segmentIndex: Int

    var startTime: Double { atoms.first?.startTime ?? 0 }
    var endTime: Double { atoms.last?.endTime ?? 0 }
    var duration: Double { endTime - startTime }
    var firstAtomOrdinal: Int { atoms.first?.atomKey.atomOrdinal ?? 0 }
    var lastAtomOrdinal: Int { atoms.last?.atomKey.atomOrdinal ?? 0 }
    var text: String { atoms.map(\.text).joined(separator: " ") }
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
            pauseThreshold: 2.0,
            maxSegmentDuration: 120.0,
            minSegmentDuration: 10.0
        )
    }

    /// Segment atoms into coherent regions.
    static func segment(
        atoms: [TranscriptAtom],
        config: Config = .default
    ) -> [AdTranscriptSegment] {
        guard !atoms.isEmpty else { return [] }

        let sorted = atoms.sorted { $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal }

        var segments: [AdTranscriptSegment] = []
        var currentAtoms: [TranscriptAtom] = [sorted[0]]
        var segmentIndex = 0

        for i in 1..<sorted.count {
            let prev = sorted[i - 1]
            let curr = sorted[i]

            let breakType = Self.breakType(
                previous: prev,
                current: curr,
                currentSegmentStart: currentAtoms.first!.startTime,
                config: config
            )

            if breakType == .hard {
                // Hard breaks (max duration, significant pause) always emit
                segments.append(AdTranscriptSegment(atoms: currentAtoms, segmentIndex: segmentIndex))
                segmentIndex += 1
                currentAtoms = [curr]
                continue
            }

            if breakType == .soft {
                // Soft breaks are suppressed if segment is below min duration
                let segDuration = prev.endTime - currentAtoms.first!.startTime
                if segDuration >= config.minSegmentDuration {
                    segments.append(AdTranscriptSegment(atoms: currentAtoms, segmentIndex: segmentIndex))
                    segmentIndex += 1
                    currentAtoms = [curr]
                    continue
                }
            }

            currentAtoms.append(curr)
        }

        // Emit final segment
        if !currentAtoms.isEmpty {
            segments.append(AdTranscriptSegment(atoms: currentAtoms, segmentIndex: segmentIndex))
        }

        return segments
    }

    // MARK: - Private

    private enum BreakType {
        case none, soft, hard
    }

    private static func breakType(
        previous: TranscriptAtom,
        current: TranscriptAtom,
        currentSegmentStart: Double,
        config: Config
    ) -> BreakType {
        let gap = current.startTime - previous.endTime

        // Hard break: max duration exceeded
        if current.startTime - currentSegmentStart >= config.maxSegmentDuration {
            return .hard
        }

        // Hard break: significant pause
        if gap >= config.pauseThreshold {
            return .hard
        }

        // Soft break: discourse marker at start of current atom after a minor pause
        if gap >= 0.5, startsWithDiscourseMarker(current.text) {
            return .soft
        }

        // Soft break: previous atom ends with sentence punctuation + minor pause
        if gap >= 0.3, endsWithSentencePunctuation(previous.text) {
            return .soft
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
}
