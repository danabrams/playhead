import CryptoKit
import Foundation

enum TargetedWindowNarrower {
    struct Inputs: Sendable {
        let analysisAssetId: String
        let podcastId: String
        let transcriptVersion: String
        let segments: [AdTranscriptSegment]
        let evidenceCatalog: EvidenceCatalog
        let auditWindowSampleRate: Double
    }

    private static let phasePaddingSegments = 1

    static func narrow(
        phase: BackfillJobPhase,
        inputs: Inputs
    ) -> [AdTranscriptSegment] {
        let ordered = orderedSegments(inputs.segments)
        guard !ordered.isEmpty else { return [] }

        switch phase {
        case .fullEpisodeScan:
            return ordered
        case .scanHarvesterProposals:
            let narrowed = narrowedSegments(
                lineRefs: evidenceLineRefs(inputs: inputs),
                orderedSegments: ordered,
                padding: phasePaddingSegments
            )
            return fallbackIfEmpty(
                narrowed,
                orderedSegments: ordered,
                seedMaterial: "\(inputs.analysisAssetId)|\(inputs.transcriptVersion)|harvester"
            )
        case .scanLikelyAdSlots:
            let narrowed = narrowedSegments(
                lineRefs: lexicalCandidateLineRefs(inputs: inputs),
                orderedSegments: ordered,
                padding: phasePaddingSegments
            )
            return fallbackIfEmpty(
                narrowed,
                orderedSegments: ordered,
                seedMaterial: "\(inputs.analysisAssetId)|\(inputs.transcriptVersion)|likely-ad"
            )
        case .scanRandomAuditWindows:
            return auditSegments(orderedSegments: ordered, inputs: inputs)
        }
    }

    static func predictedTargetedLineRefs(inputs: Inputs) -> Set<Int> {
        Set(
            BackfillJobPhase.allCases
                .filter { $0 != .fullEpisodeScan }
                .flatMap { phase in
                    narrow(phase: phase, inputs: inputs).map(\.segmentIndex)
                }
        )
    }

    static func precisionSample(
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
        let lineRefByAtomOrdinal = Dictionary(
            inputs.segments.flatMap { segment in
                segment.atoms.map { ($0.atomKey.atomOrdinal, segment.segmentIndex) }
            },
            uniquingKeysWith: { first, _ in first }
        )
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

    private static func narrowedSegments(
        lineRefs: Set<Int>,
        orderedSegments: [AdTranscriptSegment],
        padding: Int
    ) -> [AdTranscriptSegment] {
        guard !lineRefs.isEmpty else { return [] }
        let availableLineRefs = Set(orderedSegments.map(\.segmentIndex))
        let expanded = Set(
            lineRefs.flatMap { lineRef in
                ((lineRef - padding)...(lineRef + padding)).filter { availableLineRefs.contains($0) }
            }
        )
        guard let firstIndex = orderedSegments.firstIndex(where: { expanded.contains($0.segmentIndex) }),
              let lastIndex = orderedSegments.lastIndex(where: { expanded.contains($0.segmentIndex) }) else {
            return []
        }
        return Array(orderedSegments[firstIndex...lastIndex])
    }

    private static func fallbackIfEmpty(
        _ narrowed: [AdTranscriptSegment],
        orderedSegments: [AdTranscriptSegment],
        seedMaterial: String
    ) -> [AdTranscriptSegment] {
        if !narrowed.isEmpty {
            return narrowed
        }
        guard !orderedSegments.isEmpty else { return [] }
        if orderedSegments.count == 1 {
            return orderedSegments
        }
        let seed = deterministicSeed(seedMaterial)
        let index = Int(seed % UInt64(orderedSegments.count))
        return [orderedSegments[index]]
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

    private static func auditSeed(inputs: Inputs) -> UInt64 {
        let material = "\(inputs.podcastId)|\(inputs.analysisAssetId)|\(inputs.transcriptVersion)|audit"
        return deterministicSeed(material)
    }

    private static func deterministicSeed(_ material: String) -> UInt64 {
        let digest = SHA256.hash(data: Data(material.utf8))
        return digest.prefix(8).reduce(into: UInt64(0)) { partial, byte in
            partial = (partial << 8) | UInt64(byte)
        }
    }
}
