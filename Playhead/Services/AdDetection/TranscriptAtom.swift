import Foundation
import CryptoKit

// MARK: - Transcript Identity

/// Stable identity for a single transcript unit. All FM outputs, user corrections,
/// and training examples are keyed by (analysisAssetId, transcriptVersion, atomOrdinal).
struct TranscriptAtomKey: Sendable, Codable, Hashable {
    let analysisAssetId: String
    let transcriptVersion: String
    let atomOrdinal: Int
}

struct TranscriptAtom: Sendable {
    let atomKey: TranscriptAtomKey
    let contentHash: String          // for matching/debugging, not primary identity
    let startTime: Double
    let endTime: Double
    let text: String
    let chunkIndex: Int              // diagnostic convenience
    let speakerId: Int?              // B7: validated speaker label, nil when unavailable
    let reliability: TranscriptReliability  // ef2.1.3: per-atom reliability signals

    init(
        atomKey: TranscriptAtomKey,
        contentHash: String,
        startTime: Double,
        endTime: Double,
        text: String,
        chunkIndex: Int,
        speakerId: Int? = nil,
        reliability: TranscriptReliability = .default
    ) {
        self.atomKey = atomKey
        self.contentHash = contentHash
        self.startTime = startTime
        self.endTime = endTime
        self.text = text
        self.chunkIndex = chunkIndex
        self.speakerId = speakerId
        self.reliability = reliability
    }
}

struct TranscriptVersion: Sendable, Codable {
    let transcriptVersion: String    // hash of atom sequence
    let normalizationHash: String    // transcript normalization pipeline
    let sourceHash: String           // ASR model / source identity
}

/// When transcripts are reprocessed, preserves correction and training lineage.
struct TranscriptAlignmentMap: Sendable, Codable {
    let fromTranscriptVersion: String
    let toTranscriptVersion: String
    let mappings: [Int: Int]         // old atomOrdinal -> new atomOrdinal
}

/// Stable anchor for materialized cues that survives transcript version changes.
struct CueAnchor: Sendable, Codable {
    let analysisAssetId: String
    let transcriptVersion: String
    let firstAtomOrdinal: Int
    let lastAtomOrdinal: Int
    let approxStartTime: Double
    let approxEndTime: Double
    let boundaryFingerprint: String
}

// MARK: - Atomizer

enum TranscriptAtomizer {
    /// Convert TranscriptChunks into TranscriptAtoms with stable ordinal identity.
    /// Each chunk becomes one atom. The transcriptVersion is computed from the
    /// ordered atom content.
    static func atomize(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        normalizationHash: String,
        sourceHash: String
    ) -> (atoms: [TranscriptAtom], version: TranscriptVersion) {
        // Sort by chunkIndex for deterministic ordering
        let sorted = chunks.sorted { $0.chunkIndex < $1.chunkIndex }

        // Compute transcript version hash from ordered content.
        // Length-prefix each chunk to prevent boundary ambiguity:
        // ["ab","cd"] and ["a","bcd"] must produce different hashes.
        var hasher = SHA256()
        for chunk in sorted {
            let textData = Data(chunk.normalizedText.utf8)
            withUnsafeBytes(of: UInt32(textData.count).bigEndian) { hasher.update(bufferPointer: $0) }
            hasher.update(data: textData)
        }
        let versionHash = hasher.finalize().prefix(16).map { String(format: "%02x", $0) }.joined()

        let version = TranscriptVersion(
            transcriptVersion: versionHash,
            normalizationHash: normalizationHash,
            sourceHash: sourceHash
        )

        let atoms = sorted.enumerated().map { ordinal, chunk in
            // Per-atom content hash for matching/debugging
            let atomHash = SHA256.hash(data: Data(chunk.normalizedText.utf8))
                .prefix(8).map { String(format: "%02x", $0) }.joined()

            // ef2.1.3: Propagate chunk-level quality to atom reliability.
            // Build a lightweight AdTranscriptSegment to reuse the existing
            // quality estimator. NormalizationQuality stays .unknown until
            // EvidenceCatalogBuilder runs downstream.
            let singleAtom = TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: analysisAssetId,
                    transcriptVersion: versionHash,
                    atomOrdinal: ordinal
                ),
                contentHash: atomHash,
                startTime: chunk.startTime,
                endTime: chunk.endTime,
                text: chunk.text,
                chunkIndex: chunk.chunkIndex,
                speakerId: chunk.speakerId
            )
            let segment = AdTranscriptSegment(atoms: [singleAtom], segmentIndex: ordinal)
            let assessment = TranscriptQualityEstimator.assess(segment: segment)
            let reliability = TranscriptReliability(
                chunkQuality: assessment.quality,
                chunkQualityScore: assessment.qualityScore,
                normalizationQuality: .unknown,
                alternativeCount: 0
            )

            return TranscriptAtom(
                atomKey: singleAtom.atomKey,
                contentHash: atomHash,
                startTime: chunk.startTime,
                endTime: chunk.endTime,
                text: chunk.text,
                chunkIndex: chunk.chunkIndex,
                speakerId: chunk.speakerId,
                reliability: reliability
            )
        }

        return (atoms, version)
    }
}
