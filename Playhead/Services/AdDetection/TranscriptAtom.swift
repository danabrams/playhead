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
    ///
    /// Atoms come out in TIME order (playhead-r5um). They used to come out in
    /// `chunkIndex` order, which is not time order and never was:
    /// `TranscriptEngineService` numbers chunks from an in-memory counter in
    /// shard EMISSION order, and `prioritizeShards` emits by playhead
    /// proximity — `shard0 + hotPath + coldAhead + behindWithoutShard0`, the
    /// last group DESCENDING. On the 2026-07-30 device pull that made 27 of
    /// 30 assets non-monotone in time when read in chunkIndex order, worst
    /// backward step −3538.9 s: asset 8FECFDDE has a chunk at t=3540.0 s
    /// numbered 0, so the final 30 seconds of a 59-minute episode sorted to
    /// ordinal 2. Everything built on the atom sequence — the FM prompt text,
    /// `TranscriptSegmenter`'s pause/max-duration breaks, and the scan-window
    /// bounds `playhead-csbq` had to repair — read a scrambled transcript.
    ///
    /// `TranscriptChunkCanonicalizer` re-sorted only the MIXED fast/final
    /// path, so single-pass assets (7 affected on the pull) were unprotected,
    /// and five of the six production atomize call sites do not canonicalize
    /// at all. Ordering HERE is what closes both holes with one mechanism.
    static func atomize(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        normalizationHash: String,
        sourceHash: String
    ) -> (atoms: [TranscriptAtom], version: TranscriptVersion) {
        // One ordering authority, shared with the canonicalizer and the
        // transcript-peek display path. It is a total order, so this is
        // idempotent over already-canonicalized input and deterministic even
        // where `chunkIndex` collides (it does, on 21 of 30 device assets).
        let sorted = chunks.sorted(by: TranscriptChunkCanonicalizer.canonicalTimeOrder)

        let versionHash = versionHash(ofSorted: sorted)

        let version = TranscriptVersion(
            transcriptVersion: versionHash,
            normalizationHash: normalizationHash,
            sourceHash: sourceHash
        )

        let atoms = sorted.enumerated().map { ordinal, chunk in
            // Per-atom content hash for matching/debugging
            let atomHash = SHA256.hash(data: Data(chunk.normalizedText.utf8))
                .prefix(8).map { String(format: "%02x", $0) }.joined()

            let atomKey = TranscriptAtomKey(
                analysisAssetId: analysisAssetId,
                transcriptVersion: versionHash,
                atomOrdinal: ordinal
            )

            // ef2.1.3: Propagate chunk-level quality to atom reliability.
            // Build a temporary atom (with default reliability) for the quality
            // estimator, then construct the final atom with the assessed reliability.
            // NormalizationQuality stays .unknown until EvidenceCatalogBuilder runs.
            let tempAtom = TranscriptAtom(
                atomKey: atomKey,
                contentHash: atomHash,
                startTime: chunk.startTime,
                endTime: chunk.endTime,
                text: chunk.text,
                chunkIndex: chunk.chunkIndex,
                speakerId: chunk.speakerId
            )
            let assessment = TranscriptQualityEstimator.assess(
                segment: AdTranscriptSegment(atoms: [tempAtom], segmentIndex: ordinal)
            )
            let reliability = TranscriptReliability(
                chunkQuality: assessment.quality,
                chunkQualityScore: assessment.qualityScore,
                normalizationQuality: .unknown,
                alternativeCount: 0
            )

            return TranscriptAtom(
                atomKey: atomKey,
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

    /// playhead-fil5: the `transcriptVersion` for a chunk set, WITHOUT paying
    /// for atoms.
    ///
    /// `transcriptVersion` used to be half of the coverage-lane job identity;
    /// playhead-wxsv took it out of
    /// ``BackfillJobRunner/makeJobId(analysisAssetId:phase:offset:)`` and put it
    /// on the row as `backfill_jobs.attemptTranscriptVersion`, so naming a job
    /// no longer requires this hash. It is still what the runner STAMPS on an
    /// attempt, and it is still the invalidation key for episode summaries and
    /// evidence events, so every caller that needs "the version of this
    /// transcript" comes through here. Before this existed the only way to get
    /// it was
    /// ``atomize(chunks:analysisAssetId:normalizationHash:sourceHash:)``, which
    /// also runs a SHA-256 and a `TranscriptQualityEstimator` pass PER CHUNK to
    /// build atoms the caller then throws away.
    ///
    /// Extracted rather than re-implemented, and that is the whole point: two
    /// independently-written hashes over "the transcript" would agree in every
    /// test and diverge on the first ordering or length-prefix detail, which
    /// would show up as a claim row that never resolves against the job it
    /// names — silent, and only visible on a device pull.
    ///
    /// The caller owns canonicalization. `runBackfill` atomizes
    /// `TranscriptChunkCanonicalizer.canonicalize(...)` output, so a caller
    /// starting from RAW persisted rows must canonicalize first or it will
    /// compute a different version for the same asset (final-pass chunks
    /// REPLACE the fast coverage they overlap, so the two chunk sets genuinely
    /// differ). See ``SemanticScanClaim/transcriptVersion(forPersistedChunks:)``.
    static func transcriptVersionHash(chunks: [TranscriptChunk]) -> String {
        versionHash(ofSorted: chunks.sorted(by: TranscriptChunkCanonicalizer.canonicalTimeOrder))
    }

    /// The hash itself, over an ALREADY time-ordered chunk array.
    ///
    /// Length-prefix each chunk to prevent boundary ambiguity: `["ab","cd"]`
    /// and `["a","bcd"]` must produce different hashes.
    private static func versionHash(ofSorted sorted: [TranscriptChunk]) -> String {
        var hasher = SHA256()
        for chunk in sorted {
            let textData = Data(chunk.normalizedText.utf8)
            withUnsafeBytes(of: UInt32(textData.count).bigEndian) { hasher.update(bufferPointer: $0) }
            hasher.update(data: textData)
        }
        return hasher.finalize().prefix(16).map { String(format: "%02x", $0) }.joined()
    }
}
