// TranscriptChunkCanonicalizer.swift
// playhead-hc7e — one canonical transcript for every backfill consumer.
//
// Background. `AdDetectionService.runBackfill` used to select the
// atomization transcript with:
//
//     let filtered = chunks.filter { $0.pass == "final" }
//     return filtered.isEmpty ? chunks : filtered
//
// That collapses the timeline to ONLY the final-pass chunks the moment a
// single final chunk exists. But `FinalPassRetranscriptionRunner` creates
// `pass == "final"` rows ONLY around already-detected candidate windows —
// so the "final" set is candidate-local, and atomizing it produces a
// candidate-local atom timeline instead of full-episode coverage. In the
// same run, the lexical scanner still received the RAW mixed-pass array,
// so a fast chunk and an overlapping final chunk covering the same audio
// both got scanned and the same ad text contributed evidence TWICE.
//
// This canonicalizer builds ONE deterministic transcript where:
//   • final chunks REPLACE the fast coverage they overlap (final text is
//     used only in the intervals the final pass actually re-transcribed);
//   • fast chunks REMAIN everywhere the final pass never covered, so
//     full-episode coverage is retained;
//   • fast chunks FULLY inside the final coverage are dropped (their audio
//     is already represented by the higher-quality final text) — this is
//     what removes the duplicate lexical/catalog/FM evidence;
//   • fast chunks that only PARTIALLY overlap a final interval (at a window
//     edge) are KEPT, so no second of audio ever loses coverage. The
//     `residualFastFinalOverlapCount` diagnostic counts these so a partial
//     replacement that could still double-count cannot recur silently.
//
// Single-pass transcripts — all-fast or all-final — pass through
// byte-identically (same array, same order, same chunkIndex), so this is a
// no-op for every asset that has not had a final-pass run. That is the
// no-regression contract the mixed/all-fast/all-final acceptance tests pin.
//
// Ordering note: `TranscriptAtomizer.atomize` sorts its input by
// `chunkIndex`, and final-pass rows are always persisted with a chunkIndex
// strictly greater than every fast row (see
// `FinalPassRetranscriptionRunner.nextFinalChunkIndex`). Feeding a merged
// fast+final set to the atomizer without re-indexing would therefore sink
// every final chunk to the tail — out of temporal order. For the mixed
// case we re-sort by time and REASSIGN chunkIndex to the time-sorted
// position so the atom sequence stays time-ordered with final chunks
// interleaved at their true position. Re-indexing only ever runs on the
// mixed path (a genuinely new, higher-quality transcript); single-pass
// inputs are never re-indexed.

import Foundation

enum TranscriptChunkCanonicalizer {

    /// The `pass` value that marks a higher-quality final-pass chunk.
    static let finalPass = TranscriptPassType.final_.rawValue

    /// Interval-boundary tolerance (seconds). Chunk timings are ASR-derived
    /// doubles; a fixed epsilon keeps containment/overlap tests from
    /// flapping on sub-microsecond float noise without being wall-clock
    /// dependent.
    static let boundaryEpsilon: Double = 1e-6

    // MARK: - Diagnostics

    /// Coverage + duplicate-evidence diagnostics for one canonicalization.
    /// Logged once per `runBackfill` and asserted directly in tests so a
    /// silent regression back to partial (coverage-losing) replacement is
    /// caught.
    struct Diagnostics: Sendable, Equatable {
        /// Total chunks handed in (fast + final).
        let inputCount: Int
        /// `pass == "final"` chunks in the input.
        let finalCount: Int
        /// Non-final ("fast") chunks in the input.
        let fastCount: Int
        /// Fast chunks fully covered by the final union and therefore
        /// dropped (final text replaces them).
        let droppedFastCount: Int
        /// Fast chunks kept in the canonical transcript.
        let retainedFastCount: Int
        /// Retained fast chunks that STILL overlap a final interval (a
        /// partial, window-edge overlap). `0` in the clean case where the
        /// final pass fully covers every fast chunk it touches; `> 0`
        /// signals residual duplicate-evidence risk that a reviewer should
        /// look at.
        let residualFastFinalOverlapCount: Int
        /// Union of all input chunk intervals, in seconds.
        let inputCoverageSeconds: Double
        /// Union of all canonical chunk intervals, in seconds. MUST equal
        /// `inputCoverageSeconds` — dropping a fully-covered fast chunk
        /// removes no coverage because its audio is inside the final union.
        let canonicalCoverageSeconds: Double
        /// `true` when the input was single-pass and returned unchanged.
        let isPassthrough: Bool

        /// Full-episode coverage was preserved by canonicalization.
        var coverageRetained: Bool {
            abs(inputCoverageSeconds - canonicalCoverageSeconds)
                <= TranscriptChunkCanonicalizer.boundaryEpsilon
        }

        /// At least one retained fast chunk still overlaps a final interval,
        /// so overlapping fast+final text could still be scanned twice.
        var hasResidualDuplicateEvidence: Bool {
            residualFastFinalOverlapCount > 0
        }
    }

    struct Result: Sendable {
        let chunks: [TranscriptChunk]
        let diagnostics: Diagnostics
    }

    // MARK: - Canonicalize

    static func canonicalize(_ chunks: [TranscriptChunk]) -> Result {
        let finals = chunks.filter { $0.pass == finalPass }
        let fasts = chunks.filter { $0.pass != finalPass }
        let inputCoverage = coveredSeconds(chunks)

        // Single-pass ⇒ byte-identical passthrough. All-fast and all-final
        // transcripts are returned unchanged (same array, order, indices),
        // which is the no-regression guarantee.
        guard !finals.isEmpty, !fasts.isEmpty else {
            return Result(
                chunks: chunks,
                diagnostics: Diagnostics(
                    inputCount: chunks.count,
                    finalCount: finals.count,
                    fastCount: fasts.count,
                    droppedFastCount: 0,
                    retainedFastCount: fasts.count,
                    residualFastFinalOverlapCount: 0,
                    inputCoverageSeconds: inputCoverage,
                    canonicalCoverageSeconds: inputCoverage,
                    isPassthrough: true
                )
            )
        }

        // Intervals the final pass covers, merged so touching/overlapping
        // final chunks form one interval.
        let finalUnion = mergeIntervals(finals.map { ($0.startTime, $0.endTime) })

        var retainedFast: [TranscriptChunk] = []
        var droppedFast = 0
        var residualOverlap = 0
        for fast in fasts {
            if isFullyCovered(start: fast.startTime, end: fast.endTime, by: finalUnion) {
                // Final replaces this fast chunk entirely.
                droppedFast += 1
            } else {
                retainedFast.append(fast)
                if overlapsAny(start: fast.startTime, end: fast.endTime, intervals: finalUnion) {
                    residualOverlap += 1
                }
            }
        }

        // Combine, order by time, and re-index so the atomizer's
        // chunkIndex sort yields a time-ordered sequence (see file header).
        let combined = retainedFast + finals
        let ordered = combined.sorted(by: chunkOrdering)
        let reindexed = ordered.enumerated().map { position, chunk in
            withChunkIndex(chunk, position)
        }

        return Result(
            chunks: reindexed,
            diagnostics: Diagnostics(
                inputCount: chunks.count,
                finalCount: finals.count,
                fastCount: fasts.count,
                droppedFastCount: droppedFast,
                retainedFastCount: retainedFast.count,
                residualFastFinalOverlapCount: residualOverlap,
                inputCoverageSeconds: inputCoverage,
                canonicalCoverageSeconds: coveredSeconds(reindexed),
                isPassthrough: false
            )
        )
    }

    // MARK: - Ordering

    /// Deterministic total order: start, then end, then final-before-fast at
    /// an identical span, then persisted chunkIndex, then id.
    private static func chunkOrdering(_ lhs: TranscriptChunk, _ rhs: TranscriptChunk) -> Bool {
        if lhs.startTime != rhs.startTime { return lhs.startTime < rhs.startTime }
        if lhs.endTime != rhs.endTime { return lhs.endTime < rhs.endTime }
        let lr = passRank(lhs.pass)
        let rr = passRank(rhs.pass)
        if lr != rr { return lr < rr }
        if lhs.chunkIndex != rhs.chunkIndex { return lhs.chunkIndex < rhs.chunkIndex }
        return lhs.id < rhs.id
    }

    private static func passRank(_ pass: String) -> Int {
        pass == finalPass ? 0 : 1
    }

    private static func withChunkIndex(_ chunk: TranscriptChunk, _ index: Int) -> TranscriptChunk {
        TranscriptChunk(
            id: chunk.id,
            analysisAssetId: chunk.analysisAssetId,
            segmentFingerprint: chunk.segmentFingerprint,
            chunkIndex: index,
            startTime: chunk.startTime,
            endTime: chunk.endTime,
            text: chunk.text,
            normalizedText: chunk.normalizedText,
            pass: chunk.pass,
            modelVersion: chunk.modelVersion,
            transcriptVersion: chunk.transcriptVersion,
            atomOrdinal: chunk.atomOrdinal,
            weakAnchorMetadata: chunk.weakAnchorMetadata,
            speakerId: chunk.speakerId,
            avgConfidence: chunk.avgConfidence
        )
    }

    // MARK: - Interval helpers

    /// Merge intervals, dropping zero-length / inverted spans. Two
    /// intervals are joined when the next starts at or before the current
    /// end (within `boundaryEpsilon`).
    static func mergeIntervals(_ raw: [(Double, Double)]) -> [(Double, Double)] {
        let valid = raw
            .filter { $0.1 > $0.0 }
            .sorted { $0.0 < $1.0 }
        guard var current = valid.first else { return [] }
        var merged: [(Double, Double)] = []
        for interval in valid.dropFirst() {
            if interval.0 <= current.1 + boundaryEpsilon {
                current.1 = max(current.1, interval.1)
            } else {
                merged.append(current)
                current = interval
            }
        }
        merged.append(current)
        return merged
    }

    private static func isFullyCovered(
        start: Double,
        end: Double,
        by intervals: [(Double, Double)]
    ) -> Bool {
        for interval in intervals
        where interval.0 - boundaryEpsilon <= start && end <= interval.1 + boundaryEpsilon {
            return true
        }
        return false
    }

    private static func overlapsAny(
        start: Double,
        end: Double,
        intervals: [(Double, Double)]
    ) -> Bool {
        for interval in intervals
        where start < interval.1 - boundaryEpsilon && interval.0 < end - boundaryEpsilon {
            return true
        }
        return false
    }

    /// Total seconds covered by the union of every chunk's interval.
    private static func coveredSeconds(_ chunks: [TranscriptChunk]) -> Double {
        mergeIntervals(chunks.map { ($0.startTime, $0.endTime) })
            .reduce(0) { $0 + ($1.1 - $1.0) }
    }
}
