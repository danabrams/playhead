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
// ⚠️ READ THIS BEFORE CONSUMING `Result.chunks` (playhead-r5um). Because that
// passthrough is byte-identical, the array it returns is NOT time-ordered for
// a single-pass asset — it is whatever `fetchTranscriptChunks` returned, which
// is `ORDER BY chunkIndex`, and 7 of the 10 single-pass assets on the
// 2026-07-30 device pull step backward in that order (worst −1470.8 s). The
// passthrough is kept byte-identical on purpose: hc7e pins it, and
// playhead-kcz.1 pins the transcript-peek display identity that rides on it.
// So every DETECTION consumer must order the array itself with
// `canonicalTimeOrder`. `TranscriptAtomizer.atomize` does. So does
// `AdDetectionService.runBackfill`, for the consumers that read the chunks raw
// rather than atomizing (`LexicalAnchorRefiner.buildWordStream`, the
// `RegionShadowPhase` input). Sorting is idempotent, so doing it is always
// safe; assuming it has been done is not.
//
// ORDERING (playhead-r5um). `canonicalTimeOrder` below is the ONE ordering
// authority for transcript chunks in this app; `TranscriptAtomizer.atomize`
// and the transcript-peek display path both sort with it. It is a total
// order — the trailing `id` tiebreak guarantees that — so applying it twice
// is idempotent and the array this canonicalizer emits is already the atom
// sequence the atomizer will produce.
//
// This type used to REASSIGN `chunkIndex` to the time-sorted position on the
// mixed path. That existed for exactly one reason: `atomize` sorted by
// `chunkIndex`, and final-pass rows are persisted with a chunkIndex strictly
// greater than every fast row (`FinalPassRetranscriptionRunner
// .nextFinalChunkIndex`), so an un-reindexed merge sank every final chunk to
// the tail. Now that `atomize` orders by TIME directly, the re-index is dead
// weight — and worse, a second ordering mechanism that could drift from this
// one. It is gone. `chunkIndex` on the returned chunks is the persisted
// value, not a fabricated position; nothing downstream may read it as a time
// ordinal (it never was one — see `TranscriptEngineService.chunkCounter`,
// which numbers chunks in shard EMISSION order, and `prioritizeShards`,
// which emits by playhead proximity).

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

        // Combine and order by time. No re-indexing: `atomize` orders by the
        // same total order, so the array emitted here IS the atom sequence.
        let combined = retainedFast + finals
        let ordered = combined.sorted(by: canonicalTimeOrder)

        return Result(
            chunks: ordered,
            diagnostics: Diagnostics(
                inputCount: chunks.count,
                finalCount: finals.count,
                fastCount: fasts.count,
                droppedFastCount: droppedFast,
                retainedFastCount: retainedFast.count,
                residualFastFinalOverlapCount: residualOverlap,
                inputCoverageSeconds: inputCoverage,
                canonicalCoverageSeconds: coveredSeconds(ordered),
                isPassthrough: false
            )
        )
    }

    // MARK: - Ordering

    /// THE canonical time order for transcript chunks (playhead-r5um).
    ///
    /// A deterministic TOTAL order: start, then end, then final-before-fast at
    /// an identical span, then persisted chunkIndex, then id. Every consumer
    /// that needs transcript chunks in time order sorts with this and only
    /// this — `TranscriptAtomizer.atomize`, this canonicalizer's merge, and
    /// the transcript-peek display path.
    ///
    /// Totality is load-bearing, not decoration. `chunkIndex` is NOT unique
    /// per asset: `TranscriptEngineService.chunkCounter` is an in-memory
    /// counter reset to 0 whenever the engine stops or switches assets, so a
    /// re-transcribed episode carries several rows numbered 0, 1, 2… (21 of
    /// 30 assets on the 2026-07-30 device pull; one asset has five rows at
    /// index 0). `Array.sorted(by:)` is explicitly not guaranteed stable, so
    /// a comparator that leaves ties unresolved leaves the atom sequence —
    /// and therefore `transcriptVersion` — undefined for those assets. The
    /// trailing `id` tiebreak is what makes the hash mean something.
    static func canonicalTimeOrder(_ lhs: TranscriptChunk, _ rhs: TranscriptChunk) -> Bool {
        let lhsStart = orderKey(lhs.startTime)
        let rhsStart = orderKey(rhs.startTime)
        if lhsStart != rhsStart { return lhsStart < rhsStart }
        let lhsEnd = orderKey(lhs.endTime)
        let rhsEnd = orderKey(rhs.endTime)
        if lhsEnd != rhsEnd { return lhsEnd < rhsEnd }
        let lr = passRank(lhs.pass)
        let rr = passRank(rhs.pass)
        if lr != rr { return lr < rr }
        if lhs.chunkIndex != rhs.chunkIndex { return lhs.chunkIndex < rhs.chunkIndex }
        return lhs.id < rhs.id
    }

    private static func passRank(_ pass: String) -> Int {
        pass == finalPass ? 0 : 1
    }

    /// Collapse non-finite times to a single sentinel so `canonicalTimeOrder`
    /// stays a strict weak ordering.
    ///
    /// Chunk times are ASR-derived doubles with no non-finite guard on the way
    /// into SQLite. A raw `NaN` would compare unequal to everything and less
    /// than nothing, so `a < b` and `b < a` would both be false while `a` and
    /// `b` are each "equivalent" to values that are not equivalent to each
    /// other — an intransitive comparator, which is undefined behaviour in
    /// `sorted(by:)` and trips its strict-weak-ordering precondition. The old
    /// `chunkIndex` comparator was `Int` and could not reach this; ordering by
    /// time can, on all six atomize lanes.
    ///
    /// Mapping every non-finite value to `+infinity` sorts the garbage last,
    /// deterministically, and leaves the tie to be broken by `pass`,
    /// `chunkIndex` and finally `id`. Finite values — every row on the
    /// 2026-07-30 device pull — pass through untouched, so this is
    /// byte-identical for all real data.
    private static func orderKey(_ value: Double) -> Double {
        value.isFinite ? value : .infinity
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
