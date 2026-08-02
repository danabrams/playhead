// FastTranscriptCoverageIndex.swift
// playhead-mptr: an in-memory index of what the `pass = 'fast'` transcript
// chunks ACTUALLY back, used to skip re-transcribing audio we already have.
//
// WHY THIS EXISTS. `TranscriptEngineService.runTranscriptionLoop` used to hand
// every shard to `transcribeShard`, which ran the full ASR pass and only then
// deduplicated by segment fingerprint. The dedup saved a row insert; it did not
// save the transcription. So a re-run over an asset that already held 45 minutes
// of transcript paid for 45 minutes of ASR before it reached a single second of
// new audio — and `AnalysisJobRunner` caps the whole transcription stage at a
// flat 300 s. Past a certain amount of existing coverage the cap always wins,
// the run persists zero chunks, and the watermark can never advance again.
// That ceiling is self-reinforcing: every second of coverage earned makes the
// next run more expensive. It is what stranded a field episode at 68.7 % across
// five consecutive attempts, each reported as `engine_silent_timeout`.
//
// WHY NOT JUST FILTER ON THE WATERMARK. Because review playhead-rfu-aac H3
// removed exactly that, for a good reason: `fastTranscriptCoverageEndTime` is a
// HIGH-WATER REACH, not a statement that every second below it was transcribed.
// Shards behind the playhead can sit under the watermark having never run, and
// playhead-0sro documents the crash shape where the watermark outlives the
// chunks entirely. A watermark-only filter would skip real work and call the
// episode done.
//
// So this index does what H3 asked for and checks the ARTIFACTS. A shard is
// skipped only when BOTH hold:
//
//   1. the durable watermark has reached past the shard's end — some completed
//      pass claims to have gone at least this far, and
//   2. a persisted `pass = 'fast'` chunk actually overlaps the shard's range —
//      there is a real row on disk behind the claim.
//
// Condition 2 is what H3's counterexamples fail: a behind-playhead shard that
// never ran has no overlapping chunk, and neither does an asset whose chunks
// were lost. Both keep their ASR pass. Nothing is skipped on the strength of a
// scalar alone.
//
// THE TRADEOFF, TAKEN DELIBERATELY. A skipped shard no longer re-runs the
// metadata-upgrade arm of `transcribeShard` (weak-anchor merges, speaker
// backfill, confidence backfill on rows that lacked them). Those upgrades are
// worth strictly less than the episode being transcribable at all: the
// alternative is that the tail of the episode is never read, so it can never be
// scanned for ads no matter how good detection becomes. A shard with no
// overlapping chunk still takes the full path, upgrades included.

import Foundation

/// A merged, sorted view of the time ranges covered by an asset's persisted
/// `pass = 'fast'` transcript chunks.
///
/// Construction merges overlapping and touching inputs, so the stored intervals
/// are disjoint and ascending. That is what lets ``overlaps(start:end:)`` be a
/// binary search rather than a scan — the loop asks this question once per
/// shard, and an episode can carry thousands of chunks.
struct FastTranscriptCoverageIndex: Sendable, Equatable {

    /// A half-open covered range `[start, end)`. Disjoint and ascending across
    /// the array; see ``init(chunkRanges:)``.
    struct Interval: Sendable, Equatable {
        var start: Double
        var end: Double
    }

    /// Merged, disjoint, ascending.
    private(set) var intervals: [Interval]

    /// An index backing nothing. Every query returns `false`, so a caller that
    /// fails to load coverage transcribes everything — the pre-mptr behavior,
    /// which is the safe direction to fail in.
    static let empty = FastTranscriptCoverageIndex(intervals: [])

    private init(intervals: [Interval]) {
        self.intervals = intervals
    }

    /// Build from raw chunk `(startTime, endTime)` pairs in any order.
    ///
    /// Degenerate and non-finite inputs are dropped rather than merged: a chunk
    /// with `end <= start` covers nothing, and a `NaN` bound would poison every
    /// comparison in the binary search below. The store query already filters
    /// `endTime > startTime`, so this is a second line of defence for callers
    /// that build an index from somewhere else (tests, migrations).
    init(chunkRanges: [(start: Double, end: Double)]) {
        let usable = chunkRanges
            .filter { $0.start.isFinite && $0.end.isFinite && $0.end > $0.start }
            .sorted { $0.start < $1.start }
        var merged: [Interval] = []
        merged.reserveCapacity(usable.count)
        for range in usable {
            // Touching ranges merge too (`start <= last.end`, not `<`): ASR
            // segments frequently abut exactly, and leaving those as separate
            // intervals would make a shard that spans the seam read as
            // uncovered for no physical reason.
            if var last = merged.last, range.start <= last.end {
                last.end = max(last.end, range.end)
                merged[merged.count - 1] = last
            } else {
                merged.append(Interval(start: range.start, end: range.end))
            }
        }
        self.intervals = merged
    }

    /// Whether any covered interval intersects `[start, end)`.
    ///
    /// Binary search for the last interval beginning at or before `start`; that
    /// one, or its successor, is the only pair that can intersect once the
    /// intervals are disjoint and ascending.
    func overlaps(start: Double, end: Double) -> Bool {
        guard start.isFinite, end.isFinite, end > start, !intervals.isEmpty else {
            return false
        }
        var low = 0
        var high = intervals.count - 1
        var candidate = -1
        while low <= high {
            let mid = low + (high - low) / 2
            if intervals[mid].start <= start {
                candidate = mid
                low = mid + 1
            } else {
                high = mid - 1
            }
        }
        if candidate >= 0, intervals[candidate].end > start {
            return true
        }
        let next = candidate + 1
        return next < intervals.count && intervals[next].start < end
    }

    /// Whether a shard spanning `[shardStart, shardEnd)` may be skipped without
    /// running ASR over it.
    ///
    /// Both conditions are load-bearing and neither is sufficient alone — see
    /// this file's header. `watermark` is
    /// `analysis_assets.fastTranscriptCoverageEndTime`; `nil` means the asset
    /// has no watermark at all, which is never a licence to skip.
    func isShardAlreadyTranscribed(
        shardStart: Double,
        shardEnd: Double,
        watermark: Double?
    ) -> Bool {
        guard let watermark, watermark.isFinite, watermark >= shardEnd else {
            return false
        }
        return overlaps(start: shardStart, end: shardEnd)
    }
}
