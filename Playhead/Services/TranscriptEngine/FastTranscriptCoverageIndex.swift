// FastTranscriptCoverageIndex.swift
// playhead-mptr: an in-memory index of what the `pass = 'fast'` transcript
// chunks ACTUALLY back, used to transcribe unread audio BEFORE audio we already
// hold — not to skip anything.
//
// WHY THIS EXISTS. `TranscriptEngineService.runTranscriptionLoop` walks shards
// in playhead-proximity order, which on a re-run means starting over from the
// beginning of the episode. `transcribeShard` runs the full ASR pass and only
// then deduplicates by segment fingerprint — the dedup saves a row insert, not
// the transcription. So a re-run over an asset already holding 45 minutes of
// transcript paid for 45 minutes of ASR before reaching a single second of new
// audio, and `AnalysisJobRunner` caps the whole transcription stage at a flat
// 300 s. Past a certain amount of existing coverage the cap always won first:
// the run persisted zero chunks and the watermark could never advance.
//
// The ceiling is self-reinforcing — every second of coverage earned makes the
// next run more expensive — which is what stranded a field episode at 68.7 %
// across five consecutive attempts, each reported `engine_silent_timeout` with
// `chunks_persisted = 0`.
//
// WHAT THIS IS NOT. The first shape of this fix SKIPPED already-backed shards.
// It works, and two existing tests immediately showed it is a capability loss:
// `transcribeShard`'s duplicate-fingerprint arm is what backfills `speakerId`
// and `avgConfidence` onto rows that lacked them and re-emits the upgraded
// chunk, and that arm is fed by re-running the ASR. A skipped shard can never be
// enriched. So the index orders instead: everything still runs, and only the
// order changes. A wrong answer here costs latency, never coverage.
//
// WHY NOT JUST ORDER ON THE WATERMARK. Because review playhead-rfu-aac H3
// removed a watermark-only filter for a reason that still stands:
// `fastTranscriptCoverageEndTime` is a HIGH-WATER REACH, not a statement that
// every second below it was transcribed. Shards behind the playhead can sit
// under the watermark having never run, and playhead-0sro documents the crash
// shape where the watermark outlives the chunks entirely.
//
// So this index checks the ARTIFACTS. A shard counts as already transcribed —
// and therefore sorts LAST — only when BOTH hold:
//
//   1. the durable watermark has reached past the shard's end — some completed
//      pass claims to have gone at least this far, and
//   2. a persisted `pass = 'fast'` chunk actually overlaps the shard's range —
//      there is a real row on disk behind the claim.
//
// H3's counterexamples fail condition 2, so they sort as UNCOVERED and run
// first — stronger than the pre-mptr behaviour, not weaker.

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

    /// Stable partition of `shards`: audio nothing backs yet comes first,
    /// audio we already hold comes after, each group keeping the playhead
    /// proximity order `prioritizeShards` gave it.
    ///
    /// THIS IS THE FIX, AND IT IS DELIBERATELY NOT A SKIP. The first shape of
    /// playhead-mptr dropped already-backed shards outright, which does fix the
    /// stall — and which two existing tests immediately proved is a capability
    /// LOSS, not a saving. `transcribeShard`'s duplicate-fingerprint arm is what
    /// backfills `speakerId` and `avgConfidence` onto rows that lacked them and
    /// re-emits the upgraded chunk; that arm is fed by re-running the ASR, so a
    /// shard that is skipped can never be enriched.
    /// (`duplicate fingerprint can upgrade missing speakerId and avgConfidence
    /// and re-emit chunk`, and `a second row for one (asset, pass, fingerprint)
    /// is refused, and the survivor still takes the speakerId upgrade`.)
    ///
    /// Ordering costs nothing and loses nothing. The 300 s stage cap in
    /// `AnalysisJobRunner` is spent on audio nobody has read yet, so the
    /// watermark always advances; whatever budget is left over still re-runs the
    /// covered shards and still performs their upgrades. The pathology this
    /// removes is not "covered shards get transcribed" — it is "covered shards
    /// get transcribed FIRST, and consume the entire budget before the run
    /// reaches anything new", which is what made an episode's own progress the
    /// thing that stranded it.
    func orderingUncoveredFirst(
        _ shards: [AnalysisShard],
        watermark: Double?
    ) -> [AnalysisShard] {
        var uncovered: [AnalysisShard] = []
        var covered: [AnalysisShard] = []
        uncovered.reserveCapacity(shards.count)
        for shard in shards {
            if isShardAlreadyTranscribed(
                shardStart: shard.startTime,
                shardEnd: shard.startTime + shard.duration,
                watermark: watermark
            ) {
                covered.append(shard)
            } else {
                uncovered.append(shard)
            }
        }
        return uncovered + covered
    }
}
