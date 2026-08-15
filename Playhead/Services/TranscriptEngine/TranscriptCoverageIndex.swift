// TranscriptCoverageIndex.swift
// playhead-mptr: an in-memory index of what an asset's persisted transcript
// chunks ACTUALLY back, used to transcribe unread audio BEFORE audio we already
// hold — not to skip anything.
//
// playhead-6r4z: BOTH PASSES, AND THE NAME SAYS SO NOW. This shipped as
// `FastTranscriptCoverageIndex` reading `pass = 'fast'` alone, and the fix was
// partly defeating itself in the field. A region the FINAL pass covers has no
// fast artifact to point at — either the two passes ran over disjoint spans, or
// `TranscriptChunkCanonicalizer.canonicalize` dropped the fast chunks a final
// chunk fully contains — so it failed condition 2 below, sorted UNCOVERED, and,
// the partition being stable over playhead-proximity order, floated to the FRONT
// of the very pass that was minted to read the audio behind it.
//
// Measured on the 2026-08-03 device pull at 30 s shards, re-derived for this
// bead: 215 shards (6,450 s) lie below their asset's fast watermark backed ONLY
// by a final-pass chunk, out of 1,174 shards below a watermark across the 12
// assets — SEVEN of the twelve carry some (the bead's ticket named six; 83592353
// is the seventh, 5 shards / 150 s). On 48E903D7 that re-read prefix is 1,230 s
// against 103.1 s of genuinely new audio, a 11.9:1 inversion inside a 300 s stage
// cap.
//
// IS THE ARTIFACT BEHIND A MOVED SHARD THIN? This is the only way the widening
// could hurt — `overlaps` is satisfied by ANY overlap, so a one-second final
// chunk would be enough to sort a 30 s shard last. R1 review replaced the first
// draft's answer here, which was "of those 215 shards, ZERO lacked a chunk of
// either pass": true, and empty, because a shard moves precisely when a chunk
// of some pass overlaps it and the count could not have come out otherwise.
//
// The measurement with content is the FILL: what fraction of a moved shard the
// union of chunks actually spans. On the same pull the moved shards run
// min 0.610 / p10 0.806 / median 0.906, and NONE is under 0.25 — against the
// 959 shards mptr already sorted last, whose minimum is 0.266. The audio this
// deprioritises is better backed than the audio already being deprioritised,
// which is the comparison that makes the widening safe. (Chunks are
// utterance-level, ~0.9 s mean, so a fill below 1.0 is silence between
// utterances, not unread audio — which is why the CONTROL population, not 1.0,
// is the yardstick.)
//
// This is the same fast-only under-report `playhead-9y9e` fixed for the ad-scan
// bound. The canonical union — `AnalysisStore.fetchTranscribedRegion` — already
// existed; it simply had not been applied here.
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
//   2. a persisted transcript chunk of EITHER PASS actually overlaps the shard's
//      range — there is a real row on disk behind the claim.
//
// H3's counterexamples fail condition 2, so they sort as UNCOVERED and run
// first — stronger than the pre-mptr behaviour, not weaker. playhead-6r4z
// widened condition 2 and left condition 1 exactly where it was, which is the
// conservative half: the watermark is still `fastTranscriptCoverageEndTime`, so
// final-pass coverage ABOVE the fast watermark still sorts first. That is real
// and it is a separate question, because raising the watermark changes what
// "some pass claims to have gone this far" MEANS rather than which artifact
// backs the claim. Filed as `playhead-9j94`.
//
// R1 REVIEW CORRECTED THE SIZE OF IT, and the error was this bead's own defect
// class. The first draft of this paragraph said "480 s on D9B513CD and 96 s on
// 83592353", and the bead comment added "which is ALL of the remaining unread
// audio on both". Those are `episodeDurationSec - watermark` — the SPAN above
// the watermark — read as though it were the audio a final pass has already
// covered inside that span. Measured, on the same 2026-08-03 pull:
//
//   asset      span above wm   UNION of chunks in it   what is actually re-read
//   D9B513CD        479.9 s          50.3 s (10.5 %)       ~60 s, 2 shards
//   83592353         95.9 s          58.0 s (60.5 %)       ~66 s, 3 shards
//
// So raising condition 1 would recover tens of seconds, not 576 s, and it is
// ~10x smaller than quoted on the bigger of the two. The tell is the standing
// one: `MAX(endTime)` over final chunks is a REACH — D9B513CD's is 3,929.8 s
// against a 3,929.9 s episode, which reads as 100 % and is 10.5 % of the span.
// Ask what the value would read if the thing it claims to measure had never
// happened: an episode with ONE final chunk at the very end reads the same.
//
// WHY WIDENING CONDITION 2 LOSES NOTHING, since it is a behaviour change on
// every run. Re-running ASR over a final-covered region is the LEAST valuable
// place the stage's budget can go, not merely a cheaper one:
// `TranscriptChunkCanonicalizer.canonicalize` drops fast chunks a final chunk
// fully contains, so the fast rows the re-read would produce there are dropped
// from the canonical stream anyway; the finalize floor and the ad-scan bound
// have measured the UNION since playhead-9y9e, so deferring this work moves
// neither; and it is still an ORDERING, so the budget that survives the unread
// audio still reaches these shards and still performs the duplicate-fingerprint
// `speakerId` / `avgConfidence` upgrades.

import Foundation

/// A merged, sorted view of the time ranges covered by an asset's persisted
/// transcript chunks, across BOTH passes.
///
/// Construction merges overlapping and touching inputs, so the stored intervals
/// are disjoint and ascending. That is what lets ``overlaps(start:end:)`` be a
/// binary search rather than a scan — the loop asks this question once per
/// shard, and an episode can carry thousands of chunks.
struct TranscriptCoverageIndex: Sendable, Equatable {

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
    static let empty = TranscriptCoverageIndex(intervals: [])

    private init(intervals: [Interval]) {
        self.intervals = intervals
    }

    /// playhead-6r4z: THE PRODUCTION DOOR, and it takes the typed region rather
    /// than an array of pairs.
    ///
    /// The bug this bead fixed was writable in one token — `AnalysisStore`
    /// offers `fetchFastTranscriptCoveredRanges` (the fast pass alone) beside
    /// ``AnalysisStore/fetchTranscribedRegion(assetId:)`` (both passes), and
    /// until playhead-x0lb typed the second one they returned the identical bare
    /// `[(start: Double, end: Double)]`. Naming ``TranscribedRegion`` here is
    /// what makes the narrower query unwritable at the one call site that
    /// matters; rail TY33 in `scripts/mutation-battery-untypeable.py` is that
    /// substitution, and it must fail to COMPILE.
    ///
    /// ``init(chunkRanges:)`` stays for callers that are not reading the store —
    /// tests and migrations — where there is no second population in scope to
    /// confuse.
    init(transcribedRegion: TranscribedRegion) {
        self.init(chunkRanges: transcribedRegion.transcribedSpans)
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
    ///
    /// playhead-6r4z: the two conditions are deliberately asymmetric about which
    /// pass they consult. The ARTIFACT test spans both passes — a final-pass row
    /// is as real a row as a fast one — while the WATERMARK stays the fast
    /// pass's, so the widening can only ever move a shard that is already inside
    /// the reach some completed pass claimed.
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

    /// playhead-pnb5: the stable partition ``orderingUncoveredFirst(_:watermark:)``
    /// is built from — exposed so a caller can ask HOW MANY shards land in the
    /// first group without re-deriving the rule.
    ///
    /// One implementation, two readers, deliberately. `AnalysisJobRunner` uses
    /// `withNoArtifact.isEmpty` to decide whether running the transcription stage
    /// can produce anything at all, and `TranscriptEngineService` uses the
    /// ordering; if those two ever disagreed about what "already transcribed"
    /// means, the runner would skip a stage the engine would have found work in
    /// — or, worse, keep paying for a stage that has none. Expressing the
    /// ordering in terms of this partition is what makes the disagreement
    /// unwritable.
    ///
    /// - Parameter watermark: `analysis_assets.fastTranscriptCoverageEndTime`.
    ///   `nil` puts every shard in `withNoArtifact`, which is the safe direction
    ///   — it transcribes everything.
    func partitioningByTranscriptArtifact(
        _ shards: [AnalysisShard],
        watermark: Double?
    ) -> (withNoArtifact: [AnalysisShard], alreadyBacked: [AnalysisShard]) {
        var withNoArtifact: [AnalysisShard] = []
        var alreadyBacked: [AnalysisShard] = []
        withNoArtifact.reserveCapacity(shards.count)
        for shard in shards {
            if isShardAlreadyTranscribed(
                shardStart: shard.startTime,
                shardEnd: shard.startTime + shard.duration,
                watermark: watermark
            ) {
                alreadyBacked.append(shard)
            } else {
                withNoArtifact.append(shard)
            }
        }
        return (withNoArtifact, alreadyBacked)
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
        let partition = partitioningByTranscriptArtifact(shards, watermark: watermark)
        return partition.withNoArtifact + partition.alreadyBacked
    }
}
