// SupportLineIndex.swift
// playhead-shu5: WHAT `supportLineRefs` ARE INDICES INTO, and the proof.
//
// # The coordinate system
//
// A coarse (`passA`) `semantic_scan_results` row's `spansJSON` is one
// `CoarseSupportSchema` object — `{"supportLineRefs":[46],"certainty":"strong"}`
// — and the integers in it are **`AdTranscriptSegment.segmentIndex` values in
// the episode-level segmentation of the row's own `transcriptVersion`**. Not
// atom ordinals, not chunk indices, not offsets inside the window.
//
// The chain, in the order it runs:
//
//   1. `TranscriptChunkCanonicalizer.canonicalize` drops the fast chunks a
//      final pass fully covers and sorts the rest by `canonicalTimeOrder`.
//   2. `TranscriptAtomizer.atomize` numbers that array — position IS
//      `atomOrdinal` — and hashes its `normalizedText` into
//      `transcriptVersion`.
//   3. `TranscriptSegmenter.segment` groups the atoms on pauses, max duration,
//      discourse markers and sentence punctuation, numbering the groups from 0.
//      That number is `segmentIndex`.
//   4. `FoundationModelClassifier.planPassA` walks the segments in index order
//      and fills each prompt to the token budget, so a coarse window's
//      `lineRefs` are `ordered[lowerBound...upperBound].map(\.segmentIndex)` —
//      a CONTIGUOUS run — and its persisted `windowStartTime`/`windowEndTime`
//      are that run's min/max segment times.
//   5. The model answers with `supportLineRefs` drawn from the line numbers it
//      was shown (`L46> "…"`, `PermissiveAdClassifier.buildPrompt`), and
//      `focusLineRefs` filters them against `window.lineRefs`.
//
// # How that was PROVEN rather than read (2026-08-19 t4 device pull)
//
// Steps 1–3 were re-implemented offline and checked against the device's own
// numbers, because a chain read out of source is a hypothesis:
//
//   * The offline `versionHash` reproduces the device's persisted
//     `transcriptVersion` EXACTLY on six assets — four of which
//     (`0FF7EFF3` 56/56, `9126552E` 24/24, `E24FB0CD` 63/63, `E30D13AB`
//     100/100) carry every one of their scan rows at that version.
//   * On those assets the reconstructed segmentation reproduces **321 of 321**
//     persisted `passA` windows exactly — both time bounds AND both atom
//     ordinals — as a contiguous run of segments.
//   * **276 of 276** `supportLineRefs` values fall INSIDE their own window's
//     contiguous segment-index range. Zero outside. A number drawn from any
//     other index space could not do that.
//   * Read as text they land on ad copy: `E30D13AB` [3.4–81.4] cites lines
//     0–1 (the T. Rowe Price read) and not line 2 ("I can't convince you that
//     we're not the Matrix"); `9126552E` [86.0–175.2] cites 4–5 (Tostitos plus
//     the Team Coco promo) and not 6–11, which are the show.
//
// # Why this type refuses more than it resolves
//
// `segmentIndex` is a coordinate in a system that is NOT persisted, and it
// moves when the transcript moves. The witness is Dan's own episode: on
// `CD2976E6` the coarse row at [1510.4–1611.4] was scanned at
// `transcriptVersion 807613cf` and cites line 62; a later final pass added
// chunks around 1585–1590 s, which added one segment boundary, and TODAY's
// segmentation (`cd175ee9`) has line 62 at **[1570.98, 1593.24]** — 22 seconds
// of Alex Honnold describing saying goodbye to his wife. The line the model
// actually meant is today's 63, [1593.66, 1611.42], which holds the "most
// replayed moment … check the description" call to action.
//
// So an index that resolved a stale row would not merely be unhelpful: it
// would mark the show and MISS the ad, with full confidence. That is the
// standing defect class — an identity that is not an identity — and it is why
// ``resolve(supportLineRefs:in:)`` requires BOTH that the transcript versions
// match AND that the row's own window reproduces exactly in this geometry.
// Neither check alone is sufficient: the version hash covers `normalizedText`
// and not TIMES, and matching endpoints alone cannot see a segmentation that
// differs in the middle.

import Foundation

/// A closed span of episode time in seconds. Geometry only — no evidence, no
/// grade, no provenance; the composer attaches all three.
struct AdSpanBounds: Sendable, Equatable {
    var start: Double
    var end: Double

    var duration: Double { end - start }

    /// Half-open overlap, the same test `SemanticSweepMarkComposer.Extent`
    /// uses, so a span and an extent can never disagree about touching.
    func overlaps(start otherStart: Double, end otherEnd: Double) -> Bool {
        otherStart < end && otherEnd > start
    }

    /// Does this span cover any part of the OPEN interval `(lower, upper)` —
    /// the gap between two extents a merge would swallow?
    ///
    /// Open rather than closed on purpose: two extents that merely TOUCH a
    /// span's edge (`upper == span.start`) leave no swallowed audio, and a
    /// closed test would bar a merge that costs nothing.
    func coversGap(from lower: Double, to upper: Double) -> Bool {
        start < upper && end > lower
    }

    /// This span narrowed to `bounds`, or `nil` when they do not overlap.
    func clamped(to bounds: AdSpanBounds) -> AdSpanBounds? {
        let lower = Swift.max(start, bounds.start)
        let upper = Swift.min(end, bounds.end)
        return upper > lower ? AdSpanBounds(start: lower, end: upper) : nil
    }
}

/// The segment geometry a coarse row's `supportLineRefs` are indices INTO,
/// stamped with the `transcriptVersion` it belongs to.
///
/// Built by the caller from the segments it already holds — the composer stays
/// a pure function of persisted rows plus this, and never reads a store.
struct SupportLineIndex: Sendable, Equatable {

    /// One transcript line: the segment the model was shown as `L<n>>`.
    struct Line: Sendable, Equatable {
        let startTime: Double
        let endTime: Double
        let firstAtomOrdinal: Int
        let lastAtomOrdinal: Int
    }

    /// Boundary tolerance when checking that a row's window reproduces here.
    ///
    /// Both numbers come from the same `min`/`max` over the same atom times and
    /// round-trip through SQLite as `REAL`, so equality is expected to be
    /// EXACT; the epsilon absorbs a last-bit difference rather than licensing a
    /// near-miss. It is deliberately far tighter than a segment (≥ 10 s by
    /// `TranscriptSegmenter.Config.minSegmentDuration` for a soft break), so no
    /// tolerance this size can ever admit the wrong line.
    static let boundaryEpsilon: Double = 1e-6

    /// The transcript this geometry describes. A row stamped with any other
    /// version is not addressable here — see the file header for what happens
    /// when that rule is skipped.
    let transcriptVersion: String

    private let lines: [Int: Line]

    init(transcriptVersion: String, lines: [Int: Line]) {
        self.transcriptVersion = transcriptVersion
        self.lines = lines
    }

    /// Build from the segmentation the caller ran, at the version it ran on.
    init(segments: [AdTranscriptSegment], transcriptVersion: String) {
        var lines: [Int: Line] = [:]
        lines.reserveCapacity(segments.count)
        for segment in segments {
            // First writer wins, so a duplicated `segmentIndex` (which
            // `planPassA`'s own sort tolerates) cannot silently redefine a line
            // the model was already shown.
            guard lines[segment.segmentIndex] == nil else { continue }
            lines[segment.segmentIndex] = Line(
                startTime: segment.startTime,
                endTime: segment.endTime,
                firstAtomOrdinal: segment.firstAtomOrdinal,
                lastAtomOrdinal: segment.lastAtomOrdinal
            )
        }
        self.init(transcriptVersion: transcriptVersion, lines: lines)
    }

    var isEmpty: Bool { lines.isEmpty }

    var lineCount: Int { lines.count }

    func line(_ lineRef: Int) -> Line? { lines[lineRef] }

    /// The window a coarse row addresses, described in the terms this index can
    /// check: the row's own persisted coordinates.
    struct RowWindow: Sendable, Equatable {
        let transcriptVersion: String
        let firstAtomOrdinal: Int
        let lastAtomOrdinal: Int
        let startTime: Double
        let endTime: Double
    }

    /// Resolve a coarse row's `supportLineRefs` into time spans, or `nil` when
    /// THIS INDEX CANNOT SPEAK FOR THAT ROW.
    ///
    /// `nil` is a statement about our records, never about the model's verdict:
    /// the caller must read it as "unreadable", not as "unsupported". Five ways
    /// to earn it, each a place where resolving anyway would substitute one
    /// coordinate system for another:
    ///
    ///   * the row was scanned against a different `transcriptVersion`;
    ///   * this index does not hold the line whose `firstAtomOrdinal` /
    ///     `lastAtomOrdinal` the row's window names;
    ///   * the run between those two lines is not contiguous here;
    ///   * the run's min/max times do not reproduce the row's window bounds;
    ///   * a ref lies outside the run, i.e. outside the lines the model was
    ///     actually shown for this window.
    ///
    /// Returned spans are the MAXIMAL RUNS OF CONSECUTIVE refs, one span per
    /// run. Consecutive lines are one region of speech; a ref with a gap on
    /// either side is a separate claim, and joining the two would re-mint this
    /// bead's defect at line granularity. Whether two runs then become one MARK
    /// is a question about TIME, and `mergeExtents` already answers it with
    /// `mergeGapSeconds`.
    func resolve(supportLineRefs refs: [Int], in window: RowWindow) -> [AdSpanBounds]? {
        guard !refs.isEmpty else { return nil }
        guard window.transcriptVersion == transcriptVersion else { return nil }

        // Deterministic, and `min`/`max` rather than "the first one found":
        // a dictionary has no iteration order, so `first(where:)` here would
        // make the answer depend on hashing. Segment atom ranges are disjoint,
        // so at most one line can match either predicate anyway — this is what
        // makes that a property of the code rather than of the data.
        let ordered = lines.keys.sorted()
        guard let first = ordered.first(where: {
            lines[$0]?.firstAtomOrdinal == window.firstAtomOrdinal
        }),
            let last = ordered.last(where: {
                lines[$0]?.lastAtomOrdinal == window.lastAtomOrdinal
            }),
            first <= last
        else { return nil }

        // Read ONLY to check that this index reproduces the row's window; the
        // returned geometry is built by `bounds` from the dictionary.
        var run: [Line] = []
        run.reserveCapacity(last - first + 1)
        for ref in first...last {
            guard let line = lines[ref] else { return nil }
            run.append(line)
        }
        guard let runStart = run.map(\.startTime).min(),
              let runEnd = run.map(\.endTime).max(),
              abs(runStart - window.startTime) <= Self.boundaryEpsilon,
              abs(runEnd - window.endTime) <= Self.boundaryEpsilon
        else { return nil }

        let sorted = Array(Set(refs)).sorted()
        guard let lowest = sorted.first, let highest = sorted.last,
              lowest >= first, highest <= last
        else { return nil }

        // TOTAL ON PURPOSE — it must not depend on the guards above for
        // MEMORY safety, only for correctness (playhead-shu5, mutant SU10).
        //
        // This was `run[(lower - first)...(upper - first)]`, which is sound
        // only while `lowest >= first, highest <= last` holds. Deleting that
        // guard — the obvious mutation of "a ref outside the window must not
        // resolve" — therefore did not produce a WRONG ANSWER, it produced an
        // index-out-of-range that took the test host down. A crashed host emits
        // no per-test verdict at all, so the battery scored SU07, SU08, SU09
        // AND SU10 as SURVIVED off one crash: three rails silenced by the
        // fourth. A guard doing double duty as a bounds check is a guard whose
        // removal cannot be measured.
        func bounds(from lower: Int, through upper: Int) -> AdSpanBounds? {
            let present = (lower...upper).compactMap { lines[$0] }
            guard let start = present.map(\.startTime).min(),
                  let end = present.map(\.endTime).max()
            else { return nil }
            return AdSpanBounds(start: start, end: end)
        }

        var spans: [AdSpanBounds] = []
        var runStartRef = lowest
        var previous = lowest
        for ref in sorted.dropFirst() {
            if ref == previous + 1 {
                previous = ref
                continue
            }
            if let span = bounds(from: runStartRef, through: previous) {
                spans.append(span)
            }
            runStartRef = ref
            previous = ref
        }
        if let span = bounds(from: runStartRef, through: previous) {
            spans.append(span)
        }
        return spans.isEmpty ? nil : spans
    }
}
