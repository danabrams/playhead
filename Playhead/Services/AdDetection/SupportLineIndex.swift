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
    ///
    /// `lower` MUST BE BELOW `upper`, AND THAT IS THE CALLER'S DEBT
    /// (playhead-vz3l). Handed `lower >= upper` there is no interval left to be
    /// a gap, and this reads as `start < upper && end > lower` over a reversed
    /// pair — i.e. "the span strictly CONTAINS `[upper, lower]`", a containment
    /// test wearing the name of a gap. That is not hypothetical: it is what
    /// ``SemanticSweepMarkComposer/mergeExtents(_:barredBy:)`` did for every
    /// overlapping pair, which was the MAJORITY of its merge decisions.
    ///
    /// It is deliberately NOT defended here. Returning `false` for a reversed
    /// pair would make a caller that reverses them look correct, which is
    /// precisely how the defect survived review — and it would make
    /// ``SemanticSweepMarkComposer/mergeIsBarred(from:to:by:)``'s guard
    /// unfalsifiable, so the mutant that re-creates the defect would SURVIVE
    /// and report a coverage hole that is not there. The one caller guards it
    /// and says why.
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

/// ONE named support line, and THE SECONDS THAT LINE MEANT in the segmentation
/// the model was actually shown. playhead-qjcf, schema V66.
///
/// # Why both halves travel together
///
/// A `supportLineRefs` entry is a segment INDEX. An index is only a position in
/// a coordinate system, so it names nothing at all once that coordinate system
/// is gone — which is what happens every time an episode is re-transcribed and
/// re-segmented. Measured on the 2026-08-19 t4 pull: of the 301 coarse
/// `containsAd` rows, **282 name lines, 194 of those cannot be RESOLVED, and
/// 174 come out `.unreadable`** — the 20 in between are rescued one stage
/// earlier by a declined pass-B narrowing, which needs no index. Quote 194 for
/// "refs that will not resolve" and 174 for "rows this stage cannot localise";
/// they are two populations, and the second is the one V66 is about. Those rows
/// keep their whole ~95 s scan tile
/// (``SemanticSweepMarkComposer/Localisation/unreadable``), which is how a
/// verdict about nine seconds of CTA becomes a 101 s mark over the show.
///
/// **SECONDS ALONE WOULD NOT HAVE BEEN ENOUGH, and the load-bearing reason is
/// ADJACENCY.** ``SupportLineIndex/contiguousBounds(of:)`` groups the projection
/// into maximal runs of CONSECUTIVE `lineRef`s, and two segments can abut in
/// time without being adjacent lines — a bare `[{start, end}]` array cannot
/// express the difference, so a reader would have to group on seconds and would
/// merge across a line the model did not name. That is a property no in-tree
/// writer can restore later, and mutant `QJ07` is what proves it bites.
///
/// A SECOND reason, weaker and worth naming as such:
/// ``SemanticSweepMarkComposer/persistedSupportSpans(of:)`` requires the
/// `lineRef` set here to equal the row's own `supportLineRefs` exactly, so a
/// payload that has drifted from the verdict it claims to project is refused.
/// **No in-tree writer can produce that drift** — both columns are written from
/// one `screening.support` on one `INSERT OR REPLACE`, and no `UPDATE` touches
/// either — so read it as a guard against bytes off a disk nobody controls,
/// not as a defect anybody has seen.
struct SupportLineSpan: Sendable, Codable, Equatable {
    /// The segment index the model named.
    let lineRef: Int
    /// The seconds that segment covered, in the segmentation the model saw.
    let start: Double
    let end: Double
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
        //
        // playhead-qjcf: the projection and the run-grouping used to be two
        // nested helpers here. They are ``project(supportLineRefs:)`` and
        // ``contiguousBounds(of:)`` now, because the PERSISTED-seconds reader
        // needs the second of them and a second copy of a grouping rule is how
        // two readers of one format come to disagree.
        //
        // `project`'s OWN refusal is UNREACHABLE on this path and is written
        // anyway. The `run` loop above has already proved every line in
        // `first...last` exists, and the guard above bounds `sorted` inside that
        // range, so no ref reaching here can be missing — i.e. deleting
        // `else { return nil }` is an EQUIVALENT mutation from `resolve` and
        // must be killed from ``project(supportLineRefs:)``'s own direct rails
        // or not at all. It stays because the WRITER's overload has no such
        // proof standing behind it: it is handed raw segments and refs the model
        // chose, and there the refusal is the whole safety property.
        guard let projected = project(supportLineRefs: sorted) else { return nil }
        return Self.contiguousBounds(of: projected)
    }

    // MARK: - playhead-qjcf (V66): the projection, and the ONE grouping

    /// Turn the refs the model named into the SECONDS they name, in THIS index's
    /// segmentation.
    ///
    /// REFUSES (`nil`) when any ref is unknown to the index, rather than
    /// projecting the ones it can find. A partial projection would say the model
    /// named fewer lines than it did, and a consumer would then narrow a mark on
    /// a claim nobody made. The under-claiming direction is to record nothing
    /// and let the row keep its whole window — which is what a NULL
    /// `semantic_scan_results.supportLineSpansJSON` already means.
    ///
    /// The result is deduplicated and sorted by `lineRef`, so a writer and a
    /// reader handed the same refs in different orders produce the same bytes.
    ///
    /// TWO BRANCHES HERE ARE UNREACHABLE FROM EVERY PRODUCTION CALLER and are
    /// written anyway, on the same terms as the note in `resolve`: the empty
    /// guard (both callers already guard it — `resolve` on its own first line,
    /// `BackfillJobRunner.encodeSupportLineSeconds` on `!supportLineRefs.isEmpty`)
    /// and the `projected.isEmpty` tail (`refs` is non-empty here, so the loop
    /// either appends or returns). They are stated invariants, not live paths;
    /// the QJ series deliberately does not target them, because a mutant of a
    /// provable equivalent can only ever SURVIVE and be misread as a hole.
    func project(supportLineRefs refs: [Int]) -> [SupportLineSpan]? {
        guard !refs.isEmpty else { return nil }
        var projected: [SupportLineSpan] = []
        projected.reserveCapacity(refs.count)
        for ref in Array(Set(refs)).sorted() {
            guard let line = lines[ref] else { return nil }
            projected.append(
                SupportLineSpan(lineRef: ref, start: line.startTime, end: line.endTime)
            )
        }
        return projected.isEmpty ? nil : projected
    }

    /// Project `refs` against a raw segmentation — the WRITER's spelling of
    /// ``project(supportLineRefs:)``, for a caller that holds `segments` and has
    /// no version to check them against because it IS the version.
    ///
    /// The empty `transcriptVersion` is deliberate and is never persisted: this
    /// overload exists only to reach the projection, and the resulting index is
    /// discarded. Nothing here compares versions — the writer is projecting the
    /// segmentation it just ran, so there is no other coordinate system in the
    /// room to confuse it with. That is the whole reason the persisted seconds
    /// survive a re-segmentation and the refs do not.
    static func project(
        supportLineRefs refs: [Int],
        segments: [AdTranscriptSegment]
    ) -> [SupportLineSpan]? {
        SupportLineIndex(segments: segments, transcriptVersion: "")
            .project(supportLineRefs: refs)
    }

    /// Group projected lines into the MAXIMAL RUNS OF CONSECUTIVE `lineRef`s and
    /// give each run its union in seconds.
    ///
    /// **THIS IS THE ONLY GROUPING, ON PURPOSE.** ``resolve(supportLineRefs:in:)``
    /// (which reads TODAY'S segmentation) and
    /// ``SemanticSweepMarkComposer/persistedSupportSpans(of:)`` (which reads the
    /// seconds the WRITER recorded) both end here, so the live path and the
    /// recorded path cannot drift into disagreeing about what refs `[46, 48]`
    /// mean. Before playhead-qjcf this loop lived once, inside `resolve`; a
    /// second copy for the persisted payload is exactly how two readers of one
    /// format come to disagree.
    ///
    /// Consecutiveness is decided on the REF, never on the seconds: two segments
    /// can abut in time without being adjacent lines, and merging those would
    /// widen a mark across audio the model did not name. Whether two runs then
    /// become one MARK is a question about TIME, and `mergeExtents` answers it
    /// with `mergeGapSeconds`.
    static func contiguousBounds(of projected: [SupportLineSpan]) -> [AdSpanBounds]? {
        let sorted = projected.sorted { $0.lineRef < $1.lineRef }
        guard let firstSpan = sorted.first else { return nil }

        var spans: [AdSpanBounds] = []
        var runStart = firstSpan.start
        var runEnd = firstSpan.end
        var previousRef = firstSpan.lineRef
        for span in sorted.dropFirst() {
            if span.lineRef == previousRef + 1 {
                runStart = Swift.min(runStart, span.start)
                runEnd = Swift.max(runEnd, span.end)
                previousRef = span.lineRef
                continue
            }
            spans.append(AdSpanBounds(start: runStart, end: runEnd))
            runStart = span.start
            runEnd = span.end
            previousRef = span.lineRef
        }
        spans.append(AdSpanBounds(start: runStart, end: runEnd))
        // Unreachable: the append one line up always runs. Stated rather than
        // live, for `project`'s reason — do not "fix" it into a real path.
        return spans.isEmpty ? nil : spans
    }

    /// Encode a projection for `semantic_scan_results.supportLineSpansJSON`.
    ///
    /// `.sortedKeys` so the column is stable BYTES: a row rewritten by a later
    /// attempt carrying the same verdict must not read as changed to anything
    /// diffing two device pulls.
    ///
    /// Returns `nil` — i.e. WRITE NOTHING — for an empty projection or an
    /// encoder failure. A column that says "the writer recorded no seconds" is
    /// true in both cases, and `"[]"` would be a FOURTH spelling of an absence
    /// this neighbourhood already has three of (`spansJSON == "[]"`, an empty
    /// `supportLineRefs`, an undecodable payload) — the exact ambiguity
    /// ``SemanticSweepMarkComposer/supportLineRefs(of:)`` has to spend a
    /// paragraph disentangling.
    static func encodeSupportLineSpans(_ spans: [SupportLineSpan]) -> String? {
        guard !spans.isEmpty else { return nil }
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        guard let data = try? encoder.encode(spans),
              let text = String(data: data, encoding: .utf8)
        else { return nil }
        return text
    }

    /// Decode `semantic_scan_results.supportLineSpansJSON`.
    ///
    /// `nil` for a NULL column, for a payload that will not decode, and for an
    /// empty array — all three are "this row records no seconds", which is what
    /// every pre-V66 row on disk is and what a caller must handle anyway.
    ///
    /// GEOMETRY IS NOT VALIDATED HERE, deliberately: whether a span is finite,
    /// whether it lies inside the row's own window, and whether its refs are the
    /// refs the row's VERDICT named are all properties of the ROW, which this
    /// type does not hold. ``SemanticSweepMarkComposer/persistedSupportSpans(of:)``
    /// owns every one of them, and a reader that took this function's non-nil as
    /// a licence would be trusting bytes nobody had checked against a claim.
    static func decodeSupportLineSpans(_ json: String?) -> [SupportLineSpan]? {
        guard let json, let data = json.data(using: .utf8),
              let spans = try? JSONDecoder().decode([SupportLineSpan].self, from: data),
              !spans.isEmpty
        else { return nil }
        return spans
    }
}
