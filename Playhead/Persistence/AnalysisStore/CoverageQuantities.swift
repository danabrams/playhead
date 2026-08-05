// CoverageQuantities.swift
// playhead-x0lb: distinct types for the coverage/reach quantities that share a
// unit and keep being read as one another.
//
// WHY THIS FILE EXISTS. Eighteen instances of one defect across six beads in a
// single queue (2026-08-03/04), every one the same shape: two quantities share a
// unit — seconds, a ratio — and one is read as though it were the other. The
// compiler could not object, because both were `Double`. Review caught them one
// at a time, and three of the last five were introduced BY the fix for a previous
// one. A lexical canary was tried on one sub-family and was rewritten in five
// consecutive rounds, each finding a hole the last one missed. A pattern can
// always be out-spelled; a type cannot.
//
// The cost is zero at runtime: every type here is a frozen single-field struct
// over `Double`, which the optimiser lays out identically to the `Double` it
// replaces. The cost at the keyboard is one `.rawValue` at each genuine boundary,
// and that is the point — a boundary you have to name is a boundary a reviewer
// can find.
//
// WHAT IS DELIBERATELY NOT HERE. Literal expressibility IS provided
// (`ExpressibleByFloatLiteral` / `ExpressibleByIntegerLiteral`), because a
// literal has no provenance to lose: `0.98` is not a measurement of anything, so
// admitting it substitutes nothing. What is refused is passing a `Double`
// VARIABLE — the value that came from somewhere — and that is where every
// instance in the catalogue lived.

import Foundation

// MARK: - The shared shape

/// playhead-x0lb: a scalar coverage quantity carried as a distinct type so it
/// cannot be substituted for a lookalike that happens to share its unit.
///
/// Conformers are `Comparable` **only against themselves**. That is the whole
/// mechanism: `episodeCursor > planListBound` does not compile, and neither does
/// `reachRatio < densityRatio`.
protocol CoverageQuantity:
    RawRepresentable,
    Codable,
    Sendable,
    Hashable,
    Comparable,
    CustomStringConvertible,
    ExpressibleByFloatLiteral,
    ExpressibleByIntegerLiteral
where RawValue == Double {
    init(_ rawValue: Double)
}

extension CoverageQuantity {

    init(rawValue: Double) { self.init(rawValue) }

    /// Single-value `Codable`, NOT the synthesised keyed form.
    ///
    /// Load-bearing for `BackfillProgressCursor`: the persisted rows on the
    /// 2026-08-03 device pull carry `{"processedUnitCount":1,
    /// "lastProcessedUpperBoundSec":2525.82}` — a BARE NUMBER. Synthesised
    /// `Codable` on a `RawRepresentable` struct would write
    /// `{"rawValue":2525.82}` and silently orphan every cursor in the database.
    /// `BackfillProgressCursorTests` pins the wire form against literal JSON.
    init(from decoder: Decoder) throws {
        self.init(try decoder.singleValueContainer().decode(Double.self))
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }

    init(floatLiteral value: Double) { self.init(value) }
    init(integerLiteral value: Int) { self.init(Double(value)) }

    static func < (lhs: Self, rhs: Self) -> Bool { lhs.rawValue < rhs.rawValue }

    /// The DELTA between two quantities of the same kind, as a plain `Double`.
    ///
    /// It returns a `Double` rather than `Self` deliberately: a difference of
    /// two positions is not a position, and a difference of two ratios is not a
    /// ratio. This is the one arithmetic affordance the types offer, and it is
    /// the one that cannot launder anything — the operands must already be the
    /// same kind for it to type-check at all.
    static func - (lhs: Self, rhs: Self) -> Double { lhs.rawValue - rhs.rawValue }

    var description: String { String(rawValue) }

    /// `Double.isFinite`, forwarded so a caller never has to unwrap the raw
    /// value merely to ask whether it is a number. Every guard in the coverage
    /// path asks this.
    var isFinite: Bool { rawValue.isFinite }
}

extension Optional where Wrapped: CoverageQuantity {
    /// The value when it is present AND finite; `nil` otherwise.
    ///
    /// Absence and non-finiteness are the same fact to every consumer in this
    /// path — "there is no number here" — and writing that as one expression
    /// stops the two from drifting apart. playhead-41mu R1 is the instance: a
    /// non-finite `.measured` fraction returned `.complete` at one consumer
    /// while the other three read the same absence as "not read".
    var finiteValue: Wrapped? {
        guard let self, self.isFinite else { return nil }
        return self
    }
}

// MARK: - Positions on a timeline

/// playhead-x0lb: a POSITION on the episode's own timeline, in seconds from the
/// first audio sample.
///
/// * numerator / denominator: none — this is a position, not a ratio.
/// * unit: seconds of episode audio.
///
/// **The claim it makes.** As a resume cursor, a value of `x` asserts that
/// `[0, x]` OF THE EPISODE has been covered, because that is exactly how
/// `BackfillJobRunner.narrowedForResume` reads it: every segment ending at or
/// below `x` is dropped from the next attempt, forever.
///
/// It is NOT interchangeable with ``PlanListSeconds``, and the whole cursor
/// family of this bead's catalogue is that substitution: 53FC53E3's row carries
/// `2525.82` on a 2,528 s episode — the end of the segment list its job was
/// handed, read as how far through the EPISODE it had got. Verified against the
/// 2026-08-03 pull: that job's measured `adScanFraction` is 0.0142.
struct EpisodeSeconds: CoverageQuantity {
    let rawValue: Double
    init(_ rawValue: Double) { self.rawValue = rawValue }
}

/// playhead-x0lb: a POSITION whose claim is bounded by a HANDED-OVER LIST —
/// the segments a job was dispatched with, or the plans `planPassA` partitioned
/// them into — not by the episode.
///
/// * numerator / denominator: none — a position.
/// * unit: seconds of episode audio (the same absolute timebase as
///   ``EpisodeSeconds``; the difference is not the coordinate system, it is the
///   CLAIM).
///
/// **What it does and does not say.** A value of `x` asserts that the run
/// covered its list up to `x`. It says nothing whatever about `[0, listStart]`,
/// and `listStart` is whatever transcript existed when the job was dispatched —
/// which the final-pass hook makes early in a transcript's life. On the
/// 2026-08-03 pull, 53FC53E3's job was handed segments starting at 2,490 s of a
/// 2,528 s episode, so its plan-list bound of 2,525.82 covered 1.4 % of the
/// audio while looking exactly like a completed episode.
///
/// **Promotion is not free.** ``EpisodeSeconds/promoting(_:priorEpisodeCursor:firstPlannedStart:bridge:)``
/// is the only sound conversion and it demands the evidence. Sites that are
/// known to promote UNSOUNDLY go through
/// ``EpisodeSeconds/unsoundPlanListPromotion(_:site:)`` and are enumerated by
/// ``UnsoundCursorPromotionSite`` — see playhead-5pyq.
struct PlanListSeconds: CoverageQuantity {
    let rawValue: Double
    init(_ rawValue: Double) { self.rawValue = rawValue }
}

/// playhead-x0lb: the TRANSCRIPT's high-water mark — `MAX(endTime)` over
/// `transcript_chunks`, or the asset watermark column standing in for it. A
/// position, never an area.
///
/// * numerator / denominator: none — a position.
/// * unit: seconds of episode audio.
///
/// **Why it is its own type.** A watermark and an area are the same number only
/// on a gapless transcript, and real transcripts are not gapless: a
/// `transcript_chunks` row spans first word to last word, so the raw union is
/// riddled with inter-utterance holes. On the 2026-08-03 pull AD5F3A0A's fast
/// watermark is 4,280.7 s while its fast AREA is 1,645.9 s — the watermark
/// over-reports by 2.6x. Reading one as the other is the
/// "AN 100 % / TX 39 %" antipattern (playhead-sd71).
///
/// R2 review: that witness is 4,280.**7** — `MAX(endTime)` over the asset's
/// fast `transcript_chunks`, which is what this type carries when any chunk
/// landed. An earlier draft said 4,280.9, which is the
/// `analysis_assets.fastTranscriptCoverageEndTime` COLUMN (4,280.8947, and it
/// does NOT stand in here because chunks exist) rounded — and is also within
/// 0.03 s of the DECLARED duration (4,280.9208). Three near-identical numbers
/// in one unit, and the sentence named the wrong one; the 2.6x is unaffected
/// (4,280.70 ÷ 1,645.92 = 2.60).
///
/// **It is the TRANSCRIPT's reach and no other producer's** (R1 review). The
/// DSP feature frontier and `max(endTime)` of DETECTED ads are also high-water
/// marks in the same unit, and instance 9 of this bead's catalogue is exactly
/// those two being read as this one — the readiness ✓ keyed on the DSP
/// watermark and the detected-ad maximum, so an episode where detection did
/// WORSE read as MORE complete. They carry ``FrontierSeconds``; substituting
/// one for the other was a compile-clean edit until R1 planted it and watched
/// it build.
///
/// A `ReachRatio` cannot be built from one of these; that is the point.
struct WatermarkSeconds: CoverageQuantity {
    let rawValue: Double
    init(_ rawValue: Double) { self.rawValue = rawValue }

    /// playhead-x0lb R4: only a REACH past the declared end can disprove a
    /// declared duration.
    ///
    /// **This predicate exists because the property was asserted in PROSE where
    /// it needed a type.** The comment on `adScanFraction`'s guard said, and
    /// still says, that "an AREA can never disprove a duration (a gappy
    /// transcript's area is legitimately small); only a reach PAST the declared
    /// end can" — and R4 probe PA3 then wrote `adScanCoveredSec.rawValue >
    /// episodeDurationSec.rawValue + tolerance` into that exact guard and it
    /// COMPILED. A sentence forbidding a substitution, next to an expression
    /// that permits it, is the shape this whole bead is a reaction to. Making
    /// the receiver a ``WatermarkSeconds`` is what turns the sentence into a
    /// diagnostic. Rail TY23.
    func reaches(past duration: EpisodeSeconds, byMoreThan tolerance: Double) -> Bool {
        rawValue > duration.rawValue + tolerance
    }
}

/// playhead-x0lb: the ANALYSIS FRONTIER — how far the acoustic/DSP pipeline has
/// swept, or how far the latest DETECTED ad window ends. A position, never an
/// area, and never the transcript's reach.
///
/// * numerator / denominator: none — a position.
/// * unit: seconds of episode audio.
///
/// **R1 review: separated from ``WatermarkSeconds`` because instance 9 IS this
/// substitution.** Feature extraction sweeps the whole episode independently of
/// the semantic scan, so it reaches 100 % while most audio has never been
/// screened; and `confirmedAdCoverageEndSec` is not coverage at all — one
/// late-placed detection pushes it to the end of the episode. Both are
/// legitimately `max`ed with EACH OTHER to form the frontier that clips
/// ``AnalyzedSeconds``, and that is the only arithmetic they take part in.
struct FrontierSeconds: CoverageQuantity {
    let rawValue: Double
    init(_ rawValue: Double) { self.rawValue = rawValue }
}

/// playhead-x0lb: the TRANSCRIPT AREA — de-overlapped seconds of audio that
/// carry transcript text. Never a position.
///
/// * numerator / denominator: none — an area, and ``DensityRatio``'s numerator.
/// * unit: seconds of episode audio.
///
/// Overlapping spans count once and gaps are excluded, which is what makes it
/// the honest numerator for a coverage ratio and what makes it strictly smaller
/// than the corresponding ``WatermarkSeconds`` whenever the transcript has a
/// hole.
///
/// **R1 review: it is the transcript's area and no other lane's.** Three areas
/// live on `AnalysisCoverageSummary` — this one, ``AnalyzedSeconds`` and
/// ``AdScanSeconds`` — and until R1 they shared this type, so
/// `ReachRatio(examined: fastTranscriptCoveredSec, …)` compiled. That
/// expression IS playhead-fil5 R3: the transcript union over the duration,
/// published as ad-scan reach, with nought of twelve field assets clearing the
/// gate and every test green.
struct CoveredSeconds: CoverageQuantity {
    let rawValue: Double
    init(_ rawValue: Double) { self.rawValue = rawValue }
}

/// playhead-x0lb: the ANALYZED AREA — the transcript union CLIPPED to the
/// analysis frontier. An area, and a subset of ``CoveredSeconds``.
///
/// * numerator / denominator: none — an area.
/// * unit: seconds of episode audio.
///
/// It is not ad-scan reach and never was: it inherits the frontier's problems
/// in a gap-aware form (see ``FrontierSeconds``), so an episode whose DSP sweep
/// finished but whose semantic scan never ran carries a large one. R1 separated
/// it from ``AdScanSeconds`` after planting the substitution and watching it
/// build.
struct AnalyzedSeconds: CoverageQuantity {
    let rawValue: Double
    init(_ rawValue: Double) { self.rawValue = rawValue }

    /// playhead-x0lb R3: the ONLY way this area is produced — a transcribed
    /// region CLIPPED to the analysis frontier, with both operands named.
    ///
    /// **Why it exists.** `AnalysisCoverageMath.unionedSecondsClipped` is
    /// deliberately generic interval arithmetic (see the audit), so its
    /// `upperBound` is a bare `Double`, and the store handed it
    /// `frontier.rawValue`. R3 probe PC2 handed it the TRANSCRIPT's own
    /// watermark instead and it COMPILED — which would make AN identical to TX
    /// on every episode, the "AN 100 % / TX 39 %" family (playhead-sd71) in the
    /// one direction nobody would notice, because the two bars would simply
    /// agree. The helper stays generic; the BOXING is what carries the type,
    /// which is exactly where rails TY09/TY10 already decide which area a call
    /// produces. Rail TY20.
    /// R5 (Dan's scope expansion): the INTERVALS are typed too, and they are the
    /// FAST region specifically. R3 typed the bound and said so; R4 probe PA9
    /// then widened what is clipped to both passes and it compiled, because
    /// "TY20 pins which BOUND clips, not what is clipped". `analysisCoveredSec`
    /// is documented as the fast-transcript area clipped to the frontier, and
    /// playhead-9y9e deliberately did NOT widen it when it widened the ad-scan
    /// bound — so the fast region is the contract, not an accident of what was
    /// in scope. Rail TY29.
    init(clipping transcribed: FastTranscriptRegion, to frontier: FrontierSeconds) {
        self.init(AnalysisCoverageMath.unionedSecondsClipped(
            transcribed.intervals,
            upperBound: frontier.rawValue
        ))
    }

    /// playhead-x0lb R4: the Activity AN bar's fill — THIS area over the
    /// DECLARED duration — with both terms named and the division written once.
    ///
    /// **Why the two-argument helper was not enough.** R3 typed both terms of
    /// `ActivitySnapshotProvider`'s `fraction(area:ofDeclaredDuration:)`, which
    /// stops the wrong quantity ARRIVING. It does not stop the two that arrive
    /// correctly from being divided the wrong way round: inside the helper both
    /// are `Double`, and R4 probes PB1 and PB2 wrote `duration.rawValue /
    /// area.rawValue` at each of the two copies and both COMPILED. A reciprocal
    /// is dimensionally as wrong as a substitution and it renders in the same
    /// `[0, 1]` bar — on any episode whose analyzed area is under its duration
    /// it simply clamps to 1.0, i.e. a full bar on an episode nothing analyzed.
    ///
    /// This is the same answer ``ReachRatio/init(examined:ofDeclaredDuration:)``
    /// gives: the division happens ONCE, in a method whose receiver is the
    /// numerator, so there is no expression left to invert. It also collapses
    /// the two duplicated copies into one. Rails TY24 / TY25.
    func fractionOfDeclaredDuration(_ duration: EpisodeSeconds?) -> Double? {
        guard let duration, duration.rawValue > 0 else { return nil }
        return min(1.0, max(0.0, rawValue / duration.rawValue))
    }
}

/// playhead-x0lb: the AD-SCAN AREA — audio a semantic ad scan actually READ.
/// The only quantity that answers "how much of this episode has been read for
/// ads?", and ``ReachRatio``'s only numerator.
///
/// * numerator / denominator: none — an area.
/// * unit: seconds of episode audio.
///
/// Precisely: the interval union of coverage-lane `semantic_scan_results`
/// windows that produced a verdict, INTERSECTED with the transcribed region
/// (both passes, sub-``BridgeToleranceSec`` gaps bridged).
///
/// **R1 review: its own type, because the summary's own doc already had to say
/// in prose that it "is deliberately NOT any of the other scalars on this
/// summary" — and prose is what this bead exists to replace.** Every one of the
/// alternatives it warns about type-checked as this quantity until R1.
struct AdScanSeconds: CoverageQuantity {
    let rawValue: Double
    init(_ rawValue: Double) { self.rawValue = rawValue }

    /// playhead-x0lb R5: the ONLY way this area is produced — the scan windows
    /// INTERSECTED with the readable region, whose sub-ad-width gaps are bridged
    /// as part of the same operation.
    ///
    /// **Three operands, three types, one call.** The expression this replaces
    /// was `AdScanSeconds(unionedSecondsIntersecting(adScanIntervals[id] ?? [],
    /// within: bridgingShortGaps(transcribedIntervals, upTo: …)))`, and every one
    /// of its three inputs was substitutable: probe PA7 measured the area from
    /// the FAST TRANSCRIPT instead of the scan windows (playhead-fil5 R3's P0 one
    /// layer below every rail that looks for it), probe PA8 narrowed the bound to
    /// the fast pass (playhead-9y9e verbatim), and the tolerance is instance 19
    /// pre-loaded (playhead-a1x0's 60 s re-scan threshold beside this 5 s
    /// measuring tolerance). Rails TY27 / TY28 / TY07.
    ///
    /// **The bridging is INSIDE, deliberately.** R4's lesson is that a typed
    /// pair is not a typed operation: typing the operands stops the wrong
    /// quantity arriving and does nothing once it has. An unbridged bound is not
    /// a different TYPE, it is the same type with a step missed — and a missed
    /// bridge caps the ad-scan fraction below the 0.98 readiness floor on every
    /// real episode, which is an under-claim that deletes the signal. Doing it
    /// here means there is no unbridged bound in scope to pass.
    init(
        examined scanned: ScannedRegion,
        within readable: TranscribedRegion,
        bridging tolerance: BridgeToleranceSec
    ) {
        self.init(AnalysisCoverageMath.unionedSecondsIntersecting(
            scanned.intervals,
            within: AnalysisCoverageMath.bridgingShortGaps(
                readable.intervals,
                upTo: tolerance
            )
        ))
    }

    /// playhead-x0lb R4: an AREA that exceeds the whole declared duration is
    /// proof the two describe different audio.
    ///
    /// **Why it is a named predicate and not a raw comparison.** The guard in
    /// `adScanFraction` read `adScanCoveredSec.rawValue <= duration.rawValue +
    /// tolerance` inline, and R4 probe PA2 drove it from
    /// `fastTranscriptCoveredSec` — the TRANSCRIPT area, which is not the
    /// numerator this guard is protecting — with no complaint. Once both sides
    /// are `Double` the comparison accepts every area on the summary, so the
    /// sanity check that is supposed to catch a numerator describing different
    /// audio can be pointed at an area that is not the numerator at all. Rail
    /// TY22.
    func exceeds(_ duration: EpisodeSeconds, byMoreThan tolerance: Double) -> Bool {
        rawValue > duration.rawValue + tolerance
    }
}

// MARK: - Regions: the INTERVAL SETS the areas above are measured from

// playhead-x0lb R5 (Dan's 2026-08-05 decision to EXPAND scope): the scalars above
// carry types; the interval arrays they are measured FROM did not, and that is
// one layer below every rail in this bead.
//
// WHAT R4 MEASURED. Four `[(start: Double, end: Double)]` arrays are live in one
// scope inside `AnalysisStore.fetchCoverageSummariesByAssetIds` — the fast pass,
// the final pass, the fast∪final readable region, and the scan windows — and any
// of them fitted any slot. Four probes were planted and all four COMPILED:
//
//   PA5  the transcript region built from the DSP FRONTIER instead of the
//        transcript's own area, which poisons AN and the ad-scan bound at once;
//   PA7  the ad-scan area measured from the FAST TRANSCRIPT instead of the scan
//        windows — playhead-fil5 R3's P0 (the transcript published as ad-scan
//        reach) reproduced ONE LAYER BELOW every rail that looks for it, with
//        ``AdScanSeconds`` intact on the box;
//   PA8  the ad-scan bound narrowed from the readable region to the FAST pass
//        alone — playhead-9y9e's defect verbatim, worth 55.4 % vs 100.0 % on
//        0C2FC22E;
//   PA9  the AN clip's intervals WIDENED to both passes, which TY20 does not
//        pin: TY20 pins which BOUND clips, not what is clipped.
//
// Re-planted against this tree on 2026-08-05 before any fix, through the same
// harness the rails use: **PA5 PA7 PA8 PA9, four probes, four COMPILED.** The
// types below are what make them TY26–TY29.
//
// THE SHAPE, and it is the one R4 found works: every region keeps its intervals
// `fileprivate` and offers only NAMED operations, so a consumer cannot reach the
// raw array to substitute it. `AnalysisCoverageMath` stays generic — it is
// honest interval arithmetic and its tests hit it directly — and the REGION
// types are the door it is reached through from the coverage reader.
//
// WHAT THIS DOES NOT CLOSE, measured not assumed: each region is still BUILT
// from bare `Double` columns at the SQL read (`append(start:end:)`), which is
// door 2 (limit L-I) one level down. That door cannot be closed here because
// `sqlite3_column_double` returns a `Double` and nothing else; what changes is
// that there is now exactly ONE such door per region and it sits at the genuine
// boundary rather than at every consumer.

/// playhead-x0lb: the FAST-PASS transcript region — the intervals of
/// `transcript_chunks` rows with `pass = 'fast'`, or the asset watermark modelled
/// as one contiguous `[0, watermark]` span when no fast chunk landed.
///
/// * unit: half-open `[start, end)` intervals of episode audio.
/// * measures to: ``CoveredSeconds`` (its own area) and, clipped to the DSP
///   frontier, ``AnalyzedSeconds``.
///
/// It is NOT the region a semantic scan can read — that is ``TranscribedRegion``,
/// which adds the final pass — and it is not what a scan examined, which is
/// ``ScannedRegion``. Probes PA8 and PA9 are exactly those two confusions and
/// both compiled while all three were `[(start: Double, end: Double)]`.
struct FastTranscriptRegion {
    fileprivate var intervals: [(start: Double, end: Double)] = []

    /// An empty region — no fast chunk landed and no watermark stands in.
    init() {}

    /// playhead-x0lb R5: the watermark stand-in, and the ONE promotion into this
    /// region that is not a raw SQL column.
    ///
    /// The parameter is a ``CoveredSeconds`` and not a `Double` because probe PA5
    /// built this span from ``FrontierSeconds`` — the DSP sweep, which reaches
    /// 100 % on an episode nothing transcribed — and it compiled. Rail TY26.
    init(spanningFromZeroTo covered: CoveredSeconds) {
        self.intervals = [(start: 0, end: covered.rawValue)]
    }

    /// Accumulate one row. `start`/`end` are bare `Double`s because they come
    /// straight off `sqlite3_column_double`; see the note above on limit L-I.
    mutating func append(start: Double, end: Double) {
        intervals.append((start: start, end: end))
    }

    var isEmpty: Bool { intervals.isEmpty }

    /// The de-overlapped AREA of this region, typed as the transcript's area.
    ///
    /// playhead-x0lb R5: this replaces a bare
    /// `CoveredSeconds(AnalysisCoverageMath.unionedSeconds(fastIntervals[id] ?? []))`
    /// in the coverage reader — a boxing site (limit L-I) where the array was
    /// chosen by hand and the box named a quantity nothing checked it against.
    var unionedSeconds: CoveredSeconds {
        CoveredSeconds(AnalysisCoverageMath.unionedSeconds(intervals))
    }
}

/// playhead-x0lb: the FINAL-PASS transcript region — `transcript_chunks` rows
/// with `pass = 'final'`.
///
/// * unit: half-open `[start, end)` intervals of episode audio.
///
/// It has exactly one consumer: it is the other half of ``TranscribedRegion``.
/// It is a distinct type from ``FastTranscriptRegion`` rather than a second value
/// of it because the coverage reader holds both at once, and a same-type pair is
/// indistinguishable by construction (limit L-J) — the fast/final swap would be
/// unwritable in neither direction if they shared a type.
struct FinalTranscriptRegion {
    fileprivate var intervals: [(start: Double, end: Double)] = []

    init() {}

    /// Accumulate one row; see ``FastTranscriptRegion/append(start:end:)``.
    mutating func append(start: Double, end: Double) {
        intervals.append((start: start, end: end))
    }
}

/// playhead-x0lb: the TRANSCRIBED REGION — the audio a semantic scan can
/// actually READ, which is the whole transcript, BOTH passes.
///
/// * unit: half-open `[start, end)` intervals of episode audio.
/// * measures to: nothing on its own — it is the BOUND that
///   ``AdScanSeconds/init(examined:within:bridging:)`` intersects against.
///
/// **Why it is not ``FastTranscriptRegion``.** playhead-9y9e: the scan is planned
/// over the CANONICAL chunk stream, where a final-pass chunk replaces the fast
/// coverage it fully contains and every other chunk of either pass is retained —
/// so the canonical union is exactly `union(fast) ∪ union(final)`. Bounding the
/// ad-scan area by the fast union alone discarded audio the scan genuinely read:
/// 0C2FC22E measured 55.4 % fast against 100.0 % canonical on the 2026-08-03
/// device pull. R4 probe PA8 wrote the narrow one back into the bound and it
/// compiled. Rail TY28.
struct TranscribedRegion {
    fileprivate var intervals: [(start: Double, end: Double)]

    /// The only constructor, and it names both passes.
    ///
    /// **The fast term is not redundant and cannot be dropped.** When no fast
    /// chunk landed but the asset carries a `fastTranscriptCoverageEndTime`, the
    /// fast region is the watermark modelled as one contiguous span, and the
    /// final-pass intervals alone are neither contiguous nor guaranteed to reach
    /// it — so a bound built from the final pass alone would be SMALLER than the
    /// bound this replaced and an episode could measure less scanned than before.
    /// Adding rather than replacing makes the widening MONOTONE.
    init(fastPass: FastTranscriptRegion, finalPass: FinalTranscriptRegion) {
        self.intervals = fastPass.intervals + finalPass.intervals
    }
}

/// playhead-x0lb: the SCANNED REGION — the `[windowStartTime, windowEndTime]`
/// spans of coverage-lane `semantic_scan_results` rows that produced a verdict.
///
/// * unit: half-open `[start, end)` intervals of episode audio.
/// * measures to: ``AdScanSeconds``, and only after intersection with a
///   ``TranscribedRegion``.
///
/// **It is never taken at face value.** A window's persisted bounds are
/// `first.startTime ... last.endTime` over the segments that fit one prompt, so
/// where the transcript has a hole between two consecutive segments the bounds
/// straddle audio whose text never entered the prompt. That is why this type has
/// no area accessor of its own: the only measurement it offers goes through
/// ``AdScanSeconds/init(examined:within:bridging:)``, which requires the bound.
struct ScannedRegion {
    fileprivate var intervals: [(start: Double, end: Double)] = []

    init() {}

    /// Accumulate one examined window; see ``FastTranscriptRegion/append(start:end:)``.
    mutating func append(start: Double, end: Double) {
        intervals.append((start: start, end: end))
    }
}

// MARK: - Ratios, each naming both of its terms

/// playhead-x0lb: **REACH** — how much of the episode a semantic ad scan has
/// actually READ.
///
/// * numerator: ``AdScanSeconds`` — the interval union of coverage-lane
///   `semantic_scan_results` windows that produced a verdict, INTERSECTED with
///   the transcribed region (both passes, sub-ad-width gaps bridged).
/// * denominator: ``EpisodeSeconds`` — the episode's DECLARED duration
///   (`analysis_assets.episodeDurationSec`).
/// * range: `[0, 1]`, clamped. `nil` when not honestly measurable.
///
/// **It is not ``DensityRatio``, and the two coincide on exactly the fixtures a
/// test suite writes.** playhead-fil5 R3 divided the raw transcript interval
/// union by the duration and called the result reach. That is density. Nought of
/// twelve field assets cleared the gate and every test passed, because every
/// fixture was single-chunk — the one shape where the two are equal.
///
/// R1 review: the numerator is ``AdScanSeconds`` and not the shared area type,
/// because with a shared area type fil5 R3's expression still compiled — the
/// defect had two spellings and only the one that goes through
/// ``AnalysisCoverageSummary/transcriptDensity`` was closed.
struct ReachRatio: CoverageQuantity {
    let rawValue: Double
    init(_ rawValue: Double) { self.rawValue = rawValue }

    /// The only constructor that states both terms. Returns `nil` rather than a
    /// number whenever the division would be dishonest; the caller decides what
    /// an absent measurement means, and in this codebase three of the four
    /// consumers read it as "not read".
    ///
    /// Guards, in order: a non-finite or negative numerator; a missing,
    /// non-finite or non-positive denominator.
    init?(examined: AdScanSeconds?, ofDeclaredDuration duration: EpisodeSeconds?) {
        guard let examined, examined.isFinite, examined.rawValue >= 0,
              let duration, duration.isFinite, duration.rawValue > 0 else {
            return nil
        }
        self.init(min(1, examined.rawValue / duration.rawValue))
    }
}

/// playhead-x0lb: **DENSITY** — how much of the episode carries transcript text.
///
/// * numerator: ``CoveredSeconds`` — the interval union of `transcript_chunks`
///   spans.
/// * denominator: ``EpisodeSeconds`` — the episode's DECLARED duration.
/// * range: `[0, 1]`, clamped.
///
/// **A high density says nothing about ad-scan reach.** On the 2026-08-03 pull
/// 4FF3A238 has a fast density of 0.892 and NO coverage-lane row at all, so its
/// ``ReachRatio`` is absent — not 0.892, and not 0. Nine of the twelve assets
/// are in that position.
struct DensityRatio: CoverageQuantity {
    let rawValue: Double
    init(_ rawValue: Double) { self.rawValue = rawValue }

    /// Both terms named, same guard order as ``ReachRatio``.
    init?(transcribed: CoveredSeconds?, ofDeclaredDuration duration: EpisodeSeconds?) {
        guard let transcribed, transcribed.isFinite, transcribed.rawValue >= 0,
              let duration, duration.isFinite, duration.rawValue > 0 else {
            return nil
        }
        self.init(min(1, transcribed.rawValue / duration.rawValue))
    }
}

// MARK: - Gap widths: two questions that are not the same question

/// playhead-x0lb / playhead-pz32: a hole small enough to BRIDGE **when
/// measuring** — "is this gap too small to have hidden an ad from a scan that
/// ran?".
///
/// * numerator / denominator: none — a width.
/// * unit: seconds of episode audio.
///
/// Distinct from ``RescanThresholdSec`` by TYPE and not merely by name, at Dan's
/// explicit instruction on playhead-a1x0: *"Two names, or the next reviewer
/// finds the seventeenth instance of the family here."* The two constants are
/// 5 s and 60 s, they are both gap widths in seconds, and they answer opposite
/// questions.
struct BridgeToleranceSec: CoverageQuantity {
    let rawValue: Double
    init(_ rawValue: Double) { self.rawValue = rawValue }

    /// Does a gap of `gapSec` disappear when measuring?
    ///
    /// A named predicate rather than a raw comparison, because
    /// `gap > tolerance.rawValue` re-opens exactly the hole this type closes:
    /// the moment the width is back to a `Double` it can be compared against
    /// the other threshold again.
    func bridges(gapSec: Double) -> Bool { gapSec <= rawValue }
}

/// playhead-x0lb / playhead-a1x0: a hole big enough to be worth **RE-SCANNING**
/// — "is this gap wide enough to hold an ad nobody read?".
///
/// * numerator / denominator: none — a width.
/// * unit: seconds of episode audio.
///
/// **INERT until playhead-a1x0 lands.** ``adScanRescanWorthyGapSec`` is
/// declared, documented and typed here so that the constant a1x0 introduces
/// cannot be confused with ``AnalysisCoverageMath/adScanBridgeableGapSec`` at
/// the moment it is introduced — which is instance 19 of this bead's catalogue,
/// pre-loaded. Nothing in the runtime reads it yet; wiring the rule to interior
/// and tail holes is a1x0's job.
struct RescanThresholdSec: CoverageQuantity {
    let rawValue: Double
    init(_ rawValue: Double) { self.rawValue = rawValue }

    /// Is a gap of `gapSec` worth paying FM wall-clock to re-scan?
    func warrantsRescan(gapSec: Double) -> Bool { gapSec > rawValue }

    /// playhead-a1x0, Dan's decision of 2026-08-04. **60 s, measured, not
    /// chosen.**
    ///
    /// Re-derived independently against the 2026-08-03 pull for this bead
    /// (n = 668 fast-pass gaps wider than 1 s across all 12 assets):
    /// p50 1.4 s, p75 1.9 s, p90 3.3 s, p95 4.5 s, p99 15.7 s, max 2,370.4 s.
    /// The distribution is bimodal with an EMPTY BAND — the widest breath gap on
    /// the library is DE0784D8's 24.3 s and the narrowest structural hole is
    /// 83592353's 150.2 s, and no gap anywhere falls between them. 60 s is the
    /// geometric mean of that empty band, `sqrt(24 * 150) = 60.0`: 2.5x above
    /// the widest breath gap and 2.5x below the narrowest real hole, symmetric
    /// in log space.
    ///
    /// Re-measure the band on the next device pull before trusting it again —
    /// an empty band across 12 episodes is exactly the kind of fact a bigger
    /// library fills in.
    static let adScanRescanWorthyGapSec = RescanThresholdSec(60.0)
}

// MARK: - Promotion: the only way across the PlanList/Episode boundary

/// playhead-x0lb: every site that publishes a LIST-relative bound as an EPISODE
/// cursor without establishing the run began at the head.
///
/// This enum exists so the unsound promotions are ENUMERABLE rather than
/// spelled. playhead-hc7e's *"Every consumer now reads `canonicalChunks`"* was
/// a completeness claim that was not complete and hid a P0 for months; a claim
/// that is a `CaseIterable` enum can be checked by a test instead of believed.
///
/// **What that check actually proves, stated exactly** (R2 review, because the
/// earlier wording overclaimed it). `CoverageQuantitiesTests` reads
/// `allCases` and so proves a property of the ENUM, not of the SOURCE — a
/// tautology on its own. What ties the enum to the sites is the
/// `--check-inventory` preflight in `scripts/mutation-battery-untypeable.py`:
/// every case must be written at exactly one `site:` argument, the number of
/// `unsoundPlanListPromotion(` calls must equal the number of cases, and
/// `BackfillJobRunner.swift` must contain no bare `EpisodeSeconds(` or
/// `EpisodeSeconds.init(` construction. That last clause is the one that
/// matters, and L-F says why: a sixth site can be written as
/// `planListBound.map { EpisodeSeconds($0.rawValue) }` with this enum
/// untouched, and R2 probe PR5 confirmed it compiles. R3 review re-planted it
/// as `EpisodeSeconds.init($0.rawValue)` — five tagged sites intact — and the
/// preflight returned 0, so the `.init` spelling is counted now too.
///
/// Adding a case is the deliberate act. Removing one is what playhead-5pyq and
/// playhead-a1x0 do when they make the site sound.
enum UnsoundCursorPromotionSite: String, CaseIterable, Sendable {
    /// playhead-26od's incremental mid-pass checkpoint. Publishes
    /// `CoarseCoverageWalk.contiguousUpperBoundSec`, a PLAN-list prefix.
    case coarseCheckpoint
    /// playhead-pmp9's rate-limit defer. Same walk bound, via
    /// `CoverageOutcome.lastCoveredUpperBoundSec`.
    case rateLimitDefer
    /// playhead-t1kq's cancellation salvage. Same walk bound, via the honest
    /// cursor box.
    case cancellationSalvage
    /// The completion cursor for every NON-SPECIALIST phase —
    /// `segments.last?.endTime` over the POST-narrowing list. A SEGMENT-list
    /// bound rather than a plan-list one, same defect shape. This is the
    /// expression that actually wrote 53FC53E3's `2525.82` row: that row's
    /// `phase` is `fullEpisodeScan` (which the specialist completion below
    /// cannot reach) and its `status` is `complete` (which only
    /// `markBackfillJobComplete` sets). R1 review: it is NOT pinned by
    /// `processedPhaseCount == 1`, which the specialist completion also writes
    /// and which `monotonic(from:)` propagates.
    case segmentListCompletion
    /// The `specialistHostReadScan` completion cursor —
    /// `segments.last?.endTime` over the PRE-narrowing root inputs. Inert with
    /// the shipped defaults (two-key gated), and NOT named by playhead-5pyq,
    /// which counted four; this is the fifth site of the same shape.
    case specialistScanCompletion
}

extension EpisodeSeconds {

    /// playhead-x0lb / playhead-41mu: the ONE sound promotion of a list-relative
    /// bound to an episode cursor.
    ///
    /// A cursor is not a note about this run — it is an assertion that `[0, x]`
    /// of the EPISODE is covered. Publishing the end of a contiguous scanned
    /// prefix requires, at minimum, that the run's own plans began at the head,
    /// where "head" means *at or within the bridge tolerance of the prior
    /// cursor*, not "at zero". Returns `nil` when the evidence does not support
    /// the promotion, and the caller must then keep the prior cursor.
    ///
    /// **Why the bridge tolerance and not a rounding allowance.** Measured on
    /// the 2026-08-03 pull, AD5F3A0A's first fast segment starts at 2.82 s —
    /// leading silence, not a hole — while the same asset's cursor of 900 s sits
    /// below a genuine 2,370.4 s hole (its transcript does not resume until
    /// 3,270.42 s). The two witnesses exercise the two branches.
    ///
    /// **Necessary, not sufficient.** Contiguity is over the handed-over list,
    /// so a hole INSIDE the run's own segments is invisible here: ten of the
    /// twelve assets on the pull carry at least one interior fast gap wider than
    /// the bridge tolerance. Closing that is playhead-a1x0 and it needs
    /// ``RescanThresholdSec``, not this one.
    ///
    /// **The two ``PlanListSeconds`` cannot be told apart by the compiler, so
    /// they are told apart by a coherence guard** (R2 review, closing L-B): an
    /// upper bound BELOW the list's own start is not a bound, it is the two
    /// operands swapped. The swap type-checks — both are the same type, and R2
    /// probe PR3 confirmed it builds — and without the guard it fails SILENTLY
    /// and conservatively, promoting the start instead of the end. Now it is
    /// refused and `swappedOperandsAreRefused` pins it.
    static func promoting(
        _ bound: PlanListSeconds?,
        priorEpisodeCursor prior: EpisodeSeconds?,
        firstPlannedStart: PlanListSeconds?,
        bridge: BridgeToleranceSec
    ) -> EpisodeSeconds? {
        guard let bound, bound.isFinite else { return nil }
        // Vacuous on real inputs: `lastCoveredUpperBoundSec` is the end of a
        // plan that was COVERED and `firstPlannedSegmentStartSec` is the first
        // segment's start, so the former is never below the latter. When it does
        // fire the caller keeps the prior cursor, which re-scans audio already
        // read — the only direction this bead is permitted to move.
        if let firstPlannedStart, firstPlannedStart.isFinite,
           bound.rawValue < firstPlannedStart.rawValue {
            return nil
        }
        let priorUpper = prior?.rawValue ?? 0
        if let firstPlannedStart, firstPlannedStart.isFinite,
           !bridge.bridges(gapSec: firstPlannedStart.rawValue - priorUpper) {
            return nil
        }
        return EpisodeSeconds(bound.rawValue)
    }

    /// playhead-x0lb: promote a list-relative bound WITHOUT the evidence,
    /// naming the filed defect that makes it unsound.
    ///
    /// Behaviour-preserving by construction — it is the bare `Double` these
    /// sites already passed. What changes is that the promotion is now visible,
    /// enumerable and greppable instead of being invisible in a `Double`. Making
    /// these sites sound is a behaviour change with its own blast radius and its
    /// own bead (playhead-5pyq); it is deliberately NOT done here.
    ///
    /// Deliberately NOT overloaded on optionality: two entry points differing
    /// only in an `?` is how a call site ends up silently choosing the one that
    /// type-checks rather than the one it meant.
    static func unsoundPlanListPromotion(
        _ bound: PlanListSeconds?,
        site: UnsoundCursorPromotionSite
    ) -> EpisodeSeconds? {
        _ = site
        return bound.map { EpisodeSeconds($0.rawValue) }
    }
}

// MARK: - THE AUDIT (playhead-x0lb deliverable 4)
//
// Every `Double`/`Int` crossing a module boundary in the coverage/reach path,
// with its numerator, its denominator and its unit. The rule the bead sets is
// that **anything that cannot be stated in one line is a latent instance**, and
// the sections below are ordered by what that rule produced: stated and typed,
// stated and deliberately untyped, and stated but WRONG — which is where the
// audit earned its keep.
//
// ── TYPED. The substitution is now a compile error (mutation rails TY01–TY25).
//    R1 review added TY09–TY14 and the three types the first six of them
//    needed: the rails were PLANTED FIRST and three of them SURVIVED, which is
//    how the area/watermark conflations below were found rather than argued
//    about. R2 review did the same at the Activity surface and added TY15–TY17;
//    two of those three survived as well. R3 planted three more and ALL THREE
//    survived (TY18–TY21 pin them), so the running score for planting is
//    eight survivors from twelve plants across three rounds — a rate that has
//    not fallen, and the reason the LIMITS section calls itself a measurement.
//    R3's three share one shape and it is the shape R2 opened: a value that
//    reaches its slot through `?.rawValue`, or a paired term the previous round
//    typed only one half of.
//    R4 review stopped sampling and planted against the WHOLE remaining
//    surface in one pass — every substitutable boundary L-H had counted, plus
//    the two classes L-H did not count at all. TWENTY probes, TWENTY compiled,
//    against three vacuity controls (the TY05 / TY17 / TY21 mutations, run
//    through the same harness) that all three rejected with the expected
//    diagnostics. Four of the twenty are closed here (TY22–TY25); the other
//    sixteen are the untyped sections below and the limits, because closing
//    them costs a tradeoff this bead deliberately declined and one that is not
//    a reviewer's to take. THE NUMBER THAT MATTERS IS 20/20, not 4: the types
//    hold exactly where a NAMED FUNCTION takes the quantity, and nowhere else.
//    R5 (Dan's 2026-08-05 decision) took the tradeoff R4 declined and typed the
//    INTERVAL CARRIERS. The four probes R4 filed rather than fixed — PA5, PA7,
//    PA8, PA9 — were RE-PLANTED against this tree before any fix and all four
//    COMPILED again; they are now TY26–TY29. Typing the consumers then moved
//    the same confusion one layer further down, exactly as PA7 had moved it one
//    layer below R4's rails: probes PF2 and PF4 poured one query's rows into
//    another region's dictionary and both COMPILED, so the three accumulation
//    loops were split into three functions with one region each (TY30/TY31).
//    Two doors are measured OPEN and left there: PF3 (the two SQL columns
//    swapped at `append`, limit L-I at the genuine `sqlite3_column_double`
//    boundary) and PF6 (the two `[String: Double]` high-water dictionaries,
//    limit L-J). Two are measured CLOSED: PF1 and PF7 both tried to reach a
//    region's raw array from `AnalysisStore.swift` and both were REJECTED for
//    `fileprivate` protection — a clearance with a probe under it, not an
//    argument.
//
//    The list below is what carries a type. It is NOT a list of everything a
//    reader might want typed — the two sections after it are, and the middle
//    one is where the R2 and R3 findings landed.
//
//   AnalysisCoverageSummary.episodeDurationSec        EpisodeSeconds       s   position of the episode's end, DECLARED (feed or decode probe), not measured
//   AnalysisCoverageSummary.fastTranscriptCoveredSec  CoveredSeconds       s   AREA: interval union of pass='fast' transcript_chunks
//   AnalysisCoverageSummary.analysisCoveredSec        AnalyzedSeconds      s   AREA: that union clipped to the DSP frontier
//   AnalysisCoverageSummary.adScanCoveredSec          AdScanSeconds        s   AREA: coverage-lane scan windows that examined, ∩ the bridged transcript
//   AnalysisCoverageSummary.fastTranscriptCoverageEndSec  WatermarkSeconds s   REACH: MAX(endTime) of fast chunks, watermark fallback
//   AnalysisCoverageSummary.featureCoverageEndSec     FrontierSeconds      s   REACH: the DSP feature-extraction watermark
//   AnalysisCoverageSummary.confirmedAdCoverageEndSec FrontierSeconds      s   REACH: MAX(endTime) of DETECTED ad windows — not coverage at all
//   AnalysisCoverageSummary.finalPassCoverageEndSec   WatermarkSeconds     s   REACH: MAX(endTime) of final chunks, watermark fallback
//   AnalysisCoverageSummary.adScanFraction            ReachRatio      [0,1]    adScanCoveredSec ÷ episodeDurationSec
//   AnalysisCoverageSummary.transcriptDensity         DensityRatio    [0,1]    fastTranscriptCoveredSec ÷ episodeDurationSec
//   BackfillProgressCursor.lastProcessedUpperBoundSec EpisodeSeconds       s   position; asserts [0, x] OF THE EPISODE is covered
//   CoarseCoverageWalk.contiguousUpperBoundSec        PlanListSeconds      s   position; asserts a prefix OF THE HANDED-OVER LIST
//   CoverageOutcome.lastCoveredUpperBoundSec          PlanListSeconds      s   same, as the pass reports it
//   CoverageOutcome.firstPlannedSegmentStartSec       PlanListSeconds      s   where the handed-over list begins
//   AnalysisCoverageMath.adScanBridgeableGapSec       BridgeToleranceSec   s   width; "small enough to bridge WHEN MEASURING"
//   RescanThresholdSec.adScanRescanWorthyGapSec       RescanThresholdSec   s   width; "big enough to be worth RE-SCANNING" (a1x0, inert)
//   AnalysisJobRunner.semanticBackfillSufficientAdScanFraction  ReachRatio     the floor a completed ad scan is judged by
//   episodePreparationCompleteThreshold               ReachRatio      [0,1]    the same number, the same quantity, one definition
//   AnalysisCoordinator.AdScanCoverage.fraction       ReachRatio      [0,1]    the terminal's copy of adScanFraction
//
//    R5 added the INTERVAL CARRIERS, which are what the areas above are
//    measured FROM. They are regions, not scalars, so they carry no `rawValue`
//    and no `CoverageQuantity` conformance:
//
//   fetchCoverage… fastIntervals[id]      FastTranscriptRegion   pass='fast' chunk spans, or the watermark as [0, area]
//   fetchCoverage… finalIntervals[id]     FinalTranscriptRegion  pass='final' chunk spans
//   fetchCoverage… transcribedRegion      TranscribedRegion      fast ∪ final — what a semantic scan can READ (9y9e)
//   fetchCoverage… adScanIntervals[id]    ScannedRegion          coverage-lane windows that produced a verdict
//
// ── STATED AND DELIBERATELY UNTYPED. Each is one line, so none is latent; a
//    type is withheld because nothing else would share it or because typing it
//    would drag in a layer this bead is not converting.
//
//   AnalysisCoverageSummary.adScanDurationToleranceSec(episodeDurationSec:) -> Double
//       A WIDTH in seconds, min(one shard, 5 % of the duration). It is compared
//       against an AREA in one guard and a REACH in the next, deliberately, so a
//       type would have to be laundered at one of the two.
//   AnalysisCoverageMath.unionedSeconds / unionedSecondsClipped /
//   unionedSecondsIntersecting / bridgingShortGaps  ->  Double over [(start, end)]
//       Generic interval arithmetic, and it STAYS generic — R5 typed the
//       CARRIERS instead of the tuples, which is what "far cheaper than typing
//       every timestamp" below turned out to mean in practice: four region
//       structs, ~40 direct tests of these helpers untouched, and the four
//       probes closed. Everything from here to the end of this entry is the R2/
//       R3/R4 record of how the hole was found, kept because it is the evidence
//       and because R4's own reading of it was one of this bead's five failed
//       completeness claims.
//       R5 STATUS: PA5 → TY26, PA7 → TY27, PA8 → TY28, PA9 → TY29, all four
//       UNTYPEABLE. The consumers of these helpers in the coverage reader are
//       now ``AnalyzedSeconds/init(clipping:to:)`` and
//       ``AdScanSeconds/init(examined:within:bridging:)``, and the raw arrays
//       they delegate with are `fileprivate` to this file (probes PF1 and PF7
//       both REJECTED trying to reach them from `AnalysisStore.swift`).
//       R4 review: the sentence that used to stand here — "Which area a call
//       produces is decided at the ONE site that boxes the result, and the
//       boxing is what the R1 rails TY09/TY10 pin" — was FALSE, and it is the
//       fourth completeness claim of this bead to be measured and fail. TY09
//       and TY10 mutate `adScanFraction`'s CONSUMER (which summary field the
//       reach divides), not the boxing. Nothing pins which interval ARRAY goes
//       in, and four probes proved it, all four COMPILED:
//         PA7  the ad-scan area boxed from `fastIntervals` instead of
//              `adScanIntervals` — playhead-fil5 R3's own P0 (the transcript
//              published as ad-scan reach) reproduced ONE LAYER BELOW every
//              rail that looks for it, with `AdScanSeconds` intact on the box.
//         PA8  the ad-scan bound narrowed from `transcribedIntervals` to
//              `transcriptIntervals` — playhead-9y9e's defect verbatim, worth
//              55.4 % vs 100.0 % on 0C2FC22E per the comment at the site.
//         PA5  the transcript intervals themselves built from the DSP frontier,
//              which poisons TX, AN and the ad-scan bound in one edit.
//         PA9  the AN clip's INTERVALS widened to both passes. TY20 pins which
//              BOUND clips, and R3 said so; it does not pin what is clipped.
//       Four `[(start: Double, end: Double)]` arrays are live in one scope and
//       any of them fits any slot. No argument label closes this and no
//       constructor does either — argument labels are not types. The only
//       closure is to type the interval CARRIERS (a `TranscribedRegion` /
//       `ScannedRegion` over the arrays, which is far cheaper than typing every
//       timestamp), and that reverses a tradeoff this bead stated deliberately,
//       so it is presented rather than taken: it is Dan's call, filed with the
//       probes as evidence.
//       R5: Dan took it. See the R5 STATUS line at the top of this entry — the
//       four probes are closed and the sentence above is kept as the record of
//       what was proposed and why, not as an open item.
//       R3 review: `unionedSecondsClipped`'s `upperBound` is a second, separate
//       laundering point in the same call, and TY09/TY10 do NOT reach it — they
//       pin which INTERVALS go in, not which BOUND clips them. Probe PC2 clipped
//       the transcript union to the transcript's own WATERMARK instead of the
//       DSP frontier and it COMPILED, which makes AN identical to TX on every
//       episode — the one direction of the sd71 antipattern a reader cannot
//       spot, because the two bars simply agree. The helper stays generic; the
//       boxing now goes through ``AnalyzedSeconds/init(clipping:to:)``, which
//       names both operands, and rail TY20 pins it.
//   EpisodePreparationReadiness.analysisFraction / .downloadFraction -> Double
//       Post-clamp BAR FILLS in [0, 1], provenance deliberately discarded. Note
//       the two are adjacent fields with different units underneath: the analyze
//       zone's numerator is seconds and the download zone's is BYTES.
//       R3 review: that adjacency was WRITABLE, not merely noted.
//       `clampUnit(inputs.adScanFraction?.rawValue)` is the third instance of
//       the spelling R2 found twice in `ActivitySnapshotProvider` — the
//       `?.rawValue` is at the CALL SITE, so the type has stopped applying
//       before the argument is read — and probe PC3 drove the analyze zone from
//       `inputs.downloadFraction`, the BYTES-derived one, with no complaint. The
//       analyze zone now demotes inside `analyzeZoneFill(_: ReachRatio?)`; the
//       download zone keeps `clampUnit`. Rail TY21.
//       R4 review: "the download zone keeps `clampUnit`, which is honest
//       because its input is a genuine `Double`" used to end that sentence, and
//       it is an argument about where the value CAME FROM when the question is
//       what the slot ACCEPTS. Probe PE1 wrote
//       `clampUnit(inputs.adScanFraction?.rawValue)` at the DOWNLOAD zone — the
//       exact mirror of the PC3 substitution R3 closed one line above — and it
//       COMPILED. R3 fixed one direction of a two-way adjacency and argued the
//       other was safe; the argument was wrong in the same round it was
//       written. It is NOT fixed here, and the reason is a cost, not a
//       judgement that it is fine: the only closure is a distinct type for the
//       BYTES-derived download fraction, and `downloadFraction` is threaded
//       through `EpisodePreparationStatusModel`, `ActivityViewModel`,
//       `ActivityView`, `DiagnosticsExportService` and ~60 sites across eight
//       test files. That is a typing decision with its own blast radius and its
//       own bead, not a review-round edit. Recorded as limit L-K.
//   ActivitySnapshotProvider's AN fraction -> Double
//       analysisCoveredSec ÷ episodeDurationSec — a THIRD ratio over the same
//       denominator as reach and density. Left untyped because one producer and
//       one consumer share it; recorded here so the next reader does not have to
//       re-derive which area it is. Its NUMERATOR is typed (``AnalyzedSeconds``)
//       even though the ratio is not, which is what stops the ad-scan area or
//       the transcript area being substituted into it.
//       R2 review: that last sentence was FALSE when it was written, and the
//       fix is what makes it true. The two `fraction(area:durationSec:)` helpers
//       took a `Double?` and each call site read `?.rawValue` off the summary,
//       so probe PR2 substituted `summary?.adScanCoveredSec?.rawValue` and it
//       BUILT. The helpers now take an ``AnalyzedSeconds?`` and demote inside —
//       rails TY15/TY16.
//       R3 review: R2 typed the NUMERATOR and left `durationSec: Double`, which
//       made this the one ratio in the path with a half-typed pair —
//       ``ReachRatio/init(examined:ofDeclaredDuration:)`` and
//       ``DensityRatio/init(transcribed:ofDeclaredDuration:)`` both name BOTH
//       terms. Probe PC1 passed the transcript WATERMARK as the AN bar's
//       denominator and it COMPILED. Both helpers now take an
//       ``EpisodeSeconds?`` (the `?? 0` that collapsed an absent duration is
//       gone, behaviour-identically: the old zero failed `> 0` exactly where
//       the new `nil` does) — rails TY18 (UI) and TY19 (dogfood wire), one
//       per call site for the reason R2 gave: fixing one and not the other is
//       how this family survives.
//       R4 review: typing both TERMS stops the wrong quantity ARRIVING and does
//       nothing about what happens once it has. Inside the helper both operands
//       were `Double` again, and probes PB1 (UI) and PB2 (wire) wrote
//       `duration.rawValue / area.rawValue` — the RECIPROCAL — and both
//       COMPILED. It is not a cosmetic defect: an inverted ratio renders in the
//       same `[0, 1]` bar and clamps to 1.0 on every episode whose analyzed
//       area is under its duration, i.e. a FULL AN bar on an episode nothing
//       analyzed. Both helpers now delegate to
//       ``AnalyzedSeconds/fractionOfDeclaredDuration(_:)``, where the division
//       is written once with the numerator as the receiver, so there is no
//       expression left to invert — rails TY24 / TY25. THE GENERAL LESSON, and
//       it is why the ratio constructors were built this way from the start: a
//       typed PAIR is not a typed OPERATION, and only the operation is safe.
//   ActivitySnapshotProvider's TX fraction -> Double
//       transcriptDensity, demoted to a bar fill. R2 review ADDED this line: it
//       is the AN fraction's sibling, it was in neither list, and probe PR1
//       rendered `summary?.adScanFraction` — the REACH — as the transcript bar
//       with no complaint from the compiler. It now goes through
//       `transcriptBarFill(_: DensityRatio?)`, which is where the provenance is
//       deliberately discarded, and rail TY17 pins it. Two adjacent bar fills
//       whose underlying quantities differ is the same shape as the analyze /
//       download pair noted above, and it is why neither is left inline.
//   DogfoodDiagnosticsPipelineSnapshot's coverage fields -> Double?
//       R3 review ADDED this line, because the audit named exactly one member
//       of it (L5) and named it as a DEFECT rather than as a surface. SIX typed
//       quantities are demoted at one call in `ActivitySnapshotProvider`'s wire
//       builder — `episodeDurationSec` (``EpisodeSeconds``),
//       `transcriptCoveredSec` (``CoveredSeconds``), `featureCoverageEndSec`
//       and `confirmedAdCoverageEndSec` (``FrontierSeconds``),
//       `fastTranscriptWatermarkSec` and `finalPassCoverageEndSec`
//       (``WatermarkSeconds``) — into six adjacent `Double?` wire parameters,
//       where any of them fits any other. It is left untyped because the struct
//       is a `Codable` JSON snapshot and typing it would put every quantity on
//       the wire (limit L-E), and because a dogfood field is not a decision
//       input. It is NOT left unstated: L5 is proof the family is already live
//       here, one field carrying the area under the watermark's name.
//       R4 review: the surface is NINE adjacent `Double?` slots, not six. The
//       three `*_fraction` slots belong to it too — `downloadFraction`,
//       `transcriptFraction`, `analysisFraction` are three `Double?` locals
//       handed to three `Double?` parameters at the same call, and L-H's count
//       of six missed them because they reach the wire through typed HELPERS
//       and so carry no `.rawValue` of their own. Probe PB5 wrote
//       `analysisFraction: downloadFraction` — the BYTES-derived fraction into
//       the analysis slot, PE1's shape on the diagnostics wire — and it
//       COMPILED, as did PB3 (the duration slot fed the transcript watermark)
//       and PB4 (the feature watermark fed the transcript area, which also
//       feeds `analysisWatermarkSec` through `maxKnown`). Confirming a stated
//       limit is worth the three builds it cost: "any of them fits any other"
//       was an assertion until it was measured, and the measurement widened the
//       surface by half.
//
// ── LATENT INSTANCES. Found by writing the line. Each is filed; none is fixed
//    here, because each is a behaviour or wire change with its own blast radius.
//
//   L1  SemanticScanClaim.transcriptClearsFinalizeFloor(coveredSec:episodeDurationSec:)
//       `coveredSec: Double` accepts an AREA or a WATERMARK indistinguishably.
//       Production hands it the 5 s-bridged AREA; `SemanticScanClaimWireInTests`
//       hands it a WATERMARK at one site and an area at another ON PURPOSE, to
//       show they differ. Two callers, ~14 test sites — playhead-fpnt.
//   L2  SemanticScanCoverage.examinedFraction
//       examinedSeconds ÷ (examinedSeconds + unexaminedSeconds) — the span the
//       PASS ATTEMPTED, NOT the episode. Same name-shape as `adScanFraction`,
//       different denominator, and they already diverge by 30x in the suite's own
//       `a-diverge` fixture (breadcrumb 3,600 s vs summary 120 s) — playhead-dehs.
//   L3  AnalysisCoordinator.classifyBackfillTerminal's transcript ratio
//       `coverageEnd = chunks.map(\.endTime).max()` — a WATERMARK — is passed to
//       `contradictedCoverageDenominator(transcriptCoveredSec:)`, an AREA name,
//       and divided by the duration to make `transcriptRatio`. The VALUE is
//       arguably right (the 0.95 floor is calibrated for reach, "the few seconds
//       a decoder chops off the end"); the NAME is not. On AD5F3A0A the two
//       readings differ by 2.6x — playhead-pwsu.
//   L4  AnalysisWorkScheduler.capOutRetryDecision(transcriptCoverageSec:)
//       Fed `asset?.fastTranscriptCoverageEndTime` — a WATERMARK — under a
//       parameter named for coverage seconds — playhead-ps9j.
//   L5  DogfoodDiagnosticsPipelineSnapshot.transcriptWatermarkSec
//       The wire field named for a watermark carries the transcript AREA — the
//       same value as `transcript_covered_sec` one line above it — while the real
//       reach ships separately as `fast_transcript_watermark_sec`. Found by these
//       types; behaviour deliberately unchanged — playhead-vkwr.
//
// ── THE MECHANISM'S OWN LIMITS, stated because a claim of completeness that is
//    not complete is instance 18 of the catalogue. R1 review found the first
//    version of this section incomplete BY PLANTING SUBSTITUTIONS AND WATCHING
//    THEM BUILD, which is why the list below is longer than the one it replaces
//    and why `scripts/mutation-battery-untypeable.py` now carries a rail for
//    each closed hole rather than an argument that it is closed. R2 review did
//    the same again and the list grew from five to seven: L-F and L-G are both
//    survivors of planted substitutions (probes PR5 and PR6), and L-B moved
//    from "documented" to "guarded". THE HONEST READING OF THAT TRAJECTORY is
//    that this section is a measurement, not a proof — every round that has
//    planted has found one, so assume the next one exists.
//    R3 made that prediction come true twice over: it found the eighth (L-H,
//    the `.rawValue` surface, which is the GENERALISATION of the two R2 found
//    one at a time) and it found the `.init` hole inside L-F's own tripwire.
//    Five, seven, eight — the list has grown in every round that planted, and
//    the count of limits is a count of what somebody probed, not of what is
//    there.
//    R4 STOPPED SAMPLING AND MEASURED THE WHOLE THING, and the result changes
//    what this section is. Twenty substitutions planted across every remaining
//    boundary, twenty COMPILED, three vacuity controls rejected. Eleven, after
//    the four closed as TY22–TY25. The rate did not fall from round to round
//    because the rounds were unlucky; it did not fall because L-H had the
//    surface WRONG — it counted `.rawValue` reads, and `.rawValue` is one of
//    four doors. The other three are L-I (boxing: a raw `Double` entering a
//    typed slot, 23 sites, uncounted anywhere before now), L-J (two values of
//    the SAME type, which no type can separate) and L-G's literal door. Reading
//    "the list grew again" as bad news is the wrong reading; the list grew
//    because somebody finally counted, and a limit nobody has probed is not an
//    absent limit.
//
//   L-A  A ``CoveredSeconds`` can still be carrying a REACH. When no chunk
//        landed, `fastTranscriptCoveredSec` falls back to the
//        `fastTranscriptCoverageEndTime` COLUMN, which is a watermark, and the
//        only thing that says so is the `CoverageProvenance` tag beside it. The
//        types stop a watermark being handed ACROSS a boundary as an area; they
//        cannot stop one being put in the box at the bottom. That fallback is
//        playhead-0sro's shape and it is load-bearing —
//        `AnalysisJobRunner`'s `watermarkWithoutChunksStillFails` fixture is
//        built on it — so it is documented rather than removed.
//   L-B  ``EpisodeSeconds/promoting(_:priorEpisodeCursor:firstPlannedStart:bridge:)``
//        takes TWO ``PlanListSeconds``, so the 41mu R2 defect (one operand from
//        a different list) is a compile error but SWAPPING the run's own two
//        operands is not — R2 probe PR3 planted the swap and it built. THE TYPE
//        STILL CANNOT SEE IT; what closes it is a coherence guard inside
//        `promoting` (an upper bound below its own list's start is refused),
//        pinned by `swappedOperandsAreRefused`. Recorded as a limit rather than
//        deleted because the guard is a VALUE check that happens to catch this
//        shape, not a type that forbids it: a swap between two plan-list
//        positions that satisfy `bound >= start` anyway is still writable.
//   L-C  ``FrontierSeconds`` deliberately carries BOTH the DSP feature
//        watermark and the detected-ad maximum, because `analysisFrontierSec`
//        is their `max`. Reading one as the other is therefore still typeable.
//        The substitution that cost money — either of them read as the
//        TRANSCRIPT's reach — is not.
//   L-D  Nothing type-checks the ``UnsoundCursorPromotionSite`` tag against the
//        site it is written at; that is what `TY99` states as this battery's
//        vacuity control.
//   L-E  Every quantity codes as a BARE NUMBER, so the `Codable` conformance is
//        a laundering channel by construction: bytes written by one type decode
//        into any other. That is not a defect to fix — it is what keeps the
//        persisted cursor readable — and it is inert today because
//        `BackfillProgressCursor` is the only Codable carrier in this path and
//        its field has exactly one type. A second persisted quantity would make
//        it live, and the guard is that decoding names the target type
//        explicitly at the call site.
//        R2 review RE-DERIVED "the only Codable carrier" rather than believing
//        it: `AnalysisCoverageSummary`, `CoverageOutcome`, `CoarseCoverageWalk`,
//        `EpisodePreparationInputs` and `EpisodePreparationAnalysisInputs` are
//        all `Sendable, Equatable` and none is `Codable`. The claim holds.
//        R3 review re-derived it AGAIN, and widened the question from "which
//        containers in this path are Codable" to "which Codable container holds
//        a field of one of these types", which is what the limit is actually
//        about. Eleven quantity-typed stored properties exist outside this
//        file; their containers are `AnalysisCoverageSummary`,
//        `EpisodePreparationInputs`, `EpisodePreparationAnalysisInputs`,
//        `CoverageOutcome`, `CoarseCoverageWalk`, `BackfillHonestCursorBox`,
//        `CoarseCheckpointBox`, `AnalysisCoordinator.AdScanCoverage` and
//        `BackfillProgressCursor`. Only the last is `Codable`.
//        `DogfoodDiagnosticsPipelineSnapshot` IS `Codable` and is the nearest
//        thing to a counterexample — but it stores `Double?`, having been handed
//        six demoted quantities at the call (see the audit's untyped section),
//        so it is a laundering surface rather than a Codable carrier. The claim
//        still holds, for a reason one word wider than R2's.
//   L-F  **A promotion can bypass the inventory entirely.** ``PlanListSeconds``
//        → ``EpisodeSeconds`` has two named doors (``EpisodeSeconds/promoting``
//        and ``EpisodeSeconds/unsoundPlanListPromotion(_:site:)``) and one
//        unnamed one: `bound.map { EpisodeSeconds($0.rawValue) }`. R2 probe PR5
//        replaced the rate-limit site with exactly that and it COMPILED, with
//        ``UnsoundCursorPromotionSite`` and its test both still green — so a
//        SIXTH unsound promotion could land while the "inventory" said five.
//        No type can close this: `.rawValue` is a `RawRepresentable`
//        requirement and `EpisodeSeconds(_:)` is a `CoverageQuantity` one. What
//        stands in its place is the `--check-inventory` preflight in
//        `scripts/mutation-battery-untypeable.py`, which is a LEXICAL check and
//        therefore exactly the kind of thing this bead exists to distrust —
//        it is a tripwire on one file, not a proof, and it is stated here as
//        such rather than sold as one.
//        R3 review MEASURED how thin it is: the same sixth promotion written
//        `EpisodeSeconds.init($0.rawValue)`, with all five tagged sites left
//        intact, passed the preflight with rc=0. Both spellings are counted
//        now, and SwiftLint's `explicit_init` independently rejects the second
//        — but the general shape (a promotion helper declared in ANOTHER file
//        and merely called from the runner) is still unreachable by a check
//        that reads one file, and no amount of spelling closes that.
//   L-G  **Literal expressibility admits the OTHER threshold's value.**
//        `ExpressibleByFloatLiteral` is deliberate (see the file header: a
//        literal has no provenance to lose), and it is nonetheless a hole for
//        the one pair whose values are themselves measurements —
//        `bridgingShortGaps(…, upTo: 60.0)` compiles, which is instance 19 in
//        the spelling that skips the constant altogether. R2 probe PR6
//        confirmed it. Withdrawing the conformance would cost every
//        `#expect(bridge == 5.0)` and every `ReachRatio(0.98)` in the tree for a
//        hole a magic number already advertises, so the trade is taken
//        knowingly and written down instead.
//   L-H  **`.rawValue` is where the types stop applying, and it is a SURFACE,
//        not an incident.** R3 review's finding is not any one of the three
//        below — it is that both of R2's High findings went through `?.rawValue`
//        and nobody had counted the rest. Counted now, in production code added
//        by this bead: 47 genuine reads (excluding the 22 `let rawValue` /
//        memberwise-init declaration lines and every prose mention). 21 of the
//        47 are inside THIS FILE — the mechanism's own guards, divisions and
//        promotions, where both operands are already the same kind and nothing
//        can be substituted. The other 26 are real boundaries, and they classify
//        as: 4 deliberate cross-kind comparisons inside `adScanFraction`
//        (area-vs-duration, watermark-vs-duration; pinned by TY06/TY09–TY11),
//        3 demotions into generic interval arithmetic, 6 into
//        `DogfoodDiagnosticsPipelineSnapshot`'s adjacent `Double?` wire fields,
//        4 into bar fills (now behind `transcriptBarFill` / `analyzeZoneFill` /
//        the two `fraction(area:ofDeclaredDuration:)` helpers), 3 into
//        `String(format:)` diagnostics, 2 cashing the cursor in
//        `narrowedForResume` against raw segment times, 1 epsilon reconstruct in
//        `clearsFinalizeFloor`, 1 the Episode→PlanList crossing, and the
//        remainder guard-local.
//        THE RULE THAT FALLS OUT: a `.rawValue` whose result flows into a
//        parameter, a dictionary, a JSON field, a format string, an arithmetic
//        expression or a comparison **that would also accept a different
//        quantity** is a laundering point, and the fix is always the same one —
//        move the demotion INSIDE a named function that takes the quantity.
//        R3 planted against the three that looked load-bearing and all three
//        survived; that is a 3-for-3 hit rate on a surface of 26, so the honest
//        expectation is that more of the remaining ones are writable, not that
//        they are safe. What is claimed here is a CENSUS, not a clearance.
//
//        **R4 REVIEW: IT REMAINS A CENSUS, AND IT IS NOW A MEASURED ONE.** The
//        26 is a count of LINES; those lines carry 30 reads (four lines read
//        two quantities each), against 49 added `.rawValue` reads in total, 19
//        of them inside this file. R4 planted against every substitutable one
//        of the 26 and against the classes L-H does not cover. TWENTY PROBES,
//        TWENTY COMPILED — 20/20, not 3/26 extrapolated — with three vacuity
//        controls (the TY05 / TY17 / TY21 mutations through the same harness)
//        rejecting with the expected diagnostics, so the result is not a
//        harness that says yes to everything.
//
//        THE FIVE LINES THAT ARE GENUINELY NOT SUBSTITUTABLE, and why, because
//        this is the only part of the surface that is CLEARED rather than
//        counted:
//          * `transcriptBarFill`'s `density?.rawValue` and `analyzeZoneFill`'s
//            `fraction?.rawValue` — the R2 and R3 fixes. Each body has exactly
//            ONE value in scope and it arrived typed. Nothing else is reachable
//            to substitute, which is the whole reason the fix is shaped this way.
//          * `narrowedForResume`'s two reads of `cursor.rawValue` — same
//            property: one typed parameter, nothing else of any quantity type
//            in scope, and TY12 pins the call site.
//          * `adScanDurationToleranceSec`'s `0.05 * episodeDurationSec.rawValue`
//            — one typed parameter, sole quantity in scope.
//        That is the shape of every real clearance in this bead: not "the
//        expression looks careful" but "there is no second quantity in scope to
//        write". FIVE of twenty-six, and that is the whole cleared set.
//
//        WHERE THE OTHER TWENTY-ONE WENT, re-counted after the R4 fixes rather
//        than inferred: SIX lines stopped existing, because moving a demotion
//        inside a named function removes the `.rawValue` rather than guarding
//        it — the two guards at `adScanFraction` and the four that made up the
//        two `fraction` helpers. The surface outside this file is now 20 lines
//        carrying 20 reads (measured, not derived: it was 26 lines / 30 reads,
//        four of which read two quantities on one line). FIFTEEN of those 20
//        are measured OPEN by the probes above; five are the cleared set. So
//        the census shrank by closing, not by re-labelling, which is the only
//        direction that means anything.
//
//        THE CORRECTED RULE. `.rawValue` is where a type stops applying on the
//        way OUT; it is not the only place types stop applying. A quantity is
//        unprotected at FOUR doors, and L-H named one:
//          1. demotion  — `.rawValue`, this limit, 26 lines;
//          2. promotion — `Type(someRawDouble)`, limit L-I, 23 sites;
//          3. identity  — two values of the same type, limit L-J;
//          4. literal   — `ExpressibleByFloatLiteral`, limit L-G.
//        A round that audits only door 1 will keep finding survivors at the
//        other three and keep reading the rate as bad luck.
//   L-I  **BOXING IS UNGUARDED, AND IT IS A BIGGER SURFACE THAN L-H'S.** Every
//        `SomeQuantity(rawDouble)` is a point where the wrong number can be put
//        in the right-named box, and the type is not merely powerless there —
//        it is what makes the mistake INVISIBLE downstream, because everything
//        after the box reads a correctly-typed value. There are 23 such
//        constructions in production code outside this file (14 in
//        `AnalysisStore`, 6 in `BackfillJobRunner`, 2 in `AnalysisCoordinator`,
//        1 in `EpisodePreparationReadiness`), and until R4 the audit named
//        exactly one of them — L-A, the `CoveredSeconds(watermark)` fallback.
//        Three were probed and all three COMPILED: PA6 boxed the FEATURE column
//        as the final-pass `WatermarkSeconds`; PA11 boxed that same column as
//        the reach DENOMINATOR (`EpisodeSeconds`), which is the one term every
//        ratio in the path divides by; PA12 boxed the transcript AREA as the
//        transcript WATERMARK — the exact inverse of L-A and unnamed anywhere.
//        No type can close this by construction: a `CoverageQuantity` is a
//        `RawRepresentable` over `Double` and its initialiser must accept any
//        `Double`. What CAN close a given box is evidence at the box —
//        ``AnalyzedSeconds/init(clipping:to:)`` is the pattern, taking the
//        frontier as a `FrontierSeconds` rather than a number — and doing that
//        for the other 22 is a bead, not a review round.
//
//        **R5: RE-DERIVED, AND IT MOVED BY ONE.** Under R4's own definition
//        (`\bQuantity(` in production code outside this file, comment lines
//        excluded) the count reproduces at 23 on `78892f0d` and reads 22 at
//        HEAD — 13 in `AnalysisStore`, 6 in `BackfillJobRunner`, 2 in
//        `AnalysisCoordinator`, 1 in `EpisodePreparationReadiness`. The one
//        that went is `CoveredSeconds(unionedSeconds(fastIntervals[id] ?? []))`,
//        now ``FastTranscriptRegion/unionedSeconds``: a hand-picked array and a
//        hand-named box replaced by a property of the region that decides both.
//        TWO MORE of the 22 stopped being BARE boxes without leaving the count
//        — `AnalyzedSeconds(` and `AdScanSeconds(` in the reader now take named
//        typed operands rather than a computed `Double` — so under the tighter
//        reading "a quantity built from an unlabelled expression" it is 18.
//        Both readings are stated because quoting only the flattering one is
//        this bead's own defect class.
//
//        **R5 ALSO ADDED A SURFACE, counted here rather than announced as a
//        clearance.** The four region types are constructed at 9 sites in
//        `AnalysisStore`: 8 are the empty region, which can carry nothing
//        wrong, and 1 is the named watermark stand-in, whose argument is a
//        ``CoveredSeconds`` because probe PA5 fed it a ``FrontierSeconds``
//        (rail TY26). The genuine raw door is `append(start:end:)`, 3 sites,
//        taking two bare `Double`s straight off `sqlite3_column_double`: probe
//        PF3 swapped the two columns and it COMPILED. No type closes that —
//        SQLite returns a `Double` and nothing else. What R5 changed is that
//        there is now exactly ONE such door per region, inside a function that
//        reads one query into one region; before the split, probes PF2 and PF4
//        crossed regions at those very lines and both COMPILED (rails
//        TY30/TY31). Note also that
//        `CoverageProvenance` already sits beside several of these boxes
//        recording which column was used: it is a LABEL, exactly as
//        ``UnsoundCursorPromotionSite`` is, and L-D applies to it verbatim.
//   L-J  **TWO VALUES OF THE SAME TYPE ARE INDISTINGUISHABLE, BY DESIGN.** L-B
//        recorded this for one pair (``EpisodeSeconds/promoting``'s two
//        ``PlanListSeconds``) and closed it with a value guard. R4 measured how
//        much wider the class is; all four COMPILED:
//          PC1  the Episode→PlanList crossing in `checkpointCoarseProgress`
//               cashing `honest` instead of `merged`, which silently undoes the
//               monotonic merge the two lines above it exist to perform;
//          PD1  `clearsFinalizeFloor`'s epsilon reconstruct reading
//               `sufficientFraction` as the measurement, making every terminal
//               read clean;
//          PD2  the two ``ReachRatio`` operands of `diagnostic`'s
//               `String(format:)` swapped;
//          PA1  `adScanFraction`'s divide-by-zero guard reading the NUMERATOR
//               (harmless only because ``ReachRatio``'s constructor re-guards
//               the denominator — the guard is redundant, not sound).
//        R5 adds one more, measured on the post-fix tree: PF6 handed
//        `readFastTranscriptRegions` the FINAL pass's high-water dictionary.
//        Both are `[String: Double]` and it COMPILED. The region arguments
//        beside it are typed and TY30/TY31 pin them; the two watermark
//        accumulators are the same type by nature and no type separates them.
//        These are behaviour defects and BEHAVIOUR tests are the instrument for
//        them, which is the honest division of labour: the UNTYPEABLE battery
//        answers "can this be written", and for a same-type swap the answer is
//        always yes and always will be. Do not widen a type to chase one.
//   L-K  **THE DOWNLOAD/ANALYZE ADJACENCY IS STILL WRITABLE IN ONE DIRECTION.**
//        R3 closed `clampUnit(inputs.adScanFraction?.rawValue)` at the ANALYZE
//        zone and argued the download zone was safe because its input is a
//        genuine `Double`. Probe PE1 wrote that same expression at the DOWNLOAD
//        zone and it COMPILED: the ad-scan REACH rendering as the download bar,
//        the mirror of the substitution fixed one line above. The argument
//        confused where a value came from with what a slot accepts. Closing it
//        needs a distinct type for the BYTES-derived download fraction, whose
//        blast radius is ~60 sites across `EpisodePreparationStatusModel`,
//        `ActivityViewModel`, `ActivityView`, `DiagnosticsExportService` and
//        eight test files — a bead, and a typing decision that is Dan's, not a
//        reviewer's. Filed rather than fixed, and stated here rather than
//        argued away a second time.
