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
/// `BackfillJobRunner.swift` must contain no bare `EpisodeSeconds(`
/// construction. That last clause is the one that matters, and L-F says why:
/// a sixth site can be written as `planListBound.map { EpisodeSeconds($0.rawValue) }`
/// with this enum untouched, and R2 probe PR5 confirmed it compiles.
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
// ── TYPED. The substitution is now a compile error (mutation rails TY01–TY14).
//    R1 review added TY09–TY14 and the three types the first six of them
//    needed: the rails were PLANTED FIRST and three of them SURVIVED, which is
//    how the area/watermark conflations below were found rather than argued
//    about.
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
//       Generic interval arithmetic. These PRODUCE the area types; typing their
//       tuples would type every transcript-chunk timestamp in the app. Which
//       area a call produces is decided at the ONE site that boxes the result
//       — `fetchCoverageSummariesByAssetIds` — and the boxing is what the R1
//       rails TY09/TY10 pin.
//   EpisodePreparationReadiness.analysisFraction / .downloadFraction -> Double
//       Post-clamp BAR FILLS in [0, 1], provenance deliberately discarded. Note
//       the two are adjacent fields with different units underneath: the analyze
//       zone's numerator is seconds and the download zone's is BYTES.
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
//   ActivitySnapshotProvider's TX fraction -> Double
//       transcriptDensity, demoted to a bar fill. R2 review ADDED this line: it
//       is the AN fraction's sibling, it was in neither list, and probe PR1
//       rendered `summary?.adScanFraction` — the REACH — as the transcript bar
//       with no complaint from the compiler. It now goes through
//       `transcriptBarFill(_: DensityRatio?)`, which is where the provenance is
//       deliberately discarded, and rail TY17 pins it. Two adjacent bar fills
//       whose underlying quantities differ is the same shape as the analyze /
//       download pair noted above, and it is why neither is left inline.
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
//    planted has found one, so assume the eighth exists.
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
