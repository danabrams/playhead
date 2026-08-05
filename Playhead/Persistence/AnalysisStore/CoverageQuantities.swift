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

/// playhead-x0lb: a HIGH-WATER MARK — the greatest end time some producer has
/// reached. A position, never an area.
///
/// * numerator / denominator: none — a position.
/// * unit: seconds of episode audio.
///
/// **Why it is its own type.** A watermark and an area are the same number only
/// on a gapless transcript, and real transcripts are not gapless: a
/// `transcript_chunks` row spans first word to last word, so the raw union is
/// riddled with inter-utterance holes. On the 2026-08-03 pull AD5F3A0A's fast
/// watermark is 4,280.9 s while its fast AREA is 1,645.9 s — the watermark
/// over-reports by 2.6x. Reading one as the other is the
/// "AN 100 % / TX 39 %" antipattern (playhead-sd71), and instance 9 of this
/// bead's catalogue in its most expensive form: the readiness ✓ keyed on the DSP
/// watermark and `max(endTime)` of DETECTED ads, so an episode where detection
/// did WORSE read as MORE complete.
///
/// A `ReachRatio` cannot be built from one of these; that is the point.
struct WatermarkSeconds: CoverageQuantity {
    let rawValue: Double
    init(_ rawValue: Double) { self.rawValue = rawValue }
}

/// playhead-x0lb: an AREA — de-overlapped seconds of audio. Never a position.
///
/// * numerator / denominator: none — an area, and usually somebody else's
///   numerator.
/// * unit: seconds of episode audio.
///
/// Overlapping spans count once and gaps are excluded, which is what makes it
/// the honest numerator for a coverage ratio and what makes it strictly smaller
/// than the corresponding ``WatermarkSeconds`` whenever the transcript has a
/// hole.
struct CoveredSeconds: CoverageQuantity {
    let rawValue: Double
    init(_ rawValue: Double) { self.rawValue = rawValue }
}

// MARK: - Ratios, each naming both of its terms

/// playhead-x0lb: **REACH** — how much of the episode a semantic ad scan has
/// actually READ.
///
/// * numerator: ``CoveredSeconds`` — the interval union of coverage-lane
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
    init?(examined: CoveredSeconds?, ofDeclaredDuration duration: EpisodeSeconds?) {
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
    /// The `fullEpisodeScan` completion cursor — `segments.last?.endTime` over
    /// the POST-narrowing list. A SEGMENT-list bound rather than a plan-list
    /// one, same defect shape. This is the expression that actually wrote
    /// 53FC53E3's `2525.82` row (`processedPhaseCount: 1` is written by the
    /// completion path alone).
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
    static func promoting(
        _ bound: PlanListSeconds?,
        priorEpisodeCursor prior: EpisodeSeconds?,
        firstPlannedStart: PlanListSeconds?,
        bridge: BridgeToleranceSec
    ) -> EpisodeSeconds? {
        guard let bound, bound.isFinite else { return nil }
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
