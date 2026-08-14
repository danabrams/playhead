// DayZeroMarkCensus.swift
// playhead-ug9m: the on-disk fact that says an asset's day-0 marks are TRAPPED,
// and the single spelling of which of them a re-mint may supersede.
//
// THE PROBLEM THIS EXISTS FOR. `DayZeroRediffAttemptPolicy` makes `.marked`
// terminal across generations, on the reasoning that "the marks are already
// persisted, so a re-fetch would spend ~108 MB to mint nothing". That reasoning
// is sound for a mark that came out at full quality and WRONG for one that did
// not — and playhead-qs0d made the difference matter, because a STRICT
// monotonic-clean byte-exact slot now earns `.rediffByteExact` anchors and
// `eligibilityGate = .eligible` (it auto-skips) while a 9s6q segment-recovered
// one stayed `unanchored` + `.markOnly` (it bannered, forever). Two populations
// are frozen at a degraded first attempt:
//
//   * rows minted BEFORE qs0d, which persisted `unanchored` EVEN WHEN STRICT
//     because no build had yet written an anchor;
//   * rows minted AFTER qs0d whose every slot came out non-strict, which will
//     never re-attempt with different personas that might produce a strict one.
//
// playhead-pyq7 CHANGES WHO IS IN THE SECOND SET, IN BOTH DIRECTIONS, AND THE
// CENSUS ITSELF IS UNTOUCHED. Going forward a segment-recovered mint stamps
// anchors, so it reads `.anchored` here and is correctly not rescuable —
// there is nothing degraded left to improve. Rows ALREADY on disk are not
// re-stamped (the mint is idempotent), so Dan's nine `unanchored` day-0 rows of
// 2026-08-13 stay degraded and stay rescuable.
//
// playhead-c7ef CLOSES THE OTHER HALF, and the census is STILL untouched — the
// change is one new rule, `reMintMayReplace` at the bottom of this file, plus a
// `DayZeroRediffAttemptPolicy.currentGeneration` bump. Two independent
// mechanisms had those nine rows frozen and BOTH had to move:
//
//   * the mint's own `guard strict` refused every recovered re-mint, so even a
//     granted rescue would have dropped all of its slots as
//     `.allSlotsAlreadyCovered`. `reMintMayReplace` supersedes that guard;
//   * `.marked` is terminal, and `decide`'s rescue branch also needs a FOREIGN
//     generation — which, measured against the shipped `currentGeneration = 2`,
//     no device row had. The lane was closed, so no rescue fired and no byte was
//     spent. The bump is the re-entry, and it is deliberate and bounded rather
//     than a loosened terminal check.
//
// Neither half does anything alone, which is why they land together.
//
// WHY THE CENSUS IS THE RIGHT INPUT, AND NOT THE ATTEMPT RECORD. The acceptance
// arm (`alignment.monotonicClean`) that decided each slot's anchor was NEVER
// persisted — not on `ad_windows`, not in `rediff_day_zero_attempts` — and the
// B-copies it was computed from are deleted by construction (never-persist-B).
// So no amount of reading the attempt record can tell a pre-qs0d STRICT row from
// a segment-recovered one, and a migration that re-stamped anchors would be
// guessing. What the ROWS can answer, exactly, is a different and sufficient
// question: **did this asset's day-0 attempt produce any anchored mark at all?**
//
//   * At least one anchored row  ⇒ the attempt ran on a build that could stamp
//     anchors and DID; its unanchored siblings are therefore provably
//     segment-recovered, which qs0d deliberately withheld. Nothing to rescue.
//   * Every row unanchored       ⇒ either the build could not stamp anchors, or
//     no slot earned one. Both are degraded outcomes, and both are exactly the
//     trap. Rescuable.
//
// That is a fact about persisted rows, not an inference about provenance, which
// is why the rescue can be bounded by it.

import Foundation

// MARK: - Freeze state

/// What an asset's day-0 marks are, as far as the rescue is concerned. Persisted
/// nowhere — it is DERIVED from the rows plus the attempt record, so it cannot
/// go stale — but it is the value the diagnostics surface reports, so
/// "permanently frozen" is a state you can query rather than an absence you have
/// to infer from silence.
enum DayZeroMarkFreezeState: String, Sendable, Equatable, CaseIterable, Codable {
    /// Day-0 has minted nothing for this asset. Not frozen; not rescuable
    /// either — an ordinary first attempt is what this asset wants.
    case noMarks = "no_marks"

    /// At least one day-0 mark carries `.rediffByteExact` on both edges. The
    /// attempt reached full quality on the slots that earned it; any unanchored
    /// sibling is provably segment-recovered and is WITHHELD on purpose.
    case anchored = "anchored"

    /// Every day-0 mark is degraded and a rescue re-attempt is still available.
    case rescuable = "rescuable"

    /// Every day-0 mark is degraded and the rescue budget for this generation is
    /// spent. THE state this bead exists to make visible: the asset will banner
    /// and never auto-skip until `DayZeroRediffAttemptPolicy.currentGeneration`
    /// is bumped again.
    case frozen = "frozen"

    /// Every day-0 mark has been settled — vetoed, dismissed, applied or
    /// confirmed. Deliberately NOT `frozen`: nothing is being withheld from the
    /// user here, the user (or the pipeline) decided, and the fidelity ladder
    /// puts that above anything a re-fetch could derive.
    case settled = "settled"
}

// MARK: - Census

/// One asset's day-0 `ad_windows` rows, counted by the only distinction that
/// governs the rescue: whether the row carries a byte-exact anchor on BOTH
/// edges.
///
/// Pure and total over the rows; no store, no clock.
struct DayZeroMarkCensus: Sendable, Equatable {

    /// Day-0 byte-exact rows anchored `.rediffByteExact` on BOTH edges — a
    /// playhead-qs0d strict mint.
    ///
    /// Counted **whether or not the row was later settled**, and that ordering
    /// is load-bearing rather than tidy. This count is the evidence behind
    /// "the mint that produced these marks COULD stamp an anchor", which is what
    /// makes the unanchored siblings provably segment-recovered. A user vetoing
    /// the anchored row does not un-prove it — bucketing it as settled would
    /// read the asset back as rescuable and re-derive exactly the 9s6q slots
    /// playhead-qs0d withheld.
    let anchoredCount: Int

    /// Day-0 byte-exact rows carrying anything less on either edge AND still
    /// unsettled — precisely the rows a re-mint would be ALLOWED to supersede
    /// (see `isSupersedable`).
    let degradedCount: Int

    /// Day-0 byte-exact rows that are NOT fully anchored and that the user or
    /// the pipeline has settled — vetoed (`.reverted`), confirmed, already
    /// skipped, or banner-dismissed. Counted separately from `degradedCount`
    /// because they are off limits: the fidelity ladder puts a user gesture
    /// above anything a re-fetch can derive, so they are never superseded and
    /// never make an asset rescuable.
    let settledCount: Int

    static let empty = DayZeroMarkCensus(
        anchoredCount: 0,
        degradedCount: 0,
        settledCount: 0
    )

    /// Any day-0 mark at all on disk. The precondition for the rescue CEILING —
    /// an asset with marks may spend at most `maxRescueAttempts` further
    /// fetches per generation, whatever exit its last attempt took.
    var hasMarks: Bool {
        anchoredCount > 0 || degradedCount > 0 || settledCount > 0
    }

    /// May a re-attempt plausibly IMPROVE this asset?
    ///
    /// Requires BOTH legs and the conjunction is the whole safety argument:
    ///
    ///   * `degradedCount > 0` — there is something to improve. An asset with no
    ///     day-0 rows is not a rescue case (it is a first attempt), and an asset
    ///     whose only degraded rows are user-settled must not be touched.
    ///   * `anchoredCount == 0` — nothing on this asset proves the mint could
    ///     already stamp anchors. One anchored row is proof that it could, which
    ///     makes every unanchored sibling a deliberate qs0d withholding rather
    ///     than a lost one, and re-fetching would spend ~108 MB to re-derive the
    ///     same classification.
    var isRescuable: Bool {
        anchoredCount == 0 && degradedCount > 0
    }

    /// The reportable state, given how much of the rescue budget this asset has
    /// already spent. Pure; `rescueAttemptCount` comes from
    /// `RediffDayZeroAttemptRecord`.
    func freezeState(rescueAttemptCount: Int) -> DayZeroMarkFreezeState {
        guard hasMarks else { return .noMarks }
        guard anchoredCount == 0 else { return .anchored }
        guard degradedCount > 0 else { return .settled }
        return rescueAttemptCount < DayZeroRediffAttemptPolicy.maxRescueAttempts
            ? .rescuable
            : .frozen
    }

    /// Count an asset's rows. Rows that are not day-0 byte-exact marks are
    /// IGNORED — the census is a statement about the day-0 producer only, so a
    /// hot-path or aggregator row can neither make an asset look rescuable nor
    /// stop it being rescued.
    static func classify(rows: [AdWindow]) -> DayZeroMarkCensus {
        var anchored = 0
        var degraded = 0
        var settled = 0
        // ANCHORED IS TESTED FIRST, deliberately — see `anchoredCount`. A
        // vetoed-but-anchored row still proves the mint could stamp an anchor,
        // and that proof is what withholds the rescue from its segment-recovered
        // siblings.
        for row in rows where isDayZeroByteExactMark(row) {
            if isFullyAnchored(row) {
                anchored += 1
            } else if isSettled(row) {
                settled += 1
            } else {
                degraded += 1
            }
        }
        return DayZeroMarkCensus(
            anchoredCount: anchored,
            degradedCount: degraded,
            settledCount: settled
        )
    }

    /// Whether this row was minted by the day-0 byte-exact path. Keyed on the
    /// SAME literal the mint stamps
    /// (`AdDetectionService.dayZeroRediffByteExactBoundaryState`), read through
    /// that constant rather than re-spelled, so the two cannot drift.
    static func isDayZeroByteExactMark(_ row: AdWindow) -> Bool {
        row.boundaryState == AdDetectionService.dayZeroRediffByteExactBoundaryState
    }

    /// Both edges byte-exact — the playhead-qs0d strict stamp, and the ONLY
    /// reading that counts as "this attempt reached full quality". A half-
    /// anchored row cannot be produced by the day-0 mint (it stamps one `anchor`
    /// value on both edges) but is treated as degraded if it ever appears,
    /// because a rescue that improves it is harmless and a promotion that
    /// assumes it is fine is not.
    static func isFullyAnchored(_ row: AdWindow) -> Bool {
        let support = row.extentSupport
        return support.startAnchor == .rediffByteExact
            && support.endAnchor == .rediffByteExact
    }

    /// A row nobody may supersede: the user vetoed it, dismissed its banner, it
    /// was already applied to the listener, or backfill settled it.
    ///
    /// `.candidate` is the only unsettled state — the same predicate
    /// `AnalysisStore.upsertHotPathAdWindows` enforces before it will retire an
    /// id, so a row this returns `true` for would be REFUSED by the store
    /// anyway. Checking it here means the mint declines to try, rather than
    /// throwing the whole rescue away on a `staleAdWindowRevision`.
    static func isSettled(_ row: AdWindow) -> Bool {
        row.decisionState != AdDecisionState.candidate.rawValue
            || row.wasSkipped
            || row.userDismissedBanner
    }

    /// May a re-mint REPLACE this row?
    ///
    /// Only its own producer's degraded, unsettled rows. Not another detector's
    /// row (a hot-path or aggregator window keeps its own life), not an already
    /// anchored one (there is nothing to improve and the geometry might be
    /// better than what this fetch found), and never a settled one.
    static func isSupersedable(_ row: AdWindow) -> Bool {
        isDayZeroByteExactMark(row) && !isSettled(row) && !isFullyAnchored(row)
    }

    // MARK: - The re-mint supersede rule (playhead-c7ef)

    /// May THIS re-mint slot be persisted, retiring the existing rows it
    /// collides with?
    ///
    /// THE PROBLEM THIS REPLACES. playhead-ug9m spelled the rule inline at the
    /// mint as `guard strict, overlapping.allSatisfy(isSupersedable)`, and the
    /// `strict` leg carried the whole "provably better" argument: a
    /// monotonic-clean chain proves its A-timeline mapping at every edge, so a
    /// strict row replacing a degraded one is an unambiguous upgrade. That leg
    /// stopped being the right question when playhead-pyq7 promoted 9s6q
    /// SEGMENT-RECOVERED slots — a FRESH recovered mint now stamps
    /// `.rediffByteExact` on both edges and auto-skips, while a recovered
    /// RE-mint over the same episode was still refused, so the nine
    /// `unanchored`/`markOnly` day-0 rows on the owner's device (db-pull10, build
    /// 4f6bd5d3) could never be improved by anything.
    ///
    /// Widening `strict` to `skipGrade` and stopping there would have thrown
    /// away the ONE hazard ug9m's comment actually named: *"a second, worse draw
    /// could retire two correct banners and mint one."* `strict` never addressed
    /// that either — it is a statement about the acceptance ARM, not about
    /// geometry — so this rule states it directly, in three conjuncts, and each
    /// one refuses a different downgrade.
    ///
    ///   1. `slotIsSkipGrade` — the replacement must itself earn
    ///      `.rediffByteExact` on both edges (`RediffSlotOwnership
    ///      .dayZeroSlotIsSkipGrade`). A mark-only re-mint changes NOTHING, as
    ///      before: the slot is dropped, the existing row survives, and the
    ///      attempt ends `.allSlotsAlreadyCovered`. Combined with conjunct 2 this
    ///      is what makes the swap a strict rise on the anchor ladder — the row
    ///      retired is by definition NOT fully anchored, the row minted always
    ///      is. With the pyq7 promotion switch OFF this conjunct is false for the
    ///      whole recovered arm and the rule collapses to ug9m's behaviour.
    ///   2. `overlapping.allSatisfy(isSupersedable)` — UNCHANGED. The fidelity
    ///      ladder is not negotiable by a re-fetch: a user veto, a dismissed
    ///      banner, an applied skip, another producer's window or an already
    ///      anchored row all still block the slot outright.
    ///   3. THE NEW ONE, and it is what makes "provably better" a theorem rather
    ///      than a claim about arms. **Exactly one row may be retired, and the
    ///      slot must lie inside the span that row already marked, to within the
    ///      auto-skip margins.** So:
    ///        * a slot that would swallow TWO existing marks plus the show
    ///          between them is refused — that is ug9m's own named hazard, and
    ///          the two banners survive;
    ///        * a slot that runs PAST the row it replaces is refused, so a
    ///          re-mint can never extend a cut into audio nothing had marked.
    ///
    /// WHY THE TOLERANCE IS THE PADDING MARGINS, and it is a derivation rather
    /// than a fudge. The minted row is `.rediffByteExact` on both edges, so
    /// `AutoSkipEdgePadding.skipWindow` cuts `[ss + 0.50, se - 0.75]`. Requiring
    /// `ss >= rs - 0.50` and `se <= re + 0.75` therefore gives
    /// `ss + 0.50 >= rs` and `se - 0.75 <= re`:
    ///
    ///   **every second this rescue makes auto-skippable was already marked as
    ///   an ad by the row it retires.** Not approximately — the padded cut is a
    ///   subset of `[rs, re]`, by construction.
    ///
    /// That is the whole safety argument, and note what it does NOT rest on: it
    /// does not require trusting the fresh draw's geometry, or the arm that
    /// produced it, or the two draws agreeing to any tolerance. A zero tolerance
    /// would have been the naive reading and is wrong in the *unsafe* direction
    /// of usefulness rather than safety — the measured inter-arm edge delta is
    /// +0.0002 s, so an exact containment test would refuse roughly half of all
    /// genuine upgrades over floating-point dust.
    ///
    /// SHRINKING IS ALLOWED, deliberately. A slot strictly inside the retired
    /// row cuts less than the row marked; the seconds it gives up were only ever
    /// bannered, never skipped. Refusing that direction would cost the fix and
    /// buy nothing — the exposure runs the other way.
    ///
    /// Pure and total. An EMPTY `overlapping` is `true`: there is nothing to
    /// retire, so nothing to refuse, and a first-listen mint reaches this with an
    /// empty list and is unaffected.
    static func reMintMayReplace(
        slotStartSeconds: Double,
        slotEndSeconds: Double,
        overlapping: [AdWindow],
        slotIsSkipGrade: Bool
    ) -> Bool {
        if overlapping.isEmpty { return true }
        guard slotIsSkipGrade else { return false }
        guard overlapping.allSatisfy(isSupersedable) else { return false }
        guard overlapping.count == 1, let row = overlapping.first else { return false }
        return slotStartSeconds >= row.startTime - AutoSkipEdgePadding.startMarginRediffByteExactSeconds
            && slotEndSeconds <= row.endTime + AutoSkipEdgePadding.endMarginRediffByteExactSeconds
    }
}

// MARK: - Attempt context

/// Everything `DayZeroRediffAttemptPolicy.decide` reads about one asset, as ONE
/// snapshot.
///
/// The two legs are read together rather than through two providers for the same
/// reason `DayZeroRediffTrigger.transportProvider` bundles reachability, Low
/// Data Mode and the cellular preference: the decision is a conjunction over
/// them, and sampling them at different instants lets a mint that lands between
/// the two reads produce a decision that was never true of any single state of
/// the database. Here that interleaving is not hypothetical — the mint writes
/// `ad_windows` and `rediff_day_zero_attempts` in that order.
struct DayZeroAttemptContext: Sendable, Equatable {
    /// The asset's day-0 attempt history, `nil` when day-0 has never run for it.
    let record: RediffDayZeroAttemptRecord?
    /// The asset's day-0 marks as they stand on disk.
    let markCensus: DayZeroMarkCensus

    init(record: RediffDayZeroAttemptRecord?, markCensus: DayZeroMarkCensus = .empty) {
        self.record = record
        self.markCensus = markCensus
    }

    /// "Day-0 has never touched this asset" — an ordinary first attempt. The
    /// value a caller with no store supplies, and the one that reproduces the
    /// pre-playhead-p70f "always attempt" behavior.
    static let never = DayZeroAttemptContext(record: nil, markCensus: .empty)
}

// MARK: - Surfacing

/// One asset's day-0 mark state, as reported by
/// `AnalysisStore.fetchDayZeroMarkFreezeReports` and carried into the
/// diagnostics bundle.
///
/// The `state` is COMPUTED, never stored: it is a reading of the rows plus the
/// rescue counter, so it cannot go stale against them the way a persisted label
/// could. (That is the same defect this bead is fixing one level down — a row
/// whose persisted `eligibilityGate` no longer reflects what the detector could
/// prove.)
struct DayZeroMarkFreezeReport: Sendable, Equatable {
    let analysisAssetId: String
    let census: DayZeroMarkCensus
    /// Rescue re-attempts spent in the record's generation. `0` when the asset
    /// has day-0 marks but no attempt row at all.
    let rescueAttemptCount: Int
    /// The last attempt's exit, `nil` when there is no attempt row.
    let lastExit: RediffDayZeroExit?
    /// The generation that wrote the record, `nil` when there is no attempt row.
    let policyGeneration: Int?

    init(
        analysisAssetId: String,
        census: DayZeroMarkCensus,
        rescueAttemptCount: Int = 0,
        lastExit: RediffDayZeroExit? = nil,
        policyGeneration: Int? = nil
    ) {
        self.analysisAssetId = analysisAssetId
        self.census = census
        self.rescueAttemptCount = rescueAttemptCount
        self.lastExit = lastExit
        self.policyGeneration = policyGeneration
    }

    var state: DayZeroMarkFreezeState {
        census.freezeState(rescueAttemptCount: rescueAttemptCount)
    }
}
