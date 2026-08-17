// AdLikelihoodScanOrder.swift
// playhead-lxkq: order the FM coarse sweep by ad-likelihood, not by episode order.
//
// # The measurement this exists for
//
// Episode DE0784D8, 2026-08-01 device pull. FM's 42 `semantic_scan_results`
// rows cover 0–2676 s, LINEARLY, front to back, ending
// `2581-2676 | abstain | cancelled`. Dan's missed pod is at 2838–2954 s. FM had
// roughly fifteen hours between download and his listen and spent the entire
// budget sweeping the first 48% of the episode in episode order.
//
// At 2.4x slower than realtime, FM covers ten 3-minute neighbourhoods in ~72
// minutes of wall clock — which fits an overnight gap easily. The 3.7-hour
// full-episode linear sweep does not. **The budget was never the problem; the
// ORDER was.**
//
// On that same episode the acoustic seam channel fired fragments at 2667–2702
// and **2828–2836** — immediately at the missed pod's edges. The seam channel
// found the seams and the pipeline emitted them as junk verdicts instead of
// using them as pointers. That is `feedback_lexical_as_attention` exactly:
// brittle signals say WHERE to look, never the verdict. A 6.7e-6-confidence
// fragment is a terrible answer and an excellent pointer.
//
// # What this is (and deliberately is not)
//
// A **permutation**, not a filter. `order(_:seeds:)` returns exactly the plans
// it was given, in a different sequence. That is the whole safety argument for
// the "seeding must not starve the rest of the episode" requirement: total work
// is bit-for-bit identical, only the sequence changes, so no window can be
// starved by construction rather than by policy. It is also why there is no
// budget or threshold here — a window that is not promoted is not dropped, it
// is simply visited later, in its original episode position.
//
// The fallback IS the linear sweep. With no seeds the output is the input,
// element for element (`AdLikelihoodScanOrder.order` is the identity function
// on an empty seed list) — the pre-lxkq behaviour, byte for byte.
//
// # Why the promoted prefix is capped
//
// Without a cap, a cue-dense episode makes every window "seeded" and the sort
// degenerates into "reorder the whole episode by score" — which scatters the
// sweep, and (worse) destroys `BackfillJobRunner`'s honest resume cursor, whose
// value is the end of the CONTIGUOUS successfully-scanned prefix in time order.
// Capping the promoted prefix means at most `maxPromotedAudioSeconds` of audio
// is moved; everything after it stays in strict episode order, so the resume
// prefix grows normally as soon as the filler starts.
//
// The cap is expressed in SECONDS OF AUDIO rather than a window count because
// seconds is the unit the FM budget is actually denominated in: coarse windows
// on device range from 17 s to 1,183 s, so "ten windows" is not a budget and
// "thirty minutes" is. 1,800 s is the bead's arithmetic — ten 3-minute
// neighbourhoods, ~72 minutes of FM wall clock at 2.4x realtime.
//
// # What is NOT seeded here, and why
//
// The bead lists position priors (mid-episode break structure) and chapter cues
// alongside the measured channels. Neither is implemented:
//
//   * A **synthetic position prior** would fire on every episode, which makes
//     the "no seeds ⇒ linear sweep" fallback unreachable and its test vacuous.
//     A prior is a guess about where ads are; this file's contract is that a
//     seed is something a detector MEASURED. Adding an always-on prior is a
//     calibration decision with its own measurement, not a scheduling one.
//   * **Chapter cues** are gated behind `ChapterSignalMode`, whose production
//     default is `.off` (see `AdDetectionService.resolveChapterEvidenceForShadowPhase`
//     — the resolver returns nil for `.off` and `.shadow`). Seeding from a
//     channel that is dark in production would be code no field run can
//     exercise.
//
// Both are additive later: `Seed.Kind` is the extension point and the weights
// table is the only thing that needs a new row.

import Foundation

/// One measured pointer at a region of the episode worth looking at FIRST.
///
/// A seed is never a verdict. It carries no disposition and no confidence in
/// "this is an ad" — only `strength`, which is the originating detector's own
/// confidence that SOMETHING happened here (a seam, a brand mention, a cue
/// cluster). The consumer treats it strictly as attention.
struct AdLikelihoodSeed: Sendable, Equatable {

    /// Which channel produced the pointer. Ranked by how directly the channel
    /// localises an ad EDGE, which is what makes a neighbourhood worth
    /// spending an FM window on.
    enum Kind: String, Sendable, Equatable, CaseIterable {
        /// `AcousticBreakDetector` seam — energy drop / spectral spike / pause
        /// cluster. Highest weight because a seam localises an ad BOUNDARY to
        /// within a second or two, which is exactly the coordinate a
        /// neighbourhood needs. This is the channel that fired at 2828–2836 on
        /// DE0784D8 and was thrown away.
        case acousticSeam
        /// `EvidenceCatalog` entry — a brand span, URL, promo code or
        /// disclosure the deterministic extractor found in the transcript.
        case evidenceAnchor
        /// `LexicalScanner` candidate — a sponsor/CTA cue cluster. Lowest
        /// weight of the three: the widest and least edge-localising, and the
        /// one most prone to firing inside show content.
        case lexicalCue
        /// Neighbourhood of a rediff-confirmed DAI slot. Wired by a later
        /// bead; pod continuation already handles the immediately-adjacent
        /// case, this is the wider sweep around a confirmed slot.
        case rediffNeighbourhood
    }

    /// Start of the measured event, in episode audio seconds.
    let startTime: Double
    /// End of the measured event, in episode audio seconds.
    let endTime: Double
    let kind: Kind
    /// Originating detector's confidence that the EVENT occurred, clamped to
    /// `[0, 1]` by `init`. Never a confidence that the region is an ad.
    let strength: Double

    init(startTime: Double, endTime: Double, kind: Kind, strength: Double) {
        self.startTime = startTime
        self.endTime = endTime
        self.kind = kind
        self.strength = strength.isFinite ? min(max(strength, 0), 1) : 0
    }
}

/// Pure, `Sendable`, always-compiled reordering of a planned FM sweep so the
/// seeded neighbourhoods are visited first.
///
/// Deliberately generic over the plan type: the production consumer is
/// `FoundationModelClassifier.CoarsePassWindowPlan`, which lives behind
/// `canImport(FoundationModels)`-adjacent code, and keeping this file free of
/// that dependency is what lets the whole ordering policy be exercised by the
/// simulator FastTests with synthetic spans.
enum AdLikelihoodScanOrder {

    // MARK: - Tunables

    /// Half-width of the neighbourhood a seed opens around itself, in seconds.
    /// 90 s each side makes a point seam into the bead's 3-minute
    /// neighbourhood, and is what lets the 2828–2836 seam reach the 2838–2954
    /// pod that starts AFTER it.
    static let defaultNeighbourhoodRadiusSeconds: Double = 90

    /// Ceiling on the audio a promoted prefix may cover, in seconds. See the
    /// file header for why this is denominated in seconds and why it exists at
    /// all. 1,800 s ≈ ten 3-minute neighbourhoods ≈ 72 minutes of FM wall clock
    /// at 2.4x realtime.
    static let defaultMaxPromotedAudioSeconds: Double = 1_800

    /// Seeds wider than this are DROPPED, not clipped.
    ///
    /// `EvidenceEntry.coverageStartTime/coverageEndTime` span the first to the
    /// LAST occurrence of a brand, which on a show with a recurring sponsor is
    /// most of the episode. A seed that wide names no neighbourhood — it names
    /// the episode, and a "neighbourhood" covering the whole episode promotes
    /// everything, which is the same as promoting nothing. Dropping is the
    /// honest response: fewer seeds simply means more linear sweep.
    ///
    /// 300 s matches the width ceiling playhead-y3ya put on emitted sweep
    /// marks, for the same underlying reason — past that width the quantity has
    /// stopped describing a place.
    static let maxSeedWidthSeconds: Double = 300

    /// Relative weight per channel. Multiplied by the seed's own `strength`.
    /// Contributions from distinct seeds SUM (bounded-additive, the same shape
    /// `AcousticLikelihoodScorer.combine` and `AcousticFeatureFusion` use) so
    /// independent signals agreeing on a region reinforce each other rather
    /// than a single outlier taking all.
    static func weight(for kind: AdLikelihoodSeed.Kind) -> Double {
        switch kind {
        case .acousticSeam: 1.0
        case .rediffNeighbourhood: 1.0
        case .evidenceAnchor: 0.8
        case .lexicalCue: 0.6
        }
    }

    // MARK: - Ordering

    /// Reorder `plans` so that windows intersecting a seed neighbourhood are
    /// attempted first, highest-scoring first, and everything else follows in
    /// its original relative order.
    ///
    /// Contract, in the order the tests assert it:
    ///
    ///   1. **Permutation.** The result contains exactly the input elements,
    ///      once each. Nothing is dropped, nothing is duplicated.
    ///   2. **Identity on no seeds.** An empty (or entirely unusable) seed list
    ///      returns the input array unchanged — the pre-lxkq linear sweep.
    ///   3. **Promoted prefix.** Plans intersecting a seed neighbourhood are
    ///      moved to the front, ordered by descending score.
    ///   4. **Deterministic ties.** Equal scores break by earlier `start`, then
    ///      by original index. The order is a pure function of its inputs;
    ///      there is no clock, no random source and no set iteration in it.
    ///   5. **Bounded promotion.** The promoted prefix stops once admitting the
    ///      next plan would carry the prefix past `maxPromotedAudioSeconds`,
    ///      except that a non-empty seeded set always promotes at least one
    ///      plan (a single 1,183 s device window must not be excluded by a
    ///      1,800 s budget it fits inside).
    ///   6. **Filler is episode order.** Everything not promoted keeps its
    ///      original relative order.
    ///
    /// - Parameters:
    ///   - plans: the planned windows, in the order the pass would otherwise
    ///     attempt them (episode order, as `planPassA` emits them).
    ///   - seeds: measured pointers. Empty is the normal, expected case.
    ///   - span: extracts `(start, end)` in episode audio seconds from a plan.
    static func order<Plan>(
        _ plans: [Plan],
        seeds: [AdLikelihoodSeed],
        radiusSeconds: Double = defaultNeighbourhoodRadiusSeconds,
        maxPromotedAudioSeconds: Double = defaultMaxPromotedAudioSeconds,
        span: (Plan) -> (start: Double, end: Double)
    ) -> [Plan] {
        guard !plans.isEmpty else { return plans }
        let neighbourhoods = self.neighbourhoods(from: seeds, radiusSeconds: radiusSeconds)
        guard !neighbourhoods.isEmpty else { return plans }

        // Score every plan against every neighbourhood. Both lists are small
        // (tens of windows, tens of seeds) so the quadratic pass is cheaper
        // than the interval tree it would take to avoid it, and it is pure CPU
        // paid once per job against an FM pass that costs minutes.
        var scored: [(index: Int, score: Double, start: Double, duration: Double)] = []
        scored.reserveCapacity(plans.count)
        for (index, plan) in plans.enumerated() {
            let (start, end) = span(plan)
            guard start.isFinite, end.isFinite else { continue }
            let lo = min(start, end)
            let hi = max(start, end)
            var score = 0.0
            for neighbourhood in neighbourhoods where neighbourhood.lo < hi && neighbourhood.hi > lo {
                score += neighbourhood.score
            }
            guard score > 0 else { continue }
            scored.append((index, score, lo, hi - lo))
        }
        guard !scored.isEmpty else { return plans }

        // Descending score; ties to the earlier window, then to the earlier
        // original index. The final index term is what makes the sort a total
        // order — without it two identically-scored, identically-positioned
        // windows would order arbitrarily and the pass would stop being
        // reproducible run to run.
        scored.sort { lhs, rhs in
            if lhs.score != rhs.score { return lhs.score > rhs.score }
            if lhs.start != rhs.start { return lhs.start < rhs.start }
            return lhs.index < rhs.index
        }

        var promotedIndices: [Int] = []
        var promotedSeconds = 0.0
        for candidate in scored {
            let duration = max(candidate.duration, 0)
            if !promotedIndices.isEmpty,
               promotedSeconds + duration > maxPromotedAudioSeconds {
                break
            }
            promotedIndices.append(candidate.index)
            promotedSeconds += duration
        }

        let promotedSet = Set(promotedIndices)
        var result: [Plan] = []
        result.reserveCapacity(plans.count)
        for index in promotedIndices { result.append(plans[index]) }
        for (index, plan) in plans.enumerated() where !promotedSet.contains(index) {
            result.append(plan)
        }
        return result
    }

    // MARK: - Neighbourhoods

    /// A seed's neighbourhood: the span it opens, and the weighted score any
    /// window intersecting it earns.
    struct Neighbourhood: Sendable, Equatable {
        let lo: Double
        let hi: Double
        let score: Double
    }

    /// Expand usable seeds into scored neighbourhoods.
    ///
    /// Unusable seeds are dropped silently and individually — a NaN start, an
    /// infinite end, a zero-strength pointer, or a span wider than
    /// `maxSeedWidthSeconds`. Dropping one seed never invalidates the others,
    /// and dropping all of them lands on the linear-sweep fallback, which is a
    /// correct answer rather than a degraded one.
    static func neighbourhoods(
        from seeds: [AdLikelihoodSeed],
        radiusSeconds: Double = defaultNeighbourhoodRadiusSeconds
    ) -> [Neighbourhood] {
        let radius = radiusSeconds.isFinite ? max(radiusSeconds, 0) : 0
        return seeds.compactMap { seed in
            guard seed.startTime.isFinite, seed.endTime.isFinite else { return nil }
            let lo = min(seed.startTime, seed.endTime)
            let hi = max(seed.startTime, seed.endTime)
            guard hi - lo <= maxSeedWidthSeconds else { return nil }
            let score = weight(for: seed.kind) * seed.strength
            guard score > 0 else { return nil }
            return Neighbourhood(lo: lo - radius, hi: hi + radius, score: score)
        }
    }

    // MARK: - Seed derivation

    /// Build the seed list for an episode from the channels the FM backfill
    /// runner already has in hand.
    ///
    /// Every input here is ALREADY computed on the full-coverage path today.
    /// `acousticBreaks` in particular is detected in
    /// `AdDetectionService.runShadowFMPhase`, threaded through
    /// `BackfillJobRunner.AssetInputs`, and then — because
    /// `BackfillJobRunner.narrowedInputs` returns `rootInputs` unread whenever
    /// the policy is not `targetedWithAudit` — never looked at. This function
    /// is the first consumer of that field on the full-coverage path.
    ///
    /// - Note: evidence anchors use the entry's own `startTime`/`endTime`, NOT
    ///   `coverageStartTime`/`coverageEndTime`. The coverage span runs from the
    ///   first to the last occurrence of a brand and is routinely episode-wide;
    ///   as a POINTER that is useless, and `maxSeedWidthSeconds` would drop it
    ///   anyway.
    ///
    ///   This note used to add that `SpecialistScanPlanner` reads the coverage
    ///   span legitimately, "because it scores anchor DENSITY, where a wide
    ///   anchor contributes uniformly and is harmless". **That was wrong and it
    ///   is why playhead-x7rk exists.** The claim holds for the score and not
    ///   for the MERGE that runs before it: there, one episode-wide anchor is
    ///   not one uniform vote, it is one region swallowing every other anchor.
    ///   That site now anchors one mention at a time via
    ///   ``EvidenceEntry/anchorableOccurrences``. Do NOT read that as "no
    ///   consumer reads the hull any more" — playhead-rty3's grep found five
    ///   production readers and three are still open: playhead-0u3e (the fusion
    ///   ledger's weight), playhead-1prw (the dormant B9 planner) and
    ///   playhead-4grq (a 3+ repeat read as two endpoints).
    ///
    ///   Seeds here still use only the REPRESENTATIVE occurrence, so a repeat's
    ///   later mentions seed nothing. That UNDER-reads a repeat rather than
    ///   over-reading it, which is the safe direction for a pointer, and it is
    ///   deliberately outside playhead-x7rk's scope.
    static func seeds(
        acousticBreaks: [AcousticBreak],
        evidenceCatalog: EvidenceCatalog?,
        lexicalCandidates: [LexicalCandidate]
    ) -> [AdLikelihoodSeed] {
        var seeds: [AdLikelihoodSeed] = []
        seeds.reserveCapacity(
            acousticBreaks.count + (evidenceCatalog?.entries.count ?? 0) + lexicalCandidates.count
        )
        for brk in acousticBreaks {
            seeds.append(AdLikelihoodSeed(
                startTime: brk.time,
                endTime: brk.time,
                kind: .acousticSeam,
                strength: brk.breakStrength
            ))
        }
        for entry in evidenceCatalog?.entries ?? [] {
            seeds.append(AdLikelihoodSeed(
                startTime: entry.startTime,
                endTime: entry.endTime,
                kind: .evidenceAnchor,
                // The catalog carries no per-entry confidence — an entry exists
                // because a deterministic extractor matched. Full strength for
                // the channel, with the channel's own weight doing the ranking.
                strength: 1.0
            ))
        }
        for candidate in lexicalCandidates {
            seeds.append(AdLikelihoodSeed(
                startTime: candidate.startTime,
                endTime: candidate.endTime,
                kind: .lexicalCue,
                strength: candidate.confidence
            ))
        }
        return seeds
    }

    // MARK: - Order restoration

    /// Stable re-sort of `items` back into ascending `key` order.
    ///
    /// The pass ATTEMPTS windows in promoted order but REPORTS `coarse.plans`
    /// in episode order: `BackfillJobRunner` reads that list as its coverage
    /// DENOMINATOR and takes `unattemptedPlans.first` as "where the pass
    /// stopped", and both are questions about the EPISODE, not about attempt
    /// sequence.
    ///
    /// `coarse.windows` is deliberately NOT normalised, and it is the one place
    /// the reordering stays visible past `planPassA`. It is appended as windows
    /// resolve, so it carries attempt order, and
    /// `FoundationModelClassifier.planAdaptiveZoom` emits one refinement plan
    /// per coarse window in iteration order — so passB attempts the promoted
    /// neighbourhoods first too. That is the intended direction (a pass that
    /// runs out of budget mid-refinement should have refined the LIKELY slots),
    /// and it is safe because every consumer of that list was checked and is
    /// order-indifferent: `detectedAdLineRefs` is a set union,
    /// `succeededPlanIndices` is a `Set`, `SemanticScanCoverage.compute` sorts
    /// its own ranges before merging, and `recordRandomAuditEvents` both sorts
    /// and is scoped to `.scanRandomAuditWindows`, a phase this bead never
    /// seeds. Checked, not assumed — a future consumer of `coarse.windows` that
    /// needs episode order must sort, not rely on it.
    ///
    /// Stability matters: one plan can produce several failure rows (a bounded
    /// permissive shrink recovers part of a window and leaves the rest a hole),
    /// and their relative order within the plan is the order they were
    /// discovered in.
    static func restoreOrder<Item>(_ items: [Item], by key: (Item) -> Int) -> [Item] {
        items.enumerated()
            .sorted { lhs, rhs in
                let lhsKey = key(lhs.element)
                let rhsKey = key(rhs.element)
                if lhsKey != rhsKey { return lhsKey < rhsKey }
                return lhs.offset < rhs.offset
            }
            .map(\.element)
    }
}
