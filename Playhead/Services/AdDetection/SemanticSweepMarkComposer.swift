// SemanticSweepMarkComposer.swift
// playhead-y3ya: a semantic `containsAd` verdict has standing ON ITS OWN.
//
// # The field case
//
// 2026-08-01, episode DE0784D8. `semantic_scan_results` records that the
// Foundation Model returned `containsAd` for 508–599 s and for 1604–1731 s.
// There is NO `ad_window` anywhere near either. On the one episode FM has ever
// partially scanned on Dan's phone it fired twice and produced zero
// user-visible output.
//
// # Why the verdict died
//
// FM evidence reaches fusion through exactly one door:
// `AdDetectionService.buildFMLedgerEntries` walks the asset's existing
// `DecodedSpan`s and adds weight to the ones a `containsAd` window OVERLAPS. It
// cannot CREATE a span, and nothing else can either — `RegionProposalBuilder`
// iterates `window.spans`, so a coarse window with no refined spans yields no
// observation, no region, no anchored atom, no `DecodedSpan`. A sweep-lane
// verdict with no narrow lexical/acoustic/catalog seed under it therefore
// contributes to nothing and is discarded. PRESENCE WITHOUT EXTENT WAS THROWN
// AWAY, which is the detection portfolio's stated policy — any signal fires →
// banner — violated in the semantic lane alone.
//
// # What this is
//
// The same shape as `SpecialistMarkComposer`: a PURE, always-compiled function
// from persisted scan rows + the asset's existing windows to MARK-ONLY
// `AdWindow`s. No store, no actor, no model, no FM coupling — so the extent
// policy is unit-testable on synthetic rows, and the marks route through the
// IDENTICAL markOnly/suggest path day-0 rediff marks use. There is no second
// surfacing path.
//
// # The extent policy, in the order the stages run
//
//   1. PRESENCE. Only `containsAd` rows whose status says the window was
//      actually examined, and which are not playhead-pz32 no-work sentinels.
//      `noAds` / `uncertain` / `abstain` produce nothing — a verdict FM
//      declined must stay declined. This is deliberately NOT a lowered
//      threshold: nothing else in the pipeline changes admission, and the
//      near-zero-confidence acoustic population is untouched.
//
//   2. REFINE with pass B, where it has already been paid for. A `passB` row
//      is the model's OWN narrowing of a `passA` window, and
//      `BackfillJobRunner.makePassBScanResult` already projects its refined
//      spans back to SECONDS. Where such a row exists and still says
//      `containsAd`, it is the extent. Where pass B ran and DECLINED, the
//      coarse presence verdict stands: pass A said an ad is here, pass B
//      failed to localize it, and a failure to localize is not a retraction
//      (playhead-ynmk — a confirmation asserts presence, never extent). This
//      matters concretely because the pass-B writer persists an empty result
//      as `.noAds`.
//
//      WHAT IT STANDS AT CHANGED IN playhead-shu5, and the correction is one
//      sentence: a declined pass-B row's OWN WINDOW is not nothing. The
//      refinement planner builds it out of `focusLineRefs` — the coarse row's
//      `supportLineRefs`, expanded to `minimumZoomSpanLines` — and the writer
//      persists `windowSegments.map(\.startTime).min()/max()` for exactly the
//      case where the model returned no spans, so that a row "can say where we
//      looked". That is the coarse row's own localisation, projected into
//      seconds by the code that owned the segments, at the row's own
//      transcript version. Stage 6 reads it. The presence verdict still
//      stands; it stands over the seconds the model pointed at rather than
//      over the whole ~95 s tile it was handed.
//
//   3. MERGE. The sweep tiles ~95 s windows front to back, so one 3-minute pod
//      lands across two of them. Two touching banners for one ad break is a
//      worse surface than one. Bounded by `mergeGapSeconds` so two genuinely
//      separate breaks never fuse across the show between them — and, since
//      playhead-shu5, BARRED outright by any window a presence-pass row
//      EXAMINED AND DID NOT AFFIRM. `mergeGapSeconds` bounds a gap by its
//      SIZE; a cleared window is a gap somebody looked at and said no to, and
//      no size makes that absorbable.
//
//   4. CLIP to a PROVEN edge when one is available, and NEVER require one.
//      "Proven" is the same definition `SpanExtentSupport` uses — a
//      non-`unanchored` `AutoSkipEdgeAnchor` (`.rediffByteExact` /
//      `.stingerSnapped`) recorded on a persisted row. An anchor inside the
//      window and within `anchorClipRadiusSeconds` of an edge pulls that edge
//      in; absent anchors change NOTHING. Hard boundaries CLIP FM edges, they
//      must never GATE eligibility — that inversion is what produced this bug
//      (`feedback_fm_hostread_irreplaceable`).
//
//      HONEST NOTE ON HOW OFTEN THIS FIRES TODAY: stage 5 refuses to emit over
//      any existing window, and anchored edges live on existing windows, so
//      the anchored population and the orphan-verdict population are nearly
//      disjoint. The clip is here because CLIPPING is the correct relationship
//      between a hard boundary and an FM edge, not because it is load-bearing
//      on the field episode — where, measured, there was no anchor near either
//      verdict.
//
//   5. DEDUPE. Never emit over an existing window, in ANY `decisionState` —
//      including `.reverted`. That is the rule `mintByteExactDayZeroMarks` and
//      `correctionReplayCandidates` both already use, and it does two jobs
//      here. It keeps this producer strictly ADDITIVE to regions where the
//      pipeline produced nothing (the population the bead is defined over), so
//      "the surface was not re-flooded" is provable rather than argued. And it
//      stops a third producer undoing a user veto through a new door — Dan
//      vetoed the acoustic junk on this very episode five times.
//
//      THE LOSS THIS ACCEPTS, PINNED RATHER THAN PAPERED OVER: one stray
//      narrow window anywhere inside a 95-second coarse verdict suppresses the
//      whole mark. A coverage-fraction rule (`SpecialistMarkComposer`'s 0.70)
//      would keep it, at the cost of a second banner over an ad the pipeline
//      already found. Emitting only into genuine silence is the conservative
//      choice while this lane is new; revisiting it is a measurement, not a
//      guess. `anOverlappedVerdictProducesNothing` asserts the loss.
//
//   6. LOCALISE (playhead-shu5). THE EXTENT IS NOT THE SCAN WINDOW. Every
//      surviving extent is narrowed to the seconds the model itself named,
//      and it can only SHRINK — the whole pipeline above, dedupe included,
//      has already decided WHETHER this mark exists, so localisation decides
//      only HOW WIDE it is and can never admit a verdict the coarse geometry
//      suppressed. A row whose refs cannot be READ still keeps its whole
//      window: our records failed, not the model.
//
//      AND, SINCE playhead-my33, IT CAN NOW REMOVE A MARK — in exactly one
//      case, which is Dan's call of 2026-08-21. A row that named NOTHING
//      (`.absent`) contributes its window only when another row corroborates
//      the SAME WINDOW; when it is the sole backing, it contributes nothing
//      and the mark does not exist. So stage 6 is still shrink-only as a
//      geometry operation — every piece it emits is a subset of the extent it
//      was handed — and "shrink to nothing" is now a reachable answer.
//      See ``Localisation`` for why `.absent` and `.unreadable` are held
//      apart, ``corroborates(_:_:)`` for the predicate and why it is not
//      version-scoped, and ``contribution(of:in:supportLines:)`` for what the
//      rule cost, measured.
//
//      THE FIELD CASE, which is Dan's own correction of 2026-08-19. On
//      `CD2976E6` this composer marked [1510.4–1611.4] — 101.0 s — off a
//      window whose `spansJSON` reads
//      `{"certainty":"strong","supportLineRefs":[62]}`. The promotional
//      content in it is the LAST 9.5 s: *"What you just listened to was a most
//      replayed moment from a previous episode … check the description."* The
//      model was right and the boundary was wrong; 92 s of the guest talking
//      about climbing was inside a banner. Localised, that mark is
//      [1590.0–1611.4] and gives 79.6 s of show back.
//
//   7. EMIT. `eligibilityGate == .markOnly`; `decisionState == .candidate`;
//      BOTH edge anchors `.unanchored`; `metadataConfidence == nil` so the
//      banner copy is generic and no advertiser is hallucinated.
//      Content-addressed id, so a recompose over unchanged inputs is a true
//      no-op.
//
//      AND, SINCE playhead-6ruv, `evidenceSources` CARRIES WHETHER THE MODEL
//      NAMED AN ADVERTISER. Dan vetoed [1131.6–1210.9] on `CD2976E6` — the
//      guest saying *"my sponsored through the Northface … then I started
//      doing corporate speaking"*, graded `strong`, with no advertiser, no
//      product, no CTA, no URL and no promo code in it. Commercial VOCABULARY
//      read as commercial INTENT. The mark carried NOTHING that could have
//      said so, and could not have: the coarse schema is a disposition plus
//      two fields. The REFINEMENT pass answers exactly that question — a
//      `passB` `containsAd` row's `spansJSON` carries `commercialIntent`,
//      `ownership` and an `anchors` array naming a KIND (`url`, `brandSpan`,
//      `promoCode`) — and this composer read none of it. See ``Attribution``,
//      including what it does NOT do: it changes no confidence, no tier and
//      no geometry, and it does not touch Dan's mark, which has no refinement
//      under it and is `.unrefined` before and after.
//
//      playhead-mqqd: THE GATE IS DERIVED, NOT TYPED. It used to be a
//      hard-coded literal sitting beside two more hard-coded literals (the edge
//      anchors) with nothing enforcing that the three agreed — 24 of the 45
//      rows on the 2026-08-12 device pull, mark-only by constant rather than by
//      evidence. `makeMark` now reads ONE ``extentSupport`` value for the anchor
//      columns and hands the same value to ``ComposedMarkGate``. The computed
//      answer is `.markOnly`, identically, for every mark this lane can emit;
//      what changed is that the reason lives in ``extentSupport`` where a
//      measurement can revisit it.
//
//      `confidence` is DERIVED from the evidence under the extent — the model's
//      own `CertaintyBand`, the quality of the transcript it read, and whether
//      the presence-pass replicates that examined the same audio agreed. See
//      `maximumMarkConfidence` for what the constant it replaced could not say,
//      and `markConfidence(band:transcriptQuality:affirming:dissenting:)` for
//      the ladder. It is an ordinal EVIDENCE GRADE, not a probability, and it
//      is capped at the old constant so no mark is ever promoted by this.
//
//      CONSEQUENCE, STATED RATHER THAN ABSORBED (playhead-92im, Dan's call):
//      the old constant sat EXACTLY on `SkipOrchestrator`'s 0.70 preload floor,
//      so every sweep mark cleared it. A derived value clears it only at the
//      ceiling. Measured over all 22 sweep rows of the 2026-08-10 pull, graded
//      from the presence rows under each one — 19 of the 22 the composer
//      reproduces by geometry, the other 3 being stale marks whose backing rows
//      changed after they were written and which are graded on their persisted
//      extent: 6 stay at 0.70 and 16 fall below (12 of 22 at or above 0.525,
//      15 at or above 0.50, 16 at or above 0.40, minimum 0.047, 14 distinct
//      values), so those 16 would no longer hydrate on the next launch. That is
//      a REACH change and it is NOT decided here: `preloadConfidenceThreshold`
//      is playhead-atr3's settled territory and is untouched.
//
//      RE-MEASURED AFTER THE PERMISSIVE-BAND FIX, and the difference is why
//      the whole block is restated rather than patched: gating on
//      `ownershipInferenceWasSuppressed` (see ``certaintyFactor(of:)``) moves
//      every mark whose refinement was a permissive-bypass span, and 9 of the
//      pull's 11 refined spans are. The figures this comment carried before —
//      12 distinct values, 13 at or above 0.525, 20 at or above 0.40, both
//      vetoed marks at 0.467 — are the PRE-fix distribution and no longer
//      describe what the shipped code computes.
//
//      THE JUDGED MARKS, post-fix: 48E903D7 596.3–676.6 s grades 0.467 with 15
//      of 22 ABOVE it; F4CE7F47 590.0–679.4 s grades 0.233 with 17 above it —
//      they no longer share a grade, because only the second rests on a
//      permissive refinement. Both still sit in the lower half, which at an N
//      of 2 is the honest reading and NOT a claim that the grade separates good
//      marks from bad ones. What it does claim is that there is now something
//      in the number TO separate on.
//
//      THE OTHER LIVE READER, from the same grep:
//      `FinalPassRetranscriptionRunner.defaultConfidenceFloor` (0.50) on the
//      raw `confidence`. 7 of the 22 fall below it and would no longer earn a
//      final-pass re-transcription of their audio. Also Dan's, and arguably the
//      budget landing where the evidence is — but it is a change, so it is
//      named rather than absorbed.
//
//      THREE MORE READERS, found by the review round rather than by the first
//      grep. All move in the conservative direction, none is a safety
//      regression, and all are named here because absorbing them silently is
//      the thing this section exists to refuse:
//        * `TranscriptPeekViewModel.adConfidence` → the transcript overlay's
//          `AD %.0f%%` label. USER-VISIBLE: it read a constant "AD 70%" on
//          every sweep mark and now reads the mark's actual grade, as low as
//          "AD 5%" on the pull (min 0.046667).
//        * `CorrectionAttribution`'s `producerRevisionToken` hashes
//          `window.confidence`, so a sweep mark's suggest-card identity can now
//          CHANGE when a later scan re-grades it. Under the constant it never
//          could. See the id note below.
//        * A sweep mark the user CONFIRMS is re-minted at `suggested.confidence`
//          and then evaluated against the enter/stay thresholds. A confirmed
//          sweep mark used to clear both by construction and no longer does.
//
//      TWO 0.70 GATES THAT LOOK LIVE AND ARE NOT, checked rather than assumed:
//      `AnalysisJobRunner.isCueWindow` and the cross-user shareable-cue
//      predicate both ALSO require `SkipEligibilityGate.eligible`, and every
//      sweep mark computes to `.markOnly` (playhead-mqqd; a hard-coded literal
//      when this was written) — so neither has ever seen a sweep mark at any
//      confidence. A sweep mark is additionally local-only
//      (`isLocalOnlyBoundaryState`), so it is never exported.
//
//      THE ID STILL ADDRESSES GEOMETRY ONLY, and that is deliberate. A mark's
//      grade depends on the WHOLE row set — the corroboration term counts every
//      overlapping presence-pass replicate AT THE GRADED CLAIM'S OWN TRANSCRIPT
//      VERSION (playhead-kg6i; it used to count them at every version, which is
//      what that bead removed) — so a later scan can re-grade a
//      mark whose id is unchanged, and the store updates it in place. Hashing
//      the confidence into the id instead would mint a fresh row on every
//      re-screen and orphan the one the user is looking at, which is worse.
//      What is no longer true is that id-equal implies row-equal.
//
// # Why the downside is bounded
//
// Every mark is unanchored, so playhead-2350's unanchored-edge gate holds by
// construction — there is nothing for it to demote because nothing was ever
// promoted. Every mark is mark-only, so playhead-ynmk makes a confirmation a
// MARK rather than a cut. A wrong verdict costs a wrong banner. It never costs
// show.

import CryptoKit
import Foundation

/// Pure composer: persisted `semantic_scan_results` → mark-only `AdWindow`s.
/// No I/O; the caller supplies the rows and persists whatever this returns.
enum SemanticSweepMarkComposer {

    // MARK: - Provenance constants

    /// Detector version stamped on every sweep mark. `AdDetectionService`'s
    /// version-scoped reconcile (`reconcileVersionScopedMarkSets`) uses this
    /// exact string, so sweep marks and FM (`detection-v1`) / specialist
    /// (`specialist-ft-v2`) / pod-continuation (`pod-continuation-v1`) marks can
    /// never retire one another.
    static let detectorVersion = "semantic-sweep-v1"

    /// `metadataSource` stamped on every sweep mark. Paired with
    /// `metadataConfidence == nil` it guarantees the generic, no-hallucination
    /// banner copy.
    static let metadataSource = "semanticSweep"

    /// `boundaryState` stamped on every sweep mark. A NON-user literal that MUST
    /// stay OUT of `AdDetectionService.reconcileProtectedBoundaryStates` so the
    /// version-scoped reconcile can retire its own stale rows.
    static let boundaryState = "semanticSweepMark"

    // MARK: - Tunables

    /// The MOST a sweep mark can be worth (playhead-92im). Still exactly
    /// `SkipOrchestrator.preloadConfidenceThreshold`, and still for the reason
    /// the constant it replaces was pinned there — a mark at the ceiling clears
    /// the cross-launch preload floor. What changed is that it is now a CEILING
    /// rather than the value every mark receives.
    ///
    /// # Why the constant had to go
    ///
    /// It was minted for EVERY mark: 22 of 22 rows on the 2026-08-10 device pull
    /// read exactly 0.70, min and max. Apply the standing diagnostic — what
    /// would this number read if the evidence it claims to summarise had never
    /// existed? 0.70, always. A number with one value cannot separate a good
    /// mark from a bad one, so there was nothing to threshold on, and the two
    /// marks Dan has judged (F4CE7F47 590–679 s, 48E903D7 596–677 s, both
    /// vetoed) were indistinguishable from the rest of the population.
    ///
    /// The old comment's premise — "the coarse lane produces a categorical
    /// verdict, not a score" — was simply not true of the persisted rows. Every
    /// `containsAd` row carries a ``CertaintyBand`` in `spansJSON`
    /// (`CoarseSupportSchema.certainty` for `passA`, the refined spans' own
    /// bands for `passB`): 55 of 55 coarse and 11 of 11 refined rows on that
    /// pull, split 40 `strong` / 15 `moderate`. The composer simply never read
    /// it.
    ///
    /// NOT ALL OF THOSE BANDS ARE THE MODEL'S, and the difference matters more
    /// than the count: 9 of the 11 refined rows are permissive-bypass spans
    /// whose `strong` the RUNNER hardcoded. See ``certaintyFactor(of:)``, which
    /// reads them as ungraded.
    ///
    /// # It is an EVIDENCE GRADE, not a probability
    ///
    /// Nothing here is calibrated against ground truth and nothing pretends to
    /// be. It is an ordinal grade in `(0, 0.70]` that is MONOTONE in the
    /// evidence and can never exceed what the constant already claimed — see
    /// ``markConfidence(band:transcriptQuality:affirming:dissenting:)``.
    static let maximumMarkConfidence = 0.70

    /// What a mark reads when the model expressed NO certainty at all — an
    /// absent or undecodable support payload (`spansJSON == "[]"`, which
    /// `BackfillJobRunner.encodeSupport` writes for a nil `support`).
    ///
    /// It is the FLOOR of the band ladder, deliberately: an absence of evidence
    /// must never read like the presence of strong evidence. This is also the
    /// default carried by ``Extent``, so a future stage that forgets to attach
    /// evidence under-claims instead of silently minting the ceiling.
    static let unevidencedMarkConfidence =
        maximumMarkConfidence * certaintyFactor(nil)

    /// Coarse windows whose gap is at most this are one ad break, not two.
    /// The sweep tiles contiguous windows, so touching windows (gap 0) are the
    /// common case; the slack absorbs float drift at a shared boundary.
    static let mergeGapSeconds = 2.0

    /// Marks shorter than this are not emitted. Below
    /// `InventorySanityFilter`'s duration floor a mark cannot survive ingest,
    /// so emitting it would only add census rows naming a drop nobody can act
    /// on.
    static let minimumMarkDurationSeconds = 2.0

    /// Marks WIDER than this are not emitted either, and the merge refuses to
    /// grow past it.
    ///
    /// The coarse lane can return `containsAd` over enormous windows —
    /// `SpanExtentSupport`'s own header records FM windows of 17.04–1183.62 s
    /// on the THEMOVE replay. A verdict spanning nineteen minutes carries
    /// essentially NO extent information, and a banner over it claims show, not
    /// an ad. It is also nearly all show by construction, which is Dan's
    /// "inner edges eat the show" applied to a whole window. Dropping it is the
    /// honest answer: presence with no usable extent is a TARGETING problem
    /// (playhead-lxkq re-aims the budget at narrow likely slots), not something
    /// to put in front of a listener.
    ///
    /// The number is a POLICY bound, not a measurement, and it is stated as one.
    /// Its comparator is `quorumGateForFMConsensus`'s 180 s ceiling for AUTO-SKIP
    /// admission; a banner earns more latitude than a cut, so this is 300 s.
    /// Both field verdicts — 91 s and 127 s — clear it with wide margin.
    static let maximumMarkDurationSeconds = 300.0

    /// How far from an edge a proven boundary may sit and still clip it. An
    /// anchor further away than this belongs to something else, and snapping to
    /// it would INVENT extent — the failure playhead-2350 documented.
    static let anchorClipRadiusSeconds = 20.0

    /// The pass label whose rows are the model's own narrowing of a coarse
    /// window. Mirrors `BackfillJobRunner`'s literal; `SemanticScanCoverage`
    /// owns the `passA` counterpart.
    static let refinementScanPass = "passB"

    /// How far a LOCALISED span is widened at each edge before it becomes a
    /// mark (playhead-shu5).
    ///
    /// # This is a PRODUCT THRESHOLD and it is deliberately at zero
    ///
    /// It is named rather than absent so that moving it is an edit to one line
    /// with a reason attached, and it is **Dan's call**. Zero is the
    /// conservative default, and the argument for it is his own: *"if its an ad
    /// and i click yes i still lose part of the show"* — OUTER edges (an
    /// episode's very start and end) are cheap to widen, INNER edges eat the
    /// show, and every span this constant touches is an INNER edge by
    /// construction, because stage 6 only ever narrows something already
    /// inside a scan window.
    ///
    /// Two further reasons zero is the right place to start. A localised span's
    /// bounds are a SEGMENT's bounds, and a segment boundary is already a real
    /// event in the audio — `TranscriptSegmenter` breaks on a ≥ 1.5 s pause, a
    /// max-duration cut, a speaker turn or sentence punctuation — so it is not
    /// an arbitrary cut that needs slack around it. And the ONE thing a pad
    /// buys, catching an ad whose first words fall in a segment the model did
    /// not cite, is bought by under-marking a banner the listener still has to
    /// confirm, which is the cheap failure of the two.
    ///
    /// Applied symmetrically and then CLAMPED to the coarse window: a pad may
    /// never carry a mark outside the audio the model actually examined.
    static let supportLocalisationPadSeconds = 0.0

    // MARK: - Confidence (playhead-92im)
    //
    // THREE FACTORS, each of which reads its own NEUTRAL value when the thing
    // it measures is absent, so no factor can manufacture confidence out of
    // silence. Two of them can only deduct; none can ever raise a mark above
    // `maximumMarkConfidence`. The change this file makes is therefore MONOTONE
    // NON-INCREASING against the constant it replaces: no sweep mark is ever
    // promoted, and a detector that is 2-for-2 wrong on the judged episodes
    // cannot come out of this stronger than it went in.

    /// The model's own ``CertaintyBand``, as a fraction of the ceiling.
    ///
    /// THE LADDER IS BORROWED, NOT INVENTED. `AdDetectionService`'s
    /// `buildFMLedgerEntries` already maps this exact enum onto
    /// `fmCap * { 1.0, 0.75, 0.5 }` for `{ strong, moderate, weak }`. Same
    /// vocabulary, same ratios, anchored at this composer's own ceiling instead
    /// of `fmCap`, which makes the two lanes' readings of one FM verdict
    /// commensurable rather than two independent guesses.
    ///
    /// `nil` — the model returned no support object, or one that will not
    /// decode — reads the FLOOR, alongside `.weak`. A verdict that declined to
    /// grade itself is not a strong verdict.
    static func certaintyFactor(_ band: CertaintyBand?) -> Double {
        switch band {
        case .strong: 1.0
        case .moderate: 0.75
        case .weak, nil: 0.5
        }
    }

    /// Where a band sits on the "the WEAKEST claim governs" order.
    ///
    /// `nil` — the model did not grade this, or the grade on the payload was
    /// the RUNNER'S — sorts BELOW `.weak`, so a payload mixing an ungraded span
    /// with a graded `.weak` one reports the ABSENCE rather than a grade nobody
    /// gave. ``certaintyFactor(_:)`` reads the two alike (both 0.5), so this
    /// last rung is invisible to the confidence product and load-bearing only
    /// for a reader of the band itself — which is what keeps
    /// ``certaintyFactor(of:)`` byte-identical across the refactor that
    /// introduced ``certaintyBand(of:)``.
    private static func bandRank(_ band: CertaintyBand?) -> Int {
        switch band {
        case nil: 0
        case .weak: 1
        case .moderate: 2
        case .strong: 3
        }
    }

    /// How much the transcript the verdict was formed ON discounts it.
    ///
    /// ALSO BORROWED from `buildFMLedgerEntries`, which proxies a missing band
    /// out of quality alone — `good → .moderate` (0.75), `degraded → .weak`
    /// (0.5). Read as a ratio against `good`, that is `2/3` for `degraded`;
    /// `unusable` extends the SAME ladder one step further down (0.25 / 0.75).
    ///
    /// This is the factor the playhead-3gzp establish round predicted would
    /// matter, and the pull bears it out: both marks Dan vetoed rest on rows
    /// whose `transcriptQuality` is `degraded` — F4CE7F47 590–679 s on four
    /// `passA` replicates plus its `passB` refinement, all `degraded`, and
    /// 48E903D7 596–677 s likewise. The constant could not say so.
    ///
    /// There is no neutral-on-absence case to state: `transcriptQuality` is
    /// non-optional on the row and the switch is total.
    static func transcriptQualityFactor(_ quality: TranscriptQuality) -> Double {
        switch quality {
        case .good: 1.0
        case .degraded: 2.0 / 3.0
        case .unusable: 1.0 / 3.0
        }
    }

    /// Laplace-smoothed agreement among the PRESENCE-pass replicates that
    /// examined this extent: `(1 + affirming) / (1 + affirming + dissenting)`.
    ///
    /// The parenthesis that used to cite a distinct `runCorrelationId` here as
    /// evidence that those replicates are independent was WRONG. The
    /// measurement that retired it lives at
    /// ``corroboration(for:in:atTranscriptVersion:)``, which is where the
    /// replicate population is selected. The independence itself is unchanged
    /// (playhead-1gu0).
    ///
    /// SAY WHICH ROWS THIS FACTOR CAN SEE, because the four replicate windows
    /// measured at ``corroboration(for:in:atTranscriptVersion:)`` are not it. A
    /// row that did not EXAMINE its window is not a verdict and is skipped
    /// THERE, not here — this function is three lines of arithmetic and filters
    /// nothing. (It read "skipped below", which has pointed at nothing since
    /// `6e9c386f`; playhead-1gu0 review.) So on the 2026-08-10 pull only ONE of
    /// the 55 coarse `containsAd` rows has an examined dissenter over it, and
    /// only 3 of the 22 persisted sweep marks are deducted at all. The factor is
    /// right; its reach today is small, and quoting the wider count as if this
    /// read it would overstate it.
    ///
    /// **THOSE TWO FIGURES ARE PRE-kg6i AND THE FIRST OVERSTATES THE REACH —
    /// read them as history (playhead-1gu0 review).** They landed on 2026-08-10
    /// (`6e9c386f`); kg6i scoped the count to one `transcriptVersion` on
    /// 2026-08-21 (`0f0e9cb1`). Re-derived on the same pull with the same
    /// predicate — an EXAMINED presence-pass row overlapping the coarse row's
    /// own window — the un-scoped query reproduces **55** and **1** exactly,
    /// and adding kg6i's version scope takes the 1 to **0**. That direction is
    /// exact rather than approximate: a lone coarse row's extent IS its window
    /// and a narrowed one is a subinterval of it, so **no coarse backing row on
    /// the 08-10 pull contributes a dissent at its own version.**
    ///
    /// DO NOT READ THAT AS "the factor never deducts" — that is the mirror
    /// error, and it is false. The other kind of backing row
    /// ``scored(start:end:restingOn:in:)`` passes here is a `passB` refinement,
    /// and the same version-scoped query over the ELEVEN `passB` `containsAd`
    /// rows finds a same-version dissenter over **6 of the 11** — an UPPER
    /// bound, exact for an ORPHAN refinement and too wide for a narrowed pair,
    /// whose extent is the intersection. So on this pull deduction is reachable
    /// through refinements and not through coarse replicates. (2026-08-19 t4,
    /// same queries: 3 of 301 coarse and at most 3 of 53 `passB`.) The
    /// "3 of the 22 marks deducted" figure is from the same pre-kg6i run and
    /// has NOT been re-derived; correcting it needs a composer run rather than
    /// a query, so it is left to **playhead-57ern**, filed for exactly that.
    ///
    /// IT CAN ONLY DEDUCT, AND THAT IS THE POINT. The smoothing means
    /// unanimity returns exactly 1.0 whether the window was screened once or
    /// four times, so a repeat that agrees adds nothing — one scan is the
    /// baseline, and only CONTRADICTION moves the number. A plain
    /// `affirming / (affirming + dissenting)` would have read 1.0 for a single
    /// uncontradicted scan too, but by way of "unanimous", which is the
    /// absence-reads-as-corroboration shape this bead exists to remove.
    ///
    /// LIMIT, stated rather than papered over: a `noAds` denial and an
    /// `uncertain` declination are counted alike, because that is exactly how
    /// stage 1 already treats them at ADMISSION. Splitting them is a
    /// measurement, not a guess, and is not made here.
    ///
    /// WHICH ROWS ARE REPLICATES IS NOT THIS FUNCTION'S QUESTION — it is
    /// ``corroboration(for:in:atTranscriptVersion:)``'s, and playhead-kg6i
    /// answered it wrong until now. The arithmetic here is unchanged.
    static func corroborationFactor(affirming: Int, dissenting: Int) -> Double {
        let supporting = Double(max(0, affirming))
        let opposing = Double(max(0, dissenting))
        return (1 + supporting) / (1 + supporting + opposing)
    }

    /// THE derived confidence. One product, three named factors, and a hard
    /// ceiling that no input combination can beat.
    static func markConfidence(
        band: CertaintyBand?,
        transcriptQuality quality: TranscriptQuality,
        affirming: Int,
        dissenting: Int
    ) -> Double {
        markConfidence(
            certaintyFactor: certaintyFactor(band),
            transcriptQuality: quality,
            affirming: affirming,
            dissenting: dissenting
        )
    }

    /// The same product, taking the certainty term already resolved — which is
    /// how a ROW reaches it, since a row's term can come from several spans and
    /// is not one band.
    static func markConfidence(
        certaintyFactor certainty: Double,
        transcriptQuality quality: TranscriptQuality,
        affirming: Int,
        dissenting: Int
    ) -> Double {
        maximumMarkConfidence
            * certainty
            * transcriptQualityFactor(quality)
            * corroborationFactor(affirming: affirming, dissenting: dissenting)
    }

    /// One entry of `BackfillJobRunner.EncodedAnchor`, decoded down to the only
    /// field this composer reads: WHAT KIND of thing the model pointed at.
    ///
    /// `kind` is decoded as a RAW STRING and mapped afterwards, deliberately.
    /// A typed `EvidenceAnchorKind` THROWS on a raw value it does not know, and
    /// a throw here takes the WHOLE payload with it — every dimension of every
    /// span on the row would read as unreadable because one anchor was new.
    /// An unrecognised kind is one anchor we cannot name, which is what it
    /// actually is, so that is what it decodes to.
    private struct PersistedAnchor: Decodable {
        let kind: String?

        var anchorKind: EvidenceAnchorKind? {
            kind.flatMap(EvidenceAnchorKind.init(rawValue:))
        }
    }

    /// Minimal decode of the keys that say WHETHER THE MODEL JUDGED THIS and
    /// what it judged. Deliberately reads only those, so the two payload shapes
    /// and every future field they gain decode without this composer knowing
    /// about them — the same narrow-decode pattern `AdDetectionService`'s
    /// `SpanTrustDecode` uses over the same column.
    ///
    /// It is named for the first fact it carried and it now carries three more
    /// (playhead-6ruv). They live in ONE struct rather than two on purpose:
    /// every one of them is gated on the same `ownershipInferenceWasSuppressed`
    /// flag, and a second decoder over this column would be a second place for
    /// that gate to drift — which is exactly what playhead-yx0f consolidated
    /// away for the band.
    private struct PersistedCertainty: Decodable {
        let certainty: CertaintyBand?
        /// `BackfillJobRunner.EncodedRefinedSpan`'s flag, absent on the coarse
        /// payload and on legacy rows. See ``certaintyFactor(of:)`` for why a
        /// consumer of `certainty` must read it.
        let ownershipInferenceWasSuppressed: Bool?
        /// playhead-shu5: WHICH TRANSCRIPT LINES the model said its verdict
        /// rests on. Present on the `passA` (`CoarseSupportSchema`) shape only;
        /// the refined-span array carries per-span line refs under other names
        /// and is read through its own path. See ``supportLineRefs(of:)``.
        let supportLineRefs: [Int]?
        /// playhead-6ruv: paid / owned / affiliate / organic / unknown, and
        /// thirdParty / show / network / guest / unknown. Refined-span shape
        /// only. Raw strings for the reason ``PersistedAnchor/kind`` is one.
        let commercialIntent: String?
        let ownership: String?
        /// playhead-6ruv: WHAT the model pointed at inside this span — a url,
        /// a brandSpan, a promoCode, each anchored to a line.
        let anchors: [PersistedAnchor]?

        /// The band this REFINED SPAN may CONTRIBUTE — `nil` when the model
        /// did not grade it, and `nil` when the runner did the grading.
        ///
        /// The discriminator travels WITH the span
        /// (`BackfillJobRunner.EncodedRefinedSpan.ownershipInferenceWasSuppressed`),
        /// so this reads correctly on a row of any vintage, including every row
        /// written before playhead-iw7q added the column below. That is the
        /// asymmetry ``coarseAttributableBand(rowProvenance:)`` exists for.
        var attributableBand: CertaintyBand? {
            ownershipInferenceWasSuppressed == true ? nil : certainty
        }

        /// The band a COARSE (`CoarseSupportSchema`) payload may CONTRIBUTE —
        /// `nil` unless the ROW says the MODEL produced this verdict
        /// (playhead-iw7q).
        ///
        /// # Why the coarse shape needs the row and the refined shape does not
        ///
        /// `CoarseScreeningSchema` is a disposition plus a support object with
        /// exactly two fields, and neither of them is a provenance. There is no
        /// per-span flag here to read, so the row's own
        /// ``SemanticScanResult/verdictProvenance`` is the entire record —
        /// which is why `usedPermissiveFallback` had to become a COLUMN before
        /// this gate could exist at all, and why the header of
        /// ``certaintyFactor(of:)`` said for two beads that this half was not
        /// detectable at rest.
        ///
        /// # UNKNOWN IS NOT ZERO
        ///
        /// `.unknown` — every row written before schema V61 — returns `nil`,
        /// exactly as `.permissive` does, and for a DIFFERENT reason: one is
        /// *"the runner graded this"* and the other is *"nobody recorded who
        /// graded this"*. They agree on what may be SPENT and disagree on what
        /// is KNOWN, which is why the two states are kept apart on the row and
        /// collapsed only here, at the point of attribution — the same shape
        /// `ScanScenePhase`'s `nil`-versus-`.unknown` split already follows.
        ///
        /// Reading `.unknown` as `.model` would return the CEILING certainty
        /// factor for 1,406 coarse rows on the 2026-08-21 t6 pull that nothing
        /// can speak for. Reading it as `.permissive` would be a different
        /// error with the same arithmetic — it would say the bypass wrote them,
        /// which is equally unevidenced — and it is what
        /// ``ScanVerdictProvenance/isKnownPermissive`` exists to stop a
        /// TELEMETRY reader doing.
        func coarseAttributableBand(
            rowProvenance: ScanVerdictProvenance
        ) -> CertaintyBand? {
            guard rowProvenance.licensesCoarseCertaintyBand else { return nil }
            return attributableBand
        }

        /// The DIMENSIONS this span may contribute — `nil` when the RUNNER
        /// wrote them rather than the model (playhead-6ruv).
        ///
        /// The same gate as ``attributableBand`` and for a sharper reason.
        /// `PermissiveAdClassifier.makeAnchorlessSpan` hardcodes
        /// `commercialIntent: .paid` and `ownership: .thirdParty` on every
        /// permissive-bypass span — verbatim on the 2026-08-19 pull,
        /// `{"anchors":[],"certainty":"strong","commercialIntent":"paid",
        /// …,"ownership":"thirdParty","ownershipInferenceWasSuppressed":true}`
        /// — so reading those two as the model's judgement would say "a paid
        /// third-party ad" about audio no model ever classified. That is
        /// playhead-6ruv's own defect, one layer down: a value that names one
        /// thing read as though it named another.
        var attributableDimensions: SpanDimensions? {
            guard ownershipInferenceWasSuppressed != true else { return nil }
            return SpanDimensions(
                commercialIntent: commercialIntent
                    .flatMap(CommercialIntent.init(rawValue:)),
                ownership: ownership.flatMap(Ownership.init(rawValue:)),
                anchorKinds: Set((anchors ?? []).compactMap(\.anchorKind))
            )
        }
    }

    /// What ONE refined span says about who is being sold, once the permissive
    /// gate has let it speak (playhead-6ruv).
    private struct SpanDimensions {
        let commercialIntent: CommercialIntent?
        let ownership: Ownership?
        let anchorKinds: Set<EvidenceAnchorKind>
    }

    /// How much the MODEL'S OWN certainty for this row's presence claim
    /// discounts the ceiling. The floor when it expressed none.
    ///
    /// Two persisted shapes, because two writers:
    ///   • `passA` — ONE `CoarseSupportSchema` OBJECT
    ///     (`BackfillJobRunner.encodeSupport`), e.g.
    ///     `{"supportLineRefs":[17,18,20],"certainty":"strong"}`.
    ///   • `passB` — an ARRAY of refined spans (`encodeRefinedSpans`), each
    ///     carrying its own band.
    ///
    /// For an array the WEAKEST span governs, because the extent this row backs
    /// covers all of them and a mark is only as good as its weakest second. A
    /// span the model did not grade counts as the weakest rather than being
    /// skipped — dropping it would let one graded span speak for the ungraded
    /// ones. `"[]"` — what `encodeSupport` writes for a nil support — decodes
    /// to an empty array and correctly yields the floor.
    ///
    /// # A FABRICATED BAND IS NOT A BAND (playhead-92im review)
    ///
    /// `PermissiveAdClassifier` HARDCODES `certainty: .strong` on the
    /// permissive-bypass path — `makeAnchorlessSpan` for refinement,
    /// `parse(...)` for coarse — and says so: *"the FM never inferred these
    /// classification dimensions, the runner is hardcoding them."* Reading that
    /// as the model's own grade would rebuild this bead's bug on the population
    /// where it bites hardest: **9 of the 11 refined `containsAd` spans in the
    /// 2026-08-10 pull are permissive, every one of them `strong`.**
    ///
    /// `ownershipInferenceWasSuppressed` is the discriminator, and gating on it
    /// is not a new idea — `BackfillJobRunner.EncodedRefinedSpan`'s own header
    /// already requires it of the Phase-8 sponsor-memory writers, *"because
    /// their classification dimensions are not real signals."* This is the
    /// second consumer to owe that debt.
    ///
    /// # THE COARSE HALF IS DETECTABLE AT REST SINCE SCHEMA V61 (playhead-iw7q)
    ///
    /// `PermissiveAdGrammar.parse` writes the identical hardcoded `.strong`
    /// into a `passA` payload. This header said for two beads that the coarse
    /// half "is not detectable at rest … filed as its own bead" — and **no such
    /// bead existed**: `usedPermissiveFallback` was on `SemanticScanResult`
    /// with no column in `semantic_scan_results`, three separate places claimed
    /// the gap was filed, and `bd search permissive` returned nothing that
    /// matched. The bead is playhead-iw7q and the column is
    /// `usedPermissiveFallback INTEGER`.
    ///
    /// So the coarse half is gated now, by
    /// ``PersistedCertainty/coarseAttributableBand(rowProvenance:)``, and the
    /// gate opens for `.model` alone. **Every row written before V61 reads
    /// `.unknown` and is therefore ungraded**: on the 2026-08-21 t6 pull that
    /// is all 1,406 `passA` rows, of which 362 carried a band a consumer spent
    /// (357 `strong`, 5 `moderate`). There is no backfill and there cannot be
    /// one — see ``ScanVerdictProvenance``.
    static func certaintyFactor(of row: SemanticScanResult) -> Double {
        certaintyFactor(certaintyBand(of: row))
    }

    /// THE MODEL'S OWN ``CertaintyBand`` for this row's presence claim, or
    /// `nil` when it expressed none.
    ///
    /// This is the decode ``certaintyFactor(of:)`` has always performed;
    /// playhead-yx0f lifted it out so a SECOND consumer —
    /// `AdDetectionService.buildFMLedgerEntries`, which FABRICATED a band out
    /// of `transcriptQuality` — could read the same persisted value through the
    /// same decoder instead of growing a second one that would have to
    /// re-derive the `ownershipInferenceWasSuppressed` rule below on its own.
    /// The band, not the factor, is the shared quantity: the two lanes anchor
    /// their ladders at different ceilings (`maximumMarkConfidence` here,
    /// `FusionWeightConfig.fmCap` there).
    ///
    /// See ``certaintyFactor(of:)``'s documentation for the two payload shapes,
    /// why the weakest span governs an array, and why a runner-hardcoded
    /// permissive `.strong` is read as ungraded.
    static func certaintyBand(of row: SemanticScanResult) -> CertaintyBand? {
        guard let data = row.spansJSON.data(using: .utf8) else {
            return nil
        }
        let decoder = JSONDecoder()
        if let support = try? decoder.decode(PersistedCertainty.self, from: data) {
            // COARSE shape. The payload carries no discriminator of its own, so
            // the ROW is the only record — see
            // ``PersistedCertainty/coarseAttributableBand(rowProvenance:)``.
            return support.coarseAttributableBand(rowProvenance: row.verdictProvenance)
        }
        guard let spans = try? decoder.decode([PersistedCertainty].self, from: data),
              !spans.isEmpty else {
            return nil
        }
        // REFINED shape. Each span carries its own `ownershipInferenceWasSuppressed`
        // and that gate is unchanged (playhead-92im) — it works on a row of any
        // vintage, so `.unknown` must NOT veto here: doing so would discard a
        // discriminator the payload actually holds and would silently re-grade
        // every pre-V61 refinement.
        //
        // A row the record POSITIVELY says came from the bypass is a different
        // matter, and it is vetoed: `.permissive` is a claim, not an absence.
        // Note what this does NOT close — playhead-e15r (P1, OPEN).
        // `BackfillJobRunner.unionSpan` copies `certainty` from whichever input
        // ranks higher while ANDing the suppression flag, so a permissive
        // `.strong` unioned with a genuine `.moderate` persists as a `.strong`
        // with the flag CLEARED. Outward expansion merges across windows, so
        // such a span can live on a row whose OWN provenance is `.model`, and
        // no row-level gate can see it. e15r's one-line fix is what closes that.
        guard !row.verdictProvenance.isKnownPermissive else { return nil }
        // Seeded at the TOP of the order, so a non-empty array always returns
        // one of its own members' bands.
        return spans.reduce(CertaintyBand?.some(.strong)) { weakest, span in
            bandRank(span.attributableBand) < bandRank(weakest)
                ? span.attributableBand
                : weakest
        }
    }

    /// Count the PRESENCE-pass rows AT ONE TRANSCRIPT VERSION that examined this
    /// extent, and say whether each one affirmed an ad in it.
    ///
    /// Scoped to the presence pass on purpose: `SemanticScanResult`'s own
    /// documentation is that `passB`'s `.noAds` means "found no edges", never
    /// "there is no ad", so a refinement that failed to localize must not be
    /// counted as a replicate voting against presence.
    ///
    /// # And scoped to ONE VERSION, which is playhead-kg6i
    ///
    /// `version` is not a filter bolted on; it is the definition of the
    /// population this factor is about. A "replicate" is a REPEAT OF THE SAME
    /// EXPERIMENT, and an FM screening is an experiment on a transcript. Two
    /// rows formed against two different transcripts of the same audio are two
    /// different experiments, so counting one as a vote about the other's claim
    /// is this repo's standing defect class — a value that names one thing read
    /// as though it named another.
    ///
    /// It is not a hypothetical. Measured on the 2026-08-19 t4 pull: **211 of
    /// the 301 coarse `containsAd` rows carry a `transcriptVersion` the asset's
    /// current canonical chunk set no longer hashes to**, and on nine of the
    /// fifteen assets NOTHING was ever examined at the current version (six of
    /// them carry exactly one row there and it is a playhead-pz32 `noWork:`
    /// sentinel spanning `[0, 0]`). The un-scoped count therefore did not read
    /// "the replicates disagreed"; on this data it usually read "an older
    /// transcript was tiled into different windows".
    ///
    /// THE CLEANEST WITNESS, because it is one row against two: `561CEF5B`
    /// [692.76–791.70] is a single `containsAd` row at version `deace512`. The
    /// only other presence-pass rows over that audio are `[620.34–720.00]` and
    /// `[720.78–820.80]`, both `noAds`, both at `37772e3f` — a DIFFERENT
    /// transcript, tiled at boundaries that do not line up with the claim at
    /// all. Un-scoped they voted it down 1-against-2 to `(1+1)/(1+1+2) = 0.5`
    /// and the mark graded 0.350. Scoped, its own experiment is unanimous and
    /// it grades 0.700.
    ///
    /// WHAT THIS COSTS, NAMED RATHER THAN ABSORBED — the direction is UP, which
    /// is the direction that needs stating. Removing rows shrinks both counts,
    /// so the factor can move either way in principle. WHY IT MOVES ONE WAY
    /// HERE is arithmetic rather than luck: `corroborationFactor` is 1.0
    /// whenever `dissenting == 0`, however many affirmers there are, so
    /// dropping a cross-version AFFIRMER changes nothing unless there is
    /// same-version dissent for it to have been offsetting. Counted over all
    /// 301 coarse windows on the pull, the votes this removes are 282 affirming
    /// and 34 dissenting, against just 5 same-version dissenting votes in the
    /// whole file — so almost every one of the 282 was already worthless and
    /// the 34 were doing all the work.
    ///
    /// **8 of the 79 marks change, all 8 upward, and the geometry of every
    /// mark is byte-identical** (nothing downstream of the grade reads it, so a
    /// re-grade can never move an edge). Decision-level crossings, all upward,
    /// none downward: `SkipOrchestrator.preloadConfidenceThreshold` (0.70) **1**
    /// — the 561CEF5B witness above, joining the 36 marks that already sat at
    /// the ceiling; `SkipPolicyConfig.enterThreshold` (0.65) 1;
    /// `FinalPassRetranscriptionRunner.defaultConfidenceFloor` (0.50) 2;
    /// `stayThreshold` (0.45) 5; `SkipPolicyMatrix.suppressionThreshold` (0.25)
    /// 4; `shortSpanOverrideConfidence` (0.85) 0. **No threshold moved and no
    /// mark's TIER moved**: `extentSupport` is `.unanchored`, so every mark is
    /// still `.markOnly` and no confidence can reach auto-skip.
    ///
    /// WHAT THIS DELIBERATELY DOES NOT DO. It does not drop a stale row from the
    /// COMPOSITION — that is option (b) of the bead, it would remove 48 of the
    /// 79 marks and 4,739.9 s of 8,048.8 s of marked audio, and it is Dan's.
    /// Seconds are version-independent, so a stale row's window bounds are still
    /// real geometry; what is version-dependent is whether two rows are the same
    /// experiment, and that is the only thing changed here.
    ///
    /// The sweep genuinely re-screens: on the 2026-08-10 pull this was measured
    /// against (playhead-3gzp's `ground-truth.sqlite`, sha256 `bcad2d09…`,
    /// schema V47) 36 of 125 coarse windows carry more than one `passA` row,
    /// and on four of them `containsAd` is at most HALF of the rows written for
    /// the window — counting only windows that carry a `containsAd` row AT ALL,
    /// which is the unstated half of that predicate: WITH the condition the
    /// count is 4, WITHOUT it 24, because the other 20 of the 36 carry no
    /// `containsAd` row and clear "at most half" with zero affirmers. **24 is a
    /// CONTRAST between the two readings and NOT a denominator — the windows
    /// that do carry such a row number 16.** All four are a TIE (1 `containsAd`
    /// row of 2) and a STRICT minority occurs on none of **THE 16**, which is
    /// why the predicate reads "at most HALF" rather than "the MINORITY".
    /// (playhead-1gu0 review: the EXAMINED sentence below spells the condition
    /// out, "no window there carries a `containsAd` row its own replicates
    /// outvote or tie", and this one did not. Round 6 then found "worth 4
    /// against 24" transcribed elsewhere as though 24 NAMED the 16, so both
    /// numbers are written out rather than left to a contrast.)
    /// **SAY 16 AND NOT 36 THERE (playhead-1gu0 review round 7).** This
    /// sentence read "none of the 36", and over the 36 it is FALSE: drop the
    /// condition — which is exactly what the 24 above does — and `containsAd`
    /// is strictly under half on every one of the twenty zero-affirmer windows.
    /// That is the SAME 20 already named and not a seventh count: no window of
    /// the 16 is strictly under half, so the without-condition minority set IS
    /// the 20 that carry no `containsAd` row. The paragraph was applying the
    /// vacuous reading to "at most half" and the non-vacuous one to "minority"
    /// — the contrast it exists to draw, committed one predicate along.
    ///
    /// **EVERY COUNT IN THIS PARAGRAPH — 125, 36, 4, 24, 20, 16 — IS OVER
    /// `passA` AT ANY STATUS** — a THIRD population, neither the bullets' below
    /// nor this factor's own. (It read "BOTH OF THOSE COUNTS", which named the
    /// 36 and the four and was exact until round 6 added the rest; a deixis that
    /// counts is a claim that expires, so the numbers are listed.) It is said here rather than only in the
    /// **IT NEVER SEPARATED** paragraph below, which is where the qualification
    /// used to live alone — past the bullets, which carry a DIFFERENT
    /// population's numbers (playhead-1gu0 review). On the EXAMINED population
    /// the bullets use, that second count is **0**: no window there carries a
    /// `containsAd` row its own replicates outvote or tie.
    ///
    /// **WHAT SEPARATES THOSE REPLICATES IS `transcriptVersion` AND `latencyMs`
    /// — NOT `runCorrelationId`. `corroborationFactor`'s doc cited the id and
    /// playhead-1gu0 measured it out.** Over those 36, `transcriptVersion` is
    /// all-distinct on **36 of 36** and `latencyMs` on **35 of 36** — the
    /// exception is a pair of `cancelled` rows both reading `latencyMs` 0.0, two
    /// rows that examined nothing, which is why the examined bullet below reads
    /// 25 of 25 and why the headline is exact only there. SAY WHICH POPULATION,
    /// because it is the RE-SCREENING one and NOT this factor's own — examined
    /// `passA` rows grouped by `(analysisAssetId, windowStartTime,
    /// windowEndTime)`, no version scope. THE INDEPENDENCE paragraph below says
    /// what this factor's own filter adds and why the difference matters:
    ///
    ///   * **2026-08-10**, 99 windows, **25** with more than one row.
    ///     `transcriptVersion` distinct across every row of **25 of 25**;
    ///     `latencyMs` likewise **25 of 25**. `runCorrelationId` distinct on
    ///     13, and the other 12 windows are entirely NULL — those rows predate
    ///     the V42 column (all 24 of them also carry a NULL `createdAt`, which
    ///     the same rung added, so "pre-V42" is read off the row rather than
    ///     inferred). On those 13 it separated **nothing `transcriptVersion` had
    ///     not already separated**.
    ///   * **2026-08-19 t4**, 779 windows, **190** with more than one row.
    ///     `transcriptVersion` **190 of 190**, `latencyMs` **190 of 190**,
    ///     and `runCorrelationId` **0 of 190** — every one of the 190 carries a
    ///     single id. The column does not separate re-screenings at all.
    ///
    /// **IT NEVER SEPARATED ANYTHING `transcriptVersion` DID NOT** — read the 13
    /// above as the id AGREEING with the version, never as an independent
    /// signal — and the 08-10 reading was a residue rather than a property.
    /// `semantic_scan_results.backfillJobId` is the
    /// `backfill_jobs.jobId` (it was spelled `runCorrelationId` until schema
    /// V65, which is playhead-1gu0's other half), and that id is per
    /// `(asset, phase, offset)` — one value for an asset's whole backfill
    /// history. It read as per-screening on 08-10 only because
    /// `transcriptVersion` was IN the job-id preimage until **playhead-wxsv
    /// removed it on 2026-08-07**, three days BEFORE that line was written. So
    /// the pull straddles the change and the claim was already contradicted on
    /// it: over the 36 replicate windows of the population measured above
    /// (`passA`, ANY status), 16 have all-distinct ids, 12 are entirely NULL,
    /// and **8 carry a single id** — every row of those 8 was written AFTER
    /// wxsv's merge, and every one of the 16 has a row from BEFORE it (11 lie
    /// wholly before it, 5 straddle it). Both directions are stated because
    /// only the pair makes "the pull straddles the change" a measurement rather
    /// than an assertion. **THE CUTOFF IS THE MERGE INSTANT, NOT THE CALENDAR
    /// DAY** — `45f4729b`, 2026-08-07 22:41:09 UTC. Cut at midnight STARTING
    /// 2026-08-07 instead and the split reads 11 / 4 / **1 wholly after**,
    /// because `48E903D7` 596.3 s carries an earlier row at 21:03:59.8 UTC —
    /// 1 h 37 m 09 s inside the window, and the only row any of the 16 has on
    /// 08-07 at all. (Midnight ENDING that day reproduces 11 / 5 / 0, the same
    /// as the instant, which is why "at midnight" on its own settles nothing.)
    /// A day is not an instant; that substitution is the very defect this
    /// comment is about. Measured on both pulls, every distinct-id pair is
    /// also a distinct-version pair and not conversely — the job id is a
    /// COARSENING of `transcriptVersion` and can never separate two rows the
    /// version does not.
    ///
    /// **THE INDEPENDENCE IS UNCHANGED; ONLY THE EVIDENCE FOR IT IS — AND THE
    /// EVIDENCE IS ABOUT THE SWEEP, NOT ABOUT THE PAIRS THIS FACTOR COUNTS.**
    /// Two rows at different `transcriptVersion`s were screened from different
    /// transcripts in different FM calls, so the SWEEP really does re-screen
    /// independently; that is what the bullets establish and it is what the
    /// deleted parenthesis was reaching for. It is NOT a statement about the
    /// pairs that reach `affirming`/`dissenting`, because this function counts
    /// only rows AT ONE VERSION (playhead-kg6i) — so every pair this arithmetic
    /// sees shares a `transcriptVersion` by construction, and INSIDE that
    /// population the column separates nothing. Do not close the gap with
    /// ``corroborates(_:_:)``'s "a re-transcription makes the second screening
    /// MORE independent, not less": that note governs a bound-equality
    /// MEMBERSHIP test that is deliberately NOT version-scoped, and it says so
    /// itself — "kg6i … was right about the quantity it governs — but that
    /// quantity is not this one". Nothing about this factor's arithmetic or its
    /// inputs changes.
    ///
    /// - Parameter version: the `transcriptVersion` of the claim being graded —
    ///   i.e. of the row whose confidence term this count feeds. It has NO
    ///   default on purpose: a defaulted "all versions" argument would leave the
    ///   defect one keystroke away and invisible at the call site.
    static func corroboration(
        for extent: Extent,
        in rows: [SemanticScanResult],
        atTranscriptVersion version: String
    ) -> (affirming: Int, dissenting: Int) {
        var affirming = 0
        var dissenting = 0
        for row in rows where row.scanPass != refinementScanPass {
            guard row.transcriptVersion == version,
                  row.didExamineWindow,
                  row.windowStartTime.isFinite, row.windowEndTime.isFinite,
                  row.windowEndTime > row.windowStartTime,
                  extent.overlaps(start: row.windowStartTime, end: row.windowEndTime)
            else { continue }
            if row.disposition == .containsAd {
                affirming += 1
            } else {
                dissenting += 1
            }
        }
        return (affirming, dissenting)
    }

    // MARK: - Extent

    /// A candidate extent in seconds, with the graded strength of the evidence
    /// under it.
    ///
    /// `confidence` DEFAULTS to ``unevidencedMarkConfidence`` — the floor —
    /// rather than to the ceiling, so an extent nobody attached evidence to
    /// under-claims. That is the same rule the factors follow, applied to the
    /// type itself.
    struct Extent: Equatable, Sendable {
        var start: Double
        var end: Double
        var confidence: Double = SemanticSweepMarkComposer.unevidencedMarkConfidence

        var duration: Double { end - start }

        func overlaps(start otherStart: Double, end otherEnd: Double) -> Bool {
            otherStart < end && otherEnd > start
        }
    }

    // MARK: - Compose

    /// Compose mark-only `AdWindow`s for one asset from its persisted semantic
    /// scan rows.
    ///
    /// - Parameters:
    ///   - scanRows: every `semantic_scan_results` row for the asset, both
    ///     passes. Filtering is this function's job.
    ///   - existingWindows: the asset's `ad_windows`, in every `decisionState`.
    ///     Used for the additive-only dedupe AND as the anchor source when
    ///     `provenAnchorEdges` is not supplied.
    ///   - provenAnchorEdges: boundary times somebody PROVED. Defaults to the
    ///     edges harvested from `existingWindows`; pass explicitly to widen the
    ///     source. An empty array is not a refusal — it simply means no edge
    ///     gets clipped.
    ///   - supportLines: the segment geometry a coarse row's `supportLineRefs`
    ///     index into, built by the caller from the segments it already holds
    ///     (playhead-shu5). `nil` — or an index stamped with a different
    ///     `transcriptVersion` than a given row — means that row's refs are
    ///     UNREADABLE, never that it has none; see
    ///     ``localise(_:scanRows:supportLines:)`` for why the two get opposite
    ///     answers. Passing `nil` leaves this producer's geometry exactly where
    ///     the pass-B rows put it, which is what every pre-shu5 caller got.
    static func compose(
        scanRows: [SemanticScanResult],
        existingWindows: [AdWindow],
        provenAnchorEdges: [Double]? = nil,
        supportLines: SupportLineIndex? = nil,
        analysisAssetId: String
    ) -> [AdWindow] {
        let anchors = provenAnchorEdges ?? Self.provenAnchorEdges(in: existingWindows)

        // Stages 1–2: presence, refined by pass B where pass B localized it.
        let presence = presenceExtents(scanRows)
        guard !presence.isEmpty else { return [] }

        // Stage 3: merge, barred by any window a presence pass CLEARED.
        let merged = mergeExtents(presence, barredBy: clearedSpans(in: scanRows))

        // Stage 4: clip to a proven edge when one is in reach.
        let clipped = merged
            .map { clip($0, toAnchors: anchors) }
            // The width ceiling is applied AFTER the clip, so a proven edge that
            // brings an over-wide verdict back under it rescues the mark.
            .filter { $0.duration <= maximumMarkDurationSeconds }

        // Stage 5: dedupe. A PRIOR SWEEP MARK IS DELIBERATELY EXCLUDED — it
        // must not self-suppress, or the second run over unchanged inputs would
        // emit nothing and the version-scoped reconcile would retire the mark
        // the first run emitted. Idempotency rides on content-addressed ids,
        // not on this dedupe.
        let blocking = existingWindows
            .filter { $0.detectorVersion != detectorVersion }
            .map { (start: $0.startTime, end: $0.endTime) }
        let survivors = clipped.filter { extent in
            !blocking.contains { extent.overlaps(start: $0.start, end: $0.end) }
        }

        // Stage 6: localise. Runs AFTER the dedupe on purpose — see
        // `localise(_:scanRows:supportLines:)`.
        let localised = survivors.flatMap {
            localise($0, scanRows: scanRows, supportLines: supportLines)
        }

        // Stage 7: emit. playhead-6ruv attributes each surviving extent from
        // the refinement rows under IT rather than under the coarse window it
        // came from, because stages 3–6 have already decided what audio this
        // mark covers and that is the audio the banner claims.
        return localised.map {
            makeMark(
                $0,
                attribution: attribution(for: $0, in: scanRows),
                analysisAssetId: analysisAssetId
            )
        }
    }

    // MARK: - Stages 1–2: presence, refined

    /// The rows that carry a usable presence verdict, with pass-B refinement
    /// applied. See the policy note in the file header for why a DECLINED
    /// pass B leaves the coarse verdict standing.
    static func presenceExtents(_ rows: [SemanticScanResult]) -> [Extent] {
        let admissible = rows.filter(isPresenceVerdict)
        let refinements = admissible.filter { $0.scanPass == refinementScanPass }
        let coarse = admissible.filter { $0.scanPass != refinementScanPass }

        var result: [Extent] = []
        var claimedRefinements = Set<Int>()
        for window in coarse {
            var narrowed: [Extent] = []
            for (index, refinement) in refinements.enumerated()
            where refinement.windowStartTime < window.windowEndTime
                && refinement.windowEndTime > window.windowStartTime {
                claimedRefinements.insert(index)
                narrowed.append(
                    // playhead-92im: a narrowed extent rests on BOTH rows, so
                    // the WEAKER of the two claims governs it. Reading only the
                    // refinement would let a `strong` narrowing launder a
                    // `moderate` screening verdict, and vice versa.
                    scored(
                        start: max(window.windowStartTime, refinement.windowStartTime),
                        end: min(window.windowEndTime, refinement.windowEndTime),
                        restingOn: [window, refinement],
                        in: rows
                    )
                )
            }
            result.append(contentsOf: narrowed.isEmpty
                ? [scored(
                    start: window.windowStartTime,
                    end: window.windowEndTime,
                    restingOn: [window],
                    in: rows
                )]
                : narrowed)
        }
        // A refinement inside no coarse containsAd window is itself a verdict
        // and stands alone. Dropping it would rebuild, one layer down, the
        // "presence needs a host to attach to" rule this bead removes.
        //
        // playhead-92im: it stands alone at its OWN strength, which is where
        // the corroboration factor does its most visible work — the coarse
        // replicates over such an extent all examined it and did NOT affirm an
        // ad, so an orphan refinement is a CONTRADICTED claim rather than an
        // uncorroborated one. On the pull, F4CE7F47 322–402 s is exactly this:
        // three independent `passA` screenings said `uncertain`, one `passB`
        // row said `containsAd`, and it shipped at the same 0.70 as a mark two
        // clean screenings agreed on.
        //
        // playhead-kg6i NARROWS WHICH SCREENINGS COUNT AS THOSE THREE: only the
        // ones formed against the refinement's OWN `transcriptVersion`. A pass-B
        // row is re-run when the transcript moves, so it and the screenings that
        // contradict it are normally at the same version and this reading
        // survives; what no longer counts is a screening of a transcript the
        // refinement never saw. The deduction is not weakened, it is aimed.
        for (index, refinement) in refinements.enumerated()
        where !claimedRefinements.contains(index) {
            result.append(
                scored(
                    start: refinement.windowStartTime,
                    end: refinement.windowEndTime,
                    restingOn: [refinement],
                    in: rows
                )
            )
        }
        return result.filter { $0.duration >= minimumMarkDurationSeconds }
    }

    /// Build an extent and grade it from the rows it rests on plus, FOR EACH OF
    /// THEM, the presence-pass replicates OF THAT ROW'S OWN EXPERIMENT that
    /// examined the same audio.
    ///
    /// playhead-kg6i: the corroboration count is computed INSIDE the map, once
    /// per backing row, at that row's `transcriptVersion` — it used to be
    /// hoisted out and shared, which is what made one set of votes speak for
    /// claims formed against different transcripts. A backing pair is a coarse
    /// window plus its pass-B narrowing and the two can genuinely differ in
    /// version (the refinement is re-run when the transcript moves), so there is
    /// no single version this could be hoisted back to without picking one row's
    /// cohort to grade the other row's claim.
    ///
    /// The `min` over `backing` is unchanged and still means what it did: an
    /// extent resting on two rows is only as good as its weaker second.
    private static func scored(
        start: Double,
        end: Double,
        restingOn backing: [SemanticScanResult],
        in rows: [SemanticScanResult]
    ) -> Extent {
        var extent = Extent(start: start, end: end)
        extent.confidence = backing
            .map { row in
                let counts = corroboration(
                    for: extent,
                    in: rows,
                    atTranscriptVersion: row.transcriptVersion
                )
                return markConfidence(
                    certaintyFactor: certaintyFactor(of: row),
                    transcriptQuality: row.transcriptQuality,
                    affirming: counts.affirming,
                    dissenting: counts.dissenting
                )
            }
            .min() ?? unevidencedMarkConfidence
        return extent
    }

    /// Does this row assert that an ad is PRESENT in its window?
    ///
    /// Three independent conditions, each load-bearing:
    ///   • the disposition is the positive one — `noAds` / `uncertain` /
    ///     `abstain` are not presence claims and admitting `uncertain` would be
    ///     a lowered threshold wearing a different hat;
    ///   • the status says the window was actually EXAMINED, so a cancelled or
    ///     refused row cannot vote on whatever its disposition column happens
    ///     to hold (the field sweep ended `2581–2676 | abstain | cancelled`);
    ///   • the row is not a playhead-pz32 no-work sentinel, which spans the
    ///     WHOLE attempted range while meaning "no work was performed".
    static func isPresenceVerdict(_ row: SemanticScanResult) -> Bool {
        guard row.disposition == .containsAd else { return false }
        guard row.didExamineWindow else { return false }
        guard row.windowStartTime.isFinite, row.windowEndTime.isFinite else { return false }
        return row.windowEndTime > row.windowStartTime
    }

    // MARK: - Stage 3: merge

    /// Sort by start and sweep-merge extents whose start is within
    /// `mergeGapSeconds` of the running extent's end — and never past
    /// `maximumMarkDurationSeconds`.
    ///
    /// The ceiling is enforced HERE as well as on the result, and the two do
    /// different jobs: without it, a run of adjacent coarse windows fuses into
    /// one over-wide extent that the width filter then drops WHOLE, losing every
    /// verdict in the run. Stopping the merge instead keeps them, as several
    /// marks that each stay inside the bound.
    /// playhead-92im: the merged extent's confidence is the DURATION-WEIGHTED
    /// MEAN of the extents that formed it, weighted by the seconds each one
    /// contributed. It answers "how well supported is the average second of
    /// this mark", which is the claim a banner over the whole extent actually
    /// makes.
    ///
    /// Deliberately not `max`: a 290 s mark carried by one `strong` window and
    /// two `moderate` ones would then read as strong across audio the strong
    /// verdict never saw — presence laundered into extent, the same inversion
    /// stage 4's clip exists to avoid. A nested extent contributes 0 new
    /// seconds and so changes nothing, which is what keeps a recompose over
    /// unchanged inputs byte-identical.
    ///
    /// playhead-shu5: AND NEVER ACROSS A WINDOW THE MODEL CLEARED. `barredBy`
    /// carries the spans a presence-pass row EXAMINED and did NOT affirm; a
    /// merge whose gap any of them covers is refused outright, at any distance.
    /// The two bounds answer different questions and neither implies the other:
    /// `mergeGapSeconds` asks how BIG the swallowed gap is, and this asks
    /// whether anybody LOOKED at it and said no. A verdict of `noAds` over
    /// audio is evidence about that audio; absorbing it into a banner because
    /// its two neighbours both fired is precisely "presence laundered into
    /// extent", one level up from the case stage 3's own comment already
    /// refuses.
    ///
    /// Measured on the 2026-08-19 t4 pull, this fires exactly once across all
    /// 15 assets: `561CEF5B`'s [420.9–619.6] mark is two coarse `containsAd`
    /// windows 0.42 s apart, and a THIRD `passA` row examined [497.3–607.1] and
    /// returned `noAds` — a window that spans the join. It becomes two marks.
    /// One instance is the honest count and it is stated rather than rounded
    /// up: the rule is here because the shape is wrong, not because it is
    /// common.
    static func mergeExtents(
        _ extents: [Extent],
        barredBy barriers: [AdSpanBounds] = []
    ) -> [Extent] {
        let sorted = extents.sorted {
            $0.start != $1.start ? $0.start < $1.start : $0.end < $1.end
        }
        var result: [Extent] = []
        for extent in sorted {
            if var last = result.last,
               extent.start <= last.end + mergeGapSeconds,
               !barriers.contains(where: {
                   $0.coversGap(from: last.end, to: extent.start)
               }),
               max(last.end, extent.end) - last.start <= maximumMarkDurationSeconds {
                let held = max(0, last.duration)
                let added = max(0, extent.end - last.end)
                if added <= 0 {
                    // NESTED, so it adds no seconds and the weighted mean
                    // cannot see it — but it is still a verdict about audio
                    // this mark covers, and silently discarding it would let a
                    // re-screen that graded the SAME window lower vanish. Take
                    // the weaker, which is the rule the rest of this file
                    // already applies to two claims over one extent.
                    last.confidence = min(last.confidence, extent.confidence)
                } else if held + added > 0 {
                    // INTERPOLATE, do not average as a sum of products.
                    // `(a*h + b*w)/(h+w)` is algebraically the same and
                    // numerically is NOT: for a == b it lands a few ULP off,
                    // and a run of `strong`/`good` windows would then merge to
                    // 0.6999999999999998 and fall out of a `>= 0.70` gate for
                    // no reason a reader could ever see. This form returns
                    // `last.confidence` EXACTLY when the two agree.
                    //
                    // NOT measured on the 2026-08-10 pull: enumerating all 38
                    // merge events there, the ceiling count is 7 under every
                    // spelling of the average, so no mark actually fell out.
                    // (3 of the 18 interpolating events DO differ by 1 ULP
                    // between forms, at 0.487/0.252/0.187 — just never at a
                    // gate.) The guarantee below is structural, not empirical;
                    // an earlier draft of this comment claimed a measurement
                    // that does not reproduce.
                    //
                    // `held` is floored at 0 and `added` is positive here, so
                    // the weight is in `(0, 1]` and the result cannot leave
                    // `[min, max]` of the two — an unclamped ratio would
                    // EXTRAPOLATE (to a negative confidence, even) on an
                    // inverted extent, and this function is `internal`, so the
                    // invariant should not rest on `compose` being its only
                    // caller.
                    last.confidence += (extent.confidence - last.confidence)
                        * (added / (held + added))
                }
                last.end = max(last.end, extent.end)
                result[result.count - 1] = last
            } else {
                result.append(extent)
            }
        }
        return result
    }

    /// The spans a PRESENCE-pass row examined and did NOT affirm — the windows
    /// a merge may never bridge (playhead-shu5).
    ///
    /// Scoped to the presence pass for the same reason ``corroboration(for:in:atTranscriptVersion:)``
    /// is: `passB`'s `.noAds` means "found no edges", never "there is no ad", so
    /// a refinement that failed to localize must not bar anything. And gated on
    /// `didExamineWindow` for the same reason stage 1 is: a cancelled or refused
    /// row did not look, so its disposition column is not a verdict about the
    /// audio and cannot clear it.
    ///
    /// NOTE WHAT IS **NOT** THE PREDICATE HERE: `spansJSON == "[]"`. It reads
    /// like "the model cleared this window" and it is not — `encodeSupport`
    /// writes exactly that string for a NIL support object, so a row can carry
    /// `disposition == .containsAd` and `spansJSON == "[]"` at once. On the
    /// 2026-08-19 pull 19 of the 301 coarse `containsAd` rows do, including the
    /// second half of the very mark Dan vetoed. Those rows are affirmations
    /// with no localisation; reading them as denials would have inverted their
    /// meaning. What they get instead is stage 6's answer, and since
    /// playhead-my33 it is conditional rather than flat: such a row contributes
    /// its window when another row corroborates the SAME window, and nothing at
    /// all when it is the sole backing. An affirmation nobody replicated is not
    /// a denial either way — it just stops holding a banner up on its own.
    ///
    /// NOT VERSION-SCOPED, DELIBERATELY, AND THE DIRECTION IS WHY (playhead-kg6i).
    /// ``corroboration(for:in:atTranscriptVersion:)`` had to be scoped because a
    /// stale row was VOTING on a claim it is not a replicate of. A barrier casts
    /// no vote: it refuses a merge, so admitting a stale one can only ever SPLIT
    /// one mark into two narrower ones. Restricting this to the current version
    /// would let merges bridge gaps somebody looked at and cleared — a REACH
    /// change in the widening direction, on a lane whose whole downside budget is
    /// "a wrong banner" — so it is not made here and is not smuggled in under a
    /// bead about a counting error. Measured on the 2026-08-19 t4 pull, the
    /// barrier fires exactly once across all 15 assets (`561CEF5B` [420.9–619.6],
    /// see ``mergeExtents(_:barredBy:)``), so this is a statement about the rule
    /// rather than about a population.
    static func clearedSpans(in rows: [SemanticScanResult]) -> [AdSpanBounds] {
        rows.compactMap { row in
            guard row.scanPass != refinementScanPass,
                  row.didExamineWindow,
                  row.disposition != .containsAd,
                  row.windowStartTime.isFinite, row.windowEndTime.isFinite,
                  row.windowEndTime > row.windowStartTime
            else { return nil }
            return AdSpanBounds(start: row.windowStartTime, end: row.windowEndTime)
        }
    }

    // MARK: - Stage 4: clip

    /// Pull each edge in to the NEAREST proven boundary lying strictly inside
    /// the extent and within `anchorClipRadiusSeconds`. Nearest, not furthest:
    /// a clip is a snap, and minimal movement is the only defensible reading
    /// when the anchor did not prove THIS ad's edge — it proved that SOME
    /// boundary is there.
    ///
    /// A clip that would leave the mark under `minimumMarkDurationSeconds` is
    /// refused outright: refining geometry must never destroy the mark it is
    /// refining.
    static func clip(_ extent: Extent, toAnchors anchors: [Double]) -> Extent {
        guard !anchors.isEmpty else { return extent }

        let startCandidates = anchors.filter {
            $0 > extent.start && $0 < extent.end && $0 - extent.start <= anchorClipRadiusSeconds
        }
        let endCandidates = anchors.filter {
            $0 < extent.end && $0 > extent.start && extent.end - $0 <= anchorClipRadiusSeconds
        }
        // playhead-92im: the clip refines GEOMETRY. It carries the extent's
        // confidence through untouched — an anchor proves where a boundary is,
        // it says nothing about whether the model was right that an ad is here.
        let clipped = Extent(
            start: startCandidates.min() ?? extent.start,
            end: endCandidates.max() ?? extent.end,
            confidence: extent.confidence
        )
        guard clipped.duration >= minimumMarkDurationSeconds else { return extent }
        return clipped
    }

    /// Harvest the boundary times somebody PROVED from a set of persisted rows.
    ///
    /// The definition of "proven" is deliberately the SAME one
    /// `SpanExtentSupport` applies — an `AutoSkipEdgeAnchor` other than
    /// `.unanchored`, i.e. a byte-exact rediff width or a stinger snap on that
    /// specific edge. Reading anything looser (a lexical seed's edge, say)
    /// would re-import the failure playhead-2350 fixed: shipping a narrow
    /// seed's geometry as the ad's geometry.
    static func provenAnchorEdges(in windows: [AdWindow]) -> [Double] {
        var edges = Set<Double>()
        for window in windows {
            if AutoSkipEdgeAnchor(rawValue: window.startEdgeAnchor).map({ $0 != .unanchored }) == true,
               window.startTime.isFinite {
                edges.insert(window.startTime)
            }
            if AutoSkipEdgeAnchor(rawValue: window.endEdgeAnchor).map({ $0 != .unanchored }) == true,
               window.endTime.isFinite {
                edges.insert(window.endTime)
            }
        }
        return edges.sorted()
    }

    // MARK: - Stage 6: localise (playhead-shu5)

    /// THE TRANSCRIPT LINES the model said this coarse row's verdict rests on,
    /// or `nil` when it named NONE.
    ///
    /// `nil` covers three spellings of the same claim, and they are the same
    /// claim: no support object at all (`spansJSON == "[]"`, which
    /// `encodeSupport` writes for a nil `support`), a support object whose
    /// `supportLineRefs` array is empty, and a payload that will not decode as
    /// the coarse shape. In each the model asserted presence and pointed at
    /// nothing.
    ///
    /// Refinement rows are excluded outright: a `passB` payload is an ARRAY of
    /// refined spans whose geometry is already in the row's own window, so
    /// there is nothing here to resolve.
    static func supportLineRefs(of row: SemanticScanResult) -> [Int]? {
        guard row.scanPass != refinementScanPass,
              let data = row.spansJSON.data(using: .utf8),
              let support = try? JSONDecoder().decode(PersistedCertainty.self, from: data),
              let refs = support.supportLineRefs,
              !refs.isEmpty
        else { return nil }
        return refs
    }

    /// The windows a DECLINED pass-B row looked at inside this coarse window —
    /// i.e. this row's own `supportLineRefs`, already projected into seconds by
    /// the writer that owned the segments.
    ///
    /// Three conditions, each load-bearing:
    ///   * the row DECLINED (`disposition != .containsAd`) and EXAMINED its
    ///     window. An affirming pass B is stage 2's business, not this one;
    ///     an unexamined row looked at nothing and localises nothing.
    ///   * it carries the SAME `transcriptVersion` as the coarse row. A
    ///     refinement plan is built from one segmentation, so a row from
    ///     another version is a different episode's geometry wearing the same
    ///     seconds.
    ///   * it is strictly INSIDE and strictly NARROWER than the coarse window.
    ///     A zoom that covers the whole tile localised nothing, and one that
    ///     reaches outside it is not this window's zoom.
    static func declinedRefinementSpans(
        over window: SemanticScanResult,
        in rows: [SemanticScanResult]
    ) -> [AdSpanBounds] {
        let coarseDuration = window.windowEndTime - window.windowStartTime
        return rows.compactMap { row in
            guard row.scanPass == refinementScanPass,
                  row.disposition != .containsAd,
                  row.didExamineWindow,
                  row.transcriptVersion == window.transcriptVersion,
                  row.windowStartTime.isFinite, row.windowEndTime.isFinite,
                  row.windowEndTime > row.windowStartTime,
                  row.windowStartTime >= window.windowStartTime - SupportLineIndex.boundaryEpsilon,
                  row.windowEndTime <= window.windowEndTime + SupportLineIndex.boundaryEpsilon,
                  row.windowEndTime - row.windowStartTime < coarseDuration
            else { return nil }
            return AdSpanBounds(start: row.windowStartTime, end: row.windowEndTime)
        }
    }

    /// WHERE ONE PRESENCE ROW SAYS ITS AD IS — and, when it does not say, WHY
    /// NOT. The three cases are kept apart because they are three different
    /// claims, and the whole of this bead is that a claim about one thing was
    /// being read as a claim about another.
    enum Localisation: Equatable {
        /// The model named these seconds, and we can read them.
        case named([AdSpanBounds])
        /// The model named LINES WE CANNOT READ — a stale `transcriptVersion`,
        /// no index, a window this index does not reproduce. A failure of OUR
        /// RECORDS, not of the verdict.
        case unreadable
        /// The model named NOTHING: no support object, or an empty
        /// `supportLineRefs`. A property of the VERDICT.
        ///
        /// Since playhead-my33 this is the ONLY case whose consumer can decline
        /// to contribute an extent — see ``contribution(of:in:supportLines:)``.
        /// The separation from ``unreadable`` stopped being bookkeeping and
        /// started carrying a decision, which is what it was built for.
        case absent
    }

    /// Ask one row where its ad is.
    ///
    ///   1. a REFINEMENT row already IS the model's narrowing — its own window
    ///      is the answer;
    ///   2. a coarse row whose DECLINED pass B left a narrower window is
    ///      localised by that window (see ``declinedRefinementSpans(over:in:)``);
    ///   3. a coarse row whose `supportLineRefs` RESOLVE is localised by the
    ///      spans they name;
    ///   4. named but unresolvable → `.unreadable`;
    ///   5. named nothing → `.absent`.
    static func localisation(
        of row: SemanticScanResult,
        in rows: [SemanticScanResult],
        supportLines: SupportLineIndex?
    ) -> Localisation {
        let window = AdSpanBounds(start: row.windowStartTime, end: row.windowEndTime)
        guard row.scanPass != refinementScanPass else { return .named([window]) }

        let declined = declinedRefinementSpans(over: row, in: rows)
        if !declined.isEmpty { return .named(padded(declined, within: window)) }

        guard let refs = supportLineRefs(of: row) else { return .absent }
        guard let resolved = supportLines?.resolve(
            supportLineRefs: refs,
            in: SupportLineIndex.RowWindow(
                transcriptVersion: row.transcriptVersion,
                firstAtomOrdinal: row.windowFirstAtomOrdinal,
                lastAtomOrdinal: row.windowLastAtomOrdinal,
                startTime: row.windowStartTime,
                endTime: row.windowEndTime
            )
        ) else { return .unreadable }
        return .named(padded(resolved, within: window))
    }

    /// Is `candidate` a CORROBORATING REPLICATE of `row`'s presence claim —
    /// another examination of THE SAME AUDIO that also said an ad is in it?
    /// (playhead-my33.)
    ///
    /// # The predicate, and each clause's job
    ///
    ///   * **A DIFFERENT ROW.** `id` is the table's primary key, so two
    ///     distinct persisted rows always differ here and a row can never
    ///     corroborate itself. Handing the same row in twice answers `false`,
    ///     which is the direction that under-claims.
    ///   * **A PRESENCE-PASS ROW.** `passB` is excluded for the reason
    ///     ``corroboration(for:in:atTranscriptVersion:)`` and
    ///     ``clearedSpans(in:)`` both exclude it: a refinement is a second look
    ///     at a claim already made, not a second SCREENING of the audio, so it
    ///     is not independent of the row it would be propping up. (A `passB`
    ///     row that AFFIRMED is stage 2's business and has already narrowed
    ///     this extent; one that DECLINED reaches `.named` through
    ///     ``declinedRefinementSpans(over:in:)`` and never gets here.)
    ///   * **AN ADMISSIBLE PRESENCE VERDICT** — the exact stage-1 predicate, so
    ///     a row that could not mint a mark on its own cannot hold one up
    ///     either. The `abstain | exceededContextWindow` row over `A9F6DF05`
    ///     [68.9–187.3] on the pull is precisely this case, and it must not
    ///     count.
    ///   * **THE SAME WINDOW**, to ``SupportLineIndex/boundaryEpsilon`` — the
    ///     tolerance ``declinedRefinementSpans(over:in:)`` already uses for the
    ///     same job, absorbing a last-bit `REAL` round trip rather than
    ///     licensing a near-miss.
    ///
    /// # Why BOUND EQUALITY rather than overlap
    ///
    /// The claim being corroborated is *"an ad is somewhere in this tile"* and
    /// nothing narrower — that is the whole of what makes the row `.absent`. A
    /// row over DIFFERENT seconds cannot speak to it: two neighbouring tiles
    /// that each report an ad corroborate nothing about each other, and
    /// admitting a partial overlap would let a mark stand on a claim about
    /// audio it does not cover. That is "presence laundered into extent", the
    /// substitution stages 3 and 4 already refuse, one level up.
    ///
    /// It is also not academic on this pull. Dan's own vetoed mark is the
    /// witness: `CD2976E6` [1211.2–1287.2] is `.absent`, and the row it merged
    /// with is [1131.6–1210.9] — ADJACENT, 0.3 s away, a different window.
    /// Under an overlap predicate that neighbour would corroborate it and the
    /// veto would survive; under bound equality it does not, and the mark falls
    /// to [1131.6–1210.9], which is the outcome Dan asked for.
    ///
    /// # Why NOT scoped to one `transcriptVersion` (playhead-kg6i)
    ///
    /// kg6i established that a vote from a superseded transcript is not a
    /// replicate of the same experiment, and it was right about the quantity it
    /// governs — but that quantity is not this one, and the difference is in
    /// what each selects rows BY.
    ///
    ///   * ``corroboration(for:in:atTranscriptVersion:)`` selects by OVERLAP and
    ///     feeds an AGREEMENT statistic, `(1 + a) / (1 + a + d)`. Its witness is
    ///     `561CEF5B` [692.76–791.70] against [620.34–720.00] and
    ///     [720.78–820.80] — rows tiled at boundaries the claim does not share,
    ///     so they are not even about the same audio. There `transcriptVersion`
    ///     was the available proxy for "the same experiment", and the deeper
    ///     property it stood in for was "the same claim".
    ///   * This predicate asks for the same claim DIRECTLY, by requiring
    ///     identical bounds. It is a MEMBERSHIP question — did more than one
    ///     screening affirm exactly this audio — with no dissent term and no
    ///     arithmetic that needs replicates.
    ///
    /// And once the audio is pinned, a re-transcription makes the second
    /// screening MORE independent, not less: two rows at one version share
    /// their whole input and replicate only the model's sampling, while two at
    /// different versions share only the audio — which is what the claim is
    /// about. Seconds are version-independent; that is already this file's own
    /// reading, in ``corroboration(for:in:atTranscriptVersion:)``'s
    /// **WHAT THIS DELIBERATELY DOES NOT DO** note ("a stale row's window bounds
    /// are still real geometry") and in ``clearedSpans(in:)``, which is
    /// deliberately not version-scoped either. (This line said "closing note",
    /// which it was until `59539769` appended the replicate-population argument
    /// after it — name the note, never its position; playhead-1gu0 review.)
    ///
    /// MEASURED, because the choice decides the bead rather than decorating it:
    /// on the 2026-08-19 t4 pull the 19 `.absent` rows sit at **19 distinct
    /// `(window, transcriptVersion)` pairs**, and no admissible presence row
    /// shares any of those pairs. A version-scoped predicate therefore
    /// corroborates **nothing**, and Dan's sole-backing rule would collapse into
    /// the drop-all option he explicitly declined. Stated the other way round:
    /// the four windows that ARE corroborated here are corroborated *only*
    /// across versions.
    ///
    /// `backfillJobId` — the column was spelled `runCorrelationId` when this
    /// note was written, and playhead-1gu0 renamed it in schema V65 for exactly
    /// the reason below — was considered as a "different FM call" spelling and
    /// measured out: all five `containsAd` rows over `A9F6DF05` ~4038 s carry
    /// the identical `fm-9330e821aeb36a0d` across THREE calendar days and five
    /// calls (2026-08-12 ×3, -15, -16), so it does not separate re-screenings at
    /// all. This line said "four days" and four is the ASSET's span, not these
    /// five rows' — `A9F6DF05` carries 176 rows over 2026-08-12/-14/-15/-16, all
    /// under the same one id, which is the stronger reading and the one the V65
    /// rung quotes. That is a property of the column and not of this pull: a job
    /// id is per `(asset, phase, offset)`.
    static func corroborates(
        _ candidate: SemanticScanResult,
        _ row: SemanticScanResult
    ) -> Bool {
        guard candidate.id != row.id,
              candidate.scanPass != refinementScanPass,
              isPresenceVerdict(candidate)
        else { return false }
        return abs(candidate.windowStartTime - row.windowStartTime)
            <= SupportLineIndex.boundaryEpsilon
            && abs(candidate.windowEndTime - row.windowEndTime)
            <= SupportLineIndex.boundaryEpsilon
    }

    /// Did anything OTHER than `row` affirm an ad in exactly `row`'s window?
    ///
    /// The caller has already admitted `row` itself — ``localise(_:scanRows:supportLines:)``
    /// only asks ``isPresenceVerdict(_:)``-passing rows — so this states nothing
    /// about `row` and only counts its company.
    static func isCorroborated(
        _ row: SemanticScanResult,
        in rows: [SemanticScanResult]
    ) -> Bool {
        rows.contains { corroborates($0, row) }
    }

    /// What one presence row contributes to a mark's extent.
    ///
    /// # `.unreadable` KEEPS ITS WINDOW; `.absent` KEEPS IT ONLY IF CORROBORATED
    ///
    /// `.unreadable` keeping its window is not in question: our records failed,
    /// not the model, and inventing geometry is how a boundary lands on the
    /// show (`SupportLineIndex`'s header carries the measured witness, 22 s
    /// off). What IS in question is how many rows land there — **174 of the
    /// 301** on the 2026-08-19 t4 pull, every one of them because the episode's
    /// transcript has moved on since the scan and the row's segmentation no
    /// longer exists. (This line said **130** and that figure does not
    /// reproduce. Re-measured with the same offline reconstruction that returns
    /// playhead-kg6i's 211 cross-version rows to the digit, the split is 108
    /// `.named` / 174 `.unreadable` / 19 `.absent`. Three quantities live in
    /// this neighbourhood — rows at a superseded version (211), rows whose
    /// chunks are gone from the database (280), rows this stage cannot resolve
    /// (174) — and the file already warns they are three; quote whichever you
    /// took.) That bound belongs to **playhead-kg6i**, not here: shrinking it
    /// means composing from fewer versions, which removes marks.
    ///
    /// # `.absent` — DAN'S CALL, TAKEN 2026-08-21 (playhead-my33)
    ///
    /// A `containsAd` row that names no lines is presence with no localisation
    /// — the thing `maximumMarkDurationSeconds` already calls "a TARGETING
    /// problem … not something to put in front of a listener". The two options
    /// that had been measured are both lossy in one direction, so Dan chose
    /// neither: **DROP ONLY WHERE IT IS THE SOLE BACKING.** An unlocalised row
    /// may still contribute its window when another row corroborates the same
    /// window; when it is the only thing holding a mark up, it contributes
    /// nothing and the mark does not exist. See ``corroborates(_:_:)`` for what
    /// "corroborates" means and why.
    ///
    /// **COUNT THE WINDOWS, NOT THE ROWS.** The 19 `.absent` rows are only
    /// **10 distinct windows** — the sweep re-scans an episode after its
    /// transcript moves, so `A9F6DF05 6814.0–6874.1` alone appears four times.
    /// Read as transcript, **6 of the 10 are real ads** (LifeLock/Paragold,
    /// NetSuite ×2, Whisperflow, Progressive, the show's own conversation-cards
    /// promo) and **4 are show** — `A9F6DF05` 68.9–187.3 and 4368.2–4439.3,
    /// `CD2976E6` 1211.2–1287.2 (Dan's own), `E51B25E4` 4840.7–4960.7. A first
    /// draft of this note said "roughly fifteen of the nineteen are real ads",
    /// which is the row count wearing the window count's meaning — the standing
    /// defect class, in the comment describing the fix for it.
    ///
    /// # WHAT IT COST, MEASURED — and the third population nobody had counted
    ///
    /// Recomposed over all 15 assets: **78 marks / 6,979.1 s**, against the
    /// shipped **80 / 7,246.6 s**. Two marks removed and one narrowed, and all
    /// three are SHOW: `A9F6DF05` [4368.18–4439.34], `E51B25E4`
    /// [4840.74–4960.74], and Dan's own `CD2976E6` [1131.60–1287.18] falling to
    /// [1131.60–1210.86]. **No confidence value moves at all** — the other 77
    /// marks are geometry- AND grade-identical — because this stage refines
    /// geometry and admission happened four stages earlier.
    ///
    /// **THE SIX AD WINDOWS COST NOTHING, BECAUSE THEY WERE NEVER MARKS.**
    /// Every one of the six is suppressed by stage 5's additive-only dedupe:
    /// two overlap a `dayZeroRediffByteExact` window at confidence 1.0
    /// (`0FF7EFF3` ~3434 s, `561CEF5B` 0 s) and four overlap a window Dan
    /// MARKED BY HAND (`A9F6DF05` ~2656 / ~4038 / ~6814 s, `C0610BF9` ~968 s —
    /// `falseNegative` corrections with an empty `targetRefsJSON`, 2026-08-15).
    /// Only **3 of the 10** windows reach this stage at all, and all three are
    /// show. So "six correct detections lost" counts coarse VERDICTS, and the
    /// quantity that matters is composed MARKS, where the answer is zero — the
    /// same window-versus-row substitution the paragraph above corrects, one
    /// population further along. The bead's trade was stated in good faith and
    /// it is not what a recompose says.
    ///
    /// WHAT THIS MEASUREMENT CANNOT SAY, because the caveat is the interesting
    /// half: the recompose runs against the device's `ad_windows` AS THEY ARE
    /// TODAY, four of which are Dan's own later marks. It shows the permissive
    /// half of the rule doing nothing HERE; that is not evidence it does
    /// nothing. Four of the six ad windows ARE corroborated, and on an asset
    /// where no other producer had found the ad they would keep their
    /// contribution while drop-all would not. That difference IS the rule; it
    /// simply has no instance on this pull.
    ///
    /// AND DAN'S MARK IS TRIMMED, NOT REMOVED — the bead says this rule "fixes
    /// Dan's own first false positive" and that is one word too strong. Read
    /// back from `transcript_chunks`, the TAIL this drops ([1211.16–1287.18])
    /// is Alex Honnold describing breaking a climb into pieces, and the HEAD
    /// that survives ([1131.60–1210.86]) is *"my sponsored through the
    /// Northface was like … I think my 1st year was like 10 K a year"* — which
    /// is the passage playhead-6ruv records him vetoing, commercial VOCABULARY
    /// read as commercial INTENT. Both halves are show; this bead gives back
    /// 76.3 s of it and leaves 79.3 s standing. The head survives because it
    /// NAMED a line (46) that today's segmentation can no longer resolve, so it
    /// is `.unreadable` — the case this bead deliberately does not touch.
    ///
    /// The change is still one line here — and that claim is only true because
    /// mutant SU12 proved it was NOT: it used to be silently undone by
    /// ``localise(_:scanRows:supportLines:)``'s duration-floor rescue, which is
    /// now a separate refusal, and this bead is what makes that refusal
    /// REACHABLE for the first time.
    static func contribution(
        of row: SemanticScanResult,
        in rows: [SemanticScanResult],
        supportLines: SupportLineIndex?
    ) -> [AdSpanBounds] {
        switch localisation(of: row, in: rows, supportLines: supportLines) {
        case .named(let spans): spans
        case .unreadable:
            [AdSpanBounds(start: row.windowStartTime, end: row.windowEndTime)]
        case .absent:
            isCorroborated(row, in: rows)
                ? [AdSpanBounds(start: row.windowStartTime, end: row.windowEndTime)]
                : []
        }
    }

    /// Widen each span by `pad` at both edges and clamp the result to the
    /// window the model examined.
    ///
    /// THE PAD IS A PARAMETER SO THE CLAMP HAS A REACHABLE CONTRACT (mutant
    /// SU17). With ``supportLocalisationPadSeconds`` at 0.0 the clamp can never
    /// bite through either production path — a resolved span is built from
    /// lines inside a run that reproduces the row's window, and a declined
    /// pass-B window is required to lie inside it — so deleting the clamp is an
    /// EQUIVALENT mutation at today's constant, and a test written against the
    /// shipped value can only ever pass. That is not a reason to drop the
    /// clamp: it is the whole of what keeps a future non-zero pad, which is
    /// Dan's to set, from carrying a mark outside the audio the model examined.
    /// Injecting the pad lets the contract be asserted at the value that makes
    /// it load-bearing instead of at the one that makes it inert.
    static func padded(
        _ spans: [AdSpanBounds],
        within window: AdSpanBounds,
        pad: Double = supportLocalisationPadSeconds
    ) -> [AdSpanBounds] {
        spans.compactMap {
            AdSpanBounds(start: $0.start - pad, end: $0.end + pad)
                .clamped(to: window)
        }
    }

    /// Narrow one surviving extent to the seconds the model itself named.
    ///
    /// # It can only SHRINK, and that is the whole safety argument
    ///
    /// Every returned extent is a subset of `extent`, so this stage cannot
    /// admit a mark the pipeline above suppressed. That matters concretely
    /// rather than theoretically: measured on the 2026-08-19 pull, running the
    /// localisation EARLIER — before stage 5's dedupe — surfaced three brand
    /// new marks on `9126552E` at [586.4–676.0], [1332.4–1407.5] and
    /// [1446.7–1490.0], and reading their transcripts they are the two hosts
    /// talking about a sculpture, a dive and an ego. They were suppressed only
    /// because their coarse extent happened to overlap another producer's
    /// window, and narrowing them released them. A geometry fix is not a
    /// licence to change ADMISSION, so the order is: decide whether, then
    /// decide how wide.
    ///
    /// # THREE refusals, and two of them used to be ONE
    ///
    /// An extent no presence row overlaps is returned UNCHANGED — a mark is
    /// never deleted because the search for its own evidence came back empty.
    ///
    /// If contributions exist but every piece falls under
    /// `minimumMarkDurationSeconds`, the extent is returned UNCHANGED: that is
    /// `clip`'s rule (*"refining geometry must never destroy the mark it is
    /// refining"*), applied to the other refiner.
    ///
    /// And if NO contributor offered a single span, nothing here is supported
    /// and nothing is returned. **That was folded into the duration-floor
    /// refusal and mutant SU12 found it**, which is the whole argument for
    /// separating them: SU12 was playhead-my33's proposed one-line change
    /// (`.absent` contributes nothing), and with the two folded together the
    /// mark was RESCUED by the floor rule and the knob did nothing. The bead
    /// and the comment on ``contribution(of:in:supportLines:)`` both said "it
    /// is one line"; it was not, and only a mutation could say so. Two
    /// different claims — *"the refinement is too small to use"* and *"there is
    /// nothing to refine"* — sharing one `guard`.
    ///
    /// **THAT BRANCH IS LIVE AS OF playhead-my33 AND IT IS THE ONLY WAY A MARK
    /// LEAVES THIS STAGE.** Until Dan's 2026-08-21 call every `Localisation`
    /// yielded at least the row's window, so `contributed` was always true and
    /// the branch was unreachable by construction; shu5 separated it on SU12's
    /// evidence alone, one bead before anything could take it. Now an `.absent`
    /// row with no corroborating replicate offers nothing, and an extent whose
    /// contributors are ALL such rows returns `[]`. The distinction the split
    /// bought is what makes that removal survive the floor rule instead of
    /// being quietly undone by it.
    ///
    /// Pieces are unioned with `mergeGapSeconds` before the duration floor, so
    /// two adjacent supported segments become one mark rather than two
    /// touching banners — the same question stage 3 asks, asked again at the
    /// finer granularity localisation just created.
    static func localise(
        _ extent: Extent,
        scanRows: [SemanticScanResult],
        supportLines: SupportLineIndex?
    ) -> [Extent] {
        let contributors = scanRows.filter {
            isPresenceVerdict($0)
                && extent.overlaps(start: $0.windowStartTime, end: $0.windowEndTime)
        }
        guard !contributors.isEmpty else { return [extent] }

        let bounds = AdSpanBounds(start: extent.start, end: extent.end)
        var pieces: [AdSpanBounds] = []
        var contributed = false
        for row in contributors {
            let spans = contribution(of: row, in: scanRows, supportLines: supportLines)
            if !spans.isEmpty { contributed = true }
            pieces.append(contentsOf: spans.compactMap { $0.clamped(to: bounds) })
        }
        // NOTHING under this extent is supported. Distinct from the floor
        // refusal below — see the header.
        guard contributed else { return [] }

        let unioned = union(pieces)
            .filter { $0.duration >= minimumMarkDurationSeconds }
        guard !unioned.isEmpty else { return [extent] }

        // The grade is carried through untouched, exactly as `clip` does: this
        // stage refines GEOMETRY, and where an ad is says nothing about how
        // well evidenced the claim that it IS an ad was.
        return unioned.map {
            Extent(start: $0.start, end: $0.end, confidence: extent.confidence)
        }
    }

    /// Sweep-union spans whose gap is at most `mergeGapSeconds`.
    private static func union(_ spans: [AdSpanBounds]) -> [AdSpanBounds] {
        let sorted = spans.sorted {
            $0.start != $1.start ? $0.start < $1.start : $0.end < $1.end
        }
        var result: [AdSpanBounds] = []
        for span in sorted {
            if var last = result.last, span.start <= last.end + mergeGapSeconds {
                last.end = max(last.end, span.end)
                result[result.count - 1] = last
            } else {
                result.append(span)
            }
        }
        return result
    }

    // MARK: - Attribution (playhead-6ruv)

    /// WHETHER THE MODEL NAMED AN ADVERTISER UNDER THIS MARK, and — when it did
    /// not — WHY NOT. Four cases, because they are four different claims, and
    /// the whole of this bead is that a claim about one thing was being read as
    /// a claim about another.
    ///
    /// # The field case
    ///
    /// 2026-08-19, asset `CD2976E6`, window [1131.6–1210.9]. The coarse model
    /// returned `{"supportLineRefs":[46],"certainty":"strong"}` over Alex
    /// Honnold saying *"my sponsored through the Northface was like, I think my
    /// 1st year was like 10 K a year … then I started doing corporate speaking
    /// … they also had to get all this sort of marketing material"*. There is
    /// no advertiser, no product, no CTA, no URL and no promo code in it — it
    /// is a climber describing how he earns a living, and Dan vetoed the mark.
    /// Commercial VOCABULARY was read as commercial INTENT.
    ///
    /// The row carried nothing that could have said so, and it could not have:
    /// `CoarseScreeningSchema` is a disposition plus a support object with
    /// exactly two fields. **THAT MARK IS `.unrefined` AND STAYS `.unrefined`.**
    /// This type does not fix it. What it does is stop it being
    /// indistinguishable from a mark the model anchored to a brand: measured on
    /// the same pull, of the 79 persisted sweep marks **5 are `.refined` with a
    /// naming anchor, 10 are `.suppressed`, 64 are `.unrefined`** — and Dan's
    /// is one of the 64.
    ///
    /// # It is a RECORD, not a tier
    ///
    /// Nothing here moves a confidence, an `eligibilityGate` or an edge anchor,
    /// and ``makeMark(_:attribution:analysisAssetId:)`` is where that is
    /// asserted rather than promised. Treating an unnameable verdict as a
    /// weaker OBJECT than a nameable one is a policy question about what a
    /// banner may claim, and it is **Dan's** — the bead says so in as many
    /// words. This makes the distinction exist and be readable at rest; it does
    /// not spend it.
    enum Attribution: Equatable, Sendable {
        /// No refinement pass affirmed an ad anywhere in this extent. Presence
        /// with no attributable advertiser BY CONSTRUCTION — the coarse lane
        /// was never asked who the advertiser was.
        case unrefined
        /// A refinement DID affirm, and its payload yielded no spans at all: an
        /// empty array, or bytes that will not decode as the refined shape. A
        /// failure of OUR RECORDS, not of the verdict — held apart from
        /// ``unrefined`` for the same reason ``Localisation/unreadable`` is
        /// held apart from ``Localisation/absent``.
        case unreadable
        /// Spans exist and EVERY one of them is a permissive-bypass span whose
        /// dimensions `PermissiveAdClassifier` hardcoded. The model judged
        /// nothing here; the runner did. 10 of the 79 marks on the 2026-08-19
        /// pull, and 9 of the 11 refined spans on the 2026-08-10 one.
        case suppressed
        /// At least one span the MODEL itself judged, with what it judged.
        case refined(Refinement)

        /// Did the model point at something that NAMES an advertiser?
        /// `false` for every case but a ``refined`` one carrying a naming
        /// anchor — an absence of evidence never reads as evidence.
        var namesAnAdvertiser: Bool {
            switch self {
            case .refined(let refinement): refinement.namesAnAdvertiser
            case .unrefined, .unreadable, .suppressed: false
            }
        }
    }

    /// The dimensions the MODEL'S OWN spans under one mark carried, unioned.
    ///
    /// Unioned rather than reduced to a single value because a mark can rest on
    /// several refined spans and they need not agree; collapsing them would
    /// pick one span's answer and print it over audio it never saw, which is
    /// the substitution stage 3's own comment refuses at the level of extent.
    struct Refinement: Equatable, Sendable {
        var anchorKinds: Set<EvidenceAnchorKind> = []
        var commercialIntents: Set<CommercialIntent> = []
        var ownerships: Set<Ownership> = []

        /// The anchor kinds that NAME something a listener could be sold.
        ///
        /// `ctaPhrase` and `disclosurePhrase` are deliberately NOT here. *"Go
        /// to the link in the description"* is a call to action with no
        /// advertiser in it, and *"this episode is sponsored"* is a disclosure
        /// with no advertiser in it — both are exactly the commercial
        /// VOCABULARY this bead exists because the model over-reads. A url, a
        /// brand span or a promo code identifies WHO.
        static let namingAnchorKinds: Set<EvidenceAnchorKind> =
            [.brandSpan, .url, .promoCode]

        var namesAnAdvertiser: Bool {
            !anchorKinds.isDisjoint(with: Self.namingAnchorKinds)
        }
    }

    /// Every refined span the MODEL judged under this extent, unioned.
    ///
    /// # Which rows are asked
    ///
    /// The REFINEMENT pass only, affirming (`containsAd`) and EXAMINED, whose
    /// window overlaps the extent. Each condition is load-bearing and each has
    /// a precedent in this file:
    ///
    ///   * `passA` rows are excluded outright, and that is a LIMIT rather than
    ///     a design choice. What follows from it here: attribution can say the
    ///     model named a brand, and this type still cannot certify that the
    ///     PRESENCE verdict under a `.unrefined` mark was the model's.
    ///
    ///     **The reason it could not has been removed, and this bullet is not
    ///     yet the beneficiary.** It used to read "a permissive coarse row is
    ///     byte-identical to a genuine one and no read can tell them apart.
    ///     Filed separately; it needs a schema change" — and no such bead was
    ///     ever filed, which is how playhead-iw7q came to exist. The schema
    ///     change landed at V61 (`semantic_scan_results.usedPermissiveFallback`,
    ///     `SemanticScanResult.verdictProvenance`), so a coarse row's
    ///     provenance IS readable now and
    ///     ``certaintyBand(of:)``'s coarse gate already spends it. Wiring it
    ///     into this ``Attribution`` — a fifth case, or a qualifier on
    ///     ``Attribution/unrefined`` — is playhead-6ruv's territory and is NOT
    ///     done here. Two things bound the benefit before anyone reaches for
    ///     it: every row already on disk reads `.unknown`, so the distinction
    ///     is only informative for rows written from V61 onwards; and treating
    ///     an unattributable verdict as a weaker OBJECT is a policy question
    ///     about what a banner may claim, which is Dan's.
    ///   * a DECLINED `passB` row means "found no edges", never "there is no
    ///     ad" — the same reason ``corroboration(for:in:atTranscriptVersion:)`` and
    ///     ``clearedSpans(in:)`` both scope themselves — so it attributes
    ///     nothing rather than contradicting anything.
    ///   * an UNEXAMINED row looked at nothing, so its `disposition` column is
    ///     not a verdict about the audio (the field sweep really does end
    ///     `2581–2676 | abstain | cancelled`).
    ///
    /// # Overlap, and what it cannot see
    ///
    /// Plain overlap, the same predicate stage 2 uses to pair a refinement with
    /// a coarse window and ``corroboration(for:in:atTranscriptVersion:)`` uses to count replicates.
    /// A refined span's own geometry is `firstLineRef`/`lastLineRef`, which are
    /// line refs rather than seconds, and only the ROW carries the projection
    /// into seconds — so row overlap is the finest grain available at rest. A
    /// row that overlaps this extent by a fraction of a second therefore speaks
    /// for all of it. Stated rather than hidden: the alternative is resolving
    /// line refs through a `SupportLineIndex`, which stage 6 already shows can
    /// only be done for a row whose transcript version still EXISTS — and on
    /// the 2026-08-19 t4 pull **280 of the 301 coarse `containsAd` rows carry a
    /// `transcriptVersion` that no surviving `transcript_chunks` row carries**,
    /// which is playhead-kg6i's territory and not a bound this bead can move.
    /// (Say what that 280 counts: rows whose segmentation is GONE from the
    /// database, which is not the same population as the rows stage 6 answers
    /// `.unreadable` for, nor the count in kg6i's own title. Three quantities,
    /// three measurements; quote whichever you actually took.)
    static func attribution(
        for extent: Extent,
        in rows: [SemanticScanResult]
    ) -> Attribution {
        var sawRefinement = false
        var sawSpan = false
        var sawJudged = false
        var refinement = Refinement()

        for row in rows where row.scanPass == refinementScanPass {
            guard row.disposition == .containsAd,
                  row.didExamineWindow,
                  row.windowStartTime.isFinite, row.windowEndTime.isFinite,
                  row.windowEndTime > row.windowStartTime,
                  extent.overlaps(start: row.windowStartTime, end: row.windowEndTime)
            else { continue }
            sawRefinement = true
            for span in refinedSpans(of: row) {
                sawSpan = true
                guard let dimensions = span.attributableDimensions else { continue }
                sawJudged = true
                refinement.anchorKinds.formUnion(dimensions.anchorKinds)
                if let intent = dimensions.commercialIntent {
                    refinement.commercialIntents.insert(intent)
                }
                if let ownership = dimensions.ownership {
                    refinement.ownerships.insert(ownership)
                }
            }
        }

        guard sawRefinement else { return .unrefined }
        guard sawSpan else { return .unreadable }
        return sawJudged ? .refined(refinement) : .suppressed
    }

    /// The refined spans persisted on one `passB` row, or none.
    ///
    /// The ARRAY shape only — `encodeRefinedSpans` is the only writer of a
    /// refinement payload and it always writes an array. A payload that will
    /// not decode as one yields nothing, which ``attribution(for:in:)`` reports
    /// as ``Attribution/unreadable`` rather than as an absence of refinement.
    private static func refinedSpans(of row: SemanticScanResult) -> [PersistedCertainty] {
        guard let data = row.spansJSON.data(using: .utf8),
              let spans = try? JSONDecoder().decode([PersistedCertainty].self, from: data)
        else { return [] }
        return spans
    }

    // MARK: - Attribution, persisted

    /// The `evidenceSources` token this composer stamps first on every mark, so
    /// a reader can tell a sweep mark's projection from any other producer's
    /// use of the column. Deliberately the same literal as ``metadataSource``.
    static let evidenceSourceLane = metadataSource

    /// The token that says WHICH of the four attributions a mark got.
    static func evidenceSourceState(of attribution: Attribution) -> String {
        switch attribution {
        case .unrefined: "unrefined"
        case .unreadable: "refinementUnreadable"
        case .suppressed: "refinementSuppressed"
        case .refined: "refined"
        }
    }

    /// The attribution, rendered into the `evidenceSources` column.
    ///
    /// # Why this column and not `advertiser`
    ///
    /// `advertiser`, `product` and `adDescription` are BANNER COPY, and a
    /// change to what a banner says is Dan's. They are also unfillable from
    /// what is persisted: an anchor carries a `kind` and a `lineRef`, never the
    /// brand's TEXT, so the honest options were a whole transcript line in a
    /// field the UI prints verbatim, or nothing. `metadataConfidence` stays
    /// `nil` and the banner copy stays generic and no-hallucination, exactly as
    /// before. `evidenceSources` is the column that already means "which
    /// evidence channels back this window", it is NULL on all 79 sweep marks
    /// today, and no view reads it.
    ///
    /// # The one consequence, named rather than absorbed
    ///
    /// `AdWindowMaterialIdentity.producerRevisionToken` hashes this column, so
    /// a sweep mark's suggest-card identity now moves when a later scan changes
    /// its attribution. That is the SAME consequence playhead-92im accepted for
    /// `confidence` and it lands the same way: the mark's `id` is content-
    /// addressed on geometry alone, so the row is updated in place rather than
    /// orphaned, and a recompose over unchanged rows is byte-identical.
    ///
    /// # The form
    ///
    /// A JSON array of strings — one of the two shapes
    /// `AnalysisStore+CrossUserSharing` already parses out of this column; the
    /// other is comma-separated text. (The local-learning corpus's own
    /// `evidenceSources` is the same shape. It is described rather than NAMED
    /// here on purpose: `trainingExamplesRemainInsideLocalLearningBoundary`
    /// enumerates production sources for that type's NAME and asserts the set
    /// is a subset of seven allowlisted files, and a text scan cannot tell a
    /// doc comment from a consumer. Widening the allowlist to admit a comment
    /// would grant this file a standing licence for a real egress consumer
    /// later, which is the opposite of what that canary is for.)
    /// Order is CANONICAL (lane, state, then each group sorted) so two
    /// composes over the same rows produce byte-identical text and the
    /// revision token above is stable; a `Set`'s own iteration order is seeded
    /// per process and would not be.
    ///
    /// `nil` when the array will not encode, which cannot happen for `[String]`
    /// — an unwritable projection writes NOTHING rather than something wrong,
    /// which leaves the column exactly where it was before this bead.
    static func evidenceSources(for attribution: Attribution) -> String? {
        var tokens = [evidenceSourceLane, evidenceSourceState(of: attribution)]
        if case .refined(let refinement) = attribution {
            tokens += refinement.anchorKinds.map { "anchor:\($0.rawValue)" }.sorted()
            tokens += refinement.commercialIntents.map { "intent:\($0.rawValue)" }.sorted()
            tokens += refinement.ownerships.map { "ownership:\($0.rawValue)" }.sorted()
        }
        guard let data = try? JSONEncoder().encode(tokens) else { return nil }
        return String(data: data, encoding: .utf8)
    }

    // MARK: - Stage 7: emit

    /// The EXTENT this producer can prove, and the single source of truth for
    /// both the row's anchor columns and — through ``ComposedMarkGate`` — its
    /// `eligibilityGate` (playhead-mqqd).
    ///
    /// `.unanchored` on both edges, and it is a MEASURED claim about the lane
    /// rather than a policy preference:
    ///
    ///   * A stage-1/2 extent is the coarse lane's OWN window geometry — a ~95 s
    ///     tile, or pass B's narrowing of one. Nobody observed a boundary there;
    ///     the model reported presence over a region it was handed.
    ///   * A stage-4 CLIP does not change this, and that is the pre-existing
    ///     argument this bead deliberately did not overturn: an anchor sitting
    ///     within `anchorClipRadiusSeconds` of an FM edge proves that A boundary
    ///     is there, not that it is THIS ad's boundary. Calling the clipped edge
    ///     proven would ship a neighbouring row's evidence as this row's, which
    ///     is the substitution playhead-2350 exists to have undone. It would
    ///     also be self-feeding: ``provenAnchorEdges`` harvests anchors off
    ///     persisted rows, so a sweep mark that minted its own anchor would
    ///     become clip material for the next sweep mark.
    ///   * Stage 5 refuses to emit over any existing window, so this producer's
    ///     population and the anchored population are nearly disjoint by
    ///     construction — there is usually no anchor within reach to argue about.
    ///
    /// Stated as a constant BECAUSE IT IS ONE, today. What it is not is three
    /// constants that have to agree: `makeMark` reads this value for the anchor
    /// columns and hands the same value to the gate, so a change here moves the
    /// eligibility with it and cannot silently fail to.
    static let extentSupport: SpanExtentSupport = .unanchored

    /// Build the content-addressed mark-only `AdWindow` for a surviving extent.
    ///
    /// playhead-6ruv: `attribution` has NO DEFAULT, and that is the point. A
    /// defaulted `.unrefined` is a value that would read "the model named
    /// nothing" for a caller that simply forgot to ask — an absence
    /// manufactured out of an omission, which is this bead's own defect class.
    /// It moves ``AdWindow/evidenceSources`` and NOTHING else; every other
    /// field below is computed exactly as it was.
    static func makeMark(
        _ extent: Extent,
        attribution: Attribution,
        analysisAssetId: String
    ) -> AdWindow {
        // playhead-mqqd: ONE value, used twice — the anchor columns below and
        // the gate derived from it. Never two expressions that happen to agree.
        let support = extentSupport
        return AdWindow(
            id: markId(
                analysisAssetId: analysisAssetId,
                start: extent.start,
                end: extent.end
            ),
            analysisAssetId: analysisAssetId,
            startTime: extent.start,
            endTime: extent.end,
            // playhead-92im: the extent's own graded evidence, never a constant.
            confidence: extent.confidence,
            boundaryState: boundaryState,
            // NEVER confirmed/applied. A verdict is a proposal.
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: metadataSource,
            // nil → generic no-hallucination banner copy.
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            // playhead-6ruv: WHETHER THE MODEL NAMED AN ADVERTISER, projected
            // from the refinement pass that was asked exactly that question and
            // whose answer this composer read nothing of. See ``Attribution``.
            evidenceSources: evidenceSources(for: attribution),
            // playhead-mqqd: DERIVED from `support`, not typed. It reads
            // `markOnly` today because the coarse lane proved no edge — see
            // `extentSupport` for why it cannot — and it will read whatever the
            // extent earns if that ever changes. It was a hard-coded literal,
            // which made it a policy decision no evidence could revisit and
            // left it free to disagree with the two anchor columns below.
            eligibilityGate: ComposedMarkGate.eligibility(for: support).rawValue,
            catalogStoreMatchSimilarity: nil,
            // The same `support`, so the row's stamp and its evidence are one
            // statement. Saying "the coarse lane proved no edge" is what keeps
            // playhead-2350's gate true by construction rather than by
            // evaluation.
            startEdgeAnchor: support.startAnchor.rawValue,
            endEdgeAnchor: support.endAnchor.rawValue
        )
    }

    /// Content-addressed id: `sweep-<16 hex>` over
    /// `asset=…|version=semantic-sweep-v1|start=…|end=…`. Mirrors
    /// `SpecialistMarkComposer.markId`, so an identical recompose mints the
    /// identical id, the version-scoped reconcile retires nothing, and the
    /// store's INSERT-OR-REPLACE is a true no-op.
    static func markId(analysisAssetId: String, start: Double, end: Double) -> String {
        let canonical =
            "asset=\(analysisAssetId)|version=\(detectorVersion)|start=\(start)|end=\(end)"
        let digest = SHA256.hash(data: Data(canonical.utf8))
        let hex = digest.map { String(format: "%02x", $0) }.joined()
        return "sweep-\(hex.prefix(16))"
    }
}
