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
//      coarse presence verdict stands with its coarse extent: pass A said an
//      ad is here, pass B failed to localize it, and a failure to localize is
//      not a retraction (playhead-ynmk — a confirmation asserts presence,
//      never extent). This matters concretely because
//      `BackfillJobRunner.swift:3645` persists an empty pass-B result as
//      `.noAds`.
//
//   3. MERGE. The sweep tiles ~95 s windows front to back, so one 3-minute pod
//      lands across two of them. Two touching banners for one ad break is a
//      worse surface than one. Bounded by `mergeGapSeconds` so two genuinely
//      separate breaks never fuse across the show between them.
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
//   6. EMIT. `eligibilityGate == .markOnly` as a HARD-CODED literal, never
//      derived from a policy switch; `decisionState == .candidate`; BOTH edge
//      anchors `.unanchored`; `metadataConfidence == nil` so the banner copy is
//      generic and no advertiser is hallucinated. Content-addressed id, so a
//      recompose over unchanged inputs is a true no-op.
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
//      from the presence rows under each one: 6 stay at 0.70 and 16 fall below
//      (13 of 22 at or above 0.525, 20 of 22 at or above 0.40, minimum 0.093),
//      so those 16 would no longer hydrate on the next launch. Both marks Dan
//      vetoed grade at 0.467, with 16 of 22 grading ABOVE them. That is a REACH
//      change and it is NOT decided here: `preloadConfidenceThreshold` is
//      playhead-atr3's settled territory and is untouched.
//
//      THE OTHER LIVE READER, from the same grep:
//      `FinalPassRetranscriptionRunner.defaultConfidenceFloor` (0.50) on the
//      raw `confidence`. 6 of the 22 fall below it and would no longer earn a
//      final-pass re-transcription of their audio. Also Dan's, and arguably the
//      budget landing where the evidence is — but it is a change, so it is
//      named rather than absorbed.
//
//      TWO 0.70 GATES THAT LOOK LIVE AND ARE NOT, checked rather than assumed:
//      `AnalysisJobRunner.isCueWindow` and the cross-user shareable-cue
//      predicate both ALSO require `SkipEligibilityGate.eligible`, and every
//      sweep mark is `.markOnly` by hard-coded literal — so neither has ever
//      seen a sweep mark at any confidence. A sweep mark is additionally
//      local-only (`isLocalOnlyBoundaryState`), so it is never exported.
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
    /// verdict, not a score" — was simply not true of the persisted rows.
    /// EVERY `containsAd` row carries the model's OWN ``CertaintyBand`` in
    /// `spansJSON` (`CoarseSupportSchema.certainty` for `passA`, the refined
    /// spans' own bands for `passB`): 55 of 55 coarse and 11 of 11 refined rows
    /// on that pull, split 40 `strong` / 15 `moderate`. The composer simply
    /// never read it.
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
    /// The sweep genuinely re-screens: on the device pull 36 of 125 coarse
    /// windows carry more than one `passA` row, each a separate FM call in a
    /// separate backfill run (distinct `runCorrelationId`, distinct latency),
    /// and four windows are ones where `containsAd` is the MINORITY verdict.
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
        maximumMarkConfidence
            * certaintyFactor(band)
            * transcriptQualityFactor(quality)
            * corroborationFactor(affirming: affirming, dissenting: dissenting)
    }

    /// Minimal decode of the `certainty` key out of a persisted support
    /// payload. Deliberately reads ONLY that key so the two payload shapes and
    /// every future field they gain decode without this composer knowing about
    /// them — the same narrow-decode pattern `AdDetectionService`'s
    /// `SpanTrustDecode` uses over the same column.
    private struct PersistedCertainty: Decodable {
        let certainty: CertaintyBand?
    }

    /// The model's OWN certainty for this row's presence claim, or `nil` when
    /// it expressed none.
    ///
    /// Two persisted shapes, because two writers:
    ///   • `passA` — ONE `CoarseSupportSchema` OBJECT
    ///     (`BackfillJobRunner.encodeSupport`), e.g.
    ///     `{"supportLineRefs":[17,18,20],"certainty":"strong"}`.
    ///   • `passB` — an ARRAY of refined spans
    ///     (`encodeRefinedSpans`), each carrying its own band.
    ///
    /// For an array the WEAKEST band governs, because the extent this row backs
    /// covers all of its spans and a mark is only as good as its weakest
    /// second. `"[]"` — what `encodeSupport` writes for a nil support — decodes
    /// to an empty array and correctly yields `nil`.
    static func certaintyBand(of row: SemanticScanResult) -> CertaintyBand? {
        guard let data = row.spansJSON.data(using: .utf8) else { return nil }
        let decoder = JSONDecoder()
        if let support = try? decoder.decode(PersistedCertainty.self, from: data) {
            return support.certainty
        }
        guard let spans = try? decoder.decode([PersistedCertainty].self, from: data) else {
            return nil
        }
        return spans
            .compactMap(\.certainty)
            .min { certaintyFactor($0) < certaintyFactor($1) }
    }

    /// Count the PRESENCE-pass rows that examined this extent and say whether
    /// each one affirmed an ad in it.
    ///
    /// Scoped to the presence pass on purpose: `SemanticScanResult`'s own
    /// documentation is that `passB`'s `.noAds` means "found no edges", never
    /// "there is no ad", so a refinement that failed to localize must not be
    /// counted as a replicate voting against presence.
    static func corroboration(
        for extent: Extent,
        in rows: [SemanticScanResult]
    ) -> (affirming: Int, dissenting: Int) {
        var affirming = 0
        var dissenting = 0
        for row in rows where row.scanPass != refinementScanPass {
            guard row.didExamineWindow,
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
    static func compose(
        scanRows: [SemanticScanResult],
        existingWindows: [AdWindow],
        provenAnchorEdges: [Double]? = nil,
        analysisAssetId: String
    ) -> [AdWindow] {
        let anchors = provenAnchorEdges ?? Self.provenAnchorEdges(in: existingWindows)

        // Stages 1–2: presence, refined by pass B where pass B localized it.
        let presence = presenceExtents(scanRows)
        guard !presence.isEmpty else { return [] }

        // Stage 3: merge.
        let merged = mergeExtents(presence)

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

        // Stage 6: emit.
        return survivors.map { makeMark($0, analysisAssetId: analysisAssetId) }
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

    /// Build an extent and grade it from the rows it rests on plus every
    /// presence-pass replicate that examined the same audio.
    private static func scored(
        start: Double,
        end: Double,
        restingOn backing: [SemanticScanResult],
        in rows: [SemanticScanResult]
    ) -> Extent {
        var extent = Extent(start: start, end: end)
        let counts = corroboration(for: extent, in: rows)
        extent.confidence = backing
            .map {
                markConfidence(
                    band: certaintyBand(of: $0),
                    transcriptQuality: $0.transcriptQuality,
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
    static func mergeExtents(_ extents: [Extent]) -> [Extent] {
        let sorted = extents.sorted {
            $0.start != $1.start ? $0.start < $1.start : $0.end < $1.end
        }
        var result: [Extent] = []
        for extent in sorted {
            if var last = result.last,
               extent.start <= last.end + mergeGapSeconds,
               max(last.end, extent.end) - last.start <= maximumMarkDurationSeconds {
                let held = last.duration
                let added = max(0, extent.end - last.end)
                if held + added > 0 {
                    // INTERPOLATE, do not average as a sum of products.
                    // `(a*h + b*w)/(h+w)` is algebraically the same and
                    // numerically is NOT: for a == b it lands a few ULP off,
                    // and a run of `strong`/`good` windows would then merge to
                    // 0.6999999999999999 and fall out of a `>= 0.70` gate for
                    // no reason a reader could ever see. Measured: 2 of the 6
                    // ceiling-grade marks in the 2026-08-10 pull did exactly
                    // that. This form returns `last.confidence` EXACTLY when
                    // the two agree, and stays inside `[min, max]` of the two
                    // when they do not.
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

    // MARK: - Stage 6: emit

    /// Build the content-addressed mark-only `AdWindow` for a surviving extent.
    static func makeMark(_ extent: Extent, analysisAssetId: String) -> AdWindow {
        AdWindow(
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
            evidenceSources: nil,
            // ALWAYS markOnly — a hard-coded literal, never a policy switch.
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            catalogStoreMatchSimilarity: nil,
            // The coarse lane proved no edge. Saying so is what keeps
            // playhead-2350's gate true by construction rather than by
            // evaluation — and a clip in stage 4 does NOT change this, because
            // an anchor near an FM edge proved that a boundary is there, not
            // that it is THIS ad's boundary.
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
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
