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

    /// The `confidence` stamped on every mark. It is NOT a calibrated
    /// probability — the coarse lane produces a categorical verdict, not a
    /// score, and inventing a probability here is exactly the presence-shaped
    /// reasoning `SpanExtentSupport` refuses to manufacture for extent. It is
    /// pinned at `SkipOrchestrator.preloadConfidenceThreshold` because that is
    /// the ONE thing the number has to do: clear the cross-launch preload floor
    /// so the mark is still visible on the next launch.
    static let markConfidence = 0.70

    /// Coarse windows whose gap is at most this are one ad break, not two.
    /// The sweep tiles contiguous windows, so touching windows (gap 0) are the
    /// common case; the slack absorbs float drift at a shared boundary.
    static let mergeGapSeconds = 2.0

    /// Marks shorter than this are not emitted. Below
    /// `InventorySanityFilter`'s duration floor a mark cannot survive ingest,
    /// so emitting it would only add census rows naming a drop nobody can act
    /// on.
    static let minimumMarkDurationSeconds = 2.0

    /// How far from an edge a proven boundary may sit and still clip it. An
    /// anchor further away than this belongs to something else, and snapping to
    /// it would INVENT extent — the failure playhead-2350 documented.
    static let anchorClipRadiusSeconds = 20.0

    /// The pass label whose rows are the model's own narrowing of a coarse
    /// window. Mirrors `BackfillJobRunner`'s literal; `SemanticScanCoverage`
    /// owns the `passA` counterpart.
    static let refinementScanPass = "passB"

    // MARK: - Extent

    /// A candidate extent in seconds. Carries no score and no provenance —
    /// this composer never claims either.
    struct Extent: Equatable, Sendable {
        var start: Double
        var end: Double

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
        let clipped = merged.map { clip($0, toAnchors: anchors) }

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
        let refinements = admissible
            .filter { $0.scanPass == refinementScanPass }
            .map { Extent(start: $0.windowStartTime, end: $0.windowEndTime) }
        let coarse = admissible
            .filter { $0.scanPass != refinementScanPass }
            .map { Extent(start: $0.windowStartTime, end: $0.windowEndTime) }

        var result: [Extent] = []
        var claimedRefinements = Set<Int>()
        for window in coarse {
            var narrowed: [Extent] = []
            for (index, refinement) in refinements.enumerated()
            where refinement.overlaps(start: window.start, end: window.end) {
                claimedRefinements.insert(index)
                narrowed.append(
                    Extent(
                        start: max(window.start, refinement.start),
                        end: min(window.end, refinement.end)
                    )
                )
            }
            result.append(contentsOf: narrowed.isEmpty ? [window] : narrowed)
        }
        // A refinement inside no coarse containsAd window is itself a verdict
        // and stands alone. Dropping it would rebuild, one layer down, the
        // "presence needs a host to attach to" rule this bead removes.
        for (index, refinement) in refinements.enumerated()
        where !claimedRefinements.contains(index) {
            result.append(refinement)
        }
        return result.filter { $0.duration >= minimumMarkDurationSeconds }
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
    /// `mergeGapSeconds` of the running extent's end.
    static func mergeExtents(_ extents: [Extent]) -> [Extent] {
        let sorted = extents.sorted {
            $0.start != $1.start ? $0.start < $1.start : $0.end < $1.end
        }
        var result: [Extent] = []
        for extent in sorted {
            if var last = result.last, extent.start <= last.end + mergeGapSeconds {
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
        let clipped = Extent(
            start: startCandidates.min() ?? extent.start,
            end: endCandidates.max() ?? extent.end
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
            confidence: markConfidence,
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
