// AdPodContinuation.swift
// playhead-xsdz.65: one banner, whole break — recover the ad-pod NEIGHBOURS of
// an ad we already found.
//
// THE DEFECT THIS ADDRESSES
// -------------------------
// A DAI ad pod is a CHAIN of stitched creatives, not one ad. Measured on
// 2026-07-30 against rediff-confirmed slots (byte-derived pod boundaries, so
// independent of any human labelling): of the 17 slots the pipeline detected at
// all, 14 were covered by exactly ONE pipeline window, mean slot coverage 0.33,
// and 13 slots carried an uncovered run longer than 30 s — 994 s of ad audio
// sitting INSIDE pods we had already located, with a largest single hole of
// 175 s. A 175 s hole is not an edge that stopped short; it is two or three
// whole additional ads. Concretely, on `conan-2026-07-09` the pipeline emitted
// `[797.2, 810.7)` inside the pod `[~657.6, 810.7)` — it caught the tail of the
// FOURTH ad and left the SiriusXM promo, Carvana and Carter's reads untouched.
// That is exactly what the product owner hears: a banner fires, one ad is
// handled, and the next ad plays.
//
// Distinct from playhead-4xqf (single-span UNDERSIZING, a <30 s edge that
// stopped short). This type only ever adds material that has its OWN ad
// evidence; it never nudges an edge to "look better".
//
// WHAT THIS DOES — AND WHY IT CANNOT EAT THE SHOW
// -----------------------------------------------
// "Keep detecting until content resumes" is a WIDENING rule, and widening into
// show content is the failure this product cannot afford: the inner edges
// matter more than the outer ones, and eating show is far worse than missing an
// ad. So the rule is built so that every second it claims is claimed on
// POSITIVE evidence, and it is stopped by POSITIVE evidence that content
// resumed. Two structural properties do that work:
//
//  1. THE WALK TERMINATES AT EVIDENCE, NEVER PAST IT. From a seed window we
//     step outward link by link, where a LINK is a region in which the vetted
//     high-precision lexical auto-ad rule (`LexicalAutoAdEvidenceBuilder`,
//     playhead-xsdz.1: a sponsor disclosure co-occurring with a promo code or
//     URL CTA, with its negative-evidence guardrails applied) FIRES. The
//     claimed extension ends exactly at the last link's end. Nothing beyond
//     the final piece of positive ad evidence is ever claimed, so the rule
//     never reasons "no ad cue was found here, therefore this is still ad" —
//     an absence is never an input.
//
//     The seconds BETWEEN two links are claimed, and that is deliberate: they
//     are BRACKETED by positive ad evidence on both sides (the seed or previous
//     link before, the next link after). Bracketing is positive evidence about
//     the interior of a pod — it is what makes this a pod-continuation rule
//     rather than an extrapolation.
//
//  2. A POSITIVE CONTENT-RESUMED BARRIER STOPS THE WALK. A barrier is an
//     AFFIRMATIVE verdict that the audio is show content, not the absence of an
//     ad verdict:
//       • an FM `noAds` scan window over good-quality transcript — the primary
//         classifier was asked about that region and answered "there are no ads
//         here". That is the same signal `FMSuppressionGuard` already trusts to
//         downweight real evidence, used here in the same direction.
//       • an explicit spoken return marker (`LexicalPatternCategory
//         .transitionMarker`: "and now back to the show", "without further
//         ado", …) — the host saying, in words, that the break is over. This is
//         the same cue `TimeBoundaryResolver` already scores as
//         `explicitReturnMarker`.
//     A barrier intersecting the corridor TERMINATES the walk on that side; it
//     does not merely skip one step, because stepping past a wall is crossing
//     it.
//
// SAFETY CONTRACT (each item is pinned by a test)
// -----------------------------------------------
//   • MARK-ONLY, ALWAYS. Every emitted row is a NEW window with a hard-coded
//     `eligibilityGate == .markOnly`, `decisionState == .candidate`, and BOTH
//     edge anchors `.unanchored`. Under playhead-2350 an unanchored edge can
//     never auto-skip, so recovered pod material becomes a BANNER — which is
//     exactly the certainty-tiered policy: auto-skip only the deterministic
//     (byte-exact rediff) material, MARK the rest.
//   • IT NEVER TOUCHES AN EXISTING WINDOW. No seed's geometry, gate, anchors,
//     confidence or id is modified — the pass is purely additive. That makes
//     playhead-ye0n's rule (never demote a whole window for extending one edge)
//     satisfied by construction rather than by argument: there is no edge
//     rewrite and no gate arithmetic anywhere in this file.
//   • IT NEVER CROSSES A LISTENER'S MARK (playhead-lc4c). `protectedRegions`
//     are supplied by the caller because persisted `userMarked` rows are not in
//     the window list this pass receives. A protected region intersecting the
//     corridor terminates the walk, and user-marked rows are never used as
//     seeds — a manual mark outranks anything derived, in both directions.
//
// PURITY: pure functions over value types, `Foundation`-only, deterministic, no
// I/O and no actor hops — the `SpecialistMarkComposer` shape, so the whole
// contract is unit-testable with synthetic rows.

import CryptoKit
import Foundation

enum AdPodContinuation {

    // MARK: - Provenance constants

    /// Detector version stamped on every continuation mark. Reconcile isolation
    /// scopes to this exact string (`AdDetectionService
    /// .isReconcilableBackfillWindow`), so continuation marks and FM
    /// (`detection-v1`) / specialist (`specialist-ft-v2`) marks can never retire
    /// one another.
    static let detectorVersion = "pod-continuation-v1"

    /// `boundaryState` stamped on every continuation mark. A NON-user literal
    /// that MUST stay OUT of `AdDetectionService.reconcileProtectedBoundaryStates`
    /// so this pass can retire its own stale rows on a re-run.
    static let boundaryState = "podContinuation"

    /// `metadataSource` stamped on every continuation mark. Paired with
    /// `metadataConfidence == nil` so `AdBannerView.bannerCopy` uses the generic
    /// "sounds like a sponsor break" copy — we know a pod continues, we do NOT
    /// know which advertiser, and guessing one would be a hallucination.
    static let metadataSource = "pod-continuation-v1"

    // MARK: - Configuration

    struct Configuration: Sendable, Equatable {
        /// Maximum silence-or-speech gap (seconds) between the current edge and
        /// the next ad-copy link for the chain to continue.
        ///
        /// Inside a DAI pod, consecutive creatives are butt-spliced or separated
        /// by a beat, and the sponsor disclosure of the next creative lands
        /// within a sentence or two of the previous one's CTA. 30 s covers a
        /// full creative whose disclosure comes late while staying far below the
        /// distance at which "the next sponsor mention" stops being the same
        /// break. `<= 0` disables the pass (no chain can ever start).
        var maxLinkGapSeconds: Double

        /// Hard bound on how far one side of one seed may be extended.
        ///
        /// This is a BLAST-RADIUS BOUND, not the safety control — the safety
        /// control is that every step must land on positive ad evidence and
        /// must not cross a content barrier. It exists so that a pathological
        /// hit stream (e.g. a shopping-format show that says "sponsor" and a URL
        /// every 20 s for an hour) cannot produce an hour-long mark. The widest
        /// rediff-confirmed pod in the 2026-07-16 corpus is ~212 s end to end,
        /// so 180 s per side cannot be reached by a legitimate pod chain unless
        /// the seed itself is already most of the pod.
        var maxExtensionSecondsPerSide: Double

        /// Shortest continuation mark worth emitting, in seconds.
        ///
        /// The recovered span is reduced to the material NOTHING already covers
        /// (see ``compose(existingWindows:adCopyLinks:contentBarriers:protectedRegions:episodeDuration:analysisAssetId:config:)``),
        /// and that subtraction can leave sub-second slivers at the edges of an
        /// already-detected creative. A sliver is not an ad the listener hears; it
        /// is arithmetic. 3 s is below any real creative and above the
        /// transcript-interpolation slop that produces the slivers.
        var minMarkDurationSeconds: Double

        /// Ceiling for the confidence stamped on a continuation mark.
        ///
        /// A mark inherits the seed's PRESENCE confidence (the claim is "this is
        /// the same pod"), capped here: a continuation is never more certain
        /// than the ad it continues, and is never presented as more than a
        /// suggestion. 0.70 is `SkipOrchestrator.preloadConfidenceThreshold`, so
        /// a mark from a normally-confident seed still survives cross-launch
        /// preload like every other mark-only row. It is NOT a calibrated
        /// probability and it feeds no gate: the gate is the hard-coded
        /// `.markOnly` literal below.
        var markConfidenceCeiling: Double

        static let `default` = Configuration(
            maxLinkGapSeconds: 30.0,
            maxExtensionSecondsPerSide: 180.0,
            minMarkDurationSeconds: 3.0,
            markConfidenceCeiling: 0.70
        )

        init(
            maxLinkGapSeconds: Double = 30.0,
            maxExtensionSecondsPerSide: Double = 180.0,
            minMarkDurationSeconds: Double = 3.0,
            markConfidenceCeiling: Double = 0.70
        ) {
            self.maxLinkGapSeconds = maxLinkGapSeconds
            self.maxExtensionSecondsPerSide = maxExtensionSecondsPerSide
            self.minMarkDurationSeconds = minMarkDurationSeconds
            self.markConfidenceCeiling = markConfidenceCeiling
        }
    }

    // MARK: - Inputs

    /// A region in which the vetted lexical auto-ad rule FIRED — i.e. positive,
    /// guardrailed evidence that commercial copy is being read here. Produced by
    /// ``adCopyLinks(hits:analysisAssetId:builder:)`` so the authority on "is
    /// this ad copy" stays `LexicalAutoAdEvidenceBuilder` and cannot drift.
    struct AdCopyLink: Sendable, Equatable, Hashable {
        let start: Double
        let end: Double

        init(start: Double, end: Double) {
            self.start = start
            self.end = end
        }
    }

    /// A region POSITIVELY classified as show content. Never "we found no ad
    /// evidence here" — see ``contentBarriers(semanticScanResults:lexicalHits:)``
    /// for the two admissible derivations and why each is an affirmative verdict.
    struct ContentBarrier: Sendable, Equatable, Hashable {
        let start: Double
        let end: Double

        init(start: Double, end: Double) {
            self.start = start
            self.end = end
        }
    }

    /// Decision states that make an existing window "visible" — i.e. real enough
    /// to seed a chain from and to dedupe against. A suppressed or reverted row
    /// is not something the listener sees, so it neither seeds nor blocks.
    /// Mirrors `SpecialistMarkComposer.visibleDecisionStates`.
    static let visibleDecisionStates: Set<String> = [
        AdDecisionState.candidate.rawValue,
        AdDecisionState.confirmed.rawValue,
        AdDecisionState.applied.rawValue
    ]

    /// Decision states that may SEED a continuation chain. Deliberately narrower
    /// than `visibleDecisionStates`: a `.candidate` row is a coarse proposal (the
    /// segment aggregator tiles fixed 30 s regions as candidates), and chaining
    /// off an unconfirmed tile would build a pod claim on a guess. Only a
    /// verdict the pipeline actually confirmed anchors a chain.
    static let seedDecisionStates: Set<String> = [
        AdDecisionState.confirmed.rawValue,
        AdDecisionState.applied.rawValue
    ]

    // MARK: - Link derivation (the positive ad-evidence half)

    /// Project the asset's lexical hit stream into ad-copy links by asking the
    /// VETTED rule, per candidate region, whether it fires.
    ///
    /// Candidate regions are formed from sponsor × (promoCode | urlCTA) hit
    /// pairs that are close enough in time to be one read. Each candidate is
    /// then handed to `LexicalAutoAdEvidenceBuilder`, which is the sole judge:
    /// if it emits no entry (show-owned-domain negative pattern, editorial
    /// context, metadata-only legs, …) there is no link. Re-implementing that
    /// rule here would let the two copies drift, and the drift would be silent.
    ///
    /// The region handed to the builder is PADDED by the builder's own negative-
    /// context radius so its guardrails can see the neighbourhood, while the
    /// LINK we return is the tight pair interval. Both choices fail closed:
    /// padding can only add suppressors, and the tight interval claims less.
    static func adCopyLinks(
        hits: [LexicalHit],
        analysisAssetId: String,
        builder: LexicalAutoAdEvidenceBuilder = LexicalAutoAdEvidenceBuilder(),
        config: LexicalAutoAdEvidenceBuilder.Config = .default
    ) -> [AdCopyLink] {
        let usable = hits.filter {
            $0.startTime.isFinite && $0.endTime.isFinite && $0.endTime >= $0.startTime
        }
        guard !usable.isEmpty else { return [] }

        let sponsors = usable.filter {
            $0.category == .sponsor && !$0.isNegativePattern && !$0.isMetadataOrigin
        }
        let ctas = usable.filter {
            ($0.category == .promoCode || $0.category == .urlCTA)
                && !$0.isNegativePattern && !$0.isMetadataOrigin
        }
        guard !sponsors.isEmpty, !ctas.isEmpty else { return [] }

        // Distinct candidate intervals only. A show can mention one sponsor and
        // one URL many times, and every duplicated pair would otherwise cost a
        // full builder evaluation (regex work) for an interval already decided.
        var candidates = Set<AdCopyLink>()
        for sponsor in sponsors {
            for cta in ctas {
                guard abs(cta.startTime - sponsor.startTime) <= config.cooccurrenceWindow else {
                    continue
                }
                let start = min(sponsor.startTime, cta.startTime)
                let end = max(sponsor.endTime, cta.endTime)
                guard end > start else { continue }
                candidates.insert(AdCopyLink(start: start, end: end))
            }
        }
        guard !candidates.isEmpty else { return [] }

        var links: [AdCopyLink] = []
        for candidate in candidates.sorted(by: {
            $0.start != $1.start ? $0.start < $1.start : $0.end < $1.end
        }) {
            // A candidate already inside an accepted link adds nothing.
            if links.contains(where: { $0.start <= candidate.start && $0.end >= candidate.end }) {
                continue
            }
            // The builder is the authority. Pad the probe so its
            // negative-context guardrail can see nearby hits.
            let probe = DecodedSpan(
                id: "pod-continuation-probe",
                assetId: analysisAssetId,
                firstAtomOrdinal: 0,
                lastAtomOrdinal: 0,
                startTime: candidate.start - config.negativeContextRadius,
                endTime: candidate.end + config.negativeContextRadius,
                anchorProvenance: []
            )
            guard !builder.buildEntries(hits: usable, for: probe).isEmpty else { continue }
            links.append(candidate)
        }
        return mergeLinks(links)
    }

    /// Sort and union overlapping/touching links so the chain walk sees one
    /// interval per contiguous run of ad copy.
    static func mergeLinks(_ links: [AdCopyLink]) -> [AdCopyLink] {
        let merged = unionIntervals(links.map { (start: $0.start, end: $0.end) })
        return merged.map { AdCopyLink(start: $0.start, end: $0.end) }
    }

    // MARK: - Barrier derivation (the positive content-resumed half)

    /// Project FM scan rows and the lexical hit stream into positive
    /// content-resumed barriers.
    ///
    /// TWO ADMISSIBLE SOURCES, both affirmative:
    ///
    ///  • An FM scan row whose `disposition == .noAds` with `status == .success`
    ///    over `transcriptQuality == .good`. The classifier was asked about that
    ///    window and ANSWERED that it holds no ads. The quality condition is the
    ///    same one `AdDetectionService.applyFMSuppression` uses to lift such a
    ///    row to `CertaintyBand.moderate`, so "an FM noAds we are willing to act
    ///    on" has one definition in this codebase. `.uncertain` and `.abstain`
    ///    rows are NOT barriers — an abstention is an absence, and absences are
    ///    exactly what must not become evidence here.
    ///
    ///  • An explicit spoken return marker (`.transitionMarker`): the host
    ///    saying the break is over. Same cue `TimeBoundaryResolver` scores as
    ///    `explicitReturnMarker`. Widened to `returnMarkerBarrierRadius` on each
    ///    side because the hit is an interpolated instant, not an interval, and a
    ///    zero-width barrier cannot block anything.
    ///
    /// Metadata-origin lexical hits are excluded: a feed-description phrase is
    /// not an utterance at a time, so it cannot mark where content resumed.
    static func contentBarriers(
        semanticScanResults: [SemanticScanResult],
        lexicalHits: [LexicalHit]
    ) -> [ContentBarrier] {
        var barriers: [ContentBarrier] = []

        for row in semanticScanResults {
            guard row.disposition == .noAds,
                  row.status == .success,
                  row.transcriptQuality == .good,
                  row.windowStartTime.isFinite,
                  row.windowEndTime.isFinite,
                  row.windowEndTime > row.windowStartTime else {
                continue
            }
            barriers.append(
                ContentBarrier(start: row.windowStartTime, end: row.windowEndTime)
            )
        }

        for hit in lexicalHits {
            guard hit.category == .transitionMarker,
                  !hit.isMetadataOrigin,
                  !hit.isNegativePattern,
                  hit.startTime.isFinite,
                  hit.endTime.isFinite,
                  hit.endTime >= hit.startTime else {
                continue
            }
            barriers.append(
                ContentBarrier(
                    start: hit.startTime - returnMarkerBarrierRadius,
                    end: hit.endTime + returnMarkerBarrierRadius
                )
            )
        }

        let merged = unionIntervals(barriers.map { (start: $0.start, end: $0.end) })
        return merged.map { ContentBarrier(start: $0.start, end: $0.end) }
    }

    /// Half-width (seconds) given to an explicit-return-marker barrier. A
    /// lexical hit's time is interpolated within its transcript chunk, so it
    /// carries a second or two of slop; a barrier narrower than that slop could
    /// be stepped over. Small enough that it cannot wall off a real neighbouring
    /// creative on its own.
    static let returnMarkerBarrierRadius = 2.0

    // MARK: - Compose

    /// Compose mark-only continuation windows for the ad-pod neighbours of the
    /// asset's confirmed ad windows.
    ///
    /// - Parameters:
    ///   - existingWindows: every persisted/fused window for the asset. Confirmed
    ///     non-user rows seed chains; all visible rows are used for dedupe.
    ///   - adCopyLinks: positive ad-copy evidence regions (see
    ///     ``adCopyLinks(hits:analysisAssetId:builder:config:)``).
    ///   - contentBarriers: positive content-resumed regions (see
    ///     ``contentBarriers(semanticScanResults:lexicalHits:)``).
    ///   - protectedRegions: time ranges the listener defined by hand. The walk
    ///     refuses to cross one. Must be supplied by the caller: persisted
    ///     `userMarked` rows may not be in `existingWindows`.
    ///   - episodeDuration: clamps every emitted mark to real audio. `<= 0`
    ///     means unknown and applies no clamp.
    /// - Returns: new mark-only `AdWindow`s covering recovered pod material.
    ///   Empty when nothing qualifies. Never returns a modified copy of an input
    ///   window.
    static func compose(
        existingWindows: [AdWindow],
        adCopyLinks: [AdCopyLink],
        contentBarriers: [ContentBarrier],
        protectedRegions: [(start: Double, end: Double)],
        episodeDuration: Double,
        analysisAssetId: String,
        config: Configuration = .default
    ) -> [AdWindow] {
        guard config.maxLinkGapSeconds.isFinite,
              config.maxLinkGapSeconds > 0,
              config.maxExtensionSecondsPerSide.isFinite,
              config.maxExtensionSecondsPerSide > 0 else {
            return []
        }
        let links = mergeLinks(adCopyLinks.filter {
            $0.start.isFinite && $0.end.isFinite && $0.end > $0.start
        })
        guard !links.isEmpty else { return [] }

        let barriers = contentBarriers.filter {
            $0.start.isFinite && $0.end.isFinite && $0.end > $0.start
        }
        // A degenerate protected region protects nothing — otherwise one bad row
        // becomes a permanent global veto (playhead-lc4c).
        let protected = protectedRegions.filter {
            $0.start.isFinite && $0.end.isFinite && $0.end > $0.start
        }
        let walls = barriers.map { (start: $0.start, end: $0.end) }
            + protected.map { (start: $0.start, end: $0.end) }

        let seeds = existingWindows.filter(isSeed)
        guard !seeds.isEmpty else { return [] }

        // Material the listener can ALREADY see, which the recovered span is
        // reduced against so this pass only ever adds genuinely-uncovered audio.
        //
        // Our OWN prior rows are excluded deliberately (mirroring
        // `SpecialistMarkComposer`'s dedupe): including them would make a re-run
        // find zero residue, emit nothing, and therefore RETIRE the rows it wrote
        // last time — a row that flickers in and out on every backfill.
        // Idempotency rides on content-addressed ids over identical residues, not
        // on self-suppression.
        let visible: [(start: Double, end: Double)] = existingWindows
            .filter {
                visibleDecisionStates.contains($0.decisionState)
                    && $0.detectorVersion != detectorVersion
                    && $0.startTime.isFinite
                    && $0.endTime.isFinite
                    && $0.endTime > $0.startTime
            }
            .map { (start: $0.startTime, end: $0.endTime) }

        var spans: [(start: Double, end: Double, confidence: Double)] = []
        for seed in seeds {
            let trailing = walkForward(from: seed, links: links, walls: walls, config: config)
            if let end = trailing, end > seed.endTime {
                spans.append((start: seed.endTime, end: end, confidence: seed.confidence))
            }
            let leading = walkBackward(from: seed, links: links, walls: walls, config: config)
            if let start = leading, start < seed.startTime {
                spans.append((start: start, end: seed.startTime, confidence: seed.confidence))
            }
        }
        guard !spans.isEmpty else { return [] }

        // Clamp to real audio, UNION the recovered spans, then reduce each to the
        // material NOTHING already covers.
        //
        // The union comes first because several seeds in one pod propose
        // overlapping spans — a seed either side of a hole both reach across it —
        // and subtracting per-span would emit two nearly-identical rows over the
        // same audio, i.e. two banners for one ad. Unioning makes the output
        // DISJOINT by construction.
        //
        // Emitting the residue, rather than dropping a mostly-covered span,
        // is what makes this pass move the quantity the bead measures: the
        // UNCOVERED runs inside a pod. It also means the pass can never
        // double-count — a hole between two already-detected creatives yields a
        // mark for the hole, not a second row over the creatives.
        let upperBound = episodeDuration.isFinite && episodeDuration > 0
            ? episodeDuration
            : Double.greatestFiniteMagnitude
        let clamped = spans.compactMap { span -> (start: Double, end: Double)? in
            let start = max(span.start, 0)
            let end = min(span.end, upperBound)
            return end > start ? (start: start, end: end) : nil
        }

        var marks: [AdWindow] = []
        for recovered in unionIntervals(clamped) {
            // Presence confidence for this stretch: the strongest seed that
            // reached it. Never averaged down, never raised above the ceiling.
            let confidence = spans
                .filter { $0.start < recovered.end && $0.end > recovered.start }
                .map(\.confidence)
                .max() ?? 0
            for residue in subtract(covered: visible, from: recovered) {
                guard residue.end - residue.start >= config.minMarkDurationSeconds else {
                    continue
                }
                marks.append(
                    makeMark(
                        start: residue.start,
                        end: residue.end,
                        confidence: min(confidence, config.markConfidenceCeiling),
                        analysisAssetId: analysisAssetId
                    )
                )
            }
        }
        return marks
    }

    // MARK: - Seed selection

    /// A window may seed a continuation chain iff it is a CONFIRMED detector
    /// verdict with sane geometry and is not the listener's own row.
    ///
    /// `userMarked` (and the other user-owned boundary states) are excluded
    /// deliberately. A manual mark is the highest-fidelity statement about the
    /// span the listener drew — it says nothing about the neighbouring audio, and
    /// the listener who marked one creative has already told us what they wanted
    /// marked. Deriving more marks off their boundary would put a guess in a
    /// place they curated.
    static func isSeed(_ window: AdWindow) -> Bool {
        seedDecisionStates.contains(window.decisionState)
            && !AdDetectionService.reconcileProtectedBoundaryStates
                .contains(window.boundaryState)
            && window.boundaryState != boundaryState
            && window.detectorVersion != detectorVersion
            && window.startTime.isFinite
            && window.endTime.isFinite
            && window.endTime > window.startTime
    }

    // MARK: - The chain walk

    /// Walk the ad-copy chain forward from the seed's trailing edge, returning
    /// the new trailing edge (or `nil` when the chain never starts).
    ///
    /// Each step requires a link that begins within `maxLinkGapSeconds` of the
    /// current edge and reaches past it. The corridor `[edge, link.end)` must
    /// cross no wall (content barrier or protected region). The returned edge is
    /// always some link's `end`: the walk NEVER lands anywhere that positive ad
    /// evidence did not put it.
    private static func walkForward(
        from seed: AdWindow,
        links: [AdCopyLink],
        walls: [(start: Double, end: Double)],
        config: Configuration
    ) -> Double? {
        var edge = seed.endTime
        var moved = false
        // Bounded by the link count: every accepted step consumes at least one
        // link (edge strictly increases past that link's end), so this cannot
        // spin. The explicit budget keeps that true even if `links` is huge.
        for _ in 0..<links.count {
            let reachable = links.filter {
                $0.end > edge && $0.start <= edge + config.maxLinkGapSeconds
            }
            guard let next = reachable.min(by: { lhs, rhs in
                if lhs.start != rhs.start { return lhs.start < rhs.start }
                return lhs.end < rhs.end
            }) else { break }
            guard next.end - seed.endTime <= config.maxExtensionSecondsPerSide else { break }
            guard !intersectsWall(start: edge, end: next.end, walls: walls) else { break }
            edge = next.end
            moved = true
        }
        return moved ? edge : nil
    }

    /// Mirror of ``walkForward(from:links:walls:config:)`` for the leading edge.
    private static func walkBackward(
        from seed: AdWindow,
        links: [AdCopyLink],
        walls: [(start: Double, end: Double)],
        config: Configuration
    ) -> Double? {
        var edge = seed.startTime
        var moved = false
        for _ in 0..<links.count {
            let reachable = links.filter {
                $0.start < edge && $0.end >= edge - config.maxLinkGapSeconds
            }
            guard let next = reachable.max(by: { lhs, rhs in
                if lhs.end != rhs.end { return lhs.end < rhs.end }
                return lhs.start < rhs.start
            }) else { break }
            guard seed.startTime - next.start <= config.maxExtensionSecondsPerSide else { break }
            guard !intersectsWall(start: next.start, end: edge, walls: walls) else { break }
            edge = next.start
            moved = true
        }
        return moved ? edge : nil
    }

    /// `true` when any wall overlaps the half-open corridor `[start, end)`.
    ///
    /// Half-open on purpose: a barrier that begins exactly where the corridor
    /// ends is the wall we stopped AT, not one we crossed, and a barrier that
    /// ends exactly where the corridor begins is already behind us.
    private static func intersectsWall(
        start: Double,
        end: Double,
        walls: [(start: Double, end: Double)]
    ) -> Bool {
        guard end > start else { return false }
        return walls.contains { $0.start < end && $0.end > start }
    }

    // MARK: - Interval helpers

    /// `span` minus the union of `covered`, as an ascending list of gaps. The
    /// residue is what nothing already claims — the "uncovered run" this bead
    /// measures, computed the same way the offline measurement does.
    static func subtract(
        covered: [(start: Double, end: Double)],
        from span: (start: Double, end: Double)
    ) -> [(start: Double, end: Double)] {
        guard span.end > span.start else { return [] }
        let clipped = covered.compactMap { iv -> (start: Double, end: Double)? in
            let start = max(span.start, iv.start)
            let end = min(span.end, iv.end)
            return end > start ? (start: start, end: end) : nil
        }
        var result: [(start: Double, end: Double)] = []
        var cursor = span.start
        for block in unionIntervals(clipped) {
            if block.start > cursor { result.append((start: cursor, end: block.start)) }
            cursor = max(cursor, block.end)
        }
        if cursor < span.end { result.append((start: cursor, end: span.end)) }
        return result
    }

    /// Sort and union `[start, end)` intervals into a disjoint, ascending set.
    /// Touching intervals merge — for a chain walk, "adjacent" and "overlapping"
    /// are the same thing.
    static func unionIntervals(
        _ intervals: [(start: Double, end: Double)]
    ) -> [(start: Double, end: Double)] {
        let sorted = intervals
            .filter { $0.start.isFinite && $0.end.isFinite && $0.end > $0.start }
            .sorted { lhs, rhs in
                if lhs.start != rhs.start { return lhs.start < rhs.start }
                return lhs.end < rhs.end
            }
        var result: [(start: Double, end: Double)] = []
        for interval in sorted {
            if let last = result.last, interval.start <= last.end {
                result[result.count - 1] = (
                    start: last.start,
                    end: max(last.end, interval.end)
                )
            } else {
                result.append(interval)
            }
        }
        return result
    }

    // MARK: - Emit

    /// Build the content-addressed mark-only `AdWindow` for a recovered span.
    ///
    /// Every authority field is a hard-coded literal, not a derivation: the gate
    /// is `.markOnly`, the state is `.candidate`, and both edge anchors are
    /// `.unanchored`. There is no code path in this file that can produce
    /// anything else, which is what makes "a continuation can never auto-skip"
    /// a property rather than a promise.
    static func makeMark(
        start: Double,
        end: Double,
        confidence: Double,
        analysisAssetId: String
    ) -> AdWindow {
        AdWindow(
            id: markId(analysisAssetId: analysisAssetId, start: start, end: end),
            analysisAssetId: analysisAssetId,
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: boundaryState,
            // NEVER confirmed/applied.
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: detectorVersion,
            // We know a pod continues; we do NOT know whose ad this is.
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
            // ALWAYS markOnly — hard-coded literal, never a policy switch.
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            catalogStoreMatchSimilarity: nil,
            // Belt+suspenders: under playhead-2350 an unanchored edge auto-skips
            // nothing, so even a future edge policy running over these rows
            // cannot promote recovered pod material to a silent skip.
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
    }

    /// Content-addressed id: `podcont-<16 hex>` over
    /// `asset=…|version=pod-continuation-v1|start=…|end=…`. An identical recompose
    /// mints the identical id, so the version-scoped reconcile retires nothing and
    /// the store's INSERT-OR-REPLACE is a true no-op (idempotency by construction).
    static func markId(analysisAssetId: String, start: Double, end: Double) -> String {
        let canonical =
            "asset=\(analysisAssetId)|version=\(detectorVersion)|start=\(start)|end=\(end)"
        let digest = SHA256.hash(data: Data(canonical.utf8))
        let hex = digest.map { String(format: "%02x", $0) }.joined()
        return "podcont-\(hex.prefix(16))"
    }
}
