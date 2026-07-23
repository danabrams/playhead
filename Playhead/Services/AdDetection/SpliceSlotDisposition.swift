// SpliceSlotDisposition.swift
// playhead-xsdz.20 (Bead B): the PURE, SYNCHRONOUS per-span disposition engine
// that decides which post-decode spans hand WIDTH ownership to an acoustic
// splice slot, and the pure REWRITE helper that materializes those decisions.
//
// WHY A PURE FUNCTION (pinned)
// ---------------------------
// The eval's validity rests on `shadow == flag-ON by construction`: bead C's
// shadow instrumentation and this bead's flag-ON pass 5 must consume the SAME
// per-span dispositions. So passes 2–4 (negative-bank veto → greedy collision →
// slot-vs-minted overlap FIXPOINT, with the per-slot empty-atom-set check folded
// in before pass 3) live here as one pure, deterministic, side-effect-free
// function over pass-1 resolver outputs plus a PRE-AWAITED negative-bank verdict
// table. The two-phase split (resolve + await-bank-once in the service, then this
// synchronous function) exists because `NegativeFingerprintBank` is an actor and
// a pure synchronous function cannot await it, while pass 4(b) only discovers its
// absorbee set mid-fixpoint.
//
// INTERVAL SEMANTICS (pinned, pipeline-wide — mirrors SpliceSlotResolver /
// xsdz.20 spec): OVERLAP is POSITIVE-DURATION intersection. Two intervals that
// share only an endpoint are DISJOINT. FULLY ENCLOSED is subset-or-equal (shared
// endpoints allowed). `TimeRange.intersects` already encodes the positive-duration
// rule; enclosure is spelled out below.

import Foundation
import OSLog

// MARK: - Inputs

/// One span's pass-1 (resolver) output plus its pre-awaited negative-bank
/// verdicts — the complete input the pure engine needs for that span.
struct SpliceSlotCandidate: Sendable, Equatable {
    /// The span's CURRENT post-decode interval (post-Use-A snap): the interval
    /// the rewrite overwrites, and (for bead C) the shadow's minted extent.
    let mintedInterval: TimeRange
    /// The would-be slot from pass 1 (`SpliceSlotResolver.resolve`), or `nil`
    /// when the resolver produced none for this span.
    let slot: SpliceSlot?
    /// Whether the WOULD-BE slot interval INTERSECTS (positive-duration) ≥ 1
    /// atom. Consulted only when `slot != nil`: the pre-pass-3 empty-atom-set
    /// disqualification demotes a slot that covers no atoms. Pass `true` when
    /// `slot == nil` (the field is then never read).
    let slotIntersectsAtoms: Bool
    /// Negative-bank verdict for this span's CORE tokens (`>=`
    /// `NegativeFingerprintBank.defaultMatchThreshold`). Always `false` when the
    /// cross-episode-memory flag is off or no bank is wired (dormant).
    let coreBankMatch: Bool
    /// Negative-bank verdict for this span's SLOT tokens. `false` when
    /// `slot == nil` or the bank is dormant.
    let slotBankMatch: Bool

    init(
        mintedInterval: TimeRange,
        slot: SpliceSlot?,
        slotIntersectsAtoms: Bool,
        coreBankMatch: Bool = false,
        slotBankMatch: Bool = false
    ) {
        self.mintedInterval = mintedInterval
        self.slot = slot
        self.slotIntersectsAtoms = slotIntersectsAtoms
        self.coreBankMatch = coreBankMatch
        self.slotBankMatch = slotBankMatch
    }
}

// MARK: - Outputs

/// Why a would-be slot did not survive to rewrite. This is the LAST pass that
/// touched the span — bead C's shadow relies on this precedence for reason
/// attribution.
enum SpliceSlotDemotionReason: Sendable, Equatable {
    /// Pass 2: the span's slot OR core tokens matched a negative-bank entry.
    case negativeBankVeto
    /// Pre-pass-3 disqualification: the slot interval intersects no atoms.
    case emptyAtomSet
    /// Pass 3: the slot's interval collided with an already-kept slot.
    case greedyCollision
    /// Pass 4(a): the slot partially overlapped a non-kept minted interval.
    case mintedOverlap
    /// Pass 4(b): a span the slot would enclose matched the negative bank, so
    /// the slot is demoted and absorbs nothing (all-or-nothing).
    case absorbeeBankMatch
}

/// Per-span disposition returned by the engine.
enum SpliceSlotDisposition: Sendable, Equatable {
    /// The span keeps its slot; pass 5 rewrites it to the slot interval.
    case keepSlot(SpliceSlot)
    /// The resolver produced no slot; the span stays exactly as decoded.
    case noSlot
    /// A slot resolved but was demoted; the span stays as its minted extent.
    case demoted(SpliceSlotDemotionReason)
    /// The span is dropped pre-fusion and folded into the kept slot at
    /// `absorberIndex` (an index into the input candidates array).
    case absorbed(absorberIndex: Int)
}

/// The engine's result: per-span dispositions plus the pass-4 iteration count.
struct SpliceSlotDispositionResult: Sendable, Equatable {
    let dispositions: [SpliceSlotDisposition]
    /// Number of pass-4 FIXPOINT rounds that produced ≥ 1 demotion. The
    /// re-introduction fixture asserts this converges in 2.
    let fixpointRounds: Int
}

// MARK: - Engine

/// The pure passes-2–4 engine. See file header for the two-phase contract.
enum SpliceSlotDispositionEngine {

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "SpliceSlotDisposition"
    )

    /// Compute per-span dispositions. PURE, deterministic, side-effect-free.
    ///
    /// Pass order is SPEC (ordering changes output): (1 done upstream) resolve;
    /// (2) negative-bank veto; (pre-3) empty-atom-set disqualification; (3)
    /// greedy collision among survivors; (4) slot-vs-minted overlap FIXPOINT
    /// with absorption computed ONLY at the fixpoint.
    static func computeDispositions(
        _ candidates: [SpliceSlotCandidate]
    ) -> SpliceSlotDispositionResult {
        let n = candidates.count

        // `kept[i]` — the span currently owns a KEPT slot. `reason[i]` — set the
        // moment a would-be slot is demoted (LAST-pass-wins precedence).
        var kept = [Bool](repeating: false, count: n)
        var reason = [SpliceSlotDemotionReason?](repeating: nil, count: n)
        let hasSlot = candidates.map { $0.slot != nil }

        // Pass 1 already ran: every resolved slot provisionally KEEPS; passes
        // 2–4 demote.
        for i in 0..<n where hasSlot[i] { kept[i] = true }

        // Pass 2 — NEGATIVE-BANK VETO (per-span). EITHER a slot-token or a
        // core-token match discards the slot: either-match prevents token
        // dilution (prefix truncation at maxTokenCount) from defeating today's
        // suppression. Dormant (all-false verdicts) when the flag is off.
        for i in 0..<n where kept[i] {
            if candidates[i].slotBankMatch || candidates[i].coreBankMatch {
                kept[i] = false
                reason[i] = .negativeBankVeto
            }
        }

        // PRE-PASS-3 — EMPTY-ATOM-SET DISQUALIFICATION (per-slot). Runs BEFORE
        // pass 3 because it needs only the slot interval + atom stream and must
        // NOT be discovered at rewrite time: a disqualified slot's minted
        // interval STAYS in every comparison set below, so the pairwise-
        // disjointness guarantee cannot be broken by a resurrected interval.
        for i in 0..<n where kept[i] {
            if !candidates[i].slotIntersectsAtoms {
                kept[i] = false
                reason[i] = .emptyAtomSet
            }
        }

        // Pass 3 — GREEDY COLLISION (pairwise 'colliding' is NOT transitive).
        // Rank veto/empty survivors by core-coverage fraction DESC, tiebreak
        // earlier minted (core) start, then index for total determinism. Walk
        // the ranking; KEEP a slot iff its interval is DISJOINT (positive-
        // duration) from every already-KEPT slot, else DEMOTE to minted.
        let survivors = (0..<n).filter { kept[$0] }
        let ranked = survivors.sorted { a, b in
            let ca = candidates[a].slot!.coreCoverage
            let cb = candidates[b].slot!.coreCoverage
            if ca != cb { return ca > cb }
            let sa = candidates[a].mintedInterval.start
            let sb = candidates[b].mintedInterval.start
            if sa != sb { return sa < sb }
            return a < b
        }
        var acceptedSlotRanges: [TimeRange] = []
        for i in ranked {
            let slotRange = Self.slotRange(candidates[i].slot!)
            if acceptedSlotRanges.allSatisfy({ !$0.intersects(slotRange) }) {
                acceptedSlotRanges.append(slotRange)
            } else {
                kept[i] = false
                reason[i] = .greedyCollision
            }
        }

        // Pass 4 — SLOT-vs-MINTED OVERLAP FIXPOINT. Each ROUND evaluates every
        // currently-kept slot against a SNAPSHOT of the round-start comparison
        // set = the minted intervals of all NON-kept spans (kept slots' own
        // minted intervals are EXCLUDED — they are being vacated). A re-
        // introduced interval (from a demotion this round) only takes effect
        // the NEXT round, which is what makes the re-introduction fixture
        // converge in exactly 2 rounds. The kept set shrinks monotonically, so
        // the loop terminates in ≤ N rounds.
        //
        //   (a) a kept slot with ≥ 1 PARTIAL overlap (positive intersection,
        //       neither encloses) with a comparison interval is DEMOTED and
        //       absorbs nothing.
        //   (b) else, if ANY non-kept span the slot fully ENCLOSES (other than
        //       its own span) is core-bank-matched, the slot is DEMOTED
        //       (all-or-nothing) and absorbs nothing.
        //
        // Absorption itself is computed ONLY at the fixpoint (below), never per
        // round — a per-round absorb could strand a span with no covering span
        // if its would-be absorber is later demoted.
        var fixpointRounds = 0
        while true {
            let keptAtRoundStart = kept
            let comparison: [TimeRange] = (0..<n)
                .filter { !keptAtRoundStart[$0] }
                .map { candidates[$0].mintedInterval }
            var demotedThisRound = false
            for i in 0..<n where keptAtRoundStart[i] {
                let slotRange = Self.slotRange(candidates[i].slot!)
                // (a) partial overlap with any non-kept minted interval.
                if comparison.contains(where: { Self.partialOverlap(slotRange, $0) }) {
                    kept[i] = false
                    reason[i] = .mintedOverlap
                    demotedThisRound = true
                    continue
                }
                // (b) any enclosed non-kept span (other than self) core-matches.
                var enclosedBankMatch = false
                for j in 0..<n where j != i && !keptAtRoundStart[j] {
                    if Self.encloses(slotRange, candidates[j].mintedInterval),
                       candidates[j].coreBankMatch {
                        enclosedBankMatch = true
                        break
                    }
                }
                if enclosedBankMatch {
                    kept[i] = false
                    reason[i] = .absorbeeBankMatch
                    demotedThisRound = true
                }
            }
            if demotedThisRound {
                fixpointRounds += 1
            } else {
                break
            }
        }

        // Base dispositions from the FINAL kept set.
        var dispositions = [SpliceSlotDisposition](repeating: .noSlot, count: n)
        for i in 0..<n {
            if !hasSlot[i] {
                dispositions[i] = .noSlot
            } else if kept[i] {
                dispositions[i] = .keepSlot(candidates[i].slot!)
            } else {
                // reason is always set when a resolved slot is not kept.
                dispositions[i] = .demoted(reason[i] ?? .greedyCollision)
            }
        }

        // ABSORPTION — computed ONLY at the fixpoint, from the FINAL kept set.
        // Each kept slot absorbs every OTHER non-kept span whose minted interval
        // it fully encloses (the slot's own span is never an absorbee). Kept
        // slots are pairwise disjoint, so no span is enclosed by two absorbers.
        for i in 0..<n where kept[i] {
            let slotRange = Self.slotRange(candidates[i].slot!)
            for j in 0..<n where j != i && !kept[j] {
                if Self.encloses(slotRange, candidates[j].mintedInterval) {
                    dispositions[j] = .absorbed(absorberIndex: i)
                }
            }
        }

        // COMPOSITION GUARANTEE (asserted by the pod / greedy / integration
        // tests): the final persisted spans — kept slots + non-absorbed minted
        // survivors — are PAIRWISE DISJOINT (positive-duration). Sketch: kept
        // slots are pairwise disjoint (pass 3 + pass 4a); minted survivors are
        // pairwise disjoint (the decoder's `resolveOverlaps`); a minted survivor
        // is neither partial-overlapping (pass 4a demotes any kept slot that is)
        // nor enclosed by (absorption removes those) a kept slot; and a minted
        // survivor cannot ENCLOSE a kept slot because a slot has positive overlap
        // with its OWN minted extent, which is disjoint from every other minted
        // interval — so enclosing the slot would force two minted intervals to
        // intersect, a contradiction.
        return SpliceSlotDispositionResult(
            dispositions: dispositions,
            fixpointRounds: fixpointRounds
        )
    }

    // MARK: Geometry helpers

    private static func slotRange(_ slot: SpliceSlot) -> TimeRange {
        TimeRange(start: slot.startTime, end: slot.endTime)
    }

    /// `outer` fully encloses `inner` (subset-or-equal, shared endpoints OK).
    private static func encloses(_ outer: TimeRange, _ inner: TimeRange) -> Bool {
        outer.start <= inner.start && outer.end >= inner.end
    }

    /// POSITIVE-DURATION overlap where NEITHER interval encloses the other.
    private static func partialOverlap(_ a: TimeRange, _ b: TimeRange) -> Bool {
        guard a.intersects(b) else { return false }
        let aEnclosesB = a.start <= b.start && a.end >= b.end
        let bEnclosesA = b.start <= a.start && b.end >= a.end
        return !aEnclosesB && !bEnclosesA
    }
}

// MARK: - Rewrite (pass 5 materialization — pure)

/// The result of materializing engine dispositions onto the decoded-span set.
struct SpliceSlotRewriteResult: Sendable {
    /// The final span set to persist and feed the fusion loop: kept slots
    /// rewritten to their slot interval (with `.spliceSlot` provenance), minted
    /// / no-slot spans unchanged, absorbed spans removed.
    let finalSpans: [DecodedSpan]
    /// Original ids of spans that were ABSORBED (dropped). Their persisted rows
    /// must be deleted (no successor id).
    let absorbedIds: [String]
    /// Original ids that no longer exist in `finalSpans` (superseded kept ids
    /// whose ordinals — and therefore `makeId` — changed, plus absorbed ids).
    /// Their stale rows must be deleted so no ghost row survives.
    let supersededIds: [String]
}

/// PURE pass-5 rewrite. Applies engine dispositions to the decoded spans:
/// rewrites each kept slot to its slot interval, recomputes INTERSECTS ordinals
/// + `makeId`, appends the width-owner provenance marker, drops absorbed spans,
/// and reports the ids whose rows must be cleaned up. No I/O, no store, no
/// resolver.
///
/// `atomEvidence` supplies the atom stream for the INTERSECTS ordinal recompute
/// (positive-duration interval intersection with the slot). `dispositions` must
/// be index-aligned with `decodedSpans`.
///
/// `provenance` is the width-owner marker appended to each kept slot's span.
/// Defaults to `.spliceSlot` so the acoustic ownership path (playhead-xsdz.20)
/// is byte-for-byte unchanged; the rediff ownership path (playhead-xsdz.29)
/// passes `.rediffSlot` so a persisted span records WHICH oracle set its width.
/// Both are BARE, gate-inert provenance markers — only the wire token differs.
enum SpliceSlotRewriter {
    static func apply(
        decodedSpans: [DecodedSpan],
        dispositions: [SpliceSlotDisposition],
        atomEvidence: [AtomEvidence],
        provenance: AnchorRef = .spliceSlot
    ) -> SpliceSlotRewriteResult {
        precondition(
            dispositions.count == decodedSpans.count,
            "dispositions must be index-aligned with decodedSpans"
        )
        var finalSpans: [DecodedSpan] = []
        var absorbedIds: [String] = []

        for (i, span) in decodedSpans.enumerated() {
            // playhead-527u: a SACRED user-added ad is carried through verbatim —
            // no automated width disposition may drop or reshape it. TWO forms
            // qualify, and guarding BOTH here — at the width-pass-agnostic
            // rewriter — protects the user's mark for the rediff AND the (dormant,
            // rediff-exclusive) acoustic splice ownership passes alike:
            //   • a `.userCorrection`-provenance span (the synthetic
            //     `recordFalseNegative` ±15s span), and
            //   • a force-anchored span that COVERS a `.userConfirmed` atom (the
            //     `recordUserMarkedAd` `.exactTimeSpan` path — its span carries NO
            //     `.userCorrection` provenance, only the atom mask). The
            //     `computeRediffSlotPass` §5 gate already strips such a span's slot
            //     to `.noSlot`, so this is belt-and-suspenders on the rediff path
            //     and the SOLE protection should the acoustic pass ever be enabled.
            if span.anchorProvenance.contains(where: \.isUserCorrection)
                || Self.coversUserConfirmedAtom(span, atomEvidence: atomEvidence) {
                finalSpans.append(span)
                continue
            }
            switch dispositions[i] {
            case .noSlot, .demoted:
                // Unchanged minted span — carried through verbatim.
                finalSpans.append(span)

            case .absorbed:
                // Dropped pre-fusion; its atoms feed the absorber's wider slot.
                absorbedIds.append(span.id)

            case .keepSlot(let slot):
                let slotRange = TimeRange(start: slot.startTime, end: slot.endTime)
                let intersectingOrdinals = atomEvidence
                    .filter {
                        TimeRange(start: $0.startTime, end: $0.endTime)
                            .intersects(slotRange)
                    }
                    .map(\.atomOrdinal)
                // The pre-pass-3 empty-atom-set disqualification guarantees this
                // is non-empty for any kept slot. Keep a defensive fallback so a
                // future invariant break degrades to keeping the minted span
                // rather than minting a zero-atom row.
                guard let first = intersectingOrdinals.min(),
                      let last = intersectingOrdinals.max() else {
                    finalSpans.append(span)
                    continue
                }
                var newProvenance = span.anchorProvenance
                if !newProvenance.contains(provenance) {
                    newProvenance.append(provenance)
                }
                let newId = DecodedSpan.makeId(
                    assetId: span.assetId,
                    firstAtomOrdinal: first,
                    lastAtomOrdinal: last
                )
                finalSpans.append(DecodedSpan(
                    id: newId,
                    assetId: span.assetId,
                    firstAtomOrdinal: first,
                    lastAtomOrdinal: last,
                    startTime: slot.startTime,
                    endTime: slot.endTime,
                    anchorProvenance: newProvenance
                ))
            }
        }

        let finalIds = Set(finalSpans.map(\.id))
        let supersededIds = decodedSpans
            // playhead-527u: never mark a SACRED user-added span's row for
            // deletion. Both forms are always carried verbatim above (same id in
            // `finalIds`), so this filter is defensive against any future
            // disposition path that would change/drop their id.
            .filter {
                !$0.anchorProvenance.contains(where: \.isUserCorrection)
                    && !Self.coversUserConfirmedAtom($0, atomEvidence: atomEvidence)
            }
            .map(\.id)
            .filter { !finalIds.contains($0) }

        return SpliceSlotRewriteResult(
            finalSpans: finalSpans,
            absorbedIds: absorbedIds,
            supersededIds: supersededIds
        )
    }

    /// playhead-527u: true when `span` overlaps ANY atom the user explicitly
    /// CONFIRMED as an ad (`correctionMask == .userConfirmed`). Such a span is the
    /// force-anchored re-emission of a user-added mark (`recordUserMarkedAd`) and
    /// carries no `.userCorrection` provenance, so this atom-mask check is how the
    /// rewriter recognizes and protects it. Positive-duration interval overlap
    /// (touching endpoints do not count), consistent with the slot-intersect math.
    private static func coversUserConfirmedAtom(
        _ span: DecodedSpan,
        atomEvidence: [AtomEvidence]
    ) -> Bool {
        let spanRange = TimeRange(start: span.startTime, end: span.endTime)
        return atomEvidence.contains {
            $0.correctionMask == .userConfirmed
                && TimeRange(start: $0.startTime, end: $0.endTime).intersects(spanRange)
        }
    }
}
