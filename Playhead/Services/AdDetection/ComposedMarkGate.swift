// ComposedMarkGate.swift
// playhead-mqqd: the ADDITIVE MARK COMPOSERS' half of playhead-2350's
// presence-vs-extent gate — the third producer of `ad_windows`, and the one
// neither half of 2350 ever covered.
//
// WHAT WAS MEASURED
// -----------------
// 2026-08-12, device pull `db-morning7`: 45 `ad_windows` across 4 assets, and
// `eligibilityGate` took exactly ONE value — `markOnly`, 45 of 45, `wasSkipped`
// 0 of 45. 28 of those 45 (62 %) came from two composers that stamp the value
// as a HARD-CODED LITERAL:
//
//   • `SemanticSweepMarkComposer` — 24 rows, `semantic-sweep-v1`
//   • `AdPodContinuation`         — 4 rows, `pod-continuation-v1`
//
// So for 62 % of everything the pipeline produced, the eligibility stamp was a
// POLICY DECISION FROZEN AS A CONSTANT: no anchor, no confidence, no trust
// state and no amount of evidence could move it, because nothing read anything.
//
// WHY THE LITERAL IS THE DEFECT — AND WHY "FLIP IT TO ELIGIBLE" IS NOT THE FIX
// ---------------------------------------------------------------------------
// Both composers were RIGHT to be mark-only, and still are; see the two
// per-composer derivations for the measured reason each one cannot prove its
// own edges. The defect is not the value. It is that the row carried the value
// and its own contradicting-or-agreeing evidence as TWO INDEPENDENT LITERALS:
//
//     eligibilityGate: SkipEligibilityGate.markOnly.rawValue      // literal 1
//     startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue     // literal 2
//     endEdgeAnchor:   AutoSkipEdgeAnchor.unanchored.rawValue     // literal 3
//
// Nothing enforced their agreement. That is the playhead-6qvf shape exactly —
// "two expressions that happen to agree" — and it is the shape both composers'
// own headers cite while committing it. A future change that gives either
// producer a real anchor (playhead-oa82's rediff is the live candidate) rewrites
// literal 2 and 3 and leaves literal 1 untouched, silently; and a future change
// that rewrites literal 1 promotes a span whose edges nobody proved, with no
// gate anywhere downstream to catch it. Which brings us to the second half:
//
// THESE ROWS ARE COVERED BY NEITHER HALF OF 2350.
// `DecisionResult.withExtentSupport` runs in `AdDetectionService.runBackfill`'s
// FUSION emission loop. `HotPathExtentGate` runs at the two HOT-PATH sites
// (playhead-bllt). A composed mark is neither: `compose(...)` returns finished
// `AdWindow` values that `AdDetectionService` persists directly. Today that is
// harmless because literal 1 already says `markOnly`. It is harmless BY
// COINCIDENCE OF THREE CONSTANTS, which is not the same thing as being safe.
//
// AND THAT IS MEASURED, NOT INFERRED. `ComposedMarkGateTests`' delivery probe
// drives the real `SkipOrchestrator` with the real `compose(…)` output of both
// producers, in `auto` mode at trust 0.95. Mutant M3 set the rule below to a
// bare `.eligible` and left BOTH anchors `.unanchored` — the exact shape 2350
// exists to demote — and both marks reached the MANAGED tier and were SKIPPED
// (`bannerTier == .suggest` failed AND `!wasSkipped` failed). An unanchored
// `semanticSweepMark` / `podContinuation` row classifies `.fusion`, and in a
// trusted show `.fusion` is `.auto`. So the eligibility stamp was the ONLY
// thing between these 28 rows and a silent cut. It is now a derivation over the
// row's own extent, which is what the extent gate was supposed to be.
//
// THE RULE
// --------
// A composed mark is `.eligible` iff its persisted extent is DETERMINISTIC on
// BOTH edges — `SpanExtentSupport.tier == .deterministic`, i.e. byte-exact
// rediff proved where this span starts AND where it stops. Everything else is
// `.markOnly`.
//
// That predicate is not invented here. It is the SAME one
// `SkipDetectorClass.classify` uses to name the `.rediffByteExact` class — the
// only class whose mode is a property of the INSTRUMENT rather than of a show's
// history (`showIndependentSeedMode`), and the class Dan ruled should always be
// auto. `ComposedMarkGateTests.agreesWithDetectorClassification` pins the two
// spellings together so they cannot drift into being two questions.
//
// WHY STRICTER THAN 2350's OWN BAR, deliberately. 2350 admits a span at
// `isFullyAnchored`, which includes `.corroborated` (a `StingerRefiner` snap).
// These two producers' PRESENCE evidence is materially weaker than a fused
// verdict — one coarse FM `containsAd` over a ~95 s tile, or a lexical chain
// walk whose bridge seconds are bounded extrapolation — so they do not get to
// spend a stinger snap on a cut. The relation is one-directional and pinned:
// `.eligible` here IMPLIES `isFullyAnchored` there, so this gate can never
// admit anything 2350 would demote. It only ever admits less.
//
// WHY THIS IS **NOT** KEYED ON `unanchoredExtentBlocksAutoSkip`
// ------------------------------------------------------------
// That flag is the shared kill switch for 2350 and bllt, and reusing it here is
// the obvious-looking move that would be a live defect. Both of those gates
// DEMOTE a label some other code already computed, so turning the flag off
// restores the pre-2350 behaviour those producers had. A composed mark has no
// pre-2350 behaviour to restore — it was born mark-only. Keying this on the
// flag would make flipping the kill switch PROMOTE 62 % of the device's rows to
// auto-skip, which is the opposite of what a kill switch is for. The gate here
// is total and unconditional: it is not a demotion applied to somebody else's
// verdict, it IS the verdict.
//
// WHAT THIS CHANGES TODAY: NOTHING, AND THAT IS THE HONEST OUTCOME.
// Both composers derive `.unanchored` on both edges, so the rule computes
// `.markOnly` for every row either one emits — byte-identical to the literal it
// replaces, which `composedMarksAreSelfConsistent` and the two composers' own
// emit tests assert. What changed is that it is now a DERIVATION over the row's
// own evidence rather than a constant nobody can revisit: give a composer an
// anchor and the gate follows it, in the same statement, from the same value.
//
// THAT LAST SENTENCE IS THE ONE WORTH CHECKING, so it was checked as an A/B on
// the same probe. Both arms move `SemanticSweepMarkComposer.extentSupport` to
// byte-exact on both edges — the change playhead-oa82's rediff makes plausible:
//
//   M5a, derivation intact   — the sweep mark reaches the MANAGED tier and the
//                              delivery probe's `bannerTier == .suggest` and
//                              `!wasSkipped` both fail. The listener outcome
//                              MOVED.
//   M5b, the literal put back — the very same extent change, and the delivery
//                              probe PASSES unchanged. The listener outcome did
//                              not move, and nothing on main would have said so.
//                              What does fail in M5b is
//                              `sweepMarkGateTracksItsDeclaredExtent` and
//                              `composedMarksAreSelfConsistent`: the row now
//                              contradicts itself, and after this bead that is
//                              a test failure rather than a silent under-claim.

import Foundation

/// The eligibility an ADDITIVE COMPOSED mark earns from the extent it is
/// persisted with.
///
/// Pure, total, and the single place the rule is spelled for this producer
/// family — the same posture `HotPathExtentGate` takes for the hot path, and a
/// separate type for the same reason it is: a different producer with a
/// different verdict type. The hot path's verdict is a bare `String` riding the
/// orchestrator's legacy nil-gate contract; a composed mark's is a real
/// `SkipEligibilityGate`, so this returns the enum and the composer writes its
/// raw value.
enum ComposedMarkGate {

    /// The gate a composed mark earns from `extent`.
    ///
    /// - Parameter extent: the per-edge provenance the mark will be PERSISTED
    ///   with. Pass the SAME value that becomes `AdWindow.startEdgeAnchor` /
    ///   `endEdgeAnchor` — one variable used twice, never two expressions that
    ///   happen to agree. That is the whole point of this function existing
    ///   (playhead-6qvf), so a call site that recomputes the anchors separately
    ///   has reintroduced the defect while appearing to fix it.
    /// - Returns: `.eligible` iff both edges are byte-exact; `.markOnly`
    ///   otherwise.
    static func eligibility(for extent: SpanExtentSupport) -> SkipEligibilityGate {
        extent.tier == .deterministic ? .eligible : .markOnly
    }
}
