# xsdz.15 — Boundary-Authority Inversion: splice channel owns WIDTH, transcript/FM own PRESENCE

**Status:** APPROVED by Dan 2026-07-05 — as designed, with all §6 recommendations:
backfill-only v1; refiners SKIPPED on slot-owned spans; provenance via new
`AnchorRef.spliceSlot`; shadow-first rollout.
**Date:** 2026-07-05. **bd:** playhead-xsdz.15 (decision). **Attacks:** playhead-4xqf (~18% DAI-width coverage).

## 1. Problem, precisely located

The 4xqf audit showed 12/12 rediff-corroborated DAI slots are *detected* but only ~18% of
their true width is covered. A code trace (2026-07-05) found the exact mechanism:

- A span's width is minted at **`MinimalContiguousSpanDecoder.formRuns`**
  (`MinimalContiguousSpanDecoder.swift:147-192`): start/end = the time extent of a
  contiguous run of **anchored atoms**. Which atoms are anchored is fixed upstream by
  `AtomEvidenceProjector` — FM-region ordinal ranges plus **single-atom** lexical/catalog
  hits. If the FM anchors ~10s around a sponsor mention inside a 60s DAI slot, the span
  is born ~10s wide.
- Everything downstream only nudges edges or adjusts scores, never re-derives the interval:
  Use-A acoustic snap **±8s** (`applyBoundarySnap`, snapRadiusSeconds=8), `BracketAware`/
  `BoundaryRefiner` **±3s** (maxBoundaryAdjust), `.breakAlignment` **±2s tolerance,
  evidence-only**. `BoundaryExpander` (60–120s search) exists but runs **only on the user
  "Hearing an ad" correction path**, not detection.
- The xsdz.8 composite forensics channel (`AudioForensicsBoundaryDetector`) measures exactly
  the splice discontinuity we need (loudness step + spectral flux + noise floor + environment
  change, σ-normalized) — but it is wired as one capped (0.20) fusion ledger entry scored
  **on the already-refined edges**. It corroborates whatever width was minted; it cannot fix it.

**Conclusion:** width is a presence-evidence artifact. No component is authorized to say
"the slot runs splice-to-splice." DAI ads are physically spliced in server-side, so the
splices ARE the true boundaries. This design grants that authority.

## 2. Design overview

**Invert the roles for spans with splice-bounded physical evidence:**

- **Presence authority (unchanged):** FM consensus, lexical/catalog evidence, classifier
  seeds — everything that anchors atoms today — decides *whether* there is an ad near t.
- **Width authority (new):** a **`SpliceSlotResolver`** searches outward from the presence
  core for a bounding pair of acoustic splice points and, when a qualifying pair exists,
  sets the span interval **splice-to-splice**.
- **Fallback (unchanged):** no qualifying splice pair (baked-in host reads, soft
  transitions) → today's behavior exactly: edge-local snap ±8s, refiners ±3s.

The inversion is therefore **additive**: it widens only where physical splice evidence
supports it, and degrades to current behavior everywhere else.

## 3. Components

### 3.1 `SpliceSlot` (new type)

```
SpliceSlot {
  startTime, endTime            // splice-to-splice interval
  startEdge, endEdge            // per-edge EdgeEvidence {time, stepScore, signals}
  slotConfidence                // min(startEdge.score, endEdge.score), calibrated
  coreCoverage                  // fraction of the presence core inside the slot
}
```

There is no existing splice-slot concept (`AdOwnership` is producer ownership, not
insertion type); this is the first-class abstraction the audit found missing.

### 3.2 `SpliceSlotResolver` (new stage)

Input: the minted presence span (`DecodedSpan`) + episode `[FeatureWindow]` +
`[AcousticBreak]`. Algorithm:

1. **Candidate edges:** collect `AcousticBreak`s per side in
   `[coreEdge − inwardTolerance, coreEdge + searchRadius]` — defaults **120s outward**
   (matches `BoundaryExpander`'s proven radii) and **8s inward** (mirrors
   `snapRadiusSeconds`; minted extents can overshoot the true splice). Hard cap:
   resulting slot duration ≤ 180s, the existing quorum/split limit.
2. **Edge scoring:** score each candidate with the **existing**
   `AudioForensicsBoundaryDetector` edge-step math (repurposed from "score the span's
   current edges" to "score candidate times") — loudness/flux/noise-floor/environment steps,
   σ-normalized against the episode.
3. **Pair selection (champion-scan, pinned in playhead-xsdz.19):** among geometry-valid
   pairs (`slotStart < slotEnd`, slot ∩ core ≠ ∅, duration ∈ [5, 180]s), sort by non-core
   seconds ascending (ties: higher `pairScore = min(edge scores)`, then earlier start,
   then earlier end); champion = first; scanning in order, a candidate replaces the
   champion iff `pairScore(candidate) ≥ 1.5 × pairScore(champion)`. (The naive pairwise
   "wider beats tighter at ≥1.5×" relation is non-transitive — it cycles with 3+ pairs —
   and is NOT the spec.) The 1.5× factor reuses `AsymmetricSnapScorer`'s editorial-clip
   weighting: enclosing content is worse than leaking ad.
4. **Qualification gate:** both edges ≥ `spliceEdgeFloor` AND `slotConfidence ≥ slotFloor`
   AND `coreCoverage = |slot ∩ core| / |core| ≥ 0.8` AND **no user-vetoed atom
   intervals** newly enclosed beyond what the core already contained. (The negative-bank
   check is layered separately in the slot pass — bank entries are token fingerprints
   without time ranges, so it is a text-match veto: the slot is rejected if EITHER the
   widened text OR the core text matches the bank; rejecting on core-match too prevents
   token dilution from defeating today's post-fusion suppression. See playhead-xsdz.20.)
   Any failure → return nil → fallback path.

### 3.3 Integration point: a third `boundaryOwnership` mode

> **Implementation note (bead polish, 2026-07-06):** slot ownership is realized as a
> **post-decode, per-span rewrite** rather than a decode-wide decoder mode — the resolver
> needs minted presence spans as input, and qualification is per-span (one episode holds
> both slot-owned and fallback spans, which a per-decode-call mode cannot express).
> `decode()` stays `.legacyEvidence`; presence of `AnchorRef.spliceSlot` in
> `anchorProvenance` is the slot-owned marker. Semantics are unchanged: slot interval
> owns width, Use-A snap output is irrelevant (fully overwritten), refiners skipped.
> See playhead-xsdz.20 for the pinned mechanism.

~~`MinimalContiguousSpanDecoder` already models boundary ownership (`.legacyEvidence` vs
`.hypothesisOwned`). Add **`.slotOwned`**~~ — **superseded by the implementation note
above**: no decoder mode is added; the slot pass rewrites qualifying spans post-decode.
`AnchorRef` gains a **`.spliceSlot`** case so provenance records that width came from
the acoustic channel.

Ordering in `runBackfill`: mint presence spans (unchanged) → **slot pass**
(new, post-decode/pre-refine) → boundary refiners run **only on non-slot-owned spans** (slot edges are
already physical; ±3s "refinement" of a splice point would only blur it) → fusion → gates.
Fusion evidence lookups then use the slot interval, so lexical/acoustic/catalog entries
inside the full slot (not just the old sliver) contribute — this should *raise* ledger mass
and distinct-kind counts on true DAI spans.

### 3.4 What explicitly does NOT change

- Presence detection: FM, lexical, chapter, catalog — untouched.
- Hot path / Tier-1 (`SegmentAggregator` + `AutoSkipPrecisionGate`): untouched in v1.
  Backfill only.
- User correction path (`BoundaryExpander`): untouched.
- Precision gates: `AutoSkipPrecisionGate` reads scores + duration (agnostic to boundary
  source; slot duration stays within `typicalAdDuration`). The xsdz.7 fragility penalty
  operates on the ledger post-fusion; `.spliceSlot` adds an evidence family, which if
  anything *reduces* measured fragility. Neither gate needs modification; both get
  re-evaled (§5).

## 4. False-positive risk and containment

Widening spans is precision-risky in exactly one way: **skipping content**. Contained by:

1. **Physical qualification** — both edges must show real σ-normalized discontinuities;
   soft/ambiguous transitions never qualify (fallback preserves today's width).
2. **Core-coverage + veto checks** (§3.2.4) — the slot must be *about* the presence core,
   and widening must not swallow negative-evidence atoms.
3. **Asymmetric edge preference** — content-cut penalized 1.5× over ad-leak.
4. **Duration plausibility** — [5, 180]s enforced at qualification, again at quorum.
5. **Shadow-first rollout** — TWO flags (split during bead polish): ownership
   `spliceSlotOwnershipEnabled` (default OFF) and shadow `spliceSlotShadowEnabled`
   (default OFF; ON in dogfood capture builds). Shadow logs `slot-vs-minted` width
   deltas + would-be decisions (same pattern as `AsymmetricSnapScorer` shadow +
   `FragilityDiagnosticObserver`) while ownership stays OFF; both-OFF is byte-identical;
   both-ON emits no shadow. Activation only after the eval bar (§5), per the xsdz.14
   noise-aware A/B discipline. See playhead-xsdz.21.

## 5. Eval plan

- **Headline:** mean pipeline coverage of true DAI width on the rediff-corroborated corpus
  (`scripts/l2f-bd4xqf-compare.py`), baseline ~18%. Success = a large step toward 100% on
  slots where a qualifying splice pair exists; report qualification rate separately.
- **Guard metrics:** content-seconds erroneously included (FP width) per episode;
  per-pair regression check (no slot may get *worse*); precision-gate pass-rate deltas;
  fragility-score distribution shift.
- **Corpus dependency:** resolvability of the guard metrics depends on playhead-xdh7
  (corpus growth) — flagged, not blocking the shadow phase.

## 6. Open questions for review

1. **Scope:** backfill-only v1 (recommended) — or also Tier-1/hot path?
2. **Refiners on slot-owned spans:** skip entirely (recommended, §3.3) or allow ±3s
   post-refinement of splice edges?
3. **`.spliceSlot` as new `AnchorRef` case** (recommended) vs a parallel field on
   `DecodedSpan` — the enum touches persistence/provenance consumers; field is less
   invasive but weaker provenance.
4. **Shadow duration / activation bar:** propose 2 dogfood weeks + the §5 eval before
   flipping the flag.

## 7. Rough effort

`SpliceSlot` + resolver (reusing AudioForensicsBoundaryDetector scoring + BoundaryExpander
search shape): ~2-3 beads of implementation + 1 shadow/eval bead. No new frameworks, no
API changes, mandate-unaffected (all local DSP already computed in backfill).
