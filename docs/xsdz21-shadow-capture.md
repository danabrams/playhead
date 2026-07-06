# playhead-xsdz.21 — SpliceSlot shadow: capture, eval, and activation (DEFERRED ops)

Bead C's **code** is landed and unit-proven (shadow codepath, frozen v3
breadcrumb, decision-delta computer, projection tooling, both-flag matrix). What
remains needs **real hardware + time or Dan's explicit approval** and is
recorded here verbatim. Do NOT fake numbers or flip the activation flag.

## What shipped (implementable scope — DONE)

- `AdDetectionConfig.spliceSlotShadowEnabled` (default OFF). Both flags OFF ⇒
  pipeline byte-identical; both ON ⇒ shadow silent (ownership pass owns the
  disposition). Ownership OFF + shadow ON ⇒ would-be dispositions computed via
  the SHARED `computeSpliceSlotPass` → pure `SpliceSlotDispositionEngine`
  (no reimplementation), emitted but NOT applied.
- Frozen v3 breadcrumb `SpliceSlotShadowBreadcrumb.format` (13-reason enum,
  pinned slot-field sources, sentinels, both `widthDeltaSec` branches).
- `SpliceSlotShadowObserver` (nil in production, injected for capture/tests).
- `SpliceSlotDecisionDeltaComputer` (ledger mass with AND without
  `.audioForensics` suppression, distinctKinds, skipConfidence — slot vs minted).
- `SpliceSlotProjection` (substitution: ONLY `qualifying` substitutes the slot
  interval, `absorbed` removes minted, all else keeps minted; asserts
  treatment-arm pairwise disjointness). Same-run dump proven hermetically in
  `SpliceSlotShadowProjectionDumpTests`.

## DEFERRED 1 — dogfood capture (needs Dan's device + ~2 weeks)

Build a dogfood build with the shadow flag ON and the REQUIRED config so the
suppression comparison is not vacuous:

- `spliceSlotShadowEnabled = true`
- `audioForensicsEnabled = true`  (default false — else `.audioForensics`
  suppression has nothing to suppress and acceptance looks green while the
  comparison is empty)
- log `crossEpisodeMemoryEnabled` (the negative-bank veto + absorbee mirrors are
  live iff ON)

Wire a live `SpliceSlotShadowObserver` into the `AdDetectionService` on the
Mac Catalyst dogfood run path (chapter-eval precedent) and export the rows via
the dogfood diagnostics export. Capture ~2 weeks on Dan's library. Grep
`spliceslot.shadow` in Console.app to sanity-check the breadcrumb stream.

**Decision-delta population is capture-time.** The frozen DEFINITION lives in
`SpliceSlotDecisionDeltaComputer` (unit-proven: ledger mass with/without
`.audioForensics` suppression, distinctKinds, skipConfidence). It is deliberately
NOT populated in the fast-test pipeline (audio forensics OFF ⇒ the suppression
delta is vacuous, per the REQUIRED CONFIG above). In the dogfood build, for each
span build the slot-arm ledger over the resolved slot interval (refiner-free) and
call `SpliceSlotDecisionDeltaComputer.make(mintedLedger:… slotLedger:…)`, attach
it to the span's `SpliceSlotShadowRow.decisionDelta`, and export. The minted arm
is the loop's existing ledger + `skipConfidence`.

## DEFERRED 2 — eval readout (needs the captured data)

Per `docs/bd-4xqf-measurement-recipe.md`, ADAPTED (ignore its
`spanFinalizerEnabled` toggles — p56a; both arms run the current pipeline):

1. Baseline dump: current pipeline minted spans (pre-slot).
2. Treatment dump: same run, shadow rows → `SpliceSlotProjection.project(from:)`
   (substitution rule + disjointness assertion is in the tooling). Same-run
   dump is PREFERRED; the fallback join script
   (`scripts/l2f-spliceslot-project.py`, tolerance-based interval join reporting
   unmatched-record count per xsdz.14 FM variance) is only a stub/spec.
3. `scripts/l2f-bd4xqf-compare.py --baseline <pre-slot dump> --treatment
   <projected dump> --rediff <tier-a rediff>`.

HEADLINE: mean pipeline coverage of true DAI width vs the ~18% baseline —
overall AND `reason=qualifying`-only. GUARDS: FP content-seconds enclosed per
episode; per-pair regression; precision-gate pass-rate deltas; xsdz.7 fragility
distribution shift; QUALIFICATION RATE reported BOTH raw (qualified-true/total)
AND consolidation-adjusted (denominator excludes `absorbed` + `slotCollision`),
with the FULL reason breakdown. CAVEAT to document:
`edgeBelowFloor`/`slotConfidenceBelowFloor` buckets can mask qualifying
non-champion pairs (bead A gates the champion only) — champion-level signals,
not proof no qualifying pair existed.

## DEFERRED 3 — activation go/no-go (needs Dan's approval)

Go/no-go per xsdz.14 noise-aware A/B discipline (multi-run CIs where FM variance
matters). **The flag flip requires Dan's explicit approval** (no-unilateral-swaps
applies to activation): present the readout first. `spliceSlotOwnershipEnabled`
stays OFF until then. Also revisit bead B's `.audioForensics`-suppression default
against the with/without-suppression data.

Flip (only after approval): set `spliceSlotOwnershipEnabled: true` in the
production `AdDetectionConfig.default` (and drop `spliceSlotShadowEnabled`).
