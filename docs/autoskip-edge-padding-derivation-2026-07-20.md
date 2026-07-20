# Asymmetric Auto-Skip Edge Padding — Derivation (playhead-98co)

**Bead:** playhead-98co (P1, Gate-2 blocker) · **Date:** 2026-07-20
**Consumer:** `AutoSkipEdgePadding` (Playhead/Services/SkipOrchestrator/AutoSkipEdgePadding.swift)
**Reproduce:** `python3 scripts/l2f-derive-autoskip-padding.py` (stdlib-only; re-checks every number below against the frozen artifacts)

## 1. Problem

Auto-skip (held behind Gate 2) will skip rediff/stinger-marked DAI spans. Error
direction matters asymmetrically:

| Edge | Error direction | User experience | Class |
|---|---|---|---|
| start | skip starts LATE (pred start inside ad) | plays some ad | annoying, recoverable |
| start | skip starts EARLY (pred start before ad) | **clips real content** | trust-destroying |
| end | skip ends EARLY (pred end inside ad) | replays ad tail | annoying, recoverable |
| end | skip ends LATE (pred end past ad) | **clips real content** | trust-destroying |

The policy therefore pads asymmetrically: skip start = detected start + margin,
skip end = detected end − margin. Margins below are **derived from data**, per
edge and per anchor tier, sized so the content-clipping direction has zero
observed residual events in the reference data. Padding only ever SHRINKS the
skipped region.

## 2. Data

1. **Gold labels (primary):** the 2026-07-15 oracle ear-audit, 44 full breaks
   across 14 shows, attested by Dan to ±0.3 s (±0.5 s nikki-glaser stinger seam).
   Artifact: `TestFixtures/Corpus/Evaluations/earaudit-oracle-gold-b77c2804….json`
   (sources: `oracle-earaudit-ledger-2026-07-15-a31899a7….jsonl` +
   `oracle-earaudit-review-2026-07-15-9e826c08….json`, both frozen in
   `TestFixtures/Corpus/Audits/`).
2. **Gold labels (robustness / adjudication):** gold v6, 70 breaks
   (`earaudit-oracle-gold-836b8188….json`, canonical per the stinger hub) —
   the 07-15 set plus the 2026-07-16 attestations (danshows first-gold, morbid
   gate adjudications, **otm/ted requalify**).
3. **Predictions:** the 2026-07-16 stinger-era pipeline dump
   `playhead-baselines/playhead-dogfood-diagnostics-pipeline-dump-53ep-xsdz39bank-20260716.json`
   (the shipped xsdz.38-v4 joint scorer + xsdz.39 six-show stinger bank build).
   Scored with `scripts/l2f-score-oracle-gold.py` semantics (best-overlap pairing
   per break, fingerprint-checked lineage) this dump **exactly reproduces the
   bead-charter numbers**: tol-adjusted start p50 24.4 s / end p50 2.8 s,
   44/44 matched. Signed errors below are raw `predicted − gold`.
4. **Predictions (cross-build check, §6a):** the 2026-07-17 danshows build
   dump (`…pipeline-dump-53ep-danshows-20260717.json`), same scoring.
5. **Byte-rediff tier:** the xsdz.44 byte-alignment kill test
   (`analysis/byte-forensics-spike-2026-07-17.md`), 11 gold breaks over 7 A/B
   pairs (SmartLess ×3, Morbid ×4).

Tiering uses the dump's per-window `stingerRefinement` trace
(`startSnapped` / `endSnapped`) — the exact signal production has at runtime —
NOT show identity. Convention: signed start error < 0 ⇒ clip risk; signed end
error > 0 ⇒ clip risk.

## 3. Signed error distributions (predicted − gold, seconds)

### 44-break gold (primary)

| Edge / tier | n | min | p25 | p50 | p75 | max |
|---|---|---|---|---|---|---|
| START stinger-snapped | 13 | **−28.62** | −0.06 | −0.00 | +0.06 | +0.26 |
| START unsnapped | 31 | **−24.35** | +18.64 | +29.22 | +52.16 | +172.35 |
| END stinger-snapped | 22 | −30.14 | −0.36 | −0.05 | +0.11 | **+30.92** |
| END unsnapped | 22 | −134.55 | −59.84 | −33.74 | −1.84 | **+5.30** |

### Gold v6 robustness (68 matched)

| Edge / tier | n | min | p50 | max |
|---|---|---|---|---|
| START stinger-snapped | 12 | −28.62 | −0.00 | +0.26 |
| START unsnapped | 56 | −24.35 | +49.56 | +185.36 |
| END stinger-snapped | 24 | −30.14 | −0.09 | **+0.44** |
| END unsnapped | 44 | −163.44 | −26.84 | **+7.55** |

### Clip-direction events, itemized

- **START snapped, early:** exactly two, both nikki-glaser (−26.94, −28.62;
  non-grid pre-anchor snaps; the known xsdz.32/stinger-v3 false-widening
  cases — ~28 s of content enclosed ahead of the true break). Every other
  snapped start in both golds is within ±0.26 s. Morbid snapped starts
  (n=6): worst −0.18 s.
- **END snapped, late:** smartless +0.44 (both golds) and ted-business
  +30.92 (**44-gold only — adjudicated as a gold error, §4**).
- **END unsnapped, late:** +5.30 (SYSK, 44-gold), +7.55 (conan, v6; the
  conan window's end was NOT stinger-snapped, so it correctly charges the
  unanchored tier).
- **START unsnapped, early:** −24.35 (hard-fork), −12.70 (casefile), −1.96,
  −1.74; the rest of the tier errs late (median +29.2 s).
- **Byte tier (spike, n=11):** start deltas median +0.29 s, max +1.36 s,
  consistently LATE (the byte splice sits slightly after the perceptual break
  start — the safe direction); end deltas median +0.02 s, max +0.22 s.

## 4. Adjudications

**ted-business +30.92 end (44-gold) is a gold under-label, not a prediction
miss.** The 07-15 artifact bounds the pre-roll at 0.0–70.1; Dan's 2026-07-16
requalify pass (`otm-ted-requalify-review`, id `…-career-de-78`, status
`rebounded`) re-attested it as 0.0–101.3, and gold v6 carries 101.3. The
prediction ended at 101.02 with `endSnapped=true` — within tolerance of the
corrected label. The stinger-snapped END tier margin is therefore set from the
worst REPLICATED event (+0.44, smartless), and the +30.92 row is excluded with
this paper trail.

**conan +7.55 end (v6)** occurred on an unsnapped end and charges the
unanchored end tier, where it is the tier-defining tail event.

## 5. Margin derivation

Formula per (edge, tier): **worst observed clip-direction error in tier
(post-adjudication) + that break's gold attestation tolerance, rounded UP to a
0.25 s grid.** Where a tier has zero observed clip-direction events, the margin
floor is the attestation tolerance rounded up (labels themselves are only good
to ±0.3/±0.5 s). The harness enforces this per row: it asserts
`clip-direction error + that break's tolerance ≤ margin` for every matched
break (not just raw error ≤ margin), so a violation of the formula — not
merely of the raw cover — fails the run.

| Edge | Anchor tier | Basis | Margin |
|---|---|---|---|
| start | rediffByteExact | 0 early events (n=11, all late-biased) + 0.3 tol | **0.50 s** |
| start | stingerSnapped | worst −0.18 (morbid, tol 0.3) / −0.02 (nikki good snaps, tol 0.5) → 0.52 | **0.75 s** |
| start | unanchored | required ≈ 24.7 s — see verdict below | **UNSKIPPABLE → markOnly** |
| end | rediffByteExact | +0.22 + 0.3 = 0.52 | **0.75 s** |
| end | stingerSnapped | +0.44 (smartless, tol 0.3) = 0.74 | **0.75 s** |
| end | unanchored | +9.92 (smartless 05-21, 07-17 build — §6a) + 0.3 | **10.25 s** |

Degenerate rule: after padding, the remaining skip window must retain at least
1.0 s or the span is not auto-skipped (stays markOnly). Padding can only
shrink; a skip is never widened beyond the marked span.

### Per-show demotion (start edge): the-nikki-glaser-podcast

Nikki's pre-anchor misfired on 2 of 7 snapped starts (−26.9 s, −28.6 s in BOTH
golds) — a bimodal ~30 s early mode (template locking onto structure ahead of
the true break; pre-anchor confidence 0.67, the weakest in the bank). No cheap
padding exists: covering the mode needs 29.1 s, which (a) eats 32% of nikki's
median 91.5 s pod and (b) plays ~29 s of ad on the five correctly-snapped
breaks. **Nikki stinger-snapped starts stay markOnly** until the pre-anchor is
fixed (re-learn template / raise the snap gate / grid guard — successor work).
A rediff byte-exact start on nikki is NOT demoted — the demotion is scoped to
the stinger path that produced the misfires.

### Unanchored-start verdict: markOnly (the tier is unskippable-cheaply)

Covering the observed early tail (−24.35) needs a 24.75 s margin. The tier's
median start is ALREADY +29.2 s late, so a covered skip would begin ≈ 54 s into
a median 89.9 s break — the padding+lateness consumes over half the pod before
the skip starts, and the residual tail (−24.35 is a single observation; n=31)
still cannot be certified to 2%. This is a finding, not a failure: **spans
without a hard start anchor stay markOnly** (banner / suggest tier), exactly
the certainty-tier posture. Boundary-collapse recovery, not padding, is the fix
for this tier.

## 6. Achieved clip risk — honest statement

At the chosen margins there are **zero in-sample content-clip events in every
skippable tier across both golds**. The ~2%-per-edge target, however, is NOT
statistically verifiable at these sample sizes. One-sided 95% binomial upper
bounds given 0 observed events:

| Edge / tier | n (44-gold / v6) | 95% UB on clip prob |
|---|---|---|
| start rediffByteExact | 11 (spike) | 23.8% |
| start stingerSnapped (post-demotion ⇒ morbid) | 6 | 39.3% |
| end rediffByteExact | 11 (spike) | 23.8% |
| end stingerSnapped | 22 / 24 | 12.7% / 11.7% |
| end unanchored | 22 / 44 | 12.7% / 6.6% |

The morbid start distribution is extremely tight (6/6 within ±0.18 s, a
structural envelope-template lock), so the n=6 bound overstates practical risk
— but the bound is the bound. **Recommendation:** margins ship dormant
(flag default-OFF; Gate 2 held anyway). Before flag-ON, grow n through the
weekly corpus loop + gold extension (playhead-xdh7) until the upper bounds
clear the target, or revise margins from the larger sample.

### 6a. Cross-build check (the margin-fragility finding)

Re-running the harness against the NEWER 2026-07-17 danshows build
(`playhead-dogfood-diagnostics-pipeline-dump-53ep-danshows-20260717.json`)
holds every anchored-tier margin, but the UNANCHORED END tier produced a new
tail event: smartless 05-21 at **+9.92 s** (> the reference build's +7.55).
The unanchored tiers' tails are build-sensitive — exactly why they are either
markOnly (start) or carry a large margin (end). The end margin was therefore
sized across BOTH measured builds: 9.92 + 0.3 → **10.25 s**. Both builds now
pass `scripts/l2f-derive-autoskip-padding.py` (default dump and
`--dump …danshows-20260717.json`). Anchored-tier margins were identical
across builds — the hard-anchor tiers are the stable ones, which is the whole
thesis of certainty-tiered skipping.

## 7. Cost of the padding (the annoying-but-recoverable direction)

Median break width 90.1 s (44-gold). Combined margins consume:

- byte/byte (1.25 s): median 98.6% of the span still skipped (min 95.6%)
- stinger/stinger (1.50 s): median 98.3% (min 94.8%)
- stinger-start / unanchored-end (11.0 s): median 87.8% (min 61.7%)

### 7a. Interaction with the existing trailing cushion (flag-ON total)

The orchestrator's pre-existing pod-level trailing cushion
(`SkipPolicyConfig.adTrailingCushionSeconds` = 1.0 s, playhead-vn7n.2)
applies **after** these margins, to each merged cue's end in
`pushMergedCues`. The effective flag-ON end pull-in is therefore
**endMargin + 1.0 s** (byte 1.75 s, stinger 1.75 s, unanchored 11.25 s
total); the wiring tests pin the stacked values. This is deliberate,
same-direction stacking, not double-counting: the margin (derived from
predicted-end vs gold-end residuals) guarantees the cue end never passes
the true ad end; the cushion cedes one extra second of ad tail — the
recoverable direction. Two consequences worth naming:

- The §7 cost table above describes the margins alone; add 1.0 s per pod
  for the shipped flag-ON totals.
- The 1.0 s degenerate-remainder floor is checked **before** the cushion,
  so a span left with exactly the minimum remainder collapses to a
  zero-length cue after cushioning (a harmless no-op skip — the
  pre-existing clamp in `pushMergedCues`). Effective sub-second skips are
  possible for remainders in (1.0, 2.0) s; raising the floor to account
  for the cushion is a policy choice deferred to flag-ON review.

## 8. Caveats

1. **n is small everywhere.** 44 primary breaks; per-tier n as low as 6. §6 is
   the honest quantification.
2. **Build sensitivity.** Anchored-tier numbers replicated across the
   2026-07-16 xsdz39bank and 2026-07-17 danshows builds; the unanchored end
   tail did NOT (§6a) and the margin was widened to cover both. If the
   boundary stack changes again (stinger bank re-learn, joint recipe retune,
   riiz/xtpf/t1py boundary movement), RE-DERIVE before flag-ON
   (`scripts/l2f-derive-autoskip-padding.py` is the harness; run it against
   the current build's dump).
3. **Byte tier is an offline proxy.** The xsdz.44 spike used the offline Python
   aligner on 2 shows; production is the clean-room Swift `RediffByteAligner`.
   Re-measure the byte tier from production output before Gate 2 flips.
4. **Gold label noise is first-order at these magnitudes.** Two of the six
   margins are tolerance-dominated; the ted adjudication (§4) shows single gold
   labels can be off by 30 s. The margin formula adds the attestation tolerance
   for exactly this reason.
5. **Vetoed spans currently surface the auto-skip banner.** A flag-ON
   eligibility veto keeps the window `.confirmed`, which emits the
   `.autoSkipped`-tier banner ("Skipped …" copy, post-skip affordances)
   for a span that will not skip. Dormant while the flag is OFF; honest
   surfacing for vetoed spans (suggest-style "Skip?" or a manual-skip
   affordance) is part of the Gate-2 blocker set ("wraj surfacing + veto
   masks") and must land before flag-ON.
6. **Anchor provenance is not yet persisted on AdWindow rows.** The stinger
   trace and rediff slot provenance live inside `AdDetectionService` today and
   never reach `SkipOrchestrator`. Until a stamping bead lands, the wiring
   classifies every pipeline edge `.unanchored` — flag-ON therefore auto-skips
   NOTHING (each auto-promotion is demoted to markOnly), which is the correct
   conservative posture, and user-initiated skips (manual tap, accepted
   suggestion, user-marked span) are exempt from padding entirely. Per-edge
   provenance persistence is a Gate-2 prerequisite alongside this bead.
