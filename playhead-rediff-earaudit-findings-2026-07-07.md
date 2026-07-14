# Rediff ear-audit findings (xsdz.31) — 2026-07-07

Independent ground-truth ruler for the rediff width oracle. Dan ear-audited all **70** rediff-only R3 (uncorroborated, ≥20s, audit_priority=1) auto-promoted spans via `scripts/l2f-earaudit-gui.py`. Raw verdicts: `playhead-rediff-earaudit-results.jsonl`.

## Rubric (Dan)

- **ad** — was an ad, or within ~3–4s of one (tight)
- **boundary-off** — real ad present but ≥3–4s of content/adjacent ad, or ad starts mid-span
- **content** — listened ~20s and it was content

Dan's post-hoc nuance on `content`: heterogeneous — *some* was show music leading in, but *much* was **partway through a commercial** or **the last ~5s of a previous commercial** — i.e. the span **clips a real DAI transition**, window misplaced. That reclassifies most of the bucket from *hallucination* to *placement/width error*.

## Result

| verdict | n |
|---|---|
| ad (tight) | 25 |
| boundary-off (real ad, wrong edges) | 14 |
| content, **abuts real DAI structure** (≤15s) | 22 |
| content, **truly isolated** (hallucination) | 9 |

## Two rulers

- **Strict presence precision** (ad+bnd only): 39/70 = **56%** — but this counts every edge-clipping `content` as a hallucination, which Dan's ears contradict.

- **Structure-present precision** (ad+bnd+edge-clipping content): 61/70 = **87%** (Wilson 95% CI 77%–93%) — rediff lands **near a real DAI splice** this often.

- **Boundary-error rate** among real ads: 14/39 = **36%** (CI 23%–52%) — only 64% had tight edges.

- **True isolated-hallucination rate**: 9/70 = **13%**.

## Interpretation

Rediff detects **byte-change (DAI insertion)**, not *ad* — so standalone R3 promotion is noisy. But the noise is overwhelmingly **width/placement**, not **presence**: the underlying splice is really there ~87% of the time. The transitions Dan heard (music lead-in = ad-START splice; ad tail = ad-END splice) are exactly the **edges** the width oracle snaps a presence core to. This **validates the oracle architecture**: `xsdz.29` gates every rediff slot on an existing FM/lexical presence core (`coreCoverage ≥ min`) and uses rediff only for **width** — so the production oracle would NOT emit the 31 standalone `content` spans (no presence core to attach to). The audit measured raw R3 (the weakest tier); the oracle is the presence-gated form.

**Caveat:** 20 of the 22 edge-clippers are 'near a rediff slot', and R3 spans are rediff-derived, so that adjacency is partly circular. It is credible because **Dan's ears independently** heard real ad audio at the edges — not because rediff agrees with itself.

## Systematic failure: The Daily Show: Ears Edition

4 of 9 isolated hallucinations are one show (1-ad / 5-content overall). Candidate cause: **per-fetch re-encode** (whole-file diff → phantom slots) — the failure `xsdz.29`'s `alignedFraction ≥ 0.5` guard exists to reject. UNCONFIRMED here: `alignedFraction` is null in this rediff dump, so re-encode is a hypothesis, not established. Worth a targeted double-fetch with alignment capture.

## Actions

1. **Scrub the 9 isolated hallucinations** from the corpus — deferred to **l2f.5** (golden regen) because most live in *tracked* annotation files; ad-hoc mutation would create golden drift. List below.

2. **Keep the 22 edge-clippers** — they mark real transitions; deleting throws away boundary signal. They need re-bounding (the oracle's job), not deletion.

3. **The activation flip must measure the presence-gated `xsdz.29` output**, not standalone R3: treatment dump → `l2f-bd4xqf-compare.py --max-fp-seconds-per-episode` (xsdz.32 gate) + a fresh ear-audit of what the oracle actually emits. The 36% boundary-error is the bar the width claim must clear.


### Scrub list for l2f.5 (9 isolated; ear-verified content, no DAI structure within 15s)

| episodeId | start–end (s) | tracked? | nearest verified / rediff (s) |
|---|---|---|---|
| fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros | 3163–3196 | no | — / 69 |
| the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve | 718–739 | yes | 339 / 118 |
| the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve | 880–905 | yes | 173 / 227 |
| the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve | 1694–1726 | yes | 583 / 52 |
| the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve | 2237–2268 | yes | 1126 / 427 |
| the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev | 3096–3128 | yes | — / 1352 |
| the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev | 3156–3225 | yes | — / 1255 |
| why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi | 3771–3792 | no | 3723 / 152 |
| why-is-this-happening-the-chris-hayes-po-2026-05-26-the-ai-end-game-the-ethics-of-ai-with-ti | 84–195 | yes | — / 26 |
