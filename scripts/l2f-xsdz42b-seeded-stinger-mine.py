#!/usr/bin/env python3
"""Pipeline-SEEDED cross-episode stinger miner (playhead-xsdz.42b research spike).

WHY (read the prior spike first)
--------------------------------
xsdz.42 (scripts/l2f-xsdz42-unsupervised-stinger-mine.py) proved two things
about learning a show's ad "stinger" from its episodes WITHOUT gold labels:

  * Claim A VALIDATED: the true stinger recurs across a show's episodes at
    envelope-NCC 0.75-0.98, saturating at 2 episodes.
  * Claim B FALSE: naive "bank the top-recurring 7 s motif" banks the INTRO
    theme, not the ad stinger -- intros/outros/theme-music/splice-silence recur
    HARDER than the interior ad stinger (true-stinger recurrence rank 3-270).

THIS spike tests the proposed fix: SEED the recurrence mining with the app's
OWN coarse ad-CANDIDATE regions (the pipeline output, available on-device for
any show a user plays -- NOT gold). Restrict the reference episode's candidate
stinger windows to those NEAR the pipeline's ad candidates, then learn the
template from the sub-window that recurs across episodes. Intros are NOT near
ad candidates, so seeding should sidestep intro-dominance entirely.

If it works, it is the direct path to a per-user on-device self-learning
stinger bank with NO manual audit.

METHOD (gold-free mining; gold used ONLY to score/interpret afterwards)
----------------------------------------------------------------------
For a show with N corpus episodes:
  1. pool = all corpus episodes (audio -> 50 Hz log1p-RMS envelope, 16 kHz to
     match the shipped bank; reused verbatim from the xsdz.42 prototype).
  2. reference = first sorted episode that has (a) >=1 pipeline ad-candidate
     span in the dumps AND (b) a gold entry so its top motif is interpretable.
     Deterministic. The SEED = that reference's candidateDecodedSpanList.
  3. Slide the 7 s / 350-frame window across the reference at 0.5 s hop, but
     KEEP ONLY windows that overlap a seed region expanded by +-margin.
  4. Rank kept windows by median peak cross-episode NCC vs every OTHER episode's
     FULL envelope (identical recurrence machinery to naive); NMS to distinct
     motifs. Top motif = mined stinger.

The ONLY difference vs naive mining is step 3's seed restriction. Same
reference, same targets, same recurrence math -> a clean seeded-vs-naive A/B.

VALIDATION
----------
  * Recovery WITHOUT intro-banking: for banked shows (morbid/nikki/conan/
    smartless) peak NCC(mined, shipped StingerBank template) per side, AND
    whether the TOP motif lands on an ad boundary vs the intro.
  * False-intro rate: fraction of shows whose TOP-1 motif is an intro/theme
    (center <90 s and not within 8 s of any gold ad edge) -- naive vs seeded.
  * Non-banked generalization: doac / unexplained / why-is-this-happening /
    techcrunch -- does a plausible near-ad-boundary recurring cue surface?
  * Seed sensitivity: margin +-30 s vs +-60 s vs whole-candidate-region-only
    (margin 0) vs point-seed (span collapsed to midpoint, +-30 s) = maximally
    undersized/noisy seed.

Determinism: fixed reference rule, fixed hop/margins, no RNG; envelopes cached.
On-device: envelope FFT-NCC only, no ML/cloud.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import re
import statistics
import sys

import numpy as np

ROOT = pathlib.Path("/Users/dabrams/playhead")
BASELINES = pathlib.Path("/Users/dabrams/playhead-baselines")
OUT_JSON = pathlib.Path(
    "/private/tmp/claude-501/-Users-dabrams-playhead/"
    "6ce9b37b-c84d-4ce3-a585-8e33b921ee5b/scratchpad/xsdz42b-seeded-results.json"
)

# Seed dumps: prefer the FLAG-OFF pure-pipeline candidates (41ep-full), fall
# back to a non-stinger 53ep dump for shows absent from the 41ep set
# (conan/doac/unexplained/themove). candidateDecodedSpanList is the coarse
# pre-refinement "where's the ad" prior (== adWindows start/end in these dumps).
DUMPS = [
    BASELINES / "playhead-dogfood-diagnostics-pipeline-dump-41ep-full-20260715.json",
    BASELINES / "playhead-dogfood-diagnostics-pipeline-dump-53ep-lexical-on-20260716.json",
]


def _load(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


# Reuse the xsdz.42 prototype verbatim (envelope, TargetScan, ncc_align,
# bank/gold loaders, recovery + position scorers).
MINE = _load("xsdz42_mine", ROOT / "scripts" / "l2f-xsdz42-unsupervised-stinger-mine.py")
full_envelope = MINE.full_envelope
TargetScan = MINE.TargetScan
recovery_ncc = MINE.recovery_ncc
annotate_position = MINE.annotate_position
load_bank = MINE.load_bank
gold_by_show = MINE.gold_by_show
AUDIO_DIR = MINE.AUDIO_DIR
ENVELOPE_HZ = MINE.ENVELOPE_HZ
TEMPLATE_FRAMES = MINE.TEMPLATE_FRAMES
CAND_HOP = MINE.CAND_HOP
NMS_RADIUS = MINE.NMS_RADIUS
RECOVERY_NCC = MINE.RECOVERY_NCC
WITHIN_TAU = MINE.WITHIN_TAU

SILENT_ENV = 0.5  # log1p-RMS below this == effectively silent (interpretation)

SHOWS = {
    "morbid": "banked",
    "the-nikki-glaser-podcast": "banked",
    "conan": "banked",
    "smartless": "banked",
    "doac": "non-banked",
    "unexplained": "non-banked",
    "why-is-this-happening-the-chris-hayes-po": "non-banked",
    "techcrunch-daily-crunch": "non-banked",
    "themove": "non-banked",  # prior "false-discovery" control
}


def key(name: str):
    m = re.match(r"^(.+?)-(\d{4}-\d{2}-\d{2})-", name)
    return (m.group(1), m.group(2)) if m else None


def build_seed_map() -> dict:
    """(slug,date) -> list[(start_s,end_s)] from the pipeline dumps."""
    seed = {}
    for dp in DUMPS:
        d = json.loads(dp.read_text())
        for e in d["episodes"]:
            k = key(e["episodeId"])
            if k is None or k in seed:  # first dump wins (41ep-full preferred)
                continue
            spans = e.get("candidateDecodedSpanList") or []
            seed[k] = [(float(s["startTime"]), float(s["endTime"])) for s in spans]
    return seed


def episodes_for(slug: str) -> list[str]:
    out = []
    for p in sorted(AUDIO_DIR.glob("*.mp3")):
        k = key(p.name[:-4])
        if k and k[0] == slug:
            out.append(p.name)
    return sorted(out)


def characterize(tmpl: np.ndarray) -> dict:
    return {
        "mean": round(float(tmpl.mean()), 2),
        "silentfrac": round(float((tmpl < SILENT_ENV).mean()), 2),
        "hi": round(float(tmpl.max()), 2),
    }


def sparkline(tmpl: np.ndarray, buckets: int = 40) -> str:
    blocks = " ▁▂▃▄▅▆▇█"
    idx = np.linspace(0, tmpl.size, buckets + 1).astype(int)
    vals = np.array([tmpl[a:b].mean() if b > a else 0.0 for a, b in zip(idx[:-1], idx[1:])])
    lo, hi = float(vals.min()), float(vals.max())
    if hi - lo < 1e-9:
        return blocks[0] * buckets
    q = ((vals - lo) / (hi - lo) * (len(blocks) - 1)).round().astype(int)
    return "".join(blocks[i] for i in q)


def mine(reference: str, others: list[str], allowed_pred, top_k: int = 6) -> tuple:
    """Rank reference windows admitted by allowed_pred(start_frame) by median
    cross-episode peak NCC vs the FULL envelope of every other episode."""
    ref_env = full_envelope(reference)
    ref_scan = TargetScan(ref_env)
    other_scans = {ep: TargetScan(full_envelope(ep)) for ep in others}
    L = TEMPLATE_FRAMES
    n_cand = (ref_env.size - L) // CAND_HOP + 1
    per = []
    for c in range(n_cand):
        s = c * CAND_HOP
        if not allowed_pred(s):
            continue
        tmpl = ref_env[s:s + L]
        peaks = {ep: other_scans[ep].peak(tmpl) for ep in others}
        per.append((s, peaks))
    if not per:
        return ref_env, []
    medians = [statistics.median(p.values()) for _, p in per]
    mins = [min(p.values()) for _, p in per]
    order = sorted(range(len(per)), key=lambda i: (medians[i], mins[i]), reverse=True)
    chosen: list[int] = []
    for i in order:
        s = per[i][0]
        if all(abs(s - per[j][0]) >= NMS_RADIUS for j in chosen):
            chosen.append(i)
        if len(chosen) >= top_k:
            break
    motifs = []
    for rank, i in enumerate(chosen):
        s, peaks = per[i]
        tmpl = ref_env[s:s + L]
        motifs.append({
            "rank": rank,
            "ref_start_frame": s,
            "ref_center_s": round((s + L / 2) / ENVELOPE_HZ, 1),
            "median_ncc": round(float(medians[i]), 3),
            "min_ncc": round(float(mins[i]), 3),
            "max_ncc": round(float(max(peaks.values())), 3),
            "per_target": {ep: round(v, 3) for ep, v in peaks.items()},
            "within_ref_multiplicity": ref_scan.qualifying_maxima(tmpl, WITHIN_TAU),
            "acoustic": characterize(tmpl),
            "spark": sparkline(tmpl),
        })
    return ref_env, motifs


def make_allowed(seed_regions, margin, point_seed=False):
    L = TEMPLATE_FRAMES

    def pred(s):
        w0 = s / ENVELOPE_HZ
        w1 = (s + L) / ENVELOPE_HZ
        for (a, b) in seed_regions:
            if point_seed:
                mid = (a + b) / 2.0
                lo, hi = mid - margin, mid + margin
            else:
                lo, hi = a - margin, b + margin
            if w1 >= lo and w0 <= hi:
                return True
        return False
    return pred


def recovery_over(motifs, ref_env, gold_tmpl):
    """(best_ncc, at_rank, top1_ncc) of mined motifs vs a gold template."""
    if not motifs:
        return 0.0, -1, 0.0
    scored = [(recovery_ncc(m, ref_env, gold_tmpl), m["rank"]) for m in motifs]
    best = max(scored, key=lambda x: x[0])
    top1 = next((v for v, r in scored if r == 0), 0.0)
    return round(best[0], 3), best[1], round(top1, 3)


def classify_top(motif, assets, reference) -> dict:
    """AD-BOUNDARY / INTRO / interior for the TOP-1 motif (gold interpretation)."""
    pos = annotate_position(motif["ref_center_s"], assets, reference)
    on_ad = pos.startswith("AD-BOUNDARY")
    is_intro = (motif["ref_center_s"] < 90.0) and not on_ad
    return {"position": pos, "on_ad_boundary": on_ad, "false_intro": is_intro}


def run() -> int:
    seed_map = build_seed_map()
    bank = load_bank()
    golds = gold_by_show()
    report = {
        "params": {
            "sample_rate": 16000, "envelope_hz": ENVELOPE_HZ,
            "template_frames": TEMPLATE_FRAMES, "cand_hop_frames": CAND_HOP,
            "primary_margin_s": 30, "recovery_ncc": RECOVERY_NCC,
            "reference_rule": "first sorted episode with a pipeline seed AND a gold entry",
            "seed_field": "candidateDecodedSpanList (pipeline coarse ad candidates)",
            "seed_dumps": [d.name for d in DUMPS],
        },
        "shows": {},
    }

    for slug, role in SHOWS.items():
        pool = episodes_for(slug)
        if len(pool) < 2:
            print(f"[skip] {slug}: {len(pool)} episode(s)")
            continue
        assets = golds.get(slug, [])
        gold_eids = {a["episode_id"] for a in assets}

        # reference = first sorted ep with a non-empty seed AND a gold entry
        reference = None
        seed_regions = None
        for ep in pool:
            k = key(ep[:-4])
            regs = seed_map.get(k)
            if regs and ep[:-4] in gold_eids:
                reference, seed_regions = ep, regs
                break
        if reference is None:  # relax the gold requirement
            for ep in pool:
                regs = seed_map.get(key(ep[:-4]))
                if regs:
                    reference, seed_regions = ep, regs
                    break
        if reference is None:
            print(f"[skip] {slug}: no episode has a pipeline seed")
            continue
        others = [e for e in pool if e != reference]

        print(f"\n=== {slug} ({role}) ref={reference[:48]} "
              f"seeds={[(round(a),round(b)) for a,b in seed_regions]} "
              f"others={len(others)} ===")

        # ---- naive (unseeded: allow every window; same reference) ----
        ref_env, naive_motifs = mine(reference, others, lambda s: True)
        naive_top = classify_top(naive_motifs[0], assets, reference) if naive_motifs else None

        # ---- seeded, primary margin +-30 s ----
        ref_env, seeded_motifs = mine(reference, others, make_allowed(seed_regions, 30))
        seeded_top = classify_top(seeded_motifs[0], assets, reference) if seeded_motifs else None

        # ---- recovery vs shipped bank templates (banked shows) ----
        recovery = {}
        if slug in bank:
            for side in ("pre", "post"):
                g = bank[slug][side]
                if g is None:
                    continue
                nb, nr, nt = recovery_over(naive_motifs, ref_env, g)
                sb, sr, st = recovery_over(seeded_motifs, ref_env, g)
                recovery[side] = {
                    "naive_best": nb, "naive_top1": nt, "naive_rank": nr,
                    "seeded_best": sb, "seeded_top1": st, "seeded_rank": sr,
                    "seeded_recovered_top1": st >= RECOVERY_NCC,
                    "seeded_recovered_topk": sb >= RECOVERY_NCC,
                }

        # ---- seed sensitivity (recovery + top-1 position) ----
        sens = {}
        variants = [
            ("margin30", make_allowed(seed_regions, 30)),
            ("margin60", make_allowed(seed_regions, 60)),
            ("region0", make_allowed(seed_regions, 0)),
            ("point30", make_allowed(seed_regions, 30, point_seed=True)),
        ]
        for vname, pred in variants:
            _, vmot = mine(reference, others, pred)
            row = {"top_center_s": vmot[0]["ref_center_s"] if vmot else None,
                   "top_median_ncc": vmot[0]["median_ncc"] if vmot else None,
                   "n_windows": None}
            if slug in bank:
                for side in ("pre", "post"):
                    g = bank[slug][side]
                    if g is None:
                        continue
                    b, _, t1 = recovery_over(vmot, ref_env, g)
                    row[f"recov_{side}_best"] = b
                    row[f"recov_{side}_top1"] = t1
            if vmot:
                row["top_class"] = classify_top(vmot[0], assets, reference)
            sens[vname] = row

        show_rec = {
            "role": role, "reference": reference, "n_others": len(others),
            "seed_regions": [[round(a, 1), round(b, 1)] for a, b in seed_regions],
            "gold_breaks_ref": next(
                ([[round(b["start_seconds"], 1), round(b["end_seconds"], 1)]
                  for b in a["full_breaks"]]
                 for a in assets if a["episode_id"] == reference), []),
            "naive_top": {**naive_motifs[0], "class": naive_top} if naive_motifs else None,
            "seeded_top": {**seeded_motifs[0], "class": seeded_top} if seeded_motifs else None,
            "seeded_motifs": seeded_motifs,
            "recovery": recovery,
            "seed_sensitivity": sens,
        }
        report["shows"][slug] = show_rec

        # ---- console summary ----
        def line(tag, m, cls):
            print(f"    {tag:7} @{m['ref_center_s']:7.1f}s med={m['median_ncc']:.3f} "
                  f"mult={m['within_ref_multiplicity']} "
                  f"{m['acoustic']}  [{cls['position']}]")
        if naive_motifs:
            line("NAIVE", naive_motifs[0], naive_top)
        if seeded_motifs:
            line("SEEDED", seeded_motifs[0], seeded_top)
        for side, r in recovery.items():
            print(f"    recovery[{side}]: naive_best={r['naive_best']:.3f} "
                  f"| seeded_top1={r['seeded_top1']:.3f} seeded_best={r['seeded_best']:.3f}"
                  f" (rank {r['seeded_rank']}) "
                  f"-> {'RECOVERED@top1' if r['seeded_recovered_top1'] else ('recovered@topk' if r['seeded_recovered_topk'] else 'miss')}")

    # ---- aggregate false-intro / on-boundary rates ----
    agg = {"naive": {"on_ad": 0, "false_intro": 0, "n": 0},
           "seeded": {"on_ad": 0, "false_intro": 0, "n": 0}}
    for slug, s in report["shows"].items():
        for who, keyname in (("naive", "naive_top"), ("seeded", "seeded_top")):
            m = s.get(keyname)
            if not m:
                continue
            agg[who]["n"] += 1
            agg[who]["on_ad"] += int(m["class"]["on_ad_boundary"])
            agg[who]["false_intro"] += int(m["class"]["false_intro"])
    report["aggregate"] = agg
    print("\n=== aggregate top-1 (all shows) ===")
    for who in ("naive", "seeded"):
        a = agg[who]
        print(f"  {who:7}: on-ad-boundary {a['on_ad']}/{a['n']}  "
              f"false-intro {a['false_intro']}/{a['n']}")

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=1))
    print(f"\nfull results -> {OUT_JSON}")
    return 0


if __name__ == "__main__":
    sys.exit(run())
