#!/usr/bin/env python3
"""Cross-episode consensus + WITHIN-EPISODE MULTIPLICITY stinger miner
(playhead-xsdz.42d research spike).

READ THE THREE PRIOR SPIKES FIRST
---------------------------------
xsdz.42  (l2f-xsdz42-unsupervised-stinger-mine.py):
    Claim A VALIDATED -- a show's ad stinger recurs cross-episode at envelope-NCC
    0.75-0.98, recoverable from 2 episodes with NO labels. Naive top-motif
    mining NO-GO: it banks the INTRO (recurs hardest).
xsdz.42b (l2f-xsdz42b-seeded-stinger-mine.py):
    Pipeline-SEEDED mining fixes intro-dominance but the top-1 near a candidate
    is often an OUTRO / mid-ad cue / splice-silence / a pipeline FALSE positive.
    NO-GO for a zero-audit bank.
xsdz.42c (l2f-xsdz42c-consensus-stinger-mine.py):
    Cross-episode boundary CONSENSUS kills RANDOM per-episode FPs (FP/far 1->0)
    and doubles on-boundary top-1 (2/9 -> 4/9). BUT the SYSTEMATIC OUTRO survives:
    the pipeline flags the show's musical outro/theme in EVERY episode, so the
    outro gets a consistent cross-episode partner and recurs as hard as the true
    stinger. Outro top-1 landings stuck at 3/9 (conan/unexplained/themove) -- the
    WALL. True stinger sits at rank <=3 for every recoverable side.

THE xsdz.42d HYPOTHESIS -- WITHIN-EPISODE MULTIPLICITY
-----------------------------------------------------
Structural distinction the consensus signal ignores:
  * a TRUE MID-ROLL stinger fires at EVERY ad break WITHIN an episode (2-4x);
  * a musical OUTRO fires exactly ONCE per episode (at the end).
So: for each surviving consensus motif, count how many DISTINCT ad-candidate
regions it recurs near (envelope-NCC >= WITHIN_TAU) WITHIN a single episode.
Require >=MULT_MIN (=2) within-episode firings, in a QUORUM of the voting
episodes. This should demote exactly the once-per-episode outros that are 42c's
wall -- WITHOUT the positional heuristic that killed themove's post-roll ad.

CRITICAL DESIGN CHOICE -- DEMOTION TIEBREAK, NOT A HARD GATE
-----------------------------------------------------------
Pre-roll-only shows (smartless/doac/why) have a SINGLE break, so NOTHING can
fire >=2x within an episode. A hard multiplicity gate would delete their correct
on-boundary pre-roll wins. So multiplicity is applied as a STABLE DEMOTION:
motifs that satisfy multiplicity are promoted ABOVE those that don't, preserving
42c's consensus order within each group. When no motif multiplies (single-break
show) the ranking is IDENTICAL to 42c consensus -- pre-roll wins preserved.
Because it only REORDERS the same 8 motifs, recovery@topk is provably unchanged
(we cannot over-filter the true stinger out of the reported set). A HARD-GATE
variant is reported in the sensitivity sweep for contrast.

METHOD (gold-free mining; gold + bank used only to score/interpret afterwards)
------------------------------------------------------------------------------
1. Reuse xsdz.42c VERBATIM (importlib): per show, build the same Episode objects
   (50 Hz log1p-RMS envelope @16 kHz, cached; anchors = start/end of every
   pipeline candidate span; per-anchor cached local TargetScan) and run the SAME
   primary consensus mine (tau=0.75, offset_tol=10 s, R=45 s, K=2, silence gate
   ON). Assert the fresh consensus top-1 matches the stored 42c result per show.
2. For each consensus motif M (template from source ep, anchor_type atype):
   for each VOTING episode e (= source + consensus partners), count the number
   of DISTINCT same-type anchor firing LOCATIONS in e whose local NCC peak
   >= WITHIN_TAU (peaks NMS'd at 7 s so adjacent anchors that find the same event
   count once). within_mult[e] = that count.
   n_multi = #voting episodes with within_mult >= MULT_MIN.
   passes = n_multi >= quorum (any: >=1 ; majority: >=ceil(V/2)).
3. Multiplicity ranking = stable partition [passing motifs] ++ [failing motifs],
   each group in 42c consensus order. multiplicity top-1 = first of that list.

VALIDATION (the GO / PARTIAL / NO-GO)
-------------------------------------
  * TOP-1 on-ad-boundary + genuine INTERIOR MID-ROLL recovery vs 42c consensus.
  * OUTRO top-1 landings: must drop from 3/9 toward ~0 (the whole point).
  * naive / seeded / consensus / consensus+MULTIPLICITY, one classifier.
  * recovery@topk vs shipped bank (morbid/nikki/conan/smartless): stinger must
    stay recoverable at NCC>=0.70 (demotion cannot lose it; verified).
  * non-banked (unexplained/doac/why/techcrunch/themove): plausible mid-roll?
  * MULT_MIN / quorum / WITHIN_TAU / hard-gate-vs-demotion sensitivity.

Determinism: sorted episodes, fixed hop/radii/thresholds, NO RNG. Envelope
FFT-NCC + integer counting only -> on-device feasible.
"""

from __future__ import annotations

import importlib.util
import json
import math
import os
import pathlib
import sys
import time

import numpy as np

ROOT = pathlib.Path("/Users/dabrams/playhead")
BASELINES = pathlib.Path("/Users/dabrams/playhead-baselines")
SCRATCH = pathlib.Path(
    "/private/tmp/claude-501/-Users-dabrams-playhead/"
    "6ce9b37b-c84d-4ce3-a585-8e33b921ee5b/scratchpad"
)
OUT_JSON = pathlib.Path(os.environ.get("XSDZ42D_OUT", str(SCRATCH / "xsdz42d-results.json")))
STORED_42C = SCRATCH / "xsdz42c-results-final.json"


def _load(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


# Reuse the xsdz.42c consensus miner (which itself reuses 42/42b) verbatim.
C = _load("xsdz42c_consensus", ROOT / "scripts" / "l2f-xsdz42c-consensus-stinger-mine.py")

Episode = C.Episode
anchor_peak = C.anchor_peak
mine_consensus = C.mine_consensus
classify = C.classify
recover_side = C.recover_side
build_seed_map = C.build_seed_map
episodes_for = C.episodes_for
seeded_naive_top1 = C.seeded_naive_top1
key = C.key
load_bank = C.load_bank
gold_by_show = C.gold_by_show
full_envelope = C.full_envelope
sparkline = C.sparkline

ENVELOPE_HZ = C.ENVELOPE_HZ      # 50
L = C.L                          # 350 (7 s)
NMS_RADIUS = C.NMS_RADIUS        # 350 frames (7 s) -> distinct firing locations
RECOVERY_NCC = C.RECOVERY_NCC    # 0.70
SEARCH_R_S = C.SEARCH_R_S        # 45.0
DELTA_HOP = C.DELTA_HOP          # 50
TAU = C.TAU                      # 0.75 (consensus partnership threshold)
OFFSET_TOL = C.OFFSET_TOL        # 10.0
SHOWS = C.SHOWS
MINE = C.MINE
GOLD_PATH = MINE.GOLD_PATH

# ---- multiplicity params (primary) ----
WITHIN_TAU = 0.75    # within-episode acoustic-match threshold (== consensus tau)
MULT_MIN = 2         # >= this many distinct within-episode firings == "multiplies"
QUORUM = "any"       # "any" (>=1 voting ep multiplies) or "majority" (>=ceil(V/2))


def _out(*a):
    print(*a, flush=True)


def nearest_edge(center_s: float, ep_stem: str, breaks_by_eid: dict):
    """(name, edge_value_s, delta_s) of the nearest gold ad edge, or None."""
    best = None
    for b in breaks_by_eid.get(ep_stem, []):
        for name, val in (("start", b["start_seconds"]), ("end", b["end_seconds"])):
            d = center_s - val
            if best is None or abs(d) < abs(best[2]):
                best = (name, round(float(val), 1), round(float(d), 1))
    return best


def within_ep_mult(ep: "Episode", tmpl: np.ndarray, atype: str, tau: float) -> int:
    """Count DISTINCT same-type candidate-region firing LOCATIONS inside one
    episode where the motif's local envelope-NCC peak >= tau. Peaks are NMS'd at
    NMS_RADIUS (7 s) so two adjacent anchors that lock onto the SAME acoustic
    event count once -> this is 'how many separate breaks does the motif fire
    near in this episode', the mid-roll-vs-outro discriminator."""
    hits = []  # (abs_peak_frame, peak)
    for entry in ep.ascan:
        if entry[0] != atype:
            continue
        r = anchor_peak(entry, tmpl)
        if r is None:
            continue
        peak, roff = r
        if peak >= tau:
            _atype, _a0, anchor_frame, _scan = entry
            abs_frame = anchor_frame + roff * ENVELOPE_HZ
            hits.append((abs_frame, peak))
    hits.sort(key=lambda h: -h[1])  # greedy NMS keeps the strongest per location
    kept = []
    for f, p in hits:
        if all(abs(f - kf) >= NMS_RADIUS for kf, _ in kept):
            kept.append((f, p))
    return len(kept)


def annotate_multiplicity(motifs, eps, by_stem, within_tau):
    """Attach within-episode multiplicity (per voting episode) to every motif.
    Voting episodes = source + consensus partner stems. Independent of MULT_MIN /
    quorum so a single pass serves the whole sensitivity sweep."""
    for m in motifs:
        src = eps[m["src_idx"]]
        tmpl = src.env[m["ref_start_frame"]:m["ref_start_frame"] + L]
        atype = m["anchor_type"]
        voting = [m["src_ep"]] + [p[0] for p in m.get("partners", [])]
        # de-dupe voting stems (a partner list can, rarely, repeat) but keep order
        seen = set()
        voting = [v for v in voting if not (v in seen or seen.add(v))]
        mult = {}
        for stem in voting:
            ep = by_stem.get(stem)
            mult[stem] = within_ep_mult(ep, tmpl, atype, within_tau) if ep else 0
        m["within_mult"] = mult
        m["n_voting"] = len(voting)


def passes_multiplicity(m, mult_min: int, quorum: str) -> bool:
    mult = m.get("within_mult", {})
    n_multi = sum(1 for v in mult.values() if v >= mult_min)
    V = m.get("n_voting", len(mult))
    if quorum == "majority":
        need = math.ceil(V / 2)
    else:  # "any"
        need = 1
    return n_multi >= need


def multiplicity_rank(motifs, mult_min: int, quorum: str, hard_gate: bool = False):
    """Stable demotion: passing motifs first (in consensus order), then failing.
    hard_gate=True DELETES failing motifs instead of demoting them (contrast
    variant -- can lose recovery@topk on single-break shows)."""
    passing = [m for m in motifs if passes_multiplicity(m, mult_min, quorum)]
    failing = [m for m in motifs if not passes_multiplicity(m, mult_min, quorum)]
    if hard_gate:
        return passing
    return passing + failing


def run() -> int:
    t0 = time.time()
    seed_map = build_seed_map()
    bank = load_bank()
    _ = gold_by_show()
    breaks_by_eid = {}
    for a in json.loads(GOLD_PATH.read_text())["assets"]:
        breaks_by_eid[a["episode_id"]] = a["full_breaks"]

    stored = json.loads(STORED_42C.read_text()) if STORED_42C.exists() else {"shows": {}}

    report = {
        "params": {
            "envelope_hz": ENVELOPE_HZ, "template_frames": L, "search_radius_s": SEARCH_R_S,
            "consensus_tau": TAU, "offset_tol_s": OFFSET_TOL, "delta_hop_frames": DELTA_HOP,
            "within_tau": WITHIN_TAU, "mult_min": MULT_MIN, "quorum": QUORUM,
            "nms_radius_frames": NMS_RADIUS, "recovery_ncc": RECOVERY_NCC,
            "ranking": "stable demotion (passing motifs promoted above failing, "
                       "consensus order preserved within each group)",
            "determinism": "sorted episodes, fixed hop/radii/thresholds, no RNG",
        },
        "shows": {},
    }
    radius_frames = int(round(SEARCH_R_S * ENVELOPE_HZ))

    # sensitivity grid over the multiplicity layer (reuses the same consensus mine)
    sweep = [
        ("primary(mult>=2,any,wtau0.75)", dict(mult_min=2, quorum="any", wtau=0.75, gate=False)),
        ("mult>=2,majority", dict(mult_min=2, quorum="majority", wtau=0.75, gate=False)),
        ("mult>=3,any", dict(mult_min=3, quorum="any", wtau=0.75, gate=False)),
        ("wtau0.70", dict(mult_min=2, quorum="any", wtau=0.70, gate=False)),
        ("wtau0.80", dict(mult_min=2, quorum="any", wtau=0.80, gate=False)),
        ("HARDGATE(mult>=2,any)", dict(mult_min=2, quorum="any", wtau=0.75, gate=True)),
    ]

    for slug, role in SHOWS.items():
        pool = episodes_for(slug)
        cand_eps = [ep for ep in pool if seed_map.get(key(ep[:-4]))]
        if len(cand_eps) < 2:
            _out(f"[skip] {slug}: {len(cand_eps)} candidate-bearing episode(s)")
            continue
        eps = []
        durations = {}
        for ep in cand_eps:
            e = Episode(ep, seed_map[key(ep[:-4])], radius_frames)
            eps.append(e)
            durations[e.stem] = round(e.env.size / ENVELOPE_HZ, 1)
        by_stem = {e.stem: e for e in eps}
        n = len(eps)
        _out(f"\n=== {slug} ({role}) candidate-eps={n} "
             f"anchors={[len(e.anchors) for e in eps]} ===")

        # ---- primary consensus mine (identical to xsdz.42c) ----
        motifs = mine_consensus(eps, TAU, OFFSET_TOL, radius_frames, DELTA_HOP,
                                silence_gate=True)
        for m in motifs:
            m["class"] = classify(m["ref_center_s"], m["src_ep"], breaks_by_eid,
                                  durations.get(m["src_ep"], 0.0))
            m["nearest_edge"] = nearest_edge(m["ref_center_s"], m["src_ep"], breaks_by_eid)

        # determinism cross-check vs stored 42c consensus top-1
        stored_top = (stored["shows"].get(slug, {}) or {}).get("top1")
        consensus_top = motifs[0] if motifs else None
        matches_42c = None
        if stored_top and consensus_top:
            matches_42c = abs(stored_top["ref_center_s"] - consensus_top["ref_center_s"]) < 0.05

        # ---- attach within-episode multiplicity (primary within_tau) ----
        annotate_multiplicity(motifs, eps, by_stem, WITHIN_TAU)

        # ---- primary multiplicity ranking ----
        ranked = multiplicity_rank(motifs, MULT_MIN, QUORUM)
        mult_top1 = ranked[0] if ranked else None

        # ---- recovery vs shipped bank (over the SAME motif set; identical topk) ----
        recovery = {}
        if slug in bank:
            for side in ("pre", "post"):
                g = bank[slug][side]
                if g is None:
                    continue
                atype = "start" if side == "pre" else "end"
                cb, cr, ct_cons = recover_side(motifs, eps, g)          # consensus order
                # top-1 under the MULTIPLICITY order:
                mt = recover_side(ranked[:1], eps, g)[0] if ranked else 0.0
                sb, sr, _ = recover_side(motifs, eps, g, anchor_type_filter=atype)
                recovery[side] = {
                    "consensus_top1": ct_cons, "mult_top1": round(float(mt), 3),
                    "best_topk": cb, "best_rank": cr,
                    "sideedge_best": sb, "sideedge_rank": sr,
                    "recovered_topk": cb >= RECOVERY_NCC,
                    "recovered_mult_top1": mt >= RECOVERY_NCC,
                }

        # ---- naive/seeded A/B (same harness classifier; reads stored 42b) ----
        ns = seeded_naive_top1(slug, pool, seed_map, breaks_by_eid, durations)

        # ---- multiplicity sensitivity sweep (reuses same consensus motifs) ----
        # Re-annotate multiplicity for each variant's within_tau (no caching:
        # within_mult is a single field overwritten per wtau, so a cache would
        # let a later same-wtau variant read a prior variant's stale values).
        sens = {}
        for vname, vp in sweep:
            annotate_multiplicity(motifs, eps, by_stem, vp["wtau"])
            r = multiplicity_rank(motifs, vp["mult_min"], vp["quorum"], vp["gate"])
            top = r[0] if r else None
            row = {"n_motifs_kept": len(r)}
            if top:
                cl = top["class"]
                ne = top.get("nearest_edge")
                row.update({
                    "top_center_s": top["ref_center_s"], "top_type": top["anchor_type"],
                    "top_ep": top["src_ep"][-10:], "top_label": cl["label"],
                    "top_on_ad": cl["on_ad"],
                    "top_edge_val": (ne[1] if ne else None),
                    "top_within_mult": top.get("within_mult"),
                    "top_n_voting": top.get("n_voting"),
                })
            sens[vname] = row
        # restore primary annotation (last sweep var may have changed within_mult)
        annotate_multiplicity(motifs, eps, by_stem, WITHIN_TAU)
        ranked = multiplicity_rank(motifs, MULT_MIN, QUORUM)
        mult_top1 = ranked[0] if ranked else None

        # add sparkline to reported motifs
        for m in motifs:
            tmpl = eps[m["src_idx"]].env[m["ref_start_frame"]:m["ref_start_frame"] + L]
            m["spark"] = sparkline(tmpl)

        report["shows"][slug] = {
            "role": role, "n_candidate_eps": n,
            "candidate_eps": [e.stem for e in eps],
            "durations_s": durations,
            "gold_breaks": {e.stem[-10:]:
                            [[round(b["start_seconds"], 1), round(b["end_seconds"], 1)]
                             for b in breaks_by_eid.get(e.stem, [])] for e in eps},
            "consensus_top1": consensus_top, "mult_top1": mult_top1,
            "matches_stored_42c": matches_42c,
            "motifs": motifs,
            "mult_rank_order_centers": [round(m["ref_center_s"], 1) for m in ranked],
            "recovery": recovery, "naive_seeded": ns, "sensitivity": sens,
        }

        # ---- console ----
        if consensus_top and mult_top1:
            cc = consensus_top["class"]
            mc = mult_top1["class"]
            _out(f"  consensus top1 @{consensus_top['ref_center_s']:8.1f} "
                 f"[{consensus_top['anchor_type']}] -> [{cc['label']}] {cc['nearest']}"
                 f"  (matches42c={matches_42c})")
            _out(f"  MULTIPLICITY top1 @{mult_top1['ref_center_s']:8.1f} "
                 f"[{mult_top1['anchor_type']}] mult={mult_top1.get('within_mult')} "
                 f"-> [{mc['label']}] {mc['nearest']}")
        for side, r in recovery.items():
            _out(f"  recovery[{side}]: best_topk={r['best_topk']:.3f}@rk{r['best_rank']} "
                 f"cons_top1={r['consensus_top1']:.3f} mult_top1={r['mult_top1']:.3f} "
                 f"({'REC@topk' if r['recovered_topk'] else 'MISS'})")

        # checkpoint partial results after every show
        report["params"]["wall_s_partial"] = round(time.time() - t0, 1)
        OUT_JSON.write_text(json.dumps(report, indent=1))

    # ---- aggregate top-1 (4 columns, one classifier) ----
    cats = ("on", "false", "intro", "outro", "interior", "fp")
    agg = {w: {c: 0 for c in cats} | {"n": 0, "midroll": 0}
           for w in ("naive", "seeded", "consensus", "multiplicity")}
    lab2cat = {"intro": "intro", "outro": "outro", "interior": "interior", "FP/far": "fp"}
    for slug, s in report["shows"].items():
        ns = s.get("naive_seeded") or {}
        for who in ("naive", "seeded"):
            t = ns.get(who)
            if not t:
                continue
            agg[who]["n"] += 1
            agg[who]["on" if t["on_ad"] else "false"] += 1
            if not t["on_ad"]:
                agg[who][lab2cat[t["label"]]] += 1
        for who, mt in (("consensus", s.get("consensus_top1")),
                        ("multiplicity", s.get("mult_top1"))):
            if not mt:
                continue
            c = mt["class"]
            agg[who]["n"] += 1
            agg[who]["on" if c["on_ad"] else "false"] += 1
            if not c["on_ad"]:
                agg[who][lab2cat[c["label"]]] += 1
            # genuine interior mid-roll: on-boundary AND nearest edge > 60 s
            ne = mt.get("nearest_edge")
            if c["on_ad"] and ne and ne[1] > 60.0:
                agg[who]["midroll"] += 1
    report["aggregate_top1"] = agg

    _out("\n=== AGGREGATE TOP-1 (all shows, same classifier) ===")
    for who in ("naive", "seeded", "consensus", "multiplicity"):
        a = agg[who]
        _out(f"  {who:12}: on-boundary {a['on']}/{a['n']}  (genuine mid-roll {a['midroll']})"
             f"  outro {a['outro']}  interior {a['interior']}  intro {a['intro']}  FP/far {a['fp']}")

    report["params"]["wall_s"] = round(time.time() - t0, 1)
    OUT_JSON.write_text(json.dumps(report, indent=1))
    _out(f"\nwall={report['params']['wall_s']}s   full results -> {OUT_JSON}")
    return 0


if __name__ == "__main__":
    sys.exit(run())
