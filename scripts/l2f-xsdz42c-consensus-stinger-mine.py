#!/usr/bin/env python3
"""Cross-episode boundary-CONSENSUS stinger miner (playhead-xsdz.42c research spike).

WHY (read the two prior spikes first)
-------------------------------------
xsdz.42  (scripts/l2f-xsdz42-unsupervised-stinger-mine.py):
    Claim A VALIDATED -- a show's ad stinger recurs across its episodes at
    envelope-NCC 0.75-0.98, recoverable from 2 episodes WITHOUT gold.
    Naive top-motif mining: NO-GO -- it banks the INTRO (recurs hardest).
xsdz.42b (scripts/l2f-xsdz42b-seeded-stinger-mine.py):
    Pipeline-SEEDED mining (restrict windows to near the app's ad candidates)
    fixes intro-dominance (false-intro top-1 1/9 -> 0/9) and lifts the stinger's
    rank (top-k recovery 2/6 -> 4/6). BUT on-ad-boundary TOP-1 stayed 2/9: the
    top-1 motif near a candidate is often an OUTRO / mid-ad cue / splice-silence
    / the pipeline's OWN false-positive candidate. Recurrence amplifies whatever
    the pipeline believes -- including its errors. NO-GO for a zero-audit bank.

THE xsdz.42c HYPOTHESIS: cross-episode boundary CONSENSUS
--------------------------------------------------------
Require the mined stinger to recur near a pipeline ad-candidate at a CONSISTENT
WITHIN-BREAK RELATIVE POSITION across MULTIPLE episodes. Structural distinction:
  * TRUE stinger  -> appears at EVERY episode's ad boundary, so it has a
    cross-episode partner at the SAME offset RELATIVE TO A CANDIDATE EDGE in
    multiple episodes.
  * OUTRO / theme -> fixed ABSOLUTE position (episode end), so its position
    relative to a candidate edge is NOT consistent across episodes -> no
    break-relative partner.
  * pipeline FALSE-POSITIVE candidate -> episode-unique content, so it has NO
    acoustic cross-episode partner near a candidate at all.
VOTE across episodes' candidate regions; keep only motifs whose acoustic match
recurs near a same-type candidate edge at a consistent relative offset in >=K
episodes. Consensus should suppress exactly the outros + FPs that killed 42b.

METHOD (gold-free mining; gold used ONLY to score/interpret afterwards)
----------------------------------------------------------------------
For each show with >=2 candidate-bearing episodes:
  1. Every episode -> 50 Hz log1p-RMS envelope @16 kHz (bank parity; cached).
  2. ANCHORS = the start and end edge of every pipeline candidate span
     (candidateDecodedSpanList, flag-OFF 41ep dump preferred). Each anchor is
     tagged type in {start,end}.
  3. PROTOTYPES = for each source episode's anchor A, slide a 7 s window over
     [A-R, A+R] at hop; each window is tagged with rel-offset delta = center - A.
  4. CONSENSUS VOTE = for a prototype (template, type, delta) from episode e,
     count how many OTHER candidate-episodes e' contain a same-type anchor A'
     whose local NCC peak (in [A'-R, A'+R]) is >= tau AT a relative offset within
     +-offset_tol of delta. vote = 1 (source) + #partner episodes.
  5. Keep prototypes with vote >= K; NMS to distinct motifs (acoustic + spatial);
     rank by (vote, median partner NCC). TOP consensus motif = mined stinger.
  Optional acoustic SILENCE GATE: drop mostly-silent templates (splice-silence,
  not a musical stinger) -- an intrinsic, gold-free prior. Reported ON and OFF.

VALIDATION (the GO / NO-GO)
---------------------------
  * TOP-1 on-ad-boundary recovery vs xsdz.42b (naive 3/9, seeded 2/9): does
    consensus raise it? Per-show top-1 landing: real boundary vs outro/FP.
  * false-outro / false-FP rate of the top-1 (must DROP vs seeded 7/9).
  * mined-vs-gold-template NCC on the 4 testable banked shows.
  * non-banked generalization (doac/unexplained/why/techcrunch/themove).
  * K sensitivity + offset-tolerance + tau + silence-gate sweeps.

All three columns (naive / seeded / consensus) are scored in THIS harness with
the SAME classifier so the A/B is clean. Determinism: sorted episodes, fixed
hop/radii, no RNG; envelopes cached to .npy.
"""

from __future__ import annotations

import importlib.util
import json
import os
import pathlib
import re
import statistics
import sys
import time

import numpy as np

ROOT = pathlib.Path("/Users/dabrams/playhead")
BASELINES = pathlib.Path("/Users/dabrams/playhead-baselines")
SCRATCH = pathlib.Path(
    "/private/tmp/claude-501/-Users-dabrams-playhead/"
    "6ce9b37b-c84d-4ce3-a585-8e33b921ee5b/scratchpad"
)
OUT_JSON = pathlib.Path(os.environ.get(
    "XSDZ42C_OUT", str(SCRATCH / "xsdz42c-consensus-results.json")))

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


# Reuse the xsdz.42 envelope/NCC/recovery machinery, and xsdz.42b's seeded miner
# (for the seeded A/B column), verbatim via importlib.
MINE = _load("xsdz42_mine", ROOT / "scripts" / "l2f-xsdz42-unsupervised-stinger-mine.py")
SEED42B = _load("xsdz42b_seed", ROOT / "scripts" / "l2f-xsdz42b-seeded-stinger-mine.py")

full_envelope = MINE.full_envelope
TargetScan = MINE.TargetScan
ncc_curve = MINE.PROTO.ncc_curve   # scipy-correlate NCC; used on LOCAL sub-arrays
recovery_ncc = MINE.recovery_ncc
load_bank = MINE.load_bank
gold_by_show = MINE.gold_by_show
ncc_align = MINE.ncc_align
AUDIO_DIR = MINE.AUDIO_DIR
ENVELOPE_HZ = MINE.ENVELOPE_HZ          # 50
TEMPLATE_FRAMES = MINE.TEMPLATE_FRAMES  # 350 (7 s)
NMS_RADIUS = MINE.NMS_RADIUS
RECOVERY_NCC = MINE.RECOVERY_NCC        # 0.70

L = TEMPLATE_FRAMES
SILENT_ENV = 0.5     # log1p-RMS below this == effectively silent
SILENT_GATE = 0.30   # drop templates with >= this silent fraction (silence gate)

# ---- consensus params (primary) ----
SEARCH_R_S = 45.0    # search radius around each candidate anchor edge (covers
                     # boundary undersizing up to ~40 s; playhead-4xqf)
DELTA_HOP = 50       # frames (1.0 s) stride for the relative-offset sweep
TAU = 0.75           # acoustic NCC partnership threshold (stinger x-ep 0.75-0.98)
OFFSET_TOL = 10.0    # s: how consistent the break-relative offset must be
K_PRIMARY = 2        # min distinct episodes in a surviving consensus cluster
TOP_K = 8            # distinct consensus motifs reported per show

SHOWS = {
    "morbid": "banked",
    "the-nikki-glaser-podcast": "banked",
    "conan": "banked",
    "smartless": "banked",
    "doac": "non-banked",
    "unexplained": "non-banked",
    "why-is-this-happening-the-chris-hayes-po": "non-banked",
    "techcrunch-daily-crunch": "non-banked",
    "themove": "non-banked",
}


def key(name: str):
    m = re.match(r"^(.+?)-(\d{4}-\d{2}-\d{2})-", name)
    return (m.group(1), m.group(2)) if m else None


def build_seed_map() -> dict:
    """(slug,date) -> list[(start_s,end_s)] pipeline candidate spans."""
    seed = {}
    for dp in DUMPS:
        d = json.loads(dp.read_text())
        for e in d["episodes"]:
            k = key(e["episodeId"])
            if k is None or k in seed:
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


# ---------------------------------------------------------------------------
# The consensus miner
# ---------------------------------------------------------------------------
class Episode:
    __slots__ = ("stem", "env", "anchors", "ascan")

    def __init__(self, fn: str, spans: list, radius_frames: int):
        self.stem = fn[:-4]
        self.env = full_envelope(fn)
        half = L // 2
        # anchors: (anchor_frame, type)  type in {"start","end"}
        self.anchors = []
        for (a, b) in spans:
            self.anchors.append((int(round(a * ENVELOPE_HZ)), "start"))
            self.anchors.append((int(round(b * ENVELOPE_HZ)), "end"))
        # Precompute, per anchor, a TargetScan over the LOCAL sub-array covering
        # every window whose center is within +-radius of the anchor. Built once,
        # reused across every prototype and every sensitivity variant -> the
        # target FFT + local-variance are never recomputed per NCC call.
        self.ascan = []  # (atype, a0_frame, anchor_frame, TargetScan)
        for (anc, atype) in self.anchors:
            a0 = max(0, anc - radius_frames - half)
            b0 = min(self.env.size, anc + radius_frames + half)
            sub = self.env[a0:b0]
            if sub.size < L:
                continue
            self.ascan.append((atype, a0, anc, TargetScan(sub)))


def anchor_peak(scan_entry, tmpl: np.ndarray):
    """Best NCC peak (and its rel-offset in s) of `tmpl` over one anchor's cached
    local sub-array (TargetScan). Returns (peak, rel_offset_s)."""
    _atype, a0, anchor_frame, scan = scan_entry
    cur = scan.curve(tmpl)
    if cur is None:
        return None
    j = int(np.argmax(cur))
    peak = float(cur[j])
    center_frame = a0 + j + (L // 2)
    rel_off_s = (center_frame - anchor_frame) / ENVELOPE_HZ
    return peak, rel_off_s


def mine_consensus(eps: list[Episode], tau: float, offset_tol: float,
                   radius_frames: int, delta_hop: int, silence_gate: bool,
                   outro_gate: bool = False):
    """Score every (source-anchor, delta) prototype by cross-episode boundary
    consensus. Returns a ranked list of distinct motifs (dicts)."""
    half = L // 2
    durs = {e.stem: e.env.size / ENVELOPE_HZ for e in eps}
    protos = []  # raw scored prototypes
    for si, src in enumerate(eps):
        others = [o for oi, o in enumerate(eps) if oi != si]
        for (anc, atype) in src.anchors:
            # sweep relative offset delta over [-R, +R]
            lo = anc - radius_frames
            hi = anc + radius_frames
            for center in range(lo, hi + 1, delta_hop):
                s = center - half           # window start frame
                if s < 0 or s + L > src.env.size:
                    continue
                if outro_gate and (center / ENVELOPE_HZ) > 0.90 * durs[src.stem]:
                    continue                 # drop end-of-episode (outro) windows
                tmpl = src.env[s:s + L]
                ch = characterize(tmpl)
                if silence_gate and ch["silentfrac"] >= SILENT_GATE:
                    continue
                delta = (center - anc) / ENVELOPE_HZ
                # vote across other episodes' same-type anchors (cached scans)
                partners = []  # (ep_stem, peak)
                for o in others:
                    best = None
                    for entry in o.ascan:
                        if entry[0] != atype:
                            continue
                        r = anchor_peak(entry, tmpl)
                        if r is None:
                            continue
                        peak, roff = r
                        if peak >= tau and abs(roff - delta) <= offset_tol:
                            if best is None or peak > best:
                                best = peak
                    if best is not None:
                        partners.append((o.stem, round(best, 3)))
                vote = 1 + len(partners)
                if vote < 2:
                    continue
                med = statistics.median([p for _, p in partners]) if partners else 0.0
                protos.append({
                    "src_ep": src.stem,
                    "src_idx": si,
                    "anchor_type": atype,
                    "anchor_s": round(anc / ENVELOPE_HZ, 1),
                    "delta_s": round(delta, 1),
                    "ref_start_frame": s,
                    "ref_center_s": round(center / ENVELOPE_HZ, 1),
                    "vote": vote,
                    "median_ncc": round(float(med), 3),
                    "partners": partners,
                    "acoustic": ch,
                })
    # rank: vote desc, median_ncc desc, then deterministic tiebreak
    protos.sort(key=lambda m: (-m["vote"], -m["median_ncc"], m["src_ep"],
                               m["ref_center_s"]))
    # NMS to distinct motifs: suppress spatial dupes (same source ep, same type,
    # within NMS_RADIUS) and acoustic dupes (NCC>=0.85 vs an already-kept motif)
    kept = []
    kept_tmpls = []
    for m in protos:
        tmpl = eps[m["src_idx"]].env[m["ref_start_frame"]:m["ref_start_frame"] + L]
        dup = False
        for km, kt in zip(kept, kept_tmpls):
            if (km["src_ep"] == m["src_ep"] and km["anchor_type"] == m["anchor_type"]
                    and abs(km["ref_start_frame"] - m["ref_start_frame"]) < NMS_RADIUS):
                dup = True
                break
            _, nc = ncc_align(kt, tmpl)  # equal length -> single alignment
            if nc >= 0.85:
                dup = True
                break
        if dup:
            continue
        m = dict(m)
        m["spark"] = sparkline(tmpl)
        kept.append(m)
        kept_tmpls.append(tmpl)
        if len(kept) >= TOP_K:
            break
    return kept


# ---------------------------------------------------------------------------
# Classification / recovery (gold + bank used here ONLY, post-hoc)
# ---------------------------------------------------------------------------
def classify(center_s: float, ep_stem: str, breaks_by_eid: dict,
             duration_s: float) -> dict:
    breaks = breaks_by_eid.get(ep_stem, [])
    best = None
    for b in breaks:
        for name, val in (("start", b["start_seconds"]), ("end", b["end_seconds"])):
            d = center_s - val
            if best is None or abs(d) < abs(best[2]):
                best = (name, round(val, 1), round(d, 1))
    on_ad = best is not None and abs(best[2]) <= 8.0
    is_intro = (center_s < 90.0) and not on_ad
    is_outro = (duration_s > 0 and center_s > 0.90 * duration_s) and not on_ad
    far = best is not None and abs(best[2]) > 60.0  # FP-ish: no gold edge nearby
    if on_ad:
        label = "on-boundary"
    elif is_intro:
        label = "intro"
    elif is_outro:
        label = "outro"
    elif far or best is None:
        label = "FP/far"
    else:
        label = "interior"
    return {
        "label": label, "on_ad": on_ad, "false_intro": is_intro,
        "nearest": (f"{best[0]}={best[1]}s (Δ{best[2]:+.1f})" if best else "no-gold"),
    }


def recover_side(motifs, eps, gold_tmpl, anchor_type_filter=None):
    """(best_ncc, rank, top1_ncc) of motifs vs a gold/bank template. If
    anchor_type_filter is set, restrict to motifs of that anchor type."""
    if not motifs:
        return 0.0, -1, 0.0
    scored = []
    for rank, m in enumerate(motifs):
        if anchor_type_filter and m["anchor_type"] != anchor_type_filter:
            continue
        env = eps[m["src_idx"]].env
        scored.append((recovery_ncc(m, env, gold_tmpl), rank))
    if not scored:
        return 0.0, -1, 0.0
    best = max(scored, key=lambda x: x[0])
    top1 = next((v for v, r in scored if r == 0), 0.0)
    return round(best[0], 3), best[1], round(top1, 3)


# ---------------------------------------------------------------------------
# Seeded top-1 reproduction (xsdz.42b) in THIS harness, for a clean A/B column
# ---------------------------------------------------------------------------
_PRIOR42B = None


def _prior42b():
    """Load xsdz.42b's stored naive/seeded TOP-1 (avoids re-running the very slow
    full unseeded naive scan; the positions are byte-identical to a fresh run)."""
    global _PRIOR42B
    if _PRIOR42B is None:
        p = SCRATCH / "xsdz42b-seeded-results.json"
        _PRIOR42B = json.loads(p.read_text())["shows"] if p.exists() else {}
    return _PRIOR42B


def seeded_naive_top1(slug, pool, seed_map, breaks_by_eid, durations):
    """xsdz.42b naive + seeded (margin30) TOP-1, re-classified with THIS harness's
    classifier for a clean same-scoring A/B. Positions are read from the stored
    xsdz.42b results (a fresh recompute of the unseeded naive scan over 1-2 h
    episodes is prohibitively slow and byte-identical)."""
    s = _prior42b().get(slug)
    if not s:
        return None
    reference = s["reference"][:-4] if s.get("reference") else None
    if reference is None:
        return None
    dur = durations.get(reference)
    if dur is None:
        try:
            dur = full_envelope(reference + ".mp3").size / ENVELOPE_HZ
        except Exception:
            dur = 0.0

    def top1(m):
        if not m:
            return None
        cl = classify(m["ref_center_s"], reference, breaks_by_eid, dur)
        return {"center_s": m["ref_center_s"], "median_ncc": m.get("median_ncc"),
                "acoustic": m.get("acoustic"), **cl}
    return {"reference": reference,
            "naive": top1(s.get("naive_top")),
            "seeded": top1(s.get("seeded_top"))}


def run() -> int:
    t0 = time.time()
    seed_map = build_seed_map()
    bank = load_bank()
    golds = gold_by_show()
    breaks_by_eid = {}
    for a in json.loads(MINE.GOLD_PATH.read_text())["assets"]:
        breaks_by_eid[a["episode_id"]] = a["full_breaks"]

    report = {
        "params": {
            "sample_rate": 16000, "envelope_hz": ENVELOPE_HZ, "template_frames": L,
            "search_radius_s": SEARCH_R_S, "delta_hop_frames": DELTA_HOP,
            "tau": TAU, "offset_tol_s": OFFSET_TOL, "K_primary": K_PRIMARY,
            "silence_gate": SILENT_GATE, "recovery_ncc": RECOVERY_NCC,
            "seed_field": "candidateDecodedSpanList (pipeline coarse ad candidates)",
        },
        "shows": {},
    }
    radius_frames = int(round(SEARCH_R_S * ENVELOPE_HZ))

    for slug, role in SHOWS.items():
        pool = episodes_for(slug)
        cand_eps = [ep for ep in pool if seed_map.get(key(ep[:-4]))]
        if len(cand_eps) < 2:
            print(f"[skip] {slug}: {len(cand_eps)} candidate-bearing episode(s)")
            continue
        durations = {}
        eps = []
        for ep in cand_eps:
            e = Episode(ep, seed_map[key(ep[:-4])], radius_frames)
            eps.append(e)
            durations[e.stem] = round(e.env.size / ENVELOPE_HZ, 1)
        n = len(eps)
        print(f"\n=== {slug} ({role})  candidate-eps={n} "
              f"anchors={[len(e.anchors) for e in eps]} ===")

        # ---- primary consensus (silence gate ON, K=2) ----
        motifs = mine_consensus(eps, TAU, OFFSET_TOL, radius_frames, DELTA_HOP,
                                silence_gate=True)
        for m in motifs:
            m["class"] = classify(m["ref_center_s"], m["src_ep"], breaks_by_eid,
                                  durations.get(m["src_ep"], 0.0))
        top1 = motifs[0] if motifs else None

        # ---- recovery vs shipped bank templates ----
        recovery = {}
        if slug in bank:
            for side in ("pre", "post"):
                g = bank[slug][side]
                if g is None:
                    continue
                atype = "start" if side == "pre" else "end"
                # best over ALL motifs, and best restricted to the matching edge
                cb, cr, ct = recover_side(motifs, eps, g)
                sb, sr, st = recover_side(motifs, eps, g, anchor_type_filter=atype)
                recovery[side] = {
                    "consensus_top1": ct, "consensus_best": cb, "consensus_rank": cr,
                    "sideedge_best": sb, "sideedge_rank": sr,
                    "recovered_top1": ct >= RECOVERY_NCC,
                    "recovered_topk": cb >= RECOVERY_NCC,
                }

        # ---- naive/seeded A/B (same harness classifier) ----
        ns = seeded_naive_top1(slug, pool, seed_map, breaks_by_eid, durations)

        # ---- K / tau / offset-tol / silence-gate sensitivity ----
        sens = {}
        variants = [
            ("primary(tau0.75,tol10,gateON)", dict(tau=0.75, tol=10, gate=True, outro=False)),
            ("tau0.70", dict(tau=0.70, tol=10, gate=True, outro=False)),
            ("tau0.80", dict(tau=0.80, tol=10, gate=True, outro=False)),
            ("tol5", dict(tau=0.75, tol=5, gate=True, outro=False)),
            ("tol20", dict(tau=0.75, tol=20, gate=True, outro=False)),
            ("gateOFF", dict(tau=0.75, tol=10, gate=False, outro=False)),
            ("gate+outro", dict(tau=0.75, tol=10, gate=True, outro=True)),
        ]
        for vname, vp in variants:
            if vname.startswith("primary"):
                vm = motifs           # reuse the already-computed primary mine
            else:
                vm = mine_consensus(eps, vp["tau"], vp["tol"], radius_frames,
                                    DELTA_HOP, silence_gate=vp["gate"],
                                    outro_gate=vp["outro"])
            row = {"n_motifs": len(vm)}
            if vm:
                m0 = vm[0]
                cl = classify(m0["ref_center_s"], m0["src_ep"], breaks_by_eid,
                              durations.get(m0["src_ep"], 0.0))
                row.update({"top_center_s": m0["ref_center_s"], "top_ep": m0["src_ep"][-10:],
                            "top_type": m0["anchor_type"], "top_vote": m0["vote"],
                            "top_med_ncc": m0["median_ncc"], "top_label": cl["label"],
                            "top_on_ad": cl["on_ad"]})
                # per-K winner: highest motif meeting vote>=K
                for K in (2, max(2, n - 1), n):
                    wm = next((x for x in vm if x["vote"] >= K), None)
                    if wm:
                        clk = classify(wm["ref_center_s"], wm["src_ep"],
                                       breaks_by_eid, durations.get(wm["src_ep"], 0.0))
                        row[f"K{K}"] = {"center_s": wm["ref_center_s"],
                                       "vote": wm["vote"], "label": clk["label"],
                                       "on_ad": clk["on_ad"]}
                    else:
                        row[f"K{K}"] = None
            sens[vname] = row

        report["shows"][slug] = {
            "role": role, "candidate_eps": [e.stem for e in eps],
            "n_candidate_eps": n,
            "anchors_per_ep": {e.stem[-10:]: len(e.anchors) for e in eps},
            "durations_s": durations,
            "gold_breaks": {e.stem[-10:]:
                            [[round(b["start_seconds"], 1), round(b["end_seconds"], 1)]
                             for b in breaks_by_eid.get(e.stem, [])] for e in eps},
            "top1": top1, "motifs": motifs, "recovery": recovery,
            "naive_seeded": ns, "sensitivity": sens,
        }

        # ---- console ----
        if top1:
            c = top1["class"]
            print(f"  CONSENSUS top1 @{top1['ref_center_s']:8.1f}s "
                  f"[{top1['src_ep'][-10:]} {top1['anchor_type']} δ={top1['delta_s']:+.1f}] "
                  f"vote={top1['vote']}/{n} med={top1['median_ncc']:.3f} "
                  f"{top1['acoustic']} -> [{c['label']}] {c['nearest']}")
            print(f"    spark {top1['spark']}")
        if ns:
            for who in ("naive", "seeded"):
                t = ns.get(who)
                if t:
                    print(f"  {who:8} top1 @{t['center_s']:8.1f}s -> [{t['label']}] "
                          f"{t['nearest']}")
        for side, r in recovery.items():
            print(f"  recovery[{side}]: consensus top1={r['consensus_top1']:.3f} "
                  f"best={r['consensus_best']:.3f}@rank{r['consensus_rank']} "
                  f"({'REC@top1' if r['recovered_top1'] else ('rec@topk' if r['recovered_topk'] else 'MISS')})")
        for m in motifs[1:5]:
            c = m["class"]
            print(f"    #{motifs.index(m)} @{m['ref_center_s']:8.1f} "
                  f"[{m['src_ep'][-10:]} {m['anchor_type']}] vote={m['vote']} "
                  f"med={m['median_ncc']:.3f} -> [{c['label']}]")

    # ---- aggregate top-1 on-boundary / false rates ----
    agg = {w: {"on": 0, "false": 0, "intro": 0, "outro": 0, "interior": 0,
               "fp": 0, "n": 0} for w in ("naive", "seeded", "consensus")}
    for slug, s in report["shows"].items():
        # naive/seeded
        ns = s.get("naive_seeded") or {}
        for who in ("naive", "seeded"):
            t = ns.get(who)
            if not t:
                continue
            agg[who]["n"] += 1
            agg[who]["on"] += int(t["on_ad"])
            agg[who]["false"] += int(not t["on_ad"])
            if not t["on_ad"]:
                agg[who][{"intro": "intro", "outro": "outro",
                          "interior": "interior", "FP/far": "fp"}[t["label"]]] += 1
        # consensus
        t = s.get("top1")
        if t:
            c = t["class"]
            agg["consensus"]["n"] += 1
            agg["consensus"]["on"] += int(c["on_ad"])
            agg["consensus"]["false"] += int(not c["on_ad"])
            if not c["on_ad"]:
                agg["consensus"][{"intro": "intro", "outro": "outro",
                                  "interior": "interior", "FP/far": "fp"}[c["label"]]] += 1
    report["aggregate_top1"] = agg
    print("\n=== AGGREGATE TOP-1 (all shows, same classifier) ===")
    for who in ("naive", "seeded", "consensus"):
        a = agg[who]
        print(f"  {who:9}: on-boundary {a['on']}/{a['n']}  false {a['false']}/{a['n']} "
              f"(intro {a['intro']}, outro {a['outro']}, interior {a['interior']}, "
              f"FP/far {a['fp']})")

    report["params"]["wall_s"] = round(time.time() - t0, 1)
    OUT_JSON.write_text(json.dumps(report, indent=1))
    print(f"\nwall={report['params']['wall_s']}s   full results -> {OUT_JSON}")
    return 0


if __name__ == "__main__":
    sys.exit(run())
