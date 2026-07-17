#!/usr/bin/env python3
"""Unsupervised cross-episode stinger miner (playhead-xsdz.42 research spike).

QUESTION
--------
The shipped StingerBank (Playhead/Resources/StingerBank.json) learns each show's
acoustic "stinger" (the recurring music / pre-recorded cue that brackets ad
breaks) from HAND-AUDITED gold break edges. That only scales to shows we
personally audit. This spike tests the generalization hypothesis:

    A show's stinger can be recovered from a handful of its episodes by
    CROSS-EPISODE ACOUSTIC SELF-SIMILARITY, with NO gold edge labels --
    because the same stinger recurs at ad boundaries in every episode, whereas
    per-episode-unique audio (host speech, guests, content) does not recur.

If true, each user's shows could self-learn a stinger bank on-device, zero
manual audit.

METHOD (fully unsupervised -- NO gold edges consumed anywhere in mining)
------------------------------------------------------------------------
For a show with N episodes:
  1. Decode every episode to a 50 Hz log1p-RMS envelope (same pipeline as the
     shipped l2f prototype / runtime: ffmpeg -> mono f32le -> 20 ms RMS frames).
  2. Pick a deterministic reference episode (sorted-first). Slide a 350-frame
     (7 s, = shipped template length) window across it with a fixed hop.
  3. For each candidate window, measure its PEAK normalized cross-correlation
     (NCC) against the FULL envelope of every OTHER episode. A window that is
     the stinger matches strongly in every other episode; a window of unique
     host speech does not.
  4. Rank candidates by cross-episode recurrence (median peak NCC across the
     other episodes, tie-broken by min). Non-max-suppress overlapping windows
     to yield distinct recurring MOTIFS.

The top motif(s) = the mined stinger candidate(s). Everything above is
label-free: candidates come from raw audio, recurrence is raw acoustic NCC.

VALIDATION (this is where gold-derived artifacts enter, for scoring ONLY)
------------------------------------------------------------------------
  * Recovery: for each banked show, peak NCC(mined motif, shipped gold-derived
    template) per side (pre/post). >=0.70 == recovered.
  * False discovery: run on THEMOVE (improvised live reads, no consistent
    acoustic stinger per Dan). The miner should surface nothing strongly
    stinger-like at an ad boundary.
  * Held-out: shows with multiple episodes we never banked -- does a plausible
    recurring motif surface? Interpreted (never guided) against gold breaks.
  * Episode-count sensitivity: 2 vs 3 vs all episodes.
  * Speech-vs-stinger: timestamp the top motif and characterize it (position
    vs gold break edges + within-episode multiplicity).

Determinism: reference = sorted-first episode; fixed hop; no randomness/seed.
Envelopes cached to .npy so numbers are reproducible.

On-device feasibility: envelope NCC only (no ML training, no cloud). The whole
per-show mine is a few FFT-accelerated correlation scans -- feasible on device.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import pathlib
import re
import statistics
import subprocess
import sys

import numpy as np

ROOT = pathlib.Path("/Users/dabrams/playhead")
SCRATCH = pathlib.Path(
    "/private/tmp/claude-501/-Users-dabrams-playhead/"
    "6ce9b37b-c84d-4ce3-a585-8e33b921ee5b/scratchpad/xsdz42-envcache"
)
AUDIO_DIR = ROOT / "TestFixtures/Corpus/Audio"
BANK_PATH = ROOT / "Playhead/Resources/StingerBank.json"
GOLD_PATH = (
    ROOT / "TestFixtures/Corpus/Evaluations/"
    "earaudit-oracle-gold-"
    "836b81885f6d279a84c1ef0dee83302e7df6ed28f0d20ec2db621b518f1ef220.json"
)

# --- envelope / template geometry (match shipped bank: 16 kHz PCM, 50 Hz) ---
SAMPLE_RATE = 16000          # bank pcmSampleRate -> NCC parity with gold templates
ENVELOPE_HZ = 50
HOP = SAMPLE_RATE // ENVELOPE_HZ
TEMPLATE_FRAMES = 350        # 7 s, = shipped template length
CAND_HOP = 25                # 0.5 s candidate stride across the reference
NMS_RADIUS = TEMPLATE_FRAMES  # reported motifs are >= 7 s apart (non-overlapping)
GOLD_SLOP = 100              # frames of alignment freedom when scoring vs gold
RECOVERY_NCC = 0.70          # mined-vs-gold NCC that counts as "recovered"

# recurrence-threshold grid reported for context (ranking itself uses median NCC)
TAU_GRID = (0.60, 0.70, 0.80)
WITHIN_TAU = 0.70            # within-episode multiplicity threshold


# ---------------------------------------------------------------------------
# Reuse the shipped prototype's envelope + NCC helpers verbatim (importlib, so
# we do not fork the pipeline).  ncc_align/ncc_curve come straight from it.
# ---------------------------------------------------------------------------
def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PROTO = _load("l2f_boundary_stinger_prototype", "l2f-boundary-stinger-prototype.py")
ncc_align = PROTO.ncc_align  # (offset, peak) of template's best alignment in target


def full_envelope(fn: str) -> np.ndarray:
    """50 Hz log1p-RMS envelope of a whole episode (cached to .npy)."""
    SCRATCH.mkdir(parents=True, exist_ok=True)
    cache = SCRATCH / f"{fn}.{SAMPLE_RATE}.npy"
    if cache.exists():
        return np.load(cache)
    result = subprocess.run(
        ["ffmpeg", "-v", "error", "-i", str(AUDIO_DIR / fn),
         "-ac", "1", "-ar", str(SAMPLE_RATE), "-f", "f32le", "-"],
        capture_output=True, check=True,
    )
    pcm = np.frombuffer(result.stdout, dtype=np.float32)
    n = pcm.size - pcm.size % HOP
    frames = pcm[:n].reshape(-1, HOP).astype(np.float64)
    env = np.log1p(np.sqrt((frames ** 2).mean(axis=1)) * 100.0)
    np.save(cache, env)
    return env


class TargetScan:
    """Precomputes the per-target quantities that a fixed target reuses across
    every candidate template: the target FFT and the per-window local variance.
    Verified bit-identical to scipy's ncc_curve (see benchmark in the spike
    notes).  Lets us scan thousands of candidates against one episode cheaply.
    """

    def __init__(self, target: np.ndarray, template_len: int = TEMPLATE_FRAMES):
        self.M = target.size
        self.L = template_len
        self.nfft = 1 << int(np.ceil(np.log2(self.M + self.L)))
        self.Xf = np.fft.rfft(target, self.nfft)
        c1 = np.concatenate([[0.0], np.cumsum(target)])
        c2 = np.concatenate([[0.0], np.cumsum(target * target)])
        L = self.L
        sums = c1[L:self.M + 1] - c1[0:self.M - L + 1]
        sq = c2[L:self.M + 1] - c2[0:self.M - L + 1]
        self.lvar = np.maximum(sq - sums * sums / L, 1e-12)

    def curve(self, template: np.ndarray) -> np.ndarray | None:
        tc = template - template.mean()
        tn = float(np.linalg.norm(tc))
        if tn == 0 or template.size != self.L or self.M < self.L:
            return None
        Tf = np.fft.rfft(tc, self.nfft)
        raw = np.fft.irfft(self.Xf * np.conj(Tf), self.nfft)[: self.M - self.L + 1]
        return raw / (np.sqrt(self.lvar) * tn)

    def peak(self, template: np.ndarray) -> float:
        cur = self.curve(template)
        return 0.0 if cur is None else float(cur.max())

    def qualifying_maxima(self, template: np.ndarray, gate: float) -> int:
        """# of local maxima of the NCC curve at/above `gate` (within-episode
        repetition count of the motif inside this target)."""
        cur = self.curve(template)
        if cur is None:
            return 0
        count = 0
        for i in range(cur.size):
            left = cur[i - 1] if i > 0 else -np.inf
            right = cur[i + 1] if i + 1 < cur.size else -np.inf
            if cur[i] >= gate and cur[i] >= left and cur[i] > right:
                count += 1
        return count


# ---------------------------------------------------------------------------
# The miner
# ---------------------------------------------------------------------------
def mine_show(episodes: list[str], top_k: int = 6, verbose: bool = False) -> dict:
    """Mine recurring motifs from a show's episodes. NO gold used.

    Returns a dict with the reference episode, its envelope length, and a ranked
    list of motifs. Each motif: reference center frame/time, per-target peak NCC
    list, median/min cross-episode NCC, and within-reference multiplicity.
    """
    episodes = sorted(episodes)              # deterministic reference selection
    reference = episodes[0]
    others = episodes[1:]
    ref_env = full_envelope(reference)
    ref_scan = TargetScan(ref_env)
    other_scans = {ep: TargetScan(full_envelope(ep)) for ep in others}

    L = TEMPLATE_FRAMES
    n_cand = (ref_env.size - L) // CAND_HOP + 1
    # cross-episode peak NCC for every candidate window vs every other episode
    starts = []
    per_target = []  # list of dict[ep]->peak
    for c in range(n_cand):
        s = c * CAND_HOP
        tmpl = ref_env[s:s + L]
        peaks = {ep: other_scans[ep].peak(tmpl) for ep in others}
        starts.append(s)
        per_target.append(peaks)

    starts = np.array(starts)
    medians = np.array([statistics.median(p.values()) for p in per_target])
    mins = np.array([min(p.values()) for p in per_target])

    # rank by (median cross-episode NCC, then min) and non-max-suppress
    order = sorted(range(len(starts)), key=lambda i: (medians[i], mins[i]), reverse=True)
    chosen: list[int] = []
    for i in order:
        if all(abs(starts[i] - starts[j]) >= NMS_RADIUS for j in chosen):
            chosen.append(i)
        if len(chosen) >= top_k:
            break

    motifs = []
    for i in chosen:
        s = int(starts[i])
        tmpl = ref_env[s:s + L]
        peaks = per_target[i]
        # within-reference multiplicity: how many times this motif recurs inside
        # its OWN episode (ad stinger recurs at each break; an intro plays once)
        multiplicity = ref_scan.qualifying_maxima(tmpl, WITHIN_TAU)
        motifs.append({
            "ref_start_frame": s,
            "ref_center_s": round((s + L / 2) / ENVELOPE_HZ, 1),
            "median_ncc": round(float(medians[i]), 3),
            "min_ncc": round(float(mins[i]), 3),
            "max_ncc": round(float(max(peaks.values())), 3),
            "per_target": {ep: round(v, 3) for ep, v in peaks.items()},
            "recur_at": {f"{t:.2f}": int(sum(v >= t for v in peaks.values()))
                         for t in TAU_GRID},
            "within_ref_multiplicity": multiplicity,
        })
        if verbose:
            print(f"    motif @ {motifs[-1]['ref_center_s']:7.1f}s  "
                  f"median={motifs[-1]['median_ncc']:.3f} "
                  f"min={motifs[-1]['min_ncc']:.3f} "
                  f"mult={multiplicity}  n_ep={len(episodes)}")
    return {
        "reference": reference,
        "n_episodes": len(episodes),
        "episodes": episodes,
        "ref_env_len": ref_env.size,
        "motifs": motifs,
    }


# ---------------------------------------------------------------------------
# Validation helpers (gold-derived artifacts used only here, for scoring)
# ---------------------------------------------------------------------------
def load_bank() -> dict[str, dict]:
    """slug -> {'pre': np.array|None, 'post': np.array|None} from shipped bank."""
    bank = json.loads(BANK_PATH.read_text())
    out = {}
    for entry in bank["shows"]:
        slug = entry["showKeys"][0]
        sides = {}
        for side in ("pre", "post"):
            sides[side] = (np.array(entry[side]["template"], dtype=np.float64)
                           if side in entry else None)
        out[slug] = sides
    return out


def gold_by_show() -> dict[str, list[dict]]:
    gold = json.loads(GOLD_PATH.read_text())
    out: dict[str, list[dict]] = {}
    for a in gold["assets"]:
        m = re.match(r"^(.+?)-\d{4}-\d{2}-\d{2}-", a["episode_id"])
        slug = m.group(1) if m else a["episode_id"]
        out.setdefault(slug, []).append(a)
    return out


def recovery_ncc(motif: dict, reference_env: np.ndarray, gold_template: np.ndarray) -> float:
    """Peak NCC between a mined motif and a gold template, with +-GOLD_SLOP
    frames of alignment freedom (extract a longer window around the mined center
    and slide the 350-frame gold template inside it)."""
    s = motif["ref_start_frame"]
    lo = max(0, s - GOLD_SLOP)
    hi = min(reference_env.size, s + TEMPLATE_FRAMES + GOLD_SLOP)
    window = reference_env[lo:hi]
    if window.size < gold_template.size:
        return 0.0
    _, peak = ncc_align(gold_template, window)
    return float(peak)


def annotate_position(center_s: float, assets: list[dict], reference: str) -> str:
    """Interpret (post-hoc) what a mined motif's timestamp lands on, using gold
    break edges of the reference episode. Never guides mining."""
    asset = next((a for a in assets if a["episode_id"] == reference), None)
    if asset is None:
        return "no-gold-for-ref"
    best = None
    for b in asset["full_breaks"]:
        for edge_name, edge in (("start", b["start_seconds"]), ("end", b["end_seconds"])):
            d = center_s - edge
            if best is None or abs(d) < abs(best[2]):
                best = (edge_name, edge, d)
    if best is None:
        return "no-breaks" + (" intro<60s" if center_s < 60 else "")
    edge_name, edge, d = best
    tag = f"nearest gold {edge_name}={edge:.1f}s (Δ={d:+.1f}s)"
    if abs(d) <= 8.0:
        tag = "AD-BOUNDARY: " + tag
    elif center_s < 60:
        tag = "INTRO(<60s): " + tag
    else:
        tag = "interior: " + tag
    return tag


# ---------------------------------------------------------------------------
# Experiment driver
# ---------------------------------------------------------------------------
SHOWS = {
    # banked, multi-episode -> recovery test
    "morbid": "recovery",
    "the-nikki-glaser-podcast": "recovery",
    "conan": "recovery",
    "smartless": "recovery",
    # false-discovery control (improvised reads, no consistent stinger)
    "themove": "false-discovery",
    # held-out (multi-episode, never banked) -> generalization
    "doac": "held-out",
    "unexplained": "held-out",
    "why-is-this-happening-the-chris-hayes-po": "held-out",
    "techcrunch-daily-crunch": "held-out",
}
# bank slug alias (bank uses "the-nikki-glaser-podcast" as showKeys[0]? check)
BANK_ALIAS = {"the-nikki-glaser-podcast": "the-nikki-glaser-podcast"}


def episodes_for(slug: str) -> list[str]:
    files = sorted(p.name for p in AUDIO_DIR.glob("*.mp3"))
    out = []
    for f in files:
        m = re.match(r"^(.+?)-\d{4}-\d{2}-\d{2}-", f[:-4])
        if m and m.group(1) == slug:
            out.append(f)
    return sorted(out)


def run(args) -> int:
    bank = load_bank()
    golds = gold_by_show()
    report: dict = {"shows": {}, "params": {
        "sample_rate": SAMPLE_RATE, "envelope_hz": ENVELOPE_HZ,
        "template_frames": TEMPLATE_FRAMES, "cand_hop_frames": CAND_HOP,
        "reference": "sorted-first episode", "recovery_ncc": RECOVERY_NCC,
    }}

    for slug, role in SHOWS.items():
        eps = episodes_for(slug)
        if len(eps) < 2:
            print(f"[skip] {slug}: only {len(eps)} episode(s)")
            continue
        print(f"\n=== {slug}  ({role}, {len(eps)} episodes) ===")
        mined = mine_show(eps, verbose=True)
        ref_env = full_envelope(mined["reference"])
        assets = golds.get(slug, [])

        # annotate each motif's position (interpretation only)
        for mtf in mined["motifs"]:
            mtf["position"] = annotate_position(mtf["ref_center_s"], assets, mined["reference"])

        # recovery vs shipped gold-derived templates
        recovery = {}
        bank_slug = BANK_ALIAS.get(slug, slug)
        if bank_slug in bank:
            for side in ("pre", "post"):
                g = bank[bank_slug][side]
                if g is None:
                    continue
                best = max(
                    ((recovery_ncc(mtf, ref_env, g), r) for r, mtf in enumerate(mined["motifs"])),
                    key=lambda x: x[0],
                )
                recovery[side] = {"best_ncc": round(best[0], 3),
                                  "at_motif_rank": best[1],
                                  "recovered": best[0] >= RECOVERY_NCC}
        mined["recovery"] = recovery
        mined["role"] = role

        # print summary
        for side, rec in recovery.items():
            flag = "RECOVERED" if rec["recovered"] else "miss"
            print(f"    recovery[{side}]: NCC={rec['best_ncc']:.3f} "
                  f"(motif#{rec['at_motif_rank']}) -> {flag}")
        top = mined["motifs"][0]
        print(f"    TOP motif: {top['ref_center_s']}s  median={top['median_ncc']} "
              f"mult={top['within_ref_multiplicity']}  [{top['position']}]")
        report["shows"][slug] = mined

    # episode-count sensitivity on the banked recovery shows
    print("\n=== episode-count sensitivity ===")
    sens: dict = {}
    for slug in ("morbid", "the-nikki-glaser-podcast", "conan", "smartless"):
        eps = episodes_for(slug)
        bank_slug = BANK_ALIAS.get(slug, slug)
        sens[slug] = {}
        for k in (2, 3, len(eps)):
            if k > len(eps):
                continue
            mined = mine_show(eps[:k], top_k=6)
            ref_env = full_envelope(mined["reference"])
            row = {}
            for side in ("pre", "post"):
                g = bank.get(bank_slug, {}).get(side)
                if g is None:
                    continue
                best = max(recovery_ncc(mtf, ref_env, g) for mtf in mined["motifs"])
                row[side] = round(best, 3)
            sens[slug][k] = row
            print(f"  {slug:28} k={k}: " +
                  ", ".join(f"{s}={v:.3f}" for s, v in row.items()))
    report["episode_count_sensitivity"] = sens

    out = SCRATCH.parent / "xsdz42-mining-results.json"
    out.write_text(json.dumps(report, indent=1))
    print(f"\nfull results -> {out}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.parse_args()
    sys.exit(run(ap.parse_args()))
