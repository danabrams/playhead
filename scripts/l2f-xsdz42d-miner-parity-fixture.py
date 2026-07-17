#!/usr/bin/env python3
"""Generate the Swift port-parity fixture for StingerCandidateMiner (xsdz.42d).

This runs the ACTUAL xsdz.42d prototype functions
(`C.mine_consensus`, `D.annotate_multiplicity`, `D.multiplicity_rank`) on a
small, fully-synthetic, deterministic multi-episode corpus and dumps both the
INPUTS (per-episode 50 Hz envelopes + pipeline ad-candidate spans) and the
EXPECTED ranked candidates to a JSON the Swift `StingerCandidateMinerTests`
parity test loads. Swift must reproduce this ranking on the same inputs.

Fidelity choices so the reference matches the Swift port's numeric path:
  * Envelopes are float32-VALUED (widened to float64 for the math) so the JSON
    round-trips losslessly into Swift `[Float]` and both sides start from the
    identical bytes.
  * The reference computes NCC via the DIRECT `ncc_curve` (the boundary
    prototype's form the shipped `StingerRefiner` ports), NOT the prototype's
    FFT `TargetScan` — the two are documented bit-identical, and using direct
    NCC removes FFT round-off so the reference and Swift agree to
    summation-order precision. We assert the direct and FFT rankings match, so
    the substitution is proven faithful, not assumed.
  * No ffmpeg, no audio, no gold. Episodes are built directly (object.__new__)
    mirroring `Episode.__init__` but with the direct-NCC scan.

Determinism: fixed rng seed (PCG64, numpy-version-stable), no wall-clock.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib
import sys

import numpy as np

ROOT = pathlib.Path("/Users/dabrams/playhead")
WORKTREE = pathlib.Path(__file__).resolve().parents[1]
OUT = (WORKTREE / "PlayheadTests/Services/AdDetection/"
       "StingerCandidateMinerParityFixture.json")


def _load(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


D = _load("xsdz42d", ROOT / "scripts" / "l2f-xsdz42d-multiplicity-stinger-mine.py")
C = D.C                     # xsdz.42c consensus module
MINE = C.MINE               # xsdz.42 envelope/NCC module
ncc_curve = MINE.PROTO.ncc_curve   # direct NCC (the form StingerRefiner ports)

ENVELOPE_HZ = C.ENVELOPE_HZ        # 50
L = C.L                            # 350
SEARCH_R_S = C.SEARCH_R_S          # 45.0
DELTA_HOP = C.DELTA_HOP            # 50
TAU = C.TAU                        # 0.75
OFFSET_TOL = C.OFFSET_TOL          # 10.0
WITHIN_TAU = D.WITHIN_TAU          # 0.75
MULT_MIN = D.MULT_MIN              # 2
QUORUM = D.QUORUM                  # "any"
NMS_RADIUS = C.NMS_RADIUS          # 350

# The prototype PRIMARY search radius is 45 s, sized for real ad breaks that
# sit >90 s apart. This synthetic corpus keeps breaks 16 s apart (compact, so
# the CI test is small and light enough to run safely in parallel), so we run
# at a 12 s radius — below the break separation — exactly as the integrated
# Swift semantic tests do. The algorithm is radius-parametric; port parity holds
# at any radius. A radius wider than the break separation would let each
# anchor's window straddle two breaks, collapsing the mid-roll multiplicity.
FIXTURE_SEARCH_R_S = 12.0
RADIUS_FRAMES = int(round(FIXTURE_SEARCH_R_S * ENVELOPE_HZ))
HALF = L // 2


class DirectScan:
    """TargetScan-shaped local scan that computes the NCC curve with the DIRECT
    `ncc_curve` (float64) instead of the FFT. `.curve` mirrors TargetScan.curve
    (None on length mismatch / zero-variance template)."""

    def __init__(self, target, template_len: int = L):
        self.target = np.asarray(target, dtype=np.float64)
        self.M = self.target.size
        self.L = template_len

    def curve(self, template):
        t = np.asarray(template, dtype=np.float64)
        if t.size != self.L or self.M < self.L:
            return None
        tc = t - t.mean()
        if float(np.linalg.norm(tc)) == 0.0:
            return None
        return ncc_curve(t, self.target)


def make_episode(stem: str, env: np.ndarray, spans, scan_cls):
    """Construct a C.Episode without ffmpeg, mirroring Episode.__init__ but
    letting the caller pick the scan class (DirectScan or the real FFT one)."""
    e = object.__new__(C.Episode)
    e.stem = stem
    e.env = env
    e.anchors = []
    for (a, b) in spans:
        e.anchors.append((int(round(a * ENVELOPE_HZ)), "start"))
        e.anchors.append((int(round(b * ENVELOPE_HZ)), "end"))
    e.ascan = []
    for (anc, atype) in e.anchors:
        a0 = max(0, anc - RADIUS_FRAMES - HALF)
        b0 = min(e.env.size, anc + RADIUS_FRAMES + HALF)
        sub = e.env[a0:b0]
        if sub.size < L:
            continue
        e.ascan.append((atype, a0, anc, scan_cls(sub)))
    return e


def f32(x: np.ndarray) -> np.ndarray:
    """Coerce to float32-representable values, stored as float64 for the math
    (so the JSON dump is lossless and the Swift Float reconstruction is exact)."""
    return x.astype(np.float32).astype(np.float64)


def motif(rng, n: int = L, lo: float = 0.6, hi: float = 3.0) -> np.ndarray:
    """A distinctive, non-silent 7 s pattern (all values >= the 0.5 silence
    threshold, so its aligned window survives the silence gate cleanly)."""
    return f32(rng.uniform(lo, hi, n))


def plant(background: np.ndarray, plantings) -> np.ndarray:
    """Overlay planted motifs on a (non-constant, sub-threshold) background.
    A NON-constant background is load-bearing: a perfectly-flat background
    yields near-zero-variance windows where the NCC denominator hits its
    1e-12 floor and the argmax becomes numerically unstable (FFT and direct
    then disagree). Real 50 Hz envelopes always carry some variance, so the
    background here is low-amplitude per-episode noise — every window has a
    well-defined, implementation-independent NCC peak."""
    env = background.copy()
    for (frame, pattern) in plantings:
        env[frame:frame + pattern.size] = pattern
    return f32(env)


def build_corpus():
    """One 3-episode show, engineered so the demotion is demonstrated with a
    ROBUST, FP-insensitive rank difference (an integer VOTE gap, not a fragile
    median-NCC gap):

      * PRE-stinger S (identical copy) fires at BOTH mid-roll breaks in ep1 and
        ep2 only -> S has vote 2 (source + 1 partner) and within-multiplicity 2
        in each voting episode -> PASSES.
      * Outro cue O (identical copy) fires ONCE (the late 'outro' candidate) in
        ALL THREE episodes -> O has vote 3 -> it out-ranks S in CONSENSUS, but
        fires once per episode (multiplicity 1) -> FAILS.

    Consensus top-1 is therefore the once-firing outro O; the multiplicity
    demotion must PROMOTE the mid-roll stinger S above it. All copies are
    identical, so every partner NCC is exactly 1.000 -> no rounding-tie
    ambiguity and the direct / FFT rankings agree exactly."""
    rng = np.random.default_rng(20260716)
    stinger = motif(rng)         # mid-roll stinger S (shared, identical copy)
    outro = motif(rng)           # once-per-episode outro cue O (shared)

    total = 2400                 # 48 s episodes; breaks 16 s apart (> 12 s radius)
    # candidate spans (seconds): two mid-roll breaks + one late outro region
    spans = [(8.0, 15.0), (24.0, 31.0), (40.0, 47.0)]
    episodes = []
    for k in range(3):
        stem = f"parity-show-2026-01-0{k + 1}-ep"
        # Per-episode low-amplitude noise floor (all < 0.5 silence threshold,
        # so bg-heavy windows are still silence-gated, but non-constant).
        background = f32(rng.uniform(0.02, 0.30, total))
        plantings = [(2000, outro)]                # outro (@40s) in every episode
        if k < 2:                                  # stinger only in ep1, ep2
            plantings = [(400, stinger), (1200, stinger)] + plantings  # @8s, @24s
        env = plant(background, plantings)
        episodes.append((stem, env, spans))
    return episodes


def run() -> int:
    episodes = build_corpus()

    # Reference ranking via DIRECT NCC (matches the Swift port's math path).
    eps_direct = [make_episode(s, e, sp, DirectScan) for (s, e, sp) in episodes]
    motifs = C.mine_consensus(eps_direct, TAU, OFFSET_TOL, RADIUS_FRAMES,
                              DELTA_HOP, silence_gate=True)
    by_stem = {e.stem: e for e in eps_direct}
    D.annotate_multiplicity(motifs, eps_direct, by_stem, WITHIN_TAU)
    ranked = D.multiplicity_rank(motifs, MULT_MIN, QUORUM)

    # Cross-check: the prototype's real FFT path must produce the SAME ranking
    # (proves the direct-NCC substitution is faithful, not just convenient).
    eps_fft = [make_episode(s, e, sp, C.TargetScan) for (s, e, sp) in episodes]
    motifs_fft = C.mine_consensus(eps_fft, TAU, OFFSET_TOL, RADIUS_FRAMES,
                                  DELTA_HOP, silence_gate=True)
    D.annotate_multiplicity(motifs_fft, eps_fft, {e.stem: e for e in eps_fft}, WITHIN_TAU)
    ranked_fft = D.multiplicity_rank(motifs_fft, MULT_MIN, QUORUM)
    direct_centers = [round(m["ref_center_s"], 1) for m in ranked]
    fft_centers = [round(m["ref_center_s"], 1) for m in ranked_fft]
    assert direct_centers == fft_centers, (
        f"FFT vs direct ranking diverged: {fft_centers} != {direct_centers}")
    print(f"[ok] direct == FFT ranking on centers: {direct_centers}", file=sys.stderr)

    def passes(m):
        mult = m.get("within_mult", {})
        n_multi = sum(1 for v in mult.values() if v >= MULT_MIN)
        return n_multi >= 1  # QUORUM == "any"

    expected = []
    for m in ranked:
        src = eps_direct[m["src_idx"]]
        tmpl = src.env[m["ref_start_frame"]:m["ref_start_frame"] + L]
        voting = [m["src_ep"]] + [p[0] for p in m.get("partners", [])]
        seen = set()
        voting = [v for v in voting if not (v in seen or seen.add(v))]
        expected.append({
            "side": "pre" if m["anchor_type"] == "start" else "post",
            "sourceEpisodeId": m["src_ep"],
            "anchorSeconds": round(m["anchor_s"], 1),
            "centerSeconds": round(m["ref_center_s"], 1),
            "edgeOffsetSeconds": round(m["delta_s"], 1),
            "vote": m["vote"],
            "medianPartnerNCC": round(m["median_ncc"], 3),
            "partners": [{"episodeId": p[0], "peakNCC": round(float(p[1]), 3)}
                         for p in m.get("partners", [])],
            "withinMultiplicity": {k: int(v) for k, v in m.get("within_mult", {}).items()},
            "votingEpisodeCount": len(voting),
            "multiplicityPasses": bool(passes(m)),
            "silentFraction": round(float(m["acoustic"]["silentfrac"]), 2),
            "refStartFrame": int(m["ref_start_frame"]),
            "template": [float(np.float32(v)) for v in tmpl],
        })

    fixture = {
        "_comment": ("Generated by scripts/l2f-xsdz42d-miner-parity-fixture.py — "
                     "DO NOT hand-edit. Port-parity golden for "
                     "StingerCandidateMiner (playhead-xsdz.42d). Envelopes are "
                     "float32-valued; expected ranking produced by the real "
                     "prototype functions (direct == FFT verified)."),
        "config": {
            "envelopeHz": ENVELOPE_HZ, "templateFrames": L,
            "searchRadiusSeconds": FIXTURE_SEARCH_R_S, "deltaHopFrames": DELTA_HOP,
            "consensusTau": TAU, "offsetToleranceSeconds": OFFSET_TOL,
            "minEpisodeVote": 2, "topK": C.TOP_K,
            "silenceGateEnabled": True, "silentEnvelopeThreshold": C.SILENT_ENV,
            "silentFractionGate": C.SILENT_GATE, "acousticDedupeNCC": 0.85,
            "nmsRadiusFrames": NMS_RADIUS, "withinTau": WITHIN_TAU,
            "multiplicityMin": MULT_MIN, "quorum": QUORUM, "hardGate": False,
        },
        "episodes": [
            {"id": stem,
             "envelope": [float(np.float32(v)) for v in env],
             "adCandidates": [{"startSeconds": a, "endSeconds": b} for (a, b) in sp]}
            for (stem, env, sp) in episodes
        ],
        "expected": expected,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(fixture, indent=1))
    print(f"[ok] wrote {OUT} ({len(expected)} expected candidates)", file=sys.stderr)
    for e in expected:
        print(f"    {e['side']:4} center={e['centerSeconds']:7.1f} vote={e['vote']} "
              f"med={e['medianPartnerNCC']:.3f} mult={e['withinMultiplicity']} "
              f"pass={e['multiplicityPasses']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(run())
