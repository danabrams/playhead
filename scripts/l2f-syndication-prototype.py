#!/usr/bin/env python3
"""
l2f-syndication-prototype.py — proof-of-concept cross-show duplicate-segment
detector for ad-syndication evidence (xsdz.13 blindspot).

Hypothesis: the same ad creative gets re-used across many shows. If we
fingerprint every episode and find a 30-second window that is near-identical
in N distinct shows, that window is much more likely to be an ad than a
piece of host-written content.

Pipeline (Python 3, stdlib only):

  1. For each manifest episode, run `fpcalc -raw -length 7200 <mp3>` and
     cache the resulting integer-frame fingerprints under
     `TestFixtures/Corpus/Snapshots/fingerprints/<eid>.json`.
  2. Slide a 30-second window (~250 frames) with a 5-second hop
     (~41 frames) across each episode.
  3. SimHash each window to 64 bits using a textbook construction: each
     of the 32 chromaprint input-bit positions has a deterministic random
     ±1 weight vector of length 64; the window hash is sign-of-sum across
     all frames and bits. Per-byte lookup tables make this Python-fast.
  4. Index every window's SimHash into 4 × 16-bit LSH bands. Two windows
     that share a band value are candidate near-duplicates.
  5. Verify candidates by full 64-bit Hamming distance:
       ≤ 6 bits → tight match
       ≤ 12 bits → loose match
  6. For each window, count `distinct_shows`: how many distinct shows
     (per manifest.json) contain a near-duplicate of this window.

Output:
  * `analysis/syndication-prototype-2026-06-01.md` — top windows, histogram,
    audit cross-reference, honest interpretation.

Self-test mode (`--self-test`):
  * Synthesizes three fingerprint streams, plants a shared 30s segment in
    two of them, and asserts the detector finds the planted match with
    `distinct_shows == 2`. Exits non-zero on logic failure.

Pure exploration. Do not wire into production.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import random
import subprocess
import sys
import time
from collections import defaultdict
from typing import Iterable

from l2f_canonical_manifest import load_canonical_annotations

ROOT = pathlib.Path(__file__).resolve().parents[1]
AUDIO_DIR = ROOT / "TestFixtures/Corpus/Audio"
MANIFEST = ROOT / "TestFixtures/Corpus/Snapshots/manifest.json"
ANN_DIR = ROOT / "TestFixtures/Corpus/Annotations"
FP_CACHE_DIR = ROOT / "TestFixtures/Corpus/Snapshots/fingerprints"

# Tunables — chosen for a 41-episode corpus, not yet production-tuned.
FPCALC_MAX_SECONDS = 7200          # cap analysis at 2h to bound runtime/memory
APPROX_FPS = 8.30                  # chromaprint default frame rate (~120ms/frame)
WINDOW_SECONDS = 30.0
HOP_SECONDS = 5.0
LSH_BANDS = 4
BAND_BITS = 16                     # 4 × 16 = 64-bit hash
HAMMING_TIGHT = 6
HAMMING_LOOSE = 12
TOP_N_REPORT = 20
TARGET_DATE_STR = "2026-06-01"

# ----- subprocess / fpcalc -------------------------------------------------

def _which_fpcalc() -> str | None:
    for candidate in ["/opt/homebrew/bin/fpcalc", "/usr/local/bin/fpcalc", "fpcalc"]:
        try:
            p = subprocess.run(
                [candidate, "-version"], capture_output=True, timeout=5, check=False
            )
            if p.returncode == 0:
                return candidate
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
    return None


def fpcalc_raw(fpcalc_bin: str, mp3: pathlib.Path) -> list[int]:
    """Run fpcalc -raw, return list of 32-bit unsigned frame fingerprints.

    `-length 7200` caps analysis at 7200 seconds. For most episodes that's
    the whole file; for marathon episodes it bounds runtime and memory.
    """
    p = subprocess.run(
        [fpcalc_bin, "-raw", "-length", str(FPCALC_MAX_SECONDS), str(mp3)],
        capture_output=True,
        timeout=600,
        check=False,
    )
    if p.returncode != 0:
        raise RuntimeError(
            f"fpcalc rc={p.returncode}: {p.stderr.decode('utf-8', 'ignore')[:200]}"
        )
    # Plain (non-json) output is two lines:
    #   DURATION=<seconds>
    #   FINGERPRINT=<comma-separated ints>
    fp_line = None
    duration = None
    for line in p.stdout.decode("utf-8", "ignore").splitlines():
        line = line.strip()
        if line.startswith("FINGERPRINT="):
            fp_line = line[len("FINGERPRINT="):]
        elif line.startswith("DURATION="):
            try:
                duration = float(line[len("DURATION="):])
            except ValueError:
                duration = None
    if not fp_line:
        raise RuntimeError("fpcalc returned no FINGERPRINT line")
    try:
        frames = [int(tok) & 0xFFFFFFFF for tok in fp_line.split(",") if tok]
    except ValueError as e:
        raise RuntimeError(f"fpcalc fingerprint parse: {e}")
    if len(frames) < 100:
        raise RuntimeError(f"fpcalc returned only {len(frames)} frames")
    return frames


# ----- cache ---------------------------------------------------------------

def cached_fingerprint(
    fpcalc_bin: str, mp3: pathlib.Path, eid: str
) -> tuple[list[int], bool]:
    """Return (frames, was_cached). Cache invalidates if mp3 newer than cache."""
    FP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = FP_CACHE_DIR / f"{eid}.json"
    if cache_path.exists():
        try:
            if cache_path.stat().st_mtime >= mp3.stat().st_mtime:
                obj = json.loads(cache_path.read_text())
                frames = obj.get("frames")
                if isinstance(frames, list) and len(frames) >= 100:
                    return [int(x) & 0xFFFFFFFF for x in frames], True
        except (OSError, json.JSONDecodeError):
            pass  # fall through to recompute
    frames = fpcalc_raw(fpcalc_bin, mp3)
    tmp = cache_path.with_suffix(".tmp")
    tmp.write_text(json.dumps({
        "episodeId": eid,
        "frames": frames,
        "frameCount": len(frames),
        "fpcalcMaxSeconds": FPCALC_MAX_SECONDS,
    }))
    os.replace(tmp, cache_path)
    return frames, False


# ----- simhash + windowing -------------------------------------------------

# Per-(input_bit_position, output_bit_position) random weight matrix. Each of
# the 32 chromaprint input bits maps to a deterministic 64-bit weight vector;
# voting `+w[b]` when bit b is set, `-w[b]` when clear. This is the textbook
# SimHash construction and is what breaks the symmetry between speech-audio
# windows whose raw bit-distributions happen to be similar.
_SIMHASH_INPUT_BITS = 32
_SIMHASH_OUTPUT_BITS = 64

# Per-byte lookup tables. Each chromaprint frame is 4 bytes (B0..B3). For
# each byte position (0..3) and each byte value (0..255), we precompute a
# length-64 list of votes (sum of ±weights for the 8 bits in that byte).
# Hashing a frame = 4 vector-adds of length 64. For a 30s window of ~250
# frames, that's 1000 vector-adds — ~64,000 scalar ops, which Python can
# absorb at corpus scale.
_SIMHASH_BYTE_TABLES: list[list[list[int]]] = []  # [byte_pos][byte_val] -> [64]


def _build_simhash_weights() -> None:
    rng = random.Random(0xC0FFEE_BEEF)
    # 32 × 64 weights, each ±1
    weights: list[list[int]] = []
    for _ in range(_SIMHASH_INPUT_BITS):
        weights.append([1 if rng.random() >= 0.5 else -1
                        for _ in range(_SIMHASH_OUTPUT_BITS)])

    # Build per-byte tables. Byte position 0 covers input bits 0..7, etc.
    _SIMHASH_BYTE_TABLES.clear()
    for byte_pos in range(4):
        table_for_pos: list[list[int]] = []
        bit_base = byte_pos * 8
        for byte_val in range(256):
            row = [0] * _SIMHASH_OUTPUT_BITS
            for bit_in_byte in range(8):
                ib = bit_base + bit_in_byte
                w_row = weights[ib]
                if (byte_val >> bit_in_byte) & 1:
                    for ob in range(_SIMHASH_OUTPUT_BITS):
                        row[ob] += w_row[ob]
                else:
                    for ob in range(_SIMHASH_OUTPUT_BITS):
                        row[ob] -= w_row[ob]
            table_for_pos.append(row)
        _SIMHASH_BYTE_TABLES.append(table_for_pos)


def simhash64(frames: Iterable[int]) -> int:
    """SimHash 64 bits from a stream of 32-bit chromaprint frames."""
    if not _SIMHASH_BYTE_TABLES:
        _build_simhash_weights()

    t0, t1, t2, t3 = _SIMHASH_BYTE_TABLES
    votes = [0] * _SIMHASH_OUTPUT_BITS

    for f in frames:
        f &= 0xFFFFFFFF
        r0 = t0[f & 0xFF]
        r1 = t1[(f >> 8) & 0xFF]
        r2 = t2[(f >> 16) & 0xFF]
        r3 = t3[(f >> 24) & 0xFF]
        for ob in range(_SIMHASH_OUTPUT_BITS):
            votes[ob] += r0[ob] + r1[ob] + r2[ob] + r3[ob]

    h = 0
    for ob in range(_SIMHASH_OUTPUT_BITS):
        if votes[ob] > 0:
            h |= (1 << ob)
    return h


def popcount64(x: int) -> int:
    # int.bit_count() landed in Python 3.10; we still support 3.9 here.
    return bin(x & 0xFFFFFFFFFFFFFFFF).count("1")


def window_iter(num_frames: int, win_len: int, hop_len: int):
    """Yield (start_frame, end_frame_exclusive). Skips trailing partial window."""
    if win_len <= 0 or hop_len <= 0 or num_frames < win_len:
        return
    start = 0
    while start + win_len <= num_frames:
        yield start, start + win_len
        start += hop_len


def frames_to_seconds(frame_idx: int) -> float:
    return frame_idx / APPROX_FPS


# ----- LSH index -----------------------------------------------------------

class SyndicationIndex:
    """Cross-episode LSH index over per-window 64-bit SimHashes.

    Each window is (episode_id, start_sec, end_sec, simhash64). The hash
    is split into LSH_BANDS bands of BAND_BITS each. A query window
    fetches all other-window IDs that share at least one band value.
    """

    def __init__(self, lsh_bands: int = LSH_BANDS, band_bits: int = BAND_BITS):
        assert lsh_bands * band_bits == 64, "bands * band_bits must equal 64"
        self.lsh_bands = lsh_bands
        self.band_bits = band_bits
        self.band_mask = (1 << band_bits) - 1
        self.windows: list[tuple[str, float, float, int]] = []
        # band_idx -> band_value -> [window_idx]
        self.buckets: list[dict[int, list[int]]] = [
            defaultdict(list) for _ in range(lsh_bands)
        ]

    def add(self, eid: str, start_sec: float, end_sec: float, h: int) -> int:
        wid = len(self.windows)
        self.windows.append((eid, start_sec, end_sec, h))
        for b in range(self.lsh_bands):
            v = (h >> (b * self.band_bits)) & self.band_mask
            self.buckets[b][v].append(wid)
        return wid

    def candidates(self, wid: int) -> set[int]:
        h = self.windows[wid][3]
        cands: set[int] = set()
        for b in range(self.lsh_bands):
            v = (h >> (b * self.band_bits)) & self.band_mask
            for other in self.buckets[b].get(v, ()):
                if other != wid:
                    cands.add(other)
        return cands

    def near_duplicates(self, wid: int, max_hamming: int) -> list[tuple[int, int]]:
        """Return list of (other_wid, hamming_distance) for matches ≤ threshold."""
        h = self.windows[wid][3]
        out: list[tuple[int, int]] = []
        for other in self.candidates(wid):
            d = popcount64(h ^ self.windows[other][3])
            if d <= max_hamming:
                out.append((other, d))
        return out


# ----- audit-priority cross-reference --------------------------------------

def load_audit_priority1_spans() -> dict[str, list[tuple[float, float]]]:
    """Return eid -> list of (start_sec, end_sec) for audit_priority=1 windows."""
    spans: dict[str, list[tuple[float, float]]] = defaultdict(list)
    if not ANN_DIR.exists():
        return spans
    for filename, ann in load_canonical_annotations(ANN_DIR).items():
        ann_path = ANN_DIR / filename
        eid = ann.get("episodeId") or ann_path.stem
        wins = ann.get("adWindows") or ann.get("ad_windows") or []
        for w in wins:
            if w.get("audit_priority") != 1:
                continue
            try:
                s = float(w.get("startSeconds") or w.get("start_seconds") or 0)
                e = float(w.get("endSeconds") or w.get("end_seconds") or 0)
            except (TypeError, ValueError):
                continue
            if e > s:
                spans[eid].append((s, e))
    return spans


def overlaps_any(start: float, end: float, spans: list[tuple[float, float]]) -> bool:
    for s, e in spans:
        if start < e and end > s:
            return True
    return False


# ----- self-test -----------------------------------------------------------

def run_self_test() -> int:
    print("=== --self-test ===")
    rng = random.Random(42)
    # 30s @ 8.3fps ≈ 250 frames
    win_len = int(round(WINDOW_SECONDS * APPROX_FPS))
    hop_len = int(round(HOP_SECONDS * APPROX_FPS))

    def rand_stream(n: int) -> list[int]:
        return [rng.randrange(0, 1 << 32) for _ in range(n)]

    shared_segment = rand_stream(win_len)
    # Three "episodes". B and C share the segment near the middle.
    A_frames = rand_stream(2000)
    B_frames = rand_stream(800) + list(shared_segment) + rand_stream(800)
    C_frames = rand_stream(1200) + list(shared_segment) + rand_stream(500)

    # Pretend each episode comes from a different show.
    streams = {
        ("episode_a", "Show A"): A_frames,
        ("episode_b", "Show B"): B_frames,
        ("episode_c", "Show C"): C_frames,
    }
    show_by_eid = {eid: show for (eid, show), _ in streams.items()}

    idx = SyndicationIndex()
    win_owner: list[str] = []  # parallel array of episode IDs (for assertions)
    for (eid, _show), frames in streams.items():
        for s, e in window_iter(len(frames), win_len, hop_len):
            h = simhash64(frames[s:e])
            idx.add(eid, frames_to_seconds(s), frames_to_seconds(e), h)
            win_owner.append(eid)

    # For every B-window, look for matches in C (and vice versa).
    # We expect at least one with distinct_shows == 2 for the planted region.
    found_planted = False
    max_shows_seen = 1
    for wid, (eid, _s, _e, _h) in enumerate(idx.windows):
        matches = idx.near_duplicates(wid, max_hamming=HAMMING_TIGHT)
        shows = {show_by_eid[eid]}
        for other_wid, _d in matches:
            shows.add(show_by_eid[idx.windows[other_wid][0]])
        if len(shows) > max_shows_seen:
            max_shows_seen = len(shows)
        if len(shows) >= 2 and (
            (eid == "episode_b" and any(
                idx.windows[ow][0] == "episode_c"
                for ow, _ in matches
            )) or (eid == "episode_c" and any(
                idx.windows[ow][0] == "episode_b"
                for ow, _ in matches
            ))
        ):
            found_planted = True

    print(f"  max distinct_shows seen: {max_shows_seen}")
    print(f"  planted B↔C shared segment detected: {found_planted}")

    # Negative check: A should NOT have *cross-show* matches at the tight
    # threshold. Same-episode neighbors (5s-hop overlaps) are NOT spurious —
    # they share most of their input frames by design.
    a_cross_show = 0
    a_same_eid = 0
    for wid, (eid, _s, _e, _h) in enumerate(idx.windows):
        if eid != "episode_a":
            continue
        for ow, _d in idx.near_duplicates(wid, max_hamming=HAMMING_TIGHT):
            o_eid = idx.windows[ow][0]
            if o_eid == eid:
                a_same_eid += 1
            elif show_by_eid[o_eid] != show_by_eid[eid]:
                a_cross_show += 1
    print(f"  A-side intra-episode tight matches: {a_same_eid} "
          f"(expected: small; hop overlap is by design)")
    print(f"  A-side CROSS-SHOW tight matches: {a_cross_show} "
          f"(expected: 0 or close to 0)")

    if not found_planted:
        print("FAIL: planted shared segment was not detected")
        return 1
    if max_shows_seen < 2:
        print("FAIL: no window reached distinct_shows == 2")
        return 1
    print("PASS: self-test ok")
    return 0


# ----- corpus run ----------------------------------------------------------

def build_episode_universe() -> list[dict]:
    """Read manifest.json → list of {episodeId, show, mp3_path}."""
    if not MANIFEST.exists():
        raise RuntimeError(f"manifest not found: {MANIFEST}")
    raw = json.loads(MANIFEST.read_text())
    out = []
    for entry in raw:
        eid = entry.get("episodeId")
        show = entry.get("show") or "?"
        audio_path = entry.get("audioPath")
        if not eid or not audio_path:
            continue
        mp3 = ROOT / audio_path
        if not mp3.exists():
            print(f"  skip (mp3 missing): {eid}", file=sys.stderr)
            continue
        out.append({"eid": eid, "show": show, "mp3": mp3})
    return out


def run_corpus(sample_n: int | None = None) -> int:
    fpcalc_bin = _which_fpcalc()
    if not fpcalc_bin:
        print("fpcalc not found — install chromaprint (brew install chromaprint)",
              file=sys.stderr)
        return 2

    print(f"fpcalc: {fpcalc_bin}")
    episodes = build_episode_universe()
    if sample_n is not None and sample_n < len(episodes):
        random.Random(0).shuffle(episodes)
        episodes = episodes[:sample_n]
        print(f"SAMPLED: using {len(episodes)} episodes (random.seed=0)")
    print(f"episodes: {len(episodes)}")
    shows = sorted({e["show"] for e in episodes})
    print(f"distinct shows: {len(shows)}")

    win_len = int(round(WINDOW_SECONDS * APPROX_FPS))
    hop_len = int(round(HOP_SECONDS * APPROX_FPS))
    print(f"window: {win_len} frames ({WINDOW_SECONDS:.0f}s), "
          f"hop: {hop_len} frames ({HOP_SECONDS:.0f}s)")

    idx = SyndicationIndex()
    show_by_eid: dict[str, str] = {}
    total_windows = 0
    t0 = time.time()
    for i, ep in enumerate(episodes, 1):
        show_by_eid[ep["eid"]] = ep["show"]
        t_ep = time.time()
        try:
            frames, was_cached = cached_fingerprint(fpcalc_bin, ep["mp3"], ep["eid"])
        except Exception as e:
            print(f"  [{i:>2}/{len(episodes)}] FAIL {ep['eid']}: {e}",
                  file=sys.stderr)
            continue
        elapsed = time.time() - t_ep
        nw_before = len(idx.windows)
        for s, e in window_iter(len(frames), win_len, hop_len):
            h = simhash64(frames[s:e])
            idx.add(ep["eid"], frames_to_seconds(s), frames_to_seconds(e), h)
        n_windows = len(idx.windows) - nw_before
        total_windows += n_windows
        tag = "cache" if was_cached else "fpcalc"
        print(f"  [{i:>2}/{len(episodes)}] {ep['eid'][:48]:48} "
              f"frames={len(frames):>6} windows={n_windows:>4} "
              f"{tag} {elapsed:5.1f}s")

    t_index_done = time.time()
    print(f"\nTotal windows: {total_windows}")
    print(f"Index build took {t_index_done - t0:.1f}s")
    print(f"Estimated index memory: ~{(total_windows * 64) / 1e6:.1f} MB "
          f"(rough; LSH buckets add overhead)")

    # ----- pairwise scoring ----------------------------------------------
    audit_spans = load_audit_priority1_spans()

    # For each window, find near-dup matches. Record distinct_shows per
    # window (tight). For each window also record the per-match details.
    results: list[dict] = []
    for wid, (eid, ws, we, _h) in enumerate(idx.windows):
        tight = idx.near_duplicates(wid, max_hamming=HAMMING_TIGHT)
        if not tight:
            continue
        match_shows: set[str] = {show_by_eid[eid]}
        details: list[dict] = []
        for ow, d in tight:
            oeid, os_, oe, _ = idx.windows[ow]
            match_shows.add(show_by_eid[oeid])
            details.append({
                "eid": oeid, "show": show_by_eid[oeid],
                "start": os_, "end": oe, "hamming": d,
            })
        if len(match_shows) < 2:
            continue
        results.append({
            "eid": eid, "show": show_by_eid[eid],
            "start": ws, "end": we,
            "distinct_shows": len(match_shows),
            "match_count": len(details),
            "matches": details,
            "overlaps_audit_p1": overlaps_any(ws, we, audit_spans.get(eid, [])),
        })

    # Sort by distinct_shows desc, then by match_count desc.
    results.sort(key=lambda r: (-r["distinct_shows"], -r["match_count"]))

    # Loose-distribution sweep (informational only).
    loose_total = 0
    loose_cross_show_total = 0
    for wid, (eid, _ws, _we, _h) in enumerate(idx.windows):
        loose = idx.near_duplicates(wid, max_hamming=HAMMING_LOOSE)
        if loose:
            loose_total += 1
            other_shows = {show_by_eid[idx.windows[ow][0]] for ow, _ in loose}
            other_shows.discard(show_by_eid[eid])
            if other_shows:
                loose_cross_show_total += 1

    # ----- diagnostics: hamming distribution + audit-vs-random ----------
    # Sample 5000 random cross-episode pairs; histogram their hamming
    # distance. This is the noise floor — if it's centered at 32 (uniform
    # random 64-bit), then matches at low hamming are signal. If it's
    # centered well below 32, the SimHash is biased on speech audio.
    rng_diag = random.Random(31337)
    pair_dists: list[int] = []
    n_windows = len(idx.windows)
    if n_windows >= 2:
        attempts = 0
        while len(pair_dists) < 5000 and attempts < 50000:
            attempts += 1
            i = rng_diag.randrange(n_windows)
            j = rng_diag.randrange(n_windows)
            if i == j:
                continue
            if idx.windows[i][0] == idx.windows[j][0]:
                continue
            pair_dists.append(
                popcount64(idx.windows[i][3] ^ idx.windows[j][3])
            )
    pair_dist_mean = sum(pair_dists) / len(pair_dists) if pair_dists else 0.0
    pair_dist_hist: dict[int, int] = defaultdict(int)
    for d in pair_dists:
        pair_dist_hist[d] += 1

    # Audit-vs-random: for each audit_priority=1 span, find best cross-show
    # hamming match, and compare against the same metric for the same
    # number of randomly-chosen non-audit windows.
    def best_cross_show(wid: int) -> int:
        h0 = idx.windows[wid][3]
        own_show = show_by_eid[idx.windows[wid][0]]
        best = 64
        for ow, (oeid, _os, _oe, oh) in enumerate(idx.windows):
            if ow == wid:
                continue
            if show_by_eid[oeid] == own_show:
                continue
            d = popcount64(h0 ^ oh)
            if d < best:
                best = d
                if best == 0:
                    break
        return best

    # Index windows by (eid, span midpoint frame) for fast lookup
    wid_by_eid: dict[str, list[int]] = defaultdict(list)
    for wid, (eid, _ws, _we, _h) in enumerate(idx.windows):
        wid_by_eid[eid].append(wid)

    audit_best: list[int] = []
    audit_pairs_examined = 0
    for eid, spans in audit_spans.items():
        if eid not in wid_by_eid:
            continue
        for s, e in spans:
            mid = (s + e) / 2
            # find window with center closest to mid
            best_wid = None
            best_off = float("inf")
            for wid in wid_by_eid[eid]:
                _, ws, we, _ = idx.windows[wid]
                off = abs((ws + we) / 2 - mid)
                if off < best_off:
                    best_off = off
                    best_wid = wid
            if best_wid is None:
                continue
            audit_best.append(best_cross_show(best_wid))
            audit_pairs_examined += 1

    # Random control of same size
    random_best: list[int] = []
    rng_ctrl = random.Random(2718)
    if audit_pairs_examined > 0 and n_windows > 0:
        for _ in range(audit_pairs_examined):
            wid = rng_ctrl.randrange(n_windows)
            random_best.append(best_cross_show(wid))

    elapsed_total = time.time() - t0
    print(f"\nScoring done in {time.time() - t_index_done:.1f}s "
          f"(total {elapsed_total:.1f}s)")
    print(f"  windows with any tight-match cross-show hit: {len(results)}")
    print(f"  windows with any loose-match (incl. same-show): {loose_total}")
    print(f"  windows with cross-show loose-match: {loose_cross_show_total}")
    print(f"  cross-episode random-pair hamming mean: {pair_dist_mean:.1f} "
          f"(uniform-random expectation = 32)")
    if audit_best:
        print(f"  audit-p1 best cross-show hamming: n={len(audit_best)} "
              f"mean={sum(audit_best)/len(audit_best):.1f}")
    if random_best:
        print(f"  random   best cross-show hamming: n={len(random_best)} "
              f"mean={sum(random_best)/len(random_best):.1f}")

    # ----- report ---------------------------------------------------------
    write_report(
        results=results,
        total_windows=total_windows,
        episodes=episodes,
        shows=shows,
        elapsed_total=elapsed_total,
        loose_total=loose_total,
        loose_cross_show_total=loose_cross_show_total,
        audit_spans=audit_spans,
        sampled=(sample_n is not None and sample_n < len(episodes) * 2),
        pair_dist_hist=dict(pair_dist_hist),
        pair_dist_mean=pair_dist_mean,
        audit_best=audit_best,
        random_best=random_best,
    )
    return 0


# ----- report writer -------------------------------------------------------

def fmt_time(sec: float) -> str:
    m, s = divmod(int(round(sec)), 60)
    return f"{m:d}:{s:02d}"


def write_report(
    *,
    results: list[dict],
    total_windows: int,
    episodes: list[dict],
    shows: list[str],
    elapsed_total: float,
    loose_total: int,
    loose_cross_show_total: int,
    audit_spans: dict,
    sampled: bool,
    pair_dist_hist: dict[int, int],
    pair_dist_mean: float,
    audit_best: list[int],
    random_best: list[int],
) -> None:
    out_dir = ROOT / "analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"syndication-prototype-{TARGET_DATE_STR}.md"

    distinct_hist = defaultdict(int)
    audit_overlap_total = 0
    for r in results:
        bucket = r["distinct_shows"]
        if bucket >= 4:
            distinct_hist["4+"] += 1
        else:
            distinct_hist[str(bucket)] += 1
        if r["overlaps_audit_p1"]:
            audit_overlap_total += 1

    lines: list[str] = []
    lines.append(f"# Cross-show syndication prototype — {TARGET_DATE_STR}")
    lines.append("")
    lines.append("Proof of concept: can chromaprint + SimHash + LSH detect "
                 "cross-show duplicate ad segments on the current corpus?")
    lines.append("")
    lines.append("## Method")
    lines.append("")
    lines.append(f"- fpcalc `-raw -length {FPCALC_MAX_SECONDS}` per episode "
                 f"(cached under `TestFixtures/Corpus/Snapshots/fingerprints/`).")
    lines.append(f"- Window {WINDOW_SECONDS:.0f}s, hop {HOP_SECONDS:.0f}s "
                 f"at ~{APPROX_FPS} chromaprint frames/sec.")
    lines.append("- SimHash 64-bit per window: textbook construction. "
                 "Each of the 32 chromaprint input-bit positions has its "
                 "own deterministic random ±1 weight vector of length 64; "
                 "the hash is sign-of-sum across all frames and bits. "
                 "Per-byte lookup tables make this Python-fast.")
    lines.append(f"- LSH: {LSH_BANDS} × {BAND_BITS}-bit bands (any shared "
                 "band value flags a candidate pair; full 64-bit Hamming "
                 "is then computed exactly).")
    lines.append(f"- Match thresholds: tight ≤ {HAMMING_TIGHT} bits, "
                 f"loose ≤ {HAMMING_LOOSE} bits.")
    lines.append("")
    lines.append("## Corpus")
    lines.append("")
    lines.append(f"- Episodes processed: **{len(episodes)}** "
                 f"({'sampled' if sampled else 'full corpus'}).")
    lines.append(f"- Distinct shows: **{len(shows)}**.")
    lines.append(f"- Total windows indexed: **{total_windows}**.")
    lines.append(f"- Wall time: **{elapsed_total:.1f}s**.")
    lines.append("")

    lines.append("## Cross-show match distribution (tight ≤ "
                 f"{HAMMING_TIGHT} bits)")
    lines.append("")
    lines.append("| distinct_shows | windows |")
    lines.append("|---|---|")
    for k in ["2", "3", "4+"]:
        lines.append(f"| {k} | {distinct_hist.get(k, 0)} |")
    lines.append(f"| (any cross-show, total) | {len(results)} |")
    lines.append("")
    lines.append(
        f"Informational — loose threshold (≤ {HAMMING_LOOSE} bits): "
        f"{loose_cross_show_total} windows have ≥1 cross-show match "
        f"(out of {loose_total} with any loose match)."
    )
    lines.append("")

    lines.append(f"## Top {TOP_N_REPORT} windows by distinct_shows (tight)")
    lines.append("")
    if not results:
        lines.append("**(no cross-show tight matches found)**")
        lines.append("")
    else:
        for i, r in enumerate(results[:TOP_N_REPORT], 1):
            lines.append(
                f"### {i}. {r['show']} — `{r['eid']}` "
                f"@ {fmt_time(r['start'])}–{fmt_time(r['end'])}"
            )
            lines.append("")
            lines.append(
                f"- **distinct_shows:** {r['distinct_shows']}  "
                f"**match_count:** {r['match_count']}  "
                f"**overlaps audit_priority=1:** "
                f"{'yes' if r['overlaps_audit_p1'] else 'no'}"
            )
            lines.append("- Matches:")
            shown = sorted(r["matches"], key=lambda m: m["hamming"])
            for m in shown[:8]:
                lines.append(
                    f"  - {m['show']} `{m['eid']}` "
                    f"@ {fmt_time(m['start'])}–{fmt_time(m['end'])} "
                    f"(hamming={m['hamming']})"
                )
            if len(shown) > 8:
                lines.append(f"  - …and {len(shown) - 8} more")
            lines.append("")
            audio = AUDIO_DIR / f"{r['eid']}.mp3"
            dur = r["end"] - r["start"]
            lines.append(
                f"  Spot-check: "
                f"`ffplay -nodisp -autoexit -ss {r['start']:.0f} "
                f"-t {dur:.0f} '{audio.name}'`"
            )
            lines.append("")

    # SimHash noise floor
    lines.append("## SimHash noise floor (diagnostic)")
    lines.append("")
    lines.append(
        f"Hamming distance distribution across 5000 random cross-episode "
        f"window pairs:"
    )
    lines.append("")
    lines.append(
        f"- Mean: **{pair_dist_mean:.1f}** bits "
        f"(uniform-random 64-bit hashes would average 32)."
    )
    if pair_dist_hist:
        sorted_dists = sorted(pair_dist_hist.items())
        in_tight = sum(c for d, c in sorted_dists if d <= HAMMING_TIGHT)
        in_loose = sum(c for d, c in sorted_dists if d <= HAMMING_LOOSE)
        total = sum(c for _, c in sorted_dists)
        lines.append(
            f"- Random pairs within tight (≤{HAMMING_TIGHT}): "
            f"{in_tight}/{total} ({100.0*in_tight/total:.2f}%)."
        )
        lines.append(
            f"- Random pairs within loose (≤{HAMMING_LOOSE}): "
            f"{in_loose}/{total} ({100.0*in_loose/total:.2f}%)."
        )
        lines.append("")
        lines.append("Histogram (low end):")
        lines.append("")
        lines.append("| hamming | random-pair count | % |")
        lines.append("|---|---|---|")
        for d in range(0, HAMMING_LOOSE + 2):
            c = pair_dist_hist.get(d, 0)
            lines.append(f"| {d} | {c} | {100.0*c/total:.2f}% |")
        lines.append("")
    lines.append(
        f"If the noise floor mean is far below 32, the SimHash on "
        "chromaprint frames is picking up a structural \"this is speech "
        "audio\" signature shared by every podcast window, not the "
        "per-segment content. Treat sub-threshold matches with extreme "
        "suspicion when this is the case."
    )
    lines.append("")

    # Audit overlap summary
    lines.append("## Audit cross-reference")
    lines.append("")
    audit_eids = sum(1 for v in audit_spans.values() if v)
    audit_total_spans = sum(len(v) for v in audit_spans.values())
    lines.append(
        f"- Episodes with any `audit_priority=1` span: {audit_eids}  "
        f"(total spans: {audit_total_spans})."
    )
    lines.append(
        f"- Cross-show tight windows that overlap any audit_priority=1 "
        f"span: **{audit_overlap_total}** of {len(results)} "
        f"({(100.0*audit_overlap_total/len(results) if results else 0):.0f}%)."
    )
    lines.append("")

    # Audit-vs-random discrimination
    if audit_best or random_best:
        lines.append("### Audit-vs-random discrimination")
        lines.append("")
        lines.append(
            "For each known `audit_priority=1` ad span, look up the SimHash "
            "window closest to its midpoint and find its best (lowest) "
            "cross-show Hamming distance. Compare against the same metric "
            "computed for an equal-sized random sample of windows. "
            "If syndication is working, audit-p1 spans should have "
            "*systematically lower* best-cross-show distances than random "
            "windows."
        )
        lines.append("")

        def _summ(name: str, vals: list[int]) -> str:
            if not vals:
                return f"- **{name}:** (empty)"
            srt = sorted(vals)
            mid = srt[len(srt) // 2]
            return (f"- **{name}:** n={len(vals)} "
                    f"mean={sum(vals)/len(vals):.1f} "
                    f"median={mid} "
                    f"min={min(vals)} max={max(vals)}")
        lines.append(_summ("audit_priority=1", audit_best))
        lines.append(_summ("random control", random_best))
        lines.append("")
        if audit_best and random_best:
            a_mean = sum(audit_best) / len(audit_best)
            r_mean = sum(random_best) / len(random_best)
            delta = a_mean - r_mean
            if delta < -2.0:
                verdict_dr = ("Audit-p1 windows are meaningfully closer in "
                              "SimHash space to other shows than random "
                              "windows are — weak positive evidence that "
                              "the signal sees ad-syndication.")
            elif delta > 2.0:
                verdict_dr = ("Audit-p1 windows are *farther* from other "
                              "shows than random windows are. That's the "
                              "wrong direction for the syndication "
                              "hypothesis to be useful.")
            else:
                verdict_dr = ("Audit-p1 windows are statistically "
                              "indistinguishable from random windows on "
                              "this metric (Δ mean ≈ "
                              f"{delta:+.1f} bits). The SimHash is not "
                              "discriminating known ads from arbitrary "
                              "speech at this corpus size.")
            lines.append(verdict_dr)
            lines.append("")

    # Honest interpretation
    lines.append("## Honest interpretation")
    lines.append("")

    # Quantify the noise floor first
    noise_floor_strong = pair_dist_mean > 0 and pair_dist_mean < 24.0
    # Discrimination delta (signed; negative = audit closer than random = good)
    discrim_delta: float | None = None
    if audit_best and random_best:
        discrim_delta = (sum(audit_best) / len(audit_best)
                         - sum(random_best) / len(random_best))

    if noise_floor_strong:
        lines.append(
            f"**Noise floor dominates.** The mean Hamming distance "
            f"between random cross-episode window pairs is "
            f"**{pair_dist_mean:.1f}** bits — well below the "
            "32-bit expectation for uniformly-random 64-bit hashes. "
            "That means the SimHash of chromaprint frames is picking up "
            "a global \"this is podcast speech\" signature shared by "
            "nearly every window in the corpus."
        )
        lines.append("")
        lines.append(
            f"Consequence: matches at hamming ≤ {HAMMING_TIGHT} are not "
            "evidence of audio reuse. They're evidence that two windows "
            "both contain speech with similar voicing/dynamic-range "
            "characteristics, which is true for almost every window pair "
            "in this corpus."
        )
        lines.append("")
        if discrim_delta is not None:
            if discrim_delta < -2.0:
                lines.append(
                    f"That said: **the audit-vs-random discrimination test** "
                    f"shows known ads have best-cross-show distances "
                    f"{abs(discrim_delta):.1f} bits lower than random "
                    "windows. There IS some signal — but it's drowned by "
                    "noise at the current threshold."
                )
            else:
                lines.append(
                    f"**The audit-vs-random test shows no discrimination** "
                    f"(Δ = {discrim_delta:+.1f} bits, |Δ| < 2). On the "
                    f"{len(audit_best)} audit-p1 spans we could test, "
                    "their best cross-show match is no closer than a "
                    "random window's best cross-show match. The "
                    "syndication signal — if it exists in this corpus — "
                    "is too weak to extract with this fingerprint at this N."
                )
            lines.append("")
        lines.append(
            "**Verdict on the hypothesis:** chromaprint+SimHash, as built "
            "here, does NOT cleanly identify cross-show duplicate ads on "
            "this 41-episode corpus. The top-N table is dominated by "
            "false positives from generic speech-vs-speech similarity. "
            "Before wiring syndication into production, we'd need at "
            "least one of: (a) a different acoustic fingerprint less "
            "biased on voice (e.g., MFCC + DTW; openl3 embeddings), "
            "(b) a much larger corpus where real syndicated ads recur "
            "enough to stand out above the bias floor, or (c) a tighter "
            "match criterion (e.g., contiguous-run requirements on raw "
            "chromaprint frames, the way l2f-dai-rediff.py aligns "
            "duplicate episode-pair regions)."
        )
    elif not results:
        lines.append(
            f"**No clustering.** At N={len(episodes)} episodes / "
            f"{len(shows)} shows, no cross-show 30s window matched "
            f"another within {HAMMING_TIGHT} Hamming bits, and the "
            "noise floor is plausible."
        )
        lines.append("")
        lines.append(
            "Most likely: (a) the corpus is too small — most ads in this "
            "sample only appear in one episode each; (b) MP3 re-encoding "
            "shifted enough bits to push real ad pairs above the "
            "threshold; or (c) the dominant ad pattern at this N is "
            "host-read intra-show, not programmatic cross-show. Loose "
            "threshold may be more informative."
        )
    elif distinct_hist.get("4+", 0) + distinct_hist.get("3", 0) > 0:
        lines.append(
            "**Cross-show clustering exists above the noise floor.** "
            f"{distinct_hist.get('4+', 0)} window(s) appear in 4+ distinct "
            f"shows and {distinct_hist.get('3', 0)} in exactly 3 shows. "
            "Spot-check the top entries with ffplay before treating this "
            "as ad signal."
        )
        lines.append("")
        if audit_overlap_total > 0:
            lines.append(
                f"Of the {len(results)} cross-show tight windows, "
                f"{audit_overlap_total} overlap an existing "
                f"`audit_priority=1` span — partial corroboration."
            )
        else:
            lines.append(
                "None of the cross-show tight windows overlap any existing "
                "`audit_priority=1` span. Either a precision failure or "
                "a recall win for syndication — ffplay spot-checks are "
                "the only way to tell."
            )
    else:
        lines.append(
            f"**Marginal signal.** Only {distinct_hist.get('2', 0)} "
            "window(s) match across exactly 2 shows; nothing reaches 3+. "
            "At this N the two-show pairs could be SimHash noise or "
            "real sparse syndication; ffplay spot-checks required."
        )
    lines.append("")

    # Caveats
    lines.append("## Caveats")
    lines.append("")
    lines.append(
        f"- **N is small.** {len(episodes)} episodes across {len(shows)} "
        "shows. A programmatic ad needs at least two episodes from "
        "different shows in the corpus during the same campaign window "
        "to show up at all. Most ad campaigns have a longer reach than "
        "our sample.")
    lines.append(
        "- **chromaprint is not tuned for ads.** It was built for music "
        "identification; speech with similar voicing or background music "
        "may collide. Conversely, the same ad re-encoded at a different "
        "bitrate may shift bits enough to miss our tight threshold.")
    lines.append(
        "- **SimHash false positives.** Our 64-bit SimHash on 32-bit "
        "chromaprint frames is a hand-rolled summary, not a battle-tested "
        "audio fingerprint. The vote-sum construction is locality-preserving "
        "for stable inputs but can collide on near-uniform random inputs.")
    lines.append(
        "- **Window/hop tradeoff.** 30s/5s = ~10× redundancy per window. "
        "An ad that's 25s long can still be caught at the edges, but a "
        "5-second house promo will be drowned by surrounding content.")
    lines.append(
        f"- **Length cap.** fpcalc was capped at {FPCALC_MAX_SECONDS}s to "
        "keep runtime bounded; episodes longer than that are partially "
        "sampled (head only).")
    lines.append(
        "- **Same-show duplicates excluded from `distinct_shows`.** A back-"
        "catalog episode that re-uses a 30s opening from the same show "
        "won't count here. That's intentional: syndication evidence "
        "requires *cross-show* recurrence to be a useful ad signal.")
    lines.append("")

    out_path.write_text("\n".join(lines))
    print(f"\nReport written: {out_path}")


# ----- main ----------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--self-test", action="store_true",
                    help="Run logic self-test only (no real episodes).")
    ap.add_argument("--sample", type=int, default=None,
                    help="Cap to N random episodes (fallback for OOM/slow).")
    args = ap.parse_args()

    if args.self_test:
        return run_self_test()
    return run_corpus(sample_n=args.sample)


if __name__ == "__main__":
    sys.exit(main())
