#!/usr/bin/env python3
"""
l2f-dai-rediff.py — Tier-A DAI rediff: detect and extract dynamic ad insertions.

Pairs with scripts/l2f-dai-snapshot.py. The snapshot script saves an episode's
audio + sha256 NOW. Weeks later, when the podcast host rotates dynamic ad
campaigns, this script re-downloads each enclosure and ALIGNS the original
audio against the fresh one using chromaprint audio fingerprints. Segments
that appear in one file but not the other are the dynamic ad fills — and the
boundaries are *exact*, derived purely from waveform comparison. No human,
no model, no cloud.

Algorithm (v1 — keep it understandable; ~200 lines):
  1. For each manifest entry, re-download the enclosureUrl to a temp file and
     compute its sha256. If unchanged, no rotation yet — skip with a clear log.
  2. If rotated, run `fpcalc -json -raw -length 0` on BOTH the snapshot and
     the fresh download to extract integer fingerprint sequences. fpcalc emits
     ~7.93 Hz raw fingerprints (one 32-bit fingerprint per ~126 ms). We derive
     the actual rate per-file as count/duration so any future fpcalc version
     change doesn't silently corrupt the conversion to seconds.
  3. ALIGN: build an inverted index of A's fingerprint integers → positions.
     For each fingerprint in B (the fresh download), look up matches in A. We
     accept a small Hamming distance (≤ HAMMING_TOL bits, default 2) by also
     bucketing each fingerprint under its single-bit-flip neighbors with the
     top 24 most-variable bits NOT flipped (v1 keeps it simple: we do straight
     equality + a chained pass that allows a 2-bit-error window during the
     consistent-offset run extension below).
  4. RUN MERGE: a "run" is a maximal stretch where B[i..i+k] maps to A[j..j+k]
     with a constant offset (j-i is fixed throughout, modulo HAMMING_TOL noise
     on individual fingerprints). We greedily extend matched anchors into runs,
     then merge overlapping/adjacent runs.
  5. GAPS: any range in B NOT covered by a run is an INSERTED segment (likely
     a fresh ad in the rotated download). Any range in A not covered is a
     REMOVED segment (the old ad). We emit inserted-in-B segments ≥
     MIN_AD_SECONDS as ad slots, since "what's new in the rediff" is the
     practically useful corpus signal (the snapshot Audio/ already has the
     OLD audio, and the fresh download has the NEW one; the human reviewer
     wants to know where the boundaries shift between today's listen and a
     future re-listen).
  6. CONFIDENCE: derived from the matched-run quality on either side of the
     gap. A gap flanked by two long high-density runs scores high. A gap that
     trails off into low-density alignment scores lower. v1 formula:
       conf = 1 - exp(-min(left_run_sec, right_run_sec) / 60)
     so a 60-second flanking run on each side gives ~0.63 confidence; a
     5-minute flank on each side gives ~0.99.

Outputs:
  * playhead-dogfood-diagnostics-tier-a-rediff.json (repo root, git-ignored
    via the `playhead-dogfood-diagnostics-*.json` rule). Per-episode rotation
    status + ad slots + global counts.
  * TestFixtures/Corpus/Drafts/<episode-id>.dai-rediff.json (one per rotated
    episode, git-ignored via the `TestFixtures/Corpus/Drafts/*` rule). Same
    schema as l2f-draft-annotation.swift's *.draft.json output:
      { ad_windows: [{ start_seconds, end_seconds, ad_type: "dai",
                       confidence_notes, ... }],
        ... }
    so the existing l2f-promote-reviewed-corpus.py can promote these after
    a human verifies the boundaries against the audio.

Modes:
  * default: re-download every manifest entry and rediff.
  * --dry-run: skip the re-download; assume <audioPath>.fresh.mp3 (or
    <audioPath>) already exists on disk. Useful for re-running the alignment
    on a manually staged pair.
  * --self-test: synthesize a known-splice pair via ffmpeg from the FIRST
    existing snapshot, splicing in 30 SECONDS of pink-noise (anoisesrc) at
    t=120s so the insert is acoustically distinct from any podcast content
    and cannot accidentally match A elsewhere. Run the alignment, and PASS
    iff a detected inserted segment in B lies within [115,125]s × [145,155]s
    (start ≈120 ±5, end ≈150 ±5). Fail loud otherwise. This is the v1
    ground-truth check that the algorithm actually works before we wait
    weeks for a real ad rotation to happen.

Constraints:
  * Stdlib + subprocess only. fpcalc + ffmpeg + curl are external binaries
    (all already installed: /opt/homebrew/bin/{fpcalc,ffmpeg}, curl in PATH).
  * Deterministic given identical inputs.
  * Honest error handling: per-episode failures (re-download fails, fpcalc
    returns no fingerprints, alignment finds zero runs) are logged and
    recorded in the output JSON; they do NOT crash the whole batch.
"""

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import pathlib
import subprocess
import sys
import tempfile

REPO = pathlib.Path(__file__).resolve().parents[1]
AUDIO_ROOT = REPO / "TestFixtures/Corpus/Audio"
MANIFEST = REPO / "TestFixtures/Corpus/Snapshots/manifest.json"
DRAFTS = REPO / "TestFixtures/Corpus/Drafts"
DIAG_OUT = REPO / "playhead-dogfood-diagnostics-tier-a-rediff.json"

FPCALC = "/opt/homebrew/bin/fpcalc"
FFMPEG = "/opt/homebrew/bin/ffmpeg"
UA = "Mozilla/5.0 (Macintosh) Podcast/1.0"
DOWNLOAD_TIMEOUT = "240"  # seconds
MIN_AD_SECONDS = 5.0
HAMMING_TOL = 2  # bits tolerance for individual fingerprint comparison
MIN_RUN_FPS = 8  # ≈1 second minimum aligned-run length before we trust an offset

# ---------- subprocess helpers ----------

def run(cmd, timeout=120, stdin_bytes=None):
    return subprocess.run(
        cmd, input=stdin_bytes, capture_output=True, timeout=timeout, check=False
    )

def sha256_file(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def fpcalc_raw(path: pathlib.Path) -> tuple[list[int], float]:
    """Return (fingerprints, seconds_per_fp). Raises RuntimeError on failure."""
    p = run([FPCALC, "-json", "-raw", "-length", "0", str(path)], timeout=300)
    if p.returncode != 0:
        raise RuntimeError(f"fpcalc rc={p.returncode}: {p.stderr.decode('utf-8','ignore')[:200]}")
    try:
        obj = json.loads(p.stdout.decode("utf-8", "ignore"))
    except Exception as e:
        raise RuntimeError(f"fpcalc json parse: {e}")
    fp = obj.get("fingerprint")
    duration = float(obj.get("duration") or 0.0)
    if not isinstance(fp, list) or len(fp) < 10:
        raise RuntimeError(f"fpcalc returned no fingerprints (got {len(fp) if isinstance(fp,list) else 'non-list'})")
    if duration <= 0:
        raise RuntimeError(f"fpcalc returned non-positive duration: {duration}")
    sec_per_fp = duration / len(fp)
    return [int(x) & 0xFFFFFFFF for x in fp], sec_per_fp

def curl_download(url: str, out: pathlib.Path) -> tuple[bool, str]:
    """Download url → out via curl. Returns (ok, error)."""
    out.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "curl", "-sL", "-A", UA, "--max-time", DOWNLOAD_TIMEOUT,
        "-o", str(out), url,
    ]
    p = run(cmd, timeout=int(DOWNLOAD_TIMEOUT) + 30)
    if p.returncode != 0:
        return False, f"curl rc={p.returncode}: {p.stderr.decode('utf-8','ignore')[:200]}"
    if not out.exists() or out.stat().st_size < 100_000:
        sz = out.stat().st_size if out.exists() else 0
        return False, f"download too small ({sz} bytes)"
    return True, ""

# ---------- alignment ----------

def popcount(x: int) -> int:
    return bin(x & 0xFFFFFFFF).count("1")

def find_runs(fpA: list[int], fpB: list[int],
              hamming_tol: int = HAMMING_TOL,
              min_run_len: int = MIN_RUN_FPS) -> list[tuple[int, int, int, int]]:
    """
    Find maximal consistent-offset runs aligning B to A.

    A "run" is (a_start, b_start, length, errors) where for k in [0, length):
        popcount(fpA[a_start + k] ^ fpB[b_start + k]) <= hamming_tol
    and (a_start - b_start) is the run's constant offset.

    Strategy: bucket A's fingerprints by exact value → list of A-positions.
    For each i in B, look up exact matches in A. Each such anchor seeds a
    candidate offset (a_pos - i). We greedily extend the anchor in both
    directions as long as the Hamming-distance threshold holds, mark the
    covered B range, and continue past the run's end.
    """
    idxA: dict[int, list[int]] = {}
    for j, v in enumerate(fpA):
        idxA.setdefault(v, []).append(j)

    runs: list[tuple[int, int, int, int]] = []
    covered_until_b = -1  # past the last run's end in B; skip exhaustive seeding within
    i = 0
    while i < len(fpB):
        if i <= covered_until_b:
            i += 1
            continue
        anchors = idxA.get(fpB[i])
        if not anchors:
            i += 1
            continue
        best = None  # (a_start, b_start, length, errors)
        for a0 in anchors:
            # extend forward
            k = 0
            errs = 0
            while (a0 + k < len(fpA) and i + k < len(fpB)):
                d = popcount(fpA[a0 + k] ^ fpB[i + k])
                if d > hamming_tol:
                    break
                errs += d
                k += 1
            length = k
            # extend backward
            b = 1
            while (a0 - b >= 0 and i - b >= 0 and i - b > covered_until_b):
                d = popcount(fpA[a0 - b] ^ fpB[i - b])
                if d > hamming_tol:
                    break
                errs += d
                b += 1
            b -= 1
            a_start = a0 - b
            b_start = i - b
            length = k + b
            if length < min_run_len:
                continue
            if best is None or length > best[2]:
                best = (a_start, b_start, length, errs)
        if best is None:
            i += 1
            continue
        runs.append(best)
        covered_until_b = best[1] + best[2] - 1
        i = covered_until_b + 1
    return runs

def merge_runs(runs: list[tuple[int, int, int, int]],
               offset_slack: int = 2,
               gap_diff_slack: int = 2) -> list[tuple[int, int, int, int]]:
    """
    Merge runs that share approximately the same offset (j-i) AND whose
    B-gap is matched by an equal-sized A-gap (i.e. the inter-run interval
    is noise on BOTH sides, not an insertion).

    Key insight: chromaprint fingerprints occasionally fail to match within
    HAMMING_TOL within an otherwise-aligned region (MP3 re-encoding,
    psychoacoustic frame boundaries, etc.). When that happens, a single
    continuous alignment fragments into many short runs all sharing the
    same offset (j-i). For each such gap:
      * If `gap_in_A == gap_in_B` (within `gap_diff_slack`): inter-run
        interval is the same length on both sides, so it's noise — merge
        the runs into one.
      * If `gap_in_B > gap_in_A`: there's MORE B-content than A-content
        in the gap. The excess is the ad insertion. Keep the runs separate
        so gaps_in_b() reports the excess as an ad slot.
    """
    if not runs:
        return runs
    runs = sorted(runs, key=lambda r: r[1])
    merged = [runs[0]]
    for cur in runs[1:]:
        last = merged[-1]
        last_off = last[0] - last[1]
        cur_off = cur[0] - cur[1]
        b_gap = cur[1] - (last[1] + last[2])
        a_gap = cur[0] - (last[0] + last[2])
        # b_gap must be non-negative (runs are sorted by B-start). a_gap can
        # be negative if a different offset (we won't merge those).
        if (abs(last_off - cur_off) <= offset_slack
            and b_gap >= 0
            and a_gap >= 0
            and abs(b_gap - a_gap) <= gap_diff_slack):
            new_length = cur[1] + cur[2] - last[1]
            new_errors = last[3] + cur[3]
            merged[-1] = (last[0], last[1], new_length, new_errors)
        else:
            merged.append(cur)
    return merged

def gaps_in_b(runs: list[tuple[int, int, int, int]], total_b: int,
              min_gap_fps: int) -> list[tuple[int, int, int, int]]:
    """
    Inserted-segments-in-B = B indices not covered by any run.

    Returns list of (b_start, b_end_exclusive, left_run_len, right_run_len).
    left_run_len / right_run_len = the flanking run lengths (in fingerprints),
    used to compute confidence. 0 if at the boundary.
    """
    if not runs:
        return [(0, total_b, 0, 0)] if total_b >= min_gap_fps else []
    runs = sorted(runs, key=lambda r: r[1])
    gaps = []
    # head
    first = runs[0]
    if first[1] >= min_gap_fps:
        gaps.append((0, first[1], 0, first[2]))
    for a, b in zip(runs, runs[1:]):
        a_end = a[1] + a[2]
        if b[1] - a_end >= min_gap_fps:
            gaps.append((a_end, b[1], a[2], b[2]))
    last = runs[-1]
    tail_start = last[1] + last[2]
    if total_b - tail_start >= min_gap_fps:
        gaps.append((tail_start, total_b, last[2], 0))
    return gaps

def confidence_for_gap(left_run_len: int, right_run_len: int,
                       sec_per_fp: float) -> float:
    flank_sec = min(left_run_len, right_run_len) * sec_per_fp
    return round(1 - math.exp(-flank_sec / 60.0), 3)

# ---------- per-episode rediff ----------

def rediff_pair(snapshot_path: pathlib.Path, fresh_path: pathlib.Path) -> dict:
    fpA, sec_per_fp_A = fpcalc_raw(snapshot_path)
    fpB, sec_per_fp_B = fpcalc_raw(fresh_path)
    # Use B's seconds-per-fp for gap reporting (gaps are indices into B).
    runs = find_runs(fpA, fpB)
    runs = merge_runs(runs)
    if not runs:
        return {
            "ok": False,
            "error": "alignment-found-zero-runs (likely entirely different episodes)",
            "fingerprintsA": len(fpA), "fingerprintsB": len(fpB),
            "secondsPerFpA": round(sec_per_fp_A, 5),
            "secondsPerFpB": round(sec_per_fp_B, 5),
        }
    min_gap_fps = max(1, int(round(MIN_AD_SECONDS / sec_per_fp_B)))
    gaps = gaps_in_b(runs, len(fpB), min_gap_fps)
    ad_slots = []
    for (bs, be, lrun, rrun) in gaps:
        ad_slots.append({
            "startSeconds": round(bs * sec_per_fp_B, 2),
            "endSeconds": round(be * sec_per_fp_B, 2),
            "durationSeconds": round((be - bs) * sec_per_fp_B, 2),
            "confidence": confidence_for_gap(lrun, rrun, sec_per_fp_B),
            "leftRunSeconds": round(lrun * sec_per_fp_B, 2),
            "rightRunSeconds": round(rrun * sec_per_fp_B, 2),
        })
    total_run_sec = sum(r[2] for r in runs) * sec_per_fp_B
    return {
        "ok": True,
        "fingerprintsA": len(fpA), "fingerprintsB": len(fpB),
        "secondsPerFpA": round(sec_per_fp_A, 5),
        "secondsPerFpB": round(sec_per_fp_B, 5),
        "runs": len(runs),
        "alignedSecondsB": round(total_run_sec, 2),
        "adSlots": ad_slots,
    }

def write_draft(episode_id: str, snapshot_path: pathlib.Path,
                ad_slots: list[dict], fresh_sha: str) -> pathlib.Path:
    DRAFTS.mkdir(parents=True, exist_ok=True)
    out = DRAFTS / f"{episode_id}.dai-rediff.json"
    ad_windows = []
    for slot in ad_slots:
        ad_windows.append({
            "start_seconds": slot["startSeconds"],
            "end_seconds": slot["endSeconds"],
            "advertiser": None,
            "product": None,
            "advertiser_guess": None,
            "product_guess": None,
            "ad_type": "dai",
            "transition_type": None,
            "confidence_notes": (
                f"DRAFT chromaprint DAI rediff; alignment confidence={slot['confidence']}, "
                f"flanking-run-seconds=(left={slot['leftRunSeconds']}, right={slot['rightRunSeconds']}). "
                f"Boundaries derived from fingerprint alignment of original snapshot vs fresh download "
                f"(fresh sha256={fresh_sha[:12]}). Verify advertiser/product by listening to the segment."
            ),
        })
    payload = {
        "episode_id": episode_id,
        "show_name": episode_id.replace("-", " ").title(),
        "ad_windows": ad_windows,
        "audio_fingerprint": f"sha256:{fresh_sha}",
        "variant_of": None,
        "source": "l2f-dai-rediff.py",
        "draft_kind": "dai-rediff",
    }
    out.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return out

# ---------- self-test ----------

def ffmpeg_slice(src: pathlib.Path, dst: pathlib.Path, start: float, duration: float):
    # Re-encode to a uniform MP3 so concat doesn't choke on differing bitstreams.
    cmd = [
        FFMPEG, "-y", "-loglevel", "error",
        "-ss", f"{start:.3f}", "-t", f"{duration:.3f}",
        "-i", str(src),
        "-acodec", "libmp3lame", "-ar", "44100", "-ac", "2", "-b:a", "128k",
        str(dst),
    ]
    p = run(cmd, timeout=180)
    if p.returncode != 0:
        raise RuntimeError(f"ffmpeg slice failed: {p.stderr.decode('utf-8','ignore')[:300]}")

def ffmpeg_splice_with_noise(A: pathlib.Path, dst: pathlib.Path,
                             insert_at: float, insert_duration: float,
                             a_duration: float, seed: int = 42):
    """
    Build dst = A[0:insert_at] + pink_noise(insert_duration) + A[insert_at:a_duration]
    via a SINGLE ffmpeg invocation (atrim + concat filter graph). This avoids
    multi-pass slice-and-concat artifacts where seeking with -ss shifts MP3
    frame boundaries enough to disrupt chromaprint fingerprint matching on
    the post-splice content. Pink noise is acoustically distinct from speech
    and shares effectively zero fingerprint content with any podcast snapshot
    — ideal "external ad" stand-in for ground-truth self-testing.
    """
    filter_complex = (
        f"[0:a]atrim=0:{insert_at:.3f},asetpts=PTS-STARTPTS[a1];"
        f"[0:a]atrim={insert_at:.3f}:{a_duration:.3f},asetpts=PTS-STARTPTS[a3];"
        f"[1:a]aformat=channel_layouts=stereo[noise];"
        f"[a1][noise][a3]concat=n=3:v=0:a=1[out]"
    )
    cmd = [
        FFMPEG, "-y", "-loglevel", "error",
        "-i", str(A),
        "-f", "lavfi",
        "-i", f"anoisesrc=color=pink:seed={seed}:amplitude=0.5:duration={insert_duration:.3f}:sample_rate=44100",
        "-filter_complex", filter_complex,
        "-map", "[out]",
        "-acodec", "libmp3lame", "-ar", "44100", "-ac", "2", "-b:a", "128k",
        str(dst),
    ]
    p = run(cmd, timeout=240)
    if p.returncode != 0:
        raise RuntimeError(f"ffmpeg splice-with-noise failed: {p.stderr.decode('utf-8','ignore')[:400]}")

def self_test() -> int:
    manifest = json.loads(MANIFEST.read_text()) if MANIFEST.exists() else []
    src = None
    for entry in manifest:
        p = REPO / entry["audioPath"]
        if p.exists() and p.stat().st_size > 5_000_000:
            src = p
            break
    if src is None:
        print("SELF-TEST FAIL: no existing snapshot audio to splice from", file=sys.stderr)
        return 2
    print(f"self-test: splicing from {src.relative_to(REPO)}")
    tmp = pathlib.Path(tempfile.mkdtemp(prefix="l2f-dai-rediff-selftest-"))
    A = tmp / "synthetic_A.mp3"      # first 5 minutes of src
    B = tmp / "synthetic_B.mp3"      # A with 30s of pink noise spliced in at t=120s
    try:
        ffmpeg_slice(src, A, 0, 300)
        # Build B in a single ffmpeg pass so MP3 frame boundaries on either side
        # of the splice stay phase-aligned with A (otherwise -ss seeks across
        # the splice shift chromaprint fingerprints by 8-16 bits on the
        # post-splice section, fragmenting alignment).
        ffmpeg_splice_with_noise(A, B, insert_at=120.0, insert_duration=30.0,
                                 a_duration=300.0)
        result = rediff_pair(A, B)
        print("self-test rediff result:", json.dumps(result, indent=2))
        if not result.get("ok"):
            print(f"SELF-TEST FAIL: rediff_pair not ok: {result.get('error')}", file=sys.stderr)
            return 3
        # PASS iff at least one inserted slot has start ∈ [115,125] and end ∈ [145,155]
        for slot in result["adSlots"]:
            if 115 <= slot["startSeconds"] <= 125 and 145 <= slot["endSeconds"] <= 155:
                print(f"SELF-TEST PASS: detected splice at {slot['startSeconds']}-{slot['endSeconds']}s "
                      f"(target 120-150 ±5, confidence={slot['confidence']})")
                return 0
        print("SELF-TEST FAIL: no detected ad-slot fell within target window [115,125]×[145,155].",
              file=sys.stderr)
        print("Detected slots:", json.dumps(result["adSlots"], indent=2), file=sys.stderr)
        return 4
    finally:
        # keep tmp dir for inspection if KEEP env var set
        if not os.environ.get("KEEP_SELFTEST"):
            for p in tmp.glob("*"):
                try: p.unlink()
                except Exception: pass
            try: tmp.rmdir()
            except Exception: pass

# ---------- main batch ----------

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--self-test", action="store_true",
                    help="Splice a synthetic pair via ffmpeg and verify the algorithm detects the known insert.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Skip re-download; assume <audioPath>.fresh.mp3 already exists. Useful for re-running alignment.")
    ap.add_argument("--episode", action="append",
                    help="Only process episode IDs containing this substring (repeatable).")
    args = ap.parse_args()

    if args.self_test:
        sys.exit(self_test())

    if not MANIFEST.exists():
        print(f"ERROR: manifest not found at {MANIFEST}", file=sys.stderr)
        sys.exit(2)
    manifest = json.loads(MANIFEST.read_text())
    if args.episode:
        manifest = [e for e in manifest
                    if any(needle in e["episodeId"] for needle in args.episode)]
        if not manifest:
            print("ERROR: --episode filter matched zero entries", file=sys.stderr)
            sys.exit(2)

    per_episode = []
    rotated_count = 0
    unchanged_count = 0
    failed_count = 0
    drafted_count = 0
    started = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

    for entry in manifest:
        ep = entry["episodeId"]
        snapshot_path = REPO / entry["audioPath"]
        manifest_sha = entry["sha256"]
        record = {"episodeId": ep, "rotated": False, "adSlots": []}
        if not snapshot_path.exists():
            record["ok"] = False
            record["error"] = f"snapshot audio missing at {snapshot_path}"
            failed_count += 1
            print(f"  [SKIP] {ep}: {record['error']}")
            per_episode.append(record); continue

        if args.dry_run:
            # Look for a pre-staged fresh file alongside the snapshot.
            fresh_path = snapshot_path.with_suffix(".fresh.mp3")
            if not fresh_path.exists():
                record["ok"] = False
                record["error"] = (
                    f"dry-run: expected pre-staged fresh audio at "
                    f"{fresh_path.relative_to(REPO)} (did not find)"
                )
                failed_count += 1
                print(f"  [SKIP] {ep}: {record['error']}")
                per_episode.append(record); continue
            fresh_sha = sha256_file(fresh_path)
        else:
            with tempfile.NamedTemporaryFile(prefix=f"l2f-dai-rediff-{ep}-", suffix=".mp3", delete=False) as tf:
                fresh_path = pathlib.Path(tf.name)
            print(f"  ↓ re-download {ep}", flush=True)
            ok, err = curl_download(entry["enclosureUrl"], fresh_path)
            if not ok:
                record["ok"] = False
                record["error"] = f"re-download failed: {err}"
                failed_count += 1
                print(f"  [FAIL] {ep}: {err}")
                try: fresh_path.unlink()
                except Exception: pass
                per_episode.append(record); continue
            fresh_sha = sha256_file(fresh_path)

        record["freshSha256"] = fresh_sha
        record["manifestSha256"] = manifest_sha
        if fresh_sha == manifest_sha:
            record["rotated"] = False
            record["ok"] = True
            record["note"] = "no rotation yet — fresh sha256 matches manifest"
            unchanged_count += 1
            print(f"  [NO ROTATION] {ep}")
            if not args.dry_run:
                try: fresh_path.unlink()
                except Exception: pass
            per_episode.append(record); continue

        record["rotated"] = True
        print(f"  [ROTATED] {ep} (manifest {manifest_sha[:12]} → fresh {fresh_sha[:12]})")
        try:
            diff = rediff_pair(snapshot_path, fresh_path)
        except RuntimeError as e:
            record["ok"] = False
            record["error"] = f"rediff failed: {e}"
            failed_count += 1
            print(f"  [FAIL] {ep}: {e}")
            if not args.dry_run:
                try: fresh_path.unlink()
                except Exception: pass
            per_episode.append(record); continue

        record["ok"] = diff.get("ok", False)
        if not diff.get("ok"):
            record["error"] = diff.get("error")
            failed_count += 1
            print(f"  [FAIL] {ep}: {record['error']}")
        else:
            record["adSlots"] = diff["adSlots"]
            record["alignment"] = {
                "fingerprintsA": diff["fingerprintsA"],
                "fingerprintsB": diff["fingerprintsB"],
                "secondsPerFpA": diff["secondsPerFpA"],
                "secondsPerFpB": diff["secondsPerFpB"],
                "runs": diff["runs"],
                "alignedSecondsB": diff["alignedSecondsB"],
            }
            rotated_count += 1
            if diff["adSlots"]:
                draft_path = write_draft(ep, snapshot_path, diff["adSlots"], fresh_sha)
                record["draftPath"] = str(draft_path.relative_to(REPO))
                drafted_count += 1
                print(f"  [DRAFT] wrote {draft_path.relative_to(REPO)} ({len(diff['adSlots'])} ad slots)")
            else:
                print(f"  [ROTATED, no ad slots ≥{MIN_AD_SECONDS}s detected]")
        if not args.dry_run:
            try: fresh_path.unlink()
            except Exception: pass
        per_episode.append(record)

    summary = {
        "schemaVersion": 1,
        "tool": "scripts/l2f-dai-rediff.py",
        "startedIso": started,
        "completedIso": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "dryRun": args.dry_run,
        "manifestPath": str(MANIFEST.relative_to(REPO)),
        "totals": {
            "episodes": len(per_episode),
            "rotated": rotated_count,
            "unchanged": unchanged_count,
            "failed": failed_count,
            "draftsWritten": drafted_count,
        },
        "episodes": per_episode,
    }
    DIAG_OUT.write_text(json.dumps(summary, indent=2, sort_keys=True))
    print(f"\n{'='*60}")
    print(f"rediff complete: {rotated_count} rotated, {unchanged_count} unchanged, "
          f"{failed_count} failed, {drafted_count} drafts written")
    print(f"diagnostics: {DIAG_OUT.relative_to(REPO)}")
    # Honest exit code: 0 if nothing broke (rotations are not errors).
    sys.exit(0 if failed_count == 0 else 1)


if __name__ == "__main__":
    main()
