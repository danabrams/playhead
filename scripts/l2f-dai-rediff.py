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
  5. GAPS: any range in A NOT covered by a run is a REMOVED segment (usually
     the old dynamic ad). We emit removed-from-A segments ≥ MIN_AD_SECONDS
     because A is the retained corpus asset. B coordinates are forbidden —
     the B download is deleted after the diff by default (--retain-audio
     keeps it at <audioPath>.fresh.mp3 as rediff-treatment-harness input,
     not a corpus asset), so B intervals would attach coordinates to audio
     the corpus does not retain.
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
  * --dry-run: skip the re-download; requires a pre-staged
    <audioPath>.fresh.mp3 beside the snapshot (the plain <audioPath> is NOT
    accepted as the fresh side). Useful for re-running the alignment on a
    manually staged pair.
  * --retain-audio (playhead-xsdz.36.1): on a rotated episode whose rediff
    succeeded, keep the fresh B-side at <audioPath>.fresh.mp3 — the exact
    path --dry-run reads and the Swift rediff treatment harness
    (CorpusFreshBSideProvider) feeds as the B-side — instead of deleting it.
    Failure paths still delete the temp download; --dry-run is unaffected.
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
import re
import stat
import subprocess
import sys
import tempfile
import urllib.parse
from typing import Optional

from convert_annotations_to_chapter_goldens import path_has_symlink_component

REPO = pathlib.Path(__file__).resolve().parents[1]
AUDIO_ROOT = REPO / "TestFixtures/Corpus/Audio"
MANIFEST = REPO / "TestFixtures/Corpus/Snapshots/manifest.json"
DRAFTS = REPO / "TestFixtures/Corpus/Drafts"
DIAG_OUT = REPO / "playhead-dogfood-diagnostics-tier-a-rediff.json"


def fresh_sibling(snapshot_path: pathlib.Path) -> pathlib.Path:
    """The staged fresh-B-side path for a snapshot: `<audioPath stem>.fresh.mp3`
    beside the snapshot. SINGLE derivation of the naming convention (R5) —
    shared by the --dry-run reader and the --retain-audio writer, and mirrored
    by `CorpusFreshBSideProvider.freshURL` on the Swift side (manifest
    validation pins audioPath stem == episodeId, so both handoff sides agree).
    """
    return snapshot_path.with_suffix(".fresh.mp3")

FPCALC = "/opt/homebrew/bin/fpcalc"
FFMPEG = "/opt/homebrew/bin/ffmpeg"
UA = "Mozilla/5.0 (Macintosh) Podcast/1.0"
DOWNLOAD_TIMEOUT = "240"  # seconds
MIN_AD_SECONDS = 5.0
HAMMING_TOL = 2  # bits tolerance for individual fingerprint comparison
MIN_RUN_FPS = 8  # ≈1 second minimum aligned-run length before we trust an offset


class RediffInputError(ValueError):
    """The snapshot manifest cannot safely drive a mutating rediff run."""


def _require_safe_path(path: pathlib.Path, *, label: str) -> None:
    if path_has_symlink_component(path):
        raise RediffInputError(f"{label} contains a symbolic link: {path}")


def _require_safe_directory(
    path: pathlib.Path, *, label: str, create: bool = False
) -> None:
    _require_safe_path(path, label=label)
    if create:
        path.mkdir(parents=True, exist_ok=True)
        _require_safe_path(path, label=label)
    if not path.is_dir():
        raise RediffInputError(f"{label} is not a regular directory: {path}")


def _draft_path(episode_id: str) -> pathlib.Path:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,499}", episode_id) is None:
        raise RediffInputError(f"unsafe episode id for rediff draft: {episode_id!r}")
    return DRAFTS / f"{episode_id}.dai-rediff.json"


def _atomic_write_text(path: pathlib.Path, text: str, *, label: str) -> None:
    """Durably replace one safe output without following aliases."""
    _require_safe_path(path, label=label)
    _require_safe_directory(path.parent, label=f"{label} parent", create=True)
    if path.exists() and not path.is_file():
        raise RediffInputError(f"{label} is not a regular file: {path}")
    temporary: pathlib.Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            temporary = pathlib.Path(handle.name)
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        _require_safe_path(path, label=label)
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)

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


def sha256_regular_file(path: pathlib.Path, *, label: str) -> str:
    """Hash one regular file without following a final-component symlink."""
    _require_safe_path(path, label=label)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise RediffInputError(f"{label} is unavailable or unsafe: {path}") from error
    digest = hashlib.sha256()
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise RediffInputError(f"{label} is not a regular file: {path}")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    return digest.hexdigest()


def read_regular_file(path: pathlib.Path, *, label: str) -> bytes:
    """Read one regular file without following a final-component symlink."""
    _require_safe_path(path, label=label)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise RediffInputError(f"{label} is unavailable or unsafe: {path}") from error
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise RediffInputError(f"{label} is not a regular file: {path}")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            return handle.read()
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _path_has_symlink(path: pathlib.Path, *, base: pathlib.Path) -> bool:
    """Check every existing component below base without resolving it away."""
    current = base
    try:
        relative = path.relative_to(base)
    except ValueError:
        return True
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            return True
    return False


def load_validated_manifest() -> list[dict]:
    """Validate all snapshot identities before any draft is retired or written."""
    _require_safe_path(MANIFEST, label="snapshot manifest")
    _require_safe_path(AUDIO_ROOT, label="corpus audio root")
    if not MANIFEST.is_file():
        raise RediffInputError(f"snapshot manifest is not a regular file: {MANIFEST}")
    try:
        raw = read_regular_file(MANIFEST, label="snapshot manifest")
        rows = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RediffInputError(f"cannot read snapshot manifest {MANIFEST}: {error}") from error
    if not isinstance(rows, list):
        raise RediffInputError("snapshot manifest must contain an array")

    repo_root = REPO.resolve()
    audio_root = AUDIO_ROOT.resolve()
    try:
        audio_root.relative_to(repo_root)
    except ValueError as error:
        raise RediffInputError("corpus audio root escapes the repository") from error

    validated: list[dict] = []
    seen_episode_ids: set[str] = set()
    for index, raw_row in enumerate(rows):
        source = f"snapshot manifest row {index}"
        if not isinstance(raw_row, dict):
            raise RediffInputError(f"{source} must be an object")
        episode_id = raw_row.get("episodeId")
        if (
            not isinstance(episode_id, str)
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,499}", episode_id) is None
            or episode_id != episode_id.strip()
            or pathlib.Path(episode_id).name != episode_id
            or "/" in episode_id
            or "\\" in episode_id
        ):
            raise RediffInputError(f"{source}.episodeId must be a safe non-empty filename stem")
        if episode_id in seen_episode_ids:
            raise RediffInputError(f"snapshot manifest has duplicate episodeId {episode_id!r}")

        expected_sha = raw_row.get("sha256")
        if not isinstance(expected_sha, str) or re.fullmatch(r"[0-9a-f]{64}", expected_sha) is None:
            raise RediffInputError(f"{source}.sha256 must be 64 lowercase hexadecimal characters")
        raw_audio_path = raw_row.get("audioPath")
        if (
            not isinstance(raw_audio_path, str)
            or not raw_audio_path.strip()
            or raw_audio_path != raw_audio_path.strip()
            or "\0" in raw_audio_path
        ):
            raise RediffInputError(f"{source}.audioPath must be a non-empty relative path")
        relative_audio = pathlib.Path(raw_audio_path)
        if relative_audio.is_absolute() or ".." in relative_audio.parts:
            raise RediffInputError(f"{source}.audioPath is unsafe")
        unresolved_audio = repo_root / relative_audio
        if _path_has_symlink(unresolved_audio, base=repo_root):
            raise RediffInputError(f"{source}.audioPath contains a symbolic link")
        resolved_audio = unresolved_audio.resolve()
        try:
            resolved_audio.relative_to(audio_root)
        except ValueError as error:
            raise RediffInputError(f"{source}.audioPath escapes the corpus audio root") from error
        if resolved_audio.stem != episode_id:
            raise RediffInputError(f"{source}.audioPath does not match episodeId {episode_id!r}")

        enclosure_url = raw_row.get("enclosureUrl")
        if not isinstance(enclosure_url, str) or enclosure_url != enclosure_url.strip():
            raise RediffInputError(f"{source}.enclosureUrl must be a trimmed HTTP(S) URL")
        parsed_url = urllib.parse.urlsplit(enclosure_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise RediffInputError(f"{source}.enclosureUrl must be an HTTP(S) URL")

        actual_sha = sha256_regular_file(resolved_audio, label=f"snapshot audio for {episode_id}")
        if actual_sha != expected_sha:
            raise RediffInputError(
                f"snapshot audio fingerprint mismatch for {episode_id}: "
                f"manifest={expected_sha} actual={actual_sha}"
            )
        row = dict(raw_row)
        row["_snapshot_path"] = resolved_audio
        row["_snapshot_sha"] = actual_sha
        validated.append(row)
        seen_episode_ids.add(episode_id)
    return validated


def verify_rediff_inputs_unchanged(
    snapshot_path: pathlib.Path,
    fresh_path: pathlib.Path,
    *,
    episode_id: str,
    expected_snapshot_sha: str,
    expected_fresh_sha: str,
) -> None:
    """Recheck both files after fpcalc so evidence binds the bytes it compared."""
    actual_snapshot_sha = sha256_regular_file(
        snapshot_path, label=f"snapshot audio for {episode_id}"
    )
    if actual_snapshot_sha != expected_snapshot_sha:
        raise RediffInputError(
            f"snapshot audio changed during rediff for {episode_id}: "
            f"expected={expected_snapshot_sha} actual={actual_snapshot_sha}"
        )
    actual_fresh_sha = sha256_regular_file(
        fresh_path, label=f"fresh comparison audio for {episode_id}"
    )
    if actual_fresh_sha != expected_fresh_sha:
        raise RediffInputError(
            f"fresh comparison audio changed during rediff for {episode_id}: "
            f"expected={expected_fresh_sha} actual={actual_fresh_sha}"
        )

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

def gaps_in_a(runs: list[tuple[int, int, int, int]], total_a: int,
              min_gap_fps: int) -> list[tuple[int, int, int, int]]:
    """Return snapshot-A ranges absent from B, in A fingerprint indices."""
    if not runs:
        return [(0, total_a, 0, 0)] if total_a >= min_gap_fps else []
    runs = sorted(runs, key=lambda r: r[0])
    gaps = []
    first = runs[0]
    if first[0] >= min_gap_fps:
        gaps.append((0, first[0], 0, first[2]))
    for left, right in zip(runs, runs[1:]):
        left_end = left[0] + left[2]
        if right[0] - left_end >= min_gap_fps:
            gaps.append((left_end, right[0], left[2], right[2]))
    last = runs[-1]
    tail_start = last[0] + last[2]
    if total_a - tail_start >= min_gap_fps:
        gaps.append((tail_start, total_a, last[2], 0))
    return gaps

def confidence_for_gap(left_run_len: int, right_run_len: int,
                       sec_per_fp: float) -> float:
    flank_sec = min(left_run_len, right_run_len) * sec_per_fp
    return round(1 - math.exp(-flank_sec / 60.0), 3)

# ---------- per-episode rediff ----------

def rediff_pair(snapshot_path: pathlib.Path, fresh_path: pathlib.Path) -> dict:
    fpA, sec_per_fp_A = fpcalc_raw(snapshot_path)
    fpB, sec_per_fp_B = fpcalc_raw(fresh_path)
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
    by_b = sorted(runs, key=lambda run: run[1])
    if any(
        right[0] < left[0] + left[2]
        for left, right in zip(by_b, by_b[1:])
    ):
        return {
            "ok": False,
            "error": "alignment-non-monotonic-a (ambiguous repeated content)",
            "fingerprintsA": len(fpA), "fingerprintsB": len(fpB),
            "secondsPerFpA": round(sec_per_fp_A, 5),
            "secondsPerFpB": round(sec_per_fp_B, 5),
        }
    min_gap_fps = max(1, int(round(MIN_AD_SECONDS / sec_per_fp_A)))
    gaps = gaps_in_a(runs, len(fpA), min_gap_fps)
    ad_slots = []
    for (a_start, a_end, lrun, rrun) in gaps:
        ad_slots.append({
            "startSeconds": round(a_start * sec_per_fp_A, 2),
            "endSeconds": round(a_end * sec_per_fp_A, 2),
            "durationSeconds": round((a_end - a_start) * sec_per_fp_A, 2),
            "confidence": confidence_for_gap(lrun, rrun, sec_per_fp_A),
            "leftRunSeconds": round(lrun * sec_per_fp_A, 2),
            "rightRunSeconds": round(rrun * sec_per_fp_A, 2),
        })
    total_run_sec = sum(r[2] for r in runs) * sec_per_fp_A
    return {
        "ok": True,
        "fingerprintsA": len(fpA), "fingerprintsB": len(fpB),
        "secondsPerFpA": round(sec_per_fp_A, 5),
        "secondsPerFpB": round(sec_per_fp_B, 5),
        "runs": len(runs),
        "alignedSecondsA": round(total_run_sec, 2),
        "adSlots": ad_slots,
    }

def _draft_window_bounds(window: object) -> tuple[float, float]:
    if not isinstance(window, dict):
        raise RediffInputError("rediff draft ad windows must be objects")
    raw_start = window.get("start_seconds")
    raw_end = window.get("end_seconds")
    if (
        isinstance(raw_start, bool)
        or isinstance(raw_end, bool)
        or not isinstance(raw_start, (int, float))
        or not isinstance(raw_end, (int, float))
    ):
        raise RediffInputError("rediff draft ad window bounds must be numbers")
    start = float(raw_start)
    end = float(raw_end)
    if not math.isfinite(start) or not math.isfinite(end) or start < 0 or end <= start:
        raise RediffInputError("rediff draft ad window bounds are invalid")
    return start, end


def _window_comparison_fingerprint(
    window: dict, comparison_fingerprints: list[str]
) -> str:
    """Resolve an interval to one exact B asset, including legacy single-B drafts."""
    fingerprint = window.get("comparison_audio_fingerprint")
    if fingerprint is None and len(comparison_fingerprints) == 1:
        fingerprint = comparison_fingerprints[0]
    if (
        not isinstance(fingerprint, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", fingerprint) is None
        or fingerprint not in comparison_fingerprints
    ):
        raise RediffInputError(
            "prior rediff draft has an interval without an exact comparison fingerprint"
        )
    return fingerprint


def load_reusable_draft(episode_id: str, expected_snapshot_sha: str) -> Optional[dict]:
    """Keep only prior evidence already bound to the same retained A bytes."""
    _require_safe_path(DRAFTS, label="rediff drafts directory")
    if not DRAFTS.exists():
        return None
    _require_safe_directory(DRAFTS, label="rediff drafts directory")
    path = _draft_path(episode_id)
    _require_safe_path(path, label="prior rediff draft")
    if not path.exists() and not path.is_symlink():
        return None
    try:
        if not path.is_file():
            raise RediffInputError("prior rediff draft is not a regular file")
        document = json.loads(
            read_regular_file(path, label="prior rediff draft").decode("utf-8")
        )
        if not isinstance(document, dict):
            raise RediffInputError("prior rediff draft is not an object")
        if document.get("episode_id") != episode_id:
            raise RediffInputError("prior rediff draft has the wrong episode identity")
        if document.get("coordinate_space") != "snapshot_a":
            raise RediffInputError("prior rediff draft is not in snapshot-A coordinates")
        if document.get("audio_fingerprint") != f"sha256:{expected_snapshot_sha}":
            raise RediffInputError("prior rediff draft targets different audio bytes")
        comparison_fingerprints = document.get("comparison_audio_fingerprints")
        if (
            not isinstance(comparison_fingerprints, list)
            or not comparison_fingerprints
            or not all(
                isinstance(value, str)
                and re.fullmatch(r"sha256:[0-9a-f]{64}", value)
                for value in comparison_fingerprints
            )
            or len(set(comparison_fingerprints)) != len(comparison_fingerprints)
        ):
            raise RediffInputError("prior rediff draft has invalid comparison fingerprints")
        if f"sha256:{expected_snapshot_sha}" in comparison_fingerprints:
            raise RediffInputError("prior rediff draft reuses retained A as comparison B")
        single_comparison = document.get("comparison_audio_fingerprint")
        if (
            not isinstance(single_comparison, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", single_comparison) is None
            or single_comparison not in comparison_fingerprints
        ):
            raise RediffInputError("prior rediff draft has an invalid comparison fingerprint")
        windows = document.get("ad_windows")
        if not isinstance(windows, list):
            raise RediffInputError("prior rediff draft lacks an ad_windows array")
        normalized_windows: list[dict] = []
        for raw_window in windows:
            _draft_window_bounds(raw_window)
            assert isinstance(raw_window, dict)
            window = dict(raw_window)
            comparison = _window_comparison_fingerprint(
                window, comparison_fingerprints
            )
            window["comparison_audio_fingerprint"] = comparison
            normalized_windows.append(window)
        normalized = dict(document)
        normalized["ad_windows"] = normalized_windows
        return normalized
    except (OSError, UnicodeError, json.JSONDecodeError, RediffInputError) as error:
        retire_stale_draft(episode_id)
        print(f"  [RETIRED] {episode_id}: {error}")
        return None


def merge_draft_windows(existing: list[dict], additions: list[dict]) -> list[dict]:
    """Union A gaps only within one exact comparison-B asset."""
    grouped: dict[str, list[tuple[float, float, dict]]] = {}
    for window in [*existing, *additions]:
        start, end = _draft_window_bounds(window)
        comparison = window.get("comparison_audio_fingerprint")
        if (
            not isinstance(comparison, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", comparison) is None
        ):
            raise RediffInputError(
                "rediff draft interval lacks an exact comparison fingerprint"
            )
        grouped.setdefault(comparison, []).append((start, end, dict(window)))

    merged: list[dict] = []
    for comparison in sorted(grouped):
        comparison_windows: list[dict] = []
        for start, end, window in sorted(
            grouped[comparison], key=lambda item: (item[0], item[1])
        ):
            if not comparison_windows:
                comparison_windows.append(window)
                continue
            previous_start, previous_end = _draft_window_bounds(
                comparison_windows[-1]
            )
            if start > previous_end:
                comparison_windows.append(window)
                continue
            notes = [
                value
                for value in (
                    comparison_windows[-1].get("confidence_notes"),
                    window.get("confidence_notes"),
                )
                if isinstance(value, str) and value
            ]
            comparison_windows[-1]["start_seconds"] = round(
                min(previous_start, start), 2
            )
            comparison_windows[-1]["end_seconds"] = round(
                max(previous_end, end), 2
            )
            comparison_windows[-1]["confidence_notes"] = " | ".join(
                dict.fromkeys(notes)
            )
        merged.extend(comparison_windows)
    return sorted(
        merged,
        key=lambda window: (
            *_draft_window_bounds(window),
            window["comparison_audio_fingerprint"],
        ),
    )


def write_draft(episode_id: str, snapshot_path: pathlib.Path,
                ad_slots: list[dict], fresh_sha: str,
                expected_snapshot_sha: str,
                prior_draft: Optional[dict] = None) -> pathlib.Path:
    """Write A-coordinate evidence only while A still has its validated bytes."""
    if re.fullmatch(r"[0-9a-f]{64}", expected_snapshot_sha) is None:
        raise RediffInputError("expected snapshot fingerprint is not canonical sha256")
    if re.fullmatch(r"[0-9a-f]{64}", fresh_sha) is None:
        raise RediffInputError("fresh comparison fingerprint is not canonical sha256")
    if fresh_sha == expected_snapshot_sha:
        raise RediffInputError("fresh comparison B must differ from retained snapshot A")
    actual_snapshot_sha = sha256_regular_file(
        snapshot_path, label=f"snapshot audio for {episode_id}"
    )
    if actual_snapshot_sha != expected_snapshot_sha:
        raise RediffInputError(
            f"snapshot audio changed during rediff for {episode_id}: "
            f"expected={expected_snapshot_sha} actual={actual_snapshot_sha}"
        )
    _require_safe_directory(DRAFTS, label="rediff drafts directory", create=True)
    out = _draft_path(episode_id)
    _require_safe_path(out, label="rediff draft output")
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
            "comparison_audio_fingerprint": f"sha256:{fresh_sha}",
            "confidence_notes": (
                f"DRAFT chromaprint DAI rediff; alignment confidence={slot['confidence']}, "
                f"flanking-run-seconds=(left={slot['leftRunSeconds']}, right={slot['rightRunSeconds']}). "
                f"A-coordinate range removed from the retained snapshot in a fresh download "
                f"(fresh comparison sha256={fresh_sha[:12]}). Verify against retained snapshot audio."
            ),
        })
    prior_windows = prior_draft.get("ad_windows", []) if prior_draft else []
    comparison_fingerprints = set(
        prior_draft.get("comparison_audio_fingerprints", []) if prior_draft else []
    )
    if prior_draft and isinstance(prior_draft.get("comparison_audio_fingerprint"), str):
        comparison_fingerprints.add(prior_draft["comparison_audio_fingerprint"])
    comparison_fingerprints.add(f"sha256:{fresh_sha}")
    if f"sha256:{expected_snapshot_sha}" in comparison_fingerprints:
        raise RediffInputError("rediff evidence reuses retained A as comparison B")
    payload = {
        "episode_id": episode_id,
        "show_name": episode_id.replace("-", " ").title(),
        "ad_windows": merge_draft_windows(prior_windows, ad_windows),
        "audio_fingerprint": f"sha256:{expected_snapshot_sha}",
        "comparison_audio_fingerprint": f"sha256:{fresh_sha}",
        "comparison_audio_fingerprints": sorted(comparison_fingerprints),
        "coordinate_space": "snapshot_a",
        "variant_of": None,
        "source": "l2f-dai-rediff.py",
        "draft_kind": "dai-rediff",
    }
    temporary: Optional[pathlib.Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=DRAFTS, prefix=f".{out.name}.", delete=False
        ) as handle:
            temporary = pathlib.Path(handle.name)
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        _require_safe_path(out, label="rediff draft output")
        os.replace(temporary, out)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
    return out

def retire_stale_draft(episode_id: str) -> bool:
    """Remove prior rediff evidence before a real comparison supersedes it."""
    _require_safe_path(DRAFTS, label="rediff drafts directory")
    if not DRAFTS.exists():
        return False
    _require_safe_directory(DRAFTS, label="rediff drafts directory")
    path = _draft_path(episode_id)
    _require_safe_path(path, label="prior rediff draft")
    if not path.exists() and not path.is_symlink():
        return False
    if not path.is_file():
        raise RediffInputError(f"prior rediff draft is not a regular file: {path}")
    path.unlink()
    return True

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
    _require_safe_path(MANIFEST, label="snapshot manifest")
    manifest = (
        json.loads(read_regular_file(MANIFEST, label="snapshot manifest"))
        if MANIFEST.exists()
        else []
    )
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
    A = tmp / "synthetic_A.mp3"      # B plus old 30s ad retained at t=120s
    B = tmp / "synthetic_B.mp3"      # fresh comparison without the old ad
    try:
        ffmpeg_slice(src, B, 0, 300)
        # Build A in a single ffmpeg pass so MP3 frame boundaries on either side
        # of the splice stay phase-aligned with A (otherwise -ss seeks across
        # the splice shift chromaprint fingerprints by 8-16 bits on the
        # post-splice section, fragmenting alignment).
        ffmpeg_splice_with_noise(B, A, insert_at=120.0, insert_duration=30.0,
                                 a_duration=300.0)
        result = rediff_pair(A, B)
        print("self-test rediff result:", json.dumps(result, indent=2))
        if not result.get("ok"):
            print(f"SELF-TEST FAIL: rediff_pair not ok: {result.get('error')}", file=sys.stderr)
            return 3
        for slot in result["adSlots"]:
            if 115 <= slot["startSeconds"] <= 125 and 145 <= slot["endSeconds"] <= 155:
                print(f"SELF-TEST PASS: detected retained-A removal at "
                      f"{slot['startSeconds']}-{slot['endSeconds']}s")
                return 0
        print("SELF-TEST FAIL: no retained-A slot matched [115,125]×[145,155].",
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
                    help="Splice a synthetic old ad into A and verify its A-coordinate removal.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Skip re-download; assume <audioPath>.fresh.mp3 already exists. Useful for re-running alignment.")
    ap.add_argument("--episode", action="append",
                    help="Only process episode IDs containing this substring (repeatable).")
    ap.add_argument("--retain-audio", action="store_true",
                    help="On rotated episodes, keep the re-downloaded B-side audio at "
                         "<audioPath>.fresh.mp3 (the path --dry-run reads and the Swift rediff "
                         "treatment harness feeds as the B-side) instead of deleting it. "
                         "playhead-xsdz.36.1.")
    args = ap.parse_args()

    if args.self_test:
        sys.exit(self_test())

    try:
        _require_safe_path(DRAFTS, label="rediff drafts directory")
        _require_safe_path(DIAG_OUT, label="rediff diagnostics output")
        manifest = load_validated_manifest()
    except RediffInputError as error:
        print(f"ERROR: refusing rediff with invalid snapshot inputs: {error}", file=sys.stderr)
        sys.exit(2)
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
        snapshot_path = entry["_snapshot_path"]
        manifest_sha = entry["_snapshot_sha"]
        prior_draft = load_reusable_draft(ep, manifest_sha)
        record = {"episodeId": ep, "rotated": False, "adSlots": []}

        if args.dry_run:
            # Look for a pre-staged fresh file alongside the snapshot.
            fresh_path = fresh_sibling(snapshot_path)
            if not fresh_path.exists():
                record["ok"] = False
                record["error"] = (
                    f"dry-run: expected pre-staged fresh audio at "
                    f"{fresh_path.relative_to(REPO)} (did not find)"
                )
                failed_count += 1
                print(f"  [SKIP] {ep}: {record['error']}")
                per_episode.append(record); continue
            fresh_sha = sha256_regular_file(
                fresh_path, label=f"fresh comparison audio for {ep}"
            )
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
            fresh_sha = sha256_regular_file(
                fresh_path, label=f"fresh comparison audio for {ep}"
            )

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
            verify_rediff_inputs_unchanged(
                snapshot_path,
                fresh_path,
                episode_id=ep,
                expected_snapshot_sha=manifest_sha,
                expected_fresh_sha=fresh_sha,
            )
        except (RuntimeError, RediffInputError) as e:
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
                "alignedSecondsA": diff["alignedSecondsA"],
            }
            try:
                # A distinct, successfully aligned B asset remains evidence even
                # when it contributes no retained-A gaps. Persist it so a later
                # comparison can retain the complete cumulative comparison set.
                draft_path = write_draft(
                    ep,
                    snapshot_path,
                    diff["adSlots"],
                    fresh_sha,
                    manifest_sha,
                    prior_draft,
                )
            except RediffInputError as error:
                record["ok"] = False
                record["error"] = f"rediff publication failed: {error}"
                failed_count += 1
                print(f"  [FAIL] {ep}: {error}")
                if not args.dry_run:
                    try: fresh_path.unlink()
                    except Exception: pass
                per_episode.append(record)
                continue
            record["draftPath"] = str(draft_path.relative_to(REPO))
            drafted_count += 1
            if diff["adSlots"]:
                print(f"  [DRAFT] wrote {draft_path.relative_to(REPO)} ({len(diff['adSlots'])} ad slots)")
            else:
                print(
                    f"  [EVIDENCE] wrote {draft_path.relative_to(REPO)} "
                    f"(no retained-A gaps ≥{MIN_AD_SECONDS}s)"
                )
            rotated_count += 1
        if not args.dry_run:
            if args.retain_audio and record.get("rotated") and record.get("ok"):
                retained = fresh_sibling(snapshot_path)
                try:
                    os.replace(fresh_path, retained)
                    record["retainedFreshPath"] = str(retained.relative_to(REPO))
                    print(f"  [RETAIN] {ep}: kept B-side audio at {retained.relative_to(REPO)}")
                except Exception as exc:
                    print(f"  [WARN] {ep}: could not retain fresh audio ({exc}); deleting")
                    try: fresh_path.unlink()
                    except Exception: pass
            else:
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
    _atomic_write_text(
        DIAG_OUT,
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        label="rediff diagnostics output",
    )
    print(f"\n{'='*60}")
    print(f"rediff complete: {rotated_count} rotated, {unchanged_count} unchanged, "
          f"{failed_count} failed, {drafted_count} drafts written")
    print(f"diagnostics: {DIAG_OUT.relative_to(REPO)}")
    # Honest exit code: 0 if nothing broke (rotations are not errors).
    sys.exit(0 if failed_count == 0 else 1)


if __name__ == "__main__":
    main()
