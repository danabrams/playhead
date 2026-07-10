#!/usr/bin/env python3
"""
l2f-daysgap-rediff.py — +Nd re-fetch + diff for the DAI days-gap rotation
measurement (playhead-xsdz.30).

Pairs with the t0 capture (scratchpad/daysgap_capture.py, whose durable output
is playhead-dogfood-diagnostics-daysgap-t0-<UTCdate>.json at the repo root).

WHY THIS EXISTS (and why not scripts/l2f-dai-rediff.py directly):
  The reference differ scripts/l2f-dai-rediff.py re-downloads the fresh
  enclosure and aligns it against the *snapshot AUDIO file on disk*. The
  days-gap measurement runs under a hard disk constraint and DELETES each t0
  audio immediately, keeping only the (tiny) chromaprint fingerprint. So the
  t0 side of the alignment is a stored integer fingerprint, not an audio file.
  This tool loads the stored t0 fingerprint (fpA) from the t0 manifest,
  re-downloads the fresh enclosure, fpcalc's it (fpB), and runs the EXACT SAME
  alignment functions imported from scripts/l2f-dai-rediff.py
  (find_runs / merge_runs / gaps_in_b / confidence_for_gap). Identical
  algorithm, fingerprint-native inputs.

WHAT IT MEASURES:
  For each t0 episode, at whatever wall-clock gap has elapsed:
    * byte rotation:        fresh sha256 != t0 sha256  (cheap necessary signal)
    * fingerprint rotation: alignment finds an inserted-in-B or removed-in-A
                            segment >= MIN_AD_SECONDS (the audio actually
                            changed, not just a re-mux / tracking-token swap)
  It reports rotation PER SLOT (each changed ad window with boundaries +
  confidence) and aggregates the rotation RATE by gap bucket with honest N and
  Wilson 95% confidence intervals. This is the production-timescale days-gap
  number the spike left open (spike: 0/10 back-to-back, 2/10 @~65min,
  8/10 @~5wk; days-gap bracketed 20-88%).

Modes:
  scripts/l2f-daysgap-rediff.py                 # diff every episode in the newest t0 manifest
  scripts/l2f-daysgap-rediff.py --t0 <path>     # explicit t0 manifest path
  scripts/l2f-daysgap-rediff.py --episode <sub> # only episodeIds containing <sub> (repeatable)
  scripts/l2f-daysgap-rediff.py --self-test     # synthetic fingerprint align check (no net, no fpcalc)

Output:
  playhead-dogfood-diagnostics-daysgap-diff-<UTCdate>.json  (repo root, gitignored)

Stdlib + curl + fpcalc only. Polite: sequential, one audio on disk at a time,
audio deleted immediately, STOP if free disk < 3 GB.
"""
import argparse
import datetime as dt
import glob
import hashlib
import importlib.util
import json
import math
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

REPO = pathlib.Path(__file__).resolve().parents[1]
FPCALC = "/opt/homebrew/bin/fpcalc"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Podcast/1.0 (+playhead dogfood research)"
DL_TIMEOUT = 300
MIN_AUDIO_BYTES = 100_000
DISK_STOP_BYTES = 3 * 1024**3

# ---- import the reference alignment core (single source of truth) ----
_ref_path = REPO / "scripts" / "l2f-dai-rediff.py"
_spec = importlib.util.spec_from_file_location("l2f_dai_rediff", str(_ref_path))
_ref = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ref)
find_runs = _ref.find_runs
merge_runs = _ref.merge_runs
gaps_in_b = _ref.gaps_in_b
confidence_for_gap = _ref.confidence_for_gap
MIN_AD_SECONDS = _ref.MIN_AD_SECONDS


def free_bytes():
    return shutil.disk_usage(str(REPO)).free


def wilson_ci(k, n, z=1.96):
    """Wilson score 95% CI for a binomial proportion. Returns (lo, hi, phat)."""
    if n == 0:
        return (0.0, 0.0, 0.0)
    phat = k / n
    denom = 1 + z * z / n
    center = (phat + z * z / (2 * n)) / denom
    half = (z * math.sqrt(phat * (1 - phat) / n + z * z / (4 * n * n))) / denom
    return (round(max(0.0, center - half), 4), round(min(1.0, center + half), 4), round(phat, 4))


def latest_t0_manifest():
    cands = sorted(glob.glob(str(REPO / "playhead-dogfood-diagnostics-daysgap-t0-*.json")))
    return pathlib.Path(cands[-1]) if cands else None


def load_fpA(entry):
    """Prefer inline fingerprint; fall back to the per-episode fingerprint file."""
    fp = entry.get("fingerprint")
    if isinstance(fp, list) and len(fp) >= 10:
        return [int(x) & 0xFFFFFFFF for x in fp]
    ff = entry.get("fingerprintFile")
    if ff:
        p = REPO / ff
        if p.exists():
            obj = json.loads(p.read_text())
            return [int(x) & 0xFFFFFFFF for x in obj["fingerprint"]]
    raise RuntimeError("no t0 fingerprint available (inline missing and fingerprintFile absent)")


def fpcalc_raw(path):
    p = subprocess.run([FPCALC, "-json", "-raw", "-length", "0", str(path)],
                       capture_output=True, timeout=300)
    if p.returncode != 0:
        raise RuntimeError(f"fpcalc rc={p.returncode}: {p.stderr.decode('utf-8','ignore')[:160]}")
    obj = json.loads(p.stdout.decode("utf-8", "ignore"))
    fp = obj.get("fingerprint")
    dur = float(obj.get("duration") or 0.0)
    if not isinstance(fp, list) or len(fp) < 10 or dur <= 0:
        raise RuntimeError("fpcalc returned empty fingerprint")
    return [int(x) & 0xFFFFFFFF for x in fp], dur / len(fp), dur


def download(enc_url, dest):
    hdr = dest.with_suffix(".hdr")
    cmd = ["curl", "-sSL", "-A", UA, "--max-time", str(DL_TIMEOUT),
           "-D", str(hdr), "-o", str(dest),
           "-w", "%{http_code}\t%{url_effective}\t%{size_download}", enc_url]
    p = subprocess.run(cmd, capture_output=True, timeout=DL_TIMEOUT + 30)
    out = p.stdout.decode("utf-8", "ignore").strip().split("\t")
    http_code = out[0] if out else "?"
    eff = out[1] if len(out) > 1 else enc_url
    etag = last_mod = None
    if hdr.exists():
        h = hdr.read_text("utf-8", "ignore")
        for m in re.finditer(r"(?im)^ETag:\s*(.+?)\s*$", h):
            etag = m.group(1)
        for m in re.finditer(r"(?im)^Last-Modified:\s*(.+?)\s*$", h):
            last_mod = m.group(1)
        try: hdr.unlink()
        except Exception: pass
    if p.returncode != 0 or not dest.exists() or dest.stat().st_size < MIN_AUDIO_BYTES:
        sz = dest.stat().st_size if dest.exists() else 0
        return None, f"download failed rc={p.returncode} http={http_code} size={sz}", eff, etag, last_mod
    h = hashlib.sha256()
    with open(dest, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return {"sha256": h.hexdigest(), "bytes": dest.stat().st_size,
            "httpCode": http_code}, None, eff, etag, last_mod


def gaps_in_a_from_runs(runs, total_a, min_gap_fps):
    """Complement of the runs' A-coverage: ranges in A (the t0 fingerprint) not
    covered by any aligned run = REMOVED segments (old ad fills no longer present).
    Derived from the same merged runs, no second alignment pass."""
    if not runs:
        return [(0, total_a)] if total_a >= min_gap_fps else []
    iv = sorted((r[0], r[0] + r[2]) for r in runs)  # (a_start, a_end)
    gaps = []
    cur = 0
    for s, e in iv:
        if s - cur >= min_gap_fps:
            gaps.append((cur, s))
        cur = max(cur, e)
    if total_a - cur >= min_gap_fps:
        gaps.append((cur, total_a))
    return gaps


def diff_episode(entry, tmpdir):
    ep = entry["episodeId"]
    rec = {"episodeId": ep, "show": entry.get("show"), "showSlug": entry.get("showSlug"),
           "cdnHostFinal": entry.get("cdnHostFinal"), "cdnHostFirstHop": entry.get("cdnHostFirstHop")}
    # elapsed gap
    try:
        t0 = dt.datetime.fromisoformat(entry["t0FetchIso"].replace("Z", "+00:00"))
        now = dt.datetime.now(dt.timezone.utc)
        rec["elapsedDays"] = round((now - t0).total_seconds() / 86400.0, 3)
    except Exception:
        rec["elapsedDays"] = None

    dest = tmpdir / "fresh.mp3"
    if dest.exists():
        try: dest.unlink()
        except Exception: pass
    dl, err, eff, etag, last_mod = download(entry["enclosureUrl"], dest)
    fresh_bytes = dest.stat().st_size if dest.exists() else 0
    if err:
        try:
            if dest.exists(): dest.unlink()
        except Exception: pass
        rec.update({"ok": False, "error": err, "rotatedBytes": None,
                    "fingerprintRotated": None, "insertedSlots": [], "removedSlots": []})
        return rec, 0

    rec["freshSha256"] = dl["sha256"]
    rec["t0Sha256"] = entry["sha256"]
    rec["freshBytes"] = dl["bytes"]
    rec["t0Bytes"] = entry.get("contentLengthBytes")
    rec["freshEtag"] = etag
    rec["freshLastModified"] = last_mod
    rec["rotatedBytes"] = (dl["sha256"] != entry["sha256"])

    if not rec["rotatedBytes"]:
        rec.update({"ok": True, "note": "identical bytes — no rotation at this gap",
                    "fingerprintRotated": False, "insertedSlots": [], "removedSlots": []})
        try: dest.unlink()
        except Exception: pass
        return rec, dl["bytes"]

    # bytes differ -> fingerprint align to confirm/locate audio rotation
    try:
        fpA = load_fpA(entry)
        fpB, sec_per_fp_B, durB = fpcalc_raw(dest)
    except RuntimeError as e:
        try: dest.unlink()
        except Exception: pass
        rec.update({"ok": False, "error": f"fingerprint step failed: {e}",
                    "fingerprintRotated": None, "insertedSlots": [], "removedSlots": []})
        return rec, dl["bytes"]
    try: dest.unlink()
    except Exception: pass

    runs = merge_runs(find_runs(fpA, fpB))
    min_gap_fps = max(1, int(round(MIN_AD_SECONDS / sec_per_fp_B)))
    sec_per_fp_A = entry.get("secondsPerFp", sec_per_fp_B)

    inserted = []
    for (bs, be, lrun, rrun) in gaps_in_b(runs, len(fpB), min_gap_fps):
        inserted.append({
            "startSeconds": round(bs * sec_per_fp_B, 2),
            "endSeconds": round(be * sec_per_fp_B, 2),
            "durationSeconds": round((be - bs) * sec_per_fp_B, 2),
            "confidence": confidence_for_gap(lrun, rrun, sec_per_fp_B),
            "leftRunSeconds": round(lrun * sec_per_fp_B, 2),
            "rightRunSeconds": round(rrun * sec_per_fp_B, 2),
        })
    removed = []
    for (as_, ae) in gaps_in_a_from_runs(runs, len(fpA), max(1, int(round(MIN_AD_SECONDS / sec_per_fp_A)))):
        removed.append({
            "startSeconds": round(as_ * sec_per_fp_A, 2),
            "endSeconds": round(ae * sec_per_fp_A, 2),
            "durationSeconds": round((ae - as_) * sec_per_fp_A, 2),
        })
    rec["insertedSlots"] = inserted
    rec["removedSlots"] = removed
    rec["alignment"] = {
        "fpA": len(fpA), "fpB": len(fpB),
        "runs": len(runs),
        "alignedSecondsB": round(sum(r[2] for r in runs) * sec_per_fp_B, 2),
        "secondsPerFpA": round(sec_per_fp_A, 6), "secondsPerFpB": round(sec_per_fp_B, 6),
    }
    rec["fingerprintRotated"] = bool(inserted or removed)
    rec["ok"] = True
    return rec, dl["bytes"]


def self_test():
    import random
    rng = random.Random(1234)
    n = 4000
    base = [rng.getrandbits(32) for _ in range(n)]

    # Case 1: insertion of a 300-fp block (~38s at 0.126 s/fp) at index 1500.
    ins = [rng.getrandbits(32) for _ in range(300)]
    fpB = base[:1500] + ins + base[1500:]
    sec = 0.126
    runs = merge_runs(find_runs(base, fpB))
    min_gap = max(1, int(round(MIN_AD_SECONDS / sec)))
    ins_gaps = gaps_in_b(runs, len(fpB), min_gap)
    ok_ins = any(1500 * sec - 5 <= g[0] * sec <= 1500 * sec + 5 and
                 abs((g[1] - g[0]) - 300) <= 20 for g in ins_gaps)

    # Case 2: removal of a 300-fp block from base (fpB shorter) -> removed-in-A.
    fpB2 = base[:1500] + base[1800:]
    runs2 = merge_runs(find_runs(base, fpB2))
    rem_gaps = gaps_in_a_from_runs(runs2, len(base), min_gap)
    ok_rem = any(1500 - 20 <= g[0] <= 1500 + 20 and abs((g[1] - g[0]) - 300) <= 20 for g in rem_gaps)

    # Case 3: identical -> no gaps.
    runs3 = merge_runs(find_runs(base, base))
    ok_same = (len(gaps_in_b(runs3, len(base), min_gap)) == 0 and
               len(gaps_in_a_from_runs(runs3, len(base), min_gap)) == 0)

    print(f"self-test insertion-detected={ok_ins} removal-detected={ok_rem} identical-clean={ok_same}")
    print(f"  inserted gaps (fp idx): {ins_gaps}")
    print(f"  removed  gaps (fp idx): {rem_gaps}")
    if ok_ins and ok_rem and ok_same:
        print("SELF-TEST PASS (reuses l2f-dai-rediff.py alignment core on fingerprint inputs)")
        return 0
    print("SELF-TEST FAIL", file=sys.stderr)
    return 1


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--t0", help="t0 manifest path (default: newest daysgap-t0-*.json at repo root)")
    ap.add_argument("--episode", action="append", help="only episodeIds containing this substring (repeatable)")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()

    if args.self_test:
        sys.exit(self_test())

    t0_path = pathlib.Path(args.t0) if args.t0 else latest_t0_manifest()
    if not t0_path or not t0_path.exists():
        print("ERROR: no t0 manifest found (expected playhead-dogfood-diagnostics-daysgap-t0-*.json)", file=sys.stderr)
        sys.exit(2)
    t0 = json.loads(t0_path.read_text())
    episodes = t0["episodes"]
    if args.episode:
        episodes = [e for e in episodes if any(s in e["episodeId"] for s in args.episode)]
    if not episodes:
        print("ERROR: zero episodes to diff", file=sys.stderr); sys.exit(2)

    tmpdir = pathlib.Path(tempfile.mkdtemp(prefix="daysgap-rediff-"))
    records = []
    total_bytes = 0
    started = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    try:
        for i, e in enumerate(episodes, 1):
            if free_bytes() < DISK_STOP_BYTES:
                print(f"*** DISK GUARD: free < 3GB — stopping after {i-1} episodes ***")
                break
            print(f"[{i}/{len(episodes)}] {e['episodeId']}")
            rec, b = diff_episode(e, tmpdir)
            total_bytes += b
            tag = ("BYTES-SAME" if rec.get("rotatedBytes") is False else
                   "ROTATED-FP" if rec.get("fingerprintRotated") else
                   "ROTATED-BYTES-ONLY" if rec.get("rotatedBytes") else
                   f"ERROR:{rec.get('error')}")
            print(f"    {tag}  gap={rec.get('elapsedDays')}d "
                  f"inserted={len(rec.get('insertedSlots', []))} removed={len(rec.get('removedSlots', []))}")
            records.append(rec)
    finally:
        for p in tmpdir.glob("*"):
            try: p.unlink()
            except Exception: pass
        try: tmpdir.rmdir()
        except Exception: pass

    # ---- aggregate: rotation rate by gap bucket, honest N + Wilson CI ----
    fetched = [r for r in records if r.get("rotatedBytes") is not None]
    def bucket(days):
        if days is None: return "unknown"
        if days < 1: return "<1d"
        if days <= 3: return "1-3d"
        if days <= 8: return "4-8d"
        if days <= 21: return "9-21d"
        return ">21d"
    buckets = {}
    for r in fetched:
        bk = bucket(r.get("elapsedDays"))
        b = buckets.setdefault(bk, {"n": 0, "byteRot": 0, "fpRot": 0, "episodes": []})
        b["n"] += 1
        b["byteRot"] += 1 if r.get("rotatedBytes") else 0
        b["fpRot"] += 1 if r.get("fingerprintRotated") else 0
        b["episodes"].append(r["episodeId"])
    for bk, b in buckets.items():
        lo, hi, ph = wilson_ci(b["byteRot"], b["n"])
        b["byteRotationRate"] = ph
        b["byteRotationWilson95"] = [lo, hi]
        lo2, hi2, ph2 = wilson_ci(b["fpRot"], b["n"])
        b["fingerprintRotationRate"] = ph2
        b["fingerprintRotationWilson95"] = [lo2, hi2]

    n = len(fetched)
    kb = sum(1 for r in fetched if r.get("rotatedBytes"))
    kf = sum(1 for r in fetched if r.get("fingerprintRotated"))
    blo, bhi, bph = wilson_ci(kb, n)
    flo, fhi, fph = wilson_ci(kf, n)

    out = {
        "schemaVersion": 1,
        "measurement": "dai-daysgap-rotation-diff",
        "bead": "playhead-xsdz.30",
        "tool": "scripts/l2f-daysgap-rediff.py",
        "t0ManifestPath": str(t0_path.resolve().relative_to(REPO)),  # resolve() first: --t0 may be a relative path
        "t0UtcDate": t0.get("t0UtcDate"),
        "startedIso": started,
        "completedIso": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "totalBytesFetched": total_bytes,
        "overall": {
            "episodesFetched": n,
            "byteRotated": kb, "byteRotationRate": bph, "byteRotationWilson95": [blo, bhi],
            "fingerprintRotated": kf, "fingerprintRotationRate": fph, "fingerprintRotationWilson95": [flo, fhi],
        },
        "byGapBucket": buckets,
        "episodes": records,
    }
    date = dt.datetime.utcnow().strftime("%Y-%m-%d")
    out_path = REPO / f"playhead-dogfood-diagnostics-daysgap-diff-{date}.json"
    out_path.write_text(json.dumps(out, indent=1))
    print("=" * 64)
    print(f"days-gap diff complete: {n} fetched, byte-rotated {kb}/{n} "
          f"(Wilson95 {blo}-{bhi}), fingerprint-rotated {kf}/{n} (Wilson95 {flo}-{fhi})")
    for bk, b in sorted(buckets.items()):
        print(f"  gap {bk:6} n={b['n']:2} byteRot={b['byteRot']}/{b['n']} "
              f"fpRot={b['fpRot']}/{b['n']} (fp Wilson95 {b['fingerprintRotationWilson95']})")
    print(f"output: {out_path.relative_to(REPO)}")
    sys.exit(0)


if __name__ == "__main__":
    main()
