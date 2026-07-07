#!/usr/bin/env python3
"""
l2f-earaudit.py — interactive ear-audit of rediff-only auto-promoted ad spans
(playhead-xsdz.31, the independent ground-truth ruler).

The rediff-only R3 spans (audit_priority=1) come from a SINGLE signal and are
"high-precision in theory, unverified in practice". This tool plays each span
with ffplay and asks you to judge it, then reports rediff PRECISION with a
Wilson 95% CI — the independent ruler the activation flip must gate on
(NOT the circular rediff-vs-rediff coverage number).

USAGE (run at your terminal; needs speakers/headphones):
    python3 scripts/l2f-earaudit.py                 # all spans, resumes progress
    python3 scripts/l2f-earaudit.py --limit 25      # a ~30-min session
    python3 scripts/l2f-earaudit.py --shuffle        # stratified-ish random order (seeded)
    python3 scripts/l2f-earaudit.py --episode morbid # only episodes matching a substring
    python3 scripts/l2f-earaudit.py --summary        # just print the running precision + quit

Per span, ffplay plays [start, end]; then press:
    a = AD (correct promotion)            c = CONTENT (FALSE promotion — hurts precision)
    b = ad but BOUNDARY off (correct ad, note the edges)
    r = replay        p = play 10s of LEAD-IN before the span (context)
    s = skip (undecided)                   q = save + quit

Results append to playhead-rediff-earaudit-results.jsonl (resumable). CONTENT
verdicts are ALSO written to the existing reject log via the documented flag
tool convention so the corpus self-heals.
"""
from __future__ import annotations

import argparse
import json
import math
import pathlib
import random
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
ANN_DIR = ROOT / "TestFixtures/Corpus/Annotations"
AUDIO_DIR = ROOT / "TestFixtures/Corpus/Audio"
MANIFEST = ROOT / "TestFixtures/Corpus/Snapshots/manifest.json"
RESULTS = ROOT / "playhead-rediff-earaudit-results.jsonl"


def load_spans() -> list[dict]:
    show_by_eid: dict[str, str] = {}
    if MANIFEST.exists():
        try:
            for m in json.loads(MANIFEST.read_text()):
                show_by_eid[m.get("episodeId", "")] = m.get("show", "?")
        except Exception:
            pass
    spans: list[dict] = []
    for ann_path in sorted(ANN_DIR.glob("*.json")):
        try:
            ann = json.loads(ann_path.read_text())
        except Exception:
            continue
        eid = ann.get("episodeId") or ann_path.stem
        for w in (ann.get("adWindows") or ann.get("ad_windows") or []):
            if w.get("audit_priority") != 1:
                continue
            start = float(w.get("startSeconds", w.get("start", 0.0)))
            end = float(w.get("endSeconds", w.get("end", 0.0)))
            if end <= start:
                continue
            spans.append({
                "episodeId": eid,
                "show": show_by_eid.get(eid, "?"),
                "start": start,
                "end": end,
                "audioPath": w.get("audioPath") or f"{eid}.mp3",
            })
    return spans


def load_done() -> set[tuple[str, float]]:
    done: set[tuple[str, float]] = set()
    if RESULTS.exists():
        for line in RESULTS.read_text().splitlines():
            try:
                r = json.loads(line)
                if r.get("verdict") in ("ad", "content", "boundary"):
                    done.add((r["episodeId"], round(float(r["start"]), 1)))
            except Exception:
                pass
    return done


def audio_file(span: dict) -> pathlib.Path | None:
    # try the exact filename, then a stem match
    direct = AUDIO_DIR / pathlib.Path(span["audioPath"]).name
    if direct.exists():
        return direct
    for p in AUDIO_DIR.glob("*.mp3"):
        if p.stem.startswith(span["episodeId"][:40]):
            return p
    return None


def play(path: pathlib.Path, start: float, dur: float) -> None:
    subprocess.run(
        ["ffplay", "-nodisp", "-autoexit", "-loglevel", "error",
         "-ss", f"{start:.2f}", "-t", f"{dur:.2f}", str(path)],
        check=False,
    )


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float, float]:
    if n == 0:
        return (0.0, 0.0, 1.0)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (p, max(0.0, c - h), min(1.0, c + h))


def summary() -> None:
    ad = content = boundary = 0
    for line in (RESULTS.read_text().splitlines() if RESULTS.exists() else []):
        try:
            v = json.loads(line).get("verdict")
        except Exception:
            continue
        if v == "ad":
            ad += 1
        elif v == "content":
            content += 1
        elif v == "boundary":
            boundary += 1
    # precision = correct ads / judged-as-promoted (ad + boundary count as correct ad presence; content = false)
    correct = ad + boundary
    judged = correct + content
    p, lo, hi = wilson(correct, judged)
    print(f"\n=== rediff ear-audit precision ===")
    print(f"judged: {judged}   correct-ad: {correct} (incl. {boundary} boundary-off)   false-content: {content}")
    if judged:
        print(f"PRECISION: {p:.1%}  (Wilson 95% CI {lo:.1%}–{hi:.1%})")
    else:
        print("no verdicts yet")
    print(f"results: {RESULTS}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=0, help="cap number of spans this session")
    ap.add_argument("--shuffle", action="store_true", help="seeded random order (stratified-ish sampling)")
    ap.add_argument("--episode", default=None, help="only episodes whose id contains this substring")
    ap.add_argument("--summary", action="store_true", help="print running precision + CI and exit")
    a = ap.parse_args()

    if a.summary:
        summary()
        return 0

    spans = load_spans()
    if a.episode:
        spans = [s for s in spans if a.episode.lower() in s["episodeId"].lower()]
    done = load_done()
    spans = [s for s in spans if (s["episodeId"], round(s["start"], 1)) not in done]
    if a.shuffle:
        random.Random(42).shuffle(spans)
    else:
        spans.sort(key=lambda s: (s["episodeId"], s["start"]))
    if a.limit:
        spans = spans[: a.limit]

    if not spans:
        print("Nothing left to audit (all done or filtered out).")
        summary()
        return 0

    print(f"{len(spans)} span(s) to audit ({len(done)} already done). "
          f"Keys: [a]d [c]ontent [b]oundary-off [r]eplay [p]lay-leadin [s]kip [q]uit\n")
    with RESULTS.open("a") as out:
        for i, s in enumerate(spans, 1):
            path = audio_file(s)
            dur = s["end"] - s["start"]
            hdr = f"[{i}/{len(spans)}] {s['show']} · {s['episodeId'][:44]} · {s['start']:.0f}-{s['end']:.0f}s ({dur:.0f}s)"
            print(hdr)
            if path is None:
                print("  ! audio not staged — skipping\n")
                continue
            play(path, s["start"], dur)
            while True:
                try:
                    ans = input("  ad? [a/c/b/r/p/s/q] ").strip().lower()
                except (EOFError, KeyboardInterrupt):
                    print("\nsaved; bye.")
                    summary()
                    return 0
                if ans == "r":
                    play(path, s["start"], dur); continue
                if ans == "p":
                    play(path, max(0.0, s["start"] - 10.0), 12.0); continue
                if ans in ("a", "c", "b", "s"):
                    break
                if ans == "q":
                    print("saved; bye.")
                    summary()
                    return 0
                print("  keys: a c b r p s q")
            verdict = {"a": "ad", "c": "content", "b": "boundary", "s": "skip"}[ans]
            note = ""
            if verdict in ("content", "boundary"):
                note = input("  note (optional): ").strip()
            rec = {"episodeId": s["episodeId"], "show": s["show"], "start": s["start"],
                   "end": s["end"], "verdict": verdict, "note": note}
            out.write(json.dumps(rec) + "\n"); out.flush()
            if verdict == "content":
                print(f"  → flagged false. Also run if you want it removed from the annotation:")
                print(f"    python3 scripts/l2f-flag-false-promote.py {s['episodeId'][:24]} {s['start']:.0f} "
                      f"--reason='ear-audit: {note or 'host content'}'")
            print()
    summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
