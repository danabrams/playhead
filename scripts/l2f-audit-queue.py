#!/usr/bin/env python3
"""
l2f-audit-queue.py — generate a focused ffplay audit queue for rediff-only
auto-promoted ad windows (audit_priority=1).

Auto-promotion rule R3 (rediff-only, ≥20s, audit_priority=1) is the
weakest of the three triangulation rules — these spans come from a single
signal (rediff DAI-diff). They're high-precision in theory but unverified
in practice. This script generates a markdown queue the user can step
through opportunistically with ffplay.

  scripts/l2f-audit-queue.py            # all audit_priority=1 spans
  scripts/l2f-audit-queue.py --limit=5  # first 5 only (a 15-min session)
  scripts/l2f-audit-queue.py --csv      # CSV instead of markdown
"""
import argparse, json, pathlib, sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
ANN_DIR = ROOT / "TestFixtures/Corpus/Annotations"
AUDIO_DIR = ROOT / "TestFixtures/Corpus/Audio"
MANIFEST = ROOT / "TestFixtures/Corpus/Snapshots/manifest.json"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="Cap output count")
    ap.add_argument("--csv", action="store_true")
    a = ap.parse_args()

    # Build episodeId → show lookup from the manifest
    show_by_eid = {}
    if MANIFEST.exists():
        try:
            for m in json.loads(MANIFEST.read_text()):
                show_by_eid[m.get("episodeId", "")] = m.get("show", "?")
        except Exception:
            pass

    queue = []
    for ann_path in sorted(ANN_DIR.glob("*.json")):
        try:
            ann = json.loads(ann_path.read_text())
        except Exception:
            continue
        eid = ann.get("episodeId") or ann_path.stem
        wins = ann.get("adWindows") or ann.get("ad_windows") or []
        for w in wins:
            p = w.get("audit_priority")
            if p != 1: continue
            start = w.get("startSeconds") or w.get("start_seconds") or 0
            end = w.get("endSeconds") or w.get("end_seconds") or 0
            queue.append(dict(
                eid=eid, start=float(start), end=float(end),
                duration=float(end) - float(start),
                provenance=w.get("provenance") or w.get("source") or "rediff",
                show=ann.get("show") or show_by_eid.get(eid, "?"),
            ))

    if a.limit > 0:
        queue = queue[:a.limit]

    if not queue:
        print("(no audit_priority=1 spans)")
        return

    if a.csv:
        print("episode,show,start,end,duration,provenance,audio_path,ffplay_cmd")
        for q in queue:
            audio = AUDIO_DIR / f"{q['eid']}.mp3"
            print(f"{q['eid']},{q['show']},{q['start']:.1f},{q['end']:.1f},{q['duration']:.1f},{q['provenance']},{audio},"
                  f"\"ffplay -nodisp -autoexit -ss {q['start']:.1f} -t {q['duration']:.1f} '{audio}'\"")
        return

    print(f"# Audit-priority queue ({len(queue)} spans)")
    print()
    print(f"Rediff-only auto-promotions (R3, ≥20s). These are high-precision in theory")
    print(f"but unverified — opportunistically spot-check 1–2 minutes each to flag mistakes")
    print(f"before they bias activation evaluations.")
    print()
    print(f"## Workflow")
    print(f"For each row: run the ffplay command. If it sounds like an ad, mark ✓.")
    print(f"If it sounds like host content, run:")
    print(f"  `scripts/l2f-flag-false-promote.py <eid-prefix> <start_seconds> --reason='...'`")
    print(f"This removes the window from the annotation and appends to")
    print(f"`TestFixtures/Corpus/Snapshots/audit-rejects.jsonl`.")
    print()
    print(f"| # | Episode | Show | Span | Dur | ffplay |")
    print(f"|---|---------|------|------|-----|--------|")
    for i, q in enumerate(queue, 1):
        audio = AUDIO_DIR / f"{q['eid']}.mp3"
        eid_short = q['eid'][:36] + ("…" if len(q['eid']) > 36 else "")
        show_short = (q['show'] or "?")[:24]
        cmd = f"`ffplay -nodisp -autoexit -ss {q['start']:.0f} -t {q['duration']:.0f} '{audio.name}'`"
        print(f"| {i} | `{eid_short}` | {show_short} | {q['start']:.0f}-{q['end']:.0f} | {q['duration']:.0f}s | {cmd} |")
    print()
    print(f"## Quick batch")
    print(f"```bash")
    print(f"cd {AUDIO_DIR}")
    for q in queue:
        print(f"ffplay -nodisp -autoexit -ss {q['start']:.0f} -t {q['duration']:.0f} '{q['eid']}.mp3'  # {q['show'][:30]}")
    print(f"```")

if __name__ == "__main__":
    main()
