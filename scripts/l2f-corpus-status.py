#!/usr/bin/env python3
"""
l2f-corpus-status.py — one-line summary + per-episode breakdown of the
autonomous Tier-A corpus pipeline.

Reads what's on disk; never modifies anything. Designed to be the "where are we"
view for the daily + weekly launchd loops.

  scripts/l2f-corpus-status.py            # full report
  scripts/l2f-corpus-status.py --terse    # one summary line only
  scripts/l2f-corpus-status.py --gaps     # show only episodes missing something
"""
import argparse, json, pathlib, sys

from l2f_canonical_manifest import load_canonical_annotations

ROOT = pathlib.Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "TestFixtures/Corpus/Snapshots/manifest.json"
AUDIO = ROOT / "TestFixtures/Corpus/Audio"
TRANSCRIPTS = ROOT / "TestFixtures/Corpus/Transcripts"
DRAFTS = ROOT / "TestFixtures/Corpus/Drafts"
ANNOTATIONS = ROOT / "TestFixtures/Corpus/Annotations"
DAILY_LOG = ROOT / "TestFixtures/Corpus/Snapshots/daily-loop.log"
WEEKLY_LOG = ROOT / "TestFixtures/Corpus/Snapshots/weekly-expand.log"

def has(p): return p.exists() and p.stat().st_size > 0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--terse", action="store_true")
    ap.add_argument("--gaps", action="store_true")
    a = ap.parse_args()

    if not MANIFEST.exists():
        print("no manifest yet — run scripts/l2f-dai-snapshot.py to start")
        sys.exit(1)
    m = json.loads(MANIFEST.read_text())
    canonical_annotations = load_canonical_annotations(ANNOTATIONS)

    rows = []
    stats = {"manifest": len(m), "audio": 0, "transcript": 0, "drafter_draft": 0,
             "rediff_draft": 0, "annotation": 0, "ad_windows": 0,
             "audit_high": 0, "audit_low": 0}
    for entry in m:
        eid = entry["episodeId"]
        audio = AUDIO / f"{eid}.mp3"
        tx = TRANSCRIPTS / f"{eid}.json"
        d_draft = DRAFTS / f"{eid}.draft.json"
        r_draft = DRAFTS / f"{eid}.dai-rediff.json"
        ann = ANNOTATIONS / f"{eid}.json"
        st = dict(eid=eid, audio=has(audio), transcript=has(tx),
                  drafter=has(d_draft), rediff=has(r_draft),
                  annotation=ann.name in canonical_annotations,
                  show=entry.get("show", "?"), host=entry.get("enclosureHost", "?"))
        if st["audio"]: stats["audio"] += 1
        if st["transcript"]: stats["transcript"] += 1
        if st["drafter"]: stats["drafter_draft"] += 1
        if st["rediff"]: stats["rediff_draft"] += 1
        if st["annotation"]: stats["annotation"] += 1
        if st["annotation"]:
            ann_data = canonical_annotations[ann.name]
            wins = ann_data.get("adWindows") or ann_data.get("ad_windows") or []
            stats["ad_windows"] += len(wins)
            for w in wins:
                p = w.get("audit_priority")
                if p == 1: stats["audit_high"] += 1
                elif p and p >= 2: stats["audit_low"] += 1
        rows.append(st)

    summary = (f"manifest={stats['manifest']} audio={stats['audio']} "
               f"transcript={stats['transcript']} draft={stats['drafter_draft']} "
               f"rediff={stats['rediff_draft']} annotation={stats['annotation']} "
               f"({stats['ad_windows']} ad-windows: {stats['audit_low']} triangulated + {stats['audit_high']} audit-priority)")
    if a.terse:
        print(summary); return

    print(f"=== Tier-A corpus status (root: {ROOT.name}) ===")
    print(summary)
    print()
    if DAILY_LOG.exists():
        print("Last daily-loop runs:")
        print("\n".join(f"  {l}" for l in DAILY_LOG.read_text().strip().split("\n")[-3:]))
    else:
        print("Last daily-loop runs: (none yet — first fires at 03:30 local)")
    if WEEKLY_LOG.exists():
        print(f"Last weekly expand: {WEEKLY_LOG.read_text().strip().split(chr(10))[-1]}")
    else:
        print("Last weekly expand: (none yet — first fires Sunday 02:00 local)")
    print()

    if a.gaps:
        rows = [r for r in rows if not all([r["audio"], r["transcript"], r["drafter"], r["annotation"]])]
        if not rows:
            print("(no gaps — every manifest entry has audio + transcript + drafter + annotation)")
            return
        print(f"=== {len(rows)} episodes with gaps ===")
    print(f"{'episode':70} {'A':>1} {'T':>1} {'D':>1} {'R':>1} {'N':>1}  host")
    for r in rows:
        flag = lambda b: "✓" if b else "·"
        print(f"  {r['eid'][:68]:68} {flag(r['audio']):>1} {flag(r['transcript']):>1} "
              f"{flag(r['drafter']):>1} {flag(r['rediff']):>1} {flag(r['annotation']):>1}  {r['host'][:24]}")

if __name__ == "__main__":
    main()
