#!/usr/bin/env python3
"""
l2f-flag-false-promote.py — demote a falsely-promoted ad window after manual audit.

If `l2f-audit-queue.py` surfaces a span that ffplay reveals is HOST content
(not an ad), this script removes the window from the annotation and appends
a record to the rejects log so future auto-promotion can avoid the same
mistake.

  scripts/l2f-flag-false-promote.py <episode-id> <start_seconds> [--reason="..."]

Examples:
  scripts/l2f-flag-false-promote.py casefile-true-crime-2026-05-30-... 6030
  scripts/l2f-flag-false-promote.py smartless-... 2703 --reason="banter between co-hosts"

The episode-id can be a prefix (matches on startswith if unambiguous).
The start_seconds matches the window whose start is within ±2s of the given value.
"""
import argparse, json, pathlib, sys, datetime as dt

ROOT = pathlib.Path(__file__).resolve().parents[1]
ANN_DIR = ROOT / "TestFixtures/Corpus/Annotations"
REJECTS = ROOT / "TestFixtures/Corpus/Snapshots/audit-rejects.jsonl"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("episode_id", help="Full or prefix episodeId")
    ap.add_argument("start_seconds", type=float, help="Start of window to demote")
    ap.add_argument("--reason", default="", help="Why it's a false positive")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    # Resolve episodeId (allow prefix match if unambiguous)
    candidates = sorted(ANN_DIR.glob(f"{a.episode_id}*.json"))
    if not candidates:
        print(f"no annotation matching '{a.episode_id}*'", file=sys.stderr); sys.exit(2)
    if len(candidates) > 1:
        print(f"ambiguous prefix '{a.episode_id}' matches {len(candidates)} annotations:", file=sys.stderr)
        for c in candidates: print(f"  {c.name}", file=sys.stderr)
        sys.exit(2)
    ann_path = candidates[0]
    ann = json.loads(ann_path.read_text())
    eid = ann.get("episodeId", ann_path.stem)

    wins_key = "adWindows" if "adWindows" in ann else "ad_windows"
    wins = ann.get(wins_key, [])
    target = None
    for w in wins:
        start = w.get("startSeconds") or w.get("start_seconds") or 0
        if abs(float(start) - a.start_seconds) <= 2.0:
            target = w; break
    if target is None:
        print(f"no window within ±2s of start={a.start_seconds} (current windows: "
              f"{[w.get('startSeconds') or w.get('start_seconds') for w in wins]})",
              file=sys.stderr); sys.exit(2)

    end = target.get("endSeconds") or target.get("end_seconds")
    provenance = target.get("provenance") or target.get("source") or "?"
    print(f"will demote: {eid} {a.start_seconds:.0f}-{end:.0f}s (provenance={provenance})")
    if a.reason: print(f"  reason: {a.reason}")
    if a.dry_run:
        print("(dry-run; no changes)"); return

    # Remove the window
    new_wins = [w for w in wins if w is not target]
    ann[wins_key] = new_wins
    # Update bookkeeping if present
    if "adWindowCount" in ann: ann["adWindowCount"] = len(new_wins)
    ann_path.write_text(json.dumps(ann, indent=2) + "\n")

    # Append rejects log
    REJECTS.parent.mkdir(parents=True, exist_ok=True)
    rec = dict(
        ts=dt.datetime.utcnow().isoformat() + "Z",
        episodeId=eid,
        startSeconds=a.start_seconds,
        endSeconds=end,
        provenance=provenance,
        reason=a.reason,
    )
    with REJECTS.open("a") as f:
        f.write(json.dumps(rec) + "\n")
    print(f"demoted; remaining windows: {len(new_wins)}; rejects log: {REJECTS}")
    print()
    print(f"next steps:")
    print(f"  git diff -- '{ann_path.relative_to(ROOT)}'")
    print(f"  # if it looks right, commit:")
    print(f"  git add '{ann_path.relative_to(ROOT)}' '{REJECTS.relative_to(ROOT)}'")
    print(f"  git commit -m 'corpus: demote falsely-promoted ad span in {eid} ({a.start_seconds:.0f}-{end:.0f}s)'")

if __name__ == "__main__":
    main()
