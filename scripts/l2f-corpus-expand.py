#!/usr/bin/env python3
"""
l2f-corpus-expand.py — autonomous corpus growth via curated batch lists.

Builds on top of scripts/l2f-dai-snapshot.py: that script handles ONE
(show, episode-index) snapshot at a time; this one is the agentic curator
that picks the right shows and the right episode indexes for three named
growth strategies, all baked into the script so no human prompts are
involved.

================================================================
Batches
================================================================

  --batch siblings  (default if no --batch given)
      Pull 4 MORE episodes from each of the 5 shows already rotated in the
      current manifest (TechCrunch Daily Crunch, The Nikki Glaser Podcast,
      SmartLess, Morbid, Why Is This Happening? The Chris Hayes Podcast),
      walking BACK in time via the RSS <item> order so we don't re-snapshot
      what's already in the manifest. Goal: ≥3-5 episodes per show for
      xsdz.9/.11's within-show time-split.

      Cost: ~20 episodes × ~40 MB average = ~800 MB of audio.

  --batch networks
      ONE latest episode each from a curated list of shows on big
      ad-supported networks likely to share sponsors with the existing
      corpus. Goal: cross-show sponsor recurrence for xsdz.13.

      Cost: ~10 episodes × ~50 MB = ~500 MB.

  --batch fresh-rotators
      ONE episode each from 5 more DAI-confirmed shows NOT in the current
      corpus, picked across the 5 classifier-confirmed reliable-DAI hosts
      (rss.art19.com, sphinx.acast.com, pdst.fm, dts.podtrac.com, mgln.ai).
      Goal: host-pipeline diversity.

      Cost: ~5 episodes × ~40 MB = ~200 MB.

  --batch all
      Runs all three batches in sequence (siblings → networks →
      fresh-rotators). Cost: ~1.5 GB total.

================================================================
Implementation notes
================================================================
* RE-USES l2f-dai-snapshot.py via Python `importlib` so the snapshot logic
  (RSS fetch, episode pick, slug, manifest write, sha256, idempotent skip)
  is the single source of truth. NO duplication.
* Skips episodes already in the manifest by `episodeId`. Re-running this
  script is a no-op once a batch has fully completed.
* No new pip deps. Standard library only.

Usage:
    scripts/l2f-corpus-expand.py                  # = --batch siblings
    scripts/l2f-corpus-expand.py --batch networks
    scripts/l2f-corpus-expand.py --batch fresh-rotators
    scripts/l2f-corpus-expand.py --batch all
    scripts/l2f-corpus-expand.py --batch siblings --plan-only  # print plan, download nothing
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import pathlib
import sys
from typing import Iterable

REPO = pathlib.Path(__file__).resolve().parents[1]
SNAPSHOT_PY = REPO / "scripts/l2f-dai-snapshot.py"
MANIFEST = REPO / "TestFixtures/Corpus/Snapshots/manifest.json"

# Shows already rotated (per manifest). One episode each is already on disk;
# we pull 4 MORE going back in time via RSS <item> index 1..4.
SIBLINGS_SHOWS = [
    "TechCrunch Daily Crunch",
    "The Nikki Glaser Podcast",
    "SmartLess",
    "Morbid",
    "Why Is This Happening? The Chris Hayes Podcast",
]
SIBLINGS_INDEXES = [1, 2, 3, 4]

# Networks likely to share sponsors with the existing corpus. One latest
# (index 0) each.
NETWORKS_SHOWS = [
    "Up First",
    "Planet Money",
    "Fresh Air",
    "Business Wars",
    "American Scandal",
    "Stuff You Should Know",
    "The Ezra Klein Show",
    "Hard Fork",
    "Radiolab",
    "On The Media",
]

# DAI-confirmed reliable hosts — pick one show per host to maximise pipeline
# coverage. Choices are curated; this script CANNOT auto-detect host from
# RSS without downloading, so it picks well-known shows on each host.
FRESH_ROTATORS_SHOWS = [
    # rss.art19.com — Wondery network (e.g. Tech Won't Save Us is art19)
    "Tech Won't Save Us",
    # sphinx.acast.com — acast network
    "The Rest Is Politics",
    # pdst.fm — podsights/Charlie-Kirk-Show pipeline
    "The Daily Show: Ears Edition",
    # dts.podtrac.com — podtrac shows (e.g. The Daily, Pod Save America)
    "Pod Save America",
    # mgln.ai — Megaphone shows (e.g. The Mel Robbins Podcast)
    "The Mel Robbins Podcast",
]


def _load_snapshot_module():
    """Import scripts/l2f-dai-snapshot.py so we can call snapshot_one() directly."""
    spec = importlib.util.spec_from_file_location("l2f_dai_snapshot", SNAPSHOT_PY)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load snapshot module from {SNAPSHOT_PY}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


def _load_manifest() -> list[dict]:
    if not MANIFEST.exists():
        return []
    try:
        return json.loads(MANIFEST.read_text())
    except Exception as e:
        print(f"WARN: failed to parse manifest: {e}", file=sys.stderr)
        return []


def _manifest_episode_ids() -> set[str]:
    return {r.get("episodeId") for r in _load_manifest() if r.get("episodeId")}


def plan_for_batch(batch: str) -> list[tuple[str, int]]:
    """
    Returns list of (show_name, episode_index) pairs to snapshot.
    NOTE: idempotence is handled inside snapshot_one() (it skips episodes
    whose flat file already exists). We can't pre-skip by episode-id here
    because that requires fetching the RSS feed first, which costs time.
    """
    if batch == "siblings":
        return [(s, i) for s in SIBLINGS_SHOWS for i in SIBLINGS_INDEXES]
    if batch == "networks":
        return [(s, 0) for s in NETWORKS_SHOWS]
    if batch == "fresh-rotators":
        return [(s, 0) for s in FRESH_ROTATORS_SHOWS]
    if batch == "all":
        return (
            plan_for_batch("siblings")
            + plan_for_batch("networks")
            + plan_for_batch("fresh-rotators")
        )
    raise ValueError(f"unknown batch: {batch}")


def _save_manifest(rows: list[dict]) -> None:
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(rows, indent=2))


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--batch",
        choices=["siblings", "networks", "fresh-rotators", "all"],
        default="siblings",
        help="Which curated batch to run. Default: siblings.",
    )
    ap.add_argument(
        "--plan-only",
        action="store_true",
        help="Print the planned (show, episode_index) list and exit without downloading.",
    )
    args = ap.parse_args()

    plan = plan_for_batch(args.batch)
    print(f"batch={args.batch} planned-items={len(plan)}", file=sys.stderr)
    for show, idx in plan:
        print(f"  PLAN {show:<55} idx={idx}", file=sys.stderr)

    if args.plan_only:
        print("--plan-only: not downloading; exit 0", file=sys.stderr)
        return 0

    snap = _load_snapshot_module()
    snap.AUDIO_ROOT.mkdir(parents=True, exist_ok=True)
    manifest = _load_manifest()
    seen_ids = {r.get("episodeId") for r in manifest if r.get("episodeId")}

    ok = 0
    skip = 0
    fail = 0
    for show, idx in plan:
        try:
            r = snap.snapshot_one(show, episode_index=idx)
        except Exception as e:
            print(f"  FAIL [{show} idx={idx}]: {e}", file=sys.stderr)
            fail += 1
            continue
        if not r.get("ok"):
            print(f"  FAIL [{show} idx={idx}]: {r.get('error')}", file=sys.stderr)
            fail += 1
            continue
        if "skipped" in r:
            skip += 1
            print(
                f"  SKIP [{show} idx={idx}]: {r.get('skipped')} {r.get('path')}",
                file=sys.stderr,
            )
            continue
        ep_id = r.get("episodeId")
        if not ep_id:
            print(f"  WARN [{show} idx={idx}]: snapshot ok but no episodeId", file=sys.stderr)
            continue
        if ep_id in seen_ids:
            skip += 1
            print(f"  SKIP [{show} idx={idx}]: episodeId={ep_id} already in manifest",
                  file=sys.stderr)
            continue
        manifest.append({k: r[k] for k in (
            "show","showSlug","episodeId","title","publishDate","snapshotIso",
            "feedUrl","enclosureUrl","enclosureHost","audioPath","sizeBytes","sha256")})
        seen_ids.add(ep_id)
        _save_manifest(manifest)
        ok += 1
        print(
            f"  OK   [{show} idx={idx}]: {ep_id} "
            f"({r.get('sizeBytes', 0)/1024/1024:.1f} MB)",
            file=sys.stderr,
        )

    print(
        f"\nbatch={args.batch} complete: ok={ok} skip={skip} fail={fail} "
        f"manifest-now={len(manifest)} entries",
        file=sys.stderr,
    )
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
