#!/usr/bin/env python3
"""Convert hand-labeled corpus annotations into ChapterPlan golden-set fixtures.

Reads each `TestFixtures/Corpus/Annotations/*.json` and emits one
`PlayheadTests/Fixtures/ChapterPlanGoldenSet/dogfood/<episode_id>.json` in
the GoldenChapterSet schema consumed by `ChapterPlanGoldenSetLoader`.

Anonymization (per au2v.1.22 privacy rule — no advertiser/product names
verbatim in committed fixtures): topic labels are derived from `ad_type`
only; advertiser, product, and confidence notes are dropped.

Usage:
    python3 Scripts/convert_annotations_to_chapter_goldens.py [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ANNOTATIONS_DIR = REPO_ROOT / "TestFixtures" / "Corpus" / "Annotations"
DOGFOOD_DIR = (
    REPO_ROOT
    / "PlayheadTests"
    / "Fixtures"
    / "ChapterPlanGoldenSet"
    / "dogfood"
)

AD_TYPE_LABEL = {
    "host_read": "host-read sponsor",
    "blended_host_read": "blended host-read",
    "dynamic": "dynamic insertion",
    "pre_recorded": "pre-recorded spot",
}
AD_FALLBACK_LABEL = "advertisement"
CONTENT_LABEL = "editorial content"

NOTES = (
    "Auto-converted from TestFixtures/Corpus/Annotations/ by "
    "Scripts/convert_annotations_to_chapter_goldens.py. Topic labels are "
    "anonymized to ad_type / 'editorial content' — advertiser, product, "
    "and confidence_notes are stripped (au2v.1.22 privacy rule)."
)


def convert(annotation: dict) -> dict:
    episode_id = annotation["episode_id"]
    raw_hash = annotation["audio_fingerprint"]
    content_hash = raw_hash[len("sha256:"):] if raw_hash.startswith("sha256:") else raw_hash

    chapters: list[dict] = []
    for ad in annotation.get("ad_windows", []):
        chapters.append(
            {
                "startTimeSeconds": float(ad["start_seconds"]),
                "expectedDisposition": "adBreak",
                "expectedTopicLabel": AD_TYPE_LABEL.get(
                    ad.get("ad_type", ""), AD_FALLBACK_LABEL
                ),
            }
        )
    for content in annotation.get("content_windows", []):
        chapters.append(
            {
                "startTimeSeconds": float(content["start_seconds"]),
                "expectedDisposition": "content",
                "expectedTopicLabel": CONTENT_LABEL,
            }
        )
    chapters.sort(key=lambda c: c["startTimeSeconds"])

    return {
        "episodeId": episode_id,
        "episodeContentHash": content_hash,
        "chapters": chapters,
        "notes": NOTES,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print conversion plan without writing files.",
    )
    args = parser.parse_args()

    if not ANNOTATIONS_DIR.is_dir():
        print(f"error: annotations dir not found: {ANNOTATIONS_DIR}", file=sys.stderr)
        return 1

    if not args.dry_run:
        DOGFOOD_DIR.mkdir(parents=True, exist_ok=True)

    converted = 0
    skipped = 0
    for src in sorted(ANNOTATIONS_DIR.glob("*.json")):
        if src.name.startswith("_template"):
            skipped += 1
            continue
        with src.open() as f:
            annotation = json.load(f)
        golden = convert(annotation)
        out_path = DOGFOOD_DIR / f"{annotation['episode_id']}.json"
        action = "would write" if args.dry_run else "wrote"
        print(f"  {action} {out_path.relative_to(REPO_ROOT)} ({len(golden['chapters'])} chapters)")
        if not args.dry_run:
            with out_path.open("w") as f:
                json.dump(golden, f, indent=2)
                f.write("\n")
        converted += 1

    suffix = " (dry-run)" if args.dry_run else ""
    print(f"\nConverted {converted} annotations to dogfood goldens; skipped {skipped} templates.{suffix}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
