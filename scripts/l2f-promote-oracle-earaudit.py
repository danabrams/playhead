#!/usr/bin/env python3
"""Promote the frozen 2026-07-15 oracle ear-audit into a gold evaluation artifact.

Reads the content-addressed audit ledger (emitted, fingerprint-bound oracle
slots) and Dan's completed review (rebound bounds + statuses), and emits a
content-addressed evaluation JSON under TestFixtures/Corpus/Evaluations/
shaped like the earaudit-partial-silver artifact so automated scorers can
compare any future run's boundaries against gold human truth without a
re-audit (playhead-l2f.10).

Promotion rules (recorded on playhead-xsdz.36 / playhead-l2f.10):
- rebounded rows become gold full_breaks using Dan's proposed bounds;
- the three nikki-glaser ep514 rows are ONE break heard end-to-end: merged
  into a single full_break using the first row's rebound bounds (Dan: most
  accurate), all three ledger ids recorded as provenance;
- rejected rows become content_vetoes over the emitted slot interval;
- every break carries boundary_tolerance_seconds (0.5 for nikki-glaser
  stinger-seam rows, 0.3 default per Dan's calibration);
- quality tier is GOLD per Dan's explicit 2026-07-15 attestation
  (single listener, declared great care).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys

DEFAULT_LEDGER = pathlib.Path(
    "TestFixtures/Corpus/Audits/"
    "oracle-earaudit-ledger-2026-07-15-"
    "a31899a7964d149a306fd161ef402e93b7b03460018ae4fea24f6df8bceeec8d.jsonl"
)
DEFAULT_REVIEW = pathlib.Path(
    "TestFixtures/Corpus/Audits/"
    "oracle-earaudit-review-2026-07-15-"
    "9e826c081d1969b8e6ed8cc405d3d6f5a50fbff77844f2baca38b5f5acea0925.json"
)
DEFAULT_OUTPUT_DIR = pathlib.Path("TestFixtures/Corpus/Evaluations")
ARTIFACT_PREFIX = "earaudit-oracle-gold-"

# Dan, mid-audit 2026-07-15: queue rows 39/40/41 are one nikki ep514 break;
# use the first row's rebound bounds. Keyed by (episode_id, emitted start).
MERGE_GROUPS = [
    {
        "episode_id": (
            "the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev"
        ),
        "member_starts": [1154.3, 1273.8, 1290.2],
        "bounds_from_start": 1154.3,
    }
]

NIKKI_TOLERANCE_SECONDS = 0.5
DEFAULT_TOLERANCE_SECONDS = 0.3


class PromotionError(RuntimeError):
    pass


def tolerance_for(episode_id: str) -> float:
    if "nikki-glaser" in episode_id:
        return NIKKI_TOLERANCE_SECONDS
    return DEFAULT_TOLERANCE_SECONDS


def load_rows(ledger_path: pathlib.Path, review_path: pathlib.Path) -> list[dict]:
    ledger = {}
    for line_number, line in enumerate(
        ledger_path.read_text(encoding="utf-8").splitlines(), 1
    ):
        if not line.strip():
            continue
        entry = json.loads(line)
        ledger[entry["id"]] = entry
    reviews = json.load(review_path.open(encoding="utf-8"))["reviews"]
    if set(reviews) != set(ledger):
        missing = sorted(set(ledger) - set(reviews))
        extra = sorted(set(reviews) - set(ledger))
        raise PromotionError(
            f"review/ledger id mismatch: missing reviews {missing[:3]}, "
            f"unknown reviews {extra[:3]}"
        )
    rows = []
    for row_id, entry in ledger.items():
        review = reviews[row_id]
        status = review.get("status")
        if status not in {"rebounded", "rejected", "approved"}:
            raise PromotionError(f"unsupported review status {status!r} on {row_id}")
        rows.append({**entry, **review})
    rows.sort(key=lambda r: (r["episode_id"], r["current_start_seconds"]))
    return rows


def apply_merges(rows: list[dict]) -> list[dict]:
    merged_rows = list(rows)
    for group in MERGE_GROUPS:
        members = [
            r
            for r in merged_rows
            if r["episode_id"] == group["episode_id"]
            and any(
                abs(r["current_start_seconds"] - start) < 0.05
                for start in group["member_starts"]
            )
        ]
        if len(members) != len(group["member_starts"]):
            raise PromotionError(
                f"merge group expected {len(group['member_starts'])} members, "
                f"found {len(members)} for {group['episode_id']}"
            )
        primary = next(
            r
            for r in members
            if abs(r["current_start_seconds"] - group["bounds_from_start"]) < 0.05
        )
        if primary["status"] != "rebounded":
            raise PromotionError("merge primary must be a rebounded row")
        combined = dict(primary)
        combined["merged_ledger_ids"] = [m["id"] for m in members]
        combined["current_end_seconds"] = max(
            m["current_end_seconds"] for m in members
        )
        merged_rows = [
            combined if r["id"] == primary["id"] else r
            for r in merged_rows
            if r["id"] not in {m["id"] for m in members} or r["id"] == primary["id"]
        ]
    return merged_rows


def build_artifact(rows: list[dict], sources: dict) -> dict:
    assets: dict[str, dict] = {}
    review_counts = {"rebounded": 0, "rejected": 0, "approved": 0}
    for row in rows:
        asset = assets.setdefault(
            row["episode_id"],
            {
                "audio_fingerprint": row["audio_fingerprint"],
                "content_vetoes": [],
                "duration_seconds": round(row["duration_seconds"], 1),
                "episode_id": row["episode_id"],
                "full_breaks": [],
                "presence_anchors": [],
                "show_name": row["show_name"],
            },
        )
        if asset["audio_fingerprint"] != row["audio_fingerprint"]:
            raise PromotionError(f"fingerprint drift within {row['episode_id']}")
        ledger_ids = row.get("merged_ledger_ids", [row["id"]])
        review_counts[row["status"]] += 1
        if row["status"] == "rejected":
            asset["content_vetoes"].append(
                {
                    "end_seconds": round(row["current_end_seconds"], 3),
                    "source_ledger_ids": ledger_ids,
                    "source_review_ids": [row["id"]],
                    "start_seconds": round(row["current_start_seconds"], 3),
                }
            )
            continue
        if row["status"] == "approved":
            # Approval endorses the EMITTED bounds as gold truth.
            start, end = row["current_start_seconds"], row["current_end_seconds"]
        else:
            start = row.get("proposed_start_seconds")
            end = row.get("proposed_end_seconds")
        if start is None or end is None or not end > start:
            raise PromotionError(f"{row['status']} row {row['id']} lacks valid bounds")
        asset["full_breaks"].append(
            {
                "boundary_tolerance_seconds": tolerance_for(row["episode_id"]),
                "end_seconds": round(end, 3),
                "source_ledger_ids": ledger_ids,
                "source_review_ids": [row["id"]],
                "start_seconds": round(start, 3),
            }
        )
    # Dedupe: distinct source slots can be audited onto the SAME break
    # (identical pods heard twice; re-audits). Bounds agreeing within 1.0s
    # are one break — keep the first (primary-audit precedence), union the
    # provenance ids. Without this, scoring double-counts identical breaks
    # (the l2f.8 no-double-count criterion).
    for asset in assets.values():
        deduped: list[dict] = []
        for break_ in sorted(asset["full_breaks"], key=lambda b: (b["start_seconds"], b["end_seconds"])):
            merged = False
            for kept in deduped:
                if (
                    abs(kept["start_seconds"] - break_["start_seconds"]) <= 1.0
                    and abs(kept["end_seconds"] - break_["end_seconds"]) <= 1.0
                ):
                    kept["source_ledger_ids"] = sorted(
                        set(kept["source_ledger_ids"]) | set(break_["source_ledger_ids"])
                    )
                    kept["source_review_ids"] = sorted(
                        set(kept["source_review_ids"]) | set(break_["source_review_ids"])
                    )
                    merged = True
                    break
            if not merged:
                deduped.append(break_)
        asset["full_breaks"] = deduped
    asset_list = [assets[key] for key in sorted(assets)]
    full_breaks = sum(len(a["full_breaks"]) for a in asset_list)
    vetoes = sum(len(a["content_vetoes"]) for a in asset_list)
    return {
        "artifact_kind": "oracle_earaudit_gold_boundary_evaluation",
        "assets": asset_list,
        "label_semantics": {
            "content_vetoes": (
                "the exact emitted interval was heard and is human-labeled "
                "content; surrounding audio is not labeled"
            ),
            "coverage": "partial",
            "full_breaks": (
                "gold human-reviewed complete ad-break boundaries (rebound "
                "bounds from the 2026-07-15 oracle-emission ear audit); "
                "errors within boundary_tolerance_seconds score as zero"
            ),
            "presence_anchors": "none in this artifact",
            "quality": "gold",
            "attestation": (
                "Dan Abrams, single listener, explicit gold attestation "
                "2026-07-15; declared care to +-0.3s (+-0.5s nikki-glaser "
                "stinger seam)"
            ),
            "unlabeled_audio": "unknown_elsewhere",
        },
        "schema_version": 1,
        "sources": sources,
        "summary": {
            "assets": len(asset_list),
            "content_vetoes": vetoes,
            "full_break_assets": sum(1 for a in asset_list if a["full_breaks"]),
            "full_breaks": full_breaks,
            "labeled_regions": full_breaks + vetoes,
            "merged_break_groups": len(MERGE_GROUPS),
            "review_status_counts": review_counts,
        },
    }


def content_addressed_bytes(artifact: dict) -> tuple[bytes, str]:
    data = json.dumps(artifact, indent=1, sort_keys=True).encode("utf-8") + b"\n"
    return data, hashlib.sha256(data).hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=pathlib.Path, default=DEFAULT_LEDGER)
    parser.add_argument("--review", type=pathlib.Path, default=DEFAULT_REVIEW)
    parser.add_argument(
        "--extra-ledger", type=pathlib.Path, action="append", default=[],
        help="additional audit ledger (pair positionally with --extra-review)",
    )
    parser.add_argument(
        "--extra-review", type=pathlib.Path, action="append", default=[],
        help="additional review file (pair positionally with --extra-ledger)",
    )
    parser.add_argument("--output-dir", type=pathlib.Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if len(args.extra_ledger) != len(args.extra_review):
        raise PromotionError("--extra-ledger and --extra-review must pair up")
    pairs = [(args.ledger, args.review)] + list(zip(args.extra_ledger, args.extra_review))
    rows = []
    seen_ids: set[str] = set()
    for ledger_path, review_path in pairs:
        for row in load_rows(ledger_path, review_path):
            if row["id"] in seen_ids:
                raise PromotionError(f"duplicate row id across pairs: {row['id']}")
            seen_ids.add(row["id"])
            rows.append(row)
    rows.sort(key=lambda r: (r["episode_id"], r["current_start_seconds"]))
    rows = apply_merges(rows)
    sources = {
        "pairs": [
            {
                "ledger": ledger_path.name,
                "ledger_sha256": hashlib.sha256(ledger_path.read_bytes()).hexdigest(),
                "review": review_path.name,
                "review_sha256": hashlib.sha256(review_path.read_bytes()).hexdigest(),
            }
            for ledger_path, review_path in pairs
        ],
    }
    artifact = build_artifact(rows, sources)
    data, digest = content_addressed_bytes(artifact)
    name = f"{ARTIFACT_PREFIX}{digest}.json"
    print(json.dumps(artifact["summary"], indent=1, sort_keys=True))
    if args.dry_run:
        print(f"dry-run: would write {args.output_dir / name}")
        return 0
    destination = args.output_dir / name
    if destination.exists():
        raise PromotionError(f"refusing to overwrite {destination}")
    destination.write_bytes(data)
    print(f"wrote {destination}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
