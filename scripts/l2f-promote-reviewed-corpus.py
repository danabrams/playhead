#!/usr/bin/env python3
"""
Promote reviewed playhead-l2f audio decisions into corpus annotations.

Default mode is report-only. Pass --promote to write
TestFixtures/Corpus/Annotations/<episode_id>.json for episodes with no
blocking review, metadata, timing, audio, or duration issues.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import shutil
import subprocess
import sys
import wave
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
CORPUS_DIR = REPO_ROOT / "TestFixtures" / "Corpus"
DEFAULT_REVIEW_FILE = CORPUS_DIR / "Drafts" / "l2f-audio-review.json"
DEFAULT_ANNOTATIONS_DIR = CORPUS_DIR / "Annotations"
DEFAULT_AUDIO_DIR = CORPUS_DIR / "Audio"
AUDIO_EXTENSIONS = {".m4a", ".mp3", ".mp4", ".aac", ".wav", ".flac"}
VALID_AD_TYPES = {
    "host_read",
    "dynamic_insertion",
    "blended_host_read",
    "produced_segment",
    "promo",
}
VALID_TRANSITION_TYPES = {"explicit", "musical", "hard_cut", "blended"}
FINAL_REVIEW_STATUSES = {"verified_ad", "false_positive", "zero_ad_confirmed"}
BOUNDARY_TOLERANCE_SECONDS = 0.05


@dataclass
class EpisodeReport:
    episode_id: str
    entries: list[dict[str, Any]]
    issues: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    annotation: dict[str, Any] | None = None
    audio_path: Path | None = None
    duration_seconds: float | None = None

    @property
    def ready(self) -> bool:
        return not self.issues and self.annotation is not None


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report on or promote reviewed L2F audio review decisions."
    )
    parser.add_argument(
        "--review-file",
        default=str(DEFAULT_REVIEW_FILE),
        help="GUI review JSON. Default: TestFixtures/Corpus/Drafts/l2f-audio-review.json",
    )
    parser.add_argument(
        "--queue",
        help="Review queue JSON. Defaults to review-file queue_path, then Drafts review queues.",
    )
    parser.add_argument(
        "--audio-dir",
        default=str(DEFAULT_AUDIO_DIR),
        help="Directory containing local <episode_id> audio files.",
    )
    parser.add_argument(
        "--annotations-dir",
        default=str(DEFAULT_ANNOTATIONS_DIR),
        help="Output directory for promoted annotation JSON.",
    )
    parser.add_argument(
        "--episode",
        action="append",
        default=[],
        help="Promote/report only this episode_id. May be passed multiple times.",
    )
    parser.add_argument(
        "--promote",
        action="store_true",
        help="Write annotations. Without this flag the command only reports readiness.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing annotation file during --promote.",
    )
    return parser.parse_args(argv)


def resolve_path(raw: str | Path, base: Path = REPO_ROOT) -> Path:
    path = Path(raw)
    if not path.is_absolute():
        path = base / path
    return path.resolve()


def resolve_local_then_repo(raw: str | Path, local_base: Path) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path.resolve()
    local = (local_base / path).resolve()
    if local.exists():
        return local
    repo = (REPO_ROOT / path).resolve()
    if repo.exists():
        return repo
    return local


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def choose_queue_path(
    args: argparse.Namespace,
    review_path: Path,
    review_payload: dict[str, Any] | None,
) -> Path:
    if args.queue:
        return resolve_path(args.queue)
    if review_payload:
        queue_path = review_payload.get("queue_path")
        if isinstance(queue_path, str) and queue_path.strip():
            return resolve_local_then_repo(queue_path, review_path.parent)
    for candidate in (
        CORPUS_DIR / "Drafts" / "review-queue.json",
        CORPUS_DIR / "Drafts" / "codex-review-queue.json",
    ):
        if candidate.exists():
            return candidate.resolve()
    return (CORPUS_DIR / "Drafts" / "review-queue.json").resolve()


def load_review_file(path: Path) -> tuple[dict[str, Any] | None, dict[str, Any], bool]:
    payload = load_json(path)
    if payload is None:
        return None, {}, True
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    reviews = payload.get("reviews")
    if reviews is None:
        reviews = {}
    if not isinstance(reviews, dict):
        raise ValueError(f"{path} reviews must be a JSON object")
    return payload, reviews, False


def load_queue(path: Path) -> list[dict[str, Any]]:
    payload = load_json(path)
    if not isinstance(payload, dict) or not isinstance(payload.get("entries"), list):
        raise ValueError(f"{path} does not contain a playhead-l2f review queue")
    entries: list[dict[str, Any]] = []
    for index, raw in enumerate(payload["entries"], start=1):
        if not isinstance(raw, dict):
            raise ValueError(f"{path} entries[{index - 1}] must be a JSON object")
        entry = dict(raw)
        episode_id = str(entry.get("episode_id") or "").strip()
        if not episode_id:
            episode_id = str(entry.get("id") or f"episode-{index}").split("#", 1)[0]
        entry["episode_id"] = episode_id
        entry["id"] = str(entry.get("id") or f"{episode_id}#{index}")
        entries.append(entry)
    return entries


def clean_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    text = str(value).strip()
    return text or None


def number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def safe_artifact_basename(value: str) -> bool:
    return (
        bool(value)
        and "/" not in value
        and "\\" not in value
        and value not in {".", ".."}
    )


def is_false_positive_trap(entry: dict[str, Any]) -> bool:
    return entry.get("false_positive_trap") is True


def review_for(entry: dict[str, Any], reviews: dict[str, Any]) -> dict[str, Any]:
    review = reviews.get(entry["id"])
    return review if isinstance(review, dict) else {}


def value_from(review: dict[str, Any], entry: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in review and review[key] is not None:
            return review[key]
        if key in entry and entry[key] is not None:
            return entry[key]
    return None


def group_by_episode(entries: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        grouped.setdefault(entry["episode_id"], []).append(entry)
    return dict(sorted(grouped.items()))


def find_audio(
    episode_id: str,
    entries: list[dict[str, Any]],
    audio_dir: Path,
    queue_dir: Path,
) -> Path | None:
    for entry in entries:
        raw = clean_string(entry.get("audio_path"))
        if not raw:
            continue
        candidate = resolve_local_then_repo(raw, queue_dir)
        if candidate.is_file():
            return candidate
    if audio_dir.is_dir():
        for child in sorted(audio_dir.iterdir()):
            if child.is_file() and child.stem == episode_id and child.suffix.lower() in AUDIO_EXTENSIONS:
                return child.resolve()
    return None


def sha256_fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def duration_from_wav(path: Path) -> float | None:
    try:
        with wave.open(str(path), "rb") as handle:
            rate = handle.getframerate()
            if rate <= 0:
                return None
            return handle.getnframes() / float(rate)
    except (wave.Error, OSError, EOFError):
        return None


def run_duration_probe(command: list[str], pattern: re.Pattern[str] | None = None) -> float | None:
    try:
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError:
        return None
    output = f"{completed.stdout}\n{completed.stderr}"
    if pattern:
        match = pattern.search(output)
        if not match:
            return None
        return number(match.group(1))
    for line in output.splitlines():
        candidate = number(line.strip())
        if candidate and candidate > 0:
            return candidate
    return None


def duration_from_audio(path: Path) -> float | None:
    if path.suffix.lower() == ".wav":
        wav_duration = duration_from_wav(path)
        if wav_duration and wav_duration > 0:
            return wav_duration

    ffprobe_candidates = [Path("/opt/homebrew/bin/ffprobe")]
    found_ffprobe = shutil.which("ffprobe")
    if found_ffprobe:
        ffprobe_candidates.append(Path(found_ffprobe))
    for ffprobe in ffprobe_candidates:
        if ffprobe.exists():
            duration = run_duration_probe(
                [
                    str(ffprobe),
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration",
                    "-of",
                    "default=noprint_wrappers=1:nokey=1",
                    str(path),
                ]
            )
            if duration and duration > 0:
                return duration

    afinfo_candidates = [Path("/usr/bin/afinfo")]
    found_afinfo = shutil.which("afinfo")
    if found_afinfo:
        afinfo_candidates.append(Path(found_afinfo))
    afinfo_pattern = re.compile(r"(?:estimated duration|duration):\s*([0-9]+(?:\.[0-9]+)?)\s*sec", re.I)
    for afinfo in afinfo_candidates:
        if afinfo.exists():
            duration = run_duration_probe([str(afinfo), str(path)], afinfo_pattern)
            if duration and duration > 0:
                return duration
    return None


def duration_from_metadata(entries: list[dict[str, Any]], reviews: dict[str, Any]) -> float | None:
    for entry in entries:
        review = review_for(entry, reviews)
        for source in (review, entry):
            for key in ("duration_seconds", "duration", "episode_duration_seconds"):
                candidate = number(source.get(key))
                if candidate and candidate > 0:
                    return candidate
    return None


def show_name_for(episode_id: str, entries: list[dict[str, Any]], reviews: dict[str, Any]) -> tuple[str, bool]:
    for entry in entries:
        review = review_for(entry, reviews)
        for source in (review, entry):
            show_name = clean_string(source.get("show_name"))
            if show_name:
                return show_name, False

    draft = CORPUS_DIR / "Drafts" / f"{episode_id}.draft.json"
    payload = load_json(draft)
    if isinstance(payload, dict):
        show_name = clean_string(payload.get("show_name"))
        if show_name:
            return show_name, False
    return episode_id, True


def variant_of_for(entries: list[dict[str, Any]], reviews: dict[str, Any]) -> str | None:
    for entry in entries:
        review = review_for(entry, reviews)
        for source in (review, entry):
            variant_of = clean_string(source.get("variant_of"))
            if variant_of:
                return variant_of
    return None


def confidence_notes(review: dict[str, Any], entry: dict[str, Any]) -> str | None:
    notes = clean_string(review.get("confidence_notes"))
    if notes:
        return notes
    review_notes = clean_string(review.get("notes"))
    boundary = clean_string(review.get("boundary_confidence"))
    if review_notes and boundary:
        return f"{review_notes} Boundary confidence: {boundary}."
    if review_notes:
        return review_notes
    return clean_string(entry.get("notes"))


def build_content_windows(duration: float, ad_windows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    content: list[dict[str, Any]] = []
    cursor = 0.0
    sorted_ads = sorted(ad_windows, key=lambda item: item["start_seconds"])
    for index, ad in enumerate(sorted_ads):
        start = float(ad["start_seconds"])
        if start > cursor:
            content.append(
                {
                    "start_seconds": cursor,
                    "end_seconds": start,
                    "notes": content_note(index, len(sorted_ads)),
                }
            )
        cursor = float(ad["end_seconds"])
    if cursor < duration:
        content.append(
            {
                "start_seconds": cursor,
                "end_seconds": duration,
                "notes": "Post-ad content - must NEVER be skipped"
                if sorted_ads
                else "Reviewed no-ad content - must NEVER be skipped",
            }
        )
    return content


def clamp_ad_windows_to_duration(
    report: EpisodeReport,
    duration: float,
    ad_windows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    clamped: list[dict[str, Any]] = []
    for index, ad in enumerate(ad_windows, start=1):
        normalized = dict(ad)
        end = float(normalized["end_seconds"])
        if duration < end <= duration + BOUNDARY_TOLERANCE_SECONDS:
            normalized["end_seconds"] = duration
            report.warnings.append(
                f"clamped_timing: ad {index} end {end} is within "
                f"{BOUNDARY_TOLERANCE_SECONDS:.2f}s of duration {duration}"
            )
        clamped.append(normalized)
    return clamped


def content_note(index: int, ad_count: int) -> str:
    if index == 0:
        return "Pre-ad content - must NEVER be skipped"
    if index >= ad_count:
        return "Post-ad content - must NEVER be skipped"
    return "Inter-ad content - must NEVER be skipped"


def validate_ad_timing(
    report: EpisodeReport,
    ad_windows: list[dict[str, Any]],
    duration: float | None,
) -> None:
    if duration is None:
        return
    sorted_ads = sorted(ad_windows, key=lambda item: item["start_seconds"])
    for index, ad in enumerate(sorted_ads):
        start = float(ad["start_seconds"])
        end = float(ad["end_seconds"])
        if start < 0:
            report.issues.append(f"invalid_timing: ad {index + 1} start {start} is negative")
        if end <= start:
            report.issues.append(f"invalid_timing: ad {index + 1} end {end} <= start {start}")
        if end > duration + BOUNDARY_TOLERANCE_SECONDS:
            report.issues.append(
                f"invalid_timing: ad {index + 1} end {end} exceeds duration {duration}"
            )
        if index > 0:
            previous_end = float(sorted_ads[index - 1]["end_seconds"])
            if start < previous_end:
                report.issues.append(
                    f"overlap: ad {index} ending {previous_end} overlaps ad {index + 1} starting {start}"
                )


def analyze_episode(
    episode_id: str,
    entries: list[dict[str, Any]],
    reviews: dict[str, Any],
    audio_dir: Path,
    queue_dir: Path,
) -> EpisodeReport:
    report = EpisodeReport(episode_id=episode_id, entries=entries)

    if not safe_artifact_basename(episode_id):
        report.issues.append("unsafe_episode_id: episode_id cannot contain path separators")

    audio_path = find_audio(episode_id, entries, audio_dir, queue_dir)
    report.audio_path = audio_path
    if audio_path is None:
        report.issues.append("missing_audio: no local audio file found")

    duration = duration_from_audio(audio_path) if audio_path is not None else None
    if duration is None:
        duration = duration_from_metadata(entries, reviews)
    report.duration_seconds = duration
    if duration is None:
        report.issues.append("missing_duration: unable to determine episode duration")

    show_name, inferred_show_name = show_name_for(episode_id, entries, reviews)
    if inferred_show_name:
        report.issues.append("missing_show_name: no show_name found in review, queue, or draft")
    variant_of = variant_of_for(entries, reviews)
    if variant_of == episode_id:
        report.issues.append("invalid_variant: variant_of cannot equal episode_id")

    ad_windows: list[dict[str, Any]] = []
    reviewed_no_ad_trap = False
    for entry in entries:
        review = review_for(entry, reviews)
        status = clean_string(review.get("status")) or "unreviewed"
        entry_id = entry["id"]
        if status == "unreviewed":
            report.issues.append(f"unreviewed: {entry_id}")
            continue
        if status == "unsure":
            report.issues.append(f"unsure: {entry_id}")
            continue
        if status not in FINAL_REVIEW_STATUSES:
            report.issues.append(f"invalid_status: {entry_id} has status {status}")
            continue
        is_trap = is_false_positive_trap(entry)
        if status == "zero_ad_confirmed" and not is_trap:
            report.issues.append(f"invalid_zero_ad_confirmation: {entry_id} is not a trap entry")
            continue
        if status in {"false_positive", "zero_ad_confirmed"}:
            if is_trap:
                reviewed_no_ad_trap = True
            continue

        start = number(value_from(review, entry, "start_seconds", "start"))
        end = number(value_from(review, entry, "end_seconds", "end"))
        advertiser = clean_string(value_from(review, entry, "advertiser", "advertiser_guess"))
        product = clean_string(value_from(review, entry, "product", "product_guess"))
        ad_type = clean_string(value_from(review, entry, "ad_type"))
        transition_type = clean_string(value_from(review, entry, "transition_type"))
        notes = confidence_notes(review, entry)

        missing = []
        for name, value in (
            ("start_seconds", start),
            ("end_seconds", end),
            ("advertiser", advertiser),
            ("product", product),
            ("ad_type", ad_type),
            ("transition_type", transition_type),
            ("confidence_notes", notes),
        ):
            if value is None:
                missing.append(name)
        if missing:
            report.issues.append(f"missing_ad_metadata: {entry_id} missing {', '.join(missing)}")
            continue
        if ad_type not in VALID_AD_TYPES:
            report.issues.append(f"invalid_ad_type: {entry_id} has {ad_type}")
            continue
        if transition_type not in VALID_TRANSITION_TYPES:
            report.issues.append(f"invalid_transition_type: {entry_id} has {transition_type}")
            continue

        ad_windows.append(
            {
                "start_seconds": start,
                "end_seconds": end,
                "advertiser": advertiser,
                "product": product,
                "ad_type": ad_type,
                "transition_type": transition_type,
                "confidence_notes": notes,
            }
        )

    validate_ad_timing(report, ad_windows, duration)
    if not report.issues and not ad_windows and not reviewed_no_ad_trap:
        report.issues.append(
            "invalid_no_ad_annotation: no-ad promotion requires a reviewed false_positive_trap entry"
        )

    if report.issues or duration is None or audio_path is None:
        return report

    sorted_ads = clamp_ad_windows_to_duration(
        report,
        duration,
        sorted(ad_windows, key=lambda item: item["start_seconds"]),
    )
    report.annotation = {
        "episode_id": episode_id,
        "show_name": show_name,
        "duration_seconds": duration,
        "ad_windows": sorted_ads,
        "content_windows": build_content_windows(duration, sorted_ads),
        "variant_of": variant_of,
        "audio_fingerprint": sha256_fingerprint(audio_path),
    }
    return report


def review_status_counts(entries: list[dict[str, Any]], reviews: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entry in entries:
        status = clean_string(review_for(entry, reviews).get("status")) or "unreviewed"
        counts[status] = counts.get(status, 0) + 1
    return dict(sorted(counts.items()))


def category_for(entry: dict[str, Any], reviews: dict[str, Any]) -> str:
    review = review_for(entry, reviews)
    for source in (review, entry):
        category = clean_string(source.get("category")) or clean_string(source.get("corpus_category"))
        if category:
            return category
    status = clean_string(review.get("status")) or "unreviewed"
    if is_false_positive_trap(entry):
        return "edge_zero_or_false_positive_trap"
    if status == "false_positive":
        return "false_positive_rejected"
    return clean_string(value_from(review, entry, "ad_type")) or "unknown"


def category_coverage(entries: list[dict[str, Any]], reviews: dict[str, Any]) -> dict[str, dict[str, int]]:
    coverage: dict[str, dict[str, int]] = {}
    for entry in entries:
        category = category_for(entry, reviews)
        status = clean_string(review_for(entry, reviews).get("status")) or "unreviewed"
        bucket = coverage.setdefault(category, {"total": 0, "reviewed": 0, "verified_ads": 0})
        bucket["total"] += 1
        if status != "unreviewed":
            bucket["reviewed"] += 1
        if status == "verified_ad":
            bucket["verified_ads"] += 1
    return dict(sorted(coverage.items()))


def print_report(
    review_path: Path,
    review_missing: bool,
    queue_path: Path,
    entries: list[dict[str, Any]],
    reports: list[EpisodeReport],
    reviews: dict[str, Any],
) -> None:
    print("L2F reviewed corpus promotion report")
    print(f"review_file: {review_path}")
    if review_missing:
        print("review_file_status: missing (treating all queue entries as unreviewed)")
    print(f"queue_path: {queue_path}")
    print(f"entries: {len(entries)}")
    print(f"episodes: {len(reports)}")

    print("\nreview_debt:")
    for status, count in review_status_counts(entries, reviews).items():
        print(f"  {status}: {count}")

    print("\ncategory_coverage:")
    for category, counts in category_coverage(entries, reviews).items():
        print(
            f"  {category}: total={counts['total']} "
            f"reviewed={counts['reviewed']} verified_ads={counts['verified_ads']}"
        )

    print("\nepisode_readiness:")
    for report in reports:
        state = "READY" if report.ready else "BLOCKED"
        ad_count = len(report.annotation["ad_windows"]) if report.annotation else 0
        duration = f"{report.duration_seconds:.3f}" if report.duration_seconds else "unknown"
        print(
            f"  {state} {report.episode_id}: entries={len(report.entries)} "
            f"ads={ad_count} duration={duration}"
        )
        for warning in report.warnings:
            print(f"    warning: {warning}")
        for issue in report.issues:
            print(f"    issue: {issue}")


def write_annotation(path: Path, annotation: dict[str, Any], force: bool) -> None:
    if path.exists() and not force:
        raise FileExistsError(f"{path} already exists; pass --force to overwrite")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(annotation, handle, indent=2)
        handle.write("\n")
    tmp.replace(path)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    review_path = resolve_path(args.review_file)
    audio_dir = resolve_path(args.audio_dir)
    annotations_dir = resolve_path(args.annotations_dir)

    try:
        review_payload, reviews, review_missing = load_review_file(review_path)
        queue_path = choose_queue_path(args, review_path, review_payload)
        entries = load_queue(queue_path)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    selected = set(args.episode)
    if selected:
        available = {entry["episode_id"] for entry in entries}
        missing = sorted(selected - available)
        if missing:
            print(
                "error: episode_not_found: "
                + ", ".join(missing)
                + f" not present in {queue_path}",
                file=sys.stderr,
            )
            return 2
        entries = [entry for entry in entries if entry["episode_id"] in selected]
    if not entries:
        print(f"error: no review queue entries found in {queue_path}", file=sys.stderr)
        return 2
    grouped = group_by_episode(entries)
    reports = [
        analyze_episode(episode_id, episode_entries, reviews, audio_dir, queue_path.parent)
        for episode_id, episode_entries in grouped.items()
    ]

    for report in reports:
        out = annotations_dir / f"{report.episode_id}.json"
        if out.exists() and not args.force:
            report.issues.append("annotation_exists: pass --force to overwrite existing annotation")
            report.annotation = None

    print_report(review_path, review_missing, queue_path, entries, reports, reviews)
    sys.stdout.flush()

    blocked = [report for report in reports if not report.ready]
    if args.promote:
        if blocked:
            print(
                f"\nrefusing promotion: {len(blocked)} episode(s) are not promotion-ready",
                file=sys.stderr,
            )
            return 2
        written: list[Path] = []
        try:
            for report in reports:
                assert report.annotation is not None
                out = annotations_dir / f"{report.episode_id}.json"
                write_annotation(out, report.annotation, args.force)
                written.append(out)
        except Exception as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        print("\npromoted_annotations:")
        for path in written:
            print(f"  {path}")
    else:
        ready = sum(1 for report in reports if report.ready)
        print(f"\ndry_run: no annotations written; ready_episodes={ready} blocked_episodes={len(blocked)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
