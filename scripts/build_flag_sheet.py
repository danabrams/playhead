#!/usr/bin/env python3
"""Derive the cohort build's flag sheet from source.

playhead-i7kvl.4. A tester report cannot be attributed to a build without
knowing what that build actually does, and a HAND-WRITTEN list of flags is a
second source for facts the code already states — it goes stale silently, which
is the failure this repo spends most of its time on.

So the sheet is DERIVED. Each entry names a constant and the file that declares
it, and the value is read out of the source at run time. A constant that has
been renamed or deleted is an ERROR, not a blank: a sheet that quietly drops a
row is worse than no sheet, because it still looks complete.

Usage:
    python3 scripts/build_flag_sheet.py                 # markdown table
    python3 scripts/build_flag_sheet.py --check         # exit 1 if any is missing
    python3 scripts/build_flag_sheet.py -o docs/cohort-build-flags.md
"""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys

# (constant, file, what it governs for a LISTENER). Ordered as a reader meets
# them: what gets skipped, what gets shown, what gets analysed, what is recorded.
FLAGS: list[tuple[str, str, str]] = [
    ("isEnabledByDefault", "Playhead/Services/AdDetection/RediffRefetch/RediffActivation.swift",
     "Day-0 rediff runs at all — the byte-exact channel that carries the launch promise"),
    ("isEnabledByDefault", "Playhead/Services/AdDetection/RediffRefetch/RediffRefetchService.swift",
     "The LAGGED rediff sweep (distinct from day-0)"),
    ("isEnabledByDefault", "Playhead/Services/SkipOrchestrator/AutoSkipEdgePadding.swift",
     "Asymmetric auto-skip edge padding (playhead-98co, dormant pending corpus growth)"),
    ("showIndependentSeedMode", "Playhead/Services/TrustScoring/SkipDetectorClass.swift",
     "The trust rung a byte-exact detector starts at — why a NEW show can auto-skip on episode one"),
    ("defaultAutoDismissSeconds", "Playhead/Views/Components/AdBannerView.swift",
     "How long a card stays on screen"),
    ("episodePreparationCompleteThreshold", "Playhead/Views/Library/EpisodePreparationReadiness.swift",
     "Ad-scan coverage at which the library shows the ready checkmark"),
    ("legalSignoffRecorded", "Playhead/Services/Analytics/AnalyticsRecordWriter.swift",
     "Analytics UPLOAD. Must be false — the cohort is measured from bundles people choose to send"),
    ("dayZeroKickoffResumeLimit", "Playhead/App/PlayheadRuntime.swift",
     "How many owed day-0 kickoffs one launch re-drives (playhead-jra6)"),
    ("dayZeroKickoffResumeGiveUpAfter", "Playhead/App/PlayheadRuntime.swift",
     "How many times one episode may be re-driven before the sweep stops"),
    ("schedulerEventsCap", "Playhead/Support/Diagnostics/DiagnosticsBundleBuilder.swift",
     "Scheduler rows in a report. The CENSUS is counted over the whole journal (playhead-yz3o)"),
    ("workJournalTailCap", "Playhead/Support/Diagnostics/DiagnosticsBundleBuilder.swift",
     "Work-journal rows in a report"),
]

# Matches a `static let`, a `nonisolated static let`, a `private static let`, and
# a FILE-SCOPE `let`. The last one is not pedantry: `--check` caught
# `episodePreparationCompleteThreshold` — a file-scope constant — being silently
# absent under a static-only pattern, which is exactly the row-dropping this
# script refuses to do.
DECL = (
    r"(?:^|\n)\s*(?:nonisolated\s+)?(?:private\s+|internal\s+|public\s+)?"
    r"(?:static\s+)?let\s+{name}\s*(?::[^=\n]+)?=\s*(?P<value>[^\n]+)"
)


def repo_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def read_value(root: pathlib.Path, constant: str, relative: str) -> str | None:
    path = root / relative
    try:
        source = path.read_text(encoding="utf-8")
    except OSError:
        return None
    match = re.search(DECL.format(name=re.escape(constant)), source)
    if not match:
        return None
    return match.group("value").strip().rstrip(",").strip()


def build_number(root: pathlib.Path) -> str:
    try:
        sha = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=10,
        ).stdout.strip()
        return sha or "unknown"
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true",
                        help="exit 1 if any listed constant cannot be found")
    parser.add_argument("-o", "--out", type=pathlib.Path)
    args = parser.parse_args(argv)

    root = repo_root()
    rows, missing = [], []
    for constant, relative, meaning in FLAGS:
        value = read_value(root, constant, relative)
        if value is None:
            missing.append(f"{constant} in {relative}")
            value = "**NOT FOUND**"
        rows.append((constant, relative, value, meaning))

    if args.check:
        for entry in missing:
            print(f"MISSING: {entry}", file=sys.stderr)
        if missing:
            print(
                f"{len(missing)} listed constant(s) could not be read. A sheet that "
                "silently drops a row still looks complete — fix the list or the code.",
                file=sys.stderr,
            )
            return 1
        print(f"all {len(FLAGS)} flags resolve")
        return 0

    lines = [
        "# Cohort build flags",
        "",
        "**Generated — do not edit by hand.** `python3 scripts/build_flag_sheet.py "
        "-o docs/cohort-build-flags.md`",
        "",
        f"Commit: `{build_number(root)}`",
        "",
        "Every value below is read out of the source named beside it, so this sheet "
        "cannot drift from the build. A constant that is renamed or deleted makes "
        "`--check` fail rather than leaving a blank row.",
        "",
        "| Flag | Value | What a listener gets | Declared in |",
        "| --- | --- | --- | --- |",
    ]
    for constant, relative, value, meaning in rows:
        lines.append(
            f"| `{constant}` | `{value}` | {meaning} | `{relative}` |"
        )
    lines += [
        "",
        "## Reading a tester report against this",
        "",
        "- A tester who sees **cards but never a skip** is most likely on shows whose "
        "network does not vary its ad insertion, so the byte-exact channel finds "
        "nothing. Roughly 29% of episodes on the 2026-09-02 pull. Not a skip-path defect.",
        "- A tester whose library shows **no ready checkmarks** is a coverage question, "
        "not a detection one: check the threshold row above.",
        "- If `legalSignoffRecorded` is ever `true` here, stop. Nothing should upload, "
        "and `docs/site/privacy.html` becomes false the moment it ships.",
    ]
    text = "\n".join(lines) + "\n"

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text, encoding="utf-8")
        print(f"wrote {args.out} ({len(rows)} flags)")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
