#!/usr/bin/env python3
"""Turn a folder of cohort diagnostic bundles into the weekly table.

playhead-i7kvl.3. Dan recruits ~20 TestFlight listeners, they export a
diagnostic report from Settings, and this reads the folder of them.

THE NUMBER THIS EXISTS FOR is **manual skip-forward reaches per listening
hour**. Every manual reach is the listener hiring themselves to do work Playhead
should have done, which is why the competitor is the 30-second skip button and
not another podcast app.

WHY IT READS BUNDLES AND NOT AN UPLOAD. `AnalyticsUploadGate.legalSignoffRecorded`
is `false` and the production writer is `DisabledAnalyticsRecordWriter` — nothing
is transmitted, by product decision. The counters ride in the user-initiated
diagnostics bundle instead, so the cohort is measured from reports people chose
to send.

TWO RULES THIS SCRIPT WILL NOT BEND, both of them the standing defect class:

  * **A missing counter reads `not recorded`, never `0`.** A bundle from a build
    predating the counters, or one whose store was never consulted, carries
    `analytics_counters.recorded == false`. Printing `0` there would put a
    fabricated zero into the numerator of the metric the whole launch window is
    organised around.
  * **Every rate names its numerator and denominator in the header**, and a rate
    whose denominator is zero prints `—` rather than a division. "Reaches" and
    "reaches per hour" are different quantities and the table says which is
    which.

Usage:
    python3 scripts/cohort_readout.py <folder-of-bundles> [--json]
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

# The counter keys this readout uses, spelled as `AnalyticsMetricKey` does.
REACHES = "manual_skip_forward_reaches"
LISTENING_SECONDS = "listening_seconds"
BANNERS_SHOWN = "banners_shown"
BANNERS_CONFIRMED = "banners_confirmed"
BANNERS_DENIED = "banners_denied"

NOT_RECORDED = "not recorded"


class Reading:
    """One bundle, reduced to what the weekly table needs.

    Every count is `int` or `None`, and `None` means NOBODY COUNTED — it is
    never coerced to zero on the way in, because the coercion is the defect.
    """

    def __init__(self, name: str):
        self.name = name
        self.error: str | None = None
        self.recorded = False
        self.counts: dict[str, int] = {}
        self.scheduler_total: int | None = None
        self.scheduler_truncated: bool | None = None
        self.auto_skips: int | None = None
        # playhead-h9y6: launch-path failures the app used to swallow. None = the
        # bundle predates the recorder, never 0.
        self.bootstrap_failures: int | None = None

    def count(self, key: str) -> int | None:
        if not self.recorded:
            return None
        return self.counts.get(key, 0)

    @property
    def listening_hours(self) -> float | None:
        seconds = self.count(LISTENING_SECONDS)
        return None if seconds is None else seconds / 3600.0

    @property
    def reaches_per_hour(self) -> float | None:
        """THE NORTH STAR. `None` when either term is unavailable, and when the
        denominator is zero — a reach count over no listening is not a rate."""
        reaches = self.count(REACHES)
        hours = self.listening_hours
        if reaches is None or hours is None or hours <= 0:
            return None
        return reaches / hours


def _sum_cohorts(by_cohort: dict) -> int:
    """Total a metric across cohorts.

    The bundle keys counts by cohort (episode-duration bucket, plus `all`), and
    the readout wants the population. Summing is correct because the buckets
    partition — `all` is used only for counters that are not episode-scoped, and
    a metric is written under one or the other, never both.
    """
    return sum(v for v in by_cohort.values() if isinstance(v, int) and v > 0)


def read_bundle(path: pathlib.Path) -> Reading:
    reading = Reading(path.name)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        reading.error = f"unreadable: {error}"
        return reading

    default = data.get("default")
    if not isinstance(default, dict):
        reading.error = "no `default` subtree — is this a Playhead bundle?"
        return reading

    counters = default.get("analytics_counters")
    if isinstance(counters, dict):
        # `recorded` is the discriminator. Absent means a bundle written before
        # the field existed, which is also "nobody counted".
        reading.recorded = bool(counters.get("recorded", False))
        by_metric = counters.get("by_metric")
        if isinstance(by_metric, dict):
            for metric, by_cohort in by_metric.items():
                if isinstance(by_cohort, dict):
                    reading.counts[metric] = _sum_cohorts(by_cohort)

    health = default.get("launch_health")
    if isinstance(health, dict) and health.get("recorded"):
        reading.bootstrap_failures = health.get("download_bootstrap_failures")
    census = default.get("scheduler_event_census")
    if isinstance(census, dict):
        reading.scheduler_total = census.get("total")
        reading.scheduler_truncated = census.get("truncated")

    # Auto-skips come from the surface-status side of the bundle, which is a
    # different instrument with a different population — counted separately and
    # never folded into the counter totals.
    events = default.get("scheduler_events")
    if isinstance(events, list):
        reading.auto_skips = sum(
            1 for e in events
            if isinstance(e, dict) and e.get("event_type") == "auto_skip_fired"
        )
    return reading


def _fmt_int(value: int | None) -> str:
    return NOT_RECORDED if value is None else str(value)


def _fmt_rate(value: float | None) -> str:
    return "—" if value is None else f"{value:.2f}"


def render(readings: list[Reading]) -> str:
    lines: list[str] = []
    lines.append("COHORT READOUT")
    lines.append("")
    lines.append(
        "  THE NORTH STAR is reaches/hour: manual +30s presses (numerator) over"
    )
    lines.append(
        "  listening seconds/3600 (denominator). Both come from the SAME bundle,"
    )
    lines.append("  so a per-tester rate never mixes populations.")
    lines.append("")

    header = (
        f"  {'bundle':<28} {'reaches':>9} {'hours':>8} {'reach/hr':>9} "
        f"{'shown':>7} {'conf':>6} {'denied':>7} {'skips':>7} {'boot!':>6}"
    )
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))

    usable = 0
    total_reaches = 0
    total_seconds = 0
    for reading in sorted(readings, key=lambda r: r.name):
        if reading.error:
            lines.append(f"  {reading.name:<28} !! {reading.error}")
            continue
        hours = reading.listening_hours
        lines.append(
            f"  {reading.name:<28} "
            f"{_fmt_int(reading.count(REACHES)):>9} "
            f"{('—' if hours is None else f'{hours:.1f}'):>8} "
            f"{_fmt_rate(reading.reaches_per_hour):>9} "
            f"{_fmt_int(reading.count(BANNERS_SHOWN)):>7} "
            f"{_fmt_int(reading.count(BANNERS_CONFIRMED)):>6} "
            f"{_fmt_int(reading.count(BANNERS_DENIED)):>7} "
            f"{_fmt_int(reading.auto_skips):>7} "
            f"{_fmt_int(reading.bootstrap_failures):>6}"
        )
        if reading.recorded:
            usable += 1
            total_reaches += reading.count(REACHES) or 0
            total_seconds += reading.count(LISTENING_SECONDS) or 0

    lines.append("")
    # The population is stated rather than implied. A cohort rate computed over
    # an unstated number of bundles is the shape this whole script exists to
    # avoid.
    lines.append(
        f"  {usable} of {len(readings)} bundle(s) carried counters; "
        f"the rest report `{NOT_RECORDED}` and are EXCLUDED from the total below."
    )
    if usable and total_seconds > 0:
        lines.append(
            f"  COHORT reaches/hour = {total_reaches} / "
            f"{total_seconds / 3600.0:.1f} h = "
            f"{total_reaches / (total_seconds / 3600.0):.2f}"
        )
    else:
        lines.append(
            "  COHORT reaches/hour = — (no listening time recorded; a reach "
            "count over no listening is not a rate)"
        )

    truncated = [r.name for r in readings if r.scheduler_truncated]
    if truncated:
        lines.append("")
        lines.append(
            "  NOTE: scheduler tail saturated in "
            f"{len(truncated)} bundle(s) — counts above are unaffected "
            "(they come from the census, not the tail)."
        )
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=pathlib.Path)
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    args = parser.parse_args(argv)

    if not args.folder.is_dir():
        print(f"not a directory: {args.folder}", file=sys.stderr)
        return 2

    paths = sorted(args.folder.glob("*.json"))
    if not paths:
        print(f"no .json bundles in {args.folder}", file=sys.stderr)
        return 2

    readings = [read_bundle(p) for p in paths]

    if args.json:
        print(json.dumps([
            {
                "bundle": r.name,
                "error": r.error,
                "recorded": r.recorded,
                "reaches": r.count(REACHES),
                "listening_seconds": r.count(LISTENING_SECONDS),
                "reaches_per_hour": r.reaches_per_hour,
                "banners_shown": r.count(BANNERS_SHOWN),
                "banners_confirmed": r.count(BANNERS_CONFIRMED),
                "banners_denied": r.count(BANNERS_DENIED),
                "auto_skips": r.auto_skips,
                "scheduler_total": r.scheduler_total,
                "scheduler_truncated": r.scheduler_truncated,
            }
            for r in sorted(readings, key=lambda r: r.name)
        ], indent=2))
    else:
        print(render(readings))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
