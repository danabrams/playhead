#!/bin/bash
# analytics-rollup.sh — playhead-jw63.3
#
# The v1 "dashboard": a weekly text report. There is no web dashboard and
# there will not be one until the user count makes reading this tedious.
#
# The device only ever writes anonymous increment records. This is the read
# side: fetch them, sum them, and print the one number that matters —
#
#     manual +30s reaches per listening hour
#
# — because the thing Playhead replaces is the 30-second skip button.
#
# Usage:
#   scripts/analytics-rollup.sh --input records.json    # roll up a saved export
#   scripts/analytics-rollup.sh --team-id TEAMID        # fetch, then roll up
#   scripts/analytics-rollup.sh --input r.json --k 1    # lower the k floor
#
# Fetching needs a CloudKit management token in the environment
# (CLOUDKIT_MANAGEMENT_TOKEN) and `xcrun cktool`, which ships with Xcode. No
# new dependency; nothing here runs on a device or in the app.
#
# k-anonymity: envelope §6.5 proposes k >= 20 before an aggregate is
# reported. The device cannot enforce that — it has no idea how many peers
# exist — so it is enforced here. A cohort with fewer than k contributing
# records prints as "suppressed (n<k)", not as a number.

set -uo pipefail

CONTAINER="iCloud.com.playhead.app"
RECORD_TYPE="AnalyticsIncrement"
ENVIRONMENT="production"
K_FLOOR=20
INPUT=""
TEAM_ID=""

usage() {
  sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --input)       INPUT="${2:-}"; shift 2 ;;
    --team-id)     TEAM_ID="${2:-}"; shift 2 ;;
    --container)   CONTAINER="${2:-}"; shift 2 ;;
    --environment) ENVIRONMENT="${2:-}"; shift 2 ;;
    --k)           K_FLOOR="${2:-}"; shift 2 ;;
    -h|--help)     usage 0 ;;
    *) echo "analytics-rollup: unknown argument '$1'" >&2; usage 64 ;;
  esac
done

if [ -z "$INPUT" ]; then
  if [ -z "$TEAM_ID" ]; then
    echo "analytics-rollup: pass --input <file> or --team-id <id>" >&2
    exit 64
  fi
  if [ -z "${CLOUDKIT_MANAGEMENT_TOKEN:-}" ]; then
    echo "analytics-rollup: CLOUDKIT_MANAGEMENT_TOKEN is not set" >&2
    exit 64
  fi
  INPUT="$(mktemp -t playhead-analytics)"
  trap 'rm -f "$INPUT"' EXIT
  xcrun cktool query-records \
    --team-id "$TEAM_ID" \
    --container-id "$CONTAINER" \
    --environment "$ENVIRONMENT" \
    --database-type public \
    --zone-name _defaultZone \
    --record-type "$RECORD_TYPE" \
    --output-file "$INPUT" || {
      echo "analytics-rollup: cktool query failed" >&2
      exit 70
    }
fi

if [ ! -f "$INPUT" ]; then
  echo "analytics-rollup: no such file: $INPUT" >&2
  exit 66
fi

K_FLOOR="$K_FLOOR" INPUT="$INPUT" python3 <<'PY'
import json
import os
import sys

K = int(os.environ["K_FLOOR"])
PATH = os.environ["INPUT"]

METRICS = [
    "banners_shown", "banners_confirmed", "banners_denied",
    "manual_skip_forward_reaches", "listening_seconds",
    "retention_installs", "retention_d1_returned",
    "retention_d7_returned", "retention_d30_returned",
]
COHORTS = ["all", "under30m", "between30and60m", "between60and90m", "over90m"]
SCHEMA = "playhead.analytics.increment.v1"

with open(PATH, "r", encoding="utf-8") as handle:
    raw = json.load(handle)

# cktool wraps rows under "records"; a hand-saved export may be a bare list.
rows = raw.get("records", raw) if isinstance(raw, dict) else raw
if not isinstance(rows, list):
    sys.exit("analytics-rollup: expected a list of records")


def fields(row):
    """cktool renders fields as {name: {value: v, type: t}}; accept flat too."""
    section = row.get("fields", row) if isinstance(row, dict) else {}
    flat = {}
    for key, value in section.items():
        if isinstance(value, dict) and "value" in value:
            flat[key] = value["value"]
        else:
            flat[key] = value
    return flat


totals = {cohort: dict.fromkeys(METRICS, 0) for cohort in COHORTS}
counts = dict.fromkeys(COHORTS, 0)
rejected = 0

for row in rows:
    flat = fields(row)
    if flat.get("payload_schema") != SCHEMA or flat.get("envelope_version") != 1:
        rejected += 1
        continue
    cohort = flat.get("cohort_duration_bucket")
    if cohort not in totals:
        rejected += 1
        continue
    counts[cohort] += 1
    for metric in METRICS:
        value = flat.get(metric, 0)
        if isinstance(value, int) and value > 0:
            totals[cohort][metric] += value


def summed(metric, cohorts=COHORTS):
    return sum(totals[c][metric] for c in cohorts)


def contributing(cohorts=COHORTS):
    return sum(counts[c] for c in cohorts)


def cell(value, n):
    return str(value) if n >= K else f"suppressed (n={n}<{K})"


print("Playhead — weekly counters (telemetry envelope v1, addendum A)")
print(f"records: {len(rows)}   accepted: {contributing()}   rejected: {rejected}")
print(f"k-anonymity floor: {K}")
print()

overall_n = contributing()
reaches = summed("manual_skip_forward_reaches")
seconds = summed("listening_seconds")
hours = seconds / 3600.0

print("NORTH STAR — manual +30s reaches per listening hour")
if overall_n < K:
    print(f"  suppressed (n={overall_n}<{K})")
elif hours <= 0:
    print("  no listening time recorded")
else:
    print(f"  {reaches / hours:.2f}   ({reaches} reaches / {hours:.1f} hours)")
print()

print("Trust — 'Was this right?'")
shown = summed("banners_shown")
yes = summed("banners_confirmed")
no = summed("banners_denied")
answered = yes + no
print(f"  banners shown:     {cell(shown, overall_n)}")
print(f"  answered yes:      {cell(yes, overall_n)}")
print(f"  answered no:       {cell(no, overall_n)}")
if overall_n >= K and answered > 0:
    print(f"  agreement rate:    {100.0 * yes / answered:.1f}%  (n={answered})")
    if shown > 0:
        print(f"  response rate:     {100.0 * answered / shown:.1f}%")
print()

print("Retention (window ratio; returns are unlinkable to installs by design)")
installs = summed("retention_installs")
print(f"  installs:          {cell(installs, overall_n)}")
for label, metric in (
    ("D1 returned", "retention_d1_returned"),
    ("D7 returned", "retention_d7_returned"),
    ("D30 returned", "retention_d30_returned"),
):
    value = summed(metric)
    if overall_n >= K and installs > 0:
        print(f"  {label:<17}{value}  ({100.0 * value / installs:.0f}% of installs)")
    else:
        print(f"  {label:<17}{cell(value, overall_n)}")
print()

print("By episode-duration cohort")
header = f"  {'cohort':<18}{'records':>9}{'reaches':>9}{'hours':>9}{'per hour':>13}"
print(header)
for cohort in COHORTS:
    n = counts[cohort]
    cohort_reaches = totals[cohort]["manual_skip_forward_reaches"]
    cohort_hours = totals[cohort]["listening_seconds"] / 3600.0
    if n < K:
        rate = "suppressed"
    elif cohort_hours <= 0:
        rate = "-"
    else:
        rate = f"{cohort_reaches / cohort_hours:.2f}"
    print(f"  {cohort:<18}{n:>9}{cohort_reaches:>9}{cohort_hours:>9.1f}{rate:>13}")
PY
