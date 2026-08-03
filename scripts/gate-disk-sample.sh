#!/usr/bin/env bash
#
# gate-disk-sample.sh — measure what a gate actually costs in disk (playhead-3nfa).
#
# The threshold in scripts/disk_preflight.py is a measurement, and this is the
# instrument that takes it. Run it alongside a gate; it samples free space on
# the data volume and, on exit, prints the drawdown:
#
#     drawdown = free_at_start - min_free_during
#
# That is the number the preflight threshold is built from, because it is what
# the run actually consumes at its worst moment — a full-plan gate is a
# TRANSIENT disk event (the simulator inflates during the Swift Testing bulk and
# shrinks back), so the end-state delta understates it badly.
#
# Usage:
#   scripts/gate-disk-sample.sh <csv-out> [interval-seconds]   # runs until killed
#
# Sample it from a shell that is NOT the one running the gate, and remember the
# reading is periodic: a spike between samples is invisible, which is one reason
# the threshold carries a margin over the observed drawdown.
set -u

OUT="${1:?usage: gate-disk-sample.sh <csv-out> [interval]}"
INTERVAL="${2:-5}"
VOL="${PLAYHEAD_DISK_VOLUME:-/System/Volumes/Data}"

avail_kib () { df -k "$VOL" | awk 'NR==2 {print $4}'; }

START="$(avail_kib)"
MIN="$START"
printf 'epoch,avail_kib\n' >"$OUT"

summary () {
  local end drawdown
  end="$(avail_kib)"
  drawdown=$(( START - MIN ))
  printf '\n'
  printf 'gate-disk-sample: volume      %s\n' "$VOL"
  printf 'gate-disk-sample: start       %.2f GiB\n' "$(echo "$START" | awk '{print $1/1048576}')"
  printf 'gate-disk-sample: minimum     %.2f GiB\n' "$(echo "$MIN" | awk '{print $1/1048576}')"
  printf 'gate-disk-sample: end         %.2f GiB\n' "$(echo "$end" | awk '{print $1/1048576}')"
  printf 'gate-disk-sample: DRAWDOWN    %.2f GiB   <- the number the threshold is built from\n' \
    "$(echo "$drawdown" | awk '{print $1/1048576}')"
  printf 'gate-disk-sample: net kept    %.2f GiB   (start - end; what did not shrink back)\n' \
    "$(echo "$(( START - end ))" | awk '{print $1/1048576}')"
  printf 'gate-disk-sample: samples in  %s\n' "$OUT"
}
trap 'summary; exit 0' INT TERM

while :; do
  a="$(avail_kib)"
  printf '%s,%s\n' "$(date +%s)" "$a" >>"$OUT"
  [ "$a" -lt "$MIN" ] && MIN="$a"
  sleep "$INTERVAL"
done
