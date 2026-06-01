#!/usr/bin/env bash
# l2f-morning-check.sh — 30-second triage of the autonomous corpus loop.
#
# Run this in the morning (or whenever) to assess the autonomous Tier-A
# corpus-growth state in one command. Composes:
#   1. l2f-corpus-status.py --terse  — one-line operational summary
#   2. last 3 daily-loop.log lines
#   3. loop-watch.log status (empty = healthy; non-empty = stale alarm)
#   4. l2f-bd4xqf-analyze.py        — boundary-undersizing verdict if dump fresh
#   5. l2f-audit-queue.py --limit=3  — next 3 audit-priority=1 spans to spot-check
#
# Usage:
#     scripts/l2f-morning-check.sh
#     scripts/l2f-morning-check.sh --quiet   # suppress section dividers
#
# Exit code is always 0 — this is a status report, not a gate.

set -u

REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO" || exit 0

QUIET=0
for arg in "$@"; do
  case "$arg" in
    --quiet|-q) QUIET=1 ;;
  esac
done

section() {
  if [ "$QUIET" -eq 0 ]; then
    echo ""
    echo "─── $1 ───"
  fi
}

# 1. One-line operational summary.
section "corpus status"
if [ -x scripts/l2f-corpus-status.py ]; then
  scripts/l2f-corpus-status.py --terse 2>&1
else
  echo "  (scripts/l2f-corpus-status.py not executable)"
fi

# 2. Last 3 daily-loop runs (one-line each).
section "daily-loop.log (last 3 runs)"
LOG=TestFixtures/Corpus/Snapshots/daily-loop.log
if [ -f "$LOG" ]; then
  tail -3 "$LOG" | sed 's/^/  /'
else
  echo "  (no daily-loop.log yet — first run hasn't completed)"
fi

# 3. Watchdog health. Empty file = healthy.
section "loop-watch health"
WLOG=TestFixtures/Corpus/Snapshots/loop-watch.log
if [ -f "$WLOG" ]; then
  size=$(stat -f "%z" "$WLOG" 2>/dev/null || stat -c "%s" "$WLOG" 2>/dev/null || echo "?")
  if [ "$size" = "0" ]; then
    echo "  ✓ watchdog log empty (loop is healthy)"
  else
    echo "  ⚠️  watchdog log non-empty (${size} bytes):"
    sed 's/^/    /' "$WLOG"
  fi
else
  echo "  (watchdog log file not yet created — first fire is within 6h of install)"
fi

# 4. Boundary-undersizing verdict — only meaningful if the dump has the new field.
section "bd-4xqf boundary-undersizing verdict"
if [ -x scripts/l2f-bd4xqf-analyze.py ]; then
  # The analyzer prints its own graceful-fallback message if dump is pre-#201.
  scripts/l2f-bd4xqf-analyze.py 2>&1 | head -25
else
  echo "  (scripts/l2f-bd4xqf-analyze.py not executable)"
fi

# 5. Top 3 audit-priority=1 spans waiting to be spot-checked.
section "audit queue (top 3)"
if [ -x scripts/l2f-audit-queue.py ]; then
  scripts/l2f-audit-queue.py --limit=3 2>&1 | head -20
else
  echo "  (scripts/l2f-audit-queue.py not executable)"
fi

# Final summary line — useful when piping to grep or notification.
if [ "$QUIET" -eq 0 ]; then
  echo ""
  echo "─── done ───"
fi
