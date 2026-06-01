#!/usr/bin/env bash
# l2f-loop-watch.sh — daily-loop health watchdog
#
# Checks that daily-loop.log was updated within the last 25 hours.
# Exits 1 (with a message on stderr) if stale or missing; exits 0 silently if fresh.
#
# Intended to be run every 6 hours via launchd so that a silent loop failure
# is caught within one watchdog cycle after the 03:30 run window passes.
#
# ──────────────────────────────────────────────────────────────────────────────
# INSTALL (optional, NOT auto-installed)
#
#   1. Copy (or symlink) the paired plist into your LaunchAgents directory:
#
#        cp /Users/dabrams/playhead/scripts/com.playhead.l2f.loop-watch.plist \
#           ~/Library/LaunchAgents/
#
#   2. Bootstrap the job:
#
#        launchctl bootstrap gui/$(id -u) \
#          ~/Library/LaunchAgents/com.playhead.l2f.loop-watch.plist
#
#   3. To uninstall:
#
#        launchctl bootout gui/$(id -u)/com.playhead.l2f.loop-watch
#
#   4. To trigger an immediate one-off run:
#
#        launchctl kickstart -k gui/$(id -u)/com.playhead.l2f.loop-watch
#
#   5. To inspect status / next-fire / last exit code:
#
#        launchctl print gui/$(id -u)/com.playhead.l2f.loop-watch
#
#   Watchdog output goes to:
#     TestFixtures/Corpus/Snapshots/loop-watch.log  (both stdout and stderr)
#
# ──────────────────────────────────────────────────────────────────────────────
# TESTING (env-var override)
#
#   Override the log path for testing without modifying the production path:
#
#     PLAYHEAD_LOOP_WATCH_LOG=/tmp/playhead-loop-watch-test.log \
#       scripts/l2f-loop-watch.sh
#
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# Repo root: the directory containing this script's parent.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# Production log path; overridable for testing.
LOG_FILE="${PLAYHEAD_LOOP_WATCH_LOG:-"$REPO_ROOT/TestFixtures/Corpus/Snapshots/daily-loop.log"}"

# Threshold: 25 hours in seconds.
THRESHOLD_SECS=$(( 25 * 3600 ))

NOW=$(date +%s)

# ── Resolve mtime ─────────────────────────────────────────────────────────────

if [[ ! -e "$LOG_FILE" ]]; then
    ISO_TIMESTAMP="(file does not exist)"
    MTIME=0
elif [[ ! -r "$LOG_FILE" ]]; then
    ISO_TIMESTAMP="(file is not readable)"
    MTIME=0
else
    # macOS (BSD) stat: -f %m gives mtime as Unix epoch seconds.
    MTIME=$(stat -f %m "$LOG_FILE" 2>/dev/null || echo 0)
    if [[ "$MTIME" -eq 0 ]]; then
        ISO_TIMESTAMP="(could not stat file)"
    else
        # BSD date: -r <epoch> for human-readable ISO-8601-ish timestamp.
        ISO_TIMESTAMP=$(date -r "$MTIME" '+%Y-%m-%dT%H:%M:%S%z')
    fi
fi

# ── Staleness check ───────────────────────────────────────────────────────────

AGE_SECS=$(( NOW - MTIME ))

if [[ "$MTIME" -eq 0 || "$AGE_SECS" -gt "$THRESHOLD_SECS" ]]; then
    # Format age as hours with one decimal place.
    AGE_HOURS=$(awk "BEGIN { printf \"%.1f\", $AGE_SECS / 3600 }")
    echo "[loop-watch] daily-loop.log not updated since ${ISO_TIMESTAMP} (${AGE_HOURS} hours ago)" >&2
    exit 1
fi

# Fresh — exit silently.
exit 0
