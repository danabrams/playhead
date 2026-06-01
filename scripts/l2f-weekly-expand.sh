#!/usr/bin/env bash
# l2f-weekly-expand.sh
# Weekly autonomous corpus expansion — adds 4 older episodes from each show
# that has already rotated, growing within-show coverage over time. The
# daily-loop (com.playhead.l2f.daily) then picks them up automatically.
#
# Invoked by com.playhead.l2f.weekly plist (Sunday 02:00 local), runs ~3-5 min
# at LowPriorityIO. Idempotent: snapshot-tool dedupes by episodeId.

set -u
REPO_ROOT="/Users/dabrams/playhead"
LOG_FILE="$REPO_ROOT/TestFixtures/Corpus/Snapshots/weekly-expand.log"

cd "$REPO_ROOT"
mkdir -p "$(dirname "$LOG_FILE")"

ts() { date -u "+%Y-%m-%dT%H:%M:%SZ"; }
echo "[$(ts)] BEGIN weekly-expand --batch siblings" >> "$LOG_FILE"

# Run + tee the summary line (the python script prints "batch=siblings complete: ok=N skip=M fail=K manifest-now=X")
SUMMARY=$(/usr/bin/python3 -u "$REPO_ROOT/scripts/l2f-corpus-expand.py" --batch siblings 2>&1 | grep -E "^batch=.*complete" | tail -1)
RC=${PIPESTATUS[0]}
echo "[$(ts)] END   $SUMMARY  rc=$RC" >> "$LOG_FILE"
exit 0   # never propagate failures — launchd should not retry-loop a broken expansion
