#!/bin/bash
# Playhead weekly disk cleanup.
# Removes orphaned .worktrees/<branch>/.derivedData dirs whose worktree
# is no longer registered, plus stale /private/tmp/playhead-* dirs that
# predate this repo's live worktrees.
#
# Safe to run manually. Idempotent. Dry-run mode: --dry-run.
#
# Usage:
#   disk-cleanup.sh             # actually remove
#   disk-cleanup.sh --dry-run   # preview only
set -u

DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

REPO="/Users/dabrams/playhead"
LOG_DIR="$REPO/.logs"
LOG="$LOG_DIR/disk-cleanup.log"
mkdir -p "$LOG_DIR"

ts() { date '+%Y-%m-%dT%H:%M:%S%z'; }
log() {
  printf '[%s] %s\n' "$(ts)" "$*" >> "$LOG"
  [[ $DRY_RUN -eq 1 ]] && printf '[DRY] %s\n' "$*"
}

cd "$REPO" 2>/dev/null || { log "ERROR: cannot cd $REPO"; exit 0; }
command -v git >/dev/null 2>&1 || { log "ERROR: git missing"; exit 0; }

REGISTERED="$(git worktree list --porcelain 2>/dev/null | awk '/^worktree /{print $2}')"

is_registered() {
  local target="$1"
  printf '%s\n' "$REGISTERED" | grep -Fxq "$target"
}

remove() {
  local path="$1" reason="$2"
  local size
  size="$(du -sh "$path" 2>/dev/null | awk '{print $1}')"
  log "REMOVE ($reason, $size): $path"
  if [[ $DRY_RUN -eq 0 ]]; then
    rm -rf -- "$path" 2>>"$LOG" || log "  failed: $path"
  fi
}

# 1. .worktrees/<branch>/.derivedData cleanup
shopt -s nullglob 2>/dev/null || true
for wt in "$REPO"/.worktrees/*/; do
  wt="${wt%/}"
  dd="$wt/.derivedData"
  [[ -d "$dd" ]] || continue

  if ! is_registered "$wt"; then
    remove "$dd" "worktree-unregistered"
    continue
  fi

  # Registered worktree — only remove derivedData if stale AND clean.
  if [[ -z "$(find "$dd" -maxdepth 0 -mtime +7 -print 2>/dev/null)" ]]; then
    continue
  fi
  dirty="$(git -C "$wt" status --porcelain 2>/dev/null)"
  ahead="$(git -C "$wt" rev-list --count '@{u}..HEAD' 2>/dev/null || echo 0)"
  if [[ -n "$dirty" || "${ahead:-0}" -gt 0 ]]; then
    log "SKIP (dirty/unpushed): $wt"
    continue
  fi
  remove "$dd" "stale-7d-clean"
done

# 2. /private/tmp/playhead-* cleanup
for tmp in /private/tmp/playhead-*; do
  [[ -d "$tmp" ]] || continue
  if is_registered "$tmp"; then
    log "SKIP (registered worktree): $tmp"
    continue
  fi
  if [[ -n "$(find "$tmp" -maxdepth 0 -mtime +3 -print 2>/dev/null)" ]]; then
    remove "$tmp" "tmp-unregistered-3d"
  fi
done

# 3. ~/Library/Developer/Xcode/DerivedData intentionally untouched.
log "done (dry_run=$DRY_RUN)"
exit 0
