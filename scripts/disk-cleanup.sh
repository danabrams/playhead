#!/bin/bash
# Playhead weekly disk cleanup.
# Removes orphaned .worktrees/<branch>/.derivedData dirs whose worktree
# is no longer registered, plus stale /private/tmp/playhead-* dirs that
# predate this repo's live worktrees.
#
# playhead-3nfa added two more classes, both measured on 2026-08-01:
#   * superseded .xcresult bundles (~100 MB per gate run, nothing prunes them)
#   * stranded CoreSimulator device data in $TMPDIR/Deleting-* (15 GiB found
#     across seven devices)
# This is deliberately the ONLY cleaner in the repo — scripts/disk_preflight.py
# measures and refuses but delegates every removal here, so there is one set of
# safety rails rather than two that drift.
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
# Where CoreSimulator parks a device it claims to have erased. $TMPDIR carries a
# trailing slash; strip it so the prefix rail below compares cleanly. Only ever
# used with the literal `Deleting-*` glob — never bare.
TRASH_ROOT="${TMPDIR:-/nonexistent}"
TRASH_ROOT="${TRASH_ROOT%/}"
case "$TRASH_ROOT" in /var/folders/*) ;; *) TRASH_ROOT="/nonexistent" ;; esac
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

# Working directories of every live xcodebuild. A .derivedData or .xcresult
# under one of these is being written RIGHT NOW and must not be touched — that
# is the difference between reclaiming during a long run and destroying it.
#
# `pgrep -x`, never `pgrep -f`: -f matches the full argv, so it self-matches any
# shell whose command line merely CONTAINS the string "xcodebuild" — including
# this script and the agent that launched it. On 2026-08-01 an `-f` pattern
# killed a healthy gate and then raised a phantom second-build alarm.
LIVE_BUILD_DIRS=""
for pid in $(pgrep -x xcodebuild 2>/dev/null) $(pgrep -x swift-frontend 2>/dev/null); do
  cwd="$(lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p' | head -1)"
  [[ -n "$cwd" ]] && LIVE_BUILD_DIRS="$LIVE_BUILD_DIRS
$cwd"
done
[[ -n "${LIVE_BUILD_DIRS// /}" ]] && log "live build cwd(s): $(printf '%s' "$LIVE_BUILD_DIRS" | tr '\n' ' ')"

# True when $1 is at or under a live build's working directory.
under_live_build() {
  local path="$1" d
  while IFS= read -r d; do
    [[ -z "$d" ]] && continue
    [[ "$path" == "$d" || "$path" == "$d"/* ]] && return 0
  done <<< "$LIVE_BUILD_DIRS"
  return 1
}

remove() {
  local path="$1" reason="$2"
  # Prefix rail (CLAUDE.md): nothing outside these three trees is ever removed,
  # no matter which section asked. Belt and braces — each caller also checks.
  case "$path" in
    "$REPO"/.worktrees/*|/private/tmp/playhead-*|"${TRASH_ROOT:-/nonexistent}"/Deleting-*) ;;
    *) log "REFUSE (outside safe prefixes): $path"; return ;;
  esac
  # Symlink defense: rm -rf follows symlink targets. A developer-created
  # symlink (e.g. .worktrees/foo/.derivedData → /Users/dabrams/critical)
  # would otherwise clobber the link target. Refuse and log instead.
  if [[ -L "$path" ]]; then
    log "SKIP (symlink): $path"
    return
  fi
  if under_live_build "$path"; then
    log "SKIP (live build): $path"
    return
  fi
  local size
  size="$(du -sh "$path" 2>/dev/null | awk '{print $1}')"
  log "REMOVE ($reason, $size): $path"
  if [[ $DRY_RUN -eq 0 ]]; then
    # 0o300 directories (write+exec, NO read) stop `rm -rf` dead the same way
    # they stop CoreSimulator's reaper — DownloadManagerTests creates one on
    # purpose and restores it in a defer that an abnormal exit skips. u+w does
    # not help; the missing bit is READ.
    chmod -R u+rwx "$path" 2>/dev/null || true
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

# 1b. Superseded .xcresult bundles (playhead-3nfa).
# Each gate run leaves ~100 MB in .derivedData/Logs/Test/<name>.xcresult and
# nothing prunes them, so three gates without a bead close is ~300 MB gone.
# The MOST RECENT one per worktree is what gets inspected after a failure, so it
# is always kept; only strictly older bundles go. The build cache lives in
# .derivedData/Build and is untouched.
for wt in "$REPO"/.worktrees/*/; do
  wt="${wt%/}"
  td="$wt/.derivedData/Logs/Test"
  [[ -d "$td" ]] || continue
  if under_live_build "$wt"; then
    log "SKIP (live build): $td"
    continue
  fi
  # nullglob is on, so an empty array means "no bundles". Guard before `ls`:
  # bare `ls -dt` with zero arguments lists the CWD instead of nothing.
  bundles=("$td"/*.xcresult)
  [[ ${#bundles[@]} -eq 0 ]] && continue
  # Newest first; drop the head, remove the tail.
  newest=1
  while IFS= read -r bundle; do
    [[ -z "$bundle" ]] && continue
    if [[ $newest -eq 1 ]]; then
      newest=0
      continue
    fi
    remove "$bundle" "xcresult-superseded"
  done < <(ls -dt "${bundles[@]}" 2>/dev/null)
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

# 3. $TMPDIR/Deleting-* — stranded CoreSimulator device data (playhead-cgka).
# `simctl erase` does not delete a device's data in place: CoreSimulator MOVES
# it here and reaps it asynchronously. If the reap hits a directory it cannot
# READ it dies and the bytes stay forever, while erase still reports success and
# `du` on the device shows the space as freed. Seven devices had stranded 15 GiB
# this way on 2026-08-01 — the largest single reservoir on this box, and
# invisible to every `du` of the worktree, the derivedData or the simulator.
#
# These directories are CoreSimulator's own trash by construction: it put them
# there precisely because it intends them gone. Removing them cannot lose a live
# device. `remove()` chmods u+rwx first, which is the whole trick — the suite
# leaves 0o300 dirs behind (write+exec, no READ) and u+w does not fix them.
if [[ "$TRASH_ROOT" != "/nonexistent" ]]; then
  for trash in "$TRASH_ROOT"/Deleting-*; do
    [[ -d "$trash" ]] || continue
    case "${trash##*/}" in
      Deleting-[0-9A-Fa-f]*) ;;
      *) log "SKIP (unexpected name): $trash"; continue ;;
    esac
    remove "$trash" "coresim-stranded"
  done
fi

# 4. ~/Library/Developer/Xcode/DerivedData intentionally untouched.
log "done (dry_run=$DRY_RUN)"
exit 0
