#!/bin/bash
#
# mutation-battery-lock.sh — playhead-pu7e
#
# A worktree-scoped run lock, a crash-recovery path, and an honest diagnosis for
# the "no tests ran" exit. Sourced by `scripts/mutation-battery.sh`; it defines
# functions and touches nothing on its own, so a test harness can source it
# directly (`scripts/tests/test_mutation_battery_lock.py` does).
#
# WHY THIS EXISTS
# ---------------
# The battery applies a source mutation, builds, runs tests, and restores with a
# SCOPED `git checkout -- "${MUTABLE_FILES[@]}"`. Those guards are sound and this
# file does not touch them. What was missing is mutual exclusion: the clean-tree
# guard it relied on has an interleaving window. Between run A's
# `restore_and_verify` and run A's next `apply` the tree is CLEAN, so run B
# passes `require_clean_tree` and starts. From then on each run's restore reverts
# the other's mutant and every verdict on both sides is fiction — with nothing to
# detect it, because each run's hash check passes on its own restore.
#
# Measured cost, 2026-08-04 (playhead-9y9e R1): four verdict attempts destroyed,
# all four reported as `the baseline did not run tests (rc=65)`, which reads as a
# broken anchor or an unbuildable tree. Rails RT11, SC25, SC30 and SC33 still
# carry the implementer's own verdicts because no reviewer could get an
# independent one.
#
# WHY NOT flock(1)
# ----------------
# There is none on macOS — it is a util-linux tool and `command -v flock` is
# empty on this box (BSD ships `shlock`, a uucp-era pid file with no payload, and
# bash has no flock builtin to hold a descriptor with). So the lock is a
# DIRECTORY created with `mkdir`, which is atomic on every POSIX filesystem:
# exactly one of N racing processes gets it. The payload the battery needs — who
# holds it, since when, invoked how, and which files are mutated RIGHT NOW — goes
# in files inside that directory, which flock could not have carried anyway.
#
# The cost of mkdir over flock is that the kernel does not drop it when the
# holder dies, so staleness is our problem rather than the kernel's. That is
# handled explicitly below and is the part that actually matters here.
#
# SURVIVING kill -9
# -----------------
# `kill -9` cannot be trapped, so the holder's EXIT trap does not run: the lock
# directory stays and the mutant stays on disk. Two mechanisms, in this order:
#
#   1. STALENESS. The lock records the holder's pid AND that pid's start time
#      (`ps -o lstart=`). A lock is live only if a process with that pid exists
#      *and* was started at the recorded moment — pid numbers are recycled, and a
#      recycled pid would otherwise wedge this worktree until someone deleted the
#      lock by hand.
#
#   2. RECOVERY. The lock also records, for every mutable file, the hash it had
#      when the tree was proved pristine (`pre`) and — once a batch is applied —
#      the hash of the injected mutant (`post`). Reclaiming a stale lock restores
#      only what it can PROVE the dead holder wrote:
#
#        cur == pre                  nothing to do
#        cur == post                 the mutant verbatim -> restore, verify
#        post unknown, cur != pre    died mid-apply -> restore, verify
#        cur == neither, post known  REFUSE — something edited it afterwards
#
#      Every restore first copies the current bytes to a rescue directory under
#      /private/tmp/playhead-mutation-battery-rescue.* and names it on stderr, so
#      an auto-restore is never a loss even where we misjudged. That path matches
#      the `/private/tmp/playhead-*` pattern `scripts/disk-cleanup.sh` already
#      sweeps at 3 days, so rescues are reaped by the existing cron.
#
# `require_clean_tree` alone cannot do this. It correctly REFUSES, but a refusal
# leaves the mutant on disk for the next person to find, and it cannot tell a
# mutant from your work. The lock can, because it wrote down what it broke before
# it broke it.
#
# EXIT CODES the caller should use
#   75  EX_TEMPFAIL — another battery holds this worktree. Retry later.
#   2   the lock could not be established, or recovery refused.

# How long to wait for a fresh lock directory to grow its `info` file. The holder
# writes it on the line after `mkdir`, so this is microseconds in practice; the
# wait exists so that losing the race does not read as "nobody owns this".
MB_LOCK_INFO_WAIT_SECONDS="${MB_LOCK_INFO_WAIT_SECONDS:-5}"
# A lock directory with STILL no `info` after this long was created by a process
# that died in the microsecond between `mkdir` and the write. Nothing was
# recorded, so nothing was mutated under it (the first mutation happens many
# seconds later, after `require_clean_tree` and `mb_lock_record_pre`), and it is
# safe to reclaim without touching a single source file.
MB_LOCK_ABANDON_SECONDS="${MB_LOCK_ABANDON_SECONDS:-300}"

# ---------------------------------------------------------------------------
# State. All module-private except MB_LOCK_OWNED, which the caller's EXIT trap
# reads to decide whether it may release.
# ---------------------------------------------------------------------------
MB_LOCK_DIR=""
MB_LOCK_OWNED=0
MB_LOCK_RESCUE_DIR=""
MB_LOCK_FILES_PRE=""

mb__say() { printf 'mutation-battery: %s\n' "$*" >&2; }

mb__trim() { sed 's/^[[:space:]]*//;s/[[:space:]]*$//'; }

mb__hash() { shasum -a 256 "$1" 2>/dev/null | awk '{print $1}'; }

# Read `key=value` out of a lock file. The value may contain spaces and '='; only
# the FIRST '=' separates, and only the FIRST matching line is read. A missing
# key or missing file yields the empty string, which every caller treats as
# "unknown" rather than as a value.
#
# awk rather than `sed | head -1`: under `set -o pipefail` (which the battery
# sets) `head` closing the pipe early makes the whole pipeline return 141, so the
# helper's exit status would report a failure that did not happen.
mb__field() {
  local file="$1" key="$2"
  [ -f "$file" ] || return 0
  awk -v k="$key=" 'index($0, k) == 1 { print substr($0, length(k) + 1); exit }' "$file"
}

# Does a process with this pid exist, and is it the SAME process we recorded?
#
# `ps` rather than `kill -0`: `kill -0` on a process owned by another user fails
# with EPERM, which is indistinguishable from "no such process" and would declare
# a live holder stale. `ps -o lstart=` answers the existence question for any
# owner, and its output doubles as the recycled-pid discriminator.
mb__holder_alive() {
  local pid="$1" want="${2:-}" now
  case "$pid" in '' | *[!0-9]*) return 1 ;; esac
  now="$(ps -o lstart= -p "$pid" 2>/dev/null | mb__trim)"
  [ -n "$now" ] || return 1
  [ -n "$want" ] || return 0
  [ "$now" = "$want" ]
}

mb__pid_start() { ps -o lstart= -p "$1" 2>/dev/null | mb__trim; }

# Seconds since a path was last modified. BSD stat first (this is a macOS box),
# GNU stat as the fallback, and 0 — i.e. "brand new, do not reclaim" — when
# neither answers, because guessing OLD here would steal a live lock.
mb__age_seconds() {
  local mtime
  mtime="$(stat -f %m "$1" 2>/dev/null || stat -c %Y "$1" 2>/dev/null)"
  case "$mtime" in '' | *[!0-9]*) printf '0'; return 0 ;; esac
  printf '%s' "$(( $(date +%s) - mtime ))"
}

# ---------------------------------------------------------------------------
# Where the lock lives
# ---------------------------------------------------------------------------
# The worktree's own git directory: `.git` for the main checkout,
# `.git/worktrees/<slug>` for a linked one. That path is unique per worktree —
# which is exactly the scope of the corruption, since each worktree has its own
# working tree and its own `.derivedData` — and it is OUTSIDE the working tree,
# so the lock can never show up in `git status`, never trip the clean-tree guard,
# and never block `git worktree remove`.
mb_lock_path() {
  local gd
  gd="$(git rev-parse --absolute-git-dir 2>/dev/null)"
  [ -n "$gd" ] || return 1
  printf '%s/mutation-battery.lock' "$gd"
}

# ---------------------------------------------------------------------------
# Rescue copies
# ---------------------------------------------------------------------------
mb__rescue_dir() {
  if [ -z "$MB_LOCK_RESCUE_DIR" ]; then
    MB_LOCK_RESCUE_DIR="$(mktemp -d /private/tmp/playhead-mutation-battery-rescue.XXXXXX 2>/dev/null)"
  fi
  [ -n "$MB_LOCK_RESCUE_DIR" ] && [ -d "$MB_LOCK_RESCUE_DIR" ] || return 1
  printf '%s' "$MB_LOCK_RESCUE_DIR"
}

# Copy a file we are about to overwrite somewhere the operator can get it back.
# Failure here is FATAL to the restore: an auto-restore whose safety net is
# missing is just a delete.
mb__rescue() {
  local f="$1" dir
  dir="$(mb__rescue_dir)" || return 1
  mkdir -p "$dir/$(dirname "$f")" 2>/dev/null || return 1
  cp "$f" "$dir/$f" 2>/dev/null || return 1
  printf '%s/%s' "$dir" "$f"
}

# ---------------------------------------------------------------------------
# Acquire
# ---------------------------------------------------------------------------
# $1 — the invocation's own arguments, for the refusal message. Knowing the
#      holder ran `--only RT14` is what tells you whether waiting is worth it.
#
# Returns 0 holding the lock, 75 on live contention, 2 on anything else.
mb_lock_acquire() {
  local argv="${1:-}" dir info holder_pid holder_start holder_argv holder_when
  local age waited now

  dir="$(mb_lock_path)" || {
    mb__say "cannot resolve this worktree's git directory, so the run lock has"
    mb__say "nowhere to live. Refusing rather than running unlocked — an unlocked"
    mb__say "battery is how two runs silently destroy each other's verdicts."
    return 2
  }
  MB_LOCK_DIR="$dir"
  info="$dir/info"

  if mkdir "$dir" 2>/dev/null; then
    mb__write_info "$argv"
    MB_LOCK_OWNED=1
    return 0
  fi

  # Someone got there first — or died there. Give the winner a moment to write
  # its `info`: reading an absent owner as "gone" would hand this worktree to a
  # second battery at the precise moment the first is starting on it.
  waited=0
  while [ ! -s "$info" ] && [ -d "$dir" ] && [ "$waited" -lt "$MB_LOCK_INFO_WAIT_SECONDS" ]; do
    sleep 1
    waited=$((waited + 1))
  done

  if [ ! -d "$dir" ]; then
    # Released while we waited.
    if mkdir "$dir" 2>/dev/null; then
      mb__write_info "$argv"
      MB_LOCK_OWNED=1
      return 0
    fi
    mb__say "REFUSING — lost the race for this worktree's battery lock at $dir."
    mb__say "Another run took it while this one was waiting. Re-run when it ends."
    MB_LOCK_DIR=""
    return 75
  fi

  if [ ! -s "$info" ]; then
    age="$(mb__age_seconds "$dir")"
    if [ "$age" -ge "$MB_LOCK_ABANDON_SECONDS" ]; then
      mb__say "reclaiming an ABANDONED lock at $dir — created ${age}s ago and it"
      mb__say "  never recorded an owner, so the process died between mkdir and"
      mb__say "  its first write. Nothing was recorded, therefore nothing was"
      mb__say "  mutated under it: no source file is touched by this reclaim."
      rm -rf "$dir" 2>/dev/null
      if mkdir "$dir" 2>/dev/null; then
        mb__write_info "$argv"
        MB_LOCK_OWNED=1
        return 0
      fi
    fi
    mb__say "REFUSING — another mutation battery is already running in this worktree."
    mb__say "  lock       : $dir"
    mb__say "  holder pid : not recorded yet (created ${age}s ago)"
    mb__say ""
    mb__say "The lock exists but its owner has not written its identity yet, which"
    mb__say "is what a battery that started moments ago looks like. This is NOT a"
    mb__say "broken baseline and NOT anchor drift. If you are certain no battery is"
    mb__say "running, remove that directory by hand."
    MB_LOCK_DIR=""
    return 75
  fi

  holder_pid="$(mb__field "$info" pid)"
  holder_start="$(mb__field "$info" pid_start)"
  holder_argv="$(mb__field "$info" argv)"
  holder_when="$(mb__field "$info" started_human)"

  if mb__holder_alive "$holder_pid" "$holder_start"; then
    now="$(date +%s)"
    age="$(mb__field "$info" started)"
    case "$age" in '' | *[!0-9]*) age="" ;; *) age="$((now - age))s ago" ;; esac
    mb__say "REFUSING — another mutation battery is already running in this worktree."
    mb__say "  holder pid : $holder_pid"
    mb__say "  started    : ${holder_when:-unknown}${age:+ ($age)}"
    mb__say "  invoked as : ${holder_argv:-<no arguments>}"
    mb__say "  lock       : $dir"
    mb__say ""
    mb__say "This is NOT a broken baseline and NOT anchor drift. Two batteries in"
    mb__say "one worktree revert each other's mutants and both sets of verdicts"
    mb__say "become fiction, so this run stops instead of queueing. Wait for that"
    mb__say "pid to exit, or kill it and re-run — the next run will restore"
    mb__say "whatever mutant it leaves behind."
    MB_LOCK_DIR=""
    return 75
  fi

  # Stale. Say so, put the tree back, then take it.
  mb__say "reclaiming a STALE lock — pid ${holder_pid:-<unrecorded>} is gone"
  mb__say "  (started ${holder_when:-unknown}, invoked as ${holder_argv:-<no arguments>})"
  mb__say "  A holder that vanished without releasing was killed with SIGKILL, lost"
  mb__say "  its terminal, or the box went down. Its EXIT trap did not run, so the"
  mb__say "  tree may still carry its mutation."
  if ! mb_lock_recover "$dir"; then
    mb__say "refusing to take the lock while the previous run's mutation is"
    mb__say "unresolved. Nothing has been changed. Fix the tree, then re-run."
    MB_LOCK_DIR=""
    return 2
  fi
  rm -rf "$dir" 2>/dev/null

  if mkdir "$dir" 2>/dev/null; then
    mb__write_info "$argv"
    MB_LOCK_OWNED=1
    return 0
  fi

  # Two processes reclaimed the same stale lock and the other won the mkdir. The
  # window is real but bounded, and it resolves to a REFUSAL rather than to two
  # concurrent batteries, which is the outcome that matters.
  mb__say "REFUSING — lost the race to reclaim a stale lock at $dir;"
  mb__say "another run took it first. Re-run once that one finishes."
  MB_LOCK_DIR=""
  return 75
}

mb__write_info() {
  local argv="${1:-}"
  {
    printf 'pid=%s\n' "$$"
    printf 'pid_start=%s\n' "$(mb__pid_start "$$")"
    printf 'started=%s\n' "$(date +%s)"
    printf 'started_human=%s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')"
    printf 'worktree=%s\n' "$(pwd)"
    printf 'host=%s\n' "$(hostname -s 2>/dev/null)"
    # Newlines would break the one-key-per-line format and turn a value into a
    # forged key on read-back.
    printf 'argv=%s\n' "$(printf '%s' "$argv" | tr '\n\r' '  ')"
  } >"$MB_LOCK_DIR/info"
  printf 'state=clean\n' >"$MB_LOCK_DIR/state"
}

# ---------------------------------------------------------------------------
# Recording what is at risk
# ---------------------------------------------------------------------------
# Call ONCE, immediately after `require_clean_tree` has proved every mutable file
# is pristine. From here until release this process is the only legitimate writer
# of these files, which is what makes recovery decidable.
mb_lock_record_pre() {
  local f out=""
  [ "$MB_LOCK_OWNED" -eq 1 ] || return 0
  for f in "$@"; do
    out="${out}file	$(mb__hash "$f")	?	$f
"
  done
  MB_LOCK_FILES_PRE="$out"
  mb__write_state clean "" ""
}

# state file: `state=`, `batch=`, `names=`, then one `file<TAB>pre<TAB>post<TAB>path`
# per mutable file. Written to a temp and moved, so a reader can never see half
# of it — including a reader recovering from our own death.
mb__write_state() {
  local st="$1" batch="${2:-}" names="${3:-}" tmp
  [ -n "$MB_LOCK_DIR" ] && [ -d "$MB_LOCK_DIR" ] || return 0
  tmp="$MB_LOCK_DIR/state.tmp.$$"
  {
    printf 'state=%s\n' "$st"
    printf 'batch=%s\n' "$batch"
    printf 'names=%s\n' "$(printf '%s' "$names" | tr '\n\r' '  ')"
    printf '%s' "${MB_LOCK_FILES_PRE:-}"
  } >"$tmp" 2>/dev/null && mv -f "$tmp" "$MB_LOCK_DIR/state" 2>/dev/null
}

# About to apply a batch. `post` is deliberately left `?`: if we are killed
# between here and `mb_lock_note_applied` the tree holds a HALF-applied batch and
# no hash can describe it, so recovery must be TOLD that rather than left to
# infer it from a mismatch it would refuse.
mb_lock_note_mutating() {
  [ "$MB_LOCK_OWNED" -eq 1 ] || return 0
  mb__write_state mutated "${1:-}" "${2:-}"
}

# The batch applied cleanly — record the mutant's exact bytes. This is what lets
# a later recovery say "this is our mutation verbatim" instead of "this file
# differs from something".
mb_lock_note_applied() {
  local tag pre post path out=""
  [ "$MB_LOCK_OWNED" -eq 1 ] || return 0
  while IFS='	' read -r tag pre post path; do
    [ "${tag:-}" = "file" ] || continue
    out="${out}file	${pre}	$(mb__hash "$path")	${path}
"
  done <<<"${MB_LOCK_FILES_PRE:-}"
  MB_LOCK_FILES_PRE="$out"
  mb__write_state mutated \
    "$(mb__field "$MB_LOCK_DIR/state" batch)" "$(mb__field "$MB_LOCK_DIR/state" names)"
}

# The tree is back to pristine and verified byte-exact. Drop the `post` hashes: a
# stale `post` would let a later recovery match a file it no longer describes.
mb_lock_note_restored() {
  local tag pre post path out=""
  [ "$MB_LOCK_OWNED" -eq 1 ] || return 0
  while IFS='	' read -r tag pre post path; do
    [ "${tag:-}" = "file" ] || continue
    out="${out}file	${pre}	?	${path}
"
  done <<<"${MB_LOCK_FILES_PRE:-}"
  MB_LOCK_FILES_PRE="$out"
  mb__write_state clean "" ""
}

# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------
# Restore whatever a dead holder left mutated, and ONLY that. Returns 0 when the
# tree is provably pristine afterwards, 1 when anything was left unresolved — in
# which case nothing was written.
mb_lock_recover() {
  local dir="$1" state="$1/state" info="$1/info"
  local st recorded_wt cur pre post path saved tag touched=0 refused=0

  recorded_wt="$(mb__field "$info" worktree)"
  if [ -n "$recorded_wt" ] && [ "$recorded_wt" != "$(pwd)" ]; then
    mb__say "the stale lock was taken in $recorded_wt but this run is in $(pwd)."
    mb__say "Refusing to run 'git checkout --' against a tree the lock does not"
    mb__say "describe. Remove $dir by hand once you have checked that tree."
    return 1
  fi

  if [ ! -f "$state" ]; then
    mb__say "the stale lock recorded no file state — it died before it owned the"
    mb__say "tree, so there is nothing of its to restore."
    return 0
  fi
  st="$(mb__field "$state" state)"

  while IFS='	' read -r tag pre post path; do
    [ "${tag:-}" = "file" ] || continue
    [ -n "${path:-}" ] || continue
    if [ ! -f "$path" ]; then
      mb__say "REFUSING — $path is recorded in the stale lock but does not exist."
      mb__say "  A missing mutable file is not something this can guess at."
      refused=1
      continue
    fi
    cur="$(mb__hash "$path")"
    if [ "$cur" = "$pre" ]; then
      continue
    fi
    # `post=?` means "no mutant hash recorded", and that is TWO different
    # situations. It is a half-applied batch only when the run had announced it
    # was mutating; if the state still says `clean`, the dead run never reached
    # an `apply` at all, so a file that differs from pristine is SOMEBODY'S
    # EDIT. Restoring on the hash alone would discard it — the same "a value
    # that names one thing read as though it named another" this whole bead is
    # about, committed by its own repair path.
    if [ "$post" = "?" ] && [ "$st" != "mutated" ]; then
      mb__say "REFUSING — $path differs from the pristine bytes the dead run"
      mb__say "  recorded, but that run never applied a mutation (its state was"
      mb__say "  '${st:-unknown}', not 'mutated'). This difference is somebody's"
      mb__say "  edit and discarding it would destroy work."
      mb__say "    pristine : $pre"
      mb__say "    on disk  : $cur"
      mb__say "  Inspect with: git diff -- $path"
      refused=1
      continue
    fi
    if [ "$post" != "?" ] && [ "$cur" != "$post" ]; then
      mb__say "REFUSING — $path matches neither the pristine bytes the dead run"
      mb__say "  recorded nor the mutant it injected. Something edited it after"
      mb__say "  that run died, and discarding it would destroy that work."
      mb__say "    pristine : $pre"
      mb__say "    mutant   : $post"
      mb__say "    on disk  : $cur"
      mb__say "  Inspect with: git diff -- $path"
      refused=1
      continue
    fi

    saved="$(mb__rescue "$path")" || {
      mb__say "REFUSING — could not write a rescue copy of $path under"
      mb__say "  /private/tmp, and an auto-restore without one is just a delete."
      refused=1
      continue
    }
    if [ "$post" = "?" ]; then
      mb__say "$path differs from pristine and the dead run was MID-APPLY, so the"
      mb__say "  tree holds a partial mutation. Restoring it."
    else
      mb__say "$path carries the dead run's mutation verbatim (batch $(mb__field "$state" batch), $(mb__field "$state" names)). Restoring it."
    fi
    mb__say "  rescue copy: $saved"
    if ! git checkout -- "$path" 2>/dev/null; then
      mb__say "REFUSING — 'git checkout -- $path' FAILED. The mutation is still on"
      mb__say "  disk. An index.lock from another git process is the usual cause."
      refused=1
      continue
    fi
    cur="$(mb__hash "$path")"
    if [ "$cur" != "$pre" ]; then
      mb__say "REFUSING — restored $path but the bytes are NOT the ones the dead"
      mb__say "  run recorded as pristine ($cur, wanted $pre). The file moved in"
      mb__say "  git as well, so this needs a human."
      refused=1
      continue
    fi
    touched=$((touched + 1))
  done <"$state"

  [ "$refused" -eq 0 ] || return 1
  if [ "$touched" -gt 0 ]; then
    mb__say "recovered $touched file(s) left mutated by the dead run. State '$st'."
  fi
  return 0
}

# ---------------------------------------------------------------------------
# Release
# ---------------------------------------------------------------------------
# Only ever removes a lock this process owns. A run that REFUSED must not delete
# the live holder's lock on its way out — that would hand the worktree to a third
# run while the holder is still mutating it.
mb_lock_release() {
  local dir="$MB_LOCK_DIR" owner
  [ "$MB_LOCK_OWNED" -eq 1 ] || return 0
  [ -n "$dir" ] && [ -d "$dir" ] || return 0
  owner="$(mb__field "$dir/info" pid)"
  if [ "$owner" != "$$" ]; then
    mb__say "not releasing $dir — it is now held by pid $owner, not $$."
    return 0
  fi
  rm -rf "$dir" 2>/dev/null
  MB_LOCK_OWNED=0
}

# ---------------------------------------------------------------------------
# Concurrent xcodebuild — advisory, not a refusal
# ---------------------------------------------------------------------------
# The lock is worktree-scoped, which is the scope of the SOURCE corruption. It
# says nothing about a gate running in a DIFFERENT worktree, and that one still
# collides over the shared simulator and CoreSimulator service — the
# `database is locked` / `Mach error -308` family. This reports it and does not
# refuse, because a legitimate second worktree (or Xcode itself) may be building
# and only the operator knows whether that is intended.
#
# Matching is on the full argv via `ps`, never `pgrep -f`: `-f` self-matches any
# shell whose command line merely contains the string, which on 2026-08-01 killed
# a healthy gate and then raised a phantom second-build alarm. The pattern is
# `scripts/fast-gate.sh`'s own `run_gate` invocation verbatim — `xcodebuild test
# -scheme Playhead …` — so it matches what this repo actually spawns rather than
# a generic guess.
mb_other_xcodebuilds() {
  ps -Ao pid=,command= 2>/dev/null \
    | grep '[x]codebuild test -scheme Playhead' \
    | awk -v me="$$" '$1 != me'
}

mb_warn_if_xcodebuild_running() {
  local found
  found="$(mb_other_xcodebuilds)"
  [ -n "$found" ] || return 0
  mb__say "WARNING — an xcodebuild test run is ALREADY live on this box:"
  printf '%s\n' "$found" | sed 's/^/mutation-battery:   /' >&2
  mb__say "This worktree's lock does not cover another worktree, but the simulator"
  mb__say "and CoreSimulator are shared: expect 'database is locked' or a wedged"
  mb__say "runner. CLAUDE.md's ONE AT A TIME is not only about memory."
}

# ---------------------------------------------------------------------------
# The rc=65 diagnosis
# ---------------------------------------------------------------------------
# `run_focused` returning non-zero with NO tests in the log used to print
# "the baseline did not run tests (rc=65)" and then grep `error:|BUILD FAILED|
# Killed: 9`. That message is a claim ABOUT THE TREE, and three real causes
# produce it of which only one implicates the tree at all:
#
#   * CONTENTION — a second battery or gate sharing this `.derivedData`
#     (`database is locked`). Measured in playhead-9y9e R1: four verdicts lost.
#   * A WEDGED RUNNER — the build was fine and the test host never came up
#     (`blessSimulatorHub failed`, `Simulator service hub IS NOT still alive`).
#     Measured in playhead-9y9e R3 with ZERO competing batteries and a clean
#     tree; recovery was `simctl shutdown all` + boot and the next run was green.
#   * THE BUILD FAILED — the mutated tree does not compile. Not a kill: the
#     mutation was never evaluated.
#
# And a fourth outcome that is NOT this function's business, spelled out because
# quoting "rc=65" as a failure is what made the others hard to see: when the
# suites RAN, rc=65 merely means some test failed, which for a mutation battery
# is the expected outcome and is decided by name-matching, not by rc.
#
# $1 log  $2 rc  $3 label ("the baseline", "batch 7")
mb_diagnose_no_tests() {
  local log="$1" rc="$2" what="$3" hit="" others

  mb__say "$what ran NO TESTS (rc=$rc)."
  mb__say "The suites never reported, so rc is not a verdict about any mutation:"
  mb__say "a SURVIVOR requires the suites to have run and the expected test to"
  mb__say "have passed. Neither happened here."

  # Before the empty-log check: a disk refusal exits 28 having deliberately
  # produced no log at all, and calling that "empty, undiagnosed" would throw
  # away the one thing we do know.
  if [ "$rc" = "28" ]; then
    mb__say "DIAGNOSIS — DISK. fast-gate's preflight REFUSED before building"
    mb__say "  (exit 28 = POSIX ENOSPC). Nothing was run and nothing was deleted."
    mb__say "  Reclaim with scripts/disk-cleanup.sh, or check \$TMPDIR/Deleting-*"
    mb__say "  — stranded CoreSimulator trash is the biggest reservoir on this box."
    return 0
  fi

  if [ ! -s "$log" ]; then
    mb__say "DIAGNOSIS — UNDIAGNOSED: the log is empty or missing ($log)."
    return 0
  fi

  # Order matters. Contention and a dead runner are checked BEFORE the build,
  # because both of them also spray `error:` lines that look like compile
  # failures, and misreading those as a broken tree is the bug being fixed. The
  # BUILD verdict therefore requires a terminal build marker, never a bare
  # `error:`.
  if hit="$(grep -m1 -E "database is locked|unable to attach DB|index\.lock|Another instance of|could not lock" "$log")"; then
    mb__say "DIAGNOSIS — CONTENTION. Another build is using this worktree's"
    mb__say "  .derivedData, its git index, or the same simulator. THE BASELINE IS"
    mb__say "  NOT BROKEN and the anchor has not drifted; this run collided."
    mb__say "  evidence: $(printf '%s' "$hit" | mb__trim)"
    others="$(mb_other_xcodebuilds)"
    if [ -n "$others" ]; then
      mb__say "  still live now:"
      printf '%s\n' "$others" | sed 's/^/mutation-battery:     /' >&2
    fi
    mb__say "  Wait for the other run, then re-run this one. Its verdicts are"
    mb__say "  worth nothing and must not be recorded."
    return 0
  fi

  # The wedged-runner family. `blessSimulatorHub` / `service hub IS NOT still
  # alive` / `signal kill before establishing connection` are playhead-9y9e R3's
  # measured signatures; the rest are fast-gate's own retry triggers, present
  # here because reaching this function means the retry did not clear it.
  if hit="$(grep -m1 -E "blessSimulatorHub|service hub IS NOT still alive|signal (kill|term) before establishing connection|Mach error -308|Failed to install or launch the test runner|Early unexpected exit|Unable to boot device|Failed to create IXPlaceholder|Simulator device failed to launch" "$log")"; then
    mb__say "DIAGNOSIS — RUNNER NEVER LAUNCHED. The tree is not implicated: the"
    mb__say "  build finished and the test host failed to come up. This is the"
    mb__say "  wedged-simulator shape, and fast-gate's one retry did not clear it."
    mb__say "  evidence: $(printf '%s' "$hit" | mb__trim)"
    if grep -q 'unable to find utility "simctl"' "$log"; then
      mb__say "  NOTE — this log also carries 'xcrun: error: unable to find utility"
      mb__say "  \"simctl\"'. That is a SECONDARY symptom: xcodebuild's diagnostic"
      mb__say "  collection shells out through the GLOBAL xcode-select"
      mb__say "  (CommandLineTools, which has no simctl). It is NOT the"
      mb__say "  clone-parallelism gotcha of 2026-07-16 and chasing it wastes the"
      mb__say "  round — the wedged runner above is the cause."
    fi
    mb__say "  Recover the simulator before re-running (DEVELOPER_DIR-qualified,"
    mb__say "  or xcrun resolves against the global xcode-select and fails):"
    mb__say "    export DEVELOPER_DIR=/Applications/Xcode-beta.app/Contents/Developer"
    mb__say "    xcrun simctl shutdown all && xcrun simctl boot 'iPhone 17'"
    return 0
  fi

  if hit="$(grep -m1 -E "No space left on device|POSIX error 28|ENOSPC" "$log")"; then
    mb__say "DIAGNOSIS — DISK EXHAUSTION mid-run. The volume filled while building."
    mb__say "  evidence: $(printf '%s' "$hit" | mb__trim)"
    mb__say "  Reclaim with scripts/disk-cleanup.sh and check \$TMPDIR/Deleting-*,"
    mb__say "  then re-run at a PLAYHEAD_DISK_MIN_GIB floor high enough to refuse"
    mb__say "  rather than wedge."
    return 0
  fi

  if hit="$(grep -m1 -E "Killed: 9|BUILD INTERRUPTED|received signal 144" "$log")"; then
    mb__say "DIAGNOSIS — OOM. xcodebuild was killed by the system, not by a test."
    mb__say "  evidence: $(printf '%s' "$hit" | mb__trim)"
    mb__say "  This box has 16 GB and two concurrent builds exhaust it. Run one"
    mb__say "  build at a time; lower PLAYHEAD_BUILD_JOBS if it recurs alone."
    return 0
  fi

  if hit="$(grep -m1 -E "BUILD FAILED|The following build commands failed|Command SwiftCompile failed|Command SwiftEmitModule failed" "$log")"; then
    mb__say "DIAGNOSIS — THE BUILD FAILED. The mutated tree does not compile, so"
    mb__say "  the mutation was never evaluated. This is an ERROR, not a kill and"
    mb__say "  not a survivor. Usually the EDIT is stale against moved source, or"
    mb__say "  it is legal Swift that \`scripts/lint.sh --strict\` rejects."
    mb__say "  evidence: $(printf '%s' "$hit" | mb__trim)"
    grep -m 10 -E "error: " "$log" | sed 's/^/mutation-battery:     /' >&2
    return 0
  fi

  if hit="$(grep -m1 -E "lint FAILED|SwiftLint|swiftlint" "$log")"; then
    mb__say "DIAGNOSIS — LINT REFUSED THE TREE before the build started."
    mb__say "  evidence: $(printf '%s' "$hit" | mb__trim)"
    mb__say "  A mutation that cannot pass the linter cannot be evaluated; re-cut"
    mb__say "  the EDIT. (Second time this has happened — see RT10 in the header.)"
    return 0
  fi

  # The honest terminal case. Naming a cause we have not established is how four
  # verdicts got sent to the wrong explanation.
  mb__say "DIAGNOSIS — UNDIAGNOSED. No known signature matched: not contention,"
  mb__say "  not a wedged runner, not disk, not OOM, not a build failure. The"
  mb__say "  cause is NOT established — in particular this does not show that the"
  mb__say "  tree, the baseline or the anchor is broken. Read the log before"
  mb__say "  concluding:"
  mb__say "    $log"
  mb__say "  last lines:"
  tail -20 "$log" | sed 's/^/mutation-battery:     /' >&2
}
