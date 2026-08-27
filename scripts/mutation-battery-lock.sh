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
#
# THAT INFERENCE IS ONLY SOUND BECAUSE `mb__take` ENFORCES IT (review round,
# playhead-pu7e). It reads "no owner recorded" as "no owner ever existed", which
# is the absence-for-absence substitution this bead exists to remove — and it was
# false as shipped: `mb__write_info`'s redirection was UNCHECKED, so a holder
# whose identity write failed (ENOSPC on a box that hit 100 % capacity four times
# on 2026-08-01, EDQUOT, EACCES) went on to mutate the tree while its lock sat
# ownerless, and five minutes later — inside a 4-9 minute run — the next battery
# reclaimed it as ABANDONED. Two batteries, one worktree, which is the whole
# defect. `mb__take` now refuses and REMOVES the directory when it cannot record
# an owner, so an ownerless lock really can only come from a death between the
# `mkdir` and the rename.
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

# Is the live process with this pid itself a mutation battery?
#
# Corroboration, added in the review round, and it may only ever push toward
# REFUSING. `mb__holder_alive` reads a start-time mismatch as "the recorded
# process is gone and its pid was recycled", which is right for a well-formed
# record — but a MALFORMED one produces the same mismatch while the holder is
# very much alive. Planted with a half-written `pid_start` and it reclaimed a
# live holder's lock: exit 0, MB_LOCK_OWNED=1, two batteries in one worktree.
# `mb__write_info` now renames the record into place so this code can no longer
# author a partial one, and this is the second wall: before believing a recorded
# pid is dead, ask what that pid is actually running.
mb__pid_is_battery() {
  local pid="$1" cmd
  case "$pid" in '' | *[!0-9]*) return 1 ;; esac
  cmd="$(ps -o command= -p "$pid" 2>/dev/null)"
  case "$cmd" in *mutation-battery*) return 0 ;; *) return 1 ;; esac
}

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
# Taking it: mkdir + record who we are, as ONE step that either happens or does
# not (review round, playhead-pu7e)
# ---------------------------------------------------------------------------
# Returns 0 holding the lock with our identity on disk, 1 when the directory
# already exists (i.e. genuine contention — the caller decides live vs stale),
# and 2 when nothing is held and nothing can be.
#
# The two things it exists to keep apart, both of which used to read as
# contention:
#
#   * `mkdir` FAILING IS NOT EVIDENCE THAT SOMEBODY HOLDS THIS. EEXIST is the
#     contended case; ENOSPC, EACCES, EROFS and a missing parent all land on the
#     same non-zero exit. As shipped, every one of them printed "Another run took
#     it while this one was waiting" and returned 75 — EX_TEMPFAIL, i.e. "retry
#     later" — sending the operator to wait for a process that does not exist.
#     The directory's own existence is the discriminator and it is free.
#   * TAKING THE DIRECTORY IS NOT THE SAME AS OWNING IT. If the identity cannot
#     be written there is no owner for anyone to see, and the lock is worse than
#     no lock: this run believes it is exclusive while the next one reads an
#     ownerless directory. So the directory is REMOVED rather than left behind —
#     we created it atomically, so no other process can be inside it, and its
#     removal hands the worktree to the next run honestly instead of silently.
mb__take() {
  local dir="$1" argv="${2:-}" err
  if ! err="$(mkdir "$dir" 2>&1)"; then
    [ -d "$dir" ] && return 1
    if [ -e "$dir" ]; then
      mb__say "REFUSING — the run lock path exists but is NOT a directory, so it"
      mb__say "  was not left by a battery and no battery can take it."
      mb__say "    $dir"
      mb__say "  Inspect it and remove it by hand."
      return 2
    fi
    mb__say "REFUSING — could not create the run lock and NOBODY holds it: the"
    mb__say "  directory does not exist, so this is NOT contention and waiting"
    mb__say "  will not help."
    mb__say "    $dir"
    mb__say "    ${err:-mkdir failed without a message}"
    mb__say "  A full volume, a read-only or missing git directory will each do"
    mb__say "  this. Refusing to run unlocked — an unlocked battery is how two"
    mb__say "  runs silently destroy each other's verdicts."
    return 2
  fi
  if ! mb__write_info "$argv"; then
    rm -rf "$dir" 2>/dev/null
    mb__say "REFUSING — took the run lock but could NOT record who holds it, so"
    mb__say "  no other run could tell this worktree was busy. The directory has"
    mb__say "  been removed rather than left ownerless: an ownerless lock is"
    mb__say "  reclaimed as ABANDONED after ${MB_LOCK_ABANDON_SECONDS}s, which is"
    mb__say "  inside a normal run, and that would put two batteries here."
    mb__say "    $dir"
    mb__say "  A full volume is the usual cause: check df and \$TMPDIR/Deleting-*."
    return 2
  fi
  MB_LOCK_OWNED=1
  return 0
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
  local age waited now take

  dir="$(mb_lock_path)" || {
    mb__say "cannot resolve this worktree's git directory, so the run lock has"
    mb__say "nowhere to live. Refusing rather than running unlocked — an unlocked"
    mb__say "battery is how two runs silently destroy each other's verdicts."
    return 2
  }
  MB_LOCK_DIR="$dir"
  info="$dir/info"

  mb__take "$dir" "$argv"
  take=$?
  [ "$take" -eq 0 ] && return 0
  if [ "$take" -eq 2 ]; then
    MB_LOCK_DIR=""
    return 2
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
    mb__take "$dir" "$argv"
    take=$?
    [ "$take" -eq 0 ] && return 0
    if [ "$take" -eq 2 ]; then
      MB_LOCK_DIR=""
      return 2
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
      mb__take "$dir" "$argv"
      take=$?
      [ "$take" -eq 0 ] && return 0
      if [ "$take" -eq 2 ]; then
        MB_LOCK_DIR=""
        return 2
      fi
      # take == 1: another run claimed it between our rm and our mkdir. Fall
      # through to the refusal below rather than looping — a second battery is
      # the outcome this refuses, and it is now genuinely there.
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

  # Before believing the recorded pid is gone: is it running a battery right now?
  # Only reachable when the pid is alive and the recorded start time disagrees,
  # i.e. either a recycled pid (reclaim is right) or a record we misread (reclaim
  # would steal a live lock). Asking what the process IS separates them, and the
  # answer can only ever make us refuse.
  if mb__pid_is_battery "$holder_pid"; then
    mb__say "REFUSING — the lock's recorded start time does not match pid"
    mb__say "  $holder_pid, which would normally mean that pid was recycled — but"
    mb__say "  pid $holder_pid IS RUNNING A MUTATION BATTERY right now. A record"
    mb__say "  this run cannot read is not evidence that its holder is dead, and"
    mb__say "  reclaiming on it would put two batteries in this worktree."
    mb__say "  lock       : $dir"
    mb__say "  recorded   : ${holder_start:-<unrecorded>}"
    mb__say "  actual     : $(mb__pid_start "$holder_pid")"
    mb__say "  If that pid belongs to a battery in a DIFFERENT worktree, this"
    mb__say "  lock is genuinely stale — remove $dir by hand."
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
    # Deliberately does NOT claim "nothing has been changed" — `mb_lock_recover`
    # is the only thing that knows whether it refused before acting or a restore
    # failed partway, and it says which.
    mb__say "refusing to take the lock while the previous run's state is"
    mb__say "unresolved. Fix the tree, then re-run."
    MB_LOCK_DIR=""
    return 2
  fi
  rm -rf "$dir" 2>/dev/null

  mb__take "$dir" "$argv"
  take=$?
  [ "$take" -eq 0 ] && return 0
  if [ "$take" -eq 2 ]; then
    MB_LOCK_DIR=""
    return 2
  fi

  # Two processes reclaimed the same stale lock and the other won the mkdir. The
  # window is real but bounded, and it resolves to a REFUSAL rather than to two
  # concurrent batteries, which is the outcome that matters.
  mb__say "REFUSING — lost the race to reclaim a stale lock at $dir;"
  mb__say "another run took it first. Re-run once that one finishes."
  MB_LOCK_DIR=""
  return 75
}

# Write the holder's identity. TEMP-AND-RENAME, and CHECKED — both added in the
# review round, and both for the same reason `mb__write_state` was already doing
# it: a reader must see the whole record or none of it.
#
#   * Half an `info` is worse than no `info`. A reader that catches
#     `pid=<live>` plus a truncated `pid_start=` compares the fragment against
#     the real start time, finds a mismatch, and declares a LIVE holder stale —
#     then reclaims its lock. Planted and reproduced: exit 0, MB_LOCK_OWNED=1,
#     two batteries. `rename(2)` is atomic, so no reader can observe a partial
#     record and the wait loop's `-s "$info"` test becomes exact.
#   * The result was DISCARDED. `mb_lock_acquire` set MB_LOCK_OWNED=1 whatever
#     happened here, so a run whose identity write failed mutated the tree while
#     its lock sat ownerless — and the ABANDONED rule handed the worktree to the
#     next battery 300 s later. Reproduced with an unwritable lock directory;
#     ENOSPC is the production shape of it.
#
# Returns non-zero, having left no partial `info` behind, if it could not record
# the identity in full.
mb__write_info() {
  local argv="${1:-}" tmp="$MB_LOCK_DIR/info.tmp.$$"
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
  } >"$tmp" 2>/dev/null || { rm -f "$tmp" 2>/dev/null; return 1; }
  # `-s` as well as the write status: a full volume can report a successful
  # close and leave nothing, and an empty `info` is exactly what the ABANDONED
  # rule reads as "nobody was ever here".
  [ -s "$tmp" ] || { rm -f "$tmp" 2>/dev/null; return 1; }
  mv -f "$tmp" "$MB_LOCK_DIR/info" 2>/dev/null || { rm -f "$tmp" 2>/dev/null; return 1; }
  printf 'state=clean\n' >"$MB_LOCK_DIR/state" 2>/dev/null || return 1
  return 0
}

# ---------------------------------------------------------------------------
# Recording what is at risk
# ---------------------------------------------------------------------------
# Call ONCE, immediately after `require_clean_tree` has proved every mutable file
# is pristine. From here until release this process is the only legitimate writer
# of these files, which is what makes recovery decidable.
#
# Returns non-zero if it could not write the record. The caller must STOP: this
# is the last moment before the first mutation, so refusing costs nothing, and
# running on would leave a mutant no later run can identify as ours — which is
# the case `mb_lock_recover` correctly refuses to act on, i.e. a wedged worktree
# needing a human. Same unchecked-write class as `mb__write_info` above.
mb_lock_record_pre() {
  local f out=""
  [ "$MB_LOCK_OWNED" -eq 1 ] || return 0
  for f in "$@"; do
    out="${out}file	$(mb__hash "$f")	?	$f
"
  done
  MB_LOCK_FILES_PRE="$out"
  if ! mb__write_state clean "" ""; then
    mb__say "REFUSING — could not record the pristine hashes of the mutable files"
    mb__say "  in $MB_LOCK_DIR. Without them a crash leaves a mutant that no later"
    mb__say "  run can tell from your work, so it would be refused by hand instead"
    mb__say "  of restored. Nothing has been mutated. Check df."
    return 1
  fi
  return 0
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
# Restore whatever a dead holder left mutated, and ONLY that.
#
# TWO PASSES, and the split is load-bearing. Pass 1 CLASSIFIES every recorded
# file and writes nothing; pass 2 acts only if pass 1 raised no objection. A
# single loop that restored as it went would, on hitting an objection halfway
# down, have already restored the files above it — and then print "Nothing has
# been changed", which is the exact shape of lie this whole bead is about. (The
# one case that can still leave a partial tree is a `git checkout` FAILING in
# pass 2. That is an action that failed rather than a decision that changed its
# mind, and it says so.)
#
# Returns 0 when the tree is provably pristine afterwards, 1 otherwise.
mb_lock_recover() {
  local dir="$1" state="$1/state" info="$1/info"
  local st recorded_wt cur pre post path saved tag plan="" touched=0 refused=0
  local batch names

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
  batch="$(mb__field "$state" batch)"
  names="$(mb__field "$state" names)"

  # ---- pass 1: classify, write nothing --------------------------------------
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

    # A batch mutates one or two files, but the lock records `post` for EVERY
    # mutable file, so an untouched one has post == pre. The dead run provably
    # did not write that file, which makes a difference there somebody's edit —
    # and calling it "neither the pristine bytes nor the mutant it injected"
    # names a mutant that was never there. Same for `post=?` while the state is
    # still `clean`: the run had not reached an `apply` at all.
    if [ "$post" = "$pre" ] || { [ "$post" = "?" ] && [ "$st" != "mutated" ]; }; then
      mb__say "REFUSING — $path differs from the pristine bytes the dead run"
      mb__say "  recorded, but that run DID NOT WRITE this file (state"
      mb__say "  '${st:-unknown}', no mutant recorded for it). The difference is"
      mb__say "  somebody's edit and discarding it would destroy work."
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
    plan="${plan}${pre}	${post}	${path}
"
  done <"$state"

  if [ "$refused" -ne 0 ]; then
    mb__say "nothing was restored — a recovery is all-or-nothing, so the files it"
    mb__say "  could have put back are still exactly as you found them."
    return 1
  fi
  [ -n "$plan" ] || return 0

  # ---- pass 2: act ----------------------------------------------------------
  while IFS='	' read -r pre post path; do
    [ -n "${path:-}" ] || continue
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
      mb__say "$path carries the dead run's mutation verbatim (batch $batch, $names). Restoring it."
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
  done <<<"$plan"

  if [ "$refused" -ne 0 ]; then
    mb__say "a restore FAILED partway. $touched file(s) were put back before it"
    mb__say "  did; the tree is NOT in the state it was when this started. Check"
    mb__say "  'git status --porcelain' before doing anything else."
    return 1
  fi
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
#
# The owner is read with a pure-bash loop rather than `$(mb__field …)`, and that
# is not style. This function runs from an EXIT trap, and one way to reach that
# trap is SIGPIPE — at which point bash is holding an unflushed stdout buffer
# from the write that failed. A command substitution forks a subshell that
# INHERITS that buffer and flushes it into the substitution's pipe, so `$(…)`
# came back as "74929\n=== batch 1: 1 mutation(s) ===" and the release refused
# its own lock. Measured 2026-08-06. No fork, no contamination.
mb_lock_release() {
  local dir="$MB_LOCK_DIR" owner="" line
  [ "$MB_LOCK_OWNED" -eq 1 ] || return 0
  [ -n "$dir" ] && [ -d "$dir" ] || return 0
  if [ -f "$dir/info" ]; then
    while IFS= read -r line || [ -n "$line" ]; do
      case "$line" in pid=*) owner="${line#pid=}"; break ;; esac
    done <"$dir/info"
  fi
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
#
# TRUNCATED to 140 columns, deliberately. `FOCUSED_SUITES` puts ~150
# `-only-testing:` flags on that command line, so the untruncated form is about
# 10 KB per process — printed in full it buries the sentence that says what to
# do, which is the whole purpose of the warning.
mb_other_xcodebuilds() {
  ps -Ao pid=,command= 2>/dev/null \
    | grep '[x]codebuild test -scheme Playhead' \
    | awk -v me="$$" '$1 != me { print substr($0, 1, 140) (length($0) > 140 ? " …" : "") }'
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
# WHAT THIS RUN IS HOLDING, AND IT IS SAID OUT OF ONE FUNCTION SO THE TWO DISK
# ARMS CANNOT DRIFT (playhead-gjlp0 R4, generalised at R5).
#
# Since playhead-gjlp0 the battery keeps an `.xcresult` bundle per non-KILL
# batch inside `$WORK`, so a long series accumulates hundreds of megabytes
# DURING the run — the one way a disk refusal can be the run's own doing. Both
# remedies the disk arms print are useless for it: `disk-cleanup.sh` sweeps
# `/private/tmp/playhead-*` at THREE DAYS and this directory is minutes old, so
# a reader who follows the advice reclaims nothing and concludes the box is
# short; `$TMPDIR/Deleting-*` is somebody else's reservoir entirely.
#
# R4 wrote this into the `rc=28` arm alone and left the `DISK EXHAUSTION
# mid-run` arm eight lines below it printing exactly the two useless remedies —
# the same finding, in the sibling arm, unfixed. One function now.
#
# Guarded on WORK because `mb_diagnose_no_tests` is also called from the lock
# rails, where there is none: printing "holding 0 bundles" for a directory
# nobody created would be a measurement of an absence read as a measurement of
# a thing, which is the defect the bead this clause belongs to exists to remove.
mb__say_work_holdings() {
  [ -n "${WORK:-}" ] && [ -d "${WORK:-}" ] || return 0
  mb__say "  THIS RUN is holding $(du -sh "$WORK" 2>/dev/null | awk '{print $1}') in"
  mb__say "  $WORK ($(find "$WORK" -maxdepth 1 -name '*.xcresult' 2>/dev/null | wc -l | tr -d ' ') .xcresult bundle(s) kept as evidence for the"
  mb__say "  verdicts above). It is minutes old, so disk-cleanup.sh's 3-day sweep"
  mb__say "  will NOT touch it: read the verdicts, then remove it by hand."
}

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
    # AND NAME THE ONE THIS RUN IS SITTING ON — see `mb__say_work_holdings`.
    mb__say_work_holdings
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
    # Review round: list them HERE too, not only under CONTENTION. The commonest
    # way this box gets a wedged runner is playhead-zsqh — a SIGKILLed battery
    # leaves its `xcodebuild` child alive (reproduced here: pid 50621 outlived
    # parent 44904) and that orphan still holds the simulator. `simctl shutdown
    # all` loses to a live orphan, which re-boots the device underneath it, so
    # naming the process to kill has to come BEFORE the recovery command or the
    # recovery reads as "did not work".
    others="$(mb_other_xcodebuilds)"
    if [ -n "$others" ]; then
      mb__say "  an xcodebuild is STILL LIVE on this box and is the first suspect:"
      printf '%s\n' "$others" | sed 's/^/mutation-battery:     /' >&2
      mb__say "  If no run owns it, it is an ORPHAN of a killed battery"
      mb__say "  (playhead-zsqh). KILL IT FIRST — the shutdown below loses to a"
      mb__say "  live orphan, which boots the device again underneath it."
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
    # AND THE ONE THIS RUN IS SITTING ON — the same clause as the `rc=28` arm,
    # and MORE this run's own doing than that one: the preflight passed at this
    # batch's start and what filled the volume afterwards includes the bundles
    # this run has been writing. Raising PLAYHEAD_DISK_MIN_GIB does not act on
    # them either; it is a start-of-run check.
    mb__say_work_holdings
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
