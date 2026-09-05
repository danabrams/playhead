#!/usr/bin/env bash
#
# fast-gate.sh — run the PlayheadFastTests gate reliably, one command.
#
# playhead-qt8y / playhead-ekpn. This wrapper exists because the raw
# `xcodebuild test -testPlan PlayheadFastTests` invocation hits several recurring
# snags on this setup, and because the machine has a hard 16 GB ceiling:
#
#   1. FRESH-WORKTREE BOOTSTRAP: a just-created worktree lacks (a) the gitignored
#      on-device model directory (a large blob kept only in the main checkout and
#      symlinked into each worktree) and (b) an xcodegen-generated scheme wired to
#      the test plans. Without the model, `xcodegen generate` fails spec
#      validation; without the scheme, xcodebuild fails with `Scheme "Playhead"
#      does not have an associated test plan`. This script links the model from
#      the main checkout and regenerates automatically.
#
#   2. COLD-BUILD OOM: a fresh worktree builds the whole project from an empty
#      derivedData — the parallel `swiftc` compile is a memory driver independent
#      of test parallelism, heavy enough to get xcodebuild OOM-killed
#      (`Killed: 9`) on this box. This script caps concurrent compile jobs
#      (`-jobs`, default 4) to keep headroom; incremental rebuilds barely notice.
#
#   3. WEDGED SIMULATOR: a `Mach error -308` / "Failed to install or launch the
#      test runner" (or a sim bootstrap crash) leaves the sim wedged. This script
#      shuts down / erases / boots the destination sim once and retries.
#
# CLONE PARALLELISM IS DELIBERATELY NOT USED (playhead-ekpn). Passing
# `-parallel-testing-worker-count >=2` makes Xcode spawn simulator CLONES, and
# the clone helper resolves `simctl` via the GLOBAL `xcode-select` — which on
# this box is /Library/Developer/CommandLineTools (no `simctl`), so cloning dies
# with `xcrun: error: unable to find utility "simctl"` (exit 65, ~18s, zero tests
# run). `DEVELOPER_DIR` fixes `xcodebuild` itself but NOT the clone helper (the
# 2026-07-16 xcode-select gotcha; enabling clones would need a global
# `xcode-select -s` change = a system-wide, sudo, NEEDS-DAN decision). So this
# runs SINGLE-HOST — the same config as the working default gate: XCTest serial
# + Swift Testing's cheap in-process concurrency (the ~8,300-test bulk stays
# fast in one process).
#
# The dominant OOM cause is running TWO gates/builds at once. Do NOT launch a
# second gate/build alongside this — run gates ONE AT A TIME.
#
# THE VERDICT IS ABOUT THE DIFF, NOT THE COUNT (playhead-voez). This gate is
# RED on a clean checkout — dozens of load-sensitive suites blow their 60s time
# limits under the full run's own concurrency, and CLAUDE.md records a deliberate
# decision not to PerfGate them out (they test real behaviours; moving them is a
# coverage call for Dan). The consequence was that the terminal exit code could
# not distinguish "your change broke something" from "the usual ones", so every
# bead hand-diffed against a baseline nobody had written down — which is how
# playhead-ynmk (#313) merged as "looks like the usual flakes".
#
# So the known-broken set is now COMMITTED, in `scripts/gate-baseline.<plan>.json`,
# and this script's exit code is a statement about the difference:
#
#     RED (N known / 0 new)     -> exit 0
#     RED (N known / 2 NEW)     -> exit 65, both named
#     a baseline test PASSES    -> exit 65, named (Dan: the baseline is EXACT)
#
# Refresh it with `scripts/fast-gate.sh --accept-baseline`, and justify the diff
# in the commit message: the file is the record of what is known-broken, so a
# shrinking diff is good news and a growing one needs a reason.
#
# THE VERDICTS COME FROM THE .xcresult BUNDLE, NOT FROM THIS LOG (playhead-t53a).
# Everything below about the census was, until 2026-08-15, computed by parsing
# xcodebuild's stdout — which the app under test writes into at the same time,
# not line-atomically. A severed `passed after` line reads as a test that said
# NOTHING, so the census counted passes as dead hosts: measured over 27
# crash-free full-plan logs, 80 of 87 reported casualties were verdicts the
# parser could not read, and not one was a crash. So `-resultBundlePath` is
# passed below and handed to gate_baseline.py, which takes every outcome from it
# and leaves the console only what the bundle genuinely lacks (the restart
# banner, the `Failing tests:` block, the terminal marker, a failure's source
# file, and the STARTED roster that keeps a forgotten test a casualty).
#
# A CRASHED TEST HOST PRODUCES NO VERDICT (playhead-tl6l / playhead-buvn). A
# test whose host died emits no per-test line, so it used to be counted as
# neither known nor NEW — the first line above could be printed by a run that
# lost an entire test family, because the crash destroyed the evidence of
# itself. It now reads
#
#     RED (85 known / 0 new) — 11 tests got NO VERDICT (crashed host)
#
# GREEN is unreachable while that count is non-zero, and the census is ARMED the
# same way everything above it is armed — on the DIFF, not the count:
#
#     a name lost its verdict and is NOT recorded    -> named; exit 65 once ARMED
#     a DETERMINISTIC recorded name reported again   -> exit 65, named
#     a load-sensitive recorded name reported again  -> exit 0, good news
#     the count alone, or a bare host restart        -> exit 0
#
# The record lives under `no_verdict` in the same per-plan file and is shaped
# EXACTLY like `tests`: a UNION of names carrying seen/lost observation counts,
# with the tier falling out of the counts. Two runs lost the same eleven tests
# (Jaccard 1.00) and a third lost fifteen — the same eleven plus four that had
# PASSED in both — so after three accepts eleven names stand at 3/3 and are
# DETERMINISTIC while the four stand at 1/1 and are LOAD-SENSITIVE (a name
# accrues observations only from the accept that first records it; 1/1 means
# ONE OBSERVATION, not one of three). It is a set of NAMES rather than a count
# because substitution (eleven die, eleven different ones recover) is what a
# count cannot see.
#
# UNRECORDED IS NOT ZERO, AND PROVISIONAL IS NEITHER. No `no_verdict` key means
# nobody has recorded the population: INERT, and the verdict says so. One or two
# observations is PROVISIONAL — a casualty nobody recorded is NAMED and is not
# fatal, because two observations do not bound a population this load-sensitive.
# Three arms it. That ladder is what keeps this from turning main red today for
# a pre-existing crash owned by playhead-rouw.
#
# The check applies ONLY to a full-plan run. A selective invocation
# (`-only-testing:`/`-skip-testing:`, which is how scripts/mutation-battery.sh
# drives this script) is a different population — the baseline names hundreds of
# tests it never runs — so the check is skipped and the raw exit code passes
# through untouched. See scripts/gate_baseline.py for the whole design.
#
# Env overrides:
#   PLAYHEAD_DEST        xcodebuild -destination (default: iPhone 17 sim by name)
#   PLAYHEAD_DERIVED     -derivedDataPath (default: .derivedData)
#   PLAYHEAD_PLAN        test plan name (default: PlayheadFastTests)
#   PLAYHEAD_BUILD_JOBS  concurrent compile jobs cap (default: 4)
#   PLAYHEAD_SIM_ID      simulator UDID for -308 recovery (else parsed from DEST id=)
#   PLAYHEAD_SIM_TRIM=0       do NOT trim the simulator (playhead-blsh). A control
#                             run also needs `scripts/sim-trim.sh --restore` and a
#                             reboot: the disables OUTLIVE `simctl erase`.
#   PLAYHEAD_SIM_TRIM_TIER_B=1  also disable Siri/Apple Intelligence/speech. That
#                             is a COVERAGE decision — Speech and FoundationModels
#                             are both imported — and it is Dan's, not a default.
#   PLAYHEAD_SKIP_BASELINE=1  bypass the baseline verdict (raw exit code)
#   PLAYHEAD_GATE_BASELINE    baseline file path override
#   PLAYHEAD_RESULT_BUNDLE    where to write the .xcresult (default: a scratch
#                             dir removed on exit). Set it to KEEP the bundle —
#                             it is the only trustworthy record of what each
#                             test did, and the console log is not (playhead-t53a)
#   PLAYHEAD_RESIDUAL_MAX     how many lost verdicts are worth a scoped re-run
#                             before the run is called broken instead (default 120)
#   PLAYHEAD_DISK_MIN_GIB     disk-headroom threshold override (default: see
#                             scripts/disk_preflight.py, where it is derived)
#   PLAYHEAD_SKIP_DISK_PREFLIGHT=1  bypass the headroom refusal entirely
#   DEVELOPER_DIR        toolchain select (e.g. the Xcode 27 beta)
#
# Extra args are forwarded to xcodebuild (e.g. -only-testing:...).
# `--accept-baseline` and `--reclaim-disk` are consumed here, never forwarded.
#
# Exit 28 (POSIX ENOSPC) means the disk preflight REFUSED — no build, no tests.

set -uo pipefail
cd "$(dirname "$0")/.."

DEST="${PLAYHEAD_DEST:-platform=iOS Simulator,name=iPhone 17}"
DERIVED="${PLAYHEAD_DERIVED:-.derivedData}"
# playhead-4wqoi: keep the HOST's Spotlight out of the build cache. corespotlightd
# and mds were 34% of host CPU during a full plan, indexing every object file
# this run writes; the marker is the documented opt-out and needs no sudo. The
# line is printed so a log witnesses it — a silent guard is the standing defect.
mkdir -p "$DERIVED" && touch "$DERIVED/.metadata_never_index" \
  && echo "fast-gate: Spotlight excluded from $DERIVED (.metadata_never_index)"
PLAN="${PLAYHEAD_PLAN:-PlayheadFastTests}"
JOBS="${PLAYHEAD_BUILD_JOBS:-4}"

# playhead-voez: split our own flag off the xcodebuild passthrough, and notice
# whether this is a full-plan run or a selective one. A selective run must leave
# the exit code exactly as xcodebuild set it — mutation-battery.sh reads it.
ACCEPT_BASELINE=0
SELECTIVE=0
RECLAIM_DISK=0
FORWARD=()
for arg in "$@"; do
  case "$arg" in
    --accept-baseline) ACCEPT_BASELINE=1 ;;
    --reclaim-disk) RECLAIM_DISK=1 ;;
    -only-testing*|-skip-testing*) SELECTIVE=1; FORWARD+=("$arg") ;;
    *) FORWARD+=("$arg") ;;
  esac
done
set -- ${FORWARD[@]+"${FORWARD[@]}"}

BASELINE_FILE="${PLAYHEAD_GATE_BASELINE:-scripts/gate-baseline.${PLAN}.json}"

# playhead-81ig: RESOLVE THE UDID FROM A NAME TOO, AND NEVER FAIL AT IT QUIETLY.
# The default DEST is `platform=iOS Simulator,name=iPhone 17` — a NAME, with no
# `id=` in it. Only the `id=` branch existed, so on the DEFAULT invocation SIM_ID
# was empty, and every consumer guarded with `[ -n "$SIM_ID" ]` took its false
# branch in SILENCE. That is how playhead-blsh shipped a simulator trim that
# never ran: a trimmed and an untrimmed log were byte-identical, and two full
# plans were accepted as evidence for a trim that was not applied to either.
SIM_ID="${PLAYHEAD_SIM_ID:-}"
if [ -z "$SIM_ID" ]; then
  case "$DEST" in
    *id=*) SIM_ID="$(printf '%s' "$DEST" | sed -n 's/.*id=\([0-9A-Fa-f-]*\).*/\1/p')" ;;
  esac
fi
if [ -z "$SIM_ID" ]; then
  case "$DEST" in
    *name=*)
      SIM_NAME="$(printf '%s' "$DEST" | sed -n 's/.*name=\([^,]*\).*/\1/p')"
      # The FIRST device with this exact name, preferring a booted one. `simctl
      # list devices` prints `    <name> (<UDID>) (<state>)`, and the name must
      # match to the parenthesis or "iPhone 17" also matches "iPhone 17 Pro".
      SIM_ID="$(xcrun simctl list devices 2>/dev/null \
        | sed -n "s/^ *${SIM_NAME} (\([0-9A-Fa-f-]\{36\}\)) (Booted).*/\1/p" | sed -n 1p)"
      [ -n "$SIM_ID" ] || SIM_ID="$(xcrun simctl list devices 2>/dev/null \
        | sed -n "s/^ *${SIM_NAME} (\([0-9A-Fa-f-]\{36\}\)) (.*/\1/p" | sed -n 1p)"
      ;;
  esac
fi

# Snag 0: DISK HEADROOM (playhead-3nfa). Runs before everything, because a gate
# that runs out of room does not fail — it WEDGES. xcodebuild stays alive with
# zero output and never exits, having failed to write its result bundle; there is
# no POSIX 28, no non-zero exit, nothing in the log. It is indistinguishable from
# a slow test run, which is how four disk-outs on 2026-08-01 were each caught by
# someone happening to look rather than by the tooling.
#
# Refusing here costs seconds. Wedging costs minutes plus the diagnosis, and the
# run has to be redone anyway. See scripts/disk_preflight.py for the threshold
# and its derivation. `--reclaim-disk` lets it run scripts/disk-cleanup.sh ONCE
# and try again; without that flag nothing is ever deleted.
#
# Escape hatch: PLAYHEAD_SKIP_DISK_PREFLIGHT=1. Deliberately not named in the
# refusal text — same reasoning as PLAYHEAD_SKIP_BASELINE. An override printed
# in the failure message stops being an override and becomes the workaround.
if [ "${PLAYHEAD_SKIP_DISK_PREFLIGHT:-0}" != "1" ]; then
  PREFLIGHT_ARGS=(--sim-id "$SIM_ID" --dest "$DEST")
  [ -n "${PLAYHEAD_DISK_MIN_GIB:-}" ] && PREFLIGHT_ARGS+=(--min-gib "$PLAYHEAD_DISK_MIN_GIB")
  [ "$RECLAIM_DISK" -eq 1 ] && PREFLIGHT_ARGS+=(--reclaim)
  if ! python3 scripts/disk_preflight.py "${PREFLIGHT_ARGS[@]}"; then
    exit 28   # POSIX ENOSPC, as the error this would otherwise have hidden
  fi
fi

# Snag 1: bootstrap a fresh worktree — link the gitignored model, then regenerate
# the project + scheme. Only runs when the scheme is missing the plan.
# playhead-ia2s: lint BEFORE building. It costs ~0.2s warm / ~2.4s cold and
# needs no project, so catching a violation here saves the ~3 minutes the test
# run would have spent before telling you the same thing.
#
# Exit 70 from lint.sh means INFRASTRUCTURE (swiftlint or Xcode missing), not
# dirty code — warn and carry on rather than failing a whole test gate over a
# missing dev tool. Any other non-zero is a real violation and stops the gate.
# Escape hatch: PLAYHEAD_SKIP_LINT=1.
if [ "${PLAYHEAD_SKIP_LINT:-0}" != "1" ]; then
  ./scripts/lint.sh
  LINT_RC=$?
  if [ "$LINT_RC" -eq 70 ]; then
    echo "fast-gate: WARNING — lint could not run (exit 70); continuing to tests"
  elif [ "$LINT_RC" -ne 0 ]; then
    echo "fast-gate: lint FAILED (exit $LINT_RC) — stopping before the build."
    echo "fast-gate: fix the violations, or re-run with PLAYHEAD_SKIP_LINT=1 to bypass."
    exit "$LINT_RC"
  fi
fi

SCHEME=Playhead.xcodeproj/xcshareddata/xcschemes/Playhead.xcscheme
if [ ! -f "$SCHEME" ] || ! grep -q "${PLAN}.xctestplan" "$SCHEME" 2>/dev/null; then
  MODEL_REL="Playhead/Resources/Models/qwen3_0_6b_4bit_dynamic_ft_v2"
  if [ ! -e "$MODEL_REL" ]; then
    MAIN_ROOT="$(dirname "$(git rev-parse --git-common-dir 2>/dev/null)")"
    if [ -n "$MAIN_ROOT" ] && [ -d "$MAIN_ROOT/$MODEL_REL" ]; then
      echo "fast-gate: linking gitignored model from $MAIN_ROOT"
      ln -s "$MAIN_ROOT/$MODEL_REL" "$MODEL_REL" 2>/dev/null || true
    fi
  fi
  # Resolve xcodegen robustly — a detached/non-login shell often lacks
  # /opt/homebrew/bin on PATH.
  XCODEGEN="$(command -v xcodegen 2>/dev/null || true)"
  for cand in /opt/homebrew/bin/xcodegen /usr/local/bin/xcodegen; do
    [ -n "$XCODEGEN" ] && break
    [ -x "$cand" ] && XCODEGEN="$cand"
  done
  if [ -z "$XCODEGEN" ]; then
    echo "fast-gate: scheme missing plan '${PLAN}' and xcodegen not found — run 'xcodegen generate' manually"
    exit 70
  fi
  echo "fast-gate: bootstrapping scheme for '${PLAN}' — $XCODEGEN generate"
  "$XCODEGEN" generate || { echo "fast-gate: xcodegen generate FAILED (is the model linked?)"; exit 70; }
fi

# playhead-t53a: WHERE THE VERDICTS COME FROM.
#
# xcodebuild's stdout is shared with the app under test and the interleaving is
# not line-atomic, so a verdict line arrives severed — mid-word, and once inside
# a UTF-8 codepoint. The census that exists to notice a test which reported
# NOTHING read those as dead hosts: main's 06:01 run of 2026-08-15 reported
# seven "crashed host" casualties with zero crash markers in 9.9 MiB, and the
# bundle records all seven as PASSED. Measured over 27 crash-free full-plan
# logs, 80 of 87 reported casualties were verdicts the parser could not read.
#
# So the bundle is now written to a known path and handed to gate_baseline.py,
# which takes every outcome from it and leaves the console only the things the
# bundle genuinely lacks. Override the location with PLAYHEAD_RESULT_BUNDLE to
# keep it after the run — otherwise it is a scratch dir and is removed on exit.
RESULT_BUNDLE="${PLAYHEAD_RESULT_BUNDLE:-}"
BUNDLE_SCRATCH=""
if [ -z "$RESULT_BUNDLE" ]; then
  BUNDLE_SCRATCH="$(mktemp -d -t fast-gate-xcresult)"
  RESULT_BUNDLE="$BUNDLE_SCRATCH/gate.xcresult"
fi
RESIDUAL_BUNDLE="${RESULT_BUNDLE%.xcresult}-residual.xcresult"

run_gate () {
  # xcodebuild REFUSES to write over an existing bundle. Clearing it here rather
  # than once at the top is deliberate: the wedged-sim retry below re-runs the
  # whole plan, and its bundle must REPLACE attempt 1's for the same reason
  # gate_baseline.py's `last_attempt` cuts attempt 1's console output away.
  rm -rf "$RESULT_BUNDLE"
  xcodebuild test \
    -scheme Playhead \
    -testPlan "$PLAN" \
    -destination "$DEST" \
    -derivedDataPath "$DERIVED" \
    -resultBundlePath "$RESULT_BUNDLE" \
    -jobs "$JOBS" \
    "$@"
}

# Snag 2.5: TRIM THE SIMULATOR (playhead-blsh, option B).
#
# A booted iOS 27 simulator, idle and settled, costs +13.35 GiB of demand on a
# 16 GiB box — 301 processes of home screen, wallpaper posters, widgets, News,
# Health, Maps, Spotlight and Siri, none of which an audio app's unit tests
# touch. That is why the merge gate ran 5-7 GiB over the box and why six of them
# in one day produced no verdict at all (playhead-3rql). Trimmed: 112 processes
# and 13.10 GiB, i.e. 7.23 GiB back before xcodebuild compiles anything.
#
# This runs BEFORE the build deliberately: the build then doubles as the
# simulator's settle time, and a job disabled here cannot start during it.
#
# It reports rather than judges. A gate that refuses to run because three
# daemons survived is worse than one that runs with three daemons, but a trim
# that quietly did nothing is exactly the failure this bead exists to end — so
# the outcome is printed either way and the process count goes in the log next
# to the memory series that has to be read with it.
#
# PLAYHEAD_SIM_TRIM=0 turns it off (that is what a CONTROL run wants, plus
# `scripts/sim-trim.sh --restore` and a reboot — the disables OUTLIVE
# `simctl erase`). PLAYHEAD_SIM_TRIM_TIER_B=1 adds the Siri / Apple
# Intelligence / speech tier, which is a COVERAGE decision and is Dan's.
if [ "${PLAYHEAD_SIM_TRIM:-1}" != "0" ] && [ -n "$SIM_ID" ] && [ -x scripts/sim-trim.sh ]; then
  TRIM_ARGS=(--sim-id "$SIM_ID")
  [ "${PLAYHEAD_SIM_TRIM_TIER_B:-0}" = "1" ] && TRIM_ARGS+=(--include-tier-b)
  ./scripts/sim-trim.sh "${TRIM_ARGS[@]}"
  TRIM_RC=$?
  case "$TRIM_RC" in
    0) : ;;
    1) echo "fast-gate: sim-trim REFUSED — a KEEP-list job is in the job file. Fix scripts/sim-trim-jobs.txt." ;;
    2) echo "fast-gate: sim-trim could not reach the simulator — running UNTRIMMED, expect the 16 GiB ceiling." ;;
    *) echo "fast-gate: sim-trim left jobs running (exit $TRIM_RC) — the run is only PARTLY trimmed; read the names above against the memory series." ;;
  esac
  echo "fast-gate: simulator processes after trim: $(ps -Ao args= | /usr/bin/grep -c -e '/CoreSimulato[r]/' -e '\.simruntim[e]/')"
elif [ "${PLAYHEAD_SIM_TRIM:-1}" = "0" ]; then
  echo "fast-gate: sim-trim DISABLED by PLAYHEAD_SIM_TRIM=0 — running UNTRIMMED, expect the 16 GiB ceiling."
elif [ -z "$SIM_ID" ]; then
  echo "fast-gate: sim-trim SKIPPED — could not resolve a simulator UDID from '$DEST'. RUNNING UNTRIMMED."
  echo "fast-gate: set PLAYHEAD_SIM_ID to the device's UDID. A run with no trim line above it is not a trimmed run."
else
  echo "fast-gate: sim-trim SKIPPED — scripts/sim-trim.sh is missing or not executable. RUNNING UNTRIMMED."
fi

echo "fast-gate: plan=$PLAN dest=$DEST derived=$DERIVED jobs=$JOBS (single-host; no clone parallelism)"
LOG="$(mktemp -t fast-gate.XXXXXX)"

# playhead-3rql: sample memory for the WHOLE run, not once at the end.
# Six merge gates died of memory exhaustion and were each triaged by hand as a
# test problem; the controlled experiment that settled it (keep the suspect 38
# tests, remove 38 unrelated ones) died identically. Nothing in the run said so
# because nothing was measuring. The series is cheap (a vm_stat and a ps every
# 10 s) and it is what turns the next occurrence into a reading instead of a
# reconstruction. It is deliberately NOT behind a skip flag: an instrument you
# have to remember to switch on is off.
MEM_SERIES="${PLAYHEAD_MEMORY_SERIES:-}"
if [ -z "$MEM_SERIES" ]; then
  MEM_SERIES="$(mktemp -t fast-gate-mem.XXXXXX).csv"
fi
MEM_ARGS=(--interval 10 --log "$LOG")
[ "${PLAYHEAD_MEMORY_FOOTPRINT:-0}" = "1" ] && MEM_ARGS+=(--footprint)
MEM_PID=""
if command -v python3 >/dev/null 2>&1; then
  python3 scripts/gate-memory-sample.py "$MEM_SERIES" "${MEM_ARGS[@]}" &
  MEM_PID=$!
fi
stop_memory_sampler () {
  [ -n "$MEM_PID" ] || return 0
  kill -TERM "$MEM_PID" 2>/dev/null
  wait "$MEM_PID" 2>/dev/null
  MEM_PID=""
}

run_gate ${1+"$@"} 2>&1 | tee "$LOG"
RC="${PIPESTATUS[0]}"

# Snag 3: wedged-simulator recovery — shut down / erase / boot once, then retry.
# The retry APPENDS to the same log so the baseline check below sees the attempt
# that actually counted. gate_baseline.py cuts everything before the banner, so
# attempt 1's casualties — tests that were mid-flight when the sim died — can
# never union with attempt 2 and manufacture failures out of an artefact.
if grep -qE "Mach error -308|Failed to install or launch the test runner|Early unexpected exit|signal term before establishing connection" "$LOG" && [ -n "$SIM_ID" ]; then
  echo "fast-gate: wedged simulator — recovering sim $SIM_ID and retrying once"
  xcrun simctl shutdown "$SIM_ID" 2>/dev/null || true
  xcrun simctl erase "$SIM_ID" 2>/dev/null || true
  xcrun simctl boot "$SIM_ID" 2>/dev/null || true
  sleep 6
  run_gate "$@" 2>&1 | tee -a "$LOG"
  RC="${PIPESTATUS[0]}"
fi

# ---------------------------------------------------------------------------
# playhead-voez: the baseline verdict.
# ---------------------------------------------------------------------------
stop_memory_sampler

# playhead-3rql: say, in memory terms, whether the run reached a verdict at all.
# Exit codes are untouched — this reports, it does not judge. A run that DID
# reach a verdict gets one line; a run that did not gets the evidence, and the
# series is kept rather than deleted so it can be read afterwards.
MEM_VERDICT_RC=0
if [ -s "$MEM_SERIES" ]; then
  python3 scripts/gate_memory_verdict.py --log "$LOG" --rc "$RC" --series "$MEM_SERIES"
  MEM_VERDICT_RC=$?
fi

finish () {
  rm -f "$LOG"
  if [ "$MEM_VERDICT_RC" -eq 0 ] && [ -z "${PLAYHEAD_MEMORY_SERIES:-}" ]; then
    rm -f "$MEM_SERIES" "$MEM_SERIES.top"
  elif [ -s "$MEM_SERIES" ]; then
    echo "fast-gate: memory series kept at $MEM_SERIES"
  fi
  [ -n "$BUNDLE_SCRATCH" ] && rm -rf "$BUNDLE_SCRATCH"
  exit "$1"
}

# playhead-t53a. The bundle is the verdict source, so a missing one is not a
# detail to shrug at — gate_baseline.py refuses to evaluate rather than fall
# back to the console it has just been proven wrong about. Only pass the flag
# when there is something to pass, so a build that never reached the test phase
# still gets the console-only reading (and says so on the verdict).
BUNDLE_ARGS=()
[ -d "$RESULT_BUNDLE" ] && BUNDLE_ARGS+=(--xcresult "$RESULT_BUNDLE")

# THE RESIDUAL RE-RUN (playhead-t53a). A test whose host died was judged by
# nobody, and the only honest remedy is to run it again — scoped, which for this
# population is seconds. Without this the gate ends holding a hole it has to
# reason about; with it, it ends with a verdict for every test in the plan.
#
# Deliberately ONE pass, never a loop: if the re-run loses verdicts too, that is
# a finding and not something to grind against. And deliberately full-plan only
# — a selective run's residual is somebody else's population.
RESIDUAL_MAX="${PLAYHEAD_RESIDUAL_MAX:-120}"
if [ "$SELECTIVE" -eq 0 ] && [ "${#BUNDLE_ARGS[@]}" -gt 0 ] \
   && [ "${PLAYHEAD_SKIP_BASELINE:-0}" != "1" ]; then
  RESIDUAL=()
  while IFS= read -r line; do
    [ -n "$line" ] && RESIDUAL+=("$line")
  done < <(python3 scripts/gate_baseline.py residual \
             --log "$LOG" --xcresult "$RESULT_BUNDLE" 2>/dev/null)
  if [ "${#RESIDUAL[@]}" -gt 0 ] && [ "${#RESIDUAL[@]}" -le "$RESIDUAL_MAX" ]; then
    echo "fast-gate: ${#RESIDUAL[@]} test(s) got NO VERDICT — re-running exactly those"
    rm -rf "$RESIDUAL_BUNDLE"
    xcodebuild test \
      -scheme Playhead \
      -testPlan "$PLAN" \
      -destination "$DEST" \
      -derivedDataPath "$DERIVED" \
      -resultBundlePath "$RESIDUAL_BUNDLE" \
      -jobs "$JOBS" \
      "${RESIDUAL[@]}" 2>&1 | tail -20
    if [ -d "$RESIDUAL_BUNDLE" ]; then
      BUNDLE_ARGS+=(--xcresult "$RESIDUAL_BUNDLE")
    else
      echo "fast-gate: the residual re-run wrote no bundle — the census below still"
      echo "fast-gate: names those tests, which is the honest reading."
    fi
  elif [ "${#RESIDUAL[@]}" -gt "$RESIDUAL_MAX" ]; then
    # A number, not a silent skip. Hundreds of casualties is a run that lost
    # whole families, and re-running them one by one is the wrong remedy for it.
    echo "fast-gate: ${#RESIDUAL[@]} test(s) got NO VERDICT — too many to re-run scoped"
    echo "fast-gate: (cap $RESIDUAL_MAX). That many lost verdicts is a broken run, not"
    echo "fast-gate: a hole to patch: re-run the whole plan."
  fi
fi

if [ "$ACCEPT_BASELINE" -eq 1 ]; then
  if [ "$SELECTIVE" -eq 1 ]; then
    echo "fast-gate: --accept-baseline REFUSED — a selective run (-only-testing/" >&2
    echo "fast-gate: -skip-testing) sees a fraction of the plan, and accepting it" >&2
    echo "fast-gate: would delete every baseline entry the filter excluded." >&2
    finish 2
  fi
  python3 scripts/gate_baseline.py accept \
    --log "$LOG" --baseline "$BASELINE_FILE" --plan "$PLAN" \
    ${BUNDLE_ARGS[@]+"${BUNDLE_ARGS[@]}"}
  finish $?
fi

if [ "${PLAYHEAD_SKIP_BASELINE:-0}" = "1" ]; then
  echo "fast-gate: baseline check bypassed (PLAYHEAD_SKIP_BASELINE=1) — raw exit $RC"
  finish "$RC"
fi

if [ "$SELECTIVE" -eq 1 ]; then
  echo "fast-gate: baseline check SKIPPED — selective run, a different population"
  echo "fast-gate: from the recorded plan. Raw xcodebuild exit $RC stands."
  finish "$RC"
fi

python3 scripts/gate_baseline.py check \
  --log "$LOG" --baseline "$BASELINE_FILE" --plan "$PLAN" \
  ${BUNDLE_ARGS[@]+"${BUNDLE_ARGS[@]}"}
CHECK_RC=$?
case "$CHECK_RC" in
  0) finish 0 ;;
  1) finish 65 ;;
  # Could not evaluate (no baseline file, wrong plan, or a log with no terminal
  # verdict — a build failure or an OOM kill). The check makes no claim, so
  # xcodebuild's own verdict stands rather than being laundered into a pass.
  *) [ "$RC" -eq 0 ] && finish 0; finish "$RC" ;;
esac
