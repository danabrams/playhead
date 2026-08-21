#!/bin/bash
# scripts/sim-trim.sh — boot the gate's simulator WITHOUT the iOS home-screen ecosystem.
#
# playhead-blsh, option B. The arithmetic this exists for (playhead-3rql, and
# re-measured here on 2026-08-20):
#
#     box RAM                                         16.00 GiB
#     macOS, simulator SHUT DOWN                       6.98 GiB demand
#     a booted iOS 27 simulator, idle, settled 5 min  20.33 GiB demand, 301 processes
#
# `demand` is active + wired + compressor + swap. It is NOT `Pages free`, which on
# this box reads ~1 GiB at rest and is short of nothing; a previous diagnosis was
# built on `Pages free` and was wrong. An idle simulator costs +13.35 GiB here, so
# the box is over its RAM before xcodebuild has compiled anything.
#
# WHY NOT `simctl boot --disabledJob=<label>`, WHICH IS THE DOCUMENTED KNOB:
# because it is INERT on this runtime, and that is measured rather than assumed.
# Booted with six --disabledJob flags (chronod, newsd, healthd, donotdisturbd,
# amsengagementd, SpringBoard) on a freshly erased device, settled five minutes:
# all six were RUNNING, 293 processes against a vanilla 301, demand 20.17 against
# 20.33 GiB. `launchctl print-disabled` inside the device lists none of them.
#
# Read the settling curve before you re-test it, because it is a trap: at ONE
# minute after boot three of those six read "absent" and it looks like a partial
# win. They were merely not started yet. The earlier report of "process count fell
# only 230 -> 201" is almost certainly the same artefact. MEASURE AT FIVE MINUTES.
#
# WHAT DOES WORK is launchctl's own disable, inside the device:
#
#     xcrun simctl spawn <udid> launchctl disable  user/<uid>/<label>
#     xcrun simctl spawn <udid> launchctl bootout  user/<uid>/<label>
#
# `disable` writes the launchd override so the job cannot be demand-launched
# afterwards; `bootout` removes the one already running. Both are supported
# launchctl operations, neither pokes the device's files behind launchd's back.
#
# THE OVERRIDES DO SURVIVE `simctl erase`. An earlier draft of this comment said
# the opposite and it was wrong — see the --restore section below for where they
# actually live and why that is a trap for a CONTROL run. fast-gate.sh re-applies
# the trim on every run anyway, which is idempotent and is what makes the run
# report what it actually got rather than what the last one left behind.
#
# THE VERDICT IS THE PROCESS COUNT, NOT THE EXIT CODE. Every operation here can
# report success and change nothing (that is precisely what --disabledJob does),
# so the script ends by re-reading `launchctl list` and naming every label it was
# told to remove that is STILL RUNNING. A label that matches NOTHING on this
# runtime is also reported: a licence for a job nobody can find has been renamed,
# and whatever inherits the name inherits the amnesty.
#
# Usage:
#   scripts/sim-trim.sh --sim-id <udid> [--include-tier-b] [--settle N] [--report-only]
#
# Exit 0 trimmed and verified · 1 a KEEP-list label was in the job file · 2 could
# not reach the device · 3 trimmed but some labels survived (named).

set -uo pipefail
cd "$(dirname "$0")/.."

SIM_ID="${PLAYHEAD_SIM_ID:-}"
INCLUDE_TIER_B="${PLAYHEAD_SIM_TRIM_TIER_B:-0}"
SETTLE=0
REPORT_ONLY=0
RESTORE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --sim-id) SIM_ID="$2"; shift 2 ;;
    --include-tier-b) INCLUDE_TIER_B=1; shift ;;
    --settle) SETTLE="$2"; shift 2 ;;
    --report-only) REPORT_ONLY=1; shift ;;
    --restore) RESTORE=1; shift ;;
    *) echo "sim-trim: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

if [ -z "$SIM_ID" ]; then
  echo "sim-trim: no simulator UDID (pass --sim-id or set PLAYHEAD_SIM_ID)" >&2
  exit 2
fi

# THE KEEP LIST IS THE ENFORCED HALF OF THE JOB FILE'S SAFETY ARGUMENT.
# Every entry backs a framework Playhead or its tests actually import, or is the
# plumbing that installs and launches the test host. A job file that names one of
# these fails the run rather than being silently skipped — the failure mode this
# guards against is somebody widening the trim by one line and turning a whole
# test family into a silent no-op.
#
#   SpringBoard/backboardd/runningboardd  xcodebuild launches the test host through
#                                         FrontBoard; without them nothing runs at all,
#                                         and SCENE PHASE is a real behaviour here —
#                                         this repo has a documented sceneless-launch
#                                         defect class (see MEMORY, 2026-08-10).
#   dasd                                  BGTaskScheduler. BackgroundTasks: 4 app files,
#                                         14 test files.
#   nsurlsessiond                         background URLSession (BackgroundURLSessionTests).
#   mediaremoted, medialibraryd           MediaPlayer: now-playing + remote commands.
#   usernotificationsd, bulletindistributord   UserNotifications: 4 app, 5 test files.
#   storekitd                             StoreKit is imported.
#   mlhostd                               CoreML.
#   mobileassetd                          Speech and FoundationModels asset lookup.
#   installd/containermanagerd/lsd/pkd    installing and resolving the test host.
#   tccd/securityd/trustd/keybagd         permissions and keychain.
#   cfprefsd/distnoted/notifyd/syslogd/configd_sim   base IPC. Removing these is
#                                         indistinguishable from breaking the device.
#   diagnosticextensionsd                 crash reports. A gate that loses a host and
#                                         cannot say why is the thing blsh exists to end.
#   CoreSimulator.bridge, dtdeviceinfod   Xcode's own channel into the device.
#
#   com.apple.nanoregistryd — THE ONE THAT WAS FOUND BY BISECTION, NOT BY
#   READING. It is the paired-device (Watch) registry, it backs no framework
#   this app imports, and by every rule in sim-trim-jobs.txt it belongs in the
#   trim. Disabling it makes the NEXT BOOT UNUSABLE: `simctl bootstatus` prints
#   nothing at all and hangs, and `simctl launch com.apple.Preferences` never
#   returns — so xcodebuild could not install or start a test host either.
#   Measured both directions: disabling it ALONE hangs, and disabling the other
#   117 with it enabled boots to 97 processes and launches in ~10 s.
#
#   It matters because the disables OUTLIVE `simctl erase`: the damage is not to
#   the run that applies the trim, it is to the NEXT one — which is exactly the
#   kind of defect that gets blamed on somebody's diff.
KEEP='
com.apple.SpringBoard
com.apple.backboardd
com.apple.runningboardd
com.apple.dasd
com.apple.nsurlsessiond
com.apple.mediaremoted
com.apple.medialibraryd
com.apple.usernotificationsd
com.apple.bulletindistributord
com.apple.storekitd
com.apple.mlhostd
com.apple.mobileassetd
com.apple.mobile.installd
com.apple.MobileInstallationHelperService
com.apple.containermanagerd
com.apple.lsd
com.apple.pluginkit.pkd
com.apple.iconservices.iconservicesagent
com.apple.tccd
com.apple.securityd
com.apple.trustd
com.apple.cfprefsd.xpc.daemon
com.apple.distnoted.xpc.daemon
com.apple.notifyd
com.apple.syslogd
com.apple.configd_sim
com.apple.fontservicesd
com.apple.revisiond
com.apple.FileCoordination
com.apple.FileProvider
com.apple.diagnosticextensionsd
com.apple.CoreSimulator.bridge
com.apple.coredevice.dtdeviceinfod
com.apple.purplebuddy.budd
com.apple.locationd
com.apple.nanoregistryd
com.apple.contactsd
com.apple.accountsd
com.apple.akd
com.apple.appleaccountd
com.apple.cloudd
com.apple.apsd
com.apple.dataaccess.dataaccessd
com.apple.cdpd
com.apple.syncdefaultsd
com.apple.TextInput.kbd
com.apple.InputUI
'

sim_state () {
  xcrun simctl list devices 2>/dev/null | /usr/bin/grep "$SIM_ID" \
    | sed -E 's/.*\((Booted|Shutdown|Booting|Shutting Down)\).*/\1/'
}
sim_proc_count () {
  # The bracket is not decoration. `ps` and `grep` run concurrently in a
  # pipeline, so a pattern written literally appears in grep's OWN argv and the
  # count includes the counter — the same self-match that `pgrep -f` is banned
  # here for. `[r]` cannot match the argv that contains `[r]`.
  ps -Ao args= | /usr/bin/grep -c -e '/CoreSimulato[r]/' -e '\.simruntim[e]/'
}
lc () { xcrun simctl spawn "$SIM_ID" launchctl "$@" 2>&1; }

# --- reach the device ---------------------------------------------------------
if [ "$(sim_state)" != "Booted" ]; then
  echo "sim-trim: booting $SIM_ID"
  xcrun simctl boot "$SIM_ID" >/dev/null 2>&1
  xcrun simctl bootstatus "$SIM_ID" >/dev/null 2>&1
fi
[ "$(sim_state)" = "Booted" ] || { echo "sim-trim: device $SIM_ID is not Booted" >&2; exit 2; }

# The user domain launchd_sim puts these jobs in. Derived, not assumed: the label
# set is identical under system/ and user/<uid>, but only one of them is the
# domain `launchctl list` reports, and guessing it wrong makes every disable a
# no-op that still exits 0.
#
# RETRIED, because `Booted` is not `ready`: for the first few tens of seconds
# after boot `simctl spawn` can fail outright, and a single attempt turns that
# into "could not resolve a launchd domain" — which, on a --restore, leaves a
# CONTROL run silently trimmed while the harness carries on. Measured: a restore
# invoked immediately after `simctl boot` returned failed this way.
DOMAIN=""
for attempt in $(seq 1 30); do
  for uid in "$(id -u)" 501; do
    if lc print-disabled "user/$uid" | /usr/bin/grep -q "disabled services"; then DOMAIN="user/$uid"; break 2; fi
  done
  sleep 4
done
[ -n "$DOMAIN" ] || { echo "sim-trim: could not resolve a launchd domain on $SIM_ID after 120 s" >&2; exit 2; }

# --- the job list -------------------------------------------------------------
FILES=(scripts/sim-trim-jobs.txt)
[ "$INCLUDE_TIER_B" = "1" ] && FILES+=(scripts/sim-trim-jobs-tier-b.txt)
LABELS="$(cat "${FILES[@]}" | sed 's/#.*//' | tr -d ' \t' | /usr/bin/grep -v '^$' | sort -u)"
COUNT="$(printf '%s\n' "$LABELS" | wc -l | tr -d ' ')"

# --- --restore: put the device back the way an untrimmed gate finds it --------
# THE TRIM OUTLIVES `simctl erase`, which is the opposite of what the erase in
# the gate's own preamble leads you to expect. launchd's overrides for this
# device live in /private/var/tmp/com.apple.CoreSimulator.SimDevice.<udid>/
# disabled.plist — OUTSIDE the device's data directory — so erasing the device
# and rebooting it leaves every disable in place. Measured: a boot after an
# erase reported 15 jobs already recorded disabled from an earlier session.
#
# That is convenient for the gate and a trap for anyone measuring a CONTROL:
# a run intended to be untrimmed is trimmed unless somebody undoes it. This is
# the undo, and it goes through `launchctl enable` rather than deleting the
# plist, so launchd is the thing that changes its own mind.
if [ "$RESTORE" = "1" ]; then
  ALL="$(cat scripts/sim-trim-jobs.txt scripts/sim-trim-jobs-tier-b.txt | sed 's/#.*//' | tr -d ' \t' | /usr/bin/grep -v '^$' | sort -u)"
  BEFORE_R="$(sim_proc_count)"
  echo "sim-trim: RESTORING $(printf '%s\n' "$ALL" | /usr/bin/grep -c .) job(s) on $SIM_ID (procs_before=$BEFORE_R)"
  export SIM_ID DOMAIN
  printf '%s\n' "$ALL" | xargs -P 6 -I{} sh -c \
    'xcrun simctl spawn "$SIM_ID" launchctl enable "$DOMAIN/{}" >/dev/null 2>&1' >/dev/null 2>&1
  STILL="$(lc print-disabled "$DOMAIN" | sed -n 's/.*"\(.*\)" => disabled.*/\1/p' | sort -u)"
  LEFT="$(comm -12 <(printf '%s\n' "$ALL") <(printf '%s\n' "$STILL"))"
  if [ -n "$LEFT" ]; then
    echo "sim-trim: STILL DISABLED after restore — the control would be contaminated:"
    printf '  %s\n' $LEFT
    exit 3
  fi
  echo "sim-trim: restore OK — no listed job is recorded disabled."
  echo "sim-trim: REBOOT the device for the restored jobs to actually start."
  exit 0
fi

# A KEEP-list label in the job file is a hard stop, not a skip.
CLASH="$(comm -12 <(printf '%s\n' "$LABELS") <(printf '%s\n' "$KEEP" | tr -d ' \t' | /usr/bin/grep -v '^$' | sort -u))"
if [ -n "$CLASH" ]; then
  echo "sim-trim: REFUSING — these are on the KEEP list and must not be disabled:" >&2
  printf '  %s\n' $CLASH >&2
  exit 1
fi

BEFORE="$(sim_proc_count)"
# The population BEFORE anything is removed. `bootout` deletes a job from the
# domain, so a label read back afterwards is absent whether it was booted out or
# never existed — asking the question after the trim cannot tell those apart, and
# the first version of this script reported eight healthy removals as "names this
# runtime does not know". Ask first.
KNOWN_BEFORE="$(lc list | awk -F'\t' 'NR>1 {print $3}' | sort -u)"
echo "sim-trim: $SIM_ID domain=$DOMAIN jobs=$COUNT tier_b=$INCLUDE_TIER_B  procs_before=$BEFORE"

# A label is "unknown" only if launchd has neither a loaded job nor an override
# for it. Without the second clause a re-run reports all 118 as unknown — because
# `bootout` deletes the job from the domain, so everything the trim already
# removed reads as missing. That is the same shape as the census defect this repo
# keeps finding: a name that is ABSENT for a known reason read as evidence.
PRE_DISABLED="$(lc print-disabled "$DOMAIN" | sed -n 's/.*"\(.*\)" => disabled.*/\1/p' | sort -u)"
UNKNOWN="$(comm -23 <(comm -23 <(printf '%s\n' "$LABELS") <(printf '%s\n' "$KNOWN_BEFORE")) <(printf '%s\n' "$PRE_DISABLED"))"

if [ "$REPORT_ONLY" = "1" ]; then
  echo "sim-trim: --report-only, nothing disabled"
  [ -n "$UNKNOWN" ] && { echo "sim-trim: names this runtime does not know:"; printf '  %s\n' $UNKNOWN; }
  exit 0
fi

# --- disable, then remove what is already running -----------------------------
# Parallel because each spawn is a ~0.4 s round trip and there are >100 of them.
# RETRIED because a parallel spawn can fail and still leave the pipeline's exit
# status at 0: the first run of this recorded 7 of 118 disables and reported
# success. The loop re-reads the override database and re-attempts whatever is
# missing, so the number of passes is decided by the device rather than by hope.
export SIM_ID DOMAIN
disabled_set () { lc print-disabled "$DOMAIN" | sed -n 's/.*"\(.*\)" => disabled.*/\1/p' | sort -u; }
# Every label is disabled, including ones `launchctl list` does not currently
# show. `launchctl disable` records an override for any name (verified: a
# deliberately fabricated label comes back "=> disabled"), so filtering by what is
# loaded at trim time would silently skip a job that has not started YET — which
# on a device 60 s past boot is most of the interesting ones.
TODO="$LABELS"
for pass in 1 2 3; do
  MISSING="$(comm -23 <(printf '%s\n' "$TODO") <(disabled_set))"
  [ -z "$MISSING" ] && break
  N="$(printf '%s\n' "$MISSING" | /usr/bin/grep -c .)"
  echo "sim-trim: pass $pass — disabling $N job(s)"
  printf '%s\n' "$MISSING" | xargs -P 6 -I{} sh -c \
    'xcrun simctl spawn "$SIM_ID" launchctl disable "$DOMAIN/{}" >/dev/null 2>&1' >/dev/null 2>&1
done
printf '%s\n' "$TODO" | xargs -P 6 -I{} sh -c \
  'xcrun simctl spawn "$SIM_ID" launchctl bootout "$DOMAIN/{}" >/dev/null 2>&1' >/dev/null 2>&1

[ "$SETTLE" -gt 0 ] && sleep "$SETTLE"

# --- verify by reading the device back, never by an exit code -----------------
RUNNING="$(lc list | awk -F'\t' 'NR>1 && $1 != "-" {print $3}' | sort -u)"
DISABLED="$(disabled_set)"

SURVIVORS="$(comm -12 <(printf '%s\n' "$LABELS") <(printf '%s\n' "$RUNNING"))"
NOT_MARKED="$(comm -23 <(printf '%s\n' "$TODO") <(printf '%s\n' "$DISABLED"))"

AFTER="$(sim_proc_count)"
echo "sim-trim: procs_before=$BEFORE procs_after=$AFTER  (delta $((AFTER - BEFORE)))"
echo "sim-trim: $(printf '%s\n' "$DISABLED" | /usr/bin/grep -c . ) labels recorded disabled by launchd"

RC=0
if [ -n "$UNKNOWN" ]; then
  # Reported, not fatal. A name absent from this runtime costs nothing; a name
  # that was RENAMED stops being trimmed and nothing else would say so. The count
  # is printed so a jump after a runtime upgrade is visible rather than inferred.
  echo "sim-trim: $(printf '%s\n' "$UNKNOWN" | /usr/bin/grep -c .) label(s) this runtime did not have loaded at trim time:"
  printf '  %s\n' $UNKNOWN
fi
if [ -n "$NOT_MARKED" ]; then
  echo "sim-trim: asked for but NOT recorded disabled by launchd:"
  printf '  %s\n' $NOT_MARKED
  RC=3
fi
if [ -n "$SURVIVORS" ]; then
  echo "sim-trim: STILL RUNNING after the trim:"
  printf '  %s\n' $SURVIVORS
  RC=3
fi
[ "$RC" = 0 ] && echo "sim-trim: OK — every listed job is disabled and none is running"
exit "$RC"
