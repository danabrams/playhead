#!/usr/bin/env bash
#
# scratch-sampler.sh — measure where a test run's disk actually goes.
#
# playhead-cgka. A full `PlayheadFastTests` gate on this box dies with
# NSPOSIXErrorDomain Code=28 ("No space left on device") near the END of the
# suite, which is the signature of MONOTONIC growth: consumption only ever
# rises, so the failure lands wherever the ceiling happens to be. The suspected
# driver is `PlayheadTests/Helpers/TestHelpers.swift`'s `makeTempDir`, whose
# only two cleanup paths (a wipe-on-first-use and an `atexit` hook) both fire at
# PROCESS boundaries, so nothing is reclaimed while the suite runs.
#
# "Suspected" is the point of this script. Before changing anything, sample the
# real consumers over time and attribute the growth. Run it in the background
# next to a gate:
#
#     scripts/scratch-sampler.sh --out /tmp/sample.jsonl --interval 10 &
#     SAMPLER=$!
#     scripts/fast-gate.sh -only-testing:PlayheadTests/...
#     kill "$SAMPLER"
#     scripts/scratch-sampler.sh --report /tmp/sample.jsonl
#
# It samples cheaply (a whole-device `du -skx` is ~0.2s on APFS with a warm
# cache) so a 10s interval costs nothing measurable against a 3-minute run.
#
# MODES
#   (default)          sample until killed / --duration expires, one JSON line
#                      per sample on stdout and/or --out
#   --report FILE      summarise a previously captured JSONL: peak, delta,
#                      and whether scratch grew monotonically
#   --breakdown        one-shot: current scratch size grouped by makeTempDir
#                      prefix, largest first. Answers "which tests are big?"
#
# OPTIONS
#   --interval N       seconds between samples (default 10)
#   --duration N       stop after N seconds (default: run until killed)
#   --out FILE         append JSONL here as well as stdout
#   --quiet            do not echo samples to stdout
#   --with-hosttmp     also walk $TMPDIR (~1.2s/sample; off by default)
#   --guard-pid PID    kill this PID (SIGINT, then SIGTERM) if free space falls
#                      below --guard-free-mib. BY PID ONLY — never a pkill
#                      pattern, which self-matches any guard whose own command
#                      line contains the string and has already killed one
#                      agent's run, misreported afterwards as a disk failure.
#   --guard-free-mib N free-space floor for --guard-pid (default: off)
#   --device UDID      simulator to watch (default: the iPhone 17 sim)
#   --derived PATH     derivedData to watch (default: ./.derivedData)
#
# NOTE ON PERMISSIONS: some tests deliberately chmod a directory unreadable
# (e.g. DownloadManagerTests' 0o300 `complete/`), so `du` can hit EACCES and
# UNDERCOUNT. That is why free-space delta is reported alongside the du figures:
# `free_kb` is exact and needs no read permission, so it is the honest total and
# the du columns are the attribution.

set -uo pipefail

MODE=sample
INTERVAL=10
DURATION=0
OUT=""
QUIET=0
DEVICE=""
DERIVED="${PLAYHEAD_DERIVED:-.derivedData}"
REPORT_FILE=""
WITH_HOSTTMP=0
GUARD_PID=""
GUARD_FREE_MIB=0

while [ $# -gt 0 ]; do
  case "$1" in
    --interval) INTERVAL="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --quiet) QUIET=1; shift ;;
    --device) DEVICE="$2"; shift 2 ;;
    --derived) DERIVED="$2"; shift 2 ;;
    --report) MODE=report; REPORT_FILE="$2"; shift 2 ;;
    --breakdown) MODE=breakdown; shift ;;
    --with-hosttmp) WITH_HOSTTMP=1; shift ;;
    --guard-pid) GUARD_PID="$2"; shift 2 ;;
    --guard-free-mib) GUARD_FREE_MIB="$2"; shift 2 ;;
    -h|--help) sed -n '2,50p' "$0"; exit 0 ;;
    *) echo "scratch-sampler: unknown option $1" >&2; exit 2 ;;
  esac
done

SIMROOT="$HOME/Library/Developer/CoreSimulator/Devices"

resolve_device () {
  if [ -n "$DEVICE" ]; then printf '%s' "$SIMROOT/$DEVICE"; return; fi
  # Prefer whichever device dir currently holds a Playhead app container; fall
  # back to the largest device dir. Both beat hardcoding a UDID that changes
  # when the sim is recreated.
  local d
  for d in "$SIMROOT"/*/; do
    [ -d "$d" ] || continue
    if compgen -G "${d}data/Containers/Bundle/Application/*/Playhead.app" >/dev/null 2>&1; then
      printf '%s' "${d%/}"; return
    fi
  done
  du -skx "$SIMROOT"/*/ 2>/dev/null | sort -rn | head -1 | cut -f2
}

DEVDIR="$(resolve_device)"

# Every app data container that currently carries a test scratch root. There can
# be more than one: each install mints a new container UUID and the old ones are
# not always reaped, so summing is the only way to avoid missing the live one.
scratch_dirs () {
  compgen -G "$DEVDIR/data/Containers/Data/Application/*/tmp/PlayheadTestScratch" 2>/dev/null || true
}

# Free space in KiB on the volume the simulator lives on. EXACT — no read
# permission needed anywhere, unlike du.
free_kb () { df -k "$SIMROOT" 2>/dev/null | awk 'NR==2 {print $4}'; }

# `du` exits non-zero when it hits one of the deliberately-unreadable test
# directories, so the value must be taken from stdout rather than from the exit
# status — an `&& … || echo 0` chain here prints BOTH and yields "0\n0".
dir_kb () {
  local kb
  [ -e "$1" ] || { echo 0; return; }
  kb="$(du -skx "$1" 2>/dev/null | awk 'END {print $1}')"
  echo "${kb:-0}"
}

sample_line () {
  local now elapsed free dev scratch entries container derived simlogs hosttmp
  now="$(date +%s)"
  elapsed=$(( now - T0 ))
  free="$(free_kb)"
  dev="$(dir_kb "$DEVDIR")"
  scratch=0; entries=0; container=0
  local s c
  for s in $(scratch_dirs); do
    scratch=$(( scratch + $(dir_kb "$s") ))
    entries=$(( entries + $(ls -f "$s" 2>/dev/null | grep -cv '^\.\{1,2\}$') ))
    c="$(dirname "$(dirname "$s")")"
    container=$(( container + $(dir_kb "$c") ))
  done
  derived="$(dir_kb "$DERIVED")"
  simlogs="$(dir_kb "$HOME/Library/Logs/CoreSimulator")"
  # SWAP IS A DISK CONSUMER, and on this box the DOMINANT one (playhead-cgka,
  # measured 2026-08-02): a full-plan run grew the swapfiles by ~5.6 GiB, which
  # is ~70% of the free space it consumed, against ~2.0 GiB of test scratch. A
  # sampler that walks only directories attributes none of it and makes
  # "No space left on device" look like a pure file-accumulation problem.
  local swaptotal swapused hostrss
  set -- $(sysctl -n vm.swapusage 2>/dev/null)
  swaptotal="${3%M}"; swapused="${6%M}"
  # RSS of the test host, so memory growth and swap growth can be told apart.
  hostrss="$(ps -o rss= -p "$(pgrep -x Playhead 2>/dev/null | head -1)" 2>/dev/null | awk 'END {print $1}')"
  printf '{"t":%s,"elapsed":%s,"free_kb":%s,"dev_kb":%s,"scratch_kb":%s,"scratch_entries":%s,"container_kb":%s,"derived_kb":%s,"simlogs_kb":%s,"swap_total_mb":%s,"swap_used_mb":%s,"host_rss_kb":%s' \
    "$now" "$elapsed" "${free:-0}" "$dev" "$scratch" "$entries" "$container" "$derived" "$simlogs" \
    "${swaptotal:-0}" "${swapused:-0}" "${hostrss:-0}"
  # The user temp folder is tens of GiB of unrelated caches, so walking it costs
  # ~1.2s per sample. Off by default: `free_kb` is the exact catch-all, so this
  # is only needed once the sim + derivedData figures FAIL to account for the
  # consumption.
  if [ "$WITH_HOSTTMP" -eq 1 ]; then
    printf ',"hosttmp_kb":%s' "$(dir_kb "${TMPDIR:-/tmp}")"
  fi
  printf '}\n'
}

# Disk guard. Measuring the ENOSPC failure is the point, but letting the volume
# actually reach zero wedges the simulator and the host, so stop the run while
# there is still headroom and say so. Signals go to an explicit PID — NEVER a
# `pkill -f` pattern, which self-matches any guard whose own command line
# contains the string.
guard_check () {
  [ -n "$GUARD_PID" ] || return 0
  [ "$GUARD_FREE_MIB" -gt 0 ] || return 0
  [ "${GUARD_TRIPPED:-0}" -eq 0 ] || return 0
  local fkb
  fkb="$(free_kb)"
  [ -n "$fkb" ] || return 0
  [ "$fkb" -lt $(( GUARD_FREE_MIB * 1024 )) ] || return 0
  echo "# scratch-sampler: GUARD TRIPPED at $(( fkb / 1024 )) MiB free — SIGINT to pid $GUARD_PID" >&2
  [ -n "$OUT" ] && echo "# GUARD TRIPPED at $(( fkb / 1024 )) MiB free" >>"$OUT"
  kill -INT "$GUARD_PID" 2>/dev/null
  GUARD_TRIPPED=1
}

case "$MODE" in
  breakdown)
    total=0
    for s in $(scratch_dirs); do
      echo "# scratch root: $s"
      # Group by the makeTempDir prefix (everything before the UUID), summing
      # sizes. `du -kd 1` gives per-entry KiB in one pass.
      du -kd 1 "$s" 2>/dev/null \
        | awk -v root="$s" '$2 != root {
            n=split($2, parts, "/"); name=parts[n];
            sub(/-[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$/, "", name);
            kb[name] += $1; cnt[name] += 1
          }
          END {
            for (k in kb) printf "%10d KiB  %6d dirs  %8.1f KiB/dir  %s\n", kb[k], cnt[k], kb[k]/cnt[k], k
          }' \
        | sort -rn
      total=$(( total + $(dir_kb "$s") ))
    done
    echo "# TOTAL scratch: ${total} KiB"
    ;;

  report)
    [ -f "$REPORT_FILE" ] || { echo "scratch-sampler: no such file $REPORT_FILE" >&2; exit 2; }
    python3 - "$REPORT_FILE" <<'PY'
import json, sys
rows = []
for line in open(sys.argv[1]):
    line = line.strip()
    if not line.startswith("{"):
        continue
    try:
        rows.append(json.loads(line))
    except json.JSONDecodeError:
        pass
if not rows:
    print("no samples"); sys.exit(1)

def col(name):
    return [r.get(name, 0) for r in rows]

mib = lambda kb: kb / 1024.0
first, last = rows[0], rows[-1]
print(f"samples={len(rows)}  span={last['elapsed'] - first['elapsed']}s")
print()
print(f"{'metric':16} {'start':>12} {'peak':>12} {'end':>12} {'peak-start':>12}")
for name in ("free_kb", "dev_kb", "scratch_kb", "container_kb", "derived_kb",
             "simlogs_kb", "hosttmp_kb", "host_rss_kb"):
    if not any(name in r for r in rows):
        continue
    vals = col(name)
    peak = min(vals) if name == "free_kb" else max(vals)
    print(f"{name:16} {mib(vals[0]):11.1f}M {mib(peak):11.1f}M {mib(vals[-1]):11.1f}M "
          f"{mib(peak - vals[0]):11.1f}M")
ent = col("scratch_entries")
print(f"{'entries':16} {ent[0]:12d} {max(ent):12d} {ent[-1]:12d} {max(ent) - ent[0]:12d}")
print()

# Monotonicity is the whole question: a per-test teardown turns the scratch
# series from a ramp into a sawtooth. Count how often it went DOWN.
s = col("scratch_kb")
drops = sum(1 for a, b in zip(s, s[1:]) if b < a)
print(f"scratch_kb decreased in {drops}/{max(len(s) - 1, 1)} intervals "
      f"({'MONOTONIC — nothing is reclaimed mid-run' if drops == 0 else 'reclaimed mid-run'})")
e = col("scratch_entries")
edrops = sum(1 for a, b in zip(e, e[1:]) if b < a)
print(f"entries    decreased in {edrops}/{max(len(e) - 1, 1)} intervals")
print()
consumed = first["free_kb"] - min(col("free_kb"))
scratch_peak = max(col("scratch_kb"))
print(f"free space consumed at peak : {mib(consumed):.1f} MiB")
print(f"scratch at its peak         : {mib(scratch_peak):.1f} MiB "
      f"({100.0 * scratch_peak / consumed:.0f}% of consumption)" if consumed > 0 else "")
PY
    ;;

  sample)
    T0="$(date +%s)"
    GUARD_TRIPPED=0
    echo "# scratch-sampler: device=$DEVDIR derived=$DERIVED interval=${INTERVAL}s" >&2
    [ -n "$OUT" ] && echo "# scratch-sampler: device=$DEVDIR derived=$DERIVED" >>"$OUT"
    while :; do
      line="$(sample_line)"
      [ "$QUIET" -eq 1 ] || echo "$line"
      [ -n "$OUT" ] && echo "$line" >>"$OUT"
      if [ "$DURATION" -gt 0 ]; then
        now="$(date +%s)"
        [ $(( now - T0 )) -ge "$DURATION" ] && break
      fi
      # Sleep in 1s steps and check the disk guard on every one. MEASURED
      # 2026-08-02: free space fell from 1.72 GiB to 0.15 GiB inside a single
      # 10s sample period, so a guard evaluated once per sample fires far too
      # late to protect the volume.
      guard_slept=0
      while [ "$guard_slept" -lt "$INTERVAL" ]; do
        guard_check
        sleep 1
        guard_slept=$(( guard_slept + 1 ))
      done
    done
    ;;
esac
