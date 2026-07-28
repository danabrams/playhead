#!/usr/bin/env bash
# symbolicate-stability-diagnostics.sh — playhead-jw63.4
#
# Turn the `stability_diagnostics` block of a diagnostics bundle into
# readable stack traces.
#
# A crash record you cannot read is not a pipeline, so this is the other
# half of the bead: the records carry a binary UUID plus an offset into
# each image's __TEXT segment, which is exactly what `atos` consumes.
#
# Usage:
#   scripts/symbolicate-stability-diagnostics.sh <bundle.json> [dsym-search-dir]
#   scripts/symbolicate-stability-diagnostics.sh --self-test
#
#   <bundle.json>       a `playhead-diagnostics-*.json` export, OR the raw
#                       `stability-diagnostics.jsonl` ring buffer.
#   [dsym-search-dir]   where to look for .dSYM bundles. Defaults to
#                       ~/Library/Developer/Xcode/Archives, which is where
#                       every archive you have ever uploaded already lives.
#
# How a record becomes actionable, end to end:
#
#   1. Read `app_version` / `app_build_version` OFF THE RECORD, not off
#      the bundle. MetricKit delivers up to 24 h late, routinely after
#      the user has taken an update, so the bundle's version is the
#      exporting build and the record's is the crashing one.
#   2. Match `binary_uuid` against `dwarfdump --uuid` over the dSYMs in
#      the search dir. The UUID is the only reliable key — filenames and
#      versions both lie after a rebuild.
#   3. `atos -o <DWARF binary> -arch arm64e -l 0 <offset>`. Loading at 0
#      is what makes `offset_into_binary_text_segment` directly usable
#      and is why the records deliberately do NOT carry the ASLR'd
#      absolute address.
#
# Frames from system images are printed unsymbolicated — you almost never
# need them, and their dSYMs are not on this machine.

set -euo pipefail

DSYM_DIR_DEFAULT="$HOME/Library/Developer/Xcode/Archives"

# Cleanup state must be GLOBAL: an EXIT trap fires after the function
# that declared a `local` has returned, and under `set -u` a trap
# referencing a dead local aborts with "unbound variable" — which turns
# a successful run into a non-zero exit.
SCRATCH_DIR=""
UUID_INDEX=""
cleanup () {
  [ -n "$SCRATCH_DIR" ] && rm -rf "$SCRATCH_DIR"
  [ -n "$UUID_INDEX" ] && rm -f "$UUID_INDEX"
  return 0
}
trap cleanup EXIT

usage () {
  sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

# ---------------------------------------------------------------------------
# Extract records from either input shape.
#
# Emits one TSV line per frame:
#   record_index  kind  app_version  app_build  depth  binary_name  uuid  offset
# plus a `#` header line per record carrying the triage codes.
# ---------------------------------------------------------------------------
extract_records () {
  python3 - "$1" <<'PY'
import json, sys

path = sys.argv[1]
raw = open(path, 'r', encoding='utf-8').read().strip()

records = []
if raw.startswith('{') and '"stability_diagnostics"' in raw:
    doc = json.loads(raw)
    records = doc.get('default', {}).get('stability_diagnostics', [])
elif raw.startswith('['):
    records = json.loads(raw)
else:
    # JSON Lines ring buffer straight off the device.
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            # A torn tail costs one record, not the run.
            continue

if not records:
    sys.stderr.write("no stability_diagnostics records found in %s\n" % path)
    sys.exit(3)

for i, r in enumerate(records):
    summary = [
        r.get('kind', '?'),
        'app=%s(%s)' % (r.get('app_version', '?'), r.get('app_build_version', '?')),
        'os=%s' % r.get('os_version', '?'),
        'device=%s' % r.get('device_type', '?'),
    ]
    for key, label in (('signal', 'signal'),
                       ('exception_type', 'exc_type'),
                       ('exception_code', 'exc_code'),
                       ('termination_namespace', 'term_ns'),
                       ('termination_code', 'term_code'),
                       ('objc_exception_name', 'objc'),
                       ('hang_duration_ms', 'hang_ms'),
                       ('writes_caused_mb', 'writes_mb'),
                       ('launch_duration_ms', 'launch_ms')):
        if r.get(key) is not None:
            summary.append('%s=%s' % (label, r[key]))
    if r.get('frames_truncated'):
        summary.append('frames=%d/%d(truncated)' % (len(r.get('frames', [])),
                                                    r.get('frame_count', 0)))
    print('#\t%d\t%s\t%s' % (i, ' '.join(summary),
                              r.get('platform_architecture') or 'arm64e'))
    for f in r.get('frames', []):
        print('\t'.join([
            str(i),
            str(f.get('depth', 0)),
            f.get('binary_name') or '<unknown>',
            f.get('binary_uuid') or '',
            str(f.get('offset_into_binary_text_segment', 0)),
        ]))
PY
}

# ---------------------------------------------------------------------------
# Build a UUID -> DWARF-binary index over every dSYM under the search dir.
# ---------------------------------------------------------------------------
build_uuid_index () {
  local search_dir="$1" index_file="$2"
  : > "$index_file"
  [ -d "$search_dir" ] || return 0
  while IFS= read -r dsym; do
    # `dwarfdump --uuid` prints: UUID: <uuid> (<arch>) <path>
    dwarfdump --uuid "$dsym" 2>/dev/null | while read -r _ uuid _arch path; do
      [ -n "${uuid:-}" ] || continue
      printf '%s\t%s\n' "$uuid" "$path" >> "$index_file"
    done
  done < <(find "$search_dir" -maxdepth 6 -name '*.dSYM' -print 2>/dev/null)
}

self_test () {
  SCRATCH_DIR="$(mktemp -d)"
  cat > "$SCRATCH_DIR/bundle.json" <<'JSON'
{
  "default": {
    "app_version": "9.9.9",
    "stability_diagnostics": [
      {
        "kind": "hang",
        "app_version": "1.0.0",
        "app_build_version": "42",
        "os_version": "iPhone OS 27.0 (25A123)",
        "device_type": "iPhone17,1",
        "platform_architecture": "arm64e",
        "hang_duration_ms": 3300,
        "frames_truncated": true,
        "frame_count": 40,
        "frames": [
          {"binary_name": "Playhead", "binary_uuid": "A1B2C3D4-E5F6-4708-9A0B-1C2D3E4F5061",
           "offset_into_binary_text_segment": 123456, "depth": 0}
        ]
      }
    ]
  }
}
JSON
  local out
  out="$(extract_records "$SCRATCH_DIR/bundle.json")"
  printf '%s\n' "$out"
  # The record's own version must win over the bundle's — that is the
  # whole reason the field is duplicated per record.
  printf '%s' "$out" | grep -q 'app=1.0.0(42)' \
    || { echo "SELF-TEST FAIL: per-record app version not used"; exit 1; }
  printf '%s' "$out" | grep -q 'hang_ms=3300' \
    || { echo "SELF-TEST FAIL: hang duration not surfaced"; exit 1; }
  printf '%s' "$out" | grep -q 'truncated' \
    || { echo "SELF-TEST FAIL: truncation not surfaced"; exit 1; }
  printf '%s' "$out" | grep -q '123456' \
    || { echo "SELF-TEST FAIL: frame offset not surfaced"; exit 1; }
  # The architecture must reach the header line, or atos picks the
  # wrong slice of a fat dSYM.
  printf '%s' "$out" | grep -q 'arm64e' \
    || { echo "SELF-TEST FAIL: platform_architecture not surfaced"; exit 1; }
  echo "SELF-TEST OK"
}

main () {
  case "${1:-}" in
    ''|-h|--help) usage 0 ;;
    --self-test)  self_test; exit 0 ;;
  esac

  local bundle="$1"
  local dsym_dir="${2:-$DSYM_DIR_DEFAULT}"
  [ -f "$bundle" ] || { echo "no such file: $bundle" >&2; exit 2; }

  UUID_INDEX="$(mktemp)"
  build_uuid_index "$dsym_dir" "$UUID_INDEX"
  if [ ! -s "$UUID_INDEX" ]; then
    echo "note: no dSYMs found under $dsym_dir — printing raw offsets only" >&2
  fi

  local arch="arm64e"
  extract_records "$bundle" | while IFS=$'\t' read -r a b c d e; do
    if [ "$a" = "#" ]; then
      printf '\n=== record %s: %s\n' "$b" "$c"
      # Field 4 of a header line is the record's platform_architecture.
      # atos picks the wrong slice of a fat dSYM without it.
      [ -n "$d" ] && arch="$d"
      continue
    fi
    local depth="$b" name="$c" uuid="$d" offset="$e"
    local dwarf=""
    [ -n "$uuid" ] && dwarf="$(awk -F'\t' -v u="$uuid" '$1==u {print $2; exit}' "$UUID_INDEX")"
    if [ -n "$dwarf" ]; then
      local symbol
      symbol="$(atos -o "$dwarf" -arch "$arch" -l 0 "$offset" 2>/dev/null || true)"
      [ -n "$symbol" ] || symbol="<atos failed>"
      printf '  %2s  %-24s %s\n' "$depth" "$name" "$symbol"
    else
      printf '  %2s  %-24s +%s  (uuid %s — no dSYM)\n' "$depth" "$name" "$offset" "${uuid:-none}"
    fi
  done
}

main "$@"
