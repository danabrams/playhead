#!/usr/bin/env bash
# playhead-k99yv: resolve DEVELOPER_DIR when the global xcode-select is a
# CommandLineTools instance. Sourced by fast-gate.sh; safe to source anywhere.
#
# The box's `xcode-select -p` is /Library/Developer/CommandLineTools — no
# xcodebuild, no simctl — and fast-gate.sh DOCUMENTED DEVELOPER_DIR as an input
# without ever setting it, so a plain invocation died in two seconds with lines
# that read as a config notice. There is exactly one Xcode under /Applications;
# resolving it is not a decision, it is reading the disk. (Changing the GLOBAL
# xcode-select is a different, sudo, system-wide change and stays Dan's call —
# the clone helper reads that, and DEVELOPER_DIR cannot reach it.)
#
# resolve_developer_dir [apps_root]
#   0  xcodebuild already works, or DEVELOPER_DIR was resolved and EXPORTED
#      (one line printed saying which, so a log never hides it)
#   1  DEVELOPER_DIR is unset, xcodebuild does not work, and nothing under
#      apps_root carries usr/bin/xcodebuild — the caller decides what to do
#   2  DEVELOPER_DIR is SET and xcodebuild still does not work — never override
#      an explicit setting; say so
resolve_developer_dir() {
  local apps_root="${1:-/Applications}"
  if xcodebuild -version >/dev/null 2>&1; then
    return 0
  fi
  if [ -n "${DEVELOPER_DIR:-}" ]; then
    echo "fast-gate: DEVELOPER_DIR is set to '$DEVELOPER_DIR' and xcodebuild still does not run — not overriding an explicit setting."
    return 2
  fi
  local candidate chosen=""
  # Newest by name (Xcode-beta sorts after Xcode; versioned names sort by
  # version), first one whose xcodebuild is executable.
  for candidate in $(ls -d "$apps_root"/Xcode*.app 2>/dev/null | sort -r); do
    if [ -x "$candidate/Contents/Developer/usr/bin/xcodebuild" ]; then
      chosen="$candidate/Contents/Developer"; break
    fi
  done
  if [ -z "$chosen" ]; then
    return 1
  fi
  export DEVELOPER_DIR="$chosen"
  echo "fast-gate: DEVELOPER_DIR resolved to '$chosen' — xcode-select -p is '$(xcode-select -p 2>/dev/null || echo unknown)', which has no xcodebuild."
  return 0
}
