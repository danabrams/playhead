#!/bin/bash
# scripts/lint.sh — the SwiftLint gate for Playhead (playhead-ia2s)
#
# Runs in ~2-6s with no build. Config and rationale live in .swiftlint.yml.
#
#   scripts/lint.sh                      # whole repo, strict — the gate
#   scripts/lint.sh --changed            # only Swift files changed vs merge-base
#   scripts/lint.sh Playhead/Foo.swift   # explicit paths
#   scripts/lint.sh --lenient            # report violations but exit 0
#   scripts/lint.sh --fix <path>...      # autocorrect, EXPLICIT PATHS ONLY
#
# Any extra flags are forwarded to swiftlint (e.g. --reporter json).
#
# WHY --strict IS THE DEFAULT
#   Every rule in .swiftlint.yml is `warning` severity so that bare `swiftlint`
#   (Xcode, editors) stays advisory and never blocks an unrelated build. This
#   script adds --strict, which turns warnings into a non-zero exit. The
#   baseline is 0 violations, measured, so zero-tolerance costs nothing: if
#   this script fails, the violation is in YOUR diff.
#
# EXIT CODES
#   0   clean
#   2   violations found (swiftlint's own code)
#   70  swiftlint not installed, or no usable toolchain — an INFRASTRUCTURE
#       failure, deliberately distinct from "your code is dirty" so that
#       fast-gate.sh can warn-and-continue instead of failing a test run for a
#       missing dev tool.

set -uo pipefail
cd "$(dirname "$0")/.." || exit 70
REPO_ROOT="$PWD"

# ── Toolchain ────────────────────────────────────────────────────────────────
# SwiftLint loads sourcekitdInProc from the active developer dir. This box's
# global `xcode-select` points at CommandLineTools, which does not ship it, so
# bare `swiftlint` dies with:
#   "SourceKittenFramework/library_wrapper.swift: Fatal error: Loading
#    sourcekitdInProc.framework/Versions/A/sourcekitdInProc failed"
# Resolve a real Xcode unless the caller already set DEVELOPER_DIR.
if [ -z "${DEVELOPER_DIR:-}" ]; then
  for candidate in \
    /Applications/Xcode.app/Contents/Developer \
    /Applications/Xcode-beta.app/Contents/Developer
  do
    if [ -d "$candidate" ]; then
      export DEVELOPER_DIR="$candidate"
      break
    fi
  done
fi
if [ -z "${DEVELOPER_DIR:-}" ] && ! xcode-select -p 2>/dev/null | grep -qv CommandLineTools; then
  echo "lint: no full Xcode found — SwiftLint needs sourcekitd." >&2
  echo "lint: install Xcode, or set DEVELOPER_DIR=/path/to/Xcode.app/Contents/Developer" >&2
  exit 70
fi

SWIFTLINT="${SWIFTLINT_BIN:-$(command -v swiftlint 2>/dev/null || true)}"
for candidate in /opt/homebrew/bin/swiftlint /usr/local/bin/swiftlint; do
  [ -n "$SWIFTLINT" ] && break
  [ -x "$candidate" ] && SWIFTLINT="$candidate"
done
if [ -z "$SWIFTLINT" ]; then
  echo "lint: swiftlint not found — install it with 'brew install swiftlint'" >&2
  exit 70
fi

if [ ! -f "$REPO_ROOT/.swiftlint.yml" ]; then
  echo "lint: no .swiftlint.yml at $REPO_ROOT — refusing to lint with stock defaults" >&2
  echo "lint: (stock defaults emit tens of thousands of violations on this repo)" >&2
  exit 70
fi

# ── Argument parsing ─────────────────────────────────────────────────────────
MODE="all"
STRICT="--strict"
ACTION="lint"
# SwiftLint logs one "Linting 'Foo.swift' (n/1120)" line per file. That is 1,120
# lines of noise around what is usually zero violations, and it buries the
# result in any gate transcript. Quiet by default; --verbose restores it.
QUIET="--quiet"
PASSTHROUGH=()
PATHS=()

while [ $# -gt 0 ]; do
  case "$1" in
    --changed)  MODE="changed" ;;
    --lenient)  STRICT="" ;;
    --strict)   STRICT="--strict" ;;
    --verbose)  QUIET="" ;;
    --quiet)    QUIET="--quiet" ;;
    --fix|--autocorrect) ACTION="fix" ;;
    -h|--help)  sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*)         PASSTHROUGH+=("$1") ;;
    *)          PATHS+=("$1") ;;
  esac
  shift
done

# ── --fix guard ──────────────────────────────────────────────────────────────
# A repo-wide `swiftlint --fix` would rewrite ~1,100 files: it destroys `git
# blame` and conflicts with every open bead branch. .swiftlint.yml's whole
# premise is that adoption happens rule-by-rule with reviewed edits. So --fix
# is allowed only against paths you named explicitly.
if [ "$ACTION" = "fix" ]; then
  if [ ${#PATHS[@]} -eq 0 ]; then
    echo "lint: refusing a repo-wide --fix." >&2
    echo "lint: a mass autocorrect destroys git blame and collides with every open" >&2
    echo "lint: bead branch. Pass explicit paths: scripts/lint.sh --fix Playhead/Foo.swift" >&2
    exit 70
  fi
  echo "lint: autocorrecting ${#PATHS[@]} path(s) — review the diff before committing"
  "$SWIFTLINT" lint --fix --config "$REPO_ROOT/.swiftlint.yml" \
    "${PASSTHROUGH[@]+"${PASSTHROUGH[@]}"}" -- "${PATHS[@]}"
  exit $?
fi

# ── --changed: resolve the diff against the merge-base ───────────────────────
if [ "$MODE" = "changed" ]; then
  BASE=""
  for ref in origin/main main; do
    if git rev-parse --verify --quiet "$ref" >/dev/null; then
      BASE="$(git merge-base HEAD "$ref" 2>/dev/null)" && [ -n "$BASE" ] && break
    fi
  done
  if [ -z "$BASE" ]; then
    echo "lint: could not resolve a merge-base against origin/main or main; linting everything" >&2
    MODE="all"
  else
    # Committed changes vs the base, plus anything dirty or untracked in the
    # working tree — the point of --changed is "what am I about to hand over",
    # which includes edits not yet committed.
    while IFS= read -r f; do
      [ -n "$f" ] && [ -f "$f" ] && PATHS+=("$f")
    done < <(
      {
        git diff --name-only --diff-filter=ACMR "$BASE" -- '*.swift'
        git diff --name-only --diff-filter=ACMR HEAD -- '*.swift'
        git ls-files --others --exclude-standard -- '*.swift'
      } | sort -u
    )
    if [ ${#PATHS[@]} -eq 0 ]; then
      echo "lint: no Swift files changed vs $(git rev-parse --short "$BASE") — nothing to lint"
      exit 0
    fi
    echo "lint: ${#PATHS[@]} changed Swift file(s) vs $(git rev-parse --short "$BASE")"
  fi
fi

# ── Run ──────────────────────────────────────────────────────────────────────
# No pipe: a pipeline would report the LAST command's exit code and a failed
# lint would silently read as success.
if [ ${#PATHS[@]} -gt 0 ]; then
  "$SWIFTLINT" lint --config "$REPO_ROOT/.swiftlint.yml" ${STRICT:+"$STRICT"} ${QUIET:+"$QUIET"} \
    "${PASSTHROUGH[@]+"${PASSTHROUGH[@]}"}" -- "${PATHS[@]}"
else
  "$SWIFTLINT" lint --config "$REPO_ROOT/.swiftlint.yml" ${STRICT:+"$STRICT"} ${QUIET:+"$QUIET"} \
    "${PASSTHROUGH[@]+"${PASSTHROUGH[@]}"}"
fi
RC=$?

if [ "$RC" -eq 0 ]; then
  echo "lint: clean"
else
  echo "lint: FAILED (exit $RC). The baseline is green, so this is in your diff." >&2
  echo "lint: rules and rationale — .swiftlint.yml" >&2
fi
exit "$RC"
