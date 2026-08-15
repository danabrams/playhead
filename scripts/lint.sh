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
    # Everything after a bare `--` is forwarded to swiftlint verbatim, with no
    # flag/path classification. The escape hatch for anything below.
    --)         shift
                while [ $# -gt 0 ]; do PASSTHROUGH+=("$1"); shift; done
                break ;;
    # Options that take a SEPARATE value argument. Without this the value gets
    # classified as a path and swiftlint dies with "Missing value for
    # --reporter". `--flag=value` needs no special case and falls through to
    # the generic `-*` branch below.
    --reporter|--baseline|--write-baseline|--cache-path|--compiler-log-path|--working-directory)
                PASSTHROUGH+=("$1")
                if [ $# -ge 2 ]; then PASSTHROUGH+=("$2"); shift; fi ;;
    # --config would collide with the one this script passes; refuse rather
    # than hand swiftlint two and let it pick.
    --config|--config=*)
                echo "lint: --config is not overridable; this script always uses $REPO_ROOT/.swiftlint.yml" >&2
                echo "lint: to lint with a different config, call swiftlint directly" >&2
                exit 70 ;;
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

# ── Exclusion filter for auto-derived paths ──────────────────────────────────
# MEASURED GOTCHA: SwiftLint applies `excluded:` when it walks a directory, but
# NOT to paths handed to it explicitly. So `--changed` touching a Vendor file
# would lint vendored third-party source we do not author — a bead that changed
# nothing of ours could go red after a Vendor bump. Filter the derived list
# against the same `excluded:` block, so there is one source of truth.
#
# Explicitly-typed paths are deliberately NOT filtered: naming a file yourself
# is an intentional override.
excluded_prefixes() {
  awk '
    /^excluded:/            { inblock = 1; next }
    inblock && /^[^ #]/     { inblock = 0 }
    inblock && /^[[:space:]]*-[[:space:]]/ {
      sub(/^[[:space:]]*-[[:space:]]*/, ""); sub(/[[:space:]]+$/, ""); print
    }
  ' "$REPO_ROOT/.swiftlint.yml"
}

is_excluded() {
  local candidate="$1" prefix
  while IFS= read -r prefix; do
    [ -z "$prefix" ] && continue
    case "$candidate" in
      "$prefix"|"$prefix"/*) return 0 ;;
    esac
  done <<EOF
$EXCLUDED
EOF
  return 1
}

EXCLUDED="$(excluded_prefixes)"

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
    SKIPPED=0
    while IFS= read -r f; do
      [ -z "$f" ] && continue
      [ -f "$f" ] || continue          # deleted/renamed-away files
      if is_excluded "$f"; then
        SKIPPED=$((SKIPPED + 1))
        continue
      fi
      PATHS+=("$f")
    done < <(
      {
        git diff --name-only --diff-filter=ACMR "$BASE" -- '*.swift'
        git diff --name-only --diff-filter=ACMR HEAD -- '*.swift'
        git ls-files --others --exclude-standard -- '*.swift'
      } | sort -u
    )
    [ "$SKIPPED" -gt 0 ] && echo "lint: skipped $SKIPPED changed file(s) under an excluded path"
    if [ ${#PATHS[@]} -eq 0 ]; then
      echo "lint: no lintable Swift files changed vs $(git rev-parse --short "$BASE") — nothing to lint"
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

# ── SHAPE 2 preflight (playhead-mfeq) ────────────────────────────────────────
# A structural rule SwiftLint's regex `custom_rules` cannot express, because it
# needs the ENCLOSING TYPE of a declaration: an optional stored `var` named
# current*/pending*/last* on an `actor`. Four shipped defects, one shape — see
# the script's docstring.
#
# It lives here rather than in fast-gate.sh so that it fires wherever the lint
# gate does, costs no build, and reports beside the violations it is a sibling
# of. It runs even when SwiftLint found something, so one invocation reports
# everything rather than making the caller iterate.
#
# WHY IT IS ALWAYS WHOLE-REPO, EVEN UNDER --changed. The rule's second half is
# that no allowlist entry has gone stale, and staleness is a property of the
# WHOLE tree: a licensed field deleted in a file your diff does not touch is
# exactly the case a changed-files scan cannot see. It takes well under a
# second on ~470 files, so there is nothing to buy by scoping it.
if [ ${#PATHS[@]} -eq 0 ] || [ "$MODE" = "changed" ]; then
  if command -v python3 >/dev/null 2>&1; then
    python3 "$REPO_ROOT/scripts/singleton_slot_preflight.py" || {
      # 2 is this script's documented "violations found" code. A preflight
      # violation is a violation; it must not be reported as 70 (a missing dev
      # tool), which fast-gate.sh warns-and-continues on.
      RC=2
    }
  else
    echo "lint: python3 not found — SKIPPING the singleton-slot preflight" >&2
    echo "lint: this is a REAL GAP, not a pass; see scripts/singleton_slot_preflight.py" >&2
  fi
fi

exit "$RC"
