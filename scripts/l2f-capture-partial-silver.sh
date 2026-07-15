#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_id=""
output=""
corpus_root="$ROOT"
derived_data="/tmp/playhead-l2f8-derived"

usage() {
  echo "usage: $0 --run-id baseline-run-{1,2,3} --output /absolute/playhead-partial-silver-baseline-baseline-run-N.json [--corpus-root /absolute/path] [--derived-data /absolute/path]" >&2
  exit 2
}

while (($#)); do
  case "$1" in
    --run-id) run_id="${2:-}"; shift 2 ;;
    --output) output="${2:-}"; shift 2 ;;
    --corpus-root) corpus_root="${2:-}"; shift 2 ;;
    --derived-data) derived_data="${2:-}"; shift 2 ;;
    *) usage ;;
  esac
done

case "$run_id" in
  baseline-run-1|baseline-run-2|baseline-run-3) ;;
  *) usage ;;
esac
[[ "$output" = /* && "$corpus_root" = /* && "$derived_data" = /* ]] || usage
case "/${output#/}/" in
  *"/./"*|*"/../"*) usage ;;
esac
expected_output_name="playhead-partial-silver-baseline-${run_id}.json"
[[ "${output##*/}" == "$expected_output_name" ]] || {
  echo "output filename must be $expected_output_name" >&2
  exit 2
}
[[ ! -e "$output" && ! -L "$output" ]] || {
  echo "refusing to replace existing output: $output" >&2
  exit 1
}

host_macos_version="$(sw_vers -productVersion)" || {
  echo "could not determine the host macOS version" >&2
  exit 1
}
host_macos_major="${host_macos_version%%.*}"
case "$host_macos_major" in
  ''|*[!0-9]*)
    echo "could not parse the host macOS version: $host_macos_version" >&2
    exit 1
    ;;
esac
if ((host_macos_major < 27)); then
  echo "production baseline requires macOS 27 or newer for the project's iOS 27 Catalyst deployment target; host is macOS $host_macos_version" >&2
  exit 1
fi

output_dir="$(dirname "$output")"
[[ -d "$output_dir" && ! -L "$output_dir" ]] || {
  echo "output directory must already be a regular directory: $output_dir" >&2
  exit 1
}

validate_directory_components() {
  local cursor="$1"
  while [[ "$cursor" != "/" ]]; do
    if [[ -L "$cursor" ]]; then
      case "$cursor" in
        /tmp|/var|/etc) ;;
        *)
          echo "output directory contains a symbolic-link component: $cursor" >&2
          return 1
          ;;
      esac
    fi
    cursor="$(dirname "$cursor")"
  done
}
validate_directory_components "$output_dir"

cd "$ROOT"
case "$output" in
  "$ROOT"|"$ROOT"/*)
    echo "baseline output must be outside the source worktree: $output" >&2
    exit 1
    ;;
esac
revision="$(git rev-parse --verify HEAD)"
require_clean_source() {
  local status
  status="$(git status --porcelain=v1 --untracked-files=all)"
  [[ -z "$status" ]] || {
    echo "baseline capture requires a clean tracked and untracked source worktree" >&2
    printf '%s\n' "$status" >&2
    return 1
  }
}
require_clean_source || {
  exit 1
}

capture_fs="$ROOT/scripts/l2f_capture_fs.py"
IFS=$'\t' read -r staging_name output_identity staging_identity < <(
  python3 "$capture_fs" stage "$output_dir"
)
[[ -n "$staging_name" && -n "$output_identity" && -n "$staging_identity" ]] || {
  echo "could not establish descriptor-pinned capture staging" >&2
  exit 1
}
staging_dir="$output_dir/$staging_name"
staged_name="playhead-partial-silver-baseline-${run_id}.json"
staged_output="$staging_dir/$staged_name"
cleanup() {
  [[ -n "${capture_xctestrun:-}" ]] && rm -f "$capture_xctestrun" || true
  python3 "$capture_fs" cleanup \
    "$output_dir" "$output_identity" "$staging_name" "$staging_identity" \
    >/dev/null 2>&1 || true
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# The Catalyst test host does NOT inherit xcodebuild's process environment
# (and `xcodebuild test` offers no other env seam), so the baseline
# configuration must be patched into the xctestrun's EnvironmentVariables —
# the same mechanism the physical-device wrapper uses. build-for-testing
# emits the xctestrun; the explicit -testPlan keeps that to exactly one
# file (without it, every plan in the scheme gets an xctestrun). The plan
# choice does not shape the capture — -only-testing narrows to the single
# live test — but it does require the xcodegen-generated scheme; if the
# plan is missing, regenerate with `xcodegen`.
developer_dir="/Applications/Xcode-beta.app/Contents/Developer"
xcode_version_actual="$(DEVELOPER_DIR="$developer_dir" xcodebuild -version | paste -sd ' ' -)"

# A reused derived-data path can hold xctestruns from earlier builds whose
# scheme state named the test plan differently; clear them so the glob
# below can only match the file this build just emitted.
rm -f "$derived_data"/Build/Products/*.xctestrun

DEVELOPER_DIR="$developer_dir" xcodebuild build-for-testing \
  -project Playhead.xcodeproj \
  -scheme Playhead \
  -testPlan PlayheadFastTests \
  -destination 'platform=macOS,variant=Mac Catalyst' \
  -derivedDataPath "$derived_data"

set -- "$derived_data"/Build/Products/*.xctestrun
[[ "$#" -eq 1 && -f "$1" ]] || {
  echo "expected exactly one build-for-testing xctestrun" >&2
  exit 1
}
xctestrun="$1"
capture_xctestrun="${xctestrun%/*}/.playhead-l2f8-$$-catalyst.xctestrun"
rm -f "$capture_xctestrun"
python3 "$ROOT/scripts/l2f_capture_device.py" patch-xctestrun \
  "$xctestrun" "$capture_xctestrun" \
  "PLAYHEAD_PARTIAL_SILVER_BASELINE=1" \
  "PLAYHEAD_BASELINE_RUN_ID=$run_id" \
  "PLAYHEAD_BASELINE_SOURCE_REVISION=$revision" \
  "PLAYHEAD_BASELINE_OUTPUT_PATH=$staged_output" \
  "PLAYHEAD_CORPUS_ROOT=$corpus_root" \
  "XCODE_VERSION_ACTUAL=$xcode_version_actual"

DEVELOPER_DIR="$developer_dir" xcodebuild test-without-building \
  -xctestrun "$capture_xctestrun" \
  -destination 'platform=macOS,variant=Mac Catalyst' \
  -only-testing:'PlayheadTests/PipelineDumpLiveTests/testProductionPipelineDumpOnNewEpisodes'

[[ "$(git rev-parse --verify HEAD)" = "$revision" ]] || {
  echo "Git HEAD changed during baseline capture" >&2
  exit 1
}
require_clean_source || {
  echo "source worktree changed during baseline capture" >&2
  exit 1
}
[[ -f "$staged_output" && ! -L "$staged_output" ]] || {
  echo "baseline harness did not publish a regular staged output" >&2
  exit 1
}
published_sha256="$({
  python3 "$capture_fs" publish \
    "$output_dir" "$output_identity" "$staging_name" "$staging_identity" \
    "$staged_name" "$expected_output_name"
})" || {
  echo "failed to publish baseline output: $output" >&2
  exit 1
}
[[ "$(git rev-parse --verify HEAD)" = "$revision" ]] || {
  echo "Git HEAD changed during baseline publication" >&2
  exit 1
}
require_clean_source || {
  echo "source worktree changed during baseline publication" >&2
  exit 1
}
python3 "$capture_fs" verify \
  "$output_dir" "$output_identity" "$expected_output_name" "$published_sha256" || {
  echo "published baseline output changed after publication: $output" >&2
  exit 1
}
cleanup
trap - EXIT HUP INT TERM
printf 'captured %s at %s -> %s\n' "$run_id" "$revision" "$output"
