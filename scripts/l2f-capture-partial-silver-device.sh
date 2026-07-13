#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEVICE_UDID="00008140-001609A42660801C"
EXPECTED_DEVICE_NAME="iPhone 16 Pro Max"
EXPECTED_DEVICE_PRODUCT="iPhone17,2"
EXPECTED_OS_VERSION="27.0"
EXPECTED_OS_BUILD="24A5380h"
APP_BUNDLE_ID="com.playhead.app"
DEVELOPER_DIR_PATH="/Applications/Xcode-beta.app/Contents/Developer"
SIGNING_IDENTITY="Apple Development"
EXPECTED_SIGNING_CERTIFICATE_SHA1="DD824A63001CFE27369E8FDE1ADE2C4E1EE1221F"
run_id=""
output=""
corpus_root="$ROOT"
derived_data="/tmp/playhead-l2f8-device-derived"
preflight_only=0

usage() {
  echo "usage: $0 --run-id baseline-run-{1,2,3} --output /absolute/playhead-partial-silver-baseline-baseline-run-N.json [--corpus-root /absolute/path] [--derived-data /absolute/path] [--preflight-only]" >&2
  exit 2
}

while (($#)); do
  case "$1" in
    --run-id) run_id="${2:-}"; shift 2 ;;
    --output) output="${2:-}"; shift 2 ;;
    --corpus-root) corpus_root="${2:-}"; shift 2 ;;
    --derived-data) derived_data="${2:-}"; shift 2 ;;
    --preflight-only) preflight_only=1; shift ;;
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
output_dir="$(dirname "$output")"
[[ -d "$output_dir" && ! -L "$output_dir" ]] || {
  echo "output directory must already be a regular directory: $output_dir" >&2
  exit 1
}

cd "$ROOT"
case "$output" in
  "$ROOT"|"$ROOT"/*)
    echo "baseline output must be outside the source worktree: $output" >&2
    exit 1
    ;;
esac
revision="$(git rev-parse --verify HEAD)"
require_clean_source() {
  local current_status
  current_status="$(git status --porcelain=v1 --untracked-files=all)"
  [[ -z "$current_status" ]] || {
    echo "physical-device capture requires a clean tracked and untracked source worktree" >&2
    printf '%s\n' "$current_status" >&2
    return 1
  }
}
require_clean_source || exit 1

host_temp="$(mktemp -d "${TMPDIR:-/tmp}/playhead-l2f8-device.XXXXXX")"
capture_fs="$ROOT/scripts/l2f_capture_fs.py"
device_helper="$ROOT/scripts/l2f_capture_device.py"
staging_name=""
output_identity=""
staging_identity=""
preflight_xctestrun=""
capture_xctestrun=""
cleanup() {
  if [[ -n "$staging_name" ]]; then
    python3 "$capture_fs" cleanup \
      "$output_dir" "$output_identity" "$staging_name" "$staging_identity" \
      >/dev/null 2>&1 || true
  fi
  [[ -z "$preflight_xctestrun" ]] || rm -f "$preflight_xctestrun"
  [[ -z "$capture_xctestrun" ]] || rm -f "$capture_xctestrun"
  rm -rf "$host_temp"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

device_list_json="$host_temp/devices.json"
device_identity_json="$host_temp/device-identity.json"
current_device_identity="$host_temp/device-identity-current.json"
device_lock_json="$host_temp/device-lock-state.json"
read_device_identity() {
  local destination="$1"
  local core_device_identifier
  DEVELOPER_DIR="$DEVELOPER_DIR_PATH" xcrun devicectl list devices \
    --json-output "$device_list_json" --quiet
  python3 "$device_helper" device-info "$device_list_json" "$DEVICE_UDID" \
    > "$destination"
  core_device_identifier="$(python3 - "$destination" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    print(json.load(handle)["core_device_identifier"])
PY
)"
  DEVELOPER_DIR="$DEVELOPER_DIR_PATH" xcrun devicectl device info lockState \
    --device "$DEVICE_UDID" --json-output "$device_lock_json" --quiet
  python3 "$device_helper" lock-state \
    "$device_lock_json" "$core_device_identifier"
}
read_device_identity "$device_identity_json"

# Structured device metadata is pinned before build, staging, tests, and publication.
python3 - "$device_identity_json" <<PY
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    device = json.load(handle)
expected = {
    "udid": "$DEVICE_UDID",
    "marketing_name": "$EXPECTED_DEVICE_NAME",
    "product_type": "$EXPECTED_DEVICE_PRODUCT",
    "os_version": "$EXPECTED_OS_VERSION",
    "os_build": "$EXPECTED_OS_BUILD",
}
for key, value in expected.items():
    if device.get(key) != value:
        raise SystemExit(f"device {key} drift: {device.get(key)!r} != {value!r}")
PY

signing_probe="$host_temp/signing-probe"
printf 'playhead physical-device signing probe\n' > "$signing_probe"
codesign --force --sign "$EXPECTED_SIGNING_CERTIFICATE_SHA1" \
  --timestamp=none "$signing_probe" || {
  echo "Apple Development signing key is unavailable; unlock the login keychain before capture" >&2
  exit 1
}

DEVELOPER_DIR="$DEVELOPER_DIR_PATH" xcodebuild build-for-testing \
  -project Playhead.xcodeproj \
  -scheme Playhead \
  -testPlan Playhead \
  -destination "platform=iOS,id=$DEVICE_UDID" \
  -derivedDataPath "$derived_data" \
  CODE_SIGNING_ALLOWED=YES \
  CODE_SIGN_IDENTITY="$SIGNING_IDENTITY"

app_path="$derived_data/Build/Products/Debug-iphoneos/Playhead.app"
[[ -d "$app_path" ]] || {
  echo "build-for-testing did not produce the Playhead test host: $app_path" >&2
  exit 1
}
set -- "$derived_data"/Build/Products/*.xctestrun
[[ "$#" -eq 1 && -f "$1" ]] || {
  echo "expected exactly one build-for-testing xctestrun" >&2
  exit 1
}
xctestrun="$1"

codesign --verify --strict "$app_path"
codesign --verify --strict "$app_path/PlugIns/PlayheadTests.xctest"
verify_signing_leaf() {
  local signed_path="$1"
  local certificate_prefix="$2"
  codesign -d --extract-certificates="$certificate_prefix" "$signed_path"
  python3 - "${certificate_prefix}0" "$EXPECTED_SIGNING_CERTIFICATE_SHA1" <<'PY'
import hashlib
import sys

with open(sys.argv[1], "rb") as handle:
    actual = hashlib.sha1(handle.read()).hexdigest().upper()
if actual != sys.argv[2]:
    raise SystemExit(f"signed code certificate drift: {actual} != {sys.argv[2]}")
PY
}
verify_signing_leaf "$app_path" "$host_temp/app-signing-certificate-"
verify_signing_leaf \
  "$app_path/PlugIns/PlayheadTests.xctest" \
  "$host_temp/test-signing-certificate-"
entitlements="$host_temp/entitlements.plist"
codesign -d --entitlements :- "$app_path" > "$entitlements"
python3 - "$entitlements" <<'PY'
import plistlib
import sys

with open(sys.argv[1], "rb") as handle:
    entitlements = plistlib.load(handle)
identifier = entitlements.get("application-identifier", "")
if not identifier.endswith(".com.playhead.app"):
    raise SystemExit("signed test host has the wrong application identifier")
PY

python3 - "$app_path/BuildProvenance.plist" "$revision" <<'PY'
import plistlib
import sys

with open(sys.argv[1], "rb") as handle:
    provenance = plistlib.load(handle)
build_revision = provenance.get("BuildCommitSHA", "")
if not 7 <= len(build_revision) <= 40 or not sys.argv[2].startswith(build_revision):
    raise SystemExit("signed test host source revision differs from clean Git HEAD")
PY

install_json="$host_temp/install.json"
DEVELOPER_DIR="$DEVELOPER_DIR_PATH" xcrun devicectl device install app \
  --device "$DEVICE_UDID" "$app_path" --json-output "$install_json" --quiet

staging_root="$host_temp/input"
python3 "$device_helper" stage \
  "$ROOT" "$corpus_root" "$staging_root" "$revision" "$run_id"

device_relative_root="l2f8/$revision/$run_id"
device_input_relative="$device_relative_root/input"
device_input_destination="Documents/$device_input_relative"
device_output_relative="$device_input_relative/output/$expected_output_name"
device_output_source="Documents/$device_output_relative"
device_preflight_relative="$device_input_relative/output/playhead-l2f8-preflight.json"
device_preflight_source="Documents/$device_preflight_relative"

# devicectl contract: --destination Documents/l2f8/<revision>/<run>/input
# Copying to bare Documents merges content and is intentionally forbidden.
copy_json="$host_temp/copy-to-device.json"
DEVELOPER_DIR="$DEVELOPER_DIR_PATH" xcrun devicectl device copy to \
  --device "$DEVICE_UDID" \
  --source "$staging_root" \
  --destination "$device_input_destination" \
  --domain-type appDataContainer \
  --domain-identifier "$APP_BUNDLE_ID" \
  --remove-existing-content true \
  --json-output "$copy_json" --quiet

read_device_identity "$current_device_identity"
cmp -s "$device_identity_json" "$current_device_identity" || {
  echo "physical device identity changed after build or staging" >&2
  exit 1
}

xcode_version_actual="$(DEVELOPER_DIR="$DEVELOPER_DIR_PATH" xcodebuild -version | paste -sd ' ' -)"
common_environment=(
  "PLAYHEAD_BASELINE_DEVICE_MODE=1"
  "PLAYHEAD_BASELINE_RUN_ID=$run_id"
  "PLAYHEAD_BASELINE_SOURCE_REVISION=$revision"
  "PLAYHEAD_BASELINE_DEVICE_INPUT_ROOT=$device_input_relative"
  "PLAYHEAD_BASELINE_DEVICE_OUTPUT_PATH=$device_output_relative"
  "PLAYHEAD_BASELINE_DEVICE_PREFLIGHT_OUTPUT_PATH=$device_preflight_relative"
  "PLAYHEAD_BASELINE_DEVICE_UDID=$DEVICE_UDID"
  "PLAYHEAD_BASELINE_DEVICE_OS_VERSION=$EXPECTED_OS_VERSION"
  "PLAYHEAD_BASELINE_DEVICE_OS_BUILD=$EXPECTED_OS_BUILD"
  "XCODE_VERSION_ACTUAL=$xcode_version_actual"
)
xctestrun_directory="$(dirname "$xctestrun")"
preflight_xctestrun="$xctestrun_directory/.playhead-l2f8-$$-preflight.xctestrun"
python3 "$device_helper" patch-xctestrun "$xctestrun" "$preflight_xctestrun" \
  "${common_environment[@]}" \
  "PLAYHEAD_BASELINE_DEVICE_PREFLIGHT=1" \
  "PLAYHEAD_PARTIAL_SILVER_BASELINE=0"

DEVELOPER_DIR="$DEVELOPER_DIR_PATH" xcodebuild test-without-building \
  -xctestrun "$preflight_xctestrun" \
  -destination "platform=iOS,id=$DEVICE_UDID" \
  -only-testing:'PlayheadTests/PipelineDumpLiveTests/testPhysicalDeviceBaselinePreflight'

retrieved_preflight="$host_temp/playhead-l2f8-preflight.json"
retrieve_preflight_json="$host_temp/copy-preflight.json"
DEVELOPER_DIR="$DEVELOPER_DIR_PATH" xcrun devicectl device copy from \
  --device "$DEVICE_UDID" \
  --source "$device_preflight_source" \
  --destination "$retrieved_preflight" \
  --domain-type appDataContainer \
  --domain-identifier "$APP_BUNDLE_ID" \
  --json-output "$retrieve_preflight_json" --quiet
python3 "$device_helper" validate-preflight \
  "$retrieved_preflight" "$run_id" "$revision" "$device_identity_json"

[[ "$(git rev-parse --verify HEAD)" == "$revision" ]] || {
  echo "Git HEAD changed during physical-device preflight" >&2
  exit 1
}
require_clean_source || exit 1
read_device_identity "$current_device_identity"
cmp -s "$device_identity_json" "$current_device_identity" || {
  echo "physical device identity changed during preflight" >&2
  exit 1
}
if [[ "$preflight_only" -eq 1 ]]; then
  printf 'physical-device preflight passed for %s at %s on %s (%s)\n' \
    "$run_id" "$revision" "$EXPECTED_OS_VERSION" "$EXPECTED_OS_BUILD"
  exit 0
fi

IFS=$'\t' read -r staging_name output_identity staging_identity < <(
  python3 "$capture_fs" stage "$output_dir"
)
[[ -n "$staging_name" && -n "$output_identity" && -n "$staging_identity" ]] || {
  echo "could not establish descriptor-pinned capture staging" >&2
  exit 1
}
staging_dir="$output_dir/$staging_name"
staged_output="$staging_dir/$expected_output_name"

capture_xctestrun="$xctestrun_directory/.playhead-l2f8-$$-capture.xctestrun"
python3 "$device_helper" patch-xctestrun "$xctestrun" "$capture_xctestrun" \
  "${common_environment[@]}" \
  "PLAYHEAD_BASELINE_DEVICE_PREFLIGHT=0" \
  "PLAYHEAD_PARTIAL_SILVER_BASELINE=1"
DEVELOPER_DIR="$DEVELOPER_DIR_PATH" xcodebuild test-without-building \
  -xctestrun "$capture_xctestrun" \
  -destination "platform=iOS,id=$DEVICE_UDID" \
  -only-testing:'PlayheadTests/PipelineDumpLiveTests/testProductionPipelineDumpOnNewEpisodes'

retrieve_raw_json="$host_temp/copy-raw.json"
DEVELOPER_DIR="$DEVELOPER_DIR_PATH" xcrun devicectl device copy from \
  --device "$DEVICE_UDID" \
  --source "$device_output_source" \
  --destination "$staged_output" \
  --domain-type appDataContainer \
  --domain-identifier "$APP_BUNDLE_ID" \
  --json-output "$retrieve_raw_json" --quiet
python3 "$device_helper" validate-raw \
  "$staged_output" "$run_id" "$revision" "$device_identity_json" \
  "$staging_root/playhead-l2f8-device-staging.json"

[[ "$(git rev-parse --verify HEAD)" == "$revision" ]] || {
  echo "Git HEAD changed during physical-device capture" >&2
  exit 1
}
require_clean_source || exit 1
read_device_identity "$current_device_identity"
cmp -s "$device_identity_json" "$current_device_identity" || {
  echo "physical device identity changed during capture" >&2
  exit 1
}

published_sha256="$(
  python3 "$capture_fs" publish \
    "$output_dir" "$output_identity" "$staging_name" "$staging_identity" \
    "$expected_output_name" "$expected_output_name"
)" || {
  echo "failed to publish baseline output: $output" >&2
  exit 1
}
python3 "$capture_fs" verify \
  "$output_dir" "$output_identity" "$expected_output_name" "$published_sha256"
[[ "$(git rev-parse --verify HEAD)" == "$revision" ]] || {
  echo "Git HEAD changed during physical-device publication" >&2
  exit 1
}
require_clean_source || exit 1
read_device_identity "$current_device_identity"
cmp -s "$device_identity_json" "$current_device_identity" || {
  echo "physical device identity changed during publication" >&2
  exit 1
}
python3 "$capture_fs" cleanup \
  "$output_dir" "$output_identity" "$staging_name" "$staging_identity"
staging_name=""
trap - EXIT HUP INT TERM
rm -f "$preflight_xctestrun" "$capture_xctestrun"
rm -rf "$host_temp"
printf 'captured %s at %s on %s (%s) -> %s\n' \
  "$run_id" "$revision" "$EXPECTED_OS_VERSION" "$EXPECTED_OS_BUILD" "$output"
