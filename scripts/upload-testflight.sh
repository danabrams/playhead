#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/upload-testflight.sh [--ref <git-ref>] [--keep-worktree]

Builds Playhead from a clean temporary worktree for the requested git ref
(default: `main`) and uploads the resulting archive to TestFlight using the
local Xcode account, signing certificate, and provisioning profile state on
this Mac.

Options:
  --ref <git-ref>     Git ref to build. Defaults to env
                      PLAYHEAD_TESTFLIGHT_REF or `main`.
  --keep-worktree     Preserve the temporary worktree on failure/success.
  --help              Show this help text.

Required environment variables (no defaults — script aborts if unset):
  PLAYHEAD_TESTFLIGHT_TEAM_ID                  Apple Developer team ID
  PLAYHEAD_TESTFLIGHT_BUNDLE_ID                App bundle identifier
  PLAYHEAD_TESTFLIGHT_PROFILE_NAME             Provisioning profile name
  PLAYHEAD_TESTFLIGHT_CODE_SIGN_IDENTITY       Signing identity (full
                                               "Apple Distribution: …" string,
                                               including the team ID suffix)

Optional environment overrides:
  PLAYHEAD_TESTFLIGHT_REF
  PLAYHEAD_TESTFLIGHT_KEEP_WORKTREE=1
  PLAYHEAD_TESTFLIGHT_SCHEME=Playhead
  PLAYHEAD_TESTFLIGHT_PROFILE_DIR="$HOME/Library/Developer/Xcode/UserData/Provisioning Profiles"
  PLAYHEAD_TESTFLIGHT_ARTIFACTS_DIR=/custom/output/dir

Notes:
  - This script does not modify your current checkout. It builds from a clean
    temporary worktree rooted at the requested ref.
  - To build the latest remote main, run `git fetch origin main` first and use
    `--ref origin/main`, or set PLAYHEAD_TESTFLIGHT_REF=origin/main.
  - Xcode must already be signed into the correct Apple account on this Mac,
    and the matching Apple Distribution cert + App Store profile must already
    be installed and usable by `codesign`.
EOF
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "error: missing required command: $cmd" >&2
    exit 1
  fi
}

find_profile_uuid() {
  local bundle_identifier="${TEAM_ID}.${BUNDLE_ID}"
  local profile_path

  while IFS= read -r profile_path; do
    [[ -n "$profile_path" ]] || continue

    local decoded_plist
    decoded_plist="$(mktemp "/tmp/playhead-profile.XXXXXX.plist")"

    if ! openssl smime \
      -inform DER \
      -verify \
      -noverify \
      -in "$profile_path" \
      -out "$decoded_plist" \
      >/dev/null 2>&1; then
      rm -f "$decoded_plist"
      continue
    fi

    local name app_id team uuid
    name="$(/usr/libexec/PlistBuddy -c 'Print :Name' "$decoded_plist" 2>/dev/null || true)"
    app_id="$(
      /usr/libexec/PlistBuddy \
        -c 'Print :Entitlements:application-identifier' \
        "$decoded_plist" \
        2>/dev/null || true
    )"
    team="$(/usr/libexec/PlistBuddy -c 'Print :TeamIdentifier:0' "$decoded_plist" 2>/dev/null || true)"
    uuid="$(/usr/libexec/PlistBuddy -c 'Print :UUID' "$decoded_plist" 2>/dev/null || true)"
    rm -f "$decoded_plist"

    if [[ "$name" == "$PROFILE_NAME" && "$app_id" == "$bundle_identifier" && "$team" == "$TEAM_ID" && -n "$uuid" ]]; then
      printf '%s\n' "$uuid"
      return 0
    fi
  done < <(/bin/ls -t "$PROFILE_DIR"/*.mobileprovision 2>/dev/null || true)

  return 1
}

REF="${PLAYHEAD_TESTFLIGHT_REF:-main}"
KEEP_WORKTREE="${PLAYHEAD_TESTFLIGHT_KEEP_WORKTREE:-0}"
SCHEME="${PLAYHEAD_TESTFLIGHT_SCHEME:-Playhead}"
# These four are required — fail fast (with a clear message) rather than
# silently shipping with a stale developer-specific default. See script
# header for the full list and what each value should be.
require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "error: required environment variable not set: $name" >&2
    echo "       see ./scripts/upload-testflight.sh --help for required env vars" >&2
    exit 1
  fi
}
require_env PLAYHEAD_TESTFLIGHT_TEAM_ID
require_env PLAYHEAD_TESTFLIGHT_BUNDLE_ID
require_env PLAYHEAD_TESTFLIGHT_PROFILE_NAME
require_env PLAYHEAD_TESTFLIGHT_CODE_SIGN_IDENTITY
TEAM_ID="$PLAYHEAD_TESTFLIGHT_TEAM_ID"
BUNDLE_ID="$PLAYHEAD_TESTFLIGHT_BUNDLE_ID"
PROFILE_NAME="$PLAYHEAD_TESTFLIGHT_PROFILE_NAME"
CODE_SIGN_IDENTITY="$PLAYHEAD_TESTFLIGHT_CODE_SIGN_IDENTITY"
PROFILE_DIR="${PLAYHEAD_TESTFLIGHT_PROFILE_DIR:-$HOME/Library/Developer/Xcode/UserData/Provisioning Profiles}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ref)
      [[ $# -ge 2 ]] || { echo "error: --ref requires a value" >&2; exit 1; }
      REF="$2"
      shift 2
      ;;
    --keep-worktree)
      KEEP_WORKTREE=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

require_cmd git
require_cmd xcodebuild
require_cmd xcodegen
require_cmd openssl
[[ -x /usr/libexec/PlistBuddy ]] || { echo "error: missing /usr/libexec/PlistBuddy" >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TIMESTAMP="$(date +%Y%m%d%H%M%S)"
WORKTREE_PARENT="$REPO_ROOT/.worktrees"
ARTIFACTS_DIR="${PLAYHEAD_TESTFLIGHT_ARTIFACTS_DIR:-$REPO_ROOT/build/testflight-$TIMESTAMP}"
WORKTREE_DIR=""

cleanup() {
  local status=$?
  if [[ -n "$WORKTREE_DIR" && -d "$WORKTREE_DIR" && "$KEEP_WORKTREE" != "1" ]]; then
    # CLAUDE.md forbids `git worktree remove --force` without explicit
    # user approval. If the unforced remove fails because the worktree
    # is dirty (uncommitted changes, locked, etc.), surface that so the
    # operator can inspect and decide rather than silently discarding
    # state. We deliberately do NOT auto-retry with -f.
    if ! git -C "$REPO_ROOT" worktree remove "$WORKTREE_DIR" >/dev/null 2>&1; then
      echo "warning: could not remove temporary worktree $WORKTREE_DIR" >&2
      echo "         it may be dirty or locked. Inspect it manually:" >&2
      echo "         git -C \"$REPO_ROOT\" status (within $WORKTREE_DIR)" >&2
      echo "         and remove it with \`git worktree remove\` once clean." >&2
    fi
  fi
  exit "$status"
}
trap cleanup EXIT

mkdir -p "$WORKTREE_PARENT" "$ARTIFACTS_DIR"

git -C "$REPO_ROOT" rev-parse --verify "${REF}^{commit}" >/dev/null

if [[ ! -d "$PROFILE_DIR" ]]; then
  echo "error: provisioning profile cache not found: $PROFILE_DIR" >&2
  exit 1
fi

PROFILE_UUID="$(find_profile_uuid || true)"
if [[ -z "$PROFILE_UUID" ]]; then
  echo "error: could not find a matching provisioning profile named '$PROFILE_NAME' for $TEAM_ID/$BUNDLE_ID under $PROFILE_DIR" >&2
  exit 1
fi

# `git worktree add` requires the destination to not exist (or be empty
# on git ≥2.36). Use `mktemp -u` so we get a unique path template without
# actually creating the directory; `git worktree add` then creates it.
WORKTREE_DIR="$(mktemp -u "$WORKTREE_PARENT/testflight.$TIMESTAMP.XXXXXX")"
git -C "$REPO_ROOT" worktree add --detach "$WORKTREE_DIR" "$REF" >/dev/null

ARCHIVE_PATH="$ARTIFACTS_DIR/Playhead.xcarchive"
UPLOAD_PATH="$ARTIFACTS_DIR/upload"
UPLOAD_OPTIONS_PLIST="$ARTIFACTS_DIR/UploadOptions.plist"

echo "Using git ref: $REF"
echo "Using provisioning profile: $PROFILE_NAME ($PROFILE_UUID)"
echo "Worktree: $WORKTREE_DIR"
echo "Artifacts: $ARTIFACTS_DIR"

cd "$WORKTREE_DIR"
xcodebuild -version
xcodegen generate --spec project.yml

plutil -create xml1 "$UPLOAD_OPTIONS_PLIST"
/usr/libexec/PlistBuddy -c 'Add :destination string upload' "$UPLOAD_OPTIONS_PLIST"
/usr/libexec/PlistBuddy -c 'Add :manageAppVersionAndBuildNumber bool true' "$UPLOAD_OPTIONS_PLIST"
/usr/libexec/PlistBuddy -c 'Add :method string app-store-connect' "$UPLOAD_OPTIONS_PLIST"
/usr/libexec/PlistBuddy -c 'Add :provisioningProfiles dict' "$UPLOAD_OPTIONS_PLIST"
/usr/libexec/PlistBuddy -c "Add :provisioningProfiles:$BUNDLE_ID string $PROFILE_NAME" "$UPLOAD_OPTIONS_PLIST"
/usr/libexec/PlistBuddy -c "Add :signingCertificate string $CODE_SIGN_IDENTITY" "$UPLOAD_OPTIONS_PLIST"
/usr/libexec/PlistBuddy -c 'Add :signingStyle string manual' "$UPLOAD_OPTIONS_PLIST"
/usr/libexec/PlistBuddy -c 'Add :stripSwiftSymbols bool true' "$UPLOAD_OPTIONS_PLIST"
/usr/libexec/PlistBuddy -c "Add :teamID string $TEAM_ID" "$UPLOAD_OPTIONS_PLIST"
/usr/libexec/PlistBuddy -c 'Add :uploadSymbols bool true' "$UPLOAD_OPTIONS_PLIST"

xcodebuild archive \
  -project Playhead.xcodeproj \
  -scheme "$SCHEME" \
  -configuration Release \
  -destination 'generic/platform=iOS' \
  -derivedDataPath "$ARTIFACTS_DIR/DerivedData" \
  -archivePath "$ARCHIVE_PATH" \
  CODE_SIGN_STYLE=Manual \
  "CODE_SIGN_IDENTITY=$CODE_SIGN_IDENTITY" \
  "DEVELOPMENT_TEAM=$TEAM_ID" \
  "PROVISIONING_PROFILE_SPECIFIER=$PROFILE_NAME" \
  "PROVISIONING_PROFILE=$PROFILE_UUID"

xcodebuild -exportArchive \
  -archivePath "$ARCHIVE_PATH" \
  -exportPath "$UPLOAD_PATH" \
  -exportOptionsPlist "$UPLOAD_OPTIONS_PLIST"

echo
echo "TestFlight upload submitted successfully."
echo "Archive: $ARCHIVE_PATH"
echo "Upload export path: $UPLOAD_PATH"
