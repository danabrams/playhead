# GitHub Repo And CI Setup

This repo is set up to use `gh` for repo creation plus GitHub Actions for:

- CI on `main` pushes, pull requests, and manual dispatch
- Manual TestFlight deployment from GitHub Actions

## 1. Re-authenticate `gh`

The current `gh` login on this machine is invalid, so start by logging in again:

```bash
gh auth logout -h github.com -u danabrams
gh auth login -h github.com --web --git-protocol ssh
gh auth status
```

If you prefer HTTPS remotes instead of SSH, switch `--git-protocol ssh` to `--git-protocol https`.

## 2. Create The Private GitHub Repo

From the repo root:

```bash
gh repo create playhead --private --source=. --remote=origin --push --description "AI-powered podcast player for iOS"
```

If you want the repo under an org instead of your personal account:

```bash
gh repo create YOUR_ORG/playhead --private --source=. --remote=origin --push --description "AI-powered podcast player for iOS"
```

## 3. CI Workflow

The CI workflow lives at `.github/workflows/ios-ci.yml`.

It:

- installs `xcodegen`
- generates `Playhead.xcodeproj` from `project.yml`
- builds the app for an iOS simulator
- runs unit and integration tests through the `Playhead` scheme
- uploads Xcode test artifacts

## 4. TestFlight Secrets

The deploy workflow lives at `.github/workflows/testflight.yml`.

Set these GitHub Actions secrets before running it:

```bash
gh secret set APPSTORE_CERTIFICATES_FILE_BASE64 --body "$(base64 -i ios_distribution.p12)"
gh secret set APPSTORE_CERTIFICATES_PASSWORD
gh secret set APPSTORE_API_PRIVATE_KEY < AuthKey_ABC123XYZ.p8
gh secret set APPSTORE_KEY_ID --body "ABC123XYZ"
gh secret set APPSTORE_ISSUER_ID --body "00000000-0000-0000-0000-000000000000"
```

Those values map to:

- `APPSTORE_CERTIFICATES_FILE_BASE64`: base64-encoded `.p12` export of your Apple Distribution certificate
- `APPSTORE_CERTIFICATES_PASSWORD`: password used when exporting that `.p12`
- `APPSTORE_API_PRIVATE_KEY`: contents of the App Store Connect `.p8` key
- `APPSTORE_KEY_ID`: App Store Connect API key ID
- `APPSTORE_ISSUER_ID`: App Store Connect issuer ID

## 5. Run A TestFlight Upload

After the secrets are present:

```bash
gh workflow run testflight.yml
gh run list --workflow testflight.yml
gh run watch
```

The workflow will:

- import the signing certificate
- download the App Store provisioning profile for `com.playhead.app`
- archive the app
- export an `.ipa`
- upload it to TestFlight

## 6. Local Mac Mini Fallback

If GitHub Actions minutes or budget are exhausted, the repo also ships a
local upload script that uses the Mac's logged-in Xcode account instead of
GitHub secrets:

```bash
./scripts/upload-testflight.sh
```

By default it builds from a clean temporary worktree at local `main`, archives
the app, and uploads directly to TestFlight. It leaves artifacts under
`build/testflight-*`.

To build the latest fetched remote main instead:

```bash
git fetch origin main
./scripts/upload-testflight.sh --ref origin/main
```

Prerequisites:

- `xcodegen` installed locally
- Xcode signed into the correct Apple account on the Mac
- a working `Apple Distribution` certificate/private key in the login keychain
- a matching `Playhead App Store` provisioning profile installed locally

## Notes

- The workflows assume bundle ID `com.playhead.app`.
- The Apple team ID is currently hard-coded to `36Z6VYTT9X` in the deploy workflow.
- CI uses `macos-15`.
- TestFlight deployment is manual by default. If you want automatic uploads on `main`, add a `push` trigger to `.github/workflows/testflight.yml`.
