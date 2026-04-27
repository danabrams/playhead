# scripts/

Helpers for the playhead-ym57 device-lab fixture substrate.

## `upload-testflight.sh`

Builds Playhead from a clean temporary git worktree rooted at `main` by
default, then uploads the archive to TestFlight using the local Xcode
account/signing state on the Mac.

This is the local-Mac fallback when GitHub Actions budget is exhausted. The
script does not touch your current checkout, so it is safe to run even when
your working tree is dirty.

```sh
# Build local `main` and upload to TestFlight
./scripts/upload-testflight.sh

# Build the latest fetched remote main instead
git fetch origin main
./scripts/upload-testflight.sh --ref origin/main
```

Prerequisites:

- `xcodegen` installed locally
- Xcode signed into the correct Apple account
- A usable `Apple Distribution` signing identity in the login keychain
- A matching `Playhead App Store` provisioning profile installed in Xcode's
  provisioning-profile cache

The script keeps its archive and upload metadata under `build/testflight-*`
and removes the temporary worktree automatically unless `--keep-worktree` or
`PLAYHEAD_TESTFLIGHT_KEEP_WORKTREE=1` is used.

## `download-fixtures.sh`

Verifies every fixture under `PlayheadTests/Fixtures/Corpus/Media/` against
`PlayheadTests/Fixtures/Corpus/fixtures-manifest.json` (SHA-256 per entry).
If anything is missing or mismatched, the script prints the download URL it
*would* fetch from the `fixtures-v<N>` GitHub Release tag.

```sh
# Verify + (stub) download
./scripts/download-fixtures.sh

# Verify only; never download
./scripts/download-fixtures.sh --verify-only
```

Exit codes:

| code | meaning |
|------|---------|
| 0    | all fixtures present and SHA-256 matches |
| 1    | unexpected script error (missing tools, unreadable manifest) |
| 2    | missing/mismatched AND `--verify-only` (so no download attempted) |
| 3    | missing/mismatched; STUB — would download if release existed |

The downloader is a stub until a real `fixtures-v1` GitHub Release is
published; once it exists, swap the `echo "would curl ..."` line for an
actual `curl -L -f` invocation.

## `rotate-fixtures.swift`

Picks the 4 rotating fixtures for the current release from the candidate
pool file (default: `scripts/rotation-pool.json`) using the seed in
`PlayheadTests/Fixtures/Corpus/fixtures-rotation-seed.txt`. The selection is
deterministic: same seed + same pool always produces the same 4 fixtures.

```sh
# Dry-run against the default pool (empty while licensing is pending)
swift scripts/rotate-fixtures.swift --dry-run

# Dry-run against a custom pool
swift scripts/rotate-fixtures.swift --dry-run --pool scripts/rotation-pool.json

# Write the picks into fixtures-manifest.json
swift scripts/rotate-fixtures.swift
```

When the pool is empty (the current default) the script prints a notice
explaining that rotation is blocked on licensing sign-off and exits 0
without modifying the manifest. See `FIXTURES_LICENSING.md` for the policy.

## `rotation-pool.json` (not yet checked in)

Candidate fixtures for rotation. Each candidate must satisfy the same
licensing gate as a locked-core fixture. Schema:

```json
{
  "candidates": [
    {
      "id": "fixture-...-15m-music-bed",
      "file": "Media/fixture-....wav",
      "sha256": "<64-char hex>",
      "durationSec": 900,
      "taxonomy": { "durationBucket": "15m", "chapterRichness": "sparse", "adDensity": "low", "adPlacement": "mid-roll", "language": "en-US", "audioStructure": "music-bed", "dynamicInsertion": false },
      "licensingRef": "LICENSING.md#fixture-...",
      "synthetic": false,
      "syntheticDurationSec": null
    }
  ]
}
```
