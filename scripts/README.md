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

## `false_ready_rate.swift`

Summarizes `surface-status-*.jsonl` dogfood logs and the single-file JSON
archive produced by Settings > Diagnostics > Export dogfood logs.

```sh
swift scripts/false_ready_rate.swift scripts/fixtures/false_ready_rate_sample.jsonl
swift scripts/false_ready_rate.swift path/to/playhead-dogfood-diagnostics-2026-05-06T22-24-59Z.json
swift scripts/false_ready_rate.swift path/to/Diagnostics --audit
```

The strict rollup preserves the original false-ready metric. The trigger
rollup separates `analysis_completed` from `cold_start` so initial observed
ready states do not contaminate the dogfood gate candidate.

`--audit` adds a session-aware dogfood classification. It uses each unique
`(session_id, episode_id_hash)` pair with `auto_skip_fired` as the playback
proxy, separates skip-after-ready from skip-before-ready ordering, marks
ready rows without same-session playback evidence as unscored, and prints a
`DOGFOOD_GATE_AUDIT` conclusion. The default minimum is 20 playback proxies;
override with `--min-playback-proxies N` for fixtures.

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

## `l2f-local-transcribe.swift`

Runs local `whisper.cpp` against `TestFixtures/Corpus/Audio/` and writes
timestamped transcript JSON to `TestFixtures/Corpus/Transcripts/`.

```sh
# Install the local ASR binary once.
brew install whisper-cpp

# Download a GGML model separately into ./models, then transcribe local corpus audio.
mkdir -p models
curl -L --fail --output models/ggml-base.en.bin \\
  https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-base.en.bin

swift scripts/l2f-local-transcribe.swift --model models/ggml-base.en.bin

# Use CPU fallback if Metal is unavailable or fails to allocate.
swift scripts/l2f-local-transcribe.swift --model models/ggml-base.en.bin --no-gpu

# Preview the exact commands without requiring a model file.
swift scripts/l2f-local-transcribe.swift --dry-run --model models/ggml-base.en.bin
```

`whisper-cpp` supports `flac`, `mp3`, `ogg`, and `wav` directly. The script
uses `ffmpeg` to convert corpus formats such as `m4a`, `mp4`, and `aac` to
temporary 16 kHz mono WAV before transcription.

## `l2f-draft-annotation.swift`

Reads local timestamped transcripts and emits review-only L2F annotation
drafts to `TestFixtures/Corpus/Drafts/`. It computes audio fingerprints,
clusters transcript cue hits into pod-scale candidate ad windows, builds
content windows as the complement of heuristic ad candidates, and writes a
Markdown review sheet with the matched transcript segments.

```sh
swift scripts/l2f-draft-annotation.swift

# Smoke-test the parser and heuristic without audio.
swift scripts/l2f-draft-annotation.swift \\
  --allow-missing-audio \\
  --duration 150 \\
  --transcript-dir scripts/fixtures \\
  scripts/fixtures/l2f_transcript_sample.json

# Exercise the synthetic edge fixtures: zero-ad, article-level sponsor-word
# false positive, back-to-back ads, and a multi-CTA pod.
bash scripts/test-l2f-draft-annotation.sh

# Generate drafts plus a local audio review queue.
swift scripts/l2f-draft-annotation.swift --write-review-queue

# Build only the review queue from Codex's transcript review for the current
# 15 local episodes. This writes review-queue.json/md under Drafts.
swift scripts/l2f-draft-annotation.swift \\
  --review-queue-only \\
  --review-source TestFixtures/Corpus/Drafts/codex-transcript-review.json
```

Useful tuning flags are `--merge-gap-seconds`, `--expand-before-seconds`,
`--expand-after-seconds`, `--padding-seconds`, `--max-window-seconds`, and
`--review-context-seconds`; run `swift scripts/l2f-draft-annotation.swift
--help` for defaults.

Drafts and review queues are not corpus truth. Promotion for
`playhead-l2f.3`/`.4` remains gated on human local-audio review: check
boundaries to `+/-0.5s`, reject false positives and zero-ad traps, fill
advertiser/product when identifiable, and only then promote with
`l2f-promote-reviewed-corpus.py`.

## `l2f-review-gui.py`

Serves a local browser GUI for the `playhead-l2f.3` manual audio review pass.
It reads the ignored review queue, serves local corpus audio with seeking, and
saves review decisions back under `TestFixtures/Corpus/Drafts/`.

```sh
# From the Mac only.
python3 scripts/l2f-review-gui.py

# From an iPhone or another device on the same network.
python3 scripts/l2f-review-gui.py --host 0.0.0.0
```

The script prints both the local URL and, when bound to `0.0.0.0`, a LAN URL
you can open from the iPhone. Saved decisions go to
`TestFixtures/Corpus/Drafts/l2f-audio-review.json`; the "Write episode review
files" button also emits per-episode `*.audio-review.json` files in Drafts.

## `l2f-promote-reviewed-corpus.py`

Reports review debt and promotes fully reviewed GUI decisions into committed
corpus annotation JSON. The default mode is dry-run/report-only and is safe
before the manual listening pass is complete:

```sh
python3 scripts/l2f-promote-reviewed-corpus.py
```

The command reads `TestFixtures/Corpus/Drafts/l2f-audio-review.json` and the
`queue_path` saved by the GUI. If the review file does not exist yet, it can
still report against an explicit queue:

```sh
python3 scripts/l2f-promote-reviewed-corpus.py \
  --queue TestFixtures/Corpus/Drafts/codex-review-queue.json
```

After the iPhone GUI review is complete, run strict promotion:

```sh
python3 scripts/l2f-promote-reviewed-corpus.py --promote
```

Real promotion refuses to write annotations when any selected entry is
unreviewed, marked `unsure`, missing required corpus metadata such as
`show_name` or ad metadata, missing local audio, has invalid or overlapping
timing, or lacks a determinable episode duration. Use `--episode <episode_id>`
to promote a reviewed subset and `--force` only when intentionally replacing an
existing annotation. The promoter writes only
`TestFixtures/Corpus/Annotations/<episode_id>.json`; `Drafts/`, `Audio/`, and
`Transcripts/` remain local/uncommitted artifacts.
No-ad annotations require an explicitly reviewed false-positive trap entry;
rejecting an ordinary candidate as `false_positive` does not by itself prove the
whole episode is ad-free.

Focused smoke tests:

```sh
bash scripts/test-l2f-promote-reviewed-corpus.sh
```

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
