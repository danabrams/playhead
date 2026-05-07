# Dogfood fixture — 2026-05-06 (playhead-hygc.1.1)

This directory contains a **sanitized** structural snapshot of the May 6, 2026
dogfood capture. The raw evidence (a `.xcappdata` bundle plus a dogfood
diagnostics JSON) lives only on the developer's local machine and **MUST NOT
be checked in**. Downstream beads `playhead-hygc.1.2..1.9` assert against this
fixture; if you need to regenerate it from a fresh capture, see "Regenerating
the fixture" below.

## What ships here

| File | What it is |
| --- | --- |
| `analysis-health.json` | Scrubbed snapshot — Activity rows + asset coverage + chunk maxima + ad-window summaries + correction duplicates + bg-task event counts + learning-table counts + shadow FM response count |
| `README.md` | This file |

The Swift loader is one directory up at
`PlayheadTests/Fixtures/Dogfood/DogfoodAnalysisHealthFixtureLoader.swift` and
the structural-fact tests are at
`PlayheadTests/Fixtures/Dogfood/DogfoodAnalysisHealthFixtureTests.swift`.

## Source files used (NOT committed)

| Source path | Used for |
| --- | --- |
| `playhead-dogfood-diagnostics-2026-05-06T23-46-51Z.json` | `activity_snapshot` rows: section / status / is_running / queue_position / cached_audio_present + pipeline percentage fields |
| `*.xcappdata/AppData/Documents/ExportedAnalysisStore/analysis.sqlite` | `analysis_assets`, `transcript_chunks` maxima, `ad_windows` counts, `correction_events` duplicates, learning-table counts, `shadow_fm_responses` count |
| `*.xcappdata/AppData/Documents/bg-task-log.jsonl` | Background-task event counts overall and by category |

The build script does NOT read `asset-lifecycle-log.jsonl` or
`shadow-decisions.jsonl` — the shadow FM response count is sourced from the
`shadow_fm_responses` SQL table, and lifecycle events are not part of the
fixture's load-bearing surface.

## Fields retained

- **Top-level metadata** — `schema_version` (currently `1`; bump on incompatible field shape changes), `captured_on` (the `YYYY-MM-DD` capture date — load-bearing for downstream beads that pin to the May 6 baseline), `source_diagnostics_filename` (the diagnostics export filename — purely informational, no path or device identifier).
- **Activity rows** — `section`, `status.{disposition, reason, hint, playback_readiness}`, `analysis_state`, `is_running`, `queue_position`, `cached_audio_present`, and `pipeline.*` numerics (download/analysis/transcript fractions, percents, watermarks, episode duration) plus the source enums `pipeline.download_source` (e.g. `cached_audio`), `pipeline.analysis_source`, `pipeline.transcript_source`. The `download_source == cached_audio` reading is what proves the wedged state — every row already has the bytes locally, so the work-pump is starved waiting on something else.
- **Analysis assets** — `analysis_state`, `episode_duration_sec`, `fast_transcript_coverage_end_sec`, `feature_coverage_end_sec`, `final_pass_coverage_end_sec`, `confirmed_ad_coverage_end_sec`, `terminal_reason`.
- **Transcript chunk maxima** — per-(asset, pass) `MAX(endTime)` and `COUNT(*)`. No transcript text.
- **Ad window summaries** — per-asset total count, `userMarked` count, algorithmic count, max endTime. No advertiser / product / evidence text.
- **Correction rows** — `correction_type` + `scope` + duplicate `count`. Only asset-bound scopes (`exactSpan:<assetId>:…` and `exactTimeSpan:<assetId>:…`) are retained, and their `<assetId>` UUID is rewritten to the synthetic id. Non-asset-bound scopes (`sponsorOnShow`, `phraseOnShow`, `campaignOnShow`, `domainOwnershipOnShow`, `jingleOnShow`) are DROPPED at scrub time — they embed sponsor / phrase / podcastId text with no synthetic mapping in this fixture, and the build script logs a per-prefix dropped-row count to stderr when this happens. The May 6 capture has zero non-asset-bound rows.
- **Background-task events** — overall lifecycle event counts (`submit`, `start`, `complete`, `expire`, `appPhase`) and per-category counts for the registered task identifiers that fired during the capture (`com.playhead.app.analysis.backfill`, `com.playhead.app.feed-refresh`, `com.playhead.app.preanalysis.recovery` — `analysis.continued` is registered but had no events on May 6).
- **Learning-table counts** — row count per table (`sponsor_knowledge_entries`, `training_examples`, `ad_copy_fingerprints`, `boundary_priors`, `implicit_feedback_events`, `knowledge_candidate_events`, `music_bracket_trust`).
- **Shadow FM responses** — count only.

## Fields redacted

Anything that could identify a podcast, episode, user, install, or device is
stripped. Specifically:

- **Identifiers** — `analysisAssetId` UUIDs are replaced with stable synthetic
  `asset_NNN` ids (1-indexed, ordered by `createdAt`). Activity-row
  `episode_id_hash` SHA-256s are dropped entirely and replaced with positional
  `activity_NNN` ids.
- **URLs** — feed URLs (`https://feeds.simplecast.com/...`,
  `https://rss2.flightcast.com/...`, etc.), source URLs, audio paths.
- **Free-form text** — transcript chunk `text` / `normalizedText`, ad-window
  `advertiser` / `product` / `adDescription` / `evidenceText`, episode titles,
  prompt text, FM response `fmResponseBase64` payloads, surface-status
  `session_id`s.
- **Device-specific paths** — `/var/mobile/Containers/Data/Application/<UUID>/`,
  `BuildProvenance.plist`, install salts, hardware identifiers.
- **Timestamps** — wall-clock `createdAt`, `started_at`, `updated_at`,
  `capturedAt`. (We keep durations and watermark seconds — those are
  load-bearing for downstream assertions and carry no identifying signal.)

## Cross-references

Synthetic IDs preserve cross-row relationships within the fixture:

- A `correction_rows[*].scope` of `exactTimeSpan:asset_012:3386.000:3394.140`
  references the same logical asset as `analysis_assets[?].id == "asset_012"`.
- `transcript_chunk_maxima[*].asset_id` and `ad_window_summaries[*].asset_id`
  use the same synthetic id space as `analysis_assets[*].id`.
- The Activity-snapshot id space (`activity_NNN`) is **independent** from the
  analysis-asset id space (`asset_NNN`) — the dogfood capture had 22 Activity
  rows but only 19 analysis-assets rows, and their join key (`episode_id_hash`
  vs `analysisAssetId`) was scrubbed.

## Notable structural facts (asserted by tests)

These are the load-bearing observations the fixture proves out for downstream
beads — see `DogfoodAnalysisHealthFixtureTests.swift` for the actual
assertions.

1. **All 22 Activity rows are queued / up_next / waiting_for_time and not
   running**, with cached audio present and download progress at 100%. The
   work-pump is wedged: there's nothing left to download but nothing's
   moving.
2. **`asset_004`** (the dogfood UUID `9C109975-A4B9-4C87-AE62-7BAFF35CAE24`)
   is in `completeFull` but has chunk transcript coverage ≈3960 s (~66 min)
   while its stored `fastTranscriptCoverageEndTime` watermark is only 90 s
   (~1.5 min). The chunks exist but the watermark was never advanced.
3. **At least 6 `completeFull` assets carry contradictory state** — either
   their `terminal_reason` claims a coverage ratio > 1.0
   (e.g. `"transcript 1.163, feature 1.724"`) or their fast watermark is
   < 50% of the episode duration despite the asset being marked complete.
4. **Two `falseNegative` correction scopes appear 4 times each** in the
   `correction_events` table — duplicate-key structure is preserved so
   downstream beads can exercise their dedup logic.
5. **All seven learning tables are empty (0 rows)** while the shadow
   classifier wrote 1321 FM responses — the FM stack ran but nothing of
   what it wrote was promoted into a durable artifact.

## Regenerating the fixture

When a new dogfood capture lands, regenerate the fixture **outside this
worktree** so the raw evidence never appears in the diff:

```bash
# 1. Place the raw capture next to (or anywhere outside) the repo. Defaults:
#      $HOME/playhead/playhead-dogfood-diagnostics-<...>.json
#      $HOME/playhead/com.playhead.app <ts>.xcappdata/AppData/Documents/{...}
# 2. Run the build script. Use env vars to point at non-default locations.

PLAYHEAD_HYGC_DIAG="/path/to/playhead-dogfood-diagnostics-...json" \
PLAYHEAD_HYGC_XCAPPDATA="/path/to/com.playhead.app .xcappdata" \
  python3 scripts/build-dogfood-fixture-2026-05-06.py
```

The script writes
`PlayheadTests/Fixtures/Dogfood/2026-05-06/analysis-health.json` (overwriting
the prior file). The script also accepts an optional positional `<out_path>`
argument (`python3 scripts/build-dogfood-fixture-2026-05-06.py /tmp/out.json`)
which is useful for diff-checking a regen against the committed file before
overwriting it. Exit codes: `0` on success, `2` when a required raw input is
missing (preflight failure), `3` when the scrub-audit gate refused to write
because the serialized fixture contained a forbidden token / UUID / SHA-256
shape. Re-run the fixture-loader test suite — if any structural fact
changed, update the corresponding assertion in
`DogfoodAnalysisHealthFixtureTests.swift` together with this README.

When capturing a fundamentally new dogfood run (different date), prefer
copying the script under a new date stamp (`scripts/build-dogfood-fixture-<DATE>.py`)
and creating a sibling fixture directory rather than overwriting the
2026-05-06 baseline — downstream beads pin against this date deliberately.

## What this fixture deliberately does NOT include

- **FrozenTrace rows** (`PlayheadTests/Fixtures/NarlEval/2026-05-06/`) — the
  `NarlEvalCorpusBuilderTests.buildFixtures()` env-gated test already produces
  per-asset `FrozenTrace-<id>.json` files from the same `.xcappdata` bundle
  when run with `PLAYHEAD_BUILD_NARL_FIXTURES=1`. We deliberately do not
  duplicate that path here — running the corpus builder against the May 6
  capture is left to the consumer beads (hygc.1.2..1.9) since the FrozenTrace
  contract spans the entire `decision-log.jsonl` (1.6 GB+ in the May 6
  capture) and we don't want to pre-bake a partial reproduction.
- **Per-row pipeline source / sub-stage strings** beyond what's already
  retained — the fixture captures the load-bearing
  `analysis_source` / `transcript_source` strings ("feature_coverage",
  "fast_transcript_chunks") but not every internal substate name.
- **Decision-log evidence rows** — the dogfood capture has multiple gigabytes
  of `decision-log.*.jsonl`. Downstream beads that need decision-by-decision
  evidence should generate their own FrozenTrace via the env-gated builder.
