# Activity Pipeline Debug Strip — Design

**Date:** 2026-04-27
**Author:** dabrams
**Status:** approved, ready for implementation plan

## Goal

Surface per-episode pipeline progress (download / transcription / analysis) on
the Activity screen for debugging. If an episode is not on Activity, it is
considered done — Activity is the single home for in-flight work, so it is the
single home for the debug strip.

## Where it appears

Three of Activity's four sections render the strip:

- **Now** — under the existing `progressPhrase`.
- **Up Next** — under the title.
- **Paused** — under the reason · hint line.

**Recently Finished** does not render the strip — those rows are by definition
done. (`ActivityRecentlyFinishedRow` does not carry the fields.)

## What it renders

A monospaced one-liner:

```
DL 100% · TX 87% · AN 64%
```

- Font: `AppTypography.mono(size: 11, weight: .regular)` — one tick smaller
  than the row's existing mono-12 so it visually subordinates.
- Color: `AppColors.textSecondary`.
- Format per slot: `String(format: "%3d%%", Int((fraction * 100).rounded()))`
  for known fractions; `--%` when nil.
- Slots: `DL` (download), `TX` (transcription), `AN` (analysis).
- Padded `top: 2`, no horizontal padding (column-aligned with the row's text).
- Accessibility identifier: `ActivityView.<section>.pipelineStrip`
  (e.g. `ActivityView.now.pipelineStrip`).

A reusable `PipelineProgressStripView` owns the formatting. The three row views
(`NowRowView`, `UpNextRowView`, `PausedRowView`) embed it via
`if showPipelineStrip { PipelineProgressStripView(...) }` so it's a no-op when
off.

## Gating

Runtime, on any build, off by default.

- `@AppStorage("debug.showPipelineStrip")` boolean, default `false`.
- New row in `SettingsView`:
  `Toggle("Show pipeline progress on Activity", isOn: $showPipelineStrip)`.
- Toggle row visible in Release builds (matches the user's "works on any build"
  ask). Easy to wrap in `#if DEBUG` later.
- `ActivityView` reads the same `@AppStorage` value and skips the strip when
  false.

## Data model

Add three optional `Double?` fields to `ActivityEpisodeInput`:

- `downloadFraction: Double?` — `0.0...1.0`, byte-level. `nil` if no download
  is recorded for this episode this refresh (caller distinguishes
  never-downloaded vs. complete via the row's existing `isDownloaded` flag).
- `transcriptFraction: Double?` —
  `fastTranscriptCoverageEndTime / episodeDurationSec`, clamped to `0.0...1.0`.
  `nil` if either watermark or duration is unknown or duration is `<= 0`.
- `analysisFraction: Double?` —
  `confirmedAdCoverageEndTime / episodeDurationSec`, clamped. Same nil rules.

The same three optionals propagate into `ActivityNowRow`, `ActivityUpNextRow`,
and `ActivityPausedRow`. `ActivityRecentlyFinishedRow` does not carry them.

## Provider population

`ActivitySnapshotProvider` is the single source of these fractions per refresh.

- **AnalysisStore tap (already wired):** read `fastTranscriptCoverageEndTime`,
  `confirmedAdCoverageEndTime`, and `episodeDurationSec` from the
  `AnalysisAsset` row the provider already loads. Compute fractions at
  construction; no new query, no new round trip.
- **Download tap (new, narrow):** add
  `func progressSnapshot() async -> [String: Double]` to `DownloadManager`,
  returning `episodeId → fractionCompleted` for currently-active downloads
  (drained from the same `ForegroundAssistProgress` map that already drives
  `progressUpdates()`). Provider awaits it once per refresh and looks up by
  episode ID.

This matches the existing snapshot-per-refresh shape of Activity: the screen
re-aggregates on `ActivityRefreshNotification`, not via continuous streams.

## View flow

Production-shaped path so that a future polished UI can ride the same wires:

```
AnalysisStore + DownloadManager
        ↓
ActivitySnapshotProvider (tap both, compute fractions)
        ↓
ActivityEpisodeInput (carries 3 optional Double fields)
        ↓
ActivityViewModel.refresh(from:) (passes through unchanged)
        ↓
ActivityNowRow / ActivityUpNextRow / ActivityPausedRow
        ↓
NowRowView / UpNextRowView / PausedRowView
        ↓ (only if @AppStorage flag is true)
PipelineProgressStripView
```

## Tests

- **ActivitySnapshotProviderTests** — new cases:
  - watermarks + duration present → fractions computed, clamped to `0...1`;
  - duration missing or `<= 0` → fractions are `nil`;
  - download fraction plumbed through from a stub
    `DownloadManager.progressSnapshot()`;
  - episode not in download snapshot → `downloadFraction` is `nil`.
- **ActivityViewModelTests** — pin that the three optionals ride into the
  right row struct (Now/UpNext/Paused) and are absent on Recently Finished.
- **PipelineProgressStripViewTests** (new, lightweight) — formatter pins:
  - `0.0` → `0%`;
  - `0.876` → `88%`;
  - `nil` → `--%`;
  - `1.05` (overflow) → `100%`;
  - `-0.1` (underflow) → `0%`.

No view-snapshot tests for row composition. Activity rows aren't snapshot-
tested today and we will not grow that surface for a debug-only feature.

## Non-goals

- No bars, no animation, no sparklines. One mono line, three numbers.
- No Recently Finished strip.
- No reading from a separate "debug data" path. Production data flow is the
  only path.
- No streaming subscriptions. Snapshot per refresh.

## Open questions

None blocking. One trivial follow-up later: decide whether to wrap the
Settings toggle row in `#if DEBUG` once we ship a TestFlight pass that uses
it.
