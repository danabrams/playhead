# Pre-Analysis at Download Time

**Date:** 2026-04-03
**Status:** Approved
**Problem:** Pre-roll ads (e.g., Conan O'Brien Needs a Friend) play for 5-10 seconds before the analysis pipeline can detect and skip them, because analysis only starts at play-time.

## Decision

Wire download completion into the analysis pipeline so that the first N seconds of audio are pre-analyzed before the user hits play. When playback starts, skip cues for pre-roll ads are available immediately.

## Tier Model

| Tier | Depth | Trigger | Battery Requirement |
|------|-------|---------|-------------------|
| T0 | 90s | Download completes (explicit or auto) | None -- always runs |
| T1 | 3min | After T0 completes | Charging |
| T2 | 5min | After T1 completes | Charging |
| T3 | 10min | After T2 completes | Charging |

Progressive: T0 runs immediately, T1-T3 advance only while charging. If charger is removed mid-tier, work pauses and resumes on next charge. Coverage achieved is never lost.

For auto-downloads, same flow applies -- T0 always runs, T1+ only while charging.

## Approach: Option A -- AnalysisCoordinator with PreAnalysisScheduler

Extend `AnalysisCoordinator` with a new `preAnalyze()` entry point. Add a `PreAnalysisScheduler` actor to own tier progression, charging observation, and work queue management.

### AnalysisCoordinator Changes

New method:

```swift
func preAnalyze(
    episodeId: String,
    podcastId: String?,
    audioURL: URL,
    coverageTarget: TimeInterval
) async -> String?
```

Differences from playback-driven analysis:
- **Synthetic PlaybackSnapshot**: `playheadTime: 0, playbackRate: 1.0, isPlaying: false` -- processes shards linearly from start.
- **Coverage cap**: Pipeline stages stop when watermark reaches `coverageTarget`.
- **No skip cue pushing**: Writes AdWindows to SQLite only. SkipOrchestrator loads them at play-start.
- **Lower QoS**: Dispatches at `.utility` priority.
- **Yields to playback**: Cancelled if user starts playing any episode. Playback pipeline picks up from achieved coverage.

Session state reuse: walks `queued -> spooling -> featuresReady -> hotPathReady`, stops there. Backfill deferred to playback or background task.

### PreAnalysisScheduler

New actor. Sits between `DownloadManager` and `AnalysisCoordinator`.

Responsibilities:
- Tracks per-episode pre-analysis progress (current tier, coverage achieved)
- Observes charging state via `CapabilitiesService`
- Manages FIFO work queue (explicit downloads get priority)
- Cancels pre-analysis when playback starts (coordinator takes over)
- One episode at a time to limit CPU/thermal impact

Persisted state:

```
PreAnalysisRecord {
    episodeId: String
    podcastId: String?
    audioURL: String
    isExplicitDownload: Bool
    currentTier: Int          // 0-3
    coverageAchieved: Double  // seconds
    status: pending | active | paused | complete | superseded
}
```

Tier advancement:
- T0 complete + charging -> start T1 immediately
- T0 complete + not charging -> pause, observe `CapabilitiesService` for charging
- Charging detected -> resume oldest paused episode at next tier
- Charging lost mid-tier -> cancel, keep coverage achieved
- Playback starts -> mark `superseded`, cancel if in-flight

### DownloadManager Integration

Add `DownloadContext` parameter to download methods:

```swift
struct DownloadContext: Sendable {
    let podcastId: String?
    let isExplicitDownload: Bool
}
```

After `performDownload()` completes:

```swift
await preAnalysisScheduler.enqueue(
    episodeId: episodeId,
    podcastId: podcastId,
    audioURL: completeURL,
    isExplicitDownload: isExplicitDownload
)
```

Background download delegate path: scheduler checks for un-enqueued completed downloads at startup via SQLite join. Also handles upgrade path for existing users.

### Playback Handoff

1. `resolveSession()` finds existing session with coverage watermarks set
2. Session state is `hotPathReady` -- coordinator resumes from there
3. Hot path sees `featureCoverageEndTime` / `fastTranscriptCoverageEndTime` and starts where pre-analysis left off
4. `PreAnalysisScheduler` marks episode `superseded`

### SkipOrchestrator Change

In `beginEpisode()`, load existing AdWindows from SQLite:

```swift
let existingWindows = try await store.fetchAdWindows(assetId: analysisAssetId)
if !existingWindows.isEmpty {
    await receiveAdWindows(existingWindows)
}
```

Pre-roll skip cue ready before first audio sample plays.

## Error Handling

- **Pre-analysis fails mid-tier**: Coverage watermarks reflect completed work. Retried on next charge or app launch.
- **Disk eviction deletes audio**: SQLite analysis results survive. Re-download finds existing asset via fingerprint.
- **Whisper model not downloaded**: Features-only pre-analysis (acoustic signals = 60% classifier weight). Transcription runs when model arrives.
- **Thermal throttle**: Scheduler pauses, resumes when thermal state drops.
- **Multiple downloads**: FIFO queue, one at a time, explicit downloads jump to front.
- **Episode deleted during pre-analysis**: Scheduler cancels work, removes PreAnalysisRecord.
