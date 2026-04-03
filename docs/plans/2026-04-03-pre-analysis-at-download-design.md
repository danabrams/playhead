# Pre-Analysis at Download Time

**Date:** 2026-04-03
**Status:** Approved — v2 after review
**Problem:** Pre-roll ads (e.g., Conan O'Brien Needs a Friend) play for 5-10 seconds before the analysis pipeline can detect and skip them, because analysis only starts at play-time.

## Decision

Wire download completion into the analysis pipeline so that the first N seconds of audio are pre-analyzed before the user hits play. When playback starts, skip cues for pre-roll ads are available immediately.

## Tier Model

| Tier | Depth | Trigger | Battery Requirement |
|------|-------|---------|-------------------|
| T0 | Policy target (default 90s; podcast-profile adjusted 60-180s) | Download completes / Up Next / explicit play intent | None -- always runs |
| T1 | Extend to profile target A | After T0 or when model becomes available | Charging |
| T2 | Extend to profile target B | After T1 | Charging |
| T3 | Opportunistic deep warmup | After T2 | Charging |

Progressive: T0 runs immediately, T1-T3 advance only while charging. If charger is removed mid-tier, work pauses and resumes on next charge. Coverage achieved is never lost.

T0 depth is policy-driven: default 90s, raised for podcasts with historically long pre-roll ads (using `PodcastProfile.normalizedAdSlotPriors`), lowered for clean-feed shows. Explicit downloads and Up Next episodes are prioritized above auto-downloads.

For auto-downloads, same flow applies -- T0 always runs, T1+ only while charging.

## Approach: AnalysisJobRunner + AnalysisWorkScheduler

Keep `AnalysisCoordinator` focused on live playback orchestration. Add two new actors:

- **`AnalysisJobRunner`** — Reusable bounded-range analysis engine. Runs the pipeline (spool -> features -> transcript -> detection) over a `Range<TimeInterval>`, writing durable stage frontiers. Used by pre-analysis, playback hot-path, and future jobs like model-upgrade backfill.
- **`AnalysisWorkScheduler`** — Policy, constraints, and queueing. Selects the highest-priority eligible job under current constraints, manages lease-based ownership, and handles tier advancement.

```swift
struct AnalysisRangeRequest: Sendable {
    enum Mode: String, Sendable {
        case preRollWarmup
        case playback
        case backgroundBackfill
    }
    let jobId: String
    let episodeId: String
    let podcastId: String?
    let analysisAssetId: String?
    let audioURL: URL
    let range: Range<TimeInterval>
    let mode: Mode
    let outputPolicy: OutputPolicy
    let priority: TaskPriority
}

enum OutputPolicy: Sendable {
    /// Write AdWindows + compile SkipCues. Used for pre-analysis.
    case writeWindowsAndCues
    /// Write AdWindows + push live skip cues. Used for playback.
    case writeWindowsAndPushLive
    /// Write AdWindows only. Used for backfill.
    case writeWindowsOnly
}
```

### AnalysisJobRunner

Reusable core for bounded-range analysis. Does not know about playback state, tier policy, or scheduling. Receives a range request, runs the pipeline, writes durable stage frontiers, returns a structured outcome.

```swift
struct AnalysisOutcome: Sendable {
    enum StopReason: Sendable {
        case reachedTarget
        case cancelledByPlayback
        case pausedForThermal
        case blockedByModel
        case failed(String)
    }
    let assetId: String
    let requestedCoverageSec: Double
    let featureCoverageSec: Double
    let transcriptCoverageSec: Double
    let cueCoverageSec: Double
    let newCueCount: Int
    let stopReason: StopReason
}
```

Stage frontiers (feature, transcript, cue-ready) are persisted to SQLite after each stage completes. Tier advancement keys off `cueCoverageSec`, not asset existence or transcript coverage alone.

### AnalysisWorkScheduler

Replaces the v1 `PreAnalysisScheduler`. Eligibility-aware, not FIFO.

**Two logical lanes:**
- **Immediate lane**: T0 / explicit downloads / Up Next episodes. Always eligible.
- **Deferred lane**: T1+ charging-only extension work. Eligible only when charging and thermal is nominal/fair.

Selects the highest-priority eligible job under current constraints. A paused T1 record never blocks an eligible T0 job behind it. Supports preemption at tier boundaries when a new explicit or Up Next episode arrives.

One CPU-heavy job at a time to limit thermal impact.

### Versioned Job Model (replaces PreAnalysisRecord)

```swift
struct AnalysisJob: Sendable {
    let jobId: String               // UUID
    let jobType: String             // "preRollWarmup"
    let episodeId: String
    let podcastId: String?
    let analysisAssetId: String?
    let workKey: String             // fingerprint|analysisVersion|modelSet|jobType
    let sourceFingerprint: String
    let downloadId: String
    let priority: Int               // Higher = more important
    let desiredCoverageSec: Double
    let featureCoverageSec: Double
    let transcriptCoverageSec: Double
    let cueCoverageSec: Double
    let state: String               // queued|running|blocked|paused|complete|superseded|failed
    let attemptCount: Int
    let nextEligibleAt: Double?     // Unix timestamp; nil = eligible now
    let leaseOwner: String?         // "preAnalysis" | "playback" | nil
    let leaseExpiresAt: Double?     // Unix timestamp
    let lastErrorCode: String?
    let createdAt: Double
    let updatedAt: Double
}
```

**Durable key:** `workKey` is derived from `sourceFingerprint + analysisVersion + modelSet + jobType`. Duplicate enqueues with the same `workKey` are no-ops. Re-downloads with a new fingerprint create a new job and supersede the old one.

**Blocked states:** `missingFile`, `modelUnavailable`, `versionStale`, `leaseLost`. Transient failures use exponential backoff via `nextEligibleAt`. Audio path is resolved from `downloadId` / fingerprint at runtime, not persisted as a raw path.

### Lease-Based Playback Handoff

Background work holds a lease on the asset/session. Handoff is explicit:

1. Playback start requests lease transfer for the asset.
2. Background job checkpoints durable stage frontiers, releases the lease.
3. Job marked `superseded` only after lease release.
4. Playback adopts the same asset/session and resumes from persisted stage coverage.

Race between "background job checkpointing" and "playback starting" is resolved by the lease: playback waits for lease release (with a short timeout) rather than force-cancelling mid-write.

### SkipCue Materialization (replaces raw AdWindow preloading)

Raw `AdWindow` rows are internal detection artifacts. Separately materialize `SkipCue` rows:

```swift
struct SkipCue: Sendable {
    let cueId: String
    let analysisAssetId: String
    let cueHash: String             // Stable hash for dedup
    let startTime: Double
    let endTime: Double
    let confidence: Double
    let source: String              // "preAnalysis" | "liveDetection"
    let advertiser: String?
    let isEligible: Bool            // Meets confidence + span thresholds
}
```

- Pre-analysis writes `SkipCue` rows alongside `AdWindow` rows (via `OutputPolicy.writeWindowsAndCues`).
- `SkipOrchestrator.beginEpisode()` loads only `isEligible` cues, deduped by `cueHash`.
- Live playback analysis refines or supersedes cues instead of duplicating them. Overlap-merge rules prevent double-skips.

### DownloadManager Integration

Add `DownloadContext` parameter to download methods:

```swift
struct DownloadContext: Sendable {
    let podcastId: String?
    let isExplicitDownload: Bool
}
```

After `performDownload()` completes, enqueue a job via `AnalysisWorkScheduler`.

Background download delegate path: reconciler checks for un-enqueued completed downloads at startup, on background URL session completion, and on app upgrade.

### Background Runtime Strategy

T0 is attempted under a short `UIApplication.beginBackgroundTask` to finish in-flight work during the transition to background. If T0 does not complete before the background task expires, the job is checkpointed and picked up by a `BGProcessingTaskRequest`.

T1+ jobs are scheduled via `BGProcessingTaskRequest` with `requiresExternalPower = true` for deep tiers.

Reconciliation runs at:
- App launch
- Background URL session completion handler
- Model install completion
- App upgrade (first launch after update)

### Reconciliation and Error Recovery

A reconciler runs at each reconciliation point and:
- Finds jobs in `running` state with expired leases -> marks `leaseLost`, requeues.
- Finds jobs in `blocked:missingFile` -> checks if audio is re-cached, unblocks.
- Finds jobs in `blocked:modelUnavailable` -> checks model inventory, unblocks.
- Finds jobs with `workKey` matching a stale analysis version -> marks `versionStale`, creates new job.
- Garbage-collects `complete` and `superseded` jobs after a TTL (7 days).
- Applies exponential backoff for transient failures (via `nextEligibleAt`).

### Observability

Instrumentation for tuning and diagnostics:

- **OSSignposter** intervals for: queue wait, job duration, download-to-cue latency, handoff latency, thermal pause duration.
- **Metrics** (logged, not shipped): play-start cue readiness rate, mean time to first cue, false-skip rate, tier completion rate, thermal pause frequency, duplicate-cue suppression rate.
- **Feature flag** / policy injection for tier thresholds and prioritization rules, enabling A/B testing once the core is stable.

### Integration Test Matrix

Beyond unit tests, the following scenarios must be covered:

- Duplicate enqueue (same episode, same fingerprint)
- Re-download with new fingerprint (new content version)
- Paused T1 does not block eligible T0 behind it
- Model becomes available mid-queue (blocked -> eligible)
- Download completes while app is backgrounded
- User taps Play during background job commit (lease transfer)
- Overlap between preloaded cues and live-generated cues (dedup)
- Charger removed mid-T2 (pause + resume on reconnect)
- Stale job after app upgrade (version reconciliation)
- Audio file evicted by LRU while job is paused
