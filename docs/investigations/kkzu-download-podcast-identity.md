# playhead-kkzu — investigation notes

Re-verified against `195245d7` (main at branch point). Line numbers below are
from that tree, not from the bead text (the file has moved since).

## 1. The claim, re-checked

`analysis_jobs.podcastId` is written by exactly one function,
`AnalysisWorkScheduler.enqueue(episodeId:podcastId:...)`. Two production
callers, both `DownloadManager`:

| site | what it passes |
|---|---|
| `DownloadManager.swift:1364` (`performDownload`, progressive) | `context?.podcastId` |
| `DownloadManager.swift:2109` (`enqueueAnalysisIfNeeded`) | `context?.podcastId` |

`DownloadContext` is constructed at exactly one production site:
`PlayheadRuntime.swift:4018`, the streaming branch of `performPlayEpisode`.
Every other route defaults or passes `nil`:

- `DownloadManager.swift:1214` `progressiveDownload(... context: DownloadContext? = nil)`
- `DownloadManager.swift:1257` `performDownload(... context: DownloadContext? = nil)`
- `DownloadManager.swift:1694` `streamingDownload(... context: DownloadContext? = nil)`
- `DownloadManager.swift:1676` `_finalizeStreamingTransferForTesting` → `context: nil` (DEBUG only)
- **`DownloadManager.swift:3644`** `handleBackgroundDownloadComplete` → `enqueueAnalysisIfNeeded(context: nil)`
- `DownloadManager.swift:2173` `backgroundDownload(episodeId:from:)` — **no context parameter at all**

So: **CONFIRMED**. Every background / auto download enqueues its analysis job
with `podcastId = NULL`.

## 2. Where the identity actually is

`podcastId` in this codebase is the **feed URL string** (`PlayheadRuntime:4018`
comment: "The podcastId mirrors the rest of the runtime (`feedURL.absoluteString`)"),
canonicalised through `RecurrenceMaterialIdentity.canonicalIdentifier`, which is a
*validator* not a normaliser — it returns the value only in its exact canonical
spelling and `nil` otherwise (`AdCatalogStore.swift:119`).

Production callers of `backgroundDownload`, and whether the show is knowable:

| caller | has? |
|---|---|
| `EpisodePreparationCoordinator.swift:236` (via `DownloadManagerPreparationAdapter.startDownload`) | **YES** — `Request.podcastId` is already threaded in from `PlayheadRuntime:3727` and used for `enqueueUserIntentAnalysis`; the download half drops it |
| `BackgroundFeedRefreshService.swift:857` (via `AutoDownloadEnqueueing`) | **YES** — `FeedRefreshNewEpisode.feedURL` |
| `EpisodeListView.swift:238` | **YES** — a live SwiftData `Episode`, so `episode.resolvedShowIdentity` |
| `ForceQuitResumeScan.swift:498` | **NO** — a `DownloadManager` extension driven from on-disk resume blobs after a relaunch; SwiftData is not in scope |

## 3. The part that makes an in-memory map insufficient

`backgroundDownload` starts a **`URLSession` background** task. The analysis
enqueue happens in `handleBackgroundDownloadComplete`, which is reached from the
session delegate — and iOS relaunches the app to deliver
`handleEventsForBackgroundURLSession`. So the enqueue routinely runs in a
**different process** from the `backgroundDownload` that started it.

An in-process `[episodeId: DownloadContext]` dictionary would therefore lose the
show for exactly the population this bead exists to fix — a download that
completes while the app is suspended or terminated. The attribution has to be
durable.

There is already a per-episode sidecar pattern to follow in
`ForceQuitResumeScan.swift:60-215`: `<hash>.resume` / `<hash>.episode` /
`<hash>.validator` under `resumeDataDirectory`, hashed with
`Self.safeFilename(for: episodeId)`.

## 4. The djl0 shadow-mode chain — does a NULL podcastId fire it?

**NO, and the bead's own closing note already says so.** Checked rather than
assumed:

`SkipOrchestrator.beginEpisode` (`SkipOrchestrator.swift:1829`) takes
`podcastId:` from its caller. Its production caller is
`PlayheadRuntime.swift:3860`, which computes:

    let podcastId = episode.resolvedShowIdentity

`Episode.resolvedShowIdentity` (`Podcast.swift:250`) derives from the **SwiftData**
`podcast?.feedURL`, with a fallback that parses the canonical episode key. It
never reads `analysis_jobs`. So for a normally-subscribed episode the play path
resolves the show whether or not the job row has it.

`analysis_jobs.podcastId` is only consulted by `recoverShowIdentity`
(`SkipOrchestrator.swift:2027` → `AnalysisStore.fetchRecordedPodcastId`), which
runs **only when the caller's `podcastId` was already nil** — i.e. it is the
second chance, not the first. A NULL job row makes that second chance useless,
but it is not what puts an episode into `.shadow`.

So the acceptance question "does the shadow chain actually fire for a NULL
podcastId" answers **no**: `.unresolvedShowIdentity` requires the SwiftData
derivation to fail *first*. What a NULL job row does is remove the recovery net
underneath that failure.

## 5. What the NULL does cost, checked

`AnalysisWorkScheduler.swift:3952` builds the range request as
`podcastId: job.podcastId ?? ""`, and that empty string is carried by
`AnalysisRangeRequest` into per-show state:

- `AnalysisJobRunner.swift:261/320/634/849/911/974/999/1015`
- `SponsorKnowledgeStore` (show-scoped sponsor memory)
- `ImplicitFeedbackStore`
- `CorrectionAttribution` (which at `:537` filters `!$0.podcastId.isEmpty`, i.e.
  it already has to defend against this value)
- `TargetedWindowNarrower.swift:1177` mixes it into an audit material string

So every unattributed episode is pooled under one fake show `""`. That is the
07-29 "a value that names an absence" defect class, and it is the real cost —
independent of the shadow-mode question.
