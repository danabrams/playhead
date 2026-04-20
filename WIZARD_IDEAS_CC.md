# Wizard Ideas: Background Download Queue System — Claude Code (CC)

**Date:** 2026-04-16
**Scope:** Background download queue for podcast subscriptions — automatic feed discovery, episode downloading, transcription + ad classification in the background, so skip markers are ready before the user opens an episode.

## Context & Existing Architecture

Playhead already has a sophisticated analysis pipeline: chunk-based transcription with checkpoint recovery (`transcript_chunks`), thermal/battery-aware `DeviceAdmissionPolicy`, SQLite-persisted analysis state machine (`analysis_jobs` with lease pattern), BGProcessingTask for backfill, tiered processing depth (T0/T1/T2), streaming audio decode via `StreamingAudioDecoder`, and LRU cache eviction. The `AnalysisWorkScheduler` actor selects highest-priority eligible jobs under admission policy.

**What's missing** is the proactive subscription layer: automatically discovering new episodes via feed polling, downloading them before the user asks, and feeding them into the existing analysis pipeline so skip markers are ready before the user even opens the episode. The ideas below focus on this missing layer and how it integrates with the existing infrastructure.

---

## Brainstorm (30 Ideas)

1. Two-tier BGTask: BGAppRefreshTask for feed polling + BGProcessingTask for download/analysis
2. Subscription engagement tiers based on listen history
3. WiFi-only auto-download with per-podcast cellular override
4. Progressive readiness UI badges (New → Downloading → Analyzing → Ready)
5. Predictive scheduling based on listening cadence
6. Foreground idle harvesting for processing
7. Feed ETag/Last-Modified conditional requests
8. Download-ahead depth per subscription (configurable episodes to keep ready)
9. Global storage budget manager with elastic per-subscription allocation
10. Coalesced concurrent feed polling during BGAppRefreshTask
11. Adaptive feed polling frequency matched to publication cadence
12. Silent push notifications for feed updates (requires server)
13. Episode filtering before download (min duration, keyword exclusion)
14. Overlapping download + T0 analysis via streaming decode
15. Intelligent queue ordering: engagement tier × recency × estimated processing time
16. Adaptive T-depth selection based on BGTask time budget
17. Download integrity verification (Content-Length, codec header check)
18. Burst queuing for OPML import (rate-limited priority ramp)
19. Background processing analytics (BGTask grant rate, ready-before-listen %)
20. Stale analysis reprocessing when FM model updates
21. iCloud subscription sync (list only, not analysis — on-device mandate)
22. iPad Power Nap-aware aggressive processing
23. Network quality-based download scheduling (pause on slow connections)
24. Episode expiration auto-cleanup (N days unlistened)
25. Dead/moved feed detection and user notification
26. Fast-path for ad-free shows (skip FM after 10 clean episodes)
27. Graceful degradation under storage pressure (<1GB = stop, <500MB = evict)
28. Post-reboot download reconciliation with URLSession
29. "Ready to skip" local notification for favorite episodes
30. Unified queue dashboard showing all pending work

## Winnowing Analysis

**Already handled by existing infrastructure (eliminated as standalone ideas):**
Chunk-based checkpointing, thermal gating, battery awareness, SQLite persistence, job reconciliation, LRU eviction, tiered depth — all exist. Ideas that merely replicate these aren't novel.

**Eliminated for architectural reasons:**
- #12 (silent push): Requires a server component. While feed monitoring isn't "processing," it introduces operational complexity and a dependency. The two-tier BGTask approach achieves similar responsiveness without a server.
- #21 (iCloud sync): Important feature but orthogonal to the background queue system itself.
- #22 (iPad Power Nap): Niche optimization; iPad is not the primary platform.
- #5 (predictive scheduling): Elegant but premature — requires weeks of listening data before useful. The engagement-tier approach provides immediate value and naturally evolves toward prediction.

**Strong candidates grouped by theme:**

| Theme | Ideas | Why they matter |
|-------|-------|-----------------|
| Architecture | 1, 10, 7, 11 | Foundation: how the system discovers and triggers work |
| Intelligence | 2, 15, 16 | Optimization: doing the RIGHT work in limited time |
| Performance | 14, 6 | Throughput: getting more done per unit of time |
| UX | 4, 29 | Trust: making invisible work visible |
| Resilience | 8, 9, 27, 28 | Robustness: working correctly under constraints |

---

## Top 5 Ideas (Best to Worst)

---

### 1. Two-Tier BGTask Architecture with Feed-Aware Coalescing

**The single most important architectural decision for the entire subscription system.**

#### The Problem

Playhead's existing `BackgroundProcessingService` handles analysis backfill via `BGProcessingTask`, but there is no mechanism to **discover** new episodes in the background. The OS grants `BGProcessingTask` opportunities infrequently (often only when the device is charging and idle overnight). If feed polling is bundled into the same `BGProcessingTask`, discovery latency becomes unacceptable — a podcast could publish at 6 AM and the user wouldn't know until they opened the app at 7 AM.

#### The Design

**Layer 1: `BGAppRefreshTask` for Feed Discovery**

Register a new task identifier (e.g., `com.playhead.app.feed.refresh`) as a `BGAppRefreshTask`. The OS grants these more frequently than `BGProcessingTask` — typically every 1-4 hours for actively-used apps, with a 30-second execution window. This is enough time to:

1. Query all subscribed feed URLs concurrently (8 concurrent connections max via `TaskGroup`).
2. Use HTTP conditional requests (`If-None-Match` with stored ETag, `If-Modified-Since` with stored last-modified date) to skip unchanged feeds — most polls return 304 Not Modified in <100ms.
3. For changed feeds, parse only the `<item>` elements newer than the last-seen `<pubDate>`.
4. For each new episode discovered: insert a row into a new `subscription_queue` SQLite table with state `pending_download`.
5. If any new episodes were found, schedule a `BGProcessingTask` for download + analysis.
6. Always re-schedule the next `BGAppRefreshTask` before returning (required by the API).

**Layer 2: `BGProcessingTask` for Download + Analysis**

The existing `BackgroundProcessingService` is extended to handle subscription queue items alongside the existing analysis backfill. When a `BGProcessingTask` fires:

1. Check `subscription_queue` for `pending_download` items.
2. Download episodes in priority order (see Idea #2).
3. As each download completes, enqueue into the existing `analysis_jobs` table via `AnalysisWorkScheduler.enqueue()`.
4. Run analysis (T0 → T1 → T2 depending on time budget — see Idea #2).
5. On task expiration, persist all progress; downloads use `URLSession.background` so they continue independently.

**Layer 3: Adaptive Polling Frequency**

Not all feeds need the same polling cadence. Track each feed's publication history:

```swift
struct FeedPollingPolicy {
    let feedURL: URL
    let averagePublishInterval: TimeInterval  // computed from last 10 episodes
    let lastPublishDate: Date
    let lastPollDate: Date
    let consecutiveNoChanges: Int  // for exponential backoff on dead feeds

    var nextPollDate: Date {
        // Poll at 1/4 of the average interval, clamped to [4h, 7d]
        let quarter = averagePublishInterval / 4
        let interval = max(4 * 3600, min(7 * 86400, quarter))
        return lastPollDate.addingTimeInterval(interval)
    }
}
```

During a `BGAppRefreshTask`, only poll feeds whose `nextPollDate` has passed. A daily news podcast gets polled every ~6 hours. A monthly podcast gets polled every ~7 days. This saves significant battery across many subscriptions.

**Layer 4: Coalesced Concurrent Polling**

Within the 30-second BGAppRefreshTask window, maximize throughput:

```swift
func pollFeeds(eligible: [FeedPollingPolicy]) async -> [DiscoveredEpisode] {
    await withTaskGroup(of: FeedPollResult.self) { group in
        for feed in eligible.prefix(40) {  // cap to stay within time budget
            group.addTask {
                await self.pollFeed(feed, using: conditionalRequest(feed))
            }
        }
        var discovered: [DiscoveredEpisode] = []
        for await result in group {
            discovered.append(contentsOf: result.newEpisodes)
        }
        return discovered
    }
}
```

#### Why This Is the Best Idea

Without this two-tier architecture, the subscription system fundamentally cannot work in the background. `BGProcessingTask` alone is too infrequent for timely feed discovery. `BGAppRefreshTask` alone doesn't have enough runtime for downloads and analysis. The combination exploits each API's strengths: frequent, lightweight discovery via `BGAppRefreshTask`; infrequent, heavy processing via `BGProcessingTask`. The adaptive polling and conditional requests make it battery-efficient even with 50+ subscriptions.

This is the **keystone** — every other idea in this list depends on new episodes being discovered and queued in the background, which only this architecture enables.

#### Implementation Complexity: Medium-high

Requires: new `BGAppRefreshTask` registration in Info.plist, new `FeedPollingService` actor, new `subscription_queue` SQLite table, integration with existing `BackgroundProcessingService`, feed parser (lightweight XML or `FeedKit`), conditional request header management. The existing `BackgroundProcessingService` patterns (double-registration guard, expiration handler, task completion guard) provide a strong template.

#### Confidence: 95%

This is the canonical iOS pattern for subscription-based content apps (Overcast, Pocket Casts, Apple Podcasts all use this dual-task approach). The adaptive polling frequency is the value-add over a naive implementation.

---

### 2. Engagement-Weighted Priority Queue with Breadth-First Budget Allocation

**Ensures that limited background processing time goes to the episodes the user is most likely to listen to next.**

#### The Problem

A user with 20 podcast subscriptions might have 8 new episodes to process when a `BGProcessingTask` fires. The OS might grant 3-5 minutes of processing time. Full T2 analysis of one episode takes ~2 minutes. Naive FIFO ordering might process episodes from podcasts the user hasn't listened to in weeks while leaving their daily must-listen show unprocessed.

#### The Design

**Engagement Scoring**

Compute a per-podcast engagement score from listening behavior:

```
EngagementScore = (
    0.4 × listenRate          // % of episodes listened in last 30 days
  + 0.3 × recencyWeight       // days since last listen, decayed exponentially
  + 0.2 × completionRate      // average % of episode listened
  + 0.1 × speedOfConsumption  // hours between publish and first listen, inverted
)
```

Data source: the existing `Episode` SwiftData model already tracks play state. Computing this requires a lightweight query over recent listening history, run once per `BGProcessingTask` invocation.

A show listened to every day within hours of publishing scores ~0.95. A show with a 20% listen rate and last listen 3 weeks ago scores ~0.15.

**Priority Queue Ordering**

When selecting which episodes to process during a `BGProcessingTask`, score each pending episode:

```
EpisodePriority = (
    engagement × 0.5           // podcast engagement score
  + recency × 0.3              // hours since publish, normalized & decayed
  + inverseProcessingTime × 0.2 // shorter episodes = faster to ready
)
```

The `inverseProcessingTime` factor is key: a 15-minute episode can be fully T2-analyzed in under a minute, while a 3-hour episode takes 10+ minutes. Processing two short episodes to "ready" is often more valuable than getting one long episode halfway.

**Breadth-First Budget Allocation**

Rather than processing one episode to T2 completion before starting the next, allocate the BGProcessingTask budget breadth-first:

```
Phase 1: T0 (90s) on top 5 episodes by priority    (~5 × 30s = 2.5 min)
Phase 2: T1 (300s) on top 3 episodes by priority   (~3 × 60s = 3 min)
Phase 3: T2 (full) on top 1 episode if time remains
```

This means after a single BGProcessingTask, 5 episodes have basic skip markers (first 90 seconds), 3 have good skip markers (first 5 minutes), and 1 has complete coverage. The user's most-listened-to shows are guaranteed at least T0 coverage.

**Dynamic Depth Adjustment**

Monitor elapsed time during the BGProcessingTask. If the expiration handler hasn't fired after Phase 1, proceed to Phase 2. If still running after Phase 2, proceed to Phase 3. If the expiration handler fires mid-phase, persist progress via existing checkpoints and exit. The tiered approach means every interruption point leaves the maximum number of episodes partially ready rather than one episode fully ready and the rest at zero.

#### Why This Is the #2 Idea

The two-tier BGTask architecture (Idea #1) gets work queued. This idea determines **which work gets done first** with the limited time budget. The iOS background processing budget is the scarcest resource in the entire system — a well-ordered priority queue is the highest-leverage optimization possible. The breadth-first approach is specifically designed for the reality that BGProcessingTask windows are unpredictable and often short.

The engagement scoring ensures the system gets smarter over time. A new subscription starts with a default score (~0.5) and quickly calibrates based on actual behavior. If a user subscribes to 30 podcasts but only regularly listens to 8, the system naturally focuses its limited budget on those 8.

#### Implementation Complexity: Medium

Requires: engagement score computation (query over Episode play history), priority scoring function, modification to `AnalysisWorkScheduler` to use priority scores instead of simple 0/10, breadth-first phase loop in BGProcessingTask handler. The existing `analysis_jobs.priority` column becomes a continuous score. The existing T0/T1/T2 tiering in `PreAnalysisConfig` provides the depth levels.

#### Confidence: 90%

Priority queuing is proven in every production podcast app. The engagement scoring is the differentiator — most apps use simple recency. The breadth-first budget allocation is the insight that makes this transformative: in a world of scarce BGTask budget, getting 5 episodes to T0 beats getting 1 episode to T2.

---

### 3. Overlapping Download-Analysis Pipeline via Streaming Decode

**Cuts total episode readiness time by 30-50% by running download I/O and T0 analysis concurrently.**

#### The Problem

The current flow for a new episode is sequential: download audio file → decode to analysis shards → extract features → transcribe → classify. For a typical 60-minute episode (~80MB), download takes 2-3 minutes on WiFi, decode takes 30 seconds, and T0 analysis takes another 30 seconds. Total: ~3.5 minutes. During the 2-3 minute download, the CPU is idle. During analysis, the network is idle. This wastes the precious BGProcessingTask window.

#### The Design

**Streaming Decode Integration**

Playhead already has `StreamingAudioDecoder` in `AnalysisAudio.swift` that supports incremental decode as chunks arrive. The key insight: T0 analysis only needs the first 90 seconds of audio. On a 256kbps podcast, that's ~1.4MB — available within seconds of starting the download.

The pipeline becomes:

```
t=0s:   Start download via URLSession
t=3s:   First 1.4MB arrived → StreamingAudioDecoder begins
t=5s:   First 90s decoded → T0 analysis starts
        (download continues in parallel on network thread)
t=35s:  T0 complete — skip markers for first 90s READY
t=45s:  First 5 min of audio available → T1 analysis begins
t=105s: T1 complete — skip markers for first 5 min READY
t=180s: Download complete → T2 can begin when budget allows
```

Compare to sequential: T0 ready at t=210s vs t=35s. **6× faster to first skip markers.**

**Implementation Architecture**

```swift
actor SubscriptionPipelineCoordinator {
    func processNewEpisode(_ episode: QueuedEpisode) async {
        let downloadStream = downloadManager.startDownload(episode.audioURL)

        var t0Started = false, t1Started = false
        for await progress in downloadStream {
            if progress.bytesWritten >= t0AudioThreshold && !t0Started {
                t0Started = true
                Task {
                    await analysisJobRunner.runT0(
                        partialFile: progress.partialFilePath,
                        availableDuration: progress.estimatedDuration
                    )
                }
            }
            if progress.bytesWritten >= t1AudioThreshold && !t1Started {
                t1Started = true
                Task {
                    await analysisJobRunner.runT1(
                        partialFile: progress.partialFilePath,
                        availableDuration: progress.estimatedDuration
                    )
                }
            }
            if progress.isComplete {
                await analysisWorkScheduler.enqueue(
                    episode.analysisAssetId, depth: .t2
                )
            }
        }
    }
}
```

**URLSession Background Transfer Compatibility**

`URLSession.background` transfers don't provide real-time progress callbacks when the app is suspended. Two modes:

1. **Foreground/BGProcessingTask overlap**: When the app is active (foreground or during BGProcessingTask), use a standard URLSession for the download with progress callbacks. The overlap pipeline fires.
2. **Background transfer fallback**: When the download completes via background transfer (app was suspended), the completion handler triggers → enqueue T0 immediately. No overlap, but still an immediate T0 kickoff upon completion.

The foreground case is the more common one for user-initiated episodes and provides the most perceived benefit.

#### Why This Is the #3 Idea

This is a pure performance optimization with dramatic payoff. The existing `StreamingAudioDecoder` means the hard infrastructure already exists — this idea wires it into the subscription pipeline to overlap I/O with compute. The 6× improvement in time-to-first-skip-markers directly translates to more episodes being "Ready" when the user opens the app, especially during BGProcessingTask windows where every second counts.

It's #3 and not higher because it's an optimization, not a structural enabler. Ideas #1 and #2 determine WHETHER episodes are discovered and WHICH ones are processed. This idea determines HOW FAST they're processed. All three together form the complete system; but if you could only build two, you'd pick #1 and #2.

#### Implementation Complexity: Medium

Requires: new `SubscriptionPipelineCoordinator` actor, integration between `DownloadManager` progress streams and `AnalysisJobRunner`, partial-file duration estimation (from Content-Length + audio bitrate or partial header decode), threshold configuration. The `StreamingAudioDecoder` and `AnalysisJobRunner` are battle-tested; the new code is coordination logic.

#### Confidence: 85%

The architecture is sound and the APIs support it. The 85% reflects edge-case complexity: partial file corruption, download pauses/resumes, thermal interruption during overlapped analysis, and the URLSession background transfer limitation. All solvable but require careful engineering.

---

### 4. Foreground Idle Harvesting

**Supplements the scarce BGProcessingTask budget with abundant, reliable foreground processing time.**

#### The Problem

iOS grants `BGProcessingTask` opportunities unpredictably — sometimes once per day, sometimes less for apps that aren't used daily. The user might have 10 episodes queued for processing but only get one 5-minute BGProcessingTask window per day. Meanwhile, the user might spend 15 minutes per session in the app browsing the library, reading show notes, or managing subscriptions — all of which use negligible CPU.

#### The Design

**Idle Detection**

Define "foreground idle" as: app is in the foreground AND no audio is playing AND no user interaction for >5 seconds AND thermal state is nominal or fair AND battery > 30% (or charging).

The existing `CapabilitiesService` already monitors thermal state and battery. Playback state is tracked in `PlayheadRuntime`. The missing piece is user interaction recency, detectable via a `UIApplication` activity timer.

```swift
actor ForegroundHarvestCoordinator {
    private var lastInteractionDate = Date()
    private var harvestTask: Task<Void, Never>?

    func userDidInteract() {
        lastInteractionDate = Date()
        harvestTask?.cancel()
    }

    func checkIdleEligibility() -> Bool {
        let idle = Date().timeIntervalSince(lastInteractionDate)
        let snap = capabilitiesService.latestSnapshot
        return idle > 5.0
            && !playbackService.isPlaying
            && snap.thermalState <= .fair
            && (snap.isCharging || snap.batteryLevel > 0.3)
    }

    func startHarvestingIfEligible() {
        guard checkIdleEligibility() else { return }
        harvestTask = Task(priority: .utility) {
            await analysisWorkScheduler.runNextPendingJob(
                source: .foregroundHarvest
            )
        }
    }
}
```

**Integration with Existing Pipeline**

Foreground harvesting uses the **exact same pipeline** as BGProcessingTask — it calls `AnalysisWorkScheduler.runNextPendingJob()` with the same priority ordering from Idea #2. The only difference is the trigger (idle detection vs OS-granted background time) and the cancellation policy (cancel immediately on user interaction vs cancel on BGTask expiration).

**Instant Preemption**

When the user taps anything, the harvest task cancels cooperatively via Swift structured concurrency. The existing checkpoint system in `transcript_chunks` and `feature_extraction_state` ensures no work is lost. The next idle period picks up where it left off.

**Throttling**

Runs at `.utility` QoS (not `.userInitiated`). Limits to one analysis job at a time. Yields CPU periodically to keep UI responsive:

```swift
// Inside analysis hot loop, cooperative yield every 100ms
try await Task.sleep(for: .milliseconds(10))
```

**Visible Progress**

Unlike background processing (invisible), foreground harvesting happens while the user is watching. This is an opportunity to show live progress — a subtle animation on the episode row as it transitions from "Downloaded" to "Analyzing" to "Ready." The user sees the system working in real-time, building enormous trust.

#### Why This Is the #4 Idea

**Foreground time is abundant and reliable; background time is scarce and unreliable.** Most podcast app users spend 5-20 minutes per session in the app. Even 5 minutes of foreground harvesting can process 2-3 episodes to T0 — equivalent to an entire BGProcessingTask grant. Over a week, foreground harvesting might provide 3-5× more total processing time than BGProcessingTask alone.

It's #4 because it's supplementary — it only works when the user is in the app. It doesn't replace the BGTask architecture (Idea #1) or the priority system (Idea #2). But it dramatically increases total processing throughput and provides a reliable fallback when the OS is stingy with BGProcessingTask grants.

The UX benefit is the hidden gem: seeing episodes become "Ready" while you're browsing is delightful and makes the system feel alive.

#### Implementation Complexity: Low-medium

Requires: `ForegroundHarvestCoordinator` actor (~150 lines), idle detection timer, integration point in `PlayheadRuntime` for scene phase changes, `.utility` QoS dispatch. The entire analysis pipeline already exists — this is purely a new trigger mechanism.

#### Confidence: 92%

This is a well-understood pattern used by Photos (background photo analysis during foreground idle) and Safari (preloading during idle). The iOS APIs are straightforward. The only tuning risk is the idle heuristic (too aggressive = jank, too conservative = wasted opportunity), which is easily adjustable.

---

### 5. "Ready to Skip" Local Notifications with Progressive Readiness Badges

**Makes the invisible background system visible, builds user trust, and drives engagement through a carefully layered notification and badge system.**

#### The Problem

The background download + analysis system does its best work when the user isn't looking. But if the user never sees evidence of this work, they don't understand the value proposition. Worse, they might not trust the app: "Did it actually process my episodes? Are the skip markers reliable?" Every competing podcast app shows download progress; none show analysis progress. This is a differentiation opportunity.

#### The Design

**Progressive Readiness Badges (In-App)**

Each episode in the library shows a subtle state indicator:

| State | Visual | Meaning |
|-------|--------|---------|
| New | Blue dot | Episode discovered, not yet downloaded |
| Downloading | Thin progress ring (gray) | Download in progress with % |
| Downloaded | Filled circle (gray) | Audio available, analysis pending |
| Analyzing | Thin progress ring (purple) | Transcription/classification running |
| Partial | Half checkmark (amber) | T0/T1 done, T2 pending — skip markers for first few minutes |
| Ready | Checkmark (green) | Full skip markers available |

The "Partial" state is important — it communicates "skip markers exist for the first few minutes, full coverage still processing." This sets accurate expectations without making the system look broken.

```swift
enum EpisodeReadiness: Comparable {
    case new
    case downloading(progress: Double)
    case downloaded
    case analyzing(progress: Double)
    case partial(coverage: TimeInterval, total: TimeInterval)
    case ready
}

func readiness(for episode: Episode) async -> EpisodeReadiness {
    if let session = await analysisStore.session(for: episode.id) {
        switch session.state {
        case .complete: return .ready
        case .backfill:
            return .partial(
                coverage: session.coveredDuration,
                total: episode.duration
            )
        case .hotPathReady, .featuresReady, .spooling:
            return .analyzing(progress: session.progress)
        case .queued: return .downloaded
        }
    }
    if let dl = await downloadManager.activeDownload(for: episode.audioURL) {
        return .downloading(progress: dl.fractionCompleted)
    }
    return .new
}
```

**"Ready to Skip" Local Notifications**

When a high-engagement podcast's latest episode transitions to `.ready` (or `.partial` with >80% coverage), post a local notification:

```
🎧 [Podcast Name] — new episode ready
"Episode Title" · skip markers active
```

Key design decisions:

1. **Only for high-engagement podcasts** (engagement score > 0.7 from Idea #2). Don't spam notifications for every subscription.
2. **Aggregate low-priority notifications.** If 5 low-engagement episodes become ready at once, post a single summary: "5 episodes ready to play — skip markers active."
3. **Respect notification settings.** Use `UNNotificationCategory` with customizable actions: "Play Now" (deep link) and "Later" (dismiss). Users can disable per-podcast in settings.
4. **Time-gated delivery.** Don't deliver between 10 PM and 7 AM (user-configurable quiet hours) via `UNNotificationTrigger` date components.
5. **Rich notification preview.** A notification content extension showing podcast artwork and a mini waveform with skip regions highlighted. No other podcast app can preview where the ads are before playback starts.

**App Icon Badge**

Badge count = number of "Ready" episodes the user hasn't seen yet (reached `.ready` since last app open). This creates an engagement loop: badge appears → user opens app → sees green checkmarks → plays episode → experiences ad-skipping → trusts the system → leaves the app running in the background more confidently.

#### Why This Is the #5 Idea

The background system is only as valuable as the user's perception of it. A perfectly-functioning invisible system that the user doesn't trust is worth less than a slightly-less-optimal system the user loves and relies on. The progressive badges solve the trust problem; the notifications solve the engagement problem.

It's #5 (not higher) because it's a UX layer on top of the functional system, not the functional system itself. But it's in the top 5 because without it, the user might never discover that episodes are pre-processed. The "Ready" badge is the moment the user realizes Playhead is fundamentally different from every other podcast app — their episodes are waiting, analyzed, with skip markers in place, before they even thought about listening.

The notification content extension with the skip-region waveform preview is the premium differentiator. No other podcast app can show you a preview of where the ads are before you start playing. This is only possible because of the on-device analysis pipeline, and it deserves to be showcased.

#### Implementation Complexity: Medium

Requires: `EpisodeReadiness` computed property (queries existing stores), SwiftUI badge views in episode list cells, `UNUserNotificationCenter` integration, notification preference management (per-podcast, quiet hours), app badge count via `setBadgeCount()`. The notification content extension is the highest-complexity piece but also the most optional — the system provides value with plain text notifications alone.

#### Confidence: 88%

Local notifications for content readiness are a proven engagement pattern (Netflix "download complete," YouTube "video ready offline"). The progressive badges are standard UX. The 88% reflects notification fatigue risk — the engagement-score gating and aggregation are critical to avoiding it, and getting thresholds right requires user testing.
