# Playhead Background Download Queue - Wizard Ideas (Codex)

Goal: make subscribed episodes arrive "ready to skip" before first play, while staying fully on-device and battery-conscious.

## 30 Idea Longlist

1. Three-lane queue (`Now`, `Soon`, `Background`) with strict preemption rules. Tech: SwiftData queue + priority lanes. UX: tapped episode always wins instantly. Complexity: Medium.
2. Durable job DAG per episode (`feed_discovered -> download -> shard -> transcribe -> classify -> finalize`). Tech: persistent state machine rows. UX: reliable progress, fewer restarts. Complexity: Medium.
3. Write-ahead progress ledger for every stage transition. Tech: append-only journal + checkpoint snapshots. UX: survives app kill/reboot without duplicate work. Complexity: Medium.
4. Dual scheduler: `BGAppRefreshTask` for feed polling, `BGProcessingTask` for heavy analysis. Tech: BGTaskScheduler. UX: new episodes appear quietly, heavy work happens off-hours. Complexity: Low.
5. Background `URLSession` for downloads independent of process lifetime. Tech: background transfer identifier per account. UX: downloads complete even when app is terminated. Complexity: Low.
6. Episode readiness score (play probability x recency x user affinity x download size). Tech: local scoring model. UX: most likely-to-play episodes become ready first. Complexity: Medium.
7. Time-of-day behavior prior (commute, workout, bedtime) to schedule likely listens. Tech: on-device usage histograms. UX: episodes are ready right before user habits. Complexity: Medium.
8. Charging-window turbo mode (aggressive processing only on power + wifi). Tech: BGProcessingTask `requiresExternalPower`. UX: "magical readiness" without daytime battery hit. Complexity: Low.
9. Thermal governor with quality profiles (`full`, `reduced`, `pause`). Tech: `ProcessInfo.thermalState`. UX: phone stays responsive; queue slows gracefully. Complexity: Low.
10. Battery governor with low power awareness. Tech: `UIDevice.batteryState`, `ProcessInfo.isLowPowerModeEnabled`. UX: no surprise battery drain. Complexity: Low.
11. CPU budget token bucket per hour/day. Tech: local budget manager. UX: predictable battery impact. Complexity: Medium.
12. Segment checkpointing every N shards. Tech: persisted shard cursor + partial transcript commits. UX: resumes from 70 percent instead of restarting. Complexity: Medium.
13. Incremental marker availability (`ready`, `partial ready`, `fully ready`). Tech: marker versioning by coverage range. UX: useful skip markers appear early, improve over time. Complexity: Medium.
14. Fast-path lexical scan before FM classifier, then upgrade pass later. Tech: two-stage detector pipeline. UX: quick first result, better final precision. Complexity: Medium.
15. Sponsor knowledge cache warm-start per show. Tech: local sponsor lexicon reuse. UX: better first-pass detection on new episodes. Complexity: Medium.
16. Queue fairness by show and network (avoid one feed monopolizing compute). Tech: weighted round-robin scheduler. UX: broad library feels maintained. Complexity: Medium.
17. Hard cap for concurrent heavy jobs (`download`, `transcribe`, `classify`) with dynamic tuning. Tech: adaptive semaphore. UX: smooth device behavior. Complexity: Low.
18. Storage tiers: hot (ready episodes), warm (downloaded unprocessed), cold (evictable). Tech: file metadata + LRU/LFU hybrid. UX: ready items stay, old bulk evicts first. Complexity: Medium.
19. User pinning and auto-protect rules for favorite shows. Tech: policy flags in queue model. UX: "my core shows are always ready." Complexity: Low.
20. Disk pressure handling with graceful degradation. Tech: file system free-space monitor. UX: avoids hard failures and broken downloads. Complexity: Low.
21. Content dedup via audio fingerprint of media bytes. Tech: fingerprint index in SQLite/SwiftData. UX: avoids duplicate downloads from feed URL churn. Complexity: High.
22. Idempotent job keys (`episodeID + pipelineVersion + modelVersion`). Tech: deterministic keying. UX: no duplicate background runs after retries. Complexity: Medium.
23. Retry strategy by failure class (network, thermal, model load, decode). Tech: exponential backoff + reason-specific cooldowns. UX: fewer stuck episodes. Complexity: Low.
24. Self-healing watchdog that detects stalled jobs and requeues safely. Tech: lease timeout + heartbeat columns. UX: queue rarely gets "stuck forever." Complexity: Medium.
25. Model prewarm task before transcription batches. Tech: background pre-initialization of speech/FM pipeline. UX: faster throughput during short BG windows. Complexity: Medium.
26. Progressive download strategy (header + first chunks first). Tech: range requests when host supports it. UX: can start early analysis before full download. Complexity: High.
27. On-device observability dashboard (`episodes discovered`, `ready`, `deferred`, `energy saved`). Tech: local metrics store only. UX: trust via transparency. Complexity: Low.
28. "Ready to Skip" badges in library with freshness timestamp. Tech: queue state projection into UI. UX: magical perception is visible and explicit. Complexity: Low.
29. User-triggered "Prepare Next 3" action for flights/commutes. Tech: temporary priority boost lane. UX: clear control with immediate payoff. Complexity: Low.
30. Replay simulation harness for scheduler policies before rollout. Tech: deterministic simulator with captured device traces. UX: fewer regressions and better defaults. Complexity: Medium.

## Top 5 (Best -> Worst)

## 1) Durable Episode Job DAG + Checkpoint Ledger

This is the foundation that makes everything else trustworthy.

How it works:
- Each episode is represented as a persisted DAG with explicit stage nodes.
- Every state transition is written to an append-only ledger and mirrored to a materialized current-state table.
- Shard-level checkpoints persist cursor, partial transcript artifact, and classifier progress.
- Recovery replays ledger into the latest consistent state after crash/termination/reboot.

APIs and components:
- SwiftData/Core Data for DAG nodes and current state.
- Flat file artifacts for audio shards/transcript chunks.
- BG tasks simply "advance graph" from current checkpoint.

Why this is best:
- Surviving app death and reboot is non-negotiable for background magic.
- Prevents expensive rework and duplicated processing.
- Gives deterministic behavior, debuggability, and confidence for future optimization.

User impact:
- "It just keeps making progress," even across days.
- Fewer episodes stuck half-ready.

Confidence: Very high. Durable state machines are the highest leverage reliability primitive in this problem.

## 2) Three-Lane Priority Scheduler with Hard User Preemption

This is the key UX quality lever.

How it works:
- Lane 1 (`Now`): user tapped/queued episodes; always preempts all other work.
- Lane 2 (`Soon`): high readiness score episodes likely to be played next.
- Lane 3 (`Background`): long-tail subscription maintenance.
- Preemption safely pauses lower lane jobs at shard boundaries using persisted checkpoints.

APIs and components:
- Local queue priority policy in Swift actor.
- BGProcessingTask workers consume from highest eligible lane.
- `URLSession` task priority mapped from lane.

Why this is best:
- Preserves instant response for explicit user intent.
- Still allows proactive magic for likely-next episodes.
- Avoids the common failure mode where background work makes foreground feel laggy.

User impact:
- Tapped episode becomes ready fastest.
- Feels smart, not random, in what gets prepared.

Confidence: High. Priority + preemption directly aligns system behavior with perceived quality.

## 3) Energy/Thermal Adaptive Governor with Quality Profiles

This is what keeps the system sustainable on real devices.

How it works:
- Runtime policy reads thermal state, charging state, low power mode, and battery level.
- Switches among profiles:
  - `full`: download + transcription + FM classification.
  - `reduced`: download + transcription only, defer FM.
  - `pause`: metadata polling only.
- Re-evaluates every stage boundary and on state notifications.

APIs and components:
- `ProcessInfo.thermalState`
- `ProcessInfo.isLowPowerModeEnabled`
- `UIDevice.isBatteryMonitoringEnabled`, `batteryState`, `batteryLevel`

Why this is best:
- Avoids user-hostile heat/battery events that would cause feature disablement.
- Converts hard failures into graceful slowdown.
- Keeps background processing acceptable enough to stay enabled long-term.

User impact:
- Device remains cool and responsive.
- Episodes still progress when conditions improve.

Confidence: High. Without adaptive power policy, background processing quality is not durable.

## 4) Two-Tier Background Orchestration (Refresh + Processing + Background URLSession)

This is the practical iOS scheduling backbone.

How it works:
- `BGAppRefreshTask` performs lightweight feed checks and queue updates.
- `BGProcessingTask` executes heavier stages when the system grants longer windows.
- Background `URLSession` handles episode transfers independent of app lifecycle.
- Each task invocation advances a bounded amount of DAG work, then reschedules.

APIs and components:
- `BGTaskScheduler` registrations with conservative earliestBeginDate.
- `URLSessionConfiguration.background(...)` with resume-friendly downloads.

Why this is best:
- Matches iOS execution model instead of fighting it.
- Splits cheap discovery from expensive compute.
- Maximizes completion odds under unpredictable system scheduling.

User impact:
- New episodes discovered promptly.
- Heavy processing happens opportunistically without requiring app-open sessions.

Confidence: High. This is the most reliable native iOS pattern for long-lived background pipelines.

## 5) Readiness Score + Visible "Ready to Skip" UX Contract

This is what turns backend work into perceived magic.

How it works:
- Per episode readiness score combines recency, listening probability, queue position, and show affinity.
- Scheduler uses score to pick `Soon` lane candidates.
- UI surfaces concrete state: `Ready to Skip`, `Partially Ready`, `Queued`, with last-updated timestamp.
- Optional "Prepare Next 3" quick action gives users direct control.

APIs and components:
- On-device scoring model in queue service.
- Library/episode cells bound to queue state projection.
- Optional Widget/Home Screen surfacing ready count.

Why this is best:
- Improves both objective prioritization and subjective trust.
- Users understand what the app is doing and why.
- Reduces perceived randomness that kills confidence in automation.

User impact:
- Feels proactive and intelligent.
- Users notice readiness before tapping play.

Confidence: Medium-high. The algorithm can iterate, but visibility + intent-aware ordering reliably improve product perception.

## Why this top-5 set wins as a system

Together these five ideas create a coherent stack:
- Reliability base (durable DAG)
- Correct prioritization (three lanes + preemption)
- Safe operation (energy/thermal governor)
- iOS-native execution plumbing (two-tier BG orchestration)
- Clear user-facing value (readiness scoring + visible status)

That combination is what makes "already ready to skip" believable at scale, not just in demos.
