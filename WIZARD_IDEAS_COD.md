# Playhead Improvement Ideas (30 -> Top 5)

This pass respects Playhead's constraints:
- On-device only (no cloud ASR/classification)
- Existing actor-based architecture (`PlayheadRuntime`, `AnalysisCoordinator`, `AnalysisWorkScheduler`, `AdDetectionService`, `SkipOrchestrator`, `DownloadManager`, `CapabilitiesService`, `SurfaceStatus`)
- Pragmatic, accretive changes that can be phased through bd beads

## 30 Candidate Ideas

1. Episode Pipeline Watchdog + Auto-Heal
- How it works: Track per-episode heartbeat timestamps across decode, transcript, detection, and skip stages; if a stage exceeds SLA, trigger bounded recovery (retry stage, re-open lease, or downgrade lane).
- User perception: Fewer "stuck" episodes and fewer silent failures.
- Implementation: Add watchdog actor fed by `AnalysisCoordinator` + `AnalysisWorkScheduler` lifecycle events; persist health state in `AnalysisStore` work journal.

2. Lease Expiry Escalation Ladder
- How it works: Explicit retry tiers for lease expiration (quick retry -> clean stage restart -> episode quarantine with reason).
- User perception: More predictable recovery and clearer failure reasons.
- Implementation: Extend `EpisodeExecutionLease` + `CauseTaxonomy` + `SurfaceStatus` mapping.

3. Unified Resource Governor (battery/thermal/storage/network)
- How it works: One policy engine outputs a runtime budget profile used by scheduler, transcript engine, and downloads.
- User perception: Better battery life and fewer thermal slowdowns without manual tuning.
- Implementation: New governor actor consuming `CapabilitiesService`, `StorageBudget`, transport status; policy hooks into `AnalysisWorkScheduler`, `TranscriptEngineService`, and `DownloadManager`.

4. Adaptive Hot-Zone Sizing by Measured Throughput
- How it works: Continuously measure decode+ASR throughput versus playback rate; dynamically resize lookahead horizon.
- User perception: Faster first skip at 1x and better resilience at 2x/3x.
- Implementation: Add throughput estimator to scheduler instrumentation; update lane admission window and candidate selection.

5. Real "Time-to-First-Usable-Skip" Optimizer
- How it works: Explicitly optimize early pipeline for first skip candidate before broad backfill.
- User perception: App proves value sooner after hitting play.
- Implementation: Add first-value objective in `AnalysisWorkScheduler` + `AdDetectionService` candidate prioritization.

6. Backfill Debt Compactor
- How it works: Track and compact stale/low-value backfill jobs when conditions are poor (battery, storage, low trust show).
- User perception: Fewer background churn events.
- Implementation: Add debt scoring in `AnalysisJobReconciler`; prune or defer old backfill rows.

7. Deterministic Download->Analysis Handshake
- How it works: Strong handoff contract so analysis starts only when cache integrity and duration probe pass.
- User perception: Fewer failed analyses caused by partial media state.
- Implementation: Tighten `DownloadManager` completion path + `AnalysisAudioService` preflight checks + explicit terminal causes.

8. Corruption Quarantine Buckets
- How it works: Quarantine bad assets by failure class (truncated file, checksum mismatch, decode mismatch) and auto-retry only compatible classes.
- User perception: Fewer repeated failures on the same bad episode.
- Implementation: New quarantine table or columns in `AnalysisStore`; wire into admission gating.

9. Jittered BG Task Resubmission Policy
- How it works: Add deterministic jitter and cooldown to BG task resubmits to reduce burst collisions.
- User perception: Better background completion consistency.
- Implementation: `BackgroundProcessingService` + `BackgroundFeedRefreshService` submit wrappers.

10. End-to-End Pipeline SLO Gates
- How it works: Declare SLOs (first skip latency, false-ready rate, skip precision) and fail CI integration plans when regressions exceed thresholds.
- User perception: Higher release reliability.
- Implementation: Add SLO assertions in replay/integration harness; publish metrics artifacts.

11. Multi-Resolution Feature Window Cache
- How it works: Store coarse + fine acoustic windows so hot path can use coarse features quickly, refine later.
- User perception: Faster responsiveness with minimal quality loss.
- Implementation: Extend `FeatureExtractionService` and `AnalysisStore` schema version.

12. MainActor Update Coalescing for Runtime/UI
- How it works: Batch high-frequency observation updates (playback position, progress) into controlled intervals.
- User perception: Smoother UI and better battery on long sessions.
- Implementation: Coalescing buffers in view models + runtime publisher seams.

13. Priority Inversion Guard Between Lanes
- How it works: Detect and prevent lower-priority work from blocking now-lane operations.
- User perception: Immediate actions stay immediate.
- Implementation: Extend `LanePreemptionCoordinator` with wait diagnostics and forced yielding.

14. Sponsor Lexicon Confidence Aging
- How it works: Decay stale sponsor terms and boost recently corroborated terms.
- User perception: Fewer old false positives.
- Implementation: Add temporal weights in `SponsorKnowledgeStore` and lexical scanner weighting.

15. Per-Show Acoustic Signature Cache
- How it works: Persist recurring intro/music-bed fingerprints by show for faster boundary proposals.
- User perception: Better boundary accuracy over repeated listens.
- Implementation: Extend `MusicBedLedgerEvaluator` + persistent store columns.

16. Episode Readiness Timeline + ETA in Activity
- How it works: Surface readiness stage, bottleneck cause, and ETA per episode.
- User perception: Transparent progress; less confusion about "is it working?".
- Implementation: Derive ETA from scheduler telemetry; map through `EpisodeSurfaceStatusObserver` to `ActivityViewModel`.

17. "Why Skipped" Explainability Panel
- How it works: One tap on banner/timeline reveals compact evidence (lexical/acoustic/FM provenance + confidence band).
- User perception: Builds trust and reduces perceived randomness.
- Implementation: Expose decision artifact from `SkipOrchestrator` + `AdDetectionService` to UI-safe DTO.

18. One-Tap Per-Show Skip Posture Dial (Strict / Balanced / Aggressive)
- How it works: User policy preference shifts thresholds around existing trust gating.
- User perception: Control without complexity.
- Implementation: Add per-show policy modifier in SwiftData + policy matrix adjustment in `SkipPolicyMatrix`.

19. First-Run Smart Defaults by Device Class
- How it works: Configure conservative/normal/aggressive analysis defaults from `DeviceClass` + battery mode.
- User perception: Better out-of-box behavior without settings digging.
- Implementation: Seed `UserPreferences` through `CapabilitiesService` snapshot.

20. Quiet Failure Surfacing (No Alerts)
- How it works: Add subtle status chips in Activity for failures/retries rather than disruptive modal errors.
- User perception: Less anxiety, still informed.
- Implementation: `SurfaceStatus` copy templates + `ActivityView` UI chips.

21. Predictive Up-Next Preanalysis
- How it works: Start lightweight analysis for likely next episode while on Wi-Fi/charging.
- User perception: Faster skip readiness when starting next episode.
- Implementation: Use queue/order signals from Activity + scheduler low-priority lane.

22. Recovery Center in Settings
- How it works: Central place to inspect stuck jobs, clear one episode's analysis state, and retry safely.
- User perception: Self-service recovery instead of reinstalling app.
- Implementation: Build on diagnostics + analysis store actions with safeguards.

23. Transcript-Peek Correction Shortcuts
- How it works: Let user mark false positive / missed ad directly from transcript context.
- User perception: Faster correction with less friction.
- Implementation: Route to `UserCorrectionStore` and `BoundaryExpander` from `TranscriptPeekViewModel`.

24. Sponsor Memory Cards (Local)
- How it works: Optional per-show sponsor rollup from locally observed ads.
- User perception: Useful context and "what got skipped" visibility.
- Implementation: Read from ad catalog/fingerprint stores, render in Activity.

25. High-Speed Playback Safeguard Mode
- How it works: Automatically widen lookahead and relax non-critical background work at >=2x.
- User perception: Fewer misses at high speed.
- Implementation: Scheduler policy branch from playback rate stream.

26. Accessibility-First Transport Ergonomics
- How it works: Larger adaptive touch targets + consistent haptics + richer VoiceOver timeline narration.
- User perception: More usable for all listeners, especially motion/vision constraints.
- Implementation: View refinements + haptic policy unification via `HapticManager`.

27. Storage Pressure Guardrails + Guided Cleanup
- How it works: Early warning + one-tap cleanup recommendations (old transcripts, stale caches, failed assets).
- User perception: Less surprise when storage runs low.
- Implementation: Extend `StorageBudget` + settings cleanup actions with projected savings.

28. Trust Heatmap on Timeline
- How it works: Subtle confidence tinting for upcoming candidate spans.
- User perception: Understands uncertainty before skip happens.
- Implementation: UI mapping from confidence band; gate for advanced users.

29. Local Weekly Value Report
- How it works: Private on-device summary of time saved, corrected skips, and confidence trend.
- User perception: Reinforces product value and trust trajectory.
- Implementation: Aggregate from decision logs and corrections; render in Activity.

30. One-Tap Repro Bundle for a Single Episode
- How it works: Export sanitized per-episode diagnostics package with pipeline timeline and causes.
- User perception: Easier to report bugs with minimal effort.
- Implementation: Extend diagnostics exporter with episode-scoped bundle presets.

## Winnowing Method

I ranked ideas on:
- Reliability / robustness impact
- User-visible value
- Fit with current architecture
- Implementation risk and size
- Overlap with already-open beads (prefer additive, not duplicate)

## Top 5 (Best -> Worst)

## 1) Episode Pipeline Watchdog + Auto-Heal

Why this is #1:
- Playhead's biggest trust killer is silent or prolonged partial failure ("episode never becomes ready" or "analysis stalls").
- This directly raises reliability across all features, not just one screen.
- It complements existing work-journal + lease architecture instead of replacing it.

How it works:
- Define per-stage heartbeat SLAs:
  - decode heartbeat
  - transcript heartbeat
  - detection heartbeat
  - skip cue materialization heartbeat
- If heartbeat misses SLA, watchdog executes a bounded recovery ladder:
  1. Re-kick stage task with same lease
  2. Re-open stage with fresh lease and preserved progress cursor
  3. Downgrade episode to recoverable status + surface explicit cause
- Record each escalation in persistent journal for deterministic post-mortem and replay tests.

User perception:
- Fewer "stuck" episodes.
- Faster autonomous recovery after interruptions/background churn.
- More trustworthy Activity states because stalls are converted into explicit recoverable states.

Implementation in Playhead:
- Add `PipelineWatchdog` actor owned by `PlayheadRuntime`.
- Feed it from:
  - `AnalysisWorkScheduler` lane ticks/outcomes
  - `AnalysisJobRunner` stage completion callbacks
  - `AnalysisCoordinator` state transitions
- Persist escalation counters/reasons in `AnalysisStore` (work journal extension).
- Map watchdog outcomes to `SurfaceStatus` via `EpisodeSurfaceStatusObserver` + `CauseTaxonomy`.
- Add replay fixtures for stalled-heartbeat scenarios.

Why I'm confident:
- Existing architecture already has the required seams: leases, work journal, cause taxonomy, scheduler admissions.
- This is mostly control-plane logic; low risk to core inference paths.
- Reliability gains are immediate and measurable (stall rate, mean recovery time).

## 2) Unified Resource Governor (Battery/Thermal/Storage/Network)

Why this is #2:
- Right now resource decisions are distributed across services; a central governor prevents contradictory behaviors.
- It improves reliability and battery simultaneously.
- It creates a single truth source for admission decisions and user-facing expectations.

How it works:
- New actor computes a live `ResourceBudgetProfile` every few seconds and on critical events.
- Inputs:
  - `CapabilitiesService` snapshot (thermal, low power)
  - `StorageBudget` pressure
  - transport/network conditions
  - foreground/background phase
- Outputs policy knobs:
  - max hot-zone seconds
  - backfill enablement
  - download concurrency
  - FM backfill budget
  - retry aggressiveness

User perception:
- Better battery behavior on long listening days.
- Fewer thermal slowdowns and abrupt pauses.
- More consistent app responsiveness under stress.

Implementation in Playhead:
- Add `ResourceGovernor` actor in `Services/PreAnalysis` or `Services/Capabilities`.
- Thread profile into:
  - `AnalysisWorkScheduler` admission and lane sizing
  - `TranscriptEngineService` pass scheduling
  - `DownloadManager` concurrent transfer cap
  - `AdDetectionService` optional backfill/FM budgets
- Expose compact state to `SurfaceStatus` so UI can explain "deferred due to battery/thermal" accurately.

Why I'm confident:
- Existing code already has all raw signals; this consolidates policy rather than inventing new infrastructure.
- Strongly aligned with open beads like storage/admission work.
- High payoff, moderate effort, low product risk.

## 3) Adaptive Hot-Zone Sizing by Measured Throughput

Why this is #3:
- First-skip usefulness depends on staying ahead of playback.
- Static lookahead is inherently suboptimal across devices, rates, and episode complexity.
- This gives direct performance improvement where users notice it.

How it works:
- Continuously estimate effective pipeline throughput:
  - decode seconds/sec
  - transcript seconds/sec (fast pass)
  - detection latency/sec window
- Compute safety margin ratio versus current playback rate.
- Adapt lookahead target dynamically:
  - expand when throughput is healthy
  - contract to protect near-playhead freshness when stressed

User perception:
- Faster and more consistent skip readiness, especially at 1.5x-3x.
- Fewer late detections and fewer "it skipped too late" impressions.

Implementation in Playhead:
- Add estimator inside `AnalysisWorkScheduler` instrumentation path.
- Use estimator output in `CandidateWindowSelector` and lane admission.
- Feed back into `TranscriptEngineService` prioritization windows.
- Persist anonymized local metrics for replay-harness regression checks.

Why I'm confident:
- Builds on scheduler primitives that already exist.
- No model changes required.
- Easy to A/B internally through replay harness before full rollout.

## 4) Episode Readiness Timeline + ETA in Activity (SurfaceStatus-Powered)

Why this is #4:
- Reliability is only half the story; users need to understand progress.
- Playhead already has strong internal status taxonomy that can be surfaced more effectively.
- This reduces confusion and support burden with minimal engine risk.

How it works:
- For each episode, show:
  - current stage (Queued, Spooling, Hot-path ready, Backfill, Complete)
  - bottleneck cause (charging required, low battery defer, waiting for download, etc.)
  - ETA band (e.g., "~2-4 min to skip-ready")
- ETA derives from recent per-device throughput + backlog size.

User perception:
- Clear mental model of "what's happening now" and "when will it be ready".
- Less perceived flakiness because waiting has explanation.

Implementation in Playhead:
- Extend `EpisodeSurfaceStatusObserver` output with ETA hints.
- Add `ReadinessEstimateProvider` in Activity pipeline (likely near `ActivitySnapshotProvider`).
- Render in `ActivityView` with quiet, non-alerting design.
- Keep copy aligned to `SurfaceReasonCopyTemplates` and cause taxonomy.

Why I'm confident:
- Uses already-built observability surfaces.
- Mostly UI + projection logic; low risk to playback/detection loops.
- Immediate UX win with measurable reduction in repeated manual refresh/check behavior.

## 5) One-Tap Per-Show Skip Posture Dial + "Why Skipped" Evidence Panel

Why this is #5:
- Trust is personal; some users prefer conservative skipping, others aggressive.
- Explainability plus control substantially improves confidence in automatic behavior.
- This is high product value but slightly lower than top 4 because it is more UX/policy than core reliability.

How it works:
- Add per-show posture presets:
  - Strict: higher thresholds, fewer auto-skips
  - Balanced: current behavior
  - Aggressive: lower thresholds with guardrails
- Add compact "Why skipped" panel from banner/timeline:
  - dominant evidence category
  - confidence band
  - correction shortcuts (listen / not ad)

User perception:
- Feels transparent and customizable instead of opaque.
- Users can tune behavior without understanding internal scoring math.

Implementation in Playhead:
- Store per-show posture in SwiftData episode/podcast preferences.
- Adjust `SkipPolicyMatrix` and possibly `TrustScoringService` with posture multiplier.
- Expose sanitized decision artifact DTO from `SkipOrchestrator`/`AdDetectionService` to UI.
- Reuse existing correction pathways (`UserCorrectionStore`) from panel actions.

Why I'm confident:
- Architecture already separates policy from detection, which is exactly what this needs.
- Existing open issues on skip usefulness/visualization indicate strong strategic fit.
- Incremental rollout is easy: start with posture dial, then add evidence panel.

## Why these 5 beat the other 25

- They compound together: #1 reliability backbone, #2 global resource coherence, #3 near-playhead performance, #4 user transparency, #5 trust/control.
- They require no cloud services and no architectural rewrite.
- They leverage existing strengths in Playhead (actor boundaries, telemetry, surface status, replay harness) rather than fighting the current design.

