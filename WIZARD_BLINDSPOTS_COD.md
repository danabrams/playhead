# Playhead Background Queue - Blind Spots Neither Side Covered (Codex)

Date: 2026-04-16

These are angles that were largely missing from both idea sets: not just "new feature ideas," but system-shaping levers for making "ready before open" reliable on real iOS devices.

## 1) BG Grant-Window Prediction + Slice Sizing Controller

Blind spot:
- Both sides discussed BGTask APIs, but neither treated iOS background execution as a stochastic resource that should be modeled explicitly per device.

Core idea:
- Learn a per-device distribution of granted background run windows (duration, expiration frequency, thermal interruption patterns) and dynamically size work slices to fit within a high-probability completion window.

How it works:
- Persist each BG task run: start time, granted duration, whether expiration handler fired, thermal/power state, work completed.
- Train a tiny on-device estimator (rules or lightweight regression) predicting safe work budget for the next invocation.
- Scheduler chooses the largest stage bundle with >=95% completion probability (e.g., 2 shards transcribe + lexical scan, but defer FM pass).

Why this matters:
- "Ready before open" fails most often from repeated partial work that never completes inside OS-granted windows.
- This directly improves completion rate, not just queue policy quality.

Complexity:
- Moderate; mostly instrumentation + policy engine, no new ML-heavy dependency.

---

## 2) Playhead-Proximal Partial Readiness (Sub-Episode Critical Path)

Blind spot:
- Most plans optimize episode-level readiness. Users experience readiness at the current playhead, not at episode granularity.

Core idea:
- Prioritize analysis of the first likely ad regions the user will hit soon (e.g., first 12-20 minutes plus known break priors), then backfill the rest.

How it works:
- Split each episode into priority zones: `front`, `expected mid-roll windows`, `tail`.
- On constrained background windows, run `download -> decode -> transcribe -> classify` for priority zones first.
- Mark episode state as `Ready Near Playhead` before full completion.

Why this matters:
- It creates perceived magic faster: users get useful skip markers where they are about to listen, even if full episode processing is incomplete.

Complexity:
- Moderate; requires zone-aware scheduling and UI state, but uses existing pipeline stages.

---

## 3) Repeated-Ad Tile Memoization Across Episodes (Inference Reuse)

Blind spot:
- Both sides focused on better detection, not on aggressively reusing previous expensive inference for repeated inserted ad audio.

Core idea:
- Build a cache of short audio tile fingerprints (e.g., 2-5s rolling hashes) for high-confidence ad segments; when matched later, reuse prior transcript/classification/boundary data instead of re-running full ASR+FM.

How it works:
- During completed analyses, store normalized fingerprints + validated span metadata.
- On new episodes, run fast tile scan over candidate windows.
- If strong match chain appears, seed span with cached evidence and only verify boundaries/context.

Why this matters:
- Dynamically inserted ads are frequently reused across shows/episodes.
- Reusing inference cuts background compute time and energy substantially, increasing pre-open readiness rates.

Complexity:
- Medium-high (fingerprint robustness and collision control), but potentially very high ROI.

---

## 4) Model-Update Fast Revalidation via Feature Persistence

Blind spot:
- Neither side addressed lifecycle shock when ASR/FM versions or detection logic change and previously analyzed episodes become stale.

Core idea:
- Persist model-agnostic intermediate features so model/policy updates can re-run only the cheap late stages rather than full re-transcription.

How it works:
- Persist reusable artifacts: shard timing map, tokenized transcript units, lexical hits, acoustic boundary features, candidate spans.
- Version these artifacts independently from FM/policy versions.
- On model/policy update, run "revalidate from features" job to refresh skip markers quickly.

Why this matters:
- Keeps library readiness high after updates without re-burning battery on full pipeline reruns.
- Reduces tech-debt risk from model evolution.

Complexity:
- Medium; mainly schema/version discipline and replay orchestrator logic.

---

## 5) Explicit Readiness SLO + Closed-Loop Queue Control

Blind spot:
- Both sides proposed heuristics, but neither defined a measurable service objective with automated control logic.

Core idea:
- Treat readiness as an SLO problem: e.g., "Top 25 likely-to-play episodes are playhead-ready within X hours of publish at least 90% of the time." Use this objective to tune scheduling and resource policy continuously.

How it works:
- Define SLO metrics and error budget on-device.
- Continuously measure misses by reason (no BG grant, thermal throttle, network unavailable, storage eviction, pipeline failures).
- Controller adjusts knobs: lane weights, concurrency, slice size, charging-only rules, eviction aggressiveness.

Why this matters:
- Converts static queue heuristics into an adaptive system tied to the actual product promise.
- Creates operational clarity: you can tell if "magical readiness" is truly improving.

Complexity:
- Moderate; high design payoff because it aligns all subsystems to one measurable outcome.

---

## Why these are true blind spots

Neither side strongly covered:
- probabilistic BG window modeling,
- playhead-proximal partial readiness as first-class target,
- inference reuse for repeated ad audio,
- post-update fast revalidation,
- explicit SLO-driven adaptive control.

These five ideas complement the earlier proposals: they focus on throughput reliability and perceived readiness, not only detection quality.
