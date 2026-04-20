# Podcast Background Downloads & On-Device Processing Plan

*Consolidated planning memo based on the attached Dueling Idea Wizards Report, the follow-up constraint that all detection must happen on device, and a UI brainstorm captured in `docs/plans/2026-04-16-podcast-bg-ui-design.md`.*

*Prepared for planning • April 16, 2026 • Revision 3 (post-GPT-Pro Round 2 review)*

*Sources:*
- *Dueling Idea Wizards Report (`DUELING_WIZARDS_REPORT.md`)*
- *UI Design Doc (`docs/plans/2026-04-16-podcast-bg-ui-design.md`)*

> **Planning constraint:** All detection must happen on device for legal reasons. This plan assumes local-first artifacts, resumable background work, and privacy-safe measurement.

---

## Executive Summary

- **Treat the app as an incremental on-device inference system, not a downloader with background jobs.**
- **Optimize for playhead-ready coverage before full episode-wide readiness;** partial readiness is the right product contract under best-effort background scheduling.
- **Build around bounded, resumable work slices** with local checkpoints, device-class-aware slice sizing, and hard user preemption.
- **Pull deterministic metadata signals forward** because RSS text, show notes, and chapter data are cheap, local, and legally clean.
- **Move measurement forward:** define readiness SLOs, miss reasons, and privacy-safe telemetry before tuning heuristics.
- **The user-facing vocabulary is intentionally tiny.** Three verbs (Subscribe, Download, Add to Up Next) and two badges (⤓ Downloaded, ✓ Skip-ready). Analysis is implicit on download — never an explicit user action. See §5.
- **Most "obvious" engineering already exists in the codebase.** The work in this plan is mostly the *system properties* (resumability, slice sizing, miss-reason measurement, persisted features, eviction policy, UI contract) that turn a working pipeline into a reliable product. See §1.

---

## 1. Existing Foundation (codebase ground truth)

This section captures what is already shipped so the rest of the plan can avoid re-specifying it. Sources: `Playhead/Services/AnalysisCoordinator/`, `Playhead/Services/AdDetection/`, `Playhead/App/PlayheadRuntime.swift`, `Playhead/Services/PreAnalysis/AnalysisWorkScheduler.swift`, `Playhead/Services/AdDetection/ChapterEvidenceParser.swift`, and ef2.x / Phase 3–7 git history.

| Area | Shipped | Notes |
|---|---|---|
| **BackgroundTasks framework wiring** | ✅ | `BGProcessingTask` (`com.playhead.app.analysis.backfill`), `BGContinuedProcessingTask` (`com.playhead.app.analysis.continued`), recovery (`com.playhead.app.preanalysis.recovery`). Registered at app launch. |
| **Pipeline state machine** | ✅ | `AnalysisCoordinator.SessionState`: `queued → spooling → featuresReady → hotPathReady → backfill → complete`. |
| **Persistence (SQLite + SwiftData)** | ✅ | `AnalysisStore` tables: `AnalysisAsset`, `AnalysisSession`, `TranscriptChunk`, `FeatureWindow`, `FeatureExtractionCheckpoint`. Episode model carries `AnalysisSummary` (denormalized counts). |
| **Thermal / energy governance** | ✅ | `ProcessInfo.thermalState` + `UIDevice.batteryState/Level` + `isLowPowerModeEnabled`. Profiles: nominal/fair = full; serious = reduce window, pause backfill; critical = pause all. <20% battery & not charging = pause non-critical. |
| **LexicalScanner** | ✅ | Sponsor phrases, promo codes, URLs, purchase language, transition markers. Configurable mergeGap, minHits, highWeight bypass. |
| **Foundation Models classifier** | ✅ | iOS 26 FoundationModels API. Schema-trimmed for token budget (CoarseScreeningSchema, SpanRefinementSchema). Phase 3-hardened. |
| **Boundary refinement stack** | ✅ | `BoundaryExpander`, `BoundaryRefiner`, `FineBoundaryRefiner`, `TimeBoundaryResolver`, `BoundaryPriorStore`, `AsymmetricSnapScorer`. |
| **Music-bed detection** | ✅ | `MusicBedClassifier`, `MusicBoundaryEvaluator`, `MusicBracketTrustStore`. |
| **Evidence fusion** | ✅ | `EvidenceCatalogBuilder`, `BackfillEvidenceFusion`, `AtomEvidenceProjector` (Phase 5 DEBUG observer), `EVIScorer`, `TrustScoringService`, `SpanDecision`, `SpanFinalizer`. |
| **Chapter / RSS / iTunes parsing** | ✅ | `ChapterEvidenceParser` handles ID3 CHAP, Podcasting 2.0 JSON, RSS inline. Episode model persists `feedDescription`, `feedSummary`, source hashes (shadow-mode only today). |
| **Ownership / network / sponsor graphs** | ✅ | `OwnershipGraph` (eTLD+1), `SponsorEntityGraph`, `NetworkIdentity`, `RecentFeedSponsorAtlas`. |
| **User correction loop (Phase 7)** | ✅ | `UserCorrectionStore`, `FoundationModelsFeedbackStore`, `CorrectionSource` enum. |
| **Decision persistence (Phase 6)** | ✅ | `DecisionResultArtifact`, `DecisionEvent` (append-only). |
| **Shadow retry (Phase 4)** | ✅ | `needsShadowRetry` flag + capability observer for FM model availability changes. |
| **Gray-band markOnly playback UX (ef2.6.3)** | ✅ | Confidence band classification + scrubber marker treatment + auto-skip for high-confidence. |

### What is *not yet built* (and is therefore in scope for this plan)

| Gap | Status | Phase target |
|---|---|---|
| **Analysis eligibility contract (hardware / Apple Intelligence / region / language / current model availability)** | ❌ | Phase 0 |
| **Background URLSession for durable transport** | ❌ — uses standard async download | Phase 1 |
| **EpisodeExecutionLease + WorkJournal (single-writer-per-episode)** | ❌ | Phase 1 |
| **Three-lane priority scheduler with explicit lane labels** | ⚠️ partial — `AnalysisWorkScheduler` exists, multi-lane structure unclear | Phase 1 |
| **Hard user preemption at safe checkpoint boundaries** | ⚠️ partial | Phase 1 |
| **Multi-resource scheduling (transport + CPU + storage + thermal)** | ⚠️ partial | Phase 1 |
| **Device-class-aware slice sizing (B1)** | ❌ | Phase 1 |
| **BG grant-window prediction (B1)** | ❌ | Phase 1 |
| **Playhead-proximal partial readiness (B2)** | ❌ | Phase 2 |
| **Per-show capability profiles** | ❌ | Phase 3 |
| **Storage budgets + eviction (media cap + features cap)** | ❌ | Phase 0 / 1 |
| **Two-tier retention (delete media, keep features)** | ❌ | Phase 0 |
| **Repeated-ad tile memoization (B3)** | ❌ | Phase 3 |
| **Fast revalidation from persisted features (B4)** | ⚠️ partial via shadow retry | Phase 3 |
| **Readiness SLO + miss-reason taxonomy + closed-loop control (B5)** | ❌ | Phase 0 / 2 |
| **Episode-level `playbackReadiness` state (none/deferredOnly/proximal/complete + coverageRange)** | ❌ — only `AnalysisSummary` aggregate counts exist | Phase 2 |
| **Activity screen** | ❌ | Phase 2 |
| **"Download Next N" bulk affordance** | ❌ | Phase 2 |
| **Storage / Diagnostics settings surfaces** | ❌ | Phase 2 |
| **Trip-readiness notifications** | ❌ | Phase 2 |
| **Privacy-safe telemetry envelope (legal-approved)** | ❌ | Phase 0 |

**Implication for plan structure:** Phases 0–3 below are scoped exclusively to the gaps above. Existing infrastructure is treated as a foundation to extend — never to rebuild.

---

## 2. Planning Constraint and Its Implications

For legal reasons, all audio-derived detection must occur on device. That removes server-assisted detection from the design space and changes what is foundational.

The queue is no longer only a delivery mechanism. It is the runtime for a local analysis pipeline that must tolerate interruption, resume safely, and still produce useful partial results.

- **Treat transcript, fingerprint, embedding, acoustic-feature, candidate-span, and boundary artifacts as local-first by default.** (Already true per §1; the plan formalizes the *contract*.)
- **Assume background execution is opportunistic rather than guaranteed;** every unit of work should be resumable and useful on its own.
- **Limit uploaded telemetry to coarse operational counters and timings** unless legal explicitly approves richer signals.

### What changed from the original Dueling Wizards report

| Area | Planning change |
| --- | --- |
| Server-assisted detection | Removed as an option for now. All detection design assumes on-device execution only. |
| Stage ledger | Promoted into a local artifact/version graph with idempotent checkpoints and model/policy versioning. |
| Readiness score | Reframed from a single score into explicit states: Downloaded, Skip-ready, Paused. (See §5 for the user-facing form.) |
| Blind spots B1/B2/B4/B5 | Promoted from later optimization to foundational work because on-device processing lives or dies on slice sizing, partial readiness, persistence, and measurement. |
| Music-bed and host-voice work | Pushed later. Useful in selected cases, but not on the critical path for a reliable MVP. (Note: music-bed is already shipped per §1; the *deferral* is on universal-show generalization.) |
| User intent vocabulary | Collapsed to three verbs (Subscribe, Download, Add to Up Next). Analysis is implicit on download — there is no "Prepare" verb. |

---

## 3. Recommended Architecture

The recommended design has four interacting parts: an **execution plane**, a **staged local detection cascade**, a **persistent local artifact model**, and a **multi-resource scheduler**.

### Execution Plane

| Component | Recommended role | Planning note |
| --- | --- | --- |
| **Background URLSession** | Durable transport plane for media downloads | **NEW** — replace current standard async download. Maintain separate network policies for interactive vs maintenance work. |
| **BGAppRefreshTask** | Feed polling and queue nudges only | Keep light: discover new episodes, refresh priorities, resubmit work. **Not** the vehicle for expensive per-episode analysis. |
| **BGProcessingTask** | Bounded on-device inference slices | Already wired (`com.playhead.app.analysis.backfill`). Add device-class-aware slice sizing (B1). |
| **BGContinuedProcessingTask** | Immediate prep after explicit user action | Already wired (`com.playhead.app.analysis.continued`). Used for "Download" tap → assist analysis through backgrounding. |
| **Foreground assist path** | In-app analysis while user is browsing | Best-effort opportunistic work while app is foreground. |

**Cross-cutting concurrency contract (NEW):** All four execution planes above can touch the same episode. Add an `EpisodeExecutionLease` + append-only `WorkJournal` between transport and analysis, **and a lightweight global `SchedulerEpoch` above them**:
- one analysis writer per episode at any time
- explicit generation IDs on resume / retry
- orphan recovery on cold launch
- idempotent finalize / eviction transitions
- **atomic lane/resource decisions across episodes (`SchedulingPass`)** — cross-episode promotions/demotions/admissions commit under a monotonic `SchedulerEpoch` so partial crashes don't leave half-applied scheduling decisions
- every lease records the `schedulerEpoch` under which it was admitted
- stale-epoch workers yield at the next safe checkpoint and requeue

Without this, the failure modes are silent and ugly: duplicate analysis, checkpoint corruption, badge drift, retained-bundle skew, or cross-episode scheduler inconsistency (scheduler promotes X and demotes Y, then crashes after persisting only half the decision; late callbacks resurrect work that should now be demoted). High blast radius, low likelihood of being noticed early. The per-episode lease solves intra-episode races; `SchedulerEpoch` solves queue-wide crash-consistency — both are required.

### On-Device Detection Cascade

The practical goal is not "analyze the whole episode immediately." The goal is to produce useful readiness quickly, then backfill the rest.

| Order | Stage | Goal | Status |
| --- | --- | --- | --- |
| 1 | Metadata parse | Extract sponsor names, URLs, promo codes, show-note hints at enqueue time. | ✅ shadow-mode (per §1); promote to live signal. |
| 2 | Chapter ingestion | Treat publisher-declared sponsor chapters as high-confidence positives, content chapters as strong negatives. | ✅ implemented (`ChapterEvidenceParser`). |
| 3 | Cheap lexical/acoustic scan | Identify candidate windows; avoid whole-episode ASR when lighter evidence is enough. | ✅ implemented (`LexicalScanner`, feature extraction). |
| 4 | **Candidate-window ASR + first listening window (B2)** | **NEW** — Prioritize the next 12–20 minutes and likely mid-roll windows to reach Skip-ready quickly. | ❌ in scope for Phase 2. |
| 5 | Expensive classification on uncertainty hotspots | Spend heavier model budget only where cheap path is uncertain or boundary accuracy will matter soon. | ✅ implemented (FM classifier with schema-trimmed coarse + refinement). |
| 6 | Finalize local skip map | Persist a versioned output that playback can trust and revalidate cheaply after policy or model changes. | ✅ implemented (`DecisionResultArtifact`); add B4 fast-revalidation in Phase 3. |

### Local Persistence Model

Persisting intermediate artifacts locally is the difference between a system that resumes and one that redoes work. The persistence model includes:

- Raw media + shard manifest *(already)*
- Shard timing map + checkpoint cursor *(`FeatureExtractionCheckpoint` exists)*
- Partial transcript tokens / segments *(already in `TranscriptChunk`)*
- Lexical hits + sponsor lexicon evidence *(already)*
- Acoustic features + candidate spans *(already in `FeatureWindow`)*
- Final boundary decisions + confidence *(already in `DecisionResultArtifact`)*
- **Model version, policy version, feature-schema version on every artifact** — needed for B4 revalidation. Audit current schema; add version columns where missing.

**Retention policy — three internal storage classes (REVISED):**
- **`media`** — audio payload, largest, evictable under media cap.
- **`warmResumeBundle`** — decision map, coverage map, transcript segments/tokens, fingerprints, compact feature summaries, version stamps. **The differentiated asset.** Retained when media is evicted; enables instant Skip-ready on re-download (B4 enabler).
- **`scratch`** — ephemeral partial ASR shards, intermediate windows, temporary fusion artifacts. Discardable independently at any time.

When media is evicted under storage cap, retain only `warmResumeBundle` (~1% of media size). `scratch` can be discarded eagerly under any pressure. This three-way split lowers analysis-cap thrash, makes B4 revalidation cleaner, and prevents bulky version-sensitive intermediates from being retained just because they once lived under "features."

### Scheduler Model

| Policy area | Recommendation | Status |
| --- | --- | --- |
| Priority lanes | Three lanes: **Now** (currently playing), **Soon** (recent download requests), **Background** (subscription auto-downloads). Internal names — never surfaced to users. | ⚠️ partial |
| Resources | Schedule transport, CPU, storage, thermal budget separately under one policy engine. | ⚠️ partial |
| Preemption | User-started work always wins. Pause lower-priority work only at safe checkpoint boundaries. | ⚠️ partial |
| Slice sizing | Learn shard sizes and task budgets per device class (B1). Target: ≥95% slice-completion probability inside granted windows. | ❌ |
| Storage | Two budgets: **media cap** (user-chosen, default 10 GB) and **analysis cap** (default 200 MB). Admission control before starting expensive work. | ❌ |
| Network | Interactive download class for explicit user actions; maintenance class for subscription upkeep. | ❌ |

---

## 4. Revised View of the Original Ideas

The original report was directionally strong. The revisions are: (a) the order of operations — on-device-only detection makes persistence, slice sizing, playhead-first readiness, and measurement more important than classifier sophistication; (b) several "ideas" already exist in code and don't need re-spec'ing.

| Idea | Disposition | Planning note |
| --- | --- | --- |
| Two-tier background orchestration | Keep, but narrow each layer's role | Refresh for feed/queue management; processing for bounded slices; **Background URLSession for durable downloads (NEW)**. |
| Three-lane priority scheduler | Keep, upgrade | Implement as multi-resource scheduler with hard user preemption and device-aware slice sizing. Lanes are scheduler-internal vocabulary; user surfaces use Download / Skip-ready states only. |
| Durable episode DAG + checkpoint ledger | Already 80% built; finish the model-version fields | The state machine and checkpoints exist. Add `model_version`, `policy_version`, `feature_schema_version` columns where missing for B4. |
| Energy / thermal governor | Already built; codify as explicit policy | Profiles already implemented; surface them as a `QualityProfile` enum the scheduler reads instead of scattered guardrails. |
| RSS / show-notes sponsor pre-seeding | Promote to live signal | Currently shadow-mode (`feedDescription`/`feedSummary` persisted). Wire into fusion at the corroboration gate (cap contribution at 0.15 of fusion budget per Wizards report). |
| Chapter-marker ingestion | Already built | `ChapterEvidenceParser` covers ID3 CHAP, Podcasting 2.0, RSS inline. No further work in this plan. |
| Music-bed boundary cue | Already built; defer universal-show generalization | `MusicBedClassifier` exists. Generalization to recurring-jingle detection across shows is later optimization. |
| Readiness score + "Ready to Skip" | Replace with explicit state contract | See §5 — three states (⤓ Downloaded, ✓ Skip-ready, ⏸ Paused-with-reason). No score. |
| Host-voice counter-classifier | Defer | Too much infrastructure and false-positive risk for current phase. No diarization stack exists. |
| Inventory sanity check | Keep lightweight | Helpful guardrail; not a readiness driver. |
| **B1: grant-window prediction + slice sizing** | Promote to core | Phase 1. Without empirical slice sizing, background work fails to finish. |
| **B2: playhead-proximal partial readiness** | Promote to core | Phase 2. Fastest way to perceived quality under on-device constraints. |
| **B3: repeated-ad tile memoization** | Local-only | Phase 3. Frame as local reuse; no shared-cloud intelligence. |
| **B4: fast revalidation from persisted features** | Promote | Phase 3. Essential for model/policy updates. Builds on shipped Phase 4 shadow retry. |
| **B5: readiness SLO + closed-loop control** | Promote | Phase 0/2. SLO + miss-reason enum is the load-bearing measurement contract for the whole system. |

---

## 5. UI Contract

*Condensed from the full UI design in `docs/plans/2026-04-16-podcast-bg-ui-design.md`. The plan-of-record is the design doc; this section is the integration touch-point with the rest of the plan.*

### Mental model: progressive disclosure

Casual users see a calm ambient surface. Power users (and engineering / support) get a dedicated **Activity** screen and a **Settings → Diagnostics** drawer. Same UI scales by depth-of-engagement; no dual-mode toggle.

### Intent vocabulary (three verbs, no "Prepare")

| Verb | User says | System does |
|---|---|---|
| Subscribe | "I follow this show" | Per-subscription policy decides if new episodes auto-download |
| Download | "I want this offline" | Fetch media. Analysis runs in background as capacity allows. |
| Add to Up Next | "Play this after current" | Pure ordering; doesn't imply download |
| Play | "Now" | If not downloaded, stream + opportunistic analysis |

**Analysis has no explicit verb.** It's a property of being downloaded, not a separate intention.

### Readiness state contract (the single source of truth)

| State | Cell badge | Meaning |
|---|---|---|
| Not downloaded | (none) | Default. |
| Downloaded | ⤓ | Audio is offline. **No claim about skips.** |
| Skip-ready | ✓ | The next listening window is covered well enough that playback can make the skip promise now. (`playbackReadiness ∈ {proximal, complete}`) |
| Deferred-only | (no badge — stays as ⤓) | Ads found, but not within the next listening window. Library cell does **not** show ✓; episode detail status line explains. (`playbackReadiness == deferredOnly`) |
| Paused | ⏸ (with reason) | Analysis halted by transient cause (thermal, low power, storage cap, etc.). |
| Analysis unavailable | (no badge — stays as ⤓) | Foundation Models / Apple Intelligence not available on this device, region, or language. **Not** modeled as Paused. Surfaced in detail + Activity + Diagnostics. |
| Listened | (faded cell) | Already played. |

**Vocabulary rules:**
- "Skip-ready" is a *playback promise for the next listening window*, not a report that some later ad exists. Coarse on purpose.
- "Downloaded" never implies analysis state. "Deferred-only" and "Analysis unavailable" both stay as ⤓ in the library — only the detail view discloses why.
- "Analyzing" is never a library-cell word. Activity + episode detail only.
- Scheduler lane names (Now / Soon / Background) are internal — never surfaced.
- "Paused" always carries its reason and what unblocks it. **Permanent ineligibility (no Apple Intelligence, unsupported language/region) is *not* modeled as Paused** — it's its own surface state.
- **Live Activity carve-out:** `BGContinuedProcessingTask` work surfaces a system-managed Live Activity (progress + cancel). We don't suppress it; we author the displayed copy to match our state vocabulary.

### User-facing surfaces

1. **Library cell** — icon-only state badge, no text label.
2. **Episode detail view** — adds a single status line ("Skip-ready · first 18 min · analyzing remainder") under the header.
3. **Player** — already shipped (ef2.6.3 gray-band markOnly UX). Adds an "Always skip this sponsor" button on the Skipped banner (Phase 7 infra).
4. **"Download Next N" bulk affordance** on the show page (delight feature for flights / commutes).
5. **Activity screen** — Now / Up Next / Paused (with reasons — the B5 miss-reason taxonomy lives here) / Recently Finished. Reached via Library nav button.
6. **Settings → Storage** — user-chosen media cap (default 10 GB), analysis cap (default 200 MB), "Keep analysis after deletion" toggle (default On — the B4 revalidation enabler).
7. **Settings → Diagnostics** — model versions, scheduler events, "Send diagnostics" mail composer (no auto-upload).
8. **Onboarding** — one screen at first subscription. No carousel, no permissions ask at launch.
9. **Notifications** — default Off. Single opt-in path tied to "Download Next N for [Flight/Commute/Workout]." Two narrow classes: **trip-readiness** (one per batch on success) and **action-required** (one per batch when blocked by a *user-fixable* condition: storage full, Wi-Fi-only setting, analysis unavailable). Action-required never fires for transient causes (thermal, low power, no-grant). At most one of each per batch.

### Honesty principles (cross-cutting)

1. **Don't promise what you can't always deliver.** "Downloaded" is a guarantee; "Skip-ready" is a guarantee at a specific threshold; everything else describes work in progress.
2. **One verb, one direction.** "Download" and "Remove Download" never coexist.
3. **Coarse public, granular private.** Library cell binary-coarse → Activity operational → Diagnostics exhaustive.
4. **Pause is never a bare word.** Every pause carries reason + unblocker.
5. **The scheduler is invisible.** Lanes, slice sizes, model versions, EVI scores — none appear in user-facing copy.
6. **Notifications earn their channel.** One trip-readiness ping per explicit user-stated deadline.

---

## 6. Prioritized Roadmap

The roadmap is organized by **dependency**, not by how interesting an idea is. Earlier phases exist to make later sophistication worthwhile. Each phase carries an **acceptance criterion** — a single binary test that must pass before moving on.

### Phase 0 — Guardrails & contracts

**Objective:** Make the on-device path viable, measurable, and policy-versioned before any new work runs against it.

**Deliverables:**
1. Confirm legal boundary for all uploaded telemetry and diagnostics. Document the exact set of fields that may leave the device. Default-deny everything else.
2. **Define `AnalysisEligibility` and persist it per device/profile.** Fields: `{ hardwareSupported, appleIntelligenceEnabled, regionSupported, languageSupported, modelAvailableNow }`. Decide and document the product policy for non-eligible devices:
   - raise the minimum supported hardware for the skip feature, or
   - support a download-only mode with explicit "Analysis unavailable" messaging.
3. **Replace the single Readiness SLO with cohort-based SLIs.** Operational measurement layer:
   - `time_to_downloaded` *(measured on play-starts and pause transitions, not passive cell renders)*
   - `time_to_proximal_skip_ready`
   - `ready_by_first_play_rate` *(user-perceived protection SLI — fraction of first play starts on downloaded episodes where the next listening window was already protected)*
   - `false_ready_rate` *(the honesty SLI — fraction of listening-window entries with a visible ✓ where playback failed to auto-skip an auto-skippable span in that window)*
   - `unattributed_pause_rate` *(measured on pause transitions, not slices)*

   `warm_resume_hit_rate` remains a secondary optimization KPI for B4/B5, paired with a storage-efficiency metric, and is not release-gating.

   Measure by cohort:
   - **trigger:** explicit Download vs subscription auto-download
   - **analysis mode:** transport-only vs eligible-but-unavailable-now vs eligible-and-available
   - **execution condition:** favorable (Wi‑Fi+power) vs mixed vs constrained
   - **episode duration / byte bucket**

   Fixture taxonomy (chapter richness, ad density, language, dynamic insertion, etc.) remains an offline lab stratification — **not** a production telemetry cohort.

   **Thresholds to defend early** (favorable conditions, duration-bucketed where noted):
   - `time_to_downloaded` — explicit download, 30–90 min episodes: P50 ≤ 15 min, P90 ≤ 60 min
   - `time_to_proximal_skip_ready` — explicit download, eligible-and-available, 30–90 min episodes: P50 ≤ 45 min, P90 ≤ 4 h
   - `ready_by_first_play_rate` — first play starts occurring ≥30 min after explicit Download: ≥85%
   - `false_ready_rate` — dogfood exit ≤2%, ship target ≤1%
   - `unattributed_pause_rate` — harness = 0; field < 0.5% of pause transitions

   For auto-download cohorts, defend "by next overnight idle+charging window" targets — **not** fixed-field time promises.

   **Executive north-star seed (calibration target, not a Phase 0 commitment):** *"For the top 10 most-likely-to-play episodes per user, ≥80% are Skip-ready by the next overnight idle+charging window, on analysis-eligible devices."* "Top 25" is retained as a capacity-planning dashboard only.
4. **Define cause taxonomy as four layers** (instead of a single miss-reason enum). The mapping from internal cause to user surface is **not** a pure function of cause alone — it is `f(internalCause, retryBudgetRemaining, forwardProgressSinceLastRetry, analysisEligibility, userFixability)`:
   - **`InternalMissCause`** (high-cardinality, single-cause, scheduler-facing):
     `{ no_runtime_grant, task_expired, thermal, low_power_mode, battery_low_unplugged, no_network, wifi_required, media_cap, analysis_cap, user_preempted, user_cancelled, model_temporarily_unavailable, unsupported_episode_language, asr_failed, pipeline_error, app_force_quit_requires_relaunch }`
   - **`SurfaceDisposition`** (placement / terminality):
     `{ queued, paused, unavailable, failed, cancelled }`
   - **`SurfaceReason`** (copy-stable, user-facing):
     `{ waiting_for_time, phone_is_hot, power_limited, waiting_for_network, storage_full, analysis_unavailable, resume_in_app, cancelled, couldnt_analyze }`
   - **`ResolutionHint`** (what unblocks each surface reason, plus `userFixable: Bool`)
   - **`CauseAttributionPolicy`**: exactly one primary `InternalMissCause` is chosen by precedence; simultaneous blockers are retained as `suppressedCauses` for Diagnostics only.

   Not every internal cause maps directly to `Paused`, and some map **by context**:
   - `no_runtime_grant` and `user_preempted` → `queued` / `waiting_for_time`
   - `task_expired` → `queued` / `waiting_for_time` while retry budget remains; `failed` / `couldnt_analyze` only after repeated expiry without forward progress
   - `model_temporarily_unavailable` → `unavailable` / `analysis_unavailable` only when `AnalysisEligibility.modelAvailableNow == false`; otherwise stays queued for retry
   - `unsupported_episode_language` → `unavailable` / `analysis_unavailable` for that episode only; device remains eligible for other content
   - `asr_failed` and `pipeline_error` surface only after retry exhaustion
   - `app_force_quit_requires_relaunch` → `paused` / `resume_in_app` (its own user-visible state — Apple's background URLSession explicitly does not auto-relaunch force-quit apps)
   - `wifi_required` and `media_cap` / `analysis_cap` are always `userFixable: true`; `thermal` / `low_power_mode` / `no_runtime_grant` are always `userFixable: false`.
5. Audit `AnalysisStore` schema; add `model_version`, `policy_version`, `feature_schema_version` columns where missing. Backfill defaults for existing rows.
6. **Define storage budgets and three internal artifact classes** (per §3 retention policy):
   - Media cap: user-configurable, default 10 GB. Hard ceiling enforced by admission control.
   - Analysis cap: user-configurable, default 200 MB. Independent eviction policy (LRU on least-recently-used episode).
   - Three classes: `media`, `warmResumeBundle`, `scratch`. When media is evicted, only `warmResumeBundle` is retained.
   - Track `warm_resume_hit_rate` as a **secondary KPI** (not release-gating), paired with a storage-efficiency metric so the measurement doesn't reward over-retention.
7. Define `QualityProfile` enum exposing the existing thermal / battery profiles as scheduler input rather than scattered guardrails.

**Acceptance criterion (Phase 0):** Schema audit passes; `AnalysisEligibility` model is implemented and tested across eligible/ineligible device fixtures; cause taxonomy + telemetry envelope signed off by legal; eviction prototype demonstrates three-class retention end-to-end (download → evict `media` → re-download → instant Skip-ready from retained `warmResumeBundle`).

### Phase 1 — Execution plane

**Objective:** Make background work finish reliably, with hard user preemption, durable transport, and a single-writer-per-episode contract.

**Deliverables:**
1. **Build a two-part device-lab fixture substrate.**
   - **Locked core corpus:** 8 byte-pinned fixtures used for deterministic regression gates.
   - **Rotating stratified sample:** 4 fixtures per run drawn from a declared taxonomy with a frozen seed per week / release branch.

   The taxonomy must cover, at minimum: duration bucket; chapter richness / chapter correctness; ad density / break placement; supported vs unsupported content language; audio structure (clean speech, music-bed-heavy, cross-talk / poor remote audio); re-download / eviction / warm-resume path.

   All fixtures are stored as **captured media bytes plus hashes** — regressions never depend on live feed URLs (dynamic-inserted ads make live feed URLs unstable). This corpus is the substrate for every Phase 1 acceptance gate — never ad-hoc episodes.
2. Replace standard async download with **Background URLSession**. Two configurations: interactive (user-initiated downloads) and maintenance (subscription auto-downloads).
3. Formalize **three-lane scheduler** (Now / Soon / Background) inside `AnalysisWorkScheduler`. Lane names are scheduler-internal — UI never sees them.
4. Implement **hard user preemption**: any user-tapped Play or Download promotes that work to Now lane and pauses lower-lane work at the next checkpoint boundary (not mid-shard).
5. Implement **multi-resource scheduling**: transport, CPU, storage, thermal budgets evaluated as independent admission gates per work item.
6. **`EpisodeExecutionLease` + append-only `WorkJournal` + global `SchedulerEpoch`.** Lease-based coordination so foreground assist, `BGContinuedProcessingTask`, `BGProcessingTask`, and `URLSession` callbacks cannot simultaneously mutate the same episode state. Explicit generation IDs on resume / retry; orphan recovery on cold launch; idempotent finalize / eviction transitions. `SchedulerEpoch` / `SchedulingPass` makes cross-episode promotions, demotions, and resource admissions crash-consistent: every work item carries `{generationID, schedulerEpoch}`; late callbacks that do not match both are ignored or requeued.
7. **B1: grant-window prediction + slice sizing.** Per-device-class learned distribution of granted BG run windows. Slice size targets ≥95% completion probability. Ship as a per-device-class config table that the scheduler reads; adaptive learning loop is Phase 3 polish.
8. Instrument slice completion rates by device class and task type. Emit `InternalMissCause` on every paused or failed slice.
9. Add explicit foreground-assist path for "Download" tap (`BGContinuedProcessingTask` hand-off after backgrounding).
10. **Interruption harness coverage.** Automated test scaffolding for: backgrounding / relaunch / network loss / thermal downgrade / low-power transition / storage pressure / user preemption.
11. **Force-quit negative-case handling and copy.** Background URLSession does **not** automatically relaunch a force-quit app — model that explicitly. After force-quit, transfers transition to a manual-resume state with explicit copy in the Activity screen and Diagnostics.

**Acceptance criterion (Phase 1):** Three independent gates against the fixture corpus from deliverable 1.

- **Gate A — transport reliability:** On all supported devices, 3 explicit downloads on the fixture corpus reach `Downloaded` within 2h on Wi-Fi, across 30 device-nights, with 0 unexplained transfer failures.
- **Gate B — checkpoint / preemption reliability:** Across 50 forced interruption cycles (per the harness from deliverable 10), work resumes without duplicate finalization, corrupt checkpoints, or lost user-priority promotions.
- **Gate C — first-window readiness:** *On analysis-eligible devices only,* at least 2 of 3 ad-bearing fixtures reach `playbackReadiness == proximal` within 4h under Wi-Fi + power, and every paused/failed slice records exactly one `InternalMissCause`.

### Phase 1.5 — Truth surfaces & supportability

**Objective:** Prove that every user-visible readiness state is derivable, honest, and supportable *before* public UI ships in Phase 2.

**Deliverables:**
1. Introduce **`EpisodeSurfaceStatus`** as the sole reducer for **episode-readiness** surfaces (Library / Episode Detail / Activity rows / widgets / App Intents) from persisted facts (state machine + derived `playbackReadiness` + active `InternalMissCause` + `AnalysisEligibility`). Non-episode surfaces use sibling reducers — **`BatchSurfaceStatus`** (for "Download Next N" batches) and **`TaskSurfaceStatus`** (for `BGContinuedProcessingTask` Live Activities) — rather than ad-hoc logic. UI targets never read underlying facts directly.
   - Enforce with **module boundaries:** UI targets import surface DTOs only; `AnalysisStore`, `AnalysisSummary`, and scheduler enums are not visible to them.
   - Add **CI lint** blocking direct references to raw analysis enums from UI targets.
   - Add a **contract test matrix** and **snapshot suite** that renders every `EpisodeSurfaceStatus` / `BatchSurfaceStatus` / `TaskSurfaceStatus` case across all adopted surfaces.
2. Implement the **`InternalMissCause` → `SurfaceReason` → `ResolutionHint`** mapping defined in Phase 0. Tabletop-test every internal cause to confirm it lands in the right surface bucket (Up Next vs Paused vs Recently Finished vs Analysis Unavailable).
3. Wire `analysisUnavailableReason` (not a pause) for unsupported device / language / region / disabled Apple Intelligence. Verify it surfaces in episode detail status line, Activity (Recently Finished), and Diagnostics — never in the Paused section.
4. **Support-safe diagnostics bundle classes.** Default bundle contains metadata only (versions, scheduler events, eligibility state). Episode-specific artifacts (transcript excerpts, feature summaries) require explicit per-episode opt-in. Legal review.
5. **State-transition audit + impossible-state assertions.** Compile-time-enforced exhaustive switches over the state enum; runtime assertions that block illegal combinations (e.g., `playbackReadiness == proximal` AND `analysisUnavailableReason != nil`). Cross-target contract test that fails if any surface bypasses the reducer or invents copy outside the approved table.
6. **10 days of dogfood** with state-transition logging and audit. Goal: zero impossible-state assertions; every observed pause has a renderable `SurfaceReason` and `ResolutionHint`.

**Acceptance criterion (Phase 1.5):** No impossible UI-state combinations appear in dogfood; every surfaced pause has an unblocker copy; `false_ready_rate` (the honesty SLI from Phase 0) is below an agreed threshold (suggested seed: ≤2% of cells showing ✓ where playback failed to skip in the next listening window).

### Phase 2 — Fast readiness wins + UI surfaces

**Objective:** Produce user-visible value earlier; ship the UI contract.

**Deliverables:**
1. **B2: playhead-proximal partial readiness.** Cascade order changes: prioritize candidate-window ASR over the next 12–20 minutes + chapter-marked mid-roll windows before episode-wide backfill.
2. **Persist `CoverageSummary`; derive `playbackReadiness`.** Treat `playbackReadiness` as an **anchor-relative derived view**, not an intrinsic episode field. Persist `CoverageSummary` (`coverageRanges`, `firstCoveredOffset`, `isComplete`, version stamps) and derive `playbackReadiness ∈ { none, deferredOnly, proximal, complete }` from `CoverageSummary + readinessAnchor` on read.
   - `readinessAnchor` = episode start for unplayed episodes; last committed playhead for resumed episodes.
   - `proximal` means the next listening window contains no unresolved or `markOnly` spans that would weaken the auto-skip promise.
   - The same persisted coverage can correctly render as `deferredOnly` when unplayed and `proximal` after the user resumes past uncovered content — the view changes, not the analysis.
   - Library cells render ✓ **only** when the derived state is `proximal` or `complete`. `deferredOnly` stays as ⤓.
3. **Episode detail status line.** Single line above body, sourced from `EpisodeSurfaceStatus` (the reducer from Phase 1.5). Renders coverage range, deferred-only explanation, paused reason, or "Analysis unavailable" — never raw scheduler internals.
4. **Activity screen** — four sections (Now / Up Next / Paused / Recently Finished) per UI design §E. The Paused section is the user-facing home of `SurfaceReason` from Phase 0; Recently Finished surfaces "couldnt_analyze" cases and "Analysis unavailable" non-pause states.
5. **"Download Next N" bulk affordance** on the show page. Stepper 1 / 3 / 5 / 10 / 25 (no "All unplayed" — see UI design §D). Optional "for…" picker (Flight/Commute/Workout/Generic) — v1 only changes notification copy, not scheduler behavior.
6. **Settings → Downloads, Storage, Diagnostics.** Per UI design §F.
7. **Onboarding screen + first-✓ tooltip.** Per UI design §G.
8. **Trip-readiness + action-required notifications.** Single permission ask gated to non-Generic "Download Next N." Because iOS notification authorization is granted by interaction type (not by narrow-scope permission), narrowness must be enforced **structurally in our code**, not by OS policy. Notification emission is driven only by a whitelisted **`BatchNotificationEligibility`** reducer:
   `{ none, trip_ready, blocked_storage, blocked_wifi_policy, blocked_analysis_unavailable }`
   Rules:
   - the notification service **does not accept raw `SurfaceReason`** or arbitrary copy — only the whitelisted enum above
   - action-required is permitted only when the *entire batch* is blocked, the blocker is `userFixable: true`, and it persists across **two scheduler passes** (or a minimum wall-clock threshold)
   - transient causes (`thermal`, `low_power_mode`, `no_runtime_grant`, `task_expired`, `no_network`) can never produce `BatchNotificationEligibility`
   - `blocked_analysis_unavailable` is emitted only for fixable sub-cases (Apple Intelligence off / language mismatch) — never for unsupported hardware
   - at most one trip-ready and at most one action-required notification per batch
9. **Promote RSS / show-notes pre-seeding from shadow to live signal** at the corroboration gate (≤0.15 of fusion budget).
**Acceptance criterion (Phase 2):** A user who taps "Download Next 3 for Flight" on an analysis-eligible device receives at most one trip-readiness notification within 2 hours stating how many of the 3 are Skip-ready (and at most one action-required notification if blocked by a user-fixable cause). The Activity screen accurately reports each episode's `SurfaceReason` at every point during that window. **UI badges never mislead — when ✓ is shown, the player either auto-skips within the current listening window or has already established complete coverage for that window.** `false_ready_rate` remains below the Phase 1.5 threshold across the fixture corpus.

### Phase 3 — Optimization

**Objective:** Reduce cost without regressing quality.

**Deliverables:**
1. **B3: local repeated-ad tile memoization.** Cache short audio fingerprints for high-confidence ad spans. On match in new episodes, reuse prior transcript / classification / boundary data. Local-only — no shared cloud intelligence. Frame as a `RepeatedAdCache` keyed on composite fingerprint.
2. **B4: fast local revalidation from persisted features.** On model or policy version bump, run "revalidate from features" path that skips ASR and re-runs only the classifier + fusion + boundary stages from persisted `TranscriptChunk` + `FeatureWindow` rows. Build on existing Phase 4 shadow-retry capability observer.
3. **Lightweight inventory sanity check.** Catches obviously-bad skips (e.g., spans <2s, spans starting in first/last 3s of episode, spans overlapping declared content chapters). Post-hoc safety; not a readiness driver.
4. **Scoped music-bed generalization.** Detect recurring jingle patterns across episodes within a subscription; promote music-bed signal weight only on shows where pattern detection succeeded N times.
5. **B1 learning loop.** Replace the per-device-class config table from Phase 1 with an on-device adaptive estimator that adjusts slice sizes per actual grant-window history.
6. **B5 closed-loop control — bounded local controller (not online RL, not opaque self-tuning).**
   - **Boot policy**: controller starts in **`observe_only` mode** using the Phase 1 per-device-class config table as shipped defaults. Log proposed actuator changes without applying them.
   - **Observations** (locally logged per slice): device class, eligibility state, work type, episode duration bucket, show profile (if available), queue lane, bytes expected / completed, slice target vs actual runtime, `InternalMissCause`, whether the slice increased proximal coverage.
   - **Estimators**: EWMA / Beta success models with **hierarchical backoff**:
     - base cohort = **device class × work type × execution condition**
     - show profile applies only after a minimum observation floor; otherwise shrink to the base cohort (show profile is too sparse for cold start)
   - **Actuators**: slice-duration bucket, classifier budget, lane concurrency, retry backoff, background-vs-foreground deferral.
   - **Activation floor**: no actuator changes until a cohort has at least N slices and M episodes; below the floor, only log proposed changes.
   - **Version handoff**: on OS / model / policy-version change, decay or reset learned state and fall back to boot policy until the activation floor is re-met.
   - **Priors**: Beta priors may be seeded from internal fixture-lab results and shipped as static app config.
   - **Safety rails**: minimum per-episode budget; **maximum one-notch change per actuator per 24h** (prevents oscillation); **never suppress an episode to zero work**.
   - **Persistence**: policy revisions persisted locally with version stamps; surfaced in Diagnostics.
   - **Telemetry**: controller must work fully local. Any uploaded telemetry is aggregate-only and optional, gated by Phase 0 legal envelope.
7. **Per-show capability profiles** — observed (not user-set). Internal classification: chapter-rich, host-read-only, music-bed-reliable, etc. Used by scheduler to modulate budget **only after baseline SLIs are stable** (hence deferred from Phase 2 — cold-start bias risk). Exposed only in Settings → Diagnostics. **Always-on minimum per-episode budget** so a profile can never zero out work for a show.

**Acceptance criterion (Phase 3):** On the analysis-eligible cohort from Phase 1, the median compute spent per episode (CPU-seconds + GPU-seconds + Neural Engine seconds) drops by ≥30% vs Phase 2 baseline, while none of the Phase 0 cohort SLIs regress beyond agreed thresholds (in particular: `false_ready_rate` must not regress, and `time_to_proximal_skip_ready` 50th-percentile must not regress by more than 10%).

### Future arc (out of scope for this plan)

- Host-voice counter-classifier (when real diarization stack ships)
- Cross-user shared signals (would require revisiting the legal / on-device boundary)
- Active analysis hinting from listening-history models
- **Smart contextual surfacing of "Download Next N"** — calendar / charger / low-connectivity cues. Product candy; dragged calendar/privacy/intent complexity into an otherwise focused scheduler/reliability plan.

---

## 7. Cross-Cutting Risks & Decisions

### Resolved decisions

These decisions are now made (resolved during the UI brainstorm and codebase reconciliation):

1. **Mental model:** progressive disclosure — calm casual, deep power-user surface.
2. **User vocabulary:** three verbs (Subscribe / Download / Add to Up Next) + Play. No "Prepare."
3. **Readiness vocabulary:** explicit state enum (Downloaded / Skip-ready / Deferred-only / Paused-with-reason / Analysis unavailable). No score, no percentage. Skip-ready means *playback-proximal coverage*, not "some ad found somewhere."
4. **Library cell:** icon-only badge. Detail view carries the granular coverage line.
5. **Bulk affordance:** "Download Next N" with optional for…-picker. Killer feature for trips. Stepper tops out at 25; no "All unplayed" in v1.
6. **Activity surface:** dedicated screen via Library nav button. Not a tab.
7. **Storage policy:** user-chosen hard cap (default 10 GB) + three internal artifact classes (`media` / `warmResumeBundle` / `scratch`). When media is evicted, only `warmResumeBundle` is retained.
8. **Notifications:** default Off; two narrow opt-in classes — trip-readiness (success) and action-required (user-fixable blocker). At most one of each per batch.
9. **Eligibility model:** Foundation Models / Apple Intelligence requires hardware + region + language + user-toggle + current model availability. Treated as a first-class `AnalysisEligibility` contract; ineligibility is *not* modeled as Paused.
10. **Concurrency contract:** `EpisodeExecutionLease` + append-only `WorkJournal` — single-writer-per-episode across all four execution planes.
11. **Cause taxonomy:** three-layer (`InternalMissCause` → `SurfaceReason` → `ResolutionHint`); not every internal cause maps to user-visible Paused.
12. **What lives in the codebase already** is treated as foundation (per §1) — no rebuild work in scope.

### Open questions (still requiring decisions)

1. **`playbackReadiness == proximal` definition.** Treat `playbackReadiness` as an **anchor-relative derived view**, not an intrinsic episode field (see Phase 2 deliverable 2). Pin the versioned policy parameter as: *"all auto-skip-confidence spans inside the next-listening-window are bounded, and no unresolved / `markOnly` span intersects that window."* Anchor is `readinessAnchor` (episode start for unplayed; last committed playhead for resumed).
2. **Cause taxonomy validation.** §6 Phase 0 proposes 16 `InternalMissCause`s, a `SurfaceDisposition` enum, and 9 `SurfaceReason`s, plus a `CauseAttributionPolicy`. Validate against `AnalysisWorkScheduler` realities during Phase 0 implementation; confirm the precedence ladder actually picks a clean single primary cause in real scheduler traces.
3. **Per-show capability profile schema.** What fields, when persisted, when surfaced. Affects Diagnostics surface and the "this show isn't worth analyzing" modulation (capped by always-on minimum from Phase 3 deliverable 7 — deferred from Phase 2 to avoid cold-start bias).
4. **Cohort SLI thresholds.** Phase 0 now defends five concrete early thresholds (see Phase 0 deliverable 3); rest need calibration after Phase 0 field measurement. Numbers for auto-download cohorts are *not* absolute-hour commitments — defend "by next overnight idle+charging window" style targets there.
5. **Trip-readiness notification timing.** Fire when all N hit Downloaded, or aim for a user-supplied deadline (calendar-extracted) and fire at the deadline regardless of completion?
6. **Device cohort split.** Two cohorts:
   - **Transport cohort:** all supported iPhone models. Gate A applies.
   - **Analysis cohort:** Apple-Intelligence-eligible iPhones (currently 15 Pro / 16 family / 17 family per Apple's docs). Gates B and C apply only here.
   Confirm exact device list against Apple's Apple-Intelligence-supported-devices documentation at Phase 0 sign-off.
7. **Product policy for ineligible devices.** Download-only mode with explicit "Analysis unavailable" messaging, OR raise the minimum supported hardware for the entire app? Decide before Phase 0 closes (affects App Store messaging and onboarding copy).

### Cross-cutting risks

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| Background grants are too rare on representative devices to meet SLI targets | M | H | Phase 1 instrumentation surfaces this immediately; plan B is to make user-initiated Download more aggressive in foreground assist. |
| `Skip-ready` flips because of a far-later ad, not the next listening window | M | H | Phase 2 deliverable 2 (`playbackReadiness` enum); Phase 1.5 acceptance gate measures `false_ready_rate` directly. |
| Foundation Models unavailable on device / language / region / user setting | M | H | Phase 0 `AnalysisEligibility` contract; cohort-split gates; explicit "Analysis unavailable" UI state (not Paused). |
| User force-quits app; automatic relaunch of background transfers is blocked | M | H | Phase 1 deliverable 11 — model as manual-resume state, document in support copy, test the negative path explicitly. |
| Duplicate workers corrupt checkpoints or retained bundles | L | H | Phase 1 deliverable 6 — `EpisodeExecutionLease` + `WorkJournal`; idempotent finalize; orphan recovery on cold launch. |
| Diagnostics bundle leaks transcript or audio-derived data off-device | L | H | Phase 1.5 deliverable 4 — metadata-only default bundle; episode-specific artifacts require explicit per-episode opt-in; legal review. |
| Retained `warmResumeBundle` version skew after policy/model update causes badge drift | M | H | Version stamps on all retained artifacts (Phase 0 deliverable 5); Phase 3 B4 revalidation queue rebuilds stale bundles. |
| `warmResumeBundle` retention accumulates without bound | L | M | Analysis cap (default 200 MB) enforces upper bound; LRU eviction within the cap; `warm_resume_hit_rate` (secondary KPI, paired with storage-efficiency) tracks effectiveness without rewarding over-retention. |
| Trip-readiness notification fires too late to be useful | M | M | Phase 2 measurement; consider calendar-aware deadline timing in Phase 3. |
| Per-show capability profiles encode bias (e.g., "this show is hopeless") and starve worthwhile analysis | M | M | Deferred from Phase 2 → Phase 3 to avoid cold-start bias; always-on minimum analysis budget per episode (Phase 3 deliverable 7); capability profiles only modulate, never zero out. |
| Background URLSession migration regresses download reliability before Phase 1 acceptance | L | H | Ship behind a feature flag; A/B for one beta cycle before full rollout. |
| Legal envelope on telemetry is too restrictive to enable B5 closed-loop control | M | H | Phase 0 deliverable; B5 controller specified to work fully local — telemetry upload is aggregate-only and optional. |
| UI surfaces evolve faster than `EpisodeSurfaceStatus` reducer; UI starts reading underlying facts directly | M | M | Phase 1.5 deliverable 1 makes reducers (`EpisodeSurfaceStatus` + sibling `BatchSurfaceStatus` / `TaskSurfaceStatus`) the *sole* path, enforced by module boundaries and CI lint; contract/snapshot tests fail on direct raw-enum access. |
| Force-quit state surfaces as silent "waiting" instead of prompting user action | M | M | Explicit `app_force_quit_requires_relaunch` internal cause → `paused` / `resume_in_app` surface state (Phase 0 deliverable 4). |
| `playbackReadiness` drifts as an episode-global fact when the playhead moves | H | H | Phase 2 deliverable 2 persists `CoverageSummary` and derives `playbackReadiness` from coverage + `readinessAnchor`, so the same coverage flips deferredOnly↔proximal as the user resumes. |
| Scheduler crashes mid-pass and half-applies cross-episode promotions/demotions | L | H | Global `SchedulerEpoch` + atomic `SchedulingPass` (§3 + Phase 1 deliverable 6); stale-epoch callbacks are ignored. |
| Notifications drift beyond the two whitelisted classes | M | M | Phase 2 deliverable 8: notification service accepts only the `BatchNotificationEligibility` enum — not raw `SurfaceReason` or arbitrary copy. |

---

## 8. Bottom Line

The main change from the original Dueling Wizards report is not that its ideas were wrong; it is that **the legal constraint changes which ideas are foundational, and that significant infrastructure already exists in the codebase.** In an on-device-only design, reliability, persistence, partial readiness, and measurement come before most classifier sophistication — and the pieces that *do* need building are mostly system properties (slice sizing, miss-reason measurement, durable transport, two-tier eviction, UI contract) rather than new ML.

For planning purposes, the practical tests are **separate**, not blended:

- **Can the app download reliably?** (Phase 1 Gate A — transport reliability against the fixture corpus.)
- **Can it produce playback-proximal Skip-ready coverage?** (Phase 1 Gate C — analysis-eligible cohort only; Phase 2 acceptance for the user-visible promise.)
- **Can it do so without showing a dishonest badge?** (Phase 1.5 — `false_ready_rate` below the agreed threshold; impossible-state assertions in dogfood.)

If all three hold on representative devices, the later optimization ideas become worthwhile. If any fails, they are premature.

The UI design is a forcing function. It demands honest answers to: what does Skip-ready *promise*? what does Paused *mean* (and what isn't a pause at all)? what should the user *expect* to be told? The plan above — anchored on `AnalysisEligibility`, `playbackReadiness`, the three-layer cause taxonomy, the `EpisodeExecutionLease`, and the bounded local controller — is the engineering necessary to make those promises survivable.
