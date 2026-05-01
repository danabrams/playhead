# Wizard Scores: Claude Code Evaluating Codex's Ideas

> Generated 2026-04-29 by Claude Code (Opus 4.6).
> Scoring: 0 (worst) to 1000 (best).
> Criteria: (a) idea quality, (b) real-life user utility, (c) implementation
> practicality, (d) complexity/tech-debt justified, (e) design identity fit.

---

## Overall Assessment

This list is the work of a model that deeply understands Playhead's *internal
architecture* but is thinking primarily from an **engineering/ops perspective**
rather than a **product/user perspective.** Four of five top picks (#1, #2, #3,
#4) are pipeline infrastructure improvements. Only #5 is a user-facing feature
change — and even that one is really a policy control panel.

The prompt asked for ideas that make Playhead "more robust, reliable, performant,
**intuitive, user-friendly, ergonomic, useful, compelling**." This list delivers
strongly on the first three adjectives and largely ignores the last five. That's
the primary structural weakness. Reliability is a precondition for delight, not
a substitute for it.

Three critical observations:

1. **Predictive up-next preanalysis was ranked #21 (also-ran) instead of top 5.**
   This is the single biggest user-facing improvement available — it eliminates
   the latency between pressing play and the first usable skip, which is
   Playhead's most visible UX gap. The infrastructure is 90% built. Putting it
   at #21 suggests the model weighted pipeline correctness over user experience.

2. **Cross-episode transcript search is absent from the entire list.** The FTS5
   virtual table exists. The `searchTranscripts(query:)` method is implemented
   and tested. The content-sync triggers keep the index current. The only
   missing piece is a search UI. This is Playhead's most underexploited asset
   — hundreds of hours of searchable text locked behind per-episode views.

3. **Several proposals duplicate systems that already exist.** The Unified
   Resource Governor (idea #2) is essentially QualityProfile + AdmissionGate +
   DeviceAdmissionPolicy, which are already built and documented. The Watchdog
   (#1) overlaps heavily with the lease-expiry + AnalysisJobReconciler system.
   The Evidence Panel (#5) already exists as AdRegionPopover. These proposals
   read as if the model didn't fully recognize the existing implementations.

---

## Top 5 Scores

### #1 — Episode Pipeline Watchdog + Auto-Heal

**Score: 380 / 1000**

**What's genuinely good.** The instinct is right: silent pipeline failures ARE
the biggest trust killer. Heartbeat-based monitoring is a proven reliability
pattern, and bounded recovery ladders (retry → restart → quarantine) are
cleaner than unbounded retries. The proposal correctly identifies that this
should be a separate actor from the scheduler, consuming lifecycle events
rather than owning them.

**What's already built.** This overlaps heavily with existing recovery infrastructure:

- `AnalysisJobReconciler.reconcile()` runs at app launch and sweeps for
  expired leases, stranded session jobs, stale versions, missing files, and
  backed-off failures. Its `ReconciliationReport` already tracks 10 distinct
  recovery categories.
- `EpisodeExecutionLease` with lease expiry + renewal provides heartbeat
  semantics. If a job dies without releasing its lease, the reconciler
  recovers it on the next pass.
- `CauseTaxonomy` already classifies pipeline failures into actionable
  categories that surface through `SurfaceStatus`.
- **playhead-btwk** (closed) specifically fixed stranded backfill/running/paused
  jobs from prior sessions — exactly the class of bug a watchdog would catch.
- **playhead-fuo6** (closed) fixed the overnight background-processing gap
  caused by missing BGProcessingTask submissions.

The question is whether adding a *continuous* heartbeat monitor on top of the
*periodic* reconciler sweep provides enough incremental value to justify another
actor in an already complex actor graph.

**Where the idea falls short.**

- **Who watches the watchdog?** Adding a supervision layer creates a recursive
  problem. If the watchdog actor itself stalls (cooperative executor starvation,
  deadlock with a service it monitors), you need a meta-watchdog. The existing
  approach — periodic reconciliation at app launch and phase transitions — is
  simpler and has no supervision recursion.
- **Heartbeat SLAs are hard to define.** On-device ASR throughput varies by
  2-10x depending on device model, thermal state, episode audio complexity
  (music beds vs clean speech), and concurrent workload. A "60-second ASR
  heartbeat" that's generous on A18 might fire constantly on A17 Pro under
  thermal pressure — producing false alarms and unnecessary recovery churn.
- **The failure modes it catches are already rare.** The specific bugs that
  caused pipeline stalls (fuo6, btwk) were found and fixed. The reconciler
  sweep catches lease expiry. Remaining stalls would be bugs in the analysis
  pipeline itself (infinite loops, deadlocks), which need root-cause fixes,
  not watchdog restarts.
- **Complexity cost.** Another actor consuming lifecycle events from three
  services, persisting escalation state in the work journal, and mapping
  outcomes to SurfaceStatus. On a 16GB machine where Xcode OOMs at 2
  concurrent builds, another always-on actor isn't free.

**Verdict.** Solid engineering instinct, but solves a problem that's already
~80% addressed by existing infrastructure. The incremental value of continuous
heartbeats over periodic reconciliation doesn't justify the complexity and
supervision-recursion risk. Would be better as a targeted enhancement to
`AnalysisJobReconciler` (add heartbeat columns to the work journal) than as
a new actor.

---

### #2 — Unified Resource Governor (Battery/Thermal/Storage/Network)

**Score: 250 / 1000**

**What the proposal says.** "Right now resource decisions are distributed
across services; a central governor prevents contradictory behaviors."

**What already exists.** This is already built. The QualityProfile header
comment literally says:

> *Prior to this file, thermal state, low-power mode, battery level, and
> charging state were read piecewise by each consumer. [...] QualityProfile
> consolidates the read into a single named surface with explicit per-variant
> policy that the scheduler and other consumers route through. As of C1,
> `DeviceAdmissionPolicy` has been removed and `AdmissionController` derives
> its admission decision directly from this profile.*

The existing unified resource management stack:

| Component | What it governs |
|-----------|----------------|
| `QualityProfile` | Thermal + battery + LPM → 4-level admission profile (nominal/fair/serious/critical) |
| `AdmissionGate` | Multi-resource AND gate: transport + CPU + storage + thermal per work item |
| `CapabilitiesService` | Foundation Models availability, thermal state, device class, disk space |
| `DeviceClassProfile` | Per-device-class grant windows, slice sizes, throughput estimates |
| `StorageBudget` | Per-artifact-class storage caps with eviction policy |
| `TransportSnapshot` | Network reachability, session type, cellular preference |

These are already consumed by the scheduler, transcript engine, download
manager, and ad detection service — exactly the consumers the proposal
lists. The AdmissionGate even explicitly documents that it "does NOT read
`ProcessInfo.thermalState` or `isLowPowerMode` directly" — everything
routes through QualityProfile. This is the consolidation the proposal
describes, already implemented and shipping.

**Where the idea falls short.**

- **It proposes building something that exists.** The most charitable reading
  is that the model saw distributed resource checks in older code and didn't
  realize they've since been consolidated. The QualityProfile comment trail
  tells the story: `DeviceAdmissionPolicy` was created in Phase 3, then
  superseded by `QualityProfile` which absorbed its role.
- **Centralized governors are single points of failure.** The existing approach
  uses QualityProfile as a *derived value* (a pure function of OS signals)
  rather than a *stateful actor*. That's better — a pure derivation can't
  stall, deadlock, or hold stale state. An always-running governor actor
  that "computes a profile every few seconds" is strictly worse: it adds
  polling overhead, stale-state risk, and an actor that contends for the
  cooperative executor.
- **"Contradictory behaviors" is not a real problem.** The proposal claims
  distributed decisions cause contradictions, but the QualityProfile + 
  AdmissionGate architecture prevents this by design: all consumers read
  the same derived profile. The Phase 3 memory explicitly documents that
  `DeviceAdmissionPolicy` was consolidated because AdmissionController and
  BackgroundProcessingService had drifted (LPM gating inconsistency). That
  drift was the bug; `QualityProfile` was the fix. It's done.

**Verdict.** This proposes rebuilding infrastructure that already exists and
works. The lowest-scoring idea in the top 5 because it demonstrates
incomplete understanding of the current codebase state.

---

### #3 — Adaptive Hot-Zone Sizing by Measured Throughput

**Score: 520 / 1000**

**What's genuinely good.** This is the best idea in the other model's top 5.
The observation is correct: static lookahead windows (20-min unplayed, 15-min
resumed from `PreAnalysisConfig`) are inherently suboptimal across devices,
playback rates, and episode complexities. Measuring actual pipeline throughput
and adjusting the lookahead dynamically is a sound engineering approach that
addresses a real concern — especially at high playback speeds where the
pipeline must process 2-3x faster to stay ahead.

The proposal correctly identifies that the instrumentation surface already
exists (`SliceCompletionInstrumentation`, `PreAnalysisInstrumentation`) and
that the implementation builds on scheduler primitives. The mention of
"regression checks via replay harness" shows good testing instinct.

**Caveats that limit the score.**

- **The static windows are already generous.** 20 minutes of pre-analysis for
  an unplayed episode covers well beyond what's needed for first skip
  (most pre-roll ads start within 30 seconds). The 90-second hot-path
  lookahead in AdDetectionConfig is the tighter constraint, and it's already
  adapted by the existing playback-aware scheduling.
- **Related work already exists.** playhead-yqax (closed: "foreground
  transcript catch-up: playhead-aware escalation for long-episode pipeline
  coverage") and playhead-dzmu (closed: "acoustic-likelihood-driven
  transcript scheduling") already added adaptive scheduling. playhead-sew8
  (open, P2: "acoustic-likelihood trigger for transcript scheduler") is the
  next iteration. The proposal doesn't acknowledge any of these.
- **Throughput measurement is noisier than it sounds.** ASR throughput varies
  by 3-5x within a single episode depending on content (clean speech vs
  music beds vs crosstalk). A throughput estimator that responds to short-term
  measurements will oscillate; one that averages over longer windows loses
  responsiveness. Finding the right smoothing window is non-trivial.
- **Edge-case optimization.** At 1x-1.5x (the vast majority of listening), the
  pipeline easily stays ahead on any supported device (A17 Pro+). The benefit
  materializes mainly at 2x+ on thermally stressed devices — a real but narrow
  user segment.

**What would make this better.** Instead of continuous throughput measurement
and dynamic window resizing, a simpler approach: define 3-4 throughput tiers
in `DeviceClassProfile` (which already has `bytesPerCpuSecond` and
`avgShardDurationMs`), multiply by current `QualityProfile` derate factor,
and use the result to set the lookahead at episode start and on rate changes.
Same benefit, much less machinery.

**Verdict.** Sound idea that addresses a real (if narrow) performance concern.
Loses points for not acknowledging existing adaptive scheduling work and for
proposing continuous measurement when a tier-based approach would be simpler
and nearly as effective.

---

### #4 — Episode Readiness Timeline + ETA in Activity

**Score: 300 / 1000**

**What's genuinely good.** The principle is valid: users who see "not ready yet"
want to know why and when. Surfacing bottleneck causes ("waiting for download",
"deferred: low battery") reduces perceived flakiness. The proposal correctly
identifies that the observability infrastructure exists (`SurfaceStatus`,
`CauseTaxonomy`, `EpisodeSurfaceStatusObserver`).

**What fights the design identity.**

This directly conflicts with two foundational Playhead design principles:

1. **"Peace of mind, not metrics."** Dan's explicit feedback (saved in memory):
   sell the felt experience of seamless listening, not quantified counters.
   An ETA band ("~2-4 min to skip-ready") is a quantified counter. A pipeline
   stage display (Queued → Spooling → Hot-path ready → Backfill → Complete)
   is a metrics dashboard.

2. **"The intelligence stays mostly invisible."** (PLAN.md §4.1, Quiet
   Instrument brief.) Surfacing internal pipeline stages makes the intelligence
   visible, noisy, and anxiety-inducing. Users checking a readiness timeline
   are the opposite of users at peace.

The design checklist (PLAN.md §4.11) explicitly says to avoid:
- "Busy dashboards full of analytics"
- "The word 'AI' in user-facing copy"

A pipeline readiness timeline with stage names and ETAs is exactly a busy
analytics dashboard.

**ETA estimation is unreliable.** On-device ML processing time depends on
thermal state (which changes mid-processing), competing workloads (which are
unpredictable), ASR content difficulty (which varies within an episode), and
iOS background scheduling decisions (which are opaque). An ETA of "~2-4 min"
that ends up taking 12 minutes is worse than no ETA at all — it sets an
expectation and then breaks it, actively damaging trust.

**What would actually work.** Instead of a readiness timeline, make the
pipeline fast enough that users never need to check. That's idea #1 from
my list (predictive pre-analysis): analyze episodes before the user presses
play, so readiness is immediate. The right response to "users are confused
about progress" is not "show them the progress" — it's "eliminate the wait."

**Verdict.** Correct diagnosis (users want to know what's happening), wrong
treatment (pipeline dashboard). The treatment conflicts with the product's
design identity and creates a new problem (unreliable ETAs erode trust).

---

### #5 — One-Tap Per-Show Skip Posture Dial + "Why Skipped" Evidence Panel

**Score: 420 / 1000**

This bundles two ideas. I'll score them separately, then combine.

**Per-show posture dial (Strict / Balanced / Aggressive): 350.**

The observation is valid: different users want different skip aggressiveness.
But the architecture already handles this through two mechanisms:

1. **Global setting.** `UserPreferences.skipBehavior` (auto/manual/off)
   controls the global skip posture. This already exists in Settings.
2. **Per-show trust system.** `PodcastProfile.mode` (shadow/manual/auto) +
   `skipTrustScore` + `observationCount` adaptively adjust per-show behavior
   based on observed precision. This is the system's core trust innovation.

Adding Strict/Balanced/Aggressive as a third control surface creates
interaction complexity:
- If a user sets "Aggressive" on a show the trust system has demoted to
  "manual," whose decision wins?
- If global is "manual" and per-show posture is "Aggressive," what happens?
- Three presets obscure what they actually change (enter threshold? merge
  gap? minimum span? all three?). The mapping from preset to parameters is
  non-obvious and hard to explain.

The existing auto/manual/off + earned trust system is cleaner because it
separates behavior (what happens) from confidence (how sure we are). Presets
conflate the two.

**"Why Skipped" evidence panel: 500.**

This is a better idea — and it's already 80% implemented. `AdRegionPopover`
(shipped in Phase 5, playhead-u4d) shows:
- "AD SEGMENT" header with time range and duration
- "DETECTED FROM" section with per-signal provenance:
  - Foundation model consensus (brain icon)
  - Evidence catalog entries (URL, promo code, disclosure, CTA, brand)
  - Acoustic break corroboration (waveform icon)
  - User corrections (hand tap icon)
  - Classifier seed (ECG icon)
- "This isn't an ad" veto button wired to `UserCorrectionStore`

The `provenanceDescription` helper explicitly follows the "Peace of Mind,
Not Metrics" principle:

> *Per "Peace of Mind, Not Metrics," none of these strings carry quantified
> probabilities. The `_strength` values are preserved on the AnchorRef
> payload (other call sites still use them for ranking / fusion) but never
> reach the user-facing copy here.*

The proposal's suggestion to show "confidence band" directly contradicts
this design decision and the feedback memory. The existing implementation
is already the *right* version of this idea — showing what evidence was
found without quantifying how confident the system is.

**Combined score: 420.** The posture dial fights the existing trust system.
The evidence panel already exists and is better designed than what's
proposed. There's some incremental value in making AdRegionPopover more
discoverable (currently only reachable via transcript peek tap), but that's
a minor UI polish, not a top-5 idea.

---

## Notable Also-Rans

I won't score all 25, but several deserve comment:

**#14 — Sponsor Lexicon Confidence Aging: 530.** Actually the best idea in
the entire list outside the top 5. SponsorKnowledgeStore persists sponsor
terms, but temporal decay isn't implemented — a sponsor that appeared 6
months ago in a single episode has the same weight as one that appeared
last week across 5 episodes. Adding recency weighting to the lexicon
would reduce false positives from stale sponsor terms. Small, targeted,
measurable improvement.

**#21 — Predictive Up-Next Preanalysis: 780.** Ranked 21st here, 1st in
my list. The fact that this was an also-ran is the single biggest miss
in the evaluation. The other model wrote: "Start lightweight analysis for
likely next episode while on Wi-Fi/charging." That understates it — this
is the most impactful single improvement available, with 90% of the
infrastructure already built. It should be #1.

**#23 — Transcript-Peek Correction Shortcuts: 450.** The TranscriptPeekView
already has veto functionality wired through AdRegionPopover. Making
corrections more discoverable is genuine value, but the infrastructure exists.

**#7 — Deterministic Download→Analysis Handshake: 380.** The playhead-24cm
bead already tightened this path. playhead-24cm.1 (open, P2) is the
follow-up. Good idea but already identified and in progress.

**#10 — End-to-End Pipeline SLO Gates: 350.** Good CI practice, but the
replay harness already exists and the test plans already gate releases. This
is test infrastructure refinement, not a product improvement.

**#19 — First-Run Smart Defaults by Device Class: 220.** `DeviceClassProfile`
already provides per-device-class configuration. `DeviceClass.swift` maps
hardware identifiers to capability buckets. `PreAnalysisConfig.json` seeds
per-class settings. This is built.

**#29 — Local Weekly Value Report: 100.** Dan's stored feedback: "Sell the
felt experience of seamless listening, not quantified counters like 'time
saved.'" A "weekly value report" with "time saved, corrected skips, and
confidence trend" is exactly the metrics dashboard this feedback rejects.
This is the worst idea on the list for Playhead specifically — it would be
fine for a different product with different values.

**#30 — One-Tap Repro Bundle: 280.** `DebugEpisodeExporter` and
`CorpusExporter` (both in Settings, totaling ~46K of code) already provide
per-episode diagnostic exports. The Settings view has a full diagnostics
section. This is built.

---

## Summary Table

| Rank | Idea | Score | One-line reason |
|------|------|------:|-----------------|
| #1 | Pipeline Watchdog + Auto-Heal | 380 | Good instinct but overlaps with existing lease + reconciler + cause taxonomy |
| #2 | Unified Resource Governor | 250 | QualityProfile + AdmissionGate already IS this; proposal duplicates existing work |
| #3 | Adaptive Hot-Zone Sizing | 520 | Best idea in the top 5; real concern but narrow user segment and related work exists |
| #4 | Readiness Timeline + ETA | 300 | Correct diagnosis, wrong treatment; fights "peace of mind" and "invisible intelligence" |
| #5 | Skip Posture + Evidence Panel | 420 | Evidence panel already exists (AdRegionPopover); posture dial conflicts with trust system |

**Weighted average of top 5: 374 / 1000.**

For comparison, the ideas I think the list should have contained:

| Idea | Est. score | Why |
|------|----------:|-----|
| Predictive pre-analysis | 780 | Eliminates biggest UX gap; infrastructure 90% built |
| Cross-episode transcript search | 750 | FTS5 + query method already exist; just needs UI |
| Episode summaries via FM | 650 | Leverages FM + transcript for daily triage; graceful degradation |
| Shareable transcript quotes | 600 | Tiny implementation, outsized word-of-mouth; fits design identity |
| Smart sleep timer | 550 | Uses existing pauseProbability data; daily-use delight |

---

## The Deeper Disagreement

The other model optimized for **pipeline correctness.** It asked: "What could
go wrong inside the analysis pipeline, and how do we detect and recover from
it?" That's a valid engineering concern, but it produced a list of
infrastructure improvements that are mostly invisible to users and partially
redundant with existing systems.

The question the prompt actually asked was broader: how to make Playhead
"more robust, reliable, performant, intuitive, user-friendly, ergonomic,
useful, compelling." The last five adjectives require thinking about what
users DO with the product, not just whether the pipeline stays healthy.

Playhead's unique asset is the on-device transcript corpus. The most
valuable improvements exploit that corpus for new capabilities (search,
quotes, summaries) rather than making the pipeline that produces it
marginally more self-healing. The pipeline is already robust — it has leases,
reconciliation, cause taxonomy, surface status, thermal gating, and battery
management. Making it 10% more robust is worth less than giving users three
new reasons to love the product.
