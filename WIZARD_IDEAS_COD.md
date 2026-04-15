# Playhead Ad Detection Pipeline — Wizard Ideas (Codex)

This document focuses only on ad detection and skip quality in the existing on-device Playhead pipeline.

Context used:
- `CLAUDE.md`
- `Ad Detection System - Technical Deep Dive.md`
- current pipeline modules (`BackfillEvidenceFusion`, `DecisionMapper`, `SkipPolicyMatrix`, `MinimalContiguousSpanDecoder`, `TimeBoundaryResolver`, `SensitiveWindowRouter`, `PermissiveAdClassifier`, `SponsorKnowledgeStore`, `AdCopyFingerprintMatcher`, `UserCorrectionStore`, `SkipOrchestrator`)

Evaluation criteria for winnowing:
1. Improves user-visible trust (fewer bad skips, fewer obvious misses)
2. Improves core quality metrics (recall, precision, boundary MAE)
3. Is accretive to current architecture (no risky rewrite)
4. Has a realistic implementation path with phased rollout and replay validation

## 30 Ideas (Longlist)

### 1) Replay-Calibrated Skip Confidence
How it works: Replace identity-clamped skip confidence with calibration curves learned from replay corpus outcomes (per source mix and per show maturity bucket).

User perception: Fewer confusing “high-confidence but wrong” skips, more consistent behavior across shows.

Implementation: Add calibration layer after `BackfillEvidenceFusion` in `DecisionMapper`, with cohort-gated lookup tables and fallback to current identity mapping.

### 2) Uncertainty Band With Deferred Action
How it works: Add an explicit uncertainty zone (for example 0.55–0.72) that renders markers but defers auto-skip until additional evidence arrives (next FM pass, fingerprint hit, user correction prior).

User perception: Better precision without feeling like ads are ignored; users still see candidate context.

Implementation: Extend `SkipEligibilityGate` and `SkipPolicyMatrix` to support `defer` state, then re-evaluate spans during backfill checkpoints.

### 3) Negative-Evidence Scoring (Not Just Positive Evidence)
How it works: Model anti-ad evidence (organic editorial continuity, chapter topic continuity, no CTA/disclosure over long spans, stable host discourse) so non-ads can be actively protected.

User perception: Fewer false positives on interviews and advice segments that sound promotional.

Implementation: Add bounded negative contributions in fusion, with hard floor so negative evidence can dampen but not erase strong deterministic anchors.

### 4) Correction Scope Precision Upgrade
How it works: Replace broad correction ranges with typed scopes (`episode-span`, `phrase-on-show`, `sponsor-on-show`, `domain-ownership-on-show`) and expiration semantics.

User perception: “It learned exactly what I corrected” instead of broad unpredictable behavior.

Implementation: Extend `UserCorrectionStore` schema and consume it in `SkipOrchestrator`, `DecisionMapper`, and backfill rescoring.

### 5) Boundary Feedback From Post-Skip Seek Behavior
How it works: Treat immediate rewind/forward after a skip as supervised boundary signals and feed into boundary offset estimators.

User perception: Fewer clipped host intros/outros and fewer late exits from ads.

Implementation: Mine `recordListenRewind` style telemetry, update `TimeBoundaryResolver` edge priors per show.

### 6) Two-Pass Boundary Snap (Coarse Then Fine)
How it works: First snap to robust structural breaks, then run a fine local search near discourse transitions and cue phrases.

User perception: Cleaner boundaries on host-read ads with soft transitions.

Implementation: Keep current snap step, then add a cheap second local optimization pass bounded to +/- 3 seconds.

### 7) Dynamic Ad Length Priors Per Show
How it works: Learn per-show distributions for preroll/midroll/postroll length and use as priors during merge/split decisions.

User perception: Fewer absurdly short fragments and fewer merged mega-spans.

Implementation: Integrate priors into `MinimalContiguousSpanDecoder` split/merge thresholds and `SpanHypothesis` lifecycle scoring.

### 8) Hard-Negative Mining Loop
How it works: Continuously collect false positives (especially “informational CTA” content) and auto-generate challenge fixtures for replay tests.

User perception: Steady reduction of embarrassing over-skips over time.

Implementation: Add false-positive harvesting from correction events into replay fixture pipeline and CI thresholds.

### 9) Evidence Quality Weighting by Anchor Reliability
How it works: Weight anchors by historical precision (`url > promo code > disclosure > CTA phrase`) and by local context quality.

User perception: Strong deterministic anchors stay powerful while weak textual hints stop overfiring.

Implementation: Introduce anchor reliability table consumed by fusion and span decoder anchor gating.

### 10) Sponsor Alias Canonicalization
How it works: Normalize sponsor mentions (“AG1”, “Athletic Greens”, “drinkag1”) into one canonical entity per show.

User perception: Better recurring-ad recall with fewer duplicate or missed matches.

Implementation: Extend `SponsorKnowledgeStore` with alias graph + confidence promotion rules.

### 11) Multi-Feature Fingerprinting
How it works: Combine text MinHash with lightweight acoustic signature and CTA token sequence patterns.

User perception: Recurring ads are caught earlier even when script wording varies slightly.

Implementation: Extend `AdCopyFingerprintMatcher` scoring to late-fuse text and acoustic similarity with strict transfer guardrails.

### 12) Boundary Transfer Confidence Tiers for Fingerprints
How it works: Keep current transfer, but classify matches into strict/full transfer vs partial transfer vs seed-only based on alignment diagnostics.

User perception: Higher precision for “seen before” matches.

Implementation: Add explicit tiers and guardrails in fingerprint transfer path before decoded span upserts.

### 13) Sensitive Window Router Expansion
How it works: Broaden refusal-risk routing triggers (medical terms, regulated claims patterns, known refusal contexts) before FM call.

User perception: Fewer obvious misses in pharma/therapy/medical ad reads.

Implementation: Enhance `SensitiveWindowRouter` pre-classification rules with deterministic lexical probes.

### 14) Deterministic Fallback When FM Abstains
How it works: When FM returns `abstain`/refusal, run a strict deterministic classifier requiring high-confidence commercial anchors + acoustic corroboration.

User perception: Ads do not vanish just because FM declined to answer.

Implementation: Add abstain fallback branch in backfill, with conservative thresholds and shadow-mode rollout.

### 15) Loudness-Normalized Acoustic Breaks
How it works: Normalize energy features by rolling LUFS and route-specific gain context before break detection.

User perception: Better stability across loudness-compressed and inconsistent episodes.

Implementation: Insert normalization in feature extraction before acoustic break thresholds are applied.

### 16) Return-to-Show Transition Detector
How it works: Dedicated detector for “back to the show” patterns using discourse markers + speaker/music transitions.

User perception: Better end boundaries and less overrun past ad exits.

Implementation: Add explicit end-anchor channel into evidence catalog and boundary resolver.

### 17) Chapter/Show-Notes Weak Priors
How it works: Use chapter titles or show notes as weak priors for likely ad zones without allowing direct auto-skip decisions.

User perception: Higher recall in structured podcasts with chaptered ad slots.

Implementation: Add metadata priors as low-weight inputs in classifier/fusion.

### 18) Multilingual and Code-Switch Lexical Expansion
How it works: Expand lexical detector to multilingual CTA/disclosure templates and transliterated promo constructs.

User perception: Better performance on bilingual shows and non-English ad reads.

Implementation: Add locale-aware pattern packs with confidence caps until replay-validated.

### 19) Micro-Ad Detector (<15s)
How it works: Explicit short-span decoder mode for brief injected stingers and one-line promos.

User perception: More complete skip coverage on dynamic insertion inventories.

Implementation: Relax minimum span constraints only when strong anchor patterns exist.

### 20) Branded Segment Detector (>120s)
How it works: Handle long branded “storytelling ads” using continuity cues and intent/ownership refinement.

User perception: Better handling of native-style reads that exceed normal ad length assumptions.

Implementation: Extend split logic to preserve long spans when commercial intent remains consistently high.

### 21) Discourse-Aware Span Stitching
How it works: Merge adjacent candidate spans based on discourse continuity and sponsor entity continuity, not only fixed time gaps.

User perception: Less fragmented markers and cleaner skip UX.

Implementation: Add stitching heuristic in decoder post-processing stage.

### 22) Budget-Aware FM Window Selection
How it works: Allocate FM compute budget to highest-uncertainty windows and candidate boundaries rather than uniform scanning.

User perception: Better quality under same compute budget; fewer misses where it matters.

Implementation: Add priority scheduler for FM backfill windows using uncertainty and potential impact scores.

### 23) First-Minutes Preflight Focus
How it works: Prioritize aggressive analysis for first N minutes (where user trust is most sensitive) before deeper episode coverage.

User perception: Early playback feels reliable quickly.

Implementation: Re-prioritize hot-path + backfill queue ordering for near-playhead early segments.

### 24) Explainability Reason Codes In UI
How it works: Every skip marker exposes concise reason codes (URL anchor, FM consensus, fingerprint match, user rule).

User perception: Higher trust and easier correction decisions.

Implementation: Surface existing provenance in lightweight UX components.

### 25) One-Tap Skip Undo and Boundary Nudge
How it works: Let users instantly undo a skip and optionally indicate “start too early” or “end too late.”

User perception: Mistakes feel recoverable and app feels cooperative.

Implementation: Add ergonomic controls in now playing and pipe corrections into boundary feedback loop.

### 26) Per-Show Aggressiveness and Intent Policy Presets
How it works: Offer simple presets (Conservative/Balanced/Aggressive) plus intent policy toggles (skip third-party only vs include house promos).

User perception: Users keep control over what “ad” means for them.

Implementation: Map presets to threshold/policy profiles in `SkipPolicyMatrix` and fusion thresholds.

### 27) Replay Quality Gates for Precision/Recall/Boundary
How it works: Enforce minimum precision/recall and boundary MAE thresholds in CI; block regressions on real fixtures.

User perception: Fewer regressions between app updates.

Implementation: Expand existing replay harness assertions and rollout checks.

### 28) Cohort Canarying by OS + Prompt + Schema
How it works: Ship detection cohorts as explicit rollout units with automated canary metrics and kill switches.

User perception: More stable behavior across iOS updates.

Implementation: Extend current cohort controls and telemetry aggregation around FM and fallback behavior.

### 29) Edge-Case Fixture Expansion
How it works: Add dedicated fixture sets for dynamic insertion seams, bilingual reads, no-music host reads, and regulated medical copy.

User perception: Better reliability in edge scenarios that currently create surprises.

Implementation: Expand fixture corpus and targeted replay suites.

### 30) Pipeline Watchdog and Graceful Degradation
How it works: Detect failing subsystems (FM unavailable, SoundAnalysis degraded, transcription lag) and switch to safe fallback modes with clear status.

User perception: Predictable behavior instead of silent failures.

Implementation: Add subsystem health scoring and fallback policy routing.

## Winnowing Process

I scored each idea on six dimensions (1–5 each):
1. User trust impact
2. Recall and precision lift potential
3. Boundary quality lift
4. Implementation pragmatism
5. Architectural fit with current code
6. Rollout safety

The strongest ideas were those with high trust impact and high architectural fit that can ship in phases.

## Top 5 (Best to Worst)

## 1) Replay-Calibrated Confidence + Uncertainty Defer Band

Why this is #1:
- Today, several decisions are effectively thresholded on partly uncalibrated scores. Calibrating those scores and adding a defer zone directly attacks the most user-visible failure mode: confident wrong skips.
- It improves both precision and trust without sacrificing recall, because uncertain cases become “detect/show/defer” instead of “act and risk bad skip.”
- It is highly accretive: this sits on top of `BackfillEvidenceFusion`, `DecisionMapper`, and `SkipEligibilityGate` rather than replacing them.

How it would work:
- Build calibration tables from replay outcomes by source composition buckets (for example: FM+anchor, lexical+acoustic only, fingerprint transfer).
- Map raw score to calibrated probability.
- Introduce uncertainty defer band where markers remain visible but auto-skip is delayed until more evidence arrives.
- Re-run deferred decisions at backfill checkpoints and after new evidence arrives.

User-perceived outcome:
- Fewer “why did it skip that?” moments.
- More stable confidence semantics.
- Better confidence in leaving auto-skip on.

Implementation path:
1. Add offline calibration job from replay logs.
2. Introduce calibration lookup in `DecisionMapper` behind feature flag.
3. Add `defer` eligibility in `SkipPolicyMatrix` and orchestrator logic.
4. Roll out in shadow mode, compare precision/recall/MAE before promotion.

Why I’m confident:
- This is a classic high-leverage improvement in probabilistic systems.
- It needs no new model dependency and can be validated entirely with existing replay infrastructure.

## 2) Granular User Correction Learning (Scoped, Durable, Safe)

Why this is #2:
- Ad-skip products are trust products. Correction handling is the trust flywheel.
- The pipeline already has correction primitives, but broad scopes create overgeneralization risk; making scopes precise is a direct quality improvement.
- It reduces repeat mistakes and gives users agency without requiring them to tune complex settings.

How it would work:
- Persist corrections with explicit scope and TTL/decay:
  - exact span
  - phrase on show
  - sponsor on show
  - domain ownership on show
- Apply corrections in three places:
  - suppression/boost in skip decisions
  - boundary adjustments
  - sponsor memory promotion/demotion
- Protect against poisoning by requiring repeated confirmations for global-ish rules.

User-perceived outcome:
- “When I correct it, it stays corrected.”
- Fewer repeated false positives on the same show.
- More confidence that auto-skip aligns with user intent.

Implementation path:
1. Extend `UserCorrectionStore` schema and migration.
2. Wire scoped lookups into `SkipOrchestrator` and backfill fusion path.
3. Add one-tap correction affordances in now playing and ad marker UI.
4. Add replay tests for correction scope behavior.

Why I’m confident:
- The infrastructure already exists; this is primarily a precision and ergonomics upgrade.
- User trust gains are immediate and compounding.

## 3) Boundary Accuracy Loop From Real Listening Behavior

Why this is #3:
- Boundary errors are often more annoying than outright misses because they clip content.
- Playhead already logs behavior that can be converted into supervisory signals.
- This improves UX quality even when recall/precision stay constant.

How it would work:
- Use immediate post-skip seeks as boundary feedback:
  - rewind soon after skip suggests start was too early
  - quick forward suggests end was too late
- Feed offsets into per-show boundary priors and a two-pass resolver:
  - pass 1 structural cues (pause/music/speaker)
  - pass 2 local discourse-aware fine alignment
- Keep hard bounds to avoid drift/overfitting.

User-perceived outcome:
- Less clipped host speech.
- Cleaner return points after ads.
- Skips feel “human tuned” rather than robotic.

Implementation path:
1. Add boundary feedback extraction from playback telemetry.
2. Extend `TimeBoundaryResolver` with per-show offset priors.
3. Add second local snap pass with small search window.
4. Validate via boundary MAE metrics in replay harness.

Why I’m confident:
- Uses high-signal real behavior data already available.
- Can be rolled out conservatively and measured objectively.

## 4) Refusal-Resistant Sensitive-Content Path (FM Abstain Fallback)

Why this is #4:
- Known blind spot: regulated medical/pharma/therapy windows can produce FM refusal/abstain.
- Users experience these misses as obvious failures, so fixing them has outsized trust value.
- This extends existing sensitive routing and permissive path rather than introducing risky new external dependencies.

How it would work:
- Expand `SensitiveWindowRouter` triggers for refusal-prone windows.
- If FM abstains/refuses, run strict deterministic fallback requiring:
  - strong commercial anchors (URL/promo/disclosure)
  - corroborating acoustic/transition cues
  - conservative threshold for skip eligibility
- Keep fallback cohort-gated and monitored.

User-perceived outcome:
- Fewer conspicuous misses on certain ad categories.
- Better consistency of skip behavior across content types.

Implementation path:
1. Add refusal-risk lexical probes.
2. Implement abstain fallback branch in backfill decision path.
3. Expand edge-case replay fixtures for regulated ad reads.
4. Roll out via shadow cohort with kill switch.

Why I’m confident:
- Architecture already has most primitives (`SensitiveWindowRouter`, permissive classifier, redaction paths).
- Conservative gating keeps false-positive risk bounded.

## 5) Sponsor Memory + Multi-Feature Fingerprint Upgrade

Why this is #5:
- Recurring ad copy is one of the easiest compounding wins in podcast ad detection.
- Current sponsor and fingerprint systems are solid but can capture more repeat value with aliasing and multi-feature matching.
- This improves recall and early detection particularly for frequent shows.

How it would work:
- Canonicalize sponsor aliases in `SponsorKnowledgeStore`.
- Upgrade fingerprint scoring to combine text MinHash, acoustic signature hints, and CTA sequence features.
- Add transfer confidence tiers (full transfer, partial transfer, seed-only) to protect precision.

User-perceived outcome:
- Faster, earlier catches on recurring sponsors.
- Fewer misses when ad scripts vary slightly between episodes.

Implementation path:
1. Add alias graph + promotion rules to sponsor memory.
2. Extend `AdCopyFingerprintMatcher` with late-fusion scoring.
3. Implement transfer tiers and stronger alignment diagnostics.
4. Replay-test with repeated-ad fixtures across multiple shows.

Why I’m confident:
- Strong architectural fit with existing memory and fingerprint components.
- Incremental rollout possible without changing core hot path semantics.

## Why These 5 Win

These five together produce the best pragmatic outcome:
- #1 improves decision reliability globally.
- #2 turns user corrections into durable quality gains.
- #3 fixes boundary pain that users notice immediately.
- #4 closes a known high-visibility blind spot.
- #5 compounds quality over repeated listening patterns.

This set is both high-impact and deployable in stages with replay-backed safety gates.
