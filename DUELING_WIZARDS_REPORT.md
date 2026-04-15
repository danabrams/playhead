# Dueling Idea Wizards Report: Playhead Ad Detection

## Executive Summary

Two AI models (Claude Code Opus 4.6 and OpenAI Codex GPT-5.3) independently studied the Playhead codebase and generated 30 ideas each for improving ad detection, winnowed to 5 each (10 total). Near-zero overlap between the top-5 lists: Claude focused on **new signal sources** (metadata, acoustic features), Codex focused on **decision calibration and feedback loops**. After adversarial cross-scoring and a reveal phase with genuine concessions from both sides, **3 consensus winners** emerged, **2 strong ideas with caveats**, and **2 ideas that should be deprioritized**.

**Top 3 consensus picks:**
1. RSS / show-notes sponsor pre-seeding (new signal, cold-start win)
2. Music-bed envelope as dominant boundary cue (boundary accuracy, immediate UX win)
3. Granular user correction learning (trust flywheel, scoped persistence)

## Methodology

- **Agents used:** Claude Code (Opus 4.6) + Codex (GPT-5.3)
- **Ideas generated:** 30 per agent, winnowed to 5 each
- **Scoring:** Adversarial cross-model 0-1000 scale
- **Phases:** study -> ideate -> cross-score -> reveal -> synthesize
- **Focus:** Ad detection pipeline improvements
- **Duration:** ~35 minutes total

## Score Matrix

| Idea | Origin | Self-Rank | CC Score | COD Score | Post-Reveal CC | Post-Reveal COD | Verdict |
|---|---|---|---|---|---|---|---|
| RSS / show-notes pre-seeding | CC | #1 | -- | 842 | 835 | -- | **CONSENSUS WIN** |
| Music-bed envelope boundary | CC | #4 | -- | 888 | 820 | -- | **CONSENSUS WIN** |
| Granular correction learning | COD | #2 | 780 | -- | -- | still #1 | **CONSENSUS WIN** |
| Chapter-marker ingestion | CC | #2 | -- | 804 | 785 | -- | **STRONG** |
| Episode-level sanity check | CC | #5 | -- | 830 | 730 | -- | **STRONG** |
| Boundary feedback loop | COD | #3 | 710 | -- | -- | still top-3 | Good, with caveats |
| Sponsor memory + fingerprints | COD | #5 | 670 | -- | -- | decomposed | Good (unbundle first) |
| Replay-calibrated confidence | COD | #1 | 645 | -- | -- | dropped to #3 | Overrated by originator |
| Refusal-resistant FM path | COD | #4 | 590 | -- | -- | dropped to #5 | Mostly already shipped |
| Host-voice counter-classifier | CC | #3 | -- | 612 | 620 | -- | **KILLED** (premature) |

## Consensus Winners (scored 700+ by both agents)

### 1. RSS / Show-Notes Sponsor Pre-Seeding
**CC: 835 | COD: 842 | Avg: 839**

Parse RSS `<description>`, `<itunes:summary>`, and `<content:encoded>` for sponsor names, promo codes, and URLs at episode enqueue time. Inject into per-episode ephemeral sponsor lexicon consumed by `LexicalScanner` during hot path.

**Why both agree:** Deterministic, cheap, fully on-device, uniquely addresses cold-start episodes. Both agents independently flagged the corroboration requirement (metadata cannot act alone as a skip trigger).

**Key caveat from Codex:** Show notes can be stale or diverge from dynamic-insertion audio. Must be weak priors only, not direct actionable evidence. Bounded fusion weight (0.15 cap suggested).

**Implementation surface:** RSS parser + provenance tag + lexicon-merge point. `Episode` persistence needs to retain description metadata (currently dropped).

### 2. Music-Bed Envelope as Dominant Boundary Cue
**CC: 820 | COD: 888 | Avg: 854**

When a candidate ad region is flanked by symmetric music-bed onset/offset patterns (symmetry score >= 0.6, both scores >= 0.7), treat envelope peaks as primary boundary snap targets with elevated weight (0.45) and 10s snap radius.

**Why both agree:** Boundary errors are the most viscerally annoying failure mode. Existing `musicBedOnsetScore`/`musicBedOffsetScore` features are already computed but underutilized. Low complexity, high felt-UX improvement.

**Key disagreement (resolved):** Claude originally ranked this #4; conceded after reveal that boundary quality matters more than metadata harvesting for user trust. Codex's 888 is slightly optimistic per Claude (guardrails limit applicability to jingle-heavy shows).

**Implementation surface:** Bracket detection scanner + new cue class in `TimeBoundaryResolver`. Easy to validate via boundary-MAE in replay harness.

### 3. Granular User Correction Learning
**CC: 780 | COD: #1 pick | Avg: ~780+**

Upgrade `UserCorrectionStore` with typed scopes (`episode-span`, `phrase-on-show`, `sponsor-on-show`, `domain-ownership-on-show`) and TTL/decay semantics. Apply in skip decisions, boundary adjustments, and sponsor memory promotion.

**Why both agree:** User corrections are the highest-quality ground truth the system will ever see. Current correction primitives are coarse. Scoped persistence turns a blunt tool into a precision instrument.

**Key caveat from Claude:** Users don't think in scope taxonomies -- the system must infer scope from evidence context, not burden the user. Routing logic is trickier than the proposal admits.

**Implementation surface:** Schema extension to existing `UserCorrectionStore`, consume in `SkipOrchestrator`, `DecisionMapper`, and backfill fusion.

## Strong Ideas (one-sided high scores, worth pursuing)

### 4. ID3 / Podcasting 2.0 Chapter-Marker Ingestion
**COD: 804 | CC revised: 785**

Chapter-labeled ad breaks are precision gold when available. Natural companion to RSS pre-seeding (same parser pass at enqueue time). Limited by inconsistent publisher adoption.

### 5. Episode-Level Ad-Inventory Sanity Check
**COD: 830 | CC revised: 730**

Statistical outlier detection on total ad time vs per-show history. Doesn't improve detection directly but catches catastrophic silent failures and modulates UX accordingly. Valuable safety net for opaque FM behavior changes across iOS updates.

## Contested Ideas (decompose before building)

### 6. Boundary Feedback From Listening Behavior
**CC: 710 | COD: agrees post-reveal**

Both agree the signal exists but is noisier than initially framed. **Resolution:** Split into (a) deterministic two-pass boundary snap (ship first, low risk) and (b) behavioral learning from post-skip seeks (ship second, needs noise engineering).

### 7. Sponsor Memory + Multi-Feature Fingerprints
**CC: 670 | COD: agrees to decompose**

**Resolution:** Ship alias canonicalization first (easy, high ROI). Transfer-tier cleanup second. Acoustic fingerprinting is a longer-horizon R&D effort, not a near-term feature.

## Killed Ideas

### Host-Voice Editorial Counter-Classifier
**COD: 612 | CC conceded: 620**

Both agents agreed post-reveal this is premature. No production speaker-embedding pipeline exists in the codebase. Host-read ads without tidy anchors are exactly where it backfires. Claude conceded this was their biggest error: "I was seduced by the architectural symmetry argument."

### Replay-Calibrated Confidence (as originally framed)
**CC: 645 | COD: dropped from #1 to #3**

Codex's own #1 pick was their biggest concession post-reveal. Claude's critique -- that it bundles two ideas, the defer band largely already exists, and calibration has real MLOps maintenance cost -- was accepted. Codex decomposed it into: (a) replay-calibrated confidence as standalone P1, (b) explicit uncertainty defer policy as P2.

## Meta-Analysis

### Model Biases Revealed

| Bias | Claude (Opus 4.6) | Codex (GPT-5.3) |
|---|---|---|
| **Signal philosophy** | New data sources, metadata harvesting | Tune existing decision surfaces |
| **Risk appetite** | Higher (host-voice embeddings, new ML infra) | Lower (sharpen what exists, replay-validate) |
| **Quality lens** | Capability per effort | Felt quality per risk |
| **Bundling tendency** | Clean single ideas | 2-3 sub-ideas per slot |
| **Blind spot** | Overvalued architectural symmetry as a reason to build | Overvalued ideas that overlap with shipped infrastructure |

### Key Dynamics

- **Largest concession:** Claude dropping host-voice counter from #3 to #5 after Codex pointed out no speaker-embedding pipeline exists. "I got it wrong" -- rare and high-signal.
- **Largest upgrade:** Claude moving music-envelope boundary from #4 to #2 after accepting Codex's "boundary errors are the most visceral failure mode" argument.
- **Codex's self-correction:** Dropping their own #1 (calibration) after accepting Claude's critique about bundling and maintenance cost.
- **Methodological insight from Claude:** "Codex and I applied different implicit loss functions -- mine 'capability-per-effort,' theirs 'felt-quality-per-risk.' For an MVP-stage product, theirs is the more appropriate lens."

### Where Adversarial Pressure Added Value

1. **Forced decomposition** of bundled ideas (Codex's calibration+defer, sponsor+fingerprint+tiers)
2. **Exposed premature proposals** (host-voice counter was killed by implementation-reality critique)
3. **Rebalanced priorities** (boundary quality elevated over metadata harvesting by user-perception argument)
4. **Validated cross-model convergence** on corrections and RSS pre-seeding

## Recommended Next Steps

**Implementation order (both agents converged on this post-reveal):**

1. **RSS / show-notes sponsor pre-seeding** -- Largest recall lift at cheapest layer. Cold-start win. Ship first.
2. **Music-bed envelope boundary scoring** -- Most visible UX improvement. Leverage existing features. Ship second.
3. **Chapter-marker ingestion** -- Same parser pass as #1. Concentrated value on structured podcasts. Ship alongside #1.
4. **Episode-level ad-inventory sanity check** -- Safety net before rolling out aggressive detection improvements. Ship before expanding skip aggressiveness.
5. **Granular correction learning** -- Trust flywheel. Schema extension on existing store. Ship as corrections UX matures.
6. **Sponsor alias canonicalization** -- Easy win extracted from decomposed fingerprint proposal.
7. **Two-pass boundary snap** -- Deterministic improvement extracted from decomposed behavioral loop.

Items 1-3 are metadata/signal improvements. Items 4-5 are reliability/trust improvements. Items 6-7 are precision refinements. This ordering balances new capability against risk management.
