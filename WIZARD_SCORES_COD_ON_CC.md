# Playhead — Codex Scores on Claude's Ad-Detection Top 5

**Date:** 2026-04-14
**Input evaluated:** `WIZARD_IDEAS_CC.md` (Claude Code top 5)

## Scoring Method

Each idea gets a single **0–1000 overall score** reflecting:
- Real user impact in daily listening
- Expected lift to recall / precision / boundary accuracy
- Practicality in the current Playhead architecture
- Complexity and tech-debt burden vs benefit
- Reliability risk and rollout safety

These are not "is the idea clever" scores; they are "should we actually build this now" scores.

## Final Ranking (Best to Worst)

| Rank | Idea | Score |
|---|---|---:|
| 1 | Music-bed envelope as dominant boundary cue | **888** |
| 2 | RSS / show-notes sponsor pre-seeding | **842** |
| 3 | Episode-level ad-inventory sanity check | **830** |
| 4 | ID3 / Podcasting 2.0 chapter-marker ingestion | **804** |
| 5 | Host-voice editorial counter-classifier | **612** |

---

## 1) RSS / Show-Notes Sponsor Pre-Seeding

**Score: 842 / 1000**

What is strong:
- High leverage on cold-start episodes where show priors are weak.
- Fits existing lexical/evidence architecture well.
- Deterministic, cheap compute, fully on-device.

What is weak:
- Show notes are often stale, templated, or include sponsors not actually read in that episode audio.
- Dynamic insertion can diverge from feed text, especially for geographically targeted ads.

Implementation reality in this repo:
- Feed parser already captures `description/showNotes`, but `Episode` persistence currently drops this metadata.
- You need plumbing from feed ingestion to analysis inputs before this affects detection.
- Must add strict corroboration rules (metadata cannot act alone as a skip trigger).

Complexity vs payoff:
- Good trade if scoped to **weak prior only** and bounded fusion weight.
- Bad trade if treated as pre-anchored evidence with too much authority.

Candid verdict:
- Strong idea, but only if implemented as a conservative prior channel, not as direct actionable evidence.

---

## 2) ID3 / Podcasting 2.0 Chapter-Marker Ingestion

**Score: 804 / 1000**

What is strong:
- Very high precision when chapter labels are good.
- Can materially improve boundary accuracy and reduce FM work on covered episodes.
- Natural fit for boundary snapping logic.

What is weak:
- Coverage is inconsistent across publishers.
- Chapter semantics are noisy (“support us”, “housekeeping”, “cold open with sponsor mention”).
- ID3 CHAP parsing from audio files is extra engineering surface.

Implementation reality in this repo:
- Feed parser already parses inline `podcast:chapter`; external chapter URLs are noted but not fetched.
- Chapters are not persisted to `Episode`, and ad detection does not currently consume them.
- This idea is partly started, but integration is non-trivial end-to-end.

Complexity vs payoff:
- Good for shows that publish clean chapters.
- Moderate overall upside because corpus coverage is limited.

Candid verdict:
- Smart and practical, but not as universally impactful as it appears.

---

## 3) Host-Voice Editorial Counter-Classifier

**Score: 612 / 1000**

What is strong:
- Targets a real false-positive pattern: editorial host monologues misread as ads.
- In theory, this could raise precision on talk-heavy shows.

What is weak:
- Highest implementation and reliability risk of the five.
- Host-read ads are exactly where this signal can backfire.
- Co-host rotation and guest-heavy formats make stable host profiling hard.

Implementation reality in this repo:
- Current system has speaker-change proxy and optional validated labels path, but validated ASR speaker labels are effectively not active.
- No robust, production speaker-embedding pipeline exists in current code.
- Building and maintaining host voice profiles is a substantial new subsystem.

Complexity vs payoff:
- Expensive to do correctly and easy to get wrong.
- High long-term maintenance burden for uncertain incremental gain.

Candid verdict:
- Intellectually good, practically premature for this codebase right now.

---

## 4) Music-Bed Envelope as Dominant Boundary Cue

**Score: 888 / 1000**

What is strong:
- Directly attacks one of the most user-visible failure modes: bad boundaries.
- Leverages existing features (`musicBedOnsetScore`, `musicBedOffsetScore`) already in pipeline.
- Can be rolled out safely with clear gating conditions.

What is weak:
- Music-heavy editorial shows can create false boundary candidates.
- Needs robust guardrails to avoid over-snapping to unrelated music events.

Implementation reality in this repo:
- `TimeBoundaryResolver` already supports directional music cues, currently as moderate-weight inputs.
- This is mostly a scoring policy upgrade plus bracket-detection logic, not a framework change.
- Easy to A/B in replay harness and boundary-MAE metrics.

Complexity vs payoff:
- Excellent trade.
- Low-to-moderate complexity for a high, immediately felt UX win.

Candid verdict:
- Best proposal in Claude’s list for near-term detection quality improvement.

---

## 5) Episode-Level Ad-Inventory Sanity Check

**Score: 830 / 1000**

What is strong:
- Adds system-level self-awareness for catastrophic misses and over-detection.
- Strong trust protection mechanism during model drift or OS behavior changes.
- Pairs well with correction and replay workflows.

What is weak:
- Does not itself improve detection quality; it is a safety governor.
- Outlier logic can be noisy for specials, live episodes, or format changes.

Implementation reality in this repo:
- There is already per-podcast planner state and counters in backfill/coverage paths.
- Extending this to anomaly flags is feasible without architectural churn.
- UI behavior changes (downgrading auto-skip) must be conservative to avoid user frustration.

Complexity vs payoff:
- Good reliability ROI if thresholds and exemptions are tuned carefully.
- Moderate complexity, mostly in policy tuning and UX behavior.

Candid verdict:
- Very good safety-net idea; less glamorous than boundary improvements but high practical value.

---

## Bottom-Line Take

Claude’s top 5 are overall strong and pragmatic. The two best engineering bets for immediate core-quality gain are:
1. **Music-envelope-dominant boundary scoring**
2. **RSS/show-notes pre-seeding (as weak priors only)**

The **host-voice counter-classifier** is the only one I would explicitly deprioritize for now due to implementation risk and fragile assumptions relative to current pipeline maturity.
