# Playhead Wizard-Idea Scoring (Codex on Claude Code list)

Date: 2026-04-29

Scoring rubric (0-1000):
- Idea quality/intelligence
- Real user + implementation utility in Playhead
- Practicality within current actor architecture + on-device mandate
- Utility vs complexity/tech-debt tradeoff
- Fit with design identity (Quiet Instrument: calm, trustful, non-gimmicky)

I scored these against current code reality, not just conceptual merit.

---

## Top 5 Scores

## 1) Predictive Pre-Analysis of Downloaded Episodes
**Score: 620 / 1000**

### What’s strong
- Concept is excellent: precomputed ad cues before first play is a genuine experiential win.
- Strong fit with on-device strategy and scheduler pipeline.
- Correct product intuition: first-skip latency is a trust-defining moment.

### Critical weakness
- This is no longer a greenfield idea; large parts are already implemented.
- `DownloadManager` already enqueues analysis after download completion (`setAnalysisWorkScheduler`, `enqueueAnalysisIfNeeded`, background completion path).
- `AnalysisWorkScheduler` is already explicitly pre-analysis-oriented and lane-based.

### Why the score is not higher
- As a *new improvement proposal*, much of its claimed value is already captured.
- Remaining delta is mostly polish: visibility/UX for readiness, tuning, and coverage quality.
- So: high conceptual quality, but low incremental originality against current Playhead state.

### Best accretive next step if pursued
- Make readiness legible and deterministic for users (quiet “ready” affordance + ETA confidence), not more pipeline plumbing.

---

## 2) Cross-Episode Transcript Search
**Score: 905 / 1000**

### What’s strong
- Very high real-world value for power listeners.
- High switching-cost feature; deeply aligned with transcript-first product moat.
- Architecturally straightforward: FTS5 table + triggers + `searchTranscripts(query:)` already exist.

### Practicality
- This is among the most implementable high-impact features in the backlog.
- Most risk sits in UX polish and navigation semantics, not core infra.

### Weaknesses
- Not novel relative to current roadmap; it is already tracked (`playhead-90i`) with detailed implementation criteria.
- Must avoid turning UI into a “results dashboard”; should remain editorial and calm.

### Why still scored high
- Even if not novel, it is a genuinely excellent idea for Playhead and a pragmatic execution target now.

---

## 3) Episode Summaries via On-Device Foundation Model
**Score: 755 / 1000**

### What’s strong
- Solves real backlog triage pain (“what is this episode actually about?”).
- Uses on-device FM advantage in a user-facing way that can feel premium when done well.
- Can be scoped safely with deterministic sampling and strict schema outputs.

### Risks / tradeoffs
- Hallucination/over-summary risk can erode trust fast.
- FM availability and refusal behavior still introduce device/locale variability.
- Adds another persistent artifact class to maintain (versioning, invalidation, migration).

### Design-identity fit
- Can fit Quiet Instrument if subtle and optional.
- Fights identity if it becomes noisy “AI summary cards everywhere.”

### Practical verdict
- Strong candidate, but only after guardrails:
  - confidence gating,
  - explicit fallback copy,
  - very restrained UI placement,
  - robust regression tests on known-episode fixtures.

---

## 4) Shareable Transcript Quotes
**Score: 710 / 1000**

### What’s strong
- Low engineering risk, fast to ship, high delight for a vocal user segment.
- Good organic distribution channel if the share artifact is tasteful.
- Uses existing transcript/timestamp data; little systems risk.

### Weaknesses
- Narrower utility than search/sleep/reliability improvements.
- Can drift toward social garnish rather than core peace-of-mind listening utility.
- Needs careful legal/product posture around quote attribution/deep-link behavior.

### Design-identity fit
- Potentially good (editorial, typographic), if understated.
- Bad if it turns into growth-hacky branding chrome.

### Practical verdict
- Good v0.2/v0.3 adjacent feature; not top-priority versus reliability + search + table-stakes ergonomics.

---

## 5) Sleep Timer with Intelligent Stop Points
**Score: 870 / 1000**

### What’s strong
- Table-stakes feature with a distinctly Playhead execution twist.
- Daily-use utility is high for many podcast listeners.
- Elegant use of existing signal (`pauseProbability`) and existing playback fade/seek mechanics.

### Practicality
- Strong architecture fit: mostly localized changes (Playback + query + UI controls).
- Bounded fallback path makes behavior robust even when transcript/features are incomplete.

### Weaknesses
- This is already explicitly tracked (`playhead-g21`) with substantial implementation detail.
- Boundary choice heuristics can be subtle; must test thoroughly against speech cutoffs and ad-edge cases.

### Why high score anyway
- Even if not new, it is very good product work that improves perceived craftsmanship immediately.

---

## Ranking (by score)
1. Cross-Episode Transcript Search — **905**
2. Sleep Timer with Intelligent Stop Points — **870**
3. Episode Summaries via On-Device FM — **755**
4. Shareable Transcript Quotes — **710**
5. Predictive Pre-Analysis of Downloaded Episodes — **620**

---

## Candid Notes on the 25 “also-rans”

Most-principled cuts:
- Rejecting gamification/streaks and overt metrics noise is correct for Quiet Instrument.
- Rejecting confidence chrome overlays is directionally right unless hidden behind advanced/debug surfaces.
- Calling out warm-resume acceleration as already addressed is fair.

Where I think the other model underweighted value:
- “Graceful degradation UI — gap is minor”: likely too dismissive. Current beads and surface-status work indicate this remains a trust-critical area.
- “Offline feed resilience — minor”: undervalued for commuters/travel/offline-first users.
- “Sponsor transparency report — doesn’t improve listening”: maybe true for broad audience, but a compact trust ledger could reduce false-positive anxiety without violating aesthetic, if kept quiet.

Where it overstates novelty:
- Predictive pre-analysis is described as if absent; substantial wiring already exists.
- Cross-episode transcript search is presented as a new strategic discovery, but it is already a defined open feature bead.
- Sleep timer idea is similarly already represented as an open bead with transcript-aware logic.

---

## Bottom line

The list has good product taste overall, but it is partially stale relative to current Playhead implementation/backlog reality. The strongest immediate execution target remains **Transcript Search UI** (already infra-ready, high user value), followed by **Sleep Timer** (table-stakes plus Playhead differentiation). FM Summaries are promising but should be treated as a carefully gated trust feature, not a default AI surface.
