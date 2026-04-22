# narl Phase 1 — Counterfactual Evaluation Design

*Design for the counterfactual evaluation methodology that unblocks `playhead-narl` (metadata-lexicon + prior-shift + FM-scheduling activation). Phase 1 is structured as three sister beads.*

## Frame

Three activation points are built and tested but gated off in production behind `MetadataActivationConfig.counterfactualGateOpen = false`. The gate exists because flipping these on without evidence could degrade precision — especially on conversational shows like Conan O'Brien Needs a Friend where ad boundaries blend into host speech.

This design specifies the evaluation methodology that produces the evidence needed to flip (or not flip) the gate.

**Phase 1 splits into three beads:**
1. `playhead-narl-harness` — frozen-trace replay evaluator + corpus builder + first report.
2. `playhead-narl-shadow-capture` — production dual-run FM capture (the only honest path to evaluating `fmSchedulingEnabled`).
3. `playhead-narl-approval` — per-episode approval recommendation, informed by the harness report.

**Explicit non-goals for Phase 1:**
- Committed run-over-run eval history in git (trend log is local-only).
- Automated flip execution (recommendation only — deferred to possible Phase 2).

---

## Section A — Methodology decisions

Decisions locked in during brainstorming on 2026-04-21. Recorded here for downstream implementation.

### A.1 How to generate the `.allEnabled` counterfactual

**Chosen: frozen-trace replay (B).** The harness replays the deterministic stages (fusion + gate + policy) under each config against a `FrozenTrace` snapshot. Rejected alternatives:

- *Reingest from audio* — truly faithful but slow, and cached audio is sometimes evicted.
- *Dual-run shadow capture alone* — covers all three flags but blocks the harness on production code changes and listening cadence.

Replay handles two of three flags bit-for-bit deterministically:
- `lexicalInjectionEnabled` — lexicon expansion + regex matching, purely local computation.
- `classifierPriorShiftEnabled` — math over existing classifier scores.

The third flag (`fmSchedulingEnabled`) cannot be honestly replayed: it changes *which windows FM ever ran on*, and frozen traces only contain FM responses for the windows `.default` selected. Phase 1 closes that gap via Bead 2's shadow capture (see §C), which populates FrozenTrace with shadow FM responses for the windows `.allEnabled` would have chosen.

### A.2 Form factor

**Chosen: Swift Testing target (A).** The harness lives under `PlayheadTests/Services/ReplaySimulator/`, reusing the existing `FrozenTrace`, `ReplaySimulator`, `ReplayMetrics`, and scaffolded `CounterfactualEvaluator` types. Runs via `xcodebuild test` with the `PlayheadFastTests` plan.

Rejected: standalone CLI package (no existing scaffolding, duplicates types), in-app Debug panel (couples eval to device, poor CI story).

### A.3 Corpus / fixture shape

**Chosen: pre-built FrozenTrace JSON fixtures (F1).**

- **Source of truth:** an exported `.xcappdata` bundle containing `analysis.sqlite` + `decision-log.jsonl` + `corpus-export.jsonl` + (post-Bead-2) `shadow-decisions.jsonl`.
- **Corpus builder:** an env-gated Swift Testing `@Test` that reads the bundle and emits one `FrozenTrace-<episodeId>.json` per episode into `PlayheadTests/Fixtures/NarlEval/<date>/`.
- **Eval runner:** reads only the committed FrozenTrace JSONs. No sqlite dependency at eval time, no `.xcappdata` dependency at CI time.

Rejected (F2): reading raw jsonls + sqlite at each eval run. Committing an 18.9 MB sqlite per corpus snapshot is poor git hygiene, and eval becomes coupled to AnalysisStore's schema — every migration invalidates old fixtures.

### A.4 Ground truth construction

Per episode, the ground-truth positive ad-window set is:

1. Start with auto-detected spans from `decision` rows in corpus-export.jsonl.
2. Subtract any span overlapped by a `falsePositive` correction (`exactTimeSpan` scope).
3. Add every `falseNegative` correction (`exactTimeSpan` scope).
4. For ordinal-range `exactSpan` corrections that are not whole-asset vetoes: resolve atom ordinals to time via the FrozenTrace atoms, then treat as window-level.
5. **Exclude the episode entirely** if it carries an `exactSpan:<assetId>:0:INT64_MAX` veto — user flagged that episode's data as unreliable.

### A.5 Metrics

**Chosen: M3 — window-level IoU at multiple thresholds + second-level.**

- **Window-level:** for τ ∈ {0.3, 0.5, 0.7}, a predicted window is TP if it overlaps some ground-truth window with IoU ≥ τ. Report precision, recall, F1 per τ. Also mean IoU over matched pairs.
- **Second-level:** project all windows into the second timeline, count ad-seconds correctly classified vs misclassified. Report precision, recall, F1.
- **Rollups:** per-(show, config, metric). Shows: Diary of a CEO, Conan O'Brien Needs a Friend, ALL. Configs: `.default`, `.allEnabled`.

Rationale: the two test shows behave differently on the boundary-precision axis. A single metric would hide whichever failure mode dominates. The multi-threshold view separates "did the window exist" (low τ) from "were the boundaries clean" (high τ), and second-level reports total ad-time coverage independent of window segmentation.

### A.6 Output

**Chosen: O1 + local trend log.**

Each run writes:
- `.eval-out/narl/<timestamp>/report.json` — machine-readable artifact.
- `.eval-out/narl/<timestamp>/report.md` — human-readable rendered tables.
- Append one row per (show, config, metric) to `.eval-out/narl/trend.jsonl`.

All paths under `.eval-out/` are gitignored. The trend log provides "proof detection is improving over time" without committing eval history to the repo. When a gate-flip decision is made, the markdown report is copied by hand into the relevant bead's closing note or into a design doc that justifies the flip.

---

## Section B — Bead 1: `playhead-narl-harness`

### Scope

1. **Corpus builder** (`NarlEvalCorpusBuilderTests.swift`)
   - Env-gated Swift Testing `@Test` (e.g. `@Test(.enabled(if: env("PLAYHEAD_BUILD_NARL_FIXTURES") == "1"))`).
   - Input: path to an `.xcappdata` bundle.
   - Output: one `FrozenTrace-<episodeId>.json` per episode under `PlayheadTests/Fixtures/NarlEval/<date>/`, committed by Dan when new episodes are tagged.

2. **Eval runner** (`NarlEvalHarnessTests.swift`)
   - Swift Testing suite, runs in `PlayheadFastTests`.
   - Iterates fixture JSONs, replays fusion/gate/policy under `.default` and `.allEnabled`, computes §A.5 metrics, writes §A.6 artifacts.
   - Asserts only: (a) both configs ran to completion without error, (b) report artifacts were written.
   - **Metric regressions do not fail the build** — they are visible in the artifact for human judgment.

3. **Ground truth construction module**
   - Pure helper. Input: FrozenTrace + corrections. Output: ground-truth ad-window set per §A.4.
   - Unit-tested in isolation (`NarlGroundTruthTests.swift`).

4. **Report rendering**
   - JSON serialization: stable schema with version field.
   - Markdown rendering: one table per (show × config × metric family).

### Acceptance criteria

- Counterfactual evaluation methodology documented (this file) and runnable (`xcodebuild test -scheme Playhead -testPlan PlayheadFastTests` produces the artifacts).
- First report generated from the 2026-04-21 export, with at least 2 Diary of a CEO and 2 Conan episodes included.
- Precision/recall/F1 deltas between `.default` and `.allEnabled` reported per show for `lexicalInjectionEnabled` and `classifierPriorShiftEnabled`.
- `fmSchedulingEnabled` evaluated only for episodes flagged "fully shadow-covered" (requires Bead 2); partial-coverage episodes still contribute to the other two flags' metrics.

### Out of scope (this bead)

- Automated gate-flip recommendations (Bead 3).
- Production shadow capture (Bead 2).
- Committed eval history.

---

## Section C — Bead 2: `playhead-narl-shadow-capture`

### Scope

Dual-run capture of `.allEnabled` FM responses in production, so the harness can evaluate `fmSchedulingEnabled` honestly.

### T4 — two-lane capture strategy

**Lane A: JIT during strict playback.**
- Fires only when the audio session is actively playing (not just foreground).
- For a lookahead region `[playhead, playhead + N seconds]` (N tunable, default TBD during implementation), identifies windows that `.default` didn't schedule FM on but `.allEnabled` would.
- Fires bounded-budget shadow FM calls on those windows.
- Rationale: shadow data accrues immediately for the region that matters most to near-term gate-flip decisions; cost scales with listening time, not episode size.

**Lane B: background thorough pass.**
- Runs when `ProcessInfo.thermalState == .nominal` AND device is charging.
- Walks episodes with incomplete shadow coverage, fills in remaining un-seeded windows under `.allEnabled` scheduling rules.
- Rationale: completes shadow coverage for whole episodes over time, with zero foreground impact.

Both lanes write through the same persistence path — the harness doesn't know or care which lane populated which rows.

### E1 — storage in `analysis.sqlite`

New table (or column on existing FM response table — TBD during implementation):

```
shadow_fm_responses
  assetId         TEXT
  windowStart     REAL
  windowEnd       REAL
  configVariant   TEXT    -- "allEnabledShadow" for Phase 1; reserved for future variants
  fmResponse      BLOB    -- serialized response, same format as prod FM
  capturedAt      REAL
  capturedBy      TEXT    -- "laneA" | "laneB", for debugging
  PRIMARY KEY (assetId, windowStart, windowEnd, configVariant)
```

Source of truth. Survives app restart, can be queried independently, no jsonl rotation risk.

### E3 — export via `shadow-decisions.jsonl`

- New sibling file in the exported `.xcappdata` bundle's Documents dir alongside `decision-log.jsonl`.
- One JSON line per shadow FM response, stable schema.
- Added to the export manifest step in `ExportedAnalysisStore`.

### Kill switch

`ShadowCaptureConfig.dualFMCaptureEnabled` — default on for Dan's build. When false, both lanes no-op.

### Acceptance criteria

- Lane A fires shadow FM during strict playback with measurable bounded budget.
- Lane B fills in shadow coverage when thermal nominal + charging.
- `shadow_fm_responses` rows written durably to sqlite.
- Export round-trip test: export a bundle → unpack → corpus builder reconstructs FrozenTrace whose `.allEnabled` FM evidence is populated from shadow responses.
- Kill switch verified: with the flag off, no shadow FM calls and no rows written.
- No thermal regression on backfill (Lane A's playback-scoped budget + Lane B's thermal gate).

### Out of scope (this bead)

- Using shadow data in production gate decisions (only harness reads it).
- Schema evolution for non-Phase-1 config variants.

---

## Section D — Bead 3: `playhead-narl-approval`

### Scope

Ingests the harness's JSON report. Emits a per-episode recommendation for `counterfactualGateOpen`, based on a policy rule.

### Policy rule (initial proposal, tunable)

```
recommend flip for episode E if:
  allEnabled.recall(E) >= default.recall(E)
  AND allEnabled.precision(E) >= default.precision(E) - ε  (ε TBD, default 0.02)
  AND episode is fully shadow-covered  (so fmScheduling evidence is real)
```

Recommendations are written into the report alongside metrics. No mutation of any production state.

### Open question for this bead's own brainstorming

Does it also *execute* flips (persist per-episode overrides for `counterfactualGateOpen`), or only recommend them? Current lean: **recommend-only** for Phase 1, execution deferred to possible Phase 2. This is flagged for Bead 3's own brainstorming — not locked in by this design.

### Acceptance criteria (initial)

- Given a harness JSON report, produces a per-episode recommendation JSON.
- Policy rule is parameterized (ε is configurable).
- Recommendations distinguish "flip" / "don't flip" / "insufficient data" (the last for partial-coverage episodes).

### Out of scope (this bead)

- Gate-flip execution.
- Trust-threshold tuning for global gate flips.

---

## Section E — Sequencing and dependencies

```
Bead 1 (harness) ─────────────────────► ships first
                                         ▲
                                         │ reads
                                         │
Bead 2 (shadow capture) ─────────────────┘
   │
   │ accrues data over time
   ▼
fmScheduling metrics in harness reports (once coverage adequate)
   │
   ▼
Bead 3 (approval) ingests harness JSON → recommendations
```

Bead 1 and Bead 2 ship in parallel. Bead 1's report covers priorShift + lexicalInjection immediately; fmScheduling metrics appear for episodes once Bead 2's shadow coverage reaches them. Bead 3 depends only on Bead 1's report format — can be built concurrently, activated once Bead 1 has a real report to consume.

## Section F — What this design does NOT commit

- Specific values of N (Lane A lookahead), ε (approval policy margin), or IoU reporting precision. These are tunables to be set during each bead's implementation.
- Exact sqlite schema shape for `shadow_fm_responses` (table vs column). Decided during Bead 2 implementation.
- Lane A's budget shape (windows-per-second, max-concurrent-FM-calls, etc). Decided during Bead 2 implementation.
- Bead 3's recommend-vs-execute question.
