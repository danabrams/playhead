# NarlEval FrozenTrace fixtures — 2026-04-22

Four hand-crafted `frozen-trace-v2` fixtures for the `playhead-narl.1`
counterfactual eval harness. These are SYNTHETIC — they are not derived
from a corpus-export bundle. Their purpose is to exercise the harness,
ground-truth builder, and replay predictor end-to-end in CI without
requiring a dev-only `.xcappdata` bundle.

For real corpus-derived fixtures, run the env-gated
`NarlEvalCorpusBuilder` test (`PLAYHEAD_BUILD_NARL_FIXTURES=1`) against
the 2026-04-21 export.

## Fixtures

| File | Show | Purpose |
|---|---|---|
| `FrozenTrace-DoaC-ep1.json` | DoaC | Clean episode with one ad span at 200-260s + a borderline window at 1200-1260s (fused≈0.24) that flips from negative to positive under `.allEnabled` via `MetadataPriorShift`. No corrections. |
| `FrozenTrace-DoaC-ep2.json` | DoaC | Baseline detects an ad at 400-440s but MISSES a second ad at 900-960s. One `falseNegative` correction (source=reportMissedAd, correctionType=falseNegative, exactTimeSpan scope). Exercises the "add a positive to ground truth" path. |
| `FrozenTrace-Conan-ep1.json` | Conan | Baseline flags a comedy-banter span (1800-1860s) as an ad; user reverted via `listenRevert`. The correction carries NO explicit `correctionType`, so the ground-truth builder must fall back to `CorrectionSource`-based inference (listenRevert → falsePositive). Exercises the HIGH-1 fix. |
| `FrozenTrace-Conan-ep2.json` | Conan | Whole-asset veto (`exactSpan:conan-ep-002:0:9223372036854775807`). Episode is dropped from metric aggregation with exclusionReason="wholeAssetVeto:conan-ep-002". Exercises HIGH-5 excluded-episode accounting. |

## What each fixture exercises

- **Ground-truth builder paths**:
  - Baseline spans → positives (DoaC-ep1, Conan-ep1).
  - falseNegative exactTimeSpan add (DoaC-ep2).
  - falsePositive via source-heuristic (Conan-ep1 listenRevert).
  - Whole-asset veto exclusion (Conan-ep2).
- **Replay predictor branches**:
  - Branch A (`windowScores` non-empty): DoaC-ep1 prior-shift flip, DoaC-ep2
    non-flip, Conan-ep1 metadata-present windows.
  - Branch B (v1 fallback): not exercised by these fixtures — all are v2.
  - Gate-closed short circuit (`.default` config): returns baseline positives.
- **Show-label fallback chain (MEDIUM-2)**: all four carry `showLabel`
  so the harness uses it directly rather than walking to the sidecar or
  substring heuristic.

## What they do NOT exercise

- `fmSchedulingEnabled`: shadow decisions come from `narl.2`, not from
  corpus-export. The harness reports 0 shadow coverage for these fixtures.
- Ordinal `exactSpan` corrections resolved via atoms: covered by
  `NarlGroundTruthTests` instead.
- Boundary-refinement corrections (`startTooEarly`/etc): covered by unit
  tests.

## Update protocol

When the FrozenTrace v2 schema adds fields, these synthetic fixtures
need to be updated. The Codable path uses `decodeIfPresent` for all
v2-added fields, so adding non-required keys is backwards-compatible —
these JSONs will continue to decode, just without the new data.

When a schema change REMOVES or RENAMES a field, regenerate or hand-edit
these four files so the harness still runs on CI clones that don't have
a real corpus export.

## `expected-report.{json,md}`

These are the audit snapshot from running the harness against these four
fixtures at the time of commit. They are NOT consumed by any test — they
exist purely so a reviewer can confirm "yes, the harness produced
sensible numbers on these inputs". The harness writes fresh reports to
`.eval-out/narl/<runId>/` (gitignored) on every test run; `runId` and
`generatedAt` are non-deterministic, so the committed snapshot's top
fields differ from a live run even when the rollup numbers match.

Expected rollup shape for these fixtures (PriorShift and LexInj are the
only counterfactual adds on Phase 1 data):

- `DoaC default`: 2 episodes, 0 excluded, Win F1@0.3=0.80, Second F1=0.77
- `DoaC allEnabled`: 2 episodes, 0 excluded, Win F1@0.3=0.67, PriorShift adds=2
- `Conan default`: 1 episode, 1 excluded, Win F1@0.3=0.67
- `Conan allEnabled`: 1 episode, 1 excluded, Win F1@0.3=0.67, PriorShift adds=0
- `ALL default`: F1=0.75
- `ALL allEnabled`: F1=0.67 (activation adds recall OR false-positives
  depending on where the borderline window sits)
