# Phase 0 SLIs — Download→Ready Pipeline

This document restates the cohort-based SLIs in human-readable form. **The Swift types under `Playhead/Services/Observability/` are the source of truth.** When values disagree between this document and the code, the code wins.

Bead: `playhead-d99`. Consumers: `playhead-1nl6` (Phase 1 instrumentation emitter).

## Emission rule

SLIs are emitted from exactly three user-observable moments:

1. **Play-starts** — user hits play on an episode.
2. **Listening-window entries** — the player enters a new listening window (e.g. resuming after a seek or long pause).
3. **Pause transitions** — playback pauses, resumes, stops, or the user scrubs.

SLIs are **NOT** computed from passive cell renders, slice counts, or background tick loops. Passive readers do not reflect user-observed readiness.

See the comment on [`SLI.swift`](../../Playhead/Services/Observability/SLI.swift).

## The five SLIs

| SLI | Unit | Threshold | Scope |
| --- | --- | --- | --- |
| `time_to_downloaded` | seconds | P50 ≤ 15 min, P90 ≤ 60 min | explicit download, 30–90 min episode |
| `time_to_proximal_skip_ready` | seconds | P50 ≤ 45 min, P90 ≤ 4 h | explicit download, `eligibleAndAvailable` mode, 30–90 min episode |
| `ready_by_first_play_rate` | rate | ≥ 0.85 | all play-starts |
| `false_ready_rate` | rate | dogfood ≤ 0.02, ship ≤ 0.01 | `eligibleAndAvailable` mode |
| `unattributed_pause_rate` | rate | harness = 0, field < 0.005 | all pauses |

Threshold constants live in [`SLI.swift`](../../Playhead/Services/Observability/SLI.swift):

- `TimeToDownloadedThresholds.p50Seconds`, `.p90Seconds`
- `TimeToProximalSkipReadyThresholds.p50Seconds`, `.p90Seconds`
- `ReadyByFirstPlayRateThresholds.minRate`
- `FalseReadyRateThresholds.dogfoodMaxRate`, `.shipMaxRate`
- `UnattributedPauseRateThresholds.harnessMaxRate`, `.fieldMaxRate`

`warm_resume_hit_rate` is **not** an SLI. It's a secondary KPI and is intentionally absent from the `SLI` enum.

## Cohort axes

Every SLI is cut by all four of the axes below. The Swift types live in [`SLICohortAxes.swift`](../../Playhead/Services/Observability/SLICohortAxes.swift).

### Trigger

`SLITrigger`: `explicitDownload`, `subscriptionAutoDownload`.

### AnalysisMode

`SLIAnalysisMode`:

- `transportOnly` — transport/metadata ready, no semantic analysis initiated.
- `eligibleButUnavailableNow` — device eligible but resources not available.
- `eligibleAndAvailable` — analysis running or completed.

### ExecutionCondition

`SLIExecutionCondition`, computed by [`ExecutionConditionClassifier`](../../Playhead/Services/Observability/ExecutionConditionClassifier.swift):

- `favorable` = Wi-Fi **AND** (charging **OR** battery ≥ 50%) **AND** thermal ≤ fair.
- `constrained` = cellular **OR** (battery < 20% **AND** not charging) **OR** thermal ≥ serious **OR** Low Power Mode enabled.
- `mixed` = everything else.

Precedence: any constrained predicate wins. Exact-boundary values (50%, 20%, fair vs serious) have explicit boundary tests.

### EpisodeDurationBucket

`SLIEpisodeDurationBucket`, computed by [`EpisodeDurationBucketClassifier`](../../Playhead/Services/Observability/EpisodeDurationBucketClassifier.swift):

- `under30m` — duration < 30 min.
- `between30and60m` — 30 min ≤ duration ≤ 60 min.
- `between60and90m` — 60 min < duration ≤ 90 min.
- `over90m` — duration > 90 min.

Exact boundaries (30m, 60m, 90m) belong to the **lower** bucket. An episode of exactly 30 minutes is the smallest member of the 30–60m bucket. The "30–90 min episode" scope shared by the two latency SLIs is the union of `between30and60m` and `between60and90m`.

## Meaningfulness table

Not every (SLI × cohort) cell is meaningful. [`SLICohortMeaningfulness`](../../Playhead/Services/Observability/SLICohortMeaningfulness.swift) encodes the rules and `SLI.isMeaningful(for:)` delegates to it. Empty cells **must** be emitted as `nil`, never as `0`.

| SLI | Meaningful when… |
| --- | --- |
| `time_to_downloaded` | `trigger == .explicitDownload` AND duration ∈ {30–60, 60–90} |
| `time_to_proximal_skip_ready` | `trigger == .explicitDownload` AND `mode == .eligibleAndAvailable` AND duration ∈ {30–60, 60–90} |
| `ready_by_first_play_rate` | always |
| `false_ready_rate` | `mode == .eligibleAndAvailable` |
| `unattributed_pause_rate` | always |

## Phase 1 contract

The Phase 1 emitter (`playhead-1nl6`) consumes these types as a contract:

1. Assemble an `SLICohort` at the emission moment.
2. For each SLI, call `sli.isMeaningful(for: cohort)`. If `false`, emit `nil` (skip).
3. Otherwise emit the measured value with units matching `sli.unit`.
4. Compare against the thresholds in `SLI.swift` for pass/fail reporting.
