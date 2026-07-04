# Phase B — iOS 27 32k-context measurement recipe (playhead-xx7m.2)

> **Status:** ready to run on a real iOS 27 device. This recipe is the "measure"
> half of the 32k-context work. The instrumentation it greps for landed in
> playhead-xx7m.2 (this branch); the actual prompt-budget retune is a **separate,
> gated** follow-up — do NOT change `coarseBudgetDivisor` / `refinementBudgetDivisor`
> / the windowing math until this measurement confirms the numbers below.
>
> **Goal:** confirm on real hardware that (a) the iOS 27 on-device model reports a
> ~32k context window (vs iOS 26's 4096), (b) that larger window collapses the
> coarse-pass window count per episode, and (c) the wider windows materially raise
> ad-region span recall against the rediff-corroborated DAI corpus — the ~18%
> DAI-width coverage baseline recorded for `playhead-4xqf`
> ([`project_pipeline_boundary_undersizing_2026-06-01.md`](../.claude/projects/-Users-dabrams-playhead/memory/project_pipeline_boundary_undersizing_2026-06-01.md)).

---

## Why a real device is mandatory

FoundationModels is **graceful-unavailable on the simulator** — `SystemLanguageModel.default`
reports unavailable and no coarse FM pass runs. Note the simulator's `contextSize`
breadcrumb reads **0 or 4096** (the API returns 0 while the model is unavailable and its
base 4096 as it warms) — both are simulator artifacts, *not* the 32k device signal, and
the 4096 here is the API's own value, distinct from the classifier's 4096 budget fallback.
The plumbing
(the `contextSize` field flowing capability-state → snapshot → diagnostics payload, and
the breadcrumb formatting) is unit-tested on the simulator, but the *values* only
appear on a device with Apple Intelligence enabled. Use an iPhone that is:

- on **iOS 27.0** or later,
- with **Apple Intelligence enabled** (Settings → Apple Intelligence & Siri),
- **cool and charging** (thermal throttling suppresses the FM pass; see
  `CapabilitySnapshot.shouldThrottleAnalysis`).

---

## Step 1 — Build & install the instrumented build

Use the Xcode 27 beta toolchain and a real-device destination (replace the device
name with yours from `xcrun xctrace list devices`):

```bash
cd /Users/dabrams/playhead
DEVELOPER_DIR=/Users/dabrams/Downloads/Xcode-beta.app/Contents/Developer \
xcodebuild build \
  -scheme Playhead \
  -destination 'platform=iOS,name=<Your iPhone>' \
  -derivedDataPath .derivedData-ios27-device
```

Then run the app from Xcode (Cmd+R) onto the device so Console.app can attach, or
install the built `.app` and launch it manually. Trigger at least one analysis run
by opening an episode that will be analyzed (or let a queued backfill run while
charging).

---

## Step 2 — Grep the two breadcrumbs in Console.app

Open **Console.app**, select the device, and filter on subsystem `com.playhead`.
Two breadcrumbs carry the Phase B signal.

### 2a. Capability-layer contextSize (once per capability snapshot)

Category `Capabilities`. Search token:

```
fm.capability.context_window
```

Expected on iOS 27:

```
fm.capability.context_window contextSize=32768
```

- `contextSize=32768` (or whatever the model reports; the point is **≫ 4096**) →
  the iOS 27 model exposes the large window. **Confirmed.**
- `contextSize=4096` → on a **real device**, you are on the old window; verify the
  device is really on iOS 27 and Apple Intelligence is enabled. On the **simulator**,
  4096 is just the API's base value while the model is unavailable — not a measurement.
- `contextSize=0` → FoundationModels is unavailable (simulator, AI disabled, or
  ineligible device). Not a valid measurement.

The same value also rides the periodic snapshot summary line (category
`Capabilities`, token `Capability snapshot captured:` → `contextSize=…`).

### 2b. Coarse-run budget breadcrumb (once per classification run)

Category `FoundationModelClassifier`. Search token:

```
fm.coarse.run_budget
```

Expected shape:

```
fm.coarse.run_budget contextSize=32768 coarseBudget=<N> coarseWindows=<M>
```

- Fires **exactly once per classification run** (at the top of the coarse pass,
  not once per window). If you see it N times for one episode analysis, that is N
  classification runs, not log spam.
- `coarseWindows=<M>` is the headline: on iOS 26's 4096 window this was ~20–25 per
  episode; on iOS 27's ~32k window it should collapse to ~1–2.
- `coarseBudget=<N>` is the per-window prompt ceiling the run derived from
  `promptBudget()` (already clamped by `coarseBudgetDivisor`). It scales with
  `contextSize`, so a ~8× larger context yields a ~8× larger budget and far fewer
  windows.

Capture a few episodes' worth of these lines (Console.app → Save Selected, or
`log collect`) so you have a per-episode `coarseWindows` distribution.

> Tip: to pull the breadcrumbs off the device without the GUI:
> ```bash
> log collect --device --output phase-b.logarchive --last 30m
> log show phase-b.logarchive --predicate 'subsystem == "com.playhead"' \
>   --info | grep -E 'fm.capability.context_window|fm.coarse.run_budget'
> ```

---

## Step 3 — Measure the ad-region recall delta (rediff / DAI-width coverage)

The window-count collapse is only worth shipping if it **raises recall** against the
true DAI ad slots. Reuse the `playhead-4xqf` measurement harness — it quantifies
pipeline coverage of rediff-corroborated ad slots.

1. Follow [`docs/bd-4xqf-measurement-recipe.md`](bd-4xqf-measurement-recipe.md) to
   produce a pipeline dump on the **iOS 27 device build** (the dump test path is the
   same; the difference is the OS/model under it). This is your **iOS 27 arm**.
2. If you have a retained iOS 26 dump over the same corpus, use it as the **baseline
   arm**; otherwise capture one on an iOS 26 device/build first.
3. Compare with the existing comparator:

   ```bash
   scripts/l2f-bd4xqf-compare.py \
     --baseline playhead-dogfood-diagnostics-pipeline-dump-ios26-$(date +%Y%m%d).json \
     --treatment playhead-dogfood-diagnostics-pipeline-dump-ios27-$(date +%Y%m%d).json \
     --rediff playhead-dogfood-diagnostics-tier-a-rediff.json
   ```

   The headline is **mean pipeline coverage of true DAI width** (baseline ~18%).

---

## Success criteria

All three must hold to declare Phase B's measurement green and unblock the retune:

1. **contextSize ≈ 32k confirmed on iOS 27.** `fm.capability.context_window`
   reports `contextSize` ≫ 4096 (expected 32768) on the device, and `0` only when
   FoundationModels is genuinely unavailable — never the 4096 fallback masquerading
   as a real read.
2. **Coarse windows collapse.** Per-episode `coarseWindows` in
   `fm.coarse.run_budget` drops from the iOS-26 ~20–25 to ~1–2, with `coarseBudget`
   scaling up proportionally to the larger context.
3. **Recall rises materially.** Mean DAI-width coverage from the `playhead-4xqf`
   comparator rises well above today's ~18% baseline (target: a double-digit
   percentage-point gain, ideally past 50%), with no broad per-pair regression.

If (1) fails, the device/OS/AI setup is wrong — nothing downstream is meaningful.
If (1) holds but (2)/(3) don't move, the win is not in raw context size and the
retune should NOT proceed blindly; file findings against `playhead-xx7m` before
touching the divisors.

---

## What is explicitly OUT of scope here

- **Do not** change `coarseBudgetDivisor`, `refinementBudgetDivisor`, the windowing
  math, or `promptBudget()` arithmetic. This branch is instrumentation only; the
  retune is a separate gated bead that consumes this measurement.
- **Do not** un-skip PerfGate measurement/timing tests to force numbers.

---

## Cross-references

- bd: `playhead-xx7m.2` (this branch — Phase B measurement-readiness plumbing)
- bd: `playhead-4xqf` (parent boundary-undersizing investigation; ~18% DAI-width baseline)
- `docs/bd-4xqf-measurement-recipe.md` (the rediff/recall comparator recipe reused in Step 3)
- Instrumentation code:
  - `Playhead/Services/Capabilities/CapabilitiesService.swift` — `fm.capability.context_window` breadcrumb + `contextSize` in the capability state
  - `Playhead/Services/Capabilities/CapabilitySnapshot.swift` — durable `foundationModelsContextSize` field
  - `Playhead/Services/AdDetection/FoundationModelClassifier.swift` — `coarseRunBudgetBreadcrumb(...)` + the once-per-run `fm.coarse.run_budget` emit
  - `Playhead/Support/Diagnostics/DogfoodDiagnosticsAnalysisHealth.swift` — `foundation_models_context_size` in the dogfood archive
