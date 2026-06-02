# bd-4xqf Measurement Recipe (the "measure" half of "wire in then measure")

> **Status:** ready to run. This recipe gates on Mac Catalyst + Apple Intelligence + a staged corpus. All tooling on main as of 2026-06-02.
>
> **Goal:** quantify whether SpanFinalizer (wired in playhead-p56a, default-OFF) measurably improves the pipeline's coverage of true DAI ad slots on the rediff-corroborated dogfood corpus. Baseline = the 12/12 narrower / 18% coverage observation that motivated bd-4xqf (recorded in [`project_pipeline_boundary_undersizing_2026-06-01.md`](../.claude/projects/-Users-dabrams-playhead/memory/project_pipeline_boundary_undersizing_2026-06-01.md)).

---

## Prerequisites

1. **Mac Catalyst destination** in Xcode (the env-gated dump test requires it).
2. **Apple Intelligence** enabled on the machine running the test.
3. **Corpus staged**:
   - `TestFixtures/Corpus/Snapshots/manifest.json` populated (autonomous loop fires this nightly; check `scripts/l2f-corpus-status.py --terse`).
   - `TestFixtures/Corpus/Audio/*.mp3` for every manifest entry (autonomous loop downloads these).
   - `TestFixtures/Corpus/Transcripts/*.json` for the episodes the dump will process.
4. **Time budget:** ~9 full-FM passes at ~3-5 min each ≈ 30-50 min per dump. Two dumps = 60-100 min total. Plan accordingly.

---

## Step 1 — Baseline dump (`spanFinalizerEnabled = false`)

Already the default. Confirm:

```bash
grep -n "spanFinalizerEnabled" Playhead/Services/AdDetection/AdDetectionService.swift
# Expect line ~582 in the AdDetectionConfig.default literal:
#   spanFinalizerEnabled: false
```

Run the env-gated dump test:

```bash
cd /Users/dabrams/playhead
xcodebuild test \
  -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=macOS,variant=Mac Catalyst' \
  -only-testing:'PlayheadTests/PipelineDumpLiveTests/testProductionPipelineDumpOnNewEpisodes' \
  PLAYHEAD_PIPELINE_DUMP=1
```

The test writes `playhead-dogfood-diagnostics-pipeline-dump-*.json` to the repo root. **Rename it immediately so the next run doesn't clobber it:**

```bash
mv playhead-dogfood-diagnostics-pipeline-dump-*.json \
   playhead-dogfood-diagnostics-pipeline-dump-baseline-$(date +%Y%m%d).json
```

Sanity-check the new dump has the per-candidate field (added in PR #201):

```bash
python3 -c "
import json
d = json.load(open('playhead-dogfood-diagnostics-pipeline-dump-baseline-$(date +%Y%m%d).json'))
e0 = d['episodes'][0]
print('candidateDecodedSpanList in episode 0:', 'candidateDecodedSpanList' in e0)
print('episode count:', len(d['episodes']))
"
```

If `candidateDecodedSpanList` is missing, you're on a pre-#201 dump path — bail.

---

## Step 2 — Treatment dump (`spanFinalizerEnabled = true`)

Edit `Playhead/Services/AdDetection/AdDetectionService.swift` line ~582. Change:

```swift
spanFinalizerEnabled: false
```

to:

```swift
spanFinalizerEnabled: true
```

inside the `AdDetectionConfig.default` literal. (There's only one such literal.)

Re-run the same xcodebuild command from Step 1. Rename the new dump:

```bash
mv playhead-dogfood-diagnostics-pipeline-dump-*.json \
   playhead-dogfood-diagnostics-pipeline-dump-treatment-$(date +%Y%m%d).json
```

Sanity-check the treatment dump has the SpanFinalizer telemetry:

```bash
python3 -c "
import json
d = json.load(open('playhead-dogfood-diagnostics-pipeline-dump-treatment-$(date +%Y%m%d).json'))
constraints_seen = set()
for ep in d['episodes']:
    for w in ep.get('adWindows', []):
        for c in (w.get('spanFinalizerConstraintsFired') or []):
            constraints_seen.add(c)
print('SpanFinalizer constraints fired across treatment dump:', sorted(constraints_seen))
"
```

If the set is empty, the flag didn't actually flip — the dump is using the same baseline arm. Double-check the `AdDetectionConfig.default` edit was saved and the build was fresh.

**Revert the flag** before any further work on main (the production default must stay OFF):

```bash
# Manually edit line ~582 back to:
#   spanFinalizerEnabled: false
git diff Playhead/Services/AdDetection/AdDetectionService.swift
# Should be empty after the revert
```

---

## Step 3 — Compare

```bash
scripts/l2f-bd4xqf-compare.py \
  --baseline playhead-dogfood-diagnostics-pipeline-dump-baseline-$(date +%Y%m%d).json \
  --treatment playhead-dogfood-diagnostics-pipeline-dump-treatment-$(date +%Y%m%d).json \
  --rediff playhead-dogfood-diagnostics-tier-a-rediff.json
```

The script (PR #215) emits a markdown report:

- **Mean pipeline coverage: baseline → treatment (Δ)** — the headline number. Baseline is ~18% per bd-4xqf. If treatment goes UP measurably (>10 percentage points, ideally to >50%), SpanFinalizer is helping.
- **Verdict transition matrix** — how many pairs moved OK / CAND_NARROW / FUSION_DROP between arms. We want pairs to migrate INTO OK.
- **Top 5 pairs by Δ coverage (both directions)** — which episodes/slots got better or worse. Worse pairs are diagnostic — they reveal SpanFinalizer constraints that hurt.
- **Most-fired constraints in treatment** — tells you which of the 5 SpanFinalizer constraints (`mergedWithAdjacent`, `splitAboveMaxDuration`, `droppedBelowMinDuration`, `policyOverrideApplied`, `chapterPenaltyApplied`) are doing the work.

JSON output (for tooling): add `--json`.

---

## Interpreting the result

| Treatment vs baseline | Action |
|---|---|
| Mean coverage **↑ >10pp** with no per-pair regression > -5pp | **Strong evidence.** Move toward default-ON. File a follow-up bead to flip `spanFinalizerEnabled = true` in `.default`, with the comparator report attached as evidence. |
| Mean coverage **↑ marginally (1-10pp)** | **Inconclusive.** Re-run with a larger corpus (the autonomous loop adds episodes nightly). Likely needs N=30+ episodes for confidence. |
| Mean coverage **~unchanged (±1pp)** | **SpanFinalizer's wire-in is currently a no-op for boundary coverage.** Most likely cause: constraint #6 (`policyOverrideApplied`) fires on every span but doesn't change bounds, while #1-#3 (overlap-resolve, merge, duration-sanity) rarely fire on the current corpus. Move on to the follow-up beads `playhead-uorw` (merge data loss) and `playhead-z5ly` (split id collision) for the deeper fixes. |
| Mean coverage **↓** | **Regression.** Look at the per-pair worst Δ table; the constraint trace on those windows reveals which SpanFinalizer rule is collapsing them. Likely #1 (`overlapSuppressed`) trimming a wider valid window to make room for a narrower neighbor. File a bug. Default stays OFF. |

---

## Cross-references

- bd: `playhead-p56a` (SpanFinalizer wire-in; PR #214)
- bd: `playhead-4xqf` (parent investigation; baseline 12/12 narrower @ 18% mean coverage)
- `scripts/l2f-bd4xqf-analyze.py` (single-dump verdict generator; PR #203)
- `scripts/l2f-bd4xqf-compare.py` (this recipe's comparator; PR #215)
- `docs/bd-4xqf-codepath-map.md` (production code-path map)
- bd: `playhead-uorw` (SpanFinalizer merge drops curr context — follow-up if treatment is inconclusive)
- bd: `playhead-z5ly` (SpanFinalizer split id collision — follow-up if treatment shows trace doubling)
