# NARL lifecycle backfill audit — 2026-04-24 (playhead-pc0t)

**Status:** closed as not-actionable. See reasoning below.

## Bead hypothesis

Many of the 54 `unknown`-bucketed FrozenTrace fixtures were generated before
`NarlEvalCorpusBuilder` started threading gtt9.8 lifecycle fields into the
FrozenTrace schema. Some of those capture bundles on disk DO have
`asset-lifecycle-log.jsonl`. Re-running the corpus builder against them should
populate the missing fields and shrink the unknown bucket.

## Audit result

The hypothesis does not survive contact with what's actually on disk.

### What's in `.captures/` (as of 2026-04-24)

| Bundle                                                 | JSONL lifecycle log | SQLite lifecycle rows   |
|--------------------------------------------------------|---------------------|-------------------------|
| `2026-04-23 13:54.43.748.xcappdata`                    | absent              | 0 assets (empty table)  |
| `2026-04-23 17:39.48.774.xcappdata`                    | absent              | 1 asset (AA8DCCA6)      |
| `2026-04-23 21:09.44.188.xcappdata`                    | 2 assets (2.2 KB)   | 1 asset (AA8DCCA6)      |
| `2026-04-23 21:34.36.596.xcappdata`                    | 2 assets (2.2 KB)   | 3 assets (AA8DCCA6, 34C7E7CF, 71F0C2AE) |

Additionally, the 13:54 and 17:39 sqlite schemas predate the `terminalReason`
column entirely — they were captured on pre-gtt9.8 builds.

### Assets that appear in fixtures but have NO lifecycle telemetry anywhere

16 distinct asset IDs referenced across the `2026-04-22/`, `2026-04-23/`,
`2026-04-23-1354/` and `2026-04-24/` fixture directories have zero lifecycle
data in any capture bundle (JSONL or SQLite):

    1BC8D105, 26B5A7FA, 304D310B, 54B196C8, 5951989F, 6A7DFBF5,
    9007CDD0, 99E86F79, 9BA1818E, A52CFD91, A53E3CE0, C22D6EC6,
    C25A058C, D3285CBB, D787EAA8, DF5C1832

Plus the four synthetic entries in `2026-04-22/` (`Conan-ep1/2`, `DoaC-ep1/2`),
which were hand-crafted before real data existed.

### What is recoverable

- `34C7E7CF` and `71F0C2AE`: already populated in `2026-04-24/` (the
  `46a067c` commit that landed gtt9.15 produced these with the corpus
  builder reading `asset-lifecycle-log.jsonl`). No further work needed.
- `AA8DCCA6`: three duplicate fixtures (in `2026-04-23/`, `2026-04-24/`)
  are currently unknown. The 21:34 SQLite has partial lifecycle data
  (analysisState=backfill, duration=532s, ft=900s, feature=1890s, no
  terminalReason). Threading this would move the bucket from `unknown`
  to `scoring-limited` (ft/duration = 1.69 >= 0.95) or
  `pipeline-coverage-limited` depending on interpretation — but
  (a) the NarlEvalCorpusBuilder deliberately does not depend on
  analysis.sqlite (per the module header comment, to avoid a GRDB
  linkage and schema coupling), and (b) the swing is unknown 54 -> 51,
  not the <10 target the bead set.

### The rest is gone

Everything else can only be recovered by re-capturing those episodes on
the current (gtt9.8+) build — which the bead explicitly declares out of
scope ("Out of scope: Capturing new sessions").

## Comparing to gtt9.15's landing commit

The `46a067c` commit message (the gtt9.15 GREEN landing) already
documents this state precisely:

```
scoring-limited:          1   (71F0C2AE — full coverage)
pipeline-coverage-limited: 1  (34C7E7CF — stalled in backfill)
unknown:                  54  (pre-9.8 fixtures across 04-22, 04-23,
                               04-23-1354 — expected, do NOT retrofit)
```

i.e. the gtt9.15 author had already reached the "do NOT retrofit"
conclusion. The follow-up bead was a speculative second pass that this
audit has now falsified.

## Recommendation

Close `playhead-pc0t` as not-actionable. The only honest way to shrink
the `unknown` bucket is to capture new dogfood sessions under the
current build — that work lives outside this bead and will happen as a
side effect of regular test/dogfood cycles. Future fixtures generated
from post-gtt9.8 bundles will be bucketed correctly out of the box
(see the `2026-04-24/34C7E7CF` and `2026-04-24/71F0C2AE` entries as
working examples).

If the `unknown` bucket's visual noise becomes actionable before new
captures accumulate, the cheapest way to decay the prior fixtures is
to delete (not backfill) the pre-gtt9.8 `2026-04-22/`, `2026-04-23/`,
and `2026-04-23-1354/` directories. That's a separate policy call and
is out of scope here.
