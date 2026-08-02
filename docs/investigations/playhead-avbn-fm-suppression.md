# playhead-avbn — FM suppression: a dropped gate, and manufactured evidence of absence

Investigation notes. Written before any code change; the measurement section
records what was OBSERVED, and says so where a number is unavailable.

## Half 1 — `cappedByFMSuppression` names a surface it does not have

`SkipEligibilityGate.cappedByFMSuppression` (EvidenceLedgerEntry.swift) documents
itself "capped to mark-only" and carries `severity == 1`, identical to `.markOnly`.
`AdDetectionService.runBackfill` produces it and `buildFusionAdWindow` persists it.
`SkipOrchestrator.receiveAdWindows` routes only `.markOnly` to the suggest tier and
DROPS every other decoded non-`.eligible` case at playhead-bq70's guard, stamping
`ingest_dropped_blocked_gate` with `detail = "cappedByFMSuppression"`.

## Half 2 — three writers of `disposition = .noAds` that are not presence verdicts

`AdDetectionService.applyFMSuppression` builds one `FMSuppressionWindow` per
`SemanticScanResult` that overlaps the span **in time and nothing else** — no
filter on pass, status, or sentinel-ness:

```swift
let band: CertaintyBand = result.transcriptQuality == .good ? .moderate : .weak
return FMSuppressionWindow(disposition: result.disposition, band: band)
```

`FMSuppressionGuard` then fires on **2 or more** `.noAds` windows at `.moderate`+,
and `FMSuppressionApplicator` downweights every non-strong ledger entry by 0.3x and
sets `cappedToMarkOnly` when no strong proposal survives.

Enumerating every writer of `disposition = .noAds` on a scan row:

| writer | pass | meaning | is it a presence verdict? |
| --- | --- | --- | --- |
| `makeScanResult` (coarse) | passA | FM's own `noAds` for the window | **yes** |
| `makeNoWorkSentinelScanResult` | passA | "no FM work was performed"; spans the WHOLE attempted range; `errorContext` = `noWork:` | **no** |
| `makeRefinementScanResult` | passB | `spans.isEmpty ? .noAds : .containsAd` — the refiner failed to LOCALIZE | **no** |

`makeFailureScanResult` uses `.abstain`, so genuine FM failures are already correctly
non-voting. The two rows above are not.

Both non-verdict writers reach the guard at `.moderate` band:

* the passB row hardcodes `transcriptQuality: .good` (BackfillJobRunner.swift), which
  derives `.moderate` unconditionally;
* the no-work sentinel carries the range's real quality, so it lands `.moderate`
  whenever the transcript is good — and playhead-pz32 already established that this
  row must never be counted as examined audio (`isNoWorkSentinel` /
  `didExamineWindow` exist for exactly that reason). `applyFMSuppression` is a
  coverage-shaped consumer that never adopted the predicate.

So two empty pass-B refinements over a window the COARSE pass positively flagged
`containsAd` are sufficient, on their own, to manufacture a `noAds` consensus that
downweights the surviving lexical/acoustic evidence 0.3x and caps the span.

### The defect became live with playhead-qbib

Before qbib, an empty pass-B row persisted `windowStartTime = windowEndTime = 0.0`
(the row "could not say where it looked"). `applyFMSuppression` requires
`overlapEnd > overlapStart`, so those rows could never overlap anything and the
manufactured vote was inert. qbib correctly gave the row its window's own line-ref
bounds — which armed the vote. The device rows below are pre-qbib and show the
0.0/0.0 form.

## Measurement — what was and was not observable

**No fresh device pull exists on this box.** The 52 MB pull referenced as
`scratchpad/db2/analysis.sqlite` was in an ephemeral session scratchpad and is gone.
The newest surviving pulls are seven DBs under
`/Users/dabrams/coreai-spike/dogfood-pull/`, all dated **2026-07-20/21** — roughly
twelve days stale, and predating playhead-isp5 (the census) entirely.

Consequences, stated exactly:

* **`ingest_dropped_blocked_gate:cappedByFMSuppression` is NOT measurable, for two
  independent reasons.** The census row is written by
  `SurfaceStatusInvariants.adWindowIngestCensus` through
  `SurfaceStatusInvariantLogger` into a `surface-status-<ts>-<id>.jsonl` session
  file — and (a) every surviving pull predates playhead-isp5, so no build that
  wrote the row had shipped, and (b) **no pull contains the diagnostics directory
  at all**: `find … -name 'surface-status*'` over the whole pull tree returns
  nothing, and the only JSON-Lines files captured are `decision-log`,
  `bg-task-log`, `asset-lifecycle-log` and `transcript-shadow-gate`. The device
  numbers the bead asks for cannot be produced without a new pull that includes
  the diagnostics bundle.
* **Half 1 proxy, observed:** `select eligibilityGate, count(*) from ad_windows` over
  all seven pulls yields only `NULL` / `autoSkip` / `eligible` / `markOnly`.
  **Zero `cappedByFMSuppression` rows.** This is a weak zero: those DBs contain
  8–11 coarse FM windows in total, so the fusion path that writes the gate barely
  ran.
* **Half 2, observed:** every pull contains pass-B rows, and **100 % of them carry
  `transcriptQuality = 'good'`** (14 rows across the seven DBs) while pass-A on the
  same assets recorded `degraded` for 8 of its 11 rows. The hardcode is directly
  visible as a distribution artifact — pass-B's quality column carries no
  information at all.
* On asset `62B7395B-5030-4A06-8BC6-45F03226282C` the coarse pass returned
  `containsAd` three times and pass-B persisted a `noAds` row (`0.0 → 0.0`, i.e.
  pre-qbib and therefore inert at the time). That is the manufactured-absence shape
  in production data, one qbib away from voting.

## Decision

**Half 2 is the fix that recovers reach**, and it is unambiguous: a failure to
localize and a decision not to look are both being recorded as votes that there is
nothing to find. Fix at both ends —

1. `applyFMSuppression` counts only rows that actually screened their window and
   actually answered the presence question: coarse pass, `didExamineWindow` true.
2. `makeRefinementScanResult` stops hardcoding `.good` and persists the window's
   measured transcript quality, so the row is honest independent of any consumer.

**Half 1: honor the behavior, not the name.** The capped population is, by
construction, the weakest span the pipeline can produce — `FMSuppressionGuard`
already declines to fire when a URL/promo-code/sponsor lexical anchor, a catalog
entry or a fingerprint match is present, and `cappedToMarkOnly` additionally
requires that no FM `containsAd`, rediff-confirmed or auto-ad lexical entry
survived. A banner over that span is a banner the model actively voted down with
nothing corroborating it. Given the standing product line ("respect your
attention"), surfacing it is the wrong trade — so the gate is renamed
`blockedByFMConsensus` and moved to `severity == 2` alongside the other blocked
cases, and the drop becomes the documented behavior instead of a contradiction.

The reach that Half 1 appeared to promise is recovered by Half 2 instead, and more
precisely: with the manufactured votes gone, the spans that never deserved to be
capped keep their honest `.eligible` / `.markOnly` gate and reach the user through
their own tier, rather than arriving as a banner for a span FM vetoed.

Naming note: the bead suggested `suppressedByFMConsensus`. `blockedByFMConsensus` is
used instead because every other severity-≥2 case in the enum is `blockedBy*`, and
the entire point of the rename is that the name must predict the behavior.

### Consequences of moving the severity 1 → 2, checked rather than assumed

Every reader of `SkipEligibilityGate.severity` compares against
`markOnly.severity` or `blockedByPolicy.severity` with a strict `<` or `>`, so the
move is inert everywhere except the two places it is supposed to bite:

* `SpanFinalizer.capEligibility` / the merge fold — an FM-consensus block now
  DEMOTES a `.markOnly` span instead of tying with it. That is the intended
  meaning: a block outranks a banner-only mark.
* `runBackfill`'s creator-chapter demotion (`severity < blockedByPolicy.severity`)
  no longer re-labels an FM-consensus span as `.blockedByPolicy`. Both gates block,
  so the terminal outcome is identical; only the recorded cause changes, and the
  FM-consensus cause is the more informative of the two.

The self-promo demotion (`severity < markOnly.severity`) and
`BackfillEvidenceFusion`'s `markOnly.severity > gate.severity` admit only
`.eligible` and are untouched.

The persisted column is free text — no `CHECK` constraint anywhere in
`Playhead/Persistence` mentions `eligibilityGate` — so the raw-value rename needs
no migration beyond the decode alias.
