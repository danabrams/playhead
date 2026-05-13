# NARL metadata activation decision - 2026-05-13

## Decision

Activate only `MetadataActivationConfig.default.lexicalInjectionEnabled`.

Keep these gates disabled in production default:

- `classifierPriorShiftEnabled`
- `fmSchedulingEnabled`

The rollout strategy is a global default per-gate flag: the master
`counterfactualGateOpen` remains open, lexical injection is enabled, and the
two unjustified gates remain false. This is the smallest production activation
surface that consumes the current counterfactual evidence without enabling the
full `.allEnabled` bundle.

The replay harness keeps a frozen no-metadata row named `default` via
`MetadataActivationConfig.counterfactualBaseline`. That preserves the historical
approval comparison after production `.default` graduates lexical injection.

Debug override behavior is unchanged. `MetadataActivationOverride` can still
force `.allEnabled` in DEBUG builds, and release-lock resolution still returns
`.default`.

## Evidence Used

Primary report:

- `docs/narl/2026-05-06-v3-baseline.md`
- Source run: `.eval-out/narl/20260504-235504-79A299/report.json`
- Code state: main at `5d3d323f`
- Corpus: 128 FrozenTrace fixtures, 5 included shows, frozen-trace-v3 decoder

ALL-corpus Sec-F1:

| Config | Sec-F1 | Precision | Recall | TP-sec | FP-sec | FN-sec |
|---|---:|---:|---:|---:|---:|---:|
| default | 0.3955 | 0.9217 | 0.2518 | 353 | 30 | 1049 |
| allEnabled | 0.4461 | 0.9258 | 0.2939 | 412 | 33 | 990 |
| delta | +5.1pt | +0.4pt | +4.2pt | +59 | +3 | -59 |

Per-gate isolation from #126 attributed the lift to lexical injection:

| Variant | Decision |
|---|---|
| `lexicalOnly` | Approve for production default. It accounts for the measured ALL-corpus lift. |
| `priorShiftOnly` | Hold off. The v3 baseline records it as metric-neutral at the corpus rollup. |
| `fmSchedulingOnly` | Hold off. It still depends on shadow-backed scheduling evidence before a production flip. |

Current per-gate isolation rows confirm that the production shape being
activated, `lexicalOnly`, carries the measured lift:

| Config | Sec-F1 | Precision | Recall | TP-sec | FP-sec | FN-sec | LexInj adds | PriorShift adds |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| default | 0.3955 | 0.9217 | 0.2518 | 353 | 30 | 1049 | 0 | 0 |
| lexicalOnly | 0.4466 | 0.9300 | 0.2939 | 412 | 31 | 990 | 2 | 0 |
| priorShiftOnly | 0.3955 | 0.9217 | 0.2518 | 353 | 30 | 1049 | 0 | 12 |
| fmSchedulingOnly | 0.3955 | 0.9217 | 0.2518 | 353 | 30 | 1049 | 0 | 0 |
| lexicalOnly delta | +5.1pt | +0.8pt | +4.2pt | +59 | +1 | -59 | +2 | 0 |

That isolation table is from the expanded harness run with the same fixture
corpus shape and row names now pinned in `NarlEvalHarnessTests.replayConfigs`
(`.eval-out/narl/20260513-204820-0A9B19/report.json`). The older primary report
remains the preserved approval artifact for the original `default` vs.
`allEnabled` comparison.

The false-positive regression check for the activated production shape is
precision-based at corpus level: `lexicalOnly` moves precision from `0.9217` to
`0.9300` while recall adds 59 TP-sec and removes 59 FN-sec. FP-sec increases by
1 second in the isolation row. The broader `.allEnabled` bundle also improved
precision in the primary report, but it increased FP-sec by 3 seconds and
contains two unproven gates, so production graduates only the isolated lexical
gate.

## Known Risk

DoaC regressed under both `allEnabled` and the production-bound
`lexicalOnly` shape:

| Config | Sec-F1 | Precision | Recall | FP-sec |
|---|---:|---:|---:|---:|
| default | 0.7480 | 1.0000 | 0.5975 | 0 |
| allEnabled | 0.6838 | 0.7993 | 0.5975 | 60 |
| lexicalOnly | 0.6838 | 0.7993 | 0.5975 | 60 |

The baseline doc attributes that regression largely to one synthetic fixture,
`doac-ep-001` from `PlayheadTests/Fixtures/NarlEval/2026-04-22/`, where the
phrase "mid-roll promo snippet" fires lexical injection while synthetic ground
truth marks only `[200, 260]` as ad. The production activation is still
graduated rather than `.allEnabled`: prior shift and FM scheduling remain off,
metadata-only lexical hits still carry `isMetadataOrigin`, and the downstream
two-hit rule prevents metadata-only lexical evidence from promoting candidates
by itself.

This is a measured activation with a known per-show caveat, not a blanket
approval of every metadata consumption path.

## Runnable Methodology

Run the focused NARL harness and activation suites from the repo root:

```bash
xcodebuild test -project Playhead.xcodeproj -scheme Playhead -destination 'platform=iOS Simulator,name=iPhone 17 Pro,OS=latest' -derivedDataPath .deriveddata -only-testing:PlayheadTests/NarlEvalHarnessTests -only-testing:PlayheadTests/NarlApprovalDecisionStateTests -only-testing:PlayheadTests/NarlApprovalEpsilonBoundaryTests -only-testing:PlayheadTests/NarlApprovalPartialCoverageTests -only-testing:PlayheadTests/NarlApprovalMultiThresholdTests -only-testing:PlayheadTests/NarlApprovalAggregationTests -only-testing:PlayheadTests/NarlApprovalReportWriterTests -only-testing:PlayheadTests/NarlApprovalIntegrationTests -only-testing:PlayheadTests/MetadataActivationConfigTests -only-testing:PlayheadTests/MetadataLexiconInjectorTests -only-testing:PlayheadTests/MetadataLexiconTwoHitRuleTests -only-testing:PlayheadTests/MetadataPriorShiftTests -only-testing:PlayheadTests/MetadataPriorShiftRealDataBandTests -only-testing:PlayheadTests/MetadataSeededRegionTests -only-testing:PlayheadTests/MetadataActivationOverrideTests -only-testing:PlayheadTests/DecisionLoggerTests CODE_SIGNING_ALLOWED=NO
```

The harness writes local artifacts under `.eval-out/narl/<run-id>/`:

- `report.json`
- `report.md`
- `trend.jsonl`

Those generated artifacts remain gitignored. Durable activation decisions are
recorded in `docs/narl/`.

The generated report's `default` row is intentionally the frozen
`counterfactualBaseline`, not current production `.default`.

## Production Default

As of this bead:

```text
counterfactualGateOpen = true
lexicalInjectionEnabled = true
classifierPriorShiftEnabled = false
fmSchedulingEnabled = false
```

Any future activation of prior shift or FM scheduling needs a separate corpus
decision and tests that deliberately update the default pins.
