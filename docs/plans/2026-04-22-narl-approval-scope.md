# narl.3 approval — scope decision

*Per-bead brainstorm outcome for the open design question in `playhead-narl.3`.*

## Question

Does the per-episode gate-flip recommendation engine stop at *recommending* (write `recommendations.json` alongside the harness report; Dan applies by hand), or does it also *execute* (persist per-episode overrides that the runtime consumes to auto-flip `counterfactualGateOpen=true`)?

## Decision

**Recommend-only.** No production code mutated. No new persistence surface. The recommender is a pure function on the harness report, its output is written as a sibling artifact.

## Reasoning

1. **Parent epic defers execution.** The Phase 1 design doc (`2026-04-21-narl-eval-harness-design.md`) explicitly lists "automated flip execution" under *non-goals for Phase 1*. Bead 3's brainstorm question is a re-verification, not a licence to expand scope.

2. **Corpus is too small to automate.** Today's harness covers 3 non-excluded episodes across two shows. An executor would see ~1 `recommendFlip` signal in the wild. Human judgment on N=3 is fine; automation on N=3 is cargo-culting.

3. **narl.2 unlanded.** Shadow-FM coverage is the gate on `fmSchedulingEnabled` recommendations. Until narl.2 ships, the recommender correctly emits `insufficientData` for every fmScheduling evaluation (spec requires this). Shipping an execution path that can only act on lexical/priorShift recommendations from a tiny corpus is premature.

4. **Execution introduces an architectural surface needing approval.** `MetadataActivationConfig` resolution is process-global today. Adding a per-episode override store (UserDefaults, plist, or new table) is the kind of persistence-strategy change CLAUDE.md requires explicit approval for ("Never swap … persistence strategies"). Neither the parent epic nor the bead spec grants that approval.

5. **Harness philosophy.** narl.1 emits metrics; humans consume. The recommender extends the same philosophy: emit *decisions*, human applies. Keeps test/CI semantics identical (metric regressions don't fail the build).

## File location

Pure-helper evaluator lives under `PlayheadTests/Services/ReplaySimulator/NarlEval/` alongside the harness, as `NarlApprovalPolicy.swift`. Rationale: it operates only on the test-target's report types (`NarlEvalReport` etc.) and has no production callers. Placing it in `Playhead/Services/AdDetection/Eval/` would couple production code to test-only types. If Phase 2 wires execution, the applier (which *does* need production visibility) can live there, importing a production-facing mirror.

## What this bead ships

1. `NarlApprovalPolicy.swift` — pure evaluator: `(NarlEvalReport, Policy) -> [NarlRecommendation]`.
2. `NarlApprovalReport.swift` — report writer (JSON + Markdown), sibling to the harness report.
3. `NarlApprovalPolicyTests.swift` — unit tests: three states, ε boundary behavior, partial-coverage → insufficientData, missing fmScheduling data graceful.
4. `NarlApprovalIntegrationTests.swift` — reads the latest harness report, runs the recommender, writes artifacts. Separate Swift Testing suite (option b in bead spec; keeps scopes clean).

## What this bead explicitly does NOT ship

- No mutation of `MetadataActivationConfig`, `UserDefaults`, or any persistence layer.
- No per-episode override store.
- No `RecommendationApplier` protocol / hook / seam (YAGNI; Phase 2 designs this with better info).
- No changes to narl.1's schema (narl.2 extends it when shadow coverage lands).
