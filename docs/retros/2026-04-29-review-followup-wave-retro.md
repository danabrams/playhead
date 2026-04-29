# Review-followup wave — retrospective (2026-04-29)

## What this wave was

A `/skeptical-iterative-code-review since the last git tag` pass over the
~376-file, ~83 k-insertion change since the prior tag. Cycle budget: 3 per
area. The review surface was split across 4 isolated worktrees by area, each
running its own cycle-1 review → cycle-1 fixes → cycle-2 review → cycle-2 fixes
→ cycle-3 review → cycle-3 fixes loop in parallel.

## What shipped

| PR  | Branch                          | Area                                                       | Fix commits |
|-----|---------------------------------|------------------------------------------------------------|-------------|
| #60 | `bead/review-followup-rfu-aac`  | Acoustic + audio + transcript-shadow                       | 30          |
| #61 | `bead/review-followup-rfu-csp`  | Coordinator + scheduler + persistence                      | 18          |
| #62 | `bead/review-followup-rfu-sad`  | Skip-orchestrator + corrections + UI copy                  | 9           |
| #63 | `bead/review-followup-rfu-mn`   | NARL-eval + feed-refresh + feed-parser + scripts           | 10          |
| #64 | `chore/fusion-budget-clamp-…`   | Suite `.serialized` for static-observer race               | 1           |
| #65 | `fix/bps-telemetry-flake`       | AsyncStream-subscription test recorder                     | 1           |

5 squash-merges into `main`. Final `PlayheadFastTests` on main: 4519 tests in
650 suites, 0 failures, 51 s.

## Highest-leverage findings (in order they landed)

- **rfu-aac H1 — `TranscriptShadowGateLogger.swift`**: per-record
  `synchronize()` was burning fsync budget for no durability win; replaced
  with in-memory `totalBytesWritten` tracking. The gate's only real
  durability boundary is rotation, so rotation-time flush is sufficient.
- **rfu-csp H1 — `AnalysisWorkScheduler.dispatchAcousticPromotion`**: was
  persisting the escalation row before admission could fail, so a denied
  admission left an orphan promotion record. Reordered to
  admit-then-persist, mirroring the cycle-1 fix to `dispatchForegroundCatchup`.
- **rfu-sad H1 — `SkipOrchestrator` tap-then-flip race**: a user tap that
  arrived between the `AdWindow` ingest and the `AdDecisionResult` ingest
  could produce two managed windows, two banners, and two
  `auto_skip_fired` audit rows. Closed with a bounded-LRU
  `recentlyAcceptedSuggestIds` consulted on both ingest paths.
- **rfu-mn M3 — `PlayheadRuntime` BG-task wiring order**: `registerTaskHandler()`
  ran before `attachSharedTelemetry()`, so an iOS dispatch arriving in the
  millisecond between them would silently drop its `submit` row. Swapped.

## Patterns this wave taught us

### Fire-and-forget telemetry + polling tests = flake under load

Six production sites wrap recorder calls in
`Task { ... await bgTelemetry.record(...) }` (intentional — the caller must
not block on telemetry I/O). The test recorder polled with a wall-clock
deadline. Under cross-suite parallel test load the production task and the
poll task competed for cooperative slots and either could be starved past
the deadline.

The fix that landed in PR #65 — replace polling with an `AsyncStream`
subscription that yields each event the moment `append` returns; tests race
the stream against a sleep deadline via `TaskGroup` — is the reusable
template for any future test that observes a fire-and-forget side effect.

(A sweep across all `actor Recording…` / `actor …Capture` test doubles found
no other instances of the polling-vs-fire-and-forget shape; the rest already
read state mutated synchronously by production. So the pattern is contained
for now.)

### `nonisolated(unsafe) static var` test sinks need `.serialized`

`FusionBudgetClampTests` exposed a `nonisolated(unsafe) static var
testClampObserver` and Swift Testing's default parallel execution let two
tests assign and then read it concurrently. The `nonisolated(unsafe)`
doccomment already promised "the underlying tests serialize the assignment
themselves" — `.serialized` on the suite makes that promise truthful (PR #64).

Any future suite that uses a static observer hook should land with
`.serialized` from day one.

## Process notes

- **Fresh reviewer per cycle was decisive.** Reusing the same agent across
  cycles biased findings; a fresh reviewer caught what each fix introduced.
- **Area-scoped worktrees parallelized cleanly.** Four worktrees, two
  concurrent `xcodebuild` runs at any time (per the 16 GB ceiling), no OOMs.
  Squash-merging in size-ascending order surfaced no conflicts because the
  area split was clean.
- **Squash-merge breaks `git branch --merged`.** The CLAUDE.md canonical
  bead-close sequence aborts on `git branch -d` after squash-merge because
  the branch tip never lands on `main`. Verifying via `gh pr view --json
  state` and using `git branch -D` is the squash-merge-safe substitute.
  Worth folding into CLAUDE.md if this becomes a recurring pattern.

## Residuals deferred to dedicated beads

- `SpeakerShift.clusterShiftCertainty=0.7` — corpus-validation gap (rfu-aac M5)
- `BackgroundProcessingService.injectionWait` — 15 s timeout still arbitrary (rfu-csp L3)
- `AnalysisCoordinator` duration-backfill OFFSET cursor — brittleness under concurrent INSERT/DELETE (rfu-csp M2/M3)
