# Playhead — Claude Code Instructions

## Decision Authority

**Never swap frameworks, APIs, or architectural approaches without explicit approval.** Present the options and tradeoffs, then wait for a decision. This applies to:
- Switching between Apple framework APIs (e.g. SpeechAnalyzer vs SFSpeechRecognizer)
- Adding or removing dependencies
- Changing persistence strategies
- Altering the service/actor architecture

When investigation reveals a framework is broken, present findings and proposed alternatives — don't implement the swap.

## Issue Tracking

**Use `bd` (beads, Homebrew `beads` formula) for ALL issue tracking in this repo.** Canonical data lives in `.beads/dolt/`.

**Do NOT use `br` (beads_rust, cargo `beads_rust`) even if you find it installed.** It is a separate reimplementation with its own database and issue prefix (`bd-*` vs bd's `playhead-*`); using it creates a parallel ghost tracker whose IDs never resolve in the real `bd` database. If you find `br` installed, leave it alone and use `bd`. If any command or skill suggests `br`, substitute `bd`.

## Testing

Two test plans exist. **Use the correct one for your context:**

**Per-bead work (implementation, review, fix cycles):**
```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17'
```
Skips XCTest interruption-cycle integration suites. Runs in ~3 minutes on simulator.

**Memory-safe gate — recommended on this 16 GB box (`scripts/fast-gate.sh`, playhead-qt8y):** the raw command above leaves test parallelism **unbounded**, so Xcode clones the simulator up to core-count times (each clone is a full runtime + a ~1 GB Playhead test host), driving free memory to ~tens of MB → the run is OOM-killed mid-suite (`** BUILD INTERRUPTED **`, signal 144) with **no test failure** — pure resource exhaustion, near-certain if a second `xcodebuild` runs alongside. Run the gate through the wrapper instead, which caps the parallel clone count (`-parallel-testing-worker-count 2`) — bounding peak memory (~2 test hosts; measured **~57% free** vs ~0.5% unbounded) while keeping Swift Testing's cheap **in-process** concurrency, so the ~8,300-test bulk stays fast — and auto-recovers a wedged sim (`Mach error -308`):
```bash
scripts/fast-gate.sh    # bounded PlayheadFastTests gate (workers=2); forwards -only-testing:... etc.
```
A capped run that **completes reliably** beats an unbounded one that OOMs and must be retried (every kill = a wasted rebuild+rerun). Tune via `PLAYHEAD_TEST_WORKERS` / `PLAYHEAD_DEST`. The memory driver is the parallel simulator **clones**, not Swift Testing's in-process concurrency. Deferred (a coverage tradeoff for Dan's call): PerfGate-ing the load-sensitive behavioral flake families (gy2s pipeline-stall / RouteChange / Interruption / PlaybackService audio-session / playhead-7h2 runtime-shutdown) out of the default gate — those test real behaviors, so moving them is a coverage decision, not done here.

**Phase-close verification only (final gate before closing an epic):**
```bash
xcodebuild test -scheme Playhead -testPlan PlayheadIntegrationTests \
  -destination 'platform=iOS Simulator,name=iPhone 17'
```
True superset of FastTests — adds the 20 XCTest interruption-cycle suites. Runs in ~1 minute on simulator (FoundationModels gracefully unavailable). Real-device runs are slower because FM tests actually execute.

**Why both plans use `skippedTests` only (no `selectedTests`):** Xcode's xctestplan filter honors XCTest class names but **silently ignores Swift Testing identifiers** in both `selectedTests` and `skippedTests`. Selecting Swift Testing tests via `selectedTests` ends up enabling 0 of them. We work around this by using `skippedTests` (which leaves Swift Testing always enabled) and only filtering XCTest classes. Per-class filtering of Swift Testing requires `-only-testing:'PlayheadTests/StructName/method()'` on the command line, not the test plan.

The `PlayheadFastTests` plan is the default in Xcode (Cmd+U).

**Load-sensitive measurement tests (`scripts/perf-tests.sh`):** Latency/timing tests — `MainActorFreedomTests`, `PlayheadRuntimeLaunchPerfTests`, and the cancel-mid-decode scheduler tests — assert absolute wall-clock budgets that only hold on a quiescent CPU. The parallel FastTests suite (~7,900 tests) saturates the machine and makes them flake, so they are gated (`PerfGate`, opt-in via `PLAYHEAD_RUN_PERF=1`) to **skip** in FastTests/IntegrationTests and run **only** through the dedicated serial pass:
```bash
scripts/perf-tests.sh    # PlayheadPerfTests plan, parallelism off, measurement tests only
```
When adding a new load-sensitive test, gate it with `PerfGate` in the source **and** add it to the `MEASUREMENT_TESTS` list in the script. See playhead-zx0l.

## Parallelism Ceiling

**Maximum 2 concurrent subagents running `xcodebuild` at any time on this machine (16 GB RAM).** Each parallel build can spike 1–3 GB during Swift compilation; combined with Xcode GUI, simulator, sourcekit indexers, and Claude itself, going past 2 has historically OOM'd Xcode (2026-04-17 incident). When orchestrating waves of beads, queue rather than fan out beyond 2. Sequential is always safe.

## Disk Hygiene

Each bead worktree runs `xcodebuild -derivedDataPath .derivedData`, producing ~2 GB of cache. Cleanup must be deliberate — a missed step leaves orphan gigabytes. Real paths are `.worktrees/<slug>/.derivedData` (depth 3, camelCase).

### Canonical bead-close sequence

Run from the repo root after PR merge. Substitute `<slug>` (e.g. `bd-r835`) and `<branch>` (e.g. `bead/playhead-r835`).

```bash
cd /Users/dabrams/playhead && git checkout main && git pull --ff-only
git branch --merged main | grep -qx "  <branch>" || { echo "NOT MERGED — abort"; exit 1; }
WT=/Users/dabrams/playhead/.worktrees/<slug>
git -C "$WT" status --porcelain | grep -q . && { echo "DIRTY — stash or commit first"; exit 1; }
bd close playhead-<slug>
git worktree remove "$WT"
[ -d "$WT/.derivedData" ] && rm -rf "$WT/.derivedData" && echo "removed $WT/.derivedData"
git worktree prune -v
git branch -d <branch>
```

### Safety rails

- Before `rm -rf`: path must start with `/Users/dabrams/playhead/.worktrees/` or `/private/tmp/playhead-`, and must NOT appear in `git worktree list --porcelain`.
- Never pass `--force` to `git worktree remove` without explicit user approval — the refusal is the safety net.
- Echo what was removed so the transcript audits the session.

### Orphan sweep script

`scripts/disk-cleanup.sh` runs weekly via cron. Safe to run manually:

```bash
scripts/disk-cleanup.sh --dry-run   # preview
scripts/disk-cleanup.sh             # actually clean
```

It removes `.worktrees/<slug>/.derivedData` whose worktree is no longer registered, and stale `/private/tmp/playhead-*` dirs older than 3 days that are not active worktrees. Logs to `.logs/disk-cleanup.log`.
