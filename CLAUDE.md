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
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro'
```
Skips XCTest interruption-cycle integration suites. Runs in ~3 minutes on simulator.

**Phase-close verification only (final gate before closing an epic):**
```bash
xcodebuild test -scheme Playhead -testPlan PlayheadIntegrationTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro'
```
True superset of FastTests — adds the 20 XCTest interruption-cycle suites. Runs in ~1 minute on simulator (FoundationModels gracefully unavailable). Real-device runs are slower because FM tests actually execute.

**Why both plans use `skippedTests` only (no `selectedTests`):** Xcode's xctestplan filter honors XCTest class names but **silently ignores Swift Testing identifiers** in both `selectedTests` and `skippedTests`. Selecting Swift Testing tests via `selectedTests` ends up enabling 0 of them. We work around this by using `skippedTests` (which leaves Swift Testing always enabled) and only filtering XCTest classes. Per-class filtering of Swift Testing requires `-only-testing:'PlayheadTests/StructName/method()'` on the command line, not the test plan.

The `PlayheadFastTests` plan is the default in Xcode (Cmd+U).

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
