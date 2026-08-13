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

### Lint first (`scripts/lint.sh`) — playhead-ia2s

**Run this before the test gate. It takes ~0.2s warm / ~2.4s cold and needs no build.** SwiftLint was installed on this box all along but the repo had no `.swiftlint.yml`, so for months "lint" was a no-op step in every bead gate and every review round — style, dead-code and complexity regressions landed unreviewed, and human reviewers burned rounds on things a linter catches mechanically.

```bash
scripts/lint.sh                    # whole repo, strict — the gate
scripts/lint.sh --changed          # only Swift files changed vs the merge-base
scripts/lint.sh --lenient          # report violations but exit 0
scripts/lint.sh --fix <paths>      # autocorrect; EXPLICIT PATHS ONLY, refuses repo-wide
```

`scripts/fast-gate.sh` runs it automatically before building, so a violation stops you in 2 seconds instead of after a 3-minute test run. Bypass with `PLAYHEAD_SKIP_LINT=1`.

**Warnings vs errors — the policy.** Every rule in `.swiftlint.yml` is `warning` severity, and `scripts/lint.sh` adds `--strict` to turn warnings into a non-zero exit. The split is deliberate:

- Bare `swiftlint lint` — what Xcode and editor integrations run — exits 0 and paints yellow. **Lint never blocks an unrelated build.**
- `scripts/lint.sh` is the gate and is zero-tolerance. It can afford to be: the baseline is **0 violations, measured**, so any failure is in your own diff.

A rule is promoted `warning` → `error` (so it bites even without `--strict`, i.e. inside Xcode) only when it has held at zero for a full release cycle **and** its violations are always genuine defects, never taste. Style rules are never promoted — a style rule that hard-errors in the IDE is how linters get uninstalled.

**The baseline is green and must stay green.** `.swiftlint.yml` was built by measurement, not taste: every candidate rule was run against the whole tree and admitted only at a violation count of exactly zero. 84 rules are enabled; nothing was reformatted and `swiftlint --fix` was never run repo-wide. Rules that fire today are **not** disabled quietly — they are listed at the bottom of `.swiftlint.yml` with their measured counts, tiered as a follow-up roadmap (Tier D is the high-value shortlist: `variable_shadowing` 144, `unused_parameter` 401, `async_without_await` 396, `force_unwrapping` 389, and `identical_operands` 3 which is probably three real defects).

**Two things not to do.** Do not run `swiftlint --fix` across the repo to adopt a new rule — a ~1,100-file reformat destroys `git blame` and collides with every open bead branch; `scripts/lint.sh --fix` refuses without explicit paths for exactly this reason. And do not add a rule to `only_rules` without measuring it first: one red rule and everyone learns to route around the gate, which is strictly worse than having no linter.

### Test plans

Two test plans exist. **Use the correct one for your context:**

**Per-bead work (implementation, review, fix cycles):**
```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17'
```
Skips XCTest interruption-cycle integration suites. Runs in ~3 minutes on simulator.

**Self-healing gate wrapper (`scripts/fast-gate.sh`, playhead-qt8y/ekpn):** prefer the wrapper over the raw command — it bootstraps a fresh worktree (links the gitignored model from the main checkout + `xcodegen generate` when the scheme is missing the plan), caps concurrent compile jobs to survive the cold-build OOM, and auto-recovers a wedged sim (`Mach error -308`):
```bash
scripts/fast-gate.sh    # PlayheadFastTests, single-host; forwards -only-testing:... etc.
# PLAYHEAD_PLAN=PlayheadIntegrationTests scripts/fast-gate.sh   # phase-close superset
```
⚠️ **Run gates ONE AT A TIME, and do NOT add `-parallel-testing-worker-count ≥2` on this box.** Two memory drivers matter here: (1) a fresh-worktree **cold build** compiles the whole project with parallel `swiftc` — heavy enough on its own to get xcodebuild **OOM-killed** (`Killed: 9`), which is why the wrapper caps `-jobs` (default 4, env `PLAYHEAD_BUILD_JOBS`); (2) running **two gates/builds at once** exhausts the 16 GB box → one is killed mid-suite (`** BUILD INTERRUPTED **`, signal 144) with **no test failure** — pure resource exhaustion. **Clone-based parallel testing is unavailable here** (playhead-ekpn): worker-count ≥2 makes Xcode spawn sim clones, and the clone helper resolves `simctl` via the *global* `xcode-select` (`/Library/Developer/CommandLineTools`, no `simctl`), so it dies with `xcrun: error: unable to find utility "simctl"` (exit 65, ~18s, zero tests run) — `DEVELOPER_DIR` fixes `xcodebuild` but not the clone helper (the 2026-07-16 gotcha; enabling real clone parallelism would need a global `xcode-select -s` change — sudo, system-wide, Dan's call). The gate runs **single-host**: XCTest serial + Swift Testing's cheap **in-process** concurrency (the ~8,300-test bulk stays fast). Serialization + the `-jobs` cap, not in-gate clone parallelism, are the memory guardrails. Deferred (a coverage tradeoff for Dan's call): PerfGate-ing the load-sensitive behavioral flake families (gy2s pipeline-stall / RouteChange / Interruption / PlaybackService audio-session / playhead-7h2 runtime-shutdown) out of the default gate — those test real behaviors, so moving them is a coverage decision, not done here.

**The gate's verdict is about the DIFF, not the count (`scripts/gate_baseline.py`, playhead-voez).** The default gate is RED on a clean checkout — the load-sensitive families above blow their 60s time limits under the full run's own concurrency — so its exit code used to say nothing. The known-broken set is now committed in `scripts/gate-baseline.<plan>.json` and `fast-gate.sh` reports the difference:

```
RED (N known / 0 new)   -> exit 0
RED (N known / 2 NEW)   -> exit 65, both named
a baseline test PASSES  -> exit 65, named
```

Refresh with `scripts/fast-gate.sh --accept-baseline` and **justify the diff in the commit message** — the file is the record of what is known-broken, so a shrinking diff is good news and a growing one needs a reason. Do not reach for `PLAYHEAD_SKIP_BASELINE=1` to make a red gate quiet; that is the bypass this mechanism exists to remove.

Four things worth knowing before you argue with it:
- **The baseline is measured, not labelled.** Each entry carries how many observations it was seen in and failed in. `failed == seen` over ≥3 observations is *deterministic* and its **passing hard-fails** the gate (Dan's call: the baseline is exact, not a ceiling). Anything else is *load-sensitive*: passing is reported as a removal candidate but is not fatal, because one quiet run is not evidence a starvation flake is fixed. **How much this churns, measured 2026-08-01:** two full runs on identical code, quiet box, nothing else running gave 32 and 28 failures with only 19 in common — union 41, **Jaccard 0.46**. Fewer than half the failures recur. That is why a flat exact set is unusable here, and why promotion needs three observations rather than two.
- **The tolerance is not a hole, because identity includes the failure KIND.** A load-sensitive entry means "may TIME OUT", not "may fail". A known-timeout test that fails an *expectation* is reported NEW.
- **A baseline member that did not RUN fails the gate.** Renamed, deleted or newly skipped, a name nobody can reach is not evidence. This is what makes step 2 of the bead (PerfGate-ing a family) show up as a gate failure demanding a refresh, rather than silently shrinking coverage.
- **Selective runs are exempt.** `-only-testing:`/`-skip-testing:` (how `mutation-battery.sh` drives the gate) is a different population, so the check is skipped and the raw xcodebuild exit code passes through untouched.
- **A CRASHED HOST produces NO VERDICT, and the headline now says so (playhead-tl6l).** A test whose host died emits no per-test line at all, so it was matched against the baseline as neither "known" nor reported as NEW — it fell out of the arithmetic in *both* directions. Measured on two full-plan runs, 2026-08-12: **30 tests on main @ 76b0a09a and 14 on bead/mn5e** started and reported nothing, and the baseline file (117 tests, 9 observed runs) had never recorded one of them. The reassuring `RED (N known / 0 new)` could therefore be printed by a run that lost a whole test family. It now reads `RED (85 known / 0 new) — 30 tests got NO VERDICT (crashed host)`, GREEN is unreachable while the count is non-zero, and `--accept-baseline` **carries those entries forward** instead of pruning them as renames (a crash would otherwise have shrunk the file from inside the command that maintains it). **It is deliberately NOT fatal**: it fires on main today, on a pre-existing crash owned by `playhead-rouw`, and a gate that is red for a reason its reader cannot fix is one they learn to route around. A baseline member with no verdict still fails the gate — via ABSENT, unchanged — now labelled with the crash as the cause rather than "renamed, deleted or newly skipped". **But read the line, not just the exit code**: because non-fatal is addressed to a human, a change that CRASHES the test host exits 0 — the tests it kills are healthy ones in nobody's baseline, so `absent` (which covers only already-known-broken tests) never fires. That hole is knowingly accepted and is what `playhead-buvn` arms. Three things not to misread: xcodebuild's **`Failing tests:` summary is a LEAD, not a census** — it spells a test `Suite.function()` while Swift Testing's console prints its `@Test` display name, so grepping one against the other returns zero for a test that reported perfectly well (that is how tl6l came to be filed claiming 19 invisible failures when the true number was 0 and 1); the census only works because **the 30 XCTest PerfGate skips are parsed as a third outcome** — without that it reads 60 and 44 (Swift Testing's own ~11 skips contribute nothing, since a trait-disabled test never emits a `started` line); and **the census was itself first reported as 33/15**, because three of main's 10,910 `passed after` lines and one of mn5e's were truncated mid-token by interleaved app output (`passed after 100.732 secon` + a spliced log line) and the pattern required the literal word `seconds`, so four tests that demonstrably PASSED were counted as crash casualties. Widening every outcome pattern to match on the VERB and treat the duration as optional is what makes the number 30/14.

**Expect the file to grow for a run or two before it settles, and do not read that as regressions.** The recorded set is the UNION of what has been observed, and with a 0.46 Jaccard each new run surfaces flakes the earlier ones missed. Capture–recapture on the first two runs (32 and 28 failures, 19 shared) estimates the true population at **~47**; 41 are recorded after two observations. So the next `--accept-baseline` or two will legitimately add a handful of names, and the gate will report them as NEW until they are recorded. That is the mechanism working — a newly-observed flake is indistinguishable from a regression until it has been seen, which is exactly why it is reported rather than absorbed. Once the union saturates, `RED (N known / 0 new)` is the steady state. The third accepted observation is also what arms the pass-direction check.

Rails: `scripts/mutation-battery-gate-baseline.py` (R series) and `scripts/mutation-battery-disk-preflight.py` (P series, playhead-3nfa, reusing the R engine); the D/E/J/K/L/Q series stay in `mutation-battery.sh`, which is structurally a Swift battery. `scripts/mutation-battery-untypeable.py` (TY series, playhead-x0lb) is Swift too but reports a different verdict — **`UNTYPEABLE`, the mutated source must FAIL TO COMPILE** — because the defect class it covers (two quantities sharing a unit, one read as the other) is invisible to any test: the two are equal on every fixture a test author would write. It is build-only, ~13 s per rail warm (38 rails, measured at R6 review; the earlier "~5 s / 35 rails" figure was neither re-timed nor re-counted after R5 added three), and checks the diagnostic KIND so a syntax break reports `WRONG-ERROR` instead of being credited. **It builds the APP target only, so it cannot see a test file** — that is deliberate (the question is whether production type-checks) and it is why a round that edits tests still owes a `build-for-testing`. Two anti-vacuity rules were added in R2 review and are worth knowing before you drive it: **`--only` cannot drop the control** (a selection that omits `TY99` has it appended, because a partial run printing `control not run` and exiting 0 is exactly the hole this battery replaces), and **`--check-inventory`** is a buildless preflight — it runs on every invocation — tying `UnsoundCursorPromotionSite` to the actual sites, since the Swift test can only read `allCases` and that is a property of the enum rather than of the code. R3 review out-spelled that preflight inside its own file — a sixth promotion written `EpisodeSeconds.init(…)` rather than `EpisodeSeconds(…)` passed it with rc=0 — so both spellings are counted now; the general escape (a promotion helper declared in another file) remains open by construction and is limit L-F. R6 review added a second clause to the same preflight — **region ASSEMBLY is confined to `AnalysisStore.swift` and `CoverageQuantities.swift`** — because `init()`/`append(start:end:)` on the four interval-carrier types are internal, so probe PJ1 assembled a `TranscribedRegion` out of the FAST ranges in `AnalysisJobRunner` and it compiled, reproducing playhead-9y9e's shipped defect one layer below rails TY32/TY34. **R6 wrote that clause as a grep over the four TYPE NAMES and R7 got three spellings past it** — `TranscribedRegion ()` (a space before the parens), `.append(start : …)` (a space before the label's colon), and probe **PK1**, `let region: TranscribedRegion = .init(fastPass: .init(spanningFromZeroTo: CoveredSeconds(watermark)), finalPass: .init())`, which models the fast watermark as a contiguous transcribed region and feeds it to the 0.95 finalize floor; the preflight returned rc=0 and the app BUILT. Dot-`.init` names no type, so no pattern over type names can ever see it. What replaced the grep is a **compile-enforced naming obligation**: `RegionFillDoor` is a token the six assembly entry points take, so a fabrication that omits it is `missing argument for parameter 'door'` (rail TY38) and one that supplies it must write the identifier `openedByCoverageReader`, which the preflight greps for — "one identifier, every spelling" instead of "the spellings the last reviewer thought of". **It is still not a capability**: the token is `internal`, so it stops an author who is not thinking about which population they hold, not a determined one, and that is L-F's worth arrived at honestly rather than by enumeration. Unit tests: `python3 -m unittest scripts.tests.test_gate_baseline` — about a second, no build.

**ONE BATTERY PER WORKTREE, ENFORCED (playhead-pu7e).** `scripts/mutation-battery.sh` now holds a lock in the worktree's own git directory for the whole run; a second invocation exits **75** naming the holder's pid, start time and `--only` argument. Do not go looking for a way around it — the reason it exists is that a concurrent battery is **undetectable from inside either run**. Between run A's restore and its next apply the tree is genuinely clean, so the dirty-tree guard lets B in, and from then on each run's restore reverts the other's mutant while both byte-exactness checks keep passing (each verifies only its OWN restore). It cost four destroyed verdict attempts in `playhead-9y9e` R1, every one of them reported as `the baseline did not run tests (rc=65)`; rails RT11, SC25, SC30 and SC33 still carry the implementer's own verdicts because no reviewer could obtain an independent one.

**Read the exit code, not just the refusal — `75` and `2` mean opposite things.** `75` is EX_TEMPFAIL: somebody holds this worktree, and waiting is the remedy. `2` means the lock could not be *established at all* and **nobody holds it**, so waiting will never help — a full volume, an unwritable git directory, or a run that took the directory but could not record who it was. The review round found the shipped code reporting every one of those as `75` with "Another run took it", which sends you to wait for a process that does not exist; and, worse, found that a holder whose identity write failed carried on mutating while its lock sat ownerless, to be reclaimed as ABANDONED by the next battery 300 s later — inside a normal 4–9 minute run. Both closed: taking the directory and recording the owner are now one step that either happens or does not, and `info` is renamed into place so no reader can ever see half of it.

Two corollaries worth knowing before the next `rc=65`:

- **`rc=65` with no tests is no longer self-diagnosed as a broken tree.** It now names CONTENTION, a WEDGED RUNNER (`blessSimulatorHub` / `service hub IS NOT still alive`, with the `simctl shutdown all` recovery), DISK, OOM, a real BUILD FAILURE, or **UNDIAGNOSED** — and prints the matched line as evidence. `xcrun: error: unable to find utility "simctl"` is labelled a **secondary** symptom: it is xcodebuild's diagnostic collection shelling out through the global `xcode-select`, not the 2026-07-16 clone-parallelism gotcha, and chasing it wastes the round.
- **The battery has never run `git checkout -- .`, and earlier prose here and in its own header said it did.** `restore_sources` is `git checkout -- "${MUTABLE_FILES[@]}"` and cannot touch a file it does not mutate. The blanket checkout was a *remedy people copied out of that wrong prose*, and on 2026-08-04 three agents destroyed their own uncommitted work running it by hand. If you want to know whether a run left residue, ask: `git status --porcelain -- Playhead`.

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
cd /Users/dabrams/playhead && git checkout main
git pull --ff-only || git pull --rebase   # see the divergence trap below
BR=<branch>; WT=/Users/dabrams/playhead/.worktrees/<slug>; PR=<pr-number>
# Merged-ness. PR state is the AUTHORITY on this repo because PRs are SQUASH-merged.
[ "$(gh pr view "$PR" --json state -q .state)" = MERGED ] || { echo "NOT MERGED — abort"; exit 1; }
[ -z "$(git -C "$WT" status --porcelain)" ] || { echo "DIRTY — stash or commit first"; exit 1; }
bd close playhead-<slug>
git worktree remove "$WT"
[ -d "$WT/.derivedData" ] && rm -rf "$WT/.derivedData" && echo "removed $WT/.derivedData"
git worktree prune -v
git push origin --delete "$BR"
git branch -D "$BR"   # -D is CORRECT after a squash merge; see below
```

**The squash-merge trap (hit on 2026-07-28 closing playhead-8m2w).** This repo squash-merges PRs, so
the branch tip is **never** an ancestor of `main` — the squash is a brand-new commit with a different
hash and no parent link to the branch. Two consequences that make the older sequence unusable:

- `git merge-base --is-ancestor "$BR" origin/main` **always reports NOT MERGED** after a squash, for
  branches that are fully merged. Use the PR's state instead; that is the only thing that actually
  knows a squash happened.
- `git branch -d` **always refuses** for the same reason, and no amount of deleting the remote first
  will help — the refusal is not about the remote lagging, it is about the commits genuinely not
  being reachable from `main`. So `-D` is correct **here**, and the PR-state check above is what
  replaces `-d`'s refusal as the safety net. Do not drop that check: without it `-D` is a real
  footgun. (If a future PR is merged with a true merge commit rather than squashed, `-d` will
  succeed on its own — prefer it when it does.)

**The diverged-main trap (same close).** `git pull --ff-only` aborts with "Not possible to
fast-forward" whenever local `main` carries a commit that was never pushed — easy to accumulate,
since main-branch commits (docs, scripts, CLAUDE.md edits) do not go through a bead worktree. Inspect
with `git log --oneline origin/main..main` and `main..origin/main` before doing anything. If the
divergence is your own unpushed commits and the tree is clean, `git pull --rebase` replays them on
top of the merge; then push. Do **not** reach for `--force` or reset — check what diverged first.

Two further traps this sequence exists to avoid (both hit on 2026-07-26):

- **Don't test merged-ness with `git branch --merged main | grep -qx "  <branch>"`.** A branch checked
  out in a linked worktree is listed as `+ bead/foo`, not `  bead/foo` — and at close time every bead
  branch is in a worktree, so that guard reports "NOT MERGED" for branches that are merged, every time.
  `git merge-base --is-ancestor` asks the real question and is indifferent to the listing prefix.
  Compare against `origin/main`, not `main`, so you also prove the merge was actually pushed.
- **`git branch -d` refuses while the remote branch still predates the merge**, with a confusing "not
  fully merged" error even though it is merged to HEAD. Delete the remote branch first and `-d`
  succeeds. This is a *different* cause from the squash-merge refusal above, and the remedies differ:
  here `-d` genuinely succeeds once the remote is gone, so use it; after a squash it never will.
  Either way the PR-state check is what authorises the delete — never let `-D` stand alone.

### Safety rails

- Before `rm -rf`: path must start with `/Users/dabrams/playhead/.worktrees/`, `/private/tmp/playhead-`, or `$TMPDIR/Deleting-` (CoreSimulator's own trash — see below), and must NOT appear in `git worktree list --porcelain`.
- Never pass `--force` to `git worktree remove` without explicit user approval — the refusal is the safety net.
- Echo what was removed so the transcript audits the session.

### The gate REFUSES to start below 13.5 GiB free (playhead-3nfa)

**You no longer have to remember to check.** `scripts/fast-gate.sh` runs `scripts/disk_preflight.py` before lint, before the xcodegen bootstrap, before xcodebuild — and if the volume is short it prints a loud refusal naming the remedy and **exits 28** (POSIX `ENOSPC`, the error this would otherwise have hidden). Nothing is deleted; the refusal only *reports* what `scripts/disk-cleanup.sh` could reclaim.

```bash
scripts/fast-gate.sh                  # refuses under 13.5 GiB, exit 28
scripts/fast-gate.sh --reclaim-disk   # run the cleaner ONCE, re-check, then proceed or refuse
PLAYHEAD_DISK_MIN_GIB=9 scripts/fast-gate.sh    # a different threshold, deliberately
```

This exists because the failure mode is not a failure. A gate that runs out of room **wedges**: xcodebuild stays alive with zero output and never exits, having failed to write its result bundle. No POSIX 28, no non-zero exit, nothing in the log — indistinguishable from a slow test run. This box hit 100 % capacity four times on 2026-08-01 and every one was caught by somebody happening to look.

**Where 13.5 comes from — measured 2026-08-02, two full runs to a terminal verdict**, sampling `df -k` on `/System/Volumes/Data` every 5 s with `scripts/gate-disk-sample.sh`. The quantity is the **drawdown** (free at start minus the minimum during the run), because a gate is a transient event and the end-state delta understates it by more than half:

| run | conditions | start | min | end | drawdown |
|---|---|---|---|---|---|
| 1 | fresh worktree, cache from empty, sim already at 7.88 GiB | 15.07 | **3.58** | 11.39 | **11.49 GiB** |
| 2 | cache warm, sim freshly erased to ~0 | 19.08 | **6.93** | 8.86 | **12.15 GiB** |

`13.5 = 12.15 (worst observed) + 1.35`, and the margin is measured too: the largest fall inside a single 5 s sampling gap was 1.27 GiB, so 1.35 covers what the sampler cannot see. **The old "~8 GB" figure was well under what a full plan actually draws** — do not restore it without re-measuring.

Three things that will bite whoever re-measures:

- **A cold/warm split was tried and the data rejected it.** The obvious refinement is a cheaper threshold when `.derivedData/Build` exists, since a fresh worktree must create ~2.8 GiB of cache. Run 2 was the warm one and drew down **more**. Simulator state moves by ~8 GiB depending on whether the destination was recently erased, and it swamps the cache saving. Two variables, two runs, neither isolated.
- **`df` Avail on APFS lags on the way back up.** Run 2 read 8.86 GiB free the moment it exited and 16.00 GiB two minutes later. The *minimum* is still the number to use — it is the same metric by which this box "hit 100 % capacity" — but never compute what a run keeps from a reading taken at exit.
- **The threshold applies to selective runs too** (`-only-testing:`, i.e. how `mutation-battery.sh` drives the gate). A selective run costs less, but how much less was not measured, so the conservative number governs. If that bites, `--reclaim-disk` or `PLAYHEAD_DISK_MIN_GIB` are the answers.

`PLAYHEAD_SKIP_DISK_PREFLIGHT=1` bypasses the check entirely. It is deliberately **not** printed in the refusal, for the same reason as `PLAYHEAD_SKIP_BASELINE`: an override quoted in the failure message stops being an override and becomes the documented workaround.

A wedged run is also the case the baseline check is built to survive: it printed `CANNOT EVALUATE — the log is incomplete` and passed xcodebuild's own exit through (143), rather than reading ~9,900 unfinished tests as a clean sweep.

### `simctl erase` can report success and free nothing (playhead-cgka)

**Measured 2026-08-02, and it is why this box kept arriving at ~8 GiB of headroom on a 228 GB disk.** `simctl erase` does not delete a device's data in place: CoreSimulator *moves* it to `$TMPDIR/Deleting-<uuid>/` and reaps it asynchronously. If that reap hits a directory it cannot read, it dies and the bytes stay forever — the erase still reports success, and `du` on the device directory still shows the space as freed.

The test suite creates exactly such a directory by design: `DownloadManagerTests` chmods a `complete/` directory to `0o300` to prove a permission failure is handled, and restores it in a `defer` that an abnormal exit skips. Seven orphaned devices had accumulated **15 GiB**, every one of them stuck on a single unreadable `PlayheadTestScratch/…/complete`. Clearing them took the volume from 9.0 GiB to 25.4 GiB free.

```bash
du -sk "$TMPDIR"Deleting-* 2>/dev/null | sort -rn      # what is stranded
chmod -R u+rwx "$TMPDIR"Deleting-*                     # u+w is NOT enough
rm -rf "$TMPDIR"Deleting-*
```

**`chmod -R u+w` does not work here and looks like it should.** `0o300` is `-wx------`: write is already granted; READ is what an enumerator needs to walk the directory, so `rm -rf` fails with EACCES on precisely the directories that need repairing. It is `u+rwx` or nothing. (The suite's own wipe now goes through `TestScratchReaper.forceRemove`, which repairs permissions before giving up, so new instances should not accumulate — but devices stranded before that landed are still out there.)

Check `$TMPDIR/Deleting-*` **before** concluding the box is out of disk. It is the largest single reservoir on this machine and it is invisible to every `du` of the worktree, the derivedData or the simulator.

### Orphan sweep script

`scripts/disk-cleanup.sh` runs weekly via cron. Safe to run manually:

```bash
scripts/disk-cleanup.sh --dry-run   # preview
scripts/disk-cleanup.sh             # actually clean
```

It removes `.worktrees/<slug>/.derivedData` whose worktree is no longer registered, and stale `/private/tmp/playhead-*` dirs older than 3 days that are not active worktrees. Logs to `.logs/disk-cleanup.log`.

**This is the only cleaner in the repo — extend it, don't write a second one.** `scripts/disk_preflight.py` measures and refuses but delegates every removal here, so there is one set of safety rails rather than two that drift. playhead-3nfa added two more classes and one guard:

- **Superseded `.xcresult` bundles** (~93 MB each, measured). The most recent per worktree is always kept — it is what you open after a failure — and only strictly older ones go. `.derivedData/Build` is untouched.
- **`$TMPDIR/Deleting-*`**, the stranded CoreSimulator trash. Every removal `chmod -R u+rwx` first; see the `simctl erase` section above for why `u+w` looks right and does nothing.
- **A live-build guard.** Working directories of running `xcodebuild`/`swift-frontend` are resolved with `lsof -a -p <pid> -d cwd` and anything under them is skipped, so a sweep during a long run cannot destroy it. Resolution uses **`pgrep -x`, never `pgrep -f`** — `-f` matches the full argv and self-matches any shell whose command line merely contains the string, which on 2026-08-01 killed a healthy gate and then raised a phantom second-build alarm.

Verify with `python3 -m unittest scripts.tests.test_disk_preflight` (47 tests, under a second, no build) and `scripts/mutation-battery-disk-preflight.py` (P series, 16 rails + a vacuity control).
