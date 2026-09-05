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

## The record behind these rules

Nearly every rule below was measured before it was written. The measurements — tables, review-round histories, the readings that were wrong and how — live in **`docs/harness-record.md`** (moved out of this file verbatim on 2026-09-02). Each rule cites the bead that owns it; grep the record for that id before arguing with the rule. New measurement narrative goes THERE or in `docs/investigations/`; this file is loaded into every session and holds only what an agent must know to act.

## Reading a measurement — the standing defect class

The defect this repo finds most often, in product code and in its own harness, is **a value that names one thing read as though it named another**. Before quoting a number:

- Name the numerator AND the denominator, and say which population each comes from. (`kern.maxfilesperproc` 61,440 vs `RLIMIT_NOFILE` soft 2,560: same numerator, opposite conclusions — playhead-s34ux.)
- Ask what the quantity would read if the thing never happened. A guard that names an ABSENCE (`[ -n "$SIM_ID" ]`, a bare substring test for `-only-testing`) whose false branch makes no claim ships inert and silent.
- A green rail is evidence only once you have made it fire. Prove a rail by re-introducing the defect.
- Enumerate the EVENTS a command can perform and ask which of them prints nothing — a mutant can only interrogate a line that already exists (playhead-o89d R5).
- Go and count when you can count; re-derive rather than carry a figure forward, and state which run measured it.
- Reason from the errno this kernel RETURNS, not the one POSIX documents: `open(2)` at the fd ceiling sets EBADF 9 here, never EMFILE 24.

## Testing

### Lint first (`scripts/lint.sh`) — playhead-ia2s

Run it before the test gate: ~0.2 s warm, no build. `scripts/fast-gate.sh` runs it automatically (bypass: `PLAYHEAD_SKIP_LINT=1`).

```bash
scripts/lint.sh                    # whole repo, strict — the gate
scripts/lint.sh --changed          # only Swift files changed vs the merge-base
scripts/lint.sh --lenient          # report violations but exit 0
scripts/lint.sh --fix <paths>      # autocorrect; EXPLICIT PATHS ONLY, refuses repo-wide
```

- Every rule in `.swiftlint.yml` is `warning` severity and the gate adds `--strict`, so bare `swiftlint` (Xcode) never blocks a build. A rule is promoted to `error` only after a full release cycle at zero AND when its violations are always defects; style rules are never promoted.
- **The baseline is 0 violations, measured, and must stay green.** Every rule was admitted at exactly zero. Do not run `swiftlint --fix` repo-wide (destroys blame, collides with every open branch). Do not add a rule to `only_rules` without measuring it first — one red rule and everyone routes around the gate. Rules that fire today are listed at the bottom of `.swiftlint.yml` with counts, as a roadmap.
- **The SHAPE-2 preflight (`scripts/singleton_slot_preflight.py`, playhead-mfeq) runs with it.** It bans an optional stored `var` named `current*`/`pending*`/`last*` on an **`actor`** — a singleton standing for a set, four shipped defects (playhead-mk6z, lmrx F4/F9/R3). Actors are reentrant: every `await` lets another operation find the same slot. `scripts/singleton-slot-allowlist.json` is closed in BOTH directions (a new match fails; an entry matching nothing fails) and each `why` must say what bounds the population to one. A green preflight is evidence about the NAMES it can see, not the shape — `playhead-jjke` is invisible to it by construction. Verify: `python3 -m unittest scripts.tests.test_singleton_slot_preflight`; re-introducing a deleted slot must make `scripts/lint.sh` exit 2.

### Test plans

**Per-bead work (implementation, review, fix cycles): `PlayheadFastTests`.** Prefer the wrapper:

```bash
scripts/fast-gate.sh                                         # PlayheadFastTests; forwards -only-testing:… etc.
PLAYHEAD_PLAN=PlayheadIntegrationTests scripts/fast-gate.sh  # phase-close superset (final gate before closing an epic)
scripts/perf-tests.sh                                        # PlayheadPerfTests: the PerfGate'd measurement tests, serial
```

The raw command is `xcodebuild test -scheme Playhead -testPlan PlayheadFastTests -destination 'platform=iOS Simulator,name=iPhone 17'` (needs `DEVELOPER_DIR` pointing at an Xcode.app). The wrapper adds the fresh-worktree bootstrap (model symlink + `xcodegen generate`), a `-jobs` cap (default 4, `PLAYHEAD_BUILD_JOBS`) against the cold-build OOM, wedged-sim recovery (`Mach error -308`), the disk preflight, the simulator trim, memory sampling and the baseline diff.

- **The full fast plan takes ~46 minutes, not ~3.** Measured 2026-08-25: 11,789 tests in 2,809 s (playhead-vk68m). `PlayheadFastTests` is SERIALIZED (`"parallelizable": false`) for file descriptors, not memory — see below. `PlayheadIntegrationTests` is still parallel, is a strict superset (it adds the twelve XCTest classes the fast plan skips: ten in `InterruptionHarnessTests.swift`, `MetricsCorpusIntegrationTests`, `FoundationModelShadowBenchmarkTests`), and its duration is unmeasured under either regime — quote no number for it.
- **Run gates ONE AT A TIME. Do NOT add `-parallel-testing-worker-count ≥2`.** Two builds at once exhaust the 16 GB box (`** BUILD INTERRUPTED **`, signal 144, no test failure). Clone-based parallel testing cannot work here: the clone helper resolves `simctl` through the global `xcode-select` (CommandLineTools, no `simctl`), and fixing that is a sudo, system-wide change — Dan's call.
- **Test plans filter XCTest only.** `selectedTests`/`skippedTests` silently ignore Swift Testing identifiers, so both plans use `skippedTests` naming XCTest classes only; `TestPlanSkipListCanaryTests` fails the build if a plan names anything that does not reach `XCTestCase`, and pins the superset relation. To take a Swift Testing suite out of a plan, gate it in SOURCE with `PerfGate` (`PlayheadTests/Helpers/PerfGate.swift`) — never `-skip-testing:` in `fast-gate.sh`, which sets `SELECTIVE=1` and switches the baseline diff off on every run (playhead-wwbr).
- `PlayheadFastTests` is the Xcode default (Cmd+U).

### How to read a gate's verdict (`scripts/gate_baseline.py`, playhead-voez)

**The verdict is the DIFF against `scripts/gate-baseline.<plan>.json`, not the failure count:**

```
RED (N known / 0 new)                        -> exit 0
RED (N known / 2 NEW)                        -> exit 65, both named
a DETERMINISTIC baseline entry PASSES        -> exit 65, named (the baseline is exact, not a ceiling — Dan)
a baseline member did not RUN                -> exit 65 (renamed, deleted, skipped: a name nobody reaches is not evidence)
… — K tests got NO VERDICT (crashed host)    -> GREEN unreachable while K > 0; re-run the plan
… — K tests hit a RESOURCE FAILURE (re-run)  -> exit 65; names the box, not the code
BASELINE IS FICTION                          -> zero failures AND zero denials against a non-empty baseline
```

- Refresh with `scripts/fast-gate.sh --accept-baseline` and **justify the diff in the commit message**. Never `PLAYHEAD_SKIP_BASELINE=1` to quiet a red gate. Never accept from a scoped run.
- Entries carry `seen`/`failed` observation counts. `failed == seen` over ≥3 observations is *deterministic*; anything else is *load-sensitive*, and its passing is a removal candidate rather than fatal, because one quiet run does not prove a starvation flake fixed (two full runs on identical code share only about half their failures). Identity includes the failure KIND: a known-timeout test that fails an expectation is NEW, and an accept that unions a new kind into an entry prints `TOLERANCE WIDENED:`.
- Selective runs (`-only-testing:` / `-skip-testing:`) skip the check; xcodebuild's exit code passes through.
- **Verdicts come from the `.xcresult` bundle, not the console (playhead-t53a).** `fast-gate.sh` passes `-resultBundlePath`; the console contributes only the STARTED roster, unioned so neither source can silence the other. The console splices app output into verdict lines — mid-word, mid-glyph, `\224` as four ASCII characters — and the parser rejoins up to 8 lines (playhead-phn3). Read the record before touching either parser.
- **A dead host is not a failing test.** A crash-message failure (`Test crashed with signal trap.`) routes to the crashed-host census, whose remedy is re-run; the residual is re-run scoped once (`gate_baseline.py residual`). The census is armed only at 3+ recorded observations; a recorded deterministic casualty that reports again exits 65.
- **A denied resource is not a failing test either (playhead-s34ux).** Only errnos EBADF 9 / ENFILE 23 / EMFILE 24 / ENOSPC 28 route to RESOURCE, plus one prose match, `unable to open database file` — a platform limit, since the iOS 27 simulator's SQLite reports `system_errno = 0` (`SQLiteSystemErrnoPlatformProbeTests` re-measures it; read MEASUREMENTS.md M3c before re-attempting the errno capture). ENOENT and EACCES are what a product bug looks like and stay FAILURE; one genuine assertion beside a denial keeps the test a FAILURE. Root cause: the test host's `RLIMIT_NOFILE` soft limit is 2,560, and the parallel plan held ~695 concurrent WAL stores at 3 fds each against a table that admits ~702 — 99.2 % of the ceiling on the run that was denied, 95.3 % on the run that measured serialization. Serializing took the peak to 17.9 % and resource casualties from 27 to 0 — that is why the fast plan is serialized (playhead-vk68m). The FLOOR is ~449 vnodes of never-released `PlayheadRuntime` graphs in both regimes and grows ~5 per runtime-constructing test (playhead-882eg).
- **The baseline was EMPTIED on 2026-09-05 (PR #511, Dan's decision):** its 118 entries were the parallel regime's flakes, passed serialized, and `--accept-baseline` could never prune them, so every clean full plan exited 65 on `BASELINE IS FICTION`. `tests` is `{}` and `runs_observed` restarts at 3 (the serialized full plans measured that day). A clean run now exits 0; a NEW failure is named and exits 65; a load-sensitive flake under the serialized regime reads NEW once and is accepted with counts recorded under the regime that produced it. The parallel-regime ledger is in git history and `artifacts/MEASUREMENTS.md` M0/M7.
- **Read BOTH result formats.** Swift Testing prints `✘ Test "…" failed after N s`; XCTest prints `Test Case '-[…]' failed`, fast, because they are assertions. `Test run with N tests … passed` is the Swift Testing summary, not the verdict — `** TEST FAILED **` can sit below it. `passed after N seconds` is enqueue-to-completion, not test cost: ~90 % of PASSING tests report over 60 s in a full plan, so a duration heuristic selects tests that failed EARLY, not tests that failed for behavioural reasons.
- Rails, no build: `python3 -m unittest scripts.tests.test_gate_baseline` (seconds); `scripts/mutation-battery-gate-baseline.py` (R series) and `scripts/mutation-battery-disk-preflight.py` (P series).

### Memory, the simulator, and why a run dies (playhead-3rql, blsh, 81ig)

- `fast-gate.sh` samples memory for the whole run (`scripts/gate-memory-sample.py`) and classifies the outcome (`scripts/gate_memory_verdict.py`): `COMPLETE` / `RESTARTED` (a second host pid, or the restart marker — the pid is checked independently because xcodebuild's marker is buffered and can be absent) / `NO-VERDICT` (`Killed: 9`, exit 137/143, or no outcome line). Only the LAST `xcodebuild` invocation in a log is judged; a column an older sampler did not write reads `not recorded`, never 0. Rails: `python3 -m unittest scripts.tests.test_gate_memory_verdict`.
- **A booted iOS 27 simulator costs 10–13 GiB of a 16 GiB box; the test run adds ~2 GiB.** Demand is `active + wired + compressor + swap`; `Pages free` is NOT free memory. Untrimmed, the full plan peaked 7 GiB over the box on 10 GiB of swap and about one run in three was killed regardless of which tests ran — proven by swapping 38 tests for 38 others and reproducing the death exactly. Do not read a green gate as a fix or a red one as a break on an untrimmed box: run it again.
- **`fast-gate.sh` boots a TRIMMED simulator** (`scripts/sim-trim.sh`, labels in `scripts/sim-trim-jobs.txt`, applied via `launchctl disable` + `bootout` inside the device; `simctl boot --disabledJob` is inert on this runtime). It removes ~10 GiB of peak demand and brings the run 2.8 GiB UNDER the box with swap untouched. **Read the log for `fast-gate: simulator processes after trim: <n>` before believing a run was trimmed** — it shipped inert for two full plans because the UDID was resolved only from an `id=` destination (playhead-81ig); every skip path now prints UNTRIMMED. A label is trimmed only if its framework is imported nowhere in `Playhead/` or `PlayheadTests/`, and a job file naming a kept daemon fails the run. Siri/Apple Intelligence/speech (`scripts/sim-trim-jobs-tier-b.txt`, `--include-tier-b`) is a coverage decision and Dan's. **The trim outlives `simctl erase`** (launchd overrides live outside the device data directory), so a control run is trimmed unless you undo it: `scripts/sim-trim.sh --restore`, effective on the next boot.
- Serializing Swift Testing does NOT buy memory (peak demand 13.9 → 14.2 GiB); it is in the tree for descriptors and costs 10.56x on the Swift Testing phase. The `SWT_EXPERIMENTAL_MAXIMUM_PARALLELIZATION_WIDTH` env var reaches the process and is ignored; `TEST_RUNNER_<VAR>` on the command line is not forwarded to a unit-test host; the plan's `environmentVariableEntries` is the only route in.
- On a 137, read `disk free min` in the memory series before blaming RAM: swap lives on the same volume, and xcodebuild's post-kill diagnostics collection has pushed it to 30 GiB.
- **Do not edit a shell script while a run of it is in flight.** The shell reads by byte offset and resumes at a shifted position (`line 284: he: command not found`, then a second full plan on the same log).

### Mutation testing (`scripts/mutation-battery.sh`, `scripts/mutation_verdict.py`)

**Mutation testing is the priority, not the casualty.** Across the seven beads of 2026-08-12/13 every real finding came from a surviving mutant, a behavioural probe or a device pull — not one from a full-plan gate. A KILLED column is not evidence; the observed victim set matching a prediction written down BEFORE the run is.

```
KILLED    a STATED failure verdict for the expected test
SURVIVED  a STATED pass verdict — a positive ✔, never the absence of a ✘
VOID      nobody judged it (no verdict / crashed / denied / skipped), or the BATCH lost its host, or the
          bundle used a result string the parser cannot read.  exit 5.  Re-run — except an unreadable
          result string, and an expectation the MUTATION made SKIP, which a re-run can never clear.
ERROR     could not be evaluated: anchor drift, build failure, an expectation naming a test that never ran.
          exit 3.  Fix the EDIT.
```

`VOID` outranks FAILED and ABSENT because of the asymmetry, not the frequency: a discarded real kill costs a scoped `--only` re-run and announces itself; a kill credited off a dead host is a FALSE KILL that enters the ledger silently (playhead-gjlp0). Verdicts are read from the batch's own `.xcresult`, and every non-KILL verdict prints its log and bundle path — never look a batch up by number, batch numbers repeat across days.

**Four ways a mutant stops being evidence (playhead-8cjo, gjlp0):**
- **Lost rail** — KILLED, but not by the property you think (one character of case was the whole bypass). Remedy: a second mutant that re-creates the bypass VERBATIM with a DIFFERENT predicted victim set.
- **Inert mutant** — the edit no longer changes behaviour on today's code, reads SURVIVED, and sends you hunting for a rail that exists. Ask first whether the edit still changes anything.
- **False kill** — the edit became the shipped behaviour and every rail that reddened is credited. The only one that corrupts a recorded ledger; rule it out first when a bead restructures code an existing mutant targets.
- **Dead host** — a test whose host died emits no verdict, and the battery used to score that as a PASS. It is `VOID` now.

Anchor drift (`patch` matches 0 or 2+ times) announces itself as ERROR — but re-READ every mutant aimed at restructured code rather than re-anchoring mechanically, which converts drift into an inert mutant or a false kill.

- **One battery per worktree, enforced (playhead-pu7e).** Exit **75** = somebody holds this worktree; wait. Exit **2** = the lock could not be established and nobody holds it (full volume, unwritable git dir); waiting never helps. A concurrent battery is undetectable from inside either run and destroys verdicts silently.
- **The battery has never run `git checkout -- .`** — it restores only the files it mutated. Three agents destroyed their own uncommitted work running that by hand on 2026-08-04. Check for residue with `git status --porcelain -- Playhead`.
- `rc=65` with no tests is diagnosed (CONTENTION / WEDGED RUNNER with the `simctl shutdown all` recovery / DISK / OOM / BUILD FAILURE / UNDIAGNOSED) with the matched line printed. `xcrun: error: unable to find utility "simctl"` there is a secondary symptom of diagnostics collection, not the clone-parallelism gotcha.
- The baseline preflight requires an expectation to name a test that PASSED, not merely started; a red tree, including one red only by resource denials, is refused with "RE-RUN" as the remedy.
- A kept `$WORK` holds `.xcresult` bundles (~30 MB each, ~1.2 GB across a 41-batch crash-looping run) and is reaped only by `scripts/disk-cleanup.sh`'s 3-day sweep — no cap, deliberately, because dropping the oldest destroys the evidence for early batches. **Do not sweep `/private/tmp/playhead-mutation-battery.*` while a run is in flight**: `WORK` is created at script load, so an empty one is usually a run that started a second ago. Reap by the path the battery prints.
- 63 Swift Testing display names are claimed by more than one suite and three MUTATIONS records name one (`playhead-fyma7`): a namesake's failure can credit a false KILL.
- **`scripts/mutation-battery-untypeable.py` (TY series, playhead-x0lb)** reports **UNTYPEABLE** — the mutated source must FAIL TO COMPILE — for the defect class no test can see (two quantities sharing a unit, one read as the other). It builds the APP target only, so a round that edits tests still owes a `build-for-testing`; `--only` cannot drop the `TY99` control; `--check-inventory` is a buildless preflight. `RegionFillDoor` is the compile-enforced token for region assembly (limit L-F: it stops an inattentive author, not a determined one).
- Rails: `python3 -m unittest scripts.tests.test_mutation_verdict` (123 tests, ~11 min — 41 drive the battery end to end against a stubbed gate) and `scripts.tests.test_mutation_battery_lock`.

### The full plan runs ONCE, before merge — not per review round (Dan 2026-08-13)

A review round uses `-only-testing:` over the suites its diff touches; the full plan is the merge gate and nothing else. Across 61 preserved full-plan logs, 649 NEW failures were reported and essentially none was real — 60 s time limits under the full run's own load, in the same dozen suites (`BackgroundGrantBudgetTests`, `SilenceCompressionCoordinatorTests`, `BackgroundProcessingServiceTests`, `PlaybackInterruptionTests`, `SpeechRecognitionRequestGateTests`, `ForceQuitResumeTests`, …) that pass in seconds re-run scoped. The load-sensitive population is BOUNDED, not rotating without limit: 269 names over 38 runs, 168 recurring, covering ~97 % of any run's failures, so the residual of a run is its diff. Of the 269, 93 have only ever failed an EXPECTATION — the union is not a PerfGate shortlist. **What this does not relax:** mutation testing, lint, the SHA-256 pin / byte-exact restore discipline, and the merge gate's full plan read in both formats with its census.

A scoped run's cost has not been re-measured since the fast plan was serialized; re-time before budgeting a round around the old "seconds" figure.

### PerfGate'd measurement tests (`scripts/perf-tests.sh`, playhead-zx0l / o89d)

What earns a place: **the wall-clock quantity IS the assertion** (a clock read compared to a constant) AND it is scheduler-dependent waiting — a poll loop, an `AsyncStream` consumer, a cross-actor callback — not CPU-bound work, which passes fine under the full plan. Gate with `PerfGate` in source **and** add the test to `MEASUREMENT_TESTS` in the script. `PerfGate` is opt-IN, which cost six weeks of silently-unrun tests when the pass's destination did not exist, so **run the pass after gating anything and read BOTH formats of its output.** Moving a load-sensitive test here is a coverage decision, not a measurement one; `playhead-3f79`'s `settleWaitDoesNotLie` is deliberately still un-gated. The pass is green since playhead-xul6 moved the `SystemLanguageModel` read off the main actor (no budget was widened); `testInitSynchronousCallGraphIsFreeOfFoundationModels` walks init's synchronous call graph, with limits L-6/L-7 documented in `SwiftSourceCallGraph`. **The device has not been measured since** — do not assume it is fine.

### Diagnostic instruments nobody runs automatically (playhead-vk68m)

```bash
python3 scripts/gate-fd-paths.py --pid <testhost> [--cross-check]         # WHAT the host holds, by path
python3 scripts/gate-fd-paths.py --watch --interval 10 --out run/summary.jsonl --peak run/peak.json
python3 scripts/fd_ceiling_sweep.py --csv artifacts/fd-ceiling-sweep.csv   # do host-losing runs reach the ceiling?
python3 -m unittest scripts.tests.test_gate_fd_paths scripts.tests.test_fd_ceiling_sweep   # ~12 s; run if you edit either
```

`gate-fd-paths.py` self-tests its ctypes layout and exits 3 rather than sample off a wrong offset. `lsof -p PID | wc -l` is NOT the open-fd count (it also lists cwd, txt and every mapped dylib), and `proc_pidinfo(PROC_PIDLISTFDS)` with a NULL buffer returns the table's CAPACITY, a high-water mark that never falls. `scripts/gate-memory-sample.py` samples it correctly and records -1, never 0, on a failed read.

## Parallelism Ceiling

**Maximum 2 concurrent subagents running `xcodebuild` at any time on this machine (16 GB RAM).** Each parallel build can spike 1–3 GB during Swift compilation; combined with Xcode GUI, simulator, sourcekit indexers, and Claude itself, going past 2 has historically OOM'd Xcode (2026-04-17 incident). When orchestrating waves of beads, queue rather than fan out beyond 2. Sequential is always safe. Full test gates are stricter still: one at a time (see Test plans).

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

- **The squash-merge trap.** This repo squash-merges PRs, so the branch tip is never an ancestor of `main`: `git merge-base --is-ancestor` always reports NOT MERGED and `git branch -d` always refuses, for fully merged branches. PR state is the authority, and `-D` is correct only behind that check — never let it stand alone.
- **The diverged-main trap.** `git pull --ff-only` aborts whenever local `main` carries an unpushed commit (docs, scripts, this file). Inspect `git log --oneline origin/main..main` and `main..origin/main` first; if it is your own unpushed work on a clean tree, `git pull --rebase`, then push. Never `--force` or reset.
- Don't test merged-ness with `git branch --merged` — a branch checked out in a linked worktree lists as `+ bead/foo` and the guard fails every time. For a true merge commit, delete the remote branch first and `-d` succeeds.

### Safety rails

- Before `rm -rf`: path must start with `/Users/dabrams/playhead/.worktrees/`, `/private/tmp/playhead-`, or `$TMPDIR/Deleting-` (CoreSimulator's own trash — see below), and must NOT appear in `git worktree list --porcelain`. The path is proved, not assumed.
- Never pass `--force` to `git worktree remove` without explicit user approval — the refusal is the safety net.
- Echo what was removed so the transcript audits the session.

### The gate REFUSES to start below 13.5 GiB free (playhead-3nfa)

```bash
scripts/fast-gate.sh                  # refuses under 13.5 GiB with exit 28 (ENOSPC); deletes nothing
scripts/fast-gate.sh --reclaim-disk   # run the cleaner ONCE, re-check, then proceed or refuse
PLAYHEAD_DISK_MIN_GIB=9 scripts/fast-gate.sh    # a different threshold, deliberately
```

A gate that runs out of room does not fail — it WEDGES: xcodebuild stays alive with zero output and never exits, indistinguishable from a slow run. 13.5 is the worst measured full-plan drawdown (12.15 GiB, free-at-start minus minimum during the run) plus a 1.35 GiB margin for what a 5 s sampler cannot see; a cold/warm split was tried and rejected (simulator state moves ~8 GiB and swamps the cache saving). `df` lags on the way back up, so never compute what a run keeps from an exit reading. **The threshold does not cover swap, which lives on this volume:** a run that exceeds RAM went from 31.75 GiB free to 0.48 (playhead-3rql). It applies to selective runs too. `PLAYHEAD_SKIP_DISK_PREFLIGHT=1` exists and is deliberately not printed in the refusal.

### `simctl erase` can report success and free nothing (playhead-cgka)

CoreSimulator moves erased data to `$TMPDIR/Deleting-<uuid>/` and reaps it asynchronously; a directory the reaper cannot READ (the suite chmods one to `0o300` by design, and an abnormal exit skips the restore) kills the reap and strands the bytes forever. Seven orphans held 15 GiB. Check here before concluding the box is out of disk:

```bash
du -sk "$TMPDIR"Deleting-* 2>/dev/null | sort -rn      # what is stranded
chmod -R u+rwx "$TMPDIR"Deleting-*                     # u+w is NOT enough — read is what an enumerator needs
rm -rf "$TMPDIR"Deleting-*
```

### Orphan sweep script — the ONLY cleaner in the repo

`scripts/disk-cleanup.sh` runs weekly via cron; `--dry-run` previews, logs go to `.logs/disk-cleanup.log`. It removes `.worktrees/<slug>/.derivedData` whose worktree is no longer registered, `/private/tmp/playhead-*` older than 3 days that are not active worktrees, superseded `.xcresult` bundles (the most recent per worktree is always kept), and `$TMPDIR/Deleting-*` (chmod first). A live-build guard resolves the cwd of every running `xcodebuild`/`swift-frontend` with `pgrep -x` — never `pgrep -f`, which self-matches any shell whose command line contains the string — and skips anything under it. **Extend it, don't write a second one**: `scripts/disk_preflight.py` measures and refuses but delegates every removal here. Verify: `python3 -m unittest scripts.tests.test_disk_preflight` and `scripts/mutation-battery-disk-preflight.py`.
