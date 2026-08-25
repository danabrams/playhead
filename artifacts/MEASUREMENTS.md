# playhead-vk68m — measurement log

Written as each measurement lands, because an agent's context is not storage.

---

## M0. THE PARALLEL-REGIME BASELINE — every number below was taken under it

Dan chose option A (`parallelizable: false`) after this run. It is recorded
first, and separately, because the whole value of having measured today's regime
is being able to say what serialization changes. Anything compared against these
figures must have been taken the same way.

`scripts/fast-gate.sh`, `PlayheadFastTests`, iPhone 17 simulator, **trimmed**
(`fast-gate: simulator processes after trim` present in the log), fresh worktree,
cold build, 2026-08-24. Log preserved at
`/Users/dabrams/playhead-gate-artifacts/vk68m/vk68m-run1-fullplan.log`.

| quantity | parallel (this run) |
|---|---|
| tests / suites | **11,785 / 1,441** |
| Swift Testing phase | **252.792 s** |
| xcodebuild `Testing started completed` | **359.741 s** |
| test-host peak open fds (gate sampler, 10 s) | **2,439 of RLIMIT_NOFILE soft 2,560 — 95.3 %** |
| highest descriptor number ever seen (`max_fd`) | **2,558 = soft − 2** |
| peak vnodes (fd-paths watcher, 10 s) | **2,399** |
| tail FLOOR | **453 (449 vnode, 3 socket, 1 kqueue)** |
| implied concurrent WAL stores at the peak | **~650** — (2,399 − 449) / 3.00 |
| RESOURCE casualties | **27** |
| gate-baseline | **RED (5 known / 3 NEW)** |
| host restarts / crashed-host census | **0 / 0** — but read the next paragraph before quoting it |
| peak demand / swap | 13.9 GiB of 16.0 GiB / 1.5 GiB |
| xcodebuild exit | 65 (`** TEST FAILED **`) |

Two readings of the peak, and they are different instruments rather than a
disagreement: the gate's sampler and the fd-paths watcher both take a 10 s
snapshot, so both UNDER-report a transient — `max_fd` is the durable witness,
and 2,558 means descriptors 0..2,557 were all in use at once, because `open()`
returns the lowest free number.

**THE `0 / 0` WAS A HALF-QUOTE, AND THE HALF IT DROPPED REVERSES IT (review).**
The gate's own line reads, in full:

```
NO VERDICT — every test that started reported an outcome, but the evidence
below says part of this run was still lost. Read it before reading the split
above.
```

and the evidence below it is `DID NOT RUN — 2 of 118 recorded tests were never
reached` plus two `BLAMED, UNMATCHED` entries. So the crashed-host census is
genuinely zero and the run still lost things: both `DID NOT RUN` names carry
`(no verdict — denied a file it needed)`, i.e. they are two of the 27 denials
already in the row above, not a separate loss. Cutting the sentence at the comma
turns "no test died silently" into "nothing was lost", which is the standing
defect class applied to a sentence rather than to a number.

**THE 3 NEW ARE ON A TREE WITH NO PRODUCTION CHANGE AT ALL, WHICH IS NOT WHAT
THIS PARAGRAPH SAID (R2).** It said they were "on a tree whose only production
change is the `sqlite3_system_errno` suffix". That suffix landed in `c17fdc48`
at 21:36; run 1's test phase ran 21:10-21:16 and its 16.6 MB log contains
**zero** occurrences of `sqlite3_system_errno` and zero `[enzva]` lines. So run
1's `Playhead/` was byte-identical to the base commit `18a7423c`. The conclusion
— that the three are the standing rotation rather than this diff — survives and
is in fact stronger, but it was resting on a change that was not in the build:
a value that names one thing read as though it named another, in the sentence
whose whole job was to say what the tree contained. They are named in the log
and carried into the comparison below.

---

## M4. OPTION A MEASURED — the ceiling is GONE, the floor is NOT, and the cost is 10.6x

Same worktree, same simulator, same trim, same instruments. Run 2's log is
preserved beside run 1's, as
`/Users/dabrams/playhead-gate-artifacts/vk68m/vk68m-run2-fullplan-serialized.log`,
and run 2's fd dumps are committed under `artifacts/run2/` exactly as run 1's
are. (Both of those were true only after R2: the sentence claimed a preserved
log that was not there and `artifacts/run2/` was UNTRACKED, so every number in
this section would have died with the worktree at bead close.)

**THE TREE MOVED BY MORE THAN ONE LINE, AND R2 CORRECTED THIS SENTENCE (it read
"one line changed").** The intervention is one line —
`"parallelizable" : false` on the PlayheadTests target in PlayheadFastTests,
`a59281fb`. It is not the whole diff between the two runs. `c17fdc48` also
landed in between: `SQLiteSystemErrno.swift` (+117 production lines), a 15-line
change to `AnalysisStore.swift`, and the six new rails the `+6` row below
counts. The errno read is `sqlite3_system_errno(handle)` on failure paths and
opens no file, so it is very unlikely to move a descriptor count — but "very
unlikely to matter" is a judgement, and the sentence it replaced asserted there
was nothing to judge.

| quantity | PARALLEL (M0) | SERIALIZED | change |
|---|---|---|---|
| tests / suites | 11,785 / 1,441 | **11,791 / 1,442** | +6 — `c17fdc48`'s six new rails; see the paragraph above for the rest of that commit |
| Swift Testing phase | 252.792 s | **2,668.459 s** | **10.56x** |
| xcodebuild `Testing started completed` | 359.741 s | **2,766.389 s** | **7.69x** |
| test-host peak open fds (gate sampler) | 2,439 = **95.3 %** of soft 2,560 | **457 = 17.9 %** | −81 % |
| `*** AT THE CEILING ***` banner | printed | **absent** | — |
| highest descriptor number ever SEEN (`max_fd`, a 10 s sample) | 2,558 = soft − 2 | **466** | −2,092 |
| peak vnodes (fd-paths watcher) | 2,399 | **463** | — |
| tail FLOOR (see note) | 453 (449 vnode) | **452 (449 vnode)** | **unchanged** |
| implied concurrent WAL stores at the peak | ~650 | **~5** | −99 % |
| RESOURCE casualties | 27 | **0** | **−27** |
| gate-baseline | RED (5 known / 3 NEW) | **RED (0 known / 1 NEW)** | — |
| host restarts / lost verdicts | 0 / 0 | **0 / 0** | — |
| peak demand / swap | 13.9 GiB / 1.5 GiB | **14.2 GiB / 1.8 GiB** | +0.3 / +0.3 |

### Every prediction stated before the run held, including the one that mattered

The prediction was committed in `a59281fb`, before the run, precisely so this
could be read as a test of it:

* **The peak collapses.** ~650 concurrent stores to **~5**; the fd peak from
  95.3 % of the binding limit to 17.9 %. ✅
* **The FLOOR does not move.** 453 → **452**, and the vnode component is **449
  in both**. That is the sharpest confirmation available that the floor is
  `playhead-882eg`'s ~81 retained `PlayheadRuntime` graphs and not concurrency:
  retained objects accumulate whether or not tests overlap. It also means the
  floor is now **98 % of everything the host holds**, and it is the half that
  grows with every new runtime-constructing test. ✅
* **RESOURCE casualties go to zero.** 27 → **0**. ✅
* **`playhead-sip2`'s four `SkipOrchestratorRevertTests` do not fail.** ✅ —
  and the check is stronger than "absent from the failures", which is trivially
  true of a run with one failure. All **six** `@Test`s in that suite RAN and
  PASSED in run 2, by name: the Listen revert, the managed-loop and suggest-loop
  time-range reverts, the anonymous time-range revert, the suggest-only revert,
  the suggest Yes and the banner No. That fix landed in `96a4fc81` and holds
  under the regime it was written for.

The floor accrues LATE and gradually rather than at once. Read straight off
`artifacts/run2/progress.txt`, which is the only file pairing a descriptor count
with a STARTED-test count: **132 at 1,725 · 194 at 4,264 · 371 at 9,931 · 450 at
10,979**. That is what one should expect if it is one leak per
runtime-constructing test rather than a constant. (The first pair read "129 at
1,700" until R2. 129 is a real reading in `summary.jsonl` about forty seconds
earlier, i.e. before the test phase began, so it was a pre-ramp value quoted
against a test count it does not belong to. The other three reproduce exactly.)

**The two FLOOR cells are not derived the same way, and the row label said they
were** ("median of 20 plateau samples", corrected at R2). The serialized figure
is that: the last twenty watcher samples of run 2 have median 452, vnode median
449, over a tail that oscillates 447-456. Run 1 has no twenty-sample plateau —
its watcher series ends `456 456 453 453 453 453 453`, a FIVE-sample flat run,
and the median of its last twenty is **798** because the ramp is still in the
window. 453 is run 1's tail SAMPLE (449 vnode + 3 socket + 1 kqueue), which is
also what `artifacts/run1/TAIL-453.json` holds. The comparison the row is making
— 449 vnodes in both — is unaffected; the derivation is not one method.

### THE COST IS 10.6x, NOT 5.08x — do not quote playhead-blsh's figure for this

`playhead-blsh` measured 5.08x on a different tree and CLAUDE.md already warns
its two serialized phases differed by 673 s at near-identical memory, so its
number is a floor rather than a price. **Measured here it is 10.56x on the Swift
Testing phase and 7.69x on the whole test operation** — roughly double blsh's,
and it turns a ~6 minute merge gate into a ~46 minute one. That is the honest
number for this box today and it should be the one quoted.

**And memory got very slightly WORSE, not better** (13.9 → 14.2 GiB peak demand,
1.5 → 1.8 GiB swap). blsh's case for C was partly that it lowers the test host's
rss; whatever it saves there, whole-box demand did not fall, and a run that lasts
ten times as long simply has ten times as many chances to be sampled at a high
point. Serialization bought descriptors here, not memory.

### The one NEW failure is NOT a serialization casualty, and it is not this diff

`AnalyticsCounterStoreTests."The shared store is volatile under XCTest"` fails in
**both** regimes — it is one of run 1's three NEW as well — and it fails on other
branches' preserved logs (`et2d/fullgate-r5-run3.log`, `gate-462-verify.log`).
It is diagnosed and fixed in `f846e1a3`: the assertion read
`UserDefaults.standard.data(forKey:) == nil`, a property of the DEVICE's whole
history rather than of the code under test, and it fails identically on other
branches' preserved logs (`et2d/fullgate-r5-run3.log` at 117.179 s,
`gate-462-verify.log` at 128.110 s — both re-checked at R2). It asserts a DELTA
now. (This sentence said "see M5", and there was no M5; the section numbered M4
at the bottom of this file is now M5.)

The other two NEW from the parallel run — `a download the daemon answers writes
NO row…` and `a transfer created but never resumed leaves a row naming THAT
bound` — **passed** serialized, at 0.418 s and 0.140 s.

**WHY they passed is NOT established, and the first version of this line said it
was (R2).** It read "Both are in the load-sensitive families CLAUDE.md already
names, so serialization removed them along with the denials." Both live in
`BackgroundDownloadDropLedgerTests`, which appears **zero** times in CLAUDE.md
and in **none** of the 118 entries of `scripts/gate-baseline.PlayheadFastTests.json`;
and both failed on EXPECTATIONS in run 1
(`_backgroundDownloadAdmissionCountForTesting() == 1`), not on a time limit,
which is the kind CLAUDE.md explicitly says is *not* the starvation signature.
The starvation reading is a reasonable hypothesis — a starved async admission is
exactly what that assertion would report — but it is one observation each way on
one run, and it was stated as a family membership that does not exist.

---

## M1a. The ~449 FLOOR is REPRODUCIBLE — five runs, 447-459

The bead recorded the floor from ONE run (449 vnodes flat for 22 samples).
Re-derived from the memory series of every preserved full-plan run that carries
one, using `testhost_fds` from the series rather than any fresh regex:

| series | samples | pre-ramp | PEAK | max_fd | tail plateau | flat samples |
|---|---|---|---|---|---|---|
| `fd-series-s34ux.csv`     | 46 | 3 | 2539 | 2559 | **453** | 19 |
| `merge-mem2.csv`          | 36 | 3 | 2557 | 2556 | **453** | 10 |
| `merge-mem3.csv`          | 37 | 3 | 2529 | 2543 | **459** | 11 |
| `fd-462.csv`              | 30 | 3 | 2470 | 2559 | **455** |  5 |
| `fd-series-461-verify.csv`| 38 | 3 | 2441 | 2556 | **447** | 14 |
| `merge-mem.csv` (RESTARTED) | 30 | 3 | 2537 | 2537 | 23 | 3 |

**mean 453.4, range 447-459, spread 12** — i.e. **17.5-17.9 % of the 2,560
budget**, held at the tail of every run that reached one. The restarted run is
the exception that proves the reading: its host was REPLACED, and the
replacement holds 23 — the value a freshly-launched host sits at for its first
several samples, well short of the floor. (Re-derived at review: the `pre-ramp`
column above is the FIRST reading of each series, which is 3 on all six; 23 is
the short plateau that follows it. Two different early numbers, and the earlier
wording called 23 "the pre-ramp number" while the table beside it said 3.)

Two further things the series says and the bead did not quote:

* **`max_fd` reaches 2,543-2,559 on five of six runs** — within **1-17 of the
  soft limit itself (2,560)**, which is **0-16 of `soft - 1`**, the highest
  number the kernel can hand out. Two of the five land exactly on `soft - 1`.
  (The earlier wording said "within 1-17 of `soft - 1`", which is the right
  spread against the wrong anchor and understates the two exact hits.) The
  saturation witness is not one observation either.
* Every run starts at **3** descriptors. The floor is acquired DURING the run.

**Re-derived at review** from the same six CSVs, with `flat samples` defined as
the longest run of consecutive identical `testhost_fds` readings at the tail and
`samples` as the rows carrying a reading (`testhost_fds >= 0`): every cell above
reproduces except `fd-462.csv`, whose tail plateau of 455 is **5** samples and
was recorded as 1. Its series ends `... 455 455 455 455 455, -1, 6` — the `-1`
(not recorded) and the `6` (a fresh process) are after the plateau, so a count
that stops at the last row reads 1 where the plateau is 5.

---

## M1b. WHAT THE 449 ARE — one live run, every descriptor read back by PATH

Full plan, iPhone 17 simulator, trimmed, 2026-08-24, `scripts/gate-fd-paths.py`
sampling the host every 10 s and keeping every sample's complete descriptor list.
The run saturated and settled to **453** — the same floor as the five preserved
runs — and the tail dump was taken **while the host was still alive**. Its
saturation witness is `max_fd` **2,558**, at sample 22 (count 2,127), which is
`soft - 2` and is the figure M0's table quotes: descriptors 0..2,557 were all in
use at the moment 2,558 was handed out, because `open()` returns the lowest free
number. (This paragraph said **2556 = soft-4** until R2. 2,556 is the TAIL
sample's highest open descriptor — the value in `TAIL-453.json` — not the run's
highest, and quoting one for the other inside the section that corrects two
other quantities for exactly that reason is the standing defect class at home.)

| n | share | what |
|---|---|---|
| 81 | 17.9 % | production `.../Application Support/Playhead/AnalysisStore/analysis.sqlite` |
| 81 | 17.9 % | the same file's `-wal` |
| 81 | 17.9 % | `Library/Caches/Diagnostics/surface-status-<ts>-<uuid>.jsonl` — **81 DISTINCT files** |
| 76 | 16.8 % | production `.../Application Support/AdCatalog/ad_catalog.sqlite` |
| 76 | 16.8 % | the same file's `-wal` |
| 11 + 11 + 11 | 7.3 % | `tmp/PlayheadTestScratch/PlayheadTestStore-<uuid>/analysis.sqlite{,-wal,-shm}` |
| 7 | 1.5 % | `Documents/bg-task-log.jsonl` — **ONE file, seven descriptors** |
| 2 | 0.4 % | the two production `-shm` files (one each) |
| 3 + 3 + 3 | 2.0 % | SwiftData `Playhead.store{,-wal,-shm}` (one each), `/dev/*`, `/var/run/*` |
| 3 | 0.7 % | `/System/Volumes/Preboot/Cryptexes/{OS/System/Library/dyld, OS, Rosetta}` |
| 3 + 1 | 0.9 % | sockets, kqueue |

**The rows above sum to 453 — check them.** `81+81+81+76+76 = 395`, `+33` scratch
`= 428`, `+7` bg-task-log `= 435`, `+2` production `-shm` `= 437`, `+9` (SwiftData
3, `/dev/*` 3, `/var/run/*` 3) `= 446`, `+3` Cryptexes `= 449`, `+4` sockets and
kqueue `= 453`. The three Cryptexes descriptors were missing from the first
version of this table, which therefore summed to 450 against a stated 453 and
said nothing about the gap. `artifacts/run1/TAIL-grouped.txt` is the unabridged
listing and always had them.

**WHAT WAS COUNTED AND WHAT WAS EXCLUDED.** The population is the process's file
descriptor TABLE, enumerated with `proc_pidinfo(PROC_PIDLISTFDS)` into a real
buffer — the same call, with the same head-room, that `gate-memory-sample.py`
uses, so the 453 here and `testhost_fds` in the memory series are the same
quantity. Paths come from `proc_pidfdinfo(PROC_PIDFDVNODEPATHINFO)`, one call per
descriptor. **Nothing here is an `lsof` row count.** `lsof -p PID | wc -l` also
lists `cwd`, `rtd`, `txt` and every mapped dylib, none of which occupies a
descriptor — the cross-check in the tool splits on the FD column and was
validated separately against a process holding 100 known descriptors (kernel and
lsof agreed exactly; 18 rows excluded as `cwd`/`txt`). The instrument
self-tests its ctypes layout against a path this process chose before it will
report anything, because a wrong offset returns a plausible string rather than
an error. Sockets, kqueues and pipes have no path and are reported as
`<socket>`/`<kqueue>`/`<pipe>` rather than dropped.

**A DEFECT IN THIS INSTRUMENT, FOUND WHILE WRITING THIS UP, AND IT IS THE SAME
CLASS.** The watcher's `--last` file is "the most recent sample", and the target
is rediscovered every cycle as the largest-RSS process under `/Playhead.app/`.
That predicate is right DURING a run and wrong the instant it ends: a stale
simulator app satisfies it too. So after the host exited, `last.json` was
rewritten with **twelve descriptors belonging to a different process** — a file
named for this run's tail, holding something else's. The analysis above was
taken from the correct sample while the host was alive, and the canonical dump
is now recovered from the per-sample archive (`artifacts/run1/full/
sample-0094-00453.json.gz`, pid 71372, count 453) and committed as
`artifacts/run1/TAIL-453.json`; it reproduces the table above exactly. The
watcher now PINS the first host it sees, reports `HOST CHANGED`, and writes any
other process's dumps to `*.pid<N>.*` rather than over the subject's.

### The shape is the finding: it is not many stores, it is ~81 RETAINED APP RUNTIMES

Read the three big rows together. **81 descriptors on ONE database file. 76 on
another. 81 diagnostic files with one descriptor each.** That is not
"143 stores never closed" — the per-store population is the 33 in
`PlayheadTestScratch`, and it behaves exactly as the bead predicted, at 3.00
descriptors per WAL store.

`PlayheadRuntime.init` constructs all three, at the DEFAULT (production) paths:

```
Playhead/App/PlayheadRuntime.swift:842   let resolvedStore = try! AnalysisStore()          -> .../Playhead/AnalysisStore
Playhead/App/PlayheadRuntime.swift:1153  let surfaceStatusLogger = SurfaceStatusInvariantLogger()
Playhead/App/PlayheadRuntime.swift:1246  adCatalogStore = try AdCatalogStore(directoryURL: dir)
```

and the arithmetic closes — **stated as an identity at review, because the first
version of it did not add up**:

```
162   81 runtimes x (analysis.sqlite + -wal)
152   76 runtimes x (ad_catalog.sqlite + -wal)
 81   81 runtimes x one surface-status jsonl
 33   11 PlayheadTestScratch stores x (db + -wal + -shm)
  7   bg-task-log.jsonl, ONE file, seven descriptors
  3   the -shm singletons (analysis, ad_catalog, Playhead.store)
  2   SwiftData Playhead.store + -wal
 13   3 /dev/*, 3 /var/run/*, 3 Cryptexes, 3 sockets, 1 kqueue
----
453
```

The earlier wording — "~81 live runtimes x 5 = ~405, plus the three `-shm`
singletons, the 33 scratch stores and ~16 of infrastructure = 453" — is 457, and
using the measured 76 rather than 81 for `ad_catalog` it is 447. Neither is 453,
and the residual it was silently absorbing is the 7 bg-task-log descriptors plus
the 3 Cryptexes.

`AdCatalogStore` is 76 rather than 81 because its construction is inside a
`do`/`catch` (`PlayheadRuntime.swift:1244-1252`) that logs and sets the store to
`nil` on failure — so five graphs lack one. **WHY those five failed is not
established**, and one candidate is uncomfortable: a run at the descriptor
ceiling is exactly where an `AdCatalogStore` open would be DENIED, which would
make the 76/81 gap a symptom of the thing being measured rather than a
conditional taking its other branch. It is 5 descriptors either way; it is
flagged rather than explained.

**Each of the three holds its descriptor for its OWN lifetime and releases it
when deallocated** — `AnalysisStore.deinit` calls `sqlite3_close_v2`
(`AnalysisStore.swift:11455`), `AdCatalogStore.deinit` calls **`sqlite3_close`**
(`AdCatalogStore.swift:406`), every statement in the store is finalized under a
`defer`, and `SurfaceStatusInvariantLogger`'s `FileHandle` is released with the
object. (The first version of this line said both call `sqlite3_close_v2`. They
do not, and the difference is not cosmetic: `sqlite3_close` returns
`SQLITE_BUSY` and leaves the connection — and its descriptors — OPEN when any
statement is unfinalized, where `sqlite3_close_v2` always releases. For the
population measured here it does not matter, because the graphs are never
deallocated at all and neither `deinit` runs.) So this is not a missing
`close()`. **It is ~81 `PlayheadRuntime` object graphs that are never
deallocated**, built by 61 construction sites across 10 test files
(`ShowSkipModeControlTests` 13, `NowPlayingViewModelTests` 13,
`RuntimeShutdownLifecycleTests` 20, ...). `PlayheadRuntime.deinit` exists and its
own comment records that `shutdown()` is mandatory and that `withTestRuntime`
enforces it — 34 uses against 61 sites. (61 / 10 / 13 / 13 / 20 / 34 all
re-counted at review and all reproduce.)

**WHY THIS IS NOT FIXED HERE.** Making those graphs release is not a `close()`
call — it is finding what retains a whole app runtime and changing how ~61 test
sites build one. That is the "architectural, or touches how every test opens its
store" case, so it is written up and filed rather than attempted.

### What it changes for the options already on the bead

* **The floor is a FUNCTION OF THE TEST SUITE, not a constant.** It is ~5
  descriptors per runtime-constructing test that leaks, so it grows with every
  new one. 17.7 % of the budget today; twice the runtime tests and it is 35 %.
* **Serializing (option A) does not touch it.** Retained graphs accumulate
  whether or not tests run concurrently, so A collapses the PEAK and leaves the
  FLOOR exactly where it is — and the floor is the half that keeps growing.
* **It is the only lever that REDUCES DEMAND.** A lowers concurrency, B raises
  supply; releasing the runtimes returns the **395** descriptors the retained
  graphs hold, or **404** counting the 7 bg-task-log and 2 production `-shm`
  descriptors that belong to them too — i.e. 15.4-15.8 % of the budget. (The
  earlier figure, ~437, was `453 - 16` and so also claimed back the 33
  `PlayheadTestScratch` descriptors that this same section identifies as the
  per-store population *behaving correctly*.) It is not sufficient on its own —
  the peak sits **1,949 above this floor** by the watcher's own count (2,402),
  or **2,105** if you measure it by the highest descriptor NUMBER (`max_fd`
  2,558); those are two different quantities and the earlier "~2,100" quoted the
  second while the sentence was about the first. (2,105 was 2,103 until R2, off
  the tail sample's `max_fd` rather than the run's — see the paragraph above.) But it is a genuine defect
  independently of the ceiling, and it is the one thing here that is neither a
  cost trade nor a moved cliff.

---

## M2. Do the runs that lose their host reach the ceiling? THE QUESTION IS NOT ESTIMABLE FROM THESE LOGS

`scripts/fd_ceiling_sweep.py`, over `/private/tmp`, `$TMPDIR`,
`/Users/dabrams/playhead`, `/Users/dabrams/.claude` and
`/Users/dabrams/playhead-gate-artifacts`, de-duplicated by content sha256.

**THE COUNTS BELOW ARE A MOMENT, AND `artifacts/fd-ceiling-sweep.csv` IS THE
PIN.** The sweep walks whatever logs are on disk when it runs, so its `n` grows
as runs accumulate. Re-run at R1 it reported **44 logs / 9 full-plan (2 lost /
7 completed)**, the ninth being this bead's own M0 run. Re-run again at **R2 it
reports 47 logs / 10 full-plan (2 lost / 8 completed)**. Every row of the
committed CSV is still present and unchanged in both re-runs; read the CSV, not
the sentence, if a specific number matters.

**THE TABLE IS NO LONGER DEGENERATE, AND THE RUN THAT BROKE THE DEGENERACY IS
M4's (R2).** Everything below this line was written when every full-plan run
ever measured sat between 93.6 % and 99.9 % of the soft limit, and it concluded
"there is no unexposed arm, so no association can be estimated". Serializing
produced one: **run 2 is a full-plan run at 17.9 % that COMPLETED**, and the
sweep now fills the empty cell —

```
CONTINGENCY TABLE, n = 10  (ceiling = >= 90 % of the binding soft limit)
                     lost the host     completed
  AT the ceiling                 2             8
  below the ceiling              0             1
```

Three things about that single cell before anyone reads a p-value into it. It is
**n = 1**, and 2-of-10 at the ceiling is a rate this cell cannot distinguish
from. It is an **INTERVENTION** rather than an observation — the one row in this
table where the exposure was set rather than found — which is what makes one
observation worth more here than one more at-ceiling run, and still not much.
And the intervention changed the whole regime, not just the descriptor count, so
"below the ceiling" and "serialized" are perfectly confounded in it. What the
row does establish is that the exposure is now VARIABLE, which is the thing the
paragraph below correctly said it was not. Read the rest of this section in the
past tense, as the record of a nine-run population with one arm.

**40 logs carry a `peak open fds` line. 32 of them are scoped or mutation runs**
whose peaks are 6-32 and which carry NO binding soft limit, because the Swift
probe that prints it (`TestHostDescriptorCeilingTests`) never ran in a selective
population. They are excluded rather than folded in against `kern.maxfilesperproc`.

**Eight full-plan runs remain, and the table is DEGENERATE:**

```
CONTINGENCY TABLE, n = 8   (ceiling = >= 90 % of the binding soft limit)
                     lost the host     completed
  AT the ceiling                 2             6
  below the ceiling              0             0
```

Every full-plan run measured on this box **under the parallel plan** sits at
**93.6 % - 99.9 %** of the soft limit. **In that population there is no unexposed
arm, so no association can be estimated.** That is a finding about the exposure,
not about the outcome: under the parallel plan, reaching the descriptor ceiling
is not a property of a bad run, it is a property of running the plan at all.
(The words "under the parallel plan" were added at R2 — without them the
sentence is falsified by M4's own run, which is in the same file.)

**The peak of a run that DIED is a censored observation.** A host that is lost
stops accumulating, so a restarted run's measured peak is biased DOWNWARD. The
two restarts sit 3rd and 5th of 8 by peak — which cannot be read as evidence
against the mechanism any more than a top-2 finish could have been read for it.

### What the sweep DOES settle, and it cuts against the lead

`[BiomeStorage] Failed to open lockfile` is the line the bead saw repeating
immediately before the restart marker. Counted across all eight:

| run | fate | `Failed to open lockfile` | `unable to open database` | `Bad file descriptor` | RESOURCE casualties |
|---|---|---|---|---|---|
| merge-gate2       | COMPLETE  | 1244 | 36 | 62 | 13 |
| fullgate-r5-run2  | COMPLETE  | 1264 | 48 | 28 | 32 |
| merge-gate3       | COMPLETE  | 1264 | 21 | 34 | 13 |
| gate-462-verify   | COMPLETE  | 1264 | 47 | 36 | 27 |
| gate-461-verify   | COMPLETE  | 1266 | 38 | 32 | 28 |
| fullgate-r5-run3  | COMPLETE  | 1248 | 35 | 19 | 23 |
| merge-gate        | RESTARTED |  244 |  0 | 228 | — |
| fullgate-r5       | RESTARTED |  330 |  9 |   9 | — |

All twenty-four cells re-derived at review by `grep -c` against the eight
preserved logs, and all twenty-four reproduce. The `RESOURCE casualties` column
is the gate's own `N tests hit a RESOURCE FAILURE` figure and is now produced by
`fd_ceiling_sweep.py` itself rather than by hand: it is the `resource_casualties`
column of `artifacts/fd-ceiling-sweep.csv`, `-1` where the gate never printed the
line, which is why the two RESTARTED runs read `—`. That CSV was regenerated at
review over the session scratchpad alone — the identical 40-log population,
byte-identical in every column it already had — so the table above is now
checkable from a committed file. The `lockfile` / `unable to open` /
`Bad file descriptor` columns are still hand greps over the log text.

**Apple's frameworks fail to open files ~1,250 times in every run that finishes
normally.** The signature the lead was built on is not specific to host death —
it is the steady state of a run at the ceiling. The two runs that DID lose their
host show FEWER of them, which is the censoring above.

`Too many open files` appears **zero** times in all eight. EMFILE is not what
this box produces; see M3.

### The two-point dose-response does not survive six points

The bead's 2026-08-25 comment read `2544 -> 32, 2397 -> 23` as "the higher the
descriptor peak, the more tests were denied a file". Over the six completed runs:

```
Spearman(peak fds, RESOURCE casualties)              = -0.203   (n = 6)
Spearman(peak fds, `unable to open database` lines)  = +0.200   (n = 6)
Spearman(RESOURCE casualties, `unable to open` lines)= +0.841   (n = 6)
```

The first two are opposite in sign to each other and neither is distinguishable
from zero at n = 6; the third is high, which is the check that the two denial
counts are measuring the same thing and the extraction is sound. **There is no
measurable dose-response inside the at-ceiling band** — which is what one would
expect if the band is 93-100 % of a limit that is being hit either way.

All three coefficients recomputed at review from the table above (tied ranks
handled as midranks, Pearson over the ranks): **-0.2029, +0.2000, +0.8407**.
They reproduce to the digits quoted.

---

## M3. `sqlite3_system_errno` separates three different bugs behind one string — ON THE MAC. **IT IS INERT IN THE APP.**

Measured 2026-08-24 against `/usr/lib/libsqlite3.dylib` via `ctypes`, on this
box. `sqlite3_libversion()` on that library reads **3.54.0** (re-read at R1 and
again at R2).

**THE VERSION CLAIM WAS WITHDRAWN AND HAS SINCE BEEN MEASURED — this paragraph
said "withdrawn" for two commits after that stopped being true (R2).** The
withdrawal was right when it was written: "the same version the iOS SDK ships"
was an inference doing the work of a control, and nothing in the simulator run
recorded a version. `f846e1a3` settled it instead of leaving it withdrawn — the
suite prints the simulator's own reading on every run, and the preserved log
`/Users/dabrams/playhead-gate-artifacts/vk68m/vk68m-scoped-round1fix3.log`
carries it: `[enzva] simulator SQLite version: 3.54.0 (the Mac's
/usr/lib/libsqlite3.dylib measured 3.54.0)`. So **both libraries report 3.54.0**,
which makes M3b SHARPER rather than weaker: the same version string answers the
same three conditions differently, so it is a build or configuration difference
and not a version skew anyone can wait out. Note what is still not measured —
the two libraries' BUILD OPTIONS, which is the thing that would actually explain
it.

Every one of these returns `rc=14` (`SQLITE_CANTOPEN`) with
`sqlite3_errmsg` = **`unable to open database file`** — the identical sentence:

| condition                                   | `sqlite3_system_errno` |
|---------------------------------------------|------------------------|
| parent path component is a regular file      | **20 — ENOTDIR**       |
| parent directory mode `0o000`                | **13 — EACCES**        |
| descriptor table full (`RLIMIT_NOFILE` = 96) | **9 — EBADF**          |
| *(control)* a successful open                | **0**                  |

Two things this settles.

**1. `playhead-enzva`'s premise is exactly right and its stated PREDICTION is
wrong.** That bead expected `EMFILE (24)` behind the CANTOPEN population. It is
**EBADF (9)**, measured directly at exhaustion: fill the table to
`RLIMIT_NOFILE`, then `sqlite3_open_v2` an *existing, valid* database and a
*brand new* path — both come back `rc=14 system_errno=9`. This is the same
kernel behaviour playhead-vk68m already recorded for raw `open(2)` (errno 9, not
24), now confirmed one layer up through SQLite's own VFS. The exhaustion reading
does not need re-examining; the POSIX-documented answer does.

**2. `0` IS NOT AN ERRNO.** A successful open leaves `sqlite3_system_errno` at
0, and SQLite records nothing there for failures that never reached the OS. So a
captured 0 means *the VFS recorded no OS error for this failure* and must be
rendered as such — never as an errno, and never as success. This is the standing
defect class waiting to happen and the reason the rendering says
`(none recorded)` rather than printing a bare number.

Repro: `artifacts/sqlite-errno-probe.py` — which covers the **exhaustion row
only** (it lowers `RLIMIT_NOFILE` to 96, fills the table, and opens both an
existing and a brand-new database). Re-run at review it prints
`table full after 92 hogs; open() errno=9`, then `rc=14 system_errno=9` for both
opens: the row reproduces exactly. The ENOTDIR (20), EACCES (13) and control (0)
rows are NOT in the committed script; re-derived by hand at review against the
same library they also reproduce exactly, but the artifact does not carry them,
so "Repro:" was over-claiming for three of the four rows.

### M3b. THE CORRECTION, AND IT IS THE STANDING DEFECT CLASS IN MY OWN MEASUREMENT

Everything above was measured against `/usr/lib/libsqlite3.dylib` — the MAC's
SQLite. The app runs against the iOS SDK's. **Reading a measurement of one
library as a claim about another is exactly "a value that names one thing read
as though it named another",** and it took a rail written from the host figure,
failing on the simulator, to catch it.

Measured on the **iOS 27 simulator**, in the real test host, through the same
`sqlite3_open_v2` flags `AnalysisStore` uses:

| condition | rc | `sqlite3_extended_errcode` | `sqlite3_system_errno` | message |
|---|---|---|---|---|
| parent path component is a regular file | 14 | **14** | **0** | unable to open database file |
| parent directory mode `0o000` | 14 | **14** | **0** | unable to open database file |
| the path itself IS a directory | 14 | **14** | **0** | unable to open database file |
| *(control)* a path that opens fine | 0 | 0 | 0 | not an error |

**NEITHER of SQLite's two discriminators carries the cause on this platform** —
not the system errno and not the extended result code — while SQLite's own log
line in the same run reads `os_unix.c:52971: (20) open(...) - Not a directory`.
The cause is known to the library and is not exposed through either API here.

**So `playhead-enzva`'s premise is refuted for the platform that matters.** It is
a correct description of the Mac and of SQLite's documented contract, and it does
not describe the app. Three consequences:

* `scripts/gate_baseline.py`'s ONE prose match on `unable to open database file`
  **cannot be retired in favour of an errno.** That is a standing limit on this
  platform, not a piece of unfinished work.
* The **exhaustion** case is NOT measured on the simulator. Forcing a full
  descriptor table inside a shared test host would take the host down with it.
  Three unrelated causes all report 0 through the same VFS hook, so 0 is the
  expectation — an expectation, not a measurement, and it is labelled as one.
* The call is **kept anyway**: it costs one call, renders 0 as `none recorded`
  rather than as an errno, and the two sites playhead-s34ux actually observed —
  `openSQLiteHandle` and `exec` — now carry the field in every gate log. Nobody
  has to re-derive this, and a future SDK that populates it will simply start
  showing the value. `prepare`/`step`/`nextRow` are deliberately NOT wired: on
  this platform the value is 0 at every site, so three more one-line changes
  would buy three more consumer interactions to verify and no information.

---

## M5. REVIEW ROUND 1 — what the instrument got wrong about ITSELF

Six defects in the two scripts, found by driving them rather than by re-reading
them, plus a mutation battery over the rails that found three of the rails
unable to fail. Every fix is in `scripts/gate-fd-paths.py`,
`scripts/fd_ceiling_sweep.py` and their two test files; nothing in the
measurements above changes as a result, because the run they were taken from
pre-dates the pin entirely.

### M5a. THE PIN WOULD HAVE PINNED THE WRONG PROCESS ON RUN 1 — measured, in run 1's own archive

`b49a8c76` added the pin because a stale simulator app clobbered `--last` AFTER
the host exited (twelve descriptors, pid 85292). `47c7e052` then tried to stop
the pin landing on a LEFTOVER app, and `dddf0f2e` withdrew that as "a hazard
that was inferred rather than measured". **The hazard is measured, and the
evidence was already committed.** Group `artifacts/run1/summary.jsonl` by pid:

| pid | samples | peak | what it is |
|---|---|---|---|
| **58651** | **62** | **20** | a LEFTOVER app: an identical 20-descriptor table for ~620 s across the cold build, gone before the host starts |
| 71372 | 32 | **2,402** | the test host — first sample is `count 3`, ramps to 2,402, settles at 453 |
| 77131 | 3 | 23 | after the host exits |
| 79998 | 2 | 10 | after the host exits |
| 85292 | 2 | 12 | the process that clobbered `last.json` |

The watcher was started before a cold build, so the FIRST `/Playhead.app/`
process it saw was 58651. Pinned first-seen, `peak.json` for that run holds
**twenty** descriptors and the real host's 2,402 goes to `peak.pid71372.json` —
the identical defect the pin exists to prevent, with the direction reversed.
(Run 1's dumps were taken before the pin existed, which is why the committed
`peak.json` is in fact pid 71372's; the ordering above is what the shipped code
would do with the same input.)

`pin_decision()` now promotes any process holding MORE descriptors than the
pinned one has EVER held. That discriminator needs no clock — the withdrawn age
bound asked macOS `ps` for the Linux keyword `etimes` and silently returned 0
forever — and it is monotone, so it cannot oscillate. It keeps the case the pin
was built for excluded: 12 is not more than 2,402. A watch now also ends by
printing a census of every process it saw with its sample count and its peak, so
"which process is `peak.json` about" is answerable from the log rather than only
by grouping the JSONL.

### M5b. Five more, each of the same shape: a failure returned as a value

* **`lsof_cross_check` took `.stdout` off a `check=False` run.** An lsof that
  could not run at all parsed to `descriptor_rows: 0` and printed `0` beside a
  kernel count of 2,539 — which reads as two instruments disagreeing rather than
  as one not having run. Exactly the `etimes` shape, in the same file, in the
  function whose entire job is to validate the count. It now reports the exit
  status and the stderr, and a non-zero exit that STILL listed descriptors is
  still believed (lsof exits 1 over a single file it could not stat).
* **`list_fds` could not tell a truncated read from a complete one.**
  `proc_pidinfo` writes `min(actual, buffersize)` and returns what it wrote, so
  a buffer the kernel filled exactly is a SHORT COUNT wearing the shape of a
  real one — and it can only happen while the table is growing fastest, i.e. at
  the peak, the one number this instrument exists to produce. The read is now
  retried with a larger buffer while it comes back full, and returns `None`
  ("not recorded") rather than a count that is quietly too small.
* **`--watch` could not tell "never appeared" from "gone away".** One
  `gone_since` variable covered both, so with the default `--deadline 0` a
  watcher started BEFORE the gate — the only order in which it can catch the
  ramp — exited on its second cycle having measured nothing and said nothing
  about it. **Corroborated live while this review was running:**
  `artifacts/run2/watcher.log` holds two interleaved invocations, and one of
  them ends `gate-fd-paths: 0 samples` while the other reports 244. The two
  wrote to one path at independent offsets, so the arguments of the invocation
  that measured nothing are not recorded and this is corroboration rather than
  proof — but a watcher exiting with zero samples on a run whose host held
  descriptors for twenty minutes is the shape, and it happened on the shipped
  code.
* **The interloper `--peak` file was not a high-water file.** Every non-pinned
  sample was written straight over the scoped path, so a file whose own comment
  called it a high-water mark held that process's LAST sample. The standing
  defect class in the instrument's own bookkeeping.
* **`fd_ceiling_sweep.py` carried a `resource_lines` field that named nothing.**
  It counted lines matching `RESOURCE` case-insensitively — on the M0 log that
  is the gate's own explanatory prose, dozens of lines, against a real casualty
  figure of 27. It was never printed and never written to the CSV, so nobody had
  yet read it as the casualty count; it is now `resource_casualties`, the gate's
  own number, `-1` when the gate never printed one.

### M5c. Three rails could not fail, proven by mutation

40 mutants over the two scripts, each with its victim predicted before the run;
**40 killed, 0 survived, and a docstring-only control survives.** Three of them
survived the FIRST pass and all three were rails naming a property they did not
test:

* `max_fd is the highest number not the count` asserted
  `max_fd == max(r["fd"] for r in rows)` — which is the expression `snapshot`
  itself evaluates — and `max_fd >= count - 1`. In a quiet Python process the
  table is contiguous, so `max_fd = len(rows) - 1` passes both. It now opens a
  descriptor at fd 900 first, which is the field shape too (`max_fd 2559`
  against a count of 2,539).
* `no reader can see a half-written dump` passed against a plain
  `open(path, "w")`. Atomicity is only observable when a write FAILS PART WAY,
  so the rail now interrupts a `json.dump` and requires the previous dump to
  survive.
* `PinnedHostRails` tested `_scoped()`'s string splicing and nothing about the
  pinning DECISION — removing the pin entirely left every rail green.

Rails: **76 tests over the two scripts (was 40), ~1.2 s, no build.**
`python3 -m unittest scripts.tests.test_gate_fd_paths scripts.tests.test_fd_ceiling_sweep`

---

## M6. REVIEW ROUND 2 — the evidence was UNCOMMITTED, and two sections of this file contradicted each other

R2 verified R1's seven fixes by driving them, re-derived every number in M0-M4
against the artifacts, and spot-checked R1's mutation ledger. **R1's fixes are
real.** Fifteen mutants over the two scripts, each with its victim predicted
before the run: **thirteen killed exactly the predicted test**, one is a proven
equivalent (`(\d+) tests? hit a RESOURCE` with `re.I`, which reads 27 on the
real log exactly as shipped does), and **one SURVIVED and is the rail hole in
M6d**. The thirteen cover `pin_decision`'s `>` and its promote branch,
`lsof_cross_check`'s exit status, `list_fds`'s truncation retry, the
never-seen/gone split, `record_peak`'s high-water, `_scoped`'s basename split,
`_atomic_json`'s rename, `max_fd`, the end-of-watch census, `find_test_host`'s
`ps` check, `fd_path`'s short-write refusal, and both directions of the
`resource_casualties` rename. The docstring-only control survived. Nothing R1
CLAIMED was found to be untrue; what follows is what it did not reach.

(That paragraph said "fifteen mutants … and every single-victim mutant killed
exactly the predicted test", which the same section's own M6d contradicts. A
count of what was RUN read as a count of what was KILLED — the standing defect
class, in the sentence certifying that a review looked for it.) What R2 adds:

### M6a. THE M4 EVIDENCE WAS NOT COMMITTED AND ITS LOG WAS NOT PRESERVED — HIGH

`git status` read `?? artifacts/run2/`. Every M4 figure comes from
`artifacts/run2/{summary.jsonl,peak.json,last.json,progress.txt,full/}`, none of
which was tracked, and M4's own sentence "Run 2's log is preserved beside run
1's" was false — `/Users/dabrams/playhead-gate-artifacts/vk68m/` held run 1's
log and one scoped log, and nothing else. The canonical bead-close sequence runs
`git worktree remove`, so at close every number in the newest section of this
file would have become unverifiable, while the section it is compared against
(M1b) had its dumps committed for exactly that reason. `artifacts/.gitignore`'s
own first paragraph is the statement of this rule. Fixed: the dumps are
committed, `gate.log` is preserved as `vk68m-run2-fullplan-serialized.log`, the
scoped verification log for `f846e1a3` is preserved as
`vk68m-scoped-round1fix3.log`, and `watcher.log` — 128 bytes, and the ONLY
witness for M5b's never-seen/gone finding — is exempted from the `*.log` ignore
rather than left to die with the worktree.

### M6b. M2 SAID THERE WAS NO UNEXPOSED ARM AND M4 CREATED ONE

Both sections are in this file and neither mentioned the other. Corrected in
place, in M2, with the new contingency table and with what a single
INTERVENTION row does and does not license.

### M6c. Four numbers that named the wrong quantity

Each is the standing defect class and each is corrected above rather than here:
M0's attribution of the 3 NEW to a production change that was **not in run 1's
build** (zero `sqlite3_system_errno` lines in 16.6 MB); M4's "one line changed",
which omits `c17fdc48`'s 117 production lines; M1b's `max_fd` **2,556**, which
is the tail sample's highest open descriptor rather than the run's **2,558**;
and M4's "129 descriptors at 1,700 tests", which is a pre-ramp reading —
`progress.txt` says **132 at 1,725**. Also corrected: M4's floor row claimed one
derivation ("median of 20 plateau samples") for two columns derived differently,
M4's "both are in the load-sensitive families CLAUDE.md already names" for a
suite CLAUDE.md never names, M3's withdrawal of a version claim that `f846e1a3`
had since measured, a dangling `see M5`, and two `## M4.` sections.

### M6d. ONE RAIL HOLE AND ONE RESIDUAL DEFECT, both found by mutation

* **`fd_ceiling_sweep.py`'s casualty pattern was under-guarded.** Relaxing
  `_RESOURCE_CASUALTIES` to `(\d+) tests?` **survived all 76 rails** and reports
  **1** against run 1's real figure of **27**, because the first `<n> tests` in
  a full-plan tail is `Test run with 11785 tests in 1441 suites`. The existing
  anti-vacuity rail guarded the word RESOURCE in prose and not the pattern
  latching onto a different quantity — which is the defect class the field was
  renamed for. New rail; the mutant now dies.
* **The never-seen/gone fix closes half the conflation, and the other half is
  now NAMED as LIMIT-1.** The guard is `samples == 0` — has ANY `/Playhead.app/`
  process been sampled — not "has the HOST been sampled", which
  `find_test_host` cannot answer. Driven through `main()`: a leftover sampled
  twice, gone for two cycles, then the real host, and the watcher exits with
  `2 sample(s) over 1 process(es)` and `peak.json` holding the leftover's
  **twenty**. That is run 1's own timeline (pid 58651 for 62 samples, then pid
  71372, `artifacts/run1/summary.jsonl`), and it survived only because those two
  were ADJACENT samples. It is not closed — the discriminator does not exist
  here, and the withdrawn `etimes` age bound was the last attempt — so it is
  pinned by a rail, described in `main()`, and made visible two ways: the watch
  now prints **why** it ended, and every invocation prints its **pid and argv**
  (which is precisely what M5b lacked when it had to settle for "corroboration
  rather than proof" off two interleaved watchers in one log).

Rails: **80 tests over the two scripts (was 76), ~2.6 s, no build.** Nineteen
mutants run at R2, eighteen killed with the predicted victim, one proven
equivalent (`(\d+) tests? hit a RESOURCE` with `re.I` — it reads 27 on the real
log, same as shipped), control survives.
