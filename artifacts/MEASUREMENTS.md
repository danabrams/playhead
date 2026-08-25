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
| host restarts / lost verdicts | **0 / 0** — "every test that started reported an outcome" |
| peak demand / swap | 13.9 GiB of 16.0 GiB / 1.5 GiB |
| xcodebuild exit | 65 (`** TEST FAILED **`) |

Two readings of the peak, and they are different instruments rather than a
disagreement: the gate's sampler and the fd-paths watcher both take a 10 s
snapshot, so both UNDER-report a transient — `max_fd` is the durable witness,
and 2,558 means descriptors 0..2,557 were all in use at once, because `open()`
returns the lowest free number.

The **3 NEW** are on a tree whose only production change is the
`sqlite3_system_errno` suffix, so they are the standing rotation rather than
this diff; they are named in the log and carried into the comparison below.

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
| `fd-462.csv`              | 30 | 3 | 2470 | 2559 | **455** |  1 |
| `fd-series-461-verify.csv`| 38 | 3 | 2441 | 2556 | **447** | 14 |
| `merge-mem.csv` (RESTARTED) | 30 | 3 | 2537 | 2537 | 23 | 3 |

**mean 453.4, range 447-459, spread 12** — i.e. **17.5-17.9 % of the 2,560
budget**, held at the tail of every run that reached one. The restarted run is
the exception that proves the reading: its host was REPLACED, and the
replacement holds 23, which is the pre-ramp number, not the floor.

Two further things the series says and the bead did not quote:

* **`max_fd` reaches 2,543-2,559 on five of six runs** — repeatedly within 1-17
  of `soft - 1`. The saturation witness is not one observation either.
* Every run starts at **3** descriptors. The floor is acquired DURING the run.

---

## M1b. WHAT THE 449 ARE — one live run, every descriptor read back by PATH

Full plan, iPhone 17 simulator, trimmed, 2026-08-24, `scripts/gate-fd-paths.py`
sampling the host every 10 s and keeping every sample's complete descriptor list.
The run saturated (`max_fd` 2556 = soft-4, so descriptors 0..2555 were all in
use) and settled to **453** — the same floor as the five preserved runs — and the
tail dump was taken **while the host was still alive**.

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
| 3 + 3 + 3 | 2.0 % | SwiftData `Playhead.store{,-wal,-shm}`, `/dev/*`, `/var/run/*` |
| 3 + 1 | 0.9 % | sockets, kqueue |

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

and the arithmetic closes: **~81 live runtimes x (2 analysis + 2 ad_catalog + 1
surface-status) = ~405**, plus the three `-shm` singletons, the 33 scratch
stores and ~16 of infrastructure = **453**. `AdCatalogStore` is 76 rather than 81
because its construction sits behind a conditional, so five graphs lack one.

**Each of the three holds its descriptor for its OWN lifetime and closes
correctly when deallocated** — `AnalysisStore.deinit` and `AdCatalogStore.deinit`
call `sqlite3_close_v2`, every statement in the store is finalized under a
`defer`, and `SurfaceStatusInvariantLogger`'s `FileHandle` is released with the
object. So this is not a missing `close()`. **It is ~81 `PlayheadRuntime` object
graphs that are never deallocated**, built by 61 construction sites across 10
test files (`ShowSkipModeControlTests` 13, `NowPlayingViewModelTests` 13,
`RuntimeShutdownLifecycleTests` 20, ...). `PlayheadRuntime.deinit` exists and its
own comment records that `shutdown()` is mandatory and that `withTestRuntime`
enforces it — 34 uses against 61 sites.

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
  supply; releasing the runtimes returns ~437 descriptors outright. It is not
  sufficient on its own — the peak is ~2,100 ABOVE this floor — but it is a
  genuine defect independently of the ceiling, and it is the one thing here that
  is neither a cost trade nor a moved cliff.

---

## M2. Do the runs that lose their host reach the ceiling? THE QUESTION IS NOT ESTIMABLE FROM THESE LOGS

`scripts/fd_ceiling_sweep.py`, over `/private/tmp`, `$TMPDIR`,
`/Users/dabrams/playhead`, `/Users/dabrams/.claude` and
`/Users/dabrams/playhead-gate-artifacts`, de-duplicated by content sha256.

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

Every full-plan run ever measured on this box sits at **93.6 % - 99.9 %** of the
soft limit. **There is no unexposed arm, so no association can be estimated.**
That is a finding about the exposure, not about the outcome: reaching the
descriptor ceiling is not a property of a bad run, it is a property of running
the plan at all.

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

---

## M3. `sqlite3_system_errno` separates three different bugs behind one string — ON THE MAC. **IT IS INERT IN THE APP.**

Measured 2026-08-24 against `/usr/lib/libsqlite3.dylib` (**SQLite 3.54.0**, the
same version the iOS SDK ships), via `ctypes`, on this box.

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

Repro: `artifacts/sqlite-errno-probe.py`.

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
  rather than as an errno, and now every denial in every gate log carries the
  field. Nobody has to re-derive this, and a future SDK that populates it will
  simply start showing the value.
