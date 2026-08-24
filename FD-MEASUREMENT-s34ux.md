# playhead-s34ux: fd exhaustion — REFUTED, then CONFIRMED by the same run's own log

> **THE HEADLINE BELOW WAS WRONG AND IS CORRECTED AT THE BOTTOM OF THIS FILE.**
> The measurement was right; the DENOMINATOR was not. `kern.maxfilesperproc`
> (61,440) is not the limit that binds — the test host's own `RLIMIT_NOFILE`
> **soft limit is 2,560**, which the same run printed on its own line, and the
> peak of 2,539 is **99.2 % of it, not 4.1 %**. Read
> [the correction](#the-correction-fd-exhaustion-is-confirmed) first; everything
> above it is kept verbatim as the record of how the reading went wrong.

Measured 2026-08-24 by the orchestrator (the implementer agent was killed by
five consecutive transient API 529s). Full-plan `scripts/fast-gate.sh` on
`bead/playhead-s34ux` @ e77f42ac, using the fd probe committed in that commit.

Raw series: `~/playhead-gate-artifacts/fd-series-s34ux.csv` (**49 samples**, 10 s interval)
Full log:   `~/playhead-gate-artifacts/gate-s34ux-fdmeasure.log` (14.7 MB)

## The run

| | |
|---|---|
| tests started | 11,785 |
| verdict | `RED (1 known / 56 NEW)` |
| distinct Swift Testing failures | 58 |
| XCTest failures | **0** |
| host restarts | **0** |
| peak demand | 12.9 GiB of 16.0 GiB |
| swap peak | 0.9 GiB |

**Every one of the 63 recorded errors in THIS run was the same failure**, and in
this run not one was a behavioural assertion:

    44 x Caught error: Migration failed: unable to open database file
    19 x Caught error: SQLite open failed (14): unable to open database file

By SQL statement: `PRAGMA journal_mode = WAL` x30, `BEGIN IMMEDIATE` x14.

## The measurement — fd exhaustion is NOT supported

| quantity | peak observed | limit | headroom |
|---|---|---|---|
| test-host open fds | **2,539** (2,536 vnode, 2 socket, 1 kqueue) | `kern.maxfilesperproc` 61,440 | **96 %** |
| system-wide open files | 7,791 | `kern.maxfiles` 122,880 | 94 % |
| disk free (min during run) | 25,302 MiB = **24.7 GiB** | 13.5 GiB preflight | comfortable |

The fd count climbs 3 -> 2,539 across the test phase and falls to 453 after it.
That was read as a load curve rather than an exhaustion cliff, peaking at
**4.1 %** of the per-process ceiling.

**Two things in that sentence are wrong and are corrected below:** the climb is
NOT monotonic (the series has seven descents), and 4.1 % is against the wrong
denominator. Post-correction the same shape reads the other way round — 2,539 ->
966 in ten seconds is a COLLAPSE after the ceiling was hit, which is what a
descriptor table does when the tests holding it finish.

**A caveat was stated here and it was FALSE, and its falseness is the same
defect one layer along.** It read: *"the probe samples every 10 s, so a
sub-10-second spike between samples is not ruled out ... the honest claim is
'not supported at 10 s resolution'."* There was a **2-second in-process probe
running in the same run, writing to the same log**, and it is the instrument
that caught `maxfd = 2559`. The document stated a resolution limit that a better
instrument in the same evidence had already beaten. Same shape as the
denominator error: reach for the figure that is easy to quote instead of the one
that settles it, and never check whether the evidence already answers it.

## Two measurement traps found while building the probe (both the standing defect class)

* **`lsof -p PID | wc -l` IS NOT the open-fd count.** It also lists `cwd`, `rtd`,
  `txt` and every mapped dylib. Measured: a process holding 3 descriptors printed
  16 lines. This over-reports in exactly the direction that manufactures a false
  "exhausted" on a test host with hundreds of mapped images. **This is the recipe
  the bead itself proposed.**
* **`proc_pidinfo(PROC_PIDLISTFDS)` with a NULL buffer returns the table's
  CAPACITY, not the live count.** Measured: 3 fds -> 45; open 100 -> 148; close
  all 100 -> **still 148**. A high-water allocation that never falls; read as
  "open fds" it reports a leak on a process that has none.

So both obvious ways to take this measurement give a confident wrong answer, one
too high and one that never comes back down.

## The suites rotate again — third independent observation

| run | tree | NEW | top failing suites |
|---|---|---|---|
| 2026-08-23 | `7dgx` branch | 7 | AdWindowIngestAudit, AdPodContinuationDayZeroSeed, SupportLineLocalisation |
| 2026-08-23 | **main, diff absent** | 8 | ManualVetoReachesPersistedAnalysis x4, AdWindowIngestAudit x3, BackgroundProcessingService |
| 2026-08-24 | `s34ux` @ e77f42ac | 56 | SuggestBannerEntryGate 11, UnresolvedShowIdentity 9, AnalysisWorkSchedulerLaneGate 8, AnalysisPipelineStallRegression **7** |

**EXACTLY disjoint each time** — re-derived at audit, the three NEW sets share
**zero** names across all three pairs, so "near-disjoint" understated it. The
COUNT is not stable either: 7, 8, 56.

Two corrections to that table's last row, both found by counting rather than
skimming: `AnalysisPipelineStallRegression` is **7** NEW failures, not 9 (9 is
its ISSUE-LINE count — a parameterised test records one line per argument set),
and the row was ordered as though 9 outranked 8.

## THE NEXT HYPOTHESIS, stated as a lead and NOT as a finding

`SQLITE_CANTOPEN` on `PRAGMA journal_mode = WAL` implicates the **containing
directory**, not the file: WAL must create `-wal` and `-shm` siblings, so a
directory that has been removed produces exactly this error. Same for
`BEGIN IMMEDIATE`.

`PlayheadTests/Helpers/TestScratch.swift` contains a **mid-run deleter**.
`TestScratchReaper` exists (playhead-cgka) to bound peak disk DURING the run,
and reclaims a directory once its owning object is deallocated, deferred by one
sweep. Its stated premise is:

> `AnalysisStore.deinit` closes its SQLite handle, so an owner that is gone is a
> database that is closed.

**That premise is about the FIRST store. It says nothing about a SECOND store
opened against the same directory** — which is precisely what a
persistence-across-launch test does: build store A, release it, open store B on
the same path. If a sweep lands in that window, B's open fails with CANTOPEN on
`PRAGMA journal_mode = WAL`. It would rotate across whichever suites happen to be
in that window, and it would not reproduce scoped, where sweep timing differs.

NOT MEASURED. The check is cheap: log every reap with its URL and timestamp,
log every store open with its directory, and look for a reap preceding a failed
open on the same path.

## The `TestScratchReaper` lead is REFUTED, statically, and no run was needed

The lead above says a mid-run reap could remove a store's directory and produce
`CANTOPEN` on the next open. Read against the code and against WHERE the
failures land, it cannot be what happened here.

Three facts, each checkable without a build:

1. **`sweep()` never touches an UNOWNED entry.** Its first line inside the loop
   is `guard entry.isOwned else { kept.append(entry); continue }`
   (`PlayheadTests/Helpers/TestScratch.swift`). `register(_:)` — the path
   `makeTempDir` takes when no `ownedBy:` is passed — appends
   `Entry(isOwned: false)`. So a directory with no owner is only ever reclaimed
   at process exit, which the file's own "honest limit" section states.

2. **Every failing open happens while its directory is still UNOWNED.**
   `makeTestStoreWithDirectory()` runs `makeTempDir(prefix:)` (no owner),
   then `AnalysisStore(directory:)`, then `migrate()`, and only then
   `TestScratchReaper.shared.adopt(dir, owner: store)` — deliberately, so a
   throw leaves the directory unowned. All 63 recorded errors are thrown from
   `sqlite3_open_v2` or from the first migration statements
   (`PRAGMA journal_mode = WAL`, `BEGIN IMMEDIATE`), i.e. strictly BEFORE the
   `adopt` that would make the directory reapable at all.

3. **The failing directory is at most milliseconds old.** `makeTempDir` mints a
   fresh `UUID`-named directory and `createDirectory` succeeds (it throws
   otherwise, which is a different error and is not what any of the 63 carry).

So the reap window the lead describes does not exist for the observed
population. It remains a real hazard for the shape the lead names — a SECOND
store opened on a directory whose FIRST store has been released without
re-adopting — but that is not this, and none of the 11 failing suites reopens a
store on an existing directory.

**Do not read this as "the reaper is fine".** It is "the reaper cannot explain
these 63". The refutation is about the ORDER of the operations, not about the
reaper's own correctness.

## What the evidence still says, and what it does not

The one witness no database explanation covers is still the strongest thing on
the table, and it is worth restating precisely because it is easy to file under
SQLite:

    ✘ Test "every SemanticSweepMarkComposer.compose call site passes supportLines"
      Caught error: NSCocoaErrorDomain Code=256 "The file "AdDetectionService.swift"
      couldn't be opened."
      NSUnderlyingError = NSPOSIXErrorDomain Code=9 "Bad file descriptor"

**`EBADF` (9) is not `EMFILE` (24).** Exhaustion fails an `open()` and reports
"Too many open files"; `EBADF` is an operation on a descriptor that WAS obtained
and then became invalid. That is consistent with a descriptor being closed by
someone who did not own it, and it is NOT consistent with the ceiling being
touched — which is a second, independent reason the fd-exhaustion framing was
the wrong one, arrived at from the error text rather than from the series.

Two candidate mechanisms remain, and neither is measured:

* **An over-close somewhere in the process** (a descriptor closed twice, its
  number reassigned to another test's file). This would produce `EBADF` in one
  victim and `CANTOPEN` in another, would rotate across unrelated suites, and
  would not reproduce scoped — every observed property. Audited by hand:
  `PipelineDumpLiveTests.openDirectoryDescriptor` /
  `openRegularDescriptor`, `CorpusAnnotationTests`, `CorpusAnnotationLoader`,
  `ChapterPlanQualityEval` — all of the raw `Darwin.close` sites read as
  correct (guard-close-then-throw, or `defer`-close, never both on one
  descriptor). So the audit did not find it; that is not the same as it not
  being there, and the audit covered only raw `Darwin.close`, not
  `FileHandle(fileDescriptor:closeOnDealloc:)` or anything in a dependency.

* **A sub-10-second exhaustion spike** the 10 s sampler cannot see. The
  monotonic shape argues against it and the `EBADF` spelling argues against it
  harder, but neither rules it out.

**THE INSTRUMENT THAT WOULD SETTLE IT IS `sqlite3_system_errno`.** SQLite keeps
the underlying `errno` from the failed open and `AnalysisStore` throws away —
`AnalysisStoreError.openFailed` carries `sqlite3_errmsg`, which is the prose
`unable to open database file` and names no cause. `EMFILE` / `ENFILE` /
`ENOENT` / `EACCES` / `EBADF` are five different bugs behind one string, and the
call that distinguishes them is one line. Filed rather than taken here: it is a
production change on an error path, and this bead's remaining budget is owed to
making the gate honest.

## Where the rest of this went

* **`playhead-vk68m`** — the root cause, still open. fd exhaustion refuted;
  EBADF-not-EMFILE is the live lead; `TestScratchReaper` ruled out statically.
* **`playhead-enzva`** — `AnalysisStore` throws `sqlite3_system_errno()` away,
  which is why five different bugs share one string and why the classifier has
  to match that one phrase as prose rather than as an errno.
* **`playhead-5c006`** — mutant `R21` survives, and survives on `105efd5f` too.
  Pre-existing coverage hole in the gate's own rails, filed not fixed.


---

## THE CORRECTION: fd exhaustion is CONFIRMED

Found 2026-08-24 by grepping this bead's own run log for the probe's first line.
It had been there all along.

    [s34ux-fd] pid=10985 getrlimit_rc=0 RLIMIT_NOFILE soft=2560
               hard=9223372036854775807 scanCap=70000

**`RLIMIT_NOFILE` soft = 2,560.** That is the number that binds, and it is what
the bead brief asked for in the same breath as the sysctl: *"also check the
host's own soft/hard `RLIMIT_NOFILE`, which is what actually binds and is
usually far lower than the sysctl."* It is 24× lower.

| | peak | limit | share |
|---|---|---|---|
| the reading above | 2,539 | `kern.maxfilesperproc` 61,440 | 4.1 % — "vast headroom" |
| **the reading that binds** | **2,539** | **`RLIMIT_NOFILE` soft 2,560** | **99.2 % — at the ceiling** |

Same numerator. Two denominators. Opposite conclusions. This is the standing
defect class — a value that names one thing read as though it named another —
committed by the instrument built to catch it, for the second time in this
bead's own history.

### The saturation is not inferred from the count. It is stated by the fd NUMBER.

The probe records the highest descriptor the process was ever handed:

    maxfd = 2559 = soft - 1

`open()` returns the **lowest available** descriptor. To be handed 2559, every
descriptor from 0 to 2558 must have been in use at that instant — the table was
**completely full**, and the next `open()` had nothing left to return but
`EMFILE`. That is a stronger claim than any count, and it does not depend on the
count being accurate.

**Which matters, because the count is NOT accurate here and says so.** The
in-process probe's own peak count is 2,079 while its `maxfd` is 2,559: the scan
walks fd 0 → 70,000 with `fcntl(F_GETFD)` and is not atomic, so ~480 descriptors
were closed by other threads while it was still walking. A count taken over a
churning table under-reads it. `maxfd` is the durable witness; the count is a
lower bound.

### The timing, from two independent instruments

| | |
|---|---|
| external sampler peak (`proc_pidinfo`, 10 s) | **02:43:17** — fds 2,539, maxfd 2,559, elapsed 251.6 s |
| in-process probe (`fcntl` scan, 2 s) | **t = 71 s** — fds 2,079, **maxfd 2,559** |
| **the `CANTOPEN` burst** | **02:43:20 – 02:43:24** — all 63 denials, a 4-second window in a 7.5-minute run |
| next external sample | 02:43:27 — fds **966**, i.e. −62 % in ten seconds |

The two clocks reconcile: the sampler reads elapsed 251.6 s at 02:43:17 and the
probe reads t = 71 s at the same saturation, which puts the test phase's start
~180 s into the run — consistent with the build. **Both instruments put the
table's saturation in the three seconds before the denials begin.**

### What this does and does not establish

- **Establishes:** the test host really does exhaust its descriptor table, and
  `SQLITE_CANTOPEN` and the `EBADF` source-file read are what that looks like
  from above. The `TestScratchReaper` refutation above still stands and is now
  simply beside the point.
- **Does NOT establish:** *what holds 2,560 descriptors.* Swift Testing starts
  essentially every test at once (CLAUDE.md: 11,000–11,400 in flight) and a
  WAL-mode SQLite store costs three descriptors — db, `-wal`, `-shm`. 11,000
  tests cannot share 2,560 descriptors under any allocation, so the population
  is bounded only by how fast stores are RELEASED. Whether that is a leak or
  simply the shape of the plan is the open question, and it is
  **`playhead-vk68m`**.
- **RAISING THE LIMIT IS NOT A FIX** and must not be presented as one. It is
  recorded here only because the number is now known: soft 2,560, hard
  unlimited, so the ceiling is a soft limit the harness could raise. That would
  move the cliff, not remove it.

### Why the first reading survived scrutiny for a whole day

Because every quantity in it was correct. The series was real, the sampling was
sound, both measurement traps it documents are genuine, and the conclusion was
still wrong — the error was one denominator, chosen by reaching for the number
that was easy to read from outside the process instead of the number that binds
inside it. The line that settles it was printed by this bead's own probe, in the
log the whole time, and no one grepped for it.

## What holds them: arithmetic, not a leak

| | |
|---|---|
| descriptors per open WAL store | **3.00**, measured — 20 stores in one process took it from 4 fds to 64, and every one came back on close |
| store-creating call sites in `PlayheadTests` | **2,799** across 322 files (2 further mentions are in comments) |
| demand if all are in flight at once — an UPPER BOUND, not the operative figure | **~8,397** |
| `RLIMIT_NOFILE` soft | **2,560** |
| | **3.28× over** — see the correction below: the measured concurrency is ~695, not 2,799, and 695/702 is the figure that predicts the run actually observed |

**Say what that numerator is and is not.** It counts CALL SITES, not tests: some
tests open more than one store, and some call sites live in helpers several
tests share. So 8,397 is the demand if every store-opening path were in flight
at once, not a per-test figure. Swift Testing does start essentially every test
at once — 11,000–11,400 in flight against 11,794 `@Test` annotations — so the
shape holds even though the exact number does not.

**The plan needs ~3.3× more descriptors than the process is allowed, and
completes at all only because stores are released faster than new ones open.**
Every run is therefore a race, and that is the whole explanation:

- the victims **rotate** — whoever is opening when the table fills is denied;
- the count is unstable (7, 8, 56 across three runs);
- it **never reproduces scoped**, where a few dozen stores are in flight;
- the failure is one sharp burst — all 63 denials inside four seconds of a
  7.5-minute run — not a steady rate.

There is no leaked handle to find. `AnalysisStore.deinit` closes correctly and a
closed store returns all three descriptors.

**The four options and their costs are on `playhead-vk68m`, and choosing between
them is Dan's call** — it is "how every test opens its store" and "bounding
Swift Testing's concurrency", which the bead brief reserves explicitly. In
short: `parallelizable: false` is the only real fix and its **5.08×** cost is
already measured (playhead-blsh); raising the soft limit is a **mitigation that
moves the cliff**, recorded because the number is now known, and deliberately
not presented as a fix.

## The EBADF witness was evidence FOR exhaustion, not against it

The correction above still left one argument standing against exhaustion, and it
was mine, and it was wrong. It read:

> **`EBADF` (9) is not `EMFILE` (24).** Exhaustion fails an `open()` and reports
> "Too many open files"; EBADF is an operation on a descriptor that WAS obtained
> and then became invalid.

**That is what POSIX documents. It is not what this box does.** Measured with a
15-line C program — no Python, no ctypes, no Foundation, `errno` read on the
line after the failing call:

```
inherited soft=1048576 hard=9223372036854775807
opened 61, highest fd = 63 (soft-1 = 63)
failing open: errno = 9 (Bad file descriptor)   [EMFILE=24 EBADF=9]
```

Lower `RLIMIT_NOFILE` to 64, open until it refuses, and the refusing `open(2)`
sets **errno 9**. Not 24. Reproduced identically through three independent
paths: Python's `os.open`, a raw `libc.open` through ctypes, and the C program
above.

So `NSPOSIXErrorDomain Code=9 "Bad file descriptor"` on the
`AdDetectionService.swift` read — the witness this bead was handed as the thing
"no database explanation covers" — **is exactly what a process at its descriptor
ceiling produces on this platform.** It was never evidence of an over-close. It
was the ceiling, reported in the spelling this kernel uses.

Two further consequences worth stating plainly:

- **The hand-audit of every raw `Darwin.close` site was looking for something
  that was never there.** It came back clean because it was clean.
- **The classifier's errno table is right for the wrong reason.** `EBADF (9)`
  was admitted because of the observed witness, on the theory that it meant a
  descriptor closed underneath a caller. It turns out to be *the* exhaustion
  errno here, and `EMFILE (24)` may never appear on this box at all. The table
  is unchanged and correct; the reasoning recorded beside it was not.

**The method failure, since it is the third instance in one bead.** I reasoned
from the errno POSIX documents instead of the errno this kernel returns —
a value that names one thing read as though it named another, one layer below
`kern.maxfilesperproc` vs `RLIMIT_NOFILE`. Both were settled in under a minute
by an experiment, and both had gone unmeasured because the documented answer was
easier to reach for than the measured one.

## A named limit of the classifier, created by the confirmed diagnosis

**L-1: a genuine over-close in PRODUCT code would now be classified as the box.**
`EBADF (9)` routes to the RESOURCE category, and on this platform that errno is
what a full descriptor table produces — which is why it is in the table. But it
is also what a real double-close produces, and the classifier cannot tell them
apart from the message alone.

The trade is deliberate and it is not close. Treating `EBADF` as a failure puts
~60 healthy tests back in the NEW column on every affected run, which is the
defect this whole change exists to remove; and a descriptor closed twice in
product code is not a thing a unit test asserts on, so the population that would
be lost is close to empty. But it is a hole in the loud direction and it should
be stated rather than discovered.

**What would close it** is `playhead-enzva` — capturing `sqlite3_system_errno`
— plus the same idea one layer out: an error that carries the *count of open
descriptors at the moment it was raised* is self-classifying, and the process
can read its own count in microseconds. Nobody has built that and it is not in
this bead.


---

# Corrections applied after audit, each with its witness

An independent audit re-derived every number in this file. Nine were wrong.
They are corrected in place above **and listed here**, because this bead's whole
subject is a gate that told a reader something untrue, and a document that
quietly fixes itself is the same failure in a smaller font.

| # | claim as published | corrected | witness |
|---|---|---|---|
| 1 | rails "44 new, 32 fail against the shipped parser, 8 guards" | **70 new (323 total), 55 fail, 15 guards** | pre-bead `gate_baseline.py` restored into a scratch `scripts/`: `Ran 323`, 32 errors + 23 failures. `32 + 8` never equalled 44 either — the counts had grown twice under a sentence nobody re-derived |
| 2 | the run's verdict `RED (0 known / 56 NEW)` | **`RED (1 known / 56 NEW)`** | the run's own line in `gate-s34ux-fdmeasure.log` |
| 3 | disk min "25.3 GiB" | **24.7 GiB** | 25,302 MiB ÷ **1024**. It had been divided by 1000 |
| 4 | "climbs **monotonically**" | **2 descents in the climb**, both tiny (25→23, 1783→1779) | recomputed off the CSV — **and my first correction of this said 7, which was also wrong**: it counted the `-1` sentinel rows written before the test host existed as though they were readings. A sentinel is not a measurement. Over the 46 valid samples there are 6 descents, 4 of which are the post-peak collapse. The auditor's "two dips" was right and my correction over-shot it |
| 5 | "a load curve, not an exhaustion cliff" | a **collapse after the ceiling** — 2,539 → 966 in ten seconds | the same CSV, read with the right denominator |
| 6 | "50 rows" | **49 samples** | `csv.DictReader` over the file: a header line is not a sample |
| 7 | "not one behavioural assertion" (of the population) | true of the s34ux run; **false of the main run**, which had exactly one | `Expectation failed: task.completedSuccess == false`, and it is the very failure the `0 known / 1 NEW` demonstration turns on — the sentence contradicted the evidence two sections below it |
| 8 | "`AnalysisPipelineStallRegression` 9" | **7** NEW failures | 9 is the issue-LINE count; a parameterised test records one per argument set |
| 9 | mutants "RD01–RD21" | **not contiguous**: no RD04, plus RD10b and RD10c — **22** mutants | `grep -o '"RD[0-9a-z]*"'` over the battery |

And two corrections in the other direction, where the audit found the argument
weaker than it needed to be:

- **Prefer the external 2,539 over the lowest-available argument.** That figure
  is a kernel-side `PROC_PIDLISTFDS` snapshot, so **99.18 % needs no inference
  at all** — 21 descriptors of headroom, read directly. The `maxfd = soft − 1`
  reasoning is still true and still worth keeping as the independent second
  witness, but it is no longer load-bearing.
- **The timing is tighter than claimed.** All 63 denials fall in
  **02:43:20–02:43:23**, and the `maxfd = 2559` sample is at **02:43:20 — the
  same second the burst starts**, not somewhere inside a 10-second gap.

## The caveat this file owes, and did not state

**There is no `EMFILE` anywhere in the log** — no "Too many open files", no
`Code=24`, no `Code=23`. The exhaustion is established by ARITHMETIC (2,539
against a 2,560 soft limit, `maxfd` at 2,559) and by the platform measurement
that `open(2)` reports errno **9** at this ceiling. It is not read off an errno
that says "too many open files", because this kernel never emits one here. State
that plainly, or the next reader refutes the whole file with one `grep`.

## The class these corrections belong to

Seven of the nine are one habit: **quoting a figure that was easy to reach for
without asking what it is a count OF**. `kern.maxfilesperproc` instead of the
limit that binds. Issue lines instead of failures. Rows instead of samples. MiB
÷ 1000. A rail count from two edits ago. And the 10-second caveat — a stated
limit of one instrument while a better instrument in the same run had already
beaten it — which is the same move applied to the evidence rather than to a
number. **Name the numerator and the denominator, then ask whether something in
the same evidence already answers the question better.**


## The correction of correction #4, and the concurrency number it produced

**I got #4 wrong while correcting it, in the same way the original was wrong.**
The published text said the climb was monotonic. My correction said "7
descents". Both are wrong, and mine is the worse error because it was made
while looking straight at the defect class: **the series has 49 rows and only 46
readings** — the first three are `-1`, the sentinel this bead's own sampler
writes when no test host exists yet, precisely so a failed read can never be
mistaken for a measurement. I counted the sentinels as readings. A value that
names an absence, read as a value.

Recomputed over the 46 real samples:

| | |
|---|---|
| descents in the CLIMB, up to the peak | **2**, both trivial: 25→23 and 1783→1779 |
| descents after the peak | 4 — `2539 → 966 → 483 → 472 → 453`, then flat for 22 samples |

So the honest sentence is: **the climb is monotonic apart from two dips of two
and four descriptors, and the fall after the peak is a collapse, not a curve.**
The auditor said "two dips" and was right; I over-corrected them to seven.

### What the series is actually made of, which is better than the call-site arithmetic

The auditor asked, correctly, whether "2,799 call sites × 3.00" is a fair
characterisation, since it counts CALL SITES and the argument wants CONCURRENT
HOLDERS. The series answers it directly, and this supersedes the call-site
estimate:

| | |
|---|---|
| peak, by kind | **2,536 vnode**, 2 socket, 1 kqueue — the pressure is essentially all FILES |
| floor after the test phase | 453, flat for 22 consecutive samples |
| descriptors above that floor at peak | **2,086** ≈ **695 concurrent WAL stores' worth** |
| what the table admits before it is full | `(2560 − 453) / 3` ≈ **702 concurrent stores** |

**695 against a ceiling of 702.** The run did not approach the limit — it
arrived at it, with seven stores of headroom, which is why the denials are a
four-second burst rather than a rate.

And the ramp shows the shape: 30 → 127 → 328 → 956 → 1,467 → 1,783 in eighty
seconds, a plateau around 1,780–1,875 for a minute, then 2,204 → 2,219 → 2,539
and the burst. The plateau is the plan holding roughly 440 stores steadily; the
final climb is the tail of store-opening tests arriving on top of it.

**Quote 695/702 rather than 8,397/2,560.** Both say the same thing, but the
first is measured concurrency and the second is an upper bound assembled from a
count of source lines.

## A separate finding the series contains and nobody had read: ~429 descriptors are never given back

Everything above treats the peak as transient — stores opened, used, released.
Most of it is. But the series also shows a floor that moves and does not come
back, on the **same process** (`testhost_pid` is `10985` for all 46 samples, so
this is not a restarted host):

| | vnodes open |
|---|---|
| before the ramp, host alive, test phase not yet loaded (02:40:36) | **27** |
| after the test phase, flat for 22 consecutive samples over 220 s | **449** |

**429 file descriptors are acquired during the run and never released** — about
**16.8 % of the 2,560 budget, permanently**, before a single store-opening test
in the next plan has run. At 3.00 descriptors per WAL store that would be ~143
stores that were never closed, though they need not be stores at all.

**Stated as an observation, because the cause is NOT established:**

- the tail is sampled while xcodebuild is still collecting results, so the host
  is idle but not shut down, and some of those files may belong to that;
- the sampler records descriptors by KIND, not by PATH, so nothing here names
  a single one of the 449;
- 429 does not explain the peak. The peak is `2,539 − 453 = 2,086` of transient
  store pressure on top of this floor. This is a **sixth of the table gone
  before the race starts**, not the race itself.

**The one-line diagnostic that would settle it** is `lsof -p <testhost_pid>`
taken at the tail, which names every one of the 449. It is not in this bead
because the bead's remaining budget is owed to the gate's honesty, and it is
recorded on `playhead-vk68m` as the concrete next step — it is the cheapest
unexplored lead in the whole picture, and unlike the rest of it, it may well be
a genuine leak with a genuine fix.
