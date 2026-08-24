# playhead-s34ux: the fd-exhaustion hypothesis is REFUTED by measurement

Measured 2026-08-24 by the orchestrator (the implementer agent was killed by
five consecutive transient API 529s). Full-plan `scripts/fast-gate.sh` on
`bead/playhead-s34ux` @ e77f42ac, using the fd probe committed in that commit.

Raw series: `~/playhead-gate-artifacts/fd-series-s34ux.csv` (50 rows, 10 s interval)
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

**Every one of the 63 recorded errors was the same failure**, and not one was a
behavioural assertion:

    44 x Caught error: Migration failed: unable to open database file
    19 x Caught error: SQLite open failed (14): unable to open database file

By SQL statement: `PRAGMA journal_mode = WAL` x30, `BEGIN IMMEDIATE` x14.

## The measurement — fd exhaustion is NOT supported

| quantity | peak observed | limit | headroom |
|---|---|---|---|
| test-host open fds | **2,539** (2,536 vnode, 2 socket, 1 kqueue) | `kern.maxfilesperproc` 61,440 | **96 %** |
| system-wide open files | 7,791 | `kern.maxfiles` 122,880 | 94 % |
| disk free (min during run) | 25,302 MiB | 13.5 GiB preflight | comfortable |

The fd count climbs monotonically 3 -> 2,539 across the test phase and falls to
453 after it. That is a load curve, not an exhaustion cliff, and it peaks at
**4.1 %** of the per-process ceiling.

**Stated caveat, because it is the one thing this cannot exclude:** the probe
samples every 10 s, so a sub-10-second spike between samples is not ruled out.
The shape argues against it — a smooth monotonic climb, no sawtooth — but the
honest claim is "not supported at 10 s resolution", not "impossible".

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
| 2026-08-24 | `s34ux` @ e77f42ac | 56 | SuggestBannerEntryGate 11, UnresolvedShowIdentity 9, AnalysisPipelineStallRegression 9, AnalysisWorkSchedulerLaneGate 8 |

Near-disjoint each time. Note also the COUNT is not stable: 7, 8, 56.

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
