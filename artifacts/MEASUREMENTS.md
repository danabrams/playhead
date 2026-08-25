# playhead-vk68m — measurement log

Written as each measurement lands, because an agent's context is not storage.

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

## M3. `sqlite3_system_errno` separates three different bugs behind one string

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
