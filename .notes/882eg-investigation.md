# playhead-882eg — the fd FLOOR: what retains ~81 PlayheadRuntime graphs

Branch `bead/playhead-882eg`, off main `50c66760`.

## Inherited measurement (playhead-vk68m — NOT re-derived here)

Test-host descriptor FLOOR ~453, flat for the tail of every full-plan run
(447/453/453/455/459 over five preserved runs plus one live). 17.7 % of
`RLIMIT_NOFILE` soft = 2560. Tail dump by path, taken while the host was alive:

```
163  production .../Application Support/Playhead/AnalysisStore/analysis.sqlite  (81 db + 81 -wal + 1 -shm)
153  production .../Application Support/AdCatalog/ad_catalog.sqlite             (76 + 76 + 1)
 81  Library/Caches/Diagnostics/surface-status-<ts>-<uuid>.jsonl                (81 DISTINCT files)
 33  tmp/PlayheadTestScratch/PlayheadTestStore-<uuid>/                          (11 stores x 3.00)
  7  Documents/bg-task-log.jsonl                                               (ONE file, 7 fds)
 16  infrastructure
```

Those are **total** descriptor counts per path (the vk68m dump's own column),
not vnode counts. The 453 floor and the 2,539 peak both come from
`PROC_PIDLISTFDS` totals, so they are the same column.

## Static reading of the construction path (this bead, no run needed)

* `PlayheadRuntime.swift:1130` `init(isPreviewRuntime:)`.
* `:842` `let resolvedStore = try! AnalysisStore()` — **lazily opened**
  (playhead-6boz). `AnalysisStore.init` does no `sqlite3_open_v2`; the open runs
  inside `migrate()` / `ensureOpen()`.
* `:1153` `let surfaceStatusLogger = SurfaceStatusInvariantLogger()` — also
  lazy; `migrate()` resolves the directory + install-id salt, and the session
  file is opened by `ensureSessionFileLocked()` on the FIRST WRITE.
* `:1243-1246` `let adCatalogStore: AdCatalogStore?` — a **local**, not a
  stored property on the runtime. Nothing on `PlayheadRuntime` holds it; only
  whatever captured it does.
* `:2364` a fire-and-forget `Task { [weak self, analysisStore, ...,
  adCatalogStore, ..., surfaceStatusLogger, ...] in ... }` that runs the whole
  migrate chain (`analysisStore.migrate()`, `adCatalogStore.migrate()`,
  `surfaceStatusLogger.migrate()`, …). It is **not** assigned to any stored
  property, so `shutdown()` cannot cancel it.
* The Task is at init-body indentation, i.e. NOT inside any `if !isPreviewRuntime`
  guard — a preview runtime runs the same chain, which is why preview-runtime
  tests open the production `analysis.sqlite`.

## Test-side construction sites (measured with /usr/bin/grep, this branch)

| file | `PlayheadRuntime(` | `.shutdown()` | `withTestRuntime` |
|---|---|---|---|
| Design/NowPlayingBarHapticTests | 2 | 0 | 0 |
| App/ShowSkipModeControlTests | 13 | 0 | 0 |
| Views/NowPlaying/SkipModePillPresentationTests | 3 | 0 | 0 |
| Views/NowPlaying/NowPlayingViewModelTests | 13 | 0 | 0 |
| Views/NowPlaying/NowPlayingBarTests | 2 | 0 | 0 |
| Services/AdDetection/RuntimeShutdownLifecycleTests | 20 | 27 | 0 |
| Services/Diagnostics/PlayheadRuntimeLaunchPerfTests | 3 | 2 | 0 |
| Services/Diagnostics/MainActorFreedomTests | 2 | 3 | 0 |
| Services/CoreServiceTests | 1 (in a comment) | 0 | 5 |

So **33 sites never call `shutdown()`**, all of them `isPreviewRuntime: true`.

## The prior art that constrains the hypothesis

`RuntimeShutdownLifecycleTests.deinitReleasesRuntimeWithoutCycleWhenShutdownSkipped`
already asserts that a **non-preview** runtime dropped WITHOUT `shutdown()`
deallocates (event-driven on a `DeinitSentinel` associated object). That test is
in the plan and is not in the gate baseline, so "the runtime object is retained"
was NOT free to assume. The store objects are captured strongly by the deferred
Task independently of the runtime, so `store retained` and `runtime retained`
are two different claims and the fd dump cannot tell them apart.

## Probe (commit 35885ad9)

`PlayheadTests/Services/Diagnostics/RuntimeGraphRetentionProbeTests.swift`
separates them: weak refs to the runtime, its `AnalysisStore` and its
`SurfaceStatusInvariantLogger`, plus in-process `F_GETPATH` descriptor counts on
the three production paths, before and after the owning scope ends.

## M1 — WHAT IS RETAINED IS NOT THE RUNTIME (measured, probe2, 2026-08-25)

Scoped run, `-only-testing:PlayheadTests/RuntimeGraphRetentionProbeTests`,
`** TEST SUCCEEDED **`, 4 tests, 34.2 s. Log:
`/Users/dabrams/playhead-gate-artifacts/882eg/probe2.log`.

```
previewNoShutdown      runtime=released  analysisStore=RETAINED surfaceLogger=RETAINED workScheduler=RETAINED analysisCoordinator=RETAINED entitlementManager=released
previewWithShutdown    runtime=released  analysisStore=RETAINED surfaceLogger=RETAINED workScheduler=RETAINED analysisCoordinator=RETAINED entitlementManager=released
nonPreviewWithShutdown runtime=released  analysisStore=RETAINED surfaceLogger=RETAINED workScheduler=RETAINED analysisCoordinator=RETAINED entitlementManager=released
fivePreviewRuntimes    liveRuntimes=0/5  liveStores=5/5
```

Four things this settles, and the first one corrects the bead:

1. **`PlayheadRuntime` DEALLOCATES. Every time — preview or not, shutdown or
   not, 0 of 5 alive.** The bead's description says "~81 `PlayheadRuntime`
   object graphs are still alive 220 seconds after the last test finished" and
   that is the standing defect class one more time: the fd dump names FILES the
   runtime's init opened, and it was read as naming live runtimes. The existing
   rail `deinitReleasesRuntimeWithoutCycleWhenShutdownSkipped` was right all
   along.
2. **The stores outlive it, one per construction, exactly.** 5 runtimes → 5 live
   `AnalysisStore`s. That is the ~5-descriptors-per-runtime accumulation the
   bead measured, and it is a **store** lifetime, not a runtime lifetime.
3. **`shutdown()` DOES NOT HELP.** `previewWithShutdown` and
   `nonPreviewWithShutdown` are byte-identical to `previewNoShutdown`. So
   routing the 33 non-`shutdown()` test sites through `withTestRuntime` would,
   TODAY, have fixed **nothing** — the obvious candidate in the brief is
   necessary but not sufficient, and on its own it is not even necessary-first.
4. **The probe is not vacuous**: `entitlementManager` releases in all three,
   from the same measurement, in the same object graph.

## M2 — WHAT RETAINS THEM: a self-retaining perpetual loop, four of them

The shape, and it is the same shape four times:

```swift
someTask = Task { [weak self] in
    guard let self else { return }      // <- promotes to STRONG for the task's whole life
    await self.runLoop()                //    `while !Task.isCancelled { … }` — never returns
}
```

`[weak self]` is defeated by `guard let self`: the strong binding lives in the
task frame for the entire unbounded loop, so `owner -> Task -> owner` is a cycle
that only cancellation can break. A running `Task` is retained by the runtime
system whether or not anyone holds its handle, so dropping the handle does not
help either.

| # | owner / task | file:line | started from | reaches |
|---|---|---|---|---|
| 1 | `AnalysisWorkScheduler.schedulerTask` | `AnalysisWorkScheduler.swift:3389-3396`, loop at `:3624` | `PlayheadRuntime.swift:2957`, in the bootstrap Task — **no `isPreviewRuntime` guard**, so EVERY runtime | `store: AnalysisStore`, `jobRunner`, `downloadManager` |
| 2 | `AnalysisCoordinator.capabilityObserverTask` | `AnalysisCoordinator.swift:491-504` | `BackgroundProcessingService.start()` ← `PlayheadRuntime.swift:3344`, **below** the `guard !isPreviewRuntime` at `:3159` | `store`, `adDetectionService` → `adCatalogStore`, `skipOrchestrator` → `surfaceStatusLogger` |
| 3 | `BackgroundProcessingService.capabilityObserverTask` | `BackgroundProcessingService.swift:1178-1186` | same | `coordinator` → all of #2 |
| 4 | `EpisodeSummaryBackfillCoordinator.loopTask` | `EpisodeSummaryBackfillCoordinator.swift:290-295`, loop at `:381` | `PlayheadRuntime.swift:2526` (nil in preview) | `store: AnalysisStore` |

Each owner has a `stop()` that cancels its task (`:3549`, `:516`\*, `:1213`,
`:298`) and **not one of them has a single caller anywhere in `Playhead/`**.
`PlayheadRuntime.shutdown()` cancels six task handles and calls none of these
four.

\* `AnalysisCoordinator.stop()` is the exception that matters: it **deliberately
leaves `capabilityObserverTask` running**, documented at `:508-515`. So even
calling it would not close #2.

Three further Tasks in `init` have **no stored handle at all** and so cannot be
cancelled by anything: the bootstrap Task at `:2366` (strongly captures
`analysisStore`, `adCatalogStore`, `surfaceStatusLogger` for its whole
duration), the capability-subscription loop at `:2288` (a `for await` that never
ends, strongly capturing `capabilitiesService` — the sibling at `:2321` WAS
given a handle by playhead-43ed M1 and this one was missed), and the day-zero
kickoff installer at `:3251`.

Ruled OUT by search, so nobody re-treads them: no `static`/`.shared` collection
accumulates these objects; every `NotificationCenter.addObserver(forName:)`
block in `Playhead/` is `[weak self]` with a stored token; no
`objc_setAssociatedObject`, global `Timer`, `DispatchSourceTimer` or
`CADisplayLink` reaches them; `BGTaskScheduler` registrations are once-guarded
AND `[weak self]`. `DownloadManager._shared` is a single strong slot (its doc
comment claims "stored weakly" and is wrong) — that is **one** extra instance,
not 81.

`shutdown()` is called from **nowhere in `Playhead/`** — it is a test-only
teardown path today, which is what makes strengthening it safe.

## M3 — BEFORE, full plan, this branch (2026-08-25)

Artifacts: `/Users/dabrams/playhead-gate-artifacts/882eg/before2/`
(`fullplan.log`, `fd-series.jsonl`, `last.json`, `peak.json`, `watcher.log`).
Command: `scripts/gate-fd-paths.py --watch --interval 10 --deadline 90` started
before `scripts/fast-gate.sh`, both under `PLAYHEAD_SIM_ID` so the trim applies
(`simulator processes after trim: 97`).

Run health: **11,789 tests, 267.3 s, ONE test-host pid (9302), 0 restarts,
`** TEST FAILED **`, `GATE_EXIT=65`** — expected on a full plan. Baseline verdict
`RED (0 known / 1 NEW)`, 11 RESOURCE FAILURES (descriptor denials, vk68m's), 1
DID NOT RUN behind one of those. Peak fds **2,474 / 2,560 = 96.6 %**.

An earlier attempt (`before/`) is **VOID** and kept as evidence for a different
reason: it LOST ITS HOST (5693 → 6523, `Restarting after unexpected exit`,
12,214 started / 9,422 finished) at a peak of **2,460** descriptors — the
correlation playhead-vk68m named as a lead, observed again.

**The FLOOR is 499 TOTAL descriptors** (`PROC_PIDLISTFDS` totals — the same
column as the peak and as `testhost_fds`; not a vnode count), flat for the last
**six consecutive samples** (60 s) on a host that never restarted. Series tail:
`… 2161 2314 2454 1653 612 502 499 499 499 499 499 499`.

```
   179  production .../Application Support/Playhead/AnalysisStore/analysis.sqlite   (89 db + 89 -wal + 1 -shm, 3 distinct paths)
   168  production .../Application Support/AdCatalog/ad_catalog.sqlite              (84 + 83 + 1, 3 distinct paths)
    89  Library/Caches/Diagnostics/surface-status-*.jsonl                           (89 DISTINCT files)
     8  Documents/bg-task-log.jsonl                                                 (ONE file, 8 descriptors)
    39  tmp/PlayheadTestScratch/*                                                   (39 distinct)
     3  Library/Application Support/Playhead.store  (SwiftData, db + -wal + -shm)
    13  other / infrastructure
   ---
   499
```

**499 rather than the bead's 453, and the difference is the measurement's own
weight.** The bead read 81 runtimes; this reads **89**, and this branch adds
exactly **8 runtime constructions** — the probe suite's 1 + 1 + 1 + 5. So the
floor really is ~5 descriptors per constructed runtime, and the probe raised it
by the predicted amount. That is a stronger corroboration of the per-runtime
arithmetic than the original count was, and it is also the warning: **the probe
must not ship as-is.**

Peak dump, for the contrast that says which half is which: at 2,454 the same
host held **2,196 descriptors on 2,195 DISTINCT `PlayheadTestScratch` paths**
and only 51 + 43 on the two production stores. The PEAK is scratch-store
concurrency (playhead-vk68m's problem); the FLOOR is retained production stores
(this bead's).

Noted, not chased: the run reports one NEW failure,
`AnalyticsCounterStoreTests.sharedStoreIsTestIsolated` ("The shared store is
volatile under XCTest"), which asserts `UserDefaults.standard` has no
`playhead.analytics.aggregate.v1`. The simulator container is shared across
runs, so a single leftover write fails it permanently — the same shape as this
bead's second concern, one layer along. Whether this branch caused it is
answerable by whether it reproduces on the AFTER run; it is not in the committed
baseline and the baseline is not mine to touch.

## M4 — THE FOUR STOPS ARE NECESSARY AND NOT SUFFICIENT, and what that ruled out

Three scoped measurements after the shutdown fix landed, all
`** TEST SUCCEEDED **`:

| run | change under test | result |
|---|---|---|
| probe3 | `shutdown()` stops the four perpetual loops, joins the bootstrap chain, cancels the three handle-less init Tasks | **byte-identical** to before the fix |
| probe4 | probe widened from 6 to 22 boxed objects | **19 RETAINED**, 3 released |
| probe6 | + the `DownloadManager` ↔ `AnalysisWorkScheduler` cycle cut at teardown | **byte-identical again** |

```
previewNoShutdown      RETAINED[19]: analysisStore surfaceLogger workScheduler analysisCoordinator
                                     downloadManager bgProcessing jobRunner skipOrchestrator adDetection
                                     capabilities playbackService trustService surfaceObserver
                                     lanePreemption jobReconciler transcriptEngine speechService
                                     audioService silenceCompression
                       released[3]:  runtime entitlementManager storeRecovery
previewWithShutdown    ... identical ...
nonPreviewWithShutdown ... identical ...
standaloneServices     RETAINED[0]; released[6]: PlaybackService CapabilitiesService AnalysisAudioService
                                     SurfaceStatusInvariantLogger DownloadManager AnalysisStore
```

Four things this establishes, and they are the ones worth carrying forward:

1. **The released three are exactly the objects held by NOBODY BUT THE RUNTIME.**
   `entitlementManager` and `analysisStoreRecovery` release; every object that
   any other service also references does not. So the retention is a property of
   the SERVICE GRAPH, not of any one service.
2. **No service is an independent root.** All six constructed standalone release
   cleanly, including `DownloadManager`, which was the leading suspect (its
   background `URLSession`s are never invalidated). The immortality is created by
   the WIRING, not by any constructor.
3. **`analysisStoreRecovery` releasing proves the bootstrap Task completed** — it
   is in that Task's capture list and nothing else's. So the uncancellable
   bootstrap chain is not the holder either.
4. **There is exactly ONE stored-property cycle in the graph, and cutting it
   changes nothing.** Scanned mechanically across the 19 types with protocol
   conformances resolved (so `any DownloadProviding` counts as `DownloadManager`):
   the only cycle is `AnalysisWorkScheduler ↔ DownloadManager`, created for every
   runtime by `downloadManager.setAnalysisWorkScheduler(...)` against the
   scheduler's own `let downloadManager`. It is cut at teardown now and the
   retained set did not move by one name.

So the holder is a CLOSURE capture or a Task capture that a stored-property scan
cannot see, somewhere in ~2,500 lines of `init` wiring. **It is not identified,
and this bead stops looking**: each experiment costs a ~7-minute scoped gate, the
bead's harm is descriptors, and descriptors do not need the objects to die.

## THE FIX THAT FOLLOWS FROM THAT

`shutdown()` closes the three stores explicitly. A file handle belongs to
whoever opened it, and `shutdown()` is where that owner is finished with it — so
the descriptor is returned there rather than waiting on a deallocation that may
never come. Every close is IDEMPOTENT and NON-TERMINAL, which is the property
that makes it safe to do at a teardown without auditing every reader:

* `AnalysisStore.close()` resets `didOpen`, so the next SQL surface re-opens
  through `ensureOpen()` exactly as the first one did (rail:
  `analysisStoreReopensAfterClose`).
* `AdCatalogStore.close()` already existed for this exact reason.
* `SurfaceStatusInvariantLogger.close()` keeps `currentSessionFileURL` and
  `ensureSessionFileLocked` now REOPENS that file, so a later write appends to
  the same session rather than forking a second file under one `sessionId`
  (rail: `sessionLogReopensTheSameFile`).

The loop cancellations and the cycle cut are kept. They are correct on their own
terms — they stop runaway background work at teardown, which nothing did before —
and they are what makes the closed stores stay closed.

## M5 — AFTER, full plan, floor 499 → 214 (2026-08-25)

Artifacts: `/Users/dabrams/playhead-gate-artifacts/882eg/after/`. Same protocol
as the BEFORE: watcher started before the gate, `PLAYHEAD_SIM_ID` set so the trim
applies (`simulator processes after trim: 102`).

Run health: **11,789 tests, 264.4 s, ONE test-host pid (44432), 0 restarts,
`** TEST FAILED **`, `GATE_EXIT=65`.** Peak fds 2,462 / 2,560 (96.2 %). The test
population is byte-identical to the BEFORE run (11,789 both times), which is what
makes the two floors comparable.

**FLOOR 214 TOTAL descriptors** (same column as the BEFORE's 499 and as both
peaks), flat for the last **eight consecutive samples**:
`… 2063 2374 2146 852 226 214 214 214 214 214 214 214`.

| path family | BEFORE | AFTER | |
|---|---|---|---|
| production `analysis.sqlite` | **179** (89 db + 89 -wal + 1 -shm) | **84** (69 + 14 + 1) | −95 |
| production `ad_catalog.sqlite` | **168** (84 + 83 + 1) | **64** (59 + 4 + 1) | −104 |
| `Caches/Diagnostics/surface-status-*.jsonl` | **89** distinct files | **4** distinct files | **−85, 96 %** |
| `Documents/bg-task-log.jsonl` | 8 (one file) | 7 (one file) | −1 |
| `tmp/PlayheadTestScratch/*` | 39 | 39 | 0 |
| SwiftData `Playhead.store` | 3 | 3 | 0 |
| other / infrastructure | 13 | 13 | 0 |
| **total** | **499** | **214** | **−285, 57 %** |

`RLIMIT_NOFILE` soft is 2,560, so the floor falls from **19.5 %** of the budget
to **8.4 %**.

**WHAT STILL HOLDS THE REMAINDER, stated as what is measured and what is
inferred.** 148 of the 214 are still the two production SQLite files — 69
`analysis.sqlite` and 59 `ad_catalog.sqlite` connections. Measured: the
`surface-status` line, closed by the LAST statement in `shutdown()`, fell 89 → 4,
so `shutdown()` itself ran to completion for essentially every runtime and the
close path is reached. Two readings of the residual are consistent with that and
this bead does not separate them:

* the leaked service graph (playhead-panpc) touches a store after `shutdown()`
  closed it, and `close()` is deliberately NON-TERMINAL, so the touch REOPENS it;
* or some connections are not closed at all and the `-wal` counts (89 → 14,
  83 → 4) are an artefact of `F_GETPATH` on a WAL file that another connection's
  checkpoint-on-close has already unlinked.

The db/-wal asymmetry is the clue and it is recorded rather than resolved. A
terminal ("sealed") close would settle it and probably capture most of the
remaining 148, but it converts every use-after-teardown from a silent reopen into
a thrown error — a design decision with real reach, not a teardown fix.

**Two of this branch's own rails were RED on this run and are fixed** (see the
commit after this one): they called `awaitReady()` on the app's REAL
`analysis.sqlite` and came back `.migrationFailed("database is locked")` under
11,789 concurrent tests, while passing 4/4 scoped. That is playhead-vhffu biting
a rail written by the bead that filed it.

`AnalyticsCounterStoreTests.sharedStoreIsTestIsolated` failed on the BEFORE run
AND on the AFTER run, so it is not this branch's doing — the same conclusion the
BEFORE note said the AFTER run would settle.

## M6 — MUTATION LEDGER (FD series, `scripts/mutation-battery.sh`)

Seven invocations, each with its own unmutated baseline run as the vacuity
control. Logs: `/Users/dabrams/playhead-gate-artifacts/882eg/mut/`.

| mutant | PREDICTED victim set | OBSERVED | verdict |
|---|---|---|---|
| FD01 `await analysisStore.close()` deleted | `shutdown() closes …` alone | `shutdown() closes …` | KILLED, exact |
| FD02 `surfaceStatusLogger.close()` deleted | `shutdown() closes …` + `session log … SAME file` | both, and only those | KILLED, exact |
| FD03 `close()` frees the handle but keeps `didOpen` | `shutdown() closes …` + `analysis store … reopens` | both, and only those | KILLED, exact |
| FD04 logger stops reusing `currentSessionFileURL` | `session log … SAME file` ALONE | exactly that one | KILLED, exact |
| FD05 allowlist entry naming a file that constructs nothing | the canary | the canary | KILLED, exact |
| FD06 v1 — the allowlist's shutdown CHECK deleted | the canary | **nothing** | **SURVIVED** |
| FD06 v2 — an allowlisted FILE made non-compliant | the canary | the canary | KILLED, exact |

**Vacuity control: `baseline green` on all seven runs**, i.e. the focused suites
pass on unmutated sources every time, so no KILL is a pre-existing red. No
mutation killed a test other than the one predicted, so there is no false credit
in this ledger. Tree byte-exactly restored after every run (`git status
--porcelain` clean).

**FD06's survivor was correct and is worth keeping in the record.** The first
edit deleted the canary's own "an allowlisted file must contain `.shutdown()`"
check, so the offender list was never populated and the assertion passed.
Nothing anywhere asserts that a particular assertion EXISTS, so that mutation can
only ever survive — scoring it as a coverage hole would have fabricated one. The
edit was re-aimed at the defect the entry names (an allowlisted file that
constructs a runtime and never tears it down), which is the one rewrite this
script's header permits. FD04 is the mirror of the same discipline in the other
direction: it changes NO descriptor accounting at all, and it must therefore kill
the same-file rail and nothing else — which is exactly what it did.
