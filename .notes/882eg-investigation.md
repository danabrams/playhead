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
