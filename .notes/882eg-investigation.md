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
