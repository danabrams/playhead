# playhead-hx6n — `semantic_scan_results` gains its attribution

**Status:** investigation notes + design, written before the code. Updated as the
change landed.

## The gap, restated from the code

`semantic_scan_results` persists **22 columns** and not one of them says *when*,
*under what*, or *in which scene phase* a row was written. The canonical list is
`AnalysisStore.semanticScanResultColumns`:

```
id, analysisAssetId, windowFirstAtomOrdinal, windowLastAtomOrdinal,
windowStartTime, windowEndTime, scanPass, transcriptQuality,
disposition, spansJSON, status, attemptCount, errorContext,
inputTokenCount, outputTokenCount, latencyMs, prewarmHit,
scanCohortJSON, transcriptVersion, reuseKeyHash, runMode, jobPhase
```

`latencyMs` is a *duration*; nothing anchors it to a clock. `runMode`
(shadow/targeted) and `jobPhase` (harvester/lexical/audit/…) describe the KIND of
work, never the RUN it belonged to nor the app state it ran under.

`background_task_runs` (schema v24) does carry `scenePhase`, `startedAt` and
`finishedAt` — but its primary key is `runId`, a per-BGTask UUID that appears
nowhere in `semantic_scan_results`. There is no join.

### The finding that made this cheap

Every one of the **seven** `SemanticScanResult` factories in `BackfillJobRunner`
**already takes `jobId: String`** — counted, not estimated; an earlier draft of
this note said four and missed `makeRefinementFailureScanResult` and the two
`makeCoarseFailureScanResult` overloads:

* `makeNoWorkSentinelScanResult`
* `makeScanResult`
* `makeRefinementScanResult`
* `makeFailureScanResult`
* `makeRefinementFailureScanResult`
* `makeCoarseFailureScanResult` (both overloads)

and every one of them already passes it — as `reuseScope: jobId`, which feeds
`semanticScanReuseKeyHash` and is then **discarded**. The join key was in hand at
every single write site and was being hashed into an opaque digest instead of
stored. So the correlation id costs no new plumbing: it is the value that was
already there.

`reuseScope` is not the only in-memory-only field on the struct —
`refusalExplanation`, `usedPermissiveFallback` and `permissiveFallbackReason` are
also carried and never persisted. That is a separate gap; it is **not** in scope
here.

## What V42 adds

Three nullable columns on `semantic_scan_results`:

| column | type | meaning | pre-migration rows |
| --- | --- | --- | --- |
| `createdAt` | `REAL` | UNIX seconds, stamped at the write | `NULL` |
| `scenePhase` | `TEXT` | app state when the row was written | `NULL` |
| `backfillJobId` | `TEXT` | the `backfill_jobs.jobId` this row came from | `NULL` |

Plus two indexes: `idx_semantic_scan_results_createdAt` (timeline
reconstruction) and `idx_semantic_scan_results_backfill_job` (the join).

**`backfillJobId` WAS CALLED `runCorrelationId` UNTIL SCHEMA V65** (playhead-1gu0,
2026-08-26), and this whole document was written under the old spelling. The
rename is the only change: it is an `ALTER TABLE … RENAME COLUMN`, no value moves
and nothing is backfilled. It happened because the name said RUN and the writer
writes a JOB — and a `backfill_jobs.jobId` is per `(asset, phase, offset)`, so it
is ONE value for an asset's whole backfill history. Measured on the 2026-08-19
device pull, `count(DISTINCT runCorrelationId)` and `count(DISTINCT
analysisAssetId)` are both **15**, with 176 rows over four calendar days under a
single id. **Any archived device pull taken before V65 still spells the column
`runCorrelationId`**, so a query that sweeps across pulls has to handle both.
Nothing here can tell two screenings of the same window apart; `transcriptVersion`
can, and does so on every replicate window of both measured pulls.

**All three are NULLABLE with no DEFAULT, and that is the whole point.** A
`DEFAULT 0` on `createdAt` would make every historical row claim 1970; a
`DEFAULT 'foreground'` on `scenePhase` would manufacture exactly the confident,
unattributable number this bead exists to end. Rows written before this
migration are genuinely unattributable and read `NULL`.

**No backfill.** Nothing scans the existing table and guesses.

### The rung guard, and why it is not optional

`migrateSemanticScanAttributionV42IfNeeded` carries `guard observed >= 41`, not
just `guard observed < 42`. V39 is allowed to fail without throwing: it rolls
back to its savepoint and leaves `schema_version` at 38 so the next launch
retries. A plain `< 42` guard stamps 42 onto such a database — and because V39
and V40 are themselves version-gated, they can then never run. A *contained,
recoverable* failure becomes permanent.

This is not hypothetical. The first cut of this rung had the plain guard and
both V39 containment suites went red on `relaunched.schemaVersion() == 38` in
about 0.05 s. Every rung added after V39 has to repeat the guard; the ladder
does not enforce it structurally, so the next person adding V43 will have to
remember too.

### The vocabulary is borrowed, not invented

`scenePhase` reuses `BGTaskTelemetryScenePhase`'s strings **verbatim** —
`"active"`, `"inactive"`, `"background"`, `"unknown"` — the same four that
`background_task_runs.scenePhase` already holds, produced by the same helper.
This follows playhead-9v09: an existing vocabulary, not a second one. It means
a phase split over `semantic_scan_results` and one over `background_task_runs`
are directly comparable, and `GROUP BY scenePhase` across a UNION is meaningful.

Note the two distinct absences the vocabulary already supports:

* `NULL` — no binary ever recorded a phase for this row (pre-V42, or a writer
  that had none to give).
* `'unknown'` — a phase WAS recorded, and the platform declined to say which
  (`BGTaskTelemetryScenePhase.current()` returns `"unknown"` on non-UIKit hosts
  and for `@unknown default`).

Both are **unattributed** for the foreground/background split. They are kept
distinguishable because they answer different questions ("is the writer wired
up?" vs "is the platform answering?").

## What an older binary sees

Following playhead-6qvf: the new value must degrade *safely* in an older binary,
not be misread.

* **Reads.** Every pre-V42 reader selects through
  `AnalysisStore.semanticScanResultColumns`, an explicit column list that does
  not name the three new columns. `SELECT id, analysisAssetId, …` is unaffected
  by columns it does not mention — the reader binds by index against a fixed
  list, and the new columns are appended at the END of the table, so **no index
  shifts**. An older binary reads a V42 database and sees exactly what it saw
  before; the attribution is invisible to it rather than misinterpreted.
* **Writes.** An older binary's `INSERT OR REPLACE INTO semantic_scan_results
  (id, …, jobPhase) VALUES (…)` also names its columns explicitly. The three new
  columns are nullable with no `NOT NULL`, so the insert succeeds and the row
  lands with `NULL` attribution — i.e. an old binary's rows are honestly
  reported as unattributed, which is precisely true.
* **Version stamp.** `_meta.schema_version` reads `42`, which is greater than an
  older binary's `currentSchemaVersion`. `AnalysisStore.migrate()` already logs
  that case (`observed > currentSchemaVersion` → "Behavior may be undefined")
  and continues; every `V*IfNeeded` rung short-circuits on its `<` guard. This
  is the pre-existing downgrade posture and V42 does not change it.

There is no field in which an older binary can produce a WRONG attribution. The
only outcomes are "absent" and "correct".

## The measurement playhead-kvs8 could not make

kvs8 was asked to re-split the measured 2.4× slower-than-realtime FM figure by
foreground vs background and could not, because the attribution did not exist.
It does now. The query, over one device DB:

```sql
-- FM scan throughput, split by the scene phase the scan completed in.
-- `audioSeconds` is the transcript window the scan covered; `wallSeconds` is
-- the model's own measured latency for it. ratio > 1 == slower than realtime.
SELECT
    COALESCE(scenePhase, 'unattributed')            AS phase,
    COUNT(*)                                        AS scans,
    SUM(windowEndTime - windowStartTime)            AS audioSeconds,
    SUM(latencyMs) / 1000.0                         AS wallSeconds,
    (SUM(latencyMs) / 1000.0)
        / NULLIF(SUM(windowEndTime - windowStartTime), 0) AS realtimeRatio
FROM semantic_scan_results
WHERE status = 'success'
  AND latencyMs IS NOT NULL
  AND windowEndTime > windowStartTime
  AND (errorContext IS NULL OR errorContext NOT LIKE 'noWork:%')
GROUP BY phase
ORDER BY phase;
```

The `COALESCE(scenePhase, 'unattributed')` is load-bearing and is the reason the
answer is honest: pre-V42 rows form **their own bucket** and are never folded
into `active` or `background`. A reader can see at a glance how much of the
corpus is attributable at all before believing the split.

The `noWork:` exclusion matters for the same reason it matters in
`SemanticScanCoverage.compute` (playhead-pz32): a sentinel row spans a range it
never examined, so counting its (zero) latency against its (whole-episode)
audio seconds would report a spectacularly fast model that never ran.

The Swift consumer is `SemanticScanThroughputSplit` — same arithmetic, same
three buckets, and it is what the negative test bites on.

### Joining a scan row to its BGTask run

`backfillJobId` is the `backfill_jobs.jobId`. The chain to a BGTask run is
scan → job → the run whose `[startedAt, finishedAt]` window contains the scan:

```sql
SELECT s.id            AS scanId,
       s.createdAt,
       s.scenePhase    AS scanPhase,
       j.jobId,
       j.phase         AS jobPhase,
       r.runId,
       r.entryPoint,
       r.scenePhase    AS runPhase
FROM semantic_scan_results s
JOIN backfill_jobs j
  ON j.jobId = s.backfillJobId
LEFT JOIN background_task_runs r
  ON s.createdAt >= r.startedAt
 AND s.createdAt <= COALESCE(r.finishedAt, 9e18)
WHERE s.backfillJobId IS NOT NULL;
```

The scan → job join is an equality on a real key. The job → BGTask-run join is a
temporal containment, because a BGTask wake drains an arbitrary set of jobs and
no column names the run — the `createdAt` this bead adds is what makes even that
possible. `LEFT JOIN` on purpose: a scan performed in the foreground belongs to
no BGTask run at all, and that must read as "no run", not as a dropped row.

## Evidence

**Every number and every join in this document was demonstrated against a
fixture built by the test suite, not against device data.** The device DBs
available on this box are stale (newest 2026-07-22, pre-V42 by construction) and
cannot contain a single attributed row. `SemanticScanRunAttributionTests` builds
the three-table fixture, writes the rows, and runs the join and the split for
real. No field numbers are quoted here because none were measured.

## What is pinned, and where

| claim | test |
| --- | --- |
| V42 shape lands on a fresh install and on the v1-seeded ladder | `MigrationLadderTests`, `SemanticScanRunAttributionTests` |
| a v41 row survives the migration and still reads NULL (no backfill) | `v41RowSurvivesMigrationAndStaysUnattributed` |
| the scan → job → BGTask-run join resolves, and a foreground scan LEFT-JOINs to no run | `scanRowJoinsToJobAndRun` |
| the store stamps a clock and invents nothing else | `storeStampsClockButNeverInventsAPhase` |
| **a nil phase is never read as a phase** | `nilScenePhaseIsUnattributed`, `unattributedRowsNeverBecomeForeground`, `allUnattributedCorpusYieldsNoForegroundMeasurement` |
| the production write path actually stamps, and the id it stamps joins | `SemanticScanAttributionWireInTests` |
| this document's SQL computes what the shipped consumer computes | `sqlAndSwiftSplitsAgree` |

The mutation battery's **T series** (`scripts/mutation-battery.sh`, entries
T01–T15, batches 210–216) is the certification: fifteen edits that each
reproduce a real way this could be silently undone — `?? .active` on the read
line, a store that invents a phase at the write, a seam that stops stamping, a
zero denominator reported as `1.0` — and the rail each one has to trip.
