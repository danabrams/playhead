# FeatureExtraction drop redundant accumulator (playhead-wmjr)

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Eliminate the in-memory `allWindows: [FeatureWindow]` accumulator from `FeatureExtractionService.extractAndPersist(...)` so peak in-flight RAM during feature extraction stays proportional to **one shard's** windows rather than scaling linearly with episode duration.

**Architecture:** SQLite (via `AnalysisStore`) is already the durable source of truth for feature windows — every batch is persisted atomically inside the per-shard loop at `Playhead/Services/FeatureExtraction/FeatureExtraction.swift:569`. The returned array is therefore a redundant second copy. All three production callers already discard the return (`@discardableResult`); only tests read it. The fix changes the return type from `[FeatureWindow]` to `Void`, deletes `var allWindows`, deletes the in-loop mutation block (`allWindows.lastIndex(where: ...)` rebuild — a no-op for production because the same `priorWindowUpdate` value flows into the store independently via `priorWindowStoreUpdate`), and updates 5 tests to assert against `store.fetchFeatureWindows(...)` instead of the return value.

**Tech Stack:** Swift, Swift Testing (`@Suite`/`@Test`/`#expect`), SwiftData/SQLite via `AnalysisStore`, the existing `LanePreemptionCoordinator` integration.

---

## Pre-flight: Worktree state

Already set up at `/Users/dabrams/playhead/.worktrees/playhead-wmjr` on branch `bead/playhead-wmjr` from `origin/main`. `bd update playhead-wmjr --status in_progress` already run.

All subsequent tasks run in `.worktrees/playhead-wmjr`. Build/test commands use `-derivedDataPath .derivedData` per CLAUDE.md disk-hygiene policy.

---

### Task 1: Add a failing memory-bound test that pins the accumulator removal

**Files:**
- Modify: `Playhead/Services/FeatureExtraction/FeatureExtraction.swift` (add a DEBUG-only watermark seam analogous to the one we just shipped for `StreamingAudioDecoder` in playhead-jnpf)
- Modify: `PlayheadTests/Services/FeatureExtraction/FeatureExtractionSignalTests.swift` (add a new `@Test` at the end of the existing suite)

**Why this test exists:** The bead's acceptance criterion is "peak in-flight RAM proportional to **one window**, not the whole episode." Just like jnpf, we pin the bound with a DEBUG watermark seam so the invariant survives future refactors. Without a test seam, "drop the accumulator" becomes a one-time cleanup whose regression would silently re-introduce O(episode) RAM growth.

**Step 1: Add the watermark seam on the actor-equivalent**

`FeatureExtractionService` is a `final class`/`actor` (verify which — read the existing declaration line). Just inside the type body, near the existing private state, add:

```swift
#if DEBUG
/// Test-only watermark of the largest in-flight per-call accumulator
/// observed across the lifetime of this service. Used by
/// `FeatureExtractionSignalTests.peakAccumulatorBoundedAcrossShards`
/// to pin the no-double-source-of-truth invariant from playhead-wmjr.
/// Reset to zero at the start of each `extractAndPersist(...)` call.
private(set) var _peakInFlightWindowCountForTesting: Int = 0

func peakInFlightWindowCountForTesting() -> Int {
    _peakInFlightWindowCountForTesting
}
#endif
```

If `FeatureExtractionService` is an `actor`, the accessor is `func ... async -> Int` and callers `await` it. If it's a `final class`, it's a synchronous `func`. Match the surrounding type.

**Step 2: Write the failing test**

Append inside the existing `FeatureExtractionSignalTests` suite (immediately before its closing `}` at the end of the file):

```swift
@Test("Peak in-flight window count stays bounded across many shards (playhead-wmjr)")
func peakAccumulatorBoundedAcrossShards() async throws {
    let store = try await makeTestStore()
    try await store.insertAsset(makeAnalysisAsset())

    // 30 shards of 4 s each = 120 s. With 2 s windows that's 60 windows
    // in the store at the end. The bound we want to pin: peak
    // in-flight count <= ~one shard's worth of windows (~2-3) plus
    // small slack — *not* 60. The bug bound is duration-independent
    // so 30 shards proves the same invariant a 5-hour run would.
    let samples = Array(repeating: Float(0.001), count: 32)
    let config = makeFeatureExtractionConfig()
    let service = FeatureExtractionService(
        store: store,
        config: config,
        musicProbabilityTimelineBuilder: { _ in .init(observations: []) }
    )
    let shards = (0..<30).map { i in
        AnalysisShard(
            id: i,
            episodeID: "ep-bound",
            startTime: Double(i) * 4,
            duration: 4,
            samples: samples
        )
    }

    try await service.extractAndPersist(
        shards: shards,
        analysisAssetId: "asset-1",
        existingCoverage: 0
    )

    // Bound: at most one shard's windows (~3 with seam-overlap slack)
    // plus a small fudge factor. The pre-fix accumulator would hit ~60.
    let peak = await service.peakInFlightWindowCountForTesting()
    #expect(
        peak <= 8,
        "Peak in-flight window count was \(peak), expected ≤ 8 (one shard's windows). Pre-fix value would be ~60."
    )

    // Sanity: the store still has the full result.
    let persisted = try await store.fetchFeatureWindows(
        assetId: "asset-1",
        from: 0,
        to: 120
    )
    #expect(persisted.count >= 50, "Expected ≥50 persisted windows from 30 shards, got \(persisted.count)")
}
```

Notes for the implementer:
- `makeTestStore`, `makeAnalysisAsset`, `makeFeatureExtractionConfig` are existing helpers in this file — match their existing call patterns.
- The musicProbabilityTimelineBuilder closure signature should match what other tests in this file use; copy from the simplest existing call site.
- If `service.peakInFlightWindowCountForTesting()` is non-async (class), drop the `await`.

**Step 3: Run the test to verify it FAILS (proves the redundant accumulator exists)**

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData \
  -only-testing:'PlayheadTests/FeatureExtractionSignalTests/peakAccumulatorBoundedAcrossShards()' \
  2>&1 | tail -25
```

Expected: FAIL. The peak should be ≈60 (one full episode's windows), reported by the `#expect(peak <= 8)` assertion.

The watermark is updated by Task 2's implementation. To make Task 1 test fail meaningfully (i.e., show the bug), the watermark must already be wired up to the `allWindows` accumulator BEFORE the fix. So in Task 1, also add to the existing pre-fix `extractAndPersist` (immediately after `allWindows.append(contentsOf: windows)` at ~line 581):

```swift
#if DEBUG
if allWindows.count > _peakInFlightWindowCountForTesting {
    _peakInFlightWindowCountForTesting = allWindows.count
}
#endif
```

And immediately at function entry (just before `var allWindows: [FeatureWindow] = []`):

```swift
#if DEBUG
_peakInFlightWindowCountForTesting = 0
#endif
```

This gives the test something real to observe in the failing state.

**Step 4: Commit the failing test + watermark**

```bash
git add Playhead/Services/FeatureExtraction/FeatureExtraction.swift \
        PlayheadTests/Services/FeatureExtraction/FeatureExtractionSignalTests.swift
git commit -m "test(feature-extraction): pin bounded-accumulator (failing) (playhead-wmjr)"
```

---

### Task 2: Drop the accumulator and the return value (the fix)

**Files:**
- Modify: `Playhead/Services/FeatureExtraction/FeatureExtraction.swift`
- Modify: `PlayheadTests/Services/FeatureExtraction/FeatureExtractionSignalTests.swift`
- Modify: `PlayheadTests/Services/AnalysisCoordinator/LanePreemptionCoordinatorTests.swift`

**Step 1: Change the function signature and remove the accumulator**

In `FeatureExtraction.swift` around lines 495-595:

1. Remove the `@discardableResult` attribute (no longer needed without a return value).
2. Change the return type from `-> [FeatureWindow]` to `-> Void` (or omit the arrow entirely):

   ```swift
   func extractAndPersist(
       shards: [AnalysisShard],
       analysisAssetId: String,
       existingCoverage: Double = 0,
       preemption: PreemptionContext? = nil
   ) async throws {
   ```

3. Update the docstring at line 494 — replace the `- Returns: Array of extracted FeatureWindow records.` line with a note pointing to `AnalysisStore.fetchFeatureWindows(assetId:from:to:)` for retrieval.

4. Delete `var allWindows: [FeatureWindow] = []` at line 506.

5. Delete the entire in-loop mutation block at lines 538-549:

   ```swift
   if let priorWindowUpdate = extractionResult.priorWindowUpdate {
       if let lastIndex = allWindows.lastIndex(where: {
           abs($0.startTime - priorWindowUpdate.startTime) <= 1e-6 &&
           abs($0.endTime - priorWindowUpdate.endTime) <= 1e-6
       }) {
           let previous = allWindows[lastIndex]
           allWindows[lastIndex] = rebuildWindow(
               previous,
               speakerChangeProxyScore: priorWindowUpdate.speakerChangeProxyScore
           )
       }
   }
   ```

   Why this is safe: `extractionResult.priorWindowUpdate` is **separately** packaged into `priorWindowStoreUpdate` at lines 557-565 and passed to `persistFeatureExtractionBatch(...)` at line 572. The store applies it directly to the persisted row. The mutation of `allWindows[lastIndex]` was only ever observable through the return value — pure dead work for production.

6. Delete `allWindows.append(contentsOf: windows)` at line 581.

7. Delete the watermark update added in Task 1 at the same location (it referenced `allWindows.count`).

8. Replace `return allWindows` at line 590 (the preemption-acknowledged path) with a bare `return`.

9. Delete `return allWindows` at line 594 (final implicit `return ()`).

10. **Add a new watermark seam that proves the bound holds.** Just before the per-shard loop starts (after the `extractionState = ...` initialization, before `for shard in shards`), and again **inside** the loop at the place where the per-shard windows have been computed but not yet persisted, update the watermark. The peak should be at most one shard's `windows.count`:

    ```swift
    // Inside the loop, immediately after `let windows = extractionResult.windows`
    // and before `try await store.persistFeatureExtractionBatch(...)`:
    #if DEBUG
    if windows.count > _peakInFlightWindowCountForTesting {
        _peakInFlightWindowCountForTesting = windows.count
    }
    #endif
    ```

    And keep the `_peakInFlightWindowCountForTesting = 0` reset at function entry (added in Task 1).

**Step 2: Update the 4 tests in `FeatureExtractionSignalTests.swift` that read the return value**

Each of these 4 tests captures the result of `service.extractAndPersist(...)` and asserts on it. After the fix, fetch from the store instead. Concrete edits:

**(a) `extractAndPersistCarriesSeamCuesAcrossShardBoundaries` at line 280:**

Change line 302 from:
```swift
let extracted = try await service.extractAndPersist(...)
```
to:
```swift
try await service.extractAndPersist(...)
let extracted = try await store.fetchFeatureWindows(assetId: "asset-1", from: 0, to: 8)
```

The `assertMatchesWholeBufferReference(extracted: extracted, reference: reference)` call at line 323 is unchanged.

**(b) `extractAndPersistPreservesSeamStateAcrossCalls` at line 326:**

Change line 349 from:
```swift
let firstBatch = try await service.extractAndPersist(...)
```
to:
```swift
try await service.extractAndPersist(...)
let firstBatch = try await store.fetchFeatureWindows(assetId: "asset-1", from: 0, to: 4)
```

Change line 367 from:
```swift
let secondBatch = try await service.extractAndPersist(...)
```
to:
```swift
try await service.extractAndPersist(...)
let secondBatch = try await store.fetchFeatureWindows(assetId: "asset-1", from: 4, to: 8)
```

The `firstBatch.count == 2` / `secondBatch.count == 2` assertions and the `fetched` reassertion are unchanged. Note the existing `let fetched = try await store.fetchFeatureWindows(assetId: "asset-1", from: 0, to: 8)` at line 380 already proves the same thing — we keep it for explicit aggregate verification.

**(c) `extractAndPersistRollsBackFailedSeamBatch` at line 388:**

Change line 411 from:
```swift
let firstBatch = try await service.extractAndPersist(...)
```
to:
```swift
try await service.extractAndPersist(...)
let firstBatch = try await store.fetchFeatureWindows(assetId: "asset-1", from: 0, to: 4)
```

The throws-expectation block at line 426-440 already discards the return value — no edit needed.

Change line 456 from:
```swift
let retriedSecondBatch = try await service.extractAndPersist(...)
```
to:
```swift
try await service.extractAndPersist(...)
let retriedSecondBatch = try await store.fetchFeatureWindows(assetId: "asset-1", from: 4, to: 8)
```

Existing assertions on `firstBatch.count == 2`, `windowsAfterFailure.count == firstBatch.count`, `retriedSecondBatch.count == 2`, etc., are unchanged.

**(d) `extractAndPersistReplacesStaleFeatureVersionRows` at line 475:**

Change line 515 from:
```swift
let extracted = try await service.extractAndPersist(...)
```
to:
```swift
try await service.extractAndPersist(...)
let extracted = try await store.fetchFeatureWindows(assetId: "asset-1", from: 0, to: 4)
```

The existing `let fetched = try await store.fetchFeatureWindows(assetId: "asset-1", from: 0, to: 4)` at line 528 becomes redundant with `extracted`. Either delete one or keep both (deletion preferred — they're now identical reads).

**Step 3: Update the 1 test in `LanePreemptionCoordinatorTests.swift` that reads the return value**

In `LanePreemptionCoordinatorTests.swift` at line 671:

Change the `extractionTask` declaration at lines 671-678 from:
```swift
let extractionTask = Task<[FeatureWindow], Error> {
    try await featureService.extractAndPersist(
        shards: shards,
        analysisAssetId: assetId,
        existingCoverage: 0,
        preemption: context
    )
}
```
to:
```swift
let extractionTask = Task<Void, Error> {
    try await featureService.extractAndPersist(
        shards: shards,
        analysisAssetId: assetId,
        existingCoverage: 0,
        preemption: context
    )
}
```

Change `let windows = try await extractionTask.value` at line 708 to:
```swift
try await extractionTask.value
let windows = try await store.fetchFeatureWindows(assetId: assetId, from: 0, to: .infinity)
```

The downstream assertions (`#expect(!windows.isEmpty, ...)` at 719, `#expect(windows.count < 16 * 15, ...)` at 721) work unchanged with the store-fetched array.

**Step 4: Run the previously-failing test — should now pass**

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData \
  -only-testing:'PlayheadTests/FeatureExtractionSignalTests/peakAccumulatorBoundedAcrossShards()' \
  2>&1 | tail -25
```

Expected: PASS, with `peak <= 8`.

**Step 5: Run the FeatureExtraction + LanePreemptionCoordinator suites to verify no regression**

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData \
  -only-testing:'PlayheadTests/FeatureExtractionSignalTests' \
  -only-testing:'PlayheadTests/LanePreemptionSchedulerExtractionRoundTripTests' \
  2>&1 | tail -40
```

Expected: All `extractAndPersist*` tests PASS plus the new bound test PASS plus the `LanePreemptionCoordinator` round-trip test PASS.

**Step 6: Commit the fix**

```bash
git add Playhead/Services/FeatureExtraction/FeatureExtraction.swift \
        PlayheadTests/Services/FeatureExtraction/FeatureExtractionSignalTests.swift \
        PlayheadTests/Services/AnalysisCoordinator/LanePreemptionCoordinatorTests.swift
git commit -m "fix(feature-extraction): drop redundant in-memory accumulator (playhead-wmjr)"
```

---

### Task 3: Full-suite regression check + ship

**Step 1: Run the full PlayheadFastTests plan**

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData \
  2>&1 | tail -40
```

Expected: full plan green. Pay particular attention to anything in `FeatureExtractionTests`, `AnalysisCoordinator*`, and `AnalysisJobRunner*` that constructs the service or calls `extractAndPersist`.

**Step 2: Push and open PR**

```bash
git push -u origin bead/playhead-wmjr
gh pr create --title "fix(feature-extraction): drop redundant accumulator (playhead-wmjr)" \
  --body "$(cat <<'EOF'
## Summary
- `FeatureExtractionService.extractAndPersist(...)` no longer maintains an in-memory `[FeatureWindow]` array of every persisted window. Return type is now `Void` — SQLite (via `AnalysisStore.fetchFeatureWindows`) is the sole source of truth.
- Peak in-flight RAM during feature extraction is now proportional to **one shard's** windows (~3 windows for default config) instead of scaling with episode duration (~9 K windows for a 5-hour episode).
- DEBUG-only watermark seam pins the bound across regression.
- Three production callers (`AnalysisJobRunner.swift:213`, `AnalysisCoordinator.swift:1241`, `AnalysisCoordinator.swift:1760`) already discarded the return value; only test sites needed updating (4 in `FeatureExtractionSignalTests`, 1 in `LanePreemptionCoordinatorTests`) — they now fetch from the store.

Fixes playhead-wmjr.

## Test plan
- [x] `peakAccumulatorBoundedAcrossShards` (new) passes
- [x] `extractAndPersist*` suite (4 existing tests) green via store-fetch
- [x] `LanePreemptionScheduler*ExtractionRoundTrip*` test green via store-fetch
- [x] Full `PlayheadFastTests` plan green
EOF
)"
```

**Step 3: Squash-merge and clean up worktree**

After review:

```bash
gh pr merge --squash --delete-branch
cd /Users/dabrams/playhead
git fetch origin main
git checkout main
git reset --hard origin/main   # local main was ahead by the plan-only commit
bd close playhead-wmjr
WT=/Users/dabrams/playhead/.worktrees/playhead-wmjr
git worktree remove "$WT"
[ -d "$WT/.derivedData" ] && rm -rf "$WT/.derivedData"
git worktree prune -v
git branch -D bead/playhead-wmjr 2>/dev/null || true
```

---

## Notes for implementers

- **Do not** change the per-shard atomic persistence semantics — `persistFeatureExtractionBatch(assetId:windows:priorWindowUpdate:checkpoint:coverageEndTime:)` is the contract the rest of the system relies on for crash safety / resumability (playhead-01t8).
- **Do not** remove `@discardableResult` from any *other* function — only `extractAndPersist`'s.
- The `priorWindowStoreUpdate` flow at lines 557-565 must remain untouched. It carries the seam-corrected speaker-change-proxy score for the **prior** persisted window across shard boundaries. Only the in-memory mirror of that update at lines 538-549 is being deleted.
- The DEBUG-only watermark is a deliberate test seam, not production state. It is `#if DEBUG`-gated so Release archives do not ship it (same pattern as playhead-jnpf shipped earlier today).
- If the new test is flaky on simulator (window-count variance from seam smoothing producing 1 vs 2 windows for a tiny shard), the fix is to widen `allowedMax` from 8 to e.g. 16 — the bound being tested is *duration-independence*, not exact window count. Resist the temptation to lower the shard count instead, since that masks the invariant.
