# StreamingAudioDecoder bounded-accumulator fix (playhead-jnpf)

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Convert `StreamingAudioDecoder` from buffer-then-batch to truly streaming so peak in-flight PCM RAM stays bounded by ~one shard's worth (≈1.9 MB) regardless of episode duration.

**Architecture:** Move the existing `emitFullShards()` call from after the inner read-loop in `decodeAvailableFrames()` to inside the loop, immediately after each chunk's converted samples are appended to `accumulatedSamples`. This caps the peak `accumulatedSamples.count` at `samplesPerShard + (readFramesPerCycle × sampleRateRatio)`. Add a DEBUG-only peak-sample-count watermark on the actor as a test seam. Update the type-level docstring to document the new bound.

**Tech Stack:** Swift, AVFoundation (AVAudioFile + AVAudioConverter), Swift Testing (`@Suite`/`@Test`), iOS app actor, OSLog.

---

## Pre-flight: Worktree setup

Run from `/Users/dabrams/playhead`:

```bash
git fetch origin main
git checkout main
git pull --ff-only
mkdir -p .worktrees
git worktree add -b bead/playhead-jnpf .worktrees/playhead-jnpf origin/main
cd .worktrees/playhead-jnpf
bd update playhead-jnpf --status in_progress
```

All subsequent tasks run in `.worktrees/playhead-jnpf`. Build/test commands use `-derivedDataPath .derivedData` per CLAUDE.md disk-hygiene policy.

---

### Task 1: Add failing test that pins the peak-accumulator bound

**Files:**
- Modify: `PlayheadTests/Services/TranscriptEngine/TranscriptEngineTests.swift` (extend the existing `StreamingAudioDecoderTests` suite, around the existing `incrementalVsBulkFeed` test ~line 1729)

**Why this test exists:** The bead's acceptance criterion is "peak `accumulatedSamples.count` × 4 bytes stays bounded regardless of episode duration." Today the buffer grows to the full episode's samples before any shard is emitted. After the fix it must stay below `samplesPerShard + readFramesPerCycle × ratio + slack`. Picking a duration that exercises **many** shard-emit cycles is what proves the bound is duration-independent.

**Step 1: Add a peak-watermark accessor on the actor (test seam)**

In `Playhead/Services/AnalysisAudio/StreamingAudioDecoder.swift`, just after the `accumulatedSamples` declaration (line 56), add:

```swift
    #if DEBUG
    /// Test-only watermark of the largest `accumulatedSamples.count` ever
    /// observed across the lifetime of this decoder. Used by
    /// `StreamingAudioDecoderTests` to pin the bounded-accumulator invariant.
    private var _peakAccumulatedSampleCountForTesting: Int = 0

    /// Test-only accessor for the peak watermark.
    func peakAccumulatedSampleCountForTesting() -> Int {
        _peakAccumulatedSampleCountForTesting
    }
    #endif
```

The watermark is updated by the implementation in Task 2 (do not update it yet — Task 1 is intentionally a failing test).

**Step 2: Write the failing test**

Append inside `struct StreamingAudioDecoderTests` (before `// MARK: - WAV Helper` at ~line 1756):

```swift
    @Test("Peak accumulator stays bounded across many shard-emit cycles")
    func peakAccumulatorBoundedAcrossManyShards() async throws {
        // 5-minute synthetic at shardDuration=1.0s exercises ~300 emit cycles.
        // The bug bound is duration-independent, so 300 cycles proves the same
        // invariant a literal 5-hour run would (without the 576 MB temp file).
        let seconds: UInt32 = 300
        let wavData = Self.makeWAVData(seconds: seconds)

        let decoder = StreamingAudioDecoder(
            episodeID: "test",
            shardDuration: 1.0,
            contentType: "wav"
        )
        let stream = await decoder.shards()

        // Drain shards on a child task so the AsyncStream's internal buffer
        // doesn't grow without bound while we feed.
        let drain = Task<Int, Never> {
            var count = 0
            for await _ in stream { count += 1 }
            return count
        }

        // Feed in 4KB chunks (matches realistic download-pipeline cadence).
        let chunkSize = 4096
        var offset = 0
        while offset < wavData.count {
            let end = min(offset + chunkSize, wavData.count)
            await decoder.feedData(wavData[offset..<end])
            offset = end
        }
        await decoder.finish()

        let shardCount = await drain.value

        // 5 minutes at shardDuration=1.0s yields ≥ ~290 shards (resampler
        // can drop a frame or two at the boundary).
        #expect(shardCount >= 290, "Expected ≥290 shards from 5-min WAV at 1s shards, got \(shardCount)")

        // Bound: samplesPerShard (1.0s × 16_000 = 16_000) + at most one
        // converter chunk (8192 frames × ratio=1.0 for 16 kHz source = 8192).
        // Allow 4× slack for converter framing variance and the final flush.
        let peak = await decoder.peakAccumulatedSampleCountForTesting()
        let allowedMax = 16_000 + 8_192 * 4
        #expect(
            peak <= allowedMax,
            "Peak accumulator was \(peak) samples, expected ≤ \(allowedMax) (≈\(allowedMax * 4 / 1024) KB)"
        )

        await decoder.cleanup()
    }
```

**Step 3: Run the test to verify it fails (proves the bug exists)**

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData \
  -only-testing:'PlayheadTests/StreamingAudioDecoderTests/peakAccumulatorBoundedAcrossManyShards()' \
  2>&1 | tail -20
```

Expected: FAIL. The `#expect(peak <= allowedMax)` line should report a `peak` value ≈ 5 minutes × 16 000 = 4 800 000 samples (or close to it — the buffer grows the entire run pre-fix).

**Step 4: Commit the failing test**

```bash
git add Playhead/Services/AnalysisAudio/StreamingAudioDecoder.swift \
        PlayheadTests/Services/TranscriptEngine/TranscriptEngineTests.swift
git commit -m "test(streaming-decoder): add bounded-accumulator pin (failing) (playhead-jnpf)"
```

---

### Task 2: Move shard emission inside the read loop (the fix)

**Files:**
- Modify: `Playhead/Services/AnalysisAudio/StreamingAudioDecoder.swift`

**Step 1: Move `emitFullShards()` from after the inner loop to inside it**

In `decodeAvailableFrames()` (around lines 231–268), the inner loop currently looks like:

```swift
        while framesRemaining > 0 {
            // ... read chunk, convert ...
            let converted = convertBuffer(readBuffer, using: chunkConverter)
            if !converted.isEmpty {
                accumulatedSamples.append(contentsOf: converted)
            }
        }

        // Emit full shards from accumulated samples.
        emitFullShards()
```

Change it to:

```swift
        while framesRemaining > 0 {
            // ... read chunk, convert ...
            let converted = convertBuffer(readBuffer, using: chunkConverter)
            if !converted.isEmpty {
                accumulatedSamples.append(contentsOf: converted)
                #if DEBUG
                if accumulatedSamples.count > _peakAccumulatedSampleCountForTesting {
                    _peakAccumulatedSampleCountForTesting = accumulatedSamples.count
                }
                #endif
                emitFullShards()
            }
        }
        // (no post-loop emit — drained inline above)
```

**Step 2: Update the type-level docstring to document the streaming contract**

At the top of `actor StreamingAudioDecoder` (line 16–19), replace the existing brief docstring with:

```swift
/// Incrementally decodes compressed audio bytes into 16 kHz mono Float32
/// `AnalysisShard`s. Feed it chunks via `feedData(_:)` and emits a shard
/// every ~`shardDuration` seconds of accumulated audio.
///
/// **Streaming contract.** Internal PCM accumulation is bounded:
/// `accumulatedSamples.count` peaks at roughly
/// `samplesPerShard + readFramesPerCycle × (16_000 / sourceSampleRate)`.
/// For a 30 s shard at a 16 kHz source that's ≈488 KB; the buffer never
/// scales with total episode duration. (The downstream `AsyncStream`'s
/// queue is the consumer's concern — bound that on the consumer side
/// by draining promptly.)
///
/// Compressed bytes are appended to a temp file on disk; only the
/// in-progress shard's PCM lives in memory.
```

**Step 3: Run the previously-failing test — should now pass**

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData \
  -only-testing:'PlayheadTests/StreamingAudioDecoderTests/peakAccumulatorBoundedAcrossManyShards()' \
  2>&1 | tail -20
```

Expected: PASS.

**Step 4: Run the existing StreamingAudioDecoderTests suite to verify no regression**

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData \
  -only-testing:'PlayheadTests/StreamingAudioDecoderTests' \
  2>&1 | tail -25
```

Expected: All `StreamingAudioDecoderTests` cases PASS — `decodesWAVData`, `incrementalVsBulkFeed`, `belowDetectionThreshold`, `finishTerminatesStream`, `cleanupRemovesTempFile`, `shardsCalledOncePrecondition`, plus the new `peakAccumulatorBoundedAcrossManyShards`.

**Step 5: Commit the fix**

```bash
git add Playhead/Services/AnalysisAudio/StreamingAudioDecoder.swift
git commit -m "fix(streaming-decoder): emit shards inline so peak RAM stays bounded (playhead-jnpf)"
```

---

### Task 3: Full-suite regression check

**Step 1: Run the full PlayheadFastTests plan**

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData \
  2>&1 | tail -40
```

Expected: full plan green. Pay particular attention to anything in `TranscriptEngineTests` and any test that constructs a `StreamingAudioDecoder`.

**Step 2: If green, push and open PR**

```bash
git push -u origin bead/playhead-jnpf
gh pr create --title "fix(streaming-decoder): bounded-accumulator (playhead-jnpf)" \
  --body "$(cat <<'EOF'
## Summary
- `StreamingAudioDecoder` now emits shards inside the read loop instead of
  buffering the entire episode and emitting in bulk.
- Peak `accumulatedSamples.count` is now bounded by
  `samplesPerShard + readFramesPerCycle × ratio` (≈488 KB for the default
  30 s shard at a 16 kHz source) regardless of episode duration.
- New test pins the bound across ~300 emit cycles using a 5-minute synthetic
  WAV (duration-independent invariant).

Fixes playhead-jnpf.

## Test plan
- [x] `peakAccumulatorBoundedAcrossManyShards` (new) passes
- [x] Existing `StreamingAudioDecoderTests` suite still green
- [x] Full `PlayheadFastTests` plan green
EOF
)"
```

**Step 3: Squash-merge and clean up worktree**

After review:

```bash
gh pr merge --squash --delete-branch
cd /Users/dabrams/playhead
git checkout main
git pull --ff-only
bd close playhead-jnpf
WT=/Users/dabrams/playhead/.worktrees/playhead-jnpf
git worktree remove "$WT"
[ -d "$WT/.derivedData" ] && rm -rf "$WT/.derivedData"
git worktree prune -v
```

---

## Notes for implementers

- **Do not** change the `AsyncStream` continuation buffering policy. The bead explicitly scopes that out.
- **Do not** rename the type — the streaming claim now holds after the fix.
- **Do not** touch `convertBuffer` or `emitShard` — only the call-site of `emitFullShards()` and the type-level doc.
- The DEBUG-only watermark is a deliberate test seam, not production state. It is `#if DEBUG`-gated so Release archives do not ship it.
- If the new test is flaky on simulator (resampler timing variance), the fix is to widen `allowedMax`, not lower the cycle count — the bound is what's being tested, not exact shard count.
