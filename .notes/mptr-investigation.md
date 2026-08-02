# playhead-mptr investigation log

## Step 1 — the constant hunt (done, negative result)

Searched the whole tree for a configured 45-minute bound:

- `grep -rn "2700" --include="*.swift" Playhead/` -> **zero production hits**.
  Only hits are test fixtures and an unrelated omnycontent URL containing "ae2700380ca7".
- `grep -rnE "45 ?\* ?60|60 ?\* ?45|45\.0 ?\* ?60|\.minutes\(45\)"` -> one production hit,
  `Playhead/Services/Observability/SLI.swift:108` `p50Seconds: TimeInterval = 45 * 60`.
  That is an SLI *reporting threshold* (time-to-proximal-skip-ready), read by dashboards.
  It is not in the transcription path and bounds nothing.
- `grep -rnE "= *[0-9]+ *\* *60"` across all of `Playhead/` -> no duration cap at 2700 s
  anywhere. Nearest neighbours: `t2DepthSeconds = 900`, `unplayedCandidateWindowSeconds = 1200`,
  `resumedCandidateWindowSeconds = 900`, `capOutRetryCooldownSeconds = 3600`.

**Result: there is no configured 2700 s / 45-minute bound.** The orchestrator's prior
("a round number means a configured bound") is a good prior but it does not hold here.

## Step 2 — what the field row actually measures (the reframe)

`AnalysisJobRunner.emitTranscriptionTimeoutJournal`:

```swift
let currentChunkCount = (try? await store.fetchTranscriptChunks(assetId: assetId).count) ?? existingChunkCount
let chunksPersisted = max(0, currentChunkCount - existingChunkCount)
let transcriptCoverageEndTime = (try? await store.fetchAsset(id: assetId))?.fastTranscriptCoverageEndTime ?? 0
```

`transcript_coverage_end_time` is **read from the asset row's stored watermark**. It is NOT
"where the engine went silent on this attempt". `chunks_persisted = 0` says this attempt
added nothing at all.

So the bead's premise — "the speech engine went silent at exactly 2700 s, five times" — is a
misreading. The five rows report the same 2700.000 **because the watermark never moved**: it is
one stored scalar re-read five times, not five independent measurements landing on the same
offset. Watch item 2's mystery ("why the same offset five times?") dissolves: identical is the
*expected* reading of an unchanged field.

The real questions are therefore:
  (a) why is the stored watermark exactly 2700.000, and
  (b) why does every attempt persist zero chunks?

## Step 3 — the mechanism (found)

`AnalysisJobRunner.run`, stage 3, races two tasks:

```swift
group.addTask {
    try? await Task.sleep(for: .seconds(300))
    return (0, nil, false)          // <- reads out as engine_silent_timeout
}
group.addTask { ... observeTranscriptEvents(...) }
```

`observeTranscriptEvents` breaks only on `.completed` / `.failed`. No progress event resets
anything, and the sleep is unconditional. So **the 300 s "timeout" is not an inactivity
watchdog — it is a hard wall-clock cap on the entire transcription stage.** Any pass that needs
more than five minutes reports `engine_silent_timeout` no matter how healthy it is.

`TranscriptEngineService.runTranscriptionLoop` then walks EVERY shard:

```swift
// Coverage filtering is intentionally NOT applied here — per-shard
// fingerprint dedup in `transcribeShard` handles already-transcribed
// regions ... See review playhead-rfu-aac H3 for the rationale.
let prioritized = prioritizeShards(shards)
```

and `transcribeShard` runs the expensive ASR FIRST and dedups AFTER:

```swift
let segments = try await speechService.transcribe(shard: shard, podcastId: activePodcastId)
...
if let existingChunk = try await store.fetchTranscriptChunk(...) { ... continue }   // dedup
```

**The dedup is post-ASR.** It saves a row insert; it does not save the transcription.

## The trap

On a re-run of an asset that already has 2700 s of transcript, the loop re-transcribes 0–2700 s
from scratch, every segment dedups to an existing chunk (`chunksInserted = 0`), and the runner's
flat 300 s cap fires long before the loop reaches uncovered audio past 2700 s.

Result, exactly as observed in the field: `chunks_persisted = 0`, watermark unchanged, no
`.completed`, no `.failed` -> `engine_silent_timeout`. Deterministic, so identical on every
attempt until `attemptCount` caps out.

**It is self-reinforcing.** The further the watermark advances, the more audio each subsequent
run must re-transcribe before reaching anything new. Once the already-covered prefix costs more
than 300 s of ASR, the episode can never advance again. That is a permanent ceiling, and it is
what "stuck at 68.7%, five attempts, zero progress" is.

The bead's watch item 2 ("why the same offset five times?") is answered: not five measurements
landing on one offset, but one stuck watermark re-read five times, stuck because every attempt
spends its whole budget re-reading audio it already has.
