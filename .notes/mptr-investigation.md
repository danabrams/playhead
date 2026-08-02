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
