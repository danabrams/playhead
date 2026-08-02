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

## Step 4 — the roundness, answered

`AnalysisAudioService.defaultShardDuration = 30.0` (`Playhead/Services/AnalysisAudio/AnalysisAudio.swift:498`).

The watermark tracks SHARD ENDS, not chunk ends, for the whole of a pass —
`TranscriptEngineService.updateCoverage(endTime: shard.startTime + shard.duration)` — and is
reconciled to `MAX(chunk.endTime)` only at `.completed`, which this asset never reached.

So every intermediate watermark on this asset is a multiple of 30, and

    2700 = 90 x 30

is simply the 90th shard boundary. **The round number IS a configured quantity — but the
quantity is the shard grid (30 s), not a 45-minute cap.** The orchestrator's prior was sound
reasoning that pointed at the wrong constant; there is no 2700 s bound, and I searched for one
explicitly (Step 1).

The episode is 3929.9 s = 131 shards. 90 of them sat behind the watermark. The loop re-ran ASR
over all 90 before it could reach shard 91, inside a 300 s stage budget — i.e. it needed to
sustain better than 9x realtime just to arrive at new audio. On a thermally-throttled phone it
never did.

## Watch item 1 — y8f3's capRetry: MINTS CORRECTLY, BUT COULD NOT HAVE HELPED

Verified by construction (no field artifact available — see "Evidence" below).

`AnalysisWorkScheduler.capOutRetryDecision` would mint for this row:
  - `isAttemptCapTerminal`: state `superseded` + `lastErrorCode` prefix `maxAttemptsReached` — the
    field row is exactly `maxAttemptsReached:transcription`. PASSES.
  - `nextOrdinal`: no `capRetry:*` key on disk yet -> 1. PASSES.
  - cooldown: `capOutRetryCooldownSeconds = 3600`. PASSES after 1 h, as the bead expects.
  - `outstandingTranscriptTarget(2700, tiers: [90, 300, 900], episodeDurationSec: 3929.9)`:
    `coverageTierLadder` appends the episode duration, so the ladder is
    [90, 300, 900, 3929.9] and the next rung above 2700 is 3929.9. PASSES.
  -> `.mint(workKey: "<base>:capRetry:1", ordinal: 1, desiredCoverageSec: 3929.9)`.

Note the decision reads the ASSET watermark, not the job column — y8f3 already handles the trap
where `updateJobProgress` leaves the job row at 0.0. So **y8f3 has no gap of the kind the bead
feared.**

**But the retry was futile, and that is the important half.** A `capRetry:1` runs the same
transcription loop against the same 2700 s of existing coverage and hits the same 300 s wall
before reaching new audio. It would have capped out again at the same watermark. This is exactly
the bead's own escape-hatch condition — "if the capRetry also dies at 2700.000, retrying is not
the answer at all". y8f3 is a SCHEDULING remedy; the defect is a COMPUTE-BUDGET one. Retrying
harder cannot fix a pass that spends its entire budget re-reading what it already has.

The fix in this bead is what makes y8f3's retry able to make progress: with the covered prefix
skipped, a `capRetry:1` targeting 3929.9 s reaches new audio immediately.

## Watch item 3 — se2h's SpeechModelLoadJournal

`SpeechModelLoadJournal.recordFailure` is called from exactly one place:
`TranscriptEngine.prepareFastModel()`'s catch arm, and ONLY when the load actually threw and the
task was not cancelled.

For this failure the journal is expected to be EMPTY, and its emptiness is informative rather
than a wiring bug: every non-ready `prepareFastModel` outcome
(`.failed`/`.loadInFlight`/`.budgetExhausted`) makes `runTranscriptionLoop` emit
`speech_engine_not_ready`, which the runner would have observed as a `.failed` event — and that
would have classified as `engine_reported`, NOT `engine_silent_timeout`.
`Observation.classify(failure: nil, sawCompleted: false) == .engineSilentTimeout` requires
`failure == nil`, i.e. the engine reported NOTHING.

So the field row's own classification RULES OUT the model-load path. The engine did not restart
and did not fail to load; it was busy. `staleInFlightLoadThreshold = 120 s` is below the runner's
300 s cap, so even a wedged load supersedes itself and reports inside the budget. That is a
second, independent reason the diagnosis lands on the re-transcription cost rather than on the
speech stack.

## Step 5 — two consequences of the fix worth stating plainly

### (a) It hollowed out an existing regression test, and that was caught by reading, not by the gate

`TranscriptEngineFailureEventTests.dedupOnlyRerunWithOneBadShardStillCompletes` runs the real
loop twice over one store: pass 1 transcribes two shards, pass 2 re-runs them with a recognizer
that throws on the second. It pins "a run whose only work was dedup is not a total failure".

Under mptr, pass 1 satisfies BOTH skip conditions for BOTH shards, so pass 2 would skip
everything — `FailAfterFirstRecognizer` never called, no failure, `.completed` emitted. **The
test stays green and proves nothing.** The gate cannot catch this: a vacuous pass is still a
pass.

Fixed by rewinding the watermark between the passes (`resetFastTranscriptCoverage`, the one
sanctioned rewind). That keeps the chunks and removes the skip's licence, so both shards run and
shard 0 still dedups through the production fingerprint path. The test's original claim survives
intact.

### (b) The total-failure gate's verdict changes in one shape, and the new answer is the honest one

The gate is `!shardFailures.isEmpty && chunksInsertedThisRun == 0 && shardsCompletedThisRun == 0`.
A skipped shard increments NEITHER tally — deliberately, because those tallies are documented as
"THIS RUN's own progress" and a shard skipped is a shard an EARLIER run carried.

So: a re-run that skips 90 covered shards and fails the one new shard it reached now reports a
total failure, where before the 90 dedup-completions would have masked it as `.completed`.

That is the better answer. `.completed` is what tells the runner "coverage is durable", and a run
that reached exactly one new shard and failed it has produced nothing durable. Masking that is
the same class of lie playhead-8ysk removed. A run that skips everything and fails nothing still
emits `.completed`, because `shardFailures` is empty — a fully-transcribed episode re-run is
unaffected.

## Scope NOT taken (deliberate)

The `drainLoop`'s appended-shard arm calls `transcribeShard` without the skip. Left alone on
purpose: that is the PLAYBACK lane (streaming decoder appending as it decodes), not the analysis
lane, and it is not under the 300 s stage cap that produced this bug. `AnalysisJobRunner` is an
explicit batch caller — it calls `finishAppending` immediately, so its append queue is always
empty and the field defect cannot arrive through that arm. Extending the skip there would change
a second lane's behaviour without a fixture for it.

## Step 6 — THE SKIP WAS WRONG, AND THE GATE PROVED IT

Gate 1 on the skip version: **`RED (0 known / 2 NEW)`**, and both NEW failures were the same
thing —

    swift-testing::duplicate fingerprint can upgrade missing speakerId and avgConfidence
                   and re-emit chunk
    swift-testing::a second row for one (asset, pass, fingerprint) is refused, and the
                   survivor still takes the speakerId upgrade

They also crashed the host (`Fatal error: Index out of range` on `chunks[0]`), which is why the
run reported `DID NOT RUN — 41 of 41 recorded tests were never reached`. The 41 is a consequence
of the crash, not a separate defect.

**What they prove.** `transcribeShard`'s duplicate-fingerprint arm is not dead weight: it
backfills `speakerId` and `avgConfidence` onto rows that lacked them and re-emits the upgraded
chunk. That arm is FED BY re-running the ASR. A shard that is skipped can never be enriched — so
the "deliberate tradeoff" I wrote into the first version was not a tradeoff at all, it was a
silent capability deletion, and it had tests.

**The rework.** Order, do not filter. `orderingUncoveredFirst` is a stable partition: shards no
artifact backs sort first, already-backed shards follow, each group keeping the playhead-proximity
order `prioritizeShards` produced. Everything still runs; every upgrade still happens.

This is strictly better than the skip, not merely safer:

- the 300 s cap is spent on unread audio, which is the entire point;
- covered shards still get their upgrades with whatever budget remains;
- a wrong answer from the artifact test now costs LATENCY, never COVERAGE — so H3's
  counterexamples do not even need to be right for safety, only for speed. (They still sort as
  uncovered and run first, which is stronger than pre-mptr, not weaker.)
- and the two consequences recorded in Step 5 both EVAPORATE: the dedup-heavy re-run fixture
  needs no watermark rewind (both shards run again, as it always assumed), and the total-failure
  gate's tallies are untouched because covered shards still complete.

Step 5 is therefore superseded. It is kept above because the reasoning that produced it is what
found the ordering answer — the tradeoff I talked myself into was the signal that the design was
wrong.

## Operational note — the playhead-cgka trap fired mid-bead

Gate 2 stalled at the install/boot step with xcodebuild alive at 0 % CPU and no compiler
processes. Disk was at **10 GiB** and the single booted simulator was **9.0 GB** — it had NOT
shrunk back after gate 1, so the run was heading straight into the documented wedge.

Killed by PID (never `pkill -f`), then:

    xcrun simctl shutdown <udid>   # reported "current state: Shutdown" — already down
    xcrun simctl erase <udid>      # device dir 9.0 GB -> 18 MB

...and the erase freed only ~1 GiB, because CoreSimulator had *moved* the data to
`$TMPDIR/Deleting-*` rather than deleting it (playhead-cgka). Clearing it took the volume from
10 GiB to **19 GiB**:

    chmod -R u+rwx "$TMPDIR"Deleting-*     # u+w is not enough; 0o300 lacks READ
    rm -rf "$TMPDIR"Deleting-*

Worth recording because the failure presented as a hung build, not as a disk error, and because
the sim not shrinking back between two gates in one session is exactly how a box that looks like
it has headroom arrives at 100 %.
