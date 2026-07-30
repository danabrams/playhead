# Dogfood build installed 2026-07-30 — main @ 271815c9

**Already installed on the iPhone** (ED78151E…) — `devicectl` reported
`App installed: bundleID com.playhead.app`. Nothing to run unless you want to reinstall.

Built from **main @ 271815c9**, Release, signed (team 36Z6VYTT9X, `com.playhead.app`),
389 MB including the 336 MB `qwen3_0_6b_4bit_dynamic_ft_v2` specialist model and `StingerBank.json`.

## Reinstall (phone unlocked + connected)

```bash
cd /Users/dabrams/playhead
DEVELOPER_DIR=/Applications/Xcode-beta.app/Contents/Developer \
  xcrun devicectl device install app --device ED78151E-7376-5F72-8BDD-05ECBD354949 \
  .derivedDataDevice/Build/Products/Release-iphoneos/Playhead.app
```

If it prints `CoreDeviceError 4016` ("not able to fulfill the requested usage assertion
requirements"), the phone is locked or not actively paired — unlock it, confirm
`xcrun devicectl list devices` shows the iPhone as `connected` rather than `unavailable`,
then re-run.

## Verified beyond "BUILD SUCCEEDED"

The binary contains `completeAdScanPartial` (×3) and all three residual `AdScanLimit` cause
tokens (`neverRan`, `stoppedShort`, `unmeasurableDuration`), which proves **#297 is genuinely in
this artifact**, plus `partiallyAnalyzed` (×2) for pz32's ◐ state.

⚠️ **Do not verify by grepping the binary for `"Partly analyzed"` — it will return zero and that
is not a defect.** The literal is exactly 15 UTF-8 bytes, which is Swift's small-string limit, so
it is stored INLINE in the String struct rather than emitted into `__cstring`; `strings` can never
see it. Grep for the enum case name (`partiallyAnalyzed`), which comes from reflection metadata.
The previous staging note's claim that the literal was greppable was wrong.

## What is new since the last staged build (main @ 149721c4)

**#297 — playhead-gqx4 + playhead-i7qe: the ad scan now actually reaches the audio.**

The last build made the checkmark honest (pz32). This one goes after the thing the honest
checkmark exposed — that most episodes were barely scanned at all.

- **gqx4** — `completeFull` used to be declared from transcript + feature coverage only. Neither is
  the ad scan, so episodes were stamped "fully analysed" with ~3% of their audio ever examined, and
  because that state is terminal nothing ever went back. It now requires measured ad-scan coverage
  (the same number the ✓ uses), and a short scan lands in a new degraded terminal
  `completeAdScanPartial` that records WHY (refusal, guardrail, decode failure, interrupted,
  stopped short, unmeasurable duration).
- **i7qe** — the pre-analysis runner skipped the semantic scan whenever the transcript had not grown
  and no candidate windows were outstanding. But candidate windows are *produced by* the scan, so
  their absence in unscanned audio meant nothing was ever proposed there — the skip was reading an
  absence as completion. Seven assets on the last device pull sat in exactly that shape and could
  never make progress no matter how often the job re-ran.

## Worth checking on this build

**Expect ◐ to persist on old episodes at first, and expect percentages to start climbing.**

- Of the 34 episodes over 15 minutes on the last pull, **14 still have a dispatchable job** and
  should now actually get scanned — including 2 that were stamped `completeFull` (820134BF at 39%,
  70EC53D7 at 90%) and 3 stuck in `backfill`. Watch whether their percentages move.
- The other 20 will NOT improve on their own — see the known limit below. That is expected.
- Episodes analysed on the previous build read ◐ **"Amount scanned unknown"** on first launch: this
  build is a cohort rev, so prior scan rows are pruned until re-scanned. Honest but inert.
- **Still unverified visually** (carried over from the last build): nobody has looked at the ◐ glyph
  on a real screen. Worth a glance, and worth trying VoiceOver on a library row — it should say
  "Partly analyzed" and "N% scanned for ads".

## Known limit, deliberate and filed

**playhead-onn6 (P1)** — an episode that finishes its coverage tiers under-scanned has no re-drive.
Once the scheduler marks the job `complete`, `insertJob`'s `INSERT OR IGNORE` on that row's
`workKey` prevents a new one, so nothing re-queues the scan. #297 converts a false "fully analysed"
into a true "partly analysed" and unblocks episodes that still have a live job; it does not by
itself finish the scan for the rest. Also noted there: no query anywhere selects queued
`backfill_jobs` rows, so several `fullEpisodeScan` rows have sat queued for six days.
