# Dogfood build STAGED 2026-07-30 ~12:15 ET — main @ 20c821d7 — NOT YET INSTALLED

⚠️ **The install did not complete.** The build succeeded and is verified, but the transfer failed
with `CoreDeviceError 3002 "Connection interrupted"` mid-flight as the phone left, and the retry hit
`4016` (device unavailable). Run the command below when the phone is back, unlocked and connected.

**Because 3002 interrupted mid-transfer, the app on the phone may be partially written.** If Playhead
behaves oddly on next launch, re-run the install — that is the fix, not a symptom of a bad build.

## Install

```bash
cd /Users/dabrams/playhead
DEVELOPER_DIR=/Applications/Xcode-beta.app/Contents/Developer \
  xcrun devicectl device install app --device ED78151E-7376-5F72-8BDD-05ECBD354949 \
  .derivedDataDevice/Build/Products/Release-iphoneos/Playhead.app
```

`4016` means locked or not actively paired — unlock, confirm `xcrun devicectl list devices` shows
`connected`, re-run.

## Verified in the artifact, not just "BUILD SUCCEEDED"

| symbol | hits | proves |
|---|---|---|
| `adScanRedrive` | 2 | onn6 (#298) bounded re-drive |
| `AnalysisStoreRecovery` / `analysis_store_health` | 5 | wvdz (#299) no silent DB deletion |
| `completeAdScanPartial` | 3 | gqx4 (#297) degraded terminal |

Do NOT verify by grepping for `"Partly analyzed"` — that literal is 15 UTF-8 bytes, Swift's
small-string limit, so it is stored inline and `strings` can never see it. Grep the enum case names.

## What is new since the build now on the phone (main @ 271815c9)

- **#298 onn6** — under-scanned episodes get a bounded re-drive. **6 episodes** that had no
  dispatchable job at all and would never have been scanned again now get one:
  `E71CF852`, `1E32428C`, `B5786B41`, `1A9616D1`, `8FECFDDE`, `06E94E9D`.
  Two more are *deliberately* declined (superseded orphans — a pass would read no audio) and three
  are out of reach (the 7-day GC deleted their job rows).
- **#299 wvdz** — **a failed migration can no longer silently delete the analysis database.**
  It used to `removeItem` the whole store directory and retry, and the retry succeeded *because the
  directory was empty*. Now: retry first, then surface in Settings → Diagnostics after three
  failures, and the only path that touches the directory *moves* it to quarantine.

## Worth checking

- **Do any percentages climb?** That is the observable test of onn6 + i7qe.
- ◐ will **not** become ✓ from onn6 alone — a degraded terminal is a hard veto and only the session
  lane reclassifies. The percentage moves; playing the episode re-finalizes it.
- The ◐ glyph itself is still **never verified on a real screen**.

## NOT in this build

**playhead-se2h** (swallowed model-load failure). Its adversarial review found three HIGH issues,
one of which recreated the exact permanent-silent-failure shape the bead exists to fix — a load with
no timeout anywhere in its chain, leaving a sticky in-flight latch set forever. Fixes were still
uncommitted and unverified at build time, so it was held rather than shipped unverified.

## Known limit, deliberate and filed

**playhead-onn6 (P1)** — an episode that finishes its coverage tiers under-scanned has no re-drive.
Once the scheduler marks the job `complete`, `insertJob`'s `INSERT OR IGNORE` on that row's
`workKey` prevents a new one, so nothing re-queues the scan. #297 converts a false "fully analysed"
into a true "partly analysed" and unblocks episodes that still have a live job; it does not by
itself finish the scan for the rest. Also noted there: no query anywhere selects queued
`backfill_jobs` rows, so several `fullEpisodeScan` rows have sat queued for six days.
