# Dogfood build staged 2026-07-29 ~23:15 ET — one command to install

Built from **main @ 0c667b83**, Release, signed (team 36Z6VYTT9X, `com.playhead.app`),
389 MB including the 336 MB `qwen3_0_6b_4bit_dynamic_ft_v2` specialist model and `StingerBank.json`.

## Install (phone unlocked + connected)

```bash
cd /Users/dabrams/playhead
DEVELOPER_DIR=/Applications/Xcode-beta.app/Contents/Developer \
  xcrun devicectl device install app --device ED78151E-7376-5F72-8BDD-05ECBD354949 \
  .derivedDataDevice/Build/Products/Release-iphoneos/Playhead.app
```

If it prints `CoreDeviceError 4016` ("not able to fulfill the requested usage assertion
requirements"), the phone is locked or not actively paired — unlock it, confirm
`xcrun devicectl list devices` shows the iPhone as `connected` rather than `unavailable`,
then re-run. That is the only failure seen here; the build itself is complete and verified.

## What is in it

Seven merged, reviewed beads: **8m2w, p70f, 0hi9, 8ysk, u45d, ngev, 6av0** — plus the
`.swiftlint.yml` derived-data glob fix.

**playhead-aqo9 is deliberately NOT in this build.** Its rework (per-edge eligibility — outer-edge
widening no longer demotes the window) is committed on `bead/playhead-aqo9` but has one unexplained
regression in `AddedMarkSurvivesBackfillTests`. See the bead for what is ruled out.

## Worth checking on this build

- **u45d** — marking a detected span "not an ad" should now actually dismiss it, and a manual
  mark should win over a banner response.
- **8ysk** — analysis jobs should finish rather than stalling (147 acquired / 9 finalized was
  the measured starting point).
- **p70f / 0hi9** — day-0 rediff should mint, and an episode should no longer split into two
  asset rows.
- **6av0** — transcript spans should no longer appear duplicated.
