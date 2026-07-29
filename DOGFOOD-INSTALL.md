# Dogfood build staged 2026-07-30 ~03:00 ET — one command to install

Built from **main @ 149721c4**, Release, signed (team 36Z6VYTT9X, `com.playhead.app`),
389 MB including the 336 MB `qwen3_0_6b_4bit_dynamic_ft_v2` specialist model and `StingerBank.json`.

Verified beyond "BUILD SUCCEEDED": the binary contains the new `partiallyAnalyzed` state and the
"Partly analyzed" string, which proves the pz32 merge is actually in this artifact.

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

Nine merged, reviewed beads: **8m2w, p70f, 0hi9, 8ysk, u45d, ngev, 6av0**, plus tonight's
**ye0n + lc4c** (#295) and **pz32** (#296), and the `.swiftlint.yml` derived-data glob fix.

**playhead-aqo9 is NOT in this build, and never will be** — it was closed as measured-not-viable.
Its two clamps were dropped: on this device's own data 13 of 21 pre-roll windows already start at 0.0
and 8 of 9 post-roll windows already reach EOF, so genuinely recoverable edge width is ~4 s, not the
148 s the bead claimed. The larger gaps are transcribed cold opens — widening into them takes SHOW.
The two pieces worth keeping were split out and ARE in this build as ye0n and lc4c.

## Worth checking on this build

**Expect FEWER checkmarks, and that is the fix, not a regression.** pz32 makes the readiness ✓ key on
measured ad-scan coverage instead of the DSP feature watermark and last-ad-end. On the device capture,
4 of 5 episodes read ◐ "Partly analyzed" with a real number (5%, 15%, 19%, 39%). The previous ✓ was
lying — it could even read MORE complete on an episode where detection did WORSE. The low numbers are
the real reach problem (playhead-gqx4 / playhead-i7qe), now visible instead of hidden.

- **pz32** — a half-scanned episode should show ◐ with a percentage, not ✓. **UNVERIFIED VISUALLY:**
  nobody has looked at the ◐ glyph on a real screen — the state machine, glyph name and VoiceOver
  strings are unit-tested only. Worth a glance. Also try VoiceOver on a library row: it should say
  "Partly analyzed" and "N% scanned for ads".
- **ye0n / lc4c** — a pre-roll widened to 0:00 should keep auto-skipping rather than dropping to a
  banner; and a span you mark by hand must never be swallowed by a detector window widening over it.
- **u45d** — marking a detected span "not an ad" should actually dismiss it, and a manual mark should
  outrank a banner response.
- **8ysk** — analysis jobs should finish rather than stalling (147 acquired / 9 finalized was the
  measured starting point).
- **p70f / 0hi9** — day-0 rediff should mint, and an episode should no longer split into two asset rows.
- **6av0** — transcript spans should no longer appear duplicated.

## Known, deliberate under-claim

After a cohort rev (app build, OS build, prompt/schema rev) the scan rows are pruned, so a
previously-completed episode renders ◐ "Amount scanned unknown" until re-scanned. Honest but inert —
making it actionable needs a re-drive path in the job state machine.

**This build is a cohort rev.** So expect episodes analysed on the previous build to read
"Amount scanned unknown" on first launch rather than showing a percentage.
