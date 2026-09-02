# The weekly cohort readout

`playhead-i7kvl.3`. Dan recruits ~20 TestFlight listeners; this turns their
diagnostic reports into one number.

## The number

**Manual skip-forward reaches per listening hour.** Every manual reach is the
listener hiring themselves to do work Playhead should have done, which is why
the competitor is the 30-second skip button rather than another podcast app.

## The ritual

1. Ask testers to export a report on a fixed weekday: **Settings → Export
   dogfood logs**, then send the email. It attaches a file they can read first.
2. Save the attachments into one folder, one `.json` per tester.
3. Run it:

   ```bash
   python3 scripts/cohort_readout.py ~/cohort/2026-week-37
   python3 scripts/cohort_readout.py ~/cohort/2026-week-37 --json   # machine-readable
   ```

4. Paste the table into `playhead-i7kvl`, the launch-window epic, so the weeks
   sit next to each other.

## Reading it honestly

- **`not recorded` is not zero.** A bundle from a build predating the counters,
  or one whose store was never consulted, says so. Those bundles are excluded
  from the cohort total and the output states how many were used, because a rate
  over an unstated population is the thing this whole script exists to avoid.
- **A recorded zero IS a measurement**, and a good one: that listener never
  reached for the skip button.
- **`—` means the rate has no denominator.** Reaches over no listening time is
  not a rate, so it prints a dash rather than a division.
- **Auto-skips come from a different instrument** — the surface-status event
  stream, not the counters — so do not fold the two columns together.
- **A saturated scheduler tail is noted and does not affect the counts.** They
  come from `scheduler_event_census`, which is measured over the whole journal
  (`playhead-yz3o`).

## What the number cannot tell you

Roughly **29 %** of episodes yield no byte-exact window at all (6 of 21 with a
fired day-zero attempt, on the 2026-09-02 pull). Those listeners see cards and
never a skip, so their reach count is high for a reason that is not a defect in
the skip path. Read a high reaches/hour against which shows that tester follows
before concluding anything about the fast channel.

## Why this is not an upload

`AnalyticsUploadGate.legalSignoffRecorded` is `false` and the production writer
is `DisabledAnalyticsRecordWriter`. Nothing is transmitted, by product decision.
The counters ride in the **user-initiated** diagnostics bundle, so the cohort is
measured from reports people chose to send — a narrower egress than the
telemetry envelope's automatic upload, not a wider one. If the gate is ever
flipped, `docs/site/privacy.html` becomes false the moment it ships.

## Rails

```bash
python3 -m unittest scripts.tests.test_cohort_readout   # 13 tests, <1s, no build
```
