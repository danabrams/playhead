# TestFlight: What to Test

`playhead-i7kvl.5`. Paste the block below into App Store Connect's **What to
Test** field. It obeys the external-copy rules: never "ad detection", never
"AI", and it does not compare Playhead to another podcast app.

Its job is to set expectations so reports come back about **the listening
experience** rather than about analysis percentages. If the app felt like magic
nobody would open the Activity screen, so the note does not point anyone at it.

---

```
Thanks for trying Playhead.

WHAT IT DOES

Playhead steps over ad breaks it is certain about, before you reach them. When
something is borderline — a host talking warmly about a sponsor, halfway into a
story — it does not guess. A quiet card appears as you arrive, and one tap skips
it or waves it away.

It will not cut your show. That restraint is the whole design, so if it ever
does, that is the most important thing you can tell us.

THE FIRST DAY IS DOWNLOAD AND WAIT

Subscribe to a couple of shows and download some episodes, ideally overnight on
a charger. Playhead prepares them in the background. By the time you press play
there should be a checkmark and nothing to wait for.

Playing an episode the moment it downloads is a fair test too — just expect less
from it. The work is meant to be finished before you press play.

WHAT WOULD HELP MOST

- Did it ever cut part of the show? Tap to jump back and mark it — then tell us.
- Did you reach for the 30-second skip button? That is the number we care about.
  Every reach means Playhead should have handled it and did not.
- Did a card show up at the wrong moment, or say something confusing?
- Anything that felt slow, stuck, or like it had given up.

SENDING A REPORT

Settings → Export dogfood logs. It opens an email with a file attached that you
can read before sending. It contains no audio, no transcripts and no episode
titles.

Please send one at the end of each week even if nothing went wrong — a quiet
week is a result, and we cannot tell "nothing to report" from "nobody sent one".

WHAT IS NOT FINISHED

- Some shows serve every listener an identical file, and on those there is
  nothing for Playhead to compare, so you will see cards rather than skips. That
  is expected, not a bug.
- English-language shows only for now.
- Everything runs on your iPhone. Nothing you listen to is uploaded, and there is
  no account.
```

---

## Notes for whoever pastes this

- The field has no hard character limit in practice, but only the first few
  lines show without tapping "more" — the first paragraph carries the promise on
  purpose.
- Update the "what is not finished" list before each build. A stale limitation
  costs more trust than the limitation itself.
- Keep the reporting ask. `docs/cohort-readout.md` excludes bundles that carry no
  counters and states the population it used, so a week where nobody exports is
  visibly a smaller sample rather than a quiet zero.
