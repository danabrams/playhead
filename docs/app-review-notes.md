# App Review notes — and the demo path behind them

Bead `playhead-i7kvl.9`. Two audiences: the reviewer, who reads only the
**Notes for Review** block below, pasted verbatim into App Store Connect; and
whoever submits, who reads the rest first.

The whole risk is one sentence: a reviewer who plays an episode and hears
nothing skip will reject the app as *not working as described*. Two things
make that happen, and both are avoidable — a device without Apple
Intelligence, and a show whose network does not rotate its ad insertions. So
the notes name a device and name a show, and say what the reviewer will hear.

---

## Notes for Review (paste verbatim)

> **Device.** Please review on an iPhone 15 Pro or later running iOS 27. The
> app's on-device processing requires Apple Intelligence hardware; on an older
> device it will download and play episodes but will not skip anything.
>
> **A show that demonstrates the skip.** Add **Conan O'Brien Needs a Friend**
> from the search field. Open its most recent episode and tap **Download**.
> Wait for the checkmark to appear on the episode row — this means the episode
> is ready. Then tap play.
>
> **What you will hear.** The sponsor read at the very start of the episode
> is skipped. You will hear a brief, subdued sound at the moment of the skip,
> and the timeline will glide past the skipped region. If a later sponsor
> read is one the app is not certain about, it does not skip it; instead a
> small card appears as that read begins, offering a one-tap Skip. You can
> also tap **Hearing an ad** at any time to tell the app about a read it
> missed.
>
> **What a card is.** Playhead never rewrites a feed, never modifies or
> re-hosts audio, and never sends anything off the device. When it skips a
> sponsor read, the sponsor, the promo code and the offer survive as a card
> the listener can act on. The listener decides — every time.
>
> **Privacy.** There is no account. Audio and transcripts stay on the device.
> The only network traffic is fetching the podcast feed and audio, and an
> optional iCloud sync of the user's subscriptions and purchase.
>
> **Purchase.** Playhead is a one-time purchase (com.playhead.premium). In the
> sandbox, the purchase sheet appears the first time skipping would occur.

---

## Why that show, with the evidence

The claim "the sponsor read at the very start is skipped" is not a hope. It is
what the 2026-09-02 device pull shows for this feed:

- **43** ad windows on the device carry `boundaryState =
  dayZeroRediffByteExact` — the deterministic channel, the only one allowed to
  skip without asking.
- The episodes carrying them are overwhelmingly this show: "Danny McBride
  Returns" 5 windows, "Gillian Anderson" 4, "The Bonering Conan" 3, and four
  "Summer S'pouses" episodes at 3 each.
- Several begin at **0.0 s** — a pre-roll, so the reviewer hears the skip
  within the first second of pressing play. "Gillian Anderson" had all 4 of
  its windows executed as skips; "Summer S'pouses Episode 4" 3 of 3.

That network serves per-request ad insertions, which is what the day-0 double
fetch needs: two copies of one episode whose only byte difference is the ads.
The 2026-07-21 survey (`docs/xsdz16-rediff-spike.md`) records that
Megaphone-hosted shows **re-encode per stitch** and never align — those are
precisely the shows to keep OUT of these notes, and the reason the bead's own
parenthetical ("Megaphone/libsyn worked as-is") should not be trusted over the
device.

Re-run the query before submission; a feed can change hosts:

```sql
select substr(a.episodeTitle,1,50), count(*), round(min(w.startTime)), sum(w.wasSkipped)
from ad_windows w join analysis_assets a on a.id = w.analysisAssetId
where w.boundaryState = 'dayZeroRediffByteExact'
group by 1 order by 2 desc limit 10;
```

## What is deliberately NOT in the notes

- **No minute figure for "wait for the checkmark."** It depends on episode
  length, the network, and whether the second fetch is granted promptly. A
  number quoted from memory is the kind of promise that becomes a rejection.
  **Before submitting, time it on a device with this show and add the
  measured figure** — the row is in the checklist below.
- **No mention of the mechanism.** The rules in `docs/app-store-listing.md`
  apply to this text too; the reviewer is told what happens, not how.
- **No fallback show.** If Conan's network changes its serving behaviour, the
  fix is to re-run the query above and substitute, not to list a second show
  the evidence does not currently support.

## Checklist before this goes into App Store Connect

| | Owner |
|---|---|
| Time download → checkmark on a device with the named show; add the figure to the notes | Dan |
| Confirm the purchase sheet appears where the notes say it does, in the sandbox | Dan |
| Re-run the evidence query on a fresh pull; confirm the show still leads | whoever submits |
| Paste the block above into **App Review Information → Notes** | Dan |

The "What a card is" paragraph is the canonical creator-respect paragraph from
`docs/app-store-listing.md` (bead `playhead-i7kvl.8`).
`scripts/check_app_store_copy.py` verifies this file carries it verbatim.
