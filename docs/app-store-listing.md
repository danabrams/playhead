# App Store listing — copy and screenshots

Bead: `playhead-jw63.2`. Customer outcome: **a stranger understands Playhead in
ten seconds, and what they understand is relief rather than technology.**

Every character count in this document is measured by
`scripts/check_app_store_copy.py`, which fails if any field exceeds its App
Store Connect limit. Do not hand-count; the limits are exact and a listing that
is one character over is rejected at submission.

---

## The rules this copy obeys

Four, and the first two are Dan's standing external-copy rules
(`project_founder_reorder_2026-07-24`).

1. **Never "ad detection". Never "AI".** People do not buy a detector. They buy
   not having to think about this any more. The mechanism may appear in the
   privacy paragraph, where it is a *reassurance* rather than a feature.
2. **The competitor is the 30-second skip button**, not Overcast or Pocket
   Casts. Every manual reach is the listener hiring themselves to do work the
   app should have done. Copy never compares Playhead to another player.
3. **Say the thing no competitor can say.** The category has at least eight
   iPhone entrants and every one of them is reviewed the same way: it missed
   ads, and it cut my show. Nobody claims the second half. Playhead can,
   because the deterministic channel is the only one allowed to skip:

   > Playhead only skips what it is certain of, and shows you a card for the rest.

4. **The keyword field is not story copy.** Rule 1 governs what a human reads.
   The keyword field is not read by humans and it is where the category's
   demand actually lives — the literal query is "skip podcast ads", and three
   competitors put it in their app name. Withholding those words there costs
   installs and buys nothing.

---

## Fields

### App name — limit 30

```
Playhead
```

Deliberately just the name. Suffixing a keyword ("Playhead: Skip Podcast Ads")
is what the also-rans do, and it trades the one asset a premium one-time
purchase needs — looking like a product rather than a utility — for keyword
weight the field below already carries.

### Subtitle — limit 30

```
Your podcasts, without the ads
```

Lifted from the onboarding screen, which already says it better than anything
written for this document. The full stop is dropped to fit.

### Promotional text — limit 170

Editable without a new build, so it carries the thing most likely to change.

```
Playhead only skips what it's certain of, and shows you a card for the rest. Nothing to set up, nothing sent to the cloud, and no monthly bill.
```

### Keywords — limit 100, comma-separated, no spaces after commas

```
skip ads,ad free,podcast player,adblock,commercial,sponsor,offline,transcript,private,ondevice
```

The app name and subtitle are already indexed, so "podcast" and "ads" are not
repeated here. Rule 4 applies: these words are for the search index.

### Description — limit 4000

```
Podcasts are better when you are not braced for the next interruption.

Playhead listens ahead of you. When an ad break is dynamically inserted into an
episode, Playhead recognises it and steps over it before you get there. You
hear the show, and then you hear more of the show.

WHAT IT WILL NOT DO

It will not cut your show. Playhead only skips a break when it is certain. When
something is borderline — a host talking warmly about a sponsor, halfway into a
story — it does not guess. A quiet card appears as you reach it, you tap once
to skip or ignore it, and Playhead remembers what you chose for that show.

That restraint is the whole design. An app that skips a little too eagerly
takes the thing you came for, and you cannot get it back by tapping.

WHEN YOU PRESS PLAY, THE WORK IS ALREADY DONE

Download an episode and Playhead prepares it in the background — usually while
your phone is on the charger overnight. By the time you press play there is a
checkmark and nothing to wait for. No queue to manage, no button to press, no
setting to find.

IT ALL HAPPENS ON YOUR IPHONE

Nothing is uploaded. Episodes are never sent to a server for processing, there
is no account, and no one — including us — has any record of what you listen
to. Your library lives on your phone.

This is also why there is no subscription. Playhead costs nothing to run once
it is on your device, so it is a single purchase and it is yours.

A PROPER PODCAST PLAYER

Subscribe by search or by RSS. Download for offline listening. CarPlay, chapter
support, variable speed, silence trimming, and a transcript you can read along
with or search.

TRY IT ON A SHOW YOU LOVE

One show is free, with unlimited episodes — enough to hear it work on a podcast
you already know well. Unlock the rest whenever you want. One payment, no
subscription, every show you follow.

Requires an iPhone with Apple Intelligence. English-language shows.
```

### What's New — limit 4000, first release

```
First release.

Playhead skips the ad breaks it is certain about, and shows you a quiet card
for anything it is not — so it never cuts the show you came for.

Everything happens on your iPhone. Nothing is uploaded, there is no account,
and there is no subscription.
```

---

## Screenshots

Six per size, in this order. **The first two are the whole listing** — most
people never swipe past them.

| # | Shows | Caption (limit 60) |
|---|---|---|
| 1 | Now Playing mid-episode, a skip just completed, the timeline showing the stepped-over break | `The ad is already behind you` |
| 2 | A card at the moment of entry, Skip and dismiss both visible | `Not sure? It asks instead of guessing` |
| 3 | Library with a mix of ✓ ready, ◐ partly analysed, ↓ not downloaded | `Ready before you press play` |
| 4 | Transcript with the ad region marked, playhead riding the text | `Read along. Search everything you've heard` |
| 5 | Settings privacy section | `Nothing leaves your iPhone. No account, ever` |
| 6 | CarPlay Now Playing | `Works where you actually listen` |

Sizes: **6.9-inch (1320 × 2868)** and **6.5-inch (1242 × 2688)**. Those two
cover every current iPhone in App Store Connect; a 6.9-inch set alone is
accepted but renders letterboxed on older devices.

**These have not been captured yet, and that is the one open item on this
bead.** They need a real listening session on a device with a real feed, not a
simulator with fixtures — a fabricated card showing a sponsor that was never
detected is exactly the kind of claim this repo does not ship. Capture after
the cohort build lands, when there is a device with real analysed episodes on
it.

---

## App Review notes

Kept in `docs/app-review-notes.md` (bead `playhead-i7kvl.9`) rather than here,
because the reviewer needs a *demo path* rather than marketing copy. The short
version, so it is not forgotten: a reviewer on a device without Apple
Intelligence, or on a show whose network does not rotate its ad insertions,
will see nothing skip and reject the app as not working as described.

---

## Still needed before submission

| | Bead |
|---|---|
| Screenshots captured on a device | this bead |
| Privacy policy URL and support URL | `playhead-i7kvl.6` |
| Price set to $49.99 in App Store Connect and the StoreKit file | `playhead-i7kvl.7` |
| The free tier actually enforced — copy above promises one free show | `playhead-i7kvl.1` |
| App Review notes and a verified demo show | `playhead-i7kvl.9` |

**The description makes two promises the build does not yet keep**, and both
are listed above rather than softened in the copy, because the copy is right
and the build is what has to catch up: *one show is free* (there is no
entitlement gate at all today) and *chapter support* (present in the codebase;
confirm it is user-visible before submitting). Re-read this section at
submission and cut any promise still outstanding.
