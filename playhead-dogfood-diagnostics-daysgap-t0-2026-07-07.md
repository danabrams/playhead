# DAI Days-Gap Rotation — t0 Snapshot (2026-07-07)

**Bead:** playhead-xsdz.30 (days-gap rotation arm)
**t0 manifest (deliverable):** `playhead-dogfood-diagnostics-daysgap-t0-2026-07-07.json` (repo root, gitignored)
**Fingerprints (redundant copy):** `TestFixtures/Corpus/Snapshots/fingerprints/<episodeId>.t0.fp.json` (gitignored)
**Differ (durable):** `scripts/l2f-daysgap-rediff.py`
**Capture script:** `scratchpad/daysgap_capture.py` (ephemeral; t0-only)

## Why this measurement

The DAI-rotation spike measured the ad-fill turnover rate only at the extremes:
**0/10** back-to-back, **2/10** at ~65 min, **8/10** at ~5 weeks. The
**days-gap** rate — the single biggest open number — was left bracketed at a
wide **20–88%**. This t0 snapshot freezes 24 freshly-published episodes so that
+2d and +7d re-fetches can pin the production-timescale rotation rate with real
N and confidence intervals. **The existence of the t0 manifest starts the clock.**

## What was captured

**24 episodes across 24 distinct shows. 0 skipped.** Every show's newest episode
fell inside the 7-day freshness window (ages **0.05 d – 6.48 d** at t0; median ~3.5 d).
No feed 404'd, timed out, or required auth/premium.

**Total bytes fetched: 1,233,941,241 B (1.149 GiB / 1.234 GB).** Never more than one
episode's audio on disk at once — each was fpcalc-fingerprinted then **deleted
immediately**. Free disk held steady at 13.6 GB throughout (never approached the
3 GB stop).

### CDN diversity

- **10 distinct first-hop hosts** (tracking prefixes): `rss.art19.com`, `mgln.ai`,
  `sphinx.acast.com`, `podtrac.com`, `pdst.fm`, `dts.podtrac.com`,
  `prfx.byspotify.com`, `tracking.swap.fm`, `pscrb.fm`, `clrtpod.com`.
- **15 distinct final/terminal CDN hosts** (after redirect resolution): art19
  (`content.production.cdn.art19.com`), Megaphone (`dcs.megaphone.fm`,
  `dcs-cached.megaphone.fm`, `dcs-spotify.megaphone.fm`), Acast
  (`stitcher2.acast.com`), Omny/Triton (`26823/27023/27123.mc.tritondigital.com`),
  Simplecast (`stitcher.`, `injector.`, `npr.`, `nyt.simplecastaudio.com`),
  WNYC (`waaa.wnyc.org`), Buzzsprout (`audio.buzzsprout.com`), Cloudfront/audioboom
  (`d11untcg2uthr3.cloudfront.net`).

### Per-episode ledger (sorted by age at t0)

| Show | first-hop CDN | final CDN | pubDate | age(d) | MB | dur(s) | fp | etag | lastMod |
|---|---|---|---|--:|--:|--:|--:|:-:|:-:|
| American Scandal | pscrb.fm | content.production.cdn.art19.com | 2026-07-07 | 0.05 | 40.4 | 2505 | 20210 | – | – |
| The Charlie Kirk Show | pdst.fm | 27023.mc.tritondigital.com | 2026-07-07 | 0.21 | 34.5 | 4308 | 34773 | – | Y |
| Last Week in AI | rss.art19.com | content.production.cdn.art19.com | 2026-07-07 | 0.32 | 12.3 | 765 | 6154 | – | – |
| Fresh Air | prfx.byspotify.com | npr.simplecastaudio.com | 2026-07-06 | 0.56 | 48.7 | 3045 | 24575 | – | Y |
| Up First | prfx.byspotify.com | npr.simplecastaudio.com | 2026-07-06 | 0.93 | 16.0 | 1000 | 8053 | – | Y |
| SmartLess | dts.podtrac.com | stitcher.simplecastaudio.com | 2026-07-06 | 1.05 | 60.3 | 3771 | 30439 | – | Y |
| Morbid | dts.podtrac.com | stitcher.simplecastaudio.com | 2026-07-06 | 1.05 | 56.5 | 3531 | 28494 | – | Y |
| TED Business | sphinx.acast.com | stitcher2.acast.com | 2026-07-06 | 1.17 | 23.7 | 1479 | 11927 | Y | – |
| The Mel Robbins Podcast | dts.podtrac.com | stitcher.simplecastaudio.com | 2026-07-06 | 1.17 | 74.3 | 4644 | 37484 | – | Y |
| TechCrunch Daily Crunch | mgln.ai | dcs-spotify.megaphone.fm | 2026-07-04 | 2.92 | 7.4 | 458 | 3681 | – | – |
| Stuff You Should Know | podtrac.com | 27123.mc.tritondigital.com | 2026-07-04 | 2.97 | 59.8 | 3738 | 30167 | – | Y |
| Casefile True Crime | sphinx.acast.com | stitcher2.acast.com | 2026-07-04 | 2.98 | 101.8 | 6365 | 51388 | Y | – |
| Planet Money | tracking.swap.fm | npr.simplecastaudio.com | 2026-07-03 | 3.47 | 20.3 | 1271 | 10245 | – | Y |
| Radiolab | pscrb.fm | waaa.wnyc.org | 2026-07-03 | 3.76 | 44.3 | 2769 | 22340 | – | Y |
| Hard Fork | dts.podtrac.com | nyt.simplecastaudio.com | 2026-07-03 | 3.88 | 69.8 | 4360 | 35194 | – | Y |
| The Ezra Klein Show | dts.podtrac.com | nyt.simplecastaudio.com | 2026-07-03 | 3.97 | 105.1 | 6567 | 53017 | – | Y |
| The Daily Show: Ears Edition | pdst.fm | dcs.megaphone.fm | 2026-07-03 | 3.99 | 66.9 | 2769 | 22346 | – | – |
| Business Wars | pscrb.fm | content.production.cdn.art19.com | 2026-07-03 | 4.05 | 46.5 | 2865 | 23121 | Y | Y |
| On The Media | pscrb.fm | waaa.wnyc.org | 2026-07-03 | 4.05 | 52.6 | 3287 | 26529 | – | Y |
| The Rest Is Politics | pdst.fm | dcs-cached.megaphone.fm | 2026-07-02 | 4.38 | 125.9 | 3146 | 25386 | – | – |
| Pod Save America | clrtpod.com | d11untcg2uthr3.cloudfront.net | 2026-07-02 | 4.47 | 77.9 | 4828 | 38975 | – | – |
| Tech Won't Save Us | pscrb.fm | audio.buzzsprout.com | 2026-07-02 | 5.05 | 40.2 | 3349 | 27025 | Y | Y |
| The Nikki Glaser Podcast | podtrac.com | 26823.mc.tritondigital.com | 2026-07-01 | 5.48 | 1.1 | 63 | 488 | – | Y |
| Why Is This Happening? (Chris Hayes) | dts.podtrac.com | injector.simplecastaudio.com | 2026-06-30 | 6.48 | 47.6 | 2977 | 24027 | – | Y |

These 24 show identities reuse the rediff corpus that rotated **36/41** — i.e. this
is the known-DAI-carrying set, biased toward shows that demonstrably rotate.

### Skips / caveats

- **No hard skips** — all 24 feeds resolved with a fresh (<7 d) enclosure.
- **The Nikki Glaser Podcast** is a soft caveat: the show is effectively dormant
  (corpus's newest real ep was 2025-03), and its newest feed item at t0 is a
  **63-second cross-promo trailer** ("Introducing: Paul's Best Podcast", 1.1 MB).
  It was captured because it is genuinely fresh (5.5 d) and carries an enclosure,
  but a 63 s promo is a weak days-gap signal — **treat it as a low-weight / likely
  no-rotation control**, or drop it at analysis time (`--episode` filter excludes
  substrings; there is no include-except, so simply omit it from the aggregate).
- **ETag** seen on Acast + art19 + Buzzsprout; **Last-Modified** on most NPR/
  Simplecast/Omny/art19 responses. Both are recorded per episode and give a cheap
  server-side "did anything change" cross-check independent of the byte diff.

## Rotation clock note (read before interpreting the diff)

"Days-gap" here means the gap **between my two fetches** (t0 → re-fetch), which the
differ measures as `elapsedDays` from `t0FetchIso`. Because episodes were 0–6.5 d
old at t0, *time-since-publish* differs per episode and is recorded
(`publishAgeDaysAtT0`, `publishDate`) so a future analyst can control for it. DAI
stitching is server-side per-request, so re-fetching the **identical** enclosure URL
can still return rotated ad fills — that is exactly the mechanism under test.

## EXACT +2d / +7d re-fetch + diff recipe

The differ re-downloads each enclosure **one at a time**, deletes it immediately,
disk-guards at 3 GB, and aligns the **stored t0 fingerprint** (fpA) against the
freshly-fpcalc'd re-download (fpB) using the **verbatim** alignment core imported
from `scripts/l2f-dai-rediff.py` (`find_runs` / `merge_runs` / `gaps_in_b` /
`confidence_for_gap`). Same algorithm as the reference differ; only the t0 side is
a stored integer fingerprint instead of a re-fpcalc'd audio file.

> Why not `scripts/l2f-dai-rediff.py` directly? That tool aligns the fresh download
> against the **snapshot audio on disk**, which the disk constraint required us to
> delete. `l2f-daysgap-rediff.py` is the fingerprint-native equivalent for the
> audio-deleted workflow.

### +2d — run on/after 2026-07-09

```bash
cd /Users/dabrams/playhead
python3 scripts/l2f-daysgap-rediff.py \
  --t0 playhead-dogfood-diagnostics-daysgap-t0-2026-07-07.json
# -> writes playhead-dogfood-diagnostics-daysgap-diff-<UTCdate>.json (repo root, gitignored)
```

### +7d — run on/after 2026-07-14

```bash
cd /Users/dabrams/playhead
python3 scripts/l2f-daysgap-rediff.py \
  --t0 playhead-dogfood-diagnostics-daysgap-t0-2026-07-07.json
```

(With no `--t0`, the differ auto-selects the newest `daysgap-t0-*.json` at the repo
root, so the bare command works too. Passing it explicitly is safer if newer t0
cohorts get captured later.)

### Single-episode / spot-check / no-network verification

```bash
python3 scripts/l2f-daysgap-rediff.py --episode smartless        # one show (substring match, repeatable)
python3 scripts/l2f-daysgap-rediff.py --self-test                # synthetic align check, no net/fpcalc
```

### What the diff reports (per slot, by gap bucket, honest N + CI)

For every episode the diff records two rotation signals:

1. **`rotatedBytes`** — `freshSha256 != t0Sha256`. Cheap and necessary but *not*
   sufficient (a tracking-token swap or re-mux flips bytes without changing audio).
2. **`fingerprintRotated`** — alignment finds an **inserted-in-B** or
   **removed-in-A** segment ≥ 5 s (`MIN_AD_SECONDS`). **This is the honest
   ad-rotation number.** Each changed slot is emitted with `startSeconds`,
   `endSeconds`, `durationSeconds`, flanking-run seconds, and a confidence
   (`1 - exp(-min(flank)/60)`), identical to the reference differ.

Aggregates are bucketed by `elapsedDays` (`<1d / 1-3d / 4-8d / 9-21d / >21d`) with
per-bucket counts and **Wilson 95% CIs** for both the byte-rotation and
fingerprint-rotation rates, plus an overall rate. A +2d run lands in the `1-3d`
bucket; a +7d run lands in `4-8d`.

### Failure handling at +Nd

If an old enclosure URL 404s / times out / rotates off the CDN at re-fetch, that
episode is logged with `ok:false` + `error` and excluded from the rate denominator
(honest N shrinks rather than silently counting a miss as "no rotation"). Audio is
deleted on every path, including errors.
