# Cross-show syndication prototype — 2026-06-01

Proof of concept: can chromaprint + SimHash + LSH detect cross-show duplicate ad segments on the current corpus?

## Method

- fpcalc `-raw -length 7200` per episode (cached under `TestFixtures/Corpus/Snapshots/fingerprints/`).
- Window 30s, hop 5s at ~8.3 chromaprint frames/sec.
- SimHash 64-bit per window: textbook construction. Each of the 32 chromaprint input-bit positions has its own deterministic random ±1 weight vector of length 64; the hash is sign-of-sum across all frames and bits. Per-byte lookup tables make this Python-fast.
- LSH: 4 × 16-bit bands (any shared band value flags a candidate pair; full 64-bit Hamming is then computed exactly).
- Match thresholds: tight ≤ 6 bits, loose ≤ 12 bits.

## Corpus

- Episodes processed: **41** (full corpus).
- Distinct shows: **24**.
- Total windows indexed: **26005**.
- Wall time: **55.8s**.

## Cross-show match distribution (tight ≤ 6 bits)

| distinct_shows | windows |
|---|---|
| 2 | 1820 |
| 3 | 1543 |
| 4+ | 19680 |
| (any cross-show, total) | 23043 |

Informational — loose threshold (≤ 12 bits): 25705 windows have ≥1 cross-show match (out of 25948 with any loose match).

## Top 20 windows by distinct_shows (tight)

### 1. The Rest Is Politics — `the-rest-is-politics-2026-05-28-who-funds-reform-the-missing-millions` @ 12:04–12:34

- **distinct_shows:** 23  **match_count:** 800  **overlaps audit_priority=1:** no
- Matches:
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 48:40–49:10 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 68:19–68:49 (hamming=1)
  - Up First `up-first-2026-06-01-can-graham-platner-survive-another-contr` @ 5:14–5:44 (hamming=1)
  - SmartLess `smartless-2026-05-25-quot-nick-jonas-quot` @ 49:25–49:55 (hamming=1)
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 48:35–49:05 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 62:35–63:05 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev` @ 14:05–14:35 (hamming=2)
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 82:49–83:19 (hamming=2)
  - …and 792 more

  Spot-check: `ffplay -nodisp -autoexit -ss 724 -t 30 'the-rest-is-politics-2026-05-28-who-funds-reform-the-missing-millions.mp3'`

### 2. Fresh Air — `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 1:46–2:16

- **distinct_shows:** 23  **match_count:** 302  **overlaps audit_priority=1:** no
- Matches:
  - SmartLess `smartless-2026-05-18-quot-sting-quot` @ 7:40–8:10 (hamming=2)
  - SmartLess `smartless-2026-05-25-quot-nick-jonas-quot` @ 45:58–46:28 (hamming=2)
  - SmartLess `smartless-2026-05-25-quot-nick-jonas-quot` @ 62:35–63:05 (hamming=2)
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 45:17–45:47 (hamming=3)
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 45:22–45:52 (hamming=3)
  - Up First `up-first-2026-06-01-can-graham-platner-survive-another-contr` @ 19:49–20:19 (hamming=3)
  - The Mel Robbins Podcast `the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol` @ 29:06–29:36 (hamming=3)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 2:01–2:31 (hamming=3)
  - …and 294 more

  Spot-check: `ffplay -nodisp -autoexit -ss 106 -t 30 'fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros.mp3'`

### 3. The Nikki Glaser Podcast — `the-nikki-glaser-podcast-2025-03-07-516-nikki-the-famous-aunt-writers-room-m` @ 54:04–54:34

- **distinct_shows:** 23  **match_count:** 284  **overlaps audit_priority=1:** no
- Matches:
  - The Mel Robbins Podcast `the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol` @ 11:13–11:43 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t` @ 6:35–7:05 (hamming=2)
  - The Mel Robbins Podcast `the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol` @ 71:41–72:11 (hamming=2)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 17:43–18:13 (hamming=2)
  - Morbid `morbid-2026-05-29-may-bonus-episode-breaking-dawn-part-1` @ 8:26–8:56 (hamming=2)
  - The Mel Robbins Podcast `the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol` @ 9:37–10:07 (hamming=3)
  - The Mel Robbins Podcast `the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol` @ 11:18–11:48 (hamming=3)
  - The Mel Robbins Podcast `the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol` @ 21:25–21:55 (hamming=3)
  - …and 276 more

  Spot-check: `ffplay -nodisp -autoexit -ss 3244 -t 30 'the-nikki-glaser-podcast-2025-03-07-516-nikki-the-famous-aunt-writers-room-m.mp3'`

### 4. SmartLess — `smartless-2026-05-25-quot-nick-jonas-quot` @ 31:33–32:03

- **distinct_shows:** 23  **match_count:** 191  **overlaps audit_priority=1:** no
- Matches:
  - TechCrunch Daily Crunch `techcrunch-daily-crunch-2026-05-27-spotify-now-lets-you-stream-narrated-mag` @ 2:27–2:57 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 61:24–61:54 (hamming=2)
  - SmartLess `smartless-2026-05-25-quot-nick-jonas-quot` @ 31:22–31:52 (hamming=2)
  - SmartLess `smartless-2026-05-25-quot-nick-jonas-quot` @ 31:38–32:08 (hamming=2)
  - TechCrunch Daily Crunch `techcrunch-daily-crunch-2026-05-27-spotify-now-lets-you-stream-narrated-mag` @ 2:22–2:52 (hamming=2)
  - American Scandal `american-scandal-2026-05-26-chappaquiddick-the-weight-of-the-name-5` @ 33:29–33:59 (hamming=2)
  - The Rest Is Politics `the-rest-is-politics-2026-05-28-who-funds-reform-the-missing-millions` @ 27:45–28:15 (hamming=2)
  - The Mel Robbins Podcast `the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol` @ 57:11–57:41 (hamming=2)
  - …and 183 more

  Spot-check: `ffplay -nodisp -autoexit -ss 1893 -t 30 'smartless-2026-05-25-quot-nick-jonas-quot.mp3'`

### 5. Pod Save America — `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 82:49–83:19

- **distinct_shows:** 22  **match_count:** 990  **overlaps audit_priority=1:** no
- Matches:
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 21:30–22:00 (hamming=1)
  - The Charlie Kirk Show `the-charlie-kirk-show-2026-05-30-thoughtcrime-ep-129-spanking-your-kids-t` @ 3:48–4:18 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein` @ 4:18–4:48 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-07-516-nikki-the-famous-aunt-writers-room-m` @ 4:08–4:38 (hamming=1)
  - The Rest Is Politics `the-rest-is-politics-2026-05-28-who-funds-reform-the-missing-millions` @ 24:43–25:13 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi` @ 59:27–59:57 (hamming=2)
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 65:47–66:17 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t` @ 8:21–8:51 (hamming=2)
  - …and 982 more

  Spot-check: `ffplay -nodisp -autoexit -ss 4969 -t 30 'pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american.mp3'`

### 6. Radiolab — `radiolab-2026-05-29-this-american-roach` @ 27:50–28:20

- **distinct_shows:** 22  **match_count:** 978  **overlaps audit_priority=1:** no
- Matches:
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi` @ 47:29–47:59 (hamming=1)
  - The Charlie Kirk Show `the-charlie-kirk-show-2026-05-30-thoughtcrime-ep-129-spanking-your-kids-t` @ 29:26–29:56 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein` @ 8:51–9:21 (hamming=1)
  - The Daily Show: Ears Edition `the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve` @ 5:24–5:54 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 40:54–41:24 (hamming=1)
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 39:23–39:53 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 59:53–60:23 (hamming=2)
  - The Mel Robbins Podcast `the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol` @ 3:27–3:57 (hamming=2)
  - …and 970 more

  Spot-check: `ffplay -nodisp -autoexit -ss 1670 -t 30 'radiolab-2026-05-29-this-american-roach.mp3'`

### 7. Why Is This Happening? The Chris Hayes Podcast — `why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein` @ 4:18–4:48

- **distinct_shows:** 22  **match_count:** 935  **overlaps audit_priority=1:** no
- Matches:
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 82:49–83:19 (hamming=1)
  - SmartLess `smartless-2026-05-11-quot-kareem-rahma-quot` @ 19:24–19:54 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-19-the-ai-end-game-two-year-olds-vs-ai-with` @ 8:36–9:06 (hamming=1)
  - The Daily Show: Ears Edition `the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve` @ 5:04–5:34 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit` @ 33:34–34:04 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi` @ 42:00–42:30 (hamming=1)
  - The Mel Robbins Podcast `the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol` @ 3:27–3:57 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev` @ 32:53–33:23 (hamming=2)
  - …and 927 more

  Spot-check: `ffplay -nodisp -autoexit -ss 258 -t 30 'why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein.mp3'`

### 8. The Nikki Glaser Podcast — `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 40:54–41:24

- **distinct_shows:** 22  **match_count:** 867  **overlaps audit_priority=1:** no
- Matches:
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 39:23–39:53 (hamming=0)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 59:53–60:23 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t` @ 37:42–38:12 (hamming=1)
  - Stuff You Should Know `stuff-you-should-know-2026-05-30-selects-did-shakespeare-really-write-all` @ 21:15–21:45 (hamming=1)
  - Stuff You Should Know `stuff-you-should-know-2026-05-30-selects-did-shakespeare-really-write-all` @ 42:25–42:55 (hamming=1)
  - Radiolab `radiolab-2026-05-29-this-american-roach` @ 27:50–28:20 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein` @ 9:01–9:31 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi` @ 47:29–47:59 (hamming=2)
  - …and 859 more

  Spot-check: `ffplay -nodisp -autoexit -ss 2454 -t 30 'the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os.mp3'`

### 9. Pod Save America — `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 39:23–39:53

- **distinct_shows:** 22  **match_count:** 867  **overlaps audit_priority=1:** no
- Matches:
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 40:54–41:24 (hamming=0)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 59:53–60:23 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t` @ 37:42–38:12 (hamming=1)
  - Stuff You Should Know `stuff-you-should-know-2026-05-30-selects-did-shakespeare-really-write-all` @ 21:15–21:45 (hamming=1)
  - Stuff You Should Know `stuff-you-should-know-2026-05-30-selects-did-shakespeare-really-write-all` @ 42:25–42:55 (hamming=1)
  - Radiolab `radiolab-2026-05-29-this-american-roach` @ 27:50–28:20 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein` @ 9:01–9:31 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi` @ 47:29–47:59 (hamming=2)
  - …and 859 more

  Spot-check: `ffplay -nodisp -autoexit -ss 2363 -t 30 'pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american.mp3'`

### 10. Fresh Air — `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 21:30–22:00

- **distinct_shows:** 22  **match_count:** 852  **overlaps audit_priority=1:** no
- Matches:
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 82:49–83:19 (hamming=1)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 27:30–28:00 (hamming=1)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 27:35–28:05 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit` @ 43:26–43:56 (hamming=1)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 11:33–12:03 (hamming=2)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 21:20–21:50 (hamming=2)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 31:48–32:18 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t` @ 61:09–61:39 (hamming=2)
  - …and 844 more

  Spot-check: `ffplay -nodisp -autoexit -ss 1290 -t 30 'fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros.mp3'`

### 11. SmartLess — `smartless-2026-05-11-quot-kareem-rahma-quot` @ 9:52–10:22

- **distinct_shows:** 22  **match_count:** 838  **overlaps audit_priority=1:** no
- Matches:
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 39:33–40:03 (hamming=1)
  - SmartLess `smartless-2026-05-11-quot-kareem-rahma-quot` @ 27:25–27:55 (hamming=1)
  - SmartLess `smartless-2026-05-11-quot-kareem-rahma-quot` @ 30:07–30:37 (hamming=1)
  - SmartLess `smartless-2026-05-25-quot-nick-jonas-quot` @ 49:25–49:55 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein` @ 22:31–23:01 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-07-516-nikki-the-famous-aunt-writers-room-m` @ 32:33–33:03 (hamming=1)
  - The Daily Show: Ears Edition `the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve` @ 14:20–14:50 (hamming=1)
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 82:49–83:19 (hamming=2)
  - …and 830 more

  Spot-check: `ffplay -nodisp -autoexit -ss 592 -t 30 'smartless-2026-05-11-quot-kareem-rahma-quot.mp3'`

### 12. The Charlie Kirk Show — `the-charlie-kirk-show-2026-05-30-thoughtcrime-ep-129-spanking-your-kids-t` @ 29:26–29:56

- **distinct_shows:** 22  **match_count:** 832  **overlaps audit_priority=1:** no
- Matches:
  - The Mel Robbins Podcast `the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol` @ 3:33–4:03 (hamming=1)
  - Radiolab `radiolab-2026-05-29-this-american-roach` @ 6:40–7:10 (hamming=1)
  - Radiolab `radiolab-2026-05-29-this-american-roach` @ 27:50–28:20 (hamming=1)
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 33:34–34:04 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi` @ 47:29–47:59 (hamming=2)
  - SmartLess `smartless-2026-05-21-quot-re-release-nate-bargatze-quot` @ 35:30–36:00 (hamming=2)
  - SmartLess `smartless-2026-05-21-quot-re-release-nate-bargatze-quot` @ 37:01–37:31 (hamming=2)
  - SmartLess `smartless-2026-05-18-quot-sting-quot` @ 22:56–23:26 (hamming=2)
  - …and 824 more

  Spot-check: `ffplay -nodisp -autoexit -ss 1766 -t 30 'the-charlie-kirk-show-2026-05-30-thoughtcrime-ep-129-spanking-your-kids-t.mp3'`

### 13. Why Is This Happening? The Chris Hayes Podcast — `why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit` @ 43:26–43:56

- **distinct_shows:** 22  **match_count:** 805  **overlaps audit_priority=1:** no
- Matches:
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 27:30–28:00 (hamming=0)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 21:30–22:00 (hamming=1)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 31:48–32:18 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t` @ 61:09–61:39 (hamming=1)
  - Stuff You Should Know `stuff-you-should-know-2026-05-30-selects-did-shakespeare-really-write-all` @ 33:04–33:34 (hamming=1)
  - Radiolab `radiolab-2026-05-29-this-american-roach` @ 20:40–21:10 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit` @ 31:58–32:28 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit` @ 34:20–34:50 (hamming=1)
  - …and 797 more

  Spot-check: `ffplay -nodisp -autoexit -ss 2606 -t 30 'why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit.mp3'`

### 14. Fresh Air — `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 27:30–28:00

- **distinct_shows:** 22  **match_count:** 805  **overlaps audit_priority=1:** no
- Matches:
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit` @ 43:26–43:56 (hamming=0)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 21:30–22:00 (hamming=1)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 31:48–32:18 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t` @ 61:09–61:39 (hamming=1)
  - Stuff You Should Know `stuff-you-should-know-2026-05-30-selects-did-shakespeare-really-write-all` @ 33:04–33:34 (hamming=1)
  - Radiolab `radiolab-2026-05-29-this-american-roach` @ 20:40–21:10 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit` @ 31:58–32:28 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit` @ 34:20–34:50 (hamming=1)
  - …and 797 more

  Spot-check: `ffplay -nodisp -autoexit -ss 1650 -t 30 'fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros.mp3'`

### 15. Why Is This Happening? The Chris Hayes Podcast — `why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein` @ 9:01–9:31

- **distinct_shows:** 22  **match_count:** 798  **overlaps audit_priority=1:** no
- Matches:
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev` @ 9:12–9:42 (hamming=1)
  - Radiolab `radiolab-2026-05-29-this-american-roach` @ 22:16–22:46 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein` @ 24:43–25:13 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 40:54–41:24 (hamming=1)
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 39:23–39:53 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 59:53–60:23 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev` @ 13:45–14:15 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t` @ 8:26–8:56 (hamming=2)
  - …and 790 more

  Spot-check: `ffplay -nodisp -autoexit -ss 541 -t 30 'why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein.mp3'`

### 16. Why Is This Happening? The Chris Hayes Podcast — `why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi` @ 13:09–13:39

- **distinct_shows:** 22  **match_count:** 778  **overlaps audit_priority=1:** no
- Matches:
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi` @ 59:27–59:57 (hamming=2)
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 82:49–83:19 (hamming=2)
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 82:54–83:24 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t` @ 8:21–8:51 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t` @ 8:31–9:01 (hamming=2)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 24:12–24:42 (hamming=2)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 46:23–46:53 (hamming=2)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 46:28–46:58 (hamming=2)
  - …and 770 more

  Spot-check: `ffplay -nodisp -autoexit -ss 789 -t 30 'why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi.mp3'`

### 17. Why Is This Happening? The Chris Hayes Podcast — `why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit` @ 33:39–34:09

- **distinct_shows:** 22  **match_count:** 727  **overlaps audit_priority=1:** no
- Matches:
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit` @ 38:22–38:52 (hamming=1)
  - Why Is This Happening? The Chris Hayes Podcast `why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi` @ 0:25–0:55 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 56:40–57:10 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 60:08–60:38 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-13-517-the-glaser-exit` @ 7:51–8:21 (hamming=2)
  - SmartLess `smartless-2026-05-21-quot-re-release-nate-bargatze-quot` @ 38:38–39:08 (hamming=2)
  - SmartLess `smartless-2026-05-21-quot-re-release-nate-bargatze-quot` @ 38:43–39:13 (hamming=2)
  - SmartLess `smartless-2026-05-11-quot-kareem-rahma-quot` @ 12:29–12:59 (hamming=2)
  - …and 719 more

  Spot-check: `ffplay -nodisp -autoexit -ss 2019 -t 30 'why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit.mp3'`

### 18. SmartLess — `smartless-2026-05-11-quot-kareem-rahma-quot` @ 4:53–5:23

- **distinct_shows:** 22  **match_count:** 722  **overlaps audit_priority=1:** no
- Matches:
  - SmartLess `smartless-2026-05-21-quot-re-release-nate-bargatze-quot` @ 51:47–52:17 (hamming=1)
  - SmartLess `smartless-2026-05-21-quot-re-release-nate-bargatze-quot` @ 51:52–52:22 (hamming=1)
  - SmartLess `smartless-2026-05-25-quot-nick-jonas-quot` @ 46:13–46:43 (hamming=1)
  - The Mel Robbins Podcast `the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol` @ 3:27–3:57 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev` @ 32:53–33:23 (hamming=2)
  - Up First `up-first-2026-06-01-can-graham-platner-survive-another-contr` @ 34:55–35:25 (hamming=2)
  - SmartLess `smartless-2026-05-21-quot-re-release-nate-bargatze-quot` @ 42:30–43:00 (hamming=2)
  - SmartLess `smartless-2026-05-21-quot-re-release-nate-bargatze-quot` @ 51:57–52:27 (hamming=2)
  - …and 714 more

  Spot-check: `ffplay -nodisp -autoexit -ss 293 -t 30 'smartless-2026-05-11-quot-kareem-rahma-quot.mp3'`

### 19. Stuff You Should Know — `stuff-you-should-know-2026-05-30-selects-did-shakespeare-really-write-all` @ 21:15–21:45

- **distinct_shows:** 22  **match_count:** 716  **overlaps audit_priority=1:** no
- Matches:
  - Stuff You Should Know `stuff-you-should-know-2026-05-30-selects-did-shakespeare-really-write-all` @ 42:25–42:55 (hamming=0)
  - The Daily Show: Ears Edition `the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve` @ 6:25–6:55 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 40:54–41:24 (hamming=1)
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 39:23–39:53 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 59:53–60:23 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev` @ 13:40–14:10 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev` @ 16:32–17:02 (hamming=2)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 2:12–2:42 (hamming=2)
  - …and 708 more

  Spot-check: `ffplay -nodisp -autoexit -ss 1275 -t 30 'stuff-you-should-know-2026-05-30-selects-did-shakespeare-really-write-all.mp3'`

### 20. Stuff You Should Know — `stuff-you-should-know-2026-05-30-selects-did-shakespeare-really-write-all` @ 42:25–42:55

- **distinct_shows:** 22  **match_count:** 716  **overlaps audit_priority=1:** no
- Matches:
  - Stuff You Should Know `stuff-you-should-know-2026-05-30-selects-did-shakespeare-really-write-all` @ 21:15–21:45 (hamming=0)
  - The Daily Show: Ears Edition `the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve` @ 6:25–6:55 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 40:54–41:24 (hamming=1)
  - Pod Save America `pod-save-america-2026-05-31-what-does-it-mean-to-be-an-american` @ 39:23–39:53 (hamming=1)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os` @ 59:53–60:23 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev` @ 13:40–14:10 (hamming=2)
  - The Nikki Glaser Podcast `the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev` @ 16:32–17:02 (hamming=2)
  - Fresh Air `fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros` @ 2:12–2:42 (hamming=2)
  - …and 708 more

  Spot-check: `ffplay -nodisp -autoexit -ss 2545 -t 30 'stuff-you-should-know-2026-05-30-selects-did-shakespeare-really-write-all.mp3'`

## SimHash noise floor (diagnostic)

Hamming distance distribution across 5000 random cross-episode window pairs:

- Mean: **19.2** bits (uniform-random 64-bit hashes would average 32).
- Random pairs within tight (≤6): 18/5000 (0.36%).
- Random pairs within loose (≤12): 613/5000 (12.26%).

Histogram (low end):

| hamming | random-pair count | % |
|---|---|---|
| 0 | 0 | 0.00% |
| 1 | 0 | 0.00% |
| 2 | 0 | 0.00% |
| 3 | 0 | 0.00% |
| 4 | 3 | 0.06% |
| 5 | 1 | 0.02% |
| 6 | 14 | 0.28% |
| 7 | 23 | 0.46% |
| 8 | 49 | 0.98% |
| 9 | 74 | 1.48% |
| 10 | 99 | 1.98% |
| 11 | 139 | 2.78% |
| 12 | 211 | 4.22% |
| 13 | 208 | 4.16% |

If the noise floor mean is far below 32, the SimHash on chromaprint frames is picking up a structural "this is speech audio" signature shared by every podcast window, not the per-segment content. Treat sub-threshold matches with extreme suspicion when this is the case.

## Audit cross-reference

- Episodes with any `audit_priority=1` span: 4  (total spans: 8).
- Cross-show tight windows that overlap any audit_priority=1 span: **91** of 23043 (0%).

### Audit-vs-random discrimination

For each known `audit_priority=1` ad span, look up the SimHash window closest to its midpoint and find its best (lowest) cross-show Hamming distance. Compare against the same metric computed for an equal-sized random sample of windows. If syndication is working, audit-p1 spans should have *systematically lower* best-cross-show distances than random windows.

- **audit_priority=1:** n=8 mean=4.9 median=5 min=1 max=9
- **random control:** n=8 mean=3.5 median=4 min=1 max=5

Audit-p1 windows are statistically indistinguishable from random windows on this metric (Δ mean ≈ +1.4 bits). The SimHash is not discriminating known ads from arbitrary speech at this corpus size.

## Honest interpretation

**Noise floor dominates.** The mean Hamming distance between random cross-episode window pairs is **19.2** bits — well below the 32-bit expectation for uniformly-random 64-bit hashes. That means the SimHash of chromaprint frames is picking up a global "this is podcast speech" signature shared by nearly every window in the corpus.

Consequence: matches at hamming ≤ 6 are not evidence of audio reuse. They're evidence that two windows both contain speech with similar voicing/dynamic-range characteristics, which is true for almost every window pair in this corpus.

**The audit-vs-random test shows no discrimination** (Δ = +1.4 bits, |Δ| < 2). On the 8 audit-p1 spans we could test, their best cross-show match is no closer than a random window's best cross-show match. The syndication signal — if it exists in this corpus — is too weak to extract with this fingerprint at this N.

**Verdict on the hypothesis:** chromaprint+SimHash, as built here, does NOT cleanly identify cross-show duplicate ads on this 41-episode corpus. The top-N table is dominated by false positives from generic speech-vs-speech similarity. Before wiring syndication into production, we'd need at least one of: (a) a different acoustic fingerprint less biased on voice (e.g., MFCC + DTW; openl3 embeddings), (b) a much larger corpus where real syndicated ads recur enough to stand out above the bias floor, or (c) a tighter match criterion (e.g., contiguous-run requirements on raw chromaprint frames, the way l2f-dai-rediff.py aligns duplicate episode-pair regions).

## Caveats

- **N is small.** 41 episodes across 24 shows. A programmatic ad needs at least two episodes from different shows in the corpus during the same campaign window to show up at all. Most ad campaigns have a longer reach than our sample.
- **chromaprint is not tuned for ads.** It was built for music identification; speech with similar voicing or background music may collide. Conversely, the same ad re-encoded at a different bitrate may shift bits enough to miss our tight threshold.
- **SimHash false positives.** Our 64-bit SimHash on 32-bit chromaprint frames is a hand-rolled summary, not a battle-tested audio fingerprint. The vote-sum construction is locality-preserving for stable inputs but can collide on near-uniform random inputs.
- **Window/hop tradeoff.** 30s/5s = ~10× redundancy per window. An ad that's 25s long can still be caught at the edges, but a 5-second house promo will be drowned by surrounding content.
- **Length cap.** fpcalc was capped at 7200s to keep runtime bounded; episodes longer than that are partially sampled (head only).
- **Same-show duplicates excluded from `distinct_shows`.** A back-catalog episode that re-uses a 30s opening from the same show won't count here. That's intentional: syndication evidence requires *cross-show* recurrence to be a useful ad signal.
