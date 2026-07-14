# Audit-priority queue (70 spans)

Rediff-only auto-promotions (R3, ≥20s). These are high-precision in theory
but unverified — opportunistically spot-check 1–2 minutes each to flag mistakes
before they bias activation evaluations.

## Workflow
For each row: run the ffplay command. If it sounds like an ad, mark ✓.
If it sounds like host content, run:
  `scripts/l2f-flag-false-promote.py <eid-prefix> <start_seconds> --reason='...'`
This removes the window from the annotation and appends to
`TestFixtures/Corpus/Snapshots/audit-rejects.jsonl`.

| # | Episode | Show | Span | Dur | ffplay |
|---|---------|------|------|-----|--------|
| 1 | `american-scandal-2026-05-26-chappaqu…` | American Scandal | 91-154 | 63s | `ffplay -nodisp -autoexit -ss 91 -t 63 'american-scandal-2026-05-26-chappaquiddick-the-weight-of-the-name-5.mp3'` |
| 2 | `american-scandal-2026-05-26-chappaqu…` | American Scandal | 876-923 | 47s | `ffplay -nodisp -autoexit -ss 876 -t 47 'american-scandal-2026-05-26-chappaquiddick-the-weight-of-the-name-5.mp3'` |
| 3 | `american-scandal-2026-05-26-chappaqu…` | American Scandal | 924-947 | 23s | `ffplay -nodisp -autoexit -ss 924 -t 23 'american-scandal-2026-05-26-chappaquiddick-the-weight-of-the-name-5.mp3'` |
| 4 | `american-scandal-2026-05-26-chappaqu…` | American Scandal | 1830-1854 | 24s | `ffplay -nodisp -autoexit -ss 1830 -t 24 'american-scandal-2026-05-26-chappaquiddick-the-weight-of-the-name-5.mp3'` |
| 5 | `business-wars-2026-05-28-f1-vs-nasca…` | Business Wars | 132-195 | 63s | `ffplay -nodisp -autoexit -ss 132 -t 63 'business-wars-2026-05-28-f1-vs-nascar-f1-roars-into-the-u-s-4.mp3'` |
| 6 | `business-wars-2026-05-28-f1-vs-nasca…` | Business Wars | 1864-1885 | 21s | `ffplay -nodisp -autoexit -ss 1864 -t 21 'business-wars-2026-05-28-f1-vs-nascar-f1-roars-into-the-u-s-4.mp3'` |
| 7 | `fresh-air-2026-05-30-best-of-borough…` | Fresh Air | 1179-1269 | 90s | `ffplay -nodisp -autoexit -ss 1179 -t 90 'fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros.mp3'` |
| 8 | `fresh-air-2026-05-30-best-of-borough…` | Fresh Air | 1994-2071 | 77s | `ffplay -nodisp -autoexit -ss 1994 -t 77 'fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros.mp3'` |
| 9 | `fresh-air-2026-05-30-best-of-borough…` | Fresh Air | 3163-3196 | 34s | `ffplay -nodisp -autoexit -ss 3163 -t 34 'fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros.mp3'` |
| 10 | `hard-fork-2026-05-29-interesting-tim…` | Hard Fork | 0-79 | 79s | `ffplay -nodisp -autoexit -ss 0 -t 79 'hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi.mp3'` |
| 11 | `hard-fork-2026-05-29-interesting-tim…` | Hard Fork | 1294-1350 | 56s | `ffplay -nodisp -autoexit -ss 1294 -t 56 'hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi.mp3'` |
| 12 | `hard-fork-2026-05-29-interesting-tim…` | Hard Fork | 1351-1388 | 37s | `ffplay -nodisp -autoexit -ss 1351 -t 37 'hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi.mp3'` |
| 13 | `hard-fork-2026-05-29-interesting-tim…` | Hard Fork | 2523-2553 | 30s | `ffplay -nodisp -autoexit -ss 2523 -t 30 'hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi.mp3'` |
| 14 | `hard-fork-2026-05-29-interesting-tim…` | Hard Fork | 2582-2615 | 33s | `ffplay -nodisp -autoexit -ss 2582 -t 33 'hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi.mp3'` |
| 15 | `hard-fork-2026-05-29-interesting-tim…` | Hard Fork | 3670-3700 | 30s | `ffplay -nodisp -autoexit -ss 3670 -t 30 'hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi.mp3'` |
| 16 | `hard-fork-2026-05-29-interesting-tim…` | Hard Fork | 3728-3761 | 33s | `ffplay -nodisp -autoexit -ss 3728 -t 33 'hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi.mp3'` |
| 17 | `morbid-2026-05-21-the-matamoros-devi…` | Morbid | 2488-2571 | 84s | `ffplay -nodisp -autoexit -ss 2488 -t 84 'morbid-2026-05-21-the-matamoros-devil-murders-part-1.mp3'` |
| 18 | `morbid-2026-05-25-the-matamoros-devi…` | Morbid | 1105-1189 | 84s | `ffplay -nodisp -autoexit -ss 1105 -t 84 'morbid-2026-05-25-the-matamoros-devil-murders-part-2.mp3'` |
| 19 | `morbid-2026-05-25-the-matamoros-devi…` | Morbid | 2442-2500 | 58s | `ffplay -nodisp -autoexit -ss 2442 -t 58 'morbid-2026-05-25-the-matamoros-devil-murders-part-2.mp3'` |
| 20 | `morbid-2026-05-28-listener-tales-110…` | Morbid | 27-74 | 47s | `ffplay -nodisp -autoexit -ss 27 -t 47 'morbid-2026-05-28-listener-tales-110-playdates-with-the-pa.mp3'` |
| 21 | `on-the-media-2026-05-29-trump-sued-h…` | On The Media | 122-150 | 27s | `ffplay -nodisp -autoexit -ss 122 -t 27 'on-the-media-2026-05-29-trump-sued-himself-and-settled-for-a-1-8.mp3'` |
| 22 | `on-the-media-2026-05-29-trump-sued-h…` | On The Media | 1040-1168 | 128s | `ffplay -nodisp -autoexit -ss 1040 -t 128 'on-the-media-2026-05-29-trump-sued-himself-and-settled-for-a-1-8.mp3'` |
| 23 | `on-the-media-2026-05-29-trump-sued-h…` | On The Media | 2382-2449 | 67s | `ffplay -nodisp -autoexit -ss 2382 -t 67 'on-the-media-2026-05-29-trump-sued-himself-and-settled-for-a-1-8.mp3'` |
| 24 | `planet-money-2026-05-29-the-sneaky-w…` | Planet Money | 1456-1488 | 32s | `ffplay -nodisp -autoexit -ss 1456 -t 32 'planet-money-2026-05-29-the-sneaky-way-companies-get-new-chemica.mp3'` |
| 25 | `smartless-2026-05-11-quot-kareem-rah…` | SmartLess | 13-109 | 96s | `ffplay -nodisp -autoexit -ss 13 -t 96 'smartless-2026-05-11-quot-kareem-rahma-quot.mp3'` |
| 26 | `smartless-2026-05-11-quot-kareem-rah…` | SmartLess | 2589-2661 | 72s | `ffplay -nodisp -autoexit -ss 2589 -t 72 'smartless-2026-05-11-quot-kareem-rahma-quot.mp3'` |
| 27 | `smartless-2026-05-18-quot-sting-quot` | SmartLess | 28-91 | 63s | `ffplay -nodisp -autoexit -ss 28 -t 63 'smartless-2026-05-18-quot-sting-quot.mp3'` |
| 28 | `smartless-2026-05-18-quot-sting-quot` | SmartLess | 1398-1447 | 49s | `ffplay -nodisp -autoexit -ss 1398 -t 49 'smartless-2026-05-18-quot-sting-quot.mp3'` |
| 29 | `smartless-2026-05-18-quot-sting-quot` | SmartLess | 2566-2629 | 63s | `ffplay -nodisp -autoexit -ss 2566 -t 63 'smartless-2026-05-18-quot-sting-quot.mp3'` |
| 30 | `smartless-2026-05-21-quot-re-release…` | SmartLess | 0-55 | 55s | `ffplay -nodisp -autoexit -ss 0 -t 55 'smartless-2026-05-21-quot-re-release-nate-bargatze-quot.mp3'` |
| 31 | `smartless-2026-05-25-quot-nick-jonas…` | SmartLess | 2703-2737 | 35s | `ffplay -nodisp -autoexit -ss 2703 -t 35 'smartless-2026-05-25-quot-nick-jonas-quot.mp3'` |
| 32 | `smartless-2026-05-25-quot-nick-jonas…` | SmartLess | 3970-4035 | 65s | `ffplay -nodisp -autoexit -ss 3970 -t 65 'smartless-2026-05-25-quot-nick-jonas-quot.mp3'` |
| 33 | `the-daily-show-ears-edition-2026-05-…` | The Daily Show: Ears Edi | 718-739 | 20s | `ffplay -nodisp -autoexit -ss 718 -t 20 'the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve.mp3'` |
| 34 | `the-daily-show-ears-edition-2026-05-…` | The Daily Show: Ears Edi | 880-905 | 25s | `ffplay -nodisp -autoexit -ss 880 -t 25 'the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve.mp3'` |
| 35 | `the-daily-show-ears-edition-2026-05-…` | The Daily Show: Ears Edi | 1078-1111 | 33s | `ffplay -nodisp -autoexit -ss 1078 -t 33 'the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve.mp3'` |
| 36 | `the-daily-show-ears-edition-2026-05-…` | The Daily Show: Ears Edi | 1694-1726 | 32s | `ffplay -nodisp -autoexit -ss 1694 -t 32 'the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve.mp3'` |
| 37 | `the-daily-show-ears-edition-2026-05-…` | The Daily Show: Ears Edi | 2237-2268 | 31s | `ffplay -nodisp -autoexit -ss 2237 -t 31 'the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve.mp3'` |
| 38 | `the-ezra-klein-show-2026-05-29-does-…` | The Ezra Klein Show | 0-30 | 30s | `ffplay -nodisp -autoexit -ss 0 -t 30 'the-ezra-klein-show-2026-05-29-does-trump-want-to-lose-the-midterms.mp3'` |
| 39 | `the-mel-robbins-podcast-2026-06-01-h…` | The Mel Robbins Podcast | 199-231 | 32s | `ffplay -nodisp -autoexit -ss 199 -t 32 'the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol.mp3'` |
| 40 | `the-mel-robbins-podcast-2026-06-01-h…` | The Mel Robbins Podcast | 1888-1922 | 34s | `ffplay -nodisp -autoexit -ss 1888 -t 34 'the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol.mp3'` |
| 41 | `the-mel-robbins-podcast-2026-06-01-h…` | The Mel Robbins Podcast | 1924-2004 | 80s | `ffplay -nodisp -autoexit -ss 1924 -t 80 'the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol.mp3'` |
| 42 | `the-mel-robbins-podcast-2026-06-01-h…` | The Mel Robbins Podcast | 2819-2851 | 33s | `ffplay -nodisp -autoexit -ss 2819 -t 33 'the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol.mp3'` |
| 43 | `the-mel-robbins-podcast-2026-06-01-h…` | The Mel Robbins Podcast | 4914-4951 | 37s | `ffplay -nodisp -autoexit -ss 4914 -t 37 'the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol.mp3'` |
| 44 | `the-mel-robbins-podcast-2026-06-01-h…` | The Mel Robbins Podcast | 4953-4981 | 28s | `ffplay -nodisp -autoexit -ss 4953 -t 28 'the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol.mp3'` |
| 45 | `the-nikki-glaser-podcast-2025-02-27-…` | The Nikki Glaser Podcast | 3200-3286 | 85s | `ffplay -nodisp -autoexit -ss 3200 -t 85 'the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t.mp3'` |
| 46 | `the-nikki-glaser-podcast-2025-02-28-…` | The Nikki Glaser Podcast | 3096-3128 | 32s | `ffplay -nodisp -autoexit -ss 3096 -t 32 'the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev.mp3'` |
| 47 | `the-nikki-glaser-podcast-2025-02-28-…` | The Nikki Glaser Podcast | 3156-3225 | 69s | `ffplay -nodisp -autoexit -ss 3156 -t 69 'the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev.mp3'` |
| 48 | `the-nikki-glaser-podcast-2025-03-06-…` | The Nikki Glaser Podcast | 1235-1297 | 62s | `ffplay -nodisp -autoexit -ss 1235 -t 62 'the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os.mp3'` |
| 49 | `the-nikki-glaser-podcast-2025-03-06-…` | The Nikki Glaser Podcast | 2924-3015 | 91s | `ffplay -nodisp -autoexit -ss 2924 -t 91 'the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os.mp3'` |
| 50 | `the-nikki-glaser-podcast-2025-03-06-…` | The Nikki Glaser Podcast | 3016-3071 | 55s | `ffplay -nodisp -autoexit -ss 3016 -t 55 'the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os.mp3'` |
| 51 | `the-nikki-glaser-podcast-2025-03-07-…` | The Nikki Glaser Podcast | 63-125 | 62s | `ffplay -nodisp -autoexit -ss 63 -t 62 'the-nikki-glaser-podcast-2025-03-07-516-nikki-the-famous-aunt-writers-room-m.mp3'` |
| 52 | `the-nikki-glaser-podcast-2025-03-07-…` | The Nikki Glaser Podcast | 1524-1600 | 76s | `ffplay -nodisp -autoexit -ss 1524 -t 76 'the-nikki-glaser-podcast-2025-03-07-516-nikki-the-famous-aunt-writers-room-m.mp3'` |
| 53 | `the-nikki-glaser-podcast-2025-03-13-…` | The Nikki Glaser Podcast | 1694-1726 | 31s | `ffplay -nodisp -autoexit -ss 1694 -t 31 'the-nikki-glaser-podcast-2025-03-13-517-the-glaser-exit.mp3'` |
| 54 | `the-nikki-glaser-podcast-2025-03-13-…` | The Nikki Glaser Podcast | 1784-1804 | 20s | `ffplay -nodisp -autoexit -ss 1784 -t 20 'the-nikki-glaser-podcast-2025-03-13-517-the-glaser-exit.mp3'` |
| 55 | `the-nikki-glaser-podcast-2025-03-13-…` | The Nikki Glaser Podcast | 2677-2709 | 31s | `ffplay -nodisp -autoexit -ss 2677 -t 31 'the-nikki-glaser-podcast-2025-03-13-517-the-glaser-exit.mp3'` |
| 56 | `the-nikki-glaser-podcast-2025-03-13-…` | The Nikki Glaser Podcast | 2767-2932 | 165s | `ffplay -nodisp -autoexit -ss 2767 -t 165 'the-nikki-glaser-podcast-2025-03-13-517-the-glaser-exit.mp3'` |
| 57 | `up-first-2026-06-01-can-graham-platn…` | Up First | 279-365 | 86s | `ffplay -nodisp -autoexit -ss 279 -t 86 'up-first-2026-06-01-can-graham-platner-survive-another-contr.mp3'` |
| 58 | `up-first-2026-06-01-can-graham-platn…` | Up First | 1993-2032 | 39s | `ffplay -nodisp -autoexit -ss 1993 -t 39 'up-first-2026-06-01-can-graham-platner-survive-another-contr.mp3'` |
| 59 | `why-is-this-happening-the-chris-haye…` | Why Is This Happening? T | 0-48 | 48s | `ffplay -nodisp -autoexit -ss 0 -t 48 'why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi.mp3'` |
| 60 | `why-is-this-happening-the-chris-haye…` | Why Is This Happening? T | 3110-3220 | 110s | `ffplay -nodisp -autoexit -ss 3110 -t 110 'why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi.mp3'` |
| 61 | `why-is-this-happening-the-chris-haye…` | Why Is This Happening? T | 3771-3792 | 21s | `ffplay -nodisp -autoexit -ss 3771 -t 21 'why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi.mp3'` |
| 62 | `why-is-this-happening-the-chris-haye…` | Why Is This Happening? T | 0-50 | 50s | `ffplay -nodisp -autoexit -ss 0 -t 50 'why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit.mp3'` |
| 63 | `why-is-this-happening-the-chris-haye…` | Why Is This Happening? T | 2512-2591 | 79s | `ffplay -nodisp -autoexit -ss 2512 -t 79 'why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit.mp3'` |
| 64 | `why-is-this-happening-the-chris-haye…` | Why Is This Happening? T | 0-30 | 30s | `ffplay -nodisp -autoexit -ss 0 -t 30 'why-is-this-happening-the-chris-hayes-po-2026-05-19-the-ai-end-game-two-year-olds-vs-ai-with.mp3'` |
| 65 | `why-is-this-happening-the-chris-haye…` | Why Is This Happening? T | 612-642 | 30s | `ffplay -nodisp -autoexit -ss 612 -t 30 'why-is-this-happening-the-chris-hayes-po-2026-05-19-the-ai-end-game-two-year-olds-vs-ai-with.mp3'` |
| 66 | `why-is-this-happening-the-chris-haye…` | Why Is This Happening? T | 747-810 | 63s | `ffplay -nodisp -autoexit -ss 747 -t 63 'why-is-this-happening-the-chris-hayes-po-2026-05-19-the-ai-end-game-two-year-olds-vs-ai-with.mp3'` |
| 67 | `why-is-this-happening-the-chris-haye…` | Why Is This Happening? T | 1499-1577 | 78s | `ffplay -nodisp -autoexit -ss 1499 -t 78 'why-is-this-happening-the-chris-hayes-po-2026-05-19-the-ai-end-game-two-year-olds-vs-ai-with.mp3'` |
| 68 | `why-is-this-happening-the-chris-haye…` | Why Is This Happening? T | 28-58 | 30s | `ffplay -nodisp -autoexit -ss 28 -t 30 'why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein.mp3'` |
| 69 | `why-is-this-happening-the-chris-haye…` | Why Is This Happening? T | 1905-1933 | 28s | `ffplay -nodisp -autoexit -ss 1905 -t 28 'why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein.mp3'` |
| 70 | `why-is-this-happening-the-chris-haye…` | Why Is This Happening? T | 84-195 | 111s | `ffplay -nodisp -autoexit -ss 84 -t 111 'why-is-this-happening-the-chris-hayes-po-2026-05-26-the-ai-end-game-the-ethics-of-ai-with-ti.mp3'` |

## Quick batch
```bash
cd /Users/dabrams/playhead/TestFixtures/Corpus/Audio
ffplay -nodisp -autoexit -ss 91 -t 63 'american-scandal-2026-05-26-chappaquiddick-the-weight-of-the-name-5.mp3'  # American Scandal
ffplay -nodisp -autoexit -ss 876 -t 47 'american-scandal-2026-05-26-chappaquiddick-the-weight-of-the-name-5.mp3'  # American Scandal
ffplay -nodisp -autoexit -ss 924 -t 23 'american-scandal-2026-05-26-chappaquiddick-the-weight-of-the-name-5.mp3'  # American Scandal
ffplay -nodisp -autoexit -ss 1830 -t 24 'american-scandal-2026-05-26-chappaquiddick-the-weight-of-the-name-5.mp3'  # American Scandal
ffplay -nodisp -autoexit -ss 132 -t 63 'business-wars-2026-05-28-f1-vs-nascar-f1-roars-into-the-u-s-4.mp3'  # Business Wars
ffplay -nodisp -autoexit -ss 1864 -t 21 'business-wars-2026-05-28-f1-vs-nascar-f1-roars-into-the-u-s-4.mp3'  # Business Wars
ffplay -nodisp -autoexit -ss 1179 -t 90 'fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros.mp3'  # Fresh Air
ffplay -nodisp -autoexit -ss 1994 -t 77 'fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros.mp3'  # Fresh Air
ffplay -nodisp -autoexit -ss 3163 -t 34 'fresh-air-2026-05-30-best-of-boroughs-actor-alfre-woodard-ros.mp3'  # Fresh Air
ffplay -nodisp -autoexit -ss 0 -t 79 'hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi.mp3'  # Hard Fork
ffplay -nodisp -autoexit -ss 1294 -t 56 'hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi.mp3'  # Hard Fork
ffplay -nodisp -autoexit -ss 1351 -t 37 'hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi.mp3'  # Hard Fork
ffplay -nodisp -autoexit -ss 2523 -t 30 'hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi.mp3'  # Hard Fork
ffplay -nodisp -autoexit -ss 2582 -t 33 'hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi.mp3'  # Hard Fork
ffplay -nodisp -autoexit -ss 3670 -t 30 'hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi.mp3'  # Hard Fork
ffplay -nodisp -autoexit -ss 3728 -t 33 'hard-fork-2026-05-29-interesting-times-why-are-we-still-drivi.mp3'  # Hard Fork
ffplay -nodisp -autoexit -ss 2488 -t 84 'morbid-2026-05-21-the-matamoros-devil-murders-part-1.mp3'  # Morbid
ffplay -nodisp -autoexit -ss 1105 -t 84 'morbid-2026-05-25-the-matamoros-devil-murders-part-2.mp3'  # Morbid
ffplay -nodisp -autoexit -ss 2442 -t 58 'morbid-2026-05-25-the-matamoros-devil-murders-part-2.mp3'  # Morbid
ffplay -nodisp -autoexit -ss 27 -t 47 'morbid-2026-05-28-listener-tales-110-playdates-with-the-pa.mp3'  # Morbid
ffplay -nodisp -autoexit -ss 122 -t 27 'on-the-media-2026-05-29-trump-sued-himself-and-settled-for-a-1-8.mp3'  # On The Media
ffplay -nodisp -autoexit -ss 1040 -t 128 'on-the-media-2026-05-29-trump-sued-himself-and-settled-for-a-1-8.mp3'  # On The Media
ffplay -nodisp -autoexit -ss 2382 -t 67 'on-the-media-2026-05-29-trump-sued-himself-and-settled-for-a-1-8.mp3'  # On The Media
ffplay -nodisp -autoexit -ss 1456 -t 32 'planet-money-2026-05-29-the-sneaky-way-companies-get-new-chemica.mp3'  # Planet Money
ffplay -nodisp -autoexit -ss 13 -t 96 'smartless-2026-05-11-quot-kareem-rahma-quot.mp3'  # SmartLess
ffplay -nodisp -autoexit -ss 2589 -t 72 'smartless-2026-05-11-quot-kareem-rahma-quot.mp3'  # SmartLess
ffplay -nodisp -autoexit -ss 28 -t 63 'smartless-2026-05-18-quot-sting-quot.mp3'  # SmartLess
ffplay -nodisp -autoexit -ss 1398 -t 49 'smartless-2026-05-18-quot-sting-quot.mp3'  # SmartLess
ffplay -nodisp -autoexit -ss 2566 -t 63 'smartless-2026-05-18-quot-sting-quot.mp3'  # SmartLess
ffplay -nodisp -autoexit -ss 0 -t 55 'smartless-2026-05-21-quot-re-release-nate-bargatze-quot.mp3'  # SmartLess
ffplay -nodisp -autoexit -ss 2703 -t 35 'smartless-2026-05-25-quot-nick-jonas-quot.mp3'  # SmartLess
ffplay -nodisp -autoexit -ss 3970 -t 65 'smartless-2026-05-25-quot-nick-jonas-quot.mp3'  # SmartLess
ffplay -nodisp -autoexit -ss 718 -t 20 'the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve.mp3'  # The Daily Show: Ears Edition
ffplay -nodisp -autoexit -ss 880 -t 25 'the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve.mp3'  # The Daily Show: Ears Edition
ffplay -nodisp -autoexit -ss 1078 -t 33 'the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve.mp3'  # The Daily Show: Ears Edition
ffplay -nodisp -autoexit -ss 1694 -t 32 'the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve.mp3'  # The Daily Show: Ears Edition
ffplay -nodisp -autoexit -ss 2237 -t 31 'the-daily-show-ears-edition-2026-05-29-tds-time-machine-produce-pete-with-steve.mp3'  # The Daily Show: Ears Edition
ffplay -nodisp -autoexit -ss 0 -t 30 'the-ezra-klein-show-2026-05-29-does-trump-want-to-lose-the-midterms.mp3'  # The Ezra Klein Show
ffplay -nodisp -autoexit -ss 199 -t 32 'the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol.mp3'  # The Mel Robbins Podcast
ffplay -nodisp -autoexit -ss 1888 -t 34 'the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol.mp3'  # The Mel Robbins Podcast
ffplay -nodisp -autoexit -ss 1924 -t 80 'the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol.mp3'  # The Mel Robbins Podcast
ffplay -nodisp -autoexit -ss 2819 -t 33 'the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol.mp3'  # The Mel Robbins Podcast
ffplay -nodisp -autoexit -ss 4914 -t 37 'the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol.mp3'  # The Mel Robbins Podcast
ffplay -nodisp -autoexit -ss 4953 -t 28 'the-mel-robbins-podcast-2026-06-01-how-to-handle-difficult-people-7-psychol.mp3'  # The Mel Robbins Podcast
ffplay -nodisp -autoexit -ss 3200 -t 85 'the-nikki-glaser-podcast-2025-02-27-513-food-noise-achieving-greatness-amp-t.mp3'  # The Nikki Glaser Podcast
ffplay -nodisp -autoexit -ss 3096 -t 32 'the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev.mp3'  # The Nikki Glaser Podcast
ffplay -nodisp -autoexit -ss 3156 -t 69 'the-nikki-glaser-podcast-2025-02-28-514-an-elevated-gift-restaurant-pet-peev.mp3'  # The Nikki Glaser Podcast
ffplay -nodisp -autoexit -ss 1235 -t 62 'the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os.mp3'  # The Nikki Glaser Podcast
ffplay -nodisp -autoexit -ss 2924 -t 91 'the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os.mp3'  # The Nikki Glaser Podcast
ffplay -nodisp -autoexit -ss 3016 -t 55 'the-nikki-glaser-podcast-2025-03-06-515-nikki-s-unforgettable-weekend-the-os.mp3'  # The Nikki Glaser Podcast
ffplay -nodisp -autoexit -ss 63 -t 62 'the-nikki-glaser-podcast-2025-03-07-516-nikki-the-famous-aunt-writers-room-m.mp3'  # The Nikki Glaser Podcast
ffplay -nodisp -autoexit -ss 1524 -t 76 'the-nikki-glaser-podcast-2025-03-07-516-nikki-the-famous-aunt-writers-room-m.mp3'  # The Nikki Glaser Podcast
ffplay -nodisp -autoexit -ss 1694 -t 31 'the-nikki-glaser-podcast-2025-03-13-517-the-glaser-exit.mp3'  # The Nikki Glaser Podcast
ffplay -nodisp -autoexit -ss 1784 -t 20 'the-nikki-glaser-podcast-2025-03-13-517-the-glaser-exit.mp3'  # The Nikki Glaser Podcast
ffplay -nodisp -autoexit -ss 2677 -t 31 'the-nikki-glaser-podcast-2025-03-13-517-the-glaser-exit.mp3'  # The Nikki Glaser Podcast
ffplay -nodisp -autoexit -ss 2767 -t 165 'the-nikki-glaser-podcast-2025-03-13-517-the-glaser-exit.mp3'  # The Nikki Glaser Podcast
ffplay -nodisp -autoexit -ss 279 -t 86 'up-first-2026-06-01-can-graham-platner-survive-another-contr.mp3'  # Up First
ffplay -nodisp -autoexit -ss 1993 -t 39 'up-first-2026-06-01-can-graham-platner-survive-another-contr.mp3'  # Up First
ffplay -nodisp -autoexit -ss 0 -t 48 'why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi.mp3'  # Why Is This Happening? The Chr
ffplay -nodisp -autoexit -ss 3110 -t 110 'why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi.mp3'  # Why Is This Happening? The Chr
ffplay -nodisp -autoexit -ss 3771 -t 21 'why-is-this-happening-the-chris-hayes-po-2026-05-05-the-ai-end-game-who-s-leading-the-way-wi.mp3'  # Why Is This Happening? The Chr
ffplay -nodisp -autoexit -ss 0 -t 50 'why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit.mp3'  # Why Is This Happening? The Chr
ffplay -nodisp -autoexit -ss 2512 -t 79 'why-is-this-happening-the-chris-hayes-po-2026-05-12-the-ai-end-game-how-work-is-changing-wit.mp3'  # Why Is This Happening? The Chr
ffplay -nodisp -autoexit -ss 0 -t 30 'why-is-this-happening-the-chris-hayes-po-2026-05-19-the-ai-end-game-two-year-olds-vs-ai-with.mp3'  # Why Is This Happening? The Chr
ffplay -nodisp -autoexit -ss 612 -t 30 'why-is-this-happening-the-chris-hayes-po-2026-05-19-the-ai-end-game-two-year-olds-vs-ai-with.mp3'  # Why Is This Happening? The Chr
ffplay -nodisp -autoexit -ss 747 -t 63 'why-is-this-happening-the-chris-hayes-po-2026-05-19-the-ai-end-game-two-year-olds-vs-ai-with.mp3'  # Why Is This Happening? The Chr
ffplay -nodisp -autoexit -ss 1499 -t 78 'why-is-this-happening-the-chris-hayes-po-2026-05-19-the-ai-end-game-two-year-olds-vs-ai-with.mp3'  # Why Is This Happening? The Chr
ffplay -nodisp -autoexit -ss 28 -t 30 'why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein.mp3'  # Why Is This Happening? The Chr
ffplay -nodisp -autoexit -ss 1905 -t 28 'why-is-this-happening-the-chris-hayes-po-2026-05-20-ai-and-the-public-good-with-ezra-klein.mp3'  # Why Is This Happening? The Chr
ffplay -nodisp -autoexit -ss 84 -t 111 'why-is-this-happening-the-chris-hayes-po-2026-05-26-the-ai-end-game-the-ethics-of-ai-with-ti.mp3'  # Why Is This Happening? The Chr
```
