# Wizard Blindspots: What Neither Model Thought Of

> 2026-04-29. Post-adversarial round — after both models' 30-idea lists,
> cross-scoring, reactions, and a thorough re-investigation of underexploited
> capabilities in the codebase.

---

## Methodology

I re-investigated the codebase with a specific question: what data and
infrastructure exists but isn't being fully leveraged? This surfaced several
capabilities that both models walked past:

- **ShowTraitProfile** — a 7-dimensional continuous vector (musicDensity,
  speakerTurnRate, singleSpeakerDominance, structureRegularity,
  sponsorRecurrence, insertionVolatility, transcriptReliability) computed
  per show, updated via EMA (α=0.3) after each episode. Currently used
  *only* for ad detection prior calibration via PriorHierarchy. Not surfaced
  to the user or used for playback.

- **FeatureWindow audio data** — 14+ fields per 2-second window
  (musicProbability, pauseProbability, musicBedLevel, musicBedOnsetScore,
  musicBedOffsetScore, rms, spectralFlux, speakerChangeProxyScore, etc.)
  computed for every analyzed episode. Currently used only for ad detection
  and sleep timer pause-point queries. The structural understanding of audio
  this represents is vastly underexploited.

- **AdCatalogStore per-show scoping** — the `matches(fingerprint:show:)`
  method filters `WHERE show_id = ? OR show_id IS NULL`, meaning confirmed
  ad fingerprints on Show A never match candidate windows on Show B. Cross-
  show matching is a one-line SQL change with potentially significant
  cold-start impact.

- **AVAudioSession route information** — PlaybackTransport handles route
  *changes* (headphone disconnect → pause) but never reads the current
  route to adapt behavior. The app behaves identically on AirPods,
  car speakers, and a HomePod.

- **transcript_chunks as a corpus** — both models proposed FTS5 search
  (my idea #2, already bead playhead-90i) and FM summaries (my idea #3).
  Neither proposed using the raw transcript text *directly* — without FM —
  as an episode preview.

**Bead verification:** I ran `bd list --limit 0 | grep` for each candidate
before writing. Where an idea overlaps with an existing bead, I note it and
explain what's different. (Lesson learned from the first round.)

---

## Top 5 (Best to Worst)

### 1. Structure-Aware Silence Compression

**What it does.** Selectively compress non-content audio — music beds
between segments, dead air during transitions, long intro/outro jingles —
while preserving speech cadence, dramatic pauses, and conversation rhythm.
Not a uniform silence trimmer. A structural audio editor that understands
*what kind* of silence it's compressing.

**Why it's good.** Every podcast has wasted audio: 8-second music stings
between segments, 4-second applause breaks, 15-second cold-open jingles.
Overcast's Smart Speed compresses *all* silence uniformly — it can't
distinguish a dramatic pause before a punchline from dead air between
segments, so users learn to disable it on narrative shows. Playhead has
the structural data to do what Smart Speed can't: compress the jingle,
preserve the pause.

For a user who listens 2 hours daily, structural compression of music
beds and transition gaps could recover 8-15 minutes per day — without
ever touching speech pacing. The experience isn't "my podcast sounds
sped up." It's "my podcast somehow ended 10 minutes early and I didn't
miss anything."

**How it fits the architecture.** The data is already computed and stored:

- **FeatureWindow.musicProbability** — identifies music regions per 2-second
  window. Values above 0.7 with `musicBedLevel != .none` are candidate
  compression targets.
- **FeatureWindow.pauseProbability** — identifies dead air. High
  pauseProbability *within* a music region or *adjacent to* a
  speakerChange is a compression candidate. High pauseProbability
  *between two speech windows with the same speakerClusterId* is a
  dramatic pause — preserve it.
- **FeatureWindow.musicBedOnsetScore / musicBedOffsetScore** — mark the
  boundaries of music segments precisely. Compression starts at onset
  and ends at offset.
- **AdWindow boundaries** — already-classified ad regions could be
  compressed more aggressively (or skipped entirely, which already
  happens). The compression targets the *non-ad, non-speech* gaps.

Implementation: a `SilenceCompressor` that reads FeatureWindows for the
upcoming 60 seconds and adjusts AVPlayer's `rate` in real time. When the
playhead enters a compressible region, rate increases to 2.0-3.0x. When
it exits, rate returns to the user's base speed. The transitions use the
existing volume-duck mechanism (0.15s ease) so the speed change is
inaudible on music beds. Total new code: ~300 lines on PlaybackService.

**What the user perceives.** "My episodes are shorter but I didn't miss
anything." No setting to configure, no "Smart Speed" toggle, no "minutes
saved" counter. The intelligence is invisible — pure "Quiet Instrument."
A subtle animation on the timeline scrubber (the playhead moves slightly
faster through compressed regions) is the only visual signal, and only
if the user is watching.

**Risks and objections.**

- *Rate changes on music might sound bad.* Mitigated by using higher
  compression ratios only on sustained music beds (musicBedLevel `.medium`
  or `.loud`, musicProbability > 0.8), not on brief musical transitions.
  Speech-adjacent music (musicBedLevel `.soft` with low musicProbability)
  stays at base rate.
- *Users who enjoy podcast music intros.* Offer a per-show override: "Keep
  full music on this show." Defaults to compression-on, because the target
  audience (power listeners with backlogs) values time over ambiance.
- *AVPlayer rate changes might produce audio artifacts.* AVPlayer's built-in
  time-pitch algorithm (`.timePitchAlgorithm = .spectral`) handles up to
  2.0x cleanly. For higher ratios, use `.varispeed` on music-only segments
  (pitch shift is acceptable on jingles). Test on AirPods, speakers, and
  car audio.
- *Overcast patents.* Smart Speed is a marketing term, not a patent.
  Structural compression using classified audio features is a different
  mechanism — no known IP conflict.

**Bead status:** Not tracked. Closest bead is playhead-pfn (Per-Show
Playback Speed Memory), which is about *remembering* a user's chosen speed
per show — a different feature entirely.

---

### 2. Transcript Excerpt for Episode Triage

**What it does.** When browsing unplayed episodes, show an algorithmically
selected 2-3 sentence excerpt from the actual transcript — the most
information-dense passage from the first 15 minutes. Not an FM-generated
summary, not the publisher's show notes. A verbatim quote from the episode
itself, chosen by heuristic scoring.

**Why it's good.** Episode triage is a daily pain point for podcast power
listeners. Show notes are unreliable (copy-pasted ad reads, vague teasers,
or just absent). My idea #3 (Episode Summaries via FM) solves this with
FM generation, but it requires FM availability, costs battery, and risks
guardrail refusals. This idea solves the same problem with zero ML cost:
the transcript already exists.

The excerpt functions like the first paragraph of a newspaper article — it
gives the user enough signal to decide "listen" or "skip" in 5 seconds.
And because it's a verbatim quote, it's immune to hallucination. If the
excerpt is interesting, the episode is interesting. If it's boring, the
user just saved 45 minutes.

**How it fits the architecture.**

- **transcript_chunks** — already stored in AnalysisStore's SQLite database
  with `text`, `startTime`, `endTime`, and `analysisAssetId`.
- **AnalysisAsset.episodeId** — bridges chunks back to SwiftData Episode
  records for display in EpisodeListView.
- **Pre-analysis** — transcript chunks exist before the user presses play
  (for downloaded episodes), so excerpts are available at browse time.

Excerpt selection algorithm (~150 lines, pure function):

1. Query transcript_chunks for the first 15 minutes of the episode.
2. Score each chunk by *lexical density*: ratio of content words
   (nouns, verbs, adjectives — approximated by word length > 4 chars
   and not in a stop-word set) to total words. Higher density = more
   informative.
3. Penalize chunks that overlap with classified AdWindows (don't show
   an ad read as the episode preview).
4. Penalize chunks in the first 60 seconds (often boilerplate intro:
   "Welcome to The Show, I'm your host...").
5. Select the top-scoring contiguous 2-3 chunks (~40-60 words).
6. Format with leading/trailing ellipsis and a timestamp reference.

Surface in EpisodeListView as an expandable secondary line below the
episode title, styled in the editorial serif font (matching
TranscriptPeekView). Collapses to a single line on non-expanded cells.

**What the user perceives.** "I can see what the episode is actually about
before pressing play — and it's a real quote, not AI-generated." The
excerpt reads like a book jacket blurb, except it's the author's actual
words. Combined with pre-analysis, this means downloaded episodes in the
queue have both ad readiness AND content previews before the user touches
them.

**Risks and objections.**

- *Lexical density scoring might select boring passages.* The stop-word
  filter and AdWindow penalty handle the worst cases. For episodes with
  uniformly low-density transcripts (small talk, banter), the algorithm
  returns nothing — the cell shows standard show notes as fallback.
  Graceful degradation.
- *Transcript quality varies.* Low-quality ASR (heavy accents, background
  noise) produces bad excerpts. Guard with
  `ShowTraitProfile.transcriptReliability` — if below 0.4, suppress the
  excerpt and fall back to show notes.
- *Performance of scanning chunks per episode.* The query is bounded to
  the first 15 minutes (~450 chunks at 2-second windows). Pre-compute
  the excerpt on analysis completion and cache it as a single column on
  AnalysisAsset. Display cost: one JOIN per visible cell, same as the
  existing coverage-summary lookup.

**Bead status:** Not tracked. My idea #3 (Episode Summaries via FM) is the
closest equivalent, but that requires FM generation and a new SQLite table.
This is zero-FM and could ship months earlier.

---

### 3. Cross-Show Ad Fingerprint Transfer

**What it does.** When AdCatalogStore matches a candidate window's acoustic
fingerprint against stored entries, it currently filters by `WHERE show_id
= ? OR show_id IS NULL` (AdCatalogStore.swift line 399). This means a
confirmed ad on Show A never matches on Show B. Widening this to include
cross-show matches — with a higher similarity floor — enables confirmed
ads to transfer across the user's entire library.

**Why it's good.** The cold-start problem is Playhead's most persistent UX
challenge. When a user subscribes to a new show, the first few episodes
have zero show-specific trust, zero sponsor knowledge, and zero catalog
entries. Ad detection relies entirely on the lexical scanner and acoustic
classifier — the weakest signals in isolation.

But podcast advertising reuses creative. The same Squarespace read, the
same BetterHelp script, the same Athletic Greens jingle runs on dozens of
shows. A user who listens to 15 podcasts has likely already confirmed many
of the same ad creatives that will appear on show #16. Cross-show
fingerprint transfer turns the user's breadth of listening into a
competitive advantage: the more shows you subscribe to, the better ad
detection gets on *all* of them.

This creates a positive feedback loop unique to Playhead: library size
improves detection quality. No other podcast app has a mechanism where
subscribing to more podcasts makes the existing ones work better.

**How it fits the architecture.** The change is remarkably small:

1. **AdCatalogStore.matches** — add a secondary query with no `show_id`
   filter when the within-show query returns zero matches above floor.
   Use a higher similarity floor for cross-show matches (0.90 vs 0.80)
   to reduce false positives.

2. **CatalogMatch** — add a `crossShow: Bool` flag so downstream
   consumers (AutoSkipPrecisionGate, fusion path) can weight cross-show
   evidence lower than within-show evidence.

3. **AutoSkipPrecisionGate** — accept `catalogMatch` from cross-show
   entries but require at least one additional corroborating signal
   (lexical pattern OR acoustic classifier) before promoting to
   auto-skip-eligible. Cross-show catalog alone is a *precision boost*,
   not an *auto-skip trigger*.

Total change: ~40 lines across 3 files. The fingerprint computation,
storage, similarity math, and ingress pipeline are all unchanged.

**What the user perceives.** "When I subscribe to a new podcast, it already
seems to know where the ads are." Not perfect — the fingerprint only
transfers for identical ad creatives, not semantically similar ones. But
for the common case (same Squarespace/BetterHelp/AG1 read syndicated
across networks), the cold-start window shrinks from "5-7 episodes to
build trust" to "first episode, already catching the big sponsors."

**Risks and objections.**

- *False positive propagation.* A bad fingerprint match on Show A could
  cascade to Show B. Mitigated by the higher similarity floor (0.90) and
  the corroboration requirement (cross-show catalog alone can't trigger
  auto-skip). A false match would boost the candidate's evidence score
  but not unilaterally skip it.
- *Host-read ads vary by show.* The same sponsor's ad is often read
  differently by different hosts — different words, different cadence.
  These won't fingerprint-match (cosine similarity will be well below
  0.90). Cross-show transfer primarily catches *produced* ads: pre-
  recorded spots from ad networks (Megaphone, Art19) that insert
  identical audio across feeds. This is actually the most common ad
  format on large podcasts.
- *Scaling.* The current in-memory linear scan (`queryScanLimit: 5_000`)
  is fast for per-show queries but slower for cross-show scans of the
  full catalog. For a user with 1,000 confirmed ads across 20 shows,
  the scan takes ~2ms (64-float cosine similarity is cheap). If the
  catalog grows past 5,000 entries, the scan limit caps it.

**Bead status:** Partially overlaps with playhead-4my.15.2 (Cross-show
transfer learning), which approaches the same cold-start problem via
sponsor entity similarity and genre/network metadata. My proposal is a
different mechanism — acoustic fingerprint matching in AdCatalogStore
rather than SponsorKnowledgeMatcher — and is dramatically simpler to
implement (40 lines vs. a new transfer learning framework). The two
approaches are complementary, not competing.

Also related to playhead-bbrv (Cross-user analysis sharing), which is
about sharing analysis *between users*. My proposal is within a single
user's library, requires no networking, and has no privacy implications.

---

### 4. Content-Driven Podcast Discovery

**What it does.** Use on-device FM to detect when a podcast episode
mentions another podcast by name during *content* (not during ads).
Surface a subtle, dismissible suggestion: "Mentioned in this episode:
[Show Name]" with a one-tap subscribe action.

**Why it's good.** Podcast discovery is broken. The App Store editorial
page shows the same 20 shows. Algorithmic recommendations require
listening data Playhead doesn't (and shouldn't) collect server-side.
Word-of-mouth — "my favorite podcast host said to check out Show X" —
is the most trusted discovery channel, and it happens *inside the audio
Playhead is already transcribing.*

This is the only discovery mechanism that's powered by the content itself.
When Joe Rogan says "I had this great conversation with Lex Fridman on
his show last week," Playhead could surface: "Lex Fridman Podcast —
mentioned at 47:12." The host's endorsement is the most credible
recommendation a listener can get — and Playhead is the only app that
can hear it.

Crucially, Playhead already classifies ad regions. So the system can
filter out ad-read mentions ("brought to you by [network's other show]")
by checking whether the mention timestamp falls within a classified
AdWindow. Only content-region mentions surface as discovery suggestions.
This is the kind of thing that's impossible without both the transcript
AND the ad classification — both of which are uniquely Playhead.

**How it fits the architecture.**

- **FoundationModelClassifier** pattern — schema-bound extraction via
  `@Generable` struct:
  ```
  struct PodcastMention {
      let showName: String
      let approximateTimestamp: TimeInterval
      let context: String  // surrounding sentence
  }
  ```
- **Backfill scheduling** — runs as an optional post-analysis pass when
  FM is available, similar to the episode summary proposal.
- **AdWindow filtering** — discard any mention whose timestamp falls
  within a classified ad region (simple range check against existing
  AdWindow data).
- **PodcastDiscoveryService** — validate the mentioned show name against
  the iTunes Search API (already used for podcast search in the app).
  If the show exists and the user isn't subscribed, store the suggestion.
- **UI** — a subtle card in the episode detail view, below the transcript
  peek toggle. Not a notification, not a banner — the user discovers it
  when they look.

**What the user perceives.** "The app noticed that the host recommended
another podcast and offered to subscribe." It feels like having a friend
who listens carefully and passes along recommendations. This is organic
discovery that gets *better* the more podcasts you listen to — another
"library breadth as competitive advantage" mechanism.

**Risks and objections.**

- *False positives from casual mentions.* A host saying "I was on
  NPR last week" shouldn't trigger a suggestion to subscribe to NPR.
  The FM prompt needs to distinguish *recommendations* ("you should
  check out...") from *mentions* ("I appeared on..."). The FM's
  schema-bound generation can include a `mentionType` field
  (recommendation / crossPromotion / casualMention) and only surface
  recommendations.
- *FM cost.* This requires scanning the full transcript for mentions —
  more expensive than the sampling strategy used for summaries. Mitigate
  by running only on episodes the user has actually listened to (not
  pre-analyzed), and only when the device is charging. Make it a
  Background-lane backfill job.
- *iTunes Search API is a network call.* Validating show names against
  the API requires connectivity. Cache the lookup results aggressively
  (show names don't change). If offline, store the mention and validate
  later.
- *Users might find suggestions annoying.* The UI should be maximally
  passive: a line in the episode detail, not a push notification.
  Include a "Don't suggest shows" toggle in Settings. Default: on, with
  a maximum of 1 suggestion per episode (even if multiple shows are
  mentioned).

**Bead status:** Not tracked. No bead covers discovery from transcript
content. The closest feature is PodcastDiscoveryService (search by name
or top charts), which is user-initiated, not content-driven.

---

### 5. Audio Route-Aware Playback Adaptation

**What it does.** Read AVAudioSession.currentRoute to detect the output
device (built-in speaker, wired headphones, AirPods, Bluetooth car audio,
AirPlay, CarPlay) and subtly adapt playback behavior per route.

**Why it's good.** Podcast listening happens across wildly different
contexts: AirPods on a run, car speakers on a commute, phone speaker
while cooking. Each context has different needs:

- **Car (Bluetooth/CarPlay):** Road noise means quiet passages are
  inaudible. The existing FeatureWindow `rms` data enables dynamic
  volume normalization — boost quiet speech segments relative to loud
  ones. Also: the ad-skip volume duck (0.15 amplitude, 0.15s duration)
  is jarring on car speakers; extend the duck to 0.5s with a gentler
  curve.
- **AirPods/headphones:** Volume normalization is unnecessary (quiet
  environment). But the skip-transition duck sounds great on headphones
  — keep the current 0.15s behavior. This is also the optimal context
  for silence compression (idea #1) since speed changes are less
  perceptible with earbuds.
- **Speaker (phone/HomePod):** Audio is often background, not focused
  listening. Consider auto-pausing when the user leaves the room (if
  Apple's spatial awareness API is available on HomePod). At minimum:
  don't auto-skip ads on speaker — the user may not be actively
  listening, and an unexpected seek would be disorienting. Default to
  mute-in-place for speaker routes.

The adaptation is entirely invisible. The user doesn't configure anything
or know it's happening. They just notice that the app "sounds right"
whether they're driving, running, or cooking. This is the "Quiet
Instrument" philosophy applied to audio output: the instrument adapts to
the room.

**How it fits the architecture.**

- **AVAudioSession.currentRoute** — already observed for route *changes*
  in PlaybackTransport.swift. Extending to read the current route type
  on playback start is trivial.
- **AudioRoute enum** — new type (~20 lines) mapping AVAudioSession port
  types to behavioral buckets: `headphones`, `speaker`, `carAudio`,
  `airplay`.
- **PlaybackService** — existing `duckVolume` and `duckDuration` constants
  become route-dependent properties. ~15 lines changed.
- **SkipOrchestrator** — the skip behavior (auto-skip vs. mute-in-place)
  could check the current route. ~10 lines changed.
- **FeatureWindow.rms** — used for volume normalization on car routes.
  Query upcoming FeatureWindows (same as silence compression) and
  dynamically adjust AVPlayer's volume. ~50 lines in a new
  `VolumeNormalizer` helper.

Total: ~150 lines of new code, spread across existing files.

**What the user perceives.** "It just works better in the car." And "it
just works better on headphones." The user might not even consciously
notice — they'd only notice if it *stopped* adapting (by trying a
competitor that doesn't). This is the kind of subtle, compounding polish
that builds the feeling of a premium, thoughtful product.

**Risks and objections.**

- *Privacy concerns about route detection.* AVAudioSession.currentRoute
  is a standard iOS API that doesn't require any permissions. It returns
  the audio output type, not location or device identity. No privacy
  implications.
- *Users who want consistent behavior across routes.* Add a "Same
  behavior everywhere" toggle in Settings, defaulting to route-adaptive.
  The toggle disables all route-specific overrides.
- *CarPlay detection reliability.* CarPlay sessions are detectable via
  the route type (`carAudio` port) and/or `UIScene` connection. False
  positives (Bluetooth speaker misidentified as car) are possible but
  low-risk — the car behavior (gentler duck, volume normalization) is
  acceptable on any Bluetooth speaker.
- *Volume normalization introduces latency.* Querying upcoming
  FeatureWindows and computing target volume adds a small delay. Pre-
  compute the volume curve for the next 30 seconds on a background
  thread, same pattern as silence compression look-ahead.

**Bead status:** Not tracked. PlaybackTransport handles route *change
events* (pause on disconnect) but never adapts *behavior* to the
current route. No bead covers route-aware adaptation.

---

## Meta-Observation: Why Both Models Missed These

Both AI models — and I'll include myself explicitly — had the same blind
spot: we thought about Playhead as a *feature list* and asked "what
feature should we add?" The ideas above come from a different question:
"what data is Playhead already computing that it isn't using?"

- Silence compression uses FeatureWindow data (musicProbability,
  pauseProbability, musicBedLevel) that's computed for every analyzed
  episode but only consumed by ad detection.
- Transcript excerpts use transcript_chunks that are stored in SQLite
  but only surfaced via TranscriptPeekView during active playback.
- Cross-show fingerprint transfer uses AdCatalogStore entries that are
  already indexed but artificially scoped to per-show queries.
- Content-driven discovery uses transcript text that's already indexed
  in FTS5 but never scanned for podcast references.
- Route-aware adaptation uses FeatureWindow.rms and audio session
  information that's already available but never combined.

The pattern: Playhead's analysis pipeline produces a rich, multi-
dimensional understanding of every episode's audio. Almost all of that
understanding is currently consumed by a single downstream system (ad
detection). The highest-leverage ideas aren't new features that require
new infrastructure — they're new *consumers* of data that already exists.

Both models' original lists were biased toward "build new thing" rather
than "use existing thing differently." That's the shared blind spot this
round is meant to address.
