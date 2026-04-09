# Playhead Improvement Ideas

This document is grounded in the current codebase, not just the original plan.

Selection criteria used for ranking:
- Must be obviously accretive to the current architecture
- Must improve the product in ways users will actually notice
- Must be pragmatic to implement incrementally
- Must improve one or more of: trust, readiness, recall, usefulness, or operational safety
- Should prefer ideas that compound with the existing pipeline rather than replace it

## Candidate Pool: 30 Ideas

### 1. Download-time pre-analysis with visible readiness
Make downloaded episodes analyze themselves before first play, then surface a simple readiness state in the library. Users would perceive the app as much faster and more reliable because skips and transcript features would already be there when playback starts. Implementation would mostly extend the existing `AnalysisWorkScheduler`, `AnalysisJobRunner`, `SkipCueMaterializer`, and SwiftData `analysisSummary` bridge rather than introduce a new system.

### 2. User correction loop with scoped learning
Let users say "not an ad", "skip was too early", "skip was too long", "this self-promo is fine", or "always skip this sponsor on this show". Users would trust the app more because mistakes become fixable instead of mysterious. Implementation would build on existing `recordListenRewind`, `TrustScoringService`, `PodcastProfile`, and skip decision logs, but add explicit correction scopes and a durable correction store.

### 3. Sponsor memory and compiled hot-path lexicon
Turn confirmed backfill results into deterministic future wins: sponsor names, repeated CTA fragments, first-party suppressions, jingle fingerprints, and recurring slot priors should compile into a fast per-show artifact. Users would experience this as the app "learning their favorite shows" and catching ads earlier. Implementation fits the existing `PodcastProfile`, `LexicalScanner`, evidence catalog, and backfill path cleanly.

### 4. Full transcript view with search and ad markers
Promote the current transcript peek into a full transcript surface with search, ad markers, and jump-to-playhead actions. Users would value the app even when skip quality is imperfect, because the transcript becomes useful in its own right. Implementation is pragmatic because the app already stores transcript chunks and has an FTS5-oriented analysis store design.

### 5. FM refusal-resistant fallback for medical/pharma/therapy ads
Treat Foundation Models refusal as a routing problem, not a dead end. Users would notice fewer "obvious ad but not skipped" misses, especially for CVS/Walgreens/therapy-style reads. Implementation would deepen the current `SensitiveWindowRouter`, `PromptRedactor`, `PermissiveAdClassifier`, and evidence-resolution path rather than invent a second ad detector from scratch.

### 6. First-party and house-promo suppression
Teach the system to distinguish third-party ads from show/network promos and first-party calls to action. Users would perceive fewer annoying false positives and the app would feel smarter, not just more aggressive. Implementation would extend domain/sponsor ownership logic in the evidence catalog and profile layer.

### 7. "Why was this skipped?" evidence inspector
Provide a compact explanation view showing sponsor phrases, CTA text, URL hits, confidence, and whether the skip came from live detection or pre-analysis. Users would trust the product more because it would stop behaving like a black box. Implementation would mostly be UI over existing evidence and decision records.

### 8. Library-level episode intelligence summaries
Show "Ready to skip", "Transcript ready", "Analyzing", or "Needs download" directly in episode lists. Users would understand what the app has prepared without trial and error. Implementation would extend `Episode.analysisSummary` and pre-analysis progress plumbing.

### 9. Progressive analysis from partial downloads
Start useful analysis from sufficiently buffered partial audio instead of waiting for full download completion whenever possible. Users would get skips sooner on streamed episodes. Implementation would build on `StreamingAudioDecoder`, transcript append paths, and the existing progressive loader setup, but it requires careful truncation and cache-invalidating rules.

### 10. User-tunable skip aggressiveness
Offer modes like conservative, balanced, and aggressive on top of the existing auto/manual/off framing. Users who hate false positives and users who hate hearing ads could each get what they want. Implementation would mostly parameterize thresholds and suppression policy, but would need careful defaults.

### 11. Boundary feedback from seek/rewind behavior
Use "rewind into skipped segment" and immediate post-skip back-seeks to improve boundary snapping, not just trust scores. Users would perceive fewer clipped jokes and fewer awkwardly late exits from ads. Implementation would extend the current `SkipOrchestrator` logging and backfill refiners.

### 12. Explicit background queue management
Expose which episodes are queued for pre-analysis and allow pause/resume/prioritize actions. Users who download lots of podcasts would understand why some episodes are ready and others are not. Implementation would mainly be UI over `analysis_jobs` and `BackgroundProcessingService`.

### 13. Real-episode benchmark gates in CI
Add durable pass/fail thresholds around real-fixture recall and false-positive rates instead of treating all benchmarks as informational only. Users would not see this directly, but they would feel it through fewer regressions. Implementation is highly pragmatic given the existing replay and benchmark tests.

### 14. Replay harness fed by exported real episodes
Turn the debug episode exporter into a steady corpus-generation tool for regression testing. Users benefit because fixes stop breaking older shows. Implementation would extend the current export feature and fixture loaders rather than invent a new evaluation system.

### 15. Production-grade model manifest and asset delivery
Replace placeholder `example.com` URLs and zero hashes with a real model distribution workflow. Users would perceive onboarding as credible and dependable instead of fragile. Implementation is straightforward but operationally important.

### 16. Self-healing repair center for stores and caches
Offer a settings/debug action that validates SwiftData, SQLite, model files, and cache directories, then proposes safe repairs. Users would recover from corrupted local state without reinstalling. Implementation would formalize logic that already exists informally in startup recovery paths.

### 17. Better storage management UI
Show how much space is used by audio cache, transcript/analysis data, staged models, and rollback models, with reclaim actions. Users would understand the cost of the intelligence features and feel in control. Implementation would extend the current settings storage computations.

### 18. Stronger metadata priors from feeds and chapters
Use show notes, chapters, enclosure metadata, and feed-level patterns as weak priors for sponsor detection and boundary hints. Users would experience slightly better recall with minimal risk if priors stay weak. Implementation is low-risk because the parser already sees most of this data.

### 19. Shareable transcript moments
Let users select transcript regions and share text or deep links into playback timecodes. Users would find the app more useful even outside ad skipping. Implementation depends on stronger transcript UI but not on any backend.

### 20. CarPlay and remote-control polish
Deepen remote command, lock-screen, and vehicle behavior so skip cues and transport feel native in all listening contexts. Users would perceive polish and seriousness. Implementation builds directly on the existing `PlaybackService` remote command support.

### 21. Accessibility-first transcript and ad UX pass
Improve VoiceOver grouping, focus order, spoken time labels, and transcript navigation affordances. Users who depend on accessibility would get a materially better experience, and everyone benefits from cleaner structure. Implementation is highly pragmatic because most surfaces already exist.

### 22. Sponsor cards/history
Keep a lightweight per-episode history of skipped sponsors with dismissible cards and optional revisit actions. Users would appreciate not losing useful sponsor info just because an ad was skipped. Implementation builds naturally on `AdBannerView`, metadata extraction, and `AdWindow` fields.

### 23. Per-show controls and episode exceptions
Allow users to force manual mode, disable skipping, or trust a show more aggressively on a per-show basis. Users would feel ownership over the product rather than subject to its defaults. Implementation is an incremental extension of `PodcastProfile.mode`.

### 24. Smarter library sort and surfacing
Prioritize unfinished episodes, analysis-ready episodes, and recent releases instead of only simple browsing order. Users would find the library more helpful as a listening cockpit. Implementation is mostly query/view-model work.

### 25. Detection health and status surface
Show clear reasons when an episode is not yet analyzed: downloading, low battery deferral, thermal pause, missing model, or Apple Intelligence unavailable. Users would stop interpreting "no skip happened" as "the app failed silently". Implementation would expose current scheduler/capability state in a user-readable way.

### 26. Stronger anti-churn policy for cues
Keep skip cues stable once surfaced unless there is materially stronger counterevidence. Users would perceive less jitter and fewer changing skip markers between sessions. Implementation would deepen already-present anti-churn ideas in the detection/backfill flow.

### 27. Suspicious-window quarantine
When windows are high-risk for false positives, keep them in shadow/manual mode until corroborated instead of letting them behave like normal candidates. Users would see fewer embarrassing skips on borderline content. Implementation would reuse confidence/evidence signals already produced by the pipeline.

### 28. FM cohort canarying and kill switches
Treat `(OS build, prompt cohort, schema cohort)` as rollout units that can be shadowed, disabled, or promoted based on replay performance. Users would be protected from silent OS-model regressions. Implementation fits the current `ScanCohort` and shadow infrastructure extremely well.

### 29. Deterministic CTA/URL pattern expansion
Mine existing misses and extend deterministic sponsor/CTA/url patterns so more obvious ads are caught without waiting for FM. Users would perceive better recall immediately. Implementation is cheap and should be part of routine detector maintenance.

### 30. Feed-driven identity enrichment
Persist stronger identities for podcasts, episodes, and recurring sponsors so cross-episode learning is more robust under feed churn and dynamic ad insertion. Users would not notice the mechanism directly, but they would feel the system become less brittle over time. Implementation would deepen canonicalization and fingerprint bridging already present in the models and download manager.

## Winnowing Logic

I filtered the 30 ideas through four questions:

1. Does this make the core experience noticeably better for real users, not just developers?
2. Can it be built incrementally from systems already in the repo?
3. Does it compound with the ad-skip pipeline instead of distracting from it?
4. Am I confident it would still look like a good decision six months from now?

That pushes the ranking toward ideas that improve:
- first-play success
- trust after mistakes
- future episode learning
- fallback utility when skipping is imperfect
- robustness around the known FM blind spot

## Top 5 Ideas

## 1. Make Downloaded Episodes Analysis-Ready Before First Play

### What it is

Shift the product from reactive intelligence to proactive intelligence. If an episode is downloaded, or is obviously likely to be played soon, the app should precompute enough of the pipeline that the first actual playback session already has:
- initial skip cues
- transcript coverage for the opening segment
- an accurate readiness summary in the library

This should not mean "analyze everything to completion all the time." It should mean "finish the first useful tranche early, then deepen opportunistically."

### Why this is the best idea

This idea improves the single most important product moment: the first listen.

Right now, Playhead has a lot of intelligence, but much of it is still architected as something that wakes up when playback begins. That creates a perception problem. Even if the detector is good, the user can still experience the app as late, uncertain, or inconsistent if the episode is not ready when they hit play.

Pre-analysis changes that psychology completely:
- the app feels fast, not busy
- the user sees evidence that the product prepared for them
- the first ad-skip moment becomes much more likely to land
- transcript features stop feeling like "eventually consistent" and start feeling intentional

In other words, this does not just improve latency. It improves trust, product feel, and perceived quality all at once.

### How users would perceive it

Users would notice:
- episodes often begin with skip intelligence already available
- the library communicates readiness clearly instead of leaving them to guess
- downloaded episodes feel "premium" and prepared
- there is less waiting, less pipeline warmup noise, and less disappointment

This is especially valuable for commute-style listening where the user opens the app, taps play, and expects everything to just work.

### Why I am confident it is accretive and pragmatic

The repo already has most of the machinery:
- `AnalysisWorkScheduler`
- `AnalysisJobRunner`
- `AnalysisJobReconciler`
- `BackgroundProcessingService`
- `SkipCueMaterializer`
- `Episode.analysisSummary`

This means the right move is not to invent a new subsystem. It is to promote the existing pre-analysis path from a background convenience into a first-class product behavior.

### How I would implement it

1. Define explicit readiness tiers at the product level.
   Example:
   - `not_ready`
   - `preparing`
   - `ready_initial_skip`
   - `ready_transcript_opening`
   - `ready_full_backfill`

2. Store those tiers in the SwiftData-facing `analysisSummary` so library and episode views can show them cheaply.

3. On explicit download completion, enqueue a higher-priority pre-analysis job automatically.
   - first target: enough coverage to make the opening minutes intelligent
   - second target: deeper coverage while charging / idle / background-eligible

4. Add a small but prominent readiness chip in `EpisodeListView` and optionally `LibraryView`.

5. Teach the scheduler to prioritize:
   - explicitly downloaded episodes
   - most recently interacted episodes
   - unfinished episodes

6. Keep power discipline:
   - respect current battery/thermal gating
   - separate "first useful tranche" from "full luxury backfill"

### Why it beats the other candidates

It has unusually high impact for relatively low architectural disruption. It directly improves the first-run and everyday listening experience, and it uses systems the project already built. That combination is rare.

## 2. Add a Real User Correction Loop With Scoped Learning

### What it is

Turn mistakes into training signals the product can actually use.

The current architecture already has pieces of this idea:
- trust scoring
- rewind/listen signals
- decision logs
- per-show profiles

The missing piece is an explicit, user-facing correction model with durable scopes. The app should let a correction mean something more precise than "trust went down a bit."

Useful scopes would include:
- exact span in this episode
- this phrase on this show
- this sponsor on this show
- this domain is first-party for this show
- never auto-skip this show, only suggest

### Why it is ranked second

Ad-skipping products live or die on trust. Not recall alone. Not elegance alone. Trust.

A miss is annoying. A false positive is much worse. And a false positive that cannot be corrected in a meaningful way is how users stop believing the product.

This idea creates a clear product promise:
"If we get it wrong, you can teach us quickly, locally, and durably."

That is one of the strongest trust-building moves available.

### How users would perceive it

Users would experience:
- less helplessness when a skip feels wrong
- a sense that the app adapts to their actual preferences
- fewer repeated mistakes on favorite shows
- more confidence leaving auto-skip enabled

The key is not to make the UX heavy. Corrections must feel like small, obvious actions taken at the moment of annoyance.

### Why I am confident it would work

The repo already has the underlying primitives:
- `recordListenRewind`
- `TrustScoringService`
- `PodcastProfile`
- `SkipOrchestrator` decision logs
- ad banners that can serve as correction surfaces

So the work is additive:
- add correction types
- persist them
- teach detector/orchestrator paths to honor them

This is much safer than trying to solve trust entirely through better model thresholds.

### How I would implement it

1. Introduce a correction store or append-only correction event table.

2. Define correction scopes explicitly.
   Example:
   - `exact_span`
   - `phrase_on_show`
   - `sponsor_on_show`
   - `first_party_domain_on_show`
   - `show_mode_override`

3. Extend the banner and now-playing surfaces with lightweight correction options.
   Example:
   - "Not an ad"
   - "Too early"
   - "Too late"
   - "This is just a show promo"
   - "Always skip this sponsor on this show"

4. Apply corrections in three places:
   - skip suppression in `SkipOrchestrator`
   - candidate suppression / promotion in backfill
   - profile compilation for future episodes

5. Expose a minimal per-show settings page where users can review or clear learned rules.

### Why it is not #1

Pre-analysis readiness improves the first-play experience for almost everyone, even if they never touch settings or corrections. Corrections are slightly more reactive. But after readiness, this is the strongest trust multiplier in the project.

## 3. Build Sponsor Memory That Compiles Into Faster Future Catches

### What it is

Use high-confidence confirmed backfill results to create a per-show sponsor memory that feeds the hot path.

This memory should include:
- sponsor names
- repeated CTA fragments
- known domains
- first-party suppressions
- common slot priors
- repeated disclaimers
- jingle hashes when available

The critical design requirement is compilation. The hot path should not query a rich fuzzy store every time. It should consume a compact, deterministic, low-latency artifact.

### Why this idea matters so much

This is the cleanest way to make the system better over time without making it slower over time.

Many ad detectors can be good on one episode. Much fewer become structurally better on a show after observing a few episodes. That is where the compounding value is.

If Playhead learns a show locally and improves its future detection speed and recall, the app becomes:
- more accurate
- faster at first useful detection
- less dependent on expensive backfill for recurring sponsor styles
- more differentiated from naive one-shot detectors

### How users would perceive it

Users are likely to experience this as:
- "this app really understands the shows I listen to most"
- earlier and more reliable ad skips on familiar podcasts
- fewer misses on recurring sponsors
- fewer weird classifications of house promos or first-party links

They do not need to know the mechanism. They only need to feel the pattern.

### Why I am confident it is a good fit

The architecture already points in this direction:
- `PodcastProfile`
- `LexicalScanner`
- show priors
- evidence catalogs
- slot priors
- backfill metadata extraction

So this is not a speculative pivot. It is the natural next layer over systems already present.

### How I would implement it

1. Expand the profile/materialization layer to store only high-confidence, grounded knowledge.

2. Distinguish positive memory from suppressive memory.
   - positive: sponsor, CTA, URL, recurring phrase
   - suppressive: first-party domain, known house-promo marker, user correction rule

3. Compile the profile into a hot-path artifact after qualifying updates.
   - flattened sponsor lexicon
   - normalized CTA phrase list
   - domain ownership map
   - slot prior table

4. Make `LexicalScanner` and classifier input consult that compiled artifact directly.

5. Add poisoning protections:
   - only materialize from confirmed windows
   - require corroboration or repetition
   - honor user corrections as stronger-than-model signals

### Why it is ranked below corrections

This is a huge leverage idea, but its payoff is slightly less immediate than explicit corrections and readiness. It compounds over repeated listening, which is excellent, but not as universal on day one.

## 4. Ship the Full Transcript Experience, Not Just a Peek

### What it is

Take the existing transcript peek and turn it into a real feature:
- full synced transcript
- search
- jump-to-line
- visible ad markers
- highlighted skipped segments
- lightweight sponsor/evidence callouts where appropriate

This should become an "episode intelligence" surface, not merely a diagnostic sheet.

### Why it belongs in the top 5

Playhead should not be valuable only when ad-skipping works perfectly.

A full transcript experience does three important things:

1. It broadens the app's utility beyond a single automation feature.
2. It gives users a fallback value path when detection is still incomplete.
3. It makes the intelligence of the system visible and explorable.

That matters because skip quality can improve over time, but the product still needs to feel compelling right now.

### How users would perceive it

Users would get:
- searchable memory for episodes
- a better way to revisit moments
- immediate clarity about what was skipped and why
- a differentiated reason to prefer Playhead even when ads are sparse

This is the kind of feature that makes the product feel richer, not just smarter.

### Why I am confident it is pragmatic

The repo already has:
- transcript persistence
- transcript peek UI
- transcript time anchoring
- planned FTS5 storage model
- ad window persistence

That means most of the hard data plumbing is already there. The remaining work is turning existing analysis artifacts into a coherent user-facing view.

### How I would implement it

1. Expand `TranscriptPeekView` into a full transcript screen with virtualized loading if needed.

2. Add indexed search over persisted transcript chunks.

3. Overlay ad windows and optionally evidence anchors.

4. Support jump-to-time interactions and "resume from here".

5. Add basic actions:
   - copy text
   - share quote with timestamp
   - "report this skip decision" from transcript context

### Why it is not ranked higher

It broadens usefulness and makes the app more compelling, but it does not improve the core "did the ad skip work when I needed it?" moment as directly as the top three ideas do.

## 5. Harden the Medical/Pharma/Therapy Blind Spot With a Dedicated Fallback Path

### What it is

Build a deliberate recovery path for the exact content category currently causing high-value misses: ads that trip Foundation Models safety refusal.

The right framing is not "beat Apple's classifier head-on." The right framing is:
- detect risky windows early
- route them differently
- preserve grounded evidence
- avoid turning FM refusal into an automatic miss

This likely means combining:
- sensitive-window routing
- selective redaction
- permissive-content-transformations path when appropriate
- stronger deterministic evidence extraction
- conservative promotion rules

### Why it still makes the top 5 despite the technical risk

Because this repo already documents the problem in painful detail, and the problem is product-visible.

If a user hears a blatant CVS or therapy ad that Playhead misses, the product's central promise takes a hit. Solving even part of that blind spot is therefore disproportionately valuable.

### How users would perceive it

Users would simply experience fewer absurd misses on obviously commercial content. They do not need to know that Apple guardrails were involved. They only need to feel that the app stopped failing in conspicuous cases.

### Why I did not rank it higher

The problem is real, but this is the least predictable of the top five ideas because it depends on OS behavior outside the app's control. That makes it a high-value but lower-confidence bet than the first four.

### Why I still think it is worth doing

Because the codebase already contains the right primitives:
- `SensitiveWindowRouter`
- `PromptRedactor`
- `PermissiveAdClassifier`
- `FoundationModelClassifier`
- evidence catalogs
- shadow retry machinery

That means the project has already paid much of the conceptual cost. The remaining step is to make this path sharper, more selective, and better validated with regression fixtures.

### How I would implement it

1. Explicitly classify risky content categories at the router boundary.

2. Separate:
   - refusal avoidance
   - evidence grounding
   - final promotion policy

3. If FM still refuses, preserve deterministic evidence and let a conservative fallback score run instead of dropping the window.

4. Add dedicated real-fixture tests for:
   - pharmacy vaccine ads
   - therapy ads
   - regulated medical-test ads
   - known benign medical discussion

5. Keep this path cohort-gated and replay-validated so OS regressions do not silently spread.

## Why These 5 and Not the Others

These five ideas work together unusually well:

- Idea 1 makes the app feel ready before playback starts.
- Idea 2 makes the app trustworthy when it gets something wrong.
- Idea 3 makes the app improve on shows the user actually listens to.
- Idea 4 makes the product useful even outside ad-skipping.
- Idea 5 attacks a known, painful reliability hole without requiring an architectural rewrite.

That mix is why I am confident in this ranking. It improves:
- first impression
- day-to-day trust
- long-term compounding quality
- product usefulness
- technical robustness around a real failure mode

If I had to choose just one: do idea 1 first.
If I had to choose the best pair: do ideas 1 and 2 together.
If I wanted the best medium-term moat: do ideas 1, 2, and 3 in sequence.
