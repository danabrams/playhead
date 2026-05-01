# Wizard Ideas — Claude Code

> Generated 2026-04-29 by Claude Code (Opus 4.6) after deep codebase investigation.
> Constraint: on-device mandate is non-negotiable. All ideas respect the existing
> actor-based architecture (PlayheadRuntime, AnalysisCoordinator, AnalysisWorkScheduler,
> FoundationModelClassifier, etc.) and bd-tracked beads.

---

## Methodology

I brainstormed 30 candidate improvements across robustness, performance, UX, and
differentiation, then evaluated each against five criteria:

1. **Obviously accretive** — makes Playhead clearly better for real users
2. **Pragmatic** — implementable within existing architecture, not a rewrite
3. **Leverages the on-device advantage** — things only Playhead can do
4. **Fills a real gap** — not duplicating something that already works well
5. **Builds user trust / creates switching cost** — makes Playhead hard to leave

The 25 ideas that didn't make the cut (and why they lost):

- Graceful degradation UI — thermal/battery management already exists; gap is minor
- Skip confidence indicator — violates "Quiet Instrument" design (no diagnostic chrome)
- Offline feed resilience — feeds are just RSS; on-device nature already handles this
- Acoustic-only pre-roll detector — existing feature extraction already catches music beds
- Incremental timeline markers — already happening via real-time AdWindow materialization
- "Why was this skipped?" — AdRegionPopover with tap-to-explain already exists
- Ad density in episode list — chicken-and-egg without pre-analysis; derivative of idea #1
- Listening streak / gamification — wrong aesthetic for "Quiet Instrument"
- Smart speed recommendations — niche; most users have a preferred speed
- Chapter-aware timeline — ChapterEvidenceParser + gtt9.22 already ship chapter ingestion; UI is incremental polish
- Podcast comparison view — niche, doesn't leverage the transcript platform
- Dynamic playback speed — too clever; risky if it slows during an ad
- "Mark as ad" gesture — BoundaryExpander exists; incremental UX polish
- Listening position resume context — already persists position; minor improvement
- Topic segmentation — requires speaker diarization (not yet implemented); too ambitious
- Guest detection — speakerClusterId is a placeholder; premature
- Mention extraction — already in v0.3 roadmap; not a new idea
- Sponsor transparency report — interesting data but doesn't improve listening
- Clip creation — v0.3 roadmap; involves audio encoding complexity
- Smart notifications with ad density — requires pre-analysis; derivative
- Podcast health score — too meta/analytical for the aesthetic
- Adaptive skip aggressiveness slider — overrides nuanced per-show trust; net negative
- Haptic scrub ticks — risks being too busy; design doc says "instrumental, not entertaining"
- Warm-resume acceleration — ArtifactClass.warmResumeBundle already exists
- "Time saved" dashboard — Dan specifically rejected quantified metrics ("peace of mind, not metrics")

---

## Top 5 (Best to Worst)

### 1. Predictive Pre-Analysis of Downloaded Episodes

**The problem it solves.** Today, the analysis pipeline starts when the user presses play. Even on an A17 Pro, there's a window — possibly 30-90 seconds — before the first usable skip can fire. The user hears the first pre-roll ad in full. For an app whose entire value proposition is "skip ads," hearing the first ad every time is a painful irony.

**How it works.** When an episode is downloaded (manually or via auto-download / background feed refresh), the AnalysisWorkScheduler queues a pre-analysis job in the Background or Soon lane. During idle/charging time, the pipeline runs through decode → feature extraction → fast-pass transcription → lexical scanning → acoustic classification. All results are written to SQLite keyed by the analysisAssetId. When the user eventually presses play, the AnalysisCoordinator finds existing coverage in the analysis_assets and transcript_chunks tables and immediately materializes skip cues — the hot path becomes a cache hit.

**Architecture fit.** The infrastructure is 90% built:

- **AnalysisWorkScheduler** already has three lanes (Now/Soon/Background) with concurrency caps, thermal/battery gating, and lease-based job ownership.
- **AnalysisJobRunner** already executes the full decode → feature → transcript → classify pipeline.
- **CandidateWindowSelector** already handles unplayed episodes (its `playbackAnchor == nil` path selects the first 20 minutes from episode start).
- **BackgroundProcessingService** already registers BGProcessingTasks and submits on background transitions (playhead-fuo6 fix).
- **EpisodeExecutionLease** already prevents concurrent analysis of the same episode.

The missing piece is the trigger: when DownloadManager completes a download, it should enqueue an analysis job at `Soon` priority (or `Background` if the user hasn't manually queued the episode). The AnalysisJobReconciler's existing sweep can also pick up downloaded-but-unanalyzed episodes on app launch.

**What the user perceives.** "It just knew where the ads were before I even pressed play." The first skip fires within 1-2 seconds of pressing play — indistinguishable from a streaming service that has server-side ad metadata. This transforms the product from "clever tool that needs a minute to warm up" to "magic." No competing podcast app can match this because none of them have an on-device pre-analysis pipeline.

**Why I'm confident.** The value is unambiguous (eliminating the biggest UX friction), the architecture is ready (scheduling, execution, persistence, and resume are all built), and the risk is low (pre-analysis is purely additive — if it fails or doesn't finish in time, the existing hot-path pipeline kicks in as a fallback). The only real cost is battery/storage, and the existing thermal/battery/storage-budget gating handles that.

**EpisodeListView integration.** Show a subtle indicator per episode: a small mark (perhaps a muted Copper tick on the timeline preview) when pre-analysis is complete. This tells the user "this episode is ready" without being noisy. Fits the "Quiet Instrument" aesthetic — the intelligence is mostly invisible.

---

### 2. Cross-Episode Transcript Search

**The problem it solves.** Playhead transcribes every episode on-device. That corpus — potentially hundreds of hours of searchable text — is locked behind the TranscriptPeekView of individual episodes. The user can't answer "which episode mentioned that book?" or "find every time someone talked about climate policy." The transcript is a platform; right now it's being used as a feature.

**How it works.** A search UI (accessible from the Browse tab or a dedicated tab) lets users type a query. The query hits the existing `transcript_chunks_fts` FTS5 virtual table via the already-implemented `AnalysisStore.searchTranscripts(query:)` method. Results are grouped by episode, showing:

- Podcast artwork (stamp-sized) + episode title
- Matched transcript snippet with the query highlighted
- Timestamp of the match
- Tap → navigate to NowPlaying, seek to that timestamp

The FTS5 table uses content-sync triggers (`transcript_chunks_ai`, `transcript_chunks_ad`, `transcript_chunks_au`) so it stays up to date automatically as new chunks are written during analysis.

**Architecture fit.** This is the most "infrastructure-ready" idea on the list:

- **FTS5 virtual table** — created in `AnalysisStore.createTables()`, synced via triggers
- **`searchTranscripts(query:)`** — already implemented with FTS5 MATCH, query sanitization, and rank ordering
- **TranscriptChunk model** — carries `analysisAssetId`, `startTime`, `endTime`, and `text`
- **AnalysisAsset.episodeId** — bridges search results back to SwiftData Episode records
- **PlaybackService.seek(to:)** — already supports seeking to arbitrary timestamps

The only new code is the search UI and the navigation bridge (search result → load episode → seek to timestamp). No new services, no new data stores, no new analysis.

**What the user perceives.** "I can search everything ever said in any podcast I've listened to." This is a capability that doesn't exist in any podcast app — not Overcast, not Pocket Casts, not Apple Podcasts. It transforms Playhead from a podcast player into a podcast knowledge base. Users who listen to interview shows, educational podcasts, or news analysis would find this transformative.

**Why I'm confident.** The backend is literally already built and tested. The `searchTranscripts` method exists, the FTS5 index is maintained automatically, and the data model supports the full result → episode → timestamp navigation chain. The remaining work is purely UI — a search bar, a results list, and a navigation action. The risk is near-zero because FTS5 is a battle-tested SQLite feature. Combined with idea #1 (pre-analysis), the search corpus grows even for episodes the user hasn't played yet.

**Design note.** The search results view should use the editorial serif font for transcript snippets (matching TranscriptPeekView) and mono for timestamps, consistent with the "Quiet Instrument" aesthetic. Results grouped by episode, sorted by relevance within each group. No "X results found" counter — just the results.

---

### 3. Episode Summaries via On-Device Foundation Model

**The problem it solves.** Podcast backlogs are universal. A user subscribed to 15 shows has dozens of unplayed episodes. Show notes are often terrible (copy-pasted ad reads, generic descriptions, or just "In this episode, we talk to..."). The user has no good way to triage: which episodes are worth their time? What's actually discussed?

Playhead already has the transcript. The Foundation Model is already on-device. Connecting them to generate a 2-3 sentence summary per episode answers "what's this about?" before the user presses play — using actual content, not the publisher's marketing copy.

**How it works.**

1. After backfill transcription reaches sufficient coverage (e.g., ≥80% of episode duration), queue a summary extraction job.
2. Sample representative transcript text: first 3 minutes + a middle segment + last 3 minutes (avoids feeding the full transcript, which could be 50K+ tokens).
3. Feed to the on-device FM via schema-bound guided generation (same pattern as FoundationModelClassifier):
   ```swift
   struct EpisodeSummary: Codable {
       let summary: String          // 2-3 sentences
       let mainTopics: [String]     // 3-5 keyword/phrases
       let notableGuests: [String]  // names mentioned as guests
   }
   ```
4. Store in a new `episode_summaries` table in AnalysisStore, keyed by analysisAssetId.
5. Surface in EpisodeListView as an expandable subtitle below the episode title. Topic tags as small Soft Steel pills.

**Architecture fit.** Reuses the existing FM infrastructure:

- **FoundationModelClassifier** pattern — schema-bound `@Generable` struct, `LanguageModelSession`, capability gating via `CapabilitiesService`
- **Backfill scheduling** — runs in the same backfill pass as ad metadata extraction, or as a subsequent job
- **CapabilitiesService gating** — graceful degradation: no summary when FM unavailable. EpisodeListView shows the standard show-notes excerpt as fallback.
- **PermissiveAdClassifier** guardrail approach — for episodes about sensitive topics, use the permissive content transformation path to avoid refusals

New components: `EpisodeSummaryExtractor` (small service, ~200 lines), `episode_summaries` table (3 columns + FK), UI integration in EpisodeListView.

**What the user perceives.** "I can see what every episode is actually about — not the show notes, the real content." For power listeners with backlogs, this is a daily decision-support tool. It surfaces the value of the transcript corpus in a way that's immediately useful even if the user never opens TranscriptPeekView.

**Why I'm confident.** The value proposition is clear and the FM infrastructure is proven (FoundationModelClassifier already runs schema-bound generation in production). The main risk is FM guardrail refusals on controversial content — but summarization is a benign task (much less sensitive than ad classification), and the PermissiveAdClassifier path provides a fallback. Processing cost is bounded by the sampling strategy (don't feed the full transcript). The feature degrades gracefully when FM is unavailable — no summary shown, standard show-notes fallback. It ranked below search because search is infrastructure-ready today (FTS5 + query method exist), while summaries need new FM prompts and a new SQLite table.

---

### 4. Shareable Transcript Quotes

**The problem it solves.** You're listening to a podcast and hear something brilliant — a perfect quote, a surprising fact, a recommendation. You want to share it. Today your options are: screenshot the transcript peek (ugly, no context), type it out manually (tedious), or just... don't share it. Every un-shared moment is lost word-of-mouth for both the podcast and Playhead.

Playhead has a real-time, timestamped transcript. It should be trivially easy to select a passage and share it as a beautifully attributed quote — with a deep link back into the app.

**How it works.**

1. In TranscriptPeekView, add a long-press gesture on transcript lines (or a text-selection mode toggled by a subtle button).
2. User selects a range of text (highlight with Copper tint, consistent with current word highlighting).
3. A context menu appears: **Copy Quote** / **Share Quote**.
4. The formatted output:

   ```
   "The best time to start was ten years ago.
   The second best time is now."

   — The Diary of a CEO, "How to Build Discipline"
   12:47

   🎧 Shared from Playhead
   ```

5. **Copy Quote** puts this on `UIPasteboard`. **Share Quote** presents `UIActivityViewController` with the text (and optionally a `playhead://episode/{id}?t={seconds}` deep link as a URL attachment).
6. When someone with Playhead installed taps the deep link, the app opens to that episode at that timestamp.

**Architecture fit.** This is purely a UI feature — no new services, no new data stores, no new analysis:

- **TranscriptPeekView** already renders transcript chunks with timestamps and word-level sync
- **Episode metadata** (podcast title, episode title) is available via the SwiftData model
- **TimeFormatter** already exists in the Design layer for consistent timestamp formatting
- **Deep linking** — the app already has a URL scheme (or can register one trivially)

Implementation is ~200 lines of SwiftUI + a small `QuoteFormatter` struct. The gesture recognizer and share sheet are standard iOS APIs.

**What the user perceives.** "I can share the exact moment from a podcast, beautifully formatted, with one gesture." This is the kind of feature that creates organic social-media distribution. When someone posts a quote from Playhead with the attribution line, every reader sees the app name. It's free, authentic marketing that also provides genuine user value.

**Why I'm confident.** The implementation risk is near-zero (standard UIKit/SwiftUI APIs, no new data flows). The cost is low (~1-2 days of work). The value is high for the subset of users who share podcast content — and those users are exactly the "podcast power listener" target audience from PLAN.md. It ranked below summaries because it serves a narrower use case (sharing vs. daily triage), but it has the highest marketing leverage of any idea on this list.

**Design note.** The quote format should feel typographic and editorial — matching the "Quiet Instrument" identity. Serif font for the quote text in any rich-text rendering. No emoji in the attribution line (except optionally the headphones emoji, which reads as factual rather than playful). The Playhead branding line should be understated — "Shared from Playhead" not "✨ Made with Playhead AI ✨".

---

### 5. Sleep Timer with Intelligent Stop Points

**The problem it solves.** Every podcast app has a sleep timer. Every podcast app's sleep timer hard-cuts playback at an arbitrary clock time — mid-sentence, mid-word, mid-thought. You set "30 minutes" and at 30:00.000 the audio stops, possibly in the middle of "and the most important thing to remember is—". You either lose the ending or wake up to replay it.

Playhead already computes `pauseProbability` for every FeatureWindow in the analysis pipeline. It knows where the natural pauses are — sentence boundaries, paragraph breaks, speaker transitions, chapter ends. A sleep timer that stops at the *next natural break point* after the target time is a small feature with outsized delight.

**How it works.**

1. Sleep timer UI in NowPlayingView (standard options: 15, 30, 45, 60 min, end of episode, or custom).
2. Timer countdown runs as state on PlaybackService (new `sleepTimerTarget: Date?` property).
3. When the timer is ~45 seconds from expiry, PlaybackService queries FeatureWindows in AnalysisStore for the nearest high-pause-probability point:
   ```sql
   SELECT startTime, pauseProbability FROM feature_windows
   WHERE analysisAssetId = ?
     AND startTime BETWEEN ? AND ?
     AND pauseProbability > 0.6
   ORDER BY pauseProbability DESC
   LIMIT 1
   ```
   The search window is [targetTime - 15s, targetTime + 30s] — biased forward so the user gets slightly more content rather than less.
4. If a good pause point is found, begin a 3-second volume duck at that point, then pause.
5. If no pause point is found within the window, fall back to the nearest ad boundary, chapter boundary, or the exact target time (in that priority order).
6. Save a bookmark at the stop position so the user can see exactly where they left off.

**Architecture fit.** Clean integration with existing systems:

- **PlaybackService** already implements volume ducking for ad skip transitions (`duckVolume`, `duckDuration` constants). The sleep-timer duck uses the same mechanism.
- **FeatureWindows** are already persisted in SQLite with `pauseProbability` per window. No new analysis needed.
- **AnalysisStore** already has query methods for feature windows by analysisAssetId and time range.
- **Ad boundaries** are available from AdWindows in SQLite (secondary fallback).
- **Chapter boundaries** are available from ChapterEvidence (tertiary fallback).

New code: sleep timer state in PlaybackService (~50 lines), pause-point query in AnalysisStore (~20 lines), timer UI in NowPlayingView (~100 lines of SwiftUI).

**What the user perceives.** "The app always stops at a natural break — it never cuts me off mid-sentence." This is the kind of thoughtful detail that people mention in App Store reviews. It's not a switch driver on its own, but it compounds with daily use to create the feeling that Playhead *understands* audio in a way other apps don't. It's also a tangible, easy-to-explain proof of the on-device analysis: "Playhead knows where the pauses are because it actually analyzed the audio."

**Why I'm confident.** The data (`pauseProbability` in FeatureWindows) and the mechanism (volume ducking in PlaybackService) both already exist in production. The implementation is small and self-contained. The fallback chain (pause point → ad boundary → chapter boundary → exact time) means the feature always works, even when analysis is incomplete — it just gets better with more data. The risk is that `pauseProbability` quality might vary across episodes, but the 0.6 threshold and the fallback chain handle that gracefully.

**Design note.** The timer UI should feel like a physical dial or switch — not a modal picker. A subtle countdown in the NowPlayingView (perhaps replacing the remaining-time label when active) that doesn't add visual noise. When the sleep duck begins, a brief "Sleep" label fades in and out as the audio fades — the last thing the user sees before the screen dims.
