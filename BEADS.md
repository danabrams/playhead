# Playhead — Beads (Task Graph)

> Import into bd once the database is working.
> Dependencies listed as `depends-on: [bead-id, ...]`
> Priority: P0 (highest) through P4 (lowest)

---

## Epics

### E1: Project Setup & Infrastructure
### E2: Podcast Feed Integration
### E3: Audio Playback & Analysis
### E4: Basic Player UI
### E5: On-Device Transcription Pipeline
### E6: Ad Detection Engine
### E7: Skip Orchestrator
### E8: Ad Banner System
### E9: Design & Polish
### E10: Evaluation & Testing

---

## E1: Project Setup & Infrastructure

### playhead-001 — Xcode Project Scaffold
- **type:** task
- **priority:** P0
- **depends-on:** []
- **description:**
  Create the Xcode project with SwiftUI lifecycle, iOS 26.0+ deployment target, and the full folder structure. This is the root task — everything else depends on a buildable project.

  **Folder structure:**
  ```
  Playhead/
  ├── App/ (PlayheadApp.swift, ContentView.swift)
  ├── Models/
  ├── Views/ (Library/, Player/, NowPlaying/, Components/)
  ├── Services/ (PlaybackTransport/, AnalysisCoordinator/, AnalysisAudio/,
  │              FeatureExtraction/, AssetCache/, Downloads/,
  │              TranscriptEngine/, AdDetection/, SkipOrchestrator/,
  │              Capabilities/, Entitlements/, PodcastFeed/)
  ├── Persistence/ (SwiftDataStore/, AnalysisStore/)
  ├── Design/ (Theme.swift, Typography.swift, Colors.swift)
  └── Resources/
  ```

  **Acceptance criteria:**
  - Project builds and runs on iOS 26 simulator
  - All directories exist with placeholder files
  - Bundle ID, display name, and app icon asset catalog configured

### playhead-002 — SwiftData Schema (Library / User State)
- **type:** task
- **priority:** P0
- **depends-on:** [playhead-001]
- **description:**
  Define the SwiftData models for library and user state. These are the reactive models that drive the UI — podcasts, episodes, queue, preferences, purchases.

  **Models:**
  - `Podcast` — id, feedURL, title, author, artworkURL, episodes, subscribedAt
  - `Episode` — id, feedItemGUID, canonicalEpisodeKey, podcast, title, audioURL, cachedAudioURL, downloadState, lastPlayedAnalysisAssetId, analysisSummary (lightweight struct summarizing analysis state for UI), duration, publishedAt, playbackPosition, isPlayed
  - `UserPreferences` — skipBehavior (auto/manual/off), playbackSpeed, skipIntervals, backgroundProcessingPrefs

  **Key decisions:**
  - `canonicalEpisodeKey` is derived from feedItemGUID + feedURL, used for preview budget tracking. This ensures dynamic ad variants of the same episode share a preview budget.
  - `analysisSummary` is a denormalized struct (not a relationship) so the UI never needs to query SQLite. Updated by the AnalysisCoordinator when analysis state changes.
  - `lastPlayedAnalysisAssetId` bridges SwiftData and SQLite — it's a UUID that points to the currently relevant AnalysisAsset in the SQLite store.

  **Acceptance criteria:**
  - SwiftData container initializes without errors
  - Can create, read, update, delete Podcast and Episode records
  - ModelContainer configured with appropriate migration plan

### playhead-003 — SQLite/FTS5 Analysis Store
- **type:** task
- **priority:** P0
- **depends-on:** [playhead-001]
- **description:**
  Set up the SQLite database for all analysis state. This is the write-heavy, versioned, resumable store that backs transcription, ad detection, and future search.

  **Why separate from SwiftData:** Analysis data is append-heavy, versioned, needs FTS5 for future search, and must support resumable processing with checkpointing. SwiftData's reactive object graph adds overhead we don't need here. Keeping analysis in raw SQLite also means the PersistenceWriter actor can batch writes without triggering SwiftUI observation.

  **Tables:**
  - `analysis_assets` — id, episodeId, assetFingerprint, weakFingerprint, sourceURL, featureCoverageEndTime, fastTranscriptCoverageEndTime, confirmedAdCoverageEndTime, analysisState, analysisVersion, capabilitySnapshot (JSON)
  - `analysis_sessions` — id, analysisAssetId, state (queued/spooling/featuresReady/hotPathReady/backfill/complete/failed), startedAt, updatedAt, failureReason
  - `feature_windows` — analysisAssetId, startTime, endTime, rms, spectralFlux, musicProbability, pauseProbability, speakerClusterId, jingleHash, featureVersion
  - `transcript_chunks` — id, analysisAssetId, segmentFingerprint, chunkIndex, startTime, endTime, text, normalizedText, pass (fast/final), modelVersion
  - `ad_windows` — id, analysisAssetId, startTime, endTime, confidence, boundaryState, decisionState (candidate/confirmed/suppressed/applied/reverted), detectorVersion, advertiser, product, adDescription, evidenceText, evidenceStartTime, metadataSource (regex/lexicon/foundationModel/none), metadataConfidence, metadataPromptVersion, wasSkipped, userDismissedBanner
  - `podcast_profiles` — podcastId, sponsorLexicon (JSON), normalizedAdSlotPriors (JSON), repeatedCTAFragments (JSON), jingleFingerprints (JSON), implicitFalsePositiveCount, skipTrustScore, observationCount, mode (shadow/manual/auto), recentFalseSkipSignals
  - `preview_budgets` — canonicalEpisodeKey, consumedAnalysisSeconds, graceBreakWindow, lastUpdated

  **FTS5 setup:**
  - Create FTS5 virtual table over transcript_chunks (text, normalizedText) for future v0.2 search
  - Use content-sync triggers so FTS stays up to date automatically

  **Acceptance criteria:**
  - Database creates cleanly with all tables and indexes
  - File protection applied (NSFileProtectionComplete)
  - Can insert and query each table
  - FTS5 virtual table returns search results against transcript text
  - WAL mode enabled for concurrent read/write

### playhead-004 — CapabilitiesService
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-001]
- **description:**
  Build a service that detects runtime capabilities and persists a CapabilitySnapshot with each analysis run. This prevents silent failures when Foundation Models or other optional features are unavailable.

  **What it checks:**
  - `SystemLanguageModel` availability (Foundation Models framework)
  - Apple Intelligence enabled state
  - Supported locale/language for Foundation Models
  - Device thermal state
  - Low Power Mode
  - Background task support (BGProcessingTask, BGContinuedProcessingTask)
  - Available disk space (for model downloads and audio cache)

  **Runtime contract:**
  - If Foundation Models are unavailable → only banner enrichment/arbitration degrades; ad detection and skip still work normally
  - If thermal state is serious/critical → throttle all analysis
  - If Low Power Mode → reduce hot-path lookahead window, defer backfill
  - Snapshot is a Codable struct persisted as JSON in the analysis_assets table

  **Why this matters:** Even on Apple Intelligence-capable hardware, Foundation Models can be unavailable due to locale, user settings, or temporary system state. The app must never crash or silently fail because of this.

  **Acceptance criteria:**
  - First-launch self-test runs and logs capability snapshot
  - Correctly detects Foundation Models availability on simulator
  - CapabilitySnapshot can be encoded/decoded and stored in SQLite
  - Publishes capability changes via AsyncStream for reactive consumers

### playhead-005 — AssetProvider & ModelInventory
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-001, playhead-003]
- **description:**
  Build the model delivery system. For a no-backend product, large ML models are the primary download/update concern. This service handles versioned model delivery with integrity verification.

  **Components:**
  - `ModelInventory` — tracks which models are available, their versions, sizes, and compatibility
  - `AssetProvider` — handles download, verification, staging, and promotion of model files
  - Versioned manifest (bundled in app, updatable)
  - Checksum verification (SHA256)
  - Staged download directory → atomic promote to active directory
  - Rollback capability if a new model version causes issues

  **Model storage:** `Application Support/Models/` (not Documents — not user-visible)

  **Download strategy:**
  - Fast-path ASR model downloaded first (unblocks hot-path analysis)
  - Final-path ASR model + classifier deferred until fast-path is ready
  - Background Assets framework on iOS 26+ for zero-ops delivery when available
  - Fall back to URLSession background transfers otherwise
  - Resume interrupted downloads

  **Acceptance criteria:**
  - Can download a test asset, verify checksum, stage, and promote atomically
  - ModelInventory correctly reports available/missing models
  - Interrupted downloads resume correctly
  - Models in Application Support are not visible in Files app

### playhead-006 — Design Token Files
- **type:** task
- **priority:** P2
- **depends-on:** [playhead-001]
- **description:**
  Create the design system foundation files implementing the "Quiet Instrument" aesthetic.

  **Files:**
  - `Theme.swift` — spacing scale, corner radii, shadow definitions, animation curves (precise, not bouncy)
  - `Typography.swift` — font definitions for UI sans (Instrument Sans / Inter Tight / Geist), editorial serif (Newsreader / Source Serif 4), mono (IBM Plex Mono / Geist Mono). Include semantic roles: .title, .body, .caption, .timestamp, .transcript
  - `Colors.swift` — full palette: Ink (#0E1116), Charcoal (#1A1F27), Bone (#F3EEE4), Copper (#C96A3D), Muted Sage (#8C9B90), Soft Steel (#95A0AE). Both light and dark mode variants. Semantic colors: .background, .surface, .text, .accent, .secondary, .metadata

  **Usage rules encoded in comments:**
  - Copper is the signal accent — used sparingly
  - Most screens live in Ink/Bone
  - No spring/bounce physics, no parallax, no shimmer
  - Cards use long horizontal proportions

  **Acceptance criteria:**
  - All colors render correctly in light and dark mode
  - Typography scales render correctly on all supported device sizes
  - A test view using all design tokens looks cohesive

### playhead-007 — EntitlementManager (StoreKit 2)
- **type:** task
- **priority:** P2
- **depends-on:** [playhead-002]
- **description:**
  Build the purchase/entitlement system using StoreKit 2 non-consumable.

  **Components:**
  - `EntitlementManager` actor
    - Observes `Transaction.currentEntitlements` for silent unlock at launch
    - Listens to `Transaction.updates` for real-time entitlement changes
    - Publishes `isPremium: Bool` via AsyncStream
    - Uses explicit restore/sync only when user taps "Restore Purchases"
  - `PreviewBudgetStore` (reads/writes preview_budgets table in SQLite)
    - Tracks consumed analysis seconds per `canonicalEpisodeKey`
    - 12 decoded minutes base budget
    - Grace window: if ad break starts within budget, finish that break (cap 20 min total)
    - Budget keyed by canonicalEpisodeKey so dynamic ad variants share the same budget

  **Testing:**
  - StoreKit configuration file in Xcode for local testing
  - Test premium unlock, restore, budget consumption, grace window

  **Acceptance criteria:**
  - Premium state correctly detected at launch without user action
  - Preview budget correctly tracks and enforces limits
  - Grace window extends budget when ad break starts near the limit
  - Restore Purchases works in sandbox

---

## E2: Podcast Feed Integration

### playhead-010 — RSS/Atom Feed Parser
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-002]
- **description:**
  Build a robust RSS/Atom feed parser for podcast feeds. This is the data ingestion layer — it must handle the messy reality of podcast RSS feeds.

  **Parse fields:**
  - Channel: title, author, description, artworkURL, language, categories
  - Items: title, GUID, enclosure URL, enclosure type/length, pubDate, duration, description/show notes, chapter metadata (Podcasting 2.0 chapters tag)
  - iTunes extensions: itunes:author, itunes:image, itunes:duration, itunes:episode

  **Key decisions:**
  - Parse GUIDs for stable episode identity across feed refreshes
  - Parse normalized enclosure identity (URL + type + length) for asset fingerprinting
  - Parse show-note text and chapter markers as weak sponsor priors (metadata hints, never authoritative labels)
  - Handle common feed quirks: missing GUIDs, relative URLs, malformed dates, duplicate episodes

  **Acceptance criteria:**
  - Parses top-50 podcast feeds without errors
  - Correctly extracts GUID, enclosure URL, chapters when present
  - Handles feeds with missing/malformed fields gracefully
  - Unit tests with real feed samples

### playhead-011 — PodcastDiscoveryService
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-010, playhead-002]
- **description:**
  Build the discovery service using iTunes Search API as the single MVP provider.

  **API:**
  - `searchPodcasts(query:) async throws -> [Podcast]`
  - `fetchFeed(url:) async throws -> Podcast`
  - `refreshEpisodes(for:) async throws -> [Episode]`

  **Implementation:**
  - iTunes Search API for podcast search/discovery
  - On-device search result caching (debounce + cache recent queries)
  - Rate limit handling (Apple docs note ~20 req/min)
  - Provider abstraction retained so Podcast Index can be added later without changing callers
  - Defer Podcast Index until skip loop is stable and real discovery gaps appear

  **Acceptance criteria:**
  - Search returns results for common podcast names
  - Feed fetch correctly populates Podcast + Episode models in SwiftData
  - Rate limiting doesn't crash or hang the UI
  - Results cache prevents redundant network calls

---

## E3: Audio Playback & Analysis

### playhead-020 — PlaybackService (AVPlayer)
- **type:** task
- **priority:** P0
- **depends-on:** [playhead-001, playhead-002]
- **description:**
  Build the playback transport layer wrapping AVPlayer. This is the "boring and durable" core — it handles long-form playback, buffering, seeking, and all the edge cases Apple handles natively.

  **Why AVPlayer, not AVAudioEngine:** AVPlayer natively handles remote/local media, buffering, seeking, background audio, Now Playing info center, and route changes. AVAudioEngine is for DSP and low-level audio graphs — overkill for podcast playback and harder to get right.

  **API:**
  - play, pause, seek, skip ±15s/30s
  - Speed control: 0.5x–3.0x
  - Accept `CMTimeRange`s from SkipOrchestrator as skip cues
  - Publish playback state (time, rate, status) via AsyncStream

  **Skip smoothing:**
  - For streamed audio: short duck → precise seek → release
  - For fully cached local audio: optional two-item micro-crossfade
  - Never promise a true crossfade for streamed assets — duck/seek/release is more reliable

  **Edge cases:**
  - Background audio session configuration (AVAudioSession.Category.playback)
  - Now Playing info center (lock screen controls, Dynamic Island)
  - Interruptions: calls, Siri, other audio apps
  - Route changes: headphones unplugged → pause
  - Speed changes while skip cues are active

  **Actor model:** PlaybackService is a global actor to serialize all playback operations. It never blocks on SQLite queries or analysis work.

  **Acceptance criteria:**
  - Can play remote and local podcast audio
  - Lock screen controls work
  - Speed control works across the full range
  - Skip cues cause perceptually clean transitions
  - Interruptions handled correctly (pause on call, resume after)
  - Route changes handled (pause on unplug)

### playhead-021 — AnalysisAudioService
- **type:** task
- **priority:** P0
- **depends-on:** [playhead-001]
- **description:**
  Build the analysis decode path — completely separate from playback. This decodes cached audio into 16kHz mono shards for ASR and feature extraction.

  **Why separate from playback:** Playback and analysis have fundamentally different requirements. Playback needs low-latency, uninterrupted output. Analysis needs deterministic, reproducible decoding for repeatable results. They must never share threads, queues, or audio sessions.

  **Implementation:**
  - Use AVAssetReader + AVAudioConverter to decode audio to 16kHz mono Float32
  - Output reusable analysis shards (short audio segments ready for WhisperKit and feature extraction)
  - Persist decoded shards so hot-path detection, boundary snapping, and backfill all share the same decode work (no redundant decoding)
  - Operate only against locally cached audio — playback can start from remote, but analysis waits for progressive local cache

  **Threading:** Runs on a dedicated background queue, never shares with PlaybackService.

  **Acceptance criteria:**
  - Decodes a podcast episode to 16kHz mono without errors
  - Output matches WhisperKit's expected input format
  - Decode never affects playback performance (verified via Instruments)
  - Handles truncated/corrupted audio files gracefully

### playhead-022 — Audio Asset Cache & Download Manager
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-003, playhead-002]
- **description:**
  Progressive local audio cache that backs analysis. Playback can start from remote media, but transcription/classification operate against a local file view for deterministic offsets and resumability.

  **Implementation:**
  - Progressive download: cache audio as it streams for playback
  - Background URLSession transfers for full episode pre-caching
  - Asset fingerprinting: enclosure URL + HTTP metadata (ETag, Content-Length, Last-Modified) + sampled content hash once enough audio is cached
  - Promote to strong fingerprint (full content hash) once fully cached
  - Create AnalysisAsset record in SQLite when fingerprint is established
  - Integrity verification on cached files (detect truncation/corruption)

  **Cache policy:**
  - LRU eviction with configurable max cache size
  - Keep audio for episodes with incomplete analysis
  - User can manually clear cache in Settings

  **Acceptance criteria:**
  - Audio streams for playback while progressively caching locally
  - Asset fingerprint correctly identifies same vs different audio for same episode URL
  - Background downloads complete and resume after interruption
  - Cache eviction works without deleting actively-analyzed episodes

---

## E4: Basic Player UI

### playhead-030 — Now Playing Screen (MVP)
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-020, playhead-006]
- **description:**
  The most important screen in the app. Minimal chrome, beautiful timeline, typographic hierarchy. This is where the Quiet Instrument aesthetic proves itself.

  **Layout:**
  - Stamp-sized artwork (small, not poster — de-emphasize cover art)
  - No parallax on artwork
  - Timeline rail: full-width, precise, with ad segments as subtle recessed blocks
  - Playhead line: copper vertical line (the brand motif)
  - Transport controls: play/pause, skip ±15/30s, speed — confident, not oversized
  - Scrubber with time elapsed/remaining (mono font for timestamps)
  - Haptic feedback on controls

  **Not in this bead:** Transcript peek, ad banners, skip markers — those come from E7/E8.

  **Acceptance criteria:**
  - Screen renders with design tokens (Ink/Bone/Copper palette, correct typography)
  - Controls are wired to PlaybackService
  - Speed selector works
  - Scrubber allows seeking
  - Feels like a precision instrument, not a default SwiftUI app

### playhead-031 — Library View
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-011, playhead-006]
- **description:**
  Subscribed podcasts in a compact grid.

  **Layout:**
  - Artwork as stamps, not posters
  - Show name + unplayed count
  - Long-press for quick actions (unsubscribe, mark all played)
  - Pull-to-refresh (no custom animation — keep it invisible)

  **Acceptance criteria:**
  - Grid renders subscribed podcasts
  - Unplayed counts update correctly
  - Pull-to-refresh triggers feed refresh
  - Empty state is clean and inviting

### playhead-032 — Browse & Search View
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-011, playhead-006]
- **description:**
  Search + editorial-feeling discovery powered by iTunes Search API.

  **Layout:**
  - Search bar with instant results, typographic treatment
  - Category browsing (optional — can ship without for MVP)
  - Search results show podcast title, author, artwork (stamp-sized)
  - Tap result → podcast detail → subscribe

  **Acceptance criteria:**
  - Search returns results as user types (debounced)
  - Results render with correct design tokens
  - Subscribe flow works end-to-end

### playhead-033 — Episode List View
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-031, playhead-006]
- **description:**
  Per-podcast episode list with text-led layout.

  **Layout:**
  - Episode title (serif) with date and duration (mono)
  - Transcription status: subtle badge or progress tick
  - Ad count: small copper numeral (not a badge)
  - Swipe actions: play, queue, mark played

  **Acceptance criteria:**
  - Episodes sorted by publish date (newest first)
  - Swipe actions work correctly
  - Tapping episode starts playback and navigates to Now Playing

### playhead-034 — Mini Player
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-030]
- **description:**
  Thin bar at the bottom of non-player screens. Shows playhead line, title, play/pause. Tap to expand to full Now Playing.

  **Acceptance criteria:**
  - Appears when audio is playing and user navigates away from Now Playing
  - Play/pause toggle works
  - Tap expands to full Now Playing screen
  - Doesn't interfere with tab bar or safe area

### playhead-035 — Settings View
- **type:** task
- **priority:** P2
- **depends-on:** [playhead-006, playhead-005, playhead-007]
- **description:**
  Settings screen with all user-configurable options.

  **Sections:**
  - WhisperKit model selection with download management and size indicators
  - Ad skip behavior (auto/manual/off)
  - Playback defaults (speed, skip intervals)
  - Storage management (transcript cache, model files, cached audio — with sizes)
  - Background processing preferences (WiFi only, charging only, low-power fallback)
  - Restore Purchases button

  **Acceptance criteria:**
  - All settings persist via UserPreferences in SwiftData
  - Model download/delete works from Settings
  - Storage usage is accurately reported
  - Restore Purchases triggers EntitlementManager restore flow

---

## E5: On-Device Transcription Pipeline

### playhead-040 — WhisperKit Integration
- **type:** task
- **priority:** P0
- **depends-on:** [playhead-005, playhead-021]
- **description:**
  Integrate WhisperKit for on-device speech recognition. WhisperKit is Apple-optimized with CoreML/ANE acceleration.

  **Model strategy (dual-pass):**
  - Fast path: low-latency model for immediate ad lookahead near the playhead (e.g., whisper-tiny or whisper-base)
  - Final path: `small` or equivalent for backfill and durable transcript quality
  - Fast-path model downloaded first — this unblocks the hot-path analysis loop
  - Final-path model deferred until fast-path is working

  **Integration:**
  - WhisperKit via Swift Package Manager
  - Configure for streaming/segment-level callbacks (not batch)
  - Use VAD (Voice Activity Detection) for natural chunk boundaries
  - Expose word-level and segment-level timestamps

  **Performance targets:**
  - iPhone 15 Pro+ (A17 Pro): real-time or faster with `small`
  - iPhone 16+ (A18): aggressive lookahead with fast + final passes

  **Acceptance criteria:**
  - WhisperKit initializes and loads models without errors
  - Can transcribe a podcast segment with word-level timestamps
  - VAD correctly identifies speech/silence boundaries
  - Runs on background thread without affecting playback

### playhead-041 — TranscriptEngineService
- **type:** task
- **priority:** P0
- **depends-on:** [playhead-040, playhead-003]
- **description:**
  The service that orchestrates transcription. Accepts decoded audio from AnalysisAudioService, runs WhisperKit, and writes TranscriptChunks to SQLite.

  **Processing strategy:**
  - Use VAD/pause-anchored chunks (target 8–20s, small overlap)
  - Prioritize a dynamic wall-clock safety margin ahead of the playhead (converted to audio seconds at current playback rate)
  - Hot-path coverage is independent from final-pass transcript completeness
  - Cancel/reprioritize immediately on scrubs and major speed changes
  - Checkpoint per chunk hash for resumability
  - Stream fast-pass chunks as they complete (don't wait for full episode)
  - Promote chunks to final-pass transcript later when idle/charging

  **Output:** TranscriptChunk records written incrementally to SQLite with:
  - segmentFingerprint (for dedup across passes)
  - pass (.fast or .final)
  - modelVersion (for cache invalidation on model updates)

  **Acceptance criteria:**
  - Transcription stays ahead of playback at 1x speed on iPhone 15 Pro
  - Chunks are correctly written to SQLite with all metadata
  - Scrubbing forward causes reprioritization (new region transcribed first)
  - Interrupted transcription resumes from last checkpoint

### playhead-042 — AnalysisCoordinator
- **type:** task
- **priority:** P0
- **depends-on:** [playhead-041, playhead-021, playhead-003, playhead-004]
- **description:**
  The central orchestrator that coordinates all analysis work. It receives playback events, manages AnalysisSessions, and dispatches work to the transcription, feature extraction, and ad detection services.

  **Why this exists:** Without explicit coordination, play/pause, scrub, cache progress, transcript chunks, candidate windows, and skip cues can all arrive in different orders, creating race conditions. The AnalysisCoordinator makes every transition explicit and resumable.

  **State machine (AnalysisSession):**
  - `.queued` → audio identified, waiting for cache
  - `.spooling` → audio caching, decode starting
  - `.featuresReady` → feature windows extracted for hot zone
  - `.hotPathReady` → hot-path ad detection complete, skip cues available
  - `.backfill` → final-pass ASR and metadata extraction running
  - `.complete` → all analysis done
  - `.failed` → with failureReason, retryable

  **Inputs:** Playback state changes (time, rate, play/pause, scrub) from PlaybackService
  **Outputs:** Dispatches work to TranscriptEngineService, FeatureExtraction, AdDetectionService

  **Actor model:** Separate actor with explicit handoff boundaries to PlaybackCore, SkipOrchestrator, and PersistenceWriter. Never blocks on playback callbacks.

  **Acceptance criteria:**
  - State machine transitions are correct and persisted
  - Scrubbing triggers reprioritization without losing existing work
  - Crash recovery resumes from persisted session state
  - All state transitions logged for debugging

### playhead-043 — Background Processing Strategy
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-042]
- **description:**
  Implement the background processing strategy based on iOS reality.

  **Core principle:** Foreground hot-zone analysis is the MVP reliability path. Background work only improves completeness. Never require background completion for first usable skip.

  **Implementation:**
  - Hot-path: runs in foreground whenever audio is playing. Rolling analysis hot zone adapts to playback rate and thermal state.
  - `BGProcessingTask`: registered for idle/charging backfill (full-episode final-pass transcript, metadata extraction, show-prior updates). This is opportunistic — system may not grant runtime.
  - `BGContinuedProcessingTask`: used ONLY for user-initiated long-running work with visible progress (e.g., initial model download/installation). Not for analysis.
  - Checkpoint after every feature block and transcript chunk so work resumes without replaying decode.

  **Thermal management:**
  - Monitor `ProcessInfo.thermalState`
  - `.nominal`/`.fair`: full analysis
  - `.serious`: reduce hot-path window, pause backfill
  - `.critical`: pause all analysis

  **Battery management:**
  - Below 20% and not charging: pause all non-critical analysis
  - Low Power Mode: reduce hot-path lookahead, defer backfill entirely

  **Acceptance criteria:**
  - Hot-path analysis runs during foreground playback
  - BGProcessingTask registered and runs during idle/charging
  - Thermal throttling reduces work without crashing
  - Analysis resumes correctly after app suspension/termination

---

## E6: Ad Detection Engine

### playhead-050 — Feature Extraction Service
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-021, playhead-003]
- **description:**
  Extract acoustic features from decoded audio shards for the ad detection hot path. These features power Layer 0 (acoustic boundary finder) and Layer 2 (classifier).

  **Features per window:**
  - RMS energy (volume level)
  - Spectral flux (timbral change — music beds, stingers)
  - Music probability (music vs speech classifier)
  - Pause probability (silence/low-energy detection)
  - Speaker cluster ID (speaker change detection)
  - Jingle hash (repeated audio fingerprint matching)

  **Note:** The exact feature set needs validation through prototyping. Some features (especially jingle hashing and speaker clustering) may be deferred if they don't prove useful for ad boundary detection. Start with RMS, spectral flux, and pause detection — these are cheapest and most likely to help.

  **Output:** FeatureWindow records written to SQLite, reusable by both hot-path and backfill.

  **Acceptance criteria:**
  - Feature extraction runs faster than real-time on target devices
  - Features are persisted and don't need recomputation
  - Feature windows align with transcript chunk boundaries

### playhead-051 — Lexical Scanner (Layer 1)
- **type:** task
- **priority:** P0
- **depends-on:** [playhead-041]
- **description:**
  Fast regex/keyword scanner that runs on transcript chunks to produce candidate ad regions. This is the workhorse of the hot path — it catches ~60-70% of ads with high precision and near-zero latency.

  **Patterns:**
  - Sponsor phrases: "brought to you by", "sponsored by", "today's sponsor", "thanks to our sponsor"
  - Promo codes: "use code", "promo code", "discount code", "coupon code"
  - URLs/CTAs: ".com/[podcast-name]", "dot com slash", "check out", "head to", "go to [brand].com"
  - Purchase language: "free trial", "money-back guarantee", "first month free", "percent off"
  - Transition markers: "let's get back to", "and now back to", "anyway", "so"

  **Output:** Candidate ad regions with rough start/end times and a lexical confidence score. These feed into the classifier (Layer 2) for refinement.

  **Implementation:**
  - Compiled regex patterns for performance
  - Sliding window over normalized transcript text
  - Merge adjacent hits within configurable gap threshold
  - Include per-show sponsor lexicon from PodcastProfile if available

  **Acceptance criteria:**
  - Correctly identifies sponsor reads in test corpus
  - Runs in milliseconds per chunk
  - Produces candidates with reasonable (not perfect) boundaries
  - Per-show lexicon boosts detection for known sponsors

### playhead-052 — CoreML Sequence Classifier (Layer 2)
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-050, playhead-051]
- **description:**
  Small CoreML model that scores candidate regions using acoustic + lexical features. This refines the rough candidates from Layer 1 into stable ad spans with calibrated confidence.

  **Architecture:**
  - Input: feature windows + lexical scores for a candidate region
  - Output: ad probability, boundary adjustment suggestions
  - Small model (~50-100MB) trained on labeled podcast ad segments
  - Smoothing across time to prevent flickering decisions

  **Training data challenge:** We need a labeled corpus. Bootstrap approach:
  1. Use lexical scanner to auto-label high-confidence examples
  2. Manually label a smaller set of edge cases
  3. Train initial classifier, iterate

  **Per-show priors:**
  - Incorporate PodcastProfile priors (ad slot timing, sponsor patterns)
  - Weight confidence by recurring patterns and historical slot positions

  **Acceptance criteria:**
  - Classifier improves boundary accuracy over lexical-only detection
  - Runs fast enough for hot-path use (< 100ms per candidate)
  - Confidence scores are well-calibrated (high confidence = high precision)
  - Per-show priors measurably improve detection on repeat listens

### playhead-053 — Foundation Models Metadata Extraction (Layer 3)
- **type:** task
- **priority:** P2
- **depends-on:** [playhead-004, playhead-051]
- **description:**
  Optional enrichment layer using Apple's Foundation Models framework. Extracts structured metadata for banner copy and arbitrates borderline cases. NEVER the primary classifier.

  **Gating:** Only runs when CapabilitiesService confirms Foundation Models availability. If unavailable, only banner enrichment degrades — detection and skip are unaffected.

  **Implementation:**
  - Schema-bound guided generation for: advertiser, product, evidenceText, confidence
  - Evidence-bound: only extract metadata when there's transcript text backing it in the detected window
  - Prompt versioned (metadataPromptVersion stored in AdWindow)
  - Re-run lazily when prompt version or system model version changes
  - Never let Foundation Models be the sole reason a skip fires

  **Backfill only:** This runs in the backfill path, not the hot path. Banner enrichment can arrive after the skip fires — that's fine.

  **Acceptance criteria:**
  - Correctly extracts advertiser/product from transcript evidence
  - Falls back gracefully when Foundation Models unavailable
  - Schema-bound output, no free-form generation
  - Prompt version tracked and metadata re-extractable on version change

### playhead-054 — AdDetectionService
- **type:** task
- **priority:** P0
- **depends-on:** [playhead-051, playhead-052, playhead-003]
- **description:**
  The service that composes the detection layers and outputs AdWindows.

  **Hot path flow:**
  1. Receive FeatureWindows + fast-pass TranscriptChunks from AnalysisCoordinator
  2. Run Layer 1 (lexical scanner) → candidate regions
  3. Run Layer 0 (acoustic boundary finder) → refine boundaries using feature windows
  4. Run Layer 2 (classifier) → scored AdWindows with decisionState = `.candidate`
  5. Push confirmed AdWindows to SkipOrchestrator

  **Backfill flow:**
  1. Re-run classifier on final-pass transcript chunks with full context
  2. Run Layer 3 (Foundation Models metadata extraction) on confirmed windows
  3. Update PodcastProfile priors from confirmed skips and "Listen" rewinds
  4. Promote `.candidate` windows to `.confirmed` or `.suppressed`

  **Caching:** Results keyed by analysisAssetId in SQLite. Different audio bytes = different AnalysisAsset = fresh analysis.

  **Acceptance criteria:**
  - Hot path produces skip-ready AdWindows ahead of playback
  - Backfill enriches metadata without disrupting active skips
  - PodcastProfile priors update correctly from user behavior
  - Cache correctly invalidated when asset fingerprint changes

---

## E7: Skip Orchestrator

### playhead-060 — SkipOrchestrator State Machine
- **type:** task
- **priority:** P0
- **depends-on:** [playhead-054, playhead-020]
- **description:**
  The decision layer between detection and playback. This is where user trust is protected.

  **Why separate from AdDetectionService:** A model can think something is "probably an ad" without the transport being allowed to skip yet. Detection confidence and skip authorization are different things.

  **Skip decision policy:**
  - Hysteresis: higher threshold to enter ad state, lower threshold to stay in it (prevents rapid on/off)
  - Merge gaps < 4s between adjacent ad windows
  - Ignore spans < 15s unless sponsor evidence is very strong
  - Align skip boundaries to nearest silence / low-energy point (using FeatureWindows)
  - Never auto-skip until boundary is stable
  - Suppress auto-skip briefly after user-initiated seek/rewind until confidence re-stabilizes
  - Ambiguous regions fail open silently — no "possible ad?" UI in MVP
  - Late detections never rewind the listener automatically

  **AdWindow.decisionState progression:**
  `.candidate` → `.confirmed` (classifier + policy agree) → `.applied` (skip fired) or `.suppressed` (policy rejected)
  `.applied` → `.reverted` (user tapped "Listen")

  **Every skip decision is idempotent and keyed by `analysisAssetId + adWindowId + policyVersion`.**

  **Event-stream architecture:** SkipOrchestrator consumes an event stream of AdWindows from AdDetectionService. It NEVER queries SQLite synchronously from the playback callback path. Skip cues are pushed to PlaybackService as CMTimeRanges.

  **Acceptance criteria:**
  - Hysteresis prevents rapid skip on/off
  - Short gaps between ads are merged
  - Boundaries snap to silence (no mid-sentence cuts)
  - Skip suppressed after user seek until re-stabilized
  - Late detections don't cause rewinds
  - All decisions logged for evaluation harness

### playhead-061 — Per-Show Trust Scoring
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-060, playhead-003]
- **description:**
  Per-show trust policy that controls skip mode. This is the key product-level trust protection.

  **Skip modes per show:**
  - `.shadow` — detection runs, results logged, but no skips fire. Used for brand-new shows.
  - `.manual` — user sees a "Skip Ad" button during detected ads but no auto-skip. Used for low-trust shows.
  - `.auto` — full auto-skip. Only for shows with proven local precision.

  **Promotion/demotion rules:**
  - New shows start in `.shadow` unless first-episode confidence is exceptionally high
  - Promote `.shadow` → `.manual` after N observations with acceptable precision
  - Promote `.manual` → `.auto` after repeated correct skips with no "Listen" reversions
  - Demote `.auto` → `.manual` after repeated "Listen" taps or rewind-after-skip behavior
  - Demote `.manual` → `.shadow` if false-positive signals accumulate

  **Stored in PodcastProfile:** skipTrustScore, observationCount, mode, recentFalseSkipSignals

  **User override:** User can always manually set a show to auto/manual/off in Settings, overriding the trust score. But the default behavior is earned trust.

  **Acceptance criteria:**
  - New shows start in shadow mode
  - Promotion happens after consistent precision
  - Demotion happens after "Listen" taps / rewind behavior
  - User override works and is respected

---

## E8: Ad Banner System

### playhead-070 — Banner UI Component
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-030, playhead-006]
- **description:**
  The banner that appears when an ad is skipped. Styled as a calm margin note, not an alert.

  **Layout:**
  ```
  ┌─────────────────────────────────────────────────┐
  │  Skipped · Squarespace · "Build your website"   │
  │                         [Listen]    [Dismiss x]  │
  └─────────────────────────────────────────────────┘
  ```

  **Behavior:**
  - Slides in at bottom of Now Playing screen
  - Auto-dismisses after 8 seconds
  - Single banner lane with queue — coalesce adjacent/near-adjacent skips
  - Never stack multiple banners
  - Haptic on appear (subtle, not attention-grabbing)

  **Design:**
  - Ink background, Bone text, Copper accent on "Listen"
  - Long horizontal proportions (cue sheet style)
  - Typographic: sans for label, mono for metadata

  **Acceptance criteria:**
  - Banner renders with correct design tokens
  - Slides in/out smoothly
  - Queue coalesces rapid sequential skips
  - Auto-dismiss works
  - Doesn't interfere with Now Playing controls

### playhead-071 — Banner Evidence & Copy Logic
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-070, playhead-054]
- **description:**
  Wire banners to AdWindow metadata with strict evidence-bound copy rules.

  **Copy rules:**
  - If `evidenceText` is present and `metadataConfidence` is above threshold → show "Skipped · [advertiser] · [product/description]"
  - If evidence is weak or missing → show generic: "Skipped sponsor segment"
  - Never surface a brand solely because a model guessed it
  - Banner text is template-driven, never free-form generated copy
  - `metadataSource` tracks where the copy came from (regex, lexicon, foundationModel, none)

  **"Listen" behavior:**
  - Rewinds to the snapped start boundary of the skipped ad window
  - Disables auto-skip for that specific span once (the AdWindow's decisionState → `.reverted`)
  - Feeds back to PodcastProfile as a potential false-positive signal

  **Acceptance criteria:**
  - High-confidence metadata shows advertiser/product
  - Low-confidence shows generic copy
  - "Listen" correctly rewinds and plays through the ad
  - "Listen" tap updates trust scoring
  - No hallucinated sponsor names ever appear

---

## E9: Design & Polish

### playhead-080 — App Icon
- **type:** task
- **priority:** P2
- **depends-on:** [playhead-006]
- **description:**
  Dark field, single off-center copper playhead line. The brand motif at its most distilled.

  **Acceptance criteria:**
  - Renders clearly at all icon sizes (1024px down to 60px)
  - Reads as intentional and distinctive at small sizes
  - No gradients, no shimmer, no busy detail

### playhead-081 — Onboarding Flow
- **type:** task
- **priority:** P2
- **depends-on:** [playhead-005, playhead-007, playhead-030]
- **description:**
  First-launch experience. Must handle model downloads gracefully while communicating the product's value.

  **Flow:**
  1. Welcome screen with the playhead line motif
  2. Brief value prop (1-2 screens, not a tutorial)
  3. Model download with progress (fast-path model first, final-path in background)
  4. Search and subscribe to first podcast
  5. Play an episode → experience the first ad skip

  **Key:** The "aha" moment should happen as quickly as possible. The preview budget (12 min + grace window) is designed to make the first ad skip reliably land during the first listen.

  **Acceptance criteria:**
  - Model download shows clear progress
  - User can start listening before all models finish downloading
  - Flow feels premium, not utilitarian

### playhead-082 — Now Playing Skip Markers
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-030, playhead-060]
- **description:**
  Visual treatment of detected ad segments in the Now Playing timeline rail.

  **Design:**
  - Ad segments shown as subtle recessed blocks in the progress rail
  - Not highlighted or colored aggressively — recessed, muted treatment
  - Segment blocks use Charcoal (#1A1F27) or slightly darker than rail background
  - Applied skips show the playhead gliding forward fast and settling (not a jump, not a bounce)

  **Acceptance criteria:**
  - Ad segments visible in timeline as recessed blocks
  - Skip animation is smooth and precise
  - Segments update in real-time as detection produces results

### playhead-083 — Transcript Peek (Now Playing)
- **type:** task
- **priority:** P2
- **depends-on:** [playhead-030, playhead-041]
- **description:**
  Pull-up sheet from bottom of Now Playing to see live transcript in serif type. This is a preview of the v0.2 full transcript view.

  **Layout:**
  - Serif font (Newsreader / Source Serif 4) for transcript text
  - Current word or segment highlighted with Copper
  - Ad segments visually distinct (muted/recessed)
  - Pull up to expand, pull down to dismiss

  **Not in MVP scope:** Tap-to-seek, full transcript view, search. This is just a peek.

  **Acceptance criteria:**
  - Live transcript scrolls with playback
  - Current position highlighted
  - Renders fast-pass chunks (doesn't wait for final pass)
  - Feels like reading, not like a debug view

---

## E10: Evaluation & Testing

### playhead-090 — Replay Simulator
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-054, playhead-060, playhead-020]
- **description:**
  Test harness that simulates real player conditions for evaluating detection and skip quality.

  **Simulated conditions:**
  - Streamed vs fully cached audio
  - 0.5x–3.0x playback speeds
  - Scrubs / skips / late detections
  - Dynamic ad variants for the same episode
  - Low Power Mode, route changes, thermal throttling

  **Metrics tracked:**
  - False-positive skip seconds
  - False-negative ad seconds
  - Cut-speech milliseconds at resume (boundary quality)
  - Time-to-first-usable-skip
  - Manual-override rate (Listen taps, rewind-after-skip)
  - p95 banner latency
  - Battery drain and thermal escalations

  **Instrumentation:**
  - os_signpost for decode / ASR / classify / skip pipeline timing
  - MetricKit integration for production diagnostics
  - Versioned local diagnostics (tied to analysisVersion)
  - Export diagnostics only when explicitly initiated by user
  - Transcript text redacted by default in exports

  **Acceptance criteria:**
  - Can replay a labeled episode and produce metrics report
  - Simulated scrubs and speed changes produce correct behavior
  - Metrics match expected values on labeled corpus

### playhead-091 — Labeled Test Corpus
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-010]
- **description:**
  Build and maintain a labeled set of episodes with annotated sponsor boundaries and metadata. This is the ground truth for the evaluation harness.

  **Corpus requirements:**
  - 10-20 episodes across different genres and ad styles
  - Labeled: ad start/end times, advertiser, product, ad type (host-read, dynamically inserted, pre-roll, mid-roll)
  - Include edge cases: blended host-read ads, very short ads, back-to-back ads, no-ad episodes
  - Include at least 2 episodes from the same show for testing per-show priors
  - Include dynamic ad insertion variants (same episode, different ad fills)

  **Format:** JSON annotations alongside cached audio files, stored in test fixtures.

  **Acceptance criteria:**
  - Corpus covers main ad styles and edge cases
  - Annotations are accurate (verified by human listening)
  - Replay simulator can consume corpus and produce metrics

### playhead-092 — Unit Tests (Core Services)
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-003, playhead-041, playhead-054, playhead-060]
- **description:**
  Unit tests for core services with detailed logging for debugging.

  **Coverage:**
  - SQLite schema: CRUD operations, FTS5 queries, migration
  - TranscriptEngineService: chunk processing, checkpoint/resume, dedup across passes
  - AdDetectionService: lexical scanner patterns, candidate merging, confidence scoring
  - SkipOrchestrator: hysteresis, gap merging, silence snapping, trust scoring
  - CapabilitiesService: capability detection, snapshot persistence
  - EntitlementManager: premium state, preview budget enforcement, grace window
  - AssetProvider: download, verify, stage, promote, rollback

  **Acceptance criteria:**
  - All core services have unit tests
  - Tests run in CI without device
  - Detailed logging on failure for debugging

### playhead-093 — Integration Tests (End-to-End)
- **type:** task
- **priority:** P1
- **depends-on:** [playhead-092, playhead-090]
- **description:**
  End-to-end tests that exercise the full pipeline: audio → decode → transcribe → detect → skip → banner.

  **Test scenarios:**
  - Play episode with known ads → verify skip fires at correct times
  - Scrub past ad → verify no retroactive skip
  - Play at 2x → verify hot-path keeps up
  - Kill and relaunch → verify analysis resumes from checkpoint
  - Foundation Models unavailable → verify detection still works, only banners degrade
  - Preview budget exhausted → verify transcription stops, playback continues

  **Acceptance criteria:**
  - Full pipeline works end-to-end on device
  - All failure modes handled gracefully
  - Tests produce clear pass/fail with diagnostic output

---

## Dependency Summary

```
E1 (Setup) ──┬── E2 (Feed) ──── E4 (UI)
              │
              ├── E3 (Playback) ── E4 (UI)
              │
              ├── E5 (Transcription) ── E6 (Detection) ── E7 (Skip) ── E8 (Banner)
              │                                                │
              │                                                └── E9 (Polish)
              │
              └── E10 (Testing) ── spans all phases

Critical path: E1 → E3 → E5 → E6 → E7 (playback → transcription → detection → skip)
Parallel track: E1 → E2 → E4 (feed → UI)
```
