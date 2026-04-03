# Playhead — AI-Powered Podcast Player

## Vision

Playhead is a design-forward iOS podcast player that uses LLM-powered transcription to transform the listening experience. The core insight: once you have a real-time transcript, you can detect ads, extract mentions, enable search, and let users share precise segments — things no current player does well.

**Target user:** Podcast power listeners (10+ hrs/week) who are frustrated by ads, want to share specific moments, and value beautiful design.

**Design philosophy:** Not another "list of podcasts" app. Playhead should feel like a music player from the future — fluid animations, bold typography, a distinctive color palette, and interactions that feel physical. Think: the confidence of Things 3, the polish of Apollo, the personality of Overcast but pushed further.

---

## Full Feature Set (Prioritized)

### MVP (v0.1) — Ship First
1. **Ad Detection & Auto-Skip** — Multi-signal detector analyzes transcript in real-time, identifies ad segments, auto-skips them
2. **Ad Banners** — Skipped ads become dismissible banners showing what was advertised, so users can engage if interested

### v0.2 — Core Intelligence
3. **Full Transcript View** — Synced, scrollable transcript with current-word highlighting
4. **Search** — Full-text search across all transcribed episodes

### v0.3 — Social & Discovery
5. **Segment Sharing** — Highlight transcript region, generate shareable clip link
6. **Guest Link Extraction** — Detect URLs, products, books mentioned by guests; surface as tappable cards
7. **Mention Detection** — Tag all products, sites, people, companies mentioned; browseable index

---

## Architecture

### Why These Choices

| Decision | Choice | Why |
|----------|--------|-----|
| Platform | iOS native (Swift/SwiftUI) | Best audio APIs, background processing, design fidelity |
| UI Framework | SwiftUI + custom rendering | Fluid animations, rapid iteration, custom aesthetic |
| Minimum OS | iOS 26.0+ | Foundation Models framework availability starts here |
| Minimum Device | Apple Intelligence-capable iPhone (iPhone 15 Pro+) | Hardware floor for Apple Intelligence / Foundation Models |
| Capability Gating | `CapabilitiesService` + runtime self-test | Detect `SystemLanguageModel` availability, supported locale, Apple Intelligence state, background-task support, Low Power Mode, and thermal constraints |
| Playback Transport | AVPlayer / AVQueuePlayer | Native handling for remote/local media, seeking, buffering, background audio, Now Playing |
| Analysis Decode | AVAssetReader + AVAudioConverter | Deterministic 16kHz mono analysis path decoupled from playback |
| Transcription | On-device Whisper via WhisperKit | Legal liability precludes cloud; on-device = zero cost, full privacy |
| Ad Classification | Two-lane detector: hot path + backfill; Foundation Models optional | First usable skip should not depend on final transcript completeness or Foundation Models availability |
| Podcast Discovery | iTunes Search API + feed parser (MVP); provider abstraction retained | Keeps MVP network surface small; add Podcast Index only if real discovery gaps appear |
| Local Storage | SwiftData (library / user state) + SQLite/FTS5 (all analysis state) | Keep UI state simple; move write-heavy, versioned, resumable analysis into a single store |
| Networking | URLSession + async/await | Native, only needed for podcast feeds/audio download |
| Backend | None | Fully on-device architecture; no server needed |

### System Overview

```
┌──────────────────────────────────────────────────────────┐
│                      Playhead iOS                        │
│                                                          │
│  ┌──────────┐  ┌───────────┐  ┌───────────────────┐     │
│  │ Podcast  │  │ Playback  │  │  Analysis          │     │
│  │ Browser  │  │ Transport │  │  Coordinator       │     │
│  │          │  │           │  │                    │     │
│  │ Search   │  │ AVPlayer  │  │  Decode → Features │     │
│  │ Subscribe│  │ Skip      │  │  WhisperKit ASR   │     │
│  │ Library  │  │ Smoothing │  │  Ad Detection     │     │
│  └──────────┘  └─────┬─────┘  └────────┬──────────┘     │
│                      │                 │                 │
│              ┌───────┴─────────────────┴──────┐          │
│              │        Hot Path                │          │
│              │  Feature windows + fast ASR    │          │
│              │  → Acoustic + lexical candidates│          │
│              │  → CoreML classifier            │          │
│              ├────────────────────────────────┤          │
│              │        Backfill Path           │          │
│              │  Final ASR + boundary refine   │          │
│              │  Foundation Models (metadata)  │          │
│              │  Show-prior updates            │          │
│              └───────────┬────────────────────┘          │
│                          │                               │
│              ┌───────────┴──────────────┐                │
│              │    Skip Orchestrator     │                │
│              │  Hysteresis / snapping   │                │
│              │  Per-show trust scoring  │                │
│              │  Shadow → manual → auto  │                │
│              └───────────┬──────────────┘                │
│                          │                               │
│              ┌───────────┴──────────────┐                │
│              │     Ad Banner System     │                │
│              │                          │                │
│              │  Evidence-bound copy     │                │
│              │  Single quiet lane       │                │
│              │  Schema-driven metadata  │                │
│              └──────────────────────────┘                │
│                                                          │
│  ┌──────────────────────────────────────────────┐        │
│  │     SwiftData (Library / User State)         │        │
│  │  Podcasts, Episodes, Queue, Purchases,       │        │
│  │  UserPreferences                             │        │
│  ├──────────────────────────────────────────────┤        │
│  │     SQLite/FTS5 (All Analysis State)         │        │
│  │  AnalysisAssets, AnalysisSessions,           │        │
│  │  TranscriptChunks, FeatureWindows,           │        │
│  │  AdWindows, PodcastProfiles,                 │        │
│  │  PreviewBudgets, Search Index                │        │
│  └──────────────────────────────────────────────┘        │
└──────────────────────────────────────────────────────────┘
```

---

## Data Model (MVP)

### SwiftData — Library / User State

#### Podcast
- `id: UUID`
- `feedURL: URL`
- `title: String`
- `author: String`
- `artworkURL: URL?`
- `episodes: [Episode]`
- `subscribedAt: Date?`

#### Episode
- `id: UUID`
- `feedItemGUID: String?`
- `canonicalEpisodeKey: String`
- `podcast: Podcast`
- `title: String`
- `audioURL: URL`
- `cachedAudioURL: URL?`
- `downloadState: EpisodeDownloadState`
- `lastPlayedAnalysisAssetId: UUID?`
- `analysisSummary: EpisodeAnalysisSummary`
- `duration: TimeInterval`
- `publishedAt: Date`
- `playbackPosition: TimeInterval`
- `isPlayed: Bool`

### SQLite — All Analysis State

#### AnalysisAsset
- `id: UUID`
- `episodeId: UUID`
- `assetFingerprint: String`
- `weakFingerprint: String?`
- `sourceURL: URL`
- `featureCoverageEndTime: TimeInterval`
- `fastTranscriptCoverageEndTime: TimeInterval`
- `confirmedAdCoverageEndTime: TimeInterval`
- `analysisState: AnalysisState`
- `analysisVersion: String`
- `capabilitySnapshot: CapabilitySnapshot`

#### AnalysisSession
- `id: UUID`
- `analysisAssetId: UUID`
- `state: AnalysisState` — `.queued`, `.spooling`, `.featuresReady`, `.hotPathReady`, `.backfill`, `.complete`, `.failed`
- `startedAt: Date`
- `updatedAt: Date`
- `failureReason: String?`

#### FeatureWindow
- `analysisAssetId: UUID`
- `startTime: TimeInterval`
- `endTime: TimeInterval`
- `rms: Float`
- `spectralFlux: Float`
- `musicProbability: Float`
- `pauseProbability: Float`
- `speakerClusterId: Int?`
- `jingleHash: String?`
- `featureVersion: String`

#### TranscriptChunk
- `id: UUID`
- `analysisAssetId: UUID`
- `segmentFingerprint: String`
- `chunkIndex: Int`
- `startTime: TimeInterval`
- `endTime: TimeInterval`
- `text: String`
- `normalizedText: String`
- `pass: TranscriptPass` — `.fast`, `.final`
- `modelVersion: String`

#### AdWindow
- `id: UUID`
- `analysisAssetId: UUID`
- `startTime: TimeInterval`
- `endTime: TimeInterval`
- `confidence: Double`
- `boundaryState: BoundaryState`
- `decisionState: SkipDecisionState` — `.candidate`, `.confirmed`, `.suppressed`, `.applied`, `.reverted`
- `detectorVersion: String`
- `advertiser: String?`
- `product: String?`
- `adDescription: String?`
- `evidenceText: String?`
- `evidenceStartTime: TimeInterval?`
- `metadataSource: BannerMetadataSource` — `.regex`, `.lexicon`, `.foundationModel`, `.none`
- `metadataConfidence: Double`
- `metadataPromptVersion: String?`
- `wasSkipped: Bool`
- `userDismissedBanner: Bool`

#### PodcastProfile
- `podcastId: UUID`
- `sponsorLexicon: [String]`
- `normalizedAdSlotPriors: [Double]`
- `repeatedCTAFragments: [String]`
- `jingleFingerprints: [String]`
- `implicitFalsePositiveCount: Int`
- `skipTrustScore: Double`
- `observationCount: Int`
- `mode: SkipMode` — `.shadow`, `.manual`, `.auto`
- `recentFalseSkipSignals: Int`

#### PreviewBudget
- `canonicalEpisodeKey: String`
- `consumedAnalysisSeconds: TimeInterval`
- `graceBreakWindow: TimeInterval?`
- `lastUpdated: Date`

---

## MVP Implementation Plan

### Phase 1: Audio Foundation (Week 1)

#### 1.1 Project Setup
- Create Xcode project with SwiftUI lifecycle
- Set up folder structure:
  ```
  Playhead/
  ├── App/
  │   ├── PlayheadApp.swift
  │   └── ContentView.swift
  ├── Models/
  │   ├── Podcast.swift
  │   ├── Episode.swift
  │   ├── AnalysisAsset.swift
  │   └── AdWindow.swift
  ├── Views/
  │   ├── Library/
  │   ├── Player/
  │   ├── NowPlaying/
  │   └── Components/
  ├── Services/
  │   ├── PlaybackTransport/
  │   ├── AnalysisCoordinator/
  │   ├── AnalysisAudio/
  │   ├── FeatureExtraction/
  │   ├── AssetCache/
  │   ├── Downloads/
  │   ├── TranscriptEngine/    # WhisperKit
  │   ├── AdDetection/         # acoustic + lexical + classifier
  │   ├── SkipOrchestrator/
  │   ├── Capabilities/
  │   ├── Entitlements/
  │   └── PodcastFeed/
  ├── Persistence/
  │   ├── SwiftDataStore/      # library / user state
  │   └── AnalysisStore/       # SQLite/FTS5 — all analysis state
  ├── Design/
  │   ├── Theme.swift
  │   ├── Typography.swift
  │   └── Colors.swift
  └── Resources/
  ```
- Configure SwiftData schema + SQLite/FTS5 analysis store
- Set up `AssetProvider` + `ModelInventory`:
  - Versioned manifest
  - Checksum verification
  - Staged download directory
  - Atomic promote / rollback
  - Background delivery strategy (`Background Assets` on iOS 26+ / `URLSession` fallback)
- Build `CapabilitiesService`:
  - First-launch capability self-test
  - Detect `SystemLanguageModel` availability, supported locale, Apple Intelligence state
  - Detect background-task support, Low Power Mode, thermal constraints
  - Persist `CapabilitySnapshot` with each analysis run

#### 1.2 Podcast Feed Integration
- Implement RSS/Atom feed parser for podcast feeds
- Integrate iTunes Search API for discovery
- Add on-device search caching and debounce
- Keep `PodcastDiscoveryService`, but ship a single discovery provider in MVP:
  - `searchPodcasts(query:) async throws -> [Podcast]`
  - `fetchFeed(url:) async throws -> Podcast`
  - `refreshEpisodes(for:) async throws -> [Episode]`
- Parse GUID, normalized enclosure identity, artwork, audio URLs, show-note text, and available chapter metadata
- Treat show notes and chapter markers as weak sponsor priors / metadata hints, never authoritative labels
- Defer Podcast Index / backfill provider until the skip loop is stable

#### 1.3 Audio Playback & Analysis
- Build `PlaybackService` wrapping `AVPlayer`:
  - Standard controls: play, pause, seek, skip +/-15s/30s
  - Speed control: 0.5x-3.0x
  - **Skip cues**: accept `CMTimeRange`s from `SkipOrchestrator`
  - For streamed audio: short duck -> precise seek -> release
  - For fully cached local audio: optional two-item micro-crossfade
- Background audio session configuration
- Now Playing info center integration (lock screen controls)
- Handle interruptions (calls, Siri, other audio)
- Audio route changes (headphones unplugged, etc.)
- Build `AnalysisAudioService`:
  - Decode once into reusable 16kHz mono analysis shards
  - Persist low-cost feature windows so hot-path detection, boundary snapping, and backfill share the same decode work
  - Never share queues/threads with playback

#### 1.4 Basic Player UI
- Minimal but distinctive now-playing screen:
  - Stamp-sized artwork with no parallax
  - Playback controls with haptic feedback
  - Scrubber with time elapsed/remaining
  - Speed selector
- Wire up to PlaybackService

### Phase 2: On-Device Transcription Pipeline (Week 2)

#### 2.1 WhisperKit Integration
- Integrate [WhisperKit](https://github.com/argmaxinc/WhisperKit) via Swift Package Manager
  - Apple-optimized, CoreML/ANE acceleration
- **Model selection (dual-pass):**
  - Fast path model: low-latency model for immediate ad lookahead near the playhead
  - Final path model: `small` (or equivalent) for backfill and durable transcript quality
  - Models stored in `Application Support/Models`, not `Documents`
  - Download the fast-path ASR model first; defer heavier quality assets until they can be fetched without blocking first use
  - Keep classifier versioned independently from ASR model versions
- **Device performance targets:**
  - iPhone 15 Pro+ (A17 Pro): real-time or faster with `small` model
  - iPhone 16+ (A18): aggressive lookahead with fast + final passes
- Build `TranscriptEngineService`:
  - Accept audio file path (from `AnalysisAudioService`)
  - Run WhisperKit inference on background thread (never block UI/audio)
  - Stream fast-pass chunks ahead of the playhead
  - Promote chunks to final-pass transcript later when idle/charging
  - Parse output into `TranscriptChunk` records with word-level timestamps
- **Processing strategy:**
  - Use VAD/pause-anchored chunks (target 8-20s, small overlap)
  - Prioritize a dynamic wall-clock safety margin ahead of the playhead (converted to audio seconds at the current playback rate)
  - Keep hot-path coverage independent from final-pass transcript completeness
  - Cancel/reprioritize immediately on scrubs and major speed changes
  - Checkpoint per chunk hash for resumability
  - Use Metal/ANE acceleration via WhisperKit
- Store transcript chunks incrementally in SQLite
- Show real-time processing progress in UI

#### 2.2 Transcript Processing Pipeline
- Background processing queue:
  - Start hot-path feature extraction immediately when episode starts playing
  - Keep a rolling analysis hot zone around the playhead; size adapts to playback rate and thermal state
  - Schedule full-episode transcript/ad backfill opportunistically; never require it for first usable skip
  - Use `BGProcessingTask` for idle/charging backfill
  - Use `BGContinuedProcessingTask` only for user-initiated long-running work with visible progress (e.g., model installation)
  - Checkpoint after every feature block and transcript chunk so work can resume after suspension/termination without replaying decode
  - **Thermal management:** monitor `ProcessInfo.thermalState`, throttle on `.serious`/`.critical`
  - Respect battery state: pause transcription below 20% unless charging
- Progress tracking per episode (% complete, estimated time remaining)
- Resume interrupted transcriptions (track last processed chunk via `AnalysisSession`)
- Invalidate ad cache automatically when `assetFingerprint` changes (new `AnalysisAsset` created)
- **Storage management:**
  - Transcripts are compact (~50KB per hour of audio)
  - Models are versioned assets with a manifest, compatibility matrix, and eviction policy
  - Option to delete transcripts for played episodes

### Phase 3: On-Device Ad Detection (Week 2-3)

#### 3.1 Ad Classification Engine
- **Approach: Two-lane detector — hot path for time-critical skip decisions; backfill for quality, metadata, and priors**

**Hot Path** (must stay ahead of playback):

- **Layer 0 — Acoustic Boundary Finder**:
  - Detect music bed / stinger transitions via feature windows
  - Detect loudness and compression shifts
  - Detect speaker-change embeddings
  - Detect repeated jingle fingerprints (matched against `PodcastProfile`)

- **Layer 1 — Lexical Scanner** (runs on fast-pass transcript chunks, near-instant):
  - Regex/keyword matching for common ad phrases:
    - "brought to you by", "sponsored by", "today's sponsor"
    - "use code", "promo code", "discount code"
    - ".com/[podcast-name]", "dot com slash"
    - "check out", "head to", "go to [brand].com"
    - "free trial", "money-back guarantee"
  - Detect topic shifts / CTA patterns
  - Produces candidate ad regions with rough boundaries

- **Layer 2 — CoreML Sequence Classifier** (runs on candidates):
  - Score short windows using acoustic + lexical features
  - Smooth outputs across time to form stable ad spans
  - Incorporate per-show priors from `PodcastProfile` when generating candidates
  - Weight confidence by recurring sponsor patterns and historical slot priors

**Backfill Path** (runs when idle/charging, improves quality):

- **Layer 3 — On-Device Foundation Models** (metadata extraction / edge cases):
  - Use Apple's Foundation Models framework when available (gated by `CapabilitiesService`)
  - Extract only structured banner fields backed by evidence in the detected window
  - Use guided / schema-bound generation for `advertiser`, `product`, `evidenceText`, `confidence`
  - Re-run metadata extraction lazily when the prompt version or system model version changes
  - Never let Foundation Models be the sole reason a skip fires
  - **Not the primary classifier** — used only for enrichment and narrow arbitration
  - If Foundation Models are unavailable, only banner enrichment/arbitration degrades; ad detection and skip still work normally

- **Boundary refinement**: re-run classifier on final-pass transcript chunks with full context
- **Show-prior updates**: update `PodcastProfile` from confirmed skips, "Listen" rewinds, and backfill analysis

- Build `AdDetectionService`:
  - Input: `FeatureWindow`s + `TranscriptChunk`s (fast or final pass)
  - Run acoustic + lexical candidate generation (hot path)
  - Run discriminative classifier on candidates (hot path)
  - Run optional metadata extraction only when needed (backfill)
  - Output: `AdWindow`s with `decisionState` progression
  - Cache results in SQLite keyed by `analysisAssetId`

#### 3.2 Skip Integration
- Separate `SkipOrchestrator` from `AdDetectionService`
- Feed detected `AdWindow`s to `SkipOrchestrator` for decision-making
- Every skip decision is idempotent and keyed by `analysisAssetId + adWindowId + policyVersion`
- `SkipOrchestrator` consumes an event stream; it never queries SQLite synchronously from the playback callback path
- **Skip decision policy:**
  - Use enter/exit thresholds (hysteresis), not a single cutoff
  - Merge gaps < 4s between adjacent ad windows
  - Ignore spans < 15s unless sponsor evidence is very strong
  - Align skip boundaries to nearest silence / low-energy point
  - Never auto-skip until boundary is stable
  - Suppress auto-skip briefly after a user-initiated seek/rewind until confidence re-stabilizes
  - Ambiguous regions fail open silently; no "possible ad?" surface in MVP
  - Late detections never rewind the listener automatically
- **Per-show trust policy:**
  - New / low-trust shows start in `shadow` or `manual` mode unless confidence is exceptionally high
  - Promote to `auto` only after repeated local precision
  - Demote automatically after repeated "Listen" taps or rewind-after-skip behavior
- Implement skip behavior:
  - Default: auto-skip with duck/seek/release smoothing (when trust allows)
  - User setting: manual skip (show skip button during ads)
  - User setting: disable ad skip entirely
- Track skip statistics per podcast

#### 3.3 Ad Banner System
- When an ad is skipped, show a quiet horizontal strip (margin note, not alert):
  ```
  ┌─────────────────────────────────────────────────┐
  │  Skipped · Squarespace · "Build your website"   │
  │                         [Listen]    [Dismiss x]  │
  └─────────────────────────────────────────────────┘
  ```
- Slides in at bottom of now-playing screen, calm and typographic
- Auto-dismisses after 8 seconds
- "Listen" rewinds to snapped start and disables auto-skip for that span once
- Use a single banner lane with a queue; coalesce adjacent / near-adjacent skips
- If transcript-backed evidence is missing, always show generic copy: "Skipped sponsor segment"
- Never surface a brand solely because a model guessed it
- Keep banner text template-driven and typographic; no free-form generated marketing copy
- Store ad data for future features (mention detection, etc.)

### Phase 4: Design & Polish (Week 3-4)

#### 4.1 Design Direction — "Quiet Instrument"

**Design brief in one sentence:** Make it feel like a precision listening object, not an app with AI features.

**The product should feel like it was designed by:**
- an editorial art director
- a music software designer
- and a person who hates noisy interfaces

**The core idea: Editing, not blocking.** Playhead is a beautifully made listening instrument — half editorial system, half studio transport. Not a hacker tool, not a generic podcast app, and not an "AI" brand. It respects the conversation rather than attacking the ecosystem.

**Why "Quiet Instrument" fits:**
- *private* -> restrained, confident, not flashy
- *precise* -> crisp timing, sharp rails, exact markers
- *audio-native* -> transport controls, playhead logic, subtle motion
- *intelligent* -> the intelligence stays mostly invisible

**Two wrong aesthetics to avoid:**
- "AI app": purple gradients, sparkles, chatbot cues
- "Ad blocker": aggressive, adversarial, hacky

#### 4.2 The Playhead — Core Brand Motif

A single vertical line is the signature element across the entire product. Not a waveform. Not a microphone. Not headphones.

**The line appears:**
- in the app icon (dark field, single off-center copper line)
- in the now-playing timeline
- as a section divider throughout the UI
- in onboarding
- in marketing visuals
- in view transitions

**The line means:**
- playback
- timing
- precision
- moving past interruptions
- returning to a moment

Simple, ownable, directly tied to the name.

#### 4.3 Color Palette — Warm, Premium, Editorial

Avoid cold "tech blue." Go for something tactile and editorial — more gallery catalog / hi-fi object than consumer productivity app.

| Name | Hex | Usage |
|------|-----|-------|
| Ink | `#0E1116` | Primary background, dark surfaces |
| Charcoal | `#1A1F27` | Elevated surfaces, cards |
| Bone | `#F3EEE4` | Primary text on dark, light mode background |
| Copper | `#C96A3D` | **Signal accent** — playhead line, saved moments, key actions |
| Muted Sage | `#8C9B90` | Calm secondary states, inactive elements |
| Soft Steel | `#95A0AE` | Metadata, timestamps, tertiary text |

**Usage rules:**
- Most screens live in Ink / Bone
- Copper is the signal accent — used sparingly so the interface feels expensive
- Sage for calm secondary states
- Skipped ad segments: recessed, muted treatment (not highlighted or aggressive)
- Keep accent usage sparse; restraint is the point

#### 4.4 Typography — The Brand Carrier

The product should look typographic first. The conversation is the product.

| Role | Font | Where |
|------|------|-------|
| UI sans | Instrument Sans, Inter Tight, or Geist | Controls, labels, navigation |
| Editorial serif | Newsreader or Source Serif 4 | Transcript fragments, quotes, "mentioned by guest" snippets |
| Mono | IBM Plex Mono or Geist Mono | Timestamps, confidence values, precision markers |

**The non-obvious move: de-emphasize podcast cover art.** Most players are built around show art. Playhead should instead privilege time, transcript fragments, useful moments, and recovered mentions. Cover art can still exist, but smaller and more restrained — think stamp, not poster. This single decision makes the product feel immediately different.

#### 4.5 Design Primitives — Rails, Margins, and Markers

UI primitives come from editing systems, not generic app components:

| Primitive | Usage |
|-----------|-------|
| **Rails** | Progress bars, timeline tracks |
| **Ticks** | Time markers along rails |
| **Segment blocks** | Skipped ad ranges shown as subtle recessed blocks in the progress rail |
| **Pins / brackets** | Anchored markers for mentions and saved moments |
| **Margin cards** | Recovered products, books, links — styled like editorial annotations, not chat bubbles |

**Specific UI treatments:**
- Skipped segments: subtle recessed blocks in the progress rail (not highlighted or colored aggressively)
- Saved mentions: small anchored markers along the timeline
- Transcript excerpts: styled like margin notes, not chat bubbles
- Cards: long horizontal proportions, like cut strips or cue sheets
- No card soup — every element earns its space

#### 4.6 Motion — Precise, Not Playful

No bounce. No goo. No magical AI shimmer. Motion should feel like a clean scrub, a transport snap, a soft but exact cut.

**Specific behaviors:**
- Auto-skip triggers: playhead glides forward fast and settles (not a jump, not a bounce)
- Guest mention captured: a brief bracket closes around the line, then collapses into a saved marker
- Opening a saved moment: the interface re-centers around the timestamp (no theatrical flying)
- Tiny haptic confirmation for skips and saves — instrumental, not entertaining

**Avoid:**
- Spring/bounce physics on UI elements
- Parallax effects on artwork
- Gradient mesh backgrounds
- Shimmer loading states
- Any motion that draws attention to itself

#### 4.7 Now Playing Screen — The Hero

This is the most important screen. Minimal chrome, beautiful timeline, subtle skip markers, typographic hierarchy.

- **Timeline rail:** the dominant element — full-width, precise, with ad segments as recessed blocks
- **Playhead line:** copper vertical line, the same motif from the icon
- **Cover art:** present but restrained — small, stamp-sized, not a poster
- **Controls:** transport-style (play/pause, skip +/-15/30s, speed) — confident, not oversized
- **Transcript peek:** pull up from bottom to see live transcript in serif type
- **Ad banner:** when a skip occurs, a quiet horizontal strip slides in:
  ```
  ┌─────────────────────────────────────────────────┐
  │  Skipped · Squarespace · "Build your website"   │
  │                         [Listen]    [Dismiss x]  │
  └─────────────────────────────────────────────────┘
  ```
  Styled as a calm margin note, not an alert. Auto-dismisses after 8s.
- **Mini player:** when navigating away — thin bar with playhead line, title, play/pause
- **Lock screen / Dynamic Island:** standard Now Playing integration

#### 4.8 Library & Browse Views

- **Library:** Subscribed podcasts in a compact grid — artwork as stamps, not posters
  - Show name and unplayed count, not much else
  - Long-press for quick actions
  - Pull-to-refresh (no custom animation — keep it invisible)
- **Browse:** Search + editorial-feeling discovery
  - Search with instant results, typographic treatment
  - Category browsing
- **Episode list:** Per-podcast, text-led layout
  - Episode title (serif) with date and duration (mono)
  - Transcription status: subtle badge or progress tick
  - Ad count: small copper numeral, not a badge
  - Swipe actions: play, queue, mark played

#### 4.9 Settings

- WhisperKit model selection with download management and size indicators
- Ad skip behavior (auto/manual/off)
- Playback defaults (speed, skip intervals)
- Storage management (transcript cache, model files, cached audio)
- Background processing preferences (WiFi only, charging only, low-power fallback)

#### 4.10 First Aesthetic Proof — Four Artifacts

To validate the direction, design these four things first. If they feel coherent, the brand is real:

1. **App icon** — dark field, single off-center copper playhead line
2. **Now-playing screen** — minimal chrome, beautiful timeline, subtle skip markers, typographic hierarchy
3. **Episode page** — transcript-led, with "Mentioned in this episode" section styled like editorial annotations
4. **Landing page hero** — large typography, a moving vertical line, one transcript fragment, one recovered mention card

#### 4.11 What to Avoid (Checklist)

- [ ] Neon AI gradients
- [ ] Sparkle/star icons for "smart" features
- [ ] Oversized waveforms everywhere
- [ ] Microphones/headphones as central motif
- [ ] "Blocker" language and visual aggression
- [ ] Overly friendly blob shapes
- [ ] Busy dashboards full of analytics
- [ ] Purple anything
- [ ] Chatbot UI patterns
- [ ] The word "AI" in user-facing copy

The product should feel: calm, adult, exact, and slightly luxurious.

---

## Technical Considerations

### On-Device Processing Budget
- **Zero ongoing cost** — all transcription and classification run locally
- **One-time model downloads:**
  - WhisperKit models: fast-path + small (~500MB total)
  - CoreML ad classifier: small (~50-100MB)
  - Foundation Models: already on device (no download)
  - Total app footprint: ~100MB base + downloaded models
- **Battery impact:**
  - Transcription: significant CPU/GPU load; ~5-10% battery per hour of audio processed
  - Ad classification: lightweight (classifier runs on candidates only; Foundation Models only for metadata)
  - Mitigation: prefer processing while charging, throttle on battery

### Performance
- Playback transport and analysis decode MUST be fully decoupled — separate threads/queues, separate priorities
- `PlaybackCore`, `AnalysisCoordinator`, `SkipOrchestrator`, and `PersistenceWriter` are separate actors with explicit handoff boundaries
- Use Swift concurrency (async/await, actors) for thread safety
- Lazy load transcript chunks (only keep nearby chunks in memory)
- **Hot-path candidate detection must stay ahead of playback** — final-pass transcript completeness is best-effort and can lag without breaking skip reliability
- Run a first-launch capability self-test and persist a `CapabilitySnapshot` with each analysis run
- If Foundation Models are unavailable, degrade only banner enrichment / arbitration; never the core skip loop

### Asset Fingerprinting & Caching
- Cache transcript/ad results by `analysisAssetId` (which carries the `assetFingerprint`)
- For dynamic ad insertion, same episode does not mean same bytes — the enclosure URL can yield different inserted ads across listens
- Episode -> many `AnalysisAsset`s; `lastPlayedAnalysisAssetId` points to the currently relevant one
- Asset fingerprint: combination of enclosure URL + HTTP metadata + sampled content hash; promote to stronger fingerprint once audio is fully cached locally
- Progressive local audio cache backs transcription/classification — playback can start from remote, but analysis operates against local file view
- Audio/model downloads use resumable background transfers (`URLSession` background configuration) with integrity verification
- Prefer Apple-hosted / managed Background Assets on iOS 26+ for zero-ops delivery of large static model files

### Privacy & Security
- **All processing is on-device** — no audio or text ever leaves the phone
- No API keys needed (no cloud services)
- Transcripts stored locally only
- No analytics or tracking
- Apply file protection to analysis DB, transcript DB, model manifest, and local models
- This is a core selling point: "Your podcasts never leave your device"

### Error Handling
- Transcription failures: retry chunk, fall back to smaller/faster model, show partial results
- Ad detection failures: fail open (play everything, no skipping)
- Feed parse failures: show error, allow manual refresh
- Model download failures: retry with resume, show progress
- Analysis session failures: persist failure reason, allow manual retry

### Evaluation Harness
- Maintain a replay harness that simulates real player conditions:
  - Streamed vs fully cached audio
  - 0.5x-3.0x playback
  - Scrubs / skips / late detections
  - Dynamic ad variants for the same episode
  - Low Power Mode, route changes, thermal throttling
- Track:
  - False-positive skip seconds
  - False-negative ad seconds
  - Cut-speech milliseconds at resume
  - Time-to-first-usable-skip
  - Manual-override rate (`Listen`, rewind-after-skip)
  - p95 banner latency, battery drain, and thermal escalations
- Instrument decode / ASR / classify / skip with `os_signpost`, MetricKit, and versioned local diagnostics
- Export diagnostics only when explicitly initiated by the user, with transcript text redacted by default

---

## Future Features (Post-MVP)

These are documented for architectural awareness — don't build them yet, but don't make decisions that block them:

### Transcript View (v0.2)
- Full scrollable transcript synced with playback
- Tap any line to seek to that moment
- Current-word highlighting (karaoke-style)
- Ad segments visually distinct in transcript

### Search (v0.2)
- Full-text search across all transcribed episodes (powered by SQLite/FTS5)
- Search results show context + timestamp
- Tap result to play from that moment

### Segment Sharing (v0.3)
- Select text range in transcript
- Generate audio clip (server-side, needs backend)
- Shareable link with embedded player
- Deep link back into Playhead app

### Link & Mention Extraction (v0.3)
- On-device extractor identifies URLs, product names, book titles, and people
- Surfaced as cards below transcript
- Browseable index per podcast / across all podcasts

### Backend (when needed for sharing)
- Simple API for clip generation and hosting
- User accounts (optional, for sync)
- Shared clip pages (web)

---

## Monetization

**Model: Free preview -> One-time purchase (StoreKit 2 non-consumable)**

- **Free tier:** Full podcast player with playback, library, subscriptions. Each canonical episode gets a preview analysis budget of 12 decoded minutes; if the listener enters a detected ad break within that budget, finish that break (cap 20 decoded minutes total). Budget is keyed by `canonicalEpisodeKey`, not `assetFingerprint`, so dynamic ad variants and scrubs don't create loopholes.
- **Paid unlock (one-time):** Unlimited transcription + ad detection for all episodes. Price TBD ($9.99-$19.99 range — justified by zero ongoing costs).
- No subscription needed — all processing is on-device, so there are no marginal costs per user.
- This is a strong differentiator: "Pay once, skip ads forever."
- `EntitlementManager`:
  - Observes `Transaction.currentEntitlements` for silent unlock at launch
  - Listens to `Transaction.updates` for real-time entitlement changes
  - Uses explicit restore/sync only when the user taps "Restore Purchases"

---

## Success Criteria (MVP)

1. User can search for and subscribe to podcasts
2. User can play episodes with standard controls
3. Episodes are automatically transcribed on-device when played
4. Wrong-content auto-skip rate is below a strict trust threshold on the evaluation set
5. Median ad-boundary error is low enough to avoid audible mid-sentence cuts
6. Time-to-first-usable-skip is fast on target devices
7. Ambiguous regions fail open instead of causing false-positive skips
8. Skip transitions are perceptually clean (duck/seek/release or micro-crossfade)
9. Skipped ads appear as dismissible banners with evidence-bound copy
10. The app looks and feels distinctly premium — not a default SwiftUI app
11. All processing stays on-device — zero network calls for transcription/classification
12. Core skip reliability does not depend on unrestricted background runtime
13. The app remains fully useful when Foundation Models are unavailable; only banner richness degrades
14. Low-trust shows self-demote to manual/shadow mode without global settings changes

---

## Open Questions

1. **WhisperKit model pairing for dual-pass?** Need to benchmark which model combination gives the best latency/quality tradeoff for the fast-pass (near-playhead) vs final-pass (backfill).
2. **CoreML classifier training data?** Need a labeled corpus of podcast ad segments to train the sequence classifier. Consider starting with a rule-based bootstrapping approach.
3. **Acoustic boundary detection approach?** Need to evaluate off-the-shelf audio fingerprinting / speaker diarization libraries vs building lightweight custom CoreML models.
4. **FeatureWindow schema?** The acoustic feature set (rms, spectralFlux, musicProbability, etc.) needs validation through prototyping — which features actually predict ad boundaries?
5. **Pre-roll ads?** Dynamically inserted ads change per listen. The `AnalysisAsset` approach handles this naturally — different bytes produce a different fingerprint, triggering a new analysis asset.
6. **Legal position:** On-device processing avoids the cloud liability issue. All analysis happens locally on the user's own device — same as a user manually fast-forwarding. Worth confirming with counsel.
7. **Model download UX:** First-launch experience needs to handle ~500MB+ model downloads gracefully. Progressive download? Start with fast-path model, download final-path model in background?
8. **Shadow-to-auto promotion threshold?** Need to define what "repeated local precision" means quantitatively — how many correct skips before promoting a show to auto mode?
