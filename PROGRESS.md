# Playhead — Planning Progress

## Current Status

We are at **step 5 (Polish Beads)** of the planning-workflow skill. Seven polish rounds complete.

## What's Been Done

### Planning Phase
1. **PLAN.md v1** — Initial comprehensive plan drafted with Claude
2. **GPT Pro Review Round 1** — 12 revisions integrated (AVPlayer transport, asset fingerprinting, SQLite/FTS5 hybrid store, dual-pass transcription, small classifier primary detector, skip state machine, capability tiers, show-level learning, banner refinements, trust metrics, preview gating, consistency fixes)
3. **GPT Pro Review Round 2** — 11 revisions integrated (capability matrix, hot path + backfill split, AnalysisAsset model, AnalysisCoordinator state machine, background strategy reset, per-show trust scoring, evidence-bound banners, model lifecycle, monetization tightening, discovery simplification, replay simulator)
4. **User constraint added** — Apple Intelligence-capable devices only (iPhone 15 Pro+), iOS 26.0+ minimum

### Beads Phase
5. **bd initialized** — embedded dolt backend, prefix: playhead
6. **49 beads created** across 10 epics with full dependency graph
7. **Polish Round 1** completed:
   - Fixed 4 priority mismatches (Design Tokens P2→P1, Capabilities P1→P0, AssetProvider P1→P0, Entitlements P2→P1)
   - Fixed 4 missing dependencies (Replay Simulator→Corpus, Coordinator→Cache, Banner→Trust, classifier changed to `related` not blocking)
   - Added App Navigation Structure bead
   - Updated AdDetectionService: classifier optional for MVP hot path
8. **Polish Round 2** completed:
   - Massively expanded unit tests (100+ specific test cases)
   - Created 9 focused E2E test beads covering every success criterion
   - Expanded Replay Simulator with detailed metrics/regression mode
   - Expanded Labeled Corpus with 4 categories and annotation format
   - Created Final Integration Gate depending on all E2E suites
9. **Polish Round 3** completed — Dependency graph corrections:
   - Removed l2f→db9: corpus is pre-curated, doesn't need feed parser. Now ready for immediate work (2 ready beads).
   - Removed coi→uru: AssetProvider manages model files via JSON manifest, not SQLite.
   - Changed 8eh dependency from m9n to pgd: Foundation Models operates on detected ad windows.
   - Added 4 missing deps to pl6: 3cw, l3a, 4ae, coi (unit tests cover these services).
   - Promoted fye (Audio Asset Cache) P1→P0: critical path for AnalysisCoordinator.
   - Updated m9h with protocol-based dispatch and specific AC thresholds.
10. **Polish Round 4** completed — Self-containedness:
    - Added specific regex patterns (5 categories) to m9n (Lexical Scanner)
    - Added shard format, storage path, threading to drf (AnalysisAudioService)
    - Added full column types, indexes, PRAGMAs, FTS5 config to uru (SQLite Store)
    - Added AVAudioSession config, skip smoothing timing to 4s0 (PlaybackService)
11. **Polish Round 5** completed — Acceptance criteria gaps:
    - Added 7 ACs to kcz (Transcript Peek) including VoiceOver
    - Added ACs to 4hv (App Icon): render sizes, App Store validation
    - Added 8 ACs to j2u (Settings View): persistence, model management, accessibility
    - Added 8 ACs to 5p9 (Mini Player): behavior, transitions, VoiceOver
    - Made 8bb (Design Tokens) ACs objective: WCAG contrast, Dynamic Type scaling
12. **Polish Round 6** completed — Accessibility & thresholds:
    - Added VoiceOver ACs to b9i (Now Playing), 1hg (Banner), 8rr (Episode List)
    - Added Dynamic Type (AX3) criteria to all UI beads
    - Specified exact trust promotion/demotion thresholds in 3cw with score decay
    - Added error handling and API details to kcr (PodcastDiscoveryService)
13. **Polish Round 7** completed — Testing cross-references & final sweep:
    - Updated pl6 with 120+ tests cross-referencing all service beads including fye
    - Added pl6→fye dependency for AudioAssetCache tests
    - Added VoiceOver/accessibility to ugq, w1y, xu7
    - Added implementation detail to 8eh (Foundation Models) and 1v8 (Onboarding)
    - Added animation timing, haptics, VoiceOver to m3d (Skip Markers)
    - Added navigation flows and deep links to xu7

## Bead Summary

### Epics
- E1: Project Setup & Infrastructure (7 beads)
- E2: Podcast Feed Integration (2 beads)
- E3: Audio Playback & Analysis (3 beads)
- E4: Basic Player UI (7 beads, including navigation)
- E5: Transcription Pipeline (4 beads)
- E6: Ad Detection Engine (5 beads)
- E7: Skip Orchestrator (2 beads)
- E8: Ad Banner System (2 beads)
- E9: Design & Polish (4 beads)
- E10: Evaluation & Testing (13 beads)

### Key IDs
- Root: `playhead-16k` (Xcode Project Scaffold) — ready
- Also ready: `playhead-l2f` (Labeled Test Corpus) — no dependencies
- Final gate: `playhead-3xx` (Final Integration Gate)
- Critical path: 16k → drf/apn → x5s → 6kj → m9n → pgd → shm

### Graph Health
- 49 beads total (49 open)
- 0 cycles
- 2 ready beads (playhead-16k, playhead-l2f)
- AdDetectionService (pgd) has CoreML classifier as `related` (not blocking) — MVP ships with lexical scanner only
- All UI beads have VoiceOver and Dynamic Type acceptance criteria
- All service beads have specific numeric thresholds in ACs
- Unit test bead (pl6) cross-references every service bead

## What's Next

- Implementation via agent swarm
- Key files: PLAN.md (source of truth), BEADS.md (markdown backup), AGENTS.md (bd instructions)
