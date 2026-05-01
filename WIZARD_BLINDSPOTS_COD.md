# Playhead Blind-Spot Ideas (Post-Adversarial Round)

These are five high-value ideas that appear to be missing from both prior 30-idea sets and current open beads. They are designed to be accretive, on-device, and compatible with Playhead’s existing actor architecture and Quiet Instrument identity.

## 1) Trip Pack Mode (Offline-Ready Queue)

**What it does**
A one-tap mode that prepares a selected set of episodes for travel or low-connectivity windows: download, integrity-check, and pre-analyze enough of each episode to make first skips available quickly.

**Why it’s good**
It turns a stressful user moment (“I’m boarding / losing signal soon”) into confidence. This is peace-of-mind, not a dashboard: the user gets a simple “Ready for offline listening” state.

**Architecture fit**
Use existing pieces only:
- `DownloadManager` for durable background downloads
- `AnalysisWorkScheduler` + `AnalysisJobRunner` for pre-analysis coverage
- `BackgroundProcessingService` for opportunistic completion
- `SurfaceStatus` for a calm ready/not-ready projection

**Risks / objections**
- Battery/storage impact if scoped too aggressively
- Episode selection heuristics could feel wrong
- Needs strict caps (count, storage, time) and graceful fallback when not fully complete

---

## 2) Episode Trust Brake (Immediate In-Episode Fallback)

**What it does**
If the user indicates a bad skip in the current episode (for example via “Listen” or veto), auto-skip immediately softens for the rest of that episode (or a bounded window) without waiting for show-level trust to adapt across episodes.

**Why it’s good**
It prevents repeated frustration in the same listening session. Current per-show trust adaptation is strong, but this adds immediate local damage control.

**Architecture fit**
- Add ephemeral per-episode brake state in `SkipOrchestrator`
- Feed triggers from existing correction paths (`UserCorrectionStore` / revert actions)
- Keep durable trust policy in `TrustScoringService` unchanged; this is a short-lived overlay

**Risks / objections**
- Could under-skip good ad segments after one false positive
- Must be carefully bounded in scope/duration
- Needs transparent but quiet behavior (no noisy warnings)

---

## 3) Soft Landing Stitch (Post-Skip Re-Entry Smoothing)

**What it does**
After a skip, instead of hard re-entry exactly at the detected boundary, apply a tiny re-entry stitch: fade + boundary-informed micro-offset to avoid clipped first words and perceptual discontinuity.

**Why it’s good**
This improves the felt quality of playback transitions, which is central to Quiet Instrument craftsmanship. It’s subtle and daily-use meaningful.

**Architecture fit**
- `SkipOrchestrator` provides the selected landing range
- `PlaybackService` applies the envelope/offset
- Boundary hints from existing acoustic + transcript boundary cues (`FeatureWindow`, `TranscriptBoundaryCue` paths)

**Risks / objections**
- Too much offset can sound repetitive
- Too little offset gives no benefit
- Needs careful A/B against real speech-cut artifacts and high-speed playback

---

## 4) Hands-Free Control Intents (App Intents + Siri + Action Button)

**What it does**
Expose a minimal, high-signal set of local intents for frequent actions: “Play my next ready episode,” “Skip this ad once,” “That wasn’t an ad,” “Pause at next natural break.”

**Why it’s good**
It adds ergonomic utility in contexts where touch is poor (walking, cooking, driving) without adding UI clutter. This is practical accessibility and convenience, not novelty.

**Architecture fit**
- App Intents layer maps to existing runtime actions in `PlayheadRuntime`
- Correction intents route to current correction and skip services (`UserCorrectionStore`, `SkipOrchestrator`)
- Natural-break pause can reuse pause/boundary data paths already present in analysis outputs

**Risks / objections**
- Voice intent ambiguity can create wrong actions
- Requires strict confirmation behavior for destructive/revert actions
- Scope must stay tight to avoid maintenance sprawl

---

## 5) CarPlay Quiet Controls for Trust-Preserving Playback

**What it does**
A CarPlay-native control surface focused on safe essentials: current playback, skip/revert last skipped segment, and manual “play through this segment” without requiring on-phone interaction.

**Why it’s good**
Driving is a high-value listening context. Bringing Playhead’s trust model to CarPlay makes the product more complete and materially more usable.

**Architecture fit**
- Build on existing playback and now-playing integration
- Use `SkipOrchestrator` state and recent decisions for reversible actions
- Keep user-facing copy sparse via `SurfaceStatus` summaries

**Risks / objections**
- CarPlay UX/HIG constraints limit interaction complexity
- Must avoid text-heavy diagnostic UI
- Additional test matrix across route changes/interruption states

---

## Why these are genuine blind spots

- They are not the same as already-open feature beads like transcript search (`playhead-90i`) or sleep timer (`playhead-g21`).
- They avoid rebuilding existing consolidation layers (`QualityProfile`, `AdmissionGate`) and avoid duplicating current explainability UI (`AdRegionPopover`).
- They emphasize user experience in real contexts (travel, driving, hands-free use, transition quality) while reusing existing architecture rather than adding major new subsystems.
