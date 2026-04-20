# Podcast Background Processing — UI Design

*Companion to `podcast_background_processing_plan.md` (2026-04-16). User-facing surface only; backend, scheduler, and persistence are specified in the parent plan.*

## Frame

**Mental model: progressive disclosure.** Casual users see a calm, ambient surface that feels like the app "just works." Power users (and engineering / support) get a dedicated Activity screen and a Settings → Diagnostics drawer. No dual-mode toggle; the same UI scales by depth-of-engagement.

**Intent vocabulary.** Three user verbs only. Analysis is *implicit on download* — never an explicit user action.

| Verb | User says | System does |
|---|---|---|
| Subscribe | "I follow this show" | Per-subscription policy decides if new episodes auto-download |
| Download | "I want this offline" | Fetch media. Analysis runs in background as capacity allows. |
| Add to Up Next | "Play this after current" | Pure ordering; does not imply download |
| Play | "Now" | If not downloaded, stream + opportunistic analysis |

**Key consequence:** "Prepare," "Analyze," "Process," "Queue for analysis" — none of these words appear in the user-facing UI. The only readiness language is the visual badge defined in §A.

---

## Section A — States & vocabulary (the contract)

The single source of truth for every readiness surface.

| State | Cell badge | Where labeled | Meaning |
|---|---|---|---|
| Not downloaded | (none) | — | Default. No surface treatment. |
| Downloaded | ⤓ | Cell badge only | Audio is offline. No claim about skips. |
| Skip-ready | ✓ | Cell badge; "Skip-ready: first 18 min" line in detail | The next listening window is covered well enough that playback can make the skip promise now. |
| Deferred-only | (no badge — stays as ⤓) | Episode detail status line only | Ads found, but not within the next listening window. Internal `playbackReadiness == deferredOnly`. Library cell does **not** show ✓. |
| Paused | ⏸ | Detail + Activity (with reason) | Analysis halted (thermal, low power, storage cap). |
| Analysis unavailable | (no badge — stays as ⤓) | Episode detail status line + Activity (Recently Finished) + Diagnostics | Foundation Models / Apple Intelligence not available on this device, region, or language. **Not** rendered as Paused. |
| Listened | (faded cell) | Cell styling | Already played. |

### Vocabulary rules (load-bearing — keep these honest)

- **"Skip-ready" is a playback promise for the next listening window — not a report that some later ad exists.** The badge appears only when `playbackReadiness ∈ {proximal, complete}`, never on `deferredOnly`. Coarse on purpose: any specific count ("3 ads detected") risks underclaiming or overclaiming and breaks trust.
- **"Downloaded" never implies analysis state.** It is a storage fact.
- **"Analyzing" is never a library-cell word.** It only appears in the Activity screen and the episode detail view's status line.
- **Scheduler lane names (Now / Soon / Background) are internal — never surfaced.**
- **"Paused" always includes a reason** wherever it appears ("Paused — phone is hot," "Paused — needs charger," "Paused — storage full"). Never "Paused" alone.

---

## Section B — Library cell + Episode detail view

### Library cell

Layout unchanged from today; one new visual element: a status glyph in a fixed slot (trailing edge, vertically centered, 14pt SF Symbol regular weight).

```
┌─────────────────────────────────────────────────┐
│  [Artwork]  Show name                           │
│             Episode title                       │
│             47 min · 2d ago             ⤓ / ✓   │
└─────────────────────────────────────────────────┘
```

- Glyph reflects the §A enum. **No text label, ever.**
- Color: ✓ uses app tint; ⤓ neutral fill; ⏸ system orange.
- **Long-press contextual menu:** Play / Add to Up Next / Download (or Remove Download) / Mark Played / Episode Info.
- Bulk actions live on the show page (see §D), never on individual cells.

### Episode detail view

Three-zone layout, top-to-bottom:

1. **Header (existing).** Artwork, show, title, duration, primary Play/Pause/Resume.
2. **Status line (new).** A single horizontal row that mirrors §A:
   - "Skip-ready · first 18 min" (with secondary text "analyzing remainder" if backfill active)
   - "Downloaded · queued for analysis"
   - "Paused — phone is hot · will resume automatically"
   - (no row at all if not downloaded)
   Tap the line → Activity screen, scoped to this episode.
3. **Body (existing).** Show notes, chapters, transcript scrubber. Sponsor markings come from existing `ChapterEvidenceParser`.

### Edge cases

- **Streaming-only play (not downloaded):** Zone 2 absent. Skip cues still apply if `SkipOrchestrator` produces them live, but no readiness *claim* is made.
- **Downloaded, analyzed, no ads found:** "Downloaded · no ads detected" with a small ⓘ tooltip explaining "we listened and didn't find ad markers."

---

## Section C — Player surface

Mostly already shipped (ef2.6.3 gray-band markOnly UX). This section is recap + small additions implied by the new state contract.

### Already in place (do not redesign)

- High-confidence ads → auto-skip with "Skipped 47s ad · Undo" banner.
- Gray-band (low-confidence) ads → marker-only treatment in the scrubber, no auto-skip; user can tap to skip manually.
- Chapter strip honors sponsor/content chapter markings from `ChapterEvidenceParser`.

### New / refined

1. **Scrubber colorization stays minimal — three treatments only:**
   - Solid bar in app tint = confirmed ad span (auto-skipped)
   - Diagonal-hatch overlay = gray-band span (markOnly)
   - No treatment = either content or unanalyzed (deliberately not distinguished)
2. **One-line player status** below the scrubber, mirroring the episode-detail status line. Hidden when fully analyzed. Tap → Activity scoped to this episode.
3. **"Skipped" banner gains an "Always skip this sponsor" affordance** alongside Undo. Routes through existing `UserCorrectionStore` / `FoundationModelsFeedbackStore` (Phase 7 infra).
4. **No spinner during background analysis on the currently-playing episode.** A spinner reads as "the app is struggling," opposite of the calm experience.

### Honesty constraints (do not add)

- No "ad approaching in 30s" preview marker. Boundaries refine mid-playback; preview triggers "wait, why didn't it skip?" failure modes.
- No per-segment confidence percentage. Confidence is rendered as a 3-state visual (solid / hatched / none); numbers re-open every conversation §A closed.

---

## Section D — Download intent (per-episode + bulk)

The single explicit user verb in the new vocabulary. Two surfaces.

### Per-episode: the "Download" verb

- **Where:** Long-press contextual menu on cell (per §B), and a primary button in episode detail. **No swipe-from-edge gesture in v1** (revisit if telemetry shows pull-to-download demand).
- **State transitions visible in the cell glyph:**
  - tap "Download" → glyph fades in as ⤓ with a quiet 1-frame pulse
  - media completes → glyph stays ⤓ (analysis is implicit; no second confirmation)
  - any analyzed ad crosses gray-band threshold → glyph swaps to ✓
- **Cancellation:** Long-press → "Remove Download" appears in same slot as "Download." One verb, two directions, never both visible.
- **No "Download All" anywhere.** Forces user to express finite scope (next 3, next 10).

### Bulk: "Download Next N" — the delight feature

- **Where:** Show detail page header.
- **Surface treatment — a single sentence the user fills in:**
  > **"Download next [3 ▾] episodes"**
  Stepper: 1 / 3 / 5 / 10 / 25. Default 3.
- **No "All unplayed" option in v1.** The finite-scope promise is what makes the bulk action predictable for both the user and the scheduler. Resist the temptation; revisit only if telemetry shows demand.
- **Reason context (optional in v1):** A small "for…" picker — Flight / Commute / Workout / Generic. *In v1 this only changes the post-download notification copy* ("Ready for your flight"). Does **not** change scheduler behavior. Resist making this smart in v1; preserve the signal for future B1 slice-sizing and Q3-(d) smart surfacing.
- **Confirmation:** Inline summary, not a modal — "Downloading 3 episodes (~640 MB). Will fit in your 10 GB cap." If it won't fit, summary turns amber and offers "Free up space" → Activity → Storage.
- **System promise:** Media will be downloaded as fast as transport allows; analysis will run on each as capacity allows. **No promise about full skip-readiness by any time.** Notification (if opted in — see §G) fires when *all N* episodes hit at least Downloaded, with a single line summarizing how many also hit Skip-ready.

### Subscriptions (the implicit, not-quite-bulk source)

- **Per-show toggle (show settings popover):** "Auto-download new episodes: Off / Last 1 / Last 3 / All." Default Off.
- Auto-downloaded episodes carry the same ⤓ glyph as user-initiated downloads. The system does **not** visually distinguish *why* an episode was downloaded.

---

## Section E — Activity screen

Reached via a small nav-bar button on Library (suggest: SF Symbol `chart.bar.doc.horizontal` or `tray.full`). Title: **"Activity."**

### Header

No ambient gauge in v1. Start with the four sections only; add a summary strip only after the SLI semantics are stable enough that the numbers will not churn under users.

### Four sections, top to bottom

**1. Now (live work) — always present, even if empty.**
- One row per active piece of work:
  - "↓ *Hard Fork* — The OpenAI Memo · 12 MB / 47 MB · 2 min remaining"
  - "⟳ *Stratechery* — Rivian Earnings · analyzing · ~4 min remaining"
- Empty state: "Nothing running. Tap any episode to download." With a "Why?" link if any work is paused (scrolls to §3).
- Per-row swipe action: Cancel.

**2. Up next (queued, not yet running) — collapsible, shows count when collapsed.**
- Drag-to-reorder.
- Per-row swipe: Remove.
- Headers separate user-initiated downloads from auto-downloaded subscription episodes ("Subscriptions"). Subscriptions section can be collapsed independently.

**3. Paused / blocked — only present when non-empty.**
- One row per paused piece, **with reason and what unblocks it:**
  - "⏸ Phone is hot — resumes when device cools"
  - "⏸ Storage cap reached — Free up space →" (deep link to §F Storage)
  - "⏸ Needs charger — resumes on power"
- This is the home of the **B5 miss-reason taxonomy.** Each reason is a known enum from the scheduler; the screen renders a fixed copy table.

**4. Recently finished (last 24h, capped at ~20).**
- One row per completed episode with the result:
  - "✓ Skip-ready · 3 ads · 4m skipped"
  - "✓ Downloaded · no ads detected"
  - "✕ Couldn't analyze — ASR rejected the audio. Tap to retry."
- Retry routes through existing Phase 4 shadow-retry mechanism (`needsShadowRetry` flag).
- Tap any row → episode detail.

### What is deliberately not here in v1

- **No model-version / slice-completion stats.** Settings → Diagnostics, behind "Show advanced."
- **No per-show capability profiles surface.** Same place — eventually a per-show "this show isn't worth analyzing" indicator.
- **No 7-day rollup / sparkline in v1.**

---

## Section F — Settings

Three new groups (suggest order after existing Playback group): **Downloads → Storage → Diagnostics.**

### Downloads

- **Auto-download on subscribe (global default):** Off / Last 1 / Last 3 / All. Default Off.
- **Use cellular data:** Off / Wi-Fi only / Allow. Default Wi-Fi only.
- **"Download Next N" default count:** 1 / 3 / 5 / 10. Default 3.

### Storage (Q5 (a)+(d) policy surface)

- **Episode storage cap:** 1 GB / 5 GB / 10 GB / 25 GB / 50 GB / Unlimited. Default 10 GB.
- **Current usage bar:** "6.2 GB of 10 GB used · 47 episodes." Tap → breakdown by show, with per-show "Free" affordance.
- **Keep analysis after deletion:** toggle, default **On.** Copy: "When an episode is deleted to save space, keep the small analysis file (transcript + skip data) so re-downloading is instant. Uses about 1% of episode size."
- **Analysis-data cap:** 50 MB / 200 MB / 1 GB / Unlimited. Default 200 MB. Sub-line: "Keeps analysis for many episodes; exact count depends on episode size and retained bundle version."
- **Auto-evict policy (read-only):** "Oldest played episodes are removed first."

### Diagnostics (collapsed by default)

The real power-user / dev surface. Textual; not a designed UI.

- Pipeline version, model versions (FM, transcript, classifier).
- Last 50 scheduler events (BG grant times, slice completions, evictions) — copy-to-clipboard.
- Per-show capability profile when present.
- "Send diagnostics" → composes mail to a configured support address with one attached log bundle. **Never** auto-uploads.

### What is deliberately not in Settings

- **No "analysis quality" toggle.** Thermal/energy governor handles this; exposing it gives users a knob to break the experience.
- **No "skip aggressiveness" slider.** Gray-band threshold is a single fixed contract.
- **No per-show "skip aggressiveness" override.** Per-show capability profiles are *observed*, not user-set.

---

## Section G — Onboarding & notifications

### Onboarding — one screen, one moment

Shown the *first* time a user adds a podcast subscription (not at app launch — at the moment it becomes relevant):

> **Playhead skips ads for you.**
> Tap **Download** on any episode. We'll fetch it and find the ads in the background — when it's ready, you'll see a ✓ and we'll skip them automatically while you listen.
> *All processing stays on your device.*
>
> [ Got it ]

Plus one tooltip the *first* time a ✓ badge appears on an episode the user opens: "✓ means we've found ads to skip. Tap play and we'll handle the rest." Dismisses on tap. Never reappears.

### Notifications — default Off, two narrow opt-in classes

Push permission requested **only** when the user taps "Download Next N" with the optional "for…" picker set to non-Generic (Flight / Commute / Workout). Copy:

> **Want a heads-up when these are ready?**
> We'll send one notification when all 3 episodes are downloaded — and one if something blocks them.
> [ Not now ] [ Notify me ]

If granted, the only two notification classes ever sent are:

**1. Trip-readiness ping (one per batch, on success):**
> **Ready for your flight.** 3 episodes downloaded · 2 fully skip-ready.

**2. Action-required ping (one per batch, only if blocked by a user-fixable condition):**
> **Your flight episodes are stuck.** Storage is full — free space to continue.
> *(or: Wi-Fi required · Analysis unavailable on this device)*

Strict contract: **at most one trip-readiness ping AND at most one action-required ping per batch.** The action-required ping fires only when the entire batch is blocked by a *user-fixable* condition (storage cap, Wi-Fi-only setting, analysis-unavailable). It never fires for transient causes (thermal, low power, no-grant) — the system will resolve those itself.

That is the entire notification surface. **No** new-episode notifications, **no** analysis-complete pings, **no** app icon badges.

### Live Activity carve-out (BGContinuedProcessingTask)

Honesty addendum to the "scheduler is invisible" principle: the system itself surfaces a Live Activity progress UI for `BGContinuedProcessingTask` work, with a user-cancel control. We don't suppress this — instead, design the displayed title and progress copy to match our state vocabulary ("Downloading 3 episodes" rather than scheduler internals).

### What is deliberately not here

- No "analysis complete" notification (would fire too often; trains users to ignore).
- No app icon badge for unplayed episodes (covered by in-app glyph; double-surfacing would be noisy).
- No background notification permission ask at app launch.
- No "did you know you can…" coach marks.

---

## Honesty principles (cross-cutting)

These are the design constraints that produced the choices above. List them here so future contributors can extend the design without breaking trust.

1. **Don't promise what you can't always deliver.** "Downloaded" is a guarantee; "Skip-ready" is a guarantee at a specific threshold; everything else is a description of work in progress.
2. **One verb, one direction.** "Download" and "Remove Download" never coexist. The user always knows what tapping the button will do.
3. **Coarse public, granular private.** The library cell is binary-coarse; the Activity screen is operational; Diagnostics is exhaustive. Each surface scales depth to the user's commitment level.
4. **Pause is never a bare word.** Every pause carries its reason, every reason carries what unblocks it.
5. **The scheduler is invisible.** Lane names, slice sizes, model versions, EVI scores — none appear in user-facing copy.
6. **Notifications earn their channel.** One trip-readiness ping for an explicit user-stated deadline. Nothing else.

---

## Open questions for the parent plan

The UI design assumes these are resolved in `podcast_background_processing_plan.md`:

1. **Skip-ready threshold definition.** What confidence level (single number or multi-signal gate) flips the badge? Today this is implicit in `SkipOrchestrator` + ef2.6.3; the plan should pin it as a versioned policy parameter so the badge promise is stable across model updates.
2. **Pause reason enum.** The Activity screen renders a fixed copy table per reason — the scheduler must expose a stable enum (thermal / low-power / storage / no-grant / network / pipeline-error / user-cancelled).
3. **Per-show capability profile shape.** What fields, when persisted, when surfaced. Affects the Diagnostics surface and the long-tail "this show isn't worth analyzing" decision.
4. **Default "Download Next N" count.** Currently proposed 3; validate against telemetry once usage exists.
5. **Trip-readiness notification timing.** Fire when all N hit Downloaded, or when the user-supplied deadline approaches and best-effort completes? Affects scheduler signaling.
