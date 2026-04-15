# Playhead Ad Detection — Wizard Ideas (Claude Code)

**Date:** 2026-04-14
**Scope:** Improvements to the on-device ad-detection pipeline and the UX surrounding it. Proposals respect the on-device mandate, the precision-over-recall posture, and the "no unilateral framework swaps" rule. Each idea is designed to be *obviously* accretive — no speculative re-architectures.

---

## The 30 Candidate Ideas (brainstorm, brief)

1. Per-show positional heatmap of ad-break locations (richer than current slot priors).
2. Host-voice negative fingerprint: identify the primary host's voice from sustained editorial; use as an anti-signal.
3. On-device prosody model for "ad-read voice" (elevated rate, higher pitch variance, emphatic sponsor stress).
4. Cross-episode sponsor carry-forward tightening (decay-weighted prior from last N episodes).
5. Near-real-time user-correction learning loop (one-tap feedback → per-show priors).
6. Dual-ended ad-break pair inference (HMM forward-backward joint decoding of content↔ad transitions).
7. **ID3 / Podcasting 2.0 chapter-marker ingestion** (keywords: "Sponsor", "Ad", "Break").
8. **RSS feed / show-notes sponsor pre-seeding** — parse sponsor names from episode description at enqueue time, extend per-episode lexicon.
9. Pre-decoded fingerprint cache warmup.
10. Progressive skip countdown UI ("skipping in 3…2…1, tap to cancel").
11. One-tap "you missed an ad" long-press gesture on waveform.
12. Ad-type taxonomy in banners (Promo / Pharma / Host-read / Network).
13. Skip-preview: tap banner to hear 3s at head and 3s at tail of the skipped region.
14. Boundary uncertainty rendered as a gradient edge in the timeline rail.
15. 300ms crossfade on skip to mask small boundary errors.
16. **Music-bed envelope bracketing as dominant boundary cue** (jingle onset/offset drives snap).
17. Pre-roll heuristic: lower confirmation threshold for anchor-rich candidates within first 30s.
18. Expanded return-marker lexicon ("welcome back", "alright so", "where were we").
19. FM backfill priority queue keyed to imminent listening (play-now / top-of-queue first).
20. Real speaker diarization via Apple voice embeddings (replaces proxy).
21. Pause-fingerprint alignment against per-show break templates.
22. Deferred-skip mode for 0.40–0.69 band: one-tap skip banner rather than auto-skip.
23. FM prompt-prefix reuse across windows within a session.
24. Language detection + per-language lexicons.
25. Counter-classifier explicitly trained on "this is editorial, not an ad".
26. Seek-time skip prompt when user scrubs into a candidate region.
27. Overnight / on-charge pre-analysis of subscribed episodes.
28. Confidence-adjusted banner dwell time.
29. Dual-prompt FM consensus (two framings on same window must agree).
30. **Episode-level ad-inventory sanity check** (outlier detection on total ad time vs per-show history).

---

## Winnowing Criteria

I evaluated each idea against five axes:

1. **Expected quality lift** (recall, precision, boundary accuracy) — how much does this actually move the numbers?
2. **On-device compatibility** — no cloud, no legal issue.
3. **Architectural risk** — does it force a framework swap or large rewrite? (The CLAUDE.md authority rule penalizes these.)
4. **Implementation cost** vs the lift.
5. **User perception / trust** — does the win show up in how the product *feels*?

Ideas that are partial duplicates of existing infrastructure (4, 5, 9, 11, 27) scored low because "more of what you already do" is rarely obviously accretive. Ideas requiring on-device model training (3, 25) scored low on pragmatism. Ideas that force framework decisions (6, 20) scored low on architectural risk. UX polish that doesn't change detection (10, 12, 13, 14, 15, 28) was interesting but off-target for a brief that is primarily about the detection pipeline.

The five that dominate the Pareto frontier are below, ranked by overall confidence that they are **obviously, unambiguously better** for the product.

---

# Top 5 — Best to Worst

## 1. RSS / Show-Notes Sponsor Pre-Seeding

**What it is.** At episode enqueue time, parse the RSS feed entry — specifically the `<description>`, `<itunes:summary>`, and `<content:encoded>` fields — for sponsor disclosures. Extract candidate sponsor brand names, promo codes, and URLs. Inject these into a **per-episode ephemeral sponsor lexicon** that augments `CompiledSponsorLexicon` for that episode only. The lexicon is built *before the audio is decoded* and is consumed by `LexicalScanner` during the hot path.

**How it works.**
- A tiny parser runs on the RSS item body. It targets three deterministic patterns that appear in ~70% of modern podcast show notes:
  - `this episode is (?:brought to you by|sponsored by|supported by) ([A-Z][\w &.-]{2,})`
  - Bare URLs pointing at known advertiser domains (`betterhelp.com/<slug>`, `squarespace.com/<slug>`, `factor75.com/<slug>`, etc.) where the path segment after the host is the show slug.
  - Promo-code patterns (`code\s+([A-Z0-9]{3,})`).
- Extracted entities flow into `EvidenceCatalogBuilder` as **pre-anchored evidence** before transcription even starts. They get `evidenceRef` IDs just like any other catalog entry.
- A `provenance: .rssShowNotes` tag distinguishes them from in-audio evidence so downstream fusion can weight them appropriately (suggested: contribute to fusion only when corroborated by at least one audio signal, to prevent show-notes-only false positives).
- The ephemeral lexicon lives for the episode's session; it does *not* promote to the global `SponsorKnowledgeStore` until the ad read is actually observed in audio. This preserves the store's integrity.

**Why it's obviously accretive.**
- Show notes are **free, deterministic ground truth** for a large fraction of ads. The sponsor says "this episode is brought to you by Squarespace" in the description, and the host says it in the audio thirty seconds later. Today the lexical scanner has to rediscover "Squarespace" from prosody and weak priors. Pre-seeding collapses that search problem.
- The lexical layer currently catches ~60–70% of ads according to the deep dive. Even a 10–15 percentage-point lift on that layer cascades: more lexical hits → stronger classifier scores → more corroborated anchors → more FM-confirmed spans. This is a recall win at the *cheapest* layer in the stack.
- It **respects the on-device mandate trivially**: RSS metadata is public, non-audio, non-transcript text. No legal exposure.
- It is **architecturally cheap**: new parser + new provenance tag + a lexicon-merge point. No existing type needs to change shape. No framework is swapped.
- It meaningfully helps **first-episode** and **new-show** cases, which today rely entirely on in-audio discovery. That's a cold-start win the per-show priors system structurally cannot deliver.

**User perception.** Invisible when it works — users just notice that "Playhead caught the sponsor read even though I'd never listened to this show before." The only visible consequence is banners showing sponsor names more often and earlier.

**Confidence this is clearly better.** Very high. The mechanism is deterministic, the data is free, the integration is contained, and the failure mode (a bogus sponsor name from show notes) is gated behind a corroboration requirement. The worst case is no-op.

**Risk and mitigation.** Show notes sometimes list *previous* episode sponsors or cross-promotional content. The corroboration gate (require at least one audio signal before pre-seeded evidence contributes to fusion) handles this cleanly. A second mitigation: cap total pre-seeded contribution to 0.15 of the fusion budget.

---

## 2. ID3 / Podcasting 2.0 Chapter-Marker Ingestion

**What it is.** Many podcasts publish chapters inside the MP3 (ID3 `CHAP` frames) or as `<podcast:chapters>` JSON via Podcasting 2.0. A meaningful fraction label ad breaks literally: "Sponsor: Squarespace", "Ad Break", "Mid-Roll", "Support the Show". Harvest these at ingest time and treat chapters whose title matches an ad lexicon as **high-confidence bounded regions**.

**How it works.**
- Extend the existing episode-ingestion step to parse ID3 `CHAP`/`CTOC` frames and the Podcasting 2.0 chapters JSON if the feed advertises one.
- A `ChapterEvidence` record contains `(startTime, endTime, title, source: .id3 | .pc20)`.
- Titles are normalized and matched against a small dedicated regex set (`sponsor|ad(s|\s+break)?|promo|support|mid-roll|pre-roll|post-roll`). Matches are registered in the evidence catalog with provenance `.chapter`.
- In the span decoder, chapter-backed evidence gets **a preferred snap target** — boundary resolver treats chapter edges as strong cues with a wide-but-capped snap radius (±12s on both ends). The chapter title that *disambiguates* the span ("Sponsor: Athletic Greens") also extends the per-episode lexicon à la idea #1.
- A chapter that labels *content* ("Main Interview", "Q&A", "Lightning Round") is equally valuable — it acts as a hard *negative* signal. No ad span should cross into a chapter explicitly labeled as a content segment unless strongly corroborated.

**Why it's obviously accretive.**
- Chapters are **precision gold**: when a publisher bothers to label a chapter "Sponsor Break", the boundaries are usually accurate to a second or two.
- Today the system rediscovers these boundaries from audio. Every hour of chapter-rich podcasts analyzed is hours of FM compute saved and boundary snaps avoided.
- It catches a category of ads that are otherwise hard: **short host promos for network shows** ("go check out our sister show X") often lack sponsor-language but are explicitly labeled "Network Promo" in chapters.
- Integration surface is small: a parser, an evidence type, a boundary-cue weight, a negative-region check in the span decoder.
- It improves the *backfill-complete* experience asymmetrically — full chapter data yields near-perfect boundaries and near-zero FM budget spend on those spans, freeing FM compute for episodes without chapters.

**User perception.** "How does Playhead know the exact ad boundaries on this show?" For chapter-labeled podcasts, skip confidence will routinely be `high` with boundary errors under 2s. Users will feel a step-function improvement on certain shows.

**Confidence this is clearly better.** Very high. Chapter data is a publisher-declared hint. We are not trusting it blindly — the corroboration and negative-chapter logic bound the downside. When chapters are wrong or absent, the system degrades gracefully to today's behavior.

**Risk and mitigation.** Some shows use chapters for editorial segments that *contain* sponsor mentions ("Cold Open" might start with "This episode is brought to you by…"). Rule: chapter evidence is a strong *cue*, not a hard boundary. The boundary resolver already has `minImprovementOverOriginal: 0.1` — chapter cues should flow through that gate like any other cue.

---

## 3. Host-Voice Editorial Counter-Classifier

**What it is.** Every podcast has one or two primary hosts. Their voices recur across episodes. Build a lightweight on-device embedding of the host's voice from the first 90–120 seconds of sustained editorial speech in each new episode (or carry it from prior episodes of the same show). During classification, regions where the *host is clearly speaking in their normal editorial register* receive a **negative signal** — a confidence penalty that actively argues against the region being an ad.

Crucially, this is a *counter-classifier*, not an ad detector. It answers one question well: "Is this the host in their normal editorial mode?" and nothing else.

**How it works.**
- Use Apple's speech framework voice-analytics features (or `SNClassifySoundRequest` with a custom embedding head if available) to extract a compact speaker embedding per 2-second window. This slots into the existing feature extraction cadence.
- `HostVoiceProfile` is built progressively: seed it from the episode's first clean editorial segment, refine across episodes of the same show (persist in `SponsorKnowledgeStore`'s per-show store — it already has the right lifecycle).
- For each candidate ad region, compute the mean cosine similarity between the region's speaker embeddings and the host profile. If similarity is high **and** the region lacks sponsor-language anchors **and** acoustic break strength is low, apply a penalty to `adProbability` (capped at 0.20 so it never alone suppresses a genuine ad).
- Host-read ads (a real ambiguity) are protected because they typically *do* have lexical anchors (URL, promo code, disclosure) — the penalty is conditioned on anchor absence.

**Why it's obviously accretive.**
- The precision-over-recall posture means the worst failure mode today is false positives that erode trust in the skip banner. A host-voice counter-signal directly attacks the most common FP pattern: extended editorial tangents that happen to score high on transition markers ("so anyway, back to what we were saying about…") or spectral changes (laughter, theme-music interludes).
- The mechanism is **architecturally symmetric** to the existing `SpanHypothesisEngine` — we already model evidence that argues *for* an ad; this is evidence that argues *against*. Adding a per-atom negative term to fusion is a natural extension.
- It leverages the existing feature-extraction cadence and SoundAnalysis framework — no new framework dependency. Speaker embeddings are already used as a "proxy"; this path upgrades the proxy into a real signal but scoped narrowly (host identification, not full diarization) so it doesn't trigger the framework-swap rule.
- Per-show profiles improve over time. Episode N+1 has a better host profile than episode N, so precision climbs with user engagement — a virtuous loop.

**User perception.** Fewer "why did it skip the host talking about the story structure?" moments. Users don't perceive the counter-classifier directly; they perceive an overall reduction in baffling skips.

**Confidence this is clearly better.** High. The signal has clear statistical footing (host voice is the most recurrent acoustic feature of any podcast). The cap on penalty magnitude bounds the downside. The conditioning on anchor absence protects the hard case (host-read ads). The per-show persistence reuses existing infrastructure.

**Risk and mitigation.** Co-hosted shows with variable lineups: profile needs to represent a *set* of host embeddings, not one. The `SponsorKnowledgeStore` already handles multi-entity per-show state; extend the same pattern. Second risk: a show's host *is* the advertiser (indie podcasts pitching their own products). The anchor-absence condition handles this: owned-product ads still have URL/promo/CTA anchors.

---

## 4. Music-Bed Envelope as Dominant Boundary Cue

**What it is.** In a very large fraction of produced podcasts, ad breaks are bracketed by a rising→plateau→falling music bed (the jingle envelope). The existing `MusicBedClassifier` already detects onset/offset scores per window. Today those scores are one cue among many in `TimeBoundaryResolver`. Promote them: when a candidate ad region is flanked by a clean music-envelope onset at the start and a symmetric offset at the end, **treat the envelope peaks as the primary boundary and snap to them preferentially** with a higher snap weight and a larger snap radius.

**How it works.**
- Add an `envelopeBracket: BracketEvidence?` field to the span hypothesis, computed by a small finite-state scanner over the music probability stream around candidate boundaries.
- `BracketEvidence` contains `(onsetTime, onsetScore, offsetTime, offsetScore, symmetryScore)`. Symmetry is a match between onset shape and offset shape — strong brackets have similar rise/fall profiles.
- If `symmetryScore ≥ 0.6` and both onset and offset scores are ≥ 0.7, the boundary resolver uses envelope peaks as the snap anchor with weight **0.45** (higher than any current single cue) and a 10s snap radius at both ends.
- If only one side of the bracket is strong (common for outro fades that blend into content), the stronger side drives its boundary; the weaker side falls back to the current cue-weighted snap.
- The envelope signal also promotes the candidate to "precise boundary" in the refinement pass, which the UI and skip gate can use to route to shorter banner dwell and higher skip confidence.

**Why it's obviously accretive.**
- Boundary accuracy is the **most visible precision lever** to users. A span that's off by 6 seconds at the start means the listener hears "…and if you go to betterhelp.com/—" before the skip fires. That's the single most trust-destroying event in an ad-skip product.
- The music bed signal already exists and is reliable. The current cue-weighted resolver dilutes a strong signal by averaging it with weaker ones. Elevating a *symmetric* envelope to primary-cue status captures a real acoustic regularity.
- The symmetry condition is the key safety: random music (theme music, interview music, mid-story stingers) won't produce a symmetric bracket around a lexically-anchored region. Only produced ad breaks do.
- Implementation is a **new scanner + a new cue class + a branch in the resolver**. No framework change, no model training, no data collection.
- It specifically improves the high-production shows most users gravitate to — NPR, Wondery, Gimlet, major independents — where jingles are ubiquitous.

**User perception.** "The skip is dead on." On jingle-heavy shows, boundary errors drop from seconds to hundreds of milliseconds. The crossfade-on-skip UX polish that would otherwise mask errors becomes a genuinely transparent transition.

**Confidence this is clearly better.** High. The underlying signal is already computed; we're using it more precisely. The symmetry gate bounds downside. Even if the bracket analysis is wrong, the resolver falls back to today's weighted-cue behavior.

**Risk and mitigation.** Some editorial segments (DJ-style music shows, some narrative podcasts) have music envelopes that aren't ad-related. The symmetry + lexical-anchor co-occurrence gate resolves this: elevate envelope to primary cue only when the region also carries at least one non-envelope anchor. Belt and suspenders.

---

## 5. Episode-Level Ad-Inventory Sanity Check

**What it is.** After backfill completes, run a lightweight reconciliation pass: compare the total detected ad time and ad-break count for the episode against the per-show historical distribution stored in `SponsorKnowledgeStore`. If the episode is a statistical outlier — zero ads detected on a show that averages 4 breaks per episode, or 18 minutes of ads detected on a 45-minute show — **flag the episode as "review recommended"** and adjust UX accordingly: temporarily downgrade auto-skip to one-tap skip, surface a gentle "help us get this one right" affordance, and deprioritize contributions from this episode to the sponsor knowledge store until user feedback arrives.

This is a meta-safeguard, not a detection improvement per se. It catches the catastrophic failures that individual layers miss.

**How it works.**
- Per show, maintain a running distribution of `(totalAdSeconds, breakCount, meanBreakDuration)` across the last N=20 analyzed episodes. Stored alongside existing per-show priors.
- After backfill, compute z-scores for the current episode on each axis. If any axis exceeds ±2σ **and** the show has ≥5 episodes in history, set `episode.detectionHealth = .suspicious`.
- The UI layer (`NowPlayingView`, `AdBannerView`) reads `detectionHealth`:
  - `.healthy` (default): current behavior, auto-skip on eligible candidates.
  - `.suspicious`: downgrade to one-tap skip banner regardless of score. Show a subtle "Does this look right?" prompt in the transcript peek view.
  - `.cold` (first 3 episodes of a show): similar to suspicious but more permissive on the skip threshold — we need data to build priors.
- User corrections on suspicious episodes carry extra weight in the trust-scoring service (they're diagnosing known-uncertain territory).
- After 5 suspicious episodes in a row on the same show, `AnalysisCoordinator` can trigger a **full FM re-analysis** of the most recent episode rather than silently accumulating bad priors.

**Why it's obviously accretive.**
- The system today has **no self-awareness**. If the FM coarse classifier silently starts refusing more often on a particular show's content (a real risk given Apple's FM safety classifier is opaque and changes across iOS updates), detection quality degrades silently. This idea makes silent degradation visible.
- It converts the **per-show learning asymmetry** (which currently only flows one way — we learn sponsors) into a two-way loop: we also learn when we're failing on a show.
- It's a **layered-system safeguard** in the spirit of the existing architecture. Each layer (lexical, acoustic, FM, fingerprint) can fail independently; this is the meta-layer that notices when the stack as a whole produces an anomalous result.
- Implementation surface: a distribution store, a z-score pass in the backfill completion hook, a `detectionHealth` field propagated to UI, a gentle UI affordance. No ML. No framework change.
- It gives the user a productive outlet for the inevitable failures: a targeted "help us get this one right" prompt feels respectful in a way that a silent bad skip does not.

**User perception.** Rare but powerful. On the ~3% of episodes that would otherwise produce catastrophic detection failures, the user gets a visible, honest "we're uncertain about this one — tap to help" prompt instead of a silently-broken experience. That single behavior does enormous work for trust.

**Confidence this is clearly better.** High. The mechanism is purely additive — it doesn't change any detection output, it only modulates UX and feedback weighting when the system would otherwise produce anomalous results. The downside is essentially zero.

**Risk and mitigation.** Legitimately ad-free episodes (Q&A specials, live shows, patron-exclusive cuts) would trigger the low-outlier branch. Solution: allow the per-show prior to include a bimodal distribution (regular episodes vs. specials) or mark episodes tagged `bonus`/`special`/`live` in the RSS feed as exempt from sanity-check. This reuses idea #1's RSS parsing.

---

## Why These Five Together

These five ideas are not independent — they reinforce each other:

- Idea #1 (RSS pre-seeding) and Idea #2 (chapter markers) are both **pre-audio metadata harvests**. They share a parsing pass at enqueue time and feed the same `EvidenceCatalogBuilder`. Build one, the other is trivial.
- Idea #3 (host-voice counter) and Idea #4 (music envelope boundary) both operate on feature windows and use existing SoundAnalysis infrastructure. They share the feature-extraction cadence and validate each other: music-bracketed regions where the host voice *does* appear are a strong "host-read sponsor" signal (a known ambiguity) and can be routed to a different banner style.
- Idea #5 (inventory sanity) is the **meta-loop** that makes the other four provably good — it surfaces the episodes where the improvements didn't improve things, funneling those into the correction pipeline.

All five are:
- Compatible with the on-device mandate.
- Free of framework swaps (CLAUDE.md rule).
- Incremental on top of existing services — no new pipeline stage or module is required.
- Testable with existing test plans (`PlayheadFastTests` for unit, `PlayheadIntegrationTests` for corpus validation).

Implementation-order recommendation: **#1 first** (largest lift, smallest surface), then **#2** (same parser pass), then **#5** (provides telemetry to validate #3 and #4 rollouts), then **#4**, then **#3**.
