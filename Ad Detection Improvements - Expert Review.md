# Playhead Ad Detection — Proposed Improvements for Expert Review

**Date:** 2026-04-14
**Context:** Playhead is an iOS podcast player whose core feature is on-device ad detection and skip. All processing runs entirely on-device (legal requirement). The detection pipeline uses layered signals: lexical scanning, acoustic break detection, music-bed classification, a rule-based classifier, Apple's on-device Foundation Model (FM), and cross-episode ad-copy fingerprinting. Evidence from all layers fuses into a skip confidence score per detected ad region.

These three proposals were identified through independent analysis by two different AI models, then adversarially cross-scored. All three survived cross-model scrutiny with scores above 780/1000 from both evaluators. They are presented in recommended implementation order.

---

## 1. RSS / Show-Notes Sponsor Pre-Seeding

### Problem

When a user plays a podcast they've never listened to before, the detection pipeline has no per-show priors — no known sponsors, no ad-slot position history, no jingle fingerprints. The lexical scanner must discover sponsor names purely from in-audio signals, which means the first ad break on a new show is the hardest to catch. This cold-start gap is structural: no amount of tuning the existing pipeline helps until the system has observed at least one episode.

### Proposal

At episode enqueue time, parse the RSS feed entry — specifically the `<description>`, `<itunes:summary>`, and `<content:encoded>` fields — for sponsor disclosures. A large fraction of modern podcast show notes contain lines like "This episode is brought to you by Squarespace" or bare URLs pointing at known advertiser domains. Extract candidate sponsor brand names, promo codes, and URLs and inject them into a per-episode ephemeral sponsor lexicon that augments the lexical scanner for that episode only.

### Key design constraints

- **Weak prior only.** Pre-seeded metadata contributes to evidence fusion but cannot trigger a skip decision on its own. At least one corroborating in-audio signal (lexical hit, acoustic break, FM classification) is required before pre-seeded evidence influences the skip confidence score. Suggested fusion weight cap: 0.15.
- **Ephemeral scope.** The per-episode lexicon lives for the analysis session. Pre-seeded sponsor names do not promote to the persistent sponsor knowledge store until the ad read is actually observed in audio.
- **Deterministic extraction.** Three regex-class patterns cover the majority of show-notes sponsor disclosures: explicit "sponsored by / brought to you by" phrases, bare advertiser URLs with show-slug paths, and promo-code patterns. No ML required.

### Expected impact

- Recall lift on cold-start episodes (first listen to a new show), where the lexical scanner currently has no show-specific vocabulary.
- Cascade effect: more lexical hits at the cheapest pipeline layer means stronger classifier scores, more corroborated anchors, and more FM-confirmed spans downstream.
- No impact on episodes where show notes lack sponsor disclosures — the system degrades gracefully to current behavior.

### Known risks

- Show notes sometimes list previous episode sponsors or cross-promotional content not actually read in the current episode's audio. The corroboration requirement handles this: if the sponsor name never appears in the audio, the pre-seeded evidence contributes nothing.
- Dynamic ad insertion can cause the audio to diverge from the feed text, especially for geographically targeted ads. Same mitigation applies — corroboration gates the signal.

### Implementation surface

- New: RSS field parser, `provenance: .rssShowNotes` evidence tag, lexicon-merge point in `LexicalScanner` setup.
- Existing change: `Episode` persistence currently drops description metadata and would need to retain it through to the analysis input path.

---

## 2. Music-Bed Envelope as Dominant Boundary Cue

### Problem

Boundary accuracy is the most user-visible quality dimension of an ad-skip product. A skip that fires 4 seconds late means the listener hears the beginning of an ad read before the transition. A skip that ends 3 seconds early clips the host's return to editorial content. Both feel broken, even when the ad was correctly detected. The current boundary resolver uses a weighted average of five cue types (pause/VAD, speaker change, music bed change, spectral change, lexical density). This dilutes strong signals by averaging them with weaker ones.

### Proposal

In a large fraction of produced podcasts, ad breaks are bracketed by a rising-plateau-falling music bed (the jingle envelope). The existing music-bed classifier already computes onset and offset scores per 2-second window. When a candidate ad region is flanked by a clean music-envelope onset at the start and a symmetric offset at the end, promote the envelope peaks to the primary boundary snap target.

Specifically:
- Compute a `BracketEvidence` record containing onset time/score, offset time/score, and a symmetry score (how closely the onset shape matches the offset shape).
- If symmetry score >= 0.6 and both onset and offset scores >= 0.7, the boundary resolver uses envelope peaks as the snap anchor with elevated weight (0.45, higher than any current single cue) and a 10-second snap radius at both ends.
- If only one side of the bracket is strong (common for outro fades that blend into content), the stronger side drives its boundary; the weaker side falls back to the current cue-weighted snap.

### Key design constraints

- **Symmetry gate.** Random music (theme music, interview music, mid-story stingers) won't produce a symmetric bracket around a lexically-anchored region. Only produced ad breaks do. The symmetry requirement is the primary false-positive guard.
- **Anchor co-occurrence.** Envelope elevation only applies when the region also carries at least one non-envelope anchor (lexical hit, FM classification, fingerprint match). Music brackets alone don't promote a region.
- **Graceful fallback.** When the bracket analysis doesn't meet thresholds, the resolver falls back to today's weighted-cue behavior. No existing boundary quality is degraded.

### Expected impact

- Boundary errors drop from seconds to sub-second on jingle-heavy shows (NPR, Wondery, Gimlet, major independents). These are among the most-listened podcast networks.
- The boundary resolver already computes all necessary features. This is a scoring policy change, not a new data source.
- Measurable via boundary MAE (mean absolute error) in the existing replay harness.

### Known risks

- Music-heavy editorial shows (music criticism, DJ-format, heavily scored narrative) can create envelope patterns unrelated to ads. The symmetry + anchor co-occurrence gate addresses this.
- Podcasts using dry ad insertion (no jingle bed) get zero lift. This is not a universal improvement — it's a high-impact improvement on a large subset of shows.

### Implementation surface

- New: `BracketEvidence` struct, finite-state envelope scanner over music probability stream, new cue class in `TimeBoundaryResolver`.
- No new framework dependencies. No model training. Testable with existing replay infrastructure.

---

## 3. Granular User Correction Learning

### Problem

When a user taps "not an ad" or "you missed one," the correction is stored but applied broadly. The current correction system doesn't distinguish between "this specific 30-second span on this episode was wrong" and "this show's host often talks about their own products in a way that sounds like an ad but isn't." Broad corrections risk overgeneralization (suppressing future true positives on the same show), while narrow corrections risk not persisting (the same false positive recurs next episode).

### Proposal

Upgrade the user correction store with typed scopes and decay semantics:

| Scope | Example | Persistence |
|-------|---------|-------------|
| `episode-span` | "This specific 30s region on this episode is not an ad" | Permanent for this episode |
| `phrase-on-show` | "When this host says 'check out my course,' it's not an ad" | Decays after N episodes without reinforcement |
| `sponsor-on-show` | "Athletic Greens is always an ad on this show" | Promoted after 2+ confirmations, decays on rollback spike |
| `domain-ownership-on-show` | "Links to the host's own website are not third-party ads" | Decays slowly |

Corrections apply at three points in the pipeline:
1. **Skip decisions** — suppression or boost in the skip eligibility gate.
2. **Boundary adjustments** — "start was too early" / "end was too late" feedback shifts per-show boundary priors.
3. **Sponsor memory** — corrections promote or demote entries in the sponsor knowledge store.

### Key design constraints

- **User-facing simplicity.** The user taps one button ("not an ad" / "missed ad" / "boundary wrong"). The system infers the appropriate scope from the evidence context of the corrected region — the user never sees the scope taxonomy.
- **Poisoning protection.** Global-ish rules (phrase-on-show, domain-ownership) require repeated confirmations before promotion and have TTL/decay to prevent stale corrections from accumulating.
- **Veto permanence.** Explicit user vetoes on specific spans remain permanent and override all automated signals, consistent with the existing precision-over-recall design posture.

### Expected impact

- Repeat false positives on the same show are eliminated after one correction, without suppressing true positives on other shows or other ad types on the same show.
- User trust compounds: "when I correct it, it stays corrected — specifically" is the behavior that produces product loyalty in an ad-skip tool.
- The correction store already exists in the codebase. This is a schema and consumption upgrade, not a new subsystem.

### Known risks

- Scope inference from evidence context is non-trivial. A correction on a region with a URL anchor means something different from a correction on a region with only acoustic evidence. The routing logic needs careful design and testing.
- Low-volume shows may not generate enough corrections to promote phrase-level rules. A minimum-support gate prevents overfitting.
- Decay semantics must be explicitly defined: user-confirmed vetoes never decay; system-inferred broad rules do.

### Implementation surface

- Existing change: Schema extension to `UserCorrectionStore` (add scope type, TTL, confirmation count).
- Existing change: Consume scoped corrections in `SkipOrchestrator`, `DecisionMapper`, and backfill fusion path.
- New: One-tap correction affordances in now-playing and ad-marker UI (if not already present).
- Testable with replay fixtures that simulate correction sequences.

---

## Cross-Cutting Notes

- All three proposals respect the on-device mandate. No audio, transcript, or classification data leaves the device.
- All three are incremental additions to existing pipeline components — no new pipeline stages, no framework swaps, no external dependencies.
- Implementation order matters: #1 and #2 improve detection quality; #3 improves the feedback loop that makes quality gains durable. Shipping #1 and #2 first gives users better results to correct against, making #3's learning signal higher quality.
