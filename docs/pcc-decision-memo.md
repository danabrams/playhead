# Decision memo — Private Cloud Compute for Playhead

**Date:** 2026-07-06. **Author:** engineering (Claude), for Dan.
**Question:** should Playhead use Apple's `PrivateCloudComputeLanguageModel` for ad
classification / chapter labeling, given the on-device model stays at 4096 context?
**Recommendation up front: no — do not adopt now.** Details, facts, and the
revisit-triggers below. Final call is Dan's; the legal questions belong to counsel.

---

## 1. What it is (verified facts)

- New in iOS 27 (`FoundationModels`): a cloud-hosted model behind the same session
  API as the on-device model. **~32k context, materially bigger model**, adjustable
  `reasoningLevel`. (SDK-verified: own `contextSize`, `Executor`, `capabilities`.)
- **Free to the developer** — no API keys or server bills. Metered **per-user per-day
  against the user's iCloud account** (`quotaUsage`: `isApproachingLimit` /
  `isLimitReached` / `limitIncreaseSuggestion`; iCloud+ subscribers get higher caps).
- **Privacy architecture:** stateless computation, cryptographic attestation,
  "no prompts are ever stored," independently inspectable PCC stack (WWDC26 s.319).
  Not used for training. Apple claims even Apple cannot access request content.
- **Eligibility gate (the "poison pill"):** the free tier is limited to apps under
  **2M lifetime first-time downloads**, by application. Widely criticized
  (Gruber/Støvring/Troughton-Smith): success permanently disqualifies you, and
  terms are Apple's to change.
- **No published latency figures.** Requires network; offline = feature dark.

## 2. What it would buy Playhead

- The entire iOS 27 model upgrade we don't otherwise get: whole-episode-scale
  windows (the 13-tiny-windows coarse pass collapses), likely large quality gains
  on classification and the 79%-misread chapter task, fewer budget contortions.
- Engineering simplification: much of the bd-34e budget machinery becomes moot
  for PCC-routed calls.

## 3. What it costs

**Legal (the mandate's original driver).** The on-device rule exists because of
liability around processing **copyrighted podcast audio**. PCC's guarantees address
*user privacy*; they do not change that audio-derived content (transcripts,
excerpts) would be **transmitted to and processed on third-party servers**. A
rights-holder's transmission/reproduction argument is indifferent to attestation.
Until counsel says otherwise, "private cloud" is still cloud.

**Product.**
- Offline listening loses ad-skip (or forks into two quality tiers).
- Unknown per-request latency in a near-real-time pipeline.
- Per-user daily quotas: heavy listeners hit caps; degraded-mode UX needed anyway.
- Dependency risk: quota terms, eligibility, and model behavior are Apple's to
  change; the <2M-download gate makes success itself a risk.

**Strategic.**
- On-device is Playhead's differentiation vs cloud-based competitors (Superphonic)
  and matches the "peace of mind" positioning.
- **App Review 5.1.2(i)** (updated 2026-06-08): sharing personal data with
  third-party AI requires prominent named-provider consent; **on-device is
  explicitly exempt**. Staying on-device = zero AI-consent friction, and a
  marketing line competitors can't use.

## 4. Why we likely don't need it

The 2026-07-06 evidence points to a detection architecture that doesn't want a
bigger LLM:
- **Width:** rediff double-fetch (xsdz.16, in flight) gives exact DAI boundaries
  with zero model involvement; MusicUnderstanding (xsdz.25) is the on-device
  backup signal.
- **Presence:** library self-fingerprinting (xsdz.17) is FM-free and refusal-proof.
- **FM quality-per-token at 4096:** iOS 27's `includeSchemaInPrompt=false` +
  `usage` accounting (4my.19) attack the actual budget bottleneck; guardrail
  false-positives were reduced in 26.4/27 (refusal rate to be re-measured).
- **If a bigger/looser model is still needed:** Core AI custom models (4my.18)
  provide an owned, on-device path — no guardrails, RAM-bound context — without
  any cloud exposure.

## 5. Adoption shapes, if ever revisited

1. **User opt-in enhancement tier** (default off; on-device remains the baseline).
   Consent improves the *privacy* story but does NOT answer the copyright-
   transmission question, and it forks the product. Legal-gated.
2. **Dev-time only** (e.g. Apple's Evaluations framework recommends a PCC model as
   LLM-judge; `fm` CLI batch work). Even this sends corpus-derived text to the
   cloud — a smaller policy call, still Dan's, still worth one counsel question.
3. **Never in the skip path, maybe in cosmetic features** (e.g. episode summaries
   opt-in). Same legal question at lower stakes.

## 6. Questions for counsel (if/when revisited)

1. Does transmitting podcast-derived transcript excerpts to an attested,
   stateless, no-retention third-party compute service change the copyright
   exposure relative to on-device processing?
2. Does explicit user opt-in shift that exposure meaningfully?
3. Is dev-time processing of our own captured corpus different from processing
   end-user listening content?
4. Any DMCA/ToS interaction with podcast feeds' own terms?

## 7. Recommendation & revisit triggers

**Do not adopt.** The physical-signal roadmap removes most of the need; the legal
question is unresolved; the eligibility gate makes it strategically fragile; and
on-device exemption from 5.1.2(i) is free differentiation.

Revisit only if **all** of: (a) rediff + fingerprinting + FM-economics fail to
deliver acceptable detection; (b) Core AI custom models prove insufficient;
(c) counsel clears the transmission question; (d) the eligibility/quota terms
stop being a poison pill.
