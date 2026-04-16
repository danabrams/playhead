# Telemetry Envelope — Version 1

- **Status:** LEGAL REVIEW PENDING
- **Date:** 2026-04-16
- **Version:** 1
- **Bead:** playhead-pmq (Phase 0 deliverable — legal-boundary envelope)
- **Author:** drafted by engineering; awaiting counsel signoff
- **Consumers (downstream):** playhead-ghon (Phase 1.5 diagnostics bundle), playhead-hqhh (Phase 3 B5 bounded controller), playhead-1nl6 (Phase 1 SLI emitter)

## Sections

1. [Governing principle](#1-governing-principle)
2. [Allow-list of permitted fields](#2-allow-list-of-permitted-fields)
3. [Default-deny rule](#3-default-deny-rule)
4. [Prohibited list (enumerated exceptions to default-deny)](#4-prohibited-list-enumerated-exceptions-to-default-deny)
5. [Cross-references and downstream obligations](#5-cross-references-and-downstream-obligations)
6. [Ambiguities flagged for legal review](#6-ambiguities-flagged-for-legal-review)
7. [Legal signoff](#7-legal-signoff)

---

## 1. Governing principle

Three rules, user-confirmed by Dan on 2026-04-16, govern what may leave the device:

1. **Podcast metadata + audio-derived numerical representations = OK to upload.** Podcast content is licensed public material; identifiers (show / episode IDs, titles) and model-internal feature vectors / embeddings / fingerprints do not carry user PII.
2. **Human-readable transcript text = NOT OK.** Transcripts quote speech verbatim. That creates (a) licensed-content quotation-scope concerns for podcast audio and (b) a structural risk of capturing sensitive user-side audio if the pipeline ever ingests non-podcast sources (e.g. microphone or ambient capture).
3. **Per-user behavior history = NOT OK without explicit opt-in.** Listening patterns, pause/skip histories, seek traces, and session timelines describe user behavior — they are not content. They require explicit, revocable, per-episode or per-session opt-in transport paths.

Definitions used in this doc:

- **Aggregate-only** — bucketed or summarized such that a single user's data cannot be reconstructed from the transport payload. A value is aggregate-only when (a) the denominator spans at least a cohort (not a single device), and (b) the payload does not carry per-device identifiers alongside the value.
- **Cohort-keyed** — keyed by the SLI cohort axes in `Playhead/Services/Observability/SLICohortAxes.swift` (trigger × analysis mode × execution condition × episode-duration bucket). Cohort keys are not device-identifying.
- **User-initiated mail composer** — an explicit, per-invocation user action that opens `MFMailComposeViewController` with a bundle the user can inspect and cancel. Contrasted with **automatic upload**, where the app sends without per-invocation user consent.

## 2. Allow-list of permitted fields

This is the **complete** set of field keys permitted to leave the device in Version 1. It is subject to legal review; see §6 for open questions.

### 2.1 Operational telemetry (coarse counters, no user content)

| Field | Data type | Example value | Aggregation level | Transport | Retention |
| --- | --- | --- | --- | --- | --- |
| `app_version` | String | `"1.4.0 (build 2026.04.16.1)"` | per-device | user-initiated mail composer (diagnostics bundle) | no server-side retention (email, not analytics pipeline) |
| `os_version` | String | `"iOS 19.2"` | per-device | user-initiated mail composer | no server-side retention |
| `device_class` | String enum | `"iPhone17,1"` | per-device | user-initiated mail composer | no server-side retention |
| `build_type` | String enum | `"dogfood"`, `"testflight"`, `"release"` | per-device | user-initiated mail composer | no server-side retention |
| SLI aggregates | Numeric (duration seconds or rate in [0,1]) | `time_to_downloaded` P50 = 743.2 | **aggregate-only**, cohort-keyed | automatic upload permitted **only** at aggregate-only + cohort-keyed granularity | rolling 90 days server-side |
| scheduler event counts by type | Map<String enum, Int> | `{"downloadStarted": 12, "downloadCompleted": 11, "analysisBegan": 9}` | per-device counter (no per-event detail) | user-initiated mail composer | no server-side retention |
| `eligibility_snapshot.hardwareSupported` | Bool | `true` | per-device | user-initiated mail composer | no server-side retention |
| `eligibility_snapshot.regionSupported` | Bool | `true` | per-device | user-initiated mail composer | no server-side retention |
| `eligibility_snapshot.languageSupported` | Bool | `true` | per-device | user-initiated mail composer | no server-side retention |
| `eligibility_snapshot.appleIntelligenceEnabled` | Bool | `true` | per-device | user-initiated mail composer | no server-side retention |

**SLI aggregates — exhaustive list.** The five SLIs are defined in [`docs/slis/phase-0-slis.md`](../slis/phase-0-slis.md) with the Swift source of truth at [`Playhead/Services/Observability/SLI.swift`](../../Playhead/Services/Observability/SLI.swift). Only these five SLI keys, keyed by the four cohort axes in `SLICohortAxes.swift`, are permitted for automatic upload:

- `time_to_downloaded`
- `time_to_proximal_skip_ready`
- `ready_by_first_play_rate`
- `false_ready_rate`
- `unattributed_pause_rate`

Values are emitted per the Phase 1 contract (§"Phase 1 contract" of `phase-0-slis.md`): empty cells as `nil`, not `0`.

**Eligibility snapshot — exhaustive list.** Only the four boolean fields above. Free-form resolution hints, reason strings, or human-readable diagnostic messages are **not** on the allow-list; they are prohibited (see §4). Cross-reference: playhead-2fd `AnalysisEligibility` contract (in flight). The `modelAvailableNow` bool from the 2fd contract is **deferred** pending legal review (see §6).

### 2.2 Podcast metadata (public/licensed content identifiers)

| Field | Data type | Example value | Aggregation level | Transport | Retention |
| --- | --- | --- | --- | --- | --- |
| `episode_id` | String (cleartext) | `"buzzsprout-14287739"` | per-episode | user-initiated mail composer **only**, inside the opt-in per-episode bundle | no server-side retention |
| `episode_title` | String (cleartext) | `"Ep. 127: The Kelly Ripa Interview"` | per-episode | user-initiated mail composer **only**, inside the opt-in per-episode bundle | no server-side retention |
| `show_id` | String (cleartext) | `"npr-planet-money"` | per-show | user-initiated mail composer **only**, inside the opt-in per-episode bundle | no server-side retention |
| `show_title` | String (cleartext) | `"Planet Money"` | per-show | user-initiated mail composer **only**, inside the opt-in per-episode bundle | no server-side retention |
| `episode_duration_seconds` | Number | `2714.5` | per-episode | user-initiated mail composer **only**, inside the opt-in per-episode bundle | no server-side retention |
| `episode_publish_date` | ISO-8601 String | `"2026-04-12T00:00:00Z"` | per-episode | user-initiated mail composer **only**, inside the opt-in per-episode bundle | no server-side retention |

Podcast metadata in cleartext is re-identifying at the user level (combination of subscribed shows is a fingerprint). For that reason, cleartext podcast metadata is permitted **only** via the opt-in per-episode diagnostics bundle path (playhead-ghon, tier 2). The default diagnostics bundle carries `episodeId_hash`, not cleartext episode IDs (see playhead-ghon schema).

### 2.3 Audio-derived numerical representations (not reconstructable as speech)

| Field | Data type | Example value | Aggregation level | Transport | Retention |
| --- | --- | --- | --- | --- | --- |
| FM classifier feature vectors | Float array (fixed length) | `[0.12, -0.08, 0.44, ..., 0.03]` | per-slice or per-episode summary | user-initiated mail composer **only**, inside the opt-in per-episode bundle | no server-side retention |
| Feature-extractor embeddings | Float array (fixed length) | `[0.51, 0.02, ..., -0.19]` | per-slice or per-episode summary | user-initiated mail composer **only**, inside the opt-in per-episode bundle | no server-side retention |
| Audio fingerprints (content-ID hashes) | Opaque bytes / hex string | `"a1c4…f7"` | per-episode | user-initiated mail composer **only**, inside the opt-in per-episode bundle | no server-side retention |

**Reconstructability claim.** Engineering asserts these representations are **not reconstructable as intelligible speech** by inversion (they are lossy model-internal features, not audio). This assertion is subject to the legal checklist item in playhead-ghon: *"fingerprint re-identification risk review."* Until that review completes, audio-derived numerical representations ride the opt-in per-episode bundle path only — not the default bundle, and not the automatic aggregate-only SLI path.

## 3. Default-deny rule

**Any field not explicitly listed in §2 is prohibited from leaving the device.** There is no implicit "similar to an allowed field" path. New fields require (a) an updated version of this envelope document, (b) legal signoff on the updated version, and (c) corresponding changes to the downstream enforcers in playhead-ghon and playhead-hqhh.

Engineers MUST NOT add fields to diagnostics bundles, telemetry uploaders, or crash-reporting payloads without updating this envelope first.

## 4. Prohibited list (enumerated exceptions to default-deny)

The default-deny rule in §3 covers everything; this list calls out specific categories that engineering or product may be tempted to include and flags them as forbidden:

1. **Transcript text (any form).** Verbatim transcripts, paraphrased transcripts, transcript excerpts, N-grams reconstructable into text, and ASR confidence-scored word traces are all prohibited. This includes "ad window" transcript excerpts unless and until a future envelope version explicitly authorizes them after legal review (see playhead-ghon legal checklist item (b)).
2. **Per-user listening history.** Play/pause/skip/seek event streams, session timelines, per-episode completion percentages, and any time-series of user behavior. Scheduler event **counts by type** (§2.1) are permitted; the event **stream itself** is not.
3. **IP address beyond transport necessity.** IPs captured as part of SMTP/HTTPS transport metadata are unavoidable at the network layer; IPs stored, logged, or forwarded by application-layer code are prohibited.
4. **Free-form eligibility resolution hints.** Human-readable strings like `"region not supported: US required"` or `"download 47% complete"` are prohibited — only the four eligibility booleans in §2.1 and the `modelAvailableNow` bool (if legal approves it, §6) may leave the device.
5. **Any raw audio or raw audio-derived format that can be inverted to audio** (PCM, MFCCs, mel-spectrograms at resolutions sufficient for Griffin–Lim-style inversion). Only fixed-length model-internal feature vectors and fingerprint hashes (§2.3) are permitted, and those ride the opt-in bundle path only.
6. **Advertising / user identifiers.** `IDFA`, `IDFV`, Advertising IDs, push tokens, iCloud user identifiers, device names (`UIDevice.name`), and any other device- or user-stable identifier are prohibited. `device_class` (§2.1) is an SoC class, not a device-stable identifier.
7. **Any field not explicitly listed in §2.** Restating §3 for clarity.

## 5. Cross-references and downstream obligations

### 5.1 playhead-ghon (Phase 1.5 diagnostics bundle)

The diagnostics bundle defined in playhead-ghon is a **strict subset** of this envelope's allow-list. Specifically:

- **Default bundle (always support-safe)** — draws only from §2.1. Uses `episodeId_hash` instead of cleartext `episode_id`. No podcast metadata in cleartext. No audio-derived numerical representations.
- **Opt-in per-episode bundle (user toggles `Episode.diagnosticsOptIn`)** — may additionally include cleartext §2.2 podcast metadata and §2.3 audio-derived numerical representations, but still **may not** include §4 prohibited items (notably, transcript text is forbidden even with opt-in in Version 1 of this envelope — re-authorization requires legal review per playhead-ghon checklist (b)).
- Transport for both bundles is the user-initiated mail composer. No automatic upload path.

### 5.2 playhead-hqhh (Phase 3 B5 bounded local controller)

The B5 bounded controller is **documented as works-fully-local**. It has **no required-upload fields.** All actuator decisions, EWMA/Beta estimators, hierarchical backoff state, and activation-floor counters live on-device. Any optional telemetry about the controller's behavior that ships with the app MUST ride the §2.1 automatic-upload path (aggregate-only SLIs + cohort-keyed counts) and nothing else. The controller MUST remain fully functional if all upload paths are disabled by the user.

### 5.3 playhead-d99 SLI definitions

The SLIs referenced in §2.1 are defined in [`docs/slis/phase-0-slis.md`](../slis/phase-0-slis.md). Swift source of truth: [`Playhead/Services/Observability/SLI.swift`](../../Playhead/Services/Observability/SLI.swift). Cohort axes: [`Playhead/Services/Observability/SLICohortAxes.swift`](../../Playhead/Services/Observability/SLICohortAxes.swift). When values disagree between this envelope and the code, the code wins for unit/threshold questions; this envelope wins for what-may-leave-the-device questions.

### 5.4 playhead-2fd AnalysisEligibility contract (in flight in parallel)

The four eligibility booleans in §2.1 are the **only** eligibility-related data permitted to leave the device in Version 1. Free-form resolution hint strings, reason enums beyond the four booleans, and any per-show / per-episode eligibility detail are **not permitted** under this envelope. The 2fd contract also defines `modelAvailableNow: Bool`; see §6.1 below — that field is **deferred** pending legal review and is not on the V1 allow-list.

## 6. Ambiguities flagged for legal review

These are open questions the draft surfaces for counsel. Each is a point where engineering has made a **conservative default** (exclude rather than include) pending review.

### 6.1 `modelAvailableNow` eligibility bool

playhead-2fd defines a fifth eligibility field, `modelAvailableNow: Bool`, distinct from the four permanent-support booleans. This field reflects transient on-device state (model asset residency). Engineering's conservative default in V1: **exclude** from the allow-list. Legal review: is a transient model-availability boolean operationally equivalent to the four permanent booleans for envelope purposes, or does it carry different risk because it can correlate with storage state / download activity?

### 6.2 Combination re-identification risk for podcast metadata

Cleartext episode IDs are low-risk in isolation. A user's **set** of listened shows + episodes is a re-identifying fingerprint. V1 mitigates this by routing cleartext podcast metadata only through the opt-in per-episode bundle (one episode at a time, user-inspected). Legal review: is per-episode opt-in sufficient, or does the envelope need additional combination-limiting guardrails (e.g. single-episode-per-bundle hard cap)?

### 6.3 Fingerprint re-identification risk

Content-identification audio fingerprints are designed to match known content. If an attacker had a fingerprint database over user-generated audio, they could reverse-match. Engineering's claim: our pipeline only fingerprints licensed podcast content, not user audio. Legal review: is a structural claim about pipeline inputs sufficient, or does the envelope need a pipeline-source attestation (with the diagnostics bundle explicitly stating "fingerprint generated from podcast source X")? Cross-reference: playhead-ghon legal checklist item (c).

### 6.4 Transcript excerpt re-authorization path

V1 forbids transcript text (§4 item 1). playhead-ghon's opt-in bundle schema includes `transcript_excerpts[]` as a future tier-2 field. That field is **blocked** by this envelope in V1. Legal review: under what conditions (if any) could a future envelope version authorize short transcript excerpts on the opt-in path? Possible guardrails to consider: bounded window length, licensed-content-only attestation, no user-audio-sourced transcripts ever.

### 6.5 Aggregate-only definition (k-anonymity threshold)

§1 defines aggregate-only as "bucketed or summarized such that a single user's data cannot be reconstructed." V1 does not set a specific k-anonymity floor (minimum cohort size before an aggregate can leave the device). Engineering's conservative default: do not emit aggregates for cohorts below some floor. Legal review: what is the appropriate floor? (Proposed for legal consideration: k ≥ 20 cohort members before the aggregate is emitted.)

### 6.6 Crash reports / OS-level telemetry

iOS / Xcode crash reports, MetricKit payloads, and Apple-mediated telemetry channels are outside the application's direct control. V1 of this envelope governs **application-layer** data paths. Legal review: does the envelope need an explicit statement on Apple-mediated channels, and does enabling MetricKit count as automatic upload under §2.1?

## 7. Legal signoff

**This envelope is PENDING LEGAL REVIEW.** The signoff fields below are a template to be filled in by project legal counsel. Engineering MUST NOT self-sign. Until this section is completed and signed, downstream enforcers (playhead-ghon, playhead-hqhh) MUST treat this envelope as a **draft** and gate all non-local transport paths behind an internal-only build flag.

```
---------------------------------------------------------------
TELEMETRY ENVELOPE V1 — LEGAL APPROVAL

Reviewed by:            _________________________________________
                        (printed name, role)

Signature:              _________________________________________

Date of signoff:        _________________________________________

Version approved:       1

Scope of approval:      [ ] §2.1 Operational telemetry (allow-list)
                        [ ] §2.2 Podcast metadata (allow-list)
                        [ ] §2.3 Audio-derived numerical reps (allow-list)
                        [ ] §3    Default-deny rule
                        [ ] §4    Prohibited list
                        [ ] §5    Cross-reference obligations
                        [ ] §6    Ambiguities resolved (list below)

Ambiguity resolutions:  _________________________________________
                        _________________________________________
                        _________________________________________

Conditions / caveats:   _________________________________________
                        _________________________________________

Next review date:       _________________________________________
---------------------------------------------------------------
```

Once signed, update the header status from "LEGAL REVIEW PENDING" to "LEGAL APPROVED — V1 — \<date\>" and record the approving counsel's name in the header. Any material change to §2–§4 requires a new version (V2, V3, …) and a fresh signoff — this document is immutable once signed.
