# Telemetry Envelope V1 — Addendum A: product counters

- **Status:** APPROVED BY FOUNDER 2026-07-28 — **not activated.** Upload stays
  disabled for v1 by product decision, not by a pending approval. See §4.
- **Date:** 2026-07-27; approved 2026-07-28
- **Applies to:** `docs/legal/telemetry-envelope-v1.md` (Version 1)
- **Bead:** playhead-jw63.3
- **Author:** drafted by engineering; approved by Dan Abrams as principal.
  **This is a founder self-approval, not a review by qualified counsel.**
  External legal review is deliberately deferred — see §4.1.
- **Enforcer:** `Playhead/Services/Analytics/`, gated by
  `AnalyticsUploadGate.legalSignoffRecorded` (still `false` — see §4.2)

---

## Why this document exists

Envelope v1 §3 is a default-deny rule, and §2's automatic-upload allow-list
contains exactly five SLI keys. The nine counters below are **not** among
them, so under the envelope as written they may not be uploaded. §3 also
says engineers must not add fields to a telemetry uploader "without updating
this envelope first".

This addendum is that update. Envelope v1 is left byte-for-byte unchanged; a
cross-reference in its §5 points here. It was **approved by the founder on
2026-07-28** (§4) — approved, but deliberately **not activated**.

**Current state of the implementation:** the counters accumulate on-device
and nothing is transmitted. `AnalyticsUploadGate.legalSignoffRecorded` is
`false`, the production writer is `DisabledAnalyticsRecordWriter`, and
`AnalyticsEnvelopeAllowListTests.uploadGateIsClosed` fails if either
changes.

Read that state correctly: the gate is shut by **product decision**, not by a
pending approval. Envelope §7's "unsigned envelope" condition no longer
applies, so the flag is now the v1 activation switch rather than a legal
blocker. §4.3 explains why it stays shut, and what flipping it requires.

## 1. Proposed allow-list additions

Transport for **all** rows: CloudKit **public** database, write-only
anonymous append. No record is ever read, queried, updated, or subscribed to
by the device. Aggregation level for all rows: **aggregate-only**,
cohort-keyed per §2 below.

| Field | Type | Example | Meaning |
| --- | --- | --- | --- |
| `envelope_version` | Int | `1` | Envelope version the sender honors |
| `payload_schema` | String (frozen token) | `playhead.analytics.increment.v1` | Record schema |
| `cohort_duration_bucket` | String (frozen token) | `between30and60m` | Cohort key; see §2 |
| `banners_shown` | Int | `12` | Banners displayed since last accepted upload |
| `banners_confirmed` | Int | `9` | "Was this right?" → yes |
| `banners_denied` | Int | `2` | "Was this right?" → no |
| `manual_skip_forward_reaches` | Int | `7` | +30s button presses (north-star numerator) |
| `listening_seconds` | Int | `4210` | Wall-clock playback seconds (north-star denominator) |
| `retention_installs` | Int | `1` | First launch on this install; at most 1 ever |
| `retention_d1_returned` | Int | `1` | Install returned ≥1 day later; at most 1 ever |
| `retention_d7_returned` | Int | `1` | …≥7 days later; at most 1 ever |
| `retention_d30_returned` | Int | `1` | …≥30 days later; at most 1 ever |

Server-side retention proposed: **rolling 90 days**, matching the SLI
aggregates row in envelope §2.1.

This table is the complete set. `TelemetryEnvelopeV1AllowList.permittedKeys`
is generated from the same enums and pinned against a hand-written literal in
`AnalyticsEnvelopeAllowListTests.keySetIsFrozen`, so the code and this
document cannot drift apart silently.

## 2. Cohort key

`cohort_duration_bucket` takes one of exactly five values:
`all`, `under30m`, `between30and60m`, `between60and90m`, `over90m` — the
episode-duration axis of `SLICohortAxes.swift`, plus `all` for counters that
are not episode-scoped (banner feedback and retention).

The other three SLI axes (trigger × analysis mode × execution condition) are
**not** used. None of the counters can supply them at their measurement
point, and an axis emitted as a constant adds re-identification surface
without adding analytic value.

`TelemetryEnvelopeV1AllowList.permittedTokens` is the complete set of strings
any field may hold: the five cohort values plus the schema token. A string
outside that set — a title, a URL, a locale, a device name — causes the whole
record to be dropped rather than sent.

## 3. How each envelope rule is honored

| Envelope rule | How |
| --- | --- |
| §1.2 no transcript text | No field is a string except two frozen tokens. `AnalyticsEgressSentinelTests` seeds a transcript line into the pipeline and asserts it never appears in an outbound record. |
| §1.3 no per-user behavior history | No event stream, no timestamps, no session timeline, no per-episode completion. Only cumulative counters, uploaded as deltas. |
| §3 default-deny | `TelemetryEnvelopeV1AllowList.validate` rejects any unrecognized key or value, whole-record. Enforced again at rest: unknown keys are dropped when the on-disk blob is decoded. |
| §4.2 no listening history | Listening time is a single running integer of seconds, not a session log. |
| §4.6 no advertising / user identifiers | No IDFA/IDFV/push token/iCloud user id/device name in the payload. Record names are fresh UUIDs, so nothing we write links two uploads. Retention is computed on-device and uploaded as a counter; the install date never leaves the device. **Caveat:** CloudKit stamps `creatorUserRecordID` server-side — see §5.5, the open question this addendum most needs answered. |
| §6.5 k-anonymity | The device cannot know cohort size, so suppression is applied on the read side: `scripts/analytics-rollup.sh` refuses to print a cell whose contributing-record count is below `k` (default 20, the value proposed to counsel in §6.5). |
| §7 gate until signoff | `AnalyticsUploadGate.legalSignoffRecorded = false`; the production writer is the disabled one; a test fails if either flips without this document being signed. |

## 4. Approval

### 4.1 Why this is a founder approval and not counsel review

The original rule here read "Engineering MUST NOT self-sign." That rule
exists so an implementer cannot wave through their own data collection, and
it still holds: engineering did not approve this. It was approved by the
founder, who is the accountable party rather than the implementer.

External counsel is **deliberately deferred**, with reasoning recorded so a
later reader can judge whether the call was sound:

- The collection is close to the least sensitive telemetry this app could
  ship — nine integers, no device identifier, no episode content, retention
  computed on-device.
- The material legal surface for an App Store release is not these counters.
  It is (a) the accuracy of the privacy nutrition label, (b) a privacy policy
  matching actual behaviour, and (c) user-facing claims being true. The
  strongest such claim already shipped — the About screen's "Your podcasts
  never leave your device" — and these counters do not contradict it: no
  audio, no transcripts, no episode content is uploaded.
- Playhead is pre-revenue. Counsel is planned at first paid release, when a
  mistake costs refunds and delisting rather than embarrassment, and when the
  privacy policy and nutrition label need review anyway.

**Consequence to respect:** the word "anonymous" must not appear in
user-facing copy about this collection. Say concretely what is sent. §5.5
below explains why that precision matters given CloudKit's platform metadata.

### 4.2 Approval record

```
---------------------------------------------------------------
TELEMETRY ENVELOPE V1 — ADDENDUM A — APPROVAL

Reviewed by:            Dan Abrams — founder, principal
                        (NOT qualified counsel; see §4.1)

Approved:               2026-07-28

Scope of approval:      [x] §1 Allow-list additions (12 fields)
                        [x] §2 Cohort key restricted to duration bucket
                        [x] §3 Rule-by-rule mapping
                        [x] Server-side retention: 90 days
                        [x] k-anonymity floor: k = 20

Conditions / caveats:   1. APPROVED BUT NOT ACTIVATED. Upload remains
                           disabled for v1 as a product decision.
                           `legalSignoffRecorded` stays false.
                        2. Activation is a submission-time decision, taken
                           alongside the App Store Connect privacy nutrition
                           label and support URL.
                        3. "Anonymous" is barred from user-facing copy about
                           this collection.
                        4. Qualified-counsel review required before first
                           PAID release.

Next review date:       At App Store submission, and again at first paid
                        release (counsel).
---------------------------------------------------------------
```

### 4.3 Why the gate stays shut anyway

Approval removes the *legal* blocker. Upload stays off for v1 on product
grounds: at v1 user counts the k ≥ 20 floor would suppress most rollup rows,
so uploading would carry the CloudKit creator-identity question (§5.5) live
while returning little that is readable. Local accumulation plus the
diagnostics bundle — which the playhead-jw63.5 feedback channel already
ships — gives real numbers from the founder's device and any dogfooder who
sends a bundle, at zero upload surface.

To activate later: answer nothing further, flip
`AnalyticsUploadGate.legalSignoffRecorded` to `true`, and record the
activation date in this section **in the same commit**.

```
---------------------------------------------------------------
TELEMETRY ENVELOPE V1 — ADDENDUM A — LEGAL APPROVAL

Reviewed by:            _________________________________________
                        (printed name, role)

Signature:              _________________________________________

Date of signoff:        _________________________________________

Scope of approval:      [ ] §1 Allow-list additions (12 fields)
                        [ ] §2 Cohort key restricted to duration bucket
                        [ ] §3 Rule-by-rule mapping
                        [ ] Server-side retention: 90 days
                        [ ] k-anonymity floor: k = ____

Conditions / caveats:   _________________________________________
                        _________________________________________

Next review date:       _________________________________________
---------------------------------------------------------------
```

## 5. Questions — answered by the founder 2026-07-28

These were drafted as open questions for counsel. They are now **answered as
founder decisions**, per §4.1. Each answer records what was decided and what
would change it, so counsel can revisit them at first paid release without
re-deriving the context.

**Q1 — anonymous-increment sufficiency: ACCEPTED as designed.** Server-side
summation satisfies §1(a); the payload carries no per-device identifier, so
§1(b) holds absolutely. A pre-transmission k-floor is rejected: a device
cannot enforce one without asking the server how many peers it has, and that
query is exactly the linkage this design avoids. The floor is enforced on the
read side instead (Q2).

**Q2 — k floor: k = 20**, as proposed in §6.5. `scripts/analytics-rollup.sh`
reads it from one constant; changing it is a one-line edit.

**Q3 — retention ratio fidelity: EXCLUDE the install-week bucket**, adopting
engineering's conservative default. The reported figure is therefore a
*window* ratio, not a cohort ratio — stable when install volume is stable and
lagging when it swings. That imprecision is accepted in exchange for not
adding a linkage surface to a small population. Whoever reads the number must
know this; it is recorded in `RetentionBucketTracker.swift`'s header.

**Q4 — CloudKit as a first-party Apple service: no different treatment** from
a self-hosted endpoint under §2.1. Apple sees container writes the way any
transport provider sees traffic.

**Q5 — platform-stamped creator identity: ACCEPTED as the §4.3 case**, with
the mitigation already implemented. CloudKit attaches `creatorUserRecordID`
to public-database records, derived from the writing device's iCloud account.
Our payload carries no identifier and record names are fresh UUIDs, so
nothing *we* write links two records — but CloudKit's own metadata does.
This is treated as unavoidable transport metadata (§4.3), not as an
application-layer iCloud identifier (§4.6), because application code neither
stores nor forwards it: `scripts/analytics-rollup.sh` reads only the nine
counter fields, so no account-linked view is ever materialized.

**This is the one answer most likely to be wrong**, and it is the reason
"anonymous" is barred from user-facing copy (§4.1). If counsel later
disagrees, the remedy is a different `AnalyticsRecordWriting` implementation
— one whose transport does not authenticate the writer. **The payload does
not change**, so the blast radius is one class. It is also moot while upload
stays disabled (§4.3).

## 6. Original open questions as drafted for counsel

1. **Aggregate-only, one device at a time.** Each record carries one
   device's delta with no identifier. Aggregation across devices happens
   server-side. Envelope §1's definition requires "(a) the denominator spans
   at least a cohort … and (b) the payload does not carry per-device
   identifiers". (b) holds absolutely. (a) holds only after summation. Is
   the anonymous-increment model sufficient, or does the envelope want a
   k-anonymity floor enforced before transmission (which a device cannot do
   without asking the server how many peers it has — a query the design
   deliberately does not make)?
2. **k floor.** §6.5 proposes k ≥ 20. Confirm the value; the rollup script
   reads it from one constant.
3. **Retention ratio fidelity.** Because returns are unlinkable to installs,
   the ratio is a window ratio, not a cohort ratio (see the header comment in
   `RetentionBucketTracker.swift`). Carrying an install-week bucket would fix
   that and would add a linkage surface. Engineering's conservative default:
   exclude. Confirm.
4. **CloudKit public database.** Apple sees the container writes. Does
   "automatic upload" to a first-party Apple service need any different
   treatment from a self-hosted endpoint under §2.1?
5. **Platform-stamped creator identity — the most important question here.**
   CloudKit attaches server-side system metadata to every public-database
   record, including `creatorUserRecordID`, derived from the writing
   device's iCloud account. The application payload carries no identifier
   and record names are fresh UUIDs, so nothing *we* write links two
   records — but CloudKit's own metadata does, and that metadata is visible
   to whoever queries the container (us). Envelope §4.6 prohibits iCloud
   user identifiers in application-layer data; §4.3 accepts IP addresses as
   unavoidable transport metadata provided application code neither stores
   nor forwards them. Engineering reads this as the §4.3 case and has
   applied the matching mitigation: `scripts/analytics-rollup.sh` reads
   only the nine counter fields and never a system field, so no
   account-linked view is ever materialized. Counsel: is that sufficient,
   or does the anonymity claim require a transport that does not
   authenticate the writer at all? If the latter, the fix is a different
   `AnalyticsRecordWriting` implementation — the payload does not change.
