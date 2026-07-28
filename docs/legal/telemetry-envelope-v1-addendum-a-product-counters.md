# Telemetry Envelope V1 — Addendum A: product counters

- **Status:** PENDING LEGAL REVIEW — **not approved, not in effect**
- **Date:** 2026-07-27
- **Applies to:** `docs/legal/telemetry-envelope-v1.md` (Version 1)
- **Bead:** playhead-jw63.3
- **Author:** drafted by engineering; awaiting counsel signoff
- **Enforcer:** `Playhead/Services/Analytics/`, gated by
  `AnalyticsUploadGate.legalSignoffRecorded`

---

## Why this document exists

Envelope v1 §3 is a default-deny rule, and §2's automatic-upload allow-list
contains exactly five SLI keys. The nine counters below are **not** among
them, so under the envelope as written they may not be uploaded. §3 also
says engineers must not add fields to a telemetry uploader "without updating
this envelope first".

This addendum is that update, proposed rather than applied. Envelope v1 is
left byte-for-byte unchanged; a cross-reference in its §5 points here.
Nothing in this addendum is in effect until §4 below is signed.

**Current state of the implementation:** the counters accumulate on-device
and nothing is transmitted. `AnalyticsUploadGate.legalSignoffRecorded` is
`false`, the production writer is `DisabledAnalyticsRecordWriter`, and
`AnalyticsEnvelopeAllowListTests.uploadGateIsClosed` fails if either
changes. This satisfies envelope §7's requirement that non-local transport
paths stay behind an internal-only flag while the envelope is unsigned.

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
| §4.6 no advertising / user identifiers | No IDFA/IDFV/push token/iCloud user id/device name. Record names are fresh UUIDs per record, so two uploads from one device are unlinkable. Retention is computed on-device and uploaded as a counter; the install date never leaves the device. |
| §6.5 k-anonymity | The device cannot know cohort size, so suppression is applied on the read side: `scripts/analytics-rollup.sh` refuses to print a cell whose contributing-record count is below `k` (default 20, the value proposed to counsel in §6.5). |
| §7 gate until signoff | `AnalyticsUploadGate.legalSignoffRecorded = false`; the production writer is the disabled one; a test fails if either flips without this document being signed. |

## 4. Legal signoff

Engineering MUST NOT self-sign. Until this block is completed,
`AnalyticsUploadGate.legalSignoffRecorded` stays `false`.

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

Once signed: record the approving counsel's name in the header, change the
status line to `LEGAL APPROVED — <date>`, and flip
`AnalyticsUploadGate.legalSignoffRecorded` to `true` **in the same commit**.

## 5. Open questions for counsel

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
