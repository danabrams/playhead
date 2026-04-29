# Diagnostics Bundle — Legal Checklist Audit

Scope: playhead-fsy3 Scope 2 — verification artifact tying each item of
the four-part legal checklist (a)-(d) to the test that proves it.

The diagnostics bundle is the only user-driven path that exfiltrates
state off the device. Two-tier shape:

- **DefaultBundle** ships on every export. Always-safe: no cleartext
  episode IDs, no transcript text, no audio.
- **OptInBundle** ships only for episodes the user explicitly opted
  into (`Episode.diagnosticsOptIn == true`). Carries cleartext
  `episode_id`, transcript excerpts (±30 s window, 1000-char cap), and
  coarse feature summaries.

Schema reference: `Playhead/Support/Diagnostics/DiagnosticsBundle.swift`.
Encoder: `DiagnosticsExportService.encode(_:)` (sortedKeys, prettyPrinted,
ISO-8601 dates).

## (a) Default bundle shape: no `episodeId` at type level

**Contract.** The `DefaultBundle` Codable type cannot carry a raw
`episodeId`. Both nested record types (`SchedulerEvent` and
`WorkJournalRecord`) expose `episodeIdHash: String` with a CodingKey of
`episode_id_hash` — never `episodeId` / `episode_id`. The builder
(`DiagnosticsBundleBuilder.buildDefault`) routes every episode reference
through `EpisodeIdHasher.hash(installID:episodeId:)` before
construction, so the raw id has no surface to escape on.

**Verified by:**
- `PlayheadTests/Support/Diagnostics/DiagnosticsBundleShapeTests.swift:247`
  — `defaultBundleHasOnlyAllowedKeys`: top-level keys ⊆
  `{generated_at, default, opt_in}` and the `default` subtree's keys ⊆
  the documented `DefaultBundle.CodingKeys` set
  (`{app_version, os_version, device_class, build_type,
  eligibility_snapshot, analysis_unavailable_reason, scheduler_events,
  work_journal_tail}`).
- `PlayheadTests/Support/Diagnostics/DiagnosticsBundleShapeTests.swift:272`
  — `defaultBundleHasNoRawEpisodeIdKeyAnywhere`: walks every key in the
  encoded JSON tree (recursive over nested objects + arrays) and
  asserts no key matches `episodeid` / `episode_id` (case-insensitive),
  with a belt-and-suspenders sweep that the seeded raw episode IDs do
  not appear as VALUES anywhere in the encoded bytes.
- `PlayheadTests/Support/Diagnostics/DiagnosticsBundleBuilderTests.swift`
  (legacy ghon coverage) — covers each scheduler-event / work-journal
  field in isolation.

## (b) OptInBundle isolation: `transcript_excerpts` + `feature_summaries` only in opt-in bundle

**Contract.** `transcript_excerpts` and `feature_summaries` live on
`OptInBundle.Episode`. The `DefaultBundle` type cannot reference them —
there is no field of either name on `DefaultBundle` or its nested
records, and the encoder follows the explicit `CodingKeys` so a
schema-drift slip would have to add the field at the type level
(catchable in code review and by the shape test below).

**Type-level enforcement caveat.** Swift cannot express "this struct
must NOT contain these field names" at compile time. The contract is
therefore enforced by:
1. The encoder honoring explicit `CodingKeys` on every Codable type
   (no synthesised key surface).
2. The runtime tree-walk audit in
   `defaultBundleNeverContainsTranscriptOrFeatureSummaryKeys`, which is
   the assertion that would trip the alarm if (1) ever regressed.

**Verified by:**
- `PlayheadTests/Support/Diagnostics/DiagnosticsBundleShapeTests.swift:312`
  — `defaultBundleNeverContainsTranscriptOrFeatureSummaryKeys`: walks
  the default-bundle fixture's JSON tree and asserts no key named
  `transcript_excerpts`, `feature_summaries`, or `episode_title`
  appears anywhere.
- `PlayheadTests/Support/Diagnostics/DiagnosticsBundleShapeTests.swift:330`
  — `optInBundleEpisodeIdIsHashed64HexChars`: confirms the opt-in
  fixture DOES carry a populated `transcript_excerpts` array per
  opted-in episode (the test would be vacuous without this), and the
  episode's cleartext `episode_id` is non-empty.
- `PlayheadTests/E2E/Privacy/DiagnosticExportRedactionTests.swift`
  (legacy h3h coverage) — sentinel-text sweep that confirms transcript
  text appears ONLY inside the `opt_in` subtree of an end-to-end
  encoded bundle.

## (c) Hashing: per-install salt, SHA-256 lowercase hex, UTF-8 unicode handled

**Contract.** `episodeId_hash = SHA-256(installID.uuidString || episodeId)`,
emitted as 64 lowercase hex characters. The salt is a per-install UUID
provisioned by `InstallIDProvider` (SwiftData-backed singleton row).
The hash is stable within an install (so support can correlate events)
and never collides cross-install (so no third party can link two
installs' diagnostic bundles to the same episode).

**Verified by:**
- `PlayheadTests/Support/Diagnostics/EpisodeIdHasherTests.swift:25` —
  `hexShape`: 64 chars, all lowercase hex.
- `PlayheadTests/Support/Diagnostics/EpisodeIdHasherTests.swift:46` —
  `perInstallSalt`: distinct installIDs → distinct hashes for the same
  episode.
- `PlayheadTests/Support/Diagnostics/EpisodeIdHasherTests.swift:66` —
  `saltPrepended`: locks down the concatenation order
  (`installID.uuidString.utf8 + episodeId.utf8`).
- `PlayheadTests/Support/Diagnostics/EpisodeIdHasherTests.swift:91` —
  `unicodeEpisodeId`: non-ASCII episode IDs hash via UTF-8 bytes
  (verified end-to-end by feeding a `\u{1F4FB}` codepoint).
- `PlayheadTests/Support/Diagnostics/InstallIdentityTests.swift:35` /
  `:46` — provider returns a stable UUID across reads on the same
  install and distinct UUIDs across separate installs.
- `PlayheadTests/Support/Diagnostics/InstallIDProviderCoordinatorIntegrationTests.swift:96`
  — `providerUUIDFlowsIntoSaltedHash` (Scope 1): integration-level
  proof that `InstallIDProvider.installID()` flows through
  `DiagnosticsExportEnvironment.installID` into every
  `episode_id_hash` field of the encoded bundle.
- `PlayheadTests/Support/Diagnostics/DiagnosticsBundleShapeTests.swift:330`
  — `optInBundleEpisodeIdIsHashed64HexChars`: every `episode_id_hash`
  in the opt-in fixture's default subtree matches the regex
  `^[0-9a-f]{64}$`.

## (d) Opt-in reset policy: `.sent` / `.saved` clears flag; `.cancelled` / `.failed` preserves

**Contract.** Pure rule on
`(currentValue: Bool, result: DiagnosticsMailComposeResult) -> Bool`:

| `result`     | new value                  |
|--------------|----------------------------|
| `.sent`      | `false` (delivered)        |
| `.saved`     | `false` (in user's mailbox)|
| `.cancelled` | `current`  (user backed out) |
| `.failed`    | `current`  (system error, user may retry) |

The coordinator (`DiagnosticsExportCoordinator.applyOptInResetIfNeeded`)
applies the reset after the presenter completes — never before — so
cancel/fail paths cannot leak state. The
`shouldReset(result:)` predicate is kept consistent with
`!newValue(current: true, result:)` by an explicit consistency test.

**Verified by:**
- `PlayheadTests/Support/Diagnostics/DiagnosticsOptInResetPolicyTests.swift:28`
  — `(true, .sent) → false`.
- `PlayheadTests/Support/Diagnostics/DiagnosticsOptInResetPolicyTests.swift:33`
  — `(true, .saved) → false`.
- `PlayheadTests/Support/Diagnostics/DiagnosticsOptInResetPolicyTests.swift:50`
  — `(true, .cancelled) → true` (preserve).
- `PlayheadTests/Support/Diagnostics/DiagnosticsOptInResetPolicyTests.swift:55`
  — `(true, .failed) → true` (preserve).
- `PlayheadTests/Support/Diagnostics/DiagnosticsOptInResetPolicyTests.swift:76`
  — `enumExhaustivenessCanary`: alarm if a future
  `DiagnosticsMailComposeResult` case ever lands without a
  corresponding policy decision.
- `PlayheadTests/Support/Diagnostics/DiagnosticsOptInResetPolicyTests.swift:106`
  — `shouldResetConsistentWithNewValue`: predicate matches
  `!newValue(current: true, ...)` for every case.
- `PlayheadTests/Support/Diagnostics/DiagnosticsExportCoordinatorTests.swift:119`
  — `.sent` clears opt-in for every shipped episode (coordinator-level
  proof).
- `PlayheadTests/Support/Diagnostics/DiagnosticsExportCoordinatorTests.swift:165`
  / `:183` — `.cancelled` / `.failed` preserve (sink is not called).
