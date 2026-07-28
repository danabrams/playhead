# Diagnostics Bundle — Legal Checklist Audit

Scope: playhead-fsy3 Scope 2 — verification artifact tying each item of
the four-part legal checklist (a)-(d) to the test that proves it.
Later sections extend it: (e) stability diagnostics (playhead-jw63.4),
(f) delivery surfaces (playhead-jw63.5), (g) banner tallies
(playhead-bfq7).

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

## (e) Stability diagnostics: MetricKit crash + hang records carry no free text

Added by playhead-jw63.4. `DefaultBundle.stability_diagnostics` carries
the local MetricKit crash / hang ring buffer
(`StabilityDiagnosticsStore`, 50 records, `Application Support/Diagnostics/
stability-diagnostics.jsonl`, `.completeUntilFirstUserAuthentication`).

**Why this needs its own checklist item.** MetricKit is the one input to
the bundle that Playhead does not author. iOS hands the app free-text
fields that routinely contain interpolated application strings and
absolute container paths:

| MetricKit field | What it can contain | Disposition |
|---|---|---|
| `exceptionReason.composedMessage` | the `NSException` reason, e.g. `assertionFailure("no ad span for \(episode.title)")` | **never read** |
| `exceptionReason.formatString` / `.arguments` | the same message, pre-interpolation | **never read** |
| `terminationReason` | `<RBSTerminateContext\| … >` including the app bundle path | decomposed to `termination_namespace` (allowlisted token) + `termination_code` (hex); raw string discarded |
| `virtualMemoryRegionInfo` | memory-map dump with absolute paths | **never read** |
| frame `address` | ASLR'd absolute address | **never read** (`offset_into_binary_text_segment` is what `atos` wants) |

**Contract, in four parts.**

1. **Allowlist projection.** `MetricKitDiagnosticProjector` names every
   key it reads (`payloadKeys`, `metadataKeys`, `exceptionReasonKeys`).
   A field that is never read cannot leak, and a future iOS field is
   invisible by default rather than by vigilance.
2. **Character allowlist, plus a per-field SHAPE bound.** Every
   surviving string passes `DiagnosticTextSanitizer`, which admits only
   `[A-Za-z0-9_.$+\-() ]` (plus the comma in `device_type`, behind the
   ASCII-qualified Apple `iPhone17,1` model grammar). No `/`, no `:`,
   no quotes, no non-ASCII — so a URL, a POSIX path, or non-Latin
   transcript text cannot be represented.

   **Stated honestly: the character class alone does not reject English
   prose.** The space is allowlisted because real OS build strings need
   it (`iPhone OS 27.0 (25A123)`), so an unpunctuated ASCII phrase is
   allowlist-clean. What closes that is the shape bound per field:

   - `identifier(_:)` — used for `objc_exception_name`,
     `objc_exception_class_name` and `binary_name` — rejects any string
     containing a SPACE. These are single tokens by construction, and
     `exceptionName` / `className` are the ONLY strings in a record that
     application code can influence at all. A sentence cannot occupy a
     single token.
   - `versionToken(_:)` — used for the OS/bundle-supplied fields —
     permits spaces but caps at four whitespace-separated tokens.

   Rejection DROPS the field; it never truncates (a truncated leak is
   still a leak).

   This layer is the SECOND line of defence and is documented as such;
   the primary defence is the key allowlist in (1), which means the
   free-text fields are never read in the first place.

   The sanitiser also runs on DECODE, not only on projection
   (`StabilityDiagnosticRecord.init(from:)`,
   `StabilityCallStackFrame.init(from:)`). The ring buffer outlives app
   versions, so the bytes the exporter reads were written by some
   earlier build; re-validating on the way in makes the export-time
   invariant unconditional rather than "true for whichever binary is
   running".
3. **Closed shape.** `StabilityDiagnosticRecord` has no
   `metadata: [String: String]`, no `rawPayload`, no `message`, no
   `reason`. Every field is a number, a boolean, an enum rawValue, or a
   sanitised string.
4. **No episode reference at all.** Unlike `scheduler_events` /
   `work_journal_tail`, a stability record carries not even the salted
   `episode_id_hash` — a stack trace has nothing to correlate to an
   episode, so the field does not exist.

**Verified by** (`PlayheadTests/Support/Diagnostics/StabilityDiagnosticScrubbingTests.swift`):
- `sentinelsAreScrubbed` — a payload seeded with transcript text, an
  episode title, a feed URL, and a container path (in
  `composedMessage`, `arguments`, `terminationReason`, and
  `virtualMemoryRegionInfo`) projects to records whose encoded bytes
  contain none of them, nor any distinctive fragment of them. The
  records are still CAPTURED — the test asserts a count, so scrubbing
  by dropping the diagnostic would not pass.
- `encodedStringsObeyAllowlist` — walks every string in the encoded
  record tree and asserts the allowlist + length cap.
- `locatorCharactersAreUnrepresentable` — asserts `/ : ? & @ % " ' < >
  { } [ ] | ^ ~ ; , ! \` are outside the allowlist, and that a URL, a
  path, and non-ASCII text are all rejected by the sanitiser.
- `encodedRecordShapeIsClosed` — `CodingKeys` is compared for EQUALITY
  against a frozen literal key set (comparing against the type's own
  keys would be circular: a new `reason: String` field with a CodingKey
  would satisfy it). Adding any field to the record is therefore a
  deliberate edit to a privacy-audited list, made here and in this
  checklist together. Also asserts none of the known-leaky MetricKit key
  names appear on a record or a frame.
- `DiagnosticTextSanitizerTests.identifierRejectsMultiWordText` /
  `versionTokenBounds` / `deviceModelRejectsNonASCII` /
  `terminationCodeExtraction` — the shape bounds and the ASCII
  qualification, each pinned against the repo's own sentinels.
- `StabilityDiagnosticsStoreTests.decodeReSanitisesLegacyRows` — a
  hand-written row of the shape a laxer earlier build could have left on
  disk loads (it is not dropped) but arrives with every unsafe field
  nil'd, and the sentinels are absent from the re-encoded bytes.
- `metadataAllowlistIsExhaustive` + `leakyKeysAreAbsentFromProjectorCode`
  — source canaries: the projector reads only its declared metadata
  keys, and the leaky key identifiers appear nowhere in its code.
- `PlayheadTests/Support/Diagnostics/DiagnosticsBundleStabilityDiagnosticsTests.swift`
  — `payloadTravelsToBundleScrubbed` runs the whole pipeline
  (payload → projector → store → coordinator → encoded bundle) and
  sweeps the sentinels against the ENTIRE encoded bundle, not just the
  stability subtree; `noEpisodeReference` asserts part 4.
- `PlayheadTests/E2E/Privacy/MetricKitDiagnosticsWiringSourceCanaryTests.swift`
  — pins the two links the simulator cannot execute (the app-delegate
  registration and the subscriber's forward), and asserts the subscriber
  reads no MetricKit property directly, so payload interpretation cannot
  migrate out of the tested projector.

**Egress.** None added. The store is local; records leave the device
only inside a diagnostics bundle the user actively chooses to send,
through the same mail path audited in (a)-(d). The parallel
(and complementary) route is Apple's own: users who leave "Share With
App Developers" on send crash + hang diagnostics to App Store Connect
directly, with no Playhead code involved.

**Symbolication.** `scripts/symbolicate-stability-diagnostics.sh` maps
`binary_uuid` → dSYM (`dwarfdump --uuid`) and resolves each
`offset_into_binary_text_segment` with `atos -l 0`. Records carry their
OWN `app_version` / `app_build_version` because MetricKit delivers up to
24 h late, often after the user has taken an update.

## (f) Delivery surfaces: the in-app feedback channel

Added by playhead-jw63.5. The bundle gains **no field** in this bead —
`DiagnosticsBundle.swift` is untouched, so (a)–(e) hold verbatim. What
changes is that the same bytes now have a **second** way off the device,
and that a **new prose surface** travels beside them.

**Two things this section exists to pin.**

1. **The bundle's reset policy follows the bytes, not the button.**
   `DiagnosticsExportCoordinator.applyOptInResetIfNeeded` used to be
   reachable only from `exportAndPresent()`. The feedback channel reuses
   `buildAndEncode()` for its optional attachment and presents through
   its own envelope, so without a second call site an opted-in episode's
   `diagnosticsOptIn` flag would survive a bundle that had already left
   the device. `applyOptInReset(for:)` is the named seam for that, and
   `ListenerFeedbackCoordinator` invokes it **only when a bundle actually
   shipped** — never for a note sent without one, and never for a build
   that failed. Verified by
   `ListenerFeedbackCoordinatorTests.resetObserverFiresOnlyWithAttachment`
   and `.resetObserverForwardsResult` (a `.cancelled` composer result
   reaches the policy, which PRESERVES the flag per (d)).

2. **The prefilled mail body is held to the bundle's bar.** It is
   composed by `ListenerFeedbackComposer.body(...)` from a closed set of
   inputs — app version, OS version, device class, an optional playback
   offset, and an optional **salted hash prefix**. The reference token is
   the first eight hex characters of the same
   `SHA-256(installID || episodeId)` the bundle emits as
   `episode_id_hash` (c), which is what lets a note correlate with a
   bundle without either naming the episode. No title, no transcript, no
   feed URL, no advertiser, no path.

   **Verified by** `PlayheadTests/Support/Feedback/ListenerFeedbackRedactionTests.swift`:
   - `momentNoteCarriesNoSentinels` — sweeps the whole envelope (subject,
     body, shareText, recipients, attachment filename) for a show title,
     transcript text, a feed URL, a container path, an advertiser, and a
     raw episode id. Non-vacuous: it also asserts the moment line and the
     reference ARE present.
   - `onlyReferenceIsEpisodeDerived` — the raw episode id is absent and
     the emitted token is a prefix of the (c) hash.
   - `bodyContainsNoLocators` — no `://`, `http`, `/var/`, `/Users/`, `@`.
   - `bodyCarriesOnlyKnownFacts` — the body is exactly three non-blank
     lines, enumerated. Adding an interpolation to the body is therefore
     a deliberate edit made against this list, the same way adding a
     field to a stability record is (e).

**Consent vs. content.** The checklist governs CONTENT. Whether the
bundle rides along is a separate, product-level decision: it is **opt-in
per send, default OFF** (`ListenerFeedbackDefaults.attachDiagnosticsDefault`),
so a "this felt wrong" note stays a note unless the listener says
otherwise. That is a stricter posture than the checklist requires.

**Egress.** None added. Both surfaces are user-initiated and terminate
in a composer or a share sheet the listener must confirm; nothing is
uploaded. The share-sheet fallback (used when Mail is not configured)
is deliberately MORE permissive than the diagnostics-export fallback —
Copy, Messages, and AirDrop stay available — because the failure mode
there is a listener with no way to reach us at all. The artifact routed
that way is the listener's own sentence plus, only if they opted in, the
same audited bundle.

---

## (g) Banner tallies: per-episode card counts, salted reference only

Added by playhead-bfq7. `DefaultBundle.banner_tallies` carries one row
per episode listening session that put at least one banner card on
screen. It exists so a listener can answer "how many cards did that
episode show me?" from an exported bundle rather than by counting cards
by hand during playback.

This is the first counter that is **both per-episode and on an egress
surface**, so it is the first one that could carry an episode reference
off the device. Two facts make it safe:

1. **The raw episode id stops at the builder.** `BannerTallyStore`
   deliberately persists the RAW id — it is local-only, and the sibling
   `os_log` breadcrumb names the episode plainly by design (see below).
   `DiagnosticsBundleBuilder.buildDefault` is the **only** projection of
   a `BannerTallySession`, and it replaces that id with
   `EpisodeIdHasher.hash(installID:episodeId:)` — the same salted hex
   (c) that the scheduler-event and work-journal tails already use, so a
   tally correlates with those tails without either naming the episode.

2. **The row is a closed shape.** `DefaultBundle.BannerTallySummary` is
   `episode_id_hash`, `banner_count`, `auto_skipped_count`,
   `suggest_count`, `first_shown_at`, `last_shown_at`. Nothing else.
   Not carried: the episode title, the feed URL, the advertiser or
   product printed on the card, the orchestrator window ids, the local
   session key, or any transcript text.

**The `os_log` surface is NOT governed by this checklist.** The live
breadcrumb (`subsystem:com.playhead category:BannerTally`) names the
episode and the window id in cleartext. That is deliberate and stays
local: it is written to the device log, never read back by the app, and
never attached to an export. When in doubt the rule is "less in the
bundle, more in the log", and that is the split taken here.

**Verified by** `PlayheadTests/Support/Diagnostics/BannerTallyDiagnosticsPrivacyTests.swift`:

- `sentinelsAreHashedAway` — a store row whose episode id is stuffed
  with an episode title, a feed URL and transcript text projects into a
  bundle whose encoded bytes contain none of them, nor any fragment of
  them, nor the advertiser/product/window fields of the cards that
  produced it, nor the local session key. Non-vacuous: it also asserts
  the hostile row was still COUNTED.
- `episodeReferenceIsTheSaltedHash` — the reference that does ship is
  byte-equal to `EpisodeIdHasher.hash(installID:episodeId:)` and matches
  `^[0-9a-f]{64}$`.
- `encodedSummaryShapeIsClosed` — the encoded key set equals a FROZEN
  literal list (not `CodingKeys.allCases`, which would be circular).
  Adding any field to the summary is therefore a deliberate edit made
  against this checklist, the same way adding a field to a stability
  record is (e).
- `tallyReachesTheBundleWithItsTierBreakdown` — the number actually
  survives store → fetch → builder → encoded bytes, and two distinct
  episodes do not collapse onto one reference.
- `keyIsPresentWhenEmpty` / `legacyBundlesStayDecodable` — the key is
  always encoded, and a bundle predating it decodes as `[]`.

**Egress.** None added. The tally rides the bundle the listener already
chooses to send through (f)'s feedback channel or the diagnostics
export; nothing is uploaded.
