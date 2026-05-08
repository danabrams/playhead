# ChapterSignal case-study fixtures (playhead-au2v.1.20)

This directory ships a small, anonymized case-study corpus that the
chapter-signal eval harness (`ChapterSignalGate`, bead 18) replays in two
modes — `.off` and `.enabled` — to demonstrate the documented before/after
behaviour on shapes that surface most often in the dogfood diagnostics
corpus.

The fixtures themselves are intentionally tiny structural shells: every
case carries enough state for the gate to run (episode/podcast id,
duration, atom count + spacing, creator-chapter flag, optional
boundary-detector stub count) and the documented before/after counters,
plus a plain-English `expected_behavior` description and an
`synthesis_notes` field describing the corpus signal each case was modeled
on.

The Swift loader and per-case tests live at:
- `PlayheadTests/Services/ReplaySimulator/NarlEval/ChapterSignalCaseStudyTests.swift`

## What ships here

| File | What it is |
| --- | --- |
| `case-01-conversational-miss-medium.json` | conversational_miss — medium-length conversational episode, no creator chapters |
| `case-02-conversational-miss-long.json` | conversational_miss — long-form, default stub saturates upper clamp |
| `case-03-conversational-miss-dense-mid.json` | conversational_miss — mid-length, default-stub mid-band |
| `case-04-false-positive-removal-no-candidates.json` | false_positive_removal — boundary detector finds no candidates, gate must not fabricate a plan |
| `case-05-false-positive-removal-creator-chapters.json` | false_positive_removal — creator chapters present, FM-inferred plan must NOT override creator labels |
| `case-06-pre-post-roll-edge.json` | pre_post_roll_edge — short episode, exercises the lower clamp at 1 |
| `case-07-monologue-short-episode.json` | monologue_short_edge — short comedy monologue, low atom density |
| `case-08-sanity-signal-does-nothing.json` | sanity_signal_inert — gate runs but produces no consumer-visible effect |
| `README.md` | this file |

Total: 8 cases, covering the five required categories from the bead spec
(≥3 conversational misses, ≥2 false-positive removals, ≥1 pre/post-roll
edge, ≥1 monologue/short-episode edge, ≥1 sanity case where the signal
correctly does nothing).

## Fixture schema (v1)

Each case JSON is a flat object with the following shape:

```json
{
  "schema_version": 1,
  "case_id": "case-NN-…",
  "archetype": "conversational" | "narrative" | "comedy" | "news",
  "category": "conversational_miss" | "false_positive_removal"
              | "pre_post_roll_edge" | "monologue_short_edge"
              | "sanity_signal_inert",
  "expected_behavior": "<plain English description of pre/post-au2v.1 behaviour>",
  "synthesis_notes": "<provenance / what corpus shape this models>",
  "trace": {
    "episode_id_anon": "episode_anon_<short hash>",
    "podcast_id_archetype": "show_archetype_<archetype>",
    "episode_duration_sec": <Double>,
    "atom_count": <Int>,
    "atom_spacing_sec": <Double>
  },
  "gate_inputs": {
    "creator_chapters_present": <Bool>,
    "stub_chapter_count": <Int> | null
  },
  "expected_before_off": {
    "plan_generated_count": <Int>,
    "skipped_by_creator_chapters": <Int>,
    "total_fm_calls_for_chapter_labeling": <Int>,
    "aggregate_latency_ms": <Double>
  },
  "expected_after_enabled": {
    "plan_generated_count": <Int>,
    "skipped_by_creator_chapters": <Int>,
    "total_fm_calls_for_chapter_labeling": <Int>,
    "aggregate_latency_ms": <Double>
  }
}
```

`stub_chapter_count: null` means "use the default
`Config.defaultStubChapterCount`" — i.e. `clamp(atoms/50, 1, 12)`. A
non-null integer overrides the default. Fixture-time integer literals
must not be negative; the gate clamps negatives to 0 internally, but
keeping the wire shape non-negative makes the fixture self-documenting.

## Anonymization process

The dogfood diagnostics corpus this case set is modeled on contains
real-listener data — episode titles, advertiser names, transcript
snippets, listener-identifying timestamps. **None of that data is
checked in here.** The anonymization pipeline that produced each fixture
is:

1. **Episode titles → `episode_id_anon` synthetic id.** Each case has a
   short opaque id of the shape `episode_anon_<six-character hex>` chosen
   so it cannot be reversed to a podcast feed URL or RSS GUID.
2. **Podcast / show titles → `podcast_id_archetype` slot.** The fixture
   only retains the archetype label (one of `conversational`,
   `narrative`, `comedy`, `news`) — never the show name, RSS URL, or
   podcast id. This is enough for the gate's behaviour pinning (the gate
   doesn't read the podcast id anyway, beyond passing it through to the
   `EpisodeOutcome.podcastId` field) but carries no identifying signal.
3. **Advertiser names → ENTIRELY OMITTED.** The fixture has no field for
   advertiser names. If the source diagnostic row referenced an
   advertiser, the reference is dropped at fixture-construction time.
4. **Transcript text → ENTIRELY OMITTED.** Atoms are described by their
   `count` and `spacing_sec` only. The Swift loader synthesises atom
   text as the empty string at runtime — the fixture file itself never
   carries any natural-language transcript content. The gate does not
   read atom text; the count alone is sufficient for the default
   stub-count formula and for the gate's behavioural assertions.
5. **User-identifying timestamps → DROPPED.** No `created_at`,
   `captured_at`, or `started_at` wall-clock timestamps appear anywhere
   in the fixtures. The Swift loader stamps `capturedAt` to a fixed
   reference epoch (`Date(timeIntervalSince1970: 1_700_000_000)`) so
   replay outputs stay byte-for-byte deterministic across runs.
6. **Listener / install / device identifiers → DROPPED.** No
   `episode_id_hash` SHA-256s, no install UUIDs, no
   `BuildProvenance.plist` SHAs, no device-container paths.
7. **Numeric scores and durations are preserved** because they're
   load-bearing for the gate's behavioural assertions and carry no
   identifying signal on their own.

### Anonymization checklist (for new cases)

When adding a case, verify each item before commit:

- [ ] No episode title — `episode_id_anon` is a synthetic short hash.
- [ ] No podcast / show title — `podcast_id_archetype` is one of the
      four allowed archetypes.
- [ ] No advertiser names — the fixture must have no advertiser-bearing
      field, anywhere.
- [ ] No transcript text — atoms are described by count and spacing
      only; text is synthesised at runtime as empty strings.
- [ ] No wall-clock timestamps — neither in the fixture nor referenced
      by the loader (the loader uses a frozen reference date).
- [ ] No listener / install / device identifiers.
- [ ] `expected_behavior` and `synthesis_notes` are plain-English and
      do NOT quote any transcript snippet, advertiser slogan, or episode
      title.
- [ ] The case is added to the table in this README and to the case-id
      enum in `ChapterSignalCaseStudyTests.swift` so the integrity test
      sees it.

## Integrity invariants asserted by tests

`ChapterSignalCaseStudyTests` runs a sweep of structural assertions
over the directory in addition to the per-case before/after assertions:

- Every case JSON in this directory decodes cleanly under the v1 schema.
- Every case-id is unique and matches its filename stem.
- The total case count is in the bead-spec range `[5, 10]`.
- The case set covers ≥3 conversational misses, ≥2 false-positive
  removals, ≥1 pre/post-roll edge, ≥1 monologue/short-episode edge, and
  ≥1 sanity case (the bead-au2v.1.20 selection contract).
- For every case, `ChapterSignalGate.replay(trace:mode:)` produces the
  documented `expected_before_off` counters under `mode=.off` and the
  documented `expected_after_enabled` counters under `mode=.enabled`.
- For every case, the `.shadow` mode produces the same per-episode
  counter shape as `.enabled` (consumer-side divergence lives in
  `ChapterSignalMode.consumersReadChapterPlan`, not in gate output).
- For every case, replaying the same trace twice yields Equatable-equal
  results (replay determinism guard).
- For every case, the documented `expected_before_off` and
  `expected_after_enabled` counters differ on at least one of the four
  fields — i.e., every case exercises observable gate lift.
- For every case, `expected_behavior` and `synthesis_notes` are
  non-empty after trimming whitespace.
- For every case, all expected counters are non-negative; `.off`
  counters are structural zeros; `gate_inputs.stub_chapter_count`,
  `trace.atom_count`, `trace.atom_spacing_sec`, and
  `trace.episode_duration_sec` are non-negative.
- For every case, `episode_id_anon` matches the synthetic shape
  `^episode_anon_<4-16 hex>$` and `podcast_id_archetype` equals
  `show_archetype_<archetype>` (regex / equality pin on the
  anonymization §1 / §2 wire shapes).
- No case JSON contains the substring "advertiser", any 32-hex-character
  identifier shape, or other forbidden tokens — a belt-and-suspenders
  scrub-audit gate that runs against the raw bytes of every case JSON.
- README.md is exempt from concept-word forbidden tokens (the README
  legitimately uses words like "advertiser" / "sponsor" while
  *describing* anonymization), but is audited separately for
  show-specific tokens (the same set the case-JSON scrub uses, minus
  the concept words). The exact token list lives in the test source so
  it can't be smuggled into this documentation surface.
- The parameterized per-case suite is non-vacuous: the loader yields at
  least one case so the case-by-case assertions actually run.
- README.md is present in this directory.

## Why model fixtures rather than copy raw traces?

The `FrozenTrace` schema (v3) carries far more state than the gate
needs (feature windows, evidence catalog entries, decision events,
listen-rewind events, capture provenance). A raw trace from the
diagnostics corpus would either embed identifying signal we'd have to
scrub field-by-field, or it would carry empty arrays for every field
the gate doesn't read — the same shape these tiny case-study fixtures
already capture, but a couple kilobytes larger and harder to audit.

The dogfood diagnostics corpus snapshots checked in elsewhere
(`PlayheadTests/Fixtures/Dogfood/2026-05-06/`) are a richer substrate
for tests that exercise the analysis-health surface end-to-end. The
case-study fixtures here are deliberately narrow: each is a precisely
shaped behavioural pin for the chapter-signal gate, not a full
re-creation of a listener's session.
