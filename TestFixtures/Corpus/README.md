# Labeled Test Corpus (playhead-l2f)

Ground-truth annotations of real podcast episodes for evaluating
Playhead's ad-detection pipeline.

This directory holds the **scaffolding** introduced by bead
`playhead-l2f`. The corpus itself — the human-annotated audio plus
JSON labels — is filled in over a separate, manual labeling pass.

## Layout

```
TestFixtures/Corpus/
├── README.md              ← this file
├── Audio/                 ← episode audio files (.m4a, .mp3, …)
│                            named `<episode_id>.<ext>`
└── Annotations/           ← per-episode JSON annotations
    ├── _template.example.json
    ├── _template.example.md     ← field-by-field reference
    └── <episode_id>.json        ← real annotations go here
```

Filenames in `Annotations/` that begin with `_` or end with
`.example.json` are treated as templates by
`CorpusAnnotationLoader.isTemplate(_:)` and skipped during validation.

## Categories (target ≥ 15 episodes)

| Category | Target | Description |
| --- | --- | --- |
| 1 — Clear host-read ads | 5 | Host reads sponsor copy in their own voice with explicit "and now a word from our sponsor" transitions. Five different shows across genres. |
| 2 — Dynamic insertion ads | 3 | Pre-roll / mid-roll inserted by ad tech. Music beds, different production quality. **Include 2 variants of the same episode** (different ad fills) for variant-pair testing. |
| 3 — Blended / ambiguous ads | 3 | Host weaves the sponsor mention into content naturally. No clear transition markers. These tests fail-open behaviour. |
| 4 — Edge cases | 4 | Zero ads (false-positive trap), 5+ ads (high density), very short ad (< 15 s promo mention), back-to-back ads with no content between sponsors. |

## Annotation process

For every episode:

1. **Listen end to end.** Note ad start/end times to ±0.5 s precision.
2. **Mark ad windows.** For each, record advertiser, product, ad
   type, and transition type. Add free-form `confidence_notes`
   explaining why this confidence level was assigned (e.g. "Clear
   brought-to-you-by intro" or "Host blends the sponsor mention into
   the cold open — no musical cue").
3. **Mark content windows.** Together with `ad_windows`, the content
   windows must partition the timeline `[0, duration_seconds]` with
   no gaps and no overlaps. The validator enforces this.
   **When a content window and an ad window meet, set their shared
   boundary to the SAME numeric value** (e.g. content ends at
   `180.0` and ad starts at `180.0`). The validator allows ~50 ms of
   floating-point slack but treats anything larger as a real
   gap/overlap that needs annotator attention.
4. **Compute the audio fingerprint.** Run the helper:
   ```swift
   let fp = try CorpusAudioFingerprint.fingerprint(of: audioURL)
   // → "sha256:<hex>"
   ```
   Paste it into the JSON's `audio_fingerprint` field.
5. **Save the JSON** to `Annotations/<episode_id>.json` and the
   audio to `Audio/<episode_id>.<ext>`.
6. **Variants.** When labeling a DAI variant, set `variant_of` to
   the parent `episode_id`. The variant gets its own JSON and audio
   file.
7. **Run the validator.** Either via Xcode (run `CorpusAnnotationLoaderDiskTests`)
   or by calling `CorpusAnnotationLoader().loadAll(verifyAudioFingerprints: true)`.
8. **Second-pass review.** A second listener spot-checks every
   annotation before the corpus is treated as ground truth.

## Worked example

Imagine a 10-minute episode of "Example Pod" with one mid-roll
host-read ad from 3:00 to 4:00.

`Audio/corpus-001.m4a` — the episode audio.

`Annotations/corpus-001.json`:

```json
{
  "episode_id": "corpus-001",
  "show_name": "Example Pod",
  "duration_seconds": 600,
  "ad_windows": [
    {
      "start_seconds": 180.0,
      "end_seconds": 240.0,
      "advertiser": "Squarespace",
      "product": "Website builder",
      "ad_type": "host_read",
      "transition_type": "explicit",
      "confidence_notes": "Clear 'brought to you by Squarespace' intro at 3:00, host returns to topic at 4:00"
    }
  ],
  "content_windows": [
    {
      "start_seconds": 0.0,
      "end_seconds": 180.0,
      "notes": "Cold open + main interview opening — must NEVER be skipped"
    },
    {
      "start_seconds": 240.0,
      "end_seconds": 600.0,
      "notes": "Rest of interview through outro"
    }
  ],
  "variant_of": null,
  "audio_fingerprint": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
}
```

The two content windows plus the one ad window cover `[0, 600]`
exactly: `[0, 180] ∪ [180, 240] ∪ [240, 600]` with no gaps and no
overlaps. The validator confirms this on every load.

## Acceptance criteria for closing playhead-l2f

These criteria come from the bead description and are enforced by
the validator + a second human listener (not by code alone):

- Minimum 15 episodes spread across all 4 categories.
- Every annotation verified by at least one human listener.
- Content windows explicitly marked (false-positive traps).
- At least one variant pair for dynamic-ad-insertion testing.
- Annotations at ±0.5 s precision.
- The replay simulator consumes the corpus without errors.

## Why this isn't bundled into the test target

Audio files for 15 episodes total ~2–3 GB. Bundling them into
`PlayheadTests` would explode the test bundle's footprint and make
CI checkout slow. Instead, the corpus lives at the repo root in
`TestFixtures/Corpus/`, and `CorpusAnnotationLoader` resolves it
via `#filePath` rather than `Bundle.url(forResource:)`. Audio
files are gitignored at the repository level — only the JSON
annotations and this README are committed.

## Storage policy

- **Annotations** (`*.json`) — committed. Small, text-diffable.
- **Audio** (`.m4a`, `.mp3`, etc.) — NOT committed. Tracked
  manually or via a separate large-file mechanism. Ensure your
  copy of the audio matches the recorded `audio_fingerprint`
  before relying on a label.

See `.gitignore` for the exact audio exclusion rules.
