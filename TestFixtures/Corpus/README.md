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
├── Transcripts/           ← local ASR transcript JSON, ignored by git
├── Drafts/                ← generated draft annotations, ignored by git
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

0. **Optional bootstrap.** Generate a local transcript and draft
   annotation:
   ```sh
   swift scripts/l2f-local-transcribe.swift --model models/ggml-base.en.bin
   swift scripts/l2f-draft-annotation.swift --write-review-queue
   ```
   Drafts are written to `TestFixtures/Corpus/Drafts/` and are not
   ground truth. The draft generator clusters transcript cue hits into
   pod-scale review candidates; tune with `--merge-gap-seconds`,
   `--expand-before-seconds`, `--expand-after-seconds`,
   `--padding-seconds`, and `--max-window-seconds`.
   Promote a draft only after human audio review.
   To build a review queue from Codex's transcript-only review for the
   current 15 local episodes, run:
   ```sh
   swift scripts/l2f-draft-annotation.swift \
     --review-queue-only \
     --review-source TestFixtures/Corpus/Drafts/codex-transcript-review.json
   ```
   The queue writes ignored `review-queue.json` and `review-queue.md`
   artifacts under `Drafts/`, with one checklist item per candidate pod
   and explicit false-positive-trap entries for zero-ad episodes.
   For a touch-friendly review pass from another device on the same
   network, run the local GUI server:
   ```sh
   python3 scripts/l2f-review-gui.py --host 0.0.0.0
   ```
   Open the printed LAN URL on the iPhone. The GUI saves decisions under
   ignored `Drafts/` artifacts only.
   Before the listening pass is complete, check review debt and category
   coverage without writing annotations:
   ```sh
   python3 scripts/l2f-promote-reviewed-corpus.py
   ```
   If the GUI review file does not exist yet, point report mode at the queue:
   ```sh
   python3 scripts/l2f-promote-reviewed-corpus.py \
     --queue TestFixtures/Corpus/Drafts/codex-review-queue.json
   ```
1. **Listen end to end or review the draft against audio.** Note ad
   start/end times to ±0.5 s precision.
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
4. **Promote after review.** Once every selected entry has a final GUI
   decision, run:
   ```sh
   python3 scripts/l2f-promote-reviewed-corpus.py --promote
   ```
   The promoter computes `audio_fingerprint` as `sha256:<hex>` from the local
   audio bytes, probes the episode duration, validates reviewed ad metadata
   and timing, and writes explicit content windows as the complement of the
   verified ad windows. It refuses real promotion if any selected entry is
   unreviewed, marked `unsure`, missing audio or duration, missing required ad
   metadata, or has invalid/overlapping timing. Use
   `--episode <episode_id>` for a reviewed subset and `--force` only when
   intentionally replacing an existing annotation.
5. **Save local audio** at `Audio/<episode_id>.<ext>` so the recorded
   `audio_fingerprint` can be verified.
6. **Variants.** When labeling a DAI variant, set `variant_of` to
   the parent `episode_id`. The variant gets its own JSON and audio
   file.
7. **Run the validator.** Either via Xcode (run `CorpusAnnotationLoaderDiskTests`)
   or by calling `CorpusAnnotationLoader().loadAll(verifyAudioFingerprints: true)`.
8. **Second-pass review.** A second listener spot-checks every
   annotation before the corpus is treated as ground truth.

`playhead-l2f.3` and `playhead-l2f.4` are intentionally manual gates.
Transcript-derived drafts, Codex transcript review, and generated review
queues can prioritize listening work, but they must not be promoted into
`Annotations/` until a human has checked the local audio boundaries and
false-positive traps.

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
- **Transcripts** (`Transcripts/*.json`) — NOT committed. Generated
  locally from audio and may contain copyrighted transcript text.
- **Drafts** (`Drafts/*`) — NOT committed. Generated hints for a
  human labeler, never ground truth by themselves.

The promotion tool follows the same storage policy: it only writes committed
annotation JSON. GUI reviews, review queues, transcripts, and audio remain
ignored local artifacts.

See `.gitignore` for the exact audio exclusion rules.
