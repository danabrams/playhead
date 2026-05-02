# Annotation Template — Field Reference

Use `_template.example.json` as a starting point. Copy it to
`<episode_id>.json`, replace placeholders, and run the validator.

Filenames matching `_*` or `*.example.json` are skipped by
`CorpusAnnotationLoader.loadAll(...)`, so the template never
participates in real corpus runs.

## Top-level fields

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `episode_id` | string | yes | Stable identifier, e.g. `corpus-001`. Must match the audio filename stem in `Audio/`. |
| `show_name` | string | yes | Human-readable show title (e.g. "Diary of a CEO"). |
| `duration_seconds` | number | yes | Full episode length in seconds. |
| `ad_windows` | array | yes | Every labeled ad region (may be empty for zero-ad episodes). |
| `content_windows` | array | yes | Every labeled content region. Must partition `[0, duration_seconds]` together with `ad_windows`. |
| `variant_of` | string \| null | yes | If this is a DAI variant, the parent `episode_id`. `null` for primary recordings. Must NOT equal this annotation's own `episode_id`. |
| `audio_fingerprint` | string | yes | `sha256:<lowercase-hex>` of the audio file bytes. Compute via `CorpusAudioFingerprint.fingerprint(of:)`. |

## `ad_windows[i]` fields

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `start_seconds` | number | yes | Window start, ±0.5 s precision. |
| `end_seconds` | number | yes | Window end, ±0.5 s precision. Must be `> start_seconds`. |
| `advertiser` | string \| null | yes | e.g. "Squarespace". `null` allowed when an ad has no clear advertiser brand (rare). |
| `product` | string \| null | yes | e.g. "Website builder". `null` allowed when not specified. |
| `ad_type` | enum | yes | One of: `host_read`, `dynamic_insertion`, `blended_host_read`, `produced_segment`, `promo`. |
| `transition_type` | enum | yes | One of: `explicit`, `musical`, `hard_cut`, `blended`. |
| `confidence_notes` | string \| null | yes | Free-form annotator notes. Use `null` only if no extra context is needed. |

## `content_windows[i]` fields

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `start_seconds` | number | yes | |
| `end_seconds` | number | yes | Must be `> start_seconds`. |
| `notes` | string \| null | yes | Annotator notes (e.g. "false-positive trap — host reads sponsor name in passing"). |

## Partition rule

Every second of `[0, duration_seconds]` must fall in exactly one of
the windows in `ad_windows ∪ content_windows`. The validator rejects:

- Overlapping ad windows.
- Overlapping content windows.
- An ad window overlapping a content window.
- A gap where neither array covers some part of the timeline.
- Coverage that overshoots `duration_seconds`.

A 0.05 s slack (`CorpusAnnotationLoader.epsilon`) absorbs floating
point round-off so adjacent windows can share a boundary value.

## Enum value tables

`ad_type`:
- `host_read` — Host reads sponsor copy in their own voice.
- `dynamic_insertion` — Pre-produced ad inserted by ad tech.
- `blended_host_read` — Host weaves sponsor mention into content.
- `produced_segment` — Pre-produced jingle/segment with music.
- `promo` — Cross-promo for another show or related product.

`transition_type`:
- `explicit` — Spoken cue ("and now a word from our sponsor…").
- `musical` — Music sting / stinger / bumper, no spoken cue.
- `hard_cut` — Direct audio cut with no signpost at all.
- `blended` — Sponsor mention woven into the host's existing
  thought; the listener may not realise an ad has begun until
  later.
