# Bundled specialist model — `qwen3_0_6b_4bit_dynamic_ft_v2`

This directory holds the on-device ad-detection **specialist model** that
Playhead bundles into the app. The model weights (~321 MB) are deliberately
kept **out of git** — plain git-history bloat compounds across retrains — so
they are staged from a canonical export by a script rather than committed.

## Fresh checkout: stage the weights first

```bash
scripts/fetch-specialist-model.sh
```

This copies the exported model into
`Playhead/Resources/Models/qwen3_0_6b_4bit_dynamic_ft_v2/`, which
`project.yml` bundles as a **folder reference** so the app can load it at:

```swift
Bundle.main.resourceURL!.appending(path: "qwen3_0_6b_4bit_dynamic_ft_v2")
```

The script is idempotent (it no-ops when the staged copy's
`.aimodel/main.mlirb` SHA-256 already matches the source) and prints the
size + checksum for provenance.

## Build fails loud without the weights

The `Playhead` target runs a pre-build check (`Verify specialist model
staged` in `project.yml`). If the weights are missing, the build stops with
an error pointing back at `scripts/fetch-specialist-model.sh` instead of a
cryptic missing-resource failure.

## What lives here

| Path (git) | Purpose |
| --- | --- |
| `README.md` | **committed** — this file |
| `qwen3_0_6b_4bit_dynamic_ft_v2/` | **gitignored** — staged weights + tokenizer + metadata |

The staged model folder contains:

```
qwen3_0_6b_4bit_dynamic_ft_v2/
├── metadata.json
├── qwen3_0_6b_4bit_dynamic_ft_v2.aimodel/   # main.mlirb (~321 MB), main.hash, metadata.json
└── tokenizer/                               # tokenizer.json, vocab.json, merges.txt, ...
```

## Licensing

The model is a fine-tuned + 4-bit-quantized derivative of **Qwen3-0.6B**
(Apache-2.0, "Built with Qwen"). See `THIRD_PARTY_NOTICES.md` at the repo
root and the bundled `Playhead/Resources/NOTICE.txt` for full attribution of the
model and the runtime it loads on (coreai-models / swift-transformers /
xgrammar).

_playhead-b6jq PR2 — packaging only; no inference code loads this yet._
