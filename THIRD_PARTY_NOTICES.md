# Third-Party Notices

Playhead bundles and links third-party software and a bundled machine-learning
model. This file summarizes the components, their licenses, and the required
attributions. The same notice is bundled into the app at
`Playhead/Resources/NOTICE.txt`.

| Component | Use | License | Attribution |
| --- | --- | --- | --- |
| **Qwen3-0.6B** (fine-tuned + 4-bit quantized) | Bundled on-device ad-detection specialist model | Apache-2.0 | "Built with Qwen" |
| **coreai-models** (Apple) — CoreAILM | Loads/runs the bundled model on device | BSD-3-Clause | © 2026 Apple Inc. |
| **swift-transformers** (Hugging Face) | Tokenizer / generation support (transitive) | Apache-2.0 | © the swift-transformers authors |
| **xgrammar** (MLC AI) | Grammar-constrained decoding (transitive) | Apache-2.0 | © the XGrammar authors |

## 1. Qwen3-0.6B — bundled specialist model (Apache-2.0)

Playhead bundles an on-device ad-detection model that is a **derivative** of
Qwen3-0.6B: supervised fine-tuning on Playhead's own ad-detection data plus
4-bit dynamic weight quantization, compiled to Apple's Core AI (`.aimodel`)
format. The bundled weights are a derivative work.

- **Built with Qwen.** Qwen3 is developed by Alibaba Cloud / the Qwen team.
- Modifications from upstream: fine-tune + 4-bit quantization.
- Upstream: <https://huggingface.co/Qwen/Qwen3-0.6B>
- License: Apache-2.0 — <https://www.apache.org/licenses/LICENSE-2.0>

## 2. coreai-models (Apple) — CoreAILM specialist runtime (BSD-3-Clause)

Playhead vendors a trimmed fork of Apple's `coreai-models` (the `CoreAILM`
product) to load and run the bundled model. BSD-3-Clause requires reproducing
the copyright notice in binary redistribution — see the reproduced notice in
`Playhead/Resources/NOTICE.txt` and the full text at
[`Vendor/coreai-models/LICENSE`](Vendor/coreai-models/LICENSE).

- Copyright 2026 Apple Inc.
- License: BSD-3-Clause

## 3. swift-transformers (Hugging Face) — Apache-2.0

Linked transitively via `coreai-models` (the `Transformers` product) for
tokenizer loading and text-generation support.

- Upstream: <https://github.com/huggingface/swift-transformers> (v1.2.0)
- License: Apache-2.0 — <https://www.apache.org/licenses/LICENSE-2.0>

## 4. xgrammar (MLC AI) — Apache-2.0

Linked transitively via `coreai-models` (the `XGrammar` product) for
structured / JSON-schema-constrained generation.

- Upstream: <https://github.com/mlc-ai/xgrammar>
  (rev `4d145cc13d878c751ebeed36af1c013074be76bc`)
- License: Apache-2.0 — <https://www.apache.org/licenses/LICENSE-2.0>
