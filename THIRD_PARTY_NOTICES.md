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
| **swift-jinja** (Hugging Face) — Jinja | Prompt/chat-template rendering (transitive via swift-transformers Hub/Tokenizers) | Apache-2.0 | © the swift-jinja authors (Hugging Face, Inc.) |
| **swift-huggingface** (Hugging Face) — HuggingFace | Hub types / model-repo configuration used by the tokenizer stack (transitive) | Apache-2.0 | © the swift-huggingface authors (Hugging Face, Inc.) |
| **swift-nio** (Apple) — NIOCore | Linked transitively (EventSource → NIOCore); async primitives | Apache-2.0 | © Apple Inc. and the SwiftNIO project authors |
| **swift-crypto** (Apple) — Crypto | Hashing used by the Hub/tokenizer stack (transitive) | Apache-2.0 | © Apple Inc. and the SwiftCrypto project authors |
| **swift-collections** (Apple) — OrderedCollections, DequeModule | Ordered/deque containers (transitive) | Apache-2.0 w/ Runtime Library Exception | © Apple Inc. and the Swift project authors |
| **swift-atomics** (Apple) — Atomics | Atomic primitives (transitive via SwiftNIO) | Apache-2.0 w/ Runtime Library Exception | © Apple Inc. and the Swift project authors |
| **EventSource** (Mattt) | Server-Sent-Events client linked by swift-huggingface (transitive) | MIT | © 2025 Mattt |
| **yyjson** (YaoYuan) | JSON parsing used by the Hub stack (transitive) | MIT | © 2020 YaoYuan |

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

## 5. swift-jinja (Hugging Face) — Apache-2.0

Linked transitively via `swift-transformers` (the `Tokenizers`/`Hub` targets depend on the
`Jinja` product) for chat-/prompt-template rendering.

- Upstream: <https://github.com/huggingface/swift-jinja> (v2.4.2)
- Copyright © the swift-jinja authors (Hugging Face, Inc.)
- License: Apache-2.0 — <https://www.apache.org/licenses/LICENSE-2.0>

## 6. swift-huggingface (Hugging Face) — Apache-2.0

Linked transitively via `swift-transformers` (the `Hub` target depends on the `HuggingFace`
product) for hub/model-repo types used while configuring the on-device tokenizer. The Xet
transport trait is disabled, so `swift-xet` is neither resolved nor linked.

- Upstream: <https://github.com/huggingface/swift-huggingface> (v0.9.0)
- Copyright © the swift-huggingface authors (Hugging Face, Inc.)
- License: Apache-2.0 — <https://www.apache.org/licenses/LICENSE-2.0>

## 7. swift-nio (Apple) — Apache-2.0

Linked transitively via `swift-huggingface` → `EventSource` → `NIOCore`. The `NIOCore`,
`NIOConcurrencyHelpers`, and `_NIOBase64` targets are present in the shipped binary; the
`NIOPosix`/networking targets are not linked. Apache-2.0 §4(d) requires reproducing the
project's NOTICE — see `Playhead/Resources/NOTICE.txt`.

- Upstream: <https://github.com/apple/swift-nio> (v2.101.3)
- Copyright © Apple Inc. and the SwiftNIO project authors (2017–2018)
- License: Apache-2.0 — <https://www.apache.org/licenses/LICENSE-2.0>

## 8. swift-crypto (Apple) — Apache-2.0

Linked transitively via `swift-transformers`/`swift-huggingface` (the `Hub` and `HuggingFace`
targets depend on the `Crypto` product) for hashing. On Apple platforms swift-crypto's `Crypto`
module shims over the system CryptoKit; the BoringSSL/ASN.1 targets are not linked. Apache-2.0
§4(d) requires reproducing the project's NOTICE — see `Playhead/Resources/NOTICE.txt`.

- Upstream: <https://github.com/apple/swift-crypto> (v4.5.1)
- Copyright © Apple Inc. and the SwiftCrypto project authors (2019–2023)
- License: Apache-2.0 — <https://www.apache.org/licenses/LICENSE-2.0>

## 9. swift-collections (Apple) — Apache-2.0 with Runtime Library Exception

Linked transitively (`OrderedCollections` via swift-jinja/Hub; `DequeModule` via SwiftNIO).

- Upstream: <https://github.com/apple/swift-collections> (v1.6.0)
- Copyright © Apple Inc. and the Swift project authors
- License: Apache-2.0 with the Swift Runtime Library Exception —
  <https://www.apache.org/licenses/LICENSE-2.0> and
  <https://github.com/apple/swift-collections/blob/main/LICENSE.txt>

## 10. swift-atomics (Apple) — Apache-2.0 with Runtime Library Exception

Linked transitively via SwiftNIO (`NIOConcurrencyHelpers` → `Atomics`).

- Upstream: <https://github.com/apple/swift-atomics> (v1.3.1)
- Copyright © Apple Inc. and the Swift project authors
- License: Apache-2.0 with the Swift Runtime Library Exception —
  <https://www.apache.org/licenses/LICENSE-2.0> and
  <https://github.com/apple/swift-atomics/blob/main/LICENSE.txt>

## 11. EventSource (Mattt) — MIT

Linked transitively via `swift-huggingface` (the `HuggingFace` target depends on the
`EventSource` product). MIT requires the copyright and permission notice be reproduced —
see `Playhead/Resources/NOTICE.txt`.

- Upstream: <https://github.com/mattt/EventSource> (v1.4.1)
- Copyright 2025 Mattt (https://mat.tt)
- License: MIT

## 12. yyjson (YaoYuan) — MIT

Linked transitively via `swift-transformers` (the `Hub` target depends on the `yyjson`
product) for JSON parsing. MIT requires the copyright and permission notice be reproduced —
see `Playhead/Resources/NOTICE.txt`.

- Upstream: <https://github.com/ibireme/yyjson> (v0.12.0)
- Copyright © 2020 YaoYuan <ibireme@gmail.com>
- License: MIT
