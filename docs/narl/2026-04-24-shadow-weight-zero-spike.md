# Shadow `allEnabledShadow` weight=0 spike — 2026-04-24 (gtt9.4.2)

## Question

In FrozenTrace 71F0C2AE (Conan, 117 min), 14 `shadow:allEnabledShadow` evidence entries across [6900, 7050] — the region around the 30 s GT ad span [7007.34, 7037.34] — all carry `weight=0`. Three hypotheses:

1. Shadow classifier truly disagreed (actual weakness signal).
2. `ShadowDecisionsExporter` doesn't export confidence into `weight` (exporter bug).
3. Exporter writes weight=0 by design for non-positive shadow decisions, corpus builder folds them in regardless (silent false-disagreement).

## What I inspected

- `/Users/dabrams/playhead/Playhead/Services/AdDetection/ShadowDecisionsExporter.swift` — the JSONL exporter.
- `/Users/dabrams/playhead/Playhead/Services/AdDetection/ShadowFMResponse.swift` — the row schema (JSONL line shape).
- `/Users/dabrams/playhead/Playhead/Services/AdDetection/FoundationModelClassifierShadowDispatcher.swift` — `ShadowFMPayload` (blob contents).
- `/Users/dabrams/playhead/PlayheadTests/Services/ReplaySimulator/NarlEval/NarlEvalCorpusBuilderTests.swift` — `parseShadowLog` (line 327-353) and `assembleTrace` shadow-fold (line 538-548).
- `/Users/dabrams/playhead/PlayheadTests/Fixtures/NarlEval/2026-04-24/FrozenTrace-71F0C2AE-7260-4D1E-B41A-BCFD5103A641.json` — 111 shadow entries across [5520, 7097.86].

I did NOT inspect `.captures/2026-04-23/...xcappdata/shadow-decisions.jsonl` — not accessible in the worktree.

## Evidence

### Exporter write-path (ShadowDecisionsExporter.swift:101-119)

Serializes each row as JSON with these top-level fields: `schemaVersion`, `type`, `assetId`, `windowStart`, `windowEnd`, `configVariant`, `fmResponseBase64`, `capturedAt`, `capturedBy`, `fmModelVersion`. The actual shadow classifier output lives inside the base64-encoded `fmResponseBase64` blob as a serialized `ShadowFMPayload` (contains `refinementResponse: RefinementWindowSchema?`). **No top-level `isAd`, `shadowIsAd`, `shadowConfidence`, `weight`, or equivalent field is written.** The classifier's decision is opaque until someone decodes the blob.

### Corpus-builder read-path (`parseShadowLog`, line 327-353)

```
let shadowIsAd: Bool? = (obj["isAd"] as? Bool) ?? (obj["shadowIsAd"] as? Bool)
```

Builder reads for `isAd` or `shadowIsAd` top-level keys. Neither is written by the exporter. **The blob is never decoded.** Result: `shadowIsAd = nil` for every row.

### Corpus-builder fold (`assembleTrace`, line 541-548)

```swift
for s in shadow {
    fullEvidence.append(FrozenTrace.FrozenEvidenceEntry(
        source: "shadow:\(s.configVariant)",
        weight: (s.shadowIsAd ?? false) ? 1.0 : 0.0,
        windowStart: s.windowStart,
        windowEnd: s.windowEnd
    ))
}
```

`nil ?? false → false → weight=0.0`. Every shadow row lands at weight=0, regardless of what the shadow classifier actually decided.

### Fixture cross-check (71F0C2AE FrozenTrace)

- 111 / 111 shadow entries have `weight=0`. Not 110 zero + 1 non-zero. Not a distribution. Uniform zero.
- The shadow entry at `[5670.00, 5700.00]` (which covers the LIVE-classifier's strong-signal ad window at `[5670.6, 5690.52]` that reached `fusedSkipConfidence=1.0`) is weight=0.
- If hypothesis 1 were true, we'd expect that specific entry to be weight=1.0 (shadow agreed with live). It is not. This rules out hypothesis 1.

## Verdict

**Hypothesis 2 + 3 simultaneously (they're two sides of the same gap). Confidence: HIGH.**

- The **exporter** doesn't emit a boolean/confidence field that the corpus-builder can read — no one decodes the `fmResponseBase64` blob on either side (hypothesis 2).
- The **corpus builder** tolerates the missing field by silently defaulting `shadowIsAd ?? false` → `weight=0.0`, producing 111 false-disagreement entries indistinguishable from real disagreement (hypothesis 3).

Since the exporter writes no `isAd` field and the builder never decodes the blob, the weight=0 across the fixture's entire shadow coverage is structural, not signal. The uniform-zero distribution across 111 entries (including a region where the live classifier confidently said "ad") is dispositive.

### What would raise confidence further (not needed — already high)

Decoding one `fmResponseBase64` from the raw `shadow-decisions.jsonl` capture on `.xcappdata/` and confirming the embedded `refinementResponse` is non-null and meaningful for the [5670, 5700] window. Skipped — the uniform-zero fixture distribution is already decisive.

## Follow-ups filed

- `playhead-gtt9.4.4` — ShadowDecisionsExporter must export shadow isAd/confidence into JSONL (write-path fix).
- `playhead-gtt9.4.5` — NARL corpus builder must not silently default shadow weight=0 when isAd missing (read-path defense-in-depth).

## What I could not verify

1. Whether `.captures/2026-04-23/...xcappdata/shadow-decisions.jsonl` raw rows contain non-null `refinementResponse` payloads inside the `fmResponseBase64` blob (not accessible in the worktree). High-confidence theoretical: the dispatcher path logs "shadow FM refinement failed" only on FM exceptions, so in a normal run most rows should carry real `refinementResponse` data — meaning the shadow classifier DID produce decisions that are being structurally discarded before reaching `weight`.
2. Whether shadow classifier decisions, once threaded through, would materially change NARL evaluation signal (data-dependent; out of this spike's scope — belongs to a post-fix repro bead analogous to gtt9.4.3).
