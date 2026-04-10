# FoundationModels Safety Classifier — Kelly Ripa #1 Mystery

**Audience:** iOS / FoundationModels expert (same one who advised on the original pharma refusal problem)
**Author:** Dan Abrams (Playhead)
**Date:** 2026-04-08
**Status:** working but mysterious; one residual we can't explain
**Companion doc:** [`fm-safety-classifier-problem.md`](fm-safety-classifier-problem.md) — the original pharma problem and your earlier guidance

---

## TL;DR

Your original advice (use `SystemLanguageModel(guardrails: .permissiveContentTransformations)` with plain `String` output and route sensitive content there instead of trying to bypass the default guardrails) **worked**. We built two phases on top of it and got Conan recall from 2/4 to 4/4 on real iPhone hardware.

But there's one residual we can't explain: a refinement-pass refusal on a window of completely benign Kelly Ripa cross-promo content where our trigger-vocabulary router does NOT fire, and the literal rendered prompt does NOT contain any of our pharma trigger words. Apple's safety classifier still refuses with `contextDebugDescription="May contain sensitive content"`.

We're shipping anyway because outward expansion from a sibling window catches the same ad indirectly, but the direct-refinement refusal is fragile and we'd like to understand it.

---

## What we built since the last note

Following your guidance:

### Phase 1: Permissive coarse path (shipped)

- New `PermissiveAdClassifier` actor wrapping `SystemLanguageModel(guardrails: .permissiveContentTransformations)`
- Plain `String` output via a hand-rolled grammar (`NO_AD` / `UNCERTAIN` / `AD L<start>-L<end>[,L<n>-L<m>]...`)
- Per-call sessions (`LanguageModelSession(model: model)` per request) — same fix as the bd-34e Fix B v5 conversation-history accumulation bug we hit on the standard path
- Greedy sampling (`GenerationOptions(sampling: .greedy)`) for determinism
- A `SensitiveWindowRouter` that uses our existing `RedactionRules.json` dictionary as a *routing* dictionary instead of a *redaction* dictionary
- Coarse-pass dispatch: sensitive windows short-circuit the `@Generable` call entirely and go through the permissive classifier

A 124-probe validation matrix on real device produced **0 refusals** across the entire pharma family (CVS pre-roll, Trulicity, Ozempic, Rinvoq, BetterHelp, regulated medical tests). Phase 1 confirmed your architectural recommendation empirically.

### Phase 2: Permissive refinement path (just shipped today)

The first phase only handled coarse classification. Refinement (which produces precise span boundaries from a coarse `containsAd` verdict) was still going through `LanguageModelSession.respond(generating: RefinementWindowSchema.self)` and refusing on the same content. Phase 2 mirrors Phase 1 for refinement:

- New `PermissiveAdClassifier.refine(window:)` actor method
- New refinement parser preserving `(firstLineRef, lastLineRef)` pair structure (so non-contiguous spans become separate `RefinedAdSpan`s downstream)
- New `refinePassB` dispatch overload that routes sensitive plans through the permissive path
- Recall safety net: if the permissive parser fails or returns garbage, we emit a single full-window `RefinedAdSpan` as a fallback rather than losing the ad
- Per-plan routing inspects the **literal `plan.prompt`** rather than just the structured catalog — this is the cleanest signal because it's what the FM actually sees

Phase 2 recovered the CVS pre-roll refinement (which had been silently dropped by the @Generable refusal), getting recall from 2/4 to 3/4 on the first end-to-end run.

Pulling the fourth ad over the line happened via outward expansion from a sibling window — see the mystery below.

---

## The mystery

Conan "Fanhausen Revisited" episode, four ground-truth ads:

1. **CVS pharmacy pre-roll** — segments 0–3 (clearly pharma, the trigger family)
2. **Kelly Ripa cross-promo #1** — segments 4–5 (completely benign promo for her own podcast)
3. **SiriusXM credits** — segments around 19–20
4. **Kelly Ripa cross-promo #2** — segments later in the episode

The permissive refinement path routes Kelly Ripa #1's **window** through the bypass when the literal prompt contains pharma trigger vocabulary. But on the latest device run:

- Kelly Ripa #1 refinement plan window=3, lineRefs 4–5, **promptTokens=424**
- The router (`routeText(plan.prompt)`) inspected the literal rendered prompt and returned `.normal`
- The plan went through the standard `@Generable` refinement path
- Apple's safety classifier refused with:
  - `contextDebugDescription="May contain sensitive content"`
  - `GenerationError.refusal(record: TranscriptRecord)`

**The Kelly Ripa #1 window contents:**

```
L4> "Hey everyone, it's Kelly Ripa, and we're celebrating 3 years of my podcast"
L5> "Let's talk off camera"
```

These are the only two transcript segments in the window. There is nothing pharma about them. The refinement prompt is 424 tokens — too small to contain a meaningful evidence-catalog snippet pulling in pharma atoms from elsewhere in the episode. Our pharma trigger vocabulary (vaccines, drug brands, mental health services, regulated medical tests) does not fire on the rendered prompt at all — we verified by routing on `plan.prompt` directly.

Earlier Phase 2 work hypothesized that the embedded evidence catalog from elsewhere in the episode was the culprit (the catalog is built per-episode, so a window's refinement prompt might embed CVS pre-roll catalog atoms). We added a catalog-aware routing overload to inspect `plan.promptEvidence.matchedText` for trigger words. That overload also did not fire on Kelly Ripa #1 — so the catalog snippet for this plan does not contain pharma either.

**But Apple still refuses, every run, with "May contain sensitive content."**

---

## What this is NOT

We've ruled out the obvious explanations:

1. **Not the segments themselves** — "Hey everyone, it's Kelly Ripa, and we're celebrating 3 years of my podcast" / "Let's talk off camera" passes the safety classifier on its own as a coarse probe (we ran it through the 124-probe matrix).
2. **Not the embedded catalog** — `plan.promptEvidence.matchedText` for this plan contains no pharma trigger words. Our router check inspects the literal `plan.prompt` byte-for-byte and finds zero matches against our 5-category rule dictionary.
3. **Not a token-budget overflow** — promptTokens=424, well under the budget. The shrink retry loop is not in play.
4. **Not the schema** — we're using the slim `@Generable RefinementWindowSchema` that we already token-counted on this device.
5. **Not an injected adversarial preamble** — bd-34e replaced the old jailbreak-defense framing with neutral instructional framing months ago, and that fix recovered other refusals successfully.
6. **Not non-determinism we can pin** — the refusal happens **every** run on this content, with greedy sampling, with per-call sessions (no conversation-history pollution from prior windows).
7. **Not session state from prior calls** — each refinement window gets its own freshly-minted, freshly-prewarmed `LanguageModelSession`. The bd-34e Fix B v5 bug (shared sessions accumulating ~4000 tokens of conversation history) is gone.

---

## Why we're shipping anyway: outward expansion catches it

The other half of this work is bd-1my, an "outward expansion" pass: after refinement produces spans, any span that touches its window's first or last line ref triggers a re-refinement on a window expanded outward by N segments. The expansion is bounded (max 3 iterations, max ±10 segments).

On the same device run:

```
fm.classifier.expansion-trimmed sourceWindow=5 requestedLowerAdd=5 requestedUpperAdd=5 acceptedLowerAdd=2 acceptedUpperAdd=2
```

`sourceWindow=5` was originally lineRefs 6–8 (the SiriusXM credits area). Its initial refinement produced a span touching the lower boundary (lineRef 6), which triggered an outward expansion downward by 2 segments — sweeping the expanded window into lineRefs 4–8. The expanded refinement prompt covered Kelly Ripa #1's content (lineRefs 4–5) along with the SiriusXM credits, AND the expanded prompt was no longer refused. The FM produced refined spans covering 4–5, and those spans got persisted as the Kelly Ripa #1 detection.

So the architectural pair (permissive bypass + outward expansion) recovers all 4 ads, even though the direct refinement on Kelly Ripa #1 still refuses.

This works but feels load-bearing on luck. If the model's expansion behavior changes — or the SiriusXM credits stop touching their window's lower boundary — the Kelly Ripa #1 catch could regress overnight. We'd like to understand what's actually triggering the refusal so we can either pre-empt it through the permissive route OR confirm we can stop chasing it.

---

## What's in the prompt that we might be missing

Our refinement prompt builder (`buildRefinementPrompt`) constructs:

1. **Preamble** — short instructional framing: "Refine ad spans. Classify whether the following podcast transcript window contains advertising or promotional content. Each transcript line is prefixed with `L<number>>` followed by quoted text. Use the line numbers to cite supporting evidence in your output."
2. **Schema preamble** — lists the `RefinementWindowSchema` field expectations
3. **Evidence catalog snippet** — lines like `[E1] "matched-text" (kind, line N)` for each `PromptEvidenceEntry` in the plan
4. **Transcript fence** — `<<<TRANSCRIPT>>>` ... `<<<END TRANSCRIPT>>>` containing the L-prefixed window lines

The Kelly Ripa #1 prompt contains all of the above. The transcript section is just the two benign Kelly Ripa lines. The catalog section has whatever atoms the episode-wide `EvidenceCatalogBuilder` placed adjacent to lineRefs 4–5 — we believe small (the prompt is only 424 tokens total) and known to contain no pharma trigger vocabulary per our dictionary.

Could there be a **non-pharma classifier signal** in here — celebrity names? podcast cross-promotion language? "Hey everyone"? Some heuristic Apple's safety classifier applies to "this looks like ad copy I should refuse"? The ironic case would be Apple's classifier refusing because it correctly detected this is an ad and treats ad classification of ads as some kind of meta-loop.

---

## Specific questions

1. **Does Apple's safety classifier have a category for "celebrity name + show cross-promotion"** that fires independent of pharma? Our dictionary doesn't cover this, but the symptom looks like a non-pharma category trigger.

2. **Is there session state we don't see** — Kelly Ripa #1 is roughly the third refinement call in this run. Could there be a per-process or per-actor accumulation that's not visible at the `LanguageModelSession` level? We've already eliminated per-session conversation history with the per-call session pattern.

3. **Does the refusal record's `TranscriptRecord` carry diagnostic information** beyond `contextDebugDescription`? We're logging `String(describing: error)` and only seeing `recordReflect=...GenerationError.Refusal(record: TranscriptRecord)`. If there's a richer field, what's the API to read it?

4. **Is there a way to ask FoundationModels "would this prompt refuse?"** — a dry-run / preflight that returns the refusal classification without consuming the call? That would let us route on the actual classifier decision instead of trying to predict it via vocabulary.

5. **Can we file a Feedback Assistant report on this specific case** with the verbatim Kelly Ripa #1 refinement prompt and have Apple categorize the refusal? We have the exact prompt text, the exact device, the exact iOS version. Reproducer is trivial.

---

## Minimal reproducer

The exact refinement prompt that refuses (rendered from the Kelly Ripa #1 plan, 424 tokens):

```
Refine ad spans. Classify whether the following podcast transcript window contains advertising or promotional content. Each transcript line is prefixed with `L<number>>` followed by quoted text. Use the line numbers to cite supporting evidence in your output.

[evidence catalog snippet — small, no pharma trigger vocab in any line]

<<<TRANSCRIPT>>>
L4> "Hey everyone, it's Kelly Ripa, and we're celebrating 3 years of my podcast"
L5> "Let's talk off camera"
<<<END TRANSCRIPT>>>
```

Submitted via `LanguageModelSession.respond(to: prompt, generating: RefinementWindowSchema.self, options: GenerationOptions(maximumResponseTokens: ...))`.

Refuses on every run with `GenerationError.refusal(record: TranscriptRecord)` and `contextDebugDescription="May contain sensitive content"`.

Submit the same content via `SystemLanguageModel(guardrails: .permissiveContentTransformations)` + plain `String` output and it does not refuse — so the content is passable on the permissive path. The mystery is why the default-guardrails path refuses given the absence of pharma trigger vocabulary.

---

## Where the code lives

- `Playhead/Services/AdDetection/PermissiveAdClassifier.swift` — Phase 1 + Phase 2 permissive path (coarse + refinement)
- `Playhead/Services/AdDetection/SensitiveWindowRouter.swift` — routing dictionary
- `Playhead/Services/AdDetection/FoundationModelClassifier.swift` — `coarsePassA` and `refinePassB` dispatching overloads, the per-plan `routeText(plan.prompt)` check, and the standard `@Generable` paths
- `Playhead/Resources/RedactionRules.json` — the trigger vocabulary dictionary (now repurposed for routing)
- `PlayheadTests/Services/AdDetection/PermissiveAdClassifierTests.swift` — parser + builder unit tests (44 tests, all passing)
- `PlayheadTests/Services/AdDetection/SensitiveWindowRouterTests.swift` — router unit tests including catalog-aware routing
- `PlayheadTests/Services/AdDetection/PlayheadFMSmokeTests.swift` — on-device smoke test, currently asserting full 4/4 recall
- `PlayheadTests/Fixtures/RealEpisodes/ConanFanhausenRevisitedFixture.swift` — verbatim transcript with the four ground-truth ads

---

## What we'd consider "solved"

Any of:

- A documented Apple category that explains why the Kelly Ripa #1 refinement prompt refuses (so we can extend our routing dictionary to include it)
- A diagnostic API that surfaces *which* classifier rule fired the refusal
- A preflight API to predict the refusal without consuming a generation call
- An official "yes, that content trips the celebrity-cross-promo classifier and there's no way to opt out via the default guardrails — use the permissive path" answer, so we can mark this as a known limitation and stop chasing it

---

## What worked, for the record

Your original recommendation was the right call. The permissive guardrails + plain `String` output + per-call sessions + greedy sampling architecture is now load-bearing in our production path and recovers every pharma probe we've thrown at it. Phase 1 (coarse) and Phase 2 (refinement) both ship behind a router that's wired through `PlayheadRuntime` so the dispatch is byte-identical to the legacy path when the router is `.noop`. The integration was clean and the unit-test footprint is small.

Thank you again for the architectural pointer — this unblocked weeks of redactor whack-a-mole.

---

## Expert response (2026-04-08)

The expert's diagnosis: **the router is checking less text than Apple's guardrails are checking.**

`router.routeText(plan.prompt)` only inspects the visible rendered prompt. Apple's guardrails inspect the *full augmented input*, which on the guided-generation path includes:

- session instructions
- framework-injected schema text (Foundation Models automatically includes details about the `Generable` type in the prompt unless `includeSchemaInPrompt: false`)
- guide descriptions (effectively another form of prompting per Apple's docs)
- any tool descriptions / outputs

So the negative router result is **not** strong evidence the guardrail "should not" have fired — it only proves our visible prompt lacks our current trigger lexicon. The hidden schema/instruction surface is the most plausible source of the extra signal tipping the classifier over the line.

There is no documented Apple "celebrity cross-promo" category. Public categories are broader prohibited domains (regulated healthcare, legal/financial/employment/criminal-justice, social scoring, biometric inference). A Kelly-specific documented category is unlikely.

There is also recent evidence of benign false positives in the wild — Apple developer forum threads on "Tailwind", "JFK/KJFK", "frunk", "Lock Pride". An Apple staff designer in one of those threads says guardrails are classifier systems and prompt/rule attempts often won't suppress them; the suggestion is to file feedback or switch to a more lenient guardrail setting.

### The experiment that should answer this fastest

Run the failing Kelly refinement again, changing only:

1. Use the guided-generation overload with **`includeSchemaInPrompt: false`**.
2. Remove our manual schema preamble.
3. Add **one concrete example** of the desired output format in instructions.

Apple documents `includeSchemaInPrompt` specifically for cases where the model already knows the expected response format because a full example is provided. If the refusal disappears, the hidden schema text was the extra surface tipping the default guardrails.

Minimal shape:

```swift
let session = LanguageModelSession(
    instructions: Instructions {
        "You are a transcript span extractor. Return only ad-span line ranges."
        """
        Example output:
        {"spans":[{"firstLineRef":4,"lastLineRef":5}]}
        """
    }
)

do {
    let response = try await session.respond(
        to: prompt,
        generating: RefinementWindowSchema.self,
        includeSchemaInPrompt: false,
        options: GenerationOptions(sampling: .greedy)
    )
} catch LanguageModelSession.GenerationError.refusal(let refusal, let context) {
    let explanation = try? await refusal.explanation
    logger.error("Refusal: \(context.debugDescription)")
    logger.error("Explanation: \(explanation?.content ?? "<none>")")
}
```

Note `refusal.explanation` returns a model-generated `Response<String>` — better diagnostic surface than `contextDebugDescription` alone.

### Direct answers to the open questions

1. **Celebrity-cross-promo category?** No documented Apple category found.
2. **Hidden cross-session state?** Apple documents transcript/history as living on `LanguageModelSession`. No documentation for cross-session conversation accumulation. Less likely than hidden schema/instruction input.
3. **Better diagnostics than `contextDebugDescription`?** Yes — `refusal.explanation`. No public API for rule IDs or classifier-category labels on `TranscriptRecord`.
4. **Preflight / dry-run "would this refuse?" API?** None documented. The supported path is: call, catch `.refusal`, inspect `refusal.explanation`.
5. **Feedback Assistant on this exact case?** Yes. Apple explicitly asks for Foundation Models bug reports to include a language-model feedback attachment generated via `logFeedbackAttachment(sentiment:issues:desiredOutput:)` and a sysdiagnose. File with the exact Kelly prompt, OS build, device, both the failing default-path call and the succeeding permissive-path call, and the result of the `includeSchemaInPrompt: false` A/B test.

### Production recommendation

If `includeSchemaInPrompt: false` + one-shot example fixes Kelly: keep guided generation for refinement, strip the schema preamble, rely on the example.

If it does **not** fix Kelly: stop trying to extend the routing dictionary. Apple staff explicitly says guardrails usually cannot be suppressed by extra prompts/rules, and permissive mode is the supported escape hatch. Either:

1. Make all refinement use the permissive string path, or
2. At minimum, **automatically retry any default-path refinement refusal through the permissive string path, regardless of vocabulary**.

This avoids chasing undocumented classifier boundaries with lexicons.

### Expert's bottom line

> The second mystery is not really "Why does Kelly Ripa trigger a hidden celebrity classifier?" It is "Why are you still treating `plan.prompt` as the full guarded input on guided generation?" Once you account for the framework-added schema/instruction surface, the behavior stops being mysterious and starts looking like a false positive on the augmented prompt.

---

## Implementation (2026-04-09) — playhead-994, playhead-36t, playhead-eu1

Three beads landed on `feature/kelly-ripa` in response to the expert recommendations above.

### playhead-994: `includeSchemaInPrompt: false` experiment

`FoundationModelClassifier.LiveSessionActor` now has a flag-gated path activated by the environment variable `PLAYHEAD_FM_994_SCHEMA_LESS=1`. When set:

- `LanguageModelSession` is initialised with a one-shot example in its `Instructions` block instead of relying on the default framework-injected schema text.
- `session.respond(to:generating:includeSchemaInPrompt:options:)` is called with `includeSchemaInPrompt: false`.

**What to expect on device:** If the Kelly Ripa refusal disappears when running with the flag set, the hidden schema/instruction surface was the extra signal triggering Apple's guardrails. If it persists, the flag alone is not sufficient and the permissive fallback path (eu1) is the correct long-term fix.

**How to run:** Set `PLAYHEAD_FM_994_SCHEMA_LESS=1` in the Playhead scheme's environment variables in Xcode and exercise the Kelly Ripa fixture through the app.

### playhead-36t: capture `refusal.explanation`

`reportRefinementPassRefusalDetailIfNeeded` in `FoundationModelClassifier` is now `async`. When a refinement window refuses, it calls `try? await refusal.explanation` (a model-generated `Response<String>`) and:

- logs the explanation via `OSLog` alongside the existing diagnostic
- adds it to `RefinementPassRefusalDiagnostic.refusalExplanation: String?`
- propagates it through `RefinementResponseOutcome.failure(_, refusalExplanation:)` and into `SemanticScanResult.refusalExplanation: String?`

The field is nil for successful scans, for non-refusal failures, and whenever the async explanation fetch fails (permissive paths included). It is diagnostic-only and does not affect routing or persistence schema.

### playhead-eu1: auto-retry refusals via permissive path

When `refinePassB` receives a `.failure(.refusal, ...)` outcome for a window **and** a `PermissiveAdClassifier` is available, it now automatically retries via `permissive.refine(window:...)` instead of recording a failed window. If the permissive retry produces spans, the window is appended to output with `usedPermissiveFallback: true` and `permissiveFallbackReason` set to the refusal explanation (may be nil). If the permissive retry also fails or produces no spans, the window is dropped cleanly.

New fields:
- `FMRefinementWindowOutput.usedPermissiveFallback: Bool` (default `false`)
- `FMRefinementWindowOutput.permissiveFallbackReason: String?` (default `nil`)
- `SemanticScanResult.usedPermissiveFallback: Bool` (default `false`)
- `SemanticScanResult.permissiveFallbackReason: String?` (default `nil`)

`BackfillJobRunner.makeRefinementScanResult` propagates both fields from the window output to the persistence row.

This eu1 gate is vocabulary-independent — it fires on any `@Generable` refusal, including the Kelly Ripa false-positive pattern. The `SensitiveWindowRouter` path (vocabulary-triggered, pre-empts the `@Generable` call entirely) is unchanged.

## Resolution (2026-04-09) — confirmed on device

The `includeSchemaInPrompt: false` experiment (playhead-994) was run on a thermally nominal device (two runs, both thermal=nominal). Result: **zero refusals on both runs**. The Kelly Ripa #1 window passed through the default-guardrails `@Generable` path without refusal.

**Root cause confirmed:** Framework-injected schema text (the hidden augmented input added when `includeSchemaInPrompt` defaults to `true`) was tipping Apple's safety classifier threshold on the Kelly Ripa window. The visible prompt content was never the issue.

**Fix applied:** `includeSchemaInPrompt: false` is now the unconditional default for all `refinePassB` calls. The manual schema preamble in `buildRefinementPrompt` has been removed. The one-shot example in the session `Instructions` block provides reliable format guidance as the permanent replacement.

**`eu1` auto-retry** remains active as a safety net for any future false positives. `SensitiveWindowRouter` remains as a fast-path optimization.

**Status:** ✅ Resolved. `playhead-66k` (Feedback Assistant report) remains open as a report to Apple for their records.

**Test coverage:** `PlayheadTests/Services/AdDetection/KellyRipaFMSafetyTests.swift` — 9 tests across two `@Suite` structs (`PlayheadRefusalExplanationTests` for 36t, `PlayheadEu1AutoRetryTests` for eu1). All pass on simulator.
