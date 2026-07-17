# xsdz.34 — User-Correction Read-Side + Two Pinned-Invariant Overturns — Design Doc

**Bead:** playhead-xsdz.34 (P2, parent epic xsdz — multi-signal local ad scorer)
**Status:** DESIGN-DOC phase. No production Swift or tests changed by this document.
**Author:** principal-engineer design pass, 2026-07-16
**Blocks:** xsdz.36 (presence-gated rediff activation + staged rollout) → which gates rediff **auto-skip**.
**Reviewer gate:** Dan reviews this doc before any implementation. Part 3 carries a STOP-and-re-present rule; see §4 and the verdict in §8.

---

## 0. TL;DR

The **write** path for "not an ad" vetoes exists and persists (`PersistentUserCorrectionStore`, wired at `PlayheadRuntime.swift:776`). The **read** path is a stub: both `AtomEvidenceProjector` call sites pass `NoCorrectionMaskProvider()`, so no atom ever carries `.userVetoed` and user vetoes never reach detection. This is the highest-precision signal available, dropped on the floor.

This bead wires the real read side so an explicit `.exactSpan` "not an ad" veto:

1. **(Part 1, safe)** un-anchors the vetoed atoms in the projector (`isAnchored=false`) **and** feeds the existing slot-pass `vetoedRanges` / `.vetoNewlyEnclosed` gate (xsdz.20). One provider lights up both mechanisms. No invariant touched.
2. **(Part 2)** overturns the xsdz.20 pinned-orphan invariant: a veto against a would-be-**widened/absorbed** span now **blocks the widening** instead of orphaning the gesture. The pinned test is **updated** (not weakened) to the new intended behavior.
3. **(Part 3, risky)** the "0.3× weak-labels-never-act-alone" design. Verdict in §8: **GREEN under strict scoping** — an explicit `.falsePositive` veto acting **alone** in the **suppress** direction is safe and the explicit-vs-noisy boundary is *structurally* clean; but the change must be walled off from the boost / auto-skip-creating direction and from any implicit signal, and the Layer-B generalization piece should be split out.

**Critical finding (§5):** the `.vetoNewlyEnclosed` gate lives in `SpliceSlotResolver`, but the **rediff** width pass (`RediffSlotOwnership`) **bypasses the resolver entirely** and has **no veto gate**. Since rediff is the *sole production width setter* that auto-skip will activate, the read-side veto must **also** gate the rediff candidate path, or the safety valve does not actually protect the thing it exists to protect. This is a required part of the work, not an optional follow-up.

---

## 1. Architecture map — what exists today

### 1.1 The two independent suppression mechanisms

There are **two** distinct ways a user correction can suppress detection. They are easy to conflate; the design depends on keeping them separate.

| Mechanism | Keyed by | Effect | Wired today? |
|---|---|---|---|
| **A. Asset-wide passthrough factor** — `PersistentUserCorrectionStore.correctionPassthroughFactor(for:)` | `assetId` (whole episode) | Multiplies effective confidence < 1.0 → decision gates to `.blockedByUserCorrection`. A *soft*, asset-wide dampener. | **YES** — `AdDetectionService.correctionStore` set at `PlayheadRuntime.swift:1510`; consumed in `runBackfill`. |
| **B. Per-atom / per-span veto mask** — `CorrectionMaskProvider` → `.userVetoed` atoms | atom **ordinal** (exact span) | (1) projector sets `isAnchored=false` on vetoed atoms → decoder drops/splits the span; (2) slot pass builds `vetoedRanges` → resolver `.vetoNewlyEnclosed` blocks a widening slot. A *hard*, localized veto. | **NO** — `NoCorrectionMaskProvider()` stub at `AdDetectionService.swift:3436` and `:6805`. |

Mechanism **A** already works and is why the current pinned orphan test can still assert `factor < 1.0` even while the gesture is "orphaned." **This bead wires mechanism B.** The bead's phrase "user vetoes NEVER reach detection" refers specifically to B — the localized, span-precise veto that A cannot express (A can only dampen the whole asset).

### 1.2 The write path (exists — do not touch)

- `AdRegionPopover.swift:41` — "This isn't an ad" → `await correctionStore.recordVeto(span:)` → records `CorrectionScope.exactSpan(assetId:, ordinalRange:)`, `source: .manualVeto`, `correctionType: .falsePositive`. **This is the primary Part-1 signal.**
- `TranscriptPeekView` — transcript-selection revert path → time-range gestures → `recordVeto(startTime:endTime:...)` → `.exactTimeSpan`. Secondary; see §3.4.
- `recordFalseNegative(assetId:reportedTime:)` — "missed ad here" → synthetic negative-ordinal span + `.falseNegative` event. **Boost direction — explicitly out of scope for the veto read-side.**

### 1.3 The read path (the stub this bead replaces)

```
runBackfill (AdDetectionService.swift ~3431)
  → AtomEvidenceProjector.project(regions:, catalog:, atoms:,
                                  correctionMaskProvider: NoCorrectionMaskProvider())  ← INJECTION POINT
      → masks = await provider.correctionMasks(for: ordinalRange, in: assetId)   // returns [:] today
      → per atom: correctionMask = masks[ordinal] ?? .none
                  isAnchored = !anchorProvenance.isEmpty && correctionMask != .userVetoed
  → MinimalContiguousSpanDecoder.decode(atoms:)  // .userVetoed atoms excluded (line 153/219/267)
  → (flag-gated) splice slot pass → computeSpliceSlotPass():
        vetoedRanges = atomEvidence.filter { $0.correctionMask == .userVetoed }.map { TimeRange(...) }
        SpliceSlotResolver.resolveWithDiagnostics(core:, vetoedRanges:, ...)  // .vetoNewlyEnclosed gate
  → (flag-gated) rediff slot pass → RediffSlotOwnership.candidates(...)         // ⚠ NO vetoedRanges — see §5
```

Relevant types:
- `CorrectionMaskProvider` (protocol) / `NoCorrectionMaskProvider` (stub) — `AtomEvidence.swift:135/145`. Its doc already says *"Phase 7 will conform UserCorrectionStore to this protocol."*
- `CorrectionState` — `.none / .userVetoed / .userConfirmed` — `AtomEvidence.swift:123`.
- `SpliceSlotResolver` `.vetoNewlyEnclosed` gate — `SpliceSlotResolver.swift:364`: for the champion slot, `championRange.intersects(veto) && !core.intersects(veto)` → return `nil` slot. **Blocks a slot only when it would NEWLY enclose a vetoed range the core did not already cover.**

---

## 2. Part 1 — Read-side wiring (the safe subset)

### 2.1 Goal

Turn persisted explicit `.exactSpan` `.falsePositive` corrections into `.userVetoed` atom masks at the projector, so both mechanism-B effects (un-anchoring + `vetoedRanges`) light up. Touches no invariant on its own.

### 2.2 The new type — `StoreBackedCorrectionMaskProvider`

A `Sendable` adapter constructed **per backfill run** at the injection point, because resolving `.exactTimeSpan` corrections needs the atom time→ordinal map which only exists at the call site.

```
struct StoreBackedCorrectionMaskProvider: CorrectionMaskProvider {
    // A snapshot of the asset's .falsePositive corrections, pre-fetched once.
    // (exactSpan ordinal ranges + exactTimeSpan time ranges)
    let vetoedOrdinalRanges: [ClosedRange<Int>]           // from .exactSpan
    let vetoedTimeRanges: [(start: Double, end: Double)]  // from .exactTimeSpan (§3.4)
    // atom index for time→ordinal resolution of the time-range vetoes
    let atomsByOrdinal: [(ordinal: Int, start: Double, end: Double)]

    func correctionMasks(for ordinals: ClosedRange<Int>, in assetId: String)
        async -> [Int: CorrectionState] {
        var out: [Int: CorrectionState] = [:]
        // exactSpan: ordinals ∩ requested → .userVetoed
        for r in vetoedOrdinalRanges { for o in r.clamped(to: ordinals) { out[o] = .userVetoed } }
        // exactTimeSpan: atoms whose [start,end) overlaps a vetoed time range → .userVetoed
        for atom in atomsByOrdinal where ordinals.contains(atom.ordinal) {
            if vetoedTimeRanges.contains(where: { overlaps($0, atom) }) { out[atom.ordinal] = .userVetoed }
        }
        return out
    }
}
```

Why an adapter rather than conforming `PersistentUserCorrectionStore` to `CorrectionMaskProvider` directly (the AtomEvidence doc's original hint): the protocol signature only passes `(ordinals, assetId)`, so a store-as-provider cannot resolve `.exactTimeSpan` → ordinals (no atom stream). The adapter carries the atom index. `.exactSpan` alone *could* be served by store-conformance; the adapter is chosen so both scopes work through one path.

### 2.3 Sourcing the corrections

`UserCorrectionStore` (protocol) currently exposes only `correctionPassthroughFactor` / `correctionBoostFactor` / `recordVeto` / `record`. It does **not** expose the raw scopes. Add one minimal, testable async query to the protocol (default-implemented as `[]` on `NoOpUserCorrectionStore`):

```
func activeFalsePositiveScopes(for analysisAssetId: String) async -> [CorrectionScope]
```

Backed on `PersistentUserCorrectionStore` by the existing `activeCorrections(for:)` (already deduped via `distinctSemanticCorrections`), filtered to `.falsePositive` (`event.source?.kind == .falsePositive || event.source == nil`), deserialized to `CorrectionScope`. Filtering to `.falsePositive` is what **excludes** the synthetic negative-ordinal `.falseNegative` spans — so boosts never leak into the veto mask. Keeping the query on the protocol (not the concrete type) keeps it injectable for tests.

### 2.4 Injection point + safety

At `AdDetectionService.swift:3436` (and the Phase-5 projector at `:6805`), replace `NoCorrectionMaskProvider()` with:

```
let maskProvider: any CorrectionMaskProvider
if config.userCorrectionReadSideEnabled, let correctionStore {
    let scopes = await correctionStore.activeFalsePositiveScopes(for: analysisAssetId)
    maskProvider = StoreBackedCorrectionMaskProvider(fromScopes: scopes, atoms: atoms)
} else {
    maskProvider = NoCorrectionMaskProvider()   // byte-identical to today
}
```

- **Flag-gated** (`userCorrectionReadSideEnabled`, new `let ... Bool` in the same `Config` struct as `rediffSlotOwnershipEnabled`, **default OFF**). Rationale: consistent with every width-pass flag in this codebase; lets the corpus no-regression gate run both ways; makes the change reversible; supports xsdz.36 staged rollout. The **suppress direction is safe**, so the flag is a rollout/measurement convenience, not a correctness necessity. *(Flag vs. always-on is a decision for Dan — see §9.)*
- **Nil-store / flag-off → `NoCorrectionMaskProvider()`** → pipeline byte-identical. Preserves the `PlayheadRuntime.swift:1509` race note (store installed via async Task; until then, no masks — the safe default).

### 2.5 Part 1 tests

- **Unit (projector):** vetoed ordinal → `AtomEvidence.correctionMask == .userVetoed` and `isAnchored == false`; non-vetoed ordinals unaffected. (Extends `AtomEvidenceProjector` coverage with a real provider.)
- **Unit (adapter):** `.exactSpan(2...3)` → ordinals 2,3 masked; `.exactTimeSpan(20,30)` → atoms overlapping [20,30) masked; `.falseNegative` scopes ignored; requested-range clamping correct.
- **Integration (end-to-end read path):** persist an `.exactSpan` veto via the real `PersistentUserCorrectionStore` → run the projector with `StoreBackedCorrectionMaskProvider` → assert the decoder omits/splits the span at the vetoed atoms **and** `computeSpliceSlotPass` produces a non-empty `vetoedRanges` covering the vetoed atom times.
- **Flag-off identity:** with `userCorrectionReadSideEnabled == false`, output is byte-identical to `NoCorrectionMaskProvider()`.

---

## 3. Part 2 — Overturn the xsdz.20 pinned-orphan invariant

### 3.1 The exact invariant being overturned

**File:** `PlayheadTests/Services/AdDetection/SpliceSlotOwnershipCorrectionTests.swift`
**Test:** `absorptionOrphansExactSpanVeto()` — `@Test("(iii) v1 PINNED: absorption ORPHANS an .exactSpan veto on the absorbed span")`.

Current pinned behavior (v1): a `.exactSpan` veto recorded against a span that is later **absorbed** by a wider kept slot leaves the correction **orphaned** — the absorbee row is deleted, no surviving span carries the absorbee's ordinal identity, and the gesture survives only as an asset-wide `correctionPassthroughFactor < 1.0` (mechanism A). The test's own trailing comment already names the forward fix:

> *"The forward path — routing not-an-ad corrections into the resolver's vetoed-time-ranges so `.vetoNewlyEnclosed` blocks the absorption — is the fix a real CorrectionMaskProvider will bring; today the gesture is orphaned."*

### 3.2 Intended new behavior (v2)

With mechanism B wired, the absorbee's atoms carry `.userVetoed` → they appear in `vetoedRanges` → when the resolver evaluates the **absorber's** would-be widening slot, `.vetoNewlyEnclosed` fires **iff the veto lies in the region the slot newly encloses** (outside the absorber's presence core). The widening slot is rejected → the absorber stays at its minted width → it does **not** absorb the vetoed region → the veto is **honored, not orphaned**.

### 3.3 Why the current test geometry cannot simply flip its assertion

This is the subtle, load-bearing point. The `.vetoNewlyEnclosed` guard requires `championRange.intersects(veto) && !core.intersects(veto)`. In the *current* test the absorber's core is `[0,50]` and its slot is also `[0,50]` (a degenerate, non-widening geometry), and the veto `[20,30]` sits **inside** the core → `core.intersects(veto) == true` → the gate **cannot** fire. So the forward path is not exercised by the existing setup; the test must be **rewritten to a widening geometry**, which is exactly the realistic case the safety valve exists for (rediff/splice widening a narrow core ~5×).

**Updated test (v2), new geometry** — the presence core is a *narrow* true-ad tail; the slot would *widen* across a region the user vetoed:

```
// v2 (playhead-xsdz.34): an .exactSpan veto against a WOULD-BE-widened region
// now BLOCKS the widening instead of orphaning. Deliberate overturn of the v1
// pinned-orphan behavior: routing the veto into vetoedRanges makes the resolver
// return .vetoNewlyEnclosed for the widening slot, so the narrow core is kept and
// the vetoed region is never absorbed/skipped. See docs/xsdz34-…-design.md §3.
@Test("(iii-v2) OVERTURNED: a veto on a newly-enclosed region blocks the widening (no orphan)")
func vetoBlocksWideningNotOrphaned() async throws {
    // core (true ad tail): ordinals 4..4, [40,50].  Slot would widen to [0,50].
    // User vetoes [10,20] (ordinal 1) — content swept into the widened slot.
    // → vetoedRanges = [[10,20]]; core [40,50] does NOT intersect it, slot [0,50] DOES
    // → resolver → .vetoNewlyEnclosed → nil slot → disposition .noSlot → no absorption.
    // Assert: the span at ordinal 1 SURVIVES (row not deleted); the widened
    //         [0,50] span is NOT persisted; passthrough factor still < 1.0.
}
```

Companion assertion at the resolver seam (may reuse the existing `SpliceSlotResolverTests.vetoNewlyEnclosed()` unit test, which already pins `diag.failureReason == .vetoNewlyEnclosed` with a synthetic `vetoedRanges: [TimeRange(92,96)]`) so the two layers (unit gate + end-to-end wiring) both assert the behavior.

The **v1 orphan case is not deleted silently** — it is transformed. Tests (i) `exactSpanVetoResolvesAfterSlotRewrite` and (ii) `syntheticSpanUnaffectedBySlotPass` are untouched (they don't rely on orphaning). Only (iii) changes, with the comment above documenting the deliberate overturn and the geometry reason.

### 3.4 `.exactTimeSpan` corrections in Part 2

`.exactTimeSpan` vetoes carry only times. Resolved to `.userVetoed` atoms by `StoreBackedCorrectionMaskProvider` via the atom index (§2.2), they then contribute to `vetoedRanges` identically to `.exactSpan`. So Part 2 covers both scopes uniformly once §2 is in place. (The primary popover gesture records `.exactSpan`; `.exactTimeSpan` is the transcript-selection path.)

---

## 4. Part 3 — Overturn "0.3× weak-labels-never-act-alone" (act-alone authority)

### 4.1 What the current design actually is

Two orthogonal, **structurally enforced** axes govern correction authority:

**Axis 1 — explicit vs. implicit (different stores, different types):**
- **Explicit:** `CorrectionEvent` via `PersistentUserCorrectionStore`. Every user gesture ("This isn't an ad", listen-revert, "hearing an ad"). Full weight.
- **Implicit:** `ImplicitFeedbackEvent` via `ImplicitFeedbackStore`. Behavioral signals — `immediateUnskip`, `seekBackIntoSkipped`, `rapidRewindAfterSkip`, `repeatedManualSkipForward`, `showAutoSkipDisabled`. **Weight hardcoded to 0.3** and the initializer *refuses to override it* (`ImplicitFeedbackEvent.weight = 0.3`, enforced even on DB hydration). File header: *"Weak labels NEVER create permanent vetoes alone."*
- Layer-B promotion (`BroadCorrectionEvaluator`): `CorrectionFeedbackKind.explicit = 1.0`, `.implicit = 0.3`; a scope promotes to a persistent show-wide rule only when weighted count ≥ threshold (`sponsorOnShow` 2, `phraseOnShow` 3, …) **and** episode/date diversity holds. A single 0.3 implicit signal can never reach threshold alone; even a single explicit correction (1.0) is short of the ≥2 most scopes require.

**Axis 2 — suppress vs. boost (`CorrectionKind`, derived from `CorrectionSource`):**
- `.manualVeto` / `.listenRevert` → `.falsePositive` → **suppress** (don't skip).
- `.falseNegative` → **boost** (more likely to skip) — `correctionBoostFactor` (cap 2.0), and synthetic `.userCorrection` spans.

These axes are **type-level and store-level**, not thresholds. There is no fuzzy zone to disambiguate "explicit veto" from "noisy implicit" — they are different structs in different SQLite tables.

### 4.2 The safety asymmetry (the crux)

- **Suppress direction** (an explicit `.falsePositive` veto acting alone): worst case is a **missed skip** (fails to skip a real ad). That is *strictly less bad* than the product's stated worst outcome (a false skip eating ~5× content). Acting alone here is **safe**.
- **Boost / auto-skip-creating direction** (a signal acting alone to *create* a skip): worst case is a **false skip eating content**. Acting alone here — **especially for a noisy implicit signal** like `repeatedManualSkipForward` (which maps to `.falseNegative`/boost) — is the footgun the "never-act-alone" design exists to prevent.

### 4.3 What Parts 1/2 already deliver vs. what Part 3 literally asks

The mechanism-B mask path (Parts 1/2) reads **only** explicit `.falsePositive` `CorrectionEvent`s and acts **only** in the suppress direction. So *"an explicit veto acts alone"* — at the per-episode level — is **already delivered by Parts 1/2, with no 0.3× weighting anywhere on that path.** The 0.3× design is untouched by Parts 1/2 because the mask path never reads `ImplicitFeedbackStore` and never enters `BroadCorrectionEvaluator`.

The only thing the *literal* "overturn the 0.3× never-act-alone design" adds beyond Parts 1/2 is **broadening act-alone authority inside `BroadCorrectionEvaluator`** — e.g. letting **one** explicit veto promote a **show-wide (Layer B) suppression rule** without the current ≥2-count/diversity corroboration. That is a **generalization** (one episode → all episodes of the show), still in the suppress direction, but with its own precision cost: one "not an ad" tap would suppress that sponsor/phrase on *every* future episode, including episodes where the same sponsor read *is* a genuine skippable ad. That is a recall/precision tradeoff, not a false-skip tradeoff — safe against the worst outcome, but a real product-behavior change with its own UX weight.

### 4.4 Scope decision for Part 3

**Include (safe, needed):** explicit `.falsePositive` veto acts alone **in the suppress direction, per episode** — delivered by Parts 1/2. Confirm the mask path never consults `ImplicitFeedbackStore` and never enters the boost path.

**Explicitly EXCLUDE (must be walled off in code + comment):**
1. Any grant of act-alone authority in the **boost / `.falseNegative` / auto-skip-creating** direction.
2. Any change to `ImplicitFeedbackEvent.weight` (stays 0.3) or to implicit signals' ability to act alone.
3. `repeatedManualSkipForward` (implicit → false-negative) must remain unable to alone influence an auto-skip.

**Recommend SPLIT OUT to its own bead (re-present to Dan):** the `BroadCorrectionEvaluator` Layer-B generalization (one explicit veto → show-wide suppression rule). It is *not required for the safety valve* (Parts 1/2 satisfy it), it is orthogonal to the read-side wiring, and it carries a distinct false-suppression cost that deserves its own evidence/threshold discussion rather than riding in on the safety-valve bead.

### 4.5 Preserved-invariant tests (Part 3)

- **Explicit veto acts alone (suppress):** covered by the Part-1/2 integration tests (single `.exactSpan` veto suppresses with no corroboration).
- **Noisy implicit never acts alone (preserved):** assert `ImplicitFeedbackEvent.weight == 0.3` unchanged; assert a single implicit signal does **not** promote a Layer-B rule; assert `repeatedManualSkipForward` alone does not raise any span to `autoSkipEligible`.
- **Boost direction unchanged:** `correctionBoostFactor` remains cap-2.0 and multiplicative (cannot manufacture a skip from zero evidence); the mask path does not touch it.

---

## 5. Critical finding — the rediff path has NO veto gate

`RediffSlotOwnership` (header, lines 9–14) states rediff is the **sole production width setter** and *"never calls the resolver; it only produces candidates the shared disposition engine can grade."* Confirmed in code: `RediffSlotOwnership.candidates(...)` / `.resolveSpan(...)` take `decodedSpans, atomEvidence, playedSlots, coreBankMatch, slotBankMatch` — **no `vetoedRanges` parameter** — and `resolveSpan` only checks core overlap + `minCoreCoverage`. The `.vetoNewlyEnclosed` gate lives **only** in `SpliceSlotResolver`, which rediff bypasses. `SpliceSlotDispositionEngine.computeDispositions` (the shared grader) is veto-blind (it checks negative-bank matches, geometry collisions, enclosure — never `vetoedRanges`).

**Consequence:** wiring mechanism B protects the **acoustic/splice** pass via `.vetoNewlyEnclosed`, but a **rediff-widened** slot can still newly enclose (and absorb/skip) a region the user vetoed — as long as a valid core survives outside the vetoed atoms. The projector un-anchoring helps only when the veto removes the *core itself*; it does **not** stop a legitimate narrow core (e.g. `[40,50]`) from being widened by rediff across a vetoed `[10,20]`.

Since xsdz.36/auto-skip activate the **rediff** oracle specifically, **the read-side veto must also gate the rediff candidate path**, or the safety valve does not defend the surface it was built for. Required change:

- Thread `vetoedRanges` (the same `atomEvidence.filter { .userVetoed }` set) into `RediffSlotOwnership.candidates(...)` and apply the **same** newly-enclosed rule in `resolveSpan`: if the synthesized rediff slot intersects a vetoed range that the span's core does not, return `(nil, .vetoNewlyEnclosed)` → `.slot == nil` → status-quo width (no widening, no absorption).
- This reuses the existing `.vetoNewlyEnclosed` diagnostic case (already in `SpliceSlotDiagnostics.FailureReason`) so the rediff shadow tooling and breadcrumbs pick it up for free.

**Sequencing implication:** this rediff-gate work is on the path *between* wiring mechanism B and enabling rediff auto-skip. It must land in this bead (or be an explicit hard-blocker on xsdz.36), not deferred.

---

## 6. Test matrix

| # | Layer | Assertion | Part |
|---|---|---|---|
| T1 | projector unit | vetoed ordinal → `.userVetoed` + `isAnchored=false`; neighbors clean | 1 |
| T2 | adapter unit | `.exactSpan`→ordinals; `.exactTimeSpan`→atoms via time index; `.falseNegative` ignored; range clamp | 1 |
| T3 | integration | `.exactSpan` veto → decoder splits/drops span AND `computeSpliceSlotPass.vetoedRanges` non-empty | 1 |
| T4 | identity | flag-off / nil-store → byte-identical to `NoCorrectionMaskProvider()` | 1 |
| T5 | invariant (updated) | `SpliceSlotOwnershipCorrectionTests` (iii-v2): veto on newly-enclosed region blocks widening; vetoed span survives; no orphan | 2 |
| T6 | resolver unit | `.vetoNewlyEnclosed` fires for a widening slot over a vetoed gap (reuse/extend existing) | 2 |
| T7 | **rediff gate** | rediff slot that would newly enclose a veto → `.vetoNewlyEnclosed` → status-quo width (no absorption) | 5 |
| T8 | preserved | `ImplicitFeedbackEvent.weight == 0.3`; single implicit signal promotes no Layer-B rule; `repeatedManualSkipForward` alone never → `autoSkipEligible` | 3 |
| T9 | preserved | `correctionBoostFactor` unchanged (cap 2.0, multiplicative); mask path never enters boost | 3 |
| T10 | corpus no-regression | L2F live corpus (l2f.8 baseline harness): with read-side ON, **no new false skip** and precision ≥ baseline; determinism preserved | 1/2/5 |

---

## 7. FastTests gate + sequencing

- **FastTests gate (required):** every touched file — `AtomEvidence.swift`, `AtomEvidenceProjector.swift`, `AdDetectionService.swift`, `RediffSlotOwnership.swift`, `UserCorrectionStore.swift` (new protocol method), and the new `StoreBackedCorrectionMaskProvider` — is in the **Playhead production target**. Per CLAUDE.md, run the full `PlayheadFastTests` plan on iPhone 17 sim; green modulo known pre-existing. Load-sensitive perf tests stay in the dedicated serial pass (not affected here).
- **Sequencing:**
  1. Not on the fingerprinter critical path (rediff fingerprinter choice is independent).
  2. **Must precede rediff auto-skip.** Order within/before xsdz.36: (Part 1 read-side wiring) → (Part 2 splice gate + updated invariant) → (§5 rediff veto gate) → *then* xsdz.36 can flip rediff ownership/auto-skip. The §5 rediff gate is the true blocker on auto-skip; Parts 1/2 without it protect only the shadow/acoustic channel.
  3. Flag `userCorrectionReadSideEnabled` default OFF; flip ON and verify (T10 corpus) before auto-skip is enabled — aligns with xsdz.36 staged rollout.

---

## 8. Part 3 safety verdict

**GREEN — safe to implement as scoped in §4.4 — with mandatory guardrails.**

Reasoning:
- The bead's STOP trigger is "part 3 genuinely unsafe **or** the explicit-vs-noisy boundary is not cleanly drawable." The boundary **is** cleanly drawable — it is *structural*: explicit `CorrectionEvent`/`PersistentUserCorrectionStore` vs. implicit `ImplicitFeedbackEvent`/`ImplicitFeedbackStore` (weight hardcoded 0.3), crossed with `CorrectionKind.falsePositive` (suppress) vs. `.falseNegative` (boost). No threshold ambiguity.
- The needed behavior — **an explicit `.falsePositive` veto acting alone in the suppress direction** — is delivered by Parts 1/2 with no 0.3× weighting on that path, and its worst case is a missed skip (safe against the "false skip eats 5× content" worst outcome).
- The noisy-implicit-never-alone intent is **preserved by construction**: the mask path never reads the implicit store or the boost path.

**Mandatory guardrails (a violation of any is a regression):**
1. Act-alone authority is granted **only** to explicit `.falsePositive` vetoes in the **suppress** direction. No act-alone grant in the `.falseNegative` / boost / auto-skip-creating direction.
2. `ImplicitFeedbackEvent.weight` stays 0.3; no implicit signal gains act-alone authority; `repeatedManualSkipForward` alone can never move a span to `autoSkipEligible`.
3. Guard tests T8/T9 must exist and pass.

**One narrow STOP-AND-RE-PRESENT carve-out:** the *literal* overturn of "0.3× never-act-alone" inside `BroadCorrectionEvaluator` — letting **one** explicit veto promote a **show-wide Layer-B suppression rule** — is **not required for the safety valve** (Parts 1/2 satisfy it) and carries a distinct false-suppression / over-generalization cost. **Recommendation: split that into its own bead and re-present to Dan** rather than bundling it into the safety-valve landing. If Dan wants it in-scope here, it is still GREEN (suppress direction), but it should be an explicit, separately-tested decision — not an implicit consequence of "overturn the 0.3× design."

And the unambiguous **STOP** line: if any implementer reads "overturn never-act-alone" as licensing a signal (explicit `.falseNegative` **or** any implicit signal) to act **alone to create an auto-skip**, that is out of scope, unsafe, and must be re-presented — never shipped.

---

## 9. Open decisions for Dan

1. **Flag vs. always-on for the read-side mask** (`userCorrectionReadSideEnabled`). Recommend flag-gated default-OFF for staged rollout + corpus A/B; the suppress direction is safe either way.
2. **Layer-B generalization (§4.3 / §8 carve-out):** split into its own bead (recommended) or include in xsdz.34 with dedicated tests?
3. **Rediff veto gate (§5):** confirm it lands in xsdz.34 (recommended) vs. as a named hard-blocker on xsdz.36. Either way it must precede rediff auto-skip.
4. **`.exactTimeSpan` coverage in v1:** include now (adapter already resolves it, low marginal cost) or ship `.exactSpan`-only first? Recommend include — same code path, and the transcript-selection gesture already writes `.exactTimeSpan`.
