# bd-4xqf SpanFinalizer Reachability — 2026-06-01

> **Purpose.** PR #207's code-path map (`docs/bd-4xqf-codepath-map.md`)
> flagged `Playhead/Services/AdDetection/SpanFinalizer.swift` as having
> zero production callers by grep, and listed it as a top-3 suspect for
> `FUSION_DROP` boundary-shrink. This document closes that question by
> exhaustively searching for direct, indirect, and runtime-resolved call
> paths.
>
> **Read-only.** No production code is modified by this investigation
> or the accompanying PR.

---

## Verdict

**UNREACHABLE in production.** `SpanFinalizer` is compiled into the
Playhead app target but is invoked from no production source file. The
only call sites in the entire repository are inside the test target
(`PlayheadTests/Services/AdDetection/SpanFinalizerTests.swift`). It can
therefore be ruled out as a `FUSION_DROP` suspect for bd-4xqf — the
persisted `AdWindow` row cannot be narrowed by code that the
decision-emission path never reaches.

---

## Evidence

### 1. Direct call-site search

Both the original `grep` command and a follow-up ripgrep over all
`.swift` files were run. Every match below is either a self-reference
inside `SpanFinalizer.swift`, a test, or a doc comment.

| Hit | What it is |
|---|---|
| `Playhead/Services/AdDetection/SpanFinalizer.swift:68` | The struct declaration itself: `struct SpanFinalizer: Sendable {` |
| `Playhead/Services/AdDetection/SpanFinalizer.swift:69` | `private static let logger = Logger(...)` — internal |
| `Playhead/Services/AdDetection/SpanFinalizer.swift:86` | `func finalize(_ candidates: [CandidateSpan]) -> [FinalizedSpan]` — declaration |
| `Playhead/Services/AdDetection/EvidenceLedgerEntry.swift:32` | Comment only: `/// Used by SpanFinalizer.capEligibility to allow demotions...` (documents an ordering invariant; no call) |
| `PlayheadTests/.../SpanFinalizerTests.swift:66` | Test helper: `SpanFinalizer(episodeDuration: episodeDuration, chapters: chapters)` |
| `PlayheadTests/.../SpanFinalizerTests.swift:80–564` | 50+ in-test `.finalize(candidates)` invocations |
| `PlayheadTests/.../AdDetectionServiceShadowModeTests.swift:830` | Comment only: `// decision is markOnly (e.g. via SpanFinalizer chapter penalty, FM-suppression cap...)` (describes a hypothetical/future emission shape; no call) |

There is **no production source file outside `SpanFinalizer.swift`
itself that mentions `SpanFinalizer` at all**, by name or by member.

### 2. Supporting-type search (factories, generics, protocols)

The struct's public surface is three types: `SpanFinalizer`,
`FinalizedSpan`, and `FinalizerConstraint`. A construction of any of
these — or of its input type `CandidateSpan` — at a production call
site would indicate indirect wiring. None exist:

| Type | Production hits outside SpanFinalizer.swift |
|---|---|
| `FinalizedSpan` | **0** — declared at `SpanFinalizer.swift:23`, never used elsewhere |
| `FinalizerConstraint` | **0** — declared at `SpanFinalizer.swift:33`, never used elsewhere |
| `CandidateSpan` (the SpanFinalizer one at `SpanFinalizer.swift:47`) | **0** — the three production matches at `MinimalContiguousSpanDecoder.swift:169,305,316` are a *different* `private struct CandidateSpan` declared at `MinimalContiguousSpanDecoder.swift:135` with non-overlapping fields (`firstOrdinal`, `lastOrdinal`, `anchorProvenance`); they are not the same type and cannot be passed to `SpanFinalizer.finalize`. |
| `SpanFinalizer.Type` | **0** — no metatype factory references |

`SpanFinalizer` conforms only to `Sendable` (`SpanFinalizer.swift:68`),
which is a marker protocol — it provides no dynamic dispatch entry
point that could resolve to `SpanFinalizer` from a generic call site.

### 3. Runtime-lookup and DI search

No reflection-based or registry-based resolution paths exist that
could resolve `SpanFinalizer` at runtime:

- No `NSStringFromClass`/`NSClassFromString` references to
  `SpanFinalizer` anywhere in the repo (sole `NSClassFromString` use
  is `PlayheadRuntime.swift:2322` checking for `XCTestCase`).
- No service locator / dependency container pattern in the codebase
  (grep for `ServiceLocator`, `DependencyContainer`, `registerService`,
  `@Injected`, `@Dependency`, `Container.register` returns zero hits
  across `Playhead/`).
- No string-based lookup of `"SpanFinalizer"` outside the logger
  subsystem at `SpanFinalizer.swift:69`.

### 4. Production decision pipeline audit

The bd-4xqf code-path map (`docs/bd-4xqf-codepath-map.md:14–43`)
diagrams the AtomEvidence → DecodedSpan → DecisionResult → AdWindow
chain. Inspection of every stage file confirms `SpanFinalizer` is
absent from all of them:

- `Playhead/Services/AdDetection/MinimalContiguousSpanDecoder.swift` — no `SpanFinalizer` reference.
- `Playhead/Services/AdDetection/BackfillEvidenceFusion.swift` — no `SpanFinalizer` reference. Comment at line 14 of `SpanFinalizer.swift` even states explicitly: *"Does NOT modify BackfillEvidenceFusion or DecisionMapper."*
- `Playhead/Services/AdDetection/AdDetectionService.swift` (7700+ lines, the orchestration site that calls `buildFusionAdWindow` and `applyBoundaryRefinement`) — no `SpanFinalizer` reference; sole `finalize` match at line 5817 is `"Shadow retry: failed to finalize flag"`, unrelated.
- `Playhead/Services/SkipOrchestrator/SkipOrchestrator.swift` — no `SpanFinalizer` reference.

---

## Git history

`git log --follow --oneline Playhead/Services/AdDetection/SpanFinalizer.swift`:

| SHA | Date | Subject |
|---|---|---|
| `e5fb5151` | 2026-04-15 | `ef2.4.2: Deterministic span finalizer with 6-constraint safety pipeline` (introducing commit, 429 new lines of `SpanFinalizer.swift` + 481 lines of tests) |
| `3e73bc05` | 2026-04-15 | `ef2.4.2 code review: fix capEligibility gate promotion bug` (internal fix + regression test) |
| `b240df1b` | 2026-04-16 | `ef2.4 post-merge: harden trust, suppression, and finalizer edge cases` (internal edge-case fixes + 74 new test lines) |
| `1f3d0cc8` | 2026-05-05 | `feat(ad-detection): qualified track for classifier-seed spans (playhead-fqc8)` (touched comment block at lines 296–301 referencing playhead-fqc8 action-cap interaction; no behavior change to call-site search) |

`git log -S "SpanFinalizer(" --format=...` over the whole repo returns
exactly two commits:

- `e5fb5151` — introducing commit (adds the construction inside
  `SpanFinalizerTests.swift`).
- `a63e0d64` (2026-06-01) — the PR #207 codepath-map commit (only
  documents the issue; no production code).

**No commit has ever added or removed a production call site for
`SpanFinalizer(`.** It was never wired in. The introducing commit
`e5fb5151` did touch `BackfillEvidenceFusion.swift`, but only to add
`Equatable` conformance to `DecisionResult` for test purposes — it did
not insert a finalizer call. The follow-up commit `b240df1b` touched
`BackfillEvidenceFusion.swift` only to add `private(set)` to a
fingerprint-entry array.

The introducing commit's message describes the intent: *"Pure,
stateless `SpanFinalizer` enforces hard invariants after fusion ... 30
tests cover each constraint independently plus combined/edge cases and
determinism."* It says nothing about the production wiring step — and
no follow-up commit ever landed it.

---

## Why the file still compiles

`Playhead.xcodeproj/project.pbxproj:4505` lists
`SpanFinalizer.swift in Sources` under the main Playhead target build
phase (and line 5096 lists `SpanFinalizerTests.swift` under the test
target). The file is therefore type-checked and emitted into the
binary, but the linker is free to dead-strip the methods because no
production code references them. The cost today is roughly a few
hundred lines of compile work per build plus a small symbol footprint;
the behavioral risk to bd-4xqf is **zero**.

---

## Recommended follow-ups (NOT this PR)

The codepath map (`docs/bd-4xqf-codepath-map.md:100`) already
acknowledged: *"`SpanFinalizer` status. Either kill it or wire it.
Limbo code is worse than either."* This investigation does not pick
between the two options. Both are valid and should be raised as a
separate bead.

### Option A: Wire it in

The original `ef2.4.2` design (`SpanFinalizer.swift:1–14`) intended
the finalizer to run **after** `BackfillEvidenceFusion` produces a
`DecisionResult` for each span, but **before** the row is persisted as
an `AdWindow`. Specifically, the constraints it would add over the
current pipeline:

- Constraint 1 (non-overlap) — currently no production check; two
  overlapping AdWindows can be persisted simultaneously.
- Constraint 2 (3 s minimum content gap) — currently no production
  merge step exists post-fusion. The
  `MinimalContiguousSpanDecoder.mergeGapSeconds: 3.0` constant runs
  *before* fusion, on atoms; it does not re-merge after the
  classifier/FM signals come in.
- Constraint 5 (50% auto-skip cap) — no equivalent guard exists today.
  A pathological show could in principle have >50% of episode
  auto-skip eligible.

The natural insertion point is `AdDetectionService.swift` between
fusion (currently at the loop around line 2828 — *"Steps 12–14: Fusion
+ DecisionMapper + SkipPolicyMatrix"*) and the per-span emission /
`buildFusionAdWindow` call (at line 5186 per the codepath map). The
input shape (`CandidateSpan` carrying `DecodedSpan` +
`DecisionResult` + `CommercialIntent` + `AdOwnership`) is already
constructible from values that exist at that point.

If wired, the finalizer would *narrow* AdWindows (overlap-trim,
chapter-penalty `markOnly`), and could therefore become a new
`FUSION_DROP` suspect — meaning **adding it would not fix bd-4xqf and
could make it worse**. Any wire-in should be paired with bd-4xqf
boundary regression coverage.

### Option B: Remove it

`SpanFinalizer.swift` (440 lines), `SpanFinalizerTests.swift` (~570
lines), and the two doc-comment references in
`EvidenceLedgerEntry.swift:32` and
`AdDetectionServiceShadowModeTests.swift:830` can be removed in a
single PR with no behavioral impact. The constraints the file
implements (overlap-trim, gap-merge, 50% cap, etc.) would then be
genuinely absent from the pipeline — which is the current state today
in practice — so removal documents reality rather than changing it.

---

## What changes in this PR

1. This document is added at `docs/bd-4xqf-spanfinalizer-reachability-2026-06-01.md`.
2. `docs/bd-4xqf-codepath-map.md` is updated so the `SpanFinalizer`
   row in the FUSION_DROP suspect table and the snapshot note at the
   bottom both reflect the verified UNREACHABLE verdict instead of the
   "ZERO production callers per grep / suspicious" hedged language.

No production code is touched.
