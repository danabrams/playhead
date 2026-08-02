# playhead-6qvf — investigation log

## Code reachability of the chroma fallback (read, not inferred)

`AnchorRef.rediffSlot` is stamped in exactly ONE place:
`AdDetectionService.applyRediffSlotOwnershipPass` →
`SpliceSlotRewriter.apply(..., provenance: .rediffSlot)` (AdDetectionService.swift:7724).

Its slots come from `computeRediffSlotPass` (AdDetectionService.swift:7438), which
picks ONE of two differs:

  * BYTE PRIMARY — `computeByteAlignedPlayedSlots` (7591). Returns nil (→ chroma) on:
      1. `bSideURLs.isEmpty`
      2. `byteDifferASideURL(sourceURL:)` == nil  (b8hj container-UUID rewrite)
      3. A-side `Data(contentsOf:)` throws
      4. EVERY staged B misses: not-anchored / unreadable / gate-rejected
         (`rejectedNoChainedRuns`, `rejectedNonMonotonic`, `rejectedLowChainedFraction`)
  * CHROMA FALLBACK — needs ALL of:
      a. `store.fetchEpisodeFingerprints(assetId:)` non-nil (algorithmVersion-gated)
      b. `provider.refetchedBSideMono16kHz` non-nil (staged file decodes, ≤3h)
      c. `RediffSlotOwnership.gateAndDiff` == .accepted (sourceAudioIdentity double-gate)

Both land in the SAME `SpliceSlotRewriter.apply(provenance: .rediffSlot)`.

## The DAY-0 path does NOT contribute

`mintByteExactDayZeroMarks` (11511) is byte-exact ONLY — it never consults
`refetchedBSideMono16kHz`, and it does not stamp `.rediffSlot` at all; it persists
AdWindows with `boundaryState = "dayZeroRediffByteExact"`. So the collapsed
certainty class is confined to the LAGGED BGTask sweep.

## Production wiring is LIVE

`RediffActivation.isEnabledByDefault == true` (A-side capture + BGTask re-fetch +
`RediffBSideStagingProvider` injected). The production provider DOES implement
`refetchedBSideMono16kHz` (RediffRefetchProduction.swift:365), so the chroma arm is
reachable, not dead code.

`RediffActivation.productionKWayFetchCount == 1` — the lagged sweep stages exactly
ONE B-copy, so "every B gate-rejects" is a SINGLE gate rejection, no k-way cushion.

---

## STEP 1 — MEASUREMENT (2026-08-02)

### What could NOT be measured

**Field rate: UNMEASURABLE on this box.** The newest device pull is
`/Users/dabrams/coreai-spike/dogfood-pull/2026-07-22-stall/.../analysis.sqlite`
(content dated 2026-07-22). Probed directly:

    decoded_spans                                  16 rows
    decoded_spans LIKE '%rediffSlot%'               0 rows
    decoded_spans LIKE '%spliceSlot%'               0 rows
    episode_fingerprints                            9 rows

Zero `.rediffSlot` spans, and the snapshot PREDATES the rediff activation
(`RediffActivation.isEnabledByDefault` flipped true on 2026-07-23, xsdz.36).
There is no device evidence of the byte/chroma split, and no honest estimate can
be derived from this DB. Stated, not estimated.

### What WAS measured — the corpus A/B pairs

`scripts/l2f-6qvf-chroma-fallback-rate.py` drives the REFERENCE aligner
(`scripts/l2f-mp3-forensics.py` — the file `RediffByteAligner.swift` is a
parity-pinned port of) over every real A/B pair in `TestFixtures/Corpus/Audio`,
then applies `RediffSlotOwnership.gateAndDiffBytes`'s gates verbatim at
`Configuration.default` (minRunBytes 65536, minAlignedFractionB 0.5) with
`recoverNonMonotonicSegments: false` — the LAGGED path's value, and the lagged
path is the only producer of `.rediffSlot`.

**N = 51 pairs (`<ep>.mp3` A-side vs `<ep>.fresh.mp3` B-side).**

    accepted (byte path wins)          42 / 51   = 82.4%
    rejectedNonMonotonic                8 / 51
    rejectedNoChainedRuns               1 / 51
    ------------------------------------------------
    WOULD FALL BACK TO CHROMA           9 / 51   = 17.6%

The 9: casefile-2026-05-30, ted-business-2026-05-25, the-daily-show-2026-05-29,
the-rest-is-politics-2026-05-28, nikki-glaser ×4 (2025-02-27/02-28/03-06/03-07),
and pod-save-america-2026-05-31 (the single `rejectedNoChainedRuns` — a
re-encoding CDN, chainedFractionB 0.0).

### The AAC/M4A hypothesis is NOT the driver (on this corpus)

The bead's hypothesised trigger — `.mp3` suffix lying about the container, so the
frame parser finds nothing — did not fire once. All 121 corpus `.mp3` files start
with ID3v2.4 (91), ID3v2.3 (21) or a raw MPEG sync (7); ZERO carry an `ftyp` box
at offset 4, and the reference parser found ≥10,517 frames in every A and B side
(0 parse failures in 102 files). The real drivers are **non-monotonic chains**
(8/9) and a **re-encoding CDN** (1/9).

### 17.6% is a LOWER BOUND on the true fallback rate

This measures fallback trigger #4 only (every staged B gate-rejects). Triggers
#1–#3 — no staged B URL, `byteDifferASideURL` returning nil (playhead-b8hj: the
Data-container UUID is rewritten on reinstall/restore), and an unreadable A-side
— are additional and unmeasured here.

Honest caveats on external validity: the B-sides are l2f-harness fetches, not the
device's `RediffFetchPersona` re-fetches; both sides are downloads rather than a
played copy vs a re-fetch; and 51 research episodes are not Dan's library.

### VERDICT: COMMON, not rare

17.6% (Wilson 95% CI ≈ 9.5%–30.3%) of rediff-attempted episodes take the chroma
arm. Per the bead's own decision rule this is "a live wrong-skip hazard", and the
persisted-provenance change is justified.

---

## STEP 2 — the fix, and why this scope

The measurement says COMMON, so the persisted-provenance change is warranted.

### What changed

`AnchorRef.rediffSlotChroma` — a new bare case, stable type string
`"rediffSlotChroma"`. `computeRediffSlotPass` records WHICH differ arm ran
(`RediffSlotPassComputation.widthProvenance`) and `applyRediffSlotOwnershipPass`
stamps that instead of a literal `.rediffSlot`.

`carriesRediffByteExactWidth` is UNCHANGED as an expression. It is merely now
true. All six consumers move together, per the bead's hard constraint:
wraj floor, qs0d FM-suppression cap, pzy2 creator-chapter, fl4j self-promo,
sik9 post-roll, xsdz.62 `.rediffConfirmed` kind, and `SpanExtentSupport.derive`.

Every one of those is a licence to SKIP or to decline a demotion, so
under-granting costs a banner and over-granting costs show. The conservative
answer is the correct one in all six.

### What a chroma span gets now

Width OWNERSHIP is unchanged (`isWidthOwnership == true`) — the Phase-5 projector
clobber guard and the boundary-refine bypass still protect it, so the marks are
still shown at chroma width. What it loses is EXTENT certainty: both edges
resolve `.unanchored`, tier `.none`, and playhead-2350 keeps it mark-only.

**Deliberately NOT promoted to `.corroborated`.** The bead suggests that tier,
and it is the intuitive home for an independent acoustic re-observation. But
`AutoSkipEdgePadding` has no MEASURED margin for a chroma edge; reusing
`.stingerSnapped`'s 0.75 s/0.75 s would be the identical defect one tier down —
a margin derived for a different instrument. Filed as a follow-up gated on
measuring the chroma differ's per-edge error, not invented here.

### What an OLDER binary sees

`AnalysisStore` decodes `anchorProvenanceJSON` through `[LossyAnchorRef]` +
`compactMap`. A binary predating this case throws on the unknown `type`, the
element becomes `nil`, and it is DROPPED. The array still decodes; the span
survives with its other provenance intact.

The consequence, stated exactly: that span loses its width-ownership marker
(so a fresh decode may re-mint it and the boundary refiners may re-refine it,
returning it to status-quo width) and loses every certainty carve-out for that
element. It UNDER-claims. That is the safe direction, and it is why the chroma
arm got its own type string rather than `"rediffSlot"` plus a discriminating
field: the latter would decode as BYTE-EXACT on an older binary — the one
migration mistake an old binary cannot detect. Pinned by
`olderBinarySeesTheElementDropped`.

### One definition, now actually one

The mutation battery found a hole the reading missed: `SpanExtentSupport.derive`
inlined its own `contains(.rediffSlot)` rather than calling the span predicate,
so widening the predicate left the extent tier — and therefore auto-skip
ADMISSION — untouched. The span property's doc has always claimed the two read
"the SAME `.contains(.rediffSlot)` … so the 'byte-exact rediff' concept has one
definition"; it was two expressions that happened to agree. Both now route
through `[AnchorRef].carriesRediffByteExactWidth`.

### Verification

- `scripts/lint.sh` (whole repo): clean.
- Full `scripts/fast-gate.sh`: **RED (35 known / 17 NEW)**. All 17 NEW are the
  documented 60 s time-limit family at 167–196 s; ZERO XCTest failures; no
  failure under 167 s; none names any suite this bead touched. All seven
  touched/added suites pass.
- Mutation battery G01–G10 (batches 160–169) + re-cut F08 (batch 158):
  **11 KILLED / 0 SURVIVED / 0 ERROR.**
