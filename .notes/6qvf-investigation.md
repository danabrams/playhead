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
