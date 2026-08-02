# playhead-gard — investigation notes

Trust is per-show and GLOBAL ACROSS DETECTORS. Dan's decision (2026-08-01): **"per detector is
good"**. This file records what was READ before anything was changed, so the design below is
traceable to source rather than to inference.

## 1. The current shape

`podcast_profiles` carries ONE trust triple per show:

| column | role |
| --- | --- |
| `skipTrustScore` | scalar, `[0, 1]` |
| `mode` | `shadow` \| `manual` \| `auto` |
| `recentFalseSkipSignals` | integer counter, drives demotion |
| `implicitFalsePositiveCount` | lifetime tally, drives nothing |

`TrustScoringService.resolveMode(podcastId:)` reads `mode` and hands it to
`SkipOrchestrator.beginEpisode`, which stores it in `activeSkipMode` — **one value per episode,
consulted by every window regardless of which detector produced it**
(`SkipOrchestrator.swift:1920`, gate at `:6538`).

## 2. Who moves the numbers, measured

`grep -rn '<name>' --include='*.swift' Playhead/` — production target only, comments excluded:

| method | direction | production callers |
| --- | --- | --- |
| `recordFalseSkipSignal` | trust −0.10, signals +1, **demote** | 4 (`SkipOrchestrator` 4430 / 5093 / 5234, + `AdDetectionService` listen-rewind) |
| `recordWeakFalseSkipSignal` | trust −0.05, signals +1, **demote** | 2 (`SkipOrchestrator:4864`, `AdDetectionService:9842/9850`) |
| `recordFalseNegativeSignal` | trust −0.10 | 3 (`NowPlayingViewModel:368`, `TranscriptPeekView:699/736`, `SkipOrchestrator:5746`) |
| `setUserOverride` | sets `mode` directly | Settings / Now Playing control |
| `recordSuccessfulObservation` | trust +0.10, obs +1, **promote** | **ZERO** |
| `decayFalseSignals` | signals halved | **ZERO** |

The last two rows are the finding. **Every production path moves trust DOWN.** Nothing in the
shipped app calls `recordSuccessfulObservation` or `decayFalseSignals`, so:

- `evaluatePromotion` has no production caller at all — `shadow → manual → auto` never happens.
- `recentFalseSkipSignals` is a **monotonically increasing** counter. It never decays.
- The only way a show is ever in `auto` is `setUserOverride`, i.e. the user set it by hand.

## 3. Manual mode IS a one-way door — and the bead understated it

The bead asked: escaping `manual` needs `recentFalseSkipSignals == 0`, reset only on a correct
observation; does a confirmed banner count as one?

**It does not — it counts as a FALSE NEGATIVE and lowers trust further.**
`SkipOrchestrator.acceptSuggestedSkip` (`:5738-5751`) calls
`trustService.recordFalseNegativeSignal(podcastId:privacy:.explicitBannerFeedback)`, which
subtracts `falseSignalPenalty` (0.10) from `skipTrustScore` and touches neither
`recentFalseSkipSignals` nor `mode`.

So the door is shut three ways over: the counter never decays, trust never rises, and the one
gesture that looks like positive evidence is wired to the negative recorder. `setUserOverride` is
the sole escape, and it does not clear `recentFalseSkipSignals` either — so a hand-restored `auto`
show carrying 3 signals demotes again on the very next veto (`autoToManualFalseSignals == 2`).

Why the banner gesture is positive evidence at all: a suggest-tier card asks "is this an ad?" and
the user answered **yes**. That affirms the DETECTOR'S PRESENCE CLAIM. It is recorded as a miss
because the *skip surface* did not fire — a true statement about the surface, and the wrong lesson
about the detector.

Suggest banners are **not** mode-gated (`registerSuggestedWindow` / `emitSuggestBannersOnPlayheadEntry`
consult playback position, never `activeSkipMode`), so the gesture is reachable from `manual` and
`shadow`. That is what makes it usable as the escape.

## 4. The tiers that already exist — do not invent a parallel scale

- `AutoSkipEdgeAnchor` (`AutoSkipEdgePadding.swift`): `rediffByteExact` / `stingerSnapped` /
  `unanchored`, persisted PER EDGE on `ad_windows` (`startEdgeAnchor`, `endEdgeAnchor`, playhead-hdgk).
- `ExtentAnchorTier` (`SpanExtentSupport.swift`): `none` (0) / `corroborated` (1) /
  `deterministic` (2). A span is worth its WEAKER edge (`SpanExtentSupport.tier`).
- playhead-6qvf split `.rediffSlotChroma` out of `.rediffSlot`, so `deterministic` now means the
  BYTE differ and nothing else; the ~1 s chroma fallback resolves `.unanchored` and stays markOnly.

Dan's three vetoed windows were `segmentAggregated`, confidence 0.40–0.42, **both edges
unanchored** → `ExtentAnchorTier.none`. The byte-exact rediff spans they suppressed are
`.deterministic`. The certainty of what was skipped is therefore already a persisted, per-row,
post-6qvf quantity — no new scale is needed.

## 5. Detector classes reachable at skip time

Derivable from the persisted `AdWindow` row alone:

| class | how it is recognised |
| --- | --- |
| `rediffByteExact` | both edge anchors `rediffByteExact` (`SpanExtentSupport.tier == .deterministic`) |
| `segmentAggregated` | `boundaryState == "segmentAggregated"` (`AdBoundaryState`) |
| `userAsserted` | `boundaryState` decodes as `UserSpanAssertion` (`userMarked` / `userConfirmedSuggested`) |
| `fusion` | everything else — `lexical`, `acousticRefined`, unknown producers |

## 6. Migration precedent

`playhead-6qvf` added `AnchorRef.rediffSlotChroma` as a NEW value that fails the existing
`carriesRediffByteExactWidth` predicate — so every consumer that does not know about it lands on
the CONSERVATIVE branch (markOnly) rather than misreading it as deterministic. The persistence
analogue: add a nullable column an older binary never selects, keep the legacy scalar columns
written exactly as before, and make the ABSENCE of the new value mean "fall back to the legacy
scalar".
