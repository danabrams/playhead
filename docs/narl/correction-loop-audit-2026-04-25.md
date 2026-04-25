# Correction-loop audit (playhead-gtt9.25) — 2026-04-25

## TL;DR

The catalog ingest/match loop **does fire correctly in unit tests**, on the
production hot path the **wiring is honest end-to-end**, and the matcher's
**threshold and time-invariance contracts are sound**. The 3.4% catalog
firing rate cited in the bead (`catalog=5/147`) is **a misleading premise**:
that figure comes from the 2026-04-23 NARL eval, **before** AdCatalogStore
was wired into production (`gtt9.16/9.17` landed 2026-04-24), AND the figure
counts the in-pipeline `EvidenceCatalog` (sponsor-token catalog from
`EvidenceCatalogBuilder`), **not** the cross-episode `AdCatalogStore`. Those
are different "catalogs" sharing a common `.catalog` evidence-source label.

Outcome: **(B) — specific gaps found**. Two concrete, bounded gaps prevent
the loop from being measurable on real corpora today, and a third design
question is worth a follow-up. None are bugs in `gtt9.13/9.16/9.17`; all are
about **the eval pipeline being blind to the AdCatalogStore signal**.

## Methodology

This audit is investigation-only — no production code was changed, no
xcodebuild was run (parallelism ceiling reserved for sibling agents).

What I read:

- `Playhead/Services/AdDetection/AdCatalogStore.swift` (gtt9.13)
- `Playhead/Services/AdDetection/AcousticFingerprint.swift` (gtt9.13 + gtt9.17)
- `Playhead/Services/AdDetection/AdDetectionService.swift` — the full ingress
  (`recordUserMarkedAd`, line 635-727; `runBackfill` autoSkipEligible
  path, line 1841-1866) and egress (`runBackfill` per-span query, line
  1737-1758; `buildEvidenceLedger`, line 2097-2118) call sites.
- `Playhead/Services/AdDetection/BackfillEvidenceFusion.swift` —
  `FusionWeightConfig.catalogCap = 0.2` (line 86).
- `Playhead/Services/AdDetection/EvidenceCatalogBuilder.swift` — the
  *other* "catalog" (transcript-derived sponsor tokens). Source label:
  `.catalog`. NOT cross-episode.
- `Playhead/App/PlayheadRuntime.swift` — `AdCatalogStore` constructed at
  app boot (line 495-503) into Application Support; `injectUserMarkedAd
  → recordUserMarkedAd` (line 1429-1447).
- `PlayheadTests/Services/AdDetection/AdCatalogStoreTests.swift` (12
  tests, exhaustive on store contract).
- `PlayheadTests/Services/AdDetection/AdCatalogWiringTests.swift` (5
  integration tests on production wiring).
- `PlayheadTests/Services/ReplaySimulator/ReplayMetrics.swift` —
  `FrozenTrace` and `FrozenEvidenceEntry` schemas.
- `PlayheadTests/Services/ReplaySimulator/NarlEval/NarlReplayPredictor.swift`
  — confirms NARL's counterfactual replay reasons over `trace.evidenceCatalog`
  (transcript catalog), never over `AdCatalogStore`.
- `Playhead/Views/Settings/CorpusExporter.swift` — exports assets, spans,
  corrections, and shadow-decisions. Does NOT export `AdCatalogStore`
  contents.
- The 2026-04-23 NARL real-data findings doc (origin of `catalog=5/147`).
- The 2026-04-25 NARL eval (`.eval-out/narl/20260425-160022-E1D7EB/`).
- The 2026-04-25 capture xcappdata (`/Users/dabrams/playhead/.captures/2026-04-25/com.playhead.app 2026-04-25 07:43.49.095.xcappdata/`).

What I did *not* do:

- Run any backfill against a real captured episode (would require
  xcodebuild — barred by the bead).
- Inspect a real-device `ad_catalog.sqlite` (file is in Application
  Support, which is not part of any captured xcappdata bundle today).

## The two-catalog confusion

There are **two different things both called "catalog"** in this codebase.
Disambiguate before reading any further:

| Name in code | Type | Built when | Persists | Source label in fusion ledger |
|---|---|---|---|---|
| `EvidenceCatalog` (built by `EvidenceCatalogBuilder`) | In-pipeline list of sponsor tokens / URLs / promo codes / disclosures, derived deterministically from transcript atoms in **this** episode | Every backfill, before FM | No — rebuilt each run | `.catalog` |
| `AdCatalogStore` | SQLite table of `(showId, acousticFingerprint, transcriptSnippet, sponsorTokens, originalConfidence)` rows accumulated **across** episodes via auto-skip + user-correction ingress | App launch + every fusion path that gates `.autoSkipEligible` + every user correction | Yes — `Application Support/AdCatalog/ad_catalog.sqlite` | `.catalog` |

The fusion ledger uses the SAME source string (`.catalog`) for both —
`buildCatalogLedgerEntries(...)` for in-pipeline `EvidenceEntry`s, plus an
extra `EvidenceLedgerEntry(source: .catalog, ...)` appended when
`AdCatalogStore.matches` returns ≥ floor (`AdDetectionService.swift:2111`).
NARL's `FrozenEvidenceEntry` schema (which only has `source: String`) cannot
distinguish them.

This means the bead's premise — *"NARL eval shows `catalog=5` out of 147
windows (3.4%) ... either catalog isn't being populated, matcher is too
strict, or corpus lacks repeat ads"* — is examining the **transcript
sponsor catalog**, not the cross-episode fingerprint catalog. The
fingerprint catalog is **structurally invisible** to NARL.

## Findings

### Q1. Is the AdCatalogStore being populated?

**Cannot be measured from existing artifacts. No `ad_catalog.sqlite` file
is present in any captured `xcappdata` bundle.**

What I checked:

- `find .captures/2026-04-23 -name "ad_catalog*"` → 0 files.
- `find .captures/2026-04-25 -name "ad_catalog*"` → 0 files.
- The 2026-04-25 capture only includes `AppData/Documents/`. No
  `Library/Application Support/AdCatalog/` is part of the bundle.

Why this is the answer:

- Xcode's "download container" puts `Documents/` and `Library/Caches/` in
  the xcappdata, but `Application Support/` is not always pulled in.
  Even when it is, the corpus exporter (`CorpusExporter.swift`) does
  not write catalog rows to the JSONL stream.
- The 04-25 corpus export does contain **11 `correction` events** across
  3 distinct `analysisAssetId`s (3 on flightcast/DoaC, 8 on
  simplecast/Conan, all on a single Conan asset
  `E8F0F867-...`).
- `recordUserMarkedAd` (the path those 11 corrections take) inserts into
  `AdCatalogStore` after fingerprinting the user-marked feature windows
  (`AdDetectionService.swift:707-718`). So **on the device that captured
  this corpus, the catalog should now contain at least 11 entries** —
  but we have no artifact to confirm.

What we *do* know about the wiring being correct:

- `AdCatalogWiringTests.autoSkipEligibleInsertsCatalogEntry` ✓ passes
  (`gtt9.17` was committed only when this was green).
- `AdCatalogWiringTests.priorEntryMatchesSubsequentWindow` ✓ passes —
  episode 1 backfill seeds the catalog, episode 2 backfill (fresh
  analysis store, same fingerprint pattern) reads
  `lastCatalogMatchSimilarityForTesting() ≥ defaultSimilarityFloor`
  (0.80).
- `AdCatalogWiringTests.zeroSignalYieldsNoCatalogInsert` ✓ — silent /
  zero-fingerprint windows are correctly rejected.
- `recordUserMarkedAd` end-to-end has unit-test coverage of the SQLite
  insert through `AdCatalogStoreTests.correctionToCatalogToSignalIntegration`.

### Q2. Is the matcher too strict?

**No, on the contract level. The 0.80 cosine floor and the time-invariant
fingerprint design are sound.**

Time invariance: `AcousticFingerprint.fromFeatureWindows` summarizes 8
feature streams × 8 statistics into a 64-float vector. **No window
timestamps enter the summary**, so the same creative played at minute 5 of
ep1 and minute 23 of ep2 produces the same fingerprint
(`AcousticFingerprintFromWindowsTests.identicalMatchesAboveFloor` ✓).

Threshold sanity: cosine similarity for non-negative L2-normalized vectors
falls in `[0, 1]`. The 0.80 floor demands ~80% directional alignment.
`AcousticFingerprintFromWindowsTests.allZeroYieldsZero` confirms silent vs.
loud below 0.80; `correctionToCatalogToSignalIntegration` confirms a 5%
perturbation on 4-of-64 dims still clears the floor.

What might still be too strict, but I cannot prove from artifacts:

- The fingerprint is computed from **AcousticFeaturePipeline** outputs
  (rms, spectralFlux, musicProbability, speakerChangeProxyScore, etc.)
  *averaged* over the span. A 30-second sponsor read with the same VO
  will produce nearly-identical 8×8 summary stats; a 60-second mid-roll
  with a different mix can drift below 0.80 depending on host bed
  variation. This is a known design tradeoff (see
  `AcousticFingerprint.swift:284-321` header comments) — gtt9.12 may
  ship a richer feature vector if needed.

### Q3. Threshold tuning — is catalog-derived evidence weight=0 in fusion?

**No. The catalog ledger entry from AdCatalogStore matches gets
weight = `similarity × catalogCap` (≈ 0.16-0.20).** Reference:
`AdDetectionService.swift:2111-2118`:

```swift
if catalogMatchSimilarity >= AdCatalogStore.defaultSimilarityFloor {
    let weight = Double(catalogMatchSimilarity) * fusionConfig.catalogCap
    catalogLedgerEntries.append(EvidenceLedgerEntry(
        source: .catalog,
        weight: weight,
        detail: .catalog(entryCount: 1)
    ))
}
```

`fusionConfig.catalogCap = 0.2` by default
(`BackfillEvidenceFusion.swift:86`). So a perfect match gets weight 0.20;
a borderline 0.80 match gets weight 0.16. Both are above 0.

The `gtt9.4.2` shadow weight=0 spike was a **separate issue** — the
shadow-classifier exporter wrote `weight: 0` for non-positive shadow
decisions. That bug is unrelated to AdCatalogStore.

### Q4. False-positive rate on real-data corpora

**Cannot be measured. The catalog's match telemetry is not exported to any
analyzable artifact.**

Two specific gaps:

1. `lastCatalogMatchSimilarityForTesting()` is `#if DEBUG`-style and the
   value is not persisted to the asset row, decision log, or shadow log.
2. `FrozenTrace.evidenceCatalog` (the NARL replay corpus) carries
   transcript-catalog entries only. A `.catalog` source entry from an
   AdCatalogStore match is added to the fusion ledger at runtime but never
   serialized into a persistable form that the corpus exporter pulls from.

Cross-checking `FrozenTrace.evidenceCatalog` source labels across all 64
fixtures in `PlayheadTests/Fixtures/NarlEval/`:

| Source | Count |
|---|---|
| classifier | 211 |
| shadow:allEnabledShadow | 268 |
| metadata | 44 |
| fm | 30 |
| lexical | 21 |
| catalog | **20** |
| acoustic | 9 |

The 20 `.catalog` entries are all from `EvidenceCatalogBuilder` (sponsor
tokens). Zero entries come from AdCatalogStore matches. This will remain
true even after AdCatalogStore starts firing — until the export schema is
updated.

### Q5. Cross-episode fingerprint reuse semantics — `(showId, content)` or just content?

**`(showId, content)` with a null-show admit lane.** Reference:
`AdCatalogStore.swift:331-386`. The query is:

```sql
SELECT ... FROM ad_catalog_entries WHERE show_id = ? OR show_id IS NULL
```

Per `AdCatalogStoreTests.showScoping`:

- An entry inserted with `showId = "show-a"` matches queries against
  `show-a` and queries against `nil`, but NOT queries against `show-b`.
- An entry inserted with `showId = nil` matches queries against ANY show
  (acts as a global catch-all).

The null-show lane is the design release valve: if a span lacks
attribution at insert time, the entry still benefits future queries. But
**there is currently no cross-show fingerprinting** — a Squarespace ad in
Conan does not promote a Squarespace ad in Diary of a CEO unless one of
them was inserted with `showId = nil`.

This matches the bead's "Out of scope" note (cross-show is lever D, a
separate bead). Not a gap; a design choice.

### Q6. Deduplication — replay correction twice → one fingerprint or two?

**Two.** Each `insert(...)` convenience call constructs a `CatalogEntry`
with `id: UUID()` (new UUID per call). The base `insert(entry:)` uses
`INSERT OR REPLACE INTO ... ON id` — but since the id is fresh, no replace
ever occurs.

`recordUserMarkedAd` (`AdDetectionService.swift:710-718`) calls
`adCatalogStore.insert(showId:..., episodePosition: .unknown, ...)` —
the field-wise convenience overload, which generates a fresh UUID at
`AdCatalogStore.swift:301-310`. So calling it twice with byte-identical
arguments yields two rows with two different ids and identical
fingerprints.

**Implication for the matcher**: the second-best match becomes a duplicate
of the first. `matches(...)` returns both, and the fusion path takes
`matches.first?.similarity` only — no double-counting in the fusion
weight. So duplicates are **not a fusion-weight bug**, but they:

- Waste catalog rows (linear with redundant corrections).
- Distort `originalConfidence` aggregates (each row has its own value).
- Slow the linear scan (capped at 5,000 entries — likely fine for years).

**Not a precision/recall bug today, but it is tech debt.**

## Conclusion: outcome (B) — specific gaps found

Three concrete follow-ups, ordered by how much they unblock the audit
methodology:

### Gap 1 (HIGH): AdCatalogStore signal is invisible to NARL eval

**Problem**: `FrozenTrace.evidenceCatalog` schema cannot distinguish
`AdCatalogStore` matches from `EvidenceCatalogBuilder` entries (both use
source `.catalog`). And because the corpus exporter doesn't write per-span
match diagnostics from `AdDetectionService.lastCatalogMatchSimilarity`,
the AdCatalogStore signal is **structurally absent** from anything NARL
sees.

**Fix path**: extend the corpus export schema with a per-span
`catalogStoreMatchSimilarity: Double?` field, populated from
`AdDetectionService.lastCatalogMatchSimilarity` (or a per-span variant).
NARL replay can then attribute `.catalog` ledger entries to one of
`{transcript, acousticStore}` and the eval report can split out a
catalog-store firing-rate column. This is the only way "is the catalog
firing on real episodes?" becomes answerable from artifacts.

### Gap 2 (MEDIUM): No way to inspect the device catalog from a capture

**Problem**: `ad_catalog.sqlite` lives in `Application Support/AdCatalog/`
and is not pulled into xcappdata bundles by Xcode's "download container"
flow under default settings. There is also no in-app debug action that
exports catalog rows.

**Fix path**: small additive — add a `catalog-export.<timestamp>.jsonl`
artifact alongside the existing corpus export, listing
`(id, createdAt, showId, episodePosition, durationSec, sponsorTokens,
originalConfidence, fingerprintHash)`. Fingerprint blob can stay
on-device; a SHA hash is enough to verify "same fingerprint reappears
across episodes."

### Gap 3 (LOW): Deduplication is content-fingerprint blind

**Problem**: replaying or duplicating corrections produces duplicate
catalog rows. Today this only inflates row count, not fusion weight.

**Fix path**: change the convenience `insert(...)` to compute a
content-derived id (e.g.
`UUID(uuidString: SHA-256(fingerprint.data || showId).prefix(32))`) and
let `INSERT OR REPLACE` collapse duplicates. The schema already supports
this (id is the PK); only the convenience init needs updating. Tech debt,
not blocking.

### Why I cannot conclude (A)

I cannot honestly close this bead as "loop is tight enough" because the
loop's *empirical* tightness is unmeasurable from current artifacts.
The unit tests prove the *contract* is correct; the corpus tells us
*nothing* about whether the contract is actually delivering matches in
the wild. Closing on contract proof alone would be exactly the kind of
overconfidence the bead is trying to prevent.

## Out-of-scope notes

- **Cross-show fingerprinting**: confirmed it's a separate concern — Q5
  documented the current `(showId, content)` semantics. Lever D, separate
  bead, not addressed here.
- **`vjxc` (banner evidence catalog)**: confirmed it uses
  `EvidenceCatalogBuilder`'s `EvidenceCatalog` (transcript-derived
  sponsor tokens), NOT `AdCatalogStore`. The two share no infrastructure
  beyond the `.catalog` source label string.
- **Catalog UI / management**: Not audited. There is no in-app surface
  to view, clear, or correct catalog entries today. Out of scope.
- **`gtt9.12` acoustic feature richer fingerprints**: noted as future
  work in the `AcousticFingerprint` header. Not audited.
- **Performance / linear-scan cost**: the 5,000-row cap and per-query
  cosine sweep are documented as "small catalogs are fine; ANN later."
  Not audited at scale.

## References

- `playhead-gtt9.13` — AdCatalogStore + AcousticFingerprint shipped.
- `playhead-gtt9.16` — AcousticFeaturePipeline production wiring.
- `playhead-gtt9.17` — AdCatalogStore ingress + egress production wiring
  (commits `501bc59`, `069b724`, both 2026-04-24).
- `playhead-gtt9.4.2` — `docs/narl/2026-04-24-shadow-weight-zero-spike.md`
  (separate, unrelated to AdCatalogStore — confirmed).
- `vjxc` — banner evidence catalog (PR #7, 2026-04-25), uses
  `EvidenceCatalog`, not `AdCatalogStore`.
- 2026-04-23 NARL real-data findings:
  `docs/narl/2026-04-23-real-data-findings.md`. Origin of the
  `catalog=5/147` figure — pre-`gtt9.16/9.17`, counts the in-pipeline
  catalog only.
- 2026-04-25 NARL eval: `.eval-out/narl/20260425-160022-E1D7EB/` — does
  not include evidence-source counts in either `report.md` or
  `report.json`; cannot independently verify a more recent figure.
