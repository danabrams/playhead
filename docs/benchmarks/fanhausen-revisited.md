# Ad Detection Benchmark: Fanhausen Revisited

Tracking ad detection metrics on the Conan O'Brien "Fanhausen Revisited"
episode (~16:30, 4 ads, 77 total ad seconds). Re-run
`RealEpisodeBenchmarkTests` and update this file after each phase milestone.

## Ground truth

| # | Time | Type | Skip conf | Advertiser |
|---|---|---|---|---|
| 1 | 0:00-0:26 | sponsor (pre-roll) | 100% | CVS |
| 2 | 0:30-0:56 | crossPromo | 80% | Kelly Ripa "Let's Talk Off Camera" |
| 3 | 15:52-15:59 | integration (credits) | 50% | SiriusXM |
| 4 | 16:11-16:29 | crossPromo | 80% | Kelly Ripa (repeat) |

Known false-positive signal (should NOT be flagged as ad):
- **3:17-3:26**: `teamcoco.com` — first-party call-in instructions

## Benchmark history

| Phase | Date | AdWindow recall | Ad-sec coverage | Evidence recall | Evidence precision | Lexical recall | Weighted recall |
|---|---|---|---|---|---|---|---|
| Phase 2 baseline | 2026-04-06 | 0% | 2.6% | 50% | 57% | 0% | 48% |

## Per-ad span coverage over time

| Phase | CVS (0:00-0:26) | Kelly Ripa #1 | SiriusXM (15:52-15:59) | Kelly Ripa #2 |
|---|---|---|---|---|
| Phase 2 baseline | 2% | 0% (missed) | 11% | 0% (missed) |

## Phase 2 baseline notes (2026-04-06)

- Evidence catalog found `cvs.com`, `siriusxm.com` URLs (2/4 ads have URL evidence)
- Both Kelly Ripa cross-promos have ZERO lexical signals — architectural gap for Phase 3
- `teamcoco.com` is a false positive (first-party domain) — Phase 8 SponsorKnowledgeStore territory
- LexicalScanner produced 0 merged candidates despite URL hits — needs investigation (min hits threshold?)
- User would still hear 97.4% of ad content if we shipped this

## Expected improvements per upcoming phase

| Phase | Expected improvement |
|---|---|
| Path A (fix minHitsForCandidate) | LexicalScanner recall 0% → ~50%, AdWindow recall climbs |
| Path A (fix pauseProbability bug) | AcousticBreakDetector starts firing on real audio |
| Phase 3 (FM semantic scanner) | Evidence catalog recall 50% → 100% (catches Kelly Ripa) |
| Phase 4 (RegionProposalBuilder) | Ad-second coverage should climb from ~2% toward 80%+ |
| Phase 5 (MinimalContiguousSpanDecoder) | Span coverage per ad should reach 80-100% |
| Phase 8 (SponsorKnowledgeStore) | Evidence precision 57% → ~100% (teamcoco filtered) |
