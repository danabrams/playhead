# Fixtures Licensing (playhead-ym57)

Status: **DRAFT / SYNTHETIC-PLACEHOLDER FALLBACK**

This document records the rights basis for every fixture referenced in
`fixtures-manifest.json`. No real podcast audio is committed to this repo
until the Locked-Core Licensing gate below is signed off by an authorized
approver.

## Policy

The device-lab fixture substrate requires a deterministic locked-core of 8
fixtures plus a rotating pool. Because Playhead performs all transcription
and classification strictly on-device, the fixtures themselves are used only
for regression gates and device-lab benchmarks — they are never redistributed
beyond the CI environment. Nevertheless, every real captured fixture must be
covered by one of:

1. **Explicit written license** from the podcast's rights-holder, scoped to
   internal engineering use and regression testing. (Preferred.)
2. **Public domain / Creative Commons** attribution, with the license file
   linked in the per-fixture section below.
3. **Internally produced audio** (recorded by the Playhead team, or generated
   synthetically). This is the fallback used when external licensing is
   unavailable or delayed.

Per bead `playhead-ym57`:
> If external licensing is unavailable or delayed at Phase 1 acceptance time,
> this bead ships with all 8 locked-core slots filled by internally-produced
> audio. The fixture matters for its taxonomy axes, not its source identity.

All 8 locked-core slots are currently filled by **synthetic byte-deterministic
placeholders** produced by
`PlayheadTests/Fixtures/Corpus/Tools/SyntheticFixtureGenerator.swift`. These
are sine-wave WAV files whose duration is 0.5 s on disk; each slot's
`durationSec` in the manifest records the *intended* taxonomic duration (the
axis being covered), while `syntheticDurationSec` records the actual on-disk
duration. Synthetic fixtures satisfy every integrity-gate property (file
exists, SHA-256 matches, slot is filled, taxonomy axes are labeled) without
requiring legal sign-off.

## Locked-Core 8

<a id="fixture-01"></a>
### fixture-01
- **ID**: `fixture-01-30min-clean-speech`
- **Taxonomy**: 30m, sparse, medium, mid-roll, en-US, clean-speech, DAI=false
- **Source**: SYNTHETIC PLACEHOLDER (ym57 fallback)
- **Licensing basis**: Internally generated synthetic sine-wave audio; no
  third-party rights implicated.
- **Legal approver**: _pending_
- **Date-of-approval memo**: _pending_

<a id="fixture-02"></a>
### fixture-02
- **ID**: `fixture-02-60min-music-bed`
- **Taxonomy**: 60m, sparse, medium, mid-roll, en-US, music-bed, DAI=false
- **Source**: SYNTHETIC PLACEHOLDER (ym57 fallback)
- **Licensing basis**: Internally generated synthetic sine-wave audio; no
  third-party rights implicated.
- **Legal approver**: _pending_
- **Date-of-approval memo**: _pending_

<a id="fixture-03"></a>
### fixture-03
- **ID**: `fixture-03-15min-language-unsupported-zh`
- **Taxonomy**: 15m, none, none, none, zh-Hans, clean-speech, DAI=false
- **Source**: SYNTHETIC PLACEHOLDER (ym57 fallback)
- **Licensing basis**: Internally generated synthetic sine-wave audio; no
  third-party rights implicated.
- **Legal approver**: _pending_
- **Date-of-approval memo**: _pending_

<a id="fixture-04"></a>
### fixture-04
- **ID**: `fixture-04-90min-dynamic-insertion`
- **Taxonomy**: 90m, rich, high, mixed, en-US, clean-speech, DAI=true
- **Source**: SYNTHETIC PLACEHOLDER (ym57 fallback)
- **Licensing basis**: Internally generated synthetic sine-wave audio; no
  third-party rights implicated.
- **Legal approver**: _pending_
- **Date-of-approval memo**: _pending_

<a id="fixture-05"></a>
### fixture-05
- **ID**: `fixture-05-45min-poor-audio`
- **Taxonomy**: 45m, sparse, low, mid-roll, en-US, poor-remote, DAI=false
- **Source**: SYNTHETIC PLACEHOLDER (ym57 fallback)
- **Licensing basis**: Internally generated synthetic sine-wave audio; no
  third-party rights implicated.
- **Legal approver**: _pending_
- **Date-of-approval memo**: _pending_

<a id="fixture-06"></a>
### fixture-06
- **ID**: `fixture-06-30min-chapter-rich`
- **Taxonomy**: 30m, rich, medium, mid-roll, en-US, clean-speech, DAI=false
- **Source**: SYNTHETIC PLACEHOLDER (ym57 fallback)
- **Licensing basis**: Internally generated synthetic sine-wave audio; no
  third-party rights implicated.
- **Legal approver**: _pending_
- **Date-of-approval memo**: _pending_

<a id="fixture-07"></a>
### fixture-07
- **ID**: `fixture-07-60min-multi-break`
- **Taxonomy**: 60m, sparse, high, mixed, en-US, clean-speech, DAI=false
- **Source**: SYNTHETIC PLACEHOLDER (ym57 fallback)
- **Licensing basis**: Internally generated synthetic sine-wave audio; no
  third-party rights implicated.
- **Legal approver**: _pending_
- **Date-of-approval memo**: _pending_

<a id="fixture-08"></a>
### fixture-08
- **ID**: `fixture-08-90min-repeat-download-warm-resume`
- **Taxonomy**: 90m, sparse, medium, mid-roll, en-US, clean-speech, DAI=false
- **Source**: SYNTHETIC PLACEHOLDER (ym57 fallback)
- **Licensing basis**: Internally generated synthetic sine-wave audio; no
  third-party rights implicated.
- **Legal approver**: _pending_
- **Date-of-approval memo**: _pending_

## Locked-Core Licensing Sign-Off

Replacing a synthetic placeholder with captured audio requires all three
columns filled for that slot:

| Slot | Real source (feed+episode) | Licensing basis | Approver | Date |
|------|----------------------------|-----------------|----------|------|
| 1    | _pending_                  | _pending_       | _pending_| _pending_ |
| 2    | _pending_                  | _pending_       | _pending_| _pending_ |
| 3    | _pending_                  | _pending_       | _pending_| _pending_ |
| 4    | _pending_                  | _pending_       | _pending_| _pending_ |
| 5    | _pending_                  | _pending_       | _pending_| _pending_ |
| 6    | _pending_                  | _pending_       | _pending_| _pending_ |
| 7    | _pending_                  | _pending_       | _pending_| _pending_ |
| 8    | _pending_                  | _pending_       | _pending_| _pending_ |

## Rotating-Pool Policy

Rotating fixtures are selected weekly from a declared taxonomy pool
(`scripts/rotate-fixtures.swift`). Each rotating fixture must satisfy the
same licensing gate as a locked-core fixture before being committed. The
pool may include synthetic entries to maintain stratification while the
licensing queue clears.
