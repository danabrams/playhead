# Playhead Ad Detection System — Technical Deep Dive

**Date:** April 2026
**Status:** Production pipeline, phases 1–8 landed

Playhead is an iOS podcast player whose core feature is automatic ad detection and skip. All processing runs **entirely on-device** — no audio, transcripts, or classification data ever leave the phone. This document describes the full detection pipeline in detail.

---

## 1. High-Level Architecture

The pipeline has two execution modes that run concurrently during playback:

### Hot Path (real-time, during playback)
```
Audio decode → Feature extraction → Lexical scan → Acoustic breaks → Classifier → AdWindow (candidate)
```
- Runs **90 seconds ahead** of the playhead
- Produces skip-ready `AdWindow` regions with `decisionState = .candidate`
- Optimized for latency: must emit before the listener reaches the ad

### Backfill (async, after transcription completes)
```
Final-pass transcript → FM coarse screening → FM refinement → Evidence fusion → Span decoding → Confirm/suppress
```
- Re-classifies using complete transcript + Foundation Model consensus
- Promotes candidates to `.confirmed` or `.suppressed`
- Budget-capped at **300 seconds** of FM compute per backfill run

### Session State Machine
```
queued → spooling → featuresReady → hotPathReady → backfill → complete
                                                          ↓
                                                       failed
```

---

## 2. Audio Ingestion & Feature Extraction

### Audio Decoding
- Podcast audio is decoded into **16 kHz mono Float32** shards
- Shards are streamed to the feature extractor as they become available

### Feature Windows
Each **2.0-second window** produces:

| Feature | Method | Purpose |
|---------|--------|---------|
| **RMS energy** | Log-scaled amplitude | Volume level, ad/content transitions |
| **Spectral flux** | FFT magnitude delta (1024-point FFT) | Timbral change detection (stingers, jingles) |
| **Pause probability** | RMS threshold at 0.03, smooth monotonic log-RMS curve | Silence/gap detection |
| **Music probability** | Apple `SNClassifySoundRequest` (SoundAnalysis framework) + acoustic fallback | Background music beds, jingle identification |
| **Speaker change proxy** | Acoustic heuristic (validated speaker labels deferred) | Host ↔ ad reader transitions |
| **Music bed change score** | Derivative of music probability | Onset/offset of ad music beds |

Feature extraction has been through 4 versioning iterations (v1–v4), with v2 recalibrating pause detection, v3 adding SoundAnalysis integration, and v4 adding seam-state checkpoints with retro-correction.

---

## 3. On-Device Transcription

**Framework:** Apple Speech (`SFSpeechRecognizer`), fully on-device.

### Chunking Strategy
| Parameter | Value |
|-----------|-------|
| Target chunk duration | **12 seconds** |
| Min chunk | **8 seconds** |
| Max chunk | **20 seconds** |
| Chunk overlap | **0.5 seconds** |
| VAD speech threshold | **0.5** |
| Lookahead buffer | **30 seconds** ahead of playhead |

Chunks are split at VAD-anchored pause boundaries. Near-playhead chunks are prioritized for hot-path coverage; remaining chunks are processed during idle/charging time (final-pass promotion).

### Atoms
The fundamental unit of transcript analysis is the **atom** — a single transcript chunk, typically 1–2 seconds of speech. Each atom has:
- **Stable identity:** `(analysisAssetId, transcriptVersion, atomOrdinal)`
- **Content:** `startTime, endTime, text, contentHash, speakerId`
- **Version tracking:** Transcript version is a SHA256 hash of ordered atom content (length-prefixed to prevent boundary ambiguity)

Atoms ensure stable identity across reprocessing and enable correction/training lineage via `TranscriptAlignmentMap`.

---

## 4. Lexical Scanning (Layer 1)

The lexical scanner is the fastest detection path and catches ~60–70% of ads alone.

### Pattern Categories
| Category | Examples | Weight |
|----------|----------|--------|
| **Sponsor** | Brand names from per-show sponsor lexicon | varies |
| **PromoCode** | "use code PODCAST20", discount patterns | 0.8–0.9 |
| **UrlCTA** | URLs, "dot com slash", vanity URLs | **0.95** (strong) |
| **PurchaseLanguage** | "sign up", "subscribe", "free trial" | 0.8 |
| **TransitionMarker** | "back to the show", "now where were we" | 0.8 |

### Decision Rules
- **Minimum 2 distinct pattern hits** to emit a candidate
- **Exception:** A single hit with weight ≥ 0.95 (URLs, disclosures) bypasses the 2-hit minimum
- **Merge gap:** Adjacent hits within **30 seconds** are merged into one candidate
- Output: `[LexicalCandidate]` with confidence score and evidence text

---

## 5. Acoustic Break Detection (Layer 2)

Detects structural transitions in the audio signal that often coincide with ad boundaries.

### Break Signal Types
| Signal | Trigger Condition | Weight |
|--------|-------------------|--------|
| **Energy drop** | RMS falls >35%, with ≥0.25 absolute difference, and source RMS ≥ 0.05 | 0.4 |
| **Energy rise** | RMS rises >35% | 0.4 |
| **Spectral spike** | Spectral flux above 80th percentile | 0.3 |
| **Pause cluster** | ≥2 consecutive windows with pause probability ≥ 0.6 | 0.3 |

Output: `[AcousticBreak]` with `(time, breakStrength 0.0–1.0, signals[])`

---

## 6. Music Bed Classification

Identifies background music patterns that are characteristic of ad reads (production beds, jingles, stingers).

**Framework:** Apple SoundAnalysis (`SNClassifySoundRequest`) with 2-second windows.

### Classification Levels
| Level | Threshold | Meaning |
|-------|-----------|---------|
| **none** | music probability < 0.15 | No detectable music |
| **background** | 0.15–0.6, amplitude < 70% local mean | Low-level bed under speech |
| **foreground** | probability ≥ 0.6, spectral flux > 0.3 baseline | Jingles, song clips, stingers |

Each classification produces `onsetScore` and `offsetScore` (0–1) indicating how strongly music is starting or ending — useful for boundary detection.

---

## 7. Hybrid Classifier (Layer 2)

Combines all signal sources into a single ad probability per region.

### Inputs
- Feature windows (RMS, spectral, music, pause, speaker, lexical density)
- Episode position (normalized 0–1)
- Per-show priors (known sponsors, ad slot positions, jingle fingerprints)

### Per-Show Priors
Learned across episodes of the same show:
- **Ad slot position priors** — normalized episode fractions where ads typically appear
- **Sponsor lexicon** — known brand names for this show
- **Jingle fingerprints** — audio fingerprints of recurring stingers
- **Trust weight** — scaled to observation count, saturates at 20 episodes

### Output
```
adProbability: 0.0–1.0
signalBreakdown: {
    lexicalScore, rmsDropScore, spectralChangeScore,
    musicScore, speakerChangeScore, priorScore
}
```

---

## 8. Foundation Model Classification (Layer 3)

**Framework:** Apple FoundationModels (iOS 26+) — on-device LLM
**Sampling:** Greedy (deterministic)
**Session management:** Fresh `LanguageModelSession` per call (prevents ~4000 token context accumulation)

### 8a. Coarse Screening

Classifies transcript windows into one of four dispositions:

| Disposition | Meaning |
|-------------|---------|
| `noAds` | No commercial content detected |
| `containsAd` | Commercial content present |
| `uncertain` | Insufficient signal |
| `abstain` | Safety refusal or model limitation |

Each disposition carries a **certainty band**: `weak`, `moderate`, or `strong`.

**Token budget:** ~3,700 tokens fixed overhead per window, leaving ~300–400 tokens for transcript content. All `@Guide` descriptions and unused enums are stripped for efficiency.

### 8b. Refinement

For windows flagged `containsAd`, a refinement pass determines:
- **Commercial intent:** paid, owned, affiliate, organic, unknown
- **Ownership:** thirdParty, show, network, guest, unknown
- **Boundary line refs:** first/last discourse unit
- **Boundary precision:** `usable` vs `precise`
- **Evidence anchors:** url, promoCode, ctaPhrase, disclosurePhrase, brandSpan

### 8c. Permissive Classifier (Pharma/Medical Fallback)

A separate classification path using Apple's `.permissiveContentTransformations` mode:
- **Why:** Standard FM path refuses to classify pharma/medical ads (safety classifier blocks them)
- **How:** Runs with relaxed safety, plain String output (not `@Generable` schemas)
- **Validated against:** 124-probe matrix covering CVS, Trulicity, Ozempic, Rinvoq, BetterHelp ad reads
- **Output:** Same `CoarseScreeningSchema` type — downstream consumers don't distinguish the path

### 8d. Prompt Redaction

Two-tier redaction policy:
- **Minimal tier:** For on-device FM calls — sponsor text never leaves device, minimal masking
- **Typed tier:** Fallback path with typed placeholders (`[DRUG_A]`, `[DRUG_B]`, etc.)

Key insight: Masking vaccine vocabulary AND disease names together bypasses the FM safety classifier while preserving ad-relevant signals (schedule + URL + brand app patterns).

### 8e. FM Backfill Modes

| Mode | Rescore existing candidates | Propose new regions |
|------|---------------------------|-------------------|
| `off` | No | No |
| `shadow` | No (telemetry only) | No |
| `rescoreOnly` | Yes | No |
| `proposalOnly` | No | Yes |
| `full` | Yes | Yes |

---

## 9. Evidence Catalog

Deterministic entity extraction that prevents FM hallucinations from contaminating downstream systems.

### Extraction (3 phases)
1. **Anchor evidence** — URLs, promo codes, disclosures (unconditional extraction)
2. **Commercial context window** — computed from anchor positions
3. **Context-dependent evidence** — CTAs and brand names extracted only within proximity of anchors

### Evidence Types
| Category | Example | Context-gated? |
|----------|---------|---------------|
| `url` | "betterhelp.com/podcast" | No |
| `promoCode` | "code PODCAST20" | No |
| `disclosurePhrase` | "sponsored by", "brought to you by" | No |
| `ctaPhrase` | "sign up today", "click the link" | Yes |
| `brandSpan` | "Athletic Greens" | Yes |

Each entry gets a stable `evidenceRef` integer (`[E0]`, `[E1]`, etc.) for FM prompt references, plus deduplication by `(category, normalizedText)`.

---

## 10. FM Consensus & Region Building

### Consensus Rule
- **Minimum 2 overlapping FM windows** required for consensus
- Overlap measured by IoU (Intersection over Union) with threshold **0.4**

### Consensus Strength
| Level | Score | Meaning |
|-------|-------|---------|
| `none` | 0.0 | No FM agreement |
| `low` | 0.35 | Single FM window or weak agreement |
| `medium` | 0.7 | Two+ windows agree |
| `high` | 1.0 | Strong multi-window agreement |

### Region Origins (bitwise)
Regions track which detection layers contributed:
- `lexical` (bit 0), `acoustic` (bit 1), `sponsor` (bit 2), `fingerprint` (bit 3), `foundationModel` (bit 4)

---

## 11. Ad Copy Fingerprinting

Cross-episode matching of recurring ad reads using text similarity.

### Algorithm
- **Jaccard similarity** on transcript windows (~30 atoms per window, stride = window/3)
- **Strong match** (Jaccard ≥ 0.8): Transfer full boundaries after anchor alignment validation
- **Normal match** (Jaccard 0.6–0.8): Seed hypothesis for further verification

### Anchor Alignment Validation
For strong matches, per-landmark drift is checked:
- **Max drift:** 10 seconds per landmark
- **Min aligned fraction:** 50% of landmarks must align
- Prevents false transfer from structurally similar but different ads

---

## 12. Atom Evidence & Anchoring (Phase 5)

Every atom gets annotated with its evidence provenance, enabling tap-to-explain in the UI.

### Three Anchor Paths

**Path A — FM Consensus:**
Regions with `fmConsensusStrength ≥ medium` (0.7+) directly anchor atoms.

**Path B — Evidence Catalog:**
Trustworthy categories only: url, promoCode, disclosurePhrase, ctaPhrase.
(brandSpan excluded — too noisy as a standalone anchor.)

**Path C — Corroboration (requires ALL THREE simultaneously):**
1. Single FM window with strength `low` (0.35) ≤ strength < `medium` (0.7)
2. AND acoustic break with strength ≥ 0.5
3. AND break within 2 atoms of the FM window

Neither FM nor acoustic signal alone anchors in Path C — they must coincide.

### Per-Atom Annotation
```
AtomEvidence {
    isAnchored: Bool                    // precision gate
    anchorProvenance: [AnchorRef]       // WHY anchored (FM, catalog, corroborated)
    hasAcousticBreakHint: Bool          // for boundary snap
    correctionMask: CorrectionState     // userVetoed | userConfirmed | none
}
```

**Key invariant:** No span can exist without at least one anchored atom.

---

## 13. Span Decoding & Boundary Resolution

### Span Decoder (MinimalContiguousSpanDecoder)

Fixed rule order for full determinism:

| Step | Rule | Parameters |
|------|------|------------|
| 1 | **FORM RUNS** | Contiguous anchored + non-vetoed atoms → candidate spans |
| 2 | **MERGE** | Gap < **3 seconds**, no veto or acoustic break in gap |
| 3 | **SPLIT** | Spans > **180 seconds** → split at longest internal gap (recursive) |
| 4 | **SNAP** | Snap edges to nearest acoustic break within ±**8 seconds** |
| 5 | **DROP** | Spans < **5 seconds** → discard micro-fragments |

**Invariants:**
- No span without upstream anchor
- `.userVetoed` atoms excluded from all candidate sets
- `.userConfirmed` atoms do NOT create spans alone (need anchor)
- `decode(atoms, id) == decode(atoms, id)` — fully deterministic

### Boundary Snapping (TimeBoundaryResolver)

Boundaries are refined using weighted cue scoring:

**Start boundary cues:**
| Cue | Weight |
|-----|--------|
| Pause/VAD | 0.25 |
| Speaker change proxy | 0.20 |
| Music bed change | 0.15 |
| Spectral change | 0.20 |
| Lexical density delta | 0.20 |

**End boundary cues:**
| Cue | Weight |
|-----|--------|
| Pause/VAD | 0.25 |
| Speaker change proxy | 0.20 |
| Music bed change | 0.15 |
| Spectral change | 0.15 |
| Explicit return marker | 0.25 |

**Snap parameters:**
- `lambda: 0.3` — distance penalty decay
- `minBoundaryScore: 0.3` — minimum score to snap
- `minImprovementOverOriginal: 0.1` — snap only if score improves by ≥10%

**Snap distances by anchor type:**
| Anchor | Start radius | End radius | Rationale |
|--------|-------------|------------|-----------|
| Disclosure | 5s | 15s | Start-anchored (disclosure at beginning) |
| Sponsor lexicon | 5s | 15s | Start-anchored |
| URL | 15s | 5s | End-anchored (URL at end of read) |
| Promo code | 15s | 5s | End-anchored |
| FM positive | 10s | 10s | Neutral |
| Transition marker | 15s | 5s | End-anchored |

---

## 14. Evidence Fusion & Confidence Scoring

### Evidence Sources (6 types)
| Source | Weight cap | Details |
|--------|-----------|---------|
| **Foundation Model** | **0.40** | Only `containsAd` dispositions contribute; noAds/uncertain/abstain silently dropped |
| **Classifier** (rule-based) | 0.30 | Legacy heuristic signal combination |
| **Fingerprint** | 0.25 | Cross-episode ad copy matches |
| **Lexical** | 0.20 | Pattern match signals |
| **Acoustic** | 0.20 | Break detection signals |
| **Catalog** | 0.20 | Evidence entity entries |

**FM positive-only rule:** Only `containsAd` dispositions contribute to fusion. Negative FM results are silently dropped — this prevents a single FM misfire from suppressing otherwise well-supported candidates.

### Decision Thresholds
| Threshold | Value | Action |
|-----------|-------|--------|
| Candidate emission | **0.40** | Emit as skip candidate in hot path |
| Confirmation | **0.70** | Auto-confirm in backfill |
| Auto-skip eligible | **0.75** | Promote to autoSkipEligible |
| Suppression | **0.25** | Suppress — insufficient evidence |

### Skip Eligibility Gate
Even after scoring, a gate can block action:
- `.eligible` — actionable
- `.blockedByEvidenceQuorum` — weak corroboration
- `.blockedByPolicy` — external policy prevents skip
- `.blockedByUserCorrection` — user previously vetoed

The gate blocks action but does **not** clamp the score — `skipConfidence` remains the honest probability estimate.

---

## 15. Span Hypothesis Engine

Manages the lifecycle of ad region hypotheses through evidence accumulation.

### Hypothesis Lifecycle
```
idle → seeded → accumulating → confirmed → closed
```

### Anchor Polarity
| Type | Direction | Rationale |
|------|-----------|-----------|
| Disclosure, sponsor | Start-anchored | Evidence appears at ad start |
| Promo code, transition | End-anchored | Evidence appears at ad end |
| FM positive | Neutral | Balanced search |

### Evidence Decay
Confidence decays exponentially from anchor timestamps:
```
score(t) = weight × exp(-decayRate × |t - anchorTime|)
```
This means evidence far from the anchor contributes less to the hypothesis.

---

## 16. Sponsor Knowledge Store (Phase 8)

Cross-episode memory of sponsors, building show-specific priors over time.

### Entity Lifecycle
```
candidate → quarantined → active → decayed
                                  ↘ blocked
```

### Promotion Rules
| Transition | Condition |
|------------|-----------|
| candidate → quarantined | First observation |
| quarantined → active | ≥2 confirmations, rollback rate < 30% |
| active → decayed | Rollback spike > 50% |

### Entity Types
sponsor, cta (call-to-action), url, disclosure

---

## 17. Key Design Decisions & Tradeoffs

### On-Device Mandate
All processing — transcription, classification, FM inference — runs on-device. This is a **legal requirement**, not a performance choice. No audio or text data is transmitted to any server.

### Precision Over Recall
The system is deliberately tuned for **high precision** (don't skip non-ads) over high recall (catch every ad). Key mechanisms:
- No span without an anchored atom
- FM consensus requires 2+ overlapping windows
- Corroboration path (Path C) requires 3 simultaneous signals
- User vetoes are permanent and override all automated signals

### Layered Detection
Each layer operates independently and contributes to a shared evidence ledger:
1. **Lexical** — fast, catches ~60–70% of ads, no ML required
2. **Acoustic** — structural transitions, ML-free
3. **Music bed** — SoundAnalysis framework, lightweight ML
4. **Classifier** — heuristic fusion of above signals
5. **Foundation Model** — on-device LLM for semantic understanding
6. **Fingerprint** — cross-episode matching for recurring ads

### FM Safety Workarounds
Apple's on-device FM refuses to classify pharma/medical content. The permissive classifier path with typed redaction (`[DRUG_A]`, `[DRUG_B]`) recovers these windows without losing ad-relevant signals. Validated against a 124-probe matrix of real pharma ad reads.

---

## 18. Summary of Models & Frameworks Used

| Component | Framework/Model | On-device? |
|-----------|----------------|------------|
| Speech transcription | Apple Speech (`SFSpeechRecognizer`) | Yes |
| Sound classification | Apple SoundAnalysis (`SNClassifySoundRequest`) | Yes |
| Audio features | Custom FFT (1024-point) + signal processing | Yes |
| Coarse screening | Apple FoundationModels (`SystemLanguageModel.default`) | Yes |
| Refinement | Apple FoundationModels (`SystemLanguageModel.default`) | Yes |
| Metadata extraction | Apple FoundationModels (`SystemLanguageModel.default`) | Yes |
| Permissive classification | Apple FoundationModels (permissive mode) | Yes |
| Ad classifier | Custom rule-based heuristic (CoreML slot reserved) | Yes |
| Fingerprinting | Jaccard similarity on atom windows | Yes |

---

## Appendix: Numeric Constants Reference

| Constant | Value | Context |
|----------|-------|---------|
| Feature window size | 2.0 s | Audio analysis granularity |
| Sample rate | 16,000 Hz | Audio decode target |
| FFT size | 1024 | Spectral analysis |
| Hot path lookahead | 90 s | Ahead of playhead |
| FM scan budget | 300 s | Max compute per backfill |
| Candidate threshold | 0.40 | Hot path emission |
| Confirmation threshold | 0.70 | Backfill auto-confirm |
| Auto-skip threshold | 0.75 | Promote to auto-skip |
| Suppression threshold | 0.25 | Below = suppress |
| FM consensus minimum | 2 windows | Overlapping agreement |
| FM IoU threshold | 0.4 | Region overlap measure |
| Energy drop threshold | 35% | RMS fractional change |
| Min absolute RMS diff | 0.25 | False positive gate |
| Pause probability threshold | 0.6 | Pause classification |
| Min pause cluster | 2 windows | Consecutive pauses |
| Music: none threshold | < 0.15 | No music detected |
| Music: foreground threshold | ≥ 0.6 | Jingle/stinger detected |
| Background amplitude ratio | 0.7 | Bed-under-speech gate |
| Lexical merge gap | 30 s | Adjacent hit merging |
| Min lexical hits | 2 | Pattern count for candidate |
| URL bypass weight | 0.95 | Single-hit override |
| Span merge gap | 3 s | Adjacent span merge |
| Boundary snap radius | 8 s | Max snap distance |
| Min span duration | 5 s | Drop below this |
| Max span duration | 180 s | Split above this |
| Boundary snap min score | 0.3 | Minimum to snap |
| Snap improvement threshold | 0.1 | Must improve by 10%+ |
| Transcription chunk target | 12 s | Chunk duration |
| Chunk overlap | 0.5 s | Boundary overlap |
| Fingerprint strong match | Jaccard ≥ 0.8 | Transfer boundaries |
| Fingerprint normal match | Jaccard 0.6–0.8 | Seed hypothesis |
| Max landmark drift | 10 s | Alignment validation |
| Min aligned fraction | 50% | Landmark coverage |
| FM weight cap | 0.40 | Max fusion contribution |
| Classifier weight cap | 0.30 | Legacy classifier cap |
| Fingerprint weight cap | 0.25 | Cross-episode cap |
| Lexical/acoustic/catalog cap | 0.20 | Per-source caps |
| Sponsor active threshold | 2 confirmations | Quarantine → active |
| Rollback spike threshold | 50% | Active → decayed |
| Evidence decay | exponential | From anchor timestamp |
| Boundary distance penalty | λ = 0.3 | Snap scoring decay |
