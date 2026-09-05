// AdDetectionService.swift
// Composes the detection layers and outputs AdWindows.
//
// Hot path: lexical scan -> acoustic boundary refinement -> classifier
//   Produces skip-ready AdWindows with decisionState = .candidate
//   ahead of the playhead.
//
// Backfill: re-classify on final-pass transcript -> metadata extraction
//   -> prior update -> promote to .confirmed or .suppressed.
//
// Results keyed by analysisAssetId in SQLite. Different audio bytes =
// different AnalysisAsset = fresh analysis (no stale cache).

import Foundation
import OSLog

// MARK: - Detection Configuration

struct AdDetectionConfig: Sendable {
    /// Minimum classifier probability to emit a candidate AdWindow.
    let candidateThreshold: Double
    /// Minimum classifier probability to auto-confirm during backfill.
    let confirmationThreshold: Double
    /// Maximum probability below which a candidate is suppressed.
    let suppressionThreshold: Double
    /// How far ahead of the playhead (seconds) to run hot-path detection.
    let hotPathLookahead: TimeInterval
    /// Detector version tag written to each AdWindow.
    let detectorVersion: String
    /// Phase 3 Foundation Model backfill toggle. Defaults to `.full`:
    /// FM runs, persists results, and contributes to the decision ledger. See
    /// `FMBackfillMode` for the full contract.
    let fmBackfillMode: FMBackfillMode
    /// Upper bound for FM scanning work per backfill run.
    let fmScanBudgetSeconds: TimeInterval
    /// Minimum overlapping FM windows needed to count as consensus.
    let fmConsensusThreshold: Int
    /// ef2.6.3: Minimum skipConfidence to show a lightweight gray-band marker
    /// (no auto-skip). Spans in [markOnlyThreshold, autoSkipConfidenceThreshold)
    /// surface a "likely sponsor segment" marker with one-tap user actions.
    let markOnlyThreshold: Double
    /// Phase 6.5b (playhead-4my.17): skipConfidence threshold above which an otherwise
    /// detectOnly/logOnly eligible span is promoted to autoSkipEligible. Promotion
    /// applies only when eligibilityGate == .eligible and policyAction is not .suppress.
    /// ef2.6.3: raised from 0.75 to 0.80 per product-approved band spec.
    let autoSkipConfidenceThreshold: Double

    /// playhead-fqc8: Looser auto-skip threshold for spans on the
    /// `PromotionTrack.classifierSeedQualified` track — i.e. classifier-only
    /// candidates that picked up an `.acoustic`/`breakAlignment` corroborator
    /// AND whose stored classifier score is `>= 0.70`. The `0.30` structural
    /// ceiling on a classifier-only ledger sum makes the standard `0.80`
    /// threshold unreachable for these spans no matter how strong the
    /// classifier signal is. The qualified track gives them a separate
    /// eligibility floor (default `0.50`) that respects the same gate /
    /// policy guards as the standard path. Setting this `>=
    /// autoSkipConfidenceThreshold` makes the qualified track no-op.
    let classifierSeedQualifiedThreshold: Double

    /// playhead-xsdz.1: Auto-skip threshold for spans on the
    /// `PromotionTrack.lexicalAutoAdQualified` track — spans whose ledger
    /// carries a high-precision `.lexicalAutoAd` entry (a vetted strong
    /// co-occurrence of sponsor + promo code / URL CTA in a tight window,
    /// negative-evidence guardrails already cleared) and which carry no
    /// FM-class anchor. The `.lexical` family is structurally capped at
    /// `lexicalCap = 0.20`, so the standard `0.80` gate is unreachable
    /// through the lexical channel no matter how blatant the ad copy is.
    /// This separate floor (default `0.50`) lets a confirmed combo
    /// auto-skip on its own while respecting the same eligibility-gate /
    /// policy guards as every other path. The default `lexicalAutoAdCap`
    /// (0.55) sits just above this floor so a single confirmed entry
    /// clears it; corroborating signals push further past it. Setting this
    /// `>= autoSkipConfidenceThreshold` makes the qualified track a no-op
    /// (the rule still contributes fusion mass, just never promotes alone).
    let lexicalAutoAdQualifiedThreshold: Double

    /// playhead-xsdz.6: master kill switch for the high-precision lexical
    /// auto-ad rule (`LexicalAutoAdEvidenceBuilder`). When `false` (the
    /// default), `buildEvidenceLedger` builds no `.lexicalAutoAd` entry —
    /// removing BOTH the `PromotionTrack.lexicalAutoAdQualified` auto-skip
    /// AND the entry's fusion mass, restoring pre-xsdz.1 behavior. This is
    /// strictly more conservative than raising `lexicalAutoAdQualifiedThreshold`
    /// (which no-ops the auto-skip but leaves the fusion mass in play).
    ///
    /// Gated OFF by default per the 2026-05-27 per-feature live A/B
    /// (`LexicalScorerLiveABTests`, playhead-xsdz.5): with region tightening
    /// (xsdz.2+3) active, enabling this rule net-added one false positive at
    /// no recall gain (all-on 18 FP vs region-only / baseline 17 FP; span
    /// recall flat at 0.833 = 20/24 across every arm). Flip to `true` to
    /// re-enable — a one-line rollback; the rule stays fully built and
    /// unit-tested, just inert in production until a larger corpus shows lift.
    let lexicalAutoAdEnabled: Bool

    /// playhead-gtt9.11: Segment-level UI-candidate threshold. A segment-
    /// aggregated score at or above this value qualifies as a "possible ad"
    /// marker in the UI; below it the segment is telemetry-only. Distinct
    /// from `candidateThreshold` (which is the per-window classifier floor)
    /// and `markOnlyThreshold` (which is the span-level skipConfidence band
    /// for ef2.6.3 gray markers). Default 0.40 matches
    /// `SegmentAggregator.promotionThreshold` so aggregator promotion and
    /// UI-candidate persistence agree.
    let segmentUICandidateThreshold: Double

    /// playhead-gtt9.11: Segment-level auto-skip threshold. A segment at or
    /// above this value is eligible for auto-skip PROVIDED the safety-signal
    /// conjunction also fires (see `AutoSkipPrecisionGate`). Intentionally
    /// stricter than `segmentUICandidateThreshold` — "possible ad" markers
    /// should appear at lower confidence than actual auto-skips. Default
    /// 0.55 sits midway between the 0.40 aggregator promotion floor and the
    /// 0.60 single-window high-confidence seed. Not calibrated on real data;
    /// gtt9.3 owns calibration.
    let segmentAutoSkipThreshold: Double

    /// playhead-arf8: master kill switch for music-bracket boundary
    /// refinement (BracketDetector + FineBoundaryRefiner graduated from
    /// shadow). Default `true` because the components have shipped under
    /// shadow telemetry; flipping to `false` is the one-line rollback if
    /// dogfooding reveals a regression. When `false`, the backfill loop
    /// uses only the legacy `BoundaryRefiner.computeAdjustments` path.
    let bracketRefinementEnabled: Bool

    /// playhead-arf8: per-show `musicBracketTrust` floor below which
    /// bracket evidence is suppressed. Trust is sampled from the
    /// `MusicBracketTrustStore` Beta posterior; the prior mean is 0.50,
    /// so a floor of 0.40 leaves bracket refinement active for every show
    /// until accumulated outcome history pulls trust below the floor.
    /// Tightening this (e.g. to 0.55) makes the gate more conservative.
    let bracketRefinementMinTrust: Double

    /// playhead-arf8: minimum `BracketEvidence.coarseScore` required for
    /// bracket detection to influence boundary refinement. Below this
    /// threshold the detector reported a candidate but the envelope
    /// signal was too weak to override the legacy snap. Default 0.30
    /// matches `BracketDetector.Config.default.onsetScoreThreshold`.
    let bracketRefinementMinCoarseScore: Double

    /// playhead-arf8: minimum `BoundaryEstimate.confidence` from
    /// `FineBoundaryRefiner` required to apply the fine-grained snap.
    /// When the local search yields a low-confidence cue (no silence,
    /// no energy valley, no spectral discontinuity), the legacy
    /// `BoundaryRefiner` adjustment is used instead. Default 0.20 keeps
    /// the bar low because the bracket trust gate already filtered out
    /// untrustworthy shows.
    let bracketRefinementMinFineConfidence: Double

    /// playhead-kgby: master flag for the transcript-aware boundary cue.
    /// When `true` (the conservative default), `runBackfill` builds
    /// `[TranscriptBoundaryHit]` from the final-pass transcript chunks
    /// and threads them into `BoundaryRefiner.computeAdjustments`. The
    /// resolver then runs with `transcriptBoundary` weight 0.20 and
    /// `pauseVAD` weight 0.70 (down from 0.90) so a sentence terminal
    /// near a candidate boundary contributes a soft cue.
    ///
    /// Setting this flag to `false` is the one-line rollback: the
    /// service passes empty transcript hits, the resolver picks the
    /// legacy 90/10 weight schedule, and the snap output is bit-
    /// identical to pre-kgby behaviour.
    let transcriptBoundaryCueEnabled: Bool

    /// playhead-xsdz.7: master kill switch for the Evidence-Fragility
    /// precision gate. When `false` (the default), `applyFragilityPenalty`
    /// returns the input `skipConfidence` UNCHANGED — the decision output is
    /// byte-identical to pre-xsdz.7 behavior. Flip to `true` to enable the
    /// soft false-positive-suppression penalty described below.
    ///
    /// The insight (cross-model idea duel): false positives clear the
    /// auto-skip threshold on THIN, BRITTLE evidence — one dominant channel,
    /// a narrow margin over the (track-specific) threshold, few distinct
    /// evidence families — whereas true positives are supported robustly by
    /// many independent channels. The fragility score is computed purely from
    /// the EXISTING ledger geometry (no new evidence source / channel) and
    /// applied as a SOFT multiplicative penalty on `skipConfidence` BEFORE the
    /// hard eligibility / auto-skip gates. It never hard-blocks.
    let evidenceFragilityPenaltyEnabled: Bool

    /// playhead-xsdz.7: fragility score above which the soft penalty is
    /// applied. The score (see `applyFragilityPenalty`) is
    /// `concentration / max(margin, ε) / max(depth, 1)`; it grows when a
    /// single channel dominates, the margin over threshold is thin, and few
    /// evidence families fired. Default `2.0` is deliberately conservative —
    /// a span needs to look clearly brittle before the penalty bites. Raising
    /// this makes the gate fire on fewer spans; lowering it penalizes more.
    /// Only consulted when `evidenceFragilityPenaltyEnabled` is `true`.
    let fragilityThreshold: Double

    /// playhead-xsdz.7: multiplicative penalty applied to `skipConfidence`
    /// when `fragility > fragilityThreshold`. Default `0.85` shaves 15% off
    /// the confidence of a brittle span — enough to drop a span that *just*
    /// cleared its threshold back below it, without nuking a strong-but-
    /// brittle span outright (the SOFT contract). Must be in `[0, 1]`; values
    /// outside that range are clamped in `applyFragilityPenalty`. Only
    /// consulted when `evidenceFragilityPenaltyEnabled` is `true`.
    let fragilityPenalty: Double

    /// playhead-au2v.1.2: Tri-state gate for the chapter-signal feature
    /// (epic playhead-au2v.1). Defaults to `.off` for production safety
    /// — the chapter-generation phase, CoveragePlanner audit-window read,
    /// and FM-prompt chapter-context read are all no-ops until this is
    /// flipped. See `ChapterSignalMode` for the per-mode contract.
    ///
    /// Insertion points (planned, no live consumers yet):
    ///   - `ChapterGenerationPhase` entry guard (playhead-au2v.1.10):
    ///     `guard config.chapterSignalMode.runsChapterGeneration else { return }`
    ///   - CoveragePlanner audit-window read (playhead-au2v.1.14):
    ///     `if config.chapterSignalMode.consumersReadChapterPlan { ... }`
    ///   - FM prompt builders (playhead-au2v.1.16):
    ///     same `consumersReadChapterPlan` predicate.
    let chapterSignalMode: ChapterSignalMode

    /// playhead-xsdz.8: master kill switch for the composite audio-forensics
    /// boundary evidence channel (`AudioForensicsBoundaryDetector`). When
    /// `false` (the production default), `buildEvidenceLedger` builds NO
    /// `.audioForensics` entry — removing the channel's fusion mass entirely
    /// and restoring byte-identical pre-xsdz.8 behaviour. Flip to `true` to
    /// enable: the detector then measures the physical boundary discontinuity
    /// (loudness / spectral-flux / noise-floor / production-environment step)
    /// at each candidate span edge from the already-available per-window
    /// acoustic features and emits a single capped corroborative entry when
    /// the merged boundary score is significant.
    ///
    /// Gated OFF by default per the OFF-by-default mandate: main stays
    /// behavior-neutral and the channel is never wired into a production
    /// config or A/B arm until a corpus eval shows lift. The detector stays
    /// fully built and unit-tested, just inert in production.
    let audioForensicsEnabled: Bool

    /// playhead-xsdz.9: master kill switch for the cross-episode "memory"
    /// precision signal (`NegativeFingerprintBank` HARD-NEGATIVE suppression +
    /// `AdCopyFingerprintStore` POSITIVE Smith-Waterman boost). When `false`
    /// (the production default), `buildEvidenceLedger` builds NO
    /// `.crossEpisodeMemory` entry AND the decision path performs NO
    /// negative-bank read and NO suppression — so the output is byte-identical
    /// to pre-xsdz.9 behaviour and no new store reads/writes occur on the
    /// decision path.
    ///
    /// Two halves ride this ONE flag:
    ///   1. Negative-bank SUPPRESSION (the novel lever): a candidate whose
    ///      transcript tokens align (Smith-Waterman) to a CONFIRMED false
    ///      positive is multiplicatively suppressed (post-fusion, before the
    ///      hard auto-skip gate — same idiom as the xsdz.7 fragility penalty).
    ///   2. Positive copy-alignment BOOST: a candidate aligning to a confirmed-
    ///      ad bank sequence gets one capped `.crossEpisodeMemory` ledger entry.
    ///
    /// Gated OFF by default per the OFF-by-default mandate: main stays behavior-
    /// neutral and the feature is never wired into a production config or A/B
    /// arm until a corpus eval shows lift. The stores / evaluator stay fully
    /// built and unit-tested, just inert in production. The ENTIRE feature rides
    /// this ONE flag: `PlayheadRuntime` constructs / migrates the bank and wires
    /// the confirmed-FP WRITE trigger ONLY when the flag is on. With the flag off
    /// there is no bank, no DB file, no migration, no write, and no read — zero
    /// new production side-effects (same invariant xsdz.6/.7/.8 hold). When the
    /// flag is later flipped on we accept a cold-start bank.
    let crossEpisodeMemoryEnabled: Bool

    /// playhead-xsdz.12: master kill switch for the rhetorical act-sequence
    /// grammar evidence channel (`RhetoricalGrammarEvidenceBuilder`). When
    /// `false` (the production default), `buildEvidenceLedger` builds NO
    /// `.rhetoricalGrammar` entry AND the per-span transcript prose is never
    /// assembled for the grammar detector — so the output is byte-identical to
    /// pre-xsdz.12 behaviour and no new work occurs on the decision path. Flip
    /// to `true` to enable: the detector then classifies each sentence of the
    /// span's transcript into zero or more rhetorical roles (HOOK / PROBLEM /
    /// SOLUTION / EVIDENCE / OFFER / CTA), and emits a single capped
    /// corroborative entry when >= 3 distinct roles co-occur in (roughly) the
    /// canonical persuasion order.
    ///
    /// Gated OFF by default per the OFF-by-default mandate: main stays
    /// behavior-neutral and the channel is never wired into a production config
    /// or A/B arm until a corpus eval shows lift. The detector stays fully built
    /// and unit-tested, just inert in production.
    let rhetoricalGrammarEnabled: Bool

    /// playhead-xsdz.13: master kill switch for the cross-show syndication
    /// evidence channel (`CrossShowSyndicationStore` +
    /// `CrossShowSyndicationEvaluator`). When `false` (the production default),
    /// the ENTIRE feature is inert: `PlayheadRuntime` constructs / migrates NO
    /// store, the WRITE path records NO sponsor-entity observations, and
    /// `buildEvidenceLedger` builds NO `.crossShowSyndication` entry AND performs
    /// NO store read — so the output is byte-identical to pre-xsdz.13 behaviour
    /// and there are ZERO new production side-effects (no store, no SQLite file,
    /// no migration, no writes, no reads). This is the load-bearing xsdz.9
    /// full-gating lesson applied to a NEW store.
    ///
    /// When flipped to `true`: the WRITE path records each above-min-confidence
    /// sponsor ENTITY (normalized via `EvidenceCatalogBuilder.normalize`,
    /// matching `SponsorKnowledgeEntry.normalizedValue`) keyed by show, and the
    /// READ path emits a single capped corroborative `.crossShowSyndication`
    /// entry when that entity has high cross-show SPREAD (distinct-show count /
    /// spread-ratio over a threshold) AND temporal persistence (seen across time,
    /// not a one-week burst). A one-show / one-episode entity gets no boost.
    ///
    /// Gated OFF by default per the OFF-by-default mandate: main stays
    /// behavior-neutral and the channel is never wired into a production config
    /// or A/B arm until a corpus eval shows lift. The store / evaluator stay
    /// fully built and unit-tested, just inert in production. When the flag is
    /// later flipped on we accept a cold-start store (consistent with the other
    /// off-by-default channels).
    let crossShowSyndicationEnabled: Bool

    /// playhead-xsdz.10: master kill switch for lightweight temporal
    /// regularization. When `false` (the production default), `runBackfill`
    /// never invokes `TemporalRegularizer` — so the per-span decisions are
    /// byte-identical to pre-xsdz.10 behaviour. Flip to `true` to enable the
    /// deterministic, post-fusion isolation-penalty + min-dwell pass described
    /// on `TemporalRegularizer`.
    ///
    /// The insight (cross-model idea duel): real ads are CONTIGUOUS and
    /// CLUSTERED (2-4 back-to-back in a break) while false positives are
    /// typically ISOLATED one-off editorial mentions. The pass softly penalizes
    /// the `skipConfidence` of a detection with NO high-confidence neighbor in a
    /// time window, and additionally down-weights a too-short uncorroborated
    /// "island". The neighbor-support signal is HIGH-CONFIDENCE-GATED and
    /// ONE-SIDED (anti-contagion): a neighbor only counts as support if its own
    /// `skipConfidence >= temporalHighConfidenceNeighborThreshold`, and the pass
    /// can only LOWER a confidence, never raise it. Applied across the episode's
    /// collected decisions BEFORE the hard auto-skip gate; never hard-blocks.
    ///
    /// Gated OFF by default per the OFF-by-default mandate: not enabled in any
    /// production config or A/B arm. The regularizer stays fully built and
    /// unit-tested, just inert in production.
    let temporalRegularizationEnabled: Bool

    /// playhead-xsdz.10: edge-to-edge neighbor window (seconds) for temporal
    /// regularization. A neighbor within this gap of a detection can lend
    /// support. Default 120s. Only consulted when
    /// `temporalRegularizationEnabled` is `true`.
    let temporalNeighborWindowSeconds: TimeInterval

    /// playhead-xsdz.10: the anti-contagion gate. A neighbor counts as SUPPORT
    /// only when its own `skipConfidence` is at or above this threshold. Default
    /// 0.80 (the production auto-skip floor) so only a confident detection lends
    /// support and two weak FPs cannot prop each other up. Only consulted when
    /// `temporalRegularizationEnabled` is `true`.
    let temporalHighConfidenceNeighborThreshold: Double

    /// playhead-xsdz.10: multiplicative penalty applied to an ISOLATED
    /// detection's `skipConfidence` (no high-confidence neighbor in the window).
    /// Default 0.85; clamped to [0, 1]; never boosts. Only consulted when
    /// `temporalRegularizationEnabled` is `true`.
    let temporalIsolationPenaltyFactor: Double

    /// playhead-xsdz.10: a detection shorter than this (seconds) AND
    /// uncorroborated is treated as a too-short island and additionally
    /// down-weighted by `temporalMinDwellPenaltyFactor`. Default 10s. Only
    /// consulted when `temporalRegularizationEnabled` is `true`.
    let temporalMinDwellSeconds: TimeInterval

    /// playhead-xsdz.10: multiplicative penalty applied to a too-short,
    /// uncorroborated island. Default 0.90; clamped to [0, 1]; never boosts.
    /// Only consulted when `temporalRegularizationEnabled` is `true`.
    let temporalMinDwellPenaltyFactor: Double

    /// playhead-xsdz.11: master kill switch for PER-SHOW auto-skip threshold
    /// control from user feedback. When `false` (the production default),
    /// `runBackfill` never reads a per-show offset, the orchestrator never
    /// writes controller state, and the runtime constructs NO
    /// `PerShowThresholdControllerStore` — so the auto-skip gate uses the
    /// unmodified global threshold and behaviour is byte-identical to
    /// pre-xsdz.11.
    ///
    /// The insight (cross-model idea duel): adapt the per-show threshold to each
    /// user's corrections via a bounded scalar PI controller (NOT a learned
    /// model). A user listening THROUGH an auto-skipped section is a
    /// false-positive signal (raise that show's threshold = be more
    /// conservative); a user scrubbing through undetected content is a miss
    /// signal (lower the threshold = be more aggressive). Per-show adaptation
    /// handles heterogeneity a single global threshold can't.
    ///
    /// Gated OFF by default per the OFF-by-default mandate: not enabled in any
    /// production config or A/B arm. The controller + store stay fully built and
    /// unit-tested, just inert in production. Cold-start accepted when enabled.
    let perShowThresholdControlEnabled: Bool

    /// playhead-xsdz.11: PI controller proportional gain. Small so a single
    /// correction nudges, never lurches. Only consulted when
    /// `perShowThresholdControlEnabled` is `true`.
    let perShowThresholdProportionalGain: Double

    /// playhead-xsdz.11: PI controller integral gain. Applied to the running sum
    /// of correction errors so a persistent one-sided stream slowly accumulates
    /// a larger offset. Only consulted when `perShowThresholdControlEnabled`.
    let perShowThresholdIntegralGain: Double

    /// playhead-xsdz.11: maximum absolute per-show threshold offset, in
    /// confidence units. Bounds the personalization. Only consulted when
    /// `perShowThresholdControlEnabled` is `true`.
    let perShowThresholdMaxOffset: Double

    /// playhead-xsdz.11: minimum corrections before the controller emits any
    /// non-zero offset (cold-start until corrections accumulate). Only consulted
    /// when `perShowThresholdControlEnabled` is `true`.
    let perShowThresholdMinSamples: Int

    /// playhead-p56a: master kill switch for the `SpanFinalizer` safety layer.
    /// When `false` (the production default), `runBackfill` never invokes
    /// `SpanFinalizer.finalize(...)` — so the per-span decisions, ad-window
    /// bounds, and persisted rows are byte-identical to pre-p56a behaviour.
    /// Flip to `true` to enable the deterministic, post-fusion finalizer that
    /// applies 5 hard constraints (non-overlap resolution, < 3s content-gap
    /// merge, < 5s/> 180s duration sanity, content-chapter penalty, 50% action
    /// cap) plus a final skip-policy override pass.
    ///
    /// The insertion point is between the temporal-regularization block and
    /// the side-effect emission loop in `runBackfill`: the finalizer sees the
    /// fully scored `pendingDecisions` and may shrink, drop, or re-time spans
    /// before persistence. When the flag is off, `pendingDecisions` is
    /// forwarded to the emission loop unchanged — no allocations, no log
    /// lines, no per-span constraint trace.
    ///
    /// Gated OFF by default per the OFF-by-default mandate: not enabled in any
    /// production config or A/B arm. The finalizer stays fully built and
    /// unit-tested (`SpanFinalizerTests`), just inert in production until the
    /// bd-4xqf coverage measurement on the live pipeline-dump path confirms
    /// the change is safe to enable.
    let spanFinalizerEnabled: Bool

    /// playhead-xsdz.20: master flag for the post-decode splice-slot OWNERSHIP
    /// pass. When `false` (the production default), `runBackfill` never invokes
    /// the slot pass — no resolver call, no negative-bank verdict table, no span
    /// rewrite, no `.spliceSlot` provenance, and the refiner / audio-forensics
    /// paths are untouched — so the pipeline OUTPUT is byte-identical to
    /// pre-xsdz.20. Flip to `true` to let a qualifying acoustic-splice pair own
    /// an ad span's WIDTH (transcript/FM keep owning PRESENCE). Bead C's shadow
    /// instrumentation is separately gated and exempt from this flag.
    let spliceSlotOwnershipEnabled: Bool

    /// playhead-xsdz.21 (Bead C): master flag for the OWNERSHIP-OFF shadow pass.
    /// When `true` AND `spliceSlotOwnershipEnabled == false`, `runBackfill`
    /// computes the would-be dispositions by invoking the SAME shared resolver +
    /// negative-bank verdict table + pure `SpliceSlotDispositionEngine` the
    /// flag-ON path uses, emits one `spliceslot.shadow` breadcrumb per span, and
    /// records structured rows to an injected observer — WITHOUT applying any
    /// rewrite (no `.spliceSlot` provenance, no re-persist). Default OFF; ON only
    /// in dogfood-capture builds. When `spliceSlotOwnershipEnabled == true` the
    /// shadow pass NEVER runs (both-flags-ON ⇒ shadow silent by construction), so
    /// there is exactly one owner of the disposition side effects. Both flags OFF
    /// ⇒ neither pass runs and the pipeline is byte-identical.
    let spliceSlotShadowEnabled: Bool

    /// playhead-xsdz.29: master flag for the post-decode REDIFF slot OWNERSHIP
    /// pass (the double-fetch width oracle). When `true` (the production
    /// default since Dan's 2026-07-19 Ship Gate 1 enablement, playhead-lq6f),
    /// a qualifying rediff of the played A-side against a re-fetched B-side
    /// owns an ad span's WIDTH (transcript/FM keep owning PRESENCE) and stamps
    /// `.rediffSlot` provenance. Rediff presence measured 97.8% on the
    /// gold-audited corpus; this is the MARK-ONLY rung of the xsdz.36 staged
    /// ladder — width marks/banners only, no auto-skip behavior change.
    ///
    /// When `false`, `runBackfill` never invokes the rediff pass — no store
    /// read for the A-side fingerprints, no B-side fetch, no differ, no span
    /// rewrite, no `.rediffSlot` provenance — so the pipeline OUTPUT is
    /// byte-identical to pre-xsdz.29. The rediff pass ALSO no-ops when no
    /// `RediffBSideProvider` is injected (so the flag alone changes nothing
    /// until a provider is wired — the re-fetch scheduler is xsdz.28).
    /// Rediff is the SOLE production width setter (contract 2026-07-07); the
    /// acoustic `spliceSlotOwnershipEnabled` path is a separate, mutually-
    /// exclusive channel and must not be ON at the same time (splice stays
    /// `false` in production, so the init precondition holds).
    let rediffSlotOwnershipEnabled: Bool

    /// playhead-xsdz.29: master flag for the OWNERSHIP-OFF rediff SHADOW pass.
    /// When `true` AND `rediffSlotOwnershipEnabled == false` AND a
    /// `RediffBSideProvider` is injected, `runBackfill` computes the would-be
    /// rediff dispositions via the SAME engine the flag-ON path uses, emits one
    /// `rediffslot.shadow` breadcrumb per span, and records structured rows to an
    /// injected observer — WITHOUT applying any rewrite (no `.rediffSlot`
    /// provenance, no re-persist). Default OFF; ON only in dogfood-capture
    /// builds. NOTE (post Gate 1, playhead-lq6f): `rediffSlotOwnershipEnabled`
    /// now defaults ON, so a config that wants the shadow pass must ALSO pass
    /// ownership explicitly `false` — shadow-ON alone is silent by
    /// construction. Independent of the acoustic `spliceSlotShadowEnabled`
    /// (both can shadow at once, to their OWN observers).
    let rediffSlotShadowEnabled: Bool

    /// playhead-xsdz.62: master flag for counting a BYTE-EXACT rediff slot as a
    /// distinct corroborating evidence KIND in the fusion corroboration quorum.
    /// Threaded verbatim into `FusionWeightConfig.rediffConfirmedKindEnabled`.
    /// When `true`, `BackfillEvidenceFusion.buildLedger()` appends a single
    /// weight-0 `.rediffConfirmed` ledger entry for any span whose WIDTH is owned
    /// by the byte-exact rediff oracle (`.rediffSlot` provenance ==
    /// `DecodedSpan.carriesRediffByteExactWidth`). That deterministic kind counts
    /// toward the `distinctKinds.count` quorum in the three `DecisionMapper`
    /// corroboration gates, so a rediff-confirmed DAI ad + one other corroborating
    /// kind reaches eligibility WITHOUT an FM vote — FM absent/uncertain (the
    /// xsdz.63 finding on produced-DAI ads carrying URLs/"sponsored by") no longer
    /// blocks a byte-exact-rediff-confirmed insertion. Adds NO score mass (the
    /// entry is weight-0) and NEVER sets a span's width (that stays the rediff
    /// ownership pass). Acoustic splice (`.spliceSlot`) is not byte-exact and
    /// never emits this kind — and since playhead-6qvf neither is the rediff
    /// CHROMA arm (`.rediffSlotChroma`), which is a ~1 s fingerprint alignment
    /// rather than a byte-run one and so cannot claim the "deterministic kind"
    /// this flag grants.
    ///
    /// Default OFF: this is an ELIGIBILITY-affecting change, so it ships OFF and
    /// flips after a corpus A/B — the same measurement-gated discipline as its
    /// siblings (`certaintyTieredSkipEnabled`, `userCorrectionReadSideEnabled`).
    /// With OFF the constructed `FusionWeightConfig` is byte-identical to today
    /// and no `.rediffConfirmed` entry is ever emitted. (Also inert in real
    /// production regardless of the flag until a `RediffBSideProvider` is wired,
    /// since no span carries `.rediffSlot` without it.)
    let rediffConfirmedKindEnabled: Bool

    /// playhead-dsbc (Phase B1): master flag for the distilled specialist
    /// classifier's SHADOW pass. When `true` AND a
    /// `LiveSpecialistShadowDispatcher` with a non-nil
    /// `SpecialistAdClassifier.Runtime` is injected, the specialist runs
    /// alongside FM, logs a verdict, and ACTS ON NOTHING (the dispatcher owns
    /// no store-write or decision seam). When `false` — or when no runtime is
    /// injected (the B1 default; the live `CoreAILanguageModel`-backed runtime
    /// is phone-gated Phase B2) — no specialist work runs and the pipeline
    /// output is byte-identical. Default OFF; this is pure inert plumbing.
    let specialistShadowEnabled: Bool

    /// playhead-b6jq PR 4 (Phase B2): master flag for the specialist HOST-READ
    /// SCAN phase — a background backfill pass that runs the on-device
    /// specialist over candidate windows and PERSISTS raw verdicts to
    /// `specialist_scan_results`. It ACTS ON NOTHING in PR 4: no tau=0.7
    /// threshold, no mark/banner composition, and NOTHING touches auto-skip
    /// eligibility (auto-skip stays deterministic-only; `adClass` stays
    /// `"hostRead"`) — PR 5 consumes the rows. Two-key gated in
    /// `BackfillJobRunner`: the phase is enqueued ONLY when this is `true` AND a
    /// non-nil, phone-gated `SpecialistAdClassifier.Runtime` is injected.
    /// Default OFF; with the shipped default no scan job is enqueued, no rows
    /// are written, and every FM path is byte-identical. The runner's two-key
    /// enqueue guard is the sole reader.
    let specialistScanEnabled: Bool

    /// playhead-b6jq PR 5 (Phase B2): master flag for the specialist MARK
    /// COMPOSE step — turning PR4's persisted raw `specialist_scan_results` into
    /// user-visible MARK-ONLY banner marks (`SpecialistMarkComposer`: τ=0.7
    /// filter → merge → 70%-overlap dedupe → emit `AdWindow`s stamped
    /// `detectorVersion="specialist-ft-v2"`, `eligibilityGate=markOnly`). When
    /// `false` (the shipped default) BOTH compose sites — `runBackfill`'s Step 18
    /// and `BackfillJobRunner.runSpecialistHostReadScan`'s tail — short-circuit
    /// before any fetch/compose/write, so zero `ad_windows` rows are written, no
    /// banner surfaces, and every FM path is byte-identical. Composed marks are
    /// ALWAYS mark-only and can NEVER reach auto-skip; auto-skip stays
    /// deterministic-only. Independent of `specialistScanEnabled`: compose reads
    /// only persisted rows + existing windows (no model re-run), so a build can
    /// scan without composing, or (once rows exist) compose without re-scanning.
    let specialistMarkComposeEnabled: Bool

    /// playhead-xsdz.34: master flag for the user-correction READ side (the
    /// per-atom/per-span `.userVetoed` mask). When `false` (the production
    /// default), both `AtomEvidenceProjector` call sites inject
    /// `NoCorrectionMaskProvider()` — no store read, no atom carries
    /// `.userVetoed`, and the pipeline output is byte-identical to pre-xsdz.34.
    /// When `true` AND a `correctionStore` is wired, the projector injects a
    /// `StoreBackedCorrectionMaskProvider` built from the asset's active
    /// `.falsePositive` scopes, so an explicit "not an ad" veto un-anchors the
    /// vetoed atoms AND feeds the `.vetoNewlyEnclosed` gate (splice + rediff
    /// width passes). Suppress-direction only (guardrail 1); default OFF so the
    /// corpus A/B and the xsdz.36 staged rollout can flip it deliberately.
    let userCorrectionReadSideEnabled: Bool

    /// playhead-l2f.6: master kill switch for StingerBank boundary
    /// refinement. When `false`, `runBackfill` never loads the bundled
    /// `StingerBank.json`, never fetches the asset's episode identity,
    /// never reads shard PCM, and never invokes `StingerRefiner` — so the
    /// per-span bounds, persisted rows, and both stinger trace maps are
    /// byte-identical to pre-l2f.6 behaviour (same invariant
    /// `spanFinalizerEnabled` holds for the finalizer).
    ///
    /// When `true` (the production default since Dan's recorded 2026-07-16
    /// dogfood flip), each candidate ad span on a show with a bank entry
    /// is refined inside the inline boundary-refinement block: a 50 Hz
    /// log-RMS envelope over a ±90 s search span around each edge (read
    /// from the persisted 16 kHz analysis-shard cache — no second decode)
    /// is matched against the show's learned stinger template by
    /// normalized cross-correlation, and the JOINT candidate-pair recipe
    /// (v4, playhead-xsdz.38 — qualifying peaks + grid-derived partners,
    /// on-grid pair bonus, off-grid inconsistency penalty, movement
    /// tie-break) picks the best feasible (start, end) pair. Per-show
    /// gates (`max(0.50, learning_confidence − 0.15)`), the 75 s move cap
    /// on every candidate, and the revert-on-zero-overlap guard all hold.
    /// The refiner never splits or merges windows and can never produce
    /// `end <= start` (see `StingerRefiner`).
    let stingerRefinementEnabled: Bool

    /// playhead-xsdz.37: master kill switch for lexical-anchor boundary
    /// refinement. When `false` (the DEFAULT — this cut ships OFF and is
    /// measured via Catalyst dump before any flip), `runBackfill` never loads
    /// the bundled `LexicalAnchorBank.json`, never builds the transcript word
    /// stream for it, and never invokes `LexicalAnchorRefiner` — so per-span
    /// bounds, persisted rows, and both lexical trace maps are byte-identical
    /// to pre-xsdz.37 behaviour (same OFF invariant `stingerRefinementEnabled`
    /// holds).
    ///
    /// When `true`, each candidate ad span on a show WITH a bank entry is
    /// refined inside the inline boundary-refinement block, AFTER the stinger
    /// snap: an EXACT match of a curated pre/attribution phrase near the start
    /// edge (or a resume phrase near the end edge) snaps that edge to the
    /// matched-phrase position plus the anchor's learned onset offset. An exact
    /// lexical match takes precedence on the specific edge it fires; every
    /// other edge keeps the prior stinger/acoustic result. The move cap and the
    /// revert-on-zero-overlap guard hold; the refiner never splits or merges
    /// windows and can never produce `end <= start` (see `LexicalAnchorRefiner`).
    let lexicalAnchorRefinementEnabled: Bool

    /// playhead-fl4j: master kill switch for eligibility-side SELF-PROMO
    /// suppression. When `false` (the DEFAULT — this cut ships OFF; the spike's
    /// self-promo phrases were mostly single-show, so cross-show generalisation
    /// needs validation on a larger labeled set before any production flip),
    /// `runBackfill` never loads the bundled `SelfPromoBank.json`, never builds
    /// the transcript word stream for it, and never invokes `PromoSuppressor` —
    /// so per-span gates, persisted rows, and every downstream field are
    /// byte-identical to pre-fl4j behaviour (same OFF invariant
    /// `lexicalAnchorRefinementEnabled` holds).
    ///
    /// When `true`, each candidate ad span runs through the ATTENTION →
    /// VERIFICATION suppressor: a curated show-agnostic self-promo phrase match
    /// is only a CLUE, and the eligibility gate is DEMOTED to `.markOnly` only
    /// when a verifier corroborates the show is promoting ITSELF (a STRONG
    /// phrase like "follow us" is self-corroborating; an AMBIGUOUS event plug
    /// like "get tickets" / "on tour" demotes only with a first-person /
    /// show-identity self-reference marker in its local window — a bare match on
    /// a third-party event ad is NOT demoted). Demotion routes the span to a
    /// play-by-default suggest banner instead of auto-skipping. This is an
    /// ELIGIBILITY change only (boundaries and scores are untouched), and it is
    /// severity-guarded so it can ONLY relax a fully-`.eligible` span; it never
    /// overrides a harder block and never promotes.
    let selfPromoSuppressionEnabled: Bool

    /// playhead-ncv6: master kill switch for the sustained-music-offset
    /// PROPOSER (playhead-t1py / playhead-xtpf). When `true` (the production
    /// default since Dan's 2026-07-19 Ship Gate 1 enablement, playhead-lq6f),
    /// `runBackfill` threads `true` into
    /// `RegionShadowPhase.Input.sustainedMusicProposerEnabled`:
    /// `RegionShadowPhase.run` scans the episode's feature windows for
    /// sustained-music runs and seeds atom-range-WIDE `.sustainedMusic`-origin
    /// regions, which can only ever decode to `.markOnly` (music-only
    /// provenance never auto-skips — see
    /// `DecisionMapper.isMusicOnlyProvenance`). Enablement was measured via
    /// the certified config path at 47.5% ad-width coverage / 91.7% true
    /// precision / 6.0% false-banner rate — banner-only, no skip behavior
    /// change.
    ///
    /// When `false`, the proposer is never called, no `.sustainedMusic`-origin
    /// region is seeded, and the pipeline output is byte-identical to
    /// pre-ncv6 behaviour.
    let sustainedMusicProposerEnabled: Bool

    /// playhead-ncv6: master kill switch for the lexical ad-cue GATE
    /// (playhead-eki3) over the sustained-music-offset proposer's music-ONLY
    /// spans. When `true` (the production default since Dan's 2026-07-19
    /// Ship Gate 1 enablement, playhead-lq6f — part of the certified config
    /// that measured 47.5% coverage / 91.7% true precision / 6.0%
    /// false-banner), every UNCORROBORATED `.sustainedMusic`-only region
    /// whose onset window carries NO third-party ad-cue is dropped (the
    /// cue-less content / credits / theme false-banner class). INDEPENDENT of
    /// `sustainedMusicProposerEnabled`: the gate only ever acts on
    /// `.sustainedMusic`-origin proposals, so gate-on / proposer-off is a safe
    /// no-op. See `MusicOffsetLexicalGate`.
    ///
    /// When `false`, `runBackfill` threads `false` into
    /// `RegionShadowPhase.Input.musicOffsetLexicalGateEnabled`, built
    /// proposals pass through verbatim, and the pipeline output is
    /// byte-identical to pre-ncv6 behaviour.
    let musicOffsetLexicalGateEnabled: Bool

    /// playhead-ncv6: master kill switch for the FM RECOVERY pass
    /// (playhead-r2vz) over the spans the lexical gate would SUPPRESS. When
    /// `true` (the production default since Dan's 2026-07-19 Ship Gate 1
    /// enablement, playhead-lq6f — part of the certified config that measured
    /// 47.5% coverage / 91.7% true precision / 6.0% false-banner): when
    /// `musicOffsetLexicalGateEnabled` is ALSO on AND an
    /// `fmRegionRecoveryDispatcher` is wired AND FM is available, each
    /// gate-suppressed region gets one targeted FM look and is re-admitted iff
    /// the classifier returns `.ad`. Re-admitted regions carry no FM
    /// origin/evidence, so they can only ever decode to `.markOnly` —
    /// banner-only, no skip behavior change.
    ///
    /// When `false`, `runBackfill` threads `false` into
    /// `RegionShadowPhase.Input.musicOffsetFMRecoveryEnabled` and the
    /// partition-and-recover branch is inert — gate-on drops the suppressed
    /// set, gate-off passes proposals verbatim, byte-identical to pre-ncv6
    /// behaviour.
    let musicOffsetFMRecoveryEnabled: Bool

    /// playhead-wraj: master switch that surfaces the certainty-tiered auto-skip
    /// gate (shipped in `FusionWeightConfig` / `BackfillEvidenceFusion`'s
    /// `DecisionMapper` in PR #237) through production config. DEFAULT `false` —
    /// with no config change `runBackfill` threads `false` into
    /// `FusionWeightConfig.certaintyTieredEnabled`, the post-gate downgrade never
    /// arms, and the pipeline output is byte-identical to pre-wraj behaviour.
    /// Flip to `true` to enable the two additive, post-gate downgrades in
    /// `DecisionMapper.map()`: (1) a non-rediff (host-read) span whose
    /// `skipConfidence` sits below `hostReadConfidenceFloor` demotes from
    /// `.eligible` to `.markOnly` (banner, not auto-skip); rediff-anchored DAI
    /// spans are exempt and keep auto-skipping. (2) a span ending within
    /// `postRollGuardSeconds` of a KNOWN episode duration demotes to `.markOnly`
    /// regardless of score — with the SAME byte-exact-rediff exemption since
    /// playhead-sik9. Both downgrades only ever touch
    /// an already-`.eligible` gate and NEVER modify any score.
    ///
    /// playhead-nqey: **ships ON** (Dan's Gate-2 decision, taken 2026-08-01
    /// conditional on sik9, which landed as #330). Verified by the 2026-07-19
    /// gate-delta measurement (32/32 windows predicted-vs-observed agree at
    /// T=0.9 + 90s post-roll). Because the switch can only ever move
    /// `.eligible → .markOnly`, ON is strictly more conservative than OFF: it
    /// cannot create a skip, cannot widen one, and cannot change a score. OFF
    /// restores the pre-nqey behavior exactly.
    ///
    /// SCOPE, so the flag is not read as broader than it is: the only consumer
    /// is `DecisionMapper`, constructed once, in `runBackfill`
    /// (Steps 12–14). Rows minted by `mintByteExactDayZeroMarks`, by the hot
    /// path, or by the aggregator never consult that mapper and are untouched.
    let certaintyTieredSkipEnabled: Bool

    /// playhead-wraj: minimum `skipConfidence` a NON-rediff (host-read) span must
    /// reach to stay auto-skip-`.eligible` when `certaintyTieredSkipEnabled` is
    /// on. DEFAULT `0.9` — the T calibrated on the themove host-read fixture
    /// (2026-07-17: T=0.9 skips the confident reads, banners the uncertain reads
    /// and the low-confidence fragments = the safe-degradation policy). Threaded
    /// verbatim into `FusionWeightConfig.hostReadConfidenceFloor`; inert unless
    /// `certaintyTieredSkipEnabled == true`. Rediff-anchored spans are exempt
    /// from this floor.
    let hostReadConfidenceFloor: Double

    /// playhead-wraj (post-roll guard, Dan 2026-07-19): width of the end-of-
    /// episode window (seconds) inside which an `.eligible` span is demoted to
    /// `.markOnly` when `certaintyTieredSkipEnabled` is on. Post-roll ads are the
    /// least important to auto-skip (the user just jumps to the next episode) and
    /// a wrong skip near the end clips the host's closing content, so this
    /// demotion applies regardless of `skipConfidence`. Threaded verbatim into
    /// `FusionWeightConfig.postRollGuardSeconds`; inert when the episode duration
    /// is unknown (never guess the episode end) or `certaintyTieredSkipEnabled`
    /// is off. DEFAULT `90.0`.
    ///
    /// playhead-sik9 (Dan 2026-08-01): a span whose width is owned by the
    /// byte-exact rediff oracle is EXEMPT — the same carve-out
    /// `hostReadConfidenceFloor` already grants, extended here because a
    /// byte-exact tail is the most certain signal in the system and blanket-
    /// demoting it inverts the certainty tiering. Unanchored, acoustic
    /// (`.spliceSlot`) and playhead-9s6q segment-recovered tails stay guarded.
    let postRollGuardSeconds: Double

    /// playhead-2350: SAFETY gate — a fusion span with an UNANCHORED start or end
    /// edge may not be auto-skipped. DEFAULT `true`, and deliberately so: this is
    /// not a feature flag, it is the fix for a live trust bug. The 2026-07-25
    /// THEMOVE Catalyst replay shipped four windows with `eligibilityGate ==
    /// .eligible` — one of them starting 12.72 s inside the host's sign-off —
    /// whose every edge was `.unanchored`, i.e. invented by the pipeline.
    ///
    /// When `true`, `runBackfill`'s emission loop stamps
    /// `DecisionResult.extentSupport` and demotes an `.eligible` span whose
    /// extent is not fully anchored to `.markOnly` (banner, per Dan's
    /// certainty-tiered skip policy). The demotion NEVER promotes and NEVER
    /// touches a score.
    ///
    /// Set to `false` ONLY to disable the safety gate — a kill switch, and the
    /// opt-out for the few suites that must observe a pre-2350 demotion seam on
    /// a fixture whose spans are unanchored by construction. It can only ever
    /// make MORE spans auto-skippable, so flipping it off in production
    /// reintroduces the bug.
    let unanchoredExtentBlocksAutoSkip: Bool

    /// playhead-xsdz.66: pre-roll start-at-zero clamp threshold, in seconds. When
    /// the episode's FIRST ad window starts within this many seconds of t=0 — the
    /// pre-roll position — `runBackfill` extends its start edge to exactly 0.0
    /// (via `PreRollStartClamp`, applied AFTER the per-span decision loop). The
    /// widened result is capped to mark-only because the added prefix was not
    /// classified. A pre-roll begins at 0:00, but the ASR / transcript
    /// cold-start ramp routinely makes the detected start a few seconds late, and
    /// a pre-roll's start edge is "free" at 0:00 — so recovering it is a
    /// deterministic-DAI width win (measured pre-roll width coverage ~57% → ~80%).
    ///
    /// Unlike the OFF-by-default channels above, this clamp ships ON: its DEFAULT
    /// `20.0` is a live production knob, NOT a dormant flag. 20 s covers the
    /// typical cold-start miss with margin while staying far below any plausible
    /// mid-roll (the earliest mid-rolls land minutes in), so a "first" slot that
    /// starts later than this is treated as a mid-roll and left untouched. Set to
    /// `<= 0` to disable the clamp.
    let preRollStartClampSeconds: Double

    /// playhead-xsdz.65: recover the ad-pod NEIGHBOURS of an ad we already found
    /// (`AdPodContinuation`, applied as backfill Step 18b).
    ///
    /// A DAI pod is a chain of creatives; measured against rediff-confirmed pod
    /// boundaries, 13 of the 17 slots the pipeline detected at all carried an
    /// uncovered run longer than 30 s — 994 s of ad audio inside pods we had
    /// already located. This pass walks outward from a confirmed window over
    /// regions where the vetted lexical auto-ad rule fires, stopping at a
    /// positive content-resumed barrier, and emits the recovered material as NEW
    /// mark-only rows. It modifies no existing window.
    ///
    /// **SHIPS ON — flipped 2026-08-01 by playhead-eks2, on Dan's approval.**
    ///
    /// The evidence the xsdz.65 close gated on, in the order it matters:
    ///
    ///  • THE FIELD CASE. Episode D9B513CD (Conan, 65.5 min), ground truth =
    ///    Dan's own manual marks. Day-0 rediff found `0.0 – 45.1` (ad 1) with
    ///    its end edge 0.4 s from his mark, and MISSED `47.6 – 94.3` (ad 2)
    ///    entirely — two creatives 2.5 s apart in one pre-roll pod. Rediff
    ///    detects what CHANGED between two fetches and ad 2 was byte-identical
    ///    in both, so no rediff improvement reaches it. Recovering a found ad's
    ///    pod NEIGHBOURS is the only mechanism that can.
    ///
    ///  • THE CORPUS A/B (`AdPodContinuationCorpusEvalTests`, run against
    ///    byte-derived rediff pod boundaries). At the SHIPPING arm the newly
    ///    claimed seconds landing outside every rediff slot are 0.0, asserted as
    ///    a frozen budget rather than printed; no detected slot loses coverage;
    ///    uncovered ad seconds inside detected slots fall and the count of slots
    ///    still carrying a >30 s hole falls.
    ///
    ///  • THE BOUNDED DOWNSIDE. Every emitted row is `markOnly` / `candidate`
    ///    with both edge anchors `unanchored`, so playhead-2350 blocks auto-skip
    ///    and playhead-ynmk makes a banner confirmation MARK rather than cut.
    ///    A wrong continuation therefore costs a wrong BANNER, never lost show.
    ///    Both gates are asserted against rows `runBackfill` actually persisted
    ///    at this flag's shipped value in `AdPodContinuationFlipTests`.
    ///
    /// **THE FIRST-LISTEN GAP, CLOSED BY playhead-evc1.** For one day this flag
    /// shipped ON and still recovered nothing from the state Dan's device was
    /// actually in: `AdPodContinuation.seedDecisionStates` is
    /// `{confirmed, applied}` and `mintByteExactDayZeroMarks` persists a day-0
    /// byte-exact rediff mark as `.candidate` for life, so the ONLY row that had
    /// found ad 1 could not seed the mechanism built to find its neighbours.
    /// `AdPodContinuation.isDayZeroByteExactSeed` now admits that row on
    /// PROVENANCE — the single-writer day-0 `boundaryState` plus
    /// `.rediffByteExact` on both edges — while the aggregator's coarse
    /// `.candidate` tiles stay refused. `AdPodContinuationDayZeroSeedTests`
    /// carries the field case and the exclusion sweep.
    ///
    /// **THE HONEST LIMIT ON THE MEASUREMENT.** The corpus A/B could not have
    /// caught the gap and cannot evaluate the fix: the dump is 132 of 132
    /// `confirmed`/`eligible` windows with ZERO day-0 rows, and every corpus
    /// artifact in the repo contains zero occurrences of the day-0 boundary
    /// state. The evidence for the carve-out is the field case plus the bounded
    /// downside below, not a corpus number.
    let podContinuationEnabled: Bool

    /// playhead-y3ya: compose a MARK-ONLY candidate from a semantic `containsAd`
    /// verdict that fusion could not attach to anything.
    ///
    /// **The field case.** 2026-08-01, episode DE0784D8. `semantic_scan_results`
    /// records `containsAd` for 508–599 s and 1604–1731 s, and there is NO
    /// `ad_window` anywhere near either. FM evidence reaches fusion only through
    /// `buildFMLedgerEntries`, which decorates EXISTING `DecodedSpan`s — it can
    /// never create one, and `RegionProposalBuilder` iterates `window.spans`, so
    /// a coarse window with no refined spans proposes no region either. A
    /// sweep-lane verdict with no narrow seed under it therefore contributed to
    /// nothing. Presence without extent was discarded, which is the detection
    /// portfolio's stated policy — any signal fires → banner — violated in the
    /// semantic lane alone.
    ///
    /// **What flipping this OFF costs and restores.** OFF short-circuits BEFORE
    /// any fetch/compose/write, so the backfill is byte-identical to pre-y3ya
    /// (zero `ad_windows` writes, no banner) and the verdicts go back to being
    /// silently dropped. It ships ON because the marks are `markOnly` +
    /// `.candidate` + `.unanchored` on both edges by construction — so
    /// playhead-2350's gate has nothing to demote and playhead-ynmk makes a
    /// confirmation a MARK rather than a cut. The worst case is a wrong BANNER,
    /// never lost show.
    ///
    /// This is deliberately NOT a threshold change. No fusion weight, gate or
    /// promotion rule moves; the near-zero-confidence acoustic population is
    /// untouched and is only ever an INPUT to this composer's dedupe.
    let semanticSweepMarkEnabled: Bool

    /// playhead-lxkq: order the FM coarse sweep by AD-LIKELIHOOD rather than by
    /// episode position.
    ///
    /// **The measurement.** On episode DE0784D8 FM's 42 `semantic_scan_results`
    /// rows cover 0-2676 s, linearly, front to back, ending
    /// `2581-2676 | abstain | cancelled`. Dan's missed pod is at 2838-2954 s.
    /// It had ~fifteen hours of download-to-play headroom and spent all of it
    /// on the first 48% of the episode. At 2.4x slower than realtime, ten
    /// 3-minute neighbourhoods fit an overnight gap in ~72 minutes; a 3.7-hour
    /// linear sweep does not. The budget was never the problem; the order was.
    ///
    /// **This is a scheduling change, not a detection change.** The seeds are
    /// pointers, never verdicts — the acoustic seam that fired at 2828-2836 on
    /// that episode carried 6.7e-6 confidence, which is a terrible answer and
    /// an excellent pointer (`feedback_lexical_as_attention`). No threshold,
    /// fusion weight or gate moves; the same windows are scanned, in a
    /// different order, and `AdLikelihoodScanOrder.order` is a permutation so
    /// nothing can be starved.
    ///
    /// Ships ON. OFF restores the pre-lxkq front-to-back sweep exactly.
    let adLikelihoodScanOrderEnabled: Bool

    /// playhead-hvk0: require a full rescan to have MEASURABLY read its episode
    /// before its recall sample may certify the `targetedWithAudit` policy for
    /// the show.
    ///
    /// **The circularity this closes.** `CoveragePlanner` switches a show to
    /// `targetedWithAudit` — which narrows scanning to anchor windows and never
    /// re-reads an episode — as soon as `observedEpisodeCount >= 5 && stableRecall`.
    /// `stableRecall` is three full-rescan recall samples at or above 0.85, and a
    /// recall sample asks "of the ad lines THIS RESCAN found, what fraction would
    /// the targeted phases also have scanned". Its denominator is produced by the
    /// rescan itself, so a rescan that read almost none of the episode finds
    /// almost no ad lines, the targeted phases trivially cover them, and the
    /// sample reads 1.0. Measured on the 2026-07-29 device pull: every
    /// `fullEpisodeScan` on `feeds.simplecast.com/dHoohVNH` examined 82–167 s of
    /// episodes 1,503–4,379 s long (2–5%), and those runs are exactly what put
    /// three 1.0 samples in the ring and promoted the show.
    ///
    /// When `true`, a full rescan whose measured
    /// ``AnalysisCoverageSummary/adScanFraction`` is below
    /// ``AnalysisJobRunner/semanticBackfillSufficientAdScanFraction`` — or is not
    /// measurable at all — contributes no recall sample, does not restart the
    /// periodic re-validation clock, and forces the show's stable-recall flag
    /// false, so its next episode is planned with a full-episode phase. A rescan
    /// that does read its episode re-validates the show and restores the flag
    /// from the untouched ring.
    ///
    /// **SHIPS `false`, and that is a DECISION REQUEST, not caution.** The floor
    /// is deliberately the shared 0.98 constant rather than a new tunable — but
    /// on the 2026-07-29 device pull NO episode reaches it: the maximum measured
    /// `adScanFraction` across all 19 assets with a coverage-lane row is 0.943,
    /// and 0 of 34 episodes over 15 minutes clear 0.95. So with this `true`, no
    /// show is ever certified, and the steady state is a full-episode plan on
    /// every episode of every show — `targetedWithAudit`, the narrower, the audit
    /// sampler and the au2v.1.14 chapter-informed selection all become
    /// unreachable at runtime.
    ///
    /// That may well be the correct product answer — narrowing a scan on a show
    /// whose full rescans examined 82–167 s of a 1,503–4,379 s episode is
    /// indefensible at any threshold — but it retires a policy, and the measured
    /// price is steep: on this device a `fullEpisodeScan` costs 12–45 minutes of
    /// Foundation Models wall-clock per episode (120 calls, 13,407 s total) of
    /// which **92% is spent on calls that fail and yield no coverage**, against
    /// ~4 minutes for a targeted plan. Retiring a policy and paying that per
    /// episode is Dan's call under CLAUDE.md's Decision Authority rule, so the
    /// mechanism lands tested and inert and the flip is a separate, reviewed act.
    ///
    /// **What it cannot do, either way.** It is not a retry, not a widen and not
    /// a re-drive: it only decides WHICH plan an already-caused pass produces. It
    /// cannot cause a pass, so it adds no scanning to an episode nothing was
    /// going to scan anyway, and it re-opens no terminal. One caveat worth naming
    /// (it is not "only a plan choice" in one respect): flipping a show's plan
    /// shape changes `BackfillJobRunner`'s deterministic jobId inputs, so the
    /// first demoted pass mints a fresh `fullEpisodeScan` row with
    /// `retryCount == 0` rather than resuming the targeted rows' accrued budget.
    /// That is one extra attempt per asset per transcriptVersion, bounded by
    /// ``AdmissionController/maxRetries`` thereafter.
    let plannerPromotionRequiresMeasuredCoverage: Bool

    /// playhead-xsdz.11: assemble the `PerShowThresholdControllerParameters` from
    /// the per-knob config fields. The effective-threshold clamp is fixed at the
    /// bead-mandated `[0.55, 0.95]` (the controller is two-sided but must never
    /// move the threshold outside this band). Pure derivation (no actor state);
    /// exposed so the service, the store, and unit tests build the same
    /// parameter set.
    var perShowThresholdControllerParameters: PerShowThresholdControllerParameters {
        PerShowThresholdControllerParameters(
            proportionalGain: perShowThresholdProportionalGain,
            integralGain: perShowThresholdIntegralGain,
            maxOffset: perShowThresholdMaxOffset,
            minSamples: perShowThresholdMinSamples,
            effectiveMin: 0.55,
            effectiveMax: 0.95
        )
    }

    /// playhead-xsdz.10: assemble the `TemporalRegularizer.Parameters` from the
    /// per-knob config fields. Pure derivation (no actor state); exposed so the
    /// service and unit tests build the same parameter set.
    var temporalRegularizerParameters: TemporalRegularizer.Parameters {
        TemporalRegularizer.Parameters(
            neighborWindowSeconds: temporalNeighborWindowSeconds,
            highConfidenceNeighborThreshold: temporalHighConfidenceNeighborThreshold,
            isolationPenaltyFactor: temporalIsolationPenaltyFactor,
            minDwellSeconds: temporalMinDwellSeconds,
            minDwellPenaltyFactor: temporalMinDwellPenaltyFactor
        )
    }

    /// ef2.6.3: Derive ConfidenceBandThresholds from config fields for band classification.
    /// Requires candidate < markOnly < confirmation < autoSkip (asserted in debug).
    var bandThresholds: ConfidenceBandThresholds {
        ConfidenceBandThresholds(
            candidate: candidateThreshold,
            markOnly: markOnlyThreshold,
            confirm: confirmationThreshold,
            autoSkip: autoSkipConfidenceThreshold
        )
    }

    init(
        candidateThreshold: Double,
        confirmationThreshold: Double,
        suppressionThreshold: Double,
        hotPathLookahead: TimeInterval,
        detectorVersion: String,
        fmBackfillMode: FMBackfillMode = .full,
        fmScanBudgetSeconds: TimeInterval = 300,
        fmConsensusThreshold: Int = 2,
        markOnlyThreshold: Double = 0.60,
        autoSkipConfidenceThreshold: Double = 0.80,
        classifierSeedQualifiedThreshold: Double = 0.50,
        lexicalAutoAdQualifiedThreshold: Double = 0.50,
        lexicalAutoAdEnabled: Bool = false,
        segmentUICandidateThreshold: Double = 0.40,
        segmentAutoSkipThreshold: Double = 0.55,
        bracketRefinementEnabled: Bool = true,
        bracketRefinementMinTrust: Double = 0.40,
        bracketRefinementMinCoarseScore: Double = 0.30,
        bracketRefinementMinFineConfidence: Double = 0.20,
        transcriptBoundaryCueEnabled: Bool = true,
        evidenceFragilityPenaltyEnabled: Bool = false,
        fragilityThreshold: Double = 2.0,
        fragilityPenalty: Double = 0.85,
        chapterSignalMode: ChapterSignalMode = .off,
        audioForensicsEnabled: Bool = false,
        crossEpisodeMemoryEnabled: Bool = false,
        rhetoricalGrammarEnabled: Bool = false,
        crossShowSyndicationEnabled: Bool = false,
        temporalRegularizationEnabled: Bool = false,
        temporalNeighborWindowSeconds: TimeInterval = 120.0,
        temporalHighConfidenceNeighborThreshold: Double = 0.80,
        temporalIsolationPenaltyFactor: Double = 0.85,
        temporalMinDwellSeconds: TimeInterval = 10.0,
        temporalMinDwellPenaltyFactor: Double = 0.90,
        perShowThresholdControlEnabled: Bool = false,
        perShowThresholdProportionalGain: Double = 0.02,
        perShowThresholdIntegralGain: Double = 0.005,
        perShowThresholdMaxOffset: Double = 0.15,
        perShowThresholdMinSamples: Int = 5,
        spanFinalizerEnabled: Bool = false,
        spliceSlotOwnershipEnabled: Bool = false,
        spliceSlotShadowEnabled: Bool = false,
        rediffSlotOwnershipEnabled: Bool = true,
        rediffSlotShadowEnabled: Bool = false,
        rediffConfirmedKindEnabled: Bool = false,
        specialistShadowEnabled: Bool = false,
        specialistScanEnabled: Bool = false,
        specialistMarkComposeEnabled: Bool = false,
        userCorrectionReadSideEnabled: Bool = false,
        stingerRefinementEnabled: Bool = true,
        lexicalAnchorRefinementEnabled: Bool = false,
        selfPromoSuppressionEnabled: Bool = true,
        sustainedMusicProposerEnabled: Bool = true,
        musicOffsetLexicalGateEnabled: Bool = true,
        musicOffsetFMRecoveryEnabled: Bool = true,
        certaintyTieredSkipEnabled: Bool = true,
        hostReadConfidenceFloor: Double = 0.9,
        postRollGuardSeconds: Double = 90.0,
        unanchoredExtentBlocksAutoSkip: Bool = true,
        preRollStartClampSeconds: Double = 20.0,
        podContinuationEnabled: Bool = true,
        semanticSweepMarkEnabled: Bool = true,
        plannerPromotionRequiresMeasuredCoverage: Bool = false,
        adLikelihoodScanOrderEnabled: Bool = true
    ) {
        // Acoustic-splice and rediff are mutually-exclusive WIDTH setters: rediff
        // is the SOLE production width setter (contract 2026-07-07) and the
        // acoustic ownership pass is retired to shadow/eval. Enabling BOTH would
        // let the acoustic pass rewrite+persist `.spliceSlot` widths and then the
        // rediff pass re-rewrite them `.rediffSlot` on top — silent double-
        // ownership. Fail loudly at construction instead. Post Gate 1
        // (playhead-lq6f) rediff defaults ON, so any site that enables the
        // acoustic splice channel MUST pass `rediffSlotOwnershipEnabled: false`
        // explicitly (splice still defaults OFF, so the default config never
        // fires this; the splice test helpers all pass the explicit false).
        precondition(
            !(spliceSlotOwnershipEnabled && rediffSlotOwnershipEnabled),
            "spliceSlotOwnershipEnabled and rediffSlotOwnershipEnabled are mutually-exclusive width setters — enable at most one (rediff is the sole production width setter, playhead-xsdz.29)"
        )
        self.candidateThreshold = candidateThreshold
        self.confirmationThreshold = confirmationThreshold
        self.suppressionThreshold = suppressionThreshold
        self.hotPathLookahead = hotPathLookahead
        self.detectorVersion = detectorVersion
        self.fmBackfillMode = fmBackfillMode
        self.fmScanBudgetSeconds = fmScanBudgetSeconds
        self.fmConsensusThreshold = fmConsensusThreshold
        self.markOnlyThreshold = markOnlyThreshold
        self.autoSkipConfidenceThreshold = autoSkipConfidenceThreshold
        self.classifierSeedQualifiedThreshold = classifierSeedQualifiedThreshold
        self.lexicalAutoAdQualifiedThreshold = lexicalAutoAdQualifiedThreshold
        self.lexicalAutoAdEnabled = lexicalAutoAdEnabled
        self.segmentUICandidateThreshold = segmentUICandidateThreshold
        self.segmentAutoSkipThreshold = segmentAutoSkipThreshold
        self.bracketRefinementEnabled = bracketRefinementEnabled
        self.bracketRefinementMinTrust = bracketRefinementMinTrust
        self.bracketRefinementMinCoarseScore = bracketRefinementMinCoarseScore
        self.bracketRefinementMinFineConfidence = bracketRefinementMinFineConfidence
        self.transcriptBoundaryCueEnabled = transcriptBoundaryCueEnabled
        self.evidenceFragilityPenaltyEnabled = evidenceFragilityPenaltyEnabled
        self.fragilityThreshold = fragilityThreshold
        self.fragilityPenalty = fragilityPenalty
        self.chapterSignalMode = chapterSignalMode
        self.audioForensicsEnabled = audioForensicsEnabled
        self.crossEpisodeMemoryEnabled = crossEpisodeMemoryEnabled
        self.rhetoricalGrammarEnabled = rhetoricalGrammarEnabled
        self.crossShowSyndicationEnabled = crossShowSyndicationEnabled
        self.temporalRegularizationEnabled = temporalRegularizationEnabled
        self.temporalNeighborWindowSeconds = temporalNeighborWindowSeconds
        self.temporalHighConfidenceNeighborThreshold = temporalHighConfidenceNeighborThreshold
        self.temporalIsolationPenaltyFactor = temporalIsolationPenaltyFactor
        self.temporalMinDwellSeconds = temporalMinDwellSeconds
        self.temporalMinDwellPenaltyFactor = temporalMinDwellPenaltyFactor
        self.perShowThresholdControlEnabled = perShowThresholdControlEnabled
        self.perShowThresholdProportionalGain = perShowThresholdProportionalGain
        self.perShowThresholdIntegralGain = perShowThresholdIntegralGain
        self.perShowThresholdMaxOffset = perShowThresholdMaxOffset
        self.perShowThresholdMinSamples = perShowThresholdMinSamples
        self.spanFinalizerEnabled = spanFinalizerEnabled
        self.spliceSlotOwnershipEnabled = spliceSlotOwnershipEnabled
        self.spliceSlotShadowEnabled = spliceSlotShadowEnabled
        self.rediffSlotOwnershipEnabled = rediffSlotOwnershipEnabled
        self.rediffSlotShadowEnabled = rediffSlotShadowEnabled
        self.rediffConfirmedKindEnabled = rediffConfirmedKindEnabled
        self.specialistShadowEnabled = specialistShadowEnabled
        self.specialistScanEnabled = specialistScanEnabled
        self.specialistMarkComposeEnabled = specialistMarkComposeEnabled
        self.userCorrectionReadSideEnabled = userCorrectionReadSideEnabled
        self.stingerRefinementEnabled = stingerRefinementEnabled
        self.lexicalAnchorRefinementEnabled = lexicalAnchorRefinementEnabled
        self.selfPromoSuppressionEnabled = selfPromoSuppressionEnabled
        self.sustainedMusicProposerEnabled = sustainedMusicProposerEnabled
        self.musicOffsetLexicalGateEnabled = musicOffsetLexicalGateEnabled
        self.musicOffsetFMRecoveryEnabled = musicOffsetFMRecoveryEnabled
        self.certaintyTieredSkipEnabled = certaintyTieredSkipEnabled
        self.hostReadConfidenceFloor = hostReadConfidenceFloor
        self.postRollGuardSeconds = postRollGuardSeconds
        self.unanchoredExtentBlocksAutoSkip = unanchoredExtentBlocksAutoSkip
        self.preRollStartClampSeconds = preRollStartClampSeconds
        self.podContinuationEnabled = podContinuationEnabled
        self.semanticSweepMarkEnabled = semanticSweepMarkEnabled
        self.plannerPromotionRequiresMeasuredCoverage = plannerPromotionRequiresMeasuredCoverage
        self.adLikelihoodScanOrderEnabled = adLikelihoodScanOrderEnabled
    }

    static let `default` = AdDetectionConfig(
        candidateThreshold: 0.40,
        confirmationThreshold: 0.70,
        suppressionThreshold: 0.25,
        hotPathLookahead: 90.0,
        detectorVersion: "detection-v1",
        fmBackfillMode: .full,
        fmScanBudgetSeconds: 300,
        fmConsensusThreshold: 2,
        markOnlyThreshold: 0.60,
        autoSkipConfidenceThreshold: 0.80,
        classifierSeedQualifiedThreshold: 0.50,
        lexicalAutoAdQualifiedThreshold: 0.50,
        lexicalAutoAdEnabled: false,
        segmentUICandidateThreshold: 0.40,
        segmentAutoSkipThreshold: 0.55,
        bracketRefinementEnabled: true,
        bracketRefinementMinTrust: 0.40,
        bracketRefinementMinCoarseScore: 0.30,
        bracketRefinementMinFineConfidence: 0.20,
        transcriptBoundaryCueEnabled: true,
        evidenceFragilityPenaltyEnabled: false,
        fragilityThreshold: 2.0,
        fragilityPenalty: 0.85,
        chapterSignalMode: .off,
        audioForensicsEnabled: false,
        crossEpisodeMemoryEnabled: false,
        rhetoricalGrammarEnabled: false,
        crossShowSyndicationEnabled: false,
        temporalRegularizationEnabled: false,
        temporalNeighborWindowSeconds: 120.0,
        temporalHighConfidenceNeighborThreshold: 0.80,
        temporalIsolationPenaltyFactor: 0.85,
        temporalMinDwellSeconds: 10.0,
        temporalMinDwellPenaltyFactor: 0.90,
        perShowThresholdControlEnabled: false,
        perShowThresholdProportionalGain: 0.02,
        perShowThresholdIntegralGain: 0.005,
        perShowThresholdMaxOffset: 0.15,
        perShowThresholdMinSamples: 5,
        spanFinalizerEnabled: false,
        spliceSlotOwnershipEnabled: false,
        spliceSlotShadowEnabled: false,
        rediffSlotOwnershipEnabled: true,  // playhead-lq6f: flipped ON 2026-07-19 (Ship Gate 1) — rediff width marks, presence 97.8% gold-audited; the mark-only rung of the xsdz.36 ladder
        rediffSlotShadowEnabled: false,
        rediffConfirmedKindEnabled: false,  // playhead-xsdz.62: byte-exact rediff counts as a corroborating KIND; ships OFF (eligibility-affecting), flip after corpus A/B like certaintyTieredSkipEnabled. Byte-identical + inert until flipped.
        specialistShadowEnabled: false,  // playhead-dsbc (Phase B1): specialist-shadow plumbing ships OFF and fully inert; live runtime is phone-gated Phase B2
        specialistScanEnabled: false,  // playhead-b6jq (PR 4): host-read scan phase ships OFF and fully inert (persist-only, acts on nothing); PR 5 consumes the rows
        specialistMarkComposeEnabled: false,  // playhead-b6jq (PR 5): mark-compose ships OFF and fully inert (zero ad_windows writes, byte-identical); flip after corpus A/B
        userCorrectionReadSideEnabled: false,  // playhead-xsdz.34: read side ships OFF; xsdz.36 flips it after the corpus A/B
        stingerRefinementEnabled: true,
        lexicalAnchorRefinementEnabled: false,
        selfPromoSuppressionEnabled: true,  // playhead-fl4j: flipped ON 2026-07-16 — attention→verification rework measured 0/70 false-fires on real ads
        sustainedMusicProposerEnabled: true,  // playhead-lq6f: flipped ON 2026-07-19 (Ship Gate 1) — certified config measured 47.5% cov / 91.7% true prec / 6.0% false-banner; markOnly-only
        musicOffsetLexicalGateEnabled: true,  // playhead-lq6f: flipped ON 2026-07-19 (Ship Gate 1, same certified measurement as the proposer)
        musicOffsetFMRecoveryEnabled: true,  // playhead-lq6f: flipped ON 2026-07-19 (Ship Gate 1, same certified measurement as the proposer)
        certaintyTieredSkipEnabled: true,  // playhead-nqey: ships ON 2026-08-02 (Dan's Gate-2 decision, taken 2026-08-01 conditional on sik9 = #330). DEMOTIONS ONLY: the switch can move `.eligible → .markOnly` and nothing else, so ON is strictly more conservative than OFF — no new skip, no widened skip, no changed score. Sole consumer is DecisionMapper in runBackfill; day-0 rediff mints, hot-path and aggregator rows never reach it. (2026-07-19 gate-delta measurement 32/32 at T=0.9 + 90s post-roll.)
        hostReadConfidenceFloor: 0.9,  // playhead-wraj: T=0.9 themove host-read calibration (2026-07-17); armed since playhead-nqey
        postRollGuardSeconds: 90.0,  // playhead-wraj: post-roll guard window (Dan 2026-07-19); armed since playhead-nqey, with sik9's byte-anchored-inner-edge exemption
        unanchoredExtentBlocksAutoSkip: true,  // playhead-2350: SAFETY gate ships ON — a span with an invented (unanchored) start or end edge is banner-only, never auto-skip. Only ever demotes.
        preRollStartClampSeconds: 20.0,  // playhead-xsdz.66: pre-roll start-at-zero clamp ships ON — widened material is mark-only; 20s covers the cold-start miss, far below any mid-roll
        podContinuationEnabled: true,  // playhead-eks2: flipped ON 2026-08-01 (Dan) — the corpus A/B the xsdz.65 close gated on measures 0.0 newly-claimed seconds outside a byte-confirmed DAI slot at the shipping arm, and the output is mark-only/candidate/unanchored, so the worst case is a wrong BANNER (playhead-2350 + ynmk both hold, pinned by AdPodContinuationFlipTests)
        semanticSweepMarkEnabled: true,  // playhead-y3ya: ships ON. A semantic containsAd verdict fusion could not attach becomes a MARK-ONLY candidate; OFF restores the pre-y3ya silent drop. Marks are markOnly/candidate/unanchored by construction, so 2350 and ynmk both hold and the worst case is a wrong BANNER (pinned by SemanticSweepArmsSuggestTests)
        plannerPromotionRequiresMeasuredCoverage: false,  // playhead-hvk0: the read-evidence promotion gate ships OFF and fully inert. The mechanism is correct and mutation-tested, but at the shared 0.98 floor NO episode on the 2026-07-29 device pull qualifies (max measured adScanFraction 0.943), so flipping it retires `targetedWithAudit` entirely and buys a full-episode plan on every episode at a MEASURED 12-45 min of FM wall-clock each, 92% of it spent on calls that fail and yield no coverage. Retiring a policy at that price is Dan's decision under CLAUDE.md Decision Authority — see the field doc.
        adLikelihoodScanOrderEnabled: true  // playhead-lxkq: ships ON. Orders the FM coarse sweep by ad-likelihood instead of episode position. A SCHEDULING change only — `AdLikelihoodScanOrder.order` is a permutation, so the same windows are scanned and none can be starved; with no usable seed it is the identity and the sweep is the pre-lxkq linear one. Measured motivation: DE0784D8 swept 0-2676s linearly in ~15h and never reached the pod at 2838-2954, while the acoustic seam channel had already fired at 2828-2836.
    )

    /// playhead-fqc8: Pure helper that returns the active auto-skip
    /// threshold for a given `PromotionTrack`. Centralizing the switch
    /// here (rather than inlining at the call site in the service) keeps
    /// the threshold lookup unit-testable without spinning up the actor.
    func effectiveAutoSkipThreshold(for track: PromotionTrack) -> Double {
        switch track {
        case .standard:
            return autoSkipConfidenceThreshold
        case .classifierSeedQualified:
            return classifierSeedQualifiedThreshold
        case .lexicalAutoAdQualified:
            // playhead-xsdz.1: the high-precision lexical auto-ad track. See
            // `lexicalAutoAdQualifiedThreshold` for the rationale (the
            // `.lexical` family is capped at 0.20, so the standard 0.80 gate
            // is structurally unreachable through the lexical channel).
            return lexicalAutoAdQualifiedThreshold
        }
    }

    /// playhead-xsdz.7: Evidence-Fragility precision gate. A pure,
    /// deterministic post-fusion scoring adjustment that softly penalizes a
    /// span's `skipConfidence` when its evidence geometry looks BRITTLE — the
    /// hallmark of a false positive that cleared the auto-skip threshold on
    /// thin support rather than robust, independent corroboration.
    ///
    /// This is NOT a new evidence channel: it reads only the EXISTING ledger
    /// geometry and the decision's own confidence / track. It is applied
    /// BEFORE the hard eligibility / auto-skip gates and NEVER hard-blocks —
    /// the worst it can do is multiply `skipConfidence` by `fragilityPenalty`.
    ///
    /// Formula (ε = `Self.fragilityEpsilon`):
    ///   - `margin        = proposalConfidence − effectiveAutoSkipThreshold(track)`
    ///   - `concentration = maxSingleEntryWeight / max(proposalConfidence, ε)`
    ///   - `depth         = #distinct SourceEvidenceFamily with weight > 0`
    ///   - `fragility      = concentration / max(margin, ε) / max(depth, 1)`
    ///
    /// High fragility ⇒ likely false positive. When
    /// `evidenceFragilityPenaltyEnabled` is `false` the input `skipConfidence`
    /// is returned unchanged (byte-identical to pre-xsdz.7). When `true` and
    /// `fragility > fragilityThreshold`, the returned value is
    /// `skipConfidence * clamp(fragilityPenalty, 0, 1)`.
    ///
    /// - Parameters:
    ///   - skipConfidence: the decision's current skip/proposal confidence
    ///     (the value the hard gate would compare against the threshold).
    ///   - proposalConfidence: the span's fused proposal confidence (used for
    ///     the margin and concentration terms).
    ///   - promotionTrack: selects the effective threshold for the margin.
    ///   - ledger: the (post-suppression) evidence ledger for the span.
    /// - Returns: the possibly-penalized skip confidence, clamped to [0, 1].
    func applyFragilityPenalty(
        skipConfidence: Double,
        proposalConfidence: Double,
        promotionTrack: PromotionTrack,
        ledger: [EvidenceLedgerEntry]
    ) -> Double {
        guard evidenceFragilityPenaltyEnabled else { return skipConfidence }
        // Non-finite confidence is a data-integrity error; leave it untouched
        // so the existing non-finite guards downstream stay in control.
        guard skipConfidence.isFinite else { return skipConfidence }

        let fragility = fragilityScore(
            proposalConfidence: proposalConfidence,
            promotionTrack: promotionTrack,
            ledger: ledger
        )
        guard fragility.isFinite, fragility > fragilityThreshold else {
            return skipConfidence
        }

        let penalty = max(0.0, min(1.0, fragilityPenalty))
        return max(0.0, min(1.0, skipConfidence * penalty))
    }

    /// playhead-xsdz.7: epsilon guarding the divisions in the fragility
    /// formula so a zero margin / zero proposal confidence cannot produce a
    /// NaN or `+inf`. Small relative to any real threshold or confidence.
    static let fragilityEpsilon: Double = 1e-6

    /// playhead-xsdz.7: pure fragility score from the ledger geometry. Exposed
    /// (internal) so unit tests can assert the formula's edge cases directly
    /// without exercising the enable/threshold branching in
    /// `applyFragilityPenalty`. See that method's doc comment for the formula.
    ///
    /// "Evidence families" are the canonical `SourceEvidenceFamily` buckets —
    /// the SAME taxonomy the orthogonal-corroboration rule uses
    /// (`SourceEvidenceFamily.for(_:)`) — restricted to scoring sources
    /// (observability-only rows are excluded) with strictly positive weight.
    /// Using the family taxonomy (not the raw `EvidenceSourceType`) keeps the
    /// depth term honest: `.acoustic` + `.musicBed` + `.breakAlignment` are
    /// one family (shared modality), so a span supported only by audio-derived
    /// signals is correctly treated as shallow.
    func fragilityScore(
        proposalConfidence: Double,
        promotionTrack: PromotionTrack,
        ledger: [EvidenceLedgerEntry]
    ) -> Double {
        let eps = Self.fragilityEpsilon

        // Automatic-decision entries with strictly-positive weight.
        // Observability rows and learned fingerprint-catalog diagnostics never
        // enter fusion and must not count toward concentration or depth.
        let scoringEntries = ledger.filter {
            $0.contributesToAutomaticDecision && $0.weight > 0
        }

        let maxSingleEntryWeight = scoringEntries.map(\.weight).max() ?? 0.0
        let concentration = maxSingleEntryWeight / max(proposalConfidence, eps)

        let distinctFamilies = Set(scoringEntries.map { SourceEvidenceFamily.for($0.source) })
        let depth = distinctFamilies.count

        let threshold = effectiveAutoSkipThreshold(for: promotionTrack)
        let margin = proposalConfidence - threshold

        return concentration / max(margin, eps) / Double(max(depth, 1))
    }
}

// MARK: - Bracket Refinement Telemetry

/// playhead-arf8: per-`runBackfill` aggregate counts for the bracket-
/// refinement gate. Lets tests assert how the live activation distributed
/// spans across the gate paths without scraping logs.
struct BracketRefinementCounts: Sendable, Equatable {
    /// Spans where the bracket-aware refiner actually moved the boundary
    /// (path == .bracketRefined). The legacy refiner did not run for these.
    var bracketRefined: Int = 0
    /// Spans where bracket refinement was active but the detector found
    /// no envelope (host-read ad copy with no music bed).
    var noBracket: Int = 0
    /// Spans where bracket evidence existed but the per-show trust gate
    /// suppressed it. Caller fell back to the legacy refiner.
    var trustGated: Int = 0
    /// Spans where bracket evidence existed but coarse score was below
    /// the floor. Caller fell back to the legacy refiner.
    var coarseGated: Int = 0
    /// Spans where bracket evidence existed but at least one fine
    /// boundary estimate was below the confidence floor. Caller fell
    /// back to the legacy refiner.
    var fineConfidenceGated: Int = 0
    /// Spans where the bracket path was bypassed by configuration
    /// (master flag off, or not enough feature windows).
    var legacyBypass: Int = 0
}

// MARK: - Stinger refinement plumbing (playhead-l2f.6)

/// Ranged PCM source for stinger search envelopes: `(episodeID, from, to)`
/// → decoded 16 kHz mono samples, or nil when the range is unavailable.
/// Production uses `AnalysisShardPCMReader.loadSamples`; tests inject
/// synthetic PCM.
typealias StingerPCMProvider = @Sendable (
    _ episodeID: String,
    _ startSeconds: Double,
    _ endSeconds: Double
) async -> StingerPCMSlice?

/// Per-run stinger context resolved once per `runBackfill` (flag ON + the
/// show has a bank entry). `episodeID` is nil when the asset row could not
/// be fetched — the refiner is still consulted so the trace records the
/// no-PCM outcome, but neither side can snap.
private struct StingerRefinementRunContext {
    let entry: StingerShowEntry
    let episodeID: String?
}

// MARK: - Lexical-anchor refinement plumbing (playhead-xsdz.37)

/// Per-run lexical context resolved once per `runBackfill` (flag ON + the show
/// has a bank entry). `anchors` is the show's EFFECTIVE anchor set (its
/// family-(a) attribution templates plus the generic family-(b) framing
/// phrases). Nil for an unknown show ⇒ never consulted.
private struct LexicalRefinementRunContext {
    let anchors: [LexicalAnchor]
}

// MARK: - Pending Backfill Decision

/// playhead-xsdz.10: a per-span decision captured AFTER the per-span scoring
/// (DecisionMapper → FM-suppression → content-chapter demotion → xsdz.7
/// fragility penalty → xsdz.9 negative-bank suppression) but BEFORE the hard
/// auto-skip gate and the side-effect emission (window build, decision events /
/// log). Catalog and recurrence ingress are intentionally deferred to
/// authoritative runtime confirmation paths.
///
/// The backfill loop collects these so the lightweight temporal-regularization
/// pass can see ALL of an episode's candidate detections together — neighbor
/// context is required to tell a clustered ad break from a lonely false
/// positive. With `temporalRegularizationEnabled == false` the collected
/// `decision` is passed through to the emission loop UNCHANGED, so the output
/// is byte-identical to pre-xsdz.10. The two-loop shape is the only structural
/// change; each emission step is verbatim the code that previously ran inline.
struct CatalogSpanEvidence: Sendable {
    let topSimilarity: Float
    let topMatch: CatalogMatch?
    let wasEvaluated: Bool
}

private struct PendingBackfillDecision {
    /// playhead-p56a: `var` (was `let`) so the SpanFinalizer wire-in can
    /// substitute time-adjusted `DecodedSpan`s into the pending record after
    /// finalizer constraints rewrite span bounds (trim/merge/split). The
    /// finalizer-off path is unchanged — this field is only reassigned when
    /// `config.spanFinalizerEnabled == true`.
    var refinedSpan: DecodedSpan
    var decision: DecisionResult
    let ledger: [EvidenceLedgerEntry]
    let effectiveLedger: [EvidenceLedgerEntry]
    let spanTopCatalogSimilarity: Float
    let spanTopCatalogMatch: CatalogMatch?
    let spanCatalogWasEvaluated: Bool
    /// True when SpanFinalizer changed either persisted time edge. Earlier
    /// boundary-refinement traces and exact anchors no longer describe the
    /// emitted material in that case.
    let finalizerRewroteGeometry: Bool
    /// Present only for an additional finalizer split child whose inherited
    /// atom ordinals would otherwise collide with the legacy fusion ID.
    let fusionSplitDiscriminator: String?
}

private struct PrecisionGateResult {
    let label: String?
    let catalogMatch: CatalogMatch?
    /// `nil` means catalog evaluation was unavailable; `0` is an observed
    /// exact-show miss; a positive value is the persisted top match.
    let catalogStoreMatchSimilarity: Double?
}

// MARK: - Decision State

/// Lifecycle of an AdWindow from detection through confirmation.
enum AdDecisionState: String, Sendable {
    /// Initial detection from hot path -- skip-ready but not yet confirmed.
    case candidate
    /// Confirmed by backfill re-classification with full context.
    case confirmed
    /// Suppressed: below threshold after backfill re-classification.
    case suppressed
    /// Skip was applied to the listener (audio was skipped).
    case applied
    /// User tapped "Listen" — skip reverted, plays through the ad.
    case reverted
}

// MARK: - Boundary State

/// How the window boundaries were derived.
enum AdBoundaryState: String, Sendable {
    /// Rough boundaries from lexical scanner only.
    case lexical
    /// Boundaries refined using acoustic feature transitions.
    case acousticRefined
    /// Span came from `SegmentAggregator` fusing multiple sub-threshold
    /// per-window scores into a coherent segment. Its extents are the
    /// aggregator's `[startTime, endTime)`; `gtt9.4.1` boundary expansion
    /// still composes independently on top. playhead-0usd.
    case segmentAggregated
}

// MARK: - AdDetectionProviding

/// Protocol abstraction for ad detection, enabling test stubs.
protocol AdDetectionProviding: Sendable {
    func runHotPath(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        episodeDuration: Double,
        podcastId: String?
    ) async throws -> [AdWindow]

    /// Cycle 4 H5: callers that know the analysis session id at dispatch
    /// time (e.g. `AnalysisCoordinator.finalizeBackfill`) pass it here so
    /// the shadow phase can stamp `needsShadowRetry` on the exact session
    /// without a `fetchLatestSessionForAsset` lookup that races concurrent
    /// reprocessing. Legacy callers that don't track sessions (e.g.
    /// `AnalysisJobRunner`, which operates on analysis_jobs not sessions)
    /// pass `nil` and the marker is skipped on bail — acceptable because
    /// pre-roll warmup does not yet have a user-facing session to retry.
    func runBackfill(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double,
        sessionId: String?
    ) async throws

    /// playhead-zx6i — B4 fast revalidation entry point. Re-runs only
    /// the classifier + fusion + boundary stages against the persisted
    /// `TranscriptChunk` rows for `analysisAssetId`, without re-running
    /// ASR / decode / feature extraction. Called by `AnalysisJobRunner`
    /// when the `b4_revalidation_from_features_enabled` flag is ON and
    /// the `RevalidationStateStore` reports a pipeline-version bump for
    /// this asset. Throws when persisted chunks cannot be fetched — the
    /// runner falls back to the full-analysis path in that case.
    func revalidateFromFeatures(
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double,
        sessionId: String?
    ) async throws

    /// playhead-xsdz.36.4 DAY-0 byte-exact mint: byte-align the PINNED played
    /// A-side (resolved read-only from the asset row) against the k-way day-0
    /// B-copies and persist a MARK-ONLY banner for every byte-EXACT,
    /// ≥2-persona-robust divergent slot — the FIRST-LISTEN marking path that
    /// needs NO persisted transcript/analysis.
    ///
    /// playhead-p70f: returns a `RediffDayZeroMintOutcome`, not a bare `Int`.
    /// The old `Int` collapsed nine distinct failure modes into one `0`, which
    /// is why 299.6 MB of day-0 fetches on the owner's device left no evidence
    /// of WHERE they died. The default is a no-op so test/stub conformers that
    /// never run a day-0 mint compile unchanged; `AdDetectionService` overrides.
    func mintByteExactDayZeroMarks(analysisAssetId: String, bSideURLs: [URL]) async -> RediffDayZeroMintOutcome

    /// playhead-p70f change 3: the FREE, LOCAL-ONLY half of the day-0 mint's
    /// preconditions — asset row present, `sourceURL` resolving to an anchored
    /// regular file, and that file mappable. Returns `nil` when the mint has a
    /// usable A-side, or the exit that will doom it.
    ///
    /// WHY IT EXISTS: all four of these checks used to run AFTER the ~108 MB
    /// k-way fetch had already been downloaded and billed. Hoisting them ahead
    /// of the fetch makes a doomed attempt cost ZERO bytes.
    func dayZeroPrefetchBlocker(analysisAssetId: String) async -> RediffDayZeroExit?
}

extension AdDetectionProviding {
    /// Compatibility overload for callers that genuinely lack show identity.
    /// Catalog matching fails closed in that case.
    func runHotPath(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        episodeDuration: Double
    ) async throws -> [AdWindow] {
        try await runHotPath(
            chunks: chunks,
            analysisAssetId: analysisAssetId,
            episodeDuration: episodeDuration,
            podcastId: nil
        )
    }
}

extension AdDetectionProviding {
    /// Default no-op: a conformer without a byte-exact day-0 mint path mints
    /// nothing. `AdDetectionService` provides the real implementation.
    func mintByteExactDayZeroMarks(
        analysisAssetId: String,
        bSideURLs: [URL]
    ) async -> RediffDayZeroMintOutcome {
        .blocked(.minterUnavailable)
    }

    /// Default: a conformer without a day-0 mint path has nothing to pre-check.
    /// Returning `nil` (not blocked) keeps the seam honest — the blocker's job
    /// is to name a KNOWN blocker, and "I don't implement day-0" is reported by
    /// `mintByteExactDayZeroMarks` above, not by inventing a fake blocker here.
    func dayZeroPrefetchBlocker(analysisAssetId: String) async -> RediffDayZeroExit? { nil }
}

extension AdDetectionProviding {
    /// Convenience for callers that don't track session ids. Delegates
    /// to the primary entry point with `sessionId: nil`.
    func runBackfill(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double
    ) async throws {
        try await runBackfill(
            chunks: chunks,
            analysisAssetId: analysisAssetId,
            podcastId: podcastId,
            episodeDuration: episodeDuration,
            sessionId: nil
        )
    }

    /// playhead-zx6i convenience overload for the sessionId-less call
    /// sites (mirrors the `runBackfill` overload above).
    func revalidateFromFeatures(
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double
    ) async throws {
        try await revalidateFromFeatures(
            analysisAssetId: analysisAssetId,
            podcastId: podcastId,
            episodeDuration: episodeDuration,
            sessionId: nil
        )
    }
}

// MARK: - AdDetectionService

/// Composes LexicalScanner (Layer 1), acoustic boundary refinement (Layer 0),
/// ClassifierService (Layer 2), and MetadataExtractor (Layer 3) into a
/// unified detection pipeline with hot-path and backfill flows.
actor AdDetectionService {
    /// `modelVersion` tag written to synthetic replay transcript chunks
    /// produced by `syntheticReplayChunk(...)` and surfaced by the
    /// Diagnostics UI as the detection model identifier. Exposed as a
    /// static constant so the Settings panel reads the same symbol the
    /// producer writes — see playhead-l274 code-review I1.
    static let hotPathReplayModelVersion: String = "hot-path-replay"

    struct HotPathRunResult: Sendable {
        let windows: [AdWindow]
        let retiredWindowIDs: Set<String>
    }

    private struct HotPathHypothesisCandidate: Sendable {
        let candidate: LexicalCandidate
        let evidenceCount: Int
        let hasClosingAnchor: Bool
        let supportingHits: [LexicalHit]
    }

    private struct ReconciledHotPathWindow: Sendable {
        let window: AdWindow
        let matchedExistingID: String?
        let retiredExistingIDs: Set<String>
        /// Exact rows observed while choosing the stable ID. The store uses
        /// these as compare-and-swap revisions for both update and retirement.
        let expectedExistingRevisions: [String: AdWindow]
    }

    private struct ReplaySignalProfile: Sendable {
        let hasSignal: Bool
        let hasDirectionalSignal: Bool
        let backwardReach: TimeInterval
        let forwardReach: TimeInterval

        static let none = ReplaySignalProfile(
            hasSignal: false,
            hasDirectionalSignal: false,
            backwardReach: 0,
            forwardReach: 0
        )
    }

    private static let hotPathCandidateIdentityTolerance: Double = 5
    /// playhead-9ro7: Dogfood false negatives are concentrated in
    /// pre-roll/post-roll slots where Tier 1 often sees exactly one
    /// candidate-strength 30 s window. Keep this recall rule boundary-
    /// scoped and mark-only-gated so mid-episode precision still relies
    /// on SegmentAggregator's N-nearby corroboration.
    private static let boundarySingletonStartWindowSeconds: Double = 120
    private static let boundarySingletonEndWindowSeconds: Double = 180
    private static let boundarySingletonMinimumWindowSeconds: Double = 20

    private let logger = Logger(subsystem: "com.playhead", category: "AdDetectionService")

    /// cycle-1 M2 / cycle-2 M1 / cycle-3 L-1: Static logger for `private
    /// static` helpers that need to log without `self` (callers today:
    /// `decodeAdDurationStats`, `mergedTraitProfileJSON`,
    /// `initialTraitProfileJSON`). These helpers run inside
    /// `store.mutateProfile` closures on the AnalysisStore actor and
    /// must not capture `self`. Same subsystem as the instance logger
    /// so DiagnosticReports group both streams under the
    /// AdDetectionService category.
    private static let staticLogger = Logger(
        subsystem: "com.playhead",
        category: "AdDetectionService"
    )

    // MARK: - Dependencies

    private let store: AnalysisStore
    private let classifier: ClassifierService
    private let metadataExtractor: MetadataExtractor
    private let config: AdDetectionConfig
    /// Optional factory that returns a `BackfillJobRunner` for the FM shadow
    /// phase. When `nil`, FM is skipped entirely (equivalent to .off).
    /// Tests inject a deterministic runner; production wiring lives in
    /// `PlayheadRuntime`.
    private let backfillJobRunnerFactory: (@Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner)?
    /// M-D: predicate the runner consults before doing any shadow-phase work
    /// (atomization, segmentation, catalog build). Returning `false` makes the
    /// phase an immediate no-op on devices that cannot run Foundation Models
    /// — previously we built the entire input graph and then let the runner
    /// tear it down inside the factory closure. Production wiring captures a
    /// reference to `CapabilitiesService.currentSnapshot`; tests default to
    /// `{ true }` so existing fixtures continue to exercise the shadow path.
    private let canUseFoundationModelsProvider: @Sendable () async -> Bool
    /// bd-3bz (Phase 4) / H7 (cycle 2): called from `runShadowFMPhase` when
    /// the shadow guard bails on `canUseFoundationModels == false`, so the
    /// session can be flagged for a later retry.
    ///
    /// H7 fix: the marker now receives an explicit `sessionId` captured at
    /// the START of the shadow phase, before any concurrent reprocessing
    /// can race a fresh session row in for the same asset. The previous
    /// closure shape was `(assetId, podcastId)` and the runtime side did a
    /// `fetchLatestSessionForAsset` lookup at marker time, which under
    /// concurrent reprocessing could mark the wrong (newer) session. Tests
    /// that don't care about FM availability default this to a no-op.
    private let shadowSkipMarker: @Sendable (_ sessionId: String, _ podcastId: String) async -> Void
    /// playhead-xba (Phase 4 shadow wire-up): optional observation-only sink
    /// for `RegionProposalBuilder` + `RegionFeatureExtractor` output. When
    /// `nil`, the Phase 4 shadow phase inside `runBackfill` is a no-op — no
    /// atomization, no region building, no feature extraction. Production
    /// release builds construct this service with `nil`, mirroring the
    /// DEBUG-only `FoundationModelsFeedbackStore` pattern on
    /// `PlayheadRuntime`. Tests inject a live observer and assert that the
    /// pipeline produced bundles.
    private let regionShadowObserver: RegionShadowObserver?

    /// playhead-r2vz (PR2): optional FM recovery dispatcher for cue-less
    /// music-only spans the lexical gate would suppress. `nil` (the production
    /// default until measurement, and always in tests / preview / on devices
    /// that can't run FM) ⇒ `runBackfill` builds NO recovery closure, so the
    /// `RegionShadowPhase` partition-and-recover branch is inert and the region
    /// pipeline is byte-identical to PR1. When non-nil, `runBackfill` adapts it
    /// into the `@Sendable` recovery closure it hands to `RegionShadowPhase.Input`
    /// — but only after confirming `canUseFoundationModelsProvider`. Mirrors the
    /// `canUseFoundationModelsProvider` FM-provider injection and the observer
    /// nil-default pattern. Wired in `PlayheadRuntime` behind `!isPreviewRuntime`.
    private let fmRegionRecoveryDispatcher: FMRegionRecoveryDispatcher?

    /// playhead-4my.5 (Phase 5): optional observer for the AtomEvidenceProjector
    /// + MinimalContiguousSpanDecoder pipeline. When nil, step 11 is a no-op.
    /// Production release builds never inject this (DEBUG-only pattern, same as
    /// `regionShadowObserver`). Tests inject a live observer to assert Phase 5
    /// output without affecting live AdWindow or skip-cue decisions.
    private let phase5ProjectorObserver: Phase5ProjectorObserver?

    /// playhead-xsdz.7 fragility diagnostic: optional observation-only sink for
    /// the per-span Evidence-Fragility geometry. When nil (the production
    /// default — PlayheadRuntime never constructs one), the per-span diagnostic
    /// fire site in the decision path is a no-op, so the decision output is
    /// byte-identical and there is zero footprint. Tests inject a live observer
    /// to dump the per-span fragility-score distribution. Mirrors the
    /// `regionShadowObserver` / `phase5ProjectorObserver` nil-default pattern;
    /// it NEVER feeds back into the decision, so it cannot change any gate.
    private let fragilityDiagnosticObserver: FragilityDiagnosticObserver?

    /// playhead-brandab fire instrumentation: optional observation-only sink that
    /// tallies, per decoded span, whether the two brand-appearance precision
    /// channels (`.rhetoricalGrammar` / `.crossShowSyndication`) emitted a ledger
    /// entry. When nil (the production default — PlayheadRuntime never constructs
    /// one), the tap fire site in the decision path is a no-op, so the decision
    /// output is byte-identical and there is zero footprint. The brand-appearance
    /// live A/B injects a live observer so a null lift is interpretable (did the
    /// channel never fire, or fire-but-no-effect?). Mirrors the
    /// `fragilityDiagnosticObserver` nil-default pattern; it NEVER feeds back into
    /// the decision, so it cannot change any gate.
    private let brandAppearanceChannelTapObserver: BrandAppearanceChannelTapObserver?

    /// playhead-actempo.10 fire instrumentation: optional observation-only sink
    /// that records, per asset, how many candidate detections the xsdz.10
    /// temporal-regularization pass actually changed (penalty applied). When nil
    /// (the production default — PlayheadRuntime never constructs one), the fire
    /// site after the regularization pass is a no-op, so the decision output is
    /// byte-identical and there is zero footprint. The temporal-reg live A/B
    /// injects a live observer so a null lift is interpretable (did the penalty
    /// pass move any span, or fire-but-no-net-effect?). Mirrors the
    /// `fragilityDiagnosticObserver` / `brandAppearanceChannelTapObserver`
    /// nil-default pattern; it NEVER feeds back into the decision, so it cannot
    /// change any gate.
    private let temporalRegularizationObserver: TemporalRegularizationObserver?

    /// playhead-xsdz.21 (Bead C): optional observation-only sink for the
    /// splice-slot SHADOW pass. When nil (the production default — PlayheadRuntime
    /// never constructs one), the shadow pass still emits its per-span breadcrumb
    /// (when the shadow flag is on) but accumulates no structured rows. Tests /
    /// the dogfood-capture export inject a live observer to read the rows back.
    /// Mirrors the `fragilityDiagnosticObserver` nil-default pattern; it NEVER
    /// feeds back into the decision path, so it cannot change any gate, and the
    /// shadow pass NEVER mutates the decoded-span set — output stays byte-identical
    /// to the flag-OFF pipeline.
    private let spliceSlotShadowObserver: SpliceSlotShadowObserver?

    /// playhead-xsdz.29: the re-fetched B-side audio source for the rediff width
    /// oracle. `nil` in production (PlayheadRuntime never constructs one — the
    /// live re-fetch scheduler + as-played tap are DEFERRED to xsdz.28 / a
    /// follow-up), so the rediff pass no-ops and the pipeline is byte-identical.
    /// Tests inject a provider to exercise the offline oracle. It NEVER feeds
    /// back into presence decisions — rediff owns WIDTH only.
    private let rediffBSideProvider: RediffBSideProvider?

    /// playhead-xsdz.29: observation-only sink for REDIFF-sourced shadow rows,
    /// separate from `spliceSlotShadowObserver` so rediff and acoustic rows never
    /// comingle. `nil` in production; injected by dogfood-capture / tests. NEVER
    /// feeds back into the decision path.
    private let rediffSlotShadowObserver: SpliceSlotShadowObserver?

    /// playhead-fbsignals.9 fire instrumentation: optional observation-only sink
    /// that records, per asset, how many candidate spans the xsdz.9 HARD-NEGATIVE
    /// suppression actually moved (the NON-ledger half of cross-episode memory; the
    /// positive `.crossEpisodeMemory` boost is counted by the channel tap instead).
    /// When nil (the production default — PlayheadRuntime never constructs one), the
    /// fire site at the suppression step is a no-op, so the decision output is
    /// byte-identical and there is zero footprint. The cross-episode-memory live A/B
    /// injects a live observer so a null lift is interpretable (with an EMPTY
    /// cold-start bank the expected fire count is 0). Mirrors the
    /// `temporalRegularizationObserver` nil-default pattern; it NEVER feeds back into
    /// the decision, so it cannot change any gate.
    private let negativeBankSuppressionObserver: NegativeBankSuppressionObserver?

    /// playhead-fbsignals.11 fire instrumentation: optional observation-only sink
    /// that records, per asset, the per-show OFFSET the xsdz.11 controller resolved
    /// for the backfill and how many `.standard`-track spans the offset actually
    /// shifted the effective auto-skip threshold for. When nil (the production
    /// default — PlayheadRuntime never constructs one), both fire sites are no-ops,
    /// so the decision output is byte-identical and there is zero footprint. The
    /// per-show-threshold live A/B injects a live observer so a null lift is
    /// interpretable (with an EMPTY cold-start controller the resolved offset is
    /// always 0). Mirrors the `temporalRegularizationObserver` nil-default pattern;
    /// it NEVER feeds back into the decision, so it cannot change any gate.
    private let perShowThresholdOffsetObserver: PerShowThresholdOffsetObserver?

    /// Phase 6.5 (playhead-4my.16): optional skip orchestrator. When non-nil, eligible
    /// fusion decisions are forwarded after each backfill run, enabling Phase 7
    /// (UserCorrections) to have banner impressions to correct against.
    /// Production wiring lives in PlayheadRuntime. Tests inject a real orchestrator
    /// to assert that results flow through; nil suppresses the forwarding call.
    private let skipOrchestrator: SkipOrchestrator?

    /// playhead-gtt9.17: optional on-device ad-catalog store. When non-nil,
    /// `runBackfill` queries the store for each decoded span (egress).
    /// Authoritative ingress lives in `SkipOrchestrator` consumption /
    /// confirmation and the user-marked-ad path; proposals never write here.
    /// `nil` preserves the no-catalog behavior — no catalog evidence in the
    /// ledger and `lastCatalogMatchSimilarity` stays at 0. Production wires a
    /// real `AdCatalogStore`; tests inject a temp-dir store or leave nil.
    private let adCatalogStore: AdCatalogStore?

    /// playhead-xsdz.9: optional HARD-NEGATIVE fingerprint bank. When non-nil
    /// AND `config.crossEpisodeMemoryEnabled` is true, `runBackfill` queries the
    /// bank for each candidate span and multiplicatively suppresses its skip
    /// confidence when the span's transcript tokens align (Smith-Waterman) to a
    /// confirmed false positive. `nil` OR flag-off ⇒ no bank read and no
    /// suppression (byte-identical pre-xsdz.9). Production wires a real bank;
    /// tests inject a temp-dir bank or leave nil.
    private let negativeFingerprintBank: NegativeFingerprintBank?

    /// playhead-xsdz.9: optional CONFIRMED-AD (positive) fingerprint store,
    /// reused from Phase 9. When non-nil AND `config.crossEpisodeMemoryEnabled`
    /// is true, `buildEvidenceLedger` reads the active confirmed-ad sequences
    /// for the show and emits a capped `.crossEpisodeMemory` boost entry when a
    /// candidate aligns (Smith-Waterman) to one. `nil` OR flag-off ⇒ no boost
    /// entry (byte-identical pre-xsdz.9).
    private let adCopyFingerprintStore: AdCopyFingerprintStore?

    /// playhead-xsdz.9: pure evaluator that turns Smith-Waterman alignment
    /// results into the suppression factor + positive boost entries. Stateless;
    /// constructed once. Inert unless the bank/store reads above feed it.
    private let crossEpisodeMemoryEvaluator = CrossEpisodeMemoryEvaluator()

    /// playhead-xsdz.13: optional cross-show syndication observation store. When
    /// non-nil AND `config.crossShowSyndicationEnabled` is true, `runBackfill`
    /// records each above-min-confidence sponsor ENTITY (keyed by show) and reads
    /// the cross-show spread profile to emit a capped `.crossShowSyndication`
    /// boost entry when an entity has high cross-show spread AND temporal
    /// persistence. `nil` OR flag-off ⇒ no store read/write and no boost entry
    /// (byte-identical pre-xsdz.13). Production wires a real store ONLY when the
    /// flag is on; tests inject a temp-dir store or leave nil.
    private let crossShowSyndicationStore: CrossShowSyndicationStore?

    /// playhead-xsdz.13: pure evaluator that turns a `CrossShowSpreadProfile`
    /// into the (at most one) capped `.crossShowSyndication` boost entry.
    /// Stateless; constructed once. Inert unless the store reads above feed it.
    private let crossShowSyndicationEvaluator = CrossShowSyndicationEvaluator()

    /// playhead-xsdz.11: optional per-show auto-skip threshold controller store.
    /// When non-nil AND `config.perShowThresholdControlEnabled` is true,
    /// `runBackfill` reads the show's current PI-controller offset ONCE at the
    /// top of the run and adds it to the global auto-skip threshold at the hard
    /// gate (clamped to [0.55, 0.95]). `nil` OR flag-off ⇒ NO store read and the
    /// gate uses the unmodified global threshold (byte-identical pre-xsdz.11).
    /// The WRITE path lives in `SkipOrchestrator` (folds correction signals into
    /// the same store). Production wires a real store ONLY when the flag is on;
    /// tests inject a temp-dir store or leave nil. Mutable so the runtime can
    /// install it post-init (mirrors the `setUserCorrectionStore` pattern).
    private(set) var perShowThresholdControllerStore: PerShowThresholdControllerStore?

    /// playhead-xsdz.13: minimum confidence for a sponsor-entity observation to
    /// be recorded into the cross-show store. Mirrors
    /// `KnowledgePromotionThresholds.minCandidateConfidence` (0.5) so the
    /// syndication store ingests the same vetted-quality entities the per-show
    /// knowledge store does — a low-confidence brand-span guess never pollutes
    /// the cross-show aggregate.
    private static let crossShowSyndicationMinWriteConfidence: Double = 0.5

    /// Phase 7.2: optional correction store. When non-nil, `runBackfill` pre-computes
    /// a per-span correction factor by querying the store's weighted corrections for
    /// the asset. The factor is passed to `DecisionMapper` so correction-suppressed
    /// spans gate to `.blockedByUserCorrection` without making the struct async.
    private(set) var correctionStore: (any UserCorrectionStore)?

    /// playhead-shjn: optional sponsor knowledge store. When non-nil,
    /// `runBackfill` hands it to `RegionShadowPhase.Input` so
    /// `SponsorKnowledgeMatcher` reads `.active` entries for the episode's
    /// podcast and emits `.sponsor`-origin proposals.
    ///
    /// Set post-init via `setSponsorKnowledgeStore(_:)`, mirroring
    /// `setUserCorrectionStore`. Until that setter runs — and in every test
    /// that does not call it — the property is nil, the shadow phase falls
    /// back to the legacy stub, and `sponsorMatches` stays `[]`: the safe
    /// default, byte-identical to pre-shjn behaviour.
    ///
    /// Why this exists at all: before shjn the ONLY production reader of an
    /// `.active` `SponsorKnowledgeEntry` was `ASRVocabularyProvider`
    /// (contextual strings for the transcriber). The store-backed
    /// `SponsorKnowledgeMatcher` overload had zero production callers
    /// because nothing populated this field.
    private(set) var sponsorKnowledgeStore: SponsorKnowledgeStore?

    /// playhead-q45f: the TrustScoringService that owns the per-show
    /// trust state machine. Set post-init via `setTrustScoringService(_:)`
    /// so the runtime can wire it without a circular init dependency
    /// (mirrors `setUserCorrectionStore`). Optional because legacy test
    /// factories that don't construct a TrustScoringService still need
    /// `recordListenRewind` to succeed (decision flip + event log row);
    /// in that case the trust mutation simply no-ops. Production wiring
    /// in `PlayheadRuntime` always installs a real service before the
    /// first user tap.
    private(set) var trustScoringService: TrustScoringService?
#if DEBUG
    private var postUserMarkCommitHandlerForTesting:
        (@Sendable () async -> Void)?
    private var listenRewindTrustHandlerForTesting:
        (@Sendable (String) async -> Void)?
    private var userMarkedAdDerivedWorkCount = 0
    private var userMarkedAdDerivedWorkWaiters:
        [CheckedContinuation<Void, Never>] = []
#endif

    /// playhead-z3ch: Provider for feed-description metadata. `runBackfill`
    /// queries it once per asset and synthesizes `.metadata` ledger entries
    /// that are clamped at `metadataCap` (0.15) and gated by the corroboration
    /// check in `BackfillEvidenceFusion`. Production wiring lives in
    /// `PlayheadRuntime` (SwiftData-backed lookup); tests inject a
    /// deterministic stub. Defaults to a `NullEpisodeMetadataProvider` so
    /// existing call sites continue to behave identically. Mutable so the
    /// runtime can install a SwiftData-backed implementation post-init
    /// (mirrors the `setUserCorrectionStore` pattern — the SwiftData
    /// `ModelContext` isn't available until after the runtime + container
    /// are both alive).
    private(set) var episodeMetadataProvider: EpisodeMetadataProvider

    /// playhead-2hpn: Optional per-show music-bed profile store. When
    /// `nil` (default — production wiring may leave unset; tests that
    /// don't exercise the flag pass `nil`), the scoped-music-bed code
    /// path is fully disabled regardless of the
    /// `scopedMusicBedGeneralization` flag. Mutable so the runtime can
    /// install a SwiftData-backed implementation post-init (mirrors the
    /// `setEpisodeMetadataProvider` pattern). When the store is wired
    /// AND the flag is ON, `runBackfill` reads the snapshot once per
    /// episode and writes back the post-episode mutation.
    private(set) var showMusicBedProfileStore: (any ShowMusicBedProfileResolving)?

    /// playhead-h6a6: Optional per-show capability-profile store. When
    /// `nil` (default — production wiring may leave unset; tests that
    /// don't exercise the flag pass `nil`), the capability-profile
    /// code path is fully disabled regardless of the
    /// `showCapabilityProfilesEnabled` flag. Mirrors the music-bed
    /// store seam above: mutable so the runtime can install a
    /// SwiftData-backed implementation post-init via
    /// `setShowCapabilityProfileStore(_:)`. When the store is wired
    /// AND the flag is ON, `runBackfill` reads the snapshot once at
    /// the start of the run (to derive the budget adjustment) and
    /// writes back the per-episode outcome once at the end of the
    /// run.
    private(set) var showCapabilityProfileStore: (any ShowCapabilityProfileResolving)?

    /// playhead-h6a6: Caller-supplied predicate that returns `true`
    /// iff the show's cohort has Phase-2 SLIs within defended bounds
    /// per playhead-d99. The runtime owns the SLI ledger lookup; this
    /// closure is the seam through which it's read. Defaults to
    /// `{ _ in false }` so a service constructed WITHOUT an SLI
    /// predicate keeps the profile pinned at `.unknown` (the
    /// activation floor is conservatively un-met). Tests that want to
    /// exercise the observed-profile path inject a `{ _ in true }`
    /// predicate. Mutable so the runtime can install the live
    /// predicate post-init.
    private(set) var capabilityProfileSLIGate: ShowCapabilitySLIGate = { _ in false }

    /// playhead-h6a6: Most recent capability-budget adjustment
    /// resolved for a `runBackfill` invocation. Overwritten at the
    /// start of each backfill (via the read-path block in
    /// `runBackfill`) so stale adjustments from a prior run cannot
    /// leak into a fresh one. Initial value is the `.unknown`-yielded
    /// baseline (multiplier 1.0, empty bias map) so a read BEFORE
    /// the first backfill is the no-modulation no-op. Flag-OFF runs
    /// resolve to the same `.unknown`-yielded value (the snapshot is
    /// nil, the resolved kind is `.unknown`). Test-observable via
    /// `lastCapabilityBudgetAdjustmentForTesting()`.
    private var lastCapabilityBudgetAdjustment: ShowCapabilityBudgetAdjustment =
        ShowCapabilityBudgetModulator.adjustment(for: .unknown)

    /// playhead-2hpn: Cached snapshot of `PreAnalysisConfig` resolved at
    /// init from the persisted user config. Used by `runBackfill` to
    /// decide whether the scoped-music-bed-generalization flag is on
    /// for THIS process. Re-resolving inside the per-span loop would
    /// be wasteful — the flag is process-stable per-launch.
    ///
    /// Rollback latency: flipping the flag via Settings persists to
    /// `UserDefaults` immediately but this cache is only refreshed at
    /// the next `AdDetectionService` construction (i.e. next app
    /// launch). This matches the `xr3t` flag's rollback latency
    /// (consumer caches the value at init and re-reads on next init).
    /// Note: `24cm` — the OTHER `PreAnalysisConfig`-backed flag —
    /// additionally applies live via `DownloadManager
    /// .setUseDualBackgroundSessions(_:)` so its effective rollback is
    /// instant, NOT next-launch; the latency contract here is the
    /// next-init one, identical to `xr3t` (which persists to
    /// `LightweightInventoryChecksSettings`, not `PreAnalysisConfig`,
    /// but shares the same "read at consumer init" pattern). R11
    /// adversarial doc audit fix.
    private let preAnalysisConfig: PreAnalysisConfig = PreAnalysisConfig.load()

    /// playhead-8em9 (narL): Optional decision logger for offline replay.
    /// DEBUG-only; release builds keep the `NoOpDecisionLogger` default so
    /// no log file is ever written on a shipping binary.
    private(set) var decisionLogger: DecisionLoggerProtocol = NoOpDecisionLogger()

    /// playhead-gtt9.26: Versioned profile of fitted Platt-scaling
    /// coefficients applied to the post-fusion classifier score that
    /// drives `AutoSkipPrecisionGate`. Defaults to
    /// `ClassifierCalibrationProfile.production`, which currently ships
    /// empty so production behaviour is byte-identical to pre-gtt9.26
    /// — every lookup returns `.identity` (pass-through) until a fit is
    /// baked in. Tests inject `.empty` to assert the cold-start
    /// contract, or a fit-bearing profile to assert the calibration
    /// math is plumbed end-to-end.
    private let classifierCalibrationProfile: ClassifierCalibrationProfile

    // MARK: - Cached State

    /// Scanner is recreated per-episode when profile changes.
    private var scanner: LexicalScanner
    /// playhead-xsdz.1: High-precision lexical auto-ad rule. Stateless and
    /// show-agnostic (its negative-context patterns are built-in, not
    /// per-show), so unlike `scanner` it never needs recreation when the
    /// podcast profile changes. Constructed once.
    private let lexicalAutoAdBuilder = LexicalAutoAdEvidenceBuilder()

    /// playhead-xsdz.12: pure rhetorical act-sequence grammar detector. Built
    /// once per service; stateless and `Sendable`. Only consulted when
    /// `config.rhetoricalGrammarEnabled` is true (off by default), so its mere
    /// existence is inert on the production path.
    private let rhetoricalGrammarBuilder = RhetoricalGrammarEvidenceBuilder()
    /// playhead-xsdz.8: Composite audio-forensics boundary detector.
    /// Stateless, show-agnostic, and gated OFF by default
    /// (`config.audioForensicsEnabled`), so it is constructed once and only
    /// invoked when the flag is on.
    private let audioForensicsDetector = AudioForensicsBoundaryDetector()
    /// Per-show priors parsed from the current PodcastProfile.
    private var showPriors: ShowPriors
    /// playhead-8n1 / playhead-2kxd: the per-show `PodcastProfile` cache the
    /// Phase 4 shadow phase threads into `RegionFeatureExtractor` (which in
    /// turn constructs a `LexicalScanner` with per-show sponsor patterns).
    ///
    /// **KEYED BY THE SHOW IT BELONGS TO, and that is the whole point
    /// (playhead-2kxd).** Until this bead it was a single optional slot
    /// (`currentPodcastProfile`) naming "the current podcast" on an actor that
    /// more than one episode's analysis enters: `nowCap` is 2 and a playback
    /// job bypasses the cap entirely, so two episodes — possibly two different
    /// shows — can be in the pipeline at once. Worse, the slot was never
    /// LOADED for the episode being analysed; production never passes
    /// `podcastProfile:` at init, so the only writer was `updatePriors` at the
    /// END of a backfill. A read during show A's backfill therefore answered
    /// with whichever show last finished one — sequentially as well as
    /// concurrently.
    ///
    /// Three review rounds each hardened ONE read site and left the slot:
    /// `classifyCandidates` ("never fall back … that actor state may belong to
    /// the previous episode"), `resolveEpisodePriors` (snapshot `networkId`
    /// AND `observationCount` before the await), and skeptical-review-cycle-16
    /// M-1, which deleted the public `updateProfile(_:)` setter because "its
    /// post-hoc in-memory write could clobber an in-flight `updatePriors`".
    /// Vigilance per site, on a shape that keeps manufacturing sites.
    ///
    /// The cure is the one this repo has already proved twice —
    /// `AnalysisWorkScheduler.runningJobs` (`[String: RunningJob]`,
    /// playhead-lmrx R3) and `hotPathRunInFlightAssetIds` fifteen lines below
    /// — key the state on the identity it belongs to. Every read now names a
    /// show and gets that show's profile or nothing; a read that cannot name
    /// one FAILS CLOSED, which is the posture `classifyCandidates` already
    /// argued for in prose.
    ///
    /// **Reach it only through ``cachedPodcastProfile(forShowId:)`` and
    /// ``cachePodcastProfile(_:)``.** `AdDetectionServiceProfileKeyingCanaryTests`
    /// fails the build if any other line in this file mentions the storage, and
    /// if any read passes a literal instead of a show identity — a rule plus
    /// something that enforces it, because a prose survey of read sites decays
    /// the moment somebody adds one.
    ///
    /// **Bound on growth:** one entry per distinct show whose `updatePriors`
    /// completed in this process (plus at most one seeded at init), i.e. the
    /// number of subscribed shows analysed since launch — tens, holding a
    /// handful of small strings each. Entries are deliberately NOT evicted on
    /// completion the way `hotPathRunInFlightAssetIds` are: this is a cache of
    /// the latest persisted profile per show, not an in-flight registry.
    private var podcastProfilesByShowId: [String: PodcastProfile] = [:]

    /// playhead-2kxd: the ONLY read path for ``podcastProfilesByShowId``.
    ///
    /// Fails closed on a `nil` or empty show id rather than answering with
    /// some other show's profile — the same rule `classifyCandidates` applies
    /// to `RecurrenceMaterialIdentity.canonicalIdentifier`. An empty
    /// `podcastId` is already the codebase's spelling of "no show identity"
    /// (see `RegionShadowPhase.run` and `SemanticScanClaim.claimRow`), so it
    /// must not be a dictionary key.
    private func cachedPodcastProfile(forShowId showId: String?) -> PodcastProfile? {
        guard let showId, !showId.isEmpty else { return nil }
        return podcastProfilesByShowId[showId]
    }

    /// playhead-2kxd: the ONLY write path for ``podcastProfilesByShowId``.
    ///
    /// Keys on `profile.podcastId` — the identity the VALUE carries — rather
    /// than on a caller-supplied id, so a caller cannot file show B's profile
    /// under show A's name. Drops a profile with an empty id for the same
    /// reason the reader refuses one.
    private func cachePodcastProfile(_ profile: PodcastProfile) {
        guard !profile.podcastId.isEmpty else { return }
        podcastProfilesByShowId[profile.podcastId] = profile
    }

    /// playhead-2kxd: seed the per-show cache from the optional init-time
    /// profile. `static` because it runs during `init`, before `self` is
    /// available; the empty-id refusal mirrors ``cachePodcastProfile(_:)``.
    private static func seededProfileMap(
        _ profile: PodcastProfile?
    ) -> [String: PodcastProfile] {
        guard let profile, !profile.podcastId.isEmpty else { return [:] }
        return [profile.podcastId: profile]
    }

    #if DEBUG
    /// Test-only view of the per-show cache. Exists so a test can assert the
    /// KEYING directly — that a write for show B leaves show A's entry alone —
    /// without driving a full backfill. Goes through the production reader, so
    /// it cannot observe a state the production path cannot.
    func cachedPodcastProfileForTesting(showId: String?) -> PodcastProfile? {
        cachedPodcastProfile(forShowId: showId)
    }

    /// Test-only view of the cache's KEY SET, and it exists because the
    /// accessor above cannot answer the question.
    /// `cachedPodcastProfileForTesting(showId: "")` returns `nil` whether the
    /// empty id was never stored OR the reader simply refuses to look it up —
    /// so a rail written on the reader alone passes even with
    /// ``cachePodcastProfile(_:)``'s guard deleted, which is a test that would
    /// hold if the thing it names never happened. This is the only thing that
    /// can tell the two apart.
    func cachedProfileShowIdsForTesting() -> Set<String> {
        Set(podcastProfilesByShowId.keys)
    }
    #endif

    /// Episode duration for position-based scoring.
    private var episodeDuration: Double = 0

    /// playhead-hygc.1.8 (R7): per-asset in-flight tracker for
    /// `runHotPathResult`. Enforces the no-concurrent-runs-per-asset
    /// invariant that `correctionReplayCandidates` documents (UUID
    /// allocation across actor `await`s could otherwise race and
    /// double-insert replay rows). On entry the assetId is inserted; on
    /// exit (`defer`) it is removed. A second in-flight call for the same
    /// asset hits an `assertionFailure` (DEBUG: trap; RELEASE: no-op) and
    /// also emits a `logger.warning` so RELEASE has at least an
    /// observable footprint. R8: choosing `assertionFailure` over
    /// `preconditionFailure` is deliberate — a single concurrent invocation
    /// in production should NOT crash the player; the legacy
    /// belt-and-suspenders dedupe in `correctionReplayCandidates`
    /// (overlap check against the in-flight `emitted` set + the persisted
    /// `existing` set) keeps the same-call duplicate-row hazard tolerable
    /// even when the assertion is compiled out. Different assets remain
    /// independent. The actor's serialized re-entrance via `await` is the
    /// only realistic source of contention given the production caller
    /// chain (`AnalysisCoordinator.handlePersistedTranscriptChunks` and
    /// `AnalysisJobRunner.run` are both single-shot per asset).
    private var hotPathRunInFlightAssetIds: Set<String> = []

    // playhead-gtt9.16: Last snapshot of the `AcousticFeaturePipeline` funnel.
    // Captured at the end of each `runBackfill` invocation so that tests (and
    // future telemetry surfaces) can inspect which features were computed /
    // produced signal / passed gate / were included in fusion. Initialized to
    // an empty funnel so a service with no backfill runs yet reports zeros
    // rather than surfacing stale data.
    private var lastAcousticFunnel = AcousticFeatureFunnel()
    /// playhead-gtt9.16: Per-window fusion output from the most recent
    /// `AcousticFeaturePipeline.run`. Test-observable so the back-compat
    /// contract (zero signal → zero combined mass) can be asserted without
    /// round-tripping through the full decision pipeline.
    private var lastAcousticPipelineFusion: [AcousticFeatureFusion.WindowFusion] = []

    /// playhead-gtt9.17: Top `CatalogMatch.similarity` observed across all
    /// decoded spans in the most recent `runBackfill` invocation. Reset to
    /// zero at the start of every backfill so stale values from a prior
    /// episode cannot leak into a fresh one. Zero means "either the catalog
    /// was nil/empty, or nothing matched above the default similarity
    /// floor". Test-observable via `lastCatalogMatchSimilarityForTesting`.
    private var lastCatalogMatchSimilarity: Float = 0

    /// playhead-arf8: per-show music-bracket trust store. Lazily-built
    /// actor that wraps the same `AnalysisStore` used by the rest of the
    /// service. `nil` until first lookup so tests / runs that never cross
    /// the bracket-refinement gate don't pay the actor-init cost.
    /// Outcome recording is intentionally absent in this bead — trust
    /// stays at the prior `Beta(5,5)` default, which keeps every show
    /// above the configured floor (0.40) until later work introduces
    /// hit/miss signals.
    private var bracketTrustStore: MusicBracketTrustStore?

    /// playhead-arf8: counters for the most recent `runBackfill` showing
    /// how each decoded span flowed through the bracket-refinement gate.
    /// Test-observable so the activation contract can be asserted at
    /// integration level. Reset at the start of every backfill run.
    private var lastBracketRefinementCounts = BracketRefinementCounts()

    /// playhead-p56a: per-spanId constraint trace from the most recent
    /// `SpanFinalizer` invocation. Keys are `DecodedSpan.id` for spans the
    /// finalizer kept; values are the ordered list of
    /// `FinalizerConstraint.rawValue` strings that fired on each. Empty when
    /// `config.spanFinalizerEnabled == false` (no allocation, no trace —
    /// preserves the byte-identical OFF contract). Reset at the start of
    /// every backfill run so successive runs reflect only the most recent.
    /// Test-observable via `spanFinalizerConstraintsBySpanIdForTesting()`.
    private var lastSpanFinalizerConstraintsBySpanId: [String: [String]] = [:]

    /// playhead-p56a: per-AdWindow constraint trace from the most recent
    /// `SpanFinalizer` invocation. Keys are `AdWindow.id` (the UUID stamped
    /// at `buildFusionAdWindow` time); values are the same constraint trace
    /// the spanId-keyed map carries, pre-resolved per window so the live
    /// pipeline-dump test can correlate directly without a span/window
    /// lookup. Empty when `config.spanFinalizerEnabled == false`. Reset at
    /// the start of every backfill run. Test-observable via
    /// `spanFinalizerConstraintsByWindowIdForTesting()`.
    private var lastSpanFinalizerConstraintsByWindowId: [String: [String]] = [:]

    /// playhead-l2f.6: per-spanId stinger refinement trace from the most
    /// recent `runBackfill`. Keys are `DecodedSpan.id` for spans the
    /// refiner was consulted on (the show had a bank entry); values carry
    /// the snap/grid/revert outcome. Empty when
    /// `config.stingerRefinementEnabled == false` (the OFF path never
    /// resolves a bank entry, never allocates a trace — preserves the
    /// byte-identical OFF contract). Reset at the start of every backfill
    /// run. Test-observable via `stingerRefinementTraceBySpanIdForTesting()`.
    private var lastStingerRefinementTraceBySpanId: [String: StingerRefinementTrace] = [:]

    /// playhead-l2f.6: per-AdWindow stinger refinement trace, pre-resolved
    /// per window in the emission loop so the live pipeline-dump path can
    /// correlate directly (same convention as
    /// `lastSpanFinalizerConstraintsByWindowId`). Empty when
    /// `config.stingerRefinementEnabled == false`. Reset at the start of
    /// every backfill run. Test-observable via
    /// `stingerRefinementTraceByWindowIdForTesting()`.
    private var lastStingerRefinementTraceByWindowId: [String: StingerRefinementTrace] = [:]

    /// playhead-xsdz.37: per-spanId lexical refinement trace from the most
    /// recent `runBackfill`. Keys are `DecodedSpan.id` for spans the lexical
    /// refiner was consulted on (flag ON + the show had a bank entry —
    /// including spans where nothing snapped, so the no-snap outcome is
    /// observable). Empty when `config.lexicalAnchorRefinementEnabled == false`.
    /// Reset at the start of every backfill run. Test-observable via
    /// `lexicalRefinementTraceBySpanIdForTesting()`.
    private var lastLexicalRefinementTraceBySpanId: [String: LexicalRefinementTrace] = [:]

    /// playhead-xsdz.37: per-AdWindow lexical refinement trace, pre-resolved
    /// per window in the emission loop (same convention as the stinger and
    /// finalizer trace maps). Empty when
    /// `config.lexicalAnchorRefinementEnabled == false`. Reset at the start of
    /// every backfill run. Test-observable via
    /// `lexicalRefinementTraceByWindowIdForTesting()`.
    private var lastLexicalRefinementTraceByWindowId: [String: LexicalRefinementTrace] = [:]

    /// playhead-xsdz.36.1.1 (observability-only): per-AdWindow EVIDENCE
    /// PROVENANCE captured in the emission loop so the pipeline-dump treatment
    /// lane can settle whether an over-widened eligible window has exactly ONE
    /// non-FM corroborating evidence kind (→ the xsdz.59 quorum fix applies) or
    /// ≥2 distinct kinds (→ a width problem). Keys are `AdWindow.id`; each value
    /// carries the source span's `anchorProvenance` KINDS plus the DISTINCT
    /// scoring-ledger source kinds the FM-consensus quorum counts. Populated
    /// UNCONDITIONALLY (the underlying provenance + ledger always exist for
    /// every emitted window, computing the labels is cheap, and NO production
    /// decision path ever reads this map — it is pure observability, unlike the
    /// flag-gated trace maps above). Reset at the start of every backfill run.
    /// Test-observable via `evidenceProvenanceByWindowIdForTesting()`.
    private var lastEvidenceProvenanceByWindowId: [String: EvidenceProvenanceForTesting] = [:]

    /// playhead-xsdz.37: lazily-loaded bundled `LexicalAnchorBank`, cached with
    /// the same `.notLoaded`/`.loaded`/`.failed` fail-once contract as the
    /// stinger bank. Never touched when
    /// `config.lexicalAnchorRefinementEnabled == false`.
    private var lexicalAnchorBankCacheState: LexicalAnchorBankLoadState = .notLoaded

    private enum LexicalAnchorBankLoadState {
        case notLoaded
        case loaded(LexicalAnchorBank)
        case failed
    }

    /// playhead-xsdz.37: test-injectable lexical bank override (wins over the
    /// bundled resource). Production leaves this nil.
    private let lexicalAnchorBankOverride: LexicalAnchorBank?

    /// playhead-fl4j: lazily-loaded bundled `SelfPromoBank`, cached with the
    /// same `.notLoaded`/`.loaded`/`.failed` fail-once contract as the lexical
    /// and stinger banks. Never touched when
    /// `config.selfPromoSuppressionEnabled == false`.
    private var selfPromoBankCacheState: SelfPromoBankLoadState = .notLoaded

    private enum SelfPromoBankLoadState {
        case notLoaded
        case loaded(SelfPromoBank)
        case failed
    }

    /// playhead-fl4j: test-injectable self-promo bank override (wins over the
    /// bundled resource). Production leaves this nil. The resolver still gates
    /// on `config.selfPromoSuppressionEnabled` BEFORE consulting the override,
    /// so an injected bank stays unreachable on the flag-OFF path — the flag
    /// gate alone keeps the suppressor inert (the fl4j byte-identity contract).
    private let selfPromoBankOverride: SelfPromoBank?

    /// playhead-l2f.6: lazily-loaded bundled `StingerBank`. `.notLoaded`
    /// until the first flag-ON backfill on a show; a load failure is
    /// remembered (`.failed`) so a malformed bundle logs loudly ONCE and
    /// then degrades to "no bank" instead of re-reading the resource every
    /// run. Never touched when `config.stingerRefinementEnabled == false`.
    private var stingerBankCacheState: StingerBankLoadState = .notLoaded

    private enum StingerBankLoadState {
        case notLoaded
        case loaded(StingerBank)
        case failed
    }

    /// playhead-l2f.6: test-injectable bank override. When non-nil it wins
    /// over the bundled resource, letting wire-in tests plant deterministic
    /// templates without touching the app bundle. Production leaves this
    /// nil (bundle load on first flag-ON use).
    private let stingerBankOverride: StingerBank?

    /// playhead-l2f.6: ranged PCM source for the stinger search envelopes.
    /// Production default reads the persisted 16 kHz analysis-shard cache
    /// (`AnalysisShardPCMReader` — the app's existing decoded-audio path, no
    /// second decode); tests inject synthetic PCM. Only invoked when the
    /// flag is ON and the show has a bank entry.
    private let stingerPCMProvider: StingerPCMProvider

    /// playhead-43ed: optional repeated-ad cache. When non-nil and enabled,
    /// `classifyCandidates` derives a 64-bit perceptual fingerprint from each
    /// candidate's feature windows and looks it up against entries stored for
    /// the current podcast. Hits are recurrence telemetry only; source-episode
    /// absolute bounds/confidence never replace the current classifier.
    /// `nil` preserves pre-43ed behaviour exactly. Production wires a
    /// real `RepeatedAdCacheService`; tests inject deterministic seams.
    private let repeatedAdCache: RepeatedAdCacheService?

    /// playhead-au2v.1.13: optional factory that constructs a
    /// `ChapterGenerationPhase` per-backfill. When `nil`, the chapter-
    /// signal phase is never invoked from `runBackfill` regardless of
    /// `config.chapterSignalMode` — the wire-in is dormant. Production
    /// wiring (a later runtime bead) installs a factory that captures
    /// the live admission policy, boundary detector, labeler, plan
    /// cache, and event sinks. Tests inject deterministic factories
    /// closing over canned mocks.
    ///
    /// The factory shape is `() -> ChapterGenerationPhase` (no
    /// per-backfill arguments) because every dependency the phase
    /// requires is either episode-stable (admission policy, labeler,
    /// cache) or supplied at `phase.run(...)` call time (`mode`,
    /// `episodeId`, `installID`). A factory rather than a single shared
    /// instance is used so the production wiring can rebuild the phase
    /// with a fresh transcript-hash provider per call (the snapshot vs
    /// recheck contract requires the provider to read whatever the
    /// transcript pipeline most recently published).
    private let chapterGenerationPhaseFactory: (@Sendable () -> ChapterGenerationPhase)?

    /// playhead-au2v.1.13: optional cache used for the cache-hit
    /// short-circuit in `runBackfill`. When non-nil and a valid plan is
    /// found for the current content hash, the chapter-generation phase
    /// is skipped entirely (zero FM cost). When `nil`, every run that
    /// would otherwise hit the phase invokes it unconditionally — the
    /// phase still owns its own internal cache write on success, so no
    /// data is lost; the only effect of a missing short-circuit cache is
    /// re-running the phase even if a fresh plan is already on disk.
    /// Tests typically inject the same cache they pass into the
    /// factory's `ChapterGenerationPhase` so the read here and the
    /// phase's write target the same store.
    private let chapterPlanCache: ChapterPlanCache?

    /// playhead-au2v.1.13: producer of the install identifier the
    /// chapter-phase diagnostics need to emit privacy-locked event
    /// payloads. Not stored in `init` parameters list as a keyword arg
    /// because the production runtime needs a stable per-install UUID
    /// (matches the wider diagnostics privacy contract — see
    /// `EpisodeIdHasher`); tests can pass `{ UUID() }` for ergonomic
    /// fixtures. Defaults to a fresh UUID per call so an unwired test
    /// does not crash, but a production wiring bug that omits this
    /// closure surfaces as a per-run UUID churn rather than a stable
    /// hash — one of the integration tests asserts the wiring uses the
    /// injected closure verbatim.
    private let chapterPhaseInstallIDProvider: @Sendable () -> UUID

    /// Cycle 1 H2: rollout gate that intersects `config.fmBackfillMode` with
    /// per-cohort approvals before any FM execution or fusion-time consumption.
    /// `nil` = legacy behavior (the requested mode is used verbatim). When
    /// non-nil, `effectiveFMBackfillMode` consults the registry against the
    /// stored `runtimeCohort` captured at init.
    ///
    /// Value semantics are load-bearing: the struct is captured by value at
    /// init time, so subsequent mutations to a registry held elsewhere do NOT
    /// propagate into this service. If a future refactor wraps the registry
    /// in a class/actor for shared mutation, every service holding a copy
    /// will diverge — and the cohort-rollout contract becomes untestable.
    private let approvedCohortRegistry: ApprovedCohortRegistry?
    /// Cycle 3 H3: the `ScanCohort` captured ONCE at init via the supplied
    /// `scanCohortProvider`. Production uses `ScanCohort.production()` which
    /// reads `Locale.current.identifier` and other process-mutable values —
    /// re-reading on every `effectiveFMBackfillMode` access (the cycle-2
    /// design) was vulnerable to region/calendar locale flips between the
    /// bootstrap-time approval at PlayheadRuntime init and a later runtime
    /// query. iOS does NOT relaunch on region-only changes, so the registry
    /// key would silently miss its approval. Capturing once locks the cohort
    /// to the same value the bootstrap registry was keyed against, and as a
    /// side benefit eliminates the per-access JSON encode in
    /// `CohortKey.canonicalIdentity`.
    private let runtimeCohort: ScanCohort

    // MARK: - Init

    init(
        store: AnalysisStore,
        classifier: ClassifierService = CoreMLSequenceClassifier(),
        metadataExtractor: MetadataExtractor,
        config: AdDetectionConfig = .default,
        podcastProfile: PodcastProfile? = nil,
        backfillJobRunnerFactory: (@Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner)? = nil,
        canUseFoundationModelsProvider: @escaping @Sendable () async -> Bool = { true },
        shadowSkipMarker: @escaping @Sendable (_ sessionId: String, _ podcastId: String) async -> Void = { _, _ in },
        regionShadowObserver: RegionShadowObserver? = nil,
        fmRegionRecoveryDispatcher: FMRegionRecoveryDispatcher? = nil,
        phase5ProjectorObserver: Phase5ProjectorObserver? = nil,
        fragilityDiagnosticObserver: FragilityDiagnosticObserver? = nil,
        brandAppearanceChannelTapObserver: BrandAppearanceChannelTapObserver? = nil,
        temporalRegularizationObserver: TemporalRegularizationObserver? = nil,
        spliceSlotShadowObserver: SpliceSlotShadowObserver? = nil,
        rediffBSideProvider: RediffBSideProvider? = nil,
        rediffSlotShadowObserver: SpliceSlotShadowObserver? = nil,
        negativeBankSuppressionObserver: NegativeBankSuppressionObserver? = nil,
        perShowThresholdOffsetObserver: PerShowThresholdOffsetObserver? = nil,
        skipOrchestrator: SkipOrchestrator? = nil,
        adCatalogStore: AdCatalogStore? = nil,
        negativeFingerprintBank: NegativeFingerprintBank? = nil,
        adCopyFingerprintStore: AdCopyFingerprintStore? = nil,
        crossShowSyndicationStore: CrossShowSyndicationStore? = nil,
        perShowThresholdControllerStore: PerShowThresholdControllerStore? = nil,
        episodeMetadataProvider: EpisodeMetadataProvider = NullEpisodeMetadataProvider(),
        decisionLogger: DecisionLoggerProtocol? = nil,
        classifierCalibrationProfile: ClassifierCalibrationProfile = .production,
        repeatedAdCache: RepeatedAdCacheService? = nil,
        stingerBank: StingerBank? = nil,
        stingerPCMProvider: StingerPCMProvider? = nil,
        lexicalAnchorBank: LexicalAnchorBank? = nil,
        selfPromoBank: SelfPromoBank? = nil,
        chapterGenerationPhaseFactory: (@Sendable () -> ChapterGenerationPhase)? = nil,
        chapterPlanCache: ChapterPlanCache? = nil,
        chapterPhaseInstallIDProvider: @escaping @Sendable () -> UUID = { UUID() },
        approvedCohortRegistry: ApprovedCohortRegistry? = nil,
        scanCohortProvider: @escaping @Sendable () -> ScanCohort = { ScanCohort.production() }
    ) {
        self.store = store
        self.classifier = classifier
        self.metadataExtractor = metadataExtractor
        self.config = config
        self.scanner = LexicalScanner(podcastProfile: podcastProfile)
        self.showPriors = ShowPriors.from(profile: podcastProfile)
        // playhead-2kxd: seed the per-show cache under the profile's OWN
        // podcastId. Production never passes this argument (see
        // `PlayheadRuntime`), so in the shipping app the map starts empty and
        // the only writer is `updatePriors` — which is precisely why a single
        // slot answered every episode with the last show that finished one.
        self.podcastProfilesByShowId = Self.seededProfileMap(podcastProfile)
        self.backfillJobRunnerFactory = backfillJobRunnerFactory
        self.canUseFoundationModelsProvider = canUseFoundationModelsProvider
        self.shadowSkipMarker = shadowSkipMarker
        self.regionShadowObserver = regionShadowObserver
        self.fmRegionRecoveryDispatcher = fmRegionRecoveryDispatcher
        self.phase5ProjectorObserver = phase5ProjectorObserver
        self.fragilityDiagnosticObserver = fragilityDiagnosticObserver
        self.brandAppearanceChannelTapObserver = brandAppearanceChannelTapObserver
        self.temporalRegularizationObserver = temporalRegularizationObserver
        self.spliceSlotShadowObserver = spliceSlotShadowObserver
        self.rediffBSideProvider = rediffBSideProvider
        self.rediffSlotShadowObserver = rediffSlotShadowObserver
        self.negativeBankSuppressionObserver = negativeBankSuppressionObserver
        self.perShowThresholdOffsetObserver = perShowThresholdOffsetObserver
        self.skipOrchestrator = skipOrchestrator
        self.adCatalogStore = adCatalogStore
        self.negativeFingerprintBank = negativeFingerprintBank
        self.adCopyFingerprintStore = adCopyFingerprintStore
        self.crossShowSyndicationStore = crossShowSyndicationStore
        self.perShowThresholdControllerStore = perShowThresholdControllerStore
        self.episodeMetadataProvider = episodeMetadataProvider
        // playhead-8em9 (narL): allow the logger to be installed at init
        // time so there is no race with the first backfill. PlayheadRuntime
        // passes a real DecisionLogger under DEBUG; production and tests
        // that don't care about logging leave this nil, keeping the
        // NoOpDecisionLogger default already on `decisionLogger`.
        if let decisionLogger {
            self.decisionLogger = decisionLogger
        }
        self.classifierCalibrationProfile = classifierCalibrationProfile
        self.repeatedAdCache = repeatedAdCache
        self.stingerBankOverride = stingerBank
        // playhead-l2f.6: default to the production shard-cache reader.
        // The provider is only ever invoked when
        // `config.stingerRefinementEnabled == true` AND the show has a bank
        // entry, so wiring it unconditionally costs nothing on the OFF path.
        self.stingerPCMProvider = stingerPCMProvider ?? { episodeID, start, end in
            AnalysisShardPCMReader.loadSamples(
                episodeID: episodeID,
                from: start,
                to: end
            )
        }
        // playhead-xsdz.37: lexical refinement reads the transcript word
        // stream the pipeline already carries (no PCM), so it needs only the
        // optional bank override — nil ⇒ bundle load on first flag-ON use.
        self.lexicalAnchorBankOverride = lexicalAnchorBank
        // playhead-fl4j: self-promo suppression reads the transcript word stream
        // the pipeline already carries (no PCM), so it needs only the optional
        // bank override — nil ⇒ bundle load on first flag-ON use.
        self.selfPromoBankOverride = selfPromoBank
        self.chapterGenerationPhaseFactory = chapterGenerationPhaseFactory
        self.chapterPlanCache = chapterPlanCache
        self.chapterPhaseInstallIDProvider = chapterPhaseInstallIDProvider
        self.approvedCohortRegistry = approvedCohortRegistry
        // Cycle 3 H3: capture the cohort exactly once at init. See
        // `runtimeCohort`'s docstring for the rationale (region-only locale
        // flips during process lifetime previously caused silent FM demotion).
        self.runtimeCohort = scanCohortProvider()
    }

    /// Cycle 1 H2 / Cycle 3 H3: effective FM mode after intersecting
    /// `config.fmBackfillMode` with the approved-cohort registry decision
    /// for the cohort captured at init. When no registry is wired, returns
    /// `config.fmBackfillMode` verbatim — preserving legacy behavior for
    /// tests and any caller that hasn't opted into cohort gating.
    private var effectiveFMBackfillMode: FMBackfillMode {
        guard let registry = approvedCohortRegistry else {
            return config.fmBackfillMode
        }
        return registry.effectiveMode(
            osBuild: runtimeCohort.osBuild,
            scanCohort: runtimeCohort,
            requestedMode: config.fmBackfillMode
        )
    }

    #if DEBUG
    /// Cycle 8 M-5 call-site rail: DEBUG accessor that returns the factory
    /// closure the service was constructed with, so a test can invoke the
    /// very closure defined on `PlayheadRuntime.swift:214` and inspect the
    /// runner it produces. This is the "real call-site rail" the cycle-7
    /// reviewer asked for: a regression that swaps the live redactor for
    /// `.noop` inside the closure body fails the test at the construction
    /// site, not at some parallel factory.
    func backfillJobRunnerFactoryForTesting() -> (@Sendable (AnalysisStore, FMBackfillMode) -> BackfillJobRunner)? {
        backfillJobRunnerFactory
    }

    /// Cycle 2 H1: DEBUG accessor for the cohort-intersected FM mode.
    /// Production callers (four `effectiveFMBackfillMode` reads at the
    /// runBackfill admission gate, the FM ledger-entry builder, the fusion
    /// constructor, and the shadow-phase resolver — the shadow path
    /// captures once into `resolvedMode` and re-uses for the runner factory)
    /// resolve the mode internally; tests use this to assert that an
    /// injected `approvedCohortRegistry` + `scanCohortProvider` combination
    /// resolves to the expected effective mode without standing up the
    /// full backfill pipeline. Without this accessor the cycle-1 H2
    /// wire-up was untested end-to-end (the cycle-2 reviewer's H1 gap).
    func effectiveFMBackfillModeForTesting() -> FMBackfillMode {
        effectiveFMBackfillMode
    }
    #endif

    // MARK: - Phase 7.2: Correction Store Injection

    /// Set the user correction store. Called from PlayheadRuntime after init
    /// (actor property writes must be asynchronous from an init context).
    func setUserCorrectionStore(_ store: any UserCorrectionStore) {
        self.correctionStore = store
    }

    /// playhead-shjn: install the sponsor knowledge store post-init. Mirrors
    /// `setUserCorrectionStore`. `PlayheadRuntime` already constructs one
    /// `SponsorKnowledgeStore` for `LearningArtifactIngestor` (the WRITE
    /// side); this hands the SAME instance to the READ side so a sponsor the
    /// user confirmed on two episodes can propose a region on the next one.
    func setSponsorKnowledgeStore(_ store: SponsorKnowledgeStore?) {
        self.sponsorKnowledgeStore = store
    }

    /// playhead-xsdz.11: install the per-show threshold controller store
    /// post-init. Mirrors `setUserCorrectionStore`. The flag-off path NEVER
    /// reads from this store, so installing it is safe whether or not the
    /// `perShowThresholdControlEnabled` flag is on. Production wires this ONLY
    /// when the flag is on (see `PlayheadRuntime`); tests call it directly.
    func setPerShowThresholdControllerStore(_ store: PerShowThresholdControllerStore?) {
        self.perShowThresholdControllerStore = store
    }

    /// playhead-xsdz.11: resolve the per-show auto-skip threshold OFFSET for a
    /// show. Returns `0` (no adjustment) UNLESS all three conditions hold: the
    /// `perShowThresholdControlEnabled` flag is on, a controller store is wired,
    /// and a non-empty showId is supplied. This is the SINGLE read seam — both
    /// `runBackfill` and the DEBUG test accessor go through it, so a regression
    /// that bypasses the flag gate fails the test at the real call site. With
    /// the feature off this never touches the store (no read), which is the
    /// load-bearing flag-OFF invariant.
    private func resolvePerShowThresholdOffset(showId: String?) async -> Double {
        guard config.perShowThresholdControlEnabled,
              let controllerStore = perShowThresholdControllerStore,
              let showId, !showId.isEmpty else {
            return 0
        }
        return await controllerStore.offset(forShow: showId)
    }

    #if DEBUG
    /// playhead-xsdz.11: DEBUG accessor for the EFFECTIVE auto-skip threshold a
    /// `runBackfill` would use for a given show + promotion track. Resolves the
    /// per-show offset through the exact production read seam
    /// (`resolvePerShowThresholdOffset`) and applies it through the same
    /// `PerShowThresholdController.effectiveThreshold` clamp the gate uses, so
    /// the test asserts the live read path — not a parallel reimplementation.
    /// Flag-off ⇒ returns `config.effectiveAutoSkipThreshold(for:)` unchanged.
    func effectiveAutoSkipThresholdForTesting(
        showId: String?,
        track: PromotionTrack
    ) async -> Double {
        let base = config.effectiveAutoSkipThreshold(for: track)
        let offset = await resolvePerShowThresholdOffset(showId: showId)
        // Mirror the gate's scoping: the offset applies ONLY to `.standard`.
        guard config.perShowThresholdControlEnabled, offset != 0, track == .standard else {
            return base
        }
        return PerShowThresholdController.effectiveThreshold(
            globalThreshold: base,
            offset: offset,
            parameters: config.perShowThresholdControllerParameters
        )
    }
    #endif

    /// playhead-q45f: install the TrustScoringService post-init.
    /// Mirrors `setUserCorrectionStore`. PlayheadRuntime calls this in
    /// a Task after both actors exist; tests exercising the
    /// listen-rewind reroute call it directly on the service they
    /// constructed.
    func setTrustScoringService(_ service: TrustScoringService) {
        self.trustScoringService = service
    }

    #if DEBUG
    func _setPostUserMarkCommitHandlerForTesting(
        _ handler: (@Sendable () async -> Void)?
    ) {
        postUserMarkCommitHandlerForTesting = handler
    }

    func _waitForUserMarkedAdDerivedWorkForTesting() async {
        guard userMarkedAdDerivedWorkCount > 0 else { return }
        await withCheckedContinuation { continuation in
            userMarkedAdDerivedWorkWaiters.append(continuation)
        }
    }

    func _setListenRewindTrustHandlerForTesting(
        _ handler: (@Sendable (String) async -> Void)?
    ) {
        listenRewindTrustHandlerForTesting = handler
    }

    /// playhead-8em9 (narL): Test seam for installing a decision logger
    /// post-init. Production wires the logger via `init(decisionLogger:)`
    /// to avoid a Task-install race; this setter exists only for tests that
    /// swap the logger mid-life. DEBUG-only to prevent a future regression
    /// from re-introducing the race in release builds.
    func setDecisionLogger(_ logger: DecisionLoggerProtocol) {
        self.decisionLogger = logger
    }
    #endif

    /// playhead-z3ch: Set the EpisodeMetadataProvider. Called from
    /// `PlayheadApp` (after the SwiftData ModelContainer is available) so
    /// `runBackfill` can pre-seed metadata-derived ledger entries. Mirrors
    /// the `setUserCorrectionStore` pattern.
    func setEpisodeMetadataProvider(_ provider: EpisodeMetadataProvider) {
        self.episodeMetadataProvider = provider
    }

    /// playhead-2hpn: Install the per-show music-bed profile store
    /// post-init. The flag-off path never reads from this store, so
    /// installing it is safe whether or not the feature flag is on.
    /// Mirrors `setEpisodeMetadataProvider`.
    func setShowMusicBedProfileStore(_ store: any ShowMusicBedProfileResolving) {
        self.showMusicBedProfileStore = store
    }

    /// playhead-h6a6: Install the per-show capability-profile store
    /// post-init. Mirrors `setShowMusicBedProfileStore`. The
    /// flag-off path NEVER reads from or writes to this store, so
    /// installing it is safe whether or not the
    /// `showCapabilityProfilesEnabled` flag is on. The runtime calls
    /// this once on cold-start after the `ModelContainer` is alive.
    func setShowCapabilityProfileStore(_ store: any ShowCapabilityProfileResolving) {
        self.showCapabilityProfileStore = store
    }

    /// playhead-h6a6: Install the Phase-2 SLI gate predicate
    /// post-init. The predicate returns `true` iff the show's cohort
    /// has Phase-2 SLIs within defended bounds per playhead-d99.
    /// Without an installed gate the service treats every show as
    /// "SLIs not in bounds" — the conservative default that keeps
    /// the profile pinned at `.unknown` until an explicit gate is
    /// installed by the runtime.
    func setCapabilityProfileSLIGate(_ gate: @escaping ShowCapabilitySLIGate) {
        self.capabilityProfileSLIGate = gate
    }

    /// playhead-h6a6: Test seam returning the most recent
    /// capability-budget adjustment resolved in `runBackfill`. Flag-
    /// OFF runs leave this at the `.unknown`-yielded baseline
    /// (multiplier 1.0, empty bias map) — the no-modulation no-op
    /// contract. Pre-backfill (before the first `runBackfill` ever
    /// runs) the value is also the `.unknown` baseline, pinned by
    /// the init-seed on `lastCapabilityBudgetAdjustment` (line 541).
    ///
    /// h6a6 R4: the init-seed half of the contract is asserted by
    /// `ShowCapabilityBudgetModulatorTests
    /// .adDetectionServiceInitSeedsUnknownBaseline`. The full
    /// flag-on / runBackfill-driven behavioral contract ("modulator
    /// applied when flag on + profile observed; pass-through when
    /// flag off") will be asserted via this seam by the follow-on
    /// consumer bead that wires the multiplier into the per-episode
    /// budget — at which point a runBackfill-driven test on a
    /// synthetic `ShowCapabilityProfileResolving` can assert both
    /// the stamp and its consumption in one fixture.
    func lastCapabilityBudgetAdjustmentForTesting() -> ShowCapabilityBudgetAdjustment {
        lastCapabilityBudgetAdjustment
    }

    // MARK: - playhead-gtt9.16: AcousticFeaturePipeline accessors

    /// Test seam: return the funnel snapshot captured during the most recent
    /// `runBackfill` invocation. Production callers read the same state via
    /// log lines emitted at the end of backfill (`logger.info("Backfill
    /// acoustic-pipeline funnel: ...")`), but tests need direct access to
    /// the structured counters.
    func acousticFunnelForTesting() -> AcousticFeatureFunnel {
        lastAcousticFunnel
    }

    /// Test seam: return the per-window fusion output from the most recent
    /// `AcousticFeaturePipeline.run`. Empty until the first backfill
    /// completes.
    func lastAcousticPipelineFusionForTesting() -> [AcousticFeatureFusion.WindowFusion] {
        lastAcousticPipelineFusion
    }

    /// playhead-gtt9.17: Return the top `CatalogMatch.similarity` from the
    /// most recent `runBackfill`. Zero if no catalog was wired, the catalog
    /// was empty, or nothing scored above `AdCatalogStore.defaultSimilarityFloor`.
    /// Used by `AdCatalogWiringTests` to verify that prior entries do lift
    /// similarity and empty/absent catalogs do not.
    func lastCatalogMatchSimilarityForTesting() -> Float {
        lastCatalogMatchSimilarity
    }

    // MARK: - playhead-arf8: Bracket Refinement Telemetry / Trust Lookup

    /// Test seam: returns the per-`runBackfill` aggregate counts emitted by
    /// the bracket-aware refiner gate. Resets to all-zero at the start of
    /// every backfill run so successive calls reflect only the most recent
    /// run. Used by `BracketActivationTests` to assert that the master
    /// flag, trust gate, and confidence gates route spans to the expected
    /// path without log scraping.
    func bracketRefinementCountsForTesting() -> BracketRefinementCounts {
        lastBracketRefinementCounts
    }

    // MARK: - playhead-p56a: SpanFinalizer constraint-trace telemetry

    /// Test seam: returns the per-spanId list of finalizer constraints that
    /// fired during the most recent `runBackfill`. Keys are `DecodedSpan.id`;
    /// values are the ordered `FinalizerConstraint.rawValue` strings from each
    /// span's `FinalizedSpan.constraintTrace`. Empty when
    /// `config.spanFinalizerEnabled == false` (the OFF path never invokes the
    /// finalizer, never allocates a trace) and also empty when the finalizer
    /// returned no spans with any constraint fired. Pairs with
    /// `spanFinalizerConstraintsByWindowIdForTesting()`, which carries the
    /// same data pre-resolved by `AdWindow.id` for the live pipeline-dump path.
    func spanFinalizerConstraintsBySpanIdForTesting() -> [String: [String]] {
        lastSpanFinalizerConstraintsBySpanId
    }

    /// Test seam: returns the per-AdWindow constraint trace from the most
    /// recent `runBackfill`. Keys are `AdWindow.id` (the UUID stamped by
    /// `buildFusionAdWindow`); values mirror what
    /// `spanFinalizerConstraintsBySpanIdForTesting()` carries for the
    /// underlying spanId. Empty when `config.spanFinalizerEnabled == false`.
    /// The live pipeline-dump path uses this for direct per-window
    /// correlation — no span/window lookup, no time-range matching.
    func spanFinalizerConstraintsByWindowIdForTesting() -> [String: [String]] {
        lastSpanFinalizerConstraintsByWindowId
    }

    // MARK: - playhead-l2f.6: Stinger refinement trace telemetry

    /// Test seam: returns the per-spanId stinger refinement trace from the
    /// most recent `runBackfill`. Keys are `DecodedSpan.id` for every span
    /// the refiner was consulted on (flag ON + show has a bank entry —
    /// including spans where nothing snapped, so the no-snap outcome is
    /// observable). Empty when `config.stingerRefinementEnabled == false`
    /// (the OFF path never consults the refiner, never allocates a trace).
    /// Pairs with `stingerRefinementTraceByWindowIdForTesting()`.
    func stingerRefinementTraceBySpanIdForTesting() -> [String: StingerRefinementTrace] {
        lastStingerRefinementTraceBySpanId
    }

    /// Test seam: returns the per-AdWindow stinger refinement trace from
    /// the most recent `runBackfill`. Keys are `AdWindow.id` (the UUID
    /// stamped by `buildFusionAdWindow`); values mirror what
    /// `stingerRefinementTraceBySpanIdForTesting()` carries for the
    /// underlying spanId. Empty when
    /// `config.stingerRefinementEnabled == false`. The live pipeline-dump
    /// path uses this for direct per-window correlation, matching the
    /// `spanFinalizerConstraintsByWindowIdForTesting()` convention.
    func stingerRefinementTraceByWindowIdForTesting() -> [String: StingerRefinementTrace] {
        lastStingerRefinementTraceByWindowId
    }

    // MARK: - playhead-xsdz.36.1.1: Evidence-provenance telemetry (observability-only)

    /// Per-AdWindow evidence provenance surfaced by
    /// `evidenceProvenanceByWindowIdForTesting()`. A behaviour-neutral
    /// observability payload — never read by any production decision path.
    struct EvidenceProvenanceForTesting: Sendable, Equatable {
        /// The source span's `anchorProvenance` KINDS
        /// (`AnchorRef.provenanceKind`), in span order.
        let anchorProvenanceKinds: [String]
        /// The DISTINCT scoring-ledger source kinds the FM-consensus quorum
        /// counts (`EvidenceSourceType.rawValue`, sorted).
        let evidenceSourceKinds: [String]
    }

    /// The DISTINCT corroborating evidence source kinds a run's
    /// `effectiveLedger` carries, computed with the EXACT formula
    /// `DecisionMapper.quorumGateForFMConsensus` uses for its
    /// `corroboratingSources` set: filter to automatic-decision evidence,
    /// then drop the always-present zero-weight `.classifier` entry, and take
    /// the distinct `EvidenceSourceType.rawValue`s (sorted for a stable dump).
    ///
    /// PURE and behaviour-neutral — a re-derivation for the pipeline-dump test
    /// seam only. It is NOT called from the gate and changes no decision, but
    /// it MUST stay in sync with `quorumGateForFMConsensus`; the
    /// `corroboratingEvidenceSourceKindsMatchesQuorumFormula` test pins the
    /// contract against a synthetic ledger.
    static func corroboratingEvidenceSourceKinds(
        _ effectiveLedger: [EvidenceLedgerEntry]
    ) -> [String] {
        let scoringLedger = effectiveLedger.filter(
            \.contributesToAutomaticDecision
        )
        let kinds = Set(scoringLedger.compactMap { entry -> EvidenceSourceType? in
            if entry.source == .classifier, entry.weight == 0 { return nil }
            return entry.source
        })
        return kinds.map(\.rawValue).sorted()
    }

    /// Test seam: per-AdWindow evidence provenance from the most recent
    /// `runBackfill`. Keys are `AdWindow.id` (the UUID stamped by
    /// `buildFusionAdWindow`); each value carries the source span's
    /// anchor-provenance KINDS and the DISTINCT scoring-ledger source kinds the
    /// FM-consensus quorum counts. The live pipeline-dump treatment lane reads
    /// this to record per-window provenance for the xsdz.59 quorum-vs-width
    /// diagnosis. Behaviour-neutral: no production path reads it.
    func evidenceProvenanceByWindowIdForTesting() -> [String: EvidenceProvenanceForTesting] {
        lastEvidenceProvenanceByWindowId
    }

    // MARK: - playhead-xsdz.37: Lexical refinement trace telemetry

    /// Test seam: per-spanId lexical refinement trace from the most recent
    /// `runBackfill`. Keys are `DecodedSpan.id` for every span the refiner was
    /// consulted on (flag ON + show has a bank entry — including no-snap
    /// consults). Empty when `config.lexicalAnchorRefinementEnabled == false`.
    func lexicalRefinementTraceBySpanIdForTesting() -> [String: LexicalRefinementTrace] {
        lastLexicalRefinementTraceBySpanId
    }

    /// Test seam: per-AdWindow lexical refinement trace from the most recent
    /// `runBackfill`. Keys are `AdWindow.id`; values mirror what the spanId map
    /// carries for the underlying span. Empty when the flag is OFF.
    func lexicalRefinementTraceByWindowIdForTesting() -> [String: LexicalRefinementTrace] {
        lastLexicalRefinementTraceByWindowId
    }

    // MARK: - playhead-xsdz.37: Lexical refinement helpers

    /// Resolve the lexical bank: the test override wins; otherwise lazily load
    /// the bundled resource once, remembering failure so a malformed asset logs
    /// loudly exactly once and then degrades to "no bank" (refinement silently
    /// disabled — never a crash). Only reached when
    /// `config.lexicalAnchorRefinementEnabled == true`.
    private func lexicalAnchorBankIfEnabled() -> LexicalAnchorBank? {
        if let lexicalAnchorBankOverride { return lexicalAnchorBankOverride }
        switch lexicalAnchorBankCacheState {
        case .loaded(let bank):
            return bank
        case .failed:
            return nil
        case .notLoaded:
            do {
                let bank = try LexicalAnchorBank.load()
                lexicalAnchorBankCacheState = .loaded(bank)
                logger.info("[xsdz37] LexicalAnchorBank loaded: \(bank.shows.count) shows, \(bank.genericAnchors.count) generic anchors")
                return bank
            } catch {
                logger.error("[xsdz37] LexicalAnchorBank load failed — lexical refinement disabled: \(String(describing: error), privacy: .public)")
                lexicalAnchorBankCacheState = .failed
                return nil
            }
        }
    }

    /// Resolve the per-run lexical context: flag ON + known show + bank entry.
    /// Returns nil on the OFF path without touching the bank (byte-identical to
    /// pre-xsdz.37). The effective anchor set is the show's family-(a)
    /// templates plus the generic family-(b) framing phrases.
    private func resolveLexicalRefinementContext(
        showKey: String?
    ) -> LexicalRefinementRunContext? {
        guard config.lexicalAnchorRefinementEnabled,
              let bank = lexicalAnchorBankIfEnabled(),
              let anchors = bank.effectiveAnchors(forShowKey: showKey),
              !anchors.isEmpty else {
            return nil
        }
        logger.info("[xsdz37] lexical context resolved: anchors=\(anchors.count)")
        return LexicalRefinementRunContext(anchors: anchors)
    }

    /// Apply lexical refinement to one decoded span (flag ON, bank entry
    /// resolved). Runs the pure `LexicalAnchorRefiner` against the episode word
    /// stream, records the per-span trace, and returns the span with refined
    /// bounds (identity, ordinals, and provenance preserved — the refiner never
    /// splits or merges).
    private func applyLexicalRefinement(
        span: DecodedSpan,
        context: LexicalRefinementRunContext,
        words: [LexicalWord],
        episodeDuration: Double
    ) -> DecodedSpan {
        let result = LexicalAnchorRefiner.refine(
            proposalStart: span.startTime,
            proposalEnd: span.endTime,
            anchors: context.anchors,
            words: words,
            episodeDuration: episodeDuration
        )
        lastLexicalRefinementTraceBySpanId[span.id] = result.trace
        guard result.startTime != span.startTime || result.endTime != span.endTime else {
            return span
        }
        logger.info("[xsdz37] lexical snap: spanId=\(span.id, privacy: .public) [\(span.startTime, format: .fixed(precision: 2)), \(span.endTime, format: .fixed(precision: 2))] -> [\(result.startTime, format: .fixed(precision: 2)), \(result.endTime, format: .fixed(precision: 2))] startSnapped=\(result.trace.startSnapped) endSnapped=\(result.trace.endSnapped)")
        return DecodedSpan(
            id: span.id,
            assetId: span.assetId,
            firstAtomOrdinal: span.firstAtomOrdinal,
            lastAtomOrdinal: span.lastAtomOrdinal,
            startTime: result.startTime,
            endTime: result.endTime,
            anchorProvenance: span.anchorProvenance
        )
    }

    // MARK: - playhead-fl4j: Self-promo suppression helpers

    /// Resolve the self-promo bank: OFF-path short-circuits WITHOUT touching the
    /// bank or the override (byte-identical to pre-fl4j — the flag gate alone
    /// keeps an injected bank unreachable). On the ON path the test override
    /// wins; otherwise lazily load the bundled resource once, remembering
    /// failure so a malformed asset logs loudly exactly once and then degrades
    /// to "no bank" (suppression silently disabled — never a crash).
    private func selfPromoSuppressionBankIfEnabled() -> SelfPromoBank? {
        guard config.selfPromoSuppressionEnabled else { return nil }
        if let selfPromoBankOverride { return selfPromoBankOverride }
        switch selfPromoBankCacheState {
        case .loaded(let bank):
            return bank
        case .failed:
            return nil
        case .notLoaded:
            do {
                let bank = try SelfPromoBank.load()
                selfPromoBankCacheState = .loaded(bank)
                logger.info("[fl4j] SelfPromoBank loaded: \(bank.phrases.count) phrases")
                return bank
            } catch {
                logger.error("[fl4j] SelfPromoBank load failed — self-promo suppression disabled: \(String(describing: error), privacy: .public)")
                selfPromoBankCacheState = .failed
                return nil
            }
        }
    }

    // MARK: - playhead-l2f.6: Stinger refinement helpers

    /// Resolve the bank: the test override wins; otherwise lazily load the
    /// bundled resource once, remembering failure so a malformed asset logs
    /// loudly exactly once and then degrades to "no bank" (refinement
    /// silently disabled — never a crash). Only reached when
    /// `config.stingerRefinementEnabled == true`.
    private func stingerBankIfEnabled() -> StingerBank? {
        if let stingerBankOverride { return stingerBankOverride }
        switch stingerBankCacheState {
        case .loaded(let bank):
            return bank
        case .failed:
            return nil
        case .notLoaded:
            do {
                let bank = try StingerBank.load()
                stingerBankCacheState = .loaded(bank)
                logger.info("[l2f6] StingerBank loaded: \(bank.shows.count) shows")
                return bank
            } catch {
                logger.error("[l2f6] StingerBank load failed — stinger refinement disabled: \(String(describing: error), privacy: .public)")
                stingerBankCacheState = .failed
                return nil
            }
        }
    }

    /// Resolve the per-run stinger context: flag ON + known show + bank
    /// entry. The episode identity (shard-cache key) comes from the asset
    /// row; a fetch failure degrades to a context without PCM access so
    /// the trace still records the refiner outcome. Returns nil on the OFF
    /// path without touching the bank, the store, or the filesystem.
    private func resolveStingerRefinementContext(
        analysisAssetId: String,
        showKey: String?
    ) async -> StingerRefinementRunContext? {
        guard config.stingerRefinementEnabled,
              let bank = stingerBankIfEnabled(),
              let entry = bank.entry(forShowKey: showKey) else {
            return nil
        }
        var episodeID: String?
        do {
            episodeID = try await store.fetchAsset(id: analysisAssetId)?.episodeId
        } catch {
            logger.warning("[l2f6] asset fetch failed for \(analysisAssetId, privacy: .public) — stinger refinement runs without PCM: \(error.localizedDescription, privacy: .public)")
        }
        let sides = [entry.pre != nil ? "pre" : nil, entry.post != nil ? "post" : nil]
            .compactMap { $0 }
            .joined(separator: "+")
        logger.info("[l2f6] stinger context resolved: show=\(entry.showName, privacy: .public) sides=[\(sides, privacy: .public)] grid=\(entry.podWidthGridSeconds ?? 0)")
        return StingerRefinementRunContext(entry: entry, episodeID: episodeID)
    }

    /// Apply stinger refinement to one decoded span (flag ON, bank entry
    /// resolved). Loads the ±90 s search envelope per templated side from
    /// the shard-cache PCM (nil PCM ⇒ that side cannot snap), runs the pure
    /// `StingerRefiner`, records the per-span trace, and returns the span
    /// with refined bounds (identity, ordinals, and provenance preserved —
    /// the refiner never splits or merges).
    ///
    /// `episodeDuration` is threaded from the CALLER's `runBackfill`
    /// parameter — deliberately NOT the `self.episodeDuration` actor
    /// property, which a concurrently-admitted hot-path run for a
    /// different episode can reassign across this method's suspension
    /// points (actor reentrancy). The local parameter is immutable for the
    /// duration of the backfill.
    private func applyStingerRefinement(
        span: DecodedSpan,
        context: StingerRefinementRunContext,
        episodeDuration: Double
    ) async -> DecodedSpan {
        let entry = context.entry
        var startEnvelope: StingerSearchEnvelope?
        var endEnvelope: StingerSearchEnvelope?
        if let episodeID = context.episodeID {
            if entry.pre != nil {
                startEnvelope = await loadStingerSearchEnvelope(
                    episodeID: episodeID,
                    center: span.startTime,
                    episodeDuration: episodeDuration
                )
            }
            if entry.post != nil {
                endEnvelope = await loadStingerSearchEnvelope(
                    episodeID: episodeID,
                    center: span.endTime,
                    episodeDuration: episodeDuration
                )
            }
        }
        let result = StingerRefiner.refine(
            proposalStart: span.startTime,
            proposalEnd: span.endTime,
            entry: entry,
            startEnvelope: startEnvelope,
            endEnvelope: endEnvelope,
            episodeDuration: episodeDuration
        )
        lastStingerRefinementTraceBySpanId[span.id] = result.trace
        guard result.startTime != span.startTime || result.endTime != span.endTime else {
            return span
        }
        logger.info("[l2f6] stinger snap: spanId=\(span.id, privacy: .public) [\(span.startTime, format: .fixed(precision: 2)), \(span.endTime, format: .fixed(precision: 2))] -> [\(result.startTime, format: .fixed(precision: 2)), \(result.endTime, format: .fixed(precision: 2))] startSnapped=\(result.trace.startSnapped) endSnapped=\(result.trace.endSnapped) gridApplied=\(result.trace.gridApplied)")
        return DecodedSpan(
            id: span.id,
            assetId: span.assetId,
            firstAtomOrdinal: span.firstAtomOrdinal,
            lastAtomOrdinal: span.lastAtomOrdinal,
            startTime: result.startTime,
            endTime: result.endTime,
            anchorProvenance: span.anchorProvenance
        )
    }

    /// Read the decoded PCM for `center ± 90 s` (clamped to the episode)
    /// from the injected provider and reduce it to the 50 Hz log-RMS search
    /// envelope. Nil when the range is empty or no PCM is available (that
    /// side simply cannot snap — never an error). `episodeDuration` is the
    /// caller-threaded runBackfill parameter (see `applyStingerRefinement`).
    private func loadStingerSearchEnvelope(
        episodeID: String,
        center: Double,
        episodeDuration: Double
    ) async -> StingerSearchEnvelope? {
        let start = max(0.0, center - StingerRefiner.searchRadiusSeconds)
        let end = min(episodeDuration, center + StingerRefiner.searchRadiusSeconds)
        guard end > start,
              let slice = await stingerPCMProvider(episodeID, start, end) else {
            return nil
        }
        let values = StingerEnvelope.compute(samples: slice.samples)
        guard !values.isEmpty else { return nil }
        return StingerSearchEnvelope(values: values, startSeconds: slice.startSeconds)
    }

    /// playhead-hygc.1.8 (R11): test seam exposing the per-asset in-flight
    /// tracker so the `defer { hotPathRunInFlightAssetIds.remove(...) }`
    /// post-condition documented at `runHotPathResult`'s entry can be
    /// verified directly. Without this seam the cleanup contract is
    /// observable only by triggering the assertionFailure (which crashes
    /// in DEBUG) — leaving the "the defer ran" half of the concurrency
    /// invariant unpinned. R7 added the assertionFailure; R11 pins the
    /// release-side cleanup so a future refactor that drops the `defer`
    /// (e.g. moving the insert/remove inside a child task that aborts
    /// without unwinding the actor frame) fails this single test.
    func hotPathInFlightAssetIdsForTesting() -> Set<String> {
        hotPathRunInFlightAssetIds
    }

    /// Lazy accessor for `MusicBracketTrustStore`. Constructs the actor on
    /// the first request and caches it for the lifetime of the service.
    /// Both the actor itself and its `AnalysisStore` backing are safe to
    /// share across runs, so reuse is the cheapest correct option.
    private func bracketTrustStoreLazy() -> MusicBracketTrustStore {
        if let existing = bracketTrustStore {
            return existing
        }
        let fresh = MusicBracketTrustStore(store: store)
        bracketTrustStore = fresh
        return fresh
    }

    /// Increment the matching counter on `lastBracketRefinementCounts` so
    /// `bracketRefinementCountsForTesting()` and any future log emission
    /// can attribute decoded spans to gate paths. Pure bookkeeping — does
    /// not feed back into `MusicBracketTrustStore`. Outcome accumulation
    /// is intentionally deferred until offline ground-truth signals exist.
    private func tallyBracketRefinementOutcome(_ path: BracketAwareBoundaryRefiner.Path) {
        switch path {
        case .legacy:
            lastBracketRefinementCounts.legacyBypass += 1
        case .noBracket:
            lastBracketRefinementCounts.noBracket += 1
        case .trustGated:
            lastBracketRefinementCounts.trustGated += 1
        case .coarseGated:
            lastBracketRefinementCounts.coarseGated += 1
        case .fineConfidenceGated:
            lastBracketRefinementCounts.fineConfidenceGated += 1
        case .bracketRefined:
            lastBracketRefinementCounts.bracketRefined += 1
        }
    }

    // MARK: - User Correction Persistence

    /// Absorb a repeat correction into the mark the listener already owns.
    ///
    /// playhead-1mq1.2. Two outcomes, and the difference between them is the
    /// whole point of playhead-q0tj: a repeat tap that adds nothing is
    /// `.alreadyMarked` (durable state is already correct, and re-appending a
    /// correction event would inflate the show's false-negative signal for a
    /// gesture that carried no new information), while a repeat tap that
    /// reaches past the existing edge WIDENS that row and records the event.
    /// The listener who taps again because the ad is still playing is telling
    /// us the mark is too short, and that is exactly what gets stored.
    ///
    /// A failed widen returns `.rejected`, never `.alreadyMarked`. The mark
    /// still exists, but it does not cover what was asked for, and a value that
    /// says "covered" when part of the span is not is the standing defect class
    /// this repo keeps finding.
    private func foldIntoExistingUserMark(
        _ existingMark: AdWindow,
        analysisAssetId: String,
        requestedStart: Double,
        requestedEnd: Double,
        podcastId: String?
    ) async -> UserMarkPersistence {
        let unionStart = min(existingMark.startTime, requestedStart)
        let unionEnd = max(existingMark.endTime, requestedEnd)
        guard unionStart < existingMark.startTime
                || unionEnd > existingMark.endTime else {
            return .alreadyMarked(
                UserMarkIdentity(
                    windowId: existingMark.id,
                    startTime: existingMark.startTime,
                    endTime: existingMark.endTime
                )
            )
        }

        let correction = CorrectionEvent(
            analysisAssetId: analysisAssetId,
            scope: CorrectionScope.exactTimeSpan(
                assetId: analysisAssetId,
                startTime: unionStart,
                endTime: unionEnd
            ).serialized,
            source: .falseNegative,
            podcastId: podcastId,
            correctionType: .falseNegative
        )

        let didWiden: Bool
        do {
            didWiden = try await store.extendUserMarkedAd(
                id: existingMark.id,
                analysisAssetId: analysisAssetId,
                startTime: unionStart,
                endTime: unionEnd,
                correction: correction
            )
        } catch {
            logger.warning(
                "recordUserMarkedAd: failed to widen existing user mark \(existingMark.id, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            return .rejected
        }
        guard didWiden else {
            logger.warning(
                "recordUserMarkedAd: existing user mark \(existingMark.id, privacy: .public) matched no widenable row"
            )
            return .rejected
        }

        // The widened span may now reach feature windows the original mark did
        // not. This work is idempotent by contract, so re-running it over the
        // union is safe and is the only way the catalog learns the new edges.
        scheduleUserMarkedAdDerivedWork(
            analysisAssetId: analysisAssetId,
            startTime: unionStart,
            endTime: unionEnd,
            podcastId: podcastId,
            windowId: existingMark.id
        )
        return .extended(
            UserMarkIdentity(
                windowId: existingMark.id,
                startTime: unionStart,
                endTime: unionEnd
            )
        )
    }

    /// The existing listener-authored mark that a repeat correction belongs to,
    /// or nil when this correction is about a region the listener has not
    /// marked before.
    ///
    /// playhead-1mq1.2. The predicate is OVERLAP, and the reason it is safe to
    /// merge on overlap is geometric rather than heuristic: the union of two
    /// spans that overlap contains no point that neither span covered. Merging
    /// them can therefore never place unmarked SHOW content inside a skippable
    /// region — the failure Dan named as the expensive one, because a listener
    /// who loses part of the show to a skip loses more than a missed ad costs
    /// them. Two DISJOINT marks would have that gap, and disjoint marks are
    /// exactly what this predicate declines to merge.
    ///
    /// Touching is not overlapping. Two ads in one pod, the first marked to
    /// 130.0 and the second from 130.0, stay two rows; the comparison is
    /// strict on both sides.
    ///
    /// This is why the rule is not "the spans are nearly equal". No two taps
    /// ever produce equal spans, because `BoundaryExpander` re-expands from
    /// each new seed, and a listener who taps a SECOND time is usually telling
    /// us the ad ran past the edge we drew — their new span reaches mostly
    /// PAST the old one. That is the case playhead-q0tj is about, and a
    /// similarity test is precisely the test that fails it.
    private func existingUserMarkCovering(
        analysisAssetId: String,
        start: Double,
        end: Double
    ) async -> AdWindow? {
        let existing: [AdWindow]
        do {
            existing = try await store.fetchAdWindows(assetId: analysisAssetId)
        } catch {
            // A failed read must not silently become "no existing mark", which
            // would mint the duplicate row this lookup exists to prevent. Say
            // so in the log rather than swallowing it, and let the caller's
            // insert proceed — one extra row is a better outcome than dropping
            // the listener's correction on the floor.
            logger.warning(
                "recordUserMarkedAd: existing-mark lookup failed, repeat-correction dedupe is OFF for this gesture: \(error.localizedDescription, privacy: .public)"
            )
            return nil
        }
        return existing.first { candidate in
            guard candidate.boundaryState == UserSpanAssertion.userMarked.rawValue,
                  candidate.endTime > candidate.startTime else {
                return false
            }
            return start < candidate.endTime && candidate.startTime < end
        }
    }

    /// Persist a user-marked ad region as an AdWindow and CorrectionEvent.
    /// Called from PlayheadRuntime when the user reports hearing an ad that
    /// the detector missed (false negative correction).
    ///
    /// playhead-1mq1.2 (absorbs playhead-59t3 and playhead-q0tj): this returns
    /// the durable identity that now represents the correction, not a bare
    /// Bool. The Bool was the defect. Every caller minted a fresh `windowId`,
    /// passed it in, and then injected THAT id into the orchestrator, so a
    /// repeat correction over an ad the listener had already marked produced a
    /// second `ad_windows` row and a second in-memory twin for one ad. Two
    /// spans in the 2026-09-02 device pull carry exactly that duplicate pair.
    ///
    /// A caller must use `outcome.windowId` for the live cue, never the id it
    /// passed in: on `.alreadyMarked` and `.extended` those differ, and folding
    /// the cue into a different id than the durable row is the
    /// two-representations defect playhead-o4qr already paid for once.
    @discardableResult
    func recordUserMarkedAd(
        analysisAssetId: String,
        startTime: Double,
        endTime: Double,
        podcastId: String?,
        windowId: String = UUID().uuidString
    ) async -> UserMarkPersistence {
        guard RecurrenceMaterialIdentity.canonicalIdentifier(
                  analysisAssetId
              ) != nil,
              RecurrenceMaterialIdentity.canonicalIdentifier(windowId) != nil,
              podcastId == nil
                || RecurrenceMaterialIdentity.canonicalIdentifier(podcastId)
                    != nil,
              startTime.isFinite,
              endTime.isFinite,
              startTime >= 0,
              endTime > startTime else {
            return .rejected
        }

        // playhead-1mq1.2: resolve the correction to an EXISTING listener mark
        // before minting anything. `requested*` is the normalized span; the
        // caller's argument order is not guaranteed and `CorrectionScope` below
        // already normalizes it.
        let requestedStart = min(startTime, endTime)
        let requestedEnd = max(startTime, endTime)
        if let existingMark = await existingUserMarkCovering(
            analysisAssetId: analysisAssetId,
            start: requestedStart,
            end: requestedEnd
        ) {
            return await foldIntoExistingUserMark(
                existingMark,
                analysisAssetId: analysisAssetId,
                requestedStart: requestedStart,
                requestedEnd: requestedEnd,
                podcastId: podcastId
            )
        }

        let adWindow = AdWindow(
            id: windowId,
            analysisAssetId: analysisAssetId,
            startTime: startTime,
            endTime: endTime,
            confidence: 1.0,
            boundaryState: "userMarked",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "userCorrection",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: startTime,
            metadataSource: "userCorrection",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false,
            // playhead-527u (product-owner AC): a user's manual ad mark is the
            // highest-certainty "this IS an ad" signal we have, so the region is
            // AUTO-SKIP-ELIGIBLE, not merely banner/markOnly. Stamp the gate
            // `.eligible` at write time as the EXPLICIT, honest semantic.
            //
            // Reviewer-527u correction (MUST-RESOLVE #1): the reload path does
            // NOT require this stamp to auto-skip. `beginEpisode` preloads
            // `ad_windows` (confidence 1.0, `.confirmed` ⇒ preload-eligible) into
            // `receiveAdWindows`, whose gate filter drops ONLY recognised
            // NON-eligible cases (`.markOnly` → suggest tier; `.blocked*` /
            // `.blockedByFMConsensus` → dropped). Both `nil` AND `.eligible`
            // fall THROUGH to the managed path, and `evaluateWindow` never re-
            // checks the gate — so in `.auto` mode a userMarked row auto-skips
            // regardless of gate. userMarked rows never traverse the stricter
            // `receiveAdDecisionResults` eligible-only filter (that path carries
            // `AdDecisionResult`s, not persisted `AdWindow`s). Consequently a
            // PRE-EXISTING dogfood mark persisted before 527u with `gate == nil`
            // ALSO auto-skips on reload (proven by `UserAddedMarkSurvivesBackfill
            // Tests` MR1) — no migration is needed. The explicit stamp is
            // belt-and-suspenders: it is the correct value, is self-documenting,
            // and is robust to any future tightening of the `receiveAdWindows`
            // guard that would drop `nil`. It only auto-skips WHERE the mode
            // already auto-skips (`.auto`); in `.manual`/`.shadow`,
            // `evaluateWindow` returns `.confirmed` (log-only, no skip) — identical
            // to the `nil` behaviour. The re-derived fusion window over the same
            // region is deduped away in `reconcileBackfillWindows`, so this row is
            // the single surfaced window for the marked ad. Only THIS definitive
            // user-marked region is promoted; ordinary re-derived / FM host-read
            // windows keep the certainty-tiered gate they were minted with.
            eligibilityGate: SkipEligibilityGate.eligible.rawValue
        )

        let correctionScope = CorrectionScope.exactTimeSpan(
            assetId: analysisAssetId,
            startTime: min(startTime, endTime),
            endTime: max(startTime, endTime)
        )
        let correction = CorrectionEvent(
            analysisAssetId: analysisAssetId,
            scope: correctionScope.serialized,
            source: .falseNegative,
            podcastId: podcastId,
            correctionType: .falseNegative
        )

        let didPersistWindow: Bool
        do {
            try await store.persistUserMarkedAd(
                window: adWindow,
                correction: correction
            )
            didPersistWindow = true
        } catch {
            logger.warning(
                "Failed to atomically persist user-marked ad window and correction: \(error.localizedDescription)"
            )
            didPersistWindow = false
        }

        // The false-negative correction is committed in the same SQLite
        // transaction as the AdWindow above. Do not route this gesture through
        // the non-throwing `recordVeto` convenience API: a window-only success
        // would acknowledge feedback that the learning system never received.
        guard didPersistWindow else { return .rejected }

        // Training materialization and catalog ingress are derived,
        // idempotent work. The atomic user-facing receipt above is already
        // durable; never hold the live cue/timeline injection behind these
        // secondary SQLite/catalog actor hops.
#if DEBUG
        if let handler = postUserMarkCommitHandlerForTesting {
            Task {
                await handler()
            }
        } else {
            scheduleUserMarkedAdDerivedWork(
                analysisAssetId: analysisAssetId,
                startTime: startTime,
                endTime: endTime,
                podcastId: podcastId,
                windowId: windowId
            )
        }
#else
        scheduleUserMarkedAdDerivedWork(
            analysisAssetId: analysisAssetId,
            startTime: startTime,
            endTime: endTime,
            podcastId: podcastId,
            windowId: windowId
        )
#endif
        return .recorded(
            UserMarkIdentity(
                windowId: windowId,
                startTime: startTime,
                endTime: endTime
            )
        )
    }

    private func scheduleUserMarkedAdDerivedWork(
        analysisAssetId: String,
        startTime: Double,
        endTime: Double,
        podcastId: String?,
        windowId: String
    ) {
#if DEBUG
        userMarkedAdDerivedWorkCount += 1
#endif
        Task {
            await performUserMarkedAdDerivedWork(
                analysisAssetId: analysisAssetId,
                startTime: startTime,
                endTime: endTime,
                podcastId: podcastId,
                windowId: windowId
            )
#if DEBUG
            finishUserMarkedAdDerivedWorkForTesting()
#endif
        }
    }

#if DEBUG
    private func finishUserMarkedAdDerivedWorkForTesting() {
        precondition(userMarkedAdDerivedWorkCount > 0)
        userMarkedAdDerivedWorkCount -= 1
        guard userMarkedAdDerivedWorkCount == 0 else { return }
        let waiters = userMarkedAdDerivedWorkWaiters
        userMarkedAdDerivedWorkWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
    }
#endif

    private func performUserMarkedAdDerivedWork(
        analysisAssetId: String,
        startTime: Double,
        endTime: Double,
        podcastId: String?,
        windowId: String
    ) async {
        // The atomic store path intentionally bypasses
        // `PersistentUserCorrectionStore.record`, whose optional ingestor
        // normally refreshes derived training examples after an append.
        await materializeTrainingExamples(forAsset: analysisAssetId)

        // A user-confirmed span is the strongest catalog label. Missing
        // feature windows or catalog failures are secondary precision misses,
        // never failures of the already-committed user action.
        guard adCatalogStore != nil || repeatedAdCache != nil,
              let showId =
                RecurrenceMaterialIdentity.canonicalIdentifier(podcastId)
        else {
            return
        }
        let sourceStartTime = min(startTime, endTime)
        let sourceEndTime = max(startTime, endTime)
        guard sourceEndTime > sourceStartTime else { return }
        let featureWindows: [FeatureWindow]
        do {
            featureWindows = try await store.fetchFeatureWindows(
                assetId: analysisAssetId,
                from: sourceStartTime,
                to: sourceEndTime
            )
        } catch {
            logger.warning("recordUserMarkedAd: fetchFeatureWindows failed (skipping catalog insert): \(error.localizedDescription, privacy: .public)")
            return
        }
        let fingerprint = AcousticFingerprint.fromFeatureWindows(
            featureWindows
        )
        if let adCatalogStore, !fingerprint.isZero {
            do {
                _ = try await adCatalogStore.insert(
                    showId: showId,
                    episodePosition: .unknown,
                    durationSec: sourceEndTime - sourceStartTime,
                    acousticFingerprint: fingerprint,
                    transcriptSnippet: nil,
                    sponsorTokens: nil,
                    originalConfidence: 1.0,
                    learningSource: .userMarkedAd,
                    learningLifecycle: .explicitConfirmation,
                    sourceAssetId: analysisAssetId,
                    sourceWindowId: windowId,
                    sourceStartTime: sourceStartTime,
                    sourceEndTime: sourceEndTime
                )
                logger.debug("recordUserMarkedAd: inserted catalog entry for user-marked ad on asset \(analysisAssetId, privacy: .public)")
            } catch {
                logger.warning("recordUserMarkedAd: catalog insert failed: \(error.localizedDescription, privacy: .public)")
            }
        }
        let repeatedFingerprint = RepeatedAdFingerprint.from(
            featureWindows: featureWindows
        )
        if let repeatedAdCache, !repeatedFingerprint.isZero {
            do {
                _ = try await repeatedAdCache.store(
                    showId: showId,
                    fingerprint: repeatedFingerprint,
                    boundaryStart: sourceStartTime,
                    boundaryEnd: sourceEndTime,
                    confidence: 1,
                    learningSource: .userMarkedAd,
                    learningLifecycle: .explicitConfirmation,
                    sourceAssetId: analysisAssetId,
                    sourceWindowId: windowId
                )
            } catch {
                logger.warning("recordUserMarkedAd: repeated-ad insert failed: \(error.localizedDescription, privacy: .public)")
            }
        }
    }

    // skeptical-review-cycle-16 M-1: dead `updateProfile(_:)` setter
    // removed (zero callers in production or tests). The post-hoc
    // in-memory write at `currentPodcastProfile = profile` could clobber
    // an in-flight `updatePriors`'s post-await assignment if a future
    // caller were ever added, so the note here used to say: gate the
    // post-await assignments on a generation token before re-introducing
    // the setter.
    //
    // playhead-2kxd removed the slot that made that necessary. The profile
    // is now `podcastProfilesByShowId`, keyed by show, written only through
    // `cachePodcastProfile(_:)` which keys on the value's own `podcastId`.
    // A returning setter would file its argument under that argument's show,
    // so it could not clobber another show's entry at all — and against the
    // SAME show it is last-writer-wins on one row, which is what a setter
    // means. No generation token is needed; what a setter would still owe is
    // a reason it is not just re-reading what `updatePriors` persists.

    /// Rebuild the smallest hot-path replay slice that can still reproduce the
    /// hypothesis engine's transitive context growth for duplicate chunk
    /// re-emits. The closure grows only through chunks that already present a
    /// primary lexical signal or weak-anchor recovery text, then fills in the
    /// intervening transcript for boundary expansion and lexical fallback.
    func hotPathReplayContextChunks(
        from allChunks: [TranscriptChunk],
        around persistedChunks: [TranscriptChunk]
    ) -> [TranscriptChunk] {
        let fastAllChunks = allChunks
            .filter { $0.pass == TranscriptPassType.fast.rawValue }
            .sorted { lhs, rhs in
                if lhs.startTime != rhs.startTime {
                    return lhs.startTime < rhs.startTime
                }
                return lhs.endTime < rhs.endTime
            }
        let seedIDs = Set(
            persistedChunks
                .filter { $0.pass == TranscriptPassType.fast.rawValue }
                .map(\.id)
        )
        let seedChunks = fastAllChunks.filter { seedIDs.contains($0.id) }
        guard !seedChunks.isEmpty else { return [] }

        let padding = SpanHypothesisConfig.default.maximumContextPadding
        let signalProfilesByChunkID: [String: ReplaySignalProfile] = Dictionary(
            uniqueKeysWithValues: fastAllChunks.compactMap { chunk in
                let profile = replaySignalProfile(for: chunk)
                guard profile.hasSignal else { return nil }
                return (chunk.id, profile)
            }
        )
        var relevantChunkIDs = Set(seedChunks.map(\.id))
        var windowStart = seedChunks.map(\.startTime).min() ?? 0
        var windowEnd = seedChunks.map(\.endTime).max() ?? 0
        var currentBackwardReach: TimeInterval = padding
        var currentForwardReach: TimeInterval = padding

        let seedProfiles: [ReplaySignalProfile] = seedChunks.compactMap { signalProfilesByChunkID[$0.id] }
        let seedHasDirectionalSignal = seedProfiles.contains { $0.hasDirectionalSignal }
        if seedHasDirectionalSignal {
            currentBackwardReach = seedProfiles.map(\.backwardReach).max() ?? 0
            currentForwardReach = seedProfiles.map(\.forwardReach).max() ?? 0
        }

        var changed = true
        while changed {
            changed = false
            for chunk in fastAllChunks where !relevantChunkIDs.contains(chunk.id) {
                guard let signalProfile = signalProfilesByChunkID[chunk.id] else { continue }
                guard chunk.endTime >= windowStart - currentBackwardReach,
                      chunk.startTime <= windowEnd + currentForwardReach
                else {
                    continue
                }

                relevantChunkIDs.insert(chunk.id)
                windowStart = min(windowStart, chunk.startTime)
                windowEnd = max(windowEnd, chunk.endTime)
                if signalProfile.hasDirectionalSignal {
                    currentBackwardReach = max(currentBackwardReach, signalProfile.backwardReach)
                    currentForwardReach = max(currentForwardReach, signalProfile.forwardReach)
                }
                changed = true
            }
        }

        return fastAllChunks.filter { chunk in
            chunk.endTime >= windowStart && chunk.startTime <= windowEnd
        }
    }

    // MARK: - Tier 1: Feature-Only Scoring (playhead-gtt9.9)

    /// Default slot length (seconds) for Tier 1's transcript-independent
    /// sliding window. 30 s matches the canonical short-ad atom in
    /// `GlobalPriorDefaults` and keeps the number of classifier invocations
    /// proportional to episode length (≈120 calls on a 1-hour show).
    static let tier1DefaultWindowSeconds: TimeInterval = 30.0

    /// Internal slot bookkeeping for `runTier1FeatureOnlyScoring`.
    /// Each slot becomes a synthesized `LexicalCandidate` carrying features,
    /// then a classifier call, then a `DecisionLogEntry`.
    private struct Tier1Slot: Sendable {
        let index: Int
        let startTime: Double
        let endTime: Double
    }

    /// Tier 1 scoring: emit a scored `DecisionLogEntry` for every
    /// non-overlapping `windowSeconds` slot in [0, episodeDuration), regardless
    /// of transcript state. This fixes the gtt9.9 regression where empty-
    /// transcript episodes produced zero scored windows because every
    /// candidate was derived from transcript atoms.
    ///
    /// Tier 1 uses ONLY feature-derived and metadata signals (feature windows
    /// from the acoustic extractor, plus the classifier's own time-position
    /// prior fed via `episodeDuration`). Transcript-dependent evidence —
    /// lexical, FM, catalog, promotion — is Tier 2's job (`runBackfill`) and
    /// REFINES Tier 1 scores without gating whether a region is scored.
    ///
    /// Hard contract:
    /// - Every second of the episode (modulo a <1 s trailing sliver) is
    ///   evaluated and logged, even with zero transcript chunks.
    /// - The emitted `DecisionLogEntry.action` mirrors the hot path:
    ///   `autoSkipEligible` ≥ `config.autoSkipConfidenceThreshold`,
    ///   `hotPathCandidate` ≥ `config.candidateThreshold`, else
    ///   `hotPathBelowThreshold`.
    /// - Tier 1 does NOT persist `AdWindow`s. The aggregation step (gtt9.10)
    ///   owns span materialization.
    /// - Tier 1 does NOT grant auto-skip authority on its own. Existing
    ///   eligibility gates in `DecisionMapper`/`SkipPolicyMatrix` remain the
    ///   sole skip authorities.
    ///
    /// - Parameters:
    ///   - analysisAssetId: Asset to score.
    ///   - episodeDuration: Full episode length in seconds.
    ///   - windowSeconds: Slot length (default `tier1DefaultWindowSeconds`).
    /// - Returns: Number of `DecisionLogEntry` records emitted.
    @discardableResult
    func runTier1FeatureOnlyScoring(
        analysisAssetId: String,
        episodeDuration: Double,
        windowSeconds: TimeInterval = AdDetectionService.tier1DefaultWindowSeconds
    ) async throws -> Int {
        let results = try await runTier1Scoring(
            analysisAssetId: analysisAssetId,
            episodeDuration: episodeDuration,
            windowSeconds: windowSeconds
        )
        return results.count
    }

    /// playhead-0usd: Same contract as `runTier1FeatureOnlyScoring`, but
    /// returns the `[ClassifierResult]` stream so the caller (the hot path
    /// integrator) can feed the scores through `SegmentAggregator` without
    /// re-running the classifier. The decision-log emission side effect is
    /// preserved 1:1, so any existing caller of the public
    /// `runTier1FeatureOnlyScoring` sees no observable difference.
    private func runTier1Scoring(
        analysisAssetId: String,
        episodeDuration: Double,
        windowSeconds: TimeInterval = AdDetectionService.tier1DefaultWindowSeconds
    ) async throws -> [ClassifierResult] {
        // Record episode duration so the classifier's position-based prior
        // sees the same value both Tier 1 and Tier 2 use.
        self.episodeDuration = episodeDuration

        let slots = makeTier1Slots(
            episodeDuration: episodeDuration,
            windowSeconds: windowSeconds
        )
        guard !slots.isEmpty else {
            logger.info("Tier 1: no slots (episodeDuration=\(episodeDuration), windowSeconds=\(windowSeconds))")
            return []
        }

        // Single range fetch — cheaper than N overlapping queries.
        let allFeatureWindows = try await store.fetchFeatureWindows(
            assetId: analysisAssetId,
            from: 0,
            to: episodeDuration
        )
        let featureWindowsBySlot = bucketFeatureWindowsBySlot(
            allFeatureWindows,
            slots: slots
        )

        var inputs: [ClassifierInput] = []
        inputs.reserveCapacity(slots.count)
        for slot in slots {
            let candidate = makeTier1SyntheticCandidate(
                analysisAssetId: analysisAssetId,
                slot: slot
            )
            inputs.append(ClassifierInput(
                candidate: candidate,
                featureWindows: featureWindowsBySlot[slot.index] ?? [],
                episodeDuration: episodeDuration
            ))
        }

        let results = classifier.classify(inputs: inputs, priors: showPriors)
        await emitTier1DecisionLogs(
            classifierResults: results,
            analysisAssetId: analysisAssetId
        )
        logger.info("Tier 1: emitted \(results.count) decision-log entries over \(slots.count) slots (asset=\(analysisAssetId))")
        return results
    }

    /// Slice [0, episodeDuration) into non-overlapping slots of `windowSeconds`.
    /// Trailing slivers < 1 s are dropped (noise-floor guard).
    private func makeTier1Slots(
        episodeDuration: Double,
        windowSeconds: TimeInterval
    ) -> [Tier1Slot] {
        guard episodeDuration > 0, windowSeconds > 0 else { return [] }
        var slots: [Tier1Slot] = []
        var t = 0.0
        var idx = 0
        let minTail = 1.0
        while t < episodeDuration {
            let end = min(t + windowSeconds, episodeDuration)
            let span = end - t
            if span < minTail { break }
            slots.append(Tier1Slot(index: idx, startTime: t, endTime: end))
            idx += 1
            t = end
        }
        return slots
    }

    /// Bucket feature windows into the slot whose `[start, end)` contains
    /// the feature window's midpoint. O(n+m).
    private func bucketFeatureWindowsBySlot(
        _ windows: [FeatureWindow],
        slots: [Tier1Slot]
    ) -> [Int: [FeatureWindow]] {
        guard !slots.isEmpty else { return [:] }
        var buckets: [Int: [FeatureWindow]] = [:]
        let slotLength = slots.first!.endTime - slots.first!.startTime
        guard slotLength > 0 else { return [:] }
        for window in windows {
            let midpoint = (window.startTime + window.endTime) / 2
            let idx = Int(midpoint / slotLength)
            guard idx >= 0, idx < slots.count else { continue }
            buckets[idx, default: []].append(window)
        }
        return buckets
    }

    /// Build a minimal-content `LexicalCandidate` for a Tier 1 slot.
    /// confidence=0, empty categories, and a Tier 1-distinguishing id so
    /// downstream tooling can filter Tier 1 rows without extending the
    /// DecisionLogEntry schema.
    private func makeTier1SyntheticCandidate(
        analysisAssetId: String,
        slot: Tier1Slot
    ) -> LexicalCandidate {
        LexicalCandidate(
            id: "tier1-\(analysisAssetId)-\(slot.index)",
            analysisAssetId: analysisAssetId,
            startTime: slot.startTime,
            endTime: slot.endTime,
            confidence: 0.0,
            hitCount: 0,
            categories: [],
            evidenceText: "",
            evidenceStartTime: slot.startTime,
            detectorVersion: config.detectorVersion
        )
    }

    /// Mirror of `emitHotPathDecisionLogs` action naming. Kept separate from
    /// hot path so future Tier 1 evolution (acoustic evidence, metadata
    /// corroboration) does not require a flag-laden shared helper.
    private func emitTier1DecisionLogs(
        classifierResults: [ClassifierResult],
        analysisAssetId: String
    ) async {
        let snapshot = DecisionLogEntry.ActivationConfigSnapshot(
            MetadataActivationConfig.resolved()
        )
        let fusionConfig = FusionWeightConfig()
        let classifierCap = fusionConfig.classifierCap
        for result in classifierResults {
            let timestamp = Date().timeIntervalSince1970
            let passed = result.adProbability >= config.candidateThreshold
            // playhead-fqc8 cycle-1 review HIGH-2: the hot path uses the
            // standard `autoSkipConfidenceThreshold` (0.80) here because the
            // qualified-track signal — the acoustic-break alignment that
            // gates `PromotionTrack.classifierSeedQualified` — only becomes
            // available AFTER fusion runs in `runBackfill`. A classifier-
            // seeded span the hot path passes on at 0.55 may still be
            // promoted to `autoSkipEligible` later, once the alignment
            // evidence joins the ledger and `DecisionMapper.computePromotionTrack`
            // selects the looser `classifierSeedQualifiedThreshold`. This
            // hot-path / backfill bifurcation is intentional and is the
            // central design decision of bead playhead-fqc8.
            let promotesToAutoSkip = result.adProbability >= config.autoSkipConfidenceThreshold
            let action: String
            let thresholdCrossed: Double
            if promotesToAutoSkip {
                action = "autoSkipEligible"
                thresholdCrossed = config.autoSkipConfidenceThreshold
            } else if passed {
                action = "hotPathCandidate"
                thresholdCrossed = config.candidateThreshold
            } else {
                action = "hotPathBelowThreshold"
                thresholdCrossed = config.candidateThreshold
            }
            let clampedScore = max(0.0, min(1.0, result.adProbability))
            let cappedWeight = min(clampedScore * classifierCap, classifierCap)
            let classifierEntry = EvidenceLedgerEntry(
                source: .classifier,
                weight: cappedWeight,
                detail: .classifier(score: result.adProbability)
            )
            let authority: ProposalAuthority = cappedWeight > classifierCap * 0.5 ? .strong : .weak
            let breakdown = [
                SourceEvidence(
                    source: EvidenceSourceType.classifier.rawValue,
                    weight: cappedWeight,
                    capApplied: classifierCap,
                    authority: authority
                )
            ]
            // playhead-gtt9.20: mirror the hot-path emit-site expansion so
            // Tier 1 entries that clear autoSkipConfidenceThreshold also carry
            // boundary-expanded bounds. In production Tier 1 slots are 30 s
            // and the duration guard inside `expandedBounds` short-circuits
            // (no expansion); the call is a cheap no-op there. The mirror is
            // structural — keeps both emit sites aligned so the NARL harness
            // sees the same bounds shape regardless of which path produced
            // the autoSkipEligible verdict.
            let logBounds: (start: Double, end: Double)
            if promotesToAutoSkip {
                let expanded = await expandedBounds(
                    for: result,
                    analysisAssetId: analysisAssetId
                )
                logBounds = (expanded.startTime, expanded.endTime)
            } else {
                logBounds = (result.startTime, result.endTime)
            }
            let logEntry = DecisionLogEntry(
                schemaVersion: DecisionLogEntry.currentSchemaVersion,
                analysisAssetID: analysisAssetId,
                timestamp: timestamp,
                windowBounds: .init(start: logBounds.start, end: logBounds.end),
                activationConfig: snapshot,
                evidence: [DecisionLogEntry.LedgerEntry(classifierEntry)],
                fusedConfidence: .init(
                    proposalConfidence: result.adProbability,
                    skipConfidence: result.adProbability,
                    breakdown: breakdown
                ),
                finalDecision: .init(
                    action: action,
                    gate: "eligible",
                    skipConfidence: result.adProbability,
                    thresholdCrossed: thresholdCrossed
                )
            )
            await decisionLogger.record(logEntry)
        }
    }

    // MARK: - Hot Path

    /// Run the hot-path detection pipeline on fast-pass transcript chunks
    /// and feature windows. Produces candidate AdWindows ahead of the playhead.
    ///
    /// Flow:
    ///   1. LexicalScanner -> candidate regions from transcript
    ///   2. Fetch overlapping FeatureWindows from SQLite
    ///   3. ClassifierService -> scored results with boundary refinement
    ///   4. Filter by candidateThreshold and persist as AdWindows
    ///   5. Return new AdWindows for SkipOrchestrator
    ///
    /// - Parameters:
    ///   - chunks: Fast-pass TranscriptChunks from TranscriptEngineService.
    ///   - analysisAssetId: The analysis asset being processed.
    ///   - episodeDuration: Total episode duration in seconds.
    /// - Returns: Newly detected AdWindows with decisionState = .candidate.
    func runHotPath(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        episodeDuration: Double,
        podcastId: String?
    ) async throws -> [AdWindow] {
        try await runHotPathResult(
            chunks: chunks,
            analysisAssetId: analysisAssetId,
            episodeDuration: episodeDuration,
            podcastId: podcastId
        ).windows
    }

    func runHotPathResult(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        episodeDuration: Double,
        podcastId: String? = nil,
        retireUnmatchedReplayCandidates: Bool = false
    ) async throws -> HotPathRunResult {
        // playhead-hygc.1.8 (R7): enforce the documented
        // no-concurrent-runHotPath-per-asset invariant. Actor reentrancy
        // across `await`s could otherwise let a second caller compute
        // fresh UUIDs for the same FN range and double-insert replay
        // rows. In DEBUG we crash fast; in RELEASE we log a warning and
        // proceed (the legacy belt-and-suspenders dedupe in
        // `correctionReplayCandidates` keeps the same-call path
        // tolerable, but no consumer should rely on that).
        if hotPathRunInFlightAssetIds.contains(analysisAssetId) {
            assertionFailure(
                "runHotPathResult called concurrently for asset \(analysisAssetId)"
            )
            logger.warning(
                "runHotPathResult: concurrent invocation detected for asset \(analysisAssetId, privacy: .public) — production caller chain expects single-shot serialization"
            )
        }
        hotPathRunInFlightAssetIds.insert(analysisAssetId)
        defer { hotPathRunInFlightAssetIds.remove(analysisAssetId) }

        self.episodeDuration = episodeDuration

        // playhead-hygc.1.8: correction-replay recall step. A
        // `.falseNegative` `.exactTimeSpan` correction event is the user's
        // strongest possible label that "this WAS an ad" for a region the
        // detector missed — but the detector itself produces no AdWindow
        // for the region (no transcript token, no Tier-1 spike, no FM
        // signal). On the same episode this lands as a `userMarked`
        // AdWindow inside `recordUserMarkedAd`; on subsequent runs of the
        // same episode (e.g. cross-launch preload, dogfood replay) the
        // userMarked row persists and is observed via `fetchAdWindows`.
        // For DOGFOOD-CAPTURED corrections that get replayed against the
        // raw analysis SQLite via NARL, however, we need to surface the
        // correction as a *detector-emitted* candidate so the replay
        // counts it for recall. May 6 fixture: 41/65 unrecovered
        // userMarked windows had a `.falseNegative` correction event but
        // no overlapping algorithmic AdWindow; this step turns those
        // missing rows into mark-only candidates without expanding
        // auto-skip. The mark-only label keeps the precision contract
        // intact — the user still has the suggest-tier banner and can
        // veto it via `revertByTimeRange`, which (post-hygc.1.8) reverts
        // the suggest-tier dictionary AND persists `decisionState =
        // .reverted` so the next run does not re-emit.
        let correctionReplayWindows = try await correctionReplayCandidates(
            analysisAssetId: analysisAssetId
        )
        if !correctionReplayWindows.isEmpty {
            try await store.upsertHotPathAdWindows(
                correctionReplayWindows,
                existingIDs: [],
                retiredIDs: []
            )
            logger.info(
                "Hot path: correction-replay emitted \(correctionReplayWindows.count) markOnly AdWindows"
            )
        }

        // playhead-gtt9.9: Tier 1 runs FIRST, independent of transcript
        // state. Emits one DecisionLogEntry per slot across [0, episodeDuration)
        // so NARL `scoredCoverageRatio` reflects the full episode even when
        // transcript coverage has stalled. Transcript-dependent hot-path
        // evidence (lexical / hypothesis / FM) remains below the
        // chunks-empty guard — it REFINES scores, it does not gate them.
        //
        // playhead-0usd: Tier 1 results are captured locally so the
        // SegmentAggregator downstream can fuse them with Tier 2 per-window
        // scores into coherent multi-window segments.
        let tier1Results: [ClassifierResult]
        if episodeDuration > 0 {
            tier1Results = try await runTier1Scoring(
                analysisAssetId: analysisAssetId,
                episodeDuration: episodeDuration
            )
        } else {
            tier1Results = []
        }

        guard !chunks.isEmpty else {
            // playhead-0usd: Even with empty transcript chunks, the aggregator
            // is given a chance to promote a segment from Tier 1 evidence
            // alone. This is the "transcript-coverage stalled" scenario where
            // multiple sub-threshold Tier 1 windows collectively establish
            // an ad region without any lexical/FM corroboration.
            let aggregatorWindows = try await runSegmentAggregation(
                tier1Results: tier1Results,
                tier2Results: [],
                singleWindowAdWindows: [],
                analysisAssetId: analysisAssetId,
                podcastId: podcastId
            )
            if !aggregatorWindows.isEmpty {
                try await store.upsertHotPathAdWindows(
                    aggregatorWindows,
                    existingIDs: [],
                    retiredIDs: []
                )
                logger.info("Hot path: aggregator persisted \(aggregatorWindows.count) AdWindows (chunks-empty branch)")
            }
            // playhead-hygc.1.8: include correction-replay windows in the
            // returned set so SkipOrchestrator surfaces them as suggest-tier.
            return HotPathRunResult(
                windows: aggregatorWindows + correctionReplayWindows,
                retiredWindowIDs: []
            )
        }

        // playhead-hygc.1.8: correction-replay rows must NEVER be retired
        // by the algorithmic-absence path. R3 found that fresh replay rows
        // emitted in the same run were retired before the run finished;
        // R4 found the same bug shifted by one run — a previously-emitted
        // replay row, present in the DB on a subsequent run, would be
        // included in `replayCandidateIDs` because `hotPathCandidateRevisions`
        // filters by `(decisionState=.candidate, detectorVersion=current)`
        // — exactly the stamp on a replay row. The single authoritative
        // fix lives in `hotPathCandidateRevisions`, which now excludes any row
        // whose `boundaryState == correctionReplay`. The local
        // `subtracting` below is retained as a belt-and-suspenders defense
        // against future regressions in that filter (e.g. a replay row
        // mid-flight whose persisted boundaryState is somehow stale) — it
        // is a no-op when the source filter is doing its job.
        let correctionReplayWindowIDs: Set<String> =
            Set(correctionReplayWindows.map(\.id))
        let replayCandidateRevisions: [String: AdWindow]
        if retireUnmatchedReplayCandidates {
            replayCandidateRevisions = try await hotPathCandidateRevisions(
                analysisAssetId: analysisAssetId,
                overlapping: replayEnvelope(for: chunks)
            ).filter { !correctionReplayWindowIDs.contains($0.key) }
        } else {
            replayCandidateRevisions = [:]
        }
        let replayCandidateIDs = Set(replayCandidateRevisions.keys)

        // Layer 1: hypothesis windows take precedence when active; otherwise
        // preserve the legacy lexical merge path.
        let candidates = try await hotPathCandidates(
            from: chunks,
            analysisAssetId: analysisAssetId,
            // playhead-2kxd: the request's show. Already the authority for
            // recurrence matching in `classifyCandidates` below, on the same
            // reasoning; now it is the authority for the metadata trust weight
            // too, instead of whichever show last finished a backfill.
            podcastId: podcastId
        )

        guard !candidates.isEmpty else {
            logger.info("Hot path: no candidates from \(chunks.count) chunks")
            // playhead-0usd: Run aggregator over Tier 1 evidence alone. When
            // Tier 2 has no lexical candidates the single-window path can't
            // fire, but the aggregator may still coalesce Tier 1 into a
            // promoted segment.
            let aggregatorWindows = try await runSegmentAggregation(
                tier1Results: tier1Results,
                tier2Results: [],
                singleWindowAdWindows: [],
                analysisAssetId: analysisAssetId,
                podcastId: podcastId
            )
            if !aggregatorWindows.isEmpty || !replayCandidateIDs.isEmpty {
                try await store.upsertHotPathAdWindows(
                    aggregatorWindows,
                    existingIDs: [],
                    retiredIDs: replayCandidateIDs,
                    expectedProducerRevisions: replayCandidateRevisions
                )
            }
            if !aggregatorWindows.isEmpty {
                logger.info("Hot path: aggregator persisted \(aggregatorWindows.count) AdWindows (no-candidates branch)")
            }
            // playhead-hygc.1.8: include correction-replay windows.
            return HotPathRunResult(
                windows: aggregatorWindows + correctionReplayWindows,
                retiredWindowIDs: replayCandidateIDs
            )
        }

        logger.info("Hot path: \(candidates.count) candidates from \(chunks.count) chunks")

        // Layer 0 + Layer 2: Fetch features, classify, refine boundaries.
        let classifierResults = try await classifyCandidates(
            candidates,
            analysisAssetId: analysisAssetId,
            podcastId: podcastId
        )

        let candidatesByID = Dictionary(uniqueKeysWithValues: candidates.map { ($0.id, $0) })

        // Filter by candidate threshold and build AdWindows.
        // playhead-gtt9.4.1: For high-confidence, narrow classifier hits we
        // expand the persisted window extents outward to nearby acoustic breaks.
        // This does NOT re-score or change adProbability — it only widens the
        // persisted span so Sec-F1 reflects the true ad coverage.
        var adWindows: [AdWindow] = []
        let passingResults = classifierResults.filter { $0.adProbability >= config.candidateThreshold }
        for result in passingResults {
            let expanded = await expandedBounds(
                for: result,
                analysisAssetId: analysisAssetId
            )
            // playhead-gtt9.11: consult the precision gate before persistence.
            // Lexical categories come from the seeding candidate so sponsor/
            // promoCode/urlCTA/purchaseLanguage hits fire the
            // strongLexicalAdPhrase safety signal. When the result didn't
            // originate from a lexical candidate (e.g. hypothesis-driven),
            // the categories set is empty and the gate falls back to
            // acoustic/slot/user-correction signals alone.
            let lexicalCategories: Set<LexicalPatternCategory>
            if let seedingCandidate = candidatesByID[result.candidateId] {
                lexicalCategories = seedingCandidate.categories
            } else {
                lexicalCategories = []
            }
            let gateResult = await precisionGateLabel(
                analysisAssetId: analysisAssetId,
                startTime: expanded.startTime,
                endTime: expanded.endTime,
                segmentScore: result.adProbability,
                lexicalCategories: lexicalCategories,
                podcastId: podcastId
            )
            // playhead-bllt: the EXTENT half of the verdict, for the hot path.
            // `precisionGateLabel` answered PRESENCE; this site owns the edges,
            // and this site invents them — no boundary refiner and no byte
            // differ ran on the single-window path, so both anchors are
            // `.unanchored`. That is not a placeholder, it is the honest value,
            // and `HotPathExtentGate` demotes an `"autoSkip"` verdict over
            // invented edges to `"markOnly"` exactly as playhead-2350 does for
            // the fused verdict. ONE variable, persisted AND consulted, so the
            // label can never disagree with the anchors the row carries.
            let extentSupport = SpanExtentSupport.unanchored
            let gatedLabel = HotPathExtentGate.gatedLabel(
                gateResult.label,
                extent: extentSupport,
                blockingUnanchoredAutoSkip: config.unanchoredExtentBlocksAutoSkip
            )
            if gatedLabel != gateResult.label {
                logger.info(
                    "[bllt] hot-path single-window \(expanded.startTime, format: .fixed(precision: 2))–\(expanded.endTime, format: .fixed(precision: 2)) extent unanchored on \(extentSupport.unanchoredEdges.joined(separator: "+"), privacy: .public) — autoSkip→markOnly (confidence \(result.adProbability, format: .fixed(precision: 2)) unchanged)"
                )
            }
            // `.detectionOnly` is represented by a nil label and is a
            // terminal "do not persist" result. Persisting it would erase the
            // precision decision at the AdWindow boundary because legacy nil
            // gates remain admissible to SkipOrchestrator. `gatedLabel`
            // preserves nil exactly — it never drops a row and never creates
            // one.
            guard let eligibilityGate = gatedLabel else { continue }
            adWindows.append(buildAdWindow(
                from: result,
                boundaryState: .acousticRefined,
                decisionState: .candidate,
                evidenceText: candidatesByID[result.candidateId]?.evidenceText,
                evidenceStartTime: candidatesByID[result.candidateId]?.evidenceStartTime,
                expandedStartTime: expanded.startTime,
                expandedEndTime: expanded.endTime,
                eligibilityGate: eligibilityGate,
                catalogMatch: gateResult.catalogMatch,
                catalogStoreMatchSimilarity:
                    gateResult.catalogStoreMatchSimilarity,
                extentSupport: extentSupport
            ))
        }

        // playhead-8em9 (narL): log per-candidate hot-path decisions. The
        // hot path is pre-fusion: the only evidence we have here is the
        // classifier score, so the DecisionLogEntry carries a single
        // `.classifier` ledger entry and a degenerate one-source fused
        // breakdown. Replay tooling distinguishes hot-path from backfill
        // entries by evidence cardinality + finalDecision.action value.
        await emitHotPathDecisionLogs(
            classifierResults: classifierResults,
            analysisAssetId: analysisAssetId
        )

        // playhead-0usd: Build aggregator windows from Tier 1 + Tier 2
        // classifier results. Aggregator segments overlapping any single-
        // window AdWindow from this run are filtered out (the single-window
        // path wins for those regions — it has richer evidence text /
        // boundary refinement). The single-window `adWindows` are passed in
        // for overlap-dedup.
        let aggregatorWindows = try await runSegmentAggregation(
            tier1Results: tier1Results,
            tier2Results: classifierResults,
            singleWindowAdWindows: adWindows,
            analysisAssetId: analysisAssetId,
            lexicalCandidates: candidates,
            podcastId: podcastId
        )

        guard !adWindows.isEmpty || !aggregatorWindows.isEmpty else {
            logger.info("Hot path: all \(classifierResults.count) results below threshold and no aggregator segments")
            if !replayCandidateIDs.isEmpty {
                try await store.upsertHotPathAdWindows(
                    [],
                    existingIDs: [],
                    retiredIDs: replayCandidateIDs,
                    expectedProducerRevisions: replayCandidateRevisions
                )
            }
            // playhead-hygc.1.8: include correction-replay windows.
            return HotPathRunResult(
                windows: correctionReplayWindows,
                retiredWindowIDs: replayCandidateIDs
            )
        }

        let reconciledWindows = try await reconcileHotPathWindows(
            adWindows,
            analysisAssetId: analysisAssetId
        )
        guard !reconciledWindows.isEmpty || !aggregatorWindows.isEmpty else {
            logger.info("Hot path: replay matched only terminal windows; nothing new to persist")
            if !replayCandidateIDs.isEmpty {
                try await store.upsertHotPathAdWindows(
                    [],
                    existingIDs: [],
                    retiredIDs: replayCandidateIDs,
                    expectedProducerRevisions: replayCandidateRevisions
                )
            }
            // playhead-hygc.1.8: include correction-replay windows.
            return HotPathRunResult(
                windows: correctionReplayWindows,
                retiredWindowIDs: replayCandidateIDs
            )
        }

        let matchedExistingIDs = Set(reconciledWindows.compactMap(\.matchedExistingID))
        var retiredWindowIDs = reconciledWindows.reduce(into: Set<String>()) { partial, window in
            partial.formUnion(window.retiredExistingIDs)
        }
        if !replayCandidateIDs.isEmpty {
            retiredWindowIDs.formUnion(
                replayCandidateIDs
                    .subtracting(matchedExistingIDs)
                    .subtracting(retiredWindowIDs)
            )
        }

        // Persist to SQLite. playhead-0usd: aggregator-emitted windows are
        // NOT subject to the single-window reconciliation path (they carry
        // no lexical evidence to match against existing candidates), so they
        // are appended alongside the reconciled single-window set.
        let allWindowsToPersist = reconciledWindows.map(\.window) + aggregatorWindows
        var expectedProducerRevisions = replayCandidateRevisions
        for reconciled in reconciledWindows {
            for (id, revision) in reconciled.expectedExistingRevisions {
                expectedProducerRevisions[id] = revision
            }
        }
        try await store.upsertHotPathAdWindows(
            allWindowsToPersist,
            existingIDs: matchedExistingIDs,
            retiredIDs: retiredWindowIDs,
            expectedProducerRevisions: expectedProducerRevisions
        )

        logger.info("Hot path: persisted \(reconciledWindows.count) single-window + \(aggregatorWindows.count) aggregator AdWindows")

        // playhead-hygc.1.8: include correction-replay windows in the
        // returned set so SkipOrchestrator surfaces them as suggest-tier.
        // They were already persisted at the top of the function via a
        // separate `upsertHotPathAdWindows` call so they appear in
        // `fetchAdWindows` queries; we only attach them to the in-memory
        // return list here.
        return HotPathRunResult(
            windows: allWindowsToPersist + correctionReplayWindows,
            retiredWindowIDs: retiredWindowIDs
        )
    }

    // MARK: - Backfill

    /// Run the backfill pipeline: full Phase 1–16 fusion pipeline.
    /// BackfillEvidenceFusion + DecisionMapper are the sole decision authority.
    /// The old promote/suppress path (resolveDecision) is removed.
    ///
    /// Pipeline:
    ///   1.  TranscriptAtomizer
    ///   2.  TranscriptSegmenter + QualityEstimator
    ///   3.  CueHarvesters + EvidenceCatalogBuilder
    ///   4.  RuleBasedClassifier → .classifier ledger entries
    ///   5.  CoveragePlanner
    ///   6.  FM scanning (FMBackfillMode-gated)
    ///   7.  CommercialEvidenceResolver
    ///   8.  RegionProposalBuilder
    ///   9.  RegionFeatureExtractor
    ///   10. AtomEvidenceProjector
    ///   11. MinimalContiguousSpanDecoder
    ///   •   Splice-slot ownership pass (playhead-xsdz.20, flag-gated OFF): a
    ///       post-decode, per-span WIDTH rewrite that runs after decode and
    ///       `assetAcousticBreaks`, BEFORE boundary refinement. Neutral name:
    ///       the doc numbers below (refine=13 physically before fusion=12) are
    ///       left intact; this pass slots in between decode and refine.
    ///   12. BackfillEvidenceFusion + DecisionMapper
    ///   13. BoundaryRefiner
    ///   14. SkipPolicyMatrix + confidence promotion (Phase 6.5: detectOnly for unknown spans; autoSkipEligible at >=0.80)
    ///   15. MetadataExtractor
    ///   16. EvidenceEvent + DecisionEvent logging
    ///   17. Forward eligible results to SkipOrchestrator (Phase 6.5)
    ///
    /// - Parameters:
    ///   - chunks: Final-pass TranscriptChunks (full episode).
    ///   - analysisAssetId: The analysis asset being processed.
    ///   - podcastId: Podcast ID for profile prior updates.
    ///   - episodeDuration: Total episode duration in seconds.
    ///   - sessionId: Optional analysis session id for shadow retry tracking.
    func runBackfill(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double,
        sessionId: String? = nil
    ) async throws {
        self.episodeDuration = episodeDuration
        guard !chunks.isEmpty else { return }

        // ── Steps 1–3: Atomize, segment, build catalog ───────────────────────

        // playhead-hc7e: collapse the mixed fast/final chunk array into ONE
        // canonical transcript BEFORE any consumer reads it. Final-pass chunks
        // REPLACE the fast coverage they overlap; fast chunks REMAIN everywhere
        // the final pass never re-transcribed, so full-episode coverage is
        // retained. The pre-hc7e `filter { pass == "final" }` collapsed the
        // whole timeline to the candidate-local final chunks the moment a
        // single final chunk existed (a candidate-local atom timeline instead
        // of full-episode coverage), while the lexical/catalog/FM consumers
        // below still saw the RAW mixed-pass array and double-counted the
        // overlapping fast+final text. Single-pass transcripts (all-fast or
        // all-final) pass through byte-identically, so this is a no-op for
        // every asset that has never had a final-pass run.
        //
        // ⚠️ playhead-iu0t: this comment used to end "Every consumer now reads
        // `canonicalChunks`." IT WAS NOT TRUE, and its untruth is why the
        // survivors went unfound for months — two separate reviewers had to
        // rediscover them from device data rather than from the source. hc7e's
        // removal was LOCAL to this function; the collapse stood in
        // `retryShadowFMPhaseForSession` (which discarded 2,490 s of a real
        // episode's transcript — see its own comment for the hash that proves
        // it), in `runPhase5ProjectorPhase`, and in
        // `AnalysisCoordinator.pushEvidenceCatalog`.
        //
        // Do not restate that as a survey of call sites — a completeness claim
        // decays silently the moment someone adds a caller. iu0t proved that
        // twice over: its own first draft of THIS paragraph named two survivors
        // and asserted "both are converted now", and R1 found a third still
        // live on the hot path. A prose survey cannot be trusted even when it
        // is written by the person who just went looking.
        //
        // The durable form is a RULE plus something that ENFORCES it. The rule:
        // any path that hands transcript chunks to `TranscriptAtomizer.atomize`
        // or `TranscriptAtomizer.transcriptVersionHash` must canonicalize
        // first, because `transcriptVersion` is derived from the chunks handed
        // in and `BackfillJobRunner`'s job id embeds it — so an uncanonicalized
        // caller does not merely read a narrower transcript, it mints rows in
        // an id space nothing else can find.
        // `SemanticScanClaim.transcriptVersion(forPersistedChunks:)` exists to
        // make following that rule a one-liner for callers starting from raw
        // store rows.
        //
        // The enforcement is `TranscriptCanonicalizationRuleCanaryTests`, which
        // walks every `.swift` file under `Playhead/`, extracts the `chunks:`
        // argument at every call site of those two functions, and fails unless
        // each one either canonicalizes inline or is on an allow-list carrying
        // a written reason. That is what makes the rule survive the next
        // caller: adding one without canonicalizing is a RED TEST, not a
        // paragraph somebody has to re-audit by hand.
        // playhead-r5um: TIME-order the canonical array before anything reads
        // it. `canonicalize` time-sorts the MIXED path but returns single-pass
        // input byte-identically — that passthrough is a deliberate contract
        // (hc7e's no-regression pin, and the transcript-peek display identity
        // pinned by playhead-kcz.1), so the array it hands back is still in
        // `chunkIndex` order for every single-pass asset. 7 of the 10
        // single-pass assets on the 2026-07-30 device pull step BACKWARD in
        // that order, worst −1470.8 s.
        //
        // `atomize` sorts for itself, so the atom lane was already safe. These
        // are the consumers below that read `canonicalChunks` RAW and never
        // atomize: `LexicalAnchorRefiner.buildWordStream`, whose contract is a
        // flat time-ordered word stream and which matches n-grams ACROSS
        // adjacent array positions (so two chunks 1,470 s apart could form a
        // phrase), and the `RegionShadowPhase` input that reaches
        // `MusicOffsetLexicalGate.onsetWindowText`. Sorting once here is
        // idempotent on the mixed path and costs one O(n log n) pass.
        let canonicalization = TranscriptChunkCanonicalizer.canonicalize(chunks)
        let canonicalChunks = canonicalization.chunks
            .sorted(by: TranscriptChunkCanonicalizer.canonicalTimeOrder)
        let canonicalDiagnostics = canonicalization.diagnostics
        logger.info(
            "Backfill canonical transcript: asset=\(analysisAssetId, privacy: .public) input=\(canonicalDiagnostics.inputCount, privacy: .public) final=\(canonicalDiagnostics.finalCount, privacy: .public) fast=\(canonicalDiagnostics.fastCount, privacy: .public) droppedFast=\(canonicalDiagnostics.droppedFastCount, privacy: .public) retainedFast=\(canonicalDiagnostics.retainedFastCount, privacy: .public) residualFastFinalOverlap=\(canonicalDiagnostics.residualFastFinalOverlapCount, privacy: .public) coverageRetained=\(canonicalDiagnostics.coverageRetained, privacy: .public) passthrough=\(canonicalDiagnostics.isPassthrough, privacy: .public)"
        )

        let (atoms, transcriptVersion) = TranscriptAtomizer.atomize(
            chunks: canonicalChunks,
            analysisAssetId: analysisAssetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )

        let evidenceCatalog: EvidenceCatalog
        if !atoms.isEmpty {
            evidenceCatalog = EvidenceCatalogBuilder.build(
                atoms: atoms,
                analysisAssetId: analysisAssetId,
                transcriptVersion: transcriptVersion.transcriptVersion
            )
        } else {
            evidenceCatalog = EvidenceCatalog(
                analysisAssetId: analysisAssetId,
                transcriptVersion: "",
                entries: []
            )
        }

        let (metadataCues, assetChapterEvidence) = await loadEpisodeMetadataSignals(
            analysisAssetId: analysisAssetId
        )

        // playhead-084j / playhead-narl: resolve the 4-level prior hierarchy
        // once per episode before the first metadata consumption point. Lexical
        // injection and the later fusion loop must share the same snapshot so
        // concurrent profile updates cannot change metadata trust mid-run.
        // playhead-2kxd: the request's show, so the priors this episode is
        // fused against belong to the show it is an episode OF.
        let resolvedEpisodePriors = await resolveEpisodePriors(podcastId: podcastId)
        let metadataLexiconEntries = metadataLexiconEntries(
            from: metadataCues,
            metadataTrust: resolvedEpisodePriors.metadataTrust
        )

        // ── Step 4: Lexical scan + RuleBasedClassifier ───────────────────────

        let lexicalCandidates = scanner.scan(
            chunks: canonicalChunks,
            analysisAssetId: analysisAssetId,
            metadataEntries: metadataLexiconEntries
        )

        // playhead-xsdz.1: Collect the raw, time-sorted hit stream ONCE for
        // the whole episode. `LexicalAutoAdEvidenceBuilder` needs hit-level
        // co-occurrence timing + negative-pattern hits, neither of which
        // survives the lossy `LexicalCandidate` merge. Reuses the same
        // metadata-lexicon entries the candidate scan saw so the auto-ad
        // rule sees an identical hit population.
        let lexicalHits = scanner.collectHits(
            chunks: canonicalChunks,
            metadataEntries: metadataLexiconEntries
        )

        logger.info("Backfill: \(lexicalCandidates.count) lexical candidates from \(canonicalChunks.count) canonical chunks")

        let classifierResults: [ClassifierResult]
        if !lexicalCandidates.isEmpty {
            classifierResults = try await classifyCandidates(
                lexicalCandidates,
                analysisAssetId: analysisAssetId,
                podcastId: podcastId
            )
        } else {
            classifierResults = []
        }

        // ── Steps 5–6: CoveragePlanner + FM scanning ─────────────────────────
        // FM scanning: persists SemanticScanResults for downstream ledger
        // construction. Gated by fmBackfillMode; failures are swallowed so
        // they never block the fusion path.

        var fmRefinementWindows: [FMRefinementWindowOutput] = []
        // Cycle 1 H2: gate on effective mode so a known-bad cohort short-circuits
        // before any FM input graph is built.
        //
        // playhead-fil5: both arms below used to drop the scan with nothing but
        // a log line, which is why FCDDB309 could run `runBackfill` TWICE and
        // leave zero `backfill_jobs` rows behind. Each now records a durable
        // claim naming the gate that closed — see `SemanticScanClaim`.
        if effectiveFMBackfillMode == .off {
            await recordSemanticScanClaim(
                gate: .fmModeOff,
                chunks: canonicalChunks,
                analysisAssetId: analysisAssetId,
                podcastId: podcastId
            )
        } else if podcastId.isEmpty {
            logger.info("Backfill: skipping FM scan phase — missing podcastId for asset \(analysisAssetId)")
            // The empty id is handed through VERBATIM rather than pre-converted
            // to nil here. `SemanticScanClaim.claimRow` is the one place that
            // decides an empty id is an absence, and a second copy of that
            // decision at the call site is both a policy that can drift and —
            // while it agrees — one no mutant can kill: the SC09 mutant
            // survived against exactly this shape, because the row never saw
            // the "" the normalization exists to catch.
            await recordSemanticScanClaim(
                gate: .podcastIdMissing,
                chunks: canonicalChunks,
                analysisAssetId: analysisAssetId,
                podcastId: podcastId
            )
        } else {
            let shadowResult = await runShadowFMPhase(
                chunks: canonicalChunks,
                analysisAssetId: analysisAssetId,
                podcastId: podcastId,
                sessionIdOverride: sessionId
            )
            fmRefinementWindows = shadowResult.fmRefinementWindows
        }

        // ── Steps 7–9: Region proposal + feature extraction ──────────────────
        // Runs inline (production, not shadow-only) to produce RegionFeatureBundles.
        // Also feeds the optional regionShadowObserver for diagnostics.

        let featureWindows: [FeatureWindow]
        do {
            featureWindows = episodeDuration > 0
                ? try await store.fetchFeatureWindows(
                    assetId: analysisAssetId,
                    from: 0,
                    to: episodeDuration
                )
                : []
        } catch {
            logger.warning("Backfill: fetchFeatureWindows failed (continuing without acoustic features): \(error.localizedDescription)")
            featureWindows = []
        }

        // playhead-gtt9.16: Run the acoustic feature pipeline over the whole
        // episode once. Each feature has its own rolling-baseline state, so
        // we deliberately run over ALL windows rather than per-span slices.
        // Per-span ledger assembly below filters the resulting WindowFusion
        // entries to the relevant overlap.
        //
        // Empty windows → empty pipeline result, which the ledger helper
        // treats as "nothing to contribute" (no new .acoustic entries emitted).
        let acousticPipelineResult: AcousticFeaturePipeline.Result = featureWindows.isEmpty
            ? AcousticFeaturePipeline.Result(fusion: [], funnel: AcousticFeatureFunnel(), perFeatureScores: [:])
            : AcousticFeaturePipeline.run(windows: featureWindows)
        // Cache for telemetry + test inspection. Logged once at end of
        // `runBackfill` so a single line per episode summarises the funnel
        // rather than spamming per-span.
        self.lastAcousticFunnel = acousticPipelineResult.funnel
        self.lastAcousticPipelineFusion = acousticPipelineResult.fusion

        // playhead-r2vz (PR2): adapt the injected FM recovery dispatcher into
        // the `@Sendable` recovery closure `RegionShadowPhase` awaits per
        // gate-suppressed region. Gated on `canUseFoundationModelsProvider`
        // (the `if let` short-circuits, so a nil dispatcher — preview, and
        // any test that doesn't wire one — skips the await entirely).
        // nil dispatcher ⇒ nil closure ⇒ the partition-and-recover branch is
        // inert (byte-identical to PR1). PlayheadRuntime wires the live
        // dispatcher unconditionally outside preview; the recovery pass still
        // only runs when `config.musicOffsetFMRecoveryEnabled` (below) AND the
        // gate flag are on. markOnly-only is enforced by omission: nothing
        // here stamps an FM origin/evidence onto a restored region.
        //
        // playhead-ncv6: the three coverage-program flags (t1py proposer,
        // eki3 gate, r2vz recovery) are threaded from `AdDetectionConfig`
        // into the `Input` below. All three default to `true` in
        // `AdDetectionConfig.default` and in the config init since Dan's
        // 2026-07-19 Ship Gate 1 enablement (playhead-lq6f) — markOnly-only,
        // no skip behavior change. A caller that wants the pre-ncv6
        // byte-identical path must pass all three explicitly `false`.
        // No Settings UI reads these flags.
        let fmRegionRecoveryClassifier: FMRegionRecoveryClassifier?
        if let dispatcher = fmRegionRecoveryDispatcher, await canUseFoundationModelsProvider() {
            fmRegionRecoveryClassifier = FMRegionRecoveryClassifier { region, atoms in
                await dispatcher.classify(region: region, atoms: atoms)
            }
        } else {
            fmRegionRecoveryClassifier = nil
        }

        let regionInput = RegionShadowPhase.Input(
            analysisAssetId: analysisAssetId,
            chunks: canonicalChunks,
            lexicalCandidates: lexicalCandidates,
            featureWindows: featureWindows,
            episodeDuration: episodeDuration,
            priors: showPriors,
            // playhead-2kxd: THE REQUEST'S show, not "the current one". The
            // profile handed to `RegionShadowPhase` becomes that phase's
            // per-show sponsor lexicon, and `podcastId:` eight lines below is
            // already the request's — the two must name the same show or the
            // phase scans episode A's audio against episode B's sponsors.
            podcastProfile: cachedPodcastProfile(forShowId: podcastId),
            fmWindows: fmRefinementWindows,
            // playhead-shjn: `podcastId` is handed through VERBATIM (empty
            // string included) — `RegionShadowPhase.run` is the single place
            // that decides an empty id means "no podcast identity", the same
            // reasoning `SemanticScanClaim.claimRow` records above. Note this
            // does NOT arm the store-backed FINGERPRINT branch ten lines
            // below it: that branch also requires `fingerprintStore`, which
            // no production call site has ever passed.
            podcastId: podcastId,
            knowledgeStore: sponsorKnowledgeStore,
            classifierResults: classifierResults,
            sustainedMusicProposerEnabled: config.sustainedMusicProposerEnabled,
            musicOffsetLexicalGateEnabled: config.musicOffsetLexicalGateEnabled,
            musicOffsetFMRecoveryEnabled: config.musicOffsetFMRecoveryEnabled,
            fmRegionRecoveryClassifier: fmRegionRecoveryClassifier
        )
        let regionBundles: [RegionFeatureBundle]
        do {
            regionBundles = try await RegionShadowPhase.run(regionInput)
        } catch {
            logger.warning("Backfill: RegionShadowPhase.run failed (continuing without region bundles): \(error.localizedDescription)")
            regionBundles = []
        }

        // Also feed the shadow observer for diagnostics (no-op when nil).
        if let observer = regionShadowObserver, episodeDuration > 0 {
            await observer.record(assetId: analysisAssetId, bundles: regionBundles)
        }

        // ── Steps 10–11: AtomEvidenceProjector + MinimalContiguousSpanDecoder ─

        let projector = AtomEvidenceProjector()
        // playhead-xsdz.34: read-side veto mask. Flag-off / nil-store selects
        // `NoCorrectionMaskProvider()` ⇒ byte-identical to pre-xsdz.34.
        let maskProvider = await Self.makeCorrectionMaskProvider(
            enabled: config.userCorrectionReadSideEnabled,
            store: correctionStore,
            analysisAssetId: analysisAssetId,
            atoms: atoms
        )
        let atomEvidence = await projector.project(
            regions: regionBundles,
            catalog: evidenceCatalog,
            atoms: atoms,
            correctionMaskProvider: maskProvider
        )

        let decoder = MinimalContiguousSpanDecoder()
        // `var` so the post-decode splice-slot ownership pass (playhead-xsdz.20,
        // flag-gated below) can replace this with the rewritten span set before
        // the boundary-refine loop consumes it. Flag OFF leaves it untouched.
        var decodedSpans = decoder.decode(atoms: atomEvidence, assetId: analysisAssetId)

        // Persist decoded spans so TranscriptPeekView can read them.
        if !decodedSpans.isEmpty {
            do {
                try await store.upsertDecodedSpans(decodedSpans)
            } catch {
                logger.warning("Backfill: failed to persist decoded spans: \(error.localizedDescription)")
            }
        }

        // Also feed the Phase 5 shadow observer for diagnostics (no-op when nil).
        if let p5observer = phase5ProjectorObserver, !decodedSpans.isEmpty {
            await p5observer.record(assetId: analysisAssetId, spans: decodedSpans, evidence: atomEvidence)
        }

        logger.info(
            "Backfill: asset=\(analysisAssetId) atoms=\(atoms.count) anchored=\(atomEvidence.filter(\.isAnchored).count) spans=\(decodedSpans.count)"
        )

        // ── Step 11.5: ChapterGenerationPhase (playhead-au2v.1.13) ───────────
        // Wired between final-pass transcript completion (steps 1–11 above
        // produced the atom set, evidence projector, and decoded spans the
        // chapter signal will eventually feed into) and the
        // BackfillEvidenceFusion path that begins at step 12 below.
        //
        // Three gating layers, evaluated in order:
        //   a) `config.chapterSignalMode.runsChapterGeneration` — `.off`
        //      short-circuits BEFORE any factory invocation, so the
        //      `.off` path is byte-for-byte identical to pre-au2v.1.13
        //      behavior (no allocations, no FM cost, no diagnostic
        //      surface). The `chapterSignalMode == .off` default in
        //      `AdDetectionConfig` keeps shipping production silent
        //      until the runtime flag is flipped.
        //   b) `chapterGenerationPhaseFactory` non-nil — a missing
        //      factory means "no production wiring yet". Logged at
        //      `.debug` only; not an error in any mode (lets the
        //      runtime stand up the config flag before the phase
        //      dependencies exist). When the mode is `.shadow` or
        //      `.enabled` and the factory is nil, the phase silently
        //      no-ops — equivalent to mode=.off but observable in the
        //      OS log if a follow-up bead needs to debug the gap.
        //   c) `chapterPlanCache.get(...)` cache hit — when a valid
        //      plan already exists for the current content hash, the
        //      phase is short-circuited and zero FM cost is incurred.
        //      The cache hit emits no diagnostic — the phase's
        //      `chapter_phase_completed` event from the original write
        //      already documented that landing; firing another event
        //      on every re-run would over-count and pollute eval. The
        //      cache short-circuit is also gated on `chapterPlanCache`
        //      being non-nil; without a cache the phase runs every
        //      time and uses its own internal cache write only.
        //
        // Outcome handling: every non-`.cached` outcome is logged but
        // NEVER thrown. The phase already emits its own diagnostics
        // for each terminal state (admission deny, creator-chapter
        // skip, transcript unavailable, race abort, op-rate exceeded,
        // explicit cancellation). Re-emitting them here would
        // duplicate the events. The fusion step that runs immediately
        // after this block is independent of the chapter signal in
        // mode `.shadow` (the consumers only read the plan when
        // `consumersReadChapterPlan == true`, which is `.enabled`-only
        // — a contract deliberately encoded in `ChapterSignalMode`).
        //
        // Cancellation: `runBackfill` is `async throws` and the phase
        // honors `Task.isCancelled` internally, returning `.preempted`
        // on cancel. We additionally re-check `Task.isCancelled` after
        // the phase finishes so a request to cancel that arrived
        // mid-phase still propagates upward — matching the existing
        // `try Task.checkCancellation()` usage elsewhere in the
        // backfill pipeline.
        //
        // Transcript-revision race: the phase captures a transcript
        // hash on entry and re-checks it before its cache write. If
        // the transcript pipeline produces a new version mid-run, the
        // phase's recheck observes the change, discards the plan, and
        // returns `.raceAborted`. We log the abort here at `.notice`
        // so dogfood diagnostic bundles surface a single greppable
        // marker without losing the structured event payload.
        if config.chapterSignalMode.runsChapterGeneration {
            await runChapterGenerationPhaseIfWired(
                analysisAssetId: analysisAssetId,
                transcriptVersion: transcriptVersion.transcriptVersion
            )
            // Honor cancellation requests that landed during the phase.
            try Task.checkCancellation()
        }

        // ── Steps 12–14: Fusion + DecisionMapper + SkipPolicyMatrix ──────────

        // Fetch any persisted FM scan results for this asset to build FM ledger entries.
        let semanticScanResults: [SemanticScanResult]
        do {
            semanticScanResults = try await store.fetchSemanticScanResults(
                analysisAssetId: analysisAssetId
            )
        } catch {
            logger.warning("Backfill: fetchSemanticScanResults failed (no FM evidence): \(error.localizedDescription)")
            semanticScanResults = []
        }

        // playhead-wraj: surface the certainty-tiered auto-skip gate (shipped in
        // PR #237 inside `FusionWeightConfig` / `DecisionMapper`) through
        // production config. Only these three trailing fields are threaded; every
        // other `FusionWeightConfig` field keeps its default, so with the OFF
        // default (`certaintyTieredSkipEnabled == false`) the constructed config
        // is byte-identical to the pre-wraj bare `FusionWeightConfig()` and the
        // post-gate downgrade in `DecisionMapper.map()` never arms. The other
        // three `FusionWeightConfig()` sites (hot-path + Tier-1 + aggregator
        // decision logs) read only `.classifierCap`, so they stay bare.
        let fusionConfig = FusionWeightConfig(
            certaintyTieredEnabled: config.certaintyTieredSkipEnabled,
            hostReadConfidenceFloor: config.hostReadConfidenceFloor,
            postRollGuardSeconds: config.postRollGuardSeconds,
            // playhead-xsdz.62: thread the byte-exact rediff-confirmed KIND flag.
            // OFF by default, so with no config change `buildLedger` emits no
            // `.rediffConfirmed` entry and the fusion output is byte-identical.
            rediffConfirmedKindEnabled: config.rediffConfirmedKindEnabled
        )
        // transcriptQuality is the same for every span (derived from the full atom array),
        // so compute it once outside the loop rather than redundantly per span.
        let transcriptQuality = estimateTranscriptQuality(atoms: atomEvidence)
        // playhead-z3ch: metadata cues were pre-computed before lexical scan so
        // playhead-narl lexical injection and the later fusion ledger consume
        // the same feed-level snapshot. The lookup is feed-level (description
        // + summary) so it has no per-span variance; fanning out the same cues
        // across every span keeps the corroboration gate honest while sharing
        // the extraction cost.
        // playhead-gtt9.22: also pull cached `chapterEvidence` from the
        // metadata provider so the chapter ledger builder can fuse
        // publisher-supplied chapter markers into the metadata channel.
        let metadataEvidenceBuilder = FeedDescriptionEvidenceBuilder()
        let chapterEvidenceBuilder = ChapterMetadataEvidenceBuilder()
        var fusionWindows: [AdWindow] = []
        var decisionEvents: [DecisionEvent] = []
        var decisionLogEntries:
            [(windowId: String, entry: DecisionLogEntry)] = []
        // Phase 6.5 (playhead-4my.16): accumulate AdDecisionResult for step 17 forwarding.
        var fusionDecisionResults: [AdDecisionResult] = []
        // playhead-ud4n: nonterminal hot-path rows this backfill retired
        // (superseded and absent from the authoritative fusion output). Set by
        // the reconcile+persist block below and delivered to the orchestrator in
        // Step 17 BEFORE the replacement decisions (retire-before-replace).
        var retiredHotPathWindowIDs: Set<String> = []
        // playhead-xsdz.10: collect each span's scored decision (post fragility /
        // negative-bank, pre hard-gate) so the temporal-regularization pass can
        // see the whole episode's candidate detections together before the
        // side-effect emission loop below.
        var pendingDecisions: [PendingBackfillDecision] = []

        // Phase 7.2 / playhead-ar60: snapshot this asset's corrections ONCE per
        // backfill (actor-context query), then evaluate the multiplier PER SPAN.
        // Combines passthrough (false-positive suppression, [0.0, 1.0]) and boost
        // (false-negative amplification, [1.0, 2.0]) into a single multiplier.
        // Result: 1.0 = no correction effect; < 1.0 = FP suppression; > 1.0 = FN boost.
        //
        // The query is still once per run — that is what the old "not per span,
        // for performance" note was protecting, and it is unchanged. What moved
        // is the EVALUATION: `CorrectionFactorSnapshot.factor(overlapping:_:)`
        // is pure and runs inside the emission loop, so a span is affected by
        // the corrections that actually reach it. Before ar60 one scalar was
        // computed here and handed to every `DecisionMapper` in the episode, so
        // a veto at 210 s rewrote the confidence of a span 2,600 s away with
        // wholly independent evidence — witnessed on DE0784D8 as an identical
        // `skipConfidence / proposalConfidence` ratio across all five windows
        // of every run, moving 4 orders of magnitude between runs while each
        // window's own `proposalConfidence` stayed bit-identical.
        //
        // Show-wide scopes (a vetoed SPONSOR, a vetoed PHRASE) still apply to
        // every span in the SUPPRESS direction — and to no span in the BOOST
        // direction (playhead-q6y3), because a scope that names a sponsor
        // cannot say where the ads are. See `CorrectionFactorSnapshot`.
        let correctionSnapshot: CorrectionFactorSnapshot
        if let correctionStore {
            correctionSnapshot = await correctionStore
                .correctionFactorSnapshot(for: analysisAssetId)
        } else {
            correctionSnapshot = .identity
        }

        // playhead-gtt9.17: reset per-backfill state for catalog egress so a
        // fresh episode cannot inherit a stale top-similarity from the last
        // one. Empty/absent catalog leaves this at 0 for the whole run.
        lastCatalogMatchSimilarity = 0
        // Capture showId for catalog scoping: null when no podcast was
        // supplied (rare — matches the analogous priors-update guard above).
        let catalogShowId =
            RecurrenceMaterialIdentity.canonicalIdentifier(podcastId)

        // playhead-xsdz.11: resolve the per-show auto-skip threshold OFFSET
        // ONCE per backfill so every span's gate decision uses the same
        // snapshot. The flag-off path skips the lookup entirely (the store is
        // never read), leaving `perShowThresholdOffset == 0` — so the gate adds
        // nothing and behaviour is byte-identical to pre-xsdz.11. The offset is
        // applied (and clamped to [0.55, 0.95]) at the hard auto-skip gate in
        // the emission loop below. The WRITE path that grows this offset from
        // user corrections lives in `SkipOrchestrator`; here we only READ.
        let perShowThresholdOffset = await resolvePerShowThresholdOffset(showId: catalogShowId)

        // playhead-fbsignals.11 fire instrumentation (behavior-neutral): record the
        // per-show OFFSET the controller resolved for this backfill. `nil` in
        // production ⇒ no-op (no record), so this is byte-identical to
        // pre-fbsignals behavior. It NEVER feeds back into the decision; the offset
        // it records is the SAME value the gate below reads. With an EMPTY
        // cold-start controller store this resolved offset is always 0.
        if let perShowThresholdOffsetObserver {
            await perShowThresholdOffsetObserver.recordResolvedOffset(
                assetId: analysisAssetId,
                offset: perShowThresholdOffset
            )
        }

        // playhead-2hpn: resolve the per-show music-bed profile ONCE per
        // backfill so every span sees the same snapshot. The flag-off
        // path skips the lookup entirely (byte-identical to pre-2hpn).
        // The snapshot is consumed by `buildMusicBedLedgerEntries` to
        // decide whether to emit a boosted (0.25) or baseline (0.10)
        // weight entry, and by the post-loop write path to record this
        // episode's outcome. Cross-show isolation is enforced here:
        // a show's snapshot is keyed by `podcastId`, so Show A's
        // confirmation never feeds Show B's evaluation.
        let scopedMusicBedEnabled = preAnalysisConfig.scopedMusicBedGeneralization
        let showMusicBedSnapshot: ShowMusicBedProfileSnapshot?
        if scopedMusicBedEnabled, !podcastId.isEmpty, let store = showMusicBedProfileStore {
            showMusicBedSnapshot = await store.snapshot(showIdentifier: podcastId)
        } else {
            showMusicBedSnapshot = nil
        }

        // playhead-rxuv: snapshot the creator-chapter-fusion flag once
        // at the top of the backfill so every per-span iteration sees a
        // stable value. The flag controls both the evidence builder's
        // `subSource = .creatorChapter` stamp (recall side) and the
        // content-chapter suppression gate (precision side, primary value
        // of the bead). Flag-OFF is the byte-identical pre-rxuv path.
        let creatorChapterFusionEnabled = preAnalysisConfig.creatorChapterFusionEnabled

        // playhead-h6a6 (read path): resolve the per-show capability
        // profile ONCE per backfill so every consumer sees the same
        // adjustment snapshot. Flag-off, missing-store, missing-podcastId,
        // and "no profile observed yet" all short-circuit to the
        // `.unknown`-yielded baseline (multiplier 1.0, empty bias map)
        // — the no-modulation no-op contract. The adjustment is
        // exposed via `lastCapabilityBudgetAdjustmentForTesting()` so
        // the behavioral contract (modulation applied iff flag-ON +
        // profile observed) can be asserted directly.
        let capabilityProfilesEnabled = preAnalysisConfig.showCapabilityProfilesEnabled
        let capabilitySnapshot: ShowCapabilityProfileSnapshot?
        if capabilityProfilesEnabled,
           !podcastId.isEmpty,
           let store = showCapabilityProfileStore {
            capabilitySnapshot = await store.snapshot(showIdentifier: podcastId)
        } else {
            capabilitySnapshot = nil
        }
        // Resolve the budget adjustment. A `nil` snapshot OR a
        // snapshot whose kind is `.unknown` BOTH yield the
        // no-modulation baseline — there is no other path that
        // produces a non-unity multiplier when the flag is off, and
        // pinning the kind to `.unknown` (by an out-of-bounds SLI
        // gate, or a sub-floor episode count) deterministically
        // disables modulation even when the flag is on. The
        // `.unknown` no-op contract is pinned in
        // `ShowCapabilityBudgetModulatorTests.unknownIsBaseline`.
        let resolvedKind: ShowCapabilityProfileKind =
            capabilitySnapshot?.kind ?? .unknown
        lastCapabilityBudgetAdjustment = ShowCapabilityBudgetModulator.adjustment(
            for: resolvedKind
        )
        if capabilityProfilesEnabled, resolvedKind != .unknown {
            logger.info(
                "[h6a6] show=\(podcastId, privacy: .public) profile=\(resolvedKind.rawValue, privacy: .public) multiplier=\(self.lastCapabilityBudgetAdjustment.analysisBudgetMultiplier)"
            )
        }

        // playhead-arf8: reset per-backfill bracket-refinement counts and
        // resolve the per-show music-bracket trust once per run. The store
        // backs onto the same `AnalysisStore` as everything else; lookup is
        // O(1) after the first hit per show. Empty `podcastId` (rare; only
        // when the caller never supplied a podcast) skips the lookup and
        // uses the default prior mean (0.50) so refinement can still apply
        // for the duration of the run — matches the conservative default
        // configured in `AdDetectionConfig`.
        lastBracketRefinementCounts = BracketRefinementCounts()
        // playhead-p56a: reset both finalizer constraint-trace maps so a
        // prior run's entries cannot bleed into this one. The OFF path
        // (`config.spanFinalizerEnabled == false`) leaves them at empty for
        // the rest of the run — preserves the byte-identical OFF contract.
        lastSpanFinalizerConstraintsBySpanId = [:]
        lastSpanFinalizerConstraintsByWindowId = [:]
        // playhead-l2f.6: same reset contract for the stinger trace maps.
        // The OFF path (`config.stingerRefinementEnabled == false`) leaves
        // them empty for the rest of the run.
        lastStingerRefinementTraceBySpanId = [:]
        lastStingerRefinementTraceByWindowId = [:]
        // playhead-xsdz.37: same reset contract for the lexical trace maps.
        // The OFF path (`config.lexicalAnchorRefinementEnabled == false`)
        // leaves them empty for the rest of the run.
        lastLexicalRefinementTraceBySpanId = [:]
        lastLexicalRefinementTraceByWindowId = [:]
        // playhead-xsdz.36.1.1: reset the observability-only evidence
        // provenance map so a prior run's entries cannot bleed into this one.
        // Unlike the trace maps above it is repopulated unconditionally in the
        // emission loop (the provenance + ledger always exist).
        lastEvidenceProvenanceByWindowId = [:]
        // playhead-l2f.6: resolve the per-show stinger context ONCE per run
        // (bank entry + episode identity for shard-cache PCM). Flag OFF ⇒
        // nil without touching the bank, the store, or the filesystem —
        // byte-identical to pre-l2f.6.
        let stingerContext = await resolveStingerRefinementContext(
            analysisAssetId: analysisAssetId,
            showKey: catalogShowId
        )
        // playhead-xsdz.37: resolve the per-show lexical context ONCE per run
        // (bank entry ⇒ effective anchor set). Flag OFF (or unknown show) ⇒
        // nil without touching the bank — byte-identical to pre-xsdz.37. The
        // word stream is built lazily only when a context resolved, so the OFF
        // path never scans the transcript for anchors. It is built from
        // `canonicalChunks` (the same de-duplicated stream the atomizer and the
        // transcript boundary-cue builder consume — playhead-hc7e) so a
        // fast-pass duplicate of the same audio cannot double-count candidates
        // or bind the snap to a fast-pass phrase interpolation.
        let lexicalContext = resolveLexicalRefinementContext(showKey: catalogShowId)
        let lexicalWords: [LexicalWord] = lexicalContext == nil
            ? []
            : LexicalAnchorRefiner.buildWordStream(chunks: canonicalChunks)
        // playhead-fl4j: resolve the show-agnostic self-promo bank ONCE per run.
        // Flag OFF ⇒ nil without touching the bank (byte-identical to pre-fl4j).
        // The word stream is built lazily only when the bank resolved, so the
        // OFF path never scans the transcript for self-promo phrases. It is the
        // SAME `LexicalWord` stream the lexical refiner consumes (built from
        // `canonicalChunks`); it lives on its own gate so self-promo suppression
        // is independent of `lexicalAnchorRefinementEnabled` — the only cost of
        // both flags being ON at once is one extra pure word-stream build, never
        // in production (both default OFF).
        let selfPromoBank = selfPromoSuppressionBankIfEnabled()
        let selfPromoWords: [LexicalWord] = selfPromoBank == nil
            ? []
            : LexicalAnchorRefiner.buildWordStream(chunks: canonicalChunks)
        // playhead-fl4j (attention→verification): resolve the show's OWN identity
        // tokens ONCE per run so an AMBIGUOUS self-promo phrase ("get tickets",
        // "on tour") can be corroborated by the show naming itself, not only by a
        // first-person pronoun. Sourced from the cached podcast profile (title +
        // network) the service already holds; generic filler tokens are dropped
        // inside `SelfPromoShowIdentity`. Gated on `selfPromoBank == nil` (same as
        // the word stream) so the flag-OFF path builds nothing — byte-identical
        // to pre-fl4j.
        //
        // playhead-2kxd: "the show's OWN identity" is THIS REQUEST'S show. The
        // whole point of the tokens is to let the episode be recognised naming
        // ITSELF; sourced from a slot they could name the previous episode's
        // show, and a self-promo phrase would then be corroborated by a title
        // this episode never says — or, worse, fail to be corroborated by the
        // one it does. Resolved inside the `else` so the flag-OFF path still
        // touches nothing.
        let selfPromoShowIdentity: SelfPromoShowIdentity
        if selfPromoBank == nil {
            selfPromoShowIdentity = .none
        } else {
            let selfPromoShowProfile = cachedPodcastProfile(forShowId: podcastId)
            selfPromoShowIdentity = SelfPromoShowIdentity(
                title: selfPromoShowProfile?.title,
                networkId: selfPromoShowProfile?.networkId
            )
        }
        let bracketShowTrust: Double
        if config.bracketRefinementEnabled, !podcastId.isEmpty {
            let trustStore = bracketTrustStoreLazy()
            bracketShowTrust = await trustStore.trust(forShow: podcastId)
        } else {
            bracketShowTrust = 0.5
        }

        // playhead-kgby: build sentence-terminal hits from the final-pass
        // chunks once per run. We pass these into `BoundaryRefiner` so the
        // resolver can score boundary candidates near sentence ends. When
        // the master flag is off, or no chunks produce a hit (sparse or
        // unpunctuated transcript — the dominant failure mode for
        // conversational shows like Conan), the array is empty and the
        // refiner uses its legacy 90/10 weight schedule. This is the
        // graceful degradation path: when the transcript carries no
        // useful signal, the cue contributes 0 and acoustic snapping is
        // unchanged.
        let transcriptBoundaryHits: [TranscriptBoundaryHit]
        if config.transcriptBoundaryCueEnabled {
            transcriptBoundaryHits = TranscriptBoundaryCueBuilder.buildHits(
                from: canonicalChunks
            )
        } else {
            transcriptBoundaryHits = []
        }
        if !transcriptBoundaryHits.isEmpty {
            logger.info("[kgby] backfill transcript boundary hits: \(transcriptBoundaryHits.count) (from \(canonicalChunks.count) canonical chunks)")
        }

        // Bug 6 (decision-results wiring): hoist the per-asset DecisionCohort encoding
        // out of the per-window loop. The cohort is a function of detectorVersion only,
        // so it is identical for every window in this run; encoding it once avoids
        // repeated JSON work and gives Step 16.5 a single canonical value to persist
        // in `ad_decision_results.decisionCohortJSON`.
        let assetDecisionCohort = DecisionCohort.production(appBuild: config.detectorVersion)
        let assetCohortJSON = (try? JSONEncoder().encode(assetDecisionCohort))
            .flatMap { String(data: $0, encoding: .utf8) } ?? "{}"

        // playhead-084j: reuse the 4-level prior hierarchy resolved once per episode.
        // Up to this bead, `DurationPrior.standard` was hard-coded inside the
        // per-span fusion loop, so every show ran with the global default
        // typicalAdDuration of 30...90s. As of playhead-spxs all four tiers —
        // global + network + trait + show-local — are resolved once and fed
        // into `DurationPrior(resolvedPriors:)`. See the audit block on
        // `resolveEpisodePriors(...)` for the current per-tier source-of-truth
        // and which axes each tier is load-bearing on.
        //
        // Why-it-lives-here (cycle-1 M1): the hoist out of the per-span
        // loop is primarily a SNAPSHOT-CONSISTENCY guarantee, not a
        // performance optimization. Every span in this episode must see
        // the SAME resolved priors — even if a sibling task were to
        // mutate `PodcastProfile.adDurationStatsJSON` mid-run (e.g.
        // another episode's backfill completing concurrently), the
        // per-span DecisionMapper inputs would otherwise drift partway
        // through the fusion loop and produce non-deterministic decision
        // boundaries within a single episode. The arithmetic cost
        // (a few blends, well under a millisecond) is genuinely
        // negligible; the shape is locked by the wire-up tests so the
        // consistency invariant can't regress silently.
        let episodeDurationPrior = DurationPrior(resolvedPriors: resolvedEpisodePriors)

        // playhead-fqc8: detect acoustic breaks once for the whole asset
        // so the per-span `buildEvidenceLedger` can emit a `.breakAlignment`
        // ledger entry for `.classifierSeed`-anchored spans whose edges
        // line up with a strong break. Pure computation over the same
        // `featureWindows` array; safe to hoist out of the loop.
        let assetAcousticBreaks: [AcousticBreak] = featureWindows.isEmpty
            ? []
            : AcousticBreakDetector.detectBreaks(in: featureWindows)

        // playhead-xsdz.13: cross-show syndication WRITE + denominator hoist.
        // Gated by the OFF-by-default `crossShowSyndicationEnabled` flag AND a
        // wired store AND a known show; when any is absent, NO store read/write
        // happens and the decision path is byte-identical to pre-xsdz.13.
        //
        // WRITE: record THIS episode's distinct sponsor ENTITIES (brand spans
        // extracted by `EvidenceCatalogBuilder`, normalized with the same
        // `normalize` that backs `SponsorKnowledgeEntry.normalizedValue`) keyed
        // by show. Done ONCE per backfill (not per span) so an entity that
        // appears in several spans is recorded as ONE observation for this
        // episode — the cross-show signal counts DISTINCT shows, not span
        // multiplicity. Only entities above the min-write confidence are
        // recorded, so a weak one-off brand-span guess never pollutes the
        // aggregate.
        //
        // DENOMINATOR: resolve the library's distinct-observed-show count ONCE
        // AFTER the write so every span's read in this backfill shares the same
        // stable denominator (deterministic, no within-loop drift).
        var crossShowTotalObservedShows = 0
        if config.crossShowSyndicationEnabled,
           let syndicationStore = crossShowSyndicationStore,
           let show = catalogShowId {
            let sponsorEntities = Self.crossShowSponsorObservations(
                from: evidenceCatalog.entries
            )
            for (normalizedEntity, confidence) in sponsorEntities
            where confidence >= Self.crossShowSyndicationMinWriteConfidence {
                do {
                    try await syndicationStore.recordObservation(
                        normalizedEntity: normalizedEntity,
                        podcastId: show,
                        confidence: confidence
                    )
                } catch {
                    logger.debug("crossShowSyndication: recordObservation failed (non-fatal): \(String(describing: error), privacy: .public)")
                }
            }
            crossShowTotalObservedShows = await syndicationStore.totalObservedShowCount()
        }

        // ── Post-decode SPLICE-SLOT OWNERSHIP pass (playhead-xsdz.20) ─────────
        // Placed after decode (11) and after `assetAcousticBreaks`, BEFORE the
        // per-span boundary-refine loop below (numbered 13 in the doc-comment).
        // Slot ownership is PER-SPAN and POST-DECODE, NOT a decode-wide decoder
        // mode — `decoder.decode(...)` above stays `.legacyEvidence`. When a
        // qualifying acoustic-splice pair encloses a span's presence core, the
        // SLOT owns the span's WIDTH; transcript/FM keep owning PRESENCE. The
        // pass resolves every span's would-be slot, awaits the negative-bank
        // verdict table ONCE, runs the pure `SpliceSlotDispositionEngine`
        // (passes 2–4), then (flag-ON) rewrites the kept slots, re-persists, and
        // hands the rewritten set to the loop. Flag OFF ⇒ never invoked, output
        // byte-identical.
        if config.spliceSlotOwnershipEnabled {
            decodedSpans = await applySpliceSlotOwnershipPass(
                decodedSpans: decodedSpans,
                atoms: atoms,
                atomEvidence: atomEvidence,
                featureWindows: featureWindows,
                acousticBreaks: assetAcousticBreaks,
                analysisAssetId: analysisAssetId,
                showId: catalogShowId
            )
        } else if config.spliceSlotShadowEnabled {
            // playhead-xsdz.21 (Bead C): ownership OFF + shadow ON. Compute the
            // would-be dispositions and emit breadcrumbs WITHOUT applying them.
            // Guarded by the `else` so both-flags-ON ⇒ shadow silent (the
            // ownership pass is the sole owner of the disposition side effects).
            await runSpliceSlotShadowPass(
                decodedSpans: decodedSpans,
                atoms: atoms,
                atomEvidence: atomEvidence,
                featureWindows: featureWindows,
                acousticBreaks: assetAcousticBreaks,
                analysisAssetId: analysisAssetId,
                showId: catalogShowId
            )
        }

        // ── Post-decode REDIFF-SLOT OWNERSHIP pass (playhead-xsdz.29) ─────────
        // The double-fetch width oracle: the SOLE production width setter
        // (contract 2026-07-07). Independent of the acoustic block above (which
        // is mutually-exclusive by contract) — it re-diffs the stored played-copy
        // (A) against a re-fetched B-side and hands the played-timeline DAI slots
        // to the SAME `SpliceSlotDispositionEngine`, rewriting kept slots with
        // `.rediffSlot` provenance. Both branches no-op unless a
        // `RediffBSideProvider` is injected AND the stored A-side + a fresh
        // B-side exist, so with the flags OFF (or no provider — the production
        // case today) the pipeline output is byte-identical.
        if config.rediffSlotOwnershipEnabled {
            decodedSpans = await applyRediffSlotOwnershipPass(
                decodedSpans: decodedSpans,
                atoms: atoms,
                atomEvidence: atomEvidence,
                analysisAssetId: analysisAssetId,
                showId: catalogShowId
            )
        } else if config.rediffSlotShadowEnabled {
            await runRediffSlotShadowPass(
                decodedSpans: decodedSpans,
                atoms: atoms,
                atomEvidence: atomEvidence,
                analysisAssetId: analysisAssetId,
                showId: catalogShowId
            )
        }

        for span in decodedSpans {
            try Task.checkCancellation()

            // Step 13 (moved before fusion): snap span boundaries to acoustic transitions
            // so that the evidence lookup and gate decision use the final refined boundaries.
            //
            // playhead-arf8: try the bracket-aware refiner first. If it
            // successfully refines (path == .bracketRefined) we use its
            // adjustments; otherwise we fall back to the legacy
            // `BoundaryRefiner.computeAdjustments` so the bead's
            // "scored cue, not an override" contract holds — the bracket
            // path is additive when it has high-confidence evidence and
            // a no-op everywhere else.
            //
            // playhead-kgby: when the transcript-aware cue is enabled and
            // we have transcript hits, the legacy `BoundaryRefiner` runs
            // with the transcript-aware weight schedule (transcriptBoundary
            // weight 0.20, pauseVAD 0.70). The bracket-refined path is
            // unaffected — it uses `FineBoundaryRefiner` which has its
            // own snap logic. So this bead is purely additive within the
            // legacy fallback path; the bracket cascade is unchanged.
            let (startAdj, endAdj): (Double, Double)
            if span.anchorProvenance.contains(where: { $0.isWidthOwnership }) {
                // playhead-xsdz.20 / xsdz.29: WIDTH-owned spans (acoustic splice OR
                // rediff) bypass BOTH the bracket-aware refiner AND the legacy
                // BoundaryRefiner — the ±3s snap clamps would blur the physical
                // splice the slot pass just locked to (rediff is the SOLE width
                // setter, so its fingerprint-diff edges must not be nudged either).
                // Non-slot spans keep today's exact refinement path.
                startAdj = 0.0
                endAdj = 0.0
            } else if featureWindows.isEmpty {
                startAdj = 0.0
                endAdj = 0.0
            } else {
                let bracketResult = BracketAwareBoundaryRefiner.computeAdjustments(
                    windows: featureWindows,
                    candidateStart: span.startTime,
                    candidateEnd: span.endTime,
                    showTrust: bracketShowTrust,
                    config: config
                )
                tallyBracketRefinementOutcome(bracketResult.path)
                if case .bracketRefined = bracketResult.path {
                    startAdj = bracketResult.startAdjust
                    endAdj = bracketResult.endAdjust
                } else {
                    let legacy = BoundaryRefiner.computeAdjustments(
                        windows: featureWindows,
                        candidateStart: span.startTime,
                        candidateEnd: span.endTime,
                        transcriptHits: transcriptBoundaryHits
                    )
                    startAdj = legacy.startAdjust
                    endAdj = legacy.endAdjust

                    // playhead-kgby: per-span dogfood marker. The [kgby]
                    // backfill-summary line tells us if the cue *built*
                    // hits at all; this line tells us if any hit was
                    // close enough to a candidate boundary to actually
                    // influence the snap. Radius matches the resolver's
                    // production default (1.5s).
                    if !transcriptBoundaryHits.isEmpty {
                        let radius = 1.5
                        let nearStart = transcriptBoundaryHits.contains { abs($0.time - span.startTime) <= radius }
                        let nearEnd = transcriptBoundaryHits.contains { abs($0.time - span.endTime) <= radius }
                        if nearStart || nearEnd {
                            logger.info("[kgby] legacy span snap: spanId=\(span.id) startAdj=\(String(format: "%.2f", startAdj)) endAdj=\(String(format: "%.2f", endAdj)) hitsNearStart=\(nearStart) hitsNearEnd=\(nearEnd)")
                        }
                    }
                }
            }
            var refinedSpan = DecodedSpan(
                id: span.id,
                assetId: span.assetId,
                firstAtomOrdinal: span.firstAtomOrdinal,
                lastAtomOrdinal: span.lastAtomOrdinal,
                startTime: span.startTime + startAdj,
                endTime: span.endTime + endAdj,
                anchorProvenance: span.anchorProvenance
            )

            // playhead-l2f.6: stinger-anchored refinement, applied on top
            // of the acoustic snap above so fusion and persistence see the
            // stinger-refined bounds. Flag OFF ⇒ `stingerContext` is nil
            // and this is a zero-cost no-op (byte-identical). WIDTH-owned
            // spans (acoustic splice / rediff) are exempt for the same
            // reason they bypass the bracket/legacy refiners above: the
            // slot pass locked their physical edges and no snap may blur
            // them.
            if let stingerContext,
               !span.anchorProvenance.contains(where: { $0.isWidthOwnership }) {
                refinedSpan = await applyStingerRefinement(
                    span: refinedSpan,
                    context: stingerContext,
                    episodeDuration: episodeDuration
                )
            }

            // playhead-xsdz.37: lexical-anchor refinement, applied AFTER the
            // stinger snap so a high-precision EXACT lexical match takes
            // precedence on the specific edge it fires; edges the lexical
            // refiner does not fire on keep the prior stinger/acoustic result
            // (the refiner leaves un-matched edges untouched). Flag OFF ⇒
            // `lexicalContext` is nil and this is a zero-cost no-op
            // (byte-identical). WIDTH-owned spans (acoustic splice / rediff)
            // are exempt for the same reason as the stinger block: the slot
            // pass locked their physical edges and no snap may blur them.
            if let lexicalContext,
               !span.anchorProvenance.contains(where: { $0.isWidthOwnership }) {
                refinedSpan = applyLexicalRefinement(
                    span: refinedSpan,
                    context: lexicalContext,
                    words: lexicalWords,
                    episodeDuration: episodeDuration
                )
            }

            // playhead-z3ch: build per-span metadata entries from the cached cues.
            // Builder is pure; the heavy work (cue extraction) was done once above.
            var metadataEntries = metadataEvidenceBuilder.buildEntries(
                cues: metadataCues,
                for: refinedSpan
            )

            // playhead-gtt9.22: fuse chapter-derived evidence onto the
            // metadata channel for spans whose interval overlaps a
            // publisher-labeled "Sponsor"/"Ad break" chapter. The
            // builder emits at most one entry per span; the entry is
            // hard-clamped to `metadataCap` (0.15) by the same
            // `FusionBudgetClamp` that guards description/summary cues
            // — chapters cannot exceed the metadata family budget, and
            // the corroboration gate still requires an in-audio signal
            // before the metadata family can trigger a skip.
            //
            // playhead-rxuv: thread the creator-chapter-fusion flag so
            // entries from creator-source chapters carry the
            // `EvidenceSubSource.creatorChapter` tag when the flag is on.
            // Flag-OFF keeps the byte-identical pre-rxuv output (no
            // subSource on the emitted entry).
            let chapterMetadataEntries = chapterEvidenceBuilder.buildEntries(
                chapters: assetChapterEvidence,
                for: refinedSpan,
                tagCreatorChapterSubSource: creatorChapterFusionEnabled
            )
            metadataEntries.append(contentsOf: chapterMetadataEntries)

            // playhead-gtt9.17: catalog egress. Fingerprint the span's feature
            // windows (time-invariant) and query `AdCatalogStore` for known
            // entries that match above the default similarity floor. The top
            // similarity is retained in the evidence ledger and persisted
            // provenance for diagnostics, and enters
            // `AutoSkipPrecisionGateInput.catalogMatchSimilarity` as a
            // suggest-only signal. `DecisionMapper` excludes this learned
            // fingerprint-store row from scoring and quorum. Zero when no
            // valid query finds a match. Query-unavailable paths remain nil in
            // persisted telemetry rather than masquerading as observed misses.
            let spanCatalogEvidence =
                await Self.resolveCatalogSpanEvidence(
                    span: refinedSpan,
                    featureWindows: featureWindows,
                    catalogStore: adCatalogStore,
                    showId: catalogShowId
                )
            let spanTopCatalogSimilarity =
                spanCatalogEvidence.topSimilarity
            let spanTopCatalogMatch = spanCatalogEvidence.topMatch
            let spanCatalogWasEvaluated =
                spanCatalogEvidence.wasEvaluated
            if spanTopCatalogSimilarity > lastCatalogMatchSimilarity {
                lastCatalogMatchSimilarity = spanTopCatalogSimilarity
            }

            // playhead-xsdz.9: cross-episode "memory" reads. Gated by the
            // OFF-by-default `crossEpisodeMemoryEnabled` flag AND a wired bank /
            // store; when the flag is off OR neither is wired, NO bank read
            // happens and the candidate token sequence is never computed —
            // keeping the decision path byte-identical to pre-xsdz.9.
            //
            //   • Negative bank → `negativeMatch` (drives post-fusion
            //     suppression after the fragility penalty below).
            //   • Positive bank → `crossEpisodePositiveEntries` (a capped
            //     `.crossEpisodeMemory` ledger boost, built in
            //     `buildEvidenceLedger`).
            var negativeMatch: NegativeFingerprintMatch?
            var crossEpisodePositiveEntries: [EvidenceLedgerEntry] = []
            if config.crossEpisodeMemoryEnabled,
               negativeFingerprintBank != nil || adCopyFingerprintStore != nil {
                let spanAtomText = atoms
                    .filter {
                        $0.atomKey.atomOrdinal >= refinedSpan.firstAtomOrdinal
                            && $0.atomKey.atomOrdinal <= refinedSpan.lastAtomOrdinal
                    }
                    .sorted { $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal }
                    .map(\.text)
                    .joined(separator: " ")
                // Clamp the candidate to the same `maxTokenCount` ceiling the
                // bank applies to stored sequences, so BOTH the negative
                // (`bestMatch`) and positive (`buildPositiveBoostEntries`)
                // alignments are bounded at O(maxTokenCount × stored-len). The
                // negative bank clamps internally regardless; doing it once here
                // keeps the positive path (whose evaluator does not clamp)
                // equally bounded.
                let rawCandidateTokens = SmithWatermanAligner.tokenize(spanAtomText)
                let candidateTokens = rawCandidateTokens.count > NegativeFingerprintBank.maxTokenCount
                    ? Array(rawCandidateTokens.prefix(NegativeFingerprintBank.maxTokenCount))
                    : rawCandidateTokens
                if !candidateTokens.isEmpty {
                    if let bank = negativeFingerprintBank {
                        negativeMatch = await bank.bestMatch(
                            candidateTokens: candidateTokens,
                            show: catalogShowId
                        )
                    }
                    if let store = adCopyFingerprintStore {
                        let positiveSequences = await loadConfirmedAdSequences(
                            store: store,
                            podcastId: catalogShowId
                        )
                        crossEpisodePositiveEntries = crossEpisodeMemoryEvaluator
                            .buildPositiveBoostEntries(
                                candidateTokens: candidateTokens,
                                positiveSequences: positiveSequences,
                                cap: fusionConfig.crossEpisodeMemoryCap
                            )
                    }
                }
            }

            // playhead-xsdz.13: cross-show syndication READ. Gated by the same
            // OFF-by-default `crossShowSyndicationEnabled` flag AND a wired store
            // AND a known show; when the flag is off OR the store is nil, NO read
            // happens and `crossShowSyndicationEntries` stays empty — so the
            // ledger is byte-identical to pre-xsdz.13. For each NORMALIZED sponsor
            // entity overlapping this span, look up its cross-show spread profile
            // (using the denominator hoisted once above) and let the pure
            // evaluator emit a capped boost entry IFF the entity has high spread
            // AND temporal persistence. The single strongest qualifying entry is
            // kept so the channel contributes at most one entry per span (one
            // kind, one cap).
            var crossShowSyndicationEntries: [EvidenceLedgerEntry] = []
            if config.crossShowSyndicationEnabled,
               let syndicationStore = crossShowSyndicationStore,
               catalogShowId != nil {
                let spanEntities = Self.crossShowSponsorEntities(
                    from: evidenceCatalog.entries,
                    overlapping: refinedSpan
                )
                var best: EvidenceLedgerEntry?
                for entity in spanEntities {
                    guard let profile = await syndicationStore.spreadProfile(
                        forEntity: entity,
                        totalObservedShows: crossShowTotalObservedShows
                    ) else { continue }
                    let entries = crossShowSyndicationEvaluator.buildBoostEntries(
                        profile: profile,
                        cap: fusionConfig.crossShowSyndicationCap
                    )
                    if let candidate = entries.first,
                       candidate.weight > (best?.weight ?? -1) {
                        best = candidate
                    }
                }
                if let best { crossShowSyndicationEntries = [best] }
            }

            // playhead-xsdz.12: assemble the span's joined transcript prose for
            // the rhetorical act-sequence grammar detector. Gated on the
            // OFF-by-default `rhetoricalGrammarEnabled` flag: with the flag off
            // this stays "" and the detector is never called, so the decision
            // path is byte-identical to pre-xsdz.12 and no per-span text join
            // occurs. Built from the same ordinal-filtered atom slice the
            // xsdz.9 cross-episode read uses.
            let rhetoricalGrammarSpanText: String = config.rhetoricalGrammarEnabled
                ? atoms
                    .filter {
                        $0.atomKey.atomOrdinal >= refinedSpan.firstAtomOrdinal
                            && $0.atomKey.atomOrdinal <= refinedSpan.lastAtomOrdinal
                    }
                    .sorted { $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal }
                    .map(\.text)
                    .joined(separator: " ")
                : ""

            let ledger = buildEvidenceLedger(
                span: refinedSpan,
                classifierResults: classifierResults,
                lexicalCandidates: lexicalCandidates,
                lexicalHits: lexicalHits,
                featureWindows: featureWindows,
                catalogEntries: evidenceCatalog.entries,
                semanticScanResults: semanticScanResults,
                metadataEntries: metadataEntries,
                acousticPipelineFusion: acousticPipelineResult.fusion,
                acousticBreaks: assetAcousticBreaks,
                catalogMatchSimilarity: spanTopCatalogSimilarity,
                fusionConfig: fusionConfig,
                // playhead-2hpn: thread the per-show snapshot resolved
                // once above; `nil` when the flag is off, the podcast
                // is unknown, or no profile exists yet for this show.
                showMusicBedSnapshot: showMusicBedSnapshot,
                // playhead-xsdz.9: positive cross-episode boost entries built
                // above. Empty for the flag-OFF / no-store / non-firing path.
                crossEpisodeMemoryEntries: crossEpisodePositiveEntries,
                // playhead-xsdz.12: span prose for the grammar detector; "" when
                // the OFF-by-default flag is off, so behaviour is byte-identical.
                spanText: rhetoricalGrammarSpanText,
                // playhead-xsdz.13: cross-show syndication boost entries built
                // above. Empty for the flag-OFF / no-store / non-firing path, so
                // the ledger is byte-identical to pre-xsdz.13.
                crossShowSyndicationEntries: crossShowSyndicationEntries,
                episodeDuration: episodeDuration
            )

            // playhead-brandab fire instrumentation (behavior-neutral): tally
            // whether the two brand-appearance channels emitted an entry for this
            // span, reading the SAME pre-suppression `ledger` the decision builds
            // from. `nil` in production ⇒ no-op (no tally), so this is
            // byte-identical to pre-brandab behavior. It NEVER feeds back into the
            // decision; it only records what the channels already produced.
            if let brandAppearanceChannelTapObserver {
                await brandAppearanceChannelTapObserver.record(
                    assetId: analysisAssetId,
                    ledger: ledger
                )
            }

            // Phase ef2.4.6: FM suppression — targeted downweight of weak evidence
            // when FM strongly says noAds with consensus. Applied after ledger build
            // but before DecisionMapper, preserving strong positive anchors.
            let suppressionResult = applyFMSuppression(
                span: refinedSpan,
                ledger: ledger,
                semanticScanResults: semanticScanResults
            )
            let effectiveLedger = suppressionResult.suppressedLedger

            let mapper = DecisionMapper(
                span: refinedSpan,
                ledger: effectiveLedger,
                config: fusionConfig,
                transcriptQuality: transcriptQuality,
                // playhead-ar60: THIS span's corrections, not the episode's.
                correctionFactor: correctionSnapshot.factor(
                    overlapping: refinedSpan.startTime,
                    refinedSpan.endTime
                ),
                // playhead-084j: Consume the typical-ad-duration prior as a soft
                // monotonic multiplier, derived from the per-episode resolved
                // priors (global + trait + show-local). Resolution happened
                // once outside the per-span loop above; this is a struct-by-
                // value pass, no recomputation.
                durationPrior: episodeDurationPrior,
                // playhead-wraj (post-roll guard): thread `runBackfill`'s
                // episode duration so the certainty-tiered post-roll demotion
                // can see the episode end. The service's sentinel for "unknown"
                // is `0`; the mapper's contract is `nil` = unknown (guard
                // inert — never guess the episode end), so normalize here.
                episodeDuration: episodeDuration > 0 ? episodeDuration : nil
            )
            let rawDecision = mapper.map()

            // If FM suppression capped to markOnly, override the gate.
            // playhead-fqc8: preserve `promotionTrack` from the raw mapper
            // output; the FM-suppression cap only changes the eligibility
            // gate, not the threshold-selection track.
            //
            // playhead-qs0d: byte-exact rediff certainty is EXEMPT from the CAP,
            // the third member of the pzy2 exemption family (creator-chapter and
            // self-promo are below). `.blockedByFMConsensus` is by its own
            // definition "FM noAds consensus suppression" — a NON-deterministic
            // model's opinion that there is no ad here. A span whose width the
            // rediff differ owns (`.rediffSlot`) is a byte-verified DAI
            // divergence: the origin served different bytes over exactly this
            // region. Letting an FM noAds consensus force that to mark-only is
            // the FM-coin-flip blocker this bead names, and it is precisely
            // backwards — the deterministic observation outranks the model.
            //
            // Deliberately NARROW, and note what it does NOT do:
            //   • It only declines to APPLY a demotion; it never promotes. The
            //     span keeps whatever gate `DecisionMapper` gave it, so a
            //     genuinely weak rediff-owned span can still land
            //     `.blockedByEvidenceQuorum`.
            //   • It does not touch `effectiveLedger`. FM suppression still
            //     downweights the ledger, so `skipConfidence` stays exactly as
            //     honest (and as suppressed) as before — only ACTIONABILITY
            //     moves, the same presence/extent separation playhead-2350
            //     enforces.
            //   • Splice-AGNOSTIC: `.spliceSlot` is acoustic width, not
            //     byte-exact, and is NOT exempt. FM/lexical-only spans stay
            //     fully cappable, so the exemption cannot leak.
            var decision: DecisionResult
            if suppressionResult.cappedToMarkOnly,
               refinedSpan.carriesRediffByteExactWidth {
                logger.info(
                    "[qs0d] FM-suppression cap DECLINED for byte-exact rediff span=\(refinedSpan.id, privacy: .public) — deterministic divergence outranks an FM noAds consensus"
                )
                decision = rawDecision
            } else if suppressionResult.cappedToMarkOnly {
                decision = DecisionResult(
                    proposalConfidence: rawDecision.proposalConfidence,
                    skipConfidence: rawDecision.skipConfidence,
                    eligibilityGate: .blockedByFMConsensus,
                    promotionTrack: rawDecision.promotionTrack
                )
            } else {
                decision = rawDecision
            }

            // playhead-rxuv: content-chapter suppression (primary value of
            // the bead). When a candidate ad span lies substantially
            // inside a creator-labeled `.content` chapter (interview /
            // Q&A / discussion / etc. per `ChapterDispositionClassifier`),
            // demote the gate to `.blockedByPolicy` so the proposal cannot
            // auto-skip. Scoring stays honest — only the gate moves.
            //
            // Flag-OFF and "no chapter evidence" both short-circuit
            // inside `shouldSuppress(...)`, making this a zero-cost no-op
            // for episodes the bead doesn't apply to. Existing gates of
            // equal or higher severity (`.blockedByEvidenceQuorum`,
            // `.blockedByPolicy`, `.blockedByUserCorrection` — see
            // `SkipEligibilityGate.severity`) are preserved so a content-
            // chapter signal cannot UNDO an existing block; the
            // demotion only fires when the current gate is structurally
            // weaker than `.blockedByPolicy` (severity < 2 — i.e.
            // `.eligible` or `.markOnly`. playhead-avbn moved
            // `.blockedByFMConsensus` to severity 2, so it is no longer
            // demotable here — both gates block, so the terminal outcome is
            // unchanged; only the recorded cause is.
            //
            // playhead-pzy2: byte-exact rediff certainty is EXEMPT — the
            // demotion-path mirror of the self-promo exemption below. A span whose
            // width the rediff differ owns (`.rediffSlot`) is a 100%-deterministic
            // DAI divergence (the origin served different ad bytes), so it stays an
            // ad even when it sits inside a creator "content" chapter that only
            // makes it LOOK editorial — deterministic certainty outranks the
            // chapter-label clue. Splice-AGNOSTIC (`.spliceSlot` is acoustic width,
            // not exempt); FM / lexical-only spans stay demotable, so the exemption
            // cannot leak to any non-deterministic span.
            if creatorChapterFusionEnabled,
               decision.eligibilityGate.severity < SkipEligibilityGate.blockedByPolicy.severity,
               !refinedSpan.carriesRediffByteExactWidth,
               CreatorChapterSuppressionEvaluator.shouldSuppress(
                   span: refinedSpan,
                   chapters: assetChapterEvidence
               ) {
                logger.info(
                    "Backfill: [rxuv] content-chapter suppression demoting span=\(refinedSpan.id, privacy: .public) from gate=\(decision.eligibilityGate.rawValue, privacy: .public) → blockedByPolicy"
                )
                decision = DecisionResult(
                    proposalConfidence: decision.proposalConfidence,
                    skipConfidence: decision.skipConfidence,
                    eligibilityGate: .blockedByPolicy,
                    promotionTrack: decision.promotionTrack
                )
            }

            // playhead-fl4j: SELF-PROMO suppression (eligibility-side precision
            // signal) on an ATTENTION → VERIFICATION shape. A curated
            // show-agnostic self-promo phrase matching a candidate ad span's
            // transcript is only ATTENTION ("look here") — a CLUE, not a verdict.
            // `PromoSuppressor.shouldSuppress` demotes ONLY when a verifier
            // corroborates that the segment is the show promoting ITSELF: a
            // STRONG phrase ("follow us", "rate review and subscribe") is
            // self-corroborating, while an AMBIGUOUS phrase ("get tickets", "on
            // tour" — which collide with THIRD-PARTY event ads) demotes only when
            // a first-person / show-identity self-reference marker sits in its
            // local window. A bare AMBIGUOUS match with no self-reference is a
            // clue that FAILED verification and is left `.eligible`. On demotion
            // the gate moves to `.markOnly` so the span surfaces as a
            // play-by-default SUGGEST banner instead of auto-skipping. This is an
            // ELIGIBILITY change only: scoring stays honest (the confidences and
            // promotion track are forwarded verbatim), mirroring the
            // creator-chapter suppressor above.
            //
            // Severity-guarded to `< SkipEligibilityGate.markOnly.severity`
            // (i.e. only `.eligible`, severity 0): the demotion can ONLY relax a
            // fully-eligible span down to banner-only. It never overrides a
            // harder block (`.blockedBy*`, severity ≥ 2 — e.g. a user correction
            // or the creator-chapter demotion just above), never re-touches an
            // equal-severity gate (`.markOnly`, severity 1), and never
            // promotes. (playhead-avbn: `.blockedByFMConsensus` moved to
            // severity 2 and is now covered by the `.blockedBy*` clause above.)
            //
            // Flag-OFF (or the bundled bank failing to load) resolves
            // `selfPromoBank` to nil, so the `let` binding short-circuits before
            // `PromoSuppressor` is ever consulted — a zero-cost no-op that is
            // byte-identical to pre-fl4j for episodes the bead doesn't apply to.
            //
            // playhead-pzy2: byte-exact rediff certainty is EXEMPT. A span whose
            // width the rediff differ owns (`.rediffSlot`) is a 100%-deterministic
            // DAI divergence — the origin literally served different ad bytes — so
            // it stays an ad regardless of a low-certainty self-promo lexical clue
            // (a phrase that only "sounds like" the show promoting itself must not
            // demote a proven ad). This mirrors the same edges' existing exemptions
            // from the boundary refiners (`isWidthOwnership` bypass) and the
            // pre-roll start clamp. Splice-AGNOSTIC: `.spliceSlot` (acoustic width)
            // is NOT exempt, and FM / lexical-only spans stay fully demotable — the
            // exemption cannot leak to any non-deterministic span.
            if config.selfPromoSuppressionEnabled,
               let selfPromoBank,
               decision.eligibilityGate.severity < SkipEligibilityGate.markOnly.severity,
               !refinedSpan.carriesRediffByteExactWidth,
               PromoSuppressor.shouldSuppress(
                   span: refinedSpan,
                   transcriptWords: selfPromoWords,
                   bank: selfPromoBank,
                   showIdentity: selfPromoShowIdentity
               ) {
                logger.info(
                    "Backfill: [fl4j] self-promo suppression demoting span=\(refinedSpan.id, privacy: .public) from gate=\(decision.eligibilityGate.rawValue, privacy: .public) → markOnly"
                )
                decision = DecisionResult(
                    proposalConfidence: decision.proposalConfidence,
                    skipConfidence: decision.skipConfidence,
                    eligibilityGate: .markOnly,
                    promotionTrack: decision.promotionTrack
                )
            }

            // playhead-xsdz.7: Evidence-Fragility precision gate. SOFT
            // multiplicative penalty on `skipConfidence` for brittle spans
            // (one dominant channel, narrow margin over the track threshold,
            // few distinct evidence families) — the FP signature. Computed
            // from the post-suppression ledger geometry that fed the mapper,
            // applied BEFORE the hard auto-skip gate below, and never
            // hard-blocks. OFF by default: when
            // `evidenceFragilityPenaltyEnabled == false` this returns the same
            // `skipConfidence`, so `decision` is byte-identical to pre-xsdz.7.
            // The eligibility gate and promotion track are preserved verbatim.
            //
            // playhead-xsdz.7 fragility DIAGNOSTIC (behavior-neutral): fire the
            // optional diagnostic observer for EVERY span — regardless of
            // whether the penalty is enabled — using the SAME post-suppression
            // `effectiveLedger` and the SAME `decision` confidences/track the
            // real gate reads, BEFORE the penalty mutates `decision`. The score
            // comes from the existing `config.fragilityScore(...)` helper (not
            // reimplemented). `nil` in production ⇒ no-op (no score computed, no
            // record), so this is byte-identical to pre-diagnostic behavior.
            if let fragilityDiagnosticObserver {
                let diagnosticFragilityScore = config.fragilityScore(
                    proposalConfidence: decision.proposalConfidence,
                    promotionTrack: decision.promotionTrack,
                    ledger: effectiveLedger
                )
                await fragilityDiagnosticObserver.record(
                    assetId: analysisAssetId,
                    spanId: refinedSpan.id,
                    spanStart: refinedSpan.startTime,
                    spanEnd: refinedSpan.endTime,
                    proposalConfidence: decision.proposalConfidence,
                    skipConfidence: decision.skipConfidence,
                    standardAutoSkipThreshold: config.effectiveAutoSkipThreshold(for: .standard),
                    fragilityScore: diagnosticFragilityScore,
                    ledger: effectiveLedger
                )
            }

            let penalizedSkipConfidence = config.applyFragilityPenalty(
                skipConfidence: decision.skipConfidence,
                proposalConfidence: decision.proposalConfidence,
                promotionTrack: decision.promotionTrack,
                ledger: effectiveLedger
            )
            if penalizedSkipConfidence != decision.skipConfidence {
                logger.debug(
                    "Backfill: [xsdz.7] fragility penalty span=\(refinedSpan.id, privacy: .public) skipConfidence \(decision.skipConfidence, format: .fixed(precision: 3)) → \(penalizedSkipConfidence, format: .fixed(precision: 3))"
                )
                decision = DecisionResult(
                    proposalConfidence: decision.proposalConfidence,
                    skipConfidence: penalizedSkipConfidence,
                    eligibilityGate: decision.eligibilityGate,
                    promotionTrack: decision.promotionTrack
                )
            }

            // playhead-xsdz.9: HARD-NEGATIVE suppression (the novel lever).
            // When a candidate aligns (Smith-Waterman) to a CONFIRMED false
            // positive in the negative bank, multiplicatively reduce its
            // skipConfidence — we have direct evidence this exact copy is NOT an
            // ad on this show. SOFT (a multiplicative factor, never a hard
            // block) and applied AFTER the fragility penalty, BEFORE the hard
            // auto-skip gate — the same idiom as xsdz.7. `negativeMatch` is nil
            // (⇒ factor 1.0, no-op) whenever the flag is off, no bank is wired,
            // or nothing aligned above threshold, so this is byte-identical to
            // pre-xsdz.9 in those cases. The eligibility gate and promotion
            // track are preserved verbatim — scores stay honest.
            var negativeBankDidSuppress = false
            if let negativeMatch {
                let suppressed = crossEpisodeMemoryEvaluator.suppress(
                    skipConfidence: decision.skipConfidence,
                    with: negativeMatch
                )
                if suppressed != decision.skipConfidence {
                    negativeBankDidSuppress = true
                    logger.debug(
                        "Backfill: [xsdz.9] negative-bank suppression span=\(refinedSpan.id, privacy: .public) sim=\(negativeMatch.similarity, format: .fixed(precision: 3)) skipConfidence \(decision.skipConfidence, format: .fixed(precision: 3)) → \(suppressed, format: .fixed(precision: 3))"
                    )
                    decision = DecisionResult(
                        proposalConfidence: decision.proposalConfidence,
                        skipConfidence: suppressed,
                        eligibilityGate: decision.eligibilityGate,
                        promotionTrack: decision.promotionTrack
                    )
                }
            }

            // playhead-fbsignals.9 fire instrumentation (behavior-neutral): when a
            // bank is actually wired for the suppression READ (flag on AND a bank
            // present — the exact gate the bank read above used), record this span
            // as a candidate and whether the suppression moved its `skipConfidence`.
            // `nil` in production ⇒ no-op (no record), so this is byte-identical to
            // pre-fbsignals behavior. It NEVER feeds back into the decision; it only
            // records what the suppression step already computed. With an EMPTY
            // cold-start bank `negativeBankDidSuppress` is always false.
            if let negativeBankSuppressionObserver,
               config.crossEpisodeMemoryEnabled,
               negativeFingerprintBank != nil {
                await negativeBankSuppressionObserver.record(
                    assetId: analysisAssetId,
                    didSuppress: negativeBankDidSuppress
                )
            }

            // playhead-xsdz.10: STOP per-span processing here and collect the
            // scored decision. The hard auto-skip gate and all side effects run
            // in the second loop below, AFTER the temporal-regularization pass
            // has had a chance to see every candidate detection together. The
            // collected `decision` is the value the gate would have seen at this
            // point pre-xsdz.10; when the temporal flag is off it is emitted
            // unchanged, preserving byte-identical behaviour.
            pendingDecisions.append(PendingBackfillDecision(
                refinedSpan: refinedSpan,
                decision: decision,
                ledger: ledger,
                effectiveLedger: effectiveLedger,
                spanTopCatalogSimilarity: spanTopCatalogSimilarity,
                spanTopCatalogMatch: spanTopCatalogMatch,
                spanCatalogWasEvaluated: spanCatalogWasEvaluated,
                finalizerRewroteGeometry: false,
                fusionSplitDiscriminator: nil
            ))
        }

        // playhead-xsdz.10: lightweight temporal regularization. Real ads are
        // CONTIGUOUS / CLUSTERED; false positives are typically ISOLATED. This
        // deterministic, post-fusion pass softly penalizes a detection with NO
        // high-confidence neighbor in a time window (isolation penalty) and
        // additionally down-weights a too-short, uncorroborated island
        // (min-dwell). The neighbor-support signal is HIGH-CONFIDENCE-GATED and
        // ONE-SIDED (anti-contagion): the pass can only LOWER a confidence,
        // never raise it, so two adjacent WEAK false positives do NOT rescue
        // each other. Applied to `decision.skipConfidence` BEFORE the hard
        // auto-skip gate; the eligibility gate and promotion track are preserved
        // verbatim. OFF by default — when `temporalRegularizationEnabled` is
        // false the pass is never invoked and `pendingDecisions` is emitted
        // unchanged (byte-identical to pre-xsdz.10).
        if config.temporalRegularizationEnabled, pendingDecisions.count > 1 {
            let detections = pendingDecisions.map { pending in
                TemporalRegularizer.Detection(
                    id: pending.refinedSpan.id,
                    startTime: pending.refinedSpan.startTime,
                    endTime: pending.refinedSpan.endTime,
                    skipConfidence: pending.decision.skipConfidence
                )
            }
            let adjustments = TemporalRegularizer.regularize(
                detections: detections,
                parameters: config.temporalRegularizerParameters
            )
            // The regularizer keys adjustments by detection id; map them back so
            // the result is independent of the (sorted) order the pass returns.
            var adjustedById: [String: Double] = [:]
            for adjustment in adjustments where adjustment.changed {
                adjustedById[adjustment.id] = adjustment.adjustedSkipConfidence
            }
            if !adjustedById.isEmpty {
                for index in pendingDecisions.indices {
                    let span = pendingDecisions[index].refinedSpan
                    guard let adjusted = adjustedById[span.id] else { continue }
                    let before = pendingDecisions[index].decision.skipConfidence
                    logger.debug(
                        "Backfill: [xsdz.10] temporal regularization span=\(span.id, privacy: .public) skipConfidence \(before, format: .fixed(precision: 3)) → \(adjusted, format: .fixed(precision: 3))"
                    )
                    let current = pendingDecisions[index].decision
                    pendingDecisions[index].decision = DecisionResult(
                        proposalConfidence: current.proposalConfidence,
                        skipConfidence: adjusted,
                        eligibilityGate: current.eligibilityGate,
                        promotionTrack: current.promotionTrack
                    )
                }
            }

            // playhead-actempo.10 fire instrumentation (behavior-neutral): record
            // how many candidate detections the pass actually changed
            // (`adjustedById.count` is exactly the set of spans whose
            // `Adjustment.changed` was true) out of the candidates it ran over.
            // `nil` in production ⇒ no-op (no record), so this is byte-identical to
            // pre-actempo behavior. It NEVER feeds back into the decision; it only
            // records the count the pass already computed.
            if let temporalRegularizationObserver {
                await temporalRegularizationObserver.record(
                    assetId: analysisAssetId,
                    candidateSpans: pendingDecisions.count,
                    penaltyAppliedSpans: adjustedById.count
                )
            }
        }

        // playhead-p56a: SpanFinalizer safety layer. Deterministic, post-
        // fusion pass that applies five hard constraints to the scored
        // decisions before persistence:
        //   1. Non-overlap resolution (higher confidence wins, loser trimmed
        //      or suppressed).
        //   2. < 3s content-gap merge (target of the bd-4xqf coverage
        //      investigation: re-merge candidates the upstream merge-gap
        //      dropped).
        //   3. Duration sanity (< 5s dropped, > 180s split).
        //   4. Chapter penalty (spans crossing `.content` chapters demoted
        //      to `.markOnly`). Empty `chapters: []` here disables this
        //      constraint until the structured `ChapterMarker` snapshot is
        //      threaded through — bd-4xqf's primary target is constraints
        //      1/2, not 4.
        //   5. 50% action cap.
        //   6. Skip-policy override pass (always applies once a candidate is
        //      kept; trace fires only when the resolved action is not
        //      `.autoSkipEligible`).
        //
        // OFF by default — when `config.spanFinalizerEnabled == false` the
        // pass is never invoked, `pendingDecisions` is forwarded to the
        // emission loop unchanged, and `lastSpanFinalizerConstraintsBySpanId`
        // stays empty. Byte-identical to pre-p56a behaviour.
        //
        // Sources of input intent/ownership match what the emission loop
        // already uses below (`SkipPolicyMatrix.action(for: .unknown,
        // ownership: .unknown)` → `.detectOnly`) — the upstream pipeline
        // does not yet classify commercial intent at the per-span level
        // (Phase 8 work). Using the same `(.unknown, .unknown)` pair keeps
        // the finalizer's policy step consistent with the existing gate.
        if config.spanFinalizerEnabled, !pendingDecisions.isEmpty {
            let candidates: [CandidateSpan] = pendingDecisions.map { pending in
                CandidateSpan(
                    span: pending.refinedSpan,
                    decision: pending.decision,
                    commercialIntent: .unknown,
                    adOwnership: .unknown
                )
            }
            // Index pending by spanId so finalized spans can be paired with
            // their upstream non-catalog decision context in O(1). The
            // The finalizer preserves the source identity separately from
            // each emitted identity. Trims and merges retain the surviving
            // id; splits mint deterministic child ids so independently
            // persisted chunks cannot collide. Multiple finalized rows may
            // still map back to the same pending source record. Catalog
            // evidence is not inherited from that record when geometry
            // changes; each finalized material slice is re-fingerprinted.
            var pendingByOriginalId: [String: PendingBackfillDecision] = [:]
            pendingByOriginalId.reserveCapacity(pendingDecisions.count)
            for pending in pendingDecisions {
                pendingByOriginalId[pending.refinedSpan.id] = pending
            }

            let finalizer = SpanFinalizer(
                episodeDuration: episodeDuration,
                chapters: []
            )
            let finalized = finalizer.finalize(candidates)

            // Rebuild `pendingDecisions` from the finalizer output. Spans
            // the finalizer suppressed (constraint #1's `.overlapSuppressed`
            // or constraint #3's `.droppedBelowMinDuration`) are absent from
            // `finalized` and therefore correctly dropped from emission.
            var rebuilt: [PendingBackfillDecision] = []
            rebuilt.reserveCapacity(finalized.count)
            var traceById: [String: [String]] = [:]
            for span in finalized {
                try Task.checkCancellation()
                let sourcePendings = span.sourceSpanIds.compactMap {
                    pendingByOriginalId[$0]
                }
                guard sourcePendings.count == span.sourceSpanIds.count,
                      let pending = sourcePendings.first else {
                    // Shouldn't happen — finalizer only emits spans whose id
                    // came from the input candidates. A partial merge cohort
                    // would produce incomplete provenance, so drop the whole
                    // output rather than silently attributing it to one source.
                    logger.warning("[p56a] finalizer emitted unknown source cohort for spanId \(span.span.id, privacy: .public); dropping")
                    continue
                }
                let geometryWasRewritten =
                    pending.refinedSpan.startTime.bitPattern
                        != span.span.startTime.bitPattern
                    || pending.refinedSpan.endTime.bitPattern
                        != span.span.endTime.bitPattern
                // Width-ownership provenance and boundary-refinement traces are
                // exact-edge claims. Once the finalizer moves either edge, keep
                // the new geometry but drop whole-span width ownership so no
                // downstream diagnostic or policy can treat the rewritten span
                // as byte/splice exact.
                let finalizedSpan: DecodedSpan
                if geometryWasRewritten {
                    finalizedSpan = DecodedSpan(
                        id: span.span.id,
                        assetId: span.span.assetId,
                        firstAtomOrdinal: span.span.firstAtomOrdinal,
                        lastAtomOrdinal: span.span.lastAtomOrdinal,
                        startTime: span.span.startTime,
                        endTime: span.span.endTime,
                        anchorProvenance: span.span.anchorProvenance.filter {
                            !$0.isWidthOwnership
                        }
                    )
                } else {
                    finalizedSpan = span.span
                }

                let catalogEvidence: CatalogSpanEvidence
                let refreshedLedger: [EvidenceLedgerEntry]
                let refreshedEffectiveLedger: [EvidenceLedgerEntry]
                if geometryWasRewritten {
                    // A merge represents material from every absorbed source
                    // candidate. Retain all non-catalog diagnostic evidence,
                    // then replace the old per-source fingerprint rows with
                    // one match recomputed over the exact finalized geometry.
                    let sourceLedger = sourcePendings.flatMap(\.ledger)
                    let sourceEffectiveLedger =
                        sourcePendings.flatMap(\.effectiveLedger)
                    catalogEvidence =
                        await Self.resolveCatalogSpanEvidence(
                            span: finalizedSpan,
                            featureWindows: featureWindows,
                            catalogStore: adCatalogStore,
                            showId: catalogShowId
                        )
                    refreshedLedger =
                        Self.replacingFingerprintStoreCatalogEvidence(
                            in: sourceLedger,
                            with: catalogEvidence,
                            catalogCap: fusionConfig.catalogCap
                        )
                    refreshedEffectiveLedger =
                        Self.replacingFingerprintStoreCatalogEvidence(
                            in: sourceEffectiveLedger,
                            with: catalogEvidence,
                            catalogCap: fusionConfig.catalogCap
                        )
                } else {
                    catalogEvidence = CatalogSpanEvidence(
                        topSimilarity: pending.spanTopCatalogSimilarity,
                        topMatch: pending.spanTopCatalogMatch,
                        wasEvaluated: pending.spanCatalogWasEvaluated
                    )
                    refreshedLedger = pending.ledger
                    refreshedEffectiveLedger = pending.effectiveLedger
                }
                // SpanFinalizer's `WorkingSpan` does NOT retain the upstream
                // `promotionTrack` (its `toFinalized()` builds a new
                // `DecisionResult` via the default `.standard` track).
                // Re-stamp it here from the original pending decision so
                // qualified-track spans (`.classifierSeedQualified`,
                // `.lexicalAutoAdQualified`) keep their looser auto-skip
                // threshold downstream. Without this re-stamp, a finalizer
                // pass would silently demote every qualified-track span to
                // the standard 0.80 threshold — a hidden behavioral change
                // outside the byte-identical-OFF contract's coverage.
                let finalizedEligibilityGate: SkipEligibilityGate
                if geometryWasRewritten,
                   span.decision.eligibilityGate.severity
                       < SkipEligibilityGate.markOnly.severity {
                    // Geometry is decision material. A trim, merge, or split
                    // has not been classified as this exact interval, so the
                    // finalizer may keep it as a marker but may not carry the
                    // source interval's automatic authority forward. This is
                    // also a defense-in-depth backstop for any future
                    // SpanFinalizer transform that forgets to demote itself.
                    finalizedEligibilityGate = .markOnly
                } else {
                    finalizedEligibilityGate =
                        span.decision.eligibilityGate
                }
                let finalizedDecision = DecisionResult(
                    proposalConfidence: span.decision.proposalConfidence,
                    skipConfidence: span.decision.skipConfidence,
                    eligibilityGate: finalizedEligibilityGate,
                    promotionTrack: pending.decision.promotionTrack
                )
                let updated = PendingBackfillDecision(
                    refinedSpan: finalizedSpan,
                    decision: finalizedDecision,
                    ledger: refreshedLedger,
                    effectiveLedger: refreshedEffectiveLedger,
                    spanTopCatalogSimilarity:
                        catalogEvidence.topSimilarity,
                    spanTopCatalogMatch: catalogEvidence.topMatch,
                    spanCatalogWasEvaluated:
                        catalogEvidence.wasEvaluated,
                    finalizerRewroteGeometry: geometryWasRewritten,
                    fusionSplitDiscriminator:
                        span.span.id == span.sourceSpanId
                            ? nil
                            : span.span.id
                )
                rebuilt.append(updated)
                // Split outputs have distinct ids, so their traces remain
                // independently attributable through persistence/runtime.
                let traceRaw = span.constraintTrace.map { $0.rawValue }
                if !traceRaw.isEmpty {
                    traceById[span.span.id] = traceRaw
                }
            }
            pendingDecisions = rebuilt
            lastCatalogMatchSimilarity =
                rebuilt.map(\.spanTopCatalogSimilarity).max() ?? 0
            lastSpanFinalizerConstraintsBySpanId = traceById

            logger.info(
                "[p56a] finalizer ran: candidates=\(candidates.count) finalized=\(finalized.count) spansWithTrace=\(traceById.count)"
            )
        }

        // playhead-xsdz.10: emission loop. Each step below is verbatim the code
        // that previously ran inline at the tail of the per-span loop; the only
        // change is that it now reads the (possibly temporally-regularized)
        // `decision` from the collected `pendingDecisions` record.
        for pending in pendingDecisions {
            try Task.checkCancellation()

            let refinedSpan = pending.refinedSpan
            // playhead-2350 (PRESENCE vs EXTENT): derive the span's EXTENT
            // support HERE — this is the first point at which both the boundary
            // refiners and the span finalizer have finished touching the
            // geometry, so it is the only place an edge claim is final. The two
            // authoritative sources are the refined span's width provenance
            // (`.rediffSlot`) and the stinger snap trace keyed by the SAME
            // `refinedSpan.id` the trace re-key block below reads; a finalizer
            // geometry rewrite invalidates both (`finalizerRewroteGeometry`).
            //
            // When neither channel fires for a span — no `.rediffSlot` width
            // ownership and no snap in `lastStingerRefinementTraceBySpanId`
            // (which is empty outright when the stinger refiner is off) — extent
            // resolves `.unanchored` on both edges. That is the honest answer,
            // and under `unanchoredExtentBlocksAutoSkip` (default ON) it makes
            // the span mark-only rather than auto-skippable.
            let extentSupport = SpanExtentSupport.derive(
                anchorProvenance: refinedSpan.anchorProvenance,
                stingerTrace: lastStingerRefinementTraceBySpanId[refinedSpan.id],
                geometryWasRewritten: pending.finalizerRewroteGeometry
            )
            // Stamp EXTENT onto the PRESENCE-derived verdict and apply the
            // unanchored-edge auto-skip block. Everything below in this
            // iteration — the `policyAction` promotion, the persisted
            // `AdWindow.eligibilityGate`, the `AdDecisionResult` forwarded to
            // SkipOrchestrator, `decision_events`, the explanation trace and
            // the NARL decision log — reads THIS `decision`, so the demotion is
            // recorded consistently everywhere the old verdict was.
            let decision = pending.decision.withExtentSupport(
                extentSupport,
                blockingUnanchoredAutoSkip: config.unanchoredExtentBlocksAutoSkip
            )
            if decision.eligibilityGate != pending.decision.eligibilityGate {
                logger.info(
                    "[2350] span \(refinedSpan.id, privacy: .public) extent unanchored on \(extentSupport.unanchoredEdges.joined(separator: "+"), privacy: .public) — \(pending.decision.eligibilityGate.rawValue, privacy: .public)→\(decision.eligibilityGate.rawValue, privacy: .public) (presence skipConfidence \(decision.skipConfidence, format: .fixed(precision: 2)) unchanged)"
                )
            }
            let ledger = pending.ledger
            let effectiveLedger = pending.effectiveLedger
            let spanTopCatalogSimilarity = pending.spanTopCatalogSimilarity
            let spanTopCatalogMatch = pending.spanTopCatalogMatch
            let spanCatalogWasEvaluated = pending.spanCatalogWasEvaluated

            // Step 14: SkipPolicyMatrix + confidence promotion.
            // Phase 6.5 (playhead-4my.16): (.unknown, .unknown) → .detectOnly so Phase 7
            // has banner impressions to correct against.
            let rawPolicyAction = SkipPolicyMatrix.action(for: .unknown, ownership: .unknown)

            // Phase 6.5b (playhead-4my.17): confidence-gated autoSkipEligible promotion.
            // Eligible spans with skipConfidence >= threshold are promoted from
            // detectOnly/logOnly → autoSkipEligible. .suppress is never overridden.
            // Gate-blocked spans are excluded by the eligibilityGate check.
            //
            // playhead-fqc8: read the threshold from the decision's
            // `promotionTrack`. `.standard` keeps `autoSkipConfidenceThreshold`
            // (0.80 default); `.classifierSeedQualified` switches to the
            // looser `classifierSeedQualifiedThreshold` (0.50 default) so a
            // classifier-only span backed by `breakAlignment` corroboration
            // can clear the gate despite the structural ledger ceiling.
            let baseAutoSkipThreshold = config.effectiveAutoSkipThreshold(for: decision.promotionTrack)
            // playhead-xsdz.11: apply the per-show PI-controller OFFSET resolved
            // once at the top of this run, then clamp the EFFECTIVE threshold to
            // [0.55, 0.95]. With the feature off (or no controller store wired)
            // `perShowThresholdOffset` is exactly 0, so this resolves to
            // `baseAutoSkipThreshold` unchanged — byte-identical to pre-xsdz.11.
            // The offset can RAISE (FP corrections → more conservative) or LOWER
            // (miss corrections → more aggressive) the threshold, bounded both by
            // the controller's own offset cap and this effective clamp.
            //
            // SCOPE: only the `.standard` track (the global auto-skip threshold,
            // 0.80 by default — exactly the "per-show auto-skip confidence
            // threshold" the controller personalizes). The qualified precision
            // lanes (`.classifierSeedQualified`, `.lexicalAutoAdQualified`)
            // intentionally use a sub-0.55 floor (0.50) for a structurally
            // different reason and must NOT be pulled up to 0.55 by this clamp —
            // they are left untouched so their byte-identical behaviour is
            // preserved on every track but `.standard`.
            let autoSkipThreshold: Double
            if config.perShowThresholdControlEnabled,
               perShowThresholdOffset != 0,
               decision.promotionTrack == .standard {
                autoSkipThreshold = PerShowThresholdController.effectiveThreshold(
                    globalThreshold: baseAutoSkipThreshold,
                    offset: perShowThresholdOffset,
                    parameters: config.perShowThresholdControllerParameters
                )
            } else {
                autoSkipThreshold = baseAutoSkipThreshold
            }

            // playhead-fbsignals.11 fire instrumentation (behavior-neutral): when the
            // feature is on, record this span as a gate candidate and whether the
            // offset actually SHIFTED the effective threshold the gate used
            // (`autoSkipThreshold != baseAutoSkipThreshold` — the EXACT effect the
            // gate above applied). `nil` in production ⇒ no-op (no record), so this
            // is byte-identical to pre-fbsignals behavior. It NEVER feeds back into
            // the decision; it only records what the gate already computed. With an
            // EMPTY cold-start controller the resolved offset is 0, so no span shifts.
            if let perShowThresholdOffsetObserver, config.perShowThresholdControlEnabled {
                await perShowThresholdOffsetObserver.recordSpan(
                    assetId: analysisAssetId,
                    didShiftThreshold: autoSkipThreshold != baseAutoSkipThreshold
                )
            }
            let policyAction: SkipPolicyAction
            if (rawPolicyAction == .detectOnly || rawPolicyAction == .logOnly),
               decision.eligibilityGate == .eligible,
               decision.skipConfidence.isFinite,
               decision.skipConfidence >= autoSkipThreshold {
                policyAction = .autoSkipEligible
                logger.debug(
                    "Backfill: span \(refinedSpan.id, privacy: .public) promoted detectOnly→autoSkipEligible (skipConfidence=\(decision.skipConfidence, format: .fixed(precision: 2)) >= \(autoSkipThreshold, format: .fixed(precision: 2)))"
                )
            } else {
                policyAction = rawPolicyAction
            }

            // Build AdWindow from fusion decision (uses already-refined span boundaries).
            // playhead-epfk: thread the per-span top `AdCatalogStore`
            // similarity computed above so it persists to `ad_windows`
            // and surfaces in the corpus export. `nil` means catalog
            // evaluation was unavailable (including missing/stale show
            // identity or SQLite failure); `0` means an exact-show query
            // completed but no row cleared the floor.
            let catalogStoreMatchSimilarity: Double? =
                spanCatalogWasEvaluated
                    ? Double(spanTopCatalogSimilarity)
                    : nil
            // playhead-hdgk: persist the per-edge anchor tier so the auto-skip
            // edge-padding policy can classify this span by its real provenance
            // on ingest. playhead-2350 folded the derivation into
            // `extentSupport` at the top of this iteration (the gate and the
            // persisted tiers must be the same observation); this is the
            // persistence-facing view of it.
            let edgeAnchors = (
                start: extentSupport.startAnchor,
                end: extentSupport.endAnchor
            )
            let window = buildFusionAdWindow(
                span: refinedSpan,
                decision: decision,
                policyAction: policyAction,
                analysisAssetId: analysisAssetId,
                catalogStoreMatchSimilarity: catalogStoreMatchSimilarity,
                catalogMatch: spanTopCatalogMatch,
                startEdgeAnchor: edgeAnchors.start,
                endEdgeAnchor: edgeAnchors.end,
                fusionSplitDiscriminator:
                    pending.fusionSplitDiscriminator,
                // playhead-ar60 (R1 review): whether the veto that can gate
                // this span was a judgement about THIS span. Same snapshot and
                // same span bounds as the `correctionFactor` above, so the
                // question the emission asks and the question the gate answered
                // are one observation.
                userCorrectionIsSpanScoped:
                    correctionSnapshot.hasSpanScopedSuppressor(
                        overlapping: refinedSpan.startTime,
                        refinedSpan.endTime
                    )
            )
            fusionWindows.append(window)

            // playhead-p56a: resolve the per-window finalizer constraint
            // trace from the spanId map populated by the wire-in block. The
            // emission-loop window id is fresh per call, so the only place
            // this stamp can happen is here — after `buildFusionAdWindow`
            // and inside the same iteration that knows both the windowId
            // and the source `refinedSpan.id`. When the flag is off,
            // `lastSpanFinalizerConstraintsBySpanId` is empty (see top-of-
            // run reset) so the lookup short-circuits without an allocation
            // and the window-keyed map stays empty — byte-identical OFF.
            if let trace = lastSpanFinalizerConstraintsBySpanId[refinedSpan.id], !trace.isEmpty {
                lastSpanFinalizerConstraintsByWindowId[window.id] = trace
            }

            // playhead-l2f.6: resolve the per-window stinger refinement
            // trace from the spanId map populated inside the boundary-
            // refinement block. Same stamp point and rationale as the
            // p56a trace above: the window id is fresh per call, so this
            // iteration is the only place that knows both ids. When the
            // flag is off the spanId map is empty (see top-of-run reset)
            // and the window-keyed map stays empty — byte-identical OFF.
            if !pending.finalizerRewroteGeometry,
               let stingerTrace = lastStingerRefinementTraceBySpanId[refinedSpan.id] {
                lastStingerRefinementTraceByWindowId[window.id] = stingerTrace
            }

            // playhead-xsdz.37: resolve the per-window lexical refinement trace
            // from the spanId map populated inside the boundary-refinement
            // block. Same stamp point and rationale as the stinger trace above;
            // when the flag is off the spanId map is empty (top-of-run reset)
            // so the window-keyed map stays empty — byte-identical OFF.
            if !pending.finalizerRewroteGeometry,
               let lexicalTrace = lastLexicalRefinementTraceBySpanId[refinedSpan.id] {
                lastLexicalRefinementTraceByWindowId[window.id] = lexicalTrace
            }

            // playhead-xsdz.36.1.1 (observability-only): stamp the per-window
            // evidence provenance — the source span's anchor-provenance kinds
            // plus the DISTINCT scoring-ledger source kinds the FM-consensus
            // quorum counts (`Self.corroboratingEvidenceSourceKinds` re-derives
            // that set from `effectiveLedger`, the SAME ledger `DecisionMapper`
            // received). Captured UNCONDITIONALLY: the provenance + ledger
            // always exist for an emitted window and no decision path reads this
            // map, so this is behaviour-neutral for every config. Same stamp
            // point as the trace maps above so the fresh window id correlates to
            // its source span.
            lastEvidenceProvenanceByWindowId[window.id] = EvidenceProvenanceForTesting(
                anchorProvenanceKinds: refinedSpan.anchorProvenance.map(\.provenanceKind),
                evidenceSourceKinds: Self.corroboratingEvidenceSourceKinds(effectiveLedger)
            )

            // Neither recurrence mechanism is learned here. A proposal, even
            // one labelled autoSkipEligible, is not an authoritative positive.
            // SkipOrchestrator admits both the catalog and repeated-ad cache
            // only after delayed consumption or explicit confirmation.

            // Accumulate AdDecisionResult for step 17 (orchestrator forwarding).
            // SkipEligibilityGate has more cases than AdDecisionEligibilityGate; collapse
            // all non-eligible variants to .blocked — receiveAdDecisionResults guards on this.
            let orchestratorGate: AdDecisionEligibilityGate =
                decision.eligibilityGate == .eligible ? .eligible : .blocked
            fusionDecisionResults.append(AdDecisionResult(
                id: window.id,
                analysisAssetId: analysisAssetId,
                startTime: refinedSpan.startTime,
                endTime: refinedSpan.endTime,
                skipConfidence: decision.skipConfidence,
                eligibilityGate: orchestratorGate,
                recomputationRevision: 0,
                // The final persisted row is attached only after all
                // reconciliation, clamping, and metadata writes complete.
                // Never substitute this pre-write material.
                producerRevision: nil
            ))

            // Accumulate DecisionEvent for step 16. Bug 6: cohortJSON is hoisted
            // to asset scope (`assetCohortJSON`) so the per-asset
            // `ad_decision_results` row and every per-window `decision_events` row
            // serialize the same cohort string.
            let cohortJSON = assetCohortJSON
            // playhead-ef2.1.4: build structured explanation trace from ledger + decision
            let explanation = DecisionExplanation.build(
                ledger: ledger,
                decision: decision,
                policyAction: policyAction,
                config: fusionConfig,
                skipThreshold: autoSkipThreshold
            )
            let explanationJSON = (try? JSONEncoder().encode(explanation))
                .flatMap { String(data: $0, encoding: .utf8) }

            // Capture a single wall-clock once so DecisionEvent and
            // DecisionLogEntry share the exact same timestamp (previously
            // each called Date() independently, diverging by microseconds).
            let decisionTimestamp = Date().timeIntervalSince1970

            decisionEvents.append(DecisionEvent(
                id: UUID().uuidString,
                analysisAssetId: analysisAssetId,
                eventType: "backfill_fusion",
                windowId: window.id,
                proposalConfidence: decision.proposalConfidence,
                skipConfidence: decision.skipConfidence,
                eligibilityGate: decision.eligibilityGate.rawValue,
                policyAction: policyAction.rawValue,
                decisionCohortJSON: cohortJSON,
                createdAt: decisionTimestamp,
                explanationJSON: explanationJSON
            ))

            // playhead-8em9 (narL): emit per-window DecisionLogEntry for
            // offline replay. Resolves MetadataActivationConfig so the
            // snapshot matches the gated consumers' view at this decision.
            let logEntry = DecisionLogEntry(
                schemaVersion: DecisionLogEntry.currentSchemaVersion,
                analysisAssetID: analysisAssetId,
                timestamp: decisionTimestamp,
                windowBounds: .init(
                    start: refinedSpan.startTime,
                    end: refinedSpan.endTime
                ),
                activationConfig: .init(MetadataActivationConfig.resolved()),
                evidence: effectiveLedger.map(DecisionLogEntry.LedgerEntry.init),
                fusedConfidence: .init(
                    proposalConfidence: decision.proposalConfidence,
                    skipConfidence: decision.skipConfidence,
                    breakdown: explanation.evidenceBreakdown
                ),
                finalDecision: .init(
                    action: policyAction.rawValue,
                    gate: decision.eligibilityGate.rawValue,
                    skipConfidence: decision.skipConfidence,
                    thresholdCrossed: autoSkipThreshold
                )
            )
            decisionLogEntries.append((window.id, logEntry))
        }

        // ── Pre-roll start-at-zero clamp (playhead-xsdz.66) ──────────────────
        // A pre-roll ad begins at 0:00, but the ASR / transcript cold-start ramp
        // routinely makes the detected start a few seconds late. When the
        // episode's FIRST ad window sits in the pre-roll zone (an `.unanchored`
        // start within `preRollStartClampSeconds` of 0), extend its start edge to
        // exactly 0.0 — a pre-roll's start edge is "free" at 0:00. Trustworthy
        // `.rediffByteExact` / `.stingerSnapped` edges are EXEMPT (the clamp never
        // second-guesses a precise boundary), so this only reclaims the width the
        // cold-start ramp lost on the guessed presence-core starts.
        //
        // WIDTH / MARK: applied HERE — after the per-span decision loop. The
        // clamp adds material the classifier did not evaluate, so every changed
        // row is capped to mark-only (or keeps a stricter existing gate). This
        // remains fail-closed even when the independent edge-padding feature is
        // disabled.
        // The window id is ordinal-addressed, so the clamp keeps it and the
        // content-addressed reconcile below stays in place — no reorder, no churn.
        // Idempotent + monotonic (start only moves leftward → coverage never
        // shrinks, start never exceeds end). Disabled when
        // `preRollStartClampSeconds <= 0`.
        //
        // The widened start does also flow into the learned priors + metadata
        // extraction below (both read `nonSuppressedWindows`); that is benign and
        // arguably correct — a pre-roll genuinely starts at 0:00.
        // A LISTENER'S MARK IS OFF LIMITS TO THE CLAMP (playhead-lc4c). The
        // `userMarked` rows are persisted separately and are NOT in
        // `fusionWindows`, so the clamp cannot see them — a detector window can
        // otherwise be widened straight over a span the listener defined by hand.
        // Observed: a fusion window past a user's [35, 55) mark widened to [0, 60)
        // and engulfed it, leaving two windows over the marked region.
        //
        // Failing to read is fail-closed in the SAFE direction for the CLAMP but
        // NOT for the mark: an empty list means the clamp behaves exactly as it did
        // before this guard existed. That is the pre-existing behaviour rather than
        // a new risk, and swallowing the error keeps a read failure from aborting
        // an otherwise good backfill — but it does mean a mark can still be
        // widened over if this read fails, which is why the read is a plain
        // `fetchAdWindows` on the asset we already hold rather than anything that
        // can partially succeed.
        let protectedUserMarkedRegions: [(start: Double, end: Double)]
        do {
            protectedUserMarkedRegions = try await store
                .fetchAdWindows(assetId: analysisAssetId)
                .filter { $0.boundaryState == "userMarked" }
                .map { (start: $0.startTime, end: $0.endTime) }
        } catch {
            protectedUserMarkedRegions = []
        }

        fusionWindows = PreRollStartClamp.clamp(
            windows: fusionWindows,
            config: .init(maxPreRollStartSeconds: config.preRollStartClampSeconds),
            protectedRegions: protectedUserMarkedRegions
        )
        // The clamp runs after decisions are assembled, so refresh the
        // persisted/runtime decision envelope from the final window geometry.
        // Otherwise DecisionResultArtifact would serialize the detected
        // pre-clamp start while `ad_windows` and live orchestration use 0.0.
        let finalFusionWindowByID = fusionWindows.reduce(
            into: [String: AdWindow]()
        ) { result, window in
            result[window.id] = window
        }
        fusionDecisionResults = fusionDecisionResults.map { result in
            guard let window = finalFusionWindowByID[result.id],
                  window.analysisAssetId == result.analysisAssetId else {
                return result
            }
            let finalEligibilityGate: AdDecisionEligibilityGate =
                result.eligibilityGate == .eligible
                    && window.eligibilityGate
                        == SkipEligibilityGate.eligible.rawValue
                    ? .eligible
                    : .blocked
            return AdDecisionResult(
                id: result.id,
                analysisAssetId: result.analysisAssetId,
                startTime: window.startTime,
                endTime: window.endTime,
                // playhead-ar60: an ACTUATION reader. This rebind exists so
                // the exported decision describes the same final material as
                // the `ad_windows` row, and the orchestrator's envelope check
                // compares it against `producerRevision.actuationConfidence`
                // — reading `window.confidence` here would hand over the
                // DETECTION number and fail that comparison on every fusion
                // span.
                skipConfidence: window.actuationConfidence,
                eligibilityGate: finalEligibilityGate,
                recomputationRevision: result.recomputationRevision,
                // Metadata and reconciliation can still revise durable
                // material; Step 17 attaches the exact row after persistence.
                producerRevision: nil
            )
        }
        // Decision events and replay logs were assembled before the clamp.
        // Rebind their geometry and gate now so every durable or exported
        // decision artifact describes the same final material as ad_windows.
        decisionEvents = decisionEvents.map { event in
            guard let window = finalFusionWindowByID[event.windowId],
                  window.analysisAssetId == event.analysisAssetId else {
                return event
            }
            let finalGate =
                window.eligibilityGate ?? event.eligibilityGate
            let finalExplanationJSON: String?
            if let raw = event.explanationJSON,
               let data = raw.data(using: .utf8),
               let explanation = try? JSONDecoder().decode(
                   DecisionExplanation.self,
                   from: data
               ) {
                let finalized = DecisionExplanation(
                    evidenceBreakdown: explanation.evidenceBreakdown,
                    contributingFamilies:
                        explanation.contributingFamilies,
                    actionRationale: ActionRationale(
                        threshold:
                            explanation.actionRationale.threshold,
                        gate: finalGate,
                        policyAction:
                            explanation.actionRationale.policyAction,
                        skipEligible:
                            event.policyAction
                                == SkipPolicyAction.autoSkipEligible.rawValue
                            && finalGate
                                == SkipEligibilityGate.eligible.rawValue,
                        // playhead-2350: the pre-roll clamp rewrites GEOMETRY,
                        // not extent PROVENANCE (and only ever caps the result
                        // to mark-only), so the recorded anchors carry forward
                        // verbatim. Rebuilding them as nil here would erase the
                        // one field that says WHY a span was banner-only.
                        startEdgeAnchor:
                            explanation.actionRationale.startEdgeAnchor,
                        endEdgeAnchor:
                            explanation.actionRationale.endEdgeAnchor,
                        extentFullyAnchored:
                            explanation.actionRationale.extentFullyAnchored
                    )
                )
                finalExplanationJSON = (try? JSONEncoder().encode(finalized))
                    .flatMap { String(data: $0, encoding: .utf8) }
            } else {
                finalExplanationJSON = event.explanationJSON
            }
            return DecisionEvent(
                id: event.id,
                analysisAssetId: event.analysisAssetId,
                eventType: event.eventType,
                windowId: event.windowId,
                proposalConfidence: event.proposalConfidence,
                skipConfidence: event.skipConfidence,
                eligibilityGate: finalGate,
                policyAction: event.policyAction,
                decisionCohortJSON: event.decisionCohortJSON,
                createdAt: event.createdAt,
                explanationJSON: finalExplanationJSON
            )
        }
        decisionLogEntries = decisionLogEntries.map { pending in
            let entry = pending.entry
            guard let window =
                    finalFusionWindowByID[pending.windowId] else {
                return pending
            }
            let finalized = DecisionLogEntry(
                schemaVersion: entry.schemaVersion,
                analysisAssetID: entry.analysisAssetID,
                timestamp: entry.timestamp,
                windowBounds: .init(
                    start: window.startTime,
                    end: window.endTime
                ),
                activationConfig: entry.activationConfig,
                evidence: entry.evidence,
                fusedConfidence: entry.fusedConfidence,
                finalDecision: .init(
                    action: entry.finalDecision.action,
                    gate:
                        window.eligibilityGate
                            ?? entry.finalDecision.gate,
                    skipConfidence:
                        entry.finalDecision.skipConfidence,
                    thresholdCrossed:
                        entry.finalDecision.thresholdCrossed
                )
            )
            return (pending.windowId, finalized)
        }
        for pending in decisionLogEntries {
            await decisionLogger.record(pending.entry)
        }

        // Persist fusion windows and retire the nonterminal hot-path rows this
        // backfill supersedes. playhead-ud4n: backfill is authoritative over the
        // current detector version's reconcilable rows. `reconcileBackfillWindows`
        // computes the set-difference (existing reconcilable ids − new fusion
        // ids); `reconcileBackfillAdWindows` then applies INSERT-OR-REPLACE(new)
        // + DELETE(retired) in ONE transaction, so a superseded hot false-
        // positive can no longer survive beside the fusion result and later
        // auto-skip via the `beginEpisode` preload. Runs even when
        // `fusionWindows` is empty (backfill found no ads) so a rejected hot
        // candidate is still retired. `retiredHotPathWindowIDs` is delivered to
        // the orchestrator in Step 17 (retire-before-replace).
        let reconciled = try await reconcileBackfillWindows(
            fusionWindows,
            analysisAssetId: analysisAssetId
        )
        retiredHotPathWindowIDs = reconciled.retiredIDs
        if !reconciled.windows.isEmpty || !reconciled.retiredIDs.isEmpty {
            try await store.reconcileBackfillAdWindows(
                reconciled.windows,
                retiredIDs: reconciled.retiredIDs
            )
            logger.info(
                "Backfill: persisted \(reconciled.windows.count) fusion windows, retired \(reconciled.retiredIDs.count) superseded hot-path rows"
            )
        }

        // ── Step 15: MetadataExtractor ────────────────────────────────────────
        // Extract metadata for windows visible to the user (confirmed + candidate, not suppressed).
        // This set is also used by updatePriors and the coverage watermark below.
        let nonSuppressedWindows = fusionWindows.filter { $0.decisionState != AdDecisionState.suppressed.rawValue }
        for window in nonSuppressedWindows {
            try Task.checkCancellation()
            // playhead-hc7e: read the canonical transcript, not the raw mixed
            // fast/final array. The window's overlapping evidence text is joined
            // and handed to the metadata extractor; on a fully-covered ad window
            // the raw array carried BOTH the fast chunk and the overlapping final
            // chunk for the same audio, so the ad read was fed to the extractor
            // twice (stale fast text interleaved with the higher-quality final
            // text). `canonicalChunks` drops only fully-covered fast duplicates
            // and retains partial-overlap fast chunks, so no unique evidence is
            // lost — this keeps the "every consumer reads canonicalChunks"
            // invariant true for the metadata path as well.
            await extractAndPersistMetadata(window: window, chunks: canonicalChunks)
        }

        // ── Step 16: Event logging ────────────────────────────────────────────

        for event in decisionEvents {
            do {
                try await store.appendDecisionEvent(event)
            } catch {
                logger.warning("Backfill: appendDecisionEvent failed for window \(event.windowId): \(error.localizedDescription)")
            }
        }

        // ── Step 16.5: Persist the per-asset DecisionResultArtifact ──────────
        // Bug 6: prior to this block, `ad_decision_results` rows were never
        // written by production code (only by tests), even though every other
        // artifact of the backfill pipeline lands in SQLite. Building it here
        // — after `decision_events` are flushed and before SkipOrchestrator
        // forwarding — gives downstream eval/replay a single canonical roll-up
        // per asset that matches the events that were just persisted.
        //
        // The UNIQUE(analysisAssetId) constraint plus `INSERT OR REPLACE` in
        // `saveDecisionResultArtifact` makes this idempotent across re-runs
        // (e.g. cohort recomputes after a settings change). We persist even
        // when `fusionDecisionResults` is empty so that "no ads found" is
        // representable as `decisionJSON == "[]"` rather than a missing row;
        // a missing row is ambiguous (never analysed vs analysed-with-zero).
        //
        // L5 (skeptical-review-cycle-1): two distinct "empty" axes —
        //   1. `decodedSpans.isEmpty`     — upstream phases produced no
        //      anchored evidence at all. We treat this as "not analysed
        //      enough to summarise" and skip writing the row so a future
        //      run with richer transcript coverage can produce the
        //      canonical artifact.
        //   2. `fusionDecisionResults.isEmpty` — spans existed but fusion
        //      confirmed no ads. We DO write the row with `decisionJSON
        //      == "[]"` so downstream readers can distinguish
        //      analysed-with-zero from never-analysed.
        // The guard below is on (1), not (2) — the comment above describes
        // (2)'s behaviour inside the guard.
        if !decodedSpans.isEmpty {
            let inputArtifactRefs = (try? JSONEncoder().encode(fusionWindows.map(\.id)))
                .flatMap { String(data: $0, encoding: .utf8) } ?? "[]"
            let decisionJSON = (try? JSONEncoder().encode(
                fusionDecisionResults.map(PersistedDecisionResult.init(_:))
            )).flatMap { String(data: $0, encoding: .utf8) } ?? "[]"
            let artifact = DecisionResultArtifact(
                id: UUID().uuidString,
                analysisAssetId: analysisAssetId,
                decisionCohortJSON: assetCohortJSON,
                inputArtifactRefs: inputArtifactRefs,
                decisionJSON: decisionJSON,
                createdAt: Date().timeIntervalSince1970
            )
            do {
                try await store.saveDecisionResultArtifact(artifact)
            } catch {
                logger.warning("Backfill: saveDecisionResultArtifact failed for asset \(analysisAssetId): \(error.localizedDescription)")
            }
        }

        // ── Step 17: Forward eligible decisions to SkipOrchestrator ──────────
        // Phase 6.5 (playhead-4my.16): wires fusion output to the orchestrator so
        // Phase 7 (UserCorrections) has banner impressions + skip cues to correct.
        // The orchestrator guards on activeAssetId and eligibilityGate internally.
        if let orchestrator = skipOrchestrator {
            // playhead-xr3t: push the inventory-sanity-filter context
            // (declared chapters + episode duration) to the orchestrator
            // BEFORE forwarding the fusion decisions, so the filter has
            // the up-to-date context when each span is evaluated. The
            // orchestrator drops mismatched-asset pushes silently
            // (asset-switch race guard), so an out-of-order delivery
            // during an episode change is benign.
            //
            // We push unconditionally even when `fusionDecisionResults`
            // is empty: a future hot-path push using the AdWindow path
            // can still benefit from refreshed chapter / duration
            // state, and the setters are pure assignment + a guard
            // check (cheap).
            await orchestrator.setDeclaredChapters(
                assetChapterEvidence,
                analysisAssetId: analysisAssetId
            )
            await orchestrator.setEpisodeDuration(
                episodeDuration,
                analysisAssetId: analysisAssetId
            )

            // playhead-ud4n: retire-before-replace. Deliver the ids this
            // backfill retired to the orchestrator BEFORE forwarding the
            // replacement fusion decisions, mirroring the hot path's
            // retire→receive ordering. The DB rows were already hard-deleted in
            // the reconcile transaction above, so `AnalysisCoordinator.
            // finalizeBackfill`'s re-fetch and a future `beginEpisode` preload
            // can't resurrect them; this call also drops them from the live
            // in-memory window set so a superseded hot false-positive stops
            // auto-skipping in-session. Safe to deliver the full set:
            // `retireAdWindows` no-ops unknown ids and preserves only
            // user-terminal `.reverted` rows. Unconditional on
            // `fusionDecisionResults` — a clean backfill (no ads) still retires
            // a rejected hot candidate.
            if !retiredHotPathWindowIDs.isEmpty {
                await orchestrator.retireAdWindows(ids: retiredHotPathWindowIDs)
                logger.info("Backfill: retired \(retiredHotPathWindowIDs.count) superseded hot-path windows from orchestrator")
            }

            if !fusionDecisionResults.isEmpty {
                // Metadata extraction and pre-roll clamping can update producer
                // material after the in-memory decision was assembled. Re-read
                // the durable rows now so the orchestrator receives the exact
                // revision (including catalog provenance) its conditional
                // applied write will fence against.
                let persistedById: [String: AdWindow]
                do {
                    persistedById = Dictionary(
                        uniqueKeysWithValues: try await store
                            .fetchAdWindows(assetId: analysisAssetId)
                            .map { ($0.id, $0) }
                    )
                } catch {
                    persistedById = [:]
                    logger.warning(
                        "Backfill: producer revision reload failed for asset \(analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public)"
                    )
                }
                let liveDecisionResults = fusionDecisionResults.map {
                    $0.withProducerRevision(
                        persistedById[$0.id]
                    )
                }
                await orchestrator.receiveAdDecisionResults(
                    liveDecisionResults
                )
                let eligibleCount = fusionDecisionResults.filter { $0.eligibilityGate == .eligible }.count
                logger.info("Backfill: forwarded \(fusionDecisionResults.count) fusion results (\(eligibleCount) eligible) to SkipOrchestrator")
            }
        }

        // ── Step 18: Specialist mark composition (playhead-b6jq PR5) ──────────
        // Turn PR4's persisted raw `specialist_scan_results` into user-visible
        // MARK-ONLY banner marks. Hard-gated on `specialistMarkComposeEnabled`
        // (default OFF): flag-off short-circuits BEFORE any fetch/compose/write,
        // so the compose path is fully skipped and this backfill is byte-identical
        // to pre-PR5 (zero `ad_windows` writes, no banner). Runs BEFORE
        // `AnalysisCoordinator.finalizeBackfill`'s re-fetch so composed marks reach
        // the orchestrator in-session; a fresh `beginEpisode` preload picks them up
        // on cross-launch (confidence ≥ 0.70 auto-clears the preload floor).
        //
        // Compose reads only persisted rows + existing windows — no model re-run,
        // no FM coupling — so the composer is a standalone pure type. The version-
        // scoped `reconcileSpecialistMarks` (pinned to "specialist-ft-v2") cannot
        // clobber FM / user / shared marks, and content-addressed ids make a
        // recompose idempotent. Timing note (blueprint §9): on the FIRST analysis
        // the async scan job may not have drained yet, so this composes nothing and
        // marks appear on the next `beginEpisode`/re-analysis — acceptable for a
        // background-scan feature; the runner-tail compose site mitigates by
        // persisting marks the moment scan rows land.
        if config.specialistMarkComposeEnabled {
            let scanRows = try await store.fetchSpecialistScanResults(analysisAssetId: analysisAssetId)
            if !scanRows.isEmpty {
                let existingWindows = try await store.fetchAdWindows(assetId: analysisAssetId)
                let specialistMarks = SpecialistMarkComposer.compose(
                    scanRows: scanRows,
                    existingWindows: existingWindows,
                    analysisAssetId: analysisAssetId
                )
                let reconciledSpecialist = try await reconcileSpecialistMarks(
                    specialistMarks,
                    analysisAssetId: analysisAssetId
                )
                if !reconciledSpecialist.windows.isEmpty || !reconciledSpecialist.retiredIDs.isEmpty {
                    try await store.reconcileBackfillAdWindows(
                        reconciledSpecialist.windows,
                        retiredIDs: reconciledSpecialist.retiredIDs
                    )
                    logger.info(
                        "Backfill Step 18: composed \(reconciledSpecialist.windows.count) specialist mark-only windows, retired \(reconciledSpecialist.retiredIDs.count) stale specialist rows"
                    )
                }
            }
        }

        // ── Step 18b: ad-pod continuation (playhead-xsdz.65) ──────────────────
        // We catch ad #1 of a multi-ad pod and miss #2 and #3: measured against
        // rediff-confirmed (byte-derived) pod boundaries, 13 of the 17 slots the
        // pipeline detected at all carried an uncovered run longer than 30 s —
        // 994 s of ad audio inside pods we had ALREADY located, largest hole
        // 175 s. `AdPodContinuation` walks outward from each confirmed window
        // over regions where the vetted lexical auto-ad rule fires and stops at
        // a POSITIVE content-resumed barrier (FM `noAds` over good transcript,
        // or an explicit spoken return marker), emitting the recovered pod
        // material as NEW mark-only rows.
        //
        // Placed HERE, after specialist compose, for two reasons: the pass needs
        // the asset's final persisted window set (so it seeds off the reconciled
        // fusion geometry rather than a mid-pipeline snapshot), and being purely
        // additive it cannot perturb anything computed above.
        //
        // NOTHING EXISTING IS MODIFIED. No seed's geometry, gate, anchors or id
        // changes — so playhead-ye0n (never demote a whole window for extending
        // one edge) holds by construction, and playhead-2350's gate is untouched:
        // every emitted row is `markOnly` with both edge anchors `.unanchored`,
        // which can never auto-skip.
        //
        // Hard-gated on `podContinuationEnabled`, which since playhead-eks2
        // (2026-08-01) ships ON. The gate stays because it is the rollback: with
        // it off this short-circuits BEFORE any fetch/derivation/write and the
        // backfill is byte-identical to pre-xsdz.65.
        //
        // A `.confirmed` / `.applied` WINDOW SEEDS A CHAIN HERE — and so, since
        // playhead-evc1, does a STRICT day-0 byte-exact rediff mark, which is
        // persisted `.candidate` for life. That carve-out is scoped on
        // PROVENANCE (`AdPodContinuation.isDayZeroByteExactSeed`), not by
        // admitting `.candidate` wholesale: the aggregator's coarse 30 s
        // candidate tiles are exactly what the original filter was right to
        // exclude. It is what lets a pod whose only detected ad came from day-0
        // rediff — the FIRST-LISTEN population, and the only one that can be
        // covered before the listener presses play — recover its neighbours.
        //
        // A FAILURE HERE MUST NEVER COST THE BACKFILL. This is the last
        // additive step before the priors update and the coverage watermark, so
        // a throwing store read would abort a run that had already done all its
        // real work — a recall improvement is not worth losing a good analysis.
        // Errors are logged and swallowed; the only consequence is no
        // continuation marks this run, and a later run recomputes them from the
        // same inputs (content-addressed ids, so nothing churns).
        //
        // The protected-region read is INSIDE the same do-block deliberately: if
        // it fails we emit nothing at all, rather than proceeding with an empty
        // protected list and risking a mark over a listener's span.
        if config.podContinuationEnabled {
            do {
                let existingWindows = try await store.fetchAdWindows(assetId: analysisAssetId)
                // A LISTENER'S MARK IS OFF LIMITS (playhead-lc4c). `userMarked` rows
                // are persisted separately from the fusion set, so the walk must be
                // TOLD about them; it refuses to cross one. Read here rather than
                // reusing `protectedUserMarkedRegions` from the clamp because Step 18
                // may have written rows since.
                //
                // EVERY user-owned boundary state, not just `userMarked`: when the
                // listener ACCEPTS a suggest banner the orchestrator mints a
                // `userConfirmedSuggested` row, and a `userMarked`-only filter left
                // that span unwalled — so the next backfill re-claimed audio the
                // listener had already resolved.
                let protectedRegions = existingWindows
                    .filter {
                        AdPodContinuation.userOwnedBoundaryStates
                            .contains($0.boundaryState)
                    }
                    .map { (start: $0.startTime, end: $0.endTime) }
                // BOTH link sources, which is what the corpus eval measures and
                // what the calibration swept. The lexical co-occurrence carries the
                // DAI creatives that announce themselves; the rhetorical grammar
                // carries the ones whose only lexical trace is a bare URL (the
                // Conan pod's Carter's spot). On the held-out device lane the
                // lexical source alone recovered NOTHING — every mark came from the
                // grammar — so shipping only one of them would ship a pass that
                // does not fire on real episodes.
                let podLinks = AdPodContinuation.mergeLinks(
                    AdPodContinuation.adCopyLinks(
                        chunks: canonicalChunks,
                        hits: lexicalHits
                    ) + AdPodContinuation.rhetoricalLinks(chunks: canonicalChunks)
                )
                let podBarriers = AdPodContinuation.contentBarriers(
                    semanticScanResults: semanticScanResults,
                    lexicalHits: lexicalHits,
                    chunks: canonicalChunks
                )
                let continuationMarks = AdPodContinuation.compose(
                    existingWindows: existingWindows,
                    adCopyLinks: podLinks,
                    contentBarriers: podBarriers,
                    protectedRegions: protectedRegions,
                    episodeDuration: episodeDuration,
                    analysisAssetId: analysisAssetId
                )
                let reconciledContinuation = Self.reconcileVersionScopedMarkSets(
                    newMarks: continuationMarks,
                    existingWindows: existingWindows,
                    detectorVersion: AdPodContinuation.detectorVersion
                )
                if !reconciledContinuation.windows.isEmpty
                    || !reconciledContinuation.retiredIDs.isEmpty {
                    try await store.reconcileBackfillAdWindows(
                        reconciledContinuation.windows,
                        retiredIDs: reconciledContinuation.retiredIDs
                    )
                }
                logger.info(
                    "Backfill Step 18b [xsdz.65]: links=\(podLinks.count) barriers=\(podBarriers.count) protected=\(protectedRegions.count) composed=\(reconciledContinuation.windows.count) retired=\(reconciledContinuation.retiredIDs.count)"
                )
            } catch {
                logger.warning(
                    "Backfill Step 18b [xsdz.65] skipped for asset \(analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public)"
                )
            }
        }

        // ── Step 18c: semantic-sweep marks (playhead-y3ya) ────────────────────
        // FM already found ads on Dan's episode and they never reached him.
        // DE0784D8, 2026-08-01: `semantic_scan_results` carries `containsAd` for
        // 508–599 s and 1604–1731 s, and there is NO `ad_window` anywhere near
        // either. FM evidence enters fusion only through `buildFMLedgerEntries`,
        // which DECORATES existing `DecodedSpan`s and cannot create one; a coarse
        // window with no refined spans also proposes no region. So a sweep-lane
        // verdict with no narrow seed under it contributed to nothing — presence
        // without extent was discarded, in the one lane that did not honour the
        // portfolio's any-signal-fires-→-banner policy.
        //
        // Placed LAST among the additive producers, after specialist compose
        // (18) and pod continuation (18b), and that ordering is load-bearing:
        // the composer refuses to emit over ANY existing window, so running it
        // last means it fills only what every other producer left empty. That is
        // also what makes "the surface was not re-flooded" provable rather than
        // argued — the near-zero-confidence acoustic population is an INPUT to
        // the dedupe and nothing else, and no threshold, weight or gate moves.
        //
        // The marks are `markOnly` + `.candidate` + `.unanchored` on both edges
        // by construction, so playhead-2350's gate has nothing to demote and
        // playhead-ynmk makes a confirmation a MARK rather than a cut. The
        // worst case is a wrong banner, never lost show.
        //
        // Hard-gated on `semanticSweepMarkEnabled`: OFF short-circuits before any
        // read/compose/write and the backfill is byte-identical to pre-y3ya.
        // A FAILURE HERE MUST NEVER COST THE BACKFILL — same rule as 18b. This
        // is a recall lever running after all the real work; a throwing store
        // read is logged and swallowed, and the next run recomputes the same
        // marks from the same inputs (content-addressed ids, so nothing churns).
        // GATED ON THE EFFECTIVE FM MODE, not on the feature flag alone.
        // `ApprovedCohortRegistry` collapses an unapproved prompt / schema /
        // scan-plan / normalization / locale / appBuild cohort to `.shadow`, and
        // PlayheadRuntime's bootstrap calls that "exactly the protection the
        // registry was designed to provide". A banner composed from an
        // UNAPPROVED cohort's verdicts would defeat it — the whole point of
        // shadow is that its output is observed, never acted on.
        // `canProposeNewRegions` is the semantically exact predicate: a sweep
        // mark IS a new region proposed by FM, and `.rescoreOnly` means
        // "rescore what exists, propose nothing". Production is `.full`. This
        // also gives that capability bit its first production consumer — it had
        // none, which is part of why the sweep lane could never surface.
        if config.semanticSweepMarkEnabled,
           effectiveFMBackfillMode.canProposeNewRegions,
           !semanticScanResults.isEmpty {
            do {
                let existingWindows = try await store.fetchAdWindows(assetId: analysisAssetId)
                let sweepMarks = SemanticSweepMarkComposer.compose(
                    scanRows: semanticScanResults,
                    existingWindows: existingWindows,
                    // playhead-shu5: the geometry a coarse row's
                    // `supportLineRefs` are indices INTO. Segmented HERE from
                    // the same `atoms` the whole backfill reads, so the index
                    // and the rows it resolves can only ever disagree by
                    // TRANSCRIPT VERSION — which `resolve` checks, and which
                    // is the one way this could go silently wrong.
                    supportLines: SupportLineIndex(
                        segments: TranscriptSegmenter.segment(atoms: atoms),
                        transcriptVersion: transcriptVersion.transcriptVersion
                    ),
                    analysisAssetId: analysisAssetId
                )
                let reconciledSweep = Self.reconcileVersionScopedMarkSets(
                    newMarks: sweepMarks,
                    existingWindows: existingWindows,
                    detectorVersion: SemanticSweepMarkComposer.detectorVersion
                )
                if !reconciledSweep.windows.isEmpty
                    || !reconciledSweep.retiredIDs.isEmpty {
                    try await store.reconcileBackfillAdWindows(
                        reconciledSweep.windows,
                        retiredIDs: reconciledSweep.retiredIDs
                    )
                }
                logger.info(
                    "Backfill Step 18c [y3ya]: scanRows=\(semanticScanResults.count) composed=\(reconciledSweep.windows.count) retired=\(reconciledSweep.retiredIDs.count)"
                )
            } catch {
                logger.warning(
                    "Backfill Step 18c [y3ya] skipped for asset \(analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public)"
                )
            }
        }

        // ── Post-pipeline: priors + coverage watermark ────────────────────────

        if podcastId.isEmpty {
            logger.info("Backfill: skipping priors update — missing podcastId for asset \(analysisAssetId)")
        } else {
            // playhead-mn5e: the production caller `recordSuccessfulObservation`
            // never had. Ordered BEFORE `updatePriors` deliberately — see
            // `recordConfirmedWindowObservation` for why. It is one of the two
            // writers of `observationCount` (the other is the banner Yes), and
            // both claim the same episode row so the column counts EPISODES
            // rather than backfills or taps (playhead-2qz6 / playhead-fh5v).
            //
            // playhead-g7ln: the RESULT is now load-bearing rather than
            // discarded. It is the `trust_episode_observations` claim, and
            // `updatePriors` gates the ShowTraitProfile EMA on it so
            // `episodesObserved` counts episodes for the same reason and by
            // the same fact that `observationCount` does.
            let countedEpisodeObservation = await recordConfirmedWindowObservation(
                podcastId: podcastId,
                analysisAssetId: analysisAssetId,
                confirmedWindows: nonSuppressedWindows
            )
            try await updatePriors(
                podcastId: podcastId,
                nonSuppressedWindows: nonSuppressedWindows,
                episodeDuration: episodeDuration,
                featureWindows: featureWindows,
                chunks: canonicalChunks,
                countsAsEpisodeObservation: countedEpisodeObservation
            )
        }

        if let maxEnd = nonSuppressedWindows.map(\.endTime).max() {
            try await store.updateConfirmedAdCoverage(
                id: analysisAssetId,
                endTime: maxEnd
            )
        }

        logger.info("Backfill complete: spans=\(decodedSpans.count) fusion_windows=\(fusionWindows.count) decision_events=\(decisionEvents.count)")

        // playhead-gtt9.16: one-line acoustic-pipeline funnel summary per
        // episode. Emits the per-stage totals so gtt9.3 calibration can see
        // which features are producing signal and passing the fusion gate
        // without scraping per-window logs.
        let funnel = acousticPipelineResult.funnel
        logger.info(
            "Backfill acoustic-pipeline funnel: computed=\(funnel.total(.computed)) producedSignal=\(funnel.total(.producedSignal)) passedGate=\(funnel.total(.passedGate)) includedInFusion=\(funnel.total(.includedInFusion))"
        )

        // playhead-arf8: per-run bracket-refinement cascade counts. Greppable
        // marker `[arf8]` lets dogfood verify activation is firing and which
        // gate is shedding spans without scraping per-window logs.
        let arf8Counts = lastBracketRefinementCounts
        logger.info(
            "[arf8] backfill bracket counts: refined=\(arf8Counts.bracketRefined) noBracket=\(arf8Counts.noBracket) trustGated=\(arf8Counts.trustGated) coarseGated=\(arf8Counts.coarseGated) fineGated=\(arf8Counts.fineConfidenceGated) legacyBypass=\(arf8Counts.legacyBypass) showTrust=\(String(format: "%.2f", bracketShowTrust))"
        )

        // playhead-2hpn (write path): once per episode, after all spans
        // have been scored and persisted, push this episode's intro/outro
        // jingle hashes into the show's profile. Gated by the same
        // conditions as the read path so flag-OFF behavior is byte
        // identical to pre-2hpn (no store mutation, no allocation).
        //
        // Cross-show isolation: `podcastId` is the bead-spec "per show"
        // key — the store records this outcome ONLY against that show's
        // row. Show A's profile is never touched by Show B's runBackfill.
        //
        // We log a single `[2hpn]` marker so dogfood can grep activation
        // and see the post-update confirmation count / miss count without
        // scraping per-window logs.
        //
        // h6a6 R1 review fix: hoist `updatedMusicBedSnapshot` so the
        // downstream h6a6 write can consume the POST-2hpn-write
        // `isConfirmed` (rather than the stale `showMusicBedSnapshot`
        // captured at the TOP of runBackfill). Without this hoist, the
        // episode that flipped 2hpn from unconfirmed→confirmed would
        // record `musicBedConfirmed=false` against h6a6, and the user
        // would only see `.musicBedReliable` observed on the FOLLOWING
        // episode's backfill — surfacing the classification one episode
        // later than the 2hpn state would suggest.
        var updatedMusicBedSnapshot: ShowMusicBedProfileSnapshot?
        if scopedMusicBedEnabled,
           !podcastId.isEmpty,
           let store = showMusicBedProfileStore,
           episodeDuration > 0 {
            let outcome = ShowMusicBedProfileEvaluator.extractEpisodeJingleHashes(
                featureWindows: featureWindows,
                episodeDuration: episodeDuration
            )
            let updated = await store.recordEpisodeOutcome(
                showIdentifier: podcastId,
                outcome: outcome,
                now: Date()
            )
            updatedMusicBedSnapshot = updated
            logger.info(
                "[2hpn] show=\(podcastId, privacy: .public) confirmed=\(updated.isConfirmed) confirmationCount=\(updated.confirmationCount) missCount=\(updated.consecutiveMissCount) storedHashes=\(updated.confirmedJingleHashes.count)"
            )
        }

        // playhead-h6a6 (write path): once per episode, after all
        // spans have been scored and persisted, push this episode's
        // capability observations into the show's profile. Gated by
        // the same `showCapabilityProfilesEnabled` flag + non-empty
        // `podcastId` + installed store as the read path so flag-OFF
        // behavior is byte-identical to pre-h6a6 (no store mutation,
        // no allocation). The SLI gate is consulted INSIDE the
        // evaluator's `classify(...)` — passing the closure here
        // gives the evaluator access to it without coupling the
        // store to the SLI ledger directly.
        //
        // The capability observations for THIS bead are minimal: we
        // record the all-false sentinel outcome
        // (`ShowCapabilityEpisodeOutcome.nothingObserved`) plus the
        // music-bed confirmation signal threaded from the POST-2hpn-
        // write snapshot (falling back to the pre-write snapshot when
        // the 2hpn write didn't happen — flag off, store missing, or
        // zero-duration episode — so the read still reflects the
        // freshest available 2hpn state). The chapter-matched /
        // host-voiced / sponsor-declared / dynamic-insertion-shift
        // signals are wired by follow-on beads as those producers
        // stabilize their outputs; the schema is in place today so
        // adding the producers does NOT require a migration. The
        // activation-floor and SLI-gate contracts are already enforced
        // by `ShowCapabilityProfileEvaluator.classify(...)`, so the
        // music-bed-reliable predicate is the live observation path
        // exercised end-to-end as of this bead.
        //
        // h6a6 R1 review fix: prefer `updatedMusicBedSnapshot?
        // .isConfirmed` over `showMusicBedSnapshot?.isConfirmed`. The
        // top-of-backfill snapshot is the PRE-write value; the
        // post-write value is the one the user-visible 2hpn state
        // would surface. Using the stale value would surface the
        // capability profile one episode late on the transition.
        if capabilityProfilesEnabled,
           !podcastId.isEmpty,
           let capabilityStore = showCapabilityProfileStore {
            let outcome = ShowCapabilityEpisodeOutcome.nothingObserved
            let musicBedConfirmed =
                updatedMusicBedSnapshot?.isConfirmed
                ?? showMusicBedSnapshot?.isConfirmed
                ?? false
            let gate = capabilityProfileSLIGate
            let updated = await capabilityStore.recordEpisodeOutcome(
                showIdentifier: podcastId,
                outcome: outcome,
                musicBedConfirmed: musicBedConfirmed,
                sliGate: gate,
                now: Date()
            )
            logger.info(
                "[h6a6] show=\(podcastId, privacy: .public) kind=\(updated.kind.rawValue, privacy: .public) completed=\(updated.completedEpisodeCount) musicBedConfirmed=\(musicBedConfirmed)"
            )
        }

        // playhead-zx6i — Success stamp for the B4 revalidation
        // short-circuit. ONLY runs when the flag is ON; flag-OFF
        // behavior is byte-identical to pre-zx6i (no UserDefaults
        // write, no allocation). The stamp records that this asset's
        // persisted `AdWindow` / classifier / feature rows are
        // up-to-date with the current `PipelineVersions` triple. On
        // the next `AnalysisJobRunner.run` for this asset, if the
        // current triple still matches, the runner takes the existing
        // skip-hot-path / skip-backfill no-op branches; if the triple
        // has bumped, the runner takes the revalidation short-circuit
        // (skipping decode / features / ASR) and routes through
        // `revalidateFromFeatures`. Stamped only here — at the end of
        // a successful `runBackfill`, after every stage wrote — so a
        // failure that returns/throws earlier leaves the prior stamp
        // (or absent state) intact and the next run will redo the
        // work rather than incorrectly trust an aborted run.
        //
        // R1 doc audit fix: re-load the flag LIVE here instead of
        // reading the init-time `preAnalysisConfig` snapshot.
        // `AnalysisJobRunner` reads its flag live on every `run(_:)`
        // (see `b4RevalidationEnabledProvider`'s doc comment), so the
        // producer (this stamp write) and the consumer (the short-
        // circuit) must agree on the live value. Otherwise a
        // mid-session flag-ON would unlock the consumer but leave the
        // producer's cached `false` in place, so no stamp would ever
        // be written and the feature would silently never activate
        // until the next app launch. The other `preAnalysisConfig`
        // consumers (2hpn `scopedMusicBedGeneralization` at line 2123)
        // keep their snapshot-at-init semantics — only the zx6i flag
        // routes through the live read here.
        if PreAnalysisConfig.load().b4RevalidationFromFeaturesEnabled {
            RevalidationStateStore.recordCompleted(
                versions: PipelineVersions.current(),
                forAsset: analysisAssetId
            )
        }
    }

    /// playhead-zx6i — B4 fast revalidation entry point.
    ///
    /// Fetches the persisted `TranscriptChunk` rows for `analysisAssetId`
    /// and delegates to `runBackfill`, which already (a) accepts chunks
    /// as a parameter rather than re-running ASR, and (b) fetches its
    /// own `FeatureWindow` rows from the store internally. The
    /// short-circuit therefore comes from the CALLER (the
    /// `AnalysisJobRunner.run` branch that skips stages 1–3); this
    /// method's job is to provide a clean named entry point so the
    /// stub-based unit tests can assert "the runner took the
    /// revalidation path, not the full-analysis path" without grovelling
    /// through `runBackfill` call-site internals.
    ///
    /// If persisted chunks are empty the call returns without doing
    /// any classifier work — the caller's gate already checked for
    /// non-empty chunks, but defense-in-depth here keeps a
    /// chunk-races-deletion path from triggering a no-op classifier
    /// sweep. The `runBackfill` body's own `guard !chunks.isEmpty` is
    /// the structural fallback.
    func revalidateFromFeatures(
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double,
        sessionId: String? = nil
    ) async throws {
        let chunks = try await store.fetchTranscriptChunks(assetId: analysisAssetId)
        guard !chunks.isEmpty else {
            logger.info("Revalidation: skipping — no persisted chunks for asset \(analysisAssetId)")
            return
        }
        logger.info("Revalidation: re-running classifier+fusion+boundary over \(chunks.count) persisted chunks for asset \(analysisAssetId)")
        try await runBackfill(
            chunks: chunks,
            analysisAssetId: analysisAssetId,
            podcastId: podcastId,
            episodeDuration: episodeDuration,
            sessionId: sessionId
        )
    }

    // MARK: - ChapterGenerationPhase wire-up (playhead-au2v.1.13)

    /// Invoke the chapter-generation phase from `runBackfill` when a
    /// production factory has been wired in. The mode gate is checked
    /// at the call site (only `.shadow` and `.enabled` reach this
    /// helper); the cache short-circuit, factory presence, and outcome
    /// logging live here.
    ///
    /// `transcriptVersion` is the same hash `TranscriptAtomizer.atomize`
    /// computed for the current final-pass atom set. We use it as the
    /// cache key for the short-circuit on the assumption that production
    /// wiring installs a `TranscriptHashProviding` whose output matches
    /// `transcriptVersion` for the same atom set — that contract is what
    /// makes "valid plan exists for content hash → skip phase" coherent.
    ///
    /// Tests intentionally diverge the two (e.g. by injecting a
    /// `.race(entry:recheck:)` or `.unavailable` hash provider that the
    /// service-level short-circuit knows nothing about) to exercise the
    /// phase's internal protections in isolation; that's a feature of
    /// the test seam, not a contract violation. Production wiring is
    /// responsible for keeping the two keys aligned (see runtime bead).
    ///
    /// Outcomes are logged via the actor's `logger` and never thrown:
    /// the phase already emits structured `ChapterPhaseEvent`s for each
    /// terminal state, so a re-throw here would force every backfill
    /// caller to add a `try` site for what is, in product terms, an
    /// optional signal generator. Cancellation is the one exception: a
    /// `Task.isCancelled` check after this helper returns lets upstream
    /// callers observe cancellation through the existing `try`
    /// machinery in `runBackfill` (the caller-side `try
    /// Task.checkCancellation()` handles propagation; the phase itself
    /// returns `.preempted` and never raises).
    ///
    /// `transcriptVersion` empty (e.g. no atoms produced) is a no-op:
    /// the hash isn't a useful cache key in that case and the phase's
    /// `TranscriptHashProviding` would itself surface
    /// `.transcriptUnavailable`. Bailing here saves the round-trip.
    private func runChapterGenerationPhaseIfWired(
        analysisAssetId: String,
        transcriptVersion: String
    ) async {
        // Cancellation pre-check. The call site re-checks
        // `Task.checkCancellation()` after this helper returns, but if
        // the parent task was already cancelled when we got here, we
        // would otherwise still pay for the factory build, the cache
        // get, and the phase entry just to have the phase observe
        // cancellation and return `.preempted`. Bailing now skips that
        // wasted work and matches the "honor cancellation early" pattern
        // used elsewhere in `runBackfill`.
        guard !Task.isCancelled else {
            logger.debug(
                "chapterphase.backfill_wireup: cancellation observed at entry for asset=\(analysisAssetId, privacy: .public) — skipping phase"
            )
            return
        }

        // Empty transcript hash → nothing to cache against, nothing
        // for the phase to anchor on. Logged at `.debug` so dogfood
        // OS log searches can confirm the gate fired without polluting
        // the default-level logs.
        guard !transcriptVersion.isEmpty else {
            logger.debug(
                "chapterphase.backfill_wireup: empty transcriptVersion for asset=\(analysisAssetId, privacy: .public) — skipping phase"
            )
            return
        }

        // Factory not wired → mode is on but no production dependencies
        // exist yet. Mode-gate ensures we only get here on `.shadow` or
        // `.enabled`; logging at `.debug` keeps the gap observable
        // without alarming dogfood reports.
        guard let factory = chapterGenerationPhaseFactory else {
            logger.debug(
                "chapterphase.backfill_wireup: mode=\(self.config.chapterSignalMode.rawValue, privacy: .public) but no phase factory wired — skipping"
            )
            return
        }

        // Cache short-circuit. A non-nil cache + a hit on the current
        // content hash is a "fresh plan already on disk" signal — we
        // refuse to pay FM cost re-running the phase for an unchanged
        // input. The cache returns `nil` on schema mismatch / decode
        // failure / missing file (all treated as misses by
        // `ChapterPlanCache.get`), which correctly falls through to
        // re-running the phase. We deliberately do NOT emit a
        // diagnostic for the cache hit — the original phase run that
        // produced the plan already emitted `chapter_phase_completed`,
        // and re-emitting on every replay would over-count plan
        // generations in eval. The phase's own internal cache write
        // path is unchanged.
        if let cache = chapterPlanCache {
            if let cachedPlan = await cache.get(contentHash: transcriptVersion) {
                logger.debug(
                    "chapterphase.backfill_wireup: cache hit for asset=\(analysisAssetId, privacy: .public) hash=\(transcriptVersion, privacy: .public) chapters=\(cachedPlan.chapters.count, privacy: .public) — skipping phase"
                )
                return
            }
        }

        // Cache miss (or no cache wired) → invoke the phase. Production
        // wiring rebuilds the phase per call so the
        // `TranscriptHashProviding` it captures observes the
        // most-recently-published transcript hash.
        //
        // Resolve the actual episode identifier from the analysis asset.
        // The phase API takes `episodeId: String` and uses it as (a) the
        // key for `creatorChapterProvider.creatorChapters(episodeId:)`
        // — production providers look up creator chapters by EPISODE,
        // not by re-analysis ASSET — and (b) the privacy-locked input
        // to `EpisodeIdHasher.hash(installID:episodeId:)`, which must
        // hash the same episode across re-analysis attempts so dogfood
        // diagnostics correlate events from the same listener session.
        // Passing `analysisAssetId` here would break both: the provider
        // would miss real chapters, and the hash would shift per
        // re-analysis. Falls back to `analysisAssetId` only when the
        // store lookup fails (deleted asset row, transient SQLite
        // error) — better to emit a phase event keyed to the asset
        // than to skip the phase entirely. The fallback is logged so
        // dogfood bundles surface the lookup miss.
        let resolvedEpisodeId: String
        do {
            if let asset = try await store.fetchAsset(id: analysisAssetId) {
                resolvedEpisodeId = asset.episodeId
            } else {
                logger.notice(
                    "chapterphase.backfill_wireup: asset row missing for asset=\(analysisAssetId, privacy: .public) — falling back to analysisAssetId as episodeId"
                )
                resolvedEpisodeId = analysisAssetId
            }
        } catch {
            logger.error(
                "chapterphase.backfill_wireup: asset lookup failed for asset=\(analysisAssetId, privacy: .public) — falling back to analysisAssetId as episodeId, error=\(String(describing: error), privacy: .public)"
            )
            resolvedEpisodeId = analysisAssetId
        }

        let phase = factory()
        let installID = chapterPhaseInstallIDProvider()
        let outcome = await phase.run(
            mode: config.chapterSignalMode,
            episodeId: resolvedEpisodeId,
            installID: installID
        )

        // One-line outcome log per run. The structured event payload is
        // already emitted by the phase via its `ChapterPhaseEventSink`;
        // this string is for greppable dogfood diagnostics.
        switch outcome {
        case .modeOff:
            // Unreachable here — call site checks
            // `runsChapterGeneration` before invoking the helper. Logged
            // at `.error` because reaching this branch indicates the
            // phase's internal mode handling diverged from the gate's
            // predicate (a bug in either the phase or in
            // `ChapterSignalMode.runsChapterGeneration`). The error log
            // gives a single greppable signal during dogfood without
            // crashing release builds; in DEBUG we trip an
            // `assertionFailure` so the desync is loud at test time.
            logger.error(
                "chapterphase.backfill_wireup: unexpected modeOff outcome for asset=\(analysisAssetId, privacy: .public) — phase/gate desync"
            )
            assertionFailure(
                "chapterphase.backfill_wireup: phase returned .modeOff while ChapterSignalMode.runsChapterGeneration was true — gate/phase desync"
            )
        case .admissionDenied(let reason):
            logger.notice(
                "chapterphase.backfill_wireup: admission denied for asset=\(analysisAssetId, privacy: .public) reason=\(reason, privacy: .public)"
            )
        case .skippedCreatorChapters(let creatorChapterCount):
            logger.notice(
                "chapterphase.backfill_wireup: skipped — \(creatorChapterCount, privacy: .public) creator chapters present for asset=\(analysisAssetId, privacy: .public)"
            )
        case .noCandidates:
            logger.notice(
                "chapterphase.backfill_wireup: no candidates for asset=\(analysisAssetId, privacy: .public)"
            )
        case .transcriptUnavailable:
            logger.notice(
                "chapterphase.backfill_wireup: transcript unavailable for asset=\(analysisAssetId, privacy: .public)"
            )
        case .raceAborted:
            logger.notice(
                "chapterphase.backfill_wireup: transcript race for asset=\(analysisAssetId, privacy: .public) — plan discarded"
            )
        case .preempted:
            logger.notice(
                "chapterphase.backfill_wireup: preempted for asset=\(analysisAssetId, privacy: .public)"
            )
        case .operationalRateExceeded(let rate, let threshold):
            logger.notice(
                "chapterphase.backfill_wireup: op-rate exceeded for asset=\(analysisAssetId, privacy: .public) rate=\(rate, privacy: .public) threshold=\(threshold, privacy: .public)"
            )
        case .cached(let chapterCount, let planConfidence):
            // Phase-level `.cached` is the SUCCESS terminal: the phase
            // ran end-to-end, generated a plan, and persisted it via
            // its internal cache write. Not to be confused with the
            // service-level cache short-circuit above, which logs at
            // `.debug` ("cache hit … skipping phase") because no phase
            // run occurred.
            logger.info(
                "chapterphase.backfill_wireup: plan generated and cached for asset=\(analysisAssetId, privacy: .public) chapters=\(chapterCount, privacy: .public) confidence=\(planConfidence, privacy: .public)"
            )
        }
    }

    // MARK: - Fusion Evidence Construction (playhead-4my.6.4)

    /// Build an evidence ledger for a single DecodedSpan by gathering contributions
    /// from all available evidence sources. The ledger is consumed by DecisionMapper.
    ///
    /// Evidence sources:
    ///   - classifier: best-matching ClassifierResult for the span's time range
    ///   - fm: SemanticScanResults overlapping the span (positive-only: containsAd)
    ///   - lexical: LexicalCandidates overlapping the span
    ///   - acoustic: FeatureWindows in the span with energy-transition signals
    ///   - catalog: EvidenceCatalog entries overlapping the span
    /// playhead-xsdz.9: load the normalized token sequences of ACTIVE
    /// confirmed-ad fingerprint entries for a show, for Smith-Waterman positive
    /// alignment. Only `active` entries (the matcher-eligible set) are read, and
    /// only sequences long enough to align meaningfully are kept. Bounded cost:
    /// active entries per show are LRU-capped by `AdCopyFingerprintStore`. Read
    /// failures are logged and swallowed (returns `[]`) so a store hiccup never
    /// breaks the hot-path decision — same defensive posture as the catalog
    /// egress read.
    private func loadConfirmedAdSequences(
        store: AdCopyFingerprintStore,
        podcastId: String?
    ) async -> [[String]] {
        guard let podcastId, !podcastId.isEmpty else { return [] }
        do {
            let entries = try await store.activeEntries(forPodcast: podcastId)
            return entries.compactMap { entry -> [String]? in
                let tokens = SmithWatermanAligner.tokenize(entry.normalizedText)
                return tokens.count >= NegativeFingerprintBank.minTokenCount ? tokens : nil
            }
        } catch {
            logger.debug("loadConfirmedAdSequences: read failed (non-fatal): \(String(describing: error), privacy: .public)")
            return []
        }
    }

    // MARK: - playhead-xsdz.13: cross-show sponsor-entity extraction

    /// Extract the distinct NORMALIZED sponsor entities for the whole episode
    /// (the WRITE set), each paired with a derived confidence in `[0, 1]`.
    ///
    /// The sponsor ENTITY is the `.brandSpan` evidence category — the brand-like
    /// proper-noun span `EvidenceCatalogBuilder` only extracts inside COMMERCIAL
    /// CONTEXT (within ±2 atoms of a URL / promo code / disclosure anchor), so a
    /// brand span is already a vetted "a sponsor was detected here" signal. Its
    /// `normalizedText` IS the same normalization (`EvidenceCatalogBuilder.normalize`,
    /// lowercased + NFKC + control/format-stripped + trimmed) that backs
    /// `SponsorKnowledgeEntry.normalizedValue`, so "BetterHelp" / "better help"
    /// collapse to one cross-show key.
    ///
    /// Confidence is derived deterministically from repetition: a single mention
    /// just clears the write bar (0.5); each additional repeat raises it,
    /// clamped to 1.0. When an entity appears under multiple refs, the strongest
    /// (highest-count) is kept. Pure / static so tests can exercise it directly.
    static func crossShowSponsorObservations(
        from entries: [EvidenceEntry]
    ) -> [(normalizedEntity: String, confidence: Double)] {
        var bestConfidenceByEntity: [String: Double] = [:]
        var order: [String] = []
        for entry in entries where entry.category == .brandSpan {
            let entity = entry.normalizedText.trimmingCharacters(in: .whitespacesAndNewlines)
            guard entity.count >= CrossShowSyndicationStore.minEntityLength else { continue }
            let confidence = Swift.min(1.0, 0.5 + 0.1 * Double(Swift.max(0, entry.count - 1)))
            if let existing = bestConfidenceByEntity[entity] {
                if confidence > existing { bestConfidenceByEntity[entity] = confidence }
            } else {
                bestConfidenceByEntity[entity] = confidence
                order.append(entity)
            }
        }
        // Deterministic order: first appearance in the catalog's ref ordering.
        return order.map { ($0, bestConfidenceByEntity[$0] ?? 0.5) }
    }

    /// Extract the distinct NORMALIZED sponsor entities THIS span can hear (the
    /// per-span READ set). Same `.brandSpan` source and normalization as the
    /// write set; deterministically ordered by first appearance. Pure / static
    /// for direct unit testing.
    ///
    /// playhead-0u3e. This used to select on
    /// `coverageStartTime`/`coverageEndTime`, the HULL between a deduplicated
    /// entry's earliest and latest mention, so a brand read twice was in the
    /// read set of every span between its two reads and could earn that span a
    /// capped `.crossShowSyndication` ledger boost from a sponsor it never
    /// mentioned. The channel is OFF by default
    /// (``AdDetectionConfig/crossShowSyndicationEnabled``), so this was latent
    /// rather than live — it is fixed in the same change as
    /// ``buildCatalogLedgerEntries(span:entries:fusionConfig:)`` precisely so
    /// the flag cannot later be turned on onto the old reading.
    ///
    /// Same primitive, same reason: one window, at most one claim per
    /// `evidenceRef`. The set is deduplicated by entity anyway, so the choice
    /// shows up as WHICH spans see an entity, not how many times.
    static func crossShowSponsorEntities(
        from entries: [EvidenceEntry],
        overlapping span: DecodedSpan
    ) -> [String] {
        var seen: Set<String> = []
        var out: [String] = []
        for entry in entries where entry.category == .brandSpan {
            // Which MENTION this span can hear — not the hull between the first
            // and the last.
            guard entry.locatedInTimeWindow(
                start: span.startTime,
                end: span.endTime
            ) != nil else { continue }
            let entity = entry.normalizedText.trimmingCharacters(in: .whitespacesAndNewlines)
            guard entity.count >= CrossShowSyndicationStore.minEntityLength else { continue }
            if seen.insert(entity).inserted { out.append(entity) }
        }
        return out
    }

    // MARK: - Splice-slot ownership pass (playhead-xsdz.20)

    /// Post-decode splice-slot ownership pass. TWO-PHASE by construction (the
    /// negative bank is an actor): (i) run pass 1 (the pure resolver) for every
    /// span; (ii) await the bank ONCE to build a per-span verdict table (core +
    /// slot token matches); (iii) invoke the pure `SpliceSlotDispositionEngine`
    /// (passes 2–4). Then materialize the dispositions via the pure
    /// `SpliceSlotRewriter` (pass 5), reconcile persistence (delete superseded /
    /// absorbed rows, upsert the rewritten set), and return the rewritten spans
    /// for the boundary-refine loop. Only invoked when
    /// `config.spliceSlotOwnershipEnabled`.
    ///
    /// Bead C's shadow consumes the SAME engine + verdict-table inputs, so the
    /// eval's `shadow == flag-ON` identity holds by construction.
    private func applySpliceSlotOwnershipPass(
        decodedSpans: [DecodedSpan],
        atoms: [TranscriptAtom],
        atomEvidence: [AtomEvidence],
        featureWindows: [FeatureWindow],
        acousticBreaks: [AcousticBreak],
        analysisAssetId: String,
        showId: String?
    ) async -> [DecodedSpan] {
        guard !decodedSpans.isEmpty else { return decodedSpans }

        // Phases (i)–(iii): the SHARED resolver + negative-bank verdict table +
        // pure `SpliceSlotDispositionEngine`. Bead C's shadow consumes the exact
        // same helper, so `shadow == flag-ON` holds by construction.
        let computation = await computeSpliceSlotPass(
            decodedSpans: decodedSpans,
            atoms: atoms,
            atomEvidence: atomEvidence,
            featureWindows: featureWindows,
            acousticBreaks: acousticBreaks,
            showId: showId
        )
        let result = computation.result

        // Pass 5: materialize the dispositions (pure rewrite).
        let rewrite = SpliceSlotRewriter.apply(
            decodedSpans: decodedSpans,
            dispositions: result.dispositions,
            atomEvidence: atomEvidence
        )

        let keptCount = result.dispositions.filter {
            if case .keepSlot = $0 { return true } else { return false }
        }.count
        if keptCount > 0 || !rewrite.absorbedIds.isEmpty {
            logger.info(
                "[xsdz.20] slot pass asset=\(analysisAssetId, privacy: .public) spans=\(decodedSpans.count) kept=\(keptCount) absorbed=\(rewrite.absorbedIds.count) rounds=\(result.fixpointRounds) absorbedIds=\(rewrite.absorbedIds, privacy: .public)"
            )
        }

        // Reconcile persistence: delete superseded (changed-id) + absorbed rows,
        // then upsert the rewritten set (INSERT OR REPLACE overwrites unchanged
        // and slot-owned rows in place). Absorbed spans are never re-upserted.
        do {
            try await store.deleteDecodedSpans(ids: rewrite.supersededIds)
            if !rewrite.finalSpans.isEmpty {
                try await store.upsertDecodedSpans(rewrite.finalSpans)
            }
        } catch {
            logger.warning("[xsdz.20] slot-pass re-persist failed: \(error.localizedDescription)")
        }

        return rewrite.finalSpans
    }

    /// The shared output of phases (i)–(iii): the per-span resolved slot +
    /// diagnostics, the engine candidates, and the disposition result. Both the
    /// flag-ON ownership pass and the flag-ON-OFF / shadow-ON shadow pass consume
    /// this so the shadow's would-be dispositions are bit-for-bit the flag-ON
    /// dispositions (bead C's `shadow == flag-ON` identity, by construction).
    struct SpliceSlotPassComputation: Sendable {
        let resolvedSlots: [SpliceSlot?]
        let diagnostics: [SpliceSlotDiagnostics]
        let candidates: [SpliceSlotCandidate]
        let result: SpliceSlotDispositionResult
    }

    /// Phases (i)–(iii) of the splice-slot pass, index-aligned with `decodedSpans`.
    /// Resolves every span's would-be slot WITH diagnostics (the shadow needs the
    /// resolver failure reason / champion for its slot-field sourcing; discarding
    /// the diagnostics is byte-identical to the previous `resolve` call), awaits
    /// the negative-bank verdict table ONCE, builds the candidates, and runs the
    /// pure disposition engine. Side-effect-free apart from the awaited bank reads.
    private func computeSpliceSlotPass(
        decodedSpans: [DecodedSpan],
        atoms: [TranscriptAtom],
        atomEvidence: [AtomEvidence],
        featureWindows: [FeatureWindow],
        acousticBreaks: [AcousticBreak],
        showId: String?
    ) async -> SpliceSlotPassComputation {
        // Episode-wide vetoed time ranges (atoms the user marked "not an ad").
        // EMPTY unless the read side is flag-ON (playhead-xsdz.34,
        // `userCorrectionReadSideEnabled`) AND an explicit `.falsePositive` veto
        // masked an atom via `StoreBackedCorrectionMaskProvider`; flag-OFF (the
        // production default) leaves this empty and the resolver's
        // `.vetoNewlyEnclosed` gate dormant, exercised only by synthetic vetoes
        // in tests. Rediff applies the SAME set via its own §5 gate.
        let vetoedRanges: [TimeRange] = atomEvidence
            .filter { $0.correctionMask == .userVetoed }
            .map { TimeRange(start: $0.startTime, end: $0.endTime) }

        // Phase (i): pass 1 — resolve every span's would-be slot (with diagnostics).
        let resolver = SpliceSlotResolver()
        let sortedAtomEvidence = atomEvidence.sorted { $0.atomOrdinal < $1.atomOrdinal }
        let resolved: [(slot: SpliceSlot?, diagnostics: SpliceSlotDiagnostics)] = decodedSpans.map { span in
            resolver.resolveWithDiagnostics(
                core: TimeRange(start: span.startTime, end: span.endTime),
                vetoedRanges: vetoedRanges,
                breaks: acousticBreaks,
                episodeWindows: featureWindows
            )
        }
        let resolvedSlots: [SpliceSlot?] = resolved.map(\.slot)
        let diagnostics: [SpliceSlotDiagnostics] = resolved.map(\.diagnostics)

        // Whether each resolved slot's interval intersects ≥ 1 atom (the
        // pre-pass-3 empty-atom-set disqualification input). Uses the atom
        // stream (positive-duration interval intersection).
        func slotIntersectsAtoms(_ slot: SpliceSlot) -> Bool {
            let slotRange = TimeRange(start: slot.startTime, end: slot.endTime)
            return sortedAtomEvidence.contains {
                TimeRange(start: $0.startTime, end: $0.endTime).intersects(slotRange)
            }
        }

        // Phase (ii): await the negative bank ONCE for the verdict table. Only
        // when the cross-episode-memory flag is on AND a bank is wired; otherwise
        // every verdict is `false` (dormant), matching the OFF path. Shared with
        // the rediff pass via `negativeBankVerdicts` (identical values).
        let (coreMatch, slotMatch) = await negativeBankVerdicts(
            decodedSpans: decodedSpans,
            atoms: atoms,
            resolvedSlots: resolvedSlots,
            showId: showId
        )

        // Phase (iii): the pure disposition engine (passes 2–4).
        let candidates: [SpliceSlotCandidate] = decodedSpans.enumerated().map { i, span in
            let slot = resolvedSlots[i]
            return SpliceSlotCandidate(
                mintedInterval: TimeRange(start: span.startTime, end: span.endTime),
                slot: slot,
                slotIntersectsAtoms: slot.map(slotIntersectsAtoms) ?? true,
                coreBankMatch: coreMatch[i],
                slotBankMatch: slotMatch[i]
            )
        }
        let result = SpliceSlotDispositionEngine.computeDispositions(candidates)

        return SpliceSlotPassComputation(
            resolvedSlots: resolvedSlots,
            diagnostics: diagnostics,
            candidates: candidates,
            result: result
        )
    }

    /// playhead-xsdz.21 (Bead C): the OWNERSHIP-OFF shadow pass. Invoked from
    /// `runBackfill` ONLY when `spliceSlotShadowEnabled == true` AND
    /// `spliceSlotOwnershipEnabled == false`. Computes the would-be dispositions
    /// via the SAME `computeSpliceSlotPass` helper the flag-ON path uses, then —
    /// WITHOUT applying any rewrite — emits exactly one frozen v3
    /// `spliceslot.shadow` breadcrumb per span and records the structured rows to
    /// the injected observer (if any). Never mutates `decodedSpans`, never
    /// touches persistence, never stamps `.spliceSlot` provenance, so the
    /// production decision path stays byte-identical to the flag-OFF pipeline.
    private func runSpliceSlotShadowPass(
        decodedSpans: [DecodedSpan],
        atoms: [TranscriptAtom],
        atomEvidence: [AtomEvidence],
        featureWindows: [FeatureWindow],
        acousticBreaks: [AcousticBreak],
        analysisAssetId: String,
        showId: String?
    ) async {
        guard !decodedSpans.isEmpty else { return }

        let computation = await computeSpliceSlotPass(
            decodedSpans: decodedSpans,
            atoms: atoms,
            atomEvidence: atomEvidence,
            featureWindows: featureWindows,
            acousticBreaks: acousticBreaks,
            showId: showId
        )

        let rows = SpliceSlotShadowRowBuilder.makeRows(
            assetId: analysisAssetId,
            spanIds: decodedSpans.map(\.id),
            candidates: computation.candidates,
            diagnostics: computation.diagnostics,
            dispositions: computation.result.dispositions
        )

        // One breadcrumb per span (subsystem com.playhead; precedent
        // fm.coarse.run_budget). Non-qualifying spans emit too.
        for row in rows {
            logger.notice("\(SpliceSlotShadowBreadcrumb.format(row), privacy: .public)")
        }

        if let observer = spliceSlotShadowObserver {
            await observer.record(rows, assetId: analysisAssetId)
        }
    }

    // MARK: - Rediff-slot ownership pass (playhead-xsdz.29)

    /// The shared output of the rediff slot pass: per-span candidates (with the
    /// synthesized rediff slot), the resolver-parity diagnostics, and the
    /// disposition result. Both the flag-ON ownership pass and the shadow pass
    /// consume this, so the shadow's would-be dispositions are the flag-ON
    /// dispositions by construction (same `shadow == flag-ON` identity the
    /// acoustic pass relies on).
    struct RediffSlotPassComputation: Sendable {
        let candidates: [SpliceSlotCandidate]
        let diagnostics: [SpliceSlotDiagnostics]
        let result: SpliceSlotDispositionResult
        /// playhead-6qvf: WHICH differ arm produced `result`'s slots —
        /// `.rediffSlot` for the byte-run aligner, `.rediffSlotChroma` for the
        /// ~1 s chroma-fingerprint fallback. Carried here rather than
        /// re-derived at the rewrite site because this struct is the ONLY
        /// place both arms have already converged: after this point the two are
        /// indistinguishable, which is precisely how they came to share one
        /// certainty class.
        ///
        /// The shadow pass reads it too, so a shadow row records the same arm
        /// the flag-ON pass would have stamped (the `shadow == flag-ON`
        /// identity the acoustic pass relies on).
        let widthProvenance: AnchorRef
    }

    /// Compute the rediff slot pass, or `nil` when there is no rediff signal:
    /// no `RediffBSideProvider` injected (the production case today — the
    /// re-fetch scheduler is xsdz.28), no stored A-side fingerprints, no current
    /// asset row, no re-fetched B-side available, or the double-gate /
    /// alignedFraction guard rejected the comparison. `nil` ⇒ the caller leaves
    /// `decodedSpans` untouched (status-quo width — the rediff-sole-setter
    /// contract's fall-through).
    ///
    /// playhead-xsdz.57: the BYTE-RUN aligner is the PRIMARY differ. When the
    /// provider serves a raw B-side file AND the asset row points at a readable
    /// played-copy file, the byte path produces the played slots (0.02 s median
    /// end-edge error vs the chroma differ's multi-second edge overshoots —
    /// the 88 s Mick Jagger incident). When byte inputs are unavailable or the
    /// byte gate rejects (no runs / non-monotonic / re-encode CDN), the chroma
    /// path below runs EXACTLY as pre-xsdz.57. Either way the slots flow into
    /// the SAME candidates → resolveSpan (veto gate) → disposition machinery.
    ///
    /// playhead-6qvf: "the SAME machinery" is true of the GEOMETRY and must not
    /// be true of the CERTAINTY. The arm taken is recorded in the returned
    /// `widthProvenance` (`.rediffSlot` vs `.rediffSlotChroma`) and carried to
    /// the stamp site, because this function is the last point at which the two
    /// are distinguishable. Measured on the 51-pair corpus, the byte gate
    /// rejects on 9/51 (17.6%) — this is a routine branch, not an edge case.
    private func computeRediffSlotPass(
        decodedSpans: [DecodedSpan],
        atoms: [TranscriptAtom],
        atomEvidence: [AtomEvidence],
        analysisAssetId: String,
        showId: String?
    ) async -> RediffSlotPassComputation? {
        guard !decodedSpans.isEmpty else { return nil }
        // The re-fetched B-side source. Absent in production → no-op (the whole
        // pass is inert until the re-fetch scheduler / as-played tap lands).
        guard let provider = rediffBSideProvider else { return nil }

        // The asset row, needed by BOTH differ paths: the byte path reads the
        // played copy from `sourceURL`; the chroma path gates on the CURRENT
        // `assetFingerprint` (sourceAudioIdentity double-gate).
        let asset: AnalysisAsset
        do {
            guard let fetched = try await store.fetchAsset(id: analysisAssetId) else { return nil }
            asset = fetched
        } catch {
            logger.warning("[xsdz.29] fetchAsset failed: \(error.localizedDescription)")
            return nil
        }

        let playedSlots: [RediffSlotOwnership.PlayedSlot]
        // playhead-6qvf: the differ arm, decided HERE and carried to the stamp
        // site. These two branches are the entire difference between a
        // sample-accurate width and a ~1 s one, and until 6qvf both left the
        // same `.rediffSlot` mark behind.
        let widthProvenance: AnchorRef
        if let byteSlots = await computeByteAlignedPlayedSlots(
            provider: provider,
            asset: asset,
            analysisAssetId: analysisAssetId
        ) {
            // ── BYTE PRIMARY (xsdz.57) ── the chroma differ is NOT invoked.
            playedSlots = byteSlots
            widthProvenance = .rediffSlot
        } else {
            // ── CHROMA FALLBACK ── the pre-xsdz.57 path, unchanged.
            // Stored played-copy (A-side) fingerprint stream. The store's fetch
            // already gates on `algorithmVersion` (returns nil on a version
            // mismatch); the sourceAudioIdentity double-gate is applied by
            // `gateAndDiff` below.
            let storedASide: EpisodeFingerprintRecord
            do {
                guard let record = try await store.fetchEpisodeFingerprints(assetId: analysisAssetId) else { return nil }
                storedASide = record
            } catch {
                logger.warning("[xsdz.29] fetchEpisodeFingerprints failed: \(error.localizedDescription)")
                return nil
            }

            // Re-fetched B-side as mono 16 kHz PCM. Absent → no rediff signal this run.
            guard let bSideSamples = await provider.refetchedBSideMono16kHz(assetId: analysisAssetId) else { return nil }

            let outcome = RediffSlotOwnership.gateAndDiff(
                storedASide: storedASide,
                refetchedBSideSamples16kHz: bSideSamples,
                currentAssetFingerprint: asset.assetFingerprint
            )
            guard case .accepted(let acceptance) = outcome else {
                logger.info(
                    "[xsdz.29] rediff gate rejected asset=\(analysisAssetId, privacy: .public): \(String(describing: outcome), privacy: .public)"
                )
                return nil
            }
            playedSlots = acceptance.playedSlots
            // playhead-6qvf: NOT `.rediffSlot`. `gateAndDiff` aligns chroma
            // fingerprints at `ChromaFingerprinter.hopSize` granularity and its
            // merged runs fold in noise gaps, so these edges carry no claim to
            // the byte differ's measured error distribution.
            widthProvenance = .rediffSlotChroma
            logger.info(
                "[6qvf] rediff width from the CHROMA fallback asset=\(analysisAssetId, privacy: .public) slots=\(playedSlots.count) — stamping .rediffSlotChroma (NOT byte-exact: no deterministic tier, no auto-skip admission, no certainty carve-outs)"
            )
        }

        // playhead-xsdz.34 §5: the user-veto ranges (same set the acoustic pass
        // builds). Rediff is the SOLE production width setter and bypasses
        // `SpliceSlotResolver`, so its candidate path must apply the
        // `.vetoNewlyEnclosed` gate itself or a rediff-widened slot could still
        // absorb a vetoed region. EMPTY unless the read side is flag-ON and a
        // veto masked an atom (`AtomEvidenceProjector` sets the mask).
        let vetoedRanges: [TimeRange] = atomEvidence
            .filter { $0.correctionMask == .userVetoed }
            .map { TimeRange(start: $0.startTime, end: $0.endTime) }

        // playhead-527u §confirmed gate: user-CONFIRMED (added) regions are SACRED
        // WIDTH. The rediff oracle must neither RESHAPE a confirmed span nor NEWLY
        // ENCLOSE (absorb) a confirmed region into another span's slot. Two mirrors
        // of the veto path achieve this:
        //   (a) fold the confirmed ranges into the newly-enclosed gate
        //       (`protectedRanges`) so NO other span's slot can widen over a
        //       confirmed region → `.vetoNewlyEnclosed` → status-quo (no absorb);
        //   (b) strip the slot from a confirmed span's OWN candidate below so it
        //       resolves to `.noSlot` → carried at minted width (no reshape) and
        //       can never itself be an absorber.
        let confirmedRanges: [TimeRange] = atomEvidence
            .filter { $0.correctionMask == .userConfirmed }
            .map { TimeRange(start: $0.startTime, end: $0.endTime) }
        let protectedRanges = vetoedRanges + confirmedRanges

        // Phase 1: resolve per-span rediff slots (bank-agnostic — resolution does
        // not depend on the bank verdicts, only slot-token gathering does).
        let allFalse = [Bool](repeating: false, count: decodedSpans.count)
        let bundle = RediffSlotOwnership.candidates(
            decodedSpans: decodedSpans,
            atomEvidence: atomEvidence,
            playedSlots: playedSlots,
            vetoedRanges: protectedRanges,
            coreBankMatch: allFalse,
            slotBankMatch: allFalse
        )

        // Phase 2: the negative-bank verdict table over the resolved slots (the
        // SAME two-phase pattern the acoustic pass uses; dormant unless the
        // cross-episode-memory flag is on AND a bank is wired).
        let (coreMatch, slotMatch) = await negativeBankVerdicts(
            decodedSpans: decodedSpans,
            atoms: atoms,
            resolvedSlots: bundle.synthesizedSlots,
            showId: showId
        )

        // Phase 3: candidates with the real verdicts → the shared disposition engine.
        let candidates: [SpliceSlotCandidate] = decodedSpans.indices.map { i in
            // playhead-527u (b): a confirmed span keeps status-quo width — strip
            // its slot so it resolves to `.noSlot` (never reshaped, never an
            // absorber). `.vetoNewlyEnclosed` on `protectedRanges` already blocks
            // other slots from absorbing it.
            let core = TimeRange(
                start: decodedSpans[i].startTime, end: decodedSpans[i].endTime
            )
            let confirmedProtected = confirmedRanges.contains { $0.intersects(core) }
            return SpliceSlotCandidate(
                mintedInterval: bundle.candidates[i].mintedInterval,
                slot: confirmedProtected ? nil : bundle.candidates[i].slot,
                slotIntersectsAtoms: confirmedProtected ? true : bundle.candidates[i].slotIntersectsAtoms,
                coreBankMatch: coreMatch[i],
                slotBankMatch: slotMatch[i]
            )
        }
        let result = SpliceSlotDispositionEngine.computeDispositions(candidates)
        return RediffSlotPassComputation(
            candidates: candidates,
            diagnostics: bundle.diagnostics,
            result: result,
            widthProvenance: widthProvenance
        )
    }

    /// playhead-xsdz.57: the BYTE-PRIMARY differ attempt. Returns the cleaned
    /// played slots when the byte path fully succeeds, or `nil` for ANY miss —
    /// no B-side file from the provider, no anchored A-side file at the asset's
    /// `sourceURL`, unreadable bytes, or a byte-gate rejection (no runs /
    /// non-monotonic / re-encode CDN) — in which case the caller falls back to
    /// the chroma differ exactly as pre-xsdz.57.
    ///
    /// IDENTITY GATES: the chroma path's `algorithmVersion` /
    /// `sourceAudioIdentity` double-gate guards a PERSISTED fingerprint stream
    /// against staleness. The byte path consumes no persisted stream — its A
    /// input is the asset row's live audio file read at diff time — so those
    /// gates do not apply; instead both file inputs are anchored as regular,
    /// unaliased, non-empty files (bf4a2383 precedent) before a byte is read.
    ///
    /// NEVER-PERSIST-B (xsdz.28): the B bytes live only inside this call; the
    /// returned slots and every log line are A-time scalars only.
    private func computeByteAlignedPlayedSlots(
        provider: RediffBSideProvider,
        asset: AnalysisAsset,
        analysisAssetId: String
    ) async -> [RediffSlotOwnership.PlayedSlot]? {
        // playhead-xsdz.36.2 (k-way): ALL staged B-sides (the K distinct-persona
        // re-fetches). A single-B provider yields one URL — exactly today's one
        // alignment. Empty → no byte signal → chroma fallback.
        let bSideURLs = await provider.refetchedBSideFileURLs(assetId: analysisAssetId)
        guard !bSideURLs.isEmpty else { return nil }
        guard let aSideURL = Self.byteDifferASideURL(sourceURL: asset.sourceURL) else {
            logger.info(
                "[xsdz.57] byte differ unavailable asset=\(analysisAssetId, privacy: .public): no anchored A-side file — falling back to chroma"
            )
            return nil
        }
        let aData: Data
        do {
            aData = try Data(contentsOf: aSideURL, options: .mappedIfSafe)
        } catch {
            logger.warning(
                "[xsdz.57] byte differ A-side read failed asset=\(analysisAssetId, privacy: .public): \(error.localizedDescription) — falling back to chroma"
            )
            return nil
        }

        // Align A vs EACH B-side; collect the accepted per-B played-slot lists.
        // A per-B miss (unanchored file, unreadable bytes, or a byte-gate
        // rejection — no runs / non-monotonic / re-encode CDN) is SKIPPED, not
        // fatal: another persona's B may still diverge on the same pod
        // (playhead-xsdz.36.2). If EVERY B misses, the whole byte path yields
        // nothing → chroma fallback exactly as pre-xsdz.57.
        var perBSideSlots: [[RediffSlotOwnership.PlayedSlot]] = []
        for bSideURL in bSideURLs {
            guard Self.isAnchoredRegularFile(bSideURL) else {
                logger.info(
                    "[xsdz.57] byte differ skip asset=\(analysisAssetId, privacy: .public): B-side URL is not an anchored regular file"
                )
                continue
            }
            let bData: Data
            do {
                bData = try Data(contentsOf: bSideURL, options: .mappedIfSafe)
            } catch {
                logger.warning(
                    "[xsdz.57] byte differ B-side read failed asset=\(analysisAssetId, privacy: .public): \(error.localizedDescription)"
                )
                continue
            }
            let alignment = RediffByteAligner.align(aData: aData, bData: bData)
            let outcome = RediffSlotOwnership.gateAndDiffBytes(alignment: alignment)
            switch outcome {
            case .accepted(let acceptance):
                logger.info(
                    "[xsdz.57] byte differ accepted asset=\(analysisAssetId, privacy: .public) slots=\(acceptance.playedSlots.count) runsChained=\(acceptance.runsChained) chainedFractionB=\(String(format: "%.3f", acceptance.chainedFractionB), privacy: .public)"
                )
                perBSideSlots.append(acceptance.playedSlots)
            case .rejectedNoChainedRuns, .rejectedNonMonotonic, .rejectedLowChainedFraction:
                logger.info(
                    "[xsdz.57] byte differ rejected asset=\(analysisAssetId, privacy: .public): \(String(describing: outcome), privacy: .public)"
                )
                continue
            }
        }
        guard !perBSideSlots.isEmpty else {
            logger.info(
                "[xsdz.36.2] byte differ: no B-side aligned (\(bSideURLs.count) tried) asset=\(analysisAssetId, privacy: .public) — falling back to chroma"
            )
            return nil
        }
        // UNION the divergent regions across the K diffs. K=1 returns the single
        // list unchanged (byte-identical to xsdz.57); K≥2 recovers pods a single
        // pair misses.
        let unioned = RediffSlotOwnership.unionedPlayedSlots(perBSideSlots)
        if perBSideSlots.count > 1 {
            logger.info(
                "[xsdz.36.2] k-way union asset=\(analysisAssetId, privacy: .public) diffs=\(perBSideSlots.count) unionSlots=\(unioned.count)"
            )
        }
        return unioned
    }

    /// The played-copy (A-side) file for the byte differ, derived from the
    /// asset row's `sourceURL` (the downloaded episode file in production; the
    /// snapshotted corpus copy in the dump harness). `nil` unless it resolves
    /// to an anchored regular file.
    ///
    /// playhead-b8hj: resolution goes through ``AudioCacheLocation`` rather
    /// than trusting the stored string as a path. The audio cache is addressed
    /// through the app Data container, whose UUID is rewritten on reinstall and
    /// restore, so a row minted under an earlier container names a directory
    /// that no longer exists even though the artifact itself is right there
    /// under the current one. `nil` still means "no A-side" — a normal,
    /// non-error outcome that falls back to chroma / blocks the day-0 mint.
    static func byteDifferASideURL(
        sourceURL: String,
        cacheRoot: URL = DownloadManager.defaultCacheDirectory()
    ) -> URL? {
        AudioCacheLocation.resolve(sourceURL, cacheRoot: cacheRoot, isUsable: isAnchoredRegularFile)
    }

    /// The bf4a2383 filesystem anchor: the URL itself (no symlink traversal)
    /// must be a regular, unaliased, NON-EMPTY file. Mirrors the dump
    /// harness's staged-fresh probe semantics.
    static func isAnchoredRegularFile(_ url: URL) -> Bool {
        guard let values = try? url.resourceValues(
            forKeys: [.isRegularFileKey, .isSymbolicLinkKey, .fileSizeKey]
        ) else { return false }
        return values.isRegularFile == true
            && values.isSymbolicLink != true
            && (values.fileSize ?? 0) > 0
    }

    /// Flag-ON rediff OWNERSHIP pass: materialize the rediff dispositions via the
    /// pure `SpliceSlotRewriter` carrying `.rediffSlot` provenance, reconcile
    /// persistence (delete superseded/absorbed, upsert the rewritten set), and
    /// return the rewritten spans. A `nil` computation (no rediff signal) leaves
    /// `decodedSpans` byte-identical.
    private func applyRediffSlotOwnershipPass(
        decodedSpans: [DecodedSpan],
        atoms: [TranscriptAtom],
        atomEvidence: [AtomEvidence],
        analysisAssetId: String,
        showId: String?
    ) async -> [DecodedSpan] {
        guard let computation = await computeRediffSlotPass(
            decodedSpans: decodedSpans,
            atoms: atoms,
            atomEvidence: atomEvidence,
            analysisAssetId: analysisAssetId,
            showId: showId
        ) else { return decodedSpans }

        // playhead-6qvf: the provenance is the DIFFER'S, not a constant. Both
        // arms reach this one call, so a literal `.rediffSlot` here was the
        // single line that collapsed two instruments into one certainty class.
        let rewrite = SpliceSlotRewriter.apply(
            decodedSpans: decodedSpans,
            dispositions: computation.result.dispositions,
            atomEvidence: atomEvidence,
            provenance: computation.widthProvenance
        )

        let keptCount = computation.result.dispositions.filter {
            if case .keepSlot = $0 { return true } else { return false }
        }.count
        if keptCount > 0 || !rewrite.absorbedIds.isEmpty {
            logger.info(
                "[xsdz.29] rediff slot pass asset=\(analysisAssetId, privacy: .public) spans=\(decodedSpans.count) kept=\(keptCount) absorbed=\(rewrite.absorbedIds.count) rounds=\(computation.result.fixpointRounds) provenance=\(computation.widthProvenance.provenanceKind, privacy: .public)"
            )
        }

        // playhead-527u: defense-in-depth — never DELETE a user-correction span's
        // persisted row here, even if some future disposition path let its id
        // reach `supersededIds`. `SpliceSlotRewriter` already excludes them, so
        // this normally subtracts nothing.
        let userCorrectionIds = Set(
            decodedSpans
                .filter { $0.anchorProvenance.contains(where: \.isUserCorrection) }
                .map(\.id)
        )
        let idsToDelete = rewrite.supersededIds.filter { !userCorrectionIds.contains($0) }
        do {
            try await store.deleteDecodedSpans(ids: idsToDelete)
            if !rewrite.finalSpans.isEmpty {
                try await store.upsertDecodedSpans(rewrite.finalSpans)
            }
        } catch {
            logger.warning("[xsdz.29] rediff slot-pass re-persist failed: \(error.localizedDescription)")
        }

        return rewrite.finalSpans
    }

    /// Flag-OFF-ownership / shadow-ON rediff SHADOW pass: compute the would-be
    /// rediff dispositions via the SAME `computeRediffSlotPass` helper the flag-ON
    /// path uses, emit one `rediffslot.shadow` breadcrumb per span, and record
    /// structured rows to `rediffSlotShadowObserver` — WITHOUT applying any
    /// rewrite (no `.rediffSlot` provenance, no re-persist), so the decision path
    /// stays byte-identical.
    private func runRediffSlotShadowPass(
        decodedSpans: [DecodedSpan],
        atoms: [TranscriptAtom],
        atomEvidence: [AtomEvidence],
        analysisAssetId: String,
        showId: String?
    ) async {
        guard let computation = await computeRediffSlotPass(
            decodedSpans: decodedSpans,
            atoms: atoms,
            atomEvidence: atomEvidence,
            analysisAssetId: analysisAssetId,
            showId: showId
        ) else { return }

        let rows = SpliceSlotShadowRowBuilder.makeRows(
            assetId: analysisAssetId,
            spanIds: decodedSpans.map(\.id),
            candidates: computation.candidates,
            diagnostics: computation.diagnostics,
            dispositions: computation.result.dispositions
        )

        for row in rows {
            logger.notice("\(RediffSlotShadowBreadcrumb.format(row), privacy: .public)")
        }

        if let observer = rediffSlotShadowObserver {
            await observer.record(rows, assetId: analysisAssetId)
        }
    }

    /// The negative-bank per-span verdict table (core-token + slot-token matches),
    /// index-aligned with `decodedSpans`. Shared by the acoustic (`resolvedSlots`
    /// from `SpliceSlotResolver`) and rediff (`resolvedSlots` from
    /// `RediffSlotOwnership`) slot passes so both derive identical verdicts.
    /// Dormant (all-false) unless `crossEpisodeMemoryEnabled` is on AND a bank is
    /// wired — matching the flag-OFF path.
    private func negativeBankVerdicts(
        decodedSpans: [DecodedSpan],
        atoms: [TranscriptAtom],
        resolvedSlots: [SpliceSlot?],
        showId: String?
    ) async -> (coreMatch: [Bool], slotMatch: [Bool]) {
        let bankWired = config.crossEpisodeMemoryEnabled && negativeFingerprintBank != nil
        var coreMatch = [Bool](repeating: false, count: decodedSpans.count)
        var slotMatch = [Bool](repeating: false, count: decodedSpans.count)
        if bankWired, let bank = negativeFingerprintBank {
            for (i, span) in decodedSpans.enumerated() {
                // CORE tokens: atoms in the span's minted ordinal range.
                let coreTokens = Self.negativeBankTokens(
                    atoms.filter {
                        $0.atomKey.atomOrdinal >= span.firstAtomOrdinal
                            && $0.atomKey.atomOrdinal <= span.lastAtomOrdinal
                    }
                )
                if !coreTokens.isEmpty {
                    coreMatch[i] = await bank.bestMatch(candidateTokens: coreTokens, show: showId) != nil
                }
                // SLOT tokens: atoms whose interval intersects the slot interval.
                if let slot = resolvedSlots[i] {
                    let slotRange = TimeRange(start: slot.startTime, end: slot.endTime)
                    let slotTokens = Self.negativeBankTokens(
                        atoms.filter {
                            TimeRange(start: $0.startTime, end: $0.endTime).intersects(slotRange)
                        }
                    )
                    if !slotTokens.isEmpty {
                        slotMatch[i] = await bank.bestMatch(candidateTokens: slotTokens, show: showId) != nil
                    }
                }
            }
        }
        return (coreMatch, slotMatch)
    }

    /// Tokenize + prefix-clamp atoms' joined text the same way the negative-bank
    /// suppression read does (mirrors the `crossEpisodeMemory` call site), so the
    /// verdict-table alignment sees an identical token population.
    private static func negativeBankTokens(_ atoms: [TranscriptAtom]) -> [String] {
        let text = atoms
            .sorted { $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal }
            .map(\.text)
            .joined(separator: " ")
        let raw = SmithWatermanAligner.tokenize(text)
        return raw.count > NegativeFingerprintBank.maxTokenCount
            ? Array(raw.prefix(NegativeFingerprintBank.maxTokenCount))
            : raw
    }

    /// Resolve the acoustic-catalog evidence bound to one exact span geometry.
    /// A nil store, zero fingerprint, missing/non-canonical show, or catalog
    /// failure remains unevaluated; an available exact-show query with no match
    /// is an observed miss.
    static func resolveCatalogSpanEvidence(
        span: DecodedSpan,
        featureWindows: [FeatureWindow],
        catalogStore: AdCatalogStore?,
        showId: String?
    ) async -> CatalogSpanEvidence {
        guard span.startTime.isFinite,
              span.endTime.isFinite,
              span.startTime >= 0,
              span.endTime > span.startTime,
              RecurrenceMaterialIdentity.canonicalIdentifier(span.assetId)
                != nil,
              featureWindows.allSatisfy({
                  $0.analysisAssetId == span.assetId
              }) else {
            return CatalogSpanEvidence(
                topSimilarity: 0,
                topMatch: nil,
                wasEvaluated: false
            )
        }
        // Match AnalysisStore.fetchFeatureWindows(assetId:from:to:), which is
        // the population used when authoritative learning, correction, and
        // final runtime admission re-fingerprint this exact material. Including
        // edge-straddling windows here would stamp a match that cannot be
        // reproduced from persistence and would be demoted on the next actor
        // hop or launch.
        let contained = featureWindows.filter {
            $0.startTime >= span.startTime && $0.endTime <= span.endTime
        }
        let fingerprint = AcousticFingerprint.fromFeatureWindows(contained)
        guard let catalogStore, !fingerprint.isZero,
              let matches = await catalogStore.matchesIfAvailable(
                  fingerprint: fingerprint,
                  show: showId
              ) else {
            return CatalogSpanEvidence(
                topSimilarity: 0,
                topMatch: nil,
                wasEvaluated: false
            )
        }
        let topMatch = matches.first
        return CatalogSpanEvidence(
            topSimilarity: topMatch?.similarity ?? 0,
            topMatch: topMatch,
            wasEvaluated: true
        )
    }

    /// Replace only the device-local acoustic-catalog diagnostic row while
    /// preserving transcript-catalog and every other evidence producer. Used
    /// after SpanFinalizer rewrites geometry so persistence, replay telemetry,
    /// and later exact-row correction all describe the emitted material.
    static func replacingFingerprintStoreCatalogEvidence(
        in ledger: [EvidenceLedgerEntry],
        with evidence: CatalogSpanEvidence,
        catalogCap: Double
    ) -> [EvidenceLedgerEntry] {
        let isFingerprintStoreRow: (EvidenceLedgerEntry) -> Bool = {
            $0.source == .catalog && $0.subSource == .fingerprintStore
        }
        let priorIndex = ledger.firstIndex(where: isFingerprintStoreRow)
        var result = ledger.filter { !isFingerprintStoreRow($0) }
        guard evidence.wasEvaluated,
              evidence.topMatch != nil,
              evidence.topSimilarity.isFinite,
              evidence.topSimilarity >= AdCatalogStore.defaultSimilarityFloor,
              evidence.topSimilarity <= 1,
              catalogCap.isFinite,
              catalogCap >= 0 else {
            return result
        }
        let row = EvidenceLedgerEntry(
            source: .catalog,
            weight: Double(evidence.topSimilarity) * catalogCap,
            detail: .catalog(entryCount: 1),
            subSource: .fingerprintStore
        )
        result.insert(row, at: min(priorIndex ?? result.endIndex, result.endIndex))
        return result
    }

    private func buildEvidenceLedger(
        span: DecodedSpan,
        classifierResults: [ClassifierResult],
        lexicalCandidates: [LexicalCandidate],
        lexicalHits: [LexicalHit] = [],
        featureWindows: [FeatureWindow],
        catalogEntries: [EvidenceEntry],
        semanticScanResults: [SemanticScanResult],
        metadataEntries: [EvidenceLedgerEntry] = [],
        acousticPipelineFusion: [AcousticFeatureFusion.WindowFusion] = [],
        acousticBreaks: [AcousticBreak] = [],
        catalogMatchSimilarity: Float = 0,
        fusionConfig: FusionWeightConfig,
        // playhead-2hpn: when both are non-nil/positive AND the flag is
        // on, the music-bed evaluator switches to fixed weights
        // (0.10 baseline / 0.25 jingle-overlap). `nil` when the flag is
        // off — preserves byte-identical pre-2hpn fusion output.
        showMusicBedSnapshot: ShowMusicBedProfileSnapshot? = nil,
        // playhead-xsdz.9: pre-built `.crossEpisodeMemory` POSITIVE boost
        // entries (candidate aligned to a confirmed-ad bank sequence). Empty for
        // the flag-OFF / no-store / non-firing path, so the ledger is
        // byte-identical to pre-xsdz.9 in those cases.
        crossEpisodeMemoryEntries: [EvidenceLedgerEntry] = [],
        // playhead-xsdz.12: the span's joined transcript prose, used ONLY by
        // the rhetorical act-sequence grammar detector. Empty string for the
        // flag-OFF path (the call site never assembles it when the flag is
        // off) and for every existing caller that does not thread it, so the
        // ledger is byte-identical to pre-xsdz.12 in those cases.
        spanText: String = "",
        // playhead-xsdz.13: pre-built `.crossShowSyndication` boost entries (a
        // normalized sponsor entity with high cross-show spread + temporal
        // persistence). Empty for the flag-OFF / no-store / non-firing path, so
        // the ledger is byte-identical to pre-xsdz.13 in those cases.
        crossShowSyndicationEntries: [EvidenceLedgerEntry] = [],
        episodeDuration: Double = 0
    ) -> [EvidenceLedgerEntry] {
        // Classifier entry: find the best-matching ClassifierResult for this span.
        let classifierScore = bestClassifierScore(
            for: span,
            results: classifierResults
        )

        // FM entries: positive-only, mode-gated, from persisted scan results.
        // Cycle 1 H2: use the effective (registry-intersected) mode so a
        // demoted cohort does not fold FM evidence into the ledger.
        let fmEntries = buildFMLedgerEntries(
            span: span,
            scanResults: semanticScanResults,
            mode: effectiveFMBackfillMode,
            fusionConfig: fusionConfig
        )

        // Lexical entries: from LexicalCandidates overlapping the span.
        let lexicalEntries = buildLexicalLedgerEntries(
            span: span,
            candidates: lexicalCandidates,
            fusionConfig: fusionConfig
        )

        // playhead-xsdz.1: High-precision lexical auto-ad entries. The
        // builder inspects the raw hit stream for a vetted strong
        // co-occurrence (sponsor + promo code / URL CTA in a tight window)
        // and emits a high-weight `.lexicalAutoAd` entry — UNLESS a
        // negative-evidence guardrail (show-owned domain / news/review
        // context) fires. Empty when `lexicalHits` is empty (every existing
        // caller that does not thread hits, e.g. tests) or the rule does not
        // fire, preserving prior behavior for those paths.
        //
        // playhead-xsdz.6: also empty when the master kill switch is off (the
        // production default). Skipping construction — rather than just
        // raising `lexicalAutoAdQualifiedThreshold` — removes the entry's
        // fusion mass too, which is what eliminates the all-on interaction FP
        // the per-feature A/B surfaced (see `lexicalAutoAdEnabled`).
        let lexicalAutoAdEntries = config.lexicalAutoAdEnabled
            ? lexicalAutoAdBuilder.buildEntries(hits: lexicalHits, for: span)
            : []

        // playhead-xsdz.8: Composite audio-forensics boundary entries. The
        // detector measures the PHYSICAL discontinuity (loudness / spectral-
        // flux / noise-floor / production-environment step) across the span's
        // start and end edges from the per-window acoustic features that are
        // ALREADY available here, merges the sigma-normalized sub-signals into
        // ONE boundary score, and emits a single capped `.audioForensics`
        // entry when that score is significant. Gated OFF by default: when
        // `audioForensicsEnabled` is false the detector is never called, so
        // NO entry is built and the ledger is byte-identical to pre-xsdz.8.
        // playhead-xsdz.20: SUPPRESS the `.audioForensics` entry on ACOUSTIC
        // slot-owned spans. The acoustic slot width is DERIVED FROM the
        // audio-forensics seam (the resolver picks edges from
        // AudioForensicsBoundaryDetector scores), so keeping both double-counts
        // the SAME evidence. DELIBERATELY splice-only (NOT `isWidthOwnership`):
        // a rediff slot's width comes from an INDEPENDENT fingerprint diff, so an
        // audio-forensics entry at the same physical seam is corroboration, not a
        // double-count — it is kept. (Revisit this scoring choice at the rediff
        // activation eval; flag-OFF today so it is inert either way.)
        let audioForensicsEntries = config.audioForensicsEnabled
            && !span.anchorProvenance.contains(.spliceSlot)
            ? audioForensicsDetector.buildEntries(
                span: span,
                episodeWindows: featureWindows,
                fusionConfig: fusionConfig
            )
            : []

        // playhead-xsdz.12: Rhetorical act-sequence grammar entries. The
        // builder classifies each sentence of the span's transcript prose into
        // zero or more rhetorical roles (HOOK / PROBLEM / SOLUTION / EVIDENCE /
        // OFFER / CTA) and emits a single capped corroborative `.rhetoricalGrammar`
        // entry when >= 3 distinct roles co-occur in (roughly) the canonical
        // persuasion order. Gated OFF by default: when `rhetoricalGrammarEnabled`
        // is false the builder is never called (and the call site never even
        // assembles `spanText`), so NO entry is built and the ledger is
        // byte-identical to pre-xsdz.12. Also empty when `spanText` is empty
        // (every existing caller that does not thread it) or the grammar does
        // not fire.
        let rhetoricalGrammarEntries = config.rhetoricalGrammarEnabled
            ? rhetoricalGrammarBuilder.buildEntries(text: spanText, for: span)
            : []

        // Acoustic entries: from FeatureWindows in the span range.
        // playhead-fqc8: pass `acousticBreaks` so a `.classifierSeed`-anchored
        // span with an aligned break also gets a `.breakAlignment` entry,
        // which is the gate for `PromotionTrack.classifierSeedQualified`.
        let acousticAndAlignmentEntries = buildAcousticLedgerEntries(
            span: span,
            featureWindows: featureWindows,
            fusionConfig: fusionConfig,
            acousticBreaks: acousticBreaks
        )
        // playhead-fqc8 cycle-1 review HIGH-1: the helper returns RMS-drop
        // `.acoustic` and the alignment corroborator (`.breakAlignment`)
        // mixed in one list. Split them by source kind here so each family
        // flows into its own dedicated parameter on
        // `BackfillEvidenceFusion`, giving each family its own honest
        // weight cap.
        let acousticEntries = acousticAndAlignmentEntries.filter { $0.source != .breakAlignment }
        let breakAlignmentEntries = acousticAndAlignmentEntries.filter { $0.source == .breakAlignment }

        // 2026-04-23 Finding 4: music-bed coverage produces its own
        // `.musicBed` ledger entry (distinct EvidenceSourceType) so
        // the quorum gate's `distinctKinds.count` increments when a
        // span has both an RMS-drop edge and an interior bed.
        // playhead-2hpn: when the scoped-music-bed-generalization flag
        // is ON (signalled by a non-nil `showMusicBedSnapshot`), the
        // evaluator returns a boosted weight (0.25) for spans that
        // overlap a detected jingle on a confirmed show, or 0.10
        // baseline otherwise. Flag-off path leaves the evaluator on its
        // legacy presenceFraction*acousticCap math.
        let musicBedEntries = buildMusicBedLedgerEntries(
            span: span,
            featureWindows: featureWindows,
            fusionConfig: fusionConfig,
            showMusicBedSnapshot: showMusicBedSnapshot,
            episodeDuration: episodeDuration
        )
        // musicBed entries are merged into the acousticEntries list
        // passed to BackfillEvidenceFusion. The fusion code already
        // iterates over acousticEntries and preserves each entry's
        // `source`, so a `.musicBed`-sourced entry flows through with
        // the correct kind and increments distinctKinds.count.
        //
        // playhead-gtt9.16: also add aggregated `.acoustic` entries from
        // the acoustic feature pipeline output. When the pipeline produced
        // zero combined mass over the span (features all returned 0), the
        // helper returns empty, preserving pre-wire back-compat.
        let pipelineAcousticEntries = buildAcousticPipelineLedgerEntries(
            span: span,
            pipelineFusion: acousticPipelineFusion,
            fusionConfig: fusionConfig
        )
        let combinedAcousticEntries = acousticEntries + musicBedEntries + pipelineAcousticEntries

        // Catalog entries: from EvidenceEntry items overlapping the span.
        var catalogLedgerEntries = buildCatalogLedgerEntries(
            span: span,
            entries: catalogEntries,
            fusionConfig: fusionConfig
        )

        // playhead-gtt9.17: add a catalog ledger entry from the
        // `AdCatalogStore` match similarity when a prior stored ad creative
        // fingerprint-matches this span above the default floor. The
        // similarity-scaled diagnostic weight is retained for replay, but
        // `DecisionMapper.scoringLedger` excludes this `.fingerprintStore`
        // row from confidence and quorum. `entryCount: 1` preserves the
        // existing `.catalog(entryCount:)` telemetry detail variant.
        if catalogMatchSimilarity >= AdCatalogStore.defaultSimilarityFloor {
            let weight = Double(catalogMatchSimilarity) * fusionConfig.catalogCap
            catalogLedgerEntries.append(EvidenceLedgerEntry(
                source: .catalog,
                weight: weight,
                detail: .catalog(entryCount: 1),
                // playhead-epfk: stamp the cross-episode `AdCatalogStore`
                // fingerprint match so NARL replay can attribute this entry
                // to the correction-loop channel (vs. the transcript token
                // catalog which uses `.transcriptCatalog`). The raw
                // similarity that produced this weight is also persisted
                // on `AdWindow.catalogStoreMatchSimilarity` for direct
                // inspection in the corpus export.
                subSource: .fingerprintStore
            ))
        }

        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: classifierScore,
            fmEntries: fmEntries,
            lexicalEntries: lexicalEntries,
            acousticEntries: combinedAcousticEntries,
            catalogEntries: catalogLedgerEntries,
            metadataEntries: metadataEntries,
            breakAlignmentEntries: breakAlignmentEntries,
            lexicalAutoAdEntries: lexicalAutoAdEntries,
            audioForensicsEntries: audioForensicsEntries,
            crossEpisodeMemoryEntries: crossEpisodeMemoryEntries,
            rhetoricalGrammarEntries: rhetoricalGrammarEntries,
            crossShowSyndicationEntries: crossShowSyndicationEntries,
            // Cycle 1 H2: effective mode so fusion's `contributesToExistingCandidateLedger`
            // gate honors the registry's decision for this cohort.
            mode: effectiveFMBackfillMode,
            config: fusionConfig
        )
        return fusion.buildLedger()
    }

    // MARK: - FM Suppression (Phase ef2.4.6)

    /// Apply targeted FM suppression to a ledger when FM strongly says noAds.
    ///
    /// Builds FMSuppressionWindow entries from overlapping scan results, evaluates
    /// the suppression guard, and applies downweighting if all guards pass.
    ///
    /// playhead-avbn: which rows may vote is decided by
    /// ``FMSuppressionWindow/votingWindows(spanStartTime:spanEndTime:scanResults:)``,
    /// which lives next to the guard that consumes the answer. This site used to
    /// admit every scan row whose time range overlapped the span, with no filter
    /// on pass, status or sentinel-ness — so rows meaning "I failed to find the
    /// edges" or "no FM work was performed" could satisfy the 2-window `noAds`
    /// quorum by themselves and downweight the detectors that DID find something.
    private func applyFMSuppression(
        span: DecodedSpan,
        ledger: [EvidenceLedgerEntry],
        semanticScanResults: [SemanticScanResult]
    ) -> FMSuppressionResult {
        // Build suppression windows from FM scan results overlapping this span.
        let overlappingWindows = FMSuppressionWindow.votingWindows(
            spanStartTime: span.startTime,
            spanEndTime: span.endTime,
            scanResults: semanticScanResults
        )

        let guard_ = FMSuppressionGuard(
            overlappingFMResults: overlappingWindows,
            ledger: ledger,
            anchorProvenance: span.anchorProvenance
        )
        let guardResult = guard_.evaluate()

        let applicator = FMSuppressionApplicator()
        return applicator.apply(guardResult: guardResult, ledger: ledger)
    }

    /// Find the best-matching ClassifierResult for a DecodedSpan (by time overlap).
    private func bestClassifierScore(
        for span: DecodedSpan,
        results: [ClassifierResult]
    ) -> Double {
        let overlapping = results.filter { result in
            let overlapStart = max(span.startTime, result.startTime)
            let overlapEnd = min(span.endTime, result.endTime)
            return overlapEnd > overlapStart
        }
        // Use the highest adProbability among overlapping results as the classifier score.
        return overlapping.map(\.adProbability).max() ?? 0.0
    }

    /// ef2.4.5: Minimal decode struct for extracting (commercialIntent, ownership)
    /// from `SemanticScanResult.spansJSON`. Mirrors the encoding in
    /// `BackfillJobRunner.EncodedRefinedSpan` but decodes only the two fields
    /// needed for `ClassificationTrustMatrix` lookup.
    private struct SpanTrustDecode: Decodable {
        let commercialIntent: String
        let ownership: String
    }

    /// ef2.4.5: Extract the dominant classificationTrust from a scan result's spansJSON.
    /// Decodes the refined spans, maps each to a trust value, and returns the maximum
    /// (most commercially confident span wins). Returns 1.0 if spansJSON is empty or
    /// cannot be decoded (backward-compatible default).
    private func classificationTrust(from spansJSON: String) -> Double {
        guard let data = spansJSON.data(using: .utf8),
              let spans = try? JSONDecoder().decode([SpanTrustDecode].self, from: data),
              !spans.isEmpty else {
            return 1.0
        }

        return spans.map { span in
            let intent = CommercialIntent(rawValue: span.commercialIntent) ?? .unknown
            let owner = Ownership(rawValue: span.ownership) ?? .unknown
            return ClassificationTrustMatrix.trust(commercialIntent: intent, ownership: owner)
        }.max() ?? 1.0
    }

    /// Build FM ledger entries from `SemanticScanResult`s overlapping the span.
    /// Applies the Positive-Only Rule: only `containsAd` dispositions
    /// contribute.
    ///
    /// playhead-yx0f made this `internal` rather than `private`, for the same
    /// reason `buildAcousticLedgerEntries` above it is: the band→weight ladder
    /// is the whole behaviour of this function and there is no other seam from
    /// which to observe it. `FMLedgerCertaintyBandTests` replays real persisted
    /// `spansJSON` payloads through it.
    func buildFMLedgerEntries(
        span: DecodedSpan,
        scanResults: [SemanticScanResult],
        mode: FMBackfillMode,
        fusionConfig: FusionWeightConfig
    ) -> [EvidenceLedgerEntry] {
        guard mode.contributesToExistingCandidateLedger else { return [] }

        return scanResults.compactMap { result in
            // Only positive FM evidence contributes.
            guard result.disposition == .containsAd else { return nil }

            // Check time overlap with span.
            let overlapStart = max(span.startTime, result.windowStartTime)
            let overlapEnd = min(span.endTime, result.windowEndTime)
            guard overlapEnd > overlapStart else { return nil }

            // playhead-yx0f: THE MODEL'S OWN BAND, read from the row this loop
            // is already holding.
            //
            // It used to be FABRICATED out of `transcriptQuality` — "the coarse
            // scan carries transcript quality; use it as a band proxy" — and
            // the premise was false: every `containsAd` row persists the
            // model's own `CertaintyBand` in `spansJSON`
            // (`CoarseSupportSchema.certainty` on a `passA` object, each
            // refined span's own band in a `passB` array). 55 of 55 coarse and
            // 11 of 11 refined rows on the 2026-08-10 device pull carry one,
            // split 40 `strong` / 15 `moderate`. Three consequences, all in one
            // direction: the proxy could never return `.strong`, so the `fmCap`
            // rung of the ladder below was DEAD CODE at this call site; a
            // `moderate` verdict and a `strong` verdict on the same clean
            // transcript weighed the same; and the quantity being read was not
            // the one being reported — `transcriptQuality` is a deterministic
            // on-device estimate of ASR cleanliness
            // (`TranscriptQualityEstimator`), the band is the model's
            // confidence in ITS OWN verdict, and `FoundationModelClassifier`
            // keeps quality OUT of the `@Generable` schema for exactly that
            // reason.
            //
            // ONE DECODER, shared with the sweep lane. Both payload shapes,
            // the weakest span governing an array, and — the part a second
            // decoder written here would have got wrong — a runner-HARDCODED
            // permissive-bypass `.strong` read as UNGRADED
            // (`ownershipInferenceWasSuppressed`, playhead-92im review).
            //
            // playhead-iw7q: the COARSE half of that hardcode is gated too now,
            // and this call site inherited the fix for free — which is the
            // whole argument for the shared decoder. `certaintyBand(of:)` reads
            // the ROW's `verdictProvenance` (schema V61) for a `passA` payload,
            // because `CoarseSupportSchema` has no per-span flag to carry one.
            // Two consequences here: a permissive coarse row weighs
            // `fmCap * 0.5` instead of `fmCap`, and — because there is no
            // backfill and there cannot be one — SO DOES EVERY ROW WRITTEN
            // BEFORE V61, all of which read `.unknown`. On the 2026-08-21 t6
            // pull that is 1,406 `passA` rows, 362 of which carried a band this
            // line was spending. The direction is conservative in both cases:
            // an unattributable verdict stops claiming the ceiling.
            //
            // NO BAND ⇒ `.weak`, DELIBERATELY, AND NOT THE OLD PROXY. Three
            // options were available and only one of them is honest. Falling
            // back to `transcriptQuality` would reinstate this bead's defect
            // for whatever population takes the fallback — the failure mode
            // where a fix survives only on the rows nobody checks. Dropping the
            // entry would silently delete FM evidence for a verdict the model
            // did make, which is a reach change nobody asked for. `.weak` is
            // the FLOOR of the ladder, and an absence of evidence must never
            // read like the presence of evidence; it is also exactly what the
            // sibling lane's `certaintyFactor(nil)` already returns, so the two
            // lanes read one FM verdict the same way. It is the conservative
            // direction — an ungraded row on a `good` transcript now weighs
            // 0.5·fmCap where the proxy gave it 0.75·fmCap — and on the pull
            // this bead was filed from the population is EMPTY: zero of the 66
            // `containsAd` rows carry an empty support payload.
            //
            // TWO OPEN LIMITS, NAMED RATHER THAN ABSORBED. Neither is fixed
            // here; both are filed.
            //
            //   * playhead-e15r (P1, OPEN) can LAUNDER a fabricated band past
            //     the gate above. `BackfillJobRunner.unionSpan` copies
            //     `certainty` from whichever input ranks higher while ANDing
            //     `ownershipInferenceWasSuppressed`, so a permissive `.strong`
            //     unioned with a genuine `.moderate` persists as a `.strong`
            //     with the flag CLEARED — byte-indistinguishable at rest from a
            //     model grade. That path is LIVE (Dan's 2026-08-11 triage
            //     corrected the bead's own "latent" premise). So this read is
            //     honest for every row `unionSpan` did not touch, and e15r is
            //     what makes it honest for the rest.
            //
            //   * playhead-dslt (P1) is THIS defect in the SUPPRESSION lane.
            //     `FMSuppressionWindow.votingWindows` still fabricates a band
            //     out of `transcriptQuality`, and `FMSuppressionGuard` spends
            //     it on two of its five strict guards. It CANNOT be fixed by
            //     copying this line: a `noAds` row carries no band at rest at
            //     all — 51 of 51 on the same pull persist `spansJSON == "[]"`,
            //     because `CoarseSupportSchema.support` is a positive-only
            //     notion. Reading honestly there disables FM suppression
            //     outright, which is a reach decision and not a cleanup.
            let band = SemanticSweepMarkComposer.certaintyBand(of: result) ?? .weak

            // Weight proportional to band.
            let weight: Double
            switch band {
            case .strong: weight = fusionConfig.fmCap
            case .moderate: weight = fusionConfig.fmCap * 0.75
            case .weak: weight = fusionConfig.fmCap * 0.5
            }

            // ef2.4.5: look up classificationTrust from refinement data in spansJSON.
            let trust = classificationTrust(from: result.spansJSON)

            return EvidenceLedgerEntry(
                source: .fm,
                weight: weight,
                detail: .fm(
                    disposition: .containsAd,
                    band: band,
                    cohortPromptLabel: result.scanCohortJSON
                ),
                classificationTrust: trust
            )
        }
    }

    /// Build lexical ledger entries from LexicalCandidates overlapping the span.
    private func buildLexicalLedgerEntries(
        span: DecodedSpan,
        candidates: [LexicalCandidate],
        fusionConfig: FusionWeightConfig
    ) -> [EvidenceLedgerEntry] {
        candidates.compactMap { candidate in
            let overlapStart = max(span.startTime, candidate.startTime)
            let overlapEnd = min(span.endTime, candidate.endTime)
            guard overlapEnd > overlapStart else { return nil }

            let weight = min(candidate.confidence * fusionConfig.lexicalCap, fusionConfig.lexicalCap)
            let categories = candidate.categories.map(\.rawValue)
            return EvidenceLedgerEntry(
                source: .lexical,
                weight: weight,
                detail: .lexical(matchedCategories: categories)
            )
        }
    }

    /// Build acoustic ledger entries from FeatureWindows in the span's time range.
    ///
    /// `.acoustic` captures the audio-energy break (RMS-drop) signal at
    /// span boundaries. MusicBed is captured separately by
    /// `MusicBedLedgerEvaluator`'s `.musicBed` entry. Both can co-emit
    /// on the same span when both signals are present, contributing as
    /// physically independent evidence kinds (boundary energy shift vs.
    /// sustained interior music coverage).
    ///
    /// playhead-sqhj history: a 2026-04-26 follow-up to gtt9.4 briefly
    /// fused music-bed coverage into this method's combined strength.
    /// Cross-review caught that the music-bed signal already reaches
    /// production via `MusicBedLedgerEvaluator`, so emitting `.acoustic`
    /// on a music-bed-only span double-counted the same physical
    /// evidence into the quorum gate's `distinctKinds.count`. The fused
    /// path was reverted; `.acoustic` once again fires only when
    /// `breakStrength > 0`.
    ///
    /// playhead-fqc8: Optionally also emits a `source: .breakAlignment`
    /// entry when (a) the span has a `.classifierSeed` anchor in its
    /// provenance AND (b) at least one `AcousticBreak` lies within
    /// `breakAlignmentTolerance` (±2.0s) of either span boundary AND
    /// (c) that break's `breakStrength` is `>= breakAlignmentMinStrength`
    /// (0.5). The alignment entry is what gates
    /// `PromotionTrack.classifierSeedQualified` in
    /// `DecisionMapper.computePromotionTrack`. Pass `acousticBreaks: []`
    /// (the default) to preserve pre-fqc8 behavior at every existing
    /// call site.
    ///
    /// playhead-fqc8 cycle-1 review: the alignment entry now uses the
    /// dedicated `EvidenceSourceType.breakAlignment` kind (not
    /// `.acoustic` + `.subSource = .breakAlignment`) so the alignment
    /// evidence is capped against `FusionWeightConfig.breakAlignmentCap`
    /// — its own honest budget — instead of stealing from the acoustic
    /// family budget. Callers that want the entries split by family
    /// must filter on `source` (see `buildEvidenceLedger`).
    func buildAcousticLedgerEntries(
        span: DecodedSpan,
        featureWindows: [FeatureWindow],
        fusionConfig: FusionWeightConfig,
        acousticBreaks: [AcousticBreak] = []
    ) -> [EvidenceLedgerEntry] {
        let spanWindows = featureWindows.filter { fw in
            fw.startTime < span.endTime && fw.endTime > span.startTime
        }

        var entries: [EvidenceLedgerEntry] = []
        if !spanWindows.isEmpty {
            let breakStrength = RegionScoring.computeRmsDropScore(windows: spanWindows)
            if breakStrength > 0 {
                let weight = min(breakStrength * fusionConfig.acousticCap, fusionConfig.acousticCap)
                entries.append(EvidenceLedgerEntry(
                    source: .acoustic,
                    weight: weight,
                    detail: .acoustic(breakStrength: breakStrength)
                ))
            }
        }

        // playhead-fqc8: classifier-seed break-alignment corroborator.
        if let alignment = breakAlignmentEntry(
            for: span,
            acousticBreaks: acousticBreaks,
            fusionConfig: fusionConfig
        ) {
            entries.append(alignment)
        }

        return entries
    }

    /// playhead-fqc8: Tolerance window (seconds) for matching an
    /// `AcousticBreak` to either edge of a `.classifierSeed`-anchored
    /// span. Symmetric (±2.0s on each side).
    static let breakAlignmentTolerance: Double = 2.0

    /// playhead-fqc8: Minimum `AcousticBreak.breakStrength` required for
    /// the matched break to count as a corroborator. The 0.5 floor is
    /// the same threshold the boundary refiner uses to discriminate a
    /// useful cue from background noise.
    static let breakAlignmentMinStrength: Double = 0.5

    /// playhead-fqc8: Returns a `.breakAlignment` ledger entry when the
    /// span carries `.classifierSeed` provenance AND at least one
    /// `AcousticBreak` aligns to either edge with sufficient strength.
    ///
    /// playhead-fqc8 cycle-1 review M-4: the weight now scales with the
    /// matched break's `breakStrength` so a barely-passing 0.5 strength
    /// produces a 0.10 contribution while a clean 1.0 strength reaches
    /// the full `breakAlignmentCap` (default 0.20). The minimum-strength
    /// gate (0.5) is enforced before this method, so the dynamic-range
    /// floor is `0.5 × breakAlignmentCap`. Returns `nil` when any
    /// precondition fails.
    private func breakAlignmentEntry(
        for span: DecodedSpan,
        acousticBreaks: [AcousticBreak],
        fusionConfig: FusionWeightConfig
    ) -> EvidenceLedgerEntry? {
        guard !acousticBreaks.isEmpty else { return nil }
        let hasClassifierSeed = span.anchorProvenance.contains {
            if case .classifierSeed = $0 { return true }
            return false
        }
        guard hasClassifierSeed else { return nil }

        let tolerance = Self.breakAlignmentTolerance
        let minStrength = Self.breakAlignmentMinStrength

        let aligned = acousticBreaks
            .filter { brk in
                let nearStart = abs(brk.time - span.startTime) <= tolerance
                let nearEnd = abs(brk.time - span.endTime) <= tolerance
                return (nearStart || nearEnd) && brk.breakStrength >= minStrength
            }
            .max(by: { $0.breakStrength < $1.breakStrength })

        guard let matched = aligned else { return nil }

        // M-4 strength scaling: 0.5 → 0.10, 1.0 → 0.20 with the default
        // `breakAlignmentCap = 0.20`. Clamp the strength to [0, 1] so
        // an out-of-band detector value can't inflate the contribution
        // past the cap.
        let normalizedStrength = max(0.0, min(1.0, matched.breakStrength))
        let weight = min(
            normalizedStrength * fusionConfig.breakAlignmentCap,
            fusionConfig.breakAlignmentCap
        )
        return EvidenceLedgerEntry(
            source: .breakAlignment,
            weight: weight,
            detail: .breakAlignment(breakStrength: matched.breakStrength)
        )
    }

    /// playhead-gtt9.16: build a single aggregated `.acoustic` ledger entry
    /// from the `AcousticFeaturePipeline` output that overlaps this span.
    ///
    /// The pipeline's per-window `combinedScore` is a weighted blend of the
    /// eight acoustic features with `AcousticFeatureFusion.Weights.defaultPriors`.
    /// We take the maximum across windows overlapping the span, multiply by
    /// `fusionConfig.acousticCap`, and return a single entry. Returns an
    /// empty array when:
    ///   * `pipelineFusion` is empty (no windows in the episode), or
    ///   * the maximum combined score across overlapping windows is zero
    ///     (all features returned zero — back-compat: no behaviour change
    ///     vs. pre-wiring).
    ///
    /// The entry uses `source: .acoustic` and encodes the combined score as
    /// `breakStrength` in the `.acoustic(...)` detail. Downstream
    /// `BackfillEvidenceFusion` caps each entry at `config.acousticCap`
    /// separately, so the pipeline contribution is additive to the existing
    /// RMS-drop `.acoustic` entry but each entry respects the same family
    /// budget. This matches gtt9.12's design (features are new evidence,
    /// not a replacement for the RMS-drop path).
    private func buildAcousticPipelineLedgerEntries(
        span: DecodedSpan,
        pipelineFusion: [AcousticFeatureFusion.WindowFusion],
        fusionConfig: FusionWeightConfig
    ) -> [EvidenceLedgerEntry] {
        guard !pipelineFusion.isEmpty else { return [] }
        let overlapping = pipelineFusion.filter { fusion in
            fusion.windowStart < span.endTime && fusion.windowEnd > span.startTime
        }
        guard !overlapping.isEmpty else { return [] }
        let maxCombined = overlapping.map(\.combinedScore).max() ?? 0
        guard maxCombined > 0 else { return [] }
        let weight = min(maxCombined * fusionConfig.acousticCap, fusionConfig.acousticCap)
        return [EvidenceLedgerEntry(
            source: .acoustic,
            weight: weight,
            detail: .acoustic(breakStrength: maxCombined)
        )]
    }

    /// 2026-04-23 Finding 4: build `.musicBed`-source ledger entries
    /// from the span's interior `MusicBedLevel` coverage.
    ///
    /// Delegates the threshold/weight logic to the pure
    /// `MusicBedLedgerEvaluator`; this method is just the span-window
    /// filter + plumbing.
    ///
    /// playhead-2hpn: when `showMusicBedSnapshot` is non-nil (set by the
    /// caller when the `scopedMusicBedGeneralization` flag is on AND a
    /// profile resolver is wired AND the podcastId is non-empty), the
    /// evaluator runs the flag-on weighting (0.10/0.25); otherwise it
    /// preserves the legacy presenceFraction*acousticCap path
    /// byte-identically.
    private func buildMusicBedLedgerEntries(
        span: DecodedSpan,
        featureWindows: [FeatureWindow],
        fusionConfig: FusionWeightConfig,
        showMusicBedSnapshot: ShowMusicBedProfileSnapshot? = nil,
        episodeDuration: Double = 0
    ) -> [EvidenceLedgerEntry] {
        let spanWindows = featureWindows.filter { fw in
            fw.startTime < span.endTime && fw.endTime > span.startTime
        }
        let jingleBoost = makeJingleBoost(
            for: span,
            snapshot: showMusicBedSnapshot,
            episodeDuration: episodeDuration
        )
        let result = MusicBedLedgerEvaluator.evaluate(
            spanWindows: spanWindows,
            fusionConfig: fusionConfig,
            jingleBoost: jingleBoost
        )
        if let entry = result.entry {
            return [entry]
        }
        return []
    }

    /// playhead-2hpn: build the optional `JingleBoost` context for a
    /// single span.
    ///
    /// Returns `nil` (legacy fusion behavior) when:
    ///   * the show snapshot is missing (flag off, no podcastId, or no
    ///     profile recorded yet), OR
    ///   * the episode duration is non-positive (no overlap region can
    ///     be computed).
    ///
    /// Returns a non-nil `JingleBoost` otherwise. `isConfirmed` mirrors
    /// the snapshot's confirmation state; `spanOverlapsJingle` is
    /// `true` when the span overlaps `[0, jingleSliceSeconds)` or
    /// `[episodeDuration - jingleSliceSeconds, episodeDuration)` — the
    /// intro/outro slices the evaluator hashes against.
    private func makeJingleBoost(
        for span: DecodedSpan,
        snapshot: ShowMusicBedProfileSnapshot?,
        episodeDuration: Double
    ) -> MusicBedLedgerEvaluator.JingleBoost? {
        guard let snapshot, episodeDuration > 0 else { return nil }
        let overlaps = ShowMusicBedProfileEvaluator.spanOverlapsJingleRegion(
            spanStart: span.startTime,
            spanEnd: span.endTime,
            episodeDuration: episodeDuration
        )
        return MusicBedLedgerEvaluator.JingleBoost(
            isConfirmed: snapshot.isConfirmed,
            spanOverlapsJingle: overlaps
        )
    }

    /// Fraction of ``FusionWeightConfig/catalogCap`` each selected catalog entry
    /// contributes, before the cap. Twenty entries saturate the cap.
    ///
    /// playhead-0u3e named it so the corpus lane's control arm multiplies by the
    /// SAME constant the site does, rather than by a `0.05` typed twice. A
    /// literal restated in a control arm is a measurement of the restatement.
    static let catalogLedgerWeightPerEntry: Double = 0.05

    /// Build catalog ledger entries from the catalog evidence THIS span can hear.
    ///
    /// playhead-0u3e. This used to select on
    /// `coverageStartTime`/`coverageEndTime` — `firstTime`/`lastTime`, the HULL
    /// of a deduplicated entry — and its own comment named the widening as
    /// intended ("repeated evidence expands its coverage window across the
    /// earliest and latest occurrence"). It is not: a sponsor read in the
    /// pre-roll and again in the post-roll has a "coverage" span covering the
    /// episode, so it counted toward the catalog weight of every span in
    /// between, and the inflation grew with how many repeats the episode had.
    /// The weight is `min(count * catalogLedgerWeightPerEntry * catalogCap,
    /// catalogCap)` over the COUNT of selected entries — spelled with the
    /// constant, because restating `0.05` in the prose beside the constant that
    /// exists to stop it being restated is the same defect one layer down — and
    /// the entry's mere PRESENCE is a `.catalog`
    /// kind in three quorum gates (`metadataCorroborationGate`,
    /// `quorumGateForFMConsensus`, `quorumGateForFMAcoustic`) — so a hull
    /// selection buys a span both score and corroboration from evidence it
    /// never heard, and the result reaches `ad_windows`.
    ///
    /// ``EvidenceEntry/locatedInTimeWindow(start:end:)`` is the shared answer to
    /// "which mention applies here", extracted by playhead-rty3 from
    /// playhead-ad9n's `PromptEvidenceEntry.forWindow`. It is the right member
    /// of the pair here — rather than ``EvidenceEntry/locatedInWindow(seeing:)``
    /// used directly, or the per-occurrence fan-out `SpecialistScanPlanner`
    /// needs — because this loop runs INSIDE one window and must yield AT MOST
    /// ONE entry per `evidenceRef`: `overlapping.count` is a count of distinct
    /// evidence, and a fan-out would let one repeated sponsor vote twice on the
    /// span that does contain both of its mentions.
    ///
    /// Note the boundary convention changes with the primitive: the deleted
    /// expression was half-open (`<`, `>`) and `locatedInTimeWindow` is CLOSED
    /// on both ends, deliberately (rty3), so a mention whose interval merely
    /// TOUCHES a span edge now surfaces. That is the one direction in which
    /// this change can ADD evidence, and it is measured rather than assumed —
    /// `CatalogLedgerHullCorpusEvalTests` reports every such entry with a
    /// witness instead of asserting there are none. On the 2026-08-02 corpus
    /// there is exactly ONE across 74 rows, and it is not float noise: the CTA
    /// phrase "head to" spans 4408.1646–4408.4400 and the span begins at
    /// 4408.4400, i.e. the sentence that introduces the ad ends exactly where
    /// the ad's own span was snapped to begin. Admitting it is the rty3 rule
    /// working, not a leak.
    func buildCatalogLedgerEntries(
        span: DecodedSpan,
        entries: [EvidenceEntry],
        fusionConfig: FusionWeightConfig
    ) -> [EvidenceLedgerEntry] {
        let overlapping = entries.compactMap { entry in
            entry.locatedInTimeWindow(start: span.startTime, end: span.endTime)
        }
        guard !overlapping.isEmpty else { return [] }

        let weight = min(
            Double(overlapping.count) * Self.catalogLedgerWeightPerEntry * fusionConfig.catalogCap,
            fusionConfig.catalogCap
        )
        return [EvidenceLedgerEntry(
            source: .catalog,
            weight: weight,
            detail: .catalog(entryCount: overlapping.count),
            // playhead-epfk: stamp the in-pipeline transcript-token catalog
            // so NARL replay can distinguish it from `AdCatalogStore`
            // fingerprint matches that share the `.catalog` source label.
            subSource: .transcriptCatalog
        )]
    }

    /// Estimate transcript quality from the projected atom evidence.
    /// `internal` for unit-testing the 30% anchor threshold (see BackfillEvidenceFusionTests).
    func estimateTranscriptQuality(atoms: [AtomEvidence]) -> TranscriptQuality {
        guard !atoms.isEmpty else { return .degraded }
        // Use the proportion of anchored atoms as a quality proxy.
        // If > 30% of atoms are anchored, quality is considered good.
        let anchoredFraction = Double(atoms.filter(\.isAnchored).count) / Double(atoms.count)
        return anchoredFraction > 0.3 ? .good : .degraded
    }

    /// Build an AdWindow from a fusion DecisionResult.
    /// playhead-epfk: `catalogStoreMatchSimilarity` carries the per-span
    /// top similarity from `AdCatalogStore.matches`. Pass `nil` when the
    /// catalog store was not wired or no match was attempted; `0` means
    /// "wired and queried but no match cleared the floor"; positive
    /// values surface in the corpus export so NARL can measure the
    /// fingerprint-store firing rate.
    /// playhead-hdgk / playhead-2350: the per-edge auto-skip anchor tiers a
    /// fusion span persists are derived by `SpanExtentSupport.derive`, which is
    /// also the input to the unanchored-edge auto-skip block — one derivation,
    /// so the persisted tiers and the gate cannot drift apart. `runBackfill`'s
    /// emission loop calls it once per span and passes `startAnchor` / `endAnchor`
    /// into `buildFusionAdWindow` below. (A `static` tuple-returning wrapper used
    /// to live here; it was removed once the emission loop stopped calling it,
    /// rather than left as a second spelling of the same rule.)
    /// The persisted `AdDecisionState` for one fusion span.
    ///
    /// `internal` and `static` so the mapping has ONE spelling and can be
    /// exercised exhaustively over (policy action x gate x scope) without
    /// reaching into `buildFusionAdWindow`'s parameters — see
    /// `FusionEmissionShapeTests.decisionStateMappingIsExhaustive`.
    ///
    /// - Parameter userCorrectionIsSpanScoped: whether a span-scoped suppressor
    ///   actually reaches this span
    ///   (`CorrectionFactorSnapshot.hasSpanScopedSuppressor(overlapping:_:)`).
    ///   Deliberately NOT defaulted: there is one call site, and a default
    ///   would let a future one withhold a banner by omission.
    static func fusionDecisionState(
        policyAction: SkipPolicyAction,
        eligibilityGate: SkipEligibilityGate,
        userCorrectionIsSpanScoped: Bool
    ) -> AdDecisionState {
        switch policyAction {
        case .autoSkipEligible:
            return eligibilityGate == .eligible ? .confirmed : .candidate
        case .detectOnly, .logOnly:
            // logOnly and detectOnly: persist but don't auto-skip.
            return eligibilityGate == .blockedByUserCorrection
                && userCorrectionIsSpanScoped
                ? .suppressed
                : .confirmed
        case .suppress:
            return .suppressed
        }
    }

    private func buildFusionAdWindow(
        span: DecodedSpan,
        decision: DecisionResult,
        policyAction: SkipPolicyAction,
        analysisAssetId: String,
        catalogStoreMatchSimilarity: Double? = nil,
        catalogMatch: CatalogMatch? = nil,
        startEdgeAnchor: AutoSkipEdgeAnchor = .unanchored,
        endEdgeAnchor: AutoSkipEdgeAnchor = .unanchored,
        fusionSplitDiscriminator: String? = nil,
        userCorrectionIsSpanScoped: Bool
    ) -> AdWindow {
        // Map fusion policy action + gate to AdDecisionState for persistence.
        // autoSkipEligible: confirmed when gate passes, candidate otherwise.
        // detectOnly/logOnly: confirmed (no applied-skip banner; data preserved
        //   for Phase 7) — EXCEPT when the user vetoed this span.
        // suppress: always suppressed (never shown to user).
        //
        // playhead-ar60 (mechanism 3): `detectOnly` used to persist
        // `.confirmed` UNCONDITIONALLY, and `rawPolicyAction` is
        // `SkipPolicyMatrix.action(for: .unknown, ownership: .unknown)` —
        // `.detectOnly` for every fusion span that is not promoted. So a span
        // whose gate was `.blockedByUserCorrection` (the user's own "not an
        // ad") came back as a CONFIRMED row, and on episode DE0784D8 Dan was
        // shown a confirmed banner for a span he had vetoed 7 seconds earlier.
        // `.blockedByUserCorrection` is the one gate that encodes an explicit
        // human judgement about THIS span, and re-presenting it is the system
        // arguing with the user. `.suppressed` is the existing "never shown"
        // terminal state: it is reconcilable (so a later run that no longer
        // sees a veto can flip it back), it is excluded from
        // `SkipOrchestrator`'s cross-launch preload, and it is what the
        // `.suppress` policy action already produces.
        //
        // Deliberately narrow. Every OTHER blocked gate
        // (`.blockedByEvidenceQuorum`, `.blockedByFMConsensus`, `.markOnly`,
        // …) still persists `.confirmed`: those are the system's own
        // uncertainty, and a banner Dan can answer is exactly what that
        // population is for (feedback_banner_is_a_skip_affordance). A general
        // low-confidence EMISSION FLOOR is a recall decision and is NOT taken
        // here — see the bead comment; filed as playhead-4obf.
        //
        // AND ONLY WHEN THE VETO WAS ABOUT THIS SPAN — `userCorrectionIsSpanScoped`.
        //
        // ar60 R1 review: the first cut of this arm keyed on the gate alone and
        // justified itself as "the user judged THIS span", with a note saying a
        // show-wide veto could mute a whole episode's banners but was
        // unreachable because "the UI only writes `exactTimeSpan`". That
        // premise was false in the shipped tree, twice over:
        //
        //   * `AdBannerView.alwaysSkipSponsorAction` — the "Always skip this
        //     sponsor" button on the auto-skipped banner (playhead-3bv.4) —
        //     writes `CorrectionScope.sponsorOnShow` with
        //     `correctionType: .falsePositive` through
        //     `NowPlayingView.onAlwaysSkipSponsorAsync`. One tap.
        //   * `PersistentUserCorrectionStore.recordVeto(span:ledgerEntries:)`
        //     writes a second `sponsorOnShow` event for every `brandSpan`
        //     evidence entry on a vetoed span.
        //
        // A fresh suppressor carries a decay weight near 1, so passthrough is
        // near 0 and EVERY span in the asset lands under the 0.40 correction
        // gate — one tap would have taken every fusion banner in the episode
        // away, on a gesture that says something about a SPONSOR and nothing
        // about where the ads are. The device pull showing zero show-wide
        // scopes only says Dan has not pressed that button yet.
        //
        // So the arm now asks the question its own justification asks: did a
        // SPAN-SCOPED suppressor actually reach this span? A show-wide scope,
        // and an unplaceable correction (which the snapshot deliberately
        // records show-wide), answer no and keep the banner — the pre-ar60
        // behaviour, which is the safe direction when the banner is a skip
        // affordance. The witnessed DE0784D8 case is unaffected: those five
        // vetoes were `.exactTimeSpan`, so a span they overlap still suppresses
        // and a span 2,600 s away is not gated at all.
        let decisionState = Self.fusionDecisionState(
            policyAction: policyAction,
            eligibilityGate: decision.eligibilityGate,
            userCorrectionIsSpanScoped: userCorrectionIsSpanScoped
        )

        return AdWindow(
            // playhead-ud4n: content-addressed id (Design B) instead of a fresh
            // UUID. Keyed on (assetId, detectorVersion, span ordinals) so an
            // identical rerun mints an identical id — the reconcile is a no-op
            // and `AdDetectionService.reconcileBackfillWindows` retires nothing.
            // Extra finalizer split children inherit the same ordinals, so
            // those children alone carry a deterministic discriminator.
            id: BackfillJobRunner.makeFusionWindowId(
                analysisAssetId: analysisAssetId,
                detectorVersion: config.detectorVersion,
                spanStartOrdinal: span.firstAtomOrdinal,
                spanEndOrdinal: span.lastAtomOrdinal,
                splitDiscriminator: fusionSplitDiscriminator
            ),
            analysisAssetId: analysisAssetId,
            startTime: span.startTime,
            endTime: span.endTime,
            // playhead-ar60 (mechanism 2): the two quantities stop sharing a
            // column. `confidence` is the DETECTION number — what the evidence
            // says — matching every other producer of this column; the
            // ACTUATION number, detection after calibration and after the
            // user-correction factor, goes to `skipConfidence`.
            //
            // Before this bead these were reversed and lossy: `skipConfidence`
            // went into `confidence`, `proposalConfidence` went into
            // `metadataConfidence`, and the metadata extractor then OVERWROTE
            // that with its own unrelated number (40 of 40 fusion rows on the
            // 2026-08-02 pull). So the detection confidence of a persisted
            // fusion row was not recoverable at all, while the 0.7 preload
            // DETECTOR floor, the cue gates, catalog/repeated-ad learning and
            // the "AD n%" transcript label all read the actuation number in
            // its place.
            confidence: decision.proposalConfidence,
            skipConfidence: decision.skipConfidence,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: decisionState.rawValue,
            detectorVersion: config.detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: span.startTime,
            metadataSource: "fusion-v1",
            // playhead-ar60: `nil`, not `proposalConfidence`. This column is
            // the METADATA EXTRACTOR's confidence in the advertiser/product it
            // named, and `AdBannerView.bannerCopy` gates the advertiser name on
            // it at 0.60 — so parking a detection score here asked a banner
            // whether to name a sponsor using a number about something else,
            // for a row whose `advertiser` is `nil` two lines above. Every
            // other mark composer writes `nil` here for exactly this reason,
            // and the extractor overwrote it anyway on every row that reached
            // it. `nil` = "no metadata claim", which is the truth at fusion
            // time; the detection score now has its own column.
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            // playhead-ux6r: stamp the eligibility gate on the persisted
            // fusion window. Without this, the live decision-time gate
            // (consumed via AdDecisionResult) is silently dropped on
            // restart, and SkipOrchestrator.beginEpisode preload re-arms
            // a previously-demoted markOnly span for auto-skip. The
            // value here is the same SkipEligibilityGate.rawValue that
            // decision_events records for the same window, keeping the
            // ad_windows row and decision_events row consistent.
            eligibilityGate: decision.eligibilityGate.rawValue,
            catalogStoreMatchSimilarity: catalogStoreMatchSimilarity,
            catalogFingerprintVersion: catalogMatch?.entry.acousticFingerprint.version.rawValue,
            catalogMatchedEntryId: catalogMatch?.entry.id.uuidString,
            catalogMatchedShowId: catalogMatch?.entry.showId,
            catalogMatchedLearningSource:
                catalogMatch?.entry.learningSource.rawValue,
            catalogMatchedLearningLifecycle:
                catalogMatch?.entry.learningLifecycle.rawValue,
            // playhead-hdgk: persist the per-edge anchor tier derived at
            // fusion time so the (dormant) auto-skip edge-padding policy can
            // classify this span by its real provenance on ingest, instead of
            // defaulting every edge to `.unanchored`. Default `.unanchored`
            // here means "no derived anchor" — flag-ON auto-skips nothing for
            // such a span, the conservative posture.
            startEdgeAnchor: startEdgeAnchor.rawValue,
            endEdgeAnchor: endEdgeAnchor.rawValue
        )
    }

    // MARK: - Backfill ↔ hot-path reconciliation (playhead-ud4n)

    /// playhead-ud4n: nonterminal decision states backfill is authoritative
    /// over. `.applied` / `.reverted` are terminal user-facing history and are
    /// deliberately absent. `.suppressed` IS included so a rerun can flip a
    /// previously-suppressed span and so backfill's own prior output is
    /// reconciled against the new run (no accumulation).
    static let reconcilableBackfillDecisionStates: Set<String> = [
        AdDecisionState.candidate.rawValue,
        AdDecisionState.confirmed.rawValue,
        AdDecisionState.suppressed.rawValue
    ]

    /// playhead-ud4n: boundary states that mark a row as user-owned or
    /// correction-replay — never reconciled away by backfill. Mirrors the
    /// local-only boundary states the cross-user-sharing export protects.
    // Literals (not `Self.correctionReplayBoundaryState`) because a static
    // stored-property initializer may not reference the covariant `Self` of a
    // non-final class. "correctionReplay" mirrors `correctionReplayBoundaryState`;
    // the pair is pinned by the predicate axis test.
    static let reconcileProtectedBoundaryStates: Set<String> = [
        "correctionReplay",
        "userMarked",
        "userConfirmedSuggested",
        // playhead-xsdz.36.4: day-0 byte-exact rediff marks are DETERMINISTIC
        // ground truth for the user's own stitch — a later analysis run's
        // transcript/FM fusion would not re-emit them, so they must NOT be
        // reconciled away (literal mirrors `dayZeroRediffByteExactBoundaryState`;
        // pinned by `RediffActivationWiringTests.dayZeroMarksAreReconcileProtected`).
        "dayZeroRediffByteExact"
    ]

    /// playhead-ud4n: the *reconcilable invariant* (correctness backbone). A
    /// persisted `AdWindow` row is reconcilable — backfill is authoritative and
    /// may retire or replace it — iff ALL hold:
    ///   • `detectorVersion == detectorVersion` (scope: current detector only;
    ///     a stale other-version row is left untouched),
    ///   • `decisionState ∈ {candidate, confirmed, suppressed}` (nonterminal),
    ///   • `!id.hasPrefix("shared-")` (imported cross-user shares aren't ours),
    ///   • `boundaryState ∉ {correctionReplay, userMarked,
    ///     userConfirmedSuggested}` (user-owned / correction-replay preserved).
    /// Every reconcilable row absent from the authoritative backfill output is
    /// retired. Factored `static` over its inputs so it is directly
    /// unit-testable without an actor hop or a live pipeline.
    /// playhead-527u: merge possibly-overlapping `[start, end)` ranges into a
    /// disjoint, sorted set. Used by `reconcileBackfillWindows`' dominance
    /// dedupe so a covered-duration sum cannot double-count a span covered by
    /// two overlapping user marks (which would inflate the dominance fraction
    /// past 1.0 and over-drop). Degenerate/inverted ranges are dropped.
    static func mergedTimeRanges(
        _ ranges: [(start: Double, end: Double)]
    ) -> [(start: Double, end: Double)] {
        let sorted = ranges.filter { $0.end > $0.start }.sorted { $0.start < $1.start }
        var merged: [(start: Double, end: Double)] = []
        for r in sorted {
            if let last = merged.last, r.start <= last.end {
                merged[merged.count - 1].end = Swift.max(last.end, r.end)
            } else {
                merged.append(r)
            }
        }
        return merged
    }

    static func isReconcilableBackfillWindow(
        _ window: AdWindow,
        detectorVersion: String
    ) -> Bool {
        window.detectorVersion == detectorVersion
            && reconcilableBackfillDecisionStates.contains(window.decisionState)
            && !window.id.hasPrefix("shared-")
            && !reconcileProtectedBoundaryStates.contains(window.boundaryState)
    }

    /// playhead-ud4n: reconcile the authoritative backfill fusion output
    /// against the persisted `ad_windows` rows for this asset (Design B — pure
    /// set-difference over content-addressed ids, no IoU matcher). Every
    /// reconcilable existing row (see ``isReconcilableBackfillWindow``) whose id
    /// is NOT in the new fusion set is retired; the fusion windows are returned
    /// unchanged for INSERT-OR-REPLACE. Because fusion ids are content-
    /// addressed, an identical rerun produces identical ids ⇒ an empty retire
    /// set and a no-op replace (idempotency by construction). A superseded hot
    /// candidate (its own `UUID` id) is never in the new fusion set, so it is
    /// always retired — satisfying the FP-retirement AC independent of any
    /// geometric match quality.
    func reconcileBackfillWindows(
        _ fusionWindows: [AdWindow],
        analysisAssetId: String
    ) async throws -> (windows: [AdWindow], retiredIDs: Set<String>) {
        let existing = try await store.fetchAdWindows(assetId: analysisAssetId)
        var reconcilableIDs = Set<String>()
        var protectedIDs = Set<String>()
        for row in existing {
            if Self.isReconcilableBackfillWindow(row, detectorVersion: config.detectorVersion) {
                reconcilableIDs.insert(row.id)
            } else {
                protectedIDs.insert(row.id)
            }
        }
        // playhead-527u: user-mark DOMINANCE dedupe (prevents the force-anchor
        // DOUBLE window WITHOUT retiring a genuinely-distinct overlapping ad).
        // A user's EXPLICIT mark (`recordUserMarkedAd` → `boundaryState ==
        // userMarked`) is the definitive, AUTO-SKIP-ELIGIBLE ground truth for
        // its region (its row is stamped `eligibilityGate == .eligible`) and
        // surfaces as its own window. The 527u confirm mask force-anchors that
        // region, so backfill re-derives a decoded span over it that fuses into
        // a NEW `acousticRefined` fusion window. Content-addressed on span
        // ordinals, that window has a DIFFERENT id than the userMarked row's
        // UUID, so the id-collision guard below does NOT catch it — left alone
        // it co-exists with the userMarked row, producing a DOUBLE window over
        // one ad. Drop ONLY the REDUNDANT re-derived window — the one the user
        // mark reciprocally DOMINATES (`markDominates` below) — so the eligible
        // userMarked row is the single surfaced window for that ad and the skip
        // uses the USER'S boundaries. A distinct/wider ad that merely overlaps
        // the marked span, OR a small distinct ad sitting mostly inside a larger
        // mark, is NOT dominated and is KEPT: a blanket overlap-drop would
        // silently retire a legitimate auto-skip-eligible detection (a
        // precision/coverage LOSS).
        // This is the backfill mirror of the hot-path `correctionReplayCandidates`
        // overlap-skip; the re-emitted DECODED span (transcript-overlay /
        // summarizer ad-presence) is untouched — only the redundant AdWindow is
        // suppressed.
        let userMarkedRanges = Self.mergedTimeRanges(
            existing
                .filter { $0.boundaryState == "userMarked" }
                .map { (start: $0.startTime, end: $0.endTime) }
        )
        // playhead-527u (reviewer 527u): RECIPROCAL dominance — the window must be
        // "this marked ad re-detected", which requires BOTH:
        //   (a) the user-mark UNION covers ≥half the window — the window is mostly
        //       marked ground, NOT a distinct WIDER ad that merely overlaps an
        //       edge (guards the R3 wide-ad-survives case); AND
        //   (b) the window covers ≥half of at least ONE single mark — it spans the
        //       mark rather than sitting mostly-inside it. Without (b) a SMALL
        //       DISTINCT ad lying mostly under a LARGE mark (e.g. mark [35,55],
        //       ad [48,58] → 70% of the ad is inside the mark, so (a) alone drops
        //       it and loses the skip of [55,58]) would be wrongly retired. The
        //       force-anchored re-detection always covers the WHOLE mark (every
        //       confirmed atom is anchored), so (b) holds for the true double;
        //       a sliver-overlap distinct ad covers only a fraction of the mark,
        //       so (b) fails and it is KEPT (precision/coverage preserved).
        func markDominates(_ w: AdWindow) -> Bool {
            let width = w.endTime - w.startTime
            guard width > 0 else { return false }
            let covered = userMarkedRanges.reduce(0.0) { acc, r in
                acc + Swift.max(0, Swift.min(w.endTime, r.end) - Swift.max(w.startTime, r.start))
            }
            guard covered / width >= 0.5 else { return false }  // (a)
            return userMarkedRanges.contains { r in            // (b)
                let markWidth = r.end - r.start
                guard markWidth > 0 else { return false }
                let overlap = Swift.max(
                    0, Swift.min(w.endTime, r.end) - Swift.max(w.startTime, r.start)
                )
                return overlap / markWidth >= 0.5
            }
        }
        // Ids of the redundant re-derived windows the user mark dominates.
        // Excluded from `newIDs` below so that a copy persisted by a PRE-fix run
        // (same content-addressed id, now reconcilable) is RETIRED via the
        // set-difference rather than silently protected from retirement by its
        // own new-but-dropped id.
        let userMarkOverlapDroppedIDs = Set(
            fusionWindows.filter(markDominates).map(\.id)
        )
        // Terminal-collision guard: content-addressed ids are keyed on span
        // ordinals only (not decisionState), so re-detecting a span the user
        // already auto-skipped (`.applied`) or listened-through (`.reverted`)
        // mints the SAME fusion id as the persisted terminal row. A blind
        // INSERT-OR-REPLACE would clobber that terminal/user-owned/imported/
        // other-version history. Drop any new window whose id collides with a
        // protected (non-reconcilable) existing row so the protected row wins —
        // the same precedence the orchestrator applies (it refuses to reprocess
        // `.applied` / `.reverted`). Preserves the "terminal history preserved"
        // AC even on an ad-signal rerun, not just a clean one.
        let windowsToPersist = fusionWindows.filter {
            !protectedIDs.contains($0.id) && !userMarkOverlapDroppedIDs.contains($0.id)
        }
        // Pure set-difference: every reconcilable existing row NOT re-produced by
        // this backfill is retired. A reconcilable row re-produced under the same
        // id is replaced in-place (idempotent), so it is excluded from retire.
        // A user-mark-overlap-dropped id is treated as NOT re-produced (removed
        // from `newIDs`) so any stale persisted copy of it is retired.
        let newIDs = Set(fusionWindows.map(\.id)).subtracting(userMarkOverlapDroppedIDs)
        let retiredIDs = reconcilableIDs.subtracting(newIDs)
        return (windowsToPersist, retiredIDs)
    }

    // MARK: - Specialist mark reconciliation (playhead-b6jq PR5)

    /// playhead-b6jq PR 5: detector version stamped on every specialist mark.
    /// Re-exported from `SpecialistMarkComposer.detectorVersion` (single source of
    /// truth) so the version-scoped reconcile below and the composer agree by
    /// construction. Because `isReconcilableBackfillWindow` scopes to an exact
    /// `detectorVersion`, specialist marks (`"specialist-ft-v2"`) are INVISIBLE to
    /// the FM reconcile (`"detection-v1"`) and vice versa — neither can clobber
    /// the other.
    static let specialistDetectorVersion = SpecialistMarkComposer.detectorVersion

    /// playhead-b6jq PR 5: pure set-difference reconcile for specialist marks,
    /// scoped to `detectorVersion == "specialist-ft-v2"`. The exact mirror of
    /// `reconcileBackfillWindows`' body — same content-addressed set-difference,
    /// same terminal-collision guard — but pinned to the specialist version via
    /// `isReconcilableBackfillWindow(_,detectorVersion:)`. Factored `static` over
    /// its inputs so BOTH compose sites (the service's Step 18 and
    /// `BackfillJobRunner.runSpecialistHostReadScan`'s tail, which has no service
    /// instance) share ONE reconcile invariant, and so it is directly unit-testable
    /// without an actor hop.
    ///
    /// Cannot clobber FM / `shared-` / user-owned rows: those are never
    /// reconcilable under the specialist version, so they land in `protectedIDs`
    /// and never in the retire set. Idempotent: content-addressed ids mean an
    /// identical recompose retires nothing and replaces in place.
    static func reconcileSpecialistMarkSets(
        newMarks: [AdWindow],
        existingWindows: [AdWindow]
    ) -> (windows: [AdWindow], retiredIDs: Set<String>) {
        reconcileVersionScopedMarkSets(
            newMarks: newMarks,
            existingWindows: existingWindows,
            detectorVersion: specialistDetectorVersion
        )
    }

    /// The version-scoped mark reconcile, generalized over `detectorVersion` so
    /// every additive mark producer shares ONE reconcile invariant instead of
    /// copying it. Today: specialist marks (`"specialist-ft-v2"`, playhead-b6jq
    /// PR5) and ad-pod continuation marks (`"pod-continuation-v1"`,
    /// playhead-xsdz.65).
    ///
    /// Rows of any OTHER detector version are not reconcilable under this scope,
    /// so they land in `protectedIDs` and can neither be retired nor replaced —
    /// which is what keeps two additive producers from clobbering each other, and
    /// keeps both away from FM (`"detection-v1"`), `shared-` and user-owned rows.
    static func reconcileVersionScopedMarkSets(
        newMarks: [AdWindow],
        existingWindows: [AdWindow],
        detectorVersion: String
    ) -> (windows: [AdWindow], retiredIDs: Set<String>) {
        var reconcilableIDs = Set<String>()
        var protectedIDs = Set<String>()
        for row in existingWindows {
            if isReconcilableBackfillWindow(row, detectorVersion: detectorVersion) {
                reconcilableIDs.insert(row.id)
            } else {
                protectedIDs.insert(row.id)
            }
        }
        // Terminal-collision guard (mirrors reconcileBackfillWindows): drop any new
        // mark whose content-addressed id collides with a protected (non-
        // reconcilable) existing row so the protected row wins.
        let windowsToPersist = newMarks.filter { !protectedIDs.contains($0.id) }
        let newIDs = Set(newMarks.map(\.id))
        let retiredIDs = reconcilableIDs.subtracting(newIDs)
        return (windowsToPersist, retiredIDs)
    }

    /// playhead-b6jq PR 5: store-backed wrapper mirroring `reconcileBackfillWindows`
    /// — fetch the asset's persisted `ad_windows` fresh (so the reconcile sees the
    /// live snapshot right before persist, closing the same race the store-level
    /// terminal guard also covers) and delegate the set-difference to
    /// `reconcileSpecialistMarkSets`. Returns `(windows, retiredIDs)` for the
    /// reused `store.reconcileBackfillAdWindows` INSERT-OR-REPLACE + DELETE txn.
    func reconcileSpecialistMarks(
        _ marks: [AdWindow],
        analysisAssetId: String
    ) async throws -> (windows: [AdWindow], retiredIDs: Set<String>) {
        let existing = try await store.fetchAdWindows(assetId: analysisAssetId)
        return Self.reconcileSpecialistMarkSets(newMarks: marks, existingWindows: existing)
    }

    // MARK: - Read-side correction mask selection (playhead-xsdz.34)

    /// Select the projector's `CorrectionMaskProvider` (design §2.4). Pure over
    /// its arguments (no actor state) so both call sites — `runBackfill` and
    /// `runPhase5ProjectorPhase` — resolve the veto mask identically and the
    /// flag branch is directly unit-testable (T4).
    ///
    /// No store ⇒ `NoCorrectionMaskProvider()` (preserves the async-install race
    /// window note at the `correctionStore` wiring: until the store lands there
    /// are no masks — the safe default).
    ///
    /// Direction split (playhead-527u):
    ///   • `.falsePositive` VETO masks stay gated by `enabled`
    ///     (`userCorrectionReadSideEnabled`) — the suppress-direction A/B
    ///     (xsdz.36). Unchanged.
    ///   • `.falseNegative` CONFIRM masks are ALWAYS applied when a store is
    ///     present. Preserving a user's ADDED mark is trust-critical, has no
    ///     recall/precision downside (it only re-anchors a region the user
    ///     explicitly said IS an ad), and only touches assets the user actually
    ///     corrected — so it must not ride the veto A/B flag.
    ///
    /// A non-corrected asset yields BOTH sets empty ⇒ `NoCorrectionMaskProvider`
    /// ⇒ byte-identical to pre-527u (guardrail: no change to non-corrected
    /// episodes' analysis). Flag-off + only a veto ⇒ veto excluded, confirm empty
    /// ⇒ `NoCorrectionMaskProvider` (preserves the xsdz.34 flag-off identity).
    ///
    /// playhead-q6y3 — one case the sentence above no longer characterises, and
    /// why it is left alone. `activeFalseNegativeScopes` does not filter by
    /// scope SHAPE, so an asset whose only correction is an always-skip-sponsor
    /// tap now returns a non-empty `confirmedScopes` and selects
    /// `StoreBackedCorrectionMaskProvider` where it would previously have
    /// selected `NoCorrectionMaskProvider`. The MASKS are identical either way
    /// — `AtomEvidence.splitScopes` drops every show-wide scope, so both
    /// providers answer `[:]` — so this is a provider-identity difference, not
    /// a behavioural one. Filtering here instead would duplicate `splitScopes`'
    /// shape knowledge in a second place, which is how the two drift.
    static func makeCorrectionMaskProvider(
        enabled: Bool,
        store: (any UserCorrectionStore)?,
        analysisAssetId: String,
        atoms: [TranscriptAtom]
    ) async -> any CorrectionMaskProvider {
        guard let store else { return NoCorrectionMaskProvider() }
        let confirmedScopes = await store.activeFalseNegativeScopes(for: analysisAssetId)
        let vetoScopes = enabled
            ? await store.activeFalsePositiveScopes(for: analysisAssetId)
            : []
        guard !confirmedScopes.isEmpty || !vetoScopes.isEmpty else {
            return NoCorrectionMaskProvider()
        }
        return StoreBackedCorrectionMaskProvider(
            fromVetoScopes: vetoScopes,
            confirmedScopes: confirmedScopes,
            atoms: atoms
        )
    }

    // MARK: - Phase 5 Projector Phase (playhead-4my.5)

    /// Runs AtomEvidenceProjector + MinimalContiguousSpanDecoder on the Phase 4
    /// bundles and records the resulting decoded spans in the injected observer.
    /// Failures are logged and swallowed — shadow telemetry must never affect
    /// user-visible behavior.
    // Not `private`: the playhead-xsdz.20 Phase-5 clobber-guard test drives this
    // directly to prove no superseded-id row reappears over a slot-owned asset.
    func runPhase5ProjectorPhase(
        observer: Phase5ProjectorObserver,
        bundles: [RegionFeatureBundle],
        chunks: [TranscriptChunk],
        analysisAssetId: String
    ) async {
        guard !chunks.isEmpty else { return }

        // Atomize the same transcript the Phase 4 shadow phase used — which
        // means the CANONICAL one (playhead-iu0t). This was the third surviving
        // instance of the pre-hc7e `filter { pass == "final" }` collapse, and
        // here it made the comment above self-refuting: Phase 4 atomizes
        // `runBackfill`'s `canonicalChunks`, so re-collapsing to final-only
        // produced a DIFFERENT atom sequence and therefore different atom
        // ordinals, and the `bundles` argument — whose region ordinals were
        // assigned against the Phase 4 atoms — would have been projected onto
        // atoms that are not the ones it names.
        //
        // No production caller reaches this method (see the doc above); its two
        // call sites are `SpliceSlotOwnershipPhase5GuardTests`, both of which
        // pass an all-`final` fixture that canonicalize returns unchanged. So
        // this is a no-op today by measurement, and correct the moment anything
        // does call it. The dead-in-production half is filed as playhead-tqqu.
        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: TranscriptChunkCanonicalizer.canonicalize(chunks).chunks,
            analysisAssetId: analysisAssetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )
        guard !atoms.isEmpty else {
            logger.warning("Phase 5 projector: no atoms produced for asset \(analysisAssetId)")
            return
        }

        // Build the evidence catalog.
        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: analysisAssetId,
            transcriptVersion: atoms[0].atomKey.transcriptVersion
        )

        // Project atoms.
        // playhead-xsdz.34: same read-side selection as `runBackfill` — flag-off
        // / nil-store ⇒ `NoCorrectionMaskProvider()` ⇒ byte-identical.
        let projector = AtomEvidenceProjector()
        let maskProvider = await Self.makeCorrectionMaskProvider(
            enabled: config.userCorrectionReadSideEnabled,
            store: correctionStore,
            analysisAssetId: analysisAssetId,
            atoms: atoms
        )
        let evidence = await projector.project(
            regions: bundles,
            catalog: catalog,
            atoms: atoms,
            correctionMaskProvider: maskProvider
        )

        // Decode spans.
        let decoder = MinimalContiguousSpanDecoder()
        let spans = decoder.decode(atoms: evidence, assetId: analysisAssetId)

        // playhead-xsdz.20 / xsdz.29 CLOBBER GUARD: this projector upserts FRESHLY
        // decoded spans that never carry a width-ownership marker. Over a
        // width-owned asset (acoustic splice OR rediff) a fresh decode re-mints
        // the ORIGINAL ordinals → the original `makeId` → it would RE-INSERT the
        // superseded-id row the backfill slot pass deleted (resurrecting the ghost
        // row nested inside the slot span). So if this asset already has ANY
        // persisted width-owned rows, skip the upsert entirely and leave the
        // slot-owned rows authoritative. Checks BOTH markers via `isWidthOwnership`
        // so a rediff-owned asset is protected identically to a splice-owned one.
        // playhead-u45d: STRUCTURAL bookkeeping — this guard asks "does a
        // width-owned row already exist under this id?", not "is this an ad?".
        // A user-vetoed row still owns its id, so the veto-aware read would
        // let a fresh decode re-mint the very ghost row this guard exists to
        // prevent.
        let existingSpans = (try? await store.fetchDecodedSpansIncludingUserVetoed(assetId: analysisAssetId)) ?? []
        let assetIsSlotOwned = existingSpans.contains { $0.anchorProvenance.contains(where: { $0.isWidthOwnership }) }
        if assetIsSlotOwned {
            logger.info(
                "Phase 5 projector: asset=\(analysisAssetId, privacy: .public) has width-owned rows — skipping upsert to preserve slot (splice/rediff) ownership"
            )
        } else {
            // Persist to SQLite so TranscriptPeekView can read them.
            do {
                try await store.upsertDecodedSpans(spans)
            } catch {
                logger.error("Phase 5 projector: failed to persist decoded spans for asset \(analysisAssetId): \(error)")
            }
        }

        // Record results in observer (DEBUG diagnostics).
        await observer.record(assetId: analysisAssetId, spans: spans, evidence: evidence)

        logger.info(
            "Phase 5 projector: asset=\(analysisAssetId) atoms=\(atoms.count) anchored=\(evidence.filter(\.isAnchored).count) spans=\(spans.count)"
        )
    }

    // MARK: - Shadow FM Phase

    private struct ShadowFMPhaseResult: Sendable {
        let outcome: ShadowFMPhaseOutcome
        /// playhead-xba follow-up: the raw refinement windows the runner
        /// emitted for this shadow invocation, threaded through so that
        /// the Phase 4 shadow phase (step 10 of `runBackfill`) can feed
        /// them into `RegionProposalBuilder`'s FM clustering path.
        /// Empty when the phase was skipped, failed, or produced no
        /// windows.
        let fmRefinementWindows: [FMRefinementWindowOutput]

        static let skipped = ShadowFMPhaseResult(outcome: .skipped, fmRefinementWindows: [])
    }

    private enum ShadowFMPhaseOutcome: Sendable {
        case skipped
        case requeued
        case ranNeedsRetry
        case ranSucceeded
        case ranFailed

        var didExecute: Bool {
            self == .ranSucceeded || self == .ranFailed || self == .ranNeedsRetry
        }

        var shouldClearRetryFlag: Bool {
            self == .ranSucceeded
        }
    }

    /// playhead-fil5: persist the durable "this episode is owed a semantic ad
    /// scan" claim for a gate that just refused to dispatch one.
    ///
    /// Every drop this covers used to be a `logger` call and nothing else, so a
    /// device pull could not tell a scan that was never requested from one that
    /// was requested and refused — and the sessionless callers skipped even the
    /// bd-3bz retry marker, making the drop permanent. See ``SemanticScanClaim``.
    ///
    /// `chunks` is read for ONE thing: whether there is a transcript at all.
    /// Empty chunks are a no-op — there is nothing for a semantic scan to read,
    /// so there is nothing to claim (and `runBackfill` has already returned in
    /// that case).
    ///
    /// **playhead-wxsv narrowed this parameter, and the history is worth
    /// keeping.** The claim's job id used to be derived from the chunks'
    /// `transcriptVersion`, so the array had to be the SAME one the phase would
    /// have atomized or the claim named a row no dispatcher would ever look up.
    /// playhead-iu0t hit exactly that: `retryShadowFMPhaseForSession` passed a
    /// final-pass-only replay set and minted a durable rescue row for work
    /// nothing could pick up. The id is now derived from the ASSET alone, so
    /// that whole class of disagreement is gone — a claim names the same row
    /// whichever caller mints it, and whichever transcript was on disk at the
    /// time.
    private func recordSemanticScanClaim(
        gate: SemanticScanClaim.Gate,
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        podcastId: String?
    ) async {
        guard !chunks.isEmpty else { return }
        await SemanticScanClaim.record(
            gate: gate,
            analysisAssetId: analysisAssetId,
            podcastId: podcastId,
            store: store,
            logger: logger
        )
    }

    /// Invokes `BackfillJobRunner` to execute the Foundation Model backfill in
    /// shadow mode. Failures are logged but never propagated, because shadow
    /// mode must never affect cue computation or user-visible behavior. Reads
    /// `config.fmBackfillMode` to decide whether to actually execute.
    private func runShadowFMPhase(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        podcastId: String,
        sessionIdOverride: String? = nil
    ) async -> ShadowFMPhaseResult {
        // Cycle 1 H2: gate on effective mode so a known-bad cohort skips
        // the entire shadow phase rather than handing the runner a mode
        // that would have been demoted to .off downstream anyway.
        let resolvedMode = effectiveFMBackfillMode
        guard resolvedMode != .off else {
            await recordSemanticScanClaim(
                gate: .fmModeOff,
                chunks: chunks,
                analysisAssetId: analysisAssetId,
                podcastId: podcastId
            )
            return .skipped
        }

        guard let factory = backfillJobRunnerFactory else {
            logger.warning("Shadow FM phase skipped: no runner factory injected — FM evidence will be absent. Check PlayheadRuntime wiring.")
            await recordSemanticScanClaim(
                gate: .runnerFactoryMissing,
                chunks: chunks,
                analysisAssetId: analysisAssetId,
                podcastId: podcastId
            )
            return .skipped
        }
        func wrap(_ outcome: ShadowFMPhaseOutcome, _ windows: [FMRefinementWindowOutput] = []) -> ShadowFMPhaseResult {
            ShadowFMPhaseResult(outcome: outcome, fmRefinementWindows: windows)
        }

        // Cycle 4 H5: `sessionIdOverride` is the only source of truth for
        // the session id. It's captured by the caller at dispatch time:
        //   • `AnalysisCoordinator.finalizeBackfill` threads the session
        //     id it already knows through `runBackfill(sessionId:)` →
        //     `runShadowFMPhase(sessionIdOverride:)`.
        //   • `retryShadowFMPhaseForSession` passes the exact session id
        //     being retried.
        //   • `AnalysisJobRunner` (pre-roll warmup) has no session
        //     concept and passes nil — the marker is then skipped on
        //     bail, which is correct for that path (no user-facing
        //     session to retry).
        //
        // The previous `fetchLatestSessionForAsset` fallback was removed
        // because it raced concurrent reprocessing: session B for asset
        // X could land between the start of the shadow phase and the
        // marker call, and the marker would tag the wrong (newer) row.
        // With the override-only model, the race is unrepresentable.
        let resolvedSessionId: String? = sessionIdOverride

        // M-D: skip the entire shadow phase on devices that can't run
        // Foundation Models. Atomization, segmentation, and catalog builds
        // are not free — there's no point doing the work only to have the
        // runner's admission controller immediately reject it.
        //
        // bd-3bz (Phase 4): this gate used to be one-shot — a transient
        // false (Apple Intelligence still downloading, thermal probe
        // momentarily failing, locale flip) permanently dropped shadow
        // telemetry for the episode. Now we flag the session via
        // `shadowSkipMarker` before returning, and the capability observer
        // in `PlayheadRuntime` drains flagged sessions after FM becomes
        // stably available again (60s debounce). See
        // `retryShadowFMPhaseForSession` for the re-entrant retry path.
        guard await canUseFoundationModelsProvider() else {
            logger.debug("Shadow FM phase skipped: canUseFoundationModels=false (bd-3bz: marking session for retry)")
            if let resolvedSessionId {
                await shadowSkipMarker(resolvedSessionId, podcastId)
            } else {
                logger.debug("Shadow FM phase: no session id resolved, marker skipped")
            }
            // playhead-fil5: the session marker is the SESSION lane's retry
            // hook, and the two sessionless callers (the `AnalysisJobRunner`
            // pre-roll warmup and the final-pass revalidation hook) pass nil —
            // which made this drop permanent for exactly the paths that reach
            // an already-transcribed episode. The claim is durable regardless
            // of whether a session exists to be marked.
            await recordSemanticScanClaim(
                gate: .foundationModelsUnavailable,
                chunks: chunks,
                analysisAssetId: analysisAssetId,
                podcastId: podcastId
            )
            return wrap(.requeued)
        }

        // Cycle 1 H2: pass the effective mode so the runner persists scan
        // results stamped with the cohort-approved capability set.
        let runner = factory(store, resolvedMode)
        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: analysisAssetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )
        let segments = TranscriptSegmenter.segment(atoms: atoms)
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: analysisAssetId,
            transcriptVersion: version.transcriptVersion
        )
        // bd-m8k: read the real per-podcast planner state from AnalysisStore
        // instead of hardwiring cold-start values. The legacy hardwire
        // pinned `observedEpisodeCount = 0` and `stableRecall = false`,
        // which made `CoveragePlanner.shouldUseFullCoverage` always true and
        // left the targeted-with-audit branch permanently unreachable.
        // Cycle 2 C4: the field was historically named `stablePrecision`;
        // it is semantically a stable-recall flag.
        //
        // Lazy semantics: a missing row means we have never observed this
        // podcast, so we fall back to the conservative cold-start defaults.
        // The runner's `recordPodcastEpisodeObservation` call site (also
        // bd-m8k) materializes the row, advances observed-episode counters,
        // and persists full-rescan recall samples derived from the shared
        // targeted-window narrowing helper.
        //
        // Failure mode: a fetch error here must NEVER block the shadow
        // pass — the whole point of shadow mode is that it cannot affect
        // user-visible behavior. We log and fall through to the cold-start
        // defaults so the runner still runs against `fullCoverage`.
        let plannerState: PodcastPlannerState?
        do {
            plannerState = try await store.fetchPodcastPlannerState(podcastId: podcastId)
        } catch {
            logger.warning("bd-m8k: planner state fetch failed (defaulting to cold start): \(error.localizedDescription)")
            plannerState = nil
        }
        // playhead-au2v.1.27 (Phase B): thread the cached inferred
        // `ChapterPlan` into the planner context — but ONLY under
        // `chapterSignalMode == .enabled`. The resolver returns `nil` for
        // `.off` (production default) and `.shadow`, which makes the
        // context byte-identical to the pre-au2v.1.27 construction in those
        // modes (see `makeShadowPhaseCoveragePlannerContext`). This is the
        // last-mile wire-in that makes the CoveragePlanner chapter-informed
        // branch (au2v.1.14) reachable from live backfill, gated safely.
        let chapterEvidence = await resolveChapterEvidenceForShadowPhase(
            transcriptVersion: version.transcriptVersion
        )
        let plannerContext = Self.makeShadowPhaseCoveragePlannerContext(
            plannerState: plannerState,
            chapterSignalMode: config.chapterSignalMode,
            chapterEvidence: chapterEvidence
        )
        // playhead-7q3 (Phase 4): compute acoustic breaks from the episode
        // feature windows and thread them into `TargetedWindowNarrower` via
        // `AssetInputs.acousticBreaks`. The narrower snaps per-anchor
        // window edges to nearby natural audio transitions (Option D).
        //
        // Failure mode: fetching or detecting breaks must NEVER block the
        // shadow phase — shadow mode is observation-only and the narrower
        // falls back cleanly to the legacy fixed-padding behavior on an
        // empty break list. On any error we log and pass `[]`.
        let acousticBreaks: [AcousticBreak]
        if self.episodeDuration > 0 {
            do {
                let featureWindows = try await store.fetchFeatureWindows(
                    assetId: analysisAssetId,
                    from: 0,
                    to: self.episodeDuration
                )
                acousticBreaks = AcousticBreakDetector.detectBreaks(in: featureWindows)
            } catch {
                logger.warning("playhead-7q3: fetchFeatureWindows failed for break snap (falling back to fixed padding): \(error.localizedDescription)")
                acousticBreaks = []
            }
        } else {
            acousticBreaks = []
        }

        let inputs = BackfillJobRunner.AssetInputs(
            analysisAssetId: analysisAssetId,
            podcastId: podcastId,
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            transcriptVersion: version.transcriptVersion,
            plannerContext: plannerContext,
            acousticBreaks: acousticBreaks
        )

        do {
            let result = try await runner.runPendingBackfill(for: inputs)
            logger.info("Shadow FM phase: admitted=\(result.admittedJobIds.count) scans=\(result.scanResultIds.count) deferred=\(result.deferredJobIds.count) fmWindows=\(result.fmRefinementWindows.count)")
            // playhead-4my.10.1: snapshot the evidence + decision +
            // correction ledger into `training_examples` while the
            // cohort is still warm. The materializer's failures are
            // surfaced via `logger.error` (so SQLite write failures are
            // visible in production) but NEVER propagated — shadow-mode
            // invariant applies (the FM phase must not affect cue
            // computation, even when materialization explodes).
            await materializeTrainingExamples(forAsset: analysisAssetId)
            if result.deferredJobIds.isEmpty {
                return wrap(.ranSucceeded, result.fmRefinementWindows)
            }
            return wrap(.ranNeedsRetry, result.fmRefinementWindows)
        } catch {
            // cycle-3 L3: `AnalysisStoreError` (and most other errors thrown
            // off the runner path) does NOT conform to `LocalizedError`, so
            // `error.localizedDescription` returns the bridged-NSError
            // boilerplate ("The operation couldn't be completed. (X error
            // N.)") with no detail. Use `String(describing:)` (which calls
            // `description`) to surface the actual case + payload, mirroring
            // the inner catch in `materializeTrainingExamples` ~25 lines
            // below.
            logger.warning("Shadow FM phase failed (suppressed by invariant): \(String(describing: error))")
            return wrap(.ranFailed)
        }
    }

    // MARK: - Chapter-signal wire-in (playhead-au2v.1.27 Phase B)

    /// Resolve the chapter evidence to thread into the shadow-phase
    /// `CoveragePlannerContext`, gated on the chapter-signal mode.
    ///
    /// CRITICAL SAFETY CONTRACT: this returns `nil` for every mode except
    /// `.enabled`. `consumersReadChapterPlan` is `true` ONLY for `.enabled`
    /// (`.off` and `.shadow` both return `false` — see `ChapterSignalMode`),
    /// so in the production default (`.off`) and in shadow mode the planner
    /// context never carries chapter evidence and behaves byte-identically
    /// to the pre-au2v.1.27 pipeline.
    ///
    /// When `.enabled`:
    ///   - With no cache wired, or a cache miss (no plan on disk for this
    ///     content hash, schema mismatch, or decode failure — all surfaced
    ///     as `nil` by `ChapterPlanCache.get`), returns `nil`. The planner
    ///     then falls back to today's random audit selection (no crash, no
    ///     behaviour change vs. a missing plan).
    ///   - With a cache hit whose plan has at least one chapter, returns
    ///     the plan's `[ChapterEvidence]` (already `source == .inferred`).
    ///   - A plan with zero chapters resolves to `nil` so the planner's
    ///     own "empty evidence ⇒ skip chapter-informed path" guard is not
    ///     even reached with an empty array (equivalent outcome, fewer
    ///     downstream branches exercised).
    ///
    /// The cache key is `transcriptVersion` — the same `version.transcriptVersion`
    /// the chapter generation phase writes the plan under in
    /// `runChapterGenerationPhaseIfWired`, so an `.enabled` run reads back
    /// the plan its own earlier phase produced.
    private func resolveChapterEvidenceForShadowPhase(
        transcriptVersion: String
    ) async -> [ChapterEvidence]? {
        guard config.chapterSignalMode.consumersReadChapterPlan else {
            return nil
        }
        guard let cache = chapterPlanCache else {
            return nil
        }
        guard !transcriptVersion.isEmpty else {
            return nil
        }
        guard let plan = await cache.get(contentHash: transcriptVersion),
              !plan.chapters.isEmpty else {
            return nil
        }
        logger.debug(
            "chaptersignal.shadowphase: threading \(plan.chapters.count, privacy: .public) inferred chapters into CoveragePlannerContext (mode=\(self.config.chapterSignalMode.rawValue, privacy: .public))"
        )
        return plan.chapters
    }

    /// Pure builder for the shadow-phase `CoveragePlannerContext`.
    ///
    /// Extracted so the `.off`/`.shadow` byte-identical contract is
    /// provable in isolation (no store, no FM): the planner-state fields
    /// are mapped exactly as the pre-au2v.1.27 inline construction did, and
    /// the two chapter fields default to `.off` / `nil` UNLESS the caller
    /// resolved chapter evidence under `.enabled`.
    ///
    /// Contract:
    ///   - `chapterEvidence == nil` ⇒ `chapterSignalMode: .off`,
    ///     `chapterEvidence: nil` (the `CoveragePlannerContext` defaults).
    ///     This is the path taken for `.off`, `.shadow`, and `.enabled`
    ///     cache-miss, and it is byte-identical to the old construction.
    ///   - `chapterEvidence != nil` ⇒ `chapterSignalMode: mode`
    ///     (only `.enabled` ever supplies non-nil evidence, by the
    ///     resolver's gate), `chapterEvidence:` threaded through.
    static func makeShadowPhaseCoveragePlannerContext(
        plannerState: PodcastPlannerState?,
        chapterSignalMode: ChapterSignalMode,
        chapterEvidence: [ChapterEvidence]?
    ) -> CoveragePlannerContext {
        // Only carry the mode downstream when evidence actually resolved.
        // The resolver only returns non-nil evidence under `.enabled`, so
        // `.off`/`.shadow` always fall to the `.off` default here, keeping
        // the context byte-identical to the pre-au2v.1.27 construction.
        let effectiveMode: ChapterSignalMode = chapterEvidence == nil ? .off : chapterSignalMode
        return CoveragePlannerContext(
            observedEpisodeCount: plannerState?.observedEpisodeCount ?? 0,
            // historical: stored as "stablePrecisionFlag"; semantically recall
            stableRecall: plannerState?.stableRecallFlag ?? false,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: plannerState?.episodesSinceLastFullRescan ?? 0,
            periodicFullRescanIntervalEpisodes: 10,
            chapterSignalMode: effectiveMode,
            chapterEvidence: chapterEvidence
        )
    }

    #if DEBUG
    /// playhead-au2v.1.27 (Phase B) test seam: run the exact
    /// resolve-then-build sequence `runShadowFMPhase` uses to construct its
    /// `CoveragePlannerContext`, so a test can assert the chapter wire-in
    /// without standing up the full FM backfill pipeline (which requires a
    /// runner factory + FM availability). Shares the production helpers
    /// (`resolveChapterEvidenceForShadowPhase` + the static builder)
    /// verbatim — there is no test-only decision branch.
    func shadowPhaseCoveragePlannerContextForTesting(
        podcastId: String,
        transcriptVersion: String
    ) async -> CoveragePlannerContext {
        let plannerState: PodcastPlannerState?
        do {
            plannerState = try await store.fetchPodcastPlannerState(podcastId: podcastId)
        } catch {
            plannerState = nil
        }
        let evidence = await resolveChapterEvidenceForShadowPhase(
            transcriptVersion: transcriptVersion
        )
        return Self.makeShadowPhaseCoveragePlannerContext(
            plannerState: plannerState,
            chapterSignalMode: config.chapterSignalMode,
            chapterEvidence: evidence
        )
    }
    #endif

    /// playhead-4my.10.1: post-fusion materialization hook. Called from
    /// `runShadowFMPhase` after a backfill run completes (regardless of
    /// `fmBackfillMode` — `runShadowFMPhase` runs in production whenever
    /// the mode is not `.off`).
    ///
    /// Failures must NOT propagate (the shadow-mode contract is that the
    /// FM phase never affects cue computation), but they also must not be
    /// silently dropped. The materializer touches SQLite directly — a
    /// disk-full / FK-violation / migration-mismatch is exactly the kind
    /// of error we need a server-visible log line for. We log at `error`
    /// level (not `warning`) so the line surfaces in production telemetry.
    private func materializeTrainingExamples(forAsset analysisAssetId: String) async {
        let materializer = TrainingExampleMaterializer()
        do {
            try await materializer.materialize(
                forAsset: analysisAssetId,
                store: store
            )
        } catch {
            // Persistence failure: log loudly. Suppression is the
            // shadow-contract requirement; silence is not.
            //
            // playhead-4my.10.1 (cycle-2 H-A): `AnalysisStoreError` conforms to
            // `Error`/`CustomStringConvertible` but NOT `LocalizedError`, so
            // `error.localizedDescription` returns the useless bridged string
            // ("The operation couldn't be completed. (Playhead.AnalysisStoreError
            // error N.)"). Use `String(describing:)` (which calls `description`)
            // and surface a stable case-name token when the error is one of
            // ours, mirroring the `BackfillJobRunner` pattern at line ~608.
            let detail = String(describing: error)
            if let storeError = error as? AnalysisStoreError {
                let caseName = BackfillJobRunner.caseName(of: storeError)
                logger.error(
                    "TrainingExample materialization failed for asset \(analysisAssetId, privacy: .public) — error suppressed by shadow invariant: case=\(caseName, privacy: .public) detail=\(detail, privacy: .public)"
                )
            } else {
                logger.error(
                    "TrainingExample materialization failed for asset \(analysisAssetId, privacy: .public) — error suppressed by shadow invariant: detail=\(detail, privacy: .public)"
                )
            }
        }
    }

    // MARK: - Shadow FM Retry (bd-3bz Phase 4)

    /// bd-3bz (Phase 4): re-entrant retry of the Foundation Models shadow
    /// phase for a single session that was previously flagged via
    /// `markSessionNeedsShadowRetry` when the FM capability was unavailable.
    ///
    /// This path is intentionally narrow:
    ///   • It re-reads the persisted transcript chunks for the asset and
    ///     re-runs ONLY the shadow phase — transcription and coarse
    ///     detection are left alone (they are far more expensive and did
    ///     not depend on FM availability).
    ///   • It must be re-entrant against a session whose transcription and
    ///     coarse phases already completed: `BackfillJobRunner.jobId`
    ///     already keys on `transcriptVersion` so duplicate FM jobs are
    ///     deduped at the store level, not by accident here. That dedupe
    ///     only holds while this path and `runBackfill` derive the version
    ///     from the SAME chunk set — which, before playhead-iu0t, they did
    ///     not. See the note above `chunksForReplay`.
    ///   • It does not modify `AnalysisCoordinator` state. The session
    ///     stays in whatever state it was in (typically `.complete`).
    ///   • If the FM capability has flipped back to `false` before the
    ///     drain actually runs, the inner guard bails and re-marks the
    ///     session — the retry queue effectively rolls forward to the
    ///     next stable-true window.
    ///   • The session's `needsShadowRetry` flag is cleared ONLY when
    ///     the shadow phase runs to completion under a true capability.
    ///     Failures inside the runner (network, thermal, etc.) leave the
    ///     flag set so the next capability transition retries again.
    ///
    /// Returns `true` if the drain actually executed the shadow phase
    /// (regardless of runner outcome), `false` if the session was missing,
    /// not flagged, lacked chunks, the FM capability guard bailed, or the
    /// shadow phase could not even start (for example, no runner factory).
    @discardableResult
    func retryShadowFMPhaseForSession(sessionId: String) async -> Bool {
        guard let session = try? await store.fetchSession(id: sessionId) else {
            logger.debug("Shadow retry skipped: session \(sessionId) not found")
            return false
        }
        guard session.needsShadowRetry, let podcastId = session.shadowRetryPodcastId else {
            logger.debug("Shadow retry skipped: session \(sessionId) not flagged")
            return false
        }

        let analysisAssetId = session.analysisAssetId
        let chunks: [TranscriptChunk]
        do {
            chunks = try await store.fetchTranscriptChunks(assetId: analysisAssetId)
        } catch {
            logger.warning("Shadow retry skipped: failed to fetch chunks for \(analysisAssetId): \(error.localizedDescription)")
            return false
        }
        // playhead-iu0t: CANONICALIZE, exactly as `runBackfill` does. This line
        // used to read `finalChunks.isEmpty ? chunks : finalChunks` — the
        // pre-hc7e `filter { pass == "final" }` collapse, which hc7e removed
        // from `runBackfill` and left standing here.
        //
        // What it cost, measured on the 2026-08-03 device pull. Asset 53FC53E3
        // held 2,917 `fast` chunks over [0, 2490] and 32 `final` chunks over
        // [2490, 2525.82]. Its only `backfill_jobs` row, `fm-041dedcf8293523e`,
        // re-derives byte-exact from the FINAL-ONLY transcript version
        // (`55afd3e8bb41833c004ee7d4b1be7589`; canonical is `61872a4d…`,
        // fast-only `766ea8ba…`), so this line is provably the dispatcher that
        // minted it. The drain screened ONE 36 s window, completed 23 s after
        // creation at an `adScanFraction` of 0.0142, published cursor 2525.82,
        // and `countResumableBackfillJobs` then read 0 — 41 minutes of
        // transcribed audio discarded with no path back.
        //
        // Two things the collapse broke, and the canonical set fixes both:
        //   • REACH. `FinalPassRetranscriptionRunner` writes `final` rows only
        //     around already-detected candidate windows, so the final set is
        //     candidate-local by construction. Replaying it hands the FM phase
        //     a candidate-local timeline in place of the episode.
        //   • IDENTITY. `TranscriptAtomizer` derives `transcriptVersion` from
        //     whatever chunks it is given, and `BackfillJobRunner`'s job id
        //     embeds it — so a final-only replay minted rows in an id space no
        //     other dispatcher derives. They could not dedupe against
        //     `runBackfill`'s rows, and `runBackfill` could not resume theirs.
        //
        // NOT sorted here, unlike `runBackfill`'s call site. That sort exists
        // for r5um's RAW readers (`LexicalAnchorRefiner.buildWordStream`, the
        // `RegionShadowPhase` input), which this path has none of. Everything
        // this array reaches bottoms out in `TranscriptAtomizer.atomize` or
        // `TranscriptAtomizer.transcriptVersionHash` — `runShadowFMPhase`
        // atomizes it, and its `recordSemanticScanClaim` calls hash it — and
        // both of those order their own input with `canonicalTimeOrder`. So a
        // sort here would be a line whose deletion nothing could observe.
        //
        // Note the shape of that argument, because it is the one that decays:
        // it is safe only while every TERMINAL reader sorts, not while some
        // enumerated list of callers happens to. If a raw reader is ever added
        // below, add the sort — it is idempotent and free.
        let chunksForReplay = TranscriptChunkCanonicalizer.canonicalize(chunks).chunks
        guard !chunksForReplay.isEmpty else {
            logger.debug("Shadow retry skipped: no transcript chunks for \(analysisAssetId)")
            return false
        }

        // Re-check the capability inline. If it bailed again in the window
        // between the observer's drain decision and this call, the inner
        // `runShadowFMPhase` guard will re-mark the session; returning
        // early here keeps the telemetry clean ("drain skipped" vs "drain
        // executed + bailed").
        guard await canUseFoundationModelsProvider() else {
            logger.debug("Shadow retry bailed: canUseFoundationModels flipped false before drain")
            // H7: pass the explicit `sessionId` we already have so the
            // marker stamps the same session that was being retried,
            // never a newer concurrent session for the same asset.
            await shadowSkipMarker(sessionId, podcastId)
            return false
        }

        logger.info("Shadow retry: draining session \(sessionId) asset=\(analysisAssetId)")
        // playhead-xba follow-up: the retry path intentionally does NOT
        // feed FM windows into the Phase 4 region shadow phase. That
        // phase ran once when `runBackfill` first completed for this
        // session; re-running it from a shadow-retry drain would
        // double-record Phase 4 bundles for the same asset under
        // different window sets and is outside the retry contract.
        let shadowResult = await runShadowFMPhase(
            chunks: chunksForReplay,
            analysisAssetId: analysisAssetId,
            podcastId: podcastId,
            sessionIdOverride: sessionId
        )
        let outcome = shadowResult.outcome
        guard outcome.didExecute else {
            return false
        }
        guard outcome.shouldClearRetryFlag else {
            logger.debug("Shadow retry: shadow phase still has outstanding work for \(sessionId), leaving retry flag set")
            return true
        }
        // Clear only if the inner guard didn't re-stamp the session. A
        // race window exists where capability could have flipped mid-run,
        // but `runShadowFMPhase` would have re-marked the session via the
        // skip marker — so we re-read before clearing.
        do {
            if let refreshed = try await store.fetchSession(id: sessionId),
               refreshed.needsShadowRetry,
               refreshed.updatedAt > session.updatedAt {
                logger.debug("Shadow retry: session \(sessionId) re-flagged during drain, leaving flag set")
            } else {
                try await store.clearSessionShadowRetry(id: sessionId)
            }
        } catch {
            logger.warning("Shadow retry: failed to finalize flag for \(sessionId): \(error.localizedDescription)")
        }
        return true
    }

    // MARK: - User Behavior Feedback

    /// Record that the user rewound back into a skipped ad window,
    /// signaling a potential false positive.
    ///
    /// **Trust-score writer policy** (post-q45f): all `skipTrustScore`
    /// mutations now live in `TrustScoringService`. This method delegates
    /// to `recordWeakFalseSkipSignal` (q45f), which uses the smaller
    /// `weakFalseSignalPenalty` (default `0.05`) AND runs `evaluateDemotion`,
    /// so two listen-rewinds in a row demote `auto -> manual` as expected.
    /// Pre-q45f the trust mutation was an inline `updateProfileIfExists`
    /// block here that bypassed the demotion state machine.
    ///
    /// Magnitude rationale: a listen-rewind is a noisier FP signal than an
    /// explicit "Not an ad" revert (the user might've been distracted, or
    /// not minded the ad). `weakFalseSignalPenalty = 0.05` is half of
    /// `falseSignalPenalty = 0.10` to reflect that weaker confidence, while
    /// still letting repeated rewinds accumulate into a mode demotion.
    ///
    /// Side effects on every accepted call:
    ///   1. Revoke exact recurrence material from the show catalog and
    ///      repeated-ad cache. Any revocation failure rejects the correction
    ///      before its one-shot durable receipt is consumed, so the banner
    ///      remains retryable. Successful partial tombstones are idempotent.
    ///   2. Atomically flip `AdWindowDecision` to `.reverted` and append a
    ///      row to `ad_listen_rewinds`, keyed to `window.startTime`.
    ///      A missing window or insert failure rejects and rolls back the
    ///      whole receipt so the banner can remain retryable.
    ///   3. Only after that receipt commits, delegate to
    ///      `trustScoringService?.recordWeakFalseSkipSignal` for the atomic
    ///      profile mutation + demotion evaluation. Optional chaining lets
    ///      legacy test factories still get the durable correction.
    ///
    /// Do NOT re-introduce an inline profile mutation here — the
    /// `testRecordListenRewindBodyRoutesThroughTrustScoringService`
    /// canary blocks that regression at source-inspection time.
    func recordListenRewind(
        windowId: String,
        analysisAssetId: String,
        podcastId: String,
        expectedProducerRevision: AdWindow? = nil
    ) async throws {
        guard RecurrenceMaterialIdentity.canonicalIdentifier(windowId) != nil,
              RecurrenceMaterialIdentity.canonicalIdentifier(
                  analysisAssetId
              ) != nil,
              let canonicalPodcastId =
                RecurrenceMaterialIdentity.canonicalIdentifier(podcastId)
        else {
            throw AnalysisStoreError.invalidRow(column: 0)
        }
        let correctedWindow: AdWindow
        if let expectedProducerRevision {
            guard expectedProducerRevision.id == windowId,
                  expectedProducerRevision.analysisAssetId
                    == analysisAssetId else {
                throw AnalysisStoreError.notFound
            }
            correctedWindow = expectedProducerRevision
        } else {
            guard let fetched = try await store.fetchAdWindow(id: windowId),
                  fetched.analysisAssetId == analysisAssetId else {
                throw AnalysisStoreError.notFound
            }
            correctedWindow = fetched
        }
        // Revoke recurrence material before making the correction terminal.
        // Source/creative tombstones fence delayed learners. If either
        // independent store fails, propagate the failure while the durable
        // window and banner are still retryable; any tombstone that did land
        // is a conservative, idempotent partial result.
        if adCatalogStore != nil || repeatedAdCache != nil {
            var matchingFingerprint: AcousticFingerprint?
            var matchingRepeatedFingerprint: RepeatedAdFingerprint?
            var firstRevocationFailure: Error?
            let trustedCorrectedWindow: AdWindow? =
                correctedWindow.analysisAssetId == analysisAssetId
                    && correctedWindow.id == windowId
                    ? correctedWindow
                    : nil
            if let trustedCorrectedWindow {
                do {
                    let featureWindows = try await store.fetchFeatureWindows(
                        assetId: trustedCorrectedWindow.analysisAssetId,
                        from: trustedCorrectedWindow.startTime,
                        to: trustedCorrectedWindow.endTime
                    )
                    let fingerprint = AcousticFingerprint.fromFeatureWindows(
                        featureWindows
                    )
                    if !fingerprint.isZero {
                        matchingFingerprint = fingerprint
                    }
                    let repeatedFingerprint = RepeatedAdFingerprint.from(
                        featureWindows: featureWindows
                    )
                    if !repeatedFingerprint.isZero {
                        matchingRepeatedFingerprint = repeatedFingerprint
                    }
                } catch {
                    firstRevocationFailure = error
                    logger.warning(
                        "recordListenRewind: recurrence fingerprint fetch failed"
                    )
                }
            }
            // The correction request owns current show scope. Persisted
            // catalogMatchedShowId is historical diagnostic material and can
            // be stale or corrupt; it must never redirect a tombstone into a
            // different show's catalog/cache.
            if let adCatalogStore {
                do {
                    _ = try await adCatalogStore.revoke(
                        matchedEntryId: trustedCorrectedWindow?
                            .catalogMatchedEntryId
                            .flatMap(UUID.init),
                        sourceAssetId: analysisAssetId,
                        sourceWindowId: windowId,
                        sourceStartTime: trustedCorrectedWindow?.startTime,
                        sourceEndTime: trustedCorrectedWindow?.endTime,
                        source: .listenRevert,
                        matchingFingerprint: matchingFingerprint,
                        showId: canonicalPodcastId
                    )
                } catch {
                    if firstRevocationFailure == nil {
                        firstRevocationFailure = error
                    }
                    logger.warning(
                        "recordListenRewind: catalog revocation failed"
                    )
                }
            }
            if let repeatedAdCache {
                do {
                    _ = try await repeatedAdCache.revokeMatches(
                        showId: canonicalPodcastId,
                        fingerprint: matchingRepeatedFingerprint,
                        sourceAssetId: analysisAssetId,
                        sourceWindowId: windowId,
                        sourceStartTime: trustedCorrectedWindow?.startTime,
                        sourceEndTime: trustedCorrectedWindow?.endTime,
                        source: .listenRevert
                    )
                } catch {
                    if firstRevocationFailure == nil {
                        firstRevocationFailure = error
                    }
                    logger.warning(
                        "recordListenRewind: repeated-ad revocation failed"
                    )
                }
            }
            if let firstRevocationFailure {
                throw firstRevocationFailure
            }
        }

        // The decision flip and append-only event are one durable receipt.
        // Propagate any failure so the banner remains retryable; accepting a
        // partial write would consume the user's only correction surface.
        try await store.persistListenRewind(
            windowId: windowId,
            analysisAssetId: analysisAssetId,
            podcastId: podcastId,
            createdAt: Date(),
            expectedProducerRevision: correctedWindow
        )

        // playhead-q45f: route the trust-score side-effect through
        // `TrustScoringService.recordWeakFalseSkipSignal`. The pre-q45f
        // inline `updateProfileIfExists` block decremented trust by
        // 0.05 but bypassed the demotion state machine entirely — two
        // listen-rewinds in a row never demoted an `auto` show to
        // `manual`. Routing through the service keeps the weaker 0.05
        // magnitude (matched to the pre-q45f hard-code) AND now passes
        // through `evaluateDemotion`, closing the q45f defect.
        //
        // The optional chaining is deliberate: legacy test factories
        // (and any future caller that constructs an `AdDetectionService`
        // without injecting a trust service) still get the decision
        // flip + event log row. Production wiring in `PlayheadRuntime`
        // always installs a real service before the first user tap.
        // Trust calibration is downstream of the durable receipt and must not
        // hold the UI's claimed Listen action open.
        // playhead-gard: attribute the inferred revert to the DETECTOR that
        // drew the rewound window, weighted by that span's extent certainty.
        // Read from the persisted row's own columns — this seam has no
        // orchestrator ingest stamp to consult.
        let rewindAttribution = DetectorVetoAttribution(
            detector: correctedWindow.detectorClass,
            tier: SpanExtentSupport(
                startAnchor: AutoSkipEdgeAnchor(
                    rawValue: correctedWindow.startEdgeAnchor
                ) ?? .unanchored,
                endAnchor: AutoSkipEdgeAnchor(
                    rawValue: correctedWindow.endEdgeAnchor
                ) ?? .unanchored
            ).tier
        )
#if DEBUG
        if let handler = listenRewindTrustHandlerForTesting {
            Task {
                await handler(podcastId)
            }
        } else if let trustScoringService {
            Task {
                await trustScoringService.recordWeakFalseSkipSignal(
                    podcastId: podcastId,
                    attributions: [rewindAttribution]
                )
            }
        }
#else
        if let trustScoringService {
            Task {
                await trustScoringService.recordWeakFalseSkipSignal(
                    podcastId: podcastId,
                    attributions: [rewindAttribution]
                )
            }
        }
#endif

        logger.info("Recorded listen-rewind for window \(windowId), podcast \(podcastId)")
    }

    // MARK: - Classification Pipeline

    /// Route hot-path lexical hits through the span hypothesis engine when it
    /// can produce windows; otherwise fall back to the legacy 30-second lexical
    /// merge path unchanged.
    ///
    /// **playhead-99yt: canonicalize, because the hot path COUNTS chunks as
    /// evidence.** `runBackfill` has canonicalized since playhead-hc7e; this
    /// path — which is where a listener's first banner comes from — read the
    /// raw `fetchTranscriptChunks` array, so a fast chunk and the final chunk
    /// that re-transcribed the same audio were scanned as two independent
    /// observations of one utterance. `LexicalScanner.buildCandidate` requires
    /// `promotionHitCount >= config.minHitsForCandidate` (2) — a bar whose
    /// whole purpose is two INDEPENDENT hits — and then sums the same weights
    /// into `rawConfidence`, so the duplication both promotes and inflates.
    ///
    /// Measured on the 2026-08-15 device pull (9 mixed-pass assets, 3,751
    /// fast/final twins, every one an exact-span byte-identical pair):
    /// **44 hot-path lexical candidates raw against 39 canonical.** All five
    /// of the removed candidates sat at exactly `hitCount == 2` — i.e. they
    /// cleared the two-hit bar on one utterance seen twice — and a further
    /// 15 survivors carried inflated confidence (worst +0.170).
    ///
    /// One canonicalization here covers the whole hot path: `orderedChunks` is
    /// what reaches `SpanHypothesisEngine.process` (which calls
    /// `scanner.scanChunk` per chunk), `scanner.scan`,
    /// `hotPathBoundaryExpansionContext` and `makeHotPathHypothesisCandidate`.
    /// It is idempotent, and single-pass input passes through byte-identically.
    ///
    /// playhead-2kxd: `podcastId` is threaded in (rather than read off actor
    /// state) because the metadata-lexicon injection below is weighted by this
    /// show's `metadataTrust`. The hot path's caller carries it as an optional
    /// — `nil` means "this caller genuinely lacks show identity", which
    /// resolves to global priors rather than to the last show's.
    private func hotPathCandidates(
        from chunks: [TranscriptChunk],
        analysisAssetId: String,
        podcastId: String?
    ) async throws -> [LexicalCandidate] {
        // `canonicalize` returns the mixed path already time-ordered but hands
        // a single-pass array back byte-identically — i.e. in `chunkIndex`
        // order, which is NOT time order (playhead-r5um). Sorting is
        // idempotent, so it is always safe to do and never safe to assume.
        let orderedChunks = TranscriptChunkCanonicalizer.canonicalize(chunks).chunks
            .sorted(by: TranscriptChunkCanonicalizer.canonicalTimeOrder)
        let (metadataCues, _) = await loadEpisodeMetadataSignals(
            analysisAssetId: analysisAssetId
        )
        let metadataEntries = await metadataLexiconEntries(
            from: metadataCues,
            podcastId: podcastId
        )

        let hypothesisCandidates = try await hypothesisCandidates(
            from: orderedChunks,
            analysisAssetId: analysisAssetId
        )
        let lexicalCandidates = scanner.scan(
            chunks: orderedChunks,
            analysisAssetId: analysisAssetId,
            metadataEntries: metadataEntries
        )

        if !hypothesisCandidates.isEmpty {
            let survivingLexicalCandidates = lexicalCandidates.filter { lexicalCandidate in
                !hypothesisCandidates.contains { hypothesisCandidate in
                    candidatesOverlap(lexicalCandidate, hypothesisCandidate)
                }
            }
            let mergedCandidates = (hypothesisCandidates + survivingLexicalCandidates).sorted { lhs, rhs in
                if lhs.startTime != rhs.startTime {
                    return lhs.startTime < rhs.startTime
                }
                return lhs.endTime < rhs.endTime
            }

            logger.info("Hot path: hypothesis engine emitted \(hypothesisCandidates.count) candidates and preserved \(survivingLexicalCandidates.count) non-overlapping lexical candidates")
            return mergedCandidates
        }

        if !lexicalCandidates.isEmpty {
            logger.info("Hot path: lexical fallback emitted \(lexicalCandidates.count) candidates")
        }
        return lexicalCandidates
    }

    private func loadEpisodeMetadataSignals(
        analysisAssetId: String
    ) async -> (cues: [EpisodeMetadataCue], chapterEvidence: [ChapterEvidence]) {
        guard let snapshot = await episodeMetadataProvider.metadataSnapshot(
            for: analysisAssetId
        ) else {
            return ([], [])
        }

        let feedMetadata = snapshot.feedMetadata
        let extractor = MetadataCueExtractor(
            showOwnedDomains: snapshot.showOwnedDomains,
            networkOwnedDomains: snapshot.networkOwnedDomains,
            ownershipUndeterminedDomains: snapshot.ownershipUndeterminedDomains
        )
        let cues = extractor.extractCues(
            description: feedMetadata.feedDescription,
            summary: feedMetadata.feedSummary
        )
        return (cues, feedMetadata.chapterEvidence ?? [])
    }

    /// playhead-2kxd: `podcastId` names the show whose `metadataTrust` weights
    /// the injection. Threaded from the caller rather than read off a slot.
    private func metadataLexiconEntries(
        from cues: [EpisodeMetadataCue],
        podcastId: String?
    ) async -> [MetadataLexiconEntry] {
        guard !cues.isEmpty else { return [] }
        let priors = await resolveEpisodePriors(podcastId: podcastId)
        return metadataLexiconEntries(
            from: cues,
            metadataTrust: priors.metadataTrust
        )
    }

    private func metadataLexiconEntries(
        from cues: [EpisodeMetadataCue],
        metadataTrust: Float
    ) -> [MetadataLexiconEntry] {
        guard !cues.isEmpty else { return [] }
        let activationConfig = MetadataActivationConfig.resolved()
        guard activationConfig.isLexicalInjectionActive else { return [] }

        let injector = MetadataLexiconInjector(config: activationConfig)
        return injector.inject(
            cues: cues,
            metadataTrust: metadataTrust
        )
    }

    #if DEBUG
    /// playhead-2kxd: `podcastId` has no default — see
    /// `resolveEpisodePriorsForTesting` for why a test shim must not be able
    /// to ask "the current show".
    func hotPathCandidatesForTesting(
        from chunks: [TranscriptChunk],
        analysisAssetId: String,
        podcastId: String?
    ) async throws -> [LexicalCandidate] {
        try await hotPathCandidates(
            from: chunks,
            analysisAssetId: analysisAssetId,
            podcastId: podcastId
        )
    }
    #endif

    private func chunkHasReplaySignal(_ chunk: TranscriptChunk) -> Bool {
        replaySignalProfile(for: chunk).hasSignal
    }

    private func replaySignalProfile(for chunk: TranscriptChunk) -> ReplaySignalProfile {
        let hits = replaySignalHits(for: chunk)
        guard !hits.isEmpty else { return .none }

        var backwardReach: TimeInterval = 0
        var forwardReach: TimeInterval = 0
        var hasDirectionalSignal = false

        for hit in hits {
            guard let anchorEvent = SpanHypothesisEngine.mapToAnchorEvent(hit) else { continue }
            let anchorConfig = SpanHypothesisConfig.default.config(for: anchorEvent.anchorType)
            backwardReach = max(backwardReach, anchorConfig.backwardSearchRadius)
            forwardReach = max(forwardReach, anchorConfig.forwardSearchRadius)
            hasDirectionalSignal = true
        }

        return ReplaySignalProfile(
            hasSignal: true,
            hasDirectionalSignal: hasDirectionalSignal,
            backwardReach: backwardReach,
            forwardReach: forwardReach
        )
    }

    private func replaySignalHits(for chunk: TranscriptChunk) -> [LexicalHit] {
        var hits = scanner.scanChunk(chunk)
        guard let metadata = chunk.weakAnchorMetadata else { return hits }

        for alternativeText in metadata.alternativeTexts {
            guard !alternativeText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { continue }
            hits.append(contentsOf: scanner.scanChunk(
                syntheticReplayChunk(
                    text: alternativeText,
                    analysisAssetId: chunk.analysisAssetId,
                    startTime: chunk.startTime,
                    endTime: chunk.endTime
                )
            ))
        }

        for phrase in metadata.lowConfidencePhrases {
            let startTime = max(chunk.startTime, phrase.startTime)
            let endTime = min(chunk.endTime, phrase.endTime)
            guard endTime > startTime else { continue }
            hits.append(contentsOf: scanner.scanChunk(
                syntheticReplayChunk(
                    text: phrase.text,
                    analysisAssetId: chunk.analysisAssetId,
                    startTime: startTime,
                    endTime: endTime
                )
            ))
        }

        return hits
    }

    private func reconcileHotPathWindows(
        _ adWindows: [AdWindow],
        analysisAssetId: String
    ) async throws -> [ReconciledHotPathWindow] {
        let existingWindows = try await currentHotPathCandidateWindows(
            analysisAssetId: analysisAssetId
        )
        var matchedExistingIDs = Set<String>()
        var reconciled: [ReconciledHotPathWindow] = []

        for adWindow in adWindows.sorted(by: hotPathWindowOrdering) {
            let matchingWindows = matchingHotPathWindows(
                for: adWindow,
                in: existingWindows,
                excluding: matchedExistingIDs
            )
            guard let existing = bestMatchingHotPathWindow(
                for: adWindow,
                in: matchingWindows
            ) else {
                reconciled.append(
                    ReconciledHotPathWindow(
                        window: adWindow,
                        matchedExistingID: nil,
                        retiredExistingIDs: [],
                        expectedExistingRevisions: [:]
                    )
                )
                continue
            }

            let allMatchingIDs = Set(matchingWindows.map(\.id))
            matchedExistingIDs.formUnion(allMatchingIDs)
            let retiredExistingIDs = allMatchingIDs.subtracting([existing.id])
            let sameGeometry = Self.hasSameHotPathGeometry(
                existing,
                adWindow
            )
            // Reusing an ID is an identity/replay convenience, not permission
            // to transplant authority. Catalog/evidence state always comes
            // from this run: even unchanged geometry may now have a revoked,
            // unavailable, or differently-scoped learned row. An earlier
            // recognized demotion may remain as a conservative ceiling for
            // exact geometry, but an earlier automatic/legacy gate cannot
            // override the fresh verdict. Physical edge anchors may survive
            // only while both edges remain byte-identical.
            let existingDecodedGate = existing.eligibilityGate.flatMap {
                SkipEligibilityGate(rawValue: $0)
            }
            let eligibilityGate =
                sameGeometry
                    && existingDecodedGate != nil
                    && existingDecodedGate != .eligible
                ? existing.eligibilityGate
                : adWindow.eligibilityGate
            let edgeAuthority = sameGeometry ? existing : adWindow
            let descriptiveAuthority = sameGeometry ? existing : adWindow

            let preservedWindow = AdWindow(
                id: existing.id,
                analysisAssetId: adWindow.analysisAssetId,
                startTime: adWindow.startTime,
                endTime: adWindow.endTime,
                confidence: adWindow.confidence,
                // playhead-ar60: both confidences come from THIS run, like
                // `confidence` immediately above — they are one observation and
                // taking them from different runs is how a row would come to
                // report a detection score the actuation number never saw.
                skipConfidence: adWindow.skipConfidence,
                boundaryState: adWindow.boundaryState,
                decisionState: existing.decisionState,
                detectorVersion: adWindow.detectorVersion,
                advertiser: descriptiveAuthority.advertiser,
                product: descriptiveAuthority.product,
                adDescription: descriptiveAuthority.adDescription,
                evidenceText: adWindow.evidenceText,
                evidenceStartTime: sameGeometry
                    ? existing.evidenceStartTime
                        ?? adWindow.evidenceStartTime
                    : adWindow.evidenceStartTime,
                metadataSource: descriptiveAuthority.metadataSource,
                metadataConfidence: descriptiveAuthority.metadataConfidence,
                metadataPromptVersion:
                    descriptiveAuthority.metadataPromptVersion,
                wasSkipped: existing.wasSkipped,
                userDismissedBanner: existing.userDismissedBanner,
                evidenceSources: adWindow.evidenceSources,
                eligibilityGate: eligibilityGate,
                catalogStoreMatchSimilarity:
                    adWindow.catalogStoreMatchSimilarity,
                catalogFingerprintVersion:
                    adWindow.catalogFingerprintVersion,
                catalogMatchedEntryId:
                    adWindow.catalogMatchedEntryId,
                catalogMatchedShowId:
                    adWindow.catalogMatchedShowId,
                catalogMatchedLearningSource:
                    adWindow.catalogMatchedLearningSource,
                catalogMatchedLearningLifecycle:
                    adWindow.catalogMatchedLearningLifecycle,
                startEdgeAnchor: edgeAuthority.startEdgeAnchor,
                endEdgeAnchor: edgeAuthority.endEdgeAnchor
            )
            let expectedExistingRevisions = Dictionary(
                uniqueKeysWithValues: matchingWindows.map { ($0.id, $0) }
            )
            reconciled.append(
                ReconciledHotPathWindow(
                    window: preservedWindow,
                    matchedExistingID: existing.id,
                    retiredExistingIDs: retiredExistingIDs,
                    expectedExistingRevisions: expectedExistingRevisions
                )
            )
        }

        return reconciled
    }

    /// Uses the same numeric equivalence as SQLite persistence. In particular,
    /// SQLite does not distinguish `-0.0` from `+0.0`; treating those spellings
    /// as a geometry replacement here could discard a persisted conservative
    /// eligibility ceiling when a replay emits the opposite zero sign.
    static func hasSameHotPathGeometry(
        _ existing: AdWindow,
        _ incoming: AdWindow
    ) -> Bool {
        RecurrenceMaterialIdentity.canonicalTimeBitPattern(
            existing.startTime
        ) == RecurrenceMaterialIdentity.canonicalTimeBitPattern(
            incoming.startTime
        )
            && RecurrenceMaterialIdentity.canonicalTimeBitPattern(
                existing.endTime
            ) == RecurrenceMaterialIdentity.canonicalTimeBitPattern(
                incoming.endTime
            )
    }

    private func currentHotPathCandidateWindows(
        analysisAssetId: String
    ) async throws -> [AdWindow] {
        try await store.fetchAdWindows(assetId: analysisAssetId)
            .filter {
                $0.detectorVersion == config.detectorVersion
                    && $0.decisionState == AdDecisionState.candidate.rawValue
            }
    }

    private func hotPathCandidateRevisions(
        analysisAssetId: String,
        overlapping replayEnvelope: ClosedRange<Double>
    ) async throws -> [String: AdWindow] {
        let windows = try await currentHotPathCandidateWindows(
            analysisAssetId: analysisAssetId
        )
        return Dictionary(
            uniqueKeysWithValues: windows
                .filter { window in
                    // playhead-hygc.1.8 (R4): correction-replay rows are
                    // user-correction-backed shadow windows that the
                    // algorithmic detector by design does NOT re-emit (the
                    // overlap check in `correctionReplayCandidates` short-
                    // circuits whenever an existing AdWindow covers the FN
                    // span). They therefore can never appear in the new
                    // `adWindows` set produced by classification, so they
                    // would always land in `retiredWindowIDs` on a run
                    // whose chunks envelope overlaps them — which both R3
                    // (same-run) and R4 (subsequent-run) had to defuse.
                    // The cleanest invariant: correction-replay rows are
                    // never retired by the algorithmic-absence path. They
                    // are retired only by an explicit user veto via
                    // `SkipOrchestrator.revertByTimeRange`, which flips
                    // `decisionState` to `.reverted` (filtered out below
                    // by `currentHotPathCandidateWindows`).
                    window.boundaryState != Self.correctionReplayBoundaryState
                        // playhead-xsdz.36.4: day-0 byte-exact marks are
                        // deterministic ground truth the algorithmic detector
                        // does NOT re-emit; like correction-replay rows they must
                        // never be retired by the algorithmic-absence path (only
                        // by an explicit user veto → `.reverted`, filtered above).
                        && window.boundaryState != Self.dayZeroRediffByteExactBoundaryState
                        && window.endTime > replayEnvelope.lowerBound
                        && window.startTime < replayEnvelope.upperBound
                }
                .map { ($0.id, $0) }
        )
    }

    private func replayEnvelope(for chunks: [TranscriptChunk]) -> ClosedRange<Double> {
        let start = chunks.map(\.startTime).min() ?? 0
        let end = chunks.map(\.endTime).max() ?? start
        return start...max(start, end)
    }

    private func matchingHotPathWindows(
        for incoming: AdWindow,
        in existingWindows: [AdWindow],
        excluding excludedIDs: Set<String>
    ) -> [AdWindow] {
        existingWindows
            .filter { existing in
                !excludedIDs.contains(existing.id)
                    && existing.analysisAssetId == incoming.analysisAssetId
                    && existing.boundaryState == incoming.boundaryState
                    && existing.endTime > incoming.startTime
                    && existing.startTime < incoming.endTime
                    && hotPathWindowsShareIdentity(existing: existing, incoming: incoming)
            }
    }

    private func bestMatchingHotPathWindow(
        for incoming: AdWindow,
        in matchingWindows: [AdWindow]
    ) -> AdWindow? {
        matchingWindows.max { lhs, rhs in
                let lhsScore = hotPathWindowMatchScore(existing: lhs, incoming: incoming)
                let rhsScore = hotPathWindowMatchScore(existing: rhs, incoming: incoming)
                if lhsScore != rhsScore {
                    return lhsScore < rhsScore
                }

                let lhsDistance = abs(lhs.startTime - incoming.startTime) + abs(lhs.endTime - incoming.endTime)
                let rhsDistance = abs(rhs.startTime - incoming.startTime) + abs(rhs.endTime - incoming.endTime)
                if lhsDistance != rhsDistance {
                    return lhsDistance > rhsDistance
                }
                return lhs.id > rhs.id
            }
    }

    private func hotPathWindowMatchScore(existing: AdWindow, incoming: AdWindow) -> Double {
        let overlapStart = max(existing.startTime, incoming.startTime)
        let overlapEnd = min(existing.endTime, incoming.endTime)
        let overlap = max(0, overlapEnd - overlapStart)
        let union = max(existing.endTime, incoming.endTime) - min(existing.startTime, incoming.startTime)
        guard union > 0 else { return 1 }
        return overlap / union
    }

    private func hotPathWindowsShareIdentity(existing: AdWindow, incoming: AdWindow) -> Bool {
        if abs(existing.startTime - incoming.startTime) <= Self.hotPathCandidateIdentityTolerance {
            return true
        }
        if abs(existing.endTime - incoming.endTime) <= Self.hotPathCandidateIdentityTolerance {
            return true
        }
        if let existingEvidenceStartTime = existing.evidenceStartTime,
           let incomingEvidenceStartTime = incoming.evidenceStartTime,
           abs(existingEvidenceStartTime - incomingEvidenceStartTime) <= Self.hotPathCandidateIdentityTolerance
        {
            return true
        }
        if hotPathEvidenceTextSharesIdentity(existing: existing, incoming: incoming) {
            return true
        }
        return false
    }

    private func hotPathEvidenceTextSharesIdentity(existing: AdWindow, incoming: AdWindow) -> Bool {
        guard let existingText = normalizedHotPathEvidenceText(existing.evidenceText),
              let incomingText = normalizedHotPathEvidenceText(incoming.evidenceText)
        else {
            return false
        }

        let (shorter, longer) = existingText.count <= incomingText.count
            ? (existingText, incomingText)
            : (incomingText, existingText)
        guard shorter.count >= 12 else { return false }
        return longer.contains(shorter)
    }

    private func normalizedHotPathEvidenceText(_ text: String?) -> String? {
        guard let text else { return nil }
        let normalized = TranscriptEngineService.normalizeText(text)
        return normalized.isEmpty ? nil : normalized
    }

    private func hotPathWindowOrdering(_ lhs: AdWindow, _ rhs: AdWindow) -> Bool {
        if lhs.startTime != rhs.startTime {
            return lhs.startTime < rhs.startTime
        }
        return lhs.endTime < rhs.endTime
    }

    private func syntheticReplayChunk(
        text: String,
        analysisAssetId: String,
        startTime: Double,
        endTime: Double
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: UUID().uuidString,
            analysisAssetId: analysisAssetId,
            segmentFingerprint: UUID().uuidString,
            chunkIndex: 0,
            startTime: startTime,
            endTime: endTime,
            text: text,
            normalizedText: TranscriptEngineService.normalizeText(text),
            pass: TranscriptPassType.fast.rawValue,
            modelVersion: Self.hotPathReplayModelVersion,
            transcriptVersion: nil,
            atomOrdinal: nil,
            weakAnchorMetadata: nil
        )
    }

    private func hypothesisCandidates(
        from chunks: [TranscriptChunk],
        analysisAssetId: String
    ) async throws -> [LexicalCandidate] {
        let spanConfig = SpanHypothesisConfig.default
        let boundaryContext = try await hotPathBoundaryExpansionContext(
            for: chunks,
            analysisAssetId: analysisAssetId,
            config: spanConfig
        )
        var engine = SpanHypothesisEngine(
            config: spanConfig,
            boundaryExpansionContext: boundaryContext
        )
        _ = engine.process(
            chunks: chunks,
            analysisAssetId: analysisAssetId,
            scanner: scanner
        )
        let hits = engine.observedHits
        guard !hits.isEmpty else { return [] }

        let finishTime = max(
            chunks.last?.endTime ?? 0,
            hits.last?.endTime ?? 0
        )
        _ = engine.finish(
            analysisAssetId: analysisAssetId,
            at: finishTime
        )

        guard !engine.closedHypotheses.isEmpty else { return [] }
        let envelopes: [HotPathHypothesisCandidate] = engine.closedHypotheses.compactMap {
            (hypothesis: SpanHypothesis) -> HotPathHypothesisCandidate? in
            guard shouldPromoteHotPathHypothesis(hypothesis) else { return nil }

            return makeHotPathHypothesisCandidate(
                from: hypothesis,
                analysisAssetId: analysisAssetId,
                allHits: hits,
                transcriptChunks: chunks,
                featureWindows: boundaryContext.featureWindows,
                minConfirmedEvidence: spanConfig.minConfirmedEvidence
            )
        }.filter { envelope in
            envelope.candidate.endTime > envelope.candidate.startTime
        }

        return collapseHotPathHypothesisCandidates(envelopes)
            .map(\.candidate)
    }

    private func collapseHotPathHypothesisCandidates(
        _ candidates: [HotPathHypothesisCandidate]
    ) -> [HotPathHypothesisCandidate] {
        let orderedCandidates = candidates.sorted { lhs, rhs in
            if lhs.candidate.startTime != rhs.candidate.startTime {
                return lhs.candidate.startTime < rhs.candidate.startTime
            }
            return lhs.candidate.endTime < rhs.candidate.endTime
        }

        var collapsed: [HotPathHypothesisCandidate] = []
        for candidate in orderedCandidates {
            guard let last = collapsed.last else {
                collapsed.append(candidate)
                continue
            }

            if candidatesOverlap(last.candidate, candidate.candidate) {
                let preferred = prefersHotPathCandidate(candidate, over: last) ? candidate : last
                let other = preferred.candidate.id == candidate.candidate.id ? last : candidate
                collapsed[collapsed.count - 1] = mergeHotPathCandidates(preferred, with: other)
            } else {
                collapsed.append(candidate)
            }
        }

        return collapsed
    }

    private func candidatesOverlap(_ lhs: LexicalCandidate, _ rhs: LexicalCandidate) -> Bool {
        lhs.endTime >= rhs.startTime && rhs.endTime >= lhs.startTime
    }

    private func prefersHotPathCandidate(
        _ lhs: HotPathHypothesisCandidate,
        over rhs: HotPathHypothesisCandidate
    ) -> Bool {
        if lhs.evidenceCount != rhs.evidenceCount {
            return lhs.evidenceCount > rhs.evidenceCount
        }

        if lhs.hasClosingAnchor != rhs.hasClosingAnchor {
            return lhs.hasClosingAnchor
        }

        if lhs.candidate.categories.count != rhs.candidate.categories.count {
            return lhs.candidate.categories.count > rhs.candidate.categories.count
        }

        if lhs.candidate.evidenceText.count != rhs.candidate.evidenceText.count {
            return lhs.candidate.evidenceText.count > rhs.candidate.evidenceText.count
        }

        if lhs.candidate.hitCount != rhs.candidate.hitCount {
            return lhs.candidate.hitCount > rhs.candidate.hitCount
        }

        if lhs.candidate.startTime != rhs.candidate.startTime {
            return lhs.candidate.startTime < rhs.candidate.startTime
        }

        let lhsDuration = lhs.candidate.endTime - lhs.candidate.startTime
        let rhsDuration = rhs.candidate.endTime - rhs.candidate.startTime
        if lhsDuration != rhsDuration {
            return lhsDuration < rhsDuration
        }

        if lhs.candidate.confidence != rhs.candidate.confidence {
            return lhs.candidate.confidence > rhs.candidate.confidence
        }

        return lhs.candidate.endTime < rhs.candidate.endTime
    }

    private func mergeHotPathCandidates(
        _ preferred: HotPathHypothesisCandidate,
        with other: HotPathHypothesisCandidate
    ) -> HotPathHypothesisCandidate {
        let mergedHits = deduplicatedHotPathHits(preferred.supportingHits + other.supportingHits)
        let mergedStart = min(preferred.candidate.startTime, other.candidate.startTime)
        let mergedEnd = max(preferred.candidate.endTime, other.candidate.endTime)
        let categories = mergedHits.isEmpty
            ? preferred.candidate.categories.union(other.candidate.categories)
            : Set(mergedHits.map(\.category))
        let evidenceText = hotPathEvidenceText(
            from: mergedHits,
            fallbackTexts: [preferred.candidate.evidenceText, other.candidate.evidenceText]
        )

        return HotPathHypothesisCandidate(
            candidate: LexicalCandidate(
                id: preferred.candidate.id,
                analysisAssetId: preferred.candidate.analysisAssetId,
                startTime: mergedStart,
                endTime: mergedEnd,
                confidence: max(preferred.candidate.confidence, other.candidate.confidence),
                hitCount: max(preferred.candidate.hitCount, other.candidate.hitCount, mergedHits.count),
                categories: categories,
                evidenceText: evidenceText,
                evidenceStartTime: preferred.candidate.evidenceStartTime,
                detectorVersion: preferred.candidate.detectorVersion
            ),
            evidenceCount: max(preferred.evidenceCount, other.evidenceCount),
            hasClosingAnchor: preferred.hasClosingAnchor || other.hasClosingAnchor,
            supportingHits: mergedHits
        )
    }

    private func hotPathBoundaryExpansionContext(
        for chunks: [TranscriptChunk],
        analysisAssetId: String,
        config: SpanHypothesisConfig
    ) async throws -> SpanHypothesisEngine.BoundaryExpansionContext {
        let minStart = chunks.map(\.startTime).min() ?? 0
        let maxEnd = chunks.map(\.endTime).max() ?? 0
        let hypothesisBackwardMargin = config.anchorTypeConfigByType.values
            .map(\.backwardSearchRadius)
            .max() ?? 0
        let hypothesisForwardMargin = config.anchorTypeConfigByType.values
            .map(\.forwardSearchRadius)
            .max() ?? 0
        let expansionConfigs: [BoundaryExpander.ExpansionConfig] = [
            .startAnchored,
            .endAnchored,
            .neutral
        ]
        let acousticBackwardMargin = expansionConfigs
            .map(\.acousticBackwardSearchRadius)
            .max() ?? 0
        let acousticForwardMargin = expansionConfigs
            .map(\.acousticForwardSearchRadius)
            .max() ?? 0
        let backwardMargin = max(hypothesisBackwardMargin, acousticBackwardMargin)
        let forwardMargin = max(hypothesisForwardMargin, acousticForwardMargin)

        let featureWindows = try await store.fetchFeatureWindows(
            assetId: analysisAssetId,
            from: max(0, minStart - backwardMargin),
            to: maxEnd + forwardMargin
        )

        return SpanHypothesisEngine.BoundaryExpansionContext(
            featureWindows: featureWindows,
            transcriptChunks: chunks
        )
    }

    private func makeHotPathHypothesisCandidate(
        from hypothesis: SpanHypothesis,
        analysisAssetId: String,
        allHits: [LexicalHit],
        transcriptChunks: [TranscriptChunk],
        featureWindows: [FeatureWindow],
        minConfirmedEvidence: Double
    ) -> HotPathHypothesisCandidate {
        let boundary = hotPathExpandedBoundary(
            for: hypothesis,
            featureWindows: featureWindows,
            transcriptChunks: transcriptChunks
        )
        let startTime = boundary?.startTime ?? hypothesis.startCandidateTime
        let endTime = boundary?.endTime ?? hypothesis.endCandidateTime
        let supportingHits = allHits.filter { hit in
            hit.endTime >= startTime && hit.startTime <= endTime
        }
        let categories = supportingHits.isEmpty
            ? [defaultCategory(for: hypothesis.anchorType)]
            : Array(Set(supportingHits.map(\.category)))
        let evidenceTexts = supportingHits.isEmpty
            ? hypothesis.allEvidenceTexts
            : supportingHits
                .sorted { lhs, rhs in
                    if lhs.startTime != rhs.startTime {
                        return lhs.startTime < rhs.startTime
                    }
                    return lhs.endTime < rhs.endTime
                }
                .map(\.matchedText)
        let evidenceText = evidenceTexts.reduce(into: [String]()) { partial, text in
            if !partial.contains(text) {
                partial.append(text)
            }
        }.joined(separator: " | ")
        let confidence = min(
            1.0,
            hypothesis.score(at: hypothesis.lastEvidenceTime) / max(minConfirmedEvidence, 1.0)
        )
        let evidenceStartTime = hotPathEvidenceStartTime(for: hypothesis)

        return HotPathHypothesisCandidate(
            candidate: LexicalCandidate(
                id: [
                    analysisAssetId,
                    String(format: "%.3f", locale: Locale(identifier: "en_US_POSIX"), startTime),
                    String(format: "%.3f", locale: Locale(identifier: "en_US_POSIX"), endTime),
                    "hypothesis"
                ].joined(separator: ":"),
                analysisAssetId: analysisAssetId,
                startTime: startTime,
                endTime: endTime,
                confidence: confidence,
                hitCount: max(1, supportingHits.count),
                categories: Set(categories),
                evidenceText: evidenceText,
                evidenceStartTime: evidenceStartTime,
                detectorVersion: "hypothesis-v1"
            ),
            evidenceCount: 1 + hypothesis.supportingAnchors.count + hypothesis.bodyEvidence.count + (hypothesis.closingAnchor == nil ? 0 : 1),
            hasClosingAnchor: hypothesis.closingAnchor != nil,
            supportingHits: supportingHits
        )
    }

    private func hotPathEvidenceStartTime(for hypothesis: SpanHypothesis) -> Double {
        if let bodyTimestamp = hypothesis.bodyEvidence.map(\.timestamp).min() {
            return bodyTimestamp
        }
        if let closingAnchor = hypothesis.closingAnchor {
            return closingAnchor.startTime
        }
        if let supportingAnchorStart = hypothesis.supportingAnchors.map(\.startTime).min() {
            return supportingAnchorStart
        }
        return hypothesis.seedAnchor.startTime
    }

    private func hotPathExpandedBoundary(
        for hypothesis: SpanHypothesis,
        featureWindows: [FeatureWindow],
        transcriptChunks: [TranscriptChunk]
    ) -> ExpandedBoundary? {
        let additionalEvidenceCount = hypothesis.supportingAnchors.count + hypothesis.bodyEvidence.count + (hypothesis.closingAnchor == nil ? 0 : 1)
        guard additionalEvidenceCount > 0 else { return nil }

        let seed = hotPathBoundarySeed(for: hypothesis)
        let config = BoundaryExpander.ExpansionConfig.forPolarity(hypothesis.polarity)
        let evidenceTimes = hotPathEvidenceTimes(for: hypothesis)
        let hasTemporalSpread = hotPathHasTemporalSpread(hypothesis)

        let acousticOnly = BoundaryExpander().expand(
            seed: seed,
            featureWindows: featureWindows,
            transcriptChunks: [],
            adWindows: [],
            config: config,
            anchorType: hypothesis.anchorType
        )
        let usableAcousticOnly = acousticOnly.source == .fallback || !boundaryCoversEvidenceTimes(acousticOnly, evidenceTimes: evidenceTimes)
            ? nil
            : acousticOnly

        let lexicalAware = BoundaryExpander().expand(
            seed: seed,
            featureWindows: featureWindows,
            transcriptChunks: transcriptChunks,
            adWindows: [],
            config: config,
            anchorType: hypothesis.anchorType
        )
        let usableLexicalAware = lexicalAware.source == .fallback || !boundaryCoversEvidenceTimes(lexicalAware, evidenceTimes: evidenceTimes)
            ? nil
            : lexicalAware

        if hasTemporalSpread {
            return usableAcousticOnly ?? usableLexicalAware
        }

        if hypothesis.closingAnchor != nil {
            return usableLexicalAware ?? usableAcousticOnly
        }

        // Same-chunk corroboration should stay tight instead of widening into a
        // speculative open-ended window.
        if additionalEvidenceCount > 0 {
            return usableLexicalAware ?? usableAcousticOnly
        }

        return usableAcousticOnly ?? usableLexicalAware
    }

    private func hotPathBoundarySeed(for hypothesis: SpanHypothesis) -> Double {
        switch hypothesis.polarity {
        case .startAnchored:
            return hypothesis.seedAnchor.startTime
        case .endAnchored:
            return hypothesis.seedAnchor.endTime
        case .neutral:
            return (hypothesis.seedAnchor.startTime + hypothesis.seedAnchor.endTime) / 2.0
        }
    }

    private func defaultCategory(for anchorType: AnchorType) -> LexicalPatternCategory {
        switch anchorType {
        case .disclosure, .sponsorLexicon, .fmPositive:
            return .sponsor
        case .url:
            return .urlCTA
        case .promoCode:
            return .promoCode
        case .transitionMarker:
            return .transitionMarker
        }
    }

    /// playhead-xsdz.69: a sponsor BRAND — a matched sponsor entity or a bare
    /// sponsor-lexicon hit — is ATTENTION, not a verdict. Promoting it to a span
    /// requires a co-occurring CONFIRMING ad-cue (a disclosure frame like
    /// "brought to you by", a URL, a promo code, or an FM-positive). A bare brand
    /// mention with none of those is editorial talk ("the Red Bull team won"),
    /// not an ad read. Set to `false` to restore the pre-xsdz.69 behavior where a
    /// bare brand promotes unconditionally.
    static let sponsorRequiresConfirmingAdCue = true

    /// True when the hypothesis carries a confirming ad-cue beyond the mere brand
    /// name — a disclosure frame, URL, promo code, or FM-positive anchor. Shared
    /// by the sponsor-promotion gate (and reused by the backfill / candidate
    /// paths) so the "does this brand hit look like an actual ad?" test is
    /// single-sourced. Real ad reads carry a cue; bare editorial brand-talk does
    /// not (playhead-xsdz.69).
    static func hypothesisHasConfirmingAdCue(_ hypothesis: SpanHypothesis) -> Bool {
        let anchors = [hypothesis.seedAnchor] + hypothesis.supportingAnchors
            + (hypothesis.closingAnchor.map { [$0] } ?? [])
        return anchors.contains { anchor in
            switch anchor.anchorType {
            case .disclosure, .url, .promoCode, .fmPositive:
                return true
            case .sponsorLexicon, .transitionMarker:
                return false
            }
        }
    }

    private func shouldPromoteHotPathHypothesis(_ hypothesis: SpanHypothesis) -> Bool {
        let additionalEvidenceCount = hypothesis.supportingAnchors.count
            + hypothesis.bodyEvidence.count
            + (hypothesis.closingAnchor == nil ? 0 : 1)
        guard additionalEvidenceCount > 0 else { return false }

        // playhead-xsdz.69: sponsor identity (matched entity) promotes only with
        // a co-occurring confirming ad-cue — this kills the editorial brand-as-
        // sponsor false positive (e.g. sports teams named after their sponsors)
        // while real ad reads still fire. Truly-bare host-reads are the
        // specialist's job (the durable disposer).
        if hypothesis.sponsorEntity != nil {
            return Self.sponsorRequiresConfirmingAdCue
                ? Self.hypothesisHasConfirmingAdCue(hypothesis)
                : true
        }

        let anchors = [hypothesis.seedAnchor] + hypothesis.supportingAnchors
        let hasConfirmingCue = Self.hypothesisHasConfirmingAdCue(hypothesis)
        return anchors.contains { anchor in
            switch anchor.anchorType {
            case .disclosure, .fmPositive:
                return true
            case .sponsorLexicon:
                // A bare sponsor-lexicon brand hit is attention-only; it needs a
                // confirming cue to promote (playhead-xsdz.69).
                return Self.sponsorRequiresConfirmingAdCue ? hasConfirmingCue : true
            case .url, .promoCode, .transitionMarker:
                return false
            }
        }
    }

    private func hotPathEvidenceTimes(for hypothesis: SpanHypothesis) -> [Double] {
        var timestamps: [Double] = [
            hypothesis.seedAnchor.startTime,
            hypothesis.seedAnchor.endTime,
        ]
        timestamps.append(contentsOf: hypothesis.supportingAnchors.flatMap { [$0.startTime, $0.endTime] })
        timestamps.append(contentsOf: hypothesis.bodyEvidence.map(\.timestamp))
        if let closingAnchor = hypothesis.closingAnchor {
            timestamps.append(contentsOf: [closingAnchor.startTime, closingAnchor.endTime])
        }
        return timestamps
    }

    private func hotPathHasTemporalSpread(_ hypothesis: SpanHypothesis) -> Bool {
        let temporalSpreadThreshold = 5.0
        let seedTime = hypothesis.seedAnchor.endTime

        let supportingAnchorTimes = hypothesis.supportingAnchors.flatMap { [$0.startTime, $0.endTime] }
        if supportingAnchorTimes.contains(where: { abs($0 - seedTime) >= temporalSpreadThreshold }) {
            return true
        }

        if hypothesis.bodyEvidence.contains(where: { abs($0.timestamp - seedTime) >= temporalSpreadThreshold }) {
            return true
        }

        if let closingAnchor = hypothesis.closingAnchor {
            return abs(closingAnchor.endTime - seedTime) >= temporalSpreadThreshold
                || abs(closingAnchor.startTime - seedTime) >= temporalSpreadThreshold
        }

        return false
    }

    private func boundaryCoversEvidenceTimes(
        _ boundary: ExpandedBoundary,
        evidenceTimes: [Double]
    ) -> Bool {
        evidenceTimes.allSatisfy { time in
            time >= boundary.startTime && time <= boundary.endTime
        }
    }

    private func deduplicatedHotPathHits(_ hits: [LexicalHit]) -> [LexicalHit] {
        let orderedHits = hits.sorted { lhs, rhs in
            if lhs.startTime != rhs.startTime {
                return lhs.startTime < rhs.startTime
            }
            if lhs.endTime != rhs.endTime {
                return lhs.endTime < rhs.endTime
            }
            if lhs.category != rhs.category {
                return lhs.category.rawValue < rhs.category.rawValue
            }
            return lhs.matchedText < rhs.matchedText
        }

        var seen = Set<String>()
        var deduplicated: [LexicalHit] = []
        for hit in orderedHits {
            let key = [
                hit.category.rawValue,
                hit.matchedText,
                String(format: "%.6f", locale: Locale(identifier: "en_US_POSIX"), hit.startTime),
                String(format: "%.6f", locale: Locale(identifier: "en_US_POSIX"), hit.endTime)
            ].joined(separator: "|")
            if seen.insert(key).inserted {
                deduplicated.append(hit)
            }
        }
        return deduplicated
    }

    private func hotPathEvidenceText(
        from hits: [LexicalHit],
        fallbackTexts: [String]
    ) -> String {
        let orderedFragments = hits.isEmpty
            ? fallbackTexts
            : hits.map(\.matchedText)

        return orderedFragments.reduce(into: [String]()) { partial, fragment in
            guard !fragment.isEmpty else { return }
            if !partial.contains(fragment) {
                partial.append(fragment)
            }
        }.joined(separator: " | ")
    }

    /// Fetch feature windows for each lexical candidate and run the classifier.
    ///
    /// playhead-43ed: when `repeatedAdCache` is wired, lookups measure
    /// show-scoped recurrence alongside the classifier. A cache row contains
    /// source-episode absolute coordinates, so it must never replace the
    /// current candidate's classifier result.
    private func classifyCandidates(
        _ candidates: [LexicalCandidate],
        analysisAssetId: String,
        podcastId: String?
    ) async throws -> [ClassifierResult] {
        var inputs: [ClassifierInput] = []
        var deferredCacheOutcomeByCandidate: [String: Bool] = [:]
        // We intentionally do NOT record outcomes at lookup time
        // because most lexical candidates are non-ads — recording a miss
        // for a candidate the classifier ultimately rejects below the
        // store-confidence floor would saturate the rolling-window
        // hit-rate metric with noise on every fresh show, tripping the
        // 5% auto-disable floor on the very first session before the
        // cache has had any chance to warm.
        //
        // Instead we defer the outcome to AFTER classification and only
        // record the hit/miss when the classifier verdict
        // clears the same gate that controls `store(...)`
        // (`adProbability >= storeConfidenceThreshold`). That makes the
        // metric "out of confirmed-ad candidates, how many were answered
        // from cache" — the actual signal the auto-disable guard wants.
        // The request's show identity is authoritative for recurrence
        // matching. This site never fell back to the `currentPodcastProfile`
        // slot, on the grounds that "that actor state may belong to the
        // previous episode, and a nil/blank request must fail closed instead
        // of querying a stale show's cache". playhead-2kxd made that the rule
        // for the WHOLE FILE rather than the discipline of this one site: the
        // profile is keyed by show now, and `cachedPodcastProfile(forShowId:)`
        // fails closed on nil/empty exactly as this line does.
        let cacheShowId =
            RecurrenceMaterialIdentity.canonicalIdentifier(podcastId)

        for candidate in candidates {
            // Layer 0: Fetch acoustic features overlapping this candidate.
            // Extend the search range slightly to allow boundary snapping.
            let margin = 5.0
            let featureWindows = try await store.fetchFeatureWindows(
                assetId: analysisAssetId,
                from: candidate.startTime - margin,
                to: candidate.endTime + margin
            )

            // playhead-43ed: cache lookup before invoking the classifier.
            // Three guards keep the cache strictly opt-in and side-effect-
            // safe: (1) a real RepeatedAdCacheService has been wired,
            // (2) the current request carries a non-empty showId,
            // (3) the fingerprint derives to a non-zero value (zero is a
            // documented "do not cache" sentinel — see
            // RepeatedAdFingerprint.zero).
            if let cache = repeatedAdCache,
               let showId = cacheShowId,
               !showId.isEmpty {
                let fp = RepeatedAdFingerprint.from(featureWindows: featureWindows)
                if !fp.isZero {
                    // Both lookup and outcome recording are best-effort:
                    // a transient SQLite hiccup must not bring down the
                    // hot path. Lookup failures are logged and the candidate
                    // falls through to the classifier as if the cache missed.
                    do {
                        let outcome = try await cache.lookup(showId: showId, fingerprint: fp)
                        switch outcome {
                        case .hit:
                            deferredCacheOutcomeByCandidate[candidate.id] =
                                true
                        case .miss:
                            deferredCacheOutcomeByCandidate[candidate.id] =
                                false
                        case .skippedDisabled:
                            // Cache is currently disabled (kill-switch or
                            // auto-disable). Do NOT record an outcome —
                            // the rolling window only counts samples taken
                            // while the cache is live.
                            break
                        }
                    } catch {
                        logger.error("RepeatedAdCache lookup failed; falling through to classifier: \(String(describing: error))")
                    }
                }
            }

            inputs.append(ClassifierInput(
                candidate: candidate,
                featureWindows: featureWindows,
                episodeDuration: episodeDuration
            ))
        }

        // Layer 2: classify every candidate. Recurrence is never a parallel
        // automatic promotion channel.
        let classifierResults = classifier.classify(inputs: inputs, priors: showPriors)

        // Now that classification is done, record recurrence outcomes only
        // for candidates whose current-episode verdict
        // clears the same `storeConfidenceThreshold` gate that controls
        // `store(...)`. A non-ad candidate with low classifier
        // probability is noise, not signal, and must not feed the
        // rolling-window auto-disable metric.
        if let cache = repeatedAdCache,
           !deferredCacheOutcomeByCandidate.isEmpty {
            let storeFloor = cache.config.storeConfidenceThreshold
            for result in classifierResults
            where result.adProbability >= storeFloor {
                if let hit =
                    deferredCacheOutcomeByCandidate[result.candidateId] {
                    do {
                        try await cache.recordOutcome(hit: hit)
                    } catch {
                        logger.error(
                            "RepeatedAdCache outcome recording failed: \(String(describing: error))"
                        )
                    }
                }
            }
        }

        return classifierResults
    }

    // MARK: - Hot-path Decision Logging

    /// playhead-8em9 (narL): emit one DecisionLogEntry per classifier
    /// result produced by the hot path. Pre-fusion, so the ledger has a
    /// single `.classifier` entry and the fused breakdown degenerates
    /// to one source. `finalDecision.action` is "hotPathCandidate" when
    /// the result passed the candidate threshold and "hotPathBelowThreshold"
    /// otherwise — replay tooling can filter on this to distinguish from
    /// backfill-fusion entries.
    private func emitHotPathDecisionLogs(
        classifierResults: [ClassifierResult],
        analysisAssetId: String
    ) async {
        let snapshot = DecisionLogEntry.ActivationConfigSnapshot(
            MetadataActivationConfig.resolved()
        )
        // Match BackfillEvidenceFusion.buildLedger: cap-scale the classifier
        // score so hot-path and backfill ledger entries are directly comparable.
        let fusionConfig = FusionWeightConfig()
        let classifierCap = fusionConfig.classifierCap
        for result in classifierResults {
            let timestamp = Date().timeIntervalSince1970
            let passed = result.adProbability >= config.candidateThreshold
            // A classifier-only window clearing the autoSkip threshold is
            // skip-worthy on its own merit. Surfacing it as "autoSkipEligible"
            // (rather than generic "hotPathCandidate") makes the signal
            // visible to downstream consumers — including the NARL corpus
            // builder, whose `isAdUnderDefault(policyAction:)` mapping
            // (playhead-gtt9.19) treats `autoSkipEligible` as a positive ad
            // determination via exact raw-value match. `hotPathCandidate`
            // is explicitly mapped to `false` because it's an intermediate
            // state, not a final ad verdict.
            //
            // Regression: 2026-04-23 dogfood capture asset
            // 71F0C2AE-7260-4D1E-B41A-BCFD5103A641 @ [7006..7008],
            // classifier 0.8154, surfaced as "hotPathCandidate" → invisible
            // to the harness → GT=3, Pred=0, Sec-F1=0.
            //
            // playhead-fqc8 cycle-1 review HIGH-2: the hot path keeps the
            // standard 0.80 threshold here because the qualified-track
            // signal (the acoustic-break alignment that gates
            // `PromotionTrack.classifierSeedQualified`) only joins the
            // ledger after fusion runs in `runBackfill`. A classifier-
            // seeded span we passed on at, say, 0.55 may still be promoted
            // to `autoSkipEligible` later via the looser
            // `classifierSeedQualifiedThreshold` once the alignment
            // evidence is available. This bifurcation is intentional —
            // see the header docstring on `PromotionTrack`.
            let promotesToAutoSkip = result.adProbability >= config.autoSkipConfidenceThreshold
            let action: String
            let thresholdCrossed: Double
            if promotesToAutoSkip {
                action = "autoSkipEligible"
                thresholdCrossed = config.autoSkipConfidenceThreshold
            } else if passed {
                action = "hotPathCandidate"
                thresholdCrossed = config.candidateThreshold
            } else {
                action = "hotPathBelowThreshold"
                thresholdCrossed = config.candidateThreshold
            }
            let clampedScore = max(0.0, min(1.0, result.adProbability))
            let cappedWeight = min(clampedScore * classifierCap, classifierCap)
            let classifierEntry = EvidenceLedgerEntry(
                source: .classifier,
                weight: cappedWeight,
                detail: .classifier(score: result.adProbability)
            )
            // Authority mirrors DecisionExplanation.build: weight > cap/2 → strong.
            let authority: ProposalAuthority = cappedWeight > classifierCap * 0.5 ? .strong : .weak
            let breakdown = [
                SourceEvidence(
                    source: EvidenceSourceType.classifier.rawValue,
                    weight: cappedWeight,
                    capApplied: classifierCap,
                    authority: authority
                )
            ]
            // playhead-gtt9.20: for autoSkip-eligible candidates, carry the
            // gtt9.4.1 boundary-expanded bounds into the decision log instead
            // of the raw 2-s classifier slot. AdWindow already gets expanded
            // bounds via `expandedBounds(for:)` in `runHotPath`; without this
            // mirror, `DecisionLogEntry.windowBounds` stays at the narrow slot
            // and the NARL harness scores even confidently-detected closing-
            // block ads as FN (IoU = 2 / span_width ≪ 0.3).
            //
            // Below-autoSkip results keep raw bounds — they're informational
            // shadow logs, and `expandedBounds` short-circuits anyway.
            let logBounds: (start: Double, end: Double)
            if promotesToAutoSkip {
                let expanded = await expandedBounds(
                    for: result,
                    analysisAssetId: analysisAssetId
                )
                logBounds = (expanded.startTime, expanded.endTime)
            } else {
                logBounds = (result.startTime, result.endTime)
            }
            let logEntry = DecisionLogEntry(
                schemaVersion: DecisionLogEntry.currentSchemaVersion,
                analysisAssetID: analysisAssetId,
                timestamp: timestamp,
                windowBounds: .init(start: logBounds.start, end: logBounds.end),
                activationConfig: snapshot,
                evidence: [DecisionLogEntry.LedgerEntry(classifierEntry)],
                fusedConfidence: .init(
                    proposalConfidence: result.adProbability,
                    skipConfidence: result.adProbability,
                    breakdown: breakdown
                ),
                finalDecision: .init(
                    action: action,
                    gate: "eligible",
                    skipConfidence: result.adProbability,
                    thresholdCrossed: thresholdCrossed
                )
            )
            await decisionLogger.record(logEntry)
        }
    }

    // MARK: - Segment Aggregation (playhead-0usd)

    /// Fuse per-window classifier scores from Tier 1 + Tier 2 into coherent
    /// segments via `SegmentAggregator`, build `AdWindow`s for promoted
    /// segments that don't overlap an existing single-window AdWindow from
    /// this run, and emit distinguishing decision-log entries for
    /// observability.
    ///
    /// Additive contract: the aggregator path is a parallel channel to the
    /// existing single-window promotion — it never overrides a single-window
    /// AdWindow, only ADDS windows when the single-window path missed them.
    ///
    /// - Parameters:
    ///   - tier1Results: Per-Tier-1-slot classifier results (30 s slots).
    ///   - tier2Results: Per-hot-path-candidate classifier results (2 s
    ///     lexical-derived regions). Pass `[]` when the hot path bypasses
    ///     transcript scoring (empty chunks, or no candidates from chunks).
    ///   - singleWindowAdWindows: AdWindows already produced by the single-
    ///     window hot path in this run. Aggregator segments overlapping any
    ///     of these are dropped — the single-window result wins (it carries
    ///     richer evidence text / gtt9.4.1 boundary expansion).
    ///   - analysisAssetId: Asset under analysis.
    /// - Returns: Net-new aggregator-promoted AdWindows ready for
    ///   persistence. Caller wires them into `upsertHotPathAdWindows`.
    private func runSegmentAggregation(
        tier1Results: [ClassifierResult],
        tier2Results: [ClassifierResult],
        singleWindowAdWindows: [AdWindow],
        analysisAssetId: String,
        lexicalCandidates: [LexicalCandidate] = [],
        podcastId: String?
    ) async throws -> [AdWindow] {
        // Merge tier 1 + tier 2 into a single sorted WindowScore stream.
        // SegmentAggregator requires ASC by startTime.
        let allResults = (tier1Results + tier2Results)
            .filter { $0.endTime > $0.startTime }
        guard !allResults.isEmpty else { return [] }

        let windowScores: [SegmentAggregator.WindowScore] = allResults
            .map {
                SegmentAggregator.WindowScore(
                    startTime: $0.startTime,
                    endTime: $0.endTime,
                    score: max(0.0, min(1.0, $0.adProbability))
                )
            }
            .sorted { lhs, rhs in
                if lhs.startTime != rhs.startTime { return lhs.startTime < rhs.startTime }
                return lhs.endTime < rhs.endTime
            }

        let segments = SegmentAggregator.aggregate(windows: windowScores)
        let promotedSegments = segments.filter(\.promoted)
        let boundarySingletonSegments = boundarySingletonPromotedSegments(
            tier1Results: tier1Results,
            existingSegments: promotedSegments,
            episodeDuration: episodeDuration
        )
        let surfacedSegments = promotedSegments + boundarySingletonSegments
        // playhead-9ro7: boundary-singleton mark-only enforcement now
        // lives in `precisionGateLabel` (slot-only firedSignals → demote
        // to "markOnly"), so both this aggregator path and the single-
        // window path at the top of `runHotPath` honor the same rule.
        guard !surfacedSegments.isEmpty else { return [] }

        // Observability: emit one decision-log entry per promoted segment
        // with a distinguishing action string so replay tooling can
        // distinguish aggregator promotions from single-window promotions
        // (which carry "hotPathCandidate" / "autoSkipEligible").
        let fusionConfig = FusionWeightConfig()
        let classifierCap = fusionConfig.classifierCap
        let activationSnapshot = DecisionLogEntry.ActivationConfigSnapshot(
            MetadataActivationConfig.resolved()
        )
        for segment in surfacedSegments {
            let timestamp = Date().timeIntervalSince1970
            let clampedScore = max(0.0, min(1.0, segment.segmentScore))
            let cappedWeight = min(clampedScore * classifierCap, classifierCap)
            let classifierEntry = EvidenceLedgerEntry(
                source: .classifier,
                weight: cappedWeight,
                detail: .classifier(score: segment.segmentScore)
            )
            let authority: ProposalAuthority = cappedWeight > classifierCap * 0.5 ? .strong : .weak
            let breakdown = [
                SourceEvidence(
                    source: EvidenceSourceType.classifier.rawValue,
                    weight: cappedWeight,
                    capApplied: classifierCap,
                    authority: authority
                )
            ]
            let entry = DecisionLogEntry(
                schemaVersion: DecisionLogEntry.currentSchemaVersion,
                analysisAssetID: analysisAssetId,
                timestamp: timestamp,
                windowBounds: .init(start: segment.startTime, end: segment.endTime),
                activationConfig: activationSnapshot,
                evidence: [DecisionLogEntry.LedgerEntry(classifierEntry)],
                fusedConfidence: .init(
                    proposalConfidence: segment.segmentScore,
                    skipConfidence: segment.segmentScore,
                    breakdown: breakdown
                ),
                finalDecision: .init(
                    action: Self.segmentAggregatorPromotedAction,
                    gate: "eligible",
                    skipConfidence: segment.segmentScore,
                    thresholdCrossed: SegmentAggregatorConfig.default.promotionThreshold
                )
            )
            await decisionLogger.record(entry)
        }

        // Drop segments overlapping any single-window AdWindow already
        // produced this run. A half-open-interval overlap test suffices:
        // [s.start, s.end) intersects [w.start, w.end) iff s.end > w.start
        // && s.start < w.end.
        //
        // Also drop segments overlapping any previously-persisted AdWindow
        // for this asset, regardless of boundaryState. Without this guard,
        // re-running the hot path (e.g. on transcript-coverage progress)
        // would insert duplicate aggregator windows at the same span with
        // fresh UUIDs, because aggregator windows carry no lexical evidence
        // to reconcile against. Including single-window AdWindows in this
        // check covers replays where a prior single-window window is still
        // persisted but the current replay has no transcript chunks to
        // regenerate it — the aggregator would otherwise add a duplicate-
        // span aggregator window next to the existing single-window one.
        let previouslyPersistedWindows = try await store
            .fetchAdWindows(assetId: analysisAssetId)
            .filter { $0.detectorVersion == config.detectorVersion }
        let surviving = surfacedSegments.filter { segment in
            let overlapsSingleWindow = singleWindowAdWindows.contains { window in
                segment.endTime > window.startTime && segment.startTime < window.endTime
            }
            let overlapsExistingWindow = previouslyPersistedWindows.contains { window in
                segment.endTime > window.startTime && segment.startTime < window.endTime
            }
            return !overlapsSingleWindow && !overlapsExistingWindow
        }
        guard !surviving.isEmpty else { return [] }

        // Build AdWindows for surviving segments. boundaryState uses the
        // dedicated `.segmentAggregated` marker so downstream observability
        // can tell aggregator windows from lexical / acoustic-refined ones.
        //
        // playhead-gtt9.11: each aggregator segment passes through the
        // precision gate before persistence. The gate determines
        // eligibilityGate = "autoSkip" | "markOnly" based on score,
        // duration, and the safety-signal conjunction — PRESENCE only.
        // playhead-bllt then applies the EXTENT half at the emission site
        // below, which is what stops a promoted segment's invented edges from
        // reaching the auto-skip tier. Lexical categories
        // for the gate are the union across lexical candidates that
        // overlap the segment span (Tier 1-only segments carry an empty
        // set — this is honest: no lexical evidence exists).
        var newWindows: [AdWindow] = []
        for segment in surviving {
            let overlappingCategories = lexicalCandidates
                .filter { lc in
                    // Half-open overlap on [start, end).
                    lc.endTime > segment.startTime && lc.startTime < segment.endTime
                }
                .reduce(into: Set<LexicalPatternCategory>()) { acc, lc in
                    acc.formUnion(lc.categories)
                }
            let gateResult = await precisionGateLabel(
                analysisAssetId: analysisAssetId,
                startTime: segment.startTime,
                endTime: segment.endTime,
                segmentScore: segment.segmentScore,
                lexicalCategories: overlappingCategories,
                podcastId: podcastId
            )
            // playhead-bllt: the EXTENT half, for the aggregator producer.
            // A promoted segment's edges are wherever the scoring windows
            // happened to fall — the aggregator coalesces scores, it never
            // observes a boundary — so both anchors are `.unanchored`, and an
            // `"autoSkip"` verdict over them demotes to `"markOnly"` exactly as
            // a fused verdict does under playhead-2350. Same variable persisted
            // and consulted; see the single-window site for the full argument.
            let extentSupport = SpanExtentSupport.unanchored
            let gatedLabel = HotPathExtentGate.gatedLabel(
                gateResult.label,
                extent: extentSupport,
                blockingUnanchoredAutoSkip: config.unanchoredExtentBlocksAutoSkip
            )
            if gatedLabel != gateResult.label {
                logger.info(
                    "[bllt] hot-path aggregator \(segment.startTime, format: .fixed(precision: 2))–\(segment.endTime, format: .fixed(precision: 2)) extent unanchored on \(extentSupport.unanchoredEdges.joined(separator: "+"), privacy: .public) — autoSkip→markOnly (segmentScore \(segment.segmentScore, format: .fixed(precision: 2)) unchanged)"
                )
            }
            // A nil label means detection-only. Do not turn that fail-closed
            // result into a legacy nil-gated AdWindow at this second producer
            // site.
            guard let eligibilityGate = gatedLabel else { continue }
            newWindows.append(
                AdWindow(
                    id: UUID().uuidString,
                    analysisAssetId: analysisAssetId,
                    startTime: segment.startTime,
                    endTime: segment.endTime,
                    confidence: segment.segmentScore,
                    boundaryState: AdBoundaryState.segmentAggregated.rawValue,
                    decisionState: AdDecisionState.candidate.rawValue,
                    detectorVersion: config.detectorVersion,
                    advertiser: nil,
                    product: nil,
                    adDescription: nil,
                    evidenceText: nil,
                    evidenceStartTime: nil,
                    metadataSource: "none",
                    metadataConfidence: nil,
                    metadataPromptVersion: nil,
                    wasSkipped: false,
                    userDismissedBanner: false,
                    eligibilityGate: eligibilityGate,
                    catalogStoreMatchSimilarity:
                        gateResult.catalogStoreMatchSimilarity,
                    catalogFingerprintVersion: gateResult.catalogMatch?
                        .entry.acousticFingerprint.version.rawValue,
                    catalogMatchedEntryId: gateResult.catalogMatch?.entry.id.uuidString,
                    catalogMatchedShowId: gateResult.catalogMatch?.entry.showId,
                    catalogMatchedLearningSource: gateResult.catalogMatch?
                        .entry.learningSource.rawValue,
                    catalogMatchedLearningLifecycle: gateResult.catalogMatch?
                        .entry.learningLifecycle.rawValue,
                    startEdgeAnchor: extentSupport.startAnchor.rawValue,
                    endEdgeAnchor: extentSupport.endAnchor.rawValue
                )
            )
        }
        logger.info("Hot path: aggregator produced \(newWindows.count) windows (of \(surfacedSegments.count) surfaced segments, \(surfacedSegments.count - surviving.count) deduped against single-window path)")
        return newWindows
    }

    // MARK: - Correction Replay (playhead-hygc.1.8)

    /// `boundaryState` literal stamped on AdWindows produced by the
    /// correction-replay recall step. Distinct from `userMarked` (the
    /// `recordUserMarkedAd` row written when the user first reports the
    /// ad) so dogfood / NARL telemetry can attribute recall recovery to
    /// the replay path specifically.
    private static let correctionReplayBoundaryState: String = "correctionReplay"

    /// `metadataSource` stamp that mirrors `correctionReplayBoundaryState`
    /// for correction-replay rows. Keeps the `userCorrection` source
    /// (used by `recordUserMarkedAd` for the original userMarked write)
    /// distinct from this replay-derived shadow row.
    private static let correctionReplayMetadataSource: String = "userCorrectionReplay"

    /// Build mark-only AdWindows from `.falseNegative` `.exactTimeSpan`
    /// correction events that have no overlapping AdWindow on the asset.
    ///
    /// Why this exists: a `.falseNegative` correction is the user's
    /// strongest possible label — they explicitly said "this WAS an ad."
    /// In the live runtime path this becomes a `userMarked` AdWindow
    /// inside `recordUserMarkedAd`, so subsequent runs of the same
    /// episode see the row via `fetchAdWindows`. But for any analysis
    /// SQLite that arrives mid-pipeline (cross-launch preload, NARL
    /// dogfood replay, schema-rebased restore) where the userMarked
    /// AdWindow row is missing while the correction event survives, we
    /// MUST still surface a candidate or the recall metric drops on
    /// every replay.
    ///
    /// Idempotency: rows whose span overlaps any existing AdWindow on
    /// the asset are skipped — including AdWindows in `.reverted` state
    /// (so a later `.falsePositive` veto via `revertByTimeRange` is not
    /// undone by the next hot-path run). The first run synthesizes the
    /// AdWindow; the second run finds it via `fetchAdWindows` and
    /// short-circuits before re-emitting it.
    ///
    /// Suppression: a `.falseNegative` correction whose time range is
    /// fully covered by any `.falsePositive` correction (regardless of
    /// relative ordering — typically the FP is recorded after the FN, but
    /// we don't depend on `createdAt` since clock skew can scramble it)
    /// is suppressed. This protects the precision contract when the user
    /// has effectively retracted their false-negative report (e.g.
    /// reported the ad, then realised it was content and vetoed it).
    ///
    /// All emitted rows are stamped with `eligibilityGate = "markOnly"`
    /// so the suggest-tier banner — and explicitly NOT auto-skip — is
    /// the surface. This is the precision-safe recall lever.
    ///
    /// Concurrency precondition: this function runs to completion under
    /// the `AdDetectionService` actor's serialization, BUT it spans
    /// multiple `await`s (`loadCorrectionEvents`, `fetchAdWindows`, and
    /// the caller's subsequent `upsertHotPathAdWindows`). Actor
    /// reentrancy could in principle let two concurrent
    /// `runHotPathResult` calls each compute fresh UUIDs from the same
    /// FN range and double-insert. The production caller chain holds the
    /// no-concurrent-runHotPath-per-asset invariant:
    ///   - `AnalysisCoordinator.handlePersistedTranscriptChunks` is
    ///     dispatched from a single `for await` loop (single-shot per
    ///     `chunksPersisted` event, serialized).
    ///   - `AnalysisJobRunner.run` invokes `runHotPath` once per asset
    ///     job and the runner is itself a serial executor.
    /// R7: the invariant is now ENFORCED — `runHotPathResult` tracks
    /// per-asset in-flight state in `hotPathRunInFlightAssetIds` and
    /// fires `assertionFailure` (DEBUG crash, RELEASE warning log) on a
    /// concurrent re-entry for the same asset. The earlier rounds (R0–R6)
    /// only documented the invariant; R7 added the runtime check so a
    /// future caller cannot quietly violate it.
    private func correctionReplayCandidates(
        analysisAssetId: String
    ) async throws -> [AdWindow] {
        let events: [CorrectionEvent]
        do {
            events = try await store.loadCorrectionEvents(
                analysisAssetId: analysisAssetId
            )
        } catch {
            logger.warning("correctionReplayCandidates: loadCorrectionEvents failed: \(error.localizedDescription, privacy: .public)")
            return []
        }
        // Explicit banner receipts are private acknowledgements of an
        // already-materialized detection row. They must never enter the
        // correction-replay synthesizer: treating either Yes route as a
        // generic false negative would create a fresh random-ID window and
        // leak the answer into diagnostics/sharing on the next hot-path run.
        let replayEvents = events.filter {
            !$0.isPrivateExplicitFeedbackReceipt
        }
        guard !replayEvents.isEmpty else { return [] }

        // Build the set of `.exactTimeSpan` ranges that have a
        // `.falsePositive` correction. These mask `.falseNegative`
        // ranges they fully cover so a vetoed span is never re-emitted.
        var falsePositiveRanges: [(start: Double, end: Double)] = []
        for event in replayEvents
            where event.correctionType == .falsePositive {
            guard let scope = CorrectionScope.deserialize(event.scope) else { continue }
            guard case .exactTimeSpan(_, let s, let e) = scope else { continue }
            falsePositiveRanges.append((s, e))
        }

        // Collect unique `.falseNegative` `.exactTimeSpan` ranges.
        var falseNegativeRanges: [(start: Double, end: Double)] = []
        var seen: Set<String> = []
        for event in replayEvents
            where event.correctionType == .falseNegative {
            guard let scope = CorrectionScope.deserialize(event.scope) else { continue }
            guard case .exactTimeSpan(_, let s, let e) = scope else { continue }
            // Defensive: reject non-finite or zero-duration spans.
            guard s.isFinite, e.isFinite, e > s else { continue }
            // Suppress if any falsePositive range fully covers this span.
            let suppressed = falsePositiveRanges.contains { fp in
                fp.start <= s && fp.end >= e
            }
            if suppressed { continue }
            // Dedupe by serialized %.3f-%.3f pair (matches scope serialization
            // precision). One emit per unique span even if the user reported
            // it multiple times.
            let key = String(format: "%.3f-%.3f", s, e)
            if seen.contains(key) { continue }
            seen.insert(key)
            falseNegativeRanges.append((s, e))
        }
        guard !falseNegativeRanges.isEmpty else { return [] }

        // Skip ranges that already have an overlapping AdWindow (any
        // decisionState — candidate, confirmed, applied, suppressed,
        // reverted). `.reverted` rows persist a user veto, and we MUST
        // NOT resurface those.
        let existing: [AdWindow]
        do {
            existing = try await store.fetchAdWindows(assetId: analysisAssetId)
        } catch {
            logger.warning("correctionReplayCandidates: fetchAdWindows failed: \(error.localizedDescription, privacy: .public)")
            return []
        }

        var emitted: [AdWindow] = []
        let detectorVersion = config.detectorVersion
        for (start, end) in falseNegativeRanges {
            let overlapsExisting = existing.contains { window in
                window.startTime < end && window.endTime > start
            }
            if overlapsExisting { continue }
            // playhead-hygc.1.8 (R7): dedupe-by-overlap across the in-flight
            // emit set. The exact-match `seen` key above (`%.3f-%.3f`)
            // catches duplicate user reports with identical span numerics
            // but not slightly-different overlapping spans (e.g. a single
            // ad reported with ranges [600, 680] and [610, 690]). Without
            // this guard both would persist, leaving two suggest banners
            // — and an `acceptSuggestedSkip` on either would only veto its
            // own row. Skip any FN range that already overlaps a row we
            // just queued in this same call.
            let overlapsEmitted = emitted.contains { window in
                window.startTime < end && window.endTime > start
            }
            if overlapsEmitted { continue }
            let id = UUID().uuidString
            // playhead-tpoq: ONE extent, written to three columns, so the
            // gate and the anchors cannot disagree. A correction replay has no
            // proven edges — the listener said "there was an ad here" and the
            // detector drew nothing — so both anchors are unanchored and the
            // gate derives to `.markOnly`, which is the literal this used to
            // hard-code. Same value out; what changes is that the next person
            // with a reason to promote correction replay cannot do it by
            // editing one literal while the row's evidence says otherwise.
            // This is the mechanism playhead-mqqd shipped for the other two
            // composers; this was the fourth producer it did not reach.
            let support = SpanExtentSupport(
                startAnchor: .unanchored,
                endAnchor: .unanchored
            )
            let row = AdWindow(
                id: id,
                analysisAssetId: analysisAssetId,
                startTime: start,
                endTime: end,
                confidence: 1.0,
                boundaryState: Self.correctionReplayBoundaryState,
                decisionState: AdDecisionState.candidate.rawValue,
                detectorVersion: detectorVersion,
                advertiser: nil,
                product: nil,
                adDescription: nil,
                evidenceText: nil,
                evidenceStartTime: start,
                metadataSource: Self.correctionReplayMetadataSource,
                metadataConfidence: nil,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false,
                evidenceSources: nil,
                eligibilityGate: ComposedMarkGate.eligibility(for: support).rawValue,
                catalogStoreMatchSimilarity: nil,
                startEdgeAnchor: support.startAnchor.rawValue,
                endEdgeAnchor: support.endAnchor.rawValue
            )
            emitted.append(row)
        }
        return emitted
    }

    // MARK: - Day-0 byte-exact rediff mint (playhead-xsdz.36.4)

    /// `boundaryState` literal stamped on AdWindows minted by the DAY-0
    /// byte-exact rediff path — clear provenance so dogfood/NARL telemetry can
    /// attribute a first-listen mark to the deterministic day-0 byte differ
    /// (distinct from the lagged `.rediffSlot` decoded-span width marks and from
    /// `correctionReplay`). Internal (not private) so the reconcile/export
    /// protection axis tests can pin it against the literals those sites use.
    ///
    /// PROTECTED like a user mark: day-0 byte-exact marks are DETERMINISTIC
    /// ground truth for the user's OWN played stitch, so they must PERSIST across
    /// a later analysis run whose transcript/FM fusion would not re-emit them.
    /// The literal "dayZeroRediffByteExact" is therefore also listed in
    /// `reconcileProtectedBoundaryStates` (backfill), excluded in
    /// `hotPathCandidateRevisions` (hot-path retirement), and recognized as
    /// local-only in `CrossUserSharing.isLocalOnlyBoundaryState` (so it is never
    /// exported and never aborts a cross-user snapshot). Retirable ONLY by an
    /// explicit user veto (`decisionState = .reverted`).
    static let dayZeroRediffByteExactBoundaryState: String = "dayZeroRediffByteExact"

    /// `metadataSource` stamp mirroring `dayZeroRediffByteExactBoundaryState` so
    /// the mark's source is recorded on the row.
    static let dayZeroRediffByteExactMetadataSource: String = "rediffDayZeroByteExact"

    /// playhead-xsdz.36.4 — the DAY-0 byte-exact rediff mint: the FIRST-LISTEN
    /// marking path. Resolves the PINNED played A-side from the asset row (wrj8
    /// read-only mmap), byte-aligns it against each k-way B-copy, UNIONs the
    /// byte-EXACT divergent slots across personas, and persists each as a
    /// MARK-ONLY AdWindow banner. Returns the number of marks minted.
    ///
    /// WHY THIS BYPASSES PRESENCE-GATING (the narrow, guarded exception): every
    /// other rediff mark is presence-gated — a rediff slot only sets the WIDTH of
    /// a decoded span whose PRESENCE a transcript/FM core already established
    /// (`computeRediffSlotPass` guards on `!decodedSpans.isEmpty`, and
    /// `RediffSlotOwnership.candidates` widens EXISTING spans). On a true first
    /// listen no such core exists yet. But a byte-EXACT divergent region between
    /// the played A-side and a re-fetched B-side IS a dynamically-inserted ad
    /// segment, sample-accurately, with DETERMINISTIC certainty — a stronger
    /// signal than the transcript/FM core it would otherwise wait on. So a
    /// byte-exact day-0 slot is its OWN presence core. This is deliberately NARROW:
    ///   • BYTE-EXACT ONLY — the chroma differ / `refetchedBSideMono16kHz` is
    ///     never consulted here; a diff that falls back to chroma mints nothing.
    ///   • UNION (quorum = 1) — `RediffSlotOwnership.unionedPlayedSlots` mints a
    ///     slot ANY one persona's byte-exact diff reveals (the SAME lagged-path
    ///     primitive). The per-persona byte gate is the precision guard; a ≥2
    ///     AGREEMENT quorum was tried and REMOVED (playhead-wybg) — on client-
    ///     PINNED shows only ONE persona diverges (the rest COLLIDE, byte-
    ///     identical), so requiring agreement dropped real ads. Staging ≥2
    ///     distinct-persona B-copies is the COLLISION-RECOVERY floor, not agreement.
    ///   • Every false-widening guard still applies (`gateAndDiffBytes`:
    ///     min-run-bytes, re-encode/`chainedFraction` reject, min-ad-width;
    ///     then fragment-merge + duration-cap). playhead-9s6q FIX A: day-0 opts
    ///     into non-monotonic SEGMENT recovery (flag `nonMonotonicSegmentRecovery
    ///     Enabled`, default OFF) — a non-monotonic multi-break fetch is
    ///     segment-recovered (each guard re-applied over the segmented coverage)
    ///     rather than discarded; the lagged path keeps the strict reject.
    ///   • TIERED BY SKIP GRADE (playhead-qs0d, was blanket MARK-ONLY;
    ///     playhead-pyq7 widened the tier). A SKIP-GRADE slot records
    ///     `.rediffByteExact` on both edges and — under
    ///     `RediffActivation.dayZeroByteExactAutoSkipEnabled` —
    ///     `eligibilityGate = .eligible`, so the orchestrator auto-skips it at
    ///     98co-padded late-safe bounds. Skip grade is
    ///     `RediffSlotOwnership.dayZeroSlotIsSkipGrade`: every STRICT
    ///     monotonic-clean slot, plus — under
    ///     `dayZeroSegmentRecoveredAutoSkipEnabled` — every 9s6q
    ///     SEGMENT-RECOVERED one. With that switch off the recovered arm keeps
    ///     `.unanchored` + `.markOnly`, which is the qs0d behaviour verbatim.
    /// It does NOT change presence-gating for the lagged path, the chroma differ,
    /// or any non-day-0 flow.
    ///
    /// NEVER-PERSIST-B (wrj8/xsdz.28): the B bytes are read (mmap) only inside
    /// this call; the caller (`RediffRefetchService`) still deletes every B-copy.
    /// The persisted marks are A-time scalars only. The A-side file is READ-ONLY.
    func mintByteExactDayZeroMarks(
        analysisAssetId: String,
        bSideURLs: [URL]
    ) async -> RediffDayZeroMintOutcome {
        // Day-0 requires ≥2 distinct-persona B-copies for COLLISION RECOVERY: on
        // a client-pinned show a single re-fetch can collide (byte-identical → no
        // divergence), so staging ≥2 gives a divergence a chance. NOT an agreement
        // quorum — the mint UNIONs whatever diverges (playhead-wybg).
        guard bSideURLs.count >= RediffSlotOwnership.dayZeroMinKWayBCopies else {
            logger.info("[xsdz.36.4] day-0 mint skipped asset=\(analysisAssetId, privacy: .public): \(bSideURLs.count) B-side(s) < \(RediffSlotOwnership.dayZeroMinKWayBCopies) distinct-persona B-copies for collision recovery")
            return RediffDayZeroMintOutcome(exit: .tooFewBCopies, bSideCount: bSideURLs.count)
        }
        // The asset row supplies the PINNED A-side (wrj8: read-only played file).
        // playhead-p70f: resolved through the SAME helper the pre-fetch blocker
        // uses, so the free check and the real mint can never drift apart.
        let aData: Data
        switch await resolveDayZeroASide(analysisAssetId: analysisAssetId) {
        case .blocked(let exit):
            return RediffDayZeroMintOutcome(exit: exit, bSideCount: bSideURLs.count)
        case .ready(let mapped):
            aData = mapped
        }

        // BYTE-EXACT per-persona diffs ONLY. A B whose byte gate rejects (no
        // chained runs / non-monotonic / re-encode CDN) is a chroma-fallback
        // TRIGGER for the lagged pass — but day-0 mints NOTHING from it: it
        // simply does not contribute a per-persona slot list to the union.
        //
        // playhead-p70f: the two per-B `continue`s used to be invisible. They are
        // now COUNTED, because the counts are the diagnosis. `RediffByteAligner`
        // is an MP3-frame parser by construction; every device `sourceURL` ends
        // in `.mp3` but `DownloadManager` warns the suffix is normalized and is
        // "not evidence that the bytes are MP3". If the bytes are AAC/M4A the
        // aligner finds no frames and EVERY diff gate-rejects — which reads here
        // as `bSidesGateRejected == bSideCount`, unmistakably different from
        // "diffs accepted, copies simply agreed".
        var perBSideSlots: [[RediffSlotOwnership.PlayedSlot]] = []
        // playhead-qs0d: the STRICT (monotonic-clean) subset of the same
        // per-persona lists. `gateAndDiffBytes` takes the 9s6q segment-recovery
        // arm if and ONLY if `!alignment.monotonicClean`, so this partition is
        // an exact classification of which acceptance arm produced each list —
        // not a heuristic. It exists because the two arms earn different
        // certainty: a monotonic-clean chain proves its A-timeline mapping at
        // every edge, a segment-recovered one dropped runs to get there.
        var strictPerBSideSlots: [[RediffSlotOwnership.PlayedSlot]] = []
        // playhead-3zxd: the phantom-slot instrumentation, INDEX-ALIGNED WITH
        // `bSideURLs` — exactly one entry per B-copy, in k-way fetch order.
        //
        // A persona the gate did not accept contributes a ZEROED entry, which is
        // two facts at once and both are load-bearing. It adds nothing to any
        // sum (it minted no slot, so it has no emitted slots to measure, and
        // folding its run counts in would inflate the vacuity control into
        // looking like evidence) AND it holds its position, because
        // `RediffAlignedRunSpanCodec` labels each span group with its index and
        // documents that index as "the persona's position in the k-way fetch".
        //
        // R2 REVIEW: this list used to be APPENDED TO ONLY ON ACCEPTANCE, so it
        // arrived at the codec already compacted and `<bIndex>` silently became
        // the ordinal among ACCEPTED personas. One rejected copy ahead of an
        // accepted one and the payload said `0:` for persona 1 — an identity
        // that is not an identity. The codec's own test pins that it preserves
        // gaps without renumbering; production simply never handed it a gap.
        // The `defer` is what makes the alignment structural: every iteration
        // appends exactly once, down every `continue`, so an exit added here
        // later cannot silently re-compact the list.
        var perBSideByteDiagnostics: [RediffSlotOwnership.ByteDiagnostics] = []
        var unreadable = 0
        var gateRejected = 0
        for bSideURL in bSideURLs {
            var personaDiagnostics = RediffSlotOwnership.ByteDiagnostics.empty
            defer { perBSideByteDiagnostics.append(personaDiagnostics) }
            guard Self.isAnchoredRegularFile(bSideURL) else {
                unreadable += 1
                continue
            }
            let bData: Data
            do {
                bData = try Data(contentsOf: bSideURL, options: .mappedIfSafe)
            } catch {
                logger.warning("[xsdz.36.4] day-0 mint B-side read failed asset=\(analysisAssetId, privacy: .public): \(error.localizedDescription)")
                unreadable += 1
                continue
            }
            let alignment = RediffByteAligner.align(aData: aData, bData: bData)
            // playhead-9s6q FIX A: the day-0 mint OPTS IN to non-monotonic segment
            // recovery (flag default OFF) so a Fresh Air-class multi-break fetch
            // whose chain went non-monotonic yields its divergent ad slots instead
            // of nothing. Every precision guard still applies per segment.
            guard case .accepted(let acceptance) = RediffSlotOwnership.gateAndDiffBytes(
                alignment: alignment,
                recoverNonMonotonicSegments: RediffActivation.nonMonotonicSegmentRecoveryEnabled
            ) else {
                gateRejected += 1
                continue
            }
            perBSideSlots.append(acceptance.playedSlots)
            personaDiagnostics = acceptance.diagnostics
            if alignment.monotonicClean {
                strictPerBSideSlots.append(acceptance.playedSlots)
            }
        }
        let byteDiagnostics = RediffByteMintDiagnostics.combining(perBSideByteDiagnostics)

        /// Every counted outcome from here on carries the same per-B census.
        func outcome(
            _ exit: RediffDayZeroExit,
            markCount: Int = 0,
            divergentSlotCount: Int = 0,
            strictMarkCount: Int = 0,
            segmentRecoveredSkipGradeMarkCount: Int = 0,
            supersededMarkCount: Int = 0
        ) -> RediffDayZeroMintOutcome {
            RediffDayZeroMintOutcome(
                markCount: markCount,
                exit: exit,
                bSideCount: bSideURLs.count,
                bSidesAccepted: perBSideSlots.count,
                bSidesGateRejected: gateRejected,
                bSidesUnreadable: unreadable,
                divergentSlotCount: divergentSlotCount,
                strictMarkCount: strictMarkCount,
                segmentRecoveredSkipGradeMarkCount: segmentRecoveredSkipGradeMarkCount,
                supersededMarkCount: supersededMarkCount,
                byteDiagnostics: byteDiagnostics
            )
        }

        // NOT ONE copy produced a usable diff — the aligner had nothing to work
        // with. Distinct from `.noDivergentSlot` below, and the distinction is
        // the whole point: this is the exit that would fire if the .mp3 suffix
        // is lying about the container.
        guard !perBSideSlots.isEmpty else {
            logger.info("[xsdz.36.4] day-0 mint asset=\(analysisAssetId, privacy: .public): NO accepted byte diff — \(gateRejected) gate-rejected, \(unreadable) unreadable of \(bSideURLs.count) B-copies")
            return outcome(.noAcceptedByteDiff)
        }

        // UNION the byte-EXACT divergent slots across personas (false-widening
        // guards already applied per-list by `gateAndDiffBytes`). Quorum = 1: a
        // slot mints if ANY persona diverged on it — the SAME lagged-path union
        // primitive, so a client-pinned show where only one persona diverges
        // still mints (playhead-wybg). Overlapping same-slot detections collapse.
        let unioned = RediffSlotOwnership.unionedPlayedSlots(perBSideSlots)
        guard !unioned.isEmpty else {
            logger.info("[xsdz.36.4] day-0 mint asset=\(analysisAssetId, privacy: .public): \(perBSideSlots.count) byte-exact diff(s) ACCEPTED, no divergent slot — nothing minted")
            return outcome(.noDivergentSlot)
        }

        // Idempotency: skip any minted slot overlapping an existing AdWindow
        // (any decisionState, incl. a user-vetoed `.reverted` row) so a repeat
        // day-0 fire — or a day-0 run after some analysis already marked — never
        // double-banners or resurfaces a veto (mirrors correction-replay).
        let existing = (try? await store.fetchAdWindows(assetId: analysisAssetId)) ?? []
        // playhead-qs0d: classify each unioned slot by the acceptance arm that
        // produced it — STRICT monotonic-clean, or 9s6q segment-recovered. The
        // rule (and why it is an EXACT geometry match against the strict-only
        // union) lives in `strictByteExactMask`.
        let strictMask = RediffSlotOwnership.strictByteExactMask(
            unioned: unioned,
            strictPerBSideSlots: strictPerBSideSlots
        )
        var windows: [AdWindow] = []
        var strictMarkCount = 0
        // playhead-pyq7: the SEPARATE recovered counter. Deliberately not folded
        // into `strictMarkCount` — that value names the monotonic-clean
        // acceptance arm and is what the ug9m supersede guard and the rescue
        // census are stated over. This one names the promotion population, and
        // it is 0 on every build with the promotion switch off.
        var segmentRecoveredSkipGradeMarkCount = 0
        // playhead-ug9m: degraded day-0 rows from an EARLIER attempt that a
        // STRICT slot of THIS mint replaces. Keyed by id and carried as the
        // store's `expectedProducerRevisions`, so the retire is validated
        // against the row that was read rather than against a name.
        var superseded: [String: AdWindow] = [:]
        for (slotIndex, slot) in unioned.enumerated() {
            let strict = strictMask[slotIndex]
            let overlapsEmitted = windows.contains { $0.startTime < slot.endSeconds && $0.endTime > slot.startSeconds }
            if overlapsEmitted { continue }
            // playhead-c7ef: hoisted ABOVE the overlap filter, because the
            // filter now reads it. Nothing else about it moved.
            let skipGrade = RediffSlotOwnership.dayZeroSlotIsSkipGrade(
                strict: strict,
                segmentRecoveredPromotionEnabled: RediffActivation.dayZeroSegmentRecoveredAutoSkipEnabled
            )
            let overlappingExisting = existing.filter { $0.startTime < slot.endSeconds && $0.endTime > slot.startSeconds }
            if !overlappingExisting.isEmpty {
                // playhead-ug9m — THE OVERLAP FILTER, RELAXED IN EXACTLY ONE
                // DIRECTION. It still drops a slot that collides with anything
                // already on disk, with one exception: a slot this producer can
                // prove is better may supersede its OWN degraded rows.
                //
                // playhead-c7ef MOVED THE RULE INTO ONE NAMED PREDICATE and
                // replaced its `strict` leg. ug9m read `strict` here, which by
                // playhead-pyq7 no longer meant "provably better": a FRESH
                // recovered mint stamps anchors and auto-skips, while a
                // recovered RE-mint over the same episode was refused — so the
                // nine `unanchored`/`markOnly` rows on the owner's device could
                // never be improved by anything. `reMintMayReplace` states the
                // rule in three conjuncts (skip grade, the untouched fidelity
                // ladder, and one-row containment within the auto-skip margins)
                // whose joint consequence is a theorem rather than an appeal to
                // an acceptance arm: EVERY SECOND THIS MAKES AUTO-SKIPPABLE WAS
                // ALREADY MARKED AS AN AD BY THE ROW IT RETIRES. See the
                // derivation on the predicate.
                //
                // A refused slot behaves exactly as it always did: dropped, the
                // existing row survives untouched, and an attempt whose every
                // slot is refused ends `.allSlotsAlreadyCovered`.
                guard DayZeroMarkCensus.reMintMayReplace(
                    slotStartSeconds: slot.startSeconds,
                    slotEndSeconds: slot.endSeconds,
                    overlapping: overlappingExisting,
                    slotIsSkipGrade: skipGrade
                ) else { continue }
                for row in overlappingExisting { superseded[row.id] = row }
            }
            // THE BLOCKER this bead exists to clear (measured 2026-07-31): both
            // day-0 windows persisted with `startEdgeAnchor == endEdgeAnchor ==
            // unanchored` — the SAME pair the 0.40-confidence aggregator windows
            // carry — so the playhead-2350 gate held a 1.00-confidence,
            // sample-accurate boundary at mark-only, correctly, given what it
            // could see. `unanchored` meant "no anchor was recorded", never "the
            // boundary is unknown". The byte differ set BOTH edges of a strict
            // slot; record that.
            //
            // The anchor stamp is UNCONDITIONAL for a SKIP-GRADE slot — it is a
            // provenance fact, and it is what `AutoSkipEdgePadding` needs to
            // compute a late-safe window at all. Only the eligibility promotion
            // reads `dayZeroByteExactAutoSkipEnabled`.
            //
            // playhead-pyq7: skip grade is no longer the same question as
            // STRICTNESS. A 9s6q segment-recovered slot is skip-grade too when
            // `dayZeroSegmentRecoveredAutoSkipEnabled` is on — measured 2026-08-14
            // (7 phantoms → 0, 2450 s of eaten show → 0.0000 s, no recall lost,
            // inner edges +0.0002 s against a 0.50 s margin), and 69 % of every
            // day-0 mark the field has produced is in that population. The two
            // counters below stay SEPARATE for the reason this repo keeps
            // re-learning: `strictMarkCount` names the acceptance ARM and
            // widening it would make one value stand for two different things.
            if strict {
                strictMarkCount += 1
            } else if skipGrade {
                segmentRecoveredSkipGradeMarkCount += 1
            }
            let anchor: AutoSkipEdgeAnchor = skipGrade ? .rediffByteExact : .unanchored
            let gate: SkipEligibilityGate =
                (skipGrade && RediffActivation.dayZeroByteExactAutoSkipEnabled)
                    ? .eligible
                    : .markOnly
            windows.append(AdWindow(
                id: UUID().uuidString,
                analysisAssetId: analysisAssetId,
                startTime: slot.startSeconds,
                endTime: slot.endSeconds,
                confidence: 1.0,   // deterministic byte-exact certainty
                boundaryState: Self.dayZeroRediffByteExactBoundaryState,
                decisionState: AdDecisionState.candidate.rawValue,
                detectorVersion: config.detectorVersion,
                advertiser: nil,
                product: nil,
                adDescription: nil,
                evidenceText: nil,
                evidenceStartTime: slot.startSeconds,
                metadataSource: Self.dayZeroRediffByteExactMetadataSource,
                metadataConfidence: nil,
                metadataPromptVersion: nil,
                wasSkipped: false,
                userDismissedBanner: false,
                evidenceSources: nil,
                eligibilityGate: gate.rawValue,
                catalogStoreMatchSimilarity: nil,
                // playhead-qs0d: a STRICT monotonic-clean byte-exact slot
                // records `.rediffByteExact` on BOTH edges — the byte differ
                // set both, and refiners never touch a day-0 mark.
                //
                // playhead-pyq7: a 9s6q SEGMENT-RECOVERED slot records the same
                // pair, because the validation this comment used to defer to has
                // been done. What was actually wrong with the recovered arm was
                // never the EDGE (measured +0.0002 s inner, sign always safe) —
                // it was a whole-block false positive that playhead-3zxd
                // removed at its cause. With the promotion switch off it keeps
                // the conservative `.unanchored` pair and `.markOnly`, and that
                // is the rollback, not a second code path.
                startEdgeAnchor: anchor.rawValue,
                endEdgeAnchor: anchor.rawValue
            ))
        }
        guard !windows.isEmpty else {
            logger.info("[xsdz.36.4] day-0 mint asset=\(analysisAssetId, privacy: .public): all \(unioned.count) unioned slot(s) already covered by existing AdWindows — nothing minted")
            return outcome(.allSlotsAlreadyCovered, divergentSlotCount: unioned.count)
        }
        do {
            // playhead-ug9m: the retire and the insert are ONE transaction, and
            // `expectedProducerRevisions` makes the store re-validate every
            // retired row against what was read — so a concurrent user veto
            // between the read and the write aborts the whole mint rather than
            // silently deleting the veto.
            try await store.upsertHotPathAdWindows(
                windows,
                existingIDs: [],
                retiredIDs: Set(superseded.keys),
                expectedProducerRevisions: superseded
            )
        } catch {
            logger.warning("[xsdz.36.4] day-0 mint persist failed asset=\(analysisAssetId, privacy: .public): \(error.localizedDescription)")
            var failed = outcome(.persistFailed, divergentSlotCount: unioned.count)
            // playhead-luie: the SECOND writer of this field, and the one no
            // argument-shaped sweep could ever have found — it is an assignment
            // to a local's property, not a labelled argument. `detail` reaches
            // `rediff_day_zero_attempts.lastDetail` one struct later, so a
            // description here is the same durable-prose defect as the fetch
            // arm's. The arm is spelled by the row's `lastExit`
            // (`persist_failed`), so the token does not restate it.
            failed.detail = DurableThrowRecord.dayZeroAttemptDetail(for: error)
            return failed
        }
        let recoveredMarkOnlyCount = windows.count - strictMarkCount - segmentRecoveredSkipGradeMarkCount
        let skipGradeDisposition = RediffActivation.dayZeroByteExactAutoSkipEnabled
            ? "anchored, auto-skip eligible"
            : "anchored, mark-only"
        logger.info("[xsdz.36.4] day-0 byte-exact minted \(windows.count) banner(s) asset=\(analysisAssetId, privacy: .public) — \(strictMarkCount) STRICT byte-exact (\(skipGradeDisposition)), \(segmentRecoveredSkipGradeMarkCount) segment-recovered PROMOTED (\(skipGradeDisposition)) [pyq7], \(recoveredMarkOnlyCount) segment-recovered (unanchored, mark-only), \(superseded.count) degraded day-0 row(s) superseded [ug9m]")
        return outcome(
            .marked,
            markCount: windows.count,
            divergentSlotCount: unioned.count,
            strictMarkCount: strictMarkCount,
            segmentRecoveredSkipGradeMarkCount: segmentRecoveredSkipGradeMarkCount,
            supersededMarkCount: superseded.count
        )
    }

    /// playhead-p70f change 3 — the FREE, LOCAL-ONLY preconditions of the day-0
    /// mint, hoisted so the caller can run them BEFORE spending ~108 MB.
    ///
    /// Returns `nil` when the mint has a usable A-side. Every non-nil return is
    /// an exit the mint would have taken anyway — but AFTER the fetch had been
    /// downloaded and billed, which is precisely the defect: four of the mint's
    /// exits are pure local reads that cost nothing, and all four ran only once
    /// ~108 MB was already gone.
    func dayZeroPrefetchBlocker(analysisAssetId: String) async -> RediffDayZeroExit? {
        switch await resolveDayZeroASide(analysisAssetId: analysisAssetId) {
        case .blocked(let exit): return exit
        case .ready: return nil
        }
    }

    /// Shared A-side resolution for the day-0 mint AND its pre-fetch blocker:
    /// asset row → anchored `sourceURL` → read-only mmap. ONE implementation so
    /// the free pre-check and the real mint can never disagree about whether an
    /// attempt is doomed.
    ///
    /// SAFETY (wrj8): mapping is READ-ONLY. Day-0 never writes the pinned
    /// playback audio.
    private func resolveDayZeroASide(
        analysisAssetId: String
    ) async -> RediffDayZeroASideResolution {
        let asset: AnalysisAsset
        do {
            guard let fetched = try await store.fetchAsset(id: analysisAssetId) else {
                logger.info("[p70f] day-0 A-side unavailable asset=\(analysisAssetId, privacy: .public): no asset row")
                return .blocked(.assetRowMissing)
            }
            asset = fetched
        } catch {
            logger.warning("[p70f] day-0 A-side fetchAsset failed asset=\(analysisAssetId, privacy: .public): \(error.localizedDescription)")
            return .blocked(.assetFetchFailed)
        }
        guard let aSideURL = Self.byteDifferASideURL(sourceURL: asset.sourceURL) else {
            logger.info("[p70f] day-0 A-side unavailable asset=\(analysisAssetId, privacy: .public): sourceURL is not an anchored regular file")
            return .blocked(.aSideNotAnchored)
        }
        do {
            return .ready(try Data(contentsOf: aSideURL, options: .mappedIfSafe))
        } catch {
            logger.warning("[p70f] day-0 A-side read failed asset=\(analysisAssetId, privacy: .public): \(error.localizedDescription)")
            return .blocked(.aSideReadFailed)
        }
    }

    private func boundarySingletonPromotedSegments(
        tier1Results: [ClassifierResult],
        existingSegments: [AdSegmentCandidate],
        episodeDuration: Double
    ) -> [AdSegmentCandidate] {
        guard episodeDuration > 0 else { return [] }

        let slotFraction = AutoSkipPrecisionGateConfig.default.slotFraction
        let startWindow = min(
            Self.boundarySingletonStartWindowSeconds,
            max(AdDetectionService.tier1DefaultWindowSeconds, episodeDuration * slotFraction)
        )
        let endWindow = min(
            Self.boundarySingletonEndWindowSeconds,
            max(AdDetectionService.tier1DefaultWindowSeconds, episodeDuration * slotFraction)
        )
        let scoreFloor = AutoSkipPrecisionGateConfig.default.uiCandidateThreshold
        let gapTolerance = SegmentAggregatorConfig.default.maxInternalGapSeconds

        let boundaryCandidates = tier1Results
            .filter { result in
                let duration = result.endTime - result.startTime
                guard duration >= Self.boundarySingletonMinimumWindowSeconds,
                      result.adProbability >= scoreFloor
                else { return false }

                let center = (result.startTime + result.endTime) / 2
                let inStartSlot = center <= startWindow
                let inEndSlot = center >= (episodeDuration - endWindow)
                guard inStartSlot || inEndSlot else { return false }

                return !existingSegments.contains { segment in
                    result.endTime > segment.startTime && result.startTime < segment.endTime
                }
            }
            .sorted {
                if $0.startTime != $1.startTime { return $0.startTime < $1.startTime }
                return $0.endTime < $1.endTime
            }

        guard !boundaryCandidates.isEmpty else { return [] }

        var groups: [[ClassifierResult]] = []
        for result in boundaryCandidates {
            if let lastGroup = groups.last,
               let last = lastGroup.last,
               result.startTime <= last.endTime + gapTolerance {
                groups[groups.count - 1].append(result)
            } else {
                groups.append([result])
            }
        }

        return groups.compactMap { group in
            guard let first = group.first, let last = group.last else { return nil }
            let weighted = group.reduce(into: (sum: 0.0, duration: 0.0)) { acc, result in
                let duration = max(0, result.endTime - result.startTime)
                acc.sum += max(0, min(1, result.adProbability)) * duration
                acc.duration += duration
            }
            guard weighted.duration >= GlobalPriorDefaults.standard.typicalAdDuration.lowerBound,
                  weighted.duration > 0
            else { return nil }

            let score = weighted.sum / weighted.duration
            guard score >= scoreFloor else { return nil }
            return AdSegmentCandidate(
                startTime: first.startTime,
                endTime: last.endTime,
                segmentScore: score,
                windowCount: group.count,
                promoted: true
            )
        }
    }

    /// Decision-log `finalDecision.action` string stamped on aggregator-
    /// promoted segments so replay tooling can filter them out vs. single-
    /// window promotions. playhead-0usd.
    static let segmentAggregatorPromotedAction: String = "segmentAggregatorPromoted"

    // MARK: - AdWindow Construction (hot path)

    /// Build an `AdWindow` from a classifier result.
    ///
    /// - Parameters:
    ///   - expandedStartTime: Optional override for the persisted start time
    ///     produced by `PostClassifyBoundaryExpansion`. When nil, the classifier
    ///     result's own `startTime` is used. playhead-gtt9.4.1.
    ///   - expandedEndTime: Optional override for the persisted end time. Same
    ///     contract as `expandedStartTime`.
    ///   - eligibilityGate: playhead-gtt9.11 precision-gate stamp. "autoSkip"
    ///     admits the window to `SkipOrchestrator.receiveAdWindows` auto-skip
    ///     path; "markOnly" keeps it visible as a UI marker but blocks
    ///     auto-skip. Nil preserves legacy behavior (no stamp). playhead-bllt:
    ///     callers must pass the label AFTER `HotPathExtentGate.gatedLabel`,
    ///     derived from the SAME `extentSupport` they hand this function.
    ///   - extentSupport: playhead-bllt. The per-edge provenance persisted on
    ///     the row. Defaults to `.unanchored` — the conservative value, and the
    ///     one the hot path genuinely has, since nothing on this path proves an
    ///     edge. It used to be reached by omission (`AdWindow`'s own default);
    ///     naming it makes it a value the caller can pass to the gate rather
    ///     than a fact only the persistence layer knew.
    private func buildAdWindow(
        from result: ClassifierResult,
        boundaryState: AdBoundaryState,
        decisionState: AdDecisionState,
        evidenceText: String?,
        evidenceStartTime: Double?,
        expandedStartTime: Double? = nil,
        expandedEndTime: Double? = nil,
        eligibilityGate: String? = nil,
        catalogMatch: CatalogMatch? = nil,
        catalogStoreMatchSimilarity: Double? = nil,
        extentSupport: SpanExtentSupport = .unanchored
    ) -> AdWindow {
        AdWindow(
            id: UUID().uuidString,
            analysisAssetId: result.analysisAssetId,
            startTime: expandedStartTime ?? result.startTime,
            endTime: expandedEndTime ?? result.endTime,
            confidence: result.adProbability,
            boundaryState: boundaryState.rawValue,
            decisionState: decisionState.rawValue,
            detectorVersion: config.detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: evidenceText,
            evidenceStartTime: evidenceStartTime,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            eligibilityGate: eligibilityGate,
            catalogStoreMatchSimilarity: catalogStoreMatchSimilarity,
            catalogFingerprintVersion: catalogMatch?
                .entry.acousticFingerprint.version.rawValue,
            catalogMatchedEntryId: catalogMatch?.entry.id.uuidString,
            catalogMatchedShowId: catalogMatch?.entry.showId,
            catalogMatchedLearningSource:
                catalogMatch?.entry.learningSource.rawValue,
            catalogMatchedLearningLifecycle:
                catalogMatch?.entry.learningLifecycle.rawValue,
            startEdgeAnchor: extentSupport.startAnchor.rawValue,
            endEdgeAnchor: extentSupport.endAnchor.rawValue
        )
    }

    // MARK: - Precision-gate wiring (playhead-gtt9.11)

    /// playhead-gtt9.11: consult the `AutoSkipPrecisionGate` for a
    /// prospective hot-path AdWindow. Returns the string label to stamp on
    /// `AdWindow.eligibilityGate` ("autoSkip" when the gate admits the
    /// window to auto-skip, "markOnly" when the gate demotes it to UI-only,
    /// nil when the gate says detection-only — in which case callers should
    /// NOT persist a window).
    ///
    /// Inputs fetched here (not passed in) are those the call sites don't
    /// already carry. Keeping the fetch inside this helper avoids threading
    /// the full input surface through every AdWindow construction site.
    ///
    /// - Parameter analysisAssetId: asset for feature-window + correction-
    ///   store queries.
    /// - Parameter startTime: window start time in episode audio seconds.
    /// - Parameter endTime: window end time in episode audio seconds.
    /// - Parameter segmentScore: the confidence value that drives the gate's
    ///   threshold comparison (classifier `adProbability` for single-window,
    ///   `segmentScore` for aggregator).
    /// - Parameter lexicalCategories: union of lexical-pattern categories
    ///   associated with any evidence seeding this window. Aggregator path
    ///   passes an empty set when Tier 1 alone drove the segment (no
    ///   lexical evidence exists in that case — this is honest signal
    ///   absence, not a stub). Single-window path passes
    ///   `LexicalCandidate.categories` from the seeding candidate.
    /// - Returns: `"autoSkip"`, `"markOnly"`, or `nil`.
    private func precisionGateLabel(
        analysisAssetId: String,
        startTime: Double,
        endTime: Double,
        segmentScore: Double,
        lexicalCategories: Set<LexicalPatternCategory>,
        podcastId: String?
    ) async -> PrecisionGateResult {
        let overlappingFeatureWindows: [FeatureWindow]
        do {
            overlappingFeatureWindows = try await store.fetchFeatureWindows(
                assetId: analysisAssetId,
                from: startTime,
                to: endTime
            )
        } catch {
            logger.warning("precisionGateLabel: fetchFeatureWindows failed (continuing with empty features): \(error.localizedDescription)")
            overlappingFeatureWindows = []
        }

        // correctionStore is optional and only present once PlayheadRuntime
        // installs it post-init. Absence → factor 1.0, which
        // disables the userConfirmedLocalPattern safety signal for this
        // window. This is honest: without a correction store we genuinely
        // have no user-confirmation evidence.
        //
        // playhead-rfu-sad: scope the boost to the span being evaluated.
        // Asset-wide `correctionBoostFactor` would fire
        // `userConfirmedLocalPattern` on every window in the asset once
        // any single span had been corrected, including unrelated
        // segments — defeating the precision-gate purpose. The
        // span-local overload returns > 1.0 only when a false-negative
        // correction overlaps `[startTime, endTime]`.
        let boost: Double
        if let correctionStore {
            boost = await correctionStore.correctionBoostFactor(
                for: analysisAssetId,
                overlapping: startTime,
                endTime: endTime
            )
        } else {
            boost = 1.0
        }

        let gateConfig = AutoSkipPrecisionGateConfig(
            uiCandidateThreshold: config.segmentUICandidateThreshold,
            autoSkipThreshold: config.segmentAutoSkipThreshold,
            typicalAdDuration: GlobalPriorDefaults.standard.typicalAdDuration,
            minMusicBedCoverage: AutoSkipPrecisionGateConfig.default.minMusicBedCoverage,
            slotFraction: AutoSkipPrecisionGateConfig.default.slotFraction
        )

        // playhead-2m2i / playhead-o4qr: query the catalog so
        // `SafetySignal.catalogMatch` is available as diagnostic context.
        // Catalog evidence is deliberately excluded from automatic authority;
        // the service still requires a strong independent corroborator. Prior
        // to the original wiring bead, `precisionGateLabel` constructed
        // `AutoSkipPrecisionGateInput` without `catalogMatchSimilarity`,
        // so the field defaulted to 0 and the catalog signal could never
        // fire from the hot path — even when the catalog had a real
        // fingerprint match for the span. Reusing
        // `overlappingFeatureWindows` (already fetched above) means the
        // catalog fingerprint is built from the same feature-window slice
        // the gate uses for the acoustic safety signal — no second
        // `fetchFeatureWindows` round trip. The catalog DB query itself
        // (SQLite) still runs, but only when a catalog store is wired.
        // Optional match arrays preserve the telemetry distinction: nil when
        // the store/show/fingerprint/query is unavailable, an empty array for
        // an observed exact-show miss, and a populated array for a match.
        //
        let normalizedShowId =
            RecurrenceMaterialIdentity.canonicalIdentifier(podcastId)
        let catalogMatches: [CatalogMatch]?
        if let adCatalogStore,
           let normalizedShowId,
           !overlappingFeatureWindows.isEmpty {
            let fingerprint = AcousticFingerprint.fromFeatureWindows(overlappingFeatureWindows)
            if fingerprint.isZero {
                catalogMatches = nil
            } else {
                catalogMatches = await adCatalogStore.matchesIfAvailable(
                    fingerprint: fingerprint,
                    show: normalizedShowId
                )
            }
        } else {
            catalogMatches = nil
        }
        let catalogMatch = catalogMatches?.first
        let catalogMatchSimilarity = catalogMatch?.similarity ?? 0
        let persistedCatalogMatchSimilarity = catalogMatches.map {
            Double($0.first?.similarity ?? 0)
        }

        // playhead-gtt9.26: Calibrate the post-fusion classifier score
        // before it enters the gate. Cold-start (`.production` ships
        // empty) returns `.identity` so the calibrated score equals the
        // raw score and behaviour is byte-identical to pre-gtt9.26.
        // Once a fit is baked in for the active
        // (detectorVersion, buildCommitSHA), the calibrated score
        // replaces the raw score everywhere the gate compares against
        // its thresholds.
        let calibrator = classifierCalibrationProfile.calibrator(
            detectorVersion: config.detectorVersion,
            buildCommitSHA: BuildInfo.commitSHA
        )
        let calibratedScore = calibrator.calibrate(segmentScore)

        let input = AutoSkipPrecisionGateInput(
            analysisAssetId: analysisAssetId,
            segmentStartTime: startTime,
            segmentEndTime: endTime,
            segmentScore: calibratedScore,
            episodeDuration: episodeDuration,
            overlappingFeatureWindows: overlappingFeatureWindows,
            lexicalCategories: lexicalCategories,
            userCorrectionBoostFactor: boost,
            catalogMatchSimilarity: catalogMatchSimilarity
        )

        switch AutoSkipPrecisionGate.classify(input: input, config: gateConfig) {
        case .detectionOnly:
            return PrecisionGateResult(
                label: nil,
                catalogMatch: catalogMatch,
                catalogStoreMatchSimilarity:
                    persistedCatalogMatchSimilarity
            )
        case .uiCandidate:
            return PrecisionGateResult(
                label: "markOnly",
                catalogMatch: catalogMatch,
                catalogStoreMatchSimilarity:
                    persistedCatalogMatchSimilarity
            )
        case .autoSkipEligible(let firedSignals):
            // playhead-9ro7 (cycle-2 follow-up): metadataSlotPrior fires by
            // construction whenever the segment center is in the first/last
            // `slotFraction` of the episode. The pure gate admits any
            // non-empty signal set so it preserves its single-knob
            // contract (see `AutoSkipPrecisionGateTests
            // .autoSkipAdmittedBySlotPriorPreRoll`), but the service-layer
            // policy is stricter: a slot-only autoSkip is just "score in
            // the slot," with no independent corroboration. Demote those
            // to mark-only at the helper so both the single-window path
            // (line ~1345) and the aggregator path (line ~4683) inherit
            // the rule without bespoke per-call-site overrides.
            //
            // Modeled as an explicit "no strong corroborator" check rather
            // than `firedSignals == [.metadataSlotPrior]` set-equality.
            // The set-equality form silently re-admits the moment a
            // second weak signal is introduced; the inclusion-list form
            // forces any future SafetySignal author to decide whether
            // their case counts as independent corroboration.
            let strongCorroborators: Set<SafetySignal> = [
                .strongLexicalAdPhrase,
                .sustainedAcousticAdSignature,
                .userConfirmedLocalPattern,
            ]
            if firedSignals.isDisjoint(with: strongCorroborators) {
                return PrecisionGateResult(
                    label: "markOnly",
                    catalogMatch: catalogMatch,
                    catalogStoreMatchSimilarity:
                        persistedCatalogMatchSimilarity
                )
            }
            return PrecisionGateResult(
                label: "autoSkip",
                catalogMatch: catalogMatch,
                catalogStoreMatchSimilarity:
                    persistedCatalogMatchSimilarity
            )
        }
    }

    /// playhead-gtt9.4.1: compute the expanded persisted window extents for a
    /// classifier result. Fetches a wider-radius feature-window slice than the
    /// classifier used (±60 s via `BoundaryExpander.ExpansionConfig.neutral`)
    /// and delegates to `PostClassifyBoundaryExpansion.expand`.
    ///
    /// No-ops (returns the result's own extents) when the expansion
    /// preconditions inside the helper do not hold — keeps the extra DB fetch
    /// to high-confidence candidates via an up-front guard.
    private func expandedBounds(
        for result: ClassifierResult,
        analysisAssetId: String
    ) async -> (startTime: Double, endTime: Double) {
        // Up-front guard: only pay the extra feature-window fetch cost for
        // candidates that might actually expand (confidence ≥ autoSkip).
        guard result.adProbability >= config.autoSkipConfidenceThreshold else {
            return (result.startTime, result.endTime)
        }

        let typicalAdDuration = GlobalPriorDefaults.standard.typicalAdDuration
        let shortCandidateThreshold = typicalAdDuration.lowerBound / 2.0
        guard (result.endTime - result.startTime) < shortCandidateThreshold else {
            return (result.startTime, result.endTime)
        }

        let expansionConfig = BoundaryExpander.ExpansionConfig.neutral
        let expandedFrom = max(0, result.startTime - expansionConfig.acousticBackwardSearchRadius)
        let expandedTo = result.endTime + expansionConfig.acousticForwardSearchRadius

        let featureWindows: [FeatureWindow]
        do {
            featureWindows = try await store.fetchFeatureWindows(
                assetId: analysisAssetId,
                from: expandedFrom,
                to: expandedTo
            )
        } catch {
            logger.warning("PostClassifyBoundaryExpansion: feature fetch failed, keeping original extents: \(error.localizedDescription)")
            return (result.startTime, result.endTime)
        }

        return PostClassifyBoundaryExpansion.expand(
            startTime: result.startTime,
            endTime: result.endTime,
            adProbability: result.adProbability,
            featureWindows: featureWindows,
            autoSkipConfidenceThreshold: config.autoSkipConfidenceThreshold,
            typicalAdDuration: typicalAdDuration
        )
    }

    // MARK: - Metadata Extraction

    /// Extract metadata for a non-suppressed window (confirmed or candidate) and persist to SQLite.
    private func extractAndPersistMetadata(
        window: AdWindow,
        chunks: [TranscriptChunk]
    ) async {
        // Skip if metadata is already current.
        if !MetadataExtractorFactory.needsReExtraction(
            currentPromptVersion: window.metadataPromptVersion,
            currentSource: window.metadataSource
        ) { return }

        // Gather transcript text overlapping this window.
        let overlappingText = chunks
            .filter { $0.startTime < window.endTime && $0.endTime > window.startTime }
            .map(\.text)
            .joined(separator: " ")

        guard !overlappingText.isEmpty else { return }

        do {
            guard let metadata = try await metadataExtractor.extract(
                evidenceText: overlappingText,
                windowStartTime: window.startTime,
                windowEndTime: window.endTime
            ) else { return }

            let persisted = try await store.updateAdWindowMetadataIfCurrent(
                expectedProducerRevision: window,
                advertiser: metadata.advertiser,
                product: metadata.product,
                evidenceText: metadata.evidenceText,
                metadataSource: metadata.source,
                metadataConfidence: metadata.confidence,
                metadataPromptVersion: metadata.promptVersion
            )
            if !persisted {
                logger.debug(
                    "Metadata extraction result ignored for stale window \(window.id, privacy: .public)"
                )
            }
        } catch {
            logger.warning("Metadata extraction failed for window \(window.id): \(error.localizedDescription)")
        }
    }

    // MARK: - Prior Hierarchy Resolution (playhead-084j)

    /// Resolve the 4-level prior hierarchy for the episode of `podcastId`.
    ///
    /// Called from `runBackfill` exactly once per episode (outside the
    /// per-span loop). The result feeds `DurationPrior(resolvedPriors:)` so
    /// fusion is show-aware, not stuck on `GlobalPriorDefaults.standard`.
    ///
    /// **`podcastId` is the request's show and is not optional-by-default
    /// (playhead-2kxd).** It used to read a `currentPodcastProfile` slot, so
    /// the priors an episode was fused against were the last show whose
    /// backfill finished — not necessarily this one. `nil`/empty means "this
    /// caller has no show identity" (the hot path's `podcastId: String?`), and
    /// resolves to global defaults rather than to somebody else's priors.
    ///
    /// Audit (as of spxs):
    ///   • Global: always `GlobalPriorDefaults.standard`.
    ///   • Network: derived via `NetworkPriorsBuilder.build` from all
    ///     `PodcastProfile` rows that share THIS show's `networkId`.
    ///     `nil` when the show has no `networkId` recorded yet
    ///     (RSS-metadata writer lands in a follow-up bead) or when no
    ///     sibling shows in the network meet the per-show sample-count
    ///     threshold. The network fetch is `async` because of the SQL hop;
    ///     the `await` is a real reentrancy point on this actor, NOT an
    ///     atomicity guarantee — see the snapshot note below.
    ///   • Trait: this show's `traitProfile`, or `.unknown`. The persistence
    ///     layer reads cleanly; profiles without a writer fall through to
    ///     `.unknown`, which is non-reliable, so the trait level stays
    ///     inactive — graceful degradation.
    ///   • Show-local: derived from this show's `adDurationStatsJSON`
    ///     via `ShowLocalPriorsBuilder.build`. Returns nil for shows below
    ///     `ShowLocalPriorsBuilder.minSampleCount`, which keeps the resolver
    ///     at global defaults until enough confirmed ads have been observed.
    ///
    /// Failure semantics: this method does not throw. Corrupt JSON, missing
    /// columns, network-fetch errors, or any other malformed input results
    /// in `nil` priors at that tier, and the resolver falls through to the
    /// next tier (graceful degradation).
    ///
    /// Snapshot consistency: the `await` on `store.fetchProfiles(forNetworkId:)`
    /// can interleave with other turns on this actor, so this method does
    /// NOT execute as a single uninterrupted actor turn. It resolves the
    /// profile ONCE into an immutable local up front, so every tier below is
    /// computed from one value — a stronger statement than cycle-5 L-2's
    /// two-field snapshot, and one that no longer depends on a future editor
    /// noticing which fields had to be lifted. Tests pin it via
    /// `resolveEpisodePriorsSnapshotsObservationCountPreAwait` (cycle-6)
    /// and the `mutateProfile`-style write-side canaries in
    /// `AdDetectionServiceUpdatePriorsAtomicityCanaryTests`.
    private func resolveEpisodePriors(podcastId: String?) async -> ResolvedPriors {
        let episodeProfile = cachedPodcastProfile(forShowId: podcastId)
        let traitProfile = episodeProfile?.traitProfile ?? .unknown
        let showLocal = ShowLocalPriorsBuilder.build(from: episodeProfile)

        // playhead-spxs: gather the network tier. Skipped when this show has
        // no networkId recorded — falls back to nil which the resolver treats
        // as "tier inactive". Errors in the SQL fetch are also treated as
        // "tier inactive" (logged then dropped) so a transient persistence
        // failure can't block ad detection.
        //
        // cycle-5 L-2 snapshotted `networkId` AND `observationCount` before
        // the `fetchProfiles` await, because the await is a real reentrancy
        // point and an interleaving `updatePriors` turn could rewrite the
        // profile slot between the two reads. playhead-2kxd subsumes that:
        // `episodeProfile` is a `let` resolved once, so there is no slot left
        // to be rewritten and every tier above and below reads one value.
        var networkPriors: NetworkPriors? = nil
        var networkDecay: Float = 0
        if let snapshotProfile = episodeProfile,
           let networkId = snapshotProfile.networkId,
           !networkId.isEmpty {
            let observedAtSnapshot = snapshotProfile.observationCount
            do {
                let siblings = try await store.fetchProfiles(forNetworkId: networkId)
                if let priors = NetworkPriorsBuilder.build(from: siblings) {
                    networkPriors = priors
                    networkDecay = NetworkPriors.decayedWeight(episodesObserved: observedAtSnapshot)
                }
            } catch {
                logger.warning("Failed to fetch network siblings for networkId=\(networkId): \(error.localizedDescription)")
            }
        }

        return PriorHierarchyResolver.resolve(
            globalDefaults: .standard,
            networkPriors: networkPriors,
            networkDecay: networkDecay,
            traitProfile: traitProfile,
            showLocalPriors: showLocal
        )
    }

    #if DEBUG
    /// Test-only entry point that mirrors the resolver call inside
    /// `runBackfill` without requiring a full backfill setup. Locks the
    /// invariant that the production wire-up uses the in-actor profile and
    /// produces a `ResolvedPriors` matching what the fusion path consumes.
    ///
    /// `#if DEBUG` matches the existing pattern for other `*ForTesting`
    /// entry points in this file (see `acousticFunnelForTesting` etc.).
    ///
    /// playhead-2kxd: `podcastId` has NO DEFAULT, for the same reason
    /// `updatePriorsForTesting`'s `countsAsEpisodeObservation` has none — a
    /// default would let a test silently ask "the current show", which is
    /// exactly the question this bead removed from the production API.
    func resolveEpisodePriorsForTesting(podcastId: String?) async -> ResolvedPriors {
        await resolveEpisodePriors(podcastId: podcastId)
    }

    /// Test-only entry point that drives `updatePriors` end-to-end.
    /// Locks the wire-up of `adDurationStatsJSON` accumulation through the
    /// actual create / update closures inside `mutateProfile` — the same
    /// path `runBackfill` exercises post-fusion.
    ///
    /// cycle-1 L4: accepts `featureWindows` and `chunks` so tests can drive
    /// the trait-snapshot derivations with realistic signal (rather than
    /// defaulting to empty arrays, which collapse every snapshot to the
    /// no-signal neutral defaults and never exercise the real producer
    /// math). No defaults — callers without a real signal pass `[]`
    /// explicitly so the choice is visible at the call site.
    ///
    /// playhead-mn5e/2qz6: this shim drives the PRIORS MERGE only. It does not
    /// move `observationCount`. Exactly two things do — this file's
    /// `recordConfirmedWindowObservation` (reachable end-to-end through
    /// `runBackfill`) and `TrustScoringService.recordCorrectObservation` (the
    /// banner Yes) — and playhead-fh5v put BOTH behind the same
    /// `trust_episode_observations` claim, so they cannot double-count one
    /// episode between them.
    ///
    /// playhead-g7ln: `countsAsEpisodeObservation` has NO default, for the same
    /// reason `featureWindows`/`chunks` have none (cycle-1 L3). A default of
    /// `true` would let a future caller silently restore the per-backfill unit
    /// this bead removed, and a default of `false` would make every trait test
    /// quietly assert nothing. The production call site passes the
    /// `trust_episode_observations` claim result; a test states which case it
    /// is testing.
    func updatePriorsForTesting(
        podcastId: String,
        nonSuppressedWindows: [AdWindow],
        episodeDuration: Double,
        featureWindows: [FeatureWindow],
        chunks: [TranscriptChunk],
        countsAsEpisodeObservation: Bool
    ) async throws {
        try await updatePriors(
            podcastId: podcastId,
            nonSuppressedWindows: nonSuppressedWindows,
            episodeDuration: episodeDuration,
            featureWindows: featureWindows,
            chunks: chunks,
            countsAsEpisodeObservation: countsAsEpisodeObservation
        )
    }
    #endif

    // MARK: - Trust Observation (playhead-mn5e)

    /// Record this backfill's ad windows as a trust observation, running the
    /// promotion ladder. **This is the production caller
    /// `TrustScoringService.recordSuccessfulObservation` never had.**
    ///
    /// **It is one of the two writers of `observationCount`, and both go
    /// through the same claim** (playhead-2qz6 / playhead-fh5v). The other is
    /// `TrustScoringService.recordCorrectObservation`, the banner Yes — a
    /// stronger witness than this one, so it is allowed to be the FIRST to
    /// claim an episode, and it counts nothing on an episode already claimed
    /// here. Before this, the counter was incremented once per
    /// completed backfill and an episode is backfilled ~9 times — measured 36
    /// observations across 4 assets on the 2026-08-12 device pull — so
    /// `shadowToManualObservations: 3` meant "a third of one episode". Every
    /// consumer already reads the column as episodes in so many words:
    /// `ShowLocalPriorsBuilder` calls it "number of episodes processed" and
    /// `NetworkPriors.decayedWeight` names its parameter `episodesObserved`.
    ///
    /// The unit event is "a backfill of THIS asset confirmed at least one ad
    /// window, and this asset has not contributed an observation for this show
    /// before". The dedupe key is `(podcastId, analysisAssetId)` and it lives
    /// in `trust_episode_observations`, claimed atomically — see
    /// `AnalysisStore.claimEpisodeTrustObservation` for why the claim is a row
    /// rather than a flag derived from a coverage watermark or a lifecycle
    /// state.
    ///
    /// ORDER MATTERS AND IS NOT ARBITRARY: the claim is taken only when the
    /// observation is about to be recorded. A backfill that cannot record one
    /// (no trust service) claims nothing, so the episode stays claimable and a
    /// later backfill counts it. A claim standing for an observation nobody
    /// made would cost the show that episode's credit permanently.
    ///
    /// **WHAT THE EVIDENCE IS, and why this one.** Two candidates were on the
    /// table: "this episode's ad windows were confirmed" and "the user did not
    /// veto them". This implements the FIRST.
    ///
    /// The second is a value that names an ABSENCE, and it cannot tell
    /// "no veto because the detections were right" from "no veto because
    /// nobody ever pressed play". On the device that motivated this bead those
    /// are not hypothetical alternatives — 4 episodes analysed, ZERO played,
    /// ZERO correction events — so a listen-gated rule promotes nothing at all
    /// and a downloaded-but-unplayed library stays in shadow forever. The
    /// first is a positive fact available at the moment the pipeline finishes.
    ///
    /// **WHAT THAT COSTS, stated rather than hidden.** Confirmed windows are
    /// the detector's own output, so this is self-certification: the show is
    /// promoted on evidence it produced itself. **playhead-lqcp is what
    /// actually bounds that: `manual` -> `auto` DOES NOT FIRE AT ALL** while
    /// `AutoPromotionConfidenceEvidence` has only its `.unavailable` case, so
    /// the highest rung self-observation can reach is `manual`, which skips
    /// nothing without a tap. That closure exists precisely
    /// because this method un-freezes `skipTrustScore` and would otherwise have
    /// carried a fresh subscription to auto at episode 8 with no gesture at
    /// all. One further bound is unchanged: the demotion path stays entirely
    /// user-driven and asymmetric (`recordFalseSkipSignal`, two vetoes demote
    /// `auto` -> `manual`).
    ///
    /// **R6 struck two claims that used to sit here.** "…`manual`, which
    /// unlocks a BANNER" was false — `.shadow` and `.manual` are behaviourally
    /// identical at `SkipOrchestrator.evaluateWindow`, and the suggest banner
    /// is not mode-gated, so `manual` unlocks nothing (see the R6 note on
    /// `AutoPromotionConfidenceEvidence`). And "the legacy auto clauses still
    /// required `recentFalseSkipSignals == 0`, which only a banner Yes can
    /// restore" was written in the present tense about clauses playhead-lqcp
    /// DELETED two paragraphs above; there are no auto clauses left to require
    /// anything.
    ///
    /// **AN EPISODE THAT CONFIRMED NOTHING IS NOT EVIDENCE.** An empty window
    /// set is not a clean observation, it is the absence of an observation:
    /// counting it would let a show whose detection found nothing at all climb
    /// the ladder on exactly the quantity a broken detector produces. The guard
    /// below is what keeps "confirmed ad windows" from reading the same as
    /// "detection never worked here".
    ///
    /// **ORDERING.** Called BEFORE `updatePriors` so that (a) a brand-new
    /// profile is created by the trust owner rather than by the priors merge,
    /// and (b) `updatePriors` — which runs last and refreshes this show's
    /// entry in `podcastProfilesByShowId` (and `showPriors`) — reads the
    /// POST-promotion row, so the in-memory copy can never disagree with the
    /// persisted mode.
    @discardableResult
    private func recordConfirmedWindowObservation(
        podcastId: String,
        analysisAssetId: String,
        confirmedWindows: [AdWindow]
    ) async -> Bool {
        guard let trustScoringService else {
            // Not a silent drop: without the service the promotion ladder
            // cannot run at all this launch, which is the exact failure this
            // bead exists to make impossible. No claim is taken, so this
            // episode can still be counted by a later backfill.
            logger.warning(
                "[mn5e] No TrustScoringService installed — trust observation dropped for podcast \(podcastId, privacy: .public); promotion cannot run"
            )
            return false
        }
        guard !confirmedWindows.isEmpty else {
            logger.info(
                "[mn5e] Backfill confirmed no ad windows for podcast \(podcastId, privacy: .public) — no trust observation recorded"
            )
            return false
        }
        // playhead-2qz6: one observation per EPISODE. This is the dedupe, and
        // it is durable — a re-drive after relaunch, a transcript-version bump
        // and the 15d0 resume path all re-enter here for an asset already
        // claimed, and all of them must be no-ops.
        let claimed: Bool
        do {
            claimed = try await store.claimEpisodeTrustObservation(
                podcastId: podcastId,
                analysisAssetId: analysisAssetId
            )
        } catch {
            logger.warning(
                "[mn5e] Could not claim the episode trust observation for asset \(analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public); not counting it"
            )
            return false
        }
        guard claimed else {
            logger.info(
                "[mn5e] Asset \(analysisAssetId, privacy: .public) already contributed its trust observation for podcast \(podcastId, privacy: .public) — not counting it again"
            )
            return false
        }
        // DETECTION confidence, not actuation: `recordSuccessfulObservation`
        // asks "how strongly did the evidence say these were ads on this
        // show", which is exactly what `AdWindow.confidence` carries (see its
        // playhead-ar60 doc comment). `skipConfidence` is the actuation
        // number and would fold the user-correction factor into a quantity
        // that is supposed to describe the DETECTOR.
        let averageConfidence =
            confirmedWindows.reduce(0.0) { $0 + $1.confidence }
            / Double(confirmedWindows.count)
        // R4: WHICH classes drew these windows, not just how confident they
        // were. `SkipOrchestrator`'s gate reads a PER-CLASS mode, and once any
        // attributed user gesture has forked the ledger the show scalar is not
        // what it reads — so an observation that names no class promotes
        // nothing the listener can feel. Credit follows the same rule blame
        // does: only the classes that actually produced this episode's
        // evidence. Read from the rows' own columns (`AdWindow.detectorClass`);
        // this service has no ingest anchor stamp to prefer, which is what that
        // property is documented as the fallback for.
        let observedDetectors = Set(confirmedWindows.map(\.detectorClass))
        await trustScoringService.recordSuccessfulObservation(
            podcastId: podcastId,
            averageConfidence: averageConfidence,
            detectors: observedDetectors
        )
        return true
    }

    // MARK: - Prior Updates

    /// Update PodcastProfile priors from confirmed ad windows.
    /// Learns ad slot positions and sponsor names over time.
    ///
    /// skeptical-review-cycle-15 M-1 / M-2: routed through
    /// `store.mutateProfile` so the read-modify-write happens inside one
    /// AnalysisStore actor turn. Two earlier defects this closes:
    ///
    ///   • M-1 (lost-update race): the previous body did `await
    ///     store.fetchProfile()` then `await store.upsertProfile()` as
    ///     two independent actor hops. A concurrent
    ///     `TrustScoringService.recordFalseSkipSignal` (which is itself
    ///     atomic via `updateProfileIfExistsCapturing`) landing between
    ///     those hops would be silently overwritten by the carry-forward
    ///     upsert below. Post-q45f the writer policy is single-source:
    ///     TrustScoringService is the sole writer of `skipTrustScore`
    ///     (via `recordSuccessfulObservation`, `recordFalseSkipSignal`,
    ///     and the new `recordWeakFalseSkipSignal` that listen-rewind
    ///     now delegates to). `updatePriors` itself does not write
    ///     `skipTrustScore` — it only carries the existing value
    ///     through, so any concurrent decrement must remain visible
    ///     after this update commits.
    ///
    ///   • M-2 (traitProfileJSON clobber): the previous
    ///     `PodcastProfile(...)` constructor here omitted
    ///     `traitProfileJSON`. The default initializer parameter is
    ///     `nil`, and `upsertProfile`'s SQL writes
    ///     `traitProfileJSON = excluded.traitProfileJSON` (NOT
    ///     COALESCE), so every priors update silently nilled the
    ///     persisted trait profile. The `update` closure below
    ///     explicitly carries `existing.traitProfileJSON` forward.
    ///     (`title` was COALESCE-safe but is also passed through for
    ///     symmetry / defensiveness.)
    /// cycle-1 L3: `featureWindows` and `chunks` are REQUIRED — no
    /// defaults. The single production caller (`runBackfill`) always has
    /// the full signal vector in scope and must thread it through; the
    /// `updatePriorsForTesting` shim explicitly forwards them. Defaults
    /// of `[]` previously made it possible for a future refactor to
    /// introduce a new caller that silently dropped the signal and
    /// regressed the trait-tier activation back to the no-signal neutral
    /// defaults — invisible to most behavioral tests because the EMA
    /// path still increments `episodesObserved`. Required parameters
    /// surface the choice at the call site.
    ///
    /// playhead-mn5e / playhead-2qz6: **this method no longer writes
    /// `observationCount` at all** — it carries the stored value through, the
    /// way it has always carried `skipTrustScore` through. It used to be the
    /// only production incrementer, ticking once per completed backfill, and
    /// an episode is backfilled ~9 times; that is what made
    /// `shadowToManualObservations: 3` mean "a third of one episode". The
    /// counter now belongs to `recordConfirmedWindowObservation`, which claims
    /// one observation per EPISODE through `trust_episode_observations` and
    /// increments inside the same atomic mutate that evaluates promotion.
    ///
    /// Deliberately UNCONDITIONAL, with no fallback flag: a path that cannot
    /// record an episode observation must not write a per-backfill number into
    /// a column whose consumers all read it as episodes. When no trust service
    /// is installed nothing is counted and no claim is taken, so the episode
    /// stays claimable and the next backfill counts it properly.
    ///
    /// playhead-g7ln: **the TRAIT profile is now behind the same claim.**
    /// `ShowTraitProfile.episodesObserved` is documented "number of episodes
    /// that have contributed to this profile" and was incremented by
    /// `ShowTraitProfile.updated(from:)` on every call of this method — i.e.
    /// once per BACKFILL, the identical unit defect playhead-2qz6 fixed one
    /// column over. Measured on the 2026-08-18 t3 device pull: `episodesObserved`
    /// read **104 and 56** against **8 and 7** distinct episodes, 13.0x and 8.0x.
    /// `isReliable` (>= 3) and `PriorHierarchyResolver.traitBlendWeight` (0.4 at
    /// 3 ramping to 0.6 at 7+) therefore both saturated INSIDE the first episode.
    ///
    /// `countsAsEpisodeObservation` is the claim result threaded down from
    /// `recordConfirmedWindowObservation`, which is the fact rather than a proxy
    /// for it. When it is `false` the persisted `traitProfileJSON` is carried
    /// forward VERBATIM — no EMA application, no increment — because a
    /// re-backfill of an already-counted asset is not a new episode and ~9
    /// near-identical snapshots of one episode are not an average over nine.
    ///
    /// TWO CONSEQUENCES, stated rather than hidden:
    ///
    ///   * The snapshot that lands is the FIRST pass that confirmed a window,
    ///     not the most complete one. Under the old behaviour the EMA converged
    ///     on the LAST pass (alpha 0.3 over ~13 applications retains ~1 % of
    ///     what preceded it), so the traits recorded for an episode can now
    ///     carry less transcript/feature coverage than they used to. Filed as
    ///     playhead-wgst; not fixable from here, because nothing in
    ///     `runBackfill` knows which pass is the last one.
    ///   * A banner "Yes" (`TrustScoringService.recordCorrectObservation`)
    ///     shares this claim and can take it FIRST (playhead-fh5v). That
    ///     episode's trait snapshot is then never merged, so `episodesObserved`
    ///     can trail `observationCount` by the number of such episodes. It is
    ///     the conservative direction and it is bounded: the tap requires a
    ///     window this pipeline already persisted.
    private func updatePriors(
        podcastId: String,
        nonSuppressedWindows: [AdWindow],
        episodeDuration: Double,
        featureWindows: [FeatureWindow],
        chunks: [TranscriptChunk],
        countsAsEpisodeObservation: Bool
    ) async throws {
        guard !nonSuppressedWindows.isEmpty, episodeDuration > 0 else { return }

        // cycle-1 residual log: surface the trait-snapshot input cardinalities
        // at the point of consumption so an empty-input regression (a future
        // caller that drops the signal vector and silently regresses the
        // trait tier to no-signal neutral defaults) is visible in
        // DiagnosticReports / `log show` queries without a debugger
        // attached. Counts only — no payload content — so privacy-public is
        // safe.
        logger.debug(
            "[traitSnapshot] featureWindows=\(featureWindows.count, privacy: .public) chunks=\(chunks.count, privacy: .public)"
        )

        // Compute normalized ad slot positions from confirmed windows.
        // These do not depend on the existing profile so we compute them
        // once outside the closure (also keeps the closure simple).
        let newSlotPositions = nonSuppressedWindows.map { window in
            let center = (window.startTime + window.endTime) / 2.0
            return center / episodeDuration
        }

        // Collect advertiser names from confirmed windows with metadata.
        let newSponsors = nonSuppressedWindows
            .compactMap(\.advertiser)
            .map { $0.lowercased() }

        // playhead-084j: capture the per-window durations of every confirmed
        // ad in this episode so the show-local `AdDurationStats` aggregate
        // can extend with a streaming-mean update inside the mutate closure.
        // We filter to finite, positive durations to keep the EMA from being
        // perturbed by malformed window rows.
        let newAdDurations: [TimeInterval] = nonSuppressedWindows.compactMap { window in
            let d = window.endTime - window.startTime
            guard d.isFinite, d > 0 else { return nil }
            return d
        }

        let updatedProfile: PodcastProfile
        do {
            updatedProfile = try await store.mutateProfile(
                podcastId: podcastId,
                create: {
                    let initialSlotsJSON: String?
                    if let data = try? JSONEncoder().encode(newSlotPositions) {
                        initialSlotsJSON = String(data: data, encoding: .utf8)
                    } else {
                        initialSlotsJSON = nil
                    }
                    let initialSponsors: String? = newSponsors.isEmpty
                        ? nil
                        : Set(newSponsors).sorted().joined(separator: ",")

                    // playhead-084j: encode the brand-new ad-duration aggregate
                    // from this episode's observations. nil if no usable
                    // durations were captured (the COALESCE in upsertProfile
                    // will then leave the persisted column NULL — which is
                    // also what `ShowLocalPriorsBuilder.build` expects for a
                    // fresh show).
                    let initialAdDurationStatsJSON = Self.encodeAdDurationStats(
                        merging: .empty,
                        with: newAdDurations
                    )

                    // playhead-v7v8: bootstrap the trait profile from this
                    // episode's signal. Without this seed, a fresh show
                    // would walk through `episodesObserved == 1` carrying a
                    // .unknown profile (since the EMA only fires on
                    // `update:` rebases), forcing the resolver back to the
                    // global tier even though we already have one
                    // observation in hand. The first-episode merge below
                    // mirrors `ShowTraitProfile.updated(from:)` for the
                    // sentinel case (replace, don't blend).
                    //
                    // playhead-g7ln: NIL when no episode observation was
                    // claimed, for exactly the reason `observationCount` is 0
                    // three lines down — this create path is reached only when
                    // the trust path did NOT create the profile, so a seed at
                    // `episodesObserved == 1` would credit an episode nothing
                    // witnessed. The next backfill that DOES claim takes the
                    // update path, where a NULL column decodes as `.unknown`
                    // and `updated(from:)` replaces rather than blends — the
                    // same first-episode semantics, one pass later.
                    let initialTraitProfileJSON = countsAsEpisodeObservation
                        ? Self.initialTraitProfileJSON(
                            featureWindows: featureWindows,
                            chunks: chunks,
                            confirmedAdWindows: nonSuppressedWindows,
                            episodeDuration: episodeDuration
                        )
                        : nil

                    // Bug 4a default: brand-new profile gets trust=0.5
                    // (matches `setUserOverride`'s new-profile default).
                    // TrustScoringService.recordSuccessfulObservation
                    // owns subsequent trust adjustments.
                    return PodcastProfile(
                        podcastId: podcastId,
                        sponsorLexicon: initialSponsors,
                        normalizedAdSlotPriors: initialSlotsJSON,
                        repeatedCTAFragments: nil,
                        jingleFingerprints: nil,
                        implicitFalsePositiveCount: 0,
                        skipTrustScore: 0.5,
                        // playhead-mn5e/2qz6: ZERO, not 1. This create path is
                        // only reached when the trust path did NOT create the
                        // profile — i.e. no observation was recorded and no
                        // claim was taken — so crediting an episode here would
                        // count one that nothing witnessed. The next backfill
                        // that does record one takes the claim and increments
                        // from 0.
                        observationCount: 0,
                        mode: "shadow",
                        recentFalseSkipSignals: 0,
                        traitProfileJSON: initialTraitProfileJSON,
                        adDurationStatsJSON: initialAdDurationStatsJSON,
                        // playhead-spxs: brand-new profile starts with
                        // `networkId == nil`. A future bead populates it
                        // from RSS metadata via NetworkIdentityExtractor;
                        // until then the network-priors tier remains a
                        // graceful no-op for first-observation profiles.
                        networkId: nil
                    )
                },
                update: { existing in
                    // Merge slot positions (exponential moving average).
                    let existingSlots: [Double]
                    if let json = existing.normalizedAdSlotPriors,
                       let data = json.data(using: .utf8),
                       let decoded = try? JSONDecoder().decode([Double].self, from: data) {
                        existingSlots = decoded
                    } else {
                        existingSlots = []
                    }
                    let mergedSlots = Self.mergeSlotPositions(
                        existing: existingSlots,
                        new: newSlotPositions
                    )
                    let slotsJSON: String?
                    if let data = try? JSONEncoder().encode(mergedSlots) {
                        slotsJSON = String(data: data, encoding: .utf8)
                    } else {
                        slotsJSON = nil
                    }

                    // Merge sponsor lexicon.
                    let mergedSponsorLexicon: String?
                    if let lex = existing.sponsorLexicon {
                        let existingNames = Set(
                            lex.components(separatedBy: ",")
                                .map { $0.trimmingCharacters(in: .whitespaces).lowercased() }
                                .filter { !$0.isEmpty }
                        )
                        let allNames = existingNames.union(newSponsors)
                        mergedSponsorLexicon = allNames.sorted().joined(separator: ",")
                    } else if !newSponsors.isEmpty {
                        mergedSponsorLexicon = Set(newSponsors).sorted().joined(separator: ",")
                    } else {
                        mergedSponsorLexicon = existing.sponsorLexicon
                    }

                    // playhead-084j: extend the persisted `AdDurationStats`
                    // aggregate with this episode's confirmed-ad durations.
                    // We carry the existing JSON forward when no new
                    // observations were captured AND no decode-able prior
                    // exists, so a `nil` or corrupt prior value is recovered
                    // on the next episode that DOES carry observations.
                    let mergedAdDurationStatsJSON: String? = {
                        let existingStats = Self.decodeAdDurationStats(existing.adDurationStatsJSON)
                            ?? .empty
                        if newAdDurations.isEmpty {
                            // Preserve prior column verbatim so a backfill
                            // with no new ads doesn't blank out history.
                            return existing.adDurationStatsJSON
                        }
                        return Self.encodeAdDurationStats(
                            merging: existingStats,
                            with: newAdDurations
                        )
                    }()

                    // playhead-v7v8: derive the per-episode trait snapshot
                    // from the live signal that flowed into this backfill,
                    // then advance the persisted ShowTraitProfile via the
                    // existing EMA path on `ShowTraitProfile.updated(from:)`.
                    // Done INSIDE the mutate closure so the read-modify-write
                    // is part of the same AnalysisStore actor turn — same
                    // atomicity contract that cycles 15/17 enforced for the
                    // sponsorLexicon/slot priors.
                    //
                    // cycle-2 L1: compute the resolved trait JSON once via
                    // nil-coalescing (`merged ?? existing.traitProfileJSON`)
                    // rather than rebinding `existing` to an alias profile.
                    // The carry-forward is still load-bearing — the
                    // cycle-22 L-5 whole-file canary requires every
                    // PodcastProfile-constructing `existing in` closure to
                    // mention `<ident>.traitProfileJSON` somewhere in its
                    // body, which the `?? existing.traitProfileJSON`
                    // fallback satisfies on both branches.
                    //
                    // playhead-g7ln: the merge runs ONLY when this backfill
                    // claimed the episode. The two `nil` results are kept
                    // DISTINGUISHABLE rather than folded into one
                    // nil-coalescing chain: "no episode to count" and "the
                    // encoder failed" have the same effect on the column and
                    // opposite meanings, and `mergedTraitProfileJSON` logs a
                    // fault for the second. Both branches mention
                    // `existing.traitProfileJSON`, which is what the cycle-22
                    // L-5 whole-file canary requires.
                    let resolvedTraitProfileJSON: String?
                    let mergedTraitProfileJSON: String?
                    if countsAsEpisodeObservation {
                        mergedTraitProfileJSON = Self.mergedTraitProfileJSON(
                            existing: existing,
                            featureWindows: featureWindows,
                            chunks: chunks,
                            confirmedAdWindows: nonSuppressedWindows,
                            episodeDuration: episodeDuration
                        )
                        resolvedTraitProfileJSON = mergedTraitProfileJSON ?? existing.traitProfileJSON
                    } else {
                        mergedTraitProfileJSON = nil
                        resolvedTraitProfileJSON = existing.traitProfileJSON
                    }

                    // cycle-2 M2: in DEBUG, assert that a successful merge
                    // never regresses `episodesObserved`. The EMA path
                    // (`ShowTraitProfile.updated(from:)`) increments by 1
                    // on each call, so any drop signals a serialization or
                    // version-skew bug that would otherwise silently
                    // corrupt the persisted profile.
                    #if DEBUG
                    if let merged = mergedTraitProfileJSON,
                       let mergedData = merged.data(using: .utf8),
                       let mergedProfile = try? JSONDecoder().decode(
                           ShowTraitProfile.self, from: mergedData
                       ) {
                        assert(
                            mergedProfile.episodesObserved >= existing.traitProfile.episodesObserved,
                            "cycle-2 M2: merged traitProfile.episodesObserved (\(mergedProfile.episodesObserved)) regressed below existing (\(existing.traitProfile.episodesObserved)) for podcast \(podcastId)"
                        )
                    }
                    #endif

                    // Bug 4a (trust carry-forward): updatePriors does not
                    // touch `skipTrustScore`. The two writers under the
                    // current policy (C26 H-1, playhead-od4j; see
                    // `recordListenRewind` docstring for the full
                    // contract) are `TrustScoringService` and
                    // `recordListenRewind`; we copy the existing value
                    // through so neither writer's decrement is silently
                    // overwritten by this priors update.
                    return PodcastProfile(
                        podcastId: existing.podcastId,
                        sponsorLexicon: mergedSponsorLexicon,
                        normalizedAdSlotPriors: slotsJSON,
                        repeatedCTAFragments: existing.repeatedCTAFragments,
                        jingleFingerprints: existing.jingleFingerprints,
                        implicitFalsePositiveCount: existing.implicitFalsePositiveCount,
                        skipTrustScore: existing.skipTrustScore,
                        // playhead-mn5e/2qz6: CARRY FORWARD, never increment.
                        // `recordConfirmedWindowObservation` is the sole
                        // writer and it counts episodes, not backfills.
                        observationCount: existing.observationCount,
                        mode: existing.mode,
                        recentFalseSkipSignals: existing.recentFalseSkipSignals,
                        traitProfileJSON: resolvedTraitProfileJSON,
                        title: existing.title,
                        adDurationStatsJSON: mergedAdDurationStatsJSON,
                        // playhead-spxs: carry-forward (mirror of
                        // `adDurationStatsJSON`). `networkId` is
                        // COALESCE-protected in `upsertProfile`, but
                        // explicit carry-forward keeps every constructor
                        // in this file consistent with the canary
                        // contract — see
                        // `AdDetectionServiceUpdatePriorsAtomicityCanaryTests`.
                        networkId: existing.networkId
                    )
                }
            )
        }

        // Refresh the in-memory priors for subsequent use.
        //
        // ⚠️ playhead-2kxd fixed the PROFILE and deliberately did not touch the
        // two lines above it. `showPriors` and `scanner` are the same shape —
        // one slot standing for a set, derived from this same profile — and
        // they still hold whichever show wrote last. That is FILED, not fixed,
        // because their read sites are a different (larger) population; see
        // playhead-jjke. Do not read the keyed write below as covering them.
        showPriors = ShowPriors.from(profile: updatedProfile)
        scanner = LexicalScanner(podcastProfile: updatedProfile)
        // playhead-2kxd: keyed on `updatedProfile.podcastId` — the identity the
        // VALUE carries — so this cannot file one show's profile under
        // another's name even if `podcastId` and the returned row ever diverge.
        cachePodcastProfile(updatedProfile)

        logger.info("Updated priors for podcast \(podcastId): observations=\(updatedProfile.observationCount) trust=\(updatedProfile.skipTrustScore, format: .fixed(precision: 2))")
    }

    // MARK: - Trait profile merge helpers (playhead-v7v8)
    //
    // Both helpers are `private static` so the `store.mutateProfile`
    // closures (which run inside the `AnalysisStore` actor) can call them
    // without capturing `self`. Same constraint and rationale as
    // `mergeSlotPositions` and the `AdDurationStats` helpers below.

    /// Build the trait snapshot for this episode and merge it into the
    /// existing profile's persisted `ShowTraitProfile` via the standard
    /// EMA path (`ShowTraitProfile.updated(from:)`). Returns the encoded
    /// JSON, or `nil` if encoding fails (which leaves the existing
    /// `traitProfileJSON` undisturbed in the carry-forward path).
    ///
    /// Called inside the `update` closure of `mutateProfile`, where the
    /// snapshot is derived from THIS episode's signal but the EMA target
    /// is the stored profile's prior trait state.
    private static func mergedTraitProfileJSON(
        existing: PodcastProfile,
        featureWindows: [FeatureWindow],
        chunks: [TranscriptChunk],
        confirmedAdWindows: [AdWindow],
        episodeDuration: Double
    ) -> String? {
        let snapshot = EpisodeTraitSnapshotBuilder.build(
            featureWindows: featureWindows,
            chunks: chunks,
            confirmedAdWindows: confirmedAdWindows,
            existingProfile: existing,
            episodeDuration: episodeDuration
        )
        let mergedProfile = existing.traitProfile.updated(from: snapshot)
        // cycle-2 M1: surface encode failures so a silent `nil` return
        // (which leaves the existing `traitProfileJSON` undisturbed) is
        // still visible in DiagnosticReports / `log show` queries. The
        // `nil` semantic is preserved because callers depend on it.
        do {
            let data = try JSONEncoder().encode(mergedProfile)
            guard let json = String(data: data, encoding: .utf8) else {
                staticLogger.error(
                    "[traitSnapshot] mergedTraitProfileJSON: utf8 conversion produced no string"
                )
                return nil
            }
            return json
        } catch {
            staticLogger.error(
                "[traitSnapshot] mergedTraitProfileJSON: encode failed: \(error.localizedDescription, privacy: .public)"
            )
            return nil
        }
    }

    /// Build the seed trait snapshot for a brand-new profile and encode
    /// it. The first-episode merge of `ShowTraitProfile.unknown.updated(
    /// from: snapshot)` replaces the sentinel directly (no blending), so
    /// the result is just the snapshot promoted to a one-episode profile.
    private static func initialTraitProfileJSON(
        featureWindows: [FeatureWindow],
        chunks: [TranscriptChunk],
        confirmedAdWindows: [AdWindow],
        episodeDuration: Double
    ) -> String? {
        let snapshot = EpisodeTraitSnapshotBuilder.build(
            featureWindows: featureWindows,
            chunks: chunks,
            confirmedAdWindows: confirmedAdWindows,
            existingProfile: nil,
            episodeDuration: episodeDuration
        )
        let seedProfile = ShowTraitProfile.unknown.updated(from: snapshot)
        // cycle-2 M1: surface encode failures so the bootstrap-skip path
        // (column persisted as nil) is observable in DiagnosticReports
        // rather than disappearing silently.
        do {
            let data = try JSONEncoder().encode(seedProfile)
            guard let json = String(data: data, encoding: .utf8) else {
                staticLogger.error(
                    "[traitSnapshot] initialTraitProfileJSON: utf8 conversion produced no string"
                )
                return nil
            }
            return json
        } catch {
            staticLogger.error(
                "[traitSnapshot] initialTraitProfileJSON: encode failed: \(error.localizedDescription, privacy: .public)"
            )
            return nil
        }
    }

    /// Merge new slot positions with existing ones. Deduplicates slots that
    /// are within 5% of each other (same ad slot across episodes).
    ///
    /// skeptical-review-cycle-15 M-1: declared `static` so the
    /// `store.mutateProfile` closure (which runs inside the AnalysisStore
    /// actor) can call it without capturing `self`.
    private static func mergeSlotPositions(
        existing: [Double],
        new: [Double]
    ) -> [Double] {
        let proximityThreshold = 0.05
        var merged = existing

        for newSlot in new {
            let alreadyExists = merged.contains { abs($0 - newSlot) < proximityThreshold }
            if !alreadyExists {
                merged.append(newSlot)
            } else {
                // Nudge existing toward the new observation (EMA with alpha=0.3).
                merged = merged.map { existing in
                    if abs(existing - newSlot) < proximityThreshold {
                        return existing * 0.7 + newSlot * 0.3
                    }
                    return existing
                }
            }
        }

        return merged.sorted()
    }

    // playhead-084j: declared `private static` so the `store.mutateProfile`
    // closures can call them without capturing `self` (the closures run
    // inside the AnalysisStore actor, not the AdDetectionService actor —
    // same constraint as `mergeSlotPositions` above). `private` matches
    // `mergeSlotPositions`'s access level so neither helper leaks to the
    // app target's public surface.

    /// Decode a persisted `AdDurationStatsJSON` value into the typed struct,
    /// or `nil` when the column is empty / malformed.
    ///
    /// cycle-1 M2: a malformed payload silently returns `nil`, which lets
    /// the resolver fall through to global defaults — but the corrupt
    /// JSON stays on the column and every backfill thereafter pays the
    /// same decode cost without ever surfacing the corruption to a
    /// diagnostic report. Log decode failures at `.error` so the corruption
    /// is visible in DiagnosticReports / `log show` queries. Empty/nil
    /// payloads are NOT logged (the column is unset for any new show
    /// and that is the expected steady state, not a failure).
    private static func decodeAdDurationStats(_ json: String?) -> AdDurationStats? {
        guard let json, !json.isEmpty else { return nil }
        guard let data = json.data(using: .utf8) else {
            staticLogger.error(
                "[AdDurationStats] decode failed: utf8 conversion produced no data"
            )
            return nil
        }
        do {
            return try JSONDecoder().decode(AdDurationStats.self, from: data)
        } catch {
            staticLogger.error(
                "[AdDurationStats] decode failed: \(error.localizedDescription, privacy: .public)"
            )
            return nil
        }
    }

    /// Encode a freshly-merged `AdDurationStats` for persistence, or `nil`
    /// when no new observations would change the aggregate AND the existing
    /// aggregate is empty (so we don't write `{"meanDuration":0,"sampleCount":0}`).
    ///
    /// cycle-1 L4: uses the default `JSONEncoder` formatting (compact,
    /// no pretty-printing). The column footprint is intentionally
    /// minimal — two scalar fields — and pretty-printed output would
    /// only add bytes without benefiting any consumer (the value is
    /// never inspected by humans through the column). DiagnosticsExport
    /// pretty-prints separately at export time when readability matters.
    private static func encodeAdDurationStats(
        merging existing: AdDurationStats,
        with newDurations: [TimeInterval]
    ) -> String? {
        let merged = ShowLocalPriorsBuilder.mergeDurations(
            existing: existing,
            newDurations: newDurations
        )
        guard merged.sampleCount > 0 else { return nil }
        guard let data = try? JSONEncoder().encode(merged),
              let json = String(data: data, encoding: .utf8)
        else { return nil }
        return json
    }
}

// MARK: - AdDetectionProviding Conformance

extension AdDetectionService: AdDetectionProviding {}

// MARK: - PostClassifyBoundaryExpansion (playhead-gtt9.4.1)

/// Stateless helper that widens an AdWindow's persisted [startTime, endTime]
/// when the classifier produced a high-confidence hit on a narrow
/// LexicalCandidate (2-s window) inside a wider ad envelope.
///
/// Context (2026-04-24 Conan 71F0C2AE regression):
/// the classifier only scores on LexicalCandidate windows (2 s wide), so a
/// GT ad span of 30 s that matches only a single lexical hit gets persisted
/// as a 2-s AdWindow against the 30-s truth — Sec-F1 caps around 0.04.
///
/// Surface fix: when the classifier clears `autoSkipConfidenceThreshold`
/// (0.80 by default) on a candidate whose duration is shorter than
/// `typicalAdDuration.lowerBound / 2` (15 s by default), look for acoustic
/// breaks within `BoundaryExpander.ExpansionConfig.neutral` radii and expand
/// the persisted extents outward to them. Fallback to a `typicalAdDuration`-
/// wide extent centered on the candidate midpoint when no breaks are found.
///
/// Does NOT:
/// - rescore the expanded span
/// - change the `adProbability` attached to the AdWindow
/// - modify the classifier candidate's own boundaries (that's a different
///   layer — `BoundaryRefiner`)
/// - touch the evidence ledger
///
/// Downstream window reconciliation (`reconcileHotPathWindows`) already merges
/// overlapping windows, so independent expansion of adjacent high-confidence
/// candidates is safe — overlaps collapse at persistence time.
enum PostClassifyBoundaryExpansion {

    /// Expand the persisted window extents for a high-confidence, narrow
    /// classifier hit. Returns the original extents unchanged when the
    /// expansion preconditions do not hold.
    ///
    /// - Parameters:
    ///   - startTime: Classifier result start time (seconds).
    ///   - endTime: Classifier result end time (seconds).
    ///   - adProbability: Classifier ad probability.
    ///   - featureWindows: Acoustic feature windows in the vicinity of the
    ///     classifier hit. Expansion searches for AcousticBreaks within these.
    ///   - autoSkipConfidenceThreshold: Confidence threshold above which a
    ///     candidate is eligible for expansion (default 0.80 per
    ///     `AdDetectionConfig.autoSkipConfidenceThreshold`).
    ///   - typicalAdDuration: Prior on ad duration in seconds. Used twice:
    ///     (1) gate: expand only when candidate duration < lowerBound / 2.
    ///     (2) fallback extent when no acoustic break is found on a side.
    /// - Returns: Expanded `(startTime, endTime)` tuple. Returns the original
    ///   bounds unchanged when the confidence gate, duration gate, or
    ///   (bounded) non-inversion safety checks would be violated.
    static func expand(
        startTime: Double,
        endTime: Double,
        adProbability: Double,
        featureWindows: [FeatureWindow],
        autoSkipConfidenceThreshold: Double,
        typicalAdDuration: ClosedRange<TimeInterval>
    ) -> (startTime: Double, endTime: Double) {
        // Confidence gate: only expand when the classifier is confident enough
        // that a false positive is unlikely. At 0.80 (default) + the broad
        // feature-window scan, a spurious expansion onto a silent show-intro
        // gap is ~order-of-magnitude rarer than the narrow-hit problem we are
        // fixing.
        guard adProbability >= autoSkipConfidenceThreshold else {
            return (startTime, endTime)
        }

        let duration = endTime - startTime

        // Duration gate: only expand when the candidate is materially shorter
        // than a typical ad. `typicalAdDuration.lowerBound / 2` is the most
        // conservative interpretation of "shorter than half a typical ad"
        // (default 30/2 = 15 s). Candidates already wider than 15 s are left
        // alone — they plausibly cover most of the real span already.
        let shortCandidateThreshold = typicalAdDuration.lowerBound / 2.0
        guard duration < shortCandidateThreshold else {
            return (startTime, endTime)
        }

        // Non-finite / degenerate durations short-circuit to no-op.
        guard duration.isFinite, duration >= 0 else {
            return (startTime, endTime)
        }

        let expansionConfig = BoundaryExpander.ExpansionConfig.neutral
        let backwardRadius = expansionConfig.acousticBackwardSearchRadius
        let forwardRadius = expansionConfig.acousticForwardSearchRadius

        // Narrow the feature-window input to the search envelope to keep
        // AcousticBreakDetector work bounded when callers pass a larger window.
        let searchStart = startTime - backwardRadius
        let searchEnd = endTime + forwardRadius
        let nearbyWindows = featureWindows.filter { fw in
            fw.endTime >= searchStart && fw.startTime <= searchEnd
        }

        let breaks = AcousticBreakDetector.detectBreaks(in: nearbyWindows)

        // Leading break: the nearest AcousticBreak at or before `startTime`
        // within `backwardRadius`. We anchor on `startTime` (not the center)
        // because the lexical hit typically sits at or near the leading edge
        // of the ad (greeting / sponsor name / jingle are the lexical patterns
        // that seed the candidate). Picking at-or-before avoids pulling the
        // start forward into the ad body.
        let leadingBreak = breaks
            .filter { $0.time <= startTime && $0.time >= startTime - backwardRadius }
            .max(by: { $0.time < $1.time }) // nearest to startTime

        // Trailing break: the nearest AcousticBreak at or after `endTime`
        // within `forwardRadius`, for symmetric reasons.
        let trailingBreak = breaks
            .filter { $0.time >= endTime && $0.time <= endTime + forwardRadius }
            .min(by: { $0.time < $1.time }) // nearest to endTime

        // Per-side fallback: `typicalAdDuration.lowerBound / 2` (15 s by
        // default) — just enough to cover the characteristic ad half-width.
        // Using lowerBound keeps the fallback conservative; using upperBound
        // or midpoint would over-expand when AcousticBreakDetector is silent
        // because features are noisy.
        let perSideFallbackWidth = typicalAdDuration.lowerBound / 2.0

        let expandedStart: Double
        if let leading = leadingBreak {
            expandedStart = leading.time
        } else {
            expandedStart = startTime - perSideFallbackWidth
        }

        let expandedEnd: Double
        if let trailing = trailingBreak {
            expandedEnd = trailing.time
        } else {
            expandedEnd = endTime + perSideFallbackWidth
        }

        // Safety: never narrow the persisted window, never invert it, never
        // produce a negative start time.
        let finalStart = max(0, min(expandedStart, startTime))
        let finalEnd = max(expandedEnd, endTime)

        return (finalStart, finalEnd)
    }
}

// MARK: - DecisionResultArtifact serialization (Bug 6)

/// Codable DTO mirroring `AdDecisionResult` for persistence in
/// `ad_decision_results.decisionJSON`.
///
/// `AdDecisionResult` itself is a runtime type used by `SkipOrchestrator` and is
/// intentionally not `Codable` — adding the conformance there would expand the
/// model's contract beyond the orchestrator's needs and pull
/// `AdDecisionEligibilityGate` into the persistence surface. Encoding through a
/// local DTO keeps the on-disk schema decoupled from the runtime struct so a
/// future field rename in `AdDecisionResult` does not silently change the JSON
/// shape that downstream consumers (replay, eval, NARL) depend on.
struct PersistedDecisionResult: Codable, Equatable {
    let id: String
    let analysisAssetId: String
    let startTime: Double
    let endTime: Double
    let skipConfidence: Double
    let eligibilityGate: String
    let recomputationRevision: Int

    init(_ result: AdDecisionResult) {
        self.id = result.id
        self.analysisAssetId = result.analysisAssetId
        self.startTime = result.startTime
        self.endTime = result.endTime
        self.skipConfidence = result.skipConfidence
        self.eligibilityGate = result.eligibilityGate.rawValue
        self.recomputationRevision = result.recomputationRevision
    }
}
