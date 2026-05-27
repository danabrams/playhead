// BackfillEvidenceFusion.swift
// Phase 6 (playhead-4my.6.1): Evidence ledger accumulation and decision mapping.
//
// Design:
//   • BackfillEvidenceFusion: pure value type, accumulates per-source evidence
//     into an EvidenceLedgerEntry ledger controlled by FMBackfillMode.
//   • DecisionMapper: pure value type, converts the ledger into a DecisionResult
//     carrying proposalConfidence, skipConfidence, and eligibilityGate.
//   • FM Positive-Only Rule: noAds/abstain/uncertain FM entries are silently dropped
//     regardless of mode — they affect scheduling/telemetry only, not the ledger.
//   • Gate blocks action at decision level; score is never clamped by the gate.

import Foundation
import OSLog

// MARK: - ClassificationTrustMatrix (ef2.4.5)

/// Pure lookup mapping (CommercialIntent × Ownership) → classificationTrust.
///
/// Trust modulates FM evidence weight in `BackfillEvidenceFusion.buildLedger()`.
/// Whether to *skip* host-reads is a policy decision (SkipPolicyMatrix, ef2.4.1),
/// NOT a classification decision — hence `paid|show` returns 1.0 (not discounted).
///
/// | CommercialIntent | Ownership   | classificationTrust |
/// |-----------------|-------------|---------------------|
/// | paid            | thirdParty  | 1.0                 |
/// | paid            | show        | 1.0                 |
/// | owned           | show        | 0.7                 |
/// | affiliate       | thirdParty  | 0.9                 |
/// | organic         | any         | 0.15                |
/// | unknown         | any         | 0.6                 |
enum ClassificationTrustMatrix {
    /// Fallback trust for unmapped (CommercialIntent × Ownership) combinations.
    static let fallback: Double = 0.6

    /// Look up classificationTrust for a given (CommercialIntent, Ownership) pair.
    static func trust(
        commercialIntent: CommercialIntent,
        ownership: Ownership
    ) -> Double {
        switch commercialIntent {
        case .paid:
            // paid|thirdParty = 1.0, paid|show = 1.0, paid|anything = 1.0
            return 1.0
        case .owned:
            if ownership == .show { return 0.7 }
            return fallback
        case .affiliate:
            if ownership == .thirdParty { return 0.9 }
            return fallback
        case .organic:
            return 0.15
        case .unknown:
            return fallback
        }
    }
}

// MARK: - FusionWeightConfig

/// Per-source weight caps for evidence fusion. v1 defaults reflect initial calibration.
/// All caps are configurable to allow future tuning without changing fusion logic.
struct FusionWeightConfig: Sendable {
    /// Maximum weight contribution from a single FM entry.
    let fmCap: Double
    /// Maximum weight contribution from the classifier score entry.
    let classifierCap: Double
    /// Maximum weight contribution from a single lexical entry.
    let lexicalCap: Double
    /// Maximum weight contribution from a single acoustic entry.
    let acousticCap: Double
    /// playhead-fqc8: Maximum weight contribution from a single
    /// `.breakAlignment` entry. Distinct from `acousticCap` because
    /// break-alignment is independent evidence from the RMS-drop family;
    /// honest scoring requires its own budget. Without this separation,
    /// a classifier-seeded span could emit an RMS-drop `.acoustic` entry
    /// AND a `.breakAlignment` entry that each cap independently against
    /// `acousticCap = 0.20`, silently doubling the documented acoustic
    /// family budget to 0.40. See `BackfillEvidenceFusion.buildLedger()`
    /// for the dedicated branch.
    let breakAlignmentCap: Double
    /// Maximum weight contribution from a single catalog entry.
    let catalogCap: Double
    /// Maximum weight contribution from fingerprint evidence.
    let fingerprintCap: Double
    /// playhead-z3ch: Maximum weight contribution from a single metadata
    /// (RSS-feed-derived) entry. Spec mandates 0.15 of fusion budget per
    /// Plan §7.4. Hard-clamped via `FusionBudgetClamp`; never redistributed.
    let metadataCap: Double
    /// playhead-2hpn: Maximum weight contribution from a single
    /// `.musicBed` entry. Distinct from `acousticCap` because the
    /// `scopedMusicBedGeneralization` boost path emits a 0.25 weight
    /// when a confirmed-show span overlaps a detected jingle region —
    /// clamping that at `acousticCap = 0.20` would silently truncate
    /// the bead-spec'd 0.10 → 0.25 promotion to 0.10 → 0.20 and the
    /// feature would never deliver the documented boost. The default
    /// 0.25 also preserves byte-identical flag-OFF behavior: the legacy
    /// `MusicBedLedgerEvaluator` path emits at most
    /// `presenceFraction * acousticCap = 0.20`, so `min(legacy, 0.25)
    /// == legacy` for every legacy input — the new cap only bites on
    /// the boosted path.
    ///
    /// COUPLING INVARIANT (see
    /// `MusicBedLedgerEvaluator.musicBedConfirmedJingleWeight`):
    /// `musicBedCap >= musicBedConfirmedJingleWeight` must hold at all
    /// times or the boost is silently truncated. Today both default to
    /// 0.25 (the cap equals the boost — zero headroom). If you raise
    /// the boost weight you MUST raise this cap at least as much.
    ///
    /// Enforcement is layered:
    ///   * Runtime (always-on, debug + release) — the `precondition`
    ///     inside `FusionWeightConfig.init` (R4→R5) traps any caller
    ///     that constructs a config with `musicBedCap` below the boost
    ///     weight, on every initializer path.
    ///   * Compile-time-equivalent — the default-init values are
    ///     pinned by `MusicBedLedgerEvaluatorJingleBoostTests
    ///     .musicBedCapAccommodatesBoostWeight`, which lights up if a
    ///     reviewer drops the default cap below the default boost
    ///     without re-running the runtime path.
    let musicBedCap: Double

    /// playhead-xsdz.1: Maximum weight contribution from a single
    /// `.lexicalAutoAd` entry — the high-precision lexical auto-ad rule.
    /// Distinct from `lexicalCap` (0.20) because the auto-ad rule represents
    /// a *vetted* strong co-occurrence (sponsor disclosure + promo code /
    /// URL CTA inside a tight window, negative-evidence guardrails already
    /// applied), which is a far stronger ad signal than a single lexical
    /// candidate's confidence. The default 0.55 is deliberately large enough
    /// that a confirmed combo, combined with the `.lexicalAutoAdQualified`
    /// promotion track (whose default threshold is 0.50), can drive an
    /// auto-skip on its own — which the structurally-capped `.lexical`
    /// channel (max 0.20) can never do. It does NOT inflate the `.lexical`
    /// family: `.lexicalAutoAd` is its own evidence kind with its own cap,
    /// mirroring the `.breakAlignment` / `.musicBed` carve-outs. Precision is
    /// guarded UP-stream (in `LexicalAutoAdEvidenceBuilder`), not by shrinking
    /// this cap — a misfire would be a builder bug, not a weight-budget bug.
    let lexicalAutoAdCap: Double

    /// playhead-xsdz.8: Maximum weight contribution from a single
    /// `.audioForensics` entry — the composite boundary-discontinuity channel
    /// (loudness/RMS jump + spectral-flux shift + noise-floor change +
    /// production/environment change at the span edges, all merged into ONE
    /// sigma-normalized score). Default 0.20 is deliberately MODEST and equal
    /// to `acousticCap`: this channel fires CONSERVATIVELY as a corroborator,
    /// never as the sole promoter. Unlike `.lexicalAutoAd` (0.55, can drive a
    /// skip alone via its qualified track), audio-forensics has NO qualified
    /// promotion track — its only job is to add honest in-audio mass and bump
    /// `distinctKinds.count` for the corroboration quorum. The merged-channel
    /// design (one cap, not three) is the cross-model duel's explicit
    /// recommendation; per-sub-signal caps were rejected. Mirrors the
    /// `.breakAlignment` / `.musicBed` per-source carve-outs.
    let audioForensicsCap: Double

    /// playhead-xsdz.9: Maximum weight contribution from a single
    /// `.crossEpisodeMemory` POSITIVE boost entry — a candidate that aligns
    /// (Smith-Waterman local alignment) to a CONFIRMED-AD bank sequence. Default
    /// 0.20 is deliberately MODEST (peer of `acousticCap` / `audioForensicsCap`):
    /// repeating known ad copy is corroborative, not decisive, and this channel
    /// has NO qualified promotion track, so it can never drive a skip alone — it
    /// only adds honest mass and bumps `distinctKinds.count`. The HARD-NEGATIVE
    /// half of the feature does NOT use this cap (it suppresses via a post-fusion
    /// multiplicative factor, not a ledger entry — a negative weight would be
    /// clamped to 0 by the v0 identity calibrator). Mirrors the
    /// `.breakAlignment` / `.musicBed` / `.audioForensics` per-source carve-outs.
    let crossEpisodeMemoryCap: Double

    /// playhead-xsdz.12: Maximum weight contribution from a single
    /// `.rhetoricalGrammar` entry — the rhetorical act-sequence grammar channel
    /// (3+ distinct persuasion roles co-occurring in canonical order across a
    /// span's transcript prose). Default 0.20 is deliberately MODEST (peer of
    /// `acousticCap` / `audioForensicsCap` / `crossEpisodeMemoryCap`): an ad-
    /// shaped rhetorical arc is corroborative, not decisive, and this channel
    /// has NO qualified promotion track, so it can never drive a skip alone — it
    /// only adds honest text-derived mass and bumps `distinctKinds.count` for
    /// the corroboration quorum. Mirrors the `.breakAlignment` / `.musicBed` /
    /// `.audioForensics` / `.crossEpisodeMemory` per-source carve-outs.
    let rhetoricalGrammarCap: Double

    /// playhead-xsdz.13: Maximum weight contribution from a single
    /// `.crossShowSyndication` entry — the cross-show syndication channel (a
    /// normalized sponsor entity that recurs across MANY of the user's UNRELATED
    /// shows AND has persisted across time, aggregated purely from the user's own
    /// local library). Default 0.20 is deliberately MODEST (peer of `acousticCap`
    /// / `audioForensicsCap` / `crossEpisodeMemoryCap` / `rhetoricalGrammarCap`):
    /// network-syndication footprint is corroborative, not decisive, and this
    /// channel has NO qualified promotion track, so it can never drive a skip
    /// alone — it only adds honest cross-library reference mass and bumps
    /// `distinctKinds.count` for the corroboration quorum. Mirrors the
    /// `.crossEpisodeMemory` / `.rhetoricalGrammar` per-source carve-outs.
    let crossShowSyndicationCap: Double

    init(
        fmCap: Double = 0.4,
        classifierCap: Double = 0.3,
        lexicalCap: Double = 0.2,
        acousticCap: Double = 0.2,
        breakAlignmentCap: Double = 0.2,
        catalogCap: Double = 0.2,
        fingerprintCap: Double = 0.25,
        metadataCap: Double = 0.15,
        musicBedCap: Double = 0.25,
        lexicalAutoAdCap: Double = 0.55,
        audioForensicsCap: Double = 0.2,
        crossEpisodeMemoryCap: Double = 0.2,
        rhetoricalGrammarCap: Double = 0.2,
        crossShowSyndicationCap: Double = 0.2
    ) {
        self.fmCap = fmCap
        self.classifierCap = classifierCap
        self.lexicalCap = lexicalCap
        self.acousticCap = acousticCap
        self.breakAlignmentCap = breakAlignmentCap
        self.catalogCap = catalogCap
        self.fingerprintCap = fingerprintCap
        self.metadataCap = metadataCap
        self.musicBedCap = musicBedCap
        self.lexicalAutoAdCap = lexicalAutoAdCap
        self.audioForensicsCap = audioForensicsCap
        self.crossEpisodeMemoryCap = crossEpisodeMemoryCap
        self.rhetoricalGrammarCap = rhetoricalGrammarCap
        self.crossShowSyndicationCap = crossShowSyndicationCap

        // playhead-2hpn R4 (+R5): enforce the musicBedCap >=
        // musicBedConfirmedJingleWeight invariant at construction time, not
        // only inside the default-init test. Catches any non-default
        // initializer (e.g. a tuning helper or A/B harness) that would set
        // `musicBedCap` below the boost weight and silently truncate it
        // (the exact R2 bug class).
        //
        // R5 (adversarial probe #1): upgraded from `assert` to `precondition`
        // so the invariant is enforced in RELEASE builds too. The whole
        // point of the carve-out is to keep the 0.10 → 0.25 boost from
        // silently truncating in production; an A/B tuning helper that
        // ships a misconfigured cap on a TestFlight build would, under the
        // debug-only `assert`, defeat the bead with zero observable
        // signal. The cost of a single Double comparison at config-init
        // time is negligible (FusionWeightConfig is constructed a handful
        // of times per backfill), and the rest of this codebase uses
        // `precondition` for analogous config-invariant checks
        // (`RepeatedAdCacheConfig.init`, `ScoreCalibrationProfile.init`,
        // …). Consistency + always-on enforcement together justify the
        // upgrade.
        //
        // R6 (adversarial probe #3): the coupling check is intentionally
        // ASYMMETRIC across caps — only `musicBedCap` carries one.
        // Rationale: producers of every other source kind compute their
        // emitted weight as a fraction of (or strictly bounded by) the
        // corresponding cap — e.g. `MusicBedLedgerEvaluator` legacy path
        // emits `presenceFraction * acousticCap`,
        // `buildAcousticPipelineLedgerEntries` emits
        // `maxCombined * acousticCap`, `buildCatalogLedgerEntries` emits
        // `count * 0.05 * catalogCap`, `ChapterMetadataEvidenceBuilder`
        // emits `0.10 < metadataCap = 0.15`. None can structurally
        // exceed its cap and so none can be "silently truncated" by it.
        // `.musicBed` is the lone exception: when the
        // `scopedMusicBedGeneralization` flag is on, the producer emits
        // a FIXED CONSTANT (`musicBedConfirmedJingleWeight`) whose value
        // is set independently of `musicBedCap`. That asymmetry is what
        // creates the silent-truncation hazard this precondition guards
        // against. If a future bead introduces another fixed-emit
        // constant for a source kind, this same coupling check should
        // be added alongside it.
        precondition(
            musicBedCap >= MusicBedLedgerEvaluator.musicBedConfirmedJingleWeight,
            "FusionWeightConfig.musicBedCap (\(musicBedCap)) must be >= MusicBedLedgerEvaluator.musicBedConfirmedJingleWeight (\(MusicBedLedgerEvaluator.musicBedConfirmedJingleWeight)) or the scoped-music-bed jingle boost is silently truncated inside buildLedger. Raise musicBedCap to match if you change the boost weight."
        )
    }
}

// MARK: - BackfillEvidenceFusion

/// Accumulates evidence from all sources into a ledger of `EvidenceLedgerEntry` items.
///
/// FM mode gating:
///   - `.off` / `.shadow`    → FM entries excluded from decision ledger
///   - `.rescoreOnly`        → FM entries join ledger for existing candidates
///   - `.proposalOnly`       → FM can propose new regions; rescoring follows .off semantics
///   - `.full`               → all FM entries join ledger
///
/// FM Positive-Only Rule: only `containsAd` disposition entries contribute. Entries
/// with `.noAds`, `.uncertain`, or `.abstain` dispositions are silently dropped
/// regardless of mode — they affect scheduling and diagnostics only.
struct BackfillEvidenceFusion: Sendable {
    private static let logger = Logger(subsystem: "com.playhead", category: "BackfillEvidenceFusion")

    let span: DecodedSpan
    /// Raw classifier score (0–1). Converted to a capped weight and added as a `.classifier` entry.
    let classifierScore: Double
    /// Pre-constructed FM ledger entries (caller's responsibility to create from FM scan results).
    let fmEntries: [EvidenceLedgerEntry]
    /// Pre-constructed lexical ledger entries.
    let lexicalEntries: [EvidenceLedgerEntry]
    /// playhead-xsdz.1: Pre-constructed high-precision lexical auto-ad
    /// ledger entries (`source == .lexicalAutoAd`). Each entry is capped to
    /// `config.lexicalAutoAdCap` inside `buildLedger()` — a much larger
    /// budget than `lexicalCap` so a confirmed strong co-occurrence can,
    /// together with the `.lexicalAutoAdQualified` promotion track, drive an
    /// auto-skip on its own. Defaults to empty so every existing call site
    /// stays byte-compatible (flag-free additive parameter).
    let lexicalAutoAdEntries: [EvidenceLedgerEntry]
    /// Pre-constructed acoustic ledger entries.
    let acousticEntries: [EvidenceLedgerEntry]
    /// playhead-fqc8: Pre-constructed `.breakAlignment` ledger entries.
    /// Capped against `config.breakAlignmentCap` (independent from
    /// `acousticCap`) so the alignment corroborator owns its own family
    /// budget. Defaults to empty for back-compat. Producer-side, the
    /// `AdDetectionService.buildAcousticLedgerEntries(...)` helper
    /// segregates `.breakAlignment` entries from RMS-drop `.acoustic`
    /// entries; the call site that wires fusion threads them in here.
    let breakAlignmentEntries: [EvidenceLedgerEntry]
    /// Pre-constructed catalog ledger entries.
    let catalogEntries: [EvidenceLedgerEntry]
    /// Pre-constructed fingerprint ledger entries (Phase 9).
    private(set) var fingerprintEntries: [EvidenceLedgerEntry] = []
    /// playhead-z3ch: Pre-constructed metadata (feed-description-derived)
    /// ledger entries. Each entry is hard-clamped to `config.metadataCap`
    /// via `FusionBudgetClamp` inside `buildLedger()`. Defaults to empty
    /// to keep all existing call sites byte-compatible.
    let metadataEntries: [EvidenceLedgerEntry]
    /// playhead-xsdz.8: Pre-constructed `.audioForensics` ledger entries —
    /// the composite boundary-discontinuity channel. Each entry is capped
    /// against `config.audioForensicsCap` inside `buildLedger()`. Defaults to
    /// empty so every existing call site stays byte-compatible (additive
    /// parameter); the producer (`AudioForensicsBoundaryDetector`) only emits
    /// when the OFF-by-default `AdDetectionConfig.audioForensicsEnabled` flag
    /// is on AND a significant boundary discontinuity is measured.
    let audioForensicsEntries: [EvidenceLedgerEntry]
    /// playhead-xsdz.9: Pre-constructed `.crossEpisodeMemory` POSITIVE boost
    /// entries (candidate aligned to a confirmed-ad bank sequence). Each entry
    /// is capped to `config.crossEpisodeMemoryCap` inside `buildLedger()`.
    /// Defaults to empty so every existing call site stays byte-compatible and
    /// the flag-OFF path emits nothing.
    let crossEpisodeMemoryEntries: [EvidenceLedgerEntry]
    /// playhead-xsdz.12: Pre-constructed `.rhetoricalGrammar` ledger entries —
    /// the rhetorical act-sequence grammar channel (3+ persuasion roles in
    /// canonical order). Each entry is capped to `config.rhetoricalGrammarCap`
    /// inside `buildLedger()`. Defaults to empty so every existing call site
    /// stays byte-compatible and the flag-OFF path emits nothing.
    let rhetoricalGrammarEntries: [EvidenceLedgerEntry]
    /// playhead-xsdz.13: Pre-constructed `.crossShowSyndication` ledger entries —
    /// the cross-show syndication channel (a normalized sponsor entity that
    /// recurs across MANY of the user's UNRELATED shows AND has persisted across
    /// time). Each entry is capped to `config.crossShowSyndicationCap` inside
    /// `buildLedger()`. Defaults to empty so every existing call site stays
    /// byte-compatible and the flag-OFF path emits nothing.
    let crossShowSyndicationEntries: [EvidenceLedgerEntry]
    let mode: FMBackfillMode
    let config: FusionWeightConfig

    init(
        span: DecodedSpan,
        classifierScore: Double,
        fmEntries: [EvidenceLedgerEntry],
        lexicalEntries: [EvidenceLedgerEntry],
        acousticEntries: [EvidenceLedgerEntry],
        catalogEntries: [EvidenceLedgerEntry],
        fingerprintEntries: [EvidenceLedgerEntry] = [],
        metadataEntries: [EvidenceLedgerEntry] = [],
        breakAlignmentEntries: [EvidenceLedgerEntry] = [],
        lexicalAutoAdEntries: [EvidenceLedgerEntry] = [],
        audioForensicsEntries: [EvidenceLedgerEntry] = [],
        crossEpisodeMemoryEntries: [EvidenceLedgerEntry] = [],
        rhetoricalGrammarEntries: [EvidenceLedgerEntry] = [],
        crossShowSyndicationEntries: [EvidenceLedgerEntry] = [],
        mode: FMBackfillMode,
        config: FusionWeightConfig
    ) {
        self.span = span
        self.classifierScore = classifierScore
        self.fmEntries = fmEntries
        self.lexicalEntries = lexicalEntries
        self.acousticEntries = acousticEntries
        self.catalogEntries = catalogEntries
        self.fingerprintEntries = fingerprintEntries
        self.metadataEntries = metadataEntries
        self.breakAlignmentEntries = breakAlignmentEntries
        self.lexicalAutoAdEntries = lexicalAutoAdEntries
        self.audioForensicsEntries = audioForensicsEntries
        self.crossEpisodeMemoryEntries = crossEpisodeMemoryEntries
        self.rhetoricalGrammarEntries = rhetoricalGrammarEntries
        self.crossShowSyndicationEntries = crossShowSyndicationEntries
        self.mode = mode
        self.config = config
    }

    /// Build the decision ledger for this span.
    ///
    /// Always includes: classifier, lexical, acoustic, catalog entries.
    /// Conditionally includes FM entries based on `mode.contributesToExistingCandidateLedger`.
    func buildLedger() -> [EvidenceLedgerEntry] {
        var ledger: [EvidenceLedgerEntry] = []

        // Classifier entry: always included (it's the old legacy path, always present)
        // Clamp to [0, cap]: adProbability is documented as [0,1] but defensive clamping prevents
        // a negative classifier score from producing a negative ledger weight.
        let classifierWeight = min(max(0.0, classifierScore) * config.classifierCap, config.classifierCap)
        ledger.append(EvidenceLedgerEntry(
            source: .classifier,
            weight: classifierWeight,
            detail: .classifier(score: classifierScore)
        ))

        // FM entries: gated by mode, filtered to containsAd only (Positive-Only Rule).
        // Non-positive dispositions (noAds/uncertain/abstain) are intentionally dropped;
        // they inform scheduling and telemetry only, not the ledger.
        if mode.contributesToExistingCandidateLedger {
            let allFMEntries = fmEntries.filter { entry in
                guard case .fm = entry.detail else { return false }
                return true
            }
            let positiveOnlyFMEntries = allFMEntries.filter { entry in
                guard case .fm(let disposition, _, _) = entry.detail else { return false }
                return disposition == .containsAd
            }
            let droppedCount = allFMEntries.count - positiveOnlyFMEntries.count
            if droppedCount > 0 {
                // Log so callers can observe the Positive-Only Rule in action.
                Self.logger.info("FM Positive-Only Rule dropped \(droppedCount)/\(allFMEntries.count) FM entries (non-containsAd dispositions).")
            }
            for entry in positiveOnlyFMEntries {
                // ef2.4.5: modulate FM weight by classificationTrust before capping.
                // Trust is derived from (CommercialIntent × Ownership) at entry creation
                // time and stored on the entry. Default of 1.0 preserves pre-ef2.4.5 behavior.
                // playhead-fqc8 cycle-1 review: preserve subSource uniformly across all
                // re-stamp loops; today no FM producer sets subSource, but the defensive
                // pass-through keeps the invariant uniform with the catalog/acoustic loops.
                let trustModulatedWeight = entry.weight * entry.classificationTrust
                let capped = EvidenceLedgerEntry(
                    source: .fm,
                    weight: min(trustModulatedWeight, config.fmCap),
                    detail: entry.detail,
                    classificationTrust: entry.classificationTrust,
                    subSource: entry.subSource
                )
                ledger.append(capped)
            }
        }

        // Lexical entries: always included.
        // playhead-fqc8 cycle-1 review: preserve subSource uniformly per
        // the family-cap invariant; no current lexical producer sets it.
        for entry in lexicalEntries {
            let capped = EvidenceLedgerEntry(
                source: .lexical,
                weight: min(entry.weight, config.lexicalCap),
                detail: entry.detail,
                subSource: entry.subSource
            )
            ledger.append(capped)
        }

        // playhead-xsdz.1: High-precision lexical auto-ad entries. Always
        // included, capped against the dedicated `lexicalAutoAdCap` (0.55
        // default) — NOT `lexicalCap` — so a confirmed strong co-occurrence
        // (vetted by `LexicalAutoAdEvidenceBuilder`, negative-evidence
        // guardrails already applied) carries enough mass to clear the
        // `.lexicalAutoAdQualified` promotion track's auto-skip threshold on
        // its own. Routing this through its own kind + cap keeps the
        // `.lexical` family budget unchanged (mirrors the `.breakAlignment` /
        // `.musicBed` carve-outs). `subSource` is preserved per the uniform
        // family-cap invariant.
        for entry in lexicalAutoAdEntries {
            let capped = EvidenceLedgerEntry(
                source: .lexicalAutoAd,
                weight: min(entry.weight, config.lexicalAutoAdCap),
                detail: entry.detail,
                subSource: entry.subSource
            )
            ledger.append(capped)
        }

        // Acoustic entries: always included. 2026-04-23 Finding 4:
        // this list now also carries `.musicBed`-sourced entries from
        // `AdDetectionService.buildMusicBedLedgerEntries`. Preserve
        // `entry.source` so `.musicBed` keeps its distinct kind
        // instead of being flattened to `.acoustic` — that's what
        // lets the quorum gate's `distinctKinds.count` increment.
        //
        // playhead-2hpn: clamp `.musicBed` against `musicBedCap` (0.25)
        // and `.acoustic` against `acousticCap` (0.20). Before this
        // bead the two shared `acousticCap`, but the 2hpn boost path
        // emits 0.25 on a confirmed-show jingle-overlap span — that
        // would have been silently truncated to 0.20. The legacy
        // (flag-OFF) `MusicBedLedgerEvaluator` output is bounded by
        // `presenceFraction * acousticCap ≤ 0.20 < musicBedCap`, so
        // routing it through `musicBedCap` is byte-identical for the
        // flag-OFF case while admitting the boost when the flag is on.
        // playhead-fqc8 cycle-1 review: continue to preserve
        // `entry.subSource` per the uniform-invariant (no producer in
        // this loop currently sets it now that breakAlignment moved to
        // its own source kind, but the pass-through stays defensive).
        for entry in acousticEntries {
            let cap = (entry.source == .musicBed)
                ? config.musicBedCap
                : config.acousticCap
            let capped = EvidenceLedgerEntry(
                source: entry.source,
                weight: min(entry.weight, cap),
                detail: entry.detail,
                subSource: entry.subSource
            )
            ledger.append(capped)
        }

        // playhead-fqc8: BreakAlignment entries — independent evidence
        // family from the RMS-drop `.acoustic` entries above. Capped
        // against `config.breakAlignmentCap` so the alignment evidence
        // owns its own honest budget (default 0.20). The previous design
        // emitted `.acoustic` + `subSource: .breakAlignment` and capped
        // the alignment entry against `acousticCap`, silently letting
        // the acoustic family contribute up to 2 × `acousticCap` = 0.40
        // for classifier-seeded spans. The cycle-1 skeptical review
        // flagged this as a family-budget violation — moving the entry
        // to its own kind + cap fixes it.
        for entry in breakAlignmentEntries {
            let capped = EvidenceLedgerEntry(
                source: .breakAlignment,
                weight: min(entry.weight, config.breakAlignmentCap),
                detail: entry.detail,
                subSource: entry.subSource
            )
            ledger.append(capped)
        }

        // playhead-xsdz.8: AudioForensics entries — the composite
        // boundary-discontinuity channel. ONE merged kind, ONE cap
        // (`audioForensicsCap`, default 0.20). The producer
        // (`AudioForensicsBoundaryDetector`) has already merged the four
        // sigma-normalized sub-signals (loudness / spectral / noise-floor /
        // environment) into a single boundary score before emitting, so the
        // family budget is honest with a single cap here — exactly the
        // cross-model duel's recommendation (no three separate caps). Empty
        // for every flag-OFF / non-firing call site, so the loop is a no-op
        // and the ledger is byte-identical to pre-xsdz.8.
        for entry in audioForensicsEntries {
            let capped = EvidenceLedgerEntry(
                source: .audioForensics,
                weight: min(entry.weight, config.audioForensicsCap),
                detail: entry.detail,
                subSource: entry.subSource
            )
            ledger.append(capped)
        }

        // playhead-xsdz.9: CrossEpisodeMemory POSITIVE boost entries — a
        // candidate whose transcript tokens align (Smith-Waterman local
        // alignment) to a CONFIRMED-AD bank sequence. ONE kind, ONE cap
        // (`crossEpisodeMemoryCap`, default 0.20). Empty for every flag-OFF /
        // non-firing call site, so the loop is a no-op and the ledger is
        // byte-identical to pre-xsdz.9. The HARD-NEGATIVE suppression half of
        // the feature is applied OUTSIDE the ledger (post-fusion multiplicative
        // factor in `AdDetectionService`), not here.
        for entry in crossEpisodeMemoryEntries {
            let capped = EvidenceLedgerEntry(
                source: .crossEpisodeMemory,
                weight: min(entry.weight, config.crossEpisodeMemoryCap),
                detail: entry.detail,
                subSource: entry.subSource
            )
            ledger.append(capped)
        }

        // playhead-xsdz.12: RhetoricalGrammar entries — the rhetorical
        // act-sequence grammar channel (3+ distinct persuasion roles in
        // canonical order across the span's transcript prose). ONE kind, ONE
        // cap (`rhetoricalGrammarCap`, default 0.20). Empty for every flag-OFF /
        // non-firing call site, so the loop is a no-op and the ledger is
        // byte-identical to pre-xsdz.12. The channel is text-derived (shares the
        // `.textual` family with `.lexical`) and corroborative only — no
        // qualified promotion track — so it can never drive a skip on its own.
        for entry in rhetoricalGrammarEntries {
            let capped = EvidenceLedgerEntry(
                source: .rhetoricalGrammar,
                weight: min(entry.weight, config.rhetoricalGrammarCap),
                detail: entry.detail,
                subSource: entry.subSource
            )
            ledger.append(capped)
        }

        // playhead-xsdz.13: CrossShowSyndication entries — a normalized sponsor
        // entity that recurs across MANY of the user's UNRELATED shows AND has
        // persisted across time (a paid network campaign vs. a show-specific
        // editorial mention). ONE kind, ONE cap (`crossShowSyndicationCap`,
        // default 0.20). Empty for every flag-OFF / non-firing call site, so the
        // loop is a no-op and the ledger is byte-identical to pre-xsdz.13. The
        // channel is a cross-library REFERENCE-match signal (shares the
        // `.reference` family with `.fingerprint` / `.catalog` /
        // `.crossEpisodeMemory`) and corroborative only — no qualified promotion
        // track — so it can never drive a skip on its own.
        for entry in crossShowSyndicationEntries {
            let capped = EvidenceLedgerEntry(
                source: .crossShowSyndication,
                weight: min(entry.weight, config.crossShowSyndicationCap),
                detail: entry.detail,
                subSource: entry.subSource
            )
            ledger.append(capped)
        }

        // Catalog entries: always included.
        // playhead-epfk: preserve `entry.subSource` so the producer label
        // (`transcriptCatalog` vs `fingerprintStore`) survives the cap
        // re-stamp into the final ledger; otherwise the disambiguation
        // stamped at the call site would be silently lost here.
        for entry in catalogEntries {
            let capped = EvidenceLedgerEntry(
                source: .catalog,
                weight: min(entry.weight, config.catalogCap),
                detail: entry.detail,
                subSource: entry.subSource
            )
            ledger.append(capped)
        }

        // Fingerprint entries: always included (Phase 9).
        // playhead-fqc8 cycle-1 review: preserve subSource uniformly per
        // the family-cap invariant; no current fingerprint producer sets it.
        for entry in fingerprintEntries {
            let capped = EvidenceLedgerEntry(
                source: .fingerprint,
                weight: min(entry.weight, config.fingerprintCap),
                detail: entry.detail,
                subSource: entry.subSource
            )
            ledger.append(capped)
        }

        // playhead-z3ch: Metadata entries (feed-description / itunes:summary).
        // Hard-clamped via FusionBudgetClamp to `config.metadataCap` (Plan §7.4 = 0.15).
        // Per the Expert Review contract: pre-seeded metadata contributes to evidence
        // fusion but cannot trigger a skip on its own — the corroboration gate in
        // `DecisionMapper.computeGate()` enforces that requirement separately.
        // Excess weight is clamped (NOT redistributed) and audited via the clamp.
        // playhead-fqc8 cycle-1 review: `FusionBudgetClamp.clamp` already
        // preserves the input entry's subSource (it re-stamps weight only),
        // so the uniform-invariant holds for metadata entries as well.
        let metadataClamp = FusionBudgetClamp(sourceWeightCap: config.metadataCap)
        for entry in metadataEntries {
            let clamped = metadataClamp.clamp(entry, logger: Self.logger)
            ledger.append(clamped)
        }

        return ledger
    }
}

// MARK: - PromotionTrack (playhead-fqc8)

/// Selects which auto-skip threshold applies to a `DecisionResult`.
///
/// Path-2 of playhead-fqc8: a classifier-only span hits a structural
/// ceiling at `classifierCap × 1.0 = 0.30` even when the classifier is
/// fully confident. The `0.80` standard auto-skip threshold is therefore
/// unreachable for those spans no matter how strong the classifier
/// signal is. `.classifierSeedQualified` opts a span into a separate
/// (lower) eligibility threshold IFF a quorum of independent corroborators
/// has fired:
///
///   1. Span anchor provenance contains `.classifierSeed`.
///   2. The ledger contains a `.classifier(score:)` entry whose stored
///      score is `>= 0.70`.
///   3. The ledger contains an entry with `source == .breakAlignment`.
///
/// The track is consumed by the AdDetectionService auto-skip gate;
/// `proposalConfidence` and `skipConfidence` are NEVER modified by
/// this decision (scores stay honest — the consumer chooses the
/// threshold).
/// playhead-fqc8 cycle-1 review LOW-1: dropped `Codable` conformance — the
/// track is not persisted in any artifact today, and adding a conformance
/// "just in case" creates a maintenance liability if persistence is ever
/// added with a different shape. Restore it (with a migration plan) when
/// a persisted artifact actually needs the field. `Hashable` is kept so
/// the track works with set membership / dictionary keys cheaply.
enum PromotionTrack: String, Sendable, Equatable, Hashable {
    case standard
    case classifierSeedQualified
    /// playhead-xsdz.1: A span carrying a high-precision `.lexicalAutoAd`
    /// entry. Mirrors `.classifierSeedQualified`: the `.lexical` family is
    /// structurally capped at `lexicalCap = 0.20`, so even a perfect ad-copy
    /// match cannot reach the standard `0.80` auto-skip threshold through the
    /// `.lexical` channel. The auto-ad rule emits a `.lexicalAutoAd` entry
    /// (own kind, own larger cap) only after a vetted strong co-occurrence
    /// (sponsor + promo code / URL CTA in a tight window) with negative-
    /// evidence guardrails already cleared; this track gives that vetted
    /// span a separate, lower eligibility floor
    /// (`lexicalAutoAdQualifiedThreshold`, default `0.50`). Scores stay
    /// honest — the track only selects the threshold, never modifies
    /// `proposalConfidence` / `skipConfidence`.
    case lexicalAutoAdQualified
}

// MARK: - DecisionResult

/// The output of `DecisionMapper`: three orthogonal decision signals.
///
/// `eligibilityGate` can block action without affecting the honesty of `skipConfidence`.
struct DecisionResult: Sendable, Equatable {
    /// Raw confidence estimate from summing all ledger entry weights, capped at 1.0.
    let proposalConfidence: Double
    /// Calibrated score for SkipOrchestrator (0–1 scale). Derived from proposalConfidence
    /// via a linear map. Not clamped by the eligibility gate.
    let skipConfidence: Double
    /// Whether this span is actionable. A blocked gate does not reduce the score.
    let eligibilityGate: SkipEligibilityGate
    /// playhead-fqc8: Which auto-skip threshold applies to this decision. Default
    /// `.standard`; promoted to `.classifierSeedQualified` only when the quorum
    /// described on `PromotionTrack` is satisfied. Score fields are unaffected.
    let promotionTrack: PromotionTrack

    init(
        proposalConfidence: Double,
        skipConfidence: Double,
        eligibilityGate: SkipEligibilityGate,
        promotionTrack: PromotionTrack = .standard
    ) {
        self.proposalConfidence = proposalConfidence
        self.skipConfidence = skipConfidence
        self.eligibilityGate = eligibilityGate
        self.promotionTrack = promotionTrack
    }
}

// MARK: - DecisionMapper

/// Converts an accumulated evidence ledger into a `DecisionResult`.
///
/// Quorum check reads `anchorProvenance` from the span — does NOT re-derive
/// multi-window consensus. The quorum rules are:
///
/// **fmConsensus provenance:**
/// Multi-window consensus is already satisfied. Remaining checks:
///   - 2+ distinct evidence kinds in ledger
///   - transcript quality == .good
///   - span duration in [5s, 180s]
///
/// **fmAcousticCorroborated provenance:**
/// Single-window FM + acoustic co-location. Needs external corroboration:
///   - at least one lexical, catalog, or acoustic entry in ledger
///
/// **No FM provenance:**
/// Quorum check is not applicable. Gate defaults to `.eligible` if score > 0.
struct DecisionMapper: Sendable {
    let span: DecodedSpan
    let ledger: [EvidenceLedgerEntry]
    let config: FusionWeightConfig
    let transcriptQuality: TranscriptQuality
    /// Phase 7.2: Combined correction factor pre-computed by the caller (actor context).
    /// < 1.0 = false-positive suppression (user said "not an ad").
    /// > 1.0 = false-negative boost (user said "hearing an ad").
    /// Default of 1.0 means no active corrections.
    let correctionFactor: Double
    /// ef2.4.3: Per-source calibration profile. v0 (identity) is the default.
    let calibrationProfile: ScoreCalibrationProfile
    /// playhead-p2iv: Soft monotonic duration prior sourced from
    /// `ResolvedPriors.typicalAdDuration`. Applied as a multiplier over the
    /// fused ledger sum — does NOT stack as an independent voter. Defaults to
    /// `.identity` (no-op) so existing callers keep their current behavior.
    let durationPrior: DurationPrior

    init(
        span: DecodedSpan,
        ledger: [EvidenceLedgerEntry],
        config: FusionWeightConfig,
        transcriptQuality: TranscriptQuality = .good,
        correctionFactor: Double = 1.0,
        calibrationProfile: ScoreCalibrationProfile = .v0,
        durationPrior: DurationPrior = .identity
    ) {
        self.span = span
        self.ledger = ledger
        self.config = config
        self.transcriptQuality = transcriptQuality
        self.correctionFactor = correctionFactor
        self.calibrationProfile = calibrationProfile
        self.durationPrior = durationPrior
    }

    func map() -> DecisionResult {
        // ef2.4.3: Apply per-source calibration before summing contributions.
        // Each entry's weight is transformed through the source-specific calibrator
        // from the calibration profile. v0 (identity) produces identical results
        // to the uncalibrated path; v1 reshapes per-source contributions.
        let calibratedSum = scoringLedger.reduce(0.0) { sum, entry in
            let sourceCalibrator = calibrationProfile.calibrator(for: entry.source)
            return sum + sourceCalibrator.calibrate(entry.weight)
        }
        let rawProposalConfidence = min(1.0, calibratedSum)

        // playhead-p2iv: Apply the duration prior as a bounded multiplier over
        // the fused ledger sum. The multiplier is constrained to ~[0.75, 1.10]
        // by construction (see DurationPrior) so it can nudge but not dominate
        // the decision. Clamp back to [0, 1] so downstream treat-as-probability
        // consumers see a valid range even when a strong ledger × peak > 1.
        let durationMultiplier = durationPrior.multiplier(forDuration: span.duration)
        let priorAdjusted = rawProposalConfidence * durationMultiplier
        let proposalConfidence = max(0.0, min(1.0, priorAdjusted))

        // Phase 7.2: apply combined correction factor to skipConfidence.
        // The factor is pre-computed by the caller (AdDetectionService, an actor)
        // from PersistentUserCorrectionStore.
        // < 1.0 = false-positive suppression; if effective confidence drops below 0.40,
        //         gate the span as blockedByUserCorrection.
        // > 1.0 = false-negative boost; caps at 1.0 to avoid over-confident decisions.
        // TODO: ef2.1.7 — Apply calibrationProfile.decisionThresholds here.
        // v0 thresholds are zero (no-op). When a real v1 profile ships with non-zero
        // skipMinimum/proposalMinimum, gate the score before correction factor.
        let rawSkipConfidence = calibrate(proposalConfidence)
        let effectiveConfidence = min(1.0, rawSkipConfidence * max(0.0, correctionFactor))
        let gate: SkipEligibilityGate
        if correctionFactor < 1.0 && effectiveConfidence < 0.40 {
            gate = .blockedByUserCorrection
        } else {
            gate = computeGate()
        }
        let skipConfidence = effectiveConfidence

        // playhead-fqc8: Promotion-track selection. Computed AFTER the
        // gate so the score-mapping pipeline above is unchanged. The
        // track is read by the AdDetectionService auto-skip gate to
        // pick a threshold; it never modifies `proposalConfidence` or
        // `skipConfidence`.
        let promotionTrack = computePromotionTrack()

        return DecisionResult(
            proposalConfidence: proposalConfidence,
            skipConfidence: skipConfidence,
            eligibilityGate: gate,
            promotionTrack: promotionTrack
        )
    }

    // MARK: - Private

    private var scoringLedger: [EvidenceLedgerEntry] {
        ledger.filter { !$0.source.isObservabilityOnly }
    }

    /// Calibration from proposalConfidence to skipConfidence via the active profile.
    /// v0 profile: identity mapping (direct pass-through). Future profiles apply
    /// piecewise-linear calibration learned from shadow-mode data.
    ///
    /// Non-finite guard is retained as a safety belt: NaN/Inf inputs always return 0.0
    /// regardless of the profile's calibrator (a computationally overflowed ledger is a
    /// data integrity error — err on the conservative side).
    private func calibrate(_ raw: Double) -> Double {
        guard raw.isFinite else { return 0.0 }
        // The profile's calibrator handles [0,1] clamping internally.
        // We use .fusedScore (not .classifier) because proposalConfidence is a post-fusion
        // aggregate, not a raw per-source classifier score. Using a distinct key prevents
        // future non-identity profiles from conflating the two distributions.
        let clamped = max(0.0, min(1.0, raw))
        return calibrationProfile.calibrator(for: .fusedScore).calibrate(clamped)
    }

    /// Compute the promotion track for this span.
    ///
    /// Two qualified tracks exist; both NEVER modify `proposalConfidence` /
    /// `skipConfidence` — they are pure threshold-selectors consumed by the
    /// AdDetectionService auto-skip gate. The `.lexicalAutoAd` track is
    /// checked FIRST: it is the highest-precision track (the producing rule
    /// in `LexicalAutoAdEvidenceBuilder` already required a vetted strong
    /// co-occurrence and cleared negative-evidence guardrails), so when both
    /// would qualify, the lexical-auto-ad track wins. Otherwise we fall
    /// through to the classifier-seed track, then `.standard`.
    private func computePromotionTrack() -> PromotionTrack {
        if qualifiesForLexicalAutoAdTrack() {
            return .lexicalAutoAdQualified
        }
        if qualifiesForClassifierSeedTrack() {
            return .classifierSeedQualified
        }
        return .standard
    }

    /// playhead-xsdz.1: Returns `true` IFF this span qualifies for the
    /// high-precision lexical-auto-ad promotion track:
    ///   1. The ledger contains at least one `.lexicalAutoAd` entry (the
    ///      auto-ad rule already vetted the strong co-occurrence and cleared
    ///      negative-evidence guardrails before emitting it).
    ///   2. `span.anchorProvenance` contains NO FM-class anchor
    ///      (`.fmConsensus` / `.fmAcousticCorroborated`). Mirrors the
    ///      classifier-seed track's M-2 carve-out: an FM-corroborated span
    ///      has independent FM evidence and clears the standard 0.80 gate on
    ///      its own merits, so it stays on `.standard`. Keeping the qualified
    ///      track exclusively for non-FM spans avoids broadening it beyond
    ///      the bead's framing.
    private func qualifiesForLexicalAutoAdTrack() -> Bool {
        let hasLexicalAutoAd = scoringLedger.contains { $0.source == .lexicalAutoAd }
        guard hasLexicalAutoAd else { return false }

        let hasFMAnchor = span.anchorProvenance.contains { ref in
            switch ref {
            case .fmConsensus, .fmAcousticCorroborated:
                return true
            default:
                return false
            }
        }
        return !hasFMAnchor
    }

    /// playhead-fqc8: Returns `true` IFF this span qualifies for the
    /// classifier-seed promotion track. See `PromotionTrack` for the rules:
    ///   1. `span.anchorProvenance` contains a `.classifierSeed` case.
    ///   2. `span.anchorProvenance` contains NO FM-class anchor
    ///      (`.fmConsensus` or `.fmAcousticCorroborated`). The qualified
    ///      track is for classifier-only candidates; FM-corroborated spans
    ///      have a separate path to clear the standard 0.80 gate on their
    ///      own merits, and conflating the two would broaden the track
    ///      beyond the bead's framing. Cycle-1 skeptical review M-2.
    ///   3. The ledger contains a `.classifier(score:)` entry whose
    ///      stored score is `>= 0.70`.
    ///   4. The ledger contains an entry with `source == .breakAlignment`
    ///      (the dedicated alignment evidence kind introduced in cycle-1
    ///      to honor the acoustic family budget).
    private func qualifiesForClassifierSeedTrack() -> Bool {
        let hasClassifierSeed = span.anchorProvenance.contains {
            if case .classifierSeed = $0 { return true }
            return false
        }
        guard hasClassifierSeed else { return false }

        // playhead-fqc8 cycle-1 M-2: the qualified track is exclusively
        // for classifier-only candidates. A span carrying an FM-class
        // anchor has independent FM evidence and must clear the 0.80
        // standard gate via the standard track.
        let hasFMAnchor = span.anchorProvenance.contains { ref in
            switch ref {
            case .fmConsensus, .fmAcousticCorroborated:
                return true
            default:
                return false
            }
        }
        guard !hasFMAnchor else { return false }

        let hasStrongClassifier = scoringLedger.contains { entry in
            if case .classifier(let score) = entry.detail, score >= 0.70 {
                return true
            }
            return false
        }
        guard hasStrongClassifier else { return false }

        let hasBreakAlignment = scoringLedger.contains { entry in
            entry.source == .breakAlignment
        }
        return hasBreakAlignment
    }

    /// Determine the eligibility gate by reading `anchorProvenance` directly.
    /// Score is computed independently — gate NEVER modifies it.
    private func computeGate() -> SkipEligibilityGate {
        let provenance = span.anchorProvenance

        // Classify FM provenance type from anchorProvenance
        let hasFMConsensus = provenance.contains {
            if case .fmConsensus = $0 { return true }
            return false
        }
        let hasFMAcoustic = provenance.contains {
            if case .fmAcousticCorroborated = $0 { return true }
            return false
        }

        if hasFMConsensus {
            return quorumGateForFMConsensus()
        } else if hasFMAcoustic {
            return quorumGateForFMAcoustic()
        } else {
            // No FM provenance — quorum normally not applicable, but
            // playhead-z3ch enforces a metadata-corroboration gate: a span
            // whose only weighted evidence is metadata-derived MUST NOT
            // trigger a skip on its own. The Expert Review contract:
            // "Pre-seeded metadata contributes to evidence fusion but
            // cannot trigger a skip decision on its own. At least one
            // corroborating in-audio signal is required."
            return metadataCorroborationGate()
        }
    }

    /// playhead-z3ch: corroboration check for metadata-only spans.
    ///
    /// Returns `.blockedByEvidenceQuorum` when the ledger's only weighted
    /// contribution is `.metadata` (the always-present zero-weight
    /// `.classifier` entry from `buildLedger()` does NOT count as
    /// corroboration). Otherwise returns `.eligible` — preserving the
    /// pre-z3ch "no FM provenance → eligible" semantics for any ledger
    /// that contains at least one in-audio signal.
    private func metadataCorroborationGate() -> SkipEligibilityGate {
        let hasMetadata = scoringLedger.contains { $0.source == .metadata }
        guard hasMetadata else {
            // No metadata contribution at all — preserve pre-z3ch behavior.
            return .eligible
        }
        // Treat the always-present zero-weight `.classifier` entry as
        // non-corroborating (it carries no in-audio signal when its
        // adProbability is zero). Any other in-audio source — or a
        // classifier entry with a non-zero weight — counts as corroboration.
        // playhead-fqc8 cycle-2 review HIGH-2: include `.breakAlignment` —
        // boundary-alignment is real in-audio signal corroborating the
        // metadata cue. Same root cause as HIGH-1: the cycle-1 family-budget
        // fix promoted alignment to its own kind, and any gate that
        // previously accepted the alignment corroborator under `.acoustic`
        // must explicitly accept `.breakAlignment` now.
        // playhead-xsdz.1: `.lexicalAutoAd` is a strong in-audio (transcript-
        // derived) corroborator — the high-precision auto-ad rule fired on a
        // vetted strong co-occurrence. Include it so a span that carries both
        // a metadata cue AND a lexical-auto-ad hit clears this gate.
        // playhead-xsdz.8: `.audioForensics` is real in-audio signal — a
        // measured physical boundary discontinuity. Include it on the same
        // footing as its acoustic-family peers (`.acoustic` / `.breakAlignment`
        // / `.musicBed`): the "never the sole promoter" guard is its modest
        // CAP plus the absence of any qualified promotion track, NOT exclusion
        // from corroboration. Omitting it would make a STRONGER boundary
        // measurement count for less than the weaker RMS-drop `.acoustic`
        // entry — the exact inconsistency the fqc8 cycle-2 review fixed for
        // `.breakAlignment`.
        // playhead-xsdz.12: `.rhetoricalGrammar` is a text-derived in-audio
        // corroborator (the persuasion-arc grammar fired on the span's
        // transcript prose). Include it on the same footing as `.lexical` /
        // `.lexicalAutoAd`: its "never the sole promoter" guard is its modest
        // CAP plus the absence of any qualified promotion track, NOT exclusion
        // from corroboration — mirroring the xsdz.8 `.audioForensics` rationale.
        let inAudioCorroboratingSources: Set<EvidenceSourceType> = [
            .lexical, .lexicalAutoAd, .acoustic, .musicBed, .catalog, .fingerprint, .fm, .breakAlignment, .audioForensics, .rhetoricalGrammar
        ]
        let hasInAudioCorroboration = scoringLedger.contains { entry in
            if inAudioCorroboratingSources.contains(entry.source) {
                return true
            }
            if entry.source == .classifier, entry.weight > 0 {
                return true
            }
            return false
        }
        return hasInAudioCorroboration ? .eligible : .blockedByEvidenceQuorum
    }

    /// Quorum check for spans anchored by fmConsensus.
    /// Multi-window consensus is already satisfied; checks remaining requirements.
    private func quorumGateForFMConsensus() -> SkipEligibilityGate {
        // Check span duration [5s, 180s]
        let duration = span.duration
        guard duration >= 5.0 && duration <= 180.0 else {
            return .blockedByEvidenceQuorum
        }

        // Check transcript quality
        guard transcriptQuality == .good else {
            return .blockedByEvidenceQuorum
        }

        // Check 2+ distinct *corroborating* evidence kinds. The
        // always-present zero-weight `.classifier` entry from
        // `buildLedger()` is excluded so the gate cannot be satisfied by
        // a single FM signal alone. Sister gate `metadataCorroborationGate`
        // applies the same filter — keep them in sync.
        let corroboratingSources = Set(scoringLedger.compactMap { entry -> EvidenceSourceType? in
            if entry.source == .classifier, entry.weight == 0 { return nil }
            return entry.source
        })
        guard corroboratingSources.count >= 2 else {
            return .blockedByEvidenceQuorum
        }

        return .eligible
    }

    /// Quorum check for spans anchored by fmAcousticCorroborated only.
    /// Needs external corroboration from any non-FM source: classifier, lexical, catalog, or acoustic.
    /// Classifier is included because it is an independent, non-FM signal that provides corroboration —
    /// but the always-present zero-weight `.classifier` entry from `buildLedger()` is excluded so the
    /// gate cannot be satisfied by a vacuous classifier=0 record alone. Sister gates
    /// `metadataCorroborationGate` and `quorumGateForFMConsensus` apply the same filter — keep in sync.
    private func quorumGateForFMAcoustic() -> SkipEligibilityGate {
        // playhead-fqc8 cycle-2 review HIGH-1: include `.breakAlignment` in the
        // corroboration set. Pre-fqc8 the alignment corroborator was emitted as
        // `source: .acoustic + subSource: .breakAlignment` and so satisfied this
        // gate via `.acoustic`. The cycle-1 family-budget fix promoted alignment
        // to its own top-level kind (`.breakAlignment`); without adding it here,
        // a span anchored by `.fmAcousticCorroborated` whose only non-FM evidence
        // is the boundary-alignment entry would silently regress to
        // `.blockedByEvidenceQuorum` where it previously cleared.
        // playhead-xsdz.8: `.audioForensics` is in-audio boundary signal —
        // same rationale as the cycle-2 `.breakAlignment` addition. A span
        // anchored by `.fmAcousticCorroborated` whose only non-FM evidence is
        // the boundary-discontinuity entry is corroborated by a real measured
        // signal; excluding it would make audio-forensics count for less than
        // its weaker acoustic-family peers.
        // playhead-xsdz.12: include `.rhetoricalGrammar` — a text-derived
        // in-audio corroborator (the persuasion-arc grammar on the span's
        // transcript prose). Same rationale as `.lexical` / `.audioForensics`:
        // it is real non-FM signal corroborating an fmAcoustic-anchored span.
        let nonClassifierExternal: Set<EvidenceSourceType> = [.lexical, .lexicalAutoAd, .catalog, .acoustic, .musicBed, .fingerprint, .breakAlignment, .audioForensics, .rhetoricalGrammar]
        let hasExternalCorroboration = scoringLedger.contains { entry in
            if nonClassifierExternal.contains(entry.source) {
                return true
            }
            if entry.source == .classifier, entry.weight > 0 {
                return true
            }
            return false
        }
        return hasExternalCorroboration ? .eligible : .blockedByEvidenceQuorum
    }
}
