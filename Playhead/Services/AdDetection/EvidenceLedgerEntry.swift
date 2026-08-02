// EvidenceLedgerEntry.swift
// Phase 6 (playhead-4my.6.1): Per-source evidence ledger entry and eligibility gate.
//
// Design:
//   • Each evidence source contributes capped, trust-scaled EvidenceLedgerEntry items.
//   • SkipEligibilityGate controls whether a decision is actionable without clamping score.
//   • EvidenceLedgerDetail carries source-specific metadata per variant.

import Foundation

// MARK: - SkipEligibilityGate

/// Controls whether a span decision is actionable.
///
/// A gate block prevents action but does NOT clamp the score — `skipConfidence`
/// remains an honest estimate regardless of the gate value.
enum SkipEligibilityGate: String, Sendable, Codable, Equatable, CaseIterable {
    /// Decision is actionable; all quorum and policy requirements are met.
    case eligible
    /// FM-only or weak corroboration: evidence quorum not satisfied.
    case blockedByEvidenceQuorum
    /// External policy (e.g. content type, show-level overrides) prevents skip.
    case blockedByPolicy
    /// Span crosses a high-quality content chapter; eligible for banner only, not auto-skip.
    case markOnly
    /// User previously vetoed this span or region.
    case blockedByUserCorrection
    /// An FM `noAds` consensus covered this span and nothing strong survived it:
    /// the model was asked whether there is an ad here, said no twice at
    /// `.moderate`+ certainty, and no URL/promo-code/sponsor anchor, catalog
    /// entry, fingerprint match, rediff confirmation or FM `containsAd` entry
    /// contradicted it (see ``FMSuppressionGuard`` / ``FMSuppressionApplicator``).
    /// **Blocked — no auto-skip and no banner.**
    ///
    /// playhead-avbn: this case was `cappedByFMSuppression`, documented "capped
    /// to mark-only" and carrying `severity == 1` alongside ``markOnly`` — while
    /// ``SkipOrchestrator`` routed only ``markOnly`` to the suggest tier and
    /// DROPPED this one at playhead-bq70's blocked-gate guard. The name and the
    /// severity promised a banner the surface never emitted.
    ///
    /// Resolved in the direction of the behaviour rather than the name. The
    /// population is by construction the weakest span the pipeline can produce
    /// — the guard already declines to fire when a strong anchor, catalog entry
    /// or fingerprint is present, and the cap additionally requires that no
    /// strong proposal survived — so it is a span the model actively voted down
    /// with nothing corroborating it, and a banner over it is a banner for
    /// something we have positive reason to think is not an ad. The reach that
    /// the old name appeared to promise is recovered upstream instead, by
    /// playhead-avbn's other half: with pass-B refinements and no-work sentinels
    /// no longer manufacturing the `noAds` consensus, spans that never deserved
    /// the cap keep their honest ``eligible`` / ``markOnly`` gate and reach the
    /// user through their own tier.
    ///
    /// Named `blockedBy*` to match every other severity-≥2 case: the point of
    /// the rename is that the name must predict the behaviour.
    case blockedByFMConsensus

    /// playhead-avbn: the raw value ``blockedByFMConsensus`` was persisted under
    /// before the rename. `ad_windows.eligibilityGate` rows written by earlier
    /// builds still carry it, and ``SkipOrchestrator`` fails a gate it cannot
    /// decode CLOSED (`droppedMalformedEligibilityGate`) — so without the alias
    /// the terminal outcome would be unchanged but the census would attribute
    /// every legacy row to a decode fault instead of to the FM consensus that
    /// actually blocked it. The audit trail is the product here; keep it exact.
    static let legacyFMConsensusRawValue = "cappedByFMSuppression"

    /// Accepts the canonical raw value of every case, plus
    /// ``legacyFMConsensusRawValue``. Derived from ``allCases`` rather than a
    /// hand-written switch so a future case cannot be added without also being
    /// decodable. Also backs the synthesized `Codable` conformance, which
    /// routes `init(from:)` through this initializer.
    init?(rawValue: String) {
        if let canonical = Self.allCases.first(where: { $0.rawValue == rawValue }) {
            self = canonical
            return
        }
        if rawValue == Self.legacyFMConsensusRawValue {
            self = .blockedByFMConsensus
            return
        }
        return nil
    }

    /// Restriction severity for ordering: higher means more restrictive.
    /// Used by SpanFinalizer.capEligibility to allow demotions but prevent promotions.
    /// Gates at the same severity level cannot override each other (first writer wins).
    var severity: Int {
        switch self {
        case .eligible: return 0
        case .markOnly: return 1
        case .blockedByEvidenceQuorum: return 2
        case .blockedByPolicy: return 2
        case .blockedByUserCorrection: return 3
        // playhead-avbn: 2, with the other blocked cases — it was 1, which said
        // "no more restrictive than markOnly" and therefore could not demote a
        // markOnly span. A real FM veto outranks a chapter-overlap or self-promo
        // mark, and a gate that blocks must sort with the gates that block.
        case .blockedByFMConsensus: return 2
        }
    }
}

// MARK: - EvidenceLedgerDetail

/// Source-specific metadata attached to each ledger entry.
enum EvidenceLedgerDetail: Sendable {
    /// Old RuleBasedClassifier score promoted to a ledger entry.
    case classifier(score: Double)
    /// Foundation Model disposition with certainty band and cohort label.
    case fm(disposition: CoarseDisposition, band: CertaintyBand, cohortPromptLabel: String)
    /// Lexical pattern matches — categories that fired.
    case lexical(matchedCategories: [String])
    /// Acoustic break detection strength.
    case acoustic(breakStrength: Double)
    /// playhead-fqc8: Acoustic-break alignment with a `.classifierSeed`-anchored
    /// span boundary. Mirrors `.acoustic`'s shape (single `breakStrength`
    /// payload) but rides on a distinct `EvidenceSourceType.breakAlignment`
    /// kind so it can be capped against its own dedicated weight budget
    /// (`FusionWeightConfig.breakAlignmentCap`) instead of stealing from the
    /// acoustic family budget.
    case breakAlignment(breakStrength: Double)
    /// Catalog entries matched for this span.
    case catalog(entryCount: Int)
    /// Ad copy fingerprint matches for this span.
    case fingerprint(matchCount: Int, averageSimilarity: Double)
    /// playhead-z3ch: Pre-seeded evidence derived from RSS feed metadata
    /// (description / summary cues). `cueCount` is the number of distinct
    /// metadata cues that contributed; `sourceField` records which RSS
    /// field contributed the strongest cue; `dominantCueType` is the
    /// strongest contributing cue type for diagnostics.
    case metadata(
        cueCount: Int,
        sourceField: MetadataCueSourceField,
        dominantCueType: MetadataCueType
    )
    /// Music-bed coverage across the span's windows. `presenceFraction`
    /// is the ratio of windows whose `MusicBedLevel != .none`;
    /// `foregroundCount` is how many of those windows were tagged
    /// `.foreground` (jingles/stingers) vs. `.background` (production
    /// beds under voice). Emitted by `MusicBedLedgerEvaluator`.
    case musicBed(presenceFraction: Double, foregroundCount: Int)
    /// playhead-xsdz.8: Composite audio-forensics boundary evidence.
    /// `boundaryScore` is the merged, sigma-normalized boundary-discontinuity
    /// score in `[0, 1]` (loudness jump + spectral-flux shift + noise-floor
    /// change + production/environment change across the span edges).
    /// `dominantSignal` records which sub-signal contributed the most mass
    /// (for diagnostics / NARL replay — e.g. `"loudnessJump"`,
    /// `"spectralShift"`, `"noiseFloor"`, `"environment"`).
    /// `contributingSignalCount` is how many sub-signals cleared their
    /// per-signal floor. Emitted by `AudioForensicsBoundaryDetector`.
    case audioForensics(
        boundaryScore: Double,
        dominantSignal: String,
        contributingSignalCount: Int
    )
}

// MARK: - EvidenceSubSource

/// Disambiguates the producer of an `EvidenceLedgerEntry` whose
/// `source` is shared by multiple subsystems. Two source types currently
/// use sub-source labels:
///
/// **Catalog (`source == .catalog`) — playhead-epfk:**
///   - `.transcriptCatalog` — `EvidenceCatalogBuilder` extracts sponsor
///     tokens / URLs / promo codes / disclosures deterministically from
///     transcript atoms in the *current* episode. Per-backfill, never
///     persisted.
///   - `.fingerprintStore` — `AdCatalogStore` matches a span's acoustic
///     fingerprint against the cross-episode SQLite store accumulated
///     from prior auto-skips and user corrections. Per-span similarity
///     in `[0, 1]`.
///
/// **Metadata (`source == .metadata`) — playhead-rxuv:**
///   - `.creatorChapter` — `ChapterMetadataEvidenceBuilder` stamps a
///     `.metadata` entry whose underlying `ChapterEvidence` came from a
///     creator source (PC20 / RSS inline / ID3 — i.e.
///     `ChapterSource.isCreatorSource == true`). Inferred (FM-labeled)
///     chapters remain untagged; the follow-on `playhead-w7oi` bead will
///     own that label. Only stamped when
///     `PreAnalysisConfig.creatorChapterFusionEnabled` is on.
///
/// playhead-fqc8 history: an earlier draft used `subSource ==
/// .breakAlignment` on an `.acoustic` entry to mark the
/// `AcousticBreakDetector`-alignment corroborator. That shape was
/// upgraded to a top-level `EvidenceSourceType.breakAlignment` so the
/// alignment evidence has its own honest per-source budget instead of
/// sharing the acoustic family cap.
///
/// `nil` (the default for back-compat constructors) means "source label
/// is the only producer marker," matching pre-epfk fixtures and call
/// sites that predate the disambiguation.
enum EvidenceSubSource: String, Sendable, Codable, Equatable, Hashable, CaseIterable {
    /// `EvidenceCatalogBuilder`'s sponsor-token catalog (in-pipeline,
    /// transcript-derived). The label NARL replay should attribute to
    /// the per-episode evidence channel.
    case transcriptCatalog
    /// `AdCatalogStore` cross-episode fingerprint match. The label NARL
    /// replay should attribute to the cumulative correction-loop signal.
    case fingerprintStore
    /// playhead-rxuv: Creator-supplied (Podcasting 2.0 / RSS inline /
    /// ID3 CHAP) chapter marker. Distinguishes a `.metadata` entry whose
    /// `sourceField == .chapter` and whose underlying `ChapterEvidence`
    /// came from a creator source (`ChapterSource.isCreatorSource == true`),
    /// versus an inferred (FM-labeled) chapter — only creator-supplied
    /// chapters get this tag. Stamped by `ChapterMetadataEvidenceBuilder`
    /// when `PreAnalysisConfig.creatorChapterFusionEnabled` is on; absent
    /// (flag-off path) means byte-identical to pre-rxuv output.
    case creatorChapter
}

// MARK: - EvidenceLedgerEntry

/// A single capped, trust-scaled contribution from one evidence source.
///
/// Multiple entries from the same source are allowed (e.g. multiple FM windows).
/// `BackfillEvidenceFusion` accumulates these; `DecisionMapper` sums the
/// automatic-decision subset into `proposalConfidence` while retaining
/// diagnostic-only rows for replay.
struct EvidenceLedgerEntry: Sendable {
    /// Which evidence source produced this entry.
    let source: EvidenceSourceType
    /// Capped, trust-scaled weight in the range [0, cap] where cap is source-specific.
    let weight: Double
    /// Source-specific metadata for diagnostics and logging.
    let detail: EvidenceLedgerDetail
    /// ef2.4.5: Classification trust factor from (CommercialIntent × Ownership) lookup.
    /// Applied by `BackfillEvidenceFusion.buildLedger()` to modulate FM evidence weight.
    /// Default of 1.0 means no modulation (backward compatible with pre-ef2.4.5 entries).
    let classificationTrust: Double
    /// playhead-epfk: Optional disambiguator for sources that have multiple
    /// distinct producers under one umbrella label. Currently used by
    /// `.catalog` (transcript sponsor catalog vs. `AdCatalogStore`
    /// fingerprint match — playhead-epfk) and by `.metadata`
    /// (`.creatorChapter` for PC20 / RSS inline / ID3 chapter markers —
    /// playhead-rxuv). See `EvidenceSubSource` for the per-source
    /// breakdown. `nil` for every other source (and pre-epfk callers) so
    /// adding the field is purely additive: existing constructors compile
    /// unchanged and the JSONL schema gains an optional key.
    let subSource: EvidenceSubSource?

    /// Whether this row may affect an automatic decision. Learned
    /// fingerprint-catalog matches remain in the ledger for provenance and
    /// replay, but they are not current-episode presence or boundary proof and
    /// therefore cannot add score, satisfy quorum, or suppress a demotion.
    var contributesToAutomaticDecision: Bool {
        guard !source.isObservabilityOnly else { return false }
        return !(
            source == .catalog
                && subSource == .fingerprintStore
        )
    }

    init(
        source: EvidenceSourceType,
        weight: Double,
        detail: EvidenceLedgerDetail,
        classificationTrust: Double = 1.0,
        subSource: EvidenceSubSource? = nil
    ) {
        self.source = source
        self.weight = weight
        self.detail = detail
        self.classificationTrust = classificationTrust
        self.subSource = subSource
    }
}
