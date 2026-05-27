// SemanticScanResult.swift
// Persistence-facing FM scan result and evidence-event models.
//
// H16 resolved: `decisionCohortJSON` was removed from BackfillJob, the
// backfill_jobs column, and the reuse contract. Decision-time changes never
// invalidate FM scan output, so there is nothing to key on.

import Foundation
import OSLog

private let semanticScanLogger = Logger(
    subsystem: "com.playhead",
    category: "SemanticScanResult"
)

/// Rev3-M5: discriminator for `semantic_scan_results.phase` and
/// `evidence_events.phase`. Phase 3 shadow rows and Phase 5 targeted rows
/// are otherwise indistinguishable in those tables (only differ by
/// `reuseKeyHash`); the explicit phase tag lets queries filter without
/// reverse-engineering the hash inputs.
enum SemanticScanPhase: String, Sendable, Hashable, CaseIterable {
    case shadow
    case targeted
}

struct SemanticScanResult: Sendable, Equatable {
    let id: String
    let analysisAssetId: String
    let windowFirstAtomOrdinal: Int
    let windowLastAtomOrdinal: Int
    let windowStartTime: Double
    let windowEndTime: Double
    let scanPass: String
    let transcriptQuality: TranscriptQuality
    let disposition: CoarseDisposition
    let spansJSON: String
    let status: SemanticScanStatus
    let attemptCount: Int
    let errorContext: String?
    let inputTokenCount: Int?
    let outputTokenCount: Int?
    let latencyMs: Double?
    let prewarmHit: Bool
    let scanCohortJSON: String
    let transcriptVersion: String
    /// Optional stable scope included in persistence reuse hashing so
    /// logically distinct jobs/phases that share the same window bounds do
    /// not collapse each other. Nil preserves legacy reuse semantics.
    let reuseScope: String?
    /// Rev3-M5 (C4): run-mode discriminator persisted as a real column.
    /// Defaults to `.shadow` so existing call sites stay byte-identical.
    /// Targeted-narrowed rows opt-in by passing `.targeted`. Cycle-8
    /// reconciliation: renamed from `phase` → `runMode` to disambiguate
    /// from B6's `jobPhase` field.
    let runMode: SemanticScanPhase
    /// Cycle 6 B6 Rev3-M6: originating backfill phase (BackfillJobPhase.rawValue)
    /// or the sentinel `"shadow"` for rows persisted by callers that do not yet
    /// attribute phase. Stored in a distinct `jobPhase` column post cycle-8
    /// reconciliation and used by Rev3-M6 tests to verify that harvester
    /// and lexical narrowing phases actually produce strict-subset coverage.
    let jobPhase: String
    /// playhead-36t: model-generated refusal explanation captured from
    /// `LanguageModelSession.GenerationError.Refusal.explanation` when
    /// the FM classifier refuses this window. Nil for successful scans,
    /// for permissive-path scans, and when the async explanation fetch
    /// fails. Diagnostic only — does not affect routing or persistence
    /// schema.
    let refusalExplanation: String?
    /// playhead-eu1: true when the @Generable default path refused this
    /// window and the permissive string path was used as a fallback.
    let usedPermissiveFallback: Bool
    /// Model-generated explanation from `Refusal.explanation` at the time the permissive
    /// fallback was triggered. `nil` if explanation was unavailable or the fallback was not used.
    let permissiveFallbackReason: String?

    init(
        id: String,
        analysisAssetId: String,
        windowFirstAtomOrdinal: Int,
        windowLastAtomOrdinal: Int,
        windowStartTime: Double,
        windowEndTime: Double,
        scanPass: String,
        transcriptQuality: TranscriptQuality,
        disposition: CoarseDisposition,
        spansJSON: String,
        status: SemanticScanStatus,
        attemptCount: Int,
        errorContext: String?,
        inputTokenCount: Int?,
        outputTokenCount: Int?,
        latencyMs: Double?,
        prewarmHit: Bool,
        scanCohortJSON: String,
        transcriptVersion: String,
        reuseScope: String? = nil,
        runMode: SemanticScanPhase = .shadow,
        jobPhase: String = "shadow",
        refusalExplanation: String? = nil,
        usedPermissiveFallback: Bool = false,
        permissiveFallbackReason: String? = nil
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.windowFirstAtomOrdinal = windowFirstAtomOrdinal
        self.windowLastAtomOrdinal = windowLastAtomOrdinal
        self.windowStartTime = windowStartTime
        self.windowEndTime = windowEndTime
        self.scanPass = scanPass
        self.transcriptQuality = transcriptQuality
        self.disposition = disposition
        self.spansJSON = spansJSON
        self.status = status
        self.attemptCount = attemptCount
        self.errorContext = errorContext
        self.inputTokenCount = inputTokenCount
        self.outputTokenCount = outputTokenCount
        self.latencyMs = latencyMs
        self.prewarmHit = prewarmHit
        self.scanCohortJSON = scanCohortJSON
        self.transcriptVersion = transcriptVersion
        self.reuseScope = reuseScope
        self.runMode = runMode
        self.jobPhase = jobPhase
        self.refusalExplanation = refusalExplanation
        self.usedPermissiveFallback = usedPermissiveFallback
        self.permissiveFallbackReason = permissiveFallbackReason
    }

    func isReusable(
        scanCohortJSON: String,
        transcriptVersion: String
    ) -> Bool {
        // Decision cohort changes only affect downstream decisioning; the FM
        // scan remains reusable as long as the scan cohort and transcript match.
        Self.matchesScanCohortJSON(self.scanCohortJSON, scanCohortJSON) &&
        self.transcriptVersion == transcriptVersion
    }

    /// Internal hook so tests can observe decode-failure logging without
    /// scraping OSLog.
    nonisolated(unsafe) static var decodeFailureObserver: (@Sendable (String, String) -> Void)?

    static func matchesScanCohortJSON(_ lhs: String, _ rhs: String) -> Bool {
        if lhs == rhs {
            return true
        }

        let decoder = JSONDecoder()
        guard let lhsData = lhs.data(using: .utf8),
              let rhsData = rhs.data(using: .utf8) else {
            semanticScanLogger.debug("scan cohort comparison failed: invalid UTF-8")
            decodeFailureObserver?(lhs, rhs)
            return false
        }
        let lhsCohort: ScanCohort?
        let rhsCohort: ScanCohort?
        do {
            lhsCohort = try decoder.decode(ScanCohort.self, from: lhsData)
        } catch {
            semanticScanLogger.debug("scan cohort decode failed for stored value: \(String(describing: error), privacy: .public)")
            decodeFailureObserver?(lhs, rhs)
            return false
        }
        do {
            rhsCohort = try decoder.decode(ScanCohort.self, from: rhsData)
        } catch {
            semanticScanLogger.debug("scan cohort decode failed for query value: \(String(describing: error), privacy: .public)")
            decodeFailureObserver?(lhs, rhs)
            return false
        }
        return lhsCohort == rhsCohort
    }
}

enum EvidenceSourceType: String, Codable, Sendable, Hashable, CaseIterable {
    case fm
    case lexical
    case acoustic
    case catalog
    case classifier
    case fingerprint
    /// Calibration key for the fused aggregate score (proposalConfidence → skipConfidence).
    /// Distinct from `.classifier` to avoid conflating per-source classifier calibration
    /// with the post-fusion score mapping.
    case fusedScore
    /// playhead-z3ch: Pre-seeded evidence derived from RSS feed description /
    /// itunes:summary metadata. Capped at `FusionWeightConfig.metadataCap`
    /// (Plan §7.4 = 0.15) and gated by a corroboration check — metadata-only
    /// ledgers MUST resolve to `.blockedByEvidenceQuorum`.
    /// Persistence note: this enum is `Codable` and persisted via SQLite in
    /// `EvidenceEvent.sourceType`. The case is purely additive; no migration
    /// is required because no rows reference it pre-shipping.
    case metadata
    /// 2026-04-23 real-data eval Finding 4: music-bed coverage across a
    /// span's interior windows. Distinct from `.acoustic` so the quorum
    /// gate's `distinctKinds.count` increments when both an RMS-drop and
    /// a music-bed signal fire. Shares the acoustic evidence family
    /// (see `SourceEvidenceFamily.for`) for trust-update orthogonality —
    /// same underlying modality, different trigger geometry. Per-source
    /// budget is `FusionWeightConfig.musicBedCap` (NOT `acousticCap`):
    /// playhead-2hpn carved out a dedicated cap so the scoped-music-bed
    /// jingle boost (0.10 → 0.25) is not silently truncated to 0.20. See
    /// `BackfillEvidenceFusion.buildLedger()` for the dedicated branch
    /// and `MusicBedLedgerEvaluator.musicBedConfirmedJingleWeight` for
    /// the coupling invariant.
    /// Persistence note: additive case; no migration required.
    case musicBed
    /// playhead-fqc8: Acoustic-break alignment with a `.classifierSeed`-anchored
    /// span boundary. Treated as a DISTINCT evidence kind from `.acoustic`
    /// (RMS-drop) so the family budget is honest: each kind caps independently
    /// against its own per-source cap (`acousticCap` for RMS-drop,
    /// `breakAlignmentCap` for break-alignment). Without a separate kind, a
    /// classifier-seeded span could emit two `.acoustic` entries summing to
    /// 2 × `acousticCap` = 0.40, silently doubling the documented family
    /// budget. See `BackfillEvidenceFusion.buildLedger()` for the dedicated
    /// branch.
    /// Persistence note: additive case; no migration required. Forward-compatible
    /// only — a TestFlight downgrade to a build that lacks `.breakAlignment` would
    /// fail-loud at decode time (`AnalysisStore.readEvidenceEvent` throws
    /// `queryFailed("Unknown evidence source type 'breakAlignment'")`); acceptable
    /// for an additive enum and matches existing behavior for `.musicBed` etc.
    case breakAlignment
    /// playhead-xsdz.1: High-precision lexical auto-ad rule. Distinct from
    /// `.lexical` (the per-candidate confidence signal capped at the modest
    /// `lexicalCap = 0.20`) because this kind represents a strong, *vetted*
    /// co-occurrence of ad-copy signals (a sponsor disclosure PLUS a promo
    /// code and/or URL CTA) inside a tight time window, with negative-
    /// evidence guardrails already applied. It carries its own larger budget
    /// (`FusionWeightConfig.lexicalAutoAdCap`) and gates a dedicated
    /// `PromotionTrack.lexicalAutoAdQualified` so a confirmed combo can clear
    /// the auto-skip threshold on its own — which the structurally-capped
    /// `.lexical` family can never do. The separate kind also lets the
    /// quorum / corroboration gates count it as an independent in-audio
    /// evidence family without inflating the `.lexical` family cap.
    /// Persistence note: additive case; no migration required. Forward-only,
    /// matching `.breakAlignment` / `.musicBed`.
    case lexicalAutoAd
    /// Phase 11 random negative-audit marker. These rows are persisted in
    /// `evidence_events` for miss-rate estimation, but they are not positive
    /// FM evidence for training or fusion.
    case audit
    /// Phase 11 operational-health payloads for FM backfill jobs/runs.
    /// These rows use an empty atom ordinal array and are excluded from
    /// model-training evidence preparation.
    case operational

    var isObservabilityOnly: Bool {
        switch self {
        case .audit, .operational:
            return true
        case .fm, .lexical, .acoustic, .catalog, .classifier, .fingerprint,
             .fusedScore, .metadata, .musicBed, .breakAlignment, .lexicalAutoAd:
            return false
        }
    }
}

struct EvidenceEvent: Sendable, Equatable {
    let id: String
    let analysisAssetId: String
    let eventType: String
    let sourceType: EvidenceSourceType
    let atomOrdinals: String
    let evidenceJSON: String
    let scanCohortJSON: String
    let createdAt: Double
    /// Rev3-M5 (C4): run-mode discriminator persisted as a real column.
    /// Defaults to `.shadow` so existing call sites stay byte-identical.
    /// Cycle-8 reconciliation: renamed from `phase` → `runMode`.
    let runMode: SemanticScanPhase
    /// Cycle 6 B6 Rev3-M6: originating backfill phase (BackfillJobPhase.rawValue)
    /// or the sentinel `"shadow"` for legacy rows. Stored in a distinct
    /// `jobPhase` column post cycle-8 reconciliation.
    let jobPhase: String

    init(
        id: String,
        analysisAssetId: String,
        eventType: String,
        sourceType: EvidenceSourceType,
        atomOrdinals: String,
        evidenceJSON: String,
        scanCohortJSON: String,
        createdAt: Double,
        runMode: SemanticScanPhase = .shadow,
        jobPhase: String = "shadow"
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.eventType = eventType
        self.sourceType = sourceType
        self.atomOrdinals = atomOrdinals
        self.evidenceJSON = evidenceJSON
        self.scanCohortJSON = scanCohortJSON
        self.createdAt = createdAt
        self.runMode = runMode
        self.jobPhase = jobPhase
    }
}
