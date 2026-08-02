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

/// playhead-hx6n: the app state a semantic scan row was written in.
///
/// **The vocabulary is BORROWED, NOT INVENTED** (playhead-9v09). These four
/// raw values are byte-identical to the strings `BGTaskTelemetryScenePhase`
/// produces and `background_task_runs.scenePhase` already stores, because they
/// come from the same helper. That is what makes a phase split over
/// `semantic_scan_results` directly comparable with one over
/// `background_task_runs` instead of being a second, parallel vocabulary that
/// happens to use similar words.
///
/// Two absences are deliberately kept distinct, and BOTH are unattributed:
///
///   * a `nil` `SemanticScanResult.scenePhase` — no binary ever recorded a
///     phase for this row. Every row written before schema V42 is in this
///     state, permanently and correctly: they are genuinely unattributable and
///     nothing invents a value for them.
///   * `.unknown` — a phase *was* recorded and the platform declined to name
///     it (`@unknown default`, or a non-UIKit host).
///
/// They answer different questions ("is the writer wired up?" versus "is the
/// platform answering?"), so they are not collapsed at rest. They collapse only
/// at the point of ATTRIBUTION, in ``ScanAttributionBucket``, where both are
/// `.unattributed` — see ``attributionBucket``.
enum ScanScenePhase: String, Sendable, Hashable, CaseIterable {
    case active
    case inactive
    case background
    case unknown
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
    /// playhead-hx6n (schema V42): UNIX seconds at which this row was written.
    ///
    /// `nil` means UNATTRIBUTED, and it means exactly one thing on disk: the row
    /// predates V42. Post-V42 writes cannot leave it nil —
    /// ``AnalysisStore/insertSemanticScanResult(_:now:)`` stamps its own clock
    /// when the caller supplies none, so a NULL `createdAt` is an unambiguous
    /// "written by a binary that did not record this", never "a writer forgot".
    ///
    /// It is NOT defaulted to 0 or to `Date()` on the struct. A default would
    /// make every historical row claim a timestamp it does not have, which is
    /// the precise defect this bead exists to end.
    let createdAt: Double?
    /// playhead-hx6n (schema V42): the app state this row was written in.
    ///
    /// `nil` is UNATTRIBUTED and stays unattributed. Nothing in this codebase
    /// may read `nil` as `.active` — see ``ScanAttributionBucket`` and
    /// ``SemanticScanThroughputSplit``.
    let scenePhase: ScanScenePhase?
    /// playhead-hx6n (schema V42): the `backfill_jobs.jobId` this row was
    /// produced under — the join key `semantic_scan_results` never had.
    ///
    /// `nil` for rows written before V42 and for any writer with no job in
    /// hand. A non-nil value that matches no job joins to nothing, which reads
    /// as "no run" rather than as a wrong run.
    let runCorrelationId: String?

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
        permissiveFallbackReason: String? = nil,
        createdAt: Double? = nil,
        scenePhase: ScanScenePhase? = nil,
        runCorrelationId: String? = nil
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
        self.createdAt = createdAt
        self.scenePhase = scenePhase
        self.runCorrelationId = runCorrelationId
    }

    /// playhead-hx6n: the ONE seam that stamps run attribution onto a scan row.
    ///
    /// Every `SemanticScanResult` factory in `BackfillJobRunner` already had the
    /// job id in hand — each one takes `jobId:` and passes it as `reuseScope`,
    /// where it was hashed into `reuseKeyHash` and then discarded. Rather than
    /// widen six factories, attribution is applied once, at the seam where the
    /// row is handed to the store, which is also the moment that makes
    /// `scenePhase` mean what it claims: the phase AT COMPLETION, not the phase
    /// the job happened to start in. A backfill job runs for minutes and can
    /// cross a phase boundary mid-run; stamping at job start would attribute
    /// every one of its rows to whichever side it began on — the exact
    /// misattribution the measurement is trying to avoid.
    ///
    /// Deliberately non-mutating and total: it returns a copy with the three
    /// attribution fields replaced and everything else byte-identical, so it
    /// cannot perturb geometry, status, or the reuse key.
    func attributed(
        createdAt: Double,
        scenePhase: ScanScenePhase?,
        runCorrelationId: String?
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: analysisAssetId,
            windowFirstAtomOrdinal: windowFirstAtomOrdinal,
            windowLastAtomOrdinal: windowLastAtomOrdinal,
            windowStartTime: windowStartTime,
            windowEndTime: windowEndTime,
            scanPass: scanPass,
            transcriptQuality: transcriptQuality,
            disposition: disposition,
            spansJSON: spansJSON,
            status: status,
            attemptCount: attemptCount,
            errorContext: errorContext,
            inputTokenCount: inputTokenCount,
            outputTokenCount: outputTokenCount,
            latencyMs: latencyMs,
            prewarmHit: prewarmHit,
            scanCohortJSON: scanCohortJSON,
            transcriptVersion: transcriptVersion,
            reuseScope: reuseScope,
            runMode: runMode,
            jobPhase: jobPhase,
            refusalExplanation: refusalExplanation,
            usedPermissiveFallback: usedPermissiveFallback,
            permissiveFallbackReason: permissiveFallbackReason,
            createdAt: createdAt,
            scenePhase: scenePhase,
            runCorrelationId: runCorrelationId
        )
    }

    /// playhead-avbn: the pass whose ``disposition`` is a PRESENCE VERDICT — the
    /// answer to "is there an ad in this window". Deliberately defined AS
    /// ``SemanticScanCoverage/coverageScanPass`` rather than repeating the
    /// literal, because they are the same fact seen from two sides: `passA` is
    /// the pass that screens a window and says yes or no, which is exactly why
    /// it is also the pass coverage counts.
    ///
    /// `passB` is refinement. It runs only inside windows `passA` already called
    /// `containsAd` and it is asked WHERE the edges are, so its `.noAds` means
    /// "found no edges", not "there is no ad". Any consumer reading a
    /// `disposition` as evidence of ABSENCE must filter to this pass — see
    /// ``FMSuppressionWindow/votingWindows(spanStartTime:spanEndTime:scanResults:)``.
    static let presenceScanPass = SemanticScanCoverage.coverageScanPass

    /// playhead-pz32: `errorContext` prefix marking a NO-WORK SENTINEL row —
    /// see `BackfillJobRunner.makeNoWorkSentinelScanResult`. Such a row carries
    /// `status == .noAds` (whose ``SemanticScanStatus/didExamineWindow`` is
    /// `true`, because `.noAds` is normally the permissive path's genuine "I
    /// looked and there is nothing here" verdict) and spans the WHOLE attempted
    /// transcript range — while explicitly meaning **no work was performed**.
    ///
    /// Any coverage accounting that treats it as examined therefore reports an
    /// entire episode as screened off a job that made zero FM calls. The status
    /// alone cannot distinguish the two, so the `errorContext` marker is the
    /// discriminator, and it lives here so the writer, the pipeline breadcrumb
    /// and the user-facing readiness predicate all agree.
    static let noWorkSentinelErrorContextPrefix = "noWork:"

    /// playhead-pz32: true when this row is a no-work sentinel rather than a
    /// real examination. NEVER count it as scanned audio.
    var isNoWorkSentinel: Bool {
        Self.isNoWorkSentinel(errorContext: errorContext)
    }

    /// playhead-pz32: did this row screen its window and produce a verdict? The
    /// single definition of "we looked", combining the status semantics with the
    /// no-work-sentinel exclusion.
    var didExamineWindow: Bool {
        Self.didExamineWindow(status: status, errorContext: errorContext)
    }

    static func isNoWorkSentinel(errorContext: String?) -> Bool {
        errorContext?.hasPrefix(noWorkSentinelErrorContextPrefix) == true
    }

    /// playhead-pz32: THE definition of "this row screened its window", stated
    /// over the two raw column values so a narrow SQL projection can apply it
    /// without decoding a whole row.
    /// ``AnalysisStore/fetchCoverageSummariesByAssetIds(_:)`` calls this; do not
    /// re-implement the condition at a call site. A `nil` status is an
    /// unrecognised persisted string (forward-compat) and is NOT an examination —
    /// under-claim.
    static func didExamineWindow(status: SemanticScanStatus?, errorContext: String?) -> Bool {
        guard let status, status.didExamineWindow else { return false }
        return !isNoWorkSentinel(errorContext: errorContext)
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

/// playhead-qbib: honest scanned-duration accounting over persisted
/// `semantic_scan_results` rows.
///
/// Every downstream ratio — coverage %, precision measurement, the
/// playhead-0sro watermark invariants — SHOULD divide by "how much audio did
/// we actually screen". Before this type existed there was no way to ask that
/// question: a refused window and a window screened clean both persisted a
/// row, so a truncated scan that reported success was indistinguishable from
/// a complete one. `examinedSeconds` answers "we looked"; `unexaminedSeconds`
/// / `unexaminedRanges` answer "we could not look" — including audio no
/// window ever reached, when `episodeDuration` is supplied.
///
/// Scope note (playhead-qbib): today the only production consumer is the
/// run-completion breadcrumb in `BackfillJobRunner.logCoarseCoverage`. Wiring
/// it into the coverage/precision reporting and the 0sro watermark is
/// deliberately NOT part of this bead — those consumers still compute their
/// own denominators, and moving them is a measurement change, not a
/// robustness fix.
///
/// Ranges are half-open in spirit but modelled as `ClosedRange` because a
/// window's `[start, end]` is inclusive of both transcript boundaries. Zero-
/// and negative-width rows are ignored: they carry no coverage information
/// (and a `0.0 ... 0.0` row is the passB coordinate bug this bead fixed).
struct SemanticScanCoverage: Sendable, Equatable {
    /// Union of the time ranges of rows that actually obtained a verdict
    /// (`SemanticScanResult.didExamineWindow`).
    let examinedSeconds: Double
    /// Union of attempted-or-expected time that produced no verdict, with
    /// `examinedSeconds` subtracted out. A window that refused and was then
    /// recovered by a smaller retry is NOT a hole.
    let unexaminedSeconds: Double
    /// The individual holes behind `unexaminedSeconds`, in time order. These
    /// are the ranges a consumer must not describe as "no ads here".
    let unexaminedRanges: [ClosedRange<Double>]

    /// Total audio this accounting covers. With no `episodeDuration` that is
    /// "screened + tried to screen"; with one it also includes audio no
    /// window ever reached, i.e. "screened + should have screened".
    var accountedSeconds: Double { examinedSeconds + unexaminedSeconds }

    /// Fraction of `accountedSeconds` that produced a real verdict. `1.0`
    /// when nothing was accounted for, so an empty pass never reads as a hole.
    var examinedFraction: Double {
        let total = accountedSeconds
        guard total > 0 else { return 1 }
        return examinedSeconds / total
    }

    /// True when every second accounted for produced a verdict.
    var isComplete: Bool { unexaminedRanges.isEmpty }

    /// playhead-pz32: the canonical COVERAGE LANE. `passA` rows are the
    /// whole-episode screening sweep; `passB` rows are localized extent
    /// attempts INSIDE already-screened `passA` windows, so counting them
    /// would double-count.
    ///
    /// Shared with ``AnalysisStore/fetchCoverageSummariesByAssetIds(_:)``'s
    /// `adScanCoveredSec` derivation, so the pipeline breadcrumb and the
    /// user-facing readiness predicate agree about WHICH ROWS constitute
    /// coverage. They deliberately do NOT agree on the resulting SECONDS: this
    /// type unions the window bounds as persisted, while `adScanCoveredSec`
    /// additionally intersects them with the transcribed region, because a
    /// window's bounds can straddle audio its prompt never contained. Expect
    /// `examinedSeconds >= adScanCoveredSec`, sometimes by a lot — the log is
    /// answering "how much did the pass attempt to screen", the checkmark is
    /// answering "how much audio was read".
    static let coverageScanPass = "passA"

    /// Compute coverage for one scan pass.
    ///
    /// - Parameters:
    ///   - rows: persisted scan rows for a single asset. Rows from other
    ///     passes are filtered out — passB rows are localized extent
    ///     attempts inside already-screened passA windows, so mixing them in
    ///     would double-count.
    ///   - scanPass: the pass to account for. `passA` is the coverage lane.
    ///   - episodeDuration: when supplied, audio in `0 ... episodeDuration`
    ///     that no row covers at all is reported as unexamined. This is what
    ///     catches a pass that stopped at 1425.9s of a 3578s episode: the
    ///     rows it did write all look fine on their own. Note the window is
    ///     measured from zero, so an episode whose first scanned window starts
    ///     after t=0 reports that lead-in as a hole — which is correct: no
    ///     window looked there.
    static func compute(
        rows: [SemanticScanResult],
        scanPass: String = SemanticScanCoverage.coverageScanPass,
        episodeDuration: Double? = nil
    ) -> SemanticScanCoverage {
        let passRows = rows.filter { $0.scanPass == scanPass && $0.windowEndTime > $0.windowStartTime }
        // playhead-pz32: `didExamineWindow` is the row-level predicate (status
        // semantics MINUS no-work sentinels), not the bare status. A sentinel
        // spans the whole attempted range while meaning "no work performed", so
        // counting it as examined turned a job that made zero FM calls into a
        // fully-screened episode. It is correctly reported as a HOLE instead.
        let examined = merge(
            passRows
                .filter(\.didExamineWindow)
                .map { $0.windowStartTime ... $0.windowEndTime }
        )
        var unexamined = merge(
            passRows
                .filter { !$0.didExamineWindow }
                .map { $0.windowStartTime ... $0.windowEndTime }
        )
        if let episodeDuration, episodeDuration > 0 {
            let attempted = merge(passRows.map { $0.windowStartTime ... $0.windowEndTime })
            unexamined = merge(unexamined + subtract(cuts: attempted, from: [0 ... episodeDuration]))
        }
        let holes = subtract(cuts: examined, from: unexamined)
        return SemanticScanCoverage(
            examinedSeconds: total(of: examined),
            unexaminedSeconds: total(of: holes),
            unexaminedRanges: holes
        )
    }

    private static func total(of ranges: [ClosedRange<Double>]) -> Double {
        ranges.reduce(0) { $0 + ($1.upperBound - $1.lowerBound) }
    }

    /// Sort and coalesce overlapping/touching ranges into a disjoint union.
    private static func merge(_ ranges: [ClosedRange<Double>]) -> [ClosedRange<Double>] {
        let sorted = ranges.sorted { $0.lowerBound < $1.lowerBound }
        var merged: [ClosedRange<Double>] = []
        for range in sorted {
            guard let last = merged.last else {
                merged.append(range)
                continue
            }
            if range.lowerBound <= last.upperBound {
                merged[merged.count - 1] = last.lowerBound ... Swift.max(last.upperBound, range.upperBound)
            } else {
                merged.append(range)
            }
        }
        return merged
    }

    /// Remove every second covered by `cuts` from `ranges`. Both inputs are
    /// merged first so the walk only has to handle disjoint, ordered spans.
    private static func subtract(
        cuts: [ClosedRange<Double>],
        from ranges: [ClosedRange<Double>]
    ) -> [ClosedRange<Double>] {
        let orderedCuts = merge(cuts)
        var remaining: [ClosedRange<Double>] = []
        for range in merge(ranges) {
            var cursor = range.lowerBound
            for cut in orderedCuts where cut.upperBound > range.lowerBound && cut.lowerBound < range.upperBound {
                if cut.lowerBound > cursor {
                    remaining.append(cursor ... cut.lowerBound)
                }
                cursor = Swift.max(cursor, cut.upperBound)
            }
            if cursor < range.upperBound {
                remaining.append(cursor ... range.upperBound)
            }
        }
        return remaining
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
    /// playhead-xsdz.8: Composite audio-forensics boundary channel. ONE merged
    /// evidence kind carrying the PHYSICAL signature of audio insertion at a
    /// candidate ad boundary — discontinuities that ad TEXT cannot fake:
    /// loudness/RMS jump, spectral-character (flux) shift, noise-floor change,
    /// and recording-environment (production/music) change measured ACROSS the
    /// span's start and end edges relative to the span interior.
    ///
    /// Why ONE kind (not three caps): the cross-model idea duel explicitly
    /// recommended a single capped channel. Each sub-signal is sigma-normalized
    /// against the episode's own distribution and the strongest few are merged
    /// into one boundary-forensics score before it ever becomes a ledger entry,
    /// so the family budget is a single `FusionWeightConfig.audioForensicsCap`
    /// (mirrors the `.breakAlignment` / `.musicBed` carve-outs). It fires
    /// CONSERVATIVELY (corroborative only) and is OFF by default
    /// (`AdDetectionConfig.audioForensicsEnabled`).
    /// Persistence note: additive case; no migration required. Forward-only,
    /// matching `.breakAlignment` / `.musicBed` / `.lexicalAutoAd`.
    case audioForensics
    /// playhead-xsdz.9: Cross-episode "memory" POSITIVE boost. Emitted when a
    /// candidate's transcript tokens align strongly (Smith-Waterman local
    /// alignment) to a CONFIRMED-AD bank sequence — i.e. the same ad copy was
    /// already confirmed on a prior episode. A modest corroborator capped at
    /// `FusionWeightConfig.crossEpisodeMemoryCap`; it never drives a skip on its
    /// own (no qualified promotion track). Distinct from `.fingerprint` (MinHash
    /// Jaccard, order-insensitive) because this is order-SENSITIVE local
    /// alignment. The HARD-NEGATIVE half of the same feature is NOT a ledger
    /// entry — it is a post-fusion multiplicative suppression (a negative ledger
    /// weight would be clamped to 0 by the v0 identity calibrator), so only the
    /// positive boost rides this source kind. Gated OFF by default
    /// (`AdDetectionConfig.crossEpisodeMemoryEnabled`).
    /// Persistence note: additive case; no migration required. Forward-only.
    case crossEpisodeMemory
    /// playhead-xsdz.12: Rhetorical act-sequence grammar. Emitted when a
    /// candidate span's transcript prose exhibits the canonical persuasion
    /// PROGRAM — HOOK → PROBLEM → SOLUTION → EVIDENCE → OFFER → CTA — with
    /// THREE OR MORE distinct rhetorical roles co-occurring in (roughly) that
    /// order. No single role is ad-specific (content can ask questions, cite
    /// stats, give URLs); the ORDERED CO-OCCURRENCE of 3+ roles is what is
    /// almost exclusively an ad, and it fires even when the lexical
    /// sponsor/promo/URL cues do not. Text-derived, so it shares the `.textual`
    /// evidence family with `.lexical` / `.lexicalAutoAd`. A MODEST corroborator
    /// capped at `FusionWeightConfig.rhetoricalGrammarCap` with NO qualified
    /// promotion track — it never drives a skip on its own, only adds honest
    /// in-audio (transcript) mass and bumps `distinctKinds.count`. Gated OFF by
    /// default (`AdDetectionConfig.rhetoricalGrammarEnabled`).
    /// Persistence note: additive case; no migration required. Forward-only,
    /// matching `.crossEpisodeMemory` / `.audioForensics`.
    case rhetoricalGrammar
    /// playhead-xsdz.13: Cross-show syndication POSITIVE boost. Emitted when a
    /// candidate span's NORMALIZED sponsor entity (extracted by
    /// `EvidenceCatalogBuilder`) recurs across MANY of the user's UNRELATED
    /// subscribed shows AND has persisted across time — overwhelming evidence of
    /// a paid NETWORK ad campaign (ads are sold across show networks), as opposed
    /// to a show-specific editorial brand mention. Aggregated PURELY from the
    /// user's OWN local library by `CrossShowSyndicationStore` — no network, no
    /// cross-user data. A MODEST corroborator capped at
    /// `FusionWeightConfig.crossShowSyndicationCap` with NO qualified promotion
    /// track — it never drives a skip on its own, only adds honest mass and bumps
    /// `distinctKinds.count`. It is a cross-library REFERENCE-match signal (shares
    /// the `.reference` family with `.fingerprint` / `.catalog` /
    /// `.crossEpisodeMemory`), so — like `.crossEpisodeMemory` — it is
    /// deliberately NOT counted as in-audio corroboration. Gated OFF by default
    /// (`AdDetectionConfig.crossShowSyndicationEnabled`).
    /// Persistence note: additive case; no migration required. Forward-only,
    /// matching `.crossEpisodeMemory` / `.rhetoricalGrammar`.
    case crossShowSyndication
    /// playhead-xsdz.62: Byte-exact REDIFF-confirmed DAI insertion. Emitted by
    /// `BackfillEvidenceFusion.buildLedger()` (behind `rediffConfirmedKindEnabled`)
    /// for a span whose WIDTH is owned by the byte-exact rediff oracle
    /// (`.rediffSlot` in `anchorProvenance` == `DecodedSpan.carriesRediffByteExactWidth`).
    /// A byte-exact-rediff-confirmed rotating region IS a DAI-inserted ad by
    /// deterministic definition (the origin literally served different ad bytes
    /// on a re-fetch — no classification needed), so this is a DISTINCT
    /// corroborating KIND for the fusion corroboration quorum: it gives
    /// rediff-confirmed DAI a reproducible 2nd deterministic kind so FM's vote
    /// stops being load-bearing for their eligibility. It is a deterministic
    /// PRESENCE marker, NOT a score driver — the emitted entry carries weight 0
    /// (the kind itself is the whole signal), so it increments
    /// `distinctKinds.count` in the corroboration gates WITHOUT changing
    /// `proposalConfidence` / `skipConfidence`. It is a cross-fetch reference-match
    /// signal (shares the `.reference` family with `.fingerprint` / `.catalog` /
    /// `.crossEpisodeMemory` / `.crossShowSyndication`). Deliberately byte-exact
    /// ONLY: acoustic splice (`.spliceSlot`) is NOT deterministic and never
    /// triggers this kind (it fails `carriesRediffByteExactWidth`).
    /// Persistence note: additive case; no migration required. Forward-only,
    /// matching `.breakAlignment` / `.crossShowSyndication`.
    case rediffConfirmed
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
             .fusedScore, .metadata, .musicBed, .breakAlignment, .lexicalAutoAd,
             .audioForensics, .crossEpisodeMemory, .rhetoricalGrammar,
             .crossShowSyndication, .rediffConfirmed:
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
