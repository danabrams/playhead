// SpliceSlotShadow.swift
// playhead-xsdz.21 (Bead C): shadow instrumentation for the splice-slot
// OWNERSHIP pass. SELF-CONTAINED, side-effect-free primitives that turn the
// SAME pure `SpliceSlotDispositionEngine` dispositions the flag-ON path computes
// (bead B, playhead-xsdz.20) into per-span breadcrumbs + structured rows —
// WITHOUT applying any rewrite. `shadow == flag-ON` holds by construction: the
// shadow pass in `AdDetectionService` feeds these builders the exact
// `[SpliceSlotCandidate]` / `SpliceSlotDispositionResult` the ownership pass
// derives, so nothing here re-implements the pass policy.
//
// The reason enum, breadcrumb field order/spelling, slot-field sources,
// sentinels, and the `widthDeltaSec` pin are FROZEN (v3). See
// `SpliceSlotShadowBreadcrumb.format` and the unit tests that pin every branch.

import Foundation
import OSLog

// MARK: - Reason (frozen 13-value enum)

/// The FINAL splice-slot disposition for a span, as reported in the v3 shadow
/// breadcrumb `reason=` field. RawValues are the frozen wire tokens — do NOT
/// rename. Exactly one applies per span (reason precedence is already encoded in
/// the `SpliceSlotDisposition` the engine returns: a LATER pass overrides an
/// earlier one, and an enclosed loser becomes `.absorbed`).
enum SpliceSlotShadowReason: String, Sendable, Equatable, Codable, CaseIterable {
    /// A qualifying splice pair won the span's WIDTH (kept slot).
    case qualifying
    /// Resolver: core interval had zero or negative length (no-pair → sentinel).
    case degenerateCore
    /// Resolver: no geometry-valid pair at all (no-pair → sentinel).
    case noCandidatePairs
    /// Resolver: a pair existed but only failed the duration bound (no-pair → sentinel).
    case durationOutOfRange
    /// Resolver: champion had an edge below the splice-edge floor.
    case edgeBelowFloor
    /// Resolver: champion's (weaker-edge) slot confidence below floor.
    case slotConfidenceBelowFloor
    /// Resolver: champion covered less of the core than the minimum.
    case coreCoverageBelowMinimum
    /// Resolver: champion newly enclosed a vetoed range the core did not.
    case vetoNewlyEnclosed
    /// Engine: the resolved slot (or an enclosed bank-matched absorbee) tripped
    /// the negative-fingerprint bank — the slot is discarded.
    case negativeBankVeto
    /// Engine: the resolved slot lost a greedy collision to a higher-ranked slot
    /// AND its minted interval is NOT enclosed by any kept slot.
    case slotCollision
    /// Engine: the resolved slot partially overlapped a non-kept span's minted
    /// interval at the fixpoint and was demoted.
    case partialOverlapFallback
    /// Engine: the resolved slot's interval intersects zero atoms.
    case emptyAtomSet
    /// Engine: a non-kept span whose minted interval is ENCLOSED by a kept slot —
    /// its minted interval is consolidated into the absorbing slot.
    case absorbed
}

// MARK: - Row

/// One recorded span's shadow disposition. Carries exactly the fields the frozen
/// v3 breadcrumb prints, plus an optional decision-delta (populated only when the
/// slot-vs-minted arms were evaluated). Pure value type so the observer, the
/// formatter, and the projection tooling can all consume it directly.
struct SpliceSlotShadowRow: Sendable, Equatable, Codable {
    let assetId: String
    let spanId: String
    /// Post-decode (post Use-A-snap) minted interval start (bead B core pin).
    let mintedStart: Double
    let mintedEnd: Double
    /// `-1` on the no-pair sentinel rows; otherwise the sourced slot's start.
    let slotStart: Double
    let slotEnd: Double
    /// `(slotEnd − slotStart) − (mintedEnd − mintedStart)` when the slot fields
    /// are real; `0` on the sentinel rows.
    let widthDeltaSec: Double
    let startEdgeScore: Double
    let endEdgeScore: Double
    let coreCoverage: Double
    let qualified: Bool
    let reason: SpliceSlotShadowReason
    /// Slot-arm vs minted-arm decision delta. `nil` until the two arms are
    /// evaluated (dogfood-capture; see `SpliceSlotDecisionDelta`).
    let decisionDelta: SpliceSlotDecisionDelta?
}

// MARK: - Row builder (pure)

/// Maps engine dispositions + resolver diagnostics to frozen shadow rows.
/// Batch over the whole span set so `.absorbed` can look up the absorbing span's
/// slot internally. Never re-implements the pass policy — it only DESCRIBES the
/// `SpliceSlotDisposition` the shared engine already produced.
enum SpliceSlotShadowRowBuilder {

    /// Sentinel slot-field values for the no-pair reasons (degenerateCore /
    /// noCandidatePairs / durationOutOfRange), where no candidate slot exists.
    static let sentinelSlotStart = -1.0
    static let sentinelSlotEnd = -1.0

    /// Build one row per span. `spanIds`, `candidates`, `diagnostics`, and
    /// `dispositions` MUST all be index-aligned and the same length.
    static func makeRows(
        assetId: String,
        spanIds: [String],
        candidates: [SpliceSlotCandidate],
        diagnostics: [SpliceSlotDiagnostics],
        dispositions: [SpliceSlotDisposition],
        decisionDeltas: [SpliceSlotDecisionDelta?]? = nil
    ) -> [SpliceSlotShadowRow] {
        let n = dispositions.count
        precondition(
            spanIds.count == n && candidates.count == n && diagnostics.count == n,
            "shadow row builder inputs must be index-aligned"
        )
        if let decisionDeltas {
            precondition(decisionDeltas.count == n, "decisionDeltas must be index-aligned")
        }
        return (0..<n).map { i in
            makeRow(
                assetId: assetId,
                spanId: spanIds[i],
                spanIndex: i,
                mintedInterval: candidates[i].mintedInterval,
                disposition: dispositions[i],
                diagnostics: diagnostics[i],
                candidates: candidates,
                decisionDelta: decisionDeltas?[i]
            )
        }
    }

    /// Resolve a single span's `(reason, sourceSlot)`. `sourceSlot == nil` marks a
    /// sentinel row. Exposed for direct unit assertions.
    static func classify(
        disposition: SpliceSlotDisposition,
        diagnostics: SpliceSlotDiagnostics,
        candidates: [SpliceSlotCandidate],
        spanIndex: Int
    ) -> (reason: SpliceSlotShadowReason, sourceSlot: SpliceSlot?) {
        switch disposition {
        case .keepSlot(let slot):
            return (.qualifying, slot)

        case .absorbed(let absorberIndex):
            // Slot fields come from the ABSORBING span's kept slot.
            return (.absorbed, candidates[absorberIndex].slot)

        case .demoted(let demotion):
            let reason: SpliceSlotShadowReason
            switch demotion {
            case .negativeBankVeto, .absorbeeBankMatch:
                // The absorbee-bank all-or-nothing demotion surfaces as the same
                // negative-bank veto token as a direct slot/core bank hit.
                reason = .negativeBankVeto
            case .emptyAtomSet:
                reason = .emptyAtomSet
            case .greedyCollision:
                reason = .slotCollision
            case .mintedOverlap:
                reason = .partialOverlapFallback
            }
            // The span's own resolved-then-discarded slot.
            return (reason, candidates[spanIndex].slot)

        case .noSlot:
            switch diagnostics.failureReason {
            case .degenerateCore:
                return (.degenerateCore, nil)
            case .noCandidatePairs:
                return (.noCandidatePairs, nil)
            case .durationOutOfRange:
                return (.durationOutOfRange, nil)
            case .edgeBelowFloor:
                return (.edgeBelowFloor, diagnostics.bestGeometryValidPair)
            case .slotConfidenceBelowFloor:
                return (.slotConfidenceBelowFloor, diagnostics.bestGeometryValidPair)
            case .coreCoverageBelowMinimum:
                return (.coreCoverageBelowMinimum, diagnostics.bestGeometryValidPair)
            case .vetoNewlyEnclosed:
                return (.vetoNewlyEnclosed, diagnostics.bestGeometryValidPair)
            case nil:
                // Contract violation (a slot-less span must carry a failure
                // reason); fall back to the no-pair sentinel token defensively.
                return (.noCandidatePairs, nil)
            }
        }
    }

    private static func makeRow(
        assetId: String,
        spanId: String,
        spanIndex: Int,
        mintedInterval: TimeRange,
        disposition: SpliceSlotDisposition,
        diagnostics: SpliceSlotDiagnostics,
        candidates: [SpliceSlotCandidate],
        decisionDelta: SpliceSlotDecisionDelta?
    ) -> SpliceSlotShadowRow {
        let (reason, sourceSlot) = classify(
            disposition: disposition,
            diagnostics: diagnostics,
            candidates: candidates,
            spanIndex: spanIndex
        )

        let mintedWidth = mintedInterval.end - mintedInterval.start
        if let slot = sourceSlot {
            let slotWidth = slot.endTime - slot.startTime
            return SpliceSlotShadowRow(
                assetId: assetId,
                spanId: spanId,
                mintedStart: mintedInterval.start,
                mintedEnd: mintedInterval.end,
                slotStart: slot.startTime,
                slotEnd: slot.endTime,
                widthDeltaSec: slotWidth - mintedWidth,
                startEdgeScore: slot.startEdge.stepScore,
                endEdgeScore: slot.endEdge.stepScore,
                coreCoverage: slot.coreCoverage,
                qualified: reason == .qualifying,
                reason: reason,
                decisionDelta: decisionDelta
            )
        } else {
            // Sentinel row (no-pair reasons): pinned sentinels, widthDeltaSec 0.
            return SpliceSlotShadowRow(
                assetId: assetId,
                spanId: spanId,
                mintedStart: mintedInterval.start,
                mintedEnd: mintedInterval.end,
                slotStart: sentinelSlotStart,
                slotEnd: sentinelSlotEnd,
                widthDeltaSec: 0,
                startEdgeScore: 0,
                endEdgeScore: 0,
                coreCoverage: 0,
                qualified: false,
                reason: reason,
                decisionDelta: decisionDelta
            )
        }
    }
}

// MARK: - Breadcrumb formatter (frozen v3)

/// The FROZEN v3 shadow breadcrumb. Pure static formatter so the exact field
/// order/spelling is unit-testable without a live pipeline (precedent:
/// `FoundationModelClassifier.coarseRunBudgetBreadcrumb`). Field order:
///   spliceslot.shadow assetId= mintedStart= mintedEnd= slotStart= slotEnd=
///   widthDeltaSec= startEdgeScore= endEdgeScore= coreCoverage= qualified= reason=
enum SpliceSlotShadowBreadcrumb {

    /// Subsystem for the OSLog breadcrumb (precedent: `fm.coarse.run_budget`).
    static let subsystem = "com.playhead"

    static func format(_ row: SpliceSlotShadowRow) -> String {
        "spliceslot.shadow"
            + " assetId=\(row.assetId)"
            + " mintedStart=\(fmt(row.mintedStart))"
            + " mintedEnd=\(fmt(row.mintedEnd))"
            + " slotStart=\(fmt(row.slotStart))"
            + " slotEnd=\(fmt(row.slotEnd))"
            + " widthDeltaSec=\(fmt(row.widthDeltaSec))"
            + " startEdgeScore=\(fmt(row.startEdgeScore))"
            + " endEdgeScore=\(fmt(row.endEdgeScore))"
            + " coreCoverage=\(fmt(row.coreCoverage))"
            + " qualified=\(row.qualified)"
            + " reason=\(row.reason.rawValue)"
    }

    /// Deterministic numeric rendering: exact integers print without a decimal
    /// (so the sentinels read `slotStart=-1`, `widthDeltaSec=0`, scores `0`);
    /// fractional values print to three decimals.
    static func fmt(_ v: Double) -> String {
        if v.isFinite && v == v.rounded() && abs(v) < 1e15 {
            return String(Int(v))
        }
        return String(format: "%.3f", v)
    }
}

// MARK: - Decision delta (pure)

/// Per-span slot-arm vs minted-arm decision comparison. The slot arm takes NO
/// refiner adjustment and SUPPRESSES the `.audioForensics` entry (when audio
/// forensics is enabled); ledger mass is recorded BOTH with and without that
/// suppression so the readout can size the suppression's effect. The minted arm
/// is today's exact path.
struct SpliceSlotDecisionDelta: Sendable, Equatable, Codable {
    let slotLedgerMassWithSuppression: Double
    let slotLedgerMassWithoutSuppression: Double
    let slotDistinctKinds: Int
    let slotSkipConfidence: Double
    let mintedLedgerMass: Double
    let mintedDistinctKinds: Int
    let mintedSkipConfidence: Double
}

/// Pure computations over an evidence ledger. Freezes the "ledger mass" and
/// "distinctKinds" definitions and the `.audioForensics` suppression so the
/// dogfood-capture arms are unambiguous. Ledger mass and distinct-kind counts
/// use the SAME scoring filter the fusion / fragility geometry use
/// (`!isObservabilityOnly && weight > 0`).
enum SpliceSlotDecisionDeltaComputer {

    /// Sum of strictly-positive, scoring (non-observability) entry weights.
    static func ledgerMass(_ ledger: [EvidenceLedgerEntry]) -> Double {
        scoring(ledger).map(\.weight).reduce(0, +)
    }

    /// Count of distinct `EvidenceSourceType`s among scoring entries.
    static func distinctKinds(_ ledger: [EvidenceLedgerEntry]) -> Int {
        Set(scoring(ledger).map(\.source)).count
    }

    /// The ledger with every `.audioForensics` entry removed (slot-arm
    /// suppression). Slot width and the audio-forensics boundary entry derive
    /// from the SAME physical seam, so keeping both double-counts it.
    static func suppressingAudioForensics(_ ledger: [EvidenceLedgerEntry]) -> [EvidenceLedgerEntry] {
        ledger.filter { $0.source != .audioForensics }
    }

    /// Assemble a delta. `slotLedger` is the ledger built for the would-be slot
    /// interval (already refiner-free); the with/without-suppression masses are
    /// derived here so callers cannot diverge on the suppression definition.
    static func make(
        mintedLedger: [EvidenceLedgerEntry],
        mintedSkipConfidence: Double,
        slotLedger: [EvidenceLedgerEntry],
        slotSkipConfidence: Double
    ) -> SpliceSlotDecisionDelta {
        let suppressed = suppressingAudioForensics(slotLedger)
        return SpliceSlotDecisionDelta(
            slotLedgerMassWithSuppression: ledgerMass(suppressed),
            slotLedgerMassWithoutSuppression: ledgerMass(slotLedger),
            slotDistinctKinds: distinctKinds(suppressed),
            slotSkipConfidence: slotSkipConfidence,
            mintedLedgerMass: ledgerMass(mintedLedger),
            mintedDistinctKinds: distinctKinds(mintedLedger),
            mintedSkipConfidence: mintedSkipConfidence
        )
    }

    private static func scoring(_ ledger: [EvidenceLedgerEntry]) -> [EvidenceLedgerEntry] {
        ledger.filter { !$0.source.isObservabilityOnly && $0.weight > 0 }
    }
}

// MARK: - Projection (pure)

/// One span's projection input: its minted interval plus the shadow disposition
/// (reason + the would-be slot interval, `nil` on sentinel rows).
struct SpliceSlotProjectionInput: Sendable, Equatable {
    let mintedInterval: TimeRange
    let reason: SpliceSlotShadowReason
    /// The resolved would-be slot interval (`slotStart..slotEnd`), or `nil` on a
    /// sentinel row. Only consulted for `reason == .qualifying`.
    let wouldBeSlot: TimeRange?
}

/// Projects a baseline (minted) span set into the would-be TREATMENT set that a
/// flag-ON run would persist, applying the pinned SUBSTITUTION RULE:
///   • `qualifying`  → substitute the slot interval for the minted interval
///   • `absorbed`    → REMOVE the minted interval (consolidated into its absorber)
///   • everything else → keep the minted interval
/// Then reports pairwise disjointness of the treatment set under positive-
/// duration overlap semantics (touching endpoints are DISJOINT) — the flag-ON
/// composition guarantee the projection must reproduce.
enum SpliceSlotProjection {

    struct Result: Sendable, Equatable {
        let treatmentIntervals: [TimeRange]
        let disjoint: Bool
    }

    /// Convert recorded shadow rows into projection inputs. A row's would-be slot
    /// interval is real whenever its slot fields are non-sentinel
    /// (`slotStart >= 0`); the sentinel no-pair rows carry `nil`. Only
    /// `reason == .qualifying` consults it, but it is carried for every real row.
    static func inputs(from rows: [SpliceSlotShadowRow]) -> [SpliceSlotProjectionInput] {
        rows.map { row in
            let slot = row.slotStart >= 0
                ? TimeRange(start: row.slotStart, end: row.slotEnd)
                : nil
            return SpliceSlotProjectionInput(
                mintedInterval: TimeRange(start: row.mintedStart, end: row.mintedEnd),
                reason: row.reason,
                wouldBeSlot: slot
            )
        }
    }

    /// Project directly from recorded shadow rows (same-run dump convenience).
    static func project(from rows: [SpliceSlotShadowRow]) -> Result {
        project(inputs(from: rows))
    }

    static func project(_ inputs: [SpliceSlotProjectionInput]) -> Result {
        var out: [TimeRange] = []
        for input in inputs {
            switch input.reason {
            case .qualifying:
                // Substitute the slot interval; defensively keep minted if the
                // would-be slot is somehow absent.
                out.append(input.wouldBeSlot ?? input.mintedInterval)
            case .absorbed:
                continue // removed — consolidated into the absorbing slot
            default:
                out.append(input.mintedInterval) // keep minted
            }
        }
        return Result(treatmentIntervals: out, disjoint: pairwiseDisjoint(out))
    }

    /// Positive-duration pairwise disjointness (touching endpoints are disjoint).
    static func pairwiseDisjoint(_ intervals: [TimeRange]) -> Bool {
        for a in 0..<intervals.count {
            for b in (a + 1)..<intervals.count where intervals[a].intersects(intervals[b]) {
                return false
            }
        }
        return true
    }
}

// MARK: - Observer

/// Observation-only sink for shadow rows, mirroring the
/// `FragilityDiagnosticObserver` contract: compiled in all configurations,
/// NEVER fed back into the decision path, `nil` in production (PlayheadRuntime
/// never constructs one). The shadow pass logs the breadcrumb regardless; the
/// observer additionally accumulates the structured rows so tests (and the
/// dogfood-capture export) can read them back. Actor for safe cross-domain
/// access.
actor SpliceSlotShadowObserver {

    private let logger = Logger(
        subsystem: "com.playhead",
        category: "SpliceSlotShadowObserver"
    )

    private var rowsByAsset: [String: [SpliceSlotShadowRow]] = [:]

    init() {}

    func record(_ row: SpliceSlotShadowRow) {
        rowsByAsset[row.assetId, default: []].append(row)
    }

    func record(_ newRows: [SpliceSlotShadowRow], assetId: String) {
        rowsByAsset[assetId, default: []].append(contentsOf: newRows)
    }

    func rows(for assetId: String) -> [SpliceSlotShadowRow]? {
        rowsByAsset[assetId]
    }

    func recordCount(for assetId: String) -> Int {
        rowsByAsset[assetId]?.count ?? 0
    }
}
