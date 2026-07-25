// CorrectionAttribution.swift
// Phase EF2 (playhead-ef2.3.1): Causal source inference for user corrections.
//
// When a user corrects a region, CorrectionAttribution records what caused the
// error — which pipeline source was most responsible and what specific evidence
// contributed to the incorrect decision.
//
// Design:
//   - CorrectionType captures the semantic nature of the correction (FP/FN/boundary).
//   - CausalSource identifies the pipeline component most responsible for the error.
//   - CorrectionTargetRefs carries optional IDs for downstream analysis (fingerprint,
//     sponsor entity, etc.).
//   - inferCausalSource examines anchor provenance and evidence ledger entries to
//     determine which source to blame.

import Foundation

// MARK: - CorrectionType

/// The semantic nature of a user correction.
///
/// Serialized to/from rawValue (TEXT) for SQLite storage.
enum CorrectionType: String, Sendable, Codable, CaseIterable, Equatable {
    /// System flagged content as an ad, but it is not.
    case falsePositive
    /// System missed an ad that the user identified.
    case falseNegative
    /// The detected ad span starts before the actual ad begins.
    case startTooEarly
    /// The detected ad span starts after the actual ad begins.
    case startTooLate
    /// The detected ad span ends before the actual ad ends.
    case endTooEarly
    /// The detected ad span ends after the actual ad ends.
    case endTooLate
}

// MARK: - CausalSource

/// The pipeline component most likely responsible for the error that the user corrected.
///
/// Serialized to/from rawValue (TEXT) for SQLite storage.
enum CausalSource: String, Sendable, Codable, CaseIterable, Equatable {
    /// Lexical pattern matching (URL, promo code, CTA, disclosure phrases).
    case lexical
    /// Foundation model classifier.
    case foundationModel
    /// Ad-copy fingerprint matching.
    case fingerprint
    /// Music-bed bracket detection.
    case musicBracket
    /// Episode/feed metadata cues.
    case metadata
    /// Positional prior (ads tend to appear at episode start/end).
    case positionPrior
    /// Acoustic break detection.
    case acoustic
    /// playhead-b6jq PR 5: distilled on-device specialist host-read classifier.
    /// Present for forward-compat provenance + demotion-exemption: specialist
    /// marks are ALWAYS mark-only (never auto-skip), so — like `.foundationModel`
    /// and `.acoustic` — `.specialist` is deliberately ABSENT from the demotion
    /// rules (multiplier stays 1.0; nothing to demote for a signal that never
    /// auto-skips). The metadataSource→CausalSource attribution mapping in
    /// `UserCorrectionStore.recordVeto` is deferred (blueprint §9): a veto over a
    /// specialist AdWindow does not yet attribute to `.specialist` because it
    /// carries no `AnchorRef` provenance.
    case specialist
}

// MARK: - CorrectionTargetRefs

/// Lossless boundaries owned by an explicit banner-feedback receipt.
///
/// `CorrectionScope.exactTimeSpan` deliberately keeps its legacy
/// millisecond text representation because generic correction dedupe absorbs
/// small detector jitter. Banner transactions need a different invariant:
/// the persisted receipt must identify the exact material the user answered.
/// Storing the IEEE-754 bit patterns here preserves that identity without
/// changing the public/generic scope format.
struct ExactFeedbackSpan: Sendable, Codable, Equatable {
    let startTimeBitPattern: UInt64
    let endTimeBitPattern: UInt64

    init(startTime: Double, endTime: Double) {
        startTimeBitPattern = startTime.bitPattern
        endTimeBitPattern = endTime.bitPattern
    }

    var startTime: Double {
        Double(bitPattern: startTimeBitPattern)
    }

    var endTime: Double {
        Double(bitPattern: endTimeBitPattern)
    }

    func matches(startTime: Double, endTime: Double) -> Bool {
        startTimeBitPattern == startTime.bitPattern
            && endTimeBitPattern == endTime.bitPattern
    }
}

/// Optional references to specific evidence items involved in the corrected decision.
///
/// JSON-encoded for SQLite storage in the `targetRefsJSON` column.
struct CorrectionTargetRefs: Sendable, Codable, Equatable {
    /// Ad-window identity owned by an on-device feedback receipt.
    ///
    /// Diagnostic exporters use this only as an internal redaction join key;
    /// explicit banner receipts and this identifier never leave the device.
    var adWindowId: String?
    /// Every AdWindow row whose response-derived state was produced by the
    /// same explicit feedback transaction.
    ///
    /// Suggest-Yes retires the original producer row and inserts a promoted
    /// applied row. Export privacy must join against both identities; keeping
    /// the legacy singular field above preserves migration compatibility for
    /// already-persisted receipts.
    var adWindowIds: [String]?
    /// Response-independent producer row as it existed immediately before an
    /// explicit banner answer. This is private on-device transaction state:
    /// exporters consume it only to restore the detection projection and
    /// never serialize the receipt or this payload.
    var explicitFeedbackDetectionProjection:
        ExplicitFeedbackDetectionProjection?
    /// Exact displayed boundaries for an explicit feedback transaction.
    ///
    /// This field is private receipt state and follows the same no-egress
    /// contract as `explicitFeedbackDetectionProjection`.
    var exactFeedbackSpan: ExactFeedbackSpan?
    /// Atom ordinals that the correction targets.
    var atomIds: [Int]?
    /// Evidence reference identifiers (e.g. "[E0]", "[E3]").
    var evidenceRefs: [String]?
    /// Fingerprint ID if a fingerprint match contributed to the error.
    var fingerprintId: String?
    /// Podcast domain/feed identifier for show-level attribution.
    var domain: String?
    /// Sponsor entity name if the error involved a specific sponsor.
    var sponsorEntity: String?

    init(
        adWindowId: String? = nil,
        adWindowIds: [String]? = nil,
        explicitFeedbackDetectionProjection:
            ExplicitFeedbackDetectionProjection? = nil,
        exactFeedbackSpan: ExactFeedbackSpan? = nil,
        atomIds: [Int]? = nil,
        evidenceRefs: [String]? = nil,
        fingerprintId: String? = nil,
        domain: String? = nil,
        sponsorEntity: String? = nil
    ) {
        self.adWindowId = adWindowId
        self.adWindowIds = adWindowIds
        self.explicitFeedbackDetectionProjection =
            explicitFeedbackDetectionProjection
        self.exactFeedbackSpan = exactFeedbackSpan
        self.atomIds = atomIds
        self.evidenceRefs = evidenceRefs
        self.fingerprintId = fingerprintId
        self.domain = domain
        self.sponsorEntity = sponsorEntity
    }

    /// Canonical exact ownership for an explicit banner receipt.
    ///
    /// The singular field may repeat one value from `adWindowIds` because that
    /// is the backwards-compatible promoted-row marker used by Suggest-Yes.
    /// Duplicate values *inside* the plural list are malformed, as are empty
    /// identifiers or an empty union.
    var canonicalExplicitAdWindowIDs: [String]? {
        let plural = adWindowIds ?? []
        guard plural.allSatisfy({ !$0.isEmpty }),
              Set(plural).count == plural.count
        else {
            return nil
        }
        if let adWindowId, adWindowId.isEmpty {
            return nil
        }
        var ids = Set(plural)
        if let adWindowId {
            ids.insert(adWindowId)
        }
        guard !ids.isEmpty else { return nil }
        return ids.sorted()
    }
}

/// A complete copy of one detector-produced row before a private answer
/// mutates it. Keeping the original identity and diagnostics lets every
/// outward projection remain byte/count/shape-equivalent to the unanswered
/// state, including Suggest-Yes where persistence creates a promoted duplicate.
struct ExplicitFeedbackDetectionProjection:
    Sendable, Codable, Equatable
{
    let id: String
    let analysisAssetId: String
    let startTime: Double
    let endTime: Double
    let confidence: Double
    let boundaryState: String
    let decisionState: String
    let detectorVersion: String
    let advertiser: String?
    let product: String?
    let adDescription: String?
    let evidenceText: String?
    let evidenceStartTime: Double?
    let metadataSource: String
    let metadataConfidence: Double?
    let metadataPromptVersion: String?
    let wasSkipped: Bool
    let userDismissedBanner: Bool
    let evidenceSources: String?
    let eligibilityGate: String?
    let catalogStoreMatchSimilarity: Double?
    let startEdgeAnchor: String
    let endEdgeAnchor: String

    init(_ window: AdWindow) {
        id = window.id
        analysisAssetId = window.analysisAssetId
        startTime = window.startTime
        endTime = window.endTime
        confidence = window.confidence
        boundaryState = window.boundaryState
        decisionState = window.decisionState
        detectorVersion = window.detectorVersion
        advertiser = window.advertiser
        product = window.product
        adDescription = window.adDescription
        evidenceText = window.evidenceText
        evidenceStartTime = window.evidenceStartTime
        metadataSource = window.metadataSource
        metadataConfidence = window.metadataConfidence
        metadataPromptVersion = window.metadataPromptVersion
        wasSkipped = window.wasSkipped
        userDismissedBanner = window.userDismissedBanner
        evidenceSources = window.evidenceSources
        eligibilityGate = window.eligibilityGate
        catalogStoreMatchSimilarity =
            window.catalogStoreMatchSimilarity
        startEdgeAnchor = window.startEdgeAnchor
        endEdgeAnchor = window.endEdgeAnchor
    }

    func adWindow() -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: analysisAssetId,
            startTime: startTime,
            endTime: endTime,
            confidence: confidence,
            boundaryState: boundaryState,
            decisionState: decisionState,
            detectorVersion: detectorVersion,
            advertiser: advertiser,
            product: product,
            adDescription: adDescription,
            evidenceText: evidenceText,
            evidenceStartTime: evidenceStartTime,
            metadataSource: metadataSource,
            metadataConfidence: metadataConfidence,
            metadataPromptVersion: metadataPromptVersion,
            wasSkipped: wasSkipped,
            userDismissedBanner: userDismissedBanner,
            evidenceSources: evidenceSources,
            eligibilityGate: eligibilityGate,
            catalogStoreMatchSimilarity:
                catalogStoreMatchSimilarity,
            startEdgeAnchor: startEdgeAnchor,
            endEdgeAnchor: endEdgeAnchor
        )
    }
}

/// Durable, local-only snapshot of the complete outward asset projection
/// immediately before an asset's first explicit banner response.
///
/// This payload is deliberately asset-scoped rather than receipt-scoped:
/// later private learning may create, remove, or replace unrelated rows, so
/// reconstructing an unanswered export from the live table is not safe.
struct ExplicitFeedbackListenRewindProjection:
    Sendable, Codable, Equatable
{
    let analysisAssetId: String
    let windowId: String
    let podcastId: String
    let time: Double
    let createdAt: Double

    init(_ row: AdListenRewindRow) {
        analysisAssetId = row.analysisAssetId
        windowId = row.windowId
        podcastId = row.podcastId
        time = row.time
        createdAt = row.createdAt.timeIntervalSince1970
    }

    func row() -> AdListenRewindRow {
        AdListenRewindRow(
            analysisAssetId: analysisAssetId,
            windowId: windowId,
            podcastId: podcastId,
            time: time,
            createdAt: Date(timeIntervalSince1970: createdAt)
        )
    }
}

struct ExplicitFeedbackEgressBaselinePayload:
    Sendable, Codable, Equatable
{
    enum ProjectionState: String, Sendable, Codable {
        case captured
        case withheld
    }

    static let currentSchemaVersion = 1

    let schemaVersion: Int
    let analysisAssetId: String
    let projectionState: ProjectionState
    let confirmedAdCoverageEndTime: Double?
    let windows: [ExplicitFeedbackDetectionProjection]
    let decodedSpans: [DecodedSpan]
    let listenRewinds: [ExplicitFeedbackListenRewindProjection]

    private init(
        schemaVersion: Int,
        analysisAssetId: String,
        projectionState: ProjectionState,
        confirmedAdCoverageEndTime: Double?,
        windows: [ExplicitFeedbackDetectionProjection],
        decodedSpans: [DecodedSpan],
        listenRewinds: [ExplicitFeedbackListenRewindProjection]
    ) {
        self.schemaVersion = schemaVersion
        self.analysisAssetId = analysisAssetId
        self.projectionState = projectionState
        self.confirmedAdCoverageEndTime =
            confirmedAdCoverageEndTime
        self.windows = windows
        self.decodedSpans = decodedSpans
        self.listenRewinds = listenRewinds
    }

    init(
        analysisAssetId: String,
        confirmedAdCoverageEndTime: Double?,
        windows: [ExplicitFeedbackDetectionProjection],
        decodedSpans: [DecodedSpan],
        listenRewinds: [AdListenRewindRow]
    ) {
        schemaVersion = Self.currentSchemaVersion
        self.analysisAssetId = analysisAssetId
        projectionState = .captured
        self.confirmedAdCoverageEndTime =
            confirmedAdCoverageEndTime
        self.windows = windows
        self.decodedSpans = decodedSpans
        self.listenRewinds = listenRewinds.map(
            ExplicitFeedbackListenRewindProjection.init
        )
    }

    static func withheld(
        analysisAssetId: String
    ) -> Self {
        Self(
            schemaVersion: currentSchemaVersion,
            analysisAssetId: analysisAssetId,
            projectionState: .withheld,
            confirmedAdCoverageEndTime: nil,
            windows: [],
            decodedSpans: [],
            listenRewinds: []
        )
    }
}

/// Central privacy projection for every outward detection-row surface.
///
/// Explicit receipts and baseline metadata never leave the device. Once an
/// explicit receipt exists, the complete atomically captured pre-answer
/// projection is the only permissible source. A missing or ambiguous baseline
/// returns `nil`; callers must fail closed for that asset.
enum ExplicitBannerFeedbackPrivacy {
    /// Canonical complete public projection of a still-unanswered asset.
    ///
    /// Capture and no-feedback export share this path so the durable baseline
    /// is byte/row/count equivalent to what the asset exposed immediately
    /// before the first response.
    static func unansweredProjection(
        windows: [AdWindow]
    ) -> [AdWindow]? {
        var ids = Set<String>()
        guard windows.allSatisfy({
            validWindow($0)
                && ids.insert($0.id).inserted
        }) else {
            return nil
        }
        return sorted(windows.map(detectionOnlyWindow))
    }

    /// Validates a decoded durable baseline and restores its canonical
    /// AdWindow projection. Any malformed, cross-asset, duplicate, or
    /// non-canonical payload fails closed.
    static func validatedBaselineProjection(
        _ payload: ExplicitFeedbackEgressBaselinePayload,
        expectedAssetId: String
    ) -> [AdWindow]? {
        guard payload.schemaVersion
                == ExplicitFeedbackEgressBaselinePayload
                    .currentSchemaVersion,
              payload.analysisAssetId == expectedAssetId,
              payload.projectionState == .captured,
              (
                  payload.confirmedAdCoverageEndTime.map {
                      $0.isFinite && $0 >= 0
                  } ?? true
              ),
              !payload.windows.isEmpty
        else {
            return nil
        }
        let decoded = payload.windows.map { $0.adWindow() }
        guard let canonical = unansweredProjection(windows: decoded),
              canonical.allSatisfy({
                  $0.analysisAssetId == expectedAssetId
              }),
              canonical.map(ExplicitFeedbackDetectionProjection.init)
                == payload.windows
        else {
            return nil
        }
        return canonical
    }

    /// Validates and deterministically orders the decoded-span portion of an
    /// authenticated asset-wide baseline.
    static func canonicalDecodedSpans(
        _ spans: [DecodedSpan],
        expectedAssetId: String
    ) -> [DecodedSpan]? {
        var ids = Set<String>()
        guard !expectedAssetId.isEmpty,
              spans.allSatisfy({
                  !$0.id.isEmpty
                      && $0.assetId == expectedAssetId
                      && $0.firstAtomOrdinal >= 0
                      && $0.lastAtomOrdinal >= $0.firstAtomOrdinal
                      && $0.startTime.isFinite
                      && $0.endTime.isFinite
                      && $0.endTime > $0.startTime
                      && ids.insert($0.id).inserted
              }),
              (try? JSONEncoder().encode(spans)) != nil
        else {
            return nil
        }
        return spans.sorted {
            if $0.startTime != $1.startTime {
                return $0.startTime < $1.startTime
            }
            if $0.endTime != $1.endTime {
                return $0.endTime < $1.endTime
            }
            return $0.id < $1.id
        }
    }

    static func validatedBaselineDecodedSpans(
        _ payload: ExplicitFeedbackEgressBaselinePayload,
        expectedAssetId: String
    ) -> [DecodedSpan]? {
        guard payload.projectionState == .captured,
              let canonical = canonicalDecodedSpans(
                  payload.decodedSpans,
                  expectedAssetId: expectedAssetId
              ),
              canonical == payload.decodedSpans
        else {
            return nil
        }
        return canonical
    }

    /// Validates rewind identity and values against the already canonical
    /// frozen window set, then imposes a stable order independent of SQLite
    /// row order.
    static func canonicalListenRewinds(
        _ rows: [AdListenRewindRow],
        expectedAssetId: String,
        expectedWindowIDs: Set<String>
    ) -> [AdListenRewindRow]? {
        guard !expectedAssetId.isEmpty,
              !expectedWindowIDs.isEmpty || rows.isEmpty
        else {
            return nil
        }
        let projections = rows.map(
            ExplicitFeedbackListenRewindProjection.init
        )
        guard projections.allSatisfy({
            $0.analysisAssetId == expectedAssetId
                && !$0.analysisAssetId.isEmpty
                && !$0.windowId.isEmpty
                && expectedWindowIDs.contains($0.windowId)
                && !$0.podcastId.isEmpty
                && $0.time.isFinite
                && $0.time >= 0
                && $0.createdAt.isFinite
                && $0.createdAt >= 0
                && ExplicitFeedbackListenRewindProjection($0.row())
                    == $0
        }) else {
            return nil
        }
        return projections.sorted {
            if $0.time != $1.time {
                return $0.time < $1.time
            }
            if $0.createdAt != $1.createdAt {
                return $0.createdAt < $1.createdAt
            }
            if $0.windowId != $1.windowId {
                return $0.windowId < $1.windowId
            }
            if $0.podcastId != $1.podcastId {
                return $0.podcastId < $1.podcastId
            }
            return $0.analysisAssetId < $1.analysisAssetId
        }.map { $0.row() }
    }

    static func validatedBaselineListenRewinds(
        _ payload: ExplicitFeedbackEgressBaselinePayload,
        expectedAssetId: String,
        expectedWindowIDs: Set<String>
    ) -> [AdListenRewindRow]? {
        let decoded = payload.listenRewinds.map { $0.row() }
        guard payload.projectionState == .captured,
              let canonical = canonicalListenRewinds(
                  decoded,
                  expectedAssetId: expectedAssetId,
                  expectedWindowIDs: expectedWindowIDs
              ),
              canonical.map(
                  ExplicitFeedbackListenRewindProjection.init
              ) == payload.listenRewinds
        else {
            return nil
        }
        return canonical
    }

    static func responseIndependentProjection(
        windows: [AdWindow],
        corrections: [CorrectionEvent],
        frozenBaseline: [AdWindow]? = nil
    ) -> [AdWindow]? {
        let explicitEvents = corrections.filter(
            \.isPrivateExplicitFeedbackReceipt
        )
        if let frozenBaseline {
            guard let expectedAssetId =
                    frozenBaseline.first?.analysisAssetId,
                  !expectedAssetId.isEmpty,
                  frozenBaseline.allSatisfy({
                      $0.analysisAssetId == expectedAssetId
                  }),
                  explicitEvents.allSatisfy({
                      $0.analysisAssetId == expectedAssetId
                  })
            else {
                return nil
            }

            // Baseline existence is an irreversible privacy marker. Even if
            // receipts are later pruned or damaged, never fall back to live
            // private-learning-contaminated rows.
            return unansweredProjection(windows: frozenBaseline)
        }

        guard explicitEvents.isEmpty else { return nil }
        return unansweredProjection(windows: windows)
    }

    private static func validWindow(_ window: AdWindow) -> Bool {
        window.id.isEmpty == false
            && window.analysisAssetId.isEmpty == false
            && window.startTime.isFinite
            && window.endTime.isFinite
            && window.endTime > window.startTime
            && window.confidence.isFinite
            && (window.evidenceStartTime.map { $0.isFinite } ?? true)
            && (window.metadataConfidence.map { $0.isFinite } ?? true)
            && (window.catalogStoreMatchSimilarity.map { $0.isFinite }
                ?? true)
    }

    private static func sorted(_ windows: [AdWindow]) -> [AdWindow] {
        windows.sorted {
            if $0.startTime != $1.startTime {
                return $0.startTime < $1.startTime
            }
            if $0.endTime != $1.endTime {
                return $0.endTime < $1.endTime
            }
            return $0.id < $1.id
        }
    }

    /// Local playback/application fields are never detection diagnostics.
    /// Normalizing them for every row makes a pre-answer export invariant to
    /// the asynchronous applied-row write and gives all four answer routes the
    /// same response-independent baseline.
    private static func detectionOnlyWindow(_ window: AdWindow) -> AdWindow {
        let decisionState =
            window.decisionState == AdDecisionState.applied.rawValue
                || window.decisionState
                    == AdDecisionState.reverted.rawValue
            ? AdDecisionState.confirmed.rawValue
            : window.decisionState
        return AdWindow(
            id: window.id,
            analysisAssetId: window.analysisAssetId,
            startTime: window.startTime,
            endTime: window.endTime,
            confidence: window.confidence,
            boundaryState: window.boundaryState,
            decisionState: decisionState,
            detectorVersion: window.detectorVersion,
            advertiser: window.advertiser,
            product: window.product,
            adDescription: window.adDescription,
            evidenceText: window.evidenceText,
            evidenceStartTime: window.evidenceStartTime,
            metadataSource: window.metadataSource,
            metadataConfidence: window.metadataConfidence,
            metadataPromptVersion: window.metadataPromptVersion,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: window.evidenceSources,
            eligibilityGate: window.eligibilityGate,
            catalogStoreMatchSimilarity:
                window.catalogStoreMatchSimilarity,
            startEdgeAnchor: window.startEdgeAnchor,
            endEdgeAnchor: window.endEdgeAnchor
        )
    }
}

/// Deterministic material identity shared by banner emission and the durable
/// store predicates. The store must be able to recompute the card token from
/// the row it owns; an actor-only UUID cannot fence a same-ID replacement.
enum AdWindowMaterialIdentity {
    static func sameProducerRevision(
        _ lhs: AdWindow,
        _ rhs: AdWindow
    ) -> Bool {
        producerRevisionToken(lhs) == producerRevisionToken(rhs)
    }

    static func autoSkipToken(
        window: AdWindow,
        displayedStart: Double,
        displayedEnd: Double
    ) -> String {
        [
            producerRevisionToken(window),
            encoded(displayedStart),
            encoded(displayedEnd),
        ].joined(separator: "|")
    }

    static func suggestionToken(_ window: AdWindow) -> String {
        [
            producerRevisionToken(window),
            encoded(window.decisionState),
            window.wasSkipped ? "1" : "0",
            window.userDismissedBanner ? "1" : "0",
        ].joined(separator: "|")
    }

    private static func producerRevisionToken(
        _ window: AdWindow
    ) -> String {
        [
            encoded(window.id),
            encoded(window.analysisAssetId),
            encoded(window.startTime),
            encoded(window.endTime),
            encoded(window.confidence),
            encoded(window.boundaryState),
            encoded(window.detectorVersion),
            encoded(window.advertiser),
            encoded(window.product),
            encoded(window.adDescription),
            encoded(window.evidenceText),
            encoded(window.evidenceStartTime),
            encoded(window.metadataSource),
            encoded(window.metadataConfidence),
            encoded(window.metadataPromptVersion),
            encoded(window.evidenceSources),
            encoded(window.eligibilityGate),
            encoded(window.catalogStoreMatchSimilarity),
            encoded(window.startEdgeAnchor),
            encoded(window.endEdgeAnchor),
        ].joined(separator: "|")
    }

    private static func encoded(_ value: String) -> String {
        "\(value.utf8.count):\(value)"
    }

    private static func encoded(_ value: String?) -> String {
        value.map(encoded) ?? "nil"
    }

    private static func encoded(_ value: Double) -> String {
        String(value.bitPattern)
    }

    private static func encoded(_ value: Double?) -> String {
        value.map(encoded) ?? "nil"
    }
}

// MARK: - CorrectionAttribution

/// Full attribution for a user correction: what went wrong and which pipeline
/// component was most responsible.
struct CorrectionAttribution: Sendable, Equatable {
    let correctionType: CorrectionType
    let causalSource: CausalSource
    let targetRefs: CorrectionTargetRefs?
}

// MARK: - Causal Inference

/// Namespace for causal inference logic. Groups `inferCausalSource` and
/// `buildTargetRefs` to avoid global namespace pollution.
enum CausalInference {

    /// Infer the most likely causal source from a span's anchor provenance and
    /// the evidence ledger entries that contributed to the decision.
    ///
    /// Algorithm (ledger-based, when ledger is non-empty):
    ///   1. Compute per-source total weight.
    ///   2. If the top source is lexical, return .lexical.
    ///   3. If FM weight > 0.3 of total weight, return .foundationModel.
    ///   4. If the top source is fingerprint, return .fingerprint.
    ///   5. Otherwise, return the highest-weight source mapped to CausalSource.
    ///
    /// Falls back to provenance-only inference when the ledger is empty.
    static func inferCausalSource(
        provenance: [AnchorRef],
        ledgerEntries: [EvidenceLedgerEntry]
    ) -> CausalSource {
        let scoringLedger = ledgerEntries.filter { !$0.source.isObservabilityOnly }
        // If we have scoring ledger entries, use weight-based inference.
        if !scoringLedger.isEmpty {
            // Accumulate total weight per source type.
            var weightBySource: [EvidenceSourceType: Double] = [:]
            for entry in scoringLedger {
                weightBySource[entry.source, default: 0] += entry.weight
            }

            let totalWeight = weightBySource.values.reduce(0, +)
            guard totalWeight > 0 else {
                return inferFromProvenance(provenance)
            }

            // Find the source with the highest weight.
            // Tie-break by rawValue for deterministic ordering when weights are equal.
            let sorted = weightBySource.sorted {
                if $0.value != $1.value { return $0.value > $1.value }
                return $0.key.rawValue < $1.key.rawValue
            }
            let topSource = sorted[0].key

            // Rule 1: Top source is lexical.
            if topSource == .lexical {
                return .lexical
            }

            // Rule 2: FM weight > 0.3 of total.
            let fmWeight = weightBySource[.fm] ?? 0
            if fmWeight / totalWeight > 0.3 {
                return .foundationModel
            }

            // Rule 3: Fingerprint source present with highest weight.
            if topSource == .fingerprint {
                return .fingerprint
            }

            // Rule 4: Map the highest-weight source to CausalSource.
            return mapSourceType(topSource)
        }

        // Ledger is empty — fall back to provenance-only inference.
        return inferFromProvenance(provenance)
    }

    /// Map an EvidenceSourceType to the corresponding CausalSource.
    private static func mapSourceType(_ source: EvidenceSourceType) -> CausalSource {
        switch source {
        case .fm:          return .foundationModel
        case .lexical:     return .lexical
        case .lexicalAutoAd: return .lexical  // playhead-xsdz.1: high-precision lexical auto-ad rule is a lexical-family signal
        case .acoustic:    return .acoustic
        case .musicBed:    return .acoustic  // music-bed coverage is an acoustic-family signal
        case .breakAlignment: return .acoustic  // playhead-fqc8: break-alignment is an acoustic-family corroborator
        case .audioForensics: return .acoustic  // playhead-xsdz.8: composite boundary discontinuity is an audio-derived corroborator
        case .catalog:     return .lexical  // catalog entries are lexical matches
        case .classifier:  return .foundationModel  // legacy classifier ≈ FM
        case .fingerprint: return .fingerprint
        case .crossEpisodeMemory: return .fingerprint  // playhead-xsdz.9: cross-episode copy-alignment is a reference-match signal, same causal source as fingerprint
        case .rhetoricalGrammar: return .lexical  // playhead-xsdz.12: the rhetorical act-sequence grammar is a text-derived (transcript-prose) signal, same causal source as lexical
        case .crossShowSyndication: return .fingerprint  // playhead-xsdz.13: cross-show syndication is a cross-library reference-match signal, same causal source as fingerprint / crossEpisodeMemory
        case .rediffConfirmed: return .fingerprint  // playhead-xsdz.62: byte-exact rediff confirmation is a deterministic cross-fetch reference-match, same causal source as fingerprint / crossEpisodeMemory / crossShowSyndication
        case .fusedScore:  return .foundationModel  // fused aggregate ≈ FM pipeline
        case .metadata:    return .lexical  // playhead-z3ch: metadata cues are lexical-family pre-seeds
        case .audit:       return .foundationModel  // Phase 11 audit metadata, not a skip signal
        case .operational: return .foundationModel  // Phase 11 operational metadata, not a skip signal
        }
    }

    /// Infer causal source from anchor provenance alone (no ledger entries).
    private static func inferFromProvenance(_ provenance: [AnchorRef]) -> CausalSource {
        // Count anchor ref types.
        var fmCount = 0
        var evidenceCatalogCount = 0
        var acousticCount = 0

        var classifierCount = 0

        for ref in provenance {
            switch ref {
            case .fmConsensus:
                fmCount += 1
            case .evidenceCatalog:
                evidenceCatalogCount += 1
            case .fmAcousticCorroborated:
                fmCount += 1
                acousticCount += 1
            case .userCorrection:
                // User corrections are attribution-neutral — they indicate the
                // user flagged a region, not a specific pipeline source.
                break
            case .classifierSeed:
                classifierCount += 1
            case .sustainedMusicOffset:
                // Sustained-music-offset is an acoustic-music PRESENCE signal
                // (playhead-t1py) — attribute a correction on it to the
                // acoustic source, like its RMS-drop `.acoustic` siblings.
                acousticCount += 1
            case .spliceSlot:
                // Width-ownership marker (playhead-xsdz.22) — attribution-neutral,
                // like `.userCorrection`. It carries no causal-source signal, so
                // it contributes to none of the counts below.
                break
            case .rediffSlot:
                // Width-ownership marker (playhead-xsdz.29) — attribution-neutral,
                // like `.spliceSlot`. It sets WIDTH, not PRESENCE, so it carries
                // no causal-source signal and contributes to none of the counts.
                break
            }
        }

        // Prefer evidence catalog (lexical) if present, then FM, then acoustic.
        // Classifier-seeded spans attribute to `.foundationModel` — this
        // mirrors `mapSourceType`'s treatment of the `.classifier` ledger
        // source (\"legacy classifier ≈ FM\") because `CausalSource` has no
        // distinct `.classifier` case. Decision-log breakdown still surfaces
        // the classifier contribution separately from the ledger.
        if evidenceCatalogCount > 0 { return .lexical }
        if fmCount > 0 { return .foundationModel }
        if acousticCount > 0 { return .acoustic }
        if classifierCount > 0 { return .foundationModel }

        // No provenance at all — default to FM as the most common source.
        return .foundationModel
    }

    /// Build a CorrectionTargetRefs from a span's provenance and optional overrides.
    ///
    /// The `ledgerEntries` parameter is reserved for future use (e.g. extracting
    /// fingerprint IDs from ledger details); currently only provenance is inspected.
    static func buildTargetRefs(
        provenance: [AnchorRef],
        ledgerEntries: [EvidenceLedgerEntry],
        fingerprintId: String? = nil,
        domain: String? = nil,
        sponsorEntity: String? = nil
    ) -> CorrectionTargetRefs? {
        // Extract atom ordinals from evidence catalog entries.
        let atomIds: [Int] = provenance.compactMap { ref in
            if case .evidenceCatalog(let entry) = ref {
                return entry.atomOrdinal
            }
            return nil
        }

        // Extract evidence refs from evidence catalog entries.
        let evidenceRefs: [String] = provenance.compactMap { ref in
            if case .evidenceCatalog(let entry) = ref {
                return "[E\(entry.evidenceRef)]"
            }
            return nil
        }

        // Extract sponsor entity from brandSpan evidence.
        let inferredSponsor = sponsorEntity ?? provenance.compactMap { ref -> String? in
            if case .evidenceCatalog(let entry) = ref, entry.category == .brandSpan {
                return entry.normalizedText
            }
            return nil
        }.first

        let refs = CorrectionTargetRefs(
            atomIds: atomIds.isEmpty ? nil : atomIds,
            evidenceRefs: evidenceRefs.isEmpty ? nil : evidenceRefs,
            fingerprintId: fingerprintId,
            domain: domain,
            sponsorEntity: inferredSponsor
        )

        // Return nil if all fields are nil (no useful refs to store).
        if refs.atomIds == nil && refs.evidenceRefs == nil &&
           refs.fingerprintId == nil && refs.domain == nil && refs.sponsorEntity == nil {
            return nil
        }
        return refs
    }
}
