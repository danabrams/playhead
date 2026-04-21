// UserCorrectionStore.swift
// Phase 7 (playhead-4my.7.1): Protocol + persistent implementation for
// user correction gestures on detected ad spans.
//
// Design:
//   • CorrectionScope encodes the semantic level at which a veto applies:
//     exact span, sponsor on show, phrase on show, campaign on show.
//   • correctionDecayWeight(ageDays:) gives older corrections less influence.
//   • PersistentUserCorrectionStore persists events via AnalysisStore and
//     exposes weighted/filtered query APIs for Phase 7.2 orchestrator wiring.

import CryptoKit
import Foundation
import OSLog

// MARK: - CorrectionScope

/// Semantic scope at which a user correction applies.
///
/// Serialized to/from a colon-delimited string for SQLite storage:
///   exactSpan:assetId:firstOrdinal:lastOrdinal
///   exactTimeSpan:assetId:startTimeSeconds:endTimeSeconds
///   sponsorOnShow:podcastId:sponsor
///   phraseOnShow:podcastId:phrase
///   campaignOnShow:podcastId:campaign
enum CorrectionScope: Sendable, Equatable {
    /// Exact span veto: the specific atom range in a specific asset.
    case exactSpan(assetId: String, ordinalRange: ClosedRange<Int>)
    /// Exact time-range veto: a specific `[startTime, endTime]` range in an asset.
    ///
    /// Prefer `.exactSpan` whenever the caller holds a `DecodedSpan` with real
    /// atom ordinals (the backfill path). Use `.exactTimeSpan` only when the UI
    /// has precise time boundaries (from `BoundaryExpander`, transcript
    /// selection, or `AdWindow.snappedStart`/`.snappedEnd`) but no corresponding
    /// atom ordinals at persistence time. Previously these sites fell back to
    /// `.exactSpan(ordinalRange: 0...Int.max)`, which collapsed window-level
    /// corrections to whole-episode vetoes and made per-window metrics
    /// (precision, recall, IoU) impossible.
    ///
    /// Serialized times use fixed 3-decimal precision to avoid floating-point
    /// representation drift across round-trips.
    case exactTimeSpan(assetId: String, startTime: Double, endTime: Double)
    /// Sponsor veto across all episodes of a podcast.
    case sponsorOnShow(podcastId: String, sponsor: String)
    /// Phrase veto across all episodes of a podcast.
    case phraseOnShow(podcastId: String, phrase: String)
    /// Campaign veto across all episodes of a podcast.
    case campaignOnShow(podcastId: String, campaign: String)
    /// Domain ownership veto: the podcast owns this domain (e.g. "nytimes.com"),
    /// so mentions of it are not third-party ads. Layer B scope.
    case domainOwnershipOnShow(podcastId: String, domain: String)
    /// Jingle fingerprint veto: a recurring audio jingle on this show is not an
    /// ad indicator. Layer B scope.
    case jingleOnShow(podcastId: String, jingleId: String)

    // MARK: Serialization

    /// Serialize to the storage string format.
    var serialized: String {
        switch self {
        case .exactSpan(let assetId, let range):
            return "exactSpan:\(assetId):\(range.lowerBound):\(range.upperBound)"
        case .exactTimeSpan(let assetId, let startTime, let endTime):
            // Fixed 3-decimal precision avoids FP drift over serialize/deserialize
            // round-trips; mirrors the existing pattern in TranscriptPeekView
            // which formats veto IDs as "%.3f-%.3f".
            let startStr = String(format: "%.3f", startTime)
            let endStr = String(format: "%.3f", endTime)
            return "exactTimeSpan:\(assetId):\(startStr):\(endStr)"
        case .sponsorOnShow(let podcastId, let sponsor):
            return "sponsorOnShow:\(podcastId):\(sponsor)"
        case .phraseOnShow(let podcastId, let phrase):
            return "phraseOnShow:\(podcastId):\(phrase)"
        case .campaignOnShow(let podcastId, let campaign):
            return "campaignOnShow:\(podcastId):\(campaign)"
        case .domainOwnershipOnShow(let podcastId, let domain):
            return "domainOwnershipOnShow:\(podcastId):\(domain)"
        case .jingleOnShow(let podcastId, let jingleId):
            return "jingleOnShow:\(podcastId):\(jingleId)"
        }
    }

    /// Deserialize from a storage string produced by `serialized`.
    /// Returns `nil` when the string is malformed or has an unknown prefix.
    ///
    /// Uses case-specific splitting to handle colons within field values
    /// (e.g. sponsors like "Squarespace: Build It", URLs in phrases, compound
    /// campaign names). The type prefix is extracted first; then the remainder
    /// is split with the minimum number of splits needed for each case.
    ///
    /// Contract: the `.exactTimeSpan` parser depends on `serialized` using
    /// non-localized fixed-point decimal output (`String(format: "%.3f", …)`)
    /// for startTime/endTime. If you change the formatter (e.g. scientific
    /// notation, localized decimals, variable precision), update the parser
    /// so the "last two colon-separated tokens are the times" invariant still
    /// holds.
    static func deserialize(_ string: String) -> CorrectionScope? {
        guard let typeEnd = string.firstIndex(of: ":") else { return nil }
        let typeStr = String(string[string.startIndex..<typeEnd])
        let remainder = String(string[string.index(after: typeEnd)...])

        switch typeStr {
        case "exactSpan":
            // remainder = "assetId:lower:upper"
            // assetId may itself contain colons; ordinals are the last two colon-separated parts.
            let parts = remainder.split(separator: ":", maxSplits: Int.max, omittingEmptySubsequences: false)
                .map(String.init)
            guard parts.count >= 3,
                  let lower = Int(parts[parts.count - 2]),
                  let upper = Int(parts[parts.count - 1]) else { return nil }
            let assetId = parts[0..<(parts.count - 2)].joined(separator: ":")
            return .exactSpan(assetId: assetId, ordinalRange: lower...upper)
        case "exactTimeSpan":
            // remainder = "assetId:startTime:endTime"
            // assetId may itself contain colons; the times are the last two parts.
            let parts = remainder.split(separator: ":", maxSplits: Int.max, omittingEmptySubsequences: false)
                .map(String.init)
            guard parts.count >= 3,
                  let startTime = Double(parts[parts.count - 2]),
                  let endTime = Double(parts[parts.count - 1]) else { return nil }
            let assetId = parts[0..<(parts.count - 2)].joined(separator: ":")
            return .exactTimeSpan(assetId: assetId, startTime: startTime, endTime: endTime)
        case "sponsorOnShow":
            // remainder = "podcastId:sponsor" — split on first colon only so sponsor may contain colons.
            guard let sep = remainder.firstIndex(of: ":") else { return nil }
            let podcastId = String(remainder[remainder.startIndex..<sep])
            let sponsor = String(remainder[remainder.index(after: sep)...])
            return .sponsorOnShow(podcastId: podcastId, sponsor: sponsor)
        case "phraseOnShow":
            guard let sep = remainder.firstIndex(of: ":") else { return nil }
            let podcastId = String(remainder[remainder.startIndex..<sep])
            let phrase = String(remainder[remainder.index(after: sep)...])
            return .phraseOnShow(podcastId: podcastId, phrase: phrase)
        case "campaignOnShow":
            guard let sep = remainder.firstIndex(of: ":") else { return nil }
            let podcastId = String(remainder[remainder.startIndex..<sep])
            let campaign = String(remainder[remainder.index(after: sep)...])
            return .campaignOnShow(podcastId: podcastId, campaign: campaign)
        case "domainOwnershipOnShow":
            guard let sep = remainder.firstIndex(of: ":") else { return nil }
            let podcastId = String(remainder[remainder.startIndex..<sep])
            let domain = String(remainder[remainder.index(after: sep)...])
            return .domainOwnershipOnShow(podcastId: podcastId, domain: domain)
        case "jingleOnShow":
            guard let sep = remainder.firstIndex(of: ":") else { return nil }
            let podcastId = String(remainder[remainder.startIndex..<sep])
            let jingleId = String(remainder[remainder.index(after: sep)...])
            return .jingleOnShow(podcastId: podcastId, jingleId: jingleId)
        default:
            return nil
        }
    }

    // MARK: - Layer B Mapping

    /// Returns the corresponding `BroadCorrectionScope` for Layer B scopes,
    /// or `nil` for Layer A scopes (exactSpan, exactTimeSpan, campaignOnShow).
    var broadScope: BroadCorrectionScope? {
        switch self {
        case .phraseOnShow:            return .phraseOnShow
        case .sponsorOnShow:           return .sponsorOnShow
        case .domainOwnershipOnShow:   return .domainOwnershipOnShow
        case .jingleOnShow:            return .jingleOnShow
        case .exactSpan, .exactTimeSpan, .campaignOnShow:
            return nil
        }
    }
}

// MARK: - Decay Weight

/// Compute a decay weight for a correction based on how many days ago it was made.
///
/// Returns 1.0 at 0 days, decays linearly to 0.1 at 180 days, and is clamped
/// to a minimum of 0.1 for corrections older than 180 days.
///
///   weight = min(1.0, max(0.1, 1.0 - (ageDays / 180.0)))
///
/// Result is clamped to [0.1, 1.0]. Negative ageDays (future-dated corrections
/// from clock skew) are clamped to 1.0 rather than exceeding it.
func correctionDecayWeight(ageDays: Double) -> Double {
    min(1.0, max(0.1, 1.0 - (ageDays / 180.0)))
}

// MARK: - DecodedSpan + ordinalRange helper

extension DecodedSpan {
    /// Full atom ordinal range of the span, used for conservative (wider)
    /// veto coverage. Per-atom time mapping is not available on DecodedSpan.
    var fullOrdinalRange: ClosedRange<Int> {
        firstAtomOrdinal...lastAtomOrdinal
    }
}

// MARK: - UserCorrectionStore Protocol

/// Stores user corrections (vetoes) against detected ad spans.
protocol UserCorrectionStore: Sendable {
    /// Record that the user vetoed a decoded span as not-an-ad.
    ///
    /// Implementations should infer one or more `CorrectionScope` values from
    /// the span's metadata and persist a `CorrectionEvent` for each.
    func recordVeto(span: DecodedSpan) async

    /// Record that the user vetoed an `[startTime, endTime]` range as not-an-ad.
    ///
    /// Used by UI paths that have precise time boundaries but no atom ordinals
    /// (transcript selection, banner taps, orchestrator revert). Persists an
    /// `.exactTimeSpan` correction scope rather than the coarse
    /// `.exactSpan(ordinalRange: 0...Int.max)` fallback that preceded this API.
    ///
    /// The signature takes `startTime` / `endTime` as separate parameters
    /// rather than a `ClosedRange<Double>` so call sites don't construct a
    /// range via `start...end` — that operator traps on `start > end` or
    /// non-finite bounds, moving the crash surface up into the callers.
    /// Implementations are responsible for clamping / rejecting as needed.
    ///
    /// Limitation: unlike `recordVeto(span:)`, this entry point does not
    /// populate `causalSource` or `targetRefs` on the persisted event — the
    /// anchor provenance / evidence ledger that drive `CausalInference` are
    /// not carried through the UI gesture boundary. Per-window metrics work
    /// (scope is preserved); causal attribution does not. Callers with access
    /// to anchor provenance should prefer `recordVeto(span:)`.
    func recordVeto(
        startTime: Double,
        endTime: Double,
        assetId: String,
        podcastId: String?,
        source: CorrectionSource
    ) async

    /// Append a fully-formed correction event to the store.
    func record(_ event: CorrectionEvent) async throws

    /// Phase 7.2: Return an aggregate correction suppression factor for the
    /// given analysis asset. The factor is in [0.0, 1.0]:
    ///   - 1.0 means no active corrections (no suppression)
    ///   - < 1.0 means one or more corrections exist; the minimum decay-weighted
    ///     correction weight from the store reduces effective confidence.
    ///
    /// Callers (AdDetectionService) pre-compute this value from an actor context
    /// and pass it to the pure-value `DecisionMapper` to avoid making the
    /// mapper async.
    func correctionPassthroughFactor(for analysisAssetId: String) async -> Double

    /// Return an aggregate correction boost factor for the given analysis asset,
    /// derived from false negative (missed ad) reports. The factor is in [1.0, 2.0]:
    ///   - 1.0 means no active false negative corrections (no boost)
    ///   - > 1.0 means one or more false negative reports exist; the strongest
    ///     decay-weighted correction boosts effective confidence for nearby windows.
    ///
    /// Counterpart to `correctionPassthroughFactor` — where passthrough suppresses,
    /// boost amplifies.
    func correctionBoostFactor(for analysisAssetId: String) async -> Double
}

// MARK: - NoOpUserCorrectionStore

/// No-op implementation. Discards all corrections silently.
/// Used in release builds and contexts that have not yet wired up persistence.
struct NoOpUserCorrectionStore: UserCorrectionStore {
    func recordVeto(span: DecodedSpan) async {
        // No-op.
    }

    func recordVeto(
        startTime: Double,
        endTime: Double,
        assetId: String,
        podcastId: String?,
        source: CorrectionSource
    ) async {
        // No-op.
    }

    func record(_ event: CorrectionEvent) async throws {
        // No-op.
    }

    func correctionPassthroughFactor(for analysisAssetId: String) async -> Double {
        // No active corrections — no suppression.
        return 1.0
    }

    func correctionBoostFactor(for analysisAssetId: String) async -> Double {
        // No active false negative corrections — no boost.
        return 1.0
    }
}

// MARK: - PersistentUserCorrectionStore

/// Actor-backed user correction store that persists events via `AnalysisStore`.
actor PersistentUserCorrectionStore: UserCorrectionStore {

    private let store: AnalysisStore
    private let logger = Logger(subsystem: "com.playhead", category: "UserCorrectionStore")

    init(store: AnalysisStore) {
        self.store = store
    }

    // MARK: - UserCorrectionStore

    /// Record a veto from a user gesture on a decoded span.
    ///
    /// Always records an `exactSpan` scope. If the span's `anchorProvenance`
    /// contains an `evidenceCatalog` entry with category `.brandSpan`, also
    /// records a `sponsorOnShow` scope using the matched text as the sponsor
    /// name and the span's `assetId` as a proxy for show identity (the
    /// podcastId is not carried on DecodedSpan; Phase 7.2 can supply it via
    /// `record(_:)` directly when available).
    func recordVeto(span: DecodedSpan) async {
        await recordVeto(span: span, ledgerEntries: [])
    }

    /// Record a veto with optional evidence ledger entries for causal attribution.
    func recordVeto(span: DecodedSpan, ledgerEntries: [EvidenceLedgerEntry]) async {
        let ordinals = span.fullOrdinalRange
        let now = Date().timeIntervalSince1970

        // ef2.3.1: Infer causal source from provenance + ledger.
        let causalSource = CausalInference.inferCausalSource(
            provenance: span.anchorProvenance,
            ledgerEntries: ledgerEntries
        )
        let targetRefs = CausalInference.buildTargetRefs(
            provenance: span.anchorProvenance,
            ledgerEntries: ledgerEntries
        )

        // Always record the exact span scope.
        let exactScope = CorrectionScope.exactSpan(
            assetId: span.assetId,
            ordinalRange: ordinals
        )
        let exactEvent = CorrectionEvent(
            analysisAssetId: span.assetId,
            scope: exactScope.serialized,
            createdAt: now,
            source: .manualVeto,
            podcastId: nil,
            correctionType: .falsePositive,
            causalSource: causalSource,
            targetRefs: targetRefs
        )

        do {
            try await record(exactEvent)
        } catch {
            logger.error(
                "recordVeto: failed to persist exactSpan event for asset \(span.assetId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
        }

        // If the span has brandSpan evidence, also record a sponsorOnShow scope.
        let brandEntries = span.anchorProvenance.compactMap { ref -> EvidenceEntry? in
            guard case .evidenceCatalog(let entry) = ref, entry.category == .brandSpan else {
                return nil
            }
            return entry
        }
        for entry in brandEntries {
            let sponsorScope = CorrectionScope.sponsorOnShow(
                podcastId: span.assetId,  // best available show-level key without podcastId
                sponsor: entry.normalizedText
            )
            let sponsorEvent = CorrectionEvent(
                analysisAssetId: span.assetId,
                scope: sponsorScope.serialized,
                createdAt: now,
                source: .manualVeto,
                podcastId: nil,
                correctionType: .falsePositive,
                causalSource: causalSource,
                targetRefs: CorrectionTargetRefs(sponsorEntity: entry.normalizedText)
            )
            do {
                try await record(sponsorEvent)
            } catch {
                logger.error(
                    "recordVeto: failed to persist sponsorOnShow event for asset \(span.assetId, privacy: .public) sponsor \(entry.normalizedText, privacy: .public): \(error.localizedDescription, privacy: .public)"
                )
            }
        }
    }

    /// Record a veto from a user gesture that carries a precise `[startTime, endTime]`
    /// range but no atom ordinals.
    ///
    /// Persists a single `.exactTimeSpan` correction event. The correction type
    /// (false positive vs false negative) is derived from `source.kind` so the
    /// resulting event participates correctly in passthrough / boost aggregation.
    ///
    /// Non-finite bounds (NaN / ±Infinity) are rejected and logged — they would
    /// otherwise serialize to "nan"/"inf" strings and round-trip back into
    /// unsafe downstream `ClosedRange<Double>` construction.
    ///
    /// Inverted ranges (`endTime < startTime`) are silently clamped via
    /// `min`/`max` so an upstream bug does not corrupt storage. See the
    /// protocol doccomment for why the signature takes two Doubles rather
    /// than a `ClosedRange`.
    func recordVeto(
        startTime: Double,
        endTime: Double,
        assetId: String,
        podcastId: String?,
        source: CorrectionSource
    ) async {
        guard startTime.isFinite, endTime.isFinite else {
            logger.warning(
                "recordVeto(startTime:endTime:): rejecting non-finite range [\(startTime), \(endTime)] for asset \(assetId, privacy: .public)"
            )
            return
        }
        let clampedStart = Swift.min(startTime, endTime)
        let clampedEnd = Swift.max(startTime, endTime)
        if clampedStart != startTime || clampedEnd != endTime {
            logger.warning(
                "recordVeto(startTime:endTime:): inverted range [\(startTime), \(endTime)] clamped to [\(clampedStart), \(clampedEnd)] for asset \(assetId, privacy: .public)"
            )
        }
        let scope = CorrectionScope.exactTimeSpan(
            assetId: assetId,
            startTime: clampedStart,
            endTime: clampedEnd
        )
        let event = CorrectionEvent(
            analysisAssetId: assetId,
            scope: scope.serialized,
            createdAt: Date().timeIntervalSince1970,
            source: source,
            podcastId: podcastId,
            correctionType: source.kind.correctionType
        )
        do {
            try await record(event)
        } catch {
            logger.error(
                "recordVeto(startTime:endTime:): failed to persist exactTimeSpan event for asset \(assetId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
        }
    }

    /// Persist a fully-formed correction event.
    func record(_ event: CorrectionEvent) async throws {
        try await store.appendCorrectionEvent(event)
    }

    // MARK: - Protocol: correctionPassthroughFactor

    /// Returns the minimum decay-weighted correction factor for the given asset.
    ///
    /// If no corrections exist, returns 1.0 (no suppression). If corrections exist,
    /// returns `1.0 - maxWeight` where maxWeight is the highest decay-weighted
    /// correction seen — i.e. the most-recent correction has the most influence.
    /// Result is clamped to [0.0, 1.0].
    func correctionPassthroughFactor(for analysisAssetId: String) async -> Double {
        let weighted: [(CorrectionEvent, Double)]
        do {
            weighted = try await weightedCorrections(for: analysisAssetId)
        } catch {
            logger.warning(
                "correctionPassthroughFactor: failed to load corrections for \(analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            return 1.0
        }
        // Filter to false positive corrections only — false negatives must not
        // suppress detection (they should boost it, which is correctionBoostFactor's job).
        // Legacy corrections (source == nil) predate the false-negative feature and are
        // all false-positive vetoes — treat them as such to preserve pre-existing behavior.
        let falsePositives = weighted.filter { event, _ in
            event.source?.kind == .falsePositive || event.source == nil
        }
        guard !falsePositives.isEmpty else { return 1.0 }
        // The strongest (most-recent) correction has the highest weight (close to 1.0).
        // We convert to a suppression factor: 1.0 = no suppression, 0.0 = full suppression.
        let maxCorrectionWeight = falsePositives.map(\.1).max() ?? 0.0
        return max(0.0, 1.0 - maxCorrectionWeight)
    }

    // MARK: - Protocol: correctionBoostFactor

    /// Returns a boost factor derived from false negative corrections for the given asset.
    ///
    /// If no false negative corrections exist, returns 1.0 (no boost). If corrections exist,
    /// returns `1.0 + maxWeight` where maxWeight is the highest decay-weighted false negative
    /// correction seen — i.e. the most-recent false negative report has the most boost.
    /// Result is clamped to [1.0, 2.0].
    func correctionBoostFactor(for analysisAssetId: String) async -> Double {
        let weighted: [(CorrectionEvent, Double)]
        do {
            weighted = try await weightedCorrections(for: analysisAssetId)
        } catch {
            logger.warning(
                "correctionBoostFactor: failed to load corrections for \(analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            return 1.0
        }
        // Filter to false negative corrections only.
        let falseNegatives = weighted.filter { event, _ in
            event.source?.kind == .falseNegative
        }
        guard !falseNegatives.isEmpty else { return 1.0 }
        let maxCorrectionWeight = falseNegatives.map(\.1).max() ?? 0.0
        return min(2.0, 1.0 + maxCorrectionWeight)
    }

    // MARK: - False-Negative Synthetic Anchor (playhead-ef2.3.2)

    /// Record a false-negative correction ("missed ad here") and immediately create
    /// a synthetic DecodedSpan so the user sees the correction take effect.
    ///
    /// The synthetic span uses ±15s fallback boundaries around the reported time
    /// (clamped to 0 at the start). It is episode-local (scoped to `assetId`)
    /// and does NOT propagate to other episodes or the sponsor knowledge store.
    ///
    /// The span uses negative atom ordinals to avoid colliding with real transcript
    /// atom ordinals (which are always >= 0).
    func recordFalseNegative(
        assetId: String,
        reportedTime: Double
    ) async throws {
        let correctionId = UUID().uuidString
        let now = Date().timeIntervalSince1970

        // 1. Compute synthetic ordinals first so the CorrectionEvent scope matches.
        //
        // Use deterministic negative ordinals derived from SHA256 of the correction id.
        // String.hashValue is randomized per process in Swift — SHA256 is stable across
        // launches and devices, which matters for span ID reproducibility.
        let hashBytes = SHA256.hash(data: Data(correctionId.utf8))
        let hashInt = hashBytes.prefix(8).enumerated().reduce(0) { acc, pair in
            acc | (Int(pair.element) << (pair.offset * 8))
        }
        let syntheticFirst = -(abs(hashInt % 1_000_000) + 2)
        let syntheticLast = syntheticFirst + 1

        // 2. Record the correction event with the actual synthetic ordinals.
        let scope = CorrectionScope.exactSpan(
            assetId: assetId,
            ordinalRange: syntheticFirst...syntheticLast
        )
        let event = CorrectionEvent(
            analysisAssetId: assetId,
            scope: scope.serialized,
            createdAt: now,
            source: .falseNegative,
            podcastId: nil
        )
        try await record(event)

        // 3. Create synthetic DecodedSpan with ±15s fallback boundaries.
        let fallbackRadius = 15.0
        let startTime = max(0.0, reportedTime - fallbackRadius)
        let endTime = reportedTime + fallbackRadius

        let spanId = DecodedSpan.makeId(
            assetId: assetId,
            firstAtomOrdinal: syntheticFirst,
            lastAtomOrdinal: syntheticLast
        )

        let span = DecodedSpan(
            id: spanId,
            assetId: assetId,
            firstAtomOrdinal: syntheticFirst,
            lastAtomOrdinal: syntheticLast,
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: [
                .userCorrection(correctionId: correctionId, reportedTime: reportedTime)
            ]
        )

        do {
            try await store.upsertDecodedSpans([span])
            logger.info(
                "recordFalseNegative: created synthetic span \(spanId, privacy: .public) at \(startTime, format: .fixed(precision: 1))–\(endTime, format: .fixed(precision: 1)) for asset \(assetId, privacy: .public)"
            )
        } catch {
            logger.error(
                "recordFalseNegative: failed to persist synthetic span for asset \(assetId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            throw error
        }
    }

    // MARK: - Query

    /// Load all correction events for the given analysis asset, oldest first.
    func activeCorrections(for analysisAssetId: String) async throws -> [CorrectionEvent] {
        try await store.loadCorrectionEvents(analysisAssetId: analysisAssetId)
    }

    /// Load all correction events for the given asset with decay weights applied.
    ///
    /// Returns pairs of (event, weight) where weight = correctionDecayWeight(ageDays:).
    func weightedCorrections(
        for analysisAssetId: String,
        at now: Date = Date()
    ) async throws -> [(CorrectionEvent, Double)] {
        let events = try await store.loadCorrectionEvents(analysisAssetId: analysisAssetId)
        return events.map { event in
            let ageDays = (now.timeIntervalSince1970 - event.createdAt) / 86400.0
            let weight = correctionDecayWeight(ageDays: ageDays)
            return (event, weight)
        }
    }

    /// Returns true if any persisted correction event targets the given scope.
    func hasActiveCorrection(scope: CorrectionScope, at now: Date = Date()) async throws -> Bool {
        try await store.hasAnyCorrectionEvent(withScope: scope.serialized)
    }
}
