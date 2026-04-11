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

import Foundation
import OSLog

// MARK: - CorrectionScope

/// Semantic scope at which a user correction applies.
///
/// Serialized to/from a colon-delimited string for SQLite storage:
///   exactSpan:assetId:firstOrdinal:lastOrdinal
///   sponsorOnShow:podcastId:sponsor
///   phraseOnShow:podcastId:phrase
///   campaignOnShow:podcastId:campaign
enum CorrectionScope: Sendable, Equatable {
    /// Exact span veto: the specific atom range in a specific asset.
    case exactSpan(assetId: String, ordinalRange: ClosedRange<Int>)
    /// Sponsor veto across all episodes of a podcast.
    case sponsorOnShow(podcastId: String, sponsor: String)
    /// Phrase veto across all episodes of a podcast.
    case phraseOnShow(podcastId: String, phrase: String)
    /// Campaign veto across all episodes of a podcast.
    case campaignOnShow(podcastId: String, campaign: String)

    // MARK: Serialization

    /// Serialize to the storage string format.
    var serialized: String {
        switch self {
        case .exactSpan(let assetId, let range):
            return "exactSpan:\(assetId):\(range.lowerBound):\(range.upperBound)"
        case .sponsorOnShow(let podcastId, let sponsor):
            return "sponsorOnShow:\(podcastId):\(sponsor)"
        case .phraseOnShow(let podcastId, let phrase):
            return "phraseOnShow:\(podcastId):\(phrase)"
        case .campaignOnShow(let podcastId, let campaign):
            return "campaignOnShow:\(podcastId):\(campaign)"
        }
    }

    /// Deserialize from a storage string produced by `serialized`.
    /// Returns `nil` when the string is malformed or has an unknown prefix.
    ///
    /// Uses case-specific splitting to handle colons within field values
    /// (e.g. sponsors like "Squarespace: Build It", URLs in phrases, compound
    /// campaign names). The type prefix is extracted first; then the remainder
    /// is split with the minimum number of splits needed for each case.
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
        default:
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
///   weight = max(0.1, 1.0 - (ageDays / 180.0))
///
/// - Note: ageDays < 0 returns weight > 1.0; caller is responsible for clamping.
func correctionDecayWeight(ageDays: Double) -> Double {
    max(0.1, 1.0 - (ageDays / 180.0))
}

// MARK: - DecodedSpan + ordinalRange helper

extension DecodedSpan {
    /// Return the full atom ordinal range of the span.
    ///
    /// Since atom-level timing isn't carried on DecodedSpan, we use the full
    /// ordinal extent for conservative (wider) veto coverage.
    func ordinalRange(for timeRange: ClosedRange<Double>) -> ClosedRange<Int> {
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
    func recordVeto(span: DecodedSpan, timeRange: ClosedRange<Double>) async

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
    func recordVeto(span: DecodedSpan, timeRange: ClosedRange<Double>) async {
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
    func recordVeto(span: DecodedSpan, timeRange: ClosedRange<Double>) async {
        let ordinals = span.ordinalRange(for: timeRange)
        let now = Date().timeIntervalSince1970

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
            podcastId: nil
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
                podcastId: nil
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
        guard !weighted.isEmpty else { return 1.0 }
        // The strongest (most-recent) correction has the highest weight (close to 1.0).
        // We convert to a suppression factor: 1.0 = no suppression, 0.0 = full suppression.
        let maxCorrectionWeight = weighted.map(\.1).max() ?? 0.0
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
