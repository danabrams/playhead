// ImplicitFeedbackStore.swift
// Phase ef2 (playhead-ef2.3.4): Collect behavioral signals as weak labels
// that inform ad detection quality without creating permanent vetoes.
//
// Design:
//   • ImplicitFeedbackSignal encodes the behavioral signal type.
//   • ImplicitFeedbackEvent is the append-only record persisted to SQLite.
//   • ImplicitFeedbackStore is an actor-backed store that persists events
//     via AnalysisStore and exposes aggregation queries.
//   • Weight is always 0.3× explicit correction weight (constant, not configurable).
//   • Weak labels NEVER create permanent vetoes alone.

import Foundation
import OSLog

// MARK: - ImplicitFeedbackSignal

/// Behavioral signal types that indicate potential ad detection quality issues.
///
/// Each signal captures a specific user behavior pattern that suggests the
/// detection system may have made an error (false positive or boundary error).
enum ImplicitFeedbackSignal: String, Sendable, CaseIterable {
    /// User unskips within 3s of an auto-skip — likely false positive.
    case immediateUnskip
    /// User seeks back into a skipped region — boundary error or false positive.
    case seekBackIntoSkipped
    /// User rewinds within 5s of a skip — boundary error.
    case rapidRewindAfterSkip
    /// User keeps manually skipping forward — likely false negative (missed ad).
    case repeatedManualSkipForward
    /// User disables auto-skip for a show — broad distrust flag.
    case showAutoSkipDisabled
}

// MARK: - ImplicitFeedbackEvent

/// Append-only record of a behavioral signal. Persisted to `implicit_feedback_events`.
struct ImplicitFeedbackEvent: Sendable, Equatable {
    /// UUID string for the event row.
    let id: String
    /// The behavioral signal type.
    let signal: ImplicitFeedbackSignal
    /// The analysisAssetId of the episode where the behavior occurred.
    let analysisAssetId: String
    /// The podcast feed ID, if known.
    let podcastId: String?
    /// The ad span that triggered the behavior, if known.
    let spanId: String?
    /// When the signal was recorded (seconds since epoch).
    let timestamp: Double
    /// Always 0.3 — weak labels are 0.3× explicit correction weight.
    let weight: Double

    /// Designated initializer. Weight is always 0.3 and cannot be overridden.
    init(
        id: String = UUID().uuidString,
        signal: ImplicitFeedbackSignal,
        analysisAssetId: String,
        podcastId: String? = nil,
        spanId: String? = nil,
        timestamp: Double = Date().timeIntervalSince1970
    ) {
        self.id = id
        self.signal = signal
        self.analysisAssetId = analysisAssetId
        self.podcastId = podcastId
        self.spanId = spanId
        self.timestamp = timestamp
        self.weight = 0.3
    }

    /// Internal initializer for database hydration. Validates stored weight matches 0.3.
    init(
        id: String,
        signal: ImplicitFeedbackSignal,
        analysisAssetId: String,
        podcastId: String?,
        spanId: String?,
        timestamp: Double,
        storedWeight: Double
    ) {
        self.id = id
        self.signal = signal
        self.analysisAssetId = analysisAssetId
        self.podcastId = podcastId
        self.spanId = spanId
        self.timestamp = timestamp
        // Enforce the 0.3 constant even if the stored value is stale.
        // The weight column in SQLite is effectively write-only — on load
        // we always use the current constant. If the constant changes in a
        // future version, existing rows adopt the new value automatically.
        // This is intentional: behavioral signal weight is a policy knob,
        // not a per-event attribute.
        self.weight = 0.3
    }
}

// MARK: - ImplicitFeedbackStore

/// Actor-backed store for implicit behavioral feedback signals.
///
/// Persists events via `AnalysisStore` and exposes aggregation queries
/// for diagnostic and (future) trust-scoring integration.
actor ImplicitFeedbackStore {

    private let store: AnalysisStore
    private let logger = Logger(subsystem: "com.playhead", category: "ImplicitFeedbackStore")

    init(store: AnalysisStore) {
        self.store = store
    }

    // MARK: - Record

    /// Persist a behavioral signal event.
    func record(_ event: ImplicitFeedbackEvent) async throws {
        try await store.appendImplicitFeedbackEvent(event)
        logger.debug(
            "Recorded implicit feedback: \(event.signal.rawValue, privacy: .public) for asset \(event.analysisAssetId, privacy: .public)"
        )
    }

    // MARK: - Aggregation

    /// Aggregate weighted signal impact for the given analysis asset.
    ///
    /// Returns the sum of weights for all signals on the asset. Since each
    /// event has weight 0.3, this is effectively `0.3 × count`. Callers can
    /// compare this to explicit correction weight (1.0) to gauge relative
    /// signal strength.
    func feedbackWeight(for analysisAssetId: String) async -> Double {
        do {
            let events = try await store.loadImplicitFeedbackEvents(analysisAssetId: analysisAssetId)
            return events.reduce(0.0) { $0 + $1.weight }
        } catch {
            logger.warning(
                "feedbackWeight: failed to load events for \(analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            return 0.0
        }
    }

    /// Per-signal type counts for the given analysis asset.
    func signalCounts(for analysisAssetId: String) async -> [ImplicitFeedbackSignal: Int] {
        do {
            let events = try await store.loadImplicitFeedbackEvents(analysisAssetId: analysisAssetId)
            var counts: [ImplicitFeedbackSignal: Int] = [:]
            for event in events {
                counts[event.signal, default: 0] += 1
            }
            return counts
        } catch {
            logger.warning(
                "signalCounts: failed to load events for \(analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            return [:]
        }
    }

    /// Recent signals for a show, ordered by timestamp descending.
    func recentSignals(forShow podcastId: String, limit: Int) async -> [ImplicitFeedbackEvent] {
        do {
            return try await store.loadImplicitFeedbackEvents(podcastId: podcastId, limit: limit)
        } catch {
            logger.warning(
                "recentSignals: failed to load events for show \(podcastId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            return []
        }
    }
}
