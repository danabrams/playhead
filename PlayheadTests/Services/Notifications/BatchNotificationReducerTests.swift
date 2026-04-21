// BatchNotificationReducerTests.swift
// Pure-reducer coverage for `BatchNotificationReducer.reduce(...)`.
// playhead-zp0x.

import Foundation
import Testing

@testable import Playhead

@Suite("BatchNotificationReducer — precedence + persistence (playhead-zp0x)")
struct BatchNotificationReducerTests {

    // MARK: - Helpers

    private static let t0 = Date(timeIntervalSince1970: 1_700_000_000)

    private static func summary(
        key: String = "ep-1",
        disposition: SurfaceDisposition = .queued,
        reason: SurfaceReason = .waitingForTime,
        analysisUnavailableReason: AnalysisUnavailableReason? = nil,
        isReady: Bool = false,
        userFixable: Bool = false
    ) -> BatchChildSurfaceSummary {
        BatchChildSurfaceSummary(
            canonicalEpisodeKey: key,
            disposition: disposition,
            reason: reason,
            analysisUnavailableReason: analysisUnavailableReason,
            isReady: isReady,
            userFixable: userFixable
        )
    }

    private static let neverBlocked = BatchNotificationReducer.PersistenceState(
        consecutiveBlockedPasses: 0,
        firstBlockedAt: nil
    )

    /// Persistence state that has already cleared both bars (≥ 2 prior
    /// passes AND ≥ 30 minutes of wall-clock).
    private static func persistenceClearingBothBars(now: Date) -> BatchNotificationReducer.PersistenceState {
        // Prior passes = 1 means current pass makes it 2, clearing the
        // pass-count bar. firstBlockedAt 31 minutes ago clears the
        // wall-clock bar.
        BatchNotificationReducer.PersistenceState(
            consecutiveBlockedPasses: 1,
            firstBlockedAt: now.addingTimeInterval(-31 * 60)
        )
    }

    // MARK: - Happy paths (5 enum cases)

    @Test("Empty batch → .none")
    func emptyBatchReturnsNone() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [],
            persistence: Self.neverBlocked,
            now: Self.t0
        )
        #expect(result.verdict == .none)
    }

    @Test("All ready → .tripReady")
    func allReadyTripsTripReady() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(key: "a", isReady: true),
                Self.summary(key: "b", isReady: true),
            ],
            persistence: Self.neverBlocked,
            now: Self.t0
        )
        #expect(result.verdict == .tripReady)
    }

    @Test("Storage blocker (fixable, persistence cleared) → .blockedStorage")
    func storageBlockerWithPersistenceClearsToBlockedStorage() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(reason: .storageFull, userFixable: true),
            ],
            persistence: Self.persistenceClearingBothBars(now: Self.t0),
            now: Self.t0
        )
        #expect(result.verdict == .blockedStorage)
    }

    @Test("WiFi-policy blocker (fixable, persistence cleared) → .blockedWifiPolicy")
    func wifiPolicyBlockerWithPersistenceClearsToBlockedWifiPolicy() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(reason: .waitingForNetwork, userFixable: true),
            ],
            persistence: Self.persistenceClearingBothBars(now: Self.t0),
            now: Self.t0
        )
        #expect(result.verdict == .blockedWifiPolicy)
    }

    @Test("AnalysisUnavailable (AI disabled, fixable, persistence cleared) → .blockedAnalysisUnavailable")
    func analysisUnavailableAIDisabledClearsToBlockedAnalysisUnavailable() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(
                    reason: .analysisUnavailable,
                    analysisUnavailableReason: .appleIntelligenceDisabled,
                    userFixable: true
                ),
            ],
            persistence: Self.persistenceClearingBothBars(now: Self.t0),
            now: Self.t0
        )
        #expect(result.verdict == .blockedAnalysisUnavailable)
    }

    @Test("AnalysisUnavailable (language unsupported, fixable) → .blockedAnalysisUnavailable")
    func analysisUnavailableLanguageUnsupportedClearsToBlockedAnalysisUnavailable() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(
                    reason: .analysisUnavailable,
                    analysisUnavailableReason: .languageUnsupported,
                    userFixable: true
                ),
            ],
            persistence: Self.persistenceClearingBothBars(now: Self.t0),
            now: Self.t0
        )
        #expect(result.verdict == .blockedAnalysisUnavailable)
    }

    @Test("In-progress (no ready, no fixable blocker) → .none")
    func inProgressReturnsNone() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(isReady: false),
            ],
            persistence: Self.neverBlocked,
            now: Self.t0
        )
        #expect(result.verdict == .none)
    }

    // MARK: - Precedence

    @Test("AnalysisUnavailable beats Storage")
    func analysisUnavailableBeatsStorage() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(
                    key: "a",
                    reason: .analysisUnavailable,
                    analysisUnavailableReason: .appleIntelligenceDisabled,
                    userFixable: true
                ),
                Self.summary(key: "b", reason: .storageFull, userFixable: true),
            ],
            persistence: Self.persistenceClearingBothBars(now: Self.t0),
            now: Self.t0
        )
        #expect(result.verdict == .blockedAnalysisUnavailable)
    }

    @Test("Storage beats WifiPolicy")
    func storageBeatsWifiPolicy() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(key: "a", reason: .storageFull, userFixable: true),
                Self.summary(key: "b", reason: .waitingForNetwork, userFixable: true),
            ],
            persistence: Self.persistenceClearingBothBars(now: Self.t0),
            now: Self.t0
        )
        #expect(result.verdict == .blockedStorage)
    }

    @Test("AnalysisUnavailable beats both Storage and WifiPolicy")
    func analysisUnavailableBeatsAll() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(key: "a", reason: .waitingForNetwork, userFixable: true),
                Self.summary(key: "b", reason: .storageFull, userFixable: true),
                Self.summary(
                    key: "c",
                    reason: .analysisUnavailable,
                    analysisUnavailableReason: .languageUnsupported,
                    userFixable: true
                ),
            ],
            persistence: Self.persistenceClearingBothBars(now: Self.t0),
            now: Self.t0
        )
        #expect(result.verdict == .blockedAnalysisUnavailable)
    }

    // MARK: - Persistence rule (AND, not OR)

    @Test("First blocked pass alone → .none (counter and clock both fail)")
    func firstBlockedPassReturnsNone() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(reason: .storageFull, userFixable: true),
            ],
            persistence: Self.neverBlocked,   // 0 prior passes, no anchor
            now: Self.t0
        )
        #expect(result.verdict == .none)
    }

    @Test("≥2 passes BUT < 30 min → .none (wall-clock fails)")
    func passesButTooSoonReturnsNone() {
        let persistence = BatchNotificationReducer.PersistenceState(
            consecutiveBlockedPasses: 1,
            firstBlockedAt: Self.t0.addingTimeInterval(-29 * 60) // 29 min ago
        )
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(reason: .storageFull, userFixable: true),
            ],
            persistence: persistence,
            now: Self.t0
        )
        #expect(result.verdict == .none)
    }

    @Test("<2 passes BUT ≥ 30 min → .none (pass-count fails)")
    func tooFewPassesEvenWithLongClockReturnsNone() {
        // consecutiveBlockedPasses == 0 means current pass makes it 1
        // — still less than the required 2.
        let persistence = BatchNotificationReducer.PersistenceState(
            consecutiveBlockedPasses: 0,
            firstBlockedAt: Self.t0.addingTimeInterval(-60 * 60) // 60 min ago
        )
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(reason: .storageFull, userFixable: true),
            ],
            persistence: persistence,
            now: Self.t0
        )
        #expect(result.verdict == .none)
    }

    @Test("≥2 passes AND ≥30 min → eligible")
    func bothBarsClearedFires() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(reason: .storageFull, userFixable: true),
            ],
            persistence: Self.persistenceClearingBothBars(now: Self.t0),
            now: Self.t0
        )
        #expect(result.verdict == .blockedStorage)
    }

    @Test("Reset on progress (caller-supplied state) → .none, then .tripReady")
    func resetOnProgressReturnsNoneOrTripReady() {
        // Coordinator semantics: when reducer returns .tripReady or
        // .none, the coordinator will reset persistence. Here we
        // verify that a previously-blocked persistence does NOT cause
        // a .tripReady reduction to fire any blocker; the all-ready
        // short-circuit wins.
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(key: "a", isReady: true),
                Self.summary(key: "b", isReady: true),
            ],
            persistence: Self.persistenceClearingBothBars(now: Self.t0),
            now: Self.t0
        )
        #expect(result.verdict == .tripReady)
    }

    // MARK: - Hardware / region unavailable → not user-fixable

    @Test("Hardware unsupported → .none (not fixable, even with user-fixable=true smuggled)")
    func hardwareUnsupportedReturnsNone() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(
                    reason: .analysisUnavailable,
                    analysisUnavailableReason: .hardwareUnsupported,
                    // Even if the boundary somehow stamped userFixable
                    // = true (incorrectly), the reducer's filter drops
                    // the candidate.
                    userFixable: true
                ),
            ],
            persistence: Self.persistenceClearingBothBars(now: Self.t0),
            now: Self.t0
        )
        #expect(result.verdict == .none)
    }

    @Test("Region unsupported → .none")
    func regionUnsupportedReturnsNone() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(
                    reason: .analysisUnavailable,
                    analysisUnavailableReason: .regionUnsupported,
                    userFixable: true
                ),
            ],
            persistence: Self.persistenceClearingBothBars(now: Self.t0),
            now: Self.t0
        )
        #expect(result.verdict == .none)
    }

    @Test("Model temporarily unavailable → .none (transient)")
    func modelTemporarilyUnavailableReturnsNone() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(
                    reason: .analysisUnavailable,
                    analysisUnavailableReason: .modelTemporarilyUnavailable,
                    userFixable: true
                ),
            ],
            persistence: Self.persistenceClearingBothBars(now: Self.t0),
            now: Self.t0
        )
        #expect(result.verdict == .none)
    }

    // MARK: - Trip-ready short-circuits any blocker

    @Test("All ready short-circuits even with a blocker carried")
    func allReadyShortCircuits() {
        // Defensive: if every child reports isReady=true, the reducer
        // ignores any leftover reason field on those children. This
        // matches the spec's "trip-ready short-circuits any blocker
        // check" requirement.
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(
                    key: "a",
                    reason: .storageFull,
                    isReady: true,
                    userFixable: true
                ),
                Self.summary(key: "b", isReady: true),
            ],
            persistence: Self.persistenceClearingBothBars(now: Self.t0),
            now: Self.t0
        )
        #expect(result.verdict == .tripReady)
    }

    // MARK: - Eligibility helper

    @Test("isActionRequired true for blocked* cases only")
    func isActionRequiredFlag() {
        #expect(BatchNotificationEligibility.tripReady.isActionRequired == false)
        #expect(BatchNotificationEligibility.none.isActionRequired == false)
        #expect(BatchNotificationEligibility.blockedStorage.isActionRequired == true)
        #expect(BatchNotificationEligibility.blockedWifiPolicy.isActionRequired == true)
        #expect(BatchNotificationEligibility.blockedAnalysisUnavailable.isActionRequired == true)
    }

    // MARK: - pendingBlocker contract

    @Test("First blocked pass returns .none verdict but reports pendingBlocker so coordinator can advance the streak")
    func firstBlockedPassExposesPendingBlocker() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(reason: .storageFull, userFixable: true),
            ],
            persistence: Self.neverBlocked,
            now: Self.t0
        )
        #expect(result.verdict == .none)
        #expect(result.pendingBlocker == .blockedStorage)
    }

    @Test("No fixable blocker → pendingBlocker is nil so coordinator resets the streak")
    func noFixableBlockerHasNilPendingBlocker() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(isReady: false),
            ],
            persistence: Self.neverBlocked,
            now: Self.t0
        )
        #expect(result.verdict == .none)
        #expect(result.pendingBlocker == nil)
    }

    @Test("Trip-ready → pendingBlocker is nil (streak resets on progress)")
    func tripReadyHasNilPendingBlocker() {
        let result = BatchNotificationReducer.reduce(
            childSummaries: [
                Self.summary(key: "a", isReady: true),
                Self.summary(key: "b", isReady: true),
            ],
            persistence: Self.persistenceClearingBothBars(now: Self.t0),
            now: Self.t0
        )
        #expect(result.verdict == .tripReady)
        #expect(result.pendingBlocker == nil)
    }
}
