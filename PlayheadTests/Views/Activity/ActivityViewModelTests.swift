// ActivityViewModelTests.swift
// Pure-aggregation tests for the Activity screen view-model. The VM
// projects a list of `(episodeId, title, status)` inputs into the four
// canonical sections (Now / Up Next / Paused / Recently Finished); these
// tests pin that bucketing without spinning up SwiftData, the
// AnalysisStore, or any scheduler state.
//
// Scope: playhead-quh7 (Phase 2 deliverable 4 — Activity screen).
//
// What this suite deliberately does NOT cover:
//   * SurfaceReason → ResolutionHint copy mapping. That contract lives in
//     `EpisodeStatusLineCopyTests` / `SurfaceReasonCopyTemplateTests`
//     (playhead-dfem / playhead-ol05). These tests only smoke-test that
//     the VM threads SurfaceReason / ResolutionHint into the Paused
//     row's payload — they do not re-pin the copy strings.
//   * Notification-driven refresh wiring. The VM exposes a synchronous
//     `refresh(from:)` entry point; observation glue is the View's job.

import Foundation
import Testing

@testable import Playhead

@Suite("ActivityViewModel — pure aggregation (playhead-quh7)")
struct ActivityViewModelTests {

    // MARK: - Canonical fixtures

    /// Default eligibility: every gate true. Most tests start from a
    /// fully-eligible device because section bucketing turns on
    /// disposition + reason, not on eligibility (eligibility short-
    /// circuits to `.unavailable`, which lands in Recently Finished as
    /// `analysis_unavailable`).
    static let eligible = AnalysisEligibility(
        hardwareSupported: true,
        appleIntelligenceEnabled: true,
        regionSupported: true,
        languageSupported: true,
        modelAvailableNow: true,
        capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
    )

    /// Helper to build an `EpisodeSurfaceStatus` directly from a
    /// disposition / reason / hint triple. Bypasses the reducer because
    /// these tests assert the VM's projection of an already-resolved
    /// status, not the reducer's input-precedence ladder (which is
    /// covered by `EpisodeSurfaceStatusReducerTests`).
    static func makeStatus(
        disposition: SurfaceDisposition,
        reason: SurfaceReason,
        hint: ResolutionHint = .none,
        unavailable: AnalysisUnavailableReason? = nil,
        readiness: PlaybackReadiness = .none
    ) -> EpisodeSurfaceStatus {
        EpisodeSurfaceStatus(
            disposition: disposition,
            reason: reason,
            hint: hint,
            analysisUnavailableReason: unavailable,
            playbackReadiness: readiness,
            readinessAnchor: nil
        )
    }

    static func makeInput(
        id: String,
        title: String = "Some Episode",
        podcast: String? = "Some Show",
        status: EpisodeSurfaceStatus,
        isRunning: Bool = false,
        finishedAt: Date? = nil
    ) -> ActivityEpisodeInput {
        ActivityEpisodeInput(
            episodeId: id,
            episodeTitle: title,
            podcastTitle: podcast,
            status: status,
            isRunning: isRunning,
            finishedAt: finishedAt
        )
    }

    // MARK: - Bucketing

    @Test("queued + isRunning=true → Now")
    func runningQueuedLandsInNow() {
        let inputs = [
            Self.makeInput(
                id: "ep-1",
                title: "Hard Fork — The OpenAI Memo",
                podcast: "Hard Fork",
                status: Self.makeStatus(
                    disposition: .queued,
                    reason: .waitingForTime,
                    hint: .wait
                ),
                isRunning: true
            )
        ]
        let snapshot = ActivityViewModel.aggregate(inputs: inputs, now: Date())
        #expect(snapshot.now.count == 1)
        #expect(snapshot.now.first?.episodeId == "ep-1")
        #expect(snapshot.now.first?.title == "Hard Fork — The OpenAI Memo")
        #expect(snapshot.upNext.isEmpty)
        #expect(snapshot.paused.isEmpty)
        #expect(snapshot.recentlyFinished.isEmpty)
    }

    @Test("queued + isRunning=false → Up Next")
    func queuedNotRunningLandsInUpNext() {
        let inputs = [
            Self.makeInput(
                id: "ep-2",
                status: Self.makeStatus(
                    disposition: .queued,
                    reason: .waitingForTime,
                    hint: .wait
                ),
                isRunning: false
            )
        ]
        let snapshot = ActivityViewModel.aggregate(inputs: inputs, now: Date())
        #expect(snapshot.now.isEmpty)
        #expect(snapshot.upNext.count == 1)
        #expect(snapshot.upNext.first?.episodeId == "ep-2")
        #expect(snapshot.paused.isEmpty)
        #expect(snapshot.recentlyFinished.isEmpty)
    }

    @Test("paused disposition → Paused, regardless of isRunning")
    func pausedLandsInPaused() {
        let inputs = [
            Self.makeInput(
                id: "ep-3",
                status: Self.makeStatus(
                    disposition: .paused,
                    reason: .phoneIsHot,
                    hint: .wait
                ),
                isRunning: false
            ),
            Self.makeInput(
                id: "ep-4",
                status: Self.makeStatus(
                    disposition: .paused,
                    reason: .powerLimited,
                    hint: .chargeDevice
                ),
                isRunning: true // even running paused jobs land in Paused
            )
        ]
        let snapshot = ActivityViewModel.aggregate(inputs: inputs, now: Date())
        #expect(snapshot.now.isEmpty)
        #expect(snapshot.upNext.isEmpty)
        #expect(snapshot.paused.count == 2)
        #expect(Set(snapshot.paused.map(\.episodeId)) == ["ep-3", "ep-4"])
    }

    @Test("Paused row threads SurfaceReason + ResolutionHint into payload")
    func pausedRowExposesReasonAndHint() {
        // Smoke-test only: the actual copy mapping is contract-tested
        // in dfem / ol05 suites. Here we only verify the VM forwards
        // the reason/hint pair so the View can call into the canonical
        // copy resolver.
        let input = Self.makeInput(
            id: "ep-storage",
            status: Self.makeStatus(
                disposition: .paused,
                reason: .storageFull,
                hint: .freeUpStorage
            )
        )
        let snapshot = ActivityViewModel.aggregate(inputs: [input], now: Date())
        let row = try! #require(snapshot.paused.first)
        #expect(row.reason == .storageFull)
        #expect(row.hint == .freeUpStorage)
    }

    @Test("done persisted state with finishedAt → Recently Finished (success)")
    func doneRecentLandsInFinished() {
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let inputs = [
            Self.makeInput(
                id: "ep-5",
                status: Self.makeStatus(
                    disposition: .queued,
                    reason: .waitingForTime,
                    hint: .none,
                    readiness: .complete
                ),
                isRunning: false,
                finishedAt: now.addingTimeInterval(-3600)
            )
        ]
        let snapshot = ActivityViewModel.aggregate(inputs: inputs, now: now)
        #expect(snapshot.recentlyFinished.count == 1)
        let row = try! #require(snapshot.recentlyFinished.first)
        #expect(row.episodeId == "ep-5")
        #expect(row.outcome == .success)
    }

    @Test("failed disposition → Recently Finished (couldntAnalyze)")
    func failedLandsInFinishedAsCouldntAnalyze() {
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let input = Self.makeInput(
            id: "ep-failed",
            status: Self.makeStatus(
                disposition: .failed,
                reason: .couldntAnalyze,
                hint: .retry
            ),
            finishedAt: now.addingTimeInterval(-600)
        )
        let snapshot = ActivityViewModel.aggregate(inputs: [input], now: now)
        #expect(snapshot.recentlyFinished.count == 1)
        let row = try! #require(snapshot.recentlyFinished.first)
        #expect(row.outcome == .couldntAnalyze)
    }

    @Test("unavailable disposition → Recently Finished (analysisUnavailable)")
    func unavailableLandsInFinishedAsAnalysisUnavailable() {
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let input = Self.makeInput(
            id: "ep-ai-off",
            status: Self.makeStatus(
                disposition: .unavailable,
                reason: .analysisUnavailable,
                hint: .enableAppleIntelligence,
                unavailable: .appleIntelligenceDisabled
            ),
            finishedAt: now.addingTimeInterval(-300)
        )
        let snapshot = ActivityViewModel.aggregate(inputs: [input], now: now)
        #expect(snapshot.recentlyFinished.count == 1)
        let row = try! #require(snapshot.recentlyFinished.first)
        #expect(row.outcome == .analysisUnavailable(.appleIntelligenceDisabled))
    }

    @Test("Recently Finished is capped at 20 most-recent entries")
    func recentlyFinishedCapsAt20() {
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let inputs: [ActivityEpisodeInput] = (0..<30).map { i in
            Self.makeInput(
                id: "ep-\(i)",
                status: Self.makeStatus(
                    disposition: .queued,
                    reason: .waitingForTime,
                    readiness: .complete
                ),
                finishedAt: now.addingTimeInterval(TimeInterval(-i * 60))
            )
        }
        let snapshot = ActivityViewModel.aggregate(inputs: inputs, now: now)
        #expect(snapshot.recentlyFinished.count == 20)
        // The newest entry (smallest negative offset) is ep-0.
        #expect(snapshot.recentlyFinished.first?.episodeId == "ep-0")
        // The oldest kept entry is ep-19; ep-20..ep-29 are pruned.
        #expect(snapshot.recentlyFinished.last?.episodeId == "ep-19")
    }

    @Test("Recently Finished excludes entries older than 24h")
    func recentlyFinishedExcludesAged() {
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let oneHourAgo = now.addingTimeInterval(-3_600)
        let twoDaysAgo = now.addingTimeInterval(-2 * 86_400)
        let inputs = [
            Self.makeInput(
                id: "ep-fresh",
                status: Self.makeStatus(
                    disposition: .queued,
                    reason: .waitingForTime,
                    readiness: .complete
                ),
                finishedAt: oneHourAgo
            ),
            Self.makeInput(
                id: "ep-stale",
                status: Self.makeStatus(
                    disposition: .queued,
                    reason: .waitingForTime,
                    readiness: .complete
                ),
                finishedAt: twoDaysAgo
            )
        ]
        let snapshot = ActivityViewModel.aggregate(inputs: inputs, now: now)
        #expect(snapshot.recentlyFinished.count == 1)
        #expect(snapshot.recentlyFinished.first?.episodeId == "ep-fresh")
    }

    // MARK: - Section transitions

    @Test("Section transitions: Paused → Now when reason clears and job runs")
    func pausedToNowWhenReasonClears() {
        // First snapshot: paused on phoneIsHot.
        let pausedInput = Self.makeInput(
            id: "ep-thermal",
            status: Self.makeStatus(
                disposition: .paused,
                reason: .phoneIsHot,
                hint: .wait
            ),
            isRunning: false
        )
        let snapshotA = ActivityViewModel.aggregate(inputs: [pausedInput], now: Date())
        #expect(snapshotA.paused.count == 1)
        #expect(snapshotA.now.isEmpty)

        // Reason clears, job is now running.
        let runningInput = Self.makeInput(
            id: "ep-thermal",
            status: Self.makeStatus(
                disposition: .queued,
                reason: .waitingForTime,
                hint: .wait
            ),
            isRunning: true
        )
        let snapshotB = ActivityViewModel.aggregate(inputs: [runningInput], now: Date())
        #expect(snapshotB.paused.isEmpty)
        #expect(snapshotB.now.count == 1)
        #expect(snapshotB.now.first?.episodeId == "ep-thermal")
    }

    // MARK: - Empty state

    @Test("Empty input → all four sections empty")
    func emptyInputProducesEmptySnapshot() {
        let snapshot = ActivityViewModel.aggregate(inputs: [], now: Date())
        #expect(snapshot.now.isEmpty)
        #expect(snapshot.upNext.isEmpty)
        #expect(snapshot.paused.isEmpty)
        #expect(snapshot.recentlyFinished.isEmpty)
    }

    // MARK: - Cancelled disposition routing

    @Test("cancelled disposition with finishedAt → Recently Finished as couldntAnalyze")
    func cancelledLandsInFinished() {
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        // EpisodeStatusLineCopy maps cancelled → "Couldn't analyze · Retry"
        // (see playhead-zp5y rationale in EpisodeStatusLineCopy.swift).
        // The VM mirrors that routing so cancelled jobs surface as
        // couldntAnalyze in Recently Finished rather than vanishing.
        let input = Self.makeInput(
            id: "ep-cancel",
            status: Self.makeStatus(
                disposition: .cancelled,
                reason: .cancelled,
                hint: .retry
            ),
            finishedAt: now.addingTimeInterval(-120)
        )
        let snapshot = ActivityViewModel.aggregate(inputs: [input], now: now)
        #expect(snapshot.recentlyFinished.count == 1)
        let row = try! #require(snapshot.recentlyFinished.first)
        #expect(row.outcome == .couldntAnalyze)
    }
}
