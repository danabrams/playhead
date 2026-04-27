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
import SwiftData
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

    // MARK: - Up Next drag-to-reorder

    @Test("moveUpNext rewrites snapshot.upNext order in place")
    @MainActor
    func moveUpNextReordersSnapshot() {
        // Build a starting snapshot of four queued (Up Next) episodes
        // by feeding the aggregator a deterministic input list. We
        // route through `refresh(from:)` rather than constructing the
        // snapshot inline so the test exercises the same entry point
        // production uses.
        let inputs: [ActivityEpisodeInput] = (0..<4).map { i in
            Self.makeInput(
                id: "ep-\(i)",
                title: "Episode \(i)",
                status: Self.makeStatus(
                    disposition: .queued,
                    reason: .waitingForTime
                ),
                isRunning: false
            )
        }
        let vm = ActivityViewModel()
        vm.refresh(from: inputs)
        #expect(vm.snapshot.upNext.map(\.episodeId) == [
            "ep-0", "ep-1", "ep-2", "ep-3"
        ])

        // Move ep-3 (index 3) to the front (destination 0). This is
        // the same `(IndexSet, Int)` shape SwiftUI's
        // `List.onMove { src, dst in ... }` hands the closure.
        vm.moveUpNext(from: IndexSet(integer: 3), to: 0)
        #expect(vm.snapshot.upNext.map(\.episodeId) == [
            "ep-3", "ep-0", "ep-1", "ep-2"
        ])

        // A second drag to verify subsequent reorders compose against
        // the already-mutated snapshot (i.e. moveUpNext is not
        // recomputing from a stale baseline).
        vm.moveUpNext(from: IndexSet(integer: 0), to: 3)
        #expect(vm.snapshot.upNext.map(\.episodeId) == [
            "ep-0", "ep-1", "ep-3", "ep-2"
        ])

        // Reorder must not bleed into the other sections. The four
        // queued / not-running inputs all land in upNext; now / paused
        // / recentlyFinished must remain empty across the moves.
        #expect(vm.snapshot.now.isEmpty)
        #expect(vm.snapshot.paused.isEmpty)
        #expect(vm.snapshot.recentlyFinished.isEmpty)
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

    // MARK: - playhead-cjqq: queuePosition sort + persistence

    /// Variant of `makeInput` that sets `queuePosition` so the
    /// queuePosition-aware tests stay readable. The default `nil` lets
    /// the existing fixtures keep their pre-cjqq behavior unchanged.
    static func makeInput(
        id: String,
        title: String = "Some Episode",
        podcast: String? = "Some Show",
        status: EpisodeSurfaceStatus,
        isRunning: Bool = false,
        finishedAt: Date? = nil,
        queuePosition: Int?
    ) -> ActivityEpisodeInput {
        ActivityEpisodeInput(
            episodeId: id,
            episodeTitle: title,
            podcastTitle: podcast,
            status: status,
            isRunning: isRunning,
            finishedAt: finishedAt,
            queuePosition: queuePosition
        )
    }

    @Test("Up Next sort: queuePosition ascending, nil last, episodeId tiebreak")
    func upNextSortRespectsQueuePositionWithNilLast() {
        // Mixed ordering: provider hands rows in scheduler-priority
        // order; the VM must re-sort so the user's persisted
        // queuePosition wins, with un-reordered (nil) episodes at the
        // tail and a deterministic id-tiebreak between equal-position
        // entries.
        let upNextStatus = Self.makeStatus(
            disposition: .queued, reason: .waitingForTime
        )
        let inputs: [ActivityEpisodeInput] = [
            // Provider order intentionally scrambled relative to
            // queuePosition so the test would fail under the pre-cjqq
            // "input order preserved" rule.
            Self.makeInput(id: "ep-nil-b", status: upNextStatus, queuePosition: nil),
            Self.makeInput(id: "ep-pos-2", status: upNextStatus, queuePosition: 2),
            Self.makeInput(id: "ep-pos-0", status: upNextStatus, queuePosition: 0),
            Self.makeInput(id: "ep-nil-a", status: upNextStatus, queuePosition: nil),
            Self.makeInput(id: "ep-pos-1", status: upNextStatus, queuePosition: 1),
            // Two rows with the same queuePosition exercise the
            // episodeId tiebreak.
            Self.makeInput(id: "ep-pos-2-tie", status: upNextStatus, queuePosition: 2),
        ]
        let snapshot = ActivityViewModel.aggregate(inputs: inputs, now: Date())
        #expect(snapshot.upNext.map(\.episodeId) == [
            "ep-pos-0",       // queuePosition 0 first
            "ep-pos-1",       // then 1
            "ep-pos-2",       // then 2 (id "ep-pos-2" < "ep-pos-2-tie")
            "ep-pos-2-tie",   // tiebreak by episodeId asc
            "ep-nil-a",       // nil rows last, sorted by id
            "ep-nil-b",
        ])
    }

    @Test("Up Next sort: all-nil queuePositions still produce deterministic order")
    func upNextSortDeterministicWhenAllNil() {
        // Pre-cjqq behavior was "input order preserved". That bound is
        // weakened by cjqq — when every queuePosition is nil, the sort
        // falls back to `episodeId` to keep the order deterministic
        // across multiple aggregate calls.
        let upNextStatus = Self.makeStatus(
            disposition: .queued, reason: .waitingForTime
        )
        let inputs: [ActivityEpisodeInput] = [
            Self.makeInput(id: "ep-c", status: upNextStatus, queuePosition: nil),
            Self.makeInput(id: "ep-a", status: upNextStatus, queuePosition: nil),
            Self.makeInput(id: "ep-b", status: upNextStatus, queuePosition: nil),
        ]
        let snapshot = ActivityViewModel.aggregate(inputs: inputs, now: Date())
        #expect(snapshot.upNext.map(\.episodeId) == ["ep-a", "ep-b", "ep-c"])
    }

    @Test("moveUpNext writes through to persistQueueOrder with sequential renumber")
    @MainActor
    func moveUpNextPersistsSequentialRenumber() {
        var captured: [[(String, Int)]] = []
        let vm = ActivityViewModel(persistQueueOrder: { ordering in
            captured.append(ordering.map { ($0.episodeId, $0.queuePosition) })
        })

        let inputs: [ActivityEpisodeInput] = (0..<4).map { i in
            Self.makeInput(
                id: "ep-\(i)",
                title: "Episode \(i)",
                status: Self.makeStatus(
                    disposition: .queued, reason: .waitingForTime
                ),
                queuePosition: nil
            )
        }
        vm.refresh(from: inputs)
        // Initial order is the deterministic id-asc fallback.
        #expect(vm.snapshot.upNext.map(\.episodeId) == [
            "ep-0", "ep-1", "ep-2", "ep-3"
        ])

        // Move ep-3 to the front.
        vm.moveUpNext(from: IndexSet(integer: 3), to: 0)
        #expect(vm.snapshot.upNext.map(\.episodeId) == [
            "ep-3", "ep-0", "ep-1", "ep-2"
        ])
        // Persistence callback fired exactly once with the sequential
        // renumber matching the new on-screen order.
        #expect(captured.count == 1)
        #expect(captured[0].map(\.0) == ["ep-3", "ep-0", "ep-1", "ep-2"])
        #expect(captured[0].map(\.1) == [0, 1, 2, 3])
    }

    @Test("moveUpNext is idempotent: no-op move skips persistQueueOrder")
    @MainActor
    func moveUpNextIdempotentWhenOrderUnchanged() {
        var callCount = 0
        let vm = ActivityViewModel(persistQueueOrder: { _ in callCount += 1 })

        let inputs: [ActivityEpisodeInput] = (0..<3).map { i in
            Self.makeInput(
                id: "ep-\(i)",
                status: Self.makeStatus(
                    disposition: .queued, reason: .waitingForTime
                ),
                queuePosition: nil
            )
        }
        vm.refresh(from: inputs)

        // SwiftUI's `.onMove` can fire (source: {1}, destination: 1)
        // for a drag that ends at its origin; `.move(fromOffsets:
        // toOffset:)` returns the array unchanged in that case. The VM
        // must not write through.
        vm.moveUpNext(from: IndexSet(integer: 1), to: 1)
        #expect(callCount == 0)
        #expect(vm.snapshot.upNext.map(\.episodeId) == [
            "ep-0", "ep-1", "ep-2"
        ])

        // A real reorder still writes through once.
        vm.moveUpNext(from: IndexSet(integer: 2), to: 0)
        #expect(callCount == 1)
    }

    // MARK: - playhead-cjqq: schema migration round-trip

    @Test("Episode round-trips with queuePosition unset (additive optional default)")
    @MainActor
    func episodeRoundTripsWithQueuePositionNil() throws {
        let ctx = try makeCjqqInMemoryContext()
        let ep = Episode(
            feedItemGUID: "guid-nil",
            feedURL: URL(string: "https://example.com/rss")!,
            title: "Nil-position Episode",
            audioURL: URL(string: "https://example.com/a.mp3")!
        )
        ctx.insert(ep)
        try ctx.save()

        let rows = try ctx.fetch(FetchDescriptor<Episode>())
        #expect(rows.count == 1)
        #expect(rows.first?.queuePosition == nil)
    }

    @Test("Episode round-trips with queuePosition set (mutable + persisted)")
    @MainActor
    func episodeRoundTripsWithQueuePositionSet() throws {
        let ctx = try makeCjqqInMemoryContext()
        let ep = Episode(
            feedItemGUID: "guid-numbered",
            feedURL: URL(string: "https://example.com/rss")!,
            title: "Numbered Episode",
            audioURL: URL(string: "https://example.com/a.mp3")!,
            queuePosition: 5
        )
        ctx.insert(ep)
        try ctx.save()

        var rows = try ctx.fetch(FetchDescriptor<Episode>())
        #expect(rows.first?.queuePosition == 5)

        // Mutability — re-assigning the column persists.
        rows.first?.queuePosition = 0
        try ctx.save()
        let rereads = try ctx.fetch(FetchDescriptor<Episode>())
        #expect(rereads.first?.queuePosition == 0)
    }

    // MARK: - playhead-cjqq: drag survives ActivityRefreshNotification

    /// Central acceptance test (per bd spec): drag-reorder is persisted
    /// to the SwiftData column, so the next `loadInputs()` call (which
    /// is what the production `ActivityRefreshNotification` handler
    /// triggers) returns episodes in the user's manual order rather
    /// than scheduler-derived order.
    ///
    /// We exercise the full path:
    ///   1. Insert N episodes (all queuePosition == nil).
    ///   2. Build a provider closure that fetches them and produces
    ///      `ActivityEpisodeInput`s (mirrors `LiveActivitySnapshotProvider`'s
    ///      forwarder logic without needing AnalysisStore).
    ///   3. Refresh → reorder → call the persistence closure (the same
    ///      one production wires) → refresh again from the same model
    ///      context. Assert the post-refresh order matches the
    ///      post-move order.
    ///
    /// This is the test the spec specifically calls out: "drag (call
    /// moveUpNext), then post the refresh notification, then assert
    /// the order is preserved".
    @Test("drag-reorder persists across simulated ActivityRefreshNotification")
    @MainActor
    func dragSurvivesRefreshNotification() throws {
        let ctx = try makeCjqqInMemoryContext()
        let feedURL = URL(string: "https://example.com/rss")!

        // Insert four episodes in canonical id order. They start with
        // `queuePosition == nil` so the aggregator's nil-last sort
        // gives a deterministic id-asc fallback.
        for i in 0..<4 {
            ctx.insert(
                Episode(
                    feedItemGUID: "guid-\(i)",
                    feedURL: feedURL,
                    title: "Episode \(i)",
                    audioURL: URL(string: "https://example.com/a\(i).mp3")!
                )
            )
        }
        try ctx.save()

        // Provider closure: SwiftData fetch → ActivityEpisodeInput list.
        // Mirrors the relevant forwarder slice of
        // `LiveActivitySnapshotProvider.loadInputs()`. We bypass the
        // AnalysisStore filter (unrelated to queuePosition routing) by
        // synthesizing a queued/non-running status for every row.
        let queuedStatus = Self.makeStatus(
            disposition: .queued, reason: .waitingForTime
        )
        let loadInputs: @MainActor () -> [ActivityEpisodeInput] = {
            let rows = (try? ctx.fetch(FetchDescriptor<Episode>())) ?? []
            return rows.map { episode in
                Self.makeInput(
                    id: episode.canonicalEpisodeKey,
                    title: episode.title,
                    podcast: nil,
                    status: queuedStatus,
                    isRunning: false,
                    queuePosition: episode.queuePosition
                )
            }
        }

        // Persist callback identical in shape to the one wired in
        // ContentView.
        let persist: @MainActor ([(episodeId: String, queuePosition: Int)]) -> Void = { ordering in
            for entry in ordering {
                let key = entry.episodeId
                let descriptor = FetchDescriptor<Episode>(
                    predicate: #Predicate { $0.canonicalEpisodeKey == key }
                )
                if let row = try? ctx.fetch(descriptor).first {
                    row.queuePosition = entry.queuePosition
                }
            }
            try? ctx.save()
        }

        let vm = ActivityViewModel(persistQueueOrder: persist)

        // Initial refresh → all-nil → id-asc fallback.
        vm.refresh(from: loadInputs())
        let initialIds = vm.snapshot.upNext.map(\.episodeId)
        #expect(initialIds.count == 4)
        // Capture index of the row we will move so the assertion does
        // not assume a particular hash-order (canonicalEpisodeKey is
        // derived from feedURL + guid; ordering is deterministic but
        // the test reads more cleanly with explicit lookup).
        let movedId = initialIds[3]

        // User drags the last row to the front.
        vm.moveUpNext(from: IndexSet(integer: 3), to: 0)
        let postMoveIds = vm.snapshot.upNext.map(\.episodeId)
        #expect(postMoveIds.first == movedId)
        #expect(postMoveIds.count == 4)

        // Simulate an ActivityRefreshNotification post: the production
        // handler is `Task { await refresh() }` which calls
        // `loadInputs()` and then `viewModel.refresh(from:)`. We do
        // exactly that here against the same model context — the
        // queuePosition column the persist closure just wrote must
        // round-trip into the next snapshot.
        vm.refresh(from: loadInputs())
        let postRefreshIds = vm.snapshot.upNext.map(\.episodeId)
        #expect(postRefreshIds == postMoveIds,
                "Drag-reorder lost across refresh — expected \(postMoveIds), got \(postRefreshIds)")

        // Sanity: the persisted queuePositions are 0,1,2,3 in the new
        // visual order (sequential renumber).
        let allRows = try ctx.fetch(FetchDescriptor<Episode>())
        let positionByKey = Dictionary(
            uniqueKeysWithValues: allRows.compactMap { row -> (String, Int)? in
                guard let pos = row.queuePosition else { return nil }
                return (row.canonicalEpisodeKey, pos)
            }
        )
        for (i, id) in postMoveIds.enumerated() {
            #expect(positionByKey[id] == i,
                    "Episode \(id) at visual index \(i) should have queuePosition \(i)")
        }
    }

    // MARK: - playhead-5nwy: load-state for activity skeleton

    @Test("Initial loadState is .idle (no fetch attempted yet)")
    @MainActor
    func loadStateStartsIdle() {
        let vm = ActivityViewModel()
        #expect(vm.loadState == .idle)
    }

    @Test("beginLoad flips .idle → .loading")
    @MainActor
    func beginLoadEntersLoadingFromIdle() {
        let vm = ActivityViewModel()
        let started = Date(timeIntervalSince1970: 1_700_000_000)
        vm.beginLoad(now: started)
        if case let .loading(startedAt) = vm.loadState {
            #expect(startedAt == started)
        } else {
            Issue.record("Expected .loading after beginLoad — got \(vm.loadState)")
        }
    }

    @Test("refresh flips .loading → .loaded and exposes the snapshot")
    @MainActor
    func refreshEntersLoadedFromLoading() {
        let vm = ActivityViewModel()
        vm.beginLoad()
        vm.refresh(from: [], now: Date())
        #expect(vm.loadState == .loaded)
    }

    @Test("beginLoad after .loaded is a no-op — no flicker back into skeleton")
    @MainActor
    func beginLoadIdempotentOnceLoaded() {
        let vm = ActivityViewModel()
        vm.beginLoad()
        vm.refresh(from: [], now: Date())
        // Subsequent fetch tick should NOT roll the state back.
        vm.beginLoad()
        #expect(vm.loadState == .loaded)
    }

    // MARK: - playhead-btoa.1: DL/TX/AN fraction plumbing

    /// Variant of `makeInput` that exposes the three pipeline fraction
    /// fields plumbed through in playhead-btoa.1. Defaults match the
    /// production input contract: all three are `nil` unless the
    /// provider has a value to forward.
    static func makeInput(
        id: String,
        title: String = "Some Episode",
        podcast: String? = "Some Show",
        status: EpisodeSurfaceStatus,
        isRunning: Bool = false,
        finishedAt: Date? = nil,
        queuePosition: Int? = nil,
        downloadFraction: Double? = nil,
        transcriptFraction: Double? = nil,
        analysisFraction: Double? = nil
    ) -> ActivityEpisodeInput {
        ActivityEpisodeInput(
            episodeId: id,
            episodeTitle: title,
            podcastTitle: podcast,
            status: status,
            isRunning: isRunning,
            finishedAt: finishedAt,
            queuePosition: queuePosition,
            downloadFraction: downloadFraction,
            transcriptFraction: transcriptFraction,
            analysisFraction: analysisFraction
        )
    }

    @Test("DL/TX/AN fractions land on ActivityNowRow (queued + isRunning)")
    func nowRowCarriesPipelineFractions() {
        let input = Self.makeInput(
            id: "ep-now",
            status: Self.makeStatus(
                disposition: .queued,
                reason: .waitingForTime
            ),
            isRunning: true,
            downloadFraction: 0.42,
            transcriptFraction: 0.33,
            analysisFraction: 0.10
        )
        let snapshot = ActivityViewModel.aggregate(inputs: [input], now: Date())
        let row = try! #require(snapshot.now.first)
        #expect(row.downloadFraction == 0.42)
        #expect(row.transcriptFraction == 0.33)
        #expect(row.analysisFraction == 0.10)
    }

    @Test("DL/TX/AN fractions land on ActivityUpNextRow (queued + !isRunning)")
    func upNextRowCarriesPipelineFractions() {
        let input = Self.makeInput(
            id: "ep-up-next",
            status: Self.makeStatus(
                disposition: .queued,
                reason: .waitingForTime
            ),
            isRunning: false,
            downloadFraction: 1.0,
            transcriptFraction: 0.5,
            analysisFraction: 0.0
        )
        let snapshot = ActivityViewModel.aggregate(inputs: [input], now: Date())
        let row = try! #require(snapshot.upNext.first)
        #expect(row.downloadFraction == 1.0)
        #expect(row.transcriptFraction == 0.5)
        #expect(row.analysisFraction == 0.0)
    }

    @Test("DL/TX/AN fractions land on ActivityPausedRow (disposition .paused)")
    func pausedRowCarriesPipelineFractions() {
        let input = Self.makeInput(
            id: "ep-paused",
            status: Self.makeStatus(
                disposition: .paused,
                reason: .phoneIsHot,
                hint: .wait
            ),
            isRunning: false,
            downloadFraction: 0.85,
            transcriptFraction: 0.60,
            analysisFraction: 0.25
        )
        let snapshot = ActivityViewModel.aggregate(inputs: [input], now: Date())
        let row = try! #require(snapshot.paused.first)
        #expect(row.downloadFraction == 0.85)
        #expect(row.transcriptFraction == 0.60)
        #expect(row.analysisFraction == 0.25)
    }

    @Test("ActivityRecentlyFinishedRow has no slot for pipeline fractions (structural)")
    func recentlyFinishedRowHasNoPipelineFractionSlots() {
        // Structural assertion: terminal rows do not surface the strip
        // per design. Mirror's child labels expose stored properties at
        // runtime; we assert the three fraction labels are absent on
        // ActivityRecentlyFinishedRow.
        let row = ActivityRecentlyFinishedRow(
            episodeId: "ep-done",
            title: "Done Episode",
            podcastTitle: nil,
            outcome: .success,
            finishedAt: Date()
        )
        let mirrorLabels = Set(Mirror(reflecting: row).children.compactMap(\.label))
        #expect(!mirrorLabels.contains("downloadFraction"))
        #expect(!mirrorLabels.contains("transcriptFraction"))
        #expect(!mirrorLabels.contains("analysisFraction"))
    }
}

/// Builds an in-memory `ModelContext` for the cjqq tests. Mirrors
/// `makeDiagnosticsInMemoryContext()` but kept private to this suite so
/// future schema additions in the diagnostics suites do not silently
/// re-shape the cjqq fixtures.
@MainActor
private func makeCjqqInMemoryContext() throws -> ModelContext {
    let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
    let config = ModelConfiguration(isStoredInMemoryOnly: true)
    let container = try ModelContainer(for: schema, configurations: [config])
    return ModelContext(container)
}
