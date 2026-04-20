// PlayheadRuntimeNoteCommittedPlayheadTests.swift
// playhead-vhha: integration test that the production commit point
// (`PlayheadApp.persistPlaybackPosition`) reaches the
// `CandidateWindowCascade` via the scheduler's `noteCommittedPlayhead`
// entry point so the resumed-window selection actually tracks the user
// instead of staying pinned to whatever was last seeded.
//
// The cascade-facing wiring is encapsulated as a method on
// `PlayheadRuntime` (`noteCommittedPlayhead(episodeId:position:
// episodeDuration:)`) so this test can drive it directly without
// reaching through the SwiftUI App layer's SwiftData lookup. The
// production callsite in `PlayheadApp.persistPlaybackPosition` invokes
// the same runtime method after each successful position save.
//
// Verification strategy: seed an episode through the scheduler, then
// drive a > 30s synthetic seek through the runtime's wiring slot and
// assert that `currentCandidateWindows(for:)` reports a proximal
// window whose `range.lowerBound` matches the new anchor — i.e. the
// cascade re-latched on the wired path.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-vhha: PlayheadRuntime.noteCommittedPlayhead drives the cascade re-latch")
struct PlayheadRuntimeNoteCommittedPlayheadTests {

    @MainActor
    @Test("Above-threshold commit re-latches the cascade and rebases the proximal window")
    func aboveThresholdCommitRebasesProximalWindow() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            let episodeId = "ep-vhha-relatch"
            let episodeDuration: TimeInterval = 60 * 60
            let originalAnchor: TimeInterval = 10 * 60
            let newPosition: TimeInterval = 40 * 60  // > 30s away from anchor

            // Seed through the scheduler so the cascade has a baseline
            // anchor + window list before the synthetic commit fires.
            _ = await runtime.analysisWorkScheduler.seedCandidateWindows(
                episodeId: episodeId,
                episodeDuration: episodeDuration,
                playbackAnchor: originalAnchor,
                chapterEvidence: []
            )

            let beforeCommit = await runtime.analysisWorkScheduler
                .currentCandidateWindows(for: episodeId)
            #expect(beforeCommit.count == 1)
            #expect(beforeCommit.first?.kind == .proximal)
            #expect(beforeCommit.first?.range.lowerBound == originalAnchor)

            // Drive the wired commit-point. This is the exact method
            // PlayheadApp.persistPlaybackPosition calls after a
            // successful SwiftData save — the only thing the App layer
            // adds is the Episode lookup.
            await runtime.noteCommittedPlayhead(
                episodeId: episodeId,
                position: newPosition,
                episodeDuration: episodeDuration
            )

            let afterCommit = await runtime.analysisWorkScheduler
                .currentCandidateWindows(for: episodeId)
            #expect(
                afterCommit.first?.range.lowerBound == newPosition,
                "Cascade did not re-latch on the wired commit path — the resumed-window selection is still pinned to the seeded anchor"
            )
            #expect(afterCommit.first?.kind == .proximal)
        }
    }

    @MainActor
    @Test("Sub-threshold commit does not move the proximal window")
    func subThresholdCommitDoesNotRebase() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            let episodeId = "ep-vhha-no-relatch"
            let episodeDuration: TimeInterval = 60 * 60
            let originalAnchor: TimeInterval = 10 * 60
            let newPosition: TimeInterval = 10 * 60 + 25  // <= 30s

            _ = await runtime.analysisWorkScheduler.seedCandidateWindows(
                episodeId: episodeId,
                episodeDuration: episodeDuration,
                playbackAnchor: originalAnchor,
                chapterEvidence: []
            )

            await runtime.noteCommittedPlayhead(
                episodeId: episodeId,
                position: newPosition,
                episodeDuration: episodeDuration
            )

            let after = await runtime.analysisWorkScheduler
                .currentCandidateWindows(for: episodeId)
            #expect(after.first?.range.lowerBound == originalAnchor)
        }
    }
}
