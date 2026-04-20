// PlayheadRuntimeCandidateWindowCascadeTests.swift
// playhead-xiz6: integration test that the production composition root
// (`PlayheadRuntime`) wires a real `CandidateWindowCascade` into the
// `AnalysisWorkScheduler`. Before this bead the runtime constructed the
// scheduler without supplying a cascade, leaving the c3pi entry points
// (`seedCandidateWindows`, `noteCommittedPlayhead`,
// `currentCandidateWindows`) unreachable in production — every call would
// silently no-op and follow-up beads (`playhead-vhha` seek wiring,
// `playhead-swws` runner consumption) would have nothing to consume.
//
// Verification strategy: the test asserts the wiring **behaviorally**
// rather than reaching for a private accessor. The scheduler's c3pi
// entry points are documented to return empty / nil when no cascade is
// injected; calling `seedCandidateWindows(...)` against the runtime's
// scheduler and observing a non-empty `[CandidateWindow]` proves the
// cascade is both present AND functional. This sidesteps widening the
// scheduler's API surface for a test seam (the existing
// `pendingCancelCauseForTesting` pattern is for state that can ONLY be
// observed from the inside; cascade presence is observable from the
// outside via the public entry points themselves).
//
// Uses `withTestRuntime(isPreviewRuntime: true)` because cascade wiring
// is unconditional (independent of the preview gate that controls the
// shadow-retry observer), so a preview runtime is sufficient to exercise
// the scheduler construction path. The preview path also avoids the
// `BGTaskScheduler.registerBackgroundTasks()` once-per-process latch
// that constrains how many non-preview runtimes a single process may
// construct.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-xiz6: PlayheadRuntime wires CandidateWindowCascade into AnalysisWorkScheduler")
struct PlayheadRuntimeCandidateWindowCascadeTests {

    @MainActor
    @Test("Runtime-constructed scheduler returns a non-empty proximal window from seedCandidateWindows (cascade is wired)")
    func runtimeWiresCascadeIntoScheduler() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            // Seed a representative episode through the scheduler's c3pi
            // entry point. With no cascade injected this returns []. With
            // a real cascade, the proximal-window selection runs and we
            // get a single `.proximal` window covering the configured
            // unplayed-window length from episode start.
            let windows = await runtime.analysisWorkScheduler.seedCandidateWindows(
                episodeId: "ep-xiz6-runtime-wiring",
                episodeDuration: 60 * 60,
                playbackAnchor: nil,
                chapterEvidence: []
            )

            #expect(
                !windows.isEmpty,
                "PlayheadRuntime must inject a CandidateWindowCascade into AnalysisWorkScheduler — empty result means the scheduler was constructed with cascade=nil and the c3pi entry points are unreachable in production"
            )
            #expect(windows.count == 1)
            #expect(windows.first?.kind == .proximal)
            #expect(windows.first?.range.lowerBound == 0)

            // Round-trip the cascade state through the scheduler's
            // read-only accessor — also a no-op when no cascade is
            // wired, so this is a second independent witness that the
            // injection happened.
            let stored = await runtime.analysisWorkScheduler
                .currentCandidateWindows(for: "ep-xiz6-runtime-wiring")
            #expect(stored == windows)
        }
    }
}
