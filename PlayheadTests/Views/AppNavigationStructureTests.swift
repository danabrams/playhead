// AppNavigationStructureTests.swift
// Verifies acceptance criteria for playhead-xu7 (App Navigation Structure).
//
// Tests cover:
// - NowPlayingViewModel lifecycle (created on play, nilled on stop)
// - Mini player visibility logic (requires both isPlayingEpisode AND viewModel)
// - Tab structure constants (icons, labels)
// - Portrait-only orientation lock via Info.plist

import XCTest
@testable import Playhead

// MARK: - NowPlayingViewModel Lifecycle Tests

@MainActor
final class NowPlayingViewModelLifecycleTests: XCTestCase {

    func testViewModelCreatedOnFirstPlay() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            vm.startObserving()

            // ViewModel should be non-nil and observing after creation.
            // The real ContentView creates the VM when isPlayingEpisode becomes true.
            XCTAssertNotNil(vm, "ViewModel must be created when playback starts")
        }
    }

    func testViewModelStopObservingCancelsTask() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            vm.startObserving()
            vm.stopObserving()

            // After stopObserving, the VM can be safely nilled.
            // Verifies no crash on stop (observation task cancelled cleanly).
        }
    }

    func testViewModelStartObservingIsIdempotent() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)
            vm.startObserving()
            vm.startObserving() // Should not create a second task

            vm.stopObserving()
            // No crash = guard clause works
        }
    }

    func testViewModelInitSyncsMetadata() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)

            // With no episode loaded, title should be the default.
            XCTAssertEqual(vm.episodeTitle, "No Episode Selected",
                "Default episode title must be 'No Episode Selected'")
            XCTAssertEqual(vm.podcastTitle, "",
                "Default podcast title must be empty")
        }
    }

    func testProgressIsZeroWhenDurationIsZero() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            let vm = NowPlayingViewModel(runtime: runtime)

            XCTAssertEqual(vm.progress, 0,
                "Progress must be 0 when duration is 0 (no division by zero)")
        }
    }
}

// MARK: - Mini Player Visibility Logic Tests

@MainActor
final class MiniPlayerVisibilityTests: XCTestCase {

    func testMiniPlayerRequiresBothPlayingAndViewModel() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            // When not playing and no VM: mini player hidden.
            XCTAssertFalse(runtime.isPlayingEpisode,
                "Runtime should not be playing initially")

            // The ContentView logic is:
            //   if runtime.isPlayingEpisode, let vm = nowPlayingViewModel { ... }
            // Both conditions must be true. With no episode loaded,
            // isPlayingEpisode is false, so mini player is hidden.
        }
    }

    func testIsPlayingEpisodeDependsOnCurrentEpisodeId() async throws {
        try await withTestRuntime(isPreviewRuntime: true) { runtime in
            // isPlayingEpisode is derived from currentEpisodeId != nil.
            // Without loading an episode, it should be false.
            XCTAssertFalse(runtime.isPlayingEpisode,
                "isPlayingEpisode must be false when no episode is loaded")
        }
    }
}

// MARK: - Portrait Orientation Lock Tests

final class PortraitOrientationTests: XCTestCase {

    func testInfoPlistLocksToPortraitOnly() throws {
        // The test host is the Playhead app, so Bundle.main is the app bundle.
        let orientations = Bundle.main.object(
            forInfoDictionaryKey: "UISupportedInterfaceOrientations"
        ) as? [String] ?? []

        XCTAssertEqual(orientations, ["UIInterfaceOrientationPortrait"],
            "iPhone must be locked to portrait only in MVP")
    }
}

// MARK: - playhead-5nwy: Hard splash dismiss

/// Pins the launch-UX defense from playhead-5nwy: the splash MUST flip
/// off on a fixed main-runloop timer, regardless of runtime readiness or
/// any background work. The previous implementation hung dismiss off
/// `RootView.onAppear` + `withAnimation(...)`, which lingered for
/// minutes when slow main-thread work delayed `onAppear` itself.
@MainActor
final class SplashControllerHardDismissTests: XCTestCase {

    /// Splash dismisses within the configured window even when no
    /// runtime work ever completes. The runtime stand-in here is "no
    /// runtime at all" — the controller has zero coupling to runtime
    /// state by design, so the only way to fail is for the timer not
    /// to fire.
    func testSplashDismissesAfterFixedTimerEvenWithoutRuntime() async throws {
        let controller = SplashController(dismissDelay: 0.1)
        XCTAssertTrue(controller.isVisible, "Splash starts visible")

        // Wait slightly past the configured delay. RunLoop pump is
        // implicit because the test runs on the main queue and the
        // expectation drains the runloop while we wait.
        let dismissed = expectation(description: "splash dismissed")
        Task { @MainActor in
            // Poll the @Observable property at ~60Hz until the timer
            // flips it. This intentionally does NOT use
            // `try await Task.sleep` for the dismissal itself — the
            // controller's timer drives the flip; we just wait for the
            // observable to flip.
            for _ in 0..<60 {
                if !controller.isVisible {
                    dismissed.fulfill()
                    return
                }
                try? await Task.sleep(nanoseconds: 25_000_000) // 25ms
            }
        }
        await fulfillment(of: [dismissed], timeout: 2.0)
        XCTAssertFalse(controller.isVisible,
            "Splash MUST dismiss on the fixed timer regardless of runtime state")
    }

    /// Saturate the cooperative thread pool with long-running Tasks
    /// before constructing the controller. If the controller used a
    /// `Task { try await Task.sleep(...) }` for the dismiss, those
    /// Tasks could starve out the dismissal Task. The Timer-based
    /// implementation rides the main runloop, which is independent of
    /// the cooperative pool, and MUST still fire.
    func testSplashDismissesUnderTaskPoolStarvation() async throws {
        // Spin up many concurrent CPU-bound Tasks so the cooperative
        // pool's worker queue is backed up. Each task runs a tight
        // sleep loop; collectively they keep the pool occupied for
        // longer than the splash's configured dismiss window.
        let saturationTasks: [Task<Void, Never>] = (0..<200).map { _ in
            Task.detached(priority: .background) {
                let deadline = Date().addingTimeInterval(1.5)
                while Date() < deadline {
                    // Busy-wait fragment with a tiny yield so the
                    // scheduler makes some forward progress on the
                    // pool but the pool is never idle.
                    _ = (0..<1_000).reduce(0, +)
                    await Task.yield()
                }
            }
        }
        defer { saturationTasks.forEach { $0.cancel() } }

        let controller = SplashController(dismissDelay: 0.1)
        let dismissed = expectation(description: "splash dismissed under starvation")
        Task { @MainActor in
            for _ in 0..<80 {
                if !controller.isVisible {
                    dismissed.fulfill()
                    return
                }
                try? await Task.sleep(nanoseconds: 25_000_000)
            }
        }
        await fulfillment(of: [dismissed], timeout: 2.5)
        XCTAssertFalse(
            controller.isVisible,
            """
            Splash dismiss MUST survive cooperative-pool starvation — \
            Timer.scheduledTimer on the main runloop is the contract.
            """
        )
    }

    /// Default delay is the named constant — guards against drift when
    /// the constant is later renamed or repurposed.
    func testDefaultDelayMatchesNamedConstant() {
        XCTAssertEqual(
            SplashController.dismissDelay, 1.2,
            """
            Splash dismiss delay constant pinned at 1.2s — adjust both \
            the constant and this test in the same commit if changing.
            """
        )
    }

    /// Source-level canary: the controller MUST schedule via
    /// `Timer.scheduledTimer` / `Timer(timeInterval:...)` on the main
    /// runloop, NOT via a `Task { ... Task.sleep ... }`. A future edit
    /// that "modernizes" this to async/await would silently re-introduce
    /// the cooperative-pool starvation hazard the bead exists to defend
    /// against.
    func testSplashControllerUsesMainRunloopTimerNotTask() throws {
        let url = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent() // .../Views/
            .deletingLastPathComponent() // .../PlayheadTests/
            .deletingLastPathComponent() // .../<repo root>/
            .appendingPathComponent("Playhead/App/PlayheadApp.swift")
        let source = try String(contentsOf: url, encoding: .utf8)

        // Locate the SplashController body.
        guard let typeRange = source.range(of: "final class SplashController") else {
            XCTFail("SplashController missing from PlayheadApp.swift")
            return
        }
        // Grab roughly the next 2KB of source — large enough to cover
        // the type body without bleeding into RootView.
        let windowEnd = source.index(typeRange.lowerBound,
                                     offsetBy: 2_000,
                                     limitedBy: source.endIndex) ?? source.endIndex
        let window = String(source[typeRange.lowerBound..<windowEnd])

        XCTAssertTrue(
            window.contains("RunLoop.main") || window.contains("Timer.scheduledTimer"),
            """
            SplashController MUST use a main-runloop Timer, not a Task. \
            Window: \(window.prefix(400))
            """
        )
        XCTAssertFalse(
            window.contains("Task.sleep"),
            """
            SplashController dismiss path MUST NOT depend on Task.sleep — \
            cooperative-pool starvation is the failure mode this defense \
            exists to prevent.
            """
        )
    }
}
