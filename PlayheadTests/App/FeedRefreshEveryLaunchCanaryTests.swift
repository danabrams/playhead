// FeedRefreshEveryLaunchCanaryTests.swift
// playhead-m8rq: the feed-refresh service must be attached on EVERY launch,
// not only on one that builds a scene.
//
// THE DEFECT. `BGTaskScheduler.register` runs early, at runtime init, and
// routes fires through a process-wide holder that `attachSharedService(_:)`
// fills. That call lived inside the scene's `.task` — a SwiftUI scene
// modifier. A `BGAppRefreshTask` wake HAS NO SCENE, so `.task` never ran, the
// holder stayed nil, and the registered handler took its documented fallback:
// schedule the next fire and complete the task gracefully. Every headless
// refresh therefore refreshed no feeds and enqueued no auto-downloads.
//
// That is Dan's "wake up each morning and see three new episodes downloaded and
// analysed" (2026-08-11) failing with nothing to see and nothing to read.
//
// WHY THIS IS A SOURCE CANARY. The defect is a property of WHERE the call sits
// in the file, and the process that exhibits it is one no unit test can build:
// a launch with no scene. A runtime test would have to construct a scene to
// observe the thing whose absence is the bug. So this reads the source, which
// is the same tool `PlayheadRuntimeWiringSourceCanaryTests` and
// `BackgroundDownloadDropWiringSourceCanaryTests` use for the same class of
// wiring fact.
//
// SIXTH INSTANCE of the sceneless-launch class (memory:
// project_sceneless_launch_defect_class_2026-08-10). The question that finds it
// every time is: what registered this, and does that run on EVERY launch?

import Foundation
import Testing

@testable import Playhead

@Suite("Feed refresh attaches on every launch (playhead-m8rq)")
struct FeedRefreshEveryLaunchCanaryTests {

    private static func appSource() throws -> String {
        let root = try #require(
            SwiftSourceInspector.repositoryRoot(from: #filePath),
            "could not locate the repository root from \(#filePath)"
        )
        let url = root.appendingPathComponent("Playhead/App/PlayheadApp.swift")
        return try String(contentsOf: url, encoding: .utf8)
    }

    /// Offset of `var body: some Scene`. Everything after it is scene-scoped and
    /// does not run on a headless wake.
    private static func sceneBodyOffset(_ source: String) throws -> String.Index {
        try #require(
            source.range(of: "var body: some Scene")?.lowerBound,
            "PlayheadApp no longer declares `var body: some Scene` — re-read this canary"
        )
    }

    /// `init`'s body, delimited by BRACE MATCHING rather than by the next
    /// declaration.
    ///
    /// THE FIRST VERSION OF THIS CANARY TOOK EVERYTHING FROM `init() {` TO
    /// `var body`, AND IT WAS VACUOUS. That span also contains the static
    /// helpers this bead added, so `contains("attachFeedRefreshForEveryLaunch")`
    /// matched the helper's own DECLARATION and passed with the defect fully
    /// re-introduced — proven by applying the mutant and watching all five
    /// tests stay green. A region that names one thing (init's body) while
    /// covering another (init plus every declaration after it) is the standing
    /// defect class, committed by the rail written to catch it.
    private static func initBody(_ source: String) throws -> Substring {
        let start = try #require(
            source.range(of: "init() {"),
            "PlayheadApp no longer has an `init() {`"
        )
        var depth = 1
        var index = start.upperBound
        while index < source.endIndex, depth > 0 {
            if source[index] == "{" { depth += 1 }
            if source[index] == "}" { depth -= 1 }
            if depth == 0 { break }
            index = source.index(after: index)
        }
        #expect(depth == 0, "init's braces do not balance — re-read this canary")
        return source[start.upperBound..<index]
    }

    @Test("THE ACCEPTANCE: init attaches the feed-refresh service")
    func initAttachesTheService() throws {
        let initRegion = try Self.initBody(Self.appSource())

        #expect(
            initRegion.contains("attachFeedRefreshForEveryLaunch"),
            """
            PlayheadApp.init no longer attaches the feed-refresh service. A \
            BGAppRefreshTask wake has NO SCENE, so an attach that happens only \
            in `.task` leaves the process-wide holder nil on exactly the \
            launches a background refresh runs on — and the registered handler \
            silently reschedules instead of refreshing.
            """
        )
    }

    /// The direction that actually regressed. Someone moving the attach back
    /// into the scene would leave a call to `attachSharedService` in the file
    /// and pass a naive "is it wired?" check — which is how it shipped.
    @Test("the attach is NOT scene-only: it appears before `var body`")
    func attachIsNotSceneOnly() throws {
        let source = try Self.appSource()
        let initRegion = try Self.initBody(source)
        let sceneScope = source[try Self.sceneBodyOffset(source)...]

        // The attach must be reachable from a launch with no scene. `init`
        // calling it is the only such path in this file.
        #expect(
            initRegion.contains("attachFeedRefreshForEveryLaunch"),
            """
            The only attach left is inside scene scope. That is the \
            playhead-m8rq defect exactly: correct on a normal launch, inert \
            on a headless one.
            """
        )
        #expect(
            sceneScope.contains("attachSharedService"),
            "the scene path must still attach — a normal launch is unchanged"
        )
    }

    /// `start()` is scene work — it drives the foreground refresh cadence — and
    /// putting it on the headless path would start a loop nothing tends. The
    /// holder is what a BGTask fire needs, and that is all init installs.
    @Test("init attaches WITHOUT starting the foreground refresh loop")
    func initDoesNotStartTheLoop() throws {
        let initRegion = try Self.initBody(Self.appSource())

        #expect(
            !initRegion.contains("feedRefreshService.start()"),
            "start() is scene work; init installs the holder only"
        )
    }

    /// ANTI-VACUITY. Every assertion above is a substring search, and a search
    /// over a file that failed to load, or over the wrong file, passes for the
    /// worst possible reason. This proves the source really is PlayheadApp.
    @Test("the canary is reading the file it thinks it is")
    func canaryReadsTheRealSource() throws {
        let source = try Self.appSource()
        #expect(source.contains("struct PlayheadApp"))
        #expect(source.contains("var body: some Scene"))
        #expect(source.count > 5_000, "a truncated read would pass every check above")
        // And that `initBody` really isolates init rather than swallowing the
        // declarations after it — the exact way this canary was vacuous first
        // time. init is tens of lines; the file is hundreds.
        let initRegion = try Self.initBody(source)
        #expect(!initRegion.isEmpty)
        #expect(
            !initRegion.contains("var body: some Scene"),
            "initBody has swallowed the scene declaration — it is not isolating init"
        )
        #expect(
            !initRegion.contains("static func makeFeedRefreshService"),
            "initBody has swallowed the helper DECLARATIONS, which is how this rail passed with the defect in place"
        )
    }

    /// The scene path still does both, so a normal launch is unchanged: the
    /// attach is idempotent by its own contract, and `start()` still runs.
    @Test("the scene path still attaches and starts, so a normal launch is unchanged")
    func scenePathIsUnchanged() throws {
        let source = try Self.appSource()
        let bodyStart = try Self.sceneBodyOffset(source)
        let sceneScope = source[bodyStart...]
        #expect(sceneScope.contains("attachSharedService"))
        #expect(sceneScope.contains("feedRefreshService.start()"))
    }
}
