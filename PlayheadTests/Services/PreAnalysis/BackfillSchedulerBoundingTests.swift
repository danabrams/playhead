// BackfillSchedulerBoundingTests.swift
// playhead-c25o: the backfill BGTask handler must not be able to park on
// an out-of-process reply, and must arm its expiration handler before it
// awaits anything.
//
// The defect being pinned: `scheduleBackfillIfNeeded()` sets
// `backfillRescheduleInFlight = true` with a `defer` reset and then
// awaits `pendingTaskRequestIdentifiers()`, which used to be an
// unbounded `withCheckedContinuation` resumed only by a `dasd` reply.
// `handleBackfillTask` awaited that BEFORE installing its
// `expirationHandler`. One lost reply therefore meant (a) no
// `setTaskCompleted` and no armed expiration handler, so iOS killed the
// process and penalised future scheduling, and (b) the `defer` never
// ran, so the latch stayed set and every later backfill/feed-refresh
// reschedule was a no-op for the process lifetime.

import BackgroundTasks
import Foundation
import Testing
@testable import Playhead

// MARK: - Test doubles

/// A scheduler whose pending-requests query is bridged through the SAME
/// production wrapper `BGTaskScheduler` now uses, but whose "daemon"
/// never replies. This models a lost XPC reply exactly: the only thing
/// that can resolve the call is the wrapper's own timeout.
private final class NeverReplyingTaskScheduler: BackgroundTaskScheduling, @unchecked Sendable {
    private let lock = NSLock()
    private var submitted: [String] = []

    /// Short enough to keep the suite fast; the production value is
    /// asserted separately in `productionBridgeTimeoutIsShort`.
    static let stubTimeout: Duration = .milliseconds(20)

    var submittedIdentifiers: [String] { lock.withLock { submitted } }

    func submit(_ taskRequest: BGTaskRequest) throws {
        lock.withLock { submitted.append(taskRequest.identifier) }
    }

    func pendingTaskRequestIdentifiers() async -> [String] {
        await withBoundedCheckedContinuation(
            timeout: Self.stubTimeout,
            fallback: []
        ) { _ in
            // The daemon never replies.
        }
    }
}

/// Records, at every pending-requests query, whether the BG task's
/// `expirationHandler` had already been installed.
private final class ExpirationOrderScheduler: BackgroundTaskScheduling, @unchecked Sendable {
    private let lock = NSLock()
    private var task: StubBackgroundTask?
    private var armedAtQuery: [Bool] = []

    func attach(_ task: StubBackgroundTask) {
        lock.withLock { self.task = task }
    }

    /// `nil` when the scheduler was never queried.
    var armedAtFirstQuery: Bool? { lock.withLock { armedAtQuery.first } }
    var queryCount: Int { lock.withLock { armedAtQuery.count } }

    func submit(_ taskRequest: BGTaskRequest) throws {}

    func pendingTaskRequestIdentifiers() async -> [String] {
        recordQuery()
        return []
    }

    /// Synchronous so the lock is never taken from an `async` context.
    private func recordQuery() {
        lock.withLock {
            armedAtQuery.append(task?.expirationHandler != nil)
        }
    }
}

// MARK: - Suite

@Suite("Backfill scheduler bounding (playhead-c25o)")
struct BackfillSchedulerBoundingTests {

    private static let servicePath =
        "Playhead/Services/AnalysisCoordinator/BackgroundProcessingService.swift"

    private func makeService(
        scheduler: any BackgroundTaskScheduling,
        coordinator: StubAnalysisCoordinator = StubAnalysisCoordinator()
    ) -> BackgroundProcessingService {
        BackgroundProcessingService(
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService(),
            taskScheduler: scheduler,
            batteryProvider: StubBatteryProvider()
        )
    }

    // MARK: - Fail open + latch release

    @Test("A pending-requests reply that never arrives fails open instead of hanging",
          .timeLimit(.minutes(1)))
    func lostReplyFailsOpen() async {
        let scheduler = NeverReplyingTaskScheduler()
        let service = makeService(scheduler: scheduler)

        // Before the fix this call never returned; the test's time limit
        // would be the only thing that ended it.
        await service.scheduleBackfillIfNeeded()

        // Failing OPEN means "nothing pending", so both identifiers are
        // submitted. The cost of the wrong answer is a redundant submit.
        #expect(scheduler.submittedIdentifiers.contains(BackgroundTaskID.backfillProcessing))
        #expect(scheduler.submittedIdentifiers.contains(BackgroundTaskID.backfillProcessingCharged))
    }

    @Test("The reschedule latch is released on the timeout path",
          .timeLimit(.minutes(1)))
    func rescheduleLatchIsReleasedAfterTimeout() async {
        let scheduler = NeverReplyingTaskScheduler()
        let service = makeService(scheduler: scheduler)

        await service.scheduleBackfillIfNeeded()
        let afterFirst = scheduler.submittedIdentifiers.count
        #expect(afterFirst == 2)

        // THE LATCH. `backfillRescheduleInFlight` is private, so this is
        // its observable consequence: had the `defer` been skipped by a
        // parked continuation, this second call would return at the
        // reentrancy guard and submit nothing, forever.
        await service.scheduleBackfillIfNeeded()
        #expect(scheduler.submittedIdentifiers.count == afterFirst * 2,
                "a stuck backfillRescheduleInFlight would make every later reschedule a no-op")

        // And it stays released across repeated failures.
        await service.scheduleBackfillIfNeeded()
        #expect(scheduler.submittedIdentifiers.count == afterFirst * 3)
    }

    @Test("The backfill handler still completes when the scheduler query times out",
          .timeLimit(.minutes(1)))
    func backfillHandlerCompletesDespiteLostReply() async {
        let scheduler = NeverReplyingTaskScheduler()
        let coordinator = StubAnalysisCoordinator()
        let service = makeService(scheduler: scheduler, coordinator: coordinator)
        let task = StubBackgroundTask()

        await service.handleBackfillTask(task)
        await task.awaitCompletion()

        #expect(task.completedSuccess == true)
        // Reschedule-before-complete is preserved: the next window was
        // armed before iOS was told the task finished.
        #expect(scheduler.submittedIdentifiers.contains(BackgroundTaskID.backfillProcessing))
    }

    // MARK: - Expiration ordering

    @Test("The expiration handler is armed before the handler awaits anything",
          .timeLimit(.minutes(1)))
    func expirationHandlerArmedBeforeAwaitedWork() async {
        let scheduler = ExpirationOrderScheduler()
        let task = StubBackgroundTask()
        scheduler.attach(task)
        let service = makeService(scheduler: scheduler)

        await service.handleBackfillTask(task)

        // `handleBackfillTask` runs to the `expirationHandler` install
        // with no suspension point, so the handler is armed by the time
        // it returns — before any of the work task's awaits can run.
        #expect(task.expirationHandler != nil)

        await task.awaitCompletion()

        #expect(scheduler.queryCount >= 1, "the reschedule must still happen")
        #expect(scheduler.armedAtFirstQuery == true,
                "the reschedule's suspension point ran before the expiration handler was armed")
    }

    // MARK: - Source canaries

    @Test("handleBackfillTask has no suspension point before it arms the expiration handler")
    func handleBackfillTaskHasNoAwaitBeforeArming() throws {
        let source = try SwiftSourceInspector.loadSource(repoRelativePath: Self.servicePath)
        // playhead-lmrx (review round): anchored on the DECLARATION, not on a
        // whole one-line signature. `firstBody` takes the first `{` after the
        // anchor, so this survives parameters being added or the signature
        // being wrapped across lines — and playhead-lmrx did both, which broke
        // the old anchor. A canary that matches nothing does not fail quietly
        // here (the `#require` below catches it), but it was also not in any
        // suite the bead's own rounds ran, so nobody saw it for two commits.
        // Anchoring on what cannot drift is the fix; broadening the gate's
        // suite list is the other half.
        // `firstBody` takes the first `{` after the FIRST occurrence, so a
        // second occurrence (a doc comment quoting the declaration) would aim
        // this canary at the wrong site while every assertion below still ran.
        // Pin the count so that failure is named rather than inferred.
        let anchor = "func handleBackfillTask("
        #expect(source.components(separatedBy: anchor).count == 2,
                "the canary anchor '\(anchor)' must occur exactly once in \(Self.servicePath)")
        let body = try #require(
            SwiftSourceInspector.firstBody(in: source, after: anchor),
            "handleBackfillTask's declaration drifted — update this canary"
        )
        // `firstBody` returns "" (not nil) on an unbalanced brace, which
        // would make every check below vacuously true.
        try #require(!body.isEmpty, "handleBackfillTask's body did not parse")

        let code = SwiftSourceInspector.strippingComments(body)
        let topLevel = Self.elidingTaskClosureBodies(code)
        let arming = try #require(
            topLevel.range(of: "task.expirationHandler"),
            "handleBackfillTask no longer installs an expirationHandler"
        )
        let beforeArming = String(topLevel[topLevel.startIndex..<arming.lowerBound])

        // Positive controls: prove the helper actually found and elided
        // the task closures, so a silent parse failure cannot make the
        // assertion below pass by looking at nothing.
        #expect(topLevel.contains(Self.elisionMarker))
        #expect(beforeArming.contains("let workTask = Task"))

        #expect(!beforeArming.contains("await "),
                """
                handleBackfillTask must not suspend before installing its \
                expirationHandler — an OS reclaim during that window is \
                unreportable and kills the process (playhead-c25o). Move the \
                awaited work into the work task instead. Found: \
                \(beforeArming.trimmingCharacters(in: .whitespacesAndNewlines))
                """)
    }

    @Test("The production pending-requests bridge is bounded, not a bare continuation")
    func productionBridgeIsBounded() throws {
        let source = try SwiftSourceInspector.loadSource(repoRelativePath: Self.servicePath)
        let body = try #require(
            SwiftSourceInspector.firstBody(
                in: source,
                after: "extension BGTaskScheduler: BackgroundTaskScheduling {"
            ),
            "the BGTaskScheduler conformance moved — update this canary"
        )
        try #require(!body.isEmpty, "the BGTaskScheduler extension body did not parse")
        let code = SwiftSourceInspector.strippingComments(body)

        #expect(code.contains("withBoundedCheckedContinuation"))
        #expect(code.contains("fallback: []"),
                "the bridge must fail OPEN to no-pending-requests; callers treat it as advisory")
        #expect(!code.contains("await withCheckedContinuation"),
                "the dasd bridge must stay bounded — a bare continuation here can park forever")
        #expect(!code.contains("withUnsafeContinuation"),
                "an unsafe continuation would drop the double-resume trap that proves the once guard")
    }

    @Test("The production bridge timeout is short relative to the BGTask budget")
    func productionBridgeTimeoutIsShort() {
        let timeout = BGTaskScheduler.pendingTaskRequestsTimeout
        #expect(timeout > .zero)
        // The tightest caller is `appDidEnterBackground()`, which runs in
        // the few seconds iOS allows on the `.background` transition. A
        // timeout at or beyond the caller's own budget protects nothing.
        #expect(timeout <= .seconds(5))
    }

    // MARK: - Helpers

    private static let elisionMarker = "/*task-body-elided*/"

    /// Returns `source` with the body of every `Task { … }` /
    /// `Task.detached { … }` closure replaced by ``elisionMarker``, and
    /// EVERYTHING ELSE left intact — including the bodies of `if`,
    /// `guard else`, `for`, `switch` and `do`.
    ///
    /// That asymmetry is the whole point. The invariant under test is
    /// "no suspension point runs before the expiration handler is
    /// armed". A nested `Task` body is the only legitimate place for an
    /// `await` in that prefix, because it runs as a separate job; an
    /// `await` inside any other kind of brace suspends `handleBackfillTask`
    /// itself and is exactly the regression this canary exists to catch.
    /// A cheaper "keep only brace-depth zero" filter would have elided
    /// those too and passed on the very edit it is guarding against.
    ///
    /// Deliberately fails CLOSED: a `Task(priority:) { … }` form would
    /// not be recognised and its `await`s would be reported, which turns
    /// into a loud canary failure that someone updates — not a silent
    /// hole. Double-quoted string literals are skipped so a brace inside
    /// a log message cannot shift the depth.
    ///
    /// `source` is expected to have had comments stripped already.
    private static func elidingTaskClosureBodies(_ source: String) -> String {
        let characters = Array(source)
        var out = ""
        var index = 0
        var inString = false
        var escaped = false

        while index < characters.count {
            let character = characters[index]
            if inString {
                out.append(character)
                if escaped {
                    escaped = false
                } else if character == "\\" {
                    escaped = true
                } else if character == "\"" {
                    inString = false
                }
                index += 1
                continue
            }
            if character == "\"" {
                inString = true
                out.append(character)
                index += 1
                continue
            }
            if character == "{", opensTaskClosure(out) {
                index = indexAfterMatchingBrace(characters, openingAt: index)
                out.append(elisionMarker)
                continue
            }
            out.append(character)
            index += 1
        }
        return out
    }

    /// True when the text emitted so far ends with a `Task` reference,
    /// i.e. the `{` that follows opens a task closure rather than a
    /// control-flow block.
    private static func opensTaskClosure(_ emitted: String) -> Bool {
        let trimmed = emitted.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.hasSuffix("Task") || trimmed.hasSuffix("Task.detached")
    }

    /// Index just past the `}` matching the `{` at `start`.
    private static func indexAfterMatchingBrace(
        _ characters: [Character],
        openingAt start: Int
    ) -> Int {
        var depth = 0
        var index = start
        var inString = false
        var escaped = false
        while index < characters.count {
            let character = characters[index]
            if inString {
                if escaped {
                    escaped = false
                } else if character == "\\" {
                    escaped = true
                } else if character == "\"" {
                    inString = false
                }
            } else if character == "\"" {
                inString = true
            } else if character == "{" {
                depth += 1
            } else if character == "}" {
                depth -= 1
                if depth == 0 { return index + 1 }
            }
            index += 1
        }
        return characters.count
    }
}
