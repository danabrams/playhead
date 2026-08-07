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

    @Test("both BGTask handlers spend the BUDGET they were handed, not a local constant")
    func handlersThreadTheirBudgetIntoTheDrain() throws {
        // playhead-lmrx (review round 5): THE ARGUMENT, not the callee.
        //
        // `DrainEligibleStartGateTests` calls `drainEligible(deadline:
        // minimumCheckpointBudget:)` by hand with `.seconds(60)`, and rail LX04
        // mutates the guard INSIDE `drainEligible`. Between them they prove the
        // start gate works and that its callee honours it. Neither can see the
        // handler's ARGUMENT: change both call sites to
        // `minimumCheckpointBudget: .zero` and production reverts to the
        // pre-lmrx bare `now < deadline` — a whole analysis job may begin with a
        // millisecond of grant left — while every behavioural test stays green.
        // Ask the diagnostic question of the suite: what would it read if the
        // handlers passed a floor of zero? Exactly what it reads now.
        //
        // A SOURCE canary because the alternative is a new DEBUG seam recording
        // an argument, and the floor's effect is not separately observable
        // through the handler: the handler also starts the long-lived
        // `runLoop()`, which dispatches on its own poll regardless of the floor.
        //
        // THE DOOR CLAUSE COUNTS, AND DID NOT USED TO (review round 6). Round 5
        // added it to cover the recovery EXPIRY door, on the grounds that the
        // other three `closeDispatchForTeardown` sites were pinned
        // behaviourally and that one had no test driving recovery's
        // `expirationHandler`. But `contains` asks whether the string appears
        // ANYWHERE in the handler body, and each handler has TWO sites — its
        // work-deadline return and its expiry — so deleting either one left the
        // other satisfying the canary and the whole suite green. That is this
        // bead's own standing defect class read back into its instrument: a
        // value that reads the same whether or not the thing it claims to
        // measure happened. All four sites are now pinned behaviourally in
        // `BackgroundGrantBudgetTests`; this clause is the source-level
        // backstop, and it counts.
        let source = try SwiftSourceInspector.loadSource(repoRelativePath: Self.servicePath)
        for anchor in ["func handleBackfillTask(", "func handlePreAnalysisRecovery("] {
            #expect(source.components(separatedBy: anchor).count == 2,
                    "the canary anchor '\(anchor)' must occur exactly once in \(Self.servicePath)")
            let body = try #require(
                SwiftSourceInspector.firstBody(in: source, after: anchor),
                "\(anchor) drifted — update this canary"
            )
            try #require(!body.isEmpty, "\(anchor) body did not parse")
            let code = SwiftSourceInspector.strippingComments(body)

            #expect(code.contains("minimumCheckpointBudget: budget.minimumCheckpointBudget"),
                    """
                    \(anchor) must hand `drainEligible` the floor from the \
                    budget it is spending. A literal (or the `.zero` default) \
                    is the unmeasured constant `BackgroundGrantBudget` exists \
                    to delete.
                    """)
            let doorSites = code.components(
                separatedBy: "closeDispatchForTeardown(lasting: budget.teardownReserve)"
            ).count - 1
            #expect(doorSites == 2,
                    """
                    \(anchor) has TWO endings — its work-deadline return and \
                    its expiration handler — and BOTH must shut the dispatch \
                    door out of its own budget's teardown reserve before they \
                    cancel; otherwise `runLoop()`'s poll can dispatch a fresh \
                    job after the cancel has gone past, live at \
                    `setTaskCompleted`. Found \(doorSites). If a third ending \
                    was added deliberately, raise this number and pin the new \
                    site behaviourally too — do not relax it back to a \
                    `contains`, which cannot tell two sites from one.
                    """)
            #expect(code.contains("workDeadline(from:"),
                    """
                    and it must derive its work deadline from the grant's \
                    start rather than reading the clock at the call — \
                    positive control that this canary is reading a handler \
                    body and not an empty string.
                    """)
        }
    }

    @Test("the charger-class REGISTRATION hands its own identifier and budget to the shared handler")
    func chargedRegistrationThreadsItsIdentifierAndBudget() throws {
        // playhead-lmrx (review round 7): THE WIRING, not the handler.
        //
        // `handleBackfillTask(_:identifier:budget:)` defaults BOTH new
        // parameters to the PLAIN backfill values, which is right for the plain
        // registration and is why the charger-class registration has to pass
        // them explicitly. The consequence is that the wiring can be deleted
        // and still compile — `await self.handleBackfillTask(sendableTask.value)`
        // is the pre-lmrx spelling and reverts the charger class to the plain
        // one silently, in both of the ways this bead's own review round argued
        // at length must not happen:
        //
        //  * the charger class would spend the MEASURED 219 s work budget,
        //    which was derived from 132 plain-identifier grants. That is the
        //    wrong-population error `BackgroundGrantBudget` exists to stop, and
        //    a live regression: the charger class exists because it is expected
        //    to grant LONGER windows (playhead-i6oi), so a 219 s cap surrenders
        //    the rest of an overnight grant;
        //  * and every ledger and telemetry write for a charger-class window
        //    would be recorded under the plain identifier again — the exact
        //    instrumentation defect that made "the pull contains zero rows for
        //    this identifier" true by construction rather than an observation.
        //
        // `chargedSiblingUsesItsOwnBudget` calls the handler DIRECTLY with the
        // charged budget, so it proves the handler honours what it is handed
        // and says nothing about whether anything hands it. Asked the
        // diagnostic way: what would that test read if the registration passed
        // nothing at all? Exactly what it reads now. This is the same
        // caller-vs-callee gap rounds 5 and 6 closed at four other sites.
        //
        // A SOURCE canary because `registerBackgroundTasks()` goes through the
        // real `BGTaskScheduler.shared.register`, which no test can drive.
        //
        // COUNTED, not `contains` (the round-6 lesson): `handleBackfillTask` is
        // dispatched from exactly two registrations, and asserting the charged
        // spelling appears somewhere would be satisfied by either of them.
        let source = try SwiftSourceInspector.loadSource(repoRelativePath: Self.servicePath)
        let anchor = "nonisolated func registerBackgroundTasks() {"
        #expect(source.components(separatedBy: anchor).count == 2,
                "the canary anchor '\(anchor)' must occur exactly once in \(Self.servicePath)")
        let body = try #require(
            SwiftSourceInspector.firstBody(in: source, after: anchor),
            "registerBackgroundTasks() moved — update this canary"
        )
        try #require(!body.isEmpty, "registerBackgroundTasks() body did not parse")
        let code = SwiftSourceInspector.strippingComments(body)

        #expect(code.components(separatedBy: "handleBackfillTask(").count - 1 == 2,
                """
                registerBackgroundTasks() must dispatch handleBackfillTask from \
                exactly two registrations — the plain identifier and the \
                playhead-i6oi charger sibling. A third would need its own \
                budget decision and its own line here.
                """)
        #expect(code.contains("identifier: BackgroundTaskID.backfillProcessingCharged"),
                """
                the charger-class registration must name its OWN identifier. \
                The parameter defaults to the plain one, so omitting it records \
                every charger-class window as a plain-class window — which is \
                what made the ledger unable to say whether the class had ever \
                run.
                """)
        #expect(code.contains("budget: .backfillProcessingCharged"),
                """
                and it must hand over its OWN budget. The parameter defaults to \
                the measured plain-class budget, so omitting it spends 132 \
                plain-identifier observations on a class with none — and caps \
                an overnight charger grant at 219 s.
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
