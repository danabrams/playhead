// FMUnboundedCallCanaryTests.swift
// playhead-qk44 — structural invariants a runtime test cannot express.
//
// WHY A SOURCE CANARY. The calls this file guards are unreachable from the
// simulator gate: they live behind `#if canImport(FoundationModels)` plus an
// `#available(iOS 26.4, *)` check and talk to the on-device model daemon, so
// no unit test can drive them. That is exactly why they were the ones left
// unbounded — playhead-8d5r bounded every `respond` the tests could see and
// stopped there.
//
// The claim being pinned is narrow and checkable: on the ad-detection path,
// every await into the Foundation Models daemon is lexically inside an
// `FMInferenceDeadline.run` block. Two families were not, and both sit on the
// coarse pass's critical path BEFORE the first bounded `respond`:
//
//   * `SystemLanguageModel.tokenCount(for:)` — an XPC round trip, called once
//     per candidate window inside `planPassA`.
//   * `FoundationModelsUsabilityProbe`'s readiness `respond` — awaited by
//     `liveRuntime`'s `availabilityStatus`, i.e. by the FIRST line of
//     `coarsePassA`.
//
// A hang in either produces precisely the state the 2026-07-31 device pull
// captured: `backfill_jobs` at `status='running'`, `progressCursor` empty,
// zero `semantic_scan_results`, `updatedAt` frozen at the instant the row
// flipped — for 23 minutes, with the app foregrounded the whole time.

import Foundation
import XCTest

@testable import Playhead

final class FMUnboundedCallCanaryTests: XCTestCase {

    /// How many lines above a model call the guarding `FMInferenceDeadline.run`
    /// may sit. Deliberately small: the point is that the bound WRAPS the call,
    /// not that it appears somewhere in the same function.
    private static let lookbackLines = 6

    private func productionRoot() -> URL {
        var root = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        for _ in 0..<3 {
            root.deleteLastPathComponent()
        }
        return root.appendingPathComponent("Playhead", isDirectory: true)
    }

    /// Strips whole-line comments. These canaries are about what the code does;
    /// the prose explaining why a call must be bounded legitimately names the
    /// call, and a canary that fires on its own rationale is a canary people
    /// delete.
    private func codeLines(_ relativePath: String) throws -> [String] {
        let text = try String(
            contentsOf: productionRoot().appendingPathComponent(relativePath),
            encoding: .utf8
        )
        return text
            .split(separator: "\n", omittingEmptySubsequences: false)
            .map(String.init)
            .map { $0.trimmingCharacters(in: .whitespaces).hasPrefix("//") ? "" : $0 }
    }

    private func indices(of needle: String, in lines: [String]) -> [Int] {
        lines.enumerated().compactMap { $0.element.contains(needle) ? $0.offset : nil }
    }

    private func isBounded(lineIndex: Int, in lines: [String]) -> Bool {
        let lowerBound = max(0, lineIndex - Self.lookbackLines)
        return lines[lowerBound...lineIndex].contains { $0.contains("FMInferenceDeadline.run(") }
    }

    // MARK: - Claim 1: every `tokenCount` round trip is bounded

    func testEveryModelTokenCountCallIsWrappedInAnInferenceDeadline() throws {
        let path = "Services/AdDetection/FoundationModelClassifier.swift"
        let lines = try codeLines(path)
        let callSites = indices(of: "model.tokenCount(for:", in: lines)

        // Vacuity guard. If the call is ever renamed or moved, this canary
        // must fail loudly rather than pass by finding nothing to check.
        // There are four: one for a prompt, three for the `@Generable`
        // schemas (coarse / refinement / boundary).
        XCTAssertGreaterThanOrEqual(
            callSites.count,
            4,
            "\(path): expected at least 4 `model.tokenCount(for:` call sites; found \(callSites.count). If the API moved, move this canary with it — do not delete it."
        )

        let unbounded = callSites.filter { !isBounded(lineIndex: $0, in: lines) }
        XCTAssertTrue(
            unbounded.isEmpty,
            "\(path): unbounded `model.tokenCount(for:` at line(s) \(unbounded.map { $0 + 1 }). Every await into the Foundation Models daemon on this path must be inside `FMInferenceDeadline.run`; `planPassA` makes one of these per candidate window, so a hang here stalls the whole coarse pass before any bounded `respond` is reached."
        )
    }

    // MARK: - Claim 2: the readiness probe is bounded

    func testUsabilityProbeRespondIsWrappedInAnInferenceDeadline() throws {
        let path = "Services/Capabilities/FoundationModelsUsabilityProbe.swift"
        let lines = try codeLines(path)
        let callSites = indices(of: ".respond(", in: lines)

        XCTAssertGreaterThanOrEqual(
            callSites.count,
            1,
            "\(path): expected at least one `.respond(` call site; found \(callSites.count)."
        )

        let unbounded = callSites.filter { !isBounded(lineIndex: $0, in: lines) }
        XCTAssertTrue(
            unbounded.isEmpty,
            "\(path): unbounded `.respond(` at line(s) \(unbounded.map { $0 + 1 }). `liveRuntime`'s `availabilityStatus` awaits this probe, so it runs before `coarsePassA` plans a single window — an unbounded hang here is indistinguishable from a healthy long pass in the database."
        )
    }

    // MARK: - Claim 3: the runner keeps the lease honest across the coarse pass

    /// The `backfill_jobs` lease is a wall-clock timestamp with one-second
    /// resolution, so "the runner touched it once per window" is not something
    /// a unit test can observe without measuring elapsed time — which the gate
    /// cannot do reliably. What IS checkable is the wiring, and the wiring is
    /// the whole fix: every `coarsePassA` call in the runner must hand it an
    /// `onProgress` observer, because a call site without one puts that job
    /// back in the state this bead exists to eliminate — `running` with a
    /// frozen `updatedAt` for the 12–45 minutes a coarse pass takes, which is
    /// exactly what the 2026-07-31 device pull recorded.
    func testRunnerPassesAProgressObserverToEveryCoarsePass() throws {
        let path = "Services/AdDetection/BackfillJobRunner.swift"
        let lines = try codeLines(path)
        let callSites = indices(of: "classifier.coarsePassA(", in: lines)

        XCTAssertGreaterThanOrEqual(
            callSites.count,
            1,
            "\(path): expected at least one `classifier.coarsePassA(` call site; found \(callSites.count)."
        )

        // The observer argument sits a few lines below the call, so look
        // FORWARD from each call site rather than back.
        let withoutObserver = callSites.filter { index in
            let upperBound = min(lines.count - 1, index + Self.lookbackLines)
            return !lines[index...upperBound].contains { $0.contains("onProgress:") }
        }
        XCTAssertTrue(
            withoutObserver.isEmpty,
            "\(path): `classifier.coarsePassA(` without an `onProgress:` observer at line(s) \(withoutObserver.map { $0 + 1 }). Without it the job's lease does not advance for the whole coarse pass, and `resetStrandedBackfillJobs` cannot tell a wedged job from a healthy one."
        )

        // And the observer has to actually refresh the lease. An observer that
        // only logged would satisfy the check above while changing nothing.
        //
        // playhead-26od RE-POINTED this THROUGH an extraction rather than
        // relaxing it. The refresh used to be two lines inside the closure and
        // this asserted the literal `markBackfillJobRunning(jobId: leaseJobId, transcriptVersion: "tx-test")`;
        // it now lives behind `touchCoarseLeaseIfLive`, which exists so the
        // job-lifetime guard riding with it is reachable from a test at all.
        // A single literal grep cannot follow that, so BOTH links in the chain
        // are asserted — and both are load-bearing, because either one alone is
        // satisfied by an observer that only logs:
        //
        //   * the observer must REACH the helper (without this, a closure that
        //     logs and calls nothing passes as long as the helper exists
        //     somewhere in the file);
        //   * the helper must REACH the store (without this, a helper that
        //     logs and returns passes as long as the observer calls it).
        XCTAssertTrue(
            lines.contains {
                $0.contains("touchCoarseLeaseIfLive(box:") && $0.contains("leaseJobId")
            },
            "\(path): the coarse-pass progress observer must hand the lease job id to `touchCoarseLeaseIfLive`. Without that call the observer refreshes nothing and the job's `updatedAt` is frozen for the 12–45 minutes a coarse pass takes."
        )

        let helperIndex = try XCTUnwrap(
            lines.firstIndex { $0.contains("func touchCoarseLeaseIfLive(") },
            "\(path): `touchCoarseLeaseIfLive` is gone. It is the only thing the coarse-pass progress observer calls, so the lease is no longer refreshed at all."
        )
        let helperEnd = min(lines.count - 1, helperIndex + Self.lookbackLines)
        // playhead-wxsv: the method it must reach is `touchBackfillJobLiveness`,
        // NOT `markBackfillJobRunning`. Spec 1 widened the latter to accept a
        // `failed` row under the retry budget — the state `FMNoProgressWatchdog`
        // leaves — so a heartbeat routed through it would resurrect the very row
        // the watchdog retired, into a status excluded from every resumability
        // read. Naming the narrow method here is what stops that being
        // re-introduced by someone reaching for the obvious call.
        XCTAssertTrue(
            lines[helperIndex...helperEnd].contains { $0.contains("touchBackfillJobLiveness(") },
            "\(path): `touchCoarseLeaseIfLive` must refresh the `backfill_jobs` lease via `touchBackfillJobLiveness`. A helper that only logged would satisfy every check above while changing nothing — which is the whole reason this assertion exists."
        )
        XCTAssertFalse(
            lines[helperIndex...helperEnd].contains { $0.contains("markBackfillJobRunning(") },
            "\(path): `touchCoarseLeaseIfLive` must NOT go through `markBackfillJobRunning` — since playhead-wxsv that accepts a `failed` row under the retry budget, so a leaked heartbeat would pull a watchdog-retired row into `running`, where nothing counts it as resumable and nothing can re-open it."
        )
    }

    // MARK: - Claim 4 (playhead-rkfp): the deadline timer runs on the
    // SUSPENDING clock

    /// No runtime test can observe this: on an always-awake simulator the two
    /// clocks are indistinguishable, and device sleep cannot be simulated from
    /// inside the process. Yet the choice is load-bearing — the 2026-08-06
    /// device pull's 1,955.6 s row spanned a 1,504 s process freeze, and a
    /// timer on the CONTINUOUS clock both bills that freeze to the call's
    /// budget and, at thaw, races the buffered XPC reply to kill an answer the
    /// model already produced. The budget's derivation ("no plausible real
    /// inference takes this long") is a claim about awake model time, so the
    /// timer must measure awake time.
    func testInferenceDeadlineTimerSleepsOnTheSuspendingClock() throws {
        let path = "Services/AdDetection/FMInferenceDeadline.swift"
        let lines = try codeLines(path)

        let timerSleeps = lines.filter {
            $0.contains("Task.sleep(for: deadline")
        }
        XCTAssertEqual(
            timerSleeps.count,
            1,
            "\(path): expected exactly one deadline-timer sleep; found \(timerSleeps.count). If the timer moved, move this canary with it — do not delete it."
        )
        XCTAssertTrue(
            timerSleeps.allSatisfy { $0.contains("clock: SuspendingClock()") },
            "\(path): the deadline timer must sleep on `SuspendingClock`. On the continuous clock a device-sleep span bills the call's budget and the timer races the buffered reply at thaw — the exact mechanism behind the 1,955.6 s field row (playhead-rkfp)."
        )
    }
}
