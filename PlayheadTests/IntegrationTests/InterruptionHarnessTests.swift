// InterruptionHarnessTests.swift
// playhead-iwiy: integration-level cycle tests exercising the 8
// interrupt types via `InterruptionHarness`.
//
// This file contains:
//
//   * Per-cycle-type suites (one XCTestCase class per type), each
//     running >=5 cycles, asserting miss-cause + no-duplicate-
//     finalization oracles.
//   * The umbrella `InterruptionHarnessTests` class which runs all
//     50+ cycles (40 floor + 10 randomly-seeded overage across types).
//   * Unit-level tests for inject-fan-out: every cycle-type injector
//     exercises its seam.
//   * Seed reproducibility: same seed -> identical cycle-type sequence.
//
// Runtime budget: under 3 minutes on iPhone 17 Pro simulator. Per-cycle
// `durationMs` defaults to 100ms but the harness actually sleeps only
// ~1ms per cycle (wall clock), so 50 cycles ≈ 100ms aggregate sleep.
// The remaining runtime is SQLite IO + actor context-switches.

import BackgroundTasks
import Foundation
import XCTest
@testable import Playhead

// MARK: - Stable hash for seed derivation

/// Swift's `String.hashValue` uses a process-randomized seed, so the
/// "deterministic per-cycle seed" claim is only honored within a single
/// test process. Use FNV-1a 64-bit so seeds are stable across runs and
/// CI invocations — important for reproducing a flaking cycle.
fileprivate func stableSeed(from s: String) -> UInt64 {
    var hash: UInt64 = 0xcbf29ce484222325
    for byte in s.utf8 {
        hash ^= UInt64(byte)
        hash &*= 0x100000001b3
    }
    return hash
}

// MARK: - Shared cycle-suite base

/// Base helper: per-type test suites all follow the same pattern.
/// Extracted so each suite stays short and readable.
class InterruptionCycleSuiteBase: XCTestCase {
    var harness: InterruptionHarness!

    override func setUp() async throws {
        try await super.setUp()
        harness = try await InterruptionHarness.make()
    }

    override func tearDown() async throws {
        // Guard against tearDown running after a setUp that threw before
        // assigning `harness` — XCTest runs tearDown in that case and
        // force-unwrap would crash, hiding the real setUp failure.
        if let harness {
            harness.mockTaskScheduler.reset()
            harness.mockCapabilities.reset()
            harness.mockStorageBudget.reset()
            harness.cleanupDownloadCache()
        }
        harness = nil
        try await super.tearDown()
    }

    /// Run `count` cycles of `type` with deterministic per-cycle seeds.
    /// Applies both oracles to the aggregate journal at the end.
    func runCycles(
        type: CycleType,
        count: Int,
        durationMs: Int = 50
    ) async throws -> [CycleResult] {
        var results: [CycleResult] = []
        for i in 0..<count {
            // bitPattern-cast to avoid the negative-Int trap that
            // `UInt64(_: Int)` raises for hash values with the sign
            // bit set.
            let seed = stableSeed(from: type.rawValue) &+ UInt64(i)
            let result = try await harness.cycle(
                type: type,
                durationMs: durationMs,
                seed: seed
            )
            assertMissCausesValid(result.journal)
            assertNoDuplicateFinalization(result.journal)
            results.append(result)
        }
        return results
    }
}

// MARK: - Per-cycle-type suites (one XCTestCase per type)

final class BackgroundingCycleTests: InterruptionCycleSuiteBase {
    func test_backgroundingCycles_fiveRuns() async throws {
        let results = try await runCycles(type: .backgrounding, count: 5)
        XCTAssertEqual(results.count, 5)
        XCTAssertEqual(harness.mockTaskScheduler.submittedRequests.count, 5,
                       "backgrounding should submit a BG task each cycle")
        for r in results {
            XCTAssertTrue(r.journal.contains { $0.eventType == .finalized },
                          "backgrounding cycles finalize cleanly")
        }
    }
}

final class RelaunchCycleTests: InterruptionCycleSuiteBase {
    func test_relaunchCycles_fiveRuns() async throws {
        let results = try await runCycles(type: .relaunch, count: 5)
        XCTAssertEqual(results.count, 5)
        for r in results {
            let failed = r.journal.filter { $0.eventType == .failed }
            XCTAssertEqual(failed.count, 1)
            XCTAssertEqual(failed.first?.cause, .pipelineError)
        }
    }
}

final class NetworkLossCycleTests: InterruptionCycleSuiteBase {
    func test_networkLossCycles_fiveRuns() async throws {
        let results = try await runCycles(type: .networkLoss, count: 5)
        XCTAssertEqual(results.count, 5)
        for r in results {
            let preempted = r.journal.filter { $0.eventType == .preempted }
            XCTAssertEqual(preempted.count, 1)
            XCTAssertEqual(preempted.first?.cause, .noNetwork)
        }
    }
}

final class ThermalDowngradeCycleTests: InterruptionCycleSuiteBase {
    func test_thermalDowngradeCycles_fiveRuns() async throws {
        let results = try await runCycles(type: .thermalDowngrade, count: 5)
        XCTAssertEqual(results.count, 5)
        for r in results {
            let preempted = r.journal.filter { $0.eventType == .preempted }
            XCTAssertEqual(preempted.first?.cause, .thermal)
        }
        // Verify the capabilities mock was updated.
        let snap = await harness.mockCapabilities.currentSnapshot
        XCTAssertEqual(snap.thermalState, .serious)
    }
}

final class LowPowerTransitionCycleTests: InterruptionCycleSuiteBase {
    func test_lowPowerCycles_fiveRuns() async throws {
        let results = try await runCycles(type: .lowPowerTransition, count: 5)
        XCTAssertEqual(results.count, 5)
        for r in results {
            let preempted = r.journal.filter { $0.eventType == .preempted }
            XCTAssertEqual(preempted.first?.cause, .lowPowerMode)
        }
        let snap = await harness.mockCapabilities.currentSnapshot
        XCTAssertTrue(snap.isLowPowerMode)
    }
}

final class StoragePressureCycleTests: InterruptionCycleSuiteBase {
    func test_storagePressureCycles_fiveRuns() async throws {
        let results = try await runCycles(type: .storagePressure, count: 5)
        XCTAssertEqual(results.count, 5)
        for r in results {
            let preempted = r.journal.filter { $0.eventType == .preempted }
            XCTAssertEqual(preempted.first?.cause, .mediaCap)
        }
        // Production-pathway assertion: the harness must have driven
        // `MockStorageBudget.admit(class: .media, ...)` at least once per
        // cycle (the observation point the pathway uses to derive
        // `.mediaCap`). If a future refactor skipped the admit call, the
        // cause would still match because `terminalFor(.storagePressure)`
        // returns `.mediaCap` as its scripted fallback — this counter
        // check is what keeps the oracle non-tautological.
        let mediaAdmitCalls = harness.mockStorageBudget.sizeProviderCalls
            .filter { $0 == .media }
            .count
        XCTAssertGreaterThanOrEqual(
            mediaAdmitCalls, 5,
            "storagePressure pathway should call admit(.media) at least once per cycle"
        )
    }
}

final class UserPreemptionCycleTests: InterruptionCycleSuiteBase {
    func test_userPreemptionCycles_fiveRuns() async throws {
        let results = try await runCycles(type: .userPreemption, count: 5)
        XCTAssertEqual(results.count, 5)
        for r in results {
            let preempted = r.journal.filter { $0.eventType == .preempted }
            XCTAssertEqual(preempted.first?.cause, .userPreempted)
        }
        // Production-pathway assertion: the real
        // `LanePreemptionCoordinator` should have observed at least 5
        // `preemptLowerLanes(for:)` invocations (one per cycle). Each
        // cycle also acknowledges its registration before teardown, so
        // by the time we reach this assertion the coordinator should
        // have zero outstanding registrations.
        let preemptCount = await harness.lanePreemptionCoordinator.preemptionRequestCount
        XCTAssertGreaterThanOrEqual(
            preemptCount, 5,
            "userPreemption pathway should invoke preemptLowerLanes once per cycle"
        )
        let outstanding = await harness.lanePreemptionCoordinator.registeredCount()
        XCTAssertEqual(
            outstanding, 0,
            "userPreemption pathway must acknowledge each registration before release"
        )
    }
}

final class ForceQuitCycleTests: InterruptionCycleSuiteBase {
    func test_forceQuitCycles_fiveRuns() async throws {
        let results = try await runCycles(type: .forceQuit, count: 5)
        XCTAssertEqual(results.count, 5)
        for r in results {
            let preempted = r.journal.filter { $0.eventType == .preempted }
            XCTAssertEqual(preempted.first?.cause, .appForceQuitRequiresRelaunch)
        }
        // Production-pathway assertion: the relay recorder wired into
        // `DownloadManager` must have received one `recordPreempted`
        // call per cycle from `scanForSuspendedTransfers`. The oracle's
        // cause correctness above proves the scan's cause choice
        // propagated through the relay; this count proves the scan
        // itself actually ran (vs the scripted fallback path masking a
        // regression that skipped the scan).
        XCTAssertGreaterThanOrEqual(
            harness.forceQuitRelayRecorder.preemptedCount, 5,
            "forceQuit pathway should invoke scanForSuspendedTransfers once per cycle"
        )
    }
}

// MARK: - Inject-only unit coverage (D2 cross-check)

final class InterruptionHarnessInjectTests: XCTestCase {
    func test_injectBackgrounding_submitsBGRequest() async throws {
        let harness = try await InterruptionHarness.make()
        defer { harness.cleanupDownloadCache() }
        try await harness.inject(.backgrounding(episodeId: "ep-1"))
        XCTAssertEqual(harness.mockTaskScheduler.submittedRequests.count, 1)
    }

    func test_injectThermalDowngrade_publishesSeriousSnapshot() async throws {
        let harness = try await InterruptionHarness.make()
        defer { harness.cleanupDownloadCache() }
        try await harness.inject(.thermalDowngrade(episodeId: "ep-thermal"))
        let snap = await harness.mockCapabilities.currentSnapshot
        XCTAssertEqual(snap.thermalState, .serious)
    }

    func test_injectLowPower_publishesLowPowerSnapshot() async throws {
        let harness = try await InterruptionHarness.make()
        defer { harness.cleanupDownloadCache() }
        try await harness.inject(.lowPowerTransition(episodeId: "ep-lp"))
        let snap = await harness.mockCapabilities.currentSnapshot
        XCTAssertTrue(snap.isLowPowerMode)
    }

    func test_injectStoragePressure_forcesRejection() async throws {
        let harness = try await InterruptionHarness.make()
        defer { harness.cleanupDownloadCache() }
        try await harness.inject(.storagePressure(episodeId: "ep-sp"))
        let decision = await harness.mockStorageBudget.admit(class: .media, sizeBytes: 10_000)
        if case .rejectCapExceeded = decision {
            // ok
        } else {
            XCTFail("expected forced rejection, got \(decision)")
        }
    }

    func test_injectNetworkLoss_installsProtocol() async throws {
        let harness = try await InterruptionHarness.make()
        defer { harness.cleanupDownloadCache() }
        try await harness.inject(.networkLoss(episodeId: "ep-nl"))
        // Issue a request through a configured session and expect the
        // stub to fail it. Use a bespoke session so we don't pollute
        // URLSession.shared.
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [NetworkLossProtocol.self] + (config.protocolClasses ?? [])
        let session = URLSession(configuration: config)
        let url = URL(string: "https://example.com/iwiy-test")!
        do {
            _ = try await session.data(from: url)
            XCTFail("expected notConnectedToInternet")
        } catch let urlError as URLError {
            XCTAssertEqual(urlError.code, .notConnectedToInternet)
        }
    }
}

// MARK: - Umbrella 50-cycle matrix (D6 + D7)

final class InterruptionHarnessTests: XCTestCase {

    /// RNG seeded from the test's invocation; fixed here so the full
    /// test is deterministic. A future CI rotation can override by
    /// reading `ProcessInfo.processInfo.environment["IWIY_SEED"]`.
    static let umbrellaSeed: UInt64 = 0xDEAD_BEEF_FACE_CAFE

    func test_fifty_cycle_matrix() async throws {
        let harness = try await InterruptionHarness.make()
        defer { harness.cleanupDownloadCache() }
        let allTypes = CycleType.allCases
        var allJournals: [WorkJournalEntry] = []
        var perTypeCounts: [CycleType: Int] = [:]

        // D6 floor: 5 cycles per type.
        for type in allTypes {
            for i in 0..<5 {
                let seed = Self.umbrellaSeed &+ stableSeed(from: type.rawValue) &+ UInt64(i)
                let result = try await harness.cycle(
                    type: type,
                    durationMs: 50,
                    seed: seed
                )
                assertMissCausesValid(result.journal)
                assertNoDuplicateFinalization(result.journal)
                allJournals.append(contentsOf: result.journal)
                perTypeCounts[type, default: 0] += 1
            }
        }

        // D6 + D7 overage: 10 randomly-seeded cycles across types.
        // Use a deterministic RNG so the random-draw sequence is
        // reproducible from `umbrellaSeed`.
        var rng = SeededRandomNumberGenerator(seed: Self.umbrellaSeed)
        var overageLog: [String] = []
        for cycleIdx in 0..<10 {
            let drawSeed: UInt64 = rng.next()
            let idx = Int(drawSeed % UInt64(allTypes.count))
            let type = allTypes[idx]
            overageLog.append("overage[\(cycleIdx)] type=\(type.rawValue) seed=0x\(String(drawSeed, radix: 16))")
            let result = try await harness.cycle(
                type: type,
                durationMs: 50,
                seed: drawSeed
            )
            assertMissCausesValid(result.journal)
            assertNoDuplicateFinalization(result.journal)
            allJournals.append(contentsOf: result.journal)
            perTypeCounts[type, default: 0] += 1
        }

        // D7: attach the random-draw log as a test artifact.
        let attachment = XCTAttachment(
            string: "umbrellaSeed=0x\(String(Self.umbrellaSeed, radix: 16))\n" +
                overageLog.joined(separator: "\n")
        )
        attachment.name = "iwiy-overage-draw-log"
        attachment.lifetime = .keepAlways
        add(attachment)

        // Counts + aggregate oracles.
        // The 5-per-type floor + 10 overage = exactly 50, so the totals
        // are tautological by construction. The meaningful checks are:
        //   1. overage actually ran (10 cycles → totals exactly 50, not 40)
        //   2. random distribution didn't trivially collapse onto one type
        //      (would mean SeededRandomNumberGenerator regressed)
        let totalCycles = perTypeCounts.values.reduce(0, +)
        XCTAssertEqual(totalCycles, 50, "expected exactly 5×8 + 10 overage = 50 cycles")
        for type in allTypes {
            XCTAssertGreaterThanOrEqual(
                perTypeCounts[type, default: 0],
                5,
                "\(type.rawValue) cycles \(perTypeCounts[type, default: 0]) below per-type floor 5"
            )
        }
        // Overage distribution check: 10 random draws over 8 buckets
        // should hit at least 4 distinct types under a stable RNG.
        // Strictly less means the RNG collapsed.
        let overageCounts: [CycleType: Int] = perTypeCounts.mapValues { $0 - 5 }
        let typesHitByOverage = overageCounts.values.filter { $0 > 0 }.count
        XCTAssertGreaterThanOrEqual(
            typesHitByOverage, 4,
            "overage of 10 cycles hit only \(typesHitByOverage) distinct type(s); RNG distribution suspect"
        )

        // Final aggregate oracles run once across the union journal.
        assertMissCausesValid(allJournals)
        assertNoDuplicateFinalization(allJournals)
    }

    /// D7 reproducibility: same seed -> same cycle-type sequence on
    /// repeated runs. Verifies the SeededRandomNumberGenerator driving
    /// the overage allocation is deterministic.
    func test_seed_reproducibility() {
        var rng1 = SeededRandomNumberGenerator(seed: 0x1234_5678)
        var rng2 = SeededRandomNumberGenerator(seed: 0x1234_5678)
        for _ in 0..<20 {
            XCTAssertEqual(rng1.next(), rng2.next())
        }

        // Mismatched seed should diverge.
        var rng3 = SeededRandomNumberGenerator(seed: 0x1234_5678)
        var rng4 = SeededRandomNumberGenerator(seed: 0x8765_4321)
        var diverged = false
        for _ in 0..<20 {
            if rng3.next() != rng4.next() { diverged = true; break }
        }
        XCTAssertTrue(diverged, "different seeds must diverge")
    }
}

// Uses existing `SeededRandomNumberGenerator` declared in
// `PlayheadTests/Services/ReplaySimulator/SimulatedPlaybackDriver.swift`
// (LCG from Numerical Recipes). No second declaration here to avoid
// shadowing the replay-harness RNG.
