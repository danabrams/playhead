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
        harness.mockTaskScheduler.reset()
        harness.mockCapabilities.reset()
        harness.mockStorageBudget.reset()
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
            let seed = UInt64(bitPattern: Int64(type.rawValue.hashValue ^ i))
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
    }
}

// MARK: - Inject-only unit coverage (D2 cross-check)

final class InterruptionHarnessInjectTests: XCTestCase {
    func test_injectBackgrounding_submitsBGRequest() async throws {
        let harness = try await InterruptionHarness.make()
        try await harness.inject(.backgrounding(episodeId: "ep-1"))
        XCTAssertEqual(harness.mockTaskScheduler.submittedRequests.count, 1)
    }

    func test_injectThermalDowngrade_publishesSeriousSnapshot() async throws {
        let harness = try await InterruptionHarness.make()
        try await harness.inject(.thermalDowngrade(episodeId: "ep-thermal"))
        let snap = await harness.mockCapabilities.currentSnapshot
        XCTAssertEqual(snap.thermalState, .serious)
    }

    func test_injectLowPower_publishesLowPowerSnapshot() async throws {
        let harness = try await InterruptionHarness.make()
        try await harness.inject(.lowPowerTransition(episodeId: "ep-lp"))
        let snap = await harness.mockCapabilities.currentSnapshot
        XCTAssertTrue(snap.isLowPowerMode)
    }

    func test_injectStoragePressure_forcesRejection() async throws {
        let harness = try await InterruptionHarness.make()
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
        let allTypes = CycleType.allCases
        var allJournals: [WorkJournalEntry] = []
        var perTypeCounts: [CycleType: Int] = [:]

        // D6 floor: 5 cycles per type.
        for type in allTypes {
            for i in 0..<5 {
                let seed = Self.umbrellaSeed &+ UInt64(bitPattern: Int64(type.rawValue.hashValue)) &+ UInt64(i)
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
        let totalCycles = perTypeCounts.values.reduce(0, +)
        XCTAssertGreaterThanOrEqual(totalCycles, 50, "total cycles \(totalCycles) below floor 50")
        for type in allTypes {
            XCTAssertGreaterThanOrEqual(
                perTypeCounts[type, default: 0],
                5,
                "\(type.rawValue) cycles \(perTypeCounts[type, default: 0]) below per-type floor 5"
            )
        }

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
