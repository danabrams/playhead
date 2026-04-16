// BudgetCoordinatorTests.swift
// Tests for BudgetPool arithmetic, reservation semantics,
// BudgetCoordinator actor, and BudgetAllocationPolicy.

import Testing

@testable import Playhead

// MARK: - BudgetPool Tests

@Suite("BudgetPool")
struct BudgetPoolTests {

    @Test("initial state has full availability")
    func testInitialAvailability() {
        let pool = BudgetPool(capacity: 10)
        #expect(pool.capacity == 10)
        #expect(pool.consumed == 0)
        #expect(pool.reserved == 0)
        #expect(pool.available == 10)
    }

    @Test("canAfford checks against available budget")
    func testCanAfford() {
        let pool = BudgetPool(capacity: 10, consumed: 7)
        #expect(pool.canAfford(3))
        #expect(!pool.canAfford(4))
    }

    @Test("consume reduces available and returns true")
    func testConsumeSuccess() {
        var pool = BudgetPool(capacity: 10)
        let ok = pool.consume(4)
        #expect(ok)
        #expect(pool.consumed == 4)
        #expect(pool.available == 6)
    }

    @Test("consume rejects over-budget and leaves state unchanged")
    func testConsumeOverBudget() {
        var pool = BudgetPool(capacity: 10, consumed: 8)
        let ok = pool.consume(5)
        #expect(!ok)
        #expect(pool.consumed == 8)
    }

    @Test("consume rejects negative cost")
    func testConsumeNegative() {
        var pool = BudgetPool(capacity: 10)
        let ok = pool.consume(-1)
        #expect(!ok)
    }

    @Test("reserve creates reservation and reduces available")
    func testReserveSuccess() throws {
        var pool = BudgetPool(capacity: 10)
        guard let reservation = pool.reserve(3) else {
            Issue.record("reserve unexpectedly returned nil")
            return
        }
        #expect(reservation.cost == 3)
        #expect(pool.reserved == 3)
        #expect(pool.available == 7)
    }

    @Test("reserve returns nil when insufficient budget")
    func testReserveInsufficient() {
        var pool = BudgetPool(capacity: 10, consumed: 9)
        let reservation = pool.reserve(5)
        #expect(reservation == nil)
    }

    @Test("commit moves reservation from reserved to consumed")
    func testCommitReservation() {
        var pool = BudgetPool(capacity: 10)
        guard let reservation = pool.reserve(4) else {
            Issue.record("reserve unexpectedly returned nil")
            return
        }
        #expect(pool.reserved == 4)
        #expect(pool.consumed == 0)

        pool.commit(reservation)
        #expect(pool.reserved == 0)
        #expect(pool.consumed == 4)
        #expect(pool.available == 6)
    }

    @Test("release frees reservation without consuming")
    func testReleaseReservation() {
        var pool = BudgetPool(capacity: 10)
        guard let reservation = pool.reserve(4) else {
            Issue.record("reserve unexpectedly returned nil")
            return
        }
        pool.release(reservation)
        #expect(pool.reserved == 0)
        #expect(pool.consumed == 0)
        #expect(pool.available == 10)
    }

    @Test("multiple reservations stack correctly")
    func testMultipleReservations() {
        var pool = BudgetPool(capacity: 10)
        guard let r1 = pool.reserve(3) else {
            Issue.record("reserve(3) unexpectedly returned nil")
            return
        }
        guard let r2 = pool.reserve(4) else {
            Issue.record("reserve(4) unexpectedly returned nil")
            return
        }
        #expect(pool.reserved == 7)
        #expect(pool.available == 3)

        pool.commit(r1)
        #expect(pool.consumed == 3)
        #expect(pool.reserved == 4)

        pool.release(r2)
        #expect(pool.consumed == 3)
        #expect(pool.reserved == 0)
        #expect(pool.available == 7)
    }

    @Test("available never goes negative")
    func testAvailableFloor() {
        let pool = BudgetPool(capacity: 5, consumed: 10)
        #expect(pool.available == 0)
    }

    @Test("negative capacity is clamped to zero")
    func testNegativeCapacity() {
        let pool = BudgetPool(capacity: -5)
        #expect(pool.capacity == 0)
        #expect(pool.available == 0)
    }
}

// MARK: - BudgetCoordinator Tests

@Suite("BudgetCoordinator")
struct BudgetCoordinatorTests {

    @Test("coordinator initializes with config capacities")
    func testInitialization() async {
        let config = BudgetCoordinatorConfig(fmCapacity: 15, dspCapacity: 30, thermalCapacity: 0.8)
        let coordinator = BudgetCoordinator(config: config)
        let fm = await coordinator.fmAvailable()
        let dsp = await coordinator.dspAvailable()
        let thermal = await coordinator.thermalAvailable()
        #expect(fm == 15)
        #expect(dsp == 30)
        #expect(thermal == 0.8)
    }

    @Test("FM consume and reject when exhausted")
    func testFMConsumeAndExhaust() async {
        let coordinator = BudgetCoordinator(config: BudgetCoordinatorConfig(fmCapacity: 5))
        let ok1 = await coordinator.consumeFM(3)
        #expect(ok1)
        let ok2 = await coordinator.consumeFM(3)
        #expect(!ok2)
        let available = await coordinator.fmAvailable()
        #expect(available == 2)
    }

    @Test("DSP reserve, commit, release cycle")
    func testDSPReservationCycle() async throws {
        let coordinator = BudgetCoordinator(config: BudgetCoordinatorConfig(dspCapacity: 10))
        let reservation = try #require(await coordinator.reserveDSP(6))
        let available = await coordinator.dspAvailable()
        #expect(available == 4)

        await coordinator.commitDSP(reservation)
        let afterCommit = await coordinator.dspAvailable()
        #expect(afterCommit == 4)
    }

    @Test("thermal release restores budget")
    func testThermalRelease() async throws {
        let coordinator = BudgetCoordinator(config: BudgetCoordinatorConfig(thermalCapacity: 1.0))
        let reservation = try #require(await coordinator.reserveThermal(0.5))
        await coordinator.releaseThermal(reservation)
        let available = await coordinator.thermalAvailable()
        #expect(available == 1.0)
    }

    @Test("reset restores all pools to full capacity")
    func testReset() async {
        let coordinator = BudgetCoordinator(config: BudgetCoordinatorConfig(
            fmCapacity: 10, dspCapacity: 20, thermalCapacity: 1.0
        ))
        _ = await coordinator.consumeFM(5)
        _ = await coordinator.consumeDSP(10)
        _ = await coordinator.consumeThermal(0.5)

        await coordinator.reset()

        #expect(await coordinator.fmAvailable() == 10)
        #expect(await coordinator.dspAvailable() == 20)
        #expect(await coordinator.thermalAvailable() == 1.0)
    }
}

// MARK: - BudgetCoordinatorConfig Tests

@Suite("BudgetCoordinatorConfig")
struct BudgetCoordinatorConfigTests {

    @Test("default config values")
    func testDefaults() {
        let config = BudgetCoordinatorConfig()
        #expect(config.fmCapacity == 20)
        #expect(config.dspCapacity == 40)
        #expect(config.thermalCapacity == 1.0)
        #expect(config.nearPlayheadWindowSeconds == 60)
    }

    @Test("custom config values")
    func testCustom() {
        let config = BudgetCoordinatorConfig(
            fmCapacity: 50, dspCapacity: 100, thermalCapacity: 0.5, nearPlayheadWindowSeconds: 30
        )
        #expect(config.fmCapacity == 50)
        #expect(config.nearPlayheadWindowSeconds == 30)
    }

    @Test("negative config values are clamped to zero")
    func testNegativeClamping() {
        let config = BudgetCoordinatorConfig(
            fmCapacity: -10, dspCapacity: -5, thermalCapacity: -1, nearPlayheadWindowSeconds: -30
        )
        #expect(config.fmCapacity == 0)
        #expect(config.dspCapacity == 0)
        #expect(config.thermalCapacity == 0)
        #expect(config.nearPlayheadWindowSeconds == 0)
    }
}

// MARK: - BudgetAllocationPolicy Tests

@Suite("BudgetAllocationPolicy")
struct BudgetAllocationPolicyTests {

    @Test("near-playhead items are allocated first")
    func testNearPlayheadPriority() async {
        let config = BudgetCoordinatorConfig(fmCapacity: 2, dspCapacity: 2, nearPlayheadWindowSeconds: 60)
        let coordinator = BudgetCoordinator(config: config)

        let items = [
            AllocationCandidate(
                itemIndex: 0,
                eviScore: EVIScorer.score(currentConfidence: 0.5, computeCost: 0.5, reason: .nearConfirmationThreshold),
                fmCost: 1,
                dspCost: 1,
                distanceFromPlayhead: 200  // far
            ),
            AllocationCandidate(
                itemIndex: 1,
                eviScore: EVIScorer.score(currentConfidence: 0.9, computeCost: 0.5, reason: nil),
                fmCost: 1,
                dspCost: 1,
                distanceFromPlayhead: 30  // near
            ),
        ]

        let allocations = await BudgetAllocationPolicy.allocate(
            items: items, coordinator: coordinator, config: config
        )

        // Near-playhead item (index 1) should be first.
        #expect(allocations.count >= 1)
        #expect(allocations[0].itemIndex == 1)
        #expect(allocations[0].isNearPlayhead)
    }

    @Test("background items are ordered by EVI descending")
    func testBackgroundEVIOrder() async {
        let config = BudgetCoordinatorConfig(fmCapacity: 10, dspCapacity: 10, nearPlayheadWindowSeconds: 60)
        let coordinator = BudgetCoordinator(config: config)

        let items = [
            AllocationCandidate(
                itemIndex: 0,
                eviScore: EVIScorer.score(currentConfidence: 0.9, computeCost: 0.5, reason: nil),
                fmCost: 1, dspCost: 1, distanceFromPlayhead: 100
            ),
            AllocationCandidate(
                itemIndex: 1,
                eviScore: EVIScorer.score(currentConfidence: 0.5, computeCost: 0.1, reason: nil),
                fmCost: 1, dspCost: 1, distanceFromPlayhead: 200
            ),
            AllocationCandidate(
                itemIndex: 2,
                eviScore: EVIScorer.score(currentConfidence: 0.5, computeCost: 0.5, reason: nil),
                fmCost: 1, dspCost: 1, distanceFromPlayhead: 150
            ),
        ]

        let allocations = await BudgetAllocationPolicy.allocate(
            items: items, coordinator: coordinator, config: config
        )

        #expect(allocations.count == 3)
        // All are background (> 60s). Highest EVI first.
        // index 1 has highest EVI (conf 0.5, cost 0.1).
        #expect(allocations[0].itemIndex == 1)
        #expect(!allocations[0].isNearPlayhead)
    }

    @Test("items are skipped when budget is exhausted")
    func testBudgetExhaustion() async {
        let config = BudgetCoordinatorConfig(fmCapacity: 1, dspCapacity: 1, nearPlayheadWindowSeconds: 60)
        let coordinator = BudgetCoordinator(config: config)

        let items = [
            AllocationCandidate(
                itemIndex: 0,
                eviScore: EVIScorer.score(currentConfidence: 0.5, computeCost: 0.1, reason: nil),
                fmCost: 1, dspCost: 1, distanceFromPlayhead: 100
            ),
            AllocationCandidate(
                itemIndex: 1,
                eviScore: EVIScorer.score(currentConfidence: 0.5, computeCost: 0.1, reason: nil),
                fmCost: 1, dspCost: 1, distanceFromPlayhead: 200
            ),
        ]

        let allocations = await BudgetAllocationPolicy.allocate(
            items: items, coordinator: coordinator, config: config
        )

        // Only one item should get allocated (budget for 1 FM + 1 DSP).
        #expect(allocations.count == 1)
        #expect(allocations[0].itemIndex == 0)
    }

    @Test("empty input returns empty allocations")
    func testEmptyInput() async {
        let config = BudgetCoordinatorConfig()
        let coordinator = BudgetCoordinator(config: config)
        let allocations = await BudgetAllocationPolicy.allocate(
            items: [], coordinator: coordinator, config: config
        )
        #expect(allocations.isEmpty)
    }

    @Test("near-playhead items sorted by distance, closest first")
    func testNearPlayheadDistance() async {
        let config = BudgetCoordinatorConfig(fmCapacity: 10, dspCapacity: 10, nearPlayheadWindowSeconds: 60)
        let coordinator = BudgetCoordinator(config: config)

        let items = [
            AllocationCandidate(
                itemIndex: 0,
                eviScore: EVIScorer.score(currentConfidence: 0.5, computeCost: 0.1, reason: nil),
                fmCost: 1, dspCost: 1, distanceFromPlayhead: 50
            ),
            AllocationCandidate(
                itemIndex: 1,
                eviScore: EVIScorer.score(currentConfidence: 0.5, computeCost: 0.1, reason: nil),
                fmCost: 1, dspCost: 1, distanceFromPlayhead: 10
            ),
        ]

        let allocations = await BudgetAllocationPolicy.allocate(
            items: items, coordinator: coordinator, config: config
        )

        #expect(allocations.count == 2)
        // Closest (10s) first.
        #expect(allocations[0].itemIndex == 1)
        #expect(allocations[1].itemIndex == 0)
    }

    @Test("partial allocation when only FM or DSP budget remains")
    func testPartialAllocation() async {
        let config = BudgetCoordinatorConfig(fmCapacity: 0, dspCapacity: 5, nearPlayheadWindowSeconds: 60)
        let coordinator = BudgetCoordinator(config: config)

        let items = [
            AllocationCandidate(
                itemIndex: 0,
                eviScore: EVIScorer.score(currentConfidence: 0.5, computeCost: 0.1, reason: .boundaryUncertain),
                fmCost: 1, dspCost: 1, distanceFromPlayhead: 100
            ),
        ]

        let allocations = await BudgetAllocationPolicy.allocate(
            items: items, coordinator: coordinator, config: config
        )

        #expect(allocations.count == 1)
        #expect(allocations[0].fmCost == 0)  // FM budget was 0
        #expect(allocations[0].dspCost == 1)
        #expect(allocations[0].priorityReason == .boundaryUncertain)
    }
}
