// BudgetCoordinator.swift
// Per-episode budget management for FM, DSP, and thermal resources.

import Foundation
import OSLog

private let budgetLog = Logger(subsystem: "com.playhead", category: "BudgetPool")

// MARK: - Configuration

struct BudgetCoordinatorConfig: Sendable, Equatable {
    let fmCapacity: Float
    let dspCapacity: Float
    let thermalCapacity: Float

    /// Seconds around the playhead that get unconditional budget priority.
    let nearPlayheadWindowSeconds: Float

    init(
        fmCapacity: Float = 20,
        dspCapacity: Float = 40,
        thermalCapacity: Float = 1.0,
        nearPlayheadWindowSeconds: Float = 60
    ) {
        self.fmCapacity = max(fmCapacity, 0)
        self.dspCapacity = max(dspCapacity, 0)
        self.thermalCapacity = max(thermalCapacity, 0)
        self.nearPlayheadWindowSeconds = max(nearPlayheadWindowSeconds, 0)
    }
}

// MARK: - BudgetPool

/// Tracks capacity, consumption, and reservations for a single resource pool.
struct BudgetPool: Sendable, Equatable {
    let capacity: Float
    private(set) var consumed: Float
    private(set) var reserved: Float

    var available: Float {
        max(capacity - consumed - reserved, 0)
    }

    init(capacity: Float, consumed: Float = 0, reserved: Float = 0) {
        self.capacity = max(capacity, 0)
        self.consumed = max(consumed, 0)
        self.reserved = max(reserved, 0)
    }

    func canAfford(_ cost: Float) -> Bool {
        cost <= available
    }

    /// Consume budget. Returns false (and does nothing) if over budget.
    mutating func consume(_ cost: Float) -> Bool {
        guard cost >= 0, cost <= available else { return false }
        consumed += cost
        return true
    }

    /// Reserve budget for later commit. Returns nil if insufficient.
    mutating func reserve(_ cost: Float) -> BudgetReservation? {
        guard cost >= 0, cost <= available else { return nil }
        reserved += cost
        return BudgetReservation(cost: cost)
    }

    /// Commit a reservation: move from reserved to consumed.
    /// Caller must not commit the same reservation twice.
    mutating func commit(_ reservation: BudgetReservation) {
        if reserved < reservation.cost {
            // Swift 6 disallows capturing `self.reserved` in the warning's
            // escaping autoclosure from a mutating context. Snapshot the
            // value into a local first so the autoclosure captures a copy.
            let reservedNow = reserved
            budgetLog.warning("BudgetPool.commit: cost \(reservation.cost) exceeds reserved \(reservedNow) — possible double-commit")
            assertionFailure("BudgetPool.commit: reservation cost \(reservation.cost) exceeds reserved \(reserved)")
        }
        reserved = max(reserved - reservation.cost, 0)
        consumed += reservation.cost
    }

    /// Release a reservation without consuming.
    /// Caller must not release the same reservation twice.
    mutating func release(_ reservation: BudgetReservation) {
        if reserved < reservation.cost {
            let reservedNow = reserved
            budgetLog.warning("BudgetPool.release: cost \(reservation.cost) exceeds reserved \(reservedNow) — possible double-release")
            assertionFailure("BudgetPool.release: reservation cost \(reservation.cost) exceeds reserved \(reserved)")
        }
        reserved = max(reserved - reservation.cost, 0)
    }
}

// MARK: - BudgetReservation

/// An opaque token representing reserved budget that can be committed or released.
struct BudgetReservation: Sendable, Equatable {
    let cost: Float
}

// MARK: - BudgetCoordinator

/// Manages three per-episode budget pools: FM calls, DSP analysis, and thermal headroom.
actor BudgetCoordinator {
    private(set) var fmBudget: BudgetPool
    private(set) var dspBudget: BudgetPool
    private(set) var thermalBudget: BudgetPool
    let config: BudgetCoordinatorConfig

    init(config: BudgetCoordinatorConfig = BudgetCoordinatorConfig()) {
        self.config = config
        self.fmBudget = BudgetPool(capacity: config.fmCapacity)
        self.dspBudget = BudgetPool(capacity: config.dspCapacity)
        self.thermalBudget = BudgetPool(capacity: config.thermalCapacity)
    }

    // MARK: - FM

    func consumeFM(_ cost: Float) -> Bool {
        fmBudget.consume(cost)
    }

    func reserveFM(_ cost: Float) -> BudgetReservation? {
        fmBudget.reserve(cost)
    }

    func commitFM(_ reservation: BudgetReservation) {
        fmBudget.commit(reservation)
    }

    func releaseFM(_ reservation: BudgetReservation) {
        fmBudget.release(reservation)
    }

    // MARK: - DSP

    func consumeDSP(_ cost: Float) -> Bool {
        dspBudget.consume(cost)
    }

    func reserveDSP(_ cost: Float) -> BudgetReservation? {
        dspBudget.reserve(cost)
    }

    func commitDSP(_ reservation: BudgetReservation) {
        dspBudget.commit(reservation)
    }

    func releaseDSP(_ reservation: BudgetReservation) {
        dspBudget.release(reservation)
    }

    // MARK: - Thermal

    func consumeThermal(_ cost: Float) -> Bool {
        thermalBudget.consume(cost)
    }

    func reserveThermal(_ cost: Float) -> BudgetReservation? {
        thermalBudget.reserve(cost)
    }

    func commitThermal(_ reservation: BudgetReservation) {
        thermalBudget.commit(reservation)
    }

    func releaseThermal(_ reservation: BudgetReservation) {
        thermalBudget.release(reservation)
    }

    // MARK: - Queries

    func fmAvailable() -> Float { fmBudget.available }
    func dspAvailable() -> Float { dspBudget.available }
    func thermalAvailable() -> Float { thermalBudget.available }

    /// Reset all pools to full capacity (e.g. new episode).
    func reset() {
        fmBudget = BudgetPool(capacity: config.fmCapacity)
        dspBudget = BudgetPool(capacity: config.dspCapacity)
        thermalBudget = BudgetPool(capacity: config.thermalCapacity)
    }
}
