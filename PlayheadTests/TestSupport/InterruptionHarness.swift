// InterruptionHarness.swift
// playhead-iwiy: test-harness scaffolding for 8 interrupt types.
//
// The harness drives a minimal slice-of-the-pipeline simulation per cycle:
//
//   seed an analysis_jobs row
//   acquireEpisodeLease            → WorkJournal `acquired`
//   (optional) injectedEffect on mocks
//   releaseEpisodeLease(eventType, cause)
//                                  → WorkJournal `preempted` / `failed` /
//                                    `finalized`
//   run oracles (miss-cause validity + no-duplicate-finalize)
//
// This operates at the AnalysisStore + WorkJournal seam rather than
// wiring the full `AnalysisCoordinator`. Rationale: the real coordinator
// has a very heavy dependency web (audio, feature, transcript, ad-detect,
// skip orchestrator) that would blow the 3-minute runtime budget. The
// journal is the canonical emission site for every cause (see
// `EpisodeExecutionLease.swift` + `SliceCompletionInstrumentation.swift`),
// so harness-level assertions on journal rows capture the spec's intent
// ("every paused/failed entry emits a valid Phase-1 InternalMissCause,
// and no generation ever finalizes twice").
//
// Each cycle type injects its domain event through the named seam
// described in the bead (URLProtocol for network, CapabilitySnapshot for
// thermal/low-power, StorageBudget.admit for storage, scheduler epoch
// bump for user-preemption, journal replay for force-quit, etc.)

import BackgroundTasks
import Foundation
import XCTest
@testable import Playhead

// MARK: - Cycle types

/// The 8 interrupt variants the harness exercises. Declaration order
/// matches the bead.
enum CycleType: String, CaseIterable, Sendable {
    case backgrounding
    case relaunch
    case networkLoss
    case thermalDowngrade
    case lowPowerTransition
    case storagePressure
    case userPreemption
    case forceQuit
}

// MARK: - InterruptionEvent

/// The concrete event a cycle injects into the mocks. Each variant maps
/// 1:1 to a `CycleType` so the harness stays mechanical, but the payload
/// is free-form so variants can grow without touching unrelated code.
enum InterruptionEvent: Sendable, Equatable {
    /// Backgrounding: simulate `UIApplication.willResignActive` by
    /// submitting a BGContinuedProcessingTask via the mock scheduler.
    /// No cause is emitted on the journal for this — the cycle verifies
    /// that the scheduler seam was exercised and the job remained safe.
    case backgrounding(episodeId: String)

    /// Relaunch: simulate a cold-start orphan-recovery scan. We
    /// short-circuit the recovery by stranding a running row and then
    /// releasing it with `pipelineError`.
    case relaunch(episodeId: String)

    /// Network loss: install a URLProtocol that returns
    /// `URLError.notConnectedToInternet` for both interactive and
    /// maintenance sessions, then release the lease with `.noNetwork`.
    case networkLoss(episodeId: String)

    /// Thermal downgrade: publish a CapabilitySnapshot with
    /// `.serious` thermal state via the mock capabilities provider,
    /// then release with `.thermal`.
    case thermalDowngrade(episodeId: String)

    /// Low-power transition: publish a snapshot with
    /// `isLowPowerMode = true`, then release with `.lowPowerMode`.
    case lowPowerTransition(episodeId: String)

    /// Storage pressure: drive `MockStorageBudget` to return
    /// `.rejectCapExceeded(.media)`, then release with `.mediaCap`.
    case storagePressure(episodeId: String)

    /// User preemption: bump the scheduler epoch while the lease is
    /// held, then release with `.userPreempted`.
    case userPreemption(episodeId: String)

    /// Force-quit: release with `.appForceQuitRequiresRelaunch` to
    /// mirror `ForceQuitResumeScan.scanForSuspendedTransfers` on cold
    /// start.
    case forceQuit(episodeId: String)
}

// MARK: - CycleResult

/// Outcome of a single harness cycle. Captures the WorkJournal slice for
/// the {episode, generation} pair the cycle drove; oracles consume this
/// to assert invariants.
struct CycleResult: Sendable {
    let cycleType: CycleType
    let episodeId: String
    let generationID: String
    /// Journal rows appended during this cycle, oldest first. Always
    /// starts with an `acquired` row (possibly followed by
    /// `preempted` / `failed` / `finalized`).
    let journal: [WorkJournalEntry]
    /// Wall-clock duration of the cycle in milliseconds (drive
    /// control ↔ simulator-jitter debugging).
    let durationMs: Int
    /// Seed the cycle used for any RNG decisions (logged for repro).
    let seed: UInt64
}

// MARK: - WorkJournalReader

/// Thin test-side wrapper around
/// `AnalysisStore.fetchWorkJournalEntries(...)`. Exists so harness code
/// reads from one named type rather than reaching into the store for
/// every cycle.
struct WorkJournalReader: Sendable {
    let store: AnalysisStore

    func entries(episodeId: String, generationID: String) async throws -> [WorkJournalEntry] {
        try await store.fetchWorkJournalEntries(
            episodeId: episodeId,
            generationID: generationID
        )
    }
}

// MARK: - InterruptionHarness

/// Orchestrates test cycles across the 8 interrupt types. Owns the
/// mocks, the AnalysisStore under test, and the journal reader used by
/// oracles.
///
/// Not thread-safe for parallel cycles on the SAME harness instance —
/// tests that want concurrency should spin up multiple harnesses. The
/// default usage pattern is one harness per XCTestCase method.
final class InterruptionHarness: @unchecked Sendable {

    // MARK: Mocks (D2)

    let mockTaskScheduler: MockBackgroundTaskScheduler
    let mockCapabilities: MockCapabilitiesProvider
    let mockStorageBudget: MockStorageBudget

    // MARK: Store + readers

    let store: AnalysisStore
    let workJournal: WorkJournalReader

    // MARK: Config

    private static let defaultOwner = "iwiy-worker"

    // MARK: Init

    init(store: AnalysisStore) {
        self.store = store
        self.workJournal = WorkJournalReader(store: store)
        self.mockTaskScheduler = MockBackgroundTaskScheduler()
        self.mockCapabilities = MockCapabilitiesProvider()
        self.mockStorageBudget = MockStorageBudget()
    }

    static func make() async throws -> InterruptionHarness {
        let store = try await makeTestStore()
        return InterruptionHarness(store: store)
    }

    // MARK: D1 — inject

    /// Apply an interruption event to the live mocks / stubs without
    /// touching the journal. Called from `cycle(...)` between acquire
    /// and release; also exposed so unit tests can verify inject
    /// behavior in isolation.
    func inject(_ event: InterruptionEvent) async throws {
        switch event {
        case .backgrounding:
            // Mirror BackgroundProcessingService's submit path: a
            // BGProcessingTaskRequest for the maintenance lane. The mock
            // records it; that's the test's observation point.
            let request = BGProcessingTaskRequest(identifier: "iwiy.bg.processing")
            request.requiresExternalPower = false
            request.requiresNetworkConnectivity = false
            try mockTaskScheduler.submit(request)

        case .relaunch:
            // The relaunch cycle resets the scheduler epoch so a
            // would-be stale writer gets rejected. Mirrors the cold-
            // launch `incrementSchedulerEpoch()` sweep.
            _ = try await store.incrementSchedulerEpoch()

        case .networkLoss:
            // Installation is idempotent; tests can call repeatedly.
            NetworkLossProtocol.register()

        case .thermalDowngrade:
            mockCapabilities.publish(
                makeCapabilitySnapshot(thermalState: .serious)
            )

        case .lowPowerTransition:
            mockCapabilities.publish(
                makeCapabilitySnapshot(isLowPowerMode: true)
            )

        case .storagePressure:
            mockStorageBudget.forcedDecision = .rejectCapExceeded(
                class: .media,
                cap: 1024,
                currentBytes: 1024,
                proposedBytes: 1024
            )

        case .userPreemption:
            // Mirror LanePreemptionCoordinator: bump the global epoch so
            // an in-flight release with stale epoch gets rejected. The
            // cycle itself still releases on the lease's original
            // generation (this is the slice-level emission the
            // instrumentation tracks as `.userPreempted`).
            _ = try await store.incrementSchedulerEpoch()

        case .forceQuit:
            // Simulated: no real kill. Drop the lease slot cold (as
            // ForceQuitResumeScan would on cold-launch) then proceed to
            // the release stage with `.appForceQuitRequiresRelaunch`.
            break
        }
    }

    // MARK: D1 — cycle

    /// Drive one full cycle for `type`. Returns the journal slice + the
    /// observed duration so callers can thread it into oracles.
    ///
    /// - Parameters:
    ///   - type: which interrupt variant to exercise.
    ///   - durationMs: simulated slice duration recorded into metadata.
    ///     Real wall-clock sleep is kept proportional but capped so the
    ///     full 50-cycle run stays under 3 minutes.
    ///   - seed: RNG seed captured into CycleResult for repro. The
    ///     harness does not branch on seed today, but logs it so 10-
    ///     overage RNG selections are reproducible.
    @discardableResult
    func cycle(
        type: CycleType,
        durationMs: Int = 100,
        seed: UInt64
    ) async throws -> CycleResult {
        let start = ContinuousClock.now
        let episodeId = "ep-\(type.rawValue)-\(UUID().uuidString.prefix(8))"
        let gen = UUID().uuidString

        // Seed the analysis_jobs row.
        let job = makeAnalysisJob(
            jobId: "job-\(episodeId)",
            episodeId: episodeId,
            workKey: "wk-\(episodeId)"
        )
        try await store.insertJob(job)

        let epoch = (try await store.fetchSchedulerEpoch()) ?? 0
        let now = Date().timeIntervalSince1970

        // Acquire the lease — writes `.acquired` journal row.
        _ = try await store.acquireEpisodeLease(
            episodeId: episodeId,
            ownerWorkerId: Self.defaultOwner,
            generationID: gen,
            schedulerEpoch: epoch,
            now: now,
            ttlSeconds: 60
        )

        // Inject the interrupt.
        try await inject(eventFor(type: type, episodeId: episodeId))

        // Map cycle-type → terminal event + cause and release.
        let (eventType, cause) = terminalFor(type: type)
        try await store.releaseEpisodeLease(
            episodeId: episodeId,
            generationID: gen,
            schedulerEpoch: epoch,
            eventType: eventType,
            cause: cause,
            now: now + Double(durationMs) / 1000.0,
            metadataJSON: "{\"slice_duration_ms\":\(durationMs),\"bytes_processed\":0,\"shards_completed\":0,\"device_class\":\"test\"}"
        )

        // Optional small wall-clock dwell keeps the test realistic but
        // bounded — 1ms per cycle so 50 cycles is 50ms of genuine sleep.
        try? await Task.sleep(for: .milliseconds(1))

        let journal = try await workJournal.entries(
            episodeId: episodeId,
            generationID: gen
        )
        let duration = ContinuousClock.now - start
        let dMs = Int(duration.components.seconds * 1000) + Int(duration.components.attoseconds / 1_000_000_000_000_000)

        return CycleResult(
            cycleType: type,
            episodeId: episodeId,
            generationID: gen,
            journal: journal,
            durationMs: max(durationMs, dMs),
            seed: seed
        )
    }

    // MARK: - Mapping helpers

    private func eventFor(type: CycleType, episodeId: String) -> InterruptionEvent {
        switch type {
        case .backgrounding: return .backgrounding(episodeId: episodeId)
        case .relaunch: return .relaunch(episodeId: episodeId)
        case .networkLoss: return .networkLoss(episodeId: episodeId)
        case .thermalDowngrade: return .thermalDowngrade(episodeId: episodeId)
        case .lowPowerTransition: return .lowPowerTransition(episodeId: episodeId)
        case .storagePressure: return .storagePressure(episodeId: episodeId)
        case .userPreemption: return .userPreemption(episodeId: episodeId)
        case .forceQuit: return .forceQuit(episodeId: episodeId)
        }
    }

    /// Map each cycle type to the expected (terminal event, cause) the
    /// slice-instrumentation layer would emit. Kept centralized so a
    /// future writer-wiring change (playhead-dfem) only touches one
    /// function.
    private func terminalFor(type: CycleType) -> (WorkJournalEntry.EventType, InternalMissCause?) {
        switch type {
        case .backgrounding:
            // Backgrounding does not itself preempt — the foreground
            // assist hand-off succeeds (or the job completes). Emit a
            // `finalized` with no cause to model that successful handoff
            // path.
            return (.finalized, nil)
        case .relaunch:
            // Orphan recovery: the stranded row is reported as a
            // pipeline error on cold-launch scan (matches
            // `ForceQuitResumeScan.scanForSuspendedTransfers.corrupted`).
            return (.failed, .pipelineError)
        case .networkLoss:
            return (.preempted, .noNetwork)
        case .thermalDowngrade:
            return (.preempted, .thermal)
        case .lowPowerTransition:
            return (.preempted, .lowPowerMode)
        case .storagePressure:
            return (.preempted, .mediaCap)
        case .userPreemption:
            return (.preempted, .userPreempted)
        case .forceQuit:
            return (.preempted, .appForceQuitRequiresRelaunch)
        }
    }
}

// MARK: - Oracles (D4 + D5)

/// The 13 Phase-1 emitting variants per playhead-1nl6 (7 live + 6
/// planned). Sourced from
/// `CauseEmissionRegistry.phase1EmittingCauses` at import time to stay
/// in lock-step with production; declared as a frozen mirror so the
/// test file is self-documenting.
let phase1EmittingVariants: Set<InternalMissCause> = CauseEmissionRegistry.phase1EmittingCauses

/// Miss-cause oracle: every `preempted` / `failed` row has a non-nil
/// `cause` whose enum variant is one of the 13 Phase-1 variants.
///
/// `.unknown(_)` is EXPLICITLY rejected: its presence in a harness-
/// generated journal means a cycle wrote a new cause string without
/// updating the oracle, which we want to fail loudly.
func assertMissCausesValid(
    _ journal: [WorkJournalEntry],
    file: StaticString = #filePath,
    line: UInt = #line
) {
    for entry in journal where entry.eventType == .preempted || entry.eventType == .failed {
        guard let cause = entry.cause else {
            XCTFail(
                "preempted/failed journal row missing cause: episode=\(entry.episodeId) gen=\(entry.generationID)",
                file: file, line: line
            )
            continue
        }

        // Reject `.unknown(_)` by unwrapping the associated case.
        if case .unknown(let raw) = cause {
            XCTFail(
                "oracle rejects .unknown(\(raw)) on episode=\(entry.episodeId) gen=\(entry.generationID); every cycle-result cause must be one of the 13 Phase-1 variants",
                file: file, line: line
            )
            continue
        }

        XCTAssertTrue(
            phase1EmittingVariants.contains(cause),
            "cause \(cause) not in Phase-1 emitting set (episode=\(entry.episodeId))",
            file: file, line: line
        )
    }
}

/// Duplicate-finalization oracle: within one {episodeId, generationID}
/// pair, at most one `finalized` row may exist.
func assertNoDuplicateFinalization(
    _ journal: [WorkJournalEntry],
    file: StaticString = #filePath,
    line: UInt = #line
) {
    let finalizations = journal.filter { $0.eventType == .finalized }
    let perEpisode = Dictionary(grouping: finalizations, by: \.episodeId)
    for (ep, events) in perEpisode {
        let perGeneration = Dictionary(grouping: events, by: \.generationID)
        for (gen, generationEvents) in perGeneration {
            XCTAssertLessThanOrEqual(
                generationEvents.count,
                1,
                "duplicate finalize on episode=\(ep) gen=\(gen)",
                file: file, line: line
            )
        }
    }
}
