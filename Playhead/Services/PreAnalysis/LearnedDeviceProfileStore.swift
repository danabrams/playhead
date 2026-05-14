// LearnedDeviceProfileStore.swift
// playhead-beh3 (Phase 3 deliverable 5) — persistence + consumer-facing
// API for the adaptive Welford+EWMA estimator. Wraps the SwiftData
// `LearnedDeviceProfile` `@Model` with a fetch-or-create + apply +
// snapshot surface, and exposes the consumer-friendly
// `resolvedDeviceProfile(seed:deviceClass:)` helper that the scheduler
// calls.
//
// Threading: SwiftData `ModelContext` is not `Sendable`, so the
// production store is pinned to `@MainActor` (matching the pattern used
// by `InstallIDProvider` and `SwiftDataDiagnosticsOptInSink`). The
// scheduler is an actor — it consults the store through the
// `LearnedDeviceProfileProviding` protocol seam below so a non-MainActor
// caller doesn't have to know about SwiftData's isolation.
//
// Feature-flag wiring: this file does NOT read
// `PreAnalysisConfig.useAdaptiveDeviceProfile`. The flag is consulted
// at the call site (the scheduler) so a flag-off path can short-circuit
// without even constructing a store — keeping the "byte-identical to
// today" rollback contract trivially obvious from grep.

import Foundation
import OSLog
import SwiftData

// MARK: - Protocol seam

/// Consumer-facing seam for adaptive device-profile resolution. The
/// scheduler calls `resolvedDeviceProfile(seed:deviceClass:)` on every
/// admission pass and gets back the adjusted profile (or the seed
/// verbatim when the estimator has not yet activated). The
/// `recordObservation(...)` surface is separate so the slice-completion
/// site can record without ever entering the read path.
///
/// `Sendable` so the scheduler actor can hold an `any LearnedDeviceProfileProviding`
/// without further isolation.
protocol LearnedDeviceProfileProviding: Sendable {

    /// Resolve the adaptive `DeviceClassProfile` for the supplied
    /// device class, scaling the seed by the estimator's persisted
    /// factor (or returning the seed verbatim when the estimator is
    /// not activated).
    ///
    /// Read-only: this surface never mutates estimator state, so no
    /// clock input is required. The rate-limited state mutation lives
    /// in `recordObservation(_:deviceClass:seed:)` where the
    /// `GrantWindowObservation.observedAt` drives the notch rate
    /// limiter from the caller's clock.
    func resolvedDeviceProfile(
        seed: DeviceClassProfile,
        deviceClass: DeviceClass
    ) async -> DeviceClassProfile

    /// Record one grant-window observation for the supplied device
    /// class. Returns the `ApplyResult` so callers that want to log
    /// divergence-reverts (the diagnostics bundle does) can do so
    /// without re-fetching the row.
    @discardableResult
    func recordObservation(
        _ observation: GrantWindowObservation,
        deviceClass: DeviceClass,
        seed: DeviceClassProfile
    ) async -> AdaptiveDeviceProfileApplyResult

    /// Snapshot every row currently in the store, in DeviceClass.allCases
    /// order. Used by the diagnostics bundle. Returns an empty array
    /// if no rows have been provisioned yet.
    func snapshot() async -> [AdaptiveDeviceProfileState]
}

// MARK: - No-op implementation

/// No-op provider used when the feature flag is OFF, or in tests that
/// don't care about adaptive behavior. Every method short-circuits to
/// the seed and zero observations are recorded.
///
/// Construction must remain free — the scheduler defaults to this
/// when no provider is injected so existing test factories that don't
/// know about beh3 keep working.
struct NoOpLearnedDeviceProfileProvider: LearnedDeviceProfileProviding {
    init() {}

    func resolvedDeviceProfile(
        seed: DeviceClassProfile,
        deviceClass: DeviceClass
    ) async -> DeviceClassProfile {
        seed
    }

    @discardableResult
    func recordObservation(
        _ observation: GrantWindowObservation,
        deviceClass: DeviceClass,
        seed: DeviceClassProfile
    ) async -> AdaptiveDeviceProfileApplyResult {
        AdaptiveDeviceProfileApplyResult(
            persistedScaleFactorChanged: false,
            didRevertToSeed: false,
            blockedByNotchRateLimit: false,
            clampSaturatedThisObservation: false
        )
    }

    func snapshot() async -> [AdaptiveDeviceProfileState] {
        []
    }
}

// MARK: - SwiftData-backed implementation

/// Production-grade `LearnedDeviceProfileProviding` backed by SwiftData.
///
/// One row per device class is provisioned lazily on the first
/// `recordObservation` call for that class. Reads (`resolvedDeviceProfile`)
/// fetch the row if present and return the seed verbatim when no row
/// exists yet — first launch is therefore byte-identical to the
/// flag-off path.
///
/// MainActor isolation rationale: SwiftData's `ModelContext` is not
/// `Sendable`, and the fetch-then-apply-then-save sequence below is
/// not thread-safe under concurrent writers. Pinning the store to
/// `@MainActor` (mirroring `InstallIDProvider`'s pattern) collapses
/// the critical section onto one queue so the unique-row invariant is
/// preserved.
@MainActor
final class SwiftDataLearnedDeviceProfileStore: LearnedDeviceProfileProviding {

    private let context: ModelContext
    private let tuning: AdaptiveDeviceProfileTuning
    private let clock: @Sendable () -> Date
    private let logger = Logger(
        subsystem: "com.playhead",
        category: "LearnedDeviceProfileStore"
    )

    /// - Parameters:
    ///   - context: SwiftData `ModelContext` (typically the shared
    ///     container's main-actor context, same as `InstallIDProvider`).
    ///   - tuning: Estimator tuning. Production uses `.standard`.
    ///   - clock: Wall-clock source. Tests inject a fixed clock; the
    ///     store calls it only when provisioning a new row (the
    ///     `observedAt` field on observations is supplied by the
    ///     caller).
    init(
        context: ModelContext,
        tuning: AdaptiveDeviceProfileTuning = .standard,
        clock: @escaping @Sendable () -> Date = { Date() }
    ) {
        self.context = context
        self.tuning = tuning
        self.clock = clock
    }

    // MARK: - LearnedDeviceProfileProviding

    func resolvedDeviceProfile(
        seed: DeviceClassProfile,
        deviceClass: DeviceClass
    ) async -> DeviceClassProfile {
        // Read path: never provision a row, never mutate state. We
        // either find an existing row and apply its scale factor, or
        // fall back to the seed verbatim.
        guard let state = fetchSnapshot(for: deviceClass) else {
            return seed
        }
        let factor = AdaptiveDeviceProfileEstimator.resolvedScaleFactor(
            state: state,
            tuning: tuning
        )
        // Activation-gate is enforced inside `resolvedScaleFactor`
        // (returns 1.0 below the floor), so `project` is always safe.
        if abs(factor - 1.0) <= .ulpOfOne {
            return seed
        }
        return AdaptiveDeviceProfileEstimator.project(
            seed: seed,
            scaledBy: factor
        )
    }

    @discardableResult
    func recordObservation(
        _ observation: GrantWindowObservation,
        deviceClass: DeviceClass,
        seed: DeviceClassProfile
    ) async -> AdaptiveDeviceProfileApplyResult {
        let row = fetchOrCreate(for: deviceClass, seed: seed)
        let prior = row.snapshot()
        // playhead-beh3 (R8): observability for the math-layer soft-reset
        // path. R5/R6 added in-memory healing for non-finite math/Date
        // fields; the math layer is pure and intentionally has no logger,
        // so a corruption-heal would otherwise fire silently in
        // production. Detecting the prior shape here (BEFORE the apply
        // call) is the cheapest cross-cut: it requires no new field on
        // `AdaptiveDeviceProfileApplyResult` and no new enum case. If
        // corruption is firing in the wild we'll see exactly which
        // device class and which axis tripped the heal. Logged at
        // `.notice` (same level as the divergence-revert log) so it
        // surfaces in default OSLog captures without being chatty.
        //
        // R9 false-positive fix: gate the log on the observation also
        // being valid (`grantWindowSeconds` finite + > 0 AND `observedAt`
        // finite). The math layer's entry guards (estimator step (1)/(1b))
        // drop non-finite observations BEFORE the soft-reset branch
        // (step (2)) runs, returning the state unchanged. Without this
        // gate, the log would falsely claim "soft-reset triggered" when
        // a corrupt prior coincides with a corrupt observation — the
        // state is actually byte-unchanged in that case, no heal
        // happened. Mirroring the math layer's entry guards keeps the
        // log truthful: it fires exactly when apply() will reach the
        // sanitization branch.
        //
        // R10 drift-mitigation: the `priorHadCorruptMath` predicate
        // MUST mirror the math layer's soft-reset trigger exactly, or
        // the log will silently desync from the heal it claims to
        // describe. R10 extended the math heal to include
        // `sampleCount < 0` (the integer-shaped cousin of non-finite
        // math, see step (2) doc-comment in the estimator file); we
        // mirror that addition here so the log keeps firing on the
        // same axis the math layer heals on.
        let observationIsValid =
            observation.grantWindowSeconds.isFinite
            && observation.grantWindowSeconds > 0
            && observation.observedAt.timeIntervalSinceReferenceDate.isFinite
        let priorHadCorruptMath =
            !prior.welfordMean.isFinite
            || !prior.welfordM2.isFinite
            || !prior.ewmaSeconds.isFinite
            || !prior.persistedScaleFactor.isFinite
            || prior.sampleCount < 0
        let priorHadNonFiniteDate: Bool = {
            guard let last = prior.lastNotchChangeAt else { return false }
            return !last.timeIntervalSinceReferenceDate.isFinite
        }()
        if observationIsValid && (priorHadCorruptMath || priorHadNonFiniteDate) {
            logger.notice(
                "LearnedDeviceProfile soft-reset triggered for \(deviceClass.rawValue, privacy: .public) math=\(priorHadCorruptMath, privacy: .public) date=\(priorHadNonFiniteDate, privacy: .public)"
            )
        }
        let (next, result) = AdaptiveDeviceProfileEstimator.apply(
            observation: observation,
            to: prior,
            tuning: tuning
        )
        row.apply(next)
        do {
            try context.save()
        } catch {
            logger.error(
                "save failed after recordObservation for \(deviceClass.rawValue, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
        }
        if result.didRevertToSeed {
            logger.notice(
                "LearnedDeviceProfile reverted to seed for \(deviceClass.rawValue, privacy: .public) reason=\(next.lastRevertReason?.rawValue ?? "unknown", privacy: .public)"
            )
        }
        return result
    }

    func snapshot() async -> [AdaptiveDeviceProfileState] {
        snapshotSync()
    }

    /// Synchronous variant of ``snapshot()``. Same isolation
    /// (`@MainActor`) as the async surface — the only difference is
    /// the lack of an artificial `async`/`await` hop. Used by the
    /// diagnostics hatch wiring, which already runs inside a
    /// `MainActor.run` block and would otherwise have to round-trip
    /// through `await` for no reason.
    func snapshotSync() -> [AdaptiveDeviceProfileState] {
        let descriptor = FetchDescriptor<LearnedDeviceProfile>()
        let rows: [LearnedDeviceProfile]
        do {
            rows = try context.fetch(descriptor)
        } catch {
            logger.error("snapshot fetch failed: \(error.localizedDescription, privacy: .public)")
            return []
        }
        // Stable ordering by DeviceClass.allCases position so the
        // diagnostics bundle is deterministic across calls.
        let order = Dictionary(uniqueKeysWithValues: DeviceClass.allCases.enumerated().map { ($1.rawValue, $0) })
        return rows
            .map { $0.snapshot() }
            .sorted { (a, b) in
                (order[a.deviceClassRawValue] ?? .max) < (order[b.deviceClassRawValue] ?? .max)
            }
    }

    // MARK: - Internals

    /// Fetch the row for the supplied device class. Returns nil
    /// without provisioning when no row exists yet. Used by the read
    /// path so a first launch with no observations is indistinguishable
    /// from the flag-off path.
    private func fetchSnapshot(for deviceClass: DeviceClass) -> AdaptiveDeviceProfileState? {
        guard let row = fetchRow(for: deviceClass) else { return nil }
        return row.snapshot()
    }

    /// Fetch the row for the supplied device class. Returns nil when
    /// the row has not been provisioned yet.
    private func fetchRow(for deviceClass: DeviceClass) -> LearnedDeviceProfile? {
        let key = deviceClass.rawValue
        // SwiftData's macro-`#Predicate` requires the captured value
        // be a local immutable; binding `key` keeps the predicate
        // legal under Swift 6 isolation analysis.
        var descriptor = FetchDescriptor<LearnedDeviceProfile>(
            predicate: #Predicate { $0.deviceClassRawValue == key }
        )
        descriptor.fetchLimit = 1
        do {
            return try context.fetch(descriptor).first
        } catch {
            logger.error(
                "fetch failed for \(key, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
            return nil
        }
    }

    /// Fetch the row for the supplied device class, provisioning a
    /// fresh row from the seed if none exists. The seed's
    /// `grantWindowMedianSeconds` is captured into the row so a
    /// future ship of a different seed cannot retroactively shift the
    /// EWMA's anchor.
    private func fetchOrCreate(
        for deviceClass: DeviceClass,
        seed: DeviceClassProfile
    ) -> LearnedDeviceProfile {
        if let row = fetchRow(for: deviceClass) {
            return row
        }
        let state = AdaptiveDeviceProfileState(
            deviceClassRawValue: deviceClass.rawValue,
            seedGrantWindowSeconds: Double(seed.grantWindowMedianSeconds),
            createdAt: clock()
        )
        let row = LearnedDeviceProfile(snapshot: state)
        context.insert(row)
        do {
            try context.save()
        } catch {
            logger.error(
                "first-insert save failed for \(deviceClass.rawValue, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
        }
        return row
    }
}
