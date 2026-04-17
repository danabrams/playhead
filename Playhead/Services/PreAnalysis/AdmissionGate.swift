// AdmissionGate.swift
// playhead-bnrs (Phase 1 deliverable 5): multi-resource admission gate.
//
// Transport, CPU, storage, and thermal budgets evaluated as independent
// admission gates per work item (AND gate, fail-fast). Hard rejections
// short-circuit; soft throttles shrink slice size. All thermal / battery
// / low-power reads route through `QualityProfile` (closed
// playhead-5ih) — this gate does NOT read `ProcessInfo.thermalState` or
// `isLowPowerMode` directly.
//
// Design: a pure, stateless `enum AdmissionGate` with static methods so
// the admission decision is side-effect free and testable without an
// actor harness. The gate takes every runtime input as an explicit
// argument (quality profile, transport snapshot, storage snapshot,
// device-class profile, job descriptor) so the call site retains
// ownership of IO and tests can drive the full truth table deterministically.
//
// Spec references:
//   Bead: playhead-bnrs.
//   Thermal derating table: sourced from
//   `QualityProfile.SchedulerPolicy.sliceFraction`
//   (`Playhead/Services/Capabilities/QualityProfile.swift:129`). Do NOT
//   re-derive — the canonical values are 1.0 / 1.0 / 0.5 / 0.0 for
//   nominal / fair / serious / critical.

import Foundation

// MARK: - TransportSnapshot

/// Point-in-time network state consumed by the transport gate.
///
/// This type is intentionally a small input struct rather than an
/// addition to `CapabilitySnapshot`: `CapabilitySnapshot` is persisted
/// as JSON alongside every analysis run, and widening its schema would
/// require a codable migration for a read that is only consulted at
/// admission time. The scheduler synthesizes this value per pass from
/// the `NWPathMonitor` surface owned upstream.
struct TransportSnapshot: Sendable, Equatable {
    /// Network reachability axis the transport gate gates on.
    enum Reachability: Sendable, Equatable {
        /// No network reachable at all — reject with `.noNetwork`.
        case unreachable
        /// Reachable via Wi-Fi or ethernet. Unlimited transport budget.
        case wifi
        /// Reachable via cellular radio only. Subject to the cellular
        /// cap and the user's `allowsCellular` preference.
        case cellular
    }

    let reachability: Reachability

    /// Session identifier for the work item being admitted. Mirrors
    /// `BackgroundSessionIdentifier.interactive` vs `.maintenance` from
    /// closed bead playhead-24cm: `maintenance` transfers must never
    /// run on cellular (subscription auto-downloads are Wi-Fi only).
    enum Session: Sendable, Equatable {
        /// User-initiated transfer (Play / explicit Download). Cellular
        /// is permitted when the user's `allowsCellular` preference is
        /// `true`.
        case interactive
        /// Auto-download / subscription pre-cache. Cellular is rejected
        /// regardless of `allowsCellular`.
        case maintenance
    }

    let session: Session

    /// User preference mirroring `UserPreferences.allowsCellular`.
    /// Consulted only on cellular reachability with an interactive
    /// session.
    let userAllowsCellular: Bool

    /// Convenience: reachability == .cellular.
    var isCellular: Bool { reachability == .cellular }
}

// MARK: - StorageSnapshot

/// A per-artifact-class view of the storage budget state at admission
/// time. The gate consumes this as a plain struct rather than taking a
/// live `StorageBudget` actor reference so the check stays synchronous
/// and tests can exercise the full truth table without an actor.
///
/// Callers construct this by snapshotting `StorageBudget.admit(class:
/// sizeBytes:)` outcomes + `cap(for:)` - `currentBytes(for:)` remaining
/// arithmetic for every class the job writes to.
struct StorageSnapshot: Sendable, Equatable {
    /// Per-class admission verdict: `true` when `StorageBudget.admit`
    /// would return `.accept` for the proposed write.
    let canAdmit: [ArtifactClass: Bool]

    /// Remaining bytes under each class's cap. For classes sharing the
    /// analysis cap (`warmResumeBundle`, `scratch`), this is the pool
    /// remainder, mirroring `StorageBudget.currentBytesGovernedByCap`.
    let remainingBytes: [ArtifactClass: Int64]

    func canAdmit(_ cls: ArtifactClass) -> Bool {
        canAdmit[cls] ?? true
    }

    func remaining(_ cls: ArtifactClass) -> Int64 {
        remainingBytes[cls] ?? 0
    }
}

// MARK: - AdmissionJob

/// The minimum job descriptor the admission gate needs. Separated from
/// `AnalysisJob` so the gate is decoupled from the persistent row
/// shape; the scheduler constructs this per admission pass.
struct AdmissionJob: Sendable, Equatable {
    /// The primary artifact class this job will write to (used for
    /// per-class storage slice-sizing). Multi-class jobs declare their
    /// full class set in `artifactClasses`.
    let artifactClasses: Set<ArtifactClass>

    /// Caller-estimated write bytes for this slice. The gate does not
    /// split this across classes — it is a headroom input, not a
    /// per-class allocation. `StorageBudget.canAdmit` per-class is the
    /// binding constraint.
    let estimatedWriteBytes: Int64

    init(artifactClasses: Set<ArtifactClass>, estimatedWriteBytes: Int64) {
        precondition(!artifactClasses.isEmpty, "AdmissionJob must declare at least one ArtifactClass")
        precondition(estimatedWriteBytes >= 0, "AdmissionJob.estimatedWriteBytes must be non-negative")
        self.artifactClasses = artifactClasses
        self.estimatedWriteBytes = estimatedWriteBytes
    }
}

// MARK: - GateAdmissionDecision

/// Outcome of a single `AdmissionGate` evaluation.
///
/// Named `GateAdmissionDecision` rather than the shorter
/// `AdmissionDecision` to avoid collision with the distinct
/// `AdmissionDecision` type in `AdmissionController` (backfill-job
/// scheduling), and the distinct `StorageAdmissionDecision` in
/// `StorageBudget`. Three unrelated admission surfaces live in this
/// codebase; the prefix disambiguates them at every call site.
enum GateAdmissionDecision: Sendable, Equatable {
    /// Job is admitted. `sliceBytes` is the min-of-all-gates slice size
    /// the caller must not exceed this pass.
    case admit(sliceBytes: Int64)

    /// Job is rejected. `cause` is the single `InternalMissCause` the
    /// scheduler should surface; when multiple gates fail simultaneously
    /// the cause is resolved via `CauseAttributionPolicy`.
    case reject(cause: InternalMissCause)
}

// MARK: - AdmissionGate

/// Pure admission-gate evaluation. All methods are `static` — the gate
/// holds no state.
enum AdmissionGate {

    // MARK: - Tuning constants

    /// Cellular slice cap. Interactive transfers on cellular clamp
    /// `sliceBytes` to this ceiling so each slice completes within a
    /// conservative radio-on window. Default 10 MiB per bead spec.
    static let cellularSliceCapBytes: Int64 = 10 * 1024 * 1024

    // MARK: - Public entrypoint

    /// Evaluate all four gates for `job`. AND-gate semantics: any
    /// single hard rejection short-circuits; soft throttles shrink the
    /// returned slice size.
    ///
    /// - Parameters:
    ///   - job: the work item being scheduled.
    ///   - profile: the current `QualityProfile`, already derived from
    ///     thermal/battery/LPM by `CapabilitySnapshot.qualityProfile`.
    ///   - deviceClass: the device-class bucket used to look up
    ///     `nominalSliceSizeBytes`, `bytesPerCpuSecond`, `cpuWindowSeconds`.
    ///   - deviceProfile: the per-device-class row (playhead-dh9b).
    ///   - storage: per-class storage admission snapshot (playhead-h7r).
    ///   - transport: network reachability + session + user preference.
    static func admit(
        job: AdmissionJob,
        profile: QualityProfile,
        deviceClass: DeviceClass,
        deviceProfile: DeviceClassProfile,
        storage: StorageSnapshot,
        transport: TransportSnapshot
    ) -> GateAdmissionDecision {
        // Collect every cause that applies simultaneously so
        // CauseAttributionPolicy can resolve precedence. The algorithm is
        // structured as "evaluate every gate, then resolve" rather than
        // "short-circuit on first failure" because the spec requires
        // multi-cause attribution (see Acceptance §6). A single active
        // cause trivially resolves to itself.
        var causes: [InternalMissCause] = []

        // Gate 1: thermal (hard reject on pauseAllWork).
        if profile.schedulerPolicy.pauseAllWork {
            causes.append(.thermal)
        }

        // Gate 2: transport.
        if let transportCause = transportRejection(transport) {
            causes.append(transportCause)
        }

        // Gate 3: storage (per-class admit check).
        if let storageCause = storageRejection(job: job, storage: storage) {
            causes.append(storageCause)
        }

        // Gate 4: CPU (hard-reject case folded into thermal via
        // pauseAllWork; a future CPU-budget exhaustion signal would land
        // here as `.taskExpired`). At Phase 1 the CPU gate contributes
        // only as a soft throttle in slice sizing; no hard-reject.

        if !causes.isEmpty {
            // Multi-cause: route through CauseAttributionPolicy. Single
            // cause: return it directly (policy would pick it anyway).
            if causes.count == 1 {
                return .reject(cause: causes[0])
            }
            let context = CauseAttributionContext(
                modelAvailableNow: true,
                retryBudgetRemaining: 0
            )
            let primary = CauseAttributionPolicy.primaryCause(
                among: causes,
                context: context
            ) ?? causes[0]
            return .reject(cause: primary)
        }

        // All hard gates pass — compute soft slice size.
        let slice = sliceBytes(
            profile: profile,
            deviceProfile: deviceProfile,
            storage: storage,
            transport: transport,
            job: job
        )
        return .admit(sliceBytes: slice)
    }

    // MARK: - Transport gate

    /// Returns the transport rejection cause, or `nil` when transport
    /// admits. The clamp on cellular slice size is applied later in
    /// `sliceBytes(...)`.
    static func transportRejection(_ transport: TransportSnapshot) -> InternalMissCause? {
        switch transport.reachability {
        case .unreachable:
            return .noNetwork
        case .wifi:
            return nil
        case .cellular:
            // Maintenance transfers are Wi-Fi only regardless of user
            // preference (subscription auto-downloads must not burn
            // cellular bytes without explicit action).
            if transport.session == .maintenance {
                return .wifiRequired
            }
            // Interactive: honor user preference.
            if !transport.userAllowsCellular {
                return .wifiRequired
            }
            return nil
        }
    }

    // MARK: - Storage gate

    /// Returns the storage rejection cause, or `nil` when every class
    /// the job writes to admits. When multiple classes fail, the first
    /// in the job's class set (iteration order) wins — this is a stable
    /// deterministic choice; callers that require a specific precedence
    /// across storage-only rejections should split the job.
    static func storageRejection(job: AdmissionJob, storage: StorageSnapshot) -> InternalMissCause? {
        for cls in job.artifactClasses {
            if !storage.canAdmit(cls) {
                return storageCause(for: cls)
            }
        }
        return nil
    }

    /// Map an artifact class whose cap is exceeded to the surfaced
    /// cause. `warmResumeBundle` shares the analysis cap with `scratch`
    /// so both map to `.analysisCap`.
    static func storageCause(for cls: ArtifactClass) -> InternalMissCause {
        switch cls {
        case .media: return .mediaCap
        case .warmResumeBundle, .scratch: return .analysisCap
        }
    }

    // MARK: - Thermal derating

    /// Thermal derating factor sourced from
    /// `QualityProfile.SchedulerPolicy.sliceFraction`. This is NOT a
    /// separate table — the bead spec explicitly forbids re-deriving
    /// thermal/LPM numbers here because LPM is already folded into
    /// `QualityProfile` upstream (closed playhead-5ih).
    ///
    /// Deployed canonical values (see
    /// `Playhead/Services/Capabilities/QualityProfile.swift:155-186`):
    ///   nominal  = 1.0
    ///   fair     = 1.0
    ///   serious  = 0.5
    ///   critical = 0.0 (hard-rejected via pauseAllWork before we get here)
    static func thermalDeratingFactor(_ profile: QualityProfile) -> Double {
        profile.schedulerPolicy.sliceFraction
    }

    // MARK: - Slice sizing

    /// Compute the per-pass slice size as the min of every contributing
    /// gate. Thermal=critical is filtered out upstream by the
    /// `pauseAllWork` short-circuit; this method never returns 0 on a
    /// successful admit.
    static func sliceBytes(
        profile: QualityProfile,
        deviceProfile: DeviceClassProfile,
        storage: StorageSnapshot,
        transport: TransportSnapshot,
        job: AdmissionJob
    ) -> Int64 {
        let nominal = Int64(deviceProfile.nominalSliceSizeBytes)

        // CPU-bound ceiling: bytesPerCpuSecond × cpuWindowSeconds.
        let cpuCeiling = Int64(deviceProfile.bytesPerCpuSecond) * Int64(deviceProfile.cpuWindowSeconds)

        // Thermal derating relative to nominal. `sliceFraction` of 0.0
        // would be the critical-thermal case; caller must have
        // short-circuited on `pauseAllWork` before this method.
        let thermalFactor = thermalDeratingFactor(profile)
        let thermalCeiling = Int64(Double(nominal) * thermalFactor)

        // Per-class storage headroom: min over every class the job
        // writes to, halved to preserve eviction-cycle headroom (per
        // spec, media halving applies to storage throttle).
        var storageCeiling: Int64 = .max
        for cls in job.artifactClasses {
            let remaining = storage.remaining(cls)
            // Clamp non-negative; `remaining < 0` is a caller bug.
            let half = max(0, remaining) / 2
            storageCeiling = min(storageCeiling, half)
        }

        // Transport ceiling: cellular cap or unlimited.
        let transportCeiling: Int64 = transport.isCellular ? cellularSliceCapBytes : .max

        return min(nominal, cpuCeiling, thermalCeiling, storageCeiling, transportCeiling)
    }
}
