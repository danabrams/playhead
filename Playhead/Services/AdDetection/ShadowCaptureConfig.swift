// ShadowCaptureConfig.swift
// playhead-narl.2: Configuration for the FM dual-run shadow capture subsystem.
//
// The shadow capture subsystem runs in two lanes (Lane A: JIT during strict
// playback; Lane B: background thorough pass when thermal .nominal + charging).
// Both lanes are globally kill-switched by ``dualFMCaptureEnabled``.
//
// Placed alongside ``MetadataActivationConfig`` per the design doc, but kept in
// its own file so the two subsystems can evolve independently.

import Foundation

// MARK: - ShadowCaptureConfig

/// Kill switch and tuning constants for the `.allEnabled` FM shadow capture
/// subsystem. Decoupled from ``MetadataActivationConfig`` so the shadow
/// lanes can be disabled independently of the production activation flags.
///
/// The harness in `playhead-narl.1` reads shadow-captured FM responses to
/// honestly evaluate `fmSchedulingEnabled` — it does NOT participate in
/// production gate decisions.
struct ShadowCaptureConfig: Sendable, Equatable {

    // MARK: - Master Kill Switch

    /// Master kill switch. When `false`, **both lanes** no-op at their entry
    /// points — no shadow FM calls fire, no rows are written, and no export
    /// lines appear in `shadow-decisions.jsonl`.
    ///
    /// Default is `true` for Dan's build (Phase 1 evidence-gathering). A
    /// release build can leave this on — the production gate still short-
    /// circuits shadow data consumption; only the harness reads it.
    let dualFMCaptureEnabled: Bool

    // MARK: - Lane A (JIT playback) tunables

    /// Lookahead region size, in seconds, relative to the current playhead.
    /// Lane A identifies windows inside `[playhead, playhead + N]` that
    /// `.default` skipped but `.allEnabled` would schedule, and fires shadow
    /// FM on them.
    ///
    /// Start at 60s per bead spec. Keep small — a larger window costs battery
    /// even with a bounded budget. Tune only against measured regression data.
    let laneALookaheadSeconds: TimeInterval

    /// Maximum number of shadow FM calls Lane A may dispatch per minute of
    /// wall-clock time. Rate limiter is a leaky-bucket style approximation
    /// (see ``ShadowCaptureCoordinator``). Intentionally conservative —
    /// the bead spec explicitly warns about thermal/battery regression risk
    /// if this is too generous.
    let laneAMaxCallsPerMinute: Int

    /// Maximum number of shadow FM calls in flight at any instant for Lane A.
    /// FoundationModels is serialized per-session on device, so concurrency
    /// above 1 offers little throughput gain while multiplying peak memory.
    let laneAMaxInFlight: Int

    // MARK: - Lane B (background thorough) tunables

    /// Maximum number of shadow FM calls Lane B may dispatch per scheduling
    /// tick. A tick = one "the device is idle + nominal + charging" probe.
    /// Larger values drain the backlog faster but produce bigger thermal
    /// excursions per tick.
    let laneBCallsPerTick: Int

    /// Maximum number of shadow FM calls in flight at any instant for Lane B.
    let laneBMaxInFlight: Int

    // MARK: - Defaults

    /// Production/Dan-build default: dual-run capture ON, conservative budget
    /// constants. If this produces measurable thermal regression in the field,
    /// tighten the constants — or flip ``dualFMCaptureEnabled`` to `false`.
    static let `default` = ShadowCaptureConfig(
        dualFMCaptureEnabled: true,
        laneALookaheadSeconds: 60,
        laneAMaxCallsPerMinute: 4,
        laneAMaxInFlight: 1,
        laneBCallsPerTick: 2,
        laneBMaxInFlight: 1
    )

    /// Disabled preset — both lanes no-op on entry. Used by unit tests to
    /// verify the kill switch plumbing and by any production override path
    /// that wants to emergency-disable shadow capture without recompiling.
    static let disabled = ShadowCaptureConfig(
        dualFMCaptureEnabled: false,
        laneALookaheadSeconds: 60,
        laneAMaxCallsPerMinute: 0,
        laneAMaxInFlight: 0,
        laneBCallsPerTick: 0,
        laneBMaxInFlight: 0
    )
}

// MARK: - ShadowCapturedBy

/// Which lane wrote a shadow FM response. Persisted as TEXT in
/// `shadow_fm_responses.capturedBy`. Round-trips unchanged through the
/// `shadow-decisions.jsonl` export.
enum ShadowCapturedBy: String, Sendable, Codable, Hashable {
    /// Lane A: JIT during strict playback.
    case laneA
    /// Lane B: background thorough pass (thermal nominal + charging).
    case laneB
}

// MARK: - ShadowConfigVariant

/// Which `.allEnabled` configuration variant a shadow FM response was
/// captured under. Phase 1 only exercises `.allEnabledShadow`; reserved so
/// future variants (e.g. `.allEnabledMinusPriorShift`) can co-exist in the
/// same table without a schema migration.
enum ShadowConfigVariant: String, Sendable, Codable, Hashable {
    case allEnabledShadow
}
