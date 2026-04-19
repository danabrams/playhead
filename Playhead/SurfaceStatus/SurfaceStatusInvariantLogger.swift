// SurfaceStatusInvariantLogger.swift
// Forward-declaration stub for the Tier-B production logging channel that
// records impossible-state violations from the surface-status reducer.
// The real implementation is owned by playhead-ol05 (Phase 1.5 — "State-
// transition audit + impossible-state assertions + cross-target contract
// test"). This stub gives the reducer a stable type to call into so that
// ol05 can land without rewriting the reducer's call-sites.
//
// TODO(playhead-ol05): Replace the no-op implementation below with the
// real OSLog-backed signal emitter and hook it into the state-transition
// audit. The contract is:
//   * `invariantViolated(_:)` is called from inside the reducer whenever
//     a provably-impossible combination of inputs is detected (e.g. a
//     `cause` is present but the reducer has no ladder branch that would
//     surface it).
//   * The call MUST be safe from any thread and MUST NOT throw.
//   * Emission is fire-and-forget — the reducer never awaits the signal.

import Foundation

/// Tier-B production logging channel for impossible-state violations in
/// the surface-status reducer. The stub implementation is a no-op so
/// Phase 1.5 can ship the reducer before ol05 finalizes the log schema.
///
/// Modeled as a `enum` with `static` methods so there is no instance to
/// construct and no reference-counting to pay on the hot path. The
/// reducer calls through the enum namespace (`SurfaceStatusInvariantLogger.invariantViolated(...)`)
/// which ol05 can later swap to a class-backed implementation without
/// changing the call-sites.
enum SurfaceStatusInvariantLogger {

    /// Record an impossible-state violation. Stub implementation is a
    /// no-op — see the file-level TODO for the real contract.
    ///
    /// The `message` parameter is left as a plain `String` (not a
    /// structured type) because the real log schema is ol05's decision,
    /// and a leading concrete shape here would bake in a design choice
    /// the reviewer should own.
    static func invariantViolated(_ message: String) {
        // TODO(playhead-ol05): emit to the real Tier-B logging channel.
        _ = message
    }
}
