// CoverageSummary.swift
// Placeholder type consumed by `episodeSurfaceStatus(...)`. The real
// shape of `CoverageSummary` lands in playhead-cthe (Phase 2 —
// "Persist CoverageSummary + derive playbackReadiness from coverage +
// readinessAnchor"). Until that bead lands, the reducer accepts this
// minimal stub and gracefully defaults `playbackReadiness` to `.none`
// when the argument is nil.
//
// TODO(playhead-cthe): Replace this stub with the real CoverageSummary
// implementation. The reducer call-site contract is the only thing this
// file guarantees — it returns `PlaybackReadiness` so the reducer can
// plumb it through without needing to understand the internal shape.

import Foundation

// MARK: - CoverageSummary (stub)

/// Minimal placeholder for the Phase 2 `CoverageSummary` record. The real
/// type (owned by playhead-cthe) will carry confirmed ad windows, feature
/// coverage, and the derivation of `playbackReadiness`. For Phase 1.5 we
/// ship a stub that the reducer can branch on.
///
/// We model the readiness derivation behind a protocol-like surface so
/// the reducer call-site does not need to change when Phase 2 ships: the
/// reducer asks the `CoverageSummary` for its `readiness(anchor:)` and
/// the stub returns `.none`. When playhead-cthe lands, the real type
/// will implement the same method with the real logic and this file
/// can be deleted (or kept as a compatibility alias).
struct CoverageSummary: Sendable, Hashable, Codable {

    /// Phase 2 will replace this flag with per-slice coverage and a
    /// proper `playbackReadiness` derivation. For now the stub exposes
    /// a single bit so tests can assert the nil-vs-non-nil branch of
    /// the reducer (the non-nil branch still defaults to `.none` but
    /// it is distinguishable in the snapshot output).
    let hasAnyCoverage: Bool

    init(hasAnyCoverage: Bool) {
        self.hasAnyCoverage = hasAnyCoverage
    }

    /// Derive the playback-readiness signal given the current readiness
    /// anchor. Phase 1.5 stub: always returns `.none` regardless of
    /// input. Phase 2 (playhead-cthe) will replace this with the real
    /// coverage-based derivation.
    func readiness(anchor: TimeInterval?) -> PlaybackReadiness {
        _ = anchor // suppress unused-param warning; stub intentionally ignores
        return .none
    }
}

// MARK: - PlaybackReadiness

/// The playback-readiness signal surfaced alongside an
/// `EpisodeSurfaceStatus`. Phase 2 will expand this when the real
/// coverage math lands; for Phase 1.5 the reducer only emits `.none`
/// because there is no coverage data to derive `.partial` or `.ready`
/// from yet.
///
/// Named on its own (rather than as a nested `CoverageSummary.Readiness`)
/// so the snapshot-test fixtures can reference it by a stable name even
/// after Phase 2 replaces the `CoverageSummary` stub.
enum PlaybackReadiness: String, Sendable, Hashable, Codable, CaseIterable {
    /// No playback readiness signal — either coverage is absent or the
    /// reducer was invoked before any confirmed analysis output.
    case none
    /// Partial coverage: at least one ad window is confirmed for some
    /// prefix of the episode but not the whole thing. Phase 2 (cthe)
    /// will start emitting this.
    case partial
    /// Full coverage up to the readiness anchor. Phase 2 (cthe) will
    /// start emitting this.
    case ready
}
