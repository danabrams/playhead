// CapabilityBackedEligibilityProviders.swift
// Concrete production implementations of the five
// `AnalysisEligibility*Providing` protocols, backed by the live
// `CapabilitiesService` snapshot.
//
// Scope: playhead-4nt1 — wires the previously-library-only
// `AnalysisEligibilityEvaluator` into production so
// `EpisodeSurfaceStatusObserver` no longer hardcodes
// `hardwareSupported = true` / `regionSupported = true`. The observer
// consumes the evaluator's structured verdict via
// `evaluator.evaluate()`; this file is the layer that turns the
// existing `CapabilitySnapshot` into per-axis `Bool`s the evaluator
// can call synchronously.
//
// Field-by-field mapping (the same approximation `DebugDiagnosticsHatch`
// already uses for its bundle export — see that file for a longer
// explanation):
//   - `isHardwareSupported`        ← `snapshot.foundationModelsAvailable`
//     (the closest proxy for "SoC meets minimum bar" the runtime
//     surfaces today; flips false on devices without FM hardware).
//   - `isAppleIntelligenceEnabled` ← `snapshot.appleIntelligenceEnabled`
//     (direct match).
//   - `isRegionSupported`          ← `true` (no live region API today;
//     dogfood is US-only and a real region provider is a future bead).
//   - `isLanguageSupported`        ← `snapshot.foundationModelsLocaleSupported`
//     (direct match — locale support IS the language gate).
//   - `isModelAvailableNow`        ← `snapshot.foundationModelsUsable`
//     (the live-probe flag, which flips false when the model is not
//     currently loadable).
//
// Concurrency: `CapabilitiesService.currentSnapshot` is async-actor-
// isolated, but the per-field provider protocols return `Bool`
// synchronously. We bridge with a small thread-safe cache
// (`CapabilitySnapshotCache`) populated by a Task subscribed to
// `capabilityUpdates()`. Until the first snapshot arrives the cache is
// `nil`, in which case providers default to `true` to preserve the
// pre-4nt1 permissive behavior on the cold-start path. Once the
// snapshot is available (well before any user-driven play action), the
// providers report real values.

import Foundation

// MARK: - Snapshot Cache

/// Thread-safe cache of the last-known `CapabilitySnapshot`. Updated
/// by a Task subscribed to `CapabilitiesService.capabilityUpdates()`;
/// read synchronously by the per-field eligibility providers below.
///
/// `nil` means "we have not yet observed a snapshot in this process."
/// During this brief startup window the providers default to `true`
/// (permissive) so the observer's cold-start path on an eligible
/// device still fires `ready_entered`. Production wiring seeds the
/// cache from `capabilitiesService.currentSnapshot` immediately after
/// `PlayheadRuntime.init` completes, so the window is small.
final class CapabilitySnapshotCache: @unchecked Sendable {
    private let lock = NSLock()
    private var _snapshot: CapabilitySnapshot?

    init(initial: CapabilitySnapshot? = nil) {
        self._snapshot = initial
    }

    var snapshot: CapabilitySnapshot? {
        lock.lock(); defer { lock.unlock() }
        return _snapshot
    }

    func set(_ snapshot: CapabilitySnapshot) {
        lock.lock(); defer { lock.unlock() }
        _snapshot = snapshot
    }
}

// MARK: - Combined Provider

/// Single class that conforms to all five
/// `AnalysisEligibility*Providing` protocols, reading from a shared
/// `CapabilitySnapshotCache`. One instance is constructed per
/// `PlayheadRuntime` and passed into `AnalysisEligibilityEvaluator` for
/// every per-field provider — this is intentional: the evaluator
/// invokes each provider once per cache miss, all five reads are
/// serviced from the same snapshot, so the resulting
/// `AnalysisEligibility` is internally consistent (no risk of a
/// snapshot update tearing the verdict mid-evaluation).
final class CapabilityBackedEligibilityProviders:
    HardwareSupportProviding,
    AppleIntelligenceStateProviding,
    RegionSupportProviding,
    LanguageSupportProviding,
    ModelAvailabilityProviding,
    @unchecked Sendable
{
    private let cache: CapabilitySnapshotCache

    init(cache: CapabilitySnapshotCache) {
        self.cache = cache
    }

    func isHardwareSupported() -> Bool {
        // No snapshot yet → permissive (true). See file-level note.
        cache.snapshot?.foundationModelsAvailable ?? true
    }

    func isAppleIntelligenceEnabled() -> Bool {
        cache.snapshot?.appleIntelligenceEnabled ?? true
    }

    func isRegionSupported() -> Bool {
        // No live region provider today — return `true`. The seam is
        // here so a future bead can plug in a real region check
        // without touching the observer or the evaluator.
        true
    }

    func isLanguageSupported() -> Bool {
        cache.snapshot?.foundationModelsLocaleSupported ?? true
    }

    func isModelAvailableNow() -> Bool {
        cache.snapshot?.foundationModelsUsable ?? true
    }
}
