// MetadataActivationConfig.swift
// ef2.4.7: Configuration gates for metadata activation in the ad detection pipeline.
//
// Each consumption point (lexical injection, classifier prior shift, FM scheduling)
// is independently gated and configurable. All three are off by default; the
// counterfactual evaluator controls activation via feature flags.

import Foundation

// MARK: - MetadataActivationConfig

/// Gates, weights, and thresholds for the three metadata consumption points.
///
/// All consumption points are independently toggleable. When a gate is disabled
/// the corresponding pipeline stage behaves identically to the pre-metadata path.
struct MetadataActivationConfig: Sendable, Equatable {

    // MARK: - Lexical Injection

    /// Whether metadata cues are injected into the ephemeral lexicon.
    let lexicalInjectionEnabled: Bool

    /// Floor metadataTrust below which lexical injection is skipped entirely.
    /// Default 0.0 means any non-zero trust allows injection.
    let lexicalInjectionMinTrust: Float

    /// Discount factor applied: weight = baseCategoryWeight * metadataTrust * discount.
    /// Spec mandates 0.75.
    let lexicalInjectionDiscount: Double

    // MARK: - Classifier Prior Shift

    /// Whether the classifier sigmoid midpoint shifts for metadata-warmed episodes.
    let classifierPriorShiftEnabled: Bool

    /// Minimum metadataTrust required to apply the prior shift.
    /// Spec mandates 0.08.
    let classifierPriorShiftMinTrust: Float

    /// The shifted sigmoid midpoint for metadata-warmed episodes.
    /// Default: 0.33 (vs baseline 0.37). Retuned 2026-04-23 (playhead-gtt9.3)
    /// so the band `(shifted, baseline]` sits inside the real-data
    /// confidence mode [0.30, 0.40) rather than the empty zone (0.22, 0.25].
    let classifierShiftedMidpoint: Double

    /// The baseline sigmoid midpoint (no metadata). Default: 0.37 —
    /// see `classifierShiftedMidpoint` for retune context.
    let classifierBaselineMidpoint: Double

    // MARK: - FM Scheduling

    /// Whether `.metadataSeededRegion` FM scheduling is active.
    let fmSchedulingEnabled: Bool

    /// Floor metadataTrust below which FM scheduling for seeded regions is skipped.
    let fmSchedulingMinTrust: Float

    // MARK: - Counterfactual Gate

    /// Master gate: when false, all three consumption points are disabled
    /// regardless of their individual flags. Tied to counterfactual evaluation.
    let counterfactualGateOpen: Bool

    // MARK: - Defaults

    /// Production default: master `counterfactualGateOpen` is open, but
    /// every per-gate flag below it remains off. The master gate is the
    /// `(gateOpen && enabled)` short-circuit that the `is*Active`
    /// computed properties use; with the gate closed, NO per-gate flag
    /// can ever take effect regardless of how it's tuned.
    ///
    /// playhead-sqhj (2026-04-26 follow-up to gtt9.4): the spike found
    /// that `counterfactualGateOpen=false` was a master kill on every
    /// NARL metadata-activation knob. Flipping the master open allows
    /// downstream gate-tuning beads (priorShift band, lexical
    /// injection trust floor, FM seeded-region scheduling) to actually
    /// land effects on the corpus. The per-gate flags below remain
    /// off — flipping the master is a behaviour change ONLY for callers
    /// that already enable a per-gate flag (today: tests using
    /// `.allEnabled` and the DEBUG override path). Production behaviour
    /// is unchanged until a future bead also flips a per-gate flag.
    static let `default` = MetadataActivationConfig(
        lexicalInjectionEnabled: false,
        lexicalInjectionMinTrust: 0.0,
        lexicalInjectionDiscount: 0.75,
        classifierPriorShiftEnabled: false,
        classifierPriorShiftMinTrust: 0.08,
        classifierShiftedMidpoint: 0.33,
        classifierBaselineMidpoint: 0.37,
        fmSchedulingEnabled: false,
        fmSchedulingMinTrust: 0.0,
        counterfactualGateOpen: true
    )

    /// All consumption points enabled (for testing and counterfactual-approved episodes).
    static let allEnabled = MetadataActivationConfig(
        lexicalInjectionEnabled: true,
        lexicalInjectionMinTrust: 0.0,
        lexicalInjectionDiscount: 0.75,
        classifierPriorShiftEnabled: true,
        classifierPriorShiftMinTrust: 0.08,
        classifierShiftedMidpoint: 0.33,
        classifierBaselineMidpoint: 0.37,
        fmSchedulingEnabled: true,
        fmSchedulingMinTrust: 0.0,
        counterfactualGateOpen: true
    )

    // MARK: - Effective State

    /// Whether lexical injection is effectively active (individual + master gate).
    var isLexicalInjectionActive: Bool {
        counterfactualGateOpen && lexicalInjectionEnabled
    }

    /// Whether classifier prior shift is effectively active.
    var isClassifierPriorShiftActive: Bool {
        counterfactualGateOpen && classifierPriorShiftEnabled
    }

    /// Whether FM scheduling for metadata-seeded regions is effectively active.
    var isFMSchedulingActive: Bool {
        counterfactualGateOpen && fmSchedulingEnabled
    }

    // MARK: - Resolution
    //
    // playhead-8em9 (narL): DEBUG-only override gate. Release safety is the
    // non-obvious constraint: the `#if DEBUG` branch and the `isReleaseLockActive`
    // short-circuit together guarantee that no override path can execute on a
    // shipping binary, even if a test or misconfigured flag tries to flip it.

    /// Resolve the effective activation config. See `resolved()` behavior
    /// in the file header.
    ///
    /// - Parameter releaseLockActive: test-only override simulating the
    ///   release-build branch; real callers never pass this.
    static func resolved(releaseLockActive: Bool = isReleaseLockActive) -> MetadataActivationConfig {
        if releaseLockActive {
            return .default
        }
        #if DEBUG
        return MetadataActivationOverride.current ?? .default
        #else
        // Defensive: even if the lock flag is false, a non-DEBUG build must
        // never read the override. The compile-time branch makes this
        // unreachable for a release binary regardless of runtime flags.
        return .default
        #endif
    }

    /// Hard-wired compile-time branch. In release builds this is always
    /// `true`, so `resolved()` short-circuits to `.default` without even
    /// consulting the override store. Exposed as `internal` (not private)
    /// so the override module can read the same flag for its own guards.
    static var isReleaseLockActive: Bool {
        #if DEBUG
        return false
        #else
        return true
        #endif
    }
}

// MARK: - MetadataActivationOverride

/// Debug-only override store for `MetadataActivationConfig.resolved()`.
/// The public mutators are always compiled (so call sites don't need
/// `#if DEBUG`) but degrade to no-ops in release — this is the release-
/// safety rationale: even an accidental call on a shipping binary
/// cannot flip the gate. Thread-safe via `NSLock`.
enum MetadataActivationOverride {

    /// Currently installed override, or `nil` if none.
    static var current: MetadataActivationConfig? {
        storage.read()
    }

    /// Install (or replace) the override. No-op in release builds.
    static func set(_ config: MetadataActivationConfig) {
        #if DEBUG
        storage.write(config)
        #endif
    }

    /// Clear any installed override. No-op in release builds.
    static func reset() {
        #if DEBUG
        storage.write(nil)
        #endif
    }

    /// Parse the launch arguments for `-MetadataActivationOverride <preset>`
    /// and apply the matching override. Unknown values are ignored so the
    /// existing override state is preserved. No-op in release builds.
    ///
    /// Recognized preset values:
    ///   - `allEnabled`  → `MetadataActivationConfig.allEnabled`
    ///   - `default`     → clears any installed override (equivalent to reset)
    static func applyLaunchArguments(_ arguments: [String]) {
        #if DEBUG
        guard let flagIdx = arguments.firstIndex(of: "-MetadataActivationOverride"),
              flagIdx + 1 < arguments.count else { return }
        let value = arguments[flagIdx + 1]
        switch value {
        case "allEnabled":
            set(.allEnabled)
        case "default":
            reset()
        default:
            // Ignore unknown values — avoid clobbering an explicit
            // programmatic override with a typo'd launch arg.
            break
        }
        #endif
    }

    // MARK: - Storage

    /// Thread-safe slot for the override. Implementation detail.
    private final class Storage: @unchecked Sendable {
        private var value: MetadataActivationConfig?
        private let lock = NSLock()

        func read() -> MetadataActivationConfig? {
            lock.lock()
            defer { lock.unlock() }
            return value
        }

        func write(_ newValue: MetadataActivationConfig?) {
            lock.lock()
            value = newValue
            lock.unlock()
        }
    }

    private static let storage = Storage()
}
