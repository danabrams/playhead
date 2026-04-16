// AnalysisEligibility.swift
// Per-device (NOT per-user) eligibility contract for on-device analysis.
//
// Scope: playhead-2fd (Phase 0 Guardrails).
//
// Product policy — Policy B, decided 2026-04-16:
//   Ineligible devices remain full first-class citizens for podcast
//   download and playback; only the AI-powered analysis features are
//   marked "Analysis unavailable" in-app. Do NOT gate the App Store
//   listing on hardware or add an install-time capability filter — the
//   one-time-purchase + zero-marginal-cost monetization model favors
//   reach over a hardware floor. Policy B is recoverable without
//   reinstall; hardware gating at install time is a one-way door we
//   deliberately avoid. Downstream bead playhead-sueq is predicated on
//   this decision.
//
// Five fields, per spec:
//   - hardwareSupported       : SoC meets min bar for on-device ASR + classifier.
//   - appleIntelligenceEnabled: user has AI toggled on in Settings.
//   - regionSupported         : device region is in supported list.
//   - languageSupported       : primary speech/UI locale is supported.
//   - modelAvailableNow       : ML model assets currently resident and loadable
//                               (distinct from permanent support — can flip
//                               transiently on download/unload).
//
// Evaluated per-device globally, cached in-memory with a 4-hour TTL.
// Invalidated on locale change, region change, OS-version change,
// Apple Intelligence toggle, or foreground-after-background > TTL.
// The evaluator is side-effect free and safe to call from the UI
// thread — `modelAvailableNow` reflects the last-known state; refreshes
// happen opportunistically and MUST NOT block the caller.

import Foundation

// MARK: - AnalysisEligibility Record

/// Per-device eligibility snapshot for on-device analysis. Consumed by the
/// cause-attribution layer (see `CauseAttributionContext.modelAvailableNow`
/// and `ResolutionHint.enableAppleIntelligence`) and by any UI surface that
/// needs to decide whether to render the "Analysis unavailable" state.
///
/// The five fields are independent: each represents a distinct gate, and
/// `isFullyEligible` is `true` only when every gate passes. Future
/// per-field resolution hints (downstream beads playhead-sueq / playhead-5bb3)
/// map each `false` field to a concrete user-facing hint.
struct AnalysisEligibility: Codable, Sendable, Equatable {

    /// Device SoC meets the minimum bar for on-device ASR + classifier.
    /// Permanent for the device/OS pair — does NOT flip at runtime.
    let hardwareSupported: Bool

    /// User has Apple Intelligence enabled in Settings. Flips when the user
    /// toggles the system setting; see `noteAppleIntelligenceToggled()` on
    /// the evaluator.
    let appleIntelligenceEnabled: Bool

    /// Device region is in the supported list. Flips rarely (user changes
    /// region in Settings); see `noteRegionChanged()` on the evaluator.
    let regionSupported: Bool

    /// Primary speech/UI locale is supported. Flips when the user switches
    /// the primary language; see `noteLocaleChanged()` on the evaluator.
    let languageSupported: Bool

    /// ML model assets currently resident and loadable. Distinct from
    /// permanent support: can flip transiently on download/unload. Reads
    /// the last-known state only — the evaluator never blocks on a model
    /// load.
    let modelAvailableNow: Bool

    /// Timestamp of the evaluation that produced this record.
    let capturedAt: Date

    /// `true` when every gate passes.
    var isFullyEligible: Bool {
        hardwareSupported &&
        appleIntelligenceEnabled &&
        regionSupported &&
        languageSupported &&
        modelAvailableNow
    }

    init(
        hardwareSupported: Bool,
        appleIntelligenceEnabled: Bool,
        regionSupported: Bool,
        languageSupported: Bool,
        modelAvailableNow: Bool,
        capturedAt: Date
    ) {
        self.hardwareSupported = hardwareSupported
        self.appleIntelligenceEnabled = appleIntelligenceEnabled
        self.regionSupported = regionSupported
        self.languageSupported = languageSupported
        self.modelAvailableNow = modelAvailableNow
        self.capturedAt = capturedAt
    }

    // MARK: - Policy Metadata

    /// Documented product policy for ineligible devices. See the file-level
    /// comment above for full rationale.
    enum PolicyDecision: String, Sendable, Equatable {
        /// Policy B (chosen 2026-04-16): ineligible devices retain download
        /// and playback; only analysis features are marked unavailable.
        case downloadOnlyWithUnavailableMessaging = "download_only_with_unavailable_messaging"
    }

    /// The active policy for ineligible devices.
    static let policyDecision: PolicyDecision = .downloadOnlyWithUnavailableMessaging

    /// Date the policy decision was recorded. Bump this string only as part
    /// of a deliberate policy reversal — it's a grep anchor for reviewers.
    static let policyDecisionDate: String = "2026-04-16"
}

// MARK: - Clock

/// Injectable clock so TTL tests can advance virtual time without waiting.
protocol AnalysisEligibilityClock: Sendable {
    var now: Date { get }
}

/// Default wall-clock implementation.
struct SystemAnalysisEligibilityClock: AnalysisEligibilityClock {
    var now: Date { Date() }
}

// MARK: - Per-Field Providers

/// Returns whether the device SoC meets the minimum hardware bar for
/// on-device analysis. Result is expected to be stable across the device's
/// lifetime; invalidation is driven only by OS-version changes.
protocol HardwareSupportProviding: Sendable {
    func isHardwareSupported() -> Bool
}

/// Returns whether the user has enabled Apple Intelligence in Settings.
protocol AppleIntelligenceStateProviding: Sendable {
    func isAppleIntelligenceEnabled() -> Bool
}

/// Returns whether the device's current region is in the supported list.
protocol RegionSupportProviding: Sendable {
    func isRegionSupported() -> Bool
}

/// Returns whether the primary speech/UI locale is supported.
protocol LanguageSupportProviding: Sendable {
    func isLanguageSupported() -> Bool
}

/// Returns whether the required ML model assets are currently resident and
/// loadable. Implementations MUST NOT block the caller on a model load —
/// return the last-known state and refresh opportunistically elsewhere.
protocol ModelAvailabilityProviding: Sendable {
    func isModelAvailableNow() -> Bool
}

// MARK: - Evaluator Protocol

/// Public evaluator surface. The evaluator is side-effect free from the
/// caller's perspective and safe to invoke from the UI thread.
protocol AnalysisEligibilityEvaluating: AnyObject, Sendable {
    /// Returns the cached eligibility if still valid, otherwise computes and
    /// caches a fresh record.
    func evaluate() -> AnalysisEligibility

    /// Force the next `evaluate()` call to recompute.
    func invalidate()

    /// Invalidate on locale change (primary speech/UI locale switch).
    func noteLocaleChanged()

    /// Invalidate on region change (Settings > General > Language & Region).
    func noteRegionChanged()

    /// Invalidate if the OS version has changed since the last evaluation.
    /// No-op when the version string is unchanged.
    func noteOSVersionChangedIfNeeded()

    /// Invalidate on Apple Intelligence toggle in Settings.
    func noteAppleIntelligenceToggled()

    /// Call on app-foreground. Invalidates only when the cached record is
    /// older than the TTL; cheaper than an unconditional `invalidate()`
    /// because it preserves recent caches across quick backgrounding.
    func noteAppForegrounded()
}

// MARK: - Evaluator

/// Default evaluator. Thread-safe via an internal lock; `evaluate()` is
/// non-blocking (no model loads, no network, no disk beyond the injected
/// providers which contract to be non-blocking themselves).
final class AnalysisEligibilityEvaluator: AnalysisEligibilityEvaluating, @unchecked Sendable {

    /// Default cache TTL: 4 hours.
    static let defaultTTL: TimeInterval = 4 * 60 * 60

    private let hardwareProvider: HardwareSupportProviding
    private let appleIntelligenceProvider: AppleIntelligenceStateProviding
    private let regionProvider: RegionSupportProviding
    private let languageProvider: LanguageSupportProviding
    private let modelAvailabilityProvider: ModelAvailabilityProviding
    private let clock: AnalysisEligibilityClock
    private let osVersionProvider: @Sendable () -> String
    private let ttl: TimeInterval

    private let lock = NSLock()
    private var cached: AnalysisEligibility?
    private var cachedOSVersion: String?

    init(
        hardwareProvider: HardwareSupportProviding,
        appleIntelligenceProvider: AppleIntelligenceStateProviding,
        regionProvider: RegionSupportProviding,
        languageProvider: LanguageSupportProviding,
        modelAvailabilityProvider: ModelAvailabilityProviding,
        clock: AnalysisEligibilityClock = SystemAnalysisEligibilityClock(),
        osVersionProvider: @escaping @Sendable () -> String = {
            ProcessInfo.processInfo.operatingSystemVersionString
        },
        ttl: TimeInterval = AnalysisEligibilityEvaluator.defaultTTL
    ) {
        self.hardwareProvider = hardwareProvider
        self.appleIntelligenceProvider = appleIntelligenceProvider
        self.regionProvider = regionProvider
        self.languageProvider = languageProvider
        self.modelAvailabilityProvider = modelAvailabilityProvider
        self.clock = clock
        self.osVersionProvider = osVersionProvider
        self.ttl = ttl
    }

    // MARK: - Evaluation

    func evaluate() -> AnalysisEligibility {
        lock.lock()
        defer { lock.unlock() }
        if let cached, !isExpiredLocked(cached: cached) {
            return cached
        }
        return recomputeLocked()
    }

    // MARK: - Invalidation hooks

    func invalidate() {
        lock.lock()
        defer { lock.unlock() }
        cached = nil
    }

    func noteLocaleChanged() { invalidate() }

    func noteRegionChanged() { invalidate() }

    func noteOSVersionChangedIfNeeded() {
        let current = osVersionProvider()
        lock.lock()
        defer { lock.unlock() }
        if cachedOSVersion != current {
            cached = nil
        }
    }

    func noteAppleIntelligenceToggled() { invalidate() }

    func noteAppForegrounded() {
        lock.lock()
        defer { lock.unlock() }
        guard let cached else { return }
        if isExpiredLocked(cached: cached) {
            self.cached = nil
        }
    }

    // MARK: - Private (lock-held helpers)

    private func isExpiredLocked(cached: AnalysisEligibility) -> Bool {
        clock.now.timeIntervalSince(cached.capturedAt) > ttl
    }

    @discardableResult
    private func recomputeLocked() -> AnalysisEligibility {
        let snapshot = AnalysisEligibility(
            hardwareSupported: hardwareProvider.isHardwareSupported(),
            appleIntelligenceEnabled: appleIntelligenceProvider.isAppleIntelligenceEnabled(),
            regionSupported: regionProvider.isRegionSupported(),
            languageSupported: languageProvider.isLanguageSupported(),
            modelAvailableNow: modelAvailabilityProvider.isModelAvailableNow(),
            capturedAt: clock.now
        )
        cached = snapshot
        cachedOSVersion = osVersionProvider()
        return snapshot
    }
}
