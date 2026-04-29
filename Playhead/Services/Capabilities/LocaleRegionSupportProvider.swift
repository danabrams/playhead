// LocaleRegionSupportProvider.swift
// Live region-support gate for `AnalysisEligibilityEvaluator`. Replaces
// the placeholder `isRegionSupported() -> true` that previously lived on
// `CapabilityBackedEligibilityProviders`.
//
// Scope: playhead-kgn5 — wires a real region check into the eligibility
// pipeline. Today the gate is US-only (the dogfood region); expanding
// to additional regions is a single-line edit to `supportedRegions`.

import Foundation

/// Reads the device's current region from `Locale.current.region` and
/// reports whether it falls in the configured supported set.
///
/// `supportedRegions` is the single source of truth — update it here to
/// expand or contract the gate. `Locale.current.region?.identifier`
/// returns the ISO 3166-1 alpha-2 code (e.g. "US", "GB", "CA"), or nil
/// when no region is set on the device. A nil region is treated as
/// unsupported.
final class LocaleRegionSupportProvider: RegionSupportProviding, @unchecked Sendable {
    /// The set of regions where on-device analysis is enabled. Today the
    /// app is US-only; widen this constant when expanding the gate.
    static let supportedRegions: Set<String> = ["US"]

    private let supportedRegions: Set<String>
    private let regionProvider: @Sendable () -> String?

    init(
        supportedRegions: Set<String> = LocaleRegionSupportProvider.supportedRegions,
        regionProvider: @escaping @Sendable () -> String? = { Locale.current.region?.identifier }
    ) {
        self.supportedRegions = supportedRegions
        self.regionProvider = regionProvider
    }

    func isRegionSupported() -> Bool {
        guard let region = regionProvider() else { return false }
        return supportedRegions.contains(region)
    }
}
