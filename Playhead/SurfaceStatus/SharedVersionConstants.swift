// SharedVersionConstants.swift
// Single source of truth for version strings that have no natural home on
// a single service's `Config` type.
//
// Motivation (playhead-l274 code-review I1): `DiagnosticsVersions.current()`
// and service `Config.default` types each owned their own copy of the
// canonical version strings (`apple-speech-v1`, `skip-policy-v1`, etc.).
// When a service's version bumps, Diagnostics silently drifted — the
// Settings panel kept displaying the old value.
//
// Policy:
//   * If a string has a natural home (e.g. transcript model version lives
//     on `TranscriptEngineServiceConfig.default`), read it from there and
//     do NOT duplicate it here.
//   * Use this file only for strings that are not already owned by a
//     single service's `Config` type — today: the feature-schema version.
//     (`CoverageSummary` takes `featureSchemaVersion` as a parameter, so
//     the number has no single config-owner.)
//
// Every caller — Diagnostics UI, tests, persistence — must route through
// the live-service default (`TranscriptEngineServiceConfig.default.modelVersion`,
// `SkipPolicyConfig.default.policyVersion`, `AdDetectionService.hotPathReplayModelVersion`)
// or through this namespace. Duplicated literals are a code-review block.

import Foundation

/// Versions owned directly by this module (i.e. with no single-service
/// `Config` home). Service-owned versions live on the service's own
/// `Config.default`; this enum holds the rest.
enum SharedVersionConstants {
    /// Integer version of the feature schema (window sizes, mel bin
    /// counts, etc.) surfaced as a string by Diagnostics and as an `Int`
    /// by `CoverageSummary`. Bumped when a feature change invalidates
    /// prior coverage even if the model and policy versions are stable.
    ///
    /// Kept as a single constant here because `CoverageSummary` accepts
    /// this value as a parameter rather than owning a default — there is
    /// no single service-level `Config.default.featureSchemaVersion` to
    /// read from.
    static let featureSchemaVersion: Int = 1

    /// String form of `featureSchemaVersion`, used by the Diagnostics UI
    /// where the other version columns render as free-form strings.
    static var featureSchemaVersionString: String {
        String(featureSchemaVersion)
    }
}
