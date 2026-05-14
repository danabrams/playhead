// PipelineVersions.swift
// playhead-zx6i — B4 fast revalidation from persisted features.
//
// Single value type that captures the three version axes a persisted
// ad-detection run depends on:
//
//   * `modelVersion`           — `AdDetectionConfig.default.detectorVersion`
//   * `policyVersion`          — `SkipPolicyConfig.default.policyVersion`
//   * `featureSchemaVersion`   — `SharedVersionConstants.featureSchemaVersion`
//
// When any of these values change between two runs against the same
// asset, the persisted `AdWindow` / classifier rows from the prior run
// are stale. B4 revalidates by re-running classifier + fusion +
// boundary against the persisted `TranscriptChunk` + `FeatureWindow`
// rows from that prior run, skipping the expensive ASR / decode /
// feature-extraction stages.
//
// `current()` is the single source of truth for "what versions is THIS
// process running with" — every consumer (the AnalysisJobRunner
// short-circuit, the success-stamp at the end of `runBackfill`, the
// state-store comparison) routes through this function. Adding a new
// version axis is a single edit here plus a single new source-read,
// not a search-and-replace across the codebase.

import Foundation

/// The triple of pipeline versions that, taken together, defines
/// "this run's decisions are reproducible". A change to any one of
/// the three is a version bump from B4's perspective.
///
/// Encoded to JSON (via Codable) for persistence in
/// `RevalidationStateStore`. Equality is field-wise; two snapshots are
/// equal iff every axis matches.
struct PipelineVersions: Sendable, Equatable, Codable {

    /// Detection model identifier. Today this is the value of
    /// `AdDetectionConfig.default.detectorVersion` (e.g. `"detection-v1"`).
    /// Bumped when the classifier rules / weights change such that the
    /// per-chunk decisions over identical persisted features would be
    /// different.
    let modelVersion: String

    /// Skip-policy identifier. Today this is
    /// `SkipPolicyConfig.default.policyVersion` (e.g. `"skip-policy-v1"`).
    /// Bumped when the gating thresholds that decide
    /// `autoSkipEligible` vs `detectOnly` move.
    let policyVersion: String

    /// Feature-schema integer. Today this is
    /// `SharedVersionConstants.featureSchemaVersion` (e.g. `1`).
    /// Bumped when window sizes / mel bin counts / feature shapes
    /// change so that prior `FeatureWindow` rows can no longer be
    /// consumed by the current classifier without re-extraction.
    ///
    /// IMPORTANT — B4 does NOT today distinguish a `featureSchemaVersion`
    /// bump from a `modelVersion` / `policyVersion` bump: the runner
    /// fires the revalidation short-circuit on ANY field mismatch
    /// (`completed != current`), and the revalidation path consumes
    /// whatever `FeatureWindow` rows are persisted as-is. A schema bump
    /// that genuinely changes window sizes / bin counts therefore
    /// requires either (a) a paired full-pipeline migration that
    /// re-extracts features before flipping the schema version, or
    /// (b) a follow-up bead that adds a per-axis policy to the
    /// short-circuit gate. The current single-axis-or-nothing wiring is
    /// adequate for the in-scope use cases (model / policy tuning
    /// bumps; feature schema is rarely bumped without a migration
    /// alongside), but the abort-on-feature-schema-bump behaviour
    /// callers might assume from this name is NOT implemented here.
    let featureSchemaVersion: Int

    /// Read the current process's pipeline-version triple. Every
    /// consumer must route through this method — duplicated literal
    /// reads of the constituent versions cause silent drift between
    /// detector / state-store / diagnostics surfaces (the same
    /// pattern that motivated `SharedVersionConstants`).
    static func current() -> PipelineVersions {
        PipelineVersions(
            modelVersion: AdDetectionConfig.default.detectorVersion,
            policyVersion: SkipPolicyConfig.default.policyVersion,
            featureSchemaVersion: SharedVersionConstants.featureSchemaVersion
        )
    }

    /// Sentinel triple used by playhead-7mq for pre-instrumentation
    /// rows: `'pre-instrumentation' / 0 / 0`. Exposed so callers that
    /// inspect persisted SwiftData columns can compare against the
    /// known sentinel without re-importing the constants. B4 itself
    /// does NOT compare against this — it uses the UserDefaults-backed
    /// `RevalidationStateStore`, where "no entry" means "needs full
    /// analysis"; the 7mq sentinel is row-level and lives on the
    /// per-table columns.
    static let sevenMqSentinel = PipelineVersions(
        modelVersion: "pre-instrumentation",
        policyVersion: "0",
        featureSchemaVersion: 0
    )
}
