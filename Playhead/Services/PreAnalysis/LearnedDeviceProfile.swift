// LearnedDeviceProfile.swift
// playhead-beh3 (Phase 3 deliverable 5) — SwiftData persistence layer
// for the adaptive Welford+EWMA estimator's per-device-class state.
//
// One row per device class (the model carries `DeviceClass.rawValue` as
// a `@Attribute(.unique)` key, so attempting to insert a second row for
// the same bucket throws — `LearnedDeviceProfileStore` enforces fetch-
// or-create so this is never tripped under normal use).
//
// Schema migration: this model joins `SwiftDataStore.schema` and
// `PlayheadSchemaV1.models` as an additive entity. Existing installs
// observe the row on first launch the first time the estimator
// records an observation (lightweight migration creates the entity;
// the row itself is provisioned lazily by the store).
//
// Why a separate file from `AdaptiveDeviceProfileEstimator`:
//   The math layer is pure / Sendable and tested without SwiftData.
//   This file owns ONLY the persistence projection (snapshot/apply +
//   the @Model class). Keeping the layers split makes the math tests
//   cheap and the persistence tests focused.

import Foundation
import SwiftData

// MARK: - Model

/// SwiftData row holding the persistent state of the adaptive
/// device-profile estimator for ONE device class. The math layer
/// (`AdaptiveDeviceProfileEstimator`) reads/writes through the
/// `snapshot()` and `apply(_:)` projection so the algorithm code never
/// touches a `ModelContext`.
///
/// Field design mirrors `AdaptiveDeviceProfileState`. The two types are
/// kept distinct so the math layer can be unit-tested without
/// SwiftData, AND so future migrations can change one side without
/// rewriting the other.
@Model
final class LearnedDeviceProfile {

    /// Stable identifier for the device class. Matches
    /// `DeviceClass.rawValue`. Unique so the table is genuinely one-
    /// row-per-class — duplicate inserts trip a SwiftData constraint.
    @Attribute(.unique) var deviceClassRawValue: String

    /// Seed grant-window in seconds captured at the first observation.
    /// Persisted so a future ship of a different seed table cannot
    /// silently shift the EWMA's anchor point.
    var seedGrantWindowSeconds: Double

    /// Welford running mean of observed grant-window durations.
    var welfordMean: Double

    /// Welford running M2 (sum of squared deltas).
    var welfordM2: Double

    /// Total observations recorded for this row.
    var sampleCount: Int

    /// Current EWMA value, in seconds.
    var ewmaSeconds: Double

    /// Persisted, notch-rate-limited scale factor applied to the seed
    /// to produce the adaptive grant-window. Clamped to the
    /// `[clampBandLower, clampBandUpper]` band.
    var persistedScaleFactor: Double

    /// Wall-clock the persisted scale factor last advanced.
    var lastNotchChangeAt: Date?

    /// Running count of consecutive observations whose RAW EWMA
    /// candidate saturated one end of the clamp band.
    var consecutiveClampedObservations: Int

    /// Most recent reason the estimator self-reverted to the seed, if
    /// any. Stored as the closed enum's raw value so the column type
    /// stays a `String?` and future enum additions don't require a
    /// migration.
    var lastRevertReasonRawValue: String?

    /// Wall-clock the row was first inserted.
    var createdAt: Date

    /// Wall-clock the row was last touched (any field).
    var updatedAt: Date

    /// Schema version stamp. Bumped on any breaking shape change.
    /// V1 ships with this bead; the field is here from day one so
    /// future migrations have a stable axis to gate on.
    var schemaVersion: Int

    /// Construct from a math-layer snapshot. Used by the store on
    /// first-write provisioning.
    init(snapshot: AdaptiveDeviceProfileState) {
        self.deviceClassRawValue = snapshot.deviceClassRawValue
        self.seedGrantWindowSeconds = snapshot.seedGrantWindowSeconds
        self.welfordMean = snapshot.welfordMean
        self.welfordM2 = snapshot.welfordM2
        self.sampleCount = snapshot.sampleCount
        self.ewmaSeconds = snapshot.ewmaSeconds
        self.persistedScaleFactor = snapshot.persistedScaleFactor
        self.lastNotchChangeAt = snapshot.lastNotchChangeAt
        self.consecutiveClampedObservations = snapshot.consecutiveClampedObservations
        self.lastRevertReasonRawValue = snapshot.lastRevertReason?.rawValue
        self.createdAt = snapshot.createdAt
        self.updatedAt = snapshot.updatedAt
        self.schemaVersion = snapshot.schemaVersion
    }

    /// Project to the math-layer state snapshot. Used by the store
    /// before handing the row to `AdaptiveDeviceProfileEstimator.apply`.
    func snapshot() -> AdaptiveDeviceProfileState {
        AdaptiveDeviceProfileState(
            deviceClassRawValue: deviceClassRawValue,
            seedGrantWindowSeconds: seedGrantWindowSeconds,
            welfordMean: welfordMean,
            welfordM2: welfordM2,
            sampleCount: sampleCount,
            ewmaSeconds: ewmaSeconds,
            persistedScaleFactor: persistedScaleFactor,
            lastNotchChangeAt: lastNotchChangeAt,
            consecutiveClampedObservations: consecutiveClampedObservations,
            lastRevertReason: lastRevertReasonRawValue.flatMap(
                AdaptiveDeviceProfileRevertReason.init(rawValue:)
            ),
            createdAt: createdAt,
            updatedAt: updatedAt,
            schemaVersion: schemaVersion
        )
    }

    /// Overwrite the row's mutable columns from a math-layer
    /// snapshot. The unique key (`deviceClassRawValue`) and the
    /// immutable identity fields (`createdAt`, `seedGrantWindowSeconds`)
    /// are not touched — the snapshot's values for those fields must
    /// match the row's existing values (caller-side invariant; the
    /// store enforces this by always fetching the row before applying).
    func apply(_ snapshot: AdaptiveDeviceProfileState) {
        // Math layer must not be allowed to change the row identity.
        // Asserting equality in debug catches caller bugs; production
        // silently keeps the old identity to avoid a UB-flavored crash
        // in the persistence path.
        assert(snapshot.deviceClassRawValue == self.deviceClassRawValue,
               "LearnedDeviceProfile.apply: deviceClassRawValue must not change")
        assert(snapshot.seedGrantWindowSeconds == self.seedGrantWindowSeconds,
               "LearnedDeviceProfile.apply: seedGrantWindowSeconds must not change")

        self.welfordMean = snapshot.welfordMean
        self.welfordM2 = snapshot.welfordM2
        self.sampleCount = snapshot.sampleCount
        self.ewmaSeconds = snapshot.ewmaSeconds
        self.persistedScaleFactor = snapshot.persistedScaleFactor
        self.lastNotchChangeAt = snapshot.lastNotchChangeAt
        self.consecutiveClampedObservations = snapshot.consecutiveClampedObservations
        self.lastRevertReasonRawValue = snapshot.lastRevertReason?.rawValue
        self.updatedAt = snapshot.updatedAt
        // schemaVersion is intentionally not updated here — the row
        // carries the schema version it was inserted under. Migrations
        // bump it explicitly.
    }
}

// MARK: - Diagnostics projection

/// Wire-shape snapshot of one `LearnedDeviceProfile` row, emitted into
/// the diagnostics bundle so support can attribute "stuck-at-seed" or
/// "reverted on Day 3" observations. Every field uses snake_case via
/// explicit `CodingKeys` to match the existing bundle conventions
/// (see `DefaultBundle.SchedulerEvent`).
struct LearnedDeviceProfileDiagnosticRecord: Codable, Sendable, Equatable {
    let deviceClass: String
    let seedGrantWindowSeconds: Double
    let sampleCount: Int
    let welfordMean: Double
    let welfordVariance: Double
    let ewmaSeconds: Double
    let persistedScaleFactor: Double
    let activated: Bool
    let consecutiveClampedObservations: Int
    let lastRevertReason: String?
    let lastNotchChangeAt: Double?
    let createdAt: Double
    let updatedAt: Double
    let schemaVersion: Int

    enum CodingKeys: String, CodingKey {
        case deviceClass = "device_class"
        case seedGrantWindowSeconds = "seed_grant_window_seconds"
        case sampleCount = "sample_count"
        case welfordMean = "welford_mean"
        case welfordVariance = "welford_variance"
        case ewmaSeconds = "ewma_seconds"
        case persistedScaleFactor = "persisted_scale_factor"
        case activated
        case consecutiveClampedObservations = "consecutive_clamped_observations"
        case lastRevertReason = "last_revert_reason"
        case lastNotchChangeAt = "last_notch_change_at"
        case createdAt = "created_at"
        case updatedAt = "updated_at"
        case schemaVersion = "schema_version"
    }

    /// Build a diagnostic record from a math-layer snapshot.
    /// Activation is derived against the supplied tuning (production
    /// always passes `.standard`).
    static func from(
        snapshot: AdaptiveDeviceProfileState,
        tuning: AdaptiveDeviceProfileTuning = .standard
    ) -> LearnedDeviceProfileDiagnosticRecord {
        LearnedDeviceProfileDiagnosticRecord(
            deviceClass: snapshot.deviceClassRawValue,
            seedGrantWindowSeconds: snapshot.seedGrantWindowSeconds,
            sampleCount: snapshot.sampleCount,
            welfordMean: snapshot.welfordMean,
            welfordVariance: snapshot.welfordVariance,
            ewmaSeconds: snapshot.ewmaSeconds,
            persistedScaleFactor: snapshot.persistedScaleFactor,
            activated: snapshot.isActivated(tuning: tuning),
            consecutiveClampedObservations: snapshot.consecutiveClampedObservations,
            lastRevertReason: snapshot.lastRevertReason?.rawValue,
            lastNotchChangeAt: snapshot.lastNotchChangeAt?.timeIntervalSince1970,
            createdAt: snapshot.createdAt.timeIntervalSince1970,
            updatedAt: snapshot.updatedAt.timeIntervalSince1970,
            schemaVersion: snapshot.schemaVersion
        )
    }
}
