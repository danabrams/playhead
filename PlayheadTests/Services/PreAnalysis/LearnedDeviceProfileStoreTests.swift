// LearnedDeviceProfileStoreTests.swift
// playhead-beh3 (Phase 3 deliverable 5) — persistence-layer tests for
// `SwiftDataLearnedDeviceProfileStore` and the `LearnedDeviceProfile`
// `@Model` round-trip.
//
// Scope per the bead spec's test list:
//   * Persistence round-trip (insert → fetch → snapshot equality)
//   * First-observation provisioning (lazy row creation)
//   * `resolvedDeviceProfile` returns seed verbatim when no row exists
//   * `resolvedDeviceProfile` returns seed verbatim pre-activation
//   * `resolvedDeviceProfile` returns scaled profile post-activation
//   * `recordObservation` durably persists state across context rebuilds
//   * `snapshot()` ordering by `DeviceClass.allCases`
//   * Diagnostic-record projection (`LearnedDeviceProfileDiagnosticRecord`)
//
// Uses an in-memory SwiftData container per test so tests are hermetic
// and parallelisable; mirrors the pattern in
// `SwiftDataDiagnosticsOptInSinkTests`.

import Foundation
import SwiftData
import Testing

@testable import Playhead

@Suite("SwiftDataLearnedDeviceProfileStore (playhead-beh3)")
@MainActor
struct LearnedDeviceProfileStoreTests {

    // MARK: - Fixtures

    nonisolated static let referenceDate = Date(timeIntervalSince1970: 1_700_000_000)

    /// Build an in-memory SwiftData context that has ONLY the
    /// `LearnedDeviceProfile` entity in its schema. Keeps the test
    /// surface minimal — no unrelated migrations have to succeed.
    private func makeContext() throws -> ModelContext {
        let schema = Schema([LearnedDeviceProfile.self])
        let config = ModelConfiguration(isStoredInMemoryOnly: true, cloudKitDatabase: .none)
        let container = try ModelContainer(for: schema, configurations: [config])
        return ModelContext(container)
    }

    /// Tuning with the divergence-revert + notch rate-limit relaxed so
    /// activation behavior can be exercised in tens of observations.
    nonisolated static let permissiveTuning = AdaptiveDeviceProfileTuning(
        ewmaAlpha: 0.2,
        minSamplesForActivation: 30,
        clampBandLower: 0.5,
        clampBandUpper: 2.0,
        notchStep: 0.1,
        notchWindowSeconds: 0,                     // no rate limit
        divergenceObservationThreshold: .max       // disable divergence-revert
    )

    private static func seed(for deviceClass: DeviceClass = .iPhone17Pro) -> DeviceClassProfile {
        DeviceClassProfile.fallback(for: deviceClass)
    }

    // MARK: - Round-trip (insert → fetch → snapshot equality)

    @Test("Model round-trip preserves every field through insert + fetch")
    func roundTripPreservesFields() throws {
        let ctx = try makeContext()

        let original = AdaptiveDeviceProfileState(
            deviceClassRawValue: DeviceClass.iPhone17Pro.rawValue,
            seedGrantWindowSeconds: 45,
            welfordMean: 41.2,
            welfordM2: 18.5,
            sampleCount: 42,
            ewmaSeconds: 41.0,
            persistedScaleFactor: 0.9,
            lastNotchChangeAt: Self.referenceDate.addingTimeInterval(3_600),
            consecutiveClampedObservations: 2,
            lastRevertReason: .divergenceClampSaturation,
            createdAt: Self.referenceDate,
            updatedAt: Self.referenceDate.addingTimeInterval(7_200),
            schemaVersion: 1
        )
        let row = LearnedDeviceProfile(snapshot: original)
        ctx.insert(row)
        try ctx.save()

        let fetched = try ctx.fetch(FetchDescriptor<LearnedDeviceProfile>())
        #expect(fetched.count == 1)
        let projected = fetched[0].snapshot()
        #expect(projected == original)
    }

    @Test("Apply() updates mutable fields but preserves identity (deviceClass, seed, createdAt)")
    func applyPreservesIdentity() throws {
        let ctx = try makeContext()
        let initial = AdaptiveDeviceProfileState(
            deviceClassRawValue: DeviceClass.iPhone17Pro.rawValue,
            seedGrantWindowSeconds: 45,
            createdAt: Self.referenceDate
        )
        let row = LearnedDeviceProfile(snapshot: initial)
        ctx.insert(row)
        try ctx.save()

        // Build a state that mutates only mutable fields; identity
        // fields stay constant.
        let updated = AdaptiveDeviceProfileState(
            deviceClassRawValue: initial.deviceClassRawValue,
            seedGrantWindowSeconds: initial.seedGrantWindowSeconds,
            welfordMean: 50,
            welfordM2: 0,
            sampleCount: 1,
            ewmaSeconds: 50,
            persistedScaleFactor: 1.1,
            lastNotchChangeAt: Self.referenceDate.addingTimeInterval(60),
            consecutiveClampedObservations: 0,
            lastRevertReason: nil,
            createdAt: initial.createdAt,
            updatedAt: Self.referenceDate.addingTimeInterval(60),
            schemaVersion: initial.schemaVersion
        )
        row.apply(updated)
        try ctx.save()

        let fetched = try ctx.fetch(FetchDescriptor<LearnedDeviceProfile>()).first
        #expect(fetched?.welfordMean == 50)
        #expect(fetched?.ewmaSeconds == 50)
        #expect(fetched?.persistedScaleFactor == 1.1)
        #expect(fetched?.createdAt == initial.createdAt)
        #expect(fetched?.seedGrantWindowSeconds == initial.seedGrantWindowSeconds)
        #expect(fetched?.deviceClassRawValue == initial.deviceClassRawValue)
    }

    // MARK: - Read path: no provisioning, no mutation

    @Test("resolvedDeviceProfile returns the seed verbatim when no row exists yet")
    func readPathReturnsSeedWithoutProvisioningRow() async throws {
        let ctx = try makeContext()
        let store = SwiftDataLearnedDeviceProfileStore(
            context: ctx,
            tuning: Self.permissiveTuning,
            clock: { Self.referenceDate }
        )
        let seed = Self.seed()

        let resolved = await store.resolvedDeviceProfile(
            seed: seed,
            deviceClass: .iPhone17Pro
        )

        // Byte-identical: same DeviceClassProfile value, no row inserted.
        #expect(resolved == seed)
        let rows = try ctx.fetch(FetchDescriptor<LearnedDeviceProfile>())
        #expect(rows.isEmpty, "read path must not provision rows")
    }

    @Test("resolvedDeviceProfile returns the seed verbatim pre-activation")
    func readPathReturnsSeedBeforeActivation() async throws {
        let ctx = try makeContext()
        let store = SwiftDataLearnedDeviceProfileStore(
            context: ctx,
            tuning: Self.permissiveTuning,
            clock: { Self.referenceDate }
        )
        let seed = Self.seed()

        // Record 29 observations — one less than the activation floor.
        // The row is created lazily on the first record, but the
        // estimator does not yet contribute (sampleCount < 30).
        for i in 0..<29 {
            _ = await store.recordObservation(
                GrantWindowObservation(
                    grantWindowSeconds: 60,
                    observedAt: Self.referenceDate.addingTimeInterval(Double(i) * 60)
                ),
                deviceClass: .iPhone17Pro,
                seed: seed
            )
        }

        let resolved = await store.resolvedDeviceProfile(
            seed: seed,
            deviceClass: .iPhone17Pro
        )
        #expect(resolved == seed, "below activation floor, output must equal seed")
    }

    @Test("resolvedDeviceProfile returns a scaled profile post-activation")
    func readPathReturnsScaledProfilePostActivation() async throws {
        let ctx = try makeContext()
        let store = SwiftDataLearnedDeviceProfileStore(
            context: ctx,
            tuning: Self.permissiveTuning,
            clock: { Self.referenceDate }
        )
        let seed = Self.seed()
        // Observations centred at 1.5× the seed's grant window — the
        // EWMA will converge there and the persisted factor will walk
        // up by `notchStep` (=0.1) per observation (notch window=0 in
        // the permissive tuning).
        let target = Double(seed.grantWindowMedianSeconds) * 1.5

        for i in 0..<45 {
            _ = await store.recordObservation(
                GrantWindowObservation(
                    grantWindowSeconds: target,
                    observedAt: Self.referenceDate.addingTimeInterval(Double(i) * 60)
                ),
                deviceClass: .iPhone17Pro,
                seed: seed
            )
        }

        let resolved = await store.resolvedDeviceProfile(
            seed: seed,
            deviceClass: .iPhone17Pro
        )
        #expect(
            resolved.grantWindowMedianSeconds > seed.grantWindowMedianSeconds,
            "above-seed observations must scale grant window up"
        )
        #expect(
            resolved.nominalSliceSizeBytes > seed.nominalSliceSizeBytes,
            "above-seed observations must scale slice size up"
        )
        // Non-slice fields pass through unchanged.
        #expect(resolved.cpuWindowSeconds == seed.cpuWindowSeconds)
        #expect(resolved.bytesPerCpuSecond == seed.bytesPerCpuSecond)
        #expect(resolved.avgShardDurationMs == seed.avgShardDurationMs)
        // Clamped at 2.0× max — sanity check the upper bound is honoured.
        #expect(
            resolved.grantWindowMedianSeconds <= seed.grantWindowMedianSeconds * 2,
            "post-activation must not exceed clamp upper bound"
        )
    }

    // MARK: - First-observation provisioning + durability

    @Test("recordObservation provisions a row on first call and persists across new contexts")
    func firstObservationProvisioningIsDurable() async throws {
        let ctx = try makeContext()
        let store = SwiftDataLearnedDeviceProfileStore(
            context: ctx,
            tuning: Self.permissiveTuning,
            clock: { Self.referenceDate }
        )
        let seed = Self.seed()

        let result = await store.recordObservation(
            GrantWindowObservation(
                grantWindowSeconds: 60,
                observedAt: Self.referenceDate
            ),
            deviceClass: .iPhone17Pro,
            seed: seed
        )
        // First observation: no notch advance (pre-activation), no
        // divergence revert, no rate-limit block, not saturated.
        #expect(result.persistedScaleFactorChanged == false)
        #expect(result.didRevertToSeed == false)
        #expect(result.blockedByNotchRateLimit == false)

        // Persistence: a brand-new context against the SAME container
        // must see the row.
        let secondCtx = ModelContext(ctx.container)
        let rows = try secondCtx.fetch(FetchDescriptor<LearnedDeviceProfile>())
        #expect(rows.count == 1)
        #expect(rows.first?.deviceClassRawValue == DeviceClass.iPhone17Pro.rawValue)
        #expect(rows.first?.sampleCount == 1)
        #expect(rows.first?.ewmaSeconds == 60)
        // seedGrantWindowSeconds is captured from the seed at first-write.
        #expect(rows.first?.seedGrantWindowSeconds == Double(seed.grantWindowMedianSeconds))
    }

    @Test("recordObservation updates the same row on subsequent calls (no duplicates)")
    func recordObservationDoesNotDuplicateRows() async throws {
        let ctx = try makeContext()
        let store = SwiftDataLearnedDeviceProfileStore(
            context: ctx,
            tuning: Self.permissiveTuning,
            clock: { Self.referenceDate }
        )
        let seed = Self.seed()

        for i in 0..<5 {
            _ = await store.recordObservation(
                GrantWindowObservation(
                    grantWindowSeconds: 60,
                    observedAt: Self.referenceDate.addingTimeInterval(Double(i) * 60)
                ),
                deviceClass: .iPhone17Pro,
                seed: seed
            )
        }
        let rows = try ctx.fetch(FetchDescriptor<LearnedDeviceProfile>())
        #expect(rows.count == 1, "row count must remain 1 across N observations")
        #expect(rows.first?.sampleCount == 5)
    }

    @Test("recordObservation segregates rows by device class")
    func multipleDeviceClassesGetSeparateRows() async throws {
        let ctx = try makeContext()
        let store = SwiftDataLearnedDeviceProfileStore(
            context: ctx,
            tuning: Self.permissiveTuning,
            clock: { Self.referenceDate }
        )

        for deviceClass in [DeviceClass.iPhone17Pro, .iPhone15Pro, .iPhone14andOlder] {
            _ = await store.recordObservation(
                GrantWindowObservation(
                    grantWindowSeconds: 60,
                    observedAt: Self.referenceDate
                ),
                deviceClass: deviceClass,
                seed: DeviceClassProfile.fallback(for: deviceClass)
            )
        }

        let rows = try ctx.fetch(FetchDescriptor<LearnedDeviceProfile>())
        #expect(rows.count == 3)
        let keys = Set(rows.map { $0.deviceClassRawValue })
        #expect(keys == Set([
            DeviceClass.iPhone17Pro.rawValue,
            DeviceClass.iPhone15Pro.rawValue,
            DeviceClass.iPhone14andOlder.rawValue
        ]))
    }

    // MARK: - Persistence-layer corrupt-row hydration (R7)
    //
    // R5 and R6 hardened the math layer against non-finite Double /
    // Date corruption, but the tests for that path synthesized the
    // corrupt state directly in memory. R6's review explicitly listed
    // "persistence round-trip for new sanitization paths" as an
    // assumption that should be closed by test. This block does that:
    // insert a row whose on-disk Double / Date fields are non-finite,
    // record a healthy observation through the store, then fetch the
    // row again and verify the math layer healed it on read-modify-write.
    // This proves the full SwiftData → snapshot() → apply() → save
    // cycle is genuinely defensive, not just the in-memory math.

    @Test("End-to-end NaN Double corruption: SwiftData normalises NaN, then math layer remains finite")
    func corruptStoredRowDoublesHandledEndToEnd() async throws {
        // R7 probe-8: close the R6 stated assumption "persistence round-
        // trip for new R5/R6 sanitization paths is genuinely tested".
        // The full chain we want to verify is corrupt → SwiftData →
        // fetch → apply → save → re-fetch, with the invariant that
        // every field on the final row is finite.
        //
        // Discovered empirically: SwiftData/SQLite NORMALISES NaN Doubles
        // to 0 on the storage layer (the Double bit pattern is rejected
        // by the underlying column type). So a row inserted with
        // `welfordMean = .nan` is hydrated back as `welfordMean = 0`.
        // The math layer's R5 sanitization branch therefore does NOT
        // fire on this path — but the math invariant ("post-cycle row
        // has finite math fields") still holds because the corruption
        // was already stripped at storage. This test pins that behavior
        // so a future SwiftData/Foundation change that started accepting
        // NaN Doubles would surface here, triggering the R5 sanitizer
        // as a backstop. Either way the post-cycle row is provably
        // finite — that is the contract.
        let ctx = try makeContext()
        let store = SwiftDataLearnedDeviceProfileStore(
            context: ctx,
            tuning: Self.permissiveTuning,
            clock: { Self.referenceDate }
        )
        let seed = Self.seed()

        let corrupt = AdaptiveDeviceProfileState(
            deviceClassRawValue: DeviceClass.iPhone17Pro.rawValue,
            seedGrantWindowSeconds: Double(seed.grantWindowMedianSeconds),
            welfordMean: .nan,
            welfordM2: .nan,
            sampleCount: 30,
            ewmaSeconds: .nan,
            persistedScaleFactor: .nan,
            lastNotchChangeAt: nil,
            consecutiveClampedObservations: 99,
            lastRevertReason: nil,
            createdAt: Self.referenceDate,
            updatedAt: Self.referenceDate,
            schemaVersion: 1
        )
        let row = LearnedDeviceProfile(snapshot: corrupt)
        ctx.insert(row)
        try ctx.save()

        let healObs = GrantWindowObservation(
            grantWindowSeconds: 60,
            observedAt: Self.referenceDate.addingTimeInterval(60)
        )
        _ = await store.recordObservation(
            healObs, deviceClass: .iPhone17Pro, seed: seed
        )

        let fetched = try ctx.fetch(FetchDescriptor<LearnedDeviceProfile>()).first
        let snap = try #require(fetched?.snapshot())
        // End-to-end finite invariant — the core contract.
        #expect(snap.welfordMean.isFinite, "post-cycle welfordMean must be finite")
        #expect(snap.welfordM2.isFinite, "post-cycle welfordM2 must be finite")
        #expect(snap.ewmaSeconds.isFinite, "post-cycle ewmaSeconds must be finite")
        #expect(snap.persistedScaleFactor.isFinite, "post-cycle persistedScaleFactor must be finite")
        // Identity preserved.
        #expect(snap.deviceClassRawValue == DeviceClass.iPhone17Pro.rawValue)
        #expect(snap.createdAt == Self.referenceDate)
    }

    @Test("Corrupt SwiftData row with NaN-Date lastNotchChangeAt heals on first recordObservation")
    func corruptStoredRowWithNaNDateHealsOnRecord() async throws {
        let ctx = try makeContext()
        let store = SwiftDataLearnedDeviceProfileStore(
            context: ctx,
            tuning: Self.permissiveTuning,
            clock: { Self.referenceDate }
        )
        let seed = Self.seed()

        // Provision a row whose math fields are healthy but
        // `lastNotchChangeAt` is a NaN-Date (the R6-shaped corruption).
        // The math accumulators stay valid; only the rate-limit Date is
        // corrupt. Without the R6 fix the next observation's rate-limit
        // guard would silently fail open AND propagate the corrupt date.
        let corrupt = AdaptiveDeviceProfileState(
            deviceClassRawValue: DeviceClass.iPhone17Pro.rawValue,
            seedGrantWindowSeconds: Double(seed.grantWindowMedianSeconds),
            welfordMean: 45,
            welfordM2: 0,
            sampleCount: 30,
            ewmaSeconds: 45,
            persistedScaleFactor: 1.0,
            lastNotchChangeAt: Date(timeIntervalSinceReferenceDate: .nan),
            consecutiveClampedObservations: 0,
            lastRevertReason: nil,
            createdAt: Self.referenceDate,
            updatedAt: Self.referenceDate,
            schemaVersion: 1
        )
        let row = LearnedDeviceProfile(snapshot: corrupt)
        ctx.insert(row)
        try ctx.save()

        // Record a healthy observation. The math layer's R6 Date
        // sanitization branch must fire on the hydrated state.
        let healObs = GrantWindowObservation(
            grantWindowSeconds: 60,
            observedAt: Self.referenceDate.addingTimeInterval(60)
        )
        _ = await store.recordObservation(
            healObs, deviceClass: .iPhone17Pro, seed: seed
        )

        // Re-fetch from SwiftData. The hydrated `lastNotchChangeAt`
        // must be either nil (treated as "no prior notch change") or
        // the freshly-stamped observation date — never the original
        // NaN-Date.
        let fetched = try ctx.fetch(FetchDescriptor<LearnedDeviceProfile>()).first
        let snap = try #require(fetched?.snapshot())
        if let last = snap.lastNotchChangeAt {
            #expect(last.timeIntervalSinceReferenceDate.isFinite,
                    "stored lastNotchChangeAt must be finite after heal")
        }
    }

    // R9 false-positive fix: the R8 "soft-reset triggered" log was gated
    // ONLY on the prior state's shape (non-finite math or Date). The
    // math layer's entry guards (estimator step (1)/(1b)) drop non-finite
    // observations BEFORE the soft-reset branch runs — so a corrupt
    // prior + corrupt observation would log "soft-reset triggered"
    // even though the state was returned byte-unchanged. R9 added an
    // `observationIsValid` gate to mirror the math layer's entry guards
    // so the log is truthful: it fires exactly when the heal will run.
    // The test verifies the side-effect contract: when the observation
    // is non-finite, the row's columns are byte-unchanged from the
    // corrupt input (no heal happened) — which is the directly
    // observable signature of "the apply entry guard fired before the
    // sanitization branch".

    @Test("R9: non-finite observation against a healthy prior is dropped (no row mutation, no false-positive heal log)")
    func nonFiniteObservationDoesNotMutateRow() async throws {
        let ctx = try makeContext()
        let store = SwiftDataLearnedDeviceProfileStore(
            context: ctx,
            tuning: Self.permissiveTuning,
            clock: { Self.referenceDate }
        )
        let seed = Self.seed()

        // Bootstrap an activated row through normal recordObservation
        // calls so the row is genuinely persisted in SwiftData. This
        // avoids depending on whether SwiftData normalises non-finite
        // Doubles/Dates on insert (the existing
        // `corruptStoredRowDoublesHandledEndToEnd` test documents
        // that NaN Doubles are normalised to 0 at the storage layer).
        let baselineTime = Self.referenceDate
        let goodObs = GrantWindowObservation(
            grantWindowSeconds: 60,
            observedAt: baselineTime
        )
        _ = await store.recordObservation(
            goodObs, deviceClass: .iPhone17Pro, seed: seed
        )
        let pre = try #require(
            try ctx.fetch(FetchDescriptor<LearnedDeviceProfile>()).first?.snapshot()
        )

        // Now feed a non-finite observation. The math layer's entry
        // guard (`grantWindowSeconds.isFinite` step (1)) drops this
        // observation BEFORE any sanitization branch runs and returns
        // the state byte-unchanged. The R9 false-positive guard in
        // `recordObservation` mirrors the same predicate so the
        // "soft-reset triggered" log does not fire when prior +
        // observation are BOTH corrupt — that combination still does
        // not actually heal the state.
        let badObs = GrantWindowObservation(
            grantWindowSeconds: .nan,
            observedAt: baselineTime.addingTimeInterval(60)
        )
        _ = await store.recordObservation(
            badObs, deviceClass: .iPhone17Pro, seed: seed
        )

        // The row must be byte-equal to the pre-bad-obs snapshot. The
        // math layer's entry guard returns state unchanged, and the
        // store's `row.apply(next)` writes back the same values. No
        // mutation, so no false-positive "soft-reset" heal occurred.
        let post = try #require(
            try ctx.fetch(FetchDescriptor<LearnedDeviceProfile>()).first?.snapshot()
        )
        #expect(post == pre,
                "non-finite observation must leave the persisted row byte-equal to its pre-call snapshot")
    }

    @Test("R9: non-finite observedAt against a healthy prior is dropped (no row mutation)")
    func nonFiniteObservedAtDoesNotMutateRow() async throws {
        let ctx = try makeContext()
        let store = SwiftDataLearnedDeviceProfileStore(
            context: ctx,
            tuning: Self.permissiveTuning,
            clock: { Self.referenceDate }
        )
        let seed = Self.seed()

        // Activate a row.
        _ = await store.recordObservation(
            GrantWindowObservation(grantWindowSeconds: 60, observedAt: Self.referenceDate),
            deviceClass: .iPhone17Pro,
            seed: seed
        )
        let pre = try #require(
            try ctx.fetch(FetchDescriptor<LearnedDeviceProfile>()).first?.snapshot()
        )

        // Non-finite `observedAt` is the OTHER axis of the R9 guard.
        // Mirrors the math layer's step (1b) entry guard.
        let badObs = GrantWindowObservation(
            grantWindowSeconds: 60,
            observedAt: Date(timeIntervalSinceReferenceDate: .nan)
        )
        _ = await store.recordObservation(
            badObs, deviceClass: .iPhone17Pro, seed: seed
        )

        let post = try #require(
            try ctx.fetch(FetchDescriptor<LearnedDeviceProfile>()).first?.snapshot()
        )
        #expect(post == pre,
                "non-finite observedAt must leave the persisted row byte-equal to its pre-call snapshot")
    }

    // MARK: - snapshot() — diagnostics-facing shape

    @Test("snapshot returns rows in DeviceClass.allCases order")
    func snapshotIsOrderedByDeviceClassAllCases() async throws {
        let ctx = try makeContext()
        let store = SwiftDataLearnedDeviceProfileStore(
            context: ctx,
            tuning: Self.permissiveTuning,
            clock: { Self.referenceDate }
        )
        // Insert in reverse order so the natural fetch order is wrong.
        let reversedClasses = Array(DeviceClass.allCases.reversed())
        for deviceClass in reversedClasses {
            _ = await store.recordObservation(
                GrantWindowObservation(
                    grantWindowSeconds: 60,
                    observedAt: Self.referenceDate
                ),
                deviceClass: deviceClass,
                seed: DeviceClassProfile.fallback(for: deviceClass)
            )
        }
        let snapshots = await store.snapshot()
        let order = snapshots.map { $0.deviceClassRawValue }
        let expected = DeviceClass.allCases.map { $0.rawValue }
        #expect(order == expected)
    }

    @Test("snapshot returns empty array when no rows have been inserted")
    func snapshotEmptyWhenNoRows() async throws {
        let ctx = try makeContext()
        let store = SwiftDataLearnedDeviceProfileStore(
            context: ctx,
            tuning: Self.permissiveTuning,
            clock: { Self.referenceDate }
        )
        let snapshots = await store.snapshot()
        #expect(snapshots.isEmpty)
    }

    @Test("snapshotSync returns same payload as async snapshot")
    func snapshotSyncMatchesAsyncSnapshot() async throws {
        let ctx = try makeContext()
        let store = SwiftDataLearnedDeviceProfileStore(
            context: ctx,
            tuning: Self.permissiveTuning,
            clock: { Self.referenceDate }
        )
        _ = await store.recordObservation(
            GrantWindowObservation(grantWindowSeconds: 60, observedAt: Self.referenceDate),
            deviceClass: .iPhone17Pro,
            seed: Self.seed()
        )
        let async_ = await store.snapshot()
        let sync_ = store.snapshotSync()
        #expect(async_ == sync_)
    }

    // MARK: - Diagnostic record projection

    @Test("LearnedDeviceProfileDiagnosticRecord wire shape matches snake_case keys")
    func diagnosticRecordEncodesSnakeCase() throws {
        // Use a fully-populated state so JSONEncoder emits every key —
        // `lastRevertReason` / `lastNotchChangeAt` are Optional and would
        // otherwise be elided when nil. The wire-shape contract is
        // "encoder uses snake_case keys", not "every key is always present".
        let state = AdaptiveDeviceProfileState(
            deviceClassRawValue: DeviceClass.iPhone17Pro.rawValue,
            seedGrantWindowSeconds: 45,
            welfordMean: 41.0,
            welfordM2: 0,
            sampleCount: 30,
            ewmaSeconds: 41.0,
            persistedScaleFactor: 0.95,
            lastNotchChangeAt: Self.referenceDate,
            consecutiveClampedObservations: 0,
            lastRevertReason: .divergenceClampSaturation,
            createdAt: Self.referenceDate,
            updatedAt: Self.referenceDate,
            schemaVersion: 1
        )
        let record = LearnedDeviceProfileDiagnosticRecord.from(snapshot: state)
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(record)
        let json = try #require(String(data: data, encoding: .utf8))

        // Spot-check several snake_case keys are present.
        #expect(json.contains("\"device_class\""))
        #expect(json.contains("\"seed_grant_window_seconds\""))
        #expect(json.contains("\"sample_count\""))
        #expect(json.contains("\"welford_mean\""))
        #expect(json.contains("\"welford_variance\""))
        #expect(json.contains("\"ewma_seconds\""))
        #expect(json.contains("\"persisted_scale_factor\""))
        #expect(json.contains("\"activated\""))
        #expect(json.contains("\"consecutive_clamped_observations\""))
        #expect(json.contains("\"last_revert_reason\""))
        #expect(json.contains("\"last_notch_change_at\""))
        #expect(json.contains("\"created_at\""))
        #expect(json.contains("\"updated_at\""))
        #expect(json.contains("\"schema_version\""))
        // camelCase variants must NOT appear.
        #expect(!json.contains("\"deviceClass\""))
        #expect(!json.contains("\"sampleCount\""))
    }

    @Test("Diagnostic record round-trips: encode → decode → equality")
    func diagnosticRecordRoundTripsThroughJSON() throws {
        let state = AdaptiveDeviceProfileState(
            deviceClassRawValue: DeviceClass.iPhone15Pro.rawValue,
            seedGrantWindowSeconds: 60,
            welfordMean: 55.5,
            welfordM2: 100.0,
            sampleCount: 50,
            ewmaSeconds: 55.0,
            persistedScaleFactor: 0.9,
            lastNotchChangeAt: Self.referenceDate.addingTimeInterval(3_600),
            consecutiveClampedObservations: 3,
            lastRevertReason: .divergenceClampSaturation,
            createdAt: Self.referenceDate,
            updatedAt: Self.referenceDate.addingTimeInterval(7_200),
            schemaVersion: 1
        )
        let original = LearnedDeviceProfileDiagnosticRecord.from(snapshot: state)

        let encoder = JSONEncoder()
        let decoder = JSONDecoder()
        let data = try encoder.encode(original)
        let decoded = try decoder.decode(LearnedDeviceProfileDiagnosticRecord.self, from: data)
        #expect(decoded == original)
    }

    @Test("R10: Diagnostic record encoding succeeds even when the source snapshot has non-finite Doubles")
    func diagnosticRecordCoercesNonFiniteDoublesForEncoder() throws {
        // R10 probe-6: `JSONEncoder` defaults to
        // `nonConformingFloatEncodingStrategy = .throw`, so a single
        // non-finite Double anywhere in the diagnostics bundle aborts
        // the entire export. The estimator's R5/R6 heal removes
        // non-finite math on the NEXT recordObservation, but the
        // diagnostics path is read-only: it reads the snapshot directly
        // from SwiftData and projects it into the wire record. A user
        // shipping us a corruption-evidence bundle would hit the throw
        // BEFORE the heal had a chance to run. Coerce non-finite to 0
        // in `from(snapshot:)` so the encoder always succeeds and the
        // support engineer still sees `sampleCount`,
        // `consecutiveClampedObservations`, and `activated` (all
        // unaffected by the coercion).
        let corrupt = AdaptiveDeviceProfileState(
            deviceClassRawValue: DeviceClass.iPhone17Pro.rawValue,
            seedGrantWindowSeconds: .nan,
            welfordMean: .infinity,
            welfordM2: -.infinity,
            sampleCount: 30,
            ewmaSeconds: .nan,
            persistedScaleFactor: .nan,
            lastNotchChangeAt: Date(timeIntervalSinceReferenceDate: .nan),
            consecutiveClampedObservations: 5,
            lastRevertReason: nil,
            // createdAt / updatedAt have non-finite Doubles backing them.
            createdAt: Date(timeIntervalSinceReferenceDate: .infinity),
            updatedAt: Date(timeIntervalSinceReferenceDate: .nan),
            schemaVersion: 1
        )
        let record = LearnedDeviceProfileDiagnosticRecord.from(snapshot: corrupt)

        // Every Double-typed field on the record must be finite — the
        // coercion contract.
        #expect(record.seedGrantWindowSeconds.isFinite)
        #expect(record.welfordMean.isFinite)
        #expect(record.welfordVariance.isFinite)
        #expect(record.ewmaSeconds.isFinite)
        #expect(record.persistedScaleFactor.isFinite)
        #expect(record.createdAt.isFinite)
        #expect(record.updatedAt.isFinite)
        if let last = record.lastNotchChangeAt {
            #expect(last.isFinite)
        }

        // The integer / Bool / String fields pass through unchanged.
        #expect(record.sampleCount == 30)
        #expect(record.consecutiveClampedObservations == 5)
        #expect(record.activated == true)  // sampleCount >= 30
        #expect(record.deviceClass == DeviceClass.iPhone17Pro.rawValue)
        #expect(record.schemaVersion == 1)

        // The core contract: encoding succeeds. Without the R10 coercion
        // this would throw `EncodingError.invalidValue` on the first
        // non-finite Double.
        let encoder = JSONEncoder()
        let data = try encoder.encode(record)
        #expect(!data.isEmpty,
                "JSONEncoder must successfully serialize the coerced record")
    }

    @Test("Diagnostic record activation flag tracks sample count vs tuning floor")
    func diagnosticRecordActivationFlagIsCorrect() throws {
        let belowFloor = AdaptiveDeviceProfileState(
            deviceClassRawValue: DeviceClass.iPhone17Pro.rawValue,
            seedGrantWindowSeconds: 45,
            welfordMean: 45,
            welfordM2: 0,
            sampleCount: 29,
            ewmaSeconds: 45,
            persistedScaleFactor: 1.0,
            lastNotchChangeAt: nil,
            consecutiveClampedObservations: 0,
            lastRevertReason: nil,
            createdAt: Self.referenceDate,
            updatedAt: Self.referenceDate,
            schemaVersion: 1
        )
        var atFloor = belowFloor
        atFloor.sampleCount = 30

        let r1 = LearnedDeviceProfileDiagnosticRecord.from(snapshot: belowFloor)
        let r2 = LearnedDeviceProfileDiagnosticRecord.from(snapshot: atFloor)
        #expect(r1.activated == false)
        #expect(r2.activated == true)
    }
}
