// AdaptiveDeviceProfileFlagOffTests.swift
// playhead-beh3 (Phase 3 deliverable 5) — flag-off byte-identity tests
// for the adaptive Welford+EWMA estimator integration in
// `AnalysisWorkScheduler`.
//
// Acceptance criterion from the bead spec:
//   "Flag-off byte-identical to today's behavior."
//
// We prove this two ways:
//
//   (1) The no-op provider always returns the seed verbatim and never
//       persists state. Construction is free and the path is pure.
//
//   (2) With `PreAnalysisConfig.useAdaptiveDeviceProfile == false` the
//       scheduler MUST NOT consult the injected provider at all. We
//       prove this by injecting a recording provider that throws an
//       `Issue.record` on any access (`resolvedDeviceProfile` or
//       `recordObservation`), then invoking `evaluateAdmissionGate` and
//       confirming the recorder reports zero calls. The seed-only
//       admission decision is exercised end-to-end and asserted to
//       admit (since every other axis is plentiful in the fixture).
//
//   (3) With the flag ON the scheduler MUST consult the provider on
//       every admission pass. We inject the same recorder and confirm
//       a non-zero `resolvedDeviceProfile` call count.
//
// Together (1)+(2) cover the flag-off byte-identity rollback contract;
// (3) is the positive-control that the seam is genuinely live behind
// the flag — without this, (2) could pass vacuously if the scheduler
// just never reached the provider call site.

import Foundation
import Testing

@testable import Playhead

@Suite("AdaptiveDeviceProfile flag-off byte-identity (playhead-beh3)")
struct AdaptiveDeviceProfileFlagOffTests {

    // MARK: - Recording provider

    /// LearnedDeviceProfileProviding stub that records every call so the
    /// test can assert the scheduler did (or did not) consult it.
    /// All calls return the seed verbatim — the stub never alters the
    /// caller's view of the world, only its own counters.
    actor RecordingProvider: LearnedDeviceProfileProviding {
        private(set) var resolveCount: Int = 0
        private(set) var recordCount: Int = 0
        private(set) var lastResolvedDeviceClass: DeviceClass?

        func resolvedDeviceProfile(
            seed: DeviceClassProfile,
            deviceClass: DeviceClass
        ) async -> DeviceClassProfile {
            resolveCount += 1
            lastResolvedDeviceClass = deviceClass
            return seed
        }

        @discardableResult
        func recordObservation(
            _ observation: GrantWindowObservation,
            deviceClass: DeviceClass,
            seed: DeviceClassProfile
        ) async -> AdaptiveDeviceProfileApplyResult {
            recordCount += 1
            return AdaptiveDeviceProfileApplyResult(
                persistedScaleFactorChanged: false,
                didRevertToSeed: false,
                blockedByNotchRateLimit: false,
                clampSaturatedThisObservation: false
            )
        }

        func snapshot() async -> [AdaptiveDeviceProfileState] {
            []
        }
    }

    // MARK: - Storage snapshotter (admits every class)

    /// All-admit `StorageBudgetSnapshotting` so the storage axis cannot
    /// be the binding rejection cause; the admission outcome is then
    /// purely a function of the device-profile slice math, which is
    /// what we want to observe.
    private struct PlentifulSnapshotter: StorageBudgetSnapshotting {
        func canAdmit(_ cls: ArtifactClass, bytes: Int64) async -> Bool { true }
        func remainingBytes(_ cls: ArtifactClass) async -> Int64 {
            5_000_000_000
        }
    }

    // MARK: - Scheduler factory

    /// Build a scheduler with the supplied feature-flag config + provider.
    @MainActor
    private func makeScheduler(
        store: AnalysisStore,
        config: PreAnalysisConfig,
        provider: any LearnedDeviceProfileProviding
    ) -> AnalysisWorkScheduler {
        let capabilities = StubCapabilitiesProvider(
            snapshot: makeCapabilitySnapshot(
                thermalState: .nominal,
                isLowPowerMode: false,
                isCharging: true
            )
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true

        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider()
        )
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: capabilities,
            downloadManager: StubDownloadProvider(),
            batteryProvider: battery,
            transportStatusProvider: StubTransportStatusProvider(),
            storageBudgetSnapshotter: PlentifulSnapshotter(),
            config: config,
            learnedDeviceProfileProvider: provider
        )
    }

    // MARK: - (1) No-op provider unit test

    @Test("NoOpLearnedDeviceProfileProvider.resolvedDeviceProfile returns the seed verbatim across every DeviceClass case")
    func noOpReturnsSeedVerbatim() async {
        let provider = NoOpLearnedDeviceProfileProvider()
        for deviceClass in DeviceClass.allCases {
            let seed = DeviceClassProfile.fallback(for: deviceClass)
            let resolved = await provider.resolvedDeviceProfile(
                seed: seed,
                deviceClass: deviceClass
            )
            #expect(resolved == seed,
                    "no-op provider must echo the seed for \(deviceClass.rawValue)")
        }
    }

    @Test("NoOpLearnedDeviceProfileProvider.recordObservation reports inert ApplyResult flags")
    func noOpRecordReturnsInertResult() async {
        let provider = NoOpLearnedDeviceProfileProvider()
        let result = await provider.recordObservation(
            GrantWindowObservation(grantWindowSeconds: 60, observedAt: Date()),
            deviceClass: .iPhone17Pro,
            seed: DeviceClassProfile.fallback(for: .iPhone17Pro)
        )
        #expect(result.persistedScaleFactorChanged == false)
        #expect(result.didRevertToSeed == false)
        #expect(result.blockedByNotchRateLimit == false)
        #expect(result.clampSaturatedThisObservation == false)
    }

    @Test("NoOpLearnedDeviceProfileProvider.snapshot returns empty array")
    func noOpSnapshotIsEmpty() async {
        let provider = NoOpLearnedDeviceProfileProvider()
        let snapshots = await provider.snapshot()
        #expect(snapshots.isEmpty)
    }

    // MARK: - (2) Flag-OFF: scheduler must never consult the provider

    @Test("Flag OFF: scheduler does not consult the learned-profile provider on evaluateAdmissionGate")
    @MainActor
    func flagOffSchedulerSkipsProvider() async throws {
        let store = try await makeTestStore()
        let recorder = RecordingProvider()
        // Explicitly OFF.
        var config = PreAnalysisConfig()
        config.useAdaptiveDeviceProfile = false

        let scheduler = makeScheduler(store: store, config: config, provider: recorder)
        let job = makeAnalysisJob(priority: 10)

        let decision = await scheduler.evaluateAdmissionGate(for: job)

        // Provider must not have been touched.
        let resolveCount = await recorder.resolveCount
        let recordCount = await recorder.recordCount
        #expect(resolveCount == 0,
                "flag OFF must not call resolvedDeviceProfile (called \(resolveCount) times)")
        #expect(recordCount == 0,
                "flag OFF must not call recordObservation (called \(recordCount) times)")

        // Sanity-check: admission still succeeds — flag-off must not
        // perturb the existing decision surface.
        switch decision {
        case .admit:
            break
        case .reject(let cause):
            Issue.record("Flag-OFF scheduler must admit a plentiful-axis job, got reject(\(cause))")
        }
    }

    // MARK: - (3) Flag-ON: positive control — provider IS consulted

    @Test("Flag ON: scheduler consults the learned-profile provider on every evaluateAdmissionGate")
    @MainActor
    func flagOnSchedulerConsultsProvider() async throws {
        let store = try await makeTestStore()
        let recorder = RecordingProvider()
        var config = PreAnalysisConfig()
        config.useAdaptiveDeviceProfile = true

        let scheduler = makeScheduler(store: store, config: config, provider: recorder)
        let job = makeAnalysisJob(priority: 10)

        _ = await scheduler.evaluateAdmissionGate(for: job)
        _ = await scheduler.evaluateAdmissionGate(for: job)
        _ = await scheduler.evaluateAdmissionGate(for: job)

        let resolveCount = await recorder.resolveCount
        #expect(resolveCount == 3,
                "flag ON must call resolvedDeviceProfile per evaluateAdmissionGate pass (got \(resolveCount))")
    }

    // MARK: - (4) Default config has the flag OFF (rollback contract)

    @Test("PreAnalysisConfig() default value of useAdaptiveDeviceProfile is false")
    func defaultConfigHasFlagOff() {
        let config = PreAnalysisConfig()
        #expect(config.useAdaptiveDeviceProfile == false,
                "default config must keep the adaptive estimator opted OUT")
    }

    @Test("PreAnalysisConfig.decode of legacy JSON (missing flag) defaults to false")
    func legacyJSONDecodesFlagAsFalse() throws {
        // JSON shape from a pre-beh3 ship: the new key is absent.
        // The Codable init must default it to false (backward compat).
        let legacyJSON = #"{}"#.data(using: .utf8)!
        let decoder = JSONDecoder()
        let config = try decoder.decode(PreAnalysisConfig.self, from: legacyJSON)
        #expect(config.useAdaptiveDeviceProfile == false)
    }
}
