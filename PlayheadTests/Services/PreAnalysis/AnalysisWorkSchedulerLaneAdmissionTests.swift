// AnalysisWorkSchedulerLaneAdmissionTests.swift
// Verifies that AnalysisWorkScheduler routes its thermal/battery/low-power
// gating through QualityProfile and applies per-variant lane policy
// correctly. playhead-5ih.

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — QualityProfile lane admission")
struct AnalysisWorkSchedulerLaneAdmissionTests {

    // MARK: - Helpers

    private func makeScheduler(
        store: AnalysisStore,
        capabilities: any CapabilitiesProviding,
        battery: StubBatteryProvider,
        config: PreAnalysisConfig = PreAnalysisConfig()
    ) -> AnalysisWorkScheduler {
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider(),
            cueMaterializer: SkipCueMaterializer(store: store)
        )
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: capabilities,
            downloadManager: StubDownloadProvider(),
            batteryProvider: battery,
            config: config
        )
    }

    // MARK: - QualityProfile derivation via scheduler

    @Test("nominal device -> nominal QualityProfile, all lanes allowed")
    func testNominalAdmission() async throws {
        let store = try await makeTestStore()
        let capabilities = StubCapabilitiesProvider(
            snapshot: makeCapabilitySnapshot(
                thermalState: .nominal,
                isLowPowerMode: false,
                isCharging: true
            )
        )
        let battery = StubBatteryProvider()
        battery.level = 0.85
        battery.charging = true

        let scheduler = makeScheduler(store: store, capabilities: capabilities, battery: battery)
        let admission = await scheduler.currentLaneAdmission()

        #expect(admission.qualityProfile == .nominal)
        #expect(admission.policy.allowSoonLane == true)
        #expect(admission.policy.allowBackgroundLane == true)
        #expect(admission.pauseAllWork == false)
    }

    @Test("fair thermal -> fair QualityProfile, Background lane paused")
    func testFairAdmission() async throws {
        let store = try await makeTestStore()
        let capabilities = StubCapabilitiesProvider(
            snapshot: makeCapabilitySnapshot(
                thermalState: .fair,
                isLowPowerMode: false,
                isCharging: true
            )
        )
        let battery = StubBatteryProvider()
        battery.level = 0.85
        battery.charging = true

        let scheduler = makeScheduler(store: store, capabilities: capabilities, battery: battery)
        let admission = await scheduler.currentLaneAdmission()

        #expect(admission.qualityProfile == .fair)
        #expect(admission.policy.allowSoonLane == true)
        #expect(admission.policy.allowBackgroundLane == false)
        #expect(admission.pauseAllWork == false)
    }

    @Test("serious thermal -> serious QualityProfile, Soon + Background paused")
    func testSeriousAdmission() async throws {
        let store = try await makeTestStore()
        let capabilities = StubCapabilitiesProvider(
            snapshot: makeCapabilitySnapshot(
                thermalState: .serious,
                isLowPowerMode: false,
                isCharging: true
            )
        )
        let battery = StubBatteryProvider()
        battery.level = 0.85
        battery.charging = true

        let scheduler = makeScheduler(store: store, capabilities: capabilities, battery: battery)
        let admission = await scheduler.currentLaneAdmission()

        #expect(admission.qualityProfile == .serious)
        #expect(admission.policy.allowSoonLane == false)
        #expect(admission.policy.allowBackgroundLane == false)
        #expect(admission.pauseAllWork == false)
    }

    @Test("critical thermal -> critical QualityProfile, pauseAllWork")
    func testCriticalAdmission() async throws {
        let store = try await makeTestStore()
        let capabilities = StubCapabilitiesProvider(
            snapshot: makeCapabilitySnapshot(
                thermalState: .critical,
                isLowPowerMode: false,
                isCharging: true
            )
        )
        let battery = StubBatteryProvider()
        battery.level = 0.85
        battery.charging = true

        let scheduler = makeScheduler(store: store, capabilities: capabilities, battery: battery)
        let admission = await scheduler.currentLaneAdmission()

        #expect(admission.qualityProfile == .critical)
        #expect(admission.pauseAllWork == true)
    }

    @Test("low-power-mode demotes nominal baseline to fair")
    func testLowPowerDemotesNominal() async throws {
        let store = try await makeTestStore()
        let capabilities = StubCapabilitiesProvider(
            snapshot: makeCapabilitySnapshot(
                thermalState: .nominal,
                isLowPowerMode: true,
                isCharging: true
            )
        )
        let battery = StubBatteryProvider()
        battery.level = 0.85
        battery.charging = true

        let scheduler = makeScheduler(store: store, capabilities: capabilities, battery: battery)
        let admission = await scheduler.currentLaneAdmission()

        #expect(admission.qualityProfile == .fair,
                "Low-power-mode should demote nominal to fair")
    }

    @Test("low battery unplugged demotes nominal baseline to fair")
    func testLowBatteryUnpluggedDemotes() async throws {
        let store = try await makeTestStore()
        let capabilities = StubCapabilitiesProvider(
            snapshot: makeCapabilitySnapshot(
                thermalState: .nominal,
                isLowPowerMode: false,
                isCharging: false
            )
        )
        let battery = StubBatteryProvider()
        battery.level = 0.15
        battery.charging = false

        let scheduler = makeScheduler(store: store, capabilities: capabilities, battery: battery)
        let admission = await scheduler.currentLaneAdmission()

        #expect(admission.qualityProfile == .fair,
                "Low battery (<20%) while unplugged should demote nominal to fair")
    }

    // MARK: - LaneAdmission policy checks

    @Test("LaneAdmission.allowsDeferredJob: nominal allows Soon + Background")
    func testLaneAdmissionNominalAllowsAll() {
        let admission = AnalysisWorkScheduler.LaneAdmission(
            qualityProfile: .nominal,
            policy: QualityProfile.nominal.schedulerPolicy
        )
        #expect(admission.allowsDeferredJob(desiredCoverageSec: 300, t2Threshold: 900))
        #expect(admission.allowsDeferredJob(desiredCoverageSec: 900, t2Threshold: 900))
        #expect(admission.allowsDeferredJob(desiredCoverageSec: 1800, t2Threshold: 900))
    }

    @Test("LaneAdmission.allowsDeferredJob: fair allows Soon but blocks Background")
    func testLaneAdmissionFairBlocksBackground() {
        let admission = AnalysisWorkScheduler.LaneAdmission(
            qualityProfile: .fair,
            policy: QualityProfile.fair.schedulerPolicy
        )
        // Soon lane (coverage < t2Threshold)
        #expect(admission.allowsDeferredJob(desiredCoverageSec: 300, t2Threshold: 900))
        // Background lane (coverage >= t2Threshold)
        #expect(!admission.allowsDeferredJob(desiredCoverageSec: 900, t2Threshold: 900))
        #expect(!admission.allowsDeferredJob(desiredCoverageSec: 1800, t2Threshold: 900))
    }

    @Test("LaneAdmission.allowsDeferredJob: serious blocks Soon and Background")
    func testLaneAdmissionSeriousBlocksDeferred() {
        let admission = AnalysisWorkScheduler.LaneAdmission(
            qualityProfile: .serious,
            policy: QualityProfile.serious.schedulerPolicy
        )
        #expect(!admission.allowsDeferredJob(desiredCoverageSec: 300, t2Threshold: 900))
        #expect(!admission.allowsDeferredJob(desiredCoverageSec: 900, t2Threshold: 900))
    }

    @Test("LaneAdmission.allowsDeferredJob: critical blocks all deferred")
    func testLaneAdmissionCriticalBlocksDeferred() {
        let admission = AnalysisWorkScheduler.LaneAdmission(
            qualityProfile: .critical,
            policy: QualityProfile.critical.schedulerPolicy
        )
        #expect(!admission.allowsDeferredJob(desiredCoverageSec: 300, t2Threshold: 900))
        #expect(!admission.allowsDeferredJob(desiredCoverageSec: 900, t2Threshold: 900))
    }

    // MARK: - AdmissionGate integration (playhead-bnrs)

    /// Stub transport provider that lets tests drive the scheduler's
    /// admission gate into `.wifiRequired` without standing up a real
    /// `NWPathMonitor`. Defaults mirror `WifiTransportStatusProvider`
    /// (the production fallback) so individual tests can override only
    /// the axes they care about.
    private struct StubTransportStatusProvider: TransportStatusProviding {
        let reachability: TransportSnapshot.Reachability
        let allowsCellular: Bool

        init(
            reachability: TransportSnapshot.Reachability = .wifi,
            allowsCellular: Bool = true
        ) {
            self.reachability = reachability
            self.allowsCellular = allowsCellular
        }

        func currentReachability() async -> TransportSnapshot.Reachability {
            reachability
        }
        func userAllowsCellular() async -> Bool { allowsCellular }
    }

    @Test("evaluateAdmissionGate rejects a background (maintenance) job on cellular with .wifiRequired")
    func testAdmissionGateRejectsMaintenanceOnCellular() async throws {
        // Nominal thermal + charging so the thermal axis admits; the
        // rejection must come from the transport gate.
        let store = try await makeTestStore()
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

        let transport = StubTransportStatusProvider(
            reachability: .cellular,
            // Even with `allowsCellular == true`, maintenance sessions
            // are Wi-Fi-only by spec — this is the strongest signal that
            // the gate is actually consulted at the scheduler surface.
            allowsCellular: true
        )

        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            adDetection: StubAdDetectionProvider(),
            cueMaterializer: SkipCueMaterializer(store: store)
        )
        let scheduler = AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: capabilities,
            downloadManager: StubDownloadProvider(),
            batteryProvider: battery,
            transportStatusProvider: transport
        )

        // priority = 0 => .background lane => scheduler synthesizes
        // session == .maintenance.
        let job = makeAnalysisJob(priority: 0)
        let decision = await scheduler.evaluateAdmissionGate(for: job)

        switch decision {
        case .reject(let cause):
            #expect(cause == .wifiRequired,
                    "Maintenance job on cellular must reject with .wifiRequired, got \(cause)")
        case .admit(let sliceBytes):
            Issue.record("Expected .reject(.wifiRequired), got .admit(sliceBytes: \(sliceBytes))")
        }
    }
}
