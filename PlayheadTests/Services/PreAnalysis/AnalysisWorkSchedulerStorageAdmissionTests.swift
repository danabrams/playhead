// AnalysisWorkSchedulerStorageAdmissionTests.swift
// playhead-1iq1: pre-admission storage axis is a live gate.
//
// Until 1iq1, `AnalysisWorkScheduler.evaluateAdmissionGate` synthesized a
// `StorageSnapshot.plentiful` literal and the storage axis never rejected at
// admission time. The post-1iq1 scheduler injects a live
// `StorageBudgetSnapshotting` and synthesizes the snapshot per-pass. These
// tests assert the new wiring:
//
//   1. When the snapshotter says `canAdmit(.media, _) == false`, a media-
//      writing job rejects pre-work with `.mediaCap` (not `.analysisCap`,
//      not `.thermal` — every other axis is admitting in the fixture).
//   2. When the snapshotter admits everything, the scheduler reaches the
//      `.admit` branch (same as the historical plentiful behavior).
//
// Acceptance criterion mapped to test 1: "On a device with low media
// storage, admission rejects a media-writing job pre-work with `.mediaCap`."

import Foundation
import Testing
@testable import Playhead

@Suite("AnalysisWorkScheduler — storage axis pre-admission gate (playhead-1iq1)")
struct AnalysisWorkSchedulerStorageAdmissionTests {

    // MARK: - Stub snapshotter

    /// Forced-decision `StorageBudgetSnapshotting` stub. Returns
    /// `forcedCanAdmit` for the matching class and `true` otherwise; the
    /// remaining-bytes responses default to a generous headroom so they
    /// are never the binding slice constraint.
    private struct StubStorageBudgetSnapshotter: StorageBudgetSnapshotting {
        let denyClass: ArtifactClass?
        let remaining: Int64

        init(denyClass: ArtifactClass? = nil, remaining: Int64 = 5_000_000_000) {
            self.denyClass = denyClass
            self.remaining = remaining
        }

        func canAdmit(_ cls: ArtifactClass, bytes: Int64) async -> Bool {
            return cls != denyClass
        }

        func remainingBytes(_ cls: ArtifactClass) async -> Int64 {
            // Deny-class reports zero remaining so callers that consult
            // both axes see a consistent picture.
            return cls == denyClass ? 0 : remaining
        }
    }

    // skeptical-review-cycle-18 M-1: the formerly-private nested
    // StubTransportStatusProvider (cycle-16 #45 root-cause stub for
    // NWPathMonitor first-update flakiness) was promoted to
    // PlayheadTests/Helpers/Stubs.swift so every scheduler test can
    // pin reachability without copy/pasting the type.

    // MARK: - Helpers

    private func makeScheduler(
        store: AnalysisStore,
        snapshotter: any StorageBudgetSnapshotting
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
            // skeptical-review-cycle-16 #45 root-cause: pin Wi-Fi
            // reachability so the transport axis cannot intermittently
            // reject these tests under parallel-load NWPathMonitor
            // first-update latency.
            transportStatusProvider: StubTransportStatusProvider(),
            storageBudgetSnapshotter: snapshotter
        )
    }

    // MARK: - Tests

    @Test("evaluateAdmissionGate rejects a media-writing job pre-work with .mediaCap when canAdmit(.media) is false")
    func testMediaCapRejectionPreWork() async throws {
        let store = try await makeTestStore()
        let snapshotter = StubStorageBudgetSnapshotter(denyClass: .media)
        let scheduler = makeScheduler(store: store, snapshotter: snapshotter)

        // Default makeAnalysisJob has artifactClass: .media (the
        // AnalysisJob memberwise-init default per playhead-bnrs).
        // estimatedWriteBytes: 1 GB — well past any realistic remaining
        // headroom, so a real budget would also reject; the stub
        // forces `false` regardless.
        let job = makeAnalysisJob(priority: 10)

        let decision = await scheduler.evaluateAdmissionGate(for: job)

        switch decision {
        case .reject(let cause):
            #expect(cause == .mediaCap,
                    "media-class admission with canAdmit(.media)==false must reject with .mediaCap, got \(cause)")
        case .admit(let sliceBytes):
            Issue.record("Expected pre-work .reject(.mediaCap), got .admit(sliceBytes: \(sliceBytes))")
        }
    }

    @Test("evaluateAdmissionGate admits when the snapshotter admits every class (parity with previous plentiful default)")
    func testPlentifulSnapshotterStillAdmits() async throws {
        let store = try await makeTestStore()
        let snapshotter = StubStorageBudgetSnapshotter(denyClass: nil)
        let scheduler = makeScheduler(store: store, snapshotter: snapshotter)

        let job = makeAnalysisJob(priority: 10)
        let decision = await scheduler.evaluateAdmissionGate(for: job)

        switch decision {
        case .admit:
            // Pass — every axis admits in this fixture.
            break
        case .reject(let cause):
            Issue.record("Plentiful snapshotter must admit, got reject(\(cause))")
        }
    }

    // MARK: - Live StorageBudget conformance

    @Test("Live StorageBudget reports canAdmit==false when media size provider exceeds mediaCap")
    func testLiveStorageBudgetRejectsOverCap() async {
        // Drive the live actor's `StorageBudgetSnapshotting` extension
        // through the rejection path with a tiny media cap and a size
        // provider that reports 10x over-cap. The conformance must
        // route through `admit(class:sizeBytes:)` and surface
        // `canAdmit == false`.
        let mediaCap: Int64 = 1_000_000  // 1 MB
        let budget = StorageBudget(
            mediaCap: mediaCap,
            sizeProvider: { cls in
                cls == .media ? 10 * mediaCap : 0
            },
            evictor: { _, target in target }
        )
        let canAdmit = await budget.canAdmit(.media, bytes: 1)
        #expect(!canAdmit,
                "Live StorageBudget must report canAdmit==false when the media class is over cap")

        let remaining = await budget.remainingBytes(.media)
        #expect(remaining == 0,
                "Over-cap media should report 0 remaining bytes, got \(remaining)")
    }

    @Test("Live StorageBudget reports canAdmit==true when class is within cap")
    func testLiveStorageBudgetAdmitsUnderCap() async {
        let budget = StorageBudget(
            sizeProvider: { _ in 0 },
            evictor: { _, target in target }
        )
        let canAdmit = await budget.canAdmit(.media, bytes: 1_000_000)
        #expect(canAdmit,
                "Empty StorageBudget must admit a 1 MB media write")

        let remaining = await budget.remainingBytes(.media)
        #expect(remaining > 0,
                "Empty media class should report positive remaining bytes")
    }
}
