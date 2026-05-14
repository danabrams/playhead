// AdaptiveDeviceProfileWriteSeamTests.swift
// playhead-beh3 (Phase 3 deliverable 5, R2) — write-seam contract tests
// for `AnalysisWorkScheduler.recordGrantWindowObservationIfEnabled(...)`.
//
// R1 closed the static-shape + persistence gaps but left ONE explicit
// residual: the scheduler READ the estimator via `resolvedDeviceProfile`
// yet no production code path called `recordObservation`. This file
// pins the R2 wire-up at the helper that the four success outcome arms
// in `processJob` (`tierAdvance`, `allTiersDone`,
// `coverageInsufficient.{noProgress,maxAttempts}`) now funnel through.
//
// The success arms themselves are not driven through `processJob` in
// stub form (see `AnalysisWorkSchedulerJournalEmissionTests.swift`
// header for the reasoning — the success path needs the full
// decode/feature/transcript/ad-detection/cue pipeline end-to-end). We
// instead exercise the helper directly, which is the same method the
// success arms call; the call-graph from arm → helper is preserved by
// the static `recordGrantWindowObservationIfEnabled(...)` references
// in `processJob`.
//
// Coverage:
//   (1) Flag OFF: helper makes zero `recordObservation` calls. The
//       provider is never touched. Proves the byte-identical rollback
//       contract at the write seam, mirroring the read-path proof in
//       `AdaptiveDeviceProfileFlagOffTests`.
//   (2) Flag ON: helper makes exactly one `recordObservation` call per
//       invocation, threading the device class + a positive grant-
//       window duration through to the provider.
//   (3) Flag ON, clock-skew defense: a non-positive duration (clock
//       moved backward, simulated zero elapsed) drops the observation
//       at the helper rather than reaching the estimator.
//   (4) Static call-site discoverability: the four success arms in
//       `processJob` must reference `recordGrantWindowObservationIfEnabled`
//       so a future refactor that drops the wire-up regresses the
//       source-grep audit. (Compile-time guarantee — see assertion.)

import Foundation
import Testing

@testable import Playhead

@Suite("AdaptiveDeviceProfile write seam (playhead-beh3 R2)")
struct AdaptiveDeviceProfileWriteSeamTests {

    // MARK: - Recording provider

    /// `LearnedDeviceProfileProviding` stub that captures every
    /// `recordObservation` call so the test can assert exact wire-up
    /// shape: count, device class, observation duration. `resolvedDeviceProfile`
    /// is unused in this suite; the read-path tests live in
    /// `AdaptiveDeviceProfileFlagOffTests`.
    actor RecordingProvider: LearnedDeviceProfileProviding {
        struct Recorded: Sendable, Equatable {
            let deviceClass: DeviceClass
            let grantWindowSeconds: Double
        }
        private(set) var recorded: [Recorded] = []
        private(set) var resolveCount: Int = 0

        func resolvedDeviceProfile(
            seed: DeviceClassProfile,
            deviceClass: DeviceClass
        ) async -> DeviceClassProfile {
            resolveCount += 1
            return seed
        }

        @discardableResult
        func recordObservation(
            _ observation: GrantWindowObservation,
            deviceClass: DeviceClass,
            seed: DeviceClassProfile
        ) async -> AdaptiveDeviceProfileApplyResult {
            recorded.append(
                Recorded(
                    deviceClass: deviceClass,
                    grantWindowSeconds: observation.grantWindowSeconds
                )
            )
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

    // MARK: - Plentiful storage snapshotter

    private struct PlentifulSnapshotter: StorageBudgetSnapshotting {
        func canAdmit(_ cls: ArtifactClass, bytes: Int64) async -> Bool { true }
        func remainingBytes(_ cls: ArtifactClass) async -> Int64 {
            5_000_000_000
        }
    }

    // MARK: - Scheduler factory

    /// Build a scheduler bound to a deterministic clock + injected
    /// provider so the test can synthesize a lease-acquired timestamp
    /// at a known offset and assert the resulting observation's
    /// duration matches.
    @MainActor
    private func makeScheduler(
        store: AnalysisStore,
        config: PreAnalysisConfig,
        provider: any LearnedDeviceProfileProviding,
        clock: @escaping @Sendable () -> Date
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
            clock: clock,
            learnedDeviceProfileProvider: provider
        )
    }

    // MARK: - (1) Flag OFF: zero writes

    @Test("Flag OFF: recordGrantWindowObservationIfEnabled performs zero recordObservation calls")
    @MainActor
    func flagOffMakesZeroRecordCalls() async throws {
        let store = try await makeTestStore()
        let recorder = RecordingProvider()
        var config = PreAnalysisConfig()
        config.useAdaptiveDeviceProfile = false

        let leaseAcquiredAt: TimeInterval = 1_700_000_000  // any fixed epoch
        // Clock advances by 60s so the helper's positive-duration guard
        // would NOT short-circuit if we accidentally entered it.
        let clock: @Sendable () -> Date = {
            Date(timeIntervalSince1970: leaseAcquiredAt + 60)
        }
        let scheduler = makeScheduler(
            store: store, config: config, provider: recorder, clock: clock
        )

        // Drive the helper multiple times to amplify any leak.
        for _ in 0..<5 {
            await scheduler.recordGrantWindowObservationIfEnabled(
                leaseAcquiredAt: leaseAcquiredAt
            )
        }

        let recorded = await recorder.recorded
        #expect(recorded.isEmpty,
                "flag OFF must not record any observations; recorded=\(recorded.count)")
        let resolveCount = await recorder.resolveCount
        #expect(resolveCount == 0,
                "helper must not consult `resolvedDeviceProfile` either (called \(resolveCount))")
    }

    // MARK: - (2) Flag ON: one record per invocation, correct shape

    @Test("Flag ON: recordGrantWindowObservationIfEnabled records exactly one observation with the detected device class + computed duration")
    @MainActor
    func flagOnRecordsOneObservation() async throws {
        let store = try await makeTestStore()
        let recorder = RecordingProvider()
        var config = PreAnalysisConfig()
        config.useAdaptiveDeviceProfile = true

        let leaseAcquiredAt: TimeInterval = 1_700_000_000
        let elapsed: TimeInterval = 47  // arbitrary positive duration
        let clock: @Sendable () -> Date = {
            Date(timeIntervalSince1970: leaseAcquiredAt + elapsed)
        }
        let scheduler = makeScheduler(
            store: store, config: config, provider: recorder, clock: clock
        )

        await scheduler.recordGrantWindowObservationIfEnabled(
            leaseAcquiredAt: leaseAcquiredAt
        )

        let recorded = await recorder.recorded
        #expect(recorded.count == 1,
                "flag ON must record exactly one observation per invocation; got \(recorded.count)")
        guard let first = recorded.first else {
            Issue.record("missing recorded observation under flag ON")
            return
        }
        // The helper detects device class from `utsname.machine`. In the
        // simulator that maps to a stable bucket; we don't pin the
        // specific case (simulator host can vary) — only that one of the
        // declared `DeviceClass` cases is reported back.
        #expect(DeviceClass.allCases.contains(first.deviceClass),
                "detected device class must be a declared DeviceClass case; got \(first.deviceClass)")
        // Allow a vanishing rounding tolerance; the helper uses
        // `timeIntervalSince1970` arithmetic so the value is exact for
        // a fixed-clock test, but the comparison is in Doubles so a
        // strict `==` is still safe here.
        #expect(abs(first.grantWindowSeconds - elapsed) < 1e-9,
                "observed duration must equal `clock - leaseAcquiredAt`; got \(first.grantWindowSeconds)")
    }

    @Test("Flag ON: three invocations produce three observations all bucketed to the detected device class")
    @MainActor
    func flagOnMultipleInvocations() async throws {
        let store = try await makeTestStore()
        let recorder = RecordingProvider()
        var config = PreAnalysisConfig()
        config.useAdaptiveDeviceProfile = true

        let leaseAcquiredAt: TimeInterval = 1_700_000_000
        let clock: @Sendable () -> Date = {
            Date(timeIntervalSince1970: leaseAcquiredAt + 30)
        }
        let scheduler = makeScheduler(
            store: store, config: config, provider: recorder, clock: clock
        )

        for _ in 0..<3 {
            await scheduler.recordGrantWindowObservationIfEnabled(
                leaseAcquiredAt: leaseAcquiredAt
            )
        }

        let recorded = await recorder.recorded
        #expect(recorded.count == 3)
        // All three observations share the same detected device class —
        // `DeviceClass.detect()` is a pure mapping over `utsname.machine`
        // so a single run reports a single class across all calls.
        let classes = Set(recorded.map(\.deviceClass))
        #expect(classes.count == 1,
                "all observations in one run must share one device class; got \(classes)")
    }

    // MARK: - (3) Flag ON, non-positive duration is dropped at the helper

    @Test("Flag ON, zero elapsed: helper drops the observation (clock-skew defense)")
    @MainActor
    func flagOnZeroDurationIsDropped() async throws {
        let store = try await makeTestStore()
        let recorder = RecordingProvider()
        var config = PreAnalysisConfig()
        config.useAdaptiveDeviceProfile = true

        let leaseAcquiredAt: TimeInterval = 1_700_000_000
        // Clock equals leaseAcquiredAt → elapsed = 0.
        let clock: @Sendable () -> Date = {
            Date(timeIntervalSince1970: leaseAcquiredAt)
        }
        let scheduler = makeScheduler(
            store: store, config: config, provider: recorder, clock: clock
        )

        await scheduler.recordGrantWindowObservationIfEnabled(
            leaseAcquiredAt: leaseAcquiredAt
        )

        let recorded = await recorder.recorded
        #expect(recorded.isEmpty,
                "non-positive elapsed must be dropped before reaching the provider")
    }

    @Test("Flag ON, negative elapsed (clock stepped backward): helper drops the observation")
    @MainActor
    func flagOnNegativeDurationIsDropped() async throws {
        let store = try await makeTestStore()
        let recorder = RecordingProvider()
        var config = PreAnalysisConfig()
        config.useAdaptiveDeviceProfile = true

        let leaseAcquiredAt: TimeInterval = 1_700_000_000
        // Clock stepped 5 s BEFORE lease acquisition → elapsed < 0.
        let clock: @Sendable () -> Date = {
            Date(timeIntervalSince1970: leaseAcquiredAt - 5)
        }
        let scheduler = makeScheduler(
            store: store, config: config, provider: recorder, clock: clock
        )

        await scheduler.recordGrantWindowObservationIfEnabled(
            leaseAcquiredAt: leaseAcquiredAt
        )

        let recorded = await recorder.recorded
        #expect(recorded.isEmpty,
                "negative elapsed (NTP step back) must be dropped at the helper")
    }

    // MARK: - (4) Static call-site canary
    //
    // R3: the file header (section (4)) promises a "compile-time
    // guarantee" that the four success outcome arms in
    // `AnalysisWorkScheduler.processJob` reference
    // `recordGrantWindowObservationIfEnabled`. R2 wired the call sites
    // and added unit tests that drive the helper directly, but never
    // pinned the arm→helper edge. Without this canary a future refactor
    // could silently drop one of the four `await` lines and every
    // unit test would still pass.
    //
    // The canary scans `AnalysisWorkScheduler.swift` (the source file
    // owning `processJob`) and asserts:
    //   * the helper is defined exactly once, and
    //   * the four success outcome arms each have at least one
    //     call-site reference to it, demonstrated by a >=5 reference
    //     count (4 success arms + 1 definition = 5 minimum).
    //
    // We use the same source-canary pattern established by
    // `DebugDiagnosticsHatchSourceCanaryTests`. The test is in this
    // file (rather than a parallel canary file) because the contract
    // it pins is the same one this file documents.

    private static let repoRoot: URL = {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent() // .../PreAnalysis/
            .deletingLastPathComponent() // .../Services/
            .deletingLastPathComponent() // .../PlayheadTests/
            .deletingLastPathComponent() // .../<repo root>/
    }()

    @Test("Source canary: AnalysisWorkScheduler.processJob references recordGrantWindowObservationIfEnabled from all four success arms")
    func sourceCanaryAllFourSuccessArmsReferenceHelper() throws {
        let url = Self.repoRoot.appendingPathComponent(
            "Playhead/Services/PreAnalysis/AnalysisWorkScheduler.swift"
        )
        let source = try String(contentsOf: url, encoding: .utf8)

        // The helper definition must be present exactly once. Look for
        // the `func recordGrantWindowObservationIfEnabled(` signature.
        let definitionToken = "func recordGrantWindowObservationIfEnabled("
        let definitionCount = source.components(separatedBy: definitionToken).count - 1
        #expect(definitionCount == 1,
                "expected exactly one helper definition, got \(definitionCount)")

        // Total occurrences of the symbol = 1 definition + N call sites.
        // The R2 wiring places one `await` reference in each of the four
        // success arms (`tierAdvance`, `allTiersDone`,
        // `coverageInsufficient.noProgress`, `coverageInsufficient.maxAttempts`).
        // Counting raw symbol references is more refactor-robust than
        // requiring a specific surrounding shape: a future refactor that
        // routes the helper through a wrapper would still register on
        // this canary as long as the call edges remain.
        let totalReferences = source.components(separatedBy: "recordGrantWindowObservationIfEnabled").count - 1
        #expect(totalReferences >= 5,
                "expected ≥5 references (1 definition + 4 success-arm call sites); got \(totalReferences)")

        // Spot-check each success arm by name — the call must appear
        // inside the same source file as those arm labels. We assert the
        // arm tag string is present (`"tierAdvance"` etc. — these are
        // the `commitOutcomeArm` labels the production code passes to
        // the journal), and that at least one helper reference follows
        // within a generous window of source characters.
        let armTags = [
            "\"tierAdvance\"",
            "\"allTiersDone\"",
            "\"coverageInsufficient.noProgress\"",
            "\"coverageInsufficient.maxAttempts\"",
        ]
        for tag in armTags {
            guard let tagRange = source.range(of: tag) else {
                Issue.record("success arm tag \(tag) missing from AnalysisWorkScheduler.swift")
                continue
            }
            // The helper invocation appears within the success branch
            // of each arm. The arms are short (the helper call sits ≤
            // 30 lines after the tag in every case), so a 4_000-char
            // window comfortably covers each one without crossing into
            // the next arm.
            let windowEnd = source.index(tagRange.upperBound,
                                         offsetBy: 4_000,
                                         limitedBy: source.endIndex) ?? source.endIndex
            let window = source[tagRange.upperBound..<windowEnd]
            #expect(window.contains("recordGrantWindowObservationIfEnabled("),
                    "expected helper invocation within the \(tag) arm body")
        }
    }
}
