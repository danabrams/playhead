// MetricKitDiagnosticsSubscriber.swift
// MetricKit adapter: receives `MXDiagnosticPayload`s from iOS and hands
// their JSON to `MetricKitDiagnosticProjector`, whose scrubbed output
// lands in `StabilityDiagnosticsStore`.
//
// Scope: playhead-jw63.4 (crash + hang pipeline).
//
// ----- Why MetricKit and not an SDK -----
//
// First-party, already on the device, no dependency, no network, and it
// is the ONLY crash reporter that does not need us to install a signal
// handler or a Mach exception port. Sentry / Crashlytics / Bugsnag all
// do, all ship a network client, and all would need an explicit
// architectural approval under this repo's decision-authority rule.
// MetricKit gives crashes AND main-thread hangs (`MXHangDiagnostic`),
// which is the half a signal-handler crash reporter cannot do at all.
//
// The cost, stated plainly: MetricKit delivers at most once every 24 h,
// on the launch AFTER the incident, and only on real devices. It is a
// next-day pipeline, not a live one.
//
// ----- Why this file is nine lines of logic -----
//
// Everything interesting — the allowlist, the sanitiser, the frame
// walk — lives in `MetricKitDiagnosticProjector`, which is pure and
// testable. `MXDiagnosticPayload` has no public initialiser, so any
// logic placed HERE would be unreachable from the test gate. This file
// therefore does exactly three things: register, take
// `jsonRepresentation()`, forward. `MetricKitDiagnosticsWiringSourceCanaryTests`
// pins that shape so it cannot quietly grow.

import Foundation
import OSLog

#if canImport(MetricKit) && os(iOS) && !targetEnvironment(macCatalyst)
import MetricKit

/// Live `MXMetricManagerSubscriber`. Retained by
/// `MetricKitDiagnosticsInstaller` for the lifetime of the process —
/// `MXMetricManager` holds subscribers weakly, so a subscriber nobody
/// owns silently stops receiving payloads.
final class MetricKitDiagnosticsSubscriber: NSObject, MXMetricManagerSubscriber {

    private let store: StabilityDiagnosticsStore
    private let logger = Logger(subsystem: "com.playhead", category: "StabilityDiagnostics")

    init(store: StabilityDiagnosticsStore) {
        self.store = store
        super.init()
    }

    /// Protocol requirement. Aggregate performance metrics carry no
    /// per-incident stack and are not part of this bead's scope; the
    /// hang HISTOGRAM available here is a possible follow-up, the hang
    /// DIAGNOSTIC below is what makes a hang actionable.
    func didReceive(_ payloads: [MXMetricPayload]) {}

    /// Crash, hang, disk-write, CPU, and launch diagnostics. iOS calls
    /// this off the main thread, at most once per day, on the launch
    /// after the incident.
    func didReceive(_ payloads: [MXDiagnosticPayload]) {
        let receivedAt = Date()
        let records = payloads.flatMap { payload in
            MetricKitDiagnosticProjector.records(
                fromPayloadJSON: payload.jsonRepresentation(),
                receivedAt: receivedAt
            )
        }
        guard !records.isEmpty else { return }
        logger.info("ingested \(records.count, privacy: .public) stability diagnostic(s)")
        // Capture the actor, not `self`: this class is not Sendable, so
        // a closure that captures it cannot cross into the Task under
        // Swift 6 region isolation.
        let store = self.store
        Task { await store.append(records) }
    }
}
#endif

// MARK: - Installer

/// Launch-path entry point. Lives outside the MetricKit availability
/// guard so `PlayheadAppDelegate` can call it unconditionally and the
/// platform decision stays in one place.
enum MetricKitDiagnosticsInstaller {

    /// Strong reference to the live subscriber. `MXMetricManager` keeps
    /// only a weak one.
    ///
    /// `nonisolated(unsafe)` is sound here because every mutation goes
    /// through `install(store:processInfo:)`, which is `@MainActor` and
    /// idempotent — there is exactly one writer, on one actor, once per
    /// process.
    #if canImport(MetricKit) && os(iOS) && !targetEnvironment(macCatalyst)
    @MainActor private static var subscriber: MetricKitDiagnosticsSubscriber?
    #endif

    /// Environment variables that mean "this process is an XCTest host".
    static let testHostEnvironmentKeys = [
        "XCTestConfigurationFilePath",
        "XCTestBundlePath",
        "XCTestSessionIdentifier"
    ]

    /// Whether this process should register with MetricKit.
    ///
    /// Takes the environment dictionary rather than a `ProcessInfo` so
    /// the launch-path decision is a pure function the gate can drive
    /// directly. Returns false under XCTest: the gate launches the host
    /// app thousands of times, and a MetricKit registration in that path
    /// is pure risk for zero signal (the simulator never delivers
    /// diagnostics anyway).
    static func shouldInstall(
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) -> Bool {
        !testHostEnvironmentKeys.contains { environment[$0] != nil }
    }

    /// Register the subscriber, once. Safe to call repeatedly.
    ///
    /// No-ops on the simulator (MetricKit never delivers a diagnostic
    /// there, so registering only adds a launch-path dependency with no
    /// upside) and on Mac Catalyst (a different delivery path this bead
    /// does not cover).
    @MainActor
    static func install(
        store: StabilityDiagnosticsStore = .shared,
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) {
        guard shouldInstall(environment: environment) else { return }
        #if canImport(MetricKit) && os(iOS) && !targetEnvironment(macCatalyst) && !targetEnvironment(simulator)
        guard subscriber == nil else { return }
        let subscriber = MetricKitDiagnosticsSubscriber(store: store)
        Self.subscriber = subscriber
        MXMetricManager.shared.add(subscriber)
        #endif
    }
}
