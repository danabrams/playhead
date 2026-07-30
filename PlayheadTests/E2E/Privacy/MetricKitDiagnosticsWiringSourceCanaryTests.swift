// MetricKitDiagnosticsWiringSourceCanaryTests.swift
// Source canary for the two seams of the crash + hang pipeline that no
// unit test can reach.
//
// Scope: playhead-jw63.4.
//
// `MXMetricManager` only delivers on a real device, and
// `MXDiagnosticPayload` has no public initialiser — so the last two
// links in the chain (the app-delegate registration, and the
// subscriber's forward into the projector + store) are unreachable from
// the simulator gate. Everything downstream of `jsonRepresentation()`
// IS unit-tested; this file pins the ten lines that are not, using the
// same source-inspection idiom as `NetworkIsolationSchemeSourceCanaryTests`
// and `PlayheadRuntimeWiringSourceCanaryTests`.
//
// The canary is deliberately narrow. It asserts:
//   1. `didFinishLaunchingWithOptions` installs the MetricKit subscriber
//      — because a subscriber registered late misses the payload iOS
//      delivers moments after launch.
//   2. The subscriber implements `didReceive(_:[MXDiagnosticPayload])`
//      and routes `jsonRepresentation()` through the projector into the
//      store — i.e. it does not grow logic of its own.
//   3. The subscriber is RETAINED. `MXMetricManager` holds subscribers
//      weakly; a subscriber nobody owns is silently unsubscribed and
//      the whole pipeline goes quiet with no error anywhere.

import XCTest

@testable import Playhead

final class MetricKitDiagnosticsWiringSourceCanaryTests: XCTestCase {

    private func source(_ relativePath: String) throws -> String {
        let raw = try SwiftSourceInspector.loadSource(repoRelativePath: relativePath)
        return SwiftSourceInspector.strippingComments(raw)
    }

    // MARK: - 1. Launch-path registration

    func testAppDelegateInstallsMetricKitSubscriberOnLaunch() throws {
        let code = try source("Playhead/App/PlayheadAppDelegate.swift")
        guard let body = SwiftSourceInspector.firstBody(
            in: code,
            after: "didFinishLaunchingWithOptions"
        ) else {
            XCTFail("could not locate didFinishLaunchingWithOptions body — canary anchor drifted")
            return
        }
        XCTAssertTrue(
            body.contains("MetricKitDiagnosticsInstaller.install()"),
            """
            PlayheadAppDelegate.didFinishLaunchingWithOptions must install the MetricKit \
            subscriber. iOS delivers the day's diagnostic payload shortly after launch and \
            drops it if nobody is subscribed — a late registration is a silent data loss.
            """
        )
    }

    // MARK: - 2. Subscriber forwards, and only forwards

    func testSubscriberForwardsPayloadJSONIntoProjectorAndStore() throws {
        let code = try source("Playhead/Services/Diagnostics/MetricKitDiagnosticsSubscriber.swift")

        XCTAssertTrue(
            code.contains("MXMetricManagerSubscriber"),
            "the subscriber must conform to MXMetricManagerSubscriber"
        )
        guard let body = SwiftSourceInspector.firstBody(
            in: code,
            after: "func didReceive(_ payloads: [MXDiagnosticPayload])"
        ) else {
            XCTFail("could not locate didReceive([MXDiagnosticPayload]) — canary anchor drifted")
            return
        }
        XCTAssertTrue(
            body.contains("jsonRepresentation()"),
            "the subscriber must project from the payload's jsonRepresentation()"
        )
        XCTAssertTrue(
            body.contains("MetricKitDiagnosticProjector.records("),
            "the subscriber must route the payload through the scrubbing projector"
        )
        XCTAssertTrue(
            body.contains("store.append("),
            "the subscriber must persist the projected records"
        )
    }

    func testSubscriberDoesNotReadPayloadFieldsDirectly() throws {
        let raw = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/Diagnostics/MetricKitDiagnosticsSubscriber.swift"
        )
        let code = SwiftSourceInspector.strippingCommentsAndStrings(raw)
        // Reading typed MetricKit properties here would move privacy
        // logic OUT of the tested projector and INTO the one file the
        // gate cannot execute.
        for property in [
            ".exceptionReason",
            ".terminationReason",
            ".virtualMemoryRegionInfo",
            ".callStackTree",
            ".dictionaryRepresentation("
        ] {
            XCTAssertFalse(
                code.contains(property),
                """
                MetricKitDiagnosticsSubscriber reads '\(property)' directly. All payload \
                interpretation must stay in MetricKitDiagnosticProjector, which the test gate \
                can actually execute — this file cannot be unit-tested at all.
                """
            )
        }
    }

    // MARK: - 3. Retention

    func testInstallerRetainsTheSubscriber() throws {
        let code = try source("Playhead/Services/Diagnostics/MetricKitDiagnosticsSubscriber.swift")
        XCTAssertTrue(
            code.contains("static var subscriber: MetricKitDiagnosticsSubscriber?"),
            """
            MetricKitDiagnosticsInstaller must hold a strong reference to the subscriber. \
            MXMetricManager stores subscribers weakly, so an unowned subscriber is deallocated \
            immediately and the pipeline goes quiet with no error on any surface.
            """
        )
        guard let body = SwiftSourceInspector.firstBody(in: code, after: "static func install(") else {
            XCTFail("could not locate MetricKitDiagnosticsInstaller.install — anchor drifted")
            return
        }
        XCTAssertTrue(
            body.contains("Self.subscriber = subscriber"),
            "install() must store the subscriber before handing it to MXMetricManager"
        )
        XCTAssertTrue(
            body.contains("MXMetricManager.shared.add(subscriber)"),
            "install() must actually register with MXMetricManager"
        )
        XCTAssertTrue(
            body.contains("guard subscriber == nil else { return }"),
            "install() must be idempotent — a double registration double-counts every payload"
        )
    }

    // MARK: - 4. Export wiring

    /// `DiagnosticsExportCoordinator.stabilityFetch` DEFAULTS to
    /// `{ [] }`, so dropping the argument at either hatch compiles, runs,
    /// and ships an app whose `stability_diagnostics` block is
    /// permanently empty — with a fully green gate, because no test
    /// constructs either hatch's coordinator (they need a live
    /// PlayheadRuntime + ModelContext).
    func testBothHatchesPassTheStabilityFetchToTheCoordinator() throws {
        for (path, symbol) in [
            ("Playhead/Support/Diagnostics/ReleaseDiagnosticsHatch.swift",
             "stabilityFetch: ReleaseDiagnosticsHatch.stabilityFetch"),
            ("Playhead/Support/Diagnostics/DebugDiagnosticsHatch.swift",
             "stabilityFetch: DebugDiagnosticsHatch.stabilityFetch")
        ] {
            let code = try source(path)
            XCTAssertTrue(
                code.contains(symbol),
                """
                \(path) no longer passes `stabilityFetch` to DiagnosticsExportCoordinator. \
                The parameter defaults to { [] }, so this omission is silent: the app would \
                export an empty stability_diagnostics block forever and every test would pass.
                """
            )
            XCTAssertTrue(
                code.contains("StabilityDiagnosticsStore.shared.recent()"),
                "\(path) must source records from the live shared store"
            )
        }
    }

    /// playhead-wvdz: the identical hazard for `analysisStoreHealthFetch`,
    /// and the stakes here are higher than for the stability buffer.
    ///
    /// The parameter defaults to `{ .healthy }`. Drop the argument at any
    /// one of the three sites and it compiles, the whole gate stays green
    /// (the test target builds DEBUG, so the Release hatch is compiled by
    /// nothing any test runs), and every bundle from that surface reports
    /// `status: "healthy", consecutive_failure_count: 0` forever — a
    /// diagnostics field that says the analysis database is fine no matter
    /// how badly it is broken. That is precisely the invisible failure
    /// playhead-wvdz exists to end, reintroduced in the artifact built to
    /// reveal it.
    ///
    /// Three sites, not two: the listener-feedback channel builds its own
    /// coordinator and is the easiest one to forget.
    func testAllThreeHatchesPassTheAnalysisStoreHealthFetchToTheCoordinator() throws {
        for (path, symbol) in [
            ("Playhead/Support/Diagnostics/ReleaseDiagnosticsHatch.swift",
             "analysisStoreHealthFetch: ReleaseDiagnosticsHatch.analysisStoreHealthFetch"),
            ("Playhead/Support/Diagnostics/DebugDiagnosticsHatch.swift",
             "analysisStoreHealthFetch: DebugDiagnosticsHatch.analysisStoreHealthFetch"),
            ("Playhead/Support/Feedback/ListenerFeedbackHatch.swift",
             "analysisStoreHealthFetch: DiagnosticsHatch.analysisStoreHealthFetch")
        ] {
            let code = try source(path)
            XCTAssertTrue(
                code.contains(symbol),
                """
                \(path) no longer passes `analysisStoreHealthFetch` to \
                DiagnosticsExportCoordinator. The parameter defaults to { .healthy }, so \
                this omission is SILENT: every bundle from this surface would report a \
                healthy analysis store forever, however broken it actually is, and every \
                test would still pass (playhead-wvdz).
                """
            )
        }
    }

    /// playhead-se2h: the identical hazard for `speechModelLoadFetch`.
    ///
    /// The parameter defaults to `{ .unknown }`. Drop the argument at any
    /// one of the three sites and it compiles, the whole gate stays green
    /// (the test target builds DEBUG, so the Release hatch is compiled by
    /// nothing any test runs), and every bundle from that surface reports
    /// `status: "unknown"` forever — which reads as "this build predates
    /// the signal" rather than "the wiring is broken", so the regression
    /// hides behind its own default.
    ///
    /// The default is deliberately `unknown` rather than a healthy value,
    /// which is what makes the failure detectable AT ALL in a bundle. This
    /// canary is what makes it detectable in CI.
    ///
    /// Three sites, not two: the listener-feedback channel builds its own
    /// coordinator and is the easiest one to forget.
    func testAllThreeHatchesPassTheSpeechModelLoadFetchToTheCoordinator() throws {
        for (path, symbol) in [
            ("Playhead/Support/Diagnostics/ReleaseDiagnosticsHatch.swift",
             "speechModelLoadFetch: ReleaseDiagnosticsHatch.speechModelLoadFetch"),
            ("Playhead/Support/Diagnostics/DebugDiagnosticsHatch.swift",
             "speechModelLoadFetch: DebugDiagnosticsHatch.speechModelLoadFetch"),
            ("Playhead/Support/Feedback/ListenerFeedbackHatch.swift",
             "speechModelLoadFetch: DiagnosticsHatch.speechModelLoadFetch")
        ] {
            let code = try source(path)
            XCTAssertTrue(
                code.contains(symbol),
                """
                \(path) no longer passes `speechModelLoadFetch` to \
                DiagnosticsExportCoordinator. The parameter defaults to { .unknown }, so \
                this omission is SILENT: every bundle from this surface would report that \
                the ASR model's load history is unknown forever, however broken the device \
                actually is, and every test would still pass (playhead-se2h).
                """
            )
        }
    }

    func testBothHatchesForwardToTheSharedSpeechModelLoadAdapter() throws {
        for path in [
            "Playhead/Support/Diagnostics/ReleaseDiagnosticsHatch.swift",
            "Playhead/Support/Diagnostics/DebugDiagnosticsHatch.swift"
        ] {
            let code = try source(path)
            XCTAssertTrue(
                code.contains("SpeechModelLoadDiagnosticsFetchAdapter.shared"),
                """
                \(path) no longer forwards to the single unconditionally-compiled \
                SpeechModelLoadDiagnosticsFetchAdapter. A local copy here would be compiled \
                by nothing any test runs, so a divergence would ship green.
                """
            )
        }
    }

    /// Both hatch helpers must forward to the ONE unconditionally-compiled
    /// implementation. A hand-rolled second copy on the Release hatch is
    /// compiled by nothing any test can run — the same drift that made
    /// `RediffDiagnosticsFetchAdapter` a shared type.
    func testBothHatchesForwardToTheSharedAnalysisStoreHealthAdapter() throws {
        for path in [
            "Playhead/Support/Diagnostics/ReleaseDiagnosticsHatch.swift",
            "Playhead/Support/Diagnostics/DebugDiagnosticsHatch.swift"
        ] {
            let code = try source(path)
            XCTAssertTrue(
                code.contains("AnalysisStoreHealthDiagnosticsFetchAdapter.shared"),
                """
                \(path) no longer forwards to the single unconditionally-compiled \
                AnalysisStoreHealthDiagnosticsFetchAdapter. A local copy here would be \
                compiled by nothing any test runs, so a divergence would ship green.
                """
            )
        }
    }

    // MARK: - Installation predicate (this part IS executable)

    func testShouldInstallIsFalseUnderXCTest() {
        // The predicate is pure, so unlike the registration itself it
        // can be exercised here. Under the gate the host app launches
        // thousands of times; a MetricKit registration in that path is
        // pure risk for zero signal.
        XCTAssertFalse(
            MetricKitDiagnosticsInstaller.shouldInstall(),
            "the live process is running under XCTest, so shouldInstall() must be false"
        )
    }

    func testShouldInstallIsTrueForAPlainProcess() {
        XCTAssertTrue(
            MetricKitDiagnosticsInstaller.shouldInstall(environment: ["HOME": "/Users/nobody"])
        )
        for key in MetricKitDiagnosticsInstaller.testHostEnvironmentKeys {
            XCTAssertFalse(
                MetricKitDiagnosticsInstaller.shouldInstall(environment: [key: "anything"]),
                "'\(key)' must be recognised as a test host"
            )
        }
        XCTAssertFalse(
            MetricKitDiagnosticsInstaller.testHostEnvironmentKeys.isEmpty,
            "the loop above would be vacuous with an empty key list"
        )
    }
}
