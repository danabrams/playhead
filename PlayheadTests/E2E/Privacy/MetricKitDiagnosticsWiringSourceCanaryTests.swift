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
