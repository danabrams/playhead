import Foundation
import Testing
@testable import Playhead

/// playhead-h9y6 — the launch-path bootstrap failure that used to vanish into
/// an empty catch is now recorded, exported, and honest about its absence.
@Suite("playhead-h9y6: a failed download bootstrap is counted where a bundle can read it")
struct LaunchHealthTests {

    private func isolatedDefaults() -> UserDefaults {
        let name = "h9y6-\(UUID().uuidString)"
        let d = UserDefaults(suiteName: name)!
        d.removePersistentDomain(forName: name)
        return d
    }

    @Test("bootstrap() really can throw — a root that is a FILE, not a directory")
    func bootstrapThrowIsReachable() async throws {
        let tmp = FileManager.default.temporaryDirectory.appendingPathComponent("h9y6-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: tmp, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tmp) }
        let rootThatIsAFile = tmp.appendingPathComponent("AudioCache")
        try Data("not a directory".utf8).write(to: rootThatIsAFile)

        let manager = DownloadManager(cacheDirectory: rootThatIsAFile)
        await #expect(throws: (any Error).self, "creating a directory over a file must throw, or the catch under test is unreachable") {
            try await manager.bootstrap()
        }
    }

    @Test("bootstrap() is idempotent on a good root — the premise of any future retry")
    func bootstrapIsIdempotent() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("h9y6-ok-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: root) }
        let manager = DownloadManager(cacheDirectory: root)
        try await manager.bootstrap()
        try await manager.bootstrap()
    }

    @Test("the recorder counts, keeps the last error, and survives a re-read")
    func recorderRoundTrips() {
        let defaults = isolatedDefaults()
        let recorder = LaunchHealthRecorder(defaults: defaults)
        #expect(recorder.snapshot() == LaunchHealthRecorder.Snapshot(
            downloadBootstrapFailures: 0, lastDownloadBootstrapError: nil, lastDownloadBootstrapFailureAt: nil
        ))
        let at = Date(timeIntervalSince1970: 1_700_000_000)
        recorder.recordDownloadBootstrapFailure(CocoaError(.fileWriteNoPermission), at: at)
        recorder.recordDownloadBootstrapFailure(CocoaError(.fileWriteOutOfSpace), at: at.addingTimeInterval(60))

        let again = LaunchHealthRecorder(defaults: defaults).snapshot()
        #expect(again.downloadBootstrapFailures == 2)
        #expect(again.lastDownloadBootstrapError?.contains("OutOfSpace") == true || again.lastDownloadBootstrapError?.contains("640") == true,
                "the LAST error must be the one kept; got \(String(describing: again.lastDownloadBootstrapError))")
        #expect(again.lastDownloadBootstrapFailureAt == at.addingTimeInterval(60))
    }

    @Test("a bundle predating the field decodes as unrecorded, never as zero failures")
    func bundlePredatingTheFieldDecodesAsUnrecorded() throws {
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0.0",
            osVersion: "iPhone OS 27.0",
            deviceClass: .iPhone17Pro,
            buildType: .debug,
            eligibility: AnalysisEligibility(
                hardwareSupported: true, appleIntelligenceEnabled: true, regionSupported: true,
                languageSupported: true, modelAvailableNow: true,
                capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
            ),
            workJournalEntries: [],
            installID: UUID(uuidString: "33333333-3333-4333-8333-333333333333")!
        )
        #expect(bundle.launchHealth == .unrecorded, "the builder's own default must be unrecorded too")

        let encoder = JSONEncoder(); encoder.dateEncodingStrategy = .iso8601
        var object = try #require(try JSONSerialization.jsonObject(with: try encoder.encode(bundle)) as? [String: Any])
        object.removeValue(forKey: "launch_health")
        let stripped = try JSONSerialization.data(withJSONObject: object)
        let decoder = JSONDecoder(); decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(DefaultBundle.self, from: stripped)
        #expect(decoded.launchHealth == .unrecorded)
        #expect(!decoded.launchHealth.recorded, "absence must read as 'nobody counted'")
    }

    @Test("a recorded section round-trips with its count and its last error")
    func recordedSectionRoundTrips() throws {
        let section = DefaultBundle.LaunchHealth(
            LaunchHealthRecorder.Snapshot(
                downloadBootstrapFailures: 3,
                lastDownloadBootstrapError: "NSCocoaErrorDomain 640",
                lastDownloadBootstrapFailureAt: Date(timeIntervalSince1970: 1_700_000_000)
            )
        )
        let encoder = JSONEncoder(); encoder.dateEncodingStrategy = .iso8601
        let decoder = JSONDecoder(); decoder.dateDecodingStrategy = .iso8601
        let back = try decoder.decode(DefaultBundle.LaunchHealth.self, from: try encoder.encode(section))
        #expect(back == section)
        #expect(back.recorded)
        let json = String(decoding: try encoder.encode(section), as: UTF8.self)
        #expect(json.contains("\"download_bootstrap_failures\":3"), "the readout script reads this exact key")
    }
}
