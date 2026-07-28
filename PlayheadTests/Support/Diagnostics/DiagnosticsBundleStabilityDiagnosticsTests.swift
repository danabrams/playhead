// DiagnosticsBundleStabilityDiagnosticsTests.swift
// End-to-end proof that a MetricKit crash / hang travels the whole
// pipeline — payload → projector → ring buffer → diagnostics bundle —
// and arrives scrubbed.
//
// Scope: playhead-jw63.4, legal checklist item (e).
//
// The unit suites cover each stage in isolation. This one covers the
// SEAMS, which is where a pipeline usually breaks: a store that is
// written but never read, a coordinator that fetches but never
// forwards, a builder that drops the field on the floor.

import Foundation
import Testing

@testable import Playhead

@MainActor
private final class StubPresenter: DiagnosticsExportPresenter {
    func present(
        data: Data,
        filename: String,
        subject: String,
        completion: @escaping @MainActor (Result<DiagnosticsMailComposeResult, Error>) -> Void
    ) {
        completion(.success(.cancelled))
    }
}

@MainActor
private final class StubOptInSink: DiagnosticsOptInSink {
    func applyResetToEpisodes(matchingEpisodeIds: [String], newValue: Bool) {}
}

@Suite("Diagnostics bundle — stability_diagnostics (playhead-jw63.4)")
@MainActor
struct DiagnosticsBundleStabilityDiagnosticsTests {

    private static let now = Date(timeIntervalSince1970: 1_700_000_000)

    private func environment() -> DiagnosticsExportEnvironment {
        DiagnosticsExportEnvironment(
            appVersion: "1.0.0",
            osVersion: "iOS 27.0",
            deviceClass: .iPhone17Pro,
            buildType: .release,
            eligibility: AnalysisEligibility(
                hardwareSupported: true,
                appleIntelligenceEnabled: true,
                regionSupported: true,
                languageSupported: true,
                modelAvailableNow: true,
                capturedAt: Self.now
            ),
            installID: UUID(uuidString: "11111111-1111-4111-8111-111111111111")!,
            now: Self.now
        )
    }

    private func coordinator(
        stabilityFetch: @escaping DiagnosticsStabilityFetch
    ) -> DiagnosticsExportCoordinator {
        DiagnosticsExportCoordinator(
            environment: environment(),
            presenter: StubPresenter(),
            journalFetch: { [] },
            stabilityFetch: stabilityFetch,
            optInSink: StubOptInSink(),
            optInEpisodes: []
        )
    }

    private func temporaryDirectory() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("stability-bundle-\(UUID().uuidString)", isDirectory: true)
    }

    // MARK: - Full pipeline

    @Test("a hostile MetricKit payload reaches the bundle as a scrubbed record")
    func payloadTravelsToBundleScrubbed() async throws {
        let directory = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        // 1. iOS hands us a payload (crash + hang), free-text fields
        //    stuffed with forbidden content.
        let payloadData = try MetricKitPayloadFixture.payloadData(
            MetricKitPayloadFixture.payload(
                crashes: [MetricKitPayloadFixture.crashDiagnostic(leaky: true)],
                hangs: [MetricKitPayloadFixture.hangDiagnostic()]
            )
        )

        // 2. The projector scrubs; the store persists.
        let records = MetricKitDiagnosticProjector.records(
            fromPayloadJSON: payloadData,
            receivedAt: Self.now
        )
        let store = StabilityDiagnosticsStore(directory: directory)
        await store.append(records)

        // 3. The coordinator reads the store and encodes the bundle.
        let coordinator = coordinator(stabilityFetch: { await store.recent() })
        let (data, _, _) = try await coordinator.buildAndEncode()
        let json = String(decoding: data, as: UTF8.self)

        // CAPTURE: the incidents are actually in the bundle.
        let root = try #require(
            try JSONSerialization.jsonObject(with: data, options: []) as? [String: Any]
        )
        let defaultSubtree = try #require(root["default"] as? [String: Any])
        let stability = try #require(defaultSubtree["stability_diagnostics"] as? [[String: Any]])
        #expect(stability.count == 2)
        #expect(Set(stability.compactMap { $0["kind"] as? String }) == ["crash", "hang"])

        // ACTIONABLE: the crash carries what symbolication needs.
        let crash = try #require(stability.first { $0["kind"] as? String == "crash" })
        #expect(crash["app_build_version"] as? String == "42")
        #expect(crash["termination_code"] as? String == "0x8badf00d")
        let frames = try #require(crash["frames"] as? [[String: Any]])
        #expect(!frames.isEmpty)
        #expect(frames.contains { $0["binary_uuid"] as? String == MetricKitPayloadFixture.appBinaryUUID })
        #expect(frames.allSatisfy { $0["offset_into_binary_text_segment"] != nil })

        // SCRUBBED: none of the forbidden content is anywhere in the
        // encoded bundle — not just the stability subtree.
        for sentinel in MetricKitPayloadFixture.allSentinels {
            #expect(!json.contains(sentinel), "sentinel leaked into the diagnostics bundle: \(sentinel)")
        }
    }

    // MARK: - Key stability

    @Test("stability_diagnostics is emitted even when the device has never crashed")
    func keyPresentWhenEmpty() async throws {
        let coordinator = coordinator(stabilityFetch: { [] })
        let (data, _, _) = try await coordinator.buildAndEncode()
        let root = try #require(
            try JSONSerialization.jsonObject(with: data, options: []) as? [String: Any]
        )
        let defaultSubtree = try #require(root["default"] as? [String: Any])
        let stability = try #require(
            defaultSubtree["stability_diagnostics"] as? [Any],
            "the key must be present even when empty, so 'no crashes' is distinguishable from 'old bundle'"
        )
        #expect(stability.isEmpty)
    }

    @Test("a bundle predating the crash pipeline still decodes, with an empty array")
    func legacyBundleDecodes() throws {
        let legacy = """
        {
          "app_version": "0.9.0",
          "os_version": "iOS 26.0",
          "device_class": "iPhone17Pro",
          "build_type": "release",
          "eligibility_snapshot": {
            "hardwareSupported": true,
            "appleIntelligenceEnabled": true,
            "regionSupported": true,
            "languageSupported": true,
            "modelAvailableNow": true,
            "capturedAt": 700000000
          },
          "scheduler_events": [],
          "work_journal_tail": []
        }
        """
        let bundle = try JSONDecoder().decode(DefaultBundle.self, from: Data(legacy.utf8))
        #expect(bundle.stabilityDiagnostics.isEmpty)
    }

    // MARK: - Ordering

    @Test("the bundle lists the newest incident first")
    func newestFirst() async throws {
        let directory = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = StabilityDiagnosticsStore(directory: directory)
        await store.append([
            StabilityDiagnosticRecord(kind: .crash, receivedAt: 100),
            StabilityDiagnosticRecord(kind: .hang, receivedAt: 200)
        ])

        let coordinator = coordinator(stabilityFetch: { await store.recent() })
        let (data, _, _) = try await coordinator.buildAndEncode()
        let root = try #require(
            try JSONSerialization.jsonObject(with: data, options: []) as? [String: Any]
        )
        let defaultSubtree = try #require(root["default"] as? [String: Any])
        let stability = try #require(defaultSubtree["stability_diagnostics"] as? [[String: Any]])
        #expect(stability.map { $0["received_at"] as? Double } == [200, 100])
    }

    // MARK: - Isolation from the opt-in lane

    @Test("stability records carry no episode reference at all — not even a hash")
    func noEpisodeReference() async throws {
        let payloadData = try MetricKitPayloadFixture.payloadData(
            MetricKitPayloadFixture.payload(
                crashes: [MetricKitPayloadFixture.crashDiagnostic(leaky: true)]
            )
        )
        let records = MetricKitDiagnosticProjector.records(
            fromPayloadJSON: payloadData,
            receivedAt: Self.now
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let encoded = String(decoding: try encoder.encode(records), as: UTF8.self)
        for token in ["episode_id", "episodeId", "episode_id_hash", "transcript", "feed_url"] {
            #expect(!encoded.contains(token), "stability record mentions '\(token)'")
        }
    }
}
