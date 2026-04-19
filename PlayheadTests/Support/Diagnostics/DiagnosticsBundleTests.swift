// DiagnosticsBundleTests.swift
// Round-trips the JSON shape of the support-safe diagnostics bundle.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// Spec contracts under test:
//   * `DefaultBundle` round-trips through JSON Encoder/Decoder.
//   * `analysis_unavailable_reason` is OMITTED (key absent) when nil — not
//     serialized as `null`. Matches "support-safe by default" — absent key
//     == fully eligible device, no false alarm in the dump.
//   * `OptInBundle` round-trips through JSON.
//   * `scheduler_events` and `work_journal_tail` field-name shape matches
//     the spec (snake_case keys), so the support engineer's grep cheat
//     sheet keeps working across releases.

import Foundation
import Testing

@testable import Playhead

@Suite("DiagnosticsBundle — JSON round-trip + key shape (playhead-ghon)")
struct DiagnosticsBundleTests {

    // MARK: - Fixtures

    private static let t0 = Date(timeIntervalSince1970: 1_700_000_000)

    private static let eligible = AnalysisEligibility(
        hardwareSupported: true,
        appleIntelligenceEnabled: true,
        regionSupported: true,
        languageSupported: true,
        modelAvailableNow: true,
        capturedAt: t0
    )

    // MARK: - Encoding helpers

    private func encode<T: Encodable>(_ value: T) throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        return try encoder.encode(value)
    }

    private func decode<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(type, from: data)
    }

    // MARK: - Default bundle round-trip

    @Test("DefaultBundle round-trips through JSON")
    func defaultBundleRoundTrip() throws {
        let bundle = DefaultBundle(
            appVersion: "1.0.0",
            osVersion: "iOS 26.0",
            deviceClass: .iPhone17Pro,
            buildType: .debug,
            eligibilitySnapshot: Self.eligible,
            analysisUnavailableReason: nil,
            schedulerEvents: [],
            workJournalTail: []
        )
        let data = try encode(bundle)
        let decoded = try decode(DefaultBundle.self, from: data)
        #expect(decoded == bundle)
    }

    // MARK: - `analysis_unavailable_reason` is omitted (key absent) when nil

    @Test("analysis_unavailable_reason is omitted when nil (key absent, not null)")
    func unavailableReasonOmittedWhenNil() throws {
        let bundle = DefaultBundle(
            appVersion: "1.0.0",
            osVersion: "iOS 26.0",
            deviceClass: .iPhone17Pro,
            buildType: .debug,
            eligibilitySnapshot: Self.eligible,
            analysisUnavailableReason: nil,
            schedulerEvents: [],
            workJournalTail: []
        )
        let data = try encode(bundle)
        let json = try #require(String(data: data, encoding: .utf8))
        #expect(!json.contains("analysis_unavailable_reason"))
    }

    @Test("analysis_unavailable_reason is included when device is ineligible")
    func unavailableReasonIncludedWhenSet() throws {
        let bundle = DefaultBundle(
            appVersion: "1.0.0",
            osVersion: "iOS 26.0",
            deviceClass: .iPhone14andOlder,
            buildType: .debug,
            eligibilitySnapshot: AnalysisEligibility(
                hardwareSupported: false,
                appleIntelligenceEnabled: true,
                regionSupported: true,
                languageSupported: true,
                modelAvailableNow: true,
                capturedAt: Self.t0
            ),
            analysisUnavailableReason: .hardwareUnsupported,
            schedulerEvents: [],
            workJournalTail: []
        )
        let data = try encode(bundle)
        let json = try #require(String(data: data, encoding: .utf8))
        #expect(json.contains("\"analysis_unavailable_reason\":\"hardware_unsupported\""))
    }

    // MARK: - Snake-case key shape

    @Test("DefaultBundle keys use snake_case (support engineer grep contract)")
    func defaultBundleKeyShape() throws {
        let bundle = DefaultBundle(
            appVersion: "1.0.0",
            osVersion: "iOS 26.0",
            deviceClass: .iPhone17Pro,
            buildType: .release,
            eligibilitySnapshot: Self.eligible,
            analysisUnavailableReason: nil,
            schedulerEvents: [],
            workJournalTail: []
        )
        let data = try encode(bundle)
        let json = try #require(String(data: data, encoding: .utf8))
        for key in [
            "\"app_version\"",
            "\"os_version\"",
            "\"device_class\"",
            "\"build_type\"",
            "\"eligibility_snapshot\"",
            "\"scheduler_events\"",
            "\"work_journal_tail\"",
        ] {
            #expect(json.contains(key), "Expected key \(key) in \(json)")
        }
    }

    @Test("BuildType cases serialize as snake_case raw values")
    func buildTypeRawValues() {
        #expect(BuildType.debug.rawValue == "debug")
        #expect(BuildType.release.rawValue == "release")
        #expect(BuildType.testFlight.rawValue == "test_flight")
    }

    // MARK: - OptIn bundle round-trip

    @Test("OptInBundle round-trips through JSON")
    func optInBundleRoundTrip() throws {
        let bundle = OptInBundle(episodes: [
            OptInBundle.Episode(
                episodeId: "ep-1",
                episodeTitle: "Hello",
                transcriptExcerpts: [
                    OptInBundle.TranscriptExcerpt(
                        boundaryTime: 60,
                        startTime: 30,
                        endTime: 90,
                        text: "An ad about widgets."
                    ),
                ],
                featureSummaries: [
                    OptInBundle.FeatureSummary(
                        rmsMean: 0.5,
                        rmsMax: 0.9,
                        spectralFluxMean: 0.4,
                        musicProbabilityMean: 0.2,
                        pauseProbabilityMean: 0.1
                    ),
                ]
            ),
        ])
        let data = try encode(bundle)
        let decoded = try decode(OptInBundle.self, from: data)
        #expect(decoded == bundle)
    }

    @Test("OptInBundle uses snake_case keys")
    func optInBundleKeyShape() throws {
        let bundle = OptInBundle(episodes: [
            OptInBundle.Episode(
                episodeId: "ep-1",
                episodeTitle: "T",
                transcriptExcerpts: [
                    OptInBundle.TranscriptExcerpt(
                        boundaryTime: 60,
                        startTime: 30,
                        endTime: 90,
                        text: "x"
                    ),
                ],
                featureSummaries: []
            ),
        ])
        let data = try encode(bundle)
        let json = try #require(String(data: data, encoding: .utf8))
        for key in [
            "\"episode_id\"",
            "\"episode_title\"",
            "\"transcript_excerpts\"",
            "\"feature_summaries\"",
            "\"boundary_time\"",
            "\"start_time\"",
            "\"end_time\"",
        ] {
            #expect(json.contains(key), "Expected key \(key) in \(json)")
        }
    }
}
