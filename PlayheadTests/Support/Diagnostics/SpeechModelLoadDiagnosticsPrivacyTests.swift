// SpeechModelLoadDiagnosticsPrivacyTests.swift
// The privacy + seam proof for `speech_model_load` (playhead-se2h).
//
// Modelled on `AnalysisStoreHealthDiagnosticsPrivacyTests`. Three claims:
//
//   1. CLOSED SHAPE — the encoded key set is exactly the declared
//      `CodingKeys`, at every level, and there is NO free-text field
//      anywhere. A model that will not load has no episode, show or URL
//      to name, so unlike its analysis-store sibling this shape has no
//      sanitised `detail` escape hatch at all, and adding one must turn
//      this red.
//   2. SEAM — the value actually travels journal → fetch → coordinator →
//      encoded JSON. A signal that is written and never read is the usual
//      way this class of fix breaks; it is exactly how the pre-wvdz
//      arrangement broke, and the reason that bead added a call-site
//      canary.
//   3. THE DEFAULT IS NOT HEALTH. A coordinator built without the fetch
//      must emit `unknown`, never a value a reader would mistake for a
//      working speech stack. This is what makes a dropped wiring argument
//      detectable in a bundle rather than invisible.

import Foundation
import Testing

@testable import Playhead

@MainActor
private final class StubSpeechLoadPresenter: DiagnosticsExportPresenter {
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
private final class StubSpeechLoadOptInSink: DiagnosticsOptInSink {
    func applyResetToEpisodes(matchingEpisodeIds: [String], newValue: Bool) {}
}

/// Fixtures live outside the `@MainActor` suite so the `@Sendable` fetch
/// closures the coordinator takes can reach them without hopping.
private enum SpeechLoadFixture {

    static let now = Date(timeIntervalSince1970: 1_700_000_000)
    static let installID = UUID(uuidString: "33333333-3333-4333-8333-333333333333")!

    /// A state carrying every field populated, including a full failure
    /// list, so the sweep below has something to find if a free-text
    /// field is ever introduced.
    static func fullyPopulatedState() -> SpeechModelLoadState {
        SpeechModelLoadState(
            status: .persistentlyFailing,
            consecutiveFailureCount: 4,
            firstFailureAt: now.addingTimeInterval(-3_600),
            lastFailureAt: now,
            lastSuccessAt: now.addingTimeInterval(-86_400),
            lastSuccessfulRole: .asrFast,
            recentFailures: [
                SpeechModelLoadFailureRecord(
                    occurredAt: now.addingTimeInterval(-3_600),
                    failureClass: .speechAssetsUnsupported,
                    attemptNumber: 1,
                    consecutiveFailureCount: 1
                ),
                SpeechModelLoadFailureRecord(
                    occurredAt: now,
                    failureClass: .analyzerFormatUnavailable,
                    attemptNumber: 3,
                    consecutiveFailureCount: 4
                )
            ]
        )
    }
}

@Suite("Diagnostics bundle — speech_model_load (playhead-se2h)")
@MainActor
struct SpeechModelLoadDiagnosticsPrivacyTests {

    /// FROZEN LITERALS, deliberately not `CodingKeys.allCases` — deriving
    /// the allowlist from the type under test is circular, and a new
    /// free-text field would widen the allowlist and the encoding in the
    /// same edit, leaving the gate green.
    private static let frozenStateKeys: Set<String> = [
        "status",
        "consecutive_failure_count",
        "first_failure_at",
        "last_failure_at",
        "last_success_at",
        "last_successful_role",
        "recent_failures"
    ]

    /// Keys that must be present even on a device with nothing to report,
    /// so "subset" above cannot be satisfied by an empty object.
    private static let requiredStateKeys: Set<String> = [
        "status",
        "consecutive_failure_count",
        "recent_failures"
    ]

    private static let frozenFailureRecordKeys: Set<String> = [
        "occurred_at",
        "failure_class",
        "attempt_number",
        "consecutive_failure_count"
    ]

    private static func makeCoordinator(
        speechModelLoadFetch: DiagnosticsSpeechModelLoadFetch? = nil
    ) -> DiagnosticsExportCoordinator {
        let environment = DiagnosticsExportEnvironment(
            appVersion: "1.0.0",
            osVersion: "iPhone OS 27.0",
            deviceClass: .iPhone17Pro,
            buildType: .debug,
            eligibility: AnalysisEligibility(
                hardwareSupported: true,
                appleIntelligenceEnabled: true,
                regionSupported: true,
                languageSupported: true,
                modelAvailableNow: true,
                capturedAt: SpeechLoadFixture.now
            ),
            installID: SpeechLoadFixture.installID,
            now: SpeechLoadFixture.now
        )
        // Two construction shapes on purpose: WITH the argument (the
        // production wiring) and WITHOUT it (the dropped-argument
        // regression), so the default's value is itself under test.
        if let speechModelLoadFetch {
            return DiagnosticsExportCoordinator(
                environment: environment,
                presenter: StubSpeechLoadPresenter(),
                journalFetch: { [] },
                speechModelLoadFetch: speechModelLoadFetch,
                optInSink: StubSpeechLoadOptInSink()
            )
        }
        return DiagnosticsExportCoordinator(
            environment: environment,
            presenter: StubSpeechLoadPresenter(),
            journalFetch: { [] },
            optInSink: StubSpeechLoadOptInSink()
        )
    }

    private static func defaultSubtree(_ data: Data) throws -> [String: Any] {
        let root = try #require(try JSONSerialization.jsonObject(with: data) as? [String: Any])
        return try #require(root["default"] as? [String: Any])
    }

    // MARK: - 1. Closed shape, and no free text anywhere

    @Test("The encoded key set is exactly the declared CodingKeys, at every level")
    func encodedKeySetIsClosed() throws {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(SpeechLoadFixture.fullyPopulatedState())
        let object = try #require(try JSONSerialization.jsonObject(with: data) as? [String: Any])

        // Subset rather than equality: `Encodable` omits a nil optional's
        // key entirely, which is the intended encoding for a device that
        // has never succeeded.
        #expect(Set(object.keys).isSubset(of: Self.frozenStateKeys))
        #expect(Set(object.keys).isSuperset(of: Self.requiredStateKeys))

        let failures = try #require(object["recent_failures"] as? [[String: Any]])
        #expect(failures.count == 2)
        for record in failures {
            #expect(Set(record.keys) == Self.frozenFailureRecordKeys)
        }
    }

    /// Every scalar in the shape is a counter, a date, or a rawValue of an
    /// enum this repo defines. This asserts the property directly: no
    /// string value anywhere in the encoded document is outside the closed
    /// vocabularies.
    @Test("No encoded string value is outside the closed vocabularies")
    func everyStringValueComesFromAClosedVocabulary() throws {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(SpeechLoadFixture.fullyPopulatedState())
        let object = try #require(try JSONSerialization.jsonObject(with: data) as? [String: Any])

        let allowedStrings = Set(SpeechModelLoadStatus.allCases.map(\.rawValue))
            .union(TranscriptFailureClass.allCases.map(\.rawValue))
            .union(ModelRole.allCases.map(\.rawValue))

        var offenders: [String] = []
        func sweep(_ value: Any, path: String) {
            switch value {
            case let dict as [String: Any]:
                for (key, nested) in dict { sweep(nested, path: "\(path).\(key)") }
            case let array as [Any]:
                for (index, nested) in array.enumerated() { sweep(nested, path: "\(path)[\(index)]") }
            case let string as String:
                // ISO-8601 dates are the only other string shape.
                let isDate = ISO8601DateFormatter().date(from: string) != nil
                if !isDate && !allowedStrings.contains(string) {
                    offenders.append("\(path) = \"\(string)\"")
                }
            default:
                break
            }
        }
        sweep(object, path: "speech_model_load")

        #expect(
            offenders.isEmpty,
            """
            Found string values outside the closed vocabularies: \(offenders). \
            This shape must carry no free text at all — a model that will not load has \
            no episode, show or URL to name, so any free-text field is unnecessary risk.
            """
        )
    }

    // MARK: - 2. The seam actually carries the value

    @Test("The journal's value travels all the way into the encoded bundle")
    func valueTravelsFromFetchToEncodedJSON() async throws {
        let state = SpeechLoadFixture.fullyPopulatedState()
        let coordinator = Self.makeCoordinator(speechModelLoadFetch: { state })

        let (data, _, _) = try await coordinator.buildAndEncode()
        let subtree = try Self.defaultSubtree(data)
        let block = try #require(
            subtree["speech_model_load"] as? [String: Any],
            "the `speech_model_load` key must always be emitted"
        )

        #expect(block["status"] as? String == SpeechModelLoadStatus.persistentlyFailing.rawValue)
        #expect(block["consecutive_failure_count"] as? Int == 4)
        #expect(block["last_successful_role"] as? String == ModelRole.asrFast.rawValue)
        let failures = try #require(block["recent_failures"] as? [[String: Any]])
        #expect(failures.count == 2)
        #expect(
            failures.compactMap { $0["failure_class"] as? String }
                == [TranscriptFailureClass.speechAssetsUnsupported.rawValue,
                    TranscriptFailureClass.analyzerFormatUnavailable.rawValue],
            "the precise cause must survive the trip — collapsing it loses the whole diagnostic"
        )
    }

    /// A journal read through the REAL production adapter shape (a journal
    /// instance, its `load()`, and the coordinator) rather than a literal.
    /// This is the seam a dropped argument breaks.
    @Test("A real journal on disk reaches the bundle through the adapter shape")
    func realJournalReachesTheBundle() async throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("se2h-privacy-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let journal = SpeechModelLoadJournal(directory: directory)
        await journal.recordFailure(
            error: TranscriptEngineError.transcriptionFailed("assets unavailable"),
            attemptNumber: 1
        )

        let coordinator = Self.makeCoordinator(speechModelLoadFetch: { await journal.load() })
        let (data, _, _) = try await coordinator.buildAndEncode()
        let block = try #require(try Self.defaultSubtree(data)["speech_model_load"] as? [String: Any])

        #expect(block["status"] as? String == SpeechModelLoadStatus.retrying.rawValue)
        #expect(block["consecutive_failure_count"] as? Int == 1)
    }

    // MARK: - 3. The default is not health

    /// THE DROPPED-ARGUMENT CASE. The parameter has a default, so omitting
    /// it compiles and every test stays green. What must not happen is
    /// that the omission LOOKS healthy: a `status` of `loaded` here would
    /// mean a wiring regression ships bundles reporting a working speech
    /// stack forever, which is the hazard playhead-wvdz was filed for.
    @Test("A coordinator built without the fetch reports `unknown`, never a healthy value")
    func omittedFetchReportsUnknownRatherThanHealth() async throws {
        let coordinator = Self.makeCoordinator(speechModelLoadFetch: nil)

        let (data, _, _) = try await coordinator.buildAndEncode()
        let block = try #require(
            try Self.defaultSubtree(data)["speech_model_load"] as? [String: Any],
            "the key must be emitted even with no fetch, so its absence cannot be mistaken for an old bundle"
        )

        #expect(block["status"] as? String == SpeechModelLoadStatus.unknown.rawValue)
        #expect(block["last_success_at"] == nil)
        #expect(block["last_successful_role"] == nil)
        #expect(
            block["status"] as? String != SpeechModelLoadStatus.loaded.rawValue,
            "an unwired signal must never present as a working speech stack"
        )
    }

    /// Forward compatibility in the other direction: a bundle minted
    /// before this bead has no key at all, and must decode as `unknown`
    /// rather than failing or inventing health.
    @Test("A bundle predating the field decodes as unknown")
    func bundlePredatingTheFieldDecodesAsUnknown() throws {
        let state = SpeechLoadFixture.fullyPopulatedState()
        let coordinator = Self.makeCoordinator(speechModelLoadFetch: { state })
        _ = coordinator

        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0.0",
            osVersion: "iPhone OS 27.0",
            deviceClass: .iPhone17Pro,
            buildType: .debug,
            eligibility: AnalysisEligibility(
                hardwareSupported: true,
                appleIntelligenceEnabled: true,
                regionSupported: true,
                languageSupported: true,
                modelAvailableNow: true,
                capturedAt: SpeechLoadFixture.now
            ),
            workJournalEntries: [],
            installID: SpeechLoadFixture.installID
        )
        #expect(
            bundle.speechModelLoad == .unknown,
            "the builder's own default must also be `unknown`, for the same reason"
        )

        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        var object = try #require(
            try JSONSerialization.jsonObject(
                with: try encoder.encode(bundle)
            ) as? [String: Any]
        )
        object.removeValue(forKey: "speech_model_load")
        let stripped = try JSONSerialization.data(withJSONObject: object)

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(DefaultBundle.self, from: stripped)
        #expect(decoded.speechModelLoad == .unknown)
    }
}
