// DiagnosticsExportCoordinatorTests.swift
// Coverage for `DiagnosticsExportCoordinator` — the @MainActor orchestrator
// that wires the pure builder + pure reset policy to a UI presenter and a
// SwiftData opt-in sink.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// The tests exercise the coordinator against stubs so nothing touches
// MFMailComposeViewController, UIKit, or SwiftData. The SwiftData sink
// gets its own tests alongside the InstallIdentity / opt-in migration
// suites; here we only verify that the coordinator hits the sink with
// the right IDs in the right result states.

import Foundation
import Testing

@testable import Playhead

// MARK: - Fakes

@MainActor
private final class FakePresenter: DiagnosticsExportPresenter {

    enum Script {
        case succeed(DiagnosticsMailComposeResult)
        case fail(Error)
    }

    var script: Script
    private(set) var callCount = 0
    private(set) var lastData: Data?
    private(set) var lastFilename: String?
    private(set) var lastSubject: String?

    init(script: Script) { self.script = script }

    func present(
        data: Data,
        filename: String,
        subject: String,
        completion: @escaping @MainActor (Result<DiagnosticsMailComposeResult, Error>) -> Void
    ) {
        callCount += 1
        lastData = data
        lastFilename = filename
        lastSubject = subject
        switch script {
        case .succeed(let result):
            completion(.success(result))
        case .fail(let error):
            completion(.failure(error))
        }
    }
}

@MainActor
private final class FakeOptInSink: DiagnosticsOptInSink {

    struct Call: Equatable {
        let ids: [String]
        let newValue: Bool
    }

    private(set) var calls: [Call] = []

    func applyResetToEpisodes(matchingEpisodeIds: [String], newValue: Bool) {
        calls.append(.init(ids: matchingEpisodeIds, newValue: newValue))
    }
}

// MARK: - Helpers

@MainActor
private func makeEnvironment(now: Date = Date(timeIntervalSince1970: 1_700_000_000)) -> DiagnosticsExportEnvironment {
    DiagnosticsExportEnvironment(
        appVersion: "1.0.0",
        osVersion: "iOS 26.0",
        deviceClass: .iPhone17Pro,
        buildType: .release,
        eligibility: AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: now
        ),
        installID: UUID(uuidString: "11111111-1111-4111-8111-111111111111")!,
        now: now
    )
}

private func makeOptInEpisode(
    id: String,
    diagnosticsOptIn: Bool = true
) -> DiagnosticsEpisodeInput {
    DiagnosticsEpisodeInput(
        episodeId: id,
        episodeTitle: "Episode \(id)",
        diagnosticsOptIn: diagnosticsOptIn,
        adBoundaryTimes: [],
        transcriptChunks: [],
        featureSummary: nil
    )
}

private func noopJournalFetch() -> DiagnosticsJournalFetch {
    { [] }
}

// MARK: - Suite

@Suite("DiagnosticsExportCoordinator (playhead-ghon)")
@MainActor
struct DiagnosticsExportCoordinatorTests {

    // MARK: - Happy path

    @Test(".sent clears opt-in for every episode that shipped")
    func sentClearsOptIn() async throws {
        let presenter = FakePresenter(script: .succeed(.sent))
        let sink = FakeOptInSink()
        let coordinator = DiagnosticsExportCoordinator(
            environment: makeEnvironment(),
            presenter: presenter,
            journalFetch: noopJournalFetch(),
            optInSink: sink,
            optInEpisodes: [
                makeOptInEpisode(id: "ep-A"),
                makeOptInEpisode(id: "ep-B")
            ]
        )

        let result = try await coordinator.exportAndPresent()

        #expect(result == .sent)
        #expect(presenter.callCount == 1)
        #expect(sink.calls.count == 1)
        #expect(sink.calls.first?.ids == ["ep-A", "ep-B"])
        #expect(sink.calls.first?.newValue == false)
    }

    @Test(".saved clears opt-in (treated as delivered)")
    func savedClearsOptIn() async throws {
        let presenter = FakePresenter(script: .succeed(.saved))
        let sink = FakeOptInSink()
        let coordinator = DiagnosticsExportCoordinator(
            environment: makeEnvironment(),
            presenter: presenter,
            journalFetch: noopJournalFetch(),
            optInSink: sink,
            optInEpisodes: [makeOptInEpisode(id: "ep-A")]
        )

        let result = try await coordinator.exportAndPresent()

        #expect(result == .saved)
        #expect(sink.calls.count == 1)
        #expect(sink.calls.first?.ids == ["ep-A"])
        #expect(sink.calls.first?.newValue == false)
    }

    // MARK: - Preserve path

    @Test(".cancelled preserves opt-in — sink is NOT called")
    func cancelledPreservesOptIn() async throws {
        let presenter = FakePresenter(script: .succeed(.cancelled))
        let sink = FakeOptInSink()
        let coordinator = DiagnosticsExportCoordinator(
            environment: makeEnvironment(),
            presenter: presenter,
            journalFetch: noopJournalFetch(),
            optInSink: sink,
            optInEpisodes: [makeOptInEpisode(id: "ep-A")]
        )

        let result = try await coordinator.exportAndPresent()

        #expect(result == .cancelled)
        #expect(sink.calls.isEmpty)
    }

    @Test(".failed preserves opt-in — sink is NOT called")
    func failedPreservesOptIn() async throws {
        let presenter = FakePresenter(script: .succeed(.failed))
        let sink = FakeOptInSink()
        let coordinator = DiagnosticsExportCoordinator(
            environment: makeEnvironment(),
            presenter: presenter,
            journalFetch: noopJournalFetch(),
            optInSink: sink,
            optInEpisodes: [makeOptInEpisode(id: "ep-A")]
        )

        let result = try await coordinator.exportAndPresent()

        #expect(result == .failed)
        #expect(sink.calls.isEmpty)
    }

    // MARK: - Opt-in filter

    @Test("sink receives only opted-in episode IDs — unopted entries filtered out")
    func resetOmitsUnoptedEpisodes() async throws {
        let presenter = FakePresenter(script: .succeed(.sent))
        let sink = FakeOptInSink()
        let coordinator = DiagnosticsExportCoordinator(
            environment: makeEnvironment(),
            presenter: presenter,
            journalFetch: noopJournalFetch(),
            optInSink: sink,
            optInEpisodes: [
                makeOptInEpisode(id: "ep-yes-1", diagnosticsOptIn: true),
                makeOptInEpisode(id: "ep-no",    diagnosticsOptIn: false),
                makeOptInEpisode(id: "ep-yes-2", diagnosticsOptIn: true)
            ]
        )

        _ = try await coordinator.exportAndPresent()

        #expect(sink.calls.first?.ids == ["ep-yes-1", "ep-yes-2"])
    }

    @Test(".sent with zero opted-in episodes is a no-op at the sink layer")
    func sentWithNoOptInsIsNoop() async throws {
        let presenter = FakePresenter(script: .succeed(.sent))
        let sink = FakeOptInSink()
        let coordinator = DiagnosticsExportCoordinator(
            environment: makeEnvironment(),
            presenter: presenter,
            journalFetch: noopJournalFetch(),
            optInSink: sink,
            optInEpisodes: []
        )

        _ = try await coordinator.exportAndPresent()

        #expect(sink.calls.isEmpty)
    }

    // MARK: - Presenter errors

    @Test("missingHostViewController propagates out of the presenter layer")
    func missingHostViewControllerPropagates() async throws {
        let presenter = FakePresenter(
            script: .fail(DiagnosticsExportError.missingHostViewController)
        )
        let sink = FakeOptInSink()
        let coordinator = DiagnosticsExportCoordinator(
            environment: makeEnvironment(),
            presenter: presenter,
            journalFetch: noopJournalFetch(),
            optInSink: sink,
            optInEpisodes: [makeOptInEpisode(id: "ep-A")]
        )

        do {
            _ = try await coordinator.exportAndPresent()
            Issue.record("Expected missingHostViewController error to be thrown")
        } catch DiagnosticsExportError.missingHostViewController {
            // expected
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
        // Sink must not fire when the presenter failed to complete.
        #expect(sink.calls.isEmpty)
    }

    // MARK: - Encoded bundle shape

    @Test("buildAndEncode produces a parseable bundle file with correct filename + subject")
    func buildAndEncodeProducesParseableBundle() async throws {
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let presenter = FakePresenter(script: .succeed(.sent))
        let sink = FakeOptInSink()
        let coordinator = DiagnosticsExportCoordinator(
            environment: makeEnvironment(now: now),
            presenter: presenter,
            journalFetch: noopJournalFetch(),
            optInSink: sink,
            optInEpisodes: []
        )

        let (data, filename, subject) = try await coordinator.buildAndEncode()

        // Filename obeys the documented shape.
        #expect(filename.hasPrefix("playhead-diagnostics-"))
        #expect(filename.hasSuffix(".json"))
        #expect(!filename.contains(":"))

        // Subject carries the build type raw value.
        #expect(subject.contains("release"))

        // The encoded bundle round-trips through the standard decoder.
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(DiagnosticsBundleFile.self, from: data)
        #expect(decoded.default.appVersion == "1.0.0")
        #expect(decoded.default.buildType == .release)
        // No opt-in episodes → optIn field omitted.
        #expect(decoded.optIn == nil)
    }

    @Test("journal fetch result is threaded into the default bundle's work_journal_tail")
    func journalFetchPropagates() async throws {
        let presenter = FakePresenter(script: .succeed(.sent))
        let sink = FakeOptInSink()

        let entry = WorkJournalEntry(
            id: "row-1",
            episodeId: "ep-1",
            generationID: UUID(),
            schedulerEpoch: 0,
            timestamp: 1_700_000_000,
            eventType: .acquired,
            cause: nil,
            metadata: "{}",
            artifactClass: .scratch
        )

        let coordinator = DiagnosticsExportCoordinator(
            environment: makeEnvironment(),
            presenter: presenter,
            journalFetch: { [entry] },
            optInSink: sink,
            optInEpisodes: []
        )

        let (data, _, _) = try await coordinator.buildAndEncode()
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(DiagnosticsBundleFile.self, from: data)

        // Scheduler events + tail both reflect the one injected row.
        #expect(decoded.default.schedulerEvents.count == 1)
        #expect(decoded.default.workJournalTail.count == 1)
        // Episode id is hashed in the default bundle — raw id must NOT appear.
        let jsonString = String(decoding: data, as: UTF8.self)
        #expect(!jsonString.contains("\"ep-1\""))
    }
}
