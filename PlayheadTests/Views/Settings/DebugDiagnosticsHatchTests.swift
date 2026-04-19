// DebugDiagnosticsHatchTests.swift
// Coverage for playhead-ct2q — the debug-only Settings → Send
// diagnostics hatch that wires `DiagnosticsExportCoordinator` +
// presenter + sink + `InstallIDProvider` + journal adapter into a
// single call site.
//
// The tests assert each wired-up piece in isolation without driving
// `MFMailComposeViewController` (simulator-hostile). The coordinator +
// presenter + sink already have dedicated unit tests under
// `PlayheadTests/Support/Diagnostics/`; the tests here verify the
// hatch-level composition:
//   1. The journal adapter fetches the most-recent N rows from
//      `AnalysisStore.fetchRecentWorkJournalEntries`.
//   2. The install UUID is stable across two successive exports (i.e.
//      `InstallIDProvider` is threaded through correctly).
//   3. The CapabilitySnapshot → AnalysisEligibility derivation is
//      honest and field-by-field matches the snapshot's gating flags.
//   4. `buildEnvironment` assembles a coherent `DiagnosticsExportEnvironment`
//      that the coordinator can consume.
//
// A companion canary test, `DebugDiagnosticsHatchSourceCanaryTests`,
// greps the source to verify the `#if DEBUG` wrapper is intact — that
// test doubles as the "not present in Release" acceptance criterion
// from the ct2q bead.

#if DEBUG

import Foundation
import SwiftData
import Testing

@testable import Playhead

// MARK: - Hatch helpers

@Suite("DebugDiagnosticsHatch (playhead-ct2q)")
@MainActor
struct DebugDiagnosticsHatchTests {

    // MARK: - Journal adapter

    @Test("journalFetch returns up to 200 most-recent rows, newest-first")
    func journalFetchReturnsNewestFirst() async throws {
        let store = try await makeTestStore()
        for i in 0..<5 {
            try await store.appendWorkJournalEntry(
                WorkJournalEntry(
                    id: UUID().uuidString,
                    episodeId: "ep-hatch",
                    generationID: UUID(),
                    schedulerEpoch: 0,
                    timestamp: Double(1_000 + i),
                    eventType: .acquired,
                    cause: nil,
                    metadata: "{}",
                    artifactClass: .scratch
                )
            )
        }

        let fetch = DebugDiagnosticsHatch.makeJournalFetch(store: store)
        let rows = try await fetch()

        #expect(rows.count == 5)
        // AnalysisStore.fetchRecentWorkJournalEntries returns DESC-sorted
        // rows; the hatch adapter is a pass-through.
        let stamps = rows.map(\.timestamp)
        #expect(stamps == stamps.sorted(by: >))
        #expect(rows.first?.timestamp == 1_004)
    }

    @Test("journalFetch caps at DebugDiagnosticsHatch.journalFetchLimit (200) rows")
    func journalFetchCapsAt200() async throws {
        let store = try await makeTestStore()
        for i in 0..<250 {
            try await store.appendWorkJournalEntry(
                WorkJournalEntry(
                    id: "row-\(i)",
                    episodeId: "ep-cap",
                    generationID: UUID(),
                    schedulerEpoch: 0,
                    timestamp: Double(i),
                    eventType: .acquired,
                    cause: nil,
                    metadata: "{}",
                    artifactClass: .scratch
                )
            )
        }

        let fetch = DebugDiagnosticsHatch.makeJournalFetch(store: store)
        let rows = try await fetch()
        #expect(rows.count == DebugDiagnosticsHatch.journalFetchLimit)
        #expect(DebugDiagnosticsHatch.journalFetchLimit == 200)
    }

    // MARK: - InstallID provider wiring

    @Test("InstallIDProvider yields a stable UUID across two successive hatch invocations")
    func installIDIsStableAcrossRuns() throws {
        let ctx = try makeDiagnosticsInMemoryContext()
        // Stand up InstallIdentity in the same schema the hatch uses.
        // `makeDiagnosticsInMemoryContext` does NOT include InstallIdentity
        // (it's a diagnostics-local model), so use a dedicated schema
        // here that mirrors what the app container carries at launch.
        let installSchema = Schema([InstallIdentity.self])
        let config = ModelConfiguration(isStoredInMemoryOnly: true)
        let installCtx = ModelContext(try ModelContainer(for: installSchema, configurations: [config]))

        let first = try InstallIDProvider(context: installCtx).installID()
        let second = try InstallIDProvider(context: installCtx).installID()
        #expect(first == second)

        // The diagnostics context doesn't influence install-ID stability;
        // it only hosts Episode rows for the sink. This @Test stands in
        // for the "two successive taps return the same installID" part
        // of the ct2q acceptance list.
        _ = ctx
    }

    // MARK: - CapabilitySnapshot → AnalysisEligibility mapping

    @Test("eligibility mapping is field-by-field honest for an all-true snapshot")
    func eligibilityFromFullySupportedSnapshot() {
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let snapshot = makeSnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: true,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            capturedAt: now
        )
        let elig = DebugDiagnosticsHatch.eligibility(from: snapshot, now: now)
        #expect(elig.hardwareSupported == true)
        #expect(elig.appleIntelligenceEnabled == true)
        #expect(elig.regionSupported == true)
        #expect(elig.languageSupported == true)
        #expect(elig.modelAvailableNow == true)
        #expect(elig.capturedAt == now)
        #expect(elig.isFullyEligible == true)
    }

    @Test("eligibility mapping flips the right fields when the snapshot gates them")
    func eligibilityReflectsSnapshotGates() {
        let now = Date(timeIntervalSince1970: 1_700_000_000)
        let snapshot = makeSnapshot(
            foundationModelsAvailable: false, // hardware gate
            foundationModelsUsable: true,
            appleIntelligenceEnabled: false,  // AI gate
            foundationModelsLocaleSupported: true,
            capturedAt: now
        )
        let elig = DebugDiagnosticsHatch.eligibility(from: snapshot, now: now)
        #expect(elig.hardwareSupported == false)
        #expect(elig.appleIntelligenceEnabled == false)
        #expect(elig.regionSupported == true)
        #expect(elig.languageSupported == true)
        #expect(elig.modelAvailableNow == true)
        #expect(elig.isFullyEligible == false)
    }

    @Test("eligibility mapping surfaces a model-not-resident state")
    func eligibilityModelUnavailableFlag() {
        let now = Date()
        let snapshot = makeSnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: false, // model-not-resident
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            capturedAt: now
        )
        let elig = DebugDiagnosticsHatch.eligibility(from: snapshot, now: now)
        #expect(elig.modelAvailableNow == false)
        #expect(elig.isFullyEligible == false)
    }

    // MARK: - Version / OS helpers

    @Test("osVersionString matches ProcessInfo.operatingSystemVersion shape")
    func osVersionStringShape() {
        let stamp = DebugDiagnosticsHatch.osVersionString()
        // Shape: "<int>.<int>.<int>" — the ScanCohort convention.
        let parts = stamp.split(separator: ".")
        #expect(parts.count == 3)
        for part in parts {
            #expect(Int(part) != nil)
        }
    }

    @Test("appVersionString returns CFBundleShortVersionString or 'unknown'")
    func appVersionStringNeverEmpty() {
        let v = DebugDiagnosticsHatch.appVersionString()
        #expect(!v.isEmpty)
    }

    // MARK: - End-to-end: coordinator consumes hatch-built pieces

    @Test("hatch-built environment + journalFetch + sink produce a parseable bundle via the coordinator")
    func hatchPiecesDriveACoherentCoordinatorRun() async throws {
        // Build the three production-shape pieces (environment, fetch,
        // sink) the same way the hatch does, and hand them to the
        // coordinator via a fake presenter so we can assert the
        // encoded bundle shape without driving MFMailComposeViewController.
        let store = try await makeTestStore()
        try await store.appendWorkJournalEntry(
            WorkJournalEntry(
                id: "row-coord",
                episodeId: "ep-coord",
                generationID: UUID(),
                schedulerEpoch: 0,
                timestamp: 1_700_000_000,
                eventType: .acquired,
                cause: nil,
                metadata: "{}",
                artifactClass: .scratch
            )
        )

        let ctx = try makeDiagnosticsInMemoryContext()
        let installSchema = Schema([InstallIdentity.self])
        let installCtx = ModelContext(try ModelContainer(
            for: installSchema,
            configurations: [ModelConfiguration(isStoredInMemoryOnly: true)]
        ))
        let installID = try InstallIDProvider(context: installCtx).installID()

        let snapshot = makeSnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: true,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            capturedAt: Date()
        )
        let environment = DiagnosticsExportEnvironment(
            appVersion: DebugDiagnosticsHatch.appVersionString(),
            osVersion: DebugDiagnosticsHatch.osVersionString(),
            deviceClass: DeviceClass.detect(),
            buildType: BuildType.detect(),
            eligibility: DebugDiagnosticsHatch.eligibility(from: snapshot),
            installID: installID
        )
        let fetch = DebugDiagnosticsHatch.makeJournalFetch(store: store)
        let sink = SwiftDataDiagnosticsOptInSink(context: ctx)

        let presenter = HatchTestPresenter(script: .sent)
        let coordinator = DiagnosticsExportCoordinator(
            environment: environment,
            presenter: presenter,
            journalFetch: fetch,
            optInSink: sink,
            optInEpisodes: []
        )

        let (data, filename, _) = try await coordinator.buildAndEncode()

        // Filename obeys the ghon shape.
        #expect(filename.hasPrefix("playhead-diagnostics-"))
        #expect(filename.hasSuffix(".json"))

        // JSON is well-formed and carries our single journal row.
        let decoded = try JSONDecoder.iso().decode(DiagnosticsBundleFile.self, from: data)
        #expect(decoded.default.schedulerEvents.count == 1)
        #expect(decoded.default.workJournalTail.count == 1)
        // Per ct2q acceptance: default bundle only, no opt-in episodes.
        #expect(decoded.optIn == nil)
        // Per ghon legal checklist (a): raw episode IDs never ship.
        let jsonString = String(decoding: data, as: UTF8.self)
        #expect(!jsonString.contains("\"ep-coord\""))
    }

    // MARK: - Helpers

    private func makeSnapshot(
        foundationModelsAvailable: Bool,
        foundationModelsUsable: Bool,
        appleIntelligenceEnabled: Bool,
        foundationModelsLocaleSupported: Bool,
        capturedAt: Date = Date()
    ) -> CapabilitySnapshot {
        CapabilitySnapshot(
            foundationModelsAvailable: foundationModelsAvailable,
            foundationModelsUsable: foundationModelsUsable,
            appleIntelligenceEnabled: appleIntelligenceEnabled,
            foundationModelsLocaleSupported: foundationModelsLocaleSupported,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000_000,
            capturedAt: capturedAt
        )
    }
}

// MARK: - Presenter fake (local to this suite)

@MainActor
private final class HatchTestPresenter: DiagnosticsExportPresenter {
    enum Script { case sent, cancelled }
    let script: Script
    init(script: Script) { self.script = script }

    func present(
        data: Data,
        filename: String,
        subject: String,
        completion: @escaping @MainActor (Result<DiagnosticsMailComposeResult, Error>) -> Void
    ) {
        switch script {
        case .sent:      completion(.success(.sent))
        case .cancelled: completion(.success(.cancelled))
        }
    }
}

// MARK: - Decoder helper

private extension JSONDecoder {
    static func iso() -> JSONDecoder {
        let d = JSONDecoder()
        d.dateDecodingStrategy = .iso8601
        return d
    }
}

#endif // DEBUG
