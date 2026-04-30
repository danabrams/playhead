// InstallIDProviderCoordinatorIntegrationTests.swift
// playhead-fsy3 Scope 1 — integration proof that the per-install UUID
// provisioned by ``InstallIDProvider`` flows through the production
// `DiagnosticsExportEnvironment` into the encoded bundle's salted hash.
//
// Why this test exists:
//   The InstallIDProvider type was landed in playhead-ghon. The
//   production wiring landed in playhead-ct2q (DEBUG hatch) and
//   playhead-l274 (Release hatch) — both call sites invoke
//   `InstallIDProvider(context: modelContext).installID()` and pass the
//   UUID into `DiagnosticsExportEnvironment(installID:)`. This test is
//   the integration-level proof that the bytes flow end-to-end:
//
//     SwiftData ModelContext
//        → InstallIDProvider.installID()    (UUID)
//        → DiagnosticsExportEnvironment(installID:)
//        → DiagnosticsExportCoordinator.buildAndEncode()
//        → encoded JSON bundle
//        → episode_id_hash matches SHA-256(installID.uuidString || episodeId)
//
//   Existing per-component tests (`InstallIdentityTests`,
//   `EpisodeIdHasherTests`, `DiagnosticsBundleBuilderTests`) cover each
//   leg in isolation; this suite proves the wiring is intact.
//
// What this test deliberately does NOT cover:
//   * UI exposure of the install ID (out of scope per bead — Phase 2
//     l274 owns the visible surface).
//   * The two production hatches (`runDebugDiagnosticsExport`,
//     `runReleaseDiagnosticsExport`) themselves — those are exercised by
//     `DebugDiagnosticsHatchTests` and `SettingsL274Tests`. Re-driving
//     them here would require an `MFMailComposeViewController`, which is
//     simulator-hostile.

import Foundation
import SwiftData
import Testing

@testable import Playhead

@Suite("InstallIDProvider → DiagnosticsExportCoordinator integration (playhead-fsy3 Scope 1)")
@MainActor
struct InstallIDProviderCoordinatorIntegrationTests {

    // MARK: - Stubs (local; mirror the existing redaction-suite pattern)

    @MainActor
    private final class StubPresenter: DiagnosticsExportPresenter {
        func present(
            data: Data,
            filename: String,
            subject: String,
            completion: @escaping @MainActor (Result<DiagnosticsMailComposeResult, Error>) -> Void
        ) {
            completion(.success(.sent))
        }
    }

    @MainActor
    private final class StubOptInSink: DiagnosticsOptInSink {
        func applyResetToEpisodes(matchingEpisodeIds: [String], newValue: Bool) {}
    }

    // MARK: - Helpers

    private func makeInMemoryContext() throws -> ModelContext {
        let schema = Schema([InstallIdentity.self])
        let config = ModelConfiguration(isStoredInMemoryOnly: true, cloudKitDatabase: .none)
        let container = try ModelContainer(for: schema, configurations: [config])
        return ModelContext(container)
    }

    private func makeEnvironment(
        installID: UUID,
        now: Date = Date(timeIntervalSince1970: 1_700_000_000)
    ) -> DiagnosticsExportEnvironment {
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
            installID: installID,
            now: now
        )
    }

    // MARK: - End-to-end: provider → coordinator → encoded bundle

    @Test("Provider-derived UUID is the salt for every episode_id_hash in the encoded bundle")
    func providerUUIDFlowsIntoSaltedHash() async throws {
        // 1. Provision the install UUID from a real SwiftData context —
        //    the same call shape `runDebugDiagnosticsExport` /
        //    `runReleaseDiagnosticsExport` use in production.
        let context = try makeInMemoryContext()
        let provider = InstallIDProvider(context: context)
        let installID = try provider.installID()

        // 2. Build an environment with that UUID and drive the
        //    coordinator with seeded journal entries that reference
        //    real episode IDs. The coordinator should hash those IDs
        //    using the provider-derived salt.
        let rawEpisodeIds = ["ep-fsy3-A", "ep-fsy3-B"]
        let entries: [WorkJournalEntry] = rawEpisodeIds.enumerated().map { idx, id in
            WorkJournalEntry(
                id: "row-\(idx)",
                episodeId: id,
                generationID: UUID(),
                schedulerEpoch: 0,
                timestamp: 1_700_000_000 + Double(idx),
                eventType: .acquired,
                cause: nil,
                metadata: "{}",
                artifactClass: .scratch
            )
        }
        let coordinator = DiagnosticsExportCoordinator(
            environment: makeEnvironment(installID: installID),
            presenter: StubPresenter(),
            journalFetch: { entries },
            optInSink: StubOptInSink(),
            optInEpisodes: []
        )

        let (data, _, _) = try await coordinator.buildAndEncode()

        // 3. Decode and assert the hash matches what `EpisodeIdHasher`
        //    produces for the SAME provider-derived install UUID. This
        //    is the integration-level "salt provenance" check — if the
        //    wiring ever drops the install UUID and falls back to a
        //    hard-coded constant, this assertion fails.
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(DiagnosticsBundleFile.self, from: data)

        #expect(decoded.default.workJournalTail.count == rawEpisodeIds.count)
        let expected = rawEpisodeIds.map { id in
            EpisodeIdHasher.hash(installID: installID, episodeId: id)
        }
        let actual = decoded.default.workJournalTail.map(\.episodeIdHash)
        #expect(Set(actual) == Set(expected),
                "every episode_id_hash in the bundle must equal SHA-256(provider.installID || episodeId)")

        // Belt-and-suspenders: the raw episode ID must NOT appear
        // anywhere in the encoded bytes (the default bundle is
        // hash-only).
        let jsonString = String(decoding: data, as: UTF8.self)
        for raw in rawEpisodeIds {
            #expect(
                !jsonString.contains(raw),
                "raw episodeId '\(raw)' leaked into the encoded default-bundle JSON"
            )
        }
    }

    @Test("A different provider-derived UUID produces a different bundle hash for the same episode")
    func differentProviderProducesDifferentHash() async throws {
        // Two separate SwiftData contexts simulate two installs. Each
        // provisions its own UUID; passing each UUID through the
        // coordinator must produce DIFFERENT hashes for the same raw
        // episode ID. This is the per-install-salt property at the
        // bundle boundary.
        let ctx1 = try makeInMemoryContext()
        let ctx2 = try makeInMemoryContext()
        let installA = try InstallIDProvider(context: ctx1).installID()
        let installB = try InstallIDProvider(context: ctx2).installID()
        #expect(installA != installB,
                "Two separate contexts must provision distinct UUIDs (preconditions check)")

        let entry = WorkJournalEntry(
            id: "row-shared",
            episodeId: "ep-fsy3-shared",
            generationID: UUID(),
            schedulerEpoch: 0,
            timestamp: 1_700_000_000,
            eventType: .acquired,
            cause: nil,
            metadata: "{}",
            artifactClass: .scratch
        )

        func encodedHash(installID: UUID) async throws -> String {
            let coordinator = DiagnosticsExportCoordinator(
                environment: makeEnvironment(installID: installID),
                presenter: StubPresenter(),
                journalFetch: { [entry] },
                optInSink: StubOptInSink(),
                optInEpisodes: []
            )
            let (data, _, _) = try await coordinator.buildAndEncode()
            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601
            let decoded = try decoder.decode(DiagnosticsBundleFile.self, from: data)
            return try #require(decoded.default.workJournalTail.first?.episodeIdHash)
        }

        let hashA = try await encodedHash(installID: installA)
        let hashB = try await encodedHash(installID: installB)
        #expect(hashA != hashB,
                "Different provider-derived UUIDs must yield different bundle hashes for the same episode")
    }

    @Test("Provider re-fetched on the same context produces the same bundle hash (stability)")
    func providerStableAcrossFetches() async throws {
        // A fresh `InstallIDProvider` over the same SwiftData context
        // must observe the same persisted UUID — and therefore produce
        // the same bundle hash. This is the production guarantee that
        // two diagnostic exports from the same install correlate.
        let context = try makeInMemoryContext()
        let installA = try InstallIDProvider(context: context).installID()
        let installB = try InstallIDProvider(context: context).installID()
        #expect(installA == installB,
                "Same context must be stable across provider re-fetches")

        let entry = WorkJournalEntry(
            id: "row-shared",
            episodeId: "ep-fsy3-shared",
            generationID: UUID(),
            schedulerEpoch: 0,
            timestamp: 1_700_000_000,
            eventType: .acquired,
            cause: nil,
            metadata: "{}",
            artifactClass: .scratch
        )

        func encodedHash(installID: UUID) async throws -> String {
            let coordinator = DiagnosticsExportCoordinator(
                environment: makeEnvironment(installID: installID),
                presenter: StubPresenter(),
                journalFetch: { [entry] },
                optInSink: StubOptInSink(),
                optInEpisodes: []
            )
            let (data, _, _) = try await coordinator.buildAndEncode()
            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601
            let decoded = try decoder.decode(DiagnosticsBundleFile.self, from: data)
            return try #require(decoded.default.workJournalTail.first?.episodeIdHash)
        }

        let hashA = try await encodedHash(installID: installA)
        let hashB = try await encodedHash(installID: installB)
        #expect(hashA == hashB)
    }
}
