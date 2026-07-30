// AnalysisStoreHealthDiagnosticsPrivacyTests.swift
// The privacy + seam proof for `analysis_store_health`
// (playhead-wvdz), legal checklist item (h).
//
// Modelled on `BannerTallyDiagnosticsPrivacyTests` and
// `StabilityDiagnosticScrubbingTests`. Four claims:
//
//   1. CLOSED SHAPE — the encoded key set is exactly the declared
//      `CodingKeys`, at every level. A future free-text field cannot be
//      added without turning this red.
//   2. SENTINEL SWEEP — a failure whose SQLite message has been stuffed
//      with an episode title and a feed URL, and a quarantine whose
//      record is built from a full container path, both project into a
//      bundle whose encoded bytes contain none of them — while still
//      being COUNTED.
//   3. SEAM — the value actually travels journal → fetch → coordinator →
//      encoded JSON. A signal that is written and never read is the
//      usual way this breaks, and it is the specific way the pre-wvdz
//      arrangement broke.
//   4. THE EXPORT SURVIVES THE FAILURE IT DESCRIBES. This is the one
//      that matters most. `journalFetch` reads the work journal out of
//      `AnalysisStore`, so before playhead-wvdz an unopenable store took
//      the whole export down — and every UI caller wraps it in `try?`,
//      so the "Send diagnostics" button silently did nothing. The
//      artifact that would explain the failure could not be built
//      BECAUSE of the failure.

import Foundation
import Testing

@testable import Playhead

@MainActor
private final class StubHealthPresenter: DiagnosticsExportPresenter {
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
private final class StubHealthOptInSink: DiagnosticsOptInSink {
    func applyResetToEpisodes(matchingEpisodeIds: [String], newValue: Bool) {}
}

private struct DeadStoreError: Error {}

/// Fixtures live outside the `@MainActor` suite so the `@Sendable` fetch
/// closures the coordinator takes can reach them without hopping.
private enum HealthFixture {

    static let now = Date(timeIntervalSince1970: 1_700_000_000)
    static let installID = UUID(uuidString: "22222222-2222-4222-8222-222222222222")!

    /// Content that must never leave the device, shaped into the two
    /// places this record could plausibly carry it: a SQLite error
    /// message (free text from the engine, but `AnalysisStore` appends
    /// our own SQL to it) and a quarantine path (which on a real device
    /// embeds the install UUID and the user's home directory).
    static let sentinelTitle = "SENTINELEPISODE The Mattress Hour"
    static let sentinelFeed = "https://sentinelfeed.example.com/rss.xml"
    static let sentinelContainerPath =
        "/var/mobile/Containers/Data/Application/DEADBEEF-0000-0000-0000-000000000000"

    static func hostileState() -> AnalysisStoreHealthState {
        AnalysisStoreHealthState(
            status: .awaitingUserDecision,
            consecutiveFailureCount: 3,
            firstFailureAt: now,
            lastFailureAt: now,
            lastSuccessAt: nil,
            recentFailures: [
                AnalysisStoreFailureRecord(
                    occurredAt: now,
                    phase: .migration,
                    failureClass: .constraintViolation,
                    consecutiveFailureCount: 3,
                    expectedSchemaVersion: AnalysisStore.currentSchemaVersion,
                    detail: "FOREIGN KEY constraint failed for '\(sentinelTitle)' at \(sentinelFeed)"
                )
            ],
            quarantines: [
                AnalysisStoreQuarantineRecord(
                    quarantinedAt: now,
                    // A caller that mistakenly passed a full path would
                    // be caught by the sentinel sweep below.
                    directoryName: URL(fileURLWithPath: sentinelContainerPath)
                        .appendingPathComponent("AnalysisStore-quarantined-20260730")
                        .lastPathComponent,
                    byteCount: 54_652_928
                )
            ]
        )
    }
}

@Suite("Diagnostics bundle — analysis_store_health (playhead-wvdz, legal item h)")
@MainActor
struct AnalysisStoreHealthDiagnosticsPrivacyTests {

    private static let now = HealthFixture.now
    private static let installID = HealthFixture.installID

    private static func makeCoordinator(
        journalFetch: @escaping DiagnosticsJournalFetch = { [] },
        healthFetch: @escaping DiagnosticsAnalysisStoreHealthFetch
    ) -> DiagnosticsExportCoordinator {
        DiagnosticsExportCoordinator(
            environment: DiagnosticsExportEnvironment(
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
                    capturedAt: now
                ),
                installID: installID,
                now: now
            ),
            presenter: StubHealthPresenter(),
            journalFetch: journalFetch,
            analysisStoreHealthFetch: healthFetch,
            optInSink: StubHealthOptInSink()
        )
    }

    private static func encodedDefaultSubtree(
        _ data: Data
    ) throws -> [String: Any] {
        let root = try #require(
            try JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        return try #require(root["default"] as? [String: Any])
    }

    // MARK: - 1. Closed shape

    @Test("The encoded key set is exactly the declared CodingKeys, at every level")
    func encodedKeySetIsClosed() throws {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(HealthFixture.hostileState())
        let object = try #require(
            try JSONSerialization.jsonObject(with: data) as? [String: Any]
        )

        // FROZEN LITERALS, deliberately not `CodingKeys.allCases` —
        // deriving the allowlist from the type under test is circular,
        // and a new free-text field would widen the allowlist and the
        // encoding in the same edit, leaving the gate green. Adding a
        // field must be a deliberate edit made against this list and
        // against the legal checklist, exactly as it is for a stability
        // record (item e) and a banner tally (item g).
        //
        // SUBSET rather than equality against the frozen list, because
        // `Encodable` omits a nil optional's key entirely and that is
        // the intended encoding: `last_success_at` is absent on a device
        // that has never opened the store, and a rejected `detail` is
        // absent rather than null. The required-key superset check below
        // is what stops "subset" being satisfiable by an empty object.
        #expect(Set(object.keys).isSubset(of: Self.frozenHealthStateKeys))
        #expect(Set(object.keys).isSuperset(of: Self.requiredHealthStateKeys))

        let failures = try #require(object["recent_failures"] as? [[String: Any]])
        #expect(Set(failures[0].keys).isSubset(of: Self.frozenFailureRecordKeys))
        #expect(Set(failures[0].keys).isSuperset(of: Self.requiredFailureRecordKeys))

        let quarantines = try #require(object["quarantines"] as? [[String: Any]])
        #expect(Set(quarantines[0].keys) == Self.frozenQuarantineKeys)
    }

    /// Every key `analysis_store_health` may carry. Hand-written; see the
    /// circularity note in `encodedKeySetIsClosed`.
    private static let frozenHealthStateKeys: Set<String> = [
        "status",
        "consecutive_failure_count",
        "first_failure_at",
        "last_failure_at",
        "last_success_at",
        "recent_failures",
        "quarantines",
        "export_read_failures"
    ]

    /// The subset of the above that is non-optional and must always
    /// encode.
    private static let requiredHealthStateKeys: Set<String> = [
        "status",
        "consecutive_failure_count",
        "recent_failures",
        "quarantines",
        "export_read_failures"
    ]

    private static let frozenFailureRecordKeys: Set<String> = [
        "schema_version",
        "occurred_at",
        "phase",
        "failure_class",
        "consecutive_failure_count",
        "expected_schema_version",
        "detail"
    ]

    private static let requiredFailureRecordKeys: Set<String> = [
        "schema_version",
        "occurred_at",
        "phase",
        "failure_class",
        "consecutive_failure_count",
        "expected_schema_version"
    ]

    private static let frozenQuarantineKeys: Set<String> = [
        "quarantined_at",
        "directory_name",
        "byte_count"
    ]

    /// The frozen lists above are only as good as their agreement with
    /// the declared `CodingKeys`. This is the one place the two are
    /// compared, and it is not circular: it asserts that the hand-written
    /// list is COMPLETE, so a new field turns THIS red (pointing the
    /// author at the checklist) rather than silently riding out in the
    /// shape test above.
    @Test("The frozen key lists still describe the declared CodingKeys")
    func frozenListsMatchDeclaredCodingKeys() {
        #expect(
            Set(AnalysisStoreHealthState.CodingKeys.allCases.map(\.rawValue))
                == Self.frozenHealthStateKeys
        )
        #expect(
            Set(AnalysisStoreFailureRecord.CodingKeys.allCases.map(\.rawValue))
                == Self.frozenFailureRecordKeys
        )
        #expect(
            Set(AnalysisStoreQuarantineRecord.CodingKeys.allCases.map(\.rawValue))
                == Self.frozenQuarantineKeys
        )
    }

    // MARK: - 2. Sentinel sweep

    @Test("Hostile content is absent from the encoded bundle while the failure is still counted")
    func sentinelContentNeverReachesTheBundle() async throws {
        let coordinator = Self.makeCoordinator(healthFetch: { HealthFixture.hostileState() })
        let (data, _, _) = try await coordinator.buildAndEncode()
        let text = try #require(String(data: data, encoding: .utf8))

        #expect(!text.contains("SENTINELEPISODE"))
        #expect(!text.contains("Mattress"))
        #expect(!text.contains("sentinelfeed.example.com"))
        #expect(!text.contains("/var/mobile/Containers"))
        #expect(!text.contains("DEADBEEF"))

        // The point is not silence — it is that the incident is still
        // reported. A sweep that passed because nothing was emitted
        // would prove nothing.
        let subtree = try Self.encodedDefaultSubtree(data)
        let health = try #require(subtree["analysis_store_health"] as? [String: Any])
        #expect(health["status"] as? String == "awaiting_user_decision")
        #expect(health["consecutive_failure_count"] as? Int == 3)
        let failures = try #require(health["recent_failures"] as? [[String: Any]])
        #expect(failures.count == 1)
        #expect(failures[0]["failure_class"] as? String == "constraint_violation")
        // The message was rejected wholesale rather than truncated — a
        // truncated leak is still a leak, and `failure_class` is what
        // carries the diagnosis.
        #expect(failures[0]["detail"] == nil)
        let quarantines = try #require(health["quarantines"] as? [[String: Any]])
        #expect(quarantines[0]["byte_count"] as? Int64 == 54_652_928)
    }

    @Test("The default subtree carries no episode reference of any kind")
    func noEpisodeReference() async throws {
        let coordinator = Self.makeCoordinator(healthFetch: { HealthFixture.hostileState() })
        let (data, _, _) = try await coordinator.buildAndEncode()
        let subtree = try Self.encodedDefaultSubtree(data)
        let health = try #require(subtree["analysis_store_health"] as? [String: Any])
        let encoded = try JSONSerialization.data(withJSONObject: health)
        let text = try #require(String(data: encoded, encoding: .utf8))
        // Not even a hash: a database that will not open has no episode
        // to name, so there is nothing legitimate this substring could
        // be.
        #expect(!text.lowercased().contains("episode"))
    }

    // MARK: - 3. Seam

    @Test("A healthy device still emits the key, so the signal is distinguishable from its absence")
    func healthyDeviceStillEmitsTheKey() async throws {
        let coordinator = Self.makeCoordinator(healthFetch: { .healthy })
        let (data, _, _) = try await coordinator.buildAndEncode()
        let subtree = try Self.encodedDefaultSubtree(data)
        let health = try #require(subtree["analysis_store_health"] as? [String: Any])
        #expect(health["status"] as? String == "healthy")
        #expect(health["consecutive_failure_count"] as? Int == 0)
    }

    @Test("The live journal reaches the bundle through the production adapter shape")
    func journalReachesTheBundle() async throws {
        let dir = try makeTempDir(prefix: "AnalysisStoreHealthSeam")
        let journal = AnalysisStoreHealthJournal(directory: dir)
        await journal.recordFailure(
            error: AnalysisStoreError.migrationFailed("FOREIGN KEY constraint failed"),
            now: Self.now
        )

        let coordinator = Self.makeCoordinator(healthFetch: { await journal.load() })
        let (data, _, _) = try await coordinator.buildAndEncode()
        let subtree = try Self.encodedDefaultSubtree(data)
        let health = try #require(subtree["analysis_store_health"] as? [String: Any])
        #expect(health["consecutive_failure_count"] as? Int == 1)
        let failures = try #require(health["recent_failures"] as? [[String: Any]])
        #expect(failures[0]["failure_class"] as? String == "constraint_violation")
        #expect(failures[0]["detail"] as? String == "FOREIGN KEY constraint failed")
    }

    // MARK: - 4. The export survives the failure it describes

    /// THE LOAD-BEARING TEST. `journalFetch` reads the work journal out
    /// of `AnalysisStore`. Before playhead-wvdz the call was
    /// `try await journalFetch()`, unguarded, so an unopenable store
    /// threw out of `buildAndEncode` → out of `exportAndPresent` → into
    /// the `try?` every UI caller wraps it in. The button did nothing,
    /// silently, exactly when the bundle was most needed.
    @Test("A dead analysis store no longer takes the whole export down")
    func exportSurvivesADeadStore() async throws {
        let coordinator = Self.makeCoordinator(
            journalFetch: { throw AnalysisStoreError.openFailed(code: 26, message: "file is not a database") },
            healthFetch: { .healthy }
        )

        // Before the fix this call threw and there was no bundle at all.
        let (data, _, _) = try await coordinator.buildAndEncode()
        let subtree = try Self.encodedDefaultSubtree(data)

        // The read is NAMED, not silently empty. An unreadable journal
        // and an empty journal are different facts, and a bare `try?`
        // would have made them the same bundle.
        let health = try #require(subtree["analysis_store_health"] as? [String: Any])
        let readFailures = try #require(health["export_read_failures"] as? [String])
        #expect(readFailures == [AnalysisStoreHealthState.ExportRead.workJournal.rawValue])

        // And the rest of the bundle is intact rather than absent.
        #expect(subtree["app_version"] as? String == "1.0.0")
        #expect((subtree["work_journal_tail"] as? [Any])?.isEmpty == true)
    }

    @Test("A healthy export names no failed reads")
    func healthyExportNamesNoFailedReads() async throws {
        let coordinator = Self.makeCoordinator(healthFetch: { .healthy })
        let (data, _, _) = try await coordinator.buildAndEncode()
        let subtree = try Self.encodedDefaultSubtree(data)
        let health = try #require(subtree["analysis_store_health"] as? [String: Any])
        #expect((health["export_read_failures"] as? [String])?.isEmpty == true)
    }

    /// The export-read failures are an export-time observation, not
    /// state. Persisting them would make a one-off unreadable table look
    /// like a standing condition on every later bundle.
    @Test("Export read failures are never written back to the persisted document")
    func exportReadFailuresAreNotPersisted() async throws {
        let dir = try makeTempDir(prefix: "AnalysisStoreHealthNoPersist")
        let journal = AnalysisStoreHealthJournal(directory: dir)
        await journal.recordFailure(
            error: AnalysisStoreError.migrationFailed("FOREIGN KEY constraint failed"),
            now: Self.now
        )
        let coordinator = Self.makeCoordinator(
            journalFetch: { throw DeadStoreError() },
            healthFetch: { await journal.load() }
        )
        _ = try await coordinator.buildAndEncode()

        let reloaded = await AnalysisStoreHealthJournal(directory: dir).load()
        #expect(reloaded.exportReadFailures.isEmpty)
    }
}
