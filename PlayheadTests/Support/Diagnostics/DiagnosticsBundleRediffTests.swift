// DiagnosticsBundleRediffTests.swift
// playhead-p70f: `DiagnosticsBundle.swift` contained ZERO rediff references, so
// three tables of decisive telemetry were written on device and never exported.
// Establishing that day-0 had spent 299.6 MB while producing no ad windows
// required pulling the raw SQLite database off the phone; it should have taken
// a support bundle.
//
// These pin the `rediff_diagnostics` projection:
//
//   * the key is ALWAYS emitted (an empty lane is distinguishable from a bundle
//     that predates the lane);
//   * every counter and per-asset field survives the projection;
//   * the raw `analysisAssetId` NEVER reaches the JSON — only the install-scoped
//     hash, same as `music_bed_profiles` and `banner_tallies` (legal item a);
//   * the day-0 `lastDetail` free text is NOT exported (a `URLError` carries the
//     enclosure URL, which is a stronger disclosure than the raw episode id the
//     checklist already forbids);
//   * `background_runs` carries only the rediff entry point.

import Foundation
import Testing

@testable import Playhead

@Suite("DiagnosticsBundle — rediff_diagnostics (playhead-p70f)")
struct DiagnosticsBundleRediffTests {

    private static let installID = UUID(uuidString: "22222222-2222-2222-2222-222222222222")!
    private static let now = Date(timeIntervalSince1970: 1_700_000_000)

    private static let eligible = AnalysisEligibility(
        hardwareSupported: true, appleIntelligenceEnabled: true,
        regionSupported: true, languageSupported: true,
        modelAvailableNow: true, capturedAt: now
    )

    private func build(_ rediff: DiagnosticsRediffSnapshot) -> DefaultBundle {
        DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 27", deviceClass: .iPhone17Pro,
            buildType: .debug, eligibility: Self.eligible, workJournalEntries: [],
            installID: Self.installID, rediff: rediff
        )
    }

    private func encodedJSON(_ bundle: DefaultBundle) throws -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        return String(decoding: try encoder.encode(bundle), as: UTF8.self)
    }

    // MARK: - Presence

    @Test("the rediff_diagnostics key is ALWAYS emitted, even for an untouched lane")
    func keyIsAlwaysPresent() throws {
        let json = try encodedJSON(build(.empty))
        #expect(json.contains("\"rediff_diagnostics\""))
        #expect(json.contains("\"day_zero_attempts\""))
        #expect(json.contains("\"refetch_states\""))
        #expect(json.contains("\"background_runs\""))
        #expect(json.contains("\"bandwidth\""))
    }

    @Test("a legacy bundle without the key decodes as the empty lane")
    func legacyBundleDecodes() throws {
        let bundle = build(.empty)
        var object = try JSONSerialization.jsonObject(
            with: try JSONEncoder().encode(bundle)
        ) as! [String: Any]
        object.removeValue(forKey: "rediff_diagnostics")
        let trimmed = try JSONSerialization.data(withJSONObject: object)
        let decoded = try JSONDecoder().decode(DefaultBundle.self, from: trimmed)
        #expect(decoded.rediffDiagnostics == .empty)
    }

    // MARK: - The defect, made visible

    @Test("THE DEFECT: 299.6 MB with zero outcomes is now readable straight off the bundle")
    func theDefectIsVisible() throws {
        let snapshot = DiagnosticsRediffSnapshot(
            bandwidth: RediffBandwidthTotals(
                precheckBytesTotal: 0, fullFetchBytesTotal: 299_600_000,
                unchangedCount: 0, rotatedCount: 0, failedCount: 0, parkedCount: 0,
                dayZeroUnmarkedCount: 3, lastUpdatedAt: 1_700_000_000
            ),
            dayZeroAttempts: [
                RediffDayZeroAttemptRecord(
                    analysisAssetId: "asset-real-id", attemptCount: 3, lastAttemptAt: 1_700_000_000,
                    lastExit: .noAcceptedByteDiff, lastBSideCount: 2, lastBSidesAccepted: 0,
                    lastBSidesGateRejected: 2, lastFullFetchBytes: 108_000_000,
                    totalFullFetchBytes: 299_600_000, suppressedCount: 4,
                    lastSuppressedAt: 1_700_000_500, lastDetail: "https://cdn.example.com/secret.mp3"
                )
            ]
        )
        let bundle = build(snapshot)

        #expect(bundle.rediffDiagnostics.bandwidth.fullFetchBytesTotal == 299_600_000)
        // The counter that was structurally unreachable before playhead-p70f.
        #expect(bundle.rediffDiagnostics.bandwidth.dayZeroUnmarkedCount == 3)
        guard let row = bundle.rediffDiagnostics.dayZeroAttempts.first else {
            Issue.record("expected a day-0 row"); return
        }
        // The diagnosis: the byte aligner rejected EVERY copy. Distinct from
        // "diffs accepted, copies agreed" — which was the whole ambiguity.
        #expect(row.lastExit == "no_accepted_byte_diff")
        #expect(row.lastBSidesGateRejected == 2)
        #expect(row.lastBSidesAccepted == 0)
        #expect(row.totalFullFetchBytes == 299_600_000)
        #expect(row.suppressedCount == 4)
        // Review round 1: the budget is generation-scoped, so a support
        // engineer reading `attempt_count == 3` needs to know WHICH generation
        // spent it — an exhausted budget stamped below the shipping build's
        // generation is already eligible again.
        #expect(row.policyGeneration == DayZeroRediffAttemptPolicy.currentGeneration)
    }

    // MARK: - Privacy

    @Test("the raw analysisAssetId never reaches the JSON — only the install-scoped hash")
    func assetIdIsHashed() throws {
        let snapshot = DiagnosticsRediffSnapshot(
            refetchStates: [RediffRefetchStateRow(
                analysisAssetId: "raw-asset-identifier-9c1",
                attemptState: RediffRefetchPolicy.AttemptState(
                    unchangedAttempts: 2, lastAttemptAt: 100, resolved: false,
                    lastFailureClass: .transient, sameClassFailureStreak: 1
                ),
                updatedAt: 200
            )],
            dayZeroAttempts: [RediffDayZeroAttemptRecord(
                analysisAssetId: "raw-asset-identifier-9c1", attemptCount: 1,
                lastAttemptAt: 300, lastExit: .noDivergentSlot
            )]
        )
        let json = try encodedJSON(build(snapshot))

        #expect(!json.contains("raw-asset-identifier-9c1"),
                "legal checklist item (a): the raw local identifier must not ship")
        let expected = EpisodeIdHasher.hash(installID: Self.installID, episodeId: "raw-asset-identifier-9c1")
        #expect(json.contains(expected))
        // Both projections use the SAME hash, so rows correlate inside a bundle.
        #expect(build(snapshot).rediffDiagnostics.refetchStates.first?.assetIdHash
                == build(snapshot).rediffDiagnostics.dayZeroAttempts.first?.assetIdHash)
    }

    @Test("the day-0 lastDetail free text is NOT exported (a URLError carries the enclosure URL)")
    func detailIsNotExported() throws {
        let snapshot = DiagnosticsRediffSnapshot(
            dayZeroAttempts: [RediffDayZeroAttemptRecord(
                analysisAssetId: "a1", attemptCount: 1, lastAttemptAt: 1,
                lastExit: .fetchFailed,
                lastDetail: "https://traffic.megaphone.fm/PRIVATE-EPISODE-9137.mp3"
            )]
        )
        let json = try encodedJSON(build(snapshot))
        #expect(!json.contains("megaphone.fm"))
        #expect(!json.contains("PRIVATE-EPISODE-9137"))
        #expect(!json.contains("last_detail"))
        // The closed exit enum — which is what a support engineer needs — DOES ship.
        #expect(json.contains("fetch_failed"))
    }

    // MARK: - Background runs

    @Test("background_runs carries ONLY the rediff entry point")
    func backgroundRunsAreScopedToRediff() {
        func run(_ entry: BackgroundTaskRunEntryPoint, startedAt: Double) -> BackgroundTaskRunRecord {
            BackgroundTaskRunRecord(
                runId: "\(entry.rawValue)-\(startedAt)", entryPoint: entry,
                taskIdentifier: "com.playhead.app.\(entry.rawValue)",
                startedAt: startedAt, outcome: .noEligibleWork,
                deferReason: "precheckBytes=0 fullFetchBytes=0"
            )
        }
        let snapshot = DiagnosticsRediffSnapshot(backgroundRuns: [
            run(.rediffRefetch, startedAt: 100),
            run(.backfill, startedAt: 200),
            run(.rediffRefetch, startedAt: 300)
        ])
        let projected = build(snapshot).rediffDiagnostics.backgroundRuns

        #expect(projected.count == 2, "the backfill row must not leak into the rediff lane")
        #expect(projected.map(\.startedAt) == [300, 100], "newest first")
        #expect(projected.first?.outcome == "no_eligible_work")
        // The ledger's free-form bandwidth annotation is parsed into INTEGERS,
        // not forwarded as text. `DiagnosticTextSanitizer`'s allowlist has no
        // `=`, so forwarding would have dropped the annotation entirely — and
        // "the ONE lagged fire transferred ZERO bytes" is precisely the fact
        // that told playhead-p70f the 299.6 MB was day-0's, not the sweep's.
        #expect(projected.first?.precheckBytes == 0)
        #expect(projected.first?.fullFetchBytes == 0)
    }

    @Test("the bandwidth annotation is parsed to integers; no free text crosses the boundary")
    func annotationIsParsedNotForwarded() throws {
        func run(_ reason: String?) -> BackgroundTaskRunRecord {
            BackgroundTaskRunRecord(
                runId: "r-\(reason ?? "nil")", entryPoint: .rediffRefetch,
                taskIdentifier: "com.playhead.app.rediff-refetch",
                startedAt: 1, outcome: .admittedWork, deferReason: reason
            )
        }
        let parsed = build(DiagnosticsRediffSnapshot(backgroundRuns: [
            run("precheckBytes=131072 fullFetchBytes=54000000")
        ])).rediffDiagnostics.backgroundRuns.first
        #expect(parsed?.precheckBytes == 131_072)
        #expect(parsed?.fullFetchBytes == 54_000_000)

        // A row from another shape, or none at all, yields nil — never a
        // half-parsed string.
        for reason in [nil, "some unrelated defer reason", "precheckBytes=notanumber"] {
            let out = build(DiagnosticsRediffSnapshot(backgroundRuns: [run(reason)]))
                .rediffDiagnostics.backgroundRuns.first
            #expect(out?.precheckBytes == nil, "unparseable input must not become a number")
        }

        // And nothing textual reaches the JSON.
        let json = try encodedJSON(build(DiagnosticsRediffSnapshot(backgroundRuns: [
            run("precheckBytes=1 fullFetchBytes=2 leakedSecret=hunter2")
        ])))
        #expect(!json.contains("hunter2"))
        #expect(!json.contains("defer_reason"))
    }

    // MARK: - Caps

    @Test("per-asset rows are capped and the NEWEST survive")
    func rowsAreCappedNewestFirst() {
        let overCap = DiagnosticsBundleBuilder.rediffRowCap + 10
        let snapshot = DiagnosticsRediffSnapshot(
            dayZeroAttempts: (0..<overCap).map { index in
                RediffDayZeroAttemptRecord(
                    analysisAssetId: "a\(index)", attemptCount: 1,
                    lastAttemptAt: Double(index), lastExit: .noDivergentSlot
                )
            }
        )
        let projected = build(snapshot).rediffDiagnostics.dayZeroAttempts

        #expect(projected.count == DiagnosticsBundleBuilder.rediffRowCap)
        #expect(projected.first?.lastAttemptAt == Double(overCap - 1), "newest first, oldest dropped")
    }
}
