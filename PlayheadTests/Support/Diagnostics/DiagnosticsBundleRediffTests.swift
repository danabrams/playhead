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
            // REVIEW ROUND 3: `out?.precheckBytes == nil` on its own is the
            // optional-chaining vacuity shape — it holds whenever `out` is nil,
            // i.e. it would keep passing if the projection stopped emitting the
            // row at all, which is the failure this file exists to catch. The
            // row's existence is asserted first so the nil is about the ANNOTATION.
            #expect(out != nil, "the run is still projected — only its annotation is unparseable")
            #expect(out?.precheckBytes == nil, "unparseable input must not become a number")
            #expect(out?.fullFetchBytes == nil, "…and neither key half-parses")
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

// MARK: - REVIEW ROUND 2, REGION 2: the production fetch adapter

/// The adapter behind `DiagnosticsRediffFetch` used to be TWO byte-identical
/// copies — one in `DebugDiagnosticsHatch` (`#if DEBUG`), one in
/// `ReleaseDiagnosticsHatch` (`#if !DEBUG`) — with `ListenerFeedbackHatch`
/// resolving to whichever its `DiagnosticsHatch` typealias picked. Two facts
/// made that worse than ordinary duplication:
///
///   * the test target builds DEBUG, so the Release copy was compiled by
///     NOTHING any test can run. A divergence there could not be caught by a
///     test, only by a reviewer noticing;
///   * every read is `try?`, so a divergence that turned a read into a failure
///     would present as an EMPTY table — which is what a lane that has done
///     nothing looks like.
///
/// These run the single shared implementation against a real `AnalysisStore`.
@Suite("Rediff diagnostics fetch adapter (playhead-p70f review round 2)")
struct RediffDiagnosticsFetchAdapterTests {

    private func seed(_ store: AnalysisStore) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: "a1", episodeId: "ep-a1", assetFingerprint: "fp-a1",
            weakFingerprint: nil, sourceURL: "file:///tmp/a1.mp3",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil, episodeDurationSec: 100
        ))
        try await store.accumulateRediffBandwidth(
            precheckBytes: 1_000, fullFetchBytes: 299_600_000,
            unchangedCount: 0, rotatedCount: 0, failedCount: 0, parkedCount: 0,
            dayZeroUnmarkedCount: 3, at: 1_700_000_000
        )
        try await store.upsertRediffDayZeroAttempt(RediffDayZeroAttemptRecord(
            analysisAssetId: "a1", attemptCount: 2, lastAttemptAt: 1_700_000_000,
            lastExit: .noAcceptedByteDiff, totalFullFetchBytes: 216_000_000
        ))
        try await store.upsertRediffRefetchState(RediffRefetchStateRow(
            analysisAssetId: "a1", attemptState: .initial, updatedAt: 1_700_000_000
        ))
        try await store.insertBackgroundTaskRun(BackgroundTaskRunRecord(
            runId: "rediff-1", entryPoint: .rediffRefetch,
            taskIdentifier: "com.playhead.app.rediff_refetch",
            startedAt: 1_700_000_000, outcome: .noEligibleWork,
            deferReason: "precheckBytes=1000 fullFetchBytes=299600000"
        ))
    }

    @Test("the shared adapter reads all four rediff tables and reports no failures")
    func adapterReadsEveryTable() async throws {
        let store = try await makeTestStore()
        try await seed(store)

        let snapshot = await RediffDiagnosticsFetchAdapter.make(store: store)()

        #expect(snapshot.bandwidth.fullFetchBytesTotal == 299_600_000)
        #expect(snapshot.bandwidth.dayZeroUnmarkedCount == 3)
        #expect(snapshot.dayZeroAttempts.count == 1)
        #expect(snapshot.dayZeroAttempts.first?.lastExit == .noAcceptedByteDiff)
        #expect(snapshot.refetchStates.count == 1)
        #expect(snapshot.backgroundRuns.count == 1)
        #expect(snapshot.readFailures.isEmpty, "healthy store ⇒ nothing to report")
    }

    /// THE HAZARD REGION 2 ASKED ABOUT: can a swallowed error hide a real
    /// failure? It could. An unreadable `rediff_day_zero_attempts` beside a
    /// healthy ledger renders "299.6 MB spent, zero day-0 attempts" — the exact
    /// original bug report, manufactured by the export layer. So the failure is
    /// now NAMED, and the other three reads still arrive.
    @Test("ONE unreadable table is NAMED, and does not cost the export the other three")
    func partialFailureIsNamedNotSilentlyEmpty() async throws {
        let store = try await makeTestStore()
        try await seed(store)
        try await store.execForTesting("DROP TABLE rediff_day_zero_attempts")

        let snapshot = await RediffDiagnosticsFetchAdapter.make(store: store)()

        #expect(snapshot.readFailures == [RediffDiagnosticsFetchAdapter.Read.dayZeroAttempts.rawValue],
                "an unreadable table must not be indistinguishable from an empty one")
        #expect(snapshot.dayZeroAttempts.isEmpty)
        // Independently guarded: the other three survive.
        #expect(snapshot.bandwidth.fullFetchBytesTotal == 299_600_000)
        #expect(snapshot.refetchStates.count == 1)
        #expect(snapshot.backgroundRuns.count == 1)
    }

    @Test("read failures reach the exported bundle, in a fixed order and a closed vocabulary")
    func readFailuresAreExported() throws {
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 27", deviceClass: .iPhone17Pro,
            buildType: .debug,
            eligibility: AnalysisEligibility(
                hardwareSupported: true, appleIntelligenceEnabled: true,
                regionSupported: true, languageSupported: true,
                modelAvailableNow: true, capturedAt: Date(timeIntervalSince1970: 1)
            ),
            workJournalEntries: [],
            installID: UUID(uuidString: "33333333-3333-3333-3333-333333333333")!,
            rediff: DiagnosticsRediffSnapshot(readFailures: [
                "bandwidth",
                // Not in the closed vocabulary — must be dropped, so this can
                // never become a free-text channel off the device.
                "file:///var/mobile/Containers/episode-9.mp3",
                "background_runs"
            ])
        )

        #expect(bundle.rediffDiagnostics.readFailures == ["bandwidth", "background_runs"])
        let json = String(
            decoding: try JSONEncoder().encode(bundle.rediffDiagnostics), as: UTF8.self
        )
        #expect(json.contains("read_failures"))
        #expect(!json.contains("episode-9"), "only the closed vocabulary crosses the boundary")
    }

    /// The two fetch limits make load-bearing claims about the builder's caps:
    /// the background-run limit must EQUAL the cap (fetching fewer would silently
    /// truncate below what the bundle promises), and the row limit must EXCEED it
    /// (so the builder's newest-first sort, not the store's row order, decides
    /// which rows ship). Both were prose in a doc comment; neither was checked.
    @Test("the adapter's fetch limits agree with the builder's caps")
    func fetchLimitsAgreeWithBuilderCaps() {
        #expect(RediffDiagnosticsFetchAdapter.backgroundRunFetchLimit
                == DiagnosticsBundleBuilder.rediffBackgroundRunCap)
        #expect(RediffDiagnosticsFetchAdapter.rowFetchLimit
                > DiagnosticsBundleBuilder.rediffRowCap)
    }

    /// The Release hatch is `#if !DEBUG` and therefore absent from this build —
    /// which is exactly why the duplication was dangerous.
    ///
    /// REVIEW ROUND 3, on what this does and does NOT prove. It pins that the
    /// hatch AGREES with the shared adapter on every projected field: a mutation
    /// making `makeRediffFetch` return `.empty` instead of forwarding reddens it.
    /// It cannot prove the hatch is a *pure forward* — a byte-identical
    /// reimplementation would pass, and no runtime assertion can distinguish the
    /// two. What actually removes the divergence risk is structural (one
    /// unconditionally-compiled `RediffDiagnosticsFetchAdapter`, both hatches
    /// one-lining into it); this test is the regression guard on top of it, not
    /// the guarantee.
    @Test("the DEBUG hatch agrees with the shared adapter on every projected field")
    func debugHatchForwardsToTheSharedAdapter() async throws {
        let store = try await makeTestStore()
        try await seed(store)

        let viaHatch = await DebugDiagnosticsHatch.makeRediffFetch(store: store)()
        let viaAdapter = await RediffDiagnosticsFetchAdapter.make(store: store)()

        #expect(viaHatch.bandwidth.fullFetchBytesTotal == viaAdapter.bandwidth.fullFetchBytesTotal)
        #expect(viaHatch.bandwidth.dayZeroUnmarkedCount == viaAdapter.bandwidth.dayZeroUnmarkedCount)
        #expect(viaHatch.dayZeroAttempts.count == viaAdapter.dayZeroAttempts.count)
        #expect(viaHatch.refetchStates.count == viaAdapter.refetchStates.count)
        #expect(viaHatch.backgroundRuns.count == viaAdapter.backgroundRuns.count)
        #expect(viaHatch.readFailures == viaAdapter.readFailures)
    }
}
