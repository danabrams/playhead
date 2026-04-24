// NarlPipelineCoverageBucketTests.swift
// playhead-gtt9.15: coverage-aware harness classification.
//
// gtt9.8 wired `terminalReason`, `analysisState`, `durationSec`,
// `fastTranscriptCoverageEndTime`, `featureCoverageEndTime` into the
// FrozenTrace JSON via the corpus builder + lifecycle log reader.
// gtt9.15 layers a 3-bucket harness classifier on top so a downstream
// reader can see at a glance how many episodes missed because the
// pipeline never covered the ad window vs how many missed despite
// full coverage (the exact 2026-04-24 Finding 1 question for
// 71F0C2AE full-coverage vs 34C7E7CF partial-coverage).
//
// The bucketing rule (per gtt9.15 spec):
//   - "scoring-limited"          = full coverage (terminalReason
//                                  contains "full coverage" OR
//                                  fastTranscriptCoverageEndTime ≥
//                                  durationSec * 0.95)
//   - "pipeline-coverage-limited" = partial coverage with
//                                  fastTranscriptCoverageEndTime <
//                                  durationSec * 0.95 OR terminalReason
//                                  indicates a coverage failure
//   - "unknown"                   = lifecycle fields absent (pre-9.8
//                                  fixtures or any other absence)
//
// Note: this is COARSER than the per-state `terminalReasonBuckets`
// stratification. The 6-state per-bucket rollup answers "what state
// did the asset terminate in?" — useful but operator-noisy. The
// 3-bucket classifier here answers "for harness reasoning purposes,
// is this asset showing a scoring deficit or a coverage deficit?" —
// the headline cohort split.

import Foundation
import Testing
@testable import Playhead

@Suite("NarlPipelineCoverageBucket")
struct NarlPipelineCoverageBucketTests {

    // MARK: - Classifier rules

    @Test("classify returns .scoringLimited when terminalReason contains 'full coverage'")
    func classifyScoringLimitedFromFullCoverageReason() {
        let trace = Self.makeTrace(
            durationSec: 7037.83,
            fastTranscriptCoverageEndTime: 7037.34,
            terminalReason: "full coverage: transcript 1.000, feature 1.000"
        )
        #expect(NarlPipelineCoverageBucket.classify(trace) == .scoringLimited)
    }

    @Test("classify returns .scoringLimited when transcript coverage ratio ≥ 0.95")
    func classifyScoringLimitedFromHighCoverageRatio() {
        // No "full coverage" in reason, but ratio is 950/1000 = 0.95 → scoring-limited.
        let trace = Self.makeTrace(
            durationSec: 1000,
            fastTranscriptCoverageEndTime: 950,
            terminalReason: "non-canonical reason string"
        )
        #expect(NarlPipelineCoverageBucket.classify(trace) == .scoringLimited)
    }

    @Test("classify returns .pipelineCoverageLimited for low coverage ratio + non-full reason")
    func classifyPipelineLimitedFromLowCoverage() {
        // 34C7E7CF case: 840/900 = 0.933 < 0.95 → pipeline-limited.
        let trace = Self.makeTrace(
            durationSec: 900,
            fastTranscriptCoverageEndTime: 840,
            terminalReason: nil,
            analysisState: "backfill"
        )
        #expect(NarlPipelineCoverageBucket.classify(trace) == .pipelineCoverageLimited)
    }

    @Test("classify returns .pipelineCoverageLimited when terminalReason indicates coverage failure")
    func classifyPipelineLimitedFromCoverageFailureReason() {
        // High coverage end time but coverage-failure reason wins.
        // (This is contrived — production never ships a "transcript coverage X/Y"
        // string when coverage is full — but pin the rule explicitly.)
        let trace = Self.makeTrace(
            durationSec: 1000,
            fastTranscriptCoverageEndTime: 990,
            terminalReason: "transcript coverage 689.8/3600.0s (ratio 0.192 < 0.500)"
        )
        #expect(NarlPipelineCoverageBucket.classify(trace) == .pipelineCoverageLimited)
    }

    @Test("classify returns .unknown when all lifecycle fields are nil (pre-9.8 fixture)")
    func classifyUnknownWhenLifecycleAbsent() {
        let trace = Self.makeTrace(
            durationSec: nil,
            fastTranscriptCoverageEndTime: nil,
            terminalReason: nil,
            analysisState: nil
        )
        #expect(NarlPipelineCoverageBucket.classify(trace) == .unknown)
    }

    @Test("classify returns .unknown when durationSec is nil even if coverage end time set")
    func classifyUnknownWhenDurationMissing() {
        // A 0/nil duration leaves the ratio undefined. We can't honestly
        // call this scoring-limited or pipeline-limited.
        let trace = Self.makeTrace(
            durationSec: nil,
            fastTranscriptCoverageEndTime: 100,
            terminalReason: nil,
            analysisState: "completeFull"
        )
        #expect(NarlPipelineCoverageBucket.classify(trace) == .unknown)
    }

    @Test("classify returns .unknown when durationSec is 0 (degenerate input)")
    func classifyUnknownWhenDurationZero() {
        // Division-by-zero guard: a 0-second duration must not mis-classify.
        let trace = Self.makeTrace(
            durationSec: 0,
            fastTranscriptCoverageEndTime: 0,
            terminalReason: nil,
            analysisState: "queued"
        )
        #expect(NarlPipelineCoverageBucket.classify(trace) == .unknown)
    }

    // MARK: - Threshold configurability (playhead-ahez)

    /// ahez: promoting the 0.95 literal into a config struct means a
    /// caller can dial the "scoring-limited" floor up or down. This test
    /// pins the plumbing: a trace whose coverage ratio is 0.92 — strictly
    /// between the custom 0.90 floor and the default 0.95 floor — must
    /// classify differently under each. If we asserted only one side the
    /// test would tautologically pass when the default was wired through.
    @Test("classify honors a custom scoringLimitedCoverageFloor and differs from the default")
    func classifyHonorsCustomThresholds() {
        // Ratio = 920/1000 = 0.92. Between the custom floor (0.90) and
        // the default floor (0.95), so the two classifications must
        // disagree — exactly the plumbing proof this test is for.
        let trace = Self.makeTrace(
            durationSec: 1000,
            fastTranscriptCoverageEndTime: 920,
            terminalReason: nil,
            analysisState: "backfill"
        )

        let lenient = NarlPipelineCoverageThresholds(scoringLimitedCoverageFloor: 0.90)
        #expect(NarlPipelineCoverageBucket.classify(trace, thresholds: lenient) == .scoringLimited,
                "0.92 ratio meets the custom 0.90 floor → scoring-limited")

        #expect(NarlPipelineCoverageBucket.classify(trace) == .pipelineCoverageLimited,
                "0.92 ratio falls below the default 0.95 floor → pipeline-limited")
        #expect(NarlPipelineCoverageBucket.classify(trace, thresholds: .default) == .pipelineCoverageLimited,
                "explicit .default must match the implicit-default call-site")
    }

    // MARK: - Rollup aggregator

    @Test("countsPerBucket tallies traces into the three bucket counts")
    func countsPerBucketAggregatesAcrossTraces() {
        let traces: [FrozenTrace] = [
            // 2 scoring-limited via full-coverage reason
            Self.makeTrace(
                episodeId: "s1",
                durationSec: 100,
                fastTranscriptCoverageEndTime: 100,
                terminalReason: "full coverage: transcript 1.000, feature 1.000"
            ),
            Self.makeTrace(
                episodeId: "s2",
                durationSec: 200,
                fastTranscriptCoverageEndTime: 195,
                terminalReason: "full coverage: transcript 1.000, feature 0.975"
            ),
            // 3 pipeline-limited
            Self.makeTrace(
                episodeId: "p1",
                durationSec: 900,
                fastTranscriptCoverageEndTime: 840,
                terminalReason: nil,
                analysisState: "backfill"
            ),
            Self.makeTrace(
                episodeId: "p2",
                durationSec: 1000,
                fastTranscriptCoverageEndTime: 100,
                terminalReason: "transcript coverage 100.0/1000.0s (ratio 0.100 < 0.500)"
            ),
            Self.makeTrace(
                episodeId: "p3",
                durationSec: 1500,
                fastTranscriptCoverageEndTime: 500,
                terminalReason: nil,
                analysisState: "completeTranscriptPartial"
            ),
            // 1 unknown (pre-9.8)
            Self.makeTrace(
                episodeId: "u1",
                durationSec: nil,
                fastTranscriptCoverageEndTime: nil,
                terminalReason: nil,
                analysisState: nil
            ),
        ]
        let counts = NarlPipelineCoverageBucket.countsPerBucket(traces: traces)
        let byBucket = Dictionary(uniqueKeysWithValues: counts.map { ($0.bucket, $0.count) })
        #expect(byBucket[.scoringLimited] == 2)
        #expect(byBucket[.pipelineCoverageLimited] == 3)
        #expect(byBucket[.unknown] == 1)
    }

    @Test("countsPerBucket returns the canonical 3-bucket order even when some are zero")
    func countsPerBucketIncludesAllThreeBucketsAlwaysOrdered() {
        // Only scoring-limited traces — but the rollup must still emit
        // entries for pipeline-coverage-limited and unknown (count 0)
        // so report consumers can rely on a stable shape.
        let traces = [
            Self.makeTrace(
                episodeId: "s1",
                durationSec: 100,
                fastTranscriptCoverageEndTime: 100,
                terminalReason: "full coverage: transcript 1.000, feature 1.000"
            )
        ]
        let counts = NarlPipelineCoverageBucket.countsPerBucket(traces: traces)
        #expect(counts.count == 3, "must always emit all 3 buckets for stable JSON shape")
        #expect(counts.map(\.bucket) == [.scoringLimited, .pipelineCoverageLimited, .unknown],
                "buckets must appear in canonical order: scoring → pipeline → unknown")
        #expect(counts.first(where: { $0.bucket == .scoringLimited })?.count == 1)
        #expect(counts.first(where: { $0.bucket == .pipelineCoverageLimited })?.count == 0)
        #expect(counts.first(where: { $0.bucket == .unknown })?.count == 0)
    }

    @Test("countsPerBucket handles empty input")
    func countsPerBucketEmptyInput() {
        let counts = NarlPipelineCoverageBucket.countsPerBucket(traces: [])
        #expect(counts.count == 3)
        #expect(counts.allSatisfy { $0.count == 0 })
    }

    // MARK: - Codable round-trip on report schema

    @Test("NarlEvalReport.pipelineCoverageBuckets round-trips through encode/decode")
    func reportRoundTripsPipelineCoverageBuckets() throws {
        let rollup = NarlPipelineCoverageBucketRollup(bucket: .scoringLimited, count: 7)
        let report = NarlEvalReport(
            schemaVersion: NarlEvalReportSchema.version,
            generatedAt: Date(timeIntervalSince1970: 0),
            runId: "test",
            iouThresholds: [0.3, 0.5, 0.7],
            rollups: [],
            episodes: [],
            notes: [],
            terminalReasonBuckets: nil,
            pipelineCoverageBuckets: [rollup]
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(report)

        let text = String(decoding: data, as: UTF8.self)
        #expect(text.contains("\"pipelineCoverageBuckets\""),
                "encoded JSON must include the new pipelineCoverageBuckets key")

        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(NarlEvalReport.self, from: data)
        #expect(decoded.pipelineCoverageBuckets?.count == 1)
        #expect(decoded.pipelineCoverageBuckets?.first?.bucket == .scoringLimited)
        #expect(decoded.pipelineCoverageBuckets?.first?.count == 7)
    }

    @Test("pre-gtt9.15 report.json (no pipelineCoverageBuckets key) decodes with nil")
    func preBucketReportDecodesWithNil() throws {
        // Literal payload matching the pre-gtt9.15 schema: every field
        // the previous NarlEvalReport emitted is populated (incl.
        // gtt9.8 stratification's `terminalReasonBuckets`); the new
        // `pipelineCoverageBuckets` field is absent entirely. This
        // pins the `decodeIfPresent` path so historical reports keep
        // decoding once gtt9.15 ships.
        let json = """
        {
          "schemaVersion": 1,
          "generatedAt": "1970-01-01T00:00:00Z",
          "runId": "pre-gtt9.15",
          "iouThresholds": [0.3, 0.5, 0.7],
          "rollups": [],
          "episodes": [],
          "notes": [],
          "terminalReasonBuckets": []
        }
        """
        let data = Data(json.utf8)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(NarlEvalReport.self, from: data)

        #expect(decoded.runId == "pre-gtt9.15")
        #expect(decoded.terminalReasonBuckets?.isEmpty == true,
                "existing terminalReasonBuckets must continue to round-trip")
        #expect(decoded.pipelineCoverageBuckets == nil,
                "missing key must decode to nil — additive, back-compat")
    }

    // MARK: - Real-fixture validation (acceptance criterion #4)

    @Test("71F0C2AE 2026-04-24 fixture buckets as .scoringLimited (full coverage)")
    func fixture71F0C2AEBucketsAsScoringLimited() throws {
        let trace = try Self.loadFixtureTrace(
            relpath: "PlayheadTests/Fixtures/NarlEval/2026-04-24/FrozenTrace-71F0C2AE-7260-4D1E-B41A-BCFD5103A641.json"
        )
        // Sanity: lifecycle fields must be present (else the gtt9.8
        // corpus-builder change isn't actually flowing into this
        // fixture and the bead is broken upstream of the classifier).
        #expect(trace.terminalReason?.contains("full coverage") == true,
                "71F0C2AE should carry terminalReason 'full coverage…' from the lifecycle log")
        #expect(trace.durationSec ?? 0 > 0)
        #expect(trace.fastTranscriptCoverageEndTime ?? 0 > 0)

        #expect(NarlPipelineCoverageBucket.classify(trace) == .scoringLimited,
                "71F0C2AE has full coverage → scoring-limited (Finding 1: missed despite full coverage)")
    }

    @Test("34C7E7CF 2026-04-24 fixture buckets as .pipelineCoverageLimited (stalled in backfill)")
    func fixture34C7E7CFBucketsAsPipelineLimited() throws {
        let trace = try Self.loadFixtureTrace(
            relpath: "PlayheadTests/Fixtures/NarlEval/2026-04-24/FrozenTrace-34C7E7CF-931F-49EE-B51B-49D3080F1FFB.json"
        )
        // Sanity: lifecycle fields must be present.
        #expect(trace.analysisState == "backfill",
                "34C7E7CF stalled in backfill — no terminalReason emitted")
        #expect(trace.durationSec == 900)
        #expect(trace.fastTranscriptCoverageEndTime == 840,
                "34C7E7CF: 840/900 = 0.933 < 0.95 threshold → pipeline-limited")

        #expect(NarlPipelineCoverageBucket.classify(trace) == .pipelineCoverageLimited,
                "34C7E7CF stopped before episode end → pipeline-coverage-limited")
    }

    @Test("OLD fixtures (2026-04-22, 2026-04-23) bucket as .unknown")
    func oldFixturesBucketAsUnknown() throws {
        // Spot-check one fixture from each pre-9.8 date-dir. They lack
        // lifecycle fields and MUST classify as .unknown — pinning the
        // bead's "do NOT retrofit values into those old JSON files"
        // contract.
        for relpath in [
            "PlayheadTests/Fixtures/NarlEval/2026-04-22",
            "PlayheadTests/Fixtures/NarlEval/2026-04-23",
            "PlayheadTests/Fixtures/NarlEval/2026-04-23-1354",
        ] {
            let dirURL = try Self.repoRootURL().appendingPathComponent(relpath)
            guard let firstFixture = try? FileManager.default.contentsOfDirectory(
                at: dirURL, includingPropertiesForKeys: nil
            ).first(where: { $0.lastPathComponent.hasPrefix("FrozenTrace-") })
            else { continue } // Date-dir absent on shallow CI clones — silently skip.

            let trace = try Self.decodeTrace(at: firstFixture)
            // Old fixtures should have nil lifecycle fields.
            if trace.durationSec == nil
                && trace.terminalReason == nil
                && trace.fastTranscriptCoverageEndTime == nil
                && trace.analysisState == nil {
                #expect(NarlPipelineCoverageBucket.classify(trace) == .unknown,
                        "pre-9.8 fixture at \(firstFixture.lastPathComponent) must bucket as .unknown")
            }
            // If a pre-9.8 fixture happens to have a lifecycle field
            // populated (shouldn't, but the corpus builder is the
            // authority — we don't enforce here), classification falls
            // through to the normal rules; test simply doesn't assert.
        }
    }

    // MARK: - Live harness integration

    @Test("live harness run writes pipelineCoverageBuckets into report.json")
    func liveHarnessEmitsPipelineCoverageBuckets() throws {
        let (_, outputDir) = try NarlEvalHarnessTests.runHarnessCollectingReport()
        let jsonURL = outputDir.appendingPathComponent("report.json")
        let data = try Data(contentsOf: jsonURL)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(NarlEvalReport.self, from: data)

        let buckets = try #require(decoded.pipelineCoverageBuckets,
                                   "live run must emit pipelineCoverageBuckets (even when empty)")
        // Shape stability: always 3 buckets in canonical order.
        #expect(buckets.count == 3)
        #expect(buckets.map(\.bucket) == [.scoringLimited, .pipelineCoverageLimited, .unknown])

        // The rendered markdown should mention the new section header
        // so a downstream report reader can find it by name.
        let mdData = try Data(contentsOf: outputDir.appendingPathComponent("report.md"))
        let md = String(decoding: mdData, as: UTF8.self)
        #expect(md.contains("Pipeline-coverage classification") ||
                md.contains("pipelineCoverageBuckets"),
                "report.md should include the new pipeline-coverage section")
    }

    // MARK: - Helpers

    private static func makeTrace(
        episodeId: String = "ep-test",
        durationSec: Double?,
        fastTranscriptCoverageEndTime: Double?,
        terminalReason: String? = nil,
        analysisState: String? = nil
    ) -> FrozenTrace {
        FrozenTrace(
            episodeId: episodeId,
            podcastId: "test",
            episodeDuration: durationSec ?? 300,
            traceVersion: "frozen-trace-v2",
            capturedAt: Date(timeIntervalSince1970: 0),
            featureWindows: [],
            atoms: [],
            evidenceCatalog: [],
            corrections: [],
            decisionEvents: [],
            baselineReplaySpanDecisions: [],
            holdoutDesignation: .training,
            windowScores: [],
            showLabel: nil,
            durationSec: durationSec,
            analysisState: analysisState,
            terminalReason: terminalReason,
            fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime,
            featureCoverageEndTime: nil
        )
    }

    private static func loadFixtureTrace(relpath: String) throws -> FrozenTrace {
        let url = try Self.repoRootURL().appendingPathComponent(relpath)
        return try Self.decodeTrace(at: url)
    }

    private static func decodeTrace(at url: URL) throws -> FrozenTrace {
        let data = try Data(contentsOf: url)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(FrozenTrace.self, from: data)
    }

    private static func repoRootURL() throws -> URL {
        let thisFile = URL(fileURLWithPath: #filePath)
        return try NarlEvalHarnessTests.repoRoot(
            startingAt: thisFile.deletingLastPathComponent()
        )
    }
}
