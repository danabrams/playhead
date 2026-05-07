// DogfoodDiagnosticsAnalysisHealthTests.swift
// Phase 1.5 playhead-hygc.1.9 — pin the actionable no-progress
// summary the dogfood diagnostics export now ships.
//
// The tests cover three layers:
//   1. Pure-builder regression: hand-rolled
//      `DogfoodDiagnosticsActivitySnapshot` fixtures exercise each
//      branch of `DogfoodDiagnosticsAnalysisHealth.build(from:)` —
//      global counts, per-asset summaries, staleness flags,
//      progress-provenance forwarding, recommended actions.
//   2. Archive integration: round-trip through the JSON encoder /
//      decoder, schema-version bump, and v1 backwards compatibility.
//   3. Privacy / redaction: defense-in-depth scrub for any string
//      that flows from the activity snapshot into the
//      `analysis_health` block.
//
// Sibling .1.1 (sanitized May 6 fixture) is not yet on main; until
// it lands, the structural assertions here use a hand-rolled
// fixture that mirrors the May 6 *shape* (22 queued/up_next rows,
// cached audio present, analysis_state mix of completeFull /
// backfill / queued, watermark vs chunk-coverage gaps). When the
// real fixture lands, a follow-up bead can wire the same
// assertions against it without changing the public surface this
// test pins.

import Foundation
import Testing

@testable import Playhead

@Suite("DogfoodDiagnosticsAnalysisHealth — playhead-hygc.1.9")
@MainActor
struct DogfoodDiagnosticsAnalysisHealthTests {

    // MARK: - Schema versioning

    @Test("schema bumps to v2 only when analysis_health is attached")
    func schemaVersionBumpsOnlyWhenHealthAttached() throws {
        let source = try makeTempDir(prefix: "AHv2Source")
        let output = try makeTempDir(prefix: "AHv2Output")
        defer {
            try? FileManager.default.removeItem(at: source)
            try? FileManager.default.removeItem(at: output)
        }
        try writeMinimalSurfaceStatusLog(in: source)

        // No analysis_health → v1, exactly as before.
        let v1Result = try DogfoodDiagnosticsExporter.export(
            sourceDirectory: source,
            outputDirectory: output,
            now: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let v1Archive = try decodeArchive(at: v1Result.fileURL)
        #expect(v1Archive.schemaVersion == DogfoodDiagnosticsExporter.schemaVersionV1)
        #expect(v1Archive.analysisHealth == nil)

        // analysis_health attached → v2.
        let snapshot = makeMinimalActivitySnapshot()
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let v2Result = try DogfoodDiagnosticsExporter.export(
            sourceDirectory: source,
            outputDirectory: output,
            now: Date(timeIntervalSince1970: 1_700_000_001),
            activitySnapshot: snapshot,
            analysisHealth: health
        )
        let v2Archive = try decodeArchive(at: v2Result.fileURL)
        #expect(v2Archive.schemaVersion == DogfoodDiagnosticsExporter.schemaVersionV2)
        let decodedHealth = try #require(v2Archive.analysisHealth)
        #expect(decodedHealth.global.totalAssets == 1)
    }

    @Test("v1 archives still decode against the v2 struct")
    func v1ArchivesDecodeAgainstV2Struct() throws {
        // Hand-rolled v1 JSON — what an older build emitted before
        // playhead-hygc.1.9. Decoding into the new struct must not
        // crash and must surface analysis_health as nil.
        let json = """
        {
          "files": [
            {
              "byte_count": 10,
              "content": "ignored",
              "filename": "surface-status-x.jsonl",
              "role": "surface_status_jsonl"
            }
          ],
          "generated_at": "2026-04-30T00:00:00Z",
          "schema_version": 1
        }
        """
        let data = Data(json.utf8)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let archive = try decoder.decode(DogfoodDiagnosticsArchive.self, from: data)
        #expect(archive.schemaVersion == 1)
        #expect(archive.activitySnapshot == nil)
        #expect(archive.analysisHealth == nil)
        #expect(archive.files.count == 1)
    }

    @Test("v2 archives ignored by older readers without analysis_health key")
    func v2ArchivesDecodeWithoutAnalysisHealthKey() throws {
        // A v2 archive serialized with `analysis_health` present.
        // We then re-decode through a struct that does NOT know
        // about analysis_health — same as a build-N reader looking
        // at a build-(N+1) archive. JSONDecoder's default policy
        // ignores unknown keys, so the decode succeeds and the
        // older reader keeps working.
        let snapshot = makeMinimalActivitySnapshot()
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let archive = DogfoodDiagnosticsArchive(
            schemaVersion: DogfoodDiagnosticsExporter.schemaVersionV2,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            files: [DogfoodDiagnosticsArchiveFile(
                filename: "surface-status-x.jsonl",
                role: "surface_status_jsonl",
                byteCount: 10,
                content: "ignored"
            )],
            activitySnapshot: snapshot,
            analysisHealth: health
        )
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(archive)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        // Decode with a struct that mirrors the v1 surface (only
        // schema_version + files + generated_at). This proves an
        // older reader doesn't crash on the extra v2 keys.
        let v1Surface = try decoder.decode(V1SurfaceMirror.self, from: data)
        #expect(v1Surface.schemaVersion == 2)
        #expect(v1Surface.files.count == 1)
    }

    // MARK: - Global summary

    @Test("global summary counts running / queued / terminal / unknown")
    func globalSummaryCounts() {
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [
                makeActivityRow(
                    hash: "running-1",
                    section: "now",
                    disposition: "queued",
                    analysisState: "spooling",
                    isRunning: true
                ),
                makeActivityRow(
                    hash: "queued-1",
                    disposition: "queued",
                    analysisState: "queued",
                    isRunning: false
                ),
                makeActivityRow(
                    hash: "complete-1",
                    disposition: "queued",
                    analysisState: "completeFull",
                    isRunning: false
                ),
                makeActivityRow(
                    hash: "unknown-1",
                    disposition: "queued",
                    analysisState: "queued",
                    isRunning: false,
                    pipeline: makePipeline(
                        downloadPercent: "--%",
                        transcriptPercent: "--%",
                        analysisPercent: "--%"
                    )
                ),
                makeActivityRow(
                    hash: "failed-1",
                    disposition: "failed",
                    analysisState: "failed",
                    isRunning: false
                )
            ]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )

        #expect(health.global.totalAssets == 5)
        #expect(health.global.runningCount == 1)
        #expect(health.global.queuedCount == 4) // disposition='queued'
        #expect(health.global.failedCount == 1)
        #expect(health.global.terminalCompletedCount == 1)
        #expect(health.global.unknownProgressCount == 1)
    }

    @Test("global summary records latest job/work timestamps deterministically")
    func globalSummaryTimestamps() {
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [
                makeActivityRow(
                    hash: "h-old",
                    latestJob: makeJob(updatedAt: 1_777_000_000.0),
                    latestTerminalWork: makeWorkJournal(
                        eventType: "finalized",
                        timestamp: 1_777_000_500.0
                    )
                ),
                makeActivityRow(
                    hash: "h-new",
                    latestJob: makeJob(updatedAt: 1_777_001_000.0),
                    latestTerminalWork: makeWorkJournal(
                        eventType: "preempted",
                        timestamp: 1_777_001_500.0
                    )
                )
            ]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_002)
        )
        #expect(health.global.latestJobUpdateAt == 1_777_001_000.0)
        #expect(health.global.latestTerminalWorkAt == 1_777_001_500.0)
        #expect(health.global.latestTerminalWorkOutcome == "preempted")
    }

    // MARK: - Per-asset summary

    @Test("per-asset summary forwards progress provenance verbatim")
    func perAssetSummaryForwardsProvenance() {
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [
                makeActivityRow(
                    hash: "h1",
                    pipeline: makePipeline(
                        downloadSource: "cached_audio",
                        transcriptSource: "fast_transcript_chunks",
                        analysisSource: "feature_coverage"
                    )
                )
            ]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let asset = try! #require(health.assets.first)
        #expect(asset.progressProvenance.downloadSource == "cached_audio")
        #expect(asset.progressProvenance.transcriptSource == "fast_transcript_chunks")
        #expect(asset.progressProvenance.analysisSource == "feature_coverage")
    }

    @Test("watermark delta is positive when chunk coverage outruns watermark")
    func watermarkDeltaPositiveWhenChunksAhead() {
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [
                makeActivityRow(
                    hash: "h-stale-watermark",
                    pipeline: makePipeline(
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 3960,
                        fastTranscriptWatermarkSec: 90
                    )
                )
            ]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let asset = try! #require(health.assets.first)
        let delta = try! #require(asset.watermarkDeltaSec)
        #expect(delta == 3960 - 90)
    }

    // MARK: - Staleness flags

    @Test("flags terminal-state contradiction when completeFull lacks coverage")
    func flagsTerminalContradictionForCompleteFull() {
        // 50% transcript coverage AND 2.5% feature coverage on a
        // `completeFull` row → contradiction on both axes. Pins the
        // count to exactly one terminal-contradiction flag and that
        // the only other flag attributable to the row is the watermark
        // gap (transcript_covered=2000, fast_watermark=90 → delta>60).
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-contradicting",
                analysisState: "completeFull",
                pipeline: makePipeline(
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: 2000,
                    fastTranscriptWatermarkSec: 90,
                    featureCoverageEndSec: 100
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let terminalFlags = health.stalenessFlags.filter {
            $0.kind == .terminalStateContradictsCoverage
        }
        #expect(terminalFlags.count == 1)
        #expect(terminalFlags.first?.episodeIdHash == "h-contradicting")
        #expect(health.global.staleTerminalCount == 1)
        // The recommendation should agree with the flag.
        #expect(health.assets.first?.recommendedAction == .fileBug)
    }

    @Test("flags completeFull when only the feature axis is short of threshold")
    func flagsCompleteFullOnFeatureShortfallAlone() {
        // Adversarial OR-axis test: transcript is healthy (99%) but
        // feature coverage is empty (2.5%). Per the completeFull
        // contract — feature AND transcript at threshold — a feature
        // shortfall alone IS a contradiction. A logic that takes the
        // max across axes would silently miss this.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-feature-shortfall",
                analysisState: "completeFull",
                pipeline: makePipeline(
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: 3960,
                    fastTranscriptWatermarkSec: 3960,
                    featureCoverageEndSec: 100
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let flagKinds = health.stalenessFlags.map(\.kind)
        #expect(flagKinds.contains(.terminalStateContradictsCoverage))
        #expect(health.global.staleTerminalCount == 1)
        #expect(health.assets.first?.recommendedAction == .fileBug)
    }

    @Test("flags completeFull when only the transcript axis is short of threshold")
    func flagsCompleteFullOnTranscriptShortfallAlone() {
        // Mirror of the feature-shortfall test: feature is healthy
        // but transcript is empty. The contradiction is real.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-transcript-shortfall",
                analysisState: "completeFull",
                pipeline: makePipeline(
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: 100,
                    fastTranscriptWatermarkSec: 100,
                    featureCoverageEndSec: 3960
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let flagKinds = health.stalenessFlags.map(\.kind)
        #expect(flagKinds.contains(.terminalStateContradictsCoverage))
        #expect(health.assets.first?.recommendedAction == .fileBug)
    }

    @Test("does not flag completeFull when both axes meet the threshold")
    func doesNotFlagCompleteFullWhenHealthy() {
        // Healthy completeFull row: transcript and feature both at
        // threshold. Pins the no-flag side of the OR contract so a
        // future regression can't quietly start flagging healthy
        // rows.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-healthy",
                analysisState: "completeFull",
                pipeline: makePipeline(
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: 3900,
                    fastTranscriptWatermarkSec: 3900,
                    featureCoverageEndSec: 4000
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let flagKinds = health.stalenessFlags.map(\.kind)
        #expect(!flagKinds.contains(.terminalStateContradictsCoverage))
        #expect(health.global.staleTerminalCount == 0)
    }

    @Test("threshold is strict <: exactly 95.0% does not flag, just below does")
    func thresholdBoundaryAtNinetyFivePercent() {
        // Pin the exact comparison semantics around the 95% threshold.
        // duration=1000, threshold=950. Coverage=950 is healthy
        // (strict <), coverage=949.999... is a contradiction. Without
        // this pin, a future >= 0.95 swap would silently widen the
        // window (or a < 1.0 hardcode would silently narrow it).
        let healthySnapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-at-threshold",
                analysisState: "completeFull",
                pipeline: makePipeline(
                    episodeDurationSec: 1000,
                    transcriptCoveredSec: 950,
                    fastTranscriptWatermarkSec: 950,
                    featureCoverageEndSec: 950
                )
            )]
        )
        let healthy = DogfoodDiagnosticsAnalysisHealth.build(
            from: healthySnapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        #expect(!healthy.stalenessFlags.contains { $0.kind == .terminalStateContradictsCoverage })
        #expect(healthy.assets.first?.recommendedAction != .fileBug)

        // 949.999 just below threshold → flag fires.
        let belowSnapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-just-below",
                analysisState: "completeFull",
                pipeline: makePipeline(
                    episodeDurationSec: 1000,
                    transcriptCoveredSec: 949.999,
                    fastTranscriptWatermarkSec: 949.999,
                    featureCoverageEndSec: 949.999
                )
            )]
        )
        let below = DogfoodDiagnosticsAnalysisHealth.build(
            from: belowSnapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        #expect(below.stalenessFlags.contains { $0.kind == .terminalStateContradictsCoverage })
        #expect(below.assets.first?.recommendedAction == .fileBug)
    }

    @Test("flag list and recommendation list always agree on terminal contradictions")
    func flagAndRecommendationAgreeOnContradictions() {
        // R2 invariant: stalenessFlags(for:) and recommendation(for:)
        // both call the SAME terminalCompletionContradiction predicate.
        // Walk a matrix of state × axis combinations and assert the
        // bidirectional agreement: a row flagged for terminal
        // contradiction MUST be recommended fileBug, and a row
        // recommended fileBug MUST be flagged for terminal contradiction.
        // Failure modes one would-be R3 reviewer asks about: a future
        // refactor that moves the predicate inline in one site but not
        // the other, or introduces a state-specific note that diverges.
        let cases: [(String, String, Double, Double, Double, Double)] = [
            // (hash, state, duration, transcriptCovered, fastWatermark, featureCoverage)
            ("h-cf-both-short",     "completeFull",            4000, 100, 100, 100),
            ("h-cf-feature-short",  "completeFull",            4000, 3960, 3960, 100),
            ("h-cf-transcript-short","completeFull",           4000, 100, 100, 3960),
            ("h-cf-healthy",        "completeFull",            4000, 3900, 3900, 4000),
            ("h-legacy-short",      "complete",                4000, 100, 100, 100),
            ("h-legacy-healthy",    "complete",                4000, 3900, 3900, 3900),
            ("h-cfo-short",         "completeFeatureOnly",     4000, 40, 40, 100),
            ("h-cfo-healthy",       "completeFeatureOnly",     4000, 40, 40, 3900),
            ("h-ctp-empty",         "completeTranscriptPartial", 4000, 0, 0, 4000),
            ("h-ctp-partial",       "completeTranscriptPartial", 4000, 2000, 2000, 4000),
            // States outside the completion vocabulary should NEVER
            // flag for terminal contradiction, regardless of coverage.
            ("h-running",           "backfill",                4000, 0, 0, 0),
            ("h-queued",            "queued",                  4000, 0, 0, 0)
        ]
        let rows = cases.map { hashValue, state, dur, tc, fwm, fc in
            makeActivityRow(
                hash: hashValue,
                analysisState: state,
                pipeline: makePipeline(
                    episodeDurationSec: dur,
                    transcriptCoveredSec: tc,
                    fastTranscriptWatermarkSec: fwm,
                    featureCoverageEndSec: fc
                )
            )
        }
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: rows
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let flaggedHashes: Set<String> = Set(
            health.stalenessFlags
                .filter { $0.kind == .terminalStateContradictsCoverage }
                .map(\.episodeIdHash)
        )
        let fileBugHashes: Set<String> = Set(
            health.assets
                .filter { $0.recommendedAction == .fileBug }
                .map(\.episodeIdHash)
        )
        // Bidirectional agreement.
        #expect(flaggedHashes == fileBugHashes)
        // And the absolute set is what we expect — if this changes,
        // the predicate (or a state contract) shifted and we want to
        // notice.
        #expect(flaggedHashes == [
            "h-cf-both-short",
            "h-cf-feature-short",
            "h-cf-transcript-short",
            "h-legacy-short",
            "h-cfo-short",
            "h-ctp-empty"
        ])
    }

    @Test("nil episode_duration_sec on a completeFull row never flags or recommends fileBug")
    func nilDurationSuppressesContradiction() {
        // Without a duration, we cannot compute a threshold. The
        // contract is "no flag" rather than "always flag" — pin both
        // halves of that.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-no-duration",
                analysisState: "completeFull",
                pipeline: makePipeline(
                    episodeDurationSec: nil,
                    transcriptCoveredSec: 0,
                    fastTranscriptWatermarkSec: 0,
                    featureCoverageEndSec: 0
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        #expect(!health.stalenessFlags.contains { $0.kind == .terminalStateContradictsCoverage })
        #expect(health.assets.first?.recommendedAction != .fileBug)
    }

    @Test("does not flag completeFeatureOnly when transcript is intentionally low")
    func doesNotFlagCompleteFeatureOnlyForLowTranscript() {
        // Feature covered to 99% but transcript only 1% — by the
        // completeFeatureOnly contract this is intentional, not a bug.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-feature-only",
                analysisState: "completeFeatureOnly",
                pipeline: makePipeline(
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: 40,
                    fastTranscriptWatermarkSec: 40,
                    featureCoverageEndSec: 3960
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let kinds: [DogfoodDiagnosticsAnalysisHealth.StalenessFlag.Kind] = health.stalenessFlags.map(\.kind)
        #expect(!kinds.contains(.terminalStateContradictsCoverage))
    }

    @Test("flags stale fast-transcript watermark beyond tolerance")
    func flagsStaleFastTranscriptWatermark() {
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-watermark",
                analysisState: "backfill",
                pipeline: makePipeline(
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: 1500,
                    fastTranscriptWatermarkSec: 90
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        #expect(health.stalenessFlags.contains { $0.kind == .staleFastTranscriptWatermark })
        #expect(health.global.staleWatermarkCount == 1)
    }

    @Test("does not flag watermark gaps below tolerance")
    func doesNotFlagWatermarkGapBelowTolerance() {
        // Tolerance is 60 s; 30 s gap should not flag.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-fresh",
                analysisState: "backfill",
                pipeline: makePipeline(
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: 120,
                    fastTranscriptWatermarkSec: 90
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        #expect(!health.stalenessFlags.contains { $0.kind == .staleFastTranscriptWatermark })
    }

    @Test("flags unknown progress when nothing is paused or unavailable")
    func flagsUnknownProgressWithoutPause() {
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-unknown",
                cachedAudioPresent: false,
                pipeline: makePipeline(
                    downloadPercent: "--%",
                    transcriptPercent: "--%",
                    analysisPercent: "--%"
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        #expect(health.stalenessFlags.contains { $0.kind == .unknownProgressWithoutPause })
    }

    @Test("flags missing failure reason on terminal failure")
    func flagsMissingFailureReason() {
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [
                makeActivityRow(
                    hash: "h-failed-with-reason",
                    analysisState: "failed",
                    terminalReason: "decode_error"
                ),
                makeActivityRow(
                    hash: "h-failed-no-reason",
                    analysisState: "failedTranscript",
                    terminalReason: nil
                )
            ]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let missingReasonHashes = health.stalenessFlags
            .filter { $0.kind == .missingFailureReason }
            .map(\.episodeIdHash)
        #expect(missingReasonHashes == ["h-failed-no-reason"])
    }

    // MARK: - Recommended actions

    @Test("recommends file_bug for terminal-state contradictions")
    func recommendsFileBugForContradictions() {
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h",
                analysisState: "completeFull",
                pipeline: makePipeline(
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: 100,
                    fastTranscriptWatermarkSec: 90,
                    featureCoverageEndSec: 100
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        #expect(health.assets.first?.recommendedAction == .fileBug)
    }

    @Test("recommends retry on terminal failure states")
    func recommendsRetryOnFailure() {
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h",
                analysisState: "failedTranscript",
                terminalReason: "speech_analyzer_unavailable"
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        #expect(health.assets.first?.recommendedAction == .retry)
    }

    @Test("recommends open_app when no audio cached and no live download")
    func recommendsOpenAppForNoCacheNoLive() {
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h",
                cachedAudioPresent: false,
                liveDownloadFraction: nil,
                pipeline: makePipeline(
                    downloadPercent: "--%",
                    transcriptPercent: "--%",
                    analysisPercent: "--%"
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        #expect(health.assets.first?.recommendedAction == .openApp)
    }

    @Test("recommends plug_in_or_wait for queued cached rows with stale watermark")
    func recommendsPlugInOrWaitForStaleWatermarks() {
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h",
                cachedAudioPresent: true,
                pipeline: makePipeline(
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: 3000,
                    fastTranscriptWatermarkSec: 90
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        #expect(health.assets.first?.recommendedAction == .plugInOrWait)
    }

    @Test("recommends wait for currently running rows")
    func recommendsWaitForRunning() {
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h",
                analysisState: "backfill",
                isRunning: true
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        #expect(health.assets.first?.recommendedAction == .wait)
    }

    @Test("recommends wait for healthy terminal-completion rows even when transcriptPercent is --%")
    func recommendsWaitForHealthyTerminalCompletion() {
        // R4 fix: a healthy terminal-completion row (no failure, no
        // contradiction, no stale lease/watermark hazard) must
        // recommend `.wait`, not fall through to the queued+cached
        // branch. The interesting case is `completeFeatureOnly` whose
        // intentionally-low transcript leaves transcriptPercent at
        // "--%" — under the previous logic that would route to
        // `.plugInOrWait` and tell the user to plug in for an asset
        // that is already terminally complete. Pin every healthy
        // completion state so a future regression cannot misclassify
        // the row as thermal-blocked.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [
                // completeFeatureOnly with intentionally-low transcript.
                makeActivityRow(
                    hash: "h-cfo-healthy",
                    analysisState: "completeFeatureOnly",
                    pipeline: makePipeline(
                        transcriptPercent: "--%",
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 40,
                        fastTranscriptWatermarkSec: 40,
                        featureCoverageEndSec: 3960
                    )
                ),
                // completeTranscriptPartial with healthy partial coverage.
                makeActivityRow(
                    hash: "h-ctp-healthy",
                    analysisState: "completeTranscriptPartial",
                    pipeline: makePipeline(
                        transcriptPercent: "60%",
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 2400,
                        fastTranscriptWatermarkSec: 2400,
                        featureCoverageEndSec: 4000
                    )
                ),
                // completeFull, fully healthy.
                makeActivityRow(
                    hash: "h-cf-healthy",
                    analysisState: "completeFull",
                    pipeline: makePipeline(
                        transcriptPercent: "98%",
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 3960,
                        fastTranscriptWatermarkSec: 3960,
                        featureCoverageEndSec: 4000
                    )
                ),
                // legacy `complete`, fully healthy.
                makeActivityRow(
                    hash: "h-legacy-healthy",
                    analysisState: "complete",
                    pipeline: makePipeline(
                        transcriptPercent: "98%",
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 3960,
                        fastTranscriptWatermarkSec: 3960,
                        featureCoverageEndSec: 4000
                    )
                )
            ]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let actions = health.assets.map(\.recommendedAction)
        #expect(actions == [.wait, .wait, .wait, .wait])
        // R5: pin the note string too, otherwise a future rename of
        // the wire annotation would slip past the action-only check.
        // Support tooling that branches on this string would silently
        // miss the new value.
        let notes = health.assets.map(\.recommendedActionNote)
        #expect(notes == [
            "healthy_terminal_completion",
            "healthy_terminal_completion",
            "healthy_terminal_completion",
            "healthy_terminal_completion"
        ])
        // None of the healthy completions may be flagged for
        // contradiction (independent confirmation that the new branch
        // is gated correctly).
        #expect(!health.stalenessFlags.contains { $0.kind == .terminalStateContradictsCoverage })
    }

    @Test("terminal-completion rows with stale watermark drift recommend wait, not plug in")
    func terminalCompletionWithStaleWatermarkRecommendsWait() {
        // R5 fix: the May 6 `asset_004` shape — a `completeFull` row
        // whose persisted `fast_transcript_watermark_sec` lags the
        // real `transcript_covered_sec` by minutes — was incorrectly
        // routed to `.plugInOrWait` "stale_watermark_delta=..." under
        // R4's branch ordering. The asset is already terminally
        // complete; the watermark drift is a benign persistence
        // hazard (playhead-3bv.2), not a thermal block. The user has
        // nothing to do; the staleness flag still surfaces the drift
        // for support visibility.
        //
        // Pin: action=.wait, note="healthy_terminal_completion", AND
        // the staleFastTranscriptWatermark flag is still raised so a
        // future fix that suppresses the FLAG (instead of just
        // changing the recommendation) is caught here.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-asset004",
                analysisState: "completeFull",
                cachedAudioPresent: true,
                pipeline: makePipeline(
                    transcriptPercent: "99%",
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: 3960,
                    fastTranscriptWatermarkSec: 90, // 65-minute drift
                    featureCoverageEndSec: 4000
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let asset = try! #require(health.assets.first)
        #expect(asset.recommendedAction == .wait)
        #expect(asset.recommendedActionNote == "healthy_terminal_completion")
        // The flag still fires for support visibility.
        #expect(health.stalenessFlags.contains { $0.kind == .staleFastTranscriptWatermark })
        #expect(health.global.staleWatermarkCount == 1)
        // And the row is NOT a contradiction (transcript axis = 3960
        // ≥ threshold 3800 via best-known fallback).
        #expect(!health.stalenessFlags.contains { $0.kind == .terminalStateContradictsCoverage })
    }

    @Test("terminal-completion rows with evicted audio recommend wait, not open_app")
    func terminalCompletionWithEvictedAudioRecommendsWait() {
        // R6 fix: a `completeFull` (or any terminal-completion) row
        // whose cached audio was evicted post-completion — the normal
        // storage-budget path — must NOT be misrouted to `.openApp`.
        // The asset is already terminally complete; there is no
        // analysis work the foreground manager could resume by
        // having the user reopen the app. Audio eviction is a
        // playback-tier concern handled at play time, not an
        // analysis hazard.
        //
        // Without R6's reorder, the no-cached-audio + no-live +
        // downloadPercent="--%" branch fires before the healthy-
        // terminal-completion gate and incorrectly tells the user
        // to open the app. Pin every healthy completion state so a
        // future regression cannot reintroduce the misclassification.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [
                makeActivityRow(
                    hash: "h-cf-evicted",
                    analysisState: "completeFull",
                    cachedAudioPresent: false,
                    liveDownloadFraction: nil,
                    pipeline: makePipeline(
                        downloadPercent: "--%",
                        transcriptPercent: "98%",
                        analysisPercent: "100%",
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 3960,
                        fastTranscriptWatermarkSec: 3960,
                        featureCoverageEndSec: 4000
                    )
                ),
                makeActivityRow(
                    hash: "h-cfo-evicted",
                    analysisState: "completeFeatureOnly",
                    cachedAudioPresent: false,
                    liveDownloadFraction: nil,
                    pipeline: makePipeline(
                        downloadPercent: "--%",
                        transcriptPercent: "--%",
                        analysisPercent: "100%",
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 40,
                        fastTranscriptWatermarkSec: 40,
                        featureCoverageEndSec: 3960
                    )
                ),
                makeActivityRow(
                    hash: "h-ctp-evicted",
                    analysisState: "completeTranscriptPartial",
                    cachedAudioPresent: false,
                    liveDownloadFraction: nil,
                    pipeline: makePipeline(
                        downloadPercent: "--%",
                        transcriptPercent: "60%",
                        analysisPercent: "100%",
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 2400,
                        fastTranscriptWatermarkSec: 2400,
                        featureCoverageEndSec: 4000
                    )
                ),
                makeActivityRow(
                    hash: "h-legacy-evicted",
                    analysisState: "complete",
                    cachedAudioPresent: false,
                    liveDownloadFraction: nil,
                    pipeline: makePipeline(
                        downloadPercent: "--%",
                        transcriptPercent: "98%",
                        analysisPercent: "100%",
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 3960,
                        fastTranscriptWatermarkSec: 3960,
                        featureCoverageEndSec: 4000
                    )
                )
            ]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let actions = health.assets.map(\.recommendedAction)
        #expect(actions == [.wait, .wait, .wait, .wait])
        let notes = health.assets.map(\.recommendedActionNote)
        #expect(notes == [
            "healthy_terminal_completion",
            "healthy_terminal_completion",
            "healthy_terminal_completion",
            "healthy_terminal_completion"
        ])
        // Independent confirmation: the openApp branch is genuinely
        // unreachable for these rows. A non-terminal row with the
        // same cached/download shape still routes to .openApp (the
        // existing recommendsOpenAppForNoCacheNoLive test pins this
        // half).
        #expect(!actions.contains(.openApp))
    }

    @Test("terminal-completion rows with stale lease recommend wait, not clear_stale_lease")
    func terminalCompletionWithStaleLeaseRecommendsWait() {
        // R6 fix companion: a terminal-completion row that also has
        // a leased durable-job row whose lease has outlived the
        // latest session is benign — the lease will be reaped on
        // the next BG-task tick with no user action required, and
        // the asset is already terminally complete. Without the R6
        // reorder, the stale-lease branch would fire ahead of the
        // healthy-terminal-completion gate.
        //
        // The `staleJobLease` flag still fires in the
        // `staleness_flags` list for support visibility — that's
        // pinned independently below so a future fix that
        // suppresses the flag (instead of just changing the
        // recommendation) is caught here.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-completed-stale-lease",
                analysisState: "completeFull",
                cachedAudioPresent: true,
                pipeline: makePipeline(
                    transcriptPercent: "99%",
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: 3960,
                    fastTranscriptWatermarkSec: 3960,
                    featureCoverageEndSec: 4000
                ),
                latestSession: makeSession(updatedAt: 200.0),
                latestJob: makeJob(
                    leasePresent: true,
                    leaseExpiresAt: 100.0
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let asset = try! #require(health.assets.first)
        #expect(asset.recommendedAction == .wait)
        #expect(asset.recommendedActionNote == "healthy_terminal_completion")
        // Flag still fires for support visibility.
        #expect(health.stalenessFlags.contains { $0.kind == .staleJobLease })
    }

    @Test("running rows recommend wait, not plug_in_or_wait, even when watermark is stale")
    func runningRowWithStaleWatermarkRecommendsWait() {
        // R7 fix: a row currently executing (`isRunning == true`)
        // should never be classified as "thermal-blocked, plug in"
        // just because its persisted watermark drift exceeds the
        // tolerance. The active runner IS in the middle of catching
        // the watermark up; the user-facing answer is "we're working
        // on it, stand by", not "plug in." This is the same adjacent-
        // pair pattern R4/R5/R6 fixed at higher gates — a less-specific
        // hazard masking a more-specific truth.
        //
        // Pin: action=.wait, note="currently_running", AND the
        // staleFastTranscriptWatermark flag still fires for support
        // visibility so a future fix that suppresses the FLAG (instead
        // of just changing the recommendation) is caught here.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-running-stale-watermark",
                analysisState: "backfill",
                isRunning: true,
                cachedAudioPresent: true,
                pipeline: makePipeline(
                    transcriptPercent: "30%",
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: 1500,
                    fastTranscriptWatermarkSec: 90 // 23.5-minute drift
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let asset = try! #require(health.assets.first)
        #expect(asset.recommendedAction == .wait)
        #expect(asset.recommendedActionNote == "currently_running")
        // The flag still fires for support visibility.
        #expect(health.stalenessFlags.contains { $0.kind == .staleFastTranscriptWatermark })
        #expect(health.global.staleWatermarkCount == 1)
    }

    @Test("running rows recommend wait, not clear_stale_lease, even when the lease residue is present")
    func runningRowWithStaleLeaseRecommendsWait() {
        // R8 fix: a row currently executing (`isRunning == true`)
        // should never be classified as "clear a stale lease" just
        // because the lease's expiry slipped behind the latest
        // session's `updatedAt`. The active runner is mid-flight; the
        // durable-job lease residue will be reaped on the next
        // BG-task tick or as the runner finalizes the chunk it's
        // currently on. This is the same R7 ordering principle
        // (running outranks every persistence-artifact hazard hint),
        // applied to the staleLease vs running adjacent pair.
        //
        // Pin: action=.wait, note="currently_running", AND the
        // staleJobLease flag still fires for support visibility so a
        // future fix that suppresses the FLAG (instead of just
        // changing the recommendation) is caught here.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-running-stale-lease",
                analysisState: "backfill",
                isRunning: true,
                cachedAudioPresent: true,
                latestSession: makeSession(updatedAt: 200.0),
                latestJob: makeJob(
                    leasePresent: true,
                    leaseExpiresAt: 100.0
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let asset = try! #require(health.assets.first)
        #expect(asset.recommendedAction == .wait)
        #expect(asset.recommendedActionNote == "currently_running")
        // The flag still fires for support visibility.
        #expect(health.stalenessFlags.contains { $0.kind == .staleJobLease })
    }

    @Test("running rows recommend wait, not open_app, even when cached audio is missing")
    func runningRowWithMissingCachedAudioRecommendsWait() {
        // R8 fix companion: a row currently executing
        // (`isRunning == true`) should never be classified as
        // ".openApp" just because `cachedAudioPresent == false` and
        // download_percent is "--%". The combination is itself a
        // transient state — an active runner mid-flight cannot be
        // running with no audio source — but if it ever surfaces,
        // the user-facing answer is "we're working on it" not
        // "open the app so the foreground manager resumes." Same
        // R4/R5/R6/R7 principle applied to the noCachedAudio vs
        // running pair.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-running-no-cache",
                analysisState: "backfill",
                isRunning: true,
                cachedAudioPresent: false,
                liveDownloadFraction: nil,
                pipeline: makePipeline(
                    downloadPercent: "--%",
                    transcriptPercent: "--%",
                    analysisPercent: "--%"
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let asset = try! #require(health.assets.first)
        #expect(asset.recommendedAction == .wait)
        #expect(asset.recommendedActionNote == "currently_running")
    }

    @Test("recommendation branch ordering is exhaustive across every adjacent pair")
    func recommendationBranchOrderingExhaustive() {
        // Adjacent-pair audit (R8): for each adjacent pair (A, B) in
        // the branch order
        //   failure → contradiction → healthyTerminalCompletion →
        //   running → noCachedAudio → staleLease →
        //   staleWatermark → cachedAndQueued → unknown
        // assert that a row simultaneously triggering A's predicate
        // AND B's predicate routes to A. Each case is a single row
        // engineered to satisfy BOTH predicates; the assertion fails
        // loudly if a future reorder swaps the gate.
        //
        // R9: where two adjacent gates share the same `RecommendedAction`
        // (notably `.wait` for healthyTerminalCompletion vs running, and
        // `.wait` for cachedAndQueued's progress branch), the action
        // alone cannot distinguish — so the Case also pins the
        // expected NOTE string. A future swap that reroutes through a
        // different gate but still happens to return `.wait` would
        // pass an action-only assertion silently; the note assertion
        // fails loudly.
        struct Case {
            let name: String
            let row: DogfoodDiagnosticsActivityRow
            let expected: DogfoodDiagnosticsAnalysisHealth.RecommendedAction
            /// When non-nil, also assert the recommended_action_note
            /// matches this string verbatim. Used for adjacent pairs
            /// whose two gates emit the same action with different
            /// notes — see header comment.
            let expectedNote: String?
        }
        let cases: [Case] = [
            // failure outranks contradiction (terminal failure +
            // completion-state coverage shape — the failure state
            // wins; isTerminalCompletionState and
            // isTerminalFailureState are disjoint, but the ordering
            // is still a load-bearing contract — pin it.)
            //
            // R9 audit: `analysisState` is single-valued, and each
            // string is either a terminal-failure name or a terminal-
            // completion name (or neither), never both. So the failure
            // and contradiction predicates cannot fire on the same
            // row by construction — this Case is a "branch-order
            // sanity pin" rather than a true dual-trigger row. The
            // mutual exclusion is enforced in code by
            // `isTerminalFailureState`/`isTerminalCompletionState`
            // having disjoint case lists.
            Case(
                name: "failure-vs-contradiction",
                row: makeActivityRow(
                    hash: "fc",
                    analysisState: "failedTranscript",
                    terminalReason: "decode_error",
                    pipeline: makePipeline(
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 100,
                        fastTranscriptWatermarkSec: 100,
                        featureCoverageEndSec: 100
                    )
                ),
                expected: .retry,
                expectedNote: nil
            ),
            // contradiction outranks healthyTerminalCompletion
            // (a `completeFull` row with low coverage IS a
            // contradiction; healthy terminal is unreachable.)
            Case(
                name: "contradiction-vs-healthyTerminal",
                row: makeActivityRow(
                    hash: "ch",
                    analysisState: "completeFull",
                    pipeline: makePipeline(
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 100,
                        fastTranscriptWatermarkSec: 100,
                        featureCoverageEndSec: 100
                    )
                ),
                expected: .fileBug,
                expectedNote: nil
            ),
            // healthyTerminal outranks running (a row in a healthy
            // terminal-completion state should never be told
            // "currently running" even if isRunning leaks true).
            //
            // R9 fix: BOTH gates emit `.wait`, so the action alone
            // cannot distinguish a swap of the gate order. Pin the
            // note ("healthy_terminal_completion" vs "currently_running")
            // so an order swap fails loudly.
            Case(
                name: "healthyTerminal-vs-running",
                row: makeActivityRow(
                    hash: "hr",
                    analysisState: "completeFull",
                    isRunning: true,
                    pipeline: makePipeline(
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 3960,
                        fastTranscriptWatermarkSec: 3960,
                        featureCoverageEndSec: 4000
                    )
                ),
                expected: .wait,
                expectedNote: "healthy_terminal_completion"
            ),
            // running outranks noCachedAudio.
            Case(
                name: "running-vs-noCachedAudio",
                row: makeActivityRow(
                    hash: "rn",
                    analysisState: "backfill",
                    isRunning: true,
                    cachedAudioPresent: false,
                    liveDownloadFraction: nil,
                    pipeline: makePipeline(
                        downloadPercent: "--%",
                        transcriptPercent: "--%",
                        analysisPercent: "--%"
                    )
                ),
                expected: .wait,
                expectedNote: "currently_running"
            ),
            // running outranks staleLease.
            Case(
                name: "running-vs-staleLease",
                row: makeActivityRow(
                    hash: "rl",
                    analysisState: "backfill",
                    isRunning: true,
                    cachedAudioPresent: true,
                    latestSession: makeSession(updatedAt: 200.0),
                    latestJob: makeJob(leasePresent: true, leaseExpiresAt: 100.0)
                ),
                expected: .wait,
                expectedNote: "currently_running"
            ),
            // running outranks staleWatermark (R7).
            Case(
                name: "running-vs-staleWatermark",
                row: makeActivityRow(
                    hash: "rw",
                    analysisState: "backfill",
                    isRunning: true,
                    cachedAudioPresent: true,
                    pipeline: makePipeline(
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 1500,
                        fastTranscriptWatermarkSec: 90
                    )
                ),
                expected: .wait,
                expectedNote: "currently_running"
            ),
            // noCachedAudio outranks staleLease.
            Case(
                name: "noCachedAudio-vs-staleLease",
                row: makeActivityRow(
                    hash: "nl",
                    analysisState: "backfill",
                    cachedAudioPresent: false,
                    liveDownloadFraction: nil,
                    pipeline: makePipeline(
                        downloadPercent: "--%",
                        transcriptPercent: "--%",
                        analysisPercent: "--%"
                    ),
                    latestSession: makeSession(updatedAt: 200.0),
                    latestJob: makeJob(leasePresent: true, leaseExpiresAt: 100.0)
                ),
                expected: .openApp,
                expectedNote: nil
            ),
            // staleLease outranks staleWatermark.
            Case(
                name: "staleLease-vs-staleWatermark",
                row: makeActivityRow(
                    hash: "lw",
                    analysisState: "backfill",
                    cachedAudioPresent: true,
                    pipeline: makePipeline(
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 1500,
                        fastTranscriptWatermarkSec: 90
                    ),
                    latestSession: makeSession(updatedAt: 200.0),
                    latestJob: makeJob(leasePresent: true, leaseExpiresAt: 100.0)
                ),
                expected: .clearStaleLease,
                expectedNote: nil
            ),
            // staleWatermark outranks cachedAndQueued. The cached+queued
            // gate also runs on this shape; staleWatermark wins.
            Case(
                name: "staleWatermark-vs-cachedAndQueued",
                row: makeActivityRow(
                    hash: "wq",
                    disposition: "queued",
                    analysisState: "backfill",
                    cachedAudioPresent: true,
                    pipeline: makePipeline(
                        downloadPercent: "100%",
                        transcriptPercent: "30%",
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 1500,
                        fastTranscriptWatermarkSec: 90
                    )
                ),
                expected: .plugInOrWait,
                expectedNote: nil
            ),
            // cachedAndQueued outranks unknown (a queued cached row
            // with progress should not fall through to .unknown).
            //
            // R9 fix: cachedAndQueued's progress branch emits `.wait`
            // with note "queued_with_progress"; if the gate were
            // somehow reordered or removed and the row fell through
            // to `.unknown`, the action would change to `.unknown`
            // and the note would become nil — both distinguishable.
            // Pin the note so a future drift in cachedAndQueued's
            // emit shape is also caught here.
            Case(
                name: "cachedAndQueued-vs-unknown",
                row: makeActivityRow(
                    hash: "qu",
                    disposition: "queued",
                    analysisState: "queued",
                    cachedAudioPresent: true,
                    pipeline: makePipeline(
                        downloadPercent: "100%",
                        transcriptPercent: "30%",
                        analysisPercent: "20%"
                    )
                ),
                expected: .wait,
                expectedNote: "queued_with_progress"
            )
        ]
        for testCase in cases {
            let snapshot = DogfoodDiagnosticsActivitySnapshot(
                generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
                rows: [testCase.row]
            )
            let health = DogfoodDiagnosticsAnalysisHealth.build(
                from: snapshot,
                generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
            )
            let asset = try! #require(
                health.assets.first,
                "missing asset for \(testCase.name)"
            )
            #expect(
                asset.recommendedAction == testCase.expected,
                "wrong action for \(testCase.name): got \(asset.recommendedAction.rawValue), expected \(testCase.expected.rawValue)"
            )
            if let expectedNote = testCase.expectedNote {
                #expect(
                    asset.recommendedActionNote == expectedNote,
                    "wrong note for \(testCase.name): got \(asset.recommendedActionNote ?? "<nil>"), expected \(expectedNote)"
                )
            }
        }
    }

    @Test("unknown is reachable for a recognizable but non-actionable row shape")
    func unknownIsReachableForResidualRowShape() {
        // R9 (task #2): the catch-all `.unknown` branch is the
        // last resort in `recommendation(for:)`. Earlier rounds
        // (R4..R8) added gates that route what used to fall through
        // — risking an unreachable `.unknown` branch in practice.
        // Pin a row shape that genuinely lands in `.unknown` so a
        // future reorder cannot accidentally make the branch dead
        // without us noticing (an unreachable branch is a code
        // smell that should be removed, not silently kept around).
        //
        // R11: realistic production scenario — a user-paused row whose
        // analysis was mid-flight when the user hit pause. The row
        // disposition is `paused` (real, written by the activity-snapshot
        // section assignment), and the persisted `analysisState` is one
        // of the in-progress `SessionState` raw values
        // (`waitingForBackfill` here; could also be `spooling`,
        // `featuresReady`, `hotPathReady`, or `backfill`). The earlier
        // R9 comment named `analysisState="paused"`, which is NOT a
        // valid `SessionState` raw value and cannot be written by
        // production code — switching to a realistic value keeps the
        // test honest while preserving the route to `.unknown`. The
        // route is identical: `waitingForBackfill` is neither a
        // completion nor a failure, so the contradiction predicate
        // skips it and the running/no-cached/lease/watermark gates
        // also skip.
        //
        // The row that hits `.unknown` is one that:
        //   * is NOT terminal failure (waitingForBackfill is non-terminal)
        //   * is NOT terminal contradiction (non-completion state, so
        //     terminalCompletionContradiction returns nil)
        //   * is NOT terminal completion
        //   * is NOT running (isRunning=false)
        //   * does NOT match noCachedAudio (cachedAudioPresent=true)
        //   * does NOT match staleLease (no lease present)
        //   * does NOT match staleWatermark (no drift > tolerance)
        //   * does NOT match cachedAndQueued (disposition="paused",
        //     not "queued")
        // → falls through to `.unknown`.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-unknown",
                disposition: "paused",
                reason: "user_paused",
                analysisState: "waitingForBackfill",
                isRunning: false,
                cachedAudioPresent: true,
                pipeline: makePipeline(
                    downloadPercent: "100%",
                    transcriptPercent: "20%",
                    analysisPercent: "10%"
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let asset = try! #require(health.assets.first)
        #expect(asset.recommendedAction == .unknown)
        #expect(asset.recommendedActionNote == nil)
    }

    @Test("running rows route to wait whether progress evidence is present or absent")
    func runningRowRoutingIsIndependentOfProgressEvidence() {
        // R9 (task #1): R8 placed the running gate ahead of every
        // less-specific persistence-artifact gate (noCachedAudio,
        // staleLease, staleWatermark). The R9 brief asks whether a
        // row with `isRunning == true` AND no progress evidence at
        // all (e.g. all percents "--%") should still recommend
        // `.wait` — or whether the combination indicates a real
        // problem (e.g. the audio file evicted while a job runs)
        // that should bypass `.wait` for `.clearStaleLease` /
        // `.fileBug` / similar.
        //
        // Decision: BOTH shapes route to `.wait "currently_running"`.
        // The reasoning:
        //   * `isRunning` is computed at snapshot time off
        //     `runningEpisodeId` from the AnalysisStore — it is a
        //     direct truth ("a job is processing this episode RIGHT
        //     NOW"), not a persisted residue, so a stale "pseudo-
        //     running" flag is not a thing.
        //   * If the runner is mid-flight, the user's actionable
        //     answer is "we're working on it, stand by" — even if
        //     the snapshot's other fields look incoherent, those are
        //     transient point-in-time observations the runner is in
        //     the middle of resolving.
        //   * The corresponding staleness flags
        //     (`staleJobLease`, `staleFastTranscriptWatermark`,
        //     `unknownProgressWithoutPause`) STILL surface for
        //     support visibility; only the per-asset RECOMMENDATION
        //     collapses to `.wait`.
        //
        // This test pins both ends of the spectrum: a running row
        // with healthy progress, and a running row with no progress
        // evidence at all. A future regression that splits the
        // running gate into "running with evidence" vs "running
        // without evidence" needs to update this test deliberately.
        struct Shape {
            let name: String
            let row: DogfoodDiagnosticsActivityRow
        }
        let shapes: [Shape] = [
            Shape(
                name: "running_with_progress_evidence",
                row: makeActivityRow(
                    hash: "h-run-progress",
                    analysisState: "backfill",
                    isRunning: true,
                    cachedAudioPresent: true,
                    pipeline: makePipeline(
                        downloadPercent: "100%",
                        transcriptPercent: "60%",
                        analysisPercent: "40%",
                        episodeDurationSec: 3000,
                        transcriptCoveredSec: 1800,
                        fastTranscriptWatermarkSec: 1800
                    )
                )
            ),
            Shape(
                name: "running_without_progress_evidence",
                row: makeActivityRow(
                    hash: "h-run-no-progress",
                    analysisState: "backfill",
                    isRunning: true,
                    cachedAudioPresent: true,
                    pipeline: makePipeline(
                        downloadPercent: "--%",
                        transcriptPercent: "--%",
                        analysisPercent: "--%"
                    )
                )
            )
        ]
        for shape in shapes {
            let snapshot = DogfoodDiagnosticsActivitySnapshot(
                generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
                rows: [shape.row]
            )
            let health = DogfoodDiagnosticsAnalysisHealth.build(
                from: snapshot,
                generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
            )
            let asset = try! #require(
                health.assets.first,
                "missing asset for \(shape.name)"
            )
            #expect(
                asset.recommendedAction == .wait,
                "wrong action for \(shape.name): got \(asset.recommendedAction.rawValue)"
            )
            #expect(
                asset.recommendedActionNote == "currently_running",
                "wrong note for \(shape.name)"
            )
        }
    }

    @Test("global summary counts are stable across rows that hit each reordered branch")
    func globalSummaryStableAcrossReorderedBranches() {
        // Aggregation invariant (task #2): GlobalSummary counters
        // (totals, dispositions, terminal-completed, stale flags,
        // unknown-progress) are derived from input row state directly,
        // NOT from recommended-action routing. Confirm that mixing
        // rows that hit each of the R4..R8 reordered branches in a
        // single snapshot still produces the expected counts.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [
                // R4: healthy terminal completion (transcriptPercent="--%").
                makeActivityRow(
                    hash: "g-r4-htc",
                    analysisState: "completeFeatureOnly",
                    pipeline: makePipeline(
                        transcriptPercent: "--%",
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 40,
                        fastTranscriptWatermarkSec: 40,
                        featureCoverageEndSec: 3960
                    )
                ),
                // R5: terminal completion + stale watermark.
                makeActivityRow(
                    hash: "g-r5-stale-watermark-terminal",
                    analysisState: "completeFull",
                    pipeline: makePipeline(
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 3960,
                        fastTranscriptWatermarkSec: 90,
                        featureCoverageEndSec: 4000
                    )
                ),
                // R6: terminal completion + evicted audio.
                makeActivityRow(
                    hash: "g-r6-evicted-terminal",
                    analysisState: "completeFull",
                    cachedAudioPresent: false,
                    liveDownloadFraction: nil,
                    pipeline: makePipeline(
                        downloadPercent: "--%",
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 3960,
                        fastTranscriptWatermarkSec: 3960,
                        featureCoverageEndSec: 4000
                    )
                ),
                // R7: running + stale watermark.
                makeActivityRow(
                    hash: "g-r7-running-stale-watermark",
                    analysisState: "backfill",
                    isRunning: true,
                    cachedAudioPresent: true,
                    pipeline: makePipeline(
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 1500,
                        fastTranscriptWatermarkSec: 90
                    )
                ),
                // R8: running + stale lease.
                makeActivityRow(
                    hash: "g-r8-running-stale-lease",
                    analysisState: "backfill",
                    isRunning: true,
                    cachedAudioPresent: true,
                    latestSession: makeSession(updatedAt: 200.0),
                    latestJob: makeJob(leasePresent: true, leaseExpiresAt: 100.0)
                )
            ]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        // Per-row routing (sanity) — each row hits the correct branch.
        let actions = health.assets.map(\.recommendedAction)
        #expect(actions == [.wait, .wait, .wait, .wait, .wait])
        // Global aggregates are independent of recommendation routing.
        #expect(health.global.totalAssets == 5)
        #expect(health.global.runningCount == 2) // R7, R8
        #expect(health.global.queuedCount == 5)  // disposition default = "queued"
        #expect(health.global.terminalCompletedCount == 3) // R4, R5, R6
        // Two rows have stale-watermark drift (R5: terminal +
        // R7: running). Both still appear in staleness_flags
        // regardless of recommendation routing.
        #expect(health.global.staleWatermarkCount == 2)
        // Stale lease appears on R8 (running) and is still flagged
        // even though the recommendation says "currently_running".
        let leaseCount = health.stalenessFlags.filter {
            $0.kind == .staleJobLease
        }.count
        #expect(leaseCount == 1)
        // No terminal contradictions — R4/R5/R6 are all healthy.
        #expect(health.global.staleTerminalCount == 0)
    }

    @Test("captureNote prefixes are stable for both build paths")
    func captureNotePrefixesAreStable() {
        // R8: every emitted note-string prefix should be pinned.
        // captureNote has two deterministic shapes:
        //   * "activity_capture_error: …" — when build(from:) sees a
        //     non-nil captureError on the input snapshot.
        //   * "no_activity_snapshot: …" — when noSnapshot(reason:)
        //     constructs an empty summary because the activity
        //     snapshot itself was nil.
        // Support tooling routes on these prefixes. Pin both.
        let captureErrorSnapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [],
            captureError: "io_open_failed"
        )
        let withErrorHealth = DogfoodDiagnosticsAnalysisHealth.build(
            from: captureErrorSnapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let captureErrorNote = try! #require(withErrorHealth.captureNote)
        #expect(captureErrorNote.hasPrefix("activity_capture_error:"))
        #expect(captureErrorNote.contains("io_open_failed"))

        let noSnapshotHealth = DogfoodDiagnosticsAnalysisHealth.noSnapshot(
            reason: "analysis_store_unopened",
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let noSnapshotNote = try! #require(noSnapshotHealth.captureNote)
        #expect(noSnapshotNote.hasPrefix("no_activity_snapshot:"))
        #expect(noSnapshotNote.contains("analysis_store_unopened"))
    }

    @Test("recommendation note strings are stable for every deterministic branch")
    func recommendationNoteStringsAreStable() {
        // R6: every deterministic (non-parameterized) note string
        // must be pinned so support tooling can route on the value
        // without the rug pulled out from under it. Parameterized
        // notes (stale_watermark_delta=…, lease_expires_at=…) are
        // pinned by prefix in the per-branch tests already; this
        // test pins the constant-string branches end-to-end.
        struct Case {
            let name: String
            let row: DogfoodDiagnosticsActivityRow
            let expectedAction: DogfoodDiagnosticsAnalysisHealth.RecommendedAction
            let expectedNote: String
        }
        let cases: [Case] = [
            Case(
                name: "healthy_terminal_completion",
                row: makeActivityRow(
                    hash: "h-htc",
                    analysisState: "completeFull",
                    pipeline: makePipeline(
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 3960,
                        fastTranscriptWatermarkSec: 3960,
                        featureCoverageEndSec: 4000
                    )
                ),
                expectedAction: .wait,
                expectedNote: "healthy_terminal_completion"
            ),
            Case(
                name: "no_cached_audio_and_no_live_download",
                row: makeActivityRow(
                    hash: "h-nca",
                    cachedAudioPresent: false,
                    liveDownloadFraction: nil,
                    pipeline: makePipeline(
                        downloadPercent: "--%",
                        transcriptPercent: "--%",
                        analysisPercent: "--%"
                    )
                ),
                expectedAction: .openApp,
                expectedNote: "no_cached_audio_and_no_live_download"
            ),
            Case(
                name: "currently_running",
                row: makeActivityRow(
                    hash: "h-running",
                    analysisState: "backfill",
                    isRunning: true
                ),
                expectedAction: .wait,
                expectedNote: "currently_running"
            ),
            Case(
                name: "queued_with_cached_audio_no_transcript",
                row: makeActivityRow(
                    hash: "h-qcant",
                    analysisState: "queued",
                    cachedAudioPresent: true,
                    pipeline: makePipeline(
                        downloadPercent: "100%",
                        transcriptPercent: "--%",
                        analysisPercent: "--%"
                    )
                ),
                expectedAction: .plugInOrWait,
                expectedNote: "queued_with_cached_audio_no_transcript"
            ),
            Case(
                name: "queued_with_progress",
                row: makeActivityRow(
                    hash: "h-qwp",
                    analysisState: "queued",
                    cachedAudioPresent: true,
                    pipeline: makePipeline(
                        downloadPercent: "100%",
                        transcriptPercent: "30%",
                        analysisPercent: "20%"
                    )
                ),
                expectedAction: .wait,
                expectedNote: "queued_with_progress"
            )
        ]
        for testCase in cases {
            let snapshot = DogfoodDiagnosticsActivitySnapshot(
                generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
                rows: [testCase.row]
            )
            let health = DogfoodDiagnosticsAnalysisHealth.build(
                from: snapshot,
                generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
            )
            let asset = try! #require(health.assets.first, "missing asset for \(testCase.name)")
            #expect(asset.recommendedAction == testCase.expectedAction, "wrong action for \(testCase.name)")
            #expect(asset.recommendedActionNote == testCase.expectedNote, "wrong note for \(testCase.name)")
        }
    }

    @Test("parameterized recommendation notes carry stable prefixes")
    func parameterizedRecommendationNotePrefixesAreStable() {
        // R6: pin the prefix for the two parameterized note strings
        // (stale_watermark_delta=… and lease_expires_at=…). Support
        // tooling can branch on the prefix without parsing the
        // numeric tail; if the prefix drifts, this test catches it.

        // stale_watermark_delta=…
        let staleWatermarkSnapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-swm",
                analysisState: "backfill",
                cachedAudioPresent: true,
                pipeline: makePipeline(
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: 1500,
                    fastTranscriptWatermarkSec: 90
                )
            )]
        )
        let staleWatermarkHealth = DogfoodDiagnosticsAnalysisHealth.build(
            from: staleWatermarkSnapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let staleWatermarkAsset = try! #require(staleWatermarkHealth.assets.first)
        #expect(staleWatermarkAsset.recommendedAction == .plugInOrWait)
        let staleWatermarkNote = try! #require(staleWatermarkAsset.recommendedActionNote)
        #expect(staleWatermarkNote.hasPrefix("stale_watermark_delta="))

        // lease_expires_at=… session_updated_at=…
        let staleLeaseSnapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-sl",
                analysisState: "backfill",
                cachedAudioPresent: true,
                latestSession: makeSession(updatedAt: 200.0),
                latestJob: makeJob(leasePresent: true, leaseExpiresAt: 100.0)
            )]
        )
        let staleLeaseHealth = DogfoodDiagnosticsAnalysisHealth.build(
            from: staleLeaseSnapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let staleLeaseAsset = try! #require(staleLeaseHealth.assets.first)
        #expect(staleLeaseAsset.recommendedAction == .clearStaleLease)
        let staleLeaseNote = try! #require(staleLeaseAsset.recommendedActionNote)
        #expect(staleLeaseNote.hasPrefix("lease_expires_at="))
        #expect(staleLeaseNote.contains("session_updated_at="))

        // R7: the failure-state retry note carries `session_state=…
        // reason=…`. R6's drift coverage focused on the deterministic
        // notes plus the watermark/lease prefixes, but the retry path's
        // parameterized prefix slipped through. Pin it here so support
        // tooling that branches on the value catches a future rename.
        let failureSnapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-fail",
                analysisState: "failedTranscript",
                terminalReason: "speech_analyzer_unavailable"
            )]
        )
        let failureHealth = DogfoodDiagnosticsAnalysisHealth.build(
            from: failureSnapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let failureAsset = try! #require(failureHealth.assets.first)
        #expect(failureAsset.recommendedAction == .retry)
        let failureNote = try! #require(failureAsset.recommendedActionNote)
        #expect(failureNote.hasPrefix("session_state="))
        #expect(failureNote.contains("reason="))
    }

    @Test("staleness flag detail strings carry stable prefixes")
    func stalenessFlagDetailPrefixesAreStable() {
        // R7: the parallel pin to R6's recommendation-note prefix
        // coverage. Each StalenessFlag.detail follows a deterministic
        // shape (prefix-only for parameterized strings, full string
        // for the missingFailureReason `state=…` form). Support tooling
        // routes on these prefixes; a future rename should fail loudly
        // here, not silently in production.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [
                // terminalStateContradictsCoverage → "state=… duration=…"
                makeActivityRow(
                    hash: "h-tc",
                    analysisState: "completeFull",
                    pipeline: makePipeline(
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 100,
                        fastTranscriptWatermarkSec: 100,
                        featureCoverageEndSec: 100
                    )
                ),
                // staleFastTranscriptWatermark → "transcript_covered=… fast_watermark=… delta=…"
                makeActivityRow(
                    hash: "h-fw",
                    analysisState: "backfill",
                    pipeline: makePipeline(
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 1500,
                        fastTranscriptWatermarkSec: 90
                    )
                ),
                // unknownProgressWithoutPause → "disposition=… reason=…"
                makeActivityRow(
                    hash: "h-up",
                    cachedAudioPresent: false,
                    pipeline: makePipeline(
                        downloadPercent: "--%",
                        transcriptPercent: "--%",
                        analysisPercent: "--%"
                    )
                ),
                // staleJobLease → "lease_expires_at=… session_updated_at=…"
                makeActivityRow(
                    hash: "h-sjl",
                    cachedAudioPresent: true,
                    latestSession: makeSession(updatedAt: 200.0),
                    latestJob: makeJob(leasePresent: true, leaseExpiresAt: 100.0)
                ),
                // missingFailureReason → "state=…"
                makeActivityRow(
                    hash: "h-mfr",
                    analysisState: "failedTranscript",
                    terminalReason: nil
                )
            ]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        func detailFor(
            _ kind: DogfoodDiagnosticsAnalysisHealth.StalenessFlag.Kind,
            hash: String
        ) -> String {
            health.stalenessFlags.first {
                $0.kind == kind && $0.episodeIdHash == hash
            }?.detail ?? ""
        }
        let terminalDetail = detailFor(.terminalStateContradictsCoverage, hash: "h-tc")
        #expect(terminalDetail.hasPrefix("state="))
        #expect(terminalDetail.contains("duration="))
        let watermarkDetail = detailFor(.staleFastTranscriptWatermark, hash: "h-fw")
        #expect(watermarkDetail.hasPrefix("transcript_covered="))
        #expect(watermarkDetail.contains("fast_watermark="))
        #expect(watermarkDetail.contains("delta="))
        let unknownDetail = detailFor(.unknownProgressWithoutPause, hash: "h-up")
        #expect(unknownDetail.hasPrefix("disposition="))
        #expect(unknownDetail.contains("reason="))
        let leaseDetail = detailFor(.staleJobLease, hash: "h-sjl")
        #expect(leaseDetail.hasPrefix("lease_expires_at="))
        #expect(leaseDetail.contains("session_updated_at="))
        let missingDetail = detailFor(.missingFailureReason, hash: "h-mfr")
        #expect(missingDetail.hasPrefix("state="))
    }

    @Test("flags stale_job_lease and recommends clear_stale_lease when the lease outlived the latest session")
    func flagsAndRecommendsForStaleLease() {
        // Lease expires at t=100 but the latest session updatedAt is
        // t=200 — the runner already finished but the lease row is
        // pinned. Expect the staleJobLease flag AND the
        // clearStaleLease recommendation; both branches share the
        // same condition and must agree.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-stale-lease",
                cachedAudioPresent: true,
                latestSession: makeSession(updatedAt: 200.0),
                latestJob: makeJob(
                    leasePresent: true,
                    leaseExpiresAt: 100.0
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let leaseFlags = health.stalenessFlags.filter { $0.kind == .staleJobLease }
        #expect(leaseFlags.count == 1)
        #expect(leaseFlags.first?.episodeIdHash == "h-stale-lease")
        #expect(health.assets.first?.recommendedAction == .clearStaleLease)
    }

    @Test("does not flag stale_job_lease when no lease is present")
    func doesNotFlagStaleLeaseWithoutLease() {
        // Same updatedAt-vs-leaseExpires shape but with leasePresent=false:
        // there is no lease to be stale, so no flag should fire.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-no-lease",
                cachedAudioPresent: true,
                latestSession: makeSession(updatedAt: 200.0),
                latestJob: makeJob(
                    leasePresent: false,
                    leaseExpiresAt: 100.0
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        #expect(!health.stalenessFlags.contains { $0.kind == .staleJobLease })
        #expect(health.assets.first?.recommendedAction != .clearStaleLease)
    }

    @Test("does not flag unknown progress when row is paused or unavailable")
    func doesNotFlagUnknownProgressWhenPausedOrUnavailable() {
        // Both negative cases (paused and unavailable) need to be
        // pinned, otherwise a regression that loosens the gate to
        // "unknown_progress fires for paused too" would slip through.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [
                makeActivityRow(
                    hash: "h-paused",
                    disposition: "paused",
                    pipeline: makePipeline(
                        downloadPercent: "--%",
                        transcriptPercent: "--%",
                        analysisPercent: "--%"
                    )
                ),
                makeActivityRow(
                    hash: "h-unavailable",
                    disposition: "unavailable",
                    pipeline: makePipeline(
                        downloadPercent: "--%",
                        transcriptPercent: "--%",
                        analysisPercent: "--%"
                    )
                )
            ]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let unknownFlagHashes = health.stalenessFlags
            .filter { $0.kind == .unknownProgressWithoutPause }
            .map(\.episodeIdHash)
        #expect(unknownFlagHashes.isEmpty)
    }

    @Test("flags terminal-state contradiction for completeTranscriptPartial only when transcript is essentially zero")
    func completeTranscriptPartialOnlyFlagsOnZero() {
        // Spec: completeTranscriptPartial transcript is intentionally
        // short of threshold, so a 50% transcript coverage is fine
        // (no flag). Only a genuinely-zero transcript_coverage_sec
        // counts as a contradiction.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [
                // Healthy partial: 50% transcript coverage → no flag.
                makeActivityRow(
                    hash: "h-partial-healthy",
                    analysisState: "completeTranscriptPartial",
                    pipeline: makePipeline(
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 2000,
                        fastTranscriptWatermarkSec: 2000
                    )
                ),
                // Genuine contradiction: 0 transcript coverage on a
                // partial-complete row — nothing advanced.
                makeActivityRow(
                    hash: "h-partial-empty",
                    analysisState: "completeTranscriptPartial",
                    pipeline: makePipeline(
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 0,
                        fastTranscriptWatermarkSec: 0
                    )
                )
            ]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_001)
        )
        let terminalContradictionHashes = health.stalenessFlags
            .filter { $0.kind == .terminalStateContradictsCoverage }
            .map(\.episodeIdHash)
        #expect(terminalContradictionHashes == ["h-partial-empty"])
    }

    // MARK: - Closed-vocabulary string drift

    @Test("RecommendedAction wire strings match the bead spec verbatim")
    func recommendedActionStringsAreStable() {
        // Pin every wire string. If a future rename flips wait→waiting
        // or open_app→openApp, support tooling breaks silently —
        // catch it here.
        #expect(DogfoodDiagnosticsAnalysisHealth.RecommendedAction.wait.rawValue == "wait")
        #expect(DogfoodDiagnosticsAnalysisHealth.RecommendedAction.plugInOrWait.rawValue == "plug_in_or_wait")
        #expect(DogfoodDiagnosticsAnalysisHealth.RecommendedAction.openApp.rawValue == "open_app")
        #expect(DogfoodDiagnosticsAnalysisHealth.RecommendedAction.retry.rawValue == "retry")
        #expect(DogfoodDiagnosticsAnalysisHealth.RecommendedAction.clearStaleLease.rawValue == "clear_stale_lease")
        #expect(DogfoodDiagnosticsAnalysisHealth.RecommendedAction.fileBug.rawValue == "file_bug")
        #expect(DogfoodDiagnosticsAnalysisHealth.RecommendedAction.unknown.rawValue == "unknown")
        // Belt-and-braces: assert the case-iterable count so a new
        // case doesn't slip in without an updated wire-string pin.
        #expect(DogfoodDiagnosticsAnalysisHealth.RecommendedAction.allCases.count == 7)
    }

    @Test("StalenessFlag.Kind wire strings match the bead spec verbatim")
    func stalenessFlagKindStringsAreStable() {
        #expect(DogfoodDiagnosticsAnalysisHealth.StalenessFlag.Kind.terminalStateContradictsCoverage.rawValue == "terminal_state_contradicts_coverage")
        #expect(DogfoodDiagnosticsAnalysisHealth.StalenessFlag.Kind.staleFastTranscriptWatermark.rawValue == "stale_fast_transcript_watermark")
        #expect(DogfoodDiagnosticsAnalysisHealth.StalenessFlag.Kind.unknownProgressWithoutPause.rawValue == "unknown_progress_without_pause")
        #expect(DogfoodDiagnosticsAnalysisHealth.StalenessFlag.Kind.staleJobLease.rawValue == "stale_job_lease")
        #expect(DogfoodDiagnosticsAnalysisHealth.StalenessFlag.Kind.missingFailureReason.rawValue == "missing_failure_reason")
        #expect(DogfoodDiagnosticsAnalysisHealth.StalenessFlag.Kind.allCases.count == 5)
    }

    // MARK: - v1 wire-format byte identity

    @Test("v1 archive (no analysis_health) does not emit analysis_health key")
    func v1ArchiveOmitsAnalysisHealthKey() throws {
        // Acceptance criterion: when analysis_health is omitted, the
        // v1 wire format is byte-identical to the pre-bead shape. The
        // strongest pin is to inspect the raw JSON and confirm the
        // analysis_health key is not emitted (Optional Codable should
        // call encodeIfPresent and skip nil keys).
        let source = try makeTempDir(prefix: "AHV1WireSource")
        let output = try makeTempDir(prefix: "AHV1WireOutput")
        defer {
            try? FileManager.default.removeItem(at: source)
            try? FileManager.default.removeItem(at: output)
        }
        try writeMinimalSurfaceStatusLog(in: source)
        let result = try DogfoodDiagnosticsExporter.export(
            sourceDirectory: source,
            outputDirectory: output,
            now: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let raw = try String(contentsOf: result.fileURL, encoding: .utf8)
        #expect(!raw.contains("\"analysis_health\""))
        #expect(!raw.contains("\"activity_snapshot\""))
        // schema_version must be 1, not 2.
        #expect(raw.contains("\"schema_version\" : 1"))
    }

    // MARK: - Caller-supplied aggregates

    @Test("nil duplicate / learning blocks are omitted from the encoded JSON")
    func nilDuplicateAndLearningOmittedFromWire() throws {
        // Pin Optional Codable behavior: when the caller passes nil
        // for duplicates / learning, the encoded JSON must NOT contain
        // those keys (`encodeIfPresent` semantics). Catches a future
        // change that swaps to manual encode and accidentally emits
        // null literals — present-but-null and absent decode the same
        // way today but mean different things to support tooling.
        let snapshot = makeMinimalActivitySnapshot()
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        encoder.outputFormatting = [.sortedKeys, .prettyPrinted]
        let data = try encoder.encode(health)
        let json = try #require(String(data: data, encoding: .utf8))
        #expect(!json.contains("\"duplicates\""))
        #expect(!json.contains("\"learning\""))
    }

    @Test("duplicate counts and learning counts round-trip when supplied")
    func duplicatesAndLearningRoundTrip() throws {
        let snapshot = makeMinimalActivitySnapshot()
        let duplicates = DogfoodDiagnosticsAnalysisHealth.DuplicateCounts(
            duplicateCorrectionScopes: 2,
            duplicateFinalPassWindows: 3
        )
        let learning = DogfoodDiagnosticsAnalysisHealth.LearningCounts(
            rawCorrections: 8,
            dedupedCorrections: 4,
            shadowFMResponses: 1321,
            ingestedLearningArtifacts: 0,
            skippedIngestionReasons: ["duplicate_scope": 4, "transcript_unavailable": 2]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            duplicates: duplicates,
            learning: learning,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        encoder.outputFormatting = [.sortedKeys, .prettyPrinted]
        let data = try encoder.encode(health)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(DogfoodDiagnosticsAnalysisHealth.self, from: data)
        #expect(decoded.duplicates == duplicates)
        #expect(decoded.learning == learning)
    }

    @Test("populated analysis_health round-trips through encode/decode with full equality")
    func populatedHealthFullRoundTrip() throws {
        // R11: existing serialization tests check sub-fields
        // (`globalSummaryCounts`'s `decodedHealth.global.totalAssets == 1`
        // in `schemaVersionBumpsOnlyWhenHealthAttached`, the duplicates/
        // learning struct in `duplicatesAndLearningRoundTrip`) but no
        // test pins a populated multi-asset summary through encode →
        // decode → full `==` equality. A typo in any sub-struct's
        // `CodingKeys` raw value, or a future swap to manual encode that
        // accidentally drops a field, would slip past every existing
        // assertion.
        //
        // This test populates every observable field — multi-asset
        // global counts, an asset summary with a non-nil watermarkDelta,
        // a staleness flag, both caller-supplied aggregate blocks, and
        // a captureNote — and asserts the decoded value `==` the
        // encoded value via Equatable conformance. Because every
        // sub-struct in the analysis_health graph is `Equatable`, a
        // single `==` assertion exercises every field; if a CodingKey
        // drifts the field comes back nil/default and the equality
        // fails loudly.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [
                // Asset 1: terminal-completion contradiction → flag +
                // recommendation `.fileBug`. Drives a non-empty
                // staleness_flags array AND an asset summary with
                // populated watermarkDeltaSec / progressProvenance.
                makeActivityRow(
                    hash: "h-rt-contradicting",
                    section: "up_next",
                    analysisState: "completeFull",
                    pipeline: makePipeline(
                        downloadPercent: "100%",
                        transcriptPercent: "5%",
                        transcriptSource: "fast_transcript_chunks",
                        analysisPercent: "5%",
                        analysisSource: "feature_coverage",
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 200,
                        transcriptWatermarkSec: 200,
                        fastTranscriptWatermarkSec: 90,
                        analysisWatermarkSec: 200,
                        featureCoverageEndSec: 200,
                        confirmedAdCoverageEndSec: 50,
                        finalPassCoverageEndSec: 200
                    ),
                    latestSession: makeSession(
                        failureReason: nil,
                        state: "backfill",
                        updatedAt: 1_777_000_000
                    ),
                    latestJob: makeJob(
                        state: "running",
                        updatedAt: 1_777_001_000,
                        nextEligibleAt: 1_777_100_000,
                        leasePresent: false,
                        lastErrorCode: "no_audio_yet"
                    ),
                    latestTerminalWork: makeWorkJournal(
                        eventType: "preempted",
                        timestamp: 1_777_002_000
                    )
                ),
                // Asset 2: healthy completion. Drives terminalCompletedCount.
                makeActivityRow(
                    hash: "h-rt-healthy",
                    analysisState: "completeFull",
                    pipeline: makePipeline(
                        downloadPercent: "100%",
                        transcriptPercent: "98%",
                        analysisPercent: "100%",
                        episodeDurationSec: 4000,
                        transcriptCoveredSec: 3960,
                        fastTranscriptWatermarkSec: 3960,
                        featureCoverageEndSec: 4000
                    )
                )
            ],
            captureError: "io_partial_read"
        )
        let duplicates = DogfoodDiagnosticsAnalysisHealth.DuplicateCounts(
            duplicateCorrectionScopes: 7,
            duplicateFinalPassWindows: 11
        )
        let learning = DogfoodDiagnosticsAnalysisHealth.LearningCounts(
            rawCorrections: 19,
            dedupedCorrections: 13,
            shadowFMResponses: 503,
            ingestedLearningArtifacts: 5,
            skippedIngestionReasons: [
                "duplicate_scope": 6,
                "unverified_window": 2,
                "transcript_unavailable": 1
            ]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            duplicates: duplicates,
            learning: learning,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        // Sanity: the populated summary is genuinely non-trivial — if
        // any of these break it means the fixture above stopped
        // exercising the relevant branches and the equality assertion
        // below would be vacuous.
        #expect(health.assets.count == 2)
        #expect(!health.stalenessFlags.isEmpty)
        #expect(health.global.terminalCompletedCount == 2)
        #expect(health.captureNote != nil)

        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(health)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(DogfoodDiagnosticsAnalysisHealth.self, from: data)
        // Single Equatable assertion — every nested struct conforms to
        // Equatable, so a CodingKey drift on any sub-field surfaces here.
        #expect(decoded == health)
    }

    @Test("encoded analysis_health uses snake_case wire keys for every field")
    func encodedAnalysisHealthSnakeCaseWireKeys() throws {
        // R11: every CodingKey raw value is a snake_case wire string
        // — support tooling, Python parsers, and downstream archive
        // ingest depend on the snake_case form. Existing tests pin
        // RecommendedAction/StalenessFlag.Kind enum raw values and the
        // outer `analysis_health` key (via the omit-when-nil test), but
        // no test pins the inner field-name keys. A future refactor that
        // accidentally drops a `case … = "snake_case"` line (or
        // a developer renaming a Swift property without the matching
        // CodingKeys update) would silently re-emit camelCase keys and
        // break every downstream parser. Encode a populated archive,
        // assert presence of every load-bearing snake_case key, AND
        // assert the camelCase variants are absent.
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h-keys",
                analysisState: "completeFull",
                terminalReason: "thermal_budget_exceeded",
                pipeline: makePipeline(
                    downloadPercent: "100%",
                    transcriptPercent: "5%",
                    transcriptSource: "fast_transcript_chunks",
                    analysisPercent: "5%",
                    analysisSource: "feature_coverage",
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: 200,
                    transcriptWatermarkSec: 200,
                    fastTranscriptWatermarkSec: 90,
                    analysisWatermarkSec: 200,
                    featureCoverageEndSec: 200,
                    confirmedAdCoverageEndSec: 50,
                    finalPassCoverageEndSec: 200
                ),
                latestSession: makeSession(updatedAt: 1_777_000_000),
                latestJob: makeJob(leasePresent: true, leaseExpiresAt: 1_700_001_000),
                latestTerminalWork: makeWorkJournal(eventType: "finalized", timestamp: 1_777_002_000)
            )]
        )
        let duplicates = DogfoodDiagnosticsAnalysisHealth.DuplicateCounts(
            duplicateCorrectionScopes: 1,
            duplicateFinalPassWindows: 1
        )
        let learning = DogfoodDiagnosticsAnalysisHealth.LearningCounts(
            rawCorrections: 1,
            dedupedCorrections: 1,
            shadowFMResponses: 1,
            ingestedLearningArtifacts: 1,
            skippedIngestionReasons: [:]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            duplicates: duplicates,
            learning: learning,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        encoder.outputFormatting = [.sortedKeys, .prettyPrinted]
        let data = try encoder.encode(health)
        let json = try #require(String(data: data, encoding: .utf8))

        // Top-level keys.
        let topLevelKeys = [
            "summary_schema_version",
            "generated_at",
            "global",
            "assets",
            "staleness_flags",
            "duplicates",
            "learning"
            // captureNote omitted — present-but-nil; tested elsewhere.
        ]
        for key in topLevelKeys {
            #expect(json.contains("\"\(key)\""), "missing top-level key \(key)")
        }

        // GlobalSummary keys. R12: the latest_*_at / latest_*_outcome
        // keys ARE populated in this fixture (the row carries
        // latestSession/latestJob/latestTerminalWork plus pipeline
        // watermarks), so they're pinned directly here rather than
        // deferred to the populated round-trip's Equatable check —
        // the round-trip catches field-level CodingKey drift but
        // does not pin specific snake_case wire strings.
        let globalKeys = [
            "total_assets",
            "running_count",
            "queued_count",
            "paused_count",
            "failed_count",
            "unavailable_count",
            "terminal_completed_count",
            "stale_terminal_count",
            "stale_watermark_count",
            "unknown_progress_count",
            "latest_job_update_at",
            "latest_terminal_work_at",
            "latest_artifact_watermark_sec",
            "latest_terminal_work_outcome"
        ]
        for key in globalKeys {
            #expect(json.contains("\"\(key)\""), "missing global key \(key)")
        }

        // AssetSummary keys. R12: `section` was missing from the
        // earlier list — it's always populated (default "up_next" in
        // the fixture) and shipped on every row, so add it. The
        // remaining nil-when-default fields (queue_position,
        // latest_session_failure_reason, latest_job_last_error_code,
        // latest_job_next_eligible_at, latest_terminal_work_cause,
        // finished_outcome) are intentionally nil in this fixture and
        // therefore omitted by encodeIfPresent — those snake_case
        // wire keys are pinned by the populated round-trip's
        // Equatable check.
        let assetKeys = [
            "episode_id_hash",
            "section",
            "analysis_state",
            "is_running",
            "cached_audio_present",
            "download_percent",
            "transcript_percent",
            "analysis_percent",
            "transcript_covered_sec",
            "transcript_watermark_sec",
            "fast_transcript_watermark_sec",
            "watermark_delta_sec",
            "analysis_watermark_sec",
            "final_pass_coverage_end_sec",
            "progress_provenance",
            "latest_session_state",
            "latest_job_state",
            "latest_job_lease_present",
            "latest_terminal_work_event",
            "terminal_reason",
            "recommended_action",
            "recommended_action_note"
        ]
        for key in assetKeys {
            #expect(json.contains("\"\(key)\""), "missing asset key \(key)")
        }

        // ProgressProvenance keys.
        let provenanceKeys = ["download_source", "transcript_source", "analysis_source"]
        for key in provenanceKeys {
            #expect(json.contains("\"\(key)\""), "missing provenance key \(key)")
        }

        // StalenessFlag keys. R12: the fixture's completeFull row
        // with low coverage triggers a terminalStateContradictsCoverage
        // flag, so `staleness_flags` is non-empty and its sub-keys are
        // emitted. `episode_id_hash` is shared with AssetSummary (so
        // its presence alone doesn't prove the flag's CodingKey is
        // intact), but `kind` and `detail` are unique to the flag
        // block. Pin all three so a future drop of
        // `case episodeIdHash = "episode_id_hash"` from
        // StalenessFlag.CodingKeys is caught here in addition to the
        // existing camelCase prohibition (`episodeIdHash`) below.
        let stalenessFlagKeys = ["episode_id_hash", "kind", "detail"]
        for key in stalenessFlagKeys {
            #expect(json.contains("\"\(key)\""), "missing staleness_flag key \(key)")
        }

        // DuplicateCounts keys.
        let duplicateKeys = ["duplicate_correction_scopes", "duplicate_final_pass_windows"]
        for key in duplicateKeys {
            #expect(json.contains("\"\(key)\""), "missing duplicates key \(key)")
        }

        // LearningCounts keys.
        let learningKeys = [
            "raw_corrections",
            "deduped_corrections",
            "shadow_fm_responses",
            "ingested_learning_artifacts",
            "skipped_ingestion_reasons"
        ]
        for key in learningKeys {
            #expect(json.contains("\"\(key)\""), "missing learning key \(key)")
        }

        // Camel-case variants of the most-renamed fields must NOT
        // appear as JSON keys (they could legitimately appear as
        // VALUES — e.g. "completeFull" — so guard with the trailing
        // quote+colon to match key positions only).
        let camelCaseProhibited = [
            "summarySchemaVersion",
            "generatedAt",
            "totalAssets",
            "runningCount",
            "episodeIdHash",
            "analysisState",
            "isRunning",
            "cachedAudioPresent",
            "downloadPercent",
            "transcriptPercent",
            "analysisPercent",
            "transcriptCoveredSec",
            "fastTranscriptWatermarkSec",
            "watermarkDeltaSec",
            "progressProvenance",
            "recommendedAction",
            "recommendedActionNote",
            "stalenessFlags",
            "duplicateCorrectionScopes",
            "rawCorrections",
            "skippedIngestionReasons"
        ]
        for camelKey in camelCaseProhibited {
            #expect(
                !json.contains("\"\(camelKey)\" :"),
                "found camelCase key \"\(camelKey)\" — CodingKey drifted"
            )
        }
    }

    @Test("no_snapshot factory produces an empty-but-noted summary")
    func noSnapshotFactoryProducesNotedSummary() {
        let health = DogfoodDiagnosticsAnalysisHealth.noSnapshot(
            reason: "analysis_store_unopened",
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        #expect(health.assets.isEmpty)
        #expect(health.global.totalAssets == 0)
        #expect(health.captureNote?.contains("analysis_store_unopened") == true)
    }

    // MARK: - Privacy / redaction

    @Test("redacts user-home and remote URL paths in detail strings")
    func redactsPathsAndUrls() {
        let raw = "decode failed at file:///Users/dan/Library/audio.m4a "
            + "from https://feeds.example.com/show/episode.mp3 "
            + "/private/var/mobile/Containers/Data/foo "
            + "/var/mobile/Audio/x.m4a"
        let scrubbed = DogfoodDiagnosticsAnalysisHealth.redactedTruncated(raw)
        #expect(!scrubbed.contains("/Users/"))
        #expect(!scrubbed.contains("https://"))
        #expect(!scrubbed.contains("/var/mobile/"))
        #expect(!scrubbed.contains("/private/var/"))
        #expect(!scrubbed.contains("file://"))
        #expect(scrubbed.contains("[redacted]"))
    }

    @Test("truncates oversized detail strings")
    func truncatesOversizedDetail() {
        let raw = String(repeating: "x", count: 500)
        let scrubbed = DogfoodDiagnosticsAnalysisHealth.redactedTruncated(raw, limit: 50)
        #expect(scrubbed.count <= 51) // 50 chars + ellipsis (one Character)
        #expect(scrubbed.hasSuffix("…"))
    }

    @Test("redaction still fires on a path embedded in an oversized input (pre-truncate ordering)")
    func redactsBeforeTruncationBoundsRegexWork() {
        // R3 contract: the regex strip runs against a length-bounded
        // copy of the input — a hostile/buggy caller passing a
        // multi-megabyte string with a /Users/ path at the front
        // should still get the path redacted AND get bounded work.
        // Pin both halves: the path leaves the redactor as
        // "[redacted]" (not as raw "/Users/...") AND the output is
        // capped at the limit.
        let path = "/Users/dan/Library/audio.m4a "
        let trailing = String(repeating: "x", count: 5000)
        let raw = path + trailing
        let scrubbed = DogfoodDiagnosticsAnalysisHealth.redactedTruncated(raw, limit: 100)
        #expect(!scrubbed.contains("/Users/"))
        #expect(scrubbed.contains("[redacted]"))
        // 100-char cap + single trailing ellipsis Character.
        #expect(scrubbed.count <= 101)
        #expect(scrubbed.hasSuffix("…"))
    }

    @Test("encoded analysis_health does not leak raw transcript-text or audio paths")
    func encodedHealthHasNoLeaks() throws {
        // The encoded JSON should never contain raw transcript text
        // (the activity snapshot never carries it in the first
        // place — this is the defense-in-depth assertion that a
        // future change accidentally widening the snapshot's surface
        // doesn't slip past the export).
        let snapshot = DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(
                hash: "h",
                analysisState: "failed",
                terminalReason: nil,
                latestSession: makeSession(
                    failureReason: nil,
                    state: "failed"
                ),
                latestJob: makeJob(
                    lastErrorCode: "decode_failed_no_path"
                )
            )]
        )
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(health)
        let json = String(data: data, encoding: .utf8) ?? ""
        // Pin the redaction surface — none of these substrings
        // should ever appear in a serialized analysis_health block
        // even if the upstream snapshot mistakenly carried them.
        #expect(!json.contains("/Users/"))
        #expect(!json.contains("/var/mobile/"))
        #expect(!json.contains("/private/var/"))
        #expect(!json.contains("file://"))
        #expect(!json.contains("https://"))
        #expect(!json.contains("http://"))
    }

    // MARK: - Settings/export choice (Mail vs Files)

    @Test("dogfood export still produces a Files-compatible JSON URL (no email forced)")
    func dogfoodExportRemainsFileBased() throws {
        // The export contract: Settings hands off a `fileURL` the
        // user can route through ShareLink (Mail OR Files). This
        // bead must not narrow that surface — a successful export
        // returns a file URL ending in .json that we can read back.
        let source = try makeTempDir(prefix: "AHFilesSource")
        let output = try makeTempDir(prefix: "AHFilesOutput")
        defer {
            try? FileManager.default.removeItem(at: source)
            try? FileManager.default.removeItem(at: output)
        }
        try writeMinimalSurfaceStatusLog(in: source)

        let snapshot = makeMinimalActivitySnapshot()
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let result = try DogfoodDiagnosticsExporter.export(
            sourceDirectory: source,
            outputDirectory: output,
            now: Date(timeIntervalSince1970: 1_700_000_000),
            activitySnapshot: snapshot,
            analysisHealth: health
        )
        #expect(result.fileURL.pathExtension == "json")
        #expect(result.fileURL.isFileURL)
        // ShareLink-compatible: the URL points at a real file on
        // disk that any Files-routed activity can read.
        #expect(FileManager.default.fileExists(atPath: result.fileURL.path))
    }

    // MARK: - No-surface-status behavior preserved

    @Test("export still fails clearly when no surface-status logs exist (with health)")
    func noSurfaceStatusFailureBehaviorPreserved() throws {
        // Adding analysis_health support must not loosen the "we
        // need at least one surface-status log" gate — that gate is
        // pinned by an existing test, and we re-pin it here when
        // analysis_health is also requested so a future change
        // can't silently route around it.
        let source = try makeTempDir(prefix: "AHEmpty")
        let output = try makeTempDir(prefix: "AHEmptyOut")
        defer {
            try? FileManager.default.removeItem(at: source)
            try? FileManager.default.removeItem(at: output)
        }
        let snapshot = makeMinimalActivitySnapshot()
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        do {
            _ = try DogfoodDiagnosticsExporter.export(
                sourceDirectory: source,
                outputDirectory: output,
                activitySnapshot: snapshot,
                analysisHealth: health
            )
            Issue.record("Expected export to refuse with no surface-status logs even when analysis_health attached")
        } catch let error as DogfoodDiagnosticsExportError {
            #expect(error == .noSurfaceStatusLogs)
        } catch {
            Issue.record("Wrong error: \(error)")
        }
    }

    // MARK: - May 6 fixture-shape regression (hand-rolled)

    @Test("May 6 fixture shape: 22 queued/up_next rows with mixed terminal states")
    func may6FixtureShapeRegression() {
        // Mirrors the structural facts the playhead-hygc.1.1
        // sanitized fixture pins (see its README): all rows in
        // up_next + queued + cached audio + analysis_state mix.
        // The fixture itself ships from a sibling worktree; this
        // hand-rolled subset covers the load-bearing assertions
        // until that fixture lands on main, at which point a
        // follow-up bead can swap to the file.
        let snapshot = makeMay6FixtureShape()
        let health = DogfoodDiagnosticsAnalysisHealth.build(
            from: snapshot,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        // Top-line dogfood question: nothing is running.
        #expect(health.global.runningCount == 0)
        // Coverage / ad / transcript answers.
        let coverageAnswers = health.assets.map { asset in
            (asset.transcriptCoveredSec, asset.analysisWatermarkSec)
        }
        #expect(coverageAnswers.contains { $0.0 != nil })
        #expect(coverageAnswers.contains { $0.1 != nil })
        // Terminal-state truth: at least one completeFull row in
        // the May 6 shape carries a watermark contradiction.
        #expect(health.global.staleWatermarkCount >= 1)
        // R5: the `asset_004`-equivalent row (completeFull with
        // chunks at 3960 s but watermark stuck at 90 s) is terminally
        // complete; recommend `.wait`, not `.plugInOrWait`. Pin the
        // recommendation alongside the watermark flag — the flag
        // survives for support visibility while the user-facing
        // hint stays correct.
        let asset004 = health.assets.first { $0.episodeIdHash == "may6-completeFull-0" }
        let pinned = try! #require(asset004)
        #expect(pinned.recommendedAction == .wait)
        #expect(pinned.recommendedActionNote == "healthy_terminal_completion")
    }
}

// MARK: - Test-only fixtures

@MainActor
private extension DogfoodDiagnosticsAnalysisHealthTests {

    /// Mirrors the v1 `DogfoodDiagnosticsArchive` field set so we can
    /// prove an older reader (one that doesn't know about
    /// `analysis_health`) decodes a v2 archive without crashing.
    struct V1SurfaceMirror: Decodable {
        let schemaVersion: Int
        let generatedAt: Date
        let files: [DogfoodDiagnosticsArchiveFile]
        let activitySnapshot: DogfoodDiagnosticsActivitySnapshot?

        enum CodingKeys: String, CodingKey {
            case schemaVersion = "schema_version"
            case generatedAt = "generated_at"
            case files
            case activitySnapshot = "activity_snapshot"
        }
    }

    func decodeArchive(at url: URL) throws -> DogfoodDiagnosticsArchive {
        let data = try Data(contentsOf: url)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(DogfoodDiagnosticsArchive.self, from: data)
    }

    func writeMinimalSurfaceStatusLog(in directory: URL) throws {
        let logName = "surface-status-20260506T000000Z-AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE.jsonl"
        try """
        {"timestamp":"2026-05-06T00:00:00Z","event_type":"ready_entered"}

        """.write(
            to: directory.appendingPathComponent(logName),
            atomically: true,
            encoding: .utf8
        )
    }

    func makeMinimalActivitySnapshot() -> DogfoodDiagnosticsActivitySnapshot {
        DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: [makeActivityRow(hash: "h-min")]
        )
    }

    func makePipeline(
        downloadFraction: Double? = 1.0,
        downloadPercent: String = "100%",
        downloadSource: String = "cached_audio",
        transcriptFraction: Double? = nil,
        transcriptPercent: String = "--%",
        transcriptSource: String = "unknown",
        analysisFraction: Double? = nil,
        analysisPercent: String = "--%",
        analysisSource: String = "unknown",
        episodeDurationSec: Double? = 4000,
        transcriptCoveredSec: Double? = nil,
        transcriptWatermarkSec: Double? = nil,
        fastTranscriptWatermarkSec: Double? = nil,
        analysisWatermarkSec: Double? = nil,
        featureCoverageEndSec: Double? = nil,
        confirmedAdCoverageEndSec: Double? = nil,
        finalPassCoverageEndSec: Double? = nil
    ) -> DogfoodDiagnosticsPipelineSnapshot {
        DogfoodDiagnosticsPipelineSnapshot(
            downloadFraction: downloadFraction,
            downloadPercent: downloadPercent,
            downloadSource: downloadSource,
            transcriptFraction: transcriptFraction,
            transcriptPercent: transcriptPercent,
            transcriptSource: transcriptSource,
            analysisFraction: analysisFraction,
            analysisPercent: analysisPercent,
            analysisSource: analysisSource,
            episodeDurationSec: episodeDurationSec,
            transcriptCoveredSec: transcriptCoveredSec,
            transcriptWatermarkSec: transcriptWatermarkSec,
            fastTranscriptWatermarkSec: fastTranscriptWatermarkSec,
            analysisWatermarkSec: analysisWatermarkSec,
            featureCoverageEndSec: featureCoverageEndSec,
            confirmedAdCoverageEndSec: confirmedAdCoverageEndSec,
            finalPassCoverageEndSec: finalPassCoverageEndSec
        )
    }

    func makeActivityRow(
        hash: String,
        section: String = "up_next",
        disposition: String = "queued",
        reason: String = "waiting_for_time",
        analysisState: String = "queued",
        isRunning: Bool = false,
        cachedAudioPresent: Bool = true,
        liveDownloadFraction: Double? = nil,
        terminalReason: String? = nil,
        pipeline: DogfoodDiagnosticsPipelineSnapshot? = nil,
        latestSession: DogfoodDiagnosticsAnalysisSessionSnapshot? = nil,
        latestJob: DogfoodDiagnosticsAnalysisJobSnapshot? = nil,
        latestTerminalWork: DogfoodDiagnosticsWorkJournalSnapshot? = nil
    ) -> DogfoodDiagnosticsActivityRow {
        DogfoodDiagnosticsActivityRow(
            episodeIdHash: hash,
            section: section,
            status: DogfoodDiagnosticsStatusSnapshot(
                disposition: disposition,
                reason: reason,
                hint: "wait",
                analysisUnavailableReason: nil,
                playbackReadiness: "none",
                readinessAnchor: nil
            ),
            isRunning: isRunning,
            finishedOutcome: nil,
            queuePosition: nil,
            cachedAudioPresent: cachedAudioPresent,
            liveDownloadFraction: liveDownloadFraction,
            pipeline: pipeline ?? makePipeline(),
            analysisAsset: DogfoodDiagnosticsAnalysisAssetSnapshot(
                analysisState: analysisState,
                analysisVersion: 1,
                artifactClass: "media",
                terminalReason: terminalReason,
                capabilitySnapshotPresent: false
            ),
            latestSession: latestSession,
            latestJob: latestJob,
            latestTerminalWorkJournal: latestTerminalWork
        )
    }

    func makeSession(
        failureReason: String? = nil,
        state: String = "backfill",
        updatedAt: Double = 1_777_000_000.0
    ) -> DogfoodDiagnosticsAnalysisSessionSnapshot {
        DogfoodDiagnosticsAnalysisSessionSnapshot(
            state: state,
            startedAt: updatedAt - 100,
            updatedAt: updatedAt,
            failureReason: failureReason,
            needsShadowRetry: false
        )
    }

    func makeJob(
        state: String = "queued",
        updatedAt: Double = 1_777_000_000.0,
        nextEligibleAt: Double? = nil,
        leasePresent: Bool = false,
        leaseExpiresAt: Double? = nil,
        lastErrorCode: String? = nil
    ) -> DogfoodDiagnosticsAnalysisJobSnapshot {
        DogfoodDiagnosticsAnalysisJobSnapshot(
            jobType: "primary",
            state: state,
            priority: 0,
            desiredCoverageSec: 0,
            featureCoverageSec: 0,
            transcriptCoverageSec: 0,
            cueCoverageSec: 0,
            attemptCount: 0,
            nextEligibleAt: nextEligibleAt,
            leasePresent: leasePresent,
            leaseExpiresAt: leaseExpiresAt,
            lastErrorCode: lastErrorCode,
            createdAt: updatedAt - 100,
            updatedAt: updatedAt,
            generationID: "gen-1",
            schedulerEpoch: 0,
            artifactClass: "media",
            estimatedWriteBytes: 0
        )
    }

    func makeWorkJournal(
        eventType: String,
        timestamp: Double
    ) -> DogfoodDiagnosticsWorkJournalSnapshot {
        DogfoodDiagnosticsWorkJournalSnapshot(
            eventType: eventType,
            cause: nil,
            timestamp: timestamp,
            generationID: "gen-1",
            schedulerEpoch: 0,
            artifactClass: "media"
        )
    }

    /// Hand-rolled subset of the playhead-hygc.1.1 sanitized fixture
    /// shape: 22 up_next rows, all queued, mostly cached, mix of
    /// terminal completion states with the load-bearing watermark
    /// contradiction on `asset_004`-equivalent.
    func makeMay6FixtureShape() -> DogfoodDiagnosticsActivitySnapshot {
        var rows: [DogfoodDiagnosticsActivityRow] = []
        // 9 backfill rows with healthy coverage.
        for index in 0..<9 {
            rows.append(makeActivityRow(
                hash: "may6-backfill-\(index)",
                analysisState: "backfill",
                pipeline: makePipeline(
                    downloadPercent: "100%",
                    transcriptPercent: "95%",
                    transcriptSource: "fast_transcript_chunks",
                    analysisPercent: "100%",
                    analysisSource: "feature_coverage",
                    episodeDurationSec: 3000,
                    transcriptCoveredSec: 2900,
                    fastTranscriptWatermarkSec: 2900,
                    analysisWatermarkSec: 3000,
                    featureCoverageEndSec: 3000
                )
            ))
        }
        // 8 completeFull rows. One mirrors the asset_004 contradiction:
        // chunks at 3960 s but watermark stuck at 90 s.
        for index in 0..<8 {
            let isStaleWatermark = index == 0
            rows.append(makeActivityRow(
                hash: "may6-completeFull-\(index)",
                analysisState: "completeFull",
                pipeline: makePipeline(
                    downloadPercent: "100%",
                    transcriptPercent: "95%",
                    transcriptSource: "fast_transcript_chunks",
                    analysisPercent: "100%",
                    analysisSource: "feature_coverage",
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: isStaleWatermark ? 3960 : 3900,
                    fastTranscriptWatermarkSec: isStaleWatermark ? 90 : 3900,
                    analysisWatermarkSec: 4000,
                    featureCoverageEndSec: 4000
                )
            ))
        }
        // 3 queued rows (still spooling).
        for index in 0..<3 {
            rows.append(makeActivityRow(
                hash: "may6-queued-\(index)",
                analysisState: "queued",
                pipeline: makePipeline(
                    downloadPercent: "100%",
                    transcriptPercent: "--%",
                    transcriptSource: "unknown",
                    analysisPercent: "--%",
                    analysisSource: "unknown",
                    episodeDurationSec: 3000
                )
            ))
        }
        // 2 completeTranscriptPartial rows.
        for index in 0..<2 {
            rows.append(makeActivityRow(
                hash: "may6-partial-\(index)",
                analysisState: "completeTranscriptPartial",
                pipeline: makePipeline(
                    downloadPercent: "100%",
                    transcriptPercent: "60%",
                    transcriptSource: "fast_transcript_chunks",
                    analysisPercent: "100%",
                    analysisSource: "feature_coverage",
                    episodeDurationSec: 4000,
                    transcriptCoveredSec: 2400,
                    fastTranscriptWatermarkSec: 2400,
                    analysisWatermarkSec: 4000,
                    featureCoverageEndSec: 4000
                )
            ))
        }
        return DogfoodDiagnosticsActivitySnapshot(
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            rows: rows
        )
    }
}
