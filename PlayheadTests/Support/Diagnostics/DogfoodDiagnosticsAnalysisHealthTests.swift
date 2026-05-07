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
        // 50% transcript coverage on a `completeFull` row → contradiction.
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
        let kinds: [DogfoodDiagnosticsAnalysisHealth.StalenessFlag.Kind] = health.stalenessFlags.map(\.kind)
        #expect(kinds.contains(.terminalStateContradictsCoverage))
        #expect(health.global.staleTerminalCount == 1)
        // The recommendation should agree with the flag.
        #expect(health.assets.first?.recommendedAction == .fileBug)
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

    // MARK: - Caller-supplied aggregates

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
