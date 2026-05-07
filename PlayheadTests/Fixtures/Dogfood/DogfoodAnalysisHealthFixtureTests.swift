// DogfoodAnalysisHealthFixtureTests.swift
// playhead-hygc.1.1: structural assertions on the sanitized May 6, 2026
// dogfood fixture. These tests are the contract that downstream beads
// (hygc.1.2..1.9) will assert against — break one of these intentionally only
// when regenerating the fixture from a new dogfood capture.
//
// The fixture itself is at `2026-05-06/analysis-health.json`; the loader is
// `DogfoodAnalysisHealthFixtureLoader.swift`. Both live in the same dir as
// this test file so #filePath resolution is trivial.

import Foundation
import Testing

@Suite("Dogfood 2026-05-06 fixture — structural facts (playhead-hygc.1.1)")
struct DogfoodAnalysisHealthFixtureTests {

    // MARK: - Activity snapshot

    @Test("22 Activity rows, all queued/up_next/waiting_for_time, none running")
    func activityRowsAreQueued() throws {
        let fixture = try DogfoodAnalysisHealthFixtureLoader.load()
        let rows = fixture.activitySnapshot.rows

        #expect(rows.count == 22, "Expected 22 Activity rows; got \(rows.count)")
        #expect(fixture.activitySnapshot.rowCount == rows.count,
                "row_count header must match rows.count")

        for row in rows {
            #expect(row.section == "up_next",
                    "row \(row.id) section is \(row.section), expected up_next")
            #expect(row.status.disposition == "queued",
                    "row \(row.id) disposition is \(row.status.disposition ?? "nil"), expected queued")
            #expect(row.status.reason == "waiting_for_time",
                    "row \(row.id) reason is \(row.status.reason ?? "nil"), expected waiting_for_time")
            #expect(row.isRunning == false,
                    "row \(row.id) is_running was true; the dogfood capture has zero running rows")
        }
    }

    @Test("Cached audio present for every row, and download progress is 100%")
    func cachedAudioAndDownloadComplete() throws {
        let fixture = try DogfoodAnalysisHealthFixtureLoader.load()
        for row in fixture.activitySnapshot.rows {
            #expect(row.cachedAudioPresent == true,
                    "row \(row.id) cached_audio_present is false")
            #expect(row.pipeline.downloadFraction == 1.0,
                    "row \(row.id) download_fraction = \(row.pipeline.downloadFraction ?? -1), expected 1.0")
            #expect(row.pipeline.downloadPercent == "100%",
                    "row \(row.id) download_percent = \(row.pipeline.downloadPercent ?? "nil"), expected 100%")
        }
    }

    // MARK: - Chunk-vs-watermark contradiction (the asset_004 / 9C109975 case)

    @Test("At least one asset has fast-transcript chunk coverage far beyond its watermark")
    func chunkCoverageExceedsWatermark() throws {
        // The dogfood asset 9C109975-A4B9-4C87-AE62-7BAFF35CAE24 (anonymized
        // here as the synthetic `asset_004`) shows chunk transcript coverage
        // ≈3960 s (~66 min) while the asset's stored
        // `fastTranscriptCoverageEndTime` watermark is only 90 s (~1.5 min) —
        // a coverage / watermark inconsistency that downstream beads need to
        // exercise.
        let fixture = try DogfoodAnalysisHealthFixtureLoader.load()
        let assetById: [String: DogfoodAnalysisHealthFixture.AnalysisAsset] = Dictionary(
            uniqueKeysWithValues: fixture.analysisAssets.map { ($0.id, $0) }
        )

        var maxDelta: Double = 0
        var worstAssetId: String?
        var worstChunkMax: Double = 0
        var worstWatermark: Double = 0

        for chunkMax in fixture.transcriptChunkMaxima where chunkMax.pass == "fast" {
            guard let asset = assetById[chunkMax.assetId],
                  let watermark = asset.fastTranscriptCoverageEndSec else { continue }
            let delta = chunkMax.maxEndTimeSec - watermark
            if delta > maxDelta {
                maxDelta = delta
                worstAssetId = chunkMax.assetId
                worstChunkMax = chunkMax.maxEndTimeSec
                worstWatermark = watermark
            }
        }

        // Sanity: at least one asset must be more than 30 minutes "ahead"
        // of its stored watermark — significantly more than the named case
        // (~64 min). The exact pinning lives in the next test.
        #expect(
            maxDelta >= 30 * 60,
            "expected at least one asset with chunk coverage >= 30 min ahead of watermark; max delta was \(maxDelta) s on \(worstAssetId ?? "<none>")"
        )

        // Pin the named asset so the dataset's documented signal stays
        // observable — if we ever resequence asset IDs we'll need to update
        // both the README and this assertion together.
        let aroundOneAndAHalfMinutes: ClosedRange<Double> = 60...120
        let aroundSixtySixMinutes: ClosedRange<Double> = 3900...4020
        #expect(
            worstAssetId == "asset_004",
            "expected asset_004 to be the worst chunk-vs-watermark offender; got \(worstAssetId ?? "<none>")"
        )
        #expect(aroundOneAndAHalfMinutes.contains(worstWatermark),
                "expected ~1.5 min watermark on \(worstAssetId ?? "?"); got \(worstWatermark) s")
        #expect(aroundSixtySixMinutes.contains(worstChunkMax),
                "expected ~66 min chunk coverage on \(worstAssetId ?? "?"); got \(worstChunkMax) s")
    }

    // MARK: - completeFull contradictions

    @Test("At least one completeFull asset has contradictory stored coverage / terminal state")
    func completeFullContradictionPresent() throws {
        let fixture = try DogfoodAnalysisHealthFixtureLoader.load()
        let completeFulls = fixture.analysisAssets.filter { $0.analysisState == "completeFull" }
        #expect(!completeFulls.isEmpty, "fixture must contain completeFull assets")

        // A completeFull row is contradictory if EITHER:
        //   (a) its terminal_reason claims a transcript or feature ratio > 1.0
        //       (i.e. coverage allegedly exceeds duration — only possible due
        //        to a bookkeeping bug), OR
        //   (b) its fast_transcript_coverage_end_sec is < 50% of duration —
        //       i.e. the asset is "complete" but only ~1.5 min of fast
        //       transcript was ever recorded for an hours-long episode.
        let contradictory = completeFulls.filter { asset in
            let terminalText = asset.terminalReason ?? ""
            // (a) terminalReason claims a transcript or feature ratio > 1.0
            //     (e.g. "transcript 1.163, feature 1.724") — only possible
            //     due to a bookkeeping bug.
            let claimsRatioOver1 = terminalText.range(of: #"transcript\s+[1-9]\.\d{3,}"#,
                                                      options: .regularExpression) != nil
                || terminalText.range(of: #"feature\s+[1-9]\.\d{3,}"#,
                                      options: .regularExpression) != nil
            // (b) fast_transcript_coverage_end_sec is < 50% of duration —
            //     the asset is "complete" but only ~1.5 min of fast
            //     transcript was ever recorded for an hours-long episode.
            let fastWm = asset.fastTranscriptCoverageEndSec ?? 0
            let dur = asset.episodeDurationSec ?? 0
            let watermarkUnderHalf = dur > 0 && fastWm < dur * 0.5
            return claimsRatioOver1 || watermarkUnderHalf
        }
        #expect(
            !contradictory.isEmpty,
            "expected at least one completeFull asset with contradictory coverage / terminal state; sample terminal_reason: \(completeFulls.first?.terminalReason ?? "n/a")"
        )
    }

    // MARK: - Correction duplicates

    @Test("Correction rows contain duplicates, including one falseNegative scope repeated 4 times")
    func correctionDuplicatesPresent() throws {
        let fixture = try DogfoodAnalysisHealthFixtureLoader.load()
        let duplicateRows = fixture.correctionRows.filter { $0.count > 1 }
        #expect(!duplicateRows.isEmpty,
                "fixture must preserve duplicate correction-scope structure (no dedup at scrub time)")

        let fnFour = fixture.correctionRows.filter {
            $0.correctionType == "falseNegative" && $0.count == 4
        }
        #expect(!fnFour.isEmpty,
                "expected at least one falseNegative scope with count == 4 (the dogfood capture had two)")

        // Every scope must use a synthetic asset id (no raw UUIDs leaked).
        let uuidPattern = #"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}"#
        for row in fixture.correctionRows {
            #expect(row.scope.range(of: uuidPattern, options: .regularExpression) == nil,
                    "correction scope leaks a raw UUID: \(row.scope)")
        }
    }

    // MARK: - Learning vs shadow

    @Test("Learning-table counts are all zero while shadow FM response count is nonzero")
    func learningEmptyShadowNonempty() throws {
        let fixture = try DogfoodAnalysisHealthFixtureLoader.load()
        let counts = fixture.learningTableCounts
        #expect(counts.sponsorKnowledgeEntries == 0)
        #expect(counts.trainingExamples == 0)
        #expect(counts.adCopyFingerprints == 0)
        #expect(counts.boundaryPriors == 0)
        #expect(counts.implicitFeedbackEvents == 0)
        #expect(counts.knowledgeCandidateEvents == 0)
        #expect(counts.musicBracketTrust == 0)

        #expect(fixture.shadowFmResponseCount > 0,
                "shadow_fm_response_count must be positive; the dogfood capture wrote 1321")
    }

    // MARK: - Background-task event counts

    @Test("Background-task summary records submits / starts / completes / expirations and a backfill category")
    func backgroundTaskCategoriesPresent() throws {
        let fixture = try DogfoodAnalysisHealthFixtureLoader.load()
        let overall = fixture.backgroundTaskEvents.overall
        // Overall counts must include the four lifecycle events the bead's
        // downstream consumers care about.
        for key in ["submit", "start", "complete", "expire"] {
            #expect((overall[key] ?? 0) > 0,
                    "expected overall.\(key) > 0; got \(overall[key] ?? 0)")
        }

        // The analysis-backfill category must surface in by_category with the
        // same lifecycle keys — downstream beads use it to compare the
        // "background work attempted" count against the "completed" count.
        let backfill = fixture.backgroundTaskEvents.byCategory["com.playhead.app.analysis.backfill"]
        #expect(backfill != nil,
                "expected analysis-backfill category to be present in by_category")
        if let backfill {
            #expect((backfill["submit"] ?? 0) > 0)
            #expect((backfill["start"] ?? 0) > 0)
            #expect((backfill["complete"] ?? 0) > 0)
            // Expirations are particularly load-bearing for downstream beads
            // diagnosing why work falls off.
            #expect((backfill["expire"] ?? 0) > 0,
                    "expected analysis-backfill expirations > 0; got \(backfill["expire"] ?? 0)")
        }
    }

    // MARK: - Scrubbing audit

    @Test("Fixture file contains no raw identifiers, URLs, hashes, FM payloads, or device paths")
    func fixtureIsScrubbed() throws {
        let url = DogfoodAnalysisHealthFixtureLoader.fixtureURL()
        let raw = try String(contentsOf: url, encoding: .utf8)

        // Forbidden substrings — picked from the source-of-truth dogfood
        // export. Each entry has a one-line rationale so future
        // contributors know why it's banned.
        let forbidden: [(String, String)] = [
            ("9C109975", "raw analysisAssetId UUID prefix"),
            ("8A9DFC82", "raw analysisAssetId UUID prefix"),
            ("C75C2E85", "raw analysisAssetId UUID prefix"),
            ("E8F0F867", "raw analysisAssetId UUID prefix"),
            ("flightcast", "raw feed URL component"),
            ("simplecast", "raw feed URL component"),
            ("libsyn", "raw feed URL component"),
            ("acast", "raw feed URL component"),
            ("https://", "raw URL"),
            ("/var/mobile/", "device-specific filesystem path"),
            ("AudioCache", "device-specific filesystem path"),
            ("fmResponseBase64", "FM response payload field name"),
            ("episode_id_hash", "raw activity-row hash field name"),
            ("session_id", "raw session UUID field name"),
            ("BuildProvenance", "build-stamp file referenced in raw bundle"),
        ]

        for (token, why) in forbidden {
            #expect(!raw.contains(token),
                    "fixture must not contain \(token) (\(why))")
        }

        // No 36-char UUIDs of the form 8-4-4-4-12 should appear at all.
        let uuidPattern = #"[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}"#
        if let match = raw.range(of: uuidPattern, options: .regularExpression) {
            Issue.record("fixture contains UUID-shaped string: \(raw[match])")
        }

        // No 64-char hex strings (SHA-256 candidates).
        let sha256Pattern = #"\b[0-9a-f]{64}\b"#
        if let match = raw.range(of: sha256Pattern, options: .regularExpression) {
            Issue.record("fixture contains SHA-256-shaped hex: \(raw[match])")
        }
    }

    // MARK: - Loader sanity

    @Test("Loader resolves and decodes the bundled fixture")
    func loaderResolvesFixture() throws {
        let url = DogfoodAnalysisHealthFixtureLoader.fixtureURL()
        #expect(FileManager.default.fileExists(atPath: url.path),
                "fixture file missing at \(url.path)")

        let fixture = try DogfoodAnalysisHealthFixtureLoader.load()
        #expect(fixture.schemaVersion == 1)
        #expect(fixture.capturedOn == "2026-05-06")
        #expect(!fixture.analysisAssets.isEmpty)
    }
}
