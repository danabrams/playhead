// ChapterPhaseDiagnosticsTests.swift
// Golden-fixture + privacy regression coverage for the chapter-phase
// diagnostics events introduced in playhead-au2v.1.3.
//
// What this file proves:
//   * Each `ChapterPhaseEventType` round-trips through JSON Encoder /
//     Decoder at the exact wire shape documented in the bead spec.
//   * Episode IDs in payloads are HASHED via the same `EpisodeIdHasher`
//     used by `scheduler_events` (zero PII, hash-keying parity).
//   * Privacy regression: a fixture episode with a known title,
//     transcript, and advertiser name produces ZERO leakage of any of
//     those strings into the encoded chapter-phase event payload.
//   * The `chapter_phase_events` array appears as a top-level sibling
//     of `scheduler_events` in the diagnostics JSON.
//   * The existing diagnostics tooling (`DiagnosticsBundleFile` decoder
//     used by eval / dogfood pipelines) parses a bundle that carries a
//     populated `chapter_phase_events` array without errors.

import Foundation
import Testing

@testable import Playhead

// `@MainActor`-attributed because two of the suite's tests call into
// `DiagnosticsExportService.encode(_:)`, which is itself `@MainActor`
// (the service co-locates the iOS mail composer with the encoder). The
// pure-Codable assertions don't strictly need main-actor isolation, but
// keeping the whole suite on the main actor avoids a sprinkle of
// per-test annotations and matches the convention in
// `DiagnosticsBundleShapeTests`.
@Suite("ChapterPhaseDiagnostics — wire shape, hashing, privacy (playhead-au2v.1.3)")
@MainActor
struct ChapterPhaseDiagnosticsTests {

    // MARK: - Fixtures

    private static let installID = UUID(uuidString: "22222222-2222-4222-8222-222222222222")!
    private static let timestamp: Double = 1_700_000_500
    private static let rawEpisodeId = "ep-au2v-fixture-1"

    /// Pre-computed expected hash for the fixture episode id under the
    /// fixture install id. Asserts hash-keying parity with
    /// `scheduler_events` (`EpisodeIdHasher.hash(installID:episodeId:)`).
    private static var expectedEpisodeIdHash: String {
        EpisodeIdHasher.hash(installID: installID, episodeId: rawEpisodeId)
    }

    private func encode<T: Encodable>(_ value: T) throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        return try encoder.encode(value)
    }

    private func decode<T: Decodable>(_ type: T.Type, from data: Data) throws -> T {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(type, from: data)
    }

    /// Walk a parsed JSON object collecting every key it contains. Used
    /// by the privacy regression to assert specific keys are present
    /// (e.g. `episode_id_hash`) and absent (e.g. `episode_title`).
    private func collectAllKeys(in root: Any) -> [String] {
        var keys: [String] = []
        var stack: [Any] = [root]
        while let next = stack.popLast() {
            if let dict = next as? [String: Any] {
                keys.append(contentsOf: dict.keys)
                stack.append(contentsOf: dict.values)
            } else if let array = next as? [Any] {
                stack.append(contentsOf: array)
            }
        }
        return keys
    }

    // MARK: - Hashing parity with scheduler_events

    @Test("Every emit helper hashes the episode id via EpisodeIdHasher (parity with scheduler_events)")
    func emitHelpersHashEpisodeId() {
        // Build one event of every kind through the helpers and assert
        // each carries `expectedEpisodeIdHash` rather than the raw id.
        let events: [ChapterPhaseEvent] = [
            .started(
                installID: Self.installID, episodeId: Self.rawEpisodeId,
                timestamp: Self.timestamp,
                mode: "heuristic_plus_fm",
                transcriptSnapshotHash: String(repeating: "a", count: 64)
            ),
            .skippedCreatorChapters(
                installID: Self.installID, episodeId: Self.rawEpisodeId,
                timestamp: Self.timestamp,
                chapterSource: "id3", chapterCount: 7
            ),
            .skippedAdmission(
                installID: Self.installID, episodeId: Self.rawEpisodeId,
                timestamp: Self.timestamp, denyReason: "thermal_pressure"
            ),
            .noCandidates(
                installID: Self.installID, episodeId: Self.rawEpisodeId,
                timestamp: Self.timestamp
            ),
            .pathologicalRate(
                installID: Self.installID, episodeId: Self.rawEpisodeId,
                timestamp: Self.timestamp,
                candidateCount: 50, episodeDurationSec: 1800,
                candidatesPerSecond: 0.0278
            ),
            .capApplied(
                installID: Self.installID, episodeId: Self.rawEpisodeId,
                timestamp: Self.timestamp,
                detectedCount: 24, cappedCount: 12, targetDensity: 0.0067
            ),
            .labelFailed(
                installID: Self.installID, episodeId: Self.rawEpisodeId,
                timestamp: Self.timestamp,
                operational: true, errorCode: "fm_timeout",
                retryCount: 2, finalOutcome: "fell_back_to_heuristic"
            ),
            .operationalUnclearRateExceeded(
                installID: Self.installID, episodeId: Self.rawEpisodeId,
                timestamp: Self.timestamp,
                labeledCount: 10, operationalUnclearCount: 4,
                operationalUnclearRate: 0.4, threshold: 0.3
            ),
            .highUnclearRate(
                installID: Self.installID, episodeId: Self.rawEpisodeId,
                timestamp: Self.timestamp,
                labeledCount: 10, operationalUnclearCount: 2,
                semanticUnclearCount: 4,
                totalUnclearRate: 0.6, threshold: 0.5
            ),
            .completed(
                installID: Self.installID, episodeId: Self.rawEpisodeId,
                timestamp: Self.timestamp,
                chapterCount: 12, planConfidence: 0.85,
                fmCallCount: 12, latencyMs: 4200.5
            ),
            .preempted(
                installID: Self.installID, episodeId: Self.rawEpisodeId,
                timestamp: Self.timestamp
            ),
            .decodeFailure(
                installID: Self.installID, episodeId: Self.rawEpisodeId,
                timestamp: Self.timestamp,
                stage: "chapter_plan_cache", errorCode: "corrupt_data"
            ),
            .coveragePlanChapterInformed(
                installID: Self.installID, episodeId: Self.rawEpisodeId,
                timestamp: Self.timestamp,
                fractionReplaced: 0.5,
                adChapterIncludedCount: 2,
                contentChapterExcludedCount: 1,
                planConfidence: 0.65
            ),
            .coveragePlanChapterSkipped(
                installID: Self.installID, episodeId: Self.rawEpisodeId,
                timestamp: Self.timestamp,
                reason: "low_plan_confidence",
                evidenceCount: 4
            )
        ]

        for event in events {
            #expect(event.episodeIdHash == Self.expectedEpisodeIdHash,
                    "Event \(event.eventType.rawValue) must hash episode id via EpisodeIdHasher")
        }

        // Belt-and-suspenders: every documented event type is covered by
        // the helper-set above. If a new case is added to the enum, this
        // count assertion forces the test author to extend the helper
        // vocabulary in lockstep.
        #expect(events.count == ChapterPhaseEventType.allCases.count,
                "Helper coverage must match ChapterPhaseEventType case count")
        let producedTypes = Set(events.map(\.eventType))
        let allTypes = Set(ChapterPhaseEventType.allCases)
        #expect(producedTypes == allTypes,
                "Helper coverage must include every ChapterPhaseEventType case")
    }

    // MARK: - Per-event golden payload fixtures

    @Test("Golden — chapter_phase_started")
    func goldenStarted() throws {
        let event = ChapterPhaseEvent.started(
            installID: Self.installID, episodeId: Self.rawEpisodeId,
            timestamp: Self.timestamp,
            mode: "heuristic_plus_fm",
            transcriptSnapshotHash: "deadbeef" + String(repeating: "0", count: 56)
        )
        let json = try encode(event)
        let str = try #require(String(data: json, encoding: .utf8))
        let expected = """
        {"episode_id_hash":"\(Self.expectedEpisodeIdHash)","event_type":"chapter_phase_started","payload":{"started":{"mode":"heuristic_plus_fm","transcript_snapshot_hash":"deadbeef00000000000000000000000000000000000000000000000000000000"}},"timestamp":1700000500}
        """
        #expect(str == expected, "Got: \(str)")
        // Round-trip parity.
        let decoded = try decode(ChapterPhaseEvent.self, from: json)
        #expect(decoded == event)
    }

    @Test("Golden — chapter_phase_skipped_creator_chapters")
    func goldenSkippedCreatorChapters() throws {
        let event = ChapterPhaseEvent.skippedCreatorChapters(
            installID: Self.installID, episodeId: Self.rawEpisodeId,
            timestamp: Self.timestamp,
            chapterSource: "id3", chapterCount: 7
        )
        let str = try #require(String(data: try encode(event), encoding: .utf8))
        let expected = """
        {"episode_id_hash":"\(Self.expectedEpisodeIdHash)","event_type":"chapter_phase_skipped_creator_chapters","payload":{"skipped_creator_chapters":{"chapter_count":7,"chapter_source":"id3"}},"timestamp":1700000500}
        """
        #expect(str == expected)
        #expect(try decode(ChapterPhaseEvent.self, from: encode(event)) == event)
    }

    @Test("Golden — chapter_phase_skipped_admission")
    func goldenSkippedAdmission() throws {
        let event = ChapterPhaseEvent.skippedAdmission(
            installID: Self.installID, episodeId: Self.rawEpisodeId,
            timestamp: Self.timestamp, denyReason: "thermal_pressure"
        )
        let str = try #require(String(data: try encode(event), encoding: .utf8))
        let expected = """
        {"episode_id_hash":"\(Self.expectedEpisodeIdHash)","event_type":"chapter_phase_skipped_admission","payload":{"skipped_admission":{"deny_reason":"thermal_pressure"}},"timestamp":1700000500}
        """
        #expect(str == expected)
        #expect(try decode(ChapterPhaseEvent.self, from: encode(event)) == event)
    }

    @Test("Golden — chapter_phase_no_candidates (payload key absent)")
    func goldenNoCandidates() throws {
        let event = ChapterPhaseEvent.noCandidates(
            installID: Self.installID, episodeId: Self.rawEpisodeId,
            timestamp: Self.timestamp
        )
        let str = try #require(String(data: try encode(event), encoding: .utf8))
        // `payload` is optional and nil — the key must be omitted.
        let expected = """
        {"episode_id_hash":"\(Self.expectedEpisodeIdHash)","event_type":"chapter_phase_no_candidates","timestamp":1700000500}
        """
        #expect(str == expected)
        #expect(!str.contains("\"payload\""), "Expected `payload` key to be omitted when nil")
        #expect(try decode(ChapterPhaseEvent.self, from: encode(event)) == event)
    }

    @Test("Golden — chapter_phase_pathological_rate")
    func goldenPathologicalRate() throws {
        let event = ChapterPhaseEvent.pathologicalRate(
            installID: Self.installID, episodeId: Self.rawEpisodeId,
            timestamp: Self.timestamp,
            candidateCount: 50, episodeDurationSec: 1800, candidatesPerSecond: 0.025
        )
        let str = try #require(String(data: try encode(event), encoding: .utf8))
        let expected = """
        {"episode_id_hash":"\(Self.expectedEpisodeIdHash)","event_type":"chapter_phase_pathological_rate","payload":{"pathological_rate":{"candidate_count":50,"candidates_per_second":0.025,"episode_duration_sec":1800}},"timestamp":1700000500}
        """
        #expect(str == expected)
        #expect(try decode(ChapterPhaseEvent.self, from: encode(event)) == event)
    }

    @Test("Golden — chapter_phase_cap_applied")
    func goldenCapApplied() throws {
        let event = ChapterPhaseEvent.capApplied(
            installID: Self.installID, episodeId: Self.rawEpisodeId,
            timestamp: Self.timestamp,
            detectedCount: 24, cappedCount: 12, targetDensity: 0.0067
        )
        let str = try #require(String(data: try encode(event), encoding: .utf8))
        let expected = """
        {"episode_id_hash":"\(Self.expectedEpisodeIdHash)","event_type":"chapter_phase_cap_applied","payload":{"cap_applied":{"capped_count":12,"detected_count":24,"target_density":0.0067}},"timestamp":1700000500}
        """
        #expect(str == expected)
        #expect(try decode(ChapterPhaseEvent.self, from: encode(event)) == event)
    }

    @Test("Golden — chapter_phase_label_failed")
    func goldenLabelFailed() throws {
        let event = ChapterPhaseEvent.labelFailed(
            installID: Self.installID, episodeId: Self.rawEpisodeId,
            timestamp: Self.timestamp,
            operational: true, errorCode: "fm_timeout",
            retryCount: 2, finalOutcome: "fell_back_to_heuristic"
        )
        let str = try #require(String(data: try encode(event), encoding: .utf8))
        let expected = """
        {"episode_id_hash":"\(Self.expectedEpisodeIdHash)","event_type":"chapter_phase_label_failed","payload":{"label_failed":{"error_code":"fm_timeout","final_outcome":"fell_back_to_heuristic","operational":true,"retry_count":2}},"timestamp":1700000500}
        """
        #expect(str == expected)
        #expect(try decode(ChapterPhaseEvent.self, from: encode(event)) == event)
    }

    @Test("Golden — chapter_phase_operational_unclear_rate_exceeded")
    func goldenOperationalUnclearRate() throws {
        let event = ChapterPhaseEvent.operationalUnclearRateExceeded(
            installID: Self.installID, episodeId: Self.rawEpisodeId,
            timestamp: Self.timestamp,
            labeledCount: 10, operationalUnclearCount: 4,
            operationalUnclearRate: 0.4, threshold: 0.3
        )
        let str = try #require(String(data: try encode(event), encoding: .utf8))
        let expected = """
        {"episode_id_hash":"\(Self.expectedEpisodeIdHash)","event_type":"chapter_phase_operational_unclear_rate_exceeded","payload":{"operational_unclear_rate_exceeded":{"labeled_count":10,"operational_unclear_count":4,"operational_unclear_rate":0.4,"threshold":0.3}},"timestamp":1700000500}
        """
        #expect(str == expected)
        #expect(try decode(ChapterPhaseEvent.self, from: encode(event)) == event)
    }

    @Test("Golden — chapter_phase_high_unclear_rate")
    func goldenHighUnclearRate() throws {
        // playhead-au2v.1.8: emitted when (operational + semantic) /
        // labeled exceeds 50% but operational alone stayed below the
        // 30% abort threshold. The plan is still written; the event
        // is the support-engineer breadcrumb that this episode's
        // chapter labels are coarsely trusted.
        let event = ChapterPhaseEvent.highUnclearRate(
            installID: Self.installID, episodeId: Self.rawEpisodeId,
            timestamp: Self.timestamp,
            labeledCount: 10, operationalUnclearCount: 2,
            semanticUnclearCount: 4,
            totalUnclearRate: 0.6, threshold: 0.5
        )
        let str = try #require(String(data: try encode(event), encoding: .utf8))
        let expected = """
        {"episode_id_hash":"\(Self.expectedEpisodeIdHash)","event_type":"chapter_phase_high_unclear_rate","payload":{"high_unclear_rate":{"labeled_count":10,"operational_unclear_count":2,"semantic_unclear_count":4,"threshold":0.5,"total_unclear_rate":0.6}},"timestamp":1700000500}
        """
        #expect(str == expected)
        #expect(try decode(ChapterPhaseEvent.self, from: encode(event)) == event)
    }

    @Test("Golden — chapter_phase_completed")
    func goldenCompleted() throws {
        let event = ChapterPhaseEvent.completed(
            installID: Self.installID, episodeId: Self.rawEpisodeId,
            timestamp: Self.timestamp,
            chapterCount: 12, planConfidence: 0.85,
            fmCallCount: 12, latencyMs: 4200.5
        )
        let str = try #require(String(data: try encode(event), encoding: .utf8))
        let expected = """
        {"episode_id_hash":"\(Self.expectedEpisodeIdHash)","event_type":"chapter_phase_completed","payload":{"completed":{"chapter_count":12,"fm_call_count":12,"latency_ms":4200.5,"plan_confidence":0.85}},"timestamp":1700000500}
        """
        #expect(str == expected)
        #expect(try decode(ChapterPhaseEvent.self, from: encode(event)) == event)
    }

    @Test("Golden — chapter_phase_preempted (payload key absent)")
    func goldenPreempted() throws {
        let event = ChapterPhaseEvent.preempted(
            installID: Self.installID, episodeId: Self.rawEpisodeId,
            timestamp: Self.timestamp
        )
        let str = try #require(String(data: try encode(event), encoding: .utf8))
        let expected = """
        {"episode_id_hash":"\(Self.expectedEpisodeIdHash)","event_type":"chapter_phase_preempted","timestamp":1700000500}
        """
        #expect(str == expected)
        #expect(!str.contains("\"payload\""))
        #expect(try decode(ChapterPhaseEvent.self, from: encode(event)) == event)
    }

    @Test("Golden — chapter_phase_decode_failure")
    func goldenDecodeFailure() throws {
        let event = ChapterPhaseEvent.decodeFailure(
            installID: Self.installID, episodeId: Self.rawEpisodeId,
            timestamp: Self.timestamp,
            stage: "chapter_plan_cache", errorCode: "corrupt_data"
        )
        let str = try #require(String(data: try encode(event), encoding: .utf8))
        let expected = """
        {"episode_id_hash":"\(Self.expectedEpisodeIdHash)","event_type":"chapter_phase_decode_failure","payload":{"decode_failure":{"error_code":"corrupt_data","stage":"chapter_plan_cache"}},"timestamp":1700000500}
        """
        #expect(str == expected)
        #expect(try decode(ChapterPhaseEvent.self, from: encode(event)) == event)
    }

    @Test("Golden — coverage_plan_chapter_informed")
    func goldenCoveragePlanChapterInformed() throws {
        let event = ChapterPhaseEvent.coveragePlanChapterInformed(
            installID: Self.installID, episodeId: Self.rawEpisodeId,
            timestamp: Self.timestamp,
            fractionReplaced: 0.5,
            adChapterIncludedCount: 2,
            contentChapterExcludedCount: 1,
            planConfidence: 0.65
        )
        let str = try #require(String(data: try encode(event), encoding: .utf8))
        let expected = """
        {"episode_id_hash":"\(Self.expectedEpisodeIdHash)","event_type":"coverage_plan_chapter_informed","payload":{"coverage_plan_chapter_informed":{"ad_chapter_included_count":2,"content_chapter_excluded_count":1,"fraction_replaced":0.5,"plan_confidence":0.65}},"timestamp":1700000500}
        """
        #expect(str == expected)
        #expect(try decode(ChapterPhaseEvent.self, from: encode(event)) == event)
    }

    @Test("Golden — coverage_plan_chapter_skipped")
    func goldenCoveragePlanChapterSkipped() throws {
        let event = ChapterPhaseEvent.coveragePlanChapterSkipped(
            installID: Self.installID, episodeId: Self.rawEpisodeId,
            timestamp: Self.timestamp,
            reason: "low_plan_confidence",
            evidenceCount: 4
        )
        let str = try #require(String(data: try encode(event), encoding: .utf8))
        let expected = """
        {"episode_id_hash":"\(Self.expectedEpisodeIdHash)","event_type":"coverage_plan_chapter_skipped","payload":{"coverage_plan_chapter_skipped":{"evidence_count":4,"reason":"low_plan_confidence"}},"timestamp":1700000500}
        """
        #expect(str == expected)
        #expect(try decode(ChapterPhaseEvent.self, from: encode(event)) == event)
    }

    // MARK: - Privacy regression: known PII fixture

    /// The privacy regression: a fixture episode whose raw id collides
    /// with an obviously-PII title and advertiser name. We construct
    /// every event variant for this episode and assert NONE of those
    /// PII tokens appear anywhere in the encoded JSON values.
    @Test("Privacy regression — no episode title / transcript / advertiser leakage in any event payload")
    func privacyRegressionNoLeakage() throws {
        // Tokens we deliberately seed into the FIXTURE INPUTS (episode
        // id and surrounding context). The encoded events MUST NOT
        // contain any of these strings — episode id is hashed, payloads
        // do not carry titles/transcripts/advertiser names.
        let title = "EXTREMELY-PRIVATE-EPISODE-TITLE-the-divorce-special"
        let transcriptLine = "EXTREMELY-PRIVATE-TRANSCRIPT-line-mentioning-confidential-details"
        let advertiser = "EXTREMELY-PRIVATE-ADVERTISER-AcmeAds"
        // The raw episode id intentionally embeds the same tokens; if
        // the hash or the wire shape ever leaked the input it would
        // show up verbatim.
        let pollutedEpisodeId = "ep-\(title)-\(transcriptLine)-\(advertiser)"

        // Construct one event of every variant against the polluted id.
        let events: [ChapterPhaseEvent] = [
            .started(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp,
                // mode is a fixed-vocabulary snake_case string — never
                // populated from episode metadata. Same is true of
                // every other String field below.
                mode: "heuristic_plus_fm",
                transcriptSnapshotHash: String(repeating: "a", count: 64)
            ),
            .skippedCreatorChapters(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp,
                chapterSource: "pc20", chapterCount: 5
            ),
            .skippedAdmission(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp, denyReason: "fm_unavailable"
            ),
            .noCandidates(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp
            ),
            .pathologicalRate(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp,
                candidateCount: 99, episodeDurationSec: 600,
                candidatesPerSecond: 0.165
            ),
            .capApplied(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp,
                detectedCount: 30, cappedCount: 15, targetDensity: 0.025
            ),
            .labelFailed(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp,
                operational: false, errorCode: "fm_decode_failure",
                retryCount: 1, finalOutcome: "gave_up"
            ),
            .operationalUnclearRateExceeded(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp,
                labeledCount: 5, operationalUnclearCount: 3,
                operationalUnclearRate: 0.6, threshold: 0.3
            ),
            .highUnclearRate(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp,
                labeledCount: 8, operationalUnclearCount: 1,
                semanticUnclearCount: 4,
                totalUnclearRate: 0.625, threshold: 0.5
            ),
            .completed(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp,
                chapterCount: 6, planConfidence: 0.7,
                fmCallCount: 6, latencyMs: 1234
            ),
            .preempted(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp
            ),
            .decodeFailure(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp,
                stage: "boundary_candidates_cache", errorCode: "version_mismatch"
            ),
            .coveragePlanChapterInformed(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp,
                fractionReplaced: 0.5,
                adChapterIncludedCount: 1,
                contentChapterExcludedCount: 1,
                planConfidence: 0.55
            ),
            .coveragePlanChapterSkipped(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp,
                reason: "no_usable_chapters",
                evidenceCount: 3
            )
        ]
        #expect(events.count == ChapterPhaseEventType.allCases.count)

        let json = try encode(events)
        let str = try #require(String(data: json, encoding: .utf8))

        for token in [title, transcriptLine, advertiser, pollutedEpisodeId] {
            #expect(!str.contains(token),
                    "PII token leaked into chapter-phase event JSON: \(token)")
        }

        // Walk the JSON tree and confirm:
        //   * `episode_id_hash` is present and is a 64-char SHA-256 hex.
        //   * No `episode_id` / `episodeId` / `episode_title` /
        //     `transcript` / `advertiser` keys appear anywhere.
        let parsed = try JSONSerialization.jsonObject(with: json, options: [])
        let allKeys = Set(collectAllKeys(in: parsed))
        #expect(allKeys.contains("episode_id_hash"))
        for forbidden in [
            "episode_id", "episodeId", "episode_title", "title",
            "transcript", "transcript_text", "advertiser", "advertiser_name"
        ] {
            #expect(!allKeys.contains(forbidden),
                    "Forbidden key '\(forbidden)' present in chapter-phase event JSON tree")
        }

        // Hash shape: SHA-256 hex is 64 lowercase hex chars.
        let hashRegex = #/^[0-9a-f]{64}$/#
        let pollutedHash = EpisodeIdHasher.hash(installID: Self.installID, episodeId: pollutedEpisodeId)
        #expect((try? hashRegex.wholeMatch(in: pollutedHash)) != nil)
        // The encoded JSON must reference the polluted id ONLY through
        // its hash, not the raw form. The hash itself naturally appears
        // in every event.
        #expect(str.contains(pollutedHash))
    }

    // MARK: - Privacy regression at the BUNDLE-FILE surface

    /// Run the same polluted-id privacy regression but at the actual
    /// production surface: build a `DiagnosticsBundleFile` through
    /// `DiagnosticsBundleBuilder.buildDefault(...)` and encode through
    /// `DiagnosticsExportService.encode(_:)` — exactly what
    /// `DiagnosticsExportCoordinator` does at runtime. If any future
    /// refactor accidentally widens a sibling field to carry raw episode
    /// metadata, this test catches it BEFORE shipping.
    @Test("Privacy regression at the bundle-file surface — no PII anywhere in the encoded diagnostics JSON")
    func privacyRegressionAtBundleFileSurface() throws {
        let title = "BUNDLE-LEVEL-PRIVATE-TITLE-divorce-special"
        let advertiser = "BUNDLE-LEVEL-PRIVATE-ADVERTISER-AcmeAds"
        let pollutedEpisodeId = "ep-\(title)-\(advertiser)"

        let eligibility = AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Date(timeIntervalSince1970: Self.timestamp)
        )
        let chapterEvents: [ChapterPhaseEvent] = [
            .completed(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp,
                chapterCount: 4, planConfidence: 0.7,
                fmCallCount: 4, latencyMs: 800
            ),
            .labelFailed(
                installID: Self.installID, episodeId: pollutedEpisodeId,
                timestamp: Self.timestamp,
                operational: false, errorCode: "fm_decode_failure",
                retryCount: 0, finalOutcome: "gave_up"
            )
        ]
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0.0",
            osVersion: "iOS 26.0",
            deviceClass: .iPhone17Pro,
            buildType: .release,
            eligibility: eligibility,
            workJournalEntries: [],
            installID: Self.installID,
            chapterPhaseEvents: chapterEvents
        )
        let file = DiagnosticsBundleFile(
            generatedAt: Date(timeIntervalSince1970: Self.timestamp),
            default: bundle,
            optIn: nil
        )
        let data = try DiagnosticsExportService.encode(file)
        let str = try #require(String(data: data, encoding: .utf8))

        for token in [title, advertiser, pollutedEpisodeId] {
            #expect(!str.contains(token),
                    "PII token '\(token)' leaked into bundle-file JSON")
        }
    }

    // MARK: - Top-level array sibling to scheduler_events

    @Test("`chapter_phase_events` appears as a top-level array sibling of `scheduler_events` in the diagnostics JSON")
    func chapterPhaseEventsAreTopLevelSiblingOfSchedulerEvents() throws {
        // Build a default bundle the way production does — through the
        // pure builder — and assert the encoded subtree carries both
        // sibling arrays.
        let eligibility = AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Date(timeIntervalSince1970: Self.timestamp)
        )
        let chapterEvent = ChapterPhaseEvent.completed(
            installID: Self.installID, episodeId: Self.rawEpisodeId,
            timestamp: Self.timestamp,
            chapterCount: 3, planConfidence: 0.9,
            fmCallCount: 3, latencyMs: 700
        )
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0.0",
            osVersion: "iOS 26.0",
            deviceClass: .iPhone17Pro,
            buildType: .debug,
            eligibility: eligibility,
            workJournalEntries: [],
            installID: Self.installID,
            chapterPhaseEvents: [chapterEvent]
        )
        let file = DiagnosticsBundleFile(
            generatedAt: Date(timeIntervalSince1970: Self.timestamp),
            default: bundle,
            optIn: nil
        )
        let data = try DiagnosticsExportService.encode(file)
        let parsed = try JSONSerialization.jsonObject(with: data, options: [])
        let root = try #require(parsed as? [String: Any])
        let defaultSubtree = try #require(root["default"] as? [String: Any])

        #expect(defaultSubtree["scheduler_events"] != nil,
                "scheduler_events must be present at the documented sibling position")
        let chapterEvents = try #require(
            defaultSubtree["chapter_phase_events"] as? [Any],
            "chapter_phase_events must be a top-level array sibling of scheduler_events"
        )
        #expect(chapterEvents.count == 1)

        // The first (and only) entry must round-trip back to the
        // ChapterPhaseEvent we passed in, proving the bundle file's
        // decoder still sees the new array end-to-end. We mirror the
        // production encoder's `dateEncodingStrategy = .iso8601` here
        // since the file's `generatedAt` is encoded as an ISO-8601
        // string (a JSONDecoder's default for Date is a Double).
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(DiagnosticsBundleFile.self, from: data)
        #expect(decoded.default.chapterPhaseEvents == [chapterEvent])
    }

    // MARK: - Existing tooling parses the new schema without errors

    @Test("Existing diagnostics tooling parses a bundle carrying populated chapter_phase_events without errors")
    func existingDecoderParsesPopulatedChapterPhaseEvents() throws {
        // Build a bundle synthetic enough to mirror what the eval /
        // dogfood pipelines see, but populate `chapter_phase_events`
        // with one of every event kind. The decoder used in those
        // pipelines is `JSONDecoder` against `DiagnosticsBundleFile`
        // (DefaultBundle.init(from:) above) — exactly what we exercise
        // here.
        let eligibility = AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Date(timeIntervalSince1970: Self.timestamp)
        )
        let allEvents: [ChapterPhaseEvent] = ChapterPhaseEventType.allCases.map { type in
            switch type {
            case .started:
                return .started(
                    installID: Self.installID, episodeId: Self.rawEpisodeId,
                    timestamp: Self.timestamp,
                    mode: "heuristic_only",
                    transcriptSnapshotHash: String(repeating: "b", count: 64)
                )
            case .skippedCreatorChapters:
                return .skippedCreatorChapters(
                    installID: Self.installID, episodeId: Self.rawEpisodeId,
                    timestamp: Self.timestamp,
                    chapterSource: "rss_inline", chapterCount: 1
                )
            case .skippedAdmission:
                return .skippedAdmission(
                    installID: Self.installID, episodeId: Self.rawEpisodeId,
                    timestamp: Self.timestamp,
                    denyReason: "region_unsupported"
                )
            case .noCandidates:
                return .noCandidates(
                    installID: Self.installID, episodeId: Self.rawEpisodeId,
                    timestamp: Self.timestamp
                )
            case .pathologicalRate:
                return .pathologicalRate(
                    installID: Self.installID, episodeId: Self.rawEpisodeId,
                    timestamp: Self.timestamp,
                    candidateCount: 200, episodeDurationSec: 1200,
                    candidatesPerSecond: 0.166
                )
            case .capApplied:
                return .capApplied(
                    installID: Self.installID, episodeId: Self.rawEpisodeId,
                    timestamp: Self.timestamp,
                    detectedCount: 16, cappedCount: 8, targetDensity: 0.0067
                )
            case .labelFailed:
                return .labelFailed(
                    installID: Self.installID, episodeId: Self.rawEpisodeId,
                    timestamp: Self.timestamp,
                    operational: true, errorCode: "fm_timeout",
                    retryCount: 1, finalOutcome: "success"
                )
            case .operationalUnclearRateExceeded:
                return .operationalUnclearRateExceeded(
                    installID: Self.installID, episodeId: Self.rawEpisodeId,
                    timestamp: Self.timestamp,
                    labeledCount: 8, operationalUnclearCount: 3,
                    operationalUnclearRate: 0.375, threshold: 0.3
                )
            case .highUnclearRate:
                return .highUnclearRate(
                    installID: Self.installID, episodeId: Self.rawEpisodeId,
                    timestamp: Self.timestamp,
                    labeledCount: 8, operationalUnclearCount: 1,
                    semanticUnclearCount: 4,
                    totalUnclearRate: 0.625, threshold: 0.5
                )
            case .completed:
                return .completed(
                    installID: Self.installID, episodeId: Self.rawEpisodeId,
                    timestamp: Self.timestamp,
                    chapterCount: 7, planConfidence: 0.66,
                    fmCallCount: 7, latencyMs: 2100
                )
            case .preempted:
                return .preempted(
                    installID: Self.installID, episodeId: Self.rawEpisodeId,
                    timestamp: Self.timestamp
                )
            case .decodeFailure:
                return .decodeFailure(
                    installID: Self.installID, episodeId: Self.rawEpisodeId,
                    timestamp: Self.timestamp,
                    stage: "chapter_plan_cache", errorCode: "truncated_payload"
                )
            case .coveragePlanChapterInformed:
                return .coveragePlanChapterInformed(
                    installID: Self.installID, episodeId: Self.rawEpisodeId,
                    timestamp: Self.timestamp,
                    fractionReplaced: 0.5,
                    adChapterIncludedCount: 3,
                    contentChapterExcludedCount: 2,
                    planConfidence: 0.72
                )
            case .coveragePlanChapterSkipped:
                return .coveragePlanChapterSkipped(
                    installID: Self.installID, episodeId: Self.rawEpisodeId,
                    timestamp: Self.timestamp,
                    reason: "mode_disabled",
                    evidenceCount: 0
                )
            }
        }
        #expect(allEvents.count == ChapterPhaseEventType.allCases.count)

        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0.0",
            osVersion: "iOS 26.0",
            deviceClass: .iPhone17Pro,
            buildType: .testFlight,
            eligibility: eligibility,
            workJournalEntries: [],
            installID: Self.installID,
            chapterPhaseEvents: allEvents
        )
        let file = DiagnosticsBundleFile(
            generatedAt: Date(timeIntervalSince1970: Self.timestamp),
            default: bundle,
            optIn: nil
        )
        let data = try DiagnosticsExportService.encode(file)

        // Decode through the same code path the eval / dogfood pipelines
        // would use today: a `JSONDecoder` configured with
        // `dateDecodingStrategy = .iso8601` (mirrors the producer in
        // `DiagnosticsExportService.encode`). No errors thrown ==
        // "existing tooling parses the new schema".
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(DiagnosticsBundleFile.self, from: data)
        #expect(decoded.default.chapterPhaseEvents.count == allEvents.count)
        #expect(decoded.default.chapterPhaseEvents == allEvents)
    }

    // MARK: - Decoder resilience: unknown payload variants and missing payload

    @Test("Decoder rejects a payload object with no known variant key (typed dataCorrupted)")
    func decoderRejectsUnknownPayloadVariant() {
        // An event whose payload object carries ONLY an unknown variant
        // key. The decoder must not silently succeed with a default
        // payload — it must throw `dataCorrupted` so eval-pipeline
        // readers can detect schema drift.
        let json = """
        {
          "timestamp": 1700000500,
          "event_type": "chapter_phase_completed",
          "episode_id_hash": "\(Self.expectedEpisodeIdHash)",
          "payload": {"future_variant_we_have_not_shipped": {"foo": 1}}
        }
        """.data(using: .utf8)!
        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(ChapterPhaseEvent.self, from: json)
        }
    }

    @Test("Decoder accepts an event whose `payload` key is absent (parity with stateless events)")
    func decoderAcceptsEventWithMissingPayloadKey() throws {
        // Stateless events (`preempted`, `no_candidates`) ship without
        // a `payload` key. Stricter event types (`completed`, etc.)
        // would still ship one — but a forwards-compatible reader must
        // treat the missing key as "no payload" rather than throwing,
        // so an older v=au2v.1.3 reader can decode a future bundle
        // whose `chapter_phase_completed` event later loses a field.
        let json = """
        {
          "timestamp": 1700000500,
          "event_type": "chapter_phase_preempted",
          "episode_id_hash": "\(Self.expectedEpisodeIdHash)"
        }
        """.data(using: .utf8)!
        let decoded = try JSONDecoder().decode(ChapterPhaseEvent.self, from: json)
        #expect(decoded.payload == nil)
        #expect(decoded.eventType == .preempted)
    }

    @Test("Decoder rejects an event whose `event_type` is an unknown raw string")
    func decoderRejectsUnknownEventType() {
        // `ChapterPhaseEventType` is a closed enum: an unknown raw
        // value must surface as a typed decoding error rather than
        // silently downgrading to a sentinel.
        let json = """
        {
          "timestamp": 1700000500,
          "event_type": "chapter_phase_NOT_A_REAL_TYPE",
          "episode_id_hash": "\(Self.expectedEpisodeIdHash)"
        }
        """.data(using: .utf8)!
        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(ChapterPhaseEvent.self, from: json)
        }
    }

    // MARK: - Backwards-compatible decode of pre-au2v.1.3 bundles

    @Test("Default bundle decodes to chapter_phase_events == [] when the field is absent (forward-compat with v<au2v.1.3 fixtures)")
    func backwardsCompatibleDecodeOfBundleWithoutChapterField() throws {
        // Hand-rolled minimal bundle JSON that has every required field
        // EXCEPT `chapter_phase_events`. The bundle decoder must accept
        // this and produce `chapterPhaseEvents == []`.
        let json = """
        {
          "default": {
            "app_version": "0.9",
            "os_version": "iOS 25",
            "device_class": "iPhone17Pro",
            "build_type": "release",
            "eligibility_snapshot": {
              "hardwareSupported": true,
              "appleIntelligenceEnabled": true,
              "regionSupported": true,
              "languageSupported": true,
              "modelAvailableNow": true,
              "capturedAt": "2023-11-14T22:13:20Z"
            },
            "scheduler_events": [],
            "work_journal_tail": []
          },
          "generated_at": "2023-11-14T22:13:20Z"
        }
        """.data(using: .utf8)!
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(DiagnosticsBundleFile.self, from: json)
        #expect(decoded.default.chapterPhaseEvents.isEmpty)
    }
}
