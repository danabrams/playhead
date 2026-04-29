// DiagnosticExportRedactionTests.swift
// playhead-h3h: End-to-end privacy proof for the diagnostics-export
// surface — the only user-driven path that exfiltrates state off the
// device. The bead's contract:
//
//   * Default bundle: never carries raw transcript text, never carries
//     a raw episodeId. All episode references are
//     `EpisodeIdHasher.hash(installID:episodeId:)` hex.
//   * OptIn bundle (only when the user explicitly opted-in a given
//     episode): transcript excerpts bounded by ±30s window AND truncated
//     to 1000 chars per excerpt.
//   * No `audio` / `raw_audio` field anywhere in the encoded JSON.
//
// We drive `DiagnosticsExportCoordinator.buildAndEncode()` with seeded
// inputs and assert against the encoded JSON Data — the same artifact
// the user would email to support. Production has separate unit tests
// for the pure builder; this suite is the privacy-gate proof at the
// coordinator's encode boundary.
//
// What is NOT testable in-process (deferred to real-device verification):
//   * Whether MFMailComposeViewController itself rewrites the attachment
//     before send. The system mail composer is Apple-owned and cannot be
//     intercepted from inside the process. Verified manually by sending
//     a bundle to a developer-controlled inbox during release QA.

import Foundation
import Testing
@testable import Playhead

// MARK: - Fakes (parallel to the existing CoordinatorTests but local
// so this suite is self-contained and survives churn in the other file)

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

@MainActor
private func makeEnvironment(
    installID: UUID = UUID(uuidString: "DEADBEEF-0000-4000-8000-000000000001")!,
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

@Suite("playhead-h3h - diagnostic export redaction", .serialized)
@MainActor
struct DiagnosticExportRedactionTests {

    /// Sentinel transcript text the seeded chunks carry. Must NEVER
    /// appear in the default bundle, and must be bounded in the opt-in
    /// bundle.
    private static let transcriptSentinels = [
        "Welcome to the privacy gate test episode for playhead-h3h",
        "Squarespace",
        "Use code SHOW for 20 percent off",
        "Now back to our content"
    ]

    private static let rawEpisodeIds = [
        "ep-h3h-default-leak-1",
        "ep-h3h-default-leak-2",
        "ep-h3h-optin-only"
    ]

    // MARK: - Default bundle: zero transcript text, zero raw episode IDs

    @Test("Default bundle JSON contains no raw transcript text and no raw episode IDs")
    func defaultBundleContainsNoTranscriptOrEpisodeId() async throws {
        // Seed a journal with rows referencing real episode IDs so we
        // can prove they're hashed at the bundle boundary.
        let entries: [WorkJournalEntry] = Self.rawEpisodeIds.enumerated().map { idx, id in
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
            environment: makeEnvironment(),
            presenter: StubPresenter(),
            journalFetch: { entries },
            optInSink: StubOptInSink(),
            // No opt-in episodes → opt-in bundle must be omitted.
            optInEpisodes: []
        )

        let (data, _, _) = try await coordinator.buildAndEncode()
        let jsonString = String(decoding: data, as: UTF8.self)

        // No raw episode IDs anywhere.
        for raw in Self.rawEpisodeIds {
            #expect(
                !jsonString.contains(raw),
                "raw episodeId '\(raw)' leaked into default-bundle JSON"
            )
        }

        // No transcript sentinels — the default bundle has no
        // transcript shape, so this is a belt-and-suspenders check
        // against any future addition that might bring transcript text
        // into the always-on bundle.
        for sentinel in Self.transcriptSentinels {
            #expect(
                !jsonString.contains(sentinel),
                "transcript sentinel '\(sentinel)' leaked into default-bundle JSON"
            )
        }

        // Decode and assert the structural contract too.
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(DiagnosticsBundleFile.self, from: data)
        #expect(decoded.optIn == nil,
                "optIn must be omitted when no episode opted in")
        #expect(decoded.default.workJournalTail.count == Self.rawEpisodeIds.count)
        for record in decoded.default.workJournalTail {
            // episode_id_hash hex (SHA-256 → 64 hex chars). The
            // builder hashes through EpisodeIdHasher; we only assert
            // the hash is non-empty AND not equal to any raw id.
            #expect(!record.episodeIdHash.isEmpty)
            for raw in Self.rawEpisodeIds {
                #expect(record.episodeIdHash != raw)
            }
        }
    }

    // MARK: - OptIn bundle: transcript bounded by ±30s and 1000-char cap

    @Test("OptIn bundle excerpts are bounded by ±30s window and 1000-char truncation")
    func optInExcerptsAreBounded() async throws {
        // Build a chunk set that intentionally exceeds the 1000-char
        // cap when concatenated. Each chunk text is 200 chars; place
        // 10 chunks back-to-back so concat is 200 * 10 + 9 separator
        // spaces = 2009 chars, well over the cap.
        let longText = String(repeating: "x", count: 200)
        let manyChunks: [DiagnosticsTranscriptChunk] = (0..<10).map { idx in
            // Each chunk lives in a 6-second slot starting at the ad
            // boundary so all of them are within the ±30s window.
            DiagnosticsTranscriptChunk(
                startTime: 25 + Double(idx) * 6,
                endTime: 25 + Double(idx + 1) * 6,
                text: longText
            )
        }
        // One out-of-window chunk that MUST NOT appear in the excerpt.
        let outOfWindow = DiagnosticsTranscriptChunk(
            startTime: 1000,
            endTime: 1010,
            text: "OUT-OF-WINDOW-SENTINEL-h3h"
        )

        let optInEpisode = DiagnosticsEpisodeInput(
            episodeId: Self.rawEpisodeIds[2],
            episodeTitle: "OptIn Episode",
            diagnosticsOptIn: true,
            adBoundaryTimes: [60],
            transcriptChunks: manyChunks + [outOfWindow],
            featureSummary: nil
        )

        let coordinator = DiagnosticsExportCoordinator(
            environment: makeEnvironment(),
            presenter: StubPresenter(),
            journalFetch: { [] },
            optInSink: StubOptInSink(),
            optInEpisodes: [optInEpisode]
        )

        let (data, _, _) = try await coordinator.buildAndEncode()
        let jsonString = String(decoding: data, as: UTF8.self)

        // Out-of-window text must NOT appear.
        #expect(
            !jsonString.contains("OUT-OF-WINDOW-SENTINEL-h3h"),
            "out-of-window transcript chunk leaked into opt-in excerpt"
        )

        // Decode and assert the per-excerpt cap.
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(DiagnosticsBundleFile.self, from: data)
        let optIn = try #require(decoded.optIn)
        #expect(optIn.episodes.count == 1)
        let episode = optIn.episodes[0]
        #expect(episode.transcriptExcerpts.count == 1)
        let excerpt = episode.transcriptExcerpts[0]
        #expect(
            excerpt.text.count <= DiagnosticsBundleBuilder.transcriptExcerptCharCap,
            "excerpt length \(excerpt.text.count) exceeded the 1000-char cap"
        )
        // Window bounds match the ±30s contract relative to the boundary.
        #expect(excerpt.boundaryTime == 60)
        #expect(excerpt.startTime == 30)
        #expect(excerpt.endTime == 90)
    }

    // MARK: - No audio fields anywhere

    @Test("Encoded JSON contains no audio / raw_audio fields anywhere in either bundle")
    func encodedJsonContainsNoAudioFields() async throws {
        // Seed both bundles so this assertion holds across the full
        // export shape.
        let optInEpisode = DiagnosticsEpisodeInput(
            episodeId: Self.rawEpisodeIds[2],
            episodeTitle: "OptIn Episode",
            diagnosticsOptIn: true,
            adBoundaryTimes: [60],
            transcriptChunks: [
                DiagnosticsTranscriptChunk(
                    startTime: 30, endTime: 60,
                    text: "Welcome to the privacy gate test episode for playhead-h3h."
                )
            ],
            featureSummary: OptInBundle.FeatureSummary(
                rmsMean: 0.1, rmsMax: 0.5,
                spectralFluxMean: 0.2,
                musicProbabilityMean: 0.3,
                pauseProbabilityMean: 0.1
            )
        )
        let entries: [WorkJournalEntry] = [
            WorkJournalEntry(
                id: "row-1",
                episodeId: Self.rawEpisodeIds[0],
                generationID: UUID(),
                schedulerEpoch: 0,
                timestamp: 1_700_000_000,
                eventType: .acquired,
                cause: nil,
                metadata: "{}",
                artifactClass: .scratch
            )
        ]

        let coordinator = DiagnosticsExportCoordinator(
            environment: makeEnvironment(),
            presenter: StubPresenter(),
            journalFetch: { entries },
            optInSink: StubOptInSink(),
            optInEpisodes: [optInEpisode]
        )

        let (data, _, _) = try await coordinator.buildAndEncode()
        let jsonString = String(decoding: data, as: UTF8.self)

        // Forbidden field tokens. Match against the encoded key form
        // (snake_case with surrounding quote/colon).
        let forbidden = [
            "\"audio\"",
            "\"raw_audio\"",
            "\"audio_bytes\"",
            "\"audio_data\"",
            "\"audio_samples\"",
            "\"pcm\"",
            "\"waveform\""
        ]
        for token in forbidden {
            #expect(
                !jsonString.contains(token),
                "forbidden audio field '\(token)' present in encoded bundle"
            )
        }
    }

    // MARK: - Sentinel sweep at the encoded-bytes boundary

    @Test("Default bundle never contains transcript sentinels even when transcript inputs exist for opted-in episodes")
    func transcriptSentinelsConfinedToOptInBundle() async throws {
        // The default bundle is built from journal entries only — it
        // never sees `transcriptChunks`. But the coordinator wires
        // both bundles into one file. We assert that even when the
        // opt-in bundle carries transcript text, that text appears
        // ONLY inside the `opt_in` subtree of the encoded JSON, not
        // inside the `default` subtree. Achieved structurally rather
        // than by string surgery: round-trip through the decoder and
        // re-encode just the default subtree, then assert the
        // sentinels are absent there.
        let optInEpisode = DiagnosticsEpisodeInput(
            episodeId: Self.rawEpisodeIds[2],
            episodeTitle: "OptIn Episode",
            diagnosticsOptIn: true,
            adBoundaryTimes: [60],
            transcriptChunks: [
                DiagnosticsTranscriptChunk(
                    startTime: 30, endTime: 90,
                    text: "Welcome to the privacy gate test episode for playhead-h3h. Squarespace. Now back to our content."
                )
            ],
            featureSummary: nil
        )
        let coordinator = DiagnosticsExportCoordinator(
            environment: makeEnvironment(),
            presenter: StubPresenter(),
            journalFetch: { [] },
            optInSink: StubOptInSink(),
            optInEpisodes: [optInEpisode]
        )
        let (data, _, _) = try await coordinator.buildAndEncode()
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(DiagnosticsBundleFile.self, from: data)

        // Re-encode the default subtree alone and sweep for sentinels.
        let defaultEncoder = JSONEncoder()
        defaultEncoder.outputFormatting = [.sortedKeys, .prettyPrinted]
        defaultEncoder.dateEncodingStrategy = .iso8601
        let defaultData = try defaultEncoder.encode(decoded.default)
        let defaultString = String(decoding: defaultData, as: UTF8.self)
        for sentinel in Self.transcriptSentinels {
            #expect(
                !defaultString.contains(sentinel),
                "transcript sentinel '\(sentinel)' leaked into default subtree"
            )
        }

        // And the opt-in subtree IS where we expect them — preserves
        // the test as a real assertion (otherwise the inputs could be
        // wrong without anyone noticing).
        let optInData = try defaultEncoder.encode(try #require(decoded.optIn))
        let optInString = String(decoding: optInData, as: UTF8.self)
        #expect(
            optInString.contains("Welcome to the privacy gate test episode for playhead-h3h"),
            "expected sentinel to appear in opt-in subtree (so this test is non-vacuous)"
        )
    }
}
