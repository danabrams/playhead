// ChapterPlanPipelineSnapshotCaptureTests.swift
// playhead-au2v.1.23: env-gated capture of `ChapterGenerationPhase`
// pipeline-output snapshots for the 12 dogfood episodes.
//
// Purpose:
//   This test runs the production chapter-generation phase end-to-end
//   against each dogfood-corpus audio file and writes the resulting
//   `ChapterPlan` to
//   `PlayheadTests/Fixtures/ChapterPlanGoldenSet/pipeline-snapshot/<episode_id>.json`.
//   The committed snapshots are consumed by
//   `ChapterPlanQualityRealCorpusHarnessTests` to assert real
//   threshold floors (recall ≥ 0.6, precision ≥ 0.5, disposition
//   accuracy ≥ 0.7).
//
// Gating:
//   `PLAYHEAD_CHAPTER_SNAPSHOT_CAPTURE=1` MUST be set in the test
//   process environment for any capture work to run. The default
//   `PlayheadFastTests` plan does NOT set this var, so the capture
//   test is a no-op (via `XCTSkipUnless`) on a vanilla Cmd-U run.
//   Mirrors the `PLAYHEAD_FM_SMOKE` pattern used by
//   `PlayheadFMSmokeTests`; we use a distinct env var so the smoke
//   scheme is not coupled to capture timing.
//
// Audio resolution:
//   Audio files are NOT committed to the repo (see
//   `TestFixtures/Corpus/README.md`). The capture resolves the audio
//   path through `CorpusAnnotationLoader.audioFileURL(for:)`, which
//   looks for `TestFixtures/Corpus/Audio/<episode_id>.<ext>` where
//   `<ext>` is one of `m4a|mp3|mp4|aac|wav|flac`. To run the capture
//   locally:
//     1. Copy episode audio into `TestFixtures/Corpus/Audio/` with
//        filenames matching the annotation `episode_id`.
//     2. Verify each audio file's SHA-256 matches the
//        `audio_fingerprint` recorded in the corresponding
//        `TestFixtures/Corpus/Annotations/<episode_id>.json`.
//     3. Enable Apple Intelligence on the host device (the FM
//        labeler in `ChapterLabelingService.live` is on-device).
//     4. Build and run on a physical iOS 26+ device. Invoke:
//          xcodebuild test \
//            -project Playhead.xcodeproj \
//            -scheme Playhead \
//            -destination 'platform=iOS,id=<UDID>' \
//            -only-testing:'PlayheadTests/ChapterPlanPipelineSnapshotCaptureTests'
//          # plus PLAYHEAD_CHAPTER_SNAPSHOT_CAPTURE=1 in the scheme env vars
//
// Production wiring status (au2v.1.24 update):
//   * Raw-audio → `ChapterFeatureSnapshot` is now exposed via
//     `ChapterFeatureSnapshotBuilder.build(audioURL:transcript:fmAvailable:)`
//     (bead playhead-nbmj). This test drives that helper to produce
//     the snapshot the boundary detector consumes.
//   * Transcript-input gap: `CorpusAnnotation` does not currently carry
//     transcript chunks. We pass an empty transcript here, so the
//     snapshot's lexical/speaker arrays are empty and the captured
//     plan is driven only by music transitions and long-pause
//     signals. The capture is still useful as a smoke proof that the
//     phase wiring composes end-to-end against real audio; a
//     follow-up bead can thread a transcript sidecar (or a live ASR
//     pass) through this entry to enrich the captured plans.
//   * `ChapterGenerationPhase` is constructed in-test with stub
//     admission / creator-chapter / hash providers and an in-memory
//     `ChapterPlanCache`. The boundary detector is a live
//     `ChapterBoundaryDetector` adapter; the labeler is a live
//     `ChapterLabelingService.live` adapter that requires
//     `FoundationModels` (iOS 26+ device, Apple Intelligence on).
//   * The test still gates on `PLAYHEAD_CHAPTER_SNAPSHOT_CAPTURE=1`
//     so a vanilla `PlayheadFastTests` run remains a no-op (matches
//     `PLAYHEAD_FM_SMOKE`).

import Foundation
import XCTest
@testable import Playhead

final class ChapterPlanPipelineSnapshotCaptureTests: XCTestCase {

    /// Gate every test on the capture env var. The default scheme does
    /// not set the var, so the test bodies are a no-op on Cmd-U.
    private static var captureEnabled: Bool {
        ProcessInfo.processInfo.environment["PLAYHEAD_CHAPTER_SNAPSHOT_CAPTURE"] == "1"
    }

    override func setUp() async throws {
        try await super.setUp()
        try XCTSkipUnless(
            Self.captureEnabled,
            """
            ChapterPlan pipeline-snapshot capture is opt-in. Set \
            PLAYHEAD_CHAPTER_SNAPSHOT_CAPTURE=1 in the scheme env vars \
            to run on a real iOS 26+ device with Apple Intelligence \
            enabled and audio files staged under \
            TestFixtures/Corpus/Audio/. See file header for the full \
            invocation recipe.
            """
        )
    }

    // MARK: - Capture entry

    /// Iterate every committed dogfood golden, resolve its audio file,
    /// drive `ChapterGenerationPhase` end-to-end, and write the
    /// resulting `ChapterPlan` to disk under
    /// `PlayheadTests/Fixtures/ChapterPlanGoldenSet/pipeline-snapshot/`.
    ///
    /// The test reports the per-episode capture outcome in failure
    /// output so a partial capture run surfaces exactly which episodes
    /// succeeded vs. which need investigation.
    func testCaptureAllDogfoodSnapshots() async throws {
        let goldens = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        try XCTSkipIf(goldens.isEmpty, "no dogfood goldens — run Scripts/convert_annotations_to_chapter_goldens.py")

        let snapshotDir = ChapterPlanGoldenSetLoader.pipelineSnapshotDirectory()
        try ensureDirectoryExists(snapshotDir)

        // Shared per-run cache directory under `tmp/`. Each captured plan
        // is keyed by its episode content hash, so all 12 episodes can
        // safely share one cache directory without collisions. Sharing
        // also means we create + remove ONE directory per run rather than
        // 12 (one-per-episode was wasteful and never cleaned up).
        let cacheRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "ChapterSnapshotCapture-\(UUID().uuidString)",
                isDirectory: true
            )
        defer {
            // tmp/ is OS-managed but we still tidy up explicitly: capture
            // runs commonly happen on a developer machine where the temp
            // dir lingers across reboots, and leaving stale directories
            // around obscures which run produced what.
            try? FileManager.default.removeItem(at: cacheRoot)
        }

        let corpusLoader = CorpusAnnotationLoader()

        var produced: [String] = []
        var skipped: [(episodeId: String, reason: String)] = []
        var failed: [(episodeId: String, reason: String)] = []

        for (goldenURL, golden) in goldens {
            let episodeId = goldenURL.deletingPathExtension().lastPathComponent

            // Resolve audio. The corpus loader is the single source of
            // truth for the Audio/ convention; threading it through
            // here keeps the resolution rule in one place.
            let annotation: CorpusAnnotation
            do {
                annotation = try corpusLoader.decode(
                    at: corpusLoader.annotationsDirectoryURL
                        .appendingPathComponent("\(episodeId).json", isDirectory: false)
                )
            } catch {
                failed.append((
                    episodeId,
                    "annotation decode failed: \(error.localizedDescription)"
                ))
                continue
            }

            let audioURL: URL
            do {
                audioURL = try corpusLoader.audioFileURL(for: annotation)
            } catch {
                skipped.append((
                    episodeId,
                    """
                    audio file not present under \
                    \(corpusLoader.audioDirectoryURL.path). Stage \
                    `<episode_id>.m4a` (or .mp3, etc.) there to capture this episode.
                    """
                ))
                continue
            }

            // Load the corpus ASR transcript sidecar. Absent → empty
            // (the snapshot will be transcript-starved for that one
            // episode, which the downstream eval surfaces).
            let transcript: [TranscriptChunk]
            do {
                transcript = try CorpusTranscriptLoader.load(
                    episodeId: episodeId,
                    repoRoot: corpusLoader.repoRoot
                )
            } catch {
                failed.append((
                    episodeId,
                    "transcript decode failed: \(error.localizedDescription)"
                ))
                continue
            }

            // Drive the pipeline and write the snapshot.
            do {
                let plan = try await captureSnapshot(
                    for: golden,
                    episodeId: episodeId,
                    audioURL: audioURL,
                    transcript: transcript,
                    cacheRoot: cacheRoot
                )
                let outURL = snapshotDir
                    .appendingPathComponent("\(episodeId).json", isDirectory: false)
                try writeSnapshot(plan: plan, to: outURL)
                produced.append(episodeId)
            } catch let error as CaptureUnavailable {
                // The capture wiring is intentionally incomplete (see
                // file header). Surface the gap with a precise reason
                // so the operator running the capture sees what's
                // missing rather than getting silent no-ops.
                failed.append((episodeId, error.reason))
            } catch {
                failed.append((
                    episodeId,
                    "capture failed: \(error.localizedDescription)"
                ))
            }
        }

        // Final report. We want PARTIAL success to be observable: if
        // 11/12 episodes captured cleanly and 1 failed, the harness
        // can still consume the 11 snapshots and the failure output
        // tells the operator exactly which to re-investigate.
        //
        // We fail-loud in three cases (in this order):
        //   1. Any episode hit a hard `failed` path (annotation decode
        //      error, audio fingerprint mismatch, snapshot-builder
        //      decode failure, phase outcome != .cached, etc.).
        //   2. Zero episodes produced ANY snapshot and zero were
        //      hard-failed. That means the operator set the env var
        //      but every episode landed in `skipped` (audio not staged
        //      under `TestFixtures/Corpus/Audio/`). Silently exiting
        //      green would mislead the operator into thinking the
        //      capture ran when it produced nothing.
        //   3. Anything else is treated as partial-success — print the
        //      summary so the operator can confirm which snapshots
        //      were produced and which need re-investigation.
        // Build the failure tail on its own line(s) so a single failure
        // does not visually fuse with the `failed=N:` header. When there
        // are no failures the tail is the empty string and `failed=0:`
        // sits on its own line.
        let failedTail = failed.isEmpty
            ? ""
            : "\n" + failed.map { "    - \($0.episodeId): \($0.reason)" }.joined(separator: "\n")
        let summary = """
        Snapshot capture summary:
          produced=\(produced.count): \(produced.sorted().joined(separator: ", "))
          skipped (audio missing)=\(skipped.count): \(skipped.map(\.episodeId).sorted().joined(separator: ", "))
          failed=\(failed.count):\(failedTail)
        """
        if !failed.isEmpty {
            XCTFail(summary)
        } else if produced.isEmpty {
            XCTFail(
                """
                PLAYHEAD_CHAPTER_SNAPSHOT_CAPTURE=1 was set but no \
                snapshots were produced. Every dogfood episode landed \
                in `skipped` because its audio file is not staged \
                under TestFixtures/Corpus/Audio/. See the file header \
                for the staging recipe.

                \(summary)
                """
            )
        } else {
            // Print to test output even on success so the operator
            // can confirm which snapshots were produced.
            print(summary)
        }
    }

    // MARK: - Snapshot wire-up

    /// Run the chapter-generation phase end-to-end against one
    /// episode's audio and return its `ChapterPlan`.
    ///
    /// Steps:
    ///   1. Build a populated `ChapterFeatureSnapshot` from the raw
    ///      audio via `ChapterFeatureSnapshotBuilder.build(...)`
    ///      (bead playhead-nbmj / au2v.1.24). Transcript chunks are
    ///      not currently available in `CorpusAnnotation`, so we pass
    ///      `[]`; lexical / speaker arrays in the snapshot are empty,
    ///      music / pause arrays are populated by feature extraction.
    ///   2. Construct `ChapterGenerationPhase` with the snapshot
    ///      wrapped in a live `ChapterBoundaryDetector` adapter, the
    ///      live `ChapterLabelingService` (iOS 26+ + Apple
    ///      Intelligence required), stub admission / creator-chapter /
    ///      hash providers, and an in-memory `ChapterPlanCache`.
    ///   3. Run the phase and read the plan from the cache. The phase
    ///      uses the golden's `episodeContentHash` as the cache key
    ///      (the same hash the `ChapterPlanQualityRealCorpusHarnessTests`
    ///      consume).
    private func captureSnapshot(
        for golden: GoldenChapterSet,
        episodeId: String,
        audioURL: URL,
        transcript: [TranscriptChunk],
        cacheRoot: URL
    ) async throws -> ChapterPlan {
        // Defense in depth: confirm the audio exists and is readable
        // before doing any decode work. A missing audio file should
        // already have been caught by the `audioFileURL(for:)`
        // resolver, but a half-staged copy could slip through.
        guard FileManager.default.isReadableFile(atPath: audioURL.path) else {
            throw CaptureUnavailable(
                reason: "audio at \(audioURL.path) is not readable"
            )
        }
        // Sanity-check the golden's content hash matches the audio
        // file's fingerprint. The recorded fingerprint is
        // `sha256:<hex>` (see CorpusAnnotationLoader); the golden's
        // `episodeContentHash` is the hex suffix.
        let fingerprint = try CorpusAudioFingerprint.fingerprint(of: audioURL)
        let expectedFingerprint = "\(CorpusAudioFingerprint.prefix)\(golden.episodeContentHash)"
        guard fingerprint == expectedFingerprint else {
            throw CaptureUnavailable(
                reason: """
                audio fingerprint \(fingerprint) does not match golden \
                contentHash \(expectedFingerprint). The audio file at \
                \(audioURL.path) appears to be a different cut than the \
                annotation references.
                """
            )
        }

        // Live FM labeler requires iOS 26+. The capture is gated by
        // env var to a real device build, so this is the right place
        // to fail loud if we're somehow running on an older SDK.
        guard #available(iOS 26.0, *) else {
            throw CaptureUnavailable(
                reason: """
                ChapterLabelingService.live requires iOS 26.0+ \
                (FoundationModels). Current runtime is older; rerun on \
                a supported device.
                """
            )
        }

        // 1. Snapshot. The transcript populates lexical hits + speaker
        //    windows (speaker is empty for whisper — no diarization), so
        //    the boundary detector sees lexical-category signals in
        //    addition to music + pause.
        let snapshot = try await ChapterFeatureSnapshotBuilder.build(
            audioURL: audioURL,
            transcript: transcript,
            fmAvailable: true
        )

        // 2. Phase wiring. Everything below is a small, in-test
        //    composition mirroring the doubles used by
        //    `ChapterGenerationPhaseIntegrationTests` — admission,
        //    creator-chapter, hash, event-sink doubles — paired with
        //    the LIVE boundary detector + labeler adapters.
        //
        //    Cache uses the per-run `cacheRoot` (one directory shared
        //    across all 12 episodes in this run). Cache keys are
        //    content-hashed so there is no key collision between
        //    episodes; sharing keeps the on-disk footprint to one
        //    directory per capture run with `defer`-driven cleanup at
        //    the outer entry.
        let cache = ChapterPlanCache(directory: cacheRoot)

        let boundaryDetector = LiveBoundaryDetector(snapshot: snapshot)
        let labeler = LiveLabelerAdapter(service: .live(), transcript: transcript)
        let admissionPolicy = StubAdmissionPolicy()
        let creatorProvider = StubCreatorChapterProvider()
        let hashProvider = StickyHashProvider(hash: golden.episodeContentHash)
        let eventSink = NoopEventSink()

        let phase = ChapterGenerationPhase(
            admissionPolicy: admissionPolicy,
            creatorChapterProvider: creatorProvider,
            boundaryDetector: boundaryDetector,
            labeler: labeler,
            transcriptHashProvider: hashProvider,
            cache: cache,
            eventSink: eventSink
        )

        // 3. Run + read plan.
        let outcome = await phase.run(
            mode: .enabled,
            episodeId: episodeId,
            installID: UUID()
        )

        switch outcome {
        case .cached:
            // Drop through to the cache read.
            break
        case .modeOff,
             .admissionDenied,
             .skippedCreatorChapters,
             .noCandidates,
             .transcriptUnavailable,
             .raceAborted,
             .preempted,
             .operationalRateExceeded:
            throw CaptureUnavailable(
                reason: """
                ChapterGenerationPhase did not produce a cached plan \
                for episode '\(episodeId)'. Outcome: \(outcome). The \
                snapshot's empty transcript may have starved the \
                detector of lexical/speaker signals; this is expected \
                for episodes whose music+pause cues alone are too \
                sparse to clear the boundary-confidence gate. Stage a \
                transcript sidecar or land a transcript-loading bead \
                to enrich the capture for this episode.
                """
            )
        }

        guard let plan = await cache.get(contentHash: golden.episodeContentHash) else {
            throw CaptureUnavailable(
                reason: """
                ChapterGenerationPhase reported `.cached` but \
                ChapterPlanCache.get(contentHash:) returned nil for \
                episode '\(episodeId)'. This indicates a cache-write \
                regression — the phase claimed a successful write but \
                the entry is not retrievable.
                """
            )
        }
        return plan
    }

    // MARK: - On-disk write

    /// Write a captured `ChapterPlan` to JSON. Date encoding matches
    /// `ChapterPlanCache`'s default (so the loader's
    /// `loadDogfoodPipelineSnapshot` round-trip uses the same
    /// format).
    private func writeSnapshot(plan: ChapterPlan, to url: URL) throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        let data = try encoder.encode(plan)
        try data.write(to: url, options: .atomic)
    }

    private func ensureDirectoryExists(_ url: URL) throws {
        let fm = FileManager.default
        if !fm.fileExists(atPath: url.path) {
            try fm.createDirectory(at: url, withIntermediateDirectories: true)
        }
    }
}

// MARK: - Labeling diagnostic (au2v.1.24)

/// Env-gated diagnostic (`PLAYHEAD_CHAPTER_LABEL_DIAGNOSE=1`) that runs
/// the boundary detector + live FM labeler **directly** (no phase, no
/// assembler) across the dogfood corpus and records, per candidate
/// region: the FM raw disposition (the rich 7-way taxonomy, before it
/// is mapped down to adBreak/content/ambiguous), the failure mode
/// (success / semantic / operational), a sample of the region text, and
/// whether the region overlaps a golden `adBreak` span.
///
/// Purpose (playhead-au2v.1.24): the captured plans never emit
/// `adBreak`. Three hypotheses, each pointing at a different fix layer:
///   A. **Boundary misalignment** — ads are not inside any detected
///      region (fix lives in `ChapterBoundaryDetector`).
///   B. **FM misread** — ad regions are labeled `content` on a
///      successful call (fix lives in the prompt / labeler).
///   C. **Guardrail refusal** — ad regions fail operationally (the FM
///      safety guardrails refuse the ad/topic text).
/// The per-candidate cross-tab of {golden-ad-overlap} × {disposition,
/// failureMode} distinguishes A/B/C directly.
///
/// Output: `playhead-dogfood-diagnostics-chapter-label-<episode_id>.json`
/// at the repo root. That glob is git-ignored — the region-text samples
/// contain raw transcript (possibly advertiser names verbatim), so the
/// dump is LOCAL-ONLY and must never be committed (mirrors the
/// au2v.1.22 corpus privacy rule for committed fixtures).
///
/// Runs on Mac Catalyst exactly like the capture test (FM is live).
final class ChapterLabelingDiagnosticTests: XCTestCase {

    private static var diagnoseEnabled: Bool {
        ProcessInfo.processInfo.environment["PLAYHEAD_CHAPTER_LABEL_DIAGNOSE"] == "1"
    }

    override func setUp() async throws {
        try await super.setUp()
        try XCTSkipUnless(
            Self.diagnoseEnabled,
            """
            Chapter-label diagnosis is opt-in. Set \
            PLAYHEAD_CHAPTER_LABEL_DIAGNOSE=1 in the test plan env vars \
            and run on Mac Catalyst (or an iOS 26 device) with Apple \
            Intelligence enabled and corpus audio + transcripts staged.
            """
        )
    }

    /// One row per detected candidate region.
    private struct CandidateDiagnostic: Encodable {
        let startTime: Double
        let endTime: Double?
        let regionTextLength: Int
        let regionTextSample: String
        /// Raw FM taxonomy (`hostReadAd`, `programmaticAd`, `content`,
        /// …) or `"nil-result"` when the labeler returned nil.
        let rawDisposition: String
        /// `success` / `semantic` / `operational`.
        let outcome: String
        /// Golden disposition of the span containing `startTime`.
        let goldenDispositionAtStart: String?
        /// True when `[startTime, endTime)` intersects any golden
        /// `adBreak` span.
        let overlapsGoldenAdBreak: Bool
    }

    private struct EpisodeDiagnostic: Encodable {
        let episodeId: String
        let episodeDuration: TimeInterval
        let candidateCount: Int
        let goldenAdBreakSpanCount: Int
        /// Golden adBreak spans that had ≥1 overlapping candidate.
        let goldenAdBreakSpansCovered: Int
        let candidates: [CandidateDiagnostic]
    }

    func testDiagnoseLabelingAcrossDogfoodCorpus() async throws {
        guard #available(iOS 26.0, *) else {
            throw XCTSkip("ChapterLabelingService.live requires iOS 26+ / FoundationModels.")
        }

        let goldens = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        try XCTSkipIf(goldens.isEmpty, "no dogfood goldens to diagnose")

        let corpusLoader = CorpusAnnotationLoader()
        let labelService = ChapterLabelingService.live()

        // Aggregate cross-tab: among candidates overlapping a golden
        // adBreak span, how many landed in each outcome bucket? This is
        // the headline result.
        var adOverlapSuccess = 0
        var adOverlapSemantic = 0
        var adOverlapOperational = 0
        var adOverlapLabeledAdBreak = 0
        var totalGoldenAdSpans = 0
        var totalGoldenAdSpansCovered = 0
        var episodesDiagnosed: [String] = []

        for (goldenURL, golden) in goldens {
            let episodeId = goldenURL.deletingPathExtension().lastPathComponent

            // Resolve audio + transcript (soft-skip when unavailable).
            let annotation: CorpusAnnotation
            do {
                annotation = try corpusLoader.decode(
                    at: corpusLoader.annotationsDirectoryURL
                        .appendingPathComponent("\(episodeId).json", isDirectory: false)
                )
            } catch { continue }
            guard let audioURL = try? corpusLoader.audioFileURL(for: annotation) else { continue }
            let transcript = (try? CorpusTranscriptLoader.load(
                episodeId: episodeId,
                repoRoot: corpusLoader.repoRoot
            )) ?? []

            let snapshot = try await ChapterFeatureSnapshotBuilder.build(
                audioURL: audioURL,
                transcript: transcript,
                fmAvailable: true
            )
            let detector = LiveBoundaryDetector(snapshot: snapshot)
            let candidates = try await detector.detect()
            let labeler = LiveLabelerAdapter(service: labelService, transcript: transcript)

            // Golden adBreak spans: [chapter.start, nextChapter.start),
            // last running to episode end.
            let adSpans = Self.goldenAdBreakSpans(
                golden: golden,
                episodeDuration: snapshot.episodeDuration
            )
            totalGoldenAdSpans += adSpans.count
            var coveredSpanIndices: Set<Int> = []

            var rows: [CandidateDiagnostic] = []
            for candidate in candidates {
                let result = try await labeler.label(candidate: candidate)
                let regionText = LiveLabelerAdapter.regionText(
                    transcript: transcript,
                    start: candidate.startTime,
                    end: candidate.endTime
                )
                let overlaps = adSpans.enumerated().contains { index, span in
                    let hit = Self.overlaps(
                        start: candidate.startTime,
                        end: candidate.endTime ?? snapshot.episodeDuration,
                        spanStart: span.start,
                        spanEnd: span.end
                    )
                    if hit { coveredSpanIndices.insert(index) }
                    return hit
                }

                let rawDisposition = result?.labelDisposition.rawValue ?? "nil-result"
                let outcome = Self.outcomeString(result)

                if overlaps {
                    switch outcome {
                    case "success": adOverlapSuccess += 1
                    case "semantic": adOverlapSemantic += 1
                    case "operational": adOverlapOperational += 1
                    default: break
                    }
                    if result?.labelDisposition.mappedDisposition == .adBreak {
                        adOverlapLabeledAdBreak += 1
                    }
                }

                rows.append(CandidateDiagnostic(
                    startTime: candidate.startTime,
                    endTime: candidate.endTime,
                    regionTextLength: regionText.count,
                    regionTextSample: String(regionText.prefix(300)),
                    rawDisposition: rawDisposition,
                    outcome: outcome,
                    goldenDispositionAtStart: Self.goldenDisposition(
                        at: candidate.startTime,
                        golden: golden,
                        episodeDuration: snapshot.episodeDuration
                    ),
                    overlapsGoldenAdBreak: overlaps
                ))
            }

            totalGoldenAdSpansCovered += coveredSpanIndices.count

            let episodeDiag = EpisodeDiagnostic(
                episodeId: episodeId,
                episodeDuration: snapshot.episodeDuration,
                candidateCount: candidates.count,
                goldenAdBreakSpanCount: adSpans.count,
                goldenAdBreakSpansCovered: coveredSpanIndices.count,
                candidates: rows
            )

            let outURL = corpusLoader.repoRoot.appendingPathComponent(
                "playhead-dogfood-diagnostics-chapter-label-\(episodeId).json",
                isDirectory: false
            )
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
            try encoder.encode(episodeDiag).write(to: outURL, options: .atomic)
            episodesDiagnosed.append(episodeId)
        }

        // Headline cross-tab. This is what answers A/B/C.
        let adOverlapTotal = adOverlapSuccess + adOverlapSemantic + adOverlapOperational
        print(
            """
            === Chapter-label diagnosis ===
            episodes diagnosed: \(episodesDiagnosed.count) [\(episodesDiagnosed.sorted().joined(separator: ", "))]
            golden adBreak spans: \(totalGoldenAdSpans), covered by ≥1 candidate: \(totalGoldenAdSpansCovered) \
            (\(totalGoldenAdSpans == 0 ? "n/a" : String(format: "%.0f%%", 100 * Double(totalGoldenAdSpansCovered) / Double(totalGoldenAdSpans))))
              → if coverage is low, hypothesis A (boundary misalignment).
            candidates overlapping a golden adBreak span: \(adOverlapTotal)
              success: \(adOverlapSuccess)  (of those, mapped→adBreak: \(adOverlapLabeledAdBreak))
                → if success is high but adBreak is ~0, hypothesis B (FM misread).
              semantic-unclear: \(adOverlapSemantic)
              operational: \(adOverlapOperational)
                → if operational is high, hypothesis C (guardrail refusal).
            Per-candidate detail: playhead-dogfood-diagnostics-chapter-label-<episode_id>.json (git-ignored)
            """
        )
    }

    // MARK: - Golden span helpers

    private struct GoldenSpan { let start: Double; let end: Double }

    /// Build `[start, nextStart)` spans for every golden chapter whose
    /// `expectedDisposition` is `.adBreak`. The last chapter runs to
    /// `episodeDuration`.
    private static func goldenAdBreakSpans(
        golden: GoldenChapterSet,
        episodeDuration: TimeInterval
    ) -> [GoldenSpan] {
        let sorted = golden.chapters.sorted { $0.startTimeSeconds < $1.startTimeSeconds }
        var spans: [GoldenSpan] = []
        for (index, chapter) in sorted.enumerated() {
            guard chapter.expectedDisposition == .adBreak else { continue }
            let end = index + 1 < sorted.count
                ? sorted[index + 1].startTimeSeconds
                : episodeDuration
            spans.append(GoldenSpan(start: chapter.startTimeSeconds, end: end))
        }
        return spans
    }

    /// The `expectedDisposition` of the golden span containing `time`.
    private static func goldenDisposition(
        at time: Double,
        golden: GoldenChapterSet,
        episodeDuration: TimeInterval
    ) -> String? {
        let sorted = golden.chapters.sorted { $0.startTimeSeconds < $1.startTimeSeconds }
        for (index, chapter) in sorted.enumerated() {
            let end = index + 1 < sorted.count
                ? sorted[index + 1].startTimeSeconds
                : episodeDuration
            if time >= chapter.startTimeSeconds && time < end {
                return String(describing: chapter.expectedDisposition)
            }
        }
        return nil
    }

    private static func overlaps(
        start: Double, end: Double,
        spanStart: Double, spanEnd: Double
    ) -> Bool {
        start < spanEnd && spanStart < end
    }

    @available(iOS 26.0, *)
    private static func outcomeString(_ result: LabelingResult?) -> String {
        guard let result else { return "nil-result" }
        switch result.failureMode {
        case .none: return "success"
        case .some(.semantic): return "semantic"
        case .some(.operational): return "operational"
        }
    }
}

// MARK: - Corpus transcript loading (au2v.1.25)

/// Decodes a whisper.cpp transcript JSON
/// (`TestFixtures/Corpus/Transcripts/<episode_id>.json`) into the
/// `[TranscriptChunk]` shape the chapter pipeline consumes. The corpus
/// transcripts are the local ASR sidecar referenced by
/// `TestFixtures/Corpus/README.md` (git-ignored, large). Threading them
/// through the capture replaces the earlier empty-transcript shortcut
/// that starved the boundary detector (no lexical/speaker signals) and
/// the FM labeler (empty `regionText` → every chapter labeled
/// `content`).
enum CorpusTranscriptLoader {

    /// whisper.cpp `--output-json-full` envelope. We only need the
    /// `transcription` array; each segment carries millisecond
    /// `offsets` and the recognized `text`. Token-level detail is
    /// ignored — the chapter pipeline scans segment text, not tokens.
    private struct WhisperTranscript: Decodable {
        let transcription: [Segment]

        struct Segment: Decodable {
            let text: String
            let offsets: Offsets

            struct Offsets: Decodable {
                let from: Int // milliseconds
                let to: Int   // milliseconds
            }
        }
    }

    /// Load + map the corpus transcript for `episodeId`. Returns `[]`
    /// when the sidecar is absent so an operator capturing a subset of
    /// episodes still gets a (transcript-starved) snapshot rather than a
    /// hard failure — the capture summary already distinguishes starved
    /// from rich plans via the downstream eval.
    static func load(episodeId: String, repoRoot: URL) throws -> [TranscriptChunk] {
        let url = repoRoot
            .appendingPathComponent("TestFixtures/Corpus/Transcripts", isDirectory: true)
            .appendingPathComponent("\(episodeId).json", isDirectory: false)
        guard FileManager.default.fileExists(atPath: url.path) else { return [] }

        let data = try Data(contentsOf: url)
        let whisper = try JSONDecoder().decode(WhisperTranscript.self, from: data)

        return whisper.transcription.enumerated().map { index, segment in
            // `normalizedText` uses the same normalizer the production
            // ingest path applies (AdDetectionService /
            // TargetedWindowNarrower both call
            // `TranscriptEngineService.normalizeText`), so the lexical
            // hits the boundary detector sees match production behavior.
            // whisper has no diarization, so `speakerId` is nil — that
            // leaves speaker-shift signals empty (same as production for
            // a show without speaker labels), and the captured plan is
            // driven by music + pause + lexical signals.
            TranscriptChunk(
                id: "\(episodeId)-\(index)",
                analysisAssetId: episodeId,
                segmentFingerprint: "",
                chunkIndex: index,
                startTime: Double(segment.offsets.from) / 1000.0,
                endTime: Double(segment.offsets.to) / 1000.0,
                text: segment.text,
                normalizedText: TranscriptEngineService.normalizeText(segment.text),
                pass: "final",
                modelVersion: "whisper-corpus",
                transcriptVersion: nil,
                atomOrdinal: index,
                weakAnchorMetadata: nil,
                speakerId: nil,
                avgConfidence: nil
            )
        }
    }
}

// MARK: - Capture-unavailable error

/// Raised when the capture cannot complete for the requested
/// episode. Carries a one-line operator-facing reason that flows into
/// the test's failure summary. NOT an XCTest assertion failure on its
/// own — the outer loop accumulates these and reports them together so
/// a partial capture run produces one actionable summary rather than
/// N individual failures.
///
/// Cases this surfaces (post au2v.1.24):
///   * Audio file present but unreadable.
///   * Audio fingerprint does not match the golden's contentHash.
///   * iOS runtime is older than 26.0 (FoundationModels missing).
///   * `ChapterGenerationPhase` returned a non-`.cached` outcome —
///     typically because the empty-transcript snapshot starved the
///     detector of signals (see the `captureSnapshot` reason text for
///     details).
///   * Phase reported `.cached` but the cache read returned `nil`
///     (cache-write regression).
private struct CaptureUnavailable: Error {
    let reason: String
}

// MARK: - In-test adapters and stubs (au2v.1.24)

/// Boundary-detection adapter for the capture test. Wraps the LIVE
/// `ChapterBoundaryDetector` (pure function over a snapshot) so the
/// phase can call `detect() async throws -> [ChapterBoundaryCandidate]`
/// without us pre-computing candidates in the test body.
///
/// The mapping `ChapterCandidate → ChapterBoundaryCandidate` is
/// intentionally lossy: the phase only consumes `startTime` and an
/// optional `endTime`. The detector emits `startTime` per boundary;
/// `endTime` is left `nil` so the labeler treats each candidate as
/// "open-ended until the next boundary or episode end" (matches the
/// detector's contract — boundaries describe transitions, not regions).
private struct LiveBoundaryDetector: ChapterBoundaryDetecting {
    let snapshot: ChapterFeatureSnapshot
    let detector: ChapterBoundaryDetector

    init(snapshot: ChapterFeatureSnapshot) {
        self.snapshot = snapshot
        self.detector = ChapterBoundaryDetector()
    }

    func detect() async throws -> [ChapterBoundaryCandidate] {
        // Each detector boundary describes a transition. To give the
        // labeler a region to read, we close each candidate at the next
        // boundary's start (the last candidate runs to episode end).
        // This mirrors how production regions span [thisStart, nextStart)
        // and lets `LiveLabelerAdapter` slice transcript text per region
        // instead of passing an empty `regionText`.
        let starts = detector.detect(features: snapshot)
            .map(\.startTime)
            .sorted()
        return starts.enumerated().map { index, start in
            let end = index + 1 < starts.count
                ? starts[index + 1]
                : snapshot.episodeDuration
            return ChapterBoundaryCandidate(startTime: start, endTime: end)
        }
    }
}

/// Adapter wrapping the live `ChapterLabelingService` (FM-backed) as a
/// `ChapterLabeling` conformance. The capture has no transcript on
/// hand, so `regionText` is empty — the FM still produces a label, but
/// the disposition tends toward operational-unclear. That is exactly
/// what the capture should record so a downstream eval can see the
/// effect of running with no transcript context.
///
/// `chapterIndex` and `totalChapters` are filled with `1` and `1`
/// respectively because the phase calls the labeler per-candidate and
/// the test has no plan-level context to thread through. The labeling
/// prompt uses these for context only; they do not affect the output
/// schema.
@available(iOS 26.0, *)
private struct LiveLabelerAdapter: ChapterLabeling {
    let service: ChapterLabelingService
    let transcript: [TranscriptChunk]

    func label(
        candidate: ChapterBoundaryCandidate
    ) async throws -> LabelingResult? {
        let labelingCandidate = ChapterLabelingCandidate(
            startTime: candidate.startTime,
            endTime: candidate.endTime,
            regionText: Self.regionText(
                transcript: transcript,
                start: candidate.startTime,
                end: candidate.endTime
            ),
            chapterIndex: 1,
            totalChapters: 1,
            previousDisposition: nil
        )
        return await service.label(labelingCandidate)
    }

    /// Join the text of every transcript chunk whose start falls in
    /// `[start, end)` (open-ended when `end` is nil). Mirrors
    /// production's `RegionFeatureExtractor`, which joins region-atom
    /// text with a single space; `ChapterLabelingService` truncates to
    /// `regionTextCharacterCap` internally, so very long regions are
    /// capped the same way the production labeler caps them.
    static func regionText(
        transcript: [TranscriptChunk],
        start: TimeInterval,
        end: TimeInterval?
    ) -> String {
        let upper = end ?? .greatestFiniteMagnitude
        return transcript
            .filter { $0.startTime >= start && $0.startTime < upper }
            .sorted { $0.startTime < $1.startTime }
            .map(\.text)
            .joined(separator: " ")
    }
}

/// Admission stub that always admits — the capture is opt-in via env
/// var, so the operator has already accepted the cost.
private struct StubAdmissionPolicy: ChapterPhaseAdmissionPolicy {
    func decide() async -> ChapterPhaseAdmissionDecision { .admit }
}

/// Creator-chapter stub that returns no creator chapters — we want the
/// FM path to run so the captured plan exercises the inference
/// pipeline.
private struct StubCreatorChapterProvider: CreatorChapterProviding {
    func creatorChapters(episodeId: String) async -> [ChapterEvidence] { [] }
}

/// Hash stub that returns the same hash on entry and recheck so the
/// race-abort path does not fire.
private struct StickyHashProvider: TranscriptHashProviding {
    let hash: String
    func currentTranscriptHash() async -> String? { hash }
}

/// Event sink that drops everything — the capture test does not need
/// to assert phase events, only the produced plan.
private struct NoopEventSink: ChapterPhaseEventSink {
    func record(_ event: ChapterPhaseEvent) async {}
}
