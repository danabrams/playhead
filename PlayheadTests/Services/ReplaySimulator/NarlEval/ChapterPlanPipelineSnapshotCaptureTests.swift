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
// Production wiring status (au2v.1.23 LIMITATION — surfaced for review):
//   `ChapterGenerationPhase` end-to-end requires a populated
//   `ChapterFeatureSnapshot` (music windows + speaker windows +
//   lexical hits + pause windows) that drives the boundary detector.
//   In production, that snapshot is sourced from the feature-extraction
//   pipeline (`FeatureExtraction.swift`) running against the audio
//   asset; the chapter-phase factory in `AdDetectionService` is not
//   yet wired to a live `ChapterBoundaryDetecting` implementation
//   that consumes raw audio (`chapterGenerationPhaseFactory: nil` is
//   the default and no production constructor exists today).
//
//   Until that wiring lands, this capture test cannot produce real
//   snapshots from raw audio in a self-contained step. The follow-up
//   bead `playhead-nbmj` (logical name au2v.1.24) will expose a
//   raw-audio → `ChapterFeatureSnapshot` helper so this test can drop
//   in the missing piece without surprises; the audio + golden
//   plumbing here is already in place.
//
//   When invoked in this state with the env var set, the test fails
//   with a clear diagnostic naming the missing wiring AND the
//   follow-up bead (`playhead-nbmj` / au2v.1.24) rather than
//   producing an empty / synthetic snapshot.

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

            // Drive the pipeline and write the snapshot.
            do {
                let plan = try await captureSnapshot(
                    for: golden,
                    episodeId: episodeId,
                    audioURL: audioURL
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
        //   1. Any episode hit a hard `failed` path (capture wiring
        //      missing, annotation decode error, etc.). Until follow-up
        //      bead `playhead-nbmj` (au2v.1.24) lands the raw-audio →
        //      ChapterFeatureSnapshot helper, every audio-present
        //      episode will hit `CaptureUnavailable` here; this is the
        //      "fails loud" contract the spec requires.
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
                for the staging recipe. (Follow-up bead `playhead-nbmj` \
                / au2v.1.24 will land the raw-audio → \
                ChapterFeatureSnapshot helper required to actually \
                capture; until then this path is unreachable even \
                with audio staged.)

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

    /// Run the production chapter-generation phase against one
    /// episode's audio and return its `ChapterPlan`.
    ///
    /// LIMITATION (au2v.1.23): Building the `ChapterFeatureSnapshot`
    /// the boundary detector consumes requires the feature-extraction
    /// pipeline to have run against this audio file and persisted its
    /// outputs into the analysis store. That wiring is not yet
    /// available in a callable form outside `AdDetectionService.runBackfill`
    /// (which itself takes a fully-populated `AnalysisAsset` row and
    /// upstream service graph). Follow-up bead `playhead-nbmj`
    /// (au2v.1.24) exposes a "build ChapterFeatureSnapshot for this
    /// audio URL" helper; until that lands, this function throws
    /// `CaptureUnavailable` with a precise reason and the operator
    /// follow-up steps.
    private func captureSnapshot(
        for golden: GoldenChapterSet,
        episodeId: String,
        audioURL: URL
    ) async throws -> ChapterPlan {
        // Defense in depth: confirm the audio exists and is readable
        // before reporting "wiring not available". A missing audio
        // file should already have been caught by the
        // `audioFileURL(for:)` resolver, but a half-staged copy could
        // slip through.
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

        // ── MISSING WIRING ─────────────────────────────────────────
        // To produce a real `ChapterPlan` we need to:
        //   1. Run the upstream feature-extraction pipeline against
        //      this audio file (music probability windows, speaker
        //      clustering, lexical hits, pause windows) into a
        //      populated `ChapterFeatureSnapshot`.
        //   2. Construct `ChapterGenerationPhase` with:
        //        - `boundaryDetector`: a `ChapterBoundaryDetector`
        //          (live; pure function over the snapshot).
        //        - `labeler`: `ChapterLabelingService.live(...)`
        //          (FM-backed, on-device — requires iOS 26+ +
        //          Apple Intelligence).
        //        - `creatorChapterProvider`: a stub that returns
        //          empty so the FM path actually runs.
        //        - `transcriptHashProvider`: a stub that returns
        //          the golden's `episodeContentHash` (entry == recheck
        //          so no race fires).
        //        - `cache`: an in-memory `ChapterPlanCache` to capture
        //          the plan post-assembly.
        //        - admission policy: stub `.admit`.
        //   3. Call `phase.run(mode: .enabled, episodeId:, installID:)`
        //      and read the resulting plan from the cache via
        //      `cache.get(contentHash:)`.
        //
        // Step 1 is the blocker. No production constructor presently
        // takes raw audio → `ChapterFeatureSnapshot`. The closest
        // surface is `AdDetectionService.runBackfill`, which expects
        // a populated `AnalysisAsset` row and upstream services that
        // are not safe to invoke from a test-only capture entry
        // point.
        //
        // This stub fails loud so the follow-up bead
        // `playhead-nbmj` (au2v.1.24), which lands the raw-audio →
        // feature-snapshot helper, can drop the implementation in
        // here without surprises.
        throw CaptureUnavailable(
            reason: """
            ChapterFeatureSnapshot construction from raw audio is not \
            yet exposed in a callable form for episode '\(episodeId)'. \
            Follow-up bead `playhead-nbmj` (au2v.1.24) will expose the \
            raw-audio → ChapterFeatureSnapshot helper this capture \
            depends on; see ChapterPlanPipelineSnapshotCaptureTests \
            file header for the production-wiring gap. Audio at \
            \(audioURL.path) is present and matches the golden \
            fingerprint; the test infrastructure is otherwise ready \
            to capture.
            """
        )
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

// MARK: - Capture-unavailable error

/// Raised when the capture wiring is incomplete for the requested
/// episode. Carries a one-line operator-facing reason that flows into
/// the test's failure summary. NOT an XCTest assertion failure on its
/// own — the outer loop accumulates these and reports them together so
/// a partial capture run produces one actionable summary rather than
/// N individual failures.
private struct CaptureUnavailable: Error {
    let reason: String
}
