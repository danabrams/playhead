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

            // Drive the pipeline and write the snapshot.
            do {
                let plan = try await captureSnapshot(
                    for: golden,
                    episodeId: episodeId,
                    audioURL: audioURL,
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

        // 1. Snapshot.
        let snapshot = try await ChapterFeatureSnapshotBuilder.build(
            audioURL: audioURL,
            transcript: [],
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
        let labeler = LiveLabelerAdapter(service: .live())
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
        detector.detect(features: snapshot)
            .map { ChapterBoundaryCandidate(startTime: $0.startTime, endTime: nil) }
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

    func label(
        candidate: ChapterBoundaryCandidate
    ) async throws -> LabelingResult? {
        let labelingCandidate = ChapterLabelingCandidate(
            startTime: candidate.startTime,
            endTime: candidate.endTime,
            regionText: "",
            chapterIndex: 1,
            totalChapters: 1,
            previousDisposition: nil
        )
        return await service.label(labelingCandidate)
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
