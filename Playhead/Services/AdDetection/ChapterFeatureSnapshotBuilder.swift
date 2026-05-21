// ChapterFeatureSnapshotBuilder.swift
// playhead-au2v.1.24 (playhead-nbmj): Raw-audio → ChapterFeatureSnapshot
// helper. Unblocks the dogfood pipeline-snapshot capture test
// (`ChapterPlanPipelineSnapshotCaptureTests`) and exposes a single
// callable form for any downstream wiring that needs a populated
// snapshot without spinning up the full upstream service graph.
//
// Design notes (au2v.1.24):
//
//   * Shared-construction-code contract. The bead spec requires that
//     the feature-window construction subgraph used by
//     `AdDetectionService.runBackfill` and the new helper share the
//     same underlying code, NOT a parallel implementation. The shared
//     element is `FeatureExtractionService.extractWindows(...)` —
//     called from `extractAndPersist(...)` on the production hot path
//     AND from the new `FeatureExtractionService.extract(shards:analysisAssetId:)`
//     batch entry that this helper drives. Same DSP, same per-shard
//     state carry, same prior-window smoothing fix-up. The only
//     difference is the persistence side-channel (production writes to
//     SQLite, the helper keeps results in memory). Production
//     `runBackfill` continues to consume feature windows via
//     `store.fetchFeatureWindows(...)` after persistence — its
//     observable behavior is unchanged by this bead.
//
//   * `[FeatureWindow] → ChapterFeatureSnapshot` mapping is also
//     exposed via `snapshot(from:transcript:episodeDuration:)` so a
//     follow-up bead that wires the chapter-generation phase into
//     `runBackfill` can call the SAME projection without re-deriving
//     it. The boundary detector consumes a `ChapterFeatureSnapshot`,
//     not a `[FeatureWindow]`, so this mapping is the seam between the
//     extraction pipeline and the boundary detector.
//
//   * `fmAvailable` parameter. Accepted on the top-level `build(...)`
//     entry for forward-compatibility with the spec signature. The
//     snapshot's four input arrays (music / speaker / lexical / pause)
//     are derived from acoustic feature extraction, transcript
//     chunks, and the lexical scanner — none of which depend on
//     Foundation Models in this bead. The flag is currently
//     unused; a future bead may thread it through to gate an
//     FM-augmented lexical pass without changing the helper's
//     signature.
//
//   * On-device only. The helper drives `AnalysisAudioService`
//     (AVFoundation decode) and `FeatureExtractionService` (Accelerate
//     DSP + optional `SoundAnalysis` music classifier) — every step
//     runs locally. No network I/O. Matches the project-wide on-device
//     mandate.
//
//   * Non-persisting. The helper writes nothing to SQLite. Callers
//     that need persistence go through the production
//     `extractAndPersist` path; callers that only need a one-shot
//     snapshot (the capture test, future eval tooling) call
//     `build(...)` and discard the result when done.

import Foundation

// MARK: - ChapterFeatureSnapshotBuilder

/// Build a populated `ChapterFeatureSnapshot` from raw audio plus a
/// transcript. Designed for offline / eval workflows that need the
/// snapshot in isolation (no `AdDetectionService` actor, no
/// `AnalysisStore` row).
///
/// Production hot-path code does NOT call this helper — it gets feature
/// windows via the persisted `AnalysisStore` and (when wired) will
/// project them through `snapshot(from:transcript:episodeDuration:)`
/// instead. Both paths share the same underlying DSP (see file header).
enum ChapterFeatureSnapshotBuilder {

    /// Errors the builder surfaces. Kept narrow — the helper is a thin
    /// driver around existing services and most failures propagate
    /// from those.
    enum BuildError: Error, CustomStringConvertible {
        /// `audioURL` was not a `file://` URL — the analysis audio
        /// service refuses non-local URLs at the type system (see
        /// `LocalAudioURL`); we surface a typed error here so callers
        /// can distinguish "you passed a remote URL" from a generic
        /// decode failure.
        case audioURLNotLocal(URL)

        var description: String {
            switch self {
            case .audioURLNotLocal(let url):
                return "ChapterFeatureSnapshotBuilder: audioURL must be a file URL (got \(url.absoluteString))"
            }
        }
    }

    /// Decode `audioURL`, run feature extraction, and project the
    /// result into a `ChapterFeatureSnapshot` along with lexical hits
    /// derived from `transcript`.
    ///
    /// - Parameters:
    ///   - audioURL: Local audio file (mp3 / m4a / caf / wav etc.).
    ///     Must be a `file://` URL.
    ///   - transcript: Time-ordered transcript chunks for the same
    ///     audio. Used both as the lexical-hit source and (via
    ///     `speakerId`) as the speaker-cluster source for the
    ///     snapshot's `speakerWindows`. Empty is valid — yields a
    ///     snapshot with empty lexical/speaker arrays.
    ///   - fmAvailable: Reserved for future FM-augmented lexical
    ///     passes. Not used in this bead; accepted to lock the
    ///     signature so a follow-up bead can flip behavior on without
    ///     changing call sites.
    ///   - audioService: Test seam — production callers omit this.
    ///   - featureService: Test seam — production callers omit this.
    ///   - lexicalScanner: Test seam — production callers omit this.
    ///   - shardDuration: Audio-shard duration. Defaults to the same
    ///     30 s that `AnalysisAudioService` uses for the production
    ///     analysis pipeline.
    /// - Returns: A `ChapterFeatureSnapshot` with `episodeDuration`
    ///   derived from the decoded shard span and all four signal
    ///   arrays populated where the input supports them.
    static func build(
        audioURL: URL,
        transcript: [TranscriptChunk],
        fmAvailable: Bool,
        audioService: AnalysisAudioProviding = AnalysisAudioService(),
        featureService: FeatureExtractionService? = nil,
        lexicalScanner: LexicalScanner = LexicalScanner(),
        shardDuration: TimeInterval = AnalysisAudioService.defaultShardDuration
    ) async throws -> ChapterFeatureSnapshot {
        _ = fmAvailable // see file header — accepted for forward-compat

        guard let localURL = LocalAudioURL(audioURL) else {
            throw BuildError.audioURLNotLocal(audioURL)
        }

        // 1. Decode raw audio into analysis shards. Reuses the
        //    production `AnalysisAudioService` so cache hits and
        //    streaming-RAM behaviour match the hot path exactly.
        //    `episodeID` is the URL's last path component because the
        //    helper has no other stable id available — the
        //    shard-cache directory keys off this. Callers that want
        //    deterministic caching across runs can stage their audio
        //    under predictable filenames.
        let episodeID = localURL.lastPathComponent
        let shards = try await audioService.decode(
            fileURL: localURL,
            episodeID: episodeID,
            shardDuration: shardDuration
        )

        // 2. Run feature extraction across the shards. The
        //    actor-isolated batch entry shares the same
        //    `extractWindows(...)` private path that
        //    `extractAndPersist(...)` calls, so the resulting windows
        //    are byte-identical to what production persistence would
        //    have produced (modulo SoundAnalysis being non-determinis-
        //    tic across runs; same caveat applies to the production
        //    path).
        //
        //    `analysisAssetId` is the URL's path component — the field
        //    is informational here because nothing is persisted.
        //
        //    `FeatureExtractionService.init` requires an `AnalysisStore`
        //    because the persisting `extractAndPersist(...)` entry depends
        //    on it. The builder only drives the non-persisting
        //    `extract(shards:)` entry, so the store handed in is never
        //    touched. We use the `:memory:` SQLite URI form — the on-disk
        //    handle is opened lazily on first use, and since the builder
        //    never invokes a store method, no I/O ever occurs. The
        //    `try` propagates rather than force-trying so a future change
        //    to `AnalysisStore.init(path:)` that introduces eager work
        //    surfaces here as a normal error instead of a trap.
        let extractor: FeatureExtractionService
        if let featureService {
            extractor = featureService
        } else {
            let store = try AnalysisStore(path: ":memory:")
            extractor = FeatureExtractionService(store: store)
        }
        let featureWindows = await extractor.extract(
            shards: shards,
            analysisAssetId: episodeID
        )

        // 3. Project everything into the boundary-detector input
        //    shape. The episode duration is the upper bound of the
        //    decoded audio (last shard's end) — matches what the
        //    production pipeline records as `featureCoverageEndTime`.
        let episodeDuration: TimeInterval
        if let lastShard = shards.last {
            episodeDuration = lastShard.startTime + lastShard.duration
        } else {
            episodeDuration = 0
        }

        return snapshot(
            from: featureWindows,
            transcript: transcript,
            episodeDuration: episodeDuration,
            lexicalScanner: lexicalScanner
        )
    }

    /// Project `[FeatureWindow]` + transcript into a
    /// `ChapterFeatureSnapshot`. Pure mapping — no I/O, no DSP. Shared
    /// with any future production wiring that needs the same
    /// projection.
    ///
    /// Ordering invariants the boundary detector relies on are
    /// enforced here:
    ///   * music / speaker / pause windows are sorted by `startTime`
    ///     ascending (the upstream `featureWindows` is already
    ///     time-ordered, but we sort defensively in case a caller
    ///     hands in unsorted input — speaker windows come from
    ///     `transcript` and may need it).
    ///   * lexical hits are emitted in transcript order, which the
    ///     detector sorts again internally.
    static func snapshot(
        from featureWindows: [FeatureWindow],
        transcript: [TranscriptChunk],
        episodeDuration: TimeInterval,
        lexicalScanner: LexicalScanner = LexicalScanner()
    ) -> ChapterFeatureSnapshot {
        // Music + pause windows: every feature window contributes one
        // observation of each. Already sorted by start time when
        // emitted by `FeatureExtractionService`, so a `sorted` here
        // would be a no-op on production input. We sort defensively
        // because a hostile caller could in theory pass an unsorted
        // array; doing it once here is cheaper than re-checking in
        // every downstream detector.
        let sortedWindows = featureWindows.sorted { $0.startTime < $1.startTime }
        let musicWindows = sortedWindows.map { window in
            ChapterMusicWindow(
                startTime: window.startTime,
                endTime: window.endTime,
                musicProbability: window.musicProbability
            )
        }
        let pauseWindows = sortedWindows.map { window in
            ChapterPauseWindow(
                startTime: window.startTime,
                endTime: window.endTime,
                pauseProbability: window.pauseProbability
            )
        }

        // Speaker windows: built from transcript chunks whose
        // `speakerId` was populated by the upstream speaker-label
        // pipeline. Chunks without a speakerId are still emitted as
        // ChapterSpeakerWindow(clusterId: nil) — see the
        // ChapterSpeakerWindow docstring: nil-cluster windows are
        // treated as "no signal" by the detector (neither a shift nor
        // a continuation), so emitting them preserves the time
        // coverage without producing false shifts.
        let speakerWindows = transcript
            .sorted { $0.startTime < $1.startTime }
            .map { chunk in
                ChapterSpeakerWindow(
                    startTime: chunk.startTime,
                    endTime: chunk.endTime,
                    clusterId: chunk.speakerId
                )
            }

        // Lexical hits: feed the transcript through the standard
        // `LexicalScanner.scanChunk(...)` so the boundary detector
        // sees the same per-chunk regex matches the production
        // backfill pipeline does. We unwrap each chunk's hits into
        // the snapshot's `[ChapterLexicalHit]` projection.
        var lexicalHits: [ChapterLexicalHit] = []
        for chunk in transcript {
            let hits = lexicalScanner.scanChunk(chunk)
            for hit in hits {
                lexicalHits.append(ChapterLexicalHit(
                    startTime: hit.startTime,
                    category: hit.category
                ))
            }
        }

        return ChapterFeatureSnapshot(
            episodeDuration: max(0, episodeDuration),
            musicWindows: musicWindows,
            speakerWindows: speakerWindows,
            lexicalHits: lexicalHits,
            pauseWindows: pauseWindows
        )
    }
}
