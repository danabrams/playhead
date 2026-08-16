// TranscriptEngineService.swift
// Orchestrates on-device transcription for the analysis pipeline.
//
// Accepts decoded audio shards from AnalysisAudioService, runs them through
// Apple Speech via SpeechService, and writes TranscriptChunks to SQLite.
//
// Processing strategy:
//   - VAD/pause-anchored chunks (target 8-20 s with small overlap)
//   - Dynamic wall-clock safety margin ahead of the playhead
//   - Hot-path coverage independent from final-pass completeness
//   - Immediate cancel/reprioritize on scrubs and speed changes
//   - Checkpoint per chunk hash for resumability
//   - Stream fast-pass chunks as they complete
//   - Promote to final-pass when idle/charging

import CryptoKit
import Foundation
import OSLog

// MARK: - Configuration

struct TranscriptEngineServiceConfig: Sendable {
    /// Target chunk duration in seconds for VAD-anchored splits.
    let targetChunkDuration: TimeInterval
    /// Minimum chunk duration in seconds.
    let minChunkDuration: TimeInterval
    /// Maximum chunk duration in seconds.
    let maxChunkDuration: TimeInterval
    /// Overlap between consecutive chunks in seconds.
    let chunkOverlap: TimeInterval
    /// Wall-clock seconds of lookahead to maintain ahead of the playhead.
    let lookaheadWallClockSeconds: TimeInterval
    /// Minimum speech probability to consider a VAD frame as speech.
    let vadSpeechThreshold: Float
    /// Model version tag written to each TranscriptChunk.
    let modelVersion: String

    static let `default` = TranscriptEngineServiceConfig(
        targetChunkDuration: 12.0,
        minChunkDuration: 8.0,
        maxChunkDuration: 20.0,
        chunkOverlap: 0.5,
        lookaheadWallClockSeconds: 30.0,
        vadSpeechThreshold: 0.5,
        modelVersion: "apple-speech-v1"
    )
}

// MARK: - Playback state snapshot

/// Snapshot of playback state used to compute transcription priorities.
/// Provided by the caller (e.g., AnalysisCoordinator) each time
/// transcription is kicked or the playhead moves significantly.
struct PlaybackSnapshot: Sendable {
    /// Current playhead position in audio seconds.
    let playheadTime: TimeInterval
    /// Current playback rate (1.0 = normal, 2.0 = 2x, etc.).
    let playbackRate: Double
    /// Whether playback is actively playing.
    let isPlaying: Bool
}

// MARK: - TranscriptEngineServiceError

enum TranscriptEngineServiceError: Error, CustomStringConvertible {
    case noShardsAvailable
    case speechServiceNotReady
    case chunkingFailed(String)

    var description: String {
        switch self {
        case .noShardsAvailable:
            "No analysis shards available for transcription"
        case .speechServiceNotReady:
            "Speech engine is not ready"
        case .chunkingFailed(let reason):
            "Chunk boundary computation failed: \(reason)"
        }
    }
}

// MARK: - TranscriptFailureClass (playhead-8ysk)

/// Redacted, closed-vocabulary class of a transcription failure.
///
/// WHY A CLOSED ENUM AND NOT THE ERROR. Six days of `scheduler_events` from
/// the owner's device attribute 17 failures to `asr_failed`, and that label
/// carries no information: its sole emitter is
/// `AnalysisJobRunner.emitTranscriptionTimeoutJournal`, fired on
/// `transcriptCoverage == 0`, with no `Error` value in scope anywhere in the
/// block. It names an ABSENCE, not a failure, and at least nine distinct
/// causes are indistinguishable behind it.
///
/// The obvious fix — forward the error text — is not available. `runner_reason`
/// already survives to SQLite and is then dropped by
/// `DiagnosticsBundle`'s projection on PII grounds, because callers stash
/// arbitrary JSON in `metadata`. So the diagnostic value has to arrive in a
/// shape that is safe BY CONSTRUCTION. This is the same move
/// `playhead-p70f` made for the rediff lane's day-0 exits: the free-form
/// `lastDetail` (which can carry an enclosure URL) is not projected, and the
/// run ledger's free-form annotation is parsed into integers rather than
/// forwarded.
///
/// Every case here is a fixed compile-time string. None is derived from
/// audio, a feed, a URL, a title, or anything a user typed, so the whole
/// vocabulary can be exported without review. Adding a case is a deliberate
/// act with a test (`TranscriptFailureTaxonomyTests`) that pins the set.
enum TranscriptFailureClass: String, Sendable, Hashable, CaseIterable, Codable {
    /// `TranscriptEngine` rejected a shard whose samples are all zero. This
    /// is the sub-millisecond failure: a pure CPU scan, no ASR involved.
    case silentShard = "silent_shard"
    /// A shard arrived with no samples at all.
    case emptyShard = "empty_shard"
    /// A shard contained NaN or infinite samples.
    case nonFiniteSamples = "non_finite_samples"
    /// No ASR recognizer was ready (`TranscriptEngineError.modelNotLoaded`).
    /// Its usual antecedent is a model load that failed at launch and was
    /// never retried.
    case modelNotLoaded = "model_not_loaded"
    /// The device has no on-device Speech assets for the episode's locale.
    case speechAssetsUnsupported = "speech_assets_unsupported"
    /// `SpeechAnalyzer` did not negotiate a usable audio format.
    case analyzerFormatUnavailable = "analyzer_format_unavailable"
    /// PCM could not be bridged into the analyzer's input stream.
    case audioBridgeFailure = "audio_bridge_failure"
    /// The analyzer was handed a non-monotonic or overlapping timeline.
    case invalidAnalyzerTimeline = "invalid_analyzer_timeline"
    /// The analyzer session itself failed or ended abnormally.
    case analyzerSessionFailure = "analyzer_session_failure"
    /// Recognition ran and reported an error. `code` carries the underlying
    /// `NSError.code`; the domain and message are deliberately not exported.
    case transcriptionFailed = "transcription_failed"
    /// Voice-activity detection failed, so the shard could not be chunked.
    case vadFailed = "vad_failed"
    /// A shard arrived at a sample rate the engine does not accept.
    case unsupportedSampleRate = "unsupported_sample_rate"
    /// The engine was asked to transcribe with no shards at all.
    case noShards = "no_shards"
    /// `SpeechService.isReady()` was false, so the loop never started. Before
    /// this bead this path returned in silence and the runner waited out its
    /// full 300 s timeout for a `.completed` that could never arrive.
    case speechEngineNotReady = "speech_engine_not_ready"
    /// Persisting chunks to SQLite failed — every `AnalysisStoreError` out of
    /// `transcribeShard`'s `insertTranscriptChunks` / coverage writes lands
    /// here.
    ///
    /// This is not a hypothetical class. It is the failure that produced this
    /// bead's most instructive artifact: `appendShardsAfterCompletion` never
    /// seeded its `analysis_assets` row, so every chunk insert hit the foreign
    /// key and failed, and the test passed anyway because the loop reported
    /// `.completed` over a total failure. A run in that state does ASR work,
    /// writes nothing, and — until this case was wired up (round-1 review) —
    /// reported `unknown`, which is the same "names an absence" defect as
    /// `asr_failed` one layer down.
    case persistenceFailed = "persistence_failed"
    /// playhead-ngev: the run's task was cancelled. In production the
    /// canceller is almost always PLAYBACK, not a failure: one
    /// `TranscriptEngineService` is shared by `AnalysisCoordinator` and
    /// `AnalysisJobRunner` (`PlayheadRuntime` builds exactly one), and
    /// `startTranscription` — reached from `handleScrub`, `handleSpeedChange`
    /// and every new episode — cancels whatever the other owner was running.
    ///
    /// It is a failure class rather than a silent return because the runner
    /// has to be told SOMETHING: before this bead the cancelled loop returned
    /// without a word, the runner waited out its full 300 s timeout, and the
    /// row it finally wrote said `asr_failed`. Scrubbing was recorded as an
    /// ASR failure.
    case cancelled
    /// playhead-ngev: the owning caller gated this asset through
    /// `stopTranscription(analysisAssetId:)`. Distinct from `.cancelled`
    /// because the asset is also fenced against late writes and appends —
    /// this run is not merely losing the engine, it is forbidden from
    /// finishing.
    case stopped
    /// playhead-ngev: a higher lane preempted at a safe point
    /// (`TranscriptEnginePreempted`). The runner normally intercepts this
    /// through the sticky `PreemptionSignal` and reports `.preempted` rather
    /// than a failure; the class exists so the loop's exit is named at the
    /// engine boundary too, rather than being the one interruption that still
    /// returns in silence.
    case preempted
    /// Anything not yet classified. A rising count here is the signal that
    /// this taxonomy needs another case — never a reason to log free text.
    case unknown

    /// playhead-ngev: whether this class means the recognizer ACTUALLY RAN.
    ///
    /// It decides `work_journal.cause`, which was a hardcoded `.asrFailed` on
    /// every zero-coverage row — including rows whose own `failure_class`
    /// proved ASR was never invoked. A row that reads
    /// `cause = asr_failed, failure_class = no_shards` is worse than an
    /// unnamed row: it contradicts itself, and the wrong half is the one an
    /// aggregate counts.
    ///
    /// Exhaustive on purpose (no `default`): a new case must make this
    /// decision explicitly rather than inheriting whichever answer happened to
    /// be the fallback. The bar is deliberately high — recognition has to have
    /// been reached, not merely attempted — because the three sample guards,
    /// the analyzer-setup failures and the model/asset failures all describe
    /// something UPSTREAM of the recognizer, and calling those `asr_failed` is
    /// what sent two dogfood cycles looking at the wrong stage.
    var impliesRecognizerRan: Bool {
        switch self {
        case .transcriptionFailed, .vadFailed, .analyzerSessionFailure:
            // Recognition (or the analyzer session driving it) started and
            // reported an error of its own.
            true
        case .silentShard, .emptyShard, .nonFiniteSamples,
             .audioBridgeFailure, .unsupportedSampleRate:
            // The audio was rejected before recognition — a DECODE-side
            // diagnosis wearing an ASR label.
            false
        case .modelNotLoaded, .speechEngineNotReady, .speechAssetsUnsupported,
             .analyzerFormatUnavailable, .invalidAnalyzerTimeline:
            // The recognizer could not be brought up at all.
            false
        case .noShards, .persistenceFailed:
            // Nothing to recognise, or the failure was SQLite's.
            false
        case .cancelled, .stopped, .preempted:
            // The run was cut short from outside; whatever ASR did or did not
            // do, it is not what ended the run.
            false
        case .unknown:
            // Includes genuine framework `NSError`s that carry a code but no
            // recognised type. "We do not know" must not be exported as a
            // positive claim that ASR ran.
            false
        }
    }

    /// Classify a thrown error. Total by construction: everything that is not
    /// recognised is `.unknown`, so no call site can leak a description.
    static func classify(_ error: Error) -> TranscriptFailureClass {
        // playhead-8ysk review: the store's own errors, which reach this
        // classifier through `transcribeShard`'s persistence writes. Only the
        // TYPE is inspected — `AnalysisStoreError`'s payloads are free-form
        // SQLite messages and none of them is read here, so nothing this
        // classifier returns can carry one. (Its bridged NSError domain is
        // `Playhead.AnalysisStoreError`, so `TranscriptFailureReason.classify`
        // also drops the synthesised case ordinal.)
        if error is AnalysisStoreError {
            return .persistenceFailed
        }
        // playhead-ngev: the three interruptions. The loop catches each of
        // these by type before it reaches its catch-all, so these lines are
        // not how the classes are normally produced — they are here so that an
        // interruption arriving WRAPPED (rethrown out of a store call, say)
        // cannot land in `.unknown`, which is the same "names an absence"
        // defect one bucket down.
        if error is CancellationError {
            return .cancelled
        }
        if error is TranscriptEngineStopped {
            return .stopped
        }
        if error is TranscriptEnginePreempted {
            return .preempted
        }
        if let engineError = error as? TranscriptEngineError {
            switch engineError {
            case .modelNotLoaded:
                return .modelNotLoaded
            case .unsupportedSampleRate:
                return .unsupportedSampleRate
            case .vadFailed:
                return .vadFailed
            case .transcriptionFailed:
                return .transcriptionFailed
            }
        }
#if canImport(Speech)
        if let boundaryError = error as? AppleSpeechBoundaryError {
            switch boundaryError {
            case .speechAssetsUnsupported:
                return .speechAssetsUnsupported
            case .analyzerFormatUnavailable:
                return .analyzerFormatUnavailable
            case .invalidAnalyzerInputTimeline:
                return .invalidAnalyzerTimeline
            case .analyzerSessionFailure:
                return .analyzerSessionFailure
            case .audioBridgeFailure(let reason):
                // `TranscriptEngine.makeSourceBuffer`'s three sample guards
                // are all raised as `.audioBridgeFailure`, so the sub-cases
                // are recovered here rather than by widening that enum, which
                // would ripple through every existing catch. They are worth
                // separating: a silent shard is rejected by a CPU scan in well
                // under a millisecond, which is what makes a whole episode
                // fail instantly, and collapsing all three would leave the
                // dominant mode unnamed all over again.
                return SampleGuardMarker.classify(reason) ?? .audioBridgeFailure
            }
        }
#endif
        return .unknown
    }

    /// Substrings identifying the three sample guards in
    /// `TranscriptEngine.makeSourceBuffer`.
    ///
    /// These are matched, not constructed, so they are a coupling to message
    /// text in another file. `TranscriptFailureTaxonomyTests` closes that by
    /// throwing through the REAL guards and asserting the classification, so a
    /// reworded message fails a test instead of silently degrading every
    /// future diagnostic bundle to `audio_bridge_failure`.
    enum SampleGuardMarker: String, CaseIterable {
        case empty = "empty audio shard"
        case nonFinite = "NaN and"
        case silent = "is entirely silent"

        var failureClass: TranscriptFailureClass {
            switch self {
            case .empty: .emptyShard
            case .nonFinite: .nonFiniteSamples
            case .silent: .silentShard
            }
        }

        static func classify(_ reason: String) -> TranscriptFailureClass? {
            allCases.first { reason.contains($0.rawValue) }?.failureClass
        }
    }
}

/// playhead-ngev: HOW the run ended, which is a different question from what
/// went wrong in it.
///
/// The two axes have to be separate because they disagree in the case that
/// matters. A run whose shards were failing `model_not_loaded` and which was
/// then cancelled by a scrub has BOTH a genuine diagnosis (the class) and a
/// termination that was not its own doing. Collapsing them either loses the
/// class — reporting only "cancelled", throwing away what the engine knew — or
/// loses the interruption, which is what tells `AnalysisCoordinator` that a
/// successor loop is already running and this session must not be torn down.
///
/// Two compile-time literals, nothing derived from audio, a feed, a URL or
/// user text — the same construction argument `TranscriptFailureClass` won.
enum TranscriptRunTermination: String, Sendable, Hashable, Codable, CaseIterable {
    /// The loop reached its own end and judged the run a total failure.
    case ranToConclusion = "ran_to_conclusion"
    /// The loop was cut short from outside: cancelled, stopped or preempted.
    case interrupted
}

/// playhead-8ysk: what a transcription failure was, in exportable form.
///
/// `code` is an `NSError.code` when the underlying error carried one. An
/// integer cannot carry PII, and it is what separates one Speech-domain
/// failure from another once the class is known.
struct TranscriptFailureReason: Sendable, Hashable {
    let failureClass: TranscriptFailureClass
    let code: Int?
    /// How many shards failed before the loop gave up. `0` for failures
    /// raised before any shard was attempted.
    let failedShardCount: Int
    /// playhead-ngev: whether the loop finished or was cut short. Defaults to
    /// `.ranToConclusion` so every pre-existing construction — including the
    /// total-failure gate's — keeps its meaning unchanged.
    let termination: TranscriptRunTermination

    init(
        failureClass: TranscriptFailureClass,
        code: Int? = nil,
        failedShardCount: Int = 0,
        termination: TranscriptRunTermination = .ranToConclusion
    ) {
        self.failureClass = failureClass
        self.code = code
        self.failedShardCount = failedShardCount
        self.termination = termination
    }

    /// This reason, re-stamped as an interrupted run. Used where the loop has
    /// a real per-shard diagnosis to report but is exiting because something
    /// outside it said so.
    func interrupted() -> TranscriptFailureReason {
        TranscriptFailureReason(
            failureClass: failureClass,
            code: code,
            failedShardCount: failedShardCount,
            termination: .interrupted
        )
    }

    /// Bridged `NSError` domains whose `code` is a synthesised case ordinal
    /// rather than a diagnostic: our own module's types and the standard
    /// library's. Neither can produce a number a support engineer can act on.
    static let syntheticErrorDomainPrefixes = ["Playhead.", "Swift."]

    /// Classify a thrown error into an exportable reason.
    static func classify(_ error: Error, failedShardCount: Int = 0) -> TranscriptFailureReason {
        // Every Swift error bridges to an NSError, so `error as NSError` never
        // fails and `error is NSError` is always true — measured, not assumed.
        // A Swift-native enum bridges to the synthesised domain
        // "<Module>.<TypeName>" with the case's ORDINAL as its code, so
        // exporting that would be noise dressed as data (`.modelNotLoaded`
        // would ship `code: 0`). Only a genuine framework error — Speech,
        // AVFoundation, POSIX — carries a code worth having.
        //
        // playhead-ngev: the STDLIB's domain is synthesised the same way and
        // was not covered. `CancellationError` bridges to
        // `Swift.CancellationError` with code 1 (measured, not assumed — it is
        // not even 0), so a cancelled run exported `failure_code: 1` beside
        // `failure_class: cancelled`, and in a support bundle a bare integer
        // next to a class reads as a real framework code. That is the same
        // "noise dressed as data" this rule already rejects for our own types;
        // the rule was just written one module too narrow.
        let nsError = error as NSError
        let isOurSyntheticDomain = Self.syntheticErrorDomainPrefixes.contains {
            nsError.domain.hasPrefix($0)
        }
        return TranscriptFailureReason(
            failureClass: TranscriptFailureClass.classify(error),
            code: isOurSyntheticDomain ? nil : nsError.code,
            failedShardCount: failedShardCount
        )
    }
}

enum TranscriptEngineEvent: Sendable {
    case chunksPersisted(analysisAssetId: String, chunks: [TranscriptChunk])
    case completed(analysisAssetId: String)
    /// playhead-8ysk: the transcription loop ended having produced nothing.
    ///
    /// Before this case existed the event type could not express a failure at
    /// all, so the catch in `runTranscriptionLoop` logged the error and moved
    /// on — and after EVERY shard had failed the loop fell through and emitted
    /// `.completed`. A total failure was reported upward as success,
    /// distinguishable only by the coverage number being zero, which is why
    /// the journal could only ever say `asr_failed`.
    case failed(analysisAssetId: String, reason: TranscriptFailureReason)
}

/// Thrown by `transcribeShard` when the playhead-01t8 preemption
/// signal flips after a chunk batch has been persisted. The
/// transcription loop catches this to exit cleanly at the safe point
/// without logging a shard failure.
struct TranscriptEnginePreempted: Error {}

/// playhead-5uvz.5 (Gap-6): thrown by `transcribeShard` when a
/// `stopTranscription(analysisAssetId:)` lands while a shard is
/// in-flight. The transcription loop catches this to exit cleanly
/// without logging a shard failure or persisting partial output for
/// the stopped asset.
struct TranscriptEngineStopped: Error {}

// MARK: - TranscriptEngineService

/// Orchestrates transcription of decoded audio shards into TranscriptChunks
/// persisted to SQLite. Manages prioritization around the playhead, handles
/// scrub reprioritization, and supports resumable checkpointing.
actor TranscriptEngineService {

    private let logger = Logger(subsystem: "com.playhead", category: "TranscriptEngineService")

    private let speechService: SpeechService
    private let store: AnalysisStore
    private let config: TranscriptEngineServiceConfig

    /// Currently active transcription task, cancelled on scrubs or shutdown.
    private var activeTask: Task<Void, Never>?

    /// The analysis asset ID currently being processed.
    private var activeAssetId: String?

    /// The podcast ID currently being processed, used for ASR vocabulary
    /// biasing on the SpeechAnalyzer path.
    private var activePodcastId: String?

    /// Last known playback snapshot for priority computation.
    private var latestSnapshot: PlaybackSnapshot?

    /// Running chunk index counter per asset, for ordering.
    private var chunkCounter: Int = 0

    /// Shards queued for processing while the main loop is running.
    private var appendedShards: [AnalysisShard] = []

    /// True once the caller has explicitly signalled that no more shards
    /// will be appended for the currently active asset (via
    /// `finishAppending(analysisAssetId:)`). The transcription loop will
    /// only emit `.completed` after this flag is set — a momentarily
    /// empty `appendedShards` queue is NOT sufficient.
    ///
    /// Reset to `false` in `startTranscription` / `stop` so a new session
    /// does not inherit the prior session's end-of-input signal.
    private var inputClosed: Bool = false

    /// Continuations waiting for additional shards (or an end-of-input
    /// signal). `waitForMoreShards` appends here; `appendShards`,
    /// `finishAppending`, and `stop` resume every pending continuation.
    private var appendWaiters: [CheckedContinuation<Void, Never>] = []

    /// True while the transcription loop is actively processing.
    /// Used by appendShards to decide whether to start a new loop.
    private var loopRunning: Bool = false

    /// playhead-8ysk (review r3) / playhead-ngev: what ONE shard attempt
    /// produced, returned by `transcribeShard` so the loop can add it up.
    ///
    /// It used to be two actor fields zeroed at the top of
    /// `runTranscriptionLoop`, and their doc comments claimed per-pass
    /// scoping that the actor could not provide: `startTranscription` cancels
    /// its predecessor and spawns the successor WITHOUT awaiting it, and
    /// cancellation is cooperative, so two loop bodies overlap — and
    /// `transcribeShard`'s silent-shard path has no cancellation check between
    /// the recognizer returning and the increment. A cancelled predecessor
    /// could therefore credit the successor's tally and suppress a genuine
    /// `.failed`, which is the same "the count is not about this run" defect
    /// the fields were introduced to fix, one level out. Loop-locals cannot
    /// have it: there is no shared cell to write into.
    struct ShardProgress: Sendable {
        /// playhead-8ysk (review r3): durable `transcript_chunks` rows this
        /// attempt inserted. The loop sums them into `chunksInsertedThisRun`.
        ///
        /// The obvious alternative — asking the store — is wrong.
        /// `store.fetchTranscriptChunks(assetId:)` is
        /// `WHERE analysisAssetId = ?` with no pass or generation scoping, and
        /// the asset row is REUSED across passes (`AnalysisCoordinator`
        /// resolves it with `fetchAssetByEpisodeId` and keeps `existing.id`).
        /// So on a retry of an asset that once made progress, that query
        /// returns the EARLIER pass's chunks and a total failure now would
        /// read as "we produced something". A retry of a partly-transcribed
        /// asset is the exact shape of playhead-8ysk's incident (147
        /// acquisitions, 9 finalizations), so the cumulative read would have
        /// failed in the case the failure event exists for.
        ///
        /// `AnalysisJobRunner` already does this correctly and is the model:
        /// it snapshots `existingChunkCount` before transcription and reports
        /// `currentChunkCount - existingChunkCount`.
        var chunksInserted: Int = 0

        /// playhead-8ysk (review r4): whether the attempt carried the shard to
        /// a clean finish. The loop sums these into `shardsCompletedThisRun`.
        ///
        /// IT EXISTS BECAUSE COUNTING INSERTS IS NOT THE SAME AS COUNTING
        /// WORK, and the total-failure gate needs the second one.
        /// `transcribeShard` returns without inserting a row in two ordinary,
        /// successful cases: a shard whose recognizer yields no segments
        /// (silence, music — it still advances the coverage watermark and
        /// returns), and a shard whose segments all match an existing
        /// `segmentFingerprint` and are therefore deduped. The second is not
        /// an edge case: a re-run over an asset that is already transcribed
        /// dedups EVERY segment, and the loop deliberately does not filter
        /// shards by coverage, precisely so the fingerprint dedup can do it.
        ///
        /// So an insert count of zero alone reads a fully-successful re-run
        /// that happened to also hit one bad shard as "this run produced
        /// nothing", and reports a total failure for it — the same lie the
        /// gate removes, pointing the other way, costing a spurious
        /// `work_journal` row, a wrong `lastErrorCode`, a requeue with
        /// backoff, and a skipped `finalizeBackfill`.
        ///
        /// A shard that finishes has done durable work even with no row to
        /// show for it: the coverage watermark moved, so that audio is not
        /// re-attempted. Same bar `partialSuccessStillCompletes` sets — one
        /// bad shard among good ones is a partial success, not a failure.
        var finished: Bool = false
    }

    /// Optional preemption context threaded in by AnalysisJobRunner
    /// (playhead-01t8). Polled after each TranscriptChunk batch
    /// persists; on a preempt request the loop acknowledges and
    /// exits at that safe point.
    private var preemption: PreemptionContext?

    /// Broadcasts persisted chunk batches and completion signals to the
    /// analysis coordinator without forcing it to poll SQLite.
    private var eventContinuations: [UUID: AsyncStream<TranscriptEngineEvent>.Continuation] = [:]

    /// playhead-5uvz.5 (Gap-6): assets the caller explicitly stopped via
    /// `stopTranscription(analysisAssetId:)`. Used to drop late writes,
    /// late event emissions, and any queued append shards that race the
    /// stop. The set is small (one entry per stopped asset) and is
    /// cleared opportunistically when a fresh `startTranscription` is
    /// called for that asset (so a re-run after stop is not silently
    /// suppressed).
    ///
    /// Why a set rather than a single flag: `appendShards` from a
    /// streaming producer can land for an asset that the runner has
    /// already stopped — those late appends must be dropped on contact,
    /// not enqueued and then re-dropped at transcribe time. Tracking
    /// the asset id (rather than just the active id) lets us reject
    /// post-stop appends even if `activeAssetId` has rotated to a
    /// different asset in the meantime.
    private var stoppedAssetIds: Set<String> = []

    /// playhead-ngev: which transcription run is the current one.
    ///
    /// Bumped by every path that spawns a loop (`startTranscription` and
    /// `appendShards`' cold start). A loop carries the value it was born with,
    /// so it can ask "am I still the run this engine is driving?" without
    /// consulting `activeTask` (which `stopTranscription` nils out) or
    /// `activeAssetId` (which it clears).
    ///
    /// It exists for the stop-gate exemption in `emitEvent`. A `.failed` from
    /// the CURRENT run must survive the gate — that is the failure the runner
    /// is about to journal, and dropping it is how a fully classified failure
    /// got deleted 30 lines before the row that needed it was written. A
    /// `.failed` from a SUPERSEDED run must not: the successor has its own
    /// observer, and a stale reason delivered into it would fail a run that
    /// has not done anything yet.
    private var runGeneration: Int = 0

    // MARK: - Init

    init(
        speechService: SpeechService,
        store: AnalysisStore,
        config: TranscriptEngineServiceConfig = .default
    ) {
        self.speechService = speechService
        self.store = store
        self.config = config
    }

    // MARK: - Public API

    /// Start or resume transcription for an episode.
    ///
    /// Transcribes shards in priority order (near the playhead first),
    /// writing chunks to SQLite as each completes. The operation is
    /// cancellable and resumable.
    ///
    /// - Parameters:
    ///   - shards: Decoded audio shards from AnalysisAudioService.
    ///   - analysisAssetId: The analysis asset these shards belong to.
    ///   - snapshot: Current playback state for prioritization.
    func startTranscription(
        shards: [AnalysisShard],
        analysisAssetId: String,
        snapshot: PlaybackSnapshot,
        podcastId: String? = nil,
        preemption: PreemptionContext? = nil
    ) {
        // Cancel any existing work — we're starting fresh or reprioritizing.
        activeTask?.cancel()

        // A fresh start should not inherit queued append work from a prior
        // loop. When the asset changes, also reset the per-asset chunk
        // index and the end-of-input flag. When the asset matches,
        // preserve `inputClosed` so a streaming producer that already
        // finished (and called `finishAppending`) earlier in the pipeline
        // does not get its end-of-input signal silently discarded by this
        // reset.
        if activeAssetId != analysisAssetId {
            chunkCounter = 0
            inputClosed = false
        }
        appendedShards = []
        // playhead-5uvz.5: an explicit `startTranscription` for this
        // asset rescinds any prior `stopTranscription` gate — re-runs
        // are allowed and must not be silently suppressed by a stale
        // stop. We only clear the entry for *this* asset; stops for
        // other assets remain in place.
        stoppedAssetIds.remove(analysisAssetId)
        // Wake any leftover waiters from a previous loop so they exit
        // promptly; this keeps stale continuations from being orphaned.
        resumeAllAppendWaiters()

        activeAssetId = analysisAssetId
        activePodcastId = podcastId
        latestSnapshot = snapshot
        self.preemption = preemption

        runGeneration += 1
        let generation = runGeneration
        activeTask = Task { [weak self] in
            guard let self else { return }
            await self.runTranscriptionLoop(
                shards: shards,
                analysisAssetId: analysisAssetId,
                generation: generation
            )
            await self.clearActiveTask()
        }
    }

    /// Notify the service that the playhead has scrubbed to a new position.
    /// Cancels in-flight work and restarts with new priorities.
    func handleScrub(
        shards: [AnalysisShard],
        analysisAssetId: String,
        snapshot: PlaybackSnapshot
    ) {
        logger.info("Scrub detected — reprioritizing from \(snapshot.playheadTime, format: .fixed(precision: 1))s")
        startTranscription(
            shards: shards,
            analysisAssetId: analysisAssetId,
            snapshot: snapshot,
            podcastId: activePodcastId
        )
    }

    /// Notify the service that playback speed changed significantly.
    /// Updates the snapshot and reprioritizes — the lookahead window scales
    /// with playback rate, so in-flight ordering may be stale.
    func handleSpeedChange(
        shards: [AnalysisShard],
        analysisAssetId: String,
        snapshot: PlaybackSnapshot
    ) {
        logger.info("Speed changed to \(snapshot.playbackRate, format: .fixed(precision: 1))x — reprioritizing")
        startTranscription(
            shards: shards,
            analysisAssetId: analysisAssetId,
            snapshot: snapshot,
            podcastId: activePodcastId
        )
    }

    /// Append new shards to the running transcription without cancelling.
    /// If no transcription is active, starts a new loop.
    func appendShards(
        _ newShards: [AnalysisShard],
        analysisAssetId: String,
        snapshot: PlaybackSnapshot
    ) {
        guard !newShards.isEmpty else { return }
        // playhead-5uvz.5: drop appends targeting an asset that the
        // runner has explicitly stopped. Without this guard a streaming
        // producer racing the timeout could re-arm the session by
        // appending shards (and wake a waiter that is no longer parked
        // in any meaningful sense) right after the runner moved on.
        if stoppedAssetIds.contains(analysisAssetId) {
            logger.info(
                "Dropping \(newShards.count) appended shards for stopped asset \(analysisAssetId)"
            )
            return
        }
        latestSnapshot = snapshot

        // The transcript engine is single-asset. If a late append arrives for
        // an asset that is no longer active, drop it instead of mixing old
        // streaming output into the current session.
        if let activeAssetId, activeAssetId != analysisAssetId {
            logger.info(
                "Dropping \(newShards.count) appended shards for stale asset \(analysisAssetId); active asset is \(activeAssetId)"
            )
            return
        }

        if activeAssetId == nil {
            activeAssetId = analysisAssetId
            chunkCounter = 0
        }

        appendedShards.append(contentsOf: newShards)

        // Appending more work cancels any prior end-of-input signal and
        // wakes the loop if it was suspended waiting for shards.
        inputClosed = false
        resumeAllAppendWaiters()

        // If no active loop, start one for the appended shards.
        if !loopRunning {
            activeAssetId = analysisAssetId
            runGeneration += 1
            let generation = runGeneration
            activeTask = Task { [weak self] in
                guard let self else { return }
                await self.runTranscriptionLoop(
                    shards: [],  // empty — the loop will pick up from appendedShards
                    analysisAssetId: analysisAssetId,
                    generation: generation
                )
            }
        }
    }

    /// Signal that no more shards will be appended for the given asset.
    /// The transcription loop will drain any remaining backlog and then
    /// emit `.completed`.
    ///
    /// The assetId is validated against `activeAssetId`; a mismatch is
    /// logged and ignored so a stale end-of-input signal from a previous
    /// session cannot terminate the current loop early.
    func finishAppending(analysisAssetId: String) {
        guard let activeAssetId else {
            logger.debug("finishAppending(\(analysisAssetId)): no active asset — ignoring")
            return
        }
        guard activeAssetId == analysisAssetId else {
            logger.info(
                "finishAppending(\(analysisAssetId)): stale signal; active asset is \(activeAssetId)"
            )
            return
        }
        inputClosed = true
        resumeAllAppendWaiters()
    }

    /// Stop all transcription work (e.g., episode ended or user switched).
    func stop() {
        activeTask?.cancel()
        activeTask = nil
        activeAssetId = nil
        activePodcastId = nil
        latestSnapshot = nil
        chunkCounter = 0
        appendedShards = []
        loopRunning = false
        preemption = nil
        // Close input and release any suspended waiter so a loop that is
        // parked on `waitForMoreShards()` returns promptly when the task
        // is cancelled.
        inputClosed = true
        resumeAllAppendWaiters()
    }

    /// playhead-5uvz.5 (Gap-6): Stop transcription for a specific asset
    /// without disturbing other engine state.
    ///
    /// `AnalysisJobRunner.run` calls this from its 5-minute zero-coverage
    /// timeout branch. Before the fix, the runner would return
    /// `.failed("transcription:zeroCoverage")` while leaving
    /// `TranscriptEngineService` running in the background. The orphan's
    /// subsequent `transcript_chunks` writes and
    /// `analysis_assets.fastTranscriptCoverageEndTime` updates targeted
    /// an `analysisAssetId` whose owning scheduler had already moved on
    /// — so the asset's coverage advanced out-of-band after the job row
    /// was marked failed, confusing both the coverage-guard recovery
    /// path and the partial-coverage gate.
    ///
    /// Contract:
    /// - Cancels the underlying SpeechAnalyzer task if `analysisAssetId`
    ///   matches the active asset. (Mismatch is a no-op — a stale stop
    ///   call must not tear down an unrelated session.)
    /// - Drops any in-flight `appendedShards` tagged for the active
    ///   session (whose asset id, on a match, is the one being stopped).
    /// - Records the asset id so any late `transcribeShard` writes,
    ///   late `appendShards` calls, or late `emitEvent(.completed/...)`
    ///   for that asset are dropped instead of persisted.
    /// - Resumes any waiter parked in `waitForMoreShards()` so the loop
    ///   can observe cancellation and exit promptly.
    ///
    /// Idempotent: stopping an already-stopped asset is a no-op aside
    /// from re-asserting the gate. A subsequent
    /// `startTranscription(...)` for the same asset clears the stopped
    /// flag — explicit re-run is allowed.
    func stopTranscription(analysisAssetId: String) {
        // Always record the stopped asset, even on a stale call. A
        // streaming producer that already started racing more shards
        // toward this asset must see the gate on its next append even
        // if the active session has rotated away.
        stoppedAssetIds.insert(analysisAssetId)

        // Mismatch: don't tear down an unrelated active session. The
        // gate on `stoppedAssetIds` still covers the late-write case
        // even though we don't cancel.
        guard activeAssetId == analysisAssetId else {
            logger.info(
                "stopTranscription(\(analysisAssetId)): asset is not active; gate set but no task to cancel"
            )
            return
        }

        logger.info("stopTranscription(\(analysisAssetId)): cancelling active task")

        activeTask?.cancel()
        activeTask = nil
        activeAssetId = nil
        activePodcastId = nil
        latestSnapshot = nil
        chunkCounter = 0
        // Drop the queued append backlog for the now-stopped session.
        // Anything still queued was destined for the asset id we just
        // gated; the gate would drop it later anyway, but emptying the
        // queue here avoids spinning the loop through dead work.
        appendedShards = []
        loopRunning = false
        preemption = nil
        // Close input and wake any waiter so a loop parked on
        // `waitForMoreShards()` exits promptly. Cancellation is the
        // primary stop signal but the wake makes the exit deterministic.
        inputClosed = true
        resumeAllAppendWaiters()
    }

    /// Clear the active task reference when the loop completes,
    /// so appendShards knows to start a new loop.
    private func clearActiveTask() {
        activeTask = nil
    }

    /// Whether transcription is currently in progress.
    var isActive: Bool {
        activeTask != nil && activeTask?.isCancelled == false
    }

#if DEBUG
    /// playhead-8m2w test seam: hand out the live transcription task so a test
    /// can cancel it the way a *silently dying producer* would — with none of
    /// the `resumeAllAppendWaiters()` pairings that every production cancel
    /// site in this file hand-writes — and then await its exit. There is no
    /// production caller: every production path that cancels this task is
    /// reached through `stop`, `stopTranscription` or `startTranscription`.
    func activeTaskForTesting() -> Task<Void, Never>? {
        activeTask
    }

    /// playhead-8m2w test seam: non-zero exactly while the drain loop is
    /// parked in `waitForMoreShards()`. Lets a test prove it is testing the
    /// PARKED case rather than racing the loop and exiting at the pre-park
    /// `Task.isCancelled` check — which would pass even against the bug.
    var appendWaiterCountForTesting: Int {
        appendWaiters.count
    }

    /// playhead-ngev (review r1) test seam: whether `analysisAssetId` is
    /// currently fenced by `stopTranscription`.
    ///
    /// It exists because `AnalysisJobRunner.shouldStopEngine(after:)` is a
    /// pure function and can be proven on its own, but its CALL SITE cannot:
    /// the runner holds a concrete `TranscriptEngineService` with no protocol
    /// seam, so `stopTranscription` cannot be spied on and a build that simply
    /// stopped consulting the predicate would pass every test that existed.
    /// That call site is the one that decides whether the runner cancels the
    /// listener's own transcription, so leaving it asserted only by a unit
    /// test of the predicate is the gap this closes.
    func isStoppedForTesting(analysisAssetId: String) -> Bool {
        stoppedAssetIds.contains(analysisAssetId)
    }

    /// playhead-8ysk test seam: push a `.failed` through the emit path for an
    /// arbitrary asset id.
    ///
    /// The stop-gate branch is not reachable from the loop by ordinary means:
    /// `startTranscription` rescinds a stop for the asset it is starting, so
    /// no production sequence can have a fresh loop emit for an asset that is
    /// still gated. The seam exercises the gate directly rather than leaving
    /// the branch asserted only by the compiler's exhaustiveness check.
    ///
    /// playhead-ngev: `fromCurrentRun` selects which side of the exemption is
    /// under test. It is threaded through the REAL `emitFailure` — as a
    /// generation that either does or does not match `runGeneration` — so the
    /// seam cannot pass while the production comparison is broken.
    func emitFailedForTesting(
        analysisAssetId: String,
        reason: TranscriptFailureReason,
        fromCurrentRun: Bool = false
    ) {
        emitFailure(
            reason,
            analysisAssetId: analysisAssetId,
            generation: fromCurrentRun ? runGeneration : runGeneration - 1
        )
    }
#endif

    func events() -> AsyncStream<TranscriptEngineEvent> {
        let id = UUID()
        return AsyncStream { continuation in
            self.eventContinuations[id] = continuation
            continuation.onTermination = { @Sendable _ in
                Task { [weak self] in
                    await self?.removeEventContinuation(id: id)
                }
            }
        }
    }

    // MARK: - Transcription loop

    private func runTranscriptionLoop(
        shards: [AnalysisShard],
        analysisAssetId: String,
        generation: Int
    ) async {
        loopRunning = true
        defer {
            loopRunning = false
            // playhead-01t8: clear the per-job preemption context so a
            // stale signal reference does not linger across jobs. The
            // next `startTranscription` call assigns its own context.
            preemption = nil
        }
        logger.info("Starting transcription loop: \(shards.count) shards for asset \(analysisAssetId)")
        let loopStart = ContinuousClock.now

        // playhead-8ysk: the errors this loop used to destroy. The catches
        // below still `continue` — partial coverage is better than none — but
        // they no longer throw the diagnosis away with the shard.
        var shardFailures: [TranscriptFailureReason] = []
        // playhead-8ysk (review r3): this run's own production, and
        // (review r4) this run's own progress, which is not the same quantity.
        // See `ShardProgress` for both rationales.
        //
        // playhead-ngev: LOCALS, not actor fields. Their previous doc comments
        // asserted per-pass scoping that an actor field cannot give them,
        // because `startTranscription` spawns the successor without awaiting
        // the predecessor and cancellation is cooperative — two loop bodies
        // overlap, and a cancelled predecessor could credit the successor.
        var chunksInsertedThisRun = 0
        var shardsCompletedThisRun = 0

        // playhead-ngev: REPORT THE INTERRUPTION INSTEAD OF RETURNING IN
        // SILENCE.
        //
        // Thirteen returns below used to leave the loop without a word, and
        // eleven of them could do it while `shardFailures` held fully
        // classified diagnoses that then went out of scope. The runner, still
        // parked in its task group, waited out the full 300 s timeout and
        // journaled `asr_failed` — so an ordinary scrub, an ordinary speed
        // change, or starting a different episode was recorded as an ASR
        // failure, five minutes after the fact.
        //
        // Which reason to send:
        //   * `shardFailures` non-empty — the engine already knows what went
        //     wrong; send `dominantFailure`, the same value the total-failure
        //     gate sends, re-stamped as interrupted.
        //   * empty — nothing went wrong, so the REASON WE STOPPED is the
        //     whole diagnosis, and it is a fact the loop holds with certainty.
        //
        // Either way the termination says the run was cut short, which is what
        // stops `AnalysisCoordinator` from tearing down a session whose
        // successor loop is already running.
        func reportInterruption(_ interruption: TranscriptFailureClass) {
            // A gated asset means `stopTranscription` is the PROXIMATE cause
            // and the cancellation is only its mechanism: it cancels the task
            // and fences the asset in the same call, so whichever check the
            // loop happens to reach first, the honest name is `stopped`. Left
            // to the arms alone the same event would report `cancelled` or
            // `stopped` depending on where the loop was parked, which is an
            // implementation detail no bundle reader can act on. Only
            // `.cancelled` is upgraded — a preempt has its own proximate cause.
            let named: TranscriptFailureClass =
                interruption == .cancelled && stoppedAssetIds.contains(analysisAssetId)
                ? .stopped
                : interruption
            let reason = shardFailures.isEmpty
                ? TranscriptFailureReason(failureClass: named, termination: .interrupted)
                : Self.dominantFailure(shardFailures).interrupted()
            emitFailure(reason, analysisAssetId: analysisAssetId, generation: generation)
        }

        guard !shards.isEmpty || !appendedShards.isEmpty else {
            logger.warning("No shards to transcribe")
            // playhead-8ysk: this used to return in silence. The runner then
            // sat in `withTaskGroup` waiting for a `.completed` that could
            // never arrive, until its 300 s timeout fired — one of the two
            // paths behind `task_expired`.
            emitFailure(
                TranscriptFailureReason(failureClass: .noShards),
                analysisAssetId: analysisAssetId,
                generation: generation
            )
            return
        }

        // playhead-se2h: THE NATURAL RETRY TRIGGER.
        //
        // `prepareFastModel()` returns immediately (`.alreadyLoaded`) on
        // the overwhelmingly common healthy path, so this costs a single
        // actor hop per run. When the launch load failed it is the ONLY
        // thing on the device that ever tries again — before this call
        // existed, `loadFastModel()` had exactly one invocation per
        // install and a single transient failure at launch made every
        // subsequent run take the branch below, forever.
        //
        // The retry is bounded inside `SpeechService`
        // (`maxLoadAttemptsPerEpoch`), so a genuinely unavailable asset
        // does not turn every scheduled run into a download attempt: once
        // the budget is spent this returns `.budgetExhausted` without
        // touching the recognizer, and the run falls through to the same
        // named failure it reported before.
        let readiness = await speechService.prepareFastModel()
        // A load cut short by a scrub or a stop is an INTERRUPTION, not a
        // verdict on the speech stack. Routing it through
        // `reportInterruption` keeps the name honest (and lets the gated-asset
        // arm upgrade it to `.stopped`), where reporting
        // `speech_engine_not_ready` would blame the engine for the user's
        // scrub — the same mislabelling playhead-ngev removed one layer down.
        if readiness == .cancelled {
            logger.info("Speech model load cancelled — reporting the interruption, not an engine fault")
            reportInterruption(.cancelled)
            return
        }
        guard readiness.isReady else {
            logger.error(
                "Speech engine not ready (\(readiness.rawValue, privacy: .public)) — aborting transcription"
            )
            // playhead-8ysk: this used to be silent. It is the observable
            // end of the launch-time `loadFastModel()` failure — the
            // failure whose empty catch used to claim, falsely, that the
            // transcript engine would surface it when first used.
            emitFailure(
                TranscriptFailureReason(failureClass: .speechEngineNotReady),
                analysisAssetId: analysisAssetId,
                generation: generation
            )
            return
        }

        // playhead-mptr: LOAD WHAT THE PERSISTED CHUNKS ACTUALLY BACK, ONCE.
        //
        // The loop below used to walk shards in playhead-proximity order alone,
        // which on a re-run means starting over from the beginning of the
        // episode. `transcribeShard` runs the full ASR pass and deduplicates by
        // fingerprint only afterwards — the dedup saves a row insert, not the
        // transcription — so a re-run of an asset already holding 45 minutes of
        // transcript paid for 45 minutes of ASR before reaching one second of
        // new audio. `AnalysisJobRunner` caps this whole stage at a flat 300 s
        // (an unconditional `Task.sleep`, not an inactivity watchdog), so past
        // a certain amount of coverage the cap always won first: zero chunks
        // persisted, watermark frozen, and the next run facing the same wall.
        //
        // SELF-REINFORCING, which is what makes it a ceiling rather than a
        // slowdown: every second of coverage earned makes the next run more
        // expensive, so an episode that crosses the line is stranded forever.
        // A field episode sat at 68.7 % across five consecutive attempts, each
        // journaled `engine_silent_timeout` with `chunks_persisted = 0`.
        //
        // Two reads, both indexed, both once per run rather than per shard.
        // On failure the index is empty and the watermark nil, so every shard
        // sorts as uncovered and the order is exactly the pre-mptr order —
        // the safe direction to fail in.
        //
        // playhead-6r4z: THE READ IS BOTH PASSES, and it was `pass = 'fast'`
        // alone until this bead — which made the fix above partly defeat itself.
        // Audio the FINAL pass covers has no fast row to point at, so it failed
        // the artifact test, sorted UNCOVERED, and floated to the FRONT of the
        // very pass minted to read the audio behind it. On the 2026-08-03 pull
        // that is 215 shards / 6,450 s of re-read across seven of twelve assets,
        // and on 48E903D7 the re-read prefix beats the new audio 1,230 s to
        // 103 s inside a flat 300 s cap. The moved shards are densely backed —
        // union fill min 0.610 / median 0.906, none under 0.25, against a
        // minimum of 0.266 among the shards mptr already sorted last — so this
        // widening moves audio a real row genuinely covers, not audio a sliver
        // touches. The WATERMARK below is deliberately still the fast one; see
        // `TranscriptCoverageIndex`'s header for why the two halves differ, and
        // `playhead-9j94` for what that leaves on the table.
        var coverageIndex = TranscriptCoverageIndex.empty
        var coverageWatermark: Double?
        do {
            coverageIndex = TranscriptCoverageIndex(
                transcribedRegion: try await store.fetchTranscribedRegion(assetId: analysisAssetId)
            )
            coverageWatermark = try await store.fetchFastTranscriptCoverageEndTime(id: analysisAssetId)
        } catch {
            logger.warning("""
                Could not load existing transcript coverage for asset \(analysisAssetId): \(error). \
                Transcribing every shard.
                """)
        }
        // Prioritize shards by proximity to the playhead, then float the audio
        // nothing backs yet to the front.
        //
        // playhead-mptr: NOTHING IS SKIPPED — every shard still runs, and the
        // duplicate-fingerprint arm of `transcribeShard` still performs its
        // `speakerId` / `avgConfidence` upgrades on the covered ones. Only the
        // ORDER changes, so the stage's 300 s budget is spent on unread audio
        // before it is spent on audio we already hold.
        //
        // The artifact test is what makes this safe in the direction review
        // playhead-rfu-aac H3 cared about. H3 removed a watermark-only filter
        // because `fastTranscriptCoverageEndTime` is a high-water REACH, not a
        // promise every second below it was transcribed — behind-playhead shards
        // can sit under it having never run, and playhead-0sro documents the
        // watermark outliving its chunks entirely. Those shards have no chunk to
        // point at, so they sort as UNCOVERED and run first, which is stronger
        // than the pre-mptr behaviour rather than weaker. And because this is a
        // reordering, a wrong answer in either direction costs latency, never
        // coverage.
        let prioritized = coverageIndex.orderingUncoveredFirst(
            prioritizeShards(shards),
            watermark: coverageWatermark
        )
        let uncoveredShardCount = prioritized.prefix {
            !coverageIndex.isShardAlreadyTranscribed(
                shardStart: $0.startTime,
                shardEnd: $0.startTime + $0.duration,
                watermark: coverageWatermark
            )
        }.count

        for shard in prioritized {
            guard !Task.isCancelled else {
                logger.info("Transcription cancelled")
                reportInterruption(.cancelled)
                return
            }

            do {
                let progress = try await transcribeShard(
                    shard,
                    analysisAssetId: analysisAssetId
                )
                chunksInsertedThisRun += progress.chunksInserted
                shardsCompletedThisRun += progress.finished ? 1 : 0
            } catch is CancellationError {
                logger.info("Transcription cancelled during shard \(shard.id)")
                reportInterruption(.cancelled)
                return
            } catch is TranscriptEnginePreempted {
                logger.info("Transcription preempted at safe point after shard \(shard.id) [end=\(String(format: "%.1f", shard.startTime + shard.duration))s]")
                reportInterruption(.preempted)
                return
            } catch is TranscriptEngineStopped {
                // playhead-5uvz.5: caller invoked
                // `stopTranscription(analysisAssetId:)`. Exit the loop
                // without emitting `.completed`.
                //
                // playhead-ngev: but NOT without a word. The asset is gated,
                // which used to mean the report would be dropped at the emit
                // gate anyway — that drop is what deleted the only classified
                // account of the failure the runner was about to journal. The
                // gate now lets the current run's `.failed` through.
                logger.info("Transcription stopped for asset \(analysisAssetId) during shard \(shard.id)")
                reportInterruption(.stopped)
                return
            } catch {
                logger.error("""
                    Transcription failed for shard \(shard.id) \
                    [start=\(String(format: "%.2f", shard.startTime))s, \
                    duration=\(String(format: "%.2f", shard.duration))s, \
                    samples=\(shard.sampleCount), \
                    episode=\(shard.episodeID)]: \(error)
                    """)
                shardFailures.append(TranscriptFailureReason.classify(error))
                // Continue with next shard — partial coverage is better than none.
                continue
            }
        }

        // Drain any shards that were appended while we were processing,
        // and wait for either more shards or an explicit end-of-input
        // signal before emitting `.completed`. Emitting on a momentarily
        // empty queue was the root cause of analysisState=complete
        // races against a streaming decoder that hadn't finished yet.
        drainLoop: while true {
            while !appendedShards.isEmpty {
                let newBatch = appendedShards
                appendedShards = []

                let newPrioritized = prioritizeShards(newBatch)

                for shard in newPrioritized {
                    guard !Task.isCancelled else {
                        logger.info("Transcription cancelled during appended batch")
                        reportInterruption(.cancelled)
                        return
                    }
                    do {
                        let progress = try await transcribeShard(
                            shard, analysisAssetId: analysisAssetId
                        )
                        chunksInsertedThisRun += progress.chunksInserted
                        shardsCompletedThisRun += progress.finished ? 1 : 0
                    } catch is CancellationError {
                        logger.info("Transcription cancelled during appended shard \(shard.id)")
                        reportInterruption(.cancelled)
                        return
                    } catch is TranscriptEnginePreempted {
                        logger.info("Transcription preempted at safe point after appended shard \(shard.id)")
                        reportInterruption(.preempted)
                        return
                    } catch is TranscriptEngineStopped {
                        logger.info("Transcription stopped for asset \(analysisAssetId) during appended shard \(shard.id)")
                        reportInterruption(.stopped)
                        return
                    } catch {
                        logger.error("""
                            Transcription failed for appended shard \(shard.id) \
                            [start=\(String(format: "%.2f", shard.startTime))s]: \(error)
                            """)
                        shardFailures.append(TranscriptFailureReason.classify(error))
                        continue
                    }
                }
            }

            // Backlog is empty. If the caller has signalled end-of-input,
            // we're done. Otherwise suspend until someone appends more
            // shards, calls finishAppending, or stops the engine.
            if inputClosed { break drainLoop }
            if Task.isCancelled {
                reportInterruption(.cancelled)
                return
            }
            await waitForMoreShards()
        }

        // If the task was cancelled while we were suspended on a waiter,
        // exit without emitting `.completed`. Cancellation is not a
        // legitimate end-of-input.
        if Task.isCancelled {
            reportInterruption(.cancelled)
            return
        }

        // playhead-5uvz.5: a stop landing while we were parked on a
        // waiter is also not a legitimate end-of-input. Bail before
        // running the shard-0 backfill or emitting `.completed`.
        if stoppedAssetIds.contains(analysisAssetId) {
            logger.info("Transcription loop exiting for stopped asset \(analysisAssetId) — no .completed emitted")
            reportInterruption(.stopped)
            return
        }

        // Verify the first shard was transcribed. If the first 30s is missing,
        // transcribe shard 0 explicitly.
        //
        // playhead-rfu-aac M1: previously these store + transcribe calls used
        // `try?`, which silently absorbed both real persistence errors AND
        // TranscriptEngineStopped (the gate signal that drains the loop on
        // `stopTranscription`). The result was that a stop landing exactly
        // at this seam would emit `.completed` instead of bailing. Use
        // explicit do/catch so:
        //   - TranscriptEngineStopped exits the loop without emitting completed
        //   - CancellationError exits the loop without emitting completed
        //   - real errors are logged loudly but the backfill is best-effort,
        //     so we still let the loop emit completed (matching prior intent)
        if let firstShard = shards.first(where: { $0.id == 0 }) {
            let allChunks: [TranscriptChunk]
            do {
                allChunks = try await store.fetchTranscriptChunks(assetId: analysisAssetId)
            } catch is CancellationError {
                logger.info("Transcript chunk fetch cancelled — exiting before shard-0 backfill")
                reportInterruption(.cancelled)
                return
            } catch {
                logger.error("Failed to fetch transcript chunks for shard-0 backfill check: \(error)")
                allChunks = []
            }
            let hasEarlyChunk = allChunks.contains { $0.startTime < 30 }
            if !hasEarlyChunk {
                logger.warning("First 30s missing — transcribing shard 0")
                do {
                    let progress = try await transcribeShard(
                        firstShard, analysisAssetId: analysisAssetId
                    )
                    chunksInsertedThisRun += progress.chunksInserted
                    shardsCompletedThisRun += progress.finished ? 1 : 0
                } catch is CancellationError {
                    logger.info("Shard-0 backfill cancelled — exiting without .completed")
                    reportInterruption(.cancelled)
                    return
                } catch is TranscriptEnginePreempted {
                    logger.info("Shard-0 backfill preempted — exiting without .completed")
                    reportInterruption(.preempted)
                    return
                } catch is TranscriptEngineStopped {
                    logger.info("Shard-0 backfill stopped for asset \(analysisAssetId) — exiting without .completed")
                    reportInterruption(.stopped)
                    return
                } catch {
                    logger.error("Shard-0 backfill failed: \(error)")
                    // playhead-8ysk (review r4): DELIBERATELY NOT recorded into
                    // `shardFailures`, and the reason is an invariant rather
                    // than an oversight — round 4 tried adding it here and the
                    // suite rejected the change.
                    //
                    // This is the third `transcribeShard` call site, and unlike
                    // the other two it can only ever re-attempt a shard the
                    // main loop already attempted: it selects
                    // `shards.first(where: { $0.id == 0 })` out of the same
                    // array the loop iterated in full. So whatever it throws,
                    // that shard's class is already in `shardFailures`.
                    // Appending would add no diagnosis and would turn
                    // `failedShardCount` from a count of failed SHARDS into a
                    // count of failed ATTEMPTS — double-counting shard 0, and
                    // breaking the on-device duration-proxy invariant that
                    // `DiagnosticsBundleFailureClassTests` reasons about ("in a
                    // total failure it equals the shard count").
                    // `shardZeroBackfillDoesNotInflateTheFailedShardCount`
                    // pins it.
                    //
                    // Best-effort: continue to .completed below since the
                    // rest of the transcript is already persisted.
                }
            }
        }

        // playhead-0sro: reconcile the watermark against persisted chunk
        // coverage BEFORE emitting `.completed` — the runner reads the
        // asset row the moment it observes that event.
        await reconcileCoverageAtCompletion(analysisAssetId: analysisAssetId)

        let loopElapsed = ContinuousClock.now - loopStart

        // playhead-8ysk: STOP LYING ABOUT TOTAL FAILURE.
        //
        // Every shard could fail and the loop would still fall through to
        // `.completed`. The runner treats that event as "coverage is durable"
        // and queues downstream work; the only trace of the failure was a
        // coverage number of zero, and by then no `Error` was in scope
        // anywhere — which is exactly why `asr_failed` names an absence
        // rather than a cause.
        //
        // The bar is deliberately "produced nothing", not "any shard failed".
        // A run that persisted some chunks genuinely did partial work, and the
        // catches above keep continuing on purpose; downgrading those to
        // failures would discard usable transcript.
        //
        // "Produced nothing" means THIS RUN produced nothing, which is why the
        // counts come from this run's own tallies and not from the store. See
        // `ShardProgress.chunksInserted`: `fetchTranscriptChunks(assetId:)`
        // is cumulative over the asset's whole lifetime and the asset row is
        // reused across passes, so asking it here would have let a retry of a
        // partly-transcribed asset report `.completed` over a total failure —
        // reinstating the very lie this block removes, in the retry case this
        // bead was filed for.
        //
        // BOTH tallies are required (review r4). Rows inserted is not a
        // measure of work done: a shard that yields no segments, and a shard
        // whose segments all dedup against an earlier pass, both finish
        // successfully and insert nothing. Gating on the insert count alone
        // therefore reports a total failure for a re-run that transcribed
        // everything it was asked to and merely also hit one bad shard — see
        // `shardsCompletedThisRun`. A run is a total failure only when it
        // wrote nothing AND carried no shard to a clean finish.
        if !shardFailures.isEmpty, chunksInsertedThisRun == 0, shardsCompletedThisRun == 0 {
            let reason = Self.dominantFailure(shardFailures)
            logger.error("""
                Transcription produced nothing for asset \(analysisAssetId) in \(loopElapsed): \
                \(reason.failureClass.rawValue) \
                across \(reason.failedShardCount) shard(s)
                """)
            emitFailure(reason, analysisAssetId: analysisAssetId, generation: generation)
            return
        }

        // playhead-mptr: the uncovered count is the diagnostic that says where
        // the 300 s stage budget went. `uncovered` shards ran FIRST, so on a
        // run that the cap cuts short they are the ones that got read. A
        // partly-transcribed asset reporting `uncovered: 0` means the artifact
        // test declined to fire and the stall can recur.
        logger.info("""
            Transcription loop complete for asset \(analysisAssetId) in \(loopElapsed) \
            [shards: \(prioritized.count), uncovered-first: \(uncoveredShardCount)]
            """)
        emitEvent(.completed(analysisAssetId: analysisAssetId))
    }

    /// playhead-8ysk: reduce a run's per-shard failures to the one class worth
    /// exporting — the most frequent, ties broken by first occurrence.
    ///
    /// A run that dies at shard 0 for one reason and at shards 1..n for
    /// another should report the reason that actually characterises it, not
    /// whichever happened to be last.
    static func dominantFailure(_ failures: [TranscriptFailureReason]) -> TranscriptFailureReason {
        guard let first = failures.first else {
            return TranscriptFailureReason(failureClass: .unknown)
        }
        var counts: [TranscriptFailureClass: Int] = [:]
        for failure in failures {
            counts[failure.failureClass, default: 0] += 1
        }
        // `max(by:)` over the ORDER OF OCCURRENCE, not over the dictionary,
        // so ties resolve to the earliest class deterministically rather than
        // to whatever the hash seed puts first.
        var seen: Set<TranscriptFailureClass> = []
        let ordered = failures.map(\.failureClass).filter { seen.insert($0).inserted }
        let dominantClass = ordered.max(by: { (counts[$0] ?? 0) < (counts[$1] ?? 0) }) ?? first.failureClass
        let exemplar = failures.first { $0.failureClass == dominantClass } ?? first
        return TranscriptFailureReason(
            failureClass: dominantClass,
            code: exemplar.code,
            failedShardCount: failures.count
        )
    }

    // MARK: - Append-wait plumbing

    /// Suspend until another actor touches the append queue or the
    /// session ends. The resume side is any of `appendShards`,
    /// `finishAppending`, `stop`, or a fresh `startTranscription`.
    ///
    /// playhead-8m2w: the drain loop tests `Task.isCancelled` one line before
    /// it parks here, so a cancel landing in that window used to park anyway —
    /// and a parked continuation cannot be unwound by cancellation
    /// (playhead-xc6b), so the loop stayed suspended for the lifetime of the
    /// process. It was only ever saved by discipline: every
    /// `activeTask?.cancel()` in this file is hand-paired with a
    /// `resumeAllAppendWaiters()`. That pairing does not survive a producer
    /// that dies *silently* — `shardConsumerTask` stalling inside
    /// `featureService.extractAndPersist` so `finishAppending` is never
    /// reached, with no `stop()` behind it — and it silently rots the first
    /// time a future cancel site forgets the pairing.
    ///
    /// The cancellation handler closes both. It resumes through the existing
    /// `resumeAllAppendWaiters()` rather than picking out this one waiter on
    /// purpose: that function drains the list *before* resuming and runs on
    /// this actor, so actor isolation is already the once guard and no second
    /// hand-written one is introduced. Waking a sibling waiter early is
    /// harmless — a waiter that wakes with the queue still empty and input
    /// still open simply re-parks, which is the behaviour that function is
    /// already documented and used for.
    ///
    /// The handler is the load-bearing half, and it is airtight on its own.
    /// `onCancel` fires on the canceller's thread and has to hop back here to
    /// reach `appendWaiters`, but that hop *cannot be serviced until this
    /// actor job suspends* — which happens only after the append. So the
    /// waiter is always already in the list by the time the hop drains it,
    /// including for a cancel that lands before the park:
    /// `withTaskCancellationHandler` invokes `onCancel` immediately when the
    /// task is already cancelled at install time, so that ordering schedules
    /// the same hop.
    ///
    /// The `Task.isCancelled` check inside the continuation body is defence in
    /// depth, not a hole being plugged. What it buys is independence from that
    /// already-cancelled-fires-immediately contract, and one less actor hop on
    /// the common path: an already-cancelled loop never parks in the first
    /// place. (Same shape as `BoundedContinuation`'s settle-before-attach
    /// phase.) It is deliberately kept even though no test can distinguish it
    /// from the handler alone — the window it covers is a few instructions
    /// wide and not reachable from a test seam.
    private func waitForMoreShards() async {
        await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                if Task.isCancelled {
                    continuation.resume()
                } else {
                    appendWaiters.append(continuation)
                }
            }
        } onCancel: {
            Task { [weak self] in
                await self?.resumeAllAppendWaiters()
            }
        }
    }

    /// Wake every suspended waiter. Safe to call repeatedly; it drains
    /// the continuation list before resuming so a waiter that
    /// immediately re-suspends (because `appendedShards` is still empty
    /// and `inputClosed` is still false) does not race a resume from a
    /// previous wake cycle.
    private func resumeAllAppendWaiters() {
        let waiters = appendWaiters
        appendWaiters = []
        for continuation in waiters {
            continuation.resume()
        }
    }

    // MARK: - Single shard transcription

    /// - Returns: what this attempt produced (playhead-ngev). The caller adds
    ///   it to the run's own totals; nothing about one shard's work is stored
    ///   on the actor, so an overlapping predecessor cannot credit a successor.
    private func transcribeShard(
        _ shard: AnalysisShard,
        analysisAssetId: String
    ) async throws -> ShardProgress {
        try Task.checkCancellation()
        // playhead-5uvz.5: per-shard stopped check at entry. The check
        // re-runs after every await point inside the shard so a
        // `stopTranscription(analysisAssetId:)` that lands mid-shard
        // exits before any subsequent store write or event emission.
        try checkStopped(analysisAssetId: analysisAssetId)

        // Run Apple Speech transcription.
        let segments = try await speechService.transcribe(shard: shard, podcastId: activePodcastId)

        // The await above can release the actor; a stop call could land
        // here. Re-check before any persistence work.
        try checkStopped(analysisAssetId: analysisAssetId)

        guard !segments.isEmpty else {
            logger.debug("No segments from shard \(shard.id) — silence or noise")
            // Still update coverage so we don't re-process.
            try await updateCoverage(
                analysisAssetId: analysisAssetId,
                endTime: shard.startTime + shard.duration
            )
            // playhead-8ysk (review r4): a silent shard produced no row but it
            // DID finish, and the watermark it just advanced is durable. Count
            // it, or a music-heavy run reads as having produced nothing.
            return ShardProgress(chunksInserted: 0, finished: true)
        }

        // Convert segments to TranscriptChunks and persist. Metadata upgrades on
        // duplicate fingerprints re-emit the upgraded chunk but must not be
        // inserted as a new row.
        var chunksToInsert: [TranscriptChunk] = []
        var emittedChunks: [TranscriptChunk] = []

        for segment in segments {
            try Task.checkCancellation()
            try checkStopped(analysisAssetId: analysisAssetId)

            let fingerprint = computeFingerprint(
                text: segment.text,
                startTime: segment.startTime,
                endTime: segment.endTime
            )

            // Dedup: preserve the existing row, but let later passes fill
            // missing speaker labels and upgrade weak-anchor metadata when the
            // same text/timing arrives with richer recovery text.
            if let existingChunk = try await store.fetchTranscriptChunk(
                analysisAssetId: analysisAssetId,
                segmentFingerprint: fingerprint
            ) {
                let mergedMetadata = mergedWeakAnchorMetadata(
                    existing: existingChunk.weakAnchorMetadata,
                    candidate: segment.weakAnchorMetadata
                )
                var didUpdate = false
                var upgradedSpeakerId = existingChunk.speakerId
                var upgradedAvgConfidence = existingChunk.avgConfidence
                if mergedMetadata != existingChunk.weakAnchorMetadata {
                    didUpdate = try await store.updateTranscriptChunkWeakAnchorMetadata(
                        analysisAssetId: analysisAssetId,
                        segmentFingerprint: fingerprint,
                        weakAnchorMetadata: mergedMetadata,
                        speakerIdIfMissing: segment.speakerId
                    )
                    upgradedSpeakerId = existingChunk.speakerId ?? segment.speakerId
                } else if let speakerId = segment.speakerId {
                    didUpdate = try await store.updateTranscriptChunkSpeakerIdIfMissing(
                        analysisAssetId: analysisAssetId,
                        segmentFingerprint: fingerprint,
                        speakerId: speakerId
                    )
                    if didUpdate {
                        upgradedSpeakerId = existingChunk.speakerId ?? speakerId
                    }
                }
                let didUpdateConfidence = try await store.updateTranscriptChunkAvgConfidenceIfMissing(
                    analysisAssetId: analysisAssetId,
                    segmentFingerprint: fingerprint,
                    avgConfidence: segment.avgConfidence
                )
                if didUpdateConfidence {
                    didUpdate = true
                    if existingChunk.avgConfidence == nil {
                        upgradedAvgConfidence = TranscriptChunk.sanitizedAvgConfidence(segment.avgConfidence)
                    }
                }
                if didUpdate {
                    emittedChunks.append(
                        TranscriptChunk(
                            id: existingChunk.id,
                            analysisAssetId: existingChunk.analysisAssetId,
                            segmentFingerprint: existingChunk.segmentFingerprint,
                            chunkIndex: existingChunk.chunkIndex,
                            startTime: existingChunk.startTime,
                            endTime: existingChunk.endTime,
                            text: existingChunk.text,
                            normalizedText: existingChunk.normalizedText,
                            pass: existingChunk.pass,
                            modelVersion: existingChunk.modelVersion,
                            transcriptVersion: existingChunk.transcriptVersion,
                            atomOrdinal: existingChunk.atomOrdinal,
                            weakAnchorMetadata: mergedMetadata,
                            speakerId: upgradedSpeakerId,
                            avgConfidence: upgradedAvgConfidence
                        )
                    )
                } else {
                    logger.debug("Skipping duplicate segment: \(fingerprint.prefix(8))")
                }
                continue
            }

            let chunk = TranscriptChunk(
                id: UUID().uuidString,
                analysisAssetId: analysisAssetId,
                segmentFingerprint: fingerprint,
                chunkIndex: chunkCounter,
                startTime: segment.startTime,
                endTime: segment.endTime,
                text: segment.text,
                normalizedText: normalizeText(segment.text),
                pass: segment.passType.rawValue,
                modelVersion: config.modelVersion,
                transcriptVersion: nil,
                atomOrdinal: nil,
                weakAnchorMetadata: segment.weakAnchorMetadata,
                speakerId: segment.speakerId,
                avgConfidence: segment.avgConfidence
            )
            chunksToInsert.append(chunk)
            emittedChunks.append(chunk)
            chunkCounter += 1
        }

        // playhead-5uvz.5: final pre-persistence check. The previous
        // `await store.fetchTranscriptChunk(...)` / `await
        // store.updateTranscriptChunkWeakAnchorMetadata(...)` calls
        // inside the segment loop release the actor; a
        // `stopTranscription` could land before the batch insert. Bail
        // before writing rows or emitting events for a stopped asset.
        try checkStopped(analysisAssetId: analysisAssetId)

        // Batch-insert to SQLite.
        var progress = ShardProgress()
        if !chunksToInsert.isEmpty {
            // playhead-8ysk (review r3): count AFTER the insert returns, so a
            // throwing insert (the `persistence_failed` class) does not credit
            // this run with rows it never wrote.
            //
            // playhead-6av0 (review r1): credit the count the STORE reports, not
            // `chunksToInsert.count`. `insertTranscriptChunk` is now
            // `INSERT OR IGNORE`, so a batch carrying two segments that hash to
            // one `(asset, pass, fingerprint)` writes ONE row. Crediting the
            // batch size would let `chunksInsertedThisRun` claim rows the
            // database does not hold — and that counter is what decides, at the
            // end of the run, whether a shard-failure set means "produced
            // nothing" or "produced something".
            let inserted = try await store.insertTranscriptChunks(chunksToInsert)
            progress.chunksInserted = inserted
        }
        if !emittedChunks.isEmpty {
            emitEvent(.chunksPersisted(analysisAssetId: analysisAssetId, chunks: emittedChunks))
        }

        // Update coverage watermark.
        let shardEnd = shard.startTime + shard.duration
        try await updateCoverage(
            analysisAssetId: analysisAssetId,
            endTime: shardEnd
        )

        // playhead-8ysk (review r4): the shard is finished and everything it
        // produced is durable. Marked BEFORE the preemption safe point below
        // on purpose — a preempted loop returns without reaching the
        // total-failure gate, so the ordering cannot matter there, and
        // counting after the throw would under-report work that did land.
        //
        // playhead-ngev: with the progress RETURNED rather than accumulated on
        // the actor, a preempt throw does discard this shard's contribution.
        // That is inert for the same reason: the preempted arm exits before
        // the gate reads either total.
        progress.finished = true

        logger.info("Wrote \(emittedChunks.count) chunks for shard \(shard.id) [\(String(format: "%.1f", shard.startTime))-\(String(format: "%.1f", shardEnd))s]")

        // playhead-01t8 safe point (c): post-TranscriptChunk. Every
        // chunk in `emittedChunks` is durable in SQLite and the
        // coverage watermark has advanced. If a higher-lane admission
        // has flipped the preemption signal, acknowledge it here and
        // let the loop terminate — the next run resumes from this
        // shard's coverage end time via the standard dedup-by-
        // fingerprint path.
        if let preemption, await preemption.isPreemptionRequested() {
            await preemption.acknowledge()
            throw TranscriptEnginePreempted()
        }

        return progress
    }

    // MARK: - Prioritization

    /// Order shards so that those nearest the playhead (and ahead of it)
    /// are processed first. Shards behind the playhead are deprioritized.
    /// Wraps the static `prioritizeShards` with the latest playback
    /// snapshot. Coverage filtering is deliberately not applied here —
    /// see the comment in `runTranscriptionLoop` next to the per-shard
    /// fingerprint dedup. The parameter that previously accepted
    /// `existingCoverage` was never read; it has been removed (review
    /// playhead-rfu-aac H3) so callers can no longer be misled by it.
    private func prioritizeShards(
        _ shards: [AnalysisShard]
    ) -> [AnalysisShard] {
        guard let snapshot = latestSnapshot else {
            return shards
        }
        return Self.prioritizeShards(
            shards,
            playhead: snapshot.playheadTime,
            playbackRate: snapshot.playbackRate,
            chunkOverlap: config.chunkOverlap,
            lookaheadWallClockSeconds: config.lookaheadWallClockSeconds
        )
    }

    static func prioritizeShards(
        _ shards: [AnalysisShard],
        playhead: Double,
        playbackRate: Double,
        chunkOverlap: TimeInterval,
        lookaheadWallClockSeconds: TimeInterval
    ) -> [AnalysisShard] {
        let rate = max(playbackRate, 1.0)
        let lookaheadAudioSeconds = lookaheadWallClockSeconds * rate

        let ahead = shards
            .filter { $0.startTime >= playhead - chunkOverlap }
            .sorted { $0.startTime < $1.startTime }

        let behind = shards
            .filter { $0.startTime < playhead - chunkOverlap }
            .sorted { $0.startTime > $1.startTime }

        let shard0 = behind.filter { $0.startTime == 0 }
        let behindWithoutShard0 = behind.filter { $0.startTime > 0 }

        let hotPath = ahead.filter { $0.startTime < playhead + lookaheadAudioSeconds }
        let coldAhead = ahead.filter { $0.startTime >= playhead + lookaheadAudioSeconds }

        return shard0 + hotPath + coldAhead + behindWithoutShard0
    }

    // MARK: - Stop gate (playhead-5uvz.5)

    /// Throws `TranscriptEngineStopped` if the asset has been gated by
    /// `stopTranscription(analysisAssetId:)`. Called at every safe
    /// point inside `transcribeShard` so the loop bails before any
    /// post-stop persistence write or event emission.
    private func checkStopped(analysisAssetId: String) throws {
        if stoppedAssetIds.contains(analysisAssetId) {
            throw TranscriptEngineStopped()
        }
    }

    // MARK: - Coverage updates

    /// Advance the asset's fast-transcript coverage watermark to `endTime`.
    ///
    /// playhead-0sro: the store write is MONOTONIC. That matters here
    /// specifically because ``prioritizeShards(_:)`` orders shards by
    /// playhead proximity, not by time — behind-the-playhead shards run
    /// LAST, descending — and the shard-0 backfill at the tail of
    /// ``runTranscriptionLoop(analysisAssetId:)`` runs later still. So the
    /// last `updateCoverage` call of a full pass carries an EARLY shard's
    /// end time. Under the old blind `SET … = ?` that rewound a complete
    /// episode's watermark to a low shard boundary and left every consumer
    /// (yqax catch-up, glo9 drain, activity denominators) reading stale
    /// state. Coverage is a high-water reach; only advances are meaningful.
    private func updateCoverage(
        analysisAssetId: String,
        endTime: Double
    ) async throws {
        // playhead-5uvz.5: a stop landing between the
        // `speechService.transcribe` await and this write would
        // otherwise advance `analysis_assets.fastTranscriptCoverageEndTime`
        // out-of-band after the runner had moved on. Re-check the gate
        // here as a belt-and-suspenders to the per-shard checks in
        // `transcribeShard`.
        try checkStopped(analysisAssetId: analysisAssetId)
        try await store.advanceFastTranscriptCoverage(
            id: analysisAssetId,
            endTime: endTime
        )
    }

    /// playhead-0sro: FINALIZATION reconcile. Before announcing
    /// `.completed` — the signal downstream consumers treat as "coverage is
    /// durable" — raise the watermark to the canonical `pass = 'fast'`
    /// chunk `MAX(endTime)` for this asset.
    ///
    /// The monotonic per-shard advance already prevents rewinds, but the
    /// watermark tracks SHARD ends while the chunks are what actually
    /// landed; a shard that failed mid-pass (the `continue`-on-error arm of
    /// the loop) leaves chunks written by an earlier successful shard
    /// unrepresented in the shard-derived high-water mark. Reconciling once
    /// at the end makes the completion state agree with the artifacts
    /// without adding a per-chunk write.
    ///
    /// Best-effort by design: a store hiccup here must not suppress
    /// `.completed`, because a missing completion event strands the runner
    /// on its 5-minute timeout. The next resume reconcile repairs it.
    private func reconcileCoverageAtCompletion(analysisAssetId: String) async {
        // playhead-5uvz.5: honor the stop gate here the same way
        // `updateCoverage` does. A `stopTranscription` landing in the
        // window between the loop's gate check and this call must not
        // produce a coverage write for an asset the owning runner has
        // already abandoned — even a truthful one.
        guard !stoppedAssetIds.contains(analysisAssetId) else { return }
        do {
            try await store.reconcileFastTranscriptCoverage(id: analysisAssetId)
        } catch {
            logger.warning("Coverage reconcile at completion failed for asset \(analysisAssetId): \(error)")
        }
    }

    // MARK: - Fingerprinting

    /// Compute a stable fingerprint for dedup across passes.
    /// Based on content + timing so the same text at a different position
    /// is treated as a distinct chunk.
    private func computeFingerprint(
        text: String,
        startTime: TimeInterval,
        endTime: TimeInterval
    ) -> String {
        let input = "\(text)|\(startTime)|\(endTime)"
        let digest = SHA256.hash(data: Data(input.utf8))
        return digest.prefix(16).map { String(format: "%02x", $0) }.joined()
    }

    private func mergedWeakAnchorMetadata(
        existing: TranscriptWeakAnchorMetadata?,
        candidate: TranscriptWeakAnchorMetadata?
    ) -> TranscriptWeakAnchorMetadata? {
        guard let candidate else { return existing }
        guard candidate.hasRecoveryText else { return existing }
        guard let existing else { return candidate }
        guard existing.hasRecoveryText else { return candidate }
        return existing.merged(with: candidate)
    }

    // MARK: - Text normalization

    /// Normalize text for FTS indexing. Lowercases, strips punctuation,
    /// collapses whitespace.
    ///
    /// Exposed as `internal static` so test fixtures (e.g. real-episode
    /// benchmark fixtures) can produce `chunk.normalizedText` that matches
    /// production exactly. Any change to this function automatically
    /// flows through to the test pipeline.
    ///
    /// playhead-gjxf: this comment used to end "Do not call from app code
    /// outside this service — use the instance method delegating below", and
    /// that had not been true for a long time. `TargetedWindowNarrower`,
    /// `SpecialistScanPlanner`, `LexicalScanner` and `AdDetectionService` all
    /// call `TranscriptEngineService.normalizeText(_:)` directly, because they
    /// build `TranscriptChunk`s outside the engine and the column has to hold
    /// the same quantity whoever fills it. **THIS FUNCTION IS THE ONLY
    /// DEFINITION OF WHAT `TranscriptChunk.normalizedText` MEANS.** Call it;
    /// do not re-implement it. `FinalPassRetranscriptionRunner` re-implemented
    /// it as `.lowercased()` and put raw text into that column on 3,825 rows of
    /// the 2026-08-15 device pull — a value that names one thing and holds
    /// another, invisible to any fixture whose text carries no punctuation.
    /// `AnalysisStore`'s V54 migration calls it for the same reason: a second
    /// implementation of this rule, even a correct-looking one, is the defect.
    static func normalizeText(_ text: String) -> String {
        text.lowercased()
            .components(separatedBy: CharacterSet.alphanumerics.inverted)
            .filter { !$0.isEmpty }
            .joined(separator: " ")
    }

    private func normalizeText(_ text: String) -> String {
        Self.normalizeText(text)
    }

    private func removeEventContinuation(id: UUID) {
        eventContinuations.removeValue(forKey: id)
    }

    /// playhead-ngev: emit a `.failed` on behalf of run `generation`.
    ///
    /// Every failure the loop reports goes through here so the stop-gate
    /// exemption is decided in exactly one place, from the one fact that
    /// settles it: whether the emitting run is still the run this engine is
    /// driving.
    private func emitFailure(
        _ reason: TranscriptFailureReason,
        analysisAssetId: String,
        generation: Int
    ) {
        emitEvent(
            .failed(analysisAssetId: analysisAssetId, reason: reason),
            fromCurrentRun: generation == runGeneration
        )
    }

    /// - Parameter fromCurrentRun: playhead-ngev — set by `emitFailure` only.
    ///   `true` exempts a `.failed` from the stopped-asset drop. See the
    ///   `.failed` arm below for why that exemption is correct for failures
    ///   and wrong for the other two cases.
    private func emitEvent(_ event: TranscriptEngineEvent, fromCurrentRun: Bool = false) {
        // playhead-5uvz.5: silently drop events for assets that have
        // been gated by `stopTranscription`. The bead contract is that
        // a stopped asset must produce no further `.chunksPersisted`
        // or `.completed` notifications — subscribers (e.g. the
        // `AnalysisJobRunner.run` event loop) treat `.completed` as the
        // signal that coverage is durable and queue downstream work.
        let stopped: Bool
        switch event {
        case .chunksPersisted(let assetId, _):
            stopped = stoppedAssetIds.contains(assetId)
        case .completed(let assetId):
            stopped = stoppedAssetIds.contains(assetId)
        case .failed(let assetId, _):
            // playhead-ngev: a `.failed` from the CURRENT run is NOT dropped,
            // reversing playhead-8ysk's decision to gate it like `.completed`.
            //
            // The reason `.completed` is gated does not transfer. `.completed`
            // is the signal that coverage is durable, and a subscriber acts on
            // it: `AnalysisCoordinator` finalizes a backfill, the runner reads
            // the watermark and queues ad detection. Delivering a stale one
            // starts work over an asset nobody owns. `.failed` triggers no
            // downstream work at all — it only names why nothing happened.
            //
            // And the asset being gated does not mean the runner has moved on.
            // It inserts the asset id at the TOP of its zero-coverage branch
            // and journals at the BOTTOM, so between those two lines the
            // classified failure it is about to describe was being deleted for
            // belonging to an asset that is "stopped". The gate silenced the
            // one event the row needed.
            //
            // A SUPERSEDED run is still dropped WHILE THE ASSET IS GATED: its
            // generation no longer matches, and in the window between a new
            // runner subscribing with `events()` and its `startTranscription`
            // rescinding the gate, a stale reason delivered there would fail a
            // run that has not yet done anything.
            //
            // Read the conjunction exactly (review r1): an ungated asset
            // delivers a superseded run's `.failed` too, and that is the
            // INTENDED path, not a hole. The dominant supersession is a scrub,
            // where `startTranscription` ungates the asset before spawning the
            // successor — so the predecessor's interruption reaches the runner
            // immediately, which is the whole point of the bead. Dropping it
            // there would put the runner back on its 300 s timeout.
            stopped = stoppedAssetIds.contains(assetId) && !fromCurrentRun
        }
        if stopped {
            logger.info("Dropping event for stopped asset: \(String(describing: event))")
            return
        }
        for continuation in eventContinuations.values {
            continuation.yield(event)
        }
    }
}
