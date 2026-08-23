// DownloadManager.swift
// Background download management for podcast episode audio.
// Handles progressive caching during streaming playback, background
// transfers for pre-caching, resume after interruption, LRU eviction,
// and asset fingerprinting for the analysis pipeline.

import BackgroundTasks
import CryptoKit
import Foundation
import OSLog
import UIKit
import os
// MARK: - Download State Events

/// Progress and completion events for a single episode download.
struct DownloadProgress: Sendable {
    let episodeId: String
    let bytesWritten: Int64
    let totalBytes: Int64
    var fractionCompleted: Double {
        totalBytes > 0 ? Double(bytesWritten) / Double(totalBytes) : 0
    }
}

/// A chunk of raw compressed audio data from a streaming download.
struct AudioDataChunk: Sendable {
    let episodeId: String
    let data: Data
    /// Total bytes written so far (including this chunk).
    let totalBytesWritten: Int64
}

/// Metadata harvested from HTTP response headers for fingerprinting.
struct HTTPAssetMetadata: Sendable, Equatable {
    let etag: String?
    let contentLength: Int64?
    let lastModified: String?
}

/// Stable identity for one task inside one background URLSession. Task
/// identifiers are only unique within a session, so both components are
/// required when fencing late delegate callbacks after cache removal.
struct BackgroundTransferIdentity: Sendable, Hashable {
    let sessionIdentifier: String
    let taskIdentifier: Int
}

/// One terminal background-transfer failure harvested entirely on the
/// URLSession delegate queue before crossing into DownloadManager's actor.
/// Resume persistence, ownership release, and WorkJournal failure emission
/// are then sequenced by one actor callback.
struct BackgroundTransferFailure: Sendable {
    let identity: BackgroundTransferIdentity
    let episodeId: String
    let resumeData: Data?
    let sourceURL: URL?
    let metadata: HTTPAssetMetadata?
    let cause: InternalMissCause
    let errorDescription: String
    let bytesReceived: Int
    let stage: String
}

// MARK: - AudioFingerprint

/// Identifies an audio asset across re-downloads and URL changes.
/// Weak fingerprint: enclosure URL + HTTP metadata (available early).
/// Strong fingerprint: full content SHA-256 hash (available after download).
struct AudioFingerprint: Sendable, Equatable {
    /// Enclosure URL + ETag + Content-Length + Last-Modified.
    let weak: String
    /// SHA-256 of full file contents (nil until download completes).
    let strong: String?

    /// playhead-0hi9: a weak fingerprint is only usable as an identity when
    /// it is non-empty. Several paths write the `""` sentinel when no HTTP
    /// response was ever observed, and persisting that into
    /// `analysis_assets.weakFingerprint` would make every such row look
    /// identical. `canUpgradeWeakAssetToCanonicalSHA` applies the same
    /// non-empty rule on the read side, so the two ends agree.
    static func nonEmptyWeak(_ candidate: String?) -> String? {
        guard let candidate,
              !candidate.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        else { return nil }
        return candidate
    }

    /// Build weak fingerprint from URL and HTTP metadata.
    static func makeWeak(url: URL, metadata: HTTPAssetMetadata) -> String {
        let etag = metadata.etag ?? ""
        let length = metadata.contentLength.map(String.init) ?? ""
        let modified = metadata.lastModified ?? ""
        return "\(url.absoluteString)|\(etag)|\(length)|\(modified)"
    }
}

// MARK: - CacheEntry

/// Tracks a cached audio file on disk for LRU eviction decisions.
struct CacheEntry: Sendable {
    let episodeId: String
    let fileURL: URL
    let fileSize: Int64
    let lastAccessedAt: Date
    let isFullyDownloaded: Bool
    let hasActiveAnalysis: Bool
}

// MARK: - AudioAssetPin

/// playhead-wrj8: the immutable-artifact "content pin" persisted next to a
/// downloaded episode's audio file (`<hash>.pin`). Its presence + the
/// `expectedBytes` field are what let the cache distinguish a COMPLETE,
/// serveable artifact from a truncated / mid-stream / interrupted file — an
/// existence-only check cannot.
///
/// Invariant: for the life of a downloaded episode the pinned artifact is
/// immutable. Once a pin exists whose `expectedBytes` matches the on-disk
/// length, no non-rediff path may overwrite that file in place — the bytes
/// PLAYED == ANALYZED == MARKED-AGAINST never change (DAI shows re-cut a
/// different ad stitch on every fetch, so silently re-fetching would rotate
/// the audio the user marked ads against).
///
/// A `nil` pin (legacy files downloaded before wrj8) is treated as
/// complete-by-existence so the change is non-destructive; freshly
/// downloaded/streamed files always write a pin.
struct AudioAssetPin: Codable, Sendable, Equatable {
    /// Authoritative complete byte length. During a streaming download this
    /// is seeded to the HTTP `Content-Length` (or `Int64.max` when unknown)
    /// so the growing file reads as INCOMPLETE until finalized; on
    /// completion it is rewritten to the actual on-disk size.
    var expectedBytes: Int64
    /// Full-file SHA-256, populated once the download completes. Optional
    /// because it is not known until the bytes are all on disk.
    var sha256: String?
    /// Enclosure URL the bytes were fetched from (diagnostics only).
    var sourceURL: String?
    /// HTTP validator captured at download time (diagnostics only).
    var etag: String?
    /// playhead-0hi9: the weak fingerprint (`enclosure URL | ETag |
    /// Content-Length | Last-Modified`) that was live when these bytes were
    /// written.
    ///
    /// `DownloadManager.fingerprintCache` is pure in-memory state — never
    /// persisted, cleared by `clearCache()` — so after any relaunch
    /// `fingerprint(for:)` returned nil and the scheduler's canonical-SHA
    /// upgrade path (`canUpgradeWeakAssetToCanonicalSHA`) was unreachable no
    /// matter what the database held. The strong hash was already durable
    /// here; the weak one was not, and the upgrade needs BOTH. Optional so
    /// pins written before this field decode unchanged — those rehydrate a
    /// best-effort weak from `sourceURL`/`etag`/`expectedBytes` instead.
    var weakFingerprint: String?
}

// MARK: - DownloadManagerError

enum DownloadManagerError: Error, CustomStringConvertible {
    case downloadFailed(String, String)
    case fileNotFound(String)
    case checksumMismatch(expected: String, actual: String)
    case insufficientDiskSpace(needed: Int64, available: Int64)
    case alreadyDownloading(String)
    case cancelled

    var description: String {
        switch self {
        case .downloadFailed(let id, let reason):
            "Download failed for episode '\(id)': \(reason)"
        case .fileNotFound(let path):
            "Cached file not found: \(path)"
        case .checksumMismatch(let expected, let actual):
            "Content hash mismatch: expected \(expected), got \(actual)"
        case .insufficientDiskSpace(let needed, let available):
            "Insufficient disk space: need \(needed) bytes, have \(available)"
        case .alreadyDownloading(let id):
            "Already downloading episode '\(id)'"
        case .cancelled:
            "Download cancelled"
        }
    }
}

// MARK: - DownloadContext

/// Metadata passed by the caller to connect a download to the analysis pipeline.
///
/// playhead-i9dj: `podcastTitle` and `episodeTitle` carry the human-readable
/// identifiers from the SwiftData `Podcast`/`Episode` so the AnalysisStore can
/// persist them at first observation. Both fields are optional — callers that
/// don't have the SwiftData side in scope (e.g. background-session completion
/// routes) leave them `nil`, and the AnalysisStore reconciles titles lazily on
/// the next call site that does supply them.
///
/// playhead-kkzu: `podcastId` is `nil` only when a caller SAID it could not
/// resolve the show, and then `unattributedReason` names why. The designated
/// initializer takes a non-optional `String`, so a caller cannot arrive at a
/// null identity by forgetting — the compiler makes it spell the absence out.
/// This matters because a null here becomes `job.podcastId ?? ""` inside the
/// analysis pipeline, which pools every unattributed episode under one fake
/// show; an absence that was never noticed and one that was measured must not
/// look alike downstream.
struct DownloadContext: Sendable, Equatable {

    /// Why a download carries no show identity. Every case is a claim a caller
    /// had to make deliberately; there is no `.unknown` catch-all, because the
    /// point of the type is that "nobody said" stops being expressible.
    enum UnattributedReason: String, Sendable, Codable, Equatable, CaseIterable {
        /// The SwiftData row in hand has no canonical feed identity —
        /// `Episode.resolvedShowIdentity` returned nil. The show is genuinely
        /// unresolvable from what the caller holds, not merely unfetched.
        case showIdentityUnresolvable

        /// A force-quit resume replayed from an on-disk blob after the
        /// originating process died, with no attribution sidecar beside it
        /// (a transfer started before this record existed). SwiftData is not
        /// in scope on that path.
        case resumeWithoutRecordedShow

        /// A DEBUG-only test/diagnostic route that is not modelling a real
        /// show. Never reachable in a shipping build.
        case testHarness
    }

    let podcastId: String?
    /// Non-nil exactly when `podcastId` is nil.
    let unattributedReason: UnattributedReason?
    let isExplicitDownload: Bool
    let podcastTitle: String?
    let episodeTitle: String?

    /// A download whose show is known.
    ///
    /// The identity is admitted only in its exact canonical spelling
    /// (`RecurrenceMaterialIdentity.canonicalIdentifier`) — the same gate
    /// `Episode.resolvedShowIdentity` and `SkipOrchestrator.beginEpisode`
    /// apply. An empty or non-canonical string is not a show, so it becomes a
    /// NAMED absence rather than a joinable key: `""` reaching
    /// `analysis_jobs.podcastId` would pool unrelated episodes under one fake
    /// show exactly as a NULL does, only harder to see.
    init(
        podcastId: String,
        isExplicitDownload: Bool,
        podcastTitle: String? = nil,
        episodeTitle: String? = nil
    ) {
        self.init(
            canonicalPodcastId:
                RecurrenceMaterialIdentity.canonicalIdentifier(podcastId),
            unattributedReason: .showIdentityUnresolvable,
            isExplicitDownload: isExplicitDownload,
            podcastTitle: podcastTitle,
            episodeTitle: episodeTitle
        )
    }

    private init(
        canonicalPodcastId: String?,
        unattributedReason: UnattributedReason,
        isExplicitDownload: Bool,
        podcastTitle: String?,
        episodeTitle: String?
    ) {
        self.podcastId = canonicalPodcastId
        self.unattributedReason =
            canonicalPodcastId == nil ? unattributedReason : nil
        self.isExplicitDownload = isExplicitDownload
        self.podcastTitle = podcastTitle
        self.episodeTitle = episodeTitle
    }

    /// A download whose show could not be resolved, and the reason.
    static func unattributed(
        reason: UnattributedReason,
        isExplicitDownload: Bool,
        podcastTitle: String? = nil,
        episodeTitle: String? = nil
    ) -> DownloadContext {
        DownloadContext(
            canonicalPodcastId: nil,
            unattributedReason: reason,
            isExplicitDownload: isExplicitDownload,
            podcastTitle: podcastTitle,
            episodeTitle: episodeTitle
        )
    }

    /// For a caller holding an optional identity it did not compute itself.
    /// Routes to whichever of the two constructors the value actually is, so
    /// the nil branch still carries a named reason rather than a bare `nil`.
    static func resolving(
        podcastId: String?,
        unattributedReason: UnattributedReason,
        isExplicitDownload: Bool,
        podcastTitle: String? = nil,
        episodeTitle: String? = nil
    ) -> DownloadContext {
        DownloadContext(
            canonicalPodcastId:
                RecurrenceMaterialIdentity.canonicalIdentifier(podcastId),
            unattributedReason: unattributedReason,
            isExplicitDownload: isExplicitDownload,
            podcastTitle: podcastTitle,
            episodeTitle: episodeTitle
        )
    }
}

// MARK: - DownloadProviding

/// Protocol abstraction for download queries, enabling test stubs.
protocol DownloadProviding: Sendable {
    func cachedFileURL(for episodeId: String) async -> URL?
    func fingerprint(for episodeId: String) async -> AudioFingerprint?
    func allCachedEpisodeIds() async -> Set<String>
}

// MARK: - DownloadManager

/// Manages background downloads and progressive caching for podcast
/// episode audio.
///
/// Responsibilities:
/// - Progressive download: cache audio as it streams for playback
/// - Background URLSession transfers for full episode pre-caching
/// - Asset fingerprinting from HTTP metadata + content hash
/// - LRU eviction with configurable max cache size
/// - Integrity verification on cached files
actor DownloadManager {
    private let logger = Logger(subsystem: "com.playhead", category: "Downloads")

    // MARK: - Configuration

    /// Default max cache size: 2 GB.
    static let defaultMaxCacheBytes: Int64 = 2 * 1024 * 1024 * 1024

    /// Configurable max cache size in bytes.
    private var maxCacheBytes: Int64

    // MARK: - Directories

    /// Root cache directory for episode audio.
    nonisolated let cacheDirectory: URL

    /// Subdirectory for partial/in-progress downloads.
    nonisolated let partialsDirectory: URL

    /// Subdirectory for fully downloaded and verified audio.
    nonisolated let completeDirectory: URL

    /// Subdirectory for URLSession resume-data blobs persisted by
    /// `scanForSuspendedTransfers()` (playhead-hyht). One file per
    /// episode keyed by `safeFilename(for: episodeId)`. Each file's
    /// body is the opaque OS resume-data blob returned from
    /// `URLSessionDownloadTask.cancel(byProducingResumeData:)`.
    nonisolated let resumeDataDirectory: URL

    /// playhead-kkzu: subdirectory for background-transfer ATTRIBUTION —
    /// the show a queued background download belongs to. One JSON file per
    /// episode keyed by `safeFilename(for: episodeId)`.
    ///
    /// This is on disk rather than in a dictionary for one reason: the
    /// analysis enqueue for a background download happens in
    /// `handleBackgroundDownloadComplete`, and iOS relaunches the app to
    /// deliver `handleEventsForBackgroundURLSession`, so that enqueue
    /// routinely runs in a DIFFERENT PROCESS from the `backgroundDownload`
    /// that started the transfer. An in-memory map would lose the show for
    /// precisely the population whose work should already be finished when
    /// the user presses play.
    nonisolated let attributionDirectory: URL

    /// In-memory set of episode IDs the most recent scan reported as
    /// having a persisted resume-data blob. Populated by
    /// `scanForSuspendedTransfers()` and used by the idempotence guard
    /// so a second scan pass does not re-emit preempted events for the
    /// same suspended transfers. Cleared when the transfer is resumed
    /// (blob consumed) or the blob is pruned as corrupted.
    ///
    /// Visibility is `internal` (not `private`) so the scan/resume
    /// extension in `ForceQuitResumeScan.swift` can mutate it — the
    /// actor isolation keeps reads/writes ordered.
    internal var reportedSuspendedTransfers: Set<String> = []

    // MARK: - State

    /// Active download tasks keyed by episode ID.
    private var activeDownloads: [String: Task<URL, Error>] = [:]

    /// The play-while-downloading lane is singular because its raw-audio
    /// subscribers and progressive playback surface are singular. A transfer
    /// token prevents a cancelled episode's detached byte pump from finalizing
    /// or publishing into its replacement.
    private struct ActiveStreamingTransfer {
        let id: UUID
        let episodeId: String
        let fileURL: URL
        var task: Task<Void, Never>?
    }
    private var activeStreamingTransfer: ActiveStreamingTransfer?

    /// playhead-44h1 (fix): last observed progress for each active
    /// download, used to build a ``ForegroundAssistTransferSnapshot``
    /// on `UIApplication.willResignActiveNotification`. Populated from
    /// every `DownloadProgress` broadcast via ``noteTransferProgress``;
    /// cleared when the download completes (on broadcast with
    /// `bytesWritten == totalBytes`) and on explicit cancellation.
    struct ForegroundAssistProgress: Sendable, Equatable {
        let bytesWritten: Int64
        let totalBytes: Int64
        let firstObservedAt: Date
        let firstObservedBytes: Int64
        let updatedAt: Date
    }
    private var foregroundAssistProgress: [String: ForegroundAssistProgress] = [:]

    /// playhead-44h1 (fix): scheduler used to submit
    /// `BGContinuedProcessingTaskRequest` when ``handleWillResignActive``
    /// decides the handoff to a BG task is the right call. Defaults to
    /// the real `BGTaskScheduler.shared`; swapped in tests via
    /// ``setBackgroundTaskSchedulerForTesting(_:)``. Using the same
    /// `BackgroundTaskScheduling` abstraction that BPS does so the
    /// testing surface stays consistent.
    private var backgroundTaskScheduler: any BackgroundTaskScheduling = BGTaskScheduler.shared

    /// playhead-wrj8: fetches the CURRENT server validator (ETag /
    /// Content-Length) for a URL, used to decide whether a persisted
    /// resume-data blob is still safe to splice. Defaults to a real HTTP
    /// HEAD request; tests inject a deterministic stub via
    /// ``setResumeValidatorProviderForTesting(_:)``. Returns `nil` when the
    /// validator cannot be established (treated as "cannot prove freshness"
    /// → re-download fresh rather than risk splicing a rotated stitch).
    private var resumeValidatorProvider: (@Sendable (URL) async -> HTTPAssetMetadata?)?

    /// Metadata cache: episode ID -> HTTP metadata from last response.
    private var metadataCache: [String: HTTPAssetMetadata] = [:]

    /// LRU tracking: episode ID -> last access time.
    private var accessLog: [String: Date] = [:]

    /// Episodes with active/incomplete analysis or in-flight playback
    /// (protected from eviction). playhead-wrj8: refcounted (was a plain
    /// `Set`) so overlapping owners — the playback lifecycle and one or
    /// more analysis jobs on the SAME episode — compose correctly: the
    /// file backing the current episode is only eligible for eviction once
    /// EVERY protector has released. A bare `Set` let whichever owner
    /// finished first drop protection out from under the others.
    private var analysisProtectedEpisodes: [String: Int] = [:]

    /// Episode IDs whose background URLSession download is currently
    /// in flight. Background tasks aren't tracked in `activeDownloads`
    /// (foreground only), so this set gives `evictIfNeeded` a way to
    /// protect just-deposited bg files in the small window between
    /// `didFinishDownloadingTo` moving the file into completeDirectory
    /// and `handleBackgroundDownloadComplete` running `touchAccess`.
    private var bgInFlightEpisodes: Set<String> = []

    /// Background tasks admitted by this manager instance. Cache deletion
    /// retires identities before cancelling URLSession tasks; delegate
    /// callbacks carrying a retired identity are then cleanup-only.
    private var activeBackgroundTransfers:
        [BackgroundTransferIdentity: String] = [:]
    private var retiredBackgroundTransfers:
        Set<BackgroundTransferIdentity> = []
    /// Cancellable journal tails for placed background artifacts. Cache
    /// deletion retires these before unlinking bytes so a recorder suspended
    /// before its durable append cannot publish a stale `.finalized` row.
    private struct BackgroundJournalFinalization {
        let id: UUID
        let task: Task<Void, Never>
    }
    private var backgroundJournalFinalizations:
        [String: BackgroundJournalFinalization] = [:]

    /// Once an episode has been explicitly removed, any background callback
    /// for it must carry an identity registered after that removal. This also
    /// rejects a completion that was staged just before URLSession task
    /// enumeration and reached the actor just after deletion.
    private var backgroundIdentityRequiredEpisodes: Set<String> = []
    private var requireRegisteredBackgroundIdentityAfterBulkClear = false
    /// Per-episode cache ownership epoch. A completion captures this before
    /// placement; explicit cache removal increments it before its first await
    /// so every suspended continuation can prove it still owns the artifact.
    private var cacheOwnershipGenerationByEpisode:
        [String: UInt64] = [:]

    /// Fingerprint cache: episode ID -> computed fingerprint.
    private var fingerprintCache: [String: AudioFingerprint] = [:]

    /// A successful strong-pin verification is reusable only while the
    /// manager-owned immutable path retains the same file identity metadata.
    /// This removes repeated full-file hashes from ordinary cache lookups
    /// while causing an external replacement or rewrite to miss the memo and
    /// be authenticated again.
    private struct StrongPinVerification: Equatable {
        let path: String
        let expectedHash: String
        let expectedBytes: Int64
        let modificationDate: Date?
        let fileNumber: UInt64?
    }
    private var strongPinVerifications:
        [String: StrongPinVerification] = [:]
    #if DEBUG
    private var strongPinVerificationHashCount = 0
    private var audioArtifactURLsOverrideForTesting: [String: [URL]] = [:]
    private var backgroundDownloadAdmissionCountForTesting = 0
    private var forcePinWriteFailureForTesting = false
    #endif

    /// Cached file extension per episode ID (e.g. "mp3", "m4a").
    private var extensionCache: [String: String] = [:]

    /// Optional scheduler for enqueuing pre-analysis jobs after download.
    private var analysisWorkScheduler: AnalysisWorkScheduler?
    /// playhead-4dqe: notified once per completed background download that
    /// leaves a servable pinned artifact. See
    /// `setBackgroundDownloadCompletionObserver`.
    private var backgroundDownloadCompletionObserver: (@Sendable (String, URL?) -> Void)?

    /// playhead-cnql: completions that landed BEFORE any observer was installed.
    ///
    /// `notifyBackgroundDownloadCompleted` used to be `guard let observer else
    /// { return }` — the same bare, unrecorded return playhead-4dqe was created
    /// to kill, one layer above the coordinator it fixed. The observer is
    /// installed asynchronously (an actor hop) and the delegate can deliver a
    /// completion at any instant after launch, so a race that drops a day-0
    /// kickoff forever was always reachable and left nothing behind — no row in
    /// `rediff_day_zero_kickoffs`, no counter, nothing a device pull could see.
    /// Buffering makes the hand-off ORDER-INDEPENDENT rather than lucky.
    ///
    /// Keyed by episode: a re-delivered completion replaces its predecessor
    /// rather than queueing a duplicate the coordinator would only dedup later.
    private var pendingBackgroundDownloadCompletions: [(episodeId: String, sourceURL: URL?)] = []

    /// Cap on the buffer above. A process that never installs an observer (a
    /// preview runtime, a test host, a build with day-0 off) must not grow this
    /// without bound — day-0 is speculative preparation and a lost kickoff there
    /// costs one re-fetch, while an unbounded array costs the process. The
    /// oldest entry is evicted and LOGGED, so the loss is still not silent.
    static let maxPendingBackgroundDownloadCompletions = 64

    /// playhead-xsdz.71 (Signal 1, ADDITIVE/observational): optional recorder
    /// that receives the enclosure download's redirect-chain hop hosts so the
    /// DAI-stitch classifier can persist a show-level DAI-EXPECTED prior. `nil`
    /// (default, and every test) ⇒ NO redirect-recording delegate is attached
    /// and the download is byte-identical to before. Injected once by
    /// `PlayheadRuntime`. This only OBSERVES — no consumer wiring.
    private var daiStitchRecorder: (any DAIStitchChainRecording)?

    /// Background URL sessions keyed by role. Lazy-instantiated on first
    /// use so tests can construct a `DownloadManager` without spinning
    /// up NSURLSession state for identifiers they don't exercise.
    /// See `BackgroundSessionRole` for the three lanes.
    private var _sessionsByRole: [BackgroundSessionRole: URLSession] = [:]

    /// playhead-gpdb: creations already crossing into `nsurlsessiond`, one
    /// entry per resolved role, removed the moment the crossing answers.
    ///
    /// Construction is now a SUSPENSION POINT and `DownloadManager` is a
    /// re-entrant actor, so without this two callers that both find
    /// `_sessionsByRole` empty would both cross and construct TWO
    /// `URLSession`s on the SAME background identifier — a state URLSession
    /// does not support and the OS answers by logging and misbehaving. Before
    /// this bead the whole function was synchronous on the actor, so the
    /// interleaving was impossible; making it failable is what introduces it.
    /// The second caller joins the first one's crossing instead of starting a
    /// second.
    ///
    /// Holds a `Task`, not a flag: the joiner has to be able to WAIT for the
    /// answer and receive it. And the entry is dropped on BOTH outcomes — see
    /// ``backgroundSession(for:requestedBy:)`` — so a refusal is never cached.
    private var _sessionCreationsInFlight:
        [BackgroundSessionRole: Task<URLSession?, Never>] = [:]

    #if DEBUG
    /// How many callers have reached the CROSSING DECISION — found no
    /// memoized session, and are about to either join an in-flight crossing
    /// or start one.
    ///
    /// playhead-gpdb R1 review. This exists because a correct implementation
    /// leaves NO OTHER TRACE there, and a test that cannot see the arrival
    /// cannot know when it is safe to let the crossing answer:
    ///
    ///   * a caller that JOINS writes nothing at all;
    ///   * `_sessionCreationsInFlight` is keyed by role, so a caller that
    ///     WRONGLY starts a second crossing overwrites the entry rather than
    ///     growing the map;
    ///   * two `URLSession`s on one background identifier both report that
    ///     identifier, so `_sessionsByRole` and every identifier-shaped
    ///     observable read the same on both implementations.
    ///
    /// Without it the rail could only wait a while and hope every caller had
    /// arrived. That guess is what failed the 2026-08-13 merge gate: under a
    /// full plan it ran ~32 s while holding open a crossing whose own bound is
    /// 10 s, so the crossing expired mid-barrier and the rail failed for a
    /// reason unrelated to the property. Counting the arrival turns the
    /// barrier's exit condition into an event that is GUARANTEED to happen, so
    /// load can make the wait longer but can no longer change the answer.
    private(set) var sessionCrossingArrivalsForTesting = 0
    #endif

    /// Feature flag that gates the 24cm dual-session split. Copied from
    /// `PreAnalysisConfig.useDualBackgroundSessions` at init time and
    /// re-exposed via `setUseDualBackgroundSessions(_:)` so tests can
    /// flip it without reaching into UserDefaults.
    private var useDualBackgroundSessions: Bool

    /// Recorder injected by playhead-uzdq (or any test double) to emit
    /// WorkJournal events from the download delegate callbacks. Defaults
    /// to a no-op so 24cm can ship before uzdq lands.
    ///
    /// Visibility is `internal` (not `private`) so the playhead-hyht
    /// force-quit scan extension in `ForceQuitResumeScan.swift` can emit
    /// preempted/failed rows without re-entering DownloadManager.swift.
    internal var workJournalRecorder: WorkJournalRecording

    /// Delegate for background sessions. A single delegate instance
    /// serves all three identifier lanes — the session identifier is
    /// pulled from `session.configuration.identifier` on each callback
    /// if routing needs to differ per-lane.
    private let sessionDelegate: EpisodeDownloadDelegate

    /// playhead-nsjn: every call into a background `URLSession` goes
    /// through here rather than being made inline on the actor. See
    /// `BackgroundSessionIO.swift` — those calls block the calling thread
    /// in a synchronous XPC round-trip to `nsurlsessiond`, and this actor
    /// runs on the fixed-width cooperative pool.
    ///
    /// Injected so tests can substitute `.neverAnswers` and drive the
    /// daemon-unavailable branches without a genuinely wedged daemon.
    internal let sessionIO: BackgroundSessionIO

    /// The bound for background task ENUMERATION, on its own serial queue.
    ///
    /// playhead-rouw: deliberately not `sessionIO`, and the separation is
    /// load-bearing rather than tidy. `BackgroundSessionIO`'s work queue is
    /// serial by design, and `retireBackgroundTransfers` submits one
    /// enumeration per distinct background session. Sharing the instance
    /// with `downloadTask(with:)` therefore lets a silent daemon convert one
    /// `removeCache` into ten seconds during which no download can be
    /// created either.
    ///
    /// MEASURED, and read the attribution carefully because the two numbers
    /// come from DIFFERENT implementations of this line:
    ///   * SHARED queue: seventeen enumerations blew their bound in one
    ///     full-plan run — nearly three minutes of starved download path —
    ///     and that run lost 217 tests to a crashed host.
    ///   * OWN queue (what ships): four, in an equivalent run that lost 3.
    /// The seventeen is the argument FOR the separation, not a property of
    /// the shipped code. Neither number is stable run to run; the daemon's
    /// responsiveness on this box is not.
    ///
    /// Same behaviour and same bound, so a test that injects `.neverAnswers`
    /// still stalls this; own queue, so the stall stays inside the
    /// enumeration.
    ///
    /// NOT claimed: that one silent session costs only its own enumeration.
    /// This queue is serial, so with `useDualBackgroundSessions` on, three
    /// sessions are enumerated concurrently onto ONE queue and a silent
    /// first session makes the other two expire without ever being asked.
    /// The caller still waits at most `io.timeout` — see
    /// ``boundedAllTasks(of:through:)`` — but it loses all three sources of
    /// identity rather than one. Filed as playhead-f1wb.
    internal let enumerationIO: BackgroundSessionIO

    /// The bound for background session CONSTRUCTION, on a third serial queue.
    ///
    /// playhead-gpdb. `URLSession(configuration: .background(withIdentifier:))`
    /// looks like object construction and is not: it attaches to
    /// `nsurlsessiond`, the same daemon playhead-nsjn sampled parked in
    /// `mach_msg2_trap` and the same one that failed to answer
    /// `downloadTask(with:)` seventeen times in a single full-plan run. It was
    /// the ONE remaining unbounded crossing in the download path, and both of
    /// the calls nsjn and rouw bounded are reached THROUGH it —
    /// `retireBackgroundTransfers` instantiates all three roles before it
    /// enumerates anything, so a parked construction bypasses their bounds
    /// entirely.
    ///
    /// WHY A THIRD QUEUE RATHER THAN `sessionIO`'s. The argument is not the
    /// frequency — construction is memoized, so it happens ONCE per process per
    /// role and would rarely contend for the queue. It is the CONSEQUENCE when
    /// it does. ``boundedAllTasks(of:through:)`` spells out the case
    /// `BackgroundSessionIO` declines to bound: a body whose ENTERING call
    /// parks in the session's own barrier never returns, so its queue is
    /// stranded for the life of the process. `sessionIO`'s queue is where
    /// `downloadTask(with:)`, `resume()` and the abandon-path `cancel()` are
    /// submitted — every call that makes a download happen — so putting
    /// construction there means one wedged construction permanently kills
    /// downloading, which is precisely the outcome this bead exists to convert
    /// into a reported refusal. `enumerationIO`'s queue is worse still:
    /// `retireBackgroundTransfers` submits one enumeration per session onto it
    /// CONCURRENTLY, so three constructions ahead of them would push the
    /// enumerations past their own deadlines having never run. Construction
    /// gets the queue it can strand without taking anything else with it.
    ///
    /// Same behaviour and same bound as `sessionIO`, so a test that injects
    /// `.neverAnswers` refuses construction too.
    internal let sessionCreationIO: BackgroundSessionIO

    /// playhead-gpdb / playhead-oa82: where a refused background-session
    /// construction is RECORDED, beyond `os_log`.
    ///
    /// `nil` in tests that do not care and in every preview runtime; injected
    /// by `PlayheadRuntime` with the surface-status invariant logger, whose
    /// JSON Lines session file is the surface a device pull already reads.
    /// Passed at CONSTRUCTION rather than through a setter on purpose: a
    /// post-init hop is exactly what fails to run on the launch this record
    /// matters most on — an iOS relaunch with no scene — and a recorder that
    /// is nil precisely when the failure happens records nothing.
    ///
    /// Synchronous because `SurfaceStatusInvariantLogger.invariantViolated` is
    /// fire-and-forget onto its own serial write queue; nothing here touches
    /// the file system on the actor.
    internal let invariantRecorder: (
        @Sendable (InvariantViolation.Code, String) -> Void
    )?

    /// playhead-7dgx: where a background download the transfer daemon never
    /// started is DURABLY recorded.
    ///
    /// Non-optional, unlike ``invariantRecorder``. An optional recorder makes
    /// "nobody injected one" and "nothing to record" the same `nil`, and this
    /// ledger's entire purpose is telling those two apart; a named
    /// ``NoopBackgroundDownloadDropRecorder`` at least says which it is at the
    /// construction site.
    ///
    /// Passed at CONSTRUCTION, on `invariantRecorder`'s precedent and for the
    /// same reason: the launch a dropped download matters most on is the one
    /// iOS makes with no scene, and a post-init setter is precisely what does
    /// not run there.
    ///
    /// **A production manager holding the no-op is a defect.** It is not
    /// hypothetical — ``workJournalRecorder`` above is in exactly that state:
    /// its default is never replaced by `PlayheadRuntime`, so every
    /// `recordFailed` this actor makes goes nowhere. What stops that here is
    /// `BackgroundDownloadDropWiringSourceCanaryTests` plus the arming row the
    /// ledger keeps, which makes the state visible on a device pull rather
    /// than only in source.
    internal let dropRecorder: BackgroundDownloadDropRecording

    // MARK: - Streams

    private let progressContinuation: AsyncStream<DownloadProgress>.Continuation
    /// Single-consumer stream (legacy). Prefer progressUpdates() for new code.
    nonisolated let progressStream: AsyncStream<DownloadProgress>

    /// Multi-subscriber continuations for download progress.
    private var progressSubscribers: [UUID: AsyncStream<DownloadProgress>.Continuation] = [:]

    /// playhead-3xtw (L2): highest `totalBytesWritten` broadcast for each
    /// in-flight BACKGROUND transfer. The delegate spawns one actor-hop
    /// `Task` per `didWriteData` callback, so a later Task can execute
    /// before an earlier one; this monotonic high-water mark drops the
    /// out-of-order stragglers so the delivered fraction never regresses.
    /// Reset when a new download starts and cleared on completion.
    private var lastBackgroundProgressBytes: [String: Int64] = [:]

    /// Multi-subscriber continuations for raw audio data chunks.
    private var audioDataSubscribers: [UUID: AsyncStream<AudioDataChunk>.Continuation] = [:]

    // MARK: - Init

    init(
        cacheDirectory: URL? = nil,
        maxCacheBytes: Int64 = DownloadManager.defaultMaxCacheBytes,
        preAnalysisConfig: PreAnalysisConfig? = nil,
        workJournalRecorder: WorkJournalRecording = NoopWorkJournalRecorder(),
        sessionIO: BackgroundSessionIO = .shared,
        invariantRecorder: (
            @Sendable (InvariantViolation.Code, String) -> Void
        )? = nil,
        dropRecorder: BackgroundDownloadDropRecording =
            NoopBackgroundDownloadDropRecorder()
    ) {
        self.dropRecorder = dropRecorder
        self.sessionIO = sessionIO
        self.enumerationIO = sessionIO.onItsOwnQueue(
            labelled: "\(sessionIO.queueLabel).enumeration"
        )
        self.sessionCreationIO = sessionIO.onItsOwnQueue(
            labelled: "\(sessionIO.queueLabel).creation"
        )
        self.invariantRecorder = invariantRecorder
        let root = cacheDirectory ?? Self.defaultCacheDirectory()
        self.cacheDirectory = root
        self.partialsDirectory = root.appendingPathComponent("partials", isDirectory: true)
        self.completeDirectory = root.appendingPathComponent("complete", isDirectory: true)
        self.resumeDataDirectory = root.appendingPathComponent("resumeData", isDirectory: true)
        self.attributionDirectory = root.appendingPathComponent("attribution", isDirectory: true)
        self.maxCacheBytes = maxCacheBytes
        self.sessionDelegate = EpisodeDownloadDelegate()
        let config = preAnalysisConfig ?? PreAnalysisConfig.load()
        self.useDualBackgroundSessions = config.useDualBackgroundSessions
        self.workJournalRecorder = workJournalRecorder

        let (stream, continuation) = AsyncStream<DownloadProgress>.makeStream()
        self.progressStream = stream
        self.progressContinuation = continuation

        // Wire delegate → manager so finalize / failure events route back
        // onto the actor. The delegate owns the URLSession-side closure;
        // we own the state it needs to mutate.
        self.sessionDelegate.onUrlSessionDidFinishEvents = { identifier in
            Task { @MainActor in
                if let delegate = DownloadManager.appDelegate {
                    delegate.invokePendingBackgroundCompletionHandler(forIdentifier: identifier)
                }
            }
        }
        // One identity-qualified terminal callback owns resume persistence,
        // transfer release, and WorkJournal failure emission. Keeping these
        // operations in a single actor task prevents a retry from racing a
        // separate resume-harvest task for the same failed transfer.
        self.sessionDelegate.onBackgroundDownloadFailed = {
            [weak manager = self] failure in
            guard let manager else { return }
            Task {
                await manager.handleBackgroundDownloadFailed(failure)
            }
        }
        // Wire onBackgroundDownloadStaged once at init (not per-session).
        // Body is identical across sessions — only the actor hop varies,
        // and that's keyed off the staged file/metadata, not the
        // session. The prior per-session reassignment in
        // `backgroundSession(for:)` produced needless closure churn
        // under repeated session instantiation (e.g. cold-launch
        // rehydration of multiple identifiers).
        //
        // playhead-24cm.1: the delegate's job is now to stage the
        // OS-provided file into a process-global temp dir; the actor
        // owns the final placement (which honors `cacheDirectory`,
        // including custom test directories) and the synthesis of a
        // real weak fingerprint from URL + HTTP response metadata
        // harvested on the delegate queue.
        //
        // Retain-cycle note: `[weak manager = self]` mirrors
        // `onBackgroundDownloadFailed` above. The cycle would otherwise be
        // `delegate → closure → manager → sessionDelegate → closure`,
        // leaking every `DownloadManager` forever and defeating
        // `deinit` cleanup of the willResignActive observer.
        self.sessionDelegate.onBackgroundDownloadStaged = {
            [weak manager = self] identity, episodeId, stagedURL,
                originalURL, metadata in
            guard let manager else { return }
            Task {
                await manager.handleBackgroundDownloadComplete(
                    episodeId: episodeId,
                    stagedURL: stagedURL,
                    originalURL: originalURL,
                    metadata: metadata,
                    transferIdentity: identity
                )
            }
        }
        // playhead-3xtw: surface background-transfer byte progress through
        // the same broadcast surface the foreground path uses, so the
        // per-episode prepare control's download zone advances during a
        // background (pre-cache / on-demand) download. Same init-once wiring
        // + `[weak manager]` retain-cycle guard as the staged hook above.
        self.sessionDelegate.onBackgroundDownloadProgress = {
            [weak manager = self] episodeId, bytesWritten, totalBytes in
            guard let manager else { return }
            Task { [manager, episodeId, bytesWritten, totalBytes] in
                await manager.broadcastBackgroundProgress(
                    episodeId: episodeId,
                    bytesWritten: bytesWritten,
                    totalBytes: totalBytes
                )
            }
        }
    }

    /// playhead-3xtw: actor-isolated receiver for background-transfer
    /// progress harvested on the delegate queue. Yields to the live
    /// progress streams (`progressStream` + `progressUpdates()`
    /// subscribers) so the per-episode prepare control's download zone
    /// advances during a background transfer, but deliberately does NOT
    /// route through `noteTransferProgress`: background-session transfers
    /// keep running while the app is suspended, so they must not enroll in
    /// the foreground-assist `BGContinuedProcessingTaskRequest` keep-alive
    /// (that budget is for foreground transfers only). `totalBytes <= 0`
    /// (size unknown) is dropped rather than broadcast as a
    /// divide-by-zero 0% event.
    // playhead-y3q5: internal (not private) so the monotonicity regression
    // test can drive out-of-order completion/straggler ticks directly.
    func broadcastBackgroundProgress(
        episodeId: String,
        bytesWritten: Int64,
        totalBytes: Int64
    ) {
        guard totalBytes > 0 else { return }
        // playhead-3xtw (L2): drop stale, out-of-order ticks so the
        // delivered fraction is monotonic within a transfer.
        // playhead-y3q5: on the completion tick, PIN the high-water at
        // `totalBytes` (never reset it to nil). Each didWriteData callback
        // hops to the actor via an unstructured Task with NO ordering
        // guarantee, so if the 100% tick's Task wins the race, a
        // later-arriving earlier tick used to read `nil ?? 0 = 0`, pass the
        // guard, and broadcast a REGRESSED (<100%) fraction after 100% — the
        // exact non-monotonicity this guard exists to prevent. Pinning at
        // `totalBytes` (via the `min` cap) makes that straggler fail the
        // `>= highWater` guard and get dropped. The slot is cleared for a
        // re-download by the fresh-start reset in `backgroundDownload`.
        let highWater = lastBackgroundProgressBytes[episodeId] ?? 0
        guard bytesWritten >= highWater else { return }
        lastBackgroundProgressBytes[episodeId] = min(bytesWritten, totalBytes)
        let progress = DownloadProgress(
            episodeId: episodeId,
            bytesWritten: bytesWritten,
            totalBytes: totalBytes
        )
        progressContinuation.yield(progress)
        for (_, continuation) in progressSubscribers {
            continuation.yield(progress)
        }
    }

    /// Returns a fresh AsyncStream that receives all future download progress
    /// events. Each caller gets its own stream — multiple subscribers are supported.
    /// The stream ends when the continuation is cancelled or the manager is deallocated.
    func progressUpdates() -> AsyncStream<DownloadProgress> {
        let id = UUID()
        return AsyncStream { continuation in
            self.progressSubscribers[id] = continuation
            continuation.onTermination = { @Sendable _ in
                Task { [weak self] in
                    await self?.removeProgressSubscriber(id: id)
                }
            }
        }
    }

    private func removeProgressSubscriber(id: UUID) {
        progressSubscribers.removeValue(forKey: id)
    }

    /// Yield progress to both the legacy single-consumer stream and all subscribers.
    private func broadcastProgress(_ progress: DownloadProgress) {
        progressContinuation.yield(progress)
        for (_, continuation) in progressSubscribers {
            continuation.yield(progress)
        }
        noteTransferProgress(progress)
    }

    /// playhead-44h1 (fix): record the latest `DownloadProgress` for
    /// the foreground-assist handoff snapshot. Called on every
    /// `broadcastProgress` and from background-session delegate
    /// hooks so the `willResignActive` handler has up-to-date byte
    /// counters to feed `ForegroundAssistHandoff.decide(for:)`.
    ///
    /// Clears the slot when the transfer has completed
    /// (`bytesWritten >= totalBytes > 0`) so a subsequent background
    /// transition does not spuriously emit a keep-alive for a
    /// finished download.
    func noteTransferProgress(_ progress: DownloadProgress) {
        // Completed transfer: remove the slot so the snapshot does
        // not observe stale bytes after the work is done.
        if progress.totalBytes > 0 && progress.bytesWritten >= progress.totalBytes {
            foregroundAssistProgress.removeValue(forKey: progress.episodeId)
            return
        }
        let now = Date()
        let existing = foregroundAssistProgress[progress.episodeId]
        // Preserve the first-observation timestamp so the throughput
        // estimate spans the full active window, not just the most
        // recent tick. This gives `ForegroundAssistHandoff` a useful
        // `averageBytesPerSecond` even for freshly-started transfers.
        foregroundAssistProgress[progress.episodeId] = ForegroundAssistProgress(
            bytesWritten: progress.bytesWritten,
            totalBytes: progress.totalBytes,
            firstObservedAt: existing?.firstObservedAt ?? now,
            firstObservedBytes: existing?.firstObservedBytes ?? progress.bytesWritten,
            updatedAt: now
        )
    }

    /// Returns a fresh AsyncStream of raw audio data chunks for streaming decode.
    /// Each caller gets its own stream — multiple subscribers supported.
    func audioDataUpdates() -> AsyncStream<AudioDataChunk> {
        let id = UUID()
        return AsyncStream { continuation in
            self.audioDataSubscribers[id] = continuation
            continuation.onTermination = { @Sendable _ in
                Task { [weak self] in
                    await self?.removeAudioDataSubscriber(id: id)
                }
            }
        }
    }

    private func removeAudioDataSubscriber(id: UUID) {
        audioDataSubscribers.removeValue(forKey: id)
    }

    private func broadcastAudioData(_ chunk: AudioDataChunk) {
        for (_, continuation) in audioDataSubscribers {
            continuation.yield(chunk)
        }
    }

    /// Finish all audio data subscriber streams so `for await` loops exit.
    private func finishAudioDataSubscribers() {
        for (id, continuation) in audioDataSubscribers {
            continuation.finish()
        }
        audioDataSubscribers.removeAll()
    }

    /// Wire up the analysis scheduler so downloads automatically enqueue jobs.
    func setAnalysisWorkScheduler(_ scheduler: AnalysisWorkScheduler) {
        self.analysisWorkScheduler = scheduler
    }

    /// playhead-xsdz.71 (Signal 1): inject the DAI-stitch redirect-chain
    /// recorder. Wired once by `PlayheadRuntime`; left `nil` in tests so the
    /// download path stays byte-identical.
    func setDAIStitchRecorder(_ recorder: any DAIStitchChainRecording) {
        self.daiStitchRecorder = recorder
    }

    /// playhead-xsdz.71 (Signal 1): build a redirect-recording delegate when a
    /// recorder is wired AND we know the show, else `nil` (no delegate → the
    /// download call is byte-identical). Shared by the download + streaming
    /// paths.
    private func makeRedirectRecordingDelegate(
        url: URL,
        context: DownloadContext
    ) -> RedirectChainRecordingDelegate? {
        guard daiStitchRecorder != nil, context.podcastId != nil else { return nil }
        return RedirectChainRecordingDelegate(initialHost: url.host)
    }

    /// playhead-xsdz.71 (Signal 1): hand the observed redirect chain to the
    /// recorder off the download's critical path. Best-effort/observational; a
    /// `nil` recorder/delegate/podcastId is a no-op. `finalHost` is the final
    /// response URL host, appended when it differs from the last recorded hop.
    private func recordDAIStitchChain(
        delegate: RedirectChainRecordingDelegate?,
        context: DownloadContext,
        finalHost: String?
    ) {
        guard let recorder = daiStitchRecorder,
              let delegate,
              let podcastId = context.podcastId else { return }
        var hosts = delegate.hopHosts
        if let finalHost, !finalHost.isEmpty, hosts.last != finalHost {
            hosts.append(finalHost)
        }
        Task { await recorder.recordRedirectChain(podcastId: podcastId, hopHosts: hosts) }
    }

    /// playhead-44h1 (fix): inject a `BackgroundTaskScheduling` so
    /// tests can observe the `BGContinuedProcessingTaskRequest`
    /// submission path without touching `BGTaskScheduler.shared`.
    /// Production leaves the default `.shared` scheduler in place.
    func setBackgroundTaskSchedulerForTesting(_ scheduler: any BackgroundTaskScheduling) {
        self.backgroundTaskScheduler = scheduler
    }

    /// playhead-wrj8: inject the resume-freshness validator provider so
    /// tests can exercise the ETag/length-mismatch → re-download-fresh path
    /// without real network. Production leaves this `nil` and the resume
    /// path issues a real HTTP HEAD.
    func setResumeValidatorProviderForTesting(
        _ provider: @escaping @Sendable (URL) async -> HTTPAssetMetadata?
    ) {
        self.resumeValidatorProvider = provider
    }

    /// playhead-wrj8: resolves the current server validator for `url`,
    /// using the injected provider when present, otherwise a real HTTP HEAD.
    func currentServerValidator(for url: URL) async -> HTTPAssetMetadata? {
        if let provider = resumeValidatorProvider {
            return await provider(url)
        }
        var request = URLRequest(url: url)
        request.httpMethod = "HEAD"
        guard let (_, response) = try? await URLSession.shared.data(for: request),
              let http = response as? HTTPURLResponse else {
            return nil
        }
        let len = http.expectedContentLength
        return HTTPAssetMetadata(
            etag: http.value(forHTTPHeaderField: "ETag"),
            contentLength: len > 0 ? len : nil,
            lastModified: http.value(forHTTPHeaderField: "Last-Modified")
        )
    }

    // MARK: - playhead-44h1 (fix): Foreground-assist lifecycle

    /// Register a `UIApplication.willResignActiveNotification` observer
    /// so the `ForegroundAssistHandoff.decide(for:)` entry point has at
    /// least one production call-site. When the app backgrounds with an
    /// in-flight download, this observer builds a snapshot from
    /// ``foregroundAssistProgress`` and routes it through the
    /// decision module. On a `.submitContinuedProcessingRequest`
    /// verdict, it submits a `BGContinuedProcessingTaskRequest` via
    /// the injected `BackgroundTaskScheduling`.
    ///
    /// Scope note (spec state-machine step 3): the keep-alive /
    /// URLSession-background plumbing half of the decision is
    /// explicitly deferred to playhead-iwiy; the `.keepForegroundAssistAlive`
    /// branch here logs and does no work. The observer exists so a
    /// reader grepping for `decide(for:` in production code lands on
    /// this site rather than the bare module definition.
    ///
    /// Idempotent: a second call is a no-op so app-lifecycle wiring
    /// can safely invoke this during both `didFinishLaunching` and
    /// `sceneWillConnectToSession`-equivalent paths.
    func registerForegroundAssistLifecycleObserver() {
        guard foregroundAssistObserverToken == nil else { return }
        let center = NotificationCenter.default
        let token = center.addObserver(
            forName: UIApplication.willResignActiveNotification,
            object: nil,
            queue: nil
        ) { [weak self] _ in
            Task { [weak self] in
                await self?.handleWillResignActive()
            }
        }
        foregroundAssistObserverToken = token
    }

    /// Token returned by `NotificationCenter.addObserver(forName:...)`
    /// so ``deregisterForegroundAssistLifecycleObserver`` can reverse
    /// the registration. An opaque `NSObjectProtocol` per the API
    /// contract. Marked `nonisolated(unsafe)` so the actor's nonisolated
    /// `deinit` can read the token and remove the observer from
    /// `NotificationCenter.default` (itself thread-safe) without
    /// hopping onto the actor. All non-deinit mutations still happen
    /// on the actor so reads/writes remain serialized in practice.
    nonisolated(unsafe) private var foregroundAssistObserverToken: (any NSObjectProtocol)?

    /// playhead-44h1 (fix): remove the `willResignActive` observer on
    /// deinit so a released `DownloadManager` does not leave a stray
    /// `NotificationCenter` registration pointing at freed memory.
    /// Actor `deinit` is nonisolated; `NotificationCenter.removeObserver`
    /// is documented thread-safe and the token is `nonisolated(unsafe)`,
    /// so reading it here is sound. All live mutation of the token
    /// happens through actor-isolated methods, which cannot race with
    /// `deinit` (the last reference has already dropped).
    deinit {
        if let token = foregroundAssistObserverToken {
            NotificationCenter.default.removeObserver(token)
        }
    }

    /// Tear down the `willResignActive` observer. Primarily for
    /// tests so a second registration does not accumulate observers
    /// across test cases.
    func deregisterForegroundAssistLifecycleObserver() {
        if let token = foregroundAssistObserverToken {
            NotificationCenter.default.removeObserver(token)
            foregroundAssistObserverToken = nil
        }
    }

    /// Entry point for the `willResignActive` handoff decision.
    /// Exposed as a non-notification method so tests can drive it
    /// directly without posting through `NotificationCenter`.
    ///
    /// For each active transfer: build a
    /// ``ForegroundAssistTransferSnapshot``, call
    /// ``ForegroundAssistHandoff/decide(for:)``, and act on the
    /// verdict. A single `willResignActive` cycle may emit multiple
    /// submissions — one per still-active episode. The outer loop
    /// iterates distinct keys of ``foregroundAssistProgress`` so the
    /// same episode cannot be submitted twice within one cycle. Across
    /// cycles, iOS coalesces duplicate `BGContinuedProcessingTaskRequest`
    /// identifiers server-side, so we do not maintain any app-side
    /// dedupe set here.
    @discardableResult
    func handleWillResignActive(
        now: Date = Date()
    ) -> [ForegroundAssistHandoffDecision] {
        var decisions: [ForegroundAssistHandoffDecision] = []
        for (episodeId, progress) in foregroundAssistProgress {
            let snapshot = makeSnapshot(for: progress, now: now)
            let decision = ForegroundAssistHandoff.decide(for: snapshot)
            decisions.append(decision)
            logger.info(
                "foreground-assist handoff: episode=\(episodeId, privacy: .public) fraction=\(snapshot.fractionCompleted) etaSeconds=\(snapshot.remainingSeconds) decision=\(String(describing: decision), privacy: .public)"
            )
            switch decision {
            case .submitContinuedProcessingRequest:
                submitContinuedProcessingRequest(for: episodeId)
            case .keepForegroundAssistAlive:
                // Routing into a URLSession background-session
                // keep-alive is playhead-iwiy's territory. This
                // observer's job for the keep-alive branch is just
                // to LOG that a decision was made so reviewers can
                // see the handoff fired at a real call-site.
                break
            }
        }
        return decisions
    }

    /// Build a ``ForegroundAssistTransferSnapshot`` from a stored
    /// progress entry. Throughput is estimated over the full
    /// observation window (first-observed timestamp → `now`) so a
    /// freshly-resumed transfer's throughput does not alias to 0
    /// just because the most recent progress tick was a moment ago.
    private func makeSnapshot(
        for progress: ForegroundAssistProgress,
        now: Date
    ) -> ForegroundAssistTransferSnapshot {
        let elapsed = now.timeIntervalSince(progress.firstObservedAt)
        let bytesDelta = max(0, progress.bytesWritten - progress.firstObservedBytes)
        let throughput: Double
        if elapsed > 0 && bytesDelta > 0 {
            throughput = Double(bytesDelta) / elapsed
        } else {
            // No observable delta yet — treat as unknown so the
            // handoff decision errs toward BG task (the safe choice).
            throughput = 0
        }
        return ForegroundAssistTransferSnapshot(
            totalBytesWritten: progress.bytesWritten,
            totalBytesExpectedToWrite: progress.totalBytes,
            averageBytesPerSecond: throughput
        )
    }

    /// Submit a `BGContinuedProcessingTaskRequest` for `episodeId`.
    /// The identifier follows the wildcard convention
    /// `"<BackgroundTaskID.continuedProcessing>.<episodeId>"` that
    /// `BackgroundProcessingService.parseEpisodeId(from:)` expects.
    ///
    /// No app-side dedupe: iOS `BGTaskScheduler` coalesces duplicate
    /// identifiers, and `handleWillResignActive`'s caller loop already
    /// iterates distinct `episodeId`s, so a single cycle cannot submit
    /// twice for the same episode.
    ///
    /// playhead-izvj.1 (Mac Catalyst spike): `BGContinuedProcessingTaskRequest`
    /// is iOS-only — the API is unavailable in Mac Catalyst. On Catalyst
    /// we no-op and log; the desktop process is not subject to the same
    /// suspend-on-background lifecycle, so the 80%/2-min decision still
    /// flows through `ForegroundAssistHandoff.decide(...)` (so the call
    /// site logs it) but the BG task submission is skipped. A future
    /// Catalyst polish bead can decide whether to keep the URL session
    /// alive explicitly or rely on the OS not killing the process.
    private func submitContinuedProcessingRequest(for episodeId: String) {
        let identifier = "\(BackgroundTaskID.continuedProcessing).\(episodeId)"
        #if targetEnvironment(macCatalyst)
        logger.info(
            "Skipping BGContinuedProcessingTaskRequest on Mac Catalyst (API unavailable): \(identifier, privacy: .public)"
        )
        #else
        let request = BGContinuedProcessingTaskRequest(
            identifier: identifier,
            title: "Finishing download",
            subtitle: "We'll wrap up this episode in the background."
        )
        request.strategy = .fail
        do {
            try backgroundTaskScheduler.submit(request)
            logger.info("Submitted BGContinuedProcessingTaskRequest: \(identifier, privacy: .public)")
        } catch {
            logger.error(
                "Failed to submit BGContinuedProcessingTaskRequest \(identifier, privacy: .public): \(String(describing: error), privacy: .public)"
            )
        }
        #endif
    }

    /// Test hook: read the current stored foreground-assist progress
    /// for an episode. Used by unit tests to verify that
    /// `noteTransferProgress` updates the snapshot state.
    func foregroundAssistProgressForTesting(episodeId: String) -> ForegroundAssistProgress? {
        foregroundAssistProgress[episodeId]
    }

    /// Test hook: seed the foreground-assist progress map directly
    /// so tests exercising `handleWillResignActive` do not have to
    /// drive a real download to simulate bytes written.
    func seedForegroundAssistProgressForTesting(
        episodeId: String,
        bytesWritten: Int64,
        totalBytes: Int64,
        firstObservedAt: Date,
        firstObservedBytes: Int64 = 0,
        updatedAt: Date = Date()
    ) {
        foregroundAssistProgress[episodeId] = ForegroundAssistProgress(
            bytesWritten: bytesWritten,
            totalBytes: totalBytes,
            firstObservedAt: firstObservedAt,
            firstObservedBytes: firstObservedBytes,
            updatedAt: updatedAt
        )
    }

    static func defaultCacheDirectory() -> URL {
        FileManager.default
            .urls(for: .cachesDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("Playhead", isDirectory: true)
            .appendingPathComponent("AudioCache", isDirectory: true)
    }

    /// Create required directories on first use.
    ///
    /// playhead-7dgx: `async` because the FIRST thing it does is arm the
    /// dropped-download ledger. Arming FIRST, before any work that can throw,
    /// is deliberate — the claim being recorded is "this process installed a
    /// live drop recorder", which is true whether or not the directory
    /// creation below then fails, and a claim written last would be lost
    /// exactly on the launches where downloads are most broken.
    func bootstrap() async throws {
        await dropRecorder.recordInstrumentArmed(at: Date().timeIntervalSince1970)
        let fm = FileManager.default
        for dir in [
            cacheDirectory,
            partialsDirectory,
            completeDirectory,
            resumeDataDirectory,
            attributionDirectory,
        ] {
            if !fm.fileExists(atPath: dir.path) {
                try fm.createDirectory(at: dir, withIntermediateDirectories: true)
            }
            // playhead-h3h: stamp the audio-cache directories with the
            // same protection class as the AnalysisStore. The bead's
            // wishlist asks for `.complete`, but the same BG-launch
            // constraint that forced AnalysisStore down to
            // `.completeUntilFirstUserAuthentication` applies here:
            // AnalysisCoordinator opens cached audio during
            // BGProcessingTask windows that may begin pre-first-unlock.
            // `.complete` would block those reads. Re-stamping
            // unconditionally migrates pre-h3h installs.
            try? fm.setAttributes(
                [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication],
                ofItemAtPath: dir.path
            )
        }
        // Remove stale files with unrecognized extensions (e.g. ".audio"
        // from earlier builds) that AVURLAsset can't open.
        if let files = try? fm.contentsOfDirectory(atPath: completeDirectory.path) {
            for file in files {
                let ext = (file as NSString).pathExtension.lowercased()
                if !ext.isEmpty,
                   !Self.knownAudioExtensions.contains(ext),
                   ext != "partial",
                   ext != Self.pinExtension {
                    let staleURL = completeDirectory.appendingPathComponent(file)
                    try? fm.removeItem(at: staleURL)
                    logger.info("Removed stale cache file: \(file)")
                }
            }
        }

        // Rebuild access log from file system.
        try rebuildAccessLog()
        logger.info("DownloadManager bootstrapped at \(self.cacheDirectory.path)")
    }

    // MARK: - Background Session

    /// Roles (configurations) under which a background URLSession may be
    /// instantiated. playhead-24cm introduces `interactive` and
    /// `maintenance`; `legacy` remains live for one release cycle so
    /// resume data from the pre-24cm single-session build can drain.
    enum BackgroundSessionRole: Hashable {
        /// User-initiated downloads. `isDiscretionary = false`.
        case interactive
        /// Subscription auto-downloads. `isDiscretionary = true`,
        /// `allowsCellularAccess` follows the user preference.
        case maintenance
        /// Legacy single-session identifier. Retained during rollout.
        case legacy

        var identifier: String {
            switch self {
            case .interactive: return BackgroundSessionIdentifier.interactive
            case .maintenance: return BackgroundSessionIdentifier.maintenance
            case .legacy:      return BackgroundSessionIdentifier.legacy
            }
        }

        static func role(for identifier: String) -> BackgroundSessionRole? {
            switch identifier {
            case BackgroundSessionIdentifier.interactive: return .interactive
            case BackgroundSessionIdentifier.maintenance: return .maintenance
            case BackgroundSessionIdentifier.legacy:      return .legacy
            default: return nil
            }
        }
    }

    /// Who asked for a background session. Carried into the refusal record so
    /// a device pull can tell the four refusals apart — their consequences are
    /// unrelated, and so are their remedies.
    ///
    /// playhead-gpdb: a parameter rather than a `#function` capture because
    /// every caller of ``backgroundSession(for:requestedBy:)`` has to
    /// acknowledge, at the call site, that the daemon can refuse it.
    enum BackgroundSessionRequestSite: String, Sendable {
        /// `ForceQuitResumeScan.resumeSuspendedTransfer` — a resume blob is
        /// waiting and is the only copy of the bytes already fetched.
        case forceQuitResume = "force_quit_resume"
        /// `resumeSession(identifier:)` — iOS relaunched the app to deliver
        /// this session's pending background events.
        case backgroundRelaunchWake = "background_relaunch_wake"
        /// `backgroundDownload` — one episode's transfer.
        case backgroundDownload = "background_download"
        /// `retireBackgroundTransfers` — cache deletion / explicit cancel.
        case transferRetirement = "transfer_retirement"
        /// A test hook. Never reached in production.
        case testHook = "test_hook"
    }

    /// Label prefix every session-construction crossing carries, so a
    /// `refusesCallsLabelled` seam can name exactly this crossing and the
    /// source canary can pin it.
    static let sessionCreationLabelPrefix = "URLSession(background:) for "

    /// Returns the URLSession for the given role, instantiating it lazily —
    /// or `nil` when `nsurlsessiond` did not hand one back inside the bound.
    ///
    /// When the 24cm feature flag is OFF, callers that ask for `.interactive`
    /// or `.maintenance` are transparently routed to `.legacy` so the
    /// behavior matches the pre-24cm build exactly.
    ///
    /// ─────────────────────────────────────────────────────────────────────
    /// playhead-gpdb: WHY THIS IS FAILABLE, AND WHY IT IS NOT A FALLBACK
    /// ─────────────────────────────────────────────────────────────────────
    /// Constructing a BACKGROUND `URLSession` attaches to `nsurlsessiond`.
    /// That is the daemon playhead-nsjn sampled with all ten cooperative
    /// threads parked in `mach_msg2_trap` behind it, and this actor runs on
    /// that same fixed-width pool — so an unbounded construction here is the
    /// hang nsjn and rouw each bounded one layer further in, reached before
    /// either of their bounds can apply.
    ///
    /// Dan's decision (2026-08-13) was BOUND IT AND FAIL THE CALL. The
    /// alternative on the table — return a plain foreground `URLSession` so
    /// this signature could stay non-optional — was rejected by name: a
    /// session that still calls itself `background` while silently having
    /// stopped being one takes the overnight "wake up to analyzed episodes"
    /// behaviour away with nothing saying so. **Do not reintroduce it.** A
    /// caller that cannot get a background session reports that it cannot
    /// download.
    ///
    /// A refusal is NEVER CACHED: `_sessionsByRole` is written only on
    /// success and `_sessionCreationsInFlight` is cleared on both outcomes,
    /// so the next call retries naturally. That is the whole difference
    /// between a transient daemon stall and a dead download subsystem, and it
    /// is what `sessionCreationRetriesAfterARefusal` pins.
    ///
    /// Visibility is `internal` (not `private`) so the playhead-hyht
    /// force-quit scan extension in `ForceQuitResumeScan.swift` can hand
    /// a resume-data blob back to the interactive session.
    internal func backgroundSession(
        for role: BackgroundSessionRole,
        requestedBy site: BackgroundSessionRequestSite
    ) async -> URLSession? {
        let resolvedRole: BackgroundSessionRole = {
            if !useDualBackgroundSessions, role != .legacy { return .legacy }
            return role
        }()

        if let existing = _sessionsByRole[resolvedRole] { return existing }

        #if DEBUG
        // playhead-gpdb R1: this caller found no memoized session, so it is
        // about to either join an in-flight crossing or start one. See
        // `sessionCrossingArrivalsForTesting` for why the arrival needs to be
        // counted here and cannot be inferred anywhere else.
        sessionCrossingArrivalsForTesting += 1
        #endif

        // Somebody is already inside the daemon for this role. Join them
        // rather than opening a second `URLSession` on the same background
        // identifier — see `_sessionCreationsInFlight`.
        if let inFlight = _sessionCreationsInFlight[resolvedRole] {
            return await inFlight.value
        }

        let creation = Task { [self] () -> URLSession? in
            await createBackgroundSession(role: resolvedRole)
        }
        _sessionCreationsInFlight[resolvedRole] = creation
        let session = await creation.value
        // Dropped on BOTH outcomes, and there is no suspension point between
        // here and the memo write below, so no caller can observe a state
        // where neither the memo nor an in-flight crossing exists for a role
        // that has one.
        _sessionCreationsInFlight[resolvedRole] = nil

        guard let session else {
            logger.error(
                "backgroundSession(\(resolvedRole.identifier, privacy: .public)) REFUSED at \(site.rawValue, privacy: .public): the background transfer daemon did not hand back a session"
            )
            invariantRecorder?(
                .backgroundSessionCreationRefused,
                """
                site=\(site.rawValue) role=\(resolvedRole.identifier) \
                bound=\(sessionCreationIO.timeout)s — nsurlsessiond did not \
                hand back a background URLSession; nothing was cached, the \
                next request retries
                """
            )
            return nil
        }

        // onBackgroundDownloadStaged is wired once at init — see
        // DownloadManager initializer. No per-session reassignment
        // needed.

        _sessionsByRole[resolvedRole] = session
        return session
    }

    /// The crossing itself: build the configuration AND the session inside one
    /// bounded, off-pool submission.
    ///
    /// Both halves are inside the bound deliberately. The daemon attach is
    /// documented as belonging to `URLSession.init`, but
    /// `URLSessionConfiguration.background(withIdentifier:)` is opaque system
    /// code on the same identifier and nobody has measured that it never
    /// touches `nsurlsessiond`; splitting them would put an unmeasured call
    /// back on the cooperative pool to save nothing.
    ///
    /// A session that arrives AFTER the caller gave up is invalidated rather
    /// than dropped, for the same reason a late `downloadTask` is cancelled: a
    /// live session left registered on that background identifier is what the
    /// next construction attempt would collide with.
    private func createBackgroundSession(
        role: BackgroundSessionRole
    ) async -> URLSession? {
        let delegate = sessionDelegate
        return await sessionCreationIO.perform(
            label: "\(Self.sessionCreationLabelPrefix)\(role.identifier)",
            discardingLateResult: { $0.invalidateAndCancel() },
            running: {
                let config: URLSessionConfiguration
                switch role {
                case .interactive:
                    config = URLSessionConfiguration.background(
                        withIdentifier: BackgroundSessionIdentifier.interactive
                    )
                    config.sessionSendsLaunchEvents = true
                    config.isDiscretionary = false
                    config.allowsCellularAccess = true
                case .maintenance:
                    config = URLSessionConfiguration.background(
                        withIdentifier: BackgroundSessionIdentifier.maintenance
                    )
                    config.sessionSendsLaunchEvents = true
                    config.isDiscretionary = true
                    // UserPreferences.allowsCellular governs the maintenance
                    // lane because auto-downloads are the surface most likely
                    // to surprise users on cellular. Read here rather than on
                    // the actor because `UserPreferencesSnapshot` is the
                    // UserDefaults-backed value designed to be read at
                    // URLSession-construction time, off-main and synchronous.
                    config.allowsCellularAccess =
                        UserPreferencesSnapshot.current.allowsCellular
                case .legacy:
                    config = URLSessionConfiguration.background(
                        withIdentifier: BackgroundSessionIdentifier.legacy
                    )
                    config.sessionSendsLaunchEvents = true
                    config.isDiscretionary = false
                    config.allowsCellularAccess = true
                }
                return URLSession(
                    configuration: config,
                    delegate: delegate,
                    delegateQueue: nil
                )
            }
        )
    }

    /// Re-instantiates the URLSession for `identifier` so its delegate
    /// callbacks fire. Invoked by `PlayheadAppDelegate` when iOS wakes
    /// the app to relay pending background events.
    ///
    /// - Returns: `false` when the identifier is unknown, or when the daemon
    ///   refused the session.
    ///
    /// playhead-gpdb: THIS LOOKS LIKE A WARMUP AND IS NOT ONE, and the bead
    /// that filed this defect guessed the other way — so the decision is
    /// spelled out rather than left to the `_ =` that used to sit here.
    ///
    /// A warmup may swallow a failure because a real call retries it. There is
    /// no such call here. `PlayheadRuntime` documents this as "the ONLY
    /// production caller that re-instantiates the background `URLSession` in a
    /// relaunched process": without it the session object never exists, the
    /// delegate never fires, `handleBackgroundDownloadComplete` never runs, the
    /// day-0 kickoff observer has nothing to receive, and the OS completion
    /// handler is never invoked — which is how an app loses its background
    /// scheduling budget. Nobody retries any of that. So the refusal is
    /// RECORDED (by `backgroundSession(for:requestedBy:)`, tagged
    /// `.backgroundRelaunchWake`) and reported to the caller, on the launch
    /// where a silence would be least visible and most expensive.
    ///
    /// What this deliberately does NOT do is invoke the pending OS completion
    /// handler on the failure path. Whether an app that could not open the
    /// session should still tell iOS it is finished is a behaviour question
    /// about `PlayheadAppDelegate`, not about this call site; filed rather
    /// than guessed at (playhead-fhh1).
    @discardableResult
    func resumeSession(identifier: String) async -> Bool {
        guard let role = BackgroundSessionRole.role(for: identifier) else {
            return false
        }
        return await backgroundSession(
            for: role, requestedBy: .backgroundRelaunchWake
        ) != nil
    }

    /// Flip the 24cm feature flag in-process. Called from two paths:
    ///   1. Tests that want to exercise the dual-session code path
    ///      without touching UserDefaults.
    ///   2. Settings → Diagnostics → Feature flags: when the user
    ///      toggles `playhead-24cm`, `SettingsView` persists the value
    ///      via `PreAnalysisConfig.save()` and then calls this method
    ///      on the shared manager so the new lane selection takes
    ///      effect without waiting for the next app launch.
    func setUseDualBackgroundSessions(_ value: Bool) {
        self.useDualBackgroundSessions = value
    }

    // MARK: - Test hooks (internal)

    /// playhead-gpdb: optional, like the production accessor. A hook that
    /// force-unwrapped would let a test pass on a build whose construction was
    /// refused, which is the one thing this bead is about.
    func backgroundSessionForTesting(
        role: BackgroundSessionRole
    ) async -> URLSession? {
        await backgroundSession(for: role, requestedBy: .testHook)
    }

    func instantiatedSessionIdentifiersForTesting() -> Set<String> {
        Set(_sessionsByRole.values.compactMap { $0.configuration.identifier })
    }

    /// Snapshot of currently-instantiated background URLSessions across
    /// all roles. Used by `ForceQuitResumeScan.liveBackgroundDownloadEpisodeIds`
    /// to dedup the cold-launch scan against transfers the OS still
    /// owns, without triggering lazy instantiation of cold sessions
    /// (which would blow the 2 s scan SLA).
    func backgroundSessionsAlreadyInstantiated() -> [URLSession] {
        Array(_sessionsByRole.values)
    }

    #if DEBUG
    /// playhead-g2wq test seam: exposes the URLSession delegate so tests
    /// can drive `didCompleteWithError` directly and verify the
    /// resume-data harvest path writes into `resumeDataDirectory`.
    /// DEBUG-only to keep production binaries free of the delegate-escape
    /// surface.
    func sessionDelegateForTesting() -> EpisodeDownloadDelegate {
        sessionDelegate
    }

    /// playhead-6e8m test seam: cancels every in-flight task on every
    /// instantiated background URLSession and invalidates the sessions
    /// themselves. Required by tests that exercise the resume path
    /// (e.g. `ResumeSuspendedTransferTests.resumeConsumesBlob`) which
    /// hand garbage `Data` blobs to a real background URLSession on the
    /// process-global `com.playhead.transfer.interactive` identifier —
    /// without invalidation the orphaned task stays alive for the rest
    /// of the process and leaks into sibling tests that construct a
    /// fresh `DownloadManager`.
    ///
    /// Idempotent: a session that has already been invalidated is
    /// dropped from the role map, so a second call is a no-op. Drops
    /// the role map after invalidation so subsequent calls to
    /// `backgroundSession(for:)` would lazily create a fresh session
    /// (callers should treat this as an end-of-life signal for the
    /// manager-under-test).
    func invalidateBackgroundSessionsForTesting() {
        for session in _sessionsByRole.values {
            session.invalidateAndCancel()
        }
        _sessionsByRole.removeAll()
    }
    #endif

    // MARK: - Progressive Download (Streaming Cache)

    /// Starts a progressive download, caching audio as it arrives.
    /// Returns the local file URL once enough data is available for playback.
    /// The download continues in the background until complete.
    ///
    /// If the file is already fully cached, returns immediately.
    /// playhead-kkzu: `context` is required, with no default. This entry
    /// point has no production caller today, and a defaulted context is
    /// exactly how the next one would silently record a NULL show.
    func progressiveDownload(
        episodeId: String,
        from url: URL,
        context: DownloadContext
    ) async throws -> URL {
        guard activeStreamingTransfer?.episodeId != episodeId else {
            throw DownloadManagerError.alreadyDownloading(episodeId)
        }

        // Cache the source extension for this episode.
        let sourceExt = Self.cacheExtension(for: url)
        extensionCache[episodeId] = sourceExt

        // Already fully cached? playhead-wrj8: completeness-gated so a
        // truncated file is re-fetched rather than served, and a complete
        // pinned artifact is returned as-is (never re-fetched → never
        // rotated by a fresh DAI stitch).
        if let complete = servingURLIfComplete(for: episodeId) {
            touchAccess(episodeId: episodeId)
            return complete
        }

        // Already downloading?
        if let existing = activeDownloads[episodeId] {
            return try await existing.value
        }

        let task = Task<URL, Error> { [weak self] in
            guard let self else { throw DownloadManagerError.cancelled }
            return try await self.performDownload(episodeId: episodeId, url: url, context: context)
        }

        activeDownloads[episodeId] = task

        do {
            let result = try await task.value
            activeDownloads[episodeId] = nil
            return result
        } catch {
            activeDownloads[episodeId] = nil
            throw error
        }
    }

    /// Core download logic: downloads to a temp file, then moves to cache.
    /// Uses URLSession.shared.download(for:) to avoid byte-at-a-time iteration.
    private func performDownload(episodeId: String, url: URL, context: DownloadContext) async throws -> URL {
        let completeURL = completeFileURL(for: episodeId)

        let request = URLRequest(url: url)
        let fm = FileManager.default

        // playhead-xsdz.71 (Signal 1, additive): observe the enclosure's
        // redirect chain when a recorder is wired. The delegate only records hop
        // hosts and returns the proposed redirect, so passing it (or `nil`) is
        // byte-identical to today's `download(for:)`.
        let redirectDelegate = makeRedirectRecordingDelegate(url: url, context: context)

        // Download to a temporary file (handled efficiently by URLSession).
        let (tempURL, response) = try await URLSession.shared.download(
            for: request, delegate: redirectDelegate
        )
        // Clean up temp file on any error path.
        defer { try? fm.removeItem(at: tempURL) }
        try Task.checkCancellation()

        guard let httpResponse = response as? HTTPURLResponse,
              (200...299).contains(httpResponse.statusCode) else {
            let code = (response as? HTTPURLResponse)?.statusCode ?? 0
            throw DownloadManagerError.downloadFailed(episodeId, "HTTP \(code)")
        }

        // playhead-xsdz.71 (Signal 1): record the observed redirect chain
        // (no-op unless a recorder is wired).
        recordDAIStitchChain(
            delegate: redirectDelegate, context: context, finalHost: httpResponse.url?.host
        )

        // Harvest HTTP metadata for weak fingerprinting.
        let reportedLength = httpResponse.expectedContentLength
        let totalContentLength: Int64? = reportedLength > 0 ? reportedLength : nil
        let metadata = HTTPAssetMetadata(
            etag: httpResponse.value(forHTTPHeaderField: "ETag"),
            contentLength: totalContentLength,
            lastModified: httpResponse.value(forHTTPHeaderField: "Last-Modified")
        )
        metadataCache[episodeId] = metadata

        // Build weak fingerprint immediately.
        let weakFP = AudioFingerprint.makeWeak(url: url, metadata: metadata)
        fingerprintCache[episodeId] = AudioFingerprint(weak: weakFP, strong: nil)

        // playhead-wrj8: refuse to overwrite an already-complete pinned
        // artifact. If a complete file materialized between the
        // early-return check and now (a concurrent writer, or a bg
        // pre-cache that finished first), keep it — never replace the
        // played/analyzed bytes with a freshly-cut DAI stitch. The temp
        // file is cleaned up by the `defer` above.
        if let existing = servingURLIfComplete(for: episodeId) {
            touchAccess(episodeId: episodeId)
            logger.info("Download for \(episodeId): complete pinned artifact already present — keeping it, discarding re-fetch")
            return existing
        }

        // Publish an incomplete pin BEFORE placing bytes at the canonical
        // path. If the process dies between placement and finalization, the
        // leftover file remains withheld instead of being mistaken for a
        // legacy no-pin artifact on the next launch.
        try requirePinWrite(
            AudioAssetPin(
                expectedBytes: Int64.max,
                sha256: nil,
                sourceURL: url.absoluteString,
                etag: metadata.etag
            ),
            for: episodeId
        )

        // Move temp -> complete first, then hash from final location. An
        // incomplete leftover (partial from a failed stream) is safe to
        // replace — `servingURLIfComplete` returned nil for it above.
        try removeAllAudioArtifacts(for: episodeId)
        try fm.copyItem(at: tempURL, to: completeURL)

        // Get the file size.
        let attrs = try fm.attributesOfItem(atPath: completeURL.path)
        let downloaded = (attrs[.size] as? Int64) ?? 0

        // Compute strong fingerprint from the final file.
        let strongHash = try FileHasher.sha256(fileURL: completeURL)
        fingerprintCache[episodeId] = AudioFingerprint(weak: weakFP, strong: strongHash)

        // playhead-wrj8: pin the artifact as COMPLETE. From here the file
        // is immutable — cachedFileURL/streaming cache-hit/overwrite guards
        // all treat it as the single served copy.
        try requirePinWrite(
            AudioAssetPin(
                expectedBytes: downloaded,
                sha256: strongHash,
                sourceURL: url.absoluteString,
                etag: metadata.etag
            ),
            for: episodeId
        )
        rememberStrongPinVerification(
            episodeId: episodeId,
            fileURL: completeURL,
            expectedBytes: downloaded,
            expectedHash: strongHash
        )

        // Enqueue pre-analysis if scheduler is wired up.
        if let scheduler = analysisWorkScheduler {
            await scheduler.enqueue(
                episodeId: episodeId,
                podcastId: context.podcastId,
                downloadId: episodeId,
                sourceFingerprint: strongHash,
                isExplicitDownload: context.isExplicitDownload,
                podcastTitle: context.podcastTitle,
                episodeTitle: context.episodeTitle
            )
        }

        touchAccess(episodeId: episodeId)

        logger.info("Download complete for \(episodeId): \(downloaded) bytes, hash=\(strongHash.prefix(16))...")

        finishAudioDataSubscribers()

        // Evict if over budget.
        try await evictIfNeeded()

        broadcastProgress(DownloadProgress(
            episodeId: episodeId,
            bytesWritten: downloaded,
            totalBytes: downloaded
        ))

        return completeURL
    }

    // MARK: - Streaming Download (Play While Downloading)

    /// Minimum bytes before signaling playback can start.
    /// ~60s at 256 kbps = ~1.9 MB. Round up to 2 MB.
    static let defaultPlayableThreshold: Int64 = 8 * 1024 * 1024

    /// Result of a streaming download: the local file URL is available for
    /// playback once the threshold is reached; await `downloadComplete` before
    /// starting analysis (which needs the full file).
    struct StreamingDownloadResult: Sendable {
        /// Local file URL — available for playback immediately.
        let fileURL: URL
        /// Total expected file size from HTTP Content-Length, or nil if unknown.
        let totalBytes: Int64?
        /// Audio content type UTI (e.g. "public.mp3").
        let contentType: String
        /// Resolves when the entire file has been written to disk.
        let downloadComplete: @Sendable () async throws -> Void
        /// Cancels this exact transfer. A replacement transfer cannot be
        /// cancelled accidentally because the closure carries its token.
        let cancel: @Sendable () -> Void

        init(
            fileURL: URL,
            totalBytes: Int64?,
            contentType: String,
            downloadComplete: @escaping @Sendable () async throws -> Void,
            cancel: @escaping @Sendable () -> Void = {}
        ) {
            self.fileURL = fileURL
            self.totalBytes = totalBytes
            self.contentType = contentType
            self.downloadComplete = downloadComplete
            self.cancel = cancel
        }
    }

    /// Supersedes the singular streaming lane before any shared cache path is
    /// recreated. The old file descriptor may finish writing its now-unlinked
    /// inode, but it no longer owns a path, pin, subscriber, or finalization
    /// token that can affect the replacement.
    private func beginStreamingTransfer(episodeId: String, fileURL: URL) -> UUID {
        cancelActiveStreamingTransfer()
        let id = UUID()
        activeStreamingTransfer = ActiveStreamingTransfer(
            id: id,
            episodeId: episodeId,
            fileURL: fileURL,
            task: nil
        )
        return id
    }

    private func isCurrentStreamingTransfer(
        episodeId: String,
        transferId: UUID
    ) -> Bool {
        activeStreamingTransfer?.id == transferId
            && activeStreamingTransfer?.episodeId == episodeId
    }

    private func installStreamingTransferTask(
        _ task: Task<Void, Never>,
        episodeId: String,
        transferId: UUID
    ) {
        guard isCurrentStreamingTransfer(
            episodeId: episodeId,
            transferId: transferId
        ) else {
            task.cancel()
            return
        }
        activeStreamingTransfer?.task = task
    }

    private func cancelActiveStreamingTransfer() {
        guard let active = activeStreamingTransfer else { return }
        active.task?.cancel()
        activeStreamingTransfer = nil
        do {
            try removeAllAudioArtifacts(for: active.episodeId)
            deletePin(for: active.episodeId)
        } catch {
            // The incomplete pin is the fail-closed barrier. If any artifact
            // cannot be removed, keep the pin so leftover partial bytes can
            // never be reinterpreted as a legacy complete cache entry.
            logger.error(
                "Failed to remove cancelled stream artifacts for \(active.episodeId, privacy: .public); retaining incomplete pin: \(String(describing: error), privacy: .public)"
            )
        }
        finishAudioDataSubscribers()
    }

    private func cancelStreamingTransfer(
        episodeId: String,
        transferId: UUID
    ) {
        guard isCurrentStreamingTransfer(
            episodeId: episodeId,
            transferId: transferId
        ) else {
            return
        }
        cancelActiveStreamingTransfer()
    }

    private func broadcastAudioData(
        _ chunk: AudioDataChunk,
        episodeId: String,
        transferId: UUID
    ) {
        guard isCurrentStreamingTransfer(
            episodeId: episodeId,
            transferId: transferId
        ) else {
            return
        }
        broadcastAudioData(chunk)
    }

    private func broadcastStreamingProgress(
        _ progress: DownloadProgress,
        episodeId: String,
        transferId: UUID
    ) {
        guard isCurrentStreamingTransfer(
            episodeId: episodeId,
            transferId: transferId
        ) else {
            return
        }
        broadcastProgress(progress)
    }

    /// Hashes and finalizes the canonical path while actor isolation prevents
    /// a replacement from interleaving between the ownership check and the
    /// synchronous filesystem reads. Returns `accepted == false` for a stale
    /// detached pump, which must finish only its private completion stream.
    private func finalizeStreamingTransfer(
        episodeId: String,
        transferId: UUID,
        sourceURL: String,
        etag: String?,
        weakFingerprint: String,
        bytesWritten: Int64,
        context: DownloadContext
    ) async throws -> (accepted: Bool, strongHash: String?) {
        guard let active = activeStreamingTransfer,
              active.id == transferId,
              active.episodeId == episodeId else {
            return (false, nil)
        }

        let fileURL = active.fileURL
        let strongHash = try? FileHasher.sha256(fileURL: fileURL)
        try finalizeStreamingPin(
            episodeId: episodeId,
            fileURL: fileURL,
            sourceURL: sourceURL,
            etag: etag,
            sha256: strongHash
        )
        if let strongHash {
            setFingerprint(
                episodeId: episodeId,
                weak: weakFingerprint,
                strong: strongHash
            )
        }

        // Publish completion and release ownership before the optional
        // scheduler/eviction awaits. At this point the full canonical artifact
        // and its completeness pin are already immutable and serveable.
        extensionCache[episodeId] = fileURL.pathExtension
        activeStreamingTransfer = nil
        finishAudioDataSubscribers()
        touchAccess(episodeId: episodeId)
        broadcastProgress(
            DownloadProgress(
                episodeId: episodeId,
                bytesWritten: bytesWritten,
                totalBytes: bytesWritten
            )
        )

        if let strongHash {
            await enqueueAnalysisIfNeeded(
                episodeId: episodeId,
                sourceFingerprint: strongHash,
                context: context
            )
        }
        try await evictIfNeeded()
        return (true, strongHash)
    }

    #if DEBUG
    /// Deterministic ownership seams for the stale-completion regression.
    /// They exercise the same begin/finalize helpers as the network path
    /// without depending on URLSession timing.
    func _beginStreamingTransferForTesting(
        episodeId: String,
        fileExtension: String? = nil
    ) -> UUID {
        if let fileExtension {
            extensionCache[episodeId] = fileExtension
        }
        return beginStreamingTransfer(
            episodeId: episodeId,
            fileURL: completeFileURL(for: episodeId)
        )
    }

    func _setExtensionCacheForTesting(
        episodeId: String,
        fileExtension: String
    ) {
        extensionCache[episodeId] = fileExtension
    }

    func _isBackgroundDownloadInFlightForTesting(
        episodeId: String
    ) -> Bool {
        bgInFlightEpisodes.contains(episodeId)
    }

    func _registerBackgroundTransferForTesting(
        episodeId: String,
        identity requestedIdentity: BackgroundTransferIdentity? = nil
    ) -> BackgroundTransferIdentity {
        let identity = requestedIdentity ?? BackgroundTransferIdentity(
            sessionIdentifier: "test-session",
            taskIdentifier: Int.random(in: 1...Int.max)
        )
        retiredBackgroundTransfers.remove(identity)
        activeBackgroundTransfers[identity] = episodeId
        bgInFlightEpisodes.insert(episodeId)
        return identity
    }

    /// playhead-nsjn: drives the real terminal-callback bookkeeping so a rail
    /// can ask the question that matters about an abandoned transfer — not
    /// "is the map tidy?" but "does the NEXT attempt's completion still
    /// release the episode?". Reading the map directly would let an
    /// implementation that tidies a different collection pass.
    func _finishBackgroundTransferForTesting(
        identity: BackgroundTransferIdentity,
        episodeId: String
    ) {
        finishBackgroundTransfer(identity: identity, episodeId: episodeId)
    }

    func _strongPinVerificationHashCountForTesting() -> Int {
        strongPinVerificationHashCount
    }

    func _backgroundDownloadAdmissionCountForTesting() -> Int {
        backgroundDownloadAdmissionCountForTesting
    }

    func _setAudioArtifactURLsForTesting(
        episodeId: String,
        candidates: [URL]?
    ) {
        audioArtifactURLsOverrideForTesting[episodeId] = candidates
    }

    func _setForcePinWriteFailureForTesting(_ enabled: Bool) {
        forcePinWriteFailureForTesting = enabled
    }

    func _hasAccessIndexForTesting(episodeId: String) -> Bool {
        accessLog[episodeId] != nil
    }

    func _analysisProtectionCountForTesting(
        episodeId: String
    ) -> Int {
        analysisProtectedEpisodes[episodeId] ?? 0
    }

    func _finalizeStreamingTransferForTesting(
        episodeId: String,
        transferId: UUID,
        bytesWritten: Int64
    ) async -> Bool {
        do {
            return try await finalizeStreamingTransfer(
                episodeId: episodeId,
                transferId: transferId,
                sourceURL: "https://example.invalid/\(episodeId).mp3",
                etag: nil,
                weakFingerprint: "test-weak",
                bytesWritten: bytesWritten,
                // playhead-kkzu: a DEBUG harness hook that is not modelling a
                // real show. Named, not defaulted.
                context: .unattributed(
                    reason: .testHarness, isExplicitDownload: false
                )
            ).accepted
        } catch {
            return false
        }
    }
    #endif

    /// Downloads episode audio incrementally, returning the local file URL
    /// as soon as `playableThreshold` bytes have been written.
    /// The download continues in the background until complete.
    ///
    /// If the file is already fully cached, returns immediately with a
    /// no-op `downloadComplete`.
    func streamingDownload(
        episodeId: String,
        from url: URL,
        playableThreshold: Int64 = DownloadManager.defaultPlayableThreshold,
        // playhead-kkzu: required, with no default. The played path already
        // supplied a context; removing the default is what keeps the NEXT
        // caller from omitting it and landing a NULL show in analysis_jobs.
        context: DownloadContext
    ) async throws -> StreamingDownloadResult {
        if let existing = activeDownloads[episodeId] {
            existing.cancel()
            activeDownloads[episodeId] = nil
        }

        let sourceExt = Self.cacheExtension(for: url)
        extensionCache[episodeId] = sourceExt

        // A playback request owns the singular streaming lane even when its
        // target is already cached. This closes the replacement race where an
        // older episode's byte pump could otherwise outlive the cached switch
        // and later finish the new episode's global subscribers.
        cancelActiveStreamingTransfer()

        let completeURL = completeFileURL(for: episodeId)
        // playhead-wrj8: completeness-gated cache-hit. A COMPLETE pinned
        // artifact is served as-is (never re-streamed → the played bytes
        // can't be swapped for a different DAI stitch). A mid-stream /
        // truncated leftover (pin present but under-length) is NOT a
        // cache-hit and falls through to a fresh stream below.
        if let complete = servingURLIfComplete(for: episodeId) {
            touchAccess(episodeId: episodeId)
            let uti: String
            switch pinReadState(for: episodeId) {
            case .valid(let pin)
                where pin.sourceURL.flatMap(URL.init(string:)) != nil:
                let sourceURL = pin.sourceURL
                    .flatMap(URL.init(string:))!
                // Managed artifacts retain their true enclosure identity in
                // the pin. This matters when a routing suffix was normalized
                // to `.mp3` for cache discoverability: the filename is not
                // evidence that the bytes are MP3.
                uti = Self.playbackContentType(
                    sourceURL: sourceURL,
                    mimeType: nil
                )
            case .valid:
                // A managed artifact with no source provenance may have been
                // assigned the `.mp3` fallback solely for discoverability.
                // Let AVFoundation inspect it instead of asserting MP3 bytes.
                uti = "public.audio"
            case .absent:
                // Legacy no-pin files have no stronger provenance, so retain
                // the historical on-disk-extension behavior.
                uti = Self.utiForExtension(complete.pathExtension)
            case .invalid:
                // `servingURLIfComplete` fails closed for this case. Keep the
                // branch total in case the sidecar changes between reads.
                uti = "public.audio"
            }
            let attrs = try? FileManager.default.attributesOfItem(atPath: complete.path)
            let size = (attrs?[.size] as? Int64)
            return StreamingDownloadResult(fileURL: complete, totalBytes: size, contentType: uti, downloadComplete: {})
        }

        let transferId = beginStreamingTransfer(
            episodeId: episodeId,
            fileURL: completeURL
        )

        // Seed the fail-closed sidecar BEFORE creating/replacing anything at
        // the canonical audio path. This ordering makes a crash at every
        // later instruction safe: a file can never appear in the managed
        // cache without either a valid incomplete or complete pin.
        let fm = FileManager.default
        do {
            try requirePinWrite(
                AudioAssetPin(
                    expectedBytes: Int64.max,
                    sha256: nil,
                    sourceURL: url.absoluteString,
                    etag: nil
                ),
                for: episodeId
            )
            try removeAllAudioArtifacts(for: episodeId)
        } catch {
            cancelStreamingTransfer(
                episodeId: episodeId,
                transferId: transferId
            )
            throw error
        }
        // playhead-h3h: create with explicit
        // `.completeUntilFirstUserAuthentication` rather than letting the
        // file inherit the system default. Aligns with the AnalysisStore
        // protection class so a BGProcessingTask reading cached audio
        // pre-first-unlock cannot fail with EPERM mid-pipeline.
        fm.createFile(
            atPath: completeURL.path,
            contents: nil,
            attributes: [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication]
        )
        let fileHandle: FileHandle
        do {
            fileHandle = try FileHandle(forWritingTo: completeURL)
        } catch {
            cancelStreamingTransfer(
                episodeId: episodeId,
                transferId: transferId
            )
            throw error
        }
        var didHandOffBytePump = false
        defer {
            if !didHandOffBytePump {
                try? fileHandle.close()
                cancelStreamingTransfer(
                    episodeId: episodeId,
                    transferId: transferId
                )
            }
        }

        // playhead-wrj8 (R1): seed an always-incomplete pin (Int64.max) the
        // instant the empty file exists, BEFORE the network await below.
        // Without it, `servingURLIfComplete` treats the freshly-created
        // 0-byte / mid-connection file as complete-by-existence (no pin yet)
        // for the whole connection-setup window, so a concurrent cache-hit
        // reader could be handed a truncated file — the exact "serve a
        // partial" hole the invariant forbids. The real Content-Length
        // rewrites this a few lines down; `finalizeStreamingPin` stamps the
        // true length at completion.
        try requirePinWrite(
            AudioAssetPin(
                expectedBytes: Int64.max,
                sha256: nil,
                sourceURL: url.absoluteString,
                etag: nil
            ),
            for: episodeId
        )

        let request = URLRequest(url: url)
        // playhead-xsdz.71 (Signal 1, additive): observe the enclosure's
        // redirect chain when a recorder is wired. Behavior-preserving — the
        // delegate only records hop hosts and follows the proposed redirect, so
        // passing it (or `nil`) is byte-identical to today's `bytes(for:)`.
        let redirectDelegate = makeRedirectRecordingDelegate(url: url, context: context)
        let cancelTransfer: @Sendable () -> Void = { [weak self] in
            Task {
                await self?.cancelStreamingTransfer(
                    episodeId: episodeId,
                    transferId: transferId
                )
            }
        }
        let (bytes, response) = try await withTaskCancellationHandler {
            try await URLSession.shared.bytes(
                for: request,
                delegate: redirectDelegate
            )
        } onCancel: {
            cancelTransfer()
        }
        guard !Task.isCancelled,
              isCurrentStreamingTransfer(
            episodeId: episodeId,
            transferId: transferId
        ) else {
            throw DownloadManagerError.cancelled
        }

        guard let httpResponse = response as? HTTPURLResponse,
              (200...299).contains(httpResponse.statusCode) else {
            let code = (response as? HTTPURLResponse)?.statusCode ?? 0
            try? fileHandle.close()
            do {
                try removeAllAudioArtifacts(for: episodeId)
                // Drop the seed pin only after proving no audio sibling
                // remains. Otherwise partial bytes would become a legacy hit.
                deletePin(for: episodeId)
            } catch {
                logger.error(
                    "Failed to remove rejected stream artifacts for \(episodeId, privacy: .public); retaining incomplete pin: \(String(describing: error), privacy: .public)"
                )
            }
            throw DownloadManagerError.downloadFailed(episodeId, "HTTP \(code)")
        }

        // playhead-xsdz.71 (Signal 1): record the observed redirect chain now
        // that the response headers are in (the redirects completed during the
        // `bytes(for:)` await). No-op unless a recorder is wired. Fired before
        // the detached streaming body so it stays off the byte-copy loop.
        recordDAIStitchChain(
            delegate: redirectDelegate, context: context, finalHost: httpResponse.url?.host
        )

        // Harvest HTTP metadata for weak fingerprinting.
        let reportedLength = httpResponse.expectedContentLength
        let totalContentLength: Int64? = reportedLength > 0 ? reportedLength : nil
        let metadata = HTTPAssetMetadata(
            etag: httpResponse.value(forHTTPHeaderField: "ETag"),
            contentLength: totalContentLength,
            lastModified: httpResponse.value(forHTTPHeaderField: "Last-Modified")
        )
        metadataCache[episodeId] = metadata
        let weakFP = AudioFingerprint.makeWeak(url: url, metadata: metadata)
        fingerprintCache[episodeId] = AudioFingerprint(weak: weakFP, strong: nil)

        // playhead-wrj8: seed an INCOMPLETE pin — `expectedBytes` is the
        // full Content-Length (or Int64.max when the server omits it).
        // While the file grows below its size stays under `expectedBytes`,
        // so `servingURLIfComplete` withholds it from every cache path
        // (and a force-quit mid-stream leaves it withheld across relaunch,
        // never serving a truncated file). `finalizeStreamingPin` rewrites
        // it to the real length on completion.
        try requirePinWrite(
            AudioAssetPin(
                expectedBytes: totalContentLength ?? Int64.max,
                sha256: nil,
                sourceURL: url.absoluteString,
                etag: metadata.etag
            ),
            for: episodeId
        )

        let signalURL = completeURL
        let threshold = min(playableThreshold, totalContentLength ?? playableThreshold)
        let audioUTI = Self.playbackContentType(
            sourceURL: url,
            mimeType: httpResponse.mimeType
        )

        // Completion continuation — signaled when the full file is written.
        let completionStream = AsyncStream<Result<Void, Error>>.makeStream()
        let waitForComplete: @Sendable () async throws -> Void = {
            try await withTaskCancellationHandler {
                for await result in completionStream.0 {
                    switch result {
                    case .success: return
                    case .failure(let error): throw error
                    }
                }
            } onCancel: {
                cancelTransfer()
            }
        }

        // Playback-ready continuation — signaled when threshold is reached.
        let result: StreamingDownloadResult = try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let capturedLogger = self.logger
                let capturedEpisodeId = episodeId
                // playhead-wrj8: carry the source URL + validator into the
                // detached completion so the finalized pin records them.
                let capturedSourceURL = url.absoluteString
                let capturedEtag = metadata.etag
                let completionContinuation = completionStream.1
                let task = Task.detached { [weak self] in
                    var bytesWritten: Int64 = 0
                    var signaled = false
                    var buffer = Data()
                    let flushSize = 64 * 1024

                    do {
                        for try await byte in bytes {
                            buffer.append(byte)

                            if buffer.count >= flushSize {
                                guard !Task.isCancelled,
                                    await self?.isCurrentStreamingTransfer(
                                        episodeId: capturedEpisodeId,
                                        transferId: transferId
                                    ) == true
                                else {
                                    throw DownloadManagerError.cancelled
                                }
                                fileHandle.write(buffer)
                                bytesWritten += Int64(buffer.count)
                                await self?.broadcastAudioData(
                                    AudioDataChunk(
                                        episodeId: capturedEpisodeId,
                                        data: buffer,
                                        totalBytesWritten: bytesWritten
                                    ),
                                    episodeId: capturedEpisodeId,
                                    transferId: transferId
                                )
                                buffer.removeAll(keepingCapacity: true)

                                if !signaled, bytesWritten >= threshold {
                                    signaled = true
                                    capturedLogger.info(
                                        "Playable threshold reached for \(capturedEpisodeId): \(bytesWritten) bytes")
                                    continuation.resume(
                                        returning: StreamingDownloadResult(
                                            fileURL: signalURL,
                                            totalBytes: totalContentLength,
                                            contentType: audioUTI,
                                            downloadComplete: waitForComplete,
                                            cancel: cancelTransfer
                                        ))
                                }

                                await self?.broadcastStreamingProgress(
                                    DownloadProgress(
                                        episodeId: capturedEpisodeId,
                                        bytesWritten: bytesWritten,
                                        totalBytes: totalContentLength ?? bytesWritten
                                    ),
                                    episodeId: capturedEpisodeId,
                                    transferId: transferId
                                )
                            }
                        }

                        // Flush remaining bytes.
                        if !buffer.isEmpty {
                            guard !Task.isCancelled,
                                await self?.isCurrentStreamingTransfer(
                                    episodeId: capturedEpisodeId,
                                    transferId: transferId
                                ) == true
                            else {
                                throw DownloadManagerError.cancelled
                            }
                            fileHandle.write(buffer)
                            bytesWritten += Int64(buffer.count)
                            await self?.broadcastAudioData(
                                AudioDataChunk(
                                    episodeId: capturedEpisodeId,
                                    data: buffer,
                                    totalBytesWritten: bytesWritten
                                ),
                                episodeId: capturedEpisodeId,
                                transferId: transferId
                            )
                        }
                        try fileHandle.close()

                        // If file was smaller than threshold, signal both at once.
                        if !signaled {
                            guard
                                await self?.isCurrentStreamingTransfer(
                                    episodeId: capturedEpisodeId,
                                    transferId: transferId
                                ) == true
                            else {
                                throw DownloadManagerError.cancelled
                            }
                            signaled = true
                            continuation.resume(
                                returning: StreamingDownloadResult(
                                    fileURL: signalURL,
                                    totalBytes: totalContentLength,
                                    contentType: audioUTI,
                                    downloadComplete: waitForComplete,
                                    cancel: cancelTransfer
                                ))
                        }

                        let completion = try await self?.finalizeStreamingTransfer(
                            episodeId: capturedEpisodeId,
                            transferId: transferId,
                            sourceURL: capturedSourceURL,
                            etag: capturedEtag,
                            weakFingerprint: weakFP,
                            bytesWritten: bytesWritten,
                            context: context
                        )
                        guard completion?.accepted == true else {
                            throw DownloadManagerError.cancelled
                        }
                        if let strongHash = completion?.strongHash {
                            capturedLogger.info(
                                "Download complete for \(capturedEpisodeId): \(bytesWritten) bytes, hash=\(strongHash.prefix(16))..."
                            )
                        }

                        // Signal download complete.
                        completionContinuation.yield(.success(()))
                        completionContinuation.finish()
                    } catch {
                        try? fileHandle.close()
                        if !signaled {
                            continuation.resume(throwing: error)
                        }
                        await self?.cancelStreamingTransfer(
                            episodeId: capturedEpisodeId,
                            transferId: transferId
                        )
                        completionContinuation.yield(.failure(error))
                        completionContinuation.finish()
                        capturedLogger.error("Streaming download failed for \(capturedEpisodeId): \(error)")
                    }
                }
                self.installStreamingTransferTask(
                    task,
                    episodeId: capturedEpisodeId,
                    transferId: transferId
                )
            }
        } onCancel: {
            cancelTransfer()
        }

        didHandOffBytePump = true
        return result
    }

    /// Helper for detached task to update fingerprint cache.
    fileprivate func setFingerprint(episodeId: String, weak weakFP: String, strong strongFP: String) {
        fingerprintCache[episodeId] = AudioFingerprint(weak: weakFP, strong: strongFP)
    }

    /// Helper for detached task to enqueue analysis after download completes.
    fileprivate func enqueueAnalysisIfNeeded(
        episodeId: String,
        sourceFingerprint: String,
        context: DownloadContext
    ) async {
        guard let scheduler = analysisWorkScheduler else { return }
        await scheduler.enqueue(
            episodeId: episodeId,
            podcastId: context.podcastId,
            downloadId: episodeId,
            sourceFingerprint: sourceFingerprint,
            isExplicitDownload: context.isExplicitDownload,
            // playhead-i9dj: human-readable titles flow through to
            // AnalysisStore writes inside the scheduler so an exported
            // analysis.sqlite is legible on its own.
            podcastTitle: context.podcastTitle,
            episodeTitle: context.episodeTitle
        )
    }

    /// Map file extension to UTI for AVAssetResourceLoaderDelegate.
    static func utiForExtension(_ ext: String) -> String {
        switch ext.lowercased() {
        case "mp3":  return "public.mp3"
        case "m4a":  return "public.mpeg-4-audio"
        case "aac":  return "public.aac-audio"
        case "wav":  return "com.microsoft.waveform-audio"
        case "mp4":  return "public.mpeg-4"
        case "ogg":  return "org.xiph.ogg"
        case "opus": return "org.xiph.opus"
        default:     return "public.audio"
        }
    }

    /// Content type advertised to AVAsset's resource loader. Cache-path
    /// normalization and media typing are intentionally separate: a routing
    /// URL ending in `.php` is stored under a discoverable `.mp3` fallback,
    /// but must not be asserted to contain MP3 bytes. Prefer a known source
    /// suffix, then the HTTP MIME type, and otherwise use the neutral audio
    /// supertype so AVFoundation can inspect the stream.
    static func playbackContentType(
        sourceURL: URL,
        mimeType: String?
    ) -> String {
        let sourceExtension = sourceURL.pathExtension.lowercased()
        if knownAudioExtensions.contains(sourceExtension) {
            return utiForExtension(sourceExtension)
        }
        switch mimeType?.lowercased() {
        case "audio/mpeg", "audio/mp3":
            return "public.mp3"
        case "audio/mp4", "audio/m4a", "audio/x-m4a":
            return "public.mpeg-4-audio"
        case "audio/aac", "audio/aacp":
            return "public.aac-audio"
        case "audio/wav", "audio/x-wav":
            return "com.microsoft.waveform-audio"
        case "audio/ogg":
            return "org.xiph.ogg"
        case "audio/opus":
            return "org.xiph.opus"
        default:
            return "public.audio"
        }
    }

    // MARK: - Background-Transfer Attribution (playhead-kkzu)

    /// The on-disk form of a `DownloadContext`. Codable rather than the
    /// context itself so the stored shape is explicit and versionable.
    struct DownloadAttributionRecord: Codable, Sendable, Equatable {
        var podcastId: String?
        var unattributedReason: DownloadContext.UnattributedReason?
        var isExplicitDownload: Bool
        var podcastTitle: String?
        var episodeTitle: String?
    }

    private func attributionFileURL(episodeId: String) -> URL {
        attributionDirectory.appendingPathComponent(
            "\(Self.safeFilename(for: episodeId)).attribution"
        )
    }

    /// Records which show a background transfer belongs to, so the completion
    /// — which may land in a later process — can enqueue analysis against it.
    func persistDownloadAttribution(episodeId: String, context: DownloadContext) {
        let fm = FileManager.default
        if !fm.fileExists(atPath: attributionDirectory.path) {
            try? fm.createDirectory(
                at: attributionDirectory, withIntermediateDirectories: true
            )
        }
        let record = DownloadAttributionRecord(
            podcastId: context.podcastId,
            unattributedReason: context.unattributedReason,
            isExplicitDownload: context.isExplicitDownload,
            podcastTitle: context.podcastTitle,
            episodeTitle: context.episodeTitle
        )
        guard let encoded = try? JSONEncoder().encode(record) else { return }
        try? encoded.write(
            to: attributionFileURL(episodeId: episodeId), options: .atomic
        )
    }

    /// Reads back the attribution written when the transfer was queued.
    /// `nil` means no record was written — a transfer started by a build
    /// before this existed, or one whose sidecar was already consumed.
    func loadDownloadAttribution(episodeId: String) -> DownloadContext? {
        guard let data = try? Data(
            contentsOf: attributionFileURL(episodeId: episodeId)
        ),
            let record = try? JSONDecoder().decode(
                DownloadAttributionRecord.self, from: data
            ) else {
            return nil
        }
        return .resolving(
            podcastId: record.podcastId,
            unattributedReason:
                record.unattributedReason ?? .resumeWithoutRecordedShow,
            isExplicitDownload: record.isExplicitDownload,
            podcastTitle: record.podcastTitle,
            episodeTitle: record.episodeTitle
        )
    }

    /// Drops the sidecar once the transfer it describes has reached a terminal
    /// state. Called on every exit from `handleBackgroundDownloadComplete` and
    /// on explicit cancellation, so records do not accumulate.
    func deleteDownloadAttribution(episodeId: String) {
        try? FileManager.default.removeItem(
            at: attributionFileURL(episodeId: episodeId)
        )
    }

    // MARK: - Background Pre-Cache

    /// Queues a background download for an episode (pre-caching).
    /// Completes even if the app is suspended.
    ///
    /// playhead-kkzu: `context` is NOT optional and has no default. Every
    /// caller must state the show — or state, with a reason, that it cannot.
    /// The completion path (`handleBackgroundDownloadComplete`) is what
    /// enqueues the analysis job, and before this bead it passed `nil`
    /// unconditionally, so every auto/background download recorded a NULL
    /// `analysis_jobs.podcastId`. A defaulted parameter would let the next
    /// caller reintroduce that silently.
    ///
    /// playhead-nsjn: `async` because creating the URLSession task is a
    /// blocking synchronous XPC call to `nsurlsessiond` that must not run
    /// on a cooperative thread (see `BackgroundSessionIO.swift`). Every
    /// caller already awaited this actor method, so no call site changes;
    /// what does change is that the method now contains a suspension point,
    /// which is why the in-flight slot below is reserved BEFORE it.
    func backgroundDownload(
        episodeId: String,
        from url: URL,
        context: DownloadContext
    ) async {
        guard activeStreamingTransfer?.episodeId != episodeId else {
            logger.debug(
                "Skipping background download for \(episodeId): foreground stream active"
            )
            return
        }

        let sourceExt = Self.cacheExtension(for: url)
        extensionCache[episodeId] = sourceExt
        guard servingURLIfComplete(for: episodeId) == nil else {
            logger.debug("Skipping background download for \(episodeId): already cached")
            return
        }
        // playhead-3xtw: idempotent — never start a second concurrent
        // transfer for an episode already downloading (a rapid double-tap
        // of the prepare control, or an auto + on-demand collision). The
        // slot is cleared on completion/failure, so a genuine retry after a
        // finished attempt still proceeds.
        guard !bgInFlightEpisodes.contains(episodeId) else {
            logger.debug("Skipping background download for \(episodeId): already in flight")
            return
        }

        // playhead-kkzu: record the show BEFORE the transfer starts. The
        // completion that enqueues analysis may run in a later process, and
        // this file is the only thing that crosses that boundary.
        persistDownloadAttribution(episodeId: episodeId, context: context)
        if let reason = context.unattributedReason {
            logger.info(
                "Background download for \(episodeId, privacy: .public) carries no show identity: \(reason.rawValue, privacy: .public)"
            )
        }

        // playhead-nsjn: RESERVE the in-flight slot before suspending.
        // Creating the task now crosses an `await`, and actors are
        // re-entrant: without this, a second caller for the same episode
        // could clear the in-flight guard above while the first is still
        // inside the daemon call, and we would start two transfers.
        // Released again on every path that does not hand off a task.
        //
        // playhead-gpdb MOVED THIS UP, above the session lookup, because the
        // lookup is now a suspension point too. Left where it was, the very
        // first download of a process — the one that has to construct the
        // session — would suspend with nothing reserved, and a concurrent
        // caller for the same episode would sail through the in-flight guard
        // above. The reservation has to cover every await between the guard
        // and the handoff, not just the last one.
        bgInFlightEpisodes.insert(episodeId)

        // Pre-cache work: route through the maintenance lane when the
        // dual-session flag is on so it cannot starve user-initiated
        // (.interactive) downloads. When the flag is off, fall through
        // to the legacy single session.
        let session = await backgroundSession(
            for: useDualBackgroundSessions ? .maintenance : .legacy,
            requestedBy: .backgroundDownload
        )
        guard let session else {
            // playhead-gpdb: the daemon would not hand back a session, so
            // there is nothing to create a task on. Same recovery as the
            // unanswered `downloadTask(with:)` below — release the
            // reservation and drop the sidecar, so the episode stays
            // retryable and no record outlives the transfer it describes —
            // and the refusal is already recorded by
            // `backgroundSession(for:requestedBy:)`.
            releaseInFlightReservationIfUnclaimed(episodeId: episodeId)
            deleteDownloadAttribution(episodeId: episodeId)
            logger.error(
                "Background download for \(episodeId, privacy: .public) NOT started: the background transfer daemon would not open a session"
            )
            // playhead-7dgx: the cleanup above is correct and it destroys the
            // last trace that this episode was ever asked for. The row is what
            // survives it. `sessionCreationIO` is the bound that expired here —
            // NOT `sessionIO`; they are separate queues with separate
            // deadlines, and recording the wrong one would make this table lie
            // about the very quantity the widening decision reads.
            await recordBackgroundDownloadDrop(
                episodeId: episodeId,
                reason: .sessionNotVended,
                context: context,
                boundSeconds: sessionCreationIO.timeout
            )
            return
        }

        let handoff = await sessionIO.perform(
            label: "downloadTask(with:) for \(episodeId)",
            discardingLateResult: { $0.cancel() },
            running: {
                let task = session.downloadTask(with: url)
                task.taskDescription = episodeId
                return task
            }
        )
        guard let task = handoff else {
            // The daemon never answered. Nothing was started, so undo the
            // reservation and the attribution sidecar — otherwise the
            // episode is wedged "in flight" for the life of the process and
            // a stale sidecar outlives the transfer it describes.
            //
            // playhead-7l6n: the reservation above and any admission made
            // DURING the suspension are the same element of a per-episode
            // set, so releasing it unconditionally would also release a
            // transfer this call never owned.
            releaseInFlightReservationIfUnclaimed(episodeId: episodeId)
            deleteDownloadAttribution(episodeId: episodeId)
            logger.error(
                "Background download for \(episodeId, privacy: .public) NOT started: the background transfer daemon did not answer"
            )
            // playhead-7dgx: a DIFFERENT failure from the one above and it
            // gets a different reason. A session exists, so the download
            // subsystem is alive for this process — only this episode is lost,
            // and the remedy is per-episode rather than per-launch.
            await recordBackgroundDownloadDrop(
                episodeId: episodeId,
                reason: .transferTaskNotVended,
                context: context,
                boundSeconds: sessionIO.timeout
            )
            return
        }

        registerBackgroundTransfer(
            task: task,
            session: session,
            episodeId: episodeId
        )
        #if DEBUG
        backgroundDownloadAdmissionCountForTesting += 1
        #endif
        // playhead-3xtw (L2): reset the progress high-water mark for a fresh
        // transfer so a retry's early ticks aren't dropped as "stale".
        lastBackgroundProgressBytes[episodeId] = nil
        // `resume()` re-enters the same session queue, so it carries the
        // same blocking risk as the creation above and gets the same bound.
        // Registration precedes it so a fast terminal callback cannot beat
        // its own transfer into the identity map.
        if await sessionIO.perform(
            label: "resume() for \(episodeId)",
            running: { task.resume() }
        ) == nil {
            logger.error(
                "Background download for \(episodeId, privacy: .public) was created but not resumed: the background transfer daemon did not answer"
            )
            abandonUnstartedTransfer(
                task: task,
                session: session,
                episodeId: episodeId
            )
            deleteDownloadAttribution(episodeId: episodeId)
            // playhead-7dgx: THE THIRD ABANDONMENT IN THIS FUNCTION, and it is
            // recorded for a reason worth stating rather than leaving to
            // inference. The bead names two returns; a table called
            // `background_download_drops` that counted two of the three drops
            // in the function it is named after would be this repo's standing
            // defect class shipped as the instrument meant to catch it — a
            // value that names one thing read as though it named another. The
            // reason is distinct, so a reader who wants only the two can still
            // have them with a `WHERE reason IN (…)`.
            await recordBackgroundDownloadDrop(
                episodeId: episodeId,
                reason: .transferNotResumed,
                context: context,
                boundSeconds: sessionIO.timeout
            )
            return
        }
        logger.info("Queued background download for \(episodeId)")
    }

    /// Writes one `background_download_drops` row.
    ///
    /// Called only from the three abandonment paths in ``backgroundDownload``,
    /// each of which has ALREADY released the reservation and deleted the
    /// attribution sidecar — so this suspension point cannot strand any state:
    /// a re-entrant caller arriving here finds the episode retryable, which is
    /// exactly what the cleanup promised it.
    ///
    /// The recorder swallows its own errors, so this never throws and never
    /// changes the caller's outcome. A drop that could not be recorded is
    /// still a drop.
    private func recordBackgroundDownloadDrop(
        episodeId: String,
        reason: BackgroundDownloadDropReason,
        context: DownloadContext,
        boundSeconds: TimeInterval
    ) async {
        await dropRecorder.recordDrop(
            BackgroundDownloadDropRecord(
                episodeId: episodeId,
                reason: reason,
                context: context,
                boundSeconds: boundSeconds
            )
        )
    }

    /// playhead-nsjn: a transfer the daemon created but never started is
    /// worse than no transfer at all. It is suspended, so no delegate
    /// callback will ever fire for it, so nothing will ever release the
    /// episode's in-flight slot — every later attempt for that episode is
    /// refused for the life of the process. Release the slot here and ask
    /// the daemon to drop the task.
    ///
    /// The cancel is deliberately NOT awaited: we only get here because the
    /// daemon is already not answering, and making the caller wait a second
    /// bound to clean up after the first one just doubles the stall it is
    /// trying to escape.
    ///
    /// The identity map is drained HERE rather than being left to the
    /// delegate's terminal callback. Both call sites register before
    /// resuming, so by this point `activeBackgroundTransfers` holds an entry
    /// for a task that was never started — and on this branch the cancel is
    /// exactly as likely to go unanswered as the resume was, so no callback
    /// may ever arrive to drain it. A surviving entry does not merely leak:
    /// `finishBackgroundTransfer` only releases the in-flight slot when NO
    /// admitted identity still names the episode, so the abandoned entry
    /// would make the NEXT attempt's own completion fail to release the
    /// slot — reinstating the permanent per-episode outage this method
    /// exists to prevent, one attempt later and with no daemon stall in
    /// sight. Retiring the identity also makes a late cancel callback for
    /// the dead task a no-op instead of a spurious failure for whatever
    /// transfer holds the episode by then.
    ///
    /// playhead-7l6n: the in-flight slot is released CONDITIONALLY. This
    /// method retires exactly one identity, so it is not entitled to speak
    /// for the episode — see `releaseInFlightReservationIfUnclaimed`.
    internal func abandonUnstartedTransfer(
        task: URLSessionDownloadTask,
        session: URLSession,
        episodeId: String
    ) {
        let identity = backgroundTransferIdentity(task: task, session: session)
        retiredBackgroundTransfers.insert(identity)
        activeBackgroundTransfers.removeValue(forKey: identity)
        releaseInFlightReservationIfUnclaimed(episodeId: episodeId)
        let io = sessionIO
        Task.detached {
            _ = await io.perform(
                label: "cancel unstarted transfer for \(episodeId)",
                running: { task.cancel() }
            )
        }
    }

    // MARK: - Cancel

    /// Cancels an active download for the given episode.
    func cancelDownload(episodeId: String) async {
        var cancelled = false

        if activeStreamingTransfer?.episodeId == episodeId {
            cancelActiveStreamingTransfer()
            cancelled = true
        }

        if let task = activeDownloads[episodeId] {
            task.cancel()
            activeDownloads[episodeId] = nil
            cancelled = true
        }

        if await retireBackgroundTransfers(episodeId: episodeId) {
            cancelled = true
        }

        if cancelled {
            // playhead-kkzu: an explicit cancel is terminal for the transfer,
            // so its attribution goes with it. A later re-download re-queues
            // through `backgroundDownload`, which writes a fresh record.
            deleteDownloadAttribution(episodeId: episodeId)
            logger.info("Cancelled download for \(episodeId)")
        }
    }

    private func backgroundTransferIdentity(
        task: URLSessionTask,
        session: URLSession
    ) -> BackgroundTransferIdentity {
        BackgroundTransferIdentity(
            sessionIdentifier: session.configuration.identifier ?? "",
            taskIdentifier: task.taskIdentifier
        )
    }

    func registerBackgroundTransfer(
        task: URLSessionTask,
        session: URLSession,
        episodeId: String
    ) {
        let identity = backgroundTransferIdentity(task: task, session: session)
        retiredBackgroundTransfers.remove(identity)
        activeBackgroundTransfers[identity] = episodeId
        bgInFlightEpisodes.insert(episodeId)
    }

    /// `URLSession.allTasks` on a BACKGROUND session, bounded and run OFF the
    /// Swift Concurrency cooperative pool — the same containment
    /// `backgroundDownload` gets for `downloadTask(with:)`.
    ///
    /// playhead-rouw. `await session.allTasks` reads like a property access
    /// and is not one: on `__NSURLBackgroundSession` it takes a barrier on
    /// the session's serial work queue and asks `nsurlsessiond` over XPC. It
    /// therefore carries BOTH hazards playhead-nsjn measured — the entering
    /// call can park the calling thread, and the reply can simply never
    /// arrive. Unbounded, on an actor, that is an `await` which never
    /// resumes: the test that made the call NEVER REPORTS A VERDICT, and a
    /// full-plan gate that lost eight to eleven download tests per run for
    /// nine runs was exactly this, with no crash, no message and no crash
    /// report because nothing crashed.
    ///
    /// Two bounds, deliberately, both `io.timeout`:
    ///   * the INNER one releases the dedicated queue's single thread when
    ///     the daemon never replies, so one wedged call does not strand
    ///     `.shared` for the life of the process;
    ///   * the OUTER one (`perform`'s own deadline) releases the CALLER when
    ///     the body cannot even start, which is what happens when another
    ///     caller is holding the session's barrier.
    /// The caller therefore waits at most `io.timeout`, never longer.
    ///
    /// What that does NOT bound is how long the QUEUE stays busy. The two
    /// deadlines are independent: the outer one starts when the submission
    /// is made, the inner one when the body reaches the front. A body that
    /// starts at 9 s and then sits on a silent daemon holds the queue until
    /// 19 s, with nobody waiting on it from 10 s onward. 2 × `io.timeout`
    /// is the bound only while `getAllTasks` RETURNS so the latch wait can
    /// run; if its entering call parks in the session's own barrier — the
    /// shape playhead-nsjn sampled for `downloadTask(with:)` — the body never
    /// returns and this queue is stranded for the life of the process, which
    /// is precisely what `BackgroundSessionIO`'s header declines to claim
    /// away. Either way it costs one ordinary thread rather than a slice of
    /// the cooperative pool, which is the
    /// trade this file exists to make — but a submission arriving inside
    /// that window is released by its own deadline having never run, and
    /// logs `did not answer` for a daemon it never spoke to. `starved:
    /// reached the daemon queue after its caller had already given up` is
    /// the line that distinguishes the two; read both before concluding the
    /// daemon is at fault.
    ///
    /// `internal` rather than `private` so the force-quit scan in
    /// `ForceQuitResumeScan.swift` shares this one crossing; it is the only
    /// other place that enumerates a background session's tasks.
    internal static func boundedAllTasks(
        of session: URLSession,
        through io: BackgroundSessionIO
    ) async -> [URLSessionTask]? {
        let identifier = session.configuration.identifier ?? "<non-background>"
        let bound = io.timeout
        let answer = await io.perform(
            label: "allTasks for \(identifier)",
            running: { () -> [URLSessionTask]? in
                let reply = OSAllocatedUnfairLock<[URLSessionTask]?>(
                    initialState: nil
                )
                let latch = DispatchSemaphore(value: 0)
                session.getAllTasks { tasks in
                    reply.withLock { $0 = tasks }
                    latch.signal()
                }
                guard latch.wait(timeout: .now() + bound) == .success else {
                    return nil
                }
                return reply.withLock { $0 }
            }
        )
        // `perform` reports nil when the CALLER's bound expired; the inner
        // optional is nil when the daemon never replied. Both are "no
        // answer", so they flatten.
        return answer ?? nil
    }

    /// Cancels every matching OS-owned background task and retires both the
    /// enumerated identities and any completion already staged for an actor
    /// hop. All three known session identifiers are instantiated here
    /// deliberately: explicit deletion is not latency-sensitive like the
    /// launch scan, and must find tasks retained by an older process.
    @discardableResult
    private func retireBackgroundTransfers(
        episodeId: String?
    ) async -> Bool {
        if let episodeId {
            backgroundJournalFinalizations[episodeId]?.task.cancel()
        } else {
            for finalization in
                backgroundJournalFinalizations.values {
                finalization.task.cancel()
            }
        }
        let roles: [BackgroundSessionRole] = [
            .interactive, .maintenance, .legacy,
        ]
        var sessions: [URLSession] = []
        var seenSessionIdentifiers: Set<String> = []
        for role in roles {
            // playhead-gpdb: a role the daemon would not open contributes NO
            // session, and the sweep carries on with the ones it has. Failing
            // open here is the same trade playhead-rouw made one layer in for
            // a lost enumeration, and for the same reason: what is lost is a
            // SOURCE of transfer identities, never the deletion of the bytes.
            // The admitted-identity sweep below needs no daemon, the
            // identity-required guard is armed unconditionally, and every
            // later callback for a retired identity is discarded — so an
            // uncancelled transfer runs on and then has its bytes thrown away
            // on arrival. Returning early instead would leave the user's
            // delete undone while reporting success. The refusal is recorded
            // by `backgroundSession(for:requestedBy:)`.
            guard let session = await backgroundSession(
                for: role, requestedBy: .transferRetirement
            ) else { continue }
            let identifier = session.configuration.identifier ?? ""
            if seenSessionIdentifiers.insert(identifier).inserted {
                sessions.append(session)
            }
        }

        let io = enumerationIO
        let taskLists = await withTaskGroup(
            of: (URLSession, [URLSessionTask]?).self
        ) { group in
            for session in sessions {
                group.addTask {
                    (session, await Self.boundedAllTasks(of: session, through: io))
                }
            }
            var result: [(URLSession, [URLSessionTask]?)] = []
            for await item in group {
                result.append(item)
            }
            return result
        }

        var retiredAny = false
        for (session, answer) in taskLists {
            guard let tasks = answer else {
                // The enumeration is a source of identities, not the only
                // one — but it is the only source of TASK HANDLES, and
                // playhead-rouw R2 measured that distinction rather than
                // assuming it. Losing the enumeration costs the OS-side
                // `cancel()` for EVERY matching transfer, admitted or not:
                // the sweep below holds `BackgroundTransferIdentity` values,
                // never `URLSessionTask`s, so it retires an identity without
                // being able to cancel the transfer wearing it. (An earlier
                // draft of this comment said the loss was confined to tasks
                // this process never admitted; it is not.)
                //
                // What survives is the half that protects the BYTES: the
                // admitted-identity sweep still runs, the identity-required
                // guard is still armed unconditionally below, and every later
                // callback for a retired identity is still discarded — so an
                // uncancelled transfer runs on, and then has its bytes thrown
                // away on arrival. playhead-sq80 owns the one caller
                // (`cancelDownload`) that reads the return value.
                logger.error(
                    "retireBackgroundTransfers: \(session.configuration.identifier ?? "<none>", privacy: .public) did not answer allTasks — retiring from the admitted identity map only"
                )
                continue
            }
            for task in tasks {
                guard let taskEpisodeId = task.taskDescription,
                      episodeId == nil || taskEpisodeId == episodeId
                else {
                    continue
                }
                let identity = backgroundTransferIdentity(
                    task: task,
                    session: session
                )
                retiredBackgroundTransfers.insert(identity)
                activeBackgroundTransfers.removeValue(forKey: identity)
                task.cancel()
                retiredAny = true
            }
        }

        // A completed task can disappear from URLSession.allTasks after its
        // delegate staged bytes but before the actor receives that callback.
        // Retire the manager's admitted identity map as a second source.
        let tracked = activeBackgroundTransfers.filter {
            episodeId == nil || $0.value == episodeId
        }
        for (identity, _) in tracked {
            retiredBackgroundTransfers.insert(identity)
            activeBackgroundTransfers.removeValue(forKey: identity)
            retiredAny = true
        }

        if let episodeId {
            backgroundIdentityRequiredEpisodes.insert(episodeId)
            bgInFlightEpisodes.remove(episodeId)
            lastBackgroundProgressBytes.removeValue(forKey: episodeId)
        } else {
            requireRegisteredBackgroundIdentityAfterBulkClear = true
            bgInFlightEpisodes.removeAll()
            lastBackgroundProgressBytes.removeAll()
        }
        return retiredAny
    }

    private func backgroundCallbackIsRetired(
        identity: BackgroundTransferIdentity,
        episodeId: String
    ) -> Bool {
        if retiredBackgroundTransfers.contains(identity) {
            return true
        }
        let identityIsRegistered =
            activeBackgroundTransfers[identity] == episodeId
        if requireRegisteredBackgroundIdentityAfterBulkClear,
           !identityIsRegistered {
            return true
        }
        if backgroundIdentityRequiredEpisodes.contains(episodeId),
           !identityIsRegistered {
            return true
        }
        return false
    }

    private func backgroundCompletionLostOwnership(
        identity: BackgroundTransferIdentity?,
        episodeId: String,
        capturedCacheOwnershipGeneration: UInt64
    ) -> Bool {
        if cacheOwnershipGenerationByEpisode[
            episodeId,
            default: 0
        ] != capturedCacheOwnershipGeneration {
            return true
        }
        guard let identity else { return false }
        return backgroundCallbackIsRetired(
            identity: identity,
            episodeId: episodeId
        )
    }

    private func finishBackgroundTransfer(
        identity: BackgroundTransferIdentity?,
        episodeId: String
    ) {
        if let identity {
            activeBackgroundTransfers.removeValue(forKey: identity)
            retiredBackgroundTransfers.remove(identity)
        }
        releaseInFlightReservationIfUnclaimed(episodeId: episodeId)
    }

    /// playhead-7l6n: releases the episode's in-flight reservation only when
    /// no admitted identity still names it.
    ///
    /// `bgInFlightEpisodes` is keyed by EPISODE, not by transfer, and it is
    /// read for two different purposes: `backgroundDownload`'s idempotence
    /// guard, and eviction protection (`evictIfNeeded` both builds its
    /// known-episode map from it and skips any episode it contains). More
    /// than one identity can legitimately name the same episode at once —
    /// `handleBackgroundDownloadComplete` admits a task reattached from a
    /// prior process with no in-flight guard at all, and can do so while
    /// `backgroundDownload` or `resumeSuspendedTransfer` is suspended inside
    /// a `sessionIO` call. A caller that retired ONE identity therefore does
    /// not know the episode is free; removing it anyway strips the other
    /// transfer's eviction protection while its completion is still running.
    ///
    /// `retireBackgroundTransfers` is the one place an unconditional removal
    /// is correct, and the difference is exactly this: it drains EVERY
    /// identity naming the episode first, so by the time it clears the set
    /// there is provably no other claimant. Do not copy its shape here.
    private func releaseInFlightReservationIfUnclaimed(episodeId: String) {
        guard !activeBackgroundTransfers.values.contains(episodeId) else {
            return
        }
        bgInFlightEpisodes.remove(episodeId)
    }

    private func handleBackgroundDownloadFailed(
        _ failure: BackgroundTransferFailure
    ) async {
        guard !backgroundCallbackIsRetired(
            identity: failure.identity,
            episodeId: failure.episodeId
        ) else {
            // A cancellation can itself produce resume data. Explicit cache
            // deletion is terminal for that transfer, so remove any older
            // retained blob and never persist the cancellation payload.
            try? deleteResumeData(episodeId: failure.episodeId)
            finishBackgroundTransfer(
                identity: failure.identity,
                episodeId: failure.episodeId
            )
            return
        }

        if let resumeData = failure.resumeData {
            do {
                try persistResumeData(
                    episodeId: failure.episodeId,
                    data: resumeData,
                    sourceURL: failure.sourceURL,
                    validator: failure.metadata
                )
            } catch {
                Logger(
                    subsystem: "com.playhead", category: "ForceQuitResume"
                ).error("persistResumeData (harvest) failed for \(failure.episodeId, privacy: .public): \(String(describing: error), privacy: .public)")
            }
        }

        // Release ownership before the first suspension below so a retry is
        // admitted immediately after resume persistence has completed.
        finishBackgroundTransfer(
            identity: failure.identity,
            episodeId: failure.episodeId
        )
        await recordBackgroundFailure(
            episodeId: failure.episodeId,
            cause: failure.cause,
            errorDescription: failure.errorDescription,
            bytesProcessed: failure.bytesReceived,
            stage: failure.stage
        )
    }

    private func recordBackgroundFailure(
        episodeId: String,
        cause: InternalMissCause,
        errorDescription: String,
        bytesProcessed: Int,
        stage: String
    ) async {
        let metadata = await SliceCompletionInstrumentation.recordFailed(
            cause: cause,
            deviceClass: DeviceClass.detect(),
            sliceDurationMs: 0,
            bytesProcessed: bytesProcessed,
            shardsCompleted: 0,
            extras: [
                "stage": stage,
                "error": errorDescription,
            ]
        )
        await workJournalRecorder.recordFailed(
            episodeId: episodeId,
            cause: cause,
            metadataJSON: metadata.encodeJSON()
        )
    }

    // MARK: - File Locations

    /// Derive a filesystem-safe name from an episode ID.
    /// Episode IDs can contain URL characters (://) so we SHA-256 hash them.
    static func safeFilename(for episodeId: String) -> String {
        let digest = SHA256.hash(data: Data(episodeId.utf8))
        return digest.map { String(format: "%02x", $0) }.joined()
    }

    /// Stable, collision-resistant staging name for one OS-owned background
    /// transfer. Episode identity alone is insufficient because duplicate
    /// same-episode tasks can coexist across URLSession roles or process
    /// rehydration. Hash the session/task identity so it is safe as a path
    /// component and cannot collide with another task's actor-hop bytes.
    static func backgroundStagingFilename(
        episodeId: String,
        fileExtension: String,
        identity: BackgroundTransferIdentity
    ) -> String {
        let identityMaterial =
            "\(identity.sessionIdentifier)\u{0}\(identity.taskIdentifier)"
        let identityHash = safeFilename(for: identityMaterial)
        return "\(safeFilename(for: episodeId)).\(identityHash).\(fileExtension)"
    }

    /// URL for a partially-downloaded episode file.
    func partialFileURL(for episodeId: String) -> URL {
        partialsDirectory.appendingPathComponent("\(Self.safeFilename(for: episodeId)).partial")
    }

    /// URL for a fully-downloaded, verified episode file.
    /// Uses the cached source extension so AVURLAsset can identify the codec.
    func completeFileURL(for episodeId: String) -> URL {
        let ext = resolveExtension(for: episodeId)
        return completeDirectory.appendingPathComponent("\(Self.safeFilename(for: episodeId)).\(ext)")
    }

    /// Audio extensions AVURLAsset can identify.
    private static let knownAudioExtensions: Set<String> = [
        "mp3", "m4a", "aac", "wav", "caf", "aiff", "mp4", "ogg", "opus"
    ]

    /// Keep every newly-written artifact inside the extension set used by
    /// sibling discovery, serving, cleanup, and eviction. Podcast enclosure
    /// URLs commonly end in routing suffixes such as `.php` or `.ashx`; those
    /// bytes are still audio, and defaulting their cache name to `.mp3`
    /// preserves the same header-sniffing fallback used for extensionless
    /// URLs without creating an artifact the cache can no longer find.
    static func cacheExtension(for url: URL) -> String {
        let candidate = url.pathExtension.lowercased()
        return knownAudioExtensions.contains(candidate) ? candidate : "mp3"
    }

    // MARK: - Completeness pin (playhead-wrj8)

    /// File extension for the per-episode completeness pin sidecar.
    static let pinExtension = "pin"

    /// URL of the `<hash>.pin` completeness sidecar for an episode. Shares
    /// the audio file's hashed basename but a distinct extension, so it is
    /// never mistaken for the audio file by `resolveExtension`/eviction/etc.
    func pinFileURL(for episodeId: String) -> URL {
        completeDirectory
            .appendingPathComponent("\(Self.safeFilename(for: episodeId)).\(Self.pinExtension)")
    }

    private enum PinReadState {
        case absent
        case valid(AudioAssetPin)
        case invalid
    }

    /// Separates a genuinely absent legacy sidecar from an unreadable or
    /// malformed managed sidecar. Collapsing both to `nil` makes corruption
    /// fail open by reclassifying a partial managed file as legacy.
    private func pinReadState(for episodeId: String) -> PinReadState {
        let url = pinFileURL(for: episodeId)
        guard FileManager.default.fileExists(atPath: url.path) else {
            return .absent
        }
        guard let data = try? Data(contentsOf: url),
              let pin = try? JSONDecoder().decode(AudioAssetPin.self, from: data) else {
            return .invalid
        }
        return .valid(pin)
    }

    /// Loads a valid persisted completeness pin, or `nil` for callers that
    /// only need the decoded value. Completeness decisions use
    /// `pinReadState(for:)` so malformed pins never inherit legacy behavior.
    func loadPin(for episodeId: String) -> AudioAssetPin? {
        guard case .valid(let pin) = pinReadState(for: episodeId) else {
            return nil
        }
        return pin
    }

    /// Atomically writes/overwrites the completeness pin for an episode.
    @discardableResult
    func writePin(_ pin: AudioAssetPin, for episodeId: String) -> Bool {
        strongPinVerifications[episodeId] = nil
        #if DEBUG
        guard !forcePinWriteFailureForTesting else { return false }
        #endif
        // playhead-0hi9: stamp the live weak fingerprint onto every pin from
        // one place rather than threading it through nine construction sites.
        // Never overwrites a weak the caller supplied, and never downgrades a
        // recorded weak to the empty sentinel.
        var pin = pin
        if AudioFingerprint.nonEmptyWeak(pin.weakFingerprint) == nil {
            pin.weakFingerprint = AudioFingerprint.nonEmptyWeak(
                fingerprintCache[episodeId]?.weak
            )
        }
        let url = pinFileURL(for: episodeId)
        guard let data = try? JSONEncoder().encode(pin) else { return false }
        do {
            let dir = url.deletingLastPathComponent()
            if !FileManager.default.fileExists(atPath: dir.path) {
                try FileManager.default.createDirectory(
                    at: dir, withIntermediateDirectories: true
                )
            }
            try data.write(to: url, options: .atomic)
            return true
        } catch {
            logger.error(
                "Failed to write completeness pin for \(episodeId, privacy: .public): \(String(describing: error), privacy: .public)"
            )
            return false
        }
    }

    /// Production placements must not expose bytes unless their fail-closed
    /// sidecar was durably written. Tests may still call `writePin` directly
    /// and inspect its result.
    private func requirePinWrite(
        _ pin: AudioAssetPin,
        for episodeId: String
    ) throws {
        guard writePin(pin, for: episodeId) else {
            throw DownloadManagerError.downloadFailed(
                episodeId,
                "Failed to persist completeness pin"
            )
        }
    }

    /// Removes the completeness pin for an episode (no-op when absent).
    func deletePin(for episodeId: String) {
        strongPinVerifications[episodeId] = nil
        try? FileManager.default.removeItem(at: pinFileURL(for: episodeId))
    }

    /// playhead-wrj8: finalize a streaming download's pin to the actual
    /// on-disk length so `servingURLIfComplete` starts serving it. Reads the
    /// true size from disk (authoritative — the streamed byte counter could
    /// drift) and stamps the optional strong hash. Called from the detached
    /// streaming-completion task.
    fileprivate func finalizeStreamingPin(
        episodeId: String,
        fileURL: URL,
        sourceURL: String,
        etag: String?,
        sha256: String?
    ) throws {
        let attrs = try? FileManager.default.attributesOfItem(
            atPath: fileURL.path
        )
        let size = (attrs?[.size] as? Int64) ?? 0
        guard size > 0 else {
            // No bytes on disk — leave any incomplete pin in place so the
            // file stays withheld rather than being marked complete-at-zero.
            throw DownloadManagerError.downloadFailed(
                episodeId,
                "Cannot finalize an empty streamed artifact"
            )
        }
        try requirePinWrite(
            AudioAssetPin(
                expectedBytes: size,
                sha256: sha256,
                sourceURL: sourceURL,
                etag: etag
            ),
            for: episodeId
        )
        if let sha256 {
            rememberStrongPinVerification(
                episodeId: episodeId,
                fileURL: fileURL,
                expectedBytes: size,
                expectedHash: sha256
            )
        }
    }

    /// On-disk byte length of the complete audio file, or `nil` if absent.
    private func completeFileSize(for episodeId: String) -> Int64? {
        fileSize(at: completeFileURL(for: episodeId))
    }

    private func fileSize(at url: URL) -> Int64? {
        guard let attrs = try? FileManager.default.attributesOfItem(
            atPath: url.path
        ) else {
            return nil
        }
        return (attrs[.size] as? Int64)
    }

    private func listedAudioArtifactURLs(for episodeId: String) throws -> [URL] {
        let prefix = Self.safeFilename(for: episodeId)
        return try FileManager.default.contentsOfDirectory(
            at: completeDirectory,
            includingPropertiesForKeys: [.fileSizeKey]
        ).filter {
            $0.deletingPathExtension().lastPathComponent == prefix
                && Self.knownAudioExtensions.contains(
                    $0.pathExtension.lowercased()
                )
        }.sorted {
            $0.lastPathComponent < $1.lastPathComponent
        }
    }

    /// Read paths fail closed: an unavailable directory simply yields no
    /// serveable candidate. Destructive paths use the throwing enumerator
    /// below so they never mistake "could not inspect" for "nothing remains."
    private func audioArtifactURLs(for episodeId: String) -> [URL] {
        #if DEBUG
        if let override = audioArtifactURLsOverrideForTesting[episodeId] {
            return override
        }
        #endif
        return (try? listedAudioArtifactURLs(for: episodeId)) ?? []
    }

    /// Removes every supported-extension artifact sharing this episode's
    /// canonical basename. Internal so force-quit recovery can establish a
    /// genuinely clean target before dropping the shared completeness pin.
    func removeAllAudioArtifacts(for episodeId: String) throws {
        strongPinVerifications[episodeId] = nil
        for artifact in try listedAudioArtifactURLs(for: episodeId) {
            try FileManager.default.removeItem(at: artifact)
        }
        guard try listedAudioArtifactURLs(for: episodeId).isEmpty else {
            throw DownloadManagerError.downloadFailed(
                episodeId,
                "Audio artifacts remain after cleanup"
            )
        }
    }

    private func strongPinVerification(
        fileURL: URL,
        expectedBytes: Int64,
        expectedHash: String
    ) -> StrongPinVerification? {
        guard let attributes = try? FileManager.default.attributesOfItem(
            atPath: fileURL.path
        ) else {
            return nil
        }
        let size = (attributes[.size] as? NSNumber)?.int64Value
        guard size == expectedBytes else { return nil }
        return StrongPinVerification(
            path: fileURL.path,
            expectedHash: expectedHash.lowercased(),
            expectedBytes: expectedBytes,
            modificationDate: attributes[.modificationDate] as? Date,
            fileNumber: (attributes[.systemFileNumber] as? NSNumber)?
                .uint64Value
        )
    }

    private func rememberStrongPinVerification(
        episodeId: String,
        fileURL: URL,
        expectedBytes: Int64,
        expectedHash: String
    ) {
        strongPinVerifications[episodeId] = strongPinVerification(
            fileURL: fileURL,
            expectedBytes: expectedBytes,
            expectedHash: expectedHash
        )
    }

    private func strongPinMatches(
        episodeId: String,
        fileURL: URL,
        expectedBytes: Int64,
        expectedHash: String
    ) -> Bool {
        guard let verification = strongPinVerification(
            fileURL: fileURL,
            expectedBytes: expectedBytes,
            expectedHash: expectedHash
        ) else {
            strongPinVerifications[episodeId] = nil
            return false
        }
        if strongPinVerifications[episodeId] == verification {
            return true
        }
        #if DEBUG
        strongPinVerificationHashCount += 1
        #endif
        guard let actualHash = try? FileHasher.sha256(fileURL: fileURL),
              actualHash.lowercased() == expectedHash.lowercased()
        else {
            strongPinVerifications[episodeId] = nil
            return false
        }
        strongPinVerifications[episodeId] = verification
        return true
    }

    private static func uniqueWeakPinCandidate(
        candidates: [URL],
        sourceURL: String?
    ) -> URL? {
        guard let sourceExtension = sourceURL
            .flatMap(URL.init(string:))
            .map(cacheExtension(for:))
        else {
            return nil
        }
        let matches = candidates.filter {
            $0.pathExtension.lowercased() == sourceExtension
        }
        guard matches.count == 1 else { return nil }
        return matches[0]
    }

    /// playhead-wrj8: the single completeness gate. Returns the audio file
    /// URL ONLY when the episode has a fully-downloaded, serveable artifact:
    ///
    ///   * file present AND a pin exists → serveable iff the on-disk length
    ///     reached the pin's `expectedBytes` (a truncated / mid-stream /
    ///     interrupted file has fewer bytes and is withheld);
    ///   * file present AND no pin → treated as complete-by-existence
    ///     (legacy files downloaded before wrj8), so the change is
    ///     non-destructive;
    ///   * file absent → `nil`.
    ///
    /// Every "is this cached?" and "may I overwrite this?" decision routes
    /// through here so playback, analysis, and the download writers all
    /// agree on exactly one immutable artifact per episode.
    func servingURLIfComplete(for episodeId: String) -> URL? {
        // A progressive transfer owns a mutable canonical path until
        // `finalizeStreamingTransfer` closes the file, rewrites the final pin,
        // and releases this token. In particular, the growing file can equal a
        // server-supplied Content-Length just before the byte pump reaches its
        // completion boundary. Never let that transient equality promote a
        // still-owned path through the ordinary immutable-cache surface.
        guard activeStreamingTransfer?.episodeId != episodeId else {
            return nil
        }

        let candidates = audioArtifactURLs(for: episodeId).filter {
            fileSize(at: $0) != nil
        }

        guard !candidates.isEmpty else { return nil }

        let selected: URL
        switch pinReadState(for: episodeId) {
        case .invalid:
            // A sidecar exists, so this is a managed artifact. Corruption or
            // unreadability must withhold it; only genuine absence receives
            // the legacy compatibility behavior.
            return nil
        case .valid(let pin):
            guard pin.expectedBytes > 0 else { return nil }
            let completeCandidates = candidates.filter {
                // A managed weak pin is a byte-exact completeness claim.
                // Under-length is incomplete; over-length is corruption or a
                // different stitch and must not be accepted merely because it
                // crossed the expected threshold. Strong pins receive the
                // hash check below as an additional identity proof.
                (fileSize(at: $0) ?? -1) == pin.expectedBytes
            }
            guard !completeCandidates.isEmpty else { return nil }

            if let expectedHash = pin.sha256 {
                // Size is completeness, not identity. Validate the strong pin
                // even when only one extension candidate exists; otherwise a
                // lone wrong-stitch sibling with the same length is accepted.
                guard let hashMatch = completeCandidates.first(where: {
                    strongPinMatches(
                        episodeId: episodeId,
                        fileURL: $0,
                        expectedBytes: pin.expectedBytes,
                        expectedHash: expectedHash
                    )
                }) else {
                    return nil
                }
                selected = hashMatch
            } else if completeCandidates.count == 1 {
                selected = completeCandidates[0]
            } else if let sourceMatch = Self.uniqueWeakPinCandidate(
                candidates: completeCandidates,
                sourceURL: pin.sourceURL
            ) {
                selected = sourceMatch
            } else {
                // A weak pin with no unique canonical sibling cannot
                // authenticate an arbitrary same-sized file. This includes
                // case-variant siblings whose normalized extensions collide.
                return nil
            }
        case .absent:
            // Legacy no-pin artifacts remain non-destructively serveable even
            // when a refreshed enclosure URL changes its extension, but only
            // while exactly one supported-extension sibling exists. With no
            // pin there is no identity evidence that can choose between
            // multiple candidates, so arbitrary sorted-first selection would
            // risk playing a stale or differently stitched asset.
            guard candidates.count == 1 else { return nil }
            selected = candidates[0]
        }

        // Preserve the on-disk extension spelling. Bootstrap deliberately
        // accepts supported extensions case-insensitively; normalizing the
        // selected path to lowercase here would make `completeFileURL` point
        // at a different, nonexistent sibling on a case-sensitive volume.
        extensionCache[episodeId] = selected.pathExtension
        return selected
    }

    /// Resolve the file extension for an episode. Checks the in-memory cache
    /// first, then scans the complete directory for a matching file.
    private func resolveExtension(for episodeId: String) -> String {
        if let cached = extensionCache[episodeId] {
            return cached
        }
        // Scan the directory for any file matching this hash prefix.
        let prefix = Self.safeFilename(for: episodeId)
        let fm = FileManager.default
        if let files = try? fm.contentsOfDirectory(atPath: completeDirectory.path) {
            for file in files where file.hasPrefix(prefix) {
                let ext = (file as NSString).pathExtension
                if Self.knownAudioExtensions.contains(ext.lowercased()) {
                    extensionCache[episodeId] = ext
                    return ext
                }
            }
        }
        return "mp3"
    }

    /// Returns the cached file URL if the episode is fully downloaded.
    /// playhead-wrj8: gated on completeness (a truncated / mid-stream file
    /// no longer reads as "cached"), so playback + analysis never resolve a
    /// partial artifact.
    func cachedFileURL(for episodeId: String) -> URL? {
        guard let url = servingURLIfComplete(for: episodeId) else { return nil }
        touchAccess(episodeId: episodeId)
        return url
    }

    /// Returns true if the episode audio is fully cached on disk.
    func isCached(episodeId: String) -> Bool {
        servingURLIfComplete(for: episodeId) != nil
    }

    /// Returns the set of episode IDs that have fully-downloaded cached audio.
    /// Scans the complete directory and reverse-maps filenames back to episode IDs
    /// using the access log (which tracks all episodes that have been downloaded).
    func allCachedEpisodeIds() -> Set<String> {
        Set(accessLog.keys.filter { isCached(episodeId: $0) })
    }

    /// Returns the subset of the supplied episode IDs whose complete audio
    /// file is present on disk.
    ///
    /// Unlike `allCachedEpisodeIds()`, this does not depend on the LRU access
    /// log being keyed by episode ID. Each candidate still routes through the
    /// canonical completeness gate: filename existence alone cannot promote a
    /// managed, under-length artifact to downloaded/ready.
    func cachedEpisodeIds(matching episodeIds: Set<String>) -> Set<String> {
        Set(episodeIds.filter { servingURLIfComplete(for: $0) != nil })
    }

    // MARK: - Fingerprinting

    /// Returns the current fingerprint for an episode, if available.
    ///
    /// playhead-0hi9: falls back to the `.pin` sidecar on a cache miss. The
    /// cache is process-local and empty after every relaunch, and this method
    /// is the ONLY source of `currentAudioFingerprint` for
    /// `AnalysisWorkScheduler.resolveAnalysisAssetId` — so before this
    /// fallback existed, the canonical-SHA upgrade could not fire on any
    /// launch after the one that performed the download, which is half of why
    /// one episode ended up with two `analysis_assets` rows. Rehydrating also
    /// warms the cache so repeated calls stay a dictionary read.
    func fingerprint(for episodeId: String) -> AudioFingerprint? {
        if let cached = fingerprintCache[episodeId] {
            return cached
        }
        guard let rehydrated = fingerprintFromPin(for: episodeId) else {
            return nil
        }
        fingerprintCache[episodeId] = rehydrated
        return rehydrated
    }

    /// playhead-0hi9: reconstruct an `AudioFingerprint` from the durable
    /// completeness sidecar.
    ///
    /// Pins written since 0hi9 carry the exact weak string that was live at
    /// download time, so the rehydrated value is byte-identical to what the
    /// in-memory cache held. Older pins predate the field; for those the weak
    /// is rebuilt from what the sidecar does carry (`sourceURL`, `etag`,
    /// `expectedBytes`). That reconstruction omits `Last-Modified`, which the
    /// pin never recorded, so it matches the live weak only for assets served
    /// without that header — best-effort by construction, and strictly better
    /// than the nil it replaces.
    private func fingerprintFromPin(for episodeId: String) -> AudioFingerprint? {
        guard let pin = loadPin(for: episodeId) else { return nil }
        let weak: String?
        if let persisted = AudioFingerprint.nonEmptyWeak(pin.weakFingerprint) {
            weak = persisted
        } else if let source = pin.sourceURL, let url = URL(string: source) {
            weak = AudioFingerprint.makeWeak(
                url: url,
                metadata: HTTPAssetMetadata(
                    etag: pin.etag,
                    contentLength: pin.expectedBytes > 0 && pin.expectedBytes != Int64.max
                        ? pin.expectedBytes
                        : nil,
                    lastModified: nil
                )
            )
        } else {
            weak = nil
        }
        // A pin with neither a weak nor a strong identity carries no
        // information the caller can act on; report the miss rather than
        // caching an empty fingerprint that would shadow a later real one.
        guard weak != nil || pin.sha256 != nil else { return nil }
        return AudioFingerprint(weak: weak ?? "", strong: pin.sha256)
    }

    /// Computes or returns the strong fingerprint (full SHA-256) for a cached file.
    /// Returns nil if the file is not fully cached.
    func computeStrongFingerprint(episodeId: String, url: URL) throws -> AudioFingerprint? {
        let fileURL = completeFileURL(for: episodeId)
        guard FileManager.default.fileExists(atPath: fileURL.path) else { return nil }

        // If we already have a strong fingerprint, return it.
        if let existing = fingerprintCache[episodeId], existing.strong != nil {
            return existing
        }

        let hash = try FileHasher.sha256(fileURL: fileURL)
        let metadata = metadataCache[episodeId] ?? HTTPAssetMetadata(
            etag: nil, contentLength: nil, lastModified: nil
        )
        let weakFP = AudioFingerprint.makeWeak(url: url, metadata: metadata)
        let fp = AudioFingerprint(weak: weakFP, strong: hash)
        fingerprintCache[episodeId] = fp
        return fp
    }

    // MARK: - Integrity Verification

    /// Verifies that a cached file matches the expected strong fingerprint.
    func verifyIntegrity(episodeId: String, expectedHash: String) throws -> Bool {
        let fileURL = completeFileURL(for: episodeId)
        guard FileManager.default.fileExists(atPath: fileURL.path) else {
            throw DownloadManagerError.fileNotFound(fileURL.path)
        }
        let actualHash = try FileHasher.sha256(fileURL: fileURL)
        return actualHash.lowercased() == expectedHash.lowercased()
    }

    // MARK: - Analysis Protection

    /// Marks an episode as in-use (active analysis or in-flight playback),
    /// protecting its cached audio file from LRU eviction. playhead-wrj8:
    /// refcounted — balance every call with exactly one
    /// ``unprotectFromAnalysis(episodeId:)``.
    func protectForAnalysis(episodeId: String) {
        analysisProtectedEpisodes[episodeId, default: 0] += 1
    }

    /// Releases one unit of eviction protection. The episode becomes
    /// eviction-eligible only when the refcount returns to zero.
    func unprotectFromAnalysis(episodeId: String) {
        guard let count = analysisProtectedEpisodes[episodeId] else { return }
        if count <= 1 {
            analysisProtectedEpisodes.removeValue(forKey: episodeId)
        } else {
            analysisProtectedEpisodes[episodeId] = count - 1
        }
    }

    /// Test/diagnostic accessor: episodes currently protected from eviction.
    func protectedEpisodeIdsForTesting() -> Set<String> {
        Set(analysisProtectedEpisodes.keys)
    }

    // MARK: - Cache Size & Eviction

    /// Updates the maximum cache size.
    func setMaxCacheSize(_ bytes: Int64) {
        maxCacheBytes = bytes
    }

    /// Returns the current total size of cached audio files.
    func currentCacheSize() throws -> Int64 {
        let fm = FileManager.default
        var total: Int64 = 0
        for dir in [completeDirectory, partialsDirectory] {
            guard let contents = try? fm.contentsOfDirectory(
                at: dir, includingPropertiesForKeys: [.fileSizeKey]
            ) else { continue }
            for fileURL in contents {
                let values = try? fileURL.resourceValues(forKeys: [.fileSizeKey])
                total += Int64(values?.fileSize ?? 0)
            }
        }
        return total
    }

    /// Evicts least-recently-used cached files until under the size limit.
    /// Never evicts episodes with active analysis.
    func evictIfNeeded() async throws {
        var currentSize = try currentCacheSize()
        guard currentSize > maxCacheBytes else { return }

        logger.info("Cache over budget: \(currentSize) / \(self.maxCacheBytes) bytes. Evicting...")

        // Build eviction candidates: completed files, sorted by last access (oldest first).
        let fm = FileManager.default
        guard let contents = try? fm.contentsOfDirectory(
            at: completeDirectory,
            includingPropertiesForKeys: [.fileSizeKey]
        ) else { return }

        var candidates: [(
            episodeId: String?,
            displayName: String,
            urls: [URL],
            size: Int64,
            lastAccess: Date
        )] = []
        // Build a reverse map from hashed filename → episode ID. Union
        // accessLog with protected and active sets so a file deposited
        // outside the manager (or before its accessLog entry was
        // written) can still be identified for protection checks.
        var knownEpisodeIds = Set(accessLog.keys)
            .union(analysisProtectedEpisodes.keys)
            .union(activeDownloads.keys)
            .union(bgInFlightEpisodes)
        if let activeStreamingTransfer {
            knownEpisodeIds.insert(activeStreamingTransfer.episodeId)
        }
        let hashToEpisodeId: [String: String] = Dictionary(
            uniqueKeysWithValues: knownEpisodeIds.map { (Self.safeFilename(for: $0), $0) }
        )

        var urlsByHashedBasename: [String: [URL]] = [:]
        for fileURL in contents {
            let name = fileURL.deletingPathExtension().lastPathComponent
            let ext = fileURL.pathExtension.lowercased()
            guard ext == Self.pinExtension
                    || Self.knownAudioExtensions.contains(ext) else {
                continue
            }
            urlsByHashedBasename[name, default: []].append(fileURL)
        }

        for (name, urls) in urlsByHashedBasename {
            guard urls.contains(where: {
                Self.knownAudioExtensions.contains(
                    $0.pathExtension.lowercased()
                )
            }) else {
                continue
            }
            let episodeId = hashToEpisodeId[name]
            guard !(episodeId.map { analysisProtectedEpisodes[$0] != nil } ?? false) else { continue }
            guard !(episodeId.map { activeDownloads.keys.contains($0) } ?? false) else { continue }
            guard !(episodeId.map { bgInFlightEpisodes.contains($0) } ?? false) else { continue }
            if let activeStreamingTransfer,
               episodeId == activeStreamingTransfer.episodeId {
                continue
            }

            let values = urls.map {
                try? $0.resourceValues(
                    forKeys: [.fileSizeKey, .contentModificationDateKey]
                )
            }
            let size = values.reduce(Int64(0)) {
                $0 + Int64($1?.fileSize ?? 0)
            }
            // Fall back to file mtime — not `.distantPast` — so a
            // freshly-deposited background download whose accessLog
            // entry hasn't been written yet isn't the first victim.
            let lastAccess = episodeId.flatMap { accessLog[$0] }
                ?? values.compactMap { $0?.contentModificationDate }.max()
                ?? .distantPast
            candidates.append((
                episodeId,
                episodeId ?? name,
                urls,
                size,
                lastAccess
            ))
        }

        // Sort: least recently accessed first.
        candidates.sort { $0.lastAccess < $1.lastAccess }

        for candidate in candidates {
            guard currentSize > maxCacheBytes else { break }

            // Completeness identity is episode-scoped, so extension siblings
            // and the shared pin are one eviction unit.
            let audioURLs = candidate.urls.filter {
                Self.knownAudioExtensions.contains(
                    $0.pathExtension.lowercased()
                )
            }
            let pinURLs = candidate.urls.filter {
                $0.pathExtension.lowercased() == Self.pinExtension
            }
            // Delete bytes first and the completeness barrier last. A failed
            // audio deletion therefore leaves the pin in place and cannot
            // expose a partial sibling through legacy no-pin semantics.
            for url in audioURLs {
                try fm.removeItem(at: url)
            }
            for url in pinURLs {
                try fm.removeItem(at: url)
            }
            currentSize -= candidate.size
            // Only scrub the per-episode caches when we resolved a real
            // episode id. Writing nil at the hashed-filename key would
            // be a no-op AND, worse, leave any cache entries keyed by
            // the real id (held under a different filename hash) leaked.
            if let id = candidate.episodeId {
                accessLog[id] = nil
                fingerprintCache[id] = nil
                metadataCache[id] = nil
            }
            logger.info("Evicted \(candidate.displayName): freed \(candidate.size) bytes")
        }
    }

    /// Manually clear all cached episode audio.
    func clearCache() async throws {
        // A cache clear is also an ownership boundary. Stop every foreground
        // and background writer before unlinking its path. Retiring background
        // task identities first also makes cancellation-produced resume data
        // and already-staged completion callbacks cleanup-only.
        cancelActiveStreamingTransfer()
        for task in activeDownloads.values {
            task.cancel()
        }
        activeDownloads.removeAll()
        _ = await retireBackgroundTransfers(episodeId: nil)
        try clearCache(
            enumerating: { directory in
                try FileManager.default.contentsOfDirectory(
                    at: directory,
                    includingPropertiesForKeys: nil
                )
            },
            removing: { url in
                try FileManager.default.removeItem(at: url)
            }
        )
    }

    /// Fail-closed bulk deletion. Completeness pins are barriers shared by
    /// every extension sibling with the same canonical basename, so they must
    /// be removed only after every potentially serveable audio path is gone.
    /// Keeping the deletion operation injectable under DEBUG gives tests a
    /// deterministic failure point without weakening the production ordering.
    private func clearCache(
        enumerating contentsOfDirectory: (URL) throws -> [URL],
        removing removeItem: (URL) throws -> Void
    ) throws {
        // Enumerate every directory before deleting anything. If any
        // directory is inaccessible, the operation throws with cache indexes
        // and completeness barriers untouched instead of reporting success
        // after a partial clear.
        let completeContents = try contentsOfDirectory(completeDirectory)
        let partialContents = try contentsOfDirectory(partialsDirectory)
        let resumeContents = try contentsOfDirectory(resumeDataDirectory)
        // playhead-kkzu: attribution is per-transfer state, so a cache clear
        // takes it with the bytes it describes. Enumerated with the others,
        // before anything is removed, so the fail-closed property holds.
        let attributionContents = try contentsOfDirectory(attributionDirectory)

        let (pins, nonPins) = completeContents.reduce(
            into: (pins: [URL](), nonPins: [URL]())
        ) { result, url in
            if url.pathExtension.lowercased() == Self.pinExtension {
                result.pins.append(url)
            } else {
                result.nonPins.append(url)
            }
        }
        // Bytes first. If any removal fails, throwing here retains every pin
        // and prevents a leftover sibling from becoming a legacy hit.
        for fileURL in nonPins.sorted(by: {
            $0.lastPathComponent < $1.lastPathComponent
        }) {
            try removeItem(fileURL)
        }
        // Barriers last, only after the complete directory contains no
        // potentially serveable non-pin artifact.
        for fileURL in pins.sorted(by: {
            $0.lastPathComponent < $1.lastPathComponent
        }) {
            try removeItem(fileURL)
        }
        // Include partial, resume-data and attribution directories so a clear
        // + relaunch cannot resurrect a suspended transfer.
        for contents in [partialContents, resumeContents, attributionContents] {
            for fileURL in contents.sorted(by: {
                $0.lastPathComponent < $1.lastPathComponent
            }) {
                try removeItem(fileURL)
            }
        }
        accessLog.removeAll()
        fingerprintCache.removeAll()
        metadataCache.removeAll()
        strongPinVerifications.removeAll()
        logger.info("Cache cleared")
    }

    #if DEBUG
    func _clearCacheForTesting(
        removing removeItem: @escaping @Sendable (URL) throws -> Void
    ) throws {
        try clearCache(
            enumerating: { directory in
                try FileManager.default.contentsOfDirectory(
                    at: directory,
                    includingPropertiesForKeys: nil
                )
            },
            removing: removeItem
        )
    }

    func _clearCacheForTesting(
        enumerating contentsOfDirectory:
            @escaping @Sendable (URL) throws -> [URL],
        removing removeItem: @escaping @Sendable (URL) throws -> Void
    ) throws {
        try clearCache(
            enumerating: contentsOfDirectory,
            removing: removeItem
        )
    }
    #endif

    /// Removes cached audio for a specific episode.
    func removeCache(for episodeId: String) async throws {
        // Symmetric with clearCache(): deletion revokes any foreground
        // writer's ownership before touching its canonical paths.
        cacheOwnershipGenerationByEpisode[
            episodeId,
            default: 0
        ] &+= 1
        if let analysisWorkScheduler {
            await analysisWorkScheduler.retireDownloadAnalysis(
                episodeId: episodeId,
                downloadId: episodeId
            )
        }
        await cancelDownload(episodeId: episodeId)
        let fm = FileManager.default
        let partial = partialFileURL(for: episodeId)
        try removeAllAudioArtifacts(for: episodeId)
        if fm.fileExists(atPath: partial.path) {
            try fm.removeItem(at: partial)
        }
        // playhead-wrj8: drop the completeness pin too so a later
        // re-download is treated as a fresh artifact rather than colliding
        // with a stale "complete" claim.
        deletePin(for: episodeId)
        // Symmetric blob cleanup so a future scan doesn't resurrect a
        // suspended-transfer event for an episode the user just deleted.
        try deleteResumeData(episodeId: episodeId)
        accessLog[episodeId] = nil
        fingerprintCache[episodeId] = nil
        metadataCache[episodeId] = nil
        strongPinVerifications[episodeId] = nil
    }

    // MARK: - Helpers

    private func touchAccess(episodeId: String) {
        accessLog[episodeId] = Date()
    }

    /// Called by the background download delegate when a transfer
    /// completes. Owns the final-placement file move (so it honors
    /// `cacheDirectory`, including custom directories injected by tests
    /// or future multi-profile hosts), synthesizes a real weak
    /// fingerprint from URL + HTTP response metadata, computes the
    /// strong fingerprint, and enqueues analysis. (playhead-24cm.1
    /// I3 + I4.)
    ///
    /// `stagedURL` points at a process-global temp file the delegate
    /// moved out of the OS-owned location during the synchronous
    /// callback. We are responsible for moving it into
    /// `completeFileURL(for:)` and cleaning up if the move fails.
    /// `originalURL` and `metadata` may be nil if the delegate could
    /// not harvest them (e.g. the task carried no HTTP response); in
    /// that case we preserve whatever weak fingerprint a prior
    /// progressive/streaming pass already cached, rather than
    /// regressing it to the empty sentinel.
    func handleBackgroundDownloadComplete(
        episodeId: String,
        stagedURL: URL,
        originalURL: URL?,
        metadata: HTTPAssetMetadata?,
        transferIdentity: BackgroundTransferIdentity? = nil
    ) async {
        let fm = FileManager.default
        let capturedCacheOwnershipGeneration =
            cacheOwnershipGenerationByEpisode[episodeId, default: 0]

        // playhead-kkzu: the show this transfer belongs to, recovered from the
        // sidecar `backgroundDownload` wrote — possibly in an earlier process.
        // Read once, up front, so every exit below sees the same answer.
        // Absent means the transfer predates the sidecar (an in-flight upgrade)
        // or was started by a path that could not name a show; either way the
        // absence is named rather than defaulted.
        let attribution = loadDownloadAttribution(episodeId: episodeId)
            ?? .unattributed(
                reason: .resumeWithoutRecordedShow,
                isExplicitDownload: false
            )

        if let transferIdentity {
            guard !backgroundCallbackIsRetired(
                identity: transferIdentity,
                episodeId: episodeId
            ) else {
                try? fm.removeItem(at: stagedURL)
                try? deleteResumeData(episodeId: episodeId)
                // playhead-kkzu: terminal for this transfer — a retry re-queues
                // through `backgroundDownload` and rewrites the sidecar.
                deleteDownloadAttribution(episodeId: episodeId)
                finishBackgroundTransfer(
                    identity: transferIdentity,
                    episodeId: episodeId
                )
                logger.info(
                    "Discarded retired background completion for \(episodeId, privacy: .public)"
                )
                return
            }
            // A task reattached from a prior process may not have been in this
            // instance's admission map. Register it before any await so a
            // concurrent clear/remove can retire its already-staged callback.
            activeBackgroundTransfers[transferIdentity] = episodeId
            bgInFlightEpisodes.insert(episodeId)
        }

        // Playback's foreground stream owns the canonical artifact. A
        // background transfer already in flight when playback began must
        // discard its late deposit without changing path resolution.
        if activeStreamingTransfer?.episodeId == episodeId {
            try? fm.removeItem(at: stagedURL)
            finishBackgroundTransfer(
                identity: transferIdentity,
                episodeId: episodeId
            )
            try? deleteResumeData(episodeId: episodeId)
            deleteDownloadAttribution(episodeId: episodeId)
            logger.info(
                "Background completion for \(episodeId, privacy: .public): discarded because foreground stream owns the artifact"
            )
            return
        }

        // Cache the file extension so `completeFileURL(for:)` returns
        // the right path. Mirrors the progressive path which sets
        // `extensionCache` from `url.pathExtension` before computing
        // `completeFileURL`.
        // The delegate normally canonicalizes this already, but normalize
        // again at the actor boundary because tests and future resume paths
        // may provide an arbitrary staged suffix directly.
        let extensionSource = originalURL ?? stagedURL
        extensionCache[episodeId] = Self.cacheExtension(for: extensionSource)

        let destURL = completeFileURL(for: episodeId)
        let destDir = destURL.deletingLastPathComponent()

        // playhead-wrj8: REFUSE to overwrite an already-complete pinned
        // artifact. This is the vector that best matches the incident: a
        // background transfer (or a force-quit RESUME, which finalizes
        // through this same path) completing with a DIFFERENT DAI ad
        // stitch must NOT clobber the bytes the user already played and
        // marked ads against. Discard the staged deposit, adopt a pin for
        // the existing file if it has none, and keep the played copy.
        if let existing = servingURLIfComplete(for: episodeId) {
            try? fm.removeItem(at: stagedURL)
            if loadPin(for: episodeId) == nil,
               let size = completeFileSize(for: episodeId) {
                writePin(
                    AudioAssetPin(
                        expectedBytes: size,
                        sha256: fingerprintCache[episodeId]?.strong,
                        sourceURL: originalURL?.absoluteString,
                        etag: metadata?.etag
                    ),
                    for: episodeId
                )
            }
            touchAccess(episodeId: episodeId)
            finishBackgroundTransfer(
                identity: transferIdentity,
                episodeId: episodeId
            )
            try? deleteResumeData(episodeId: episodeId)
            deleteDownloadAttribution(episodeId: episodeId)
            logger.info("Background completion for \(episodeId, privacy: .public): complete pinned artifact already present — kept it, discarded re-fetch at \(existing.lastPathComponent, privacy: .public)")
            return
        }

        do {
            // Establish a durable incomplete state before the staged file can
            // appear at the canonical path. A crash or final-pin write error
            // then leaves the bytes withheld rather than legacy-serveable.
            try requirePinWrite(
                AudioAssetPin(
                    expectedBytes: Int64.max,
                    sha256: nil,
                    sourceURL: originalURL?.absoluteString,
                    etag: metadata?.etag
                ),
                for: episodeId
            )
            try removeAllAudioArtifacts(for: episodeId)
            if !fm.fileExists(atPath: destDir.path) {
                try fm.createDirectory(at: destDir, withIntermediateDirectories: true)
            }
            try fm.moveItem(at: stagedURL, to: destURL)
            // playhead-h3h: stamp the freshly-deposited cached audio so
            // the protection class matches the parent directory. Files
            // moved in from the URLSession session container inherit
            // the system-default class, which is `.complete` on
            // background-session containers — that would block reads
            // during pre-first-unlock BG processing windows.
            try? fm.setAttributes(
                [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication],
                ofItemAtPath: destURL.path
            )
            logger.info("Background download complete for \(episodeId)")
        } catch {
            logger.error(
                "Failed to place background download for \(episodeId, privacy: .public): \(String(describing: error), privacy: .public)"
            )
            // Best-effort cleanup of the staged file; otherwise it
            // accumulates in the temp directory.
            try? fm.removeItem(at: stagedURL)
            finishBackgroundTransfer(
                identity: transferIdentity,
                episodeId: episodeId
            )
            await recordBackgroundFailure(
                episodeId: episodeId,
                cause: .pipelineError,
                errorDescription: String(describing: error),
                bytesProcessed: 0,
                stage: "downloadManager.placeBackgroundCompletion"
            )
            return
        }

        // Synthesize the weak fingerprint from URL + HTTP metadata
        // harvested by the delegate. Mirrors what the progressive path
        // does at `performStreamingDownload`. If the delegate could
        // not harvest a URL or response, fall back to whatever a prior
        // foreground pass already populated — never overwrite a real
        // weak with the empty sentinel (playhead-24cm.1 I4).
        if let originalURL {
            let synthesizedMetadata = metadata ?? HTTPAssetMetadata(
                etag: nil, contentLength: nil, lastModified: nil
            )
            metadataCache[episodeId] = synthesizedMetadata
            let weakFP = AudioFingerprint.makeWeak(
                url: originalURL, metadata: synthesizedMetadata
            )
            fingerprintCache[episodeId] = AudioFingerprint(weak: weakFP, strong: nil)
        }

        // Compute the strong fingerprint (full SHA-256). Logging the
        // hash failure rather than swallowing it via `try?` so support
        // triage can spot a corrupt deposit; the cache entry without
        // the strong field still carries the weak fingerprint and is
        // useful to dedup re-downloads (playhead-24cm.1 I4).
        // playhead-wrj8: pin the freshly-deposited artifact as COMPLETE
        // (actual on-disk length) so it becomes the single immutable served
        // copy. Written regardless of whether the strong-hash step below
        // succeeds, so the file can never remain "unpinned/incomplete" and
        // get re-fetched into a different stitch.
        guard let size = completeFileSize(for: episodeId) else {
            logger.error(
                "Background download for \(episodeId, privacy: .public) has no readable final size; leaving it withheld"
            )
            finishBackgroundTransfer(
                identity: transferIdentity,
                episodeId: episodeId
            )
            await recordBackgroundFailure(
                episodeId: episodeId,
                cause: .pipelineError,
                errorDescription: "Final artifact size is unreadable",
                bytesProcessed: 0,
                stage: "downloadManager.finalizeBackgroundPin"
            )
            return
        }
        do {
            try requirePinWrite(
                AudioAssetPin(
                    expectedBytes: size,
                    sha256: nil,
                    sourceURL: originalURL?.absoluteString,
                    etag: metadata?.etag
                ),
                for: episodeId
            )
        } catch {
            logger.error(
                "Failed to finalize completeness pin for \(episodeId, privacy: .public): \(String(describing: error), privacy: .public)"
            )
            finishBackgroundTransfer(
                identity: transferIdentity,
                episodeId: episodeId
            )
            await recordBackgroundFailure(
                episodeId: episodeId,
                cause: .pipelineError,
                errorDescription: String(describing: error),
                bytesProcessed: Int(size),
                stage: "downloadManager.finalizeBackgroundPin"
            )
            return
        }

        do {
            let strongHash = try FileHasher.sha256(fileURL: destURL)
            let weakFP = fingerprintCache[episodeId]?.weak ?? ""
            fingerprintCache[episodeId] = AudioFingerprint(weak: weakFP, strong: strongHash)
            // Backfill the strong hash into the pin now that it's computed.
            guard var pin = loadPin(for: episodeId) else {
                throw DownloadManagerError.downloadFailed(
                    episodeId,
                    "Final completeness pin disappeared"
                )
            }
            pin.sha256 = strongHash
            guard writePin(pin, for: episodeId) else {
                throw DownloadManagerError.downloadFailed(
                    episodeId,
                    "Strong completeness pin write failed"
                )
            }
            rememberStrongPinVerification(
                episodeId: episodeId,
                fileURL: destURL,
                expectedBytes: pin.expectedBytes,
                expectedHash: strongHash
            )
            // playhead-kkzu: was `context: nil` — the single line that made
            // every background/auto download record a NULL podcastId.
            await enqueueAnalysisIfNeeded(
                episodeId: episodeId,
                sourceFingerprint: strongHash,
                context: attribution
            )
            deleteDownloadAttribution(episodeId: episodeId)
        } catch {
            logger.error(
                "Strong fingerprint hash failed for \(episodeId, privacy: .public): \(String(describing: error), privacy: .public)"
            )
            finishBackgroundTransfer(
                identity: transferIdentity,
                episodeId: episodeId
            )
            await recordBackgroundFailure(
                episodeId: episodeId,
                cause: .pipelineError,
                errorDescription: String(describing: error),
                bytesProcessed: Int(size),
                stage: "downloadManager.strongBackgroundIdentity"
            )
            return
        }

        if backgroundCompletionLostOwnership(
            identity: transferIdentity,
            episodeId: episodeId,
            capturedCacheOwnershipGeneration:
                capturedCacheOwnershipGeneration
        ) {
            // Explicit deletion may have run while analysis enqueue yielded.
            // It already removed the artifact; do not recreate cache indexes
            // or a resume-data record on this stale continuation.
            finishBackgroundTransfer(
                identity: transferIdentity,
                episodeId: episodeId
            )
            return
        }

        // WorkJournal success belongs to the manager, not the delegate's
        // staging callback. At this point canonical placement, the strong pin,
        // and required analysis enqueue work have all completed.
        let journalFinalizationID = UUID()
        let recorder = workJournalRecorder
        let journalTask = Task {
            guard !Task.isCancelled else { return }
            await recorder.recordFinalized(episodeId: episodeId)
        }
        backgroundJournalFinalizations[episodeId] =
            BackgroundJournalFinalization(
                id: journalFinalizationID,
                task: journalTask
            )
        await journalTask.value
        if backgroundJournalFinalizations[episodeId]?.id
            == journalFinalizationID {
            backgroundJournalFinalizations.removeValue(
                forKey: episodeId
            )
        }
        if backgroundCompletionLostOwnership(
            identity: transferIdentity,
            episodeId: episodeId,
            capturedCacheOwnershipGeneration:
                capturedCacheOwnershipGeneration
        ) {
            finishBackgroundTransfer(
                identity: transferIdentity,
                episodeId: episodeId
            )
            return
        }
        touchAccess(episodeId: episodeId)
        finishBackgroundTransfer(
            identity: transferIdentity,
            episodeId: episodeId
        )
        // A successful completion can resurrect a stale resume-data blob
        // from a prior failed attempt: without this delete, the next
        // cold-launch `scanForSuspendedTransfers` would emit a phantom
        // `appForceQuitRequiresRelaunch` event for an episode that's
        // already fully downloaded.
        try? deleteResumeData(episodeId: episodeId)
        // Background pre-cache deposits weren't subject to LRU eviction;
        // without this, the cache could grow past `maxCacheBytes` until
        // the next foreground download. Best-effort: a failure here just
        // defers cleanup to the next eviction trigger.
        try? await evictIfNeeded()
        // playhead-4dqe: announce the completed background deposit.
        //
        // Placed HERE, at the last statement of the success path, on purpose:
        // every early `return` above is a completion that did NOT leave a
        // servable pinned artifact (retired callback, foreground stream owns
        // it, placement failed, unreadable size, pin write failed, hash failed,
        // lost ownership). An observer told about those would start a
        // preparation wait for bytes that will never resolve, and the resulting
        // `no_pinned_file` give-up would be this hook's fault, not the network's.
        notifyBackgroundDownloadCompleted(
            episodeId: episodeId,
            sourceURL: originalURL ?? URL(string: loadPin(for: episodeId)?.sourceURL ?? "")
        )
    }

    /// playhead-4dqe: called once per background download that lands a servable
    /// pinned artifact, with the episode's ORIGINAL enclosure URL when known.
    ///
    /// This is the seam that makes Dan's "yes rediff on background" possible:
    /// before it, day-0 at download time existed only for the explicit
    /// "Download & Analyze" tap, so every auto-downloaded episode had to wait
    /// for the 19-second in-play race or the lagged ≥24 h sweep. Wired once by
    /// `PlayheadRuntime`; left `nil` in tests and previews, so the download path
    /// is byte-identical without it.
    ///
    /// A CALLBACK RATHER THAN A STREAM, deliberately. `progressUpdates()` is an
    /// `AsyncStream` because it has many subscribers and drops are harmless;
    /// this has exactly one consumer and a drop is a lost day-0 attempt, so it
    /// is a direct hand-off, backed by a small bounded buffer for completions
    /// that land before the observer is installed (oldest evicted, and logged).
    /// Same `set…` shape as `setAnalysisWorkScheduler` / `setDAIStitchRecorder`.
    /// playhead-cnql: installing an observer also DRAINS whatever completed
    /// before it arrived. Without this the buffer would only ever grow.
    ///
    /// playhead-oa82: RETURNS how many buffered completions the install drained.
    /// The installer records that number on the diagnostics session file, and it
    /// is the only evidence available about which side of the race the install
    /// landed on: a non-zero drain says completions had already arrived and were
    /// replayed, which is a materially different story from an install that got
    /// there first. `@discardableResult` because every existing caller installs
    /// for the side effect and the count is new information, not a new
    /// obligation.
    @discardableResult
    func setBackgroundDownloadCompletionObserver(
        _ observer: @escaping @Sendable (String, URL?) -> Void
    ) -> Int {
        self.backgroundDownloadCompletionObserver = observer
        let backlog = pendingBackgroundDownloadCompletions
        pendingBackgroundDownloadCompletions.removeAll()
        for completed in backlog {
            observer(completed.episodeId, completed.sourceURL)
        }
        return backlog.count
    }

    private func notifyBackgroundDownloadCompleted(episodeId: String, sourceURL: URL?) {
        guard let observer = backgroundDownloadCompletionObserver else {
            bufferBackgroundDownloadCompletion(
                episodeId: episodeId,
                sourceURL: sourceURL
            )
            return
        }
        observer(episodeId, sourceURL)
    }

    private func bufferBackgroundDownloadCompletion(
        episodeId: String,
        sourceURL: URL?
    ) {
        pendingBackgroundDownloadCompletions.removeAll { $0.episodeId == episodeId }
        pendingBackgroundDownloadCompletions.append(
            (episodeId: episodeId, sourceURL: sourceURL)
        )
        while pendingBackgroundDownloadCompletions.count
            > Self.maxPendingBackgroundDownloadCompletions {
            let evicted = pendingBackgroundDownloadCompletions.removeFirst()
            logger.error(
                "Day-0 kickoff hand-off buffer full; dropped completion for \(evicted.episodeId, privacy: .public)"
            )
        }
    }

    #if DEBUG
    /// playhead-cnql test seam: how many completions are waiting for an
    /// observer. The eviction cap is otherwise unobservable, and an untested cap
    /// is how a bound becomes decorative.
    func _pendingBackgroundDownloadCompletionCountForTesting() -> Int {
        pendingBackgroundDownloadCompletions.count
    }

    /// playhead-cnql test seam: the buffer's own entry point. Driving the cap
    /// through 70 real completions would mean 70 file placements and 70 SHA-256
    /// passes to exercise one `while` loop.
    func _bufferBackgroundDownloadCompletionForTesting(
        episodeId: String,
        sourceURL: URL?
    ) {
        bufferBackgroundDownloadCompletion(
            episodeId: episodeId,
            sourceURL: sourceURL
        )
    }
    #endif

    /// Rebuilds the LRU access log from file modification dates.
    private func rebuildAccessLog() throws {
        let fm = FileManager.default
        guard let contents = try? fm.contentsOfDirectory(
            at: completeDirectory,
            includingPropertiesForKeys: [.contentModificationDateKey]
        ) else { return }

        for fileURL in contents {
            let name = fileURL.deletingPathExtension().lastPathComponent
            let values = try? fileURL.resourceValues(
                forKeys: [.contentModificationDateKey]
            )
            accessLog[name] = values?.contentModificationDate ?? Date.distantPast
        }
    }

}

// MARK: - DownloadProviding Conformance

extension DownloadManager: DownloadProviding {}

// MARK: - Progress Snapshot (playhead-btoa.2)

extension DownloadManager {
    /// Snapshot of the per-episode foreground download fraction for
    /// in-flight transfers. Episodes not currently downloading are absent
    /// from the map. Computed from the same `ForegroundAssistProgress`
    /// state that drives `progressUpdates()`. Per-call: O(N) over active
    /// downloads (typically tiny — single-digit episodes).
    ///
    /// Entries with `totalBytes == 0` (size-unknown transfers) are
    /// skipped to avoid divide-by-zero. Background-session transfers are
    /// out of scope here — see `bgInFlightEpisodes` for that lane.
    ///
    /// Used by the Activity provider once per refresh tick to populate
    /// `ActivityEpisodeInput.downloadFraction`.
    func progressSnapshot() -> [String: Double] {
        var result: [String: Double] = [:]
        for (episodeId, progress) in foregroundAssistProgress {
            guard progress.totalBytes > 0 else { continue }
            result[episodeId] =
                Double(progress.bytesWritten) / Double(progress.totalBytes)
        }
        return result
    }
}

// MARK: - Shared Reference Plumbing (playhead-24cm)

extension DownloadManager {
    /// Non-owning shared reference used by `PlayheadAppDelegate` to
    /// reach the live download manager during background wake events.
    /// Stored weakly so tests and non-app hosts don't keep a manager
    /// alive. The `PlayheadRuntime` registers its manager at boot.
    ///
    /// This is intentionally minimal — we don't expose a service
    /// locator, just a single slot the app delegate can consult.
    nonisolated(unsafe) private static var _shared: DownloadManager?

    /// Registers `manager` as the app-wide shared DownloadManager for
    /// background session wake-up routing. Pass `nil` to clear the slot
    /// — useful for test teardown so a later test starting in a fresh
    /// state does not accidentally observe a previous test's manager.
    @MainActor
    static func registerShared(_ manager: DownloadManager?) {
        _shared = manager
    }

    /// Current shared DownloadManager, if one has been registered.
    static var shared: DownloadManager? {
        _shared
    }

    /// Non-owning reference to the app delegate. Set by
    /// `PlayheadApp.registerAppDelegate(_:)` at boot so the URLSession
    /// finish-events callback can reach the pending-handler map.
    nonisolated(unsafe) private static var _appDelegate: PlayheadAppDelegate?

    @MainActor
    static func registerAppDelegate(_ delegate: PlayheadAppDelegate) {
        _appDelegate = delegate
    }

    static var appDelegate: PlayheadAppDelegate? {
        _appDelegate
    }
}

// MARK: - UserPreferencesSnapshot

/// Snapshot of the subset of `UserPreferences` that download manager
/// background configuration needs to read at URLSession-construction
/// time (which may be off-main, synchronous, and before SwiftData is
/// ready). Persisted in UserDefaults by the settings UI. See
/// `UserPreferences.allowsCellular` for the source of truth.
struct UserPreferencesSnapshot: Sendable {
    var allowsCellular: Bool
    /// playhead-jzik: mirrored copy of `UserPreferences.episodeSummariesEnabled`
    /// so the off-main-actor `EpisodeSummaryBackfillCoordinator` can read
    /// the toggle without a SwiftData hop. Defaults to `true` to match the
    /// SwiftData default; the Settings toggle calls
    /// `save(episodeSummariesEnabled:)` to keep the slot in sync.
    var episodeSummariesEnabled: Bool
    /// playhead-4dqe: mirrored copy of `UserPreferences.dayZeroAllowsCellular`
    /// — whether background PREPARATION traffic may use cellular. Read from
    /// `DayZeroRediffTrigger` and `URLSessionFullEpisodeFetcher`, neither of
    /// which has a SwiftData hop. Defaults to
    /// `RediffActivation.dayZeroAllowsCellularByDefault` (WiFi only), matching
    /// the SwiftData default so the two stores cannot disagree on day 1.
    var dayZeroAllowsCellular: Bool

    static let defaultsKey = "UserPreferencesSnapshot.allowsCellular"
    static let episodeSummariesDefaultsKey = "UserPreferencesSnapshot.episodeSummariesEnabled"
    static let dayZeroAllowsCellularDefaultsKey = "UserPreferencesSnapshot.dayZeroAllowsCellular"

    static var current: UserPreferencesSnapshot {
        current(from: .standard)
    }

    /// playhead-4dqe: the `UserDefaults`-injectable read. Extracted so the
    /// defaults and the round-trip are testable in an isolated suite rather
    /// than against the shared standard domain, where one test's write leaks
    /// into every other test in the process.
    static func current(from defaults: UserDefaults) -> UserPreferencesSnapshot {
        let allows = defaults.object(forKey: defaultsKey) as? Bool ?? true
        let summaries = defaults.object(forKey: episodeSummariesDefaultsKey) as? Bool ?? true
        let dayZeroCellular = defaults.object(forKey: dayZeroAllowsCellularDefaultsKey) as? Bool
            ?? RediffActivation.dayZeroAllowsCellularByDefault
        return UserPreferencesSnapshot(
            allowsCellular: allows,
            episodeSummariesEnabled: summaries,
            dayZeroAllowsCellular: dayZeroCellular
        )
    }

    static func save(allowsCellular: Bool) {
        save(allowsCellular: allowsCellular, to: .standard)
    }

    static func save(allowsCellular: Bool, to defaults: UserDefaults) {
        defaults.set(allowsCellular, forKey: defaultsKey)
    }

    static func save(episodeSummariesEnabled: Bool) {
        UserDefaults.standard.set(episodeSummariesEnabled, forKey: episodeSummariesDefaultsKey)
    }

    static func save(dayZeroAllowsCellular: Bool) {
        save(dayZeroAllowsCellular: dayZeroAllowsCellular, to: .standard)
    }

    static func save(dayZeroAllowsCellular: Bool, to defaults: UserDefaults) {
        defaults.set(dayZeroAllowsCellular, forKey: dayZeroAllowsCellularDefaultsKey)
    }
}

// MARK: - EpisodeDownloadDelegate

/// URLSession delegate for handling background episode download events.
///
/// Serves both the 24cm-split `interactive`/`maintenance` sessions and the
/// legacy `com.playhead.episode-downloads` session during the rollout
/// window. The session identifier is pulled from
/// `session.configuration.identifier` on each callback so downstream
/// observers can tell the lanes apart without the delegate tracking
/// per-lane state.
final class EpisodeDownloadDelegate: NSObject, URLSessionDownloadDelegate, Sendable {
    private let logger = Logger(subsystem: "com.playhead", category: "EpisodeDownload")

    /// Callback fired on the delegate queue once a completed background
    /// transfer has been staged into a process-global temp directory.
    /// Carries the episode ID, the staged file URL (caller takes
    /// ownership and is expected to move it into the cache), the
    /// original request URL, and HTTP response metadata harvested for
    /// weak-fingerprint synthesis (playhead-24cm.1 I3 + I4).
    ///
    /// Same init-once / read-many invariant as
    /// `onBackgroundDownloadFailed`.
    nonisolated(unsafe) var onBackgroundDownloadStaged: (
        (
            BackgroundTransferIdentity,
            String,
            URL,
            URL?,
            HTTPAssetMetadata?
        ) -> Void
    )?

    /// playhead-3xtw: fired on the delegate queue for each byte-progress
    /// callback of a background (pre-cache / on-demand) transfer, carrying
    /// `(episodeId, totalBytesWritten, totalBytesExpectedToWrite)`. The
    /// background session previously only LOGGED progress — so the
    /// download zone of the per-episode prepare control (and the Activity
    /// screen) could not observe an in-flight background transfer. Wiring
    /// this hook into `DownloadManager.broadcastProgress` closes that gap
    /// (the "44h1 will add" hook the didWriteData comment anticipated).
    /// Same init-once / read-many invariant as `onBackgroundDownloadStaged`.
    nonisolated(unsafe) var onBackgroundDownloadProgress: (
        (String, Int64, Int64) -> Void
    )?

    /// Invoked when the URLSession has drained all pending events after
    /// a background wake. Forwards the session's identifier so the app
    /// delegate can match it against its pending completion-handler map.
    nonisolated(unsafe) var onUrlSessionDidFinishEvents: ((String) -> Void)?

    /// Identity-qualified terminal failure callback. The delegate harvests
    /// every non-Sendable task field synchronously, then emits one Sendable
    /// value. DownloadManager performs optional resume persistence, ownership
    /// release, and WorkJournal failure recording in that actor order.
    ///
    /// Thread-safety invariant: init-once / read-many, matching the other
    /// delegate callbacks above.
    nonisolated(unsafe) var onBackgroundDownloadFailed:
        ((BackgroundTransferFailure) -> Void)?

    func urlSession(
        _ session: URLSession,
        downloadTask: URLSessionDownloadTask,
        didFinishDownloadingTo location: URL
    ) {
        guard let episodeId = downloadTask.taskDescription else {
            logger.warning("Background download finished but no episode ID set")
            return
        }
        let transferIdentity = BackgroundTransferIdentity(
            sessionIdentifier: session.configuration.identifier ?? "",
            taskIdentifier: downloadTask.taskIdentifier
        )

        let originalURL = downloadTask.originalRequest?.url
        // Harvest HTTP metadata for the weak fingerprint here, while the
        // delegate-queue stack still has access to the task. The actor
        // hop below cannot read `downloadTask.response` because
        // URLSessionDownloadTask is non-Sendable.
        let httpMetadata: HTTPAssetMetadata? = {
            guard let response = downloadTask.response as? HTTPURLResponse else {
                return nil
            }
            let reportedLength = response.expectedContentLength
            return HTTPAssetMetadata(
                etag: response.value(forHTTPHeaderField: "ETag"),
                contentLength: reportedLength > 0 ? reportedLength : nil,
                lastModified: response.value(forHTTPHeaderField: "Last-Modified")
            )
        }()
        stageBackgroundDownload(
            identity: transferIdentity,
            episodeId: episodeId,
            location: location,
            originalURL: originalURL,
            metadata: httpMetadata
        )
    }

    private func stageBackgroundDownload(
        identity: BackgroundTransferIdentity,
        episodeId: String,
        location: URL,
        originalURL: URL?,
        metadata: HTTPAssetMetadata?
    ) {
        // Stage under the same canonical extension set used by serving,
        // cleanup, and eviction. Routing suffixes such as .php/.ashx are not
        // audio types and must never create an invisible cache artifact.
        let ext = originalURL.map(DownloadManager.cacheExtension(for:)) ?? "mp3"

        // playhead-24cm.1 (I3): stage the file into a process-global temp
        // directory synchronously on the delegate queue. The OS-provided
        // `location` URL is only valid during this callback; once we
        // return, the file may be deleted. Staging into a stable temp
        // path lets the actor — which knows the real `cacheDirectory`,
        // even when it's not the default — perform the final placement.
        let stagingDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("PlayheadBGStaging", isDirectory: true)
        let stagedURL = stagingDir.appendingPathComponent(
            DownloadManager.backgroundStagingFilename(
                episodeId: episodeId,
                fileExtension: ext,
                identity: identity
            )
        )

        let fm = FileManager.default
        do {
            if !fm.fileExists(atPath: stagingDir.path) {
                try fm.createDirectory(at: stagingDir, withIntermediateDirectories: true)
            }
            if fm.fileExists(atPath: stagedURL.path) {
                try fm.removeItem(at: stagedURL)
            }
            try fm.moveItem(at: location, to: stagedURL)
            logger.info("Background download staged for \(episodeId)")

            // Hand the staged file to the actor, which will move it into
            // the correct `cacheDirectory`-relative `complete/` directory
            // and populate fingerprint state with a real weak fingerprint
            // (playhead-24cm.1 I3 + I4).
            onBackgroundDownloadStaged?(
                identity,
                episodeId,
                stagedURL,
                originalURL,
                metadata
            )
        } catch {
            logger.error("Failed to stage background download for \(episodeId): \(error.localizedDescription)")
            let bytesProcessed = (try? fm
                .attributesOfItem(atPath: location.path)[.size] as? Int) ?? 0
            onBackgroundDownloadFailed?(
                BackgroundTransferFailure(
                    identity: identity,
                    episodeId: episodeId,
                    resumeData: nil,
                    sourceURL: originalURL,
                    metadata: metadata,
                    cause: .pipelineError,
                    errorDescription: error.localizedDescription,
                    bytesReceived: bytesProcessed,
                    stage: "downloadManager.didFinishDownloadingTo"
                )
            )
        }
    }

    #if DEBUG
    func _stageBackgroundDownloadForTesting(
        identity: BackgroundTransferIdentity,
        episodeId: String,
        location: URL,
        originalURL: URL?,
        metadata: HTTPAssetMetadata? = nil
    ) {
        stageBackgroundDownload(
            identity: identity,
            episodeId: episodeId,
            location: location,
            originalURL: originalURL,
            metadata: metadata
        )
    }
    #endif

    func urlSession(
        _ session: URLSession,
        downloadTask: URLSessionDownloadTask,
        didWriteData bytesWritten: Int64,
        totalBytesWritten: Int64,
        totalBytesExpectedToWrite: Int64
    ) {
        let progress = totalBytesExpectedToWrite > 0
            ? Double(totalBytesWritten) / Double(totalBytesExpectedToWrite)
            : 0
        let episodeId = downloadTask.taskDescription ?? "<missing-task-description>"
        logger.debug("Episode \(episodeId, privacy: .public) download: \(String(format: "%.1f", progress * 100))%")
        // playhead-3xtw: emit through the DownloadManager broadcast
        // surface so `progressUpdates()` / `progressSnapshot()` reflect
        // in-flight BACKGROUND transfers (previously this callback only
        // logged). Guard the missing-taskDescription sentinel so we never
        // key progress under a bogus episode id.
        if downloadTask.taskDescription != nil {
            onBackgroundDownloadProgress?(episodeId, totalBytesWritten, totalBytesExpectedToWrite)
        }
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didCompleteWithError error: (any Error)?
    ) {
        guard let error else {
            // Success path is handled by didFinishDownloadingTo. Nothing
            // to do here — don't double-emit a finalized event.
            return
        }
        let episodeId = task.taskDescription ?? "<missing-task-description>"
        let transferIdentity = BackgroundTransferIdentity(
            sessionIdentifier: session.configuration.identifier ?? "",
            taskIdentifier: task.taskIdentifier
        )
        let cause = InternalMissCause.fromTaskError(error)
        logger.error("Episode \(episodeId, privacy: .public) download failed (\(cause.rawValue)): \(error.localizedDescription)")

        let nsError = error as NSError
        // Only URLSession populates the resume-data key in NSURLErrorDomain.
        // Harvest every task-bound value now and emit exactly one callback;
        // the manager actor persists the optional blob before releasing
        // ownership, then records the terminal failure.
        let resumeData: Data? = {
            guard nsError.domain == NSURLErrorDomain,
                  let data = nsError.userInfo[
                      NSURLSessionDownloadTaskResumeData
                  ] as? Data,
                  !data.isEmpty else {
                return nil
            }
            return data
        }()
        let sourceURL = task.originalRequest?.url ?? task.currentRequest?.url
        let harvestedMetadata: HTTPAssetMetadata? = {
            guard let response = task.response as? HTTPURLResponse else {
                return nil
            }
            let len = response.expectedContentLength
            return HTTPAssetMetadata(
                etag: response.value(forHTTPHeaderField: "ETag"),
                contentLength: len > 0 ? len : nil,
                lastModified: response.value(forHTTPHeaderField: "Last-Modified")
            )
        }()
        onBackgroundDownloadFailed?(
            BackgroundTransferFailure(
                identity: transferIdentity,
                episodeId: episodeId,
                resumeData: resumeData,
                sourceURL: sourceURL,
                metadata: harvestedMetadata,
                cause: cause,
                errorDescription: error.localizedDescription,
                bytesReceived: Int(task.countOfBytesReceived),
                stage: "downloadManager.didCompleteWithError"
            )
        )
    }

    /// Called by URLSession after all pending background events have
    /// been delivered to this delegate. Forwards the identifier so the
    /// `PlayheadAppDelegate` can invoke its stored completion handler.
    func urlSessionDidFinishEvents(forBackgroundURLSession session: URLSession) {
        guard let identifier = session.configuration.identifier else { return }
        onUrlSessionDidFinishEvents?(identifier)
    }
}
